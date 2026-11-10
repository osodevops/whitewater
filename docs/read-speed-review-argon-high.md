# Read Speed Review — Argon High

> **See also:** [`docs/pipelined-quorum-review-argon-high.md`](./pipelined-quorum-review-argon-high.md) for the Argon High adversarial review of pipelined quorum rounds, 3-range write bottlenecks, and the Argon High vs. SWE-2 MAX implementation roadmap.


> End-to-end architectural and code-level review of Feed consumption (read) performance across the Whitewater server (`whitewater`) and the Rust SDK (`whitewater-rust`), updated **2026-11-10** (`whitewater` `78cdaaa`, `whitewater-rust` `43bb04c`) with shipped optimizations, current benchmarks, and a concrete **Next Steps Pick-Up Guide** for the remaining read/consume bottlenecks.

---

## 1. Executive Summary & Current Status (2026-11-10)

In Whitewater, consuming a Feed (the equivalent of consuming a Kafka topic) takes four distinct server and client code paths depending on whether the consumer is stateless, a durable single consumer, or a cooperating member of a Subscription:

| Consumption Surface | Wire Endpoint(s) | Current Wire Format | Progress / Coordination Path | Status & Throughput (2026-11-10) |
|---|---|---|---|---|
| **Stateless binary replay / tail** (`bench_feed`, `bench-feed.py`, `Client::temporary_fetch_frames`, `Feed::fetch_frames`) | `POST /v1/readers/temporary/fetch` (`Accept: application/vnd.whitewater.frames`) | Raw stored frames (`FSR2` / `FSR3`) streamed in ~512 KiB chunks | Stateless (`next_cursor` in frame page header + peer-shared `ReadContinuationCache`) | **SHIPPED & FASTER THAN KAFKA**: **405k-424k rec/s** (1 range vs Kafka 341k-369k, **1.17x WIN**) and **397k-412k rec/s** (3 ranges vs Kafka 315k-330k, **1.26x WIN**) |
| **Stateless JSON replay / tail** (`Client::temporary_fetch`, `Feed::fetch`) | `POST /v1/readers/temporary/fetch` (default JSON) | JSON + per-field `base64` (`RecordResponse`) | Stateless (`next_cursor` in JSON + `ReadContinuationCache`) | **~32k-58k rec/s** (3.1x-4.5x slower than binary due to server decode + base64 + JSON) |
| **Public history walk** (`Client::records`, `Feed::records`) | `GET /v1/feeds/records` | JSON + per-field `base64` only | Stateless (`after` query param + `ReadContinuationCache`) | Bounded by JSON + base64 transcoding (no binary frame support yet) |
| **Durable named Reader** (`Reader`, `AutoReader`, `FeedReader`, `FeedConsumer`, `Feed::subscribe`) | `POST /v1/readers/open` -> `fetch` -> `ack` | **JSON + per-field `base64` only** (sticky session endpoints shipped in `2ef8aa0`) | **2 Control Plane Raft commits per page** (`RecordReaderFrontier`/`Delivery` on `fetch` + `AcknowledgeReader` on `ack`) | **OPEN BOTTLENECK**: ~10-50 pages/s (throttled by per-fetch/per-ack Raft writes + JSON-only wire format) |
| **Cooperative Subscription Member** (`Member` in `whitewater-rust`) | `POST /v1/subscriptions/members/claim` -> `fetch` -> `ack` | **JSON + per-field `base64` only** | **3 sequential RPCs per page** (`claim` RF3 2PC + `fetch` 2x RF3 reads + `ack` RF3 2PC), max **256 records/page** | **OPEN BOTTLENECK**: ~5-30 pages/s (bounded by 6+ internal quorum phases and 4 `SyncAll` Fjall txns per <=256 records) |

---

## 2. What Has Already Shipped (2026-11-01 -> 2026-11-10)

Since the initial Argon High read review on 2026-11-01, the following recommendations were implemented on the **stateless binary read path** (`temporary/fetch`), raising single-Feed and multi-range read speed from **169k-225k rec/s** (and 62k rec/s on 3-range Feeds) to **397k-424k rec/s**:

1. **Rust SDK Binary Frame Fetch + Cursor-Chained Streaming Prefetch (`whitewater-rust` `8f638ad`, `df93583`)**:
   - Wired `src/frame.rs` into `Transport::request_frame_page` (`whitewater-rust/src/transport.rs:64-111`), which incrementally decodes streamed `application/vnd.whitewater.frames` responses and fires an `on_header` oneshot as soon as `next_cursor` is parsed so page $k+1$ prefetch overlaps page $k$ body transfer.
   - Added `Client::temporary_fetch_frames` and `Feed::fetch_frames`.
2. **Sticky Session Endpoints + `AutoReader` in `whitewater-rust` (`2ef8aa0`)**:
   - `Reader` and `Writer` now pin to the Node endpoint that opened the session (`Client::session_call` / `self.pinned` in `whitewater-rust/src/reader.rs:39-130`), preventing per-request round-robin from stranding Node-local `ReaderPacingTable` state.
   - Added `AutoReader` (`ReadTuner`) to grow page limits automatically when pages return full.
3. **Zero-Copy `Bytes` Read Spans, Header-Only Size Checks, Lock-Free Committed Reads & 65,536-Frame Clamps (`whitewater` `c62772c`)**:
   - Raised inner range page clamps from `2,048` to `MAX_LOGICAL_READ_FRAMES` (`65,536`) under the `96 MiB` byte budget.
   - Replaced repeated full-body CRC32 passes in `logical_frame_bytes` with a single header-only size check.
   - Moved local committed segment reads outside the range mutex and coalesced streamed response chunks to ~512 KiB.
4. **Verify-Once BLAKE3 Tracking & Parallel Range Reads (`whitewater` `285bb5b`)**:
   - `FileActiveRangeStore` now tracks process-verified positions (frames appended by this process or already BLAKE3-verified on an earlier read in `src/active_range/store.rs`), cutting 16k-frame server page assembly time in half (~37 ms -> ~18 ms) while keeping full verification on first read after open/recovery/materialization and end-to-end SDK CRC32 checks.
   - `read_complete_feed` now fetches all ranges concurrently.
5. **Multi-Range `ReadContinuationCache` (`whitewater` `78cdaaa`)**:
   - `read_paginated_feed` (`src/api.rs:18661-18950`) now maintains a bounded (4,096-entry) continuation cache keyed by `(FeedId, last_emitted_cursor)` holding per-range resume positions and a placement fingerprint, shared across peers via `/internal/feed/read-continuation`.
   - Multi-range paginated reads jumped from **62k rec/s to 397k-412k rec/s** on 3 ranges.

---

## 3. Next Steps & Pick-Up Guide (Prioritized Open Suggestions)

The stateless binary replay path (`temporary/fetch`) now beats the Kafka Java SDK on both 1-range and 3-range Feeds. The **next wave of read speed improvements** is about bringing **durable SDK consumers** (`Reader`, `FeedConsumer`, `Subscription` `Member`) up to that same 300k-400k+ rec/s tier and removing the remaining server/SDK overheads.

### Priority Matrix

| Step | Priority | Area | Summary | Expected Gain |
|---|---|---|---|---|
| **Step 1** | **P0** | Server + `whitewater-rust` | Support `Accept: application/vnd.whitewater.frames` on `/v1/readers/fetch`, `/v1/subscriptions/members/fetch`, and `/v1/feeds/records`, and switch `Reader::fetch` / `FeedConsumer` / `Member::fetch` to binary frames. | **3x-4.5x higher throughput** on durable Readers & Subscription Members; keeps `FSR3` LZ4 compressed on the wire. |
| **Step 2** | **P0** | Server (`api.rs`, `reader.rs`) | Remove per-fetch Control Plane Raft commits (`RecordReaderFrontier`/`RecordReaderDelivery`) and support **piggybacked `ack_cursor`** on `/v1/readers/fetch`. | **10x-50x higher page rate** for named Readers & `FeedConsumer`; halves RPCs per page (2 -> 1) and eliminates Raft leader serialization. |
| **Step 3** | **P1** | Server (`api.rs`) | Replace per-read `read_placement` linearizable Raft `InspectPlacement` commands with local applied catalog reads (`active_range_map` + `active_range_assignments_for_feed`). | Saves **2-3 Raft quorum round-trips** (and follower->leader HTTP forwards) on every single read request. |
| **Step 4** | **P1** | Server (`store.rs`, `api.rs`) | Carry `(ingest_time_ns, message_id)` in `StoredRangeFrame` from `EntryIndex` (or peek the 60-byte `FSR2` header) during multi-range k-way merges, fix double `decode_record` in `fetch_frontier_page`, and add `/internal/active-range/read/committed-bin`. | Eliminates per-frame `decode_record` heap allocations during multi-range merges and removes base64+JSON on cross-Node owner reads. |
| **Step 5** | **P1** | Server (`api.rs`, `replication.rs`) | Short-circuit `verify_read_quorum` / `verify_local_read_quorum` on the first confirming peer replica, and cache follower `(CommitPosition, digest)` watermarks from replication acks on the Append Owner. | Eliminates 2s tail stalls when 1 of 3 nodes is slow/down and removes `/internal/active-range/read/evidence` RPCs on owner reads. |
| **Step 6** | **P2** | `whitewater-rust` + Server | Replace 50ms sleep polling on `wait_ms` with a commit-watermark `tokio::sync::watch`, add `wait_ms` to `/v1/readers/fetch`, fix `Feed::subscribe` (0ms spin) & `for_each` (1ms spin), and batch pages over `FeedConsumer`'s channel. | Cuts tail delivery latency from 50-250ms to `<1ms`, eliminates idle CPU spin, and removes per-record boxed `Future` allocations. |
| **Step 7** | **P2** | Server (`api.rs`, `reader.rs`) | Collapse double RF3 quorum read in `subscription_member_fetch` into one `inspect` call, raise `SUBSCRIPTION_MEMBER_FETCH_MAX_LIMIT` (`256` -> `4_096`), and add combined `claim`+`fetch` / `ack`+`fetch`. | Cuts Subscription Member quorum phases per page by 50%-66% and increases max batch size by 16x. |

---

## 4. Detailed Technical Specifications for Open Next Steps

### Step 1 (P0): Binary Frame Transport (`application/vnd.whitewater.frames`) for Named Readers, Subscription Members, and `/v1/feeds/records`

- **Where to look**:
  - Server:
    - `temporary_reader_fetch` binary framing: `C:\Code\Stackapps\whitewater\src\api.rs:11260-11340, 11452-11460`
    - `reader_fetch`: `C:\Code\Stackapps\whitewater\src\api.rs:10734-11040`
    - `subscription_member_fetch`: `C:\Code\Stackapps\whitewater\src\api.rs:4458-4820`
    - `read_feed_records`: `C:\Code\Stackapps\whitewater\src\api.rs:19225-19260`
  - Rust SDK:
    - `Reader::fetch`: `C:\Code\Stackapps\whitewater-rust\src\reader.rs:38-84`
    - `Member::fetch`: `C:\Code\Stackapps\whitewater-rust\src\member.rs:112-155`
    - `Client::records`: `C:\Code\Stackapps\whitewater-rust\src\client.rs:1060-1085`
- **Problem**:
  - In `bytes-route-performance-review.md`, binary frame reads (`application/vnd.whitewater.frames`) measured **3.1x faster on 1 feed** (116k vs 32k rec/s) and **4.5x faster on 3 feeds** (262k vs 58k rec/s) than JSON.
  - However, `Reader::fetch` (`FeedConsumer`, `FeedReader`, `AutoReader`), `Member::fetch`, and `Client::records` still only speak JSON + per-field base64.
- **Implementation Plan**:
  1. On `/v1/feeds/records` (`read_feed_records`), check `wants_frame_binary(&headers)` and return `encode_frame_fetch_stream(next_cursor, frames)` with `Content-Type: application/vnd.whitewater.frames`.
  2. On `/v1/readers/fetch` (`reader_fetch`), when `wants_frame_binary(&headers)` is true:
     - Pass `delivered_cursor` as the frame stream's `next_cursor` slot, and emit response metadata headers (`x-whitewater-acknowledged-cursor`, `x-whitewater-recommended-capacity`, `x-whitewater-retry-after-ms`) alongside the `application/vnd.whitewater.frames` body stream so the binary frame page parser (`frame::parse_frame_page`) works without changing the frame wire layout!
     - Skip `decode_record` and `record_response` completely when `wants_frame_binary(&headers)` is true.
  3. In `whitewater-rust/src/reader.rs`, update `Reader::fetch` to call `request_frame_page` (falling back to JSON when a scripted test `Transport` returns `SdkError::Refused("binary fetch is not supported...")`, matching how `temporary_fetch_frames` handles test doubles).

---

### Step 2 (P0): Decouple Named Reader `fetch` & `ack` from Control Plane Raft Writes + Piggyback `ack_cursor`

- **Where to look**:
  - `reader_fetch` catalog commit: `C:\Code\Stackapps\whitewater\src\api.rs:10923-11020`
  - `reader_ack` catalog commit: `C:\Code\Stackapps\whitewater\src\api.rs:11158-11195`
  - `PersistentRaftStore::persist`: `C:\Code\Stackapps\whitewater\src\control_plane.rs:95-109, 273-318`
  - `FjallReaderProgressStore`: `C:\Code\Stackapps\whitewater\src\reader.rs:1129-1349`
- **Problem**:
  - Every non-empty `reader_fetch` commits `Command::RecordReaderFrontier` or `Command::RecordReaderDelivery` through OpenRaft before returning the page, and every `reader_ack` commits `Command::AcknowledgeReader` through OpenRaft.
  - `PersistentRaftStore::persist` writes and `sync_all()`s the entire `PersistentRaftData` JSON file twice per Raft commit.
- **Implementation Plan**:
  1. **Add optional `ack_cursor: Option<String>` to `ReaderFetchRequest`**:
     - When `Reader::ack()` is called in a consumer loop (`FeedConsumer`, `FeedReader::for_each`, `Feed::subscribe`), allow latching the cursor to acknowledge on the next `fetch` call (or flushing on `close()` / idle timeout).
     - On the server, `reader_fetch` applies the acknowledgement and the new delivery in a single operation, cutting round-trips and state writes in half.
  2. **Make `delivered_cursor` self-describing so `fetch` is read-only**:
     - Encode the delivered per-range positions `(RangeId -> (RangePosition, cursor))` + `request_id` + `session_epoch` into an authenticated/checkable `delivered_cursor` token (or cache the delivery bracket in the Node-local session table, since sessions are now sticky to the opening Node!).
     - Only persist durable state on `ack` (when `acknowledged_cursor` advances), turning `reader_fetch` into a pure data-plane read with **zero** Raft commits per fetch.
  3. **Move durable Reader `ack` state to `FjallReaderProgressStore` / RF3 progress shards**:
     - Wire `FjallReaderProgressStore` (`src/reader.rs:1129`) so `ack` commits to the dedicated progress store rather than bloating the Control Plane Raft log.

---

### Step 3 (P1): Eliminate Double/Triple Linearizable Raft `read_placement` Calls Per Read

- **Where to look**:
  - `read_placement`: `C:\Code\Stackapps\whitewater\src\api.rs:17968-18020`
  - Call sites in `api.rs`: lines `10853`, `18463`, `18478`, `18574`, `18956`, `19081`, `19198`, `19216`.
- **Problem**:
  - When `state.control_plane.is_some()`, `read_placement` calls `control_plane.execute_commands(vec![Command::InspectPlacement { feed }])`, which forwards an HTTP POST to the Raft leader if the serving Node is a follower, runs `raft.ensure_linearizable()` (a Raft heartbeat quorum round-trip), serializes the `RangeMap` to `serde_json::Value`, and deserializes it back.
  - Every `read_single_range`, `read_paginated_feed`, `read_complete_feed`, and `read_reader_frontier` calls `read_placement` **twice** (before and after reading frames).
- **Implementation Plan**:
  - In `read_placement`, read `state.control.active_range_map(feed_id).await` and `state.control.active_range_assignments_for_feed(feed_id).await` directly from the local applied catalog (the same fast path `subscription_member_fetch` uses at `src/api.rs:5563-5566`).
  - Why this is safe and fail-closed:
    1. Every individual range page read (`read_range_page_inner` and `read_range_cursor_page_inner` in `src/active_range/replication.rs`) already checks `self.control.active_range_assignment_by_id(assignment.range_id) == Some(assignment.clone())` before and after reading the store.
    2. Every range page read on a Control Plane Riverbed already corroborates `CommitPosition` and `generation`/`ownership_epoch` against a 2-of-3 RF3 replica quorum (`verify_read_quorum` / `verify_local_read_quorum`).
    3. Only if the local catalog has no `RangeMap` for `feed_id` (unapplied new Feed) should `read_placement` fall back to `control_plane.execute_commands(InspectPlacement)`.

---

### Step 4 (P1): Zero-Decode Multi-Range Merge Headers & Binary Internal Owner Read Hop

- **Where to look**:
  - `StoredRangeFrame` & `EntryIndex`: `C:\Code\Stackapps\whitewater\src\active_range\store.rs:1740-1751, 3710-3760`
  - Multi-range merge loops calling `decode_record`:
    - `fetch_frontier_page`: `C:\Code\Stackapps\whitewater\src\api.rs:5618` and `5644` (called **twice** per record!)
    - `read_reader_frontier`: `C:\Code\Stackapps\whitewater\src\api.rs:18544`
    - `read_paginated_feed`: `C:\Code\Stackapps\whitewater\src\api.rs:19036`
    - `merge_feed_frames`: `C:\Code\Stackapps\whitewater\src\api.rs:18350`
  - Internal owner read route: `C:\Code\Stackapps\whitewater\src\api.rs:17859-17966, 18280-18338` (`/internal/active-range/read/committed`).
- **Problem**:
  1. Multi-range k-way merges only need `(ingest_time_ns, message_id)` to order frames across ranges, yet they call `decode_record(&frame.frame)` on every frame — which runs `crc32fast::hash`, decompresses LZ4 if `FSR3`, and allocates `key: Vec<u8>`, `payload: Vec<u8>`, and `metadata: BTreeMap`.
  2. In `fetch_frontier_page` (`src/api.rs:5618, 5644`), `decode_record` is called when refilling `heads[index]` and then called **a second time** on the exact same frame when popping `heads[index]`.
  3. When a Node is not a replica of a range (e.g. a >3-Node Riverbed or a lagging follower falling back to the owner), `fetch_range_page` calls `/internal/active-range/read/committed`, which still encodes frames as `frame_base64` strings inside JSON (`ReadRangePageResponse`).
- **Implementation Plan**:
  1. Add `pub ingest_time_ns: i64` and `pub message_id: Uuid` to `StoredRangeFrame` (populated directly from `EntryIndex` in `store.rs`, where both fields are already in memory, or via `codec::peek_record_header` which already exists from commit `c62772c`!).
  2. Use `(frame.ingest_time_ns, frame.message_id)` in `read_paginated_feed`, `read_reader_frontier`, and `merge_feed_frames` so binary multi-range reads perform **zero** `decode_record` calls.
  3. In `fetch_frontier_page`, keep the decoded `StoredRecord` in `heads` so JSON callers decode once instead of twice.
  4. Add `/internal/active-range/read/committed-bin` returning packed binary frames (matching `/internal/active-range/owner/append-batch-bin`).

---

### Step 5 (P1): Short-Circuit Read Quorum Corroboration & Cache Follower Watermarks on Append Owner

- **Where to look**:
  - `verify_read_quorum` & `verify_local_read_quorum`: `C:\Code\Stackapps\whitewater\src\api.rs:17665-17813`
  - `MajorityAppendCoordinator` follower responses: `C:\Code\Stackapps\whitewater\src\active_range\majority.rs`
- **Problem**:
  - On the first range page of every read (`expected_commit.is_none()`), `verify_read_quorum` / `verify_local_read_quorum` sends `/internal/active-range/read/evidence` to all other replicas using `join_all` with a 2-second timeout.
  - If 1 of the 2 peer replicas is slow or down, `join_all` waits up to 2 seconds even though the other peer replica already confirmed the boundary in `<1 ms`.
  - Furthermore, when the serving Node is the Append Owner, it already receives each follower's committed position and frame digest on every replication batch round, yet it still makes a separate `/internal/active-range/read/evidence` HTTP call on every read!
- **Implementation Plan**:
  1. On the Append Owner, record each follower's latest confirmed `(CommitPosition, frame_digest)` in a per-range in-memory watermark map whenever a replica append-batch round succeeds.
  2. In `verify_read_quorum`, if the owner's `position` is `<=` a follower's cached confirmed `CommitPosition` (and `>=` `range_commit_proof`), corroborate immediately in memory with **0 network calls**.
  3. When an `/internal/active-range/read/evidence` RPC is needed (follower reads or cold owner reads), poll peer futures with `FuturesUnordered` and return as soon as the first valid peer confirms `committed >= position` with matching `digest`.

---

### Step 6 (P2): Event-Driven Tail Long-Polling (`wait_ms`) & SDK Consumer Loop Polish

- **Where to look**:
  - Server tail loop: `C:\Code\Stackapps\whitewater\src\api.rs:11419-11490` (`temporary_reader_fetch`)
  - SDK loops:
    - `Feed::subscribe`: `C:\Code\Stackapps\whitewater-rust\src\feed.rs` (0ms tight loop on empty page)
    - `FeedReader::for_each`: `C:\Code\Stackapps\whitewater-rust\src\feed.rs` (`retry_after_ms.unwrap_or(1).max(1)` -> 1ms poll = 1,000 req/s when idle)
    - `FeedConsumer`: `C:\Code\Stackapps\whitewater-rust\src\consumer.rs:45, 321-329, 443-448` (`EMPTY_POLL = 250ms` fixed sleep + per-record boxed `Future`)
- **Implementation Plan**:
  1. Add a `tokio::sync::watch::Sender<CommitPosition>` to `FileActiveRangeStoreInner` (notified on `commit` / `append_commit_batch`) so `temporary_reader_fetch` (and a new optional `wait_ms` on `/v1/readers/fetch`) wakes immediately on commit instead of sleeping 50ms per poll.
  2. In `whitewater-rust`, add exponential/adaptive idle backoff (`5ms -> 50ms` or server `wait_ms`) to `Feed::subscribe`, `FeedReader::for_each`, and `FeedConsumer` so none of them spin at 0-1ms or stall for a fixed 250ms.
  3. In `FeedConsumer::start_channel_mode` (`whitewater-rust/src/consumer.rs:321`), send `Result<Vec<FeedMessage<K, V>>, SdkError>` (whole pages) over the `mpsc` channel and pop from a `VecDeque` in `FeedConsumer::next()`, eliminating one heap-allocated `Pin<Box<dyn Future>>` and one channel send per record.

---

### Step 7 (P2): Subscription Member Fast Path (`claim` / `fetch` / `ack`)

- **Where to look**:
  - `subscription_member_fetch`: `C:\Code\Stackapps\whitewater\src\api.rs:4458-4820`
  - `SUBSCRIPTION_MEMBER_FETCH_MAX_LIMIT`: `C:\Code\Stackapps\whitewater\src\api.rs:4399` (`256`)
  - `SubscriptionProgressCoordinator`: `C:\Code\Stackapps\whitewater\src\reader.rs:2435-2850`
- **Implementation Plan**:
  1. In `subscription_member_fetch` (`src/api.rs:4496, 4520`), replace the two separate RF3 quorum reads (`read_member_state(&coordinator)` followed by `coordinator.read_committed()`) with a single `coordinator.inspect()` quorum read that returns both `committed` and `members` in one pass.
  2. Raise `SUBSCRIPTION_MEMBER_FETCH_MAX_LIMIT` from `256` to `4_096` (bounded by `MAX_LOGICAL_READ_BYTES`).
  3. Add optional `claim: bool` / `ack: Option<...>` fields on `/v1/subscriptions/members/fetch` (or a combined `/v1/subscriptions/members/poll` endpoint) so a cooperating member can ack the previous work unit, claim the next lease, and fetch its records in **1 HTTP round-trip** instead of 3.

---

## 5. Verification Checklist When Picking Up Any Step

1. **Before benchmarking**:
   - Rebuild the Linux release binary for `bench_feed` (`cargo build --release --example bench_feed --target-dir target-linux` inside `rust:1.90-bookworm`) and verify its timestamp is newer than `whitewater-rust/src/*`.
   - Ensure `RUST_LOG` is set to `info` (never `debug`, which costs ~20-30% throughput).
2. **Correctness & regression gates** (mandatory before committing):
   - In `whitewater`: `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all-targets`, and the acceptance suite (`scripts/run-acceptance.py`).
   - In `whitewater-rust`: `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all-targets`.
