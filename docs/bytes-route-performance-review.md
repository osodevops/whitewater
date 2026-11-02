# Bytes-route performance review — reads and writes, Rust SDK as client

Scope: the end-to-end data plane between `whitewater-rust` and a
three-Node RF3 Riverbed, judged against one goal — **records travel as
bytes, never as JSON/base64, from the SDK to the replica store and back**.
JSON stays the curl/ops-friendly default contract; the bytes route is the
SDK's throughput conduit.

Reviewed at `whitewater` `add207b` (FSA1 ingress `b349232`) and
`whitewater-rust` `e08851e` (opt-in `Writer::binary_appends`). Companion
docs: [performance-testing.md](performance-testing.md) (methodology,
`BENCH_RESULT` contract, historical baselines) and
[read-speed-review-argon-high.md](read-speed-review-argon-high.md)
(read-path deep dive; its sticky-session and binary temporary-fetch items
have since shipped).

## 1. Summary

| Path | Today | Verdict |
|---|---|---|
| Read, SDK ← Node | `application/vnd.whitewater.frames` on `/v1/readers/temporary/fetch` only | **Bytes route proven**: 3.1–4.5x over JSON (measured below). Not yet on `Reader::fetch`, `Member::fetch`, `/v1/feeds/records`. |
| Write, SDK → ingress | `FSA1` frames, opt-in (`Writer::binary_appends`) | Wire is bytes; JSON is default. SDK binary path drops Metadata and compression. |
| Write, ingress → owner → replicas | Ingress decodes `FSA1` then **re-encodes base64**; owner forward is JSON; frames carried as `frame_base64: String` until the replica send decodes them again | **Bytes route stops at the front door.** ~7 base64 passes, 2 JSON serde passes and ≥4 full copies per record remain inside the server. |

The single most valuable change is to **carry decoded bytes (`bytes::Bytes`)
through the internal write path** so FSA1's saving is not paid back
inside the Node. At 1 KiB records the write rate is bound by per-range
quorum round cadence, not the codec (the FSA1 A/B in
performance-testing.md shows 24.0k JSON vs 23.7k binary), so the wins
are: CPU headroom (Nodes ran 61–83% CPU during 1 KiB writes), larger
payloads (8 KiB: +64% from FSA1 at the edge alone), a deeper request
budget, and freeing round time for the engine-side pipelining work that
actually lifts small-record throughput.

## 2. Measured baseline (this review)

Fixture: `compose.verify.yml` as project `whitewater-bench` (7771–7773,
subnet `10.250.245.0/24`), image built from `b349232`-equivalent source,
`WHITEWATER_RANGE_SYNC_INTERVAL_MS=100` (acks=all-equivalent), default
auto-split settings. Client: `whitewater-rust` `examples/bench_feed`
release build, run in `rust:1.90-bookworm` **on the bench network**
(`http://node{1,2,3}:7070`, no Docker Desktop port proxy). 8 Writers,
`append_batch(512)`, JSON writes, page limit 65,536. Docker Desktop on
Windows; runs were sequential, fixture torn down with `down -v` after.

| Shape | Write rec/s | Write p50 / p99 | Read format | Read rec/s (MiB/s) |
|---|---|---|---|---|
| 60k × 1 KiB, 1 feed | 17,063 / 15,692 | 226–231 / 292–310 ms | binary | 100,913 / 116,439 (99–114) |
| 60k × 1 KiB, 1 feed | 16,289 | 227 / 288 ms | **json** | 32,158 (31) |
| 60k × 1 KiB, 3 feeds | 21,185 / 21,398 | 157–167 / 285–337 ms | binary | 262,601 / 254,378 (248–256) |
| 60k × 1 KiB, 3 feeds | 21,983 | 150 / 283 ms | **json** | 58,312 (57) |
| 200k × 1 KiB, 3 feeds | 23,255 | 159 / 309 ms | binary | 248,676 (243) |
| 60k × 256 B, 3 feeds | 33,923 | 81 / 222 ms | binary | 771,077 (188) |
| 60k × 1 KiB, 3 feeds, `autotuned` (cap 512, 4 in-flight) | 21,046 | 985 / 1,591 ms | binary | 292,789 (286) |
| 30k × **4 KiB**, 3 feeds, batch 512 | **26 (28,672 errors)** | — | — | — |

Node CPU during the 200k write phase (`docker stats`): 61% / 80% / 83%.

What the numbers say:

* **Reads: bytes beat JSON 3.1x (1 feed) and 4.5x (3 feeds).** The gap
  widens with parallelism because the JSON path's per-record
  decode→base64→serde work is CPU on the serving Node, while the frames
  path ships stored bytes verbatim.
* **Writes are record-rate bound, not byte bound, at ≤1 KiB.** Shrinking
  records 4x (1 KiB → 256 B) only raised rec/s 1.6x and *cut* MiB/s
  2.6x — each record and each quorum round has a fixed cost. Bytes on the
  wire matter once payloads grow (see the 8 KiB FSA1 A/B in
  performance-testing.md: +64% rec/s).
* **4 KiB × 512 fails outright with HTTP 413.** A JSON batch is ~2.9 MB,
  over axum's 2 MiB default body limit on `/v1/writers/append-batch`
  (no explicit `DefaultBodyLimit` layer at `src/api.rs:711`). FSA1 would
  be ~2.1 MB — still over. The SDK pump then retries the 413 four times
  with 200 ms sleeps before failing every record (finding W6, S3).

## 3. Wire path today

### Write (SDK `append_batch` → majority commit)

```
SDK  Writer::append_batch                           whitewater-rust/src/writer.rs:94
 │   JSON: json! DOM + base64(key,payload)   ─or─   FSA1: encode_append_page (frame.rs:226)
 │   (JSON omits metadata + use_compression; FSA1 hard-codes none/false)
 ▼   POST /v1/writers/append-batch  (pinned ingress Node, 2 MiB body limit)
Ingress  writer_batch_append                        src/api.rs:15523
 │   FSA1 → decode_append_frame_page → STANDARD.encode(key,payload,metadata)   (api.rs:15548)
 │   writer_batch_prepare: decode key; clone base64 strings into ClientAppendRequest  (api.rs:15419)
 │   group by range → route_append_batch_to_owner   (api.rs:16795)
 ▼   if ingress ≠ owner: JSON POST /internal/active-range/owner/append-batch (api.rs:16830)
Owner  owner_append_batch_local                     src/api.rs:16593
 │   decode base64 key, payload, metadata          (api.rs:16704)
 │   encoder: record.clone() → encode_record → STANDARD.encode(frame)   (api.rs:16737-16740)
 ▼   MajorityAppendCoordinator::append_minted_batch (majority.rs:848)
 │   local append_batch → base64 decode per frame   (replication.rs:1855)
 │   dedup digest check → base64 decode again       (majority.rs:1077)
 │   follower_frames.clone() per send               (majority.rs:1161,1198)
 ▼   post_batch_bytes: base64 decode per follower   (majority.rs:1929)
Followers  /internal/active-range/replica/append-batch-bin  → append_batch_decoded (replication.rs:1894)
```

### Read (SDK `temporary_fetch_frames`)

```
SDK  Client::temporary_fetch_frames                 whitewater-rust/src/client.rs:746
 ▼   POST /v1/readers/temporary/fetch, Accept: application/vnd.whitewater.frames
Node temporary_reader_fetch                         src/api.rs:11280
 │   read_placement (Control Plane InspectPlacement)           api.rs:17482
 │   read_single_range / read_complete_feed: fetch_range_page in 2,048-frame steps
 │   (api.rs:17922, 18329; store clamp replication.rs:1398,1607) — all frames buffered
 │   read_placement again (fence)
 │   encode_frame_fetch_stream: memcpy every frame into 4 MiB chunks (api.rs:11250)
 ▼   Body::from_stream over the pre-built chunk Vec
SDK  request_binary → bytes().to_vec() (transport.rs:150) → parse_frame_page (frame.rs:174)
     per record: crc32, optional LZ4 into fresh Vec, key/payload .to_vec() (frame.rs:108,145)
```

## 4. Transcode ledger (per record, ingress ≠ owner, two followers)

| Step | JSON today | FSA1 today | Bytes end-to-end target |
|---|---|---|---|
| SDK encode | base64 ×2 + `Value` DOM + serde | FSA1 body Vec + copy into page + `body.to_vec()` | one `BytesMut` write, frozen to `Bytes` (0 extra copies) |
| Ingress parse | serde → base64 strings | FSA1 decode → **base64 encode ×(2+m)** | FSA1 decode to `Bytes` slices of the request body |
| Prepare | decode key; clone strings | same | slice reference, no copy |
| Owner forward | JSON serialize + parse | same | binary owner route (header + raw bytes) |
| Owner build | base64 decode ×(2+m); `record.clone()`; encode frame; **base64 encode** | same | encode frame once into `Bytes` |
| Local store | base64 decode | same | pass `Bytes` |
| Dedup check | base64 decode (dedup only) | same | digest over `Bytes` |
| Follower send ×2 | deep clone + base64 decode each | same | `Bytes` refcount clone |
| **Total base64 passes** | ~7 (1 in SDK) | ~7 (the SDK's encode moves to ingress) | **0** |
| **JSON serde passes** | 4 (SDK, ingress, forward ×2) | 2 (forward) | 0 on data plane |

(m = Metadata entries.) The response leg is JSON on every path: one
`WriterAppendResponse` with a `feedback` object per record.

## 5. Write-path findings

**W1 — FSA1 re-encodes to base64 at ingress (critical for the bytes
route).** `writer_batch_append` (`src/api.rs:15523-15563`) turns each
decoded `AppendFrameRecord` back into a `WriterSessionAppendRequest` with
`STANDARD.encode` on key, payload and every Metadata value. FSA1 therefore
saves the client-wire bytes and the SDK-side JSON, but every later step
still pays the base64 tax. *Fix*: introduce an internal
`AppendRecordBytes { request_id, writer, session_epoch, event_time_ns,
use_compression, key: Bytes, payload: Bytes, metadata: BTreeMap<String,
Bytes> }` and make `writer_batch_append_inner`, `writer_batch_prepare` and
`ClientAppendRequest` operate on it. The JSON handler decodes base64
**once** into the same type; the JSON-only per-record fallback
(`writer_session_append`) gets an adapter. `decode_append_frame_page`
should slice the request `Bytes` (`Bytes::slice`) instead of `to_vec()`.

**W2 — Internal frames are `String` base64 (`frame_base64`).**
`ReplicaAppendRequest` / `ReplicaBatchFrame`
(`src/active_range/replication.rs:39,168`) carry the stored frame as
base64. The owner encodes it (`api.rs:16740`) only for it to be decoded
again in three places: the local store (`replication.rs:1855`), the dedup
digest check (`majority.rs:1077`), and once per follower in
`post_batch_bytes` (`majority.rs:1929`). `follower_frames.clone()` also
deep-copies the strings for each follower send (`majority.rs:1161,1198`).
*Fix*: carry `frame: Bytes` in the in-process types; base64 survives only
as a serde adapter on the JSON internal routes for compatibility. The
binary replica route already proves the shape (`append_batch_decoded`,
`replication.rs:1894`). `encode_batch_body` should accept `&[Bytes]` so
the two follower sends share one encoded body.

**W3 — Owner forwarding is JSON.** When the pinned ingress is not the
range's Append Owner, `route_append_batch_to_owner` (`api.rs:16795`)
POSTs `InternalOwnerBatchAppendRequest` as JSON: two serde passes and
base64 payloads. With three Nodes and owners spread across ranges, most
batches take this hop. *Fix*: add
`/internal/active-range/owner/append-batch-bin` in the
`ReplicaBatchAppendHeader` style (a small JSON or packed header plus
length-prefixed `Bytes`), with the same auth and the same refusal mapping.

**W4 — Writer pins to the session-opening Node, not the owner.** Sticky
sessions fixed pacing (see performance-testing.md), but the pinned Node
is arbitrary relative to range ownership, so W3's hop is the common case.
*Fix*: when the whole batch routed to one remote owner, return an opaque
`preferred_endpoint` hint in Writer feedback, and have the SDK re-pin on
it. The hint is a Node URL, never a range or partition, and the ingress
still forwards correctly when the hint is stale. *Verify first*: add a
counter beside `replicated_frames` and count forwarded vs local owner
batches in a 3-feed run.

**W5 — Per-record clones in the owner encoder.** The minted-sequence
encoder closure calls `record.clone()` (a full key + payload + Metadata
copy) before `encode_record_with_compression` (`api.rs:16737`). *Fix*:
encode with the sequence patched in place. The frame layout is
fixed-width up to the key, so one option is to encode once with a
placeholder sequence and then patch that field and the CRC.

**W6 — 2 MiB public body limit caps the bytes route.**
`/v1/writers/append-batch` has no `DefaultBodyLimit` layer (`api.rs:711`),
so axum's 2 MiB default applies to both contracts. The internal replica
budget, by contrast, is `MAX_REPLICA_BATCH_BASE64_BYTES` = 16 MiB
(`replication.rs:154`). Measured: 4 KiB × 512 returns 413 on every batch.
*Fix*: set an explicit limit on this route aligned with the internal
budget (for example 16 MiB, or a policy bound on
`MAX_REPLICA_BATCH_FRAMES × MAX_FRAME_BYTES`). Publish the bound in
`/v1/health` capabilities so SDKs size batches by bytes, not by count.

**W7 — Quorum round cadence is the 1 KiB bound (engine work, not wire).**
At 1 KiB, batch 512 vs 1,000 and JSON vs FSA1 all land the same rate; one
range sustains about 20 rounds/s. The send-turn pipeline
(`majority.rs:1139`) already overlaps local work with network flight. The
next step is deeper in-flight rounds per follower (window > 1) with
ordered acknowledgement. W1–W3 shrink each round's CPU and serialization
time, so they are prerequisites that make this measurable, not
substitutes for it.

## 6. Read-path findings

**R1 — Bytes route only on temporary fetch.** `wants_frame_binary`
(`api.rs:11228`) has one caller. `Reader::fetch`, `Member::fetch` and
`/v1/feeds/records` still decode stored frames and re-encode them as JSON
and base64, which is the 3.1–4.5x gap measured above. *Fix*: negotiate
`Accept: application/vnd.whitewater.frames` on those routes. Put the
session fields (`delivered_cursor`, `recommended_capacity`,
`retry_after_ms`, pacing) in a small length-prefixed JSON or packed header
ahead of the frame section, so the format stays one parser.

**R2 — Frames are copied into chunks, and the "stream" is pre-built.**
`encode_frame_fetch_stream` (`api.rs:11250`) memcpys every frame into
4 MiB `Vec`s and collects all the chunks before it returns the iterator.
`read_complete_feed` has already buffered the whole page by then, so
nothing overlaps and peak memory is about twice the page size. *Fix*:
make `StoredRangeFrame.frame` a `Bytes` (`store.rs:107`; the span-buffer
read can `Bytes::slice` one buffer per span) and emit
`[len-prefix Bytes, frame Bytes]` chunks with no copy. Then encode each
`fetch_range_page` result as it arrives inside an `async_stream`, so the
first bytes leave while later range pages are still being read. The
fencing `read_placement` check moves to a trailer, or the stream aborts
on a placement change.

**R3 — Placement is read through the Control Plane twice per request.**
`read_placement` (`api.rs:17482`) executes `InspectPlacement` and parses
`serde_json::Value`, both before and after every read. *Fix*: read the
applied catalog (`active_range_map` + assignments) locally, fenced by the
assignment `generation` / `ownership_epoch` that `fetch_range_page`
already validates. Keep the post-read comparison, against the local
applied view.

**R4 — 2,048-frame inner clamp.** `read_single_range` (`api.rs:17922`),
`read_complete_feed` (`api.rs:18329`) and the store
(`replication.rs:1398,1607`) step 2,048 frames at a time. One
65,536-record page is therefore about 32 `fetch_range_page` calls, each
with its own lock and `spawn_blocking`. *Fix*: raise the clamp to
`MAX_LOGICAL_READ_FRAMES` and keep the existing 96 MiB
`MAX_COMMITTED_READ_BYTES` budget as the real bound.

**R5 — Small per-frame waste.** `logical_frame_bytes` runs twice per
frame (`api.rs:17954-17955`). The `wait_ms` loop re-reads the whole Feed
every 50 ms (`api.rs:11386`) instead of waking on a commit `watch`.

## 7. Rust SDK findings

**S1 — FSA1 path silently drops Metadata and compression.**
`append_batch_binary` hard-codes `use_compression: false` and
`metadata: &NO_METADATA` (`writer.rs:146,149`), ignoring
`Client::use_compression`. The JSON `append_batch` omits both as well;
only the single `append` sends them. *Fix*: take
`&[AppendItem { request_id, key, payload, event_time_ns, metadata }]`
(keep the tuple overload as a shim) and pass `self.client.use_compression`.
This is a conformance gap, so add a scenario to `sdk/spec/conformance`.

**S2 — Three copies to send one FSA1 batch.** `encode_append_page`
builds a per-record body `Vec` and then copies it into the page
(`frame.rs:226`). `request_binary_body` copies the page again with
`body.to_vec()` (`transport.rs:170`). *Fix*: encode straight into one
`BytesMut`: reserve 4 bytes, write the body, then back-patch the length
and the CRC (`crc32fast::Hasher` over the written slice), and `freeze()`.
Make `Transport::request_binary_body` take `bytes::Bytes`, which reqwest
sends without copying. Separately, the writer name repeats in every
frame. A page-level writer/epoch header would save those bytes, but it is
a format revision, so defer it.

**S3 — Pump retries non-retryable refusals.** `spawn_pump` retries any
error four times with 200 ms sleeps (`writer.rs:279-283`), including 413,
`StaleEpoch` and `Conflict`. *Fix*: retry only `Transport`, `Unavailable`
and `Ambiguous`, and surface everything else at once. On 413, split the
batch in half and resend; the `request_id`s make that safe.

**S4 — Possible same-Key reordering with `max_in_flight > 1` (correctness
hypothesis; verify before tuning).** The owner mints Writer sequences in
arrival order, and two in-flight `append-batch` requests from one session
can arrive in either order. Records for one Key split across those two
batches could then commit out of submission order. *Verify*: write
monotonically numbered records for a few hot Keys with `autotuned`
(4 in-flight) and assert per-Key order on read-back. If it reproduces,
limit in-flight concurrency per Key, either by partitioning the queue by
Key hash or by sequencing batches per session. Per-Key ordering is the
contract.

**S5 — Read decode copies.** `request_binary` returns
`bytes().to_vec()` (`transport.rs:150`), and `parse_frame_page` /
`decode_body` call `to_vec()` on every key and payload (`frame.rs:145`).
*Fix*: keep the body as `Bytes`, and give records `Bytes` fields sliced
from the page with no copy, either on `Record` or on a new `RecordBytes`
returned by a `temporary_fetch_frames_bytes` call. LZ4 frames still
decompress into an owned buffer; that copy is unavoidable. `Vec<u8>`
stays the stable public type, and the `Bytes` form sits alongside it.

**S6 — No prefetch, default HTTP client.** Readers fetch page k+1 only
after page k has been decoded and delivered. `HttpTransport` uses reqwest
defaults (`transport.rs:87`). *Fix*: add optional one-page-ahead
prefetch to `temporary_fetch_frames` streams and `FeedReader`. Set
`pool_max_idle_per_host` to at least Writers + Readers and
`tcp_nodelay(true)`, and benchmark `http2_prior_knowledge` for many
concurrent sessions per Node.

**S7 — Tail polling (carried over from the read review).**
`Feed::subscribe` spins at 0 ms, `FeedReader::for_each` at 1 ms, and
`FeedConsumer` sleeps a fixed 250 ms (`consumer.rs:45`). Once R5 provides
a commit `watch`, unify all three on server `wait_ms` long-polling.

## 8. Prioritized roadmap

| Pri | Item | Repo | Expected effect | Proof |
|---|---|---|---|---|
| **P0** | W1+W2: `Bytes` through ingress → owner → replicas; zero base64 on the internal data plane | whitewater | Removes ~7 base64 passes and at least 4 copies per record. Lower Node CPU at 1 KiB; larger rec/s gain at 4 KiB and up | `bench_feed` A/B at 1 KiB and 8 KiB with `BENCH_WRITE_FORMAT=binary`, plus `docker stats`. Unit: JSON and FSA1 inputs with the same `request_id` produce byte-identical stored frames, cursor and digest |
| **P0** | S1: FSA1 and JSON batches carry Metadata and compression | whitewater-rust | Correctness and parity; unblocks making binary the SDK default | Unit: `encode_append_page` round-trips against the server's `decode_append_frame_page` fixture bytes. Conformance scenario with Metadata and compression |
| **P0** | W6+S3: explicit append-batch body limit advertised in capabilities; split on 413; no retry of non-retryable errors | both | 4 KiB × 512 goes from 100% failure to working; no wasted 800 ms retry stalls | `bench_feed BENCH_BYTES=4096 BENCH_BATCH=512` with 0 errors. Unit: the pump splits on 413 and does not retry `StaleEpoch` |
| **P1** | R1: frames transport on `Reader::fetch`, `Member::fetch` and `/v1/feeds/records`, wired into the SDK | both | Named Readers and Subscriptions get the measured 3–4.5x | Bench a named-Reader drain, JSON vs binary. Conformance: same records and Cursors in both formats |
| **P1** | W3+W4: binary owner-forward route and owner endpoint hint | both | Removes JSON from the forward hop, and most forward hops altogether | Forwarded/local counter before and after. Fault test: a stale hint still commits |
| **P1** | R2+S2+S5: `Bytes` frames on read encode/decode and on SDK write encode | both | Lower peak memory, real send/read overlap, fewer copies | Read MiB/s at 1 feed in-network; RSS during a 200k drain |
| **P1** | R3+R4: local placement fence; drop the 2,048 clamp | whitewater | Fewer Control Plane round trips and store calls per page | Read rec/s at 1 feed. Existing placement-change acceptance stays green |
| **P2** | S4: verify and, if needed, fix per-Key order under the pump | whitewater-rust | Correctness before throughput | Ordered-Key property test against a live Riverbed |
| **P2** | W7: windowed quorum rounds per range | whitewater | The real lever for 1 KiB write rec/s | Rounds/s/range metric and the 1 KiB bench; fault suite |
| **P2** | S6+S7+R5: prefetch, client tuning, `watch`-backed long-poll | both | Tail latency and idle CPU | Soak bench (`bench-soak.py`) curve; idle CPU |

Sequencing: W1/W2 and S1 go first. They are pure internal refactors
behind unchanged contracts, and once they land the wire choice no longer
changes any stored byte. At that point `binary_appends` can become the
SDK default at no risk, with a JSON fallback when `/v1/health` does not
advertise the FSA1 capability.

## 9. Format notes for cross-SDK parity

* FSA1 append frames are big-endian, like the stored `FSR*` codec, but
  the fetch page envelope (`count` and the cursor lengths) is
  little-endian. Both layouts are fixed now and must stay that way, so
  document both byte layouts in `sdk/spec` for the
  Python/Java/C#/Node/Go SDKs to implement.
* Event and ingest times stay signed 64-bit nanoseconds in both binary
  formats, and Node.js decodes them as `bigint`.
* One media type, `application/vnd.whitewater.frames`, serves both
  directions. The request `Content-Type` selects the append contract, and
  the request `Accept` header selects the fetch contract.

## 10. Alignment with product principles

* **No partition leakage**: the owner hint is an opaque Node endpoint,
  ranges stay internal, and binary envelopes carry opaque Cursors.
* **Idempotency unchanged**: `request_id`s ride inside FSA1, and the
  owner still mints message identity, Writer sequence and ingest time.
  Dedup and digest fences see byte-identical frames whichever wire
  contract was used.
* **Fail closed**: every binary decoder keeps its length bounds and
  CRC32, and the store keeps blake3. Corruption is a typed error, never a
  partial batch.
* **Correctness before throughput**: S1 and S4 come before any default
  flip and before the windowed-round engine work.
* **Metadata, not Headers**, in every new type and field name.

## Reproduce

```bash
# fixture (isolated project; tear down with: docker compose -p whitewater-bench down -v)
COMPOSE_PROJECT_NAME=whitewater-bench WHITEWATER_RANGE_SYNC_INTERVAL_MS=100 \
  WHITEWATER_VERIFY_PORT_1=7771 WHITEWATER_VERIFY_PORT_2=7772 WHITEWATER_VERIFY_PORT_3=7773 \
  WHITEWATER_VERIFY_SUBNET=10.250.245.0/24 docker compose -f compose.verify.yml up -d --build
# client, in the whitewater-rust checkout (Git Bash: export MSYS_NO_PATHCONV=1)
docker run --rm -v "$PWD:/src" -w /src -e CARGO_TARGET_DIR=/src/target-linux \
  rust:1.90-bookworm cargo build --release --example bench_feed
docker run --rm --network whitewater-bench_default -v "$PWD:/src" -w /src \
  -e WHITEWATER_ENDPOINTS=http://node1:7070,http://node2:7070,http://node3:7070 \
  -e BENCH_RECORDS=60000 -e BENCH_WRITERS=8 -e BENCH_BATCH=512 -e BENCH_FEEDS=3 \
  -e BENCH_READ_FORMAT=binary -e BENCH_WRITE_FORMAT=json \
  rust:1.90-bookworm ./target-linux/release/examples/bench_feed
```
