# Whitewater Pipelined Quorum Rounds & 3-Range Write Review (Argon High)

## 1. Context & Measured Baseline

Current same-session SDK-vs-SDK baseline (`docs/performance-testing.md`, 300,000 x 1 KiB records, 16 writers, 3 Nodes / 3 brokers, RF3 quorum durability, binary wire paths):

| Shape | Metric | Kafka Java SDK | Whitewater Rust SDK | Result | Factor |
|---|---|---|---|---|---|
| 1 partition / 1 range | write rec/s | 75.4k / 87.3k | 77.7k / 80.5k | TIE | 0.97x |
| | write p50 | 1,279-1,495 ms | 602-650 ms | **WIN** | 2.22x |
| | read rec/s | 341k / 369k | 405k / 424k | **WIN** | 1.17x |
| 3 partitions / 3 ranges | write rec/s | 108.5k / 110.2k | 91.0k / 96.9k | LOSE | 0.86x |
| | write p50 | 632-667 ms | 506-528 ms | **WIN** | 1.26x |
| | read rec/s | 315k / 330k | 397k / 412k | **WIN** | 1.26x |

This review stress-tests the pipelined quorum round implementation (`src/active_range/majority.rs`, `src/active_range/replication.rs`, `src/active_range/store.rs`, `src/api.rs`, `src/control.rs`, and `whitewater-rust/src/writer.rs`) across four dimensions:

1. **The invariant surface**: Writer sequence minting, commit ordering, and failed mid-pipeline rounds.
2. **`OrderedTurn` & follower admission interaction**: load-bearing vs. incidental serialization, and why 3-range write throughput is `0.86x` of Kafka.
3. **Fault interleavings**: range-owner loss, follower loss, pending-effect release/discard, single appends, and uncommitted tail truncation while rounds are in flight.
4. **Dedup contract**: `request_id` batch retries hitting a pipeline mid-flight or merging in `group_queues`.

---

## 2. Dimension 1 — Invariant Surface: Submission Order, Commit Order & Failed Mid-Pipeline Rounds

### Hole 1A: Segment Rotation Split-Brain Between Owner and Followers Under Pipelining (Critical)

- **Code locations**: `src/active_range/store.rs:1981-1994, 2849-2860`, `src/active_range/majority.rs:1311-1314, 1403-1426`.
- **What happens today**:
  1. Both `append_locked` and `rotate_if_needed` refuse to rotate the active segment unless `loaded.persisted.appended == loaded.persisted.committed` (`store.rs:1984, 2857`), because `truncate_uncommitted_locked` refuses `UncommittedSealedSegment` (`store.rs:2787-2795`).
  2. **Dead mid-batch rotation branch**: Inside any multi-frame batch (`staged > 0`), frame `0` advances `loaded.persisted.appended = position.value()` (`store.rs:2086`), whereas `loaded.persisted.committed` is only updated *after* the frame loop finishes (`store.rs:1660-1670`). Therefore, for every frame `index > 0` (`staged > 0`), `loaded.persisted.appended == loaded.persisted.committed` is **always false** — a batch can never rotate mid-batch.
  3. **Owner vs. Follower segment boundary divergence**:
     - On the **Owner**, `this.local.append_batch(envelope(local_frames, None))` runs with `commit_at = None`, and `_append_guard` is dropped at `majority.rs:1426` before the round's network flight and commit complete. Whenever `>= 1` earlier round is still in flight on the Owner when Round `N + 1` enters `local.append_batch`, the Owner has `appended > committed` (`appended != committed`) and **skips `rotate_if_needed`** on frame `0` of Round `N + 1`.
     - On **Followers**, each plain batch arrives with a fused `commit_at = Some(tail_position)` (`majority.rs:1403-1411`), and `ReplicaAppendService::append_frames_inner` holds `append_gate` across the entire `import_commit_batch` call (`replication.rs:2056-2103`). At the end of Round `N`, the Follower has `appended == committed`. When Round `N + 1` starts on the Follower, frame `0` (`staged == 0`) sees `appended == committed == true` and **rotates to `active_segment + 1`**, while the Owner kept Round `N + 1` in `active_segment`.
     - Under sustained pipelined load where at least one round is always in flight on the Owner, the Owner never rotates until the pipeline drains, while Followers rotate at every `max_segment_bytes` boundary — breaking the requirement that RF3 replicas rotate at identical positions and produce identical `SegmentManifest` digests.
- **Recommended fix**:
  1. Teach `truncate_uncommitted_locked` (`store.rs:2779-2847`) to unseal/remove uncommitted segment files above `committed` and roll `active_segment` back to the segment covering `committed`, or defer sealing/checkpointing rotated segments until `committed` crosses the segment's `last_position`.
  2. Drive segment rotation deterministically from segment byte length (`loaded.active_length + staged + encoded.len() > max_segment_bytes`) without gating on `loaded.persisted.appended == loaded.persisted.committed`, so both mid-batch rotation and pipelined Owner/Follower rotation occur at the exact same entry position on all replicas.
  3. **Unit test**: Pipeline two batches across `max_segment_bytes` on an Owner + 2 Followers while Round 1 is still in flight on the Owner; assert all three replicas produce identical `segment_count`, per-segment entry spans, and `SegmentManifest` digests.

### Hole 1B: Failed Round `N` Silently Committed by Round `N + 1` via Gap Repair + Cumulative Watermark (Critical)

- **Code locations**: `src/active_range/majority.rs:173-217, 1476-1513, 1625-1733`.
- **Concrete fault trace**:
  1. Follower `N3` is down/unreachable. Round `N` (`1..=100`) and Round `N + 1` (`101..=200`) both append locally on Owner `N1` (`N1.appended = 200, N1.committed = 0`).
  2. Round `N`'s send to Follower `N2` experiences a transient transport drop, while Round `N + 1`'s send reaches `N2`.
  3. `N2` waits `REPLICA_ORDER_WAIT` (500 ms) in `wait_for_appended(101)` and returns `PositionGap(expected: 1)`.
  4. Round `N + 1` catches `PositionGap` and calls `this.fill_replica_gap(assignment, &requests[0], &N2, Some(1), last_position = 200)` (`majority.rs:1499-1510`), copying **Round `N`'s uncommitted frames (`1..=100`)** from `N1`'s store to `N2`, then re-sending Round `N + 1` (`101..=200`) with fused `commit_at = 200`. `N2` accepts `1..=200` and advances `N2.committed = 200`.
  5. Meanwhile, Round `N` saw errors from both `N2` and `N3`. Round `N` calls `this.local.note_owner_quorum_blocked(range_id)` (`majority.rs:1626`), returns `Err(NoDurableMajority)` to Round `N`'s callers, and drops its `commit_permit(ticket = 0)`.
  6. Because `TurnPermit::drop` unconditionally advances `ready`, Round `N + 1` wakes at `commit_permit.wait().await` (`majority.rs:1659`), calls `this.local.commit(commit_position = 200)` on `N1`, and returns `Ok` for `101..=200`.
  7. **Consequences**:
     - Because `committed` is a cumulative prefix watermark (`200`), Round `N + 1` silently ratifies Round `N`'s frames (`1..=100`) as committed **after** Round `N` reported `Err(NoDurableMajority)` to its callers.
     - Because `note_owner_quorum_blocked` (`majority.rs:1626`) and `note_owner_quorum_restored` (`majority.rs:1635`) execute *before* `commit_permit.wait().await`, Round `N` can run line 1626 *after* Round `N + 1` ran line 1635, leaving `quorum_blocked_ranges` falsely latched on a healthy range!
- **Recommended fix**:
  1. Move `note_owner_quorum_blocked` and `note_owner_quorum_restored` inside the `commit_permit`-ordered critical section (or gate `note_owner_quorum_blocked` on `last_position > current_committed`) so an earlier round's failure can never clobber a later round's quorum restoration.
  2. When Round `N + 1` repairs a gap covering Round `N` and commits position `P_{N+1} > P_N`, or when Round `N` fails quorum, ensure fate consistency: either abort Round `N + 1` if its predecessor round failed quorum, or share the cumulative follower durable/commit watermark so Round `N` observes that a follower has already durably accepted `>= P_N` before returning `NoDurableMajority`.

---

## 3. Dimension 2 — `OrderedTurn` Interaction & 3-Range Write Bottlenecks

### Hole 2A: `send_permit` Is Dropped After `tokio::spawn`, Before Socket Checkout (High)

- **Code location**: `src/active_range/majority.rs:1476-1485`.
- **What happens today**:
  - `send_to` spawns `async move { transport.append_batch(&node, request).await }` onto the Tokio runtime and immediately calls `drop(turn_permit)`.
  - Under Tokio's multi-threaded work-stealing scheduler, task `N + 1` can be polled and check out an HTTP connection from `reqwest`'s pool before task `N` is first polled.
  - Consequently, `send_turns` on the Owner does not guarantee wire arrival order; wire ordering relies on the follower's `wait_for_appended` (`order_watermark`) in `src/active_range/replication.rs:837-851`.

### Hole 2B: Follower `append_gate` Serializes Lock-Free `blake3` Precomputation Across Pipelined Batches (Performance)

- **Code locations**: `src/active_range/replication.rs:2056-2099` vs. `src/active_range/store.rs:1489-1546`.
- **What happens today**:
  - `store::append_batch` computes `peek_record_header` + `blake3::hash` in parallel *before* acquiring `inner.loaded.lock()` (`store.rs:1493-1545`) so CPU hashing of batch `N + 1` overlaps disk write of batch `N`.
  - However, `ReplicaAppendService::append_frames_inner` acquires `self.append_gate(range_id)` at `replication.rs:2056` *before* calling `store.import_commit_batch`.
  - Because `append_gate` is held for the entire `append_frames_inner` call, pipelined batches arriving at a follower cannot overlap their lock-free `blake3` + header precomputation with the preceding batch's disk write.
- **Recommended fix**:
  - Precompute `(RecordHeader, frame_digest)` before `wait_for_appended` and `append_gate` on the follower (or acquire `append_gate` only after precomputation once `wait_for_appended` resolves).

### Hole 2C: 3-Range Split Owner Balancing & Ingress-to-Owner Forwarding Hop (Performance — Explains the 0.86x 3-Range Gap)

- **Code locations**: `src/control.rs:6720-6750`, `src/api.rs:17204-17275`, `whitewater-rust/src/writer.rs:24-26`.
- **What happens today**:
  1. **Split owner concentration**: When splitting a Feed into 3 ranges, picking the first non-source-owner replica in `placement.replicas` can assign 2 of the 3 range owners to the same Node instead of spreading ownership 1-per-Node across the 3 Nodes. Preferring `min_by_key(|node| feed_owner_load(node))` (`src/control.rs:6726-6743`) balances range leaders across Nodes like Kafka partition leaders.
  2. **Ingress forwarding hop**: Each SDK `Writer` pins to a single ingress Node (`pinned` in `whitewater-rust/src/writer.rs`). On a 3-range Feed with 1 owner per Node, 2/3 of every batch sent to the pinned ingress Node must be serialized and forwarded over an extra internal HTTP hop (`/internal/active-range/owner/append-batch-bin`) via `route_append_batch_to_owner` before entering `group_queues`. Kafka's producer routes per-partition batches directly to each partition's leader broker.

---

## 4. Dimension 3 — Fault Interleavings: Pending Effects, Single Appends & Truncation

### Hole 3A: `discard_pending_effect` Truncates In-Flight Plain Rounds on the Owner (Critical)

- **Code locations**: `src/active_range/majority.rs:1994-2221`, `src/active_range/store.rs:1154-1183`.
- **Concrete fault trace**:
  1. Plain Round `R1` (`1..=100`) appends locally on Owner `N1` (`N1.appended = 100, N1.committed = 0`), drops `_append_guard` (`majority.rs:1426`), and sends `1..=100` (`commit_at = 100`) to Followers `N2` and `N3` (which both commit `1..=100`).
  2. Before `R1` executes `this.local.commit(100)` on `N1`, a pending-effect append `P` (`101..=105`) acquires `_append_guard`, appends `101..=105`, replicates to followers, and then aborts via `discard_pending_effect`.
  3. `discard_pending_effect` acquires only `append_lock` (`majority.rs:2131`) — it does **not** drain `in_flight_window`.
  4. In `store.rs:1177`, `discard_pending_effect` calls `truncate_uncommitted(inner, ...)`, which truncates **everything above `loaded.persisted.committed` (`0`)** — wiping `R1`'s frames (`1..=100`) off `N1` even though `N2` and `N3` already committed `1..=100`!
- **Recommended fix**:
  - Drain `in_flight_window(range_id).acquire_many_owned(MAX_INFLIGHT_ROUNDS as u32)` before acquiring `append_lock` in `commit_pending_effect`, `discard_pending_effect`, and whenever a batch or single append carries `pending_effect.is_some()` (matching `freeze_for_cutover` at `majority.rs:486-500`).

### Hole 3B: Single-Append Path (`append_inner`) Bypasses `in_flight_window`, `send_turn`, `send_lane`, `repair_lock`, and `commit_turn` (High)

- **Code location**: `src/active_range/majority.rs:570-938`.
- **What happens today**:
  - `append_inner` (used by `/v1/writers/append` and the sequence-healing fallback in `writer_batch_append_inner` at `src/api.rs:15937-15949`) only acquires `append_lock`.
  - Once a pipelined batch round drops `_append_guard` at `majority.rs:1426`, a concurrent `append_inner` call can mint position `101`, send to followers ahead of Batch `1..=100` (triggering `PositionGap` on followers because `import == false` skips `wait_for_appended`), run `fill_replica_gap` without `repair_lock`, and call `self.local.commit(101)` without waiting for `commit_turn` — advancing `committed` past Batch `1..=100` before Batch `1..=100` finishes checking follower responses for `CommittedPositionConflict` divergence.
- **Recommended fix**:
  - Acquire `in_flight_window(request.range_id).acquire_many_owned(MAX_INFLIGHT_ROUNDS as u32)` at the entry of `append_inner` before `append_lock` so single appends and pending-effect appends never interleave with in-flight pipelined batch rounds.

### Hole 3C: `order_watermarks` Is Monotonic and Never Reset on Uncommitted Tail Truncation (High)

- **Code locations**: `src/active_range/replication.rs:825-851, 1039-1048, 1083-1147, 1208-1252, 1337-1363`.
- **What happens today**:
  - `note_appended` (`replication.rs:825-831`) updates `*watermark = (*watermark).max(position.value())`.
  - None of `truncate_uncommitted_for_assignment`, `reconcile_recovery_for_assignment`, `discard_pending`, or `quarantine_assignment_for_repair` resets `order_watermarks` to the truncated `appended`/`committed` position.
  - After any uncommitted tail truncation on a replica (e.g., from `200` back to `100`), `order_watermarks` remains at `200`. Subsequent out-of-order pipelined batches (`expected_position = 151` arriving before `101..=150`) see `*receiver.borrow() >= 150` (`200 >= 150`) and return immediately from `wait_for_appended` instead of waiting for `101..=150`, failing with avoidable `PositionGap` errors.
- **Recommended fix**:
  - Add `set_order_watermark(&self, range_id: RangeId, position: u64)` (setting `*watermark = position`) and call it whenever a store truncates uncommitted frames, reconciles recovery, discards a pending tail, or quarantines for repair.

---

## 5. Dimension 4 — Dedup Contract: `request_id` Retries Hitting a Pipeline Mid-Flight

### Hole 4A: Merging a Deduplicated Retry Batch with a Fresh Batch in `group_queues` Breaks Follower Contiguity and `commit_hint` (Critical)

- **Code locations**: `src/active_range/majority.rs:1115-1144, 1341-1411`, `src/active_range/store.rs:1646-1658`.
- **Concrete fault trace**:
  1. Batch A (`1..=100`) and Batch B (`101..=200`) land locally on Owner `N1`.
  2. Client A times out and retries Batch A with identical `request_id`s. In `group_queues`, Retry-A is merged with fresh Batch C.
  3. In `store::append_batch`, Retry-A's records deduplicate via `cursor_positions` to `position = 1..=100`, while Batch C's fresh records append at `position = 201..=300`.
  4. The merged `follower_frames` now has `expected_position` jumping from `100` to `201` (non-contiguous).
  5. Worse, if Retry-A sits *after* Batch C in the merged vector (`[Batch C (201..=300), Retry-A (1..=100)]`), `local_results.last()` has `position = 100`. `commit_hint` (`majority.rs:1406`) is then set to `CommitPosition(100)` instead of `300`, leaving Batch C (`201..=300`) uncommitted on followers or failing `fused commit does not match the batch tail` (`store.rs:1657`)!
- **Recommended fix**:
  1. Compute `last_position` and `commit_hint` from the frame with the **maximum** `position` in `local_results` (`local_results.iter().max_by_key(|r| r.position)`), not `local_results.last()`.
  2. Sort and deduplicate `follower_frames` by `expected_position` (or omit already-committed deduplicated frames below the contiguous new tail when pushing to followers, only pushing frames at or above the contiguous run start) so `follower_frames.last()` always matches `commit_hint` and positions stay strictly increasing.

### Hole 4B: Sequence Inflation and Serial Disk Reloads on Cursor-Deduped Batch Retries (Medium)

- **Code locations**: `src/active_range/majority.rs:1238-1260, 1355-1388`, `src/active_range/store.rs:1845-1899`.
- **What happens today**:
  - `append_batch_inner` mints a fresh `identity.sequence` (`S + N`) for every record *before* checking whether `request.cursor` is already present in the store.
  - On a retried batch, the newly patched sequence changes `frame_digest`, missing `digest_positions` and hitting `cursor_positions` (`store.rs:1845`), which reads each existing frame off disk, advances `loaded.deduplication` to `S + N`, and triggers up to 1,000 sequential `read_assignment_uncommitted` calls in `majority.rs:1369`.
- **Recommended fix**:
  - Check existing cursor positions (or expose a batch cursor lookup on the local store) before minting new Writer sequences so already-landed `request_id`s reuse their original sequence without inflating the Writer high-water mark or re-reading frames one by one.

---

## 6. Model Assignment & Remaining Project Roadmap

| Phase | Model | Scope |
|---|---|---|
| **Phase 1 (Immediate)** | **Gemini 4 Argon High** | Implement and unit-test **Holes 1A, 1B, 2B, 2C, 3A, 3B, 3C, 4A, 4B** (`store.rs`, `replication.rs`, `majority.rs`, `control.rs`). |
| **Phase 2** | **Gemini 4 Argon High** | **P0 Named `Reader::fetch` / `ack` Raft decoupling** (self-describing `delivered_cursor`, `FjallReaderProgressStore` RF3 progress, piggybacked `ack_cursor`) + **P1 local `read_placement` fencing**. |
| **Phase 3** | **SWE-2 MAX** | Run the 1-range and 3-range Docker benchmark matrix (`bench_feed` vs. `bench-kafka-sdk.py`), update `docs/performance-testing.md`, and run the full sequential `scripts/test-*.py` acceptance suite. |
| **Phase 4** | **SWE-2 MAX** | **P0/P1/P2 Read & Write Plumbings**: `Accept: application/vnd.whitewater.frames` on `/v1/readers/fetch`, `/v1/subscriptions/members/fetch`, `/v1/feeds/records`, `/internal/active-range/read/committed`; raise 2,048-frame inner clamp to `65,536`; propagate `(ingest_time_ns, message_id)` in `StoredRangeFrame`; `watch`-backed `wait_ms` long-polling; `ReaderPacingTable` cross-Node reconciliation. |
| **Phase 5** | **SWE-2 MAX** | **SDK Rollout & Auto-Tune Remainder**: per-Writer slider API, learned load curves, virtual clock tests, typed `FeedWriter`/`FeedReader` adoption, and porting binary frames + adaptive batching across Python, Java, C#, Node.js/TypeScript, and Go (`sdk/spec/conformance`). |
| **Phase 6** | **Argon High (design) -> SWE-2 MAX (impl)** | Long-term tracked items: **k8s correlation**, **controlled execution** (proposals/approval/ledger/kill-switch), and **baseline remainder** (weekly recurrence, drift, human feedback, FP/injection fixtures). |
