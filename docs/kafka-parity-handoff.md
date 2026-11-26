# Kafka-parity handoff — status and next steps (2026-11-10)

> **2026-11-23 update:** re-measured same-session after the owner-spread commit
> landed — **the 3-range write loss is closed** (77.2k vs Kafka 76.1k, 1.01x) and
> every cell is now a Whitewater win (writes 1.13x/1.01x, write p50 ~1.3–2.3x,
> reads 1.46x/1.66x). Fresh table in `performance-testing.md` "Same-session
> result — 2026-11-23". The remaining next steps below are optimizations toward
> the 100k–200k sustained-write target, not parity gaps.

**Where it stands** (pushed: whitewater `78cdaaa`, whitewater-rust `43bb04c`; see
the 2026-11-03 same-session table in `performance-testing.md`): grand average **WIN 1.29x** vs the Kafka
Java SDK. Wins: write p50 (both shapes), reads (1 and 3 ranges). Tie: 1-range
write rec/s. **Only remaining loss: 3-range write rec/s, 0.86x** (WW 91–97k vs
Kafka 108–110k, 300k × 1 KiB, 16 writers, no fsync on either side).

**Uncommitted in the working tree (needs the full gate before commit):**
- `src/control.rs` split owner selection: the right-hand child now goes to the
  eligible replica owning the fewest of the Feed's ranges (ties keep consensus
  draw order). Before, 3 ranges landed on 2 owners (one Node owned none). Verified
  live (owners spread 1/1/1); throughput effect within noise (80–90k). No unit
  test yet — add one asserting two successive splits yield three distinct owners.
- `src/active_range/majority.rs` shows only a line-ending diff; discard it.

**What the 3-range write profiling ruled out** (all on the bench Riverbed):

| Suspect | Evidence | Verdict |
|---|---|---|
| Disk | tmpfs volumes: 93–105k | not the limit |
| Client in-flight depth | `BENCH_IN_FLIGHT` 2/5/8/12 → ≈90k each | not the limit |
| Replica send window | `MAX_INFLIGHT_SENDS_PER_FOLLOWER` 4→16: no change (reverted) | not the limit |
| CPU | client ~2.5–3 cores, each Node ~2 of 16 | not saturated |
| Per-range commit rounds | owner phase timing: each range busy ~30–50% | not saturated |
| Ingress forwarding alone | client pinned to one Node: 67–74k on each | inconclusive |
| Owner placement | fixed (above) | no throughput change |

Note: `bench_feed` caps each writer at 4,096 outstanding records, so client p50
mostly reflects client-side queueing, not server latency.

**Leading hypothesis (unconfirmed): bytes on the Docker network per record.**
Whitewater moves ~4.9 KB per 1 KiB record (client→ingress ~1.3 KB, ingress→owner
~0.9 KB for 2 of 3 requests, owner→2 replicas ~2.7 KB at ~1,330 B per frame;
measured `body_bytes` ≈ 266 MB for 200k replica frames) vs Kafka's ~3 KB.

**Next steps, in order:**
1. Confirm the hypothesis: measure container network throughput (`docker stats`
   NET I/O) during a 900k-record run on both systems.
2. Leaner replica frames (server only): drop or derive per-frame `cursor` and
   `identity` strings in `ReplicaBatchAppendHeader` (`majority.rs`
   `post_batch_bytes`, `replication.rs` `append_batch_decoded`), versioned so
   mixed-version replicas still interoperate. Target ~0.5 KB/record saved.
3. Only if a gap remains: owner-direct writes inside the SDK (no API change) —
   fetch range owners, refresh on ownership change, keep forwarding as the
   fallback. Every SDK would eventually need it.
4. Then: full gate (cargo fmt/clippy/test both repos + 34 acceptance scripts),
   same-session Kafka rerun, update the table in `performance-testing.md`, commit.

**How to reproduce a run** (Docker Desktop; leave `ks-site-test` alone):

```bash
# Whitewater bench Riverbed (disposable; down -v between server builds)
export COMPOSE_PROJECT_NAME=whitewater-bench WHITEWATER_VERIFY_PORT_1=7771 \
  WHITEWATER_VERIFY_PORT_2=7772 WHITEWATER_VERIFY_PORT_3=7773 \
  WHITEWATER_VERIFY_SUBNET=10.250.245.0/24 WHITEWATER_RANGE_SYNC_INTERVAL_MS=never
docker compose -f compose.verify.yml down -v && docker compose -f compose.verify.yml up -d --build

# Rust SDK bench (build target-linux/release/examples/bench_feed first and
# CHECK ITS TIMESTAMP is newer than whitewater-rust/src — a stale binary
# invalidated a week of results)
MSYS_NO_PATHCONV=1 docker run --rm --network whitewater-bench_default \
  -v "C:/Code/Stackapps/whitewater-rust:/src" -w /src \
  -e WHITEWATER_ENDPOINTS=http://node1:7070,http://node2:7070,http://node3:7070 \
  -e BENCH_RECORDS=300000 -e BENCH_WRITERS=16 -e BENCH_BATCH=512 -e BENCH_FEEDS=1 \
  -e BENCH_BATCHED=1 -e BENCH_WRITE_FORMAT=binary -e BENCH_PAGE_LIMIT=16384 \
  -e BENCH_RANGES=3 rust:1.90-bookworm ./target-linux/release/examples/bench_feed

# Kafka (one system at a time; fresh topic per run)
WW_KEEP_UP=1 BENCH_RECORDS=300000 BENCH_WRITERS=16 BENCH_BYTES=1024 \
  BENCH_PARTITIONS=3 BENCH_TOPIC=bench-<unique> python scripts/bench-kafka-sdk.py
COMPOSE_PROJECT_NAME=whitewater-kafka-bench docker compose -f compose.kafka.yml down -v
```

Profiling: start the Riverbed with
`RUST_LOG="whitewater=info,whitewater::active_range=debug,whitewater::api=debug"`
and parse the JSON log lines `append batch phase timing` (owner round phases),
`replica batch handler timing` / `replica append inner timing` (follower) and
`append-batch done` (ingress end-to-end). Debug logging itself costs ~20–30%
throughput, so never record benchmark numbers with it on. `BENCH_BATCH` above
512 is refused (`record field is too large`).

## 2026-11-26 — write-path ceiling investigation: the server already sustains ~166k

Fresh `bench_feed` binary verified (Nov 26, target-linux release). All runs write-only
(`BENCH_MODE=write`), 1 KiB records, batch 512, binary appends, sync=never, 16 cores
visible inside the Docker VM.

### Measurements (single client process unless noted)

| topology | rec/s | notes |
|---|---|---|
| 1 feed × 1 range, 16 writers | 56,261 | no fragmentation, full 512-rounds |
| 1 feed × 3 ranges, 16 writers | 74,071 | batches fragment ~170/round |
| 1 feed × 9 ranges, 16 writers | 49,612 | fragmentation destroys amortization (~57/round) |
| 3 feeds × 1 range, 15 writers | 65,429 | 3 independent pipelines ≈ 21.8k each |
| 4 KiB records, 1 feed × 3 ranges, batch 128 | 27,002 (105 MiB/s!) | byte throughput UP 46% vs 1 KiB — neither pure wire- nor round-bound |
| 32 writers (1 feed × 3 ranges) | 59,716 | MORE writers = slower: server convoy, not client cap |
| BENCH_IN_FLIGHT=20 (vs default 5) | 63,938 | deeper pump pipeline does NOT help |
| **3 parallel client containers, 3 feeds × 1 range each (9 independent pipelines)** | **75.5k + 46.8k + 45.7k ≈ 166k aggregate** | errors=0 everywhere |

### What this means

1. **The server already sustains ~166k rec/s aggregate** when driven by enough independent
   pipelines — inside the 100–200k target. The long-standing ~74k "ceiling" is an artifact
   of driving the system from one client process with few writers over fragmented ranges.
2. **Per-pipeline throughput ≈ 18–25k rec/s** (single-range, fragment-free). Total system
   throughput scales ~linearly with independent pipeline count (at least to 9).
3. **CPU is NOT the cap**: nodes peak at ~1.5–2.4 cores each of 16 available. Network is
   not obviously the cap either (~250–280 MB/s/node rx at peak). The system is
   **latency/convoy-bound**: serialized quorum rounds per range + queueing coupling.
4. **Fragmentation is the enemy**: batches split per-range by key routing
   (`writer_batch_append_inner` groups by range). More ranges per feed = smaller rounds =
   worse throughput. Kafka parity was won with the client effectively doing the same;
   Kafka producers batch per-partition client-side, WW batches pre-routing.
5. Wire amplification measured end-to-end: **8.6× payload** counting rx+tx on all nodes
   (intra-cluster hops double-counted; structural estimate ≈4.2×: client→ingress ~1.1 KB,
   ingress→owner relay ~0.73 KB avg, owner→2 replicas ~1.17 KB each per 1 KiB record).
   Important for byte-cost reduction but NOT the current binding constraint.
6. Bytes/record wire decomposition per replica frame ≈ 1169 B: 1024 payload + key +
   ~80 stored-codec overhead (magic, 2 uuids, 3 u64s, lens, crc) + ~46 batch-wire
   (41 of which is the per-frame cursor string — derivable from (stream, position)
   since `encode_cursor` is a pure function; elidable but only ~2% of total wire — LOW priority).
7. 4 KiB record test: rec/s fell 63% but MiB/s rose 46% — both per-record fixed cost
   AND per-byte cost matter. Model fit: fixed ≈5.7 µs/record + ≈7.7 ns/byte through the pipeline.

### Server round-phase instrumentation (already in tree, DEBUG level)

`writer_batch_append` elapsed, `batch-trace` per-range chunks, `owner batch forward`,
`append batch phase timing` (lock_wait/mint_store/send_wait/send/commit per round),
`replica batch handler timing` / `replica append inner timing` (decode/service/gate/store).
Debug logging costs 20–30% throughput — read relative structure only. At saturation the
convoy inflates everything: rounds show 640+ ms lock_wait, fan-out send_wait dominates,
replica store is 5–15 ms per ~2–4k frames (≈7–8 µs/frame serialized per range-store).

### Next levers, re-ranked by evidence

1. **Pipelined quorum rounds per range** (the original handoff lever, still the big one):
   one round in flight per range today; round latency ~7 ms even unfragmented.
   Per-pipeline 20k × more pipelines hits a convoy wall — pipelining raises every pipeline's cap.
   Consensus-critical: positions must stay ordered/contiguous per range; speculative position
   assignment with in-order commit is the design problem.
2. **Range-affine writes / client-side per-range batching**: kill fragmentation. Either the SDK
   learns the key→range table and forms per-range batches (Kafka-producer-style), or the ingress
   re-batches per range over a linger window. Big win for multi-range feeds specifically.
3. **Owner-direct writes** (SDK resolves owner, skips relay hop): ~17% wire + one full
   decode/forward CPU pass removed per record. Matters more once rounds pipeline.
4. Cursor elision: real but ~2% — do it opportunistically with other wire work.
5. Benchmark methodology for validating the target: N parallel client containers × independent
   single-range feeds. The 166k data point should be reproduced cleanly (synchronized start,
   longer window, read-back verification) before/after each lever lands.

### Open questions

- Why exactly does per-writer throughput cap at ~5–10k regardless of in-flight depth?
  In-order per-writer sequence completion interacting with the range round serialization
  is the suspect — verify when implementing pipelining.
- Does the 9-pipeline aggregate keep scaling past 166k (12+ pipelines) or is there a
  per-node serialization ceiling? Unanswered — cheap follow-up experiment.
- Docker Desktop WSL2 bridge bandwidth ceiling unmeasured — irrelevant until CPU/convoy fixed.
