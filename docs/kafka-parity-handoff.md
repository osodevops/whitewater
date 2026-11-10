# Kafka-parity handoff — status and next steps (2026-11-10)

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
