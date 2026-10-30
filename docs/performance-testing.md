# Ad-hoc performance testing

Performance drivers live next to the acceptance scripts but are **not**
part of `run-acceptance.py` — the `bench-*.py` prefix stays outside its
`test-*.py` glob, so they never gate CI. Run them on demand to measure a
Riverbed's throughput/latency and to compare Whitewater against Kafka on
the same workload.

## Drivers

| Driver | System under test | Fixture | Default ports |
|---|---|---|---|
| `scripts/bench-feed.py` | Whitewater (Python/HTTP protocol path) | `compose.verify.yml` as project `whitewater-bench` | 7771-7773 |
| `scripts/bench-kafka.py` | Kafka 3.8.0 (3 combined KRaft brokers) | `compose.kafka.yml` as project `whitewater-kafka-bench` | 19092-19094 |
| `whitewater-java/examples/BenchFeed.java` | Whitewater through the Java SDK | attaches to a live Riverbed | `WHITEWATER_ENDPOINTS` |

Every driver prints `BENCH_RESULT {json}` lines under one metric
contract: `system`, `mode` (`write`/`read`), `records`,
`bytes_per_record`, `records_per_sec`, `mib_per_sec`, and write-mode
latency percentiles (`p50_ms`/`p95_ms`/`p99_ms`/`max_ms`).

## Running

```powershell
# Whitewater — writer throughput + latency, then full-Feed replay
python scripts/bench-feed.py
BENCH_RECORDS=50000 BENCH_BYTES=4096 BENCH_WRITERS=8 BENCH_BATCH=100 python scripts/bench-feed.py
BENCH_MODE=read python scripts/bench-feed.py   # replay only (needs a seeded Feed)

# Whitewater — Kafka-acks=all-equivalent durability (quorum replication is
# the evidence; per-range fsyncs amortize to the interval) plus N
# pre-provisioned Active Ranges — the partition-count equivalent:
WHITEWATER_RANGE_SYNC_INTERVAL_MS=100 BENCH_RANGES=4 \
  BENCH_RECORDS=60000 BENCH_WRITERS=32 BENCH_BATCH=128 python scripts/bench-feed.py

# Kafka comparison — RF3 topic, stock kafka-producer/consumer-perf-test
python scripts/bench-kafka.py
BENCH_PARTITIONS=12 BENCH_PRODUCER_PROPS="acks=all linger.ms=5" python scripts/bench-kafka.py

# Java SDK against a running Riverbed (e.g. the bench fixture kept up
# with WW_KEEP_UP=1, or the development Riverbed on 7071-7073)
$env:WHITEWATER_ENDPOINTS = "http://localhost:7771,http://localhost:7772,http://localhost:7773"
cd whitewater-java
mvn -q -DskipTests package
mvn -q dependency:build-classpath -Dmdep.outputFile=target/cp.txt
javac -cp "target/classes;$(cat target/cp.txt)" -d target/examples examples/BenchFeed.java
java -cp "target/examples;target/classes;$(cat target/cp.txt)" BenchFeed
```

`WW_KEEP_UP=1` leaves a fixture running for repeated runs or the Java
driver; otherwise every driver tears its own project down with `down -v`.

## What is and is not comparable

* **Durability**: Whitewater appends always commit through the RF3
  quorum. The Kafka driver defaults to `acks=all` so both sides measure
  their strongest acknowledged-durability setting.
* **Batching**: Kafka batches inside the client (`linger.ms` +
  `batch.size`); Whitewater batches records per call on
  `/v1/writers/append-batch` (`BENCH_BATCH`). Neither is exactly the
  other's shape — tune both and state which was used.
* **Client path**: `bench-feed.py` exercises the HTTP wire protocol from
  Python; `BenchFeed` measures the Java SDK end-to-end (its codec and
  session management included); `bench-kafka.py` runs Kafka's own perf
  tools inside a broker container, which avoids client-side network
  overhead entirely.
* **Reads**: Whitewater replays via `temporary/fetch` pages
  (server-corroborated committed reads); Kafka replays via the
  consumer-perf tool. `mib_per_sec` is the honest cross-system number;
  per-record latency is only measured on writes.
* **Concurrency**: Whitewater writers are strictly ordered per session,
  so `BENCH_WRITERS` independent Writers provide concurrency — a single
  Key's ordering is never parallelized (per-Key ordering is the contract,
  and hot Keys can be isolated but not fanned out).
* **Sync mode**: `WHITEWATER_RANGE_SYNC_INTERVAL_MS=0` (default) keeps
  strict sync-before-ack; `>0` matches Kafka `acks=all` — acknowledged
  records live on a replica quorum's page cache and fsyncs amortize to
  the interval, so only a correlated multi-Node power loss can drop the
  acknowledged tail. Pending-effect markers, freezes, split staging, and
  epoch moves stay strict in both modes.
* **Range fan-out**: `BENCH_RANGES=N` pre-splits the Feed into N Active
  Ranges at even KeyToken boundaries before writing — the internal
  equivalent of Kafka's partition count. Ranges are independent quorum
  pipelines; per-range groups of one client batch commit concurrently,
  preserving per-Key order inside each range.

Numbers from a run are only reproducible with their parameters — always
report `BENCH_RECORDS`, `BENCH_BYTES`, `BENCH_WRITERS`, `BENCH_BATCH`,
`BENCH_PARTITIONS`, and host characteristics alongside results.

## Measured baseline

Numbers on a Windows dev machine (Docker Desktop, image cache warm,
`BENCH_BYTES=1024`, 60,000 records, 32 Writers). Whitewater runs the
fused-commit batch path — one quorum round carries a 128-record run with
its CommitPosition marker riding the same durability unit:

| System | Write records/s | Write p50 | Read records/s |
|---|---|---|---|
| Whitewater, strict sync, 1 range | 1,614 | 1,910 ms | 22,928 |
| Whitewater, `RANGE_SYNC_INTERVAL_MS=100`, 1 range | 6,414 | 543 ms | 19,355 |
| Whitewater, `RANGE_SYNC_INTERVAL_MS=100`, **4 ranges** | **11,621** | **355 ms** | 1,829 |
| Kafka 3.8.0 (3 brokers, RF3, `acks=all`, 12 partitions) | 11,007 | ~931 ms | ~150,000 |

Read on the multi-range Feed is the remaining gap: the temporary Reader
fans range pages serially, so per-range history (~15k records each)
turns into many small round trips — single-range replay stays ~20-23k.

History of the write-path work: the original per-record pipeline
saturated at ~5-10 records/s (each record paid a local fsync, a commit
fsync, and four HTTP round trips under the range lock). Batching the
quorum round lifted it to ~1,100; fusing the CommitPosition marker into
the replica batch removed the second network round (~1,614); the
deferred-sync mode removed the per-batch fsync chain that the 3-Node
fixture serialized on (~6,400/pipeline); and parallel per-range groups
multiplied pipelines to Kafka-parity aggregate. Per-pipeline,
Whitewater's ~6.4k/s is ~7x Kafka's ~917/s per partition at half the
latency.

**Auto-split caveat**: mid-run splits freeze a generation for cutover
(seconds under load) — pre-provisioning (`BENCH_RANGES`) measures
steady-state fan-out, while tuning auto-split under live load is
separate work.
