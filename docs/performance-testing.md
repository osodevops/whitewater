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

Numbers from a run are only reproducible with their parameters — always
report `BENCH_RECORDS`, `BENCH_BYTES`, `BENCH_WRITERS`, `BENCH_BATCH`,
`BENCH_PARTITIONS`, and host characteristics alongside results.

## Measured baseline

First-run numbers on a Windows dev machine (Docker Desktop, image cache
warm, `BENCH_BYTES=1024`). These are harness baselines — the honest gap
to the 100k-200k records/s task target — not tuning claims:

| System | Write records/s | Write p50 | Read records/s |
|---|---|---|---|
| Whitewater (bench-feed.py, 4 Writers, batch 50) | ~5-10 sustained* | ~37 ms per 50-record batch (healthy window) | ~10,700 |
| Whitewater via Java SDK (`BenchFeed`, same shape) | ~6 sustained* | ~26 s per batch (stall window) | ~16,100 |
| Kafka 3.8.0 (3 brokers, RF3, `acks=all`) | ~11,900 | ~931 ms | ~162,600 |

\* Sustained Whitewater writes are bounded by the Control Plane, not the
store: each record lands roughly one consensus-catalog write (~1 raft
entry per record observed), and under sustained load replicas
periodically report `durable on fewer than two current replicas` until
currency proofs catch up — multi-second stall windows dominate the
end-to-end rate. In healthy windows a 50-record batch commits in ~15-37ms
(~0.3-0.7 ms/record). Both sides acknowledge only quorum-committed
records; Kafka's client-side batching additionally amortizes round trips
in a way Whitewater's per-call batches do not. The comparison's point is
the contract being measured, not parity: closing the write-path gap is
exactly what the scaling work tracked against this benchmark must do.
