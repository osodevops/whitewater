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
| `scripts/bench-kafka.py` | Kafka 3.8.0 (3 combined KRaft brokers, perf tools) | `compose.kafka.yml` as project `whitewater-kafka-bench` | 19092-19094 |
| `scripts/bench-kafka-sdk.py` | Kafka 3.8.0 through the Java SDK (`benchmarks/kafka-sdk`, plain `kafka-clients` — Spring delegates to it) | same `whitewater-kafka-bench` project; client runs in its own container on the bench network | 19092-19094 |
| `whitewater-java/examples/BenchFeed.java` | Whitewater through the Java SDK | attaches to a live Riverbed | `WHITEWATER_ENDPOINTS` |
| `whitewater-rust/examples/bench_feed.rs` | Whitewater through the Rust SDK (JSON writes; `application/vnd.whitewater.frames` binary reads) | attaches to a live Riverbed | `WHITEWATER_ENDPOINTS` |

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

# Parallel read/write shapes — N independent Feeds and N reader threads
# (the N-partition/N-consumer-thread equivalent; ranges stay internal):
BENCH_FEEDS=3 BENCH_READ_FORMAT=binary BENCH_PAGE_LIMIT=65536   BENCH_RECORDS=60000 BENCH_WRITERS=32 BENCH_BATCH=512 python scripts/bench-feed.py
# BENCH_READ_FORMAT=binary selects the raw-frames fetch transport;
# BENCH_PAGE_LIMIT sizes one streamed page. Kafka's matching knob is
# BENCH_READ_THREADS for kafka-consumer-perf-test.

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
  the interval (`WHITEWATER_RANGE_SYNC_INTERVAL_MS=never` disables
  periodic fsync entirely — RF3 quorum + OS writeback, matching
  Kafka's acks=all default) while strict/forced syncs still run;
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

## Mixed-workload soak (`bench-soak.py`)

The per-mode benches measure write or read in isolation; production load
is simultaneous, so `scripts/bench-soak.py` runs writers AND
tail-following readers concurrently for `BENCH_SOAK_SECONDS` (default
120) and reports the throughput **curve** — per-`BENCH_SOAK_INTERVAL`
rates in the `soak` row's `curve` field — not just the average. A system
that holds its peak for 60s and collapses at minute 3 fails this bench
where the fixed-size passes miss it entirely.

```bash
python scripts/bench-soak.py
BENCH_SOAK_SECONDS=300 BENCH_WRITERS=8 BENCH_READERS=4 python scripts/bench-soak.py
```

Knobs: `BENCH_SOAK_SECONDS`, `BENCH_SOAK_INTERVAL` (default 5s),
`BENCH_BYTES`, `BENCH_WRITERS`, `BENCH_BATCH`, `BENCH_READERS`,
`BENCH_FEEDS`, `BENCH_PAGE_LIMIT`, `BENCH_READ_WAIT_MS` (consumer
long-poll), `BENCH_BASE_PORT`, `WW_KEEP_UP`. Readers use the binary
frame transport and poll the tail exactly like attached consumers, so
read throughput tracks write throughput once the tail is caught — the
signal is whether the write rate stays flat and errors stay zero as
replication, segment writes, and read serving contend for the same
disk and NIC.

Smoke run (30s window, 8 writers × 256-record batches, 4 feeds, 4
readers, Docker Desktop): **~12.1–12.5k records/s written and read,
flat across every 5s interval, zero errors** — readers kept exact pace
with writers (a consumer cannot read faster than the producer). The
`soak-write`/`soak-read` rows' averages understate the in-window rate
because their `seconds` includes the post-deadline drain grace; the
`curve` array is the figure of record.

## Measured baseline

### Matched-config comparison (isolated runs)

Kafka ran **alone** in Docker, was removed with `down -v`, then
Whitewater ran **alone** — only one system ever occupied the Docker
host. Same workload both sides: 60,000 records × 1,024 bytes, 3
brokers/3 Nodes, RF3, matched fan-out, matched producer semantics.

| Config knob | Kafka 3.8.0 | Whitewater |
|---|---|---|
| Acknowledged durability | `acks=all` (ISR quorum, no per-record fsync) | `WHITEWATER_RANGE_SYNC_INTERVAL_MS=100` (quorum replication is the evidence; fsyncs amortize per interval) |
| Idempotent writes | `enable.idempotence=true` (per-partition sequence dedup) | Always-on request_id + per-Key Writer-sequence + frame-digest fences |
| Producer batching | `linger.ms=5` + `batch.size` client-side | `BENCH_BATCH=128` per `/v1/writers/append-batch` call |
| Producer concurrency | perf tool's async sender | `BENCH_WRITERS=32` independent Writers |
| Fan-out | `--partitions N` (4 / 12) | `BENCH_RANGES=N` (4 / 12) — internal, invisible to the app |
| Replication | RF3 per partition (12/36 replicas) | RF3 per range (12/36 replicas) — same physical replica footprint |

| System | Pipelines | Write records/s | Write p50 | Write p99 | Read records/s |
|---|---|---|---|---|---|
| Kafka 3.8.0, `acks=all` + `enable.idempotence` | 4 partitions | 12,589 | 1,798 ms | 2,341 ms | 192,926 |
| Kafka 3.8.0, `acks=all` + `enable.idempotence` | 12 partitions | 22,099 | 905 ms | 1,225 ms | 84,388 |
| Whitewater, deferred sync | 4 ranges | 9,800 | 410 ms | 527 ms | 1,883 |
| Whitewater, deferred sync | 12 ranges | 6,554 | 605 ms | 792 ms | 1,690 |
| Whitewater, **strict sync** (per-batch fsync) | 1 range | 1,614 | 1,910 ms | — | 22,928 |
| Whitewater, deferred sync | 1 range | 6,414 | 543 ms | — | 19,355 |

**Write throughput**: Kafka's 12-partition aggregate (22.1k) leads
Whitewater's best measured fan-out on this host (9.8k @ 4 ranges) —
Kafka's async producer keeps far more batches in flight per broker.
**Per-pipeline Whitewater is faster**: 2,450 rec/s/range vs
1,842 rec/s/partition at the matched 4-vs-12 comparison, and **latency
is 2–4× lower** at every configuration (410–605 ms p50 vs 905–1,798 ms).

**Per-node utilization** (`docker stats`, sustained write phase):
Kafka brokers ran 251–288% CPU each with balanced block writes
(~2.9 GB/broker); Whitewater Nodes ran 0.2–3.5% CPU each with balanced
block writes (253–268 MB/Node). Every broker/Node carried replicas —
the difference is that Whitewater's bottleneck is quorum round-trip
serialization, not compute: it does its measured write work at ~1/100th
the CPU per node.

**Read**: Kafka's consumer-perf replays from broker page cache via
sendfile (84–193k rec/s). Whitewater's temporary Reader corroborates
committed pages against replica owners; multi-range Feeds fan those
pages **serially** today — 1.7–1.9k rec/s vs ~20–23k on a single-range
Feed. Multi-range read fan-out is the tracked gap, not a
Kafka-equivalent ceiling: the read path has no sendfile shortcut, it
pays a quorum-corroboration round per page.

**Run-to-run variance**: absolute numbers on a shared Docker Desktop
host move ±50% between runs (earlier same-shape runs measured Kafka
11,007 and Whitewater 11,621 at 12-vs-4). Compare within a row set, not
across sessions; the per-pipeline and latency relationships held across
every run.

### Write-path optimization round (post-baseline)

A phase-timed pass over the append path found the previous numbers were
not measuring the quorum engine at all. Fix-by-fix:

1. **Benchmark client connection churn.** `bench-feed.py` opened a new
   TCP connection per call through the Docker Desktop port proxy while
   Kafka's perf tool ran inside the broker network. Persistent HTTP
   connections: 9,800 -> 11,936 rec/s (+22%).
2. **Round payload cap.** `MAX_REPLICA_BATCH_FRAMES` 128 -> 512 (wire
   budget was already 16 MiB): ~19,000 rec/s best single run.
3. **Pipelined quorum rounds.** `append_lock` previously covered mint +
   local append + follower fan-out + commit for the whole range.
   The lock now covers sequence mint and local tail append only;
   per-(range,follower) `OrderedTurn` tickets keep replica delivery
   ordered (the store is strict tail-append), a per-range commit turn
   keeps the local watermark mint-ordered, and a bounded in-flight
   `Semaphore` (8/range) both bounds concurrency and lets
   `freeze_for_cutover` drain all permits before reading a boundary.
4. **Binary internal transport.** The replica batch route used ~1 MB of
   JSON + base64 per direction per round, including `frame_digest`
   serializing as a ~100-char array per result.
   `/internal/active-range/replica/append-batch-bin` carries a small
   JSON header + length-prefixed raw frames + packed binary result
   rows; same auth, same limits, ~35% fewer bytes and no serde pass.
5. **Batch-scoped catalog lookups.** Writer prepare and the owner append
   resolved Feed/Range metadata per record; now cached per batch.
6. **O(N) orphan scan removed.** `append_locked` checked a supplied
   position against `entries` with a linear scan per frame; the index
   is position-ordered, so it now binary-searches.
7. **The decisive fix: strict vs deferred fsync.** The benchmark was
   accidentally running `WHITEWATER_RANGE_SYNC_INTERVAL_MS=0` — strict
   mode, `fsync` inside every store call, ~30 ms on Docker Desktop's
   virtualized disk, serialized inside each send. Deferred mode (100 ms)
   amortizes fsyncs per interval **and is the honest `acks=all`
   equivalent**: Kafka's `acks=all` waits for ISR replication, not
   fsync either. Follower store time dropped 30 ms -> 1.3 ms per batch.

Same fixture and workload (3 Nodes, RF3, deferred sync, 4 ranges,
32 Writers x batch 512), final image:

| Metric | Whitewater | Kafka best (12 partitions) |
|---|---|---|
| Write records/s, 60k run | **23,429-26,974** | 22,099 |
| Write records/s, 200k run | **26,546** | — |
| Write p50 | **527-630 ms** | 905 ms |
| Write p99 | 713-906 ms | 1,225 ms |
| Read records/s (1 pipeline / 3 pipelines) | **169,020 / 225,066** | 203,390 / 219,780 |

Kafka's consumer speaks its native binary fetch protocol, so the only
like-for-like read row is Whitewater's binary frame transport
(`application/vnd.whitewater.frames`) — the path SDKs use. The JSON
fetch serialization exists for curl/ops/debugging, not bulk consume;
measured separately it reaches 74,601 rec/s on three feeds — the gap
to binary is the per-record base64+JSON transcode tax, the same tax
Kafka's REST Proxy pays over its native protocol.

**Whitewater now writes faster than Kafka's best measured fan-out on
this host, at ~30-40% lower p50.** Node utilization is now real work —
23-57% CPU per Node vs 0.2-3.5% when the fsync stall dominated; Kafka
used 251-288% per broker.

**Read-path rounds**: five stacked optimizations took the multi-range
read from 1,844 to **225,066 rec/s** (122x) — now matching or beating
Kafka's consumer-perf at equivalent pipeline counts.

1. `reader_range_head` paid a corroborated remote page **per record**
   (`page_limit: 1`) — ~60-70ms per record. The frontier merge now runs
   on buffered `RangeFrameStream` pages and refills drained heads
   concurrently via `join_all` (1,844 -> 11,356).
2. **Local-replica committed reads**: RF3 keeps a full committed copy on
   every replica, so `fetch_range_page` serves from whichever Node the
   client chose — the owner uses the owner read contract, a follower
   corroborates its claimed boundary against another replica's
   committed/digest evidence and falls back to the owner when its
   committed prefix lags the read Cursor. Reads stopped paying a remote
   owner round trip per page; the internal path also dropped the
   JSON/base64 `RepairFrame` envelope in favour of decoded frames
   end-to-end (~34k -> ~52k single feed).
3. **Binary fetch transport**: `Accept: application/vnd.whitewater.frames`
   returns raw stored frames (u32 count, next-Cursor, per-record
   cursor+frame lengths) instead of base64-in-JSON, chunked into 4 MiB
   stream chunks so the send overlaps page assembly (~52k -> ~101k on
   three feeds).
4. **Store-side sequential reads**: `read_indexed_frame` opened,
   statted, seeked and read the segment file **per frame** (~17us/record).
   Contiguous same-segment runs now read once into a span buffer and
   slice per frame — one syscall per page — while the per-frame blake3
   integrity check is retained (~114k -> ~143k).
5. **Page-size plumbing**: hidden clamps split every read into 32/128-
   frame store windows (313 sequential store calls per 10k page). The
   service/page/request clamps now run at 2,048-65,536 frames under a
   96 MiB committed-read byte budget, so a full Feed drain is one
   streamed response (~143k -> 225k).

Matched-shape comparison (3 Nodes each, RF3, `acks=all`-equivalent
deferred quorum sync, 1 KiB records, sequential on the same host):

| Config | System | Write rec/s | Read rec/s |
|---|---|---|---|
| 1 partition / 1 feed, 1 consumer thread | Kafka | 8,711 | 203,390 |
| | Whitewater binary | ~7,000 | **169,020** host / ~202k in-network |
| 3 partitions / 3 feeds, 3 consumer threads | Kafka | 11,926 | 219,780 |
| | Whitewater binary | ~15,500 | **225,066** |

Whitewater wins the parallel read (the throughput-relevant shape). The
single-feed host-side number is bounded by the Docker Desktop port
proxy at ~165-200 MiB/s per connection — the same drain measured
in-network (`docker exec` into the fixture, matching how Kafka's
consumer-perf runs inside its container) reaches ~202k rec/s, i.e.
parity with Kafka's sendfile consumer. Per-request server assembly is
~230-330 MiB/s, so a faster client (Rust SDK) would show it directly.

### SDK-vs-SDK comparison (Java `kafka-clients` vs Rust SDK)

Remaining JSON/base64 work on the bytes route (SDK -> ingress -> owner
-> replicas, and the read path) is reviewed in
[bytes-route-performance-review.md](bytes-route-performance-review.md).

Same isolation as the matched-config runs: Kafka alone, `down -v`,
then Whitewater alone. Both clients ran in **dedicated containers on
the bench compose network** — neither rides localhost, so the veth hop
is symmetric. 60,000 x 1,024-byte records, 8 producer/Writer threads,
RF3, acknowledged-at-quorum both sides.

| Config knob | Kafka Java SDK | Whitewater Rust SDK |
|---|---|---|
| Client | `org.apache.kafka:kafka-clients` 3.8.0 (`benchmarks/kafka-sdk`; Spring Kafka delegates to this client) | `whitewater-rust` (`Client`, `Writer`, binary `temporary_fetch_frames`) |
| Durability | `acks=all`, RF3 topic, `min.insync.replicas=2` | RF3 quorum commit, `WHITEWATER_RANGE_SYNC_INTERVAL_MS=100` (acks=all-equivalent) |
| Idempotence | `enable.idempotence=true` | Always-on per-record `request_id` dedup |
| Batching | `linger.ms=5`, `batch.size=128 KiB` client-side | `append_batch` 512/call (server bound: 1,000 records/request) |
| Read | `consumer.assign()` — no group join (stateless, matching WW temporary fetch); ~8 MiB `max.partition.fetch.bytes` measured optimum | `temporary_fetch_frames` pages of 65,536 records, `application/vnd.whitewater.frames` |

Measured (60k x 1 KB, 8 writers):

| Pipelines | Metric | Kafka Java SDK | Whitewater Rust SDK | Winner |
|---|---|---|---|---|
| 1 partition / 1 feed | write records/s | 32,500-34,400 | 16,500-17,100 | Kafka ~2x |
| | write p50 | 521-605 ms | 215-223 ms | **Whitewater ~3x lower** |
| | read records/s | 84,000-88,000 | 106,000-133,000 | **Whitewater ~1.3-1.5x** |
| 3 partitions / 3 feeds | write records/s | 34,700 | 20,600-23,500 | Kafka ~1.6x |
| | write p50 | 515 ms | 149-170 ms | **Whitewater ~3x lower** |
| | read records/s | 86,200 | 227,000-299,000 | **Whitewater ~3x** |

**Reads: Whitewater leads at both shapes and scales with pipelines**
(106-133k single feed -> 227-299k three feeds). Three things stack in
its favor: no group join or partition-assignment phase (temporary fetch
is stateless), 65,536-record pages vs Kafka's ~1-8 MiB per-partition
fetch responses, and the binary frame transport — the SDK decodes
`FSR2`/`FSR3` frames client-side, so reads ship stored bytes verbatim.
Kafka's Java read stayed ~86k regardless of partition count on this
fixture — poll cadence per consumer bounds it, not partition fan-out.
(For reference the stock `kafka-consumer-perf-test` reported
168k `fetch.nMsg.sec` on the same data — it runs inside the broker on
localhost, skipping the client-network hop both SDK benches pay.)

**Writes: Kafka's producer batching still wins on rate (~1.6-2x)** —
its producers coalesce sequential sends into per-partition batches
asynchronously, keeping more records in flight per broker round trip.
Whitewater serializes quorum rounds per range: batch 512 and batch
1,000 landed identical rates (20.6k vs 20.8k) — the bound is round
cadence, not request size. Whitewater's write latency is ~3x *lower*
(p50 149-223 ms vs 515-605 ms): `linger.ms` buys Kafka's throughput
with added delay, and a single-range quorum round is already
sub-batch-sized at this shape.

**Two honest caveats.** Whitewater's appends still ride JSON+base64 on
the wire (a binary append endpoint does not exist yet; reads went
binary first because bulk replay pays the most) — the write path's
~33% base64 inflation and serde pass are real SDK-visible costs this
comparison includes. Second, the Kafka write rate is *methodology
sensitive*: the perf tool measured 8,711-22,099 under its own sender,
while a straightforward 8-thread `kafka-clients` producer measures
~32-35k — batch coalescing rewards naive parallelism.

Reproduce:

```bash
# Kafka (alone): 1 then 3 pipelines
python scripts/bench-kafka-sdk.py                    # BENCH_PARTITIONS=1
BENCH_PARTITIONS=3 BENCH_READ_THREADS=3 BENCH_TOPIC=bench3.events     python scripts/bench-kafka-sdk.py
docker compose -f compose.kafka.yml -p whitewater-kafka-bench down -v

# Whitewater (alone): fixture on 7771-7773, deferred sync
WHITEWATER_RANGE_SYNC_INTERVAL_MS=100 COMPOSE_PROJECT_NAME=whitewater-bench     WHITEWATER_VERIFY_PORT_1=7771 WHITEWATER_VERIFY_PORT_2=7772     WHITEWATER_VERIFY_PORT_3=7773 WHITEWATER_VERIFY_SUBNET=10.250.245.0/24     docker compose -f compose.verify.yml up -d
# in the whitewater-rust checkout, release-built once:
docker run --rm --network whitewater-bench_default     -v /path/to/whitewater-rust:/src -w /src rust:1.90-bookworm     bash -c 'cargo build --release --example bench_feed &&
    WHITEWATER_ENDPOINTS=http://node1:7070,http://node2:7070,http://node3:7070     BENCH_RECORDS=60000 BENCH_WRITERS=8 BENCH_BATCH=512 BENCH_FEEDS=3     ./target-linux/release/examples/bench_feed'
```


### Adaptive write batching (`Writer::autotuned`, `Writer::batched`)

The Rust SDK now offers two ways to close Kafka's `linger.ms` gap:

* `writer.batched(Batching)` — the explicit equivalent: records wait up
  to `linger` for the batch to fill to `max_batch`, with
  `max_in_flight` concurrent `append-batch` rounds.
* `writer.autotuned(AutoTune)` — self-adjusting Nagle semantics: an
  **idle** Writer sends immediately (a trickle pays zero added
  latency); a **busy** Writer accumulates roughly one measured round
  trip — the EWMA of observed response time *is* the linger.

Adaptation is split deliberately: latency never shrinks the batch
window (under load, RTT mostly reflects the tuner's own queue, so
reacting to it is a negative feedback loop that collapses throughput —
measured: 32-record rounds shrank to 16 and the run fell to 5k rec/s).
Instead, server feedback sheds **concurrency**: a high-pressure
`recommended_batch_count` (16, or 8 on a hot Key) halves outstanding
rounds down to one; `retry_after_ms` serializes outright and floors
the accumulation window. Fast, unpressured rounds recover slots toward
`max_in_flight`. The batch cap itself is a ceiling you fill, not a
target — the server's normal advisory (`100`) is populated on every
outcome, so following it literally would pin every pipelined client to
100-record rounds; explicit clients ignore it and win.

Measured (60k x 1 KB, 8 writers, 3 feeds, same fixture as above):

| Write mode | records/s | p50 | p99 |
|---|---|---|---|
| explicit `append_batch(512)` — tuned | 22,554 | 145 ms | 262 ms |
| `autotuned` (cap 512, 4 in-flight) | 19,550 | 1,148 ms | 1,815 ms |
| `autotuned` (cap 512, 8 in-flight) | 19,892 | 1,105 ms | 1,760 ms |
| `autotuned` (cap 512, 1 in-flight) | 11,552 | 1,311 ms | 3,608 ms |

The tuned pump lands ~88% of explicit sizing with **zero** tuning
input — records queue while their batch forms, which is the honest
latency cost of batch coalescing (still below Kafka's p50 515-605 ms
at the upper envelope). The residual gap is pump plumbing overhead
(channel, spawn, queue drain) under flood, not a semantic difference.
For trickle traffic autotune is strictly better than either: a single
record sends on the next loop iteration with no linger at all.

`BENCH_AUTOTUNE=1` (+ `BENCH_MAX_BATCH`, `BENCH_IN_FLIGHT`) exercises
this path in `examples/bench_feed.rs`.

Two related fixes shipped with it:

* **Sticky session endpoints** — `Reader`/`Writer`/`Member` calls now
  pin to the Node that opened them and hop only on transport failure
  (re-pinning to the survivor). The previous per-request round-robin
  stranded Node-local pacing state: a `fetch` on one Node and its
  `ack` on another left the `ReaderPacingTable` accumulating
  unacknowledged bytes, halving page limits toward 8 and injecting
  `retry_after_ms` throttles.
* `reader.autotuned()` — adaptive page limit: full pages double the
  next `limit` toward the transport bound; empty pages relax the tail.
  Session fetch remains server-paced via `retry_after_ms`.


### Binary append transport (`FSA1`, opt-in `Writer::binary_appends`)

`/v1/writers/append-batch` now selects the wire contract on
`Content-Type`: the JSON request stays the default, and
`application/vnd.whitewater.frames` bodies carry `FSA1` append frames —
`u32 count` then `u32 body_len | body | u32 crc32` frames holding the
client-owned fields (writer, request_id, session_epoch, event_time_ns,
compression flag, key, payload, metadata). Message identity, Writer
sequence, and ingest time are still minted downstream, so idempotency
and retry semantics are identical. The Rust SDK opts in per Writer via
`Writer::binary_appends()` (`BENCH_WRITE_FORMAT=binary`).

Measured (same fixture, 8 writers, 3 feeds):

| Shape | Transport | records/s | MiB/s | p50 |
|---|---|---|---|---|
| 60k x 1 KB, batch 512 | JSON | 24,054 | 23.5 | 143 ms |
| | binary | 23,741 | 23.2 | 134 ms |
| 20k x 8 KB, batch 128 | JSON | 4,143 | 32.4 | 210 ms |
| | binary | **6,797** | **53.1** | **112 ms** |

Two findings, both worth recording:

* **At 1 KB the transports are statistically identical** — the
  per-range quorum round (~50 ms serialized send per round, ~20
  rounds/s/range) is the bound, not the wire codec. The write gap vs
  Kafka at small payloads is *not* serialization; it is quorum round
  cadence, and closing it is engine work (deeper round pipelining),
  not more transport.
* **At 8 KB the codec pass dominates** — base64+JSON was adding ~100 ms
  per batch; binary nearly doubles the byte ceiling (+64% rec/s, +64%
  MiB/s, p50 halved). Bigger payloads are where the transport earns
  its keep, so binary is the raw-throughput conduit while JSON stays
  the curl-friendly default.

The 2 MiB default body limit applies to both contracts: at 8 KB
records, JSON tops out around batch 128 (~1.36 MB on the wire) while
binary fits ~190 — the frames path reaches roughly 25% deeper into the
same request budget.


### Bytes-to-bytes matrix — 8 KB records, native transports both sides

Same fixture and methodology as the SDK-vs-SDK run, payloads raised to
8 KB with binary transports end-to-end on both sides: Kafka
`kafka-clients` (native binary protocol, `batch.size` raised to 1 MiB so
it coalesces comparably to Whitewater's 128-record frame batch) versus
Whitewater `FSA1` writes + `application/vnd.whitewater.frames` reads.
20k records, 8 writers, RF3 quorum both sides:

| Pipelines | Metric | Kafka native client | Whitewater (binary both ways) | Winner |
|---|---|---|---|---|
| 1 partition / 1 feed | write rec/s (MiB/s) | 7,427 (58.0) | 5,545 (43.3) | Kafka ~1.3x |
| | write p50 | 924 ms | 158 ms | **Whitewater ~6x lower** |
| | read rec/s (MiB/s) | 29,591 (231) | 21,271 (166) | Kafka ~1.4x |
| 3 partitions / 3 feeds | write rec/s (MiB/s) | 9,348 (73.0) | 4,275-4,590 (33-36) | Kafka ~2x |
| | write p50 | 750 ms | 172-210 ms | **Whitewater ~4x lower** |
| | read rec/s (MiB/s) | 24,057 (188) | 49,474 (387) | **Whitewater ~2x** |

Honest reads at 8 KB:

* **Reads: Whitewater scales past Kafka decisively** — 2x at 3
  pipelines (387 vs 188 MiB/s), the same scaling shape the 1 KB runs
  showed. Kafka's per-partition fetch responses cap out around
  190-230 MiB/s on this fixture regardless of partition count.
* **Writes: Kafka's coalescing scales with bytes too** — 1 MiB in
  flight per partition amortizes its ~750 ms commit over ~128 records;
  Whitewater's per-range serialized quorum rounds carry the same bytes
  but the round cadence does not amortize (4.3-4.6k at 3 feeds vs 5.5k
  at 1 — more pipelines did not help because the round, not the
  transport, is the bound). This is the concrete engine work item:
  pipelined quorum rounds per range, not more transport work.
* **Latency stays decisively Whitewater's**: write p50 158-210 ms vs
  Kafka's 750-924 ms at identical durability — the linger/coalesce tax
  is ~4-6x at this payload.

(The earlier same-shape 3-feed binary run measured 6,797 rec/s on a
better host day — Docker Desktop variance; compare within a run set.)

### Kafka-parity rounds — SDK vs SDK as a pass/fail gate (2026-11-01)

The gate: the Whitewater Rust SDK must match or beat the Kafka Java SDK
on write rec/s, write p50 and read rec/s on an equivalent setup.
Kafka runs `org.apache.kafka:kafka-clients` 3.8.0 (`benchmarks/kafka-sdk`)
with `acks=all`, `enable.idempotence=true`, RF3 and `min.insync.replicas=2`
on 3 brokers, and reads with direct `assign()` (no group join). Whitewater
runs `whitewater-rust` `examples/bench_feed` on 3 Nodes with RF3 quorum
commit and `WHITEWATER_RANGE_SYNC_INTERVAL_MS=100`, using binary writes
(`BENCH_WRITE_FORMAT=binary`, FSA1) and binary reads.

Both clients ran in containers on their own bench network, and only one
system ran at a time (`down -v` between them). Each Kafka run used a
fresh topic: a reused topic inflates reads, because records left from
earlier runs get counted. All runs were on the same day on Docker
Desktop.

Measured at 60,000 × 1 KiB records with 8 writers (Whitewater batch 512).
Values are run ranges. For 1-feed reads, the range spans page limits
65,536 / 8,192 / 4,096 records, with and without prefetch.
"After" covers the final two server builds, which differ only in
fsync-error latching and SDK prefetch. One earlier-build outlier, an
87,034 rec/s 1-feed read at a 65,536 page with no prefetch, is excluded.

| Shape | Metric | Kafka Java SDK | Whitewater before this round (JSON writes) | Whitewater after | Gate |
|---|---|---|---|---|---|
| 1 partition / 1 feed | write rec/s | 31,816 / 35,385 / 37,286 | 15,692–17,063 | 36,724 / 37,059 / 37,252 / 37,671 | pass |
| | write p50 | 377–576 ms | 226–231 ms | 78–85 ms | pass |
| | read rec/s | 195,532 / 222,096 / 249,783 | 100,913–116,439 | 142,734–180,955 | **fail** |
| 3 partitions / 3 feeds | write rec/s | 36,307 / 41,537 / 41,791 | 21,185–23,255 | 43,634 / 44,018 / 46,954 | pass |
| | write p50 | 174–358 ms | 150–167 ms | 46–51 ms | pass |
| | read rec/s | 87,715 / 88,141 / 88,251 | 248,676–262,601 | 285,175–386,506 | pass |

The larger shape (300,000 × 1 KiB, 16 writers, 1 partition / 1 feed)
does not pass yet:

| Metric | Kafka Java SDK | Whitewater after |
|---|---|---|
| write rec/s | 72,701 / 76,751 | 34,885 |
| write p50 | 1,447 / 1,480 ms | 200 ms |
| read rec/s | 312,838 / 368,876 | 204,527–243,989 (8,192/16,384 pages, ± prefetch) |

Kafka's producers keep more batches in flight per broker as writer
count grows. They gain throughput at roughly 7x the latency. A single
Whitewater range currently tops out around 35–37k rec/s. That limit is
the owner's local append section, about 15–20 ms per 512-record round.

What changed, and the phase timing that motivated each change
(`RUST_LOG=whitewater=debug`, `"append batch phase timing"`):

1. **Windowed replica sends.** Before, each (range, follower) lane carried
   one send at a time. Under load, rounds spent `send_wait_ms` p50
   438 ms queued behind the previous send, while the send itself took
   24 ms. Now up to 4 sends per lane are in flight (`majority.rs`
   `MAX_INFLIGHT_SENDS_PER_FOLLOWER`). Tickets still start sends in mint
   order. Followers admit batches in position order: they wait on an
   appended-watermark `watch` for up to 500 ms before the existing
   `PositionGap`/repair path. A late duplicate batch whose fused commit
   is already below the committed watermark is accepted only when every
   frame dedups and the digest matches. After this change `send_wait_ms`
   is 0.
2. **Balanced Append Owners.** Owner selection had been pure rendezvous
   hash, and one run left a Node owning no ranges (rounds per Node were
   75 / 45 / 0). The owner is now the replica with the fewest owned
   ranges, with ties broken by hash rank. Rounds per Node: 277 / 269 / 254.
3. **Bytes in process.** Key, payload, Metadata and stored frames are now
   carried as `bytes::Bytes` from ingress to the replicas. serde adapters
   keep the internal JSON wire format byte-identical. There is no
   base64 re-encode after FSA1 ingress and no encode→decode pair on the
   owner. The owner encoder no longer clones records, the store reads
   the record header without a full decode, and the forward hop to the
   owner is binary (`/internal/active-range/owner/append-batch-bin`, with
   a JSON fallback on 404/405).
4. **Deferred fsync off the append lock.** In deferred mode, the
   periodic `sync_data` + directory sync used to run inline under the
   range lock, causing ~30 ms p90 spikes. It now runs on a cloned fd in
   a background thread. A failed sync latches `DeferredSyncFailed`, and
   every later mutation fails closed until the store reopens.
   Strict/forced syncs are unchanged.
5. **Read path.**
   - The budget checks use a header-only size, computed once per frame,
     instead of three CRC passes per frame.
   - Spans are read once and sliced as zero-copy `Bytes`, and blake3 is
     verified in parallel for runs of 1,024 frames or more.
   - Committed local runs are read outside the range lock; uncommitted
     reads stay locked.
   - Pages go up to 65,536 frames, still bounded by the 96 MiB budget.
   - The response streams lazily in ~512 KiB coalesced chunks. An
     earlier 2-chunks-per-record version made transfer 5x slower.
   - SDK side: the page is decoded while it streams. The pipelined fetch
     starts page k+1 as soon as page k's header (with `next_cursor`) has
     been parsed.
   - Result for one 60k-frame page in-network (curl): TTFB dropped from
     ~270 ms to ~160 ms.
6. **Body limit.** `/v1/writers/append-batch` now takes up to 8 MiB.
   Before, 4 KiB × 512 batches hit axum's 2 MiB default and got HTTP 413
   (26 rec/s, 28,672 errors). They now run at 9,070 rec/s with 0 errors.
   The SDK splits a batch in half on 413 and no longer retries
   authoritative refusals.

Open items:

* Single-feed read assembly costs ~2.7 µs/record server-side.
* The owner's local append section limits a single range.
* SDK per-Writer pipelining will keep per-Key order: in-flight batches
  never share a Key.
* One isolated ~29 s stall (1 of ~11 write runs). It is consistent with
  the batch fast path falling back to per-record appends. Warnings now
  log that fallback, and the stall has not recurred.

#### Round 2 — group commit, staged store writes, per-Key pipelining (same session, both systems re-measured)

Setup is the same as above. Every Kafka run used a fresh topic, and the
systems ran one at a time. Whitewater ran with info-level logging and
`BENCH_PAGE_LIMIT=16384`. The 300k shape used `BENCH_BATCHED=1`, the
Kafka-producer-equivalent mode: linger 5 ms, 512-record batches, 5
in flight per Writer, and no two in-flight batches from one Writer ever
share a Key.

| Shape | Metric | Kafka Java SDK | Whitewater Rust SDK | Gate |
|---|---|---|---|---|
| 60k × 1 KiB, 8 writers, 1 partition / 1 feed | write rec/s | 29,134 / 37,343 | 46,125 / 64,945 | pass |
| | write p50 | 595 / 410 ms | 54 / 37 ms | pass |
| | read rec/s | 218,182 / 232,959 | 191,305 / 130,817 | **fail** |
| 60k × 1 KiB, 8 writers, 3 partitions / 3 feeds | write rec/s | 32,160 / 45,247 | 48,622 / 48,198 | pass |
| | write p50 | 370 / 144 ms | 30 / 30 ms | pass |
| | read rec/s | 85,649 / 88,576 | 240,823 / 331,099 | pass |
| 300k × 1 KiB, 16 writers, 1 partition / 1 feed | write rec/s | 93,970 / 103,477 | 49,510 / 50,515 | **fail** |
| | write p50 | 1,134 / 893 ms | 83 / 90 ms | pass |
| | read rec/s | 282,654 / 214,549 | 190,672 / 158,603 | **fail** |
| 300k × 1 KiB, 16 writers, 3 partitions / 3 feeds | write rec/s | 101,417 / 113,512 | 61,866 / 63,236 | **fail** |
| | write p50 | 740 / 626 ms | 69 / 74 ms | pass |
| | read rec/s | 304,324 / 290,849 | 241,741 / 306,724 | mixed |

Changes in this round:

* **Owner group commit.** Client batches waiting on one range merge
  into a single quorum round. Rounds observed at 1–3k frames, capped at
  4,096 frames / 16 MiB. A merged round that fails non-retryably re-runs
  each submission alone, so one bad batch cannot fail its neighbours.
  Pending-effect submissions are never merged. The in-flight window
  permit is taken per round, after enqueue.
  `MAX_REPLICA_BATCH_FRAMES` is now 4096: a follower still on an older
  version refuses merged rounds.
* **Staged store writes.** One `write_all` per batch, with the active
  segment length tracked in memory instead of a seek per frame. The
  persisted state is updated in place. Header peek and blake3 digests are
  computed in parallel before the range lock. When no segments are
  tiered, the remote-manifest scan is skipped.
* **Encoding outside the append lock.** Frames are encoded before the
  lock; under it, only the minted sequence is patched in and the CRC
  recomputed.
* **Checkpoints without rename.** Lazy (deferred-mode) checkpoints go to
  alternating `range-state.lazy-a`/`-b` slots, each with a sequence
  number and checksum, written with no rename and no fsync. Strict
  checkpoints keep tmp + fsync + rename. Open, retention sweep and tier
  scan all resolve the newest valid checkpoint.
* **SDK.** `Writer::batched` pipelines up to `max_in_flight` batches,
  with in-flight batches never sharing a Key. A finished batch wakes the
  pump immediately; before, it could idle for up to 500 ms.

Verdict: Whitewater passes every latency gate, by 4–13x. It passes write
throughput at the 8-writer shape and read throughput across 3 feeds.

It does not yet match Kafka on write throughput with 16 producers, or on
single-feed reads. Kafka's leader scales to ~100k rec/s on one partition
by coalescing many in-flight producer batches, at 0.9–1.1 s p50. One
Whitewater range currently saturates around 50k rec/s. Once the owner
lock is no longer the bottleneck, the limit is the follower apply path:
it is serialized per range and costs ~10–14 ms per 1k-frame round. Node
CPU stays below ~1.4 of 16 cores, so the remaining gap is serialization,
not compute.

#### Round 3 — 1 GiB default segments (Kafka's `log.segment.bytes`)

At 256 MiB, a 300k-record run rotates the active segment partway
through. Sealing the old segment (a ~256 MB `sync_all`, a ~38 MB index
checkpoint, and a full-segment manifest hash) flooded Docker Desktop's
shared virtual disk. Writes then capped at ~48k rec/s with p95 of
1.1–1.7 s, even with sealing moved off the append path. Kafka never
rolls a segment at this size.

`WHITEWATER_MAX_SEGMENT_BYTES` now defaults to 1 GiB. During a rolling
upgrade, pin the old 256 MiB value explicitly until every Node runs the
new default: a divergent bound diverges segment boundaries and manifest
digests.

Shape: 300k × 1 KiB, 16 writers, `BENCH_BATCHED=1`, binary writes, same
host. The Kafka figures are from the same day.

| Durability | Shape | Kafka Java SDK | Whitewater (1 GiB default) |
|---|---|---|---|
| no fsync (Kafka default / `never`) | 1 partition / 1 range | 75,180 / 97,358 rec/s, p50 984–1,433 ms | 75,465 / 76,398 rec/s, p50 86–87 ms |
| | 3 partitions / 3 ranges | 99,342 / 121,445 rec/s, p50 470–827 ms | 77,366 / 74,648 rec/s, p50 82–83 ms |
| fsync every 100 ms (`log.flush.*=100` / `100`) | 1 partition / 1 range | 79,367 / 73,235 rec/s, p50 1,431–1,448 ms | 56,475 / 56,356 rec/s, p50 88–90 ms |
| | 3 partitions / 3 ranges | 100,010 / 112,927 rec/s, p50 596–811 ms | 59,509 / 59,260 rec/s, p50 100–102 ms |

For the fsync-every-100 ms comparison, Kafka needs both
`KAFKA_LOG_FLUSH_INTERVAL_MS=100` and
`KAFKA_LOG_FLUSH_SCHEDULER_INTERVAL_MS=100`. The scheduler also
defaults to Long.MAX, so setting only the first knob does nothing.

### Write-path history

The original per-record pipeline saturated at ~5-10 records/s (each
record paid a local fsync, a commit fsync, and four HTTP round trips
under the range lock). Batching the quorum round lifted it to ~1,100;
fusing the CommitPosition marker into the replica batch removed the
second network round (~1,614); the deferred-sync mode removed the
per-batch fsync chain that the 3-Node fixture serialized on
(~6,400/pipeline); and parallel per-range groups multiplied pipelines.

**Auto-split caveat**: mid-run splits freeze a generation for cutover
(seconds under load) — pre-provisioning (`BENCH_RANGES`) measures
steady-state fan-out, while tuning auto-split under live load is
separate work.
