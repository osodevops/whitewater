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
