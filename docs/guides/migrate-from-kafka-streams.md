# Migrating from Kafka Streams

The mental model translates directly — the work does not. What changes
is *where* the machinery lives: Whitewater moves progress, state,
leases, retries, and topology inside the Riverbed, so the per-language
SDK is a facade, not a second processing framework to operate.

## Concept mapping

| Kafka Streams | Whitewater | What you stop owning |
|---|---|---|
| Topic | **Feed** (immutable temporal history) | partition count, replication factor, `min.insync.replicas` |
| `KStream`/`KTable` | Feed reads + **StateStore**/Index views | materialized-store directories, changelog topics |
| Consumer group + `group.id` | named **Subscription** (durable shared progress) | rebalancing storms, `max.poll.interval.ms`, static membership tuning |
| `consumer.poll` loop | supervised `consumer()` / member claim-fetch-ack | offset commit plumbing, `enable.auto.commit` traps |
| `KStream.transform`/DSL topology | managed **Pipe** (forward / enrich / window) | topology builder, `StreamsBuilder`, `KafkaStreams` lifecycle |
| RocksDB state stores | server-side **StateStore** with named keys | local state dirs, standby replicas, restore time |
| `windowedBy` + grace | `PipeOperation.window(...)` + `maxLate`/`watermark`/`grace` durations | window-store plumbing, suppression, retention config |
| `Materialized.as` + Interactive Queries | `store.get`/`get_by`/`iterate` + declared Index queries | IQ plumbing, metadata discovery, host:port state RPC |
| Exactly-once `processing.guarantee` | Pipe effect journal (atomic consume/declare/apply) | transaction timeouts, `__consumer_offsets`, fenced producers |
| `DeserializationExceptionHandler` → DLQ | `EnrichMiss`/`quarantine_feed`, `late` rows | hand-rolled DLQ producers + poison-pill loops |
| `retry`/`default.deserialization.exception.handler` | typed error taxonomy (`refused`/`conflict`/`ambiguous`/`unavailable`/`stale_epoch`/`index_behind`) | guessing which exceptions mean "retryable" |
| Partitions + keys for ordering | Feed + application Key — same-Key order, no total cross-Key order | `partition.assignment.strategy`, co-partitioning joins |

## The topology becomes a declaration

A Kafka Streams enrichment topology — source topic, repartitioned join
against a `KTable`, output topic, DLQ topic, and the broker-side
repartition/changelog topics it silently creates — is one Whitewater
Pipe declaration:

```text
Kafka Streams:                       Whitewater:
source topic    (activity.events)    CREATE FEED accounts.events;
KTable topic    (users + changelog)  CREATE STATESTORE accounts.users;
DLQ             (misses topic)       CREATE FEED accounts.misses;
repartition     (internal topic)     — nothing: no partition to co-locate
output          (enriched topic)     CREATE FEED accounts.enriched;
consumer group  (app.id)             CREATE SUBSCRIPTION enrich-work FROM FEED accounts.events;
DSL graph                          client.define_*_pipe("enrich-users", …);
```

The declaration (identical in all six SDKs — see
[enrichment](enrichment.md)) names the Subscription, the output Feed,
the StateStore, the key extractor, the freshness bound, and the miss
policy. The Riverbed owns the member leases, the progress frontier, the
effect journal, and the quarantine route; the application owns the
business types only.

## What actually moves

- **Ordering**: Kafka gives per-partition order; Whitewater gives
  same-Feed/same-Key order. If your topology relied on global partition
  order, re-model around Key — it is the real contract you were using.
- **Offsets → Cursors/progress**: there is no `offset` to seek. Members
  claim fenced work; operators reposition via `SEEK SUBSCRIPTION`, and
  replay uses `records`/`temporary_fetch` with opaque Cursors.
- **DLQ handling**: replace `ProductionExceptionHandler` + a DLQ
  producer with the Pipe's `quarantine_feed` (enrich misses) or `late`
  rows (window out-of-bounds) — both are first-class server outputs.
- **State restore**: nothing restores — StateStores are RF3 server
  state; a crash does not reload a local RocksDB.
- **Interactive queries**: `lookup`/`get_by`/`iterate` replace the
  IQ + metadata-discovery layer; `strict` reads return `index_behind`
  instead of silently stale rows.
- **Windows**: `windowedBy().aggregate()` + grace/retention config maps
  to `PipeOperation.window(width, key, aggregation)` +
  `watermark`/`grace`/`expire`/`maxLate` durations (each SDK's native
  time type — `Time.h(1)`, `time.Hour`, `timedelta`, `TimeSpan`,
  `Duration`) — see
  [late-event policy](late-event-policy.md) for the semantics mapping.

## Not yet covered

Joins between two Feeds, session windows, and arbitrary DSL graphs are
not declared Pipe operations today — the supported operation set is
`forward`, `enrich`, `window`. Consumer-loop code ports mechanically
(supervised `consumer()` per SDK); complex topologies may need the loop
form until the operation set grows — check
[sdk-support-matrix](../sdk-support-matrix.md) for the gated surface.
