# Lessons from Apache Kafka Source

Whitewater is a clean-sheet system, but it should learn from Kafka's solved problems rather than reject them reflexively. The local reference checkout is `C:\Code\Stackapps\kafka-trunk`. This document records source-level observations that influence Whitewater design.

## Independent consumer positions are essential

Kafka distinguishes two positions in `clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java`:

- The current consumer position: the next record returned to that consumer.
- The committed position: the durable restart point stored for a consumer group.

Kafka also supports manual assignment and `seek`, allowing a consumer to operate independently and store its own offsets outside Kafka. This is a strong and necessary capability.

Whitewater retains and clarifies that behavior:

```text
Reader Cursor
    private to one Reader instance
    advances as that Reader reads
    may seek without affecting anyone else
    need not be committed

Subscription Cursor
    durable progress for one named Subscription
    shared by Readers cooperating in that Subscription
    independent from every other Subscription
    advances only through explicit acknowledgement or atomic processing
```

Consequences:

- Reading does not remove an event from a Feed.
- Any number of independent Readers may traverse the same Feed at different positions.
- Any number of durable Subscriptions may process the same Feed independently.
- Readers in one Subscription share leased work but do not affect another Subscription.
- A standalone Reader may retain its Cursor in application state without creating a Subscription.
- Seeking one Reader or Subscription never rewinds another.

The Phase 1 read API is stateless and already accepts an `after` Cursor, so clients can maintain independent positions. Durable Subscription Cursor storage belongs in the replicated control plane and must not be simulated as authoritative local Node state.

## Current position and committed position must remain distinct

Kafka's distinction is valuable because delivery and processing are not the same event. A client may have received data that it has not safely processed.

Whitewater terminology:

```text
Delivered Cursor  = highest position delivered to a Reader
Acknowledged Cursor = highest position durably accepted by a Subscription
```

A crash resumes from acknowledged progress. This gives base at-least-once delivery. Atomic consume-and-append can advance the acknowledged Cursor and commit output effects together.

Whitewater must not auto-acknowledge merely because bytes were sent over the network.

## Kafka group sharing remains useful

Kafka consumer groups ensure one member of a group processes a partition while separate groups each receive the full logical history. The partition-based assignment mechanism is what Whitewater changes, not the useful fan-out behavior.

Whitewater Subscriptions preserve this model:

```text
Feed
  -> Subscription billing
       -> Reader A
       -> Reader B
  -> Subscription fraud
       -> Reader C
  -> standalone audit Reader
```

`billing`, `fraud`, and the audit Reader each have independent progress. Reader A and Reader B cooperate only inside `billing` through small epoch-fenced leases.

## Kafka timestamps use millisecond epoch values

Kafka's `ProducerRecord` documents its timestamp as milliseconds since the Unix epoch. The producer may provide CreateTime, or the broker may replace it with LogAppendTime according to topic configuration.

Kafka's record batches store timestamp deltas relative to a base timestamp. `storage/src/main/java/org/apache/kafka/storage/internals/log/TimeIndex.java` stores an eight-byte timestamp and four-byte relative offset per entry, with monotonically increasing indexed timestamps and binary-search lookup.

Whitewater adopts the good parts while increasing precision and removing the either/or timestamp choice.

Every new Whitewater record stores:

```text
event_time_ns   signed 64-bit Unix epoch nanoseconds
ingest_time_ns  signed 64-bit Unix epoch nanoseconds
```

- `event_time_ns` is supplied by the Writer or defaults to acceptance time.
- `ingest_time_ns` is assigned by the accepting Whitewater Node.
- Both are retained; one never overwrites the other.
- Signed 64-bit epoch nanoseconds support instants through approximately year 2262 and permit pre-epoch data.
- Binary APIs and storage use integer nanoseconds, avoiding floating-point time.
- JSON APIs use decimal strings because epoch nanoseconds exceed JavaScript's exact integer range.

Nanosecond representation does not guarantee that the host clock is accurate to one nanosecond. It preserves available precision and prevents the storage format from discarding sub-millisecond information. Clock source, synchronization quality, and monotonic duration measurement remain separate concerns.

## Time indexing lessons

Kafka's sparse `TimeIndex` is rebuildable and maps timestamps to nearby offsets rather than indexing every record. Whitewater should preserve that economy.

Whitewater's future temporal index should consider:

- Sparse entries over immutable segments
- Nanosecond timestamps
- Binary search to a candidate Cursor
- Rebuildability from checksummed records
- Separate event-time and ingest-time indexes where requested
- Explicit behavior for non-monotonic event time
- Monotonic ingest-time indexing within an accepted append sequence
- Bounded index memory and disk overhead

Event time may arrive out of order, so a simple monotonic sparse index cannot answer every event-time query exactly. Ingest time can support a monotonic segment index; arbitrary event-time range query may require an explicit secondary Index.

## Offset-index lessons

Kafka uses sparse relative offset indexes per segment rather than one heavyweight global entry per record. Whitewater Cursors hide physical position, but internal segment lookup still benefits from sparse indexes and a bounded scan from the nearest entry.

The public abstraction should remain opaque even if the internal implementation uses concepts learned from Kafka's OffsetIndex and TimeIndex.

## Group coordination lessons

Kafka's coordinator solves real problems:

- Membership
- Failure detection
- Durable committed progress
- Assignment
- Generation fencing
- Offset commit validation

Whitewater should preserve these correctness responsibilities while changing their granularity:

- Small range leases rather than public partition assignments
- Incremental transfer rather than group-wide synchronization
- Lease epochs rather than one global group generation
- Capacity-aware allocation
- Subscription progress independent of Reader liveness

## Practices for future source research

When Whitewater implements a subsystem already present in Kafka:

1. Identify the correctness contract Kafka provides.
2. Inspect the public API and internal implementation separately.
3. Read related tests and Kafka Improvement Proposals.
4. Identify which complexity is fundamental and which follows from partitions or compatibility.
5. Preserve proven failure handling where applicable.
6. Write Whitewater's contract before writing code.
7. Add fault tests for the same failures plus the new architecture's failures.

Areas worth continued study:

- Record batch encoding and compression
- Sparse offset and time indexes
- Idempotent producer state
- Transaction coordinator fencing
- KRaft snapshots and quorum recovery
- Replica fetch and divergence handling
- Log recovery and corruption behavior
- Tiered-storage interfaces
- Consumer protocol evolution
- Kafka Streams state restoration
- Protocol compatibility testing and fuzzing

Kafka is a valuable body of production engineering. Whitewater's goal is to retain its hard-earned correctness lessons while removing public topology coupling.
