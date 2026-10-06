# Whitewater Architecture

Visual companion: [Whitewater sequence diagrams](sequence-diagrams.md).

> A clean-sheet design for a distributed event platform that keeps Kafka's durable ordered-history model while removing the architectural coupling that makes Kafka difficult to scale, operate, and evolve.

## Document status

- **Status:** Early architecture proposal
- **Purpose:** Living design document for continued development and prototyping
- **Product:** Whitewater
- **Company and package namespace:** FinnStream / `finnstream`
- **Implementation language:** Rust
- **Motivation and terminology:** [Why Whitewater](why-whitewater.md)
- **Operational experience:** [Humane operations and day-two requirements](operational-experience.md)
- **Kafka source research:** [Lessons retained from Apache Kafka](kafka-source-lessons.md)

## Core thesis

Kafka makes a partition perform too many unrelated jobs:

```text
partition =
    ordering boundary
    + parallelism unit
    + storage unit
    + replication unit
    + consumer assignment unit
```

This coupling is the source of much of Kafka's operational and application complexity.

Whitewater separates those responsibilities. Its public abstraction becomes:

```text
riverbed -> space -> feed -> key -> cursor -> subscription
```

A Feed has an immutable FeedId and a mutable dotted FeedName. The physical layout, replication strategy, work assignment, and degree of parallelism remain internal implementation details that can change dynamically. For the distinction between Kafka's public partitions and Whitewater's internal Active Ranges, including implementation limits, see [Kafka partitions versus Whitewater Active Ranges](why-whitewater.md#kafka-partitions-versus-whitewater-active-ranges).

## Design principles

1. **Ordering, storage, replication, parallelism, and consumer ownership are independent concerns.**
2. **Applications never choose or manage partitions.**
3. **Events sharing a key are ordered; unrelated keys can be processed independently.**
4. **Normal operation requires almost no tuning.**
5. **Retries are safe by default.**
6. **Reader membership changes do not stop unrelated processing.**
7. **The core protocol remains small, binary, and schema-neutral.**
8. **Recent data is fast; old data is cheap; the distinction is transparent to applications.**
9. **Coordination and consensus are used only where correctness requires them.**
10. **Multi-tenancy and workload isolation are foundational, not later additions.**
11. **Every supported Riverbed has at least three Nodes and every active range has at least three replicas.**
12. **Feed history is immutable; arbitrary application-defined secondary Indexes and current state are core persisted replicated storage capabilities, not later query optimizations.** Their [shared keyspace and transaction contract](why-whitewater.md#index-storage-contract-and-fjall-layout) must be implemented before claiming Index availability.
13. **TLS and scoped API-key authentication are mandatory defaults, not deployment options.**
14. **Feed identity is immutable while its dotted human-readable name may change.**
15. **Development uses the same three-Node topology and protocol contracts as production.**
16. **Expected operational change is gradual, explainable, reversible where possible, and should not wake somebody up.**
17. **Whitewater is both a distributed database and a streaming platform: Feeds hold temporal history, while persisted replicated Indexes hold current and queryable state.**

## 1. Ordering without partitions

The user-visible ordering contract is intentionally simple:

```text
Events with the same key are ordered.
Events with unrelated keys are independently parallel.
```

Internally, keys map onto a large number of lightweight logical ranges or shards:

```text
customer-123 -> shard 48193
customer-456 -> shard 72811
```

These shards are not visible to applications. The system may move, split, merge, or remap them while preserving per-key ordering.

This removes the need for an application developer to predict a topic's lifetime partition count or treat it as permanent architecture.

### Desired properties

- Millions of cheap logical shards per cluster
- Dynamic placement across data nodes
- Hot-key detection
- Online shard splitting and migration
- Stable per-key ordering across migrations
- No client-visible topology changes

## 2. Independent storage and processing placement

A Node should not permanently own both a logical Feed and its physical files. Durable storage and active processing placement should be independently movable.

```text
                    CONTROL PLANE
                 small Raft cluster
                        |
              ownership / metadata / epochs
                        |
          +-------------+-------------+
          |                           |
          v                           v
      DATA NODE                   DATA NODE
    +------------+              +------------+
    | append log |              | append log |
    | RAM cache  |              | RAM cache  |
    | NVMe       |              | NVMe       |
    +-----+------+              +-----+------+
          |                           |
          +-------------+-------------+
                        v
                   OBJECT STORE
                immutable segments
```

Data nodes own active append ranges temporarily. Recent data remains on replicated local NVMe for low latency. Sealed immutable segments can move to object storage without changing stream semantics.

Compute or data nodes may then come and go without requiring the wholesale migration of long-lived stream history.

## 3. Lease-based consumption instead of group rebalancing

Kafka-style stop-the-world consumer group rebalancing should not exist.

Readers cooperating in one Subscription acquire short-lived leases over small work ranges:

```text
Reader A: ranges 1-184
Reader B: ranges 185-401
Reader C: ranges 402-612
```

If Reader C disappears, only its leases expire. Other Readers progressively acquire those ranges while continuing their existing work.

When a Reader joins, the system gradually transfers leases toward a balanced assignment. There is no global pause and no need for all members to agree on a new generation simultaneously. Separate Subscriptions and standalone Readers remain fully independent and each maintain their own Cursor positions.

### Lease requirements

- Epoch-fenced ownership to prevent stale consumers from committing
- Short renewal interval without excessive control-plane traffic
- Incremental acquisition and release
- Capacity-aware allocation
- Affinity hints for cache locality
- Bounded duplicate delivery during failures

## 4. Opaque cursors instead of public offsets

Offsets expose physical storage layout and make topology evolution harder.

Clients should receive opaque cursors:

```text
cursor = 8af19d...
```

Internally, a cursor may represent:

```text
FeedId
range generation
segment identity
record position
```

The encoding is a protocol detail. Clients can store, commit, compare where supported, and seek with cursors without understanding their contents.

This allows the underlying storage organization to change while keeping the client contract stable.

### Independent Reader and Subscription positions

Reading never removes an event. Every standalone Reader owns a private current Cursor and may seek without affecting any other Reader. Every named Subscription owns a separate durable acknowledged Cursor, independent from all other Subscriptions. Readers cooperating inside one Subscription share leased work and advance that Subscription's progress only through acknowledgement.

Delivered position and acknowledged position remain distinct. A crash resumes from acknowledged progress, not from the last bytes sent to a Reader. Durable Subscription Cursors belong in replicated control-plane state; they must not depend on one Node's local process memory.

### Nanosecond record time

Every record stores two signed 64-bit Unix epoch nanosecond values:

```text
event_time_ns   Writer-supplied event time, or acceptance time when omitted
ingest_time_ns  Whitewater acceptance time
```

Both are retained; ingest time never overwrites event time. The binary protocol stores signed 64-bit integers. JSON represents them as decimal strings because epoch nanoseconds exceed JavaScript's exact integer range. Nanosecond representation preserves sub-millisecond source precision, although it does not claim that every host clock is physically accurate to one nanosecond. Monotonic durations use a monotonic clock rather than epoch timestamps.

## 5. Atomic consume-and-append, not transactions everywhere

Full distributed transactions are expensive and unnecessary for most streaming work.

The fundamental processing primitive should be deterministic processing followed by an atomic consume-and-append operation:

```text
commit {
    advance cursor(input)
    append(output1)
    append(output2)
}
```

This atomically records both input progress and derived output. A crash leaves either all of the operation committed or none of it committed.

Full multi-stream or multi-resource transactions may exist, but they are an explicit exceptional feature rather than the default execution model.

### Initial scope

- One input cursor
- Zero or more output appends
- Optional state mutations owned by the same processing unit
- Epoch fencing against stale lease holders
- Deterministic retry following an ambiguous network result

## 6. Protocol-level idempotency

Every append carries an identity such as:

```text
MessageId
ProducerId
Sequence
```

The cluster tracks enough producer state to recognize a retry. Therefore:

```text
send
timeout
send again
```

does not create a second logical message.

Idempotency is part of the base protocol, not an optional producer mode. Exactly-once stream processing then follows primarily from idempotent append plus atomic consume-and-append rather than a separate collection of client-side mechanisms.

## 7. Protocol-level backpressure

Consumers advertise actual capacity rather than relying on a collection of loosely related polling and timeout settings:

```text
maxMessages  = 5000
maxBytes     = 64 MiB
targetLatency = 100 ms
```

The server continuously adapts delivery to observed processing rate, acknowledgement latency, available memory, and current load.

Lag remains observable, but it is not the flow-control mechanism.

### Expected behaviour

- Never exceed the consumer's advertised in-flight limits
- Reduce delivery automatically when acknowledgements slow
- Fairly share capacity between subscribed streams
- Avoid starving low-throughput streams
- Expose whether delay comes from producers, storage, delivery, or processing

## 8. Adaptive producer batching

The normal producer API should be little more than:

```rust
stream.send(event).await?;
```

The client runtime dynamically selects batching, compression, and in-flight concurrency using:

- Latency target
- Network round-trip time
- Current throughput
- Compression effectiveness
- Message size distribution
- Server load and feedback

Expert overrides may exist, but fixed values analogous to `linger.ms`, `batch.size`, and `max.in.flight` should not be required for normal use.

## 9. Feeds as hierarchical dotted namespaces

A Feed has an immutable FeedId and a mutable dotted FeedName rather than being a heavyweight physical storage object:

```text
commerce.orders.europe
commerce.orders.us
payments.visa.events
payments.mastercard.events
```

Feed names are lowercase ASCII, at most 512 bytes, and each segment starts with a letter followed by letters or numbers. Subscriptions can target an exact Feed or a namespace pattern:

```text
commerce.orders.*
```

or:

```text
commerce.orders.{region}
```

Spaces and namespace prefixes inherit policy where useful, including history, authorization, quotas, encryption, and schema metadata, while allowing specific overrides.

The implementation must avoid turning each dotted prefix into an expensive independently coordinated object.

## 10. Bytes at the core, schemas above it

The storage and wire protocol remain schema-neutral:

```text
metadata: bytes
key:      bytes
payload:  bytes
```

Schemas are an optional higher-level service and client capability. The core engine should not depend on JSON, Avro, Protobuf, or a particular type-evolution model.

This keeps the storage engine compact and usable for workloads that do not want structured records, while still allowing excellent schema support to be built as a first-class companion feature.

## 11. Multi-tenancy from the beginning

One installation should safely serve many teams and applications.

Built-in isolation includes:

- Namespace-level storage quotas
- Ingress and egress bandwidth quotas
- CPU and IO scheduling
- Producer and consumer connection limits
- Retention limits
- Access-control policies
- Per-tenant encryption boundaries where required
- Protection from hot or abusive neighbours

Resource accounting must follow a tenant's work across the entire system, including object-store reads, replication, compaction, and consumer delivery.

## 12. Replication below the Feed abstraction

Kafka replicates partitions. Whitewater replicates active append ranges and immutable storage chunks beneath the logical Feed abstraction.

```text
append buffer
    -> replicated active range
    -> immutable segment
    -> seal and verify
    -> object storage
```

Consensus is kept small and focused on:

- Ownership
- Epochs
- Metadata
- Membership
- Recovery decisions

The normal data path should not require global consensus for each message. Acknowledgement policy can determine when an append is considered durable, for example after replication to a quorum of active-range replicas.

## 13. A deliberately small wire protocol

The essential protocol should remain small enough to understand and implement correctly. An initial operation set might be:

```text
OPEN

APPEND
APPEND_BATCH

SUBSCRIBE
FETCH
ACK

SEEK

CREATE_STREAM
DELETE_STREAM
DESCRIBE

BEGIN
COMMIT
ABORT
```

Additional capabilities should generally be expressed through these primitives, negotiated extensions, or higher-level services rather than continually expanding the mandatory core.

## 14. Deliberate elasticity without oscillation

Nodes report demand and capacity, but an external control-plane reconciler changes the process or container count. Data nodes must not receive infrastructure credentials or direct access to a Docker or orchestration socket.

Expansion occurs one node at a time only after pressure remains above a configured threshold for a sustained window. A cooldown follows every change so a new node has time to join, receive placement, warm caches, and affect measured pressure before another decision.

Contraction is intentionally slower. Pressure must remain below a separate lower threshold for a longer window, and the selected node must first be drained of active ownership and unique local data. A low-load signal alone is never permission to delete a data node.

```text
sustained high pressure
    -> add one node
    -> wait for membership and placement
    -> cooldown
    -> measure again

sustained low pressure
    -> select one removal candidate
    -> transfer leases and active ranges
    -> verify durable replicas
    -> remove one node
    -> cooldown
```

Hysteresis, minimum and maximum cluster sizes, disruption budgets, and explicit safe-to-remove status are required. Autoscaling recommendations remain advisory until active-range replication and drain protocols make removal safe.

## Public mental model

The application developer should think in these terms:

```text
riverbed -> space -> feed -> key -> cursor -> subscription

feed
  -> immutable identity with a mutable dotted name
  -> ordered by key
  -> read through capacity-aware subscriptions
  -> progress represented by opaque cursors
  -> retries safe by default
```

The developer should not need to reason about:

- Partition counts
- Partition leaders
- Replica placement
- Consumer group generations
- Poll intervals
- Batch sizes
- Segment locations
- Data movement between NVMe and object storage

## Correctness contracts

The first prototype should define these contracts precisely before optimizing performance:

### Ordering

- Appends for the same stream and key are observed in accepted order.
- Reassignment or shard splitting cannot reorder committed records for a key.
- Ordering between unrelated keys is deliberately unspecified.

### Durability

- The chosen acknowledgement level defines the failure set a committed append survives.
- Sealed segments are immutable and content-addressed or checksummed.
- Recovery never exposes an append that the system has definitively rejected.

### Delivery

- Base delivery is at least once.
- Duplicate appends caused by producer retries are suppressed by producer identity and sequence.
- Atomic consume-and-append enables exactly-once effects within the system's boundary.

### Ownership

- Only the current lease epoch may acknowledge or commit work.
- Expired or superseded consumers are fenced.
- Lease reassignment affects only the relevant work ranges.

## Major engineering risks

### Hot keys

Per-key ordering means a single extremely hot key cannot be parallelized without relaxing its ordering contract. The system can isolate it, allocate more resources around it, and report it clearly, but cannot remove that fundamental limit.

### Cursor stability

Opaque cursors must survive shard splits, segment movement, retention, and restoration without becoming an unbounded metadata burden.

### Small-shard overhead

Millions of logical shards are useful only if their ownership, progress, and lease metadata are compact and efficiently aggregated.

### Object-store latency

Transparent tiering needs predictable prefetch, caching, and admission control so historical reads do not overwhelm active workloads.

### Deduplication state

Producer sequence tracking needs clear lifetime and recovery rules. Infinite deduplication history is impossible; sessions, epochs, or bounded windows will be required.

### Atomic commit scope

Atomic consume-and-append is straightforward only when its coordination boundary is tightly controlled. Cross-node output and state placement require careful protocol design.

## Suggested prototype sequence

### Phase 1: Three-Node Riverbed and local correctness

- Standard three-Node development Riverbed
- Append-only segmented log on each Node
- FeedId, dotted FeedName, keys, and Cursors
- Writer identities and sequence-based deduplication
- Key-ordered reads
- Subscription leases
- Atomic consume-and-append within one active range

### Phase 2: Replicated active ranges

- Minimum three active replicas
- Small Raft control plane
- Epoch-based ownership
- Quorum replication for active append ranges
- Node failure and recovery
- Incremental lease transfer

### Phase 3: Dynamic logical shards

- Large fixed logical-shard space
- Capacity-aware placement
- Online shard movement
- Hot-shard detection
- Shard split and generation-aware cursors

### Phase 4: Tiered storage

- Immutable sealed segments
- Object-store upload and verification
- Local cache and historical fetch
- Retention and deletion

### Phase 5: Production concerns

- Tenant isolation and quotas
- Authentication and authorization
- Observability
- Rolling upgrades
- Disaster recovery
- Compatibility testing and fault injection

## Questions to resolve next

1. What exactly is the atomicity boundary of consume-and-append?
2. How are keys mapped to logical shards, and how is that mapping versioned?
3. Can a cursor be compact, opaque, and stable without consulting permanent translation metadata?
4. What is the active-range replication protocol?
5. When may producer deduplication state expire safely?
6. How are leases represented and fenced without burdening the control plane?
7. Are subscriptions push-based, pull-based, or a credit-driven hybrid?
8. How are wildcard namespace subscriptions expanded efficiently?
9. Which guarantees remain possible when object storage is temporarily unavailable?
10. What minimum protocol surface is needed for a convincing benchmark prototype?

## Primary differentiator

The goal is not merely to build a faster Kafka. Redpanda and other systems already compete effectively on implementation efficiency.

The defining architectural advantage is:

> **Decouple ordering, storage, parallelism, replication, and consumer ownership so they are not all represented by the same partition.**

That change creates room for transparent elasticity, non-disruptive consumption, simpler clients, adaptive operation, and a much smaller application-facing model.

