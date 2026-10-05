# Why Whitewater

> Whitewater is FinnStream's clean-sheet distributed database and streaming platform: a self-managing, partitionless event fabric built around durable feeds, key-defined ordering, opaque cursors, first-class indexes, secure defaults, and incremental elasticity.

## Document status

- **Status:** Product motivation and terminology proposal
- **Product:** Whitewater
- **Company and package namespace:** FinnStream / `finnstream`
- **Architecture source:** [Whitewater architecture](kafka-successor-architecture.md)
- **Operational experience:** [Humane operations and day-two requirements](operational-experience.md)
- **Public model:** `fabric -> space -> feed -> key -> cursor -> subscription`
- **Implementation:** Rust

This document explains why Whitewater exists, how it differs from Kafka, and how familiar Kafka concepts translate into the Whitewater model. It distinguishes committed principles from areas that still require prototyping and benchmarking.

## A database and a streaming platform

Whitewater is designed as both systems from the beginning rather than a message broker with database behavior assembled around it:

```text
Feed         = immutable temporal database and replayable event history
Index        = persisted replicated current/queryable state
Subscription = continuous capacity-aware delivery
Pipe         = stateful or stateless streaming computation
Cursor       = stable opaque position in temporal history
```

The database and streaming responsibilities share FeedId, durability, security, placement, replication, observability, backup, and disaster-recovery contracts. Applications should not need to export every Feed into another database for basic key lookup, nor should database-style Indexes erase or redefine the underlying event history.

## What Kafka got right

Kafka established several ideas worth preserving:

- A durable append-only history is a powerful integration primitive.
- Writers and readers can be decoupled in time and deployment.
- Replay is more useful than destructive queue consumption for many systems.
- Sequential IO and immutable segments can deliver excellent throughput.
- Replication makes event infrastructure a system of record rather than transient plumbing.
- An ecosystem can grow around a small durable-log core.

Whitewater is not based on the claim that Kafka is useless or universally slow. It starts from the observation that Kafka's public and operational abstractions expose too much of its physical implementation and couple responsibilities that should evolve independently.

## Why build something new

### 1. Partitions became too many things

A Kafka partition is simultaneously:

```text
ordering boundary
+ throughput unit
+ storage unit
+ replication unit
+ leader-election unit
+ consumer-assignment unit
+ scaling decision
```

That coupling forces application teams to make permanent infrastructure decisions before they know future traffic. Increasing partition count changes ordering and key distribution. Reducing it is not a normal operation. Hot partitions remain hot even when the rest of a cluster is idle.

Whitewater keeps only the useful public guarantee:

```text
Events with the same key are observed in accepted order.
Unrelated keys may proceed independently.
```

Physical ranges, replicas, files, and placement are internal. They may split, merge, or move without changing a Feed or its client contract. The [partition-versus-range comparison](#kafka-partitions-versus-whitewater-active-ranges) below explains the mechanism and current implementation boundaries.

### 2. Partition counts leak infrastructure into application design

Kafka clients and operators must understand partition counts, leaders, replicas, assignment, and partition-local offsets. Capacity planning becomes part of topic creation, and changing capacity can alter behavior.

A Whitewater Writer selects a Feed and key. A Reader selects a Feed or Feed pattern. The Fabric decides placement and parallelism.

### 3. Consumer-group rebalancing interrupts unrelated work

Kafka group membership changes can trigger coordinated reassignments. Cooperative protocols improve this, but applications still reason about group generations, poll intervals, session timeouts, assignment callbacks, and revoked partitions.

Whitewater uses short, epoch-fenced leases over small internal work ranges. A failed Reader loses only its leases. Other Readers continue and acquire abandoned work incrementally. Joining capacity causes gradual lease transfer, not a global stop.

### 4. Offsets expose physical layout

A Kafka offset identifies a position inside one physical partition. Applications store topic, partition, and offset tuples, so physical topology becomes part of their state.

Whitewater returns an opaque Cursor. A Cursor may encode Feed identity, range generation, segment, and record position, but clients do not interpret it. This leaves Whitewater free to reorganize storage while retaining a stable read contract.

### 5. Configuration interactions are difficult to reason about

Kafka exposes many individually defensible settings whose combinations define correctness and availability:

- `acks`
- replication factor
- `min.insync.replicas`
- producer retries and idempotence
- in-flight request limits
- delivery timeouts
- consumer poll and session timeouts
- fetch byte and wait settings
- partition assignment strategies
- segment and retention settings
- cleanup policy
- controller, broker, listener, and security settings

The result is not merely a large configuration file. It is a large correctness state space where apparently harmless changes can weaken durability, duplicate work, interrupt consumption, or prevent progress.

Whitewater chooses safe defaults as protocol behavior. Expert controls may exist, but basic durability, retry safety, backpressure, and membership must not depend on assembling the right combination of dozens of settings.

### 6. Broker, controller, and KRaft concerns leak into operation

Kafka has evolved from ZooKeeper coordination to KRaft, with brokers, controllers, process roles, listener matrices, quorum configuration, and migration history. KRaft is a substantial improvement, but operators still manage topology that applications should not need to understand.

Whitewater runs one Node binary. Nodes may hold control-plane, data-plane, or mixed responsibilities internally, but role placement is reconciled by the Fabric. An orchestrator manages container count; it does not define Whitewater's logical data model.

### 7. Elasticity is not transparent

Adding Kafka brokers does not automatically redistribute existing partitions or remove hot spots. Removing brokers requires reassignment. Partition placement and data movement are explicit operational projects.

Whitewater separates Feed identity, key ordering, active-range ownership, immutable history, and compute placement. Scaling changes capacity one Node at a time. The control plane incrementally moves internal ranges and leases. Scale-in is permitted only after a Node is drained and its durability obligations are satisfied.

### 8. Cleanup policy mixes event history with materialized state

Kafka asks a topic to choose deletion, compaction, or both. Compaction can approximate latest-value state for a key, while Kafka Streams applications commonly maintain local state stores and changelog topics. Whitewater separates these responsibilities instead of offering compacted versus non-compacted *Spaces*.

**A Space is an ownership/policy namespace, not a cleanup-policy type.** There is one public **Feed** history model: committed events remain immutable until a History Policy expires or tiers them (the closest Kafka analogy is a delete-retained topic). A Space may contain many Feeds and Indexes and supply inherited quotas, access, retention, and accounting. Neither a Space nor a Feed changes into a compacted history when an Index is added. History Policy, tiering, and expiry are design requirements, not a completed retention implementation.

A **persisted Index layer is core Whitewater storage**, not a later performance trick. It holds current/queryable state derived from Feed history; applications do not have to operate separate compacted Feeds or changelog topics to make their state durable. An Index is a deliberate, named resource because each extra application-defined field/index costs write IO, replication bandwidth, disk, memory, and rebuild time. Not every Feed needs an Index, but the platform must support arbitrary declared secondary indexes as a first-class capability.

### 9. Current state and secondary lookups belong to persisted Indexes

For example, one Feed can support several application-defined access paths without changing its immutable history:

```text
Space:   commerce
Feed:    commerce.orders.events        (committed append-only history)
Primary: FeedId + order Key            (latest derived state for that Key)
Index:   commerce.orders.bycustomer    (customer_id -> order Keys)
Index:   commerce.orders.byregiondate  ((region, date) -> order Keys)
```

Primary **Feed records** still live in the replicated sequential Active Range log; each record has a stable MessageId/Cursor and an internal range position. The derived **current-state primary row** is keyed by `FeedId + application Key`, not a public Kafka partition/offset. For example, a `users` projection keyed by `userId` can have a shared secondary Index whose entries are `(city IndexId, "London", userId) -> primary reference`; an exact city-prefix scan returns every London user's current row. Changing a user's city deletes the old city posting and inserts the new one in the same local Index transaction. Secondary entries point to this stable logical identity (or to a stable MessageId for immutable-event indexes), never to a range position that might change after a split. Schema-neutral byte payloads require a declared, deterministic, versioned extractor before indexing a payload field; arbitrary application-specific indexes cannot imply that Whitewater understands every payload automatically.

The target Index is persisted, RF3-replicated, independently recoverable, checkpointed by applied Cursor, and queryable by exact value, text prefix, composite leading fields, or bounded range without scanning a Feed. These are **required storage behavior**, not guarantees already implemented in the current prototype. Index creation must work through the same typed Control API/WCL as other resources; see the [Index storage contract](#index-storage-contract-and-fjall-layout) below.

### 10. Kafka lacks general first-class indexing

Kafka efficiently retrieves sequential records by partition offset and time indexes, but general lookup by business key or another field typically needs a separate store, Kafka Streams state, ksqlDB, search, or application-maintained projections. Whitewater keeps immutable Feed history while making separately named primary/current-state and secondary Indexes a core query model. An Index's applied Cursor and policy must tell the caller how fresh it is; a strict read must wait or report lag rather than silently returning stale state.

### 11. Changelog plus reconstructed store duplicates responsibility

Kafka Streams commonly uses a local store backed by a changelog topic, which applications and their framework must restore. Whitewater's intended recovery source is the **retained Feed history plus replicated Index checkpoints**, so applications do not create a second public changelog Feed to maintain the Index. An Index can catch up from its last applied Cursor; if its persisted derived state is discarded, a controlled rebuild reads the owning Feed(s) in a new Index generation and publishes the replacement only after it is verified and caught up.

A Space-scoped `REBUILD INDEXES IN SPACE commerce` would select Indexes owned by that Space and replay their source Feeds; it would **not** erase the Space, clear Feed records, or mean that every Index is rebuildable from data that has already expired. If retained history and a usable checkpoint/backup are insufficient, the rebuild must refuse with an explanation rather than report success from incomplete state. Reset/empty operations require authorization, an explicit retained-history check, audit, and a shadow build instead of exposing an empty live Index.

A Space owns Feeds and StateStores; it has no single log of its own. A **Feed-derived** StateStore replays its explicitly named source Feed(s) with versioned deterministic extraction. A **manual** StateStore instead needs a Whitewater-managed replicated mutation journal and checkpoint: manual `PUT`/`DELETE` operations cannot be recreated from unrelated Feeds. The Control Plane can currently persist a `declared` manual or same-Space Feed-derived StateStore definition, but it cannot yet accept StateStore writes, replicate Fjall state, or serve lookups.

The intended enrichment experience is topology-free: a Reader obtains an event from `activity.events`, looks up `users[userId]` at a recorded state version through any Node, and writes to `activity.enriched` with an atomic input-progress/output effect. An internal owner may fetch state remotely or optimize placement; application code supplies no range, owner, partition count, or co-partitioning plan. An unavailable/lagging user store must not silently produce an unenriched output. This Pipe/runtime workflow and its cross-Node consistency tests remain planned, not implemented.

### 12. Security has too many optional paths

Kafka can be deployed securely, but it can also expose plaintext listeners, and operators choose among TLS, mTLS, SASL mechanisms, JAAS configuration, ACL systems, and external identity integrations.

Whitewater's baseline is intentionally narrow:

- All client and inter-Node traffic is encrypted in transit.
- There is no plaintext production listener.
- Client authentication uses scoped API keys over TLS.
- API keys map to identities and capabilities at Space and Feed boundaries.
- Keys are stored as one-way verifiers; plaintext credentials are not persisted.
- Rotation and overlapping validity are standard operations.
- Audit events are part of the control plane.

An API key must not be used directly as an encryption key. If a Feed requires a separate encryption boundary, its Encryption Policy references a Key ID. Data is encrypted with a data-encryption key, and that key is wrapped by a key-encryption key supplied by a KMS, HSM, or customer key provider. A separate scoped API key may authorize access to the Feed, but authentication material and encryption material remain distinct.

### 13. Safe durability should not be optional

Whitewater requires at least three Nodes. Active data has at least three replicas across distinct eligible Nodes, and normal durable acknowledgement requires a quorum.

```text
minimum Fabric size:     3 Nodes
minimum active replicas: 3
normal write quorum:      2 of 3
```

These are baseline invariants rather than per-Feed tuning. A future Durability Policy may request more replicas or geographic copies, but not fewer than the safe baseline.

Development also runs three Nodes. Three containers on one laptop validate topology and failure logic but do not create three physical failure domains. Production placement must spread replicas across machines and, where available, zones.

A single-Node mode may exist only as a storage-engine unit-test harness. It is not a supported Fabric deployment and must not silently present production durability semantics.

### 14. Renaming should not rewrite data

A Kafka topic name is its identity. Renaming normally means creating another topic and copying records, then migrating clients, ACLs, schemas, connectors, and consumer state.

Whitewater separates identity from naming:

```text
FeedId:   immutable system identity
FeedName: mutable human-readable alias
```

A rename updates metadata atomically. Records, Cursors, Subscriptions, Pipes, Indexes, policies, schemas, and placement remain attached to the FeedId.

Feed names use lowercase dotted notation with a maximum encoded length of 512 bytes:

```regex
^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$
```

Examples:

```text
commerce.orders.created
payments.authorisations.requested
identity.customers.updated
region1.telemetry.received
```

Every segment begins with a lowercase ASCII letter. Numbers may appear after the first character. There are no empty segments, leading dots, trailing dots, uppercase letters, spaces, hyphens, or underscores.

Rename aliases may optionally remain as time-bounded redirects, but an old name must never be silently reassigned while a redirect or client reference remains valid.

### 15. JVM costs are not the same as saying Java is slow

Kafka demonstrates that Java can sustain excellent throughput. Whitewater chooses Rust for different reasons:

- Predictable tail latency without garbage-collection pauses.
- Lower baseline memory overhead per Node.
- Memory safety without a managed runtime.
- Efficient binary protocols and zero-copy opportunities.
- One native artifact with fewer runtime layers.
- Fine-grained control over IO, allocation, caching, and backpressure.
- Safer concurrency for a storage and consensus implementation.

Rust does not automatically make Whitewater faster. Algorithms, disk layout, replication, batching, syscalls, and failure handling matter more than language marketing. Every performance claim requires reproducible benchmarks.

### 16. Local development should resemble production

Kafka development environments often use weaker replication, fewer controllers, different listeners, and different security than production. Problems then emerge only after deployment.

Whitewater's standard development Fabric uses three Nodes, TLS, API-key authentication, membership, replication topology, and the same protocol as production. Local tooling may automate certificates and credentials, but it must not replace the architecture with a different single-Node product.

### 17. Schemas should be first-class but not mandatory in storage

Kafka's core stores bytes, while schemas generally live in a separately deployed registry. Whitewater also keeps Feed storage schema-neutral, but schema metadata, compatibility policy, validation, and generated clients can be integrated services under the same control plane and identity model.

A Feed can accept arbitrary bytes or reference a Schema Policy. Schema enforcement is explicit and does not force JSON, Avro, or Protobuf into the storage engine.

### 18. Multi-tenancy and isolation should not be retrofits

Whitewater Spaces are policy and accounting boundaries from the beginning. CPU, memory, local IO, object-store IO, network bandwidth, connection count, Feed count, Index cost, and retained bytes must be attributable to a Space.

Schedulers protect small workloads from noisy neighbours and prevent one hot Feed or key from consuming an entire Fabric unnoticed.

### 19. Operational feedback should explain causes, not just lag

Kafka lag is useful but insufficient. Whitewater should identify whether delay originates from:

- Writer ingress
- Quorum replication
- Storage flush
- Index application
- Reader capacity
- Lease availability
- Object-store retrieval
- A hot key
- Quota enforcement
- A failed or draining Node

Capacity-aware subscriptions and protocol credits become control inputs, while metrics explain the resulting behavior.

## Whitewater foundations

### Fabric

A Fabric is one cooperating Whitewater installation. It has a stable FabricId, a control-plane quorum, at least three Nodes, and one security and policy domain.

### Node

A Node is a replaceable process or container contributing storage, network, CPU, and optionally control-plane capacity. Applications do not address a specific Node for normal Feed operations.

### Space

A Space is a hierarchical administrative boundary for ownership, policy inheritance, quotas, authorization, schemas, encryption, and billing. Dotted Feed names naturally project into Space prefixes.

### Feed

A Feed is immutable, replayable event history with an immutable FeedId and mutable dotted FeedName. It has no public partitions and no cleanup-policy mode.

### Key

The key defines ordering. Events accepted for the same FeedId and key are observed in accepted order. Ordering between unrelated keys is deliberately unspecified so they can move and execute independently.

### Metadata

Metadata is the optional record-attached map of names to arbitrary bytes. It replaces Kafka record-header terminology and carries tracing, content description, provenance, routing hints, and application annotations without changing the payload. Metadata does not define ordering and must remain bounded by protocol limits.

### Record time

Every record stores `event_time_ns` and `ingest_time_ns` as signed 64-bit Unix epoch nanoseconds. Event time preserves Writer/source precision; ingest time records Whitewater acceptance. Keeping both avoids Kafka's CreateTime-versus-LogAppendTime replacement choice. JSON APIs encode these values as decimal strings to preserve exact 64-bit values in JavaScript.

### Cursor

A Cursor is an opaque, Feed-scoped position. Clients store, commit, and return it but do not calculate with its internal fields. Each standalone Reader owns an independent current Cursor and may seek without affecting anyone else.

### Subscription

A Subscription describes durable read intent, progress, filtering, and delivery policy. Every Subscription has its own durable acknowledged Cursor, independent from every other Subscription. Readers cooperating in one Subscription receive epoch-fenced leases over internal ranges and share that Subscription's progress; membership changes do not globally rebalance all work.

### Pipe

A Pipe is managed processing from one or more Feeds into Feeds and Indexes. Atomic consume-and-append advances input progress and commits output effects together within a defined boundary.

### Index

An Index is a named, persisted, replicated projection over a Feed or Pipe output. A Key Index replaces the common compacted-topic/KTable/latest-value use case without changing Feed history semantics.

### StateStore

A StateStore is a named current-state resource owned by a Space, with an application primary Key and optional declared secondary Indexes. Its source is either specified Feed history (processed by a deterministic versioned projection) or manually submitted mutations recorded in a Whitewater-managed replicated journal. It is logically queryable from any Node without co-partitioning; ownership, placement, replay, and Index storage remain internal. The current Control Plane only persists `declared` definitions; it does not yet replicate or serve StateStore data.

### History Policy

A History Policy controls hot local duration, tiering, legal retention, archival, and eventual expiry. It does not switch a Feed between deletion and compaction personalities.

### Durability Policy

The baseline is three replicas and quorum acknowledgement. Stronger policies may add replicas, zones, or regions. Applications do not tune `acks`, ISR thresholds, and replication factor independently.

### Encryption Policy

An Encryption Policy specifies transport requirements, at-rest encryption, customer-managed key references, and rotation. Authentication API keys authorize access but are not encryption keys.

## Kafka partitions versus Whitewater Active Ranges

**A range is not a renamed partition.** Both systems use replicated, ordered storage internally, but they put different responsibilities into their public contracts. Kafka exposes a topic's partitions and partition-local offsets to applications; Whitewater presents a Feed and business keys while keeping its Active Ranges, owners, replicas, generations, and positions internal. The distinction is about what an application must depend on, not a claim that Whitewater has no physical divisions.

| Question | Kafka partition | Whitewater Active Range |
|---|---|---|
| What does it cover? | One numbered append log within a topic; a producer's partitioner chooses its destination. | One contiguous interval of a Feed's **hashed key-token space**, such as `[start, end)`. |
| What do clients name? | Topic and, when selecting work or seeking, often partition and offset. A keyed producer can normally let a partitioner choose. | Feed and Key for writes; Feed and opaque Cursor for reads. No RangeId, token, or replica is required in application requests. |
| What is ordered? | Records within each partition; there is no topic-wide total order across partitions. | The intended public guarantee is accepted order for the **same Feed and Key**; unrelated keys need not have a shared order. |
| What scales independently? | Partitions provide append and consumer parallelism but also define physical offsets, leader/replica placement, and consumer assignments. | Different ranges can have different append owners. Reader/Subscription work assignment is designed to be independent of storage placement; shared Subscription leases are still roadmap work. |
| Who chooses the number? | Operators commonly choose a partition count at topic creation; Kafka supports increasing it, but reducing it is not a routine in-place operation. | Applications never choose a range count. The Fabric starts with one full-keyspace route and may change its internal map as capacity needs change. |
| How does placement change? | Leaders and replicas can move without changing a partition ID; reassignment and count changes remain operational concerns. | The Control Plane tracks range assignments and epochs. Splits/merges change routes; moving replicas or owners changes placement without redefining the Feed. |

### A concrete routing example

A new Feed starts with one internal route covering all 128-bit KeyTokens. Whitewater hashes the **key bytes** with BLAKE3 and uses the first 128 bits of the digest to locate the key's route in the current RangeMap. The same key hashes to the same token, irrespective of which Node receives the append. The map is validated to cover the full keyspace without gaps or overlaps.

The numbers below are **illustrative**, not actual hashes or application-visible identifiers:

```text
Before:  [0, end)       -> Range A -> owner Node 1

After:   [0, 50)        -> Range A -> owner Node 1
         [50, end)      -> Range B -> owner Node 2

hash(order-123) = 27    -> Range A
hash(order-456) = 83    -> Range B
```

Intervals are half-open: a token exactly equal to `50` belongs to the right-hand range. Ranges describe **which keys route together**, not a time interval, payload prefix, or public slice of a Feed's record offsets. A range has an internal RangeId, generation, append owner, RF3 replica assignment, CommitPosition, storage files, and Writer deduplication state. Feed creation does not ask for any of these.

### Ordering and parallelism: an important difference

Kafka preserves append order **within one partition**; two distinct keys sent to that same partition share that log's order, even if the application does not need an ordering relationship between them. Kafka's default key-based partitioning normally keeps a key on one partition for a stable partition count, but changing the count or partitioner can change where subsequent records for that key go. Applications that rely on per-key history across such changes must account for that behavior.

Whitewater makes the same-Feed/same-Key accepted order the public contract. At any one map generation a key's token maps to exactly one Active Range. Internal splits, merges, and placement changes are intended to preserve that key's order by staging committed history, fencing the old assignment, and changing routing through the Control Plane. Unrelated keys may be processed on different owners; **there is no promised total order over the entire Feed**. In particular, two records accepted on different ranges are not assigned a single public offset sequence. Adding Nodes also does not parallelize one strictly ordered hot key.

The current prototype merges locally available committed range histories for Feed reads using ingest time and MessageId as a presentation order. That is **not** a durable, globally sequenced Feed log or a cross-range ordering guarantee. Readers should use the returned Cursor as an opaque continuation token, not sort by ingest time or infer a range position.

### What changes during a split, merge, or move?

- **Split:** One key-token interval becomes two adjacent intervals. Whitewater stages the committed source history into both candidate generations, filters by key, checks RF3 evidence, briefly freezes the source for the final boundary, then activates the candidate RangeMap through consensus. The split gives unrelated keys opportunities to use separate owners; it does not divide one key.
- **Merge:** Two adjacent cold intervals become one. A candidate generation combines their committed histories and Writer state before the one-range map is activated. A merge reduces internal overhead; it does not rewrite the Feed's public identity.
- **Move:** A range can retain its key bounds and RangeId while its replica/owner assignment changes. Authenticated follower replacement and four-Node development acceptance now preserve the owner and Cursors while changing one RF3 follower. Append Owner movement has an authenticated remote development workflow and isolated three-Node acceptance: the candidate must verify the frozen committed boundary before higher-epoch Control Plane cutover. Scalable large-history verification, automatic draining, and production-grade inter-Node identity/encryption are **not yet complete**. Direct metadata-only transfer is refused.

A split or merge can change internal physical positions and generations; Writer sequencing must be reconstructed or scoped internally. A movement can advance an ownership epoch to fence stale replicas or owners. Clients continue addressing the same Feed and Key and retaining their original Cursors. A brief freeze can make an append retryable; the client should retry with the same request identity rather than inventing a new logical record.

### Cursors, readers, and today's limits

A Kafka consumer commonly tracks `(topic, partition, offset)`. A Whitewater Reader instead receives and returns a Cursor associated with a committed record; the client never has to derive a new physical position after a split. The split/merge tests exercise preservation of record-attached Cursors through generation changes, and named Reader sessions track delivered progress separately from acknowledged progress.

This is an **early implementation**, not yet a claim of unlimited cross-Node continuation: the current multi-range read path requires **every current range** to be available on the ingress Node, scans up to 10,000 committed records per range, merges that local data, and finds the supplied Cursor in the result. If any range is placed elsewhere, the Node fails the Feed read as retryable rather than returning a misleading partial Feed. It does not yet fetch remote histories or offer a scalable global read index; a Cursor beyond the scanned prefix may still be reported as unknown. Full cross-Node, long-history Cursor continuity remains work; an opaque Cursor is the API shape, not proof that every planned migration and retention scenario is already implemented.

### Operator view versus developer view

Developers should not choose a range count, assign a Reader to a range, handle partition rebalances, or react to range-movement callbacks. Operators can inspect internal placement and in-progress topology plans to diagnose why a Feed is busy or a move is deferred; that inspection is **not** part of the application contract. Whitewater must eventually automate cold/hot placement, bounded movement, failure-domain separation, and safe Node drain rather than merely hiding the corresponding Kafka responsibilities. See [Milestone 4 and its evidence](tasks.md#milestone-4--multiple-internal-ranges) for what is currently implemented, in progress, or planned.

## Kafka-to-Whitewater equivalents

| Kafka concept | Whitewater concept | Difference |
|---|---|---|
| Kafka cluster | Fabric | Self-managing installation with a stable FabricId and minimum three Nodes |
| Broker | Node | Replaceable capacity; applications do not target storage owners |
| KRaft controller | Control plane | Small consensus scope for identity, ownership, epochs, membership, and recovery decisions |
| Topic | Feed | Immutable history with an immutable FeedId and mutable dotted name |
| Topic name | FeedName | Mutable alias; rename does not copy records or reset consumers |
| Topic UUID | FeedId | Primary identity used by Cursors, policies, Indexes, and Subscriptions |
| Partition | No public equivalent | Internal ranges are invisible and may split, merge, or move |
| Partition leader | Active-range owner | Temporary epoch-fenced placement hidden from clients |
| Record key | Key | Explicit ordering identity rather than only a partitioning hint |
| Record headers | Metadata | Optional bounded key/value bytes for tracing, provenance, content description, and annotations |
| Offset | Cursor | Opaque and stable across internal topology evolution |
| Producer | Writer | Appends by Feed and key without partition selection |
| Idempotent producer | Every Writer | Identity and sequence deduplication are protocol defaults |
| Consumer | Reader | Receives work according to advertised capacity |
| Consumer group | Subscription | Durable read intent using incremental leases rather than global generations |
| Group rebalance | Lease transfer | Only affected internal ranges move; unrelated work continues |
| Group generation | Lease epoch | Fences stale Readers at the smallest practical ownership boundary |
| Consumer lag | Cursor distance and delay breakdown | Reports storage, replication, Index, delivery, and processing causes |
| Bootstrap servers | Fabric endpoint/discovery | Clients discover healthy Nodes and need no broker topology knowledge |
| Replication factor | Durability Policy | Minimum three replicas; no unsafe lower setting |
| `acks` | Durability Policy | Safe quorum behavior is standard rather than a producer tuning choice |
| ISR | Replica health | Internal operational state, not an application configuration concern |
| `min.insync.replicas` | Quorum invariant | Derived from the Durability Policy |
| Topic `cleanup.policy=delete` | Feed plus History Policy | Immutable history expires or tiers according to lifecycle policy |
| Topic `cleanup.policy=compact` | Feed plus Key Index | Current state is a persisted replicated Index; history remains history |
| Tombstone | Explicit Index deletion mutation | Deletes indexed state without redefining Feed storage semantics |
| Log segment | Immutable segment | Internal storage chunk that may move to object storage |
| Retention bytes/time | History Policy | Inherited through Spaces and coordinated with tiering and legal rules |
| Kafka Streams state store | Index or Pipe state | Persisted and replicated first-class state, with memory as a cache |
| Changelog topic | Index replication/checkpoint history | Recovery uses replicated checkpoints plus Feed tail rather than full replay by default |
| KTable | Key Index | Direct latest-value view with known freshness Cursor |
| Transactional producer | Atomic consume-and-append | Narrow, streaming-focused atomic primitive |
| Kafka Connect | Adapter/Gateway | Integration runs outside the storage core through stable Feed APIs |
| Schema Registry | Schema service | Integrated identity and policy model but optional for byte storage |
| ACL | Space/Feed capability policy | Scoped API-key identity with inherited policy |
| SASL mechanisms | API-key authentication over TLS | One baseline client authentication model |
| SSL/plaintext listeners | TLS-only listener | No production plaintext mode |
| Quotas | Space resource policy | Accounts for CPU, IO, storage, object retrieval, Indexes, and network |
| Rack awareness | Failure-domain placement | Mandatory replica separation where infrastructure exposes domains |
| Tiered storage | Transparent segment lifecycle | Clients keep Feed/Cursor semantics while storage location changes |
| AdminClient | Control API and `wwctl` | Operates on logical resources, not partitions and broker assignments |

## Index storage contract and Fjall layout

**Status:** This is the required storage architecture. `src/index.rs` now includes the versioned key encoding, mutation planner, and an isolated **local Fjall 3.1.10 storage prototype**: three shared keyspaces, serializable single-writer transactions for current-state rows and postings, exact secondary lookup, local uniqueness checks, durable commits, and restart tests. The production-path Active Range store is still a separate Rust file/segment log; there is no RF3-replicated Index, global uniqueness, public `CREATE INDEX`, or atomic Feed-to-Index commit. Engine selection and live integration still require workload and fault evidence before the public API claims those guarantees.

### A small fixed set of engine keyspaces

Fjall is the pure-Rust embedded LSM candidate closest to RocksDB in this design. It supports ordered prefix/range iteration and cross-keyspace atomic operations, but does **not** automatically maintain relational secondary indexes. Whitewater owns the Index catalog, extractors, encoded keys, transactional maintenance, and replay protocol. A Fjall keyspace is a physical LSM tree, so the design uses a **small fixed set** of keyspaces per local Index replica/engine instance, **not one keyspace per user-defined Index**:

```text
index_primary      [v1][feed_id:16][escaped application_key][00 00] -> current projection + prior indexed tuples + applied Cursor
index_entries      [v1][posting_tag][index_id:16][typed composite tuple][encoded primary Key] -> empty / stable reference
index_entries      [v1][unique_tag][index_id:16][typed composite tuple] -> encoded primary Key
index_checkpoints  [index_id:16][generation] -> source FeedId, applied Cursor, durability/freshness/build state
```

The shared `index_entries` keyspace clusters entries for a given `(tag, IndexId, value)` so an exact-match lookup scans only a bounded prefix. The final stable logical primary reference disambiguates duplicate values and permits all matching rows. A text-prefix lookup omits the terminator for the final text component; a composite Index can scan by its leftmost complete fields and then a typed range on the next field. Versioned encoding, type tags, signed-integer normalization, escaped zero bytes, explicit terminators, size limits, and declared text collation prevent ambiguous keys and accidental cross-Index scans. Null uniqueness semantics are explicit: multiple rows may carry null without taking a unique-value claim.

`src/index.rs` implements `IndexId`, `PrimaryRef`, `IndexField`, `IndexDefinition`, typed `IndexValue`, primary/posting/unique-claim encoding, prefix and signed-range starts, `plan_projection_change`, and `FjallIndexStore`. Its tests cover composite order, duplicates, embedded zero bytes, stale-entry removals on update/delete, nullable unique claims, conflicting local owner claims, bounded keys, transactional local upsert/delete, restart, and exact lookup. These are **local prototype tests**, not RF3 or cross-engine commit evidence. A stable MessageId reference, rather than a physical `(RangeId, RangePosition)`, is the planned variant for secondary indexes over *immutable events*. Internal range position or Kafka-style partition/offset may help locate bytes locally but is not a durable application-visible primary identity.

### Transaction and uniqueness boundary

For an update/delete of one current-state primary row, a **single serializable transaction** must read its old indexed tuples, remove old posting/unique-claim entries, check all new unique claims, write the new primary row and postings, and advance the Index applied Cursor/checkpoint together. A plain atomic `WriteBatch` across Fjall keyspaces is useful for applying *known* changes but is not enough for concurrent read-check-write uniqueness; evaluate a supported transactional Fjall database (single-writer or optimistic) with the intended IO and crash workload. Never acknowledge a strict Index mutation merely because a key was inserted in an unflushed cache; specify the durable flush/replica evidence separately.

**One local Fjall transaction cannot atomically commit the existing independent Active Range file log or a transaction on another Node.** To offer synchronous append-plus-Index consistency, Whitewater must extend the replicated append/commit boundary to include deterministic Index mutation intents and quorum-durable Index application (or replace/co-design the local log and Index journal with one recoverable transaction). On replay the same MessageId/Cursor must apply at most once. For asynchronous Index policies, writes may commit before the Index; strict queries must fence on the requested applied Cursor or return an explicit `index_behind` result. Until that cross-store protocol is implemented and tested, do **not** expose an Index as transactionally current or enable a misleading `CREATE INDEX` path.

A unique value spanning different Active Ranges additionally needs **global ownership** of `(IndexId, encoded value)` and a replicated conditional reservation/commit protocol; separate local serializable transactions cannot enforce global uniqueness. Fail creation of a globally unique Index until this is proven; do not quietly reinterpret it as per-range uniqueness. Composite fields, repeated/duplicate values on different primary keys, null handling, versioned extractors, collation, and Index generation are part of the definition, so changes require validation and often a rebuild.

### Control API, inspection, and safe rebuild

The **proposed, not yet accepted** WCL shape (mirrored by typed Admin API commands) is:

```sql
CREATE INDEX commerce.orders.bycustomer
  ON commerce.orders.events (customer_id) NONUNIQUE;
CREATE UNIQUE INDEX commerce.orders.byexternalid
  ON commerce.orders.events (external_id);
INSPECT INDEX commerce.orders.bycustomer;
REBUILD INDEX commerce.orders.bycustomer FROM FEED commerce.orders.events;
REBUILD INDEXES IN SPACE commerce;
```

An Index definition names the immutable FeedId, fields/extractor versions, types/collation, uniqueness, and build/consistency policy. A new Index starts in `building` with a pinned source Cursor and **shadow generation**, scans retained Feed history in bounded batches, catches up with live writes, verifies its primary/posting counts and unique claims on RF3, then switches the catalog's active generation through the Control Plane. Existing queries remain on the old generation until the new one is ready. Any proposed `RESET/EMPTY INDEX` must be implemented as an authorized rebuild that **never makes the live Index silently empty**. A failed build can be resumed or abandoned without changing Feed history; if the source's retained history is insufficient, require an appropriate checkpoint/backup or reject the rebuild. Operators should inspect applied Cursor, lag, build progress, cost, and reason for refusal by logical Space/Feed/Index.

### Proof required before claiming the feature

- Validate per-Index definition/extractor compatibility and deterministic extraction without assuming payload bytes have a schema; compare Fjall against redb and RocksDB on sustained mixed append/read, many declared Indexes, compaction stalls, memory, replication, and recovery cost.
- Prove transaction rollback after a crash at each primary/posting/unique/checkpoint boundary; stale postings never survive update/delete or an interrupted rebuild.
- Prove concurrent unique conflicts, idempotent ambiguous retries, multiple nulls, duplicate nonunique values, composite/prefix/range ordering, and split/merge/move of the underlying Feed without changing logical references.
- Prove RF3 Index catch-up, applied-Cursor fencing for strict reads, restart/checkpoint recovery, retention-blocked rebuilds, and bounded per-Space resource usage. Mark the runtime feature complete only after these tests and the public API pass.

## Index engine direction

Whitewater should define an internal Index Engine trait before selecting one implementation. Required capabilities include:

- Durable write batches
- Crash recovery
- Prefix and range iteration
- A small fixed set of isolated keyspaces shared by many application-defined Indexes, not one LSM tree per Index
- Snapshots or MVCC
- Checksums
- Explicit fsync/persistence control
- Bounded write amplification
- Background maintenance and compaction control
- Backup/checkpoint creation
- Efficient replication snapshot transfer
- Metrics for cache, stalls, compaction, and disk usage
- Stable behavior under sustained mixed append/read load

Candidates for benchmarking:

| Candidate | Characteristics | Main concern |
|---|---|---|
| RocksDB via Rust bindings | Mature LSM, column families, broad operational history | C++ dependency, build complexity, FFI, large tuning surface |
| Fjall | Pure safe Rust LSM, keyspaces, transactions, compression, range/prefix scans | Younger operational history; durability modes and long-running workloads need validation |
| redb | Pure Rust ACID embedded KV using copy-on-write B+ trees | Different write-amplification and large sustained-ingest characteristics from an LSM |
| Purpose-built Whitewater engine | Exact replication/checkpoint integration and minimal surface | Highest engineering and correctness cost; should not be the first assumption |

The preferred outcome is a pure-Rust engine, with Fjall and redb treated as serious first candidates rather than secondary fallbacks. This keeps the deployment, memory-safety, debugging, and build story aligned with Whitewater's Rust core.

The selection must still follow benchmarks and fault tests using Whitewater's real index workload. “Written in Rust” is valuable but not sufficient to outweigh recovery correctness and operational evidence.

## Additional improvements to pursue

### Data and processing

- Online internal-range split, merge, and movement without client-visible changes
- Hot-key isolation and explicit hot-key diagnostics
- Atomic consume-and-append with optional co-located state mutations
- Built-in delayed delivery and retry schedules without retry-Feed conventions
- Dead-letter handling as a Subscription policy rather than naming conventions
- Point-in-time Index snapshots
- Feed branching for test and replay environments
- Server-side filtering with cost controls and Index-aware planning
- Wildcard subscriptions over dotted Feed namespaces
- Consistent snapshots across selected Feeds where explicitly requested

### Storage

- Replicated active append ranges on local NVMe
- Immutable checksummed sealed segments
- Transparent object-store tiering
- Predictive prefetch for historical reads
- Content verification and repair
- Per-Space storage accounting
- Online disk replacement and cache rebalancing
- Background maintenance that yields to foreground latency targets

### Clients and protocol

- Credit-based delivery with explicit message, byte, and latency budgets
- Adaptive batching, compression, and concurrency
- Protocol-native idempotency
- Server-advertised backoff and overload signals
- Small binary protocol with negotiated extensions
- Generated clients from optional schemas
- First-class Rust, Python, Java, C#, Node.js/TypeScript, and Go clients with the same [Whitewater Streams semantics and shared conformance suite](streams-clients.md)
- Evaluate C/C++ and other clients after a stable protocol/ABI and demonstrated demand, without weakening core language parity
- Connection migration without application-visible ownership events

### Operations

- One Node binary with automatic internal role placement
- Rolling upgrades with protocol/version fencing
- Slow hysteretic autoscaling through Docker, Kubernetes, Nomad, ECS, or a webhook actuator
- Drain-before-remove guarantees
- Failure-domain-aware placement
- Continuous replica verification and self-healing
- Built-in fault injection for development Fabrics
- Explainable pressure and placement decisions
- Deterministic configuration snapshots and audit history
- No unsafe production configuration combinations

### Security

- TLS-only client and Node communication
- Short, scoped, rotatable API credentials
- Capability inheritance through Spaces
- Customer-managed envelope-encryption keys
- Feed- and Index-specific encryption boundaries
- Full control-plane audit Feed
- Rate limits and anomaly detection per identity
- Secret-free logs and diagnostics
- Automated certificate and key rotation

### Developer experience

- Three-Node development Fabric by default
- One command to start, inspect, test failure, and reset
- Feed names that can be renamed without migration
- No partition-count decision during creation
- No exposed offsets, leaders, ISR, or assignment callbacks
- Human-readable diagnostics for durability, pressure, and delayed delivery
- Built-in replay and deterministic test fixtures
- Local tooling that exercises the same protocol and topology as production

## Decisions versus open questions

### Directional decisions

- Product name is Whitewater by FinnStream.
- Public resources are Fabric, Space, Feed, Key, Cursor, Subscription, Pipe, Index, and Node.
- Feed identity is immutable and separate from mutable dotted naming.
- Keys define ordering.
- Partitions are not public.
- Minimum supported Fabric size is three Nodes.
- Baseline active replication is three copies with quorum acknowledgement.
- TLS is mandatory.
- Scoped API keys are the baseline client-authentication mechanism.
- Feed history and current-state indexing are separate concepts.
- There is no public cleanup-policy mode.
- Scale-in requires verified draining and safe-to-remove state.

### Still requiring prototypes or benchmarks

- Exact active-range replication protocol
- Raft library and control-plane state layout
- FeedId representation and Cursor encoding evolution
- Atomic boundary between append, cursor advancement, Pipe output, and strict Index updates
- Index Engine selection
- Index replication and checkpoint transfer
- History expiry guarantees when Index rebuildability is required
- Multi-region consistency and failover
- API-key root of trust and KMS integration
- Wildcard namespace expansion strategy
- Hot-key mitigation limits
- Object-store outage behavior
- Query and Index definition language

## Summary

Kafka made durable event history mainstream, but its partition abstraction couples ordering, scaling, storage, replication, and consumption. Whitewater separates those concerns.

The intended developer experience is:

```text
choose a Space
name a Feed
append by Key
resume with a Cursor
process through a Subscription or Pipe
add an Index when queryable state is needed
```

The Fabric owns placement, replicas, files, leases, tiering, batching, and scaling. Safe behavior is the default rather than the outcome of correctly tuning a large configuration matrix.
