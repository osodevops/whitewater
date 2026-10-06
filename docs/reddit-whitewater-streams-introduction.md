# Reddit launch content for Whitewater Streams

This document contains copy ready to adapt for a Whitewater Reddit community, an introductory channel, and a pinned launch post. Performance and production-readiness statements are deliberately separated into implemented facts, architectural targets, and hypotheses requiring benchmarks.

## Community naming note

If this is a Reddit **chat channel**, `whitewater-streams` is valid because channel names may contain hyphens.

If this is a new Reddit **community/subreddit**, Reddit community names are permanent, must be 3–21 characters, and should be checked carefully before creation. Suitable forms include:

```text
r/WhitewaterStreams
r/WhitewaterDB
r/WhitewaterRiverbed
```

The product remains **Whitewater** regardless of the community name.

## Short community description

Whitewater is a clean-sheet Rust project for a distributed database and partitionless streaming platform. It is being designed to replace topics, partitions, brokers, offsets, and global consumer rebalances with durable Feeds, key-defined ordering, opaque Cursors, capacity-aware Subscriptions, and first-class persisted replicated Indexes. This community follows the design, prototype, benchmarks, failures, and lessons in public.

## Short channel description

Discussing Whitewater: a Rust-based distributed database and streaming platform designed around Feeds, Keys, Cursors, Subscriptions, and first-class Indexes—with no public partitions or brokers.

## Suggested pinned-post title

# What if a streaming platform did not expose partitions at all? Introducing Whitewater

## Pinned introduction

I run FinnStream in Finland, and I am building a clean-sheet Kafka replacement called **Whitewater**.

Whitewater is intended to be both:

- A distributed database for durable temporal history and queryable current state
- A streaming platform for continuous delivery and stateful processing

The goal is not to reproduce the Kafka API in Rust. The goal is to remove the architectural coupling that makes Kafka powerful but difficult to evolve and operate.

The public model starts at the Riverbed boundary:

```text
riverbed -> space -> feed -> key -> cursor -> subscription
```

Applications should not know which server owns their data, how many physical ranges exist, where replicas live, or how work moved after capacity changed.

### First: Kafka deserves credit

Kafka made durable, replayable event history mainstream. Its append-only log, sequential IO, replication model, and decoupling of Writers and Readers changed distributed application architecture.

Whitewater is not based on “Kafka is bad” or “Java is always slow.” Experienced teams operate Kafka successfully, and managed Kafka removes a great deal of work.

The question is different:

> If we started again today, would we still make one partition represent ordering, storage, replication, parallelism, leadership, and consumer assignment?

Whitewater's answer is no.

## The central problem with partitions

A Kafka partition is simultaneously:

```text
ordering boundary
+ throughput unit
+ storage directory
+ replication unit
+ leader-election unit
+ consumer-assignment unit
+ scaling decision
```

That means an application team often chooses infrastructure topology while creating a logical business resource.

Choose too few partitions and future parallelism is limited. Choose too many and the cluster pays metadata, file, election, assignment, and operational overhead immediately. Increase the count later and keyed distribution changes. Add a broker and existing partitions do not automatically move onto it.

Whitewater keeps the useful contract and hides the rest:

```text
Events with the same Feed and Key are observed in accepted order.
Events with unrelated Keys may proceed independently.
```

Internally, Whitewater may split, merge, replicate, or move ranges. Applications do not see those ranges and do not receive topology-change callbacks.

## Topics become Feeds

A Whitewater **Feed** is immutable, replayable event history.

Each Feed has:

```text
FeedId    = immutable identity
FeedName  = mutable human-readable dotted name
```

Example:

```text
FeedId:    0195d8c1-51ea-7a42-b853-c551558f860e
FeedName:  commerce.orders.created
```

Renaming it:

```text
commerce.orders.created
    ->
commerce.orders.accepted
```

updates metadata rather than copying records into another Feed. Cursors, Subscriptions, Indexes, Pipes, schemas, policies, and placement remain attached to the FeedId.

Feed names are lowercase dotted paths, up to 512 bytes:

```text
commerce.orders.created
payments.authorisations.requested
identity.customers.updated
region1.telemetry.received
```

## Keys define ordering

Whitewater does not ask for a partition number or partition count.

The Key is the ordering identity:

```text
Feed: commerce.orders.events
Key:  order123
```

Events for `order123` remain ordered. Events for unrelated orders can be processed, placed, and moved independently.

A single extremely hot Key remains a real sequential limit. Whitewater will isolate and report it, but it will not pretend that strict ordering can be parallelized magically.

## Offsets become opaque Cursors

Kafka applications store topic, partition, and offset tuples. That exposes physical storage layout and makes topology evolution harder.

Whitewater returns an opaque, Feed-scoped **Cursor**:

```text
AbGTj_j2dWCi7QI_7Ffxw7wAAAAAAAAAACB2FoI
```

Clients may store, commit, seek with, and return a Cursor. They do not calculate with its internal fields. Every standalone Reader owns its own current Cursor, and every named Subscription has independent durable progress. Seeking or reading from one never changes another. Whitewater can change physical placement while preserving the logical read contract.

## Consumer groups become Subscriptions

Whitewater replaces global group reassignment with small, short-lived, epoch-fenced leases.

A **Subscription** describes durable read intent. Readers advertise real capacity:

```text
max events in flight
max bytes in flight
target acknowledgement latency
```

When a Reader joins, leases move gradually. When one disappears, only its leases expire. Unrelated Readers continue working.

The intended result is no stop-the-world processing pause simply because an application deployment changed membership. Separate Subscriptions still each receive the Feed independently, preserving one of Kafka's strongest properties.

## Nanosecond event and ingest time

Every record stores both `event_time_ns` and `ingest_time_ns` as signed Unix epoch nanoseconds. Writer/source time is never overwritten by Node acceptance time, and sub-millisecond precision is not discarded. Nanosecond representation preserves precision; it does not pretend every machine clock is physically accurate to one nanosecond.

## It is also a database

Whitewater is not only message transport.

```text
Feed         = immutable temporal database and replayable history
Index        = persisted replicated current/queryable state
Subscription = continuous capacity-aware delivery
Pipe         = stateful or stateless streaming computation
Cursor       = opaque position in temporal history
```

Kafka compacted topics can preserve eventual latest values, but a topic is not itself a point-query database. Kafka Streams commonly materializes local RocksDB state backed by compacted changelog topics, and applications still need to solve remote query routing and restoration.

Whitewater makes an **Index** a first-class Riverbed resource:

```text
Feed:  commerce.orders.events
Index: commerce.orders.byid
Key:   order123
Value: latest indexed order state
```

Indexes are intended to be:

- Persisted
- Replicated
- Queryable
- Checkpointed
- Movable
- Rebuildable from retained Feed history
- Explicit about their applied Cursor and freshness

Memory is a cache over persisted state, not the only live representation.

Pure-Rust storage engines such as Fjall and redb are serious initial candidates. RocksDB remains a benchmark and reference point. The final choice must survive crash, corruption, sustained-ingest, compaction, replication-snapshot, and recovery tests.

## No cleanup-policy personalities

Whitewater does not plan separate “delete Feed” and “compact Feed” personalities.

A Feed is always immutable history.

- History Policy controls hot retention, object-store tiering, legal retention, archive, and eventual expiry.
- Key Indexes provide latest-value state.
- Secondary Indexes provide explicitly selected query paths.
- Index deletion changes indexed state; it does not rewrite the meaning of the Feed.

This separates event history from current state instead of asking one log-cleaning mode to represent both.

## Safer durability as the baseline

A supported Whitewater Riverbed always has at least three Nodes.

```text
minimum Riverbed size:     3 Nodes
minimum active replicas: 3
normal write quorum:      2 of 3
```

This is the development topology too. Three containers on one laptop do not provide three physical failure domains, but they exercise the same membership and replication shape as production.

There is no supported single-Node Riverbed that quietly presents weaker durability under the same success response.

## Security should not be an optional assembly exercise

Whitewater's target baseline is narrow:

- TLS for all client and inter-Node traffic
- No plaintext production listener
- Scoped, rotatable API-key identities
- Predictable inherited capability policy through Spaces
- Audit events for administrative mutations
- Secret-free logs, metrics, and support bundles

API keys authorize access; they are not encryption keys.

Feeds requiring a separate encryption boundary reference a Key ID. Data uses envelope encryption with keys supplied by a KMS, HSM, or customer-managed key provider.

## Scaling should be boring

Adding a Kafka broker does not itself redistribute existing partitions. Reassignment can compete with foreground IO and network traffic, so operators plan batches, choose throttles, monitor progress, and verify completion.

Whitewater's target behavior is:

```text
sustained high pressure
    -> add one Node
    -> join and verify it
    -> move small internal ranges gradually
    -> measure again after a cooldown

sustained low pressure
    -> choose one candidate
    -> transfer leases and active ranges
    -> verify durable replicas
    -> mark safe to remove
    -> remove one Node
```

Scaling policy uses hysteresis, cooldowns, minimum and maximum sizes, and one-Node steps. A Node is never removed merely because load is low.

## Why Rust?

Not because “Java is slow.” Kafka demonstrates that Java can deliver excellent throughput.

Rust is attractive for Whitewater because it provides:

- Memory safety without a managed runtime
- Predictable tail latency without garbage-collection pauses
- Lower baseline process overhead
- Fine control over allocation, IO, buffering, and backpressure
- Efficient binary protocol implementation
- One native deployment artifact
- Strong concurrency guarantees for storage and consensus code

Those are architectural advantages, not benchmark results. Whitewater still has to prove performance with reproducible tests against realistic failure and workload conditions.

## What should be easier?

The intended developer experience is:

```text
choose a Space
name a Feed
append by Key
resume with a Cursor
process through a Subscription or Pipe
add an Index when queryable state is needed
```

A developer should not need to choose or tune:

- Partition counts
- Partition leaders
- Replica assignments
- Consumer group generations
- Poll intervals
- Rebalance callbacks
- ISR thresholds
- Producer in-flight counts
- Fixed batching delays
- Log-directory placement

The intended operator experience is:

- One Node binary
- Automatic discovery
- Declarative desired state
- Safe three-replica baseline
- Incremental movement and draining
- Explainable pressure and placement
- TLS and authentication by default
- SLO-aware background work
- One reasoned answer to “why is this delayed?”

The principle is:

> Expected operational change must not wake somebody up.

## Why this could be faster

Whitewater is being designed to remove work as much as to optimize work:

- No client-visible partition metadata or assignment protocol
- Small binary core protocol
- Sequential append path
- Checksummed immutable segments
- Adaptive batching and compression
- Credit-based backpressure
- Direct key ordering rather than partition-level coupling
- Persisted Indexes for point and range queries instead of full Feed scans
- Local NVMe for active data
- Object storage for sealed history
- Incremental range movement rather than whole logical-resource migration
- Rust-native execution without JVM/GC overhead

These are performance hypotheses until measured. The project will publish throughput, latency, recovery, resource, and failure benchmarks rather than claim victory from architecture diagrams.

## What exists today

The current Phase 1 Rust prototype includes:

- Durable checksummed binary append frames
- Hierarchical prototype namespaces
- Opaque stream-scoped Cursors with independent Reader positions
- Nanosecond event and ingest timestamps with legacy millisecond-record decoding
- Writer identity and sequence deduplication
- Duplicate suppression surviving restart
- Read-after-Cursor HTTP API
- DNS-seeded Node discovery
- Membership heartbeats, expiry, and graceful leave
- Three-Node Docker development Riverbed
- Arbitrary Docker replica scaling
- Slow hysteresis-based scale recommendations
- Node demand and storage-safety telemetry
- Safe scale-in refusal for non-empty Nodes
- Unit, restart, and Docker membership tests

The prototype has successfully exercised dynamic membership and host-side scaling between different replica counts.

## What does not exist yet

Whitewater is **not production-ready**.

The current prototype does not yet provide:

- Raft-backed metadata consensus
- Replicated active-range storage
- Automatic redistribution of existing data after scale-out
- Ownership epochs for accepted writes
- Subscription lease delivery
- Atomic consume-and-append
- Persisted replicated Indexes
- TLS/API-key enforcement
- Object-store tiering
- Schema service
- Multi-region disaster recovery
- Production client SDKs
- Proven Kafka-beating benchmarks

Current Docker membership demonstrates discovery and control-loop behavior, not production durability.

## Roadmap

### Phase 1 — correctness foundation

- FeedId and dotted FeedName migration
- Durable segmented log
- Opaque Cursors
- Writer deduplication
- Three-Node development Riverbed
- Failure and corruption tests

### Phase 2 — real distributed durability

- Small Raft control plane
- Active-range ownership epochs
- Three-replica quorum append
- Replica verification and repair
- Safe Node drain
- Incremental lease transfer

### Phase 3 — partitionless elasticity

- Large internal range space
- Capacity-aware placement
- Online range split, merge, and movement
- Hot-key isolation
- Autoscaling tied to verified placement and draining

### Phase 4 — database capability

- Pure-Rust Index Engine evaluation
- Persisted replicated Key Indexes
- Secondary Index definitions
- Index checkpoints and transfer
- Query routing and freshness contracts
- Pipe state and atomic effects

### Phase 5 — production platform

- Mandatory TLS and scoped API keys
- Space isolation and quotas
- Object-storage tiering
- Rolling upgrades
- Disaster recovery
- Operator APIs, `wwctl`, Kubernetes Operator, and MCP inspection tools
- Reproducible comparative benchmarks

## How this differs from other Kafka replacements

Several excellent systems improve Kafka's implementation, cloud economics, protocol compatibility, or operational model.

Whitewater is intentionally not starting with Kafka protocol compatibility. Compatibility would preserve topics, partitions, offsets, acknowledgements, and group semantics—the public coupling this project is trying to remove.

The experiment is whether a better application-facing model can justify a new protocol:

```text
Kafka:
cluster -> broker -> topic -> partition -> offset -> consumer group

Whitewater:
riverbed -> space -> feed -> key -> cursor -> subscription
```

That is a larger and riskier project than implementing a faster broker. It may also create room for substantially simpler applications and operations.

## Frequently asked questions

### Is this just Kafka rewritten in Rust?

No. The storage implementation is new, but the main difference is the public model. Whitewater deliberately avoids public partitions, physical offsets, broker topology, and group-wide assignment generations.

### Is Whitewater claiming to be faster than Kafka already?

No. The architecture targets lower overhead and more predictable latency, but comparative claims require reproducible benchmarks after replication and the real protocol exist.

### Why not contribute these changes to Kafka?

Many ideas can improve Kafka, and Kafka continues to evolve. But removing public partitions and offsets would break its protocol and application model. Whitewater explores what becomes possible when compatibility is not the first constraint.

### Why require three Nodes in development?

Because single-Node development hides the failure and membership behavior that defines a distributed system. Three local containers do not create physical resilience, but they prevent a different product from being used in development.

### Does key ordering eliminate hot keys?

No. A single strictly ordered Key remains sequential. Whitewater aims to isolate, resource, and explain hot keys without allowing them to dominate unrelated keys.

### Are Indexes just Kafka Streams state stores?

They solve related problems, but Whitewater intends Indexes to be persisted, replicated, queryable Riverbed resources with automatic routing and explicit freshness—not application-local stores whose remote query API is left to each application.

### Is the core schema-specific?

No. Metadata, Keys, and payloads remain bytes. Optional Schema Policies can validate and describe those bytes without making the storage engine dependent on JSON, Avro, or Protobuf.

### Is the project open source?

The implementation, licensing, contribution model, and public repository details should be announced only when FinnStream has made those decisions. This community can still discuss architecture and publish progress without promising a model prematurely.

### When is it production-ready?

There is no credible date yet. Consensus, replication, draining, security, upgrade, recovery, and fault-injection work must come before production claims.

## What this community is for

This community should be useful even if Whitewater changes direction.

Good contributions include:

- Distributed-systems criticism
- Kafka operational stories and lessons
- Storage-engine discussion
- Failure scenarios we have missed
- Protocol and Cursor design
- Index consistency models
- Rust performance and safety analysis
- Kubernetes, Nomad, ECS, and Docker orchestration patterns
- Security and key-management review
- Benchmarks and benchmark methodology
- Disaster-recovery experience
- Developer API design

The community should avoid:

- Kafka tribalism
- Unverified performance claims
- Benchmark screenshots without methodology
- Dismissing operational experience as user error
- Treating every configuration option as a feature
- Hiding known limitations
- Personal attacks on maintainers or users of other systems

Kafka's design came from real constraints and has taught the industry an enormous amount. Whitewater should learn from it without being trapped by it.

## Questions for the community

1. Which Kafka operational task causes your team the most recurring work?
2. Which Kafka setting has produced the most surprising incident?
3. What is the narrowest useful atomic consume-and-append boundary?
4. What should an opaque Cursor guarantee across range movement and disaster recovery?
5. Should strict Index updates be part of append acknowledgement or offer explicit consistency levels?
6. Which pure-Rust Index Engine workloads should we benchmark first?
7. What information would make automatic placement and scaling trustworthy?
8. What is missing from the proposed Feed/Key/Cursor/Subscription model?
9. Which failure should be in the first public chaos test?
10. What would convince you that a new protocol is worth adopting?

## Closing

Whitewater is an ambitious project, and the difficult parts are still ahead.

The thesis is simple:

> Ordering, storage, replication, parallelism, and Reader ownership should not all be represented by one public partition.

If that coupling is removed, event infrastructure can become easier to use, safer to operate, more elastic, more queryable, and potentially more efficient.

That is what Whitewater is trying to prove.

— FinnStream, Finland
