# Kafka Pain Points Whitewater Must Address

> Kafka makes simple things easy enough, but once a team leaves the happy path it expects them to become Kafka experts.

## Document status

- **Status:** Product problem catalogue and design input
- **Product:** Whitewater by FinnStream
- **Audience:** Product designers, contributors, application developers, platform engineers, SREs, security engineers, and FinOps
- **Companion documents:** [Why Whitewater](why-whitewater.md), [Whitewater operational experience](operational-experience.md), and [Whitewater architecture](kafka-successor-architecture.md)

This catalogue preserves the problems Whitewater is intended to solve. It is not a claim that every Kafka deployment suffers every problem, nor that Kafka lacks successful operational practices. It records where Kafka's abstractions and operational model impose recurring expertise, infrastructure, or cost burdens.

Future Whitewater design work should use these pains as inputs, turn relevant items into measurable acceptance criteria, and avoid recreating them under different terminology. A feature is not an improvement merely because it hides an implementation detail from one interface; the detail must be safely automated, made explainable, or removed from the user's responsibility.

## Traceability and delivery status

The tables below group related pains so that the response remains readable. They are a delivery map, not a claim that planned behavior exists.

| Status | Meaning |
|---|---|
| **Prototype** | Working in the current prototype, but not yet a complete production contract. |
| **Foundation** | A contract, domain model, or local implementation exists; distributed behavior is incomplete. |
| **Designed** | The intended contract is documented, but implementation has not started or is incomplete. |
| **Planned** | Work appears in [tasks and milestones](tasks.md), but its prerequisite milestone has not completed. |
| **Roadmap gap** | The pain is accepted and tracked, but no delivery milestone has been sequenced yet. |

### DevOps and platform traceability

| Kafka pain | Whitewater response | Status | Evidence or delivery point |
|---|---|---|---|
| Many separately operated components and role-specific processes | One Rust Node binary with internally assigned Control, Storage, Compute, Gateway, and Cache capabilities; one typed control contract serves WCL, APIs, CLIs, SDKs, Operators, and future MCP tools. | **Foundation** | One Node binary and typed `ControlController` exist; role-aware placement is Milestone 5. |
| Partition counts must be chosen early | Feeds expose keys and ordering, never partition counts. Internal Active Ranges split and merge without changing the Feed contract. | **Foundation** | Feed administration is partitionless; fixed Active Range work is Milestone 1 and online split/merge is Milestone 4. |
| Repartitioning and reassignment hammer disk and network | Internal range movement is incremental, resumable, checksum-verified, and bounded by foreground SLO and disruption budgets. | **Designed** | Active Range recovery is specified; catch-up is M1.10 and general movement is Milestones 4–5. |
| Hot partitions and uneven placement | The Fabric detects hot ranges and keys, isolates unrelated keys, and rebalances movable ranges by capacity and failure domain. It reports honestly that one strictly ordered hot key cannot be parallelized automatically. | **Planned** | Hot-key detection and multiple ranges are Milestone 4; role-aware placement is Milestone 5. |
| Adding brokers does not automatically redistribute useful work | Nodes advertise capabilities and pressure; the Fabric places or moves only the work that addresses the measured bottleneck. | **Foundation** | Membership, demand telemetry, and hysteretic recommendations exist; useful role-aware scaling is Milestone 5. |
| Capacity planning couples many hidden constraints | Capacity is reported by logical resource and constrained capability, with slow bounded recommendations rather than partition arithmetic. | **Foundation** | Node demand and storage telemetry exist; attribution and capability-specific pressure remain Milestone 5 work. |
| Retention, compaction, replication, or producers fill disks unexpectedly | Reserve recovery headroom, project growth, admit writes safely, tier sealed history, and identify the responsible Space and Feed before exhaustion. | **Designed** | Operational contract exists; tiered history is Milestone 8 and disk-full suites are Milestone 9. |
| Broker replacement and recovery are operational projects | A replacement Node catches up verified committed ranges automatically; stale owners are epoch-fenced and scale-in is refused until drain completes. | **Foundation** | Fencing and recovery models exist; owner recovery is M1.9, repair is M1.10, and general drain is Milestone 5. |
| Consumer lag is visible but difficult to explain | End-to-end diagnostics identify the limiting stage from writer admission through quorum, storage, Index, Subscription, and Reader. | **Designed** | Diagnostic requirements exist in `operational-experience.md`; unified explanation APIs remain unscheduled beyond scaling explanations in Milestone 5. |
| Group rebalances stop processing | Subscriptions use small epoch-fenced leases transferred incrementally according to Reader capacity; unrelated Readers continue. | **Designed** | Reader capacity starts in Milestone 3; Subscription leases and incremental transfer are Milestone 7. |
| Tuning folklore and dangerous configuration combinations | Durability, retry safety, and bounded backpressure are protocol invariants with narrow named policies rather than independent low-level switches. | **Foundation** | RF3/two-of-three contract and executable model exist; quorum-durable append is the current Milestone 1. |
| TLS, SASL, ACLs, and certificate rotation are difficult | TLS-only traffic, scoped API-key identities, capability inheritance, explainable authorization, and overlapping credential rotation are secure defaults. | **Foundation** | Authenticated Admin API exists; production TLS, identity persistence, rotation, audit, and complete enforcement are Milestone 9. |
| JVM tuning and garbage-collection behavior burden operators | A native Rust implementation avoids JVM deployment and GC tuning while retaining explicit bounded-resource engineering. | **Prototype** | The service is implemented in Rust; long-duration memory and resource characterization remains required. |
| Upgrades require protocol and compatibility choreography | Nodes advertise compatibility; the Control Plane computes a gated rolling plan, pauses on reduced health, and exposes rollback boundaries. | **Designed** | Upgrade contract exists; implementation and mixed-version tests are Milestone 9. |
| Cross-datacenter replication and disaster recovery need separate offset translation systems | Remote Feed replicas and Cursor checkpoints use stable Feed identity; DR policy exposes RPO, RTO, promotion, failback, and exercise status. | **Designed** | DR behavior is documented; backup, restore, and DR exercises are Milestone 9. |
| Topic sprawl leaves unknown owners and retention | Spaces provide ownership, namespace authorization, quotas, policy inheritance, accounting, and safe lifecycle controls over Feeds. | **Foundation** | Spaces, Feeds, grants, inspection, and safe logical drop exist; quotas, ownership enforcement, and History Policy remain incomplete. |
| Schema governance becomes another separately operated bureaucracy | Schemas remain optional for byte storage but become first-class Feed policy with compatibility, validation, identity, and generated-client support under one control plane. | **Roadmap gap** | Product direction exists, but `tasks.md` has no explicit schema milestone. |
| Exactly-once marketing hides operational consequences | Whitewater names the actual boundary: idempotent append for duplicate-safe writes and atomic consume-and-append for effects wholly inside Whitewater; external effects retain explicit ambiguity. | **Foundation** | Local append deduplication exists; replicated retry safety is Milestone 1 and atomic effects are Milestone 7. |
| Observability is fragmented across products and dashboards | Metrics, traces, logs, events, and automatic decisions share logical IDs and feed a built-in “why?” explanation. | **Designed** | Basic tracing and metrics exist; correlated explanation, bounded cardinality, and support bundles remain Milestone 9 or a roadmap gap. |
| Managed Kafka costs scale sharply; self-hosting hides human cost | Attribute storage, replication, egress, movement, Subscription, Pipe, and Index cost to logical owners; automate routine operations safely. | **Designed** | FinOps requirements exist; complete cost attribution has no dedicated milestone and Index cost begins in Milestone 6. |
| Production-like testing is expensive | A standard three-Node local Fabric uses the same topology and correctness paths as production, automated by Docker Compose. | **Prototype** | The three-Node Compose Fabric is implemented and health checked; TLS and record replication are not yet production-equivalent. |
| Incidents are obscure while components look healthy | Every diagnostic states what is happening, scope, cause, automatic action, current durability/availability risk, and next safe action. | **Designed** | Required output is documented; end-to-end implementation remains a roadmap gap. |

### Software-developer traceability

| Kafka pain | Whitewater response | Status | Evidence or delivery point |
|---|---|---|---|
| “Just send a message” requires infrastructure expertise | A Writer chooses Feed, Key, payload, Metadata, and event time; owner, range, replica, sequence, batching, and retry routing stay internal. | **Foundation** | Local append exists; replicated append is Milestone 1 and ergonomic Writer sessions/SDK are Milestone 2. |
| Partitions and partition-local ordering leak into design | The public guarantee is same-Feed/same-Key accepted order. Internal ranges can move, split, and merge without client topology callbacks. | **Foundation** | Public APIs expose no partitions; multi-range ordering is Milestone 4. |
| Choosing or changing a key becomes infrastructure architecture | Keys express only the business ordering boundary; physical placement is independent. Managed Pipes will make re-keying and shuffle explicit-cost implementation details. | **Designed** | Key ordering is defined; Pipes and atomic effects are Milestone 7. |
| Consumer groups and rebalances are difficult and pause applications | Independent Readers use opaque Cursors; cooperating Subscription Readers use capacity-aware incremental leases instead of global assignment generations. | **Foundation** | Independent local Cursor reads exist; Reader sessions are Milestone 3 and Subscription leases are Milestone 7. |
| Applications must understand and manually manage offsets | Clients store, acknowledge, seek, and replay with opaque Feed-scoped Cursors that survive internal topology changes. | **Foundation** | Opaque local Cursors and WCL seek exist; acknowledged durable Reader progress is Milestone 3. |
| Duplicate messages and ambiguous retries require custom design | Stable Writer identity plus sequence returns the original MessageId and Cursor for identical retry and rejects conflicting reuse. | **Foundation** | Proven for local restart; quorum-safe deduplication is part of Milestone 1 and Writer sessions are Milestone 2. |
| Exactly-once is misunderstood across external side effects | APIs state the effect boundary precisely; Whitewater can atomically commit Subscription progress with Whitewater outputs, not arbitrary external systems. | **Designed** | Atomic consume-and-append is Milestone 7. |
| Retries, delayed delivery, dead letters, and poison events are scattered application code | Subscription policy owns retry schedule, bounded attempts, delayed delivery, quarantine, skip rules, and queryable final disposition without stalling unrelated keys. | **Roadmap gap** | The behavior is required by product docs, but Milestone 7 does not yet enumerate the complete workflow. |
| Backpressure is left to each application | Readers advertise capacity credits; the server bounds in-flight work and isolates slow Readers and keys. | **Designed** | Capacity credits and backpressure are Milestone 3. |
| Long-running processing conflicts with liveness and rebalance timeouts | Reader liveness, work lease renewal, processing duration, and per-key disposition are separate contracts. | **Designed** | Reader sessions are Milestone 3 and leases are Milestone 7. |
| Client configuration is enormous | SDKs negotiate safe behavior and adapt batching from observed traffic; applications configure intent and policy rather than transport internals. | **Planned** | Writer SDK begins in Milestone 2; production SDK coverage remains incomplete. |
| Serialization errors appear far from their source | Optional Feed Schema Policy validates at admission and reports the offending field, compatibility rule, impact, and correction. | **Roadmap gap** | Schema behavior is designed but has no explicit implementation milestone. |
| Schema Registry and schema formats add separate tooling | Schema identity, compatibility, validation, and generated clients live under the Whitewater control model while storage remains format-neutral. | **Roadmap gap** | No explicit schema milestone exists yet. |
| Local development and integration tests are heavy | One Compose command starts the supported three-Node topology; client logic should also be testable against SDK abstractions without a Fabric. | **Prototype** | Three-Node Compose exists; lightweight SDK test doubles are not scheduled. |
| Kafka Streams state, changelogs, and repartition topics are magical | Pipes expose computation; persisted replicated Indexes expose current state and applied-Cursor freshness without application-managed changelog stores. | **Designed** | Indexes are Milestone 6; Pipes and atomic effects are Milestone 7. |
| Stream joins, windows, grace periods, and late arrivals are difficult | Pipes must expose explicit event-time, watermark, lateness, and state contracts with explainable cost and no hidden internal resources. | **Roadmap gap** | Pipes are scheduled in Milestone 7, but joins/windows/watermarks lack explicit tasks. |
| Replaying or deleting data becomes offset manipulation | Readers seek by opaque Cursor or time-oriented API; History Policy governs lifecycle separately from current-state Indexes. | **Foundation** | Cursor reads and WCL seek exist; time seek and complete replay UX are Milestone 3 or unscheduled, while lifecycle is Milestone 8. |
| Inspecting a topic or finding one event is cumbersome | Built-in Feed inspection supports bounded scans; persisted Indexes provide point/prefix/field lookup without adding another product. | **Designed** | Sequential reads exist; Indexes are Milestone 6, while richer event inspection/search UX is a roadmap gap. |
| Distributed flows are hard to debug and trace | Record Metadata carries trace context; correlated Feed, Subscription, Pipe, Index, and control-decision diagnostics reconstruct a business flow. | **Designed** | Metadata exists; end-to-end business-flow tracing has no explicit milestone. |
| Java APIs are verbose and non-Java clients diverge | A versioned language-neutral protocol defines semantics; idiomatic SDKs share conformance tests and Java has no privileged behavior. | **Roadmap gap** | A typed Rust client starts in Milestone 2; cross-language SDK and conformance milestones are not yet defined. |
| Transactional APIs are difficult | Atomic consume-and-append presents a narrow operation around acknowledged input progress and Whitewater output effects. | **Designed** | Milestone 7. |
| Errors are accurate but not actionable | Structured errors include cause, affected resource, retry safety, impact, and next safe action using consistent codes across SDKs. | **Roadmap gap** | Some domain errors are precise, but a protocol-wide actionable error contract is not scheduled. |

## Current pain-driven priority

M1.6 delivered owner-side majority commit. The owner flushes locally, concurrently replicates the exact frame to both followers, requires one matching durable follower, and persists CommitPosition evidence on two current replicas before returning success. One healthy replica cannot succeed; either owner-plus-follower pair can. Identical retry after a lost response returns the original logical result.

The next implementation is **M1.7 — route append through any Node**:

1. Write failing tests for append through the owner and through each non-owner.
2. Add a minimal authenticated client append request that contains Feed, Key, payload, Metadata, event time, and stable request/Writer identity—but no physical topology.
3. Resolve FeedName to FeedId and the committed Active Range assignment.
4. Forward non-owner requests to the current owner while preserving one request ID and append identity.
5. Have the owner encode once and invoke the M1.6 majority coordinator.
6. Return the same MessageId, Cursor, and durability meaning regardless of the ingress Node.

This work directly addresses “just send a message” complexity and topology leakage. Applications must not discover owners, replicas, epochs, positions, or forwarding routes.

## DevOps and platform-team pain

For DevOps and platform teams, the pain is primarily operational complexity, reliability, scaling, and cost.

### Operational surface area

- Too many moving parts: brokers, controllers, KRaft, Schema Registry, Connect, ACLs, monitoring, certificates, exporters, and related services.
- Kafka tuning depends on hundreds of configuration options and substantial “it depends” knowledge.
- Configuration mistakes involving retention, ISR, acknowledgements, replication factors, or timeouts can be catastrophic.
- TLS, SASL, and ACL configuration is powerful but tedious and easy to get wrong.
- Certificate rotation introduces another failure-prone operational workflow.
- JVM heap, garbage collection, and memory behavior remain operator concerns.
- Upgrades require careful handling of protocol versions, rolling order, compatibility, and client versions.
- Production-like Kafka test environments are expensive.

### Capacity, placement, and scaling

- Partition counts must be chosen early and are often regretted later.
- Repartitioning and moving partitions can saturate disk and network resources.
- Hot partitions can overload one path while other brokers remain relatively idle.
- Storage, partition, and traffic placement becomes uneven and requires continuing attention.
- Adding brokers does not automatically and intelligently redistribute existing work.
- Capacity planning couples CPU, RAM, network, disk IOPS, partition counts, message sizes, and retention.
- Developers can create hundreds or thousands of topics while platform teams inherit the operational consequences.
- Old topics, mystery topics, unknown owners, and unclear retention policies create topic sprawl.

### Reliability and incident response

- Retention, compaction, replication, or badly behaved producers can fill disks unexpectedly.
- Broker replacement and recovery are conceptually simple but unpleasant at scale.
- Consumer lag is easy to detect but often difficult to explain.
- Consumer-group rebalances can suddenly pause processing.
- Kafka incidents can remain obscure while every individual component appears healthy and the system slowly degrades.
- Exactly-once guarantees carry operational consequences that are not obvious from the marketing term.
- Disaster recovery is non-trivial, particularly when offsets, schemas, and dependent services are involved.
- Cross-datacenter replication through MirrorMaker or similar systems adds another operational layer.

### Governance, observability, and cost

- Observability is fragmented across broker metrics, JVM metrics, consumer lag, Connect, Schema Registry, and application metrics.
- Schema governance can become organisational bureaucracy.
- Managed Kafka becomes expensive as throughput and storage increase.
- Self-hosted Kafka appears cheaper until the human cost of operating it is included.

## Software-developer pain

For software developers, Kafka's largest problem is that its implementation model leaks into application architecture and everyday code.

### A messaging abstraction that leaks

- “Just send a message” is not actually simple.
- Partitions leak into application design.
- Ordering exists only inside a partition.
- Choosing a message key becomes an architectural decision.
- Changing the key later can be painful.
- Consumer groups take time to understand correctly.
- Rebalances can unexpectedly pause applications.
- Developers must understand offsets.
- Manual offset management is easy to get wrong.
- Developers end up learning Kafka internals when they wanted messaging.

### Delivery, failure, and flow control

- Duplicate messages are normal unless the application is deliberately designed around them.
- Exactly-once does not mean exactly once across every external effect.
- Retries are surprisingly complicated.
- Dead-letter queues are not a native first-class workflow.
- Poison messages require application-specific handling.
- Error handling becomes scattered through consumer code.
- Backpressure remains primarily the application's responsibility.
- Long-running processing conflicts with consumer timeout and rebalance behavior.
- Transactional producer APIs are not particularly developer-friendly.
- Many Kafka errors are technically accurate but practically unhelpful.

### Client and schema complexity

- Kafka client configuration is enormous.
- A simple consumer can have dozens of meaningful settings.
- Serialization failures often appear far from where invalid data originated.
- Schema evolution becomes difficult when fields are removed or renamed.
- Avro, Protobuf, or JSON Schema introduces additional tooling and build complexity.
- Schema Registry becomes another dependency developers must understand.
- Java Kafka APIs are verbose.
- Non-Java clients can lag behind Java features or behave differently.
- Client compatibility and version matrices create anxiety.

### Local development and testing

- Local development is heavier than using a conventional database or queue.
- Integration tests can be slow and awkward.
- Testcontainers helps, but developers still start Kafka containers to test application logic.
- Production-like environments are difficult to reproduce cheaply.

### Stream processing and state

- Kafka Streams has a significant learning curve.
- State stores, changelogs, and repartition topics feel magical until they fail.
- Kafka Streams topology debugging can be difficult.
- Joining streams is much harder than joining database tables.
- Windowing semantics require understanding event time, processing time, grace periods, and late arrivals.
- KTables versus streams confuses newcomers.
- Internal topics appear without being obvious application resources.
- A small code change can cause a large repartitioning workload.

### Replay, inspection, and debugging

- Deleting or replaying data is not intuitive.
- “Replay from yesterday” often becomes an offset-management exercise.
- Inspecting the contents of a topic is unnecessarily cumbersome.
- Finding one event among millions of records is painful.
- General querying requires another product.
- Distributed event flows are much harder to debug than REST calls.
- Tracing one business transaction across many topics is difficult.

## Shared root problem

The shared problem is expertise amplification: once a workload leaves the happy path, both operators and developers must understand Kafka internals to preserve correctness and availability.

Whitewater's opportunity is to make partitions, balancing, recovery, schemas, retries, state, observability, and scaling implementation details rather than mandatory expertise. Hiding those concepts is not enough. Whitewater must provide safe automation, stable public contracts, bounded behavior, and explanations of what the Fabric is doing and why.

## Product test for Whitewater work

For every feature, design, or operational control, ask:

1. Which pain in this catalogue does the work remove or reduce?
2. Does it eliminate responsibility, safely automate it, or merely rename it?
3. What stable user-facing contract replaces the Kafka-specific knowledge?
4. How does the Fabric explain automatic decisions and degraded states?
5. What unit, integration, fault, or acceptance test proves the improvement?
6. Does the happy path remain simple without making failure behavior obscure?
7. Does the solution work consistently for operators and developers, including non-Java clients?
8. What cost, capacity, security, and disaster-recovery consequences remain visible?

A proposal that cannot answer these questions should not claim to be a DevOps- or developer-experience improvement.