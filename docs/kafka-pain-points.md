# Kafka Pain Points Whitewater Must Address

> Kafka makes simple things easy enough, but once a team leaves the happy path it expects them to become Kafka experts.

## Document status

- **Status:** Product problem catalogue and design input
- **Product:** Whitewater by FinnStream
- **Audience:** Product designers, contributors, application developers, platform engineers, SREs, security engineers, and FinOps
- **Companion documents:** [Why Whitewater](why-whitewater.md), [Whitewater operational experience](operational-experience.md), and [Whitewater architecture](kafka-successor-architecture.md)

This catalogue preserves the problems Whitewater is intended to solve. It is not a claim that every Kafka deployment suffers every problem, nor that Kafka lacks successful operational practices. It records where Kafka's abstractions and operational model impose recurring expertise, infrastructure, or cost burdens.

Future Whitewater design work should use these pains as inputs, turn relevant items into measurable acceptance criteria, and avoid recreating them under different terminology. A feature is not an improvement merely because it hides an implementation detail from one interface; the detail must be safely automated, made explainable, or removed from the user's responsibility.

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