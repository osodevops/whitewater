# Whitewater vs Kafka: partition-free streaming, fewer operational decisions

> No partitions to create, count, assign, or repartition. Build around your business data — Whitewater handles the physical layout.

**Whitewater is a partition-free streaming platform and distributed database, built in Rust.** A Feed has no user-defined partitions. You do not choose a partition count when creating it, assign consumers to numbered partitions, or plan repartitioning when your workload grows. You choose a Feed and a business Key; the Riverbed manages the storage and replication beneath them.

This is the central difference from Kafka: **partition management is removed from the public model, not made into a friendlier configuration screen.** Applications work with Feeds, Keys, opaque Cursors, Subscriptions, and StateStores. Operators manage service capacity, durability intent, and resource policies rather than a partition plan for each Feed.

**Partition-free by design. Replication managed by the platform. Three Nodes and three active replicas as the baseline — with a larger service footprint when your workload needs it.**

## Partition-free: what you no longer have to manage

| With Kafka | With Whitewater |
|---|---|
| A topic is divided into a configured number of partitions. | A Feed has no partition-count setting. |
| Partitioning determines which partition receives a Key. | The business Key defines ordering; physical routing is internal. |
| A consumer group assigns topic partitions to its consumers. | Applications use Readers and Subscriptions, not partition assignments. |
| Stored progress is expressed through topic/partition offsets. | Progress is represented by opaque Cursors, not physical addresses. |
| Capacity planning includes partition parallelism and placement. | Capacity is a Riverbed concern; adding capacity does not require users to change a Feed's partition count. |
| Stateful processing can require matching partitioning across inputs. | The StateStore/Pipe contract uses logical lookups without client co-partitioning plans. |

**Create the Feed. Choose the business Key. Write, read, and process data — without partition arithmetic.**

Partition-free does not mean the storage engine has no internal divisions. Whitewater uses internal Active Ranges that can split, merge, and move beneath a stable Feed identity. These are storage mechanisms, not public partitions that customers must size or assign. Authorized diagnostics may show placement to explain an incident; inspecting it does not make it part of the application contract.

Ordering is defined for the same Feed and Key, not as a total order across the entire Feed. Adding Nodes does not parallelize one strictly ordered hot Key. The partition-free interface is the contract; the delivery status of automatic placement, scaling, and processing is identified below.

## What this comparison means

This is a sales and evaluation checklist, not a declaration that every roadmap feature has shipped. It compares Kafka's public model and typical operational responsibilities with Whitewater's intended simpler experience. Managed Kafka, operators, good defaults, and newer consumer protocols can automate or reduce several Kafka responsibilities; Whitewater's goal is to remove them from the application contract itself.

- **Contract:** a defined Whitewater public model or baseline invariant, not certification of every production failure scenario.
- **Direction:** an architectural requirement with implementation and acceptance work still to complete.
- **Proposal:** a product option discussed here, not a shipped setting or approved algorithm.

The checkboxes below are for a reader evaluating which simplifications matter to their team. They are not engineering completion markers. See [delivery milestones](tasks.md) for implementation evidence.

## Side-by-side: the easier-platform checklist

| Evaluate | The job | Kafka model / typical team responsibility | Whitewater experience | Basis |
|---|---|---|---|---|
| [ ] | Create a partition-free event stream | A topic has partitions, with a count and replication factor explicitly chosen or inherited from defaults. | Create a Feed with no public partitions: no partition count to choose, no partition assignments to maintain, and no replica layout to supply. | Contract |
| [ ] | Preserve business ordering | Keys map to partitions; partitioning strategy and count changes can affect that mapping. | Supply the business Key. Same-Feed/same-Key accepted order is the contract; physical placement stays internal. | Contract |
| [ ] | Set the durability baseline | Coordinate replication factor, producer acknowledgements, minimum in-sync replicas, and related policies; Kafka `acks=all` waits for the current in-sync replica set, subject to minimum ISR. | RF3 by default, with requested ACK ALL success on every replica required by the committed placement unless an authorized override is explicit. No silent downgrade when a replica is unavailable. | Direction: [ACK ALL tasks](tasks.md#m53--ack-all-by-default-explicit-acknowledgement-overrides); current code uses two-of-three quorum acknowledgement |
| [ ] | Choose a larger service | Plan broker capacity separately from partition placement and assignments. | Set a Riverbed capacity floor, such as eight Nodes, without changing every Feed. Node count and replication factor remain separate choices. | Direction; eight-Node example below is not a verified deployment claim |
| [ ] | Absorb sustained traffic growth | Add brokers and arrange useful placement through reassignment, an operator, or managed-service tooling. | Add capacity gradually and move eligible work onto it under platform control; no Feed-by-Feed reassignment plan for the customer. | Direction |
| [ ] | Scale back down | Ensure data and leadership are safely moved before decommissioning a broker. | Automatically refuse removal until ownership/data drain is complete and the Node is verified safe to remove. | Direction |
| [ ] | Request stronger durability | Broker defaults help new topics; changing replication for existing topics still needs a controlled reassignment workflow or tooling. | Set an optional Riverbed-wide durability policy; the platform would reconcile eligible data without routine per-Feed RF configuration. | Proposal for the service-wide control and reconciliation workflow |
| [ ] | Follow progress and replay | Consumer progress is expressed through topic/partition offsets, often managed by the client library. | Store and return opaque Cursors; applications do not calculate physical positions. | Contract |
| [ ] | Add processing workers | Consumer-group membership and partition assignment govern work; cooperative and newer protocols reduce disruption. | Incremental, epoch-fenced Subscription leases aim to move only affected work, bounded by Reader capacity. | Direction; full guarantees require recovery/load evidence |
| [ ] | Get current state | Use compacted topics, Kafka Streams stores, an external database, or another processing service. | Use Domain-owned StateStores and explicit Indexes, separate from immutable Feed history, with primary and named secondary lookups. | Contract; supported operations are version-specific |
| [ ] | Develop outside Java | Kafka clients are multilingual; the native Kafka Streams topology, DSL, and stateful runtime are Java/JVM-based. | The same conceptual streaming and state suite across Rust, Python, Java, C#, Node.js/TypeScript, and Go — idiomatic syntax, not a reduced product. | Direction; shared conformance gates parity |
| [ ] | Manage retained history | Retention, compaction where configured, replica catch-up, and disk headroom compete for resources. | History Policy governs expiry/tiering; current state lives separately. Budget lifecycle and maintenance work alongside foreground traffic. | Direction |
| [ ] | Control access | Manage principals, authentication, and resource/operation ACLs, with available tooling. | Scoped API-key identities and Domain/Feed capabilities, with explainable access and UI-assisted management. | Direction; complete security and UI workflows remain gated |
| [ ] | Investigate a problem | Combine data inspection, lag, broker metrics, logs, and processing-system evidence. | Bring data-centric Views, filters, samples, and Playbooks together with Whitewater metrics and operational explanations. | Direction; UI adaptation and management suite in progress/planned |
| [ ] | Run the data service | Operate a JVM-based broker runtime and its resource configuration. | Run a native Rust Node binary with no JVM or garbage-collector tuning in the data service. | Contract; performance still needs measured evidence |

**The advantage is fewer recurring decisions — not merely fewer visible settings.** Whitewater must own the placement, recovery, and safety work that disappears from the application interface.

## Replication is automatic, not absent

“Replica management-free” does not mean “no replicas.” It means an application developer should not have to decide how many physical copies a Feed needs or where to put them before writing the first event.

Keep three separate concepts clear:

| Concept | What it controls | Default / proposed example |
|---|---|---|
| Minimum Node count | The service's capacity floor: CPU, memory, disks, network, and maintenance headroom. | Supported baseline: at least three Nodes. Proposed larger-service example: keep at least eight. |
| Replication factor (RF) | The number of active copies of each internal range, not the number of Nodes in the entire Riverbed. | Default and minimum baseline: three replicas on distinct eligible Nodes. |
| Autoscaling ceiling and budgets | How far the service may grow and how much disruption/cost a change may incur. | Operator-defined limits; sustained pressure, hysteresis, cooldowns, and one-Node steps. |

**Eight Nodes does not mean eight copies of every record.** With RF3, different internal ranges can place their three copies across different eligible Nodes in a larger Riverbed. The same Feed/Key interface remains in place. That placement model is a design objective, not evidence that the current runtime already distributes a live eight-Node workload this way.

A larger service can keep RF3 and use additional Nodes for useful capacity. More replicas are a different decision: stronger durability or additional serving copies may justify them, but they also consume more disk, replication bandwidth, and recovery IO. Raising RF is not a general shortcut to higher write throughput.

## Example: keep eight Nodes, grow when busy

**Illustrative target operating policy — not runnable configuration or a shipped eight-Node autoscaling claim.**

| Situation | What the customer chooses | What Whitewater is intended to handle |
|---|---|---|
| Establish the service | A minimum of eight Nodes, a maximum size, resource budgets, and required failure-domain coverage. Keep the default three replicas. | Distribute eligible storage and processing work while reserving room for recovery and maintenance. |
| Traffic stays high | No edits to individual Feeds. | Identify the actual CPU, IO, network, or processing bottleneck; request one additional Node when policy allows it. |
| New capacity joins | Keep using the same SDK calls. | Catch up and verify replicas where needed, move eligible range ownership or compute leases, warm caches, then measure before the next step. |
| Foreground throughput expands | Retain the service policy. | Use extra capacity for parallelizable work, allowing sufficient IO/CPU budget for retained history, storage maintenance, and recovery. |
| Retained data keeps growing | Keep the retention and durability requirements explicit. | Treat storage demand and maintenance backlog as capacity signals even when event throughput falls. |
| Traffic subsides | Keep eight as the configured floor. | Remove only surplus Nodes that can be fully drained without violating storage headroom, durability, or maintenance budgets; otherwise retain the capacity and explain why. |

The baseline eight Nodes are a **capacity floor**, not eight permanently assigned “storage-only” machines. New Nodes are not automatically “throughput-only” either. Role-aware placement should move work according to bottlenecks and capabilities. Offloading foreground work can free resources for storage and cleanup, but that result requires placement and scheduling — simply starting another process does not create usable throughput.

The service may need to remain above its configured minimum because its retained data no longer fits safely below that size. Nor can adding Nodes parallelize one strictly ordered hot Key. Physical capacity and ordering constraints still matter; the difference is who manages them and how clearly the platform explains them.

### What “log cleaning” means here

Whitewater does not turn a Feed into a Kafka-style compacted topic. Committed history stays immutable until its History Policy permits expiry or tiering. Current/queryable state belongs to separate Indexes and StateStores.

In this design, background work includes expired-history reclamation, segment lifecycle/tiering, Index maintenance, checkpoints, and replica repair. It needs bounded IO/CPU budgets and recovery headroom, with foreground work protected. Pressure is not permission to delete still-retained business data. These lifecycle and scheduling capabilities remain delivery work, not a claim that all cleanup is already automatic.

## Optional RF control: one service policy, not one decision per Feed

The proposed user experience is:

- [ ] Start with RF3 without asking each application team to choose replicas.
- [ ] If stronger durability is required, let an authorized operator select a Riverbed-wide Durability Policy at the server/control-plane level.
- [ ] Let existing and future Feeds inherit that policy, with controlled reconciliation for existing data rather than only changing the default for new Feeds.
- [ ] Optionally allow a policy-bounded algorithm to recommend or reconcile RF changes after the protocol and evidence support them.
- [ ] Keep SDKs and ordinary Feed creation independent of those physical choices.

“Server-level” should mean **one consistent Riverbed policy**, not independent settings on individual Nodes that can disagree about what constitutes a durable write.

Adaptive RF is a proposal, not a chosen algorithm. Before implementing it, define the allowed RF values, quorum and membership-transition rules, failure-domain constraints, cost limits, cooldowns, and interaction with repair and scaling. A new replica must catch up and be verified before it counts. Removing a replica must preserve the configured durability target and never go below the three-replica baseline. Incomplete transitions must pause or recover safely and expose a verified rollback path, not silently weaken acknowledgements to make progress.

Any algorithm must honor operator intent. It must not reduce a stronger durability policy simply because traffic is quiet, or increase RF in response to write pressure without considering the extra replication load. Explain the reason, expected benefit, cost, transition state, and next safe action.

**Set durability intent once. Let the platform reconcile the physical copies.** No routine replication-factor prompt on every Feed.

## What stays the customer's responsibility?

Whitewater aims to automate mechanics, not conceal business decisions:

- Choose a service capacity floor, ceiling, and spending limits.
- Decide how long data must be kept and what recovery objectives are required.
- Provide real failure domains; three containers on one host are not three independent machines.
- Define business ordering Keys, access boundaries, and supported application processing behavior.
- Review exceptional operations and proposed policy changes with their impact and audit trail.

The scaling controller runs outside the data Nodes; Nodes do not need Docker or orchestrator credentials. Safe automation is bounded and explainable, rather than a privileged process that can add or remove infrastructure indiscriminately.

## Delivery boundary: sell the experience, prove the guarantee

The partition-free public model, RF3 baseline, and Rust implementation underpin this comparison. Automatic useful placement over larger live Riverbeds, complete Node draining, lifecycle scheduling, and production operational guarantees still require their acceptance evidence. The current code already combines configured storage identities with independently registered storage Nodes, excludes draining/retired Nodes, and uses rendezvous hashing to place three replicas and an owner per Feed range. Replication factor remains fixed at three. The existing request-local autoscaling policy exposes minimum/maximum Node counts but does not yet provide an authoritative persisted Riverbed policy or restore a below-minimum service independently of traffic. See [capacity-policy tasks](tasks.md#m51--riverbed-capacity-policy-and-minimum-node-reconciliation) and [configurable-replication tasks](tasks.md#m52--service-wide-durability-policy-and-configurable-replication-factor). Discovery or a placement simulation alone does not prove an eight-Node data service.

The eight-Node example and optional service-wide/adaptive RF controls describe direction and proposals. They do not change the architecture contract, introduce configuration keys, or certify throughput, storage capacity, or production readiness. Before promoting them to shipped features, demonstrate safe scale-out/in, sustained storage growth, maintenance under load, Node loss, interrupted movement, policy transitions, and recovery without losing acknowledged data.

**Whitewater's pitch: build around your business data, not around partition arithmetic and replica administration.**

## Read more

- [Why Whitewater](why-whitewater.md)
- [Whitewater architecture](kafka-successor-architecture.md)
- [Operational experience and acceptance criteria](operational-experience.md)
- [Kafka pain points](kafka-pain-points.md)
- [Whitewater SDKs: choose your language, not your capabilities](whitewater-sdks.md)
- [Whitewater UI](whitewater-ui.md)
- [Delivery milestones](tasks.md)
