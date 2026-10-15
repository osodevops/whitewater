# Whitewater vs Kafka: partition-free, durable by default

> No partition planning. No durability tuning exercise. Build around your business data — Whitewater handles placement and enforces its durability baseline.

**Whitewater is a partition-free streaming platform and distributed database, built in Rust.** A Feed has no user-defined partitions. You do not choose a partition count when creating it, assign consumers to numbered partitions, or plan repartitioning when your workload grows. You choose a Feed and a business Key; the Riverbed manages the storage and replication beneath them.

This is the central difference from Kafka: **partition management is removed from the public model, not made into a friendlier configuration screen.** Applications work with Feeds, Keys, opaque Cursors, Subscriptions, and StateStores. Operators manage service capacity, durability intent, and resource policies rather than a partition plan for each Feed.

**Partition-free by design. Durable by default. Replication managed by the platform.**

### Durable by default — protection is not an opt-in feature

Whitewater's RF3 baseline uses three replicas and requires matching durable data and commit evidence from at least two before acknowledging an ordinary write. There is no `min.insync` setting to remember or accidentally lower. With all replicas healthy, all three participate; one replica alone can never report durable success.

**Developers get a defined success contract. DevOps teams have fewer dangerous configuration combinations to manage. Operators get safe refusal when the required durability cannot be met.** The benefit is not that Kafka cannot be durable — it is that Whitewater builds this baseline into the protocol rather than asking each application team to configure it correctly.

This is backed by the [durability tests below](#what-the-durability-tests-demonstrate), not a promise of unlimited fault tolerance or a substitute for independent failure domains and tested backups.

## Coordination is built into the Riverbed from the outset

**A Riverbed includes its Control Plane as part of the platform, not as an afterthought for application teams to assemble.** The component comparable to Kafka's KRaft metadata coordination is Whitewater's integrated Control Plane; the Riverbed is the whole cooperating installation, including storage and processing.

Whitewater's Rust Node binary includes Control Plane participation. The standard three-Node development topology uses majority-committed metadata for logical resources and ownership decisions. Applications address Domains and Feeds through the same Control API regardless of which Node receives a request; they do not manage a separate public consensus resource. Feed-record replication remains a distinct data-plane responsibility, not something the metadata consensus alone guarantees.

The intended benefit is **less configuration exposed to applications and less repeated setup per Feed**: no partition count, no per-Feed replica assignment, and no configurable minimum in-sync count. Coordination and the logical resource model are designed together, while operators focus on service capacity, identities, security, and policy.

This does not mean zero configuration: Node identities, endpoints, Control Plane bootstrap, credentials, and transport still need correct setup today. Nor does it mean modern Kafka needs ZooKeeper — KRaft is already Kafka's integrated metadata system. The question for Whitewater is how much of the remaining operational configuration can be safely standardized or automated, rather than merely renamed. See the [Control Plane contract](control-plane.md).

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

These are comparison and research notes intended to invite practical feedback, not a claim that Whitewater wins every trade-off or that every roadmap feature has shipped. They compare Kafka's public model and typical operational responsibilities with Whitewater's intended simpler experience. Managed Kafka, operators, good defaults, and newer consumer protocols can automate or reduce several Kafka responsibilities; Whitewater's goal is to remove them from the application contract itself.

- **Contract:** a defined Whitewater public model or baseline invariant, not certification of every production failure scenario.
- **Direction:** an architectural requirement with implementation and acceptance work still to complete.
- **Proposal:** a product option discussed here, not a shipped setting or approved algorithm.

The checkboxes below are for a reader evaluating which simplifications matter to their team. They are not engineering completion markers. See [delivery milestones](tasks.md) for implementation evidence.

## Side-by-side: the easier-platform checklist

| Evaluate | The job | Kafka model / typical team responsibility | Whitewater experience | Basis |
|---|---|---|---|---|
| [ ] | Create a partition-free event stream | A topic has partitions, with a count and replication factor explicitly chosen or inherited from defaults. | Create a Feed with no public partitions: no partition count to choose, no partition assignments to maintain, and no replica layout to supply. | Contract |
| [ ] | Preserve business ordering | Keys map to partitions; partitioning strategy and count changes can affect that mapping. | Supply the business Key. Same-Feed/same-Key accepted order is the contract; physical placement stays internal. | Contract |
| [ ] | **Durable by default** | Coordinate replication factor, producer acknowledgements, `min.insync.replicas`, and related policies. | **Three replicas by default; a fixed minimum of two durable, matching replicas.** No `min.insync` argument. All three participate when healthy; the owner plus either follower can acknowledge when the other is unavailable. One replica alone never succeeds. | Implemented and tested: [majority commit](../tests/majority_commit.rs) and [replica protocol](../tests/replica_append_protocol.rs) |
| [ ] | Built-in metadata coordination | KRaft coordinates Kafka metadata; operators configure broker/controller roles, endpoints, and quorum bootstrap. | The Riverbed includes its Control Plane in the Whitewater Node binary from the outset, behind one logical resource/control model. No separate consensus resource for applications to configure. | Integrated Control Plane exists; bootstrap/security configuration remains, with further operational simplification a design goal |
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
| [ ] | Ask why an incident is happening | Correlate evidence using operational expertise, runbooks, and available monitoring/assistant tooling. | Planned AI Assist through any authorized Node correlates scoped evidence, identifies likely causes, and recommends safe next steps without requiring knowledge of data ownership. | Direction; [Operations Advisor](operations-advisor.md), not implemented |
| [ ] | Run the data service | Operate a JVM-based broker runtime and its resource configuration. | Run a native Rust Node binary with no JVM or garbage-collector tuning in the data service. | Contract; performance still needs measured evidence |

**The advantage is fewer recurring decisions — not merely fewer visible settings.** Whitewater must own the placement, recovery, and safety work that disappears from the application interface.

## What changes in your working day?

**Whitewater aims to reduce the infrastructure knowledge needed to build applications — and the manual work needed to keep them running.** The benefits below describe the product direction, building on the partition-free contract; full automation, security, SDK parity, and operational workflows remain subject to their delivery gates.

### For developers: concentrate on application behavior

- **Business Keys, not infrastructure topology.** Describe which events must stay ordered without choosing partition counts or coupling worker deployment to storage layout.
- **History and current state in one platform.** Replay a Feed and look up a StateStore by primary or named secondary key, rather than automatically adding another database and a synchronization pipeline for basic enrichment.
- **Learn once, use your language.** The goal is the same streaming and state capabilities across all six SDKs, not a full processing suite for Java and basic producer/consumer clients for everyone else.
- **Retries with a defined meaning.** Distinguish a rejected request from one that may have committed, preserve its identity, and keep Whitewater processing effects separate from external side effects. A timeout is not permission to create a duplicate payment or event.
- **Turn investigations into repeatable tests.** Whitewater UI's samples, Views, filters, and Playbooks are intended to help capture an event, replay a scenario, and retain useful test evidence instead of rebuilding the investigation after every fix.

**Your business logic should describe an order, payment, or customer — not which machine or partition currently owns it.**

### For DevOps: deploy intent, not placement instructions

- **One control model across tools.** WCL, typed APIs, CLI, SDKs, and the UI converge on the same validated control operations. Automation should not depend on a special UI-only administrative path.
- **Repeatable environment setup.** Reuse resource declarations and, as policy support lands, access and service configuration across development, test, and production instead of preparing each environment by hand.
- **Capacity and durability are separate decisions.** A larger service footprint should not require changing every Feed or multiplying every record's replica count. Declare the service floor, ceiling, and durability intent; let the platform reconcile placement.
- **Production-shaped development.** Standard development starts with three Nodes so distributed behavior is exercised early. Three containers on a laptop still share one failure domain; local convenience is not a production-resilience claim.
- **Understand effective configuration.** The intended policy tooling shows what is active, where it came from, and the impact of a proposed change before it is applied.

**Declare the service you need. Whitewater's goal is to manage how that service is placed, protected, and scaled.**

### For operations: make ordinary changes routine

- **Explain why, not just show lag.** Trace delay to replication, storage, Reader capacity, state freshness, a hot Key, or another evidenced bottleneck rather than treating every slow consumer as the same problem.
- **Replace and maintain safely.** Build on verified catch-up and drain machinery, with the planned ACK-aware maintenance gates refusing unsafe removal or explaining when a maintenance window is necessary.
- **Preview before changing.** Planned impact analysis should make capacity, cost, durability, and availability consequences visible before a policy change is approved.
- **Recover with evidence.** The recovery goal includes isolated restore exercises, verified data and progress, and visible recovery objectives — not just a successful backup-file upload.
- **Protect live workloads.** Planned Domain budgets and background-work scheduling prevent replay, repair, or one busy tenant from consuming all resources needed for live processing.

**See what happened, what Whitewater is doing about it, and the next safe action.**

## Coming: AI Assist through any Node

**Planned, not implemented:** AI Assist will be accessible through any available, authorized Whitewater Node, helping developers and operators investigate issues without first finding the machine that owns the affected data. This is the [Whitewater Operations Advisor](operations-advisor.md) direction: a consistent entry point into an evidence-backed investigation, not a separate troubleshooting system for each Node.

Ask questions such as:

- “Why is this Writer retrying?”
- “Why is this Subscription falling behind?”
- “Which resource is limiting throughput, and would another Node help?”
- “Why is this StateStore behind, or this Pipe no longer producing output?”
- “Is this Node safe to drain, and what is blocking it?”

AI Assist is intended to correlate authorized metrics, commit/repair evidence, logs, traces, and control decisions across the affected scope. Its explanation should identify the issue, affected resources, likely cause, supporting observations, what automatic recovery has already attempted, and recommended next steps with their risks. Facts and hypotheses must remain distinguishable; missing or contradictory evidence means an incomplete diagnosis, not invented certainty.

“Through any Node” describes access and routing, not a requirement to run an inference model inside every data process. Diagnostic collection and inference must be bounded and isolated from foreground traffic. If a Node or model is unavailable, normal streaming continues; if part of the Riverbed cannot be inspected, the answer must state that limitation. Tenant permissions, redaction, and deployment policy govern which data may be inspected or sent to a model.

The first mode is **read-only assistance**. AI helps determine what is wrong and recommends a safe response; it does not silently change access, lower replication or ACK guarantees, reset progress, or remove a Node. Any later supported action remains subject to authorization, approval/policy gates, deterministic safety checks, and audit.

**Fewer responsibilities pushed onto applications. Safer automation for operators. Clear explanations when the platform cannot proceed — with AI assistance to help connect the evidence.**

## Replication is automatic, not absent

“Replica management-free” does not mean “no replicas.” It means an application developer should not have to decide how many physical copies a Feed needs or where to put them before writing the first event.

**The minimum in-sync requirement is always two. There is no `min.insync` argument to choose or lower.** In the implemented RF3 protocol, that means matching durable record and commit evidence from the owner and at least one assigned follower — not merely two Nodes reporting healthy.

### What the durability tests demonstrate

The [majority-commit suite](../tests/majority_commit.rs) uses three replica services with file-backed storage and fault-injected transport. The [replica-protocol suite](../tests/replica_append_protocol.rs) checks persistence, checksums, fencing, visibility, and retry behavior.

| Tested situation | Result |
|---|---|
| All three replicas healthy | Success with three matching durable-replica and commit-evidence entries. |
| Either follower unavailable | The owner and remaining follower succeed with two matching durable-replica and commit-evidence entries. |
| Both followers unavailable | Refusal: one healthy replica never returns success. |
| Frames persist but neither follower persists commit evidence | Retryable ambiguous failure, not ordinary write success. |
| A follower reports a conflicting digest | Refusal rather than accepting contradictory evidence. |
| The same append is retried after commit | The original MessageId, Cursor, and position are returned; the retry is deduplicated. |
| A replica holds an uncommitted frame | It stays invisible to committed reads until the commit position is durably recorded. |

Both suites were run for this documentation review: **23 majority-commit tests and 12 replica-protocol tests passed**. These are in-process integration tests with file-backed replica services, not a substitute for live multi-Node acceptance, cross-language conformance, or production certification. Deferred processing-effect appends have a separate pending/release phase; durable staging alone is not a committed output.

The implementation attempts replication to both followers and enforces the fixed two-copy success floor. These tests do **not** establish an independently configurable ACK ALL mode or a Kafka-style ISR membership service, and do not require all three configured replicas to acknowledge every successful write. Any future ACK naming, override, or stronger policy must preserve the proven floor and have its own conformance evidence; the [remaining tasks](tasks.md#m53--fixed-durability-floor-and-acknowledgement-policy) cover those gaps rather than reimplementing RF3 durability.

**Durability is built into the platform, not assembled from a settings matrix.**

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
