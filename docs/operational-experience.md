# Whitewater Operational Experience

> Expected operational change must not wake somebody up.

## Document status

- **Status:** Experience goals and design requirements
- **Product:** Whitewater by FinnStream
- **Category:** Distributed database and partitionless streaming platform
- **Companion documents:** [Sequence diagrams](sequence-diagrams.md), [Kafka pain points](kafka-pain-points.md), [Why Whitewater](why-whitewater.md), and [Whitewater architecture](kafka-successor-architecture.md)
- **Audience:** Application developers, platform engineers, SREs, security teams, data engineers, FinOps, and incident responders

This document turns recurring Kafka operational pain into explicit Whitewater behavior. It is not a claim that every Kafka installation suffers every problem. Mature teams operate Kafka successfully, and managed platforms remove substantial work. The goal is to learn from the expertise, tooling, and runbooks those teams had to build and make the common safe behavior part of Whitewater itself.

## A humane infrastructure principle

Infrastructure should not punish people for ordinary change.

The following events are normal and must be boring:

- Deploying or restarting a Reader
- Adding or removing capacity
- Replacing a failed Node
- Rotating an API key or certificate
- Renaming a Feed
- Changing a History Policy
- Rebuilding an Index
- Replaying old events
- Performing a rolling upgrade
- Losing a zone
- Restoring from backup
- Investigating slow delivery

A person should not need perfect recall of a configuration matrix while responding to an incident. Whitewater should prefer safe refusal, gradual action, clear explanations, and reversible changes over maximum configurability.

## Perspectives we must design for

### Application developer

The application developer wants to append and read events while preserving business ordering. They should not need to:

- Predict partition count years in advance
- Choose a partitioner
- Handle partition assignment callbacks
- Coordinate poll timing with business processing duration
- Interpret topic-partition-offset tuples
- Configure producer idempotency correctly
- Tune batches before the client has observed real traffic
- Build a remote-query layer over local state stores
- Create retry and dead-letter Feed naming conventions for every application
- Understand which server currently owns their data

Whitewater promise:

```text
choose Feed
choose Key
append
receive Cursor
subscribe with capacity
acknowledge or atomically produce effects
```

### DevOps and platform engineer

The platform engineer wants a service that behaves like a modern orchestrated workload. They should not need a recurring project for:

- Assigning data to new Nodes after scaling out
- Planning partition reassignment batches
- Selecting reassignment throttles that are both safe and useful
- Draining a Node by moving named physical partitions
- Balancing disks while live traffic competes for IO
- Maintaining different broker and controller manifests
- Preserving a large matrix of listener, role, storage, and security settings
- Writing one-off scripts to identify dangerous configuration combinations
- Comparing development and production topologies that behave differently

Whitewater promise:

- Nodes contribute capacity and advertise failure domains.
- The Fabric incrementally places and moves internal ranges.
- Movement yields to foreground SLOs and has a disruption budget.
- Scale-in is blocked until the candidate is drained and verified.
- Desired state is declarative and reconciled through an orchestrator adapter.
- One Node binary supports automatic internal role placement.

### Site reliability engineer

The SRE wants symptoms tied to causes and safe recovery actions. They should not receive only “lag increased” while determining whether the source is:

- Slow application processing
- Reader starvation
- A rebalance
- A hot partition
- A shrinking ISR
- Disk saturation
- Page-cache eviction caused by historical reads
- Compaction pressure
- Replica catch-up
- Controller instability
- Network saturation during reassignment
- Object-store latency
- A security or quota rejection

Whitewater promise:

Every pressure or delay signal identifies its current limiting stage:

```text
writer -> admission -> quorum -> storage -> index -> subscription -> reader
```

Diagnostic output must include:

1. What is happening
2. Which logical resources are affected
3. Why the Fabric believes it is happening
4. What automatic action is in progress
5. Whether durability or availability is reduced
6. What a person can safely do next

### Incident responder

The incident responder needs bounded, reversible operations. They should not have to decide under pressure whether to:

- Enable unclean leader election
- Lower minimum ISR
- Disable acknowledgements
- Remove a replica before it catches up
- Cancel a large reassignment manually
- Guess which throttle will avoid making the incident worse
- Reset offsets without understanding duplicate or loss boundaries
- Delete data to recover disk space

Whitewater promise:

- Dangerous durability reductions are not ordinary runtime settings.
- Recovery actions expose expected data-loss and availability consequences before execution.
- Every long-running control-plane action has progress, pause, cancel, and rollback semantics where correctness permits.
- The Fabric automatically stops background movement when foreground health degrades.
- Support bundles are automatically redacted and contain the relevant decision history.

### Security engineer

The security engineer wants secure defaults and understandable identities. They should not need to assemble security from plaintext listeners, TLS, mTLS, several SASL mechanisms, JAAS, ACL patterns, super users, and listener-specific overrides.

Whitewater promise:

- Authenticated encryption is mandatory for client and inter-Node traffic. Native TLS/mTLS is the default; a trusted service mesh or equivalent orchestrator-provided transport may satisfy the inter-Node guarantee when peer identity, rotation, audit, and downgrade prevention are verified.
- API keys are scoped identities, stored only as verifiers, and designed for rotation.
- Space and Feed capabilities inherit predictably.
- Deny and allow decisions are explainable through an authorization trace.
- Every administrative mutation produces an audit event.
- Authentication secrets and data-encryption keys are separate.
- Customer-managed encryption uses envelope encryption and named key references.
- Logs, support bundles, and metrics never expose credentials or payloads by default.

### Data engineer

The data engineer wants replayable history and queryable state without assembling several products for basic lookup. They should not need to:

- Treat a compacted topic as a database even though it has no point-query API
- Run a local RocksDB store plus a changelog Feed for every materialized view
- Build routing logic to find the instance holding a state-store key
- Replay years of history after routine movement when a checkpoint could be transferred
- Infer Index freshness from application-specific metrics
- Manage repartition topics because a processing key changed

Whitewater promise:

- Feeds remain immutable history.
- Indexes are persisted, replicated, queryable resources.
- Each Index reports the applied Cursor and freshness.
- Index checkpoints transfer before tail catch-up.
- Query routing follows Index ownership automatically.
- Re-keying and internal shuffle are Pipe implementation details with explicit cost reporting.

### FinOps and capacity owner

The capacity owner wants cost to follow useful work. They should not need to accept:

- Idle brokers after scale-out because existing data did not move
- Permanent over-partitioning for hypothetical future throughput
- Duplicate hot data in changelogs and rebuilt state stores without attribution
- Historical scans evicting latency-sensitive cache without a budget
- Unknown per-team storage, replication, egress, and Index cost
- Standby disaster-recovery infrastructure with unclear readiness

Whitewater promise:

- Cost is attributed to Space, Feed, Subscription, Pipe, and Index.
- Capacity recommendations explain which resource caused the recommendation.
- Autoscaling is slow, hysteretic, and bounded.
- Background movement has bandwidth and cost budgets.
- Hot, warm, and object-storage bytes are reported separately.
- Index cost is visible before creation and continuously afterwards.
- DR readiness includes measurable RPO, restore time, and last successful exercise.

## Recurring operational pain and Whitewater responses

### Adding capacity that does not immediately provide capacity

In Kafka, adding a broker does not automatically redistribute existing partitions. Operators plan reassignment, monitor catch-up, choose throttles, and verify completion. Moving too aggressively competes with production traffic; moving too slowly may take hours or days.

Whitewater response:

- New Nodes join automatically.
- Placement uses capacity, failure domain, active pressure, and data locality.
- Internal ranges move incrementally.
- Movement is rate-limited by an SLO-aware disruption budget.
- New placement is not counted as available until replicas are caught up and verified.
- Autoscaling waits through a cooldown before making another decision.

Acceptance criteria:

- Adding one Node requires no Feed-level plan.
- Foreground latency remains within its configured movement budget.
- The Fabric reports bytes remaining, estimated completion range, and limiting resource.
- A failed move resumes or rolls back without leaving ambiguous ownership.

### Consumer deployment causing a processing pause

Traditional group protocols can require group-wide synchronization. Slow startup, long processing, missed polls, or rolling deployment may extend or repeatedly trigger rebalances.

Whitewater response:

- Subscription work is split into small leases.
- Existing leases remain valid during unrelated membership changes.
- New Readers acquire a bounded number of leases gradually.
- Lease transfer honors Reader capacity and locality.
- Epoch fencing prevents stale acknowledgement.

Acceptance criteria:

- Adding one Reader does not pause unaffected Readers.
- Removing one Reader affects only its leased ranges.
- A rolling deployment produces bounded duplicate work without a global stop.
- Lease movement rate is visible and controllable.

### A slow or poisoned event causing group instability

Kafka poll timing can confuse “processing a long task” with “consumer failed,” causing eviction and rebalance. Increasing timeouts delays real failure detection.

Whitewater response:

- Reader liveness is independent from the processing time of an individual event.
- Capacity credits bound in-flight work.
- Leases renew independently while processing remains healthy.
- A poison event can be retried, delayed, quarantined, or skipped according to Subscription policy without stopping unrelated keys.

Acceptance criteria:

- One long-running key does not stall unrelated keys.
- Failure detection does not require guessing the longest future task duration.
- Retry state and final disposition are queryable.

### Disk filling as a cliff-edge failure

Kafka local disks hold partition segments, replica catch-up, indexes, and compaction work. A full or failed log directory may make replicas unavailable, shrink ISR, reject writes, or stop a broker. Existing data does not automatically move just because another disk is empty.

Whitewater response:

- Storage admission reserves emergency and recovery headroom.
- Sealed segments tier continuously according to policy.
- Placement considers projected growth, not only current bytes.
- The Fabric begins evacuation before a hard threshold.
- Background Index maintenance and historical reads yield to active writes.
- A full device is isolated; the Node remains available for unaffected resources where safe.

Acceptance criteria:

- No acknowledged write fails solely because an expected background task consumed reserved headroom.
- Operators receive time-to-exhaustion and the responsible Spaces/Feeds.
- Automatic action begins before the emergency threshold.
- Recovery does not require deleting unknown business data by hand.

### Historical replay damaging live latency

Large backfills can displace hot page-cache data and compete for disk and network bandwidth with tailing readers and writers.

Whitewater response:

- Historical and live workloads have separate budgets and scheduling classes.
- Object-store reads use admission control and predictive prefetch.
- Live tail latency takes precedence by default.
- Replay can request a completion target and receive an estimated cost.

Acceptance criteria:

- Historical work cannot consume all local cache, disk queue, or egress.
- The cause of throttled replay is reported explicitly.
- Teams may reserve replay capacity without retuning every Reader.

### Partition-count decisions becoming permanent architecture

Partition count controls Kafka parallelism, placement, and ordering. Increasing it changes keyed distribution; reducing it is not a routine operation. Teams often over-partition for future growth, paying metadata and file overhead immediately.

Whitewater response:

- Feed creation asks for no parallelism count.
- Key ranges split and merge internally.
- Existing keys retain ordering across range-generation changes.
- Metadata is aggregated so millions of logical ranges do not become millions of expensive control-plane objects.

Acceptance criteria:

- Feed throughput can grow without changing its public definition.
- No client receives a topology-change callback.
- Split and merge progress is observable and reversible.

### Hot keys and uneven traffic

A skewed key can overload one Kafka partition and its leader while other brokers remain idle. Adding partitions cannot parallelize one key without relaxing ordering.

Whitewater response:

- The fundamental hot-key limit is reported honestly.
- Hot keys are isolated from unrelated keys.
- More CPU, cache, and IO priority may be assigned around the hot key.
- Applications receive key-level diagnostics and options for explicitly changing their ordering model.

Acceptance criteria:

- One hot key does not obscure pressure from the rest of the Feed.
- The Fabric never claims that adding Nodes can parallelize one strictly ordered key.

### Replication settings trading durability for availability unexpectedly

Kafka durability depends on interacting producer acknowledgements, replication factor, ISR, minimum ISR, election policy, and replica health. During incidents, changing one setting can restore writes while silently weakening guarantees.

Whitewater response:

- Baseline three replicas and quorum acknowledgement are invariants.
- Reduced durability is a named emergency mode with explicit scope, expiry, audit, and predicted loss envelope—not a casual configuration edit.
- Writes report the Durability Policy actually satisfied.

Acceptance criteria:

- A success response has one stable documented meaning.
- Emergency overrides automatically expire.
- Control APIs explain which failure scenarios an override permits.

### Rolling upgrades becoming a topology exercise

Kafka upgrades require compatibility planning, rolling broker/controller restarts, ISR and disk-health gates, and sometimes protocol or metadata-version sequencing.

Whitewater response:

- Nodes advertise protocol, storage, and feature compatibility.
- The control plane computes a rolling plan and blocks unsafe progression.
- One Node drains at a time within a disruption budget.
- Mixed-version behavior is covered by compatibility tests.
- Rollback remains possible until an explicitly identified irreversible storage migration.

Acceptance criteria:

- Upgrade status identifies the current Node, gate, and rollback boundary.
- The process pauses automatically on reduced durability or SLO breach.
- Routine upgrades require no Feed movement plan.

### Security bootstrap and rotation causing outages

Kafka security may involve listener-specific TLS and SASL settings, JAAS, keystores, truststores, ACLs, super-user rules, and component-specific credentials. Certificate or principal mistakes can isolate brokers, controllers, or clients.

Whitewater response:

- A development Fabric bootstraps a local CA and short-lived credentials automatically.
- Production integrates with an external CA/KMS but uses the same protocol.
- Credential rotation supports overlapping validity.
- Authorization policy can be simulated before activation.
- Every denial can return a safe authorization decision trace.

Acceptance criteria:

- Rotating credentials causes no application outage.
- No secret is placed in command-line arguments, logs, or metrics.
- Policy changes support dry-run impact analysis.

### Queryable state requiring a second architecture

Compacted topics preserve eventual latest values but do not provide a direct point-query interface. Kafka Streams materializes partition-local state stores, often backed by RocksDB and compacted changelogs; remote query routing remains an application concern.

Whitewater response:

- Key and secondary Indexes are Fabric resources.
- Index state is persisted and replicated directly.
- Index freshness is represented by an applied Cursor.
- Query routing follows ownership automatically.
- Checkpoints transfer before replaying the remaining Feed tail.
- Managed rolling-window Pipes keep durable StateStore state on RF3 storage replicas while epoch-fenced compute leases can run on the baseline three Nodes or move to added compute capacity; no dedicated fourth Node is required.
- Users choose a business Key, duration, and aggregate, not window panes/slide, co-partitioning, or an application-maintained changelog. Watermarks, late events, state cost, and recovery progress are explained.

Acceptance criteria:

- A point lookup does not scan a Feed.
- Moving an Index or a rolling-window compute lease does not require application routing changes.
- Strict reads reject stale replicas rather than silently returning old state.
- A three-Node Fabric runs the same window contract as a scaled-out Fabric, while late-event handling and compute/state resource costs remain visible.

### Disaster recovery requiring parallel offset translation

Cross-cluster Kafka DR commonly needs a replication system, topic naming policy, consumer-offset translation, checkpoint intervals, client redirection, and rehearsed failover/failback. Offset synchronization is asynchronous and may produce gaps or duplicates around failover.

Whitewater response:

- FeedId and Cursor formats are designed for Fabric/region identity from the beginning.
- A DR policy declares RPO, RTO, directionality, and conflict rules.
- Remote replicas and Cursor checkpoints are control-plane resources rather than separately assembled connectors.
- Failover and failback are exercised continuously with non-production probes.

Acceptance criteria:

- DR readiness reports last replicated Cursor, estimated RPO, and last successful failover exercise.
- Promotion is idempotent and auditable.
- Clients use a stable discovery identity rather than manually renamed Feeds.

### Large events forcing cluster-wide tuning

Kafka message-size limits span broker, topic, producer, consumer, replica fetch, and connector settings. A mismatch may permit writes that replicas or Readers cannot transfer.

Whitewater response:

- One negotiated protocol limit applies end to end.
- Larger payloads use an explicit blob/reference capability where appropriate.
- Admission checks verify that the selected Durability Policy and all required replicas can carry the event before acceptance.

Acceptance criteria:

- A rejected event identifies the exact limit and alternative.
- No accepted event is too large for required replication or delivery.

### Too many dashboards but not enough explanation

Kafka operations often combine JMX metrics, exporter mappings, consumer-lag tools, logs, controller state, disk metrics, client metrics, and ecosystem-specific dashboards. Cardinality and naming vary across components.

Whitewater response:

- Metrics, traces, logs, events, and control-plane decisions share Fabric, Space, FeedId, SubscriptionId, PipeId, IndexId, and NodeId dimensions.
- High-cardinality key diagnostics are sampled and bounded.
- Every automatic action records its inputs, policy, decision, and result.
- A built-in health explanation API answers “why is this delayed?”

Acceptance criteria:

- A person can move from a delayed Subscription to the responsible stage without joining unrelated dashboards manually.
- Telemetry cost and cardinality are reported and bounded.

## Wholesome operational design rules

### Prefer safe refusal over surprising success

If Whitewater cannot satisfy the promised durability, authorization, ordering, or Index consistency, it should reject or explicitly downgrade only through a named emergency policy. It must not acknowledge a weaker result under the same success response.

### Explain automatic behavior

Automation without explanation becomes another source of fear. Every automatic placement, throttle, scale, drain, repair, or failover decision records:

- Observed inputs
- Applicable policy
- Alternatives considered
- Chosen action
- Expected impact
- Completion and rollback state

### Make changes gradual

Scale, movement, restoration, and rebalancing occur in bounded steps. The system pauses between steps to measure the result. One control loop should not make several speculative changes before feedback arrives.

### Make routine actions reversible

Renames, policy changes, scaling, and upgrades should have rollback windows. Irreversible actions require explicit confirmation and name the affected FeedIds and retained-history boundary.

### Protect the person on call

Errors should be actionable without requiring source-code archaeology. Runbooks should be generated from live state where possible. Support bundles must redact secrets and payloads automatically.

### Do not blame applications for infrastructure ambiguity

“Consumer too slow” is not a diagnosis. Whitewater should distinguish CPU saturation, an exhausted credit budget, a hot key, blocked downstream IO, failed acknowledgements, and service-level quota enforcement.

### Preserve quiet systems

A small Feed should remain cheap even inside a large Fabric. Background balancing, metrics, and metadata should not turn low-volume workloads into constant noise.

### Keep development honest

Development uses at least three Nodes, TLS, API keys, the real membership protocol, and the same Cursor and durability contracts. Convenience tooling automates these pieces rather than deleting them.

### Expose cost before commitment

Before creating an Index, increasing history, adding regions, or changing durability, the control API estimates additional storage, replication, IO, and recovery cost.

### Design for deletion obligations

Immutable history must coexist with privacy and legal deletion. Whitewater should support policy-driven expiry and cryptographic erasure through isolated encryption keys without pretending that immutable bytes can simply be forgotten in every backup. Exact guarantees and audit evidence must be explicit.

## Day-two operation acceptance tests

Whitewater is not operationally better merely because its diagrams are cleaner. The following scenarios should become automated acceptance and fault-injection suites.

### Capacity and placement

- Add a Node during peak traffic without an SLO breach.
- Remove a fully drained Node without unavailable Feeds.
- Refuse removal when unique data or ownership remains.
- Recover from a failed move without duplicate ownership.
- Isolate a hot key while unrelated keys maintain latency.

### Reader behavior

- Add and remove Readers without a global processing pause.
- Kill a Reader and fence stale acknowledgements.
- Process one long event without lease churn for unrelated keys.
- Quarantine a poisoned event without stopping the Feed.

### Storage and failure

- Fill a device gradually and verify early evacuation/backpressure.
- Lose one Node and maintain quorum writes.
- Lose a zone according to the selected Durability Policy.
- Corrupt a segment and repair it from a verified replica.
- Restore an Index from checkpoint and Feed tail within its objective.
- Run historical replay without breaching live-tail SLO.

### Security

- Rotate API keys and certificates without disconnecting healthy clients.
- Reject plaintext traffic.
- Verify that logs and support bundles contain no credentials.
- Simulate a policy change and list affected identities before activation.
- Revoke one identity without restarting Nodes.

### Upgrade and DR

- Roll forward and backward across every supported adjacent version.
- Pause an upgrade automatically when replica health degrades.
- Promote a remote Fabric and report measured RPO/RTO.
- Fail back without name changes or manual Cursor translation.
- Exercise DR automatically and prove the standby is usable.

### Cost and isolation

- Attribute hot, warm, remote, replicated, and indexed bytes per Space.
- Enforce one Space's quotas without stalling another.
- Throttle background maintenance before foreground traffic.
- Show the cost estimate and actual cost of an Index.

## Operator-facing APIs and tools

Whitewater should expose stable machine interfaces before building a large UI:

- Declarative desired-state API
- Health explanation API
- Placement and movement plan API
- Drain and safe-to-remove API
- Feed and Index freshness API
- Authorization simulation API
- Upgrade plan and gate API
- DR readiness and exercise API
- Cost-estimation API
- Redacted support-bundle API

`wwctl` should consume these APIs and produce both human-readable and structured output. Kubernetes, Nomad, ECS, Docker, and custom systems integrate through an orchestrator-neutral actuator interface.

An MCP server may expose read-only inspection and recommendation tools for engineers and assistants:

```text
whitewater.fabric.health
whitewater.fabric.explain_pressure
whitewater.feed.describe
whitewater.subscription.explain_delay
whitewater.index.freshness
whitewater.scale.recommend
whitewater.upgrade.plan
whitewater.dr.status
```

Mutating MCP tools must require explicit authorization and confirmation, and they must invoke the same idempotent control APIs as every other client. MCP is not the consensus, reconciliation, or real-time autoscaling protocol. The proposed [Whitewater Operations Advisor](operations-advisor.md) expands this read-only-first interface into evidence-backed investigation, schedule learning, and policy-gated remediation; it is not implemented yet.

## Research sources and lessons

The design requirements above synthesize recurring themes from Kafka's own documentation, KIPs, vendor documentation, community reports, and operator guides. These sources demonstrate the engineering work required to operate Kafka; they are not presented as evidence that Kafka cannot be operated successfully.

- [Apache Kafka KRaft operations](https://kafka.apache.org/43/operations/kraft/) documents broker/controller roles, metadata quorums, controller discovery, and why combined roles are not recommended for critical deployments.
- [Apache Kafka broker configuration](https://kafka.apache.org/43/configuration/broker-configs/) illustrates the breadth and interaction of controller, listener, storage, replication, and runtime settings.
- [Confluent: choosing topic partition counts](https://www.confluent.io/blog/how-choose-number-topics-partitions-kafka-cluster/) explains partition count as producer/consumer parallelism and notes ordering consequences when keyed-topic partition counts change.
- [Confluent partition determination](https://docs.confluent.io/kafka/operations-tools/partition-determination.html) recommends throughput measurement and discusses over-partitioning for future parallelism.
- [Confluent: debugging increased consumer rebalance time](https://www.confluent.io/blog/debug-apache-kafka-pt-3/) describes the group synchronization process and stop-the-world interval in the classic protocol.
- [Confluent consumer configuration](https://docs.confluent.io/platform/7.4/installation/configuration/consumer-configs.html) documents the trade-off among poll interval, heartbeat/session timeout, failure detection, and rebalance behavior.
- [KIP-236: interruptible partition reassignment](https://cwiki.apache.org/confluence/display/KAFKA/KIP-236:+Interruptible+Partition+Reassignment) records operational limitations and performance impact around expensive reassignment batches.
- [Apache Kafka SASL authentication](https://kafka.apache.org/43/security/authentication-using-sasl/), [TLS](https://kafka.apache.org/43/security/encryption-and-authentication-using-ssl/), and [ACL authorization](https://kafka.apache.org/43/security/authorization-and-acls/) show the flexibility and corresponding configuration surface of Kafka security.
- [KIP-545: automated consumer offset sync](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/133632115/KIP-545+support+automated+consumer+offset+sync+across+clusters+in+MM+2.0) explains why cross-cluster failover needs offset translation and periodic synchronization.
- [Apache Kafka Streams internal data management](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65864917) documents local state stores, compacted changelogs, repartition topics, and restoration.
- [Apache Kafka Streams interactive queries](https://kafka.apache.org/42/streams/developer-guide/interactive-queries/) explains partition-local state and the application responsibility around remote state-query routing.

## Product test

The simplest test of every proposed feature is:

> Does this remove a class of decisions and incidents for users, or merely move the same complexity behind a new name?

Whitewater succeeds only when safe operation is measurably calmer, recovery is bounded and explainable, and developers can work in terms of Spaces, Feeds, Keys, Cursors, and Subscriptions without inheriting the physical topology.
