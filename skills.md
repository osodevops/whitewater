# Whitewater Skills Map

Whitewater is both a distributed database and a partitionless streaming platform. Building it requires correctness-focused expertise across storage, consensus, networking, security, operations, and developer experience. No single contributor is expected to master every area; this document identifies the capabilities the project needs and how they fit together.

## Core engineering principles

Every contributor should understand these project-wide rules:

- Public applications use `riverbed -> domain -> feed -> key -> cursor -> subscription`.
- Physical partitions are never exposed.
- Keys define ordering.
- Readers and Subscriptions maintain independent Cursor positions; Readers cooperating in one Subscription share only that Subscription's durable progress.
- Records preserve separate signed nanosecond `event_time_ns` and `ingest_time_ns` values.
- FeedId is immutable; dotted FeedName is mutable.
- Feed history is immutable temporal data.
- Persisted replicated Indexes provide current and queryable state.
- Supported Riverbeds have at least three Nodes and three active replicas.
- TLS and scoped API-key authentication are mandatory defaults.
- Correctness contracts come before throughput optimization.
- Expected operational change should be gradual, explainable, and safe.
- Scale-in is forbidden until ownership and unique data have been drained.
- Performance claims require reproducible benchmarks.

## 1. Rust systems engineering

Required knowledge:

- Ownership, borrowing, lifetimes, and trait design
- Tokio and asynchronous cancellation
- Structured concurrency
- Bounded channels and backpressure
- Atomics and synchronization primitives
- Zero-copy and low-allocation IO
- Binary encoding and checksums
- Error taxonomies and recovery boundaries
- Safe FFI evaluation when unavoidable
- Profiling CPU, allocations, syscalls, and lock contention

Whitewater expectations:

- Avoid unbounded queues and detached tasks.
- Every background task has shutdown and failure semantics.
- Panics do not form part of normal error handling.
- Unsafe Rust requires a documented invariant and focused tests.
- Hot-path allocation is measured rather than guessed.

## 2. Distributed-systems correctness

Required knowledge:

- Raft or equivalent consensus
- Quorums and failure models
- Leader election and fencing epochs
- Membership changes
- Linearizability and weaker consistency models
- Split-brain prevention
- Idempotency and deduplication windows
- Logical clocks and monotonic sequencing
- Retry ambiguity
- Fault domains and placement constraints

Key Whitewater problems:

- Feed metadata consensus
- Active-range ownership
- Three-replica quorum append
- Online range movement
- Range split and merge
- Cursor stability across topology changes
- Safe Node draining
- Multi-region replication and promotion
- Atomic consume-and-append boundaries

## 3. Storage-engine engineering

Required knowledge:

- Write-ahead logs
- Segmented append logs
- Checksums and torn-write recovery
- fsync and durability semantics
- Page cache and direct IO trade-offs
- LSM trees and B+ trees
- Bloom filters and block indexes
- Compaction scheduling
- Snapshots and MVCC
- Backup, checkpoint, and restore
- Object-storage tiering
- Corruption detection and repair

Index-engine candidates:

- Fjall
- redb
- RocksDB through Rust bindings as a benchmark/reference

Evaluation must cover:

- Crash consistency
- Recovery time
- Sustained mixed workloads
- Write amplification
- Compaction stalls
- Snapshot transfer
- Replication integration
- Long-duration stability
- Memory and file-descriptor behavior

## 4. Database and indexing design

Required knowledge:

- Primary and secondary indexes
- Point, prefix, and range queries
- Query planning
- Index freshness and consistency
- Materialized views
- Transaction isolation
- Online index creation
- Backfill and catch-up
- Schema evolution
- Cost-based decisions

Whitewater Index requirements:

- Persisted and replicated
- Named Riverbed resources
- Applied-Cursor freshness
- Automatic query routing
- Transferable checkpoints
- Rebuildable from retained Feed history
- Explicit storage and write-cost accounting
- Strict reads must not silently use stale replicas

## 5. Streaming semantics

Required knowledge:

- At-least-once delivery
- Idempotent append
- Exactly-once effects
- Event-time and processing-time semantics
- Windows and watermarks
- Retry and delayed-delivery design
- Poison-event isolation
- Stateful processing
- Backpressure and credit-based flow control

Whitewater concepts:

- Subscription leases replace global group rebalances.
- Capacity is advertised directly by Readers.
- Pipes represent streaming computation.
- Atomic consume-and-append advances input and output together.
- One slow Key must not stall unrelated Keys.

## 6. Protocol and networking engineering

Required knowledge:

- Binary protocol framing
- Version and capability negotiation
- Multiplexing
- Connection migration
- TLS
- Load balancing and discovery
- Congestion and backpressure
- Partial failure and timeout design
- Cross-language compatibility

Protocol requirements:

- Small mandatory core
- Opaque Cursor handling
- Idempotent request identity
- Explicit durability result
- Server pressure feedback
- Bounded payload and frame handling
- Forward-compatible extensions
- Fuzzed decoders

## 7. Security engineering

Required knowledge:

- TLS and certificate lifecycle
- API-key generation, hashing, and rotation
- Capability-based authorization
- Envelope encryption
- KMS and HSM integration
- Audit design
- Secret redaction
- Threat modeling
- Tenant isolation
- Supply-chain security

Whitewater rules:

- No plaintext production listener.
- API keys authorize access but are not encryption keys.
- Authentication material is stored only as a verifier.
- Feed encryption references a KeyId.
- Administrative changes emit audit events.
- Support bundles redact secrets and payloads automatically.

## 8. Orchestration and platform engineering

Required knowledge:

- Kubernetes operators and controllers
- Docker and Compose
- Nomad, ECS, and webhook integration
- Declarative desired state
- Reconciliation and idempotency
- Graceful shutdown and draining
- Placement constraints
- Rolling deployment
- Autoscaling and disruption budgets

Whitewater requirements:

- Infrastructure credentials remain outside data Nodes.
- Autoscaling uses sustained thresholds and hysteresis.
- Capacity changes occur one Node at a time.
- New Nodes are not counted until joined and ready.
- Removed Nodes must be drained and safe-to-remove.
- Orchestrator adapters implement one stable control contract.

## 9. SRE and observability

Required knowledge:

- Metrics, logs, traces, and events
- SLOs and error budgets
- High-cardinality controls
- Capacity forecasting
- Incident response
- Runbook design
- Chaos testing
- Upgrade safety
- Disaster recovery

Whitewater observability should answer:

- Why is this Subscription delayed?
- Which stage is limiting throughput?
- Why is this range moving?
- Is durability currently reduced?
- What automatic action is running?
- Is an Index current?
- Is this Node safe to remove?
- What will this policy change cost?

## 10. Performance engineering

Required knowledge:

- Workload modeling
- Throughput and latency distributions
- Coordinated omission
- Tail-latency analysis
- Flame graphs
- Disk and network profiling
- Cache behavior
- NUMA awareness
- Benchmark reproducibility

Benchmark dimensions:

- Append throughput
- Same-Key latency
- High-cardinality key distribution
- Quorum cost
- Reader fan-out
- Index write/read load
- Recovery and catch-up
- Node loss
- Range movement
- Object-store history reads
- Memory per Feed, range, Subscription, and Index

## 11. Fault testing and verification

Required knowledge:

- Property-based testing
- Fuzzing
- Model-based testing
- Deterministic simulation
- Jepsen-style history analysis
- Network and disk fault injection
- Crash-loop testing
- Corruption testing

Required scenarios:

- Process crash during append
- Torn frame tail
- Duplicate and reordered requests
- Network partition
- Stale owner write
- Replica divergence
- Controller loss
- Disk-full behavior
- Corrupt segment repair
- Scale-in refusal
- Upgrade pause and rollback
- DR promotion and failback

## 12. Client SDK engineering

Required languages over time:

- Rust
- Java
- .NET
- Go
- Python
- JavaScript/TypeScript
- C-compatible core where justified

SDK expectations:

- Simple safe API
- Adaptive batching
- Protocol-level idempotency
- Automatic discovery
- Capacity-aware reading
- Cursor persistence helpers
- Clear retry semantics
- Consistent behavior across languages

## 13. Developer experience and documentation

Required knowledge:

- API design
- Technical writing
- Example-driven teaching
- Error-message design
- Migration planning
- CLI ergonomics
- Compatibility documentation

Developer-experience goals:

- Feed creation asks for no partition count.
- Errors explain cause, impact, and safe next action.
- Three-Node local development starts with one command.
- Examples use production protocol and security shapes.
- Kafka terminology is translated without preserving its coupling.
- Limitations and unimplemented guarantees remain visible.

## 14. Product and community skills

Required capabilities:

- Gathering operator stories without dismissing them as user error
- Turning pain points into measurable acceptance criteria
- Comparing systems fairly
- Publishing reproducible benchmarks
- Maintaining a constructive technical community
- Separating roadmap from implemented fact
- Legal, licensing, trademark, and standards awareness

Community principle:

> Criticism is useful when it improves a contract, test, design, or explanation.

## Current prototype capability

Implemented today:

- Checksummed binary records
- Durable local append log
- Opaque Cursors and independent Reader positions
- Nanosecond event and ingest timestamps with legacy record decoding
- Writer sequence deduplication
- Restart persistence tests
- DNS-based membership
- Heartbeats and graceful leave
- Demand telemetry
- WCL v0 typed parser, authenticated HTTP controller, and `wwctl`
- Persistent three-voter OpenRaft Control Plane with leader forwarding and restart recovery
- Hysteretic scaling recommendations
- Three-Node Docker environments

Not yet implemented:

- Consensus
- Record replication
- Ownership epochs
- Range placement and movement
- Subscription leases
- Atomic consume-and-append
- Persisted replicated Indexes
- TLS and API-key enforcement
- Object-storage tiering
- Schema service
- Production SDKs

## Suggested contribution tracks

### Correctness track

- Formalize append, Cursor, ownership, and durability contracts.
- Build deterministic failure simulation.
- Prototype Raft metadata state.

### Storage track

- Build segmented-log rotation.
- Benchmark Fjall and redb.
- Design Index checkpoints and replica transfer.

### Protocol track

- Specify framing, negotiation, credits, and errors.
- Add fuzzing and cross-version tests.

### Operations track

- Define orchestrator-neutral reconciliation.
- Add drain state and placement explanations.
- Build chaos and upgrade exercises.

### Security track

- Define API-key identity and capability policy.
- Add TLS bootstrap and rotation.
- Threat-model Feed encryption and KMS integration.

### Developer track

- Migrate the prototype stream API to FeedId and dotted FeedName.
- Design the Rust Writer and Subscription API.
- Build `wwctl` around stable control APIs.

## Definition of done for a subsystem

A subsystem is not complete because its happy path works. It requires:

- Documented correctness contract
- Unit and integration tests
- Failure and restart tests
- Metrics and health explanation
- Bounded resource behavior
- Secure defaults
- Upgrade and compatibility story
- Operational runbook
- Benchmark or capacity characterization
- Clear statement of remaining limitations
