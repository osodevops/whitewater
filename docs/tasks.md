# Whitewater Tasks and Milestones

> This is the living execution checklist for Whitewater. Update it as work starts and finishes so implementation order, evidence, and architecture decisions remain understandable.

## Status legend

- `[x]` Complete and verified
- `[~]` In progress or partially implemented
- `[ ]` Not started
- **Decision required** means implementation must not begin until the stated choice is recorded

## How to update this document

1. Keep exactly one milestone marked **Current focus**.
2. Change a task to `[~]` when implementation begins.
3. Change it to `[x]` only when its tests and evidence are linked.
4. Do not mark a Definition of Done item complete because code exists; the required fault or integration test must pass.
5. Add newly discovered work beneath the milestone that depends on it.
6. Record architecture decisions in the relevant design document and link them here.
7. Keep speculative ideas under **Open decisions**, not in the committed task sequence.

## Current focus

**Milestone 1 — Quorum-durable append for one Active Range**

The target is deliberately narrow:

```text
One Feed
One Active Range
One current Append Owner
Three replicas
Two-of-three durable majority commit
```

Dynamic range splitting, elastic placement, Writer UX, Reader sessions, Indexes, and Pipes are later milestones.

---

# Completed foundations

## F0 — Product and architecture

- [x] Name the product Whitewater under FinnStream.
- [x] Define the public model: `fabric -> space -> feed -> key -> cursor -> subscription`.
- [x] Replace Topic with Feed, Headers with Metadata, and Offset with opaque Cursor terminology.
- [x] Define immutable FeedId and mutable dotted FeedName.
- [x] Define lowercase dotted names and 512-byte maximum.
- [x] Treat Whitewater as both a distributed database and streaming platform.
- [x] Document Kafka pain points and Whitewater equivalents.
- [x] Document humane operational requirements.
- [x] Document lessons retained from Kafka source.

Evidence:

- [Why Whitewater](why-whitewater.md)
- [Architecture](kafka-successor-architecture.md)
- [Operational experience](operational-experience.md)
- [Kafka source lessons](kafka-source-lessons.md)

## F1 — Local record correctness

- [x] Implement checksummed binary record frames.
- [x] Persist key, payload, and Metadata bytes.
- [x] Store signed nanosecond `event_time_ns` and `ingest_time_ns`.
- [x] Decode legacy millisecond FSR1 records into FSR2 nanoseconds.
- [x] Implement opaque Feed-scoped Cursors.
- [x] Implement Writer identity and sequence deduplication.
- [x] Return the original MessageId and Cursor on duplicate retry.
- [x] Recover records and deduplication state after restart.
- [x] Prove independent Readers can retain different Cursor positions.

Evidence:

- `src/codec.rs`
- `src/cursor.rs`
- `src/storage.rs`
- `tests/storage_restart.rs`

## F2 — Membership and Docker development

- [x] Implement DNS-seeded Node discovery.
- [x] Implement heartbeats, expiry, and graceful leave.
- [x] Provide a fixed three-Node development Fabric.
- [x] Provide arbitrary Docker membership scaling.
- [x] Add demand and safe-to-remove telemetry.
- [x] Add slow hysteresis-based scale recommendations.
- [x] Verify membership convergence from three to five to two and back.

Evidence:

- `src/membership.rs`
- `src/demand.rs`
- `src/autoscale.rs`
- `compose.yml`
- `compose.cluster.yml`

## F3 — Administration

- [x] Implement WCL v0 parser and typed command model.
- [x] Implement Spaces, Feeds, Writers, Readers, Roles, and namespace grants.
- [x] Implement immutable IDs and rename without physical storage rename.
- [x] Implement `SHOW`, `DESCRIBE`, `EXPLAIN ACCESS`, `SEEK`, and safe logical `DROP`.
- [x] Expose authenticated WCL and typed JSON Admin API routes.
- [x] Implement Rust AdminClient resource methods.
- [x] Implement `wwctl` interactive shell and local `wcl-cli` frontend.
- [x] Add API-key prompting and Windows user installation.

Evidence:

- [Admin API](admin-api.md)
- [WCL](wcl.md)
- `src/admin.rs`
- `src/control.rs`
- `scripts/wcl-cli.ps1`

## F4 — Replicated Control Plane

- [x] Name the public subsystem Control Plane; keep Raft/quorum terminology internal.
- [x] Add persistent OpenRaft metadata consensus.
- [x] Configure three static voters in the standard development Fabric.
- [x] Implement leader election and authenticated internal Raft RPC.
- [x] Forward Admin commands received by followers to the elected leader.
- [x] Replicate deterministic commands and resource IDs.
- [x] Persist votes, logs, commits, membership, snapshots, catalog, and request results.
- [x] Implement linearizable `SHOW`, `DESCRIBE`, and `EXPLAIN ACCESS` without log mutation.
- [x] Implement request IDs and idempotent retry across restart.
- [x] Verify leader failure, re-election, follower catch-up, and restart recovery.
- [x] Verify no success response without majority.
- [x] Verify an uncommitted command disappears after isolated leader loss and new-majority election.

Evidence:

- [Control Plane](control-plane.md)
- `src/control_plane.rs`
- `src/control.rs`
- `compose.yml`

---

# Milestone 1 — Quorum-durable append for one Active Range

**Status: Current focus**

## Goal

An acknowledged append survives loss of one Node, is never exposed before majority commit, is idempotent under ambiguous retry, and cannot be accepted by a stale owner.

## M1.1 — Correctness contract

- [x] Define `accepted`, `appended`, `flushed`, `committed`, `visible`, and `acknowledged` precisely.
- [x] Define the two-of-three durable acknowledgement rule.
- [x] Define same-Feed/same-Key ordering under retry and owner change.
- [x] Define how ambiguous append timeouts are retried.
- [x] Define when uncommitted tails are truncated.
- [x] Define stale-owner and stale-replica rejection.
- [x] Define visibility rules for Readers.
- [x] Record the contract in a dedicated Active Range design document.

Evidence:

- [Active Range replication contract](active-range-replication.md)
- Contract review checklist completed in the design document.
- State-transition and crash-outcome tables cover normal append, timeout, owner loss, visibility, and recovery.

## M1.2 — Active Range domain model

- [x] Add `RangeId`.
- [x] Add `RangeGeneration`.
- [x] Add `OwnershipEpoch`.
- [x] Add `RangePosition`.
- [x] Add `CommitPosition`.
- [x] Add `ReplicaSet` with exactly three replicas for this milestone.
- [x] Add `ActiveRangeAssignment` domain type; replicated Control Plane persistence remains M1.4.
- [x] Ensure these physical concepts remain absent from public Feed APIs.

Evidence:

- `src/active_range/model.rs`
- `src/active_range/mod.rs`
- Unit tests cover newtype ordering and serialization, unique RF3 assignments, owner membership, assignment round-trip, generation rejection, epoch fencing, quorum commit, committed-only visibility, retry identity, conflicting frames, and monotonic position boundaries.
- Full Rust verification: 40 library tests, 2 CLI tests, and restart integration test passed; Clippy passed with warnings denied.

## M1.3 — ActiveRangeStore

- [ ] Define the `ActiveRangeStore` trait.
- [ ] Store ownership epoch and range generation.
- [ ] Store last appended position.
- [ ] Store last flushed position.
- [ ] Store last committed position.
- [ ] Store Writer sequence/deduplication state.
- [ ] Rotate checksummed immutable segments.
- [ ] Recover the active segment after torn-tail writes.
- [ ] Truncate uncommitted records safely.
- [ ] Persist range state atomically.

Planned layout:

```text
FeedId/
  RangeId/
    generation-1/
      segment-000000.log
      segment-000001.log
      range-state.json
```

Evidence required:

- Restart test.
- Torn-write test.
- Corruption-detection test.
- Uncommitted-tail truncation test.

## M1.4 — Fixed placement in the Control Plane

- [ ] Create one Active Range when the test Feed is created.
- [ ] Select one Append Owner and three distinct storage-capable replicas.
- [ ] Persist assignment and ownership epoch through Raft.
- [ ] Expose an authenticated placement inspection API.
- [ ] Increment epoch whenever ownership changes.
- [ ] Refuse assignment when three eligible storage Nodes are unavailable.

Evidence required:

- All Control Plane voters return the same assignment and epoch.
- Assignment survives complete Fabric restart.

## M1.5 — Internal replica append protocol

- [ ] Define the replica append request and response.
- [ ] Send the already encoded record frame; do not reconstruct records on replicas.
- [ ] Validate FeedId, RangeId, generation, epoch, and expected position.
- [ ] Validate frame checksum before durable acknowledgement.
- [ ] Reject position gaps.
- [ ] Reject different bytes at an existing position.
- [ ] Reject stale ownership epochs.
- [ ] Authenticate and eventually encrypt internal replication traffic.

Evidence required:

- Identical checksummed bytes on all replicas.
- Gap, conflict, checksum, and stale-epoch tests.

## M1.6 — Majority commit

- [ ] Append and flush locally.
- [ ] Replicate concurrently to the two followers.
- [ ] Wait for one durable follower acknowledgement.
- [ ] Advance commit position after two durable copies exist.
- [ ] Propagate commit position to followers.
- [ ] Return success only after majority commit.
- [ ] Return a clearly ambiguous result if client delivery fails after commit.
- [ ] Preserve request/Writer sequence idempotency.

Evidence required:

- Three healthy replicas: append succeeds.
- Any two healthy replicas: append succeeds.
- One healthy replica: no success response.

## M1.7 — Route append through any Node

- [ ] Add a minimal authenticated append API.
- [ ] Resolve FeedName to FeedId.
- [ ] Resolve FeedId to current Active Range assignment.
- [ ] Forward non-owner requests to the Append Owner.
- [ ] Preserve one request ID across forwarding and retries.
- [ ] Hide owner and replica topology from the client.

Evidence required:

- Append through owner succeeds.
- Append through each non-owner succeeds with the same semantics.

## M1.8 — Committed-only reads

- [ ] Restrict reads to `position <= commit_position`.
- [ ] Ensure locally appended but uncommitted frames are invisible.
- [ ] Make Cursor creation depend on committed position.
- [ ] Ensure historical reads do not alter another Reader's position.

Evidence required:

- Reader cannot observe a frame before majority commit.
- Reader sees the frame after commit.

## M1.9 — Owner failure and fencing

- [ ] Detect unavailable owner.
- [ ] Select an eligible caught-up replica.
- [ ] Commit a higher ownership epoch through the Control Plane.
- [ ] Fence the old owner before new writes are accepted.
- [ ] Compare replica append and commit positions.
- [ ] Discard tails that never reached majority commit.
- [ ] Preserve records known committed by a majority.
- [ ] Resume appends at the next valid position.

Evidence required:

- Old owner cannot append after returning.
- New owner preserves committed records.
- New owner hides/discards uncommitted records.

## M1.10 — Replica catch-up and repair

- [ ] Detect an under-replicated Active Range.
- [ ] Request missing committed frames or sealed segments.
- [ ] Verify transferred checksums.
- [ ] Catch up before marking a replica healthy.
- [ ] Rebuild Writer deduplication state where required.
- [ ] Expose catch-up progress and limiting resource.

Evidence required:

- Restarted replica catches up automatically.
- Corrupt replica is repaired from a verified copy.

## M1.11 — Fault suite

- [ ] Owner crashes before follower replication.
- [ ] Owner crashes after one follower durable acknowledgement.
- [ ] Owner crashes after commit but before client response.
- [ ] One follower is unavailable.
- [ ] Both followers are unavailable.
- [ ] Old owner returns after epoch change.
- [ ] Follower receives a position gap.
- [ ] Follower receives conflicting bytes at one position.
- [ ] Replica disk fills during append.
- [ ] Network partitions owner from one follower.
- [ ] Network partitions owner from both followers.
- [ ] Same Writer sequence retries during each failure point.
- [ ] Complete Fabric restarts and recovers committed data.

Evidence required:

- Automated test report linked here.
- No acknowledged record loss.
- No uncommitted record visibility.
- No duplicate logical record from retries.

## Milestone 1 Definition of Done

- [ ] **D1:** Writer can append through any Node. Depends on M1.4 and M1.7.
- [ ] **D2:** Request reaches the current Append Owner. Depends on M1.4 and M1.7.
- [ ] **D3:** Two of three replicas durably persist before success. Depends on M1.5 and M1.6.
- [ ] **D4:** Only committed records are readable. Depends on M1.8.
- [ ] **D5:** Loss of one Node does not lose acknowledged data. Depends on M1.6 and M1.9.
- [ ] **D6:** Loss of two replica Nodes prevents successful writes. Depends on M1.6.
- [ ] **D7:** A stale owner cannot append. Depends on M1.5 and M1.9.
- [ ] **D8:** Retry cannot create a duplicate logical record. Depends on M1.1, M1.3, and M1.6.
- [ ] **D9:** Restarted replicas catch up automatically. Depends on M1.10.
- [ ] **D10:** Fault tests prove D1–D9. Depends on M1.11.

Do not begin Milestone 2 until D1–D10 are checked.

---

# Node capacity and 3-of-N replication

## What replication factor 3 means

Replication factor 3 applies to each Active Range, not to the whole Fabric:

```text
Active Range A -> Nodes 1, 2, 3
Active Range B -> Nodes 2, 3, 4
Active Range C -> Nodes 1, 3, 4
Active Range D -> Nodes 1, 2, 4
```

With four storage-capable Nodes, every Active Range still has three replicas. The fourth Node lets placement spread storage, append ownership, read IO, and failure-recovery work across more machines.

Adding a Node does not automatically change RF3 into RF4. Increasing per-range failure tolerance is a separate Durability Policy decision.

## Failure tolerance

For one RF3 range:

```text
3 replicas healthy -> full redundancy
2 replicas healthy -> majority writes continue
1 replica healthy  -> data copy exists, but majority writes stop
0 replicas healthy -> data unavailable
```

A five-Node Fabric with RF3 has more placement and capacity options, but each range still tolerates only the failures allowed by its own three-replica set. Five cluster Nodes do not mean every range can tolerate two arbitrary Node failures while remaining writable. That stronger guarantee requires five replicas and a three-of-five majority.

## Recommended Node capability model

One Whitewater binary may expose a small set of capabilities rather than separate products:

```text
Control capability
  participates as a stable Control Plane voter

Storage capability
  persists Active Range replicas on durable local storage

Compute capability
  runs Pipes, transformations, Reader work, and query execution

Gateway capability
  accepts client connections, authenticates, batches, compresses, and routes

Cache capability
  holds replaceable hot or historical data without becoming authoritative
```

A Node may combine capabilities.

### Small development Fabric

```text
3 Nodes
  each: Control + Storage + Compute + Gateway
```

This is simple and exercises every correctness path.

### Example stable production baseline

```text
5 baseline Nodes
  3 or 5 stable Control voters
  5 storage-capable Nodes
  compute/gateway capability as capacity permits
```

A configured minimum of five means autoscaling never contracts below five. It does not mean all five store every Active Range.

### Example burst scale-out

```text
normal:  5 baseline Nodes at 100k records/second
burst:   8 Nodes when traffic approaches 200k records/second
```

The three added Nodes may be assigned capabilities according to the bottleneck:

- Append/storage pressure: add storage-capable Nodes with durable volumes and move/rebalance replicas gradually.
- Reader/Pipe/query CPU pressure: add stateless compute Nodes.
- Connection/compression/routing pressure: add stateless gateway Nodes.
- Historical retrieval pressure: add replaceable cache/compute capacity.

Stateless compute Nodes do not improve quorum append throughput for an overloaded Active Range unless work can be moved away from its owner or they become storage-capable replicas. The pressure explanation must say which capability is constrained.

## Stable baseline versus permanent core

**Recommended direction:** define a minimum baseline pool, not special immortal data Nodes.

```text
minimum_control_voters = 3 or 5, changed deliberately through joint consensus
minimum_storage_nodes  = deployment policy, for example 5
minimum_compute_nodes  = deployment policy
minimum_gateway_nodes  = deployment policy
```

Any individual storage Node should still be replaceable after its ranges drain. Calling the first five Nodes a permanent core would make hardware replacement and rolling maintenance harder.

Control voters are more stable because membership changes require consensus, but they must also be replaceable through learner catch-up and joint membership change.

## Scale-in differences

```text
Stateless compute/gateway Node
  -> stop accepting work
  -> drain in-flight requests
  -> remove

Storage-capable Node
  -> move/re-replicate every Active Range
  -> verify three healthy replicas elsewhere
  -> mark safe-to-remove
  -> remove

Control voter
  -> add replacement learner
  -> catch up metadata log
  -> joint-consensus membership change
  -> remove old voter
```

Autoscaling must never apply one generic removal procedure to all three cases.

## Open Node-model decisions

- [ ] **Decision required:** Are capabilities declared by orchestrator labels, discovered resources, or both?
- [ ] **Decision required:** Can a Node change capabilities without restart?
- [ ] **Decision required:** Should burst Nodes default to compute/gateway only?
- [ ] **Decision required:** What metrics distinguish storage pressure from compute pressure?
- [ ] **Decision required:** Is the recommended production Control Plane three or five voters?
- [ ] **Decision required:** Can control voters run without Feed storage in large deployments?
- [ ] **Decision required:** How are persistent volumes reattached after container replacement?
- [ ] **Decision required:** When does object storage allow a storage Node to be mostly stateless?
- [ ] **Decision required:** Which operations may use stale cache replicas?
- [ ] **Decision required:** How is cost attributed across storage, compute, gateway, cache, and replication work?

## Node-model tasks

- [ ] Add capability descriptors to Node membership.
- [ ] Add durable-storage identity separate from ephemeral container identity.
- [ ] Add CPU, memory, disk, network, and failure-domain capacity signals.
- [ ] Add placement eligibility and exclusion reasons.
- [ ] Add capability-specific pressure metrics.
- [ ] Add minimum baseline policy.
- [ ] Add storage drain state machine.
- [ ] Add compute/gateway graceful drain.
- [ ] Add Control voter learner and replacement flow.
- [ ] Add SLO-aware movement budgets.
- [ ] Build 100k-to-200k records/second scaling benchmark.
- [ ] Prove scale-out helps the constrained resource rather than merely increasing Node count.

---

# Later milestones

## Milestone 2 — Operational Writers

- [ ] Implement authenticated WriterSession creation.
- [ ] Bind sessions to immutable WriterId and FeedId.
- [ ] Fence stale Writer sessions with epoch.
- [ ] Allocate and persist sequence state.
- [ ] Add typed append SDK API.
- [ ] Add `wcl-cli write` as an API-only frontend.
- [ ] Add payload, file, stdin, binary, Metadata, and event-time options.
- [ ] Add Writer session inspection and revocation.

Definition of Done:

- [ ] Applications append without knowing owner, range, replica, epoch, or sequence internals.
- [ ] Ambiguous retry returns original MessageId and Cursor.
- [ ] CLI and SDK use the same data API.

## Milestone 3 — Reader sessions and live delivery

- [ ] Implement temporary independent Reader sessions.
- [ ] Implement persisted Reader lookup by name and ReaderId.
- [ ] Separate delivered and acknowledged Cursors.
- [ ] Implement explicit acknowledgement.
- [ ] Implement capacity credits and backpressure.
- [ ] Implement `read` from beginning by default.
- [ ] Implement `--wait` to continue after catch-up.
- [ ] Implement `--new-only` as an atomic end-of-Feed start for temporary Readers.
- [ ] Reject `--new-only` for persistent Readers; require explicit seek.
- [ ] Implement `tail`, `limit`, `after`, JSON, and payload-only output.

Definition of Done:

- [ ] Independent Readers never change each other's positions.
- [ ] Readers never receive uncommitted records.
- [ ] Named Reader restart resumes from acknowledged Cursor.
- [ ] Slow Reader backpressure does not destabilize unrelated Readers.

## Milestone 4 — Multiple internal ranges

- [ ] Define a large logical range space.
- [ ] Map keys to ranges without exposing topology.
- [ ] Place multiple ranges across N storage Nodes.
- [ ] Split hot ranges online.
- [ ] Merge cold ranges.
- [ ] Move ranges while preserving same-Key ordering.
- [ ] Keep Cursors valid across generation changes.
- [ ] Add hot-key detection and isolation.

Definition of Done:

- [ ] Feed throughput grows without changing public Feed definition.
- [ ] Clients receive no topology-change callback.
- [ ] Same-Key order survives split, move, merge, and owner failure.

## Milestone 5 — Role-aware elasticity

- [ ] Integrate Node capabilities into placement.
- [ ] Keep Control voters out of automatic replica-count scaling.
- [ ] Add storage-capable Nodes gradually when storage/append pressure is high.
- [ ] Add stateless compute/gateway Nodes when processing or connection pressure is high.
- [ ] Drain according to capability before scale-in.
- [ ] Preserve configured baseline pools.
- [ ] Explain every scaling decision and constrained resource.
- [ ] Verify 5-to-8-to-5 scaling under synthetic 100k-to-200k load.

Definition of Done:

- [ ] Scale-out measurably reduces the identified pressure.
- [ ] Scale-in never reduces durability or removes unique data.
- [ ] Stateless burst capacity leaves no durable cleanup obligation.

## Milestone 6 — Persisted replicated Indexes

- [ ] Benchmark Fjall, redb, and RocksDB reference workloads.
- [ ] Select Index Engine through fault and workload evidence.
- [ ] Implement Key Index.
- [ ] Implement Index applied-Cursor freshness.
- [ ] Implement three-replica Index durability.
- [ ] Implement checkpoints and transfer.
- [ ] Implement automatic query routing.
- [ ] Add `CREATE INDEX`, `GET`, and Index inspection APIs.

Definition of Done:

- [ ] Point lookup never scans a Feed.
- [ ] Strict read never silently returns stale state.
- [ ] Index owner loss preserves acknowledged indexed state.

## Milestone 7 — Subscriptions, Pipes, and atomic effects

- [ ] Implement durable Subscription progress.
- [ ] Implement epoch-fenced small-range leases.
- [ ] Implement incremental lease transfer.
- [ ] Implement Pipe definitions.
- [ ] Implement atomic consume-and-append.
- [ ] Include co-located Index mutations in the defined atomic boundary.
- [ ] Implement deterministic retry after ambiguous result.

Definition of Done:

- [ ] Membership change does not globally pause processing.
- [ ] Stale Reader cannot acknowledge after lease transfer.
- [ ] Input progress and Whitewater output effects commit together.

## Milestone 8 — Tiered history

- [ ] Seal immutable checksummed segments.
- [ ] Upload and verify object-store copies.
- [ ] Implement local hot cache and admission control.
- [ ] Implement historical prefetch.
- [ ] Implement History Policy lifecycle.
- [ ] Implement deletion, legal retention, and cryptographic erasure contracts.

## Milestone 9 — Production security and operations

- [ ] Replace development internal keys with TLS/mTLS Node identity.
- [ ] Persist hashed API-key identities.
- [ ] Implement credential issue, rotation, expiry, and revocation.
- [ ] Enforce namespace grants on every Admin and data command.
- [ ] Emit immutable audit events.
- [ ] Implement rolling compatibility gates and rollback boundaries.
- [ ] Implement backup, restore, and DR exercises.
- [ ] Implement redacted support bundles.
- [ ] Run long-duration, disk-full, corruption, and chaos suites.

---

# Cross-cutting test matrix

These tests accumulate across milestones and must never regress:

- [ ] One Node process crash
- [ ] Two Node process crashes
- [ ] Control leader failure
- [ ] Active Range owner failure
- [ ] Network partition
- [ ] Disk full
- [ ] Torn write
- [ ] Corrupt frame
- [ ] Duplicate request
- [ ] Delayed response after commit
- [ ] Rolling restart
- [ ] Rolling version upgrade
- [ ] Credential rotation
- [ ] Range movement during writes
- [ ] Historical replay during live traffic
- [ ] Scale-out during peak traffic
- [ ] Scale-in with under-replicated data
- [ ] Complete Fabric restart

# Immediate next actions

1. [x] Write the M1.1 correctness contract and state-transition table.
2. [x] Review and approve Active Range terminology.
3. [x] Implement M1.2 domain types with unit tests.
4. [ ] Define the `ActiveRangeStore` trait.
5. [ ] Create the first torn-tail and uncommitted-truncation tests before implementation.

The first unchecked item in this section is the next task unless a blocking architecture decision is recorded above.
