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

**Milestone 2 — Operational Writers**

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

- [Kafka pain points and delivery traceability](kafka-pain-points.md)
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

**Status: Complete**

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

- [x] Write failing torn-tail and uncommitted-tail truncation tests.
- [x] Define the `ActiveRangeStore` trait.
- [x] Store ownership epoch and range generation.
- [x] Store last appended position.
- [x] Store last flushed position.
- [x] Store last committed position.
- [x] Store Writer sequence/deduplication state.
- [x] Rotate checksummed immutable segments.
- [x] Recover the active segment after torn-tail writes.
- [x] Truncate uncommitted records safely.
- [x] Persist range state atomically.

On-disk layout:

```text
FeedId/
  RangeId/
    generation-1/
      segment-000000.log
      segment-000001.log
      range-state.json
```

Evidence:

- `src/active_range/store.rs`
- `tests/active_range_store.rs`
- Nine integration tests cover restart recovery, persisted deduplication, Writer sequence conflicts/gaps/stale retries, torn active tails, complete-entry corruption, uncommitted-tail truncation, committed segment rotation, generation/epoch fencing, durable epoch change, and commit-position boundaries.
- Full Rust verification passes with formatting, Clippy warnings denied, 40 library tests, 2 CLI tests, 9 Active Range store integration tests, and the existing storage restart test.

## M1.4 — Fixed placement in the Control Plane

- [x] Create one Active Range when the test Feed is created.
- [x] Select one Append Owner and three distinct storage-capable replicas.
- [x] Persist assignment and ownership epoch through Raft.
- [x] Expose an authenticated placement inspection API.
- [x] Increment epoch whenever ownership changes.
- [x] Refuse assignment when three eligible storage Nodes are unavailable.

Evidence:

- `src/control.rs`
- `src/control_plane.rs`
- `tests/active_range_placement.rs`
- Five integration tests prove identical deterministic RF3 placement across voters, leader-embedded placement despite different follower candidate views, insufficient-node refusal, durable monotonic ownership transfer, catalog restart recovery, and snapshot installation.
- Authenticated API tests prove placement inspection rejects unauthenticated requests.
- Placement is prepared once by the leader and embedded in the replicated command; followers never derive it from local gossip or Node-local state.
- Full verification passes with formatting, Clippy warnings denied, 42 library tests, 2 CLI tests, 5 placement integration tests, 9 Active Range store integration tests, and the storage restart test.
- The rebuilt three-Node Docker Fabric returned one identical assignment from all voters and retained it across complete Fabric restart.

## M1.5 — Internal replica append protocol

- [x] Define the replica append request and response.
- [x] Send the already encoded record frame; do not reconstruct records on replicas.
- [x] Validate FeedId, RangeId, generation, epoch, and expected position.
- [x] Validate frame checksum before durable acknowledgement.
- [x] Reject position gaps.
- [x] Reject different bytes at an existing position.
- [x] Reject stale ownership epochs.
- [x] Authenticate internal replication traffic before decoding the bounded request body.
- [ ] Replace the shared internal credential and HTTP transport in Milestone 9 with authenticated inter-Node encryption and Node identity; native mTLS is the default, while a verified service-mesh or equivalent orchestrator transport is an optional mechanism.

Evidence:

- `src/active_range/replication.rs`
- `src/active_range/store.rs`
- `src/api.rs`
- `tests/replica_append_protocol.rs`
- Six integration tests cover exact-frame durability on all three replicas, identical retry, position gaps, conflicting bytes, invalid frame checksums, wrong range/generation/epoch/owner, non-replica receivers, unknown assignments, invalid base64, and bounded frame size.
- API tests prove authentication runs before JSON body decoding.
- Every durable response returns the BLAKE3 digest of the exact encoded frame persisted by `ActiveRangeStore`.
- Full verification passes with formatting, Clippy warnings denied, 43 library tests, 2 CLI tests, 5 placement integration tests, 9 Active Range store integration tests, 6 replica protocol integration tests, and the storage restart test.
- The rebuilt three-Node Docker Fabric rejected missing credentials before JSON decoding and durably persisted one live encoded frame on all three assigned replicas with an identical digest.

## M1.6 — Majority commit

- [x] Append and flush locally.
- [x] Replicate concurrently to the two followers.
- [x] Wait for one durable follower acknowledgement.
- [x] Advance commit position after two durable copies exist.
- [x] Propagate commit position to followers.
- [x] Return success only after majority commit.
- [x] Return a retryable ambiguous result when frame majority exists without commit majority; a lost success response is recovered by identical retry.
- [x] Preserve request/Writer sequence idempotency.

Evidence:

- `src/active_range/majority.rs`
- `src/active_range/replication.rs`
- `tests/majority_commit.rs`
- Six integration tests prove three-replica commit, either possible owner-plus-follower majority, one-replica refusal, frame-majority/commit-minority ambiguity, conflicting digest rejection, and original-result retry after commit.
- CommitPosition evidence is accepted only when the exact frame digest is already durable at that position on the current replica.
- The production coordinator is wired to a bounded authenticated HTTP transport; M1.7 will expose the client append route.
- Full verification passes with formatting, Clippy warnings denied, 43 library tests, 2 CLI tests, 5 placement tests, 9 Active Range store tests, 6 replica protocol tests, 6 majority-commit tests, and the storage restart test.
- The rebuilt three-Node Docker Fabric durably accepted matching CommitPosition evidence for the same exact frame on all three replicas.

## M1.7 — Route append through any Node

- [x] Add a minimal authenticated append API.
- [x] Resolve FeedName to FeedId.
- [x] Resolve FeedId to current Active Range assignment.
- [x] Forward non-owner requests to the Append Owner.
- [x] Preserve one request ID across forwarding and retries.
- [x] Hide owner and replica topology from the client.
- [x] Convert the live three-Node owner/non-owner acceptance check into an automated integration test.

Evidence:

- `POST /v1/feeds/append` accepts only Feed, Writer identity/epoch/sequence, key, payload, Metadata, event time, and request ID.
- Non-owner ingress forwards the unchanged request over authenticated internal transport; only the owner encodes and majority-commits the record.
- The rebuilt three-Node Fabric returned the same MessageId, Cursor, deduplication result, and `majority_committed` durability through the owner and both non-owner Nodes.
- Cross-Node retry with the same request ID and Writer sequence returned the original result without a duplicate.
- `python scripts/test-m17-topology-free-append.py` automates three-Node health, authentication refusal, unique Feed creation, owner and both non-owner ingress paths, stable MessageId/Cursor, majority durability, deduplication, and unknown-Feed rejection.

## M1.8 — Committed-only reads

- [x] Restrict reads to `position <= commit_position`.
- [x] Ensure locally appended but uncommitted frames are invisible.
- [x] Return Cursors only for committed records and accept opaque Cursor continuation.
- [x] Ensure historical reads remain stateless and do not alter another Reader's position.

Evidence:

- `ReplicaAppendService::read_committed` resolves only the committed Active Range store and rejects unknown or uncommitted Cursors.
- Integration tests prove a durable but uncommitted frame is invisible, appears after CommitPosition persistence, and continuation after its Cursor returns no duplicate.
- `GET /v1/feeds/records?feed=<name>&after=<cursor>&limit=<n>` decodes only committed exact frames.
- The automated three-Node acceptance script verifies the same committed MessageId and Cursor through all three Nodes and independent stateless continuation.
- Full verification passes with formatting, Clippy warnings denied, 43 library tests, 2 CLI tests, 5 placement tests, 9 Active Range store tests, 7 replica protocol tests, 6 majority-commit tests, and the storage restart test.

## M1.9 — Owner failure and fencing

- [x] Connect sustained owner-unavailability detection to automatic recovery initiation.
- [x] Select an eligible caught-up replica from current RF3 health/progress reports.
- [x] Commit a higher ownership epoch through a compare-and-set Control Plane command.
- [x] Fence the old owner before new writes are accepted.
- [x] Compare replica append and commit positions and derive the highest majority-supported committed prefix.
- [x] Collect authenticated live replica progress reports over internal transport.
- [x] Discard tails that never reached majority commit on the selected owner and replicas.
- [x] Preserve the majority-supported committed prefix in the recovery plan.
- [x] Reconcile local store epochs and resume appends at the next valid position automatically.

Evidence:

- `src/active_range/recovery.rs`
- `tests/owner_recovery.rs`
- Recovery planning refuses a healthy owner or fewer than two healthy replicas, selects a caught-up replica, and computes a higher epoch and majority-supported committed prefix.
- The replicated recovery command uses expected owner and epoch as compare-and-set guards, so racing or stale plans cannot overwrite newer ownership.
- Existing epoch validation fences the previous owner immediately after the Control Plane transition.

Completion evidence:

- Authenticated internal progress and reconcile endpoints feed a leader-only recovery supervisor.
- Three consecutive failed owner probes trigger planning; a successful probe resets failure evidence.
- The Control Plane compare-and-set transition commits the higher epoch before replica reconciliation.
- Healthy replicas advance to the majority-supported committed prefix, update epoch, and truncate uncommitted tails.
- Four integration tests cover planning, quorum refusal, stale/racing plans, fencing, epoch reconciliation, committed-prefix preservation, and tail truncation.
- `python scripts/test-m19-owner-recovery.py` stops the live owner, waits for automatic transfer, verifies one epoch increase, appends with majority durability through a survivor, and always restarts the failed container.

## M1.10 — Replica catch-up and repair

- [x] Connect under-replication detection to automatic local repair scheduling.
- [x] Request missing committed frames in bounded resumable batches.
- [x] Verify transferred frame checksums and accepted position/digest.
- [x] Refuse readiness until target CommitPosition matches the verified source.
- [x] Rebuild Writer deduplication state from transferred frames.
- [x] Quarantine a corrupt replica directory before rebuilding from a verified source.
- [x] Expose transferred records, bytes, source/target CommitPosition, quarantine status, limiting resource, and readiness.
- [x] Use frame batches for current gaps; sealed-segment transfer remains a Milestone 8 optimization rather than a correctness dependency.

Evidence:

- `src/active_range/repair.rs`
- `tests/replica_repair.rs`
- Missing committed frames transfer in configurable batches, preserve exact bytes/Cursors, advance commit evidence, and rebuild deduplication state.
- Complete-entry corruption fails normal recovery, triggers directory quarantine, and rebuilds from a checksummed healthy source without deleting forensic evidence.
- Two integration tests prove missing-frame catch-up and corrupt-replica quarantine/rebuild.

Completion evidence:

- Every replica runs a local repair supervisor that compares itself with the current owner and pulls bounded committed-frame batches over an authenticated internal endpoint.
- A restarted replica remains behind until exact position/digest verification and CommitPosition catch-up complete.
- Corrupt generations are quarantined before a clean rebuild; forensic bytes are retained.
- `python scripts/test-m110-replica-catchup.py` stops a non-owner replica, commits three records while it is down, restarts it, and waits until all three records are readable locally.
- The live three-Node catch-up acceptance test passed.

## M1.11 — Fault suite

- [x] Owner crashes before follower replication.
- [x] Owner crashes after one follower durable acknowledgement.
- [x] Owner crashes after commit but before client response.
- [x] One follower is unavailable.
- [x] Both followers are unavailable.
- [x] Old owner returns after epoch change.
- [x] Follower receives a position gap.
- [x] Follower receives conflicting bytes at one position.
- [x] Replica disk fills during append.
- [x] Network partitions owner from one follower.
- [x] Network partitions owner from both followers.
- [x] Same Writer sequence retries across pre-commit, ambiguous-commit, lost-response, and recovered-owner boundaries.
- [x] Complete Fabric restarts and recovers committed data.

Evidence:

- `tests/active_range_store.rs` covers torn writes, corruption, disk-capacity exhaustion, restart, truncation, fencing, and deduplication.
- `tests/majority_commit.rs` deterministically injects one/both follower unavailability, frame-majority/commit-minority ambiguity, digest conflict, and retry after lost success.
- `tests/replica_append_protocol.rs` covers gaps, conflicting positions, invalid checksums, stale epochs, wrong owners, and committed-only visibility.
- `tests/owner_recovery.rs` covers owner loss, CAS epoch transfer, stale-owner fencing, committed-prefix preservation, and tail removal.
- `tests/replica_repair.rs` covers missing-frame catch-up and corruption quarantine/rebuild.
- `python scripts/test-m111-fault-suite.py` runs topology-free append/retry, live owner failure, live replica restart/catch-up, and complete Fabric restart suites.
- The aggregate automated suite passed with no acknowledged record loss, uncommitted visibility, stale-owner success, or duplicate logical record.

## Milestone 1 Definition of Done

- [x] **D1:** Writer can append through any Node. Depends on M1.4 and M1.7.
- [x] **D2:** Request reaches the current Append Owner. Depends on M1.4 and M1.7.
- [x] **D3:** Two of three replicas durably persist before success. Depends on M1.5 and M1.6.
- [x] **D4:** Only committed records are readable. Depends on M1.8.
- [x] **D5:** Loss of one Node does not lose acknowledged data. Depends on M1.6 and M1.9.
- [x] **D6:** Loss of two replica Nodes prevents successful writes. Depends on M1.6.
- [x] **D7:** A stale owner cannot append. Depends on M1.5 and M1.9.
- [x] **D8:** Retry cannot create a duplicate logical record. Depends on M1.1, M1.3, and M1.6.
- [x] **D9:** Restarted replicas catch up automatically. Depends on M1.10.
- [x] **D10:** Fault tests prove D1–D9. Depends on M1.11.

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

**Status: Current focus**

- [x] Implement authenticated WriterSession creation through the Admin API and WCL.
- [x] Bind sessions to immutable WriterId and FeedId.
- [x] Fence stale or revoked Writer sessions with a monotonically increasing epoch.
- [x] Allocate and persist idempotent sequence state through Control Plane consensus.
- [x] Add topology-free Writer-session append API and typed Rust `WriterSessionClient`.
- [x] Add adaptive `auto` batching policy and bounded multi-record Writer API, tuning count, bytes, and linger from message sizes, target latency, server pressure, retries, and bounded memory.
- [x] Add server feedback fields for recommended batch bytes/count, pressure, retry delay, and maximum accepted frame size without exposing physical topology.
- [~] Add Writer CLI frontends: `wwctl write` is complete; the standalone PowerShell `wcl-cli write` wrapper remains.
- [x] Add text/base64 key and payload, file, stdin, binary, Metadata, event-time, request-ID, and JSON response options to `wwctl write`.
- [x] Add Writer session inspection through `DESCRIBE WRITER` and epoch-checked revocation.

Evidence:

- Writer definitions persist `session_epoch`, `next_sequence`, and active/revoked state in Control Plane catalog snapshots.
- Opening a session increments the epoch and resets sequence allocation for the newly fenced incarnation.
- Sequence allocation is replicated and idempotent by request ID; retry returns the original sequence.
- Restart tests prove session state persists, a newer session fences the previous epoch, and revocation prevents further allocation.
- Rust `AdminClient` methods and WCL commands expose open, allocate, inspect, and revoke operations through the same typed controller.

Additional evidence:

- `python scripts/test-m2-writer-session.py` verifies session creation, automatic idempotent sequence allocation, append through all three ingress Nodes, stable MessageId, stale-epoch rejection, revocation, and batching feedback.
- The adaptive policy has unit coverage for message-size, pressure, retry, frame-size, and retry-delay adjustments.
- Full Rust verification passes with 45 library tests plus all existing integration suites.

Definition of Done:

- [x] Applications append without knowing owner, range, replica, ownership epoch, or sequence internals.
- [x] Ambiguous retry returns original MessageId and Cursor.
- [~] Rust SDK and `wwctl write` use the same data API; PowerShell `wcl-cli write` remains.

## Milestone 3 — Reader sessions and live delivery

- [x] Implement persisted named and anonymous stateless temporary Reader sessions.
- [x] Implement persisted Reader lookup by name and immutable ReaderId.
- [x] Separate delivered and acknowledged Cursors.
- [x] Implement explicit cumulative acknowledgement with delivered-Cursor validation.
- [x] Implement bounded capacity credits per fetch for backpressure.
- [x] Implement committed reads from beginning by default.
- [x] Implement bounded wait-after-catch-up through API `wait_ms` and CLI `--wait`.
- [x] Implement `new_only` as an atomic end-of-Feed start for temporary Readers.
- [x] Keep `new_only` absent from persistent Reader APIs; persistent Readers require explicit seek.
- [x] Implement temporary `tail`, `limit`, `after`, JSON records, and CLI payload-only output.

Evidence:

- Reader catalog state persists session epoch, capacity, delivered Cursor, acknowledged Cursor, and active state through Control Plane restart.
- Fetch returns committed records only, advances delivered progress independently, and never acknowledges implicitly.
- Acknowledgement must match the latest delivered Cursor; reopening increments epoch, fences the previous session, and resumes from acknowledged progress.
- Typed Rust `ReaderSessionClient` uses the same open/fetch/ack/close API.
- `python scripts/test-m3-reader-session.py` majority-commits Writer records, delivers two under capacity, acknowledges, reopens through another Node, resumes with the remaining record, rejects the stale session, starts a temporary Reader at end-of-Feed, waits for a new record, and tails the last two records.

Definition of Done:

- [x] Independent Readers never change each other's positions.
- [x] Readers never receive uncommitted records.
- [x] Named Reader restart resumes from acknowledged Cursor.
- [x] Per-session capacity bounds delivery without changing unrelated Reader progress.

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

- [ ] Replace development HTTP/shared-key transport with authenticated inter-Node encryption and Node identity: native mTLS by default, with a verified service-mesh or equivalent orchestrator transport as an optional mechanism.
- [ ] Persist hashed API-key identities.
- [ ] Implement credential issue, rotation, expiry, and revocation.
- [ ] Enforce namespace grants on every Admin and data command.
- [ ] Emit immutable audit events.
- [ ] Implement rolling compatibility gates and rollback boundaries.
- [ ] Implement backup, restore, and DR exercises.
- [ ] Implement redacted support bundles.
- [ ] Run long-duration, disk-full, corruption, and chaos suites.

## Unscheduled pain-point backlog

These accepted product requirements need dependency review and explicit milestone placement. They do not supersede the current focus or the first unchecked immediate action.

- [ ] Define first-class Subscription retry, delayed-delivery, quarantine, skip, and final-disposition workflows.
- [ ] Define a Schema Policy milestone covering identity, compatibility, admission validation, evolution, and generated clients.
- [ ] Expand Pipe work into explicit join, window, watermark, grace-period, and late-arrival contracts.
- [ ] Define a correlated health-explanation API spanning Writer, quorum, storage, Index, Subscription, and Reader stages.
- [ ] Define logical-resource cost attribution for storage, replication, movement, egress, Subscriptions, Pipes, and Indexes.
- [ ] Define richer Feed inspection, time seek, bounded search, and single-event investigation workflows.
- [ ] Define end-to-end business-flow tracing through Metadata and logical resource IDs.
- [ ] Define a versioned language-neutral data protocol, cross-language SDK sequence, and shared conformance suite.
- [ ] Define protocol-wide structured errors covering cause, scope, impact, retry safety, and next safe action.
- [ ] Define lightweight SDK test doubles so application logic does not always require a running Fabric.

Source: [Kafka pain points and delivery traceability](kafka-pain-points.md).

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
4. [x] Create the first failing torn-tail and uncommitted-tail truncation tests.
5. [x] Implement and verify `ActiveRangeStore` from the tested recovery contract.
6. [x] Write the failing M1.4 test that requires Feed creation to commit one identical RF3 Active Range assignment on every Control Plane voter and recover it after complete Fabric restart.
7. [x] Persist fixed Active Range placement and ownership epoch through the Control Plane.
8. [x] Write the failing M1.5 replica protocol tests for identical bytes, position gaps, conflicts, invalid checksums, wrong generation, and stale ownership epoch.
9. [x] Define the authenticated internal replica append request and response around the committed assignment.
10. [x] Write the failing M1.6 tests proving three healthy replicas and any two healthy replicas commit, while one healthy replica never returns success.
11. [x] Implement owner-side concurrent replication and two-of-three durable commit evidence.
12. [x] Verify append through the owner and both non-owners reaches the same owner and preserves one request identity.
13. [x] Add the minimal authenticated append API and forwarding path without exposing Active Range topology.
14. [x] Convert the M1.7 live owner/non-owner append verification into an automated three-Node integration test.
15. [x] Write the failing M1.8 tests proving uncommitted frames remain invisible and become readable only after majority commit.
16. [x] Route Feed reads through committed Active Range storage and return only committed opaque Cursors.
17. [x] Write M1.9 owner-failure tests for stale-owner fencing, committed-prefix preservation, and uncommitted-tail removal.
18. [x] Implement automatic owner failure detection, consensus-backed epoch transfer, replica reconciliation, and live Docker acceptance.
19. [x] Write M1.10 tests for missing-frame transfer, checksum verification, readiness gating, deduplication rebuild, and corrupt-replica quarantine.
20. [x] Implement bounded exact-frame catch-up and repair before marking a replacement ready.
21. [x] Connect restarted/under-replicated Node detection to automatic repair scheduling and add a live three-Node restart/catch-up test.
22. [x] Expose repair progress and limiting-resource diagnostics; defer sealed-segment transfer to tiered-history optimization.
23. [ ] Automate the complete M1.11 crash, network, disk, retry, and restart fault matrix.
24. [ ] Prove Milestone 1 D1-D10 without acknowledged loss, uncommitted visibility, stale-owner writes, or duplicate logical records.

The first unchecked item in this section is the next task unless a blocking architecture decision is recorded above.
