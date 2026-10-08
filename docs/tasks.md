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

**Milestone 4 — Multiple internal ranges**

The current delivery track is multiple internal Active Ranges. In parallel, [Milestone 6](#milestone-6--persisted-replicated-indexes) treats replicated StateStores and secondary Indexes as core storage, **not** future performance optimizations. An isolated Fjall prototype persists primary rows and shared secondary entries locally, and a StateStore can be declared through a typed Control Plane command. Neither is available on the live Feed append/query path; RF3 durability, logical cross-Node lookup, and the cross-store processing boundary must be proven before claiming carefree enrichment or `CREATE INDEX` support.

---

# Completed foundations

## F0 — Product and architecture

- [x] Name the product Whitewater under FinnStream.
- [x] Define the public model: `riverbed -> domain -> feed -> key -> cursor -> subscription`.
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
- [x] Provide a fixed three-Node development Riverbed.
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
- [x] Implement Domains, Feeds, Writers, Readers, Roles, and namespace grants.
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
- [x] Configure three static voters in the standard development Riverbed.
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
- The rebuilt three-Node Docker Riverbed returned one identical assignment from all voters and retained it across complete Riverbed restart.

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
- The rebuilt three-Node Docker Riverbed rejected missing credentials before JSON decoding and durably persisted one live encoded frame on all three assigned replicas with an identical digest.

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
- The rebuilt three-Node Docker Riverbed durably accepted matching CommitPosition evidence for the same exact frame on all three replicas.

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
- The rebuilt three-Node Riverbed returned the same MessageId, Cursor, deduplication result, and `majority_committed` durability through the owner and both non-owner Nodes.
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
- The recovery supervisor uses bounded concurrent Feed-owner health probes and recovery attempts so one slow owner or a backlog of Feeds cannot indefinitely delay checking another Feed. A deterministic unit test blocks one probe and proves a second proceeds; isolated live recovery passed with pre-existing test Feeds.

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
- [x] Complete Riverbed restarts and recovers committed data.

Evidence:

- `tests/active_range_store.rs` covers torn writes, corruption, disk-capacity exhaustion, restart, truncation, fencing, and deduplication.
- `tests/majority_commit.rs` deterministically injects one/both follower unavailability, frame-majority/commit-minority ambiguity, digest conflict, and retry after lost success.
- `tests/replica_append_protocol.rs` covers gaps, conflicting positions, invalid checksums, stale epochs, wrong owners, and committed-only visibility.
- `tests/owner_recovery.rs` covers owner loss, CAS epoch transfer, stale-owner fencing, committed-prefix preservation, and tail removal.
- `tests/replica_repair.rs` covers missing-frame catch-up and corruption quarantine/rebuild.
- `python scripts/test-m111-fault-suite.py` runs topology-free append/retry, live owner failure, live replica restart/catch-up, and complete Riverbed restart suites.
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

Replication factor 3 applies to each Active Range, not to the whole Riverbed:

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

A five-Node Riverbed with RF3 has more placement and capacity options, but each range still tolerates only the failures allowed by its own three-replica set. Five cluster Nodes do not mean every range can tolerate two arbitrary Node failures while remaining writable. That stronger guarantee requires five replicas and a three-of-five majority.

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

### Small development Riverbed

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

**Status: Foundation complete; PowerShell Writer wrapper remains**

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

- `python scripts/test-m2-writer-session.py` verifies session creation, automatic idempotent sequence allocation, append through all three ingress Nodes, stable MessageId, stale-epoch rejection, revocation, and batching feedback. Ingress now waits briefly for freshly committed Writer metadata to apply locally before returning a retryable unavailable error; a unit test covers the apply race.
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
- [x] Implement signed nanosecond timestamp starts/seeks for persistent and temporary Readers.
- [x] Add bounded exponential reconnect/backoff for stateless Reader fetches and pressure-aware capacity feedback for persistent sessions.
- [~] Fence concurrent named Reader deliveries and ambiguous duplicate fetch request IDs rather than returning a different page under the same progress token; the prototype now fails closed but does not replay an identical prior fetch response.
- [~] Move per-Reader delivered/acknowledged progress and bounded fetch-result deduplication to a ReaderId-sharded RF3 data-plane journal/checkpoint, leaving only Reader definitions and shard placement in the Control Plane. A local transactional Fjall prototype keyed by ReaderId now proves fencing, one bounded retry receipt, independent ack, and restart; it is not replicated, selected as the production engine, or wired to Reader HTTP requests. RF3 ownership, journal/checkpoint recovery, fault-tested cutover, and scale-in drain remain.
- [~] Build measured per-Reader consumption pacing: a pure bounded policy now slows on replica unavailability, high acknowledgement latency, unacknowledged-byte pressure or Node pressure, and raises credits only after sustained healthy backlog. It is not yet connected to live Reader metrics or API scheduling. Prove independent progress, slow-Reader isolation and attributable CPU/memory/egress with many concurrent Readers and fault/load evidence; no industrial-scale claim from the current single-catalog prototype.

Evidence:

- Reader catalog state persists session epoch, capacity, delivered Cursor, acknowledged Cursor, and active state through Control Plane restart.
- Fetch returns committed records only, advances delivered progress independently, and never acknowledges implicitly.
- Acknowledgement must match the latest delivered Cursor; reopening increments epoch, fences the previous session, and resumes from acknowledged progress.
- Typed Rust `ReaderSessionClient` uses the same open/fetch/ack/close API.
- `python scripts/test-m3-reader-session.py` majority-commits Writer records, delivers two under capacity, acknowledges, reopens through another Node, resumes with the remaining record, rejects the stale session, starts a temporary Reader at end-of-Feed, waits for a new record, and tails the last two records.
- `ControlController` rejects stale concurrent frontier writes and repeated fetch identities for each Reader independently. A fetch verifies that the committed request result matches its expected frontier; if an ambiguous retry would return another page, it returns retryable failure rather than misreporting progress. Identical fetch-response replay remains to be implemented outside the global Control Plane. The isolated four-Node cross-Node read suite now checks two independent named Readers on the same Feed.
- `FjallReaderProgressStore` is an isolated one-keyspace local prototype: `ReaderId`-keyed rows atomically persist an epoch-fenced delivered/acknowledged frontier and bounded last-page Cursor receipt. Unit tests and `tests/reader_progress.rs` cover two independent Readers sharing a Feed, identical and conflicting retries, acknowledgement isolation, stale epochs, unacknowledged replay after process restart, and size refusal without advancement. The integration test exercises the public Rust progress-engine interface against one Fjall directory; it is not RF3 or a live HTTP path.
- `ReaderPacingController` is a pure per-Reader policy with tests for slow-Reader isolation, hysteretic credit increases, rapid decreases on pressure, byte/capacity limits and replica-outage delay. It has no live metrics or automatic API feedback integration yet.

Definition of Done:

- [x] Independent Readers never change each other's positions.
- [x] Readers never receive uncommitted records.
- [x] Named Reader restart resumes from acknowledged Cursor.
- [x] Per-session capacity bounds delivery without changing unrelated Reader progress.

## Milestone 4 — Multiple internal ranges

**Status: Current focus**

- [x] Define a stable 128-bit internal logical keyspace.
- [x] Map keys deterministically to validated contiguous ranges without exposing topology.
- [x] Persist authoritative per-Feed RangeMaps and RangeId-keyed RF3 assignments through Control Plane snapshots.
- [x] Route public appends from decoded Keys through the committed RangeMap.
- [x] Persist idempotent split plans with candidate maps, deterministic right-hand RF3 placement, and prepared/catching-up/ready stages.
- [x] Stage both candidate generations from one captured committed source boundary, with key filtering, contiguous target positions, exact logical record bytes, sparse Writer dedup rebuild, and RF3 checksum evidence.
- [x] Add a generation-scoped cutover barrier that drains in-flight appends, freezes generation 1, captures an immutable final CommitPosition, restages both candidates to that boundary, and supports explicit abort/unfreeze.
- [x] Activate ready splits atomically through consensus, installing both assignments, range-specific Writer sequence state, and the candidate map while generation 1 remains fenced.
- [x] Place and route multiple active RF3 ranges across storage Nodes.
- [x] Split hot ranges online from sustained per-range append pressure with sampled key-token boundaries, cooldown, authenticated RF3 staging, and Control Plane cutover.
- [x] Merge cold adjacent ranges using leader-aggregated cross-owner metrics, sustained low-rate evidence, cooldown, authenticated RF3 staging, rollback, Writer-state rebuild, and atomic activation.
- [~] Move ranges while preserving same-Key ordering: authenticated follower replacement passes four-Node live acceptance, and authenticated frozen-boundary Append Owner cutover passes isolated three-Node live acceptance with unavailable-target retry and Cursor continuity. Unsafe metadata-only owner transfer remains refused. Bounded page-by-page owner verification now checks committed histories past 10,000 records without materializing an unbounded prefix; resumable checkpointed movement across restart, role-separated placement, automatic drain, movement budgets, and production Node identity/encryption remain.
- [~] Keep opaque Cursors usable across range changes: the ingress fetches missing range owners for Feed/Reader reads, and a four-Node live test verifies continuation after follower movement. Control Plane Riverbeds now require owner progress and frame-digest corroboration from another RF3 replica before serving a range; owner loss or lag fails closed. Single-range Feeds resolve committed Cursors through a rebuilt local index beyond the old 10,000-record prefix. Named Readers starting at Beginning now persist a versioned opaque delivered/acknowledged per-range frontier through the Control Plane and fetch bounded pages from current owners without rescanning a 10,000-record Feed prefix. Split/merge frontier translation, legacy deep Cursor seek, temporary/public Feed pagination, durable committed-frontier proof, bounded timestamp scans, and checkpointed index recovery remain.
- [~] Define and prove a versioned opaque multi-range read continuation/frontier without exposing placement: named Reader sessions now persist the frontier and return a Reader progress token distinct from record Cursors. Historical Writer Cursor seek, temporary/public Feed pagination, split/merge translation, retry identity, and concurrent-append/clock-skew fault evidence remain.
- [ ] Persist an authoritative committed-frontier proof that survives loss of two local copies; current per-read RF3 corroboration cannot prove history that has already disappeared from a majority.
- [ ] Add hot-key detection and isolation.

Evidence:

- `src/active_range/routing.rs` defines portable 128-bit `KeyToken`, half-open `KeyRange`, validated full-coverage `RangeMap`, deterministic routing, and split generation planning.
- Unit tests prove stable same-key routing, exact split-boundary behavior, total keyspace coverage, invalid gap/overlap rejection, and portable serialization.
- Feed creation now commits an initial full RangeMap plus a RangeId-keyed assignment; old catalog snapshots migrate from the original per-Feed assignment automatically.
- Placement snapshot tests prove the RangeMap and assignment recover identically, and append ingress/owner routing resolves assignments from decoded Keys.
- Split-plan tests prove request idempotency, a distinct right-hand owner, candidate two-range coverage, complete source-prefix scanning with verified checksums, snapshot recovery, and no authoritative routing change before cutover.
- `tests/range_split_staging.rs` proves left/right key filtering from one immutable source boundary, sparse Writer-sequence import, contiguous per-range positions, byte-identical records across RF3, staged commit evidence, generation-scoped freeze/abort behavior, atomic activation, and four-record merged history after cutover.
- Range-specific Writer sequence maxima are committed with the split, so future appends remain contiguous independently in left and right ranges without exposing sequencing or topology.
- `ControlPlaneSplitCutover` executes readiness and activation through the Raft-backed Control Plane; `orchestrate_split_cutover` automatically aborts and unfreezes on any pre-activation failure.
- `SplitPressureTracker` requires sustained pressure, resets evidence below threshold, and enforces cooldown before another automatic split trigger.
- Authenticated `/internal/active-range/split/*` endpoints stage and freeze each replica, compare logical evidence across RF3, and unfreeze all contacted Nodes on pre-activation failure. Split now freezes the Append Owner first under the majority-append lock before freezing followers or reconciling uncommitted tails; deterministic paused-commit tests prove an in-flight acknowledged frame survives that boundary, including owner movement.
- `python scripts/test-m4-live-split.py` writes through all three ingress Nodes, activates a midpoint split, verifies two owners, continues appending through the new map, and reads all 16 records as one Feed.
- Per-range metrics collect records, bytes, and a bounded key-token sample; the scheduler applies sustained thresholds and cooldown, then sends the sampled median through the same authenticated split workflow.
- `python scripts/test-m4-auto-split.py` runs with low test thresholds, generates sustained traffic, retries transient ambiguous 503s using the same Writer request ID, observes an automatic one-to-two range transition with two owners, and restores production-like defaults afterward.
- Adjacent merge planning rejects reversed/non-adjacent selections, preserves full keyspace coverage, advances generation, persists idempotently through snapshots, and leaves the two-range authoritative map unchanged until data staging is verified.
- Merge staging freezes both source generations, merges committed records by ingest order into one RF3 candidate, preserves Cursors and exact frames, rebuilds Writer sequence state, automatically unfreezes on failure, and activates the one-range map only after checksum evidence is ready.
- The split/merge integration proves four records survive one-to-two split and two-to-one merge transitions while remaining readable as one logical Feed.
- `ColdRangeTracker` requires sustained low rates for both ranges, resets on either hot sample, and enforces cooldown; `cold_adjacent_pairs` never proposes non-adjacent merges.
- Authenticated merge staging runs locally on each planned replica through `/internal/active-range/merge/stage-local`; `/v1/admin/ranges/merge` compares RF3 evidence and commits readiness plus activation through the Control Plane.
- The Control Plane leader aggregates cumulative per-range counters from `/internal/active-range/pressure`, computes owner-independent rates, and invokes merge only after both adjacent ranges remain cold through the configured sustained window and cooldown policy.
- Follower movement keeps the current owner and the two retained replicas authoritative during bounded committed-prefix transfer to a fourth eligible Node. A generation-scoped freeze under the majority append lock captures final commit progress, then a consensus compare-and-set can install the RF3 replacement and higher ownership epoch. A failed final copy unfreezes the old owner; ambiguous readiness or activation leaves it frozen pending Control Plane resolution.
- `tests/active_range_placement.rs`, `tests/majority_commit.rs`, and `tests/replica_repair.rs` cover plan idempotency, stale-epoch refusal, snapshot recovery, catch-up gating, frozen-boundary behavior, failed-target rollback, ambiguity safety, Cursor-preserving restart, and continued ordered appends on the replacement. Authenticated internal movement export/stage/freeze endpoints gate Control Plane cutover, while a persisted completed-plan record makes retry-safe unfreeze possible after activation.
- `python scripts/test-m4-follower-move.py` runs an isolated four-voter development Riverbed, tests target outage before activation and retry using the same request ID, replaces one follower while maintaining RF3, appends through all ingress Nodes, verifies Cursor continuation on the new replica, and restarts it. It stops its containers without deleting persistent test volumes. The standard three-Node Riverbed is untouched. Append-owner movement and production role separation are not yet implemented.
- `active_range::replication` still refuses standalone local Feed reads when one range is absent, but public Feed and Reader endpoints now use a linearizable Control Plane placement check and authenticated owner-only per-range pages, returning a complete bounded merged result or retryable failure. Public Feed, temporary Reader, and legacy seek multi-range reads still refuse histories above 10,000 records per range or Feed, 16 MiB of decoded data, or 128 ranges; named Reader frontier reads use bounded per-range pages. Single-range reads page from a committed Cursor or the tail without rescanning earlier frames, within a per-response 10,000-record/16 MiB budget. Scalable global Cursor ordering/indexing remains unimplemented.
- `python scripts/test-m4-cross-node-read.py` uses the isolated four-Node Riverbed to split a Feed, move one follower so an ingress Node hosts only one of two ranges, then verify full Feed reads, Cursor continuation, named and temporary Readers, and no partial successful read when the remote owner is unavailable.
- `src/active_range/store.rs` rebuilds a committed Cursor-to-position index from the checked log on restart; storage tests verify truncation/collision safety and reconstruction past record 10,000. `api::tests::single_range_read_continues_beyond_ten_thousand_and_reports_unsupported_full_scan` verifies indexed start/tail continuation past the old prefix while refusing a full-history timestamp scan that cannot be served completely. Owner pages enforce a byte budget before materializing up to 32 frames. New Cursors bind stable FeedId as well as request identity, while persisted older tokens remain record-attached; a unit test checks cross-Feed distinction. Multi-range ordering still needs a durable logical read index.
- Authenticated `/internal/active-range/read/evidence` provides assignment-fenced replica CommitPosition and digest at the read boundary. Before the first page, the current owner requires one other assigned replica to corroborate the committed prefix; any observed replica ahead of the owner or a mismatched checksum fails the read as retryable. `api::tests::read_quorum_refuses_a_lost_or_behind_owner_and_digest_disagreement` checks failure boundaries; `tests/majority_commit.rs` quarantines an owner store in a temporary directory and observes its lost progress against a surviving replica. This guards single-copy loss but is not a persisted quorum watermark after two-copy loss.
- `ControlController` persists a bounded per-Reader delivery/acknowledgement frontier separately from public Reader definitions, restores acknowledged progress on session reopen, rejects stale session epochs and forged public typed commands, and clears the frontier on seek/drop. Named multi-range Reader fetch keeps a quorum-validated bounded head for every current range, merges only those heads for presentation, advances only the delivered range, and returns a versioned opaque `delivered_cursor`; per-record Cursors stay record-attached. `python scripts/test-m4-cross-node-read.py` verifies paging, acknowledgement, and reopening on another ingress Node on a split four-Node Riverbed. It does not yet prove a >10,000-record multi-range Riverbed, split/merge frontier migration, or identical fetch-response replay for the same request ID.
- Owner-move planning persists an idempotent source assignment and higher-epoch candidate on the same RF3 set. Local finalization drains appends and freezes the source and target, refuses a lagging target, compares committed record bytes/Cursors/identities through the final boundary, then activates by Control Plane compare-and-set. `tests/active_range_placement.rs` covers readiness gating, snapshot recovery, operation conflicts, stale recovery placement, and legacy direct-transfer refusal; `tests/majority_commit.rs` covers lagging-target rollback, corrupt-but-caught-up target refusal, ambiguous readiness keeping both frozen, repair, old-owner fencing, and subsequent ordered appends. Owner verification reads at most 128 frames and 64 MiB per page, checks contiguous positions and exact bytes/Cursors through the frozen boundary, and now accepts histories beyond 10,000 records; a deterministic 10,001-record disk/restart test rejects a shortened target. It is not a checkpointed, disruption-budgeted movement protocol. Authenticated `/internal/active-range/owner-move/*` endpoints verify the frozen source and target over the network; `/v1/admin/ranges/move-owner` commits the plan and cutover, and `python scripts/test-m4-owner-move.py` passes against a separate three-Node Compose Riverbed (ports 7271–7273), proving target-outage retry, continued appends, identical RF3 placement, Cursor continuity on every Node, and restart of the new owner. Production-grade Node authentication/encryption and large-history movement remain.
- These types are internal only; Feed, Key, Cursor, Writer, and Reader APIs remain unchanged.

Definition of Done:

- [ ] Feed throughput grows without changing public Feed definition.
- [ ] Clients receive no topology-change callback.
- [ ] Same-Key order survives split, move, merge, and owner failure.

## Milestone 5 — Role-aware elasticity

- [ ] Integrate Node capabilities into placement.
- [ ] Keep Control voters out of automatic replica-count scaling.
- [ ] Demonstrate 12- and 24-Node live Riverbeds with independently placed RF3 Feed ranges and Subscription progress shards, failure-domain separation, bounded Control Plane metadata/egress, and verified scale-in; the current eligible storage pool comes from statically configured Control Nodes and Feed creation still initially selects the first three. Do not claim an unbounded Node count from placement-only tests.
- [ ] Add storage-capable Nodes gradually when storage/append pressure is high.
- [ ] Add stateless compute/gateway Nodes when processing or connection pressure is high; a fourth Node may take rolling-window Pipe leases but is never required or a single window master.
- [ ] Drain according to capability before scale-in.
- [ ] Preserve configured baseline pools.
- [ ] Explain every scaling decision and constrained resource.
- [ ] Verify 5-to-8-to-5 scaling under synthetic 100k-to-200k load.

Definition of Done:

- [ ] Scale-out measurably reduces the identified pressure.
- [ ] Scale-in never reduces durability or removes unique data.
- [ ] Stateless burst capacity leaves no durable cleanup obligation.

## Milestone 6 — Persisted replicated Indexes

**Core storage requirement, not an optional optimization.** [Index storage contract and Fjall key layout](why-whitewater.md#index-storage-contract-and-fjall-layout) defines the work. The model/key codec and an isolated local Fjall Index prototype exist, but the current Feed append path is a separate file log and does **not** transactionally update the Index. Keep M4 as the single current-focus milestone while prioritizing production Index delivery before role-aware elasticity; do not expose this prototype as an application-queryable resource.

- [x] Define an internal IndexId, stable logical primary reference, shared secondary/unique-claim key encoding, composite/prefix/range access, and an update/delete mutation planner with focused unit tests (`src/index.rs`).
- [x] Prototype a locally durable Fjall-backed Index with a fixed three-keyspace layout, serializable primary/posting/checkpoint upserts and deletes, bounded exact secondary lookups, local uniqueness checks, and restart/concurrent-conflict tests (`src/index.rs`). This does not implement distributed uniqueness or Feed-to-Index atomicity.
- [x] Persist idempotent Domain-scoped StateStore declarations with manual or same-Domain Feed source in Control Plane catalogs and snapshots; their `declared` stage explicitly cannot serve data (`src/control.rs`).
- [ ] Define and persist versioned application-owned secondary Index definitions with typed fields/extractors, collation, consistency, scope, and build state through the Control Plane.
- [ ] Replicate manual StateStore mutations and their idempotency identities on RF3 with a Whitewater-managed durable journal; never claim a manually populated store can be recreated from unrelated Feed history.
- [ ] Replay Feed-derived StateStores from verified source FeedId/Cursor and bounded checkpoints, refusing rebuild if required history is gone.
- [ ] Route primary and secondary StateStore lookups from any ingress Node without application topology or co-partition configuration; bound scatter/gather and report freshness, network cost, and unavailable/lagging state.
- [ ] Benchmark vetted Fjall, redb, and RocksDB reference workloads: many user Indexes sharing a bounded number of LSM trees, mixed writes/reads, crashes, compaction stalls, and replication/checkpoint cost.
- [ ] Select and integrate an Index Engine behind a narrow transactional interface; validate serializable read-modify-write, durable batch semantics, and memory/disk budgets.
- [ ] Co-design Feed commit and required synchronous primary/secondary Index mutations across the existing file log and replicated Index state; do not claim cross-engine atomicity from a local Fjall write batch.
- [ ] Implement current-state primary rows and arbitrary declared nonunique secondary Indexes in one shared entries keyspace; atomically remove stale entries on update/delete.
- [ ] Implement global uniqueness across Active Ranges with a replicated conditional claim protocol; reject UNIQUE definitions until it is proven.
- [ ] Implement Index applied-Cursor freshness and strict reads that wait or explicitly report `index_behind`.
- [ ] Implement three-replica Index durability, verified checkpoints, bounded catch-up, and transfer.
- [ ] Implement controlled shadow-generation rebuild from retained Feed history, including `REBUILD INDEX` and `REBUILD INDEXES IN DOMAIN`, refusal when history/checkpoints are insufficient, and atomic activation.
- [ ] Implement automatic logical query routing and expose `CREATE INDEX`, `GET`, exact/prefix/range queries, and Index inspection through typed Admin API and WCL.

Definition of Done:

- [ ] Point lookup never scans a Feed.
- [ ] Strict read never silently returns stale state.
- [ ] Index owner loss preserves acknowledged indexed state.

## Milestone 7 — Subscriptions, Pipes, and atomic effects

- [x] Declare Domain-scoped Subscriptions by logical name and source Feed through typed Control Plane commands and WCL, idempotently across snapshots; `stage=declared` does not allow clients to join or acknowledge. Tests prove a declaration creates no internal/public Feed and refuses cross-Domain sources or unsafe Feed drop.
- [ ] Add a first-class Domain-wide Subscription source alongside exact-Feed sources, without resetting existing SubscriptionIds, FeedIds, or acknowledged Cursors; preserve the serialized `feed_id` of legacy exact-Feed declarations while adding a versioned source variant. Plan typed/WCL `FROM DOMAIN <domain>` as a distinct, versioned source scope bound to immutable Domain identity; a Subscription may start in an empty Domain and automatically cover all current and future authorized Feeds in it. Keep all physical placement and per-Feed progress internal; no global cross-Feed order is promised.
- [ ] Track Domain-wide Feed membership incrementally by immutable FeedId: register new Feeds from their first retained record by default (existing Feeds honor the Subscription's declared start policy), durably establish the discovery/initial-frontier boundary so writes during registration cannot be skipped, and survive Control Plane snapshots, restart, owner loss, and backfill/retention limits. Feed rename retains progress; removing or moving a Feed must not pause other Feeds or silently acknowledge/discard its pending work—fence and drain it or require explicit disposition before physical deletion. A replacement Feed with the same name must have fresh progress. Recheck authorization on newly discovered Feeds, bound work/state/egress as Feed counts grow, and explain per-Feed attaching, draining, blocked, and cost states.
- [ ] Supply mandatory server-owned source Metadata to **every** Feed and Domain consumer delivery and to every SDK `consume(record, metadata)` callback: `metadata.source.feed_name` is the last dotted Feed-name segment and `metadata.source.full_feed_name` is the full Domain-qualified Feed name (for example `created` and `orders.created`); include immutable FeedId internally for identity, without exposing range topology. Keep this typed delivery source separate from arbitrary Writer-provided record Metadata so users cannot spoof or overwrite it. Capture names in each delivery receipt so exact retry/redelivery is stable; deliveries first created after a rename use the new names without changing Cursor/FeedId. Do not claim the current `RecordResponse.metadata_base64` already provides this.
- [ ] Prove Domain subscriptions and source Metadata on an isolated multi-Node Riverbed: empty and populated Domain creation, concurrent Feed creation/append, multiple Feeds/independent Subscriptions, rename and remove/recreate while a Reader is in flight, owner loss, restart/restore, one unavailable replica, authorization changes, stable same-request replay, per-Feed same-Key ordering, no global cross-Feed ordering guarantee, bounded many-Feed load, and parity fixtures for Rust, Python, Java, C#, Node.js/TypeScript, and Go. Keep joins/ack disabled until RF3 progress, authenticated Node identity, and failover recovery pass.
- [~] Implement durable Subscription progress in hidden, ReaderId/SubscriptionId-sharded RF3 internal state and mutation journals, not Kafka-style internal Feeds. A local Fjall `subscription_progress` replica prototype now durably prepares one bounded, epoch-fenced mutation per SubscriptionId and keeps it invisible until a separate commit step with two distinct matching prepare votes; deterministic unit and restart integration tests reject conflicting retries, stale epochs and one-copy evidence. An in-process coordinator now checks a supplied three-replica assignment, requires the owner plus another matching durable prepare and commit result, and returns ambiguous failure if only one commit succeeds; deterministic integration tests cover one unavailable replica, contradictory votes, and retry after a partial commit. A separate internal `reconcile_retry` now probes all assigned replicas for the **same request identity** after an ambiguous response, rejects other identities/stale epochs/owner unavailability, replays the original mutation to complete partial commits, and returns only after a matching committed read; a still-lagging readable replica yields a retryable ambiguous result rather than an acknowledged Cursor. Restart, contradiction, lagging-replica, and no-quorum tests cover this in-process path. It cannot by itself recover a lost owner or authenticate forwarded votes. New declarations now persist a private initial owner, three replicas, and ownership epoch in Control Plane state, leader-selected over all configured eligible storage Nodes using SubscriptionId-scoped rendezvous scoring and replicated as fixed command placement; public Subscription definitions expose none of that topology. Snapshot/restart tests preserve this placement despite different local Node candidates, and older declarations without it fail closed. Deterministic tests with 3, 12, and 24 eligible candidates verify each Subscription still has RF3 while different Subscriptions spread across the configured pool; this is placement logic only, not a live 12/24-Node Riverbed or an unbounded capacity claim. Each Control Plane Node now opens a private durable `subscription-progress` Fjall directory and exposes bounded internal prepare/commit endpoints protected by the development Control Plane key; receiver/owner/epoch and vote membership are checked against current private placement before blocking Fjall writes run off Tokio. An async HTTP transport targets assigned Node endpoints; typed prepare/commit/committed-read replies now carry the claimed responder NodeId, SubscriptionId and epoch, and the coordinator refuses mismatches even when digests match. Unit/router and deterministic transport tests cover credential rejection, placement fencing, wrong-replica replies, local persistence and request delivery; the shared development key does not authenticate these identities. Optional Subscription-only mTLS now uses a separate certificate-authenticated listener, validates a configured CA and one-or-more BLAKE3 leaf-certificate pins per NodeId, and removes the Subscription routes from the plaintext listener while enabled; runtime-generated certificate tests reject missing, wrong, unpinned, and impersonated peers and exercise an overlapping-pin restart. Partial TLS configuration fails startup. This is **not** production RF3: the remaining development transport uses a shared key and plaintext, forwarded commit-vote evidence is not independently signed, the HTTP transport is not wired to a public consume path, and placement movement/drain and quorum-backed recovery remain absent. An internal placement-fenced committed-read prototype now requires at least two identical assigned replica responses and refuses any observed contradiction, stale epoch, missing majority, or uncorroborated empty read; deterministic tests cover restart, one unavailable replica, disagreement and ambiguous retry. Its development shared-key replies are **not verified Node evidence**, and there is no owner-loss reconciliation or live consumption. Member joins stay disabled. Persist separate expiring member leases, retry identities, and acknowledged frontiers without per-fetch Control Plane writes before enabling joins.
- [ ] Implement transaction/effect coordinator state in separately bounded and RF3-replicated internal storage, not an application-visible Feed; do not claim atomic consume-and-append from local Fjall transactions.
- [~] Implement epoch-fenced small-work leases: `SubscriptionLeaseTracker` now locally proves member-session fencing, bounded claims, non-overlapping grants, idempotent claim, expiry, stale-ack rejection, renewal and checked-clock behavior. It is an isolated state-machine prototype; RF3 journal/placement, verified time source, failover recovery, authenticated transport and public member joins remain.
- [ ] Implement incremental lease transfer.
- [ ] Implement Pipe definitions.
- [ ] Implement atomic consume-and-append.
- [ ] Include co-located Index mutations in the defined atomic boundary.
- [ ] Implement deterministic retry after ambiguous result.
- [ ] Add built-in per-Key rolling-window count/sum/average as managed Pipe/StateStore views: declare the business duration, Key, and aggregation, while Whitewater chooses pane/slide/checkpoint/lease layout; do not require a fourth Node or client-side Fjall.
- [ ] Define event-time default from `event_time_ns`, optional ingest-time policy, exact rolling boundaries, bounded watermark/grace, idle expiry, late-event revisions or explicit quarantine, versioned results, and retention/rebuild headroom.
- [ ] Attribute per-Domain window state/CPU/backfill cost, enforce active-Key and byte limits, and schedule epoch-fenced compute leases on the supported three-Node baseline with optional role-aware scale-out.
- [ ] Prove window boundary/overflow, duplicate and out-of-order input, late events, crash/restart, owner change, compute-lease migration, no-input expiry, bounded memory, and a three-to-four-Node run without changing the SDK contract.
- [ ] Implement a topology-free point-lookup enrichment Pipe: read a Feed, extract userId, fetch a versioned user StateStore row regardless of storage range, write an output Feed, and atomically record input progress plus output identity. Missing user or lagging state follows an explicit retry/quarantine policy.
- [ ] Run a live three-Node acceptance with User Writer updates, Reader redelivery, Node restart/owner change, userId enrichment, and same-request retries; prove exactly one committed output effect and no acknowledged input loss without co-partition configuration.
- [ ] Require the same enrichment and rolling-window semantics, retry/error contract, and runnable guide in Rust, Python, Java, C#, Node.js/TypeScript, and Go through the [shared SDK conformance plan](streams-clients.md) before calling Whitewater Streams supported.

Definition of Done:

- [ ] Membership change does not globally pause processing.
- [ ] Stale Reader cannot acknowledge after lease transfer.
- [ ] Input progress and Whitewater output effects commit together.
- [ ] Enrichment and rolling windows never require a user-defined partition count, co-partition plan, window slide/pane configuration, topology callback, or application-maintained Fjall/changelog copy.

## Cross-cutting delivery — Whitewater Streams SDK parity and guides

[Contract and guide sequence](streams-clients.md). This is scheduled product work across M2/M3 (existing Rust Writer/Reader), M6 (replicated StateStores), M7 (Subscriptions/Pipes/effects), and M9 (security/compatibility). It is not a second current-focus milestone and does not imply that Python, Java, C#, Node.js/TypeScript, or Go packages exist today. Unlike Kafka Streams, no language is a privileged processing runtime.

- [x] Define the intended language-neutral Writer, Reader, Subscription, StateStore, Pipe, Cursor, ordering, and idempotent effect semantics; publish the guide/conformance delivery plan (`docs/streams-clients.md`).
- [ ] Version and publish canonical language-neutral request/response schemas, capability negotiation, TLS/auth defaults, byte/Metadata/nanosecond encodings, and bounded transport limits without exposing Active Range topology.
- [ ] Define stable structured error codes, scope, retryability, ambiguous-commit handling, backoff, and next safe action; refuse unsupported server capabilities instead of silently downgrading safety.
- [ ] Complete the Rust application SDK beyond AdminClient: ergonomic Writer, Reader, StateStore, Subscription, and processing-effect façades once the underlying services pass tests.
- [ ] Implement Python async and sync clients with the same server contract and a runnable Writer/Reader quickstart, then the same StateStore/Pipe guide when implemented.
- [ ] Implement Java asynchronous and optional blocking clients with the same server contract; no Java-only Pipe or local-state semantics.
- [ ] Implement C# Task/IAsyncEnumerable clients with the same server contract, cancellation, and native byte/Guid/long representations.
- [ ] Implement a Node.js/TypeScript client with Promise/AsyncIterable, Buffer/Uint8Array, AbortSignal, and `bigint` nanoseconds serialized as decimal JSON strings; never expose 64-bit time as an imprecise JS number.
- [ ] Implement a Go client with context.Context cancellation, []byte records/Metadata, int64 nanoseconds, bounded iteration, and the same ambiguity/idempotency guarantees.
- [ ] Run one shared three-Node conformance suite for all six libraries: exact bytes/time/Cursor results, identical duplicate and conflicting retries, stale epochs, Reader redelivery/ack, bounded capacity, loss of an owner, restart, authentication errors, and later atomic enrichment/state freshness.
- [ ] Provide per-language unit-test doubles and publish matching runnable user guides for append/read/replay, state/index queries, enrichment, rolling-window aggregation, late-event policy, failure handling, migration from Kafka Streams, and safe operator diagnostics; mark chapters unsupported until server and SDK evidence passes.
- [ ] Verify package compatibility across supported runtimes, reproducible releases, and a published support matrix so no language quietly lacks a documented core operation.

Definition of Done:

- [ ] Rust, Python, Java, C#, Node.js/TypeScript, and Go applications can run the same named Whitewater Streams scenarios with equivalent results and retry safety, without choosing a range or co-partitioning.
- [ ] Every published guide is executable against a supported three-Node Riverbed, and unsupported operations fail explicitly.

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

## Milestone 10 — Whitewater Operations Advisor (proposed)

Design: [Whitewater Operations Advisor](operations-advisor.md). This is **not implemented**. It follows the structured health explanations, scoped identity/audit controls, and safe drain/movement gates in earlier milestones; it does not replace deterministic Control Plane recovery or block the current M4 focus.

- [ ] Define versioned, correlated diagnostic schemas for Nodes, Control Plane, storage, Writers, Readers, and later Subscriptions/Indexes/Pipes, including cause, risk, automatic action, and next safe step.
- [ ] Define a correlated health-explanation API spanning Writer, quorum, storage, Index, Subscription, and Reader stages.
- [ ] Add bounded, redacted, authorized metrics/log/trace retrieval by Riverbed, Domain, Feed, Node, and time window; treat returned content as untrusted.
- [ ] Expose read-only Whitewater health/explanation/recommendation tools via MCP, backed by the authenticated Admin API rather than a separate catalog.
- [ ] Correlate scoped Kubernetes and network evidence through a least-privilege external integration without mounting infrastructure credentials in data Nodes.
- [ ] Detect recurring busy/quiet schedules and incidents using bounded aggregates, confidence, drift detection, separate thresholds, hysteresis, and human feedback.
- [ ] Produce evidence-linked recommendations with alternatives, cost, capacity/SLO impact, durability risk, and an explicit verification/rollback plan; keep read-only advice as the default.
- [ ] Add typed action proposals, policy allowlists, human approval, short-lived scoped execution identities, immutable audit history, and an idempotent action ledger.
- [ ] Permit only pre-approved, individually proven low-risk actions via the existing Control API or a separate Operator actuator, with independent quorum/epoch/replica/drain/budget precondition checks and a kill switch.
- [ ] Test false positives, prompt injection in logs and tool responses, tenant isolation, stale evidence, model/Kubernetes outage, ambiguous action results, restart, and production-style fault scenarios.

Definition of Done:

- [ ] An operator or developer can trace a concrete Node/Writer/Reader symptom to a sourced, scoped diagnosis and safe next step without exposing internal placement to application APIs.
- [ ] No recommendation mutates state by default; approval/authorization cannot bypass RF3, Cursor/ordering, retention, or Control Plane guarantees.
- [ ] Unavailable or disabled Advisor leaves core appends, reads, and deterministic recovery unaffected.

## Unscheduled pain-point backlog

These accepted product requirements need dependency review and explicit milestone placement. They do not supersede the current focus or the first unchecked immediate action.

- [ ] Define first-class Subscription retry, delayed-delivery, quarantine, skip, and final-disposition workflows.
- [ ] Define a Schema Policy milestone covering identity, compatibility, admission validation, evolution, and generated clients.
- [ ] Expand Pipe work beyond M7's scheduled rolling-window/watermark/grace core into stream-to-stream joins, session windows, and cross-Feed event-time alignment/retractions.
- [ ] Define logical-resource cost attribution for storage, replication, movement, egress, Subscriptions, Pipes, and Indexes.
- [ ] Define richer Feed inspection, time seek, bounded search, and single-event investigation workflows.
- [ ] Define end-to-end business-flow tracing through Metadata and logical resource IDs.
Versioned protocol, structured errors, cross-language SDKs, conformance, and test doubles are scheduled under [Whitewater Streams SDK parity and guides](#cross-cutting-delivery--whitewater-streams-sdk-parity-and-guides), rather than left in this unscheduled backlog.

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
- [ ] Complete Riverbed restart

# Immediate next actions

1. [x] Write the M1.1 correctness contract and state-transition table.
2. [x] Review and approve Active Range terminology.
3. [x] Implement M1.2 domain types with unit tests.
4. [x] Create the first failing torn-tail and uncommitted-tail truncation tests.
5. [x] Implement and verify `ActiveRangeStore` from the tested recovery contract.
6. [x] Write the failing M1.4 test that requires Feed creation to commit one identical RF3 Active Range assignment on every Control Plane voter and recover it after complete Riverbed restart.
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
