# Active Range Replication Contract

## Status

- **Milestone:** M1.1
- **Status:** Approved initial correctness contract
- **Scope:** One Feed, one Active Range, three replicas, one Append Owner
- **Durability:** Two-of-three majority
- **Public visibility:** Active Range topology remains internal
- **Visual companion:** [Whitewater sequence diagrams](sequence-diagrams.md)

This document defines the correctness contract that Milestone 1 must implement. Performance optimizations may change message batching, pipelining, or transport, but must not weaken these guarantees.

## Goal

An acknowledged append:

- Exists durably on a majority of the Active Range replica set.
- Has durable majority commit evidence.
- Is visible to Readers.
- Survives loss of one replica Node.
- Is returned exactly once as one logical record under retry.
- Cannot be superseded by a stale Append Owner.

## Scope

Milestone 1 intentionally implements:

```text
one Feed
one Active Range
one RangeGeneration
one current Append Owner
three storage replicas
one monotonically increasing OwnershipEpoch
two-of-three durable majority commit
```

It does not implement dynamic range splitting, moving ranges between arbitrary Nodes, WriterSession UX, Reader delivery sessions, Indexes, or Pipes.

## Terms

### Accepted

The receiving Node has authenticated and validated the request and resolved the current Active Range assignment. No durability is implied.

### Appended

The encoded record frame has been added to a replica's active log buffer at the expected RangePosition. It may not yet survive process or machine failure.

### Flushed

The encoded frame and required local range metadata have been synchronized to durable storage on that replica.

### Majority replicated

The identical checksummed frame is flushed on at least two distinct members of the current three-replica set under the current RangeGeneration and OwnershipEpoch.

### Committed

The record is majority replicated and a CommitPosition including that record has durable commit evidence on at least two current replicas.

### Visible

The record position is at or below the local replica's committed position and may be returned to Readers.

### Acknowledged

The Writer has received a successful response containing the MessageId and Cursor. Whitewater may send success only after the record is committed.

### Uncommitted tail

Frames above CommitPosition. They are never visible and may be truncated during owner recovery.

## Core invariants

```text
visible_position <= commit_position
commit_position <= flushed_position on every replica acknowledging commit
commit_position never moves backwards
RangePosition never contains two different checksummed frames
only the current RangeGeneration may append
only the current OwnershipEpoch may append or commit
Writer identity + session/epoch + sequence identifies one logical append
success requires durable majority frame and commit evidence
```

## Append state machine

```text
Prepared
    -> Accepted
    -> Appended
    -> Flushed
    -> MajorityReplicated
    -> Committed
    -> Visible
    -> Acknowledged
```

Failure paths:

```text
Prepared / Accepted
    -> Rejected
```

```text
Appended / Flushed / MajorityReplicated
    -> owner failure without durable majority commit evidence
    -> Uncommitted
    -> Truncated
```

```text
Committed / Visible
    -> owner failure
    -> Recovered under a higher OwnershipEpoch
    -> Visible
```

```text
Committed
    -> client response lost
    -> retry with same append identity
    -> original MessageId and Cursor returned
```

## State-transition table

| Current state | Event | Preconditions | Next state | Writer success allowed |
|---|---|---|---|---|
| Prepared | Validate | Current assignment and permissions | Accepted | No |
| Accepted | Append owner frame | Current generation and epoch | Appended | No |
| Appended | Flush owner frame | Durable local frame | Flushed | No |
| Flushed | Follower durable acknowledgement | Same frame, generation, epoch, expected position | MajorityReplicated | No |
| MajorityReplicated | Persist commit evidence | CommitPosition durable on two replicas | Committed | Yes |
| Committed | Reader lookup | Position at or below CommitPosition | Visible | Already allowed |
| Any noncommitted | Owner failure | No durable majority commit evidence | Uncommitted | No |
| Uncommitted | Recovery truncation | Higher epoch established | Truncated | No |
| Committed | Owner failure | Higher epoch and committed-prefix recovery | Recovered | Existing result retained |
| Any | Stale generation or epoch | Request lower than current | Rejected | No |
| Any existing position | Different frame bytes | Digest differs | Rejected and replica unhealthy | No |

## Majority acknowledgement rule

Whitewater initially uses a strict two-stage majority rule:

1. The encoded frame is flushed by at least two replicas.
2. CommitPosition covering the frame is durably recorded by at least two replicas.
3. The Writer may then receive success.

The commit evidence may later be safely pipelined or batched, but client-visible semantics remain identical.

A single durable frame is never enough for success. The Append Owner is not a durability exception.

## Read visibility

Readers may observe only positions at or below CommitPosition.

```text
position <= commit_position -> readable
position > commit_position  -> invisible
```

A local replica may contain additional flushed frames. Those frames are an uncommitted tail and must not produce Cursors or appear in reads.

Independent Readers and Subscriptions retain their own logical Cursor positions. Range positions remain internal.

## Same-Key ordering

For one FeedId and Key:

- Committed records are observed in accepted sequence order.
- Retry does not create a second ordering position.
- Owner recovery resumes after the committed prefix.
- Uncommitted tail truncation cannot reorder committed records.
- Relative ordering between unrelated Keys remains unspecified.

## Idempotency and ambiguous results

An append identity consists of a stable Writer identity/session epoch and sequence. The same identity:

- With identical content returns the original MessageId and Cursor.
- With different key, payload, or Metadata is rejected as a conflict.
- Remains valid across owner failure and retry.

A network timeout does not prove failure. The operation may have committed after the caller stopped waiting. The caller retries with the same append identity until it receives the original result or an explicit terminal error.

## Generation and epoch fencing

### RangeGeneration

Changes when the logical range is replaced through split, merge, or reconstruction. Requests for older generations are rejected.

### OwnershipEpoch

Changes whenever the Control Plane assigns a new Append Owner. A request with a lower epoch is rejected by every replica, including the former owner after it returns.

An epoch may only increase through a committed Control Plane transition.

## Replica append validation

A replica accepts a frame only when:

- FeedId matches.
- RangeId matches.
- RangeGeneration matches.
- OwnershipEpoch equals the current epoch.
- Sender is the current Append Owner.
- Position is the expected next position or an identical retry.
- Frame checksum is valid.
- Existing bytes at that position, if present, are identical.

A position gap, stale epoch, wrong generation, invalid checksum, or content conflict is rejected.

## Internal replica append protocol

The current authenticated internal endpoint is:

```http
POST /internal/active-range/replica/append
x-whitewater-control-key: <internal-credential>
Content-Type: application/json
```

Authentication executes before JSON body decoding. The endpoint applies a bounded body limit derived from the maximum encoded record frame.

The request carries:

```text
FeedId
RangeId
RangeGeneration
OwnershipEpoch
Append Owner StorageNodeId
expected RangePosition
Writer session identity, epoch, and sequence
opaque Cursor
base64 of the already encoded checksummed record frame
```

The receiver resolves the committed assignment from its applied Control Plane catalog. It never trusts topology supplied only by the sender. It verifies that the sender is the current owner and that the local Node is one of the three current replicas.

The receiver decodes and validates the frame but never reconstructs or re-encodes it. `ActiveRangeStore` synchronizes the exact supplied bytes and returns their BLAKE3 digest. An identical retry at an existing position returns the original durable result; different bytes or identity at that position are rejected.

Protocol errors include a stable code, actionable message, and retryability flag. Missing local assignment may be retryable while the receiver catches up; wrong range, stale owner, invalid checksum, position gap, and content conflict require caller correction or recovery action.

The prototype endpoint uses the existing internal shared credential over HTTP. Production encryption in transit remains mandatory Milestone 9 work; the shared credential is authentication, not transport encryption.

Operators may choose how inter-Node encryption is supplied:

- Whitewater-native TLS with mutual Node authentication, the default and recommended deployment.
- A service mesh or sidecar that provides mutually authenticated TLS while Whitewater verifies the authenticated peer identity delivered by that trusted boundary.
- An orchestrator or private-network transport with equivalent authenticated encryption, when its identity, rotation, audit, and downgrade-prevention guarantees are explicitly integrated and verified.

The mechanism is selectable; the production guarantee is not. Plain HTTP is development-only, encryption must cover Control Plane and replica traffic end to end between trusted Node identities, and Whitewater must fail closed when the configured secure transport or peer identity cannot be verified. Payload or at-rest encryption is separate and does not replace transport encryption.

## Majority commit coordinator

The Append Owner performs one append as a two-stage majority operation:

1. Validate the committed assignment and ownership epoch.
2. Append and synchronize the exact frame locally.
3. Send the same expected position and encoded frame to both followers concurrently.
4. Require one follower to acknowledge the same position and BLAKE3 digest, producing two durable frame copies.
5. Send the commit position and expected frame digest to every follower that durably stored the frame.
6. Require one follower to persist matching commit evidence.
7. Persist CommitPosition locally as the second evidence record, then return success.

A follower accepts commit evidence only when the exact digest is already durable at the target position. CommitPosition remains monotonic and cannot pass the flushed boundary.

```text
3 healthy replicas -> three durable copies and up to three commit records
owner + follower A  -> two durable copies and two commit records -> success
owner + follower B  -> two durable copies and two commit records -> success
owner only          -> one durable copy -> no success
```

A frame majority without a commit-evidence majority returns a retryable ambiguous failure. The frame may exist on two or three replicas but remains invisible. Retrying the identical Writer identity and sequence reuses those durable frames and attempts commit again. A successful commit whose client response is lost is recovered the same way: retry returns the original MessageId, Cursor, and position.

The initial coordinator contacts both followers concurrently and waits for both bounded transport outcomes before completing. Optimizing the non-required follower into supervised background propagation is deferred until correctness and fault tests are complete.

## Owner failure recovery

Recovery proceeds only after the Control Plane commits a higher OwnershipEpoch.

The new owner:

1. Obtains append, flush, and commit positions from available replicas.
2. Determines the highest prefix with durable majority commit evidence.
3. Preserves that committed prefix.
4. Truncates frames above the recovered CommitPosition.
5. Repairs replicas missing committed frames.
6. Restores Writer deduplication state through the committed prefix.
7. Resumes appends at the next position.

The longest individual replica log is not automatically truth.

## Failure outcomes

| Failure point | Required outcome |
|---|---|
| Before owner append | No frame exists |
| After owner append, before flush | Frame may disappear; no visibility or success |
| After owner flush, before follower flush | Single copy may remain; no visibility or success |
| After two frame flushes, before majority commit evidence | No success; recovery follows durable commit evidence only |
| After majority commit evidence, before response | Record survives; retry returns original result |
| After Writer receives success | Record survives any one-replica loss |
| Old owner returns | Higher epoch fences all append and commit attempts |
| One follower unavailable | Majority writes continue with owner and remaining follower |
| Both followers unavailable | No write success |

## Initial executable model tests

```text
cannot_commit_with_one_durable_copy
can_commit_with_two_of_three_durable_copies
uncommitted_record_is_never_visible
commit_position_never_moves_backwards
higher_epoch_fences_previous_owner
old_generation_is_rejected
replica_set_requires_three_unique_nodes
owner_must_be_a_replica
conflicting_bytes_at_same_position_are_rejected
retry_after_commit_returns_original_result
```

## M1.1 review checklist

- [x] Accepted, appended, flushed, committed, visible, and acknowledged are distinct.
- [x] Majority frame durability is defined.
- [x] Majority commit evidence is defined.
- [x] Reader visibility is limited to committed positions.
- [x] Same-Key ordering is defined across retry and owner recovery.
- [x] Ambiguous retry uses stable append identity.
- [x] Uncommitted-tail truncation is defined.
- [x] RangeGeneration and OwnershipEpoch fencing are distinct.
- [x] Recovery uses majority commit evidence rather than the longest replica.
- [x] Crash points map to expected outcomes and tests.

## Deferred optimizations

The following may change after correctness is proven without changing this contract:

- Batch several frames per replica request.
- Pipeline follower replication.
- Batch CommitPosition persistence.
- Piggyback commit progress on later replication.
- Compress frame batches.
- Acknowledge different durability policies stronger than RF3.

Any optimization must continue to satisfy the invariants and fault tests above.
