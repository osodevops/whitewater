use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{watch, Mutex, Semaphore};
use uuid::Uuid;

use crate::control::{unix_ns, ControlController, RangeCommitProof};

use super::{
    commit_proof::CommitProofReporter, CommitPosition, RangePosition, ReplicaAppendAccepted,
    ReplicaAppendError, ReplicaAppendErrorCode, ReplicaAppendRequest, ReplicaAppendResponse,
    ReplicaAppendService, ReplicaCommitAccepted, ReplicaCommitRequest, ReplicaCommitResponse,
    ReplicaDiscardPendingRequest, ReplicaDiscardPendingResponse, StorageNodeId,
    ACTIVE_RANGE_COMMIT_QUORUM,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MajorityAppendResult {
    pub message_id: Uuid,
    pub cursor: String,
    pub position: super::RangePosition,
    pub frame_digest: [u8; 32],
    pub durable_replicas: Vec<StorageNodeId>,
    pub commit_evidence: Vec<StorageNodeId>,
    pub deduplicated: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MajorityAppendErrorCode {
    AssignmentNotFound,
    NotCurrentOwner,
    FrameConflict,
    NoDurableMajority,
    NoCommitMajority,
    PendingEffectOccupied,
    LocalStorageFailure,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MajorityAppendError {
    pub code: MajorityAppendErrorCode,
    pub message: String,
    pub retryable: bool,
    pub durable_replicas: Vec<StorageNodeId>,
    pub commit_evidence: Vec<StorageNodeId>,
    /// The effect holding the tail when `code` is `PendingEffectOccupied`.
    #[serde(default)]
    pub pending_effect: Option<super::store::PendingEffect>,
}

impl fmt::Display for MajorityAppendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(formatter)
    }
}

impl std::error::Error for MajorityAppendError {}

#[derive(Clone, Debug)]
pub struct ReplicaTransportError {
    pub message: String,
    pub retryable: bool,
    /// The replica's refusal code, when the failure was a replica verdict
    /// rather than a transport failure.
    pub code: Option<ReplicaAppendErrorCode>,
    /// The replica's next expected RangePosition on a `PositionGap` verdict.
    pub expected_position: Option<u64>,
}

impl ReplicaTransportError {
    /// A transport-layer failure: the replica's verdict is unknown.
    pub fn transport(message: impl Into<String>, retryable: bool) -> Self {
        Self {
            message: message.into(),
            retryable,
            code: None,
            expected_position: None,
        }
    }

    /// A refusal the replica returned with a structured verdict.
    pub fn replica(error: ReplicaAppendError) -> Self {
        Self {
            message: error.message,
            retryable: error.retryable,
            code: Some(error.code),
            expected_position: error.expected_position,
        }
    }
}

impl fmt::Display for ReplicaTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(formatter)
    }
}

impl std::error::Error for ReplicaTransportError {}

#[async_trait]
pub trait ReplicaTransport: Send + Sync {
    async fn append(
        &self,
        replica: &StorageNodeId,
        request: ReplicaAppendRequest,
    ) -> Result<ReplicaAppendAccepted, ReplicaTransportError>;

    /// Fan out one ordered contiguous batch in a single round-trip.
    async fn append_batch(
        &self,
        replica: &StorageNodeId,
        request: crate::active_range::replication::ReplicaBatchAppendRequest,
    ) -> Result<Vec<ReplicaAppendAccepted>, ReplicaTransportError>;

    async fn commit(
        &self,
        replica: &StorageNodeId,
        request: ReplicaCommitRequest,
    ) -> Result<ReplicaCommitAccepted, ReplicaTransportError>;

    /// Discard one effect's pending tail on `replica`; marker verification
    /// inside the replica store truncates only the matching occupant.
    async fn discard_pending(
        &self,
        replica: &StorageNodeId,
        request: ReplicaDiscardPendingRequest,
    ) -> Result<ReplicaDiscardPendingResponse, ReplicaTransportError>;
}

/// Rounds a follower or the local commit may process concurrently per
/// range. Tickets still issue in mint order, so the window bounds
/// uncommitted exposure without letting later rounds overtake earlier
/// ones on any single replica.
const MAX_INFLIGHT_ROUNDS: usize = 16;

/// Concurrent sends one (range, follower) lane may hold in flight. The
/// `OrderedTurn` still admits sends strictly in mint order, so a replica
/// observes the identical position sequence — the window only overlaps
/// the network round-trips, matching Kafka's multiple in-flight
/// producer requests per partition.
const MAX_INFLIGHT_SENDS_PER_FOLLOWER: usize = 4;

/// FIFO-ordered turn queue: tickets mint inside the range's serialized
/// mint/append section, and each holder waits for its ticket before
/// running its critical section (one follower send, or the local commit).
/// This keeps every replica's append order and the local commit order
/// identical to mint order while rounds pipeline across the network.
type SendTurns =
    Arc<Mutex<std::collections::BTreeMap<(super::RangeId, StorageNodeId), Arc<OrderedTurn>>>>;
type RangeTurns = Arc<Mutex<std::collections::BTreeMap<super::RangeId, Arc<OrderedTurn>>>>;
type InFlightPermits = Arc<Mutex<std::collections::BTreeMap<super::RangeId, Arc<Semaphore>>>>;
type SendLanes =
    Arc<Mutex<std::collections::BTreeMap<(super::RangeId, StorageNodeId), Arc<Semaphore>>>>;
type RepairLocks =
    Arc<Mutex<std::collections::BTreeMap<(super::RangeId, StorageNodeId), Arc<Mutex<()>>>>>;

struct OrderedTurn {
    next_issue: AtomicU64,
    ready: watch::Sender<u64>,
    ready_rx: watch::Receiver<u64>,
}

/// A minted place in an `OrderedTurn`. Dropping it advances the queue —
/// even when the holder's round failed — so a cancelled or refused round
/// can never wedge the pipeline behind it.
struct TurnPermit {
    turn: Arc<OrderedTurn>,
    ticket: u64,
}

impl OrderedTurn {
    fn new() -> Self {
        let (ready, ready_rx) = watch::channel(0_u64);
        Self {
            next_issue: AtomicU64::new(0),
            ready,
            ready_rx,
        }
    }

    /// Mint a place in line; call inside the serialized section so ticket
    /// order matches mint order.
    fn hold(self: &Arc<Self>) -> TurnPermit {
        TurnPermit {
            turn: Arc::clone(self),
            ticket: self.next_issue.fetch_add(1, Ordering::SeqCst),
        }
    }

    async fn wait_turn(&self, ticket: u64) {
        let mut rx = self.ready_rx.clone();
        // `>=`: a dropped predecessor already advanced the queue.
        let _ = rx.wait_for(|next| *next >= ticket).await;
    }
}

impl TurnPermit {
    async fn wait(&self) {
        self.turn.wait_turn(self.ticket).await;
    }
}

impl Drop for TurnPermit {
    fn drop(&mut self) {
        self.turn.ready.send_modify(|next| *next += 1);
    }
}

/// Frames one group round may carry: keeps merged quorum rounds inside
/// the follower replica-batch bounds (`MAX_REPLICA_BATCH_FRAMES` on
/// current followers; older Nodes still enforce 512 and would refuse a
/// merged round — mixed-version clusters merge less).
const MAX_GROUP_ROUND_FRAMES: usize = 4_096;
/// Raw byte cap per merged round (frame + prefix overhead included).
const MAX_GROUP_ROUND_BYTES: usize = 16 * 1024 * 1024;
/// Per-range bound on submissions parked behind the append lock.
const MAX_GROUP_QUEUE: usize = 4_096;

static NEXT_GROUP_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// One client batch parked on a Range's group-commit queue. The driver
/// that drains it runs the quorum round and reports the tail slice back.
struct GroupSubmission {
    id: u64,
    /// Held until this submission is drained — the queue-depth bound.
    _queue_permit: tokio::sync::OwnedSemaphorePermit,
    requests: Vec<super::replication::ReplicaAppendRequest>,
    /// Sequence-0 frames for minted submissions (`None` per request for
    /// explicit-sequence submissions).
    frames: Vec<Option<Vec<u8>>>,
    reply: tokio::sync::oneshot::Sender<Result<Vec<MajorityAppendResult>, MajorityAppendError>>,
}

/// True when two submissions may merge into one quorum round: same
/// Feed/Range/generation/epoch, owner-local minting, and no pending
/// effect (effect runs stay fenced solo).
fn group_compatible(
    a: &super::replication::ReplicaAppendRequest,
    b: &super::replication::ReplicaAppendRequest,
) -> bool {
    a.feed_id == b.feed_id
        && a.range_id == b.range_id
        && a.generation == b.generation
        && a.ownership_epoch == b.ownership_epoch
        && a.pending_effect.is_none()
        && b.pending_effect.is_none()
}

/// One detached group round handed to a range's executor thread: the
/// closure captures every Send input (submissions, guards, permits) and
/// materializes the round's future inside the executor's own runtime.
// The future is intentionally NOT Send-bound: it is constructed and
// run on the range's executor thread via spawn_local, so non-Send
// sections never cross a boundary. Only the CLOSURE (Send captures)
// crosses the channel.
type RoundJob = Box<
    dyn FnOnce() -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                    Output = Result<Vec<MajorityAppendResult>, MajorityAppendError>,
                >,
            >,
        > + Send,
>;

type RoundReply =
    tokio::sync::oneshot::Sender<Result<Vec<MajorityAppendResult>, MajorityAppendError>>;

/// Fixed shared pool of round-executor lanes — never grows, never
/// leaks a thread per range.
type RoundExecutorLanes =
    Arc<Mutex<Option<Vec<tokio::sync::mpsc::UnboundedSender<(RoundJob, RoundReply)>>>>>;

#[derive(Clone)]
pub struct MajorityAppendCoordinator {
    local: Arc<ReplicaAppendService>,
    control: Arc<ControlController>,
    transport: Arc<dyn ReplicaTransport>,
    // Serialized per range: position allocation and the local tail append
    // must not interleave, while unrelated ranges on this Node pipeline
    // independently. Follower sends and the local commit leave this
    // section early — their order is preserved by OrderedTurn queues —
    // so the network round-trip of one quorum round overlaps the mint +
    // local append of the next.
    append_locks: Arc<Mutex<std::collections::BTreeMap<super::RangeId, Arc<Mutex<()>>>>>,
    // Per (range, follower) send order — replicas demand contiguous
    // positions, so concurrent in-flight rounds must still reach each
    // follower in mint order.
    send_turns: SendTurns,
    // Per range local commit order — the committed watermark only advances
    // over contiguous quorum evidence in mint order.
    commit_turns: RangeTurns,
    // Per range bound on concurrent rounds; cutover freezes drain it by
    // acquiring every permit so the frozen boundary is quorum-stable.
    in_flight: InFlightPermits,
    // Per (range, follower) bound on concurrently in-flight sends; a
    // round holds one permit from send start through any gap repair.
    send_lanes: SendLanes,
    // Per (range, follower) serializer for gap re-drives: overlapping
    // rounds that each miss positions repair one at a time — re-driven
    // frames dedup idempotently on the replica.
    repair_locks: RepairLocks,
    /// Per-range group-commit queue: submissions waiting on the append
    /// lock merge into one quorum round, amortizing the fixed cost of a
    /// round across waiting client batches — the mechanism that lets a
    /// single Range pipeline like Kafka followers fetching accumulated
    /// records.
    group_queues: Arc<
        Mutex<
            std::collections::BTreeMap<super::RangeId, std::collections::VecDeque<GroupSubmission>>,
        >,
    >,
    /// Per-range bound on queued-but-undrained submissions.
    group_queue_windows: Arc<Mutex<std::collections::BTreeMap<super::RangeId, Arc<Semaphore>>>>,
    /// Per-range round executor: a dedicated OS thread with its own
    /// current-thread runtime runs group rounds detached — cancelling
    /// the driving HTTP request can never abort a quorum round. The
    /// round future is CONSTRUCTED on the executor thread so non-Send
    /// sections never cross a thread boundary.
    round_executors: RoundExecutorLanes,

    proofs: Option<Arc<CommitProofReporter>>,
}

impl MajorityAppendCoordinator {
    pub fn new(
        local: Arc<ReplicaAppendService>,
        control: Arc<ControlController>,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Self {
        Self {
            local,
            control,
            transport,
            append_locks: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            send_turns: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            commit_turns: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            in_flight: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            send_lanes: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            repair_locks: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            group_queues: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            group_queue_windows: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            round_executors: Arc::new(Mutex::new(None)),
            proofs: None,
        }
    }

    async fn send_turn(
        &self,
        range_id: super::RangeId,
        follower: &StorageNodeId,
    ) -> Arc<OrderedTurn> {
        self.send_turns
            .lock()
            .await
            .entry((range_id, follower.clone()))
            .or_insert_with(|| Arc::new(OrderedTurn::new()))
            .clone()
    }

    async fn commit_turn(&self, range_id: super::RangeId) -> Arc<OrderedTurn> {
        self.commit_turns
            .lock()
            .await
            .entry(range_id)
            .or_insert_with(|| Arc::new(OrderedTurn::new()))
            .clone()
    }

    /// Shared executor lanes: `min(available_parallelism, 8)` threads,
    /// each with its own current-thread runtime + LocalSet; a range
    /// hashes to one lane so rounds run detached without a thread per
    /// range.
    async fn round_executor(
        &self,
        range_id: super::RangeId,
    ) -> tokio::sync::mpsc::UnboundedSender<(RoundJob, RoundReply)> {
        let mut lanes = self.round_executors.lock().await;
        let lanes = lanes.get_or_insert_with(|| {
            (0..std::thread::available_parallelism()
                .map(|count| count.get().min(8))
                .unwrap_or(1))
                .map(|lane| {
                    let (tx, mut rx) =
                        tokio::sync::mpsc::unbounded_channel::<(RoundJob, RoundReply)>();
                    std::thread::Builder::new()
                        .name(format!("ww-group-round-{lane}"))
                        .spawn(move || {
                            let runtime = tokio::runtime::Builder::new_current_thread()
                                .enable_all()
                                .build()
                                .expect("group round runtime");
                            let local = tokio::task::LocalSet::new();
                            local.block_on(&runtime, async move {
                                while let Some((job, reply)) = rx.recv().await {
                                    tokio::task::spawn_local(async move {
                                        let outcome = job().await;
                                        let _ = reply.send(outcome);
                                    });
                                }
                            });
                        })
                        .expect("group round thread");
                    tx
                })
                .collect()
        });
        lanes[(range_id.as_uuid().as_u128() as usize) % lanes.len()].clone()
    }

    async fn group_queue_window(&self, range_id: super::RangeId) -> Arc<Semaphore> {
        self.group_queue_windows
            .lock()
            .await
            .entry(range_id)
            .or_insert_with(|| Arc::new(Semaphore::new(MAX_GROUP_QUEUE)))
            .clone()
    }

    async fn in_flight_window(&self, range_id: super::RangeId) -> Arc<Semaphore> {
        self.in_flight
            .lock()
            .await
            .entry(range_id)
            .or_insert_with(|| Arc::new(Semaphore::new(MAX_INFLIGHT_ROUNDS)))
            .clone()
    }

    async fn send_lane(
        &self,
        range_id: super::RangeId,
        follower: &StorageNodeId,
    ) -> Arc<Semaphore> {
        self.send_lanes
            .lock()
            .await
            .entry((range_id, follower.clone()))
            .or_insert_with(|| Arc::new(Semaphore::new(MAX_INFLIGHT_SENDS_PER_FOLLOWER)))
            .clone()
    }

    async fn repair_lock(
        &self,
        range_id: super::RangeId,
        follower: &StorageNodeId,
    ) -> Arc<Mutex<()>> {
        self.repair_locks
            .lock()
            .await
            .entry((range_id, follower.clone()))
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Report a durable committed-frontier proof through `reporter` after
    /// each quorum commit, so loss of two local copies can be told apart
    /// from replica lag when only the majority-replicated catalog
    /// survives.
    pub fn with_commit_proofs(mut self, reporter: Arc<CommitProofReporter>) -> Self {
        self.proofs = Some(reporter);
        self
    }

    fn report_commit_proof(
        &self,
        feed_id: Uuid,
        range_id: super::RangeId,
        position: CommitPosition,
        frame_digest: [u8; 32],
        evidence: Vec<StorageNodeId>,
    ) {
        let Some(reporter) = &self.proofs else {
            return;
        };
        reporter.report(RangeCommitProof {
            feed_id,
            range_id,
            position,
            frame_digest,
            recorded_at_ns: unix_ns(),
            evidence,
            reporter: self.local.local_node().clone(),
        });
    }

    pub async fn next_position(
        &self,
        feed_id: Uuid,
        range_id: super::RangeId,
    ) -> Result<super::RangePosition, MajorityAppendError> {
        self.local
            .next_position(feed_id, range_id)
            .await
            .map_err(|error| {
                self.error(
                    MajorityAppendErrorCode::LocalStorageFailure,
                    error.to_string(),
                    error.retryable,
                    vec![],
                    vec![],
                )
            })
    }

    pub async fn freeze_for_follower_move(
        &self,
        source: &super::ActiveRangeAssignment,
    ) -> Result<CommitPosition, MajorityAppendError> {
        self.freeze_for_cutover(source).await
    }

    pub async fn freeze_for_split(
        &self,
        source: &super::ActiveRangeAssignment,
    ) -> Result<CommitPosition, MajorityAppendError> {
        self.freeze_for_cutover(source).await
    }

    async fn append_lock(&self, range_id: super::RangeId) -> Arc<Mutex<()>> {
        self.append_locks
            .lock()
            .await
            .entry(range_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn freeze_for_cutover(
        &self,
        source: &super::ActiveRangeAssignment,
    ) -> Result<CommitPosition, MajorityAppendError> {
        // Drain the in-flight window first: every round that already
        // minted must reach its commit turn so the frozen boundary
        // reflects every quorum that can still complete. New rounds block
        // on the exhausted window and then hit the frozen-generation
        // refusal at their local append.
        let _drain = self
            .in_flight_window(source.range_id)
            .await
            .acquire_many_owned(MAX_INFLIGHT_ROUNDS as u32)
            .await
            .map_err(|_| {
                self.error(
                    MajorityAppendErrorCode::LocalStorageFailure,
                    "Active Range append window is closed",
                    true,
                    vec![],
                    vec![],
                )
            })?;
        let _append_guard = self.append_lock(source.range_id).await.lock_owned().await;
        let current = self
            .control
            .active_range_assignment_by_id(source.range_id)
            .await;
        if current.as_ref() != Some(source) || source.owner != *self.local.local_node() {
            return Err(self.error(
                MajorityAppendErrorCode::NotCurrentOwner,
                "Active Range placement changed before cutover freeze",
                true,
                vec![],
                vec![],
            ));
        }
        self.local
            .freeze_generation(source.range_id, source.generation)
            .await;
        let result = async {
            let status = self.local.recovery_status_for_assignment(source).await?;
            self.local
                .truncate_uncommitted_for_assignment(source)
                .await?;
            Ok::<_, super::ReplicaAppendError>(status.committed)
        }
        .await;
        match result {
            Ok(commit) => Ok(commit),
            Err(error) => {
                self.local
                    .unfreeze_generation(source.range_id, source.generation)
                    .await;
                Err(self.error(
                    MajorityAppendErrorCode::LocalStorageFailure,
                    error.to_string(),
                    error.retryable,
                    vec![],
                    vec![],
                ))
            }
        }
    }

    pub async fn append(
        &self,
        request: ReplicaAppendRequest,
    ) -> Result<MajorityAppendResult, MajorityAppendError> {
        self.append_inner(
            request,
            None::<fn(u64) -> Result<bytes::Bytes, crate::codec::CodecError>>,
        )
        .await
    }

    /// Owner-minted session append: under the range's append lock the owner
    /// derives the next contiguous Writer sequence from the replica's landed
    /// dedup stream, then `encode_with_sequence` builds the frame so the
    /// record's `producer_sequence` and the replicated identity agree before
    /// any replica sees bytes. Retries deduplicate on the request-derived
    /// Cursor, and cutovers inherit the minted stream from staged frames.
    pub async fn append_minted<F>(
        &self,
        request: ReplicaAppendRequest,
        encode_with_sequence: F,
    ) -> Result<MajorityAppendResult, MajorityAppendError>
    where
        F: FnOnce(u64) -> Result<bytes::Bytes, crate::codec::CodecError> + Send,
    {
        self.append_inner(request, Some(encode_with_sequence)).await
    }

    async fn append_inner<F>(
        &self,
        mut request: ReplicaAppendRequest,
        mint: Option<F>,
    ) -> Result<MajorityAppendResult, MajorityAppendError>
    where
        F: FnOnce(u64) -> Result<bytes::Bytes, crate::codec::CodecError> + Send,
    {
        let _append_guard = self.append_lock(request.range_id).await.lock_owned().await;
        request.expected_position = self
            .next_position(request.feed_id, request.range_id)
            .await?;
        let assignment = self
            .control
            .active_range_assignment_by_id(request.range_id)
            .await
            .ok_or_else(|| {
                self.error(
                    MajorityAppendErrorCode::AssignmentNotFound,
                    "Active Range assignment is not available",
                    true,
                    vec![],
                    vec![],
                )
            })?;
        if assignment.owner != *self.local.local_node() || request.append_owner != assignment.owner
        {
            // An ownership cutover leaves ingress Nodes and callers holding
            // the previous placement for a few catalog applies; the refusal
            // is transient, so classify it retryable — each retry re-resolves
            // the assignment and lands on the converged owner.
            return Err(self.error(
                MajorityAppendErrorCode::NotCurrentOwner,
                format!(
                    "Node {} is not the current Append Owner",
                    self.local.local_node()
                ),
                true,
                vec![],
                vec![],
            ));
        }
        // The caller stamps the epoch from its own catalog view, which can
        // lag a committed cutover; surfacing that lag as a fatal refusal
        // would wedge Writers on routing residue. The owner just proved it
        // is the current owner, so the catalog epoch it read is the
        // authoritative fencing stamp for the frame this append produces —
        // replicas and the local store fence on it identically.
        request.ownership_epoch = assignment.ownership_epoch;
        if let Some(encode_with_sequence) = mint {
            // Owner-minted Writer sequences: the session append path sends no
            // pre-allocated number; the owner derives it from this replica's
            // landed dedup stream under the per-range append lock, so a mint
            // is contiguous with what actually committed and survives
            // restarts and owner cutovers. A failed append consumes nothing —
            // the next mint re-derives the same number — and a retried
            // request deduplicates by its request-derived Cursor.
            request.identity.sequence = self
                .local
                .next_writer_sequence(
                    &assignment,
                    request.identity.writer_session_id,
                    request.identity.writer_epoch,
                )
                .await
                .map_err(|error| {
                    self.error(
                        MajorityAppendErrorCode::LocalStorageFailure,
                        format!("owner could not mint the Writer sequence: {error}"),
                        true,
                        vec![],
                        vec![],
                    )
                })?;
            request.frame = encode_with_sequence(request.identity.sequence).map_err(|error| {
                self.error(
                    MajorityAppendErrorCode::LocalStorageFailure,
                    format!("owner could not encode the minted record: {error}"),
                    false,
                    vec![],
                    vec![],
                )
            })?;
        }
        let local = self.local.append(request.clone()).await.map_err(|error| {
            if error.code == ReplicaAppendErrorCode::PendingEffectOccupied {
                return self.occupied_error(error.pending_effect.map(|boxed| *boxed));
            }
            self.error(
                MajorityAppendErrorCode::LocalStorageFailure,
                error.to_string(),
                error.retryable,
                vec![],
                vec![],
            )
        })?;
        // The frame's true position is where the local append landed it; a
        // retried request deduplicates to an earlier position than the
        // pre-allocation tip, and replicas must be offered that position.
        request.expected_position = local.position;
        if local.deduplicated {
            // A Cursor-dedup hit can land the request on a stored frame
            // whose identity differs from the request's: a re-sequenced
            // retry shares the request-derived Cursor but carries a freshly
            // allocated or healed sequence. Pushing the request's bytes at
            // that position makes every replica refuse a different committed
            // occupant; offer the canonical stored frame so replicas dedup
            // or append it on its own identity.
            let request_digest = *blake3::hash(&request.frame).as_bytes();
            if request_digest != local.frame_digest {
                let after = local
                    .position
                    .value()
                    .checked_sub(1)
                    .map(RangePosition::new);
                let frames = self
                    .local
                    .read_assignment_uncommitted(&assignment, after, 1)
                    .await
                    .map_err(|error| {
                        self.error(
                            MajorityAppendErrorCode::LocalStorageFailure,
                            format!("owner could not reload the deduplicated frame: {error}"),
                            true,
                            vec![],
                            vec![],
                        )
                    })?;
                if let Some(frame) = frames.into_iter().next() {
                    if frame.position == local.position {
                        request.frame = frame.frame.clone();
                        request.identity = frame.identity;
                        request.cursor = frame.cursor;
                    }
                }
            }
        }
        let mut durable = vec![self.local.local_node().clone()];
        let followers = assignment
            .replicas
            .iter()
            .filter(|node| *node != self.local.local_node())
            .cloned()
            .collect::<Vec<_>>();
        let first = self.transport.append(&followers[0], request.clone());
        let second = self.transport.append(&followers[1], request.clone());
        let (first_result, second_result) = tokio::join!(first, second);
        let follower_results = [
            (followers[0].clone(), first_result),
            (followers[1].clone(), second_result),
        ];
        let mut durable_followers = Vec::new();
        let mut divergent: Vec<StorageNodeId> = Vec::new();
        for (node, result) in follower_results {
            let result = match result {
                Err(error) if error.code == Some(ReplicaAppendErrorCode::PositionGap) => match self
                    .fill_replica_gap(
                        &assignment,
                        &request,
                        &node,
                        error.expected_position,
                        local.position,
                    )
                    .await
                {
                    Ok(()) => self.transport.append(&node, request.clone()).await,
                    Err(fill_error) => Err(fill_error),
                },
                other => other,
            };
            match result {
                Ok(accepted) => {
                    // Committed-divergent evidence: the replica matched this
                    // frame to a different position inside its committed
                    // prefix — the store-level proof a forked owner left
                    // behind. Counting it as a quorum refusal would let the
                    // durable pair forge committed divergence — veto the
                    // append and surface the fork instead. A bare digest or
                    // uncommitted-position mismatch is a response-level
                    // contradiction, not fork evidence: keep it fatal.
                    if accepted.position != local.position && accepted.committed {
                        divergent.push(node);
                    } else if accepted.position != local.position
                        || accepted.frame_digest != local.frame_digest
                    {
                        return Err(self.error(
                            MajorityAppendErrorCode::FrameConflict,
                            format!("replica {node} acknowledged different bytes or position"),
                            false,
                            durable,
                            vec![],
                        ));
                    } else {
                        durable.push(node.clone());
                        durable_followers.push(node);
                    }
                }
                Err(error) => {
                    if error.code == Some(ReplicaAppendErrorCode::CommittedPositionConflict) {
                        divergent.push(node.clone());
                    }
                    tracing::warn!(
                        replica = %node,
                        feed_id = %request.feed_id,
                        expected_position = request.expected_position.value(),
                        ownership_epoch = ?request.ownership_epoch,
                        writer_session = %request.identity.writer_session_id,
                        writer_epoch = request.identity.writer_epoch,
                        writer_sequence = request.identity.sequence,
                        retryable = error.retryable,
                        error = %error.message,
                        "Active Range replica append refused by follower"
                    );
                }
            }
        }
        if !divergent.is_empty() {
            // A surviving replica proved a different committed frame at this
            // position: committing the durable pair would forge committed
            // divergence the cluster can never reconcile. Latch the owner
            // divergent until ownership moves onto the evidence holder, and
            // tell the client to retry across the cutover instead of
            // wedging the Writer sequence on a permanent refusal.
            self.local
                .note_owner_diverged(request.range_id, request.ownership_epoch)
                .await;
            return Err(self.error(
                MajorityAppendErrorCode::NoDurableMajority,
                format!(
                    "replica(s) {} hold divergent committed evidence; ownership resolving",
                    divergent
                        .iter()
                        .map(|node| node.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                true,
                durable,
                vec![],
            ));
        }
        if durable.len() < ACTIVE_RANGE_COMMIT_QUORUM {
            // The owner cannot commit: mark the range so recovery probes
            // report it unhealthy and ownership can move to a replica that
            // can form quorum instead of refusing appends indefinitely.
            self.local.note_owner_quorum_blocked(request.range_id).await;
            return Err(self.error(
                MajorityAppendErrorCode::NoDurableMajority,
                "append is durable on fewer than two current replicas",
                true,
                durable,
                vec![],
            ));
        }
        self.local
            .note_owner_quorum_restored(request.range_id)
            .await;
        if request.pending_effect.is_some() {
            // Deferred commit: the frame is durable on a quorum of replicas
            // but stays fenced behind the committed boundary. The effect
            // journal's terminal marker releases it through
            // commit_pending_effect, and owner loss or abort clears it via
            // the uncommitted-tail truncation path.
            durable.sort();
            return Ok(MajorityAppendResult {
                message_id: local.message_id,
                cursor: local.cursor,
                position: local.position,
                frame_digest: local.frame_digest,
                durable_replicas: durable,
                commit_evidence: vec![],
                deduplicated: local.deduplicated,
            });
        }
        let commit_request = ReplicaCommitRequest {
            feed_id: request.feed_id,
            range_id: request.range_id,
            generation: request.generation,
            ownership_epoch: request.ownership_epoch,
            append_owner: request.append_owner,
            commit_position: CommitPosition::new(local.position.value()),
            frame_digest: local.frame_digest,
        };
        let mut evidence = Vec::new();
        let mut last_commit_error = String::new();
        if durable_followers.len() == 1 {
            match self
                .transport
                .commit(&durable_followers[0], commit_request.clone())
                .await
            {
                Ok(_) => evidence.push(durable_followers[0].clone()),
                Err(error) => {
                    last_commit_error = format!(
                        "; replica {} refused: {}",
                        durable_followers[0], error.message
                    )
                }
            }
        } else {
            let first = self
                .transport
                .commit(&durable_followers[0], commit_request.clone());
            let second = self
                .transport
                .commit(&durable_followers[1], commit_request.clone());
            let (first_result, second_result) = tokio::join!(first, second);
            match first_result {
                Ok(_) => evidence.push(durable_followers[0].clone()),
                Err(error) => {
                    last_commit_error = format!(
                        "; replica {} refused: {}",
                        durable_followers[0], error.message
                    )
                }
            }
            match second_result {
                Ok(_) => evidence.push(durable_followers[1].clone()),
                Err(error) => {
                    last_commit_error = format!(
                        "; replica {} refused: {}",
                        durable_followers[1], error.message
                    )
                }
            }
        }
        if evidence.is_empty() {
            return Err(self.error(
                MajorityAppendErrorCode::NoCommitMajority,
                format!(
                    "frame majority exists but no follower persisted CommitPosition evidence; retry with the same append identity{last_commit_error}"
                ),
                true,
                durable,
                evidence,
            ));
        }
        self.local.commit(commit_request).await.map_err(|error| {
            self.error(
                MajorityAppendErrorCode::LocalStorageFailure,
                error.to_string(),
                // A PositionGap here means the local tail was truncated by
                // a landing ownership move; the frame deduplicates safely on
                // the client's retry wherever it committed.
                error.retryable || error.code == ReplicaAppendErrorCode::PositionGap,
                durable.clone(),
                evidence.clone(),
            )
        })?;
        evidence.push(self.local.local_node().clone());
        durable.sort();
        evidence.sort();
        self.report_commit_proof(
            request.feed_id,
            request.range_id,
            CommitPosition::new(local.position.value()),
            local.frame_digest,
            evidence.clone(),
        );
        Ok(MajorityAppendResult {
            message_id: local.message_id,
            cursor: local.cursor,
            position: local.position,
            frame_digest: local.frame_digest,
            durable_replicas: durable,
            commit_evidence: evidence,
            deduplicated: local.deduplicated,
        })
    }

    /// Ordered contiguous Writer batch under one range lock: the local
    /// batch lands in a single store transaction (one file sync), one
    /// fan-out round ships it to each follower (one remote sync), and
    /// one commit marker releases the whole contiguous prefix — N
    /// records cost one quorum round instead of N serialized rounds.
    /// Per-frame identity, dedup, position, digest, and fencing checks
    /// are identical to `append_inner`: replicas are offered the exact
    /// owner-landed positions, so a retried batch replays through the
    /// same request-derived Cursors and deduped frames never fork.
    pub async fn append_batch(
        &self,
        requests: Vec<ReplicaAppendRequest>,
    ) -> Result<Vec<MajorityAppendResult>, MajorityAppendError> {
        self.append_batch_inner(requests, None).await
    }

    /// Owner-minted batch: sequences are derived per record from the
    /// replica's landed dedup stream under the same lock the batch
    /// appends under, so minted numbers stay contiguous with committed
    /// history exactly like `append_minted`.
    pub async fn append_minted_batch<E>(
        &self,
        requests: Vec<ReplicaAppendRequest>,
        encoders: Vec<E>,
    ) -> Result<Vec<MajorityAppendResult>, MajorityAppendError>
    where
        E: FnOnce(u64) -> Result<bytes::Bytes, crate::codec::CodecError> + Send,
    {
        if encoders.len() != requests.len() {
            return Err(self.error(
                MajorityAppendErrorCode::LocalStorageFailure,
                "minted batch encoders must pair one-to-one with requests",
                false,
                vec![],
                vec![],
            ));
        }
        // Encoders run sequence-0 here, outside the append lock: only
        // the cheap producer_sequence patch needs the minted number, and
        // moving the encode (and its CRC pass) out of the critical
        // section is what lets rounds pipeline.
        let mut frames = Vec::with_capacity(encoders.len());
        for encode_with_sequence in encoders {
            frames.push(
                encode_with_sequence(0)
                    .map(|bytes| bytes.to_vec())
                    .map_err(|error| {
                        self.error(
                            MajorityAppendErrorCode::LocalStorageFailure,
                            format!("owner could not encode the minted record: {error}"),
                            false,
                            vec![],
                            vec![],
                        )
                    })?,
            );
        }
        self.append_batch_inner(requests, Some(frames.into_iter().map(Some).collect()))
            .await
    }

    async fn append_batch_inner(
        &self,
        requests: Vec<ReplicaAppendRequest>,
        pre_encoded: Option<Vec<Option<Vec<u8>>>>,
    ) -> Result<Vec<MajorityAppendResult>, MajorityAppendError> {
        if requests.is_empty() {
            return Ok(vec![]);
        }
        let range_id = requests[0].range_id;
        if requests.iter().any(|request| request.range_id != range_id) {
            return Err(self.error(
                MajorityAppendErrorCode::LocalStorageFailure,
                "batch requests must target one Active Range",
                false,
                vec![],
                vec![],
            ));
        }
        let feed_id = requests[0].feed_id;
        if requests.iter().any(|request| request.feed_id != feed_id) {
            return Err(self.error(
                MajorityAppendErrorCode::LocalStorageFailure,
                "batch requests must target one Feed",
                false,
                vec![],
                vec![],
            ));
        }
        // Group-commit queue first: submissions WAITING for the window
        // are visible to drivers and merge, so the 16-permit window
        // bounds in-flight ROUNDS rather than client batches.
        let _phase_timer = std::time::Instant::now();
        let frames: Vec<Option<Vec<u8>>> = match pre_encoded {
            Some(frames) => frames,
            None => requests.iter().map(|_| None).collect(),
        };
        let (reply_tx, mut reply_rx) = tokio::sync::oneshot::channel();
        let my_id = NEXT_GROUP_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Bounded queue: the permit releases when this submission is
        // drained (dropped inside the driver), so depth is capped at
        // MAX_GROUP_QUEUE regardless of driver pacing.
        let _queue_permit = self
            .group_queue_window(range_id)
            .await
            .acquire_owned()
            .await
            .map_err(|_| {
                self.error(
                    MajorityAppendErrorCode::LocalStorageFailure,
                    "Active Range group queue is closed",
                    true,
                    vec![],
                    vec![],
                )
            })?;
        {
            self.group_queues
                .lock()
                .await
                .entry(range_id)
                .or_default()
                .push_back(GroupSubmission {
                    id: my_id,
                    _queue_permit,
                    requests,
                    frames,
                    reply: reply_tx,
                });
        }
        // Either this submission becomes a driver (permit acquired →
        // drain + round) or an earlier driver already took it (reply).
        let window = self.in_flight_window(range_id).await;
        let _in_flight_permit = tokio::select! {
            permit = window.acquire_owned() => permit.map_err(|_| {
                self.error(
                    MajorityAppendErrorCode::LocalStorageFailure,
                    "Active Range append window is closed",
                    true,
                    vec![],
                    vec![],
                )
            })?,
            reply = &mut reply_rx => {
                return match reply {
                    Ok(outcome) => outcome,
                    Err(_) => Err(self.error(
                        MajorityAppendErrorCode::LocalStorageFailure,
                        "group-commit driver dropped the reply",
                        true,
                        vec![],
                        vec![],
                    )),
                };
            }
        };
        let _append_guard = self.append_lock(range_id).await.lock_owned().await;
        // Drain compatible submissions FIFO — always including this one.
        let (mut subs, mine) = {
            let mut map = self.group_queues.lock().await;
            let queue = map.get_mut(&range_id).expect("queue entry exists");
            let my_pos = queue.iter().position(|sub| sub.id == my_id);
            match my_pos {
                None => (Vec::new(), usize::MAX),
                Some(my_pos) => {
                    let mine_requests: Vec<_> = queue[my_pos].requests.clone();
                    let mut drained = Vec::new();
                    let mut frames_total = 0usize;
                    let mut bytes_total = 0usize;
                    // Compatible-with-ours in queue order (ours may sit
                    // mid-queue); an incompatible earlier submission is
                    // left for its own driver's round.
                    let mut index = 0_usize;
                    while index < queue.len() {
                        let first = &queue[index].requests[0];
                        let compatible = group_compatible(&mine_requests[0], first);
                        let fits = compatible
                            && frames_total + queue[index].requests.len() <= MAX_GROUP_ROUND_FRAMES
                            && bytes_total
                                + queue[index]
                                    .requests
                                    .iter()
                                    .map(|r| r.frame.len() + 64)
                                    .sum::<usize>()
                                <= MAX_GROUP_ROUND_BYTES;
                        if queue[index].id == my_id || fits {
                            frames_total += queue[index].requests.len();
                            bytes_total += queue[index]
                                .requests
                                .iter()
                                .map(|r| r.frame.len() + 64)
                                .sum::<usize>();
                            drained.push(queue.remove(index).expect("queued"));
                        } else {
                            index += 1;
                        }
                    }
                    let mine = drained
                        .iter()
                        .position(|sub| sub.id == my_id)
                        .expect("own submission drained");
                    (drained, mine)
                }
            }
        };
        if subs.is_empty() {
            // An earlier driver already took this submission — the round
            // it lands in reports our slice back over the channel.
            drop(_append_guard);
            return match reply_rx.await {
                Ok(outcome) => outcome,
                Err(_) => Err(self.error(
                    MajorityAppendErrorCode::LocalStorageFailure,
                    "group-commit driver dropped the reply",
                    true,
                    vec![],
                    vec![],
                )),
            };
        }
        // The round runs detached: if the driver's HTTP request is
        // cancelled mid-round, the quorum round must still complete and
        // reply to every merged submission — the caller only awaits its
        // own slice. Guards/permits move into the spawned task with the
        // exact lifetimes they had here.
        let this = self.clone();
        let executor = this.round_executor(range_id).await;
        let job: RoundJob = Box::new(move || {
            Box::pin(async move {
                let merged_submissions = subs.len() > 1;
                // Reassemble the round in queue order; per-request `None` keeps
                // explicit-sequence submissions unpatched.
                let mut merged_requests = Vec::new();
                let mut merged_frames: Vec<Option<Vec<u8>>> = Vec::new();
                // Capture per-submission frame counts before the vecs drain —
                // the split below slices results by them.
                let counts: Vec<usize> = subs.iter().map(|sub| sub.frames.len()).collect();
                for sub in &mut subs {
                    merged_requests.append(&mut sub.requests);
                    merged_frames.append(&mut sub.frames);
                }
                let mut requests = merged_requests;
                let _locked_at = _phase_timer.elapsed();
                let round_result: Result<Vec<MajorityAppendResult>, MajorityAppendError> = async {
        let _locked_at = _phase_timer.elapsed();
        let assignment = this
            .control
            .active_range_assignment_by_id(range_id)
            .await
            .ok_or_else(|| {
                this.error(
                    MajorityAppendErrorCode::AssignmentNotFound,
                    "Active Range assignment is not available",
                    true,
                    vec![],
                    vec![],
                )
            })?;
        let _assign_ms = _phase_timer.elapsed() - _locked_at;
        if assignment.owner != *this.local.local_node()
            || requests.iter().any(|r| r.append_owner != assignment.owner)
        {
            return Err(this.error(
                MajorityAppendErrorCode::NotCurrentOwner,
                format!(
                    "Node {} is not the current Append Owner",
                    this.local.local_node()
                ),
                true,
                vec![],
                vec![],
            ));
        }
        if merged_frames.iter().any(Option::is_some) {
            // Mint the whole sequence run up front: the dedup watermark
            // only advances when frames land, so a batch mints each
            // Writer's base once and assigns the run contiguously.
            // `None` entries (explicit-sequence merged submissions)
            // keep their supplied identity.sequence.
            let mut minted: BTreeMap<(Uuid, u64), u64> = BTreeMap::new();
            for (request, pre_frame) in requests.iter_mut().zip(merged_frames) {
                request.ownership_epoch = assignment.ownership_epoch;
                let Some(mut pre_frame) = pre_frame else {
                    continue;
                };
                let writer_key = (
                    request.identity.writer_session_id,
                    request.identity.writer_epoch,
                );
                request.identity.sequence = match minted.get(&writer_key) {
                    Some(allocated) => *allocated,
                    None => {
                        let next = this
                            .local
                            .next_writer_sequence(
                                &assignment,
                                request.identity.writer_session_id,
                                request.identity.writer_epoch,
                            )
                            .await
                            .map_err(|error| {
                                this.error(
                                    MajorityAppendErrorCode::LocalStorageFailure,
                                    format!("owner could not mint the Writer sequence: {error}"),
                                    true,
                                    vec![],
                                    vec![],
                                )
                            })?;
                        minted.insert(writer_key, next);
                        next
                    }
                };
                minted.insert(writer_key, request.identity.sequence + 1);
                crate::codec::patch_producer_sequence_unchecked(
                    &mut pre_frame,
                    request.identity.sequence,
                )
                .map_err(|error| {
                    this.error(
                        MajorityAppendErrorCode::LocalStorageFailure,
                        format!("owner could not encode the minted record: {error}"),
                        false,
                        vec![],
                        vec![],
                    )
                })?;
                request.frame = pre_frame.into();
            }
        } else {
            for request in requests.iter_mut() {
                request.ownership_epoch = assignment.ownership_epoch;
            }
        }
        // One local batch under one store lock + one file sync. The
        // frames land as unconditional tail appends — a retried prefix
        // dedupes to its stored positions — and the returned positions
        // are the canonical owner positions replicas must match.
        let envelope =
            |frames: Vec<crate::active_range::replication::ReplicaBatchFrame>,
             commit_at: Option<crate::active_range::replication::ReplicaBatchCommit>| {
                crate::active_range::replication::ReplicaBatchAppendRequest {
                    feed_id: requests[0].feed_id,
                    range_id,
                    generation: requests[0].generation,
                    ownership_epoch: requests[0].ownership_epoch,
                    append_owner: requests[0].append_owner.clone(),
                    frames,
                    commit_at,
                }
            };
        let local_frames = requests
            .iter()
            .map(
                |request| crate::active_range::replication::ReplicaBatchFrame {
                    expected_position: None,
                    identity: request.identity.clone(),
                    cursor: request.cursor.clone(),
                    frame: request.frame.clone(),
                    pending_effect: request.pending_effect,
                },
            )
            .collect();
        let _encode_ms = _phase_timer.elapsed() - _locked_at - _assign_ms;
        let _store_mark = _phase_timer.elapsed();
        let local_results = this
            .local
            .append_batch(envelope(local_frames, None))
            .await
            .map_err(|error| {
                if error.code == ReplicaAppendErrorCode::PendingEffectOccupied {
                    return this.occupied_error(error.pending_effect.map(|boxed| *boxed));
                }
                this.error(
                    MajorityAppendErrorCode::LocalStorageFailure,
                    error.to_string(),
                    error.retryable,
                    vec![],
                    vec![],
                )
            })?;
        if local_results.len() != requests.len() {
            return Err(this.error(
                MajorityAppendErrorCode::LocalStorageFailure,
                "local batch append returned a truncated result set",
                false,
                vec![],
                vec![],
            ));
        }
        // Replicas are offered the owner-landed positions, mirroring N
        // single appends. A Cursor-deduped frame whose stored bytes
        // differ from the request's (a re-sequenced retry) must be
        // offered as the stored occupant — replicas would otherwise
        // refuse a forked frame at that position.
        let mut follower_frames: Vec<crate::active_range::replication::ReplicaBatchFrame> =
            requests
                .iter()
                .zip(local_results.iter())
                .map(
                    |(request, result)| crate::active_range::replication::ReplicaBatchFrame {
                        expected_position: Some(result.position),
                        identity: request.identity.clone(),
                        cursor: request.cursor.clone(),
                        frame: request.frame.clone(),
                        pending_effect: request.pending_effect,
                    },
                )
                .collect();
        for (index, result) in local_results.iter().enumerate() {
            if !result.deduplicated {
                continue;
            }
            let request_digest = *blake3::hash(&follower_frames[index].frame).as_bytes();
            if request_digest == result.frame_digest {
                continue;
            }
            let after = result
                .position
                .value()
                .checked_sub(1)
                .map(RangePosition::new);
            let frames = this
                .local
                .read_assignment_uncommitted(&assignment, after, 1)
                .await
                .map_err(|error| {
                    this.error(
                        MajorityAppendErrorCode::LocalStorageFailure,
                        format!("owner could not reload the deduplicated frame: {error}"),
                        true,
                        vec![],
                        vec![],
                    )
                })?;
            if let Some(frame) = frames.into_iter().next() {
                if frame.position == result.position {
                    follower_frames[index].frame = frame.frame.clone();
                    follower_frames[index].identity = frame.identity;
                    follower_frames[index].cursor = frame.cursor;
                }
            }
        }
        let followers = assignment
            .replicas
            .iter()
            .filter(|node| *node != this.local.local_node())
            .cloned()
            .collect::<Vec<_>>();
        // Fused commit: plain batches pin the commit marker to the run's
        // tail so each replica lands frames + watermark in one durability
        // unit — the same CommitPosition the two-round protocol writes,
        // one network round earlier. Pending-effect runs stay fenced
        // behind their effect journal and keep the deferred path.
        let has_pending = follower_frames
            .iter()
            .any(|frame| frame.pending_effect.is_some());
        let commit_hint = if has_pending {
            None
        } else {
            let tail = &local_results[local_results.len() - 1];
            Some(crate::active_range::replication::ReplicaBatchCommit {
                position: CommitPosition::new(tail.position.value()),
                frame_digest: tail.frame_digest,
            })
        };
        // Pipeline point: mint order is fixed — sequences, owner
        // positions, and the fused commit hint are all decided. Sends to
        // each follower and the local commit run through ordered turn
        // queues minted here, so releasing the append lock lets the next
        // round's local work overlap this round's network flight without
        // ever letting a later batch reach a replica first.
        let send_turn_a = this.send_turn(range_id, &followers[0]).await;
        let send_turn_b = this.send_turn(range_id, &followers[1]).await;
        let send_permit_a = send_turn_a.hold();
        let send_permit_b = send_turn_b.hold();
        let commit_permit = this.commit_turn(range_id).await.hold();
        let last_position = local_results[local_results.len() - 1].position;
        let _store_ms = _phase_timer.elapsed() - _store_mark;
        let _local_ms = _phase_timer.elapsed() - _locked_at;
        drop(_append_guard);
        // Timing accumulators: [send_wait_ms, send_ms] max per follower.
        let timing = std::sync::Mutex::new((0.0_f64, 0.0_f64));
        let mut durable = vec![this.local.local_node().clone()];
        // Windowed sends: the OrderedTurn still mints and admits sends
        // strictly in order, but a ticket is released as soon as its
        // request is on the wire — up to MAX_INFLIGHT_SENDS_PER_FOLLOWER
        // sends per lane overlap their round-trips while the replica's
        // ordered admission lands them in position order. The lane
        // permit is held across the response AND any gap repair, so a
        // lane never exceeds the window even mid-repair.
        let lane_a = this.send_lane(range_id, &followers[0]).await;
        let lane_b = this.send_lane(range_id, &followers[1]).await;
        let repair_a = this.repair_lock(range_id, &followers[0]).await;
        let repair_b = this.repair_lock(range_id, &followers[1]).await;
        let send_transport = this.transport.clone();
        let send_to =
            |node: StorageNodeId,
             turn_permit: TurnPermit,
             lane: Arc<Semaphore>,
             repair: Arc<Mutex<()>>,
             request: crate::active_range::replication::ReplicaBatchAppendRequest| {
                let timing = &timing;
                let requests = &requests;
                let assignment = &assignment;
                let send_transport = &send_transport;
                let this = &this;
                async move {
                    let t = std::time::Instant::now();
                    turn_permit.wait().await;
                    // Pending-effect rounds fence the whole lane — they must
                    // never overlap another send to this replica.
                    let lane_permit = if has_pending {
                        lane.clone()
                            .acquire_many_owned(MAX_INFLIGHT_SENDS_PER_FOLLOWER as u32)
                            .await
                    } else {
                        lane.clone().acquire_owned().await
                    };
                    let _lane_permit = match lane_permit {
                        Ok(permit) => permit,
                        Err(_) => {
                            return Err(ReplicaTransportError::transport(
                                "replica send lane is closed",
                                true,
                            ))
                        }
                    };
                    let waited = t.elapsed().as_secs_f64() * 1000.0;
                    let t = std::time::Instant::now();
                    let send = tokio::spawn({
                        let transport = send_transport.clone();
                        let node = node.clone();
                        let request = request.clone();
                        async move { transport.append_batch(&node, request).await }
                    });
                    // The ticket's only job was ordering send starts; release
                    // it now this send is on the wire so the next round may
                    // start its own while this response is in flight.
                    drop(turn_permit);
                    let result = match send.await {
                        Ok(result) => result,
                        Err(join_error) => Err(ReplicaTransportError::transport(
                            format!("replica send task failed: {join_error}"),
                            true,
                        )),
                    };
                    let result = match result {
                        Err(error) if error.code == Some(ReplicaAppendErrorCode::PositionGap) => {
                            // Overlapping rounds that each miss positions
                            // serialize on the lane's repair lock; re-driven
                            // frames dedup idempotently on the replica.
                            let _repair = repair.lock().await;
                            match this
                                .fill_replica_gap(
                                    assignment,
                                    &requests[0],
                                    &node,
                                    error.expected_position,
                                    last_position,
                                )
                                .await
                            {
                                Ok(()) => send_transport.append_batch(&node, request).await,
                                Err(fill_error) => Err(fill_error),
                            }
                        }
                        other => other,
                    };
                    let sent = t.elapsed().as_secs_f64() * 1000.0;
                    let mut guard = timing.lock().unwrap();
                    guard.0 = guard.0.max(waited);
                    guard.1 = guard.1.max(sent);
                    result
                }
            };
        let send_a = send_to(
            followers[0].clone(),
            send_permit_a,
            lane_a,
            repair_a,
            envelope(follower_frames.clone(), commit_hint),
        );
        let send_b = send_to(
            followers[1].clone(),
            send_permit_b,
            lane_b,
            repair_b,
            envelope(follower_frames.clone(), commit_hint),
        );
        let (first_result, second_result) = tokio::join!(send_a, send_b);
        let _sends_done = _phase_timer.elapsed();
        let follower_results = [
            (followers[0].clone(), first_result),
            (followers[1].clone(), second_result),
        ];
        let mut durable_followers = Vec::new();
        let mut fused_commit: Vec<StorageNodeId> = Vec::new();
        let mut divergent: Vec<StorageNodeId> = Vec::new();
        for (node, result) in follower_results {
            let accepted = match result {
                Ok(accepted) => accepted,
                Err(error) => {
                    if error.code == Some(ReplicaAppendErrorCode::CommittedPositionConflict) {
                        divergent.push(node.clone());
                    }
                    tracing::warn!(
                        replica = %node,
                        feed_id = %requests[0].feed_id,
                        range_id = %range_id,
                        retryable = error.retryable,
                        error = %error.message,
                        "Active Range replica batch append refused by follower"
                    );
                    continue;
                }
            };
            if accepted.len() != local_results.len() {
                return Err(this.error(
                    MajorityAppendErrorCode::FrameConflict,
                    format!("replica {node} acknowledged a truncated batch"),
                    false,
                    durable,
                    vec![],
                ));
            }
            // Committed-divergent evidence vetoes the batch exactly like
            // the single-append fork proof; a bare position/digest
            // mismatch stays a fatal contradiction.
            let mut mismatch = false;
            for (accepted_frame, local_frame) in accepted.iter().zip(local_results.iter()) {
                if accepted_frame.position != local_frame.position && accepted_frame.committed {
                    divergent.push(node.clone());
                    mismatch = true;
                    break;
                }
                if accepted_frame.position != local_frame.position
                    || accepted_frame.frame_digest != local_frame.frame_digest
                {
                    return Err(this.error(
                        MajorityAppendErrorCode::FrameConflict,
                        format!("replica {node} acknowledged different bytes or position"),
                        false,
                        durable,
                        vec![],
                    ));
                }
            }
            if mismatch {
                continue;
            }
            durable.push(node.clone());
            if accepted.last().is_some_and(|frame| frame.committed) {
                // The fused marker landed with the frames — this replica
                // already holds CommitPosition evidence and needs no
                // commit round.
                fused_commit.push(node.clone());
            }
            durable_followers.push(node);
        }
        if !divergent.is_empty() {
            this.local
                .note_owner_diverged(range_id, requests[0].ownership_epoch)
                .await;
            return Err(this.error(
                MajorityAppendErrorCode::NoDurableMajority,
                format!(
                    "replica(s) {} hold divergent committed evidence; ownership resolving",
                    divergent
                        .iter()
                        .map(|node| node.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                true,
                durable,
                vec![],
            ));
        }
        if durable.len() < ACTIVE_RANGE_COMMIT_QUORUM {
            this.local.note_owner_quorum_blocked(range_id).await;
            return Err(this.error(
                MajorityAppendErrorCode::NoDurableMajority,
                "batch append is durable on fewer than two current replicas",
                true,
                durable,
                vec![],
            ));
        }
        this.local.note_owner_quorum_restored(range_id).await;
        if has_pending {
            // Deferred commit: pending-effect frames stay fenced behind
            // their effect marker exactly like the single-append path —
            // the effect journal's terminal marker releases the tail.
            durable.sort();
            return Ok(local_results
                .into_iter()
                .map(|local| MajorityAppendResult {
                    message_id: local.message_id,
                    cursor: local.cursor,
                    position: local.position,
                    frame_digest: local.frame_digest,
                    durable_replicas: durable.clone(),
                    commit_evidence: vec![],
                    deduplicated: local.deduplicated,
                })
                .collect());
        }
        // Commit phase runs in mint order: the quorum is established, so
        // the watermark may advance — but only after every earlier round's
        // commit turn, keeping the local watermark and per-follower commit
        // order contiguous.
        let _commit_wait_at = _phase_timer.elapsed();
        commit_permit.wait().await;
        let _commit_waited = _phase_timer.elapsed() - _commit_wait_at;
        let commit_request = ReplicaCommitRequest {
            feed_id: requests[0].feed_id,
            range_id,
            generation: requests[0].generation,
            ownership_epoch: requests[0].ownership_epoch,
            append_owner: requests[0].append_owner.clone(),
            commit_position: CommitPosition::new(last_position.value()),
            frame_digest: local_results[local_results.len() - 1].frame_digest,
        };
        // Replicas that fused the commit already hold evidence; only a
        // durable follower that landed frames without the marker gets a
        // commit round (e.g. a gap-repaired tail).
        let mut evidence: Vec<StorageNodeId> = fused_commit;
        let mut last_commit_error = String::new();
        let unfused: Vec<StorageNodeId> = durable_followers
            .iter()
            .filter(|node| !evidence.contains(node))
            .cloned()
            .collect();
        if unfused.len() == 1 {
            match this
                .transport
                .commit(&unfused[0], commit_request.clone())
                .await
            {
                Ok(_) => evidence.push(unfused[0].clone()),
                Err(error) => {
                    last_commit_error =
                        format!("; replica {} refused: {}", unfused[0], error.message)
                }
            }
        } else if unfused.len() == 2 {
            let first_commit = this.transport.commit(&unfused[0], commit_request.clone());
            let second_commit = this.transport.commit(&unfused[1], commit_request.clone());
            let (first_result, second_result) = tokio::join!(first_commit, second_commit);
            match first_result {
                Ok(_) => evidence.push(unfused[0].clone()),
                Err(error) => {
                    last_commit_error =
                        format!("; replica {} refused: {}", unfused[0], error.message)
                }
            }
            match second_result {
                Ok(_) => evidence.push(unfused[1].clone()),
                Err(error) => {
                    last_commit_error =
                        format!("; replica {} refused: {}", unfused[1], error.message)
                }
            }
        }
        if evidence.is_empty() {
            return Err(this.error(
                MajorityAppendErrorCode::NoCommitMajority,
                format!(
                    "batch majority exists but no follower persisted CommitPosition evidence; retry with the same append identities{last_commit_error}"
                ),
                true,
                durable,
                evidence,
            ));
        }
        this.local.commit(commit_request).await.map_err(|error| {
            this.error(
                MajorityAppendErrorCode::LocalStorageFailure,
                error.to_string(),
                // A PositionGap here means the local tail was truncated by
                // a landing ownership move; the frames deduplicate safely
                // on the client's retry wherever they committed.
                error.retryable || error.code == ReplicaAppendErrorCode::PositionGap,
                durable.clone(),
                evidence.clone(),
            )
        })?;
        {
            let total = _phase_timer.elapsed();
            let (wait_ms, send_ms) = *timing.lock().unwrap();
            tracing::debug!(
                frames = requests.len(),
                lock_wait_ms = _locked_at.as_secs_f64() * 1000.0,
                mint_local_ms = _local_ms.as_secs_f64() * 1000.0,
                mint_assign_ms = _assign_ms.as_secs_f64() * 1000.0,
                mint_encode_ms = _encode_ms.as_secs_f64() * 1000.0,
                mint_store_ms = _store_ms.as_secs_f64() * 1000.0,
                send_wait_ms = wait_ms,
                send_ms = send_ms,
                sends_total_ms = (_sends_done - _locked_at - _local_ms).as_secs_f64() * 1000.0,
                commit_wait_ms = _commit_waited.as_secs_f64() * 1000.0,
                commit_ms = (total - _sends_done - _commit_waited).as_secs_f64() * 1000.0,
                total_ms = total.as_secs_f64() * 1000.0,
                "append batch phase timing"
            );
        }
        evidence.push(this.local.local_node().clone());
        durable.sort();
        evidence.sort();
        this.report_commit_proof(
            requests[0].feed_id,
            range_id,
            CommitPosition::new(last_position.value()),
            local_results[local_results.len() - 1].frame_digest,
            evidence.clone(),
        );
        Ok(local_results
            .into_iter()
            .map(|local| MajorityAppendResult {
                message_id: local.message_id,
                cursor: local.cursor,
                position: local.position,
                frame_digest: local.frame_digest,
                durable_replicas: durable.clone(),
                commit_evidence: evidence.clone(),
                deduplicated: local.deduplicated,
            })
            .collect())
        }
        .await;
                // Split the merged round back per submission in queue order.
                let groups = subs.len();
                let mine_index = mine;
                match round_result {
                    Ok(results) => {
                        let mut offset = 0_usize;
                        let mut mine = None;
                        for (index, sub) in subs.into_iter().enumerate() {
                            let end = offset + counts[index];
                            let slice = results[offset..end].to_vec();
                            offset = end;
                            if index == mine_index {
                                mine = Some(slice);
                            } else {
                                let _ = sub.reply.send(Ok(slice));
                            }
                        }
                        tracing::debug!(groups, "append batch group round");
                        Ok(mine.expect("own slice"))
                    }
                    Err(error) => {
                        // Poison isolation: a merged round's per-frame refusal
                        // may not apply to every submission — re-run each solo
                        // so one bad batch cannot take innocent ones down.
                        // Non-retryable refusals may be per-frame: solo re-runs
                        // isolate the bad submission from the innocent ones.
                        let poisonous = !error.retryable;
                        if merged_submissions && poisonous {
                            let mut mine = None;
                            for (index, sub) in subs.into_iter().enumerate() {
                                // Solo re-run lands the pre-encoded frames as
                                // explicit requests — sequences were already
                                // minted into request.identity by the failed
                                // round, and dedup replays any landed prefix.
                                let mut sub = sub;
                                for (request, frame) in
                                    sub.requests.iter_mut().zip(sub.frames.iter_mut())
                                {
                                    if let Some(frame) = frame.take() {
                                        request.frame = frame.into();
                                    }
                                }
                                let outcome = this.append_batch(sub.requests).await;
                                if index == mine_index {
                                    mine = Some(outcome);
                                } else {
                                    let _ = sub.reply.send(outcome);
                                }
                            }
                            mine.expect("own outcome")
                        } else {
                            for (index, sub) in subs.into_iter().enumerate() {
                                let cloned = error.clone();
                                if index == mine_index {
                                    continue;
                                }
                                let _ = sub.reply.send(Err(cloned));
                            }
                            Err(error)
                        }
                    }
                }
            })
        });
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        if executor.send((job, done_tx)).is_err() {
            return Err(self.error(
                MajorityAppendErrorCode::LocalStorageFailure,
                "group-commit executor is closed",
                true,
                vec![],
                vec![],
            ));
        }
        match done_rx.await {
            Ok(outcome) => outcome,
            Err(_) => Err(self.error(
                MajorityAppendErrorCode::LocalStorageFailure,
                "group-commit round task failed",
                true,
                vec![],
                vec![],
            )),
        }
    }

    /// Re-drive the positions a lagging replica never stored so the caller
    /// can retry the current frame. A replica that missed an earlier push
    /// refuses every later position as a gap; replaying the owner's
    /// appended log into it restores quorum inside the request instead of
    /// waiting on background repair.
    async fn fill_replica_gap(
        &self,
        assignment: &super::ActiveRangeAssignment,
        request: &ReplicaAppendRequest,
        replica: &StorageNodeId,
        replica_next: Option<u64>,
        upto: RangePosition,
    ) -> Result<(), ReplicaTransportError> {
        let Some(next) = replica_next else {
            return Err(ReplicaTransportError::transport(
                "replica reported a position gap without its next expected position",
                true,
            ));
        };
        let next = RangePosition::new(next);
        if next.value() >= upto.value() {
            return Ok(());
        }
        let after = next.value().checked_sub(1).map(RangePosition::new);
        let frames = self
            .local
            .read_assignment_uncommitted(assignment, after, (upto.value() - next.value()) as usize)
            .await
            .map_err(|error| {
                ReplicaTransportError::transport(
                    format!(
                        "owner could not re-drive missing positions: {}",
                        error.message
                    ),
                    true,
                )
            })?;
        let frames: Vec<_> = frames
            .into_iter()
            .take_while(|frame| frame.position.value() < upto.value())
            .collect();
        // Re-drive the gap with batch sends chunked by the same bounds a
        // merged round obeys — one HTTP call per ~4096 frames instead of
        // one per frame. No commit_at on repair chunks, mirroring the
        // per-frame path which carries no commit evidence.
        let mut chunk = Vec::new();
        let mut chunk_bytes = 0_usize;
        for frame in &frames {
            let bytes = frame.frame.len() + 64;
            if !chunk.is_empty()
                && (chunk.len() >= super::replication::MAX_REPLICA_BATCH_FRAMES
                    || chunk_bytes + bytes > MAX_GROUP_ROUND_BYTES)
            {
                self.fill_replica_chunk(assignment, request, replica, std::mem::take(&mut chunk))
                    .await?;
                chunk_bytes = 0;
            }
            chunk_bytes += bytes;
            chunk.push(frame.clone());
        }
        if !chunk.is_empty() {
            self.fill_replica_chunk(assignment, request, replica, chunk)
                .await?;
        }
        Ok(())
    }

    /// Send one repair chunk as a single `append_batch` round-trip and
    /// verify every accepted position lands where the owner stored it.
    async fn fill_replica_chunk(
        &self,
        _assignment: &super::ActiveRangeAssignment,
        request: &ReplicaAppendRequest,
        replica: &StorageNodeId,
        chunk: Vec<super::store::StoredRangeFrame>,
    ) -> Result<(), ReplicaTransportError> {
        let expected: Vec<RangePosition> = chunk.iter().map(|f| f.position).collect();
        let accepted = self
            .transport
            .append_batch(
                replica,
                crate::active_range::replication::ReplicaBatchAppendRequest {
                    feed_id: request.feed_id,
                    range_id: request.range_id,
                    generation: request.generation,
                    ownership_epoch: request.ownership_epoch,
                    append_owner: request.append_owner.clone(),
                    frames: chunk
                        .iter()
                        .map(
                            |frame| crate::active_range::replication::ReplicaBatchFrame {
                                expected_position: Some(frame.position),
                                identity: frame.identity.clone(),
                                cursor: frame.cursor.clone(),
                                frame: frame.frame.clone(),
                                pending_effect: None,
                            },
                        )
                        .collect(),
                    commit_at: None,
                },
            )
            .await?;
        if accepted.len() != expected.len() {
            return Err(ReplicaTransportError::transport(
                format!(
                    "replica {replica} accepted {} re-driven frames for {} supplied",
                    accepted.len(),
                    expected.len(),
                ),
                true,
            ));
        }
        for (accepted, expected) in accepted.iter().zip(expected.iter()) {
            if accepted.position != *expected {
                return Err(ReplicaTransportError::transport(
                    format!(
                        "replica {replica} landed re-driven frame at RangePosition {}",
                        accepted.position
                    ),
                    true,
                ));
            }
        }
        Ok(())
    }

    /// The pending marker currently holding a range's uncommitted tail.
    pub async fn pending_effect(
        &self,
        assignment: &super::ActiveRangeAssignment,
    ) -> Result<Option<super::store::PendingEffect>, MajorityAppendError> {
        self.local
            .pending_effect(assignment)
            .await
            .map_err(|error| {
                self.error(
                    MajorityAppendErrorCode::LocalStorageFailure,
                    error.to_string(),
                    error.retryable,
                    vec![],
                    vec![],
                )
            })
    }

    /// Release one effect's pending tail: broadcast the deferred commit
    /// position (the marker's last frame) to the replica quorum and commit it
    /// locally once a follower persists matching evidence. `NotPending` means
    /// the marker already resolved (released or truncated); `Occupied` means a
    /// different effect holds the tail and must be swept first.
    pub async fn commit_pending_effect(
        &self,
        request: PendingReleaseRequest,
    ) -> Result<PendingReleaseOutcome, MajorityAppendError> {
        let _append_guard = self.append_lock(request.range_id).await.lock_owned().await;
        let assignment = self
            .control
            .active_range_assignment_by_id(request.range_id)
            .await
            .ok_or_else(|| {
                self.error(
                    MajorityAppendErrorCode::AssignmentNotFound,
                    "Active Range assignment is not available",
                    true,
                    vec![],
                    vec![],
                )
            })?;
        if assignment.feed_id != request.feed_id
            || assignment.generation != request.generation
            || assignment.ownership_epoch != request.ownership_epoch
            || assignment.owner != *self.local.local_node()
            || request.append_owner != assignment.owner
        {
            return Err(self.error(
                MajorityAppendErrorCode::NotCurrentOwner,
                format!(
                    "Node {} is not the current Append Owner for this release",
                    self.local.local_node()
                ),
                false,
                vec![],
                vec![],
            ));
        }
        let committed = self
            .local
            .recovery_status_for_assignment(&assignment)
            .await
            .map(|status| status.committed)
            .map_err(|error| {
                self.error(
                    MajorityAppendErrorCode::LocalStorageFailure,
                    error.to_string(),
                    error.retryable,
                    vec![],
                    vec![],
                )
            })?;
        let marker = self.pending_effect(&assignment).await?;
        let Some(marker) = marker else {
            return Ok(PendingReleaseOutcome::NotPending {
                committed: committed.value(),
            });
        };
        if marker.effect_id != request.effect_id
            || marker.subscription_id != request.subscription_id
        {
            return Ok(PendingReleaseOutcome::Occupied { occupant: marker });
        }
        if marker.last_position <= committed.value() {
            // Defensive: a marker at or behind the committed boundary is
            // residue; the records are already released.
            return Ok(PendingReleaseOutcome::Released {
                commit_position: committed,
            });
        }
        let commit_request = ReplicaCommitRequest {
            feed_id: request.feed_id,
            range_id: request.range_id,
            generation: request.generation,
            ownership_epoch: request.ownership_epoch,
            append_owner: assignment.owner.clone(),
            commit_position: super::CommitPosition::new(marker.last_position),
            frame_digest: marker.last_frame_digest,
        };
        let followers = assignment
            .replicas
            .iter()
            .filter(|node| *node != self.local.local_node())
            .cloned()
            .collect::<Vec<_>>();
        let mut evidence = Vec::new();
        let first = self.transport.commit(&followers[0], commit_request.clone());
        let second = self.transport.commit(&followers[1], commit_request.clone());
        let (first_result, second_result) = tokio::join!(first, second);
        if first_result.is_ok() {
            evidence.push(followers[0].clone());
        }
        if second_result.is_ok() {
            evidence.push(followers[1].clone());
        }
        if evidence.is_empty() {
            return Err(self.error(
                MajorityAppendErrorCode::NoCommitMajority,
                "pending effect release reached no follower; retry with the same effect identity",
                true,
                vec![],
                vec![],
            ));
        }
        let released = commit_request.commit_position;
        let released_digest = commit_request.frame_digest;
        self.local.commit(commit_request).await.map_err(|error| {
            self.error(
                MajorityAppendErrorCode::LocalStorageFailure,
                error.to_string(),
                error.retryable,
                vec![],
                evidence.clone(),
            )
        })?;
        evidence.push(self.local.local_node().clone());
        evidence.sort();
        self.report_commit_proof(
            request.feed_id,
            request.range_id,
            released,
            released_digest,
            evidence.clone(),
        );
        Ok(PendingReleaseOutcome::Released {
            commit_position: released,
        })
    }

    /// Discard one effect's pending tail on this Range: the local replica
    /// truncates the marker's uncommitted frames and the discard then
    /// broadcasts to both followers best-effort — including when the local
    /// marker already resolved, so a follower that missed the first
    /// discard still heals when the request reaches it. `Occupied`
    /// reports the local marker's foreign owner and discards nothing
    /// locally.
    pub async fn discard_pending_effect(
        &self,
        request: PendingReleaseRequest,
    ) -> Result<super::store::PendingDiscardOutcome, MajorityAppendError> {
        let _append_guard = self.append_lock(request.range_id).await.lock_owned().await;
        let assignment = self
            .control
            .active_range_assignment_by_id(request.range_id)
            .await
            .ok_or_else(|| {
                self.error(
                    MajorityAppendErrorCode::AssignmentNotFound,
                    "Active Range assignment is not available",
                    true,
                    vec![],
                    vec![],
                )
            })?;
        if assignment.feed_id != request.feed_id
            || assignment.generation != request.generation
            || assignment.ownership_epoch != request.ownership_epoch
            || assignment.owner != *self.local.local_node()
            || request.append_owner != assignment.owner
        {
            return Err(self.error(
                MajorityAppendErrorCode::NotCurrentOwner,
                format!(
                    "Node {} is not the current Append Owner for this discard",
                    self.local.local_node()
                ),
                false,
                vec![],
                vec![],
            ));
        }
        let discard = ReplicaDiscardPendingRequest {
            feed_id: request.feed_id,
            range_id: request.range_id,
            generation: request.generation,
            ownership_epoch: request.ownership_epoch,
            append_owner: assignment.owner.clone(),
            effect_id: request.effect_id,
            subscription_id: request.subscription_id,
        };
        let local_reply = self
            .local
            .discard_pending(discard.clone())
            .await
            .map_err(|error| {
                self.error(
                    MajorityAppendErrorCode::LocalStorageFailure,
                    error.to_string(),
                    error.retryable,
                    vec![],
                    vec![],
                )
            })?;
        let followers = assignment
            .replicas
            .iter()
            .filter(|node| *node != self.local.local_node())
            .cloned()
            .collect::<Vec<_>>();
        if followers.len() >= 2 {
            // Best-effort: an unreachable follower keeps its copy of the
            // uncommitted tail until the discard is re-driven (abort
            // replay or occupant sweep) or owner recovery truncates it.
            let first = self
                .transport
                .discard_pending(&followers[0], discard.clone());
            let second = self.transport.discard_pending(&followers[1], discard);
            let _ = tokio::join!(first, second);
        }
        let outcome = match (local_reply.outcome, local_reply.occupant) {
            (Some(super::replication::ReplicaDiscardOutcome::NotPending), _) => {
                super::store::PendingDiscardOutcome::NotPending
            }
            (Some(super::replication::ReplicaDiscardOutcome::Occupied), Some(marker)) => {
                super::store::PendingDiscardOutcome::Occupied(marker)
            }
            (Some(super::replication::ReplicaDiscardOutcome::Discarded { dropped_frames }), _) => {
                super::store::PendingDiscardOutcome::Discarded { dropped_frames }
            }
            _ => {
                return Err(self.error(
                    MajorityAppendErrorCode::LocalStorageFailure,
                    "local replica returned no discard outcome",
                    true,
                    vec![],
                    vec![],
                ))
            }
        };
        Ok(outcome)
    }

    fn occupied_error(&self, occupant: Option<super::store::PendingEffect>) -> MajorityAppendError {
        let mut error = self.error(
            MajorityAppendErrorCode::PendingEffectOccupied,
            "Active Range tail is held by another pending effect; resolve it before appending",
            true,
            vec![],
            vec![],
        );
        error.pending_effect = occupant;
        error
    }

    fn error(
        &self,
        code: MajorityAppendErrorCode,
        message: impl Into<String>,
        retryable: bool,
        durable_replicas: Vec<StorageNodeId>,
        commit_evidence: Vec<StorageNodeId>,
    ) -> MajorityAppendError {
        MajorityAppendError {
            code,
            message: message.into(),
            retryable,
            durable_replicas,
            commit_evidence,
            pending_effect: None,
        }
    }
}

/// Owner-side request to release (commit) one effect's pending tail.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PendingReleaseRequest {
    pub feed_id: Uuid,
    pub range_id: super::RangeId,
    pub generation: super::RangeGeneration,
    pub ownership_epoch: super::OwnershipEpoch,
    pub append_owner: StorageNodeId,
    pub effect_id: Uuid,
    pub subscription_id: Uuid,
}

/// The result of a pending release attempt.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PendingReleaseOutcome {
    /// The marker's last position committed on the owner and a follower.
    Released {
        commit_position: super::CommitPosition,
    },
    /// No marker holds the tail; `committed` is the current boundary so the
    /// caller can tell released-apart from truncated.
    NotPending { committed: u64 },
    /// A different effect holds the tail; resolve the occupant first.
    Occupied {
        occupant: super::store::PendingEffect,
    },
}

#[derive(Clone)]
pub struct HttpReplicaTransport {
    endpoints: Arc<crate::internal_plane::InternalEndpoints>,
    key: String,
    http: reqwest::Client,
}

impl HttpReplicaTransport {
    pub fn new(
        endpoints: impl Into<crate::internal_plane::InternalEndpoints>,
        key: String,
        timeout: Duration,
    ) -> Result<Self, ReplicaTransportError> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|error| ReplicaTransportError::transport(error.to_string(), true))?;
        Self::with_client(endpoints, key, http)
    }

    pub fn with_client(
        endpoints: impl Into<crate::internal_plane::InternalEndpoints>,
        key: String,
        http: reqwest::Client,
    ) -> Result<Self, ReplicaTransportError> {
        Ok(Self {
            endpoints: Arc::new(endpoints.into()),
            key,
            http,
        })
    }

    async fn post<Request, Response>(
        &self,
        replica: &StorageNodeId,
        path: &str,
        request: &Request,
    ) -> Result<Response, ReplicaTransportError>
    where
        Request: Serialize + Sync,
        Response: for<'de> Deserialize<'de>,
    {
        let endpoint = self.endpoints.resolve(replica).await.ok_or_else(|| {
            ReplicaTransportError::transport(
                format!("replica {replica} has no configured endpoint"),
                false,
            )
        })?;
        let response = self
            .http
            .post(format!("{}{}", endpoint.trim_end_matches('/'), path))
            .header("x-whitewater-control-key", &self.key)
            .json(request)
            .send()
            .await
            .map_err(|error| ReplicaTransportError::transport(error.to_string(), true))?;
        if !response.status().is_success() {
            return Err(ReplicaTransportError::transport(
                format!("replica {replica} returned HTTP {}", response.status()),
                response.status().is_server_error(),
            ));
        }
        response
            .json()
            .await
            .map_err(|error| ReplicaTransportError::transport(error.to_string(), true))
    }

    async fn post_batch_bytes(
        &self,
        replica: &StorageNodeId,
        request: crate::active_range::replication::ReplicaBatchAppendRequest,
    ) -> Result<crate::active_range::replication::ReplicaBatchAppendResponse, ReplicaTransportError>
    {
        use crate::active_range::replication::{
            encode_batch_body, ReplicaBatchAppendHeader, ReplicaBatchFrameMeta,
        };
        let mut frames = Vec::with_capacity(request.frames.len());
        for frame in &request.frames {
            frames.push(frame.frame.to_vec());
        }
        let header = ReplicaBatchAppendHeader {
            feed_id: request.feed_id,
            range_id: request.range_id,
            generation: request.generation,
            ownership_epoch: request.ownership_epoch,
            append_owner: request.append_owner,
            frames: request
                .frames
                .iter()
                .map(|frame| ReplicaBatchFrameMeta {
                    expected_position: frame.expected_position,
                    identity: frame.identity.clone(),
                    cursor: frame.cursor.clone(),
                    pending_effect: frame.pending_effect,
                })
                .collect(),
            commit_at: request.commit_at,
        };
        let body = encode_batch_body(&header, &frames)
            .map_err(|error| ReplicaTransportError::transport(error.to_string(), false))?;
        let endpoint = self.endpoints.resolve(replica).await.ok_or_else(|| {
            ReplicaTransportError::transport(
                format!("replica {replica} has no configured endpoint"),
                false,
            )
        })?;
        let response = self
            .http
            .post(format!(
                "{}/internal/active-range/replica/append-batch-bin",
                endpoint.trim_end_matches('/')
            ))
            .header("x-whitewater-control-key", &self.key)
            .header("content-type", "application/octet-stream")
            .body(body)
            .send()
            .await
            .map_err(|error| ReplicaTransportError::transport(error.to_string(), true))?;
        if !response.status().is_success() {
            return Err(ReplicaTransportError::transport(
                format!("replica {replica} returned HTTP {}", response.status()),
                response.status().is_server_error(),
            ));
        }
        let body = response
            .bytes()
            .await
            .map_err(|error| ReplicaTransportError::transport(error.to_string(), true))?;
        crate::active_range::replication::decode_batch_result_body(&body)
            .map_err(ReplicaTransportError::replica)
    }
}

#[async_trait]
impl ReplicaTransport for HttpReplicaTransport {
    async fn append(
        &self,
        replica: &StorageNodeId,
        request: ReplicaAppendRequest,
    ) -> Result<ReplicaAppendAccepted, ReplicaTransportError> {
        let response: ReplicaAppendResponse = self
            .post(replica, "/internal/active-range/replica/append", &request)
            .await?;
        match (response.result, response.error) {
            (Some(result), _) => Ok(result),
            (None, Some(error)) => Err(ReplicaTransportError::replica(error)),
            (None, None) => Err(ReplicaTransportError::transport(
                "replica returned no append result",
                true,
            )),
        }
    }

    async fn append_batch(
        &self,
        replica: &StorageNodeId,
        request: crate::active_range::replication::ReplicaBatchAppendRequest,
    ) -> Result<Vec<ReplicaAppendAccepted>, ReplicaTransportError> {
        let response: crate::active_range::replication::ReplicaBatchAppendResponse =
            self.post_batch_bytes(replica, request).await?;
        match (response.results, response.error) {
            (Some(results), _) => Ok(results),
            (None, Some(error)) => Err(ReplicaTransportError::replica(error)),
            (None, None) => Err(ReplicaTransportError::transport(
                "replica returned no batch append results",
                true,
            )),
        }
    }

    async fn commit(
        &self,
        replica: &StorageNodeId,
        request: ReplicaCommitRequest,
    ) -> Result<ReplicaCommitAccepted, ReplicaTransportError> {
        let response: ReplicaCommitResponse = self
            .post(replica, "/internal/active-range/replica/commit", &request)
            .await?;
        match (response.result, response.error) {
            (Some(result), _) => Ok(result),
            (None, Some(error)) => Err(ReplicaTransportError::replica(error)),
            (None, None) => Err(ReplicaTransportError::transport(
                "replica returned no commit result",
                true,
            )),
        }
    }

    async fn discard_pending(
        &self,
        replica: &StorageNodeId,
        request: ReplicaDiscardPendingRequest,
    ) -> Result<ReplicaDiscardPendingResponse, ReplicaTransportError> {
        let response: ReplicaDiscardPendingResponse = self
            .post(
                replica,
                "/internal/active-range/replica/discard-pending",
                &request,
            )
            .await?;
        match (response.outcome.as_ref(), response.error.as_ref()) {
            (Some(_), _) => Ok(response),
            (None, Some(error)) => Err(ReplicaTransportError::transport(
                error.message.clone(),
                error.retryable,
            )),
            (None, None) => Err(ReplicaTransportError::transport(
                "replica returned no discard outcome",
                true,
            )),
        }
    }
}
