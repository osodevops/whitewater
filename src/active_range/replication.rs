use std::{
    collections::{HashMap, HashSet},
    fmt,
    path::PathBuf,
    sync::Arc,
};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use crate::{
    codec::{decode_record, MAX_FRAME_BYTES},
    control::ControlController,
};

use super::{
    ActiveRangeAppend, ActiveRangeAssignment, ActiveRangeDescriptor, ActiveRangeError,
    ActiveRangeStore, ActiveRangeStoreError, AppendIdentity, CommitPosition, FileActiveRangeStore,
    FileActiveRangeStoreOptions, OwnershipEpoch, RangeGeneration, RangeId, RangePosition,
    StorageNodeId,
};

pub const MAX_REPLICA_FRAME_BASE64_BYTES: usize = MAX_FRAME_BYTES.div_ceil(3) * 4;
pub const MAX_COMMITTED_READ_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReplicaAppendRequest {
    pub feed_id: Uuid,
    pub range_id: RangeId,
    pub generation: RangeGeneration,
    pub ownership_epoch: OwnershipEpoch,
    pub append_owner: StorageNodeId,
    pub expected_position: RangePosition,
    pub identity: AppendIdentity,
    pub cursor: String,
    pub frame_base64: String,
    /// When set, the frame joins the named effect's pending tail: durable on
    /// the replica quorum but fenced behind the committed boundary until the
    /// effect's terminal journal marker releases it.
    #[serde(default)]
    pub pending_effect: Option<super::store::PendingEffectRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaAppendAccepted {
    pub position: RangePosition,
    pub message_id: Uuid,
    pub cursor: String,
    pub frame_digest: [u8; 32],
    pub deduplicated: bool,
    pub durable: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaAppendErrorCode {
    AssignmentNotFound,
    WrongRange,
    WrongGeneration,
    StaleOwnershipEpoch,
    NotCurrentOwner,
    ReceiverNotReplica,
    InvalidFrameEncoding,
    FrameTooLarge,
    InvalidFrame,
    PositionGap,
    PositionConflict,
    WriterSequenceConflict,
    RangeFrozen,
    PendingEffectOccupied,
    StorageFailure,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaAppendError {
    pub code: ReplicaAppendErrorCode,
    pub message: String,
    pub retryable: bool,
    /// The effect holding the tail when `code` is `PendingEffectOccupied`.
    /// Boxed to keep the error type small enough for hot paths.
    #[serde(default)]
    pub pending_effect: Option<Box<super::store::PendingEffect>>,
    /// The replica's next expected RangePosition when `code` is
    /// `PositionGap`; the Append Owner uses it to re-drive the missing
    /// positions instead of abandoning the replica.
    #[serde(default)]
    pub expected_position: Option<u64>,
}

impl ReplicaAppendError {
    fn rejected(code: ReplicaAppendErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: false,
            pending_effect: None,
            expected_position: None,
        }
    }

    fn temporary(code: ReplicaAppendErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: true,
            pending_effect: None,
            expected_position: None,
        }
    }

    fn storage(message: impl Into<String>) -> Self {
        Self::temporary(ReplicaAppendErrorCode::StorageFailure, message)
    }

    fn pending_occupied(occupant: super::store::PendingEffect) -> Self {
        Self {
            code: ReplicaAppendErrorCode::PendingEffectOccupied,
            message: format!(
                "Active Range tail is held by pending effect {}",
                occupant.effect_id
            ),
            retryable: true,
            pending_effect: Some(Box::new(occupant)),
            expected_position: None,
        }
    }
}

impl fmt::Display for ReplicaAppendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(formatter)
    }
}

impl std::error::Error for ReplicaAppendError {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaAppendResponse {
    pub result: Option<ReplicaAppendAccepted>,
    pub error: Option<ReplicaAppendError>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReplicaCommitRequest {
    pub feed_id: Uuid,
    pub range_id: RangeId,
    pub generation: RangeGeneration,
    pub ownership_epoch: OwnershipEpoch,
    pub append_owner: StorageNodeId,
    pub commit_position: CommitPosition,
    pub frame_digest: [u8; 32],
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaCommitAccepted {
    pub commit_position: CommitPosition,
    pub durable: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaCommitResponse {
    pub result: Option<ReplicaCommitAccepted>,
    pub error: Option<ReplicaAppendError>,
}

/// Discard one effect's pending tail on a member replica: the uncommitted
/// frames are truncated and the marker cleared when the occupant matches.
/// Members the owner lost contact with still resolve their tail through
/// the uncommitted-truncation path on ownership change.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReplicaDiscardPendingRequest {
    pub feed_id: Uuid,
    pub range_id: RangeId,
    pub generation: RangeGeneration,
    pub ownership_epoch: OwnershipEpoch,
    pub append_owner: StorageNodeId,
    pub effect_id: Uuid,
    pub subscription_id: Uuid,
}

/// The replica-level outcome of a discard request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum ReplicaDiscardOutcome {
    /// No effect holds the tail on this replica.
    NotPending,
    /// A different effect holds the tail; `occupant` carries its identity.
    Occupied,
    /// The matching marker's uncommitted tail was truncated and cleared.
    Discarded { dropped_frames: u64 },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReplicaDiscardPendingResponse {
    /// `None` means a store-level discard outcome could not be produced
    /// (the error field carries the reason).
    #[serde(default)]
    pub outcome: Option<ReplicaDiscardOutcome>,
    /// The occupant identity when the tail is held by a different effect.
    #[serde(default)]
    pub occupant: Option<super::store::PendingEffect>,
    #[serde(default)]
    pub error: Option<ReplicaAppendError>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadReplicaEvidence {
    pub node: StorageNodeId,
    pub committed: CommitPosition,
    pub digest: Option<[u8; 32]>,
}

/// The exclusive start selectors for a single-range committed read. Exactly
/// one of `cursor`/`tail_count`/`ingest_threshold` may be set, or `after`
/// alone for physical continuation; the service rejects combinations.
#[derive(Clone, Copy, Debug, Default)]
pub struct RangeReadSpec<'a> {
    /// Opaque record Cursor to resolve against the committed index.
    pub cursor: Option<&'a str>,
    /// Physical continuation position; `None` starts at the committed head.
    pub after: Option<RangePosition>,
    /// Serve the last `n` committed frames.
    pub tail_count: Option<usize>,
    /// Ingest-clock probe: resolve the committed ingest boundary without
    /// streaming frames; the reply carries `ingest_boundary` instead.
    pub ingest_threshold: Option<i64>,
    /// Bounded point reads: resolve each committed Cursor and return the
    /// frame at that exact position — used by Subscription member fetches
    /// re-delivering deferred positions owed to a Key window.
    pub cursors: Option<&'a [String]>,
}

/// Outcome of one `retention_sweep` over every range directory on this
/// Node: spans deleted, bytes reclaimed, and per-range skips/failures a
/// later tick retries — one corrupt range never stalls the rest.
#[derive(Clone, Debug, Default)]
pub struct RetentionSweepReport {
    pub ranges_scanned: u64,
    /// Ranges skipped because their Feed is frozen by a legal hold —
    /// counted separately from errors so an operator can see the
    /// freeze working.
    pub held_ranges: u64,
    pub deleted_segments: u64,
    pub reclaimed_bytes: u64,
    pub skipped: Vec<String>,
}

#[derive(Clone)]
pub struct ReplicaAppendService {
    root: Arc<PathBuf>,
    local_node: StorageNodeId,
    control: Arc<ControlController>,
    stores: Arc<RwLock<HashMap<(RangeId, RangeGeneration), FileActiveRangeStore>>>,
    store_open_gate: Arc<Mutex<()>>,
    frozen_generations: Arc<RwLock<HashSet<(RangeId, RangeGeneration)>>>,
    /// Ranges whose owner-side appends last failed to reach a durable quorum
    /// (the local frame landed but the followers rejected). In-memory like
    /// `frozen_generations`; `recovery_status` reports such an owner as
    /// unhealthy so ownership moves instead of writes stalling forever.
    quorum_blocked_ranges: Arc<RwLock<HashSet<RangeId>>>,
    // Serialized per range: appends and generation freezes for one range
    // must not interleave, but unrelated ranges pipeline on this Node.
    append_gates: Arc<Mutex<std::collections::BTreeMap<RangeId, Arc<Mutex<()>>>>>,
    index_apply: Arc<std::sync::RwLock<Option<Arc<crate::index_build::IndexCommitApply>>>>,
    store_options: FileActiveRangeStoreOptions,
}

impl ReplicaAppendService {
    pub fn new(
        root: impl Into<PathBuf>,
        local_node: StorageNodeId,
        control: Arc<ControlController>,
    ) -> Self {
        Self::with_options(
            root,
            local_node,
            control,
            FileActiveRangeStoreOptions::default(),
        )
    }

    /// Same as `new` but carries the segment-size bound into every store
    /// this service opens — wired from `WHITEWATER_MAX_SEGMENT_BYTES` so
    /// development and test topologies can exercise rotation and tiering
    /// without writing 256 MiB of history first.
    pub fn with_options(
        root: impl Into<PathBuf>,
        local_node: StorageNodeId,
        control: Arc<ControlController>,
        store_options: FileActiveRangeStoreOptions,
    ) -> Self {
        Self {
            root: Arc::new(root.into()),
            local_node,
            control,
            stores: Arc::new(RwLock::new(HashMap::new())),
            store_open_gate: Arc::new(Mutex::new(())),
            frozen_generations: Arc::new(RwLock::new(HashSet::new())),
            quorum_blocked_ranges: Arc::new(RwLock::new(HashSet::new())),
            append_gates: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            index_apply: Arc::new(std::sync::RwLock::new(None)),
            store_options,
        }
    }

    /// Installs the commit-boundary Index apply trigger. Optional: Nodes
    /// without an Index store simply skip the hook, and the periodic
    /// build driver remains the materialization backstop either way.
    pub fn set_index_apply(&self, apply: Arc<crate::index_build::IndexCommitApply>) {
        if let Ok(mut slot) = self.index_apply.write() {
            *slot = Some(apply);
        }
    }

    fn kick_index_apply(&self, feed_id: Uuid) {
        let apply = self
            .index_apply
            .read()
            .ok()
            .and_then(|slot| slot.as_ref().map(Arc::clone));
        if let Some(apply) = apply {
            apply.kick(feed_id);
        }
    }

    pub fn local_node(&self) -> &StorageNodeId {
        &self.local_node
    }

    /// Applies each Feed's effective History Policy to this Node's range
    /// storage: sealed spans past `expire_after_ns` or over
    /// `max_retained_bytes` are deleted as one contiguous prefix and the
    /// durable retained floor advances. Live stores expire under their
    /// range lock; unopened directories take the identical filesystem
    /// pass. Every replica resolves the same content-derived manifest
    /// fields, so keep/delete decisions agree without coordination.
    pub async fn retention_sweep(
        &self,
        policies: &HashMap<Uuid, crate::control::HistoryPolicy>,
        legal_holds: &HashSet<Uuid>,
        objects: Option<Arc<dyn super::tier::ObjectStore>>,
        now: i64,
    ) -> RetentionSweepReport {
        let mut report = RetentionSweepReport::default();
        for directory in super::tier::generation_directories(self.root.as_path()) {
            let state_path = directory.join("range-state.json");
            let mut persisted = match std::fs::read(&state_path).ok().and_then(|bytes| {
                serde_json::from_slice::<super::store::PersistedRangeState>(&bytes).ok()
            }) {
                Some(state) => state,
                None => continue,
            };
            report.ranges_scanned += 1;
            // A legal hold freezes every destruction path: neither
            // expiry nor the byte budget may touch this Feed's history.
            if legal_holds.contains(&persisted.feed_id) {
                report.held_ranges += 1;
                continue;
            }
            let Some(policy) = policies.get(&persisted.feed_id).cloned() else {
                continue;
            };
            if policy.expire_after_ns.is_none() && policy.max_retained_bytes.is_none() {
                continue;
            }
            let key = (persisted.range_id, persisted.generation);
            let live = self.stores.read().await.get(&key).cloned();
            let outcome = match live {
                Some(store) => store.apply_retention(&policy, objects.clone(), now).await,
                None => {
                    let objects = objects.clone();
                    let path = directory.clone();
                    tokio::task::spawn_blocking(move || {
                        super::store::expire_range_directory(
                            &path,
                            &mut persisted,
                            &policy,
                            objects.as_deref(),
                            now,
                        )
                    })
                    .await
                    .map_err(|error| super::store::ActiveRangeStoreError::Worker(error.to_string()))
                    .and_then(|inner| inner)
                }
            };
            match outcome {
                Ok(expired) => {
                    report.deleted_segments += expired.deleted_segments;
                    report.reclaimed_bytes += expired.reclaimed_bytes;
                    report.skipped.extend(
                        expired
                            .skipped
                            .into_iter()
                            .map(|why| format!("{}: {why}", directory.display())),
                    );
                }
                Err(error) => report
                    .skipped
                    .push(format!("{}: {error}", directory.display())),
            }
        }
        report
    }

    async fn append_gate(&self, range_id: RangeId) -> Arc<Mutex<()>> {
        self.append_gates
            .lock()
            .await
            .entry(range_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub async fn freeze_generation(&self, range_id: RangeId, generation: RangeGeneration) {
        let _gate = self.append_gate(range_id).await.lock_owned().await;
        self.frozen_generations
            .write()
            .await
            .insert((range_id, generation));
    }

    pub async fn unfreeze_generation(&self, range_id: RangeId, generation: RangeGeneration) {
        self.frozen_generations
            .write()
            .await
            .remove(&(range_id, generation));
    }

    pub async fn generation_is_frozen(
        &self,
        range_id: RangeId,
        generation: RangeGeneration,
    ) -> bool {
        self.frozen_generations
            .read()
            .await
            .contains(&(range_id, generation))
    }

    /// Owner-side signal: the local frame landed but durable quorum failed.
    pub async fn note_owner_quorum_blocked(&self, range_id: RangeId) {
        self.quorum_blocked_ranges.write().await.insert(range_id);
    }

    /// Clears the quorum-blocked mark after a successful quorum append.
    pub async fn note_owner_quorum_restored(&self, range_id: RangeId) {
        self.quorum_blocked_ranges.write().await.remove(&range_id);
    }

    /// The next contiguous Writer sequence on this replica's landed dedup
    /// stream. Owner-only use: the coordinator reads it under the range's
    /// append serialization so the mint cannot interleave with a landing.
    pub async fn next_writer_sequence(
        &self,
        assignment: &ActiveRangeAssignment,
        writer_session_id: Uuid,
        writer_epoch: u64,
    ) -> Result<u64, ReplicaAppendError> {
        let store = self.store_for(assignment).await?;
        store
            .next_writer_sequence(writer_session_id, writer_epoch)
            .await
            .map_err(|error| ReplicaAppendError::storage(error.to_string()))
    }

    pub async fn next_position(
        &self,
        feed_id: Uuid,
        range_id: RangeId,
    ) -> Result<RangePosition, ReplicaAppendError> {
        let assignment = self
            .control
            .active_range_assignment_by_id(range_id)
            .await
            .ok_or_else(|| {
                ReplicaAppendError::temporary(
                    ReplicaAppendErrorCode::AssignmentNotFound,
                    format!("no committed Active Range assignment exists for Feed {feed_id}"),
                )
            })?;
        let snapshot = self
            .store_for(&assignment)
            .await?
            .snapshot()
            .await
            .map_err(map_store_error)?;
        snapshot
            .progress
            .appended()
            .checked_next()
            .map_err(|error| ReplicaAppendError::storage(error.to_string()))
    }

    pub async fn stage_split_frame(
        &self,
        assignment: &ActiveRangeAssignment,
        position: RangePosition,
        identity: AppendIdentity,
        cursor: String,
        frame: Vec<u8>,
    ) -> Result<super::ActiveRangeAppendResult, ReplicaAppendError> {
        if !assignment.replicas.contains(&self.local_node) {
            return Err(ReplicaAppendError::rejected(
                ReplicaAppendErrorCode::ReceiverNotReplica,
                format!("Node {} is not a staged replica", self.local_node),
            ));
        }
        self.store_for(assignment)
            .await?
            .import_split(ActiveRangeAppend {
                generation: assignment.generation,
                ownership_epoch: assignment.ownership_epoch,
                expected_position: Some(position),
                identity,
                cursor,
                frame,
                pending_effect: None,
            })
            .await
            .map_err(map_store_error)
    }

    /// Staged-import an ordered batch of frames in one call. Split/merge
    /// staging copies the full committed prefix per replica, so per-frame
    /// durability round-trips turn large histories into an unbounded
    /// single-request stall: batching pays one flush per call and each frame
    /// still dedups on position/identity/digest, so a retried call resumes
    /// instead of restaging.
    pub async fn stage_split_frames(
        &self,
        assignment: &ActiveRangeAssignment,
        frames: Vec<(RangePosition, AppendIdentity, String, Vec<u8>)>,
    ) -> Result<Vec<super::ActiveRangeAppendResult>, ReplicaAppendError> {
        if !assignment.replicas.contains(&self.local_node) {
            return Err(ReplicaAppendError::rejected(
                ReplicaAppendErrorCode::ReceiverNotReplica,
                format!("Node {} is not a staged replica", self.local_node),
            ));
        }
        self.store_for(assignment)
            .await?
            .import_split_batch(
                frames
                    .into_iter()
                    .map(|(position, identity, cursor, frame)| ActiveRangeAppend {
                        generation: assignment.generation,
                        ownership_epoch: assignment.ownership_epoch,
                        expected_position: Some(position),
                        identity,
                        cursor,
                        frame,
                        pending_effect: None,
                    })
                    .collect(),
            )
            .await
            .map_err(map_store_error)
    }

    pub async fn read_staged_committed(
        &self,
        assignment: &ActiveRangeAssignment,
        after: Option<RangePosition>,
        limit: usize,
    ) -> Result<Vec<super::StoredRangeFrame>, ReplicaAppendError> {
        self.store_for(assignment)
            .await?
            .read_committed(after, limit)
            .await
            .map_err(map_store_error)
    }

    pub async fn commit_staged_split(
        &self,
        assignment: &ActiveRangeAssignment,
        position: CommitPosition,
    ) -> Result<(), ReplicaAppendError> {
        let committed = self
            .store_for(assignment)
            .await?
            .commit(assignment.generation, assignment.ownership_epoch, position)
            .await
            .map_err(map_store_error);
        if committed.is_ok() {
            // Staged commits advance committed history without the append
            // path's commit RPC — the deterministic Index apply still rides
            // this boundary so children cover their inherited prefix.
            self.kick_index_apply(assignment.feed_id);
        }
        committed
    }

    pub async fn truncate_uncommitted_for_assignment(
        &self,
        assignment: &ActiveRangeAssignment,
    ) -> Result<u64, ReplicaAppendError> {
        self.store_for(assignment)
            .await?
            .truncate_uncommitted(assignment.generation, assignment.ownership_epoch)
            .await
            .map_err(map_store_error)
    }

    pub async fn recovery_status_for_assignment(
        &self,
        assignment: &ActiveRangeAssignment,
    ) -> Result<super::ReplicaRecoveryStatus, ReplicaAppendError> {
        let store = self.store_for(assignment).await?;
        let snapshot = store.snapshot().await.map_err(map_store_error)?;
        Ok(super::ReplicaRecoveryStatus {
            node: self.local_node.clone(),
            healthy: !store.persistence_failure(),
            appended: snapshot.progress.appended(),
            committed: snapshot.progress.commit_position(),
        })
    }

    /// The pending marker currently holding a range's uncommitted tail.
    pub async fn pending_effect(
        &self,
        assignment: &ActiveRangeAssignment,
    ) -> Result<Option<super::store::PendingEffect>, ReplicaAppendError> {
        let store = self.store_for(assignment).await?;
        store.pending_effect().await.map_err(map_store_error)
    }

    /// Discard one effect's pending tail on this replica. Fencing matches
    /// the commit path: assignment, generation, epoch, and member checks
    /// all run before the store truncates anything.
    pub async fn discard_pending(
        &self,
        request: ReplicaDiscardPendingRequest,
    ) -> Result<ReplicaDiscardPendingResponse, ReplicaAppendError> {
        let assignment = self
            .control
            .active_range_assignment_by_id(request.range_id)
            .await
            .filter(|assignment| assignment.feed_id == request.feed_id)
            .ok_or_else(|| {
                ReplicaAppendError::temporary(
                    ReplicaAppendErrorCode::AssignmentNotFound,
                    format!(
                        "no committed Active Range assignment exists for Feed {}",
                        request.feed_id
                    ),
                )
            })?;
        assignment
            .validate_request(
                request.generation,
                request.ownership_epoch,
                &request.append_owner,
            )
            .map_err(map_assignment_error)?;
        if !assignment.replicas.contains(&self.local_node) {
            return Err(ReplicaAppendError::rejected(
                ReplicaAppendErrorCode::ReceiverNotReplica,
                format!("Node {} is not a current replica", self.local_node),
            ));
        }
        let store = self.store_for(&assignment).await?;
        match store
            .discard_pending_effect(
                assignment.generation,
                assignment.ownership_epoch,
                super::store::PendingEffectRef {
                    effect_id: request.effect_id,
                    subscription_id: request.subscription_id,
                },
            )
            .await
            .map_err(map_store_error)?
        {
            super::store::PendingDiscardOutcome::NotPending => Ok(ReplicaDiscardPendingResponse {
                outcome: Some(ReplicaDiscardOutcome::NotPending),
                occupant: None,
                error: None,
            }),
            super::store::PendingDiscardOutcome::Occupied(marker) => {
                Ok(ReplicaDiscardPendingResponse {
                    outcome: Some(ReplicaDiscardOutcome::Occupied),
                    occupant: Some(marker),
                    error: None,
                })
            }
            super::store::PendingDiscardOutcome::Discarded { dropped_frames } => {
                Ok(ReplicaDiscardPendingResponse {
                    outcome: Some(ReplicaDiscardOutcome::Discarded { dropped_frames }),
                    occupant: None,
                    error: None,
                })
            }
        }
    }

    pub async fn recovery_status(
        &self,
        feed_id: Uuid,
    ) -> Result<super::ReplicaRecoveryStatus, ReplicaAppendError> {
        let assignment = self
            .control
            .active_range_assignment(feed_id)
            .await
            .ok_or_else(|| {
                ReplicaAppendError::temporary(
                    ReplicaAppendErrorCode::AssignmentNotFound,
                    format!("no committed Active Range assignment exists for Feed {feed_id}"),
                )
            })?;
        let store = self.store_for(&assignment).await?;
        let snapshot = store.snapshot().await.map_err(map_store_error)?;
        // A stale quorum mark left on a replica that stopped owning the range
        // must not degrade it: the flag only counts while this Node is owner.
        let quorum_blocked = assignment.owner == self.local_node
            && self
                .quorum_blocked_ranges
                .read()
                .await
                .contains(&assignment.range_id);
        Ok(super::ReplicaRecoveryStatus {
            node: self.local_node.clone(),
            healthy: !store.persistence_failure() && !quorum_blocked,
            appended: snapshot.progress.appended(),
            committed: snapshot.progress.commit_position(),
        })
    }

    pub async fn reconcile_recovery(
        &self,
        feed_id: Uuid,
        committed_prefix: CommitPosition,
    ) -> Result<u64, ReplicaAppendError> {
        let assignment = self
            .control
            .active_range_assignment(feed_id)
            .await
            .ok_or_else(|| {
                ReplicaAppendError::temporary(
                    ReplicaAppendErrorCode::AssignmentNotFound,
                    format!("no committed Active Range assignment exists for Feed {feed_id}"),
                )
            })?;
        if !assignment.replicas.contains(&self.local_node) {
            return Err(ReplicaAppendError::rejected(
                ReplicaAppendErrorCode::ReceiverNotReplica,
                format!(
                    "Node {} is not a replica for Active Range {}",
                    self.local_node, assignment.range_id
                ),
            ));
        }
        let store = self.store_for(&assignment).await?;
        let snapshot = store.snapshot().await.map_err(map_store_error)?;
        // A replica behind the verified prefix cannot commit frames it does
        // not hold: promote the prefix it has and let committed repair
        // import the remainder instead of failing the recovery attempt.
        let promotable = CommitPosition::new(
            committed_prefix
                .value()
                .min(snapshot.progress.appended().value()),
        );
        if snapshot.progress.commit_position() < promotable {
            store
                .commit(
                    assignment.generation,
                    assignment.ownership_epoch,
                    promotable,
                )
                .await
                .map_err(map_store_error)?;
        }
        store
            .truncate_uncommitted(assignment.generation, assignment.ownership_epoch)
            .await
            .map_err(map_store_error)
    }

    pub async fn export_assignment_committed(
        &self,
        assignment: &ActiveRangeAssignment,
        after: Option<RangePosition>,
        limit: usize,
    ) -> Result<Vec<super::StoredRangeFrame>, ReplicaAppendError> {
        self.store_for(assignment)
            .await?
            .read_committed(after, limit)
            .await
            .map_err(map_store_error)
    }

    /// Frames through the appended tip, including uncommitted positions.
    /// The Append Owner replays these to replicas that report a position
    /// gap so a missed frame does not wedge the replica permanently.
    pub async fn read_assignment_uncommitted(
        &self,
        assignment: &ActiveRangeAssignment,
        after: Option<RangePosition>,
        limit: usize,
    ) -> Result<Vec<super::StoredRangeFrame>, ReplicaAppendError> {
        self.store_for(assignment)
            .await?
            .read_uncommitted(after, limit)
            .await
            .map_err(map_store_error)
    }

    pub async fn read_assignment_committed_bounded(
        &self,
        assignment: &ActiveRangeAssignment,
        after: Option<RangePosition>,
        limit: usize,
        max_bytes: usize,
    ) -> Result<Vec<super::StoredRangeFrame>, ReplicaAppendError> {
        self.store_for(assignment)
            .await?
            .read_committed_bounded(after, limit, max_bytes)
            .await
            .map_err(map_store_error)
    }

    pub async fn export_committed(
        &self,
        feed_id: Uuid,
        after: Option<RangePosition>,
        limit: usize,
    ) -> Result<Vec<super::StoredRangeFrame>, ReplicaAppendError> {
        let assignment = self
            .control
            .active_range_assignment(feed_id)
            .await
            .ok_or_else(|| {
                ReplicaAppendError::temporary(
                    ReplicaAppendErrorCode::AssignmentNotFound,
                    format!("no committed Active Range assignment exists for Feed {feed_id}"),
                )
            })?;
        self.store_for(&assignment)
            .await?
            .read_committed(after, limit)
            .await
            .map_err(map_store_error)
    }

    pub async fn quarantine_for_repair(
        &self,
        feed_id: Uuid,
    ) -> Result<Option<PathBuf>, ReplicaAppendError> {
        let assignment = self
            .control
            .active_range_assignment(feed_id)
            .await
            .ok_or_else(|| {
                ReplicaAppendError::temporary(
                    ReplicaAppendErrorCode::AssignmentNotFound,
                    format!("no committed Active Range assignment exists for Feed {feed_id}"),
                )
            })?;
        self.stores
            .write()
            .await
            .retain(|(range_id, _), _| range_id != &assignment.range_id);
        let directory = self
            .root
            .join(assignment.feed_id.to_string())
            .join(assignment.range_id.to_string())
            .join(format!("generation-{}", assignment.generation.value()));
        if !directory.exists() {
            return Ok(None);
        }
        let quarantine = directory.with_extension(format!("corrupt-{}", Uuid::new_v4()));
        tokio::task::spawn_blocking({
            let directory = directory.clone();
            let quarantine = quarantine.clone();
            move || std::fs::rename(directory, &quarantine)
        })
        .await
        .map_err(|error| ReplicaAppendError::storage(error.to_string()))?
        .map_err(|error| ReplicaAppendError::storage(error.to_string()))?;
        Ok(Some(quarantine))
    }

    pub async fn read_committed(
        &self,
        feed_id: Uuid,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<super::StoredRangeFrame>, ReplicaAppendError> {
        let assignments = self
            .control
            .active_range_assignments_for_feed(feed_id)
            .await;
        if assignments.is_empty() {
            return Err(ReplicaAppendError::temporary(
                ReplicaAppendErrorCode::AssignmentNotFound,
                format!("no committed Active Range assignments exist for Feed {feed_id}"),
            ));
        }
        require_all_assignments_locally(&assignments, &self.local_node)?;
        let mut merged = Vec::new();
        for assignment in &assignments {
            merged.extend(
                self.store_for(assignment)
                    .await?
                    .read_committed(None, 10_000)
                    .await
                    .map_err(map_store_error)?,
            );
        }
        merged.sort_by_key(|item| {
            decode_record(&item.frame)
                .map(|record| (record.ingest_time_ns, record.message_id))
                .unwrap_or((i64::MAX, Uuid::nil()))
        });
        let start = match after {
            Some(cursor) => merged
                .iter()
                .position(|item| item.cursor == cursor)
                .map(|index| index + 1)
                .ok_or_else(|| {
                    ReplicaAppendError::rejected(
                        ReplicaAppendErrorCode::PositionConflict,
                        "Cursor is unknown, uncommitted, or belongs to another Feed",
                    )
                })?,
            None => 0,
        };
        Ok(merged.into_iter().skip(start).take(limit).collect())
    }

    pub async fn read_owned_range_page(
        &self,
        assignment: &ActiveRangeAssignment,
        after: Option<RangePosition>,
        expected_commit: Option<CommitPosition>,
        page_limit: usize,
    ) -> Result<(CommitPosition, Vec<super::StoredRangeFrame>), ReplicaAppendError> {
        if self.local_node != assignment.owner
            || self
                .control
                .active_range_assignment_by_id(assignment.range_id)
                .await
                != Some(assignment.clone())
        {
            return Err(ReplicaAppendError::temporary(
                ReplicaAppendErrorCode::NotCurrentOwner,
                "range read requires the current committed Append Owner assignment",
            ));
        }
        let store = self.store_for(assignment).await?;
        let committed = store
            .snapshot()
            .await
            .map_err(map_store_error)?
            .progress
            .commit_position();
        if expected_commit.is_some_and(|expected| expected > committed) {
            return Err(ReplicaAppendError::temporary(
                ReplicaAppendErrorCode::StorageFailure,
                "previously committed history is unavailable on the current owner",
            ));
        }
        let boundary = expected_commit.unwrap_or(committed);
        let frames = store
            .read_committed_bounded(after, page_limit.clamp(1, 128), MAX_COMMITTED_READ_BYTES)
            .await
            .map_err(map_store_error)?
            .into_iter()
            .filter(|frame| frame.position.value() <= boundary.value())
            .collect();
        if self
            .control
            .active_range_assignment_by_id(assignment.range_id)
            .await
            != Some(assignment.clone())
        {
            return Err(ReplicaAppendError::temporary(
                ReplicaAppendErrorCode::NotCurrentOwner,
                "range placement changed during the committed read; retry safely",
            ));
        }
        Ok((boundary, frames))
    }

    pub async fn read_owned_range_cursor_page(
        &self,
        assignment: &ActiveRangeAssignment,
        spec: RangeReadSpec<'_>,
        expected_commit: Option<CommitPosition>,
        page_limit: usize,
    ) -> Result<
        (
            CommitPosition,
            Option<RangePosition>,
            Option<super::store::IngestBoundary>,
            Vec<super::StoredRangeFrame>,
        ),
        ReplicaAppendError,
    > {
        if self.local_node != assignment.owner
            || self
                .control
                .active_range_assignment_by_id(assignment.range_id)
                .await
                != Some(assignment.clone())
        {
            return Err(ReplicaAppendError::temporary(
                ReplicaAppendErrorCode::NotCurrentOwner,
                "single-range read requires the current committed Append Owner assignment",
            ));
        }
        if [
            spec.cursor.is_some(),
            spec.after.is_some(),
            spec.tail_count.is_some(),
            spec.ingest_threshold.is_some(),
            spec.cursors.is_some(),
        ]
        .iter()
        .filter(|set| **set)
        .count()
            > 1
        {
            return Err(ReplicaAppendError::rejected(
                ReplicaAppendErrorCode::InvalidFrame,
                "Cursor, physical continuation, tail, ingest-threshold, and point-read starts cannot be combined",
            ));
        }
        if spec.cursors.as_ref().is_some_and(|cursors| {
            cursors.is_empty()
                || cursors.len() > 64
                || cursors.iter().any(|c| c.is_empty() || c.len() > 256)
        }) {
            return Err(ReplicaAppendError::rejected(
                ReplicaAppendErrorCode::InvalidFrame,
                "point-reads require 1-64 bounded Cursors",
            ));
        }
        let store = self.store_for(assignment).await?;
        let committed = store
            .snapshot()
            .await
            .map_err(map_store_error)?
            .progress
            .commit_position();
        if expected_commit.is_some_and(|expected| expected > committed) {
            return Err(ReplicaAppendError::temporary(
                ReplicaAppendErrorCode::StorageFailure,
                "previously committed history is unavailable on the current owner",
            ));
        }
        let boundary = expected_commit.unwrap_or(committed);
        let mut ingest_boundary = None;
        let start = if let Some(cursor) = spec.cursor {
            Some(
                store
                    .committed_position_for_cursor(cursor)
                    .await
                    .map_err(map_store_error)?
                    .filter(|position| position.value() <= boundary.value())
                    .ok_or_else(|| {
                        ReplicaAppendError::rejected(
                            ReplicaAppendErrorCode::PositionConflict,
                            "Cursor is unknown, uncommitted, or belongs to another Feed",
                        )
                    })?,
            )
        } else if let Some(count) = spec.tail_count {
            Some(RangePosition::new(
                boundary.value().saturating_sub(count as u64),
            ))
        } else if let Some(threshold) = spec.ingest_threshold {
            let resolved = store
                .committed_ingest_boundary(threshold, boundary)
                .await
                .map_err(map_store_error)?;
            let position = match &resolved {
                super::store::IngestBoundary::Ordered { position, .. } => *position,
                super::store::IngestBoundary::Unordered => None,
            };
            ingest_boundary = Some(resolved);
            position
        } else {
            spec.after
        };
        // An ingest probe resolves its boundary in the index and serves no
        // frames; `cursors` point-reads resolve each Cursor to its frame;
        // other modes stream the bounded page after `start`.
        let frames = if spec.ingest_threshold.is_some() {
            Vec::new()
        } else if let Some(cursors) = spec.cursors {
            let mut frames = Vec::with_capacity(cursors.len());
            for cursor in cursors {
                let position = store
                    .committed_position_for_cursor(cursor)
                    .await
                    .map_err(map_store_error)?
                    .filter(|position| position.value() <= boundary.value())
                    .ok_or_else(|| {
                        ReplicaAppendError::rejected(
                            ReplicaAppendErrorCode::PositionConflict,
                            "Cursor is unknown, uncommitted, or belongs to another Feed",
                        )
                    })?;
                let frame = store
                    .read_committed_bounded(
                        Some(RangePosition::new(position.value().saturating_sub(1))),
                        1,
                        MAX_COMMITTED_READ_BYTES,
                    )
                    .await
                    .map_err(map_store_error)?
                    .into_iter()
                    .next()
                    .filter(|frame| frame.position == position)
                    .ok_or_else(|| {
                        ReplicaAppendError::rejected(
                            ReplicaAppendErrorCode::PositionConflict,
                            "Cursor resolved to a committed position whose frame is unavailable",
                        )
                    })?;
                frames.push(frame);
            }
            frames
        } else {
            store
                .read_committed_bounded(start, page_limit.clamp(1, 32), MAX_COMMITTED_READ_BYTES)
                .await
                .map_err(map_store_error)?
                .into_iter()
                .filter(|frame| frame.position.value() <= boundary.value())
                .collect()
        };
        if self
            .control
            .active_range_assignment_by_id(assignment.range_id)
            .await
            != Some(assignment.clone())
        {
            return Err(ReplicaAppendError::temporary(
                ReplicaAppendErrorCode::NotCurrentOwner,
                "range placement changed during the committed read; retry safely",
            ));
        }
        Ok((boundary, start, ingest_boundary, frames))
    }

    pub async fn read_replica_evidence(
        &self,
        assignment: &ActiveRangeAssignment,
        position: CommitPosition,
    ) -> Result<ReadReplicaEvidence, ReplicaAppendError> {
        if !assignment.replicas.contains(&self.local_node)
            || self
                .control
                .active_range_assignment_by_id(assignment.range_id)
                .await
                != Some(assignment.clone())
        {
            return Err(ReplicaAppendError::temporary(
                ReplicaAppendErrorCode::ReceiverNotReplica,
                "replica no longer hosts the current committed range assignment",
            ));
        }
        let store = self.store_for(assignment).await?;
        let committed = store
            .snapshot()
            .await
            .map_err(map_store_error)?
            .progress
            .commit_position();
        let digest = if position.value() > 0 && committed >= position {
            Some(
                store
                    .frame_digest(RangePosition::new(position.value()))
                    .await
                    .map_err(map_store_error)?
                    .ok_or_else(|| {
                        ReplicaAppendError::storage("committed frame digest is unavailable")
                    })?,
            )
        } else {
            None
        };
        if self
            .control
            .active_range_assignment_by_id(assignment.range_id)
            .await
            != Some(assignment.clone())
        {
            return Err(ReplicaAppendError::temporary(
                ReplicaAppendErrorCode::NotCurrentOwner,
                "range placement changed during read evidence collection; retry safely",
            ));
        }
        Ok(ReadReplicaEvidence {
            node: self.local_node.clone(),
            committed,
            digest,
        })
    }

    pub async fn append(
        &self,
        request: ReplicaAppendRequest,
    ) -> Result<ReplicaAppendAccepted, ReplicaAppendError> {
        self.append_inner(request, false).await
    }

    /// Replay one owner-authoritative frame under the same replica fencing
    /// as `append`, but without live-Writer sequence ordering: replica
    /// fan-out, gap re-drives, and repair/catch-up all install positions from
    /// the owner's log, whose Writer sequences can sit below the local dedup
    /// high-water when dedup-consumed retries advanced the stream watermark
    /// without landing frames. Position, digest, Cursor, and pending-effect
    /// integrity checks still apply; a conflicting occupant still fails
    /// closed.
    pub async fn import_committed(
        &self,
        request: ReplicaAppendRequest,
    ) -> Result<ReplicaAppendAccepted, ReplicaAppendError> {
        self.append_inner(request, true).await
    }

    async fn append_inner(
        &self,
        request: ReplicaAppendRequest,
        import: bool,
    ) -> Result<ReplicaAppendAccepted, ReplicaAppendError> {
        let _gate = self.append_gate(request.range_id).await.lock_owned().await;
        if self
            .generation_is_frozen(request.range_id, request.generation)
            .await
        {
            return Err(ReplicaAppendError::temporary(
                ReplicaAppendErrorCode::RangeFrozen,
                "Active Range generation is frozen for split cutover; retry safely",
            ));
        }
        if request.frame_base64.len() > MAX_REPLICA_FRAME_BASE64_BYTES {
            return Err(ReplicaAppendError::rejected(
                ReplicaAppendErrorCode::FrameTooLarge,
                format!(
                    "encoded replica frame exceeds the {} byte limit",
                    MAX_REPLICA_FRAME_BASE64_BYTES
                ),
            ));
        }
        let frame = STANDARD.decode(&request.frame_base64).map_err(|error| {
            ReplicaAppendError::rejected(
                ReplicaAppendErrorCode::InvalidFrameEncoding,
                format!("replica frame is not valid base64: {error}"),
            )
        })?;
        if frame.len() > MAX_FRAME_BYTES {
            return Err(ReplicaAppendError::rejected(
                ReplicaAppendErrorCode::FrameTooLarge,
                format!("decoded replica frame exceeds the {MAX_FRAME_BYTES} byte limit"),
            ));
        }
        let assignment = match self
            .control
            .active_range_assignment_by_id(request.range_id)
            .await
        {
            Some(assignment) if assignment.feed_id == request.feed_id => assignment,
            Some(_) => {
                return Err(ReplicaAppendError::temporary(
                    ReplicaAppendErrorCode::AssignmentNotFound,
                    format!(
                        "no committed Active Range assignment exists for Feed {}",
                        request.feed_id
                    ),
                ))
            }
            None => self
                .control
                .active_range_assignment(request.feed_id)
                .await
                .ok_or_else(|| {
                    ReplicaAppendError::temporary(
                        ReplicaAppendErrorCode::AssignmentNotFound,
                        format!(
                            "no committed Active Range assignment exists for Feed {}",
                            request.feed_id
                        ),
                    )
                })?,
        };
        self.validate_assignment(&assignment, &request)?;
        let store = self.store_for(&assignment).await?;
        let append = ActiveRangeAppend {
            generation: request.generation,
            ownership_epoch: request.ownership_epoch,
            expected_position: Some(request.expected_position),
            identity: request.identity,
            cursor: request.cursor,
            frame,
            pending_effect: request.pending_effect,
        };
        let result = if import {
            store.import_split(append).await
        } else {
            store.append(append).await
        }
        .map_err(map_store_error)?;
        Ok(ReplicaAppendAccepted {
            position: result.position,
            message_id: result.message_id,
            cursor: result.cursor,
            frame_digest: result.frame_digest,
            deduplicated: result.deduplicated,
            durable: true,
        })
    }

    pub async fn commit(
        &self,
        request: ReplicaCommitRequest,
    ) -> Result<ReplicaCommitAccepted, ReplicaAppendError> {
        let assignment = match self
            .control
            .active_range_assignment_by_id(request.range_id)
            .await
        {
            Some(assignment) if assignment.feed_id == request.feed_id => assignment,
            Some(_) => {
                return Err(ReplicaAppendError::temporary(
                    ReplicaAppendErrorCode::AssignmentNotFound,
                    format!(
                        "no committed Active Range assignment exists for Feed {}",
                        request.feed_id
                    ),
                ))
            }
            None => self
                .control
                .active_range_assignment(request.feed_id)
                .await
                .ok_or_else(|| {
                    ReplicaAppendError::temporary(
                        ReplicaAppendErrorCode::AssignmentNotFound,
                        format!(
                            "no committed Active Range assignment exists for Feed {}",
                            request.feed_id
                        ),
                    )
                })?,
        };
        if assignment.range_id != request.range_id {
            return Err(ReplicaAppendError::rejected(
                ReplicaAppendErrorCode::WrongRange,
                "commit RangeId does not match the committed assignment",
            ));
        }
        assignment
            .validate_request(
                request.generation,
                request.ownership_epoch,
                &request.append_owner,
            )
            .map_err(map_assignment_error)?;
        if !assignment.replicas.contains(&self.local_node) {
            return Err(ReplicaAppendError::rejected(
                ReplicaAppendErrorCode::ReceiverNotReplica,
                format!("Node {} is not a current replica", self.local_node),
            ));
        }
        let store = self.store_for(&assignment).await?;
        let position = RangePosition::new(request.commit_position.value());
        let local_digest = match store
            .frame_digest(position)
            .await
            .map_err(map_store_error)?
        {
            Some(digest) => digest,
            None => {
                let mut error = ReplicaAppendError::rejected(
                    ReplicaAppendErrorCode::PositionGap,
                    format!("RangePosition {position} is not durable on this replica"),
                );
                error.expected_position = store
                    .snapshot()
                    .await
                    .ok()
                    .map(|snapshot| snapshot.progress.appended().value().saturating_add(1));
                return Err(error);
            }
        };
        if local_digest != request.frame_digest {
            return Err(ReplicaAppendError::rejected(
                ReplicaAppendErrorCode::PositionConflict,
                format!("RangePosition {position} has a different frame digest"),
            ));
        }
        match store
            .commit(
                request.generation,
                request.ownership_epoch,
                request.commit_position,
            )
            .await
        {
            Ok(()) => {}
            // A same-identity append retry reaches commit after the frame has
            // already joined the committed prefix. The digest check above
            // proved it is the same frame, so the CommitPosition evidence this
            // request asks for is already persisted; report it rather than
            // turning a completed append into a permanent failure.
            Err(ActiveRangeStoreError::CommitMovedBackwards { .. }) => {}
            Err(error) => return Err(map_store_error(error)),
        }
        // The committed frames are the replicated Index mutation intents:
        // every replica reaching this point applies the same deterministic
        // postings at the same committed boundary. The kick is coalesced
        // and async so the commit RPC never waits on Index latency.
        self.kick_index_apply(request.feed_id);
        Ok(ReplicaCommitAccepted {
            commit_position: request.commit_position,
            durable: true,
        })
    }

    fn validate_assignment(
        &self,
        assignment: &ActiveRangeAssignment,
        request: &ReplicaAppendRequest,
    ) -> Result<(), ReplicaAppendError> {
        if assignment.range_id != request.range_id {
            return Err(ReplicaAppendError::rejected(
                ReplicaAppendErrorCode::WrongRange,
                format!(
                    "request RangeId {} does not match committed RangeId {}",
                    request.range_id, assignment.range_id
                ),
            ));
        }
        assignment
            .validate_request(
                request.generation,
                request.ownership_epoch,
                &request.append_owner,
            )
            .map_err(map_assignment_error)?;
        if !assignment.replicas.contains(&self.local_node) {
            return Err(ReplicaAppendError::rejected(
                ReplicaAppendErrorCode::ReceiverNotReplica,
                format!(
                    "Node {} is not a replica for Active Range {}",
                    self.local_node, assignment.range_id
                ),
            ));
        }
        Ok(())
    }

    async fn store_for(
        &self,
        assignment: &ActiveRangeAssignment,
    ) -> Result<FileActiveRangeStore, ReplicaAppendError> {
        let store_key = (assignment.range_id, assignment.generation);
        if let Some(store) = self.stores.read().await.get(&store_key).cloned() {
            synchronize_store_epoch(&store, assignment).await?;
            return Ok(store);
        }
        let _open_guard = self.store_open_gate.lock().await;
        if let Some(store) = self.stores.read().await.get(&store_key).cloned() {
            synchronize_store_epoch(&store, assignment).await?;
            return Ok(store);
        }
        let root = Arc::clone(&self.root);
        let descriptor = ActiveRangeDescriptor {
            feed_id: assignment.feed_id,
            range_id: assignment.range_id,
            generation: assignment.generation,
            ownership_epoch: assignment.ownership_epoch,
        };
        let store_options = self.store_options.clone();
        let opened = tokio::task::spawn_blocking(move || {
            FileActiveRangeStore::open_with_options(root.as_ref(), descriptor, store_options)
        })
        .await
        .map_err(|error| ReplicaAppendError::storage(error.to_string()))?
        .map_err(|error| ReplicaAppendError::storage(error.to_string()))?;
        let mut stores = self.stores.write().await;
        Ok(stores.entry(store_key).or_insert(opened).clone())
    }
}

fn require_all_assignments_locally(
    assignments: &[ActiveRangeAssignment],
    local: &StorageNodeId,
) -> Result<(), ReplicaAppendError> {
    if let Some(missing) = assignments
        .iter()
        .find(|assignment| !assignment.replicas.contains(local))
    {
        return Err(ReplicaAppendError::temporary(
            ReplicaAppendErrorCode::ReceiverNotReplica,
            format!(
                "Node {local} does not have Active Range {}; local-only reads require every assigned range, so use the topology-free Feed read API for cross-Node retrieval",
                missing.range_id
            ),
        ));
    }
    Ok(())
}

async fn synchronize_store_epoch(
    store: &FileActiveRangeStore,
    assignment: &ActiveRangeAssignment,
) -> Result<(), ReplicaAppendError> {
    let snapshot = store
        .snapshot()
        .await
        .map_err(|error| ReplicaAppendError::storage(error.to_string()))?;
    if snapshot.generation != assignment.generation {
        return Err(ReplicaAppendError::rejected(
            ReplicaAppendErrorCode::WrongGeneration,
            format!(
                "local generation {} does not match committed generation {}",
                snapshot.generation, assignment.generation
            ),
        ));
    }
    if snapshot.ownership_epoch < assignment.ownership_epoch {
        store
            .adopt_ownership_epoch(
                assignment.generation,
                snapshot.ownership_epoch,
                assignment.ownership_epoch,
            )
            .await
            .map_err(map_store_error)?;
    } else if snapshot.ownership_epoch > assignment.ownership_epoch {
        return Err(ReplicaAppendError::rejected(
            ReplicaAppendErrorCode::StaleOwnershipEpoch,
            format!(
                "local ownership epoch {} is ahead of committed epoch {}",
                snapshot.ownership_epoch, assignment.ownership_epoch
            ),
        ));
    }
    Ok(())
}

fn map_assignment_error(error: ActiveRangeError) -> ReplicaAppendError {
    match error {
        ActiveRangeError::WrongGeneration { .. } => {
            ReplicaAppendError::rejected(ReplicaAppendErrorCode::WrongGeneration, error.to_string())
        }
        ActiveRangeError::StaleEpoch { .. } => ReplicaAppendError::rejected(
            ReplicaAppendErrorCode::StaleOwnershipEpoch,
            error.to_string(),
        ),
        ActiveRangeError::NotCurrentOwner(_) => {
            ReplicaAppendError::rejected(ReplicaAppendErrorCode::NotCurrentOwner, error.to_string())
        }
        _ => {
            ReplicaAppendError::rejected(ReplicaAppendErrorCode::StorageFailure, error.to_string())
        }
    }
}

fn map_store_error(error: ActiveRangeStoreError) -> ReplicaAppendError {
    match error {
        ActiveRangeStoreError::Codec(_) | ActiveRangeStoreError::WriterIdentityMismatch => {
            ReplicaAppendError::rejected(ReplicaAppendErrorCode::InvalidFrame, error.to_string())
        }
        ActiveRangeStoreError::PositionGap { expected, .. } => {
            let mut mapped = ReplicaAppendError::rejected(
                ReplicaAppendErrorCode::PositionGap,
                error.to_string(),
            );
            mapped.expected_position = Some(expected.value());
            mapped
        }
        ActiveRangeStoreError::PositionConflict(_) => ReplicaAppendError::rejected(
            ReplicaAppendErrorCode::PositionConflict,
            error.to_string(),
        ),
        ActiveRangeStoreError::WriterSequenceConflict { .. }
        | ActiveRangeStoreError::StaleWriterSequence { .. }
        | ActiveRangeStoreError::WriterSequenceGap { .. } => ReplicaAppendError::rejected(
            ReplicaAppendErrorCode::WriterSequenceConflict,
            error.to_string(),
        ),
        ActiveRangeStoreError::WrongGeneration { .. } => {
            ReplicaAppendError::rejected(ReplicaAppendErrorCode::WrongGeneration, error.to_string())
        }
        ActiveRangeStoreError::StaleOwnershipEpoch { .. } => ReplicaAppendError::rejected(
            ReplicaAppendErrorCode::StaleOwnershipEpoch,
            error.to_string(),
        ),
        ActiveRangeStoreError::PendingEffectOccupied(occupant) => {
            ReplicaAppendError::pending_occupied(occupant)
        }
        other => ReplicaAppendError::storage(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        active_range::ReplicaSet,
        control::Command,
        storage::{FileLogStore, LogStore},
    };

    #[tokio::test]
    async fn owner_quorum_block_marks_recovery_status_unhealthy_until_restored() {
        let root = tempfile::TempDir::new().unwrap();
        let nodes = ["storage-1", "storage-2", "storage-3"]
            .map(|node| StorageNodeId::try_new(node).unwrap());
        let legacy: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(root.path().join("legacy")).unwrap());
        let control = Arc::new(
            ControlController::open_with_storage_nodes(
                root.path().join("catalog.json"),
                legacy,
                nodes.to_vec(),
            )
            .unwrap(),
        );
        let created = control
            .execute_commands(vec![
                Command::CreateSpace {
                    name: "orders".to_owned(),
                },
                Command::CreateFeed {
                    name: "orders.events".to_owned(),
                },
            ])
            .await
            .unwrap();
        let feed_id: Uuid =
            serde_json::from_value(created.results[1].data["feed_id"].clone()).unwrap();
        let assignment = control.active_range_assignment(feed_id).await.unwrap();
        let descriptor = ActiveRangeDescriptor {
            feed_id,
            range_id: assignment.range_id,
            generation: assignment.generation,
            ownership_epoch: assignment.ownership_epoch,
        };
        let owner_root = root.path().join("owner");
        let peer_root = root.path().join("peer");
        crate::active_range::store::seed_committed_history(&owner_root, &descriptor, 7);
        crate::active_range::store::seed_committed_history(&peer_root, &descriptor, 7);
        let owner =
            ReplicaAppendService::new(&owner_root, assignment.owner.clone(), control.clone());
        let peer_id = assignment
            .replicas
            .iter()
            .find(|node| **node != assignment.owner)
            .unwrap()
            .clone();
        let peer = ReplicaAppendService::new(&peer_root, peer_id, control.clone());

        assert!(owner.recovery_status(feed_id).await.unwrap().healthy);

        // Owner appended locally but followers refused: the range must probe
        // unhealthy so ownership can move off the stalled owner.
        owner.note_owner_quorum_blocked(assignment.range_id).await;
        assert!(!owner.recovery_status(feed_id).await.unwrap().healthy);

        // The same mark on a replica that does not own the range must not
        // degrade it.
        peer.note_owner_quorum_blocked(assignment.range_id).await;
        assert!(peer.recovery_status(feed_id).await.unwrap().healthy);

        owner.note_owner_quorum_restored(assignment.range_id).await;
        assert!(owner.recovery_status(feed_id).await.unwrap().healthy);
    }

    #[test]
    fn a_partially_local_multi_range_feed_cannot_appear_complete() {
        let feed_id = Uuid::from_u128(1);
        let first = StorageNodeId::try_new("node-1").unwrap();
        let second = StorageNodeId::try_new("node-2").unwrap();
        let third = StorageNodeId::try_new("node-3").unwrap();
        let fourth = StorageNodeId::try_new("node-4").unwrap();
        let left = ActiveRangeAssignment::try_new(
            feed_id,
            RangeId::from_uuid(Uuid::from_u128(2)),
            RangeGeneration::new(1),
            first.clone(),
            ReplicaSet::try_new([first.clone(), second.clone(), third.clone()]).unwrap(),
            OwnershipEpoch::new(1),
        )
        .unwrap();
        let right = ActiveRangeAssignment::try_new(
            feed_id,
            RangeId::from_uuid(Uuid::from_u128(3)),
            RangeGeneration::new(1),
            second.clone(),
            ReplicaSet::try_new([second.clone(), third, fourth.clone()]).unwrap(),
            OwnershipEpoch::new(2),
        )
        .unwrap();
        assert!(require_all_assignments_locally(std::slice::from_ref(&left), &first).is_ok());
        assert!(require_all_assignments_locally(&[left.clone(), right.clone()], &second).is_ok());
        let missing_right =
            require_all_assignments_locally(&[left.clone(), right.clone()], &first).unwrap_err();
        assert_eq!(
            missing_right.code,
            ReplicaAppendErrorCode::ReceiverNotReplica
        );
        assert!(missing_right.retryable);
        assert!(missing_right.message.contains(&right.range_id.to_string()));
        assert!(require_all_assignments_locally(&[left, right], &fourth).is_err());
    }

    #[tokio::test]
    async fn import_committed_replays_history_below_the_writer_high_water() {
        let root = tempfile::TempDir::new().unwrap();
        let nodes = ["storage-1", "storage-2", "storage-3"]
            .map(|node| StorageNodeId::try_new(node).unwrap());
        let legacy: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(root.path().join("legacy")).unwrap());
        let control = Arc::new(
            ControlController::open_with_storage_nodes(
                root.path().join("catalog.json"),
                legacy,
                nodes.to_vec(),
            )
            .unwrap(),
        );
        let created = control
            .execute_commands(vec![
                Command::CreateSpace {
                    name: "orders".to_owned(),
                },
                Command::CreateFeed {
                    name: "orders.events".to_owned(),
                },
            ])
            .await
            .unwrap();
        let feed_id: Uuid =
            serde_json::from_value(created.results[1].data["feed_id"].clone()).unwrap();
        let assignment = control.active_range_assignment(feed_id).await.unwrap();
        let descriptor = ActiveRangeDescriptor {
            feed_id,
            range_id: assignment.range_id,
            generation: assignment.generation,
            ownership_epoch: assignment.ownership_epoch,
        };
        let replica_root = root.path().join("replica");
        crate::active_range::store::seed_committed_history(&replica_root, &descriptor, 4);
        let replica_id = assignment
            .replicas
            .iter()
            .find(|node| **node != assignment.owner)
            .unwrap()
            .clone();
        use crate::domain::StoredRecord;
        let replica = ReplicaAppendService::new(&replica_root, replica_id, control.clone());

        // Dedup-consumed retries advance the Writer stream watermark to 8
        // without landing frames: every retry re-requests its already-landed
        // Cursor, so position 4 stays the last frame while sequence 8 is on
        // record. The seeded history uses writer_session_id 3 with sequences
        // 1-4 at positions 1-4.
        let writer_session_id = Uuid::from_u128(3);
        for sequence in 5..=8 {
            let record = StoredRecord {
                message_id: Uuid::from_u128(104),
                producer_id: writer_session_id,
                producer_sequence: sequence,
                event_time_ns: 4,
                ingest_time_ns: 4,
                key: b"key".to_vec(),
                payload: Vec::new(),
                metadata: Default::default(),
            };
            let accepted = replica
                .append(ReplicaAppendRequest {
                    feed_id,
                    range_id: assignment.range_id,
                    generation: assignment.generation,
                    ownership_epoch: assignment.ownership_epoch,
                    append_owner: assignment.owner.clone(),
                    expected_position: RangePosition::new(5),
                    identity: AppendIdentity {
                        writer_session_id,
                        writer_epoch: 1,
                        sequence,
                    },
                    cursor: "cursor-4".to_owned(),
                    frame_base64: STANDARD.encode(crate::codec::encode_record(&record).unwrap()),
                    pending_effect: None,
                })
                .await
                .unwrap();
            assert!(accepted.deduplicated);
            assert_eq!(accepted.position, RangePosition::new(4));
        }

        // The repair frame is committed history from the quorum: position 5,
        // Writer sequence 5. The live-Writer append path must still refuse
        // it as stale against the stream watermark.
        let repair_record = StoredRecord {
            message_id: Uuid::from_u128(105),
            producer_id: writer_session_id,
            producer_sequence: 5,
            event_time_ns: 5,
            ingest_time_ns: 5,
            key: b"key".to_vec(),
            payload: Vec::new(),
            metadata: Default::default(),
        };
        let repair_request = ReplicaAppendRequest {
            feed_id,
            range_id: assignment.range_id,
            generation: assignment.generation,
            ownership_epoch: assignment.ownership_epoch,
            append_owner: assignment.owner.clone(),
            expected_position: RangePosition::new(5),
            identity: AppendIdentity {
                writer_session_id,
                writer_epoch: 1,
                sequence: 5,
            },
            cursor: "cursor-5".to_owned(),
            frame_base64: STANDARD.encode(crate::codec::encode_record(&repair_record).unwrap()),
            pending_effect: None,
        };
        let live_error = replica
            .append(repair_request.clone())
            .await
            .expect_err("live appends below the stream watermark stay fenced");
        assert_eq!(
            live_error.code,
            ReplicaAppendErrorCode::WriterSequenceConflict
        );
        assert!(live_error.message.contains("is stale"));

        let accepted = replica
            .import_committed(repair_request)
            .await
            .expect("committed-history replay must bypass the stream ordering fence");
        assert_eq!(accepted.position, RangePosition::new(5));
        assert!(!accepted.deduplicated);

        replica
            .commit(ReplicaCommitRequest {
                feed_id,
                range_id: assignment.range_id,
                generation: assignment.generation,
                ownership_epoch: assignment.ownership_epoch,
                append_owner: assignment.owner.clone(),
                commit_position: CommitPosition::new(5),
                frame_digest: accepted.frame_digest,
            })
            .await
            .unwrap();

        // Position and digest remain the fork guards under import: replaying
        // the same committed frame deduplicates, while different bytes at an
        // already-committed position conflict rather than fork the log.
        let mut replay = repair_request_for(&assignment, 5);
        replay.identity.sequence = 5;
        replay.cursor = "cursor-5".to_owned();
        replay.frame_base64 = STANDARD.encode(crate::codec::encode_record(&repair_record).unwrap());
        let replayed = replica.import_committed(replay).await.unwrap();
        assert!(replayed.deduplicated);
        assert_eq!(replayed.position, RangePosition::new(5));

        let mut tampered = repair_record.clone();
        tampered.payload = b"tampered".to_vec();
        let mut tampered_request = repair_request_for(&assignment, 5);
        tampered_request.identity.sequence = 5;
        tampered_request.cursor = "cursor-5b".to_owned();
        tampered_request.frame_base64 =
            STANDARD.encode(crate::codec::encode_record(&tampered).unwrap());
        let tampered_error = replica
            .import_committed(tampered_request)
            .await
            .expect_err("different bytes at a committed position must conflict");
        assert_eq!(
            tampered_error.code,
            ReplicaAppendErrorCode::PositionConflict
        );
    }

    fn repair_request_for(
        assignment: &ActiveRangeAssignment,
        position: u64,
    ) -> ReplicaAppendRequest {
        ReplicaAppendRequest {
            feed_id: assignment.feed_id,
            range_id: assignment.range_id,
            generation: assignment.generation,
            ownership_epoch: assignment.ownership_epoch,
            append_owner: assignment.owner.clone(),
            expected_position: RangePosition::new(position),
            identity: AppendIdentity {
                writer_session_id: Uuid::from_u128(3),
                writer_epoch: 1,
                sequence: position,
            },
            cursor: String::new(),
            frame_base64: String::new(),
            pending_effect: None,
        }
    }
}
