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
    OwnershipEpoch, RangeGeneration, RangeId, RangePosition, StorageNodeId,
};

pub const MAX_REPLICA_FRAME_BASE64_BYTES: usize = MAX_FRAME_BYTES.div_ceil(3) * 4;

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
    StorageFailure,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaAppendError {
    pub code: ReplicaAppendErrorCode,
    pub message: String,
    pub retryable: bool,
}

impl ReplicaAppendError {
    fn rejected(code: ReplicaAppendErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: false,
        }
    }

    fn temporary(code: ReplicaAppendErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: true,
        }
    }

    fn storage(message: impl Into<String>) -> Self {
        Self::temporary(ReplicaAppendErrorCode::StorageFailure, message)
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

#[derive(Clone)]
pub struct ReplicaAppendService {
    root: Arc<PathBuf>,
    local_node: StorageNodeId,
    control: Arc<ControlController>,
    stores: Arc<RwLock<HashMap<(RangeId, RangeGeneration), FileActiveRangeStore>>>,
    store_open_gate: Arc<Mutex<()>>,
    frozen_generations: Arc<RwLock<HashSet<(RangeId, RangeGeneration)>>>,
    append_gate: Arc<Mutex<()>>,
}

impl ReplicaAppendService {
    pub fn new(
        root: impl Into<PathBuf>,
        local_node: StorageNodeId,
        control: Arc<ControlController>,
    ) -> Self {
        Self {
            root: Arc::new(root.into()),
            local_node,
            control,
            stores: Arc::new(RwLock::new(HashMap::new())),
            store_open_gate: Arc::new(Mutex::new(())),
            frozen_generations: Arc::new(RwLock::new(HashSet::new())),
            append_gate: Arc::new(Mutex::new(())),
        }
    }

    pub fn local_node(&self) -> &StorageNodeId {
        &self.local_node
    }

    pub async fn freeze_generation(&self, range_id: RangeId, generation: RangeGeneration) {
        let _gate = self.append_gate.lock().await;
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
            })
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
        self.store_for(assignment)
            .await?
            .commit(assignment.generation, assignment.ownership_epoch, position)
            .await
            .map_err(map_store_error)
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
        let snapshot = self
            .store_for(assignment)
            .await?
            .snapshot()
            .await
            .map_err(map_store_error)?;
        Ok(super::ReplicaRecoveryStatus {
            node: self.local_node.clone(),
            healthy: true,
            appended: snapshot.progress.appended(),
            committed: snapshot.progress.commit_position(),
        })
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
        let snapshot = self
            .store_for(&assignment)
            .await?
            .snapshot()
            .await
            .map_err(map_store_error)?;
        Ok(super::ReplicaRecoveryStatus {
            node: self.local_node.clone(),
            healthy: true,
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
        let store = self.store_for(&assignment).await?;
        let snapshot = store.snapshot().await.map_err(map_store_error)?;
        if snapshot.progress.commit_position() < committed_prefix {
            store
                .commit(
                    assignment.generation,
                    assignment.ownership_epoch,
                    committed_prefix,
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

    pub async fn append(
        &self,
        request: ReplicaAppendRequest,
    ) -> Result<ReplicaAppendAccepted, ReplicaAppendError> {
        let _gate = self.append_gate.lock().await;
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
        let result = store
            .append(ActiveRangeAppend {
                generation: request.generation,
                ownership_epoch: request.ownership_epoch,
                expected_position: Some(request.expected_position),
                identity: request.identity,
                cursor: request.cursor,
                frame,
            })
            .await
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
        let local_digest = store
            .frame_digest(position)
            .await
            .map_err(map_store_error)?
            .ok_or_else(|| {
                ReplicaAppendError::rejected(
                    ReplicaAppendErrorCode::PositionGap,
                    format!("RangePosition {position} is not durable on this replica"),
                )
            })?;
        if local_digest != request.frame_digest {
            return Err(ReplicaAppendError::rejected(
                ReplicaAppendErrorCode::PositionConflict,
                format!("RangePosition {position} has a different frame digest"),
            ));
        }
        store
            .commit(
                request.generation,
                request.ownership_epoch,
                request.commit_position,
            )
            .await
            .map_err(map_store_error)?;
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
        let opened = tokio::task::spawn_blocking(move || {
            FileActiveRangeStore::open(root.as_ref(), descriptor)
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
                "Node {local} does not have Active Range {}; complete Feed reads require another fully caught-up Node until cross-Node reads are supported",
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
            .update_ownership_epoch(
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
        ActiveRangeStoreError::PositionGap { .. } => {
            ReplicaAppendError::rejected(ReplicaAppendErrorCode::PositionGap, error.to_string())
        }
        ActiveRangeStoreError::PositionConflict(_) => ReplicaAppendError::rejected(
            ReplicaAppendErrorCode::PositionConflict,
            error.to_string(),
        ),
        ActiveRangeStoreError::WriterSequenceConflict
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
        other => ReplicaAppendError::storage(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::active_range::ReplicaSet;

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
}
