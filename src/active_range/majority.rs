use std::{fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::control::{unix_ns, ControlController, RangeCommitProof};

use super::{
    commit_proof::CommitProofReporter, CommitPosition, ReplicaAppendAccepted,
    ReplicaAppendErrorCode, ReplicaAppendRequest, ReplicaAppendResponse, ReplicaAppendService,
    ReplicaCommitAccepted, ReplicaCommitRequest, ReplicaCommitResponse,
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

#[derive(Clone)]
pub struct MajorityAppendCoordinator {
    local: Arc<ReplicaAppendService>,
    control: Arc<ControlController>,
    transport: Arc<dyn ReplicaTransport>,
    append_lock: Arc<Mutex<()>>,
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
            append_lock: Arc::new(Mutex::new(())),
            proofs: None,
        }
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

    async fn freeze_for_cutover(
        &self,
        source: &super::ActiveRangeAssignment,
    ) -> Result<CommitPosition, MajorityAppendError> {
        let _append_guard = self.append_lock.lock().await;
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
        mut request: ReplicaAppendRequest,
    ) -> Result<MajorityAppendResult, MajorityAppendError> {
        let _append_guard = self.append_lock.lock().await;
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
            return Err(self.error(
                MajorityAppendErrorCode::NotCurrentOwner,
                format!(
                    "Node {} is not the current Append Owner",
                    self.local.local_node()
                ),
                false,
                vec![],
                vec![],
            ));
        }
        let local = self.local.append(request.clone()).await.map_err(|error| {
            if error.code == ReplicaAppendErrorCode::PendingEffectOccupied {
                return self.occupied_error(error.pending_effect);
            }
            self.error(
                MajorityAppendErrorCode::LocalStorageFailure,
                error.to_string(),
                error.retryable,
                vec![],
                vec![],
            )
        })?;
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
        for (node, result) in follower_results {
            if let Ok(accepted) = result {
                if accepted.position != local.position
                    || accepted.frame_digest != local.frame_digest
                {
                    return Err(self.error(
                        MajorityAppendErrorCode::FrameConflict,
                        format!("replica {node} acknowledged different bytes or position"),
                        false,
                        durable,
                        vec![],
                    ));
                }
                durable.push(node.clone());
                durable_followers.push(node);
            }
        }
        if durable.len() < ACTIVE_RANGE_COMMIT_QUORUM {
            return Err(self.error(
                MajorityAppendErrorCode::NoDurableMajority,
                "append is durable on fewer than two current replicas",
                true,
                durable,
                vec![],
            ));
        }
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
        if durable_followers.len() == 1 {
            if self
                .transport
                .commit(&durable_followers[0], commit_request.clone())
                .await
                .is_ok()
            {
                evidence.push(durable_followers[0].clone());
            }
        } else {
            let first = self
                .transport
                .commit(&durable_followers[0], commit_request.clone());
            let second = self
                .transport
                .commit(&durable_followers[1], commit_request.clone());
            let (first_result, second_result) = tokio::join!(first, second);
            if first_result.is_ok() {
                evidence.push(durable_followers[0].clone());
            }
            if second_result.is_ok() {
                evidence.push(durable_followers[1].clone());
            }
        }
        if evidence.is_empty() {
            return Err(self.error(
                MajorityAppendErrorCode::NoCommitMajority,
                "frame majority exists but no follower persisted CommitPosition evidence; retry with the same append identity",
                true,
                durable,
                evidence,
            ));
        }
        self.local.commit(commit_request).await.map_err(|error| {
            self.error(
                MajorityAppendErrorCode::LocalStorageFailure,
                error.to_string(),
                error.retryable,
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
        let _append_guard = self.append_lock.lock().await;
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
        let _append_guard = self.append_lock.lock().await;
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
            .map_err(|error| ReplicaTransportError {
                message: error.to_string(),
                retryable: true,
            })?;
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
        let endpoint =
            self.endpoints
                .resolve(replica)
                .await
                .ok_or_else(|| ReplicaTransportError {
                    message: format!("replica {replica} has no configured endpoint"),
                    retryable: false,
                })?;
        let response = self
            .http
            .post(format!("{}{}", endpoint.trim_end_matches('/'), path))
            .header("x-whitewater-control-key", &self.key)
            .json(request)
            .send()
            .await
            .map_err(|error| ReplicaTransportError {
                message: error.to_string(),
                retryable: true,
            })?;
        if !response.status().is_success() {
            return Err(ReplicaTransportError {
                message: format!("replica {replica} returned HTTP {}", response.status()),
                retryable: response.status().is_server_error(),
            });
        }
        response
            .json()
            .await
            .map_err(|error| ReplicaTransportError {
                message: error.to_string(),
                retryable: true,
            })
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
            (None, Some(error)) => Err(ReplicaTransportError {
                message: error.message,
                retryable: error.retryable,
            }),
            (None, None) => Err(ReplicaTransportError {
                message: "replica returned no append result".to_owned(),
                retryable: true,
            }),
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
            (None, Some(error)) => Err(ReplicaTransportError {
                message: error.message,
                retryable: error.retryable,
            }),
            (None, None) => Err(ReplicaTransportError {
                message: "replica returned no commit result".to_owned(),
                retryable: true,
            }),
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
            (None, Some(error)) => Err(ReplicaTransportError {
                message: error.message.clone(),
                retryable: error.retryable,
            }),
            (None, None) => Err(ReplicaTransportError {
                message: "replica returned no discard outcome".to_owned(),
                retryable: true,
            }),
        }
    }
}
