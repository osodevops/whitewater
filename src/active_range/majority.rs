use std::{collections::BTreeMap, fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::control::ControlController;

use super::{
    CommitPosition, ReplicaAppendAccepted, ReplicaAppendRequest, ReplicaAppendResponse,
    ReplicaAppendService, ReplicaCommitAccepted, ReplicaCommitRequest, ReplicaCommitResponse,
    StorageNodeId, ACTIVE_RANGE_COMMIT_QUORUM,
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
    LocalStorageFailure,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MajorityAppendError {
    pub code: MajorityAppendErrorCode,
    pub message: String,
    pub retryable: bool,
    pub durable_replicas: Vec<StorageNodeId>,
    pub commit_evidence: Vec<StorageNodeId>,
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
}

#[derive(Clone)]
pub struct MajorityAppendCoordinator {
    local: Arc<ReplicaAppendService>,
    control: Arc<ControlController>,
    transport: Arc<dyn ReplicaTransport>,
    append_lock: Arc<Mutex<()>>,
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
        }
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
        }
    }
}

#[derive(Clone)]
pub struct HttpReplicaTransport {
    endpoints: Arc<BTreeMap<StorageNodeId, String>>,
    key: String,
    http: reqwest::Client,
}

impl HttpReplicaTransport {
    pub fn new(
        endpoints: BTreeMap<StorageNodeId, String>,
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
        Ok(Self {
            endpoints: Arc::new(endpoints),
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
        let endpoint = self
            .endpoints
            .get(replica)
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
}
