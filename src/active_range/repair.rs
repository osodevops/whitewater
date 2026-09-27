use std::{collections::BTreeMap, sync::Arc, time::Duration};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::control::ControlController;

use super::{
    ActiveRangeAssignment, CommitPosition, RangePosition, ReplicaAppendRequest,
    ReplicaAppendService, ReplicaCommitRequest, ReplicaProgressRequest, ReplicaProgressResponse,
    StorageNodeId,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaRepairProgress {
    pub source_committed: CommitPosition,
    pub target_committed: CommitPosition,
    pub transferred_records: u64,
    pub transferred_bytes: u64,
    pub quarantined_corruption: bool,
    pub limiting_resource: Option<String>,
    pub ready: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepairExportRequest {
    pub feed_id: Uuid,
    pub after: Option<RangePosition>,
    pub limit: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepairFrame {
    pub position: RangePosition,
    pub identity: super::AppendIdentity,
    pub cursor: String,
    pub frame_base64: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepairExportResponse {
    pub frames: Vec<RepairFrame>,
    pub error: Option<String>,
}

#[derive(Debug, Error)]
pub enum ReplicaRepairError {
    #[error("source replica is unavailable or corrupt: {0}")]
    Source(String),
    #[error("target replica repair failed: {0}")]
    Target(String),
    #[error("target replica is ahead of the verified source")]
    TargetAhead,
    #[error("transferred frame position or checksum did not match")]
    VerificationFailed,
}

pub async fn repair_replica(
    assignment: &ActiveRangeAssignment,
    source: Arc<ReplicaAppendService>,
    target: Arc<ReplicaAppendService>,
    batch_size: usize,
) -> Result<ReplicaRepairProgress, ReplicaRepairError> {
    let source_status = source
        .recovery_status(assignment.feed_id)
        .await
        .map_err(|error| ReplicaRepairError::Source(error.to_string()))?;
    let mut quarantined = false;
    let mut target_status = match target.recovery_status(assignment.feed_id).await {
        Ok(status) => status,
        Err(_) => {
            target
                .quarantine_for_repair(assignment.feed_id)
                .await
                .map_err(|error| ReplicaRepairError::Target(error.to_string()))?;
            quarantined = true;
            target
                .recovery_status(assignment.feed_id)
                .await
                .map_err(|error| ReplicaRepairError::Target(error.to_string()))?
        }
    };
    if target_status.committed > source_status.committed {
        return Err(ReplicaRepairError::TargetAhead);
    }
    target
        .reconcile_recovery(assignment.feed_id, target_status.committed)
        .await
        .map_err(|error| ReplicaRepairError::Target(error.to_string()))?;
    target_status = target
        .recovery_status(assignment.feed_id)
        .await
        .map_err(|error| ReplicaRepairError::Target(error.to_string()))?;
    let mut after = (target_status.appended.value() > 0).then_some(target_status.appended);
    let mut transferred_records = 0_u64;
    let mut transferred_bytes = 0_u64;
    loop {
        let frames = source
            .export_committed(assignment.feed_id, after, batch_size.clamp(1, 10_000))
            .await
            .map_err(|error| ReplicaRepairError::Source(error.to_string()))?;
        if frames.is_empty() {
            break;
        }
        for item in frames {
            let digest = *blake3::hash(&item.frame).as_bytes();
            let accepted = target
                .append(ReplicaAppendRequest {
                    feed_id: assignment.feed_id,
                    range_id: assignment.range_id,
                    generation: assignment.generation,
                    ownership_epoch: assignment.ownership_epoch,
                    append_owner: assignment.owner.clone(),
                    expected_position: item.position,
                    identity: item.identity,
                    cursor: item.cursor,
                    frame_base64: STANDARD.encode(&item.frame),
                })
                .await
                .map_err(|error| ReplicaRepairError::Target(error.to_string()))?;
            if accepted.position != item.position || accepted.frame_digest != digest {
                return Err(ReplicaRepairError::VerificationFailed);
            }
            target
                .commit(ReplicaCommitRequest {
                    feed_id: assignment.feed_id,
                    range_id: assignment.range_id,
                    generation: assignment.generation,
                    ownership_epoch: assignment.ownership_epoch,
                    append_owner: assignment.owner.clone(),
                    commit_position: CommitPosition::new(item.position.value()),
                    frame_digest: digest,
                })
                .await
                .map_err(|error| ReplicaRepairError::Target(error.to_string()))?;
            transferred_records = transferred_records.saturating_add(1);
            transferred_bytes = transferred_bytes.saturating_add(item.frame.len() as u64);
            after = Some(item.position);
        }
    }
    let repaired = target
        .recovery_status(assignment.feed_id)
        .await
        .map_err(|error| ReplicaRepairError::Target(error.to_string()))?;
    let ready = repaired.committed == source_status.committed
        && repaired.appended.value() >= repaired.committed.value();
    Ok(ReplicaRepairProgress {
        source_committed: source_status.committed,
        target_committed: repaired.committed,
        transferred_records,
        transferred_bytes,
        quarantined_corruption: quarantined,
        limiting_resource: (transferred_records > 0).then(|| "network_or_source_read".to_owned()),
        ready,
    })
}

#[derive(Clone)]
pub struct LocalRepairSupervisor {
    local_node: StorageNodeId,
    local: Arc<ReplicaAppendService>,
    control: Arc<ControlController>,
    endpoints: Arc<BTreeMap<StorageNodeId, String>>,
    key: String,
    http: reqwest::Client,
    batch_size: usize,
}

impl LocalRepairSupervisor {
    pub fn new(
        local_node: StorageNodeId,
        local: Arc<ReplicaAppendService>,
        control: Arc<ControlController>,
        endpoints: BTreeMap<StorageNodeId, String>,
        key: String,
        timeout: Duration,
        batch_size: usize,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            local_node,
            local,
            control,
            endpoints: Arc::new(endpoints),
            key,
            http: reqwest::Client::builder().timeout(timeout).build()?,
            batch_size: batch_size.clamp(1, 10_000),
        })
    }

    pub async fn tick(&self) -> Vec<Result<ReplicaRepairProgress, ReplicaRepairError>> {
        let mut results = Vec::new();
        for (_, assignment) in self.control.active_feed_assignments().await {
            if !assignment.replicas.contains(&self.local_node)
                || assignment.owner == self.local_node
            {
                continue;
            }
            let source = match self
                .remote_progress(&assignment.owner, assignment.feed_id)
                .await
            {
                Ok(status) => status,
                Err(_) => continue,
            };
            let mut quarantined = false;
            let local = match self.local.recovery_status(assignment.feed_id).await {
                Ok(status) => status,
                Err(_) => {
                    match self.local.quarantine_for_repair(assignment.feed_id).await {
                        Ok(_) => quarantined = true,
                        Err(error) => {
                            results.push(Err(ReplicaRepairError::Target(error.to_string())));
                            continue;
                        }
                    }
                    match self.local.recovery_status(assignment.feed_id).await {
                        Ok(status) => status,
                        Err(error) => {
                            results.push(Err(ReplicaRepairError::Target(error.to_string())));
                            continue;
                        }
                    }
                }
            };
            if local.committed >= source.committed {
                continue;
            }
            results.push(
                self.catch_up(&assignment, source.committed, local.appended, quarantined)
                    .await,
            );
        }
        results
    }

    async fn remote_progress(
        &self,
        node: &StorageNodeId,
        feed_id: Uuid,
    ) -> Result<super::ReplicaRecoveryStatus, String> {
        let response: ReplicaProgressResponse = self
            .post(
                node,
                "/internal/active-range/recovery/progress",
                &ReplicaProgressRequest { feed_id },
            )
            .await?;
        response.status.ok_or_else(|| {
            response
                .error
                .unwrap_or_else(|| "missing progress".to_owned())
        })
    }

    async fn catch_up(
        &self,
        assignment: &ActiveRangeAssignment,
        source_committed: CommitPosition,
        mut after: RangePosition,
        quarantined: bool,
    ) -> Result<ReplicaRepairProgress, ReplicaRepairError> {
        let mut transferred_records = 0_u64;
        let mut transferred_bytes = 0_u64;
        while after.value() < source_committed.value() {
            let response: RepairExportResponse = self
                .post(
                    &assignment.owner,
                    "/internal/active-range/repair/export",
                    &RepairExportRequest {
                        feed_id: assignment.feed_id,
                        after: (after.value() > 0).then_some(after),
                        limit: self.batch_size,
                    },
                )
                .await
                .map_err(ReplicaRepairError::Source)?;
            if let Some(error) = response.error {
                return Err(ReplicaRepairError::Source(error));
            }
            if response.frames.is_empty() {
                return Err(ReplicaRepairError::Source(
                    "source returned no frames before its CommitPosition".to_owned(),
                ));
            }
            for item in response.frames {
                let frame = STANDARD
                    .decode(&item.frame_base64)
                    .map_err(|error| ReplicaRepairError::Source(error.to_string()))?;
                let digest = *blake3::hash(&frame).as_bytes();
                let accepted = self
                    .local
                    .append(ReplicaAppendRequest {
                        feed_id: assignment.feed_id,
                        range_id: assignment.range_id,
                        generation: assignment.generation,
                        ownership_epoch: assignment.ownership_epoch,
                        append_owner: assignment.owner.clone(),
                        expected_position: item.position,
                        identity: item.identity,
                        cursor: item.cursor,
                        frame_base64: item.frame_base64,
                    })
                    .await
                    .map_err(|error| ReplicaRepairError::Target(error.to_string()))?;
                if accepted.frame_digest != digest || accepted.position != item.position {
                    return Err(ReplicaRepairError::VerificationFailed);
                }
                self.local
                    .commit(ReplicaCommitRequest {
                        feed_id: assignment.feed_id,
                        range_id: assignment.range_id,
                        generation: assignment.generation,
                        ownership_epoch: assignment.ownership_epoch,
                        append_owner: assignment.owner.clone(),
                        commit_position: CommitPosition::new(item.position.value()),
                        frame_digest: digest,
                    })
                    .await
                    .map_err(|error| ReplicaRepairError::Target(error.to_string()))?;
                after = item.position;
                transferred_records = transferred_records.saturating_add(1);
                transferred_bytes = transferred_bytes.saturating_add(frame.len() as u64);
            }
        }
        Ok(ReplicaRepairProgress {
            source_committed,
            target_committed: CommitPosition::new(after.value()),
            transferred_records,
            transferred_bytes,
            quarantined_corruption: quarantined,
            limiting_resource: (transferred_records > 0)
                .then(|| "network_or_source_read".to_owned()),
            ready: after.value() == source_committed.value(),
        })
    }

    async fn post<Request, Response>(
        &self,
        node: &StorageNodeId,
        path: &str,
        request: &Request,
    ) -> Result<Response, String>
    where
        Request: Serialize + Sync,
        Response: for<'de> Deserialize<'de>,
    {
        let endpoint = self
            .endpoints
            .get(node)
            .ok_or_else(|| format!("Node {node} has no endpoint"))?;
        self.http
            .post(format!("{}{}", endpoint.trim_end_matches('/'), path))
            .header("x-whitewater-control-key", &self.key)
            .json(request)
            .send()
            .await
            .map_err(|error| error.to_string())?
            .json()
            .await
            .map_err(|error| error.to_string())
    }
}
