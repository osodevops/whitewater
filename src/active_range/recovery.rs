use std::{collections::BTreeMap, sync::Arc, time::Duration};

use tokio::sync::Mutex;

use async_trait::async_trait;
use futures_util::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    control::{Command, ControlController},
    control_plane::ControlPlane,
};

use super::{
    ActiveRangeAssignment, CommitPosition, OwnershipEpoch, RangePosition, StorageNodeId,
    ACTIVE_RANGE_COMMIT_QUORUM,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaRecoveryStatus {
    pub node: StorageNodeId,
    pub healthy: bool,
    pub appended: RangePosition,
    pub committed: CommitPosition,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnerRecoveryPlan {
    pub previous_owner: StorageNodeId,
    pub previous_epoch: OwnershipEpoch,
    pub new_owner: StorageNodeId,
    pub new_epoch: OwnershipEpoch,
    pub committed_prefix: CommitPosition,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum OwnerRecoveryError {
    #[error("current Append Owner is still healthy")]
    OwnerStillHealthy,
    #[error("fewer than two current replicas are healthy")]
    NoHealthyMajority,
    #[error("no healthy caught-up replica can own the committed prefix")]
    NoCaughtUpReplica,
    #[error("ownership epoch overflow")]
    EpochOverflow,
}

pub fn plan_owner_recovery(
    assignment: &ActiveRangeAssignment,
    statuses: &[ReplicaRecoveryStatus],
) -> Result<OwnerRecoveryPlan, OwnerRecoveryError> {
    if statuses
        .iter()
        .any(|status| status.node == assignment.owner && status.healthy)
    {
        return Err(OwnerRecoveryError::OwnerStillHealthy);
    }
    let healthy = statuses
        .iter()
        .filter(|status| status.healthy && assignment.replicas.contains(&status.node))
        .collect::<Vec<_>>();
    if healthy.len() < ACTIVE_RANGE_COMMIT_QUORUM {
        return Err(OwnerRecoveryError::NoHealthyMajority);
    }
    let mut candidates = healthy
        .iter()
        .map(|status| status.committed.value())
        .collect::<Vec<_>>();
    candidates.sort_unstable();
    candidates.dedup();
    let committed_prefix = candidates
        .into_iter()
        .filter(|position| {
            healthy
                .iter()
                .filter(|status| status.committed.value() >= *position)
                .count()
                >= ACTIVE_RANGE_COMMIT_QUORUM
        })
        .max()
        .unwrap_or(0);
    let new_owner = healthy
        .into_iter()
        .filter(|status| status.appended.value() >= committed_prefix)
        .max_by_key(|status| {
            (
                status.committed.value(),
                status.appended.value(),
                std::cmp::Reverse(status.node.clone()),
            )
        })
        .ok_or(OwnerRecoveryError::NoCaughtUpReplica)?
        .node
        .clone();
    let new_epoch = assignment
        .ownership_epoch
        .checked_next()
        .map_err(|_| OwnerRecoveryError::EpochOverflow)?;
    Ok(OwnerRecoveryPlan {
        previous_owner: assignment.owner.clone(),
        previous_epoch: assignment.ownership_epoch,
        new_owner,
        new_epoch,
        committed_prefix: CommitPosition::new(committed_prefix),
    })
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaProgressRequest {
    pub feed_id: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaProgressResponse {
    pub status: Option<ReplicaRecoveryStatus>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaReconcileRequest {
    pub feed_id: Uuid,
    pub committed_prefix: CommitPosition,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaReconcileResponse {
    pub removed_records: Option<u64>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Error)]
pub enum RecoveryExecutionError {
    #[error("Feed or Active Range assignment is unavailable")]
    AssignmentUnavailable,
    #[error("recovery planning failed: {0}")]
    Planning(#[from] OwnerRecoveryError),
    #[error("Control Plane recovery command failed: {0}")]
    Control(String),
    #[error("replica recovery failed: {0}")]
    Replica(String),
}

#[async_trait]
pub trait RecoveryTransport: Send + Sync {
    async fn progress(
        &self,
        node: &StorageNodeId,
        feed_id: Uuid,
    ) -> Result<ReplicaRecoveryStatus, String>;

    async fn reconcile(
        &self,
        node: &StorageNodeId,
        request: ReplicaReconcileRequest,
    ) -> Result<u64, String>;
}

#[derive(Clone)]
pub struct OwnerRecoveryExecutor {
    control: Arc<ControlController>,
    control_plane: Arc<ControlPlane>,
    transport: Arc<dyn RecoveryTransport>,
}

impl OwnerRecoveryExecutor {
    pub fn new(
        control: Arc<ControlController>,
        control_plane: Arc<ControlPlane>,
        transport: Arc<dyn RecoveryTransport>,
    ) -> Self {
        Self {
            control,
            control_plane,
            transport,
        }
    }

    pub async fn recover(
        &self,
        feed_name: &str,
        failed_owner: &StorageNodeId,
    ) -> Result<OwnerRecoveryPlan, RecoveryExecutionError> {
        let feed = self
            .control
            .active_feed_by_name(feed_name)
            .await
            .ok_or(RecoveryExecutionError::AssignmentUnavailable)?;
        let assignment = self
            .control
            .active_range_assignment(feed.feed_id)
            .await
            .ok_or(RecoveryExecutionError::AssignmentUnavailable)?;
        if &assignment.owner != failed_owner {
            return Err(RecoveryExecutionError::Control(
                "owner changed before recovery began".to_owned(),
            ));
        }
        let mut statuses = Vec::new();
        for node in assignment.replicas.iter() {
            if node == failed_owner {
                statuses.push(ReplicaRecoveryStatus {
                    node: node.clone(),
                    healthy: false,
                    appended: RangePosition::new(0),
                    committed: CommitPosition::new(0),
                });
            } else if let Ok(status) = self.transport.progress(node, feed.feed_id).await {
                statuses.push(status);
            }
        }
        let plan = plan_owner_recovery(&assignment, &statuses)?;
        self.control_plane
            .execute_commands(vec![Command::RecoverActiveRangeOwnership {
                feed: feed_name.to_owned(),
                expected_owner: plan.previous_owner.clone(),
                expected_epoch: plan.previous_epoch,
                new_owner: plan.new_owner.clone(),
            }])
            .await
            .map_err(|error| RecoveryExecutionError::Control(error.to_string()))?;
        let request = ReplicaReconcileRequest {
            feed_id: feed.feed_id,
            committed_prefix: plan.committed_prefix,
        };
        for status in statuses.iter().filter(|status| status.healthy) {
            self.transport
                .reconcile(&status.node, request.clone())
                .await
                .map_err(RecoveryExecutionError::Replica)?;
        }
        Ok(plan)
    }
}

#[derive(Clone)]
pub struct RecoverySupervisor {
    control: Arc<ControlController>,
    control_plane: Arc<ControlPlane>,
    transport: Arc<dyn RecoveryTransport>,
    executor: OwnerRecoveryExecutor,
    threshold: u32,
    failures: Arc<Mutex<BTreeMap<Uuid, (StorageNodeId, u32)>>>,
}

async fn probe_active_owners(
    transport: &dyn RecoveryTransport,
    assignments: Vec<(String, ActiveRangeAssignment)>,
) -> Vec<(String, ActiveRangeAssignment, bool)> {
    stream::iter(assignments)
        .map(|(feed_name, assignment)| async move {
            let healthy = transport
                .progress(&assignment.owner, assignment.feed_id)
                .await
                .is_ok();
            (feed_name, assignment, healthy)
        })
        .buffer_unordered(16)
        .collect()
        .await
}

impl RecoverySupervisor {
    pub fn new(
        control: Arc<ControlController>,
        control_plane: Arc<ControlPlane>,
        transport: Arc<dyn RecoveryTransport>,
        threshold: u32,
    ) -> Self {
        Self {
            executor: OwnerRecoveryExecutor::new(
                control.clone(),
                control_plane.clone(),
                transport.clone(),
            ),
            control,
            control_plane,
            transport,
            threshold: threshold.max(1),
            failures: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub async fn tick(&self) -> Vec<Result<OwnerRecoveryPlan, RecoveryExecutionError>> {
        if self.control_plane.status().await.state != "leader" {
            return Vec::new();
        }
        let mut pending = Vec::new();
        let probes = probe_active_owners(
            self.transport.as_ref(),
            self.control.active_feed_assignments().await,
        )
        .await;
        for (feed_name, assignment, healthy) in probes {
            let should_recover = {
                let mut failures = self.failures.lock().await;
                if healthy {
                    failures.remove(&assignment.feed_id);
                    false
                } else {
                    let entry = failures
                        .entry(assignment.feed_id)
                        .or_insert((assignment.owner.clone(), 0));
                    if entry.0 != assignment.owner {
                        *entry = (assignment.owner.clone(), 0);
                    }
                    entry.1 = entry.1.saturating_add(1);
                    entry.1 >= self.threshold
                }
            };
            if should_recover {
                pending.push((feed_name, assignment));
            }
        }
        stream::iter(pending)
            .map(|(feed_name, assignment)| async move {
                let result = self.executor.recover(&feed_name, &assignment.owner).await;
                self.failures.lock().await.remove(&assignment.feed_id);
                result
            })
            .buffer_unordered(4)
            .collect()
            .await
    }
}

#[derive(Clone)]
pub struct HttpRecoveryTransport {
    endpoints: Arc<crate::internal_plane::InternalEndpoints>,
    key: String,
    http: reqwest::Client,
}

impl HttpRecoveryTransport {
    pub fn new(
        endpoints: impl Into<crate::internal_plane::InternalEndpoints>,
        key: String,
        timeout: Duration,
    ) -> Result<Self, reqwest::Error> {
        Self::with_client(
            endpoints,
            key,
            reqwest::Client::builder().timeout(timeout).build()?,
        )
    }

    pub fn with_client(
        endpoints: impl Into<crate::internal_plane::InternalEndpoints>,
        key: String,
        http: reqwest::Client,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            endpoints: Arc::new(endpoints.into()),
            key,
            http,
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
            .resolve(node)
            .await
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

#[async_trait]
impl RecoveryTransport for HttpRecoveryTransport {
    async fn progress(
        &self,
        node: &StorageNodeId,
        feed_id: Uuid,
    ) -> Result<ReplicaRecoveryStatus, String> {
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

    async fn reconcile(
        &self,
        node: &StorageNodeId,
        request: ReplicaReconcileRequest,
    ) -> Result<u64, String> {
        let response: ReplicaReconcileResponse = self
            .post(node, "/internal/active-range/recovery/reconcile", &request)
            .await?;
        response.removed_records.ok_or_else(|| {
            response
                .error
                .unwrap_or_else(|| "missing reconcile result".to_owned())
        })
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::Notify;

    use super::*;
    use crate::active_range::{RangeGeneration, RangeId, ReplicaSet};

    struct BlockedProbe {
        first: Uuid,
        first_seen: Arc<Notify>,
        second_seen: Arc<Notify>,
        release_first: Arc<Notify>,
    }

    #[async_trait]
    impl RecoveryTransport for BlockedProbe {
        async fn progress(
            &self,
            _node: &StorageNodeId,
            feed_id: Uuid,
        ) -> Result<ReplicaRecoveryStatus, String> {
            if feed_id == self.first {
                self.first_seen.notify_one();
                self.release_first.notified().await;
            } else {
                self.second_seen.notify_one();
            }
            Err("owner unavailable".to_owned())
        }

        async fn reconcile(
            &self,
            _node: &StorageNodeId,
            _request: ReplicaReconcileRequest,
        ) -> Result<u64, String> {
            Err("reconcile is not used by a health probe".to_owned())
        }
    }

    #[tokio::test]
    async fn a_slow_owner_probe_does_not_block_other_feeds() {
        let first = Uuid::from_u128(1);
        let transport = Arc::new(BlockedProbe {
            first,
            first_seen: Arc::new(Notify::new()),
            second_seen: Arc::new(Notify::new()),
            release_first: Arc::new(Notify::new()),
        });
        let assignments = [first, Uuid::from_u128(2)]
            .into_iter()
            .map(|feed_id| {
                let owner = StorageNodeId::try_new("storage-1").unwrap();
                let assignment = ActiveRangeAssignment::try_new(
                    feed_id,
                    RangeId::from_uuid(Uuid::new_v4()),
                    RangeGeneration::new(1),
                    owner.clone(),
                    ReplicaSet::try_new([
                        owner,
                        StorageNodeId::try_new("storage-2").unwrap(),
                        StorageNodeId::try_new("storage-3").unwrap(),
                    ])
                    .unwrap(),
                    OwnershipEpoch::new(1),
                )
                .unwrap();
                (format!("feed-{feed_id}"), assignment)
            })
            .collect();
        let probe = transport.clone();
        let task =
            tokio::spawn(async move { probe_active_owners(probe.as_ref(), assignments).await });
        tokio::time::timeout(Duration::from_secs(1), transport.first_seen.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), transport.second_seen.notified())
            .await
            .unwrap();
        transport.release_first.notify_one();
        let results = task.await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|(_, _, healthy)| !healthy));
    }
}
