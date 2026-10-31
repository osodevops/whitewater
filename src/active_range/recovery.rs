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
    ActiveRangeAssignment, CommitPosition, OwnershipEpoch, RangeId, RangePosition, StorageNodeId,
    ACTIVE_RANGE_COMMIT_QUORUM,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaRecoveryStatus {
    pub node: StorageNodeId,
    pub healthy: bool,
    /// The replica proved a committed-divergent occupant while owning the
    /// range: it must not stay owner, but it is alive and can serve quorum
    /// as a replica once its divergent pending tail is truncated.
    #[serde(default)]
    pub diverged: bool,
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
    #[error("current Append Owner is not healthy")]
    OwnerUnhealthy,
    #[error("fewer than two current replicas are healthy")]
    NoHealthyMajority,
    #[error("no healthy caught-up replica can own the committed prefix")]
    NoCaughtUpReplica,
    #[error("no healthy replica is committed ahead of the current Append Owner")]
    NoAheadReplica,
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
    // The post-move set must be able to form commit quorum: healthy
    // replicas plus the diverged owner — it proved a committed fork, so it
    // can never hold ownership under this plan, but it is a live replica
    // whose divergent tail truncates on reconcile and rejoins quorum.
    // Replicas that are unreachable or persistence-failed count for
    // neither.
    let alive = statuses
        .iter()
        .filter(|status| {
            (status.healthy || status.diverged) && assignment.replicas.contains(&status.node)
        })
        .count();
    if alive < ACTIVE_RANGE_COMMIT_QUORUM || healthy.is_empty() {
        return Err(OwnerRecoveryError::NoHealthyMajority);
    }
    // A replica only advances its CommitPosition from verified quorum
    // commit evidence, so even a single live replica reporting a position
    // proves it was committed. The prefix must be the maximum reported:
    // understating it would let the new owner allocate over a committed
    // tail whose marker survived on just one replica.
    let committed_prefix = healthy
        .iter()
        .map(|status| status.committed.value())
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

/// The live-owner variant: a healthy replica holding a strictly larger
/// committed frontier than a healthy owner means quorum-committed
/// history exists that the owner cannot see — every append it attempts
/// lands below the replica's committed marker and every quorum read
/// fails closed. Ownership must move to the replica carrying the proof
/// (a CommitPosition only advances on verified quorum evidence) and the
/// shared reconcile then converges the set. An unhealthy owner routes
/// to `plan_owner_recovery` instead.
pub fn plan_behind_owner_recovery(
    assignment: &ActiveRangeAssignment,
    statuses: &[ReplicaRecoveryStatus],
) -> Result<OwnerRecoveryPlan, OwnerRecoveryError> {
    let owner = statuses
        .iter()
        .find(|status| status.node == assignment.owner)
        .ok_or(OwnerRecoveryError::NoCaughtUpReplica)?;
    if !owner.healthy {
        return Err(OwnerRecoveryError::OwnerUnhealthy);
    }
    let new_owner = statuses
        .iter()
        .filter(|status| {
            status.healthy
                && status.node != owner.node
                && assignment.replicas.contains(&status.node)
                && status.committed > owner.committed
        })
        .max_by_key(|status| {
            (
                status.committed.value(),
                status.appended.value(),
                std::cmp::Reverse(status.node.clone()),
            )
        })
        .ok_or(OwnerRecoveryError::NoAheadReplica)?;
    let new_epoch = assignment
        .ownership_epoch
        .checked_next()
        .map_err(|_| OwnerRecoveryError::EpochOverflow)?;
    Ok(OwnerRecoveryPlan {
        previous_owner: assignment.owner.clone(),
        previous_epoch: assignment.ownership_epoch,
        new_owner: new_owner.node.clone(),
        new_epoch,
        committed_prefix: new_owner.committed,
    })
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaProgressRequest {
    pub feed_id: Uuid,
    /// Exact Active Range to probe; multi-range Feeds must pass this or the
    /// receiver resolves the Feed's first range instead.
    #[serde(default)]
    pub range_id: Option<RangeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaProgressResponse {
    pub status: Option<ReplicaRecoveryStatus>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaReconcileRequest {
    pub feed_id: Uuid,
    /// Exact Active Range to reconcile; multi-range Feeds must pass this or
    /// the receiver resolves the Feed's first range instead.
    #[serde(default)]
    pub range_id: Option<RangeId>,
    pub committed_prefix: CommitPosition,
    /// Only the evidence-holding new owner may commit local frames to the
    /// prefix; other replicas truncate their uncommitted tail and converge
    /// through verified committed import, so a divergent pending tail can
    /// never be promoted into committed history.
    #[serde(default)]
    pub authoritative: bool,
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
        assignment: &ActiveRangeAssignment,
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

    async fn replica_statuses(
        &self,
        assignment: &ActiveRangeAssignment,
        presumed_dead: Option<&StorageNodeId>,
    ) -> Vec<ReplicaRecoveryStatus> {
        let mut statuses = Vec::new();
        for node in assignment.replicas.iter() {
            if Some(node) == presumed_dead {
                statuses.push(ReplicaRecoveryStatus {
                    node: node.clone(),
                    healthy: false,
                    diverged: false,
                    appended: RangePosition::new(0),
                    committed: CommitPosition::new(0),
                });
            } else if let Ok(status) = self.transport.progress(node, assignment).await {
                statuses.push(status);
            }
        }
        statuses
    }

    async fn apply_plan(
        &self,
        feed: &crate::control::FeedDefinition,
        feed_name: &str,
        assignment: &ActiveRangeAssignment,
        plan: &OwnerRecoveryPlan,
        statuses: &[ReplicaRecoveryStatus],
    ) -> Result<(), RecoveryExecutionError> {
        self.control_plane
            .execute_commands(vec![Command::RecoverActiveRangeOwnership {
                feed: feed_name.to_owned(),
                expected_owner: plan.previous_owner.clone(),
                expected_epoch: plan.previous_epoch,
                new_owner: plan.new_owner.clone(),
            }])
            .await
            .map_err(|error| RecoveryExecutionError::Control(error.to_string()))?;
        // Diverged owners reconcile too: the CAS has moved ownership, so
        // their store fences the stale epoch and the non-authoritative arm
        // truncates the pending tail that forked against committed history.
        for status in statuses
            .iter()
            .filter(|status| status.healthy || status.diverged)
        {
            let request = ReplicaReconcileRequest {
                feed_id: feed.feed_id,
                range_id: Some(assignment.range_id),
                committed_prefix: plan.committed_prefix,
                authoritative: status.node == plan.new_owner,
            };
            self.transport
                .reconcile(&status.node, request)
                .await
                .map_err(RecoveryExecutionError::Replica)?;
        }
        Ok(())
    }

    pub async fn recover(
        &self,
        feed_name: &str,
        assignment: &ActiveRangeAssignment,
        failed_owner: &StorageNodeId,
    ) -> Result<OwnerRecoveryPlan, RecoveryExecutionError> {
        let feed = self
            .control
            .active_feed_by_name(feed_name)
            .await
            .ok_or(RecoveryExecutionError::AssignmentUnavailable)?;
        if &assignment.owner != failed_owner {
            return Err(RecoveryExecutionError::Control(
                "owner changed before recovery began".to_owned(),
            ));
        }
        let statuses = self.replica_statuses(assignment, Some(failed_owner)).await;
        let plan = plan_owner_recovery(assignment, &statuses)?;
        self.apply_plan(&feed, feed_name, assignment, &plan, &statuses)
            .await?;
        Ok(plan)
    }

    /// Recover a live owner that a healthy replica's committed frontier
    /// has passed: the owner stays reachable, so the supervisor only
    /// escalates after the skew persists across its failure threshold —
    /// an in-flight commit briefly looks identical but resolves itself.
    pub async fn recover_behind(
        &self,
        feed_name: &str,
        assignment: &ActiveRangeAssignment,
        current_owner: &StorageNodeId,
    ) -> Result<OwnerRecoveryPlan, RecoveryExecutionError> {
        let feed = self
            .control
            .active_feed_by_name(feed_name)
            .await
            .ok_or(RecoveryExecutionError::AssignmentUnavailable)?;
        if &assignment.owner != current_owner {
            return Err(RecoveryExecutionError::Control(
                "owner changed before recovery began".to_owned(),
            ));
        }
        let statuses = self.replica_statuses(assignment, None).await;
        let plan = plan_behind_owner_recovery(assignment, &statuses)?;
        self.apply_plan(&feed, feed_name, assignment, &plan, &statuses)
            .await?;
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
    failures: Arc<Mutex<BTreeMap<Uuid, OwnerWatch>>>,
}

/// Per-feed recovery watch: consecutive unhealthy-or-wedged probes plus
/// the last owner committed marker observed. The behind-owner signal
/// only accrues while the owner's own committed frontier stays flat —
/// a healthy append cycle always leaves a window where a follower's
/// persisted CommitPosition transiently leads the owner's, so raw skew
/// alone must never move ownership.
struct OwnerWatch {
    owner: StorageNodeId,
    failures: u32,
    last_owner_committed: Option<CommitPosition>,
}

/// Sustained-failure bookkeeping for one feed, called only when the
/// probe was unhealthy OR behind-skewed. An unreachable owner always
/// accrues; a healthy-but-behind owner accrues only while its committed
/// marker is frozen against the last sighting — any advance re-baselines
/// and clears the streak. Returns whether the streak earns a recovery
/// attempt.
fn accrue_failure(
    watch: &mut OwnerWatch,
    owner: &StorageNodeId,
    owner_healthy: bool,
    owner_committed: Option<CommitPosition>,
    threshold: u32,
) -> bool {
    if watch.owner != *owner {
        *watch = OwnerWatch {
            owner: owner.clone(),
            failures: 0,
            last_owner_committed: None,
        };
    }
    if owner_healthy {
        if watch.last_owner_committed == owner_committed {
            watch.failures = watch.failures.saturating_add(1);
        } else {
            watch.failures = 0;
        }
        watch.last_owner_committed = owner_committed;
    } else {
        watch.failures = watch.failures.saturating_add(1);
    }
    watch.failures >= threshold
}

/// Per-assignment probe snapshot: the owner's status (None = unreachable)
/// plus every replica's status, so the supervisor can distinguish a dead
/// owner from a live owner wedged behind a healthier replica.
struct AssignmentProbe {
    owner: Option<ReplicaRecoveryStatus>,
    replicas: Vec<ReplicaRecoveryStatus>,
}

async fn probe_active_owners(
    transport: &dyn RecoveryTransport,
    assignments: Vec<(String, ActiveRangeAssignment)>,
) -> Vec<(String, ActiveRangeAssignment, AssignmentProbe)> {
    stream::iter(assignments)
        .map(|(feed_name, assignment)| async move {
            let owner = transport
                .progress(&assignment.owner, &assignment)
                .await
                .ok();
            let mut replicas = Vec::new();
            for node in assignment.replicas.iter() {
                if node == &assignment.owner {
                    if let Some(status) = &owner {
                        replicas.push(status.clone());
                    }
                    continue;
                }
                if let Ok(status) = transport.progress(node, &assignment).await {
                    replicas.push(status);
                }
            }
            (feed_name, assignment, AssignmentProbe { owner, replicas })
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
        for (feed_name, assignment, probe) in probes {
            // Two recovery cases share one sustained-failure counter:
            // the owner is unreachable (dead-owner path) or a healthy
            // owner is wedged behind a healthy replica's committed
            // frontier (live-owner path). Raw committed skew alone is
            // not the wedge signal: followers persist CommitPosition
            // before the owner marks its own, so every in-flight commit
            // makes a replica look momentarily ahead — under sustained
            // writes that window never closes. The wedge signature is
            // skew held across consecutive probes while the owner's own
            // committed marker refuses to advance: a wedged owner cannot
            // commit at all, a healthy one moves every tick.
            let owner_healthy = probe
                .owner
                .as_ref()
                .map(|status| status.healthy)
                .unwrap_or(false);
            let owner_committed = probe.owner.as_ref().map(|status| status.committed);
            let behind = owner_healthy
                && probe.replicas.iter().any(|replica| {
                    replica.healthy
                        && replica.node != assignment.owner
                        && Some(replica.committed) > owner_committed
                });
            let should_recover = {
                let mut failures = self.failures.lock().await;
                if owner_healthy && !behind {
                    failures.remove(&assignment.feed_id);
                    false
                } else {
                    let entry = failures
                        .entry(assignment.feed_id)
                        .or_insert_with(|| OwnerWatch {
                            owner: assignment.owner.clone(),
                            failures: 0,
                            last_owner_committed: None,
                        });
                    accrue_failure(
                        entry,
                        &assignment.owner,
                        owner_healthy,
                        owner_committed,
                        self.threshold,
                    )
                }
            };
            if should_recover {
                pending.push((feed_name, assignment, owner_healthy));
            }
        }
        stream::iter(pending)
            .map(|(feed_name, assignment, owner_healthy)| async move {
                let result = if owner_healthy {
                    self.executor
                        .recover_behind(&feed_name, &assignment, &assignment.owner)
                        .await
                } else {
                    self.executor
                        .recover(&feed_name, &assignment, &assignment.owner)
                        .await
                };
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
        assignment: &ActiveRangeAssignment,
    ) -> Result<ReplicaRecoveryStatus, String> {
        let response: ReplicaProgressResponse = self
            .post(
                node,
                "/internal/active-range/recovery/progress",
                &ReplicaProgressRequest {
                    feed_id: assignment.feed_id,
                    range_id: Some(assignment.range_id),
                },
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
        first_blocked: std::sync::atomic::AtomicBool,
        first_seen: Arc<Notify>,
        second_seen: Arc<Notify>,
        release_first: Arc<Notify>,
    }

    #[async_trait]
    impl RecoveryTransport for BlockedProbe {
        async fn progress(
            &self,
            _node: &StorageNodeId,
            assignment: &ActiveRangeAssignment,
        ) -> Result<ReplicaRecoveryStatus, String> {
            if assignment.feed_id == self.first
                && self
                    .first_blocked
                    .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                // Only the first probe for this feed blocks: the test
                // proves a slow OWNER probe does not serialize the
                // other feed's probes, and the replica probes that now
                // follow the owner probe must not hang on a one-shot
                // release.
                self.first_seen.notify_one();
                self.release_first.notified().await;
            } else if assignment.feed_id != self.first {
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
            first_blocked: std::sync::atomic::AtomicBool::new(true),
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
        assert!(results.iter().all(|(_, _, probe)| !probe
            .owner
            .as_ref()
            .map(|s| s.healthy)
            .unwrap_or(false)));
    }

    struct UnhealthyProbe;

    #[async_trait]
    impl RecoveryTransport for UnhealthyProbe {
        async fn progress(
            &self,
            node: &StorageNodeId,
            _assignment: &ActiveRangeAssignment,
        ) -> Result<ReplicaRecoveryStatus, String> {
            Ok(ReplicaRecoveryStatus {
                node: node.clone(),
                healthy: false,
                diverged: false,
                appended: RangePosition::new(3),
                committed: CommitPosition::new(2),
            })
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
    async fn a_reachable_owner_reporting_persistence_failure_is_unhealthy() {
        // A live-but-full Node still answers read probes; the probe must
        // carry the replica's reported write health rather than treating a
        // response alone as healthy.
        let owner = StorageNodeId::try_new("storage-1").unwrap();
        let assignment = ActiveRangeAssignment::try_new(
            Uuid::from_u128(9),
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
        let results = probe_active_owners(
            &UnhealthyProbe,
            [("feed".to_owned(), assignment)].into_iter().collect(),
        )
        .await;
        assert_eq!(results.len(), 1);
        let probe = &results[0].2;
        assert!(!probe.owner.as_ref().map(|s| s.healthy).unwrap_or(false));
        assert!(probe.replicas.iter().all(|status| !status.healthy));
    }

    fn behind_assignment(owner: &StorageNodeId) -> ActiveRangeAssignment {
        ActiveRangeAssignment::try_new(
            Uuid::from_u128(11),
            RangeId::from_uuid(Uuid::from_u128(77)),
            RangeGeneration::new(1),
            owner.clone(),
            ReplicaSet::try_new([
                owner.clone(),
                StorageNodeId::try_new("storage-2").unwrap(),
                StorageNodeId::try_new("storage-3").unwrap(),
            ])
            .unwrap(),
            OwnershipEpoch::new(4),
        )
        .unwrap()
    }

    fn status(node: &str, healthy: bool, appended: u64, committed: u64) -> ReplicaRecoveryStatus {
        ReplicaRecoveryStatus {
            node: StorageNodeId::try_new(node).unwrap(),
            healthy,
            diverged: false,
            appended: RangePosition::new(appended),
            committed: CommitPosition::new(committed),
        }
    }

    #[test]
    fn diverged_owner_counts_toward_quorum_but_never_ownership() {
        // The wedged-incident shape: the owner proved a committed fork on a
        // replica and latched diverged, while the third replica is dead.
        // Ownership must move to the lone healthy evidence holder — the
        // diverged node stays a live replica whose tail truncates, so the
        // post-move set can still form quorum.
        let owner = StorageNodeId::try_new("storage-1").unwrap();
        let assignment = behind_assignment(&owner);
        let mut diverged_owner = status("storage-1", false, 45, 40);
        diverged_owner.diverged = true;
        let statuses = vec![
            diverged_owner,
            status("storage-2", true, 41, 41),
            status("storage-3", false, 0, 0),
        ];
        let plan = plan_owner_recovery(&assignment, &statuses).unwrap();
        assert_eq!(plan.new_owner, StorageNodeId::try_new("storage-2").unwrap());
        assert_eq!(plan.committed_prefix, CommitPosition::new(41));
        assert_eq!(plan.previous_epoch, OwnershipEpoch::new(4));
        assert_eq!(plan.new_epoch, OwnershipEpoch::new(5));
    }

    #[test]
    fn diverged_owner_alone_cannot_form_a_plan() {
        // Without any healthy replica there is no evidence holder to move
        // onto: the fork cannot resolve by re-electing the forked node.
        let owner = StorageNodeId::try_new("storage-1").unwrap();
        let assignment = behind_assignment(&owner);
        let mut diverged_owner = status("storage-1", false, 45, 40);
        diverged_owner.diverged = true;
        let statuses = vec![
            diverged_owner,
            status("storage-2", false, 0, 0),
            status("storage-3", false, 0, 0),
        ];
        assert_eq!(
            plan_owner_recovery(&assignment, &statuses),
            Err(OwnerRecoveryError::NoHealthyMajority)
        );
    }

    #[test]
    fn behind_owner_recovery_moves_ownership_to_the_most_committed_replica() {
        // The owner answers probes but a replica carries a strictly
        // larger committed frontier: ownership moves to the replica
        // holding the quorum-committed proof, and the recovered prefix
        // is that replica's committed marker — never less, since a
        // CommitPosition only advances on verified quorum evidence.
        let owner = StorageNodeId::try_new("storage-1").unwrap();
        let assignment = behind_assignment(&owner);
        let statuses = vec![
            status("storage-1", true, 20, 10),
            status("storage-2", true, 30, 15),
            status("storage-3", true, 28, 14),
        ];
        let plan = plan_behind_owner_recovery(&assignment, &statuses).unwrap();
        assert_eq!(plan.new_owner, StorageNodeId::try_new("storage-2").unwrap());
        assert_eq!(plan.committed_prefix, CommitPosition::new(15));
        assert_eq!(plan.previous_owner, owner);
        assert_eq!(plan.previous_epoch, OwnershipEpoch::new(4));
        assert_eq!(plan.new_epoch, OwnershipEpoch::new(5));
    }

    #[test]
    fn behind_owner_recovery_refuses_when_no_replica_is_ahead() {
        // A healthy owner at the frontier must not churn ownership —
        // in-flight commits make a replica look momentarily ahead, but
        // equality is not ahead.
        let owner = StorageNodeId::try_new("storage-1").unwrap();
        let assignment = behind_assignment(&owner);
        let statuses = vec![
            status("storage-1", true, 20, 15),
            status("storage-2", true, 22, 15),
            status("storage-3", true, 19, 12),
        ];
        assert_eq!(
            plan_behind_owner_recovery(&assignment, &statuses),
            Err(OwnerRecoveryError::NoAheadReplica)
        );
    }

    #[test]
    fn behind_owner_recovery_defers_to_the_dead_owner_path() {
        // An unreachable owner is the dead-owner case; the live-owner
        // planner refuses so the supervisor routes to `recover`, which
        // probes the surviving replicas for the caught-up replacement.
        let owner = StorageNodeId::try_new("storage-1").unwrap();
        let assignment = behind_assignment(&owner);
        let statuses = vec![
            status("storage-1", false, 0, 0),
            status("storage-2", true, 30, 15),
            status("storage-3", true, 28, 14),
        ];
        assert_eq!(
            plan_behind_owner_recovery(&assignment, &statuses),
            Err(OwnerRecoveryError::OwnerUnhealthy)
        );
    }

    fn watch(owner: &StorageNodeId) -> OwnerWatch {
        OwnerWatch {
            owner: owner.clone(),
            failures: 0,
            last_owner_committed: None,
        }
    }

    #[test]
    fn behind_owner_streak_only_accrues_while_committed_is_frozen() {
        // The wedge gate: a replica can report a larger committed
        // frontier than the owner on every healthy append cycle —
        // followers persist CommitPosition before the owner marks its
        // own — so skew alone must never move ownership. Only skew held
        // while the owner's own committed marker refuses to advance is
        // the signature of an owner that can no longer commit.
        let owner = StorageNodeId::try_new("storage-1").unwrap();
        let mut watch = watch(&owner);
        let committed = |position| Some(CommitPosition::new(position));

        // Sustained writes: replica stays ahead while the owner's own
        // committed frontier keeps advancing — the streak never accrues
        // past the first frozen probe.
        for position in 10..30 {
            assert!(!accrue_failure(
                &mut watch,
                &owner,
                true,
                committed(position),
                3
            ));
        }
        // First frozen sighting baselines without counting.
        assert!(!accrue_failure(&mut watch, &owner, true, committed(30), 3));
        assert_eq!(watch.failures, 0);
        // Frozen across consecutive probes: the streak builds to the
        // threshold and earns the live-owner recovery attempt.
        assert!(!accrue_failure(&mut watch, &owner, true, committed(30), 3));
        assert!(!accrue_failure(&mut watch, &owner, true, committed(30), 3));
        assert!(accrue_failure(&mut watch, &owner, true, committed(30), 3));
    }

    #[test]
    fn dead_owner_streak_accrues_without_a_committed_sample() {
        // The dead-owner path is unchanged: an unreachable owner has no
        // committed marker to compare, so every failed probe counts.
        let owner = StorageNodeId::try_new("storage-1").unwrap();
        let mut watch = watch(&owner);
        assert!(!accrue_failure(&mut watch, &owner, false, None, 3));
        assert!(!accrue_failure(&mut watch, &owner, false, None, 3));
        assert!(accrue_failure(&mut watch, &owner, false, None, 3));
    }

    #[test]
    fn owner_change_restarts_the_streak() {
        // A lease/ownership move mid-streak must not let the new owner
        // inherit the old owner's failure debt.
        let owner = StorageNodeId::try_new("storage-1").unwrap();
        let successor = StorageNodeId::try_new("storage-2").unwrap();
        let mut watch = watch(&owner);
        assert!(!accrue_failure(&mut watch, &owner, false, None, 2));
        assert!(accrue_failure(&mut watch, &owner, false, None, 2));
        // Same feed, different owner: the counter and the committed
        // baseline reset rather than immediately re-firing.
        assert!(!accrue_failure(
            &mut watch,
            &successor,
            true,
            Some(CommitPosition::new(40)),
            2
        ));
        assert_eq!(watch.failures, 0);
    }

    #[test]
    fn behind_owner_recovery_ignores_unhealthy_or_non_replica_claims() {
        // Only healthy members of the replica set can take ownership:
        // a dead replica's stale committed marker and an outsider's
        // claim are both excluded before comparing frontiers.
        let owner = StorageNodeId::try_new("storage-1").unwrap();
        let assignment = behind_assignment(&owner);
        let mut outsider = status("storage-9", true, 40, 40);
        outsider.node = StorageNodeId::try_new("storage-9").unwrap();
        let statuses = vec![
            status("storage-1", true, 20, 10),
            status("storage-2", false, 30, 30),
            status("storage-3", true, 25, 12),
            outsider,
        ];
        let plan = plan_behind_owner_recovery(&assignment, &statuses).unwrap();
        assert_eq!(plan.new_owner, StorageNodeId::try_new("storage-3").unwrap());
        assert_eq!(plan.committed_prefix, CommitPosition::new(12));
    }
}
