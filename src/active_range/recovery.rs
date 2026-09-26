use serde::{Deserialize, Serialize};
use thiserror::Error;

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
