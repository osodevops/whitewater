use std::{collections::BTreeMap, sync::Arc};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{codec::decode_record, control::RangeSplitPlan};

use super::{
    ActiveRangeAssignment, CommitPosition, RangePosition, RangeRoute, ReplicaAppendService,
    StorageNodeId,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SplitStagingResult {
    pub source_scanned_through: CommitPosition,
    pub target_commit: CommitPosition,
    pub transferred_records: u64,
    pub transferred_bytes: u64,
    pub checksum: [u8; 32],
    pub replicas_verified: Vec<StorageNodeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CandidateSplitStagingResult {
    pub source_commit: CommitPosition,
    pub left: SplitStagingResult,
    pub right: SplitStagingResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FrozenSplitBoundary {
    pub source_range_id: super::RangeId,
    pub source_generation: super::RangeGeneration,
    pub final_commit: CommitPosition,
    pub staging: CandidateSplitStagingResult,
}

#[derive(Debug, Error)]
pub enum SplitStagingError {
    #[error("source range scan failed: {0}")]
    Source(String),
    #[error("staged replica {node} failed: {message}")]
    Target {
        node: StorageNodeId,
        message: String,
    },
    #[error("staged replica {0} is unavailable")]
    MissingTarget(StorageNodeId),
    #[error("candidate RangeMap does not contain a planned range")]
    MissingRoute,
    #[error("split plan does not contain a staged left assignment")]
    MissingLeftAssignment,
    #[error("staged replica accepted a conflicting position or frame digest")]
    VerificationConflict,
    #[error("source CommitPosition changed after the cutover freeze")]
    BoundaryMoved,
}

pub async fn freeze_and_stage_final_boundary(
    plan: &RangeSplitPlan,
    source_assignment: &ActiveRangeAssignment,
    source: Arc<ReplicaAppendService>,
    targets: &BTreeMap<StorageNodeId, Arc<ReplicaAppendService>>,
    batch_size: usize,
) -> Result<FrozenSplitBoundary, SplitStagingError> {
    source
        .freeze_generation(source_assignment.range_id, source_assignment.generation)
        .await;
    let final_commit = source
        .recovery_status(plan.feed_id)
        .await
        .map_err(|error| SplitStagingError::Source(error.to_string()))?
        .committed;
    let staging = match stage_candidate_ranges(plan, source.clone(), targets, batch_size).await {
        Ok(staging) => staging,
        Err(error) => {
            source
                .unfreeze_generation(source_assignment.range_id, source_assignment.generation)
                .await;
            return Err(error);
        }
    };
    let after = source
        .recovery_status(plan.feed_id)
        .await
        .map_err(|error| SplitStagingError::Source(error.to_string()))?
        .committed;
    if staging.source_commit != final_commit || after != final_commit {
        source
            .unfreeze_generation(source_assignment.range_id, source_assignment.generation)
            .await;
        return Err(SplitStagingError::BoundaryMoved);
    }
    Ok(FrozenSplitBoundary {
        source_range_id: source_assignment.range_id,
        source_generation: source_assignment.generation,
        final_commit,
        staging,
    })
}

pub async fn abort_frozen_split(source: &ReplicaAppendService, boundary: &FrozenSplitBoundary) {
    source
        .unfreeze_generation(boundary.source_range_id, boundary.source_generation)
        .await;
}

pub async fn stage_candidate_ranges(
    plan: &RangeSplitPlan,
    source: Arc<ReplicaAppendService>,
    targets: &BTreeMap<StorageNodeId, Arc<ReplicaAppendService>>,
    batch_size: usize,
) -> Result<CandidateSplitStagingResult, SplitStagingError> {
    let source_commit = source
        .recovery_status(plan.feed_id)
        .await
        .map_err(|error| SplitStagingError::Source(error.to_string()))?
        .committed;
    let left_assignment = plan
        .left_assignment
        .as_ref()
        .ok_or(SplitStagingError::MissingLeftAssignment)?;
    let left_route = route_for(plan, left_assignment)?;
    let right_route = route_for(plan, &plan.right_assignment)?;
    let left = stage_route(
        plan.feed_id,
        left_assignment,
        left_route,
        source.clone(),
        targets,
        batch_size,
        source_commit,
    )
    .await?;
    let right = stage_route(
        plan.feed_id,
        &plan.right_assignment,
        right_route,
        source,
        targets,
        batch_size,
        source_commit,
    )
    .await?;
    Ok(CandidateSplitStagingResult {
        source_commit,
        left,
        right,
    })
}

pub async fn stage_right_range(
    plan: &RangeSplitPlan,
    source: Arc<ReplicaAppendService>,
    targets: &BTreeMap<StorageNodeId, Arc<ReplicaAppendService>>,
    batch_size: usize,
) -> Result<SplitStagingResult, SplitStagingError> {
    let source_commit = source
        .recovery_status(plan.feed_id)
        .await
        .map_err(|error| SplitStagingError::Source(error.to_string()))?
        .committed;
    stage_route(
        plan.feed_id,
        &plan.right_assignment,
        route_for(plan, &plan.right_assignment)?,
        source,
        targets,
        batch_size,
        source_commit,
    )
    .await
}

fn route_for<'a>(
    plan: &'a RangeSplitPlan,
    assignment: &ActiveRangeAssignment,
) -> Result<&'a RangeRoute, SplitStagingError> {
    plan.candidate_map
        .routes()
        .iter()
        .find(|route| route.range_id == assignment.range_id)
        .ok_or(SplitStagingError::MissingRoute)
}

async fn stage_route(
    feed_id: uuid::Uuid,
    assignment: &ActiveRangeAssignment,
    route: &RangeRoute,
    source: Arc<ReplicaAppendService>,
    targets: &BTreeMap<StorageNodeId, Arc<ReplicaAppendService>>,
    batch_size: usize,
    source_commit: CommitPosition,
) -> Result<SplitStagingResult, SplitStagingError> {
    let mut source_after = None;
    let mut target_position = 0_u64;
    let mut transferred_bytes = 0_u64;
    let mut checksum = blake3::Hasher::new();
    while source_after.map_or(0, RangePosition::value) < source_commit.value() {
        let frames = source
            .export_committed(feed_id, source_after, batch_size.clamp(1, 10_000))
            .await
            .map_err(|error| SplitStagingError::Source(error.to_string()))?;
        if frames.is_empty() {
            return Err(SplitStagingError::Source(
                "source ended before the captured CommitPosition".to_owned(),
            ));
        }
        for frame in frames
            .into_iter()
            .take_while(|frame| frame.position.value() <= source_commit.value())
        {
            source_after = Some(frame.position);
            let record = decode_record(&frame.frame)
                .map_err(|error| SplitStagingError::Source(error.to_string()))?;
            if !route
                .bounds
                .contains(super::KeyToken::from_key(&record.key))
            {
                continue;
            }
            target_position = target_position.saturating_add(1);
            let position = RangePosition::new(target_position);
            let expected_digest = *blake3::hash(&frame.frame).as_bytes();
            for node in assignment.replicas.iter() {
                let target = targets
                    .get(node)
                    .ok_or_else(|| SplitStagingError::MissingTarget(node.clone()))?;
                let accepted = target
                    .stage_split_frame(
                        assignment,
                        position,
                        frame.identity.clone(),
                        frame.cursor.clone(),
                        frame.frame.clone(),
                    )
                    .await
                    .map_err(|error| SplitStagingError::Target {
                        node: node.clone(),
                        message: error.to_string(),
                    })?;
                if accepted.position != position || accepted.frame_digest != expected_digest {
                    return Err(SplitStagingError::VerificationConflict);
                }
            }
            checksum.update(&frame.position.value().to_be_bytes());
            checksum.update(&expected_digest);
            transferred_bytes = transferred_bytes.saturating_add(frame.frame.len() as u64);
        }
    }
    let target_commit = CommitPosition::new(target_position);
    if target_position > 0 {
        for node in assignment.replicas.iter() {
            targets[node]
                .commit_staged_split(assignment, target_commit)
                .await
                .map_err(|error| SplitStagingError::Target {
                    node: node.clone(),
                    message: error.to_string(),
                })?;
        }
    }
    Ok(SplitStagingResult {
        source_scanned_through: source_commit,
        target_commit,
        transferred_records: target_position,
        transferred_bytes,
        checksum: *checksum.finalize().as_bytes(),
        replicas_verified: assignment.replicas.iter().cloned().collect(),
    })
}
