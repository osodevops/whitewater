use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    codec::decode_record,
    control::{Command, RangeSplitPlan},
    control_plane::ControlPlane,
};

use super::{
    ActiveRangeAssignment, CommitPosition, RangePosition, RangeRoute, ReplicaAppendService,
    StorageNodeId,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StagedWriterSequence {
    pub writer_session_id: uuid::Uuid,
    pub writer_epoch: u64,
    pub max_sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SplitStagingResult {
    pub source_scanned_through: CommitPosition,
    pub target_commit: CommitPosition,
    pub transferred_records: u64,
    pub transferred_bytes: u64,
    pub checksum: [u8; 32],
    pub replicas_verified: Vec<StorageNodeId>,
    pub writer_sequences: Vec<StagedWriterSequence>,
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

#[derive(Clone, Debug)]
pub struct SplitPressureTracker {
    threshold: u64,
    sustained_samples: u32,
    cooldown_samples: u32,
    evidence: u32,
    cooldown_remaining: u32,
}

impl SplitPressureTracker {
    pub fn new(threshold: u64, sustained_samples: u32, cooldown_samples: u32) -> Self {
        Self {
            threshold,
            sustained_samples: sustained_samples.max(1),
            cooldown_samples,
            evidence: 0,
            cooldown_remaining: 0,
        }
    }

    pub fn observe(&mut self, appends_per_second: u64) -> bool {
        if self.cooldown_remaining > 0 {
            self.cooldown_remaining -= 1;
            self.evidence = 0;
            return false;
        }
        if appends_per_second < self.threshold {
            self.evidence = 0;
            return false;
        }
        self.evidence = self.evidence.saturating_add(1);
        if self.evidence < self.sustained_samples {
            return false;
        }
        self.evidence = 0;
        self.cooldown_remaining = self.cooldown_samples;
        true
    }
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
    #[error("Control Plane split transition failed: {0}")]
    Control(String),
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

#[async_trait]
pub trait SplitCutoverControl: Send + Sync {
    async fn mark_ready(
        &self,
        feed: &str,
        plan: &RangeSplitPlan,
        boundary: &FrozenSplitBoundary,
    ) -> Result<(), String>;

    async fn activate(
        &self,
        feed: &str,
        plan: &RangeSplitPlan,
        boundary: &FrozenSplitBoundary,
    ) -> Result<(), String>;
}

#[derive(Clone)]
pub struct ControlPlaneSplitCutover {
    control_plane: Arc<ControlPlane>,
}

impl ControlPlaneSplitCutover {
    pub fn new(control_plane: Arc<ControlPlane>) -> Self {
        Self { control_plane }
    }
}

#[async_trait]
impl SplitCutoverControl for ControlPlaneSplitCutover {
    async fn mark_ready(
        &self,
        feed: &str,
        plan: &RangeSplitPlan,
        boundary: &FrozenSplitBoundary,
    ) -> Result<(), String> {
        self.control_plane
            .execute_commands(vec![Command::RecordActiveRangeSplitCatchUp {
                feed: feed.to_owned(),
                plan_id: plan.plan_id,
                source_commit: boundary.final_commit,
                source_scanned_through: boundary.staging.source_commit,
                right_commit: boundary.staging.right.target_commit,
                checksum_verified: true,
            }])
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    async fn activate(
        &self,
        feed: &str,
        plan: &RangeSplitPlan,
        boundary: &FrozenSplitBoundary,
    ) -> Result<(), String> {
        self.control_plane
            .execute_commands(vec![Command::ActivateActiveRangeSplit {
                feed: feed.to_owned(),
                plan_id: plan.plan_id,
                left_writer_sequences: boundary.staging.left.writer_sequences.clone(),
                right_writer_sequences: boundary.staging.right.writer_sequences.clone(),
            }])
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

pub async fn orchestrate_split_cutover(
    control: &dyn SplitCutoverControl,
    feed: &str,
    plan: &RangeSplitPlan,
    source_assignment: &ActiveRangeAssignment,
    source: Arc<ReplicaAppendService>,
    targets: &BTreeMap<StorageNodeId, Arc<ReplicaAppendService>>,
    batch_size: usize,
) -> Result<FrozenSplitBoundary, SplitStagingError> {
    let boundary = freeze_and_stage_final_boundary(
        plan,
        source_assignment,
        source.clone(),
        targets,
        batch_size,
    )
    .await?;
    if let Err(error) = control.mark_ready(feed, plan, &boundary).await {
        abort_frozen_split(&source, &boundary).await;
        return Err(SplitStagingError::Control(error));
    }
    if let Err(error) = control.activate(feed, plan, &boundary).await {
        abort_frozen_split(&source, &boundary).await;
        return Err(SplitStagingError::Control(error));
    }
    Ok(boundary)
}

pub async fn stage_candidate_ranges_local(
    plan: &RangeSplitPlan,
    source: Arc<ReplicaAppendService>,
    local: Arc<ReplicaAppendService>,
    batch_size: usize,
    source_commit: CommitPosition,
) -> Result<CandidateSplitStagingResult, SplitStagingError> {
    let local_node = local.local_node().clone();
    let targets = BTreeMap::from([(local_node.clone(), local)]);
    let left_assignment = plan
        .left_assignment
        .as_ref()
        .ok_or(SplitStagingError::MissingLeftAssignment)?;
    let left = stage_route(
        plan.feed_id,
        left_assignment,
        route_for(plan, left_assignment)?,
        source.clone(),
        &targets,
        batch_size,
        source_commit,
        std::slice::from_ref(&local_node),
    )
    .await?;
    let right = stage_route(
        plan.feed_id,
        &plan.right_assignment,
        route_for(plan, &plan.right_assignment)?,
        source,
        &targets,
        batch_size,
        source_commit,
        std::slice::from_ref(&local_node),
    )
    .await?;
    Ok(CandidateSplitStagingResult {
        source_commit,
        left,
        right,
    })
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
        left_assignment.replicas.as_array(),
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
        plan.right_assignment.replicas.as_array(),
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
        plan.right_assignment.replicas.as_array(),
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

#[allow(clippy::too_many_arguments)]
async fn stage_route(
    feed_id: uuid::Uuid,
    assignment: &ActiveRangeAssignment,
    route: &RangeRoute,
    source: Arc<ReplicaAppendService>,
    targets: &BTreeMap<StorageNodeId, Arc<ReplicaAppendService>>,
    batch_size: usize,
    source_commit: CommitPosition,
    replica_nodes: &[StorageNodeId],
) -> Result<SplitStagingResult, SplitStagingError> {
    let mut source_after = None;
    let mut target_position = 0_u64;
    let mut transferred_bytes = 0_u64;
    let mut writer_sequences = BTreeMap::<(uuid::Uuid, u64), u64>::new();
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
            for node in replica_nodes.iter() {
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
            writer_sequences
                .entry((
                    frame.identity.writer_session_id,
                    frame.identity.writer_epoch,
                ))
                .and_modify(|sequence| *sequence = (*sequence).max(frame.identity.sequence))
                .or_insert(frame.identity.sequence);
            checksum.update(&frame.position.value().to_be_bytes());
            checksum.update(&expected_digest);
            transferred_bytes = transferred_bytes.saturating_add(frame.frame.len() as u64);
        }
    }
    let target_commit = CommitPosition::new(target_position);
    if target_position > 0 {
        for node in replica_nodes.iter() {
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
        replicas_verified: replica_nodes.to_vec(),
        writer_sequences: writer_sequences
            .into_iter()
            .map(
                |((writer_session_id, writer_epoch), max_sequence)| StagedWriterSequence {
                    writer_session_id,
                    writer_epoch,
                    max_sequence,
                },
            )
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressure_requires_sustained_evidence_and_observes_cooldown() {
        let mut tracker = SplitPressureTracker::new(1_000, 3, 2);
        assert!(!tracker.observe(1_100));
        assert!(!tracker.observe(900));
        assert!(!tracker.observe(1_100));
        assert!(!tracker.observe(1_200));
        assert!(tracker.observe(1_300));
        assert!(!tracker.observe(2_000));
        assert!(!tracker.observe(2_000));
        assert!(!tracker.observe(2_000));
        assert!(!tracker.observe(2_000));
        assert!(tracker.observe(2_000));
    }
}
