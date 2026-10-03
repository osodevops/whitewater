use std::{collections::BTreeMap, sync::Arc};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{codec::decode_record, control::RangeMergePlan};

use super::{
    ActiveRangeAssignment, CommitPosition, RangePosition, ReplicaAppendService,
    StagedWriterSequence, StorageNodeId,
};

#[derive(Clone, Debug)]
pub struct ColdRangeTracker {
    max_rate: u64,
    sustained_samples: u32,
    cooldown_samples: u32,
    evidence: u32,
    cooldown_remaining: u32,
}

impl ColdRangeTracker {
    pub fn new(max_rate: u64, sustained_samples: u32, cooldown_samples: u32) -> Self {
        Self {
            max_rate,
            sustained_samples: sustained_samples.max(1),
            cooldown_samples,
            evidence: 0,
            cooldown_remaining: 0,
        }
    }

    pub fn observe(&mut self, left_rate: u64, right_rate: u64) -> bool {
        if self.cooldown_remaining > 0 {
            self.cooldown_remaining -= 1;
            self.evidence = 0;
            return false;
        }
        if left_rate > self.max_rate || right_rate > self.max_rate {
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

pub fn cold_adjacent_pairs(
    range_map: &super::RangeMap,
    rates: &BTreeMap<super::RangeId, u64>,
    max_rate: u64,
) -> Vec<(super::RangeId, super::RangeId)> {
    range_map
        .routes()
        .windows(2)
        .filter(|routes| {
            rates.get(&routes[0].range_id).copied().unwrap_or(0) <= max_rate
                && rates.get(&routes[1].range_id).copied().unwrap_or(0) <= max_rate
        })
        .map(|routes| (routes[0].range_id, routes[1].range_id))
        .collect()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MergeStagingResult {
    pub left_commit: CommitPosition,
    pub right_commit: CommitPosition,
    pub merged_commit: CommitPosition,
    pub transferred_records: u64,
    pub transferred_bytes: u64,
    pub checksum: [u8; 32],
    pub writer_sequences: Vec<StagedWriterSequence>,
    pub replicas_verified: Vec<StorageNodeId>,
}

#[derive(Debug, Error)]
pub enum MergeStagingError {
    #[error("merge plan source assignment is unavailable")]
    MissingSource,
    #[error("merge source read failed: {0}")]
    Source(String),
    #[error("merged replica {node} failed: {message}")]
    Target {
        node: StorageNodeId,
        message: String,
    },
    #[error("merged replica {0} is unavailable")]
    MissingTarget(StorageNodeId),
    #[error("merged replica accepted conflicting bytes or position")]
    VerificationConflict,
}

pub async fn stage_merged_range(
    plan: &RangeMergePlan,
    left_assignment: &ActiveRangeAssignment,
    right_assignment: &ActiveRangeAssignment,
    left_source: Arc<ReplicaAppendService>,
    right_source: Arc<ReplicaAppendService>,
    targets: &BTreeMap<StorageNodeId, Arc<ReplicaAppendService>>,
) -> Result<MergeStagingResult, MergeStagingError> {
    left_source
        .freeze_generation(left_assignment.range_id, left_assignment.generation)
        .await;
    right_source
        .freeze_generation(right_assignment.range_id, right_assignment.generation)
        .await;
    match stage_merged_range_inner(
        plan,
        left_assignment,
        right_assignment,
        left_source.clone(),
        right_source.clone(),
        targets,
    )
    .await
    {
        Ok(result) => Ok(result),
        Err(error) => {
            abort_merged_range(
                &left_source,
                left_assignment,
                &right_source,
                right_assignment,
            )
            .await;
            Err(error)
        }
    }
}

pub async fn abort_merged_range(
    left_source: &ReplicaAppendService,
    left_assignment: &ActiveRangeAssignment,
    right_source: &ReplicaAppendService,
    right_assignment: &ActiveRangeAssignment,
) {
    left_source
        .unfreeze_generation(left_assignment.range_id, left_assignment.generation)
        .await;
    right_source
        .unfreeze_generation(right_assignment.range_id, right_assignment.generation)
        .await;
}

async fn stage_merged_range_inner(
    plan: &RangeMergePlan,
    left_assignment: &ActiveRangeAssignment,
    right_assignment: &ActiveRangeAssignment,
    left_source: Arc<ReplicaAppendService>,
    right_source: Arc<ReplicaAppendService>,
    targets: &BTreeMap<StorageNodeId, Arc<ReplicaAppendService>>,
) -> Result<MergeStagingResult, MergeStagingError> {
    let left_status = left_source
        .recovery_status_for_assignment(left_assignment)
        .await
        .map_err(|error| MergeStagingError::Source(error.to_string()))?;
    let right_status = right_source
        .recovery_status_for_assignment(right_assignment)
        .await
        .map_err(|error| MergeStagingError::Source(error.to_string()))?;
    let right_store = right_source
        .read_staged_committed(right_assignment, None, 10_000)
        .await
        .map_err(|error| MergeStagingError::Source(error.to_string()))?;
    let left_frames = left_source
        .export_assignment_committed(left_assignment, None, 10_000)
        .await
        .map_err(|error| MergeStagingError::Source(error.to_string()))?;
    if left_frames.len() as u64 != left_status.committed.value() {
        return Err(MergeStagingError::Source(
            "left source committed history exceeds bounded merge scan".to_owned(),
        ));
    }
    let right_commit = right_status.committed;
    if right_store.len() as u64 != right_commit.value() {
        return Err(MergeStagingError::Source(
            "right source committed history exceeds bounded merge scan".to_owned(),
        ));
    }
    let mut frames = left_frames;
    frames.extend(right_store);
    frames.sort_by_key(|item| {
        decode_record(&item.frame)
            .map(|record| (record.ingest_time_ns, record.message_id))
            .unwrap_or((i64::MAX, uuid::Uuid::nil()))
    });
    let mut checksum = blake3::Hasher::new();
    let mut writer_sequences = BTreeMap::<(uuid::Uuid, u64), u64>::new();
    let mut transferred_bytes = 0_u64;
    for (index, frame) in frames.iter().enumerate() {
        let position = RangePosition::new(index as u64 + 1);
        let digest = *blake3::hash(&frame.frame).as_bytes();
        for node in plan.merged_assignment.replicas.iter() {
            let target = targets
                .get(node)
                .ok_or_else(|| MergeStagingError::MissingTarget(node.clone()))?;
            let accepted = target
                .stage_split_frame(
                    &plan.merged_assignment,
                    position,
                    frame.identity.clone(),
                    frame.cursor.clone(),
                    frame.frame.clone(),
                )
                .await
                .map_err(|error| MergeStagingError::Target {
                    node: node.clone(),
                    message: error.to_string(),
                })?;
            if accepted.position != position || accepted.frame_digest != digest {
                return Err(MergeStagingError::VerificationConflict);
            }
        }
        writer_sequences
            .entry((
                frame.identity.writer_session_id,
                frame.identity.writer_epoch,
            ))
            .and_modify(|sequence| *sequence = (*sequence).max(frame.identity.sequence))
            .or_insert(frame.identity.sequence);
        checksum.update(&digest);
        transferred_bytes = transferred_bytes.saturating_add(frame.frame.len() as u64);
    }
    let merged_commit = CommitPosition::new(frames.len() as u64);
    if !frames.is_empty() {
        for node in plan.merged_assignment.replicas.iter() {
            targets[node]
                .commit_staged_split(&plan.merged_assignment, merged_commit)
                .await
                .map_err(|error| MergeStagingError::Target {
                    node: node.clone(),
                    message: error.to_string(),
                })?;
        }
    }
    Ok(MergeStagingResult {
        left_commit: left_status.committed,
        right_commit,
        merged_commit,
        transferred_records: frames.len() as u64,
        transferred_bytes,
        checksum: *checksum.finalize().as_bytes(),
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
        replicas_verified: plan.merged_assignment.replicas.iter().cloned().collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::active_range::{KeyToken, RangeGeneration, RangeId, RangeMap};
    use uuid::Uuid;

    fn range(value: u128) -> RangeId {
        RangeId::from_uuid(Uuid::from_u128(value))
    }

    #[test]
    fn cold_evidence_resets_for_hot_samples_and_observes_cooldown() {
        let mut tracker = ColdRangeTracker::new(10, 3, 2);
        assert!(!tracker.observe(2, 3));
        assert!(!tracker.observe(2, 20));
        assert!(!tracker.observe(2, 3));
        assert!(!tracker.observe(4, 5));
        assert!(tracker.observe(6, 7));
        assert!(!tracker.observe(1, 1));
        assert!(!tracker.observe(1, 1));
    }

    #[test]
    fn selector_only_returns_adjacent_pairs_below_threshold() {
        let split = KeyToken::from_bytes([0x80; 16]);
        let map = RangeMap::single(range(1), RangeGeneration::new(1))
            .split(range(1), split, range(2))
            .unwrap();
        let rates = BTreeMap::from([(range(1), 2), (range(2), 3)]);
        assert_eq!(
            cold_adjacent_pairs(&map, &rates, 5),
            vec![(range(1), range(2))]
        );
        let hot = BTreeMap::from([(range(1), 2), (range(2), 30)]);
        assert!(cold_adjacent_pairs(&map, &hot, 5).is_empty());
    }
}
