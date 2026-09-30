use std::{collections::BTreeMap, sync::Arc};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{codec::decode_record, control::RangeSplitPlan};

use super::{CommitPosition, RangePosition, ReplicaAppendService, StorageNodeId};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SplitStagingResult {
    pub source_scanned_through: CommitPosition,
    pub right_commit: CommitPosition,
    pub transferred_records: u64,
    pub transferred_bytes: u64,
    pub checksum: [u8; 32],
    pub replicas_verified: Vec<StorageNodeId>,
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
    #[error("candidate RangeMap does not contain the planned right range")]
    MissingRightRoute,
    #[error("staged replica accepted a conflicting position or frame digest")]
    VerificationConflict,
}

pub async fn stage_right_range(
    plan: &RangeSplitPlan,
    source: Arc<ReplicaAppendService>,
    targets: &BTreeMap<StorageNodeId, Arc<ReplicaAppendService>>,
    batch_size: usize,
) -> Result<SplitStagingResult, SplitStagingError> {
    let source_status = source
        .recovery_status(plan.feed_id)
        .await
        .map_err(|error| SplitStagingError::Source(error.to_string()))?;
    let right_route = plan
        .candidate_map
        .routes()
        .iter()
        .find(|route| route.range_id == plan.right_assignment.range_id)
        .ok_or(SplitStagingError::MissingRightRoute)?;
    let mut source_after = None;
    let mut target_position = 0_u64;
    let mut transferred_bytes = 0_u64;
    let mut checksum = blake3::Hasher::new();
    loop {
        let frames = source
            .export_committed(plan.feed_id, source_after, batch_size.clamp(1, 10_000))
            .await
            .map_err(|error| SplitStagingError::Source(error.to_string()))?;
        if frames.is_empty() {
            break;
        }
        for frame in frames {
            source_after = Some(frame.position);
            let record = decode_record(&frame.frame)
                .map_err(|error| SplitStagingError::Source(error.to_string()))?;
            if !right_route
                .bounds
                .contains(super::KeyToken::from_key(&record.key))
            {
                continue;
            }
            target_position = target_position.saturating_add(1);
            let position = RangePosition::new(target_position);
            let expected_digest = *blake3::hash(&frame.frame).as_bytes();
            for node in plan.right_assignment.replicas.iter() {
                let target = targets
                    .get(node)
                    .ok_or_else(|| SplitStagingError::MissingTarget(node.clone()))?;
                let accepted = target
                    .stage_split_frame(
                        &plan.right_assignment,
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
    let right_commit = CommitPosition::new(target_position);
    if target_position > 0 {
        for node in plan.right_assignment.replicas.iter() {
            targets[node]
                .commit_staged_split(&plan.right_assignment, right_commit)
                .await
                .map_err(|error| SplitStagingError::Target {
                    node: node.clone(),
                    message: error.to_string(),
                })?;
        }
    }
    Ok(SplitStagingResult {
        source_scanned_through: source_status.committed,
        right_commit,
        transferred_records: target_position,
        transferred_bytes,
        checksum: *checksum.finalize().as_bytes(),
        replicas_verified: plan.right_assignment.replicas.iter().cloned().collect(),
    })
}
