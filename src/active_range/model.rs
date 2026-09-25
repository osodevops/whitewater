use std::{collections::BTreeSet, fmt};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub const ACTIVE_RANGE_REPLICA_COUNT: usize = 3;
pub const ACTIVE_RANGE_COMMIT_QUORUM: usize = 2;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct RangeId(Uuid);

impl RangeId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    pub fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for RangeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RangeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct StorageNodeId(String);

impl StorageNodeId {
    pub fn try_new(value: impl Into<String>) -> Result<Self, ActiveRangeError> {
        let value = value.into();
        if value.is_empty() || value.len() > 255 {
            return Err(ActiveRangeError::InvalidStorageNodeId(value));
        }
        if !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '.' | '-'))
        {
            return Err(ActiveRangeError::InvalidStorageNodeId(value));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StorageNodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

macro_rules! ordered_counter {
    ($name:ident) => {
        #[derive(
            Clone,
            Copy,
            Debug,
            Default,
            Serialize,
            Deserialize,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
        )]
        #[serde(transparent)]
        pub struct $name(u64);

        impl $name {
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            pub const fn value(self) -> u64 {
                self.0
            }

            pub fn checked_next(self) -> Result<Self, ActiveRangeError> {
                self.0
                    .checked_add(1)
                    .map(Self)
                    .ok_or(ActiveRangeError::CounterOverflow(stringify!($name)))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

ordered_counter!(RangeGeneration);
ordered_counter!(OwnershipEpoch);
ordered_counter!(RangePosition);
ordered_counter!(CommitPosition);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaSet {
    replicas: [StorageNodeId; ACTIVE_RANGE_REPLICA_COUNT],
}

impl ReplicaSet {
    pub fn try_new(
        replicas: [StorageNodeId; ACTIVE_RANGE_REPLICA_COUNT],
    ) -> Result<Self, ActiveRangeError> {
        let unique = replicas.iter().collect::<BTreeSet<_>>();
        if unique.len() != ACTIVE_RANGE_REPLICA_COUNT {
            return Err(ActiveRangeError::DuplicateReplica);
        }
        Ok(Self { replicas })
    }

    pub fn contains(&self, node_id: &StorageNodeId) -> bool {
        self.replicas.contains(node_id)
    }

    pub fn iter(&self) -> impl Iterator<Item = &StorageNodeId> {
        self.replicas.iter()
    }

    pub fn as_array(&self) -> &[StorageNodeId; ACTIVE_RANGE_REPLICA_COUNT] {
        &self.replicas
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActiveRangeAssignment {
    pub feed_id: Uuid,
    pub range_id: RangeId,
    pub generation: RangeGeneration,
    pub owner: StorageNodeId,
    pub replicas: ReplicaSet,
    pub ownership_epoch: OwnershipEpoch,
}

impl ActiveRangeAssignment {
    pub fn try_new(
        feed_id: Uuid,
        range_id: RangeId,
        generation: RangeGeneration,
        owner: StorageNodeId,
        replicas: ReplicaSet,
        ownership_epoch: OwnershipEpoch,
    ) -> Result<Self, ActiveRangeError> {
        if !replicas.contains(&owner) {
            return Err(ActiveRangeError::OwnerNotReplica(owner));
        }
        Ok(Self {
            feed_id,
            range_id,
            generation,
            owner,
            replicas,
            ownership_epoch,
        })
    }

    pub fn transfer_ownership(
        &mut self,
        owner: StorageNodeId,
        ownership_epoch: OwnershipEpoch,
    ) -> Result<(), ActiveRangeError> {
        if !self.replicas.contains(&owner) {
            return Err(ActiveRangeError::OwnerNotReplica(owner));
        }
        if ownership_epoch <= self.ownership_epoch {
            return Err(ActiveRangeError::StaleEpoch {
                current: self.ownership_epoch,
                supplied: ownership_epoch,
            });
        }
        self.owner = owner;
        self.ownership_epoch = ownership_epoch;
        Ok(())
    }

    pub fn validate_request(
        &self,
        generation: RangeGeneration,
        ownership_epoch: OwnershipEpoch,
        owner: &StorageNodeId,
    ) -> Result<(), ActiveRangeError> {
        if generation != self.generation {
            return Err(ActiveRangeError::WrongGeneration {
                current: self.generation,
                supplied: generation,
            });
        }
        if ownership_epoch != self.ownership_epoch {
            return Err(ActiveRangeError::StaleEpoch {
                current: self.ownership_epoch,
                supplied: ownership_epoch,
            });
        }
        if owner != &self.owner {
            return Err(ActiveRangeError::NotCurrentOwner(owner.clone()));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecordState {
    Prepared,
    Flushed,
    MajorityReplicated,
    Committed,
    Visible,
    Truncated,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendIdentity {
    pub writer_session_id: Uuid,
    pub writer_epoch: u64,
    pub sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommittedAppendResult {
    pub message_id: Uuid,
    pub cursor: String,
    pub position: RangePosition,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RetryOutcome {
    Pending,
    Original(CommittedAppendResult),
}

#[derive(Clone, Debug)]
pub struct RecordReplicationModel {
    assignment: ActiveRangeAssignment,
    position: RangePosition,
    identity: AppendIdentity,
    frame_digest: [u8; 32],
    message_id: Uuid,
    cursor: String,
    durable_replicas: BTreeSet<StorageNodeId>,
    commit_evidence: BTreeSet<StorageNodeId>,
    state: RecordState,
}

impl RecordReplicationModel {
    pub fn new(
        assignment: ActiveRangeAssignment,
        position: RangePosition,
        identity: AppendIdentity,
        frame_digest: [u8; 32],
        message_id: Uuid,
        cursor: impl Into<String>,
    ) -> Self {
        Self {
            assignment,
            position,
            identity,
            frame_digest,
            message_id,
            cursor: cursor.into(),
            durable_replicas: BTreeSet::new(),
            commit_evidence: BTreeSet::new(),
            state: RecordState::Prepared,
        }
    }

    pub fn state(&self) -> RecordState {
        self.state
    }

    pub fn durable_replica_count(&self) -> usize {
        self.durable_replicas.len()
    }

    pub fn commit_evidence_count(&self) -> usize {
        self.commit_evidence.len()
    }

    pub fn committed_result(&self) -> Option<CommittedAppendResult> {
        matches!(self.state, RecordState::Committed | RecordState::Visible).then(|| {
            CommittedAppendResult {
                message_id: self.message_id,
                cursor: self.cursor.clone(),
                position: self.position,
            }
        })
    }

    pub fn record_durable_frame(
        &mut self,
        append_owner: &StorageNodeId,
        replica: StorageNodeId,
        generation: RangeGeneration,
        ownership_epoch: OwnershipEpoch,
        frame_digest: [u8; 32],
    ) -> Result<(), ActiveRangeError> {
        self.assignment
            .validate_request(generation, ownership_epoch, append_owner)?;
        if !self.assignment.replicas.contains(&replica) {
            return Err(ActiveRangeError::UnknownReplica(replica));
        }
        if frame_digest != self.frame_digest {
            return Err(ActiveRangeError::ConflictingFrame(self.position));
        }
        if matches!(self.state, RecordState::Truncated) {
            return Err(ActiveRangeError::InvalidTransition {
                from: self.state,
                operation: "record durable frame",
            });
        }
        self.durable_replicas.insert(replica);
        if !matches!(self.state, RecordState::Committed | RecordState::Visible) {
            self.state = if self.durable_replicas.len() >= ACTIVE_RANGE_COMMIT_QUORUM {
                RecordState::MajorityReplicated
            } else {
                RecordState::Flushed
            };
        }
        Ok(())
    }

    pub fn record_commit_evidence(
        &mut self,
        replica: StorageNodeId,
        ownership_epoch: OwnershipEpoch,
    ) -> Result<(), ActiveRangeError> {
        if ownership_epoch != self.assignment.ownership_epoch {
            return Err(ActiveRangeError::StaleEpoch {
                current: self.assignment.ownership_epoch,
                supplied: ownership_epoch,
            });
        }
        if self.durable_replicas.len() < ACTIVE_RANGE_COMMIT_QUORUM {
            return Err(ActiveRangeError::InsufficientDurableReplicas(
                self.durable_replicas.len(),
            ));
        }
        if !self.durable_replicas.contains(&replica) {
            return Err(ActiveRangeError::CommitWithoutDurableFrame(replica));
        }
        self.commit_evidence.insert(replica);
        if self.commit_evidence.len() >= ACTIVE_RANGE_COMMIT_QUORUM
            && self.state != RecordState::Visible
        {
            self.state = RecordState::Committed;
        }
        Ok(())
    }

    pub fn make_visible(&mut self) -> Result<(), ActiveRangeError> {
        if self.state != RecordState::Committed {
            return Err(ActiveRangeError::NotCommitted);
        }
        self.state = RecordState::Visible;
        Ok(())
    }

    pub fn truncate_uncommitted(&mut self) -> Result<(), ActiveRangeError> {
        if matches!(self.state, RecordState::Committed | RecordState::Visible) {
            return Err(ActiveRangeError::CommittedRecordCannotBeTruncated);
        }
        self.state = RecordState::Truncated;
        Ok(())
    }

    pub fn retry(
        &self,
        identity: &AppendIdentity,
        frame_digest: [u8; 32],
    ) -> Result<RetryOutcome, ActiveRangeError> {
        if identity != &self.identity || frame_digest != self.frame_digest {
            return Err(ActiveRangeError::RetryConflict);
        }
        Ok(self
            .committed_result()
            .map_or(RetryOutcome::Pending, RetryOutcome::Original))
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeProgress {
    appended: RangePosition,
    flushed: RangePosition,
    committed: CommitPosition,
    visible: RangePosition,
}

impl RangeProgress {
    pub fn new() -> Self {
        Self {
            appended: RangePosition::new(0),
            flushed: RangePosition::new(0),
            committed: CommitPosition::new(0),
            visible: RangePosition::new(0),
        }
    }

    pub fn commit_position(self) -> CommitPosition {
        self.committed
    }

    pub fn advance_appended(&mut self, position: RangePosition) -> Result<(), ActiveRangeError> {
        ensure_not_backwards("appended", self.appended.value(), position.value())?;
        self.appended = position;
        Ok(())
    }

    pub fn advance_flushed(&mut self, position: RangePosition) -> Result<(), ActiveRangeError> {
        ensure_not_backwards("flushed", self.flushed.value(), position.value())?;
        if position > self.appended {
            return Err(ActiveRangeError::PositionBeyond {
                position: "flushed",
                boundary: "appended",
            });
        }
        self.flushed = position;
        Ok(())
    }

    pub fn advance_committed(&mut self, position: CommitPosition) -> Result<(), ActiveRangeError> {
        ensure_not_backwards("committed", self.committed.value(), position.value())?;
        if position.value() > self.flushed.value() {
            return Err(ActiveRangeError::PositionBeyond {
                position: "committed",
                boundary: "flushed",
            });
        }
        self.committed = position;
        Ok(())
    }

    pub fn advance_visible(&mut self, position: RangePosition) -> Result<(), ActiveRangeError> {
        ensure_not_backwards("visible", self.visible.value(), position.value())?;
        if position.value() > self.committed.value() {
            return Err(ActiveRangeError::PositionBeyond {
                position: "visible",
                boundary: "committed",
            });
        }
        self.visible = position;
        Ok(())
    }
}

impl Default for RangeProgress {
    fn default() -> Self {
        Self::new()
    }
}

fn ensure_not_backwards(
    position: &'static str,
    current: u64,
    supplied: u64,
) -> Result<(), ActiveRangeError> {
    if supplied < current {
        Err(ActiveRangeError::PositionMovedBackwards {
            position,
            current,
            supplied,
        })
    } else {
        Ok(())
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ActiveRangeError {
    #[error("invalid storage Node ID: {0}")]
    InvalidStorageNodeId(String),
    #[error("replica set contains duplicate Nodes")]
    DuplicateReplica,
    #[error("Append Owner is not a member of the replica set: {0}")]
    OwnerNotReplica(StorageNodeId),
    #[error("unknown replica: {0}")]
    UnknownReplica(StorageNodeId),
    #[error("request was sent by a Node that is not the current Append Owner: {0}")]
    NotCurrentOwner(StorageNodeId),
    #[error("wrong RangeGeneration: current={current}, supplied={supplied}")]
    WrongGeneration {
        current: RangeGeneration,
        supplied: RangeGeneration,
    },
    #[error("stale OwnershipEpoch: current={current}, supplied={supplied}")]
    StaleEpoch {
        current: OwnershipEpoch,
        supplied: OwnershipEpoch,
    },
    #[error("counter overflow: {0}")]
    CounterOverflow(&'static str),
    #[error("conflicting bytes at RangePosition {0}")]
    ConflictingFrame(RangePosition),
    #[error("only {0} replicas contain a durable frame")]
    InsufficientDurableReplicas(usize),
    #[error("replica cannot persist commit evidence without the durable frame: {0}")]
    CommitWithoutDurableFrame(StorageNodeId),
    #[error("record is not committed")]
    NotCommitted,
    #[error("committed record cannot be truncated")]
    CommittedRecordCannotBeTruncated,
    #[error("retry identity or content conflicts with the original append")]
    RetryConflict,
    #[error("invalid transition from {from:?}: {operation}")]
    InvalidTransition {
        from: RecordState,
        operation: &'static str,
    },
    #[error("{position} position cannot move backwards: current={current}, supplied={supplied}")]
    PositionMovedBackwards {
        position: &'static str,
        current: u64,
        supplied: u64,
    },
    #[error("{position} position cannot advance beyond {boundary}")]
    PositionBeyond {
        position: &'static str,
        boundary: &'static str,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str) -> StorageNodeId {
        StorageNodeId::try_new(name).unwrap()
    }

    fn assignment() -> ActiveRangeAssignment {
        ActiveRangeAssignment::try_new(
            Uuid::from_u128(1),
            RangeId::from_uuid(Uuid::from_u128(2)),
            RangeGeneration::new(1),
            node("node1"),
            ReplicaSet::try_new([node("node1"), node("node2"), node("node3")]).unwrap(),
            OwnershipEpoch::new(1),
        )
        .unwrap()
    }

    fn record() -> RecordReplicationModel {
        RecordReplicationModel::new(
            assignment(),
            RangePosition::new(1),
            AppendIdentity {
                writer_session_id: Uuid::from_u128(3),
                writer_epoch: 1,
                sequence: 1,
            },
            [7; 32],
            Uuid::from_u128(4),
            "cursor-1",
        )
    }

    #[test]
    fn ordered_counters_compare_and_serialize_as_numbers() {
        assert!(RangeGeneration::new(2) > RangeGeneration::new(1));
        assert!(OwnershipEpoch::new(2) > OwnershipEpoch::new(1));
        assert!(RangePosition::new(2) > RangePosition::new(1));
        assert_eq!(serde_json::to_string(&OwnershipEpoch::new(9)).unwrap(), "9");
        assert_eq!(
            serde_json::from_str::<RangePosition>("12").unwrap(),
            RangePosition::new(12)
        );
    }

    #[test]
    fn replica_set_requires_three_unique_nodes() {
        assert_eq!(
            ReplicaSet::try_new([node("node1"), node("node1"), node("node3")]),
            Err(ActiveRangeError::DuplicateReplica)
        );
    }

    #[test]
    fn owner_must_be_a_replica() {
        let result = ActiveRangeAssignment::try_new(
            Uuid::from_u128(1),
            RangeId::from_uuid(Uuid::from_u128(2)),
            RangeGeneration::new(1),
            node("node4"),
            ReplicaSet::try_new([node("node1"), node("node2"), node("node3")]).unwrap(),
            OwnershipEpoch::new(1),
        );
        assert_eq!(
            result,
            Err(ActiveRangeError::OwnerNotReplica(node("node4")))
        );
    }

    #[test]
    fn assignment_round_trips_without_losing_physical_identity() {
        let assignment = assignment();
        let encoded = serde_json::to_vec(&assignment).unwrap();
        assert_eq!(
            serde_json::from_slice::<ActiveRangeAssignment>(&encoded).unwrap(),
            assignment
        );
    }

    #[test]
    fn higher_epoch_fences_previous_owner() {
        let mut assignment = assignment();
        assignment
            .transfer_ownership(node("node2"), OwnershipEpoch::new(2))
            .unwrap();
        assert_eq!(assignment.owner, node("node2"));
        assert_eq!(
            assignment.validate_request(
                RangeGeneration::new(1),
                OwnershipEpoch::new(1),
                &node("node1")
            ),
            Err(ActiveRangeError::StaleEpoch {
                current: OwnershipEpoch::new(2),
                supplied: OwnershipEpoch::new(1)
            })
        );
    }

    #[test]
    fn old_generation_is_rejected() {
        assert_eq!(
            assignment().validate_request(
                RangeGeneration::new(0),
                OwnershipEpoch::new(1),
                &node("node1")
            ),
            Err(ActiveRangeError::WrongGeneration {
                current: RangeGeneration::new(1),
                supplied: RangeGeneration::new(0)
            })
        );
    }

    #[test]
    fn cannot_commit_with_one_durable_copy() {
        let mut record = record();
        record
            .record_durable_frame(
                &node("node1"),
                node("node1"),
                RangeGeneration::new(1),
                OwnershipEpoch::new(1),
                [7; 32],
            )
            .unwrap();
        assert_eq!(record.state(), RecordState::Flushed);
        assert_eq!(
            record.record_commit_evidence(node("node1"), OwnershipEpoch::new(1)),
            Err(ActiveRangeError::InsufficientDurableReplicas(1))
        );
        assert_eq!(record.committed_result(), None);
    }

    #[test]
    fn can_commit_with_two_of_three_durable_copies() {
        let mut record = record();
        for replica in [node("node1"), node("node2")] {
            record
                .record_durable_frame(
                    &node("node1"),
                    replica,
                    RangeGeneration::new(1),
                    OwnershipEpoch::new(1),
                    [7; 32],
                )
                .unwrap();
        }
        assert_eq!(record.state(), RecordState::MajorityReplicated);
        record
            .record_commit_evidence(node("node1"), OwnershipEpoch::new(1))
            .unwrap();
        assert_eq!(record.state(), RecordState::MajorityReplicated);
        record
            .record_commit_evidence(node("node2"), OwnershipEpoch::new(1))
            .unwrap();
        assert_eq!(record.state(), RecordState::Committed);
    }

    #[test]
    fn uncommitted_record_is_never_visible() {
        let mut record = record();
        record
            .record_durable_frame(
                &node("node1"),
                node("node1"),
                RangeGeneration::new(1),
                OwnershipEpoch::new(1),
                [7; 32],
            )
            .unwrap();
        assert_eq!(record.make_visible(), Err(ActiveRangeError::NotCommitted));
        record.truncate_uncommitted().unwrap();
        assert_eq!(record.state(), RecordState::Truncated);
    }

    #[test]
    fn conflicting_bytes_at_same_position_are_rejected() {
        let mut record = record();
        assert_eq!(
            record.record_durable_frame(
                &node("node1"),
                node("node1"),
                RangeGeneration::new(1),
                OwnershipEpoch::new(1),
                [8; 32]
            ),
            Err(ActiveRangeError::ConflictingFrame(RangePosition::new(1)))
        );
    }

    #[test]
    fn retry_after_commit_returns_original_result() {
        let mut record = record();
        for replica in [node("node1"), node("node2")] {
            record
                .record_durable_frame(
                    &node("node1"),
                    replica,
                    RangeGeneration::new(1),
                    OwnershipEpoch::new(1),
                    [7; 32],
                )
                .unwrap();
        }
        for replica in [node("node1"), node("node2")] {
            record
                .record_commit_evidence(replica, OwnershipEpoch::new(1))
                .unwrap();
        }
        assert_eq!(
            record
                .retry(
                    &AppendIdentity {
                        writer_session_id: Uuid::from_u128(3),
                        writer_epoch: 1,
                        sequence: 1
                    },
                    [7; 32]
                )
                .unwrap(),
            RetryOutcome::Original(CommittedAppendResult {
                message_id: Uuid::from_u128(4),
                cursor: "cursor-1".to_owned(),
                position: RangePosition::new(1)
            })
        );
    }

    #[test]
    fn commit_and_visible_positions_never_move_backwards_or_cross_boundaries() {
        let mut progress = RangeProgress::new();
        progress.advance_appended(RangePosition::new(5)).unwrap();
        progress.advance_flushed(RangePosition::new(5)).unwrap();
        progress.advance_committed(CommitPosition::new(4)).unwrap();
        progress.advance_visible(RangePosition::new(4)).unwrap();
        assert_eq!(
            progress.advance_committed(CommitPosition::new(3)),
            Err(ActiveRangeError::PositionMovedBackwards {
                position: "committed",
                current: 4,
                supplied: 3
            })
        );
        assert_eq!(
            progress.advance_visible(RangePosition::new(5)),
            Err(ActiveRangeError::PositionBeyond {
                position: "visible",
                boundary: "committed"
            })
        );
    }
}
