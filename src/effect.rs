//! Bounded, RF3-replicated effect journal for atomic consume-and-append.
//!
//! An effect records one decision — the consumed input frontier and the
//! declared output records — inside hidden internal storage rather than an
//! application Feed. Each journal row is replicated to its assigned storage
//! Nodes through the same durable prepare/commit protocol used by
//! Subscription progress: a prepared mutation is invisible until two distinct
//! assigned replicas commit it under matching digests, ownership epochs fence
//! stale coordinators, and recovery adopts a quorum-corroborated value
//! without ever rolling back a committed decision. The apply step that turns
//! a committed `Declare` into Feed appends and consumed progress is a
//! separate concern; this module guarantees the decision itself is durable,
//! fenced, and replayable.

use std::{collections::BTreeMap, path::Path};

use fjall::Readable;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::active_range::RangeId;

/// Maximum output records one effect may declare.
pub const EFFECT_MAX_OUTPUTS: usize = 64;
/// Maximum serialized bytes for one effect mutation or journal row.
pub const EFFECT_MAX_MUTATION_BYTES: usize = 256 * 1024;
/// Maximum base64-encoded bytes for a single output Key or payload.
pub const EFFECT_MAX_OUTPUT_FIELD: usize = 96 * 1024;
/// Maximum per-range consumed positions carried as input evidence.
pub const EFFECT_MAX_POSITIONS: usize = 128;

/// One output record an effect declares it will append to a Feed. The
/// writer identity and sequence are deterministic so a retried apply lands
/// on the Feed's idempotent-append path instead of duplicating output.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectOutput {
    pub feed_id: Uuid,
    pub key_base64: String,
    pub payload_base64: String,
    pub event_time_ns: i64,
    pub writer_session_id: Uuid,
    pub sequence: u64,
}

/// Input-consumption evidence an effect declares: which Subscription
/// frontier this decision consumes, and where the frontier moves to once the
/// apply step acknowledges it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectConsume {
    pub feed_id: Uuid,
    pub expected_cursor: Option<String>,
    pub cursor: String,
    pub positions: BTreeMap<RangeId, String>,
}

/// The lifecycle transition a journal mutation commits.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum EffectTransition {
    /// The effect decision: consumed input evidence plus declared outputs.
    Declare {
        consume: Option<EffectConsume>,
        outputs: Vec<EffectOutput>,
    },
    /// Every declared output durably appended and the consume frontier
    /// committed; `declares` identifies the committed `Declare` request.
    Applied { declares: Uuid },
    /// The decision was abandoned by policy (for example a poison input was
    /// quarantined); `declares` identifies the committed `Declare` request.
    Aborted { declares: Uuid, reason: String },
}

/// One bounded mutation against an effect journal row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectMutation {
    pub effect_id: Uuid,
    /// The Subscription whose progress assignment owns this journal row:
    /// the same RF3 replica set and ownership epoch fence the decision.
    pub subscription_id: Uuid,
    pub ownership_epoch: u64,
    pub sequence: u64,
    pub request_id: Uuid,
    pub transition: EffectTransition,
}

/// Digest evidence that a replica durably prepared a mutation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectPrepareVote {
    pub effect_id: Uuid,
    pub request_id: Uuid,
    pub digest: [u8; 32],
}

/// Two distinct replica votes proving a mutation was durably prepared.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EffectCommitEvidence {
    pub effect_id: Uuid,
    pub request_id: Uuid,
    pub votes: [(crate::active_range::StorageNodeId, [u8; 32]); 2],
}

impl EffectCommitEvidence {
    pub fn new(
        effect_id: Uuid,
        request_id: Uuid,
        owner: crate::active_range::StorageNodeId,
        owner_digest: [u8; 32],
        member: crate::active_range::StorageNodeId,
        member_digest: [u8; 32],
    ) -> Self {
        Self {
            effect_id,
            request_id,
            votes: [(owner, owner_digest), (member, member_digest)],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectPrepareRequest {
    pub owner: crate::active_range::StorageNodeId,
    pub receiver: crate::active_range::StorageNodeId,
    pub mutation: EffectMutation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectCommitRequest {
    pub owner: crate::active_range::StorageNodeId,
    pub receiver: crate::active_range::StorageNodeId,
    pub subscription_id: Uuid,
    pub effect_id: Uuid,
    pub ownership_epoch: u64,
    pub evidence: EffectCommitEvidence,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectReadRequest {
    pub owner: crate::active_range::StorageNodeId,
    pub receiver: crate::active_range::StorageNodeId,
    pub subscription_id: Uuid,
    pub effect_id: Uuid,
    pub ownership_epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectAdoptRequest {
    pub owner: crate::active_range::StorageNodeId,
    pub receiver: crate::active_range::StorageNodeId,
    pub subscription_id: Uuid,
    pub effect_id: Uuid,
    pub ownership_epoch: u64,
    pub committed: Option<EffectMutation>,
}

/// A replica reply carrying the responder's claimed identity so the
/// coordinator can refuse wrong-Node evidence even when digests match.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EffectReplicaReply<T> {
    pub replica: crate::active_range::StorageNodeId,
    pub effect_id: Uuid,
    pub ownership_epoch: u64,
    pub result: T,
}

/// Full committed/prepared state a replica exposes for recovery evidence.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectJournalInspection {
    pub committed: Option<EffectMutation>,
    pub prepared: Option<EffectMutation>,
}

#[derive(Debug, Error)]
pub enum EffectJournalError {
    #[error(transparent)]
    Engine(#[from] fjall::Error),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
    #[error("effect journal has a conflicting identity, request, or transition")]
    Conflict,
    #[error("effect journal ownership changed; reconcile before retrying")]
    StaleEpoch,
    #[error("effect journal sequence has a gap or overflow")]
    Sequence,
    #[error("effect journal mutation exceeds its bounded storage budget")]
    TooLarge,
    #[error("effect journal has no valid two-replica commit evidence")]
    NoQuorum,
    #[error("effect journal replica placement or owner is invalid")]
    InvalidAssignment,
    #[error("effect journal replica is unavailable; retry the same request identity")]
    Unavailable,
    #[error("effect journal commit result is ambiguous; retry the same request identity")]
    AmbiguousCommit,
}

impl EffectJournalError {
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Conflict => "conflict",
            Self::StaleEpoch => "stale_epoch",
            Self::Sequence => "sequence",
            Self::TooLarge => "too_large",
            Self::NoQuorum => "no_quorum",
            Self::InvalidAssignment => "invalid_assignment",
            Self::Unavailable => "unavailable",
            Self::AmbiguousCommit => "ambiguous_commit",
            Self::Engine(_) => "engine",
            Self::Serialization(_) => "serialization",
        }
    }

    pub(crate) fn from_code(code: &str) -> Option<Self> {
        Some(match code {
            "conflict" => Self::Conflict,
            "stale_epoch" => Self::StaleEpoch,
            "sequence" => Self::Sequence,
            "too_large" => Self::TooLarge,
            "no_quorum" => Self::NoQuorum,
            "invalid_assignment" => Self::InvalidAssignment,
            "unavailable" => Self::Unavailable,
            "ambiguous_commit" => Self::AmbiguousCommit,
            _ => return None,
        })
    }
}

/// Replies that mean a replica is behind the current placement rather than
/// contradicting quorum state; they are lagging witnesses and never
/// disqualify otherwise healthy evidence.
fn lagging_witness(error: &EffectJournalError) -> bool {
    matches!(
        error,
        EffectJournalError::Unavailable
            | EffectJournalError::StaleEpoch
            | EffectJournalError::InvalidAssignment
            | EffectJournalError::Sequence
    )
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EffectJournalRow {
    effect_id: Uuid,
    ownership_epoch: u64,
    committed: Option<EffectMutation>,
    prepared: Option<EffectMutation>,
}

/// One storage Node's durable journal partition for effect decisions. Rows
/// are keyed by EffectId and carry at most one committed decision chain —
/// `Declare` then a terminal `Applied`/`Aborted` — plus one in-flight
/// prepared mutation.
pub struct FjallEffectJournalReplica {
    db: fjall::SingleWriterTxDatabase,
    journal: fjall::SingleWriterTxKeyspace,
}

impl FjallEffectJournalReplica {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, EffectJournalError> {
        let db = fjall::SingleWriterTxDatabase::builder(path).open()?;
        let journal = db.keyspace("effect_journal", fjall::KeyspaceCreateOptions::default)?;
        Ok(Self { db, journal })
    }

    fn read_row(&self, effect_id: Uuid) -> Result<Option<EffectJournalRow>, EffectJournalError> {
        self.db
            .read_tx()
            .get(&self.journal, effect_id.as_bytes())?
            .map(|bytes| serde_json::from_slice::<EffectJournalRow>(&bytes))
            .transpose()
            .map_err(EffectJournalError::from)
    }

    fn check_row(row: &EffectJournalRow, effect_id: Uuid) -> Result<(), EffectJournalError> {
        let mismatched = |mutation: &EffectMutation| mutation.effect_id != effect_id;
        if row.effect_id != effect_id
            || row.committed.as_ref().is_some_and(mismatched)
            || row.prepared.as_ref().is_some_and(mismatched)
        {
            return Err(EffectJournalError::Conflict);
        }
        Ok(())
    }

    /// The committed mutation for one effect, if the row exists.
    pub fn local_committed(
        &self,
        effect_id: Uuid,
    ) -> Result<Option<EffectMutation>, EffectJournalError> {
        match self.read_row(effect_id)? {
            Some(row) => {
                Self::check_row(&row, effect_id)?;
                Ok(row.committed)
            }
            None => Ok(None),
        }
    }

    /// Committed and prepared state for recovery evidence.
    pub fn local_state(
        &self,
        effect_id: Uuid,
    ) -> Result<EffectJournalInspection, EffectJournalError> {
        match self.read_row(effect_id)? {
            Some(row) => {
                Self::check_row(&row, effect_id)?;
                Ok(EffectJournalInspection {
                    committed: row.committed,
                    prepared: row.prepared,
                })
            }
            None => Ok(EffectJournalInspection::default()),
        }
    }

    /// Install a recovered decision under a higher ownership epoch. Adoption
    /// never rolls back or forks a committed decision: an existing committed
    /// row with a newer or conflicting same-sequence mutation is refused,
    /// while identical or lagging state is retained.
    pub fn adopt_recovered(
        &self,
        effect_id: Uuid,
        subscription_id: Uuid,
        ownership_epoch: u64,
        committed: Option<EffectMutation>,
    ) -> Result<Option<EffectMutation>, EffectJournalError> {
        if ownership_epoch == 0 {
            return Err(EffectJournalError::StaleEpoch);
        }
        if committed.as_ref().is_some_and(|mutation| {
            mutation.effect_id != effect_id
                || mutation.subscription_id != subscription_id
                || mutation.ownership_epoch > ownership_epoch
        }) {
            return Err(EffectJournalError::Conflict);
        }
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let previous = tx
            .get(&self.journal, effect_id.as_bytes())?
            .map(|bytes| serde_json::from_slice::<EffectJournalRow>(&bytes))
            .transpose()?;
        if let Some(row) = &previous {
            Self::check_row(row, effect_id)?;
            if row.ownership_epoch > ownership_epoch {
                return Err(EffectJournalError::StaleEpoch);
            }
            if let Some(existing) = &row.committed {
                match &committed {
                    // Never roll back, erase, or fork a committed decision.
                    Some(candidate) if existing.sequence > candidate.sequence => {
                        return Err(EffectJournalError::Conflict)
                    }
                    Some(candidate)
                        if existing.sequence == candidate.sequence && existing != candidate =>
                    {
                        return Err(EffectJournalError::Conflict)
                    }
                    None => return Err(EffectJournalError::Conflict),
                    _ => {}
                }
            }
        }
        let mut row = previous.unwrap_or(EffectJournalRow {
            effect_id,
            ownership_epoch,
            committed: None,
            prepared: None,
        });
        if let Some(candidate) = committed {
            let newer = row
                .committed
                .as_ref()
                .is_none_or(|existing| candidate.sequence >= existing.sequence);
            if newer {
                row.committed = Some(candidate);
            }
        }
        row.ownership_epoch = ownership_epoch;
        row.prepared = None;
        tx.insert(
            &self.journal,
            row.effect_id.as_bytes(),
            serde_json::to_vec(&row)?,
        );
        tx.commit()?;
        Ok(row.committed)
    }

    /// Durably prepare one mutation. A prepared mutation is invisible to
    /// committed reads until a matching quorum commit lands.
    pub fn prepare(
        &self,
        mutation: EffectMutation,
    ) -> Result<EffectPrepareVote, EffectJournalError> {
        validate_effect_bounds(&mutation)?;
        let bytes = serde_json::to_vec(&mutation)?;
        let vote = EffectPrepareVote {
            effect_id: mutation.effect_id,
            request_id: mutation.request_id,
            digest: *blake3::hash(&bytes).as_bytes(),
        };
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let previous = tx
            .get(&self.journal, mutation.effect_id.as_bytes())?
            .map(|bytes| serde_json::from_slice::<EffectJournalRow>(&bytes))
            .transpose()?;
        let mut row = match previous {
            Some(row) => row,
            None => EffectJournalRow {
                effect_id: mutation.effect_id,
                ownership_epoch: mutation.ownership_epoch,
                committed: None,
                prepared: None,
            },
        };
        if row.effect_id != mutation.effect_id {
            return Err(EffectJournalError::Conflict);
        }
        if row.ownership_epoch != mutation.ownership_epoch {
            return Err(EffectJournalError::StaleEpoch);
        }
        if let Some(committed) = &row.committed {
            if committed.request_id == mutation.request_id {
                return if committed == &mutation {
                    Ok(vote)
                } else {
                    Err(EffectJournalError::Conflict)
                };
            }
        }
        if let Some(prepared) = &row.prepared {
            if prepared == &mutation {
                return Ok(vote);
            }
            if prepared.request_id == mutation.request_id {
                return Err(EffectJournalError::Conflict);
            }
            // A residue from an abandoned vote: this replica is behind the
            // quorum's committed trail, so it answers as a lagging witness
            // rather than contradicting the mutation.
            return Err(EffectJournalError::Sequence);
        }
        let prior_sequence = row.committed.as_ref().map_or(0, |prior| prior.sequence);
        if prior_sequence.checked_add(1) != Some(mutation.sequence) {
            return Err(EffectJournalError::Sequence);
        }
        validate_transition(row.committed.as_ref(), &mutation)?;
        row.prepared = Some(mutation);
        let encoded = serde_json::to_vec(&row)?;
        if encoded.len() > EFFECT_MAX_MUTATION_BYTES {
            return Err(EffectJournalError::TooLarge);
        }
        tx.insert(&self.journal, row.effect_id.as_bytes(), encoded);
        tx.commit()?;
        Ok(vote)
    }

    /// Commit a previously prepared mutation once two distinct assigned
    /// replicas report matching digests.
    pub fn commit_with_quorum(
        &self,
        evidence: EffectCommitEvidence,
    ) -> Result<EffectMutation, EffectJournalError> {
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let bytes = tx
            .get(&self.journal, evidence.effect_id.as_bytes())?
            .ok_or(EffectJournalError::Conflict)?;
        let mut row: EffectJournalRow = serde_json::from_slice(&bytes)?;
        Self::check_row(&row, evidence.effect_id)?;
        let candidate = row
            .prepared
            .as_ref()
            .filter(|value| value.request_id == evidence.request_id)
            .or_else(|| {
                row.committed
                    .as_ref()
                    .filter(|value| value.request_id == evidence.request_id)
            })
            .ok_or(EffectJournalError::Conflict)?;
        let digest = *blake3::hash(&serde_json::to_vec(candidate)?).as_bytes();
        if evidence.request_id != candidate.request_id
            || evidence.votes[0].0 == evidence.votes[1].0
            || evidence.votes.iter().any(|(_, value)| *value != digest)
        {
            return Err(EffectJournalError::NoQuorum);
        }
        if row
            .prepared
            .as_ref()
            .is_none_or(|value| value.request_id != evidence.request_id)
        {
            return row.committed.ok_or(EffectJournalError::Conflict);
        }
        let committed = row.prepared.take().ok_or(EffectJournalError::Conflict)?;
        row.committed = Some(committed.clone());
        tx.insert(
            &self.journal,
            row.effect_id.as_bytes(),
            serde_json::to_vec(&row)?,
        );
        tx.commit()?;
        Ok(committed)
    }
}

impl EffectTransition {
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Applied { .. } | Self::Aborted { .. })
    }
}

fn validate_effect_bounds(mutation: &EffectMutation) -> Result<(), EffectJournalError> {
    if mutation.ownership_epoch == 0 || mutation.sequence == 0 {
        return Err(EffectJournalError::TooLarge);
    }
    match &mutation.transition {
        EffectTransition::Declare { consume, outputs } => {
            if outputs.len() > EFFECT_MAX_OUTPUTS {
                return Err(EffectJournalError::TooLarge);
            }
            if outputs.iter().any(|output| {
                output.key_base64.is_empty()
                    || output.key_base64.len() > EFFECT_MAX_OUTPUT_FIELD
                    || output.payload_base64.len() > EFFECT_MAX_OUTPUT_FIELD
                    || output.sequence == 0
            }) {
                return Err(EffectJournalError::TooLarge);
            }
            if let Some(consume) = consume {
                if consume.cursor.is_empty()
                    || consume.cursor.len() > 256
                    || consume.positions.is_empty()
                    || consume.positions.len() > EFFECT_MAX_POSITIONS
                    || consume.positions.values().any(|value| value.len() > 256)
                    || consume
                        .expected_cursor
                        .as_ref()
                        .is_some_and(|expected| expected.is_empty() || expected.len() > 256)
                {
                    return Err(EffectJournalError::TooLarge);
                }
            }
        }
        EffectTransition::Applied { .. } => {}
        EffectTransition::Aborted { reason, .. } => {
            if reason.is_empty() || reason.len() > 256 {
                return Err(EffectJournalError::TooLarge);
            }
        }
    }
    Ok(())
}

/// Legality of the transition against the committed decision chain: a row
/// begins with `Declare` and ends with exactly one terminal transition.
fn validate_transition(
    committed: Option<&EffectMutation>,
    mutation: &EffectMutation,
) -> Result<(), EffectJournalError> {
    match (&mutation.transition, committed) {
        (EffectTransition::Declare { .. }, None) => Ok(()),
        (EffectTransition::Declare { .. }, Some(_)) => Err(EffectJournalError::Conflict),
        (
            EffectTransition::Applied { declares } | EffectTransition::Aborted { declares, .. },
            Some(prior),
        ) => {
            if prior.transition.is_terminal() {
                return Err(EffectJournalError::Conflict);
            }
            if *declares != prior.request_id {
                return Err(EffectJournalError::Conflict);
            }
            Ok(())
        }
        (EffectTransition::Applied { .. } | EffectTransition::Aborted { .. }, None) => {
            Err(EffectJournalError::Conflict)
        }
    }
}

/// Prepare/commit/read evidence against journal replicas.
#[async_trait::async_trait]
pub trait EffectJournalTransport: Send + Sync {
    async fn prepare(
        &self,
        replica: &crate::active_range::StorageNodeId,
        mutation: EffectMutation,
    ) -> Result<EffectReplicaReply<EffectPrepareVote>, EffectJournalError>;

    async fn commit(
        &self,
        replica: &crate::active_range::StorageNodeId,
        evidence: EffectCommitEvidence,
    ) -> Result<EffectReplicaReply<EffectMutation>, EffectJournalError>;

    async fn committed(
        &self,
        replica: &crate::active_range::StorageNodeId,
        effect_id: Uuid,
        ownership_epoch: u64,
    ) -> Result<EffectReplicaReply<Option<EffectMutation>>, EffectJournalError>;

    async fn inspect(
        &self,
        replica: &crate::active_range::StorageNodeId,
        effect_id: Uuid,
        ownership_epoch: u64,
    ) -> Result<EffectReplicaReply<EffectJournalInspection>, EffectJournalError>;

    async fn adopt(
        &self,
        replica: &crate::active_range::StorageNodeId,
        effect_id: Uuid,
        ownership_epoch: u64,
        committed: Option<EffectMutation>,
    ) -> Result<EffectReplicaReply<Option<EffectMutation>>, EffectJournalError>;
}

/// Drives mutations through the replicated prepare/commit protocol over an
/// injected transport. The coordinator is bound to one Subscription's
/// progress assignment: every effect in the scope shares that RF3 replica
/// set and ownership epoch, so draining or moving the Subscription's
/// progress placement moves the journal with it.
pub struct EffectCoordinator {
    assignment: crate::reader::SubscriptionProgressAssignment,
    transport: std::sync::Arc<dyn EffectJournalTransport>,
}

impl EffectCoordinator {
    pub fn new(
        assignment: crate::reader::SubscriptionProgressAssignment,
        transport: std::sync::Arc<dyn EffectJournalTransport>,
    ) -> Self {
        Self {
            assignment,
            transport,
        }
    }

    /// Resolve the coordinator for a Subscription's current progress
    /// placement through the catalog.
    pub async fn for_subscription(
        control: &crate::control::ControlController,
        subscription_id: Uuid,
        transport: std::sync::Arc<dyn EffectJournalTransport>,
    ) -> Result<Self, EffectJournalError> {
        let assignment = control
            .active_subscription_progress_assignment_by_id(subscription_id)
            .await
            .ok_or(EffectJournalError::InvalidAssignment)?;
        Ok(Self::new(assignment, transport))
    }

    /// The quorum-corroborated committed mutation for one effect, if any.
    pub async fn read_committed(
        &self,
        effect_id: Uuid,
    ) -> Result<Option<EffectMutation>, EffectJournalError> {
        let mut observed: Option<EffectMutation> = None;
        let mut votes = 0;
        let mut lagged = false;
        let replies = futures_util::future::join_all(self.assignment.replicas.iter().map(|node| {
            self.transport
                .committed(node, effect_id, self.assignment.ownership_epoch)
        }))
        .await;
        for (node, reply) in self.assignment.replicas.iter().zip(replies) {
            match reply {
                Ok(reply) => {
                    if reply.replica != *node
                        || reply.effect_id != effect_id
                        || reply.ownership_epoch != self.assignment.ownership_epoch
                    {
                        return Err(EffectJournalError::InvalidAssignment);
                    }
                    let mutation = reply.result;
                    if let Some(candidate) = mutation.as_ref() {
                        if candidate.effect_id != effect_id {
                            return Err(EffectJournalError::Conflict);
                        }
                        if candidate.ownership_epoch != self.assignment.ownership_epoch {
                            if candidate.ownership_epoch > self.assignment.ownership_epoch {
                                return Err(EffectJournalError::StaleEpoch);
                            }
                            lagged = true;
                            continue;
                        }
                    }
                    votes += 1;
                    if let Some(candidate) = mutation {
                        match &observed {
                            Some(current) if *current != candidate => {
                                match candidate.sequence.cmp(&current.sequence) {
                                    std::cmp::Ordering::Greater => observed = Some(candidate),
                                    std::cmp::Ordering::Equal => {
                                        return Err(EffectJournalError::Conflict)
                                    }
                                    std::cmp::Ordering::Less => {}
                                }
                            }
                            Some(_) => {}
                            None => observed = Some(candidate),
                        }
                    }
                }
                Err(error) if lagging_witness(&error) => lagged = true,
                Err(error) => return Err(error),
            }
        }
        if votes < 2 || (observed.is_none() && (votes < 3 || lagged)) {
            return Err(EffectJournalError::NoQuorum);
        }
        Ok(observed)
    }

    /// Prepare the mutation on every assigned replica, then commit it where
    /// matching digests prove durability. Owner participation is mandatory
    /// at both stages; anything weaker is ambiguous and must be reconciled
    /// by request identity.
    pub async fn apply(
        &self,
        mutation: EffectMutation,
    ) -> Result<EffectMutation, EffectJournalError> {
        if mutation.subscription_id != self.assignment.subscription_id
            || mutation.ownership_epoch != self.assignment.ownership_epoch
        {
            return Err(EffectJournalError::InvalidAssignment);
        }
        let mut prepared = Vec::with_capacity(3);
        let replies = futures_util::future::join_all(
            self.assignment
                .replicas
                .iter()
                .map(|node| self.transport.prepare(node, mutation.clone())),
        )
        .await;
        for (node, reply) in self.assignment.replicas.iter().zip(replies) {
            match reply {
                Ok(reply)
                    if reply.replica != *node
                        || reply.effect_id != mutation.effect_id
                        || reply.ownership_epoch != self.assignment.ownership_epoch =>
                {
                    return Err(EffectJournalError::InvalidAssignment)
                }
                Ok(reply) => prepared.push((node.clone(), reply.result.digest)),
                Err(error) if lagging_witness(&error) => {}
                Err(error) => return Err(error),
            }
        }
        if prepared.len() < 2
            || !prepared
                .iter()
                .any(|(node, _)| node == &self.assignment.owner)
        {
            return Err(EffectJournalError::NoQuorum);
        }
        let digest = prepared
            .iter()
            .find(|(node, _)| node == &self.assignment.owner)
            .map(|(_, digest)| *digest)
            .ok_or(EffectJournalError::NoQuorum)?;
        if prepared.iter().any(|(_, value)| *value != digest) {
            return Err(EffectJournalError::Conflict);
        }
        let other = prepared
            .iter()
            .find(|(node, _)| node != &self.assignment.owner)
            .map(|(node, _)| node.clone())
            .ok_or(EffectJournalError::NoQuorum)?;
        let evidence = EffectCommitEvidence {
            effect_id: mutation.effect_id,
            request_id: mutation.request_id,
            votes: [(self.assignment.owner.clone(), digest), (other, digest)],
        };
        let mut committed = Vec::with_capacity(3);
        let replies = futures_util::future::join_all(
            prepared
                .iter()
                .map(|(node, _)| self.transport.commit(node, evidence.clone())),
        )
        .await;
        for ((node, _), reply) in prepared.iter().zip(replies) {
            match reply {
                Ok(reply)
                    if reply.replica != *node
                        || reply.effect_id != mutation.effect_id
                        || reply.ownership_epoch != self.assignment.ownership_epoch =>
                {
                    return Err(EffectJournalError::InvalidAssignment)
                }
                Ok(reply) if reply.result == mutation => committed.push(node.clone()),
                Ok(_) => return Err(EffectJournalError::Conflict),
                Err(error) if lagging_witness(&error) => {}
                Err(EffectJournalError::Conflict) => {
                    return Err(EffectJournalError::AmbiguousCommit)
                }
                Err(error) => return Err(error),
            }
        }
        if committed.len() < 2 || !committed.contains(&self.assignment.owner) {
            return Err(EffectJournalError::AmbiguousCommit);
        }
        Ok(mutation)
    }

    /// After an ambiguous result, probe all assigned replicas for the same
    /// request identity: replay to complete partial commits, and only return
    /// once quorum evidence shows the mutation committed.
    pub async fn reconcile_retry(
        &self,
        mutation: EffectMutation,
    ) -> Result<EffectMutation, EffectJournalError> {
        if mutation.subscription_id != self.assignment.subscription_id
            || mutation.ownership_epoch != self.assignment.ownership_epoch
        {
            return Err(EffectJournalError::InvalidAssignment);
        }
        let mut reachable = 0;
        let mut owner_seen = false;
        let mut owner_committed = false;
        let mut matching = 0;
        let replies = futures_util::future::join_all(self.assignment.replicas.iter().map(|node| {
            self.transport
                .committed(node, mutation.effect_id, mutation.ownership_epoch)
        }))
        .await;
        for (node, reply) in self.assignment.replicas.iter().zip(replies) {
            match reply {
                Ok(reply) => {
                    if reply.replica != *node
                        || reply.effect_id != mutation.effect_id
                        || reply.ownership_epoch != mutation.ownership_epoch
                    {
                        return Err(EffectJournalError::InvalidAssignment);
                    }
                    reachable += 1;
                    if node == &self.assignment.owner {
                        owner_seen = true;
                    }
                    match reply.result {
                        Some(current) if current == mutation => {
                            matching += 1;
                            if node == &self.assignment.owner {
                                owner_committed = true;
                            }
                        }
                        Some(current) if current.sequence < mutation.sequence => {}
                        None => {}
                        _ => return Err(EffectJournalError::Conflict),
                    }
                }
                Err(error) if lagging_witness(&error) => {}
                Err(error) => return Err(error),
            }
        }
        if reachable < 2 || !owner_seen {
            return Err(EffectJournalError::NoQuorum);
        }
        if owner_committed && matching >= 2 {
            match self.read_committed(mutation.effect_id).await {
                Ok(Some(committed)) if committed == mutation => return Ok(mutation),
                Err(EffectJournalError::Conflict) => {}
                Err(error) => return Err(error),
                _ => return Err(EffectJournalError::AmbiguousCommit),
            }
        }
        match self.apply(mutation).await {
            Ok(committed) => Ok(committed),
            Err(error) => Err(error),
        }
    }
}

/// One Node's placement-fenced effect journal endpoint surface: every
/// request is checked against the Subscription progress assignment that owns
/// the journal scope before the durable replica is touched.
pub struct EffectJournalService {
    local_node: crate::active_range::StorageNodeId,
    control: std::sync::Arc<crate::control::ControlController>,
    replica: std::sync::Arc<FjallEffectJournalReplica>,
}

impl EffectJournalService {
    pub fn new(
        local_node: crate::active_range::StorageNodeId,
        control: std::sync::Arc<crate::control::ControlController>,
        replica: std::sync::Arc<FjallEffectJournalReplica>,
    ) -> Self {
        Self {
            local_node,
            control,
            replica,
        }
    }

    async fn check_placement(
        &self,
        subscription_id: Uuid,
        owner: &crate::active_range::StorageNodeId,
        receiver: &crate::active_range::StorageNodeId,
        epoch: u64,
    ) -> Result<crate::reader::SubscriptionProgressAssignment, EffectJournalError> {
        let assignment = self
            .control
            .active_subscription_progress_assignment_by_id(subscription_id)
            .await
            .ok_or(EffectJournalError::InvalidAssignment)?;
        if assignment.owner != *owner
            || epoch != assignment.ownership_epoch
            || *receiver != self.local_node
            || !assignment.replicas.iter().any(|node| node == receiver)
        {
            return Err(EffectJournalError::InvalidAssignment);
        }
        Ok(assignment)
    }

    pub async fn prepare(
        &self,
        request: EffectPrepareRequest,
    ) -> Result<EffectReplicaReply<EffectPrepareVote>, EffectJournalError> {
        let assignment = self
            .check_placement(
                request.mutation.subscription_id,
                &request.owner,
                &request.receiver,
                request.mutation.ownership_epoch,
            )
            .await?;
        let replica = self.replica.clone();
        let mutation = request.mutation;
        let effect_id = mutation.effect_id;
        let result = tokio::task::spawn_blocking(move || replica.prepare(mutation))
            .await
            .map_err(|_| EffectJournalError::Unavailable)??;
        Ok(EffectReplicaReply {
            replica: self.local_node.clone(),
            effect_id,
            ownership_epoch: assignment.ownership_epoch,
            result,
        })
    }

    pub async fn commit(
        &self,
        request: EffectCommitRequest,
    ) -> Result<EffectReplicaReply<EffectMutation>, EffectJournalError> {
        let assignment = self
            .check_placement(
                request.subscription_id,
                &request.owner,
                &request.receiver,
                request.ownership_epoch,
            )
            .await?;
        if request.evidence.effect_id != request.effect_id
            || request
                .evidence
                .votes
                .iter()
                .any(|(node, _)| !assignment.replicas.iter().any(|member| member == node))
            || !request
                .evidence
                .votes
                .iter()
                .any(|(node, _)| node == &assignment.owner)
        {
            return Err(EffectJournalError::InvalidAssignment);
        }
        let replica = self.replica.clone();
        let result =
            tokio::task::spawn_blocking(move || replica.commit_with_quorum(request.evidence))
                .await
                .map_err(|_| EffectJournalError::Unavailable)??;
        Ok(EffectReplicaReply {
            replica: self.local_node.clone(),
            effect_id: result.effect_id,
            ownership_epoch: result.ownership_epoch,
            result,
        })
    }

    pub async fn committed(
        &self,
        request: EffectReadRequest,
    ) -> Result<EffectReplicaReply<Option<EffectMutation>>, EffectJournalError> {
        let assignment = self
            .check_placement(
                request.subscription_id,
                &request.owner,
                &request.receiver,
                request.ownership_epoch,
            )
            .await?;
        let replica = self.replica.clone();
        let effect_id = request.effect_id;
        let committed = tokio::task::spawn_blocking(move || replica.local_committed(effect_id))
            .await
            .map_err(|_| EffectJournalError::Unavailable)??;
        if let Some(mutation) = &committed {
            if mutation.effect_id != effect_id
                || mutation.subscription_id != request.subscription_id
            {
                return Err(EffectJournalError::Conflict);
            }
            if mutation.ownership_epoch != request.ownership_epoch {
                return Err(EffectJournalError::StaleEpoch);
            }
        }
        Ok(EffectReplicaReply {
            replica: self.local_node.clone(),
            effect_id,
            ownership_epoch: assignment.ownership_epoch,
            result: committed,
        })
    }

    pub async fn inspect(
        &self,
        request: EffectReadRequest,
    ) -> Result<EffectReplicaReply<EffectJournalInspection>, EffectJournalError> {
        let assignment = self
            .check_placement(
                request.subscription_id,
                &request.owner,
                &request.receiver,
                request.ownership_epoch,
            )
            .await?;
        let replica = self.replica.clone();
        let effect_id = request.effect_id;
        let inspection = tokio::task::spawn_blocking(move || replica.local_state(effect_id))
            .await
            .map_err(|_| EffectJournalError::Unavailable)??;
        Ok(EffectReplicaReply {
            replica: self.local_node.clone(),
            effect_id,
            ownership_epoch: assignment.ownership_epoch,
            result: inspection,
        })
    }

    pub async fn adopt(
        &self,
        request: EffectAdoptRequest,
    ) -> Result<EffectReplicaReply<Option<EffectMutation>>, EffectJournalError> {
        let assignment = self
            .check_placement(
                request.subscription_id,
                &request.owner,
                &request.receiver,
                request.ownership_epoch,
            )
            .await?;
        let replica = self.replica.clone();
        let effect_id = request.effect_id;
        let subscription_id = request.subscription_id;
        let ownership_epoch = request.ownership_epoch;
        let result = tokio::task::spawn_blocking(move || {
            replica.adopt_recovered(
                effect_id,
                subscription_id,
                ownership_epoch,
                request.committed,
            )
        })
        .await
        .map_err(|_| EffectJournalError::Unavailable)??;
        Ok(EffectReplicaReply {
            replica: self.local_node.clone(),
            effect_id,
            ownership_epoch: assignment.ownership_epoch,
            result,
        })
    }
}

/// HTTP transport targeting the assigned journal replicas over the internal
/// plane; the mTLS variant presents the Node certificate so the receiving
/// listener authenticates the caller by its pinned identity.
pub struct HttpEffectJournalTransport {
    assignment: crate::reader::SubscriptionProgressAssignment,
    endpoints: crate::internal_plane::InternalEndpoints,
    key: Option<String>,
    client: reqwest::Client,
}

impl HttpEffectJournalTransport {
    pub fn new(
        assignment: crate::reader::SubscriptionProgressAssignment,
        endpoints: impl Into<crate::internal_plane::InternalEndpoints>,
        key: String,
        timeout: std::time::Duration,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            assignment,
            endpoints: endpoints.into(),
            key: Some(key),
            client: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(1))
                .timeout(timeout)
                .build()?,
        })
    }

    pub fn new_mtls(
        assignment: crate::reader::SubscriptionProgressAssignment,
        endpoints: impl Into<crate::internal_plane::InternalEndpoints>,
        ca_pem: &[u8],
        identity_pem: &[u8],
        timeout: std::time::Duration,
    ) -> Result<Self, EffectJournalError> {
        let endpoints = endpoints.into();
        for (node, endpoint) in endpoints.configured() {
            let url =
                reqwest::Url::parse(endpoint).map_err(|_| EffectJournalError::InvalidAssignment)?;
            if url.scheme() != "https" || url.host_str() != Some(node.as_str()) {
                return Err(EffectJournalError::InvalidAssignment);
            }
        }
        let ca = reqwest::Certificate::from_pem(ca_pem)
            .map_err(|_| EffectJournalError::InvalidAssignment)?;
        let identity = reqwest::Identity::from_pem(identity_pem)
            .map_err(|_| EffectJournalError::InvalidAssignment)?;
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(1))
            .timeout(timeout)
            .https_only(true)
            .tls_built_in_root_certs(false)
            .add_root_certificate(ca)
            .identity(identity)
            .build()
            .map_err(|_| EffectJournalError::InvalidAssignment)?;
        Ok(Self {
            assignment,
            endpoints,
            key: None,
            client,
        })
    }

    async fn send<R: Serialize, T: serde::de::DeserializeOwned>(
        &self,
        node: &crate::active_range::StorageNodeId,
        path: &str,
        body: &R,
    ) -> Result<T, EffectJournalError> {
        if !self.assignment.replicas.iter().any(|member| member == node) {
            return Err(EffectJournalError::InvalidAssignment);
        }
        let endpoint = self
            .endpoints
            .resolve(node)
            .await
            .ok_or(EffectJournalError::Unavailable)?;
        let mut request = self
            .client
            .post(format!("{}{path}", endpoint.trim_end_matches('/')))
            .json(body);
        if let Some(key) = &self.key {
            request = request.header("x-whitewater-control-key", key);
        }
        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(node = %node, path, %error, "effect journal request failed");
                return Err(EffectJournalError::Unavailable);
            }
        };
        if !response.status().is_success() {
            let status = response.status();
            let body = response.bytes().await.unwrap_or_default();
            tracing::warn!(node = %node, path, %status, "effect journal replica refused");
            let code = body
                .get(..4096)
                .and_then(|head| serde_json::from_slice::<serde_json::Value>(head).ok())
                .and_then(|value| {
                    value
                        .get("code")
                        .and_then(|code| code.as_str())
                        .map(str::to_owned)
                });
            if let Some(error) = code.as_deref().and_then(EffectJournalError::from_code) {
                return Err(error);
            }
            return Err(if status == reqwest::StatusCode::CONFLICT {
                EffectJournalError::Conflict
            } else {
                EffectJournalError::Unavailable
            });
        }
        serde_json::from_slice(
            &response
                .bytes()
                .await
                .map_err(|_| EffectJournalError::Unavailable)?,
        )
        .map_err(|_| EffectJournalError::Unavailable)
    }
}

#[async_trait::async_trait]
impl EffectJournalTransport for HttpEffectJournalTransport {
    async fn prepare(
        &self,
        replica: &crate::active_range::StorageNodeId,
        mutation: EffectMutation,
    ) -> Result<EffectReplicaReply<EffectPrepareVote>, EffectJournalError> {
        if mutation.subscription_id != self.assignment.subscription_id
            || mutation.ownership_epoch != self.assignment.ownership_epoch
        {
            return Err(EffectJournalError::InvalidAssignment);
        }
        self.send(
            replica,
            "/internal/effect-journal/prepare",
            &EffectPrepareRequest {
                owner: self.assignment.owner.clone(),
                receiver: replica.clone(),
                mutation,
            },
        )
        .await
    }

    async fn commit(
        &self,
        replica: &crate::active_range::StorageNodeId,
        evidence: EffectCommitEvidence,
    ) -> Result<EffectReplicaReply<EffectMutation>, EffectJournalError> {
        self.send(
            replica,
            "/internal/effect-journal/commit",
            &EffectCommitRequest {
                owner: self.assignment.owner.clone(),
                receiver: replica.clone(),
                subscription_id: self.assignment.subscription_id,
                effect_id: evidence.effect_id,
                ownership_epoch: self.assignment.ownership_epoch,
                evidence,
            },
        )
        .await
    }

    async fn committed(
        &self,
        replica: &crate::active_range::StorageNodeId,
        effect_id: Uuid,
        ownership_epoch: u64,
    ) -> Result<EffectReplicaReply<Option<EffectMutation>>, EffectJournalError> {
        if ownership_epoch != self.assignment.ownership_epoch {
            return Err(EffectJournalError::StaleEpoch);
        }
        self.send(
            replica,
            "/internal/effect-journal/committed",
            &EffectReadRequest {
                owner: self.assignment.owner.clone(),
                receiver: replica.clone(),
                subscription_id: self.assignment.subscription_id,
                effect_id,
                ownership_epoch,
            },
        )
        .await
    }

    async fn inspect(
        &self,
        replica: &crate::active_range::StorageNodeId,
        effect_id: Uuid,
        ownership_epoch: u64,
    ) -> Result<EffectReplicaReply<EffectJournalInspection>, EffectJournalError> {
        if ownership_epoch != self.assignment.ownership_epoch {
            return Err(EffectJournalError::StaleEpoch);
        }
        self.send(
            replica,
            "/internal/effect-journal/inspect",
            &EffectReadRequest {
                owner: self.assignment.owner.clone(),
                receiver: replica.clone(),
                subscription_id: self.assignment.subscription_id,
                effect_id,
                ownership_epoch,
            },
        )
        .await
    }

    async fn adopt(
        &self,
        replica: &crate::active_range::StorageNodeId,
        effect_id: Uuid,
        ownership_epoch: u64,
        committed: Option<EffectMutation>,
    ) -> Result<EffectReplicaReply<Option<EffectMutation>>, EffectJournalError> {
        if ownership_epoch < self.assignment.ownership_epoch {
            return Err(EffectJournalError::StaleEpoch);
        }
        self.send(
            replica,
            "/internal/effect-journal/adopt",
            &EffectAdoptRequest {
                owner: self.assignment.owner.clone(),
                receiver: replica.clone(),
                subscription_id: self.assignment.subscription_id,
                effect_id,
                ownership_epoch,
                committed,
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::active_range::StorageNodeId;
    use tempfile::TempDir;

    fn output(sequence: u64) -> EffectOutput {
        EffectOutput {
            feed_id: Uuid::new_v4(),
            key_base64: "a2V5".to_owned(),
            payload_base64: "cGF5bG9hZA==".to_owned(),
            event_time_ns: 1,
            writer_session_id: Uuid::new_v4(),
            sequence,
        }
    }

    fn consume() -> EffectConsume {
        EffectConsume {
            feed_id: Uuid::new_v4(),
            expected_cursor: None,
            cursor: "cursor-1".to_owned(),
            positions: BTreeMap::from([(RangeId::from_uuid(Uuid::from_u128(1)), "p-1".to_owned())]),
        }
    }

    fn declare(sequence: u64) -> EffectMutation {
        EffectMutation {
            effect_id: Uuid::new_v4(),
            subscription_id: Uuid::new_v4(),
            ownership_epoch: 1,
            sequence,
            request_id: Uuid::new_v4(),
            transition: EffectTransition::Declare {
                consume: Some(consume()),
                outputs: vec![output(1)],
            },
        }
    }

    fn evidence(effect_id: Uuid, request_id: Uuid, digest: [u8; 32]) -> EffectCommitEvidence {
        EffectCommitEvidence::new(
            effect_id,
            request_id,
            StorageNodeId::try_new("control-1").unwrap(),
            digest,
            StorageNodeId::try_new("control-2").unwrap(),
            digest,
        )
    }

    #[test]
    fn prepared_mutations_stay_invisible_until_quorum_commit() {
        let directory = TempDir::new().unwrap();
        let replica = FjallEffectJournalReplica::open(directory.path()).unwrap();
        let mutation = declare(1);
        let vote = replica.prepare(mutation.clone()).unwrap();
        assert_eq!(replica.local_committed(mutation.effect_id).unwrap(), None);
        let committed = replica
            .commit_with_quorum(evidence(
                mutation.effect_id,
                mutation.request_id,
                vote.digest,
            ))
            .unwrap();
        assert_eq!(committed, mutation);
        assert_eq!(
            replica.local_committed(mutation.effect_id).unwrap(),
            Some(mutation)
        );
    }

    #[test]
    fn identical_prepare_retries_are_idempotent_and_conflicts_fence() {
        let directory = TempDir::new().unwrap();
        let replica = FjallEffectJournalReplica::open(directory.path()).unwrap();
        let mutation = declare(1);
        let first = replica.prepare(mutation.clone()).unwrap();
        assert_eq!(replica.prepare(mutation.clone()).unwrap(), first);
        let mut conflicting = mutation.clone();
        conflicting.sequence = 7;
        assert!(matches!(
            replica.prepare(conflicting),
            Err(EffectJournalError::Conflict)
        ));
        let other = EffectMutation {
            request_id: Uuid::new_v4(),
            ..mutation.clone()
        };
        assert!(matches!(
            replica.prepare(other),
            Err(EffectJournalError::Sequence)
        ));
    }

    #[test]
    fn committed_rows_survive_restart_and_terminal_transitions_fence() {
        let directory = TempDir::new().unwrap();
        let replica = FjallEffectJournalReplica::open(directory.path()).unwrap();
        let mutation = declare(1);
        let vote = replica.prepare(mutation.clone()).unwrap();
        replica
            .commit_with_quorum(evidence(
                mutation.effect_id,
                mutation.request_id,
                vote.digest,
            ))
            .unwrap();
        drop(replica);
        let replica = FjallEffectJournalReplica::open(directory.path()).unwrap();
        assert_eq!(
            replica.local_committed(mutation.effect_id).unwrap(),
            Some(mutation.clone())
        );
        let applied = EffectMutation {
            sequence: 2,
            request_id: Uuid::new_v4(),
            transition: EffectTransition::Applied {
                declares: mutation.request_id,
            },
            ..mutation.clone()
        };
        let vote = replica.prepare(applied.clone()).unwrap();
        replica
            .commit_with_quorum(evidence(applied.effect_id, applied.request_id, vote.digest))
            .unwrap();
        assert!(matches!(
            replica.prepare(EffectMutation {
                sequence: 3,
                request_id: Uuid::new_v4(),
                transition: EffectTransition::Applied {
                    declares: applied.request_id,
                },
                ..applied.clone()
            }),
            Err(EffectJournalError::Conflict)
        ));
    }

    #[test]
    fn terminal_transitions_require_a_committed_declare() {
        let directory = TempDir::new().unwrap();
        let replica = FjallEffectJournalReplica::open(directory.path()).unwrap();
        let mutation = declare(1);
        let applied = EffectMutation {
            sequence: 1,
            request_id: Uuid::new_v4(),
            transition: EffectTransition::Applied {
                declares: mutation.request_id,
            },
            ..mutation.clone()
        };
        assert!(matches!(
            replica.prepare(applied),
            Err(EffectJournalError::Conflict)
        ));
    }

    #[test]
    fn stale_epochs_and_wrong_declare_references_are_refused() {
        let directory = TempDir::new().unwrap();
        let replica = FjallEffectJournalReplica::open(directory.path()).unwrap();
        let mutation = declare(1);
        replica.prepare(mutation.clone()).unwrap();
        let mut stale = mutation.clone();
        stale.ownership_epoch = 2;
        assert!(matches!(
            replica.prepare(stale),
            Err(EffectJournalError::StaleEpoch)
        ));
        let vote = replica.prepare(mutation.clone()).unwrap();
        replica
            .commit_with_quorum(evidence(
                mutation.effect_id,
                mutation.request_id,
                vote.digest,
            ))
            .unwrap();
        let wrong = EffectMutation {
            sequence: 2,
            request_id: Uuid::new_v4(),
            transition: EffectTransition::Applied {
                declares: Uuid::new_v4(),
            },
            ..mutation.clone()
        };
        assert!(matches!(
            replica.prepare(wrong),
            Err(EffectJournalError::Conflict)
        ));
    }

    #[test]
    fn oversized_mutations_are_refused_before_durability() {
        let directory = TempDir::new().unwrap();
        let replica = FjallEffectJournalReplica::open(directory.path()).unwrap();
        let mut mutation = declare(1);
        mutation.transition = EffectTransition::Declare {
            consume: None,
            outputs: (0..=EFFECT_MAX_OUTPUTS as u64)
                .map(output)
                .collect::<Vec<_>>(),
        };
        assert!(matches!(
            replica.prepare(mutation),
            Err(EffectJournalError::TooLarge)
        ));
        let mut mutation = declare(1);
        mutation.transition = EffectTransition::Declare {
            consume: None,
            outputs: vec![EffectOutput {
                payload_base64: "x".repeat(EFFECT_MAX_OUTPUT_FIELD + 1),
                ..output(1)
            }],
        };
        assert!(matches!(
            replica.prepare(mutation),
            Err(EffectJournalError::TooLarge)
        ));
    }

    #[test]
    fn adopt_recovered_never_rolls_back_committed_decisions() {
        let directory = TempDir::new().unwrap();
        let replica = FjallEffectJournalReplica::open(directory.path()).unwrap();
        let mutation = declare(1);
        let vote = replica.prepare(mutation.clone()).unwrap();
        replica
            .commit_with_quorum(evidence(
                mutation.effect_id,
                mutation.request_id,
                vote.digest,
            ))
            .unwrap();
        assert!(matches!(
            replica.adopt_recovered(mutation.effect_id, mutation.subscription_id, 2, None),
            Err(EffectJournalError::Conflict)
        ));
        let mut fork = mutation.clone();
        fork.request_id = Uuid::new_v4();
        assert!(matches!(
            replica.adopt_recovered(mutation.effect_id, mutation.subscription_id, 2, Some(fork)),
            Err(EffectJournalError::Conflict)
        ));
        assert_eq!(
            replica
                .adopt_recovered(
                    mutation.effect_id,
                    mutation.subscription_id,
                    2,
                    Some(mutation.clone())
                )
                .unwrap(),
            Some(mutation.clone())
        );
        let state = replica.local_state(mutation.effect_id).unwrap();
        assert_eq!(state.committed, Some(mutation.clone()));
        assert!(matches!(
            replica.adopt_recovered(
                state.committed.clone().unwrap().effect_id,
                mutation.subscription_id,
                1,
                None
            ),
            Err(EffectJournalError::StaleEpoch) | Err(EffectJournalError::Conflict)
        ));
    }

    #[test]
    fn adopt_recovered_seeds_a_lagging_replica() {
        let directory = TempDir::new().unwrap();
        let replica = FjallEffectJournalReplica::open(directory.path()).unwrap();
        let mutation = declare(1);
        assert_eq!(
            replica
                .adopt_recovered(
                    mutation.effect_id,
                    mutation.subscription_id,
                    1,
                    Some(mutation.clone())
                )
                .unwrap(),
            Some(mutation.clone())
        );
        assert_eq!(
            replica.local_committed(mutation.effect_id).unwrap(),
            Some(mutation)
        );
    }
}
