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
/// Maximum declared co-located StateStore mutations one effect may carry.
pub const EFFECT_MAX_STATE_MUTATIONS: usize = 16;
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
    #[serde(default)]
    pub metadata_base64: std::collections::BTreeMap<String, String>,
    pub event_time_ns: i64,
    pub writer_session_id: Uuid,
    pub writer_epoch: u64,
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

/// One declared co-located StateStore mutation inside the effect's atomic
/// boundary. The mutation commits through the store's own RF3 journal only
/// after the effect's terminal marker is durable, and `mutation_id` is a
/// deterministic dedupe identity so a replayed apply commits exactly one
/// row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectStateMutation {
    pub store_id: Uuid,
    /// Client-dedupe identity on the store journal. Producers derive it
    /// with `effect_state_mutation_id(effect_id, index)` so every replay
    /// targets the same row.
    pub mutation_id: Uuid,
    pub op: crate::state_store::StateStoreMutationOp,
}

/// The lifecycle transition a journal mutation commits.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum EffectTransition {
    /// The effect decision: consumed input evidence plus declared outputs.
    Declare {
        consume: Option<EffectConsume>,
        outputs: Vec<EffectOutput>,
        /// Co-located StateStore mutations released with the effect.
        #[serde(default)]
        state_mutations: Vec<EffectStateMutation>,
    },
    /// Every declared output durably appended and the consume frontier
    /// committed; `declares` identifies the committed `Declare` request.
    /// `outputs` restates the declared outputs so a terminal row alone can
    /// still drive release of the pending output tail after any crash.
    Applied {
        declares: Uuid,
        #[serde(default)]
        outputs: Vec<EffectOutput>,
        /// The declared store mutations, restated so a terminal row alone
        /// replays them after a crash between marker and mutation commits.
        #[serde(default)]
        state_mutations: Vec<EffectStateMutation>,
    },
    /// The decision was abandoned by policy (for example a poison input was
    /// quarantined); `declares` identifies the committed `Declare` request.
    Aborted {
        declares: Uuid,
        reason: String,
        /// The declared outputs whose pending tails must be discarded on
        /// apply replay; restated on the terminal row like `Applied` so a
        /// post-crash replay is self-contained.
        #[serde(default)]
        outputs: Vec<EffectOutput>,
        /// The abandoned consume, restated so a post-crash replay can still
        /// pass the declared input over the frontier as skipped rather than
        /// re-planning the dead effect forever.
        #[serde(default)]
        consume: Option<EffectConsume>,
    },
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

/// Bounded per-step retries settle transient or ambiguous sink results in
/// place: each retry replays the identical deterministic identity, so the
/// outcome converges to exactly one result and a genuinely stuck
/// dependency still fails the apply closed.
const MAX_APPLY_ATTEMPTS: usize = 3;

/// Bounded occupant sweeps per output Range before failing closed.
const MAX_PENDING_SWEEPS: usize = 4;

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
    /// The journal row's current fencing epoch: the assignment epoch the
    /// replica last adopted, independent of any committed decision's
    /// historical stamp.
    #[serde(default)]
    pub ownership_epoch: u64,
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
    #[error("an output Active Range tail is held by another pending effect")]
    PendingOccupied {
        effect_id: Uuid,
        subscription_id: Uuid,
    },
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
            Self::PendingOccupied { .. } => "pending_occupied",
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

/// `ownership_epoch` stamps which assignment issued a mutation; it fences
/// writes but is not part of the replicated decision. A replayed identical
/// decision under a moved epoch is the same decision, so equality and
/// quorum digests compare decision content only.
fn decisions_equal(left: &EffectMutation, right: &EffectMutation) -> bool {
    EffectMutation {
        ownership_epoch: 0,
        ..left.clone()
    } == EffectMutation {
        ownership_epoch: 0,
        ..right.clone()
    }
}

/// Quorum digest over decision content - the epoch stamp excluded for the
/// same reason `decisions_equal` excludes it: a stored decision and its
/// epoch-moved replay hash identically so commit evidence always matches
/// the stored candidate.
fn decision_digest(mutation: &EffectMutation) -> Result<[u8; 32], EffectJournalError> {
    let canonical = EffectMutation {
        ownership_epoch: 0,
        ..mutation.clone()
    };
    let bytes = serde_json::to_vec(&canonical)?;
    Ok(*blake3::hash(&bytes).as_bytes())
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
                    ownership_epoch: row.ownership_epoch,
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
                        if existing.sequence == candidate.sequence
                            && !decisions_equal(existing, candidate) =>
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
        let vote = EffectPrepareVote {
            effect_id: mutation.effect_id,
            request_id: mutation.request_id,
            digest: decision_digest(&mutation)?,
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
        // Fencing is directional: the placement check upstream already
        // proved the request epoch is the authoritative assignment, so a
        // row fenced behind it ratchets forward on install below rather
        // than wedging the mutation chain. A mutation stamped under an
        // older epoch than the row dies here as a stale write.
        if row.ownership_epoch > mutation.ownership_epoch {
            return Err(EffectJournalError::StaleEpoch);
        }
        if let Some(committed) = &row.committed {
            if committed.request_id == mutation.request_id {
                return if decisions_equal(committed, &mutation) {
                    Ok(vote)
                } else {
                    Err(EffectJournalError::Conflict)
                };
            }
        }
        if let Some(prepared) = &row.prepared {
            if decisions_equal(prepared, &mutation) {
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
        row.ownership_epoch = mutation.ownership_epoch;
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
        let digest = decision_digest(candidate)?;
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
    let state_mutation_bounded = |mutation: &EffectStateMutation| {
        mutation.mutation_id != Uuid::nil()
            && serde_json::to_vec(&mutation.op)
                .map(|encoded| encoded.len() <= EFFECT_MAX_OUTPUT_FIELD)
                .unwrap_or(false)
    };
    match &mutation.transition {
        EffectTransition::Declare {
            consume,
            outputs,
            state_mutations,
        } => {
            if outputs.len() > EFFECT_MAX_OUTPUTS
                || state_mutations.len() > EFFECT_MAX_STATE_MUTATIONS
                || !state_mutations.iter().all(state_mutation_bounded)
            {
                return Err(EffectJournalError::TooLarge);
            }
            if outputs.iter().any(|output| {
                output.key_base64.is_empty()
                    || output.key_base64.len() > EFFECT_MAX_OUTPUT_FIELD
                    || output.payload_base64.len() > EFFECT_MAX_OUTPUT_FIELD
                    || output
                        .metadata_base64
                        .values()
                        .any(|value| value.len() > EFFECT_MAX_OUTPUT_FIELD)
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
        EffectTransition::Applied {
            outputs,
            state_mutations,
            ..
        } => {
            if outputs.len() > EFFECT_MAX_OUTPUTS
                || state_mutations.len() > EFFECT_MAX_STATE_MUTATIONS
                || !state_mutations.iter().all(state_mutation_bounded)
                || outputs.iter().any(|output| {
                    output.key_base64.is_empty()
                        || output.key_base64.len() > EFFECT_MAX_OUTPUT_FIELD
                        || output.payload_base64.len() > EFFECT_MAX_OUTPUT_FIELD
                        || output.sequence == 0
                })
            {
                return Err(EffectJournalError::TooLarge);
            }
        }
        EffectTransition::Aborted {
            reason, outputs, ..
        } => {
            if reason.is_empty()
                || reason.len() > 256
                || outputs.len() > EFFECT_MAX_OUTPUTS
                || outputs.iter().any(|output| {
                    output.key_base64.is_empty()
                        || output.key_base64.len() > EFFECT_MAX_OUTPUT_FIELD
                        || output.payload_base64.len() > EFFECT_MAX_OUTPUT_FIELD
                        || output.sequence == 0
                })
            {
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
            EffectTransition::Applied { declares, .. } | EffectTransition::Aborted { declares, .. },
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
                        // The committed decision's stamped epoch is history,
                        // not fencing: a decision committed under a prior
                        // epoch still counts once the row adopted forward.
                        if candidate.ownership_epoch > self.assignment.ownership_epoch {
                            return Err(EffectJournalError::StaleEpoch);
                        }
                    }
                    votes += 1;
                    if let Some(candidate) = mutation {
                        match &observed {
                            Some(current) if !decisions_equal(current, &candidate) => {
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
                Ok(reply) if decisions_equal(&reply.result, &mutation) => {
                    committed.push(node.clone())
                }
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
                        Some(current) if decisions_equal(&current, &mutation) => {
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
                Ok(Some(committed)) if decisions_equal(&committed, &mutation) => {
                    return Ok(mutation)
                }
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

    /// Drive a committed `Declare` to its terminal `Applied` marker and then
    /// release the pending output tail. The phases are idempotent and ordered
    /// so a crash anywhere converges on retry:
    ///
    /// 1. **Ensure outputs**: every declared output appends under the pending
    ///    fence - durable on the output Range quorum but fenced behind the
    ///    committed boundary, so no reader can observe it yet.
    /// 2. **Release**: each touched output Range's pending tail commits,
    ///    making every output visible in one step per Range.
    /// 3. **Mutations**: each declared co-located StateStore mutation commits
    ///    through its own RF3 journal under the dedupe identity.
    /// 4. **Frontier**: the consumed Subscription frontier commits last, so an
    ///    interrupted apply leaves the input unconsumed and the next drive
    ///    refetches the identical page instead of skipping it.
    /// 5. **`Applied`**: the terminal journal marker commits - the effect's
    ///    decision is final and replayable from this row alone.
    ///
    /// An already-`Applied` row still re-runs the ensure and release phases:
    /// replay re-appends outputs idempotently and releases any tail the
    /// earlier attempt left pending. A `PendingOccupied` refusal defers to
    /// `resolve_pending` so the occupant's own apply can settle the tail.
    /// Transient or ambiguous sink results (`Unavailable`, `NoQuorum`,
    /// `AmbiguousCommit`) retry in place a bounded number of times: every
    /// step replays an identical deterministic identity, so settling an
    /// ambiguous result is repetition, not a new decision.
    pub async fn apply_committed<A: EffectApply + ?Sized>(
        &self,
        effect_id: Uuid,
        apply: &A,
    ) -> Result<EffectMutation, EffectJournalError> {
        let committed = self
            .read_committed(effect_id)
            .await?
            .ok_or(EffectJournalError::Conflict)?;
        if committed.subscription_id != self.assignment.subscription_id {
            return Err(EffectJournalError::InvalidAssignment);
        }
        let (consume, outputs, state_mutations) = match &committed.transition {
            EffectTransition::Declare {
                consume,
                outputs,
                state_mutations,
            } => (
                consume.as_ref(),
                outputs.as_slice(),
                state_mutations.as_slice(),
            ),
            EffectTransition::Applied {
                outputs,
                state_mutations,
                ..
            } => (None, outputs.as_slice(), state_mutations.as_slice()),
            EffectTransition::Aborted {
                declares,
                outputs,
                consume,
                ..
            } => {
                // Terminal replay: the abort decision is durable; discard
                // any pending tail that survived the first pass so the
                // output Ranges unwedge, then pass the abandoned input over
                // the frontier as skipped so the dead effect never re-plans.
                self.discard_declared_outputs(outputs, effect_id, committed.subscription_id, apply)
                    .await?;
                self.skip_abandoned_consume(consume.as_ref(), *declares, apply)
                    .await?;
                return Ok(committed);
            }
        };
        let is_declare = matches!(committed.transition, EffectTransition::Declare { .. });
        match self
            .apply_phases(
                &committed,
                effect_id,
                consume,
                outputs,
                state_mutations,
                apply,
            )
            .await
        {
            Ok(terminal) => Ok(terminal),
            // A non-retryable failure before the terminal marker must not
            // wedge the output Ranges: journal `Aborted`, then discard
            // this effect's pending tails so the Range frees for the next
            // effect.
            Err(error) if is_declare && !effect_retryable(&error) => {
                self.abort_declared(&committed, outputs, error, apply).await
            }
            Err(error) => Err(error),
        }
    }

    /// The append, frontier, terminal-marker, release, and state-mutation
    /// phases for a non-terminal row — and the replay of those phases for
    /// an already-`Applied` row.
    async fn apply_phases<A: EffectApply + ?Sized>(
        &self,
        committed: &EffectMutation,
        effect_id: Uuid,
        consume: Option<&EffectConsume>,
        outputs: &[EffectOutput],
        state_mutations: &[EffectStateMutation],
        apply: &A,
    ) -> Result<EffectMutation, EffectJournalError> {
        let mut receipts = Vec::with_capacity(outputs.len());
        for output in outputs {
            let mut sweeps = 0;
            let mut attempts = 0;
            loop {
                match apply.append_output(output, effect_id).await {
                    Ok(receipt) => {
                        receipts.push(receipt);
                        break;
                    }
                    Err(EffectJournalError::PendingOccupied {
                        effect_id: occupant_effect,
                        subscription_id: occupant_subscription,
                    }) if sweeps < MAX_PENDING_SWEEPS => {
                        sweeps += 1;
                        match apply
                            .resolve_pending(EffectOccupant {
                                effect_id: occupant_effect,
                                subscription_id: occupant_subscription,
                            })
                            .await
                        {
                            Ok(()) => {}
                            Err(error) if effect_retryable(&error) => {
                                attempts += 1;
                                if attempts >= MAX_APPLY_ATTEMPTS {
                                    return Err(error);
                                }
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    Err(error) if effect_retryable(&error) => {
                        attempts += 1;
                        if attempts >= MAX_APPLY_ATTEMPTS {
                            return Err(error);
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        let mut ranges: BTreeMap<(Uuid, crate::active_range::RangeId), ()> = BTreeMap::new();
        for receipt in &receipts {
            ranges.insert((receipt.feed_id, receipt.range_id), ());
        }
        for (feed_id, range_id) in ranges.into_keys() {
            let mut sweeps = 0;
            let mut attempts = 0;
            let mut transient = 0;
            loop {
                let outcome = match apply
                    .release_output(&EffectReleaseTarget {
                        feed_id,
                        range_id,
                        effect_id,
                        subscription_id: committed.subscription_id,
                    })
                    .await
                {
                    Ok(outcome) => outcome,
                    Err(error) if effect_retryable(&error) => {
                        transient += 1;
                        if transient >= MAX_APPLY_ATTEMPTS {
                            return Err(error);
                        }
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                match outcome {
                    EffectReleaseOutcome::Released { .. } => break,
                    EffectReleaseOutcome::NotPending { committed } => {
                        // The tail resolved already; every receipt for this
                        // Range must sit at or below the committed boundary,
                        // otherwise its frames were truncated and the outputs
                        // have to append again under a fresh marker.
                        let settled = receipts
                            .iter()
                            .filter(|receipt| {
                                receipt.feed_id == feed_id && receipt.range_id == range_id
                            })
                            .all(|receipt| receipt.position.value() <= committed);
                        if settled {
                            break;
                        }
                        attempts += 1;
                        if attempts > MAX_PENDING_SWEEPS {
                            return Err(EffectJournalError::Conflict);
                        }
                        for output in outputs {
                            apply.append_output(output, effect_id).await?;
                        }
                    }
                    EffectReleaseOutcome::Occupied { occupant } => {
                        sweeps += 1;
                        if sweeps > MAX_PENDING_SWEEPS {
                            return Err(EffectJournalError::Unavailable);
                        }
                        match apply.resolve_pending(occupant).await {
                            Ok(()) => {}
                            Err(error) if effect_retryable(&error) => {
                                transient += 1;
                                if transient >= MAX_APPLY_ATTEMPTS {
                                    return Err(error);
                                }
                            }
                            Err(error) => return Err(error),
                        }
                    }
                }
            }
        }
        // Co-located store mutations join the boundary last: each commits
        // through the store's own RF3 journal under its declared dedupe
        // identity, so a replayed apply converges to one row per mutation.
        for state_mutation in state_mutations {
            let mut attempts = 0;
            loop {
                match apply.mutate_state(state_mutation).await {
                    Ok(()) => break,
                    Err(error) if effect_retryable(&error) => {
                        attempts += 1;
                        if attempts >= MAX_APPLY_ATTEMPTS {
                            return Err(error);
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        // The consumed frontier commits only after every declared effect is
        // durable: outputs appended and released, co-located mutations
        // committed. An interrupted apply therefore leaves the frontier
        // unmoved, so the next drive refetches the same records, replans the
        // identical effect identity, and settles the residue instead of
        // skipping the input. Replays past this point are no-ops: the CAS
        // returns early once the frontier already rests at the target.
        if let Some(consume) = consume {
            let mut attempts = 0;
            loop {
                match apply
                    .commit_frontier(
                        consume,
                        effect_apply_request_id(committed.request_id, b"frontier"),
                    )
                    .await
                {
                    Ok(()) => break,
                    Err(error) if effect_retryable(&error) => {
                        attempts += 1;
                        if attempts >= MAX_APPLY_ATTEMPTS {
                            return Err(error);
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        let terminal = if matches!(committed.transition, EffectTransition::Declare { .. }) {
            let applied = EffectMutation {
                effect_id,
                subscription_id: committed.subscription_id,
                ownership_epoch: self.assignment.ownership_epoch,
                sequence: committed.sequence + 1,
                request_id: effect_apply_request_id(committed.request_id, b"applied"),
                transition: EffectTransition::Applied {
                    declares: committed.request_id,
                    outputs: outputs.to_vec(),
                    state_mutations: state_mutations.to_vec(),
                },
            };
            match self.apply(applied.clone()).await {
                Ok(committed) => committed,
                Err(EffectJournalError::AmbiguousCommit | EffectJournalError::NoQuorum) => {
                    self.reconcile_retry(applied).await?
                }
                Err(error) => return Err(error),
            }
        } else {
            committed.clone()
        };
        Ok(terminal)
    }

    /// Terminalize a committed `Declare` whose apply hit a permanent
    /// failure: journal `Aborted` under a deterministic request identity,
    /// then discard this effect's pending tails and pass the declared
    /// consume over the frontier as skipped. The terminal row is journaled
    /// first — restating the consume on it — so a crash before the discard
    /// or skip replays both from the `Aborted` arm instead of leaving the
    /// marker wedged or the abandoned input re-planned forever.
    async fn abort_declared<A: EffectApply + ?Sized>(
        &self,
        committed: &EffectMutation,
        outputs: &[EffectOutput],
        cause: EffectJournalError,
        apply: &A,
    ) -> Result<EffectMutation, EffectJournalError> {
        let consume = match &committed.transition {
            EffectTransition::Declare { consume, .. } => consume.clone(),
            _ => None,
        };
        let aborted = EffectMutation {
            effect_id: committed.effect_id,
            subscription_id: committed.subscription_id,
            ownership_epoch: self.assignment.ownership_epoch,
            sequence: committed.sequence + 1,
            request_id: effect_apply_request_id(committed.request_id, b"aborted"),
            transition: EffectTransition::Aborted {
                declares: committed.request_id,
                reason: abort_reason(&cause),
                outputs: outputs.to_vec(),
                consume: consume.clone(),
            },
        };
        let terminal = match self.apply(aborted.clone()).await {
            Ok(row) => row,
            Err(EffectJournalError::AmbiguousCommit | EffectJournalError::NoQuorum) => {
                self.reconcile_retry(aborted).await?
            }
            Err(error) => return Err(error),
        };
        self.discard_declared_outputs(
            outputs,
            committed.effect_id,
            committed.subscription_id,
            apply,
        )
        .await?;
        self.skip_abandoned_consume(consume.as_ref(), committed.request_id, apply)
            .await?;
        Ok(terminal)
    }

    /// Pass an abandoned consume's declared positions over the Subscription
    /// frontier as `skipped`: the dead effect's input is durably passed
    /// over — recorded, not re-planned — so a committed `Aborted` row
    /// settles instead of storming the identical `Declare` forever. A
    /// frontier no-op consume (emission- or expiry-only effect) moves
    /// nothing and is skipped outright. Non-retryable failures propagate:
    /// the `Aborted` row is already durable, so every later replay retries
    /// the same deterministic skip until it lands or fails closed.
    async fn skip_abandoned_consume<A: EffectApply + ?Sized>(
        &self,
        consume: Option<&EffectConsume>,
        declare_request_id: Uuid,
        apply: &A,
    ) -> Result<(), EffectJournalError> {
        let Some(consume) = consume else {
            return Ok(());
        };
        if consume.expected_cursor.as_deref() == Some(consume.cursor.as_str()) {
            return Ok(());
        }
        let mut attempts = 0;
        loop {
            match apply
                .commit_frontier_skipped(
                    consume,
                    effect_apply_request_id(declare_request_id, b"frontier-skip"),
                )
                .await
            {
                Ok(()) => return Ok(()),
                // The CAS refusal means the frontier moved under a different
                // decision since this consume was declared: the input's
                // disposition now belongs to that decision, so the dead
                // effect's claim on it is void rather than an error.
                Err(EffectJournalError::Conflict) => return Ok(()),
                Err(error) if effect_retryable(&error) => {
                    attempts += 1;
                    if attempts >= MAX_APPLY_ATTEMPTS {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Discard this effect's pending tail on every declared output's
    /// Range. Marker verification inside the Range store guarantees only
    /// tails owned by this effect are truncated; a foreign occupant is
    /// swept through its own journal, bounded, before failing closed.
    async fn discard_declared_outputs<A: EffectApply + ?Sized>(
        &self,
        outputs: &[EffectOutput],
        effect_id: Uuid,
        subscription_id: Uuid,
        apply: &A,
    ) -> Result<(), EffectJournalError> {
        for output in outputs {
            let mut sweeps = 0;
            let mut attempts = 0;
            loop {
                let outcome = match apply
                    .discard_output(output, effect_id, subscription_id)
                    .await
                {
                    Ok(outcome) => outcome,
                    Err(error) if effect_retryable(&error) => {
                        attempts += 1;
                        if attempts >= MAX_APPLY_ATTEMPTS {
                            return Err(error);
                        }
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                match outcome {
                    EffectDiscardOutcome::Discarded { .. } | EffectDiscardOutcome::NotPending => {
                        break
                    }
                    EffectDiscardOutcome::Occupied { occupant } => {
                        sweeps += 1;
                        if sweeps > MAX_PENDING_SWEEPS {
                            return Err(EffectJournalError::Unavailable);
                        }
                        match apply.resolve_pending(occupant).await {
                            Ok(()) => {}
                            Err(error) if effect_retryable(&error) => {
                                attempts += 1;
                                if attempts >= MAX_APPLY_ATTEMPTS {
                                    return Err(error);
                                }
                            }
                            Err(error) => return Err(error),
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// A bounded, non-empty abort reason for the terminal journal row.
fn abort_reason(cause: &EffectJournalError) -> String {
    let mut reason = cause.to_string();
    while reason.len() > 256 {
        reason.pop();
    }
    if reason.is_empty() {
        "permanent apply failure".to_owned()
    } else {
        reason
    }
}

/// Where one appended output landed; the release phase commits each touched
/// Range's pending tail once, not per record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectOutputReceipt {
    pub feed_id: Uuid,
    pub range_id: crate::active_range::RangeId,
    pub position: crate::active_range::RangePosition,
}

/// The Range whose pending tail a release targets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectReleaseTarget {
    pub feed_id: Uuid,
    pub range_id: crate::active_range::RangeId,
    pub effect_id: Uuid,
    pub subscription_id: Uuid,
}

/// The outcome of one pending-tail release on an output Range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EffectReleaseOutcome {
    /// The effect's marker committed; its outputs are now visible.
    Released { committed: u64 },
    /// No marker holds the tail; `committed` is the Range's current boundary
    /// so the caller can tell released-apart from truncated-and-lost.
    NotPending { committed: u64 },
    /// A different effect holds the tail; its journal decides the outcome.
    Occupied { occupant: EffectOccupant },
}

/// The outcome of discarding an effect's pending tail on one Range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EffectDiscardOutcome {
    /// The marker's uncommitted tail was truncated and cleared.
    Discarded { dropped_frames: u64 },
    /// No tail holds the Range — already resolved or never appended.
    NotPending,
    /// A different effect holds the tail; its journal decides the outcome.
    Occupied { occupant: EffectOccupant },
}

/// An effect whose pending tail occupies an output Range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EffectOccupant {
    pub effect_id: Uuid,
    pub subscription_id: Uuid,
}

/// Executes the side effects of a committed `Declare`: idempotent pending
/// output appends, the consumed frontier move, and the release that makes
/// outputs visible once the terminal journal marker is durable.
#[async_trait::async_trait]
pub trait EffectApply: Send + Sync {
    /// Idempotently append one declared output under the effect's pending
    /// fence. A retry of the same writer identity and sequence must dedupe
    /// rather than duplicate; the record must not become visible before the
    /// release phase commits it.
    async fn append_output(
        &self,
        output: &EffectOutput,
        effect_id: Uuid,
    ) -> Result<EffectOutputReceipt, EffectJournalError>;

    /// Idempotently commit the declared frontier move on the consumed
    /// Subscription's replicated progress journal.
    async fn commit_frontier(
        &self,
        consume: &EffectConsume,
        request_id: Uuid,
    ) -> Result<(), EffectJournalError>;

    /// Idempotently commit the declared frontier move for an effect whose
    /// apply was durably abandoned: the consumed positions pass the
    /// frontier recorded as `skipped` — final-disposition evidence — so the
    /// dead effect's input never re-plans. Runs after the terminal
    /// `Aborted` row is durable and again on `Aborted` replay, so a crash
    /// between marker and frontier still converges. The default falls back
    /// to a plain frontier commit for sinks without skip evidence; either
    /// way the abandoned input must not return.
    async fn commit_frontier_skipped(
        &self,
        consume: &EffectConsume,
        request_id: Uuid,
    ) -> Result<(), EffectJournalError> {
        self.commit_frontier(consume, request_id).await
    }

    /// Release the pending tail one of this effect's outputs occupies on the
    /// target Range so the outputs become visible atomically with the
    /// effect's terminal journal marker.
    async fn release_output(
        &self,
        target: &EffectReleaseTarget,
    ) -> Result<EffectReleaseOutcome, EffectJournalError>;

    /// Commit one declared co-located StateStore mutation through the
    /// store's RF3 journal under the mutation's declared dedupe identity.
    /// Runs in the release phase, after the terminal journal marker is
    /// durable; an ambiguous result must settle by retrying the same
    /// `mutation_id`.
    async fn mutate_state(&self, mutation: &EffectStateMutation) -> Result<(), EffectJournalError>;

    /// Resolve another effect holding an output Range's tail: drive its own
    /// committed journal entry to release so this effect can proceed. The
    /// default refuses, which turns `PendingOccupied` into a retryable
    /// `Unavailable` for callers that cannot reach the occupant's journal.
    async fn resolve_pending(&self, _occupant: EffectOccupant) -> Result<(), EffectJournalError> {
        Err(EffectJournalError::Unavailable)
    }

    /// Discard this effect's pending tail on `output`'s Range: truncate the
    /// uncommitted frames and clear the marker when the occupant is this
    /// effect. Called after the terminal `Aborted` row is durable, and
    /// again on replay so a crash between terminal commit and tail cleanup
    /// still converges. The default refuses, which turns the discard into
    /// a retryable `Unavailable` for callers without Range routing.
    async fn discard_output(
        &self,
        _output: &EffectOutput,
        _effect_id: Uuid,
        _subscription_id: Uuid,
    ) -> Result<EffectDiscardOutcome, EffectJournalError> {
        Err(EffectJournalError::Unavailable)
    }
}

/// The deterministic store-journal dedupe identity of the `index`th
/// declared co-located mutation in `effect_id`'s decision. Producers stamp
/// it on `EffectStateMutation.mutation_id` at Declare time so every apply
/// replay commits the same row.
pub fn effect_state_mutation_id(effect_id: Uuid, index: usize) -> Uuid {
    effect_apply_request_id(effect_id, format!("state-mutation-{index}").as_bytes())
}

/// Errors that may reflect a transient or ambiguous quorum result rather
/// than a final refusal. Every step in `apply_committed` replays an
/// identical deterministic identity, so repeating the step is the defined
/// way to settle them; any other error fails the apply immediately.
pub fn effect_retryable(error: &EffectJournalError) -> bool {
    matches!(
        error,
        EffectJournalError::Unavailable
            | EffectJournalError::AmbiguousCommit
            | EffectJournalError::NoQuorum
            | EffectJournalError::PendingOccupied { .. }
    )
}

/// A deterministic request identity derived from the committed Declare so a
/// retried apply replays the identical journal mutation.
pub fn effect_apply_request_id(declare_request_id: Uuid, tag: &[u8]) -> Uuid {
    let mut material = declare_request_id.as_bytes().to_vec();
    material.extend_from_slice(tag);
    let digest = blake3::hash(&material);
    Uuid::from_bytes(digest.as_bytes()[..16].try_into().expect("16-byte prefix"))
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
            ownership_epoch: assignment.ownership_epoch,
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
            if mutation.ownership_epoch > request.ownership_epoch {
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
            let code = refusal_code(&body);
            tracing::warn!(
                node = %node,
                path,
                %status,
                code = code.as_deref().unwrap_or(""),
                "effect journal replica refused"
            );
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

/// Decodes the wire `code` a journal replica embedded in an error body.
/// Only the leading 4 KiB is inspected — coded refusals are small, and a
/// body beyond the head falls back to status mapping. Slicing the head
/// (not `body.get(..4096)`, which is `None` for shorter bodies) keeps
/// coded refusals from collapsing into a bare `Conflict`.
fn refusal_code(body: &[u8]) -> Option<String> {
    let head = &body[..body.len().min(4096)];
    serde_json::from_slice::<serde_json::Value>(head)
        .ok()
        .and_then(|value| {
            value
                .get("code")
                .and_then(|code| code.as_str())
                .map(str::to_owned)
        })
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
            metadata_base64: BTreeMap::new(),
            event_time_ns: 1,
            writer_session_id: Uuid::new_v4(),
            writer_epoch: 1,
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
                state_mutations: Vec::new(),
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
                outputs: vec![],
                state_mutations: Vec::new(),
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
                    outputs: vec![],
                    state_mutations: Vec::new(),
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
                outputs: vec![],
                state_mutations: Vec::new(),
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
        // The fence is directional: the placement check upstream proves the
        // authoritative epoch, so the identical decision restamped under an
        // adopted epoch re-votes instead of wedging.
        let mut forwarded = mutation.clone();
        forwarded.ownership_epoch = 2;
        assert_eq!(
            replica.prepare(forwarded).unwrap().digest,
            replica.prepare(mutation.clone()).unwrap().digest
        );
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
                outputs: vec![],
                state_mutations: Vec::new(),
            },
            ..mutation.clone()
        };
        assert!(matches!(
            replica.prepare(wrong),
            Err(EffectJournalError::Conflict)
        ));
        // Once adoption ratchets the row fence forward, a mutation stamped
        // behind it is refused as a stale write.
        replica
            .adopt_recovered(
                mutation.effect_id,
                mutation.subscription_id,
                3,
                Some(mutation.clone()),
            )
            .unwrap();
        assert!(matches!(
            replica.prepare(mutation),
            Err(EffectJournalError::StaleEpoch)
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
            state_mutations: Vec::new(),
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
            state_mutations: Vec::new(),
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

    struct LoopbackTransport {
        stores: std::collections::BTreeMap<StorageNodeId, FjallEffectJournalReplica>,
        down: std::sync::Mutex<std::collections::BTreeSet<StorageNodeId>>,
    }

    impl LoopbackTransport {
        fn new(
            stores: std::collections::BTreeMap<StorageNodeId, FjallEffectJournalReplica>,
        ) -> Self {
            Self {
                stores,
                down: std::sync::Mutex::new(std::collections::BTreeSet::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl EffectJournalTransport for LoopbackTransport {
        async fn prepare(
            &self,
            node: &StorageNodeId,
            mutation: EffectMutation,
        ) -> Result<EffectReplicaReply<EffectPrepareVote>, EffectJournalError> {
            if self.down.lock().unwrap().contains(node) {
                return Err(EffectJournalError::Unavailable);
            }
            let store = self
                .stores
                .get(node)
                .ok_or(EffectJournalError::InvalidAssignment)?;
            let effect_id = mutation.effect_id;
            let ownership_epoch = mutation.ownership_epoch;
            Ok(EffectReplicaReply {
                replica: node.clone(),
                effect_id,
                ownership_epoch,
                result: store.prepare(mutation)?,
            })
        }

        async fn commit(
            &self,
            node: &StorageNodeId,
            evidence: EffectCommitEvidence,
        ) -> Result<EffectReplicaReply<EffectMutation>, EffectJournalError> {
            if self.down.lock().unwrap().contains(node) {
                return Err(EffectJournalError::Unavailable);
            }
            let store = self
                .stores
                .get(node)
                .ok_or(EffectJournalError::InvalidAssignment)?;
            let result = store.commit_with_quorum(evidence)?;
            Ok(EffectReplicaReply {
                replica: node.clone(),
                effect_id: result.effect_id,
                ownership_epoch: store.local_state(result.effect_id)?.ownership_epoch,
                result,
            })
        }

        async fn committed(
            &self,
            node: &StorageNodeId,
            effect_id: Uuid,
            ownership_epoch: u64,
        ) -> Result<EffectReplicaReply<Option<EffectMutation>>, EffectJournalError> {
            let store = self
                .stores
                .get(node)
                .ok_or(EffectJournalError::InvalidAssignment)?;
            Ok(EffectReplicaReply {
                replica: node.clone(),
                effect_id,
                ownership_epoch,
                result: store.local_committed(effect_id)?,
            })
        }

        async fn inspect(
            &self,
            node: &StorageNodeId,
            effect_id: Uuid,
            ownership_epoch: u64,
        ) -> Result<EffectReplicaReply<EffectJournalInspection>, EffectJournalError> {
            let store = self
                .stores
                .get(node)
                .ok_or(EffectJournalError::InvalidAssignment)?;
            Ok(EffectReplicaReply {
                replica: node.clone(),
                effect_id,
                ownership_epoch,
                result: store.local_state(effect_id)?,
            })
        }

        async fn adopt(
            &self,
            node: &StorageNodeId,
            effect_id: Uuid,
            ownership_epoch: u64,
            committed: Option<EffectMutation>,
        ) -> Result<EffectReplicaReply<Option<EffectMutation>>, EffectJournalError> {
            let store = self
                .stores
                .get(node)
                .ok_or(EffectJournalError::InvalidAssignment)?;
            Ok(EffectReplicaReply {
                replica: node.clone(),
                effect_id,
                ownership_epoch,
                result: store.adopt_recovered(
                    effect_id,
                    Uuid::from_u128(0),
                    ownership_epoch,
                    committed,
                )?,
            })
        }
    }

    #[derive(Default)]
    struct RecordingApply {
        appended: std::sync::Mutex<Vec<EffectOutput>>,
        frontier_requests: std::sync::Mutex<Vec<Uuid>>,
        released: std::sync::Mutex<Vec<(Uuid, crate::active_range::RangeId)>>,
        fail_outputs: std::sync::Mutex<std::collections::BTreeSet<Uuid>>,
        occupied: std::sync::Mutex<Option<EffectOccupant>>,
        resolved: std::sync::Mutex<Vec<EffectOccupant>>,
        not_pending: std::sync::Mutex<Option<u64>>,
        fail_mutations: std::sync::Mutex<usize>,
        events: std::sync::Mutex<Vec<&'static str>>,
    }

    #[async_trait::async_trait]
    impl EffectApply for RecordingApply {
        async fn append_output(
            &self,
            output: &EffectOutput,
            _effect_id: Uuid,
        ) -> Result<EffectOutputReceipt, EffectJournalError> {
            if self
                .fail_outputs
                .lock()
                .unwrap()
                .contains(&output.writer_session_id)
            {
                return Err(EffectJournalError::Unavailable);
            }
            if let Some(occupant) = *self.occupied.lock().unwrap() {
                return Err(EffectJournalError::PendingOccupied {
                    effect_id: occupant.effect_id,
                    subscription_id: occupant.subscription_id,
                });
            }
            self.events.lock().unwrap().push("append");
            let mut appended = self.appended.lock().unwrap();
            if !appended
                .iter()
                .any(|seen| seen.writer_session_id == output.writer_session_id)
            {
                appended.push(output.clone());
            }
            Ok(EffectOutputReceipt {
                feed_id: output.feed_id,
                range_id: crate::active_range::RangeId::from_uuid(Uuid::from_u128(7)),
                position: crate::active_range::RangePosition::new(1),
            })
        }

        async fn commit_frontier(
            &self,
            _consume: &EffectConsume,
            request_id: Uuid,
        ) -> Result<(), EffectJournalError> {
            self.events.lock().unwrap().push("frontier");
            self.frontier_requests.lock().unwrap().push(request_id);
            Ok(())
        }

        async fn release_output(
            &self,
            target: &EffectReleaseTarget,
        ) -> Result<EffectReleaseOutcome, EffectJournalError> {
            self.events.lock().unwrap().push("release");
            if let Some(committed) = *self.not_pending.lock().unwrap() {
                return Ok(EffectReleaseOutcome::NotPending { committed });
            }
            self.released
                .lock()
                .unwrap()
                .push((target.feed_id, target.range_id));
            Ok(EffectReleaseOutcome::Released { committed: 1 })
        }

        async fn mutate_state(
            &self,
            _mutation: &EffectStateMutation,
        ) -> Result<(), EffectJournalError> {
            self.events.lock().unwrap().push("mutate");
            let mut failures = self.fail_mutations.lock().unwrap();
            if *failures > 0 {
                *failures -= 1;
                return Err(EffectJournalError::Unavailable);
            }
            Ok(())
        }

        async fn resolve_pending(
            &self,
            occupant: EffectOccupant,
        ) -> Result<(), EffectJournalError> {
            self.resolved.lock().unwrap().push(occupant);
            *self.occupied.lock().unwrap() = None;
            Ok(())
        }
    }

    fn apply_fixture() -> (
        Vec<TempDir>,
        [StorageNodeId; 3],
        std::sync::Arc<LoopbackTransport>,
        EffectCoordinator,
    ) {
        let directories = vec![
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
        ];
        let nodes = [
            StorageNodeId::try_new("apply-a").unwrap(),
            StorageNodeId::try_new("apply-b").unwrap(),
            StorageNodeId::try_new("apply-c").unwrap(),
        ];
        let stores = nodes
            .iter()
            .cloned()
            .zip(
                directories
                    .iter()
                    .map(|directory| FjallEffectJournalReplica::open(directory.path()).unwrap()),
            )
            .collect();
        let transport = std::sync::Arc::new(LoopbackTransport::new(stores));
        let coordinator = EffectCoordinator::new(
            crate::reader::SubscriptionProgressAssignment::try_new(
                Uuid::from_u128(0),
                nodes[0].clone(),
                crate::active_range::ReplicaSet::try_new(nodes.clone()).unwrap(),
                1,
            )
            .unwrap(),
            transport.clone(),
        );
        (directories, nodes, transport, coordinator)
    }

    fn scoped_declare(effect_id: Uuid) -> EffectMutation {
        let mut mutation = declare(1);
        mutation.effect_id = effect_id;
        mutation.subscription_id = Uuid::from_u128(0);
        mutation
    }

    #[tokio::test]
    async fn apply_committed_runs_outputs_frontier_then_marks_applied() {
        let (_directories, _nodes, _transport, coordinator) = apply_fixture();
        let mutation = scoped_declare(Uuid::new_v4());
        coordinator.apply(mutation.clone()).await.unwrap();
        let sink = RecordingApply::default();
        let applied = coordinator
            .apply_committed(mutation.effect_id, &sink)
            .await
            .unwrap();
        let EffectTransition::Applied {
            declares, outputs, ..
        } = &applied.transition
        else {
            panic!("expected applied")
        };
        assert_eq!(*declares, mutation.request_id);
        assert_eq!(outputs.len(), 1);
        assert_eq!(applied.sequence, 2);
        assert_eq!(
            applied.request_id,
            effect_apply_request_id(mutation.request_id, b"applied")
        );
        let appended = sink.appended.lock().unwrap();
        assert_eq!(appended.len(), 1);
        assert_eq!(appended[0].writer_epoch, 1);
        drop(appended);
        assert_eq!(
            sink.frontier_requests.lock().unwrap().as_slice(),
            &[effect_apply_request_id(mutation.request_id, b"frontier")]
        );
    }

    #[tokio::test]
    async fn apply_committed_replays_after_a_mid_apply_failure() {
        let (_directories, _nodes, _transport, coordinator) = apply_fixture();
        let mutation = scoped_declare(Uuid::new_v4());
        coordinator.apply(mutation.clone()).await.unwrap();
        let sink = RecordingApply::default();
        let EffectTransition::Declare { outputs, .. } = &mutation.transition else {
            panic!("expected declare")
        };
        sink.fail_outputs
            .lock()
            .unwrap()
            .insert(outputs[0].writer_session_id);
        assert!(matches!(
            coordinator.apply_committed(mutation.effect_id, &sink).await,
            Err(EffectJournalError::Unavailable)
        ));
        assert_eq!(sink.appended.lock().unwrap().len(), 0);
        assert!(sink.frontier_requests.lock().unwrap().is_empty());

        sink.fail_outputs.lock().unwrap().clear();
        let applied = coordinator
            .apply_committed(mutation.effect_id, &sink)
            .await
            .unwrap();
        assert!(matches!(
            applied.transition,
            EffectTransition::Applied { .. }
        ));
        assert_eq!(sink.appended.lock().unwrap().len(), 1);
        assert_eq!(sink.frontier_requests.lock().unwrap().len(), 1);

        // A further call replays the sink work deterministically and sees the
        // already-terminal row, so no additional journal transition commits.
        let replayed = coordinator
            .apply_committed(mutation.effect_id, &sink)
            .await
            .unwrap();
        assert_eq!(replayed, applied);
        assert_eq!(sink.appended.lock().unwrap().len(), 1);
        assert_eq!(sink.frontier_requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn apply_committed_commits_the_frontier_after_state_mutations() {
        let (_directories, _nodes, _transport, coordinator) = apply_fixture();
        let mut mutation = scoped_declare(Uuid::new_v4());
        let EffectTransition::Declare {
            state_mutations, ..
        } = &mut mutation.transition
        else {
            panic!("expected declare")
        };
        state_mutations.push(EffectStateMutation {
            store_id: Uuid::from_u128(5),
            mutation_id: Uuid::from_u128(6),
            op: crate::state_store::StateStoreMutationOp::DeleteRow { key: b"k".to_vec() },
        });
        coordinator.apply(mutation.clone()).await.unwrap();
        let sink = RecordingApply::default();
        // Exhaust the bounded in-place retries so the apply fails while the
        // declared mutation is still unsettled.
        *sink.fail_mutations.lock().unwrap() = MAX_APPLY_ATTEMPTS;
        assert!(matches!(
            coordinator.apply_committed(mutation.effect_id, &sink).await,
            Err(EffectJournalError::Unavailable)
        ));
        // The frontier must not have committed: leaving it unmoved lets the
        // next drive refetch the same records and replay this exact effect
        // instead of silently skipping the consumed input.
        assert!(sink.frontier_requests.lock().unwrap().is_empty());
        let applied = coordinator
            .apply_committed(mutation.effect_id, &sink)
            .await
            .unwrap();
        assert!(matches!(
            applied.transition,
            EffectTransition::Applied { .. }
        ));
        let events = sink.events.lock().unwrap().clone();
        let last_mutate = events.iter().rposition(|event| *event == "mutate");
        let first_frontier = events.iter().position(|event| *event == "frontier");
        assert!(
            last_mutate.is_some() && first_frontier.is_some() && last_mutate < first_frontier,
            "every mutation must settle before the frontier commits: {events:?}"
        );
    }

    #[tokio::test]
    async fn apply_committed_survives_a_member_replica_loss() {
        let (_directories, nodes, transport, coordinator) = apply_fixture();
        let mutation = scoped_declare(Uuid::new_v4());
        coordinator.apply(mutation.clone()).await.unwrap();
        transport.down.lock().unwrap().insert(nodes[2].clone());
        let sink = RecordingApply::default();
        let applied = coordinator
            .apply_committed(mutation.effect_id, &sink)
            .await
            .unwrap();
        assert!(matches!(
            applied.transition,
            EffectTransition::Applied { .. }
        ));
    }

    #[tokio::test]
    async fn apply_committed_converges_across_an_ownership_epoch_move() {
        // A Declare committed under epoch 1 keeps its stamp; after the
        // assignment moves to epoch 2, the recovered coordinator replans the
        // identical page into the identical effect identity and must drive it
        // terminal - not wedge on the old epoch stamp.
        let (_directories, nodes, transport, coordinator) = apply_fixture();
        let mutation = scoped_declare(Uuid::new_v4());
        let committed = coordinator.apply(mutation.clone()).await.unwrap();
        assert!(matches!(
            committed.transition,
            EffectTransition::Declare { .. }
        ));

        for node in &nodes {
            transport
                .adopt(node, mutation.effect_id, 2, Some(committed.clone()))
                .await
                .unwrap();
        }
        let recovered = EffectCoordinator::new(
            crate::reader::SubscriptionProgressAssignment::try_new(
                Uuid::from_u128(0),
                nodes[0].clone(),
                crate::active_range::ReplicaSet::try_new(nodes.clone()).unwrap(),
                2,
            )
            .unwrap(),
            transport.clone(),
        );

        // The identical decision replanned under the new epoch is the same
        // request: the journal re-verifies the committed row instead of
        // conflicting on the epoch stamp.
        let mut replanned = mutation.clone();
        replanned.ownership_epoch = 2;
        recovered.apply(replanned).await.unwrap();
        let observed = recovered
            .read_committed(mutation.effect_id)
            .await
            .unwrap()
            .expect("committed declare survives the epoch move");
        assert!(decisions_equal(&observed, &committed));

        let sink = RecordingApply::default();
        let applied = recovered
            .apply_committed(mutation.effect_id, &sink)
            .await
            .unwrap();
        assert!(matches!(
            applied.transition,
            EffectTransition::Applied { .. }
        ));
        assert_eq!(sink.appended.lock().unwrap().len(), 1);
        assert_eq!(sink.frontier_requests.lock().unwrap().len(), 1);
        let expected_mutations = match &committed.transition {
            EffectTransition::Declare {
                state_mutations, ..
            } => state_mutations.len(),
            _ => 0,
        };
        assert_eq!(
            sink.events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| **event == "mutate")
                .count(),
            expected_mutations
        );
    }

    #[tokio::test]
    async fn apply_committed_rejects_a_row_from_another_scope() {
        let (_directories, _nodes, transport, coordinator) = apply_fixture();
        let mut foreign = declare(1);
        foreign.subscription_id = Uuid::from_u128(9);
        foreign.effect_id = Uuid::new_v4();
        let other = EffectCoordinator::new(
            crate::reader::SubscriptionProgressAssignment::try_new(
                foreign.subscription_id,
                StorageNodeId::try_new("apply-a").unwrap(),
                crate::active_range::ReplicaSet::try_new(
                    ["apply-a", "apply-b", "apply-c"]
                        .map(|name| StorageNodeId::try_new(name).unwrap()),
                )
                .unwrap(),
                1,
            )
            .unwrap(),
            transport.clone(),
        );
        // Commit through the coordinator scoped to the foreign Subscription;
        // then confirm the original coordinator refuses to apply it.
        other.apply(foreign.clone()).await.unwrap();
        let sink = RecordingApply::default();
        assert!(matches!(
            coordinator.apply_committed(foreign.effect_id, &sink).await,
            Err(EffectJournalError::InvalidAssignment)
        ));
    }

    #[test]
    fn refusal_code_decodes_wire_codes_regardless_of_body_size() {
        // Regression: `body.get(..4096)` returns `None` for a body shorter
        // than 4 KiB, collapsing coded refusals (placement fences, stale
        // epochs) into a bare `Conflict` that aborts effect application.
        let coded = br#"{"code":"invalid_assignment","error":"placement moved"}"#;
        assert!(matches!(
            refusal_code(coded)
                .as_deref()
                .and_then(EffectJournalError::from_code),
            Some(EffectJournalError::InvalidAssignment)
        ));
        assert_eq!(refusal_code(b"plain refusal"), None);
        assert_eq!(refusal_code(b"{}"), None);
        let mut oversized = br#"{"code":"conflict","pad":""#.to_vec();
        oversized.extend(std::iter::repeat_n(b' ', 5000));
        oversized.extend_from_slice(b"}");
        assert_eq!(refusal_code(&oversized), None);
    }
}
