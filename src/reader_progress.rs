//! ReaderId-sharded RF3 progress journal — the data-plane counterpart of
//! the catalog's Reader definitions. Mirrors the Subscription progress
//! machinery in `reader.rs`: every declared Reader carries a
//! `reader_progress_assignments` placement (owner + RF3 replicas +
//! ownership epoch) committed through the Control Plane, while the
//! delivered/acknowledged frontier and bounded fetch-replay evidence live
//! in a per-Reader journal row on the assigned replicas, mutated through
//! the same prepare → quorum-digest commit → committed-read protocol.
//!
//! Reader rows are deliberately simpler than Subscription rows: a named
//! Reader is a single epoch-fenced session rather than a shared member
//! set, so the journal carries no leases, member maps, or quarantine
//! evidence — just the frontier, the session epoch, and the last fetch's
//! replay receipt. The catalog keeps Reader definitions, shard placement,
//! and staged cutover translations; it never stores the frontier itself.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use fjall::Readable;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::active_range::RangeId;

/// Upper bound on per-Reader position maps, matching the catalog
/// frontier's range bound.
const READER_PROGRESS_MAX_RANGES: usize = 128;
/// Upper bound on one serialized journal row.
const READER_PROGRESS_MAX_ROW_BYTES: usize = 256 * 1024;

#[derive(Debug, Error)]
pub enum ReaderProgressJournalError {
    #[error(transparent)]
    Engine(#[from] fjall::Error),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
    #[error("Reader progress has a conflicting identity, request, or Cursor")]
    Conflict,
    #[error("Reader progress ownership changed; reconcile before retrying")]
    StaleEpoch,
    #[error("Reader progress sequence has a gap or overflow")]
    Sequence,
    #[error("Reader progress exceeds its bounded storage budget")]
    TooLarge,
    #[error("Reader progress has no valid two-replica commit evidence")]
    NoQuorum,
    #[error("Reader progress replica placement or owner is invalid")]
    InvalidAssignment,
    #[error("Reader progress replica is unavailable; retry the same request identity")]
    Unavailable,
    #[error("Reader progress commit result is ambiguous; retry the same request identity")]
    AmbiguousCommit,
}

impl ReaderProgressJournalError {
    /// Wire-stable identifier carried by internal replica responses, as in
    /// `SubscriptionProgressError::code`.
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Engine(_) => "engine",
            Self::Serialization(_) => "serialization",
            Self::Conflict => "conflict",
            Self::StaleEpoch => "stale_epoch",
            Self::Sequence => "sequence",
            Self::TooLarge => "too_large",
            Self::NoQuorum => "no_quorum",
            Self::InvalidAssignment => "invalid_assignment",
            Self::Unavailable => "unavailable",
            Self::AmbiguousCommit => "ambiguous_commit",
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

/// One committed journal row: the Reader's durable delivered/acknowledged
/// frontier plus the bounded replay receipt for its most recent fetch —
/// the same fields the catalog `ReaderFrontier` carried, fenced by shard
/// sequence and request identity instead of a Raft apply index.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReaderProgressMutation {
    pub reader_id: Uuid,
    /// The Reader's bound Feed — the row's scope identity, pinned at
    /// adopt/first mutation and verified on every replica write.
    pub feed_id: Uuid,
    pub ownership_epoch: u64,
    /// Monotonic per-row decision sequence: `committed.sequence + 1`.
    pub sequence: u64,
    /// Idempotent request identity: a replayed mutation with the same
    /// request identity and identical content replays its vote; identical
    /// identity with different content conflicts.
    pub request_id: Uuid,
    /// Compare-and-set guard on the committed frontier's `cursor`; `None`
    /// only when no row is committed yet.
    pub expected_cursor: Option<String>,
    /// The delivered-Cursor token this mutation installs.
    pub cursor: String,
    /// The reader session epoch this mutation was issued under; replicas
    /// refuse a mutation stamped behind the committed session.
    pub session_epoch: u64,
    pub acknowledged: BTreeMap<RangeId, String>,
    pub delivered: BTreeMap<RangeId, String>,
    /// The per-range positions the last delivered page started from —
    /// persisted so a duplicate `last_fetch_request_id` can re-read and
    /// prove an identical page rather than blindly refusing.
    #[serde(default)]
    pub previous: BTreeMap<RangeId, String>,
    /// The record bound the last delivered page used; replay is only
    /// provable under the identical bound.
    #[serde(default)]
    pub last_fetch_limit: usize,
    /// The backoff advice the last delivered page was paced under; replay
    /// reports the identical value on any ingress Node.
    #[serde(default)]
    pub last_fetch_retry_after_ms: u64,
    #[serde(default)]
    pub last_fetch_request_id: Option<Uuid>,
}

impl ReaderProgressMutation {
    /// Decision identity for quorum digesting: everything except the
    /// stamped ownership epoch, which is assignment history rather than
    /// decision content (same exclusion as `progress_decision_digest`).
    fn digest(&self) -> Result<[u8; 32], ReaderProgressJournalError> {
        let canonical = ReaderProgressMutation {
            ownership_epoch: 0,
            ..self.clone()
        };
        let bytes = serde_json::to_vec(&canonical)?;
        Ok(*blake3::hash(&bytes).as_bytes())
    }

    fn same_identity(&self, other: &ReaderProgressMutation) -> bool {
        self.reader_id == other.reader_id
            && self.feed_id == other.feed_id
            && self.sequence == other.sequence
            && self.request_id == other.request_id
            && self.expected_cursor == other.expected_cursor
            && self.cursor == other.cursor
            && self.session_epoch == other.session_epoch
            && self.acknowledged == other.acknowledged
            && self.delivered == other.delivered
            && self.previous == other.previous
            && self.last_fetch_limit == other.last_fetch_limit
            && self.last_fetch_retry_after_ms == other.last_fetch_retry_after_ms
            && self.last_fetch_request_id == other.last_fetch_request_id
    }

    /// Bounds shared by prepare and adopt so neither path admits a row the
    /// other would refuse.
    fn bounds_ok(&self) -> bool {
        let positions_ok = |map: &BTreeMap<RangeId, String>| {
            map.len() <= READER_PROGRESS_MAX_RANGES
                && map
                    .values()
                    .all(|value| !value.is_empty() && value.len() <= 256)
        };
        self.ownership_epoch != 0
            && self.sequence != 0
            && self.session_epoch != 0
            && !self.cursor.is_empty()
            && self.cursor.len() <= 256
            && positions_ok(&self.acknowledged)
            && positions_ok(&self.delivered)
            && positions_ok(&self.previous)
            && self.expected_cursor.as_ref().is_none_or(|expected| {
                !expected.is_empty() && expected.len() <= 256 && expected != &self.cursor
            })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReaderProgressAssignment {
    pub reader_id: Uuid,
    pub owner: crate::active_range::StorageNodeId,
    pub replicas: crate::active_range::ReplicaSet,
    pub ownership_epoch: u64,
}

impl ReaderProgressAssignment {
    pub fn try_new(
        reader_id: Uuid,
        owner: crate::active_range::StorageNodeId,
        replicas: crate::active_range::ReplicaSet,
        ownership_epoch: u64,
    ) -> Result<Self, ReaderProgressJournalError> {
        if ownership_epoch == 0 || !replicas.contains(&owner) {
            return Err(ReaderProgressJournalError::InvalidAssignment);
        }
        Ok(Self {
            reader_id,
            owner,
            replicas,
            ownership_epoch,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReaderReplicaReply<T> {
    pub replica: crate::active_range::StorageNodeId,
    pub reader_id: Uuid,
    pub ownership_epoch: u64,
    pub result: T,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReaderPrepareVote {
    pub reader_id: Uuid,
    pub request_id: Uuid,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReaderCommitEvidence {
    votes: [(crate::active_range::StorageNodeId, [u8; 32]); 2],
    reader_id: Uuid,
    request_id: Uuid,
}

impl ReaderCommitEvidence {
    pub fn new(
        reader_id: Uuid,
        request_id: Uuid,
        votes: [(crate::active_range::StorageNodeId, [u8; 32]); 2],
    ) -> Result<Self, ReaderProgressJournalError> {
        if votes[0].0 == votes[1].0 {
            return Err(ReaderProgressJournalError::NoQuorum);
        }
        Ok(Self {
            votes,
            reader_id,
            request_id,
        })
    }

    pub fn request_id(&self) -> Uuid {
        self.request_id
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReaderPrepareRequest {
    pub owner: crate::active_range::StorageNodeId,
    pub receiver: crate::active_range::StorageNodeId,
    pub mutation: ReaderProgressMutation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReaderCommitRequest {
    pub owner: crate::active_range::StorageNodeId,
    pub receiver: crate::active_range::StorageNodeId,
    pub reader_id: Uuid,
    pub ownership_epoch: u64,
    pub evidence: ReaderCommitEvidence,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReaderCommittedReadRequest {
    pub owner: crate::active_range::StorageNodeId,
    pub receiver: crate::active_range::StorageNodeId,
    pub reader_id: Uuid,
    pub ownership_epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReaderProgressInspection {
    pub committed: Option<ReaderProgressMutation>,
    pub prepared: Option<ReaderProgressMutation>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReaderProgressAdoptRequest {
    pub owner: crate::active_range::StorageNodeId,
    pub receiver: crate::active_range::StorageNodeId,
    pub reader_id: Uuid,
    pub ownership_epoch: u64,
    pub committed: Option<ReaderProgressMutation>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ReaderProgressReplicaRow {
    reader_id: Uuid,
    feed_id: Uuid,
    ownership_epoch: u64,
    committed: Option<ReaderProgressMutation>,
    prepared: Option<ReaderProgressMutation>,
}

/// The replica-side journal store: one Fjall row per ReaderId holding the
/// committed frontier decision plus any prepared vote awaiting commit
/// evidence. Mirrors `FjallSubscriptionProgressReplica` — every write is
/// a durable `SyncAll` transaction, and fencing is directional (a row
/// stamped by a newer placement fences stale mutations; a row behind the
/// request's placement ratchets forward).
pub struct FjallReaderProgressReplica {
    db: fjall::SingleWriterTxDatabase,
    progress: fjall::SingleWriterTxKeyspace,
}

impl FjallReaderProgressReplica {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ReaderProgressJournalError> {
        let db = fjall::SingleWriterTxDatabase::builder(path).open()?;
        let progress = db.keyspace(
            "reader_progress_shard",
            fjall::KeyspaceCreateOptions::default,
        )?;
        Ok(Self { db, progress })
    }

    fn read_row(
        &self,
        reader_id: Uuid,
    ) -> Result<Option<ReaderProgressReplicaRow>, ReaderProgressJournalError> {
        let row = self
            .db
            .read_tx()
            .get(&self.progress, reader_id.as_bytes())?
            .map(|bytes| serde_json::from_slice::<ReaderProgressReplicaRow>(&bytes))
            .transpose()?;
        match row {
            Some(row)
                if row.reader_id != reader_id
                    || row.committed.as_ref().is_some_and(|committed| {
                        committed.reader_id != reader_id
                            || committed.feed_id != row.feed_id
                            || committed.ownership_epoch > row.ownership_epoch
                    })
                    || row.prepared.as_ref().is_some_and(|prepared| {
                        prepared.reader_id != reader_id
                            || prepared.feed_id != row.feed_id
                            || prepared.ownership_epoch > row.ownership_epoch
                    }) =>
            {
                Err(ReaderProgressJournalError::Conflict)
            }
            Some(row) => Ok(Some(row)),
            None => Ok(None),
        }
    }

    pub fn local_committed(
        &self,
        reader_id: Uuid,
    ) -> Result<Option<ReaderProgressMutation>, ReaderProgressJournalError> {
        Ok(self.read_row(reader_id)?.and_then(|row| row.committed))
    }

    pub fn local_state(
        &self,
        reader_id: Uuid,
    ) -> Result<ReaderProgressInspection, ReaderProgressJournalError> {
        Ok(self
            .read_row(reader_id)?
            .map(|row| ReaderProgressInspection {
                committed: row.committed,
                prepared: row.prepared,
            })
            .unwrap_or(ReaderProgressInspection {
                committed: None,
                prepared: None,
            }))
    }

    /// Install a quorum-recovered row under a new ownership epoch — the
    /// adoption half of progress-shard moves, mirroring
    /// `FjallSubscriptionProgressReplica::adopt_recovered`.
    pub fn adopt_recovered(
        &self,
        reader_id: Uuid,
        feed_id: Uuid,
        ownership_epoch: u64,
        committed: Option<ReaderProgressMutation>,
    ) -> Result<Option<ReaderProgressMutation>, ReaderProgressJournalError> {
        if ownership_epoch == 0 {
            return Err(ReaderProgressJournalError::StaleEpoch);
        }
        if let Some(mutation) = &committed {
            if mutation.reader_id != reader_id
                || mutation.feed_id != feed_id
                || mutation.ownership_epoch > ownership_epoch
            {
                return Err(ReaderProgressJournalError::Conflict);
            }
            if !mutation.bounds_ok() {
                return Err(ReaderProgressJournalError::TooLarge);
            }
        }
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let previous = tx
            .get(&self.progress, reader_id.as_bytes())?
            .map(|bytes| serde_json::from_slice::<ReaderProgressReplicaRow>(&bytes))
            .transpose()?;
        let mut row = previous.unwrap_or(ReaderProgressReplicaRow {
            reader_id,
            feed_id,
            ownership_epoch: 0,
            committed: None,
            prepared: None,
        });
        if row.reader_id != reader_id || row.feed_id != feed_id {
            return Err(ReaderProgressJournalError::Conflict);
        }
        if row.ownership_epoch > ownership_epoch {
            return Err(ReaderProgressJournalError::StaleEpoch);
        }
        if row.ownership_epoch == ownership_epoch {
            let same_committed = match (&row.committed, &committed) {
                (None, None) => true,
                (Some(existing), Some(candidate)) => existing.same_identity(candidate),
                _ => false,
            };
            return if same_committed {
                Ok(row.committed)
            } else {
                Err(ReaderProgressJournalError::Conflict)
            };
        }
        if let Some(existing) = &row.committed {
            let Some(recovered) = &committed else {
                return Err(ReaderProgressJournalError::Conflict);
            };
            if recovered.sequence < existing.sequence
                || (recovered.sequence == existing.sequence && !existing.same_identity(recovered))
                || (recovered.sequence == existing.sequence + 1
                    && recovered.expected_cursor.as_deref() != Some(existing.cursor.as_str()))
            {
                return Err(ReaderProgressJournalError::Conflict);
            }
        }
        row.ownership_epoch = ownership_epoch;
        row.committed = committed;
        row.prepared = None;
        let encoded = serde_json::to_vec(&row)?;
        if encoded.len() > READER_PROGRESS_MAX_ROW_BYTES {
            return Err(ReaderProgressJournalError::TooLarge);
        }
        tx.insert(&self.progress, row.reader_id.as_bytes(), encoded);
        tx.commit()?;
        Ok(row.committed)
    }

    pub fn prepare(
        &self,
        mutation: ReaderProgressMutation,
    ) -> Result<ReaderPrepareVote, ReaderProgressJournalError> {
        if !mutation.bounds_ok() {
            return Err(ReaderProgressJournalError::TooLarge);
        }
        if serde_json::to_vec(&mutation)?.len() > READER_PROGRESS_MAX_ROW_BYTES {
            return Err(ReaderProgressJournalError::TooLarge);
        }
        let vote = ReaderPrepareVote {
            reader_id: mutation.reader_id,
            request_id: mutation.request_id,
            digest: mutation.digest()?,
        };
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let previous = tx
            .get(&self.progress, mutation.reader_id.as_bytes())?
            .map(|bytes| serde_json::from_slice::<ReaderProgressReplicaRow>(&bytes))
            .transpose()?;
        let mut row = match previous {
            Some(row) => row,
            None => ReaderProgressReplicaRow {
                reader_id: mutation.reader_id,
                feed_id: mutation.feed_id,
                ownership_epoch: mutation.ownership_epoch,
                committed: None,
                prepared: None,
            },
        };
        if row.reader_id != mutation.reader_id || row.feed_id != mutation.feed_id {
            return Err(ReaderProgressJournalError::Conflict);
        }
        // Directional fencing, as in the subscription replica: a mutation
        // stamped under an older epoch than the row dies as a stale write,
        // while a row behind the placement-checked request ratchets
        // forward on install.
        if row.ownership_epoch > mutation.ownership_epoch {
            return Err(ReaderProgressJournalError::StaleEpoch);
        }
        if let Some(committed) = &row.committed {
            if committed.request_id == mutation.request_id {
                return if committed.same_identity(&mutation) {
                    Ok(vote)
                } else {
                    Err(ReaderProgressJournalError::Conflict)
                };
            }
        }
        if let Some(prepared) = &row.prepared {
            if prepared.same_identity(&mutation) {
                return Ok(vote);
            }
            if prepared.request_id == mutation.request_id {
                return Err(ReaderProgressJournalError::Conflict);
            }
            // A residue from an abandoned vote: this replica is behind the
            // quorum's committed trail and answers as a lagging witness.
            return Err(ReaderProgressJournalError::Sequence);
        }
        let prior_sequence = row.committed.as_ref().map_or(0, |prior| prior.sequence);
        if prior_sequence.checked_add(1) != Some(mutation.sequence) {
            return Err(ReaderProgressJournalError::Sequence);
        }
        if row.committed.as_ref().map(|prior| prior.cursor.as_str())
            != mutation.expected_cursor.as_deref()
        {
            return Err(ReaderProgressJournalError::Conflict);
        }
        if row
            .committed
            .as_ref()
            .is_some_and(|prior| mutation.session_epoch < prior.session_epoch)
        {
            // The stamped session may not regress: a reopened session
            // bumps the epoch and a fenced old session must never write.
            return Err(ReaderProgressJournalError::StaleEpoch);
        }
        row.ownership_epoch = mutation.ownership_epoch;
        row.prepared = Some(mutation);
        let encoded = serde_json::to_vec(&row)?;
        if encoded.len() > READER_PROGRESS_MAX_ROW_BYTES {
            return Err(ReaderProgressJournalError::TooLarge);
        }
        tx.insert(&self.progress, row.reader_id.as_bytes(), encoded);
        tx.commit()?;
        Ok(vote)
    }

    pub fn commit_with_quorum(
        &self,
        evidence: ReaderCommitEvidence,
    ) -> Result<ReaderProgressMutation, ReaderProgressJournalError> {
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let bytes = tx
            .get(&self.progress, evidence.reader_id.as_bytes())?
            .ok_or(ReaderProgressJournalError::Conflict)?;
        let mut row: ReaderProgressReplicaRow = serde_json::from_slice(&bytes)?;
        if row.reader_id != evidence.reader_id {
            return Err(ReaderProgressJournalError::Conflict);
        }
        let candidate = row
            .prepared
            .as_ref()
            .filter(|value| value.request_id == evidence.request_id)
            .or_else(|| {
                row.committed
                    .as_ref()
                    .filter(|value| value.request_id == evidence.request_id)
            })
            .ok_or(ReaderProgressJournalError::Conflict)?;
        let digest = candidate.digest()?;
        if evidence.request_id != candidate.request_id
            || evidence.votes[0].0 == evidence.votes[1].0
            || evidence.votes.iter().any(|(_, value)| *value != digest)
        {
            return Err(ReaderProgressJournalError::NoQuorum);
        }
        if row
            .prepared
            .as_ref()
            .is_none_or(|value| value.request_id != evidence.request_id)
        {
            tracing::warn!(
                reader = %evidence.reader_id,
                evidence_request = %evidence.request_id,
                prepared_request = ?row.prepared.as_ref().map(|value| value.request_id),
                "Reader progress commit found no matching prepared vote"
            );
            return row.committed.ok_or(ReaderProgressJournalError::Conflict);
        }
        let committed = row
            .prepared
            .take()
            .ok_or(ReaderProgressJournalError::Conflict)?;
        row.committed = Some(committed.clone());
        tx.insert(
            &self.progress,
            row.reader_id.as_bytes(),
            serde_json::to_vec(&row)?,
        );
        tx.commit()?;
        Ok(committed)
    }
}

/// Placement-fenced internal endpoint handlers — the receiving half of
/// the progress shard, mirroring `SubscriptionProgressReplicaService`.
pub struct ReaderProgressReplicaService {
    local_node: crate::active_range::StorageNodeId,
    control: std::sync::Arc<crate::control::ControlController>,
    replica: std::sync::Arc<FjallReaderProgressReplica>,
}

impl ReaderProgressReplicaService {
    pub fn new(
        local_node: crate::active_range::StorageNodeId,
        control: std::sync::Arc<crate::control::ControlController>,
        replica: std::sync::Arc<FjallReaderProgressReplica>,
    ) -> Self {
        Self {
            local_node,
            control,
            replica,
        }
    }

    async fn check_placement(
        &self,
        reader_id: Uuid,
        owner: &crate::active_range::StorageNodeId,
        receiver: &crate::active_range::StorageNodeId,
        epoch: u64,
    ) -> Result<ReaderProgressAssignment, ReaderProgressJournalError> {
        let assignment = self
            .control
            .active_reader_progress_assignment_by_id(reader_id)
            .await
            .ok_or(ReaderProgressJournalError::InvalidAssignment)?;
        if assignment.owner != *owner
            || epoch != assignment.ownership_epoch
            || *receiver != self.local_node
            || !assignment.replicas.contains(receiver)
        {
            return Err(ReaderProgressJournalError::InvalidAssignment);
        }
        Ok(assignment)
    }

    async fn check_scope(
        &self,
        reader_id: Uuid,
        feed_id: Uuid,
    ) -> Result<(), ReaderProgressJournalError> {
        if self
            .control
            .active_reader_progress_scope_by_id(reader_id)
            .await
            != Some(feed_id)
        {
            return Err(ReaderProgressJournalError::Conflict);
        }
        Ok(())
    }

    pub async fn prepare(
        &self,
        request: ReaderPrepareRequest,
    ) -> Result<ReaderReplicaReply<ReaderPrepareVote>, ReaderProgressJournalError> {
        let assignment = self
            .check_placement(
                request.mutation.reader_id,
                &request.owner,
                &request.receiver,
                request.mutation.ownership_epoch,
            )
            .await?;
        self.check_scope(request.mutation.reader_id, request.mutation.feed_id)
            .await?;
        let replica = self.replica.clone();
        let result = tokio::task::spawn_blocking(move || replica.prepare(request.mutation))
            .await
            .map_err(|_| ReaderProgressJournalError::Unavailable)??;
        Ok(ReaderReplicaReply {
            replica: self.local_node.clone(),
            reader_id: assignment.reader_id,
            ownership_epoch: assignment.ownership_epoch,
            result,
        })
    }

    pub async fn commit(
        &self,
        request: ReaderCommitRequest,
    ) -> Result<ReaderReplicaReply<ReaderProgressMutation>, ReaderProgressJournalError> {
        let assignment = self
            .check_placement(
                request.reader_id,
                &request.owner,
                &request.receiver,
                request.ownership_epoch,
            )
            .await?;
        if request.evidence.reader_id != request.reader_id
            || request
                .evidence
                .votes
                .iter()
                .any(|(node, _)| !assignment.replicas.contains(node))
            || !request
                .evidence
                .votes
                .iter()
                .any(|(node, _)| node == &assignment.owner)
        {
            return Err(ReaderProgressJournalError::InvalidAssignment);
        }
        let replica = self.replica.clone();
        let result =
            tokio::task::spawn_blocking(move || replica.commit_with_quorum(request.evidence))
                .await
                .map_err(|_| ReaderProgressJournalError::Unavailable)??;
        Ok(ReaderReplicaReply {
            replica: self.local_node.clone(),
            reader_id: assignment.reader_id,
            ownership_epoch: assignment.ownership_epoch,
            result,
        })
    }

    pub async fn committed(
        &self,
        request: ReaderCommittedReadRequest,
    ) -> Result<ReaderReplicaReply<Option<ReaderProgressMutation>>, ReaderProgressJournalError>
    {
        let assignment = self
            .check_placement(
                request.reader_id,
                &request.owner,
                &request.receiver,
                request.ownership_epoch,
            )
            .await?;
        let feed_id = self
            .control
            .active_reader_progress_scope_by_id(request.reader_id)
            .await
            .ok_or(ReaderProgressJournalError::InvalidAssignment)?;
        let replica = self.replica.clone();
        let reader_id = request.reader_id;
        let committed = tokio::task::spawn_blocking(move || replica.local_committed(reader_id))
            .await
            .map_err(|_| ReaderProgressJournalError::Unavailable)??;
        if let Some(progress) = &committed {
            if progress.reader_id != reader_id || progress.feed_id != feed_id {
                return Err(ReaderProgressJournalError::Conflict);
            }
            if progress.ownership_epoch > request.ownership_epoch {
                return Err(ReaderProgressJournalError::StaleEpoch);
            }
        }
        Ok(ReaderReplicaReply {
            replica: self.local_node.clone(),
            reader_id: assignment.reader_id,
            ownership_epoch: assignment.ownership_epoch,
            result: committed,
        })
    }

    pub async fn inspect(
        &self,
        request: ReaderCommittedReadRequest,
    ) -> Result<ReaderReplicaReply<ReaderProgressInspection>, ReaderProgressJournalError> {
        let assignment = self
            .check_placement(
                request.reader_id,
                &request.owner,
                &request.receiver,
                request.ownership_epoch,
            )
            .await?;
        let replica = self.replica.clone();
        let reader_id = request.reader_id;
        let inspection = tokio::task::spawn_blocking(move || replica.local_state(reader_id))
            .await
            .map_err(|_| ReaderProgressJournalError::Unavailable)??;
        Ok(ReaderReplicaReply {
            replica: self.local_node.clone(),
            reader_id: assignment.reader_id,
            ownership_epoch: assignment.ownership_epoch,
            result: inspection,
        })
    }

    pub async fn adopt(
        &self,
        request: ReaderProgressAdoptRequest,
    ) -> Result<ReaderReplicaReply<Option<ReaderProgressMutation>>, ReaderProgressJournalError>
    {
        let assignment = self
            .check_placement(
                request.reader_id,
                &request.owner,
                &request.receiver,
                request.ownership_epoch,
            )
            .await?;
        let feed_id = self
            .control
            .active_reader_progress_scope_by_id(request.reader_id)
            .await
            .ok_or(ReaderProgressJournalError::InvalidAssignment)?;
        let replica = self.replica.clone();
        let reader_id = request.reader_id;
        let ownership_epoch = request.ownership_epoch;
        let committed = request.committed;
        let result = tokio::task::spawn_blocking(move || {
            replica.adopt_recovered(reader_id, feed_id, ownership_epoch, committed)
        })
        .await
        .map_err(|_| ReaderProgressJournalError::Unavailable)??;
        Ok(ReaderReplicaReply {
            replica: self.local_node.clone(),
            reader_id: assignment.reader_id,
            ownership_epoch: assignment.ownership_epoch,
            result,
        })
    }
}

#[async_trait::async_trait]
pub trait ReaderProgressTransport: Send + Sync {
    async fn prepare(
        &self,
        replica: &crate::active_range::StorageNodeId,
        mutation: ReaderProgressMutation,
    ) -> Result<ReaderReplicaReply<ReaderPrepareVote>, ReaderProgressJournalError>;

    async fn commit(
        &self,
        replica: &crate::active_range::StorageNodeId,
        evidence: ReaderCommitEvidence,
    ) -> Result<ReaderReplicaReply<ReaderProgressMutation>, ReaderProgressJournalError>;

    async fn committed(
        &self,
        replica: &crate::active_range::StorageNodeId,
        reader_id: Uuid,
        ownership_epoch: u64,
    ) -> Result<ReaderReplicaReply<Option<ReaderProgressMutation>>, ReaderProgressJournalError>;

    async fn inspect(
        &self,
        replica: &crate::active_range::StorageNodeId,
        reader_id: Uuid,
        ownership_epoch: u64,
    ) -> Result<ReaderReplicaReply<ReaderProgressInspection>, ReaderProgressJournalError>;

    async fn adopt(
        &self,
        replica: &crate::active_range::StorageNodeId,
        reader_id: Uuid,
        ownership_epoch: u64,
        committed: Option<ReaderProgressMutation>,
    ) -> Result<ReaderReplicaReply<Option<ReaderProgressMutation>>, ReaderProgressJournalError>;
}

fn lagging_witness(error: &ReaderProgressJournalError) -> bool {
    matches!(
        error,
        ReaderProgressJournalError::Unavailable
            | ReaderProgressJournalError::StaleEpoch
            | ReaderProgressJournalError::InvalidAssignment
            | ReaderProgressJournalError::Sequence
    )
}

fn lagging_prepare(error: &ReaderProgressJournalError) -> bool {
    lagging_witness(error)
}

fn refusal_error(status: reqwest::StatusCode, body: &[u8]) -> ReaderProgressJournalError {
    let head = &body[..body.len().min(4096)];
    let code = serde_json::from_slice::<serde_json::Value>(head)
        .ok()
        .and_then(|value| {
            value
                .get("code")
                .and_then(|code| code.as_str())
                .map(str::to_owned)
        });
    if let Some(error) = code
        .as_deref()
        .and_then(ReaderProgressJournalError::from_code)
    {
        return error;
    }
    if status == reqwest::StatusCode::CONFLICT {
        ReaderProgressJournalError::Conflict
    } else {
        ReaderProgressJournalError::Unavailable
    }
}

/// The coordinator driving one Reader's progress shard: reads the
/// committed frontier across the replica set, drives prepare →
/// quorum-digest commit for each mutation, and settles ambiguous
/// outcomes by re-reading. Mirrors `SubscriptionProgressCoordinator`.
pub struct ReaderProgressCoordinator {
    assignment: ReaderProgressAssignment,
    transport: std::sync::Arc<dyn ReaderProgressTransport>,
}

impl ReaderProgressCoordinator {
    pub async fn for_reader(
        control: &crate::control::ControlController,
        reader_id: Uuid,
        transport: std::sync::Arc<dyn ReaderProgressTransport>,
    ) -> Result<Self, ReaderProgressJournalError> {
        let assignment = control
            .active_reader_progress_assignment_by_id(reader_id)
            .await
            .ok_or(ReaderProgressJournalError::InvalidAssignment)?;
        Ok(Self::new(assignment, transport))
    }

    pub fn new(
        assignment: ReaderProgressAssignment,
        transport: std::sync::Arc<dyn ReaderProgressTransport>,
    ) -> Self {
        Self {
            assignment,
            transport,
        }
    }

    pub async fn read_committed(
        &self,
    ) -> Result<Option<ReaderProgressMutation>, ReaderProgressJournalError> {
        let mut observed: Option<ReaderProgressMutation> = None;
        let mut votes = 0;
        let mut lagged = false;
        let replies = futures_util::future::join_all(self.assignment.replicas.iter().map(|node| {
            self.transport.committed(
                node,
                self.assignment.reader_id,
                self.assignment.ownership_epoch,
            )
        }))
        .await;
        for (node, reply) in self.assignment.replicas.iter().zip(replies) {
            match reply {
                Ok(reply) => {
                    if reply.replica != *node
                        || reply.reader_id != self.assignment.reader_id
                        || reply.ownership_epoch != self.assignment.ownership_epoch
                    {
                        return Err(ReaderProgressJournalError::InvalidAssignment);
                    }
                    let progress = reply.result;
                    if let Some(mutation) = progress.as_ref() {
                        if mutation.reader_id != self.assignment.reader_id {
                            return Err(ReaderProgressJournalError::Conflict);
                        }
                        if mutation.ownership_epoch > self.assignment.ownership_epoch {
                            return Err(ReaderProgressJournalError::StaleEpoch);
                        }
                    }
                    votes += 1;
                    if let Some(candidate) = progress {
                        match &observed {
                            Some(current) if !current.same_identity(&candidate) => {
                                match candidate.sequence.cmp(&current.sequence) {
                                    std::cmp::Ordering::Greater => observed = Some(candidate),
                                    std::cmp::Ordering::Equal => {
                                        return Err(ReaderProgressJournalError::Conflict)
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
            return Err(ReaderProgressJournalError::NoQuorum);
        }
        Ok(observed)
    }

    /// Reconcile a mutation whose commit outcome was ambiguous: read the
    /// quorum's committed trail, accept an identical committed row, or
    /// re-apply when the decision never landed. Mirrors
    /// `SubscriptionProgressCoordinator::reconcile_retry`.
    pub async fn reconcile_retry(
        &self,
        mutation: ReaderProgressMutation,
    ) -> Result<ReaderProgressMutation, ReaderProgressJournalError> {
        if mutation.reader_id != self.assignment.reader_id
            || mutation.ownership_epoch != self.assignment.ownership_epoch
        {
            return Err(ReaderProgressJournalError::InvalidAssignment);
        }
        let mut reachable = 0;
        let mut owner_seen = false;
        let mut owner_committed = false;
        let mut matching = 0;
        let replies = futures_util::future::join_all(self.assignment.replicas.iter().map(|node| {
            self.transport
                .committed(node, mutation.reader_id, mutation.ownership_epoch)
        }))
        .await;
        for (node, reply) in self.assignment.replicas.iter().zip(replies) {
            match reply {
                Ok(reply) => {
                    if reply.replica != *node
                        || reply.reader_id != mutation.reader_id
                        || reply.ownership_epoch != mutation.ownership_epoch
                    {
                        return Err(ReaderProgressJournalError::InvalidAssignment);
                    }
                    reachable += 1;
                    if node == &self.assignment.owner {
                        owner_seen = true;
                    }
                    match reply.result {
                        Some(current) if current.same_identity(&mutation) => {
                            matching += 1;
                            if node == &self.assignment.owner {
                                owner_committed = true;
                            }
                        }
                        Some(current) if current.sequence < mutation.sequence => {}
                        None => {}
                        _ => return Err(ReaderProgressJournalError::Conflict),
                    }
                }
                Err(error) if lagging_witness(&error) => {}
                Err(error) => return Err(error),
            }
        }
        if reachable < 2 || !owner_seen {
            return Err(ReaderProgressJournalError::NoQuorum);
        }
        if owner_committed && matching >= 2 {
            match self.read_committed().await {
                Ok(Some(committed)) if committed.same_identity(&mutation) => return Ok(mutation),
                Err(ReaderProgressJournalError::Conflict) => {}
                Err(error) => return Err(error),
                _ => return Err(ReaderProgressJournalError::AmbiguousCommit),
            }
        }
        self.apply(mutation.clone()).await?;
        match self.read_committed().await {
            Ok(Some(committed)) if committed.same_identity(&mutation) => Ok(mutation),
            Err(ReaderProgressJournalError::Conflict) | Ok(_) => {
                Err(ReaderProgressJournalError::AmbiguousCommit)
            }
            Err(error) => Err(error),
        }
    }

    /// Drive one mutation through prepare → quorum-digest commit, exactly
    /// as `SubscriptionProgressCoordinator::apply`: the owner prepares
    /// first so a stale assignment surfaces its fencing signal, then two
    /// matching digests (owner plus one) become the commit evidence.
    pub async fn apply(
        &self,
        mutation: ReaderProgressMutation,
    ) -> Result<Vec<crate::active_range::StorageNodeId>, ReaderProgressJournalError> {
        if mutation.reader_id != self.assignment.reader_id
            || mutation.ownership_epoch != self.assignment.ownership_epoch
        {
            return Err(ReaderProgressJournalError::InvalidAssignment);
        }
        let digest = mutation.digest()?;
        let owner = &self.assignment.owner;
        let owner_reply = match self.transport.prepare(owner, mutation.clone()).await {
            Ok(reply) => reply,
            Err(ReaderProgressJournalError::Unavailable) => {
                return Err(match self.read_committed().await {
                    Err(error) => error,
                    Ok(_) => ReaderProgressJournalError::NoQuorum,
                });
            }
            Err(error) => return Err(error),
        };
        if owner_reply.replica != *owner
            || owner_reply.reader_id != self.assignment.reader_id
            || owner_reply.ownership_epoch != self.assignment.ownership_epoch
        {
            return Err(ReaderProgressJournalError::InvalidAssignment);
        }
        if owner_reply.result.reader_id != mutation.reader_id
            || owner_reply.result.request_id != mutation.request_id
            || owner_reply.result.digest != digest
        {
            tracing::warn!(
                reader = %mutation.reader_id,
                vote_request = %owner_reply.result.request_id,
                mutation_request = %mutation.request_id,
                "Reader progress owner prepare vote mismatched"
            );
            return Err(ReaderProgressJournalError::Conflict);
        }
        let mut prepared = vec![(owner.clone(), digest)];
        let mut first_refusal: Option<ReaderProgressJournalError> = None;
        let followers: Vec<_> = self
            .assignment
            .replicas
            .iter()
            .filter(|node| *node != owner)
            .collect();
        let replies = futures_util::future::join_all(
            followers
                .iter()
                .map(|node| self.transport.prepare(node, mutation.clone())),
        )
        .await;
        for (node, reply) in followers.into_iter().zip(replies) {
            match reply {
                Ok(reply) => {
                    if reply.replica != *node
                        || reply.reader_id != self.assignment.reader_id
                        || reply.ownership_epoch != self.assignment.ownership_epoch
                    {
                        return Err(ReaderProgressJournalError::InvalidAssignment);
                    }
                    let vote = reply.result;
                    if vote.reader_id != mutation.reader_id
                        || vote.request_id != mutation.request_id
                        || vote.digest != digest
                    {
                        tracing::warn!(
                            %node,
                            reader = %mutation.reader_id,
                            vote_request = %vote.request_id,
                            mutation_request = %mutation.request_id,
                            "Reader progress follower prepare vote mismatched"
                        );
                        return Err(ReaderProgressJournalError::Conflict);
                    }
                    prepared.push((node.clone(), vote.digest));
                }
                Err(error) if lagging_prepare(&error) => {
                    if !matches!(error, ReaderProgressJournalError::Unavailable)
                        && first_refusal.is_none()
                    {
                        first_refusal = Some(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        if prepared.len() < 2
            || !prepared
                .iter()
                .any(|(node, _)| node == &self.assignment.owner)
        {
            return Err(first_refusal.unwrap_or(ReaderProgressJournalError::NoQuorum));
        }
        let other = prepared
            .iter()
            .find(|(node, _)| node != &self.assignment.owner)
            .ok_or(ReaderProgressJournalError::NoQuorum)?;
        let evidence = ReaderCommitEvidence {
            reader_id: mutation.reader_id,
            request_id: mutation.request_id,
            votes: [
                (self.assignment.owner.clone(), digest),
                (other.0.clone(), other.1),
            ],
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
                        || reply.reader_id != self.assignment.reader_id
                        || reply.ownership_epoch != self.assignment.ownership_epoch =>
                {
                    return Err(ReaderProgressJournalError::InvalidAssignment);
                }
                Ok(reply) if reply.result.same_identity(&mutation) => committed.push(node.clone()),
                Ok(reply) => {
                    tracing::warn!(
                        %node,
                        reader = %mutation.reader_id,
                        committed_seq = reply.result.sequence,
                        committed_request = %reply.result.request_id,
                        mutation_seq = mutation.sequence,
                        mutation_request = %mutation.request_id,
                        "Reader progress commit reply mismatched mutation"
                    );
                    return Err(ReaderProgressJournalError::Conflict);
                }
                Err(error) if lagging_witness(&error) => {}
                Err(error) => return Err(error),
            }
        }
        if committed.len() < 2 || !committed.contains(&self.assignment.owner) {
            return Err(ReaderProgressJournalError::AmbiguousCommit);
        }
        Ok(committed)
    }

    /// Replica-side prepared/committed evidence for recovery and stranded
    /// votes — mirrors `inspect_evidence`.
    pub(crate) async fn inspect_evidence(
        &self,
    ) -> Result<
        Vec<(crate::active_range::StorageNodeId, ReaderProgressInspection)>,
        ReaderProgressJournalError,
    > {
        let mut evidence = Vec::with_capacity(3);
        let replies = futures_util::future::join_all(self.assignment.replicas.iter().map(|node| {
            self.transport.inspect(
                node,
                self.assignment.reader_id,
                self.assignment.ownership_epoch,
            )
        }))
        .await;
        for (node, reply) in self.assignment.replicas.iter().zip(replies) {
            match reply {
                Ok(reply) => {
                    if reply.replica != *node
                        || reply.reader_id != self.assignment.reader_id
                        || reply.ownership_epoch != self.assignment.ownership_epoch
                    {
                        return Err(ReaderProgressJournalError::InvalidAssignment);
                    }
                    evidence.push((node.clone(), reply.result));
                }
                Err(error) if lagging_witness(&error) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(evidence)
    }

    /// Settle a stranded two-phase vote: when a quorum of replicas holds
    /// the identical prepared row extending the committed tip, that
    /// decision was already chosen — finish its commit rather than let
    /// the residue wedge the chain. Mirrors
    /// `SubscriptionProgressCoordinator::settle_stranded_prepare`.
    pub async fn settle_stranded_prepare(
        &self,
    ) -> Result<Option<ReaderProgressMutation>, ReaderProgressJournalError> {
        let evidence = self.inspect_evidence().await?;
        let mut residue: Option<(ReaderProgressMutation, usize)> = None;
        let mut divergent = false;
        for (_, inspection) in &evidence {
            if let Some(prepared) = &inspection.prepared {
                match &residue {
                    Some((existing, count)) if existing.same_identity(prepared) => {
                        residue = Some((existing.clone(), count + 1));
                    }
                    Some(_) => divergent = true,
                    None => residue = Some((prepared.clone(), 1)),
                }
            }
        }
        if divergent {
            return Ok(None);
        }
        let Some((prepared, holders)) = residue else {
            return Ok(None);
        };
        if holders < 2 {
            return Ok(None);
        }
        // The stranded vote must extend the committed tip: a quorum
        // committed read proves the chosen chain before we finish it.
        match self.read_committed().await? {
            Some(committed) if committed.sequence + 1 == prepared.sequence => {}
            None if prepared.sequence == 1 => {}
            _ => return Ok(None),
        }
        let owner_digest = prepared.digest()?;
        let evidence = ReaderCommitEvidence {
            reader_id: prepared.reader_id,
            request_id: prepared.request_id,
            votes: [
                (self.assignment.owner.clone(), owner_digest),
                (
                    self.assignment
                        .replicas
                        .iter()
                        .find(|node| *node != &self.assignment.owner)
                        .cloned()
                        .ok_or(ReaderProgressJournalError::NoQuorum)?,
                    owner_digest,
                ),
            ],
        };
        let mut committed = Vec::with_capacity(3);
        let replies = futures_util::future::join_all(
            self.assignment
                .replicas
                .iter()
                .map(|node| self.transport.commit(node, evidence.clone())),
        )
        .await;
        for (node, reply) in self.assignment.replicas.iter().zip(replies) {
            match reply {
                Ok(reply)
                    if reply.result.same_identity(&prepared)
                        && reply.replica == *node
                        && reply.reader_id == self.assignment.reader_id =>
                {
                    committed.push(node.clone());
                }
                Ok(_) => return Err(ReaderProgressJournalError::Conflict),
                Err(error) if lagging_witness(&error) => {}
                Err(error) => return Err(error),
            }
        }
        if committed.len() < 2 {
            return Err(ReaderProgressJournalError::AmbiguousCommit);
        }
        Ok(Some(prepared))
    }
}

/// HTTP client for the internal reader-progress routes — mirrors
/// `HttpSubscriptionProgressTransport`, including the mTLS variant that
/// pins per-Node certificate endpoints.
pub struct HttpReaderProgressTransport {
    assignment: ReaderProgressAssignment,
    endpoints: crate::internal_plane::InternalEndpoints,
    key: Option<String>,
    client: reqwest::Client,
}

impl HttpReaderProgressTransport {
    pub fn new(
        assignment: ReaderProgressAssignment,
        endpoints: impl Into<crate::internal_plane::InternalEndpoints>,
        key: String,
        timeout: Duration,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            assignment,
            endpoints: endpoints.into(),
            key: Some(key),
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(1))
                .timeout(timeout)
                .build()?,
        })
    }

    pub fn new_mtls(
        assignment: ReaderProgressAssignment,
        endpoints: impl Into<crate::internal_plane::InternalEndpoints>,
        ca_pem: &[u8],
        identity_pem: &[u8],
        timeout: Duration,
    ) -> Result<Self, ReaderProgressJournalError> {
        let endpoints = endpoints.into();
        for (node, endpoint) in endpoints.configured() {
            let url = reqwest::Url::parse(endpoint)
                .map_err(|_| ReaderProgressJournalError::InvalidAssignment)?;
            if url.scheme() != "https" || url.host_str() != Some(node.as_str()) {
                return Err(ReaderProgressJournalError::InvalidAssignment);
            }
        }
        let ca = reqwest::Certificate::from_pem(ca_pem)
            .map_err(|_| ReaderProgressJournalError::InvalidAssignment)?;
        let identity = reqwest::Identity::from_pem(identity_pem)
            .map_err(|_| ReaderProgressJournalError::InvalidAssignment)?;
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(1))
            .timeout(timeout)
            .https_only(true)
            .tls_built_in_root_certs(false)
            .add_root_certificate(ca)
            .identity(identity)
            .build()
            .map_err(|_| ReaderProgressJournalError::InvalidAssignment)?;
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
    ) -> Result<T, ReaderProgressJournalError> {
        if !self.assignment.replicas.contains(node) {
            return Err(ReaderProgressJournalError::InvalidAssignment);
        }
        let endpoint = self
            .endpoints
            .resolve(node)
            .await
            .ok_or(ReaderProgressJournalError::Unavailable)?;
        let mut request = self
            .client
            .post(format!("{}{path}", endpoint.trim_end_matches('/')))
            .json(body);
        if let Some(key) = self.key.as_deref() {
            request = request.header("x-whitewater-control-key", key);
        }
        let mut response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(node = %node, path, %error, "Reader progress request failed");
                return Err(ReaderProgressJournalError::Unavailable);
            }
        };
        if !response.status().is_success() {
            let status = response.status();
            let body = response.bytes().await.unwrap_or_default();
            let error = refusal_error(status, &body);
            tracing::warn!(node = %node, path, %status, code = error.code(), "Reader progress replica refused");
            return Err(error);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| ReaderProgressJournalError::Unavailable)?
        {
            if bytes
                .len()
                .checked_add(chunk.len())
                .is_none_or(|size| size > 512 * 1024)
            {
                return Err(ReaderProgressJournalError::TooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| ReaderProgressJournalError::Unavailable)
    }
}

#[async_trait::async_trait]
impl ReaderProgressTransport for HttpReaderProgressTransport {
    async fn prepare(
        &self,
        replica: &crate::active_range::StorageNodeId,
        mutation: ReaderProgressMutation,
    ) -> Result<ReaderReplicaReply<ReaderPrepareVote>, ReaderProgressJournalError> {
        if mutation.reader_id != self.assignment.reader_id
            || mutation.ownership_epoch != self.assignment.ownership_epoch
        {
            return Err(ReaderProgressJournalError::InvalidAssignment);
        }
        self.send(
            replica,
            "/internal/reader-progress/prepare",
            &ReaderPrepareRequest {
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
        evidence: ReaderCommitEvidence,
    ) -> Result<ReaderReplicaReply<ReaderProgressMutation>, ReaderProgressJournalError> {
        if evidence.reader_id != self.assignment.reader_id {
            return Err(ReaderProgressJournalError::InvalidAssignment);
        }
        self.send(
            replica,
            "/internal/reader-progress/commit",
            &ReaderCommitRequest {
                owner: self.assignment.owner.clone(),
                receiver: replica.clone(),
                reader_id: evidence.reader_id,
                ownership_epoch: self.assignment.ownership_epoch,
                evidence,
            },
        )
        .await
    }

    async fn committed(
        &self,
        replica: &crate::active_range::StorageNodeId,
        reader_id: Uuid,
        ownership_epoch: u64,
    ) -> Result<ReaderReplicaReply<Option<ReaderProgressMutation>>, ReaderProgressJournalError>
    {
        if reader_id != self.assignment.reader_id
            || ownership_epoch != self.assignment.ownership_epoch
        {
            return Err(ReaderProgressJournalError::InvalidAssignment);
        }
        self.send(
            replica,
            "/internal/reader-progress/committed",
            &ReaderCommittedReadRequest {
                owner: self.assignment.owner.clone(),
                receiver: replica.clone(),
                reader_id,
                ownership_epoch,
            },
        )
        .await
    }

    async fn inspect(
        &self,
        replica: &crate::active_range::StorageNodeId,
        reader_id: Uuid,
        ownership_epoch: u64,
    ) -> Result<ReaderReplicaReply<ReaderProgressInspection>, ReaderProgressJournalError> {
        if reader_id != self.assignment.reader_id
            || ownership_epoch != self.assignment.ownership_epoch
        {
            return Err(ReaderProgressJournalError::InvalidAssignment);
        }
        self.send(
            replica,
            "/internal/reader-progress/inspect",
            &ReaderCommittedReadRequest {
                owner: self.assignment.owner.clone(),
                receiver: replica.clone(),
                reader_id,
                ownership_epoch,
            },
        )
        .await
    }

    async fn adopt(
        &self,
        replica: &crate::active_range::StorageNodeId,
        reader_id: Uuid,
        ownership_epoch: u64,
        committed: Option<ReaderProgressMutation>,
    ) -> Result<ReaderReplicaReply<Option<ReaderProgressMutation>>, ReaderProgressJournalError>
    {
        if reader_id != self.assignment.reader_id
            || ownership_epoch != self.assignment.ownership_epoch
        {
            return Err(ReaderProgressJournalError::InvalidAssignment);
        }
        self.send(
            replica,
            "/internal/reader-progress/adopt",
            &ReaderProgressAdoptRequest {
                owner: self.assignment.owner.clone(),
                receiver: replica.clone(),
                reader_id,
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

    fn node(name: &str) -> crate::active_range::StorageNodeId {
        crate::active_range::StorageNodeId::try_new(name).unwrap()
    }

    fn mutation(
        reader_id: Uuid,
        feed_id: Uuid,
        sequence: u64,
        request: u128,
    ) -> ReaderProgressMutation {
        ReaderProgressMutation {
            reader_id,
            feed_id,
            ownership_epoch: 1,
            sequence,
            request_id: Uuid::from_u128(request),
            expected_cursor: if sequence == 1 {
                None
            } else {
                Some(format!("cursor-{}", sequence - 1))
            },
            cursor: format!("cursor-{sequence}"),
            session_epoch: 7,
            acknowledged: BTreeMap::new(),
            delivered: BTreeMap::from([(
                RangeId::from_uuid(Uuid::from_u128(9)),
                format!("pos-{sequence}"),
            )]),
            previous: BTreeMap::new(),
            last_fetch_limit: 100,
            last_fetch_retry_after_ms: 0,
            last_fetch_request_id: None,
        }
    }

    #[test]
    fn replica_prepare_commit_and_replay() {
        let directory = tempfile::tempdir().unwrap();
        let replica = FjallReaderProgressReplica::open(directory.path()).unwrap();
        let reader_id = Uuid::from_u128(1);
        let feed_id = Uuid::from_u128(2);
        let mutation = mutation(reader_id, feed_id, 1, 100);
        let vote = replica.prepare(mutation.clone()).unwrap();
        assert_eq!(vote.digest, mutation.digest().unwrap());
        // Identical replay votes again without duplicating state.
        let replay = replica.prepare(mutation.clone()).unwrap();
        assert_eq!(replay.digest, vote.digest);
        // A same-request different-content prepare conflicts.
        let mut forged = mutation.clone();
        forged.cursor = "forged".to_owned();
        assert!(matches!(
            replica.prepare(forged).unwrap_err(),
            ReaderProgressJournalError::Conflict
        ));
        // Commit requires quorum evidence matching the prepared digest.
        let evidence = ReaderCommitEvidence::new(
            reader_id,
            mutation.request_id,
            [
                (node("control-1"), vote.digest),
                (node("control-2"), vote.digest),
            ],
        )
        .unwrap();
        let committed = replica.commit_with_quorum(evidence.clone()).unwrap();
        assert!(committed.same_identity(&mutation));
        // The same evidence replays the committed row.
        let replayed = replica.commit_with_quorum(evidence).unwrap();
        assert!(replayed.same_identity(&mutation));
        assert_eq!(
            replica
                .local_committed(reader_id)
                .unwrap()
                .unwrap()
                .sequence,
            1
        );
    }

    #[test]
    fn replica_sequence_and_cas_fence() {
        let directory = tempfile::tempdir().unwrap();
        let replica = FjallReaderProgressReplica::open(directory.path()).unwrap();
        let reader_id = Uuid::from_u128(1);
        let feed_id = Uuid::from_u128(2);
        let first = mutation(reader_id, feed_id, 1, 100);
        let vote = replica.prepare(first.clone()).unwrap();
        let evidence = ReaderCommitEvidence::new(
            reader_id,
            first.request_id,
            [
                (node("control-1"), vote.digest),
                (node("control-2"), vote.digest),
            ],
        )
        .unwrap();
        replica.commit_with_quorum(evidence).unwrap();
        // A sequence gap is a lagging-witness signal, not a conflict.
        let gapped = mutation(reader_id, feed_id, 3, 300);
        assert!(matches!(
            replica.prepare(gapped).unwrap_err(),
            ReaderProgressJournalError::Sequence
        ));
        // A next-sequence mutation with the wrong CAS guard conflicts.
        let mut wrong_cas = mutation(reader_id, feed_id, 2, 200);
        wrong_cas.expected_cursor = Some("not-the-committed-cursor".to_owned());
        assert!(matches!(
            replica.prepare(wrong_cas).unwrap_err(),
            ReaderProgressJournalError::Conflict
        ));
        // The correct CAS vote lands.
        let second = mutation(reader_id, feed_id, 2, 200);
        replica.prepare(second).unwrap();
    }

    #[test]
    fn replica_epoch_session_and_adopt_fencing() {
        let directory = tempfile::tempdir().unwrap();
        let replica = FjallReaderProgressReplica::open(directory.path()).unwrap();
        let reader_id = Uuid::from_u128(1);
        let feed_id = Uuid::from_u128(2);
        let newer = mutation(reader_id, feed_id, 1, 100);
        let vote = replica.prepare(newer.clone()).unwrap();
        let evidence = ReaderCommitEvidence::new(
            reader_id,
            newer.request_id,
            [
                (node("control-1"), vote.digest),
                (node("control-2"), vote.digest),
            ],
        )
        .unwrap();
        replica.commit_with_quorum(evidence).unwrap();
        // Session epoch may not regress across mutations.
        let mut regressed = mutation(reader_id, feed_id, 2, 200);
        regressed.session_epoch = 3;
        assert!(matches!(
            replica.prepare(regressed).unwrap_err(),
            ReaderProgressJournalError::StaleEpoch
        ));
        // Adopt under a newer epoch installs recovered state.
        let adopted = replica
            .adopt_recovered(reader_id, feed_id, 2, Some(newer.clone()))
            .unwrap();
        assert!(adopted.unwrap().same_identity(&newer));
        // Same-epoch adoption with different content conflicts.
        let mut drift = newer.clone();
        drift.cursor = "drift".to_owned();
        assert!(matches!(
            replica
                .adopt_recovered(reader_id, feed_id, 2, Some(drift))
                .unwrap_err(),
            ReaderProgressJournalError::Conflict
        ));
    }

    struct ScriptedReplica {
        replica: FjallReaderProgressReplica,
        node: crate::active_range::StorageNodeId,
    }

    struct Fanout {
        nodes: Vec<(crate::active_range::StorageNodeId, ScriptedReplica)>,
    }

    #[async_trait::async_trait]
    impl ReaderProgressTransport for Fanout {
        async fn prepare(
            &self,
            replica: &crate::active_range::StorageNodeId,
            mutation: ReaderProgressMutation,
        ) -> Result<ReaderReplicaReply<ReaderPrepareVote>, ReaderProgressJournalError> {
            let (_, scripted) = self
                .nodes
                .iter()
                .find(|(node, _)| node == replica)
                .ok_or(ReaderProgressJournalError::InvalidAssignment)?;
            let result = scripted.replica.prepare(mutation)?;
            Ok(ReaderReplicaReply {
                replica: scripted.node.clone(),
                reader_id: result.reader_id,
                ownership_epoch: 1,
                result,
            })
        }

        async fn commit(
            &self,
            replica: &crate::active_range::StorageNodeId,
            evidence: ReaderCommitEvidence,
        ) -> Result<ReaderReplicaReply<ReaderProgressMutation>, ReaderProgressJournalError>
        {
            let (_, scripted) = self
                .nodes
                .iter()
                .find(|(node, _)| node == replica)
                .ok_or(ReaderProgressJournalError::InvalidAssignment)?;
            let result = scripted.replica.commit_with_quorum(evidence)?;
            Ok(ReaderReplicaReply {
                replica: scripted.node.clone(),
                reader_id: result.reader_id,
                ownership_epoch: 1,
                result,
            })
        }

        async fn committed(
            &self,
            replica: &crate::active_range::StorageNodeId,
            reader_id: Uuid,
            ownership_epoch: u64,
        ) -> Result<ReaderReplicaReply<Option<ReaderProgressMutation>>, ReaderProgressJournalError>
        {
            let (_, scripted) = self
                .nodes
                .iter()
                .find(|(node, _)| node == replica)
                .ok_or(ReaderProgressJournalError::InvalidAssignment)?;
            let result = scripted.replica.local_committed(reader_id)?;
            Ok(ReaderReplicaReply {
                replica: scripted.node.clone(),
                reader_id,
                ownership_epoch,
                result,
            })
        }

        async fn inspect(
            &self,
            replica: &crate::active_range::StorageNodeId,
            reader_id: Uuid,
            ownership_epoch: u64,
        ) -> Result<ReaderReplicaReply<ReaderProgressInspection>, ReaderProgressJournalError>
        {
            let (_, scripted) = self
                .nodes
                .iter()
                .find(|(node, _)| node == replica)
                .ok_or(ReaderProgressJournalError::InvalidAssignment)?;
            let result = scripted.replica.local_state(reader_id)?;
            Ok(ReaderReplicaReply {
                replica: scripted.node.clone(),
                reader_id,
                ownership_epoch,
                result,
            })
        }

        async fn adopt(
            &self,
            replica: &crate::active_range::StorageNodeId,
            reader_id: Uuid,
            ownership_epoch: u64,
            committed: Option<ReaderProgressMutation>,
        ) -> Result<ReaderReplicaReply<Option<ReaderProgressMutation>>, ReaderProgressJournalError>
        {
            let (_, scripted) = self
                .nodes
                .iter()
                .find(|(node, _)| node == replica)
                .ok_or(ReaderProgressJournalError::InvalidAssignment)?;
            let result = scripted.replica.adopt_recovered(
                reader_id,
                Uuid::from_u128(2),
                ownership_epoch,
                committed,
            )?;
            Ok(ReaderReplicaReply {
                replica: scripted.node.clone(),
                reader_id,
                ownership_epoch,
                result,
            })
        }
    }

    fn fanout(dir: &tempfile::TempDir) -> ReaderProgressCoordinator {
        let nodes: Vec<_> = ["control-1", "control-2", "control-3"]
            .iter()
            .enumerate()
            .map(|(index, name)| {
                let replica =
                    FjallReaderProgressReplica::open(dir.path().join(format!("r{index}"))).unwrap();
                (
                    node(name),
                    ScriptedReplica {
                        replica,
                        node: node(name),
                    },
                )
            })
            .collect();
        let replicas = crate::active_range::ReplicaSet::try_new([
            node("control-1"),
            node("control-2"),
            node("control-3"),
        ])
        .unwrap();
        let assignment =
            ReaderProgressAssignment::try_new(Uuid::from_u128(1), node("control-1"), replicas, 1)
                .unwrap();
        ReaderProgressCoordinator::new(assignment, std::sync::Arc::new(Fanout { nodes }))
    }

    #[tokio::test]
    async fn coordinator_commits_across_a_quorum_and_replays() {
        let dir = tempfile::tempdir().unwrap();
        let coordinator = fanout(&dir);
        let first = mutation(Uuid::from_u128(1), Uuid::from_u128(2), 1, 100);
        let committed = coordinator.apply(first.clone()).await.unwrap();
        assert_eq!(committed.len(), 3);
        let observed = coordinator.read_committed().await.unwrap().unwrap();
        assert!(observed.same_identity(&first));
        // The same request identity replays without a second decision.
        coordinator.apply(first.clone()).await.unwrap();
        // CAS chain: sequence 2 requires the committed cursor.
        let second = mutation(Uuid::from_u128(1), Uuid::from_u128(2), 2, 200);
        coordinator.apply(second.clone()).await.unwrap();
        let observed = coordinator.read_committed().await.unwrap().unwrap();
        assert!(observed.same_identity(&second));
    }

    #[tokio::test]
    async fn coordinator_reconcile_retry_accepts_the_committed_decision() {
        let dir = tempfile::tempdir().unwrap();
        let coordinator = fanout(&dir);
        let first = mutation(Uuid::from_u128(1), Uuid::from_u128(2), 1, 100);
        coordinator.apply(first.clone()).await.unwrap();
        let resolved = coordinator.reconcile_retry(first.clone()).await.unwrap();
        assert!(resolved.same_identity(&first));
    }
}
