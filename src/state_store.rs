//! Replicated manual StateStore mutations.
//!
//! A declared manual StateStore (`StateStoreSource::Manual`) accepts writes
//! only through this durable mutation journal. Each mutation carries a
//! client-chosen `mutation_id` idempotency identity and is replicated to the
//! store's RF3 `StateStoreAssignment` before it is acknowledged: the assigned
//! owner allocates a gap-free per-store `sequence` inside its durable
//! prepare, quorum-matching digest votes prove at least two replicas durably
//! hold the entry, and each replica then applies the mutation to its local
//! `IndexStore` at that sequence. A manually populated store is therefore
//! never claimed to be rebuildable from unrelated Feed history — the journal
//! is its source of truth.

use std::path::Path;
use std::sync::Arc;

use fjall::Readable;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::active_range::{ReplicaSet, StorageNodeId};

/// One prepared/committed journal row may not exceed this bound.
pub const STATE_STORE_MAX_MUTATION_BYTES: usize = 256 * 1024;
/// Application-visible row key budget for manual StateStore mutations.
pub const STATE_STORE_MAX_KEY_BYTES: usize = 60 * 1024;
/// Application-visible row payload budget for manual StateStore mutations.
pub const STATE_STORE_MAX_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

const HEAD_KEYSPACE: &str = "state_store_journal_head";
const ENTRIES_KEYSPACE: &str = "state_store_journal_entries";
const IDEMPOTENCY_KEYSPACE: &str = "state_store_journal_idempotency";
const JOURNAL_REPLAY_BUDGET_BYTES: u64 = 64 * 1024 * 1024;
const MEMTABLE_FLUSH_BYTES: u64 = 16 * 1024 * 1024;
const MAX_ALLOCATE_ATTEMPTS: usize = 4;

fn bounded_keyspace() -> fjall::KeyspaceCreateOptions {
    fjall::KeyspaceCreateOptions::default().max_memtable_size(MEMTABLE_FLUSH_BYTES)
}

/// The mutation an application applies to a manual StateStore row. Values
/// remain opaque bytes; typed field extraction is a property of declared
/// Indexes, not of the mutation journal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StateStoreMutationOp {
    /// Create or replace one row.
    PutRow { key: Vec<u8>, payload: Vec<u8> },
    /// Remove one row; deleting a missing row is a recorded no-op.
    DeleteRow { key: Vec<u8> },
}

impl StateStoreMutationOp {
    fn key(&self) -> &[u8] {
        match self {
            Self::PutRow { key, .. } | Self::DeleteRow { key } => key,
        }
    }
}

/// One journaled manual StateStore mutation. `mutation_id` is the
/// client-owned idempotency identity: a retry of the same `mutation_id`
/// returns the committed outcome rather than double-applying, while the same
/// identity carrying different content is refused as a conflict.
/// `sequence` is the owner-allocated store-local total order; a request with
/// `sequence == 0` asks the assigned owner to allocate the next position
/// atomically inside its durable prepare.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateStoreMutation {
    pub store_id: Uuid,
    pub mutation_id: Uuid,
    pub request_id: Uuid,
    pub ownership_epoch: u64,
    pub sequence: u64,
    pub op: StateStoreMutationOp,
    pub issued_at_ns: i64,
}

/// The RF3 placement of one StateStore's mutation journal: three eligible
/// storage Nodes and the owner that allocates sequence positions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateStoreAssignment {
    pub store_id: Uuid,
    pub owner: StorageNodeId,
    pub replicas: ReplicaSet,
    pub ownership_epoch: u64,
}

impl StateStoreAssignment {
    pub fn try_new(
        store_id: Uuid,
        owner: StorageNodeId,
        replicas: ReplicaSet,
        ownership_epoch: u64,
    ) -> Result<Self, StateStoreMutationError> {
        if ownership_epoch == 0 || !replicas.contains(&owner) {
            return Err(StateStoreMutationError::InvalidAssignment);
        }
        Ok(Self {
            store_id,
            owner,
            replicas,
            ownership_epoch,
        })
    }
}

/// Digest evidence that one replica durably prepared a mutation. The vote
/// carries the fully allocated mutation so the coordinator can forward the
/// owner-assigned sequence to the remaining replicas.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateStorePrepareVote {
    pub store_id: Uuid,
    pub mutation_id: Uuid,
    pub request_id: Uuid,
    pub mutation: StateStoreMutation,
    pub digest: [u8; 32],
}

/// Two distinct replica votes proving a mutation was durably prepared.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StateStoreCommitEvidence {
    pub store_id: Uuid,
    pub request_id: Uuid,
    pub mutation_id: Uuid,
    pub sequence: u64,
    pub votes: [(StorageNodeId, [u8; 32]); 2],
}

/// A replica reply carrying the responder's claimed identity so the
/// coordinator can refuse wrong-Node evidence even when digests match.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StateStoreReplicaReply<T> {
    pub replica: StorageNodeId,
    pub store_id: Uuid,
    pub ownership_epoch: u64,
    pub result: T,
}

/// Full committed/prepared/applied state a replica exposes for recovery
/// evidence and coordinator sequence reads.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateStoreJournalInspection {
    pub committed: Option<StateStoreMutation>,
    pub prepared: Option<StateStoreMutation>,
    pub applied_seq: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateStorePrepareRequest {
    pub owner: StorageNodeId,
    pub receiver: StorageNodeId,
    pub mutation: StateStoreMutation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateStoreCommitRequest {
    pub owner: StorageNodeId,
    pub receiver: StorageNodeId,
    pub store_id: Uuid,
    pub ownership_epoch: u64,
    pub evidence: StateStoreCommitEvidence,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateStoreInspectRequest {
    pub owner: StorageNodeId,
    pub receiver: StorageNodeId,
    pub store_id: Uuid,
    pub ownership_epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateStoreLookupRequest {
    pub owner: StorageNodeId,
    pub receiver: StorageNodeId,
    pub store_id: Uuid,
    pub mutation_id: Uuid,
    pub ownership_epoch: u64,
}

#[derive(Debug, Error)]
pub enum StateStoreMutationError {
    #[error(transparent)]
    Engine(#[from] fjall::Error),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
    #[error("StateStore is not declared or not ready for mutations")]
    NotFound,
    #[error("StateStore journal has a conflicting identity, request, or mutation")]
    Conflict,
    #[error("StateStore journal ownership changed; reconcile before retrying")]
    StaleEpoch,
    #[error("StateStore journal sequence has a gap or is occupied")]
    Sequence,
    #[error("StateStore mutation exceeds its bounded storage budget")]
    TooLarge,
    #[error("StateStore journal has no valid two-replica commit evidence")]
    NoQuorum,
    #[error("StateStore journal replica placement or owner is invalid")]
    InvalidAssignment,
    #[error("StateStore journal replica is unavailable; retry the same mutation identity")]
    Unavailable,
    #[error("StateStore mutation result is ambiguous; retry the same mutation identity")]
    AmbiguousCommit,
    #[error("StateStore index apply failed after durable commit")]
    ApplyFailed,
}

impl StateStoreMutationError {
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::NotFound => "state_store.not_found",
            Self::Conflict => "state_store.conflict",
            Self::StaleEpoch => "state_store.stale_epoch",
            Self::Sequence => "state_store.sequence",
            Self::TooLarge => "state_store.too_large",
            Self::NoQuorum => "state_store.no_quorum",
            Self::InvalidAssignment => "state_store.invalid_assignment",
            Self::Unavailable => "state_store.unavailable",
            Self::AmbiguousCommit => "state_store.ambiguous_commit",
            Self::ApplyFailed => "state_store.apply_failed",
            Self::Engine(_) => "state_store.engine",
            Self::Serialization(_) => "state_store.serialization",
        }
    }

    pub(crate) fn from_code(code: &str) -> Option<Self> {
        Some(match code {
            "state_store.not_found" => Self::NotFound,
            "state_store.conflict" => Self::Conflict,
            "state_store.stale_epoch" => Self::StaleEpoch,
            "state_store.sequence" => Self::Sequence,
            "state_store.too_large" => Self::TooLarge,
            "state_store.no_quorum" => Self::NoQuorum,
            "state_store.invalid_assignment" => Self::InvalidAssignment,
            "state_store.unavailable" => Self::Unavailable,
            "state_store.ambiguous_commit" => Self::AmbiguousCommit,
            "state_store.apply_failed" => Self::ApplyFailed,
            _ => return None,
        })
    }
}

/// Replies that mean a replica is behind the current placement rather than
/// contradicting quorum state; they are lagging witnesses and never
/// disqualify otherwise healthy evidence.
fn lagging_witness(error: &StateStoreMutationError) -> bool {
    matches!(
        error,
        StateStoreMutationError::Unavailable
            | StateStoreMutationError::StaleEpoch
            | StateStoreMutationError::InvalidAssignment
            | StateStoreMutationError::Sequence
    )
}

fn validate_mutation_bounds(mutation: &StateStoreMutation) -> Result<(), StateStoreMutationError> {
    if mutation.sequence == 0 {
        // Sequence zero requests owner allocation; the owner fills it inside
        // its durable prepare transaction.
    }
    if mutation.op.key().is_empty() || mutation.op.key().len() > STATE_STORE_MAX_KEY_BYTES {
        return Err(StateStoreMutationError::TooLarge);
    }
    if let StateStoreMutationOp::PutRow { payload, .. } = &mutation.op {
        if payload.len() > STATE_STORE_MAX_PAYLOAD_BYTES {
            return Err(StateStoreMutationError::TooLarge);
        }
    }
    Ok(())
}

/// Durable per-store journal state: one head row plus the committed
/// mutation log and the mutation-id idempotency map.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct StateStoreJournalHead {
    store_id: Uuid,
    ownership_epoch: u64,
    committed_seq: u64,
    committed_mutation_id: Option<Uuid>,
    applied_seq: u64,
    prepared: Option<StateStoreMutation>,
}

impl StateStoreJournalHead {
    fn empty(store_id: Uuid, ownership_epoch: u64) -> Self {
        Self {
            store_id,
            ownership_epoch,
            committed_seq: 0,
            committed_mutation_id: None,
            applied_seq: 0,
            prepared: None,
        }
    }
}

fn entries_key(store_id: Uuid, sequence: u64) -> [u8; 24] {
    let mut key = [0u8; 24];
    key[..16].copy_from_slice(store_id.as_bytes());
    key[16..].copy_from_slice(&sequence.to_be_bytes());
    key
}

fn idempotency_key(store_id: Uuid, mutation_id: Uuid) -> [u8; 32] {
    let mut key = [0u8; 32];
    key[..16].copy_from_slice(store_id.as_bytes());
    key[16..].copy_from_slice(mutation_id.as_bytes());
    key
}

fn mutation_digest(mutation: &StateStoreMutation) -> Result<[u8; 32], StateStoreMutationError> {
    let bytes = serde_json::to_vec(mutation)?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

/// One storage Node's durable journal for manual StateStore mutations. The
/// head keyspace holds per-store ordering/fencing state; `entries` is the
/// committed mutation log replicas apply from; `idempotency` maps
/// client mutation identities onto their committed sequence.
pub struct FjallStateStoreMutationJournal {
    db: fjall::SingleWriterTxDatabase,
    head: fjall::SingleWriterTxKeyspace,
    entries: fjall::SingleWriterTxKeyspace,
    idempotency: fjall::SingleWriterTxKeyspace,
}

impl FjallStateStoreMutationJournal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StateStoreMutationError> {
        let db = fjall::SingleWriterTxDatabase::builder(path)
            .max_journaling_size(JOURNAL_REPLAY_BUDGET_BYTES)
            .open()?;
        let head = db.keyspace(HEAD_KEYSPACE, bounded_keyspace)?;
        let entries = db.keyspace(ENTRIES_KEYSPACE, bounded_keyspace)?;
        let idempotency = db.keyspace(IDEMPOTENCY_KEYSPACE, bounded_keyspace)?;
        Ok(Self {
            db,
            head,
            entries,
            idempotency,
        })
    }

    fn read_head(
        &self,
        store_id: Uuid,
    ) -> Result<Option<StateStoreJournalHead>, StateStoreMutationError> {
        self.db
            .read_tx()
            .get(&self.head, store_id.as_bytes())?
            .map(|bytes| serde_json::from_slice::<StateStoreJournalHead>(&bytes))
            .transpose()
            .map_err(StateStoreMutationError::from)
    }

    /// The committed mutation at one sequence position, if any.
    pub fn entry(
        &self,
        store_id: Uuid,
        sequence: u64,
    ) -> Result<Option<StateStoreMutation>, StateStoreMutationError> {
        self.db
            .read_tx()
            .get(&self.entries, entries_key(store_id, sequence))?
            .map(|bytes| serde_json::from_slice::<StateStoreMutation>(&bytes))
            .transpose()
            .map_err(StateStoreMutationError::from)
    }

    /// The committed mutation recorded for one client idempotency identity.
    pub fn mutation_by_id(
        &self,
        store_id: Uuid,
        mutation_id: Uuid,
    ) -> Result<Option<StateStoreMutation>, StateStoreMutationError> {
        let Some(seq_bytes) = self
            .db
            .read_tx()
            .get(&self.idempotency, idempotency_key(store_id, mutation_id))?
        else {
            return Ok(None);
        };
        let seq_bytes: [u8; 8] = seq_bytes
            .as_ref()
            .try_into()
            .map_err(|_| StateStoreMutationError::Conflict)?;
        self.entry(store_id, u64::from_be_bytes(seq_bytes))
    }

    /// Committed and prepared state for recovery evidence and coordinator
    /// sequence reads.
    pub fn inspect(
        &self,
        store_id: Uuid,
    ) -> Result<StateStoreJournalInspection, StateStoreMutationError> {
        let Some(head) = self.read_head(store_id)? else {
            return Ok(StateStoreJournalInspection::default());
        };
        let committed = match head.committed_mutation_id {
            Some(mutation_id) => self.mutation_by_id(store_id, mutation_id)?,
            None => None,
        };
        Ok(StateStoreJournalInspection {
            committed,
            prepared: head.prepared,
            applied_seq: head.applied_seq,
        })
    }

    /// The last applied sequence the replica durably recorded.
    pub fn applied_seq(&self, store_id: Uuid) -> Result<u64, StateStoreMutationError> {
        Ok(self
            .read_head(store_id)?
            .map(|head| head.applied_seq)
            .unwrap_or(0))
    }

    /// Committed entries the local replica has not yet marked applied.
    pub fn pending_applies(
        &self,
        store_id: Uuid,
    ) -> Result<Vec<StateStoreMutation>, StateStoreMutationError> {
        let Some(head) = self.read_head(store_id)? else {
            return Ok(Vec::new());
        };
        let mut pending = Vec::new();
        for sequence in (head.applied_seq + 1)..=head.committed_seq {
            if let Some(mutation) = self.entry(store_id, sequence)? {
                pending.push(mutation);
            }
        }
        Ok(pending)
    }

    /// Store ids with journal head state, for restart replay sweeps.
    pub fn stores(&self) -> Result<Vec<Uuid>, StateStoreMutationError> {
        let mut stores = Vec::new();
        for guard in self.db.read_tx().iter(&self.head) {
            let (key, _) = guard.into_inner()?;
            let bytes: [u8; 16] = key
                .as_ref()
                .try_into()
                .map_err(|_| StateStoreMutationError::Conflict)?;
            stores.push(Uuid::from_bytes(bytes));
        }
        Ok(stores)
    }

    /// Durably mark committed entries as applied to the local IndexStore.
    /// Applied never moves backwards.
    pub fn mark_applied(
        &self,
        store_id: Uuid,
        sequence: u64,
    ) -> Result<(), StateStoreMutationError> {
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let mut head = tx
            .get(&self.head, store_id.as_bytes())?
            .map(|bytes| serde_json::from_slice::<StateStoreJournalHead>(&bytes))
            .transpose()?
            .ok_or(StateStoreMutationError::NotFound)?;
        if head.committed_seq < sequence {
            return Err(StateStoreMutationError::Sequence);
        }
        if head.applied_seq < sequence {
            head.applied_seq = sequence;
            tx.insert(&self.head, store_id.as_bytes(), serde_json::to_vec(&head)?);
        }
        tx.commit()?;
        Ok(())
    }

    /// Durably prepare one mutation. When `mutation.sequence == 0` the owner
    /// allocates `committed_seq + 1` inside the same durable transaction, so
    /// sequence assignment can never fork or repeat. Idempotent by
    /// `mutation_id`: an identical re-prepare re-votes, a different mutation
    /// under a known identity conflicts, and a foreign in-flight prepare
    /// answers as a lagging witness.
    pub fn prepare(
        &self,
        mutation: StateStoreMutation,
    ) -> Result<StateStorePrepareVote, StateStoreMutationError> {
        validate_mutation_bounds(&mutation)?;
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let previous = tx
            .get(&self.head, mutation.store_id.as_bytes())?
            .map(|bytes| serde_json::from_slice::<StateStoreJournalHead>(&bytes))
            .transpose()?;
        let mut head = match previous {
            Some(head) => head,
            None => StateStoreJournalHead::empty(mutation.store_id, mutation.ownership_epoch),
        };
        if head.store_id != mutation.store_id {
            return Err(StateStoreMutationError::Conflict);
        }
        if head.ownership_epoch != mutation.ownership_epoch {
            return Err(StateStoreMutationError::StaleEpoch);
        }

        let existing_seq = tx
            .get(
                &self.idempotency,
                idempotency_key(mutation.store_id, mutation.mutation_id),
            )?
            .map(|bytes| <[u8; 8]>::try_from(bytes.as_ref()))
            .transpose()
            .map_err(|_| StateStoreMutationError::Conflict)?
            .map(u64::from_be_bytes);
        if let Some(sequence) = existing_seq {
            let committed = tx
                .get(&self.entries, entries_key(mutation.store_id, sequence))?
                .map(|bytes| serde_json::from_slice::<StateStoreMutation>(&bytes))
                .transpose()?
                .ok_or(StateStoreMutationError::Sequence)?;
            return if committed == mutation {
                Ok(StateStorePrepareVote {
                    store_id: committed.store_id,
                    mutation_id: committed.mutation_id,
                    request_id: committed.request_id,
                    digest: mutation_digest(&committed)?,
                    mutation: committed,
                })
            } else {
                Err(StateStoreMutationError::Conflict)
            };
        }

        let mut mutation = mutation;
        let allocating = mutation.sequence == 0;
        if allocating {
            mutation.sequence = head
                .committed_seq
                .checked_add(1)
                .ok_or(StateStoreMutationError::Sequence)?;
        }
        if let Some(prepared) = &head.prepared {
            if prepared == &mutation {
                return Ok(StateStorePrepareVote {
                    store_id: mutation.store_id,
                    mutation_id: mutation.mutation_id,
                    request_id: mutation.request_id,
                    digest: mutation_digest(&mutation)?,
                    mutation,
                });
            }
            if prepared.mutation_id == mutation.mutation_id {
                // Same client identity retried with a fresh request_id: the
                // prepared entry re-votes when every committed-significant
                // field matches, and conflicts when the identity was reused
                // for different content.
                let same = prepared.store_id == mutation.store_id
                    && prepared.ownership_epoch == mutation.ownership_epoch
                    && prepared.sequence == mutation.sequence
                    && prepared.op == mutation.op;
                return if same {
                    Ok(StateStorePrepareVote {
                        store_id: prepared.store_id,
                        mutation_id: prepared.mutation_id,
                        request_id: mutation.request_id,
                        digest: mutation_digest(prepared)?,
                        mutation: prepared.clone(),
                    })
                } else {
                    Err(StateStoreMutationError::Conflict)
                };
            }
            // Residue from an abandoned vote: this replica is behind the
            // quorum's committed trail; answer as a lagging witness.
            return Err(StateStoreMutationError::Sequence);
        }
        if !allocating && head.committed_seq.checked_add(1) != Some(mutation.sequence) {
            return Err(StateStoreMutationError::Sequence);
        }
        let vote = StateStorePrepareVote {
            store_id: mutation.store_id,
            mutation_id: mutation.mutation_id,
            request_id: mutation.request_id,
            digest: mutation_digest(&mutation)?,
            mutation: mutation.clone(),
        };
        head.prepared = Some(mutation);
        let encoded = serde_json::to_vec(&head)?;
        if encoded.len() > STATE_STORE_MAX_MUTATION_BYTES {
            return Err(StateStoreMutationError::TooLarge);
        }
        tx.insert(&self.head, head.store_id.as_bytes(), encoded);
        tx.commit()?;
        Ok(vote)
    }

    /// Commit the prepared mutation when quorum digest evidence matches. A
    /// matching already-committed entry answers idempotently; a conflicting
    /// one refuses. The entry, the idempotency identity, and the head flip
    /// land in one durable transaction.
    pub fn commit_with_quorum(
        &self,
        store_id: Uuid,
        ownership_epoch: u64,
        evidence: &StateStoreCommitEvidence,
    ) -> Result<StateStoreMutation, StateStoreMutationError> {
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let mut head = tx
            .get(&self.head, store_id.as_bytes())?
            .map(|bytes| serde_json::from_slice::<StateStoreJournalHead>(&bytes))
            .transpose()?
            .ok_or(StateStoreMutationError::NotFound)?;
        if head.ownership_epoch != ownership_epoch {
            return Err(StateStoreMutationError::StaleEpoch);
        }
        if evidence.store_id != store_id {
            return Err(StateStoreMutationError::Conflict);
        }
        if let Some(prepared) = &head.prepared {
            if prepared.mutation_id == evidence.mutation_id
                && prepared.sequence == evidence.sequence
            {
                let digest = mutation_digest(prepared)?;
                if evidence.votes.iter().all(|(_, vote)| *vote == digest) {
                    let mutation = prepared.clone();
                    let encoded = serde_json::to_vec(&mutation)?;
                    if encoded.len() > STATE_STORE_MAX_MUTATION_BYTES {
                        return Err(StateStoreMutationError::TooLarge);
                    }
                    tx.insert(
                        &self.entries,
                        entries_key(store_id, mutation.sequence),
                        encoded,
                    );
                    tx.insert(
                        &self.idempotency,
                        idempotency_key(store_id, mutation.mutation_id),
                        mutation.sequence.to_be_bytes(),
                    );
                    head.committed_seq = mutation.sequence;
                    head.committed_mutation_id = Some(mutation.mutation_id);
                    head.prepared = None;
                    tx.insert(&self.head, store_id.as_bytes(), serde_json::to_vec(&head)?);
                    tx.commit()?;
                    return Ok(mutation);
                }
                return Err(StateStoreMutationError::Conflict);
            }
        }
        // No matching prepared entry: a retry after ambiguous commit finds
        // the already-committed mutation by its idempotency identity.
        let existing_seq = tx
            .get(
                &self.idempotency,
                idempotency_key(store_id, evidence.mutation_id),
            )?
            .map(|bytes| <[u8; 8]>::try_from(bytes.as_ref()))
            .transpose()
            .map_err(|_| StateStoreMutationError::Conflict)?
            .map(u64::from_be_bytes);
        if let Some(sequence) = existing_seq {
            let committed = tx
                .get(&self.entries, entries_key(store_id, sequence))?
                .map(|bytes| serde_json::from_slice::<StateStoreMutation>(&bytes))
                .transpose()?
                .ok_or(StateStoreMutationError::Sequence)?;
            if committed.request_id == evidence.request_id
                || (committed.mutation_id == evidence.mutation_id
                    && committed.sequence == evidence.sequence)
            {
                return Ok(committed);
            }
            return Err(StateStoreMutationError::Conflict);
        }
        Err(StateStoreMutationError::Conflict)
    }
}

/// Applies a committed mutation to the replica's local IndexStore. Kept as a
/// narrow seam so tests can capture applies without storage.
pub trait StateStoreApply: Send + Sync {
    fn apply(&self, mutation: &StateStoreMutation) -> Result<(), StateStoreMutationError>;
}

/// Applies manual mutations as primary rows: the store id namespaces
/// `PrimaryRef.feed_id` (manual stores have no source Feed), the row key is
/// the application key, and the journal sequence is the applied Cursor.
pub struct IndexStoreStateApply {
    store: Arc<dyn crate::index::IndexStore>,
}

impl IndexStoreStateApply {
    pub fn new(store: Arc<dyn crate::index::IndexStore>) -> Self {
        Self { store }
    }

    fn cursor(sequence: u64) -> String {
        format!("{sequence:020}")
    }
}

impl StateStoreApply for IndexStoreStateApply {
    fn apply(&self, mutation: &StateStoreMutation) -> Result<(), StateStoreMutationError> {
        let reference = crate::index::PrimaryRef {
            feed_id: mutation.store_id,
            application_key: mutation.op.key().to_vec(),
        };
        match &mutation.op {
            StateStoreMutationOp::PutRow { payload, .. } => self
                .store
                .upsert(&reference, payload, &[], &Self::cursor(mutation.sequence))
                .map_err(|_| StateStoreMutationError::ApplyFailed)?,
            StateStoreMutationOp::DeleteRow { .. } => {
                self.store
                    .delete(&reference, &Self::cursor(mutation.sequence))
                    .map_err(|_| StateStoreMutationError::ApplyFailed)?;
            }
        }
        Ok(())
    }
}

/// The wire surface one replica offers the coordinator. Injected so the
/// coordinator is transport-agnostic and unit-testable in-memory.
#[async_trait::async_trait]
pub trait StateStoreMutationTransport: Send + Sync {
    async fn prepare(
        &self,
        replica: &StorageNodeId,
        request: StateStorePrepareRequest,
    ) -> Result<StateStoreReplicaReply<StateStorePrepareVote>, StateStoreMutationError>;

    async fn commit(
        &self,
        replica: &StorageNodeId,
        request: StateStoreCommitRequest,
    ) -> Result<StateStoreReplicaReply<StateStoreMutation>, StateStoreMutationError>;

    async fn inspect(
        &self,
        replica: &StorageNodeId,
        request: StateStoreInspectRequest,
    ) -> Result<StateStoreReplicaReply<StateStoreJournalInspection>, StateStoreMutationError>;

    async fn lookup(
        &self,
        replica: &StorageNodeId,
        request: StateStoreLookupRequest,
    ) -> Result<StateStoreReplicaReply<Option<StateStoreMutation>>, StateStoreMutationError>;
}

/// Drives manual mutations through the replicated prepare/commit protocol
/// over an injected transport. Bound to one StateStore's assignment: owner
/// participation is mandatory at both stages, matching the effect journal's
/// quorum rules.
pub struct StateStoreMutationCoordinator {
    assignment: StateStoreAssignment,
    transport: Arc<dyn StateStoreMutationTransport>,
}

/// The acknowledged outcome of a replicated mutation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateStoreMutationResult {
    pub store_id: Uuid,
    pub mutation_id: Uuid,
    pub sequence: u64,
    pub deduplicated: bool,
}

impl StateStoreMutationCoordinator {
    pub fn new(
        assignment: StateStoreAssignment,
        transport: Arc<dyn StateStoreMutationTransport>,
    ) -> Self {
        Self {
            assignment,
            transport,
        }
    }

    /// Resolve the coordinator for a StateStore's current placement through
    /// the catalog.
    pub async fn for_store(
        control: &crate::control::ControlController,
        store_id: Uuid,
        transport: Arc<dyn StateStoreMutationTransport>,
    ) -> Result<Self, StateStoreMutationError> {
        let assignment = control
            .active_state_store_assignment_by_id(store_id)
            .await
            .ok_or(StateStoreMutationError::InvalidAssignment)?;
        Ok(Self::new(assignment, transport))
    }

    fn request(&self, mutation: StateStoreMutation) -> StateStorePrepareRequest {
        StateStorePrepareRequest {
            owner: self.assignment.owner.clone(),
            receiver: self.assignment.owner.clone(),
            mutation,
        }
    }

    /// Quorum-read the journal by client idempotency identity; any replica
    /// answering a committed mutation settles retries of an ambiguous
    /// outcome without re-applying.
    pub async fn read_committed(
        &self,
        mutation_id: Uuid,
    ) -> Result<Option<StateStoreMutation>, StateStoreMutationError> {
        let mut observed: Option<StateStoreMutation> = None;
        let mut votes = 0;
        let store_id = self.assignment.store_id;
        let replies = futures_util::future::join_all(self.assignment.replicas.iter().map(|node| {
            self.transport.lookup(
                node,
                StateStoreLookupRequest {
                    owner: self.assignment.owner.clone(),
                    receiver: node.clone(),
                    store_id,
                    mutation_id,
                    ownership_epoch: self.assignment.ownership_epoch,
                },
            )
        }))
        .await;
        for (node, reply) in self.assignment.replicas.iter().zip(replies) {
            match reply {
                Ok(reply) => {
                    if reply.replica != *node
                        || reply.store_id != store_id
                        || reply.ownership_epoch != self.assignment.ownership_epoch
                    {
                        return Err(StateStoreMutationError::InvalidAssignment);
                    }
                    votes += 1;
                    if let Some(candidate) = reply.result {
                        match &observed {
                            Some(current) if *current != candidate => {
                                match candidate.sequence.cmp(&current.sequence) {
                                    std::cmp::Ordering::Greater => observed = Some(candidate),
                                    std::cmp::Ordering::Equal => {
                                        return Err(StateStoreMutationError::Conflict)
                                    }
                                    std::cmp::Ordering::Less => {}
                                }
                            }
                            Some(_) => {}
                            None => observed = Some(candidate),
                        }
                    }
                }
                Err(error) if lagging_witness(&error) => {}
                Err(error) => return Err(error),
            }
        }
        if votes < 2 {
            return Err(StateStoreMutationError::NoQuorum);
        }
        Ok(observed)
    }

    /// Prepare one mutation on the owner (which allocates the sequence when
    /// `sequence == 0`), forward the allocated mutation to the remaining
    /// replicas, then commit where matching digests prove durability.
    /// Returns the acknowledged result; an ambiguous outcome must be retried
    /// with the same `mutation_id`, which `read_committed` resolves.
    pub async fn mutate(
        &self,
        mut mutation: StateStoreMutation,
    ) -> Result<StateStoreMutationResult, StateStoreMutationError> {
        if mutation.store_id != self.assignment.store_id
            || mutation.ownership_epoch != self.assignment.ownership_epoch
        {
            return Err(StateStoreMutationError::InvalidAssignment);
        }
        // Idempotent fast path: the identity already committed quorum-wide.
        if let Some(prior) = self.read_committed(mutation.mutation_id).await? {
            if prior.store_id == mutation.store_id && prior.op == mutation.op {
                return Ok(StateStoreMutationResult {
                    store_id: prior.store_id,
                    mutation_id: prior.mutation_id,
                    sequence: prior.sequence,
                    deduplicated: true,
                });
            }
            return Err(StateStoreMutationError::Conflict);
        }

        let mut prepared = Vec::with_capacity(3);
        if mutation.sequence == 0 {
            // Owner-allocated sequencing: the owner's durable prepare assigns
            // committed_seq + 1, then the allocated mutation fans out. A
            // Sequence reply means a foreign prepare still occupies the
            // slot — bounded retry lets that in-flight commit finish.
            let reply = {
                let mut attempts = 0usize;
                loop {
                    let owner_request = self.request(mutation.clone());
                    match self
                        .transport
                        .prepare(&self.assignment.owner, owner_request)
                        .await
                    {
                        Err(StateStoreMutationError::Sequence)
                            if attempts + 1 < MAX_ALLOCATE_ATTEMPTS =>
                        {
                            attempts += 1;
                            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                            continue;
                        }
                        Err(error) if lagging_witness(&error) => {
                            return Err(StateStoreMutationError::NoQuorum)
                        }
                        Err(error) => return Err(error),
                        Ok(reply) => break reply,
                    }
                }
            };
            if reply.replica != self.assignment.owner
                || reply.store_id != mutation.store_id
                || reply.ownership_epoch != self.assignment.ownership_epoch
                || reply.result.mutation.sequence == 0
            {
                return Err(StateStoreMutationError::InvalidAssignment);
            }
            mutation = reply.result.mutation.clone();
            prepared.push((self.assignment.owner.clone(), reply.result.digest));
        }
        let replies = futures_util::future::join_all(
            self.assignment
                .replicas
                .iter()
                .filter(|node| !prepared.iter().any(|(member, _)| member == *node))
                .map(|node| {
                    let mut request = self.request(mutation.clone());
                    request.receiver = node.clone();
                    self.transport.prepare(node, request)
                }),
        )
        .await;
        for reply in replies {
            match reply {
                Ok(reply)
                    if !self.assignment.replicas.contains(&reply.replica)
                        || reply.store_id != mutation.store_id
                        || reply.ownership_epoch != self.assignment.ownership_epoch
                        || reply.result.mutation != mutation =>
                {
                    return Err(StateStoreMutationError::InvalidAssignment)
                }
                Ok(reply) => prepared.push((reply.replica.clone(), reply.result.digest)),
                Err(error) if lagging_witness(&error) => {}
                Err(error) => return Err(error),
            }
        }
        if prepared.len() < 2
            || !prepared
                .iter()
                .any(|(node, _)| node == &self.assignment.owner)
        {
            return Err(StateStoreMutationError::NoQuorum);
        }
        let digest = prepared
            .iter()
            .find(|(node, _)| node == &self.assignment.owner)
            .map(|(_, digest)| *digest)
            .ok_or(StateStoreMutationError::NoQuorum)?;
        if prepared.iter().any(|(_, value)| *value != digest) {
            return Err(StateStoreMutationError::Conflict);
        }
        let other = prepared
            .iter()
            .find(|(node, _)| node != &self.assignment.owner)
            .map(|(node, _)| node.clone())
            .ok_or(StateStoreMutationError::NoQuorum)?;
        let evidence = StateStoreCommitEvidence {
            store_id: mutation.store_id,
            request_id: mutation.request_id,
            mutation_id: mutation.mutation_id,
            sequence: mutation.sequence,
            votes: [(self.assignment.owner.clone(), digest), (other, digest)],
        };
        let mut committed = Vec::with_capacity(3);
        let replies = futures_util::future::join_all(prepared.iter().map(|(node, _)| {
            self.transport.commit(
                node,
                StateStoreCommitRequest {
                    owner: self.assignment.owner.clone(),
                    receiver: node.clone(),
                    store_id: mutation.store_id,
                    ownership_epoch: self.assignment.ownership_epoch,
                    evidence: evidence.clone(),
                },
            )
        }))
        .await;
        for reply in replies {
            match reply {
                Ok(reply)
                    if !self.assignment.replicas.contains(&reply.replica)
                        || reply.store_id != mutation.store_id
                        || reply.ownership_epoch != self.assignment.ownership_epoch =>
                {
                    return Err(StateStoreMutationError::InvalidAssignment)
                }
                Ok(reply) if reply.result == mutation => committed.push(reply.replica),
                Ok(_) => return Err(StateStoreMutationError::Conflict),
                Err(error) if lagging_witness(&error) => {}
                Err(StateStoreMutationError::Conflict) => {
                    return Err(StateStoreMutationError::AmbiguousCommit)
                }
                Err(error) => return Err(error),
            }
        }
        if committed.len() < 2 || !committed.contains(&self.assignment.owner) {
            return Err(StateStoreMutationError::AmbiguousCommit);
        }
        Ok(StateStoreMutationResult {
            store_id: mutation.store_id,
            mutation_id: mutation.mutation_id,
            sequence: mutation.sequence,
            deduplicated: false,
        })
    }
}

/// HTTP transport over the internal plane (shared key or mTLS), the same
/// wire shape the effect journal uses.
pub struct HttpStateStoreMutationTransport {
    assignment: StateStoreAssignment,
    endpoints: crate::internal_plane::InternalEndpoints,
    key: Option<String>,
    client: reqwest::Client,
}

impl HttpStateStoreMutationTransport {
    pub fn new(
        assignment: StateStoreAssignment,
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
        assignment: StateStoreAssignment,
        endpoints: impl Into<crate::internal_plane::InternalEndpoints>,
        ca_pem: &[u8],
        identity_pem: &[u8],
        timeout: std::time::Duration,
    ) -> Result<Self, StateStoreMutationError> {
        let endpoints = endpoints.into();
        for (node, endpoint) in endpoints.configured() {
            let url = reqwest::Url::parse(endpoint)
                .map_err(|_| StateStoreMutationError::InvalidAssignment)?;
            if url.scheme() != "https" || url.host_str() != Some(node.as_str()) {
                return Err(StateStoreMutationError::InvalidAssignment);
            }
        }
        let ca = reqwest::Certificate::from_pem(ca_pem)
            .map_err(|_| StateStoreMutationError::InvalidAssignment)?;
        let identity = reqwest::Identity::from_pem(identity_pem)
            .map_err(|_| StateStoreMutationError::InvalidAssignment)?;
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(1))
            .timeout(timeout)
            .https_only(true)
            .tls_built_in_root_certs(false)
            .add_root_certificate(ca)
            .identity(identity)
            .build()
            .map_err(|_| StateStoreMutationError::InvalidAssignment)?;
        Ok(Self {
            assignment,
            endpoints,
            key: None,
            client,
        })
    }

    async fn send<R: Serialize, T: serde::de::DeserializeOwned>(
        &self,
        node: &StorageNodeId,
        path: &str,
        body: &R,
    ) -> Result<T, StateStoreMutationError> {
        if !self.assignment.replicas.iter().any(|member| member == node) {
            return Err(StateStoreMutationError::InvalidAssignment);
        }
        let endpoint = self
            .endpoints
            .resolve(node)
            .await
            .ok_or(StateStoreMutationError::Unavailable)?;
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
                tracing::warn!(node = %node, path, %error, "StateStore journal request failed");
                return Err(StateStoreMutationError::Unavailable);
            }
        };
        if !response.status().is_success() {
            let status = response.status();
            let body = response.bytes().await.unwrap_or_default();
            tracing::warn!(node = %node, path, %status, "StateStore journal replica refused");
            let code = body
                .get(..4096)
                .and_then(|head| serde_json::from_slice::<serde_json::Value>(head).ok())
                .and_then(|value| {
                    value
                        .get("code")
                        .and_then(|code| code.as_str())
                        .map(str::to_owned)
                });
            if let Some(error) = code.as_deref().and_then(StateStoreMutationError::from_code) {
                return Err(error);
            }
            return Err(if status == reqwest::StatusCode::CONFLICT {
                StateStoreMutationError::Conflict
            } else {
                StateStoreMutationError::Unavailable
            });
        }
        serde_json::from_slice(
            &response
                .bytes()
                .await
                .map_err(|_| StateStoreMutationError::Unavailable)?,
        )
        .map_err(|_| StateStoreMutationError::Unavailable)
    }
}

#[async_trait::async_trait]
impl StateStoreMutationTransport for HttpStateStoreMutationTransport {
    async fn prepare(
        &self,
        replica: &StorageNodeId,
        request: StateStorePrepareRequest,
    ) -> Result<StateStoreReplicaReply<StateStorePrepareVote>, StateStoreMutationError> {
        self.send(replica, "/internal/state-store-journal/prepare", &request)
            .await
    }

    async fn commit(
        &self,
        replica: &StorageNodeId,
        request: StateStoreCommitRequest,
    ) -> Result<StateStoreReplicaReply<StateStoreMutation>, StateStoreMutationError> {
        if request.ownership_epoch != self.assignment.ownership_epoch {
            return Err(StateStoreMutationError::StaleEpoch);
        }
        self.send(replica, "/internal/state-store-journal/commit", &request)
            .await
    }

    async fn inspect(
        &self,
        replica: &StorageNodeId,
        request: StateStoreInspectRequest,
    ) -> Result<StateStoreReplicaReply<StateStoreJournalInspection>, StateStoreMutationError> {
        if request.ownership_epoch != self.assignment.ownership_epoch {
            return Err(StateStoreMutationError::StaleEpoch);
        }
        self.send(replica, "/internal/state-store-journal/inspect", &request)
            .await
    }

    async fn lookup(
        &self,
        replica: &StorageNodeId,
        request: StateStoreLookupRequest,
    ) -> Result<StateStoreReplicaReply<Option<StateStoreMutation>>, StateStoreMutationError> {
        if request.ownership_epoch != self.assignment.ownership_epoch {
            return Err(StateStoreMutationError::StaleEpoch);
        }
        self.send(replica, "/internal/state-store-journal/lookup", &request)
            .await
    }
}

/// Node-side service fencing journal RPCs against the catalog's current
/// placement, mirroring the effect journal's guards.
pub struct StateStoreMutationService {
    local_node: StorageNodeId,
    control: Arc<crate::control::ControlController>,
    journal: Arc<FjallStateStoreMutationJournal>,
    apply: Arc<dyn StateStoreApply>,
}

impl StateStoreMutationService {
    pub fn new(
        local_node: StorageNodeId,
        control: Arc<crate::control::ControlController>,
        journal: Arc<FjallStateStoreMutationJournal>,
        apply: Arc<dyn StateStoreApply>,
    ) -> Self {
        Self {
            local_node,
            control,
            journal,
            apply,
        }
    }

    async fn check_placement(
        &self,
        store_id: Uuid,
        owner: &StorageNodeId,
        receiver: &StorageNodeId,
        epoch: u64,
    ) -> Result<StateStoreAssignment, StateStoreMutationError> {
        let assignment = self
            .control
            .active_state_store_assignment_by_id(store_id)
            .await
            .ok_or(StateStoreMutationError::InvalidAssignment)?;
        if assignment.owner != *owner
            || epoch != assignment.ownership_epoch
            || *receiver != self.local_node
            || !assignment.replicas.iter().any(|node| node == receiver)
        {
            return Err(StateStoreMutationError::InvalidAssignment);
        }
        Ok(assignment)
    }

    pub async fn prepare(
        &self,
        request: StateStorePrepareRequest,
    ) -> Result<StateStoreReplicaReply<StateStorePrepareVote>, StateStoreMutationError> {
        let assignment = self
            .check_placement(
                request.mutation.store_id,
                &request.owner,
                &request.receiver,
                request.mutation.ownership_epoch,
            )
            .await?;
        let journal = self.journal.clone();
        let mutation = request.mutation;
        let store_id = mutation.store_id;
        let result = tokio::task::spawn_blocking(move || journal.prepare(mutation))
            .await
            .map_err(|_| StateStoreMutationError::Unavailable)??;
        Ok(StateStoreReplicaReply {
            replica: self.local_node.clone(),
            store_id,
            ownership_epoch: assignment.ownership_epoch,
            result,
        })
    }

    pub async fn commit(
        &self,
        request: StateStoreCommitRequest,
    ) -> Result<StateStoreReplicaReply<StateStoreMutation>, StateStoreMutationError> {
        let assignment = self
            .check_placement(
                request.store_id,
                &request.owner,
                &request.receiver,
                request.ownership_epoch,
            )
            .await?;
        if request.evidence.store_id != request.store_id {
            return Err(StateStoreMutationError::Conflict);
        }
        let journal = self.journal.clone();
        let apply = self.apply.clone();
        let store_id = request.store_id;
        let epoch = request.ownership_epoch;
        let evidence = request.evidence;
        let committed = tokio::task::spawn_blocking(move || {
            journal.commit_with_quorum(store_id, epoch, &evidence)
        })
        .await
        .map_err(|_| StateStoreMutationError::Unavailable)??;
        // The replica applies to its local IndexStore at the committed
        // sequence, then durably marks the position. Same-sequence replays
        // are index-level idempotent, so a partially applied restart simply
        // re-runs pending entries.
        let apply_mutation = committed.clone();
        let apply_result = tokio::task::spawn_blocking(move || apply.apply(&apply_mutation)).await;
        match apply_result {
            Ok(Ok(())) => {
                let journal = self.journal.clone();
                let sequence = committed.sequence;
                tokio::task::spawn_blocking(move || journal.mark_applied(store_id, sequence))
                    .await
                    .map_err(|_| StateStoreMutationError::Unavailable)??;
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => return Err(StateStoreMutationError::Unavailable),
        }
        Ok(StateStoreReplicaReply {
            replica: self.local_node.clone(),
            store_id,
            ownership_epoch: assignment.ownership_epoch,
            result: committed,
        })
    }

    pub async fn inspect(
        &self,
        request: StateStoreInspectRequest,
    ) -> Result<StateStoreReplicaReply<StateStoreJournalInspection>, StateStoreMutationError> {
        let assignment = self
            .check_placement(
                request.store_id,
                &request.owner,
                &request.receiver,
                request.ownership_epoch,
            )
            .await?;
        let journal = self.journal.clone();
        let store_id = request.store_id;
        let result = tokio::task::spawn_blocking(move || journal.inspect(store_id))
            .await
            .map_err(|_| StateStoreMutationError::Unavailable)??;
        Ok(StateStoreReplicaReply {
            replica: self.local_node.clone(),
            store_id,
            ownership_epoch: assignment.ownership_epoch,
            result,
        })
    }

    pub async fn lookup(
        &self,
        request: StateStoreLookupRequest,
    ) -> Result<StateStoreReplicaReply<Option<StateStoreMutation>>, StateStoreMutationError> {
        let assignment = self
            .check_placement(
                request.store_id,
                &request.owner,
                &request.receiver,
                request.ownership_epoch,
            )
            .await?;
        let journal = self.journal.clone();
        let store_id = request.store_id;
        let mutation_id = request.mutation_id;
        let result =
            tokio::task::spawn_blocking(move || journal.mutation_by_id(store_id, mutation_id))
                .await
                .map_err(|_| StateStoreMutationError::Unavailable)??;
        Ok(StateStoreReplicaReply {
            replica: self.local_node.clone(),
            store_id,
            ownership_epoch: assignment.ownership_epoch,
            result,
        })
    }

    /// Replay committed-but-unapplied entries after restart. Called at
    /// service construction; a partially applied position simply re-runs.
    pub async fn replay_pending(&self, store_id: Uuid) -> Result<u64, StateStoreMutationError> {
        let journal = self.journal.clone();
        let pending = tokio::task::spawn_blocking(move || journal.pending_applies(store_id))
            .await
            .map_err(|_| StateStoreMutationError::Unavailable)??;
        let mut replayed = 0u64;
        for mutation in pending {
            let apply = self.apply.clone();
            let replay = mutation.clone();
            tokio::task::spawn_blocking(move || apply.apply(&replay))
                .await
                .map_err(|_| StateStoreMutationError::Unavailable)??;
            let journal = self.journal.clone();
            tokio::task::spawn_blocking(move || journal.mark_applied(store_id, mutation.sequence))
                .await
                .map_err(|_| StateStoreMutationError::Unavailable)??;
            replayed += 1;
        }
        Ok(replayed)
    }

    /// Journal store ids for the restart replay sweep.
    pub async fn journaled_stores(&self) -> Result<Vec<Uuid>, StateStoreMutationError> {
        let journal = self.journal.clone();
        tokio::task::spawn_blocking(move || journal.stores())
            .await
            .map_err(|_| StateStoreMutationError::Unavailable)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{FjallIndexStore, IndexStore, PrimaryRef};
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use tempfile::TempDir;

    fn node(name: &str) -> StorageNodeId {
        StorageNodeId::try_new(name.to_owned()).unwrap()
    }

    fn assignment(store_id: Uuid) -> StateStoreAssignment {
        StateStoreAssignment::try_new(
            store_id,
            node("storage-1"),
            ReplicaSet::try_new([node("storage-1"), node("storage-2"), node("storage-3")]).unwrap(),
            1,
        )
        .unwrap()
    }

    fn mutation(store_id: Uuid, sequence: u64, key: &str) -> StateStoreMutation {
        StateStoreMutation {
            store_id,
            mutation_id: Uuid::new_v4(),
            request_id: Uuid::new_v4(),
            ownership_epoch: 1,
            sequence,
            op: StateStoreMutationOp::PutRow {
                key: key.as_bytes().to_vec(),
                payload: b"payload".to_vec(),
            },
            issued_at_ns: 1,
        }
    }

    fn evidence(
        mutation: &StateStoreMutation,
        assignment: &StateStoreAssignment,
    ) -> StateStoreCommitEvidence {
        let digest = mutation_digest(mutation).unwrap();
        StateStoreCommitEvidence {
            store_id: mutation.store_id,
            request_id: mutation.request_id,
            mutation_id: mutation.mutation_id,
            sequence: mutation.sequence,
            votes: [
                (assignment.owner.clone(), digest),
                (node("storage-2"), digest),
            ],
        }
    }

    #[test]
    fn owner_prepare_allocates_gap_free_sequence() {
        let directory = TempDir::new().unwrap();
        let journal = FjallStateStoreMutationJournal::open(directory.path()).unwrap();
        let store_id = Uuid::new_v4();
        let first = journal.prepare(mutation(store_id, 0, "a")).unwrap();
        let second = journal.prepare(mutation(store_id, 0, "b")).unwrap_err();
        assert!(matches!(second, StateStoreMutationError::Sequence));
        assert_eq!(first.mutation.sequence, 1);
    }

    #[test]
    fn prepare_commit_is_durable_and_idempotent_across_restart() {
        let directory = TempDir::new().unwrap();
        let store_id = Uuid::new_v4();
        let placement = assignment(store_id);
        {
            let journal = FjallStateStoreMutationJournal::open(directory.path()).unwrap();
            let vote = journal.prepare(mutation(store_id, 0, "row")).unwrap();
            journal
                .commit_with_quorum(store_id, 1, &evidence(&vote.mutation, &placement))
                .unwrap();
        }
        let journal = FjallStateStoreMutationJournal::open(directory.path()).unwrap();
        let inspection = journal.inspect(store_id).unwrap();
        let committed = inspection.committed.clone().unwrap();
        assert_eq!(committed.sequence, 1);
        // Idempotent re-commit of the same evidence answers with the entry.
        let retry = journal
            .commit_with_quorum(store_id, 1, &evidence(&committed, &placement))
            .unwrap();
        assert_eq!(retry.sequence, 1);
    }

    #[test]
    fn mutation_id_identity_dedupes_retries_and_conflicts_on_change() {
        let directory = TempDir::new().unwrap();
        let journal = FjallStateStoreMutationJournal::open(directory.path()).unwrap();
        let store_id = Uuid::new_v4();
        let placement = assignment(store_id);
        let vote = journal.prepare(mutation(store_id, 0, "row")).unwrap();
        journal
            .commit_with_quorum(store_id, 1, &evidence(&vote.mutation, &placement))
            .unwrap();
        // Same identity + same content is a retry.
        let retry = journal.prepare(vote.mutation.clone()).unwrap();
        assert_eq!(retry.mutation.sequence, 1);
        // Same identity + different content conflicts.
        let mut changed = vote.mutation.clone();
        changed.op = StateStoreMutationOp::PutRow {
            key: b"other".to_vec(),
            payload: b"different".to_vec(),
        };
        assert!(matches!(
            journal.prepare(changed),
            Err(StateStoreMutationError::Conflict)
        ));
    }

    #[test]
    fn applied_checkpoint_only_moves_forward() {
        let directory = TempDir::new().unwrap();
        let journal = FjallStateStoreMutationJournal::open(directory.path()).unwrap();
        let store_id = Uuid::new_v4();
        let placement = assignment(store_id);
        let vote = journal.prepare(mutation(store_id, 0, "row")).unwrap();
        journal
            .commit_with_quorum(store_id, 1, &evidence(&vote.mutation, &placement))
            .unwrap();
        journal.mark_applied(store_id, 1).unwrap();
        assert_eq!(journal.applied_seq(store_id).unwrap(), 1);
        journal.mark_applied(store_id, 1).unwrap();
        assert!(matches!(
            journal.mark_applied(store_id, 9),
            Err(StateStoreMutationError::Sequence)
        ));
        assert_eq!(journal.pending_applies(store_id).unwrap().len(), 0);
    }

    #[test]
    fn pending_applies_replay_to_the_index_store() {
        let journal_dir = TempDir::new().unwrap();
        let index_dir = TempDir::new().unwrap();
        let store_id = Uuid::new_v4();
        let placement = assignment(store_id);
        let journal = FjallStateStoreMutationJournal::open(journal_dir.path()).unwrap();
        let index: Arc<dyn IndexStore> = Arc::new(FjallIndexStore::open(index_dir.path()).unwrap());
        let apply = IndexStoreStateApply::new(index.clone());
        for key in ["a", "b"] {
            let vote = journal.prepare(mutation(store_id, 0, key)).unwrap();
            journal
                .commit_with_quorum(store_id, 1, &evidence(&vote.mutation, &placement))
                .unwrap();
        }
        // Committed but never marked applied → replay applies both.
        let pending = journal.pending_applies(store_id).unwrap();
        assert_eq!(pending.len(), 2);
        for m in pending {
            apply.apply(&m).unwrap();
            journal.mark_applied(store_id, m.sequence).unwrap();
        }
        let row = index
            .get(&PrimaryRef {
                feed_id: store_id,
                application_key: b"a".to_vec(),
            })
            .unwrap()
            .unwrap();
        assert_eq!(row.payload, b"payload");
        assert_eq!(row.applied_cursor, "00000000000000000001");
        assert_eq!(journal.applied_seq(store_id).unwrap(), 2);
    }

    struct InMemoryTransport {
        replies: Mutex<BTreeMap<String, Arc<FjallStateStoreMutationJournal>>>,
    }

    impl InMemoryTransport {
        fn new(nodes: &[(&str, Arc<FjallStateStoreMutationJournal>)]) -> Self {
            Self {
                replies: Mutex::new(
                    nodes
                        .iter()
                        .map(|(name, journal)| (name.to_string(), journal.clone()))
                        .collect(),
                ),
            }
        }
    }

    #[async_trait::async_trait]
    impl StateStoreMutationTransport for InMemoryTransport {
        async fn prepare(
            &self,
            replica: &StorageNodeId,
            request: StateStorePrepareRequest,
        ) -> Result<StateStoreReplicaReply<StateStorePrepareVote>, StateStoreMutationError>
        {
            let journal = self
                .replies
                .lock()
                .unwrap()
                .get(replica.as_str())
                .cloned()
                .ok_or(StateStoreMutationError::Unavailable)?;
            let result = journal.prepare(request.mutation.clone())?;
            Ok(StateStoreReplicaReply {
                replica: replica.clone(),
                store_id: request.mutation.store_id,
                ownership_epoch: request.mutation.ownership_epoch,
                result,
            })
        }

        async fn commit(
            &self,
            replica: &StorageNodeId,
            request: StateStoreCommitRequest,
        ) -> Result<StateStoreReplicaReply<StateStoreMutation>, StateStoreMutationError> {
            let journal = self
                .replies
                .lock()
                .unwrap()
                .get(replica.as_str())
                .cloned()
                .ok_or(StateStoreMutationError::Unavailable)?;
            let result = journal.commit_with_quorum(
                request.store_id,
                request.ownership_epoch,
                &request.evidence,
            )?;
            Ok(StateStoreReplicaReply {
                replica: replica.clone(),
                store_id: request.store_id,
                ownership_epoch: request.ownership_epoch,
                result,
            })
        }

        async fn inspect(
            &self,
            replica: &StorageNodeId,
            request: StateStoreInspectRequest,
        ) -> Result<StateStoreReplicaReply<StateStoreJournalInspection>, StateStoreMutationError>
        {
            let journal = self
                .replies
                .lock()
                .unwrap()
                .get(replica.as_str())
                .cloned()
                .ok_or(StateStoreMutationError::Unavailable)?;
            let result = journal.inspect(request.store_id)?;
            Ok(StateStoreReplicaReply {
                replica: replica.clone(),
                store_id: request.store_id,
                ownership_epoch: request.ownership_epoch,
                result,
            })
        }

        async fn lookup(
            &self,
            replica: &StorageNodeId,
            request: StateStoreLookupRequest,
        ) -> Result<StateStoreReplicaReply<Option<StateStoreMutation>>, StateStoreMutationError>
        {
            let journal = self
                .replies
                .lock()
                .unwrap()
                .get(replica.as_str())
                .cloned()
                .ok_or(StateStoreMutationError::Unavailable)?;
            let result = journal.mutation_by_id(request.store_id, request.mutation_id)?;
            Ok(StateStoreReplicaReply {
                replica: replica.clone(),
                store_id: request.store_id,
                ownership_epoch: request.ownership_epoch,
                result,
            })
        }
    }

    #[tokio::test]
    async fn coordinator_replicates_to_quorum_and_dedupes_retries() {
        let store_id = Uuid::new_v4();
        let dirs = [
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
        ];
        let journals: Vec<_> = dirs
            .iter()
            .map(|dir| Arc::new(FjallStateStoreMutationJournal::open(dir.path()).unwrap()))
            .collect();
        let placement = assignment(store_id);
        let transport = Arc::new(InMemoryTransport::new(&[
            ("storage-1", journals[0].clone()),
            ("storage-2", journals[1].clone()),
            ("storage-3", journals[2].clone()),
        ]));
        let coordinator = StateStoreMutationCoordinator::new(placement, transport);
        let result = coordinator
            .mutate(mutation(store_id, 0, "row"))
            .await
            .unwrap();
        assert_eq!(result.sequence, 1);
        assert!(!result.deduplicated);
        for journal in &journals {
            let committed = journal.inspect(store_id).unwrap().committed.unwrap();
            assert_eq!(committed.sequence, 1);
        }
        // Retrying the same mutation_id settles without re-applying.
        let mut retry = mutation(store_id, 0, "row");
        retry.mutation_id = journals[0]
            .inspect(store_id)
            .unwrap()
            .committed
            .unwrap()
            .mutation_id;
        let deduped = coordinator.mutate(retry).await.unwrap();
        assert!(deduped.deduplicated);
        assert_eq!(deduped.sequence, 1);
    }

    #[tokio::test]
    async fn coordinator_refuses_without_owner_quorum() {
        let store_id = Uuid::new_v4();
        let dirs = [TempDir::new().unwrap(), TempDir::new().unwrap()];
        let journals: Vec<_> = dirs
            .iter()
            .map(|dir| Arc::new(FjallStateStoreMutationJournal::open(dir.path()).unwrap()))
            .collect();
        let placement = assignment(store_id);
        // Owner journal missing → prepare fails as Unavailable → NoQuorum.
        let transport = Arc::new(InMemoryTransport::new(&[
            ("storage-2", journals[0].clone()),
            ("storage-3", journals[1].clone()),
        ]));
        let coordinator = StateStoreMutationCoordinator::new(placement, transport);
        let error = coordinator
            .mutate(mutation(store_id, 0, "row"))
            .await
            .unwrap_err();
        assert!(matches!(error, StateStoreMutationError::NoQuorum));
    }
}
