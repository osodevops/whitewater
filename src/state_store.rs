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

/// Strict reads wait at most this long for a replica's applied position to
/// reach the committed frontier observed at request entry.
pub const MAX_STRICT_READ_WAIT_MS: u64 = 2_000;
/// One catch-up page carries at most this many committed entries and this
/// many serialized bytes, keeping replica pulls bounded.
pub const MAX_CATCHUP_PAGE_ENTRIES: usize = 64;
pub const MAX_CATCHUP_PAGE_BYTES: usize = 8 * 1024 * 1024;
/// One transfer page carries at most this many rows and bytes.
pub const MAX_TRANSFER_PAGE_ROWS: usize = 256;
pub const MAX_TRANSFER_PAGE_BYTES: usize = 8 * 1024 * 1024;
/// Application-visible row payload budget for manual StateStore mutations.
pub const STATE_STORE_MAX_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

const HEAD_KEYSPACE: &str = "state_store_journal_head";
const ENTRIES_KEYSPACE: &str = "state_store_journal_entries";
const IDEMPOTENCY_KEYSPACE: &str = "state_store_journal_idempotency";
const DIGESTS_KEYSPACE: &str = "state_store_journal_digests";
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
    /// Create or replace one row. `named` carries the row's values for each
    /// declared named key; postings and unique claims commit in the same
    /// atomic batch as the row.
    PutRow {
        key: Vec<u8>,
        payload: Vec<u8>,
        #[serde(default)]
        named: Vec<NamedKeyValue>,
    },
    /// Remove one row and all of its named-key postings; deleting a missing
    /// row is a recorded no-op.
    DeleteRow { key: Vec<u8> },
    /// Delete every row in the store. Journaled like any other mutation:
    /// replicas apply it as bounded row-delete batches and a restart before
    /// the applied marker replays the whole clear idempotently.
    ClearAll,
    /// Atomically merge up to 64 additive accumulator rows: each entry adds
    /// `count_delta`/`sum_delta` onto its row's 24-byte accumulator payload
    /// and replaces that row's named-key postings, all inside one durable
    /// transaction on every replica. Rows already stamped with this
    /// mutation's sequence cursor are skipped, which makes replay of a
    /// committed-but-unmarked mutation a no-op rather than a double count.
    /// This is the primitive managed Pipes (rolling windows) write through;
    /// it is journaled and fenced exactly like every other mutation.
    AccumulateRows { entries: Vec<AccumulateRowEntry> },
}

/// One additive merge inside an `AccumulateRows` mutation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccumulateRowEntry {
    pub key: Vec<u8>,
    #[serde(default)]
    pub count_delta: u64,
    /// One entry's value delta (an extracted i64); the row's stored
    /// accumulator widens to i128 so sums cannot overflow.
    #[serde(default)]
    pub sum_delta: i64,
    #[serde(default)]
    pub named: Vec<NamedKeyValue>,
}

impl StateStoreMutationOp {
    fn key(&self) -> &[u8] {
        match self {
            Self::PutRow { key, .. } | Self::DeleteRow { key } => key,
            Self::AccumulateRows { entries } => entries
                .first()
                .map(|entry| entry.key.as_slice())
                .unwrap_or(&[]),
            Self::ClearAll => &[],
        }
    }
}

/// One named-key value a `PutRow` mutation writes. The mutating service
/// stamps `unique` from the store's declared `named_keys`, so the journaled
/// mutation is self-describing and replays identically on every replica.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamedKeyValue {
    pub name: String,
    pub values: Vec<crate::index::IndexValue>,
    #[serde(default)]
    pub unique: bool,
}

/// The posting/claim namespace for one StateStore named key. Derived
/// deterministically from the store id and key name so every replica writes
/// identical postings under the same IndexId; scoped per store so named
/// keys never collide across stores or with declared Feed Indexes.
pub fn named_key_index_id(store_id: Uuid, name: &str) -> crate::index::IndexId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"ww-state-store-named-key");
    hasher.update(store_id.as_bytes());
    hasher.update(name.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    crate::index::IndexId(Uuid::from_bytes(bytes))
}

/// One journaled manual StateStore mutation. `mutation_id` is the
/// client-owned idempotency identity: a retry of the same `mutation_id`
/// returns the committed outcome rather than double-applying, while the same
/// identity carrying different content is refused as a conflict.
/// `sequence` is the owner-allocated store-local total order; a request with
/// `sequence == 0` asks the assigned owner to allocate the next position
/// atomically inside its durable prepare.
/// The position a replayed mutation occupied in its source Feed. Journaled
/// on the mutation so the replay frontier survives restart on every replica.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplaySource {
    pub feed_id: Uuid,
    pub cursor: String,
}

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
    /// Set only on mutations a Feed-derived replay pass submitted.
    #[serde(default)]
    pub source: Option<ReplaySource>,
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
    /// Applied chain digest at `applied_seq` (see `StateStoreJournalHead`).
    #[serde(default)]
    pub applied_digest: Option<String>,
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

/// A named-key read: resolve `values` postings under the store's derived
/// named-key IndexId to at most `limit` primary rows.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StateStoreNamedRead {
    pub name: String,
    pub values: Vec<crate::index::IndexValue>,
    #[serde(default = "default_named_read_limit")]
    pub limit: usize,
}

/// A bounded range read over one named key's values: resolves rows whose
/// named posting falls inside `[lower, upper]` under typed `IndexValue`
/// order (absent bound = unbounded; each bound may be inclusive or
/// exclusive). Mutually exclusive with `by_named` equality and
/// primary-key point reads.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StateStoreNamedRangeRead {
    pub name: String,
    #[serde(default)]
    pub lower: Option<crate::control::IndexBound>,
    #[serde(default)]
    pub upper: Option<crate::control::IndexBound>,
    #[serde(default = "default_named_read_limit")]
    pub limit: usize,
}

fn default_named_read_limit() -> usize {
    64
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateStoreReadRequest {
    pub owner: StorageNodeId,
    pub receiver: StorageNodeId,
    pub store_id: Uuid,
    pub ownership_epoch: u64,
    /// Primary key for a point read; ignored (may be empty) when `by_named`
    /// selects a named-key resolution instead.
    pub key: Vec<u8>,
    /// Strict reads wait (bounded by `wait_ms`) for the replica's applied
    /// position to reach the committed frontier observed at entry.
    #[serde(default)]
    pub strict: bool,
    #[serde(default)]
    pub wait_ms: u64,
    #[serde(default)]
    pub by_named: Option<StateStoreNamedRead>,
    #[serde(default)]
    pub by_named_range: Option<StateStoreNamedRangeRead>,
}

/// One applied StateStore row returned by a replica read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateStoreRow {
    pub payload: Vec<u8>,
    /// The journal-sequence cursor this row last applied at.
    pub applied_cursor: String,
}

/// One row resolved by a named-key read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateStoreNamedRow {
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
    pub applied_cursor: String,
}

/// A replica's row read plus the freshness evidence to interpret it:
/// `applied_seq` behind `committed_seq` means the replica is lagging.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StateStoreReadOutcome {
    pub row: Option<StateStoreRow>,
    #[serde(default)]
    pub named_rows: Vec<StateStoreNamedRow>,
    pub applied_seq: u64,
    pub committed_seq: u64,
}

/// Bounded committed-entry page request for replica catch-up.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateStoreEntriesRequest {
    pub owner: StorageNodeId,
    pub receiver: StorageNodeId,
    pub store_id: Uuid,
    pub ownership_epoch: u64,
    pub after_seq: u64,
    pub limit: usize,
}

/// One committed journal entry paired with the serving replica's applied
/// chain digest at that sequence (`None` while the server has not applied
/// it yet — verification then waits for a later entry that carries one).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateStoreCommittedEntry {
    pub mutation: StateStoreMutation,
    #[serde(default)]
    pub applied_digest: Option<String>,
}

/// The bounded page a replica serves for catch-up.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateStoreEntriesPage {
    pub entries: Vec<StateStoreCommittedEntry>,
    pub committed_seq: u64,
    pub applied_seq: u64,
    #[serde(default)]
    pub applied_digest: Option<String>,
}

/// Bounded applied-row page request for checkpoint transfer.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateStoreTransferRequest {
    pub owner: StorageNodeId,
    pub receiver: StorageNodeId,
    pub store_id: Uuid,
    pub ownership_epoch: u64,
    #[serde(default)]
    pub after_key: Option<Vec<u8>>,
    pub limit: usize,
}

/// One applied row offered by a checkpoint transfer.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateStoreTransferRow {
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
    pub applied_seq: u64,
}

/// The bounded page a replica serves for transfer. Rows are strictly
/// ordered by key and only include mutations applied at or below
/// `pinned_seq`, so the set stays stable while the receiver pages through.
/// `applied_digest` is the sender's applied chain value at `pinned_seq`;
/// the receiver installs it and every later catch-up step re-verifies the
/// chain continued from it against peer-attested digests.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateStoreTransferPage {
    pub rows: Vec<StateStoreTransferRow>,
    pub next_key: Option<Vec<u8>>,
    pub pinned_seq: u64,
    #[serde(default)]
    pub applied_digest: Option<String>,
}

/// Result of one bounded catch-up pass on this replica.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateStoreCatchUpOutcome {
    pub store_id: Uuid,
    pub installed: u64,
    pub applied: u64,
    pub committed_seq: u64,
    pub applied_seq: u64,
    /// `true` when this replica's applied position reached the freshest
    /// observed peer's committed position.
    pub complete: bool,
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
    #[error("StateStore replica applied state diverges from peer-attested checkpoints")]
    Diverged,
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
            Self::Diverged => "state_store.diverged",
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
            "state_store.diverged" => Self::Diverged,
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
    // ClearAll carries no row key — the empty key is its identity.
    let key_bound = match &mutation.op {
        StateStoreMutationOp::ClearAll => mutation.op.key().is_empty(),
        _ => !mutation.op.key().is_empty(),
    };
    if !key_bound || mutation.op.key().len() > STATE_STORE_MAX_KEY_BYTES {
        return Err(StateStoreMutationError::TooLarge);
    }
    match &mutation.op {
        StateStoreMutationOp::PutRow { payload, .. } => {
            if payload.len() > STATE_STORE_MAX_PAYLOAD_BYTES {
                return Err(StateStoreMutationError::TooLarge);
            }
        }
        StateStoreMutationOp::AccumulateRows { entries } => {
            // One mutation spans at most one driver page of accumulator
            // rows; every row key honours the same bound as a single-row op.
            if entries.is_empty()
                || entries.len() > 64
                || entries.iter().any(|entry| {
                    entry.key.is_empty()
                        || entry.key.len() > STATE_STORE_MAX_KEY_BYTES
                        || entry.named.len() > 64
                })
            {
                return Err(StateStoreMutationError::TooLarge);
            }
        }
        _ => {}
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
    /// Hash chain over every applied committed mutation:
    /// `blake3(previous_digest | sequence | mutation_digest)` rooted at the
    /// store genesis. Replicas that applied the same committed prefix
    /// always agree on the digest at a shared sequence, which is how
    /// catch-up verifies transferred or replayed state.
    #[serde(default)]
    applied_digest: Option<String>,
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
            applied_digest: None,
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

/// Extend the applied chain: `blake3(previous | sequence | mutation_digest)`
/// hex-encoded. `previous` is the digest at `sequence - 1` or the store's
/// genesis anchor when no prior applied digest exists.
fn applied_chain_digest(
    previous: Option<&str>,
    mutation: &StateStoreMutation,
) -> Result<String, StateStoreMutationError> {
    let mutation_digest = mutation_digest(mutation)?;
    let mut hasher = blake3::Hasher::new();
    match previous {
        Some(digest) => hasher.update(digest.as_bytes()),
        None => hasher.update(b"whitewater-state-store-genesis"),
    };
    hasher.update(&mutation.sequence.to_be_bytes());
    hasher.update(mutation_digest.as_slice());
    Ok(hasher.finalize().to_hex().to_string())
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
    digests: fjall::SingleWriterTxKeyspace,
}

impl FjallStateStoreMutationJournal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StateStoreMutationError> {
        let db = fjall::SingleWriterTxDatabase::builder(path)
            .max_journaling_size(JOURNAL_REPLAY_BUDGET_BYTES)
            .open()?;
        let head = db.keyspace(HEAD_KEYSPACE, bounded_keyspace)?;
        let entries = db.keyspace(ENTRIES_KEYSPACE, bounded_keyspace)?;
        let idempotency = db.keyspace(IDEMPOTENCY_KEYSPACE, bounded_keyspace)?;
        let digests = db.keyspace(DIGESTS_KEYSPACE, bounded_keyspace)?;
        Ok(Self {
            db,
            head,
            entries,
            idempotency,
            digests,
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
            applied_digest: head.applied_digest,
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
    /// Applied never moves backwards. `digest` is the applied chain value
    /// at `sequence`, recorded per-entry and on the head so peers can
    /// verify their own application converged.
    pub fn mark_applied(
        &self,
        store_id: Uuid,
        sequence: u64,
        digest: &str,
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
        tx.insert(
            &self.digests,
            entries_key(store_id, sequence),
            digest.as_bytes(),
        );
        if head.applied_seq < sequence {
            head.applied_seq = sequence;
            head.applied_digest = Some(digest.to_owned());
            tx.insert(&self.head, store_id.as_bytes(), serde_json::to_vec(&head)?);
        }
        tx.commit()?;
        Ok(())
    }

    /// The applied chain digest recorded at one sequence, if the replica
    /// applied that far.
    pub fn entry_applied_digest(
        &self,
        store_id: Uuid,
        sequence: u64,
    ) -> Result<Option<String>, StateStoreMutationError> {
        self.db
            .read_tx()
            .get(&self.digests, entries_key(store_id, sequence))?
            .map(|bytes| String::from_utf8(bytes.to_vec()))
            .transpose()
            .map_err(|_| StateStoreMutationError::Conflict)
    }

    /// Committed entries strictly after `after`, bounded by count and
    /// serialized bytes, each paired with this replica's applied chain
    /// digest when it applied that far. Catch-up pages through this.
    pub fn entries_after(
        &self,
        store_id: Uuid,
        after: u64,
        limit: usize,
        byte_budget: usize,
    ) -> Result<Vec<StateStoreCommittedEntry>, StateStoreMutationError> {
        let Some(head) = self.read_head(store_id)? else {
            return Ok(Vec::new());
        };
        let mut entries = Vec::new();
        let mut bytes_seen = 0usize;
        for sequence in (after + 1)..=head.committed_seq {
            if entries.len() >= limit {
                break;
            }
            let Some(mutation) = self.entry(store_id, sequence)? else {
                return Err(StateStoreMutationError::Sequence);
            };
            let serialized = serde_json::to_vec(&mutation)?;
            bytes_seen = bytes_seen
                .checked_add(serialized.len())
                .ok_or(StateStoreMutationError::TooLarge)?;
            // The first entry is always included even when it alone exceeds
            // the byte budget — a page must make progress or catch-up
            // deadlocks on a single large mutation.
            if !entries.is_empty() && bytes_seen > byte_budget {
                break;
            }
            let applied_digest = self.entry_applied_digest(store_id, sequence)?;
            entries.push(StateStoreCommittedEntry {
                mutation,
                applied_digest,
            });
        }
        Ok(entries)
    }

    /// Install a peer-committed entry during catch-up. The entry was already
    /// quorum-committed at its origin, so no prepare vote or quorum
    /// re-evidence is required; the replica durably records it at its
    /// stated sequence. Existing entries must match exactly — a different
    /// mutation at the same sequence is a `Conflict`.
    pub fn install_committed(
        &self,
        mutation: &StateStoreMutation,
    ) -> Result<bool, StateStoreMutationError> {
        validate_mutation_bounds(mutation)?;
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let head_bytes = tx.get(&self.head, mutation.store_id.as_bytes())?;
        let mut head = match head_bytes {
            Some(bytes) => serde_json::from_slice::<StateStoreJournalHead>(&bytes)?,
            None => StateStoreJournalHead::empty(mutation.store_id, mutation.ownership_epoch),
        };
        if mutation.sequence <= head.committed_seq {
            match tx.get(
                &self.entries,
                entries_key(mutation.store_id, mutation.sequence),
            )? {
                Some(existing) => {
                    let existing: StateStoreMutation = serde_json::from_slice(&existing)?;
                    if existing == *mutation {
                        tx.commit()?;
                        return Ok(false);
                    }
                    return Err(StateStoreMutationError::Conflict);
                }
                // Covered by an installed checkpoint; nothing to do.
                None => {
                    tx.commit()?;
                    return Ok(false);
                }
            }
        }
        if mutation.sequence != head.committed_seq + 1 {
            return Err(StateStoreMutationError::Sequence);
        }
        tx.insert(
            &self.entries,
            entries_key(mutation.store_id, mutation.sequence),
            serde_json::to_vec(mutation)?,
        );
        tx.insert(
            &self.idempotency,
            idempotency_key(mutation.store_id, mutation.mutation_id),
            mutation.sequence.to_be_bytes(),
        );
        head.committed_seq = mutation.sequence;
        head.committed_mutation_id = Some(mutation.mutation_id);
        tx.insert(
            &self.head,
            mutation.store_id.as_bytes(),
            serde_json::to_vec(&head)?,
        );
        tx.commit()?;
        Ok(true)
    }

    /// Install a transferred checkpoint: the replica's applied position and
    /// chain digest move to the attested values and the committed floor
    /// rises to cover the transferred prefix. Applied never moves
    /// backwards; a checkpoint below the current position is a no-op.
    pub fn install_checkpoint(
        &self,
        store_id: Uuid,
        ownership_epoch: u64,
        applied_seq: u64,
        applied_digest: &str,
    ) -> Result<bool, StateStoreMutationError> {
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let head_bytes = tx.get(&self.head, store_id.as_bytes())?;
        let mut head = match head_bytes {
            Some(bytes) => serde_json::from_slice::<StateStoreJournalHead>(&bytes)?,
            None => StateStoreJournalHead::empty(store_id, ownership_epoch),
        };
        if applied_seq <= head.applied_seq {
            tx.commit()?;
            return Ok(false);
        }
        head.applied_seq = applied_seq;
        head.applied_digest = Some(applied_digest.to_owned());
        head.committed_seq = head.committed_seq.max(applied_seq);
        tx.insert(
            &self.digests,
            entries_key(store_id, applied_seq),
            applied_digest.as_bytes(),
        );
        tx.insert(&self.head, store_id.as_bytes(), serde_json::to_vec(&head)?);
        tx.commit()?;
        Ok(true)
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
            // A retried request arrives with a fresh request_id, a new
            // issued_at_ns and an unallocated sequence (0); it is the same
            // mutation when the committed-significant fields match. Reused
            // identity carrying different content is a true conflict.
            let same = committed.store_id == mutation.store_id
                && committed.ownership_epoch == mutation.ownership_epoch
                && committed.op == mutation.op
                && (mutation.sequence == 0 || mutation.sequence == committed.sequence);
            return if same {
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

    /// Read the applied row for one primary key, if present.
    fn get_row(
        &self,
        store_id: Uuid,
        key: &[u8],
    ) -> Result<Option<StateStoreRow>, StateStoreMutationError>;

    /// Ordered applied-row scan for checkpoint transfer, starting strictly
    /// after `after_key` and bounded by `limit`.
    fn scan_rows(
        &self,
        store_id: Uuid,
        after_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<StateStoreTransferRow>, StateStoreMutationError>;

    /// Install one transferred row at its attested applied position. The
    /// write goes through the same durable apply path as journal replay —
    /// cursor-conflict rules still fence divergent content.
    fn install_row(
        &self,
        store_id: Uuid,
        row: &StateStoreTransferRow,
    ) -> Result<(), StateStoreMutationError>;

    /// The backing Index engine when this apply seam is index-backed —
    /// the shadow-generation Index builder writes alongside StateStore
    /// rows, scoped by disjoint ids. In-memory test applies report None.
    fn index_store(&self) -> Option<Arc<dyn crate::index::IndexStore>> {
        None
    }
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

    /// The shared Index engine hosting this Node's StateStore rows and
    /// declared-Index postings.
    pub fn store(&self) -> Arc<dyn crate::index::IndexStore> {
        self.store.clone()
    }

    fn cursor(sequence: u64) -> String {
        format!("{sequence:020}")
    }

    /// Bounded row-delete sweep for `ClearAll`: each pass removes up to 512
    /// rows plus their postings under the mutation's journal cursor, so the
    /// clear is journaled once and applied in small transactions rather than
    /// one unbounded delete.
    fn clear_store(&self, mutation: &StateStoreMutation) -> Result<(), StateStoreMutationError> {
        const CLEAR_PAGE: usize = 512;
        loop {
            let rows = self
                .store
                .scan(mutation.store_id, None, CLEAR_PAGE)
                .map_err(|_| StateStoreMutationError::ApplyFailed)?;
            if rows.is_empty() {
                return Ok(());
            }
            for row in rows {
                self.store
                    .delete(&row.reference, &Self::cursor(mutation.sequence))
                    .map_err(|_| StateStoreMutationError::ApplyFailed)?;
            }
        }
    }
}

impl StateStoreApply for IndexStoreStateApply {
    fn index_store(&self) -> Option<Arc<dyn crate::index::IndexStore>> {
        Some(self.store.clone())
    }

    fn apply(&self, mutation: &StateStoreMutation) -> Result<(), StateStoreMutationError> {
        if matches!(mutation.op, StateStoreMutationOp::ClearAll) {
            return self.clear_store(mutation);
        }
        let reference = crate::index::PrimaryRef {
            feed_id: mutation.store_id,
            application_key: mutation.op.key().to_vec(),
        };
        match &mutation.op {
            StateStoreMutationOp::PutRow { payload, named, .. } => {
                let entries: Vec<crate::index::IndexEntry> = named
                    .iter()
                    .map(|entry| crate::index::IndexEntry {
                        id: named_key_index_id(mutation.store_id, &entry.name),
                        values: entry.values.clone(),
                        unique: entry.unique,
                    })
                    .collect();
                match self.store.upsert(
                    &reference,
                    payload,
                    &entries,
                    &Self::cursor(mutation.sequence),
                ) {
                    Ok(()) => {}
                    // Unique named-key claims arbitrate by journal order:
                    // first-committed-wins, so a conflicting put is a journaled
                    // no-op on every replica — identical resolution everywhere
                    // because every replica shares the same applied prefix.
                    Err(crate::index::IndexStoreError::Key(
                        crate::index::IndexKeyError::UniqueConflict,
                    )) => {}
                    Err(_) => return Err(StateStoreMutationError::ApplyFailed),
                }
            }
            StateStoreMutationOp::DeleteRow { .. } => {
                self.store
                    .delete(&reference, &Self::cursor(mutation.sequence))
                    .map_err(|_| StateStoreMutationError::ApplyFailed)?;
            }
            StateStoreMutationOp::AccumulateRows { entries } => {
                let rows: Vec<crate::index::AccumulateRowWrite> = entries
                    .iter()
                    .map(|entry| crate::index::AccumulateRowWrite {
                        reference: crate::index::PrimaryRef {
                            feed_id: mutation.store_id,
                            application_key: entry.key.clone(),
                        },
                        count_delta: entry.count_delta,
                        sum_delta: entry.sum_delta,
                        entries: entry
                            .named
                            .iter()
                            .map(|named| crate::index::IndexEntry {
                                id: named_key_index_id(mutation.store_id, &named.name),
                                values: named.values.clone(),
                                unique: named.unique,
                            })
                            .collect(),
                    })
                    .collect();
                self.store
                    .accumulate_rows(&rows, &Self::cursor(mutation.sequence))
                    .map_err(|_| StateStoreMutationError::ApplyFailed)?;
            }
            StateStoreMutationOp::ClearAll => unreachable!("handled above"),
        }
        Ok(())
    }

    fn get_row(
        &self,
        store_id: Uuid,
        key: &[u8],
    ) -> Result<Option<StateStoreRow>, StateStoreMutationError> {
        self.store
            .get(&crate::index::PrimaryRef {
                feed_id: store_id,
                application_key: key.to_vec(),
            })
            .map(|row| {
                row.map(|row| StateStoreRow {
                    payload: row.payload,
                    applied_cursor: row.applied_cursor,
                })
            })
            .map_err(|_| StateStoreMutationError::ApplyFailed)
    }

    fn scan_rows(
        &self,
        store_id: Uuid,
        after_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<StateStoreTransferRow>, StateStoreMutationError> {
        self.store
            .scan(store_id, after_key, limit)
            .map(|rows| {
                rows.into_iter()
                    .map(|row| {
                        Ok(StateStoreTransferRow {
                            key: row.reference.application_key,
                            payload: row.payload,
                            applied_seq: row
                                .applied_cursor
                                .parse::<u64>()
                                .map_err(|_| StateStoreMutationError::ApplyFailed)?,
                        })
                    })
                    .collect::<Result<_, StateStoreMutationError>>()
            })
            .map_err(|_| StateStoreMutationError::ApplyFailed)?
    }

    fn install_row(
        &self,
        store_id: Uuid,
        row: &StateStoreTransferRow,
    ) -> Result<(), StateStoreMutationError> {
        self.store
            .upsert(
                &crate::index::PrimaryRef {
                    feed_id: store_id,
                    application_key: row.key.clone(),
                },
                &row.payload,
                &[],
                &Self::cursor(row.applied_seq),
            )
            .map_err(|_| StateStoreMutationError::ApplyFailed)
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

    /// Read one applied row plus freshness evidence from a replica.
    async fn read(
        &self,
        replica: &StorageNodeId,
        request: StateStoreReadRequest,
    ) -> Result<StateStoreReplicaReply<StateStoreReadOutcome>, StateStoreMutationError>;

    /// Bounded committed-entry page for replica catch-up.
    async fn entries(
        &self,
        replica: &StorageNodeId,
        request: StateStoreEntriesRequest,
    ) -> Result<StateStoreReplicaReply<StateStoreEntriesPage>, StateStoreMutationError>;

    /// Bounded applied-row page for checkpoint transfer.
    async fn transfer(
        &self,
        replica: &StorageNodeId,
        request: StateStoreTransferRequest,
    ) -> Result<StateStoreReplicaReply<StateStoreTransferPage>, StateStoreMutationError>;
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

    /// The owner's committed head mutation. For Feed-derived stores its
    /// source cursor is the durable replay frontier.
    pub async fn committed_head(
        &self,
    ) -> Result<StateStoreJournalInspection, StateStoreMutationError> {
        let reply = self
            .transport
            .inspect(
                &self.assignment.owner,
                StateStoreInspectRequest {
                    owner: self.assignment.owner.clone(),
                    receiver: self.assignment.owner.clone(),
                    store_id: self.assignment.store_id,
                    ownership_epoch: self.assignment.ownership_epoch,
                },
            )
            .await?;
        if reply.replica != self.assignment.owner
            || reply.store_id != self.assignment.store_id
            || reply.ownership_epoch != self.assignment.ownership_epoch
        {
            return Err(StateStoreMutationError::InvalidAssignment);
        }
        Ok(reply.result)
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

    async fn read(
        &self,
        replica: &StorageNodeId,
        request: StateStoreReadRequest,
    ) -> Result<StateStoreReplicaReply<StateStoreReadOutcome>, StateStoreMutationError> {
        if request.ownership_epoch != self.assignment.ownership_epoch {
            return Err(StateStoreMutationError::StaleEpoch);
        }
        self.send(replica, "/internal/state-store-journal/read", &request)
            .await
    }

    async fn entries(
        &self,
        replica: &StorageNodeId,
        request: StateStoreEntriesRequest,
    ) -> Result<StateStoreReplicaReply<StateStoreEntriesPage>, StateStoreMutationError> {
        if request.ownership_epoch != self.assignment.ownership_epoch {
            return Err(StateStoreMutationError::StaleEpoch);
        }
        self.send(replica, "/internal/state-store-journal/entries", &request)
            .await
    }

    async fn transfer(
        &self,
        replica: &StorageNodeId,
        request: StateStoreTransferRequest,
    ) -> Result<StateStoreReplicaReply<StateStoreTransferPage>, StateStoreMutationError> {
        if request.ownership_epoch != self.assignment.ownership_epoch {
            return Err(StateStoreMutationError::StaleEpoch);
        }
        self.send(replica, "/internal/state-store-journal/transfer", &request)
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

    /// The Index engine backing this Node's StateStore applies, when the
    /// apply seam is index-backed — shared with the shadow-generation
    /// Index builder under disjoint ids.
    pub fn index_store(&self) -> Option<Arc<dyn crate::index::IndexStore>> {
        self.apply.index_store()
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
        self.apply_and_mark(&committed).await?;
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

    /// One replica's applied row plus freshness evidence. Placement and
    /// ownership fencing match the journal mutation paths.
    pub async fn read_row(
        &self,
        request: StateStoreReadRequest,
    ) -> Result<StateStoreReplicaReply<StateStoreReadOutcome>, StateStoreMutationError> {
        let assignment = self
            .check_placement(
                request.store_id,
                &request.owner,
                &request.receiver,
                request.ownership_epoch,
            )
            .await?;
        if request.by_named.is_some() && request.by_named_range.is_some() {
            return Err(StateStoreMutationError::Conflict);
        }
        if request.by_named.is_none()
            && request.by_named_range.is_none()
            && (request.key.is_empty() || request.key.len() > STATE_STORE_MAX_KEY_BYTES)
        {
            return Err(StateStoreMutationError::TooLarge);
        }
        if let Some(named) = &request.by_named {
            if named.values.is_empty() || named.limit == 0 || named.limit > 1_000 {
                return Err(StateStoreMutationError::TooLarge);
            }
        }
        if let Some(range) = &request.by_named_range {
            if range.limit == 0 || range.limit > 1_000 {
                return Err(StateStoreMutationError::TooLarge);
            }
            if let (Some(lower), Some(upper)) = (&range.lower, &range.upper) {
                let order = crate::index::cmp_index_value(&lower.value, &upper.value);
                if order == std::cmp::Ordering::Greater
                    || (order == std::cmp::Ordering::Equal && !(lower.inclusive && upper.inclusive))
                {
                    return Err(StateStoreMutationError::Conflict);
                }
            }
        }
        let journal = self.journal.clone();
        let apply = self.apply.clone();
        let store_id = request.store_id;
        let key = request.key;
        let strict = request.strict;
        let wait = std::time::Duration::from_millis(request.wait_ms.min(MAX_STRICT_READ_WAIT_MS));
        let by_named = request.by_named.clone();
        let by_named_range = request.by_named_range.clone();
        let (row, named_rows, applied_seq, committed_seq) =
            tokio::task::spawn_blocking(move || {
                // Strict freshness pins to the commit frontier observed at
                // entry; chasing a moving target under write load could wait
                // forever.
                let mut inspection = journal.inspect(store_id)?;
                let target = inspection
                    .committed
                    .as_ref()
                    .map(|mutation| mutation.sequence)
                    .unwrap_or(0);
                if strict {
                    let deadline = std::time::Instant::now() + wait;
                    while inspection.applied_seq < target && std::time::Instant::now() < deadline {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                        inspection = journal.inspect(store_id)?;
                    }
                }
                let (row, named_rows) = match (by_named, by_named_range) {
                    (Some(named), _) => {
                        let index = apply
                            .index_store()
                            .ok_or(StateStoreMutationError::ApplyFailed)?;
                        let rows = index
                            .lookup_exact(
                                named_key_index_id(store_id, &named.name),
                                &named.values,
                                named.limit,
                            )
                            .map_err(|_| StateStoreMutationError::ApplyFailed)?
                            .into_iter()
                            .map(|row| StateStoreNamedRow {
                                key: row.reference.application_key,
                                payload: row.payload,
                                applied_cursor: row.applied_cursor,
                            })
                            .collect();
                        (None, rows)
                    }
                    (None, Some(range)) => {
                        let index = apply
                            .index_store()
                            .ok_or(StateStoreMutationError::ApplyFailed)?;
                        use std::ops::Bound;
                        let bound = |value: Option<crate::control::IndexBound>| {
                            value.map_or(Bound::Unbounded, |bound| {
                                if bound.inclusive {
                                    Bound::Included(bound.value)
                                } else {
                                    Bound::Excluded(bound.value)
                                }
                            })
                        };
                        let rows = index
                            .lookup_range(
                                named_key_index_id(store_id, &range.name),
                                &[],
                                bound(range.lower.clone()),
                                bound(range.upper.clone()),
                                range.limit,
                            )
                            .map_err(|_| StateStoreMutationError::ApplyFailed)?
                            .into_iter()
                            .map(|row| StateStoreNamedRow {
                                key: row.reference.application_key,
                                payload: row.payload,
                                applied_cursor: row.applied_cursor,
                            })
                            .collect();
                        (None, rows)
                    }
                    (None, None) => (apply.get_row(store_id, &key)?, Vec::new()),
                };
                Ok::<_, StateStoreMutationError>((
                    row,
                    named_rows,
                    inspection.applied_seq,
                    inspection
                        .committed
                        .as_ref()
                        .map(|mutation| mutation.sequence)
                        .unwrap_or(0),
                ))
            })
            .await
            .map_err(|_| StateStoreMutationError::Unavailable)??;
        Ok(StateStoreReplicaReply {
            replica: self.local_node.clone(),
            store_id,
            ownership_epoch: assignment.ownership_epoch,
            result: StateStoreReadOutcome {
                row,
                named_rows,
                applied_seq,
                committed_seq,
            },
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
            self.apply_and_mark(&mutation).await?;
            replayed += 1;
        }
        Ok(replayed)
    }

    /// Apply one committed mutation to the local IndexStore, then durably
    /// record the applied chain digest: `blake3(previous | sequence |
    /// mutation_digest)`. Replicas applying the same committed prefix
    /// converge on identical digests, which is what catch-up verifies.
    async fn apply_and_mark(
        &self,
        mutation: &StateStoreMutation,
    ) -> Result<(), StateStoreMutationError> {
        let apply = self.apply.clone();
        let apply_mutation = mutation.clone();
        tokio::task::spawn_blocking(move || apply.apply(&apply_mutation))
            .await
            .map_err(|_| StateStoreMutationError::Unavailable)??;
        let journal = self.journal.clone();
        let digest_mutation = mutation.clone();
        let store_id = mutation.store_id;
        let sequence = mutation.sequence;
        tokio::task::spawn_blocking(move || {
            let previous = sequence
                .checked_sub(1)
                .filter(|prior| *prior > 0)
                .map(|prior| journal.entry_applied_digest(store_id, prior))
                .transpose()?
                .flatten();
            let digest = applied_chain_digest(previous.as_deref(), &digest_mutation)?;
            journal.mark_applied(store_id, sequence, &digest)
        })
        .await
        .map_err(|_| StateStoreMutationError::Unavailable)??;
        Ok(())
    }

    /// Bounded committed-entry page for a catching-up peer. Fenced by the
    /// same placement/epoch rules as mutation RPCs.
    pub async fn entries(
        &self,
        request: StateStoreEntriesRequest,
    ) -> Result<StateStoreReplicaReply<StateStoreEntriesPage>, StateStoreMutationError> {
        let assignment = self
            .check_placement(
                request.store_id,
                &request.owner,
                &request.receiver,
                request.ownership_epoch,
            )
            .await?;
        if request.limit == 0 || request.limit > MAX_CATCHUP_PAGE_ENTRIES {
            return Err(StateStoreMutationError::TooLarge);
        }
        let journal = self.journal.clone();
        let store_id = request.store_id;
        let after = request.after_seq;
        let limit = request.limit;
        let page = tokio::task::spawn_blocking(move || {
            let entries = journal.entries_after(store_id, after, limit, MAX_CATCHUP_PAGE_BYTES)?;
            let inspection = journal.inspect(store_id)?;
            Ok::<_, StateStoreMutationError>(StateStoreEntriesPage {
                entries,
                committed_seq: inspection
                    .committed
                    .as_ref()
                    .map(|mutation| mutation.sequence)
                    .unwrap_or(0),
                applied_seq: inspection.applied_seq,
                applied_digest: inspection.applied_digest,
            })
        })
        .await
        .map_err(|_| StateStoreMutationError::Unavailable)??;
        Ok(StateStoreReplicaReply {
            replica: self.local_node.clone(),
            store_id: request.store_id,
            ownership_epoch: assignment.ownership_epoch,
            result: page,
        })
    }

    /// Bounded applied-row page for checkpoint transfer. Rows carry the
    /// sequence they applied at; only rows at or below the pinned applied
    /// position are offered, so the transferred set is stable while the
    /// receiver pages through.
    pub async fn transfer(
        &self,
        request: StateStoreTransferRequest,
    ) -> Result<StateStoreReplicaReply<StateStoreTransferPage>, StateStoreMutationError> {
        let assignment = self
            .check_placement(
                request.store_id,
                &request.owner,
                &request.receiver,
                request.ownership_epoch,
            )
            .await?;
        if request.limit == 0 || request.limit > MAX_TRANSFER_PAGE_ROWS {
            return Err(StateStoreMutationError::TooLarge);
        }
        let journal = self.journal.clone();
        let apply = self.apply.clone();
        let store_id = request.store_id;
        let after_key = request.after_key.clone();
        let limit = request.limit;
        let page = tokio::task::spawn_blocking(move || {
            let inspection = journal.inspect(store_id)?;
            let pinned = inspection.applied_seq;
            let mut rows = Vec::new();
            let mut bytes_seen = 0usize;
            let mut cursor: Option<Vec<u8>> = after_key;
            loop {
                let scanned = apply.scan_rows(store_id, cursor.as_deref(), limit)?;
                let fetched = scanned.len();
                for row in scanned {
                    if row.applied_seq > pinned {
                        continue;
                    }
                    bytes_seen = bytes_seen
                        .checked_add(row.key.len() + row.payload.len())
                        .ok_or(StateStoreMutationError::TooLarge)?;
                    if bytes_seen > MAX_TRANSFER_PAGE_BYTES || rows.len() >= limit {
                        break;
                    }
                    rows.push(row);
                }
                if fetched < limit || rows.len() >= limit || bytes_seen > MAX_TRANSFER_PAGE_BYTES {
                    break;
                }
                cursor = rows.last().map(|row| row.key.clone());
                if cursor.is_none() {
                    break;
                }
            }
            let next_key = (rows.len() >= limit)
                .then(|| rows.last().map(|row| row.key.clone()))
                .flatten();
            Ok::<_, StateStoreMutationError>(StateStoreTransferPage {
                rows,
                next_key,
                pinned_seq: pinned,
                applied_digest: inspection.applied_digest,
            })
        })
        .await
        .map_err(|_| StateStoreMutationError::Unavailable)??;
        Ok(StateStoreReplicaReply {
            replica: self.local_node.clone(),
            store_id: request.store_id,
            ownership_epoch: assignment.ownership_epoch,
            result: page,
        })
    }

    /// One bounded catch-up pass for a store this Node replicates. The
    /// replica compares its committed position against the freshest peer,
    /// installs missing committed entries (each already quorum-committed
    /// at its origin), then applies pending entries while verifying the
    /// applied chain digest wherever the source attested one.
    ///
    /// `budget` bounds how many entries install+apply in one pass; the
    /// driver ticks again until `complete`.
    pub async fn catch_up_replica(
        &self,
        transport: &Arc<dyn StateStoreMutationTransport>,
        assignment: &StateStoreAssignment,
        budget: usize,
    ) -> Result<StateStoreCatchUpOutcome, StateStoreMutationError> {
        if !assignment.replicas.contains(&self.local_node) {
            return Err(StateStoreMutationError::InvalidAssignment);
        }
        let store_id = assignment.store_id;
        let journal = self.journal.clone();
        let local = tokio::task::spawn_blocking(move || journal.inspect(store_id))
            .await
            .map_err(|_| StateStoreMutationError::Unavailable)??;
        let local_committed = local
            .committed
            .as_ref()
            .map(|mutation| mutation.sequence)
            .unwrap_or(0)
            .max(local.applied_seq);

        // Freshest peer first: owner, then other replicas.
        let mut sources: Vec<&StorageNodeId> = Vec::new();
        if assignment.owner != self.local_node {
            sources.push(&assignment.owner);
        }
        for replica in assignment.replicas.iter() {
            if *replica != self.local_node && !sources.contains(&replica) {
                sources.push(replica);
            }
        }
        let mut source: Option<(StorageNodeId, u64)> = None;
        for candidate in sources {
            match transport
                .inspect(
                    candidate,
                    StateStoreInspectRequest {
                        owner: assignment.owner.clone(),
                        receiver: candidate.clone(),
                        store_id,
                        ownership_epoch: assignment.ownership_epoch,
                    },
                )
                .await
            {
                Ok(reply) => {
                    let committed = reply
                        .result
                        .committed
                        .as_ref()
                        .map(|mutation| mutation.sequence)
                        .unwrap_or(0);
                    if source
                        .as_ref()
                        .map(|(_, seq)| committed > *seq)
                        .unwrap_or(true)
                    {
                        source = Some((candidate.clone(), committed));
                    }
                }
                Err(error) if lagging_witness(&error) => {}
                Err(error) => return Err(error),
            }
        }
        let Some((source, source_committed)) = source else {
            return Err(StateStoreMutationError::NoQuorum);
        };

        let mut installed = 0u64;
        let mut attested: std::collections::BTreeMap<u64, String> =
            std::collections::BTreeMap::new();
        let mut cursor = local_committed;
        while cursor < source_committed && installed < budget as u64 {
            let reply = transport
                .entries(
                    &source,
                    StateStoreEntriesRequest {
                        owner: assignment.owner.clone(),
                        receiver: source.clone(),
                        store_id,
                        ownership_epoch: assignment.ownership_epoch,
                        after_seq: cursor,
                        limit: (budget as u64 - installed).min(MAX_CATCHUP_PAGE_ENTRIES as u64)
                            as usize,
                    },
                )
                .await?;
            if reply.result.entries.is_empty() {
                break;
            }
            for entry in reply.result.entries {
                if let Some(digest) = entry.applied_digest.clone() {
                    attested.insert(entry.mutation.sequence, digest);
                }
                let journal = self.journal.clone();
                let mutation = entry.mutation;
                cursor = mutation.sequence;
                let installed_now =
                    tokio::task::spawn_blocking(move || journal.install_committed(&mutation))
                        .await
                        .map_err(|_| StateStoreMutationError::Unavailable)??;
                if installed_now {
                    installed += 1;
                }
            }
        }

        // Apply committed-but-unapplied entries, verifying the chain digest
        // against peer-attested values where the source had applied.
        let journal = self.journal.clone();
        let pending = tokio::task::spawn_blocking(move || journal.pending_applies(store_id))
            .await
            .map_err(|_| StateStoreMutationError::Unavailable)??;
        let mut applied = 0u64;
        for mutation in pending.into_iter().take(budget) {
            self.apply_and_mark(&mutation).await?;
            applied += 1;
            if let Some(attested_digest) = attested.get(&mutation.sequence) {
                let journal = self.journal.clone();
                let store_id = mutation.store_id;
                let sequence = mutation.sequence;
                let ours = tokio::task::spawn_blocking(move || {
                    journal.entry_applied_digest(store_id, sequence)
                })
                .await
                .map_err(|_| StateStoreMutationError::Unavailable)??;
                if ours.as_deref() != Some(attested_digest.as_str()) {
                    return Err(StateStoreMutationError::Diverged);
                }
            }
        }
        let journal = self.journal.clone();
        let inspection = tokio::task::spawn_blocking(move || journal.inspect(store_id))
            .await
            .map_err(|_| StateStoreMutationError::Unavailable)??;
        Ok(StateStoreCatchUpOutcome {
            store_id,
            installed,
            applied,
            committed_seq: inspection
                .committed
                .as_ref()
                .map(|mutation| mutation.sequence)
                .unwrap_or(0),
            applied_seq: inspection.applied_seq,
            complete: inspection.applied_seq >= source_committed,
        })
    }

    /// Journal store ids for the restart replay sweep.
    pub async fn journaled_stores(&self) -> Result<Vec<Uuid>, StateStoreMutationError> {
        let journal = self.journal.clone();
        tokio::task::spawn_blocking(move || journal.stores())
            .await
            .map_err(|_| StateStoreMutationError::Unavailable)?
    }
}

/// One committed source-Feed record offered to StateStore replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplaySourceRecord {
    pub message_id: Uuid,
    pub cursor: String,
    pub key: Vec<u8>,
    pub metadata: std::collections::BTreeMap<String, Vec<u8>>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum StateStoreReplayError {
    #[error("the declared source Feed no longer resolves; its history is gone")]
    SourceUnavailable,
    #[error(
        "the replayed frontier no longer resolves in retained history; rebuild the StateStore"
    )]
    FrontierOutOfRange,
    #[error("the source Feed could not be read: {0}")]
    SourceRead(String),
    #[error("StateStore has no Feed source to replay")]
    ManualSource,
    #[error(transparent)]
    Mutation(#[from] StateStoreMutationError),
}

/// Bounded committed reads from a replay source Feed. The API layer
/// implements this over the same bounded Feed read path Readers use.
#[async_trait::async_trait]
pub trait FeedReplaySource: Send + Sync {
    /// Whether the declared source Feed still resolves to committed
    /// history; replay refuses rather than silently materialize a store
    /// whose source history is gone.
    async fn source_exists(&self, feed_id: Uuid) -> bool;

    /// Committed records strictly after `cursor` (None = the beginning),
    /// bounded by `limit`. [`StateStoreReplayError::FrontierOutOfRange`]
    /// when `cursor` no longer resolves in retained history.
    async fn read_committed_records(
        &self,
        feed_id: Uuid,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ReplaySourceRecord>, StateStoreReplayError>;
}

/// The outcome of one bounded replay pass.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StateStoreReplayOutcome {
    pub store_id: Uuid,
    pub applied: u64,
    pub frontier: Option<String>,
    /// True when the source Feed returned fewer records than the pass
    /// budget, meaning replay has reached the committed tip.
    pub complete: bool,
}

/// Deterministic idempotency identity for one replayed source record.
/// Resuming a pass re-derives the same identity so re-reads dedupe; a
/// rebuild pass namespaces identities by its token so its mutations commit
/// as new sequence positions and re-apply their rows.
pub fn replay_mutation_id(
    store_id: Uuid,
    feed_id: Uuid,
    message_id: Uuid,
    rebuild: Option<Uuid>,
) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"whitewater-state-store-replay-v1");
    hasher.update(store_id.as_bytes());
    hasher.update(feed_id.as_bytes());
    hasher.update(message_id.as_bytes());
    if let Some(token) = rebuild {
        hasher.update(token.as_bytes());
    }
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

fn unix_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .try_into()
        .unwrap_or(i64::MAX)
}

/// Builds a quorum coordinator bound to one assignment's replicas over
/// the caller's transport wiring.
pub type StateStoreCoordinatorFactory = Arc<
    dyn Fn(&StateStoreAssignment) -> Result<StateStoreMutationCoordinator, StateStoreMutationError>
        + Send
        + Sync,
>;

/// Drives a Feed-derived StateStore's replay against committed source
/// history. Each committed record becomes one journaled quorum mutation
/// carrying its source cursor, so retries, restarts, and rebuild passes all
/// converge through mutation-identity deduplication.
pub struct StateStoreReplayDriver {
    control: Arc<crate::control::ControlController>,
    source: Arc<dyn FeedReplaySource>,
    coordinator_for: StateStoreCoordinatorFactory,
}

impl StateStoreReplayDriver {
    pub fn new(
        control: Arc<crate::control::ControlController>,
        source: Arc<dyn FeedReplaySource>,
        coordinator_for: StateStoreCoordinatorFactory,
    ) -> Self {
        Self {
            control,
            source,
            coordinator_for,
        }
    }

    /// The journaled replay frontier: the source cursor carried by the
    /// newest committed mutation on the assigned owner.
    pub async fn frontier(
        &self,
        store_id: Uuid,
    ) -> Result<Option<String>, StateStoreMutationError> {
        let assignment = self
            .control
            .active_state_store_assignment_by_id(store_id)
            .await
            .ok_or(StateStoreMutationError::InvalidAssignment)?;
        let coordinator = (self.coordinator_for)(&assignment)?;
        Ok(coordinator
            .committed_head()
            .await?
            .committed
            .and_then(|mutation| mutation.source.map(|source| source.cursor)))
    }

    /// One bounded replay pass over the source Feed. With `rebuild`, the
    /// pass starts at the beginning and namespaces mutation identities by
    /// its token so every retained record re-commits and re-applies.
    pub async fn pass(
        &self,
        store: &crate::control::StateStoreDefinition,
        rebuild: Option<Uuid>,
        limit: usize,
    ) -> Result<StateStoreReplayOutcome, StateStoreReplayError> {
        let crate::control::StateStoreSource::Feed { feed_id } = store.source else {
            return Err(StateStoreReplayError::ManualSource);
        };
        if !self.source.source_exists(feed_id).await {
            return Err(StateStoreReplayError::SourceUnavailable);
        }
        let assignment = self
            .control
            .active_state_store_assignment_by_id(store.store_id)
            .await
            .ok_or(StateStoreMutationError::InvalidAssignment)?;
        let coordinator = (self.coordinator_for)(&assignment)?;
        let limit = limit.max(1);
        let after = match rebuild {
            Some(_) => None,
            None => self.frontier(store.store_id).await?,
        };
        let records = self
            .source
            .read_committed_records(feed_id, after.as_deref(), limit)
            .await?;
        let complete = records.len() < limit;
        let mut applied = 0_u64;
        let mut frontier = after;
        for record in records {
            // Declared projections turn the committed source record into
            // typed named-key values; extraction failures resolve to a Null
            // posting (the same absent-value position a missing JSON field
            // or Metadata entry takes) so one malformed record cannot wedge
            // the replay frontier.
            let named: Vec<NamedKeyValue> = store
                .named_keys
                .iter()
                .map(|key| NamedKeyValue {
                    name: key.name.clone(),
                    unique: key.unique,
                    values: key
                        .extractor
                        .as_ref()
                        .map(|spec| {
                            crate::index_build::extract_index_values(
                                std::slice::from_ref(spec),
                                &record,
                            )
                            .unwrap_or_else(|_| vec![crate::index::IndexValue::Null])
                        })
                        .unwrap_or_default(),
                })
                .collect();
            let mutation = StateStoreMutation {
                store_id: store.store_id,
                mutation_id: replay_mutation_id(
                    store.store_id,
                    feed_id,
                    record.message_id,
                    rebuild,
                ),
                request_id: Uuid::new_v4(),
                ownership_epoch: assignment.ownership_epoch,
                sequence: 0,
                op: StateStoreMutationOp::PutRow {
                    key: record.key.clone(),
                    payload: record.payload.clone(),
                    named,
                },
                issued_at_ns: unix_ns(),
                source: Some(ReplaySource {
                    feed_id,
                    cursor: record.cursor.clone(),
                }),
            };
            coordinator.mutate(mutation).await?;
            applied += 1;
            frontier = Some(record.cursor);
        }
        Ok(StateStoreReplayOutcome {
            store_id: store.store_id,
            applied,
            frontier,
            complete,
        })
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
                named: Vec::new(),
            },
            issued_at_ns: 1,
            source: None,
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
            named: Vec::new(),
        };
        assert!(matches!(
            journal.prepare(changed),
            Err(StateStoreMutationError::Conflict)
        ));
    }

    #[test]
    fn committed_mutation_dedupes_retry_with_fresh_request_envelope() {
        // A client retry after an ambiguous commit reuses only the
        // mutation_id: the server mints a new request_id and the incoming
        // mutation carries an unallocated sequence. The journal must still
        // recognise the committed entry instead of reporting a conflict.
        let directory = TempDir::new().unwrap();
        let journal = FjallStateStoreMutationJournal::open(directory.path()).unwrap();
        let store_id = Uuid::new_v4();
        let placement = assignment(store_id);
        let vote = journal.prepare(mutation(store_id, 0, "row")).unwrap();
        journal
            .commit_with_quorum(store_id, 1, &evidence(&vote.mutation, &placement))
            .unwrap();
        let mut retry = mutation(store_id, 0, "row");
        retry.mutation_id = vote.mutation.mutation_id;
        let revote = journal.prepare(retry).unwrap();
        assert_eq!(revote.mutation, vote.mutation);
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
        let digest = applied_chain_digest(None, &vote.mutation).unwrap();
        journal.mark_applied(store_id, 1, &digest).unwrap();
        assert_eq!(journal.applied_seq(store_id).unwrap(), 1);
        journal.mark_applied(store_id, 1, &digest).unwrap();
        assert!(matches!(
            journal.mark_applied(store_id, 9, &digest),
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
        let mut previous: Option<String> = None;
        for m in pending {
            apply.apply(&m).unwrap();
            let digest = applied_chain_digest(previous.as_deref(), &m).unwrap();
            journal.mark_applied(store_id, m.sequence, &digest).unwrap();
            previous = Some(digest);
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

        async fn read(
            &self,
            replica: &StorageNodeId,
            request: StateStoreReadRequest,
        ) -> Result<StateStoreReplicaReply<StateStoreReadOutcome>, StateStoreMutationError>
        {
            let journal = self
                .replies
                .lock()
                .unwrap()
                .get(replica.as_str())
                .cloned()
                .ok_or(StateStoreMutationError::Unavailable)?;
            // In-memory transport has no applied index; report journal
            // freshness so coordinator-level tests can assert on it.
            let inspection = journal.inspect(request.store_id)?;
            Ok(StateStoreReplicaReply {
                replica: replica.clone(),
                store_id: request.store_id,
                ownership_epoch: request.ownership_epoch,
                result: StateStoreReadOutcome {
                    row: None,
                    named_rows: Vec::new(),
                    applied_seq: inspection.applied_seq,
                    committed_seq: inspection
                        .committed
                        .map(|mutation| mutation.sequence)
                        .unwrap_or(0),
                },
            })
        }

        async fn entries(
            &self,
            replica: &StorageNodeId,
            request: StateStoreEntriesRequest,
        ) -> Result<StateStoreReplicaReply<StateStoreEntriesPage>, StateStoreMutationError>
        {
            let journal = self
                .replies
                .lock()
                .unwrap()
                .get(replica.as_str())
                .cloned()
                .ok_or(StateStoreMutationError::Unavailable)?;
            let entries = journal.entries_after(
                request.store_id,
                request.after_seq,
                request.limit.min(MAX_CATCHUP_PAGE_ENTRIES),
                MAX_CATCHUP_PAGE_BYTES,
            )?;
            let inspection = journal.inspect(request.store_id)?;
            Ok(StateStoreReplicaReply {
                replica: replica.clone(),
                store_id: request.store_id,
                ownership_epoch: request.ownership_epoch,
                result: StateStoreEntriesPage {
                    entries,
                    committed_seq: inspection
                        .committed
                        .as_ref()
                        .map(|mutation| mutation.sequence)
                        .unwrap_or(0),
                    applied_seq: inspection.applied_seq,
                    applied_digest: inspection.applied_digest,
                },
            })
        }

        async fn transfer(
            &self,
            replica: &StorageNodeId,
            request: StateStoreTransferRequest,
        ) -> Result<StateStoreReplicaReply<StateStoreTransferPage>, StateStoreMutationError>
        {
            let journal = self
                .replies
                .lock()
                .unwrap()
                .get(replica.as_str())
                .cloned()
                .ok_or(StateStoreMutationError::Unavailable)?;
            // In-memory transports carry no applied index; an empty page
            // pins at the replica's applied position.
            let inspection = journal.inspect(request.store_id)?;
            Ok(StateStoreReplicaReply {
                replica: replica.clone(),
                store_id: request.store_id,
                ownership_epoch: request.ownership_epoch,
                result: StateStoreTransferPage {
                    rows: Vec::new(),
                    next_key: None,
                    pinned_seq: inspection.applied_seq,
                    applied_digest: inspection.applied_digest,
                },
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

    struct StubFeedSource {
        feed_id: Uuid,
        exists: bool,
        records: Vec<ReplaySourceRecord>,
    }

    #[async_trait::async_trait]
    impl FeedReplaySource for StubFeedSource {
        async fn source_exists(&self, feed_id: Uuid) -> bool {
            self.exists && feed_id == self.feed_id
        }

        async fn read_committed_records(
            &self,
            feed_id: Uuid,
            after: Option<&str>,
            limit: usize,
        ) -> Result<Vec<ReplaySourceRecord>, StateStoreReplayError> {
            assert_eq!(feed_id, self.feed_id);
            let start = match after {
                None => 0,
                Some(cursor) => self
                    .records
                    .iter()
                    .position(|record| record.cursor == cursor)
                    .map(|position| position + 1)
                    .ok_or(StateStoreReplayError::FrontierOutOfRange)?,
            };
            Ok(self
                .records
                .iter()
                .skip(start)
                .take(limit)
                .cloned()
                .collect())
        }
    }

    fn replay_record(sequence: u64, key: &str) -> ReplaySourceRecord {
        ReplaySourceRecord {
            metadata: std::collections::BTreeMap::new(),
            message_id: Uuid::from_u128(0xF00D + sequence as u128),
            cursor: format!("cursor-{sequence}"),
            key: key.as_bytes().to_vec(),
            payload: format!("payload-{sequence}").into_bytes(),
        }
    }

    struct ReplayFixture {
        store: crate::control::StateStoreDefinition,
        feed_id: Uuid,
        control: Arc<crate::control::ControlController>,
        journals: Vec<Arc<FjallStateStoreMutationJournal>>,
        _dirs: Vec<TempDir>,
        _catalog_dir: TempDir,
    }

    impl ReplayFixture {
        fn driver(&self, source: Arc<StubFeedSource>) -> StateStoreReplayDriver {
            let transport: Arc<dyn StateStoreMutationTransport> =
                Arc::new(InMemoryTransport::new(&[
                    ("storage-1", self.journals[0].clone()),
                    ("storage-2", self.journals[1].clone()),
                    ("storage-3", self.journals[2].clone()),
                ]));
            StateStoreReplayDriver::new(
                self.control.clone(),
                source,
                Arc::new(move |assignment| {
                    Ok(StateStoreMutationCoordinator::new(
                        assignment.clone(),
                        transport.clone(),
                    ))
                }),
            )
        }
    }

    async fn replay_fixture() -> ReplayFixture {
        let catalog_dir = TempDir::new().unwrap();
        let store: Arc<dyn crate::storage::LogStore> =
            Arc::new(crate::storage::FileLogStore::open(catalog_dir.path().join("data")).unwrap());
        let control = Arc::new(
            crate::control::ControlController::open_with_storage_nodes(
                catalog_dir.path().join("catalog.json"),
                store,
                ["storage-1", "storage-2", "storage-3"]
                    .into_iter()
                    .map(|node| StorageNodeId::try_new(node).unwrap())
                    .collect(),
            )
            .unwrap(),
        );
        let created = control
            .execute_commands(vec![
                crate::control::Command::CreateSpace {
                    name: "orders".to_owned(),
                },
                crate::control::Command::CreateFeed {
                    name: "orders.events".to_owned(),
                },
            ])
            .await
            .unwrap();
        let feed_id =
            serde_json::from_value::<Uuid>(created.results[1].data["feed_id"].clone()).unwrap();
        control
            .execute_commands(vec![crate::control::Command::DefineStateStore {
                name: "orders.profiles".to_owned(),
                source: crate::control::StateStoreSourceRequest::Feed {
                    feed: "orders.events".to_owned(),
                },
                named_keys: vec![],
            }])
            .await
            .unwrap();
        let definition = control
            .declared_state_store_by_name("orders.profiles")
            .await
            .unwrap();
        let dirs = vec![
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
        ];
        let journals: Vec<Arc<FjallStateStoreMutationJournal>> = dirs
            .iter()
            .map(|dir| Arc::new(FjallStateStoreMutationJournal::open(dir.path()).unwrap()))
            .collect();
        ReplayFixture {
            store: definition,
            feed_id,
            control,
            journals,
            _dirs: dirs,
            _catalog_dir: catalog_dir,
        }
    }

    #[test]
    fn replay_mutation_id_is_deterministic_and_namespaces_rebuilds() {
        let store_id = Uuid::new_v4();
        let feed_id = Uuid::new_v4();
        let message_id = Uuid::new_v4();
        assert_eq!(
            replay_mutation_id(store_id, feed_id, message_id, None),
            replay_mutation_id(store_id, feed_id, message_id, None)
        );
        let token_a = Uuid::new_v4();
        assert_eq!(
            replay_mutation_id(store_id, feed_id, message_id, Some(token_a)),
            replay_mutation_id(store_id, feed_id, message_id, Some(token_a))
        );
        assert_ne!(
            replay_mutation_id(store_id, feed_id, message_id, None),
            replay_mutation_id(store_id, feed_id, message_id, Some(token_a))
        );
        assert_ne!(
            replay_mutation_id(store_id, feed_id, message_id, Some(token_a)),
            replay_mutation_id(store_id, feed_id, message_id, Some(Uuid::new_v4()))
        );
    }

    #[tokio::test]
    async fn replay_pass_commits_records_and_advances_the_durable_frontier() {
        let records = vec![
            replay_record(1, "user-1"),
            replay_record(2, "user-2"),
            replay_record(3, "user-1"),
        ];
        let fixture = replay_fixture().await;
        let driver = fixture.driver(Arc::new(StubFeedSource {
            feed_id: fixture.feed_id,
            exists: true,
            records,
        }));
        let outcome = driver.pass(&fixture.store, None, 8).await.unwrap();
        assert_eq!(outcome.applied, 3);
        assert!(outcome.complete);
        assert_eq!(outcome.frontier.as_deref(), Some("cursor-3"));
        for journal in &fixture.journals {
            let inspection = journal.inspect(fixture.store.store_id).unwrap();
            let committed = inspection.committed.unwrap();
            assert_eq!(committed.sequence, 3);
            assert_eq!(committed.source.as_ref().unwrap().cursor, "cursor-3");
            assert_eq!(committed.source.unwrap().feed_id, fixture.feed_id);
        }
        // A resumed pass sees no new source records and applies nothing.
        let second = driver.pass(&fixture.store, None, 8).await.unwrap();
        assert_eq!(second.applied, 0);
        assert!(second.complete);
        assert_eq!(second.frontier.as_deref(), Some("cursor-3"));
    }

    #[tokio::test]
    async fn replay_projects_named_key_values_from_source_records() {
        let fixture = replay_fixture().await;
        fixture
            .control
            .execute_commands(vec![crate::control::Command::DefineStateStore {
                name: "orders.tagged".to_owned(),
                source: crate::control::StateStoreSourceRequest::Feed {
                    feed: "orders.events".to_owned(),
                },
                named_keys: vec![
                    crate::control::NamedStateStoreKey {
                        name: "passport".to_owned(),
                        unique: true,
                        extractor: Some(crate::control::IndexFieldSpec::Metadata {
                            name: "pp".to_owned(),
                        }),
                    },
                    crate::control::NamedStateStoreKey {
                        name: "department".to_owned(),
                        unique: false,
                        extractor: Some(crate::control::IndexFieldSpec::Projection {
                            extractor: "json".to_owned(),
                            version: 1,
                            field: "dept".to_owned(),
                        }),
                    },
                ],
            }])
            .await
            .unwrap();
        let store = fixture
            .control
            .declared_state_store_by_name("orders.tagged")
            .await
            .unwrap();

        let mut first = replay_record(1, "user-1");
        first.metadata.insert("pp".to_owned(), b"P111".to_vec());
        first.payload = br#"{"dept":"eng","extra":1}"#.to_vec();
        // Missing metadata + a non-JSON payload project Null postings
        // rather than wedging replay at the frontier.
        let second = replay_record(2, "user-2");
        let driver = fixture.driver(Arc::new(StubFeedSource {
            feed_id: fixture.feed_id,
            exists: true,
            records: vec![first, second],
        }));

        let outcome = driver.pass(&store, None, 8).await.unwrap();
        assert_eq!(outcome.applied, 2);

        let entries = fixture.journals[0]
            .entries_after(store.store_id, 0, 8, 1 << 20)
            .unwrap();
        assert_eq!(entries.len(), 2);
        let StateStoreMutationOp::PutRow { named, .. } = &entries[0].mutation.op else {
            panic!("replay writes put_row mutations")
        };
        assert!(named.iter().any(|value| {
            value.name == "passport"
                && value.unique
                && value.values == vec![crate::index::IndexValue::Bytes(b"P111".to_vec())]
        }));
        assert!(named.iter().any(|value| {
            value.name == "department"
                && value.values == vec![crate::index::IndexValue::Text("eng".to_owned())]
        }));
        let StateStoreMutationOp::PutRow {
            named: second_named,
            ..
        } = &entries[1].mutation.op
        else {
            panic!("replay writes put_row mutations")
        };
        assert!(second_named
            .iter()
            .all(|value| { value.values == vec![crate::index::IndexValue::Null] }));
    }

    #[tokio::test]
    async fn replay_pass_resumes_mid_history_and_dedupes_overlap() {
        let records = vec![replay_record(1, "a"), replay_record(2, "b")];
        let fixture = replay_fixture().await;
        let driver = fixture.driver(Arc::new(StubFeedSource {
            feed_id: fixture.feed_id,
            exists: true,
            records,
        }));
        let first = driver.pass(&fixture.store, None, 1).await.unwrap();
        assert_eq!(first.applied, 1);
        assert!(!first.complete);
        assert_eq!(first.frontier.as_deref(), Some("cursor-1"));
        let second = driver.pass(&fixture.store, None, 8).await.unwrap();
        assert_eq!(second.applied, 1);
        assert!(second.complete);
        assert_eq!(second.frontier.as_deref(), Some("cursor-2"));
    }

    #[tokio::test]
    async fn replay_pass_refuses_when_source_is_gone() {
        let fixture = replay_fixture().await;
        let driver = fixture.driver(Arc::new(StubFeedSource {
            feed_id: fixture.feed_id,
            exists: false,
            records: Vec::new(),
        }));
        let error = driver.pass(&fixture.store, None, 8).await.unwrap_err();
        assert!(matches!(error, StateStoreReplayError::SourceUnavailable));
        // Rebuild refuses too: a vanished source cannot rematerialize rows.
        let rebuild = driver
            .pass(&fixture.store, Some(Uuid::new_v4()), 8)
            .await
            .unwrap_err();
        assert!(matches!(rebuild, StateStoreReplayError::SourceUnavailable));
    }

    #[tokio::test]
    async fn replay_pass_reports_frontier_out_of_range() {
        let records = vec![replay_record(1, "a"), replay_record(2, "b")];
        let fixture = replay_fixture().await;
        let driver = fixture.driver(Arc::new(StubFeedSource {
            feed_id: fixture.feed_id,
            exists: true,
            records,
        }));
        driver.pass(&fixture.store, None, 8).await.unwrap();
        // Rotate the source: the journaled frontier cursor-2 no longer
        // resolves, so the pass refuses until an explicit rebuild.
        let pruned = fixture.driver(Arc::new(StubFeedSource {
            feed_id: fixture.feed_id,
            exists: true,
            records: vec![replay_record(5, "c")],
        }));
        let error = pruned.pass(&fixture.store, None, 8).await.unwrap_err();
        assert!(matches!(error, StateStoreReplayError::FrontierOutOfRange));
    }

    #[tokio::test]
    async fn read_row_serves_applied_row_with_freshness_and_fencing() {
        let fixture = replay_fixture().await;
        let directory = TempDir::new().unwrap();
        let index: Arc<dyn IndexStore> = Arc::new(FjallIndexStore::open(directory.path()).unwrap());
        let journal = fixture.journals[0].clone();
        let service = StateStoreMutationService::new(
            node("storage-1"),
            fixture.control.clone(),
            journal.clone(),
            Arc::new(IndexStoreStateApply::new(index.clone())),
        );
        let store_id = fixture.store.store_id;
        let assignment = fixture
            .control
            .active_state_store_assignment_by_id(store_id)
            .await
            .unwrap();
        let owner = assignment.owner.clone();
        let epoch = assignment.ownership_epoch;
        let read = |key: &[u8], epoch: u64| StateStoreReadRequest {
            by_named: None,
            by_named_range: None,
            owner: owner.clone(),
            receiver: node("storage-1"),
            store_id,
            ownership_epoch: epoch,
            key: key.to_vec(),
            strict: false,
            wait_ms: 0,
        };
        // Empty journal: nothing committed or applied yet.
        let empty = service.read_row(read(b"k", epoch)).await.unwrap();
        assert!(empty.result.row.is_none());
        assert_eq!(empty.result.applied_seq, 0);
        assert_eq!(empty.result.committed_seq, 0);

        // Commit + apply one mutation directly through the journal seam.
        let mut first = mutation(store_id, 1, "user-7");
        first.ownership_epoch = epoch;
        journal.prepare(first.clone()).unwrap();
        let votes = [
            (owner.clone(), mutation_digest(&first).unwrap()),
            (node("storage-2"), mutation_digest(&first).unwrap()),
        ];
        let committed = journal
            .commit_with_quorum(
                store_id,
                epoch,
                &StateStoreCommitEvidence {
                    store_id,
                    request_id: first.request_id,
                    mutation_id: first.mutation_id,
                    sequence: 1,
                    votes,
                },
            )
            .unwrap();
        // Committed-but-unapplied: freshness evidence shows the lag before
        // the row appears — callers see `applied_seq < committed_seq`.
        let lagging = service.read_row(read(b"user-7", epoch)).await.unwrap();
        assert!(lagging.result.row.is_none());
        assert_eq!(lagging.result.applied_seq, 0);
        assert_eq!(lagging.result.committed_seq, 1);
        service.apply.apply(&committed).unwrap();
        journal
            .mark_applied(
                store_id,
                1,
                &applied_chain_digest(None, &committed).unwrap(),
            )
            .unwrap();

        let reply = service.read_row(read(b"user-7", epoch)).await.unwrap();
        assert_eq!(reply.replica, node("storage-1"));
        let row = reply.result.row.unwrap();
        assert_eq!(row.payload, b"payload");
        assert_eq!(row.applied_cursor, "00000000000000000001");
        assert_eq!(reply.result.applied_seq, 1);
        assert_eq!(reply.result.committed_seq, 1);

        // Missing key reads back absent without error.
        let missing = service.read_row(read(b"none", epoch)).await.unwrap();
        assert!(missing.result.row.is_none());

        // A strict read on a lagging replica waits, then reports the lag.
        let mut second = mutation(store_id, 2, "user-8");
        second.ownership_epoch = epoch;
        journal.prepare(second.clone()).unwrap();
        let committed = journal
            .commit_with_quorum(
                store_id,
                epoch,
                &StateStoreCommitEvidence {
                    store_id,
                    request_id: second.request_id,
                    mutation_id: second.mutation_id,
                    sequence: 2,
                    votes: [
                        (owner.clone(), mutation_digest(&second).unwrap()),
                        (node("storage-2"), mutation_digest(&second).unwrap()),
                    ],
                },
            )
            .unwrap();
        let mut strict = read(b"user-8", epoch);
        strict.strict = true;
        strict.wait_ms = 60;
        let lagged = service.read_row(strict.clone()).await.unwrap();
        assert!(lagged.result.row.is_none());
        assert_eq!(lagged.result.committed_seq, 2);
        assert!(lagged.result.applied_seq < 2);
        // After the apply lands, the same strict read serves the row.
        service.apply.apply(&committed).unwrap();
        let prior = journal.entry_applied_digest(store_id, 1).unwrap();
        journal
            .mark_applied(
                store_id,
                2,
                &applied_chain_digest(prior.as_deref(), &committed).unwrap(),
            )
            .unwrap();
        let caught = service.read_row(strict).await.unwrap();
        assert!(caught.result.row.is_some());
        assert_eq!(caught.result.applied_seq, 2);

        // Stale ownership epochs and oversized keys are fenced.
        assert!(matches!(
            service.read_row(read(b"k", 9)).await.unwrap_err(),
            StateStoreMutationError::InvalidAssignment
        ));
        assert!(matches!(
            service
                .read_row(read(&vec![0_u8; STATE_STORE_MAX_KEY_BYTES + 1], 1))
                .await
                .unwrap_err(),
            StateStoreMutationError::TooLarge
        ));
    }

    #[tokio::test]
    async fn rebuild_recommits_records_under_a_namespaced_identity() {
        let records = vec![replay_record(1, "a"), replay_record(2, "b")];
        let fixture = replay_fixture().await;
        let driver = fixture.driver(Arc::new(StubFeedSource {
            feed_id: fixture.feed_id,
            exists: true,
            records,
        }));
        driver.pass(&fixture.store, None, 8).await.unwrap();
        let token = Uuid::new_v4();
        let rebuilt = driver.pass(&fixture.store, Some(token), 8).await.unwrap();
        // Rebuild re-commits every retained record at fresh sequence
        // positions; rows converge by overwrite.
        assert_eq!(rebuilt.applied, 2);
        assert!(rebuilt.complete);
        assert_eq!(rebuilt.frontier.as_deref(), Some("cursor-2"));
        let inspection = fixture.journals[0].inspect(fixture.store.store_id).unwrap();
        assert_eq!(inspection.committed.unwrap().sequence, 4);
        // A retried rebuild with the same token dedupes.
        let retried = driver.pass(&fixture.store, Some(token), 8).await.unwrap();
        assert_eq!(retried.applied, 2);
        assert_eq!(
            fixture.journals[0]
                .inspect(fixture.store.store_id)
                .unwrap()
                .committed
                .unwrap()
                .sequence,
            4
        );
    }

    #[test]
    fn install_committed_fences_gaps_dedupes_and_conflicts() {
        let directory = TempDir::new().unwrap();
        let journal = FjallStateStoreMutationJournal::open(directory.path()).unwrap();
        let store_id = Uuid::new_v4();

        // Sequence must be gap-free: installing 2 before 1 refuses.
        let mut second = mutation(store_id, 2, "b");
        second.ownership_epoch = 7;
        assert!(matches!(
            journal.install_committed(&second),
            Err(StateStoreMutationError::Sequence)
        ));

        let mut first = mutation(store_id, 1, "a");
        first.ownership_epoch = 7;
        assert!(journal.install_committed(&first).unwrap());
        // Same content at the same sequence dedupes; different content
        // conflicts.
        assert!(!journal.install_committed(&first).unwrap());
        let mut conflicting = first.clone();
        conflicting.mutation_id = Uuid::new_v4();
        assert!(matches!(
            journal.install_committed(&conflicting),
            Err(StateStoreMutationError::Conflict)
        ));
        second.ownership_epoch = 7;
        assert!(journal.install_committed(&second).unwrap());
        let inspection = journal.inspect(store_id).unwrap();
        assert_eq!(inspection.committed.unwrap().sequence, 2);
        // The installed mutation is idempotency-visible too.
        assert_eq!(
            journal
                .mutation_by_id(store_id, first.mutation_id)
                .unwrap()
                .unwrap()
                .sequence,
            1
        );
    }

    #[test]
    fn entries_after_bounds_pages_and_reports_digests() {
        let directory = TempDir::new().unwrap();
        let journal = FjallStateStoreMutationJournal::open(directory.path()).unwrap();
        let store_id = Uuid::new_v4();
        let placement = assignment(store_id);
        let mut previous: Option<String> = None;
        for sequence in 1..=5u64 {
            let vote = journal
                .prepare(mutation(store_id, 0, &format!("k{sequence}")))
                .unwrap();
            let mutation = journal
                .commit_with_quorum(store_id, 1, &evidence(&vote.mutation, &placement))
                .unwrap();
            let digest = applied_chain_digest(previous.as_deref(), &mutation).unwrap();
            journal.mark_applied(store_id, sequence, &digest).unwrap();
            previous = Some(digest);
        }

        let page = journal
            .entries_after(store_id, 0, 3, MAX_CATCHUP_PAGE_BYTES)
            .unwrap();
        assert_eq!(page.len(), 3);
        assert_eq!(page[0].mutation.sequence, 1);
        assert_eq!(page[2].mutation.sequence, 3);
        assert!(page.iter().all(|entry| entry.applied_digest.is_some()));
        let rest = journal
            .entries_after(store_id, 3, 64, MAX_CATCHUP_PAGE_BYTES)
            .unwrap();
        assert_eq!(rest.len(), 2);
        // A tiny byte budget still returns at least one entry then stops.
        let tight = journal.entries_after(store_id, 0, 64, 8).unwrap();
        assert_eq!(tight.len(), 1);
        // The chain digest is deterministic: recomputing from genesis lands
        // on the same value the replica recorded at mark_applied.
        let head_digest = journal.inspect(store_id).unwrap().applied_digest.unwrap();
        let mut rebuilt: Option<String> = None;
        for sequence in 1..=5u64 {
            let mutation = journal.entry(store_id, sequence).unwrap().unwrap();
            rebuilt = Some(applied_chain_digest(rebuilt.as_deref(), &mutation).unwrap());
        }
        assert_eq!(rebuilt.as_deref(), Some(head_digest.as_str()));
    }

    #[test]
    fn install_checkpoint_advances_monotonically() {
        let directory = TempDir::new().unwrap();
        let journal = FjallStateStoreMutationJournal::open(directory.path()).unwrap();
        let store_id = Uuid::new_v4();
        assert!(journal
            .install_checkpoint(store_id, 7, 40, "chain-at-40")
            .unwrap());
        let inspection = journal.inspect(store_id).unwrap();
        assert_eq!(inspection.applied_seq, 40);
        assert_eq!(inspection.applied_digest.as_deref(), Some("chain-at-40"));
        // Applied never moves backwards.
        assert!(!journal
            .install_checkpoint(store_id, 7, 39, "older")
            .unwrap());
        let inspection = journal.inspect(store_id).unwrap();
        assert_eq!(inspection.applied_seq, 40);
        assert_eq!(inspection.applied_digest.as_deref(), Some("chain-at-40"));
        // Entries at or below the checkpoint are covered; the next
        // committed position installs normally.
        let mut covered = mutation(store_id, 3, "old");
        covered.ownership_epoch = 7;
        assert!(!journal.install_committed(&covered).unwrap());
        let mut next = mutation(store_id, 41, "new");
        next.ownership_epoch = 7;
        assert!(journal.install_committed(&next).unwrap());
    }

    #[tokio::test]
    async fn catch_up_installs_missing_committed_entries_and_verifies_digests() {
        let fixture = replay_fixture().await;
        let store_id = fixture.store.store_id;
        let assignment = fixture
            .control
            .active_state_store_assignment_by_id(store_id)
            .await
            .unwrap();
        let owner = assignment.owner.clone();
        let epoch = assignment.ownership_epoch;
        // The lagging replica is the replica that is neither the owner nor
        // the second quorum member; its journal never sees prepare/commit.
        let lagging_index = assignment
            .replicas
            .iter()
            .position(|replica| *replica != owner)
            .unwrap();
        let lagging_node = assignment
            .replicas
            .iter()
            .nth(lagging_index)
            .unwrap()
            .clone();
        let committing_nodes: Vec<(String, Arc<FjallStateStoreMutationJournal>)> = assignment
            .replicas
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != lagging_index)
            .map(|(index, replica)| (replica.to_string(), fixture.journals[index].clone()))
            .collect();
        let committing_refs: Vec<(&str, Arc<FjallStateStoreMutationJournal>)> = committing_nodes
            .iter()
            .map(|(name, journal)| (name.as_str(), journal.clone()))
            .collect();
        let committing = Arc::new(InMemoryTransport::new(&committing_refs));
        let coordinator = StateStoreMutationCoordinator::new(assignment.clone(), committing);
        for key in ["alpha", "beta", "gamma"] {
            let mut m = mutation(store_id, 0, key);
            m.ownership_epoch = epoch;
            m.op = StateStoreMutationOp::PutRow {
                key: key.as_bytes().to_vec(),
                payload: format!("v-{key}").into_bytes(),
                named: Vec::new(),
            };
            coordinator.mutate(m).await.unwrap();
        }
        // The two healthy replicas applied through their journals so they
        // carry applied chain digests the lagging replica verifies against.
        let dirs: Vec<TempDir> = (0..3).map(|_| TempDir::new().unwrap()).collect();
        let services: Vec<StateStoreMutationService> = fixture
            .journals
            .iter()
            .enumerate()
            .map(|(index, journal)| {
                let index_store: Arc<dyn IndexStore> =
                    Arc::new(FjallIndexStore::open(dirs[index].path()).unwrap());
                StateStoreMutationService::new(
                    assignment.replicas.iter().nth(index).unwrap().clone(),
                    fixture.control.clone(),
                    journal.clone(),
                    Arc::new(IndexStoreStateApply::new(index_store)),
                )
            })
            .collect();
        for (index, service) in services.iter().enumerate() {
            if index != lagging_index {
                assert_eq!(journal_pending(service, store_id).await, 3);
                assert_eq!(service.replay_pending(store_id).await.unwrap(), 3);
            }
        }
        // The lagging replica's journal is still empty.
        let lagging = fixture.journals[lagging_index].inspect(store_id).unwrap();
        assert!(lagging.committed.is_none());
        assert_eq!(lagging.applied_seq, 0);

        // Catch up over the in-memory transport that now includes all three.
        let transport: Arc<dyn StateStoreMutationTransport> = Arc::new(InMemoryTransport::new(&[
            ("storage-1", fixture.journals[0].clone()),
            ("storage-2", fixture.journals[1].clone()),
            ("storage-3", fixture.journals[2].clone()),
        ]));
        let outcome = services[lagging_index]
            .catch_up_replica(&transport, &assignment, 64)
            .await
            .unwrap();
        assert_eq!(outcome.installed, 3);
        assert_eq!(outcome.applied, 3);
        assert!(outcome.complete);
        let caught = fixture.journals[lagging_index].inspect(store_id).unwrap();
        assert_eq!(caught.committed.unwrap().sequence, 3);
        assert_eq!(caught.applied_seq, 3);
        // Chain digests converge with a healthy replica at every sequence.
        let reference_index = assignment
            .replicas
            .iter()
            .position(|replica| *replica == owner)
            .unwrap();
        let reference = fixture.journals[reference_index].inspect(store_id).unwrap();
        assert_eq!(caught.applied_digest, reference.applied_digest);
        // The caught-up replica serves the applied rows.
        let row = services[lagging_index]
            .read_row(StateStoreReadRequest {
                by_named: None,
                by_named_range: None,
                owner: owner.clone(),
                receiver: lagging_node.clone(),
                store_id,
                ownership_epoch: epoch,
                key: b"beta".to_vec(),
                strict: true,
                wait_ms: 10,
            })
            .await
            .unwrap();
        assert_eq!(row.result.row.unwrap().payload, b"v-beta");
        assert_eq!(row.result.applied_seq, row.result.committed_seq);

        // A second pass is idle work that stays idempotent.
        let idle = services[lagging_index]
            .catch_up_replica(&transport, &assignment, 64)
            .await
            .unwrap();
        assert_eq!(idle.installed, 0);
        assert_eq!(idle.applied, 0);
        assert!(idle.complete);
    }

    #[tokio::test]
    async fn catch_up_reports_diverged_when_peer_digest_disagrees() {
        let fixture = replay_fixture().await;
        let store_id = fixture.store.store_id;
        let assignment = fixture
            .control
            .active_state_store_assignment_by_id(store_id)
            .await
            .unwrap();
        let epoch = assignment.ownership_epoch;
        let lagging_index = assignment
            .replicas
            .iter()
            .position(|replica| *replica != assignment.owner)
            .unwrap();
        let committing_nodes: Vec<(String, Arc<FjallStateStoreMutationJournal>)> = assignment
            .replicas
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != lagging_index)
            .map(|(index, replica)| (replica.to_string(), fixture.journals[index].clone()))
            .collect();
        let committing_refs: Vec<(&str, Arc<FjallStateStoreMutationJournal>)> = committing_nodes
            .iter()
            .map(|(name, journal)| (name.as_str(), journal.clone()))
            .collect();
        let committing = Arc::new(InMemoryTransport::new(&committing_refs));
        let coordinator = StateStoreMutationCoordinator::new(assignment.clone(), committing);
        let mut first = mutation(store_id, 0, "a");
        first.ownership_epoch = epoch;
        coordinator.mutate(first.clone()).await.unwrap();
        // Poison BOTH reachable replicas' attested digest for sequence 1:
        // whichever peer the lagging replica pulls from attests a chain
        // value its own application will not reproduce.
        let committed = fixture.journals[0]
            .inspect(store_id)
            .unwrap()
            .committed
            .unwrap();
        for (index, _) in assignment.replicas.iter().enumerate() {
            if index != lagging_index {
                fixture.journals[index]
                    .mark_applied(store_id, 1, "bogus-attested-digest")
                    .unwrap();
            }
        }
        let _ = applied_chain_digest(None, &committed).unwrap();
        let dirs: Vec<TempDir> = (0..3).map(|_| TempDir::new().unwrap()).collect();
        let service = StateStoreMutationService::new(
            assignment
                .replicas
                .iter()
                .nth(lagging_index)
                .unwrap()
                .clone(),
            fixture.control.clone(),
            fixture.journals[lagging_index].clone(),
            Arc::new(IndexStoreStateApply::new(Arc::new(
                FjallIndexStore::open(dirs[lagging_index].path()).unwrap(),
            ))),
        );
        let transport: Arc<dyn StateStoreMutationTransport> = Arc::new(InMemoryTransport::new(&[
            ("storage-1", fixture.journals[0].clone()),
            ("storage-2", fixture.journals[1].clone()),
            ("storage-3", fixture.journals[2].clone()),
        ]));
        let error = service
            .catch_up_replica(&transport, &assignment, 64)
            .await
            .unwrap_err();
        assert!(matches!(error, StateStoreMutationError::Diverged));
    }

    #[tokio::test]
    async fn transfer_pages_applied_rows_and_install_advances_checkpoint() {
        let fixture = replay_fixture().await;
        let store_id = fixture.store.store_id;
        let assignment = fixture
            .control
            .active_state_store_assignment_by_id(store_id)
            .await
            .unwrap();
        let epoch = assignment.ownership_epoch;
        let owner_dir = TempDir::new().unwrap();
        let owner_index: Arc<dyn IndexStore> =
            Arc::new(FjallIndexStore::open(owner_dir.path()).unwrap());
        let owner_apply = Arc::new(IndexStoreStateApply::new(owner_index));
        let source = StateStoreMutationService::new(
            node("storage-1"),
            fixture.control.clone(),
            fixture.journals[0].clone(),
            owner_apply,
        );
        // Commit + apply three rows on the source through the service path.
        let committing = Arc::new(InMemoryTransport::new(&[
            ("storage-1", fixture.journals[0].clone()),
            ("storage-2", fixture.journals[1].clone()),
            ("storage-3", fixture.journals[2].clone()),
        ]));
        let coordinator =
            StateStoreMutationCoordinator::new(assignment.clone(), committing.clone());
        for key in ["k1", "k2", "k3"] {
            let mut m = mutation(store_id, 0, key);
            m.ownership_epoch = epoch;
            m.op = StateStoreMutationOp::PutRow {
                key: key.as_bytes().to_vec(),
                payload: format!("payload-{key}").into_bytes(),
                named: Vec::new(),
            };
            coordinator.mutate(m).await.unwrap();
        }
        // Commit lands via journal seam; drive the source's pending applies.
        source.replay_pending(store_id).await.unwrap();

        // Page the transfer at two rows per page.
        let request = |after: Option<Vec<u8>>| StateStoreTransferRequest {
            owner: assignment.owner.clone(),
            receiver: node("storage-1"),
            store_id,
            ownership_epoch: epoch,
            after_key: after,
            limit: 2,
        };
        let first = source.transfer(request(None)).await.unwrap().result;
        assert_eq!(first.rows.len(), 2);
        assert_eq!(first.pinned_seq, 3);
        let next = first.next_key.clone().unwrap();
        let second = source.transfer(request(Some(next))).await.unwrap().result;
        assert_eq!(second.rows.len(), 1);
        assert!(second.next_key.is_none());

        // A fresh replica installs the rows and adopts the checkpoint.
        let fresh_dir = TempDir::new().unwrap();
        let fresh_index: Arc<dyn IndexStore> =
            Arc::new(FjallIndexStore::open(fresh_dir.path()).unwrap());
        let fresh_apply = IndexStoreStateApply::new(fresh_index.clone());
        for row in first.rows.iter().chain(second.rows.iter()) {
            fresh_apply.install_row(store_id, row).unwrap();
        }
        fixture.journals[2]
            .install_checkpoint(store_id, epoch, first.pinned_seq, "attested")
            .unwrap();
        let inspection = fixture.journals[2].inspect(store_id).unwrap();
        assert_eq!(inspection.applied_seq, 3);
        assert_eq!(inspection.applied_digest.as_deref(), Some("attested"));
        let row = fresh_index
            .get(&PrimaryRef {
                feed_id: store_id,
                application_key: b"k2".to_vec(),
            })
            .unwrap()
            .unwrap();
        assert_eq!(row.payload, b"payload-k2");
    }

    #[test]
    fn put_row_writes_named_postings_and_unique_claims() {
        let dir = TempDir::new().unwrap();
        let index: Arc<dyn IndexStore> = Arc::new(FjallIndexStore::open(dir.path()).unwrap());
        let apply = IndexStoreStateApply::new(index.clone());
        let store_id = Uuid::new_v4();

        let mut first = mutation(store_id, 1, "user-1");
        first.op = StateStoreMutationOp::PutRow {
            key: b"user-1".to_vec(),
            payload: b"profile".to_vec(),
            named: vec![
                NamedKeyValue {
                    name: "passport".to_owned(),
                    values: vec![crate::index::IndexValue::Text("P123".to_owned())],
                    unique: true,
                },
                NamedKeyValue {
                    name: "department".to_owned(),
                    values: vec![crate::index::IndexValue::Text("eng".to_owned())],
                    unique: false,
                },
            ],
        };
        apply.apply(&first).unwrap();
        let rows = index
            .lookup_exact(
                named_key_index_id(store_id, "department"),
                &[crate::index::IndexValue::Text("eng".to_owned())],
                8,
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].reference.application_key, b"user-1");

        // A second row with the same non-unique value joins the list.
        let mut second = mutation(store_id, 2, "user-2");
        second.op = StateStoreMutationOp::PutRow {
            key: b"user-2".to_vec(),
            payload: b"profile-2".to_vec(),
            named: vec![NamedKeyValue {
                name: "department".to_owned(),
                values: vec![crate::index::IndexValue::Text("eng".to_owned())],
                unique: false,
            }],
        };
        apply.apply(&second).unwrap();
        let rows = index
            .lookup_exact(
                named_key_index_id(store_id, "department"),
                &[crate::index::IndexValue::Text("eng".to_owned())],
                8,
            )
            .unwrap();
        assert_eq!(rows.len(), 2);

        // A third row claiming the taken unique value loses by journal
        // order: the put is a journaled no-op — no row, no postings, and the
        // earlier claim still points at user-1.
        let mut conflict = mutation(store_id, 3, "user-3");
        conflict.op = StateStoreMutationOp::PutRow {
            key: b"user-3".to_vec(),
            payload: b"conflict".to_vec(),
            named: vec![NamedKeyValue {
                name: "passport".to_owned(),
                values: vec![crate::index::IndexValue::Text("P123".to_owned())],
                unique: true,
            }],
        };
        apply.apply(&conflict).unwrap();
        assert!(index
            .get(&PrimaryRef {
                feed_id: store_id,
                application_key: b"user-3".to_vec(),
            })
            .unwrap()
            .is_none());
        let winners = index
            .lookup_exact(
                named_key_index_id(store_id, "passport"),
                &[crate::index::IndexValue::Text("P123".to_owned())],
                8,
            )
            .unwrap();
        assert_eq!(winners.len(), 1);
        assert_eq!(winners[0].reference.application_key, b"user-1");

        // Deleting a row removes its named-key postings.
        let mut delete = mutation(store_id, 4, "user-1");
        delete.op = StateStoreMutationOp::DeleteRow {
            key: b"user-1".to_vec(),
        };
        apply.apply(&delete).unwrap();
        let rows = index
            .lookup_exact(
                named_key_index_id(store_id, "department"),
                &[crate::index::IndexValue::Text("eng".to_owned())],
                8,
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].reference.application_key, b"user-2");
        assert!(index
            .lookup_exact(
                named_key_index_id(store_id, "passport"),
                &[crate::index::IndexValue::Text("P123".to_owned())],
                8,
            )
            .unwrap()
            .is_empty());
    }

    #[test]
    fn clear_all_empties_rows_and_named_postings() {
        let dir = TempDir::new().unwrap();
        let index: Arc<dyn IndexStore> = Arc::new(FjallIndexStore::open(dir.path()).unwrap());
        let apply = IndexStoreStateApply::new(index.clone());
        let store_id = Uuid::new_v4();
        for (sequence, key) in ["a", "b", "c"].iter().enumerate() {
            let mut m = mutation(store_id, sequence as u64 + 1, key);
            m.op = StateStoreMutationOp::PutRow {
                key: key.as_bytes().to_vec(),
                payload: b"v".to_vec(),
                named: vec![NamedKeyValue {
                    name: "department".to_owned(),
                    values: vec![crate::index::IndexValue::Text("eng".to_owned())],
                    unique: false,
                }],
            };
            apply.apply(&m).unwrap();
        }
        let mut clear = mutation(store_id, 4, "");
        clear.op = StateStoreMutationOp::ClearAll;
        apply.apply(&clear).unwrap();
        assert!(index.scan(store_id, None, 64).unwrap().is_empty());
        assert!(index
            .lookup_exact(
                named_key_index_id(store_id, "department"),
                &[crate::index::IndexValue::Text("eng".to_owned())],
                8,
            )
            .unwrap()
            .is_empty());
        // Re-applying the committed clear is a recorded no-op.
        apply.apply(&clear).unwrap();
        // A row written after the clear survives.
        let mut after = mutation(store_id, 5, "z");
        after.op = StateStoreMutationOp::PutRow {
            key: b"z".to_vec(),
            payload: b"new".to_vec(),
            named: Vec::new(),
        };
        apply.apply(&after).unwrap();
        assert_eq!(index.scan(store_id, None, 64).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn read_row_resolves_by_named_postings() {
        let catalog_dir = TempDir::new().unwrap();
        let store: Arc<dyn crate::storage::LogStore> =
            Arc::new(crate::storage::FileLogStore::open(catalog_dir.path().join("data")).unwrap());
        let control = Arc::new(
            crate::control::ControlController::open_with_storage_nodes(
                catalog_dir.path().join("catalog"),
                store,
                ["storage-1", "storage-2", "storage-3"]
                    .into_iter()
                    .map(|node| StorageNodeId::try_new(node).unwrap())
                    .collect(),
            )
            .unwrap(),
        );
        control.execute("CREATE SPACE orders;").await.unwrap();
        control
            .execute_commands(vec![crate::control::Command::DefineStateStore {
                name: "orders.profiles".to_owned(),
                source: crate::control::StateStoreSourceRequest::Manual,
                named_keys: vec![crate::control::NamedStateStoreKey {
                    name: "department".to_owned(),
                    unique: false,
                    extractor: None,
                }],
            }])
            .await
            .unwrap();
        let definition = control
            .declared_state_store_by_name("orders.profiles")
            .await
            .unwrap();
        let assignment = control
            .active_state_store_assignment_by_id(definition.store_id)
            .await
            .unwrap();

        let index_dir = TempDir::new().unwrap();
        let index: Arc<dyn IndexStore> = Arc::new(FjallIndexStore::open(index_dir.path()).unwrap());
        let apply = IndexStoreStateApply::new(index);
        let mut m = mutation(definition.store_id, 1, "user-1");
        m.op = StateStoreMutationOp::PutRow {
            key: b"user-1".to_vec(),
            payload: b"profile".to_vec(),
            named: vec![NamedKeyValue {
                name: "department".to_owned(),
                values: vec![crate::index::IndexValue::Text("eng".to_owned())],
                unique: false,
            }],
        };
        apply.apply(&m).unwrap();

        let journal_dir = TempDir::new().unwrap();
        let journal = Arc::new(FjallStateStoreMutationJournal::open(journal_dir.path()).unwrap());
        let service = StateStoreMutationService::new(
            assignment.owner.clone(),
            control,
            journal,
            Arc::new(apply),
        );
        let reply = service
            .read_row(StateStoreReadRequest {
                owner: assignment.owner.clone(),
                receiver: assignment.owner.clone(),
                store_id: definition.store_id,
                ownership_epoch: assignment.ownership_epoch,
                key: Vec::new(),
                strict: false,
                wait_ms: 0,
                by_named: Some(StateStoreNamedRead {
                    name: "department".to_owned(),
                    values: vec![crate::index::IndexValue::Text("eng".to_owned())],
                    limit: 8,
                }),
                by_named_range: None,
            })
            .await
            .unwrap();
        assert_eq!(reply.result.named_rows.len(), 1);
        assert_eq!(reply.result.named_rows[0].key, b"user-1");
        assert_eq!(reply.result.named_rows[0].payload, b"profile");
    }

    #[tokio::test]
    async fn read_row_resolves_named_range_postings_in_typed_order() {
        let catalog_dir = TempDir::new().unwrap();
        let store: Arc<dyn crate::storage::LogStore> =
            Arc::new(crate::storage::FileLogStore::open(catalog_dir.path().join("data")).unwrap());
        let control = Arc::new(
            crate::control::ControlController::open_with_storage_nodes(
                catalog_dir.path().join("catalog"),
                store,
                ["storage-1", "storage-2", "storage-3"]
                    .into_iter()
                    .map(|node| StorageNodeId::try_new(node).unwrap())
                    .collect(),
            )
            .unwrap(),
        );
        control.execute("CREATE SPACE orders;").await.unwrap();
        control
            .execute_commands(vec![crate::control::Command::DefineStateStore {
                name: "orders.panes".to_owned(),
                source: crate::control::StateStoreSourceRequest::Manual,
                named_keys: vec![crate::control::NamedStateStoreKey {
                    name: "pane".to_owned(),
                    unique: false,
                    extractor: None,
                }],
            }])
            .await
            .unwrap();
        let definition = control
            .declared_state_store_by_name("orders.panes")
            .await
            .unwrap();
        let assignment = control
            .active_state_store_assignment_by_id(definition.store_id)
            .await
            .unwrap();
        let index_dir = TempDir::new().unwrap();
        let index: Arc<dyn IndexStore> = Arc::new(FjallIndexStore::open(index_dir.path()).unwrap());
        let apply = IndexStoreStateApply::new(index);
        for (sequence, key, pane) in [(1u64, "row-a", 2i64), (2, "row-b", 5), (3, "row-c", 9)] {
            let mut m = mutation(definition.store_id, sequence, key);
            m.op = StateStoreMutationOp::PutRow {
                key: key.as_bytes().to_vec(),
                payload: format!("acc-{pane}").into_bytes(),
                named: vec![NamedKeyValue {
                    name: "pane".to_owned(),
                    values: vec![crate::index::IndexValue::I64(pane)],
                    unique: false,
                }],
            };
            apply.apply(&m).unwrap();
        }
        let journal_dir = TempDir::new().unwrap();
        let journal = Arc::new(FjallStateStoreMutationJournal::open(journal_dir.path()).unwrap());
        let service = StateStoreMutationService::new(
            assignment.owner.clone(),
            control,
            journal,
            Arc::new(apply),
        );
        let range = |lower: Option<crate::index::IndexValue>,
                     upper: Option<crate::index::IndexValue>,
                     limit| StateStoreReadRequest {
            owner: assignment.owner.clone(),
            receiver: assignment.owner.clone(),
            store_id: definition.store_id,
            ownership_epoch: assignment.ownership_epoch,
            key: Vec::new(),
            strict: false,
            wait_ms: 0,
            by_named: None,
            by_named_range: Some(StateStoreNamedRangeRead {
                name: "pane".to_owned(),
                lower: lower.map(|value| crate::control::IndexBound {
                    value,
                    inclusive: true,
                }),
                upper: upper.map(|value| crate::control::IndexBound {
                    value,
                    inclusive: true,
                }),
                limit,
            }),
        };
        // Inclusive upper bound collects panes <= 5 in ascending order.
        let reply = service
            .read_row(range(None, Some(crate::index::IndexValue::I64(5)), 8))
            .await
            .unwrap();
        let keys: Vec<&[u8]> = reply
            .result
            .named_rows
            .iter()
            .map(|row| row.key.as_slice())
            .collect();
        assert_eq!(keys, vec![b"row-a".as_slice(), b"row-b".as_slice()]);
        // A bounded window [5, 9] collects exactly its endpoints.
        let reply = service
            .read_row(range(
                Some(crate::index::IndexValue::I64(5)),
                Some(crate::index::IndexValue::I64(9)),
                8,
            ))
            .await
            .unwrap();
        let keys: Vec<&[u8]> = reply
            .result
            .named_rows
            .iter()
            .map(|row| row.key.as_slice())
            .collect();
        assert_eq!(keys, vec![b"row-b".as_slice(), b"row-c".as_slice()]);
        // The limit bounds the scan.
        let reply = service.read_row(range(None, None, 1)).await.unwrap();
        assert_eq!(reply.result.named_rows.len(), 1);
        // Inverted bounds and mixed read shapes refuse.
        assert!(service
            .read_row(range(
                Some(crate::index::IndexValue::I64(9)),
                Some(crate::index::IndexValue::I64(5)),
                8,
            ))
            .await
            .is_err());
        let mut mixed = range(None, None, 8);
        mixed.by_named = Some(StateStoreNamedRead {
            name: "pane".to_owned(),
            values: vec![crate::index::IndexValue::I64(5)],
            limit: 8,
        });
        assert!(service.read_row(mixed).await.is_err());
        // A range read on an undeclared named key resolves empty.
        let mut missing = range(None, None, 8);
        missing.by_named_range.as_mut().unwrap().name = "nope".to_owned();
        assert!(service
            .read_row(missing)
            .await
            .unwrap()
            .result
            .named_rows
            .is_empty());
    }

    /// Pending committed count on a service's own journal.
    async fn journal_pending(service: &StateStoreMutationService, store_id: Uuid) -> usize {
        let journal = service.journal.clone();
        tokio::task::spawn_blocking(move || journal.pending_applies(store_id))
            .await
            .unwrap()
            .unwrap()
            .len()
    }
    #[test]
    fn accumulate_rows_apply_merges_and_replays_idempotently() {
        let dir = TempDir::new().unwrap();
        let index: Arc<dyn IndexStore> = Arc::new(FjallIndexStore::open(dir.path()).unwrap());
        let apply = IndexStoreStateApply::new(index.clone());
        let store_id = Uuid::new_v4();

        let mut first = mutation(store_id, 1, "pane");
        first.op = StateStoreMutationOp::AccumulateRows {
            entries: vec![
                AccumulateRowEntry {
                    key: b"pane-a".to_vec(),
                    count_delta: 2,
                    sum_delta: 12,
                    named: vec![NamedKeyValue {
                        name: "pane".to_owned(),
                        values: vec![crate::index::IndexValue::I64(0)],
                        unique: false,
                    }],
                },
                AccumulateRowEntry {
                    key: b"pane-b".to_vec(),
                    count_delta: 1,
                    sum_delta: -3,
                    named: Vec::new(),
                },
            ],
        };
        apply.apply(&first).unwrap();
        let reference = |key: &[u8]| PrimaryRef {
            feed_id: store_id,
            application_key: key.to_vec(),
        };
        let row_a = index.get(&reference(b"pane-a")).unwrap().unwrap();
        assert_eq!(
            crate::index::decode_accumulator(&row_a.payload),
            Some((2, 12))
        );
        // Named postings rode the same atomic batch as the merge.
        assert_eq!(
            index
                .lookup_exact(
                    named_key_index_id(store_id, "pane"),
                    &[crate::index::IndexValue::I64(0)],
                    8,
                )
                .unwrap()
                .len(),
            1
        );

        // A restart before the applied marker replays the committed
        // mutation: the row's sequence cursor makes the re-apply a no-op,
        // never a double count.
        apply.apply(&first).unwrap();
        let row_a = index.get(&reference(b"pane-a")).unwrap().unwrap();
        assert_eq!(
            crate::index::decode_accumulator(&row_a.payload),
            Some((2, 12))
        );

        // The next committed sequence merges again — multi-page windows
        // accumulate across mutations.
        let mut second = mutation(store_id, 2, "pane");
        second.op = StateStoreMutationOp::AccumulateRows {
            entries: vec![AccumulateRowEntry {
                key: b"pane-a".to_vec(),
                count_delta: 1,
                sum_delta: 5,
                named: Vec::new(),
            }],
        };
        apply.apply(&second).unwrap();
        let row_a = index.get(&reference(b"pane-a")).unwrap().unwrap();
        assert_eq!(
            crate::index::decode_accumulator(&row_a.payload),
            Some((3, 17))
        );
    }

    #[test]
    fn accumulate_rows_bounds_refuse_unbounded_mutations() {
        let store_id = Uuid::new_v4();
        let entry = |key: &[u8]| AccumulateRowEntry {
            key: key.to_vec(),
            count_delta: 1,
            sum_delta: 1,
            named: Vec::new(),
        };
        let mut empty = mutation(store_id, 1, "x");
        empty.op = StateStoreMutationOp::AccumulateRows { entries: vec![] };
        assert!(matches!(
            validate_mutation_bounds(&empty),
            Err(StateStoreMutationError::TooLarge)
        ));
        let mut too_many = mutation(store_id, 1, "x");
        too_many.op = StateStoreMutationOp::AccumulateRows {
            entries: vec![entry(b"k"); 65],
        };
        assert!(matches!(
            validate_mutation_bounds(&too_many),
            Err(StateStoreMutationError::TooLarge)
        ));
        let mut empty_key = mutation(store_id, 1, "x");
        empty_key.op = StateStoreMutationOp::AccumulateRows {
            entries: vec![entry(b"")],
        };
        assert!(matches!(
            validate_mutation_bounds(&empty_key),
            Err(StateStoreMutationError::TooLarge)
        ));
        let mut fine = mutation(store_id, 1, "x");
        fine.op = StateStoreMutationOp::AccumulateRows {
            entries: vec![entry(b"k1"), entry(b"k2")],
        };
        validate_mutation_bounds(&fine).unwrap();
    }
}
