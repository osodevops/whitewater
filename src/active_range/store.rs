use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    codec::{decode_record, CodecError, MAX_FRAME_BYTES},
    control::HistoryPolicy,
    domain::StoredRecord,
};

use super::{
    AppendIdentity, CommitPosition, OwnershipEpoch, RangeGeneration, RangeId, RangePosition,
    RangeProgress,
};

const STATE_VERSION: u32 = 1;
const ENTRY_MAGIC: &[u8; 4] = b"WAR1";
const ENTRY_FIXED_BODY_BYTES: usize = 4 + 8 + 16 + 8 + 8 + 4 + 4 + 32;
const MAX_CURSOR_BYTES: usize = 64 * 1024;
const MAX_ENTRY_BODY_BYTES: usize = ENTRY_FIXED_BODY_BYTES + MAX_CURSOR_BYTES + MAX_FRAME_BYTES;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActiveRangeDescriptor {
    pub feed_id: Uuid,
    pub range_id: RangeId,
    pub generation: RangeGeneration,
    pub ownership_epoch: OwnershipEpoch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActiveRangeAppend {
    pub generation: RangeGeneration,
    pub ownership_epoch: OwnershipEpoch,
    pub expected_position: Option<RangePosition>,
    pub identity: AppendIdentity,
    pub cursor: String,
    pub frame: Vec<u8>,
    /// When set, the appended record joins the named effect's pending tail:
    /// durable on the replica quorum but fenced behind the committed
    /// boundary until the effect's terminal journal marker releases it.
    pub pending_effect: Option<PendingEffectRef>,
}

/// The effect an append is pending under. Only records carrying the same
/// identity may join a pending tail already held by that effect.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PendingEffectRef {
    pub effect_id: Uuid,
    pub subscription_id: Uuid,
}

/// One range's persisted pending tail: the effect holding the uncommitted
/// records at the tail plus the position and digest of its latest frame so
/// the release commit can broadcast exactly the evidence the append quorum
/// already accepted.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PendingEffect {
    pub effect_id: Uuid,
    pub subscription_id: Uuid,
    pub last_position: u64,
    pub last_frame_digest: [u8; 32],
}

impl PendingEffect {
    fn covers(&self, identity: PendingEffectRef) -> bool {
        self.effect_id == identity.effect_id && self.subscription_id == identity.subscription_id
    }
}

/// The result of discarding one effect's pending tail.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum PendingDiscardOutcome {
    /// No effect holds the tail — nothing to discard.
    NotPending,
    /// A different effect holds the tail; its journal decides the outcome.
    Occupied(PendingEffect),
    /// The marker's uncommitted tail was truncated and cleared.
    Discarded { dropped_frames: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActiveRangeAppendResult {
    pub position: RangePosition,
    pub message_id: Uuid,
    pub cursor: String,
    pub frame_digest: [u8; 32],
    pub deduplicated: bool,
    pub committed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredRangeFrame {
    pub position: RangePosition,
    pub identity: AppendIdentity,
    pub cursor: String,
    pub frame: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActiveRangeSnapshot {
    pub feed_id: Uuid,
    pub range_id: RangeId,
    pub generation: RangeGeneration,
    pub ownership_epoch: OwnershipEpoch,
    pub progress: RangeProgress,
    pub segment_count: u64,
    pub writer_count: usize,
    /// The effect whose uncommitted records currently hold the tail, if any.
    pub pending_effect: Option<PendingEffect>,
}

/// Verified remote copy of a sealed segment, implemented by
/// `active_range::tier::TierSource`. Reads consult it only for a sealed
/// segment whose local file is absent while a manifest plus tier marker
/// prove a verified remote copy — the active append path stays purely
/// local, and a Node with no object store configured serves everything it
/// holds locally and reports remote reads as unavailable.
pub trait RemoteSegmentSource: std::fmt::Debug + Send + Sync {
    /// Fetches the segment's remote object into the local cache, verifies
    /// its bytes against the manifest digest, and returns the cache path.
    /// Implementations must bound remote concurrency and deduplicate
    /// concurrent fetches of the same object.
    fn fetch_sealed(
        &self,
        directory: &Path,
        segment: u64,
        manifest: &SegmentManifest,
    ) -> Result<PathBuf, ActiveRangeStoreError>;
    /// Per-range ceiling on concurrently cached remote bytes; materialized
    /// segments beyond the budget demote back to remote coldest-first.
    fn remote_cache_bytes(&self) -> u64;
}

#[derive(Clone, Debug)]
pub struct FileActiveRangeStoreOptions {
    pub max_segment_bytes: u64,
    pub max_range_bytes: Option<u64>,
    /// Remote-read source for sealed segments whose local file is absent.
    /// `None` (the default) keeps every read local.
    pub tier: Option<std::sync::Arc<dyn RemoteSegmentSource>>,
}

impl Default for FileActiveRangeStoreOptions {
    fn default() -> Self {
        Self {
            max_segment_bytes: 256 * 1024 * 1024,
            max_range_bytes: None,
            tier: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum ActiveRangeStoreError {
    #[error("wrong RangeGeneration: current={current}, supplied={supplied}")]
    WrongGeneration {
        current: RangeGeneration,
        supplied: RangeGeneration,
    },
    #[error("stale OwnershipEpoch: current={current}, supplied={supplied}")]
    StaleOwnershipEpoch {
        current: OwnershipEpoch,
        supplied: OwnershipEpoch,
    },
    #[error("new OwnershipEpoch must be greater than the current epoch")]
    OwnershipEpochDidNotAdvance,
    #[error("Writer identity does not match the encoded record")]
    WriterIdentityMismatch,
    #[error("Writer sequence {actual} is stale; latest accepted sequence is {latest}")]
    StaleWriterSequence { actual: u64, latest: u64 },
    #[error("Writer sequence {actual} has a gap; expected {expected}")]
    WriterSequenceGap { actual: u64, expected: u64 },
    #[error("Writer sequence was reused with different record bytes; latest accepted sequence is {latest}")]
    WriterSequenceConflict { latest: u64 },
    #[error("Cursor is too large")]
    CursorTooLarge,
    #[error("RangePosition counter overflow")]
    PositionOverflow,
    #[error(
        "replica storage capacity is exhausted: required={required} bytes, limit={limit} bytes"
    )]
    DiskCapacityExceeded { required: u64, limit: u64 },
    #[error("committed frame exceeds the bounded read byte budget; streaming larger frames is not available")]
    ReadBudgetExceeded,
    #[error("remote segment read unavailable: {0}")]
    RemoteFetch(String),
    #[error("replica append has a position gap: expected={expected}, supplied={supplied}")]
    PositionGap {
        expected: RangePosition,
        supplied: RangePosition,
    },
    #[error("different bytes or append identity already occupy RangePosition {0}")]
    PositionConflict(RangePosition),
    #[error("commit position {supplied} exceeds flushed position {flushed}")]
    CommitBeyondFlushed { supplied: u64, flushed: u64 },
    #[error("commit position cannot move backwards: current={current}, supplied={supplied}")]
    CommitMovedBackwards { current: u64, supplied: u64 },
    #[error("persisted range identity does not match the requested range")]
    RangeIdentityMismatch,
    #[error("persisted range state is invalid: {0}")]
    InvalidState(String),
    #[error("segment is corrupt: {path}: {reason}")]
    CorruptSegment { path: PathBuf, reason: String },
    #[error("an immutable segment contains uncommitted records")]
    UncommittedSealedSegment,
    #[error(
        "range position {position} was deleted by history retention; retained history starts at {retained_from}"
    )]
    RetentionExpired { position: u64, retained_from: u64 },
    #[error("Active Range tail is held by pending effect {0:?}")]
    PendingEffectOccupied(PendingEffect),
    #[error("storage worker failed: {0}")]
    Worker(String),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// The ingest-time seek boundary for one Range. `Ordered` carries the
/// resolved boundary; `Unordered` tells the caller the range's ingest
/// clock is not monotonic and the boundary must be resolved by scanning.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IngestBoundary {
    /// `position`/`cursor` name the last committed frame ingested before
    /// the threshold; `None`/`""` means every committed frame is eligible.
    Ordered {
        position: Option<RangePosition>,
        cursor: String,
    },
    /// ingest_time_ns is not monotonic inside this Range's committed
    /// history; the boundary must be resolved by scanning frames.
    Unordered,
}

#[async_trait]
pub trait ActiveRangeStore: Send + Sync {
    async fn snapshot(&self) -> Result<ActiveRangeSnapshot, ActiveRangeStoreError>;
    async fn append(
        &self,
        request: ActiveRangeAppend,
    ) -> Result<ActiveRangeAppendResult, ActiveRangeStoreError>;
    async fn import_split(
        &self,
        request: ActiveRangeAppend,
    ) -> Result<ActiveRangeAppendResult, ActiveRangeStoreError>;
    /// Staged-import one ordered batch of frames under a single range lock,
    /// flushing file data and the persisted snapshot once at the end.
    /// Staged candidates are re-derivable from the split/merge source and
    /// verified across RF3 replicas before cutover, so per-frame fsync buys
    /// nothing over batch durability; a partially staged batch stays
    /// resumable because each frame dedups on position/identity/digest on
    /// the next attempt. The first failing frame aborts the batch after its
    /// already-written prefix is flushed.
    async fn import_split_batch(
        &self,
        requests: Vec<ActiveRangeAppend>,
    ) -> Result<Vec<ActiveRangeAppendResult>, ActiveRangeStoreError>;
    async fn commit(
        &self,
        generation: RangeGeneration,
        ownership_epoch: OwnershipEpoch,
        position: CommitPosition,
    ) -> Result<(), ActiveRangeStoreError>;
    async fn read_committed(
        &self,
        after: Option<RangePosition>,
        limit: usize,
    ) -> Result<Vec<StoredRangeFrame>, ActiveRangeStoreError>;
    async fn read_committed_bounded(
        &self,
        after: Option<RangePosition>,
        limit: usize,
        max_bytes: usize,
    ) -> Result<Vec<StoredRangeFrame>, ActiveRangeStoreError>;
    /// Read frames through the appended tip, including positions that have
    /// not committed. The Append Owner uses this to re-drive positions a
    /// replica never stored.
    async fn read_uncommitted(
        &self,
        after: Option<RangePosition>,
        limit: usize,
    ) -> Result<Vec<StoredRangeFrame>, ActiveRangeStoreError>;
    async fn committed_position_for_cursor(
        &self,
        cursor: &str,
    ) -> Result<Option<RangePosition>, ActiveRangeStoreError>;
    /// The next contiguous Writer sequence this Range accepts for the
    /// stream, derived from landed dedup state. The caller must serialize
    /// mints for this Range (the coordinator's per-range append lock) so a
    /// minted number cannot be offered twice; a minted sequence is only
    /// consumed when the append lands, so a failed mint never gaps the
    /// stream and every retry re-derives the same number or deduplicates.
    async fn next_writer_sequence(
        &self,
        writer_session_id: Uuid,
        writer_epoch: u64,
    ) -> Result<u64, ActiveRangeStoreError>;
    /// Resolve the ingest-time boundary inside the committed prefix: the
    /// last committed frame ingested before `threshold` (its position and
    /// Cursor), or the range-start marker when the committed head already
    /// qualifies. `Unordered` reports a range whose ingest clock is not
    /// monotonic; callers resolve those by scanning instead.
    async fn committed_ingest_boundary(
        &self,
        threshold: i64,
        boundary: CommitPosition,
    ) -> Result<IngestBoundary, ActiveRangeStoreError>;
    async fn frame_digest(
        &self,
        position: RangePosition,
    ) -> Result<Option<[u8; 32]>, ActiveRangeStoreError>;
    async fn truncate_uncommitted(
        &self,
        generation: RangeGeneration,
        ownership_epoch: OwnershipEpoch,
    ) -> Result<u64, ActiveRangeStoreError>;
    /// The effect whose records currently hold the uncommitted tail, if any.
    async fn pending_effect(&self) -> Result<Option<PendingEffect>, ActiveRangeStoreError>;
    /// Discard one effect's pending tail when `identity` names the
    /// occupant: the uncommitted frames are truncated and the marker
    /// cleared. `NotPending` means the tail already resolved;
    /// `Occupied` means a different effect holds it and must be swept.
    async fn discard_pending_effect(
        &self,
        generation: RangeGeneration,
        ownership_epoch: OwnershipEpoch,
        identity: PendingEffectRef,
    ) -> Result<PendingDiscardOutcome, ActiveRangeStoreError>;
    /// Adopt a newer committed OwnershipEpoch and stamp it durably. The
    /// uncommitted tail is left in place on purpose: it may hold frames a
    /// replica quorum already committed under the previous owner, which the
    /// recovery reconcile promotes to the verified committed prefix. A
    /// validated owner frame that conflicts with an orphaned uncommitted
    /// position truncates the rest of the tail instead.
    async fn adopt_ownership_epoch(
        &self,
        generation: RangeGeneration,
        current_epoch: OwnershipEpoch,
        new_epoch: OwnershipEpoch,
    ) -> Result<(), ActiveRangeStoreError>;
}

#[derive(Clone)]
pub struct FileActiveRangeStore {
    inner: Arc<FileActiveRangeStoreInner>,
}

struct FileActiveRangeStoreInner {
    directory: PathBuf,
    options: FileActiveRangeStoreOptions,
    /// Remote-read source consulted only for sealed segments whose local
    /// file is absent; `None` serves every read locally.
    tier: Option<Arc<dyn RemoteSegmentSource>>,
    loaded: Mutex<LoadedRange>,
    /// Set when a mutating store operation fails at the persistence layer —
    /// I/O, worker, or integrity errors — and cleared by the next successful
    /// mutation. Recovery probes read it as replica write health: a replica
    /// that cannot durably accept writes must not remain an Append Owner.
    persistence_failure: std::sync::atomic::AtomicBool,
    /// Collapses speculative prefetch storms: a read burst across cold
    /// remote spans spawns at most one in-flight read-ahead per range.
    prefetch_running: std::sync::atomic::AtomicBool,
}

struct LoadedRange {
    persisted: PersistedRangeState,
    active_file: File,
    entries: Vec<EntryIndex>,
    cursor_positions: HashMap<String, RangePosition>,
    digest_positions: HashMap<[u8; 32], RangePosition>,
    deduplication: HashMap<WriterKey, DeduplicationState>,
    /// Sealed segments whose local file is absent but whose tier marker
    /// proves a verified remote copy — the permanent remote-span registry
    /// for this range. Entries may be materialized on demand; demotion
    /// removes their spliced entries but the span stays remote forever.
    remote: BTreeMap<u64, SegmentManifest>,
    /// Remote segments currently fetched into the local cache. Their
    /// entries are spliced into `entries` and resolved through the cache
    /// path rather than the range directory.
    materialized: BTreeMap<u64, MaterializedSegment>,
    /// Sum of `MaterializedSegment::bytes` — the per-range cache budget is
    /// enforced against this, never against on-disk accounting.
    materialized_bytes: u64,
    /// Monotonic stamp for LRU ordering of `materialized`.
    use_seq: u64,
    /// True while committed ingest_time_ns is non-decreasing in position
    /// order; a backwards clock step disables indexed ingest seeks, which
    /// callers resolve by scanning instead.
    ingest_monotonic: bool,
}

/// A remote segment fetched into the tier cache: the cache file path, its
/// byte cost against the per-range budget, the last-read stamp used for
/// coldest-first demotion, and the span's own recovered entry index.
/// Remote spans never splice into `entries` — reads resolve remote
/// positions through this index, so demotion can never leave `entries`
/// sparse.
#[derive(Debug)]
struct MaterializedSegment {
    path: PathBuf,
    bytes: u64,
    last_used: u64,
    /// Recovered entries for this span only — dense and position-ordered,
    /// verified against the manifest's coverage at admission.
    entries: Vec<EntryIndex>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PersistedRangeState {
    version: u32,
    pub(crate) feed_id: Uuid,
    pub(crate) range_id: RangeId,
    pub(crate) generation: RangeGeneration,
    ownership_epoch: OwnershipEpoch,
    appended: u64,
    flushed: u64,
    committed: u64,
    pub(crate) active_segment: u64,
    /// Retention floor: positions strictly below this were deleted by the
    /// expiry/budget sweep (or an erasure). Always lands on a sealed
    /// segment boundary — `last_deleted.last_position + 1` — so leading
    /// segment files may legitimately be absent with a retention marker.
    #[serde(default)]
    pub(crate) retained_from_position: Option<u64>,
    deduplication: Vec<PersistedDeduplicationState>,
    #[serde(default)]
    pending_effect: Option<PendingEffect>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedDeduplicationState {
    writer_session_id: Uuid,
    writer_epoch: u64,
    sequence: u64,
    position: u64,
    message_id: Uuid,
    cursor: String,
    frame_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct WriterKey {
    writer_session_id: Uuid,
    writer_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DeduplicationState {
    sequence: u64,
    position: RangePosition,
    message_id: Uuid,
    cursor: String,
    frame_digest: [u8; 32],
}

#[derive(Clone, Debug)]
struct DecodedEntry {
    position: RangePosition,
    identity: AppendIdentity,
    cursor: String,
    frame: Vec<u8>,
    frame_digest: [u8; 32],
    message_id: Uuid,
    ingest_time_ns: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct EntryIndex {
    position: RangePosition,
    identity: AppendIdentity,
    cursor: String,
    frame_digest: [u8; 32],
    message_id: Uuid,
    segment: u64,
    entry_end: u64,
    frame_offset: u64,
    frame_length: u64,
    ingest_time_ns: i64,
}

impl FileActiveRangeStore {
    pub fn open(
        root: impl AsRef<Path>,
        descriptor: ActiveRangeDescriptor,
    ) -> Result<Self, ActiveRangeStoreError> {
        Self::open_with_options(root, descriptor, FileActiveRangeStoreOptions::default())
    }

    pub fn open_with_options(
        root: impl AsRef<Path>,
        descriptor: ActiveRangeDescriptor,
        options: FileActiveRangeStoreOptions,
    ) -> Result<Self, ActiveRangeStoreError> {
        if options.max_segment_bytes == 0 {
            return Err(ActiveRangeStoreError::InvalidState(
                "max_segment_bytes must be greater than zero".to_owned(),
            ));
        }
        let directory = range_directory(root.as_ref(), &descriptor);
        fs::create_dir_all(&directory)?;
        let state_path = directory.join("range-state.json");
        let mut persisted = if state_path.exists() {
            serde_json::from_slice::<PersistedRangeState>(&fs::read(&state_path)?)?
        } else {
            PersistedRangeState::new(&descriptor)
        };
        validate_persisted_identity(&persisted, &descriptor)?;
        if persisted.ownership_epoch < descriptor.ownership_epoch {
            persisted.ownership_epoch = descriptor.ownership_epoch;
        }
        let (entries, recovered_deduplication, remote) = recover_segments(&directory, &persisted)?;
        // Remote-only sealed positions still exist — a Writer whose dedup
        // high-water landed in a tiered segment keeps its state, or a stale
        // sequence would replay a second copy of committed history.
        let mut surviving_positions = entries
            .iter()
            .map(|entry| entry.position.value())
            .collect::<std::collections::HashSet<_>>();
        for manifest in remote.values() {
            surviving_positions.extend(manifest.first_position..=manifest.last_position);
        }
        let recovered_deduplication = merge_deduplication(
            recovered_deduplication,
            persisted.deduplication.clone(),
            &surviving_positions,
        );
        let cursor_positions = entries
            .iter()
            .map(|entry| (entry.cursor.clone(), entry.position))
            .collect::<HashMap<_, _>>();
        if cursor_positions.len() != entries.len() {
            return Err(ActiveRangeStoreError::InvalidState(
                "duplicate Cursor identity".to_owned(),
            ));
        }
        let digest_positions = entries
            .iter()
            .map(|entry| (entry.frame_digest, entry.position))
            .collect::<HashMap<_, _>>();
        if digest_positions.len() != entries.len() {
            return Err(ActiveRangeStoreError::InvalidState(
                "duplicate frame digest identity".to_owned(),
            ));
        }
        // Ingest order is provable only where entry bytes were recovered
        // locally; a still-remote gap makes the claim unprovable until the
        // span materializes and recomputes the check.
        let ingest_monotonic = remote.is_empty()
            && entries
                .windows(2)
                .all(|pair| pair[0].ingest_time_ns <= pair[1].ingest_time_ns);
        let recovered_position = entries
            .last()
            .map(|entry| entry.position.value())
            .into_iter()
            .chain(remote.values().map(|manifest| manifest.last_position))
            .max()
            .unwrap_or(0);
        if persisted.committed > recovered_position {
            return Err(ActiveRangeStoreError::InvalidState(format!(
                "commit position {} exceeds recovered position {recovered_position}",
                persisted.committed
            )));
        }
        persisted.appended = recovered_position;
        persisted.flushed = recovered_position;
        persisted.deduplication = persisted_deduplication(&recovered_deduplication);
        if persisted
            .pending_effect
            .as_ref()
            .is_some_and(|pending| pending.last_position > recovered_position)
        {
            // A pending marker that reaches beyond the recovered tail can
            // never be released honestly; the frames it named are gone, so
            // the effect journal's replay decides the outcome instead.
            persisted.pending_effect = None;
        }
        persist_state(&directory, &persisted)?;
        let active_file = open_segment(&directory, persisted.active_segment)?;
        Ok(Self {
            inner: Arc::new(FileActiveRangeStoreInner {
                directory,
                tier: options.tier.clone(),
                options,
                persistence_failure: std::sync::atomic::AtomicBool::new(false),
                prefetch_running: std::sync::atomic::AtomicBool::new(false),
                loaded: Mutex::new(LoadedRange {
                    persisted,
                    active_file,
                    entries,
                    cursor_positions,
                    digest_positions,
                    deduplication: recovered_deduplication,
                    remote,
                    materialized: BTreeMap::new(),
                    materialized_bytes: 0,
                    use_seq: 0,
                    ingest_monotonic,
                }),
            }),
        })
    }

    async fn blocking<T, F>(&self, operation: F) -> Result<T, ActiveRangeStoreError>
    where
        T: Send + 'static,
        F: FnOnce(&FileActiveRangeStoreInner) -> Result<T, ActiveRangeStoreError> + Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || operation(&inner))
            .await
            .map_err(|error| ActiveRangeStoreError::Worker(error.to_string()))?
    }

    /// Same as `blocking`, but the closure sees the inner Arc so work it
    /// starts — like read-ahead prefetch — can outlive the call.
    async fn blocking_arc<T, F>(&self, operation: F) -> Result<T, ActiveRangeStoreError>
    where
        T: Send + 'static,
        F: FnOnce(&Arc<FileActiveRangeStoreInner>) -> Result<T, ActiveRangeStoreError>
            + Send
            + 'static,
    {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || operation(&inner))
            .await
            .map_err(|error| ActiveRangeStoreError::Worker(error.to_string()))?
    }

    /// Runs a mutating store operation and records replica write health: a
    /// persistence-level failure marks the replica unable to durably accept
    /// writes until some mutation succeeds again, while logical rejections
    /// never degrade it.
    async fn blocking_mutation<T, F>(&self, operation: F) -> Result<T, ActiveRangeStoreError>
    where
        T: Send + 'static,
        F: FnOnce(&FileActiveRangeStoreInner) -> Result<T, ActiveRangeStoreError> + Send + 'static,
    {
        let result = self.blocking(operation).await;
        match &result {
            Ok(_) => self
                .inner
                .persistence_failure
                .store(false, std::sync::atomic::Ordering::Relaxed),
            Err(error) if store_error_is_persistence_failure(error) => self
                .inner
                .persistence_failure
                .store(true, std::sync::atomic::Ordering::Relaxed),
            Err(_) => {}
        }
        result
    }

    /// Applies the Feed's effective History Policy: deletes sealed spans
    /// past `expire_after_ns` or over `max_retained_bytes` as one
    /// contiguous prefix, advancing the retained floor durably. Runs as a
    /// store mutation — the floor, in-memory indexes, and on-disk state
    /// move atomically; remote object deletion uses the supplied store
    /// when configured.
    pub(crate) async fn apply_retention(
        &self,
        policy: &HistoryPolicy,
        objects: Option<Arc<dyn super::tier::ObjectStore>>,
        now: i64,
    ) -> Result<ExpireReport, ActiveRangeStoreError> {
        let policy = policy.clone();
        self.blocking_mutation(move |inner| expire_loaded(inner, &policy, objects.as_deref(), now))
            .await
    }

    /// True when the last mutating store operation failed at the persistence
    /// layer and no mutation has succeeded since.
    pub fn persistence_failure(&self) -> bool {
        self.inner
            .persistence_failure
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Rehashes every sealed segment against its persisted manifest and
    /// returns the manifests in segment order — the fail-closed integrity
    /// proof the object-store upload path runs before shipping bytes
    /// off-Node. Segments still open for append are excluded by
    /// definition: the active tail is mutable and carries no manifest.
    /// Test-only until the uploader lands as the production caller.
    #[cfg(test)]
    pub(crate) async fn verify_sealed_segments(
        &self,
    ) -> Result<Vec<SegmentManifest>, ActiveRangeStoreError> {
        self.blocking(|inner| {
            let (persisted, remote) = {
                let loaded = inner
                    .loaded
                    .lock()
                    .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
                (loaded.persisted.clone(), loaded.remote.clone())
            };
            let mut manifests = Vec::new();
            for segment in 0..persisted.active_segment {
                // A demoted segment has no local bytes to rehash; its
                // stored manifest is the provenance record the uploader
                // already verified, so report it rather than failing the
                // scan on an absent file.
                if let Some(manifest) = remote.get(&segment) {
                    manifests.push(manifest.clone());
                } else {
                    manifests.push(verify_sealed_segment(
                        &inner.directory,
                        segment,
                        &persisted,
                    )?);
                }
            }
            Ok(manifests)
        })
        .await
    }
}

/// Errors that mean the replica cannot durably accept writes — disk
/// failures, worker failures, or integrity violations — as opposed to
/// logical rejections such as stale epochs, sequence gaps, fencing, or
/// declared bounds, which say nothing about the replica's ability to persist
/// and must not make it unhealthy. `DiskCapacityExceeded` stays logical: it
/// also fires when one oversized frame exceeds the bound on an empty range,
/// which is request poison rather than replica sickness.
fn store_error_is_persistence_failure(error: &ActiveRangeStoreError) -> bool {
    matches!(
        error,
        ActiveRangeStoreError::Io(_)
            | ActiveRangeStoreError::Worker(_)
            | ActiveRangeStoreError::CorruptSegment { .. }
            | ActiveRangeStoreError::UncommittedSealedSegment
            | ActiveRangeStoreError::InvalidState(_)
    )
}

#[async_trait]
impl ActiveRangeStore for FileActiveRangeStore {
    async fn snapshot(&self) -> Result<ActiveRangeSnapshot, ActiveRangeStoreError> {
        self.blocking(|inner| {
            let loaded = inner
                .loaded
                .lock()
                .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
            snapshot(&loaded)
        })
        .await
    }

    async fn append(
        &self,
        request: ActiveRangeAppend,
    ) -> Result<ActiveRangeAppendResult, ActiveRangeStoreError> {
        self.blocking_mutation(move |inner| append(inner, request, false))
            .await
    }

    async fn import_split(
        &self,
        request: ActiveRangeAppend,
    ) -> Result<ActiveRangeAppendResult, ActiveRangeStoreError> {
        self.blocking_mutation(move |inner| append(inner, request, true))
            .await
    }

    async fn import_split_batch(
        &self,
        requests: Vec<ActiveRangeAppend>,
    ) -> Result<Vec<ActiveRangeAppendResult>, ActiveRangeStoreError> {
        self.blocking_mutation(move |inner| {
            prematerialize_for_appends(inner, &requests)?;
            let mut loaded = inner
                .loaded
                .lock()
                .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
            let mut results = Vec::with_capacity(requests.len());
            for request in requests {
                if request.cursor.len() > MAX_CURSOR_BYTES {
                    return Err(ActiveRangeStoreError::CursorTooLarge);
                }
                let record = decode_record(&request.frame)?;
                if record.producer_id != request.identity.writer_session_id
                    || record.producer_sequence != request.identity.sequence
                {
                    return Err(ActiveRangeStoreError::WriterIdentityMismatch);
                }
                match append_locked(inner, &mut loaded, request, true, false, &record) {
                    Ok(result) => results.push(result),
                    Err(error) => {
                        // Flush whatever the batch staged before reporting the
                        // failure so a retry resumes from deduped positions
                        // instead of restaging the prefix.
                        loaded.active_file.sync_data()?;
                        persist_state(&inner.directory, &loaded.persisted)?;
                        return Err(error);
                    }
                }
            }
            loaded.active_file.sync_data()?;
            persist_state(&inner.directory, &loaded.persisted)?;
            Ok(results)
        })
        .await
    }

    async fn commit(
        &self,
        generation: RangeGeneration,
        ownership_epoch: OwnershipEpoch,
        position: CommitPosition,
    ) -> Result<(), ActiveRangeStoreError> {
        self.blocking_mutation(move |inner| commit(inner, generation, ownership_epoch, position))
            .await
    }

    async fn read_committed(
        &self,
        after: Option<RangePosition>,
        limit: usize,
    ) -> Result<Vec<StoredRangeFrame>, ActiveRangeStoreError> {
        self.blocking_arc(move |inner| read_committed(inner, after, limit))
            .await
    }

    async fn read_committed_bounded(
        &self,
        after: Option<RangePosition>,
        limit: usize,
        max_bytes: usize,
    ) -> Result<Vec<StoredRangeFrame>, ActiveRangeStoreError> {
        self.blocking_arc(move |inner| read_committed_bounded(inner, after, limit, max_bytes))
            .await
    }

    async fn read_uncommitted(
        &self,
        after: Option<RangePosition>,
        limit: usize,
    ) -> Result<Vec<StoredRangeFrame>, ActiveRangeStoreError> {
        self.blocking_arc(move |inner| read_uncommitted(inner, after, limit))
            .await
    }

    async fn committed_position_for_cursor(
        &self,
        cursor: &str,
    ) -> Result<Option<RangePosition>, ActiveRangeStoreError> {
        let cursor = cursor.to_owned();
        self.blocking(move |inner| {
            // A Cursor whose frame sits on a demoted segment resolves only
            // after the span materializes — the position index survives
            // demotion, but it is populated from entries, so an uncached
            // remote span reports absent until fetched.
            loop {
                let next = {
                    let loaded = inner.loaded.lock().map_err(|_| {
                        ActiveRangeStoreError::Worker("range lock poisoned".to_owned())
                    })?;
                    if let Some(position) = loaded
                        .cursor_positions
                        .get(&cursor)
                        .copied()
                        .filter(|position| position.value() <= loaded.persisted.committed)
                    {
                        let floor = loaded.persisted.retained_from_position.unwrap_or(1).max(1);
                        if position.value() < floor {
                            return Err(ActiveRangeStoreError::RetentionExpired {
                                position: position.value(),
                                retained_from: floor,
                            });
                        }
                        return Ok(Some(position));
                    }
                    loaded
                        .remote
                        .values()
                        .find(|manifest| !loaded.materialized.contains_key(&manifest.segment))
                        .cloned()
                };
                match next {
                    Some(manifest) => materialize_remote(inner, &manifest)?,
                    None => return Ok(None),
                }
            }
        })
        .await
    }

    async fn next_writer_sequence(
        &self,
        writer_session_id: Uuid,
        writer_epoch: u64,
    ) -> Result<u64, ActiveRangeStoreError> {
        self.blocking(move |inner| {
            let loaded = inner
                .loaded
                .lock()
                .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
            let key = WriterKey {
                writer_session_id,
                writer_epoch,
            };
            match loaded.deduplication.get(&key) {
                Some(state) => state
                    .sequence
                    .checked_add(1)
                    .ok_or(ActiveRangeStoreError::PositionOverflow),
                None => Ok(1),
            }
        })
        .await
    }

    async fn committed_ingest_boundary(
        &self,
        threshold: i64,
        boundary: CommitPosition,
    ) -> Result<IngestBoundary, ActiveRangeStoreError> {
        self.blocking(move |inner| {
            let loaded = inner
                .loaded
                .lock()
                .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
            if !loaded.ingest_monotonic {
                return Ok(IngestBoundary::Unordered);
            }
            let upto = usize::try_from(boundary.value())
                .unwrap_or(usize::MAX)
                .min(loaded.entries.len());
            let prefix = &loaded.entries[..upto];
            // partition_point counts committed frames ingested before the
            // threshold; its predecessor is the boundary frame.
            let first_eligible = prefix.partition_point(|entry| entry.ingest_time_ns < threshold);
            let boundary_frame = first_eligible.checked_sub(1).map(|index| &prefix[index]);
            Ok(IngestBoundary::Ordered {
                position: boundary_frame.map(|entry| entry.position),
                cursor: boundary_frame.map_or_else(String::new, |entry| entry.cursor.clone()),
            })
        })
        .await
    }

    async fn frame_digest(
        &self,
        position: RangePosition,
    ) -> Result<Option<[u8; 32]>, ActiveRangeStoreError> {
        self.blocking(move |inner| {
            let loaded = inner
                .loaded
                .lock()
                .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
            Ok(loaded
                .entries
                .iter()
                .find(|entry| entry.position == position)
                .map(|entry| entry.frame_digest))
        })
        .await
    }

    async fn truncate_uncommitted(
        &self,
        generation: RangeGeneration,
        ownership_epoch: OwnershipEpoch,
    ) -> Result<u64, ActiveRangeStoreError> {
        self.blocking_mutation(move |inner| {
            truncate_uncommitted(inner, generation, ownership_epoch)
        })
        .await
    }

    async fn pending_effect(&self) -> Result<Option<PendingEffect>, ActiveRangeStoreError> {
        self.blocking(|inner| {
            let loaded = inner
                .loaded
                .lock()
                .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
            Ok(loaded.persisted.pending_effect.clone())
        })
        .await
    }

    async fn discard_pending_effect(
        &self,
        generation: RangeGeneration,
        ownership_epoch: OwnershipEpoch,
        identity: PendingEffectRef,
    ) -> Result<PendingDiscardOutcome, ActiveRangeStoreError> {
        self.blocking_mutation(move |inner| {
            {
                let loaded = inner
                    .loaded
                    .lock()
                    .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
                match &loaded.persisted.pending_effect {
                    None => return Ok(PendingDiscardOutcome::NotPending),
                    Some(marker) if !marker.covers(identity) => {
                        return Ok(PendingDiscardOutcome::Occupied(marker.clone()))
                    }
                    _ => {}
                }
            }
            // Held frames are uncommitted by construction, so the shared
            // truncation path removes exactly the pending tail and clears
            // the marker atomically with the persisted range state.
            let dropped = truncate_uncommitted(inner, generation, ownership_epoch)?;
            Ok(PendingDiscardOutcome::Discarded {
                dropped_frames: dropped,
            })
        })
        .await
    }

    async fn adopt_ownership_epoch(
        &self,
        generation: RangeGeneration,
        current_epoch: OwnershipEpoch,
        new_epoch: OwnershipEpoch,
    ) -> Result<(), ActiveRangeStoreError> {
        self.blocking_mutation(move |inner| {
            {
                let loaded = inner
                    .loaded
                    .lock()
                    .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
                if loaded.persisted.generation != generation {
                    return Err(ActiveRangeStoreError::WrongGeneration {
                        current: loaded.persisted.generation,
                        supplied: generation,
                    });
                }
                if loaded.persisted.ownership_epoch == new_epoch {
                    return Ok(());
                }
                if loaded.persisted.ownership_epoch != current_epoch {
                    return Err(ActiveRangeStoreError::StaleOwnershipEpoch {
                        current: loaded.persisted.ownership_epoch,
                        supplied: current_epoch,
                    });
                }
            }
            if new_epoch <= current_epoch {
                return Err(ActiveRangeStoreError::OwnershipEpochDidNotAdvance);
            }
            let mut loaded = inner
                .loaded
                .lock()
                .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
            let mut next = loaded.persisted.clone();
            next.ownership_epoch = new_epoch;
            persist_state(&inner.directory, &next)?;
            loaded.persisted = next;
            Ok(())
        })
        .await
    }
}

impl PersistedRangeState {
    fn new(descriptor: &ActiveRangeDescriptor) -> Self {
        Self {
            version: STATE_VERSION,
            feed_id: descriptor.feed_id,
            range_id: descriptor.range_id,
            generation: descriptor.generation,
            ownership_epoch: descriptor.ownership_epoch,
            appended: 0,
            flushed: 0,
            committed: 0,
            active_segment: 0,
            retained_from_position: None,
            deduplication: Vec::new(),
            pending_effect: None,
        }
    }
}

fn range_directory(root: &Path, descriptor: &ActiveRangeDescriptor) -> PathBuf {
    root.join(descriptor.feed_id.to_string())
        .join(descriptor.range_id.to_string())
        .join(format!("generation-{}", descriptor.generation.value()))
}

pub(crate) fn segment_path(directory: &Path, segment: u64) -> PathBuf {
    directory.join(format!("segment-{segment:06}.log"))
}

fn range_segment_bytes(directory: &Path, active_segment: u64) -> Result<u64, std::io::Error> {
    let mut total = 0_u64;
    for segment in 0..=active_segment {
        let path = segment_path(directory, segment);
        if path.exists() {
            total = total.saturating_add(path.metadata()?.len());
        }
    }
    Ok(total)
}

fn open_segment(directory: &Path, segment: u64) -> Result<File, std::io::Error> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .append(true)
        .open(segment_path(directory, segment))
}

fn validate_persisted_identity(
    persisted: &PersistedRangeState,
    descriptor: &ActiveRangeDescriptor,
) -> Result<(), ActiveRangeStoreError> {
    if persisted.version != STATE_VERSION {
        return Err(ActiveRangeStoreError::InvalidState(format!(
            "unsupported state version {}",
            persisted.version
        )));
    }
    if persisted.feed_id != descriptor.feed_id
        || persisted.range_id != descriptor.range_id
        || persisted.generation != descriptor.generation
        || persisted.ownership_epoch > descriptor.ownership_epoch
    {
        return Err(ActiveRangeStoreError::RangeIdentityMismatch);
    }
    if persisted.committed > persisted.flushed || persisted.flushed > persisted.appended {
        return Err(ActiveRangeStoreError::InvalidState(
            "persisted positions cross durability boundaries".to_owned(),
        ));
    }
    Ok(())
}

fn validate_generation_and_epoch(
    persisted: &PersistedRangeState,
    generation: RangeGeneration,
    ownership_epoch: OwnershipEpoch,
) -> Result<(), ActiveRangeStoreError> {
    if generation != persisted.generation {
        return Err(ActiveRangeStoreError::WrongGeneration {
            current: persisted.generation,
            supplied: generation,
        });
    }
    if ownership_epoch != persisted.ownership_epoch {
        return Err(ActiveRangeStoreError::StaleOwnershipEpoch {
            current: persisted.ownership_epoch,
            supplied: ownership_epoch,
        });
    }
    Ok(())
}

fn snapshot(loaded: &LoadedRange) -> Result<ActiveRangeSnapshot, ActiveRangeStoreError> {
    let mut progress = RangeProgress::new();
    progress
        .advance_appended(RangePosition::new(loaded.persisted.appended))
        .map_err(|error| ActiveRangeStoreError::InvalidState(error.to_string()))?;
    progress
        .advance_flushed(RangePosition::new(loaded.persisted.flushed))
        .map_err(|error| ActiveRangeStoreError::InvalidState(error.to_string()))?;
    progress
        .advance_committed(CommitPosition::new(loaded.persisted.committed))
        .map_err(|error| ActiveRangeStoreError::InvalidState(error.to_string()))?;
    progress
        .advance_visible(RangePosition::new(loaded.persisted.committed))
        .map_err(|error| ActiveRangeStoreError::InvalidState(error.to_string()))?;
    Ok(ActiveRangeSnapshot {
        feed_id: loaded.persisted.feed_id,
        range_id: loaded.persisted.range_id,
        generation: loaded.persisted.generation,
        ownership_epoch: loaded.persisted.ownership_epoch,
        progress,
        segment_count: loaded.persisted.active_segment + 1,
        writer_count: loaded.deduplication.len(),
        pending_effect: loaded.persisted.pending_effect.clone(),
    })
}

/// Remote spans holding dedup evidence for `requests`. Expected
/// positions, frame digests, request cursors, and writer-sequence
/// high-water marks can all point at a demoted sealed segment; the
/// entries.find lookups inside `append_locked` must see them as dense
/// history or an idempotent retry degrades into a false PositionConflict.
/// Fetching happens here — before the write lock — so the byte-level
/// dedup comparisons still run against real frame content.
fn prematerialize_for_appends(
    inner: &FileActiveRangeStoreInner,
    requests: &[ActiveRangeAppend],
) -> Result<(), ActiveRangeStoreError> {
    loop {
        let needed = {
            let loaded = inner
                .loaded
                .lock()
                .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
            let mut needed: Vec<SegmentManifest> = Vec::new();
            let mut consider = |position: RangePosition| {
                let position = position.value();
                if let Some(manifest) = loaded.remote.values().find(|manifest| {
                    !loaded.materialized.contains_key(&manifest.segment)
                        && manifest.first_position <= position
                        && position <= manifest.last_position
                }) {
                    if needed
                        .iter()
                        .all(|span: &SegmentManifest| span.segment != manifest.segment)
                    {
                        needed.push(manifest.clone());
                    }
                }
            };
            for request in requests {
                if let Some(supplied) = request
                    .expected_position
                    .filter(|supplied| supplied.value() <= loaded.persisted.appended)
                {
                    consider(supplied);
                }
                let frame_digest = blake3::hash(&request.frame);
                if let Some(position) = loaded
                    .digest_positions
                    .get(frame_digest.as_bytes())
                    .copied()
                {
                    consider(position);
                }
                if let Some(position) = loaded.cursor_positions.get(&request.cursor).copied() {
                    consider(position);
                }
                let writer_key = WriterKey {
                    writer_session_id: request.identity.writer_session_id,
                    writer_epoch: request.identity.writer_epoch,
                };
                if let Some(deduplication) = loaded.deduplication.get(&writer_key) {
                    consider(deduplication.position);
                }
            }
            needed
        };
        if needed.is_empty() {
            return Ok(());
        }
        for manifest in needed {
            materialize_remote(inner, &manifest)?;
        }
    }
}

fn append(
    inner: &FileActiveRangeStoreInner,
    request: ActiveRangeAppend,
    import: bool,
) -> Result<ActiveRangeAppendResult, ActiveRangeStoreError> {
    if request.cursor.len() > MAX_CURSOR_BYTES {
        return Err(ActiveRangeStoreError::CursorTooLarge);
    }
    let record = decode_record(&request.frame)?;
    if record.producer_id != request.identity.writer_session_id
        || record.producer_sequence != request.identity.sequence
    {
        return Err(ActiveRangeStoreError::WriterIdentityMismatch);
    }
    prematerialize_for_appends(inner, std::slice::from_ref(&request))?;
    let mut loaded = inner
        .loaded
        .lock()
        .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
    append_locked(inner, &mut loaded, request, import, true, &record)
}

/// The dedup, validation, write, and index body of `append` with the caller
/// already holding the range lock. `flush` controls durability: `true` runs
/// the single-frame contract (file sync + persisted-state flush before the
/// call returns); `false` is for batched staged imports, which defer the
/// sync/state flush to the batch end because staged candidate frames are
/// re-derivable from the source and a partial batch stays resumable through
/// the same dedup checks.
fn append_locked(
    inner: &FileActiveRangeStoreInner,
    loaded: &mut LoadedRange,
    request: ActiveRangeAppend,
    import: bool,
    flush: bool,
    record: &StoredRecord,
) -> Result<ActiveRangeAppendResult, ActiveRangeStoreError> {
    let frame_digest = *blake3::hash(&request.frame).as_bytes();
    let writer_key = WriterKey {
        writer_session_id: request.identity.writer_session_id,
        writer_epoch: request.identity.writer_epoch,
    };
    validate_generation_and_epoch(
        &loaded.persisted,
        request.generation,
        request.ownership_epoch,
    )?;
    // A different frame already occupying a supplied uncommitted position
    // is a superseded-epoch orphan: the caller passed the committed
    // placement's owner/generation/epoch validation, so its frame is
    // authoritative for the uncommitted tail. Dropping the tail here lets
    // the checks below replay the frame instead of wedging the replica;
    // occupants at or below the committed boundary still refuse through
    // PositionConflict.
    if let Some(supplied) = request.expected_position {
        if supplied.value() > loaded.persisted.committed
            && loaded.entries.iter().any(|entry| {
                entry.position == supplied
                    && (entry.identity != request.identity || entry.frame_digest != frame_digest)
            })
        {
            truncate_uncommitted_locked(
                inner,
                loaded,
                request.generation,
                request.ownership_epoch,
            )?;
        }
    }
    let next_position = RangePosition::new(
        loaded
            .persisted
            .appended
            .checked_add(1)
            .ok_or(ActiveRangeStoreError::PositionOverflow)?,
    );
    if let Some(supplied) = request.expected_position {
        if supplied < next_position {
            let existing = entry_at(loaded, supplied)
                .ok_or(ActiveRangeStoreError::PositionConflict(supplied))?;
            if existing.identity != request.identity || existing.frame_digest != frame_digest {
                tracing::warn!(
                    position = supplied.value(),
                    committed = loaded.persisted.committed,
                    ownership_epoch = ?request.ownership_epoch,
                    supplied_session = %request.identity.writer_session_id,
                    supplied_epoch = request.identity.writer_epoch,
                    supplied_sequence = request.identity.sequence,
                    supplied_digest = ?frame_digest,
                    occupant_session = %existing.identity.writer_session_id,
                    occupant_epoch = existing.identity.writer_epoch,
                    occupant_sequence = existing.identity.sequence,
                    occupant_digest = ?existing.frame_digest,
                    "Active Range position conflict detail"
                );
                return Err(ActiveRangeStoreError::PositionConflict(supplied));
            }
            return Ok(ActiveRangeAppendResult {
                position: existing.position,
                message_id: existing.message_id,
                cursor: existing.cursor.clone(),
                frame_digest,
                deduplicated: true,
                committed: existing.position.value() <= loaded.persisted.committed,
            });
        }
        if supplied > next_position {
            return Err(ActiveRangeStoreError::PositionGap {
                expected: next_position,
                supplied,
            });
        }
    }
    if let Some(position) = loaded.digest_positions.get(&frame_digest).copied() {
        let existing =
            entry_at(loaded, position).ok_or(ActiveRangeStoreError::PositionConflict(position))?;
        return Ok(ActiveRangeAppendResult {
            position: existing.position,
            message_id: existing.message_id,
            cursor: existing.cursor.clone(),
            frame_digest,
            deduplicated: true,
            committed: existing.position.value() <= loaded.persisted.committed,
        });
    }
    if let Some(existing_position) = loaded.cursor_positions.get(&request.cursor).copied() {
        // The Cursor derives from (FeedId, request_id), so a hit means this
        // request already landed. Resequence after a split or merge can hand
        // the retry a fresh sequence — digest and writer dedup miss it — but
        // the request-derived fields stay identical; return the original
        // position instead of appending a duplicate. This check runs before
        // the writer-sequence fence: a known Cursor is proof of the request's
        // own earlier append, while a re-sequenced retry carries a stale or
        // future sequence that would otherwise wedge the retry as a
        // StaleWriterSequence against the stream's high-water mark.
        let existing = entry_at(loaded, existing_position)
            .ok_or(ActiveRangeStoreError::PositionConflict(existing_position))?;
        let existing_frame = read_indexed_frame(&inner.directory, loaded, &existing)?;
        let existing_record = decode_record(&existing_frame.frame)?;
        if same_request_record(&existing_record, record) {
            // The API layer allocated this retry a fresh sequence before the
            // Cursor dedup caught it; that number is consumed even though no
            // record lands. Record it against the original position so the
            // stream's high-water mark stays contiguous across retries and
            // restarts, and a replay of the same allocated sequence dedupes
            // on its request-derived fields above.
            let landed_position = existing.position;
            let landed_message_id = existing.message_id;
            let landed_cursor = existing.cursor.clone();
            let landed_digest = existing.frame_digest;
            let landed_committed = landed_position.value() <= loaded.persisted.committed;
            let consumed = DeduplicationState {
                sequence: request.identity.sequence,
                position: landed_position,
                message_id: landed_message_id,
                cursor: landed_cursor.clone(),
                frame_digest,
            };
            let advanced = loaded
                .deduplication
                .get(&writer_key)
                .is_none_or(|previous| previous.sequence < consumed.sequence);
            if advanced {
                loaded.deduplication.insert(writer_key, consumed);
                let mut next = loaded.persisted.clone();
                next.deduplication = persisted_deduplication(&loaded.deduplication);
                if flush {
                    persist_state(&inner.directory, &next)?;
                }
                loaded.persisted = next;
            }
            return Ok(ActiveRangeAppendResult {
                position: landed_position,
                message_id: landed_message_id,
                cursor: landed_cursor,
                frame_digest: landed_digest,
                deduplicated: true,
                committed: landed_committed,
            });
        }
        return Err(ActiveRangeStoreError::InvalidState(
            "Cursor already belongs to another position".to_owned(),
        ));
    }
    if !import {
        if let Some(previous) = loaded.deduplication.get(&writer_key) {
            if request.identity.sequence == previous.sequence {
                if frame_digest != previous.frame_digest {
                    let existing = entry_at(loaded, previous.position).ok_or(
                        ActiveRangeStoreError::WriterSequenceConflict {
                            latest: previous.sequence,
                        },
                    )?;
                    let existing_frame = read_indexed_frame(&inner.directory, loaded, &existing)?;
                    let existing_record = decode_record(&existing_frame.frame)?;
                    if !same_request_record(&existing_record, record) {
                        return Err(ActiveRangeStoreError::WriterSequenceConflict {
                            latest: previous.sequence,
                        });
                    }
                }
                return Ok(ActiveRangeAppendResult {
                    position: previous.position,
                    message_id: previous.message_id,
                    cursor: previous.cursor.clone(),
                    frame_digest: previous.frame_digest,
                    deduplicated: true,
                    committed: previous.position.value() <= loaded.persisted.committed,
                });
            }
            if request.identity.sequence < previous.sequence {
                return Err(ActiveRangeStoreError::StaleWriterSequence {
                    actual: request.identity.sequence,
                    latest: previous.sequence,
                });
            }
            let expected = previous
                .sequence
                .checked_add(1)
                .ok_or(ActiveRangeStoreError::PositionOverflow)?;
            if request.identity.sequence != expected {
                return Err(ActiveRangeStoreError::WriterSequenceGap {
                    actual: request.identity.sequence,
                    expected,
                });
            }
        }
    }
    match (&loaded.persisted.pending_effect, request.pending_effect) {
        (Some(pending), Some(requested)) if pending.covers(requested) => {}
        (Some(pending), _) => {
            return Err(ActiveRangeStoreError::PendingEffectOccupied(
                pending.clone(),
            ));
        }
        (None, _) => {}
    }
    let position = next_position;
    let decoded = DecodedEntry {
        position,
        identity: request.identity,
        cursor: request.cursor,
        frame: request.frame,
        frame_digest,
        message_id: record.message_id,
        ingest_time_ns: record.ingest_time_ns,
    };
    let encoded = encode_entry(&decoded)?;
    if let Some(limit) = inner.options.max_range_bytes {
        let required = range_segment_bytes(&inner.directory, loaded.persisted.active_segment)?
            .saturating_add(encoded.len() as u64);
        if required > limit {
            return Err(ActiveRangeStoreError::DiskCapacityExceeded { required, limit });
        }
    }
    rotate_if_needed(inner, loaded, encoded.len() as u64)?;
    let entry_start = loaded.active_file.seek(SeekFrom::End(0))?;
    loaded.active_file.write_all(&encoded)?;
    if flush {
        loaded.active_file.sync_data()?;
    }
    let entry_end = entry_start
        .checked_add(encoded.len() as u64)
        .ok_or(ActiveRangeStoreError::PositionOverflow)?;
    let frame_offset = entry_start
        .checked_add(frame_offset_in_entry(&decoded)? as u64)
        .ok_or(ActiveRangeStoreError::PositionOverflow)?;
    let index = EntryIndex {
        position,
        identity: decoded.identity.clone(),
        cursor: decoded.cursor.clone(),
        frame_digest,
        message_id: decoded.message_id,
        segment: loaded.persisted.active_segment,
        entry_end,
        frame_offset,
        frame_length: decoded.frame.len() as u64,
        ingest_time_ns: decoded.ingest_time_ns,
    };
    let deduplication = DeduplicationState {
        sequence: decoded.identity.sequence,
        position,
        message_id: decoded.message_id,
        cursor: decoded.cursor.clone(),
        frame_digest,
    };
    loaded
        .cursor_positions
        .insert(index.cursor.clone(), position);
    loaded.digest_positions.insert(frame_digest, position);
    loaded.ingest_monotonic = loaded.ingest_monotonic
        && loaded
            .entries
            .last()
            .is_none_or(|tail| tail.ingest_time_ns <= index.ingest_time_ns);
    loaded.entries.push(index);
    match loaded.deduplication.get_mut(&writer_key) {
        Some(existing) if existing.sequence >= deduplication.sequence => {}
        _ => {
            loaded.deduplication.insert(writer_key, deduplication);
        }
    }
    let mut next = loaded.persisted.clone();
    next.appended = position.value();
    next.flushed = position.value();
    next.deduplication = persisted_deduplication(&loaded.deduplication);
    if let Some(requested) = request.pending_effect {
        next.pending_effect = Some(PendingEffect {
            effect_id: requested.effect_id,
            subscription_id: requested.subscription_id,
            last_position: position.value(),
            last_frame_digest: frame_digest,
        });
    }
    if flush {
        // A pending-effect marker must be crash-durable with its frames; a
        // plain append's dedup/appended/flushed bookkeeping is recomputed
        // by recovery rescan, so it takes the checkpoint-only write.
        let persistence = if next.pending_effect.is_some() {
            persist_state(&inner.directory, &next)
        } else {
            persist_state_lazy(&inner.directory, &next)
        };
        loaded.persisted = next;
        persistence?;
    } else {
        loaded.persisted = next;
    }
    Ok(ActiveRangeAppendResult {
        position,
        message_id: decoded.message_id,
        cursor: decoded.cursor,
        frame_digest,
        deduplicated: false,
        committed: false,
    })
}

fn commit(
    inner: &FileActiveRangeStoreInner,
    generation: RangeGeneration,
    ownership_epoch: OwnershipEpoch,
    position: CommitPosition,
) -> Result<(), ActiveRangeStoreError> {
    let mut loaded = inner
        .loaded
        .lock()
        .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
    validate_generation_and_epoch(&loaded.persisted, generation, ownership_epoch)?;
    if position.value() < loaded.persisted.committed {
        return Err(ActiveRangeStoreError::CommitMovedBackwards {
            current: loaded.persisted.committed,
            supplied: position.value(),
        });
    }
    if position.value() > loaded.persisted.flushed {
        return Err(ActiveRangeStoreError::CommitBeyondFlushed {
            supplied: position.value(),
            flushed: loaded.persisted.flushed,
        });
    }
    let mut next = loaded.persisted.clone();
    next.committed = position.value();
    if next
        .pending_effect
        .as_ref()
        .is_some_and(|pending| pending.last_position <= position.value())
    {
        next.pending_effect = None;
    }
    persist_state(&inner.directory, &next)?;
    loaded.persisted = next;
    Ok(())
}

/// The remote manifest covering `position`, when the position belongs to
/// a demoted sealed span. Remote manifests pin coverage permanently, so
/// this lookup never depends on cache state.
fn remote_manifest_containing(loaded: &LoadedRange, position: u64) -> Option<&SegmentManifest> {
    loaded
        .remote
        .values()
        .find(|manifest| manifest.first_position <= position && position <= manifest.last_position)
}

/// Entry lookup that sees remote spans: local `entries` first, then the
/// per-span index of a materialized segment. Positions still remote-only
/// return `None` — callers materialize first when they need the bytes.
fn entry_at(loaded: &LoadedRange, position: RangePosition) -> Option<EntryIndex> {
    let position_value = position.value();
    if let Ok(index) = loaded
        .entries
        .binary_search_by_key(&position_value, |entry| entry.position.value())
    {
        return Some(loaded.entries[index].clone());
    }
    let manifest = remote_manifest_containing(loaded, position_value)?;
    loaded
        .materialized
        .get(&manifest.segment)?
        .entries
        .iter()
        .find(|entry| entry.position == position)
        .cloned()
}

/// Fetches one remote sealed segment through the configured tier source,
/// rescans the cache copy into a per-span entry index, and registers it
/// in the materialized overlay. The remote read happens with the range
/// lock released so a slow object store never stalls appends; the
/// manifest's span and digest were verified at upload, and `fetch_sealed`
/// re-verifies fetched bytes before admitting them to cache.
fn materialize_remote(
    inner: &FileActiveRangeStoreInner,
    manifest: &SegmentManifest,
) -> Result<(), ActiveRangeStoreError> {
    let tier = inner.tier.clone().ok_or_else(|| {
        ActiveRangeStoreError::RemoteFetch(format!(
            "segment {} is remote-only and no object store is configured",
            manifest.segment
        ))
    })?;
    let path = tier.fetch_sealed(&inner.directory, manifest.segment, manifest)?;
    let recovered = recover_segment(&path, manifest.segment, false)?;
    if recovered.len() as u64 != manifest.entries
        || recovered.first().map(|entry| entry.position.value()) != Some(manifest.first_position)
        || recovered.last().map(|entry| entry.position.value()) != Some(manifest.last_position)
    {
        return Err(ActiveRangeStoreError::CorruptSegment {
            path,
            reason: format!(
                "cached copy does not cover manifest span {}..={} (entries={})",
                manifest.first_position, manifest.last_position, manifest.entries
            ),
        });
    }
    let mut loaded = inner
        .loaded
        .lock()
        .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
    if loaded.materialized.contains_key(&manifest.segment) {
        return Ok(());
    }
    if !loaded.remote.contains_key(&manifest.segment) {
        return Err(ActiveRangeStoreError::InvalidState(format!(
            "materialized segment {} is not registered remote",
            manifest.segment
        )));
    }
    for entry in &recovered {
        loaded
            .cursor_positions
            .insert(entry.cursor.clone(), entry.position);
        loaded
            .digest_positions
            .insert(entry.frame_digest, entry.position);
    }
    loaded.use_seq = loaded.use_seq.saturating_add(1);
    let last_used = loaded.use_seq;
    loaded.materialized.insert(
        manifest.segment,
        MaterializedSegment {
            path,
            bytes: manifest.bytes,
            last_used,
            entries: recovered,
        },
    );
    loaded.materialized_bytes = loaded.materialized_bytes.saturating_add(manifest.bytes);
    demote_to_budget(inner, &mut loaded);
    Ok(())
}

/// Demotes the coldest cached remote segments until the configured cache
/// budget holds. Eviction drops the span's per-segment index and cache
/// file — the manifest stays pinned in `remote`, so the span
/// re-materializes on the next read — and `entries` is untouched because
/// remote spans never lived there. The last materialized segment is
/// never evicted even when it alone exceeds the budget: a span must stay
/// servable after its fetch, or a sub-segment budget would loop
/// fetch-and-evict forever.
fn demote_to_budget(inner: &FileActiveRangeStoreInner, loaded: &mut LoadedRange) {
    let Some(tier) = inner.tier.as_ref() else {
        return;
    };
    let budget = tier.remote_cache_bytes();
    while loaded.materialized_bytes > budget && loaded.materialized.len() > 1 {
        let Some((segment, evicted)) = loaded
            .materialized
            .iter()
            .min_by_key(|(_, materialized)| materialized.last_used)
            .map(|(segment, materialized)| {
                (*segment, (materialized.path.clone(), materialized.bytes))
            })
        else {
            break;
        };
        loaded.materialized.remove(&segment);
        loaded.materialized_bytes = loaded.materialized_bytes.saturating_sub(evicted.1);
        let _ = std::fs::remove_file(evicted.0);
    }
}

/// Position-ordered window read shared by the committed, uncommitted, and
/// byte-bounded paths. Each next position resolves exactly one way: a
/// local entry, or a remote span that materializes under a dropped lock
/// and then serves through its per-segment index. Dense positions are
/// the store's own invariant — a hole that is neither local nor remote is
/// an InvalidState, never a silent skip.
fn read_window(
    inner: &Arc<FileActiveRangeStoreInner>,
    after: Option<RangePosition>,
    limit: u64,
    committed_only: bool,
    max_bytes: Option<usize>,
) -> Result<Vec<StoredRangeFrame>, ActiveRangeStoreError> {
    let outcome = read_window_inner(inner, after, limit, committed_only, max_bytes);
    // Sequential historical readers stall once per segment boundary
    // without read-ahead; a read that reached remote spans warms the
    // next few so the cursor keeps moving. Spans strictly after the
    // served tail are the only candidates, so a read that ends on local
    // data selects nothing and no prefetch thread spawns. Errors are
    // irrelevant — prefetch is a hint and every span refetches on demand.
    if let Ok(frames) = &outcome {
        if let Some(last) = frames.last().map(|frame| frame.position.value()) {
            prefetch_remote(inner, last);
        }
    }
    outcome
}

/// Speculative read-ahead for historical scans: materialize up to
/// `PREFETCH_REMOTE_SPANS` sealed remote spans positioned after `last`,
/// skipping what already fits the cache only when the budget has room.
/// One in-flight prefetch per range collapses read storms; fetch
/// single-flights collapse duplicate objects across ranges. Runs on a
/// detached thread — the read path stays unblocked and the worker lock
/// is never held across remote I/O.
const PREFETCH_REMOTE_SPANS: usize = 2;

fn prefetch_remote(inner: &Arc<FileActiveRangeStoreInner>, last: u64) {
    let spans = {
        let Ok(loaded) = inner.loaded.lock() else {
            return;
        };
        let Some(budget) = inner.tier.as_ref().map(|tier| tier.remote_cache_bytes()) else {
            return;
        };
        if inner
            .prefetch_running
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
        let mut pending = loaded.materialized_bytes;
        let mut spans = Vec::new();
        for manifest in loaded.remote.values() {
            if manifest.first_position <= last
                || loaded.materialized.contains_key(&manifest.segment)
            {
                continue;
            }
            if pending.saturating_add(manifest.bytes) > budget {
                break;
            }
            pending = pending.saturating_add(manifest.bytes);
            spans.push(manifest.clone());
            if spans.len() >= PREFETCH_REMOTE_SPANS {
                break;
            }
        }
        spans
    };
    if spans.is_empty() {
        inner
            .prefetch_running
            .store(false, std::sync::atomic::Ordering::SeqCst);
        return;
    }
    let fallback = Arc::clone(inner);
    let inner = Arc::clone(inner);
    let spawned = std::thread::Builder::new()
        .name("whitewater-remote-prefetch".to_owned())
        .spawn(move || {
            // The guard clears the in-flight flag even if a fetch panics —
            // a wedged flag would silently disable read-ahead for the
            // range's remaining lifetime.
            struct ClearOnExit<'a>(&'a std::sync::atomic::AtomicBool);
            impl Drop for ClearOnExit<'_> {
                fn drop(&mut self) {
                    self.0.store(false, std::sync::atomic::Ordering::SeqCst);
                }
            }
            let _guard = ClearOnExit(&inner.prefetch_running);
            for manifest in spans {
                let _ = materialize_remote(&inner, &manifest);
            }
        });
    if spawned.is_err() {
        fallback
            .prefetch_running
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

fn read_window_inner(
    inner: &Arc<FileActiveRangeStoreInner>,
    after: Option<RangePosition>,
    limit: u64,
    committed_only: bool,
    max_bytes: Option<usize>,
) -> Result<Vec<StoredRangeFrame>, ActiveRangeStoreError> {
    let mut next = after.map_or(0, RangePosition::value).saturating_add(1);
    // History below the retention floor is gone: a cursor resuming below
    // it fails closed rather than silently skipping lost positions, while
    // an open-ended history read (`after = None`) clamps to the retained
    // beginning — the same honesty rule the API documents.
    {
        let loaded = inner
            .loaded
            .lock()
            .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
        let floor = loaded.persisted.retained_from_position.unwrap_or(1).max(1);
        if next < floor {
            if after.is_some() {
                return Err(ActiveRangeStoreError::RetentionExpired {
                    position: next,
                    retained_from: floor,
                });
            }
            next = floor;
        }
    }
    let mut frames = Vec::new();
    let mut bytes = 0_usize;
    loop {
        let fetch = {
            let mut loaded = inner
                .loaded
                .lock()
                .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
            let bound = if committed_only {
                loaded.persisted.committed
            } else {
                loaded.persisted.appended
            };
            if next > bound || frames.len() as u64 >= limit {
                return Ok(frames);
            }
            // Retention may have advanced the floor under an in-progress
            // scan — continuing positions are gone, so the read fails
            // closed instead of jumping over deleted history.
            let floor = loaded.persisted.retained_from_position.unwrap_or(1).max(1);
            if next < floor {
                return Err(ActiveRangeStoreError::RetentionExpired {
                    position: next,
                    retained_from: floor,
                });
            }
            let remaining = (limit - frames.len() as u64) as usize;
            match remote_manifest_containing(&loaded, next).cloned() {
                Some(manifest) if !loaded.materialized.contains_key(&manifest.segment) => {
                    Some(manifest)
                }
                Some(manifest) => {
                    let materialized = loaded
                        .materialized
                        .get(&manifest.segment)
                        .expect("materialized checked above");
                    let offset = (next - manifest.first_position) as usize;
                    let take = remaining.min((manifest.last_position - next + 1) as usize);
                    let mut served = 0_usize;
                    for entry in materialized.entries.iter().skip(offset).take(take) {
                        if entry.position.value() != next {
                            return Err(ActiveRangeStoreError::InvalidState(format!(
                                "remote index gap at position {next}"
                            )));
                        }
                        let frame = read_indexed_frame(&inner.directory, &loaded, entry)?;
                        if let Some(cap) = max_bytes {
                            let size = crate::codec::logical_frame_size(&frame.frame)?;
                            let candidate = bytes
                                .checked_add(size)
                                .ok_or(ActiveRangeStoreError::ReadBudgetExceeded)?;
                            if candidate > cap {
                                if frames.is_empty() {
                                    return Err(ActiveRangeStoreError::ReadBudgetExceeded);
                                }
                                return Ok(frames);
                            }
                            bytes = candidate;
                        }
                        frames.push(frame);
                        next = next.saturating_add(1);
                        served += 1;
                    }
                    if served == 0 {
                        return Err(ActiveRangeStoreError::InvalidState(format!(
                            "remote index missing position {next}"
                        )));
                    }
                    loaded.use_seq = loaded.use_seq.saturating_add(1);
                    let stamp = loaded.use_seq;
                    if let Some(materialized) = loaded.materialized.get_mut(&manifest.segment) {
                        materialized.last_used = stamp;
                    }
                    None
                }
                None => {
                    let start = loaded
                        .entries
                        .partition_point(|entry| entry.position.value() < next);
                    let mut served = 0_usize;
                    for entry in loaded
                        .entries
                        .iter()
                        .skip(start)
                        .take_while(|entry| entry.position.value() <= bound)
                        .take(remaining)
                    {
                        if entry.position.value() != next {
                            break;
                        }
                        let frame = read_indexed_frame(&inner.directory, &loaded, entry)?;
                        if let Some(cap) = max_bytes {
                            let size = crate::codec::logical_frame_size(&frame.frame)?;
                            let candidate = bytes
                                .checked_add(size)
                                .ok_or(ActiveRangeStoreError::ReadBudgetExceeded)?;
                            if candidate > cap {
                                if frames.is_empty() {
                                    return Err(ActiveRangeStoreError::ReadBudgetExceeded);
                                }
                                return Ok(frames);
                            }
                            bytes = candidate;
                        }
                        frames.push(frame);
                        next = next.saturating_add(1);
                        served += 1;
                    }
                    if served == 0 && next <= bound {
                        return Err(ActiveRangeStoreError::InvalidState(format!(
                            "position {next} is neither local nor remote"
                        )));
                    }
                    None
                }
            }
        };
        if let Some(manifest) = fetch {
            materialize_remote(inner, &manifest)?;
        }
    }
}

fn read_uncommitted(
    inner: &Arc<FileActiveRangeStoreInner>,
    after: Option<RangePosition>,
    limit: usize,
) -> Result<Vec<StoredRangeFrame>, ActiveRangeStoreError> {
    read_window(inner, after, limit.clamp(1, 10_000) as u64, false, None)
}

fn read_committed(
    inner: &Arc<FileActiveRangeStoreInner>,
    after: Option<RangePosition>,
    limit: usize,
) -> Result<Vec<StoredRangeFrame>, ActiveRangeStoreError> {
    read_window(inner, after, limit.clamp(1, 10_000) as u64, true, None)
}

fn read_committed_bounded(
    inner: &Arc<FileActiveRangeStoreInner>,
    after: Option<RangePosition>,
    limit: usize,
    max_bytes: usize,
) -> Result<Vec<StoredRangeFrame>, ActiveRangeStoreError> {
    read_window(
        inner,
        after,
        limit.clamp(1, 128) as u64,
        true,
        Some(max_bytes),
    )
}

fn truncate_uncommitted(
    inner: &FileActiveRangeStoreInner,
    generation: RangeGeneration,
    ownership_epoch: OwnershipEpoch,
) -> Result<u64, ActiveRangeStoreError> {
    let mut loaded = inner
        .loaded
        .lock()
        .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
    truncate_uncommitted_locked(inner, &mut loaded, generation, ownership_epoch)
}

fn truncate_uncommitted_locked(
    inner: &FileActiveRangeStoreInner,
    loaded: &mut LoadedRange,
    generation: RangeGeneration,
    ownership_epoch: OwnershipEpoch,
) -> Result<u64, ActiveRangeStoreError> {
    validate_generation_and_epoch(&loaded.persisted, generation, ownership_epoch)?;
    let committed = loaded.persisted.committed;
    if loaded.entries.iter().any(|entry| {
        entry.position.value() > committed && entry.segment != loaded.persisted.active_segment
    }) || loaded
        .remote
        .values()
        .any(|manifest| manifest.last_position > committed)
    {
        return Err(ActiveRangeStoreError::UncommittedSealedSegment);
    }
    let active_length = loaded
        .entries
        .iter()
        .rev()
        .find(|entry| {
            entry.segment == loaded.persisted.active_segment && entry.position.value() <= committed
        })
        .map_or(0, |entry| entry.entry_end);
    // `active_file` is opened append-only (FILE_APPEND_DATA on Windows),
    // which the platform refuses to truncate; a read/write handle performs
    // the resize instead. Appends keep landing at the new EOF because
    // FILE_APPEND_DATA ignores the stale handle's position.
    let truncate = OpenOptions::new()
        .read(true)
        .write(true)
        .open(segment_path(
            &inner.directory,
            loaded.persisted.active_segment,
        ))?;
    truncate.set_len(active_length)?;
    truncate.sync_all()?;
    let previous = loaded.persisted.appended;
    loaded
        .entries
        .retain(|entry| entry.position.value() <= committed);
    loaded
        .cursor_positions
        .retain(|_, position| position.value() <= committed);
    loaded
        .digest_positions
        .retain(|_, position| position.value() <= committed);
    let surviving_positions = loaded
        .entries
        .iter()
        .map(|entry| entry.position.value())
        .collect();
    loaded.deduplication = merge_deduplication(
        deduplication_from_entries(&loaded.entries),
        persisted_deduplication(&loaded.deduplication),
        &surviving_positions,
    );
    let mut next = loaded.persisted.clone();
    next.appended = committed;
    next.flushed = committed;
    next.deduplication = persisted_deduplication(&loaded.deduplication);
    next.pending_effect = None;
    persist_state(&inner.directory, &next)?;
    loaded.persisted = next;
    Ok(previous.saturating_sub(committed))
}

fn rotate_if_needed(
    inner: &FileActiveRangeStoreInner,
    loaded: &mut LoadedRange,
    next_entry_bytes: u64,
) -> Result<(), ActiveRangeStoreError> {
    let current_length = loaded.active_file.metadata()?.len();
    if current_length == 0
        || current_length.saturating_add(next_entry_bytes) <= inner.options.max_segment_bytes
        || loaded.persisted.appended != loaded.persisted.committed
    {
        return Ok(());
    }
    loaded.active_file.sync_all()?;
    let next_segment = loaded
        .persisted
        .active_segment
        .checked_add(1)
        .ok_or(ActiveRangeStoreError::PositionOverflow)?;
    let next_file = open_segment(&inner.directory, next_segment)?;
    next_file.sync_all()?;
    let mut next = loaded.persisted.clone();
    next.active_segment = next_segment;
    persist_state(&inner.directory, &next)?;
    // The sealed segment's committed index is now immutable; persist it as
    // a checkpoint so reopen recovers it without rescanning frame bodies.
    let sealed_entries: Vec<EntryIndex> = loaded
        .entries
        .iter()
        .filter(|entry| entry.segment == loaded.persisted.active_segment)
        .cloned()
        .collect();
    write_segment_checkpoint(
        &inner.directory,
        loaded.persisted.active_segment,
        &next,
        &sealed_entries,
    )?;
    write_segment_manifest(
        &inner.directory,
        loaded.persisted.active_segment,
        &next,
        &sealed_entries,
    )?;
    loaded.persisted = next;
    loaded.active_file = next_file;
    Ok(())
}

/// A sealed segment is immutable, so its committed index rows persist once
/// into `index-checkpoint-{segment}.bin` at rotation time. Reopen loads a
/// sealed segment's `EntryIndex` rows from the checkpoint instead of
/// rescanning every frame body, bounding restart recovery to the single
/// active segment's tail rescan. The checkpoint is an optimization layer:
/// identity, contiguity, coverage, and a tail-frame BLAKE3 spot check are
/// re-verified against the segment on every load, and any invalid or
/// missing checkpoint falls back to the full segment rescan.
const INDEX_CHECKPOINT_MAGIC: &[u8; 4] = b"WWIC";
const INDEX_CHECKPOINT_VERSION: u32 = 1;
/// A paranoid bound on checkpointed rows: one full segment can never hold
/// more committed entries than `max_segment_bytes` smallest-possible frames.
const MAX_ENTRY_COUNT_HINT: u64 = 1 << 24;

fn checkpoint_path(directory: &Path, segment: u64) -> PathBuf {
    directory.join(format!("index-checkpoint-{segment:06}.bin"))
}

fn write_segment_checkpoint(
    directory: &Path,
    segment: u64,
    persisted: &PersistedRangeState,
    entries: &[EntryIndex],
) -> Result<(), ActiveRangeStoreError> {
    let mut body = Vec::new();
    body.extend_from_slice(INDEX_CHECKPOINT_MAGIC);
    body.extend_from_slice(&INDEX_CHECKPOINT_VERSION.to_be_bytes());
    body.extend_from_slice(persisted.feed_id.as_bytes());
    body.extend_from_slice(persisted.range_id.as_uuid().as_bytes());
    body.extend_from_slice(&persisted.generation.value().to_be_bytes());
    let count: u64 = entries
        .len()
        .try_into()
        .map_err(|_| ActiveRangeStoreError::PositionOverflow)?;
    body.extend_from_slice(&count.to_be_bytes());
    for entry in entries {
        body.extend_from_slice(&entry.position.value().to_be_bytes());
        body.extend_from_slice(&entry.segment.to_be_bytes());
        body.extend_from_slice(entry.identity.writer_session_id.as_bytes());
        body.extend_from_slice(&entry.identity.writer_epoch.to_be_bytes());
        body.extend_from_slice(&entry.identity.sequence.to_be_bytes());
        let cursor = entry.cursor.as_bytes();
        let cursor_length: u32 = cursor
            .len()
            .try_into()
            .map_err(|_| ActiveRangeStoreError::CursorTooLarge)?;
        body.extend_from_slice(&cursor_length.to_be_bytes());
        body.extend_from_slice(cursor);
        body.extend_from_slice(&entry.frame_digest);
        body.extend_from_slice(entry.message_id.as_bytes());
        body.extend_from_slice(&entry.entry_end.to_be_bytes());
        body.extend_from_slice(&entry.frame_offset.to_be_bytes());
        body.extend_from_slice(&entry.frame_length.to_be_bytes());
        body.extend_from_slice(&entry.ingest_time_ns.to_be_bytes());
    }
    body.extend_from_slice(&crc32fast::hash(&body).to_be_bytes());
    let path = checkpoint_path(directory, segment);
    let temporary = path.with_extension("tmp");
    {
        let mut file = File::create(&temporary)?;
        file.write_all(&body)?;
        file.sync_all()?;
    }
    fs::rename(&temporary, &path)?;
    sync_directory(directory)?;
    Ok(())
}

/// A sealed segment is immutable and content-addressable: its manifest is
/// the integrity contract every later tier — object-store upload, replica
/// repair, operator verification — checks before trusting the bytes. The
/// whole-segment digest is computed once at rotation from the fsynced file
/// (durable bytes, not remembered write buffers) and the manifest is
/// self-healed at reopen when an older deployment or a torn write left a
/// sealed segment without one.
const SEGMENT_MANIFEST_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentManifest {
    pub version: u32,
    pub feed_id: Uuid,
    pub range_id: RangeId,
    pub generation: RangeGeneration,
    pub segment: u64,
    /// Exact byte length of the sealed segment file.
    pub bytes: u64,
    /// Committed entries covered, dense from `first_position`.
    pub entries: u64,
    pub first_position: u64,
    pub last_position: u64,
    /// BLAKE3-256 hex of the whole sealed segment file.
    pub segment_digest: String,
    /// Ingest timestamp of the span's newest committed record —
    /// content-derived like every other manifest field, so every RF3
    /// replica computes identical bytes and the expiry sweep makes the
    /// same keep/delete decision everywhere. `None` only on manifests
    /// written before the field existed; they re-heal on first touch.
    #[serde(default)]
    pub last_ingest_time_ns: Option<i64>,
}

/// Why the retention sweep deleted a sealed span. `Expired` is the
/// `expire_after_ns` ingest-age bound; `Budget` is `max_retained_bytes`
/// trimming oldest-first; `Erased` is a cryptographic-erasure or legal
/// destruction event (Slice 3 — declared now so marker files are stable).
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RetentionReason {
    Expired,
    Budget,
    Erased,
}

/// Durable record that a sealed span was deleted by the retention sweep,
/// written before the span's files are removed so a crash mid-delete can
/// never make the absence look like corruption. The marker is
/// self-proving — it carries the span's coverage, so recovery tolerates
/// the missing segment even when its manifest is also gone. Deterministic
/// content (no wall-clock fields) keeps replicas byte-identical.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RetentionMarker {
    pub version: u32,
    pub segment: u64,
    pub segment_digest: String,
    pub first_position: u64,
    pub last_position: u64,
    pub reason: RetentionReason,
    pub retained_from_position: u64,
}

const RETENTION_MARKER_VERSION: u32 = 1;

pub(crate) fn retention_marker_path(directory: &Path, segment: u64) -> PathBuf {
    directory.join(format!("segment-retention-{segment:06}.json"))
}

pub(crate) fn read_retention_marker(directory: &Path, segment: u64) -> Option<RetentionMarker> {
    let bytes = fs::read(retention_marker_path(directory, segment)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn write_retention_marker(
    directory: &Path,
    manifest: &SegmentManifest,
    reason: RetentionReason,
    retained_from: u64,
) -> Result<(), ActiveRangeStoreError> {
    let marker = RetentionMarker {
        version: RETENTION_MARKER_VERSION,
        segment: manifest.segment,
        segment_digest: manifest.segment_digest.clone(),
        first_position: manifest.first_position,
        last_position: manifest.last_position,
        reason,
        retained_from_position: retained_from,
    };
    let serialized = serde_json::to_vec(&marker)?;
    let target = retention_marker_path(directory, manifest.segment);
    let temporary = target.with_extension(format!("{}.tmp", Uuid::new_v4()));
    {
        let mut file = File::create(&temporary)?;
        file.write_all(&serialized)?;
        file.sync_all()?;
    }
    fs::rename(&temporary, &target)?;
    sync_directory(directory)?;
    Ok(())
}

pub(crate) fn manifest_path(directory: &Path, segment: u64) -> PathBuf {
    directory.join(format!("segment-manifest-{segment:06}.json"))
}

fn hash_segment_file(path: &Path) -> Result<blake3::Hash, ActiveRangeStoreError> {
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update_reader(&mut file)?;
    Ok(hasher.finalize())
}

/// Streams the sealed segment file once, binds its digest and coverage to
/// the range identity, and persists the manifest with the same
/// tmp+rename+dir-sync durability contract as the index checkpoint.
pub(crate) fn write_segment_manifest(
    directory: &Path,
    segment: u64,
    persisted: &PersistedRangeState,
    entries: &[EntryIndex],
) -> Result<SegmentManifest, ActiveRangeStoreError> {
    let first = entries.first().ok_or_else(|| {
        ActiveRangeStoreError::InvalidState("sealed segment has no entries".to_owned())
    })?;
    let last = entries.last().ok_or_else(|| {
        ActiveRangeStoreError::InvalidState("sealed segment has no entries".to_owned())
    })?;
    let path = segment_path(directory, segment);
    let manifest = SegmentManifest {
        version: SEGMENT_MANIFEST_VERSION,
        feed_id: persisted.feed_id,
        range_id: persisted.range_id,
        generation: persisted.generation,
        segment,
        bytes: path.metadata()?.len(),
        entries: entries
            .len()
            .try_into()
            .map_err(|_| ActiveRangeStoreError::PositionOverflow)?,
        first_position: first.position.value(),
        last_position: last.position.value(),
        segment_digest: hash_segment_file(&path)?.to_hex().to_string(),
        last_ingest_time_ns: Some(last.ingest_time_ns),
    };
    let serialized = serde_json::to_vec(&manifest)?;
    let target = manifest_path(directory, segment);
    let temporary = target.with_extension(format!("{}.tmp", Uuid::new_v4()));
    {
        let mut file = File::create(&temporary)?;
        file.write_all(&serialized)?;
        file.sync_all()?;
    }
    fs::rename(&temporary, &target)?;
    sync_directory(directory)?;
    Ok(manifest)
}

/// Reads a manifest only when it parses and binds to the expected
/// Feed/Range/generation/segment identity — anything else is treated as
/// absent so the heal path can re-derive it from the sealed file itself.
pub(crate) fn read_segment_manifest(
    directory: &Path,
    segment: u64,
    persisted: &PersistedRangeState,
) -> Option<SegmentManifest> {
    let bytes = fs::read(manifest_path(directory, segment)).ok()?;
    let manifest = serde_json::from_slice::<SegmentManifest>(&bytes).ok()?;
    (manifest.version == SEGMENT_MANIFEST_VERSION
        && manifest.feed_id == persisted.feed_id
        && manifest.range_id == persisted.range_id
        && manifest.generation == persisted.generation
        && manifest.segment == segment)
        .then_some(manifest)
}

/// Re-derives the manifest for a sealed segment that lacks a valid one.
/// The sealed file is the local source of truth, so healing rewrites a
/// torn or absent manifest with the digest the bytes actually hold; a
/// mismatched file then fails closed at `verify_sealed_segment` time.
pub(crate) fn ensure_segment_manifest(
    directory: &Path,
    segment: u64,
    persisted: &PersistedRangeState,
    entries: &[EntryIndex],
) -> Result<(), ActiveRangeStoreError> {
    if entries.is_empty() || read_segment_manifest(directory, segment, persisted).is_some() {
        return Ok(());
    }
    write_segment_manifest(directory, segment, persisted, entries)?;
    Ok(())
}

/// Returns the sealed segment's manifest, healing it from the file when
/// absent — the single entry point the object-store uploader uses per
/// segment. A missing or invalid manifest is re-derived by rescanning the
/// sealed file, which is always accurate: the segment is immutable, and a
/// pre-upgrade or torn-write absence heals itself rather than blocking
/// upload forever.
pub(crate) fn sealed_segment_manifest(
    directory: &Path,
    segment: u64,
    persisted: &PersistedRangeState,
) -> Result<SegmentManifest, ActiveRangeStoreError> {
    if let Some(manifest) = read_segment_manifest(directory, segment, persisted) {
        if manifest.last_ingest_time_ns.is_some() {
            return Ok(manifest);
        }
        // A pre-field manifest is valid but cannot drive the expiry
        // sweep; re-derive it from the sealed file so the content-
        // derived ingest bound converges identically on every replica.
    }
    let entries = recover_segment(&segment_path(directory, segment), segment, false)?;
    write_segment_manifest(directory, segment, persisted, &entries)
}

/// Rehashes the sealed segment file and checks it against the persisted
/// manifest — the fail-closed primitive the object-store upload path runs
/// before trusting local bytes, proving "seal and verify" per segment
/// rather than per frame. A missing/invalid manifest, a length drift, or
/// a digest mismatch all surface as `CorruptSegment`.
pub(crate) fn verify_sealed_segment(
    directory: &Path,
    segment: u64,
    persisted: &PersistedRangeState,
) -> Result<SegmentManifest, ActiveRangeStoreError> {
    let path = segment_path(directory, segment);
    let manifest = read_segment_manifest(directory, segment, persisted)
        .ok_or_else(|| corrupt(&path, "sealed segment manifest is missing or invalid"))?;
    let bytes = path.metadata()?.len();
    if bytes != manifest.bytes {
        return Err(corrupt(
            &path,
            "sealed segment length does not match its manifest",
        ));
    }
    if hash_segment_file(&path)?.to_hex().to_string() != manifest.segment_digest {
        return Err(corrupt(
            &path,
            "sealed segment digest does not match its manifest",
        ));
    }
    Ok(manifest)
}

/// Loads a sealed segment's checkpoint when it is present and valid.
/// `expected_start` is the first RangePosition the segment must cover:
/// positions are dense across segments, so a checkpoint whose coverage does
/// not begin exactly there cannot pair with this log. Validation checks the
/// persisted Feed/Range/generation identity, contiguous positions, full
/// coverage of the segment bytes, and a BLAKE3 spot check of the last
/// recorded frame — anything else fails closed to a normal rescan.
fn load_segment_checkpoint(
    directory: &Path,
    segment: u64,
    persisted: &PersistedRangeState,
    expected_start: u64,
) -> Option<Vec<EntryIndex>> {
    let bytes = fs::read(checkpoint_path(directory, segment)).ok()?;
    let entries = parse_segment_checkpoint(&bytes, segment, persisted, expected_start).ok()?;
    corroborate_segment_checkpoint(directory, segment, &entries).ok()?;
    Some(entries)
}

fn parse_segment_checkpoint(
    bytes: &[u8],
    segment: u64,
    persisted: &PersistedRangeState,
    expected_start: u64,
) -> Result<Vec<EntryIndex>, ActiveRangeStoreError> {
    let invalid =
        || ActiveRangeStoreError::InvalidState("index checkpoint content is invalid".to_owned());
    if bytes.len() < 52 {
        return Err(invalid());
    }
    let (body, trailer) = bytes.split_at(bytes.len() - 4);
    if crc32fast::hash(body) != u32::from_be_bytes(trailer.try_into().map_err(|_| invalid())?) {
        return Err(invalid());
    }
    let mut reader = ByteReader::new(body);
    if reader.take(4)? != INDEX_CHECKPOINT_MAGIC || reader.u32()? != INDEX_CHECKPOINT_VERSION {
        return Err(invalid());
    }
    if Uuid::from_slice(reader.take(16)?).map_err(|_| invalid())? != persisted.feed_id
        || RangeId::from_uuid(Uuid::from_slice(reader.take(16)?).map_err(|_| invalid())?)
            != persisted.range_id
        || RangeGeneration::new(reader.u64()?) != persisted.generation
    {
        return Err(invalid());
    }
    let count = reader.u64()?;
    if count == 0 || count > MAX_ENTRY_COUNT_HINT {
        return Err(invalid());
    }
    let mut entries = Vec::with_capacity(count as usize);
    let mut expected = expected_start;
    for _ in 0..count {
        let position = RangePosition::new(reader.u64()?);
        if position.value() != expected || reader.u64()? != segment {
            return Err(invalid());
        }
        expected = expected.checked_add(1).ok_or(invalid())?;
        let writer_session_id = Uuid::from_slice(reader.take(16)?).map_err(|_| invalid())?;
        let writer_epoch = reader.u64()?;
        let sequence = reader.u64()?;
        let cursor_length = reader.u32()? as usize;
        let cursor =
            String::from_utf8(reader.take(cursor_length)?.to_vec()).map_err(|_| invalid())?;
        let frame_digest: [u8; 32] = reader.take(32)?.try_into().map_err(|_| invalid())?;
        let message_id = Uuid::from_slice(reader.take(16)?).map_err(|_| invalid())?;
        let entry_end = reader.u64()?;
        let frame_offset = reader.u64()?;
        let frame_length = reader.u64()?;
        let ingest_time_ns = i64::from_be_bytes(reader.take(8)?.try_into().map_err(|_| invalid())?);
        if frame_length == 0 || frame_length > MAX_FRAME_BYTES as u64 {
            return Err(invalid());
        }
        entries.push(EntryIndex {
            position,
            identity: AppendIdentity {
                writer_session_id,
                writer_epoch,
                sequence,
            },
            cursor,
            frame_digest,
            message_id,
            segment,
            entry_end,
            frame_offset,
            frame_length,
            ingest_time_ns,
        });
    }
    if !reader.is_empty() {
        return Err(invalid());
    }
    Ok(entries)
}

/// Re-verifies a parsed checkpoint against the segment bytes it claims to
/// index: the checkpoint must cover the segment exactly (its last entry_end
/// is the file length) and the last entry is re-read in full — its CRC
/// proves the on-disk bytes are intact while its frame BLAKE3 proves the
/// checkpoint pairs with these bytes rather than a rewritten segment.
/// Interior frames stay trusted: sealing made the bytes immutable and the
/// CRC over the checkpoint pins the pairing.
fn corroborate_segment_checkpoint(
    directory: &Path,
    segment: u64,
    entries: &[EntryIndex],
) -> Result<(), ActiveRangeStoreError> {
    let invalid = || {
        ActiveRangeStoreError::InvalidState(
            "index checkpoint does not corroborate the sealed segment".to_owned(),
        )
    };
    let path = segment_path(directory, segment);
    let mut file = File::open(&path)?;
    let last = entries.last().ok_or_else(invalid)?;
    if last.entry_end != file.metadata()?.len() {
        return Err(invalid());
    }
    let frame_in_body = ENTRY_FIXED_BODY_BYTES
        .checked_add(last.cursor.len())
        .ok_or_else(invalid)?;
    let entry_start = last
        .frame_offset
        .checked_sub(4 + frame_in_body as u64)
        .ok_or_else(invalid)?;
    let entry_length = last
        .entry_end
        .checked_sub(entry_start)
        .ok_or_else(invalid)?;
    if entry_length < 8 || entry_length > (MAX_ENTRY_BODY_BYTES + 8) as u64 {
        return Err(invalid());
    }
    let mut bytes = vec![0_u8; entry_length as usize];
    file.seek(SeekFrom::Start(entry_start))?;
    file.read_exact(&mut bytes)?;
    let declared = u32::from_be_bytes(bytes[..4].try_into().map_err(|_| invalid())?) as usize;
    if declared.checked_add(8) != Some(bytes.len()) {
        return Err(invalid());
    }
    let body = &bytes[4..4 + declared];
    let trailer = &bytes[4 + declared..];
    if crc32fast::hash(body) != u32::from_be_bytes(trailer.try_into().map_err(|_| invalid())?) {
        return Err(invalid());
    }
    let frame = body
        .get(frame_in_body..frame_in_body + last.frame_length as usize)
        .ok_or_else(invalid)?;
    if blake3::hash(frame).as_bytes() != &last.frame_digest {
        return Err(invalid());
    }
    Ok(())
}

type RecoveredSegments = (
    Vec<EntryIndex>,
    HashMap<WriterKey, DeduplicationState>,
    BTreeMap<u64, SegmentManifest>,
);

fn recover_segments(
    directory: &Path,
    persisted: &PersistedRangeState,
) -> Result<RecoveredSegments, ActiveRangeStoreError> {
    let mut entries = Vec::new();
    let mut remote = BTreeMap::new();
    // The retention floor is the lowest position that may still be read;
    // everything below it was deleted by the expiry/budget sweep and
    // proves itself through a self-describing retention marker.
    let floor = persisted.retained_from_position.unwrap_or(1).max(1);
    let mut expected_position = floor;
    for segment in 0..=persisted.active_segment {
        let segment_start = entries.len();
        let path = segment_path(directory, segment);
        if !path.exists() {
            if segment == persisted.active_segment {
                File::create(&path)?.sync_all()?;
            } else {
                // A sealed file is legitimately absent in two cases: the
                // retention sweep deleted it (self-proving marker below
                // the floor), or the range proves a verified remote copy
                // (manifest binds span/digest, tier marker binds object).
                // Anything less stays fail-closed corruption, and the
                // detection must run before checkpoint loading because a
                // checkpoint can outlive the file it indexes.
                let retention = read_retention_marker(directory, segment).filter(|marker| {
                    marker.version == RETENTION_MARKER_VERSION
                        && marker.segment == segment
                        && marker.last_position < floor
                });
                if let Some(marker) = retention {
                    expected_position = expected_position.max(
                        marker
                            .last_position
                            .checked_add(1)
                            .ok_or(ActiveRangeStoreError::PositionOverflow)?,
                    );
                    continue;
                }
                let manifest = read_segment_manifest(directory, segment, persisted)
                    .filter(|manifest| manifest.first_position == expected_position)
                    .filter(|manifest| manifest.last_position >= floor);
                let marker_matches = manifest.as_ref().is_some_and(|manifest| {
                    super::tier::read_tier_marker(directory, segment).is_some_and(|marker| {
                        marker.segment == segment
                            && marker.segment_digest == manifest.segment_digest
                    })
                });
                match manifest {
                    Some(manifest) if marker_matches => {
                        expected_position = manifest
                            .last_position
                            .checked_add(1)
                            .ok_or(ActiveRangeStoreError::PositionOverflow)?;
                        remote.insert(segment, manifest);
                        continue;
                    }
                    _ => {
                        return Err(ActiveRangeStoreError::CorruptSegment {
                            path,
                            reason: "sealed segment is missing".to_owned(),
                        });
                    }
                }
            }
        }
        if segment < persisted.active_segment {
            if let Some(mut checkpointed) =
                load_segment_checkpoint(directory, segment, persisted, expected_position)
            {
                // Positions below the floor in a still-present file are
                // expired survivors of an interrupted sweep — drop them
                // from the index; the next pass reclaims the bytes.
                checkpointed.retain(|entry| entry.position.value() >= floor);
                expected_position = expected_position
                    .checked_add(checkpointed.len() as u64)
                    .ok_or(ActiveRangeStoreError::PositionOverflow)?;
                entries.extend(checkpointed);
                ensure_segment_manifest(directory, segment, persisted, &entries[segment_start..])?;
                continue;
            }
        }
        let allow_torn_tail = segment == persisted.active_segment;
        let recovered = recover_segment(&path, segment, allow_torn_tail)?;
        for entry in recovered {
            if entry.position.value() < floor {
                continue;
            }
            if entry.position.value() != expected_position {
                return Err(ActiveRangeStoreError::CorruptSegment {
                    path: path.clone(),
                    reason: format!(
                        "expected RangePosition {expected_position}, found {}",
                        entry.position.value()
                    ),
                });
            }
            expected_position = expected_position
                .checked_add(1)
                .ok_or(ActiveRangeStoreError::PositionOverflow)?;
            entries.push(entry);
        }
        if segment < persisted.active_segment {
            ensure_segment_manifest(directory, segment, persisted, &entries[segment_start..])?;
        }
    }
    let deduplication = deduplication_from_entries(&entries);
    Ok((entries, deduplication, remote))
}

pub(crate) fn recover_segment(
    path: &Path,
    segment: u64,
    allow_torn_tail: bool,
) -> Result<Vec<EntryIndex>, ActiveRangeStoreError> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let file_length = file.metadata()?.len();
    let mut entries = Vec::new();
    let mut valid_length = 0_u64;
    while valid_length < file_length {
        file.seek(SeekFrom::Start(valid_length))?;
        let mut length_bytes = [0_u8; 4];
        if let Err(error) = file.read_exact(&mut length_bytes) {
            return recover_torn_or_fail(file, path, allow_torn_tail, valid_length, error, entries);
        }
        let body_length = u32::from_be_bytes(length_bytes) as usize;
        if body_length > MAX_ENTRY_BODY_BYTES {
            return Err(corrupt(path, "entry length exceeds the configured maximum"));
        }
        let entry_length = body_length
            .checked_add(8)
            .ok_or(ActiveRangeStoreError::PositionOverflow)?;
        let entry_end = valid_length
            .checked_add(entry_length as u64)
            .ok_or(ActiveRangeStoreError::PositionOverflow)?;
        if entry_end > file_length {
            return recover_torn_or_fail(
                file,
                path,
                allow_torn_tail,
                valid_length,
                std::io::Error::new(ErrorKind::UnexpectedEof, "partial range entry"),
                entries,
            );
        }
        let mut body = vec![0_u8; body_length];
        file.read_exact(&mut body)?;
        let mut checksum_bytes = [0_u8; 4];
        file.read_exact(&mut checksum_bytes)?;
        if crc32fast::hash(&body) != u32::from_be_bytes(checksum_bytes) {
            return Err(corrupt(path, "entry checksum is invalid"));
        }
        let decoded =
            decode_entry(&body).map_err(|error| ActiveRangeStoreError::CorruptSegment {
                path: path.to_path_buf(),
                reason: error.to_string(),
            })?;
        let frame_offset = valid_length
            .checked_add(4)
            .and_then(|offset| offset.checked_add(frame_offset_in_body(&decoded) as u64))
            .ok_or(ActiveRangeStoreError::PositionOverflow)?;
        entries.push(EntryIndex {
            position: decoded.position,
            identity: decoded.identity,
            cursor: decoded.cursor,
            frame_digest: decoded.frame_digest,
            message_id: decoded.message_id,
            ingest_time_ns: decoded.ingest_time_ns,
            segment,
            entry_end,
            frame_offset,
            frame_length: decoded.frame.len() as u64,
        });
        valid_length = entry_end;
    }
    Ok(entries)
}

fn recover_torn_or_fail(
    file: File,
    path: &Path,
    allow_torn_tail: bool,
    valid_length: u64,
    error: std::io::Error,
    entries: Vec<EntryIndex>,
) -> Result<Vec<EntryIndex>, ActiveRangeStoreError> {
    if allow_torn_tail && error.kind() == ErrorKind::UnexpectedEof {
        file.set_len(valid_length)?;
        file.sync_all()?;
        Ok(entries)
    } else {
        Err(ActiveRangeStoreError::CorruptSegment {
            path: path.to_path_buf(),
            reason: error.to_string(),
        })
    }
}

fn encode_entry(entry: &DecodedEntry) -> Result<Vec<u8>, ActiveRangeStoreError> {
    let cursor = entry.cursor.as_bytes();
    if cursor.len() > MAX_CURSOR_BYTES {
        return Err(ActiveRangeStoreError::CursorTooLarge);
    }
    let cursor_length: u32 = cursor
        .len()
        .try_into()
        .map_err(|_| ActiveRangeStoreError::CursorTooLarge)?;
    let frame_length: u32 = entry
        .frame
        .len()
        .try_into()
        .map_err(|_| CodecError::FrameTooLarge)?;
    let body_capacity = ENTRY_FIXED_BODY_BYTES
        .checked_add(cursor.len())
        .and_then(|length| length.checked_add(entry.frame.len()))
        .ok_or(ActiveRangeStoreError::PositionOverflow)?;
    if body_capacity > MAX_ENTRY_BODY_BYTES {
        return Err(CodecError::FrameTooLarge.into());
    }
    let mut body = Vec::with_capacity(body_capacity);
    body.extend_from_slice(ENTRY_MAGIC);
    body.extend_from_slice(&entry.position.value().to_be_bytes());
    body.extend_from_slice(entry.identity.writer_session_id.as_bytes());
    body.extend_from_slice(&entry.identity.writer_epoch.to_be_bytes());
    body.extend_from_slice(&entry.identity.sequence.to_be_bytes());
    body.extend_from_slice(&cursor_length.to_be_bytes());
    body.extend_from_slice(&frame_length.to_be_bytes());
    body.extend_from_slice(&entry.frame_digest);
    body.extend_from_slice(cursor);
    body.extend_from_slice(&entry.frame);
    let body_length: u32 = body
        .len()
        .try_into()
        .map_err(|_| CodecError::FrameTooLarge)?;
    let mut encoded = Vec::with_capacity(body.len() + 8);
    encoded.extend_from_slice(&body_length.to_be_bytes());
    encoded.extend_from_slice(&body);
    encoded.extend_from_slice(&crc32fast::hash(&body).to_be_bytes());
    Ok(encoded)
}

fn decode_entry(body: &[u8]) -> Result<DecodedEntry, ActiveRangeStoreError> {
    let mut reader = ByteReader::new(body);
    if reader.take(4)? != ENTRY_MAGIC {
        return Err(ActiveRangeStoreError::InvalidState(
            "range entry magic is invalid".to_owned(),
        ));
    }
    let position = RangePosition::new(reader.u64()?);
    let writer_session_id = Uuid::from_slice(reader.take(16)?).map_err(|error| {
        ActiveRangeStoreError::InvalidState(format!("Writer UUID is invalid: {error}"))
    })?;
    let writer_epoch = reader.u64()?;
    let sequence = reader.u64()?;
    let cursor_length = reader.u32()? as usize;
    let frame_length = reader.u32()? as usize;
    if cursor_length > MAX_CURSOR_BYTES || frame_length > MAX_FRAME_BYTES {
        return Err(ActiveRangeStoreError::InvalidState(
            "range entry field exceeds its configured maximum".to_owned(),
        ));
    }
    let frame_digest: [u8; 32] = reader
        .take(32)?
        .try_into()
        .map_err(|_| ActiveRangeStoreError::InvalidState("frame digest is truncated".to_owned()))?;
    let cursor = std::str::from_utf8(reader.take(cursor_length)?)
        .map_err(|error| ActiveRangeStoreError::InvalidState(error.to_string()))?
        .to_owned();
    let frame = reader.take(frame_length)?.to_vec();
    if !reader.is_empty() {
        return Err(ActiveRangeStoreError::InvalidState(
            "range entry contains trailing bytes".to_owned(),
        ));
    }
    if *blake3::hash(&frame).as_bytes() != frame_digest {
        return Err(ActiveRangeStoreError::InvalidState(
            "record frame digest is invalid".to_owned(),
        ));
    }
    let record = decode_record(&frame)?;
    if record.producer_id != writer_session_id || record.producer_sequence != sequence {
        return Err(ActiveRangeStoreError::WriterIdentityMismatch);
    }
    Ok(DecodedEntry {
        position,
        identity: AppendIdentity {
            writer_session_id,
            writer_epoch,
            sequence,
        },
        cursor,
        frame,
        frame_digest,
        message_id: record.message_id,
        ingest_time_ns: record.ingest_time_ns,
    })
}

fn frame_offset_in_body(entry: &DecodedEntry) -> usize {
    ENTRY_FIXED_BODY_BYTES + entry.cursor.len()
}

fn frame_offset_in_entry(entry: &DecodedEntry) -> Result<usize, ActiveRangeStoreError> {
    4_usize
        .checked_add(frame_offset_in_body(entry))
        .ok_or(ActiveRangeStoreError::PositionOverflow)
}

/// Same logical record ignoring the server-allocated `producer_sequence`:
/// a resequenced retry carries identical request-derived fields.
fn same_request_record(left: &StoredRecord, right: &StoredRecord) -> bool {
    left.message_id == right.message_id
        && left.producer_id == right.producer_id
        && left.event_time_ns == right.event_time_ns
        && left.key == right.key
        && left.payload == right.payload
        && left.metadata == right.metadata
}

fn read_indexed_frame(
    directory: &Path,
    loaded: &LoadedRange,
    entry: &EntryIndex,
) -> Result<StoredRangeFrame, ActiveRangeStoreError> {
    let path = loaded
        .materialized
        .get(&entry.segment)
        .map(|materialized| materialized.path.clone())
        .unwrap_or_else(|| segment_path(directory, entry.segment));
    let mut file = File::open(&path)?;
    let frame_end = entry
        .frame_offset
        .checked_add(entry.frame_length)
        .ok_or(ActiveRangeStoreError::PositionOverflow)?;
    if frame_end > file.metadata()?.len() {
        return Err(corrupt(
            &path,
            format!(
                "stored frame at RangePosition {} extends beyond the segment: offset={}, bytes={}",
                entry.position, entry.frame_offset, entry.frame_length
            ),
        ));
    }
    file.seek(SeekFrom::Start(entry.frame_offset))?;
    let frame_length: usize = entry
        .frame_length
        .try_into()
        .map_err(|_| CodecError::FrameTooLarge)?;
    let mut frame = vec![0_u8; frame_length];
    file.read_exact(&mut frame)?;
    if *blake3::hash(&frame).as_bytes() != entry.frame_digest {
        return Err(corrupt(
            &segment_path(directory, entry.segment),
            "record frame changed after recovery",
        ));
    }
    Ok(StoredRangeFrame {
        position: entry.position,
        identity: entry.identity.clone(),
        cursor: entry.cursor.clone(),
        frame,
    })
}

fn deduplication_from_entries(entries: &[EntryIndex]) -> HashMap<WriterKey, DeduplicationState> {
    let mut deduplication: HashMap<WriterKey, DeduplicationState> = HashMap::new();
    for entry in entries {
        let key = WriterKey {
            writer_session_id: entry.identity.writer_session_id,
            writer_epoch: entry.identity.writer_epoch,
        };
        let candidate = DeduplicationState {
            sequence: entry.identity.sequence,
            position: entry.position,
            message_id: entry.message_id,
            cursor: entry.cursor.clone(),
            frame_digest: entry.frame_digest,
        };
        match deduplication.get(&key) {
            Some(existing) if existing.sequence >= candidate.sequence => {}
            _ => {
                deduplication.insert(key, candidate);
            }
        }
    }
    deduplication
}

/// Merge entry-derived dedup state with previously persisted state: a
/// deduplicated retry consumes a Writer sequence without landing a record, so
/// the landed-entry scan alone can regress the per-Writer high-water mark and
/// permanently gap the stream after recovery or truncation. Keep the highest
/// recorded sequence per Writer whose landing position still exists; a
/// persisted entry pointing at a truncated position is dropped because a
/// replay can no longer be answered honestly — the client retries it as new
/// work under a fresh sequence. When two candidates carry the same sequence
/// the entry-derived one wins because its position is known to exist.
fn merge_deduplication(
    derived: HashMap<WriterKey, DeduplicationState>,
    persisted: Vec<PersistedDeduplicationState>,
    surviving_positions: &std::collections::HashSet<u64>,
) -> HashMap<WriterKey, DeduplicationState> {
    let mut merged = derived;
    for state in persisted {
        if !surviving_positions.contains(&state.position) {
            continue;
        }
        let key = WriterKey {
            writer_session_id: state.writer_session_id,
            writer_epoch: state.writer_epoch,
        };
        let candidate = DeduplicationState {
            sequence: state.sequence,
            position: RangePosition::new(state.position),
            message_id: state.message_id,
            cursor: state.cursor,
            frame_digest: state.frame_digest,
        };
        match merged.get(&key) {
            Some(existing) if existing.sequence >= candidate.sequence => {}
            _ => {
                merged.insert(key, candidate);
            }
        }
    }
    merged
}

fn persisted_deduplication(
    deduplication: &HashMap<WriterKey, DeduplicationState>,
) -> Vec<PersistedDeduplicationState> {
    let mut values = deduplication
        .iter()
        .map(|(key, value)| PersistedDeduplicationState {
            writer_session_id: key.writer_session_id,
            writer_epoch: key.writer_epoch,
            sequence: value.sequence,
            position: value.position.value(),
            message_id: value.message_id,
            cursor: value.cursor.clone(),
            frame_digest: value.frame_digest,
        })
        .collect::<Vec<_>>();
    values.sort_by_key(|value| (value.writer_session_id, value.writer_epoch));
    values
}

/// Outcome of one retention pass over a range directory: how many sealed
/// spans were deleted, how many bytes they held, and where the retained
/// floor now sits. `skipped` names spans the sweep could not decide on
/// this pass (a remote-only manifest still missing its ingest bound, or a
/// segment whose manifest could not be verified) — they retry next tick.
#[derive(Clone, Debug, Default)]
pub(crate) struct ExpireReport {
    pub deleted_segments: u64,
    pub reclaimed_bytes: u64,
    pub retained_from: u64,
    pub skipped: Vec<String>,
}

/// The sealed spans the History Policy marks for deletion, as a single
/// contiguous prefix of the range. Retention deletes prefixes only — a
/// middle span never becomes a hole below retained history because read
/// scans resolve coverage position by position (the same prefix contract
/// Kafka's segment-level retention carries, derived deterministically
/// from manifest content so every replica decides identically).
fn retention_delete_set(
    directory: &Path,
    persisted: &PersistedRangeState,
    policy: &HistoryPolicy,
    now: i64,
    skipped: &mut Vec<String>,
) -> Result<Vec<(SegmentManifest, RetentionReason)>, ActiveRangeStoreError> {
    let floor = persisted.retained_from_position.unwrap_or(1).max(1);
    let mut manifests = Vec::new();
    for segment in 0..persisted.active_segment {
        match sealed_segment_manifest(directory, segment, persisted) {
            Ok(manifest) if manifest.last_position < floor => continue,
            Ok(manifest) => manifests.push(manifest),
            Err(error) => {
                skipped.push(format!("segment-{segment:06}: {error}"));
            }
        }
    }
    // Manifests arrive in segment order, which is position order.
    let expire_after = policy
        .expire_after_ns
        .and_then(|bound| i64::try_from(bound).ok());
    let mut reasons: Vec<Option<RetentionReason>> = manifests
        .iter()
        .map(|manifest| {
            let expired = expire_after.is_some_and(|expire| {
                manifest
                    .last_ingest_time_ns
                    .is_some_and(|ingest| ingest.saturating_add(expire) < now)
            });
            expired.then_some(RetentionReason::Expired)
        })
        .collect();
    // Budget trim: oldest sealed spans go first until survivors fit the
    // declared retained-bytes ceiling.
    if let Some(budget) = policy.max_retained_bytes {
        let mut retained: u64 = manifests.iter().map(|m| m.bytes).sum();
        for (index, manifest) in manifests.iter().enumerate() {
            if retained <= budget {
                break;
            }
            retained = retained.saturating_sub(manifest.bytes);
            if reasons[index].is_none() {
                reasons[index] = Some(RetentionReason::Budget);
            }
        }
    }
    // The delete set is the longest qualifying prefix.
    let mut delete_set = Vec::new();
    for (manifest, reason) in manifests.into_iter().zip(reasons.into_iter()) {
        match reason {
            Some(reason) => delete_set.push((manifest, reason)),
            None => break,
        }
    }
    Ok(delete_set)
}

/// Removes a deleted span's physical artifacts: remote objects (when an
/// object store is configured and a tier marker names them), the local
/// segment file, its checkpoint, and the tier marker itself. The
/// manifest and the retention marker stay — they are the durable proof
/// of what was deleted and where the floor sits. Missing files are
/// ignored so a retried pass is idempotent.
fn retention_delete_span(
    directory: &Path,
    manifest: &SegmentManifest,
    objects: Option<&dyn super::tier::ObjectStore>,
    report: &mut ExpireReport,
) {
    if let Some(store) = objects {
        if let Some(marker) = super::tier::read_tier_marker(directory, manifest.segment) {
            for key in [
                marker.object_key,
                marker.manifest_key,
                marker.checkpoint_key.unwrap_or_default(),
            ] {
                if key.is_empty() {
                    continue;
                }
                if let Err(error) = store.delete(&key) {
                    report.skipped.push(format!("object {key}: {error}"));
                }
            }
        }
    }
    for path in [
        segment_path(directory, manifest.segment),
        checkpoint_path(directory, manifest.segment),
        super::tier::tier_marker_path(directory, manifest.segment),
    ] {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => report.skipped.push(format!("{}: {error}", path.display())),
        }
    }
}

/// Filesystem-level expiry for a range whose store is not loaded: marker
/// writes, floor advance, state persist, then the physical deletes. The
/// running-store path (`expire_loaded`) performs the identical sequence
/// plus in-memory index updates under the range lock.
pub(crate) fn expire_range_directory(
    directory: &Path,
    persisted: &mut PersistedRangeState,
    policy: &HistoryPolicy,
    objects: Option<&dyn super::tier::ObjectStore>,
    now: i64,
) -> Result<ExpireReport, ActiveRangeStoreError> {
    let mut report = ExpireReport::default();
    let delete_set = retention_delete_set(directory, persisted, policy, now, &mut report.skipped)?;
    report.retained_from = persisted.retained_from_position.unwrap_or(1).max(1);
    if delete_set.is_empty() {
        return Ok(report);
    }
    let floor = delete_set
        .last()
        .map(|(manifest, _)| manifest.last_position)
        .and_then(|last| last.checked_add(1))
        .ok_or(ActiveRangeStoreError::PositionOverflow)?;
    for (manifest, reason) in &delete_set {
        write_retention_marker(directory, manifest, *reason, floor)?;
    }
    persisted.retained_from_position = Some(floor);
    persist_state(directory, persisted)?;
    report.retained_from = floor;
    report.deleted_segments = delete_set.len() as u64;
    report.reclaimed_bytes = delete_set.iter().map(|(m, _)| m.bytes).sum();
    for (manifest, _) in &delete_set {
        retention_delete_span(directory, manifest, objects, &mut report);
    }
    Ok(report)
}

/// The running-store expiry pass: identical durable sequence to
/// `expire_range_directory`, performed under the range lock so the
/// floor, the in-memory indexes, and the on-disk state move atomically.
/// Physical deletes run after the lock releases — a crash between leaves
/// orphaned files the next pass reclaims, never a torn state.
fn expire_loaded(
    inner: &FileActiveRangeStoreInner,
    policy: &HistoryPolicy,
    objects: Option<&dyn super::tier::ObjectStore>,
    now: i64,
) -> Result<ExpireReport, ActiveRangeStoreError> {
    let mut report = ExpireReport::default();
    let (delete_set, dropped_cache) = {
        let mut loaded = inner
            .loaded
            .lock()
            .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
        let delete_set = retention_delete_set(
            &inner.directory,
            &loaded.persisted,
            policy,
            now,
            &mut report.skipped,
        )?;
        report.retained_from = loaded.persisted.retained_from_position.unwrap_or(1).max(1);
        if delete_set.is_empty() {
            return Ok(report);
        }
        let floor = delete_set
            .last()
            .map(|(manifest, _)| manifest.last_position)
            .and_then(|last| last.checked_add(1))
            .ok_or(ActiveRangeStoreError::PositionOverflow)?;
        for (manifest, reason) in &delete_set {
            write_retention_marker(&inner.directory, manifest, *reason, floor)?;
        }
        // In-memory views drop below the floor before the floor persists:
        // a read that wins the lock between the two updates sees entries
        // with no floor, which is the safe direction (a stale read of
        // present bytes is still accurate data).
        loaded
            .remote
            .retain(|_, manifest| manifest.last_position >= floor);
        let mut dropped_cache = Vec::new();
        let kept: BTreeMap<u64, MaterializedSegment> = std::mem::take(&mut loaded.materialized)
            .into_iter()
            .filter(|(segment, span)| {
                if loaded.remote.contains_key(segment) {
                    true
                } else {
                    dropped_cache.push(span.path.clone());
                    false
                }
            })
            .collect();
        loaded.materialized = kept;
        loaded.materialized_bytes = loaded.materialized.values().map(|span| span.bytes).sum();
        loaded
            .entries
            .retain(|entry| entry.position.value() >= floor);
        loaded
            .cursor_positions
            .retain(|_, position| position.value() >= floor);
        loaded
            .digest_positions
            .retain(|_, position| position.value() >= floor);
        let mut surviving: std::collections::HashSet<u64> = loaded
            .entries
            .iter()
            .map(|entry| entry.position.value())
            .collect();
        for manifest in loaded.remote.values() {
            surviving.extend(manifest.first_position..=manifest.last_position);
        }
        loaded.deduplication = merge_deduplication(
            deduplication_from_entries(&loaded.entries),
            loaded.persisted.deduplication.clone(),
            &surviving,
        );
        loaded.ingest_monotonic = loaded.remote.is_empty()
            && loaded
                .entries
                .windows(2)
                .all(|pair| pair[0].ingest_time_ns <= pair[1].ingest_time_ns);
        let mut next = loaded.persisted.clone();
        next.retained_from_position = Some(floor);
        next.deduplication = persisted_deduplication(&loaded.deduplication);
        persist_state(&inner.directory, &next)?;
        loaded.persisted = next;
        report.retained_from = floor;
        report.deleted_segments = delete_set.len() as u64;
        report.reclaimed_bytes = delete_set.iter().map(|(m, _)| m.bytes).sum();
        (delete_set, dropped_cache)
    };
    for (manifest, _) in &delete_set {
        retention_delete_span(&inner.directory, manifest, objects, &mut report);
    }
    // Remove materialized cache copies of deleted spans after the lock
    // released — the registry dropped them already, so these are orphans.
    for path in dropped_cache {
        let _ = fs::remove_file(path);
    }
    Ok(report)
}

fn persist_state(
    directory: &Path,
    state: &PersistedRangeState,
) -> Result<(), ActiveRangeStoreError> {
    let target = directory.join("range-state.json");
    let temporary = directory.join("range-state.json.tmp");
    let encoded = serde_json::to_vec(state)?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(&encoded)?;
    file.sync_all()?;
    fs::rename(&temporary, &target)?;
    sync_directory(directory)?;
    Ok(())
}

/// Checkpoint-only write for state whose durable record is the frame log
/// itself: appended/flushed watermarks and Writer dedup streams are
/// recomputed by recovery rescan, so the per-append path writes the
/// snapshot atomically (temporary file + rename) but skips the fsync
/// ceremony — a crash replays the tail and rebuilds the checkpoint.
/// Committed positions, topology epochs, and pending-effect markers take
/// `persist_state` instead: losing them would change observed semantics.
fn persist_state_lazy(
    directory: &Path,
    state: &PersistedRangeState,
) -> Result<(), ActiveRangeStoreError> {
    let target = directory.join("range-state.json");
    let temporary = directory.join("range-state.json.tmp");
    let encoded = serde_json::to_vec(state)?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(&encoded)?;
    fs::rename(&temporary, &target)?;
    Ok(())
}

#[cfg(unix)]
pub(crate) fn sync_directory(directory: &Path) -> Result<(), std::io::Error> {
    File::open(directory)?.sync_all()
}

#[cfg(not(unix))]
pub(crate) fn sync_directory(_directory: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

fn corrupt(path: &Path, reason: impl Into<String>) -> ActiveRangeStoreError {
    ActiveRangeStoreError::CorruptSegment {
        path: path.to_path_buf(),
        reason: reason.into(),
    }
}

struct ByteReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> ByteReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ActiveRangeStoreError> {
        let end = self.position.checked_add(length).ok_or_else(|| {
            ActiveRangeStoreError::InvalidState("entry length overflow".to_owned())
        })?;
        let value = self.bytes.get(self.position..end).ok_or_else(|| {
            ActiveRangeStoreError::InvalidState("range entry is truncated".to_owned())
        })?;
        self.position = end;
        Ok(value)
    }

    fn u32(&mut self) -> Result<u32, ActiveRangeStoreError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().map_err(
            |_| ActiveRangeStoreError::InvalidState("u32 field is truncated".to_owned()),
        )?))
    }

    fn u64(&mut self) -> Result<u64, ActiveRangeStoreError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().map_err(
            |_| ActiveRangeStoreError::InvalidState("u64 field is truncated".to_owned()),
        )?))
    }

    fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }
}

#[cfg(test)]
pub(crate) fn seed_committed_history(root: &Path, descriptor: &ActiveRangeDescriptor, count: u64) {
    seed_committed_history_shaped(
        root,
        descriptor,
        count,
        &|_| b"key".to_vec(),
        &|sequence| sequence as i64,
        0,
    );
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn seed_committed_history_shaped(
    root: &Path,
    descriptor: &ActiveRangeDescriptor,
    count: u64,
    key_for: &dyn Fn(u64) -> Vec<u8>,
    ingest_for: &dyn Fn(u64) -> i64,
    payload_len: usize,
) {
    drop(FileActiveRangeStore::open(root, descriptor.clone()).unwrap());
    let directory = range_directory(root, descriptor);
    let writer_session_id = Uuid::from_u128(3);
    let mut bytes = Vec::new();
    for sequence in 1..=count {
        let record = StoredRecord {
            message_id: Uuid::from_u128(100 + sequence as u128),
            producer_id: writer_session_id,
            producer_sequence: sequence,
            event_time_ns: sequence as i64,
            ingest_time_ns: ingest_for(sequence),
            key: key_for(sequence),
            payload: vec![0xAB; payload_len],
            metadata: std::collections::BTreeMap::new(),
        };
        let frame = crate::codec::encode_record(&record).unwrap();
        bytes.extend(
            encode_entry(&DecodedEntry {
                position: RangePosition::new(sequence),
                identity: AppendIdentity {
                    writer_session_id,
                    writer_epoch: 1,
                    sequence,
                },
                cursor: format!("cursor-{sequence}"),
                frame_digest: *blake3::hash(&frame).as_bytes(),
                ingest_time_ns: record.ingest_time_ns,
                message_id: record.message_id,
                frame,
            })
            .unwrap(),
        );
    }
    fs::write(segment_path(&directory, 0), bytes).unwrap();
    let mut persisted = PersistedRangeState::new(descriptor);
    persisted.appended = count;
    persisted.flushed = count;
    persisted.committed = count;
    persist_state(&directory, &persisted).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn restart_rebuilds_cursor_index_beyond_ten_thousand_records() {
        let root = tempfile::TempDir::new().unwrap();
        let descriptor = ActiveRangeDescriptor {
            feed_id: Uuid::from_u128(1),
            range_id: RangeId::from_uuid(Uuid::from_u128(2)),
            generation: RangeGeneration::new(1),
            ownership_epoch: OwnershipEpoch::new(1),
        };
        seed_committed_history(root.path(), &descriptor, 10_001);
        let reopened = FileActiveRangeStore::open(root.path(), descriptor).unwrap();
        let cursor = reopened
            .committed_position_for_cursor("cursor-10000")
            .await
            .unwrap();
        assert_eq!(cursor, Some(RangePosition::new(10_000)));
        let page = reopened.read_committed(cursor, 1).await.unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].cursor, "cursor-10001");
    }

    fn test_descriptor() -> ActiveRangeDescriptor {
        ActiveRangeDescriptor {
            feed_id: Uuid::from_u128(1),
            range_id: RangeId::from_uuid(Uuid::from_u128(2)),
            generation: RangeGeneration::new(1),
            ownership_epoch: OwnershipEpoch::new(1),
        }
    }

    #[tokio::test]
    async fn committed_ingest_boundary_resolves_within_the_committed_prefix() {
        let root = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        seed_committed_history_shaped(
            root.path(),
            &descriptor,
            10,
            &|_| b"key".to_vec(),
            &|sequence| sequence as i64 * 10,
            0,
        );
        let store = FileActiveRangeStore::open(root.path(), descriptor).unwrap();
        let committed = CommitPosition::new(10);

        // A mid-history threshold lands on the last frame ingested before it.
        match store
            .committed_ingest_boundary(45, committed)
            .await
            .unwrap()
        {
            IngestBoundary::Ordered { position, cursor } => {
                assert_eq!(position, Some(RangePosition::new(4)));
                assert_eq!(cursor, "cursor-4");
            }
            other => panic!("expected an ordered boundary, got {other:?}"),
        }
        // A threshold at-or-before the head resolves to the range-start
        // marker: no boundary position, empty cursor.
        match store
            .committed_ingest_boundary(10, committed)
            .await
            .unwrap()
        {
            IngestBoundary::Ordered { position, cursor } => {
                assert_eq!(position, None);
                assert!(cursor.is_empty());
            }
            other => panic!("expected an ordered boundary, got {other:?}"),
        }
        // A threshold beyond the tail resolves to the last committed frame.
        match store
            .committed_ingest_boundary(1_000, committed)
            .await
            .unwrap()
        {
            IngestBoundary::Ordered { position, cursor } => {
                assert_eq!(position, Some(RangePosition::new(10)));
                assert_eq!(cursor, "cursor-10");
            }
            other => panic!("expected an ordered boundary, got {other:?}"),
        }
        // The caller's committed boundary clamps the lookup: frames beyond
        // it are invisible even though they are indexed locally.
        match store
            .committed_ingest_boundary(1_000, CommitPosition::new(3))
            .await
            .unwrap()
        {
            IngestBoundary::Ordered { position, cursor } => {
                assert_eq!(position, Some(RangePosition::new(3)));
                assert_eq!(cursor, "cursor-3");
            }
            other => panic!("expected an ordered boundary, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unordered_ingest_history_disables_the_indexed_boundary() {
        let root = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        // A clock step-back inside the committed prefix breaks the
        // monotonicity the indexed lookup relies on.
        seed_committed_history_shaped(
            root.path(),
            &descriptor,
            10,
            &|_| b"key".to_vec(),
            &|sequence| {
                if sequence == 5 {
                    1
                } else {
                    sequence as i64 * 10
                }
            },
            0,
        );
        let store = FileActiveRangeStore::open(root.path(), descriptor).unwrap();
        assert_eq!(
            store
                .committed_ingest_boundary(45, CommitPosition::new(10))
                .await
                .unwrap(),
            IngestBoundary::Unordered
        );
    }

    #[tokio::test]
    async fn uncommitted_appends_do_not_extend_the_ingest_boundary() {
        let root = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        seed_committed_history_shaped(
            root.path(),
            &descriptor,
            5,
            &|_| b"key".to_vec(),
            &|sequence| sequence as i64 * 10,
            0,
        );
        let store = FileActiveRangeStore::open(root.path(), descriptor.clone()).unwrap();
        for sequence in 6..=7_u64 {
            let record = StoredRecord {
                message_id: Uuid::from_u128(500 + sequence as u128),
                producer_id: Uuid::from_u128(3),
                producer_sequence: sequence,
                event_time_ns: sequence as i64,
                ingest_time_ns: sequence as i64 * 10,
                key: b"key".to_vec(),
                payload: Vec::new(),
                metadata: std::collections::BTreeMap::new(),
            };
            store
                .append(ActiveRangeAppend {
                    generation: descriptor.generation,
                    ownership_epoch: descriptor.ownership_epoch,
                    expected_position: Some(RangePosition::new(sequence)),
                    identity: AppendIdentity {
                        writer_session_id: Uuid::from_u128(3),
                        writer_epoch: 1,
                        sequence,
                    },
                    cursor: format!("cursor-{sequence}"),
                    frame: crate::codec::encode_record(&record).unwrap(),
                    pending_effect: None,
                })
                .await
                .unwrap();
        }
        // The uncommitted tail exists in the local index but the supplied
        // committed boundary still clamps the probe.
        match store
            .committed_ingest_boundary(1_000, CommitPosition::new(5))
            .await
            .unwrap()
        {
            IngestBoundary::Ordered { position, cursor } => {
                assert_eq!(position, Some(RangePosition::new(5)));
                assert_eq!(cursor, "cursor-5");
            }
            other => panic!("expected an ordered boundary, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_step_back_in_an_uncommitted_append_disables_the_indexed_boundary() {
        let root = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        seed_committed_history(root.path(), &descriptor, 5);
        let store = FileActiveRangeStore::open(root.path(), descriptor.clone()).unwrap();
        let record = StoredRecord {
            message_id: Uuid::from_u128(600),
            producer_id: Uuid::from_u128(3),
            producer_sequence: 6,
            event_time_ns: 6,
            ingest_time_ns: 1,
            key: b"key".to_vec(),
            payload: Vec::new(),
            metadata: std::collections::BTreeMap::new(),
        };
        store
            .append(ActiveRangeAppend {
                generation: descriptor.generation,
                ownership_epoch: descriptor.ownership_epoch,
                expected_position: Some(RangePosition::new(6)),
                identity: AppendIdentity {
                    writer_session_id: Uuid::from_u128(3),
                    writer_epoch: 1,
                    sequence: 6,
                },
                cursor: "cursor-6".to_owned(),
                frame: crate::codec::encode_record(&record).unwrap(),
                pending_effect: None,
            })
            .await
            .unwrap();
        // The committed prefix is still ordered, but the index refuses to
        // serve while any indexed frame breaks monotonicity; callers fall
        // back to the committed-only frame scan.
        assert_eq!(
            store
                .committed_ingest_boundary(1_000, CommitPosition::new(5))
                .await
                .unwrap(),
            IngestBoundary::Unordered
        );
    }

    #[tokio::test]
    async fn persistence_failures_mark_the_replica_unhealthy_until_a_mutation_succeeds() {
        let root = tempfile::TempDir::new().unwrap();
        let store = FileActiveRangeStore::open(root.path(), test_descriptor()).unwrap();
        assert!(!store.persistence_failure());

        store
            .blocking_mutation(|_| -> Result<(), ActiveRangeStoreError> {
                Err(ActiveRangeStoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::StorageFull,
                    "no space left on device",
                )))
            })
            .await
            .unwrap_err();
        assert!(store.persistence_failure());

        // A logical rejection leaves the degraded state untouched: fencing
        // and sequencing errors say nothing about the replica's ability to
        // persist, so they neither set nor clear write health.
        store
            .blocking_mutation(|_| -> Result<(), ActiveRangeStoreError> {
                Err(ActiveRangeStoreError::OwnershipEpochDidNotAdvance)
            })
            .await
            .unwrap_err();
        assert!(store.persistence_failure());

        // The next successful mutation proves the replica persists again.
        store
            .blocking_mutation(|_| -> Result<(), ActiveRangeStoreError> { Ok(()) })
            .await
            .unwrap();
        assert!(!store.persistence_failure());
    }

    #[tokio::test]
    async fn logical_rejections_do_not_degrade_a_healthy_replica() {
        let root = tempfile::TempDir::new().unwrap();
        let store = FileActiveRangeStore::open(root.path(), test_descriptor()).unwrap();

        store
            .blocking_mutation(|_| -> Result<(), ActiveRangeStoreError> {
                Err(ActiveRangeStoreError::StaleOwnershipEpoch {
                    current: OwnershipEpoch::new(2),
                    supplied: OwnershipEpoch::new(1),
                })
            })
            .await
            .unwrap_err();
        assert!(!store.persistence_failure());

        store
            .blocking_mutation(|_| -> Result<(), ActiveRangeStoreError> {
                Err(ActiveRangeStoreError::PositionGap {
                    expected: RangePosition::new(7),
                    supplied: RangePosition::new(9),
                })
            })
            .await
            .unwrap_err();
        assert!(!store.persistence_failure());
    }

    /// Appends and commits `count` records through the production store
    /// path so segment rotation, sealing, and manifest writes all run real
    /// write-path code.
    async fn append_committed_records(
        store: &FileActiveRangeStore,
        descriptor: &ActiveRangeDescriptor,
        start: u64,
        count: u64,
        payload_len: usize,
    ) {
        for sequence in start..start + count {
            let record = StoredRecord {
                message_id: Uuid::from_u128(700 + sequence as u128),
                producer_id: Uuid::from_u128(3),
                producer_sequence: sequence,
                event_time_ns: sequence as i64,
                ingest_time_ns: sequence as i64 * 10,
                key: b"key".to_vec(),
                payload: vec![0xCD; payload_len],
                metadata: std::collections::BTreeMap::new(),
            };
            store
                .append(ActiveRangeAppend {
                    generation: descriptor.generation,
                    ownership_epoch: descriptor.ownership_epoch,
                    expected_position: Some(RangePosition::new(sequence)),
                    identity: AppendIdentity {
                        writer_session_id: Uuid::from_u128(3),
                        writer_epoch: 1,
                        sequence,
                    },
                    cursor: format!("cursor-{sequence}"),
                    frame: crate::codec::encode_record(&record).unwrap(),
                    pending_effect: None,
                })
                .await
                .unwrap();
            store
                .commit(
                    descriptor.generation,
                    descriptor.ownership_epoch,
                    CommitPosition::new(sequence),
                )
                .await
                .unwrap();
        }
    }

    /// Opens a store whose tiny segment bound seals a segment after only a
    /// handful of committed entries, then appends until segment 0 rotates.
    async fn rotated_store(
        root: &Path,
        descriptor: &ActiveRangeDescriptor,
    ) -> FileActiveRangeStore {
        let store = FileActiveRangeStore::open_with_options(
            root,
            descriptor.clone(),
            FileActiveRangeStoreOptions {
                max_segment_bytes: 2_048,
                ..FileActiveRangeStoreOptions::default()
            },
        )
        .unwrap();
        let directory = range_directory(root, descriptor);
        let mut appended = 0_u64;
        while appended < 64 && !manifest_path(&directory, 0).exists() {
            append_committed_records(&store, descriptor, appended + 1, 1, 512).await;
            appended += 1;
        }
        assert!(
            manifest_path(&directory, 0).exists(),
            "64 committed entries never sealed segment 0"
        );
        store
    }

    #[tokio::test]
    async fn sealing_a_segment_writes_a_manifest_binding_its_content_digest() {
        let root = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = rotated_store(root.path(), &descriptor).await;
        let directory = range_directory(root.path(), &descriptor);

        let manifests = store.verify_sealed_segments().await.unwrap();
        assert_eq!(manifests.len(), 1);
        let manifest = &manifests[0];
        assert_eq!(manifest.version, SEGMENT_MANIFEST_VERSION);
        assert_eq!(manifest.feed_id, descriptor.feed_id);
        assert_eq!(manifest.range_id, descriptor.range_id);
        assert_eq!(manifest.generation, descriptor.generation);
        assert_eq!(manifest.segment, 0);
        assert_eq!(manifest.first_position, 1);
        assert_eq!(manifest.last_position, manifest.entries);

        let file_bytes = fs::read(segment_path(&directory, 0)).unwrap();
        assert_eq!(manifest.bytes as usize, file_bytes.len());
        assert_eq!(
            manifest.segment_digest,
            blake3::hash(&file_bytes).to_hex().to_string()
        );
    }

    #[tokio::test]
    async fn verify_fails_closed_on_a_tampered_sealed_segment() {
        let root = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = rotated_store(root.path(), &descriptor).await;
        let directory = range_directory(root.path(), &descriptor);
        let path = segment_path(&directory, 0);

        // Flip one committed byte mid-file: the per-frame digests still
        // index fine, but the whole-segment manifest digest must refuse.
        let mut bytes = fs::read(&path).unwrap();
        bytes[40] ^= 0xFF;
        fs::write(&path, &bytes).unwrap();
        match store.verify_sealed_segments().await {
            Err(ActiveRangeStoreError::CorruptSegment { reason, .. }) => {
                assert!(reason.contains("digest"));
            }
            other => panic!("expected CorruptSegment, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn verify_fails_closed_on_a_tampered_manifest() {
        let root = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = rotated_store(root.path(), &descriptor).await;
        let directory = range_directory(root.path(), &descriptor);

        // A manifest naming a foreign Feed identity no longer binds this
        // segment: verification treats it as missing/invalid.
        let raw = fs::read(manifest_path(&directory, 0)).unwrap();
        let mut manifest = serde_json::from_slice::<SegmentManifest>(&raw).unwrap();
        manifest.feed_id = Uuid::from_u128(999);
        fs::write(
            manifest_path(&directory, 0),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        match store.verify_sealed_segments().await {
            Err(ActiveRangeStoreError::CorruptSegment { reason, .. }) => {
                assert!(reason.contains("manifest"));
            }
            other => panic!("expected CorruptSegment, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn the_active_segment_never_carries_a_manifest() {
        let root = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = rotated_store(root.path(), &descriptor).await;
        let directory = range_directory(root.path(), &descriptor);

        // Segment 1 is the open tail: mutable, unsealed, manifest-free.
        let persisted: PersistedRangeState =
            serde_json::from_slice(&fs::read(directory.join("range-state.json")).unwrap()).unwrap();
        assert_eq!(persisted.active_segment, 1);
        assert!(!manifest_path(&directory, 1).exists());
        match verify_sealed_segment(&directory, 1, &persisted) {
            Err(ActiveRangeStoreError::CorruptSegment { reason, .. }) => {
                assert!(reason.contains("manifest"));
            }
            other => panic!("expected CorruptSegment, got {other:?}"),
        }
        assert!(store.verify_sealed_segments().await.unwrap().len() == 1);
    }

    #[tokio::test]
    async fn reopen_heals_a_missing_sealed_segment_manifest() {
        let root = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = rotated_store(root.path(), &descriptor).await;
        let directory = range_directory(root.path(), &descriptor);
        let original = store.verify_sealed_segments().await.unwrap()[0].clone();
        fs::remove_file(manifest_path(&directory, 0)).unwrap();
        drop(store);

        let reopened = FileActiveRangeStore::open(root.path(), descriptor.clone()).unwrap();
        let healed = reopened.verify_sealed_segments().await.unwrap();
        assert_eq!(healed.len(), 1);
        assert_eq!(healed[0].segment_digest, original.segment_digest);
        assert_eq!(healed[0].bytes, original.bytes);
        assert_eq!(healed[0].entries, original.entries);
    }

    #[tokio::test]
    async fn successive_sealed_segments_each_carry_a_verified_manifest() {
        let root = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = rotated_store(root.path(), &descriptor).await;
        let directory = range_directory(root.path(), &descriptor);
        let persisted: PersistedRangeState =
            serde_json::from_slice(&fs::read(directory.join("range-state.json")).unwrap()).unwrap();
        let mut appended = persisted.appended;

        // Drive a second rotation so both sealed segments verify.
        while appended < 128 && persisted_segment_count(&directory) < 2 {
            append_committed_records(&store, &descriptor, appended + 1, 1, 512).await;
            appended += 1;
        }
        let manifests = store.verify_sealed_segments().await.unwrap();
        assert!(manifests.len() >= 2);
        for (index, manifest) in manifests.iter().enumerate() {
            assert_eq!(manifest.segment, index as u64);
        }
        assert_eq!(manifests[1].first_position, manifests[0].last_position + 1);
    }

    fn persisted_segment_count(directory: &Path) -> usize {
        fs::read_dir(directory)
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("segment-manifest-")
            })
            .count()
    }

    // --- remote-only sealed segments + hot cache ---

    use crate::active_range::tier::{FileSystemObjectStore, ObjectStore, TierSource};

    /// Counting `get` wrapper so fetch-vs-cache-hit behavior is provable.
    struct CountingGetStore {
        inner: FileSystemObjectStore,
        gets: Mutex<std::collections::HashMap<String, u64>>,
    }

    impl CountingGetStore {
        fn open(root: &Path) -> Self {
            Self {
                inner: FileSystemObjectStore::open(root).unwrap(),
                gets: Mutex::new(std::collections::HashMap::new()),
            }
        }

        fn gets(&self, key: &str) -> u64 {
            *self.gets.lock().unwrap().get(key).unwrap_or(&0)
        }
    }

    impl std::fmt::Debug for CountingGetStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("CountingGetStore")
        }
    }

    impl ObjectStore for CountingGetStore {
        fn put(
            &self,
            key: &str,
            source: &Path,
        ) -> Result<u64, crate::active_range::tier::TierError> {
            self.inner.put(key, source)
        }

        fn get(&self, key: &str) -> Result<Vec<u8>, crate::active_range::tier::TierError> {
            *self.gets.lock().unwrap().entry(key.to_owned()).or_insert(0) += 1;
            self.inner.get(key)
        }

        fn head(&self, key: &str) -> Result<Option<u64>, crate::active_range::tier::TierError> {
            self.inner.head(key)
        }

        fn delete(&self, key: &str) -> Result<(), crate::active_range::tier::TierError> {
            self.inner.delete(key)
        }
    }

    fn remote_options(
        store: Arc<dyn ObjectStore>,
        remote_cache_bytes: u64,
    ) -> FileActiveRangeStoreOptions {
        FileActiveRangeStoreOptions {
            max_segment_bytes: 2_048,
            tier: Some(Arc::new(TierSource::with_store(store, remote_cache_bytes))),
            ..FileActiveRangeStoreOptions::default()
        }
    }

    /// Tiers `segments` to `object_root`, deletes their local log files,
    /// and returns their manifests — the exact state a range reopens into
    /// after immutable history demotes to object storage.
    fn demote_segments_to_remote(
        root: &Path,
        descriptor: &ActiveRangeDescriptor,
        object_root: &Path,
        segments: &[u64],
    ) -> Vec<SegmentManifest> {
        let objects = FileSystemObjectStore::open(object_root).unwrap();
        let report = crate::active_range::tier::tier_scan_once(root, &objects, &|_| None);
        assert!(
            report.failed.is_empty(),
            "tier scan failed: {:?}",
            report.failed
        );
        let directory = range_directory(root, descriptor);
        let persisted: PersistedRangeState =
            serde_json::from_slice(&fs::read(directory.join("range-state.json")).unwrap()).unwrap();
        let mut manifests = Vec::new();
        for segment in segments {
            assert!(
                crate::active_range::tier::read_tier_marker(&directory, *segment).is_some(),
                "segment {segment} was not tiered"
            );
            manifests.push(read_segment_manifest(&directory, *segment, &persisted).unwrap());
            fs::remove_file(segment_path(&directory, *segment)).unwrap();
        }
        manifests
    }

    #[tokio::test]
    async fn expiry_deletes_the_sealed_prefix_and_advances_the_retained_floor() {
        let root = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = rotated_store(root.path(), &descriptor).await;
        let directory = range_directory(root.path(), &descriptor);
        let committed = store.read_committed(None, 10_000).await.unwrap();
        let persisted: PersistedRangeState =
            serde_json::from_slice(&fs::read(directory.join("range-state.json")).unwrap()).unwrap();
        let manifest0 = read_segment_manifest(&directory, 0, &persisted).unwrap();

        let policy = HistoryPolicy {
            expire_after_ns: Some(1),
            ..HistoryPolicy::default()
        };
        let report = store
            .apply_retention(&policy, None, i64::MAX)
            .await
            .unwrap();
        assert_eq!(report.deleted_segments, 1);
        assert_eq!(report.retained_from, manifest0.last_position + 1);

        // The marker is the durable tombstone; the bytes are gone.
        let marker = read_retention_marker(&directory, 0).unwrap();
        assert_eq!(marker.reason, RetentionReason::Expired);
        assert_eq!(marker.last_position, manifest0.last_position);
        assert_eq!(marker.retained_from_position, report.retained_from);
        assert!(!segment_path(&directory, 0).exists());
        assert!(manifest_path(&directory, 0).exists());

        // Open-ended history reads clamp to the retained beginning; a
        // resume below the floor fails closed rather than skipping lost
        // positions.
        let page = store.read_committed(None, 10_000).await.unwrap();
        assert_eq!(
            page.first().unwrap().position,
            RangePosition::new(report.retained_from)
        );
        match store.read_committed(Some(RangePosition::new(1)), 1).await {
            Err(ActiveRangeStoreError::RetentionExpired {
                position,
                retained_from,
            }) => {
                assert_eq!(position, 2);
                assert_eq!(retained_from, report.retained_from);
            }
            other => panic!("expected RetentionExpired, got {other:?}"),
        }
        // Deleted cursors resolve as unknown — dedup/index state for the
        // span was dropped with it.
        assert_eq!(
            store
                .committed_position_for_cursor("cursor-1")
                .await
                .unwrap(),
            None
        );

        // Restart: recovery tolerates the absent span via the marker and
        // serves only retained history.
        drop(store);
        let reopened = FileActiveRangeStore::open(root.path(), descriptor).unwrap();
        let page = reopened.read_committed(None, 10_000).await.unwrap();
        assert_eq!(
            page.len() as u64,
            committed.len() as u64 - manifest0.entries
        );
    }

    #[tokio::test]
    async fn budget_trims_oldest_sealed_spans_first() {
        let root = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = rotated_store(root.path(), &descriptor).await;
        let directory = range_directory(root.path(), &descriptor);
        // Force a second seal so two sealed spans exist.
        let mut appended = store.read_committed(None, 10_000).await.unwrap().len() as u64;
        let mut spins = 0_u64;
        while spins < 128 && !manifest_path(&directory, 1).exists() {
            append_committed_records(&store, &descriptor, appended + 1, 1, 512).await;
            appended += 1;
            spins += 1;
        }
        assert!(
            manifest_path(&directory, 1).exists(),
            "segment 1 never sealed"
        );
        let persisted: PersistedRangeState =
            serde_json::from_slice(&fs::read(directory.join("range-state.json")).unwrap()).unwrap();
        let manifest1 = read_segment_manifest(&directory, 1, &persisted).unwrap();

        // Budget fits only the newest span — the oldest is reclaimed.
        let policy = HistoryPolicy {
            max_retained_bytes: Some(manifest1.bytes),
            ..HistoryPolicy::default()
        };
        let report = store
            .apply_retention(&policy, None, i64::MAX)
            .await
            .unwrap();
        assert_eq!(report.deleted_segments, 1);
        let marker = read_retention_marker(&directory, 0).unwrap();
        assert_eq!(marker.reason, RetentionReason::Budget);
        assert!(segment_path(&directory, 1).exists());
    }

    #[tokio::test]
    async fn expiry_never_deletes_the_active_segment_or_splits_a_prefix() {
        let root = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = FileActiveRangeStore::open_with_options(
            root.path(),
            descriptor.clone(),
            FileActiveRangeStoreOptions {
                max_segment_bytes: 2_048,
                ..FileActiveRangeStoreOptions::default()
            },
        )
        .unwrap();
        let directory = range_directory(root.path(), &descriptor);
        // Segment 0 seals holding FRESH ingest times; segment 1 seals
        // holding OLD ones — expiry qualifies the younger segment while
        // the elder survives, so the delete set is not a prefix.
        let mut appended = 0_u64;
        while appended < 64 && !manifest_path(&directory, 0).exists() {
            append_committed_records_ingest(&store, &descriptor, appended + 1, 1, 512, 10_000)
                .await;
            appended += 1;
        }
        assert!(manifest_path(&directory, 0).exists());
        let mut spins = 0_u64;
        while spins < 128 && !manifest_path(&directory, 1).exists() {
            append_committed_records_ingest(&store, &descriptor, appended + 1, 1, 512, 1).await;
            appended += 1;
            spins += 1;
        }
        assert!(
            manifest_path(&directory, 1).exists(),
            "segment 1 never sealed"
        );

        let policy = HistoryPolicy {
            expire_after_ns: Some(100),
            ..HistoryPolicy::default()
        };
        // now=5_000: segment 1's ingest (1) expired; segment 0's (10_000) did not.
        let report = store.apply_retention(&policy, None, 5_000).await.unwrap();
        assert_eq!(
            report.deleted_segments, 0,
            "a non-prefix delete must never open a hole in retained history"
        );
        assert_eq!(report.retained_from, 1);
        assert!(segment_path(&directory, 0).exists());
        assert!(segment_path(&directory, 1).exists());

        // And the active segment itself is never eligible even when its
        // records are old — only sealed spans expire.
        let fresh = FileActiveRangeStore::open(
            root.path().join("fresh"),
            ActiveRangeDescriptor {
                feed_id: Uuid::from_u128(9),
                range_id: RangeId::from_uuid(Uuid::from_u128(9)),
                generation: RangeGeneration::new(1),
                ownership_epoch: OwnershipEpoch::new(1),
            },
        )
        .unwrap();
        append_committed_records(
            &fresh,
            &ActiveRangeDescriptor {
                feed_id: Uuid::from_u128(9),
                range_id: RangeId::from_uuid(Uuid::from_u128(9)),
                generation: RangeGeneration::new(1),
                ownership_epoch: OwnershipEpoch::new(1),
            },
            1,
            3,
            64,
        )
        .await;
        let report = fresh
            .apply_retention(&policy, None, i64::MAX)
            .await
            .unwrap();
        assert_eq!(report.deleted_segments, 0);
        assert_eq!(fresh.read_committed(None, 10).await.unwrap().len(), 3);
    }

    async fn append_committed_records_ingest(
        store: &FileActiveRangeStore,
        descriptor: &ActiveRangeDescriptor,
        start: u64,
        count: u64,
        payload_len: usize,
        ingest_time_ns: i64,
    ) {
        for sequence in start..start + count {
            let record = StoredRecord {
                message_id: Uuid::from_u128(900 + sequence as u128),
                producer_id: Uuid::from_u128(3),
                producer_sequence: sequence,
                event_time_ns: sequence as i64,
                ingest_time_ns,
                key: b"key".to_vec(),
                payload: vec![0xCD; payload_len],
                metadata: std::collections::BTreeMap::new(),
            };
            store
                .append(ActiveRangeAppend {
                    generation: descriptor.generation,
                    ownership_epoch: descriptor.ownership_epoch,
                    expected_position: Some(RangePosition::new(sequence)),
                    identity: AppendIdentity {
                        writer_session_id: Uuid::from_u128(3),
                        writer_epoch: 1,
                        sequence,
                    },
                    cursor: format!("cursor-{sequence}"),
                    frame: crate::codec::encode_record(&record).unwrap(),
                    pending_effect: None,
                })
                .await
                .unwrap();
            store
                .commit(
                    descriptor.generation,
                    descriptor.ownership_epoch,
                    CommitPosition::new(sequence),
                )
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn closed_range_directory_expires_without_a_live_store() {
        let root = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = rotated_store(root.path(), &descriptor).await;
        let directory = range_directory(root.path(), &descriptor);
        drop(store);
        let mut persisted: PersistedRangeState =
            serde_json::from_slice(&fs::read(directory.join("range-state.json")).unwrap()).unwrap();
        let manifest0 = read_segment_manifest(&directory, 0, &persisted).unwrap();

        let report = expire_range_directory(
            &directory,
            &mut persisted,
            &HistoryPolicy {
                expire_after_ns: Some(1),
                ..HistoryPolicy::default()
            },
            None,
            i64::MAX,
        )
        .unwrap();
        assert_eq!(report.deleted_segments, 1);
        // The state file advanced durably even though no store was open.
        let on_disk: PersistedRangeState =
            serde_json::from_slice(&fs::read(directory.join("range-state.json")).unwrap()).unwrap();
        assert_eq!(
            on_disk.retained_from_position,
            Some(manifest0.last_position + 1)
        );
        let reopened = FileActiveRangeStore::open(root.path(), descriptor).unwrap();
        let page = reopened.read_committed(None, 10_000).await.unwrap();
        assert_eq!(
            page.first().unwrap().position,
            RangePosition::new(manifest0.last_position + 1)
        );
    }

    #[tokio::test]
    async fn expiry_deletes_remote_objects_and_drops_the_remote_registry() {
        let root = tempfile::TempDir::new().unwrap();
        let objects = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = rotated_store(root.path(), &descriptor).await;
        let directory = range_directory(root.path(), &descriptor);
        drop(store);
        demote_segments_to_remote(root.path(), &descriptor, objects.path(), &[0]);

        let mut persisted: PersistedRangeState =
            serde_json::from_slice(&fs::read(directory.join("range-state.json")).unwrap()).unwrap();
        let marker = crate::active_range::tier::read_tier_marker(&directory, 0).unwrap();
        let store = FileSystemObjectStore::open(objects.path()).unwrap();
        let report = expire_range_directory(
            &directory,
            &mut persisted,
            &HistoryPolicy {
                expire_after_ns: Some(1),
                ..HistoryPolicy::default()
            },
            Some(&store),
            i64::MAX,
        )
        .unwrap();
        assert_eq!(report.deleted_segments, 1);
        // Every remote artifact for the span is gone, idempotently.
        for key in [marker.object_key, marker.manifest_key] {
            assert!(store.head(&key).unwrap().is_none(), "object {key} survived");
        }
        if let Some(key) = marker.checkpoint_key {
            assert!(store.head(&key).unwrap().is_none(), "object {key} survived");
        }
        // A remote-only span that was deleted cannot be "not tiered" —
        // the retention marker, not the tier marker, proves the absence.
        assert!(read_retention_marker(&directory, 0).is_some());
        assert!(crate::active_range::tier::read_tier_marker(&directory, 0).is_none());
    }

    #[tokio::test]
    async fn remote_only_segment_opens_and_serves_committed_reads() {
        let root = tempfile::TempDir::new().unwrap();
        let objects = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = rotated_store(root.path(), &descriptor).await;
        let committed = store.read_committed(None, 10_000).await.unwrap().len() as u64;
        drop(store);
        let manifests = demote_segments_to_remote(root.path(), &descriptor, objects.path(), &[0]);

        let reopened = FileActiveRangeStore::open_with_options(
            root.path(),
            descriptor.clone(),
            remote_options(
                Arc::new(FileSystemObjectStore::open(objects.path()).unwrap()),
                1 << 30,
            ),
        )
        .unwrap();
        let frames = reopened.read_committed(None, 10_000).await.unwrap();
        assert_eq!(frames.len() as u64, committed);
        for (index, frame) in frames.iter().enumerate() {
            assert_eq!(frame.position.value(), index as u64 + 1);
        }
        // The remote span's frames carry the manifest's coverage.
        assert_eq!(
            frames[manifests[0].first_position as usize - 1]
                .position
                .value(),
            manifests[0].first_position
        );
        // A cursor buried in the remote span still resolves — the fetch
        // repopulates the position index on materialize.
        let position = reopened
            .committed_position_for_cursor("cursor-1")
            .await
            .unwrap();
        assert_eq!(position, Some(RangePosition::new(1)));
    }

    #[tokio::test]
    async fn missing_sealed_segment_without_tier_marker_fails_closed() {
        let root = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = rotated_store(root.path(), &descriptor).await;
        drop(store);
        let directory = range_directory(root.path(), &descriptor);
        // Manifest present but no verified remote copy: fail closed.
        fs::remove_file(segment_path(&directory, 0)).unwrap();
        let result = FileActiveRangeStore::open(root.path(), descriptor);
        assert!(matches!(
            result.err(),
            Some(ActiveRangeStoreError::CorruptSegment { .. })
        ));
    }

    #[tokio::test]
    async fn remote_read_without_tier_source_reports_unavailable() {
        let root = tempfile::TempDir::new().unwrap();
        let objects = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = rotated_store(root.path(), &descriptor).await;
        drop(store);
        demote_segments_to_remote(root.path(), &descriptor, objects.path(), &[0]);

        // Open succeeds — provenance is intact — but reads have nowhere to
        // fetch from and must say so rather than silently skip history.
        let reopened = FileActiveRangeStore::open(root.path(), descriptor).unwrap();
        let error = reopened.read_committed(None, 16).await.unwrap_err();
        assert!(
            matches!(error, ActiveRangeStoreError::RemoteFetch(ref message) if message.contains("no object store")),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn writer_dedup_survives_remote_demotion() {
        let root = tempfile::TempDir::new().unwrap();
        let objects = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = rotated_store(root.path(), &descriptor).await;
        drop(store);
        demote_segments_to_remote(root.path(), &descriptor, objects.path(), &[0]);

        let reopened = FileActiveRangeStore::open_with_options(
            root.path(),
            descriptor.clone(),
            remote_options(
                Arc::new(FileSystemObjectStore::open(objects.path()).unwrap()),
                1 << 30,
            ),
        )
        .unwrap();
        // Replay append sequence 1 byte-for-byte — its position lives in
        // the demoted segment, so dedup must materialize, byte-compare,
        // and return the original position rather than a duplicate.
        let record = StoredRecord {
            message_id: Uuid::from_u128(701),
            producer_id: Uuid::from_u128(3),
            producer_sequence: 1,
            event_time_ns: 1,
            ingest_time_ns: 10,
            key: b"key".to_vec(),
            payload: vec![0xCD; 512],
            metadata: std::collections::BTreeMap::new(),
        };
        let result = reopened
            .append(ActiveRangeAppend {
                generation: descriptor.generation,
                ownership_epoch: descriptor.ownership_epoch,
                expected_position: Some(RangePosition::new(1)),
                identity: AppendIdentity {
                    writer_session_id: Uuid::from_u128(3),
                    writer_epoch: 1,
                    sequence: 1,
                },
                cursor: "cursor-1".to_owned(),
                frame: crate::codec::encode_record(&record).unwrap(),
                pending_effect: None,
            })
            .await
            .unwrap();
        assert!(result.deduplicated);
        assert_eq!(result.position, RangePosition::new(1));
    }

    #[tokio::test]
    async fn cache_budget_demotes_coldest_remote_segment() {
        let root = tempfile::TempDir::new().unwrap();
        let objects_root = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = rotated_store(root.path(), &descriptor).await;
        let directory = range_directory(root.path(), &descriptor);
        let persisted: PersistedRangeState =
            serde_json::from_slice(&fs::read(directory.join("range-state.json")).unwrap()).unwrap();
        let mut appended = persisted.appended;
        while appended < 128 && persisted_segment_count(&directory) < 2 {
            append_committed_records(&store, &descriptor, appended + 1, 1, 512).await;
            appended += 1;
        }
        assert_eq!(persisted_segment_count(&directory), 2);
        drop(store);
        let manifests =
            demote_segments_to_remote(root.path(), &descriptor, objects_root.path(), &[0, 1]);

        // Budget fits exactly one segment: reading span 0 then span 1
        // demotes 0; reading 0 again costs a second remote get.
        let counting = Arc::new(CountingGetStore::open(objects_root.path()));
        let reopened = FileActiveRangeStore::open_with_options(
            root.path(),
            descriptor.clone(),
            remote_options(counting.clone(), manifests[0].bytes),
        )
        .unwrap();
        let key0 = crate::active_range::tier::segment_object_key(&manifests[0]);
        let key1 = crate::active_range::tier::segment_object_key(&manifests[1]);

        // Reads walk contiguous positions, so each read's limit is the
        // span's own coverage — a longer limit would legitimately fetch
        // the next span inside the same call.
        let span0 = manifests[0].entries as usize;
        let first = reopened
            .read_committed(
                Some(RangePosition::new(manifests[0].first_position - 1)),
                span0,
            )
            .await
            .unwrap();
        assert_eq!(first[0].position.value(), manifests[0].first_position);
        assert_eq!(counting.gets(&key0), 1);
        let cache0 = directory.join("cache-segment-000000.log");
        assert!(cache0.exists());

        let second = reopened
            .read_committed(
                Some(RangePosition::new(manifests[1].first_position - 1)),
                manifests[1].entries as usize,
            )
            .await
            .unwrap();
        assert_eq!(second[0].position.value(), manifests[1].first_position);
        assert_eq!(counting.gets(&key1), 1);
        // Demotion evicted span 0 back to remote — file and entries gone.
        assert!(!cache0.exists());

        let third = reopened
            .read_committed(
                Some(RangePosition::new(manifests[0].first_position - 1)),
                span0,
            )
            .await
            .unwrap();
        assert_eq!(third[0].position.value(), manifests[0].first_position);
        assert_eq!(counting.gets(&key0), 2);
    }

    #[tokio::test]
    async fn remote_fetch_rejects_forged_object_bytes() {
        let root = tempfile::TempDir::new().unwrap();
        let objects_root = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = rotated_store(root.path(), &descriptor).await;
        drop(store);
        let manifests =
            demote_segments_to_remote(root.path(), &descriptor, objects_root.path(), &[0]);
        // An attacker/corruption rewrites the object under its
        // content-addressed key — the digest mismatch must refuse
        // admission rather than serve poisoned history.
        let key = crate::active_range::tier::segment_object_key(&manifests[0]);
        fs::write(objects_root.path().join(&key), b"forged bytes").unwrap();

        let reopened = FileActiveRangeStore::open_with_options(
            root.path(),
            descriptor,
            remote_options(
                Arc::new(FileSystemObjectStore::open(objects_root.path()).unwrap()),
                1 << 30,
            ),
        )
        .unwrap();
        let error = reopened.read_committed(None, 16).await.unwrap_err();
        assert!(
            matches!(error, ActiveRangeStoreError::RemoteFetch(_)),
            "unexpected error: {error}"
        );
        let directory = range_directory(root.path(), &test_descriptor());
        assert!(!directory.join("cache-segment-000000.log").exists());
    }

    #[tokio::test]
    async fn cache_file_survives_reopen_without_remote_read() {
        let root = tempfile::TempDir::new().unwrap();
        let objects_root = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = rotated_store(root.path(), &descriptor).await;
        drop(store);
        let manifests =
            demote_segments_to_remote(root.path(), &descriptor, objects_root.path(), &[0]);

        let counting = Arc::new(CountingGetStore::open(objects_root.path()));
        let key = crate::active_range::tier::segment_object_key(&manifests[0]);
        let options = remote_options(counting.clone(), 1 << 30);
        {
            let reopened = FileActiveRangeStore::open_with_options(
                root.path(),
                descriptor.clone(),
                options.clone(),
            )
            .unwrap();
            reopened.read_committed(None, 4).await.unwrap();
            assert_eq!(counting.gets(&key), 1);
        }
        // Reopen with a fresh source over the same backend: the cache file
        // the first run installed verifies against the manifest and is
        // adopted without another remote get. The read returns the whole
        // committed prefix — three frames here — and the object counter
        // must not move.
        let reopened =
            FileActiveRangeStore::open_with_options(root.path(), descriptor, options).unwrap();
        let frames = reopened.read_committed(None, 16).await.unwrap();
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].position, RangePosition::new(1));
        assert_eq!(counting.gets(&key), 1);
    }

    /// Appends through the production path until `sealed` manifests exist —
    /// the multi-span remote history prefetch tests need.
    async fn multi_sealed_store(
        root: &Path,
        descriptor: &ActiveRangeDescriptor,
        sealed: u64,
    ) -> FileActiveRangeStore {
        let store = FileActiveRangeStore::open_with_options(
            root,
            descriptor.clone(),
            FileActiveRangeStoreOptions {
                max_segment_bytes: 2_048,
                ..FileActiveRangeStoreOptions::default()
            },
        )
        .unwrap();
        let directory = range_directory(root, descriptor);
        let mut appended = 0_u64;
        while appended < 256
            && (0..sealed).any(|segment| !manifest_path(&directory, segment).exists())
        {
            append_committed_records(&store, descriptor, appended + 1, 1, 512).await;
            appended += 1;
        }
        for segment in 0..sealed {
            assert!(
                manifest_path(&directory, segment).exists(),
                "256 appends never sealed segment {segment}"
            );
        }
        store
    }

    /// Polls until a remote span materializes — the prefetch thread is
    /// detached, so the assertion converges rather than assuming a tick.
    fn materialized_within(store: &FileActiveRangeStore, segment: u64, attempts: u32) -> bool {
        for _ in 0..attempts {
            if store
                .inner
                .loaded
                .lock()
                .unwrap()
                .materialized
                .contains_key(&segment)
            {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        false
    }

    #[tokio::test]
    async fn remote_reads_prefetch_the_next_sealed_spans() {
        let root = tempfile::TempDir::new().unwrap();
        let objects = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = multi_sealed_store(root.path(), &descriptor, 3).await;
        let committed = store.read_committed(None, 10_000).await.unwrap().len() as u64;
        drop(store);
        demote_segments_to_remote(root.path(), &descriptor, objects.path(), &[0, 1, 2]);

        let reopened = FileActiveRangeStore::open_with_options(
            root.path(),
            descriptor.clone(),
            remote_options(
                Arc::new(FileSystemObjectStore::open(objects.path()).unwrap()),
                1 << 30,
            ),
        )
        .unwrap();
        // One frame from span 0 is enough to trigger read-ahead.
        let frames = reopened.read_committed(None, 1).await.unwrap();
        assert_eq!(frames.len(), 1);
        assert!(materialized_within(&reopened, 0, 1));
        assert!(materialized_within(&reopened, 1, 200));
        assert!(materialized_within(&reopened, 2, 200));
        // The warm spans serve without another fetch pass.
        let all = reopened.read_committed(None, 10_000).await.unwrap();
        assert_eq!(all.len() as u64, committed);
    }

    #[tokio::test]
    async fn prefetch_stays_inside_the_remote_cache_budget() {
        let root = tempfile::TempDir::new().unwrap();
        let objects = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = multi_sealed_store(root.path(), &descriptor, 3).await;
        drop(store);
        let manifests =
            demote_segments_to_remote(root.path(), &descriptor, objects.path(), &[0, 1, 2]);
        // Budget admits span 0 plus span 1 only — span 2 must not be
        // prefetched past the budget even though read-ahead wants it.
        let budget = manifests[0].bytes + manifests[1].bytes;

        let reopened = FileActiveRangeStore::open_with_options(
            root.path(),
            descriptor.clone(),
            remote_options(
                Arc::new(FileSystemObjectStore::open(objects.path()).unwrap()),
                budget,
            ),
        )
        .unwrap();
        reopened.read_committed(None, 1).await.unwrap();
        assert!(materialized_within(&reopened, 0, 1));
        assert!(materialized_within(&reopened, 1, 200));
        // Span 2 may never materialize — give the prefetch thread ample
        // time to prove it is not coming.
        assert!(!materialized_within(&reopened, 2, 50));
    }

    #[tokio::test]
    async fn a_tail_read_never_prefetches_remote_history() {
        let root = tempfile::TempDir::new().unwrap();
        let objects = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = multi_sealed_store(root.path(), &descriptor, 3).await;
        drop(store);
        let manifests =
            demote_segments_to_remote(root.path(), &descriptor, objects.path(), &[0, 1]);

        let reopened = FileActiveRangeStore::open_with_options(
            root.path(),
            descriptor.clone(),
            remote_options(
                Arc::new(FileSystemObjectStore::open(objects.path()).unwrap()),
                1 << 30,
            ),
        )
        .unwrap();
        // Read only the live tail — the remote spans all sit before the
        // served position, so prefetch selects nothing and no fetch runs.
        let after = Some(RangePosition::new(manifests[1].last_position));
        let frames = reopened.read_committed(after, 16).await.unwrap();
        assert!(!frames.is_empty());
        assert!(frames.iter().all(|frame| {
            !manifests.iter().any(|m| {
                m.first_position <= frame.position.value()
                    && frame.position.value() <= m.last_position
            })
        }));
        assert!(
            reopened
                .inner
                .loaded
                .lock()
                .unwrap()
                .materialized
                .is_empty(),
            "a local-tail read fetched remote history"
        );
    }

    #[tokio::test]
    async fn a_failed_prefetch_never_fails_the_read_that_triggered_it() {
        let root = tempfile::TempDir::new().unwrap();
        let objects = tempfile::TempDir::new().unwrap();
        let descriptor = test_descriptor();
        let store = multi_sealed_store(root.path(), &descriptor, 3).await;
        let span0_last = {
            let directory = range_directory(root.path(), &descriptor);
            let persisted: PersistedRangeState =
                serde_json::from_slice(&fs::read(directory.join("range-state.json")).unwrap())
                    .unwrap();
            read_segment_manifest(&directory, 0, &persisted)
                .unwrap()
                .last_position
        };
        drop(store);
        let manifests =
            demote_segments_to_remote(root.path(), &descriptor, objects.path(), &[0, 1]);
        // Corrupt span 1's object after tiering — the verified fetch must
        // reject it, and the prefetch that hits it must die quietly.
        let damaged = objects
            .path()
            .join(crate::active_range::tier::segment_object_key(&manifests[1]));
        fs::write(&damaged, b"corrupted remote bytes").unwrap();

        let reopened = FileActiveRangeStore::open_with_options(
            root.path(),
            descriptor.clone(),
            remote_options(
                Arc::new(FileSystemObjectStore::open(objects.path()).unwrap()),
                1 << 30,
            ),
        )
        .unwrap();
        // Serve only span 0 — the triggered prefetch of span 1 fails on
        // the corrupted bytes but the foreground read must not notice.
        let frames = reopened.read_committed(None, 1).await.unwrap();
        assert_eq!(frames.len(), 1);
        assert!(materialized_within(&reopened, 0, 1));
        // Let the failed prefetch settle, then demand span 1: the read
        // fails closed on the corrupted object rather than serving it.
        std::thread::sleep(std::time::Duration::from_millis(200));
        let error = reopened
            .read_committed(Some(RangePosition::new(span0_last)), 8)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ActiveRangeStoreError::InvalidState(_)
                | ActiveRangeStoreError::CorruptSegment { .. }
                | ActiveRangeStoreError::RemoteFetch(_)
                | ActiveRangeStoreError::Io(_)
        ));
    }
}
