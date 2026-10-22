use std::{
    collections::HashMap,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileActiveRangeStoreOptions {
    pub max_segment_bytes: u64,
    pub max_range_bytes: Option<u64>,
}

impl Default for FileActiveRangeStoreOptions {
    fn default() -> Self {
        Self {
            max_segment_bytes: 256 * 1024 * 1024,
            max_range_bytes: None,
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
    loaded: Mutex<LoadedRange>,
    /// Set when a mutating store operation fails at the persistence layer —
    /// I/O, worker, or integrity errors — and cleared by the next successful
    /// mutation. Recovery probes read it as replica write health: a replica
    /// that cannot durably accept writes must not remain an Append Owner.
    persistence_failure: std::sync::atomic::AtomicBool,
}

struct LoadedRange {
    persisted: PersistedRangeState,
    active_file: File,
    entries: Vec<EntryIndex>,
    cursor_positions: HashMap<String, RangePosition>,
    digest_positions: HashMap<[u8; 32], RangePosition>,
    deduplication: HashMap<WriterKey, DeduplicationState>,
    /// True while committed ingest_time_ns is non-decreasing in position
    /// order; a backwards clock step disables indexed ingest seeks, which
    /// callers resolve by scanning instead.
    ingest_monotonic: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedRangeState {
    version: u32,
    feed_id: Uuid,
    range_id: RangeId,
    generation: RangeGeneration,
    ownership_epoch: OwnershipEpoch,
    appended: u64,
    flushed: u64,
    committed: u64,
    active_segment: u64,
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
struct EntryIndex {
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
        let (entries, recovered_deduplication) = recover_segments(&directory, &persisted)?;
        let surviving_positions = entries.iter().map(|entry| entry.position.value()).collect();
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
        let ingest_monotonic = entries
            .windows(2)
            .all(|pair| pair[0].ingest_time_ns <= pair[1].ingest_time_ns);
        let recovered_position = entries.last().map_or(0, |entry| entry.position.value());
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
                options,
                persistence_failure: std::sync::atomic::AtomicBool::new(false),
                loaded: Mutex::new(LoadedRange {
                    persisted,
                    active_file,
                    entries,
                    cursor_positions,
                    digest_positions,
                    deduplication: recovered_deduplication,
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

    /// True when the last mutating store operation failed at the persistence
    /// layer and no mutation has succeeded since.
    pub fn persistence_failure(&self) -> bool {
        self.inner
            .persistence_failure
            .load(std::sync::atomic::Ordering::Relaxed)
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
        self.blocking(move |inner| read_committed(inner, after, limit))
            .await
    }

    async fn read_committed_bounded(
        &self,
        after: Option<RangePosition>,
        limit: usize,
        max_bytes: usize,
    ) -> Result<Vec<StoredRangeFrame>, ActiveRangeStoreError> {
        self.blocking(move |inner| read_committed_bounded(inner, after, limit, max_bytes))
            .await
    }

    async fn read_uncommitted(
        &self,
        after: Option<RangePosition>,
        limit: usize,
    ) -> Result<Vec<StoredRangeFrame>, ActiveRangeStoreError> {
        self.blocking(move |inner| {
            let loaded = inner
                .loaded
                .lock()
                .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
            let after = after.map_or(0, RangePosition::value);
            let start = loaded
                .entries
                .partition_point(|entry| entry.position.value() <= after);
            loaded
                .entries
                .iter()
                .skip(start)
                .take_while(|entry| entry.position.value() <= loaded.persisted.appended)
                .take(limit.clamp(1, 10_000))
                .map(|entry| read_indexed_frame(&inner.directory, entry))
                .collect()
        })
        .await
    }

    async fn committed_position_for_cursor(
        &self,
        cursor: &str,
    ) -> Result<Option<RangePosition>, ActiveRangeStoreError> {
        let cursor = cursor.to_owned();
        self.blocking(move |inner| {
            let loaded = inner
                .loaded
                .lock()
                .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
            Ok(loaded
                .cursor_positions
                .get(&cursor)
                .copied()
                .filter(|position| position.value() <= loaded.persisted.committed))
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

fn segment_path(directory: &Path, segment: u64) -> PathBuf {
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
            let existing = loaded
                .entries
                .iter()
                .find(|entry| entry.position == supplied)
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
        let existing = loaded
            .entries
            .iter()
            .find(|entry| entry.position == position)
            .ok_or(ActiveRangeStoreError::PositionConflict(position))?;
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
        let existing = loaded
            .entries
            .iter()
            .find(|entry| entry.position == existing_position)
            .ok_or(ActiveRangeStoreError::PositionConflict(existing_position))?;
        let existing_frame = read_indexed_frame(&inner.directory, existing)?;
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
                    let existing = loaded
                        .entries
                        .iter()
                        .find(|entry| entry.position == previous.position)
                        .ok_or(ActiveRangeStoreError::WriterSequenceConflict {
                            latest: previous.sequence,
                        })?;
                    let existing_frame = read_indexed_frame(&inner.directory, existing)?;
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

fn read_committed(
    inner: &FileActiveRangeStoreInner,
    after: Option<RangePosition>,
    limit: usize,
) -> Result<Vec<StoredRangeFrame>, ActiveRangeStoreError> {
    let loaded = inner
        .loaded
        .lock()
        .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
    let after = after.map_or(0, RangePosition::value);
    let limit = limit.clamp(1, 10_000);
    let start = loaded
        .entries
        .partition_point(|entry| entry.position.value() <= after);
    loaded
        .entries
        .iter()
        .skip(start)
        .take_while(|entry| entry.position.value() <= loaded.persisted.committed)
        .take(limit)
        .map(|entry| read_indexed_frame(&inner.directory, entry))
        .collect()
}

fn read_committed_bounded(
    inner: &FileActiveRangeStoreInner,
    after: Option<RangePosition>,
    limit: usize,
    max_bytes: usize,
) -> Result<Vec<StoredRangeFrame>, ActiveRangeStoreError> {
    let loaded = inner
        .loaded
        .lock()
        .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
    let start = loaded
        .entries
        .partition_point(|entry| entry.position.value() <= after.map_or(0, RangePosition::value));
    let mut frames = Vec::new();
    let mut bytes = 0_usize;
    for entry in loaded
        .entries
        .iter()
        .skip(start)
        .take_while(|entry| entry.position.value() <= loaded.persisted.committed)
        .take(limit.clamp(1, 128))
    {
        let frame = read_indexed_frame(&inner.directory, entry)?;
        let size = crate::codec::logical_frame_size(&frame.frame)?;
        let next = bytes
            .checked_add(size)
            .ok_or(ActiveRangeStoreError::ReadBudgetExceeded)?;
        if next > max_bytes {
            if frames.is_empty() {
                return Err(ActiveRangeStoreError::ReadBudgetExceeded);
            }
            break;
        }
        frames.push(frame);
        bytes = next;
    }
    Ok(frames)
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
    }) {
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

fn recover_segments(
    directory: &Path,
    persisted: &PersistedRangeState,
) -> Result<(Vec<EntryIndex>, HashMap<WriterKey, DeduplicationState>), ActiveRangeStoreError> {
    let mut entries = Vec::new();
    let mut expected_position = 1_u64;
    for segment in 0..=persisted.active_segment {
        if segment < persisted.active_segment {
            if let Some(checkpointed) =
                load_segment_checkpoint(directory, segment, persisted, expected_position)
            {
                expected_position = expected_position
                    .checked_add(checkpointed.len() as u64)
                    .ok_or(ActiveRangeStoreError::PositionOverflow)?;
                entries.extend(checkpointed);
                continue;
            }
        }
        let path = segment_path(directory, segment);
        if !path.exists() {
            if segment == persisted.active_segment {
                File::create(&path)?.sync_all()?;
            } else {
                return Err(ActiveRangeStoreError::CorruptSegment {
                    path,
                    reason: "sealed segment is missing".to_owned(),
                });
            }
        }
        let allow_torn_tail = segment == persisted.active_segment;
        let recovered = recover_segment(&path, segment, allow_torn_tail)?;
        for entry in recovered {
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
    }
    let deduplication = deduplication_from_entries(&entries);
    Ok((entries, deduplication))
}

fn recover_segment(
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
    entry: &EntryIndex,
) -> Result<StoredRangeFrame, ActiveRangeStoreError> {
    let mut file = File::open(segment_path(directory, entry.segment))?;
    let frame_end = entry
        .frame_offset
        .checked_add(entry.frame_length)
        .ok_or(ActiveRangeStoreError::PositionOverflow)?;
    if frame_end > file.metadata()?.len() {
        return Err(corrupt(
            &segment_path(directory, entry.segment),
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
fn sync_directory(directory: &Path) -> Result<(), std::io::Error> {
    File::open(directory)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_directory: &Path) -> Result<(), std::io::Error> {
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
}
