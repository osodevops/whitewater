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
    #[error("Writer sequence was reused with different record bytes")]
    WriterSequenceConflict,
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
    #[error("storage worker failed: {0}")]
    Worker(String),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
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
    async fn committed_position_for_cursor(
        &self,
        cursor: &str,
    ) -> Result<Option<RangePosition>, ActiveRangeStoreError>;
    async fn frame_digest(
        &self,
        position: RangePosition,
    ) -> Result<Option<[u8; 32]>, ActiveRangeStoreError>;
    async fn truncate_uncommitted(
        &self,
        generation: RangeGeneration,
        ownership_epoch: OwnershipEpoch,
    ) -> Result<u64, ActiveRangeStoreError>;
    async fn update_ownership_epoch(
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
}

struct LoadedRange {
    persisted: PersistedRangeState,
    active_file: File,
    entries: Vec<EntryIndex>,
    cursor_positions: HashMap<String, RangePosition>,
    digest_positions: HashMap<[u8; 32], RangePosition>,
    deduplication: HashMap<WriterKey, DeduplicationState>,
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
        persist_state(&directory, &persisted)?;
        let active_file = open_segment(&directory, persisted.active_segment)?;
        Ok(Self {
            inner: Arc::new(FileActiveRangeStoreInner {
                directory,
                options,
                loaded: Mutex::new(LoadedRange {
                    persisted,
                    active_file,
                    entries,
                    cursor_positions,
                    digest_positions,
                    deduplication: recovered_deduplication,
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
        self.blocking(move |inner| append(inner, request, false))
            .await
    }

    async fn import_split(
        &self,
        request: ActiveRangeAppend,
    ) -> Result<ActiveRangeAppendResult, ActiveRangeStoreError> {
        self.blocking(move |inner| append(inner, request, true))
            .await
    }

    async fn commit(
        &self,
        generation: RangeGeneration,
        ownership_epoch: OwnershipEpoch,
        position: CommitPosition,
    ) -> Result<(), ActiveRangeStoreError> {
        self.blocking(move |inner| commit(inner, generation, ownership_epoch, position))
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
        self.blocking(move |inner| truncate_uncommitted(inner, generation, ownership_epoch))
            .await
    }

    async fn update_ownership_epoch(
        &self,
        generation: RangeGeneration,
        current_epoch: OwnershipEpoch,
        new_epoch: OwnershipEpoch,
    ) -> Result<(), ActiveRangeStoreError> {
        self.blocking(move |inner| {
            let mut loaded = inner
                .loaded
                .lock()
                .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
            validate_generation_and_epoch(&loaded.persisted, generation, current_epoch)?;
            if new_epoch <= current_epoch {
                return Err(ActiveRangeStoreError::OwnershipEpochDidNotAdvance);
            }
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
    let frame_digest = *blake3::hash(&request.frame).as_bytes();
    let writer_key = WriterKey {
        writer_session_id: request.identity.writer_session_id,
        writer_epoch: request.identity.writer_epoch,
    };
    let mut loaded = inner
        .loaded
        .lock()
        .map_err(|_| ActiveRangeStoreError::Worker("range lock poisoned".to_owned()))?;
    validate_generation_and_epoch(
        &loaded.persisted,
        request.generation,
        request.ownership_epoch,
    )?;
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
    if !import {
        if let Some(previous) = loaded.deduplication.get(&writer_key) {
            if request.identity.sequence == previous.sequence {
                if frame_digest != previous.frame_digest {
                    let existing = loaded
                        .entries
                        .iter()
                        .find(|entry| entry.position == previous.position)
                        .ok_or(ActiveRangeStoreError::WriterSequenceConflict)?;
                    let existing_frame = read_indexed_frame(&inner.directory, existing)?;
                    let existing_record = decode_record(&existing_frame.frame)?;
                    if !same_logical_record(&existing_record, &record) {
                        return Err(ActiveRangeStoreError::WriterSequenceConflict);
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
    if loaded.cursor_positions.contains_key(&request.cursor) {
        return Err(ActiveRangeStoreError::InvalidState(
            "Cursor already belongs to another position".to_owned(),
        ));
    }
    let position = next_position;
    let decoded = DecodedEntry {
        position,
        identity: request.identity,
        cursor: request.cursor,
        frame: request.frame,
        frame_digest,
        message_id: record.message_id,
    };
    let encoded = encode_entry(&decoded)?;
    if let Some(limit) = inner.options.max_range_bytes {
        let required = range_segment_bytes(&inner.directory, loaded.persisted.active_segment)?
            .saturating_add(encoded.len() as u64);
        if required > limit {
            return Err(ActiveRangeStoreError::DiskCapacityExceeded { required, limit });
        }
    }
    rotate_if_needed(inner, &mut loaded, encoded.len() as u64)?;
    let entry_start = loaded.active_file.seek(SeekFrom::End(0))?;
    loaded.active_file.write_all(&encoded)?;
    loaded.active_file.sync_data()?;
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
    let persistence = persist_state(&inner.directory, &next);
    loaded.persisted = next;
    persistence?;
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
        let size = usize::try_from(entry.frame_length)
            .map_err(|_| ActiveRangeStoreError::ReadBudgetExceeded)?;
        let next = bytes
            .checked_add(size)
            .ok_or(ActiveRangeStoreError::ReadBudgetExceeded)?;
        if next > max_bytes {
            if frames.is_empty() {
                return Err(ActiveRangeStoreError::ReadBudgetExceeded);
            }
            break;
        }
        frames.push(read_indexed_frame(&inner.directory, entry)?);
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
    loaded.active_file.set_len(active_length)?;
    loaded.active_file.sync_all()?;
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
    loaded.deduplication = deduplication_from_entries(&loaded.entries);
    let mut next = loaded.persisted.clone();
    next.appended = committed;
    next.flushed = committed;
    next.deduplication = persisted_deduplication(&loaded.deduplication);
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
    loaded.persisted = next;
    loaded.active_file = next_file;
    Ok(())
}

fn recover_segments(
    directory: &Path,
    persisted: &PersistedRangeState,
) -> Result<(Vec<EntryIndex>, HashMap<WriterKey, DeduplicationState>), ActiveRangeStoreError> {
    let mut entries = Vec::new();
    let mut expected_position = 1_u64;
    for segment in 0..=persisted.active_segment {
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

fn same_logical_record(left: &StoredRecord, right: &StoredRecord) -> bool {
    left.message_id == right.message_id
        && left.producer_id == right.producer_id
        && left.producer_sequence == right.producer_sequence
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
            ingest_time_ns: sequence as i64,
            key: b"key".to_vec(),
            payload: Vec::new(),
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
}
