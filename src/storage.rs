use std::{
    collections::{BTreeSet, HashMap},
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    codec::{decode_record, encode_record, CodecError, MAX_FRAME_BYTES},
    cursor::{decode_cursor, encode_cursor, stream_fingerprint, CursorError},
    domain::{
        AppendInput, AppendResult, CursorRecord, StorageStats, StoredRecord, StreamDescription,
    },
};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("stream name must be an absolute namespace path containing only letters, numbers, '.', '_', and '-'")]
    InvalidStreamName,
    #[error("stream does not exist: {0}")]
    StreamNotFound(String),
    #[error("record key must not be empty")]
    EmptyKey,
    #[error("producer sequence {actual} is stale; latest accepted sequence is {latest}")]
    StaleSequence { actual: u64, latest: u64 },
    #[error("producer sequence {actual} has a gap; expected {expected}")]
    SequenceGap { actual: u64, expected: u64 },
    #[error("producer sequence was reused with different record content")]
    SequenceConflict,
    #[error("cursor points beyond the retained stream")]
    CursorOutOfRange,
    #[error(transparent)]
    InvalidCursor(#[from] CursorError),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("storage worker failed: {0}")]
    Worker(String),
}

#[async_trait]
pub trait LogStore: Send + Sync {
    async fn create_stream(&self, stream: &str) -> Result<StreamDescription, StorageError>;
    async fn append(&self, stream: &str, input: AppendInput) -> Result<AppendResult, StorageError>;
    async fn read(
        &self,
        stream: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<CursorRecord>, StorageError>;
    async fn describe(&self, stream: &str) -> Result<StreamDescription, StorageError>;
    async fn list_streams(&self) -> Result<Vec<StreamDescription>, StorageError>;
    async fn stats(&self) -> Result<StorageStats, StorageError>;
}

#[derive(Clone)]
pub struct FileLogStore {
    root: Arc<PathBuf>,
    state: Arc<Mutex<HashMap<String, StreamState>>>,
}

struct StreamState {
    file: File,
    records: Vec<StoredRecord>,
    producers: HashMap<Uuid, ProducerState>,
}

struct ProducerState {
    sequence: u64,
    cursor: String,
    message_id: Uuid,
    digest: blake3::Hash,
}

impl FileLogStore {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let root = root.into();
        fs::create_dir_all(root.join("streams"))?;
        Ok(Self {
            root: Arc::new(root),
            state: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    async fn blocking<T, F>(&self, operation: F) -> Result<T, StorageError>
    where
        T: Send + 'static,
        F: FnOnce(Self) -> Result<T, StorageError> + Send + 'static,
    {
        let store = self.clone();
        tokio::task::spawn_blocking(move || operation(store))
            .await
            .map_err(|error| StorageError::Worker(error.to_string()))?
    }

    fn ensure_loaded<'a>(
        &self,
        states: &'a mut HashMap<String, StreamState>,
        stream: &str,
        create: bool,
    ) -> Result<&'a mut StreamState, StorageError> {
        if !states.contains_key(stream) {
            let directory = self.stream_directory(stream);
            if !directory.exists() && !create {
                return Err(StorageError::StreamNotFound(stream.to_owned()));
            }
            fs::create_dir_all(&directory)?;
            let name_path = directory.join("stream-name");
            if name_path.exists() {
                let stored_name = fs::read_to_string(&name_path)?;
                if stored_name != stream {
                    return Err(StorageError::InvalidStreamName);
                }
            } else {
                fs::write(&name_path, stream.as_bytes())?;
            }
            states.insert(
                stream.to_owned(),
                load_stream_state(&directory.join("active.log"), stream)?,
            );
        }
        Ok(states.get_mut(stream).expect("loaded stream state"))
    }

    fn stream_directory(&self, stream: &str) -> PathBuf {
        self.root
            .join("streams")
            .join(hex(&stream_fingerprint(stream)))
    }
}

#[async_trait]
impl LogStore for FileLogStore {
    async fn create_stream(&self, stream: &str) -> Result<StreamDescription, StorageError> {
        validate_stream_name(stream)?;
        let stream = stream.to_owned();
        self.blocking(move |store| {
            let mut states = store.state.lock().expect("storage lock poisoned");
            let state = store.ensure_loaded(&mut states, &stream, true)?;
            Ok(StreamDescription {
                name: stream,
                records: state.records.len() as u64,
            })
        })
        .await
    }

    async fn append(&self, stream: &str, input: AppendInput) -> Result<AppendResult, StorageError> {
        validate_stream_name(stream)?;
        if input.key.is_empty() {
            return Err(StorageError::EmptyKey);
        }
        let stream = stream.to_owned();
        self.blocking(move |store| {
            let digest = input_digest(&input);
            let mut states = store.state.lock().expect("storage lock poisoned");
            let state = store.ensure_loaded(&mut states, &stream, true)?;
            if let Some(previous) = state.producers.get(&input.producer_id) {
                if input.producer_sequence == previous.sequence {
                    if digest != previous.digest {
                        return Err(StorageError::SequenceConflict);
                    }
                    return Ok(AppendResult {
                        cursor: previous.cursor.clone(),
                        message_id: previous.message_id,
                        deduplicated: true,
                    });
                }
                if input.producer_sequence < previous.sequence {
                    return Err(StorageError::StaleSequence {
                        actual: input.producer_sequence,
                        latest: previous.sequence,
                    });
                }
                let expected = previous.sequence.saturating_add(1);
                if input.producer_sequence != expected {
                    return Err(StorageError::SequenceGap {
                        actual: input.producer_sequence,
                        expected,
                    });
                }
            }
            let producer_id = input.producer_id;
            let producer_sequence = input.producer_sequence;
            let message_id = input.message_id;
            let record_index = state.records.len() as u64;
            let cursor = encode_cursor(&stream, record_index);
            let record = StoredRecord::from(input);
            let frame = encode_record(&record)?;
            state.file.seek(SeekFrom::End(0))?;
            state.file.write_all(&frame)?;
            state.file.sync_data()?;
            state.records.push(record);
            state.producers.insert(
                producer_id,
                ProducerState {
                    sequence: producer_sequence,
                    cursor: cursor.clone(),
                    message_id,
                    digest,
                },
            );
            Ok(AppendResult {
                cursor,
                message_id,
                deduplicated: false,
            })
        })
        .await
    }

    async fn read(
        &self,
        stream: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<CursorRecord>, StorageError> {
        validate_stream_name(stream)?;
        let stream = stream.to_owned();
        let after = after.map(ToOwned::to_owned);
        let limit = limit.clamp(1, 10_000);
        self.blocking(move |store| {
            let mut states = store.state.lock().expect("storage lock poisoned");
            let state = store.ensure_loaded(&mut states, &stream, false)?;
            let start = match after {
                Some(cursor) => decode_cursor(&stream, &cursor)?
                    .checked_add(1)
                    .ok_or(StorageError::CursorOutOfRange)?
                    as usize,
                None => 0,
            };
            if start > state.records.len() {
                return Err(StorageError::CursorOutOfRange);
            }
            Ok(state
                .records
                .iter()
                .enumerate()
                .skip(start)
                .take(limit)
                .map(|(index, record)| CursorRecord {
                    cursor: encode_cursor(&stream, index as u64),
                    record: record.clone(),
                })
                .collect())
        })
        .await
    }

    async fn describe(&self, stream: &str) -> Result<StreamDescription, StorageError> {
        validate_stream_name(stream)?;
        let stream = stream.to_owned();
        self.blocking(move |store| {
            let mut states = store.state.lock().expect("storage lock poisoned");
            let state = store.ensure_loaded(&mut states, &stream, false)?;
            Ok(StreamDescription {
                name: stream,
                records: state.records.len() as u64,
            })
        })
        .await
    }

    async fn list_streams(&self) -> Result<Vec<StreamDescription>, StorageError> {
        self.blocking(move |store| {
            let names = stored_stream_names(&store.root)?;
            let mut states = store.state.lock().expect("storage lock poisoned");
            let mut descriptions = Vec::new();
            for name in names {
                let state = store.ensure_loaded(&mut states, &name, false)?;
                descriptions.push(StreamDescription {
                    name,
                    records: state.records.len() as u64,
                });
            }
            Ok(descriptions)
        })
        .await
    }

    async fn stats(&self) -> Result<StorageStats, StorageError> {
        self.blocking(move |store| {
            let names = stored_stream_names(&store.root)?;
            let mut states = store.state.lock().expect("storage lock poisoned");
            let mut record_count = 0_u64;
            let mut bytes_on_disk = 0_u64;
            for name in &names {
                let state = store.ensure_loaded(&mut states, name, false)?;
                record_count = record_count.saturating_add(state.records.len() as u64);
                bytes_on_disk = bytes_on_disk.saturating_add(state.file.metadata()?.len());
            }
            Ok(StorageStats {
                stream_count: names.len() as u64,
                record_count,
                bytes_on_disk,
                safe_to_remove: record_count == 0,
            })
        })
        .await
    }
}

fn stored_stream_names(root: &Path) -> Result<BTreeSet<String>, StorageError> {
    let mut names = BTreeSet::new();
    for entry in fs::read_dir(root.join("streams"))? {
        let name_path = entry?.path().join("stream-name");
        if name_path.is_file() {
            names.insert(fs::read_to_string(name_path)?);
        }
    }
    Ok(names)
}

fn load_stream_state(path: &Path, stream: &str) -> Result<StreamState, StorageError> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    let mut records = Vec::new();
    let mut producers = HashMap::new();
    let mut valid_length = 0_u64;
    loop {
        let frame_start = file.stream_position()?;
        let mut length_bytes = [0_u8; 4];
        match file.read_exact(&mut length_bytes) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                file.set_len(valid_length)?;
                break;
            }
            Err(error) => return Err(error.into()),
        }
        let body_length = u32::from_be_bytes(length_bytes) as usize;
        if body_length > MAX_FRAME_BYTES {
            return Err(CodecError::FrameTooLarge.into());
        }
        let mut remainder = vec![0_u8; body_length + 4];
        match file.read_exact(&mut remainder) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                file.set_len(frame_start)?;
                break;
            }
            Err(error) => return Err(error.into()),
        }
        let mut frame = Vec::with_capacity(remainder.len() + 4);
        frame.extend_from_slice(&length_bytes);
        frame.extend_from_slice(&remainder);
        let record = decode_record(&frame)?;
        let cursor = encode_cursor(stream, records.len() as u64);
        producers.insert(
            record.producer_id,
            ProducerState {
                sequence: record.producer_sequence,
                cursor,
                message_id: record.message_id,
                digest: record_digest(&record),
            },
        );
        records.push(record);
        valid_length = file.stream_position()?;
    }
    file.seek(SeekFrom::End(0))?;
    Ok(StreamState {
        file,
        records,
        producers,
    })
}

fn validate_stream_name(stream: &str) -> Result<(), StorageError> {
    if stream.len() < 2 || stream.len() > 256 || !stream.starts_with('/') || stream.ends_with('/') {
        return Err(StorageError::InvalidStreamName);
    }
    if stream.split('/').skip(1).any(|segment| {
        segment.is_empty()
            || !segment.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
            })
    }) {
        return Err(StorageError::InvalidStreamName);
    }
    Ok(())
}

fn input_digest(input: &AppendInput) -> blake3::Hash {
    content_digest(&input.key, &input.payload, &input.metadata)
}

fn record_digest(record: &StoredRecord) -> blake3::Hash {
    content_digest(&record.key, &record.payload, &record.metadata)
}

fn content_digest(
    key: &[u8],
    payload: &[u8],
    metadata: &std::collections::BTreeMap<String, Vec<u8>>,
) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(key.len() as u64).to_be_bytes());
    hasher.update(key);
    hasher.update(&(payload.len() as u64).to_be_bytes());
    hasher.update(payload);
    for (name, value) in metadata {
        hasher.update(&(name.len() as u64).to_be_bytes());
        hasher.update(name.as_bytes());
        hasher.update(&(value.len() as u64).to_be_bytes());
        hasher.update(value);
    }
    hasher.finalize()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tempfile::TempDir;

    use super::*;

    fn input(producer_id: Uuid, sequence: u64, payload: &[u8]) -> AppendInput {
        AppendInput {
            message_id: Uuid::new_v4(),
            producer_id,
            producer_sequence: sequence,
            event_time_ns: 1,
            ingest_time_ns: 2,
            key: b"customer-1".to_vec(),
            payload: payload.to_vec(),
            metadata: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn append_is_durable_ordered_and_cursor_based() {
        let directory = TempDir::new().unwrap();
        let producer = Uuid::new_v4();
        let store = FileLogStore::open(directory.path()).unwrap();
        let first = store
            .append("/orders/eu", input(producer, 1, b"one"))
            .await
            .unwrap();
        let second = store
            .append("/orders/eu", input(producer, 2, b"two"))
            .await
            .unwrap();
        drop(store);

        let reopened = FileLogStore::open(directory.path()).unwrap();
        let after_first = reopened
            .read("/orders/eu", Some(&first.cursor), 10)
            .await
            .unwrap();
        assert_eq!(after_first.len(), 1);
        assert_eq!(after_first[0].cursor, second.cursor);
        assert_eq!(after_first[0].record.payload, b"two");
    }

    #[tokio::test]
    async fn readers_advance_independently_with_their_own_cursors() {
        let directory = TempDir::new().unwrap();
        let producer = Uuid::new_v4();
        let store = FileLogStore::open(directory.path()).unwrap();
        for (sequence, payload) in [
            (1, b"one".as_slice()),
            (2, b"two".as_slice()),
            (3, b"three".as_slice()),
        ] {
            store
                .append("/orders/eu", input(producer, sequence, payload))
                .await
                .unwrap();
        }

        let reader_a = store.read("/orders/eu", None, 1).await.unwrap();
        let reader_b = store.read("/orders/eu", None, 2).await.unwrap();
        assert_eq!(reader_a[0].record.payload, b"one");
        assert_eq!(
            reader_b
                .iter()
                .map(|item| item.record.payload.as_slice())
                .collect::<Vec<_>>(),
            vec![b"one".as_slice(), b"two".as_slice()]
        );

        let reader_a_next = store
            .read("/orders/eu", Some(&reader_a[0].cursor), 10)
            .await
            .unwrap();
        let reader_b_next = store
            .read("/orders/eu", Some(&reader_b[1].cursor), 10)
            .await
            .unwrap();
        assert_eq!(
            reader_a_next
                .iter()
                .map(|item| item.record.payload.as_slice())
                .collect::<Vec<_>>(),
            vec![b"two".as_slice(), b"three".as_slice()]
        );
        assert_eq!(reader_b_next.len(), 1);
        assert_eq!(reader_b_next[0].record.payload, b"three");
    }

    #[tokio::test]
    async fn duplicate_sequence_returns_original_cursor_without_second_record() {
        let directory = TempDir::new().unwrap();
        let producer = Uuid::new_v4();
        let store = FileLogStore::open(directory.path()).unwrap();
        let first = store
            .append("/orders/eu", input(producer, 7, b"one"))
            .await
            .unwrap();
        let duplicate = store
            .append("/orders/eu", input(producer, 7, b"one"))
            .await
            .unwrap();
        assert!(duplicate.deduplicated);
        assert_eq!(duplicate.cursor, first.cursor);
        assert_eq!(duplicate.message_id, first.message_id);
        assert_eq!(store.describe("/orders/eu").await.unwrap().records, 1);
        assert!(matches!(
            store
                .append("/orders/eu", input(producer, 7, b"different"))
                .await,
            Err(StorageError::SequenceConflict)
        ));
    }

    #[tokio::test]
    async fn storage_stats_only_allow_empty_nodes_to_be_removed() {
        let directory = TempDir::new().unwrap();
        let store = FileLogStore::open(directory.path()).unwrap();
        assert!(store.stats().await.unwrap().safe_to_remove);
        store.create_stream("/orders/eu").await.unwrap();
        assert!(store.stats().await.unwrap().safe_to_remove);
        store
            .append("/orders/eu", input(Uuid::new_v4(), 1, b"one"))
            .await
            .unwrap();
        let stats = store.stats().await.unwrap();
        assert_eq!(stats.stream_count, 1);
        assert_eq!(stats.record_count, 1);
        assert!(stats.bytes_on_disk > 0);
        assert!(!stats.safe_to_remove);
    }

    #[tokio::test]
    async fn producer_sequence_gaps_are_rejected() {
        let directory = TempDir::new().unwrap();
        let producer = Uuid::new_v4();
        let store = FileLogStore::open(directory.path()).unwrap();
        store
            .append("/orders/eu", input(producer, 4, b"one"))
            .await
            .unwrap();
        assert!(matches!(
            store.append("/orders/eu", input(producer, 6, b"two")).await,
            Err(StorageError::SequenceGap {
                expected: 5,
                actual: 6
            })
        ));
    }
}
