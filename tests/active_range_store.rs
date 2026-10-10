use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use finnstream::{
    active_range::{
        ActiveRangeAppend, ActiveRangeDescriptor, ActiveRangeStore, ActiveRangeStoreError,
        AppendIdentity, CommitPosition, FileActiveRangeStore, FileActiveRangeStoreOptions,
        OwnershipEpoch, RangeGeneration, RangeId, RangePosition,
    },
    codec::encode_record,
    domain::StoredRecord,
};
use tempfile::TempDir;
use uuid::Uuid;

fn descriptor() -> ActiveRangeDescriptor {
    ActiveRangeDescriptor {
        feed_id: Uuid::from_u128(1),
        range_id: RangeId::from_uuid(Uuid::from_u128(2)),
        generation: RangeGeneration::new(1),
        ownership_epoch: OwnershipEpoch::new(1),
    }
}

fn append(sequence: u64, payload: &[u8]) -> ActiveRangeAppend {
    let writer_session_id = Uuid::from_u128(3);
    let record = StoredRecord {
        message_id: Uuid::from_u128(100 + sequence as u128),
        producer_id: writer_session_id,
        producer_sequence: sequence,
        event_time_ns: sequence as i64,
        ingest_time_ns: sequence as i64 + 1,
        key: b"account-1".to_vec(),
        payload: payload.to_vec(),
        metadata: BTreeMap::new(),
    };
    ActiveRangeAppend {
        generation: RangeGeneration::new(1),
        ownership_epoch: OwnershipEpoch::new(1),
        expected_position: None,
        identity: AppendIdentity {
            writer_session_id,
            writer_epoch: 1,
            sequence,
        },
        cursor: format!("cursor-{sequence}"),
        frame: encode_record(&record).unwrap(),
    }
}

fn range_directory(root: &Path) -> PathBuf {
    root.join(Uuid::from_u128(1).to_string())
        .join(Uuid::from_u128(2).to_string())
        .join("generation-1")
}

#[tokio::test]
async fn torn_active_segment_tail_is_removed_during_recovery() {
    let directory = TempDir::new().unwrap();
    let store = FileActiveRangeStore::open(directory.path(), descriptor()).unwrap();
    store.append(append(1, b"committed")).await.unwrap();
    store
        .commit(
            RangeGeneration::new(1),
            OwnershipEpoch::new(1),
            CommitPosition::new(1),
        )
        .await
        .unwrap();
    drop(store);

    let segment = range_directory(directory.path()).join("segment-000000.log");
    let valid_length = fs::metadata(&segment).unwrap().len();
    let mut file = OpenOptions::new().append(true).open(&segment).unwrap();
    file.write_all(&[0, 0, 0, 64, b'W', b'A', b'R']).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let reopened = FileActiveRangeStore::open(directory.path(), descriptor()).unwrap();
    assert_eq!(fs::metadata(segment).unwrap().len(), valid_length);
    assert_eq!(
        reopened
            .snapshot()
            .await
            .unwrap()
            .progress
            .appended()
            .value(),
        1
    );
    assert_eq!(reopened.read_committed(None, 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn uncommitted_tail_is_truncated_without_losing_committed_deduplication() {
    let directory = TempDir::new().unwrap();
    let store = FileActiveRangeStore::open(directory.path(), descriptor()).unwrap();
    let first = store.append(append(1, b"committed")).await.unwrap();
    store
        .commit(
            RangeGeneration::new(1),
            OwnershipEpoch::new(1),
            CommitPosition::new(1),
        )
        .await
        .unwrap();
    store.append(append(2, b"uncommitted")).await.unwrap();

    let removed = store
        .truncate_uncommitted(RangeGeneration::new(1), OwnershipEpoch::new(1))
        .await
        .unwrap();
    assert_eq!(removed, 1);
    let snapshot = store.snapshot().await.unwrap();
    assert_eq!(snapshot.progress.appended(), RangePosition::new(1));
    assert_eq!(snapshot.progress.flushed(), RangePosition::new(1));
    assert_eq!(snapshot.progress.commit_position(), CommitPosition::new(1));

    let duplicate = store.append(append(1, b"committed")).await.unwrap();
    assert!(duplicate.deduplicated);
    assert_eq!(duplicate.message_id, first.message_id);
    let replacement = store.append(append(2, b"replacement")).await.unwrap();
    assert_eq!(replacement.position, RangePosition::new(2));

    drop(store);
    let reopened = FileActiveRangeStore::open(directory.path(), descriptor()).unwrap();
    assert_eq!(reopened.read_committed(None, 10).await.unwrap().len(), 1);
    assert_eq!(
        reopened
            .snapshot()
            .await
            .unwrap()
            .progress
            .appended()
            .value(),
        2
    );
}

#[tokio::test]
async fn cursor_lookup_is_committed_indexed_and_survives_truncation_and_restart() {
    let directory = TempDir::new().unwrap();
    let store = FileActiveRangeStore::open(directory.path(), descriptor()).unwrap();
    for sequence in 1..=3 {
        store.append(append(sequence, b"value")).await.unwrap();
        if sequence <= 2 {
            store
                .commit(
                    RangeGeneration::new(1),
                    OwnershipEpoch::new(1),
                    CommitPosition::new(sequence),
                )
                .await
                .unwrap();
        }
    }
    assert_eq!(
        store
            .committed_position_for_cursor("cursor-2")
            .await
            .unwrap(),
        Some(RangePosition::new(2))
    );
    assert_eq!(
        store
            .committed_position_for_cursor("cursor-3")
            .await
            .unwrap(),
        None
    );
    store
        .truncate_uncommitted(RangeGeneration::new(1), OwnershipEpoch::new(1))
        .await
        .unwrap();
    assert_eq!(
        store
            .committed_position_for_cursor("cursor-3")
            .await
            .unwrap(),
        None
    );
    store.append(append(3, b"value")).await.unwrap();
    store
        .commit(
            RangeGeneration::new(1),
            OwnershipEpoch::new(1),
            CommitPosition::new(3),
        )
        .await
        .unwrap();
    drop(store);
    let reopened = FileActiveRangeStore::open(directory.path(), descriptor()).unwrap();
    assert_eq!(
        reopened
            .committed_position_for_cursor("cursor-2")
            .await
            .unwrap(),
        Some(RangePosition::new(2))
    );
    let tail = reopened
        .read_committed(Some(RangePosition::new(2)), 1)
        .await
        .unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].cursor, "cursor-3");
}

#[tokio::test]
async fn committed_pages_observe_a_byte_budget_without_skipping_frames() {
    let directory = TempDir::new().unwrap();
    let store = FileActiveRangeStore::open(directory.path(), descriptor()).unwrap();
    store.append(append(1, b"first")).await.unwrap();
    store.append(append(2, b"second")).await.unwrap();
    store
        .commit(
            RangeGeneration::new(1),
            OwnershipEpoch::new(1),
            CommitPosition::new(2),
        )
        .await
        .unwrap();
    let first = store.read_committed(None, 1).await.unwrap();
    let budget = first[0].frame.len();
    let page = store
        .read_committed_bounded(None, 32, budget)
        .await
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].cursor, "cursor-1");
    let next = store
        .read_committed_bounded(Some(page[0].position), 32, budget + 1)
        .await
        .unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].cursor, "cursor-2");
    assert!(matches!(
        store.read_committed_bounded(None, 32, budget - 1).await,
        Err(ActiveRangeStoreError::ReadBudgetExceeded)
    ));
}

#[tokio::test]
async fn one_cursor_cannot_identify_two_positions() {
    let directory = TempDir::new().unwrap();
    let store = FileActiveRangeStore::open(directory.path(), descriptor()).unwrap();
    store.append(append(1, b"first")).await.unwrap();
    let mut collision = append(2, b"second");
    collision.cursor = "cursor-1".to_owned();
    assert!(matches!(
        store.append(collision).await,
        Err(ActiveRangeStoreError::InvalidState(_))
    ));
    assert_eq!(
        store.snapshot().await.unwrap().progress.appended(),
        RangePosition::new(1)
    );
}

#[tokio::test]
async fn range_state_and_writer_deduplication_survive_restart() {
    let directory = TempDir::new().unwrap();
    let store = FileActiveRangeStore::open(directory.path(), descriptor()).unwrap();
    let original = store.append(append(7, b"payload")).await.unwrap();
    store
        .commit(
            RangeGeneration::new(1),
            OwnershipEpoch::new(1),
            CommitPosition::new(1),
        )
        .await
        .unwrap();
    drop(store);

    let reopened = FileActiveRangeStore::open(directory.path(), descriptor()).unwrap();
    let duplicate = reopened.append(append(7, b"payload")).await.unwrap();
    assert!(duplicate.deduplicated);
    assert_eq!(duplicate.position, original.position);
    assert_eq!(duplicate.message_id, original.message_id);
    assert!(duplicate.committed);
    assert_eq!(
        reopened.snapshot().await.unwrap().ownership_epoch,
        OwnershipEpoch::new(1)
    );
}

#[tokio::test]
async fn full_entry_corruption_fails_closed() {
    let directory = TempDir::new().unwrap();
    let store = FileActiveRangeStore::open(directory.path(), descriptor()).unwrap();
    store.append(append(1, b"payload")).await.unwrap();
    drop(store);

    let segment = range_directory(directory.path()).join("segment-000000.log");
    let mut bytes = fs::read(&segment).unwrap();
    let index = bytes.len() - 5;
    bytes[index] ^= 0xff;
    fs::write(&segment, bytes).unwrap();

    assert!(matches!(
        FileActiveRangeStore::open(directory.path(), descriptor()),
        Err(ActiveRangeStoreError::CorruptSegment { .. })
    ));
}

#[tokio::test]
async fn committed_segments_rotate_and_remain_readable() {
    let directory = TempDir::new().unwrap();
    let store = FileActiveRangeStore::open_with_options(
        directory.path(),
        descriptor(),
        FileActiveRangeStoreOptions {
            max_segment_bytes: 1,
            max_range_bytes: None,
        },
    )
    .unwrap();
    store.append(append(1, b"first")).await.unwrap();
    store
        .commit(
            RangeGeneration::new(1),
            OwnershipEpoch::new(1),
            CommitPosition::new(1),
        )
        .await
        .unwrap();
    store.append(append(2, b"second")).await.unwrap();

    let snapshot = store.snapshot().await.unwrap();
    assert_eq!(snapshot.segment_count, 2);
    assert_eq!(snapshot.progress.appended(), RangePosition::new(2));
    assert_eq!(
        fs::read_dir(range_directory(directory.path()))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("segment-"))
            .count(),
        2
    );

    store
        .commit(
            RangeGeneration::new(1),
            OwnershipEpoch::new(1),
            CommitPosition::new(2),
        )
        .await
        .unwrap();
    assert_eq!(store.read_committed(None, 10).await.unwrap().len(), 2);
}

#[tokio::test]
async fn stale_generation_and_epoch_cannot_mutate_storage() {
    let directory = TempDir::new().unwrap();
    let store = FileActiveRangeStore::open(directory.path(), descriptor()).unwrap();
    let mut wrong_generation = append(1, b"payload");
    wrong_generation.generation = RangeGeneration::new(0);
    assert!(matches!(
        store.append(wrong_generation).await,
        Err(ActiveRangeStoreError::WrongGeneration { .. })
    ));

    let mut stale_epoch = append(1, b"payload");
    stale_epoch.ownership_epoch = OwnershipEpoch::new(0);
    assert!(matches!(
        store.append(stale_epoch).await,
        Err(ActiveRangeStoreError::StaleOwnershipEpoch { .. })
    ));
    assert_eq!(
        store.snapshot().await.unwrap().progress.appended().value(),
        0
    );
}

#[tokio::test]
async fn writer_sequence_conflicts_gaps_and_stale_retries_are_rejected() {
    let directory = TempDir::new().unwrap();
    let store = FileActiveRangeStore::open(directory.path(), descriptor()).unwrap();
    store.append(append(4, b"accepted")).await.unwrap();

    assert!(matches!(
        store.append(append(4, b"different")).await,
        Err(ActiveRangeStoreError::WriterSequenceConflict)
    ));
    assert!(matches!(
        store.append(append(6, b"gap")).await,
        Err(ActiveRangeStoreError::WriterSequenceGap {
            actual: 6,
            expected: 5
        })
    ));
    assert!(matches!(
        store.append(append(3, b"stale")).await,
        Err(ActiveRangeStoreError::StaleWriterSequence {
            actual: 3,
            latest: 4
        })
    ));
    assert_eq!(
        store.snapshot().await.unwrap().progress.appended().value(),
        1
    );
}

#[tokio::test]
async fn disk_capacity_exhaustion_rejects_append_without_advancing_progress() {
    let directory = TempDir::new().unwrap();
    let store = FileActiveRangeStore::open_with_options(
        directory.path(),
        descriptor(),
        FileActiveRangeStoreOptions {
            max_segment_bytes: 1024 * 1024,
            max_range_bytes: Some(1),
        },
    )
    .unwrap();
    assert!(matches!(
        store.append(append(1, b"payload")).await,
        Err(ActiveRangeStoreError::DiskCapacityExceeded { .. })
    ));
    assert_eq!(
        store.snapshot().await.unwrap().progress.appended().value(),
        0
    );
}

#[tokio::test]
async fn ownership_epoch_update_is_durable_and_fences_the_previous_epoch() {
    let directory = TempDir::new().unwrap();
    let store = FileActiveRangeStore::open(directory.path(), descriptor()).unwrap();
    store
        .update_ownership_epoch(
            RangeGeneration::new(1),
            OwnershipEpoch::new(1),
            OwnershipEpoch::new(2),
        )
        .await
        .unwrap();

    assert!(matches!(
        store.append(append(1, b"stale")).await,
        Err(ActiveRangeStoreError::StaleOwnershipEpoch { .. })
    ));
    let mut current = append(1, b"current");
    current.ownership_epoch = OwnershipEpoch::new(2);
    store.append(current).await.unwrap();
    drop(store);

    let mut current_descriptor = descriptor();
    current_descriptor.ownership_epoch = OwnershipEpoch::new(2);
    let reopened = FileActiveRangeStore::open(directory.path(), current_descriptor).unwrap();
    assert_eq!(
        reopened.snapshot().await.unwrap().ownership_epoch,
        OwnershipEpoch::new(2)
    );
}

#[tokio::test]
async fn commit_position_is_monotonic_and_cannot_cross_the_flushed_boundary() {
    let directory = TempDir::new().unwrap();
    let store = FileActiveRangeStore::open(directory.path(), descriptor()).unwrap();
    store.append(append(1, b"payload")).await.unwrap();
    assert!(matches!(
        store
            .commit(
                RangeGeneration::new(1),
                OwnershipEpoch::new(1),
                CommitPosition::new(2),
            )
            .await,
        Err(ActiveRangeStoreError::CommitBeyondFlushed { .. })
    ));
    store
        .commit(
            RangeGeneration::new(1),
            OwnershipEpoch::new(1),
            CommitPosition::new(1),
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .commit(
                RangeGeneration::new(1),
                OwnershipEpoch::new(1),
                CommitPosition::new(0),
            )
            .await,
        Err(ActiveRangeStoreError::CommitMovedBackwards { .. })
    ));
}

fn staged(sequence: u64, payload: &[u8], cursor: &str, position: u64) -> ActiveRangeAppend {
    let writer_session_id = Uuid::from_u128(3);
    let record = StoredRecord {
        message_id: Uuid::from_u128(200 + sequence as u128 + position as u128),
        producer_id: writer_session_id,
        producer_sequence: sequence,
        event_time_ns: sequence as i64,
        ingest_time_ns: sequence as i64 + 1,
        key: format!("key-{cursor}").into_bytes(),
        payload: payload.to_vec(),
        metadata: BTreeMap::new(),
    };
    ActiveRangeAppend {
        generation: RangeGeneration::new(1),
        ownership_epoch: OwnershipEpoch::new(1),
        expected_position: Some(RangePosition::new(position)),
        identity: AppendIdentity {
            writer_session_id,
            writer_epoch: 1,
            sequence,
        },
        cursor: cursor.to_owned(),
        frame: encode_record(&record).unwrap(),
    }
}

#[tokio::test]
async fn imported_merge_branches_deduplicate_colliding_sequences_by_frame_digest() {
    let directory = TempDir::new().unwrap();
    let store = FileActiveRangeStore::open(directory.path(), descriptor()).unwrap();
    // Two merge branches carry the same writer sequences with different records.
    let left_one = staged(1, b"left-1", "left-1", 1);
    let right_one = staged(1, b"right-1", "right-1", 2);
    let left_two = staged(2, b"left-2", "left-2", 3);
    let right_two = staged(2, b"right-2", "right-2", 4);
    for frame in [
        left_one.clone(),
        right_one.clone(),
        left_two.clone(),
        right_two.clone(),
    ] {
        store.import_split(frame).await.unwrap();
    }
    store
        .commit(
            RangeGeneration::new(1),
            OwnershipEpoch::new(1),
            CommitPosition::new(4),
        )
        .await
        .unwrap();
    // Re-staging an identical frame is idempotent.
    let replay = store.import_split(right_two.clone()).await.unwrap();
    assert!(replay.deduplicated);
    assert_eq!(replay.position, RangePosition::new(4));
    assert_eq!(replay.cursor, "right-2");
    // A staged retry without a position resolves by frame digest.
    let mut position_free = right_one.clone();
    position_free.expected_position = None;
    let replay = store.import_split(position_free).await.unwrap();
    assert!(replay.deduplicated);
    assert_eq!(replay.position, RangePosition::new(2));
    // Live appends continue at the merged writer maximum.
    let third = store.append(append(3, b"merged-3")).await.unwrap();
    assert_eq!(third.position, RangePosition::new(5));
    // Retries of collided branch frames return their original results.
    let mut live_retry = right_one.clone();
    live_retry.expected_position = None;
    let replay = store.append(live_retry).await.unwrap();
    assert!(replay.deduplicated);
    assert_eq!(replay.position, RangePosition::new(2));
    assert_eq!(replay.cursor, "right-1");
    let mut live_retry_left = left_one.clone();
    live_retry_left.expected_position = None;
    let replay = store.append(live_retry_left).await.unwrap();
    assert!(replay.deduplicated);
    assert_eq!(replay.position, RangePosition::new(1));
    // Distinct payloads reusing a committed sequence still fail closed.
    assert!(matches!(
        store.append(append(3, b"different")).await,
        Err(ActiveRangeStoreError::WriterSequenceConflict)
    ));
    assert!(matches!(
        store.append(append(2, b"different")).await,
        Err(ActiveRangeStoreError::StaleWriterSequence { .. })
    ));
    drop(store);
    let reopened = FileActiveRangeStore::open(directory.path(), descriptor()).unwrap();
    let mut retry = right_two.clone();
    retry.expected_position = None;
    let replay = reopened.append(retry).await.unwrap();
    assert!(replay.deduplicated);
    assert_eq!(replay.cursor, "right-2");
    assert!(matches!(
        reopened.append(append(3, b"another")).await,
        Err(ActiveRangeStoreError::WriterSequenceConflict)
    ));
}
