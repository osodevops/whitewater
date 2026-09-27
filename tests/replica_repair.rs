use std::{collections::BTreeMap, fs, sync::Arc};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use finnstream::{
    active_range::{
        repair_replica, AppendIdentity, CommitPosition, OwnershipEpoch, RangeGeneration,
        RangePosition, ReplicaAppendRequest, ReplicaAppendService, ReplicaCommitRequest,
        StorageNodeId,
    },
    codec::encode_record,
    control::{Command, ControlController},
    domain::StoredRecord,
    storage::{FileLogStore, LogStore},
};
use tempfile::TempDir;
use uuid::Uuid;

fn node(value: &str) -> StorageNodeId {
    StorageNodeId::try_new(value).unwrap()
}

#[tokio::test]
async fn missing_committed_frames_are_verified_transferred_and_dedup_rebuilt() {
    let directory = TempDir::new().unwrap();
    let legacy: Arc<dyn LogStore> =
        Arc::new(FileLogStore::open(directory.path().join("legacy")).unwrap());
    let control = Arc::new(
        ControlController::open_with_storage_nodes(
            directory.path().join("catalog.json"),
            legacy,
            vec![node("storage-1"), node("storage-2"), node("storage-3")],
        )
        .unwrap(),
    );
    let created = control
        .execute_commands(vec![
            Command::CreateSpace {
                name: "orders".to_owned(),
            },
            Command::CreateFeed {
                name: "orders.events".to_owned(),
            },
        ])
        .await
        .unwrap();
    let feed_id = serde_json::from_value(created.results[1].data["feed_id"].clone()).unwrap();
    let assignment = control.active_range_assignment(feed_id).await.unwrap();
    let source = Arc::new(ReplicaAppendService::new(
        directory.path().join("source"),
        node("storage-1"),
        control.clone(),
    ));
    let target = Arc::new(ReplicaAppendService::new(
        directory.path().join("target"),
        node("storage-2"),
        control,
    ));
    let writer = Uuid::from_u128(500);
    for sequence in 1..=3_u64 {
        let record = StoredRecord {
            message_id: Uuid::from_u128(600 + sequence as u128),
            producer_id: writer,
            producer_sequence: sequence,
            event_time_ns: sequence as i64,
            ingest_time_ns: sequence as i64,
            key: b"key".to_vec(),
            payload: format!("value-{sequence}").into_bytes(),
            metadata: BTreeMap::new(),
        };
        let request = ReplicaAppendRequest {
            feed_id,
            range_id: assignment.range_id,
            generation: RangeGeneration::new(1),
            ownership_epoch: OwnershipEpoch::new(1),
            append_owner: assignment.owner.clone(),
            expected_position: RangePosition::new(sequence),
            identity: AppendIdentity {
                writer_session_id: writer,
                writer_epoch: 1,
                sequence,
            },
            cursor: format!("cursor-{sequence}"),
            frame_base64: STANDARD.encode(encode_record(&record).unwrap()),
        };
        let accepted = source.append(request.clone()).await.unwrap();
        source
            .commit(ReplicaCommitRequest {
                feed_id,
                range_id: assignment.range_id,
                generation: assignment.generation,
                ownership_epoch: assignment.ownership_epoch,
                append_owner: assignment.owner.clone(),
                commit_position: CommitPosition::new(sequence),
                frame_digest: accepted.frame_digest,
            })
            .await
            .unwrap();
        if sequence == 1 {
            let accepted = target.append(request).await.unwrap();
            target
                .commit(ReplicaCommitRequest {
                    feed_id,
                    range_id: assignment.range_id,
                    generation: assignment.generation,
                    ownership_epoch: assignment.ownership_epoch,
                    append_owner: assignment.owner.clone(),
                    commit_position: CommitPosition::new(1),
                    frame_digest: accepted.frame_digest,
                })
                .await
                .unwrap();
        }
    }
    let progress = repair_replica(&assignment, source, target.clone(), 1)
        .await
        .unwrap();
    assert!(progress.ready);
    assert_eq!(progress.transferred_records, 2);
    assert_eq!(progress.target_committed, CommitPosition::new(3));
    assert_eq!(
        target
            .read_committed(feed_id, None, 10)
            .await
            .unwrap()
            .len(),
        3
    );
}

#[tokio::test]
async fn corrupt_replica_directory_is_quarantined_before_rebuild() {
    let directory = TempDir::new().unwrap();
    let legacy: Arc<dyn LogStore> =
        Arc::new(FileLogStore::open(directory.path().join("legacy")).unwrap());
    let control = Arc::new(
        ControlController::open_with_storage_nodes(
            directory.path().join("catalog.json"),
            legacy,
            vec![node("storage-1"), node("storage-2"), node("storage-3")],
        )
        .unwrap(),
    );
    let created = control
        .execute_commands(vec![
            Command::CreateSpace {
                name: "orders".to_owned(),
            },
            Command::CreateFeed {
                name: "orders.events".to_owned(),
            },
        ])
        .await
        .unwrap();
    let feed_id = serde_json::from_value(created.results[1].data["feed_id"].clone()).unwrap();
    let assignment = control.active_range_assignment(feed_id).await.unwrap();
    let source = Arc::new(ReplicaAppendService::new(
        directory.path().join("source"),
        node("storage-1"),
        control.clone(),
    ));
    let target_root = directory.path().join("target");
    let target = Arc::new(ReplicaAppendService::new(
        &target_root,
        node("storage-2"),
        control.clone(),
    ));
    let writer = Uuid::from_u128(700);
    let record = StoredRecord {
        message_id: Uuid::from_u128(701),
        producer_id: writer,
        producer_sequence: 1,
        event_time_ns: 1,
        ingest_time_ns: 1,
        key: b"key".to_vec(),
        payload: b"value".to_vec(),
        metadata: BTreeMap::new(),
    };
    let request = ReplicaAppendRequest {
        feed_id,
        range_id: assignment.range_id,
        generation: assignment.generation,
        ownership_epoch: assignment.ownership_epoch,
        append_owner: assignment.owner.clone(),
        expected_position: RangePosition::new(1),
        identity: AppendIdentity {
            writer_session_id: writer,
            writer_epoch: 1,
            sequence: 1,
        },
        cursor: "cursor-1".to_owned(),
        frame_base64: STANDARD.encode(encode_record(&record).unwrap()),
    };
    for service in [&source, &target] {
        let accepted = service.append(request.clone()).await.unwrap();
        service
            .commit(ReplicaCommitRequest {
                feed_id,
                range_id: assignment.range_id,
                generation: assignment.generation,
                ownership_epoch: assignment.ownership_epoch,
                append_owner: assignment.owner.clone(),
                commit_position: CommitPosition::new(1),
                frame_digest: accepted.frame_digest,
            })
            .await
            .unwrap();
    }
    drop(target);
    let segment = target_root
        .join(feed_id.to_string())
        .join(assignment.range_id.to_string())
        .join("generation-1")
        .join("segment-000000.log");
    let mut bytes = fs::read(&segment).unwrap();
    let corrupt_index = bytes.len() - 5;
    bytes[corrupt_index] ^= 0xff;
    fs::write(&segment, bytes).unwrap();
    let target = Arc::new(ReplicaAppendService::new(
        &target_root,
        node("storage-2"),
        control,
    ));
    let progress = repair_replica(&assignment, source, target, 10)
        .await
        .unwrap();
    assert!(progress.ready);
    assert!(progress.quarantined_corruption);
    assert_eq!(progress.transferred_records, 1);
}
