use std::{collections::BTreeMap, sync::Arc};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use finnstream::{
    active_range::{
        stage_right_range, AppendIdentity, CommitPosition, KeyToken, RangePosition,
        ReplicaAppendRequest, ReplicaAppendService, ReplicaCommitRequest, StorageNodeId,
    },
    codec::encode_record,
    control::{Command, ControlController, RangeSplitPlan},
    domain::StoredRecord,
    storage::{FileLogStore, LogStore},
};
use tempfile::TempDir;
use uuid::Uuid;

fn node(value: &str) -> StorageNodeId {
    StorageNodeId::try_new(value).unwrap()
}

#[tokio::test]
async fn committed_right_hand_records_stage_identically_on_all_replicas() {
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
    let source_assignment = control.active_range_assignment(feed_id).await.unwrap();
    let services = ["storage-1", "storage-2", "storage-3"]
        .into_iter()
        .map(|value| {
            let id = node(value);
            let service = Arc::new(ReplicaAppendService::new(
                directory.path().join(value),
                id.clone(),
                control.clone(),
            ));
            (id, service)
        })
        .collect::<BTreeMap<_, _>>();
    let source = services[&source_assignment.owner].clone();
    let split_at = KeyToken::from_bytes([0x80; 16]);
    let mut left_key = Vec::new();
    let mut right_key = Vec::new();
    for value in 0..10_000_u64 {
        let key = format!("key-{value}").into_bytes();
        if KeyToken::from_key(&key) < split_at && left_key.is_empty() {
            left_key = key;
        } else if KeyToken::from_key(&key) >= split_at && right_key.is_empty() {
            right_key = key;
        }
        if !left_key.is_empty() && !right_key.is_empty() {
            break;
        }
    }
    let writer = Uuid::from_u128(900);
    for (index, key) in [left_key.clone(), right_key.clone(), left_key, right_key]
        .into_iter()
        .enumerate()
    {
        let sequence = index as u64 + 1;
        let record = StoredRecord {
            message_id: Uuid::from_u128(1_000 + sequence as u128),
            producer_id: writer,
            producer_sequence: sequence,
            event_time_ns: sequence as i64,
            ingest_time_ns: sequence as i64,
            key,
            payload: format!("value-{sequence}").into_bytes(),
            metadata: BTreeMap::new(),
        };
        let accepted = source
            .append(ReplicaAppendRequest {
                feed_id,
                range_id: source_assignment.range_id,
                generation: source_assignment.generation,
                ownership_epoch: source_assignment.ownership_epoch,
                append_owner: source_assignment.owner.clone(),
                expected_position: RangePosition::new(sequence),
                identity: AppendIdentity {
                    writer_session_id: writer,
                    writer_epoch: 1,
                    sequence,
                },
                cursor: format!("cursor-{sequence}"),
                frame_base64: STANDARD.encode(encode_record(&record).unwrap()),
            })
            .await
            .unwrap();
        source
            .commit(ReplicaCommitRequest {
                feed_id,
                range_id: source_assignment.range_id,
                generation: source_assignment.generation,
                ownership_epoch: source_assignment.ownership_epoch,
                append_owner: source_assignment.owner.clone(),
                commit_position: CommitPosition::new(sequence),
                frame_digest: accepted.frame_digest,
            })
            .await
            .unwrap();
    }
    let prepared = control
        .execute_commands(vec![Command::PrepareActiveRangeSplit {
            feed: "orders.events".to_owned(),
            split_at,
        }])
        .await
        .unwrap();
    let plan: RangeSplitPlan = serde_json::from_value(prepared.results[0].data.clone()).unwrap();
    let staged = stage_right_range(&plan, source, &services, 1)
        .await
        .unwrap();
    assert_eq!(staged.source_scanned_through, CommitPosition::new(4));
    assert_eq!(staged.right_commit, CommitPosition::new(2));
    assert_eq!(staged.transferred_records, 2);
    assert_eq!(staged.replicas_verified.len(), 3);
    for replica in plan.right_assignment.replicas.iter() {
        let records = services[replica]
            .read_staged_committed(&plan.right_assignment, None, 10)
            .await
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].identity.sequence, 2);
        assert_eq!(records[1].identity.sequence, 4);
        assert_eq!(
            records[0].frame,
            services[&plan.right_assignment.owner]
                .read_staged_committed(&plan.right_assignment, None, 10)
                .await
                .unwrap()[0]
                .frame
        );
    }
    assert_eq!(
        control
            .active_range_map(feed_id)
            .await
            .unwrap()
            .routes()
            .len(),
        1
    );
}
