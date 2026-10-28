//! The legal-hold gate on the retention sweep: a Feed under hold keeps
//! every retained byte — expiry and byte-budget deletion refuse — until
//! the hold is explicitly dropped. The sweep itself (delete mechanics,
//! retained floor, fail-closed reads) is proven by the store unit tests;
//! this file proves the catalog freeze reaches storage Nodes.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tempfile::TempDir;
use uuid::Uuid;
use whitewater::{
    active_range::{
        replication::ReplicaAppendService,
        store::{
            ActiveRangeAppend, ActiveRangeDescriptor, ActiveRangeStore, FileActiveRangeStore,
            FileActiveRangeStoreOptions,
        },
        AppendIdentity, CommitPosition, RangePosition, StorageNodeId,
    },
    codec::encode_record,
    control::{Command, ControlController, HistoryPolicy},
    domain::StoredRecord,
    storage::{FileLogStore, LogStore},
};

async fn append_committed(
    store: &FileActiveRangeStore,
    descriptor: &ActiveRangeDescriptor,
    start: u64,
    count: u64,
    payload_len: usize,
) {
    for sequence in start..start + count {
        let record = StoredRecord {
            message_id: Uuid::from_u128(500 + sequence as u128),
            producer_id: Uuid::from_u128(7),
            producer_sequence: sequence,
            event_time_ns: sequence as i64,
            ingest_time_ns: sequence as i64,
            key: b"key".to_vec(),
            payload: vec![0xAB; payload_len],
            metadata: std::collections::BTreeMap::new(),
        };
        store
            .append(ActiveRangeAppend {
                generation: descriptor.generation,
                ownership_epoch: descriptor.ownership_epoch,
                expected_position: Some(RangePosition::new(sequence)),
                identity: AppendIdentity {
                    writer_session_id: Uuid::from_u128(7),
                    writer_epoch: 1,
                    sequence,
                },
                cursor: format!("cursor-{sequence}"),
                frame: encode_record(&record).unwrap(),
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
async fn legal_hold_freezes_the_retention_sweep_until_dropped() {
    let catalog_directory = TempDir::new().unwrap();
    let replica_root = TempDir::new().unwrap();
    let store: Arc<dyn LogStore> =
        Arc::new(FileLogStore::open(catalog_directory.path().join("data")).unwrap());
    let controller = Arc::new(
        ControlController::open_with_storage_nodes(
            catalog_directory.path().join("catalog.json"),
            store,
            ["storage-1", "storage-2", "storage-3"]
                .into_iter()
                .map(|node| StorageNodeId::try_new(node).unwrap())
                .collect(),
        )
        .unwrap(),
    );
    let execution = controller
        .execute_commands(vec![
            Command::CreateSpace {
                name: "orders".to_owned(),
            },
            Command::CreateFeed {
                name: "orders.created".to_owned(),
            },
        ])
        .await
        .unwrap();
    let feed_id: Uuid =
        serde_json::from_value(execution.results[1].data["feed_id"].clone()).unwrap();
    let assignment = controller.active_range_assignment(feed_id).await.unwrap();
    let descriptor = ActiveRangeDescriptor {
        feed_id,
        range_id: assignment.range_id,
        generation: assignment.generation,
        ownership_epoch: assignment.ownership_epoch,
    };

    // Seal one segment through the real store path (2048-byte bound forces
    // rotation) so the sweep has a deletable span, then close it.
    {
        let store = FileActiveRangeStore::open_with_options(
            replica_root.path(),
            descriptor.clone(),
            FileActiveRangeStoreOptions {
                max_segment_bytes: 2_048,
                ..FileActiveRangeStoreOptions::default()
            },
        )
        .unwrap();
        let directory = replica_root
            .path()
            .join(descriptor.feed_id.to_string())
            .join(descriptor.range_id.as_uuid().to_string())
            .join(format!("generation-{}", descriptor.generation.value()));
        let mut appended = 0_u64;
        while appended < 64 && !directory.join("segment-000001.log").exists() {
            append_committed(&store, &descriptor, appended + 1, 1, 512).await;
            appended += 1;
        }
        assert!(
            directory.join("segment-000001.log").exists(),
            "segment 0 never sealed"
        );
    }

    let service = ReplicaAppendService::new(
        replica_root.path(),
        StorageNodeId::try_new("storage-1").unwrap(),
        controller.clone(),
    );
    let policies: HashMap<Uuid, HistoryPolicy> = HashMap::from([(
        feed_id,
        HistoryPolicy {
            expire_after_ns: Some(1),
            ..HistoryPolicy::default()
        },
    )]);

    // Frozen: the hold the catalog declares reaches the sweep's skip set.
    controller
        .execute("SET LEGAL HOLD ON FEED orders.created REASON 'matter 42';")
        .await
        .unwrap();
    let held: HashSet<Uuid> = controller.legal_hold_feed_ids().await;
    let report = service
        .retention_sweep(&policies, &held, None, i64::MAX)
        .await;
    assert_eq!(report.held_ranges, 1);
    assert_eq!(report.deleted_segments, 0);
    let directory = replica_root
        .path()
        .join(descriptor.feed_id.to_string())
        .join(descriptor.range_id.as_uuid().to_string())
        .join(format!("generation-{}", descriptor.generation.value()));
    assert!(directory.join("segment-000000.log").exists());

    // Dropping the hold unfreezes the sweep — the same policy now deletes.
    controller
        .execute("DROP LEGAL HOLD ON FEED orders.created;")
        .await
        .unwrap();
    let held: HashSet<Uuid> = controller.legal_hold_feed_ids().await;
    let report = service
        .retention_sweep(&policies, &held, None, i64::MAX)
        .await;
    assert_eq!(report.held_ranges, 0);
    assert_eq!(report.deleted_segments, 1);
    assert!(!directory.join("segment-000000.log").exists());
}
