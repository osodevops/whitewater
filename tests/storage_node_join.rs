use std::{collections::BTreeMap, sync::Arc, time::Duration};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use finnstream::{
    active_range::{
        AppendIdentity, OwnershipEpoch, RangeGeneration, RangeId, RangePosition,
        ReplicaAppendErrorCode, ReplicaAppendRequest, ReplicaAppendService, StorageNodeId,
    },
    codec::encode_record,
    control::{Command, ControlController},
    domain::StoredRecord,
    internal_plane::{
        CatalogSnapshot, CatalogSnapshotSource, CatalogSyncOutcome, CatalogSyncSupervisor,
    },
    storage::FileLogStore,
};
use tempfile::TempDir;
use uuid::Uuid;

fn node(name: &str) -> StorageNodeId {
    StorageNodeId::try_new(name).unwrap()
}

fn controller(directory: &TempDir, nodes: Vec<StorageNodeId>) -> Arc<ControlController> {
    Arc::new(
        ControlController::open_with_storage_nodes(
            directory.path().join("catalog.json"),
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap()),
            nodes,
        )
        .unwrap(),
    )
}

/// A storage-only Node's catalog: synced-replica mode like production
/// non-voters (`into_synced_replica`), refusing local mutations.
fn storage_controller(directory: &TempDir) -> Arc<ControlController> {
    Arc::new(
        ControlController::open_with_storage_nodes(
            directory.path().join("catalog.json"),
            Arc::new(FileLogStore::open(directory.path().join("data")).unwrap()),
            vec![],
        )
        .unwrap()
        .into_synced_replica(),
    )
}

struct StaticSource {
    snapshot: Option<CatalogSnapshot>,
}

#[async_trait::async_trait]
impl CatalogSnapshotSource for StaticSource {
    async fn latest_snapshot(&self) -> Option<CatalogSnapshot> {
        self.snapshot.clone()
    }
}

async fn snapshot_of(control: &ControlController) -> CatalogSnapshot {
    CatalogSnapshot {
        revision: control.revision().await,
        bytes: control.snapshot_bytes().await.unwrap(),
    }
}

/// Creates feeds until one places a replica on `wanted` (rendezvous
/// placement spreads each Feed's RF3 set across the eligible pool).
async fn feed_placed_on(
    control: &ControlController,
    wanted: &StorageNodeId,
) -> (Uuid, RangeId, StorageNodeId) {
    control
        .execute_commands(vec![Command::CreateSpace {
            name: "orders".to_owned(),
        }])
        .await
        .unwrap();
    for attempt in 0..12_u64 {
        let feed = format!("orders.joined{attempt}");
        control
            .execute_commands(vec![Command::CreateFeed { name: feed.clone() }])
            .await
            .unwrap();
        let feed_id = control.active_feed_by_name(&feed).await.unwrap().feed_id;
        let assignment = control.active_range_assignment(feed_id).await.unwrap();
        if assignment.replicas.iter().any(|replica| replica == wanted) {
            return (feed_id, assignment.range_id, assignment.owner.clone());
        }
    }
    panic!("no Feed placed on {wanted} after several attempts");
}

fn replica_request(
    feed_id: Uuid,
    range_id: RangeId,
    owner: &StorageNodeId,
    position: u64,
) -> ReplicaAppendRequest {
    let record = StoredRecord {
        message_id: Uuid::from_u128(1_000 + position as u128),
        producer_id: Uuid::from_u128(900),
        producer_sequence: position,
        event_time_ns: position as i64,
        ingest_time_ns: position as i64 + 1,
        key: b"customer-1".to_vec(),
        payload: b"payload".to_vec(),
        metadata: BTreeMap::new(),
    };
    ReplicaAppendRequest {
        feed_id,
        range_id,
        generation: RangeGeneration::new(1),
        ownership_epoch: OwnershipEpoch::new(1),
        append_owner: owner.clone(),
        expected_position: RangePosition::new(position),
        identity: AppendIdentity {
            writer_session_id: Uuid::from_u128(900),
            writer_epoch: 1,
            sequence: position,
        },
        cursor: format!("cursor-{position}"),
        frame_base64: STANDARD.encode(encode_record(&record).unwrap()),
    }
}

#[tokio::test]
async fn unsynced_storage_node_fences_appends_until_the_catalog_arrives() {
    // A committed leader catalog places a replica on the registered Node.
    let leader_dir = TempDir::new().unwrap();
    let leader = controller(
        &leader_dir,
        vec![node("control-1"), node("control-2"), node("control-3")],
    );
    leader
        .execute_commands(vec![Command::RegisterStorageNode {
            node: node("storage-9"),
            endpoint: "https://storage-9:7271".to_owned(),
            cert_pins: std::collections::BTreeSet::new(),
        }])
        .await
        .unwrap();
    let (feed_id, range_id, owner) = feed_placed_on(&leader, &node("storage-9")).await;

    // The storage Node starts with an empty local catalog and must refuse
    // replica traffic rather than trust caller-supplied placement.
    let storage_dir = TempDir::new().unwrap();
    let storage_catalog = storage_controller(&storage_dir);
    let replica_dir = TempDir::new().unwrap();
    let service = ReplicaAppendService::new(
        replica_dir.path().join("replicas"),
        node("storage-9"),
        storage_catalog.clone(),
    );
    let request = replica_request(feed_id, range_id, &owner, 1);
    let refused = service.append(request.clone()).await.unwrap_err();
    assert_eq!(refused.code, ReplicaAppendErrorCode::AssignmentNotFound);

    // Installing the newest committed snapshot arms the fencing checks.
    let supervisor = CatalogSyncSupervisor::new(
        storage_catalog.clone(),
        StaticSource {
            snapshot: Some(snapshot_of(&leader).await),
        },
        Duration::from_secs(1),
    );
    let outcome = supervisor.sync_once().await;
    assert!(matches!(outcome, CatalogSyncOutcome::Installed { .. }));
    let accepted = service.append(request).await.unwrap();
    assert_eq!(accepted.position, RangePosition::new(1));
}

#[tokio::test]
async fn catalog_sync_installs_only_newer_revisions() {
    let leader_dir = TempDir::new().unwrap();
    let leader = controller(
        &leader_dir,
        vec![node("control-1"), node("control-2"), node("control-3")],
    );
    leader.execute("CREATE SPACE orders;").await.unwrap();
    let first = snapshot_of(&leader).await;

    let storage_dir = TempDir::new().unwrap();
    let storage = controller(&storage_dir, vec![]);
    let supervisor = CatalogSyncSupervisor::new(
        storage.clone(),
        StaticSource {
            snapshot: Some(first.clone()),
        },
        Duration::from_secs(1),
    );
    assert_eq!(
        supervisor.sync_once().await,
        CatalogSyncOutcome::Installed {
            revision: first.revision
        }
    );
    assert!(storage.active_feed_by_name("orders.events").await.is_none());
    // Re-installing the same snapshot reports Current, not work.
    assert_eq!(
        supervisor.sync_once().await,
        CatalogSyncOutcome::Current {
            revision: first.revision
        }
    );
    // A stale snapshot can never roll the catalog backwards.
    let stale = CatalogSyncSupervisor::new(
        storage.clone(),
        StaticSource {
            snapshot: Some(CatalogSnapshot {
                revision: first.revision.saturating_sub(1),
                bytes: b"{}".to_vec(),
            }),
        },
        Duration::from_secs(1),
    );
    assert_eq!(
        stale.sync_once().await,
        CatalogSyncOutcome::Current {
            revision: first.revision
        }
    );
    // A newer committed snapshot installs and materializes its state.
    leader.execute("CREATE FEED orders.events;").await.unwrap();
    let newer = CatalogSyncSupervisor::new(
        storage.clone(),
        StaticSource {
            snapshot: Some(snapshot_of(&leader).await),
        },
        Duration::from_secs(1),
    );
    let outcome = newer.sync_once().await;
    assert!(matches!(
        outcome,
        CatalogSyncOutcome::Installed { revision } if revision > first.revision
    ));
    assert!(storage.active_feed_by_name("orders.events").await.is_some());
}

#[tokio::test]
async fn catalog_sync_reports_unavailable_sources() {
    let storage_dir = TempDir::new().unwrap();
    let storage = controller(&storage_dir, vec![]);
    let supervisor = CatalogSyncSupervisor::new(
        storage,
        StaticSource { snapshot: None },
        Duration::from_secs(1),
    );
    assert_eq!(
        supervisor.sync_once().await,
        CatalogSyncOutcome::Unavailable
    );
}

#[tokio::test]
async fn synced_replica_catalog_refuses_local_mutations_but_accepts_snapshots() {
    let leader_dir = TempDir::new().unwrap();
    let leader = controller(
        &leader_dir,
        vec![node("control-1"), node("control-2"), node("control-3")],
    );
    leader.execute("CREATE SPACE orders;").await.unwrap();

    let storage_dir = TempDir::new().unwrap();
    let storage = storage_controller(&storage_dir);
    // Reads are fine; local writes are refused so the replica can never
    // diverge from majority-committed authority.
    assert!(storage.execute("SHOW SPACES;").await.is_ok());
    assert!(storage.execute("CREATE SPACE rogue;").await.is_err());
    assert!(storage
        .execute_commands(vec![Command::RegisterStorageNode {
            node: node("storage-10"),
            endpoint: "https://storage-10:7271".to_owned(),
            cert_pins: std::collections::BTreeSet::new(),
        }])
        .await
        .is_err());
    // Snapshot installs and revision reads still work.
    let supervisor = CatalogSyncSupervisor::new(
        storage.clone(),
        StaticSource {
            snapshot: Some(snapshot_of(&leader).await),
        },
        Duration::from_secs(1),
    );
    assert!(matches!(
        supervisor.sync_once().await,
        CatalogSyncOutcome::Installed { .. }
    ));
    assert!(storage.execute("SHOW SPACES;").await.is_ok());
}
