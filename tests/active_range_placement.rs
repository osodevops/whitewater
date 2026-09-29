use std::sync::Arc;

use finnstream::{
    active_range::{CommitPosition, KeyToken, StorageNodeId},
    control::{Command, ControlController, ControlError, RangeSplitPlan, RangeSplitStage},
    storage::{FileLogStore, LogStore},
};
use tempfile::TempDir;
use uuid::Uuid;

fn storage_nodes(names: &[&str]) -> Vec<StorageNodeId> {
    names
        .iter()
        .map(|name| StorageNodeId::try_new(*name).unwrap())
        .collect()
}

fn controller(directory: &TempDir, nodes: Vec<StorageNodeId>) -> ControlController {
    let store: Arc<dyn LogStore> =
        Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
    ControlController::open_with_storage_nodes(directory.path().join("catalog.json"), store, nodes)
        .unwrap()
}

async fn create_feed(controller: &ControlController, request_id: Uuid) {
    controller
        .execute_commands_with_request_id(
            vec![
                Command::CreateSpace {
                    name: "orders".to_owned(),
                },
                Command::CreateFeed {
                    name: "orders.created".to_owned(),
                },
            ],
            request_id,
        )
        .await
        .unwrap();
}

async fn placement(controller: &ControlController) -> serde_json::Value {
    controller
        .execute_commands(vec![Command::InspectPlacement {
            feed: "orders.created".to_owned(),
        }])
        .await
        .unwrap()
        .results
        .remove(0)
        .data
}

#[tokio::test]
async fn replicated_feed_creation_produces_one_identical_rf3_assignment_on_every_voter() {
    let directories = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = storage_nodes(&["storage-4", "storage-2", "storage-1", "storage-3"]);
    let controllers = directories
        .iter()
        .map(|directory| controller(directory, nodes.clone()))
        .collect::<Vec<_>>();
    let request_id = Uuid::from_u128(100);

    for controller in &controllers {
        create_feed(controller, request_id).await;
    }
    let expected = placement(&controllers[0]).await;
    assert_eq!(expected["generation"], 1);
    assert_eq!(expected["ownership_epoch"], 1);
    assert_eq!(expected["owner"], "storage-1");
    assert_eq!(
        expected["replicas"],
        serde_json::json!(["storage-1", "storage-2", "storage-3"])
    );
    for controller in &controllers[1..] {
        assert_eq!(placement(controller).await, expected);
    }

    drop(controllers);
    for directory in &directories {
        let reopened = controller(directory, nodes.clone());
        assert_eq!(placement(&reopened).await, expected);
    }
}

#[tokio::test]
async fn followers_apply_the_leaders_embedded_placement_not_their_local_candidate_view() {
    let leader_directory = TempDir::new().unwrap();
    let follower_directory = TempDir::new().unwrap();
    let leader = controller(
        &leader_directory,
        storage_nodes(&["storage-1", "storage-2", "storage-3"]),
    );
    let follower = controller(
        &follower_directory,
        storage_nodes(&["storage-7", "storage-8", "storage-9"]),
    );
    let create_space = leader
        .prepare_replicated(
            Uuid::from_u128(401),
            1,
            Command::CreateSpace {
                name: "orders".to_owned(),
            },
        )
        .unwrap();
    let create_feed = leader
        .prepare_replicated(
            Uuid::from_u128(402),
            2,
            Command::CreateFeed {
                name: "orders.created".to_owned(),
            },
        )
        .unwrap();

    for target in [&leader, &follower] {
        assert!(target
            .apply_replicated(create_space.clone())
            .await
            .error
            .is_none());
        assert!(target
            .apply_replicated(create_feed.clone())
            .await
            .error
            .is_none());
    }
    assert_eq!(placement(&follower).await, placement(&leader).await);
    assert_eq!(placement(&follower).await["owner"], "storage-1");
}

#[tokio::test]
async fn feed_creation_is_refused_when_three_storage_nodes_are_not_available() {
    let directory = TempDir::new().unwrap();
    let controller = controller(&directory, storage_nodes(&["storage-1", "storage-2"]));
    controller
        .execute_commands(vec![Command::CreateSpace {
            name: "orders".to_owned(),
        }])
        .await
        .unwrap();

    let result = controller
        .execute_commands(vec![Command::CreateFeed {
            name: "orders.created".to_owned(),
        }])
        .await;
    assert!(matches!(
        result,
        Err(ControlError::InvalidOperation(message))
            if message.contains("three eligible storage Nodes")
    ));
    let feeds = controller
        .execute("SHOW FEEDS;")
        .await
        .unwrap()
        .results
        .remove(0)
        .data;
    assert_eq!(feeds, serde_json::json!([]));
}

#[tokio::test]
async fn ownership_transfer_increments_epoch_and_is_idempotent_by_request_id() {
    let directory = TempDir::new().unwrap();
    let controller = controller(
        &directory,
        storage_nodes(&["storage-1", "storage-2", "storage-3"]),
    );
    create_feed(&controller, Uuid::from_u128(200)).await;
    let request_id = Uuid::from_u128(201);
    let command = Command::TransferActiveRangeOwnership {
        feed: "orders.created".to_owned(),
        owner: StorageNodeId::try_new("storage-2").unwrap(),
    };

    let first = controller
        .execute_commands_with_request_id(vec![command.clone()], request_id)
        .await
        .unwrap();
    let repeated = controller
        .execute_commands_with_request_id(vec![command], request_id)
        .await
        .unwrap();
    assert_eq!(first.results[0].data, repeated.results[0].data);
    assert_eq!(first.results[0].data["owner"], "storage-2");
    assert_eq!(first.results[0].data["ownership_epoch"], 2);
    assert_eq!(placement(&controller).await["ownership_epoch"], 2);

    let invalid = controller
        .execute_commands(vec![Command::TransferActiveRangeOwnership {
            feed: "orders.created".to_owned(),
            owner: StorageNodeId::try_new("storage-4").unwrap(),
        }])
        .await;
    assert!(matches!(invalid, Err(ControlError::InvalidOperation(_))));
    assert_eq!(placement(&controller).await["ownership_epoch"], 2);
}

#[tokio::test]
async fn installed_catalog_snapshot_contains_active_range_placement() {
    let leader_directory = TempDir::new().unwrap();
    let follower_directory = TempDir::new().unwrap();
    let nodes = storage_nodes(&["storage-1", "storage-2", "storage-3"]);
    let leader = controller(&leader_directory, nodes.clone());
    let follower = controller(&follower_directory, nodes);
    create_feed(&leader, Uuid::from_u128(300)).await;

    follower
        .install_snapshot_bytes(&leader.snapshot_bytes().await.unwrap())
        .await
        .unwrap();
    let leader_placement = placement(&leader).await;
    assert_eq!(placement(&follower).await, leader_placement);
    let feed_id = Uuid::parse_str(leader_placement["feed_id"].as_str().unwrap()).unwrap();
    let range_map = follower.active_range_map(feed_id).await.unwrap();
    assert_eq!(range_map.routes().len(), 1);
    let (route, assignment) = follower
        .active_range_for_key(feed_id, b"order-123")
        .await
        .unwrap();
    assert_eq!(route.range_id, assignment.range_id);
    assert_eq!(route.range_id, range_map.routes()[0].range_id);
}

#[tokio::test]
async fn split_plan_is_consensus_persisted_but_cannot_change_authoritative_routing() {
    let leader_directory = TempDir::new().unwrap();
    let follower_directory = TempDir::new().unwrap();
    let nodes = storage_nodes(&["storage-1", "storage-2", "storage-3"]);
    let leader = controller(&leader_directory, nodes.clone());
    let follower = controller(&follower_directory, nodes);
    create_feed(&leader, Uuid::from_u128(400)).await;
    let placement_before = placement(&leader).await;
    let feed_id = Uuid::parse_str(placement_before["feed_id"].as_str().unwrap()).unwrap();
    let source_owner = placement_before["owner"].as_str().unwrap().to_owned();
    let request_id = Uuid::from_u128(401);
    let split_at = KeyToken::from_bytes([0x80; 16]);
    let command = Command::PrepareActiveRangeSplit {
        feed: "orders.created".to_owned(),
        split_at,
    };
    let first = leader
        .execute_commands_with_request_id(vec![command.clone()], request_id)
        .await
        .unwrap();
    let repeated = leader
        .execute_commands_with_request_id(vec![command], request_id)
        .await
        .unwrap();
    assert_eq!(first.results[0].data, repeated.results[0].data);
    let plan: RangeSplitPlan = serde_json::from_value(first.results[0].data.clone()).unwrap();
    assert_eq!(plan.stage, RangeSplitStage::Prepared);
    assert_eq!(plan.candidate_map.routes().len(), 2);
    assert_ne!(plan.right_assignment.owner.as_str(), source_owner);
    assert_eq!(
        leader
            .active_range_map(feed_id)
            .await
            .unwrap()
            .routes()
            .len(),
        1
    );

    let catching_up = leader
        .execute_commands(vec![Command::RecordActiveRangeSplitCatchUp {
            feed: "orders.created".to_owned(),
            plan_id: plan.plan_id,
            source_commit: CommitPosition::new(10),
            right_commit: CommitPosition::new(9),
            checksum_verified: true,
        }])
        .await
        .unwrap();
    assert_eq!(catching_up.results[0].data["stage"], "catching_up");
    let ready = leader
        .execute_commands(vec![Command::RecordActiveRangeSplitCatchUp {
            feed: "orders.created".to_owned(),
            plan_id: plan.plan_id,
            source_commit: CommitPosition::new(10),
            right_commit: CommitPosition::new(10),
            checksum_verified: true,
        }])
        .await
        .unwrap();
    assert_eq!(ready.results[0].data["stage"], "ready");
    assert_eq!(
        leader
            .active_range_map(feed_id)
            .await
            .unwrap()
            .routes()
            .len(),
        1
    );

    follower
        .install_snapshot_bytes(&leader.snapshot_bytes().await.unwrap())
        .await
        .unwrap();
    assert_eq!(
        placement(&follower).await["range_split_plan"]["stage"],
        "ready"
    );
    assert_eq!(
        follower
            .active_range_map(feed_id)
            .await
            .unwrap()
            .routes()
            .len(),
        1
    );
}
