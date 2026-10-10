use std::{collections::BTreeMap, sync::Arc};

use finnstream::{
    active_range::{CommitPosition, KeyToken, StorageNodeId},
    control::{
        Command, ControlController, ControlError, RangeMergePlan, RangeSplitPlan, RangeSplitStage,
        ReaderFrontierTranslation, ReaderStart,
    },
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
async fn domain_typed_command_and_legacy_space_share_replicated_identity() {
    let leader_dir = TempDir::new().unwrap();
    let follower_dir = TempDir::new().unwrap();
    let leader = controller(
        &leader_dir,
        storage_nodes(&["storage-1", "storage-2", "storage-3"]),
    );
    let follower = controller(
        &follower_dir,
        storage_nodes(&["storage-3", "storage-2", "storage-1"]),
    );
    let command = leader
        .prepare_replicated(
            Uuid::from_u128(777),
            1_700_000_000,
            Command::CreateDomain {
                name: "orders".to_owned(),
            },
        )
        .unwrap();
    let left = leader.apply_replicated(command.clone()).await;
    let right = follower.apply_replicated(command).await;
    assert_eq!(left, right);
    assert_eq!(left.result.as_ref().unwrap().statement, "CREATE DOMAIN");
    let id = left.result.unwrap().data["space_id"].clone();
    follower
        .install_snapshot_bytes(&leader.snapshot_bytes().await.unwrap())
        .await
        .unwrap();
    let described = follower
        .execute("DESCRIBE DOMAIN orders; DESCRIBE SPACE orders; SHOW DOMAINS; SHOW SPACES;")
        .await
        .unwrap();
    assert_eq!(described.results[0].data["space_id"], id);
    assert_eq!(described.results[0].data, described.results[1].data);
    assert_eq!(described.results[2].data, described.results[3].data);
}

#[tokio::test]
async fn subscription_progress_uses_the_full_candidate_pool_without_changing_rf3() {
    for candidate_count in [3, 12, 24] {
        let directory = TempDir::new().unwrap();
        let nodes = (0..candidate_count)
            .map(|index| format!("storage-{index:02}"))
            .collect::<Vec<_>>();
        let node_ids = nodes
            .iter()
            .map(|name| StorageNodeId::try_new(name.clone()).unwrap())
            .collect::<Vec<_>>();
        let control = controller(&directory, node_ids.clone());
        create_feed(&control, Uuid::from_u128(30_000 + candidate_count as u128)).await;
        let mut selected = std::collections::BTreeSet::new();
        for index in 0..96 {
            let result = control
                .execute_commands_with_request_id(
                    vec![Command::CreateSubscription {
                        name: format!("orders.sub{index}"),
                        feed: "orders.created".to_owned(),
                        start: ReaderStart::Beginning,
                    }],
                    Uuid::from_u128(40_000 + index),
                )
                .await
                .unwrap();
            let public = &result.results[0].data;
            assert!(public.get("replicas").is_none());
            assert!(public.get("owner").is_none());
            let snapshot: serde_json::Value =
                serde_json::from_slice(&control.snapshot_bytes().await.unwrap()).unwrap();
            let subscription_id = public["subscription_id"].as_str().unwrap();
            let assignment = &snapshot["subscription_progress_assignments"][subscription_id];
            let replicas = assignment["replicas"].as_array().unwrap();
            assert_eq!(replicas.len(), 3);
            let assigned = replicas
                .iter()
                .map(|node| node.as_str().unwrap().to_owned())
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(assigned.len(), 3);
            assert!(assigned.contains(assignment["owner"].as_str().unwrap()));
            assert!(assigned.iter().all(|node| nodes.contains(node)));
            selected.extend(assigned);
        }
        assert!(
            selected.len() == 3 && candidate_count == 3
                || candidate_count == 12 && selected.len() >= 8
                || candidate_count == 24 && selected.len() >= 16
        );
    }
}

#[tokio::test]
async fn follower_uses_leader_subscription_placement_not_local_candidates() {
    let leader_dir = TempDir::new().unwrap();
    let follower_dir = TempDir::new().unwrap();
    let leader_nodes = (0..24)
        .map(|index| format!("storage-{index:02}"))
        .collect::<Vec<_>>();
    let leader = controller(
        &leader_dir,
        leader_nodes
            .iter()
            .map(|node| StorageNodeId::try_new(node.clone()).unwrap())
            .collect(),
    );
    let follower = controller(
        &follower_dir,
        storage_nodes(&["storage-97", "storage-98", "storage-99"]),
    );
    create_feed(&leader, Uuid::from_u128(50_000)).await;
    follower
        .install_snapshot_bytes(&leader.snapshot_bytes().await.unwrap())
        .await
        .unwrap();
    let command = leader
        .prepare_replicated(
            Uuid::from_u128(50_001),
            1_700_000_000,
            Command::CreateSubscription {
                name: "orders.billing".to_owned(),
                feed: "orders.created".to_owned(),
                start: ReaderStart::Beginning,
            },
        )
        .unwrap();
    let fixed = command.fixed_subscription_progress.as_ref().unwrap();
    assert!(fixed
        .replicas
        .iter()
        .all(|node| leader_nodes.iter().any(|name| name == node.as_str())));
    assert_eq!(
        leader.apply_replicated(command.clone()).await,
        follower.apply_replicated(command).await
    );
    let left: serde_json::Value =
        serde_json::from_slice(&leader.snapshot_bytes().await.unwrap()).unwrap();
    let right: serde_json::Value =
        serde_json::from_slice(&follower.snapshot_bytes().await.unwrap()).unwrap();
    assert_eq!(
        left["subscription_progress_assignments"],
        right["subscription_progress_assignments"]
    );
    assert!(leader
        .active_subscription_by_name("orders.billing")
        .await
        .is_some());
}

#[tokio::test]
async fn subscription_definition_survives_catalog_snapshot_without_creating_a_feed() {
    let source_dir = TempDir::new().unwrap();
    let follower_dir = TempDir::new().unwrap();
    let nodes = storage_nodes(&["storage-1", "storage-2", "storage-3"]);
    let source = controller(&source_dir, nodes);
    let follower = controller(
        &follower_dir,
        storage_nodes(&["storage-4", "storage-3", "storage-2", "storage-1"]),
    );
    create_feed(&source, Uuid::from_u128(9_001)).await;
    let defined = source
        .execute("CREATE SUBSCRIPTION orders.billing FROM orders.created;")
        .await
        .unwrap();
    assert_eq!(defined.results[0].data["stage"], "declared");
    let subscription = source
        .active_subscription_by_name("orders.billing")
        .await
        .unwrap();
    assert_eq!(
        subscription.feed_id,
        source
            .active_feed_by_name("orders.created")
            .await
            .unwrap()
            .feed_id
    );
    assert!(defined.results[0].data.get("replicas").is_none());
    let feeds = source.execute("SHOW FEEDS;").await.unwrap();
    assert_eq!(feeds.results[0].data.as_array().unwrap().len(), 1);
    let source_snapshot = source.snapshot_bytes().await.unwrap();
    let snapshot: serde_json::Value = serde_json::from_slice(&source_snapshot).unwrap();
    let subscription_key = subscription.subscription_id.to_string();
    let progress_assignment = snapshot["subscription_progress_assignments"]
        .get(subscription_key.as_str())
        .unwrap();
    assert_eq!(progress_assignment["ownership_epoch"], 1);
    assert_eq!(progress_assignment["replicas"].as_array().unwrap().len(), 3);
    follower
        .install_snapshot_bytes(&source_snapshot)
        .await
        .unwrap();
    assert_eq!(
        follower.active_subscription_by_name("orders.billing").await,
        Some(subscription)
    );
    let installed: serde_json::Value =
        serde_json::from_slice(&follower.snapshot_bytes().await.unwrap()).unwrap();
    assert_eq!(
        installed["subscription_progress_assignments"][subscription_key.as_str()],
        *progress_assignment
    );
    assert!(follower.execute("DROP FEED orders.created;").await.is_err());
    follower
        .execute("DROP SUBSCRIPTION orders.billing; DROP FEED orders.created;")
        .await
        .unwrap();
    assert!(follower
        .active_subscription_by_name("orders.billing")
        .await
        .is_none());
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
async fn owner_move_requires_a_verified_caught_up_target_and_atomic_placement_change() {
    let leader_dir = TempDir::new().unwrap();
    let follower_dir = TempDir::new().unwrap();
    let nodes = storage_nodes(&["storage-1", "storage-2", "storage-3"]);
    let leader = controller(&leader_dir, nodes.clone());
    let follower = controller(&follower_dir, nodes);
    create_feed(&leader, Uuid::from_u128(190)).await;
    let original = placement(&leader).await;
    let range_id = serde_json::from_value(original["range_id"].clone()).unwrap();
    let request_id = Uuid::from_u128(191);
    let command = Command::PrepareOwnerMove {
        feed: "orders.created".to_owned(),
        range_id,
        new_owner: StorageNodeId::try_new("storage-2").unwrap(),
    };
    let prepared = leader
        .execute_commands_with_request_id(vec![command.clone()], request_id)
        .await
        .unwrap();
    let retry = leader
        .execute_commands_with_request_id(vec![command], request_id)
        .await
        .unwrap();
    assert_eq!(prepared.results[0].data, retry.results[0].data);
    assert_eq!(placement(&leader).await["owner"], original["owner"]);
    let plan_id = serde_json::from_value(prepared.results[0].data["plan_id"].clone()).unwrap();
    let activate = || Command::ActivateOwnerMove {
        feed: "orders.created".to_owned(),
        plan_id,
    };
    assert!(leader.execute_commands(vec![activate()]).await.is_err());
    leader
        .execute_commands(vec![Command::RecordOwnerMoveCatchUp {
            feed: "orders.created".to_owned(),
            plan_id,
            source_commit: CommitPosition::new(4),
            target_commit: CommitPosition::new(3),
            checksum_verified: true,
        }])
        .await
        .unwrap();
    assert!(leader.execute_commands(vec![activate()]).await.is_err());
    leader
        .execute_commands(vec![Command::RecordOwnerMoveCatchUp {
            feed: "orders.created".to_owned(),
            plan_id,
            source_commit: CommitPosition::new(4),
            target_commit: CommitPosition::new(4),
            checksum_verified: true,
        }])
        .await
        .unwrap();
    follower
        .install_snapshot_bytes(&leader.snapshot_bytes().await.unwrap())
        .await
        .unwrap();
    assert_eq!(
        placement(&follower).await["owner_move_plans"][0]["stage"],
        "ready"
    );
    let moved = leader
        .execute_commands(vec![activate()])
        .await
        .unwrap()
        .results
        .remove(0)
        .data;
    assert_eq!(moved["owner"], "storage-2");
    assert_eq!(moved["ownership_epoch"], 2);
    assert_eq!(moved["replicas"], original["replicas"]);
    assert!(placement(&leader).await["owner_move_plans"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn owner_move_plan_cannot_overwrite_newer_recovery_placement() {
    let directory = TempDir::new().unwrap();
    let control = controller(
        &directory,
        storage_nodes(&["storage-1", "storage-2", "storage-3", "storage-4"]),
    );
    create_feed(&control, Uuid::from_u128(202)).await;
    let original = placement(&control).await;
    let range_id = serde_json::from_value(original["range_id"].clone()).unwrap();
    let prepared = control
        .execute_commands(vec![Command::PrepareOwnerMove {
            feed: "orders.created".to_owned(),
            range_id,
            new_owner: StorageNodeId::try_new("storage-2").unwrap(),
        }])
        .await
        .unwrap();
    let plan_id = serde_json::from_value(prepared.results[0].data["plan_id"].clone()).unwrap();
    assert!(control
        .execute_commands(vec![Command::PrepareFollowerMove {
            feed: "orders.created".to_owned(),
            range_id,
            removed_replica: StorageNodeId::try_new("storage-3").unwrap(),
            replacement_replica: StorageNodeId::try_new("storage-4").unwrap(),
        }])
        .await
        .is_err());
    control
        .execute_commands(vec![Command::RecordOwnerMoveCatchUp {
            feed: "orders.created".to_owned(),
            plan_id,
            source_commit: CommitPosition::new(3),
            target_commit: CommitPosition::new(3),
            checksum_verified: true,
        }])
        .await
        .unwrap();
    control
        .execute_commands(vec![Command::RecoverActiveRangeOwnership {
            feed: "orders.created".to_owned(),
            expected_owner: StorageNodeId::try_new("storage-1").unwrap(),
            expected_epoch: finnstream::active_range::OwnershipEpoch::new(1),
            new_owner: StorageNodeId::try_new("storage-3").unwrap(),
        }])
        .await
        .unwrap();
    assert!(control
        .execute_commands(vec![Command::ActivateOwnerMove {
            feed: "orders.created".to_owned(),
            plan_id,
        }])
        .await
        .is_err());
    assert_eq!(placement(&control).await["owner"], "storage-3");
    control
        .execute_commands(vec![Command::AbortOwnerMove {
            feed: "orders.created".to_owned(),
            plan_id,
        }])
        .await
        .unwrap();
    assert!(placement(&control).await["owner_move_plans"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn direct_owner_transfer_is_refused_without_verified_catch_up() {
    let directory = TempDir::new().unwrap();
    let controller = controller(
        &directory,
        storage_nodes(&["storage-1", "storage-2", "storage-3"]),
    );
    create_feed(&controller, Uuid::from_u128(200)).await;
    let original = placement(&controller).await;
    let result = controller
        .execute_commands(vec![Command::TransferActiveRangeOwnership {
            feed: "orders.created".to_owned(),
            owner: StorageNodeId::try_new("storage-2").unwrap(),
        }])
        .await;
    assert!(
        matches!(result, Err(ControlError::InvalidOperation(message)) if message.contains("verified owner-movement"))
    );
    assert_eq!(placement(&controller).await["owner"], original["owner"]);
    assert_eq!(
        placement(&controller).await["ownership_epoch"],
        original["ownership_epoch"]
    );
    let range_id = serde_json::from_value(original["range_id"].clone()).unwrap();
    assert!(controller
        .execute_commands(vec![Command::PrepareOwnerMove {
            feed: "orders.created".to_owned(),
            range_id,
            new_owner: StorageNodeId::try_new("storage-4").unwrap(),
        }])
        .await
        .is_err());
}

#[tokio::test]
async fn follower_move_keeps_rf3_until_verified_replacement_and_survives_snapshot() {
    let leader_dir = TempDir::new().unwrap();
    let follower_dir = TempDir::new().unwrap();
    let four_nodes = storage_nodes(&["storage-1", "storage-2", "storage-3", "storage-4"]);
    let leader = controller(&leader_dir, four_nodes);
    let follower = controller(
        &follower_dir,
        storage_nodes(&["storage-1", "storage-2", "storage-3"]),
    );
    create_feed(&leader, Uuid::from_u128(500)).await;
    let original = placement(&leader).await;
    let range_id = serde_json::from_value(original["range_id"].clone()).unwrap();
    let move_command = Command::PrepareFollowerMove {
        feed: "orders.created".to_owned(),
        range_id,
        removed_replica: storage_nodes(&["storage-3"]).remove(0),
        replacement_replica: storage_nodes(&["storage-4"]).remove(0),
    };
    let request_id = Uuid::from_u128(501);
    let first = leader
        .execute_commands_with_request_id(vec![move_command.clone()], request_id)
        .await
        .unwrap();
    let retry = leader
        .execute_commands_with_request_id(vec![move_command], request_id)
        .await
        .unwrap();
    assert_eq!(first.results[0].data, retry.results[0].data);
    let plan_id = serde_json::from_value(first.results[0].data["plan_id"].clone()).unwrap();
    assert_eq!(first.results[0].data["stage"], "prepared");
    assert_eq!(
        first.results[0].data["candidate_assignment"]["ownership_epoch"],
        2
    );
    assert_eq!(
        first.results[0].data["candidate_assignment"]["replicas"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(placement(&leader).await["replicas"], original["replicas"]);
    assert!(leader
        .execute_commands(vec![Command::ActivateFollowerMove {
            feed: "orders.created".to_owned(),
            plan_id,
        }])
        .await
        .is_err());
    leader
        .execute_commands(vec![Command::RecordFollowerMoveCatchUp {
            feed: "orders.created".to_owned(),
            plan_id,
            source_commit: CommitPosition::new(8),
            target_commit: CommitPosition::new(7),
            checksum_verified: true,
        }])
        .await
        .unwrap();
    assert_eq!(
        placement(&leader).await["range_move_plans"][0]["stage"],
        "catching_up"
    );
    assert!(leader
        .execute_commands(vec![Command::ActivateFollowerMove {
            feed: "orders.created".to_owned(),
            plan_id,
        }])
        .await
        .is_err());
    leader
        .execute_commands(vec![Command::RecordFollowerMoveCatchUp {
            feed: "orders.created".to_owned(),
            plan_id,
            source_commit: CommitPosition::new(8),
            target_commit: CommitPosition::new(8),
            checksum_verified: true,
        }])
        .await
        .unwrap();
    follower
        .install_snapshot_bytes(&leader.snapshot_bytes().await.unwrap())
        .await
        .unwrap();
    assert_eq!(
        placement(&follower).await["range_move_plans"][0]["stage"],
        "ready"
    );
    let new_assignment = follower
        .execute_commands(vec![Command::ActivateFollowerMove {
            feed: "orders.created".to_owned(),
            plan_id,
        }])
        .await
        .unwrap()
        .results
        .remove(0)
        .data;
    assert_eq!(new_assignment["owner"], original["owner"]);
    assert_eq!(new_assignment["generation"], original["generation"]);
    assert_eq!(new_assignment["ownership_epoch"], 2);
    assert_eq!(
        new_assignment["replicas"],
        serde_json::json!(["storage-1", "storage-2", "storage-4"])
    );
    assert!(placement(&follower).await["range_move_plans"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        placement(&follower).await["range_map"],
        original["range_map"]
    );
    drop(follower);
    let restarted = controller(
        &follower_dir,
        storage_nodes(&["storage-1", "storage-2", "storage-3"]),
    );
    assert_eq!(
        placement(&restarted).await["replicas"],
        new_assignment["replicas"]
    );
    assert_eq!(placement(&restarted).await["ownership_epoch"], 2);
    assert_eq!(
        restarted
            .completed_follower_move(range_id)
            .await
            .unwrap()
            .plan_id,
        plan_id
    );
}

#[tokio::test]
async fn follower_move_rejects_owner_replacement_ineligible_node_and_conflicting_plan() {
    let dir = TempDir::new().unwrap();
    let control = controller(
        &dir,
        storage_nodes(&["storage-1", "storage-2", "storage-3", "storage-4"]),
    );
    create_feed(&control, Uuid::from_u128(510)).await;
    let current = placement(&control).await;
    let range_id = serde_json::from_value(current["range_id"].clone()).unwrap();
    for (removed, replacement) in [
        ("storage-1", "storage-4"),
        ("storage-3", "storage-2"),
        ("storage-3", "storage-5"),
    ] {
        assert!(control
            .execute_commands(vec![Command::PrepareFollowerMove {
                feed: "orders.created".to_owned(),
                range_id,
                removed_replica: storage_nodes(&[removed]).remove(0),
                replacement_replica: storage_nodes(&[replacement]).remove(0),
            }])
            .await
            .is_err());
    }
    let prepared = control
        .execute_commands(vec![Command::PrepareFollowerMove {
            feed: "orders.created".to_owned(),
            range_id,
            removed_replica: storage_nodes(&["storage-3"]).remove(0),
            replacement_replica: storage_nodes(&["storage-4"]).remove(0),
        }])
        .await
        .unwrap();
    let plan_id = serde_json::from_value(prepared.results[0].data["plan_id"].clone()).unwrap();
    assert!(control
        .execute_commands(vec![Command::PrepareFollowerMove {
            feed: "orders.created".to_owned(),
            range_id,
            removed_replica: storage_nodes(&["storage-2"]).remove(0),
            replacement_replica: storage_nodes(&["storage-4"]).remove(0),
        }])
        .await
        .is_err());
    control
        .execute_commands(vec![Command::AbortFollowerMove {
            feed: "orders.created".to_owned(),
            plan_id,
        }])
        .await
        .unwrap();
    assert_eq!(placement(&control).await["replicas"], current["replicas"]);
}

#[tokio::test]
async fn follower_move_cannot_overwrite_newer_ownership() {
    let dir = TempDir::new().unwrap();
    let control = controller(
        &dir,
        storage_nodes(&["storage-1", "storage-2", "storage-3", "storage-4"]),
    );
    create_feed(&control, Uuid::from_u128(520)).await;
    let original = placement(&control).await;
    let range_id = serde_json::from_value(original["range_id"].clone()).unwrap();
    let prepared = control
        .execute_commands(vec![Command::PrepareFollowerMove {
            feed: "orders.created".to_owned(),
            range_id,
            removed_replica: storage_nodes(&["storage-3"]).remove(0),
            replacement_replica: storage_nodes(&["storage-4"]).remove(0),
        }])
        .await
        .unwrap();
    let plan_id = serde_json::from_value(prepared.results[0].data["plan_id"].clone()).unwrap();
    control
        .execute_commands(vec![Command::RecordFollowerMoveCatchUp {
            feed: "orders.created".to_owned(),
            plan_id,
            source_commit: CommitPosition::new(3),
            target_commit: CommitPosition::new(3),
            checksum_verified: true,
        }])
        .await
        .unwrap();
    control
        .execute_commands(vec![Command::RecoverActiveRangeOwnership {
            feed: "orders.created".to_owned(),
            expected_owner: storage_nodes(&["storage-1"]).remove(0),
            expected_epoch: finnstream::active_range::OwnershipEpoch::new(1),
            new_owner: storage_nodes(&["storage-2"]).remove(0),
        }])
        .await
        .unwrap();
    assert!(control
        .execute_commands(vec![Command::ActivateFollowerMove {
            feed: "orders.created".to_owned(),
            plan_id,
        }])
        .await
        .is_err());
    assert_eq!(placement(&control).await["owner"], "storage-2");
    assert_eq!(placement(&control).await["replicas"], original["replicas"]);
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
            source_scanned_through: CommitPosition::new(9),
            right_commit: CommitPosition::new(4),
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
            source_scanned_through: CommitPosition::new(10),
            right_commit: CommitPosition::new(5),
            checksum_verified: true,
        }])
        .await
        .unwrap();
    assert_eq!(ready.results[0].data["stage"], "ready");
    let activated = leader
        .execute_commands(vec![Command::ActivateActiveRangeSplit {
            feed: "orders.created".to_owned(),
            plan_id: plan.plan_id,
            left_writer_sequences: Vec::new(),
            right_writer_sequences: Vec::new(),
            reader_translations: Vec::new(),
        }])
        .await
        .unwrap();
    assert_eq!(
        activated.results[0].data["range_map"]["routes"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let active_map = leader.active_range_map(feed_id).await.unwrap();
    assert_eq!(active_map.routes().len(), 2);
    let merge_request_id = Uuid::from_u128(402);
    let merge_command = Command::PrepareActiveRangeMerge {
        feed: "orders.created".to_owned(),
        left_range_id: active_map.routes()[0].range_id,
        right_range_id: active_map.routes()[1].range_id,
    };
    let merge = leader
        .execute_commands_with_request_id(vec![merge_command.clone()], merge_request_id)
        .await
        .unwrap();
    let merge_retry = leader
        .execute_commands_with_request_id(vec![merge_command], merge_request_id)
        .await
        .unwrap();
    assert_eq!(merge.results[0].data, merge_retry.results[0].data);
    assert_eq!(merge.results[0].data["stage"], "prepared");
    assert_eq!(
        merge.results[0].data["candidate_map"]["routes"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        leader
            .active_range_map(feed_id)
            .await
            .unwrap()
            .routes()
            .len(),
        2
    );

    follower
        .install_snapshot_bytes(&leader.snapshot_bytes().await.unwrap())
        .await
        .unwrap();
    let follower_placement = placement(&follower).await;
    assert!(follower_placement["range_split_plan"].is_null());
    assert_eq!(follower_placement["range_merge_plan"]["stage"], "prepared");
    assert_eq!(
        follower
            .active_range_map(feed_id)
            .await
            .unwrap()
            .routes()
            .len(),
        2
    );
}

#[tokio::test]
async fn split_and_merge_install_reader_translations_and_fence_stale_sessions() {
    let directory = TempDir::new().unwrap();
    let control = controller(
        &directory,
        storage_nodes(&["storage-1", "storage-2", "storage-3"]),
    );
    create_feed(&control, Uuid::from_u128(90_001)).await;
    control
        .execute(
            "CREATE READER audit FROM orders.created START AT BEGINNING; \
             CREATE READER metrics FROM orders.created START AT NOW;",
        )
        .await
        .unwrap();
    let audit = control.active_reader_by_name("audit").await.unwrap();
    let metrics = control.active_reader_by_name("metrics").await.unwrap();
    let feed_id = audit.feed_id;
    let source_range = control.active_range_map(feed_id).await.unwrap().routes()[0].range_id;
    control
        .execute_commands(vec![Command::OpenReaderSession {
            reader: "audit".to_owned(),
            capacity: 8,
        }])
        .await
        .unwrap();
    let acknowledged = BTreeMap::from([(source_range, "ack-cursor-1".to_owned())]);
    control
        .execute_commands(vec![Command::RecordReaderFrontier {
            reader: "audit".to_owned(),
            session_epoch: 1,
            cursor: "ack-token-1".to_owned(),
            positions: acknowledged.clone(),
            expected_cursor: None,
            fence_delivery: true,
            fetch_request_id: Some(Uuid::from_u128(90_002)),
        }])
        .await
        .unwrap();
    control
        .execute_commands(vec![Command::AcknowledgeReader {
            reader: "audit".to_owned(),
            session_epoch: 1,
            cursor: "ack-token-1".to_owned(),
        }])
        .await
        .unwrap();
    let delivered = BTreeMap::from([(source_range, "delivered-cursor-2".to_owned())]);
    control
        .execute_commands(vec![Command::RecordReaderFrontier {
            reader: "audit".to_owned(),
            session_epoch: 1,
            cursor: "del-token-2".to_owned(),
            positions: delivered.clone(),
            expected_cursor: Some("ack-token-1".to_owned()),
            fence_delivery: true,
            fetch_request_id: Some(Uuid::from_u128(90_003)),
        }])
        .await
        .unwrap();

    let prepared = control
        .execute_commands(vec![Command::PrepareActiveRangeSplit {
            feed: "orders.created".to_owned(),
            split_at: KeyToken::from_bytes([0x80; 16]),
        }])
        .await
        .unwrap();
    let plan: RangeSplitPlan = serde_json::from_value(prepared.results[0].data.clone()).unwrap();
    let (left_range, right_range) = (
        plan.candidate_map.routes()[0].range_id,
        plan.candidate_map.routes()[1].range_id,
    );
    for (scanned, right_commit, stage) in [(9_u64, 4_u64, "catching_up"), (10, 5, "ready")] {
        let result = control
            .execute_commands(vec![Command::RecordActiveRangeSplitCatchUp {
                feed: "orders.created".to_owned(),
                plan_id: plan.plan_id,
                source_commit: CommitPosition::new(10),
                source_scanned_through: CommitPosition::new(scanned),
                right_commit: CommitPosition::new(right_commit),
                checksum_verified: true,
            }])
            .await
            .unwrap();
        assert_eq!(result.results[0].data["stage"], stage);
    }

    let audit_translation = ReaderFrontierTranslation {
        reader_id: audit.reader_id,
        expected_session_epoch: 1,
        expected_acknowledged_cursor: Some("ack-token-1".to_owned()),
        expected_delivered_cursor: Some("del-token-2".to_owned()),
        expected_acknowledged: acknowledged.clone(),
        expected_delivered: delivered.clone(),
        expected_has_frontier: true,
        acknowledged: BTreeMap::from([
            (left_range, "ack-cursor-1".to_owned()),
            (right_range, String::new()),
        ]),
    };
    let metrics_translation = ReaderFrontierTranslation {
        reader_id: metrics.reader_id,
        expected_session_epoch: 0,
        expected_acknowledged_cursor: None,
        expected_delivered_cursor: None,
        expected_acknowledged: BTreeMap::new(),
        expected_delivered: BTreeMap::new(),
        expected_has_frontier: false,
        acknowledged: BTreeMap::from([(left_range, String::new()), (right_range, String::new())]),
    };

    // An incomplete cutover cannot activate: every active Reader needs a translation.
    assert!(control
        .execute_commands(vec![Command::ActivateActiveRangeSplit {
            feed: "orders.created".to_owned(),
            plan_id: plan.plan_id,
            left_writer_sequences: Vec::new(),
            right_writer_sequences: Vec::new(),
            reader_translations: vec![audit_translation.clone()],
        }])
        .await
        .is_err());
    // Stale Reader evidence must refuse rather than guess a frontier.
    let mut stale = audit_translation.clone();
    stale.expected_delivered_cursor = Some("other-token".to_owned());
    assert!(control
        .execute_commands(vec![Command::ActivateActiveRangeSplit {
            feed: "orders.created".to_owned(),
            plan_id: plan.plan_id,
            left_writer_sequences: Vec::new(),
            right_writer_sequences: Vec::new(),
            reader_translations: vec![stale, metrics_translation.clone()],
        }])
        .await
        .is_err());
    // A translation that does not cover every candidate route is rejected.
    let mut uncovered = audit_translation.clone();
    uncovered.acknowledged.remove(&right_range);
    assert!(control
        .execute_commands(vec![Command::ActivateActiveRangeSplit {
            feed: "orders.created".to_owned(),
            plan_id: plan.plan_id,
            left_writer_sequences: Vec::new(),
            right_writer_sequences: Vec::new(),
            reader_translations: vec![uncovered, metrics_translation.clone()],
        }])
        .await
        .is_err());
    assert_eq!(
        control
            .active_range_map(feed_id)
            .await
            .unwrap()
            .routes()
            .len(),
        1
    );

    let activated = control
        .execute_commands(vec![Command::ActivateActiveRangeSplit {
            feed: "orders.created".to_owned(),
            plan_id: plan.plan_id,
            left_writer_sequences: Vec::new(),
            right_writer_sequences: Vec::new(),
            reader_translations: vec![audit_translation, metrics_translation],
        }])
        .await
        .unwrap();
    assert_eq!(
        activated.results[0].data["range_map"]["routes"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let audit_after = control.active_reader_by_name("audit").await.unwrap();
    assert_eq!(audit_after.session_epoch, 2);
    assert!(!audit_after.session_active);
    assert_eq!(audit_after.delivered_cursor.as_deref(), Some("ack-token-1"));
    assert!(control
        .execute_commands(vec![Command::RecordReaderFrontier {
            reader: "audit".to_owned(),
            session_epoch: 1,
            cursor: "stale-delivery".to_owned(),
            positions: BTreeMap::from(
                [(left_range, "x".to_owned()), (right_range, String::new()),]
            ),
            expected_cursor: Some("ack-token-1".to_owned()),
            fence_delivery: true,
            fetch_request_id: Some(Uuid::from_u128(90_004)),
        }])
        .await
        .is_err());
    let metrics_after = control.active_reader_by_name("metrics").await.unwrap();
    assert_eq!(metrics_after.session_epoch, 1);
    assert_eq!(metrics_after.acknowledged_cursor, None);
    assert_eq!(metrics_after.delivered_cursor, None);

    let merged = control
        .execute_commands(vec![Command::PrepareActiveRangeMerge {
            feed: "orders.created".to_owned(),
            left_range_id: left_range,
            right_range_id: right_range,
        }])
        .await
        .unwrap();
    let merge_plan: RangeMergePlan =
        serde_json::from_value(merged.results[0].data.clone()).unwrap();
    let merged_range = merge_plan.candidate_map.routes()[0].range_id;
    control
        .execute_commands(vec![Command::RecordActiveRangeMergeStaging {
            feed: "orders.created".to_owned(),
            plan_id: merge_plan.plan_id,
            left_commit: CommitPosition::new(2),
            right_commit: CommitPosition::new(2),
            merged_commit: CommitPosition::new(4),
            checksum_verified: true,
        }])
        .await
        .unwrap();
    let split_acknowledged = BTreeMap::from([
        (left_range, "ack-cursor-1".to_owned()),
        (right_range, String::new()),
    ]);
    let audit_merge = ReaderFrontierTranslation {
        reader_id: audit.reader_id,
        expected_session_epoch: 2,
        expected_acknowledged_cursor: Some("ack-token-1".to_owned()),
        expected_delivered_cursor: Some("ack-token-1".to_owned()),
        expected_acknowledged: split_acknowledged.clone(),
        expected_delivered: split_acknowledged,
        expected_has_frontier: true,
        acknowledged: BTreeMap::from([(merged_range, "ack-cursor-1".to_owned())]),
    };
    let metrics_merge = ReaderFrontierTranslation {
        reader_id: metrics.reader_id,
        expected_session_epoch: 1,
        expected_acknowledged_cursor: None,
        expected_delivered_cursor: None,
        expected_acknowledged: BTreeMap::new(),
        expected_delivered: BTreeMap::new(),
        expected_has_frontier: false,
        acknowledged: BTreeMap::from([(merged_range, String::new())]),
    };
    let merge_activated = control
        .execute_commands(vec![Command::ActivateActiveRangeMerge {
            feed: "orders.created".to_owned(),
            plan_id: merge_plan.plan_id,
            writer_sequences: Vec::new(),
            reader_translations: vec![audit_merge, metrics_merge],
        }])
        .await
        .unwrap();
    assert_eq!(
        merge_activated.results[0].data["range_map"]["routes"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let audit_merged = control.active_reader_by_name("audit").await.unwrap();
    assert_eq!(audit_merged.session_epoch, 3);
    assert!(control
        .execute_commands(vec![Command::RecordReaderFrontier {
            reader: "audit".to_owned(),
            session_epoch: 2,
            cursor: "stale".to_owned(),
            positions: BTreeMap::from([(merged_range, "x".to_owned())]),
            expected_cursor: Some("ack-token-1".to_owned()),
            fence_delivery: true,
            fetch_request_id: Some(Uuid::from_u128(90_005)),
        }])
        .await
        .is_err());
    let reopened = control
        .execute_commands(vec![Command::OpenReaderSession {
            reader: "audit".to_owned(),
            capacity: 4,
        }])
        .await
        .unwrap();
    assert_eq!(reopened.results[0].data["session_epoch"], 4);
    assert_eq!(reopened.results[0].data["delivered_cursor"], "ack-token-1");
}
