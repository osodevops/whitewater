use std::sync::Arc;

use finnstream::{
    active_range::{
        plan_owner_recovery, CommitPosition, OwnerRecoveryError, RangePosition,
        ReplicaRecoveryStatus, StorageNodeId,
    },
    control::{Command, ControlController, ControlError},
    storage::{FileLogStore, LogStore},
};
use tempfile::TempDir;

fn node(value: &str) -> StorageNodeId {
    StorageNodeId::try_new(value).unwrap()
}

async fn fixture() -> (
    TempDir,
    ControlController,
    finnstream::active_range::ActiveRangeAssignment,
) {
    let directory = TempDir::new().unwrap();
    let store: Arc<dyn LogStore> =
        Arc::new(FileLogStore::open(directory.path().join("legacy")).unwrap());
    let controller = ControlController::open_with_storage_nodes(
        directory.path().join("catalog.json"),
        store,
        vec![node("storage-1"), node("storage-2"), node("storage-3")],
    )
    .unwrap();
    let result = controller
        .execute("CREATE SPACE orders; CREATE FEED orders.events;")
        .await
        .unwrap();
    let feed_id = serde_json::from_value(result.results[1].data["feed_id"].clone()).unwrap();
    let assignment = controller.active_range_assignment(feed_id).await.unwrap();
    (directory, controller, assignment)
}

#[tokio::test]
async fn failed_owner_recovery_selects_a_caught_up_replica_and_higher_epoch() {
    let (_directory, _controller, assignment) = fixture().await;
    let plan = plan_owner_recovery(
        &assignment,
        &[
            ReplicaRecoveryStatus {
                node: node("storage-1"),
                healthy: false,
                appended: RangePosition::new(5),
                committed: CommitPosition::new(4),
            },
            ReplicaRecoveryStatus {
                node: node("storage-2"),
                healthy: true,
                appended: RangePosition::new(5),
                committed: CommitPosition::new(4),
            },
            ReplicaRecoveryStatus {
                node: node("storage-3"),
                healthy: true,
                appended: RangePosition::new(4),
                committed: CommitPosition::new(4),
            },
        ],
    )
    .unwrap();
    assert_eq!(plan.previous_owner, node("storage-1"));
    assert_eq!(plan.new_owner, node("storage-2"));
    assert_eq!(plan.new_epoch.value(), 2);
    assert_eq!(plan.committed_prefix.value(), 4);
}

#[tokio::test]
async fn recovery_refuses_a_healthy_owner_or_loss_of_replica_majority() {
    let (_directory, _controller, assignment) = fixture().await;
    let healthy_owner = ReplicaRecoveryStatus {
        node: node("storage-1"),
        healthy: true,
        appended: RangePosition::new(1),
        committed: CommitPosition::new(1),
    };
    assert_eq!(
        plan_owner_recovery(&assignment, std::slice::from_ref(&healthy_owner)),
        Err(OwnerRecoveryError::OwnerStillHealthy)
    );
    let failed_owner = ReplicaRecoveryStatus {
        healthy: false,
        ..healthy_owner
    };
    assert_eq!(
        plan_owner_recovery(
            &assignment,
            &[
                failed_owner,
                ReplicaRecoveryStatus {
                    node: node("storage-2"),
                    healthy: true,
                    appended: RangePosition::new(1),
                    committed: CommitPosition::new(1),
                },
            ],
        ),
        Err(OwnerRecoveryError::NoHealthyMajority)
    );
}

#[tokio::test]
async fn consensus_recovery_command_is_compare_and_set_and_fences_old_owner() {
    let (_directory, controller, assignment) = fixture().await;
    controller
        .execute_commands(vec![Command::RecoverActiveRangeOwnership {
            feed: "orders.events".to_owned(),
            expected_owner: assignment.owner.clone(),
            expected_epoch: assignment.ownership_epoch,
            new_owner: node("storage-2"),
        }])
        .await
        .unwrap();
    let recovered = controller
        .active_range_assignment(assignment.feed_id)
        .await
        .unwrap();
    assert_eq!(recovered.owner, node("storage-2"));
    assert_eq!(recovered.ownership_epoch.value(), 2);
    assert!(recovered
        .validate_request(
            assignment.generation,
            assignment.ownership_epoch,
            &assignment.owner,
        )
        .is_err());
    let stale = controller
        .execute_commands(vec![Command::RecoverActiveRangeOwnership {
            feed: "orders.events".to_owned(),
            expected_owner: assignment.owner,
            expected_epoch: assignment.ownership_epoch,
            new_owner: node("storage-3"),
        }])
        .await;
    assert!(matches!(stale, Err(ControlError::InvalidOperation(_))));
}
