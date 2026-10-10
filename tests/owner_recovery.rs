use std::{collections::BTreeMap, sync::Arc};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use finnstream::{
    active_range::{
        plan_owner_recovery, AppendIdentity, CommitPosition, OwnerRecoveryError, RangeGeneration,
        RangePosition, ReplicaAppendRequest, ReplicaAppendService, ReplicaCommitRequest,
        ReplicaRecoveryStatus, StorageNodeId,
    },
    codec::encode_record,
    control::{Command, ControlController, ControlError},
    domain::StoredRecord,
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
    let owner = assignment.owner.clone();
    let followers = assignment
        .replicas
        .iter()
        .filter(|node| **node != owner)
        .cloned()
        .collect::<Vec<_>>();
    let caught_up = followers[0].clone();
    let lagging = followers[1].clone();
    let plan = plan_owner_recovery(
        &assignment,
        &[
            ReplicaRecoveryStatus {
                node: owner.clone(),
                healthy: false,
                appended: RangePosition::new(5),
                committed: CommitPosition::new(4),
            },
            ReplicaRecoveryStatus {
                node: caught_up.clone(),
                healthy: true,
                appended: RangePosition::new(5),
                committed: CommitPosition::new(4),
            },
            ReplicaRecoveryStatus {
                node: lagging,
                healthy: true,
                appended: RangePosition::new(4),
                committed: CommitPosition::new(4),
            },
        ],
    )
    .unwrap();
    assert_eq!(plan.previous_owner, owner);
    assert_eq!(plan.new_owner, caught_up);
    assert_eq!(
        plan.new_epoch.value(),
        assignment.ownership_epoch.value() + 1
    );
    assert_eq!(plan.committed_prefix.value(), 4);
}

#[tokio::test]
async fn recovery_refuses_a_healthy_owner_or_loss_of_replica_majority() {
    let (_directory, _controller, assignment) = fixture().await;
    let healthy_owner = ReplicaRecoveryStatus {
        node: assignment.owner.clone(),
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
                    node: assignment
                        .replicas
                        .iter()
                        .find(|node| **node != assignment.owner)
                        .unwrap()
                        .clone(),
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
            new_owner: assignment
                .replicas
                .iter()
                .find(|node| **node != assignment.owner)
                .unwrap()
                .clone(),
        }])
        .await
        .unwrap();
    let recovered = controller
        .active_range_assignment(assignment.feed_id)
        .await
        .unwrap();
    assert_eq!(
        recovered.owner,
        *assignment
            .replicas
            .iter()
            .find(|node| **node != assignment.owner)
            .unwrap()
    );
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
            new_owner: node("storage-4"),
        }])
        .await;
    assert!(matches!(stale, Err(ControlError::InvalidOperation(_))));
}

#[tokio::test]
async fn recovered_replicas_advance_epoch_preserve_commit_and_truncate_tail() {
    let (directory, controller, assignment) = fixture().await;
    let controller = Arc::new(controller);
    let services = ["storage-1", "storage-2", "storage-3"]
        .into_iter()
        .map(|value| {
            let node = node(value);
            let service = Arc::new(ReplicaAppendService::new(
                directory.path().join(value),
                node.clone(),
                controller.clone(),
            ));
            (node, service)
        })
        .collect::<BTreeMap<_, _>>();
    let writer = uuid::Uuid::from_u128(900);
    let request = |position: u64, sequence: u64| {
        let record = StoredRecord {
            message_id: uuid::Uuid::from_u128(1_000 + sequence as u128),
            producer_id: writer,
            producer_sequence: sequence,
            event_time_ns: sequence as i64,
            ingest_time_ns: sequence as i64,
            key: b"account-1".to_vec(),
            payload: format!("event-{sequence}").into_bytes(),
            metadata: BTreeMap::new(),
        };
        ReplicaAppendRequest {
            feed_id: assignment.feed_id,
            range_id: assignment.range_id,
            generation: RangeGeneration::new(1),
            ownership_epoch: assignment.ownership_epoch,
            append_owner: assignment.owner.clone(),
            expected_position: RangePosition::new(position),
            identity: AppendIdentity {
                writer_session_id: writer,
                writer_epoch: 1,
                sequence,
            },
            cursor: format!("cursor-{sequence}"),
            frame_base64: STANDARD.encode(encode_record(&record).unwrap()),
        }
    };
    let first = request(1, 1);
    let mut digest = [0; 32];
    for service in services.values() {
        digest = service.append(first.clone()).await.unwrap().frame_digest;
    }
    let followers = assignment
        .replicas
        .iter()
        .filter(|node| **node != assignment.owner)
        .cloned()
        .collect::<Vec<_>>();
    for service in [
        services[&followers[0]].clone(),
        services[&followers[1]].clone(),
    ] {
        service
            .commit(ReplicaCommitRequest {
                feed_id: assignment.feed_id,
                range_id: assignment.range_id,
                generation: assignment.generation,
                ownership_epoch: assignment.ownership_epoch,
                append_owner: assignment.owner.clone(),
                commit_position: CommitPosition::new(1),
                frame_digest: digest,
            })
            .await
            .unwrap();
        service.append(request(2, 2)).await.unwrap();
    }
    controller
        .execute_commands(vec![Command::RecoverActiveRangeOwnership {
            feed: "orders.events".to_owned(),
            expected_owner: assignment.owner.clone(),
            expected_epoch: assignment.ownership_epoch,
            new_owner: followers[0].clone(),
        }])
        .await
        .unwrap();
    for service in [
        services[&followers[0]].clone(),
        services[&followers[1]].clone(),
    ] {
        assert_eq!(
            service
                .reconcile_recovery(assignment.feed_id, CommitPosition::new(1))
                .await
                .unwrap(),
            1
        );
        let status = service.recovery_status(assignment.feed_id).await.unwrap();
        assert_eq!(status.appended, RangePosition::new(1));
        assert_eq!(status.committed, CommitPosition::new(1));
    }
    assert!(services[&followers[0]].append(request(2, 2)).await.is_err());
}
