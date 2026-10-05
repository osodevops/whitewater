use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use finnstream::{
    active_range::{
        AppendIdentity, MajorityAppendCoordinator, MajorityAppendErrorCode, OwnershipEpoch,
        RangeGeneration, RangePosition, ReplicaAppendAccepted, ReplicaAppendRequest,
        ReplicaAppendService, ReplicaCommitAccepted, ReplicaCommitRequest, ReplicaTransport,
        ReplicaTransportError, StorageNodeId,
    },
    codec::encode_record,
    control::{Command, ControlController},
    domain::StoredRecord,
    storage::{FileLogStore, LogStore},
};
use tempfile::TempDir;
use uuid::Uuid;

#[derive(Clone)]
struct DirectTransport {
    services: Arc<BTreeMap<StorageNodeId, Arc<ReplicaAppendService>>>,
    append_down: Arc<BTreeSet<StorageNodeId>>,
    commit_down: Arc<BTreeSet<StorageNodeId>>,
    corrupt_digest: Option<StorageNodeId>,
}

#[async_trait]
impl ReplicaTransport for DirectTransport {
    async fn append(
        &self,
        replica: &StorageNodeId,
        request: ReplicaAppendRequest,
    ) -> Result<ReplicaAppendAccepted, ReplicaTransportError> {
        if self.append_down.contains(replica) {
            return Err(ReplicaTransportError {
                message: "append unavailable".to_owned(),
                retryable: true,
            });
        }
        let mut accepted = self.services[replica]
            .append(request)
            .await
            .map_err(|error| ReplicaTransportError {
                message: error.message,
                retryable: error.retryable,
            })?;
        if self.corrupt_digest.as_ref() == Some(replica) {
            accepted.frame_digest[0] ^= 0xff;
        }
        Ok(accepted)
    }

    async fn commit(
        &self,
        replica: &StorageNodeId,
        request: ReplicaCommitRequest,
    ) -> Result<ReplicaCommitAccepted, ReplicaTransportError> {
        if self.commit_down.contains(replica) {
            return Err(ReplicaTransportError {
                message: "commit unavailable".to_owned(),
                retryable: true,
            });
        }
        self.services[replica]
            .commit(request)
            .await
            .map_err(|error| ReplicaTransportError {
                message: error.message,
                retryable: error.retryable,
            })
    }
}

struct Fixture {
    _catalog: TempDir,
    _replicas: TempDir,
    control: Arc<ControlController>,
    services: BTreeMap<StorageNodeId, Arc<ReplicaAppendService>>,
    request: ReplicaAppendRequest,
}

impl Fixture {
    async fn new() -> Self {
        let catalog = TempDir::new().unwrap();
        let replicas = TempDir::new().unwrap();
        let nodes = ["storage-1", "storage-2", "storage-3"]
            .map(|node| StorageNodeId::try_new(node).unwrap());
        let legacy: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(catalog.path().join("legacy")).unwrap());
        let control = Arc::new(
            ControlController::open_with_storage_nodes(
                catalog.path().join("catalog.json"),
                legacy,
                nodes.to_vec(),
            )
            .unwrap(),
        );
        let execution = control
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
        let feed_id = serde_json::from_value(execution.results[1].data["feed_id"].clone()).unwrap();
        let assignment = control.active_range_assignment(feed_id).await.unwrap();
        let owner = nodes[0].clone();
        let services = nodes
            .into_iter()
            .map(|node| {
                let service = Arc::new(ReplicaAppendService::new(
                    replicas.path().join(node.as_str()),
                    node.clone(),
                    control.clone(),
                ));
                (node, service)
            })
            .collect::<BTreeMap<_, _>>();
        let writer = Uuid::from_u128(700);
        let record = StoredRecord {
            message_id: Uuid::from_u128(701),
            producer_id: writer,
            producer_sequence: 1,
            event_time_ns: 1,
            ingest_time_ns: 2,
            key: b"customer-1".to_vec(),
            payload: b"created".to_vec(),
            metadata: BTreeMap::new(),
        };
        let request = ReplicaAppendRequest {
            feed_id,
            range_id: assignment.range_id,
            generation: RangeGeneration::new(1),
            ownership_epoch: OwnershipEpoch::new(1),
            append_owner: owner,
            expected_position: RangePosition::new(1),
            identity: AppendIdentity {
                writer_session_id: writer,
                writer_epoch: 1,
                sequence: 1,
            },
            cursor: "cursor-1".to_owned(),
            frame_base64: STANDARD.encode(encode_record(&record).unwrap()),
        };
        Self {
            _catalog: catalog,
            _replicas: replicas,
            control,
            services,
            request,
        }
    }

    fn coordinator(
        &self,
        append_down: &[&str],
        commit_down: &[&str],
        corrupt_digest: Option<&str>,
    ) -> MajorityAppendCoordinator {
        let transport = DirectTransport {
            services: Arc::new(self.services.clone()),
            append_down: Arc::new(
                append_down
                    .iter()
                    .map(|node| StorageNodeId::try_new(*node).unwrap())
                    .collect(),
            ),
            commit_down: Arc::new(
                commit_down
                    .iter()
                    .map(|node| StorageNodeId::try_new(*node).unwrap())
                    .collect(),
            ),
            corrupt_digest: corrupt_digest.map(|node| StorageNodeId::try_new(node).unwrap()),
        };
        MajorityAppendCoordinator::new(
            self.services[&StorageNodeId::try_new("storage-1").unwrap()].clone(),
            self.control.clone(),
            Arc::new(transport),
        )
    }
}

#[tokio::test]
async fn three_healthy_replicas_commit_with_three_matching_evidence_records() {
    let fixture = Fixture::new().await;
    let result = fixture
        .coordinator(&[], &[], None)
        .append(fixture.request.clone())
        .await
        .unwrap();
    assert_eq!(result.durable_replicas.len(), 3);
    assert_eq!(result.commit_evidence.len(), 3);
}

#[tokio::test]
async fn either_follower_can_form_a_two_of_three_majority_with_the_owner() {
    for unavailable in ["storage-2", "storage-3"] {
        let fixture = Fixture::new().await;
        let result = fixture
            .coordinator(&[unavailable], &[], None)
            .append(fixture.request.clone())
            .await
            .unwrap();
        assert_eq!(result.durable_replicas.len(), 2);
        assert_eq!(result.commit_evidence.len(), 2);
        assert!(!result
            .durable_replicas
            .contains(&StorageNodeId::try_new(unavailable).unwrap()));
    }
}

#[tokio::test]
async fn follower_move_freeze_drains_majority_append_and_rejects_new_writes() {
    let fixture = Fixture::new().await;
    let coordinator = fixture.coordinator(&[], &[], None);
    coordinator.append(fixture.request.clone()).await.unwrap();
    let assignment = fixture
        .control
        .active_range_assignment(fixture.request.feed_id)
        .await
        .unwrap();
    let committed = coordinator
        .freeze_for_follower_move(&assignment)
        .await
        .unwrap();
    assert_eq!(committed.value(), 1);
    let error = coordinator
        .append(fixture.request.clone())
        .await
        .unwrap_err();
    assert_eq!(error.code, MajorityAppendErrorCode::LocalStorageFailure);
    assert!(error.retryable);
    fixture.services[&assignment.owner]
        .unfreeze_generation(assignment.range_id, assignment.generation)
        .await;
}

#[tokio::test]
async fn one_healthy_replica_never_returns_success() {
    let fixture = Fixture::new().await;
    let error = fixture
        .coordinator(&["storage-2", "storage-3"], &[], None)
        .append(fixture.request.clone())
        .await
        .unwrap_err();
    assert_eq!(error.code, MajorityAppendErrorCode::NoDurableMajority);
    assert_eq!(error.durable_replicas.len(), 1);
}

#[tokio::test]
async fn frame_majority_without_commit_majority_returns_retryable_ambiguous_failure() {
    let fixture = Fixture::new().await;
    let error = fixture
        .coordinator(&[], &["storage-2", "storage-3"], None)
        .append(fixture.request.clone())
        .await
        .unwrap_err();
    assert_eq!(error.code, MajorityAppendErrorCode::NoCommitMajority);
    assert!(error.retryable);
    assert_eq!(error.commit_evidence.len(), 0);
}

#[tokio::test]
async fn conflicting_follower_digest_is_rejected() {
    let fixture = Fixture::new().await;
    let error = fixture
        .coordinator(&[], &[], Some("storage-2"))
        .append(fixture.request.clone())
        .await
        .unwrap_err();
    assert_eq!(error.code, MajorityAppendErrorCode::FrameConflict);
}

#[tokio::test]
async fn retry_after_commit_returns_the_original_logical_result() {
    let fixture = Fixture::new().await;
    let coordinator = fixture.coordinator(&[], &[], None);
    let first = coordinator.append(fixture.request.clone()).await.unwrap();
    let retry = coordinator.append(fixture.request.clone()).await.unwrap();
    assert_eq!(retry.message_id, first.message_id);
    assert_eq!(retry.cursor, first.cursor);
    assert_eq!(retry.position, first.position);
    assert!(retry.deduplicated);
}
