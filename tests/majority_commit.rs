use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use finnstream::{
    active_range::{
        repair_replica, AppendIdentity, CommitPosition, MajorityAppendCoordinator,
        MajorityAppendErrorCode, OwnerMoveControl, OwnerMoveError, OwnerMoveEvidence,
        OwnerMoveExecutor, OwnershipEpoch, RangeGeneration, RangePosition, ReplicaAppendAccepted,
        ReplicaAppendErrorCode, ReplicaAppendRequest, ReplicaAppendService, ReplicaCommitAccepted,
        ReplicaCommitRequest, ReplicaTransport, ReplicaTransportError, StorageNodeId,
    },
    codec::encode_record,
    control::{Command, ControlController, RangeOwnerMovePlan},
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

struct LocalOwnerMoveControl(Arc<ControlController>);

#[async_trait]
impl OwnerMoveControl for LocalOwnerMoveControl {
    async fn record_ready(
        &self,
        feed: &str,
        plan: &RangeOwnerMovePlan,
        evidence: &OwnerMoveEvidence,
    ) -> Result<(), String> {
        self.0
            .execute_commands(vec![Command::RecordOwnerMoveCatchUp {
                feed: feed.to_owned(),
                plan_id: plan.plan_id,
                source_commit: evidence.source_commit,
                target_commit: evidence.target_commit,
                checksum_verified: evidence.ready,
            }])
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    async fn activate(&self, feed: &str, plan: &RangeOwnerMovePlan) -> Result<(), String> {
        self.0
            .execute_commands(vec![Command::ActivateOwnerMove {
                feed: feed.to_owned(),
                plan_id: plan.plan_id,
            }])
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

struct UncertainOwnerMoveControl;

#[async_trait]
impl OwnerMoveControl for UncertainOwnerMoveControl {
    async fn record_ready(
        &self,
        _feed: &str,
        _plan: &RangeOwnerMovePlan,
        _evidence: &OwnerMoveEvidence,
    ) -> Result<(), String> {
        Err("readiness response was lost".to_owned())
    }

    async fn activate(&self, _feed: &str, _plan: &RangeOwnerMovePlan) -> Result<(), String> {
        Err("activation must not run without readiness".to_owned())
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
async fn follower_move_freeze_discards_only_the_uncommitted_owner_tail() {
    let fixture = Fixture::new().await;
    let owner = fixture.coordinator(&["storage-2", "storage-3"], &[], None);
    assert_eq!(
        owner
            .append(fixture.request.clone())
            .await
            .unwrap_err()
            .code,
        MajorityAppendErrorCode::NoDurableMajority
    );
    let assignment = fixture
        .control
        .active_range_assignment(fixture.request.feed_id)
        .await
        .unwrap();
    assert_eq!(
        owner
            .next_position(assignment.feed_id, assignment.range_id)
            .await
            .unwrap()
            .value(),
        2
    );
    assert_eq!(
        owner
            .freeze_for_follower_move(&assignment)
            .await
            .unwrap()
            .value(),
        0
    );
    assert_eq!(
        owner
            .next_position(assignment.feed_id, assignment.range_id)
            .await
            .unwrap()
            .value(),
        1
    );
    fixture.services[&assignment.owner]
        .unfreeze_generation(assignment.range_id, assignment.generation)
        .await;
}

#[tokio::test]
async fn owner_move_fences_old_owner_only_after_verified_catch_up() {
    let fixture = Fixture::new().await;
    let original = fixture
        .control
        .active_range_assignment(fixture.request.feed_id)
        .await
        .unwrap();
    let source = fixture.services[&original.owner].clone();
    let target = fixture.services[&StorageNodeId::try_new("storage-2").unwrap()].clone();
    let owner = Arc::new(fixture.coordinator(&[], &["storage-2"], None));
    owner.append(fixture.request.clone()).await.unwrap();
    let prepared = fixture
        .control
        .execute_commands(vec![Command::PrepareOwnerMove {
            feed: "orders.events".to_owned(),
            range_id: original.range_id,
            new_owner: target.local_node().clone(),
        }])
        .await
        .unwrap();
    let plan: RangeOwnerMovePlan =
        serde_json::from_value(prepared.results[0].data.clone()).unwrap();
    let executor = OwnerMoveExecutor::new(
        fixture.control.clone(),
        owner.clone(),
        source.clone(),
        target.clone(),
    );
    let mut forged = plan.clone();
    forged.candidate_assignment.ownership_epoch = OwnershipEpoch::new(99);
    assert!(matches!(
        executor
            .finalize(
                &LocalOwnerMoveControl(fixture.control.clone()),
                "orders.events",
                &forged
            )
            .await,
        Err(OwnerMoveError::PlanMismatch)
    ));
    assert!(
        !source
            .generation_is_frozen(original.range_id, original.generation)
            .await
    );
    assert!(matches!(
        executor
            .finalize(
                &LocalOwnerMoveControl(fixture.control.clone()),
                "orders.events",
                &plan
            )
            .await,
        Err(OwnerMoveError::CatchingUp)
    ));
    assert!(
        !source
            .generation_is_frozen(original.range_id, original.generation)
            .await
    );
    assert!(
        !target
            .generation_is_frozen(original.range_id, original.generation)
            .await
    );
    assert_eq!(
        fixture
            .control
            .active_range_assignment(original.feed_id)
            .await
            .unwrap(),
        original
    );
    repair_replica(&original, source.clone(), target.clone(), 1)
        .await
        .unwrap();
    assert!(matches!(
        executor
            .finalize(&UncertainOwnerMoveControl, "orders.events", &plan)
            .await,
        Err(OwnerMoveError::Readiness(_))
    ));
    assert!(
        source
            .generation_is_frozen(original.range_id, original.generation)
            .await
    );
    assert!(
        target
            .generation_is_frozen(original.range_id, original.generation)
            .await
    );
    assert_eq!(
        fixture
            .control
            .active_range_assignment(original.feed_id)
            .await
            .unwrap(),
        original
    );
    source
        .unfreeze_generation(original.range_id, original.generation)
        .await;
    target
        .unfreeze_generation(original.range_id, original.generation)
        .await;
    let moved = executor
        .finalize(
            &LocalOwnerMoveControl(fixture.control.clone()),
            "orders.events",
            &plan,
        )
        .await
        .unwrap();
    assert!(moved.ready);
    assert_eq!(moved.target_commit, CommitPosition::new(1));
    let current = fixture
        .control
        .active_range_assignment(original.feed_id)
        .await
        .unwrap();
    assert_eq!(current.owner, *target.local_node());
    assert_eq!(current.ownership_epoch.value(), 2);
    assert_eq!(current.replicas, original.replicas);
    assert!(
        !source
            .generation_is_frozen(original.range_id, original.generation)
            .await
    );
    assert!(
        !target
            .generation_is_frozen(original.range_id, original.generation)
            .await
    );
    assert_eq!(
        owner
            .append(fixture.request.clone())
            .await
            .unwrap_err()
            .code,
        MajorityAppendErrorCode::NotCurrentOwner
    );
    let record = StoredRecord {
        message_id: Uuid::from_u128(702),
        producer_id: fixture.request.identity.writer_session_id,
        producer_sequence: 2,
        event_time_ns: 2,
        ingest_time_ns: 3,
        key: b"customer-1".to_vec(),
        payload: b"updated".to_vec(),
        metadata: BTreeMap::new(),
    };
    let mut request = fixture.request.clone();
    request.ownership_epoch = current.ownership_epoch;
    request.append_owner = current.owner.clone();
    request.expected_position = RangePosition::new(2);
    request.identity.sequence = 2;
    request.cursor = "cursor-2".to_owned();
    request.frame_base64 = STANDARD.encode(encode_record(&record).unwrap());
    let transport = DirectTransport {
        services: Arc::new(fixture.services.clone()),
        append_down: Arc::new(BTreeSet::new()),
        commit_down: Arc::new(BTreeSet::new()),
        corrupt_digest: None,
    };
    let new_owner = MajorityAppendCoordinator::new(
        target.clone(),
        fixture.control.clone(),
        Arc::new(transport),
    );
    assert_eq!(new_owner.append(request).await.unwrap().position.value(), 2);
    let frames = target
        .read_committed(original.feed_id, None, 10)
        .await
        .unwrap();
    assert_eq!(
        frames
            .iter()
            .map(|frame| frame.cursor.as_str())
            .collect::<Vec<_>>(),
        vec!["cursor-1", "cursor-2"]
    );
}

#[tokio::test]
async fn owner_move_rejects_a_caught_up_follower_with_different_bytes() {
    let fixture = Fixture::new().await;
    let original = fixture
        .control
        .active_range_assignment(fixture.request.feed_id)
        .await
        .unwrap();
    let source = fixture.services[&original.owner].clone();
    let target = fixture.services[&StorageNodeId::try_new("storage-2").unwrap()].clone();
    let owner = Arc::new(fixture.coordinator(&[], &[], None));
    owner.append(fixture.request.clone()).await.unwrap();
    target
        .quarantine_for_repair(original.feed_id)
        .await
        .unwrap();
    let mut conflicting = fixture.request.clone();
    let record = StoredRecord {
        message_id: Uuid::from_u128(701),
        producer_id: conflicting.identity.writer_session_id,
        producer_sequence: 1,
        event_time_ns: 1,
        ingest_time_ns: 2,
        key: b"customer-1".to_vec(),
        payload: b"different".to_vec(),
        metadata: BTreeMap::new(),
    };
    conflicting.frame_base64 = STANDARD.encode(encode_record(&record).unwrap());
    let accepted = target.append(conflicting).await.unwrap();
    target
        .commit(ReplicaCommitRequest {
            feed_id: original.feed_id,
            range_id: original.range_id,
            generation: original.generation,
            ownership_epoch: original.ownership_epoch,
            append_owner: original.owner.clone(),
            commit_position: CommitPosition::new(1),
            frame_digest: accepted.frame_digest,
        })
        .await
        .unwrap();
    let prepared = fixture
        .control
        .execute_commands(vec![Command::PrepareOwnerMove {
            feed: "orders.events".to_owned(),
            range_id: original.range_id,
            new_owner: target.local_node().clone(),
        }])
        .await
        .unwrap();
    let plan: RangeOwnerMovePlan =
        serde_json::from_value(prepared.results[0].data.clone()).unwrap();
    let executor = OwnerMoveExecutor::new(
        fixture.control.clone(),
        owner,
        source.clone(),
        target.clone(),
    );
    assert!(matches!(
        executor
            .finalize(
                &LocalOwnerMoveControl(fixture.control.clone()),
                "orders.events",
                &plan
            )
            .await,
        Err(OwnerMoveError::VerificationFailed)
    ));
    assert_eq!(
        fixture
            .control
            .active_range_assignment(original.feed_id)
            .await
            .unwrap(),
        original
    );
    assert!(
        !source
            .generation_is_frozen(original.range_id, original.generation)
            .await
    );
    assert!(
        !target
            .generation_is_frozen(original.range_id, original.generation)
            .await
    );
}

#[tokio::test]
async fn committed_range_pages_require_the_current_owner_and_preserve_the_boundary() {
    let fixture = Fixture::new().await;
    let assignment = fixture
        .control
        .active_range_assignment(fixture.request.feed_id)
        .await
        .unwrap();
    let owner = fixture.services[&assignment.owner].clone();
    let follower = fixture.services[&StorageNodeId::try_new("storage-2").unwrap()].clone();
    let (empty_boundary, empty) = owner
        .read_owned_range_page(&assignment, None, None)
        .await
        .unwrap();
    assert_eq!(empty_boundary, CommitPosition::new(0));
    assert!(empty.is_empty());
    fixture
        .coordinator(&[], &[], None)
        .append(fixture.request.clone())
        .await
        .unwrap();
    let (boundary, page) = owner
        .read_owned_range_page(&assignment, None, None)
        .await
        .unwrap();
    assert_eq!(boundary, CommitPosition::new(1));
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].cursor, fixture.request.cursor);
    let (_, exhausted) = owner
        .read_owned_range_page(&assignment, Some(RangePosition::new(1)), Some(boundary))
        .await
        .unwrap();
    assert!(exhausted.is_empty());
    let (_, resolved, suffix) = owner
        .read_owned_range_cursor_page(
            &assignment,
            Some(&fixture.request.cursor),
            None,
            None,
            None,
            32,
        )
        .await
        .unwrap();
    assert_eq!(resolved, Some(RangePosition::new(1)));
    assert!(suffix.is_empty());
    let (_, start, tail) = owner
        .read_owned_range_cursor_page(&assignment, None, None, None, Some(1), 32)
        .await
        .unwrap();
    assert_eq!(start, Some(RangePosition::new(0)));
    assert_eq!(tail[0].cursor, fixture.request.cursor);
    assert_eq!(
        owner
            .read_owned_range_cursor_page(&assignment, Some("unknown"), None, None, None, 32)
            .await
            .unwrap_err()
            .code,
        ReplicaAppendErrorCode::PositionConflict
    );
    assert_eq!(
        follower
            .read_owned_range_page(&assignment, None, None)
            .await
            .unwrap_err()
            .code,
        ReplicaAppendErrorCode::NotCurrentOwner
    );
}

#[tokio::test]
async fn cursor_pages_continue_after_a_bounded_batch_without_rescanning_the_prefix() {
    let fixture = Fixture::new().await;
    let assignment = fixture
        .control
        .active_range_assignment(fixture.request.feed_id)
        .await
        .unwrap();
    let owner = fixture.services[&assignment.owner].clone();
    let coordinator = fixture.coordinator(&[], &[], None);
    coordinator.append(fixture.request.clone()).await.unwrap();
    for sequence in 2..=34_u64 {
        let mut request = fixture.request.clone();
        request.expected_position = RangePosition::new(sequence);
        request.identity.sequence = sequence;
        request.cursor = format!("cursor-{sequence}");
        request.frame_base64 = STANDARD.encode(
            encode_record(&StoredRecord {
                message_id: Uuid::from_u128(700 + sequence as u128),
                producer_id: fixture.request.identity.writer_session_id,
                producer_sequence: sequence,
                event_time_ns: sequence as i64,
                ingest_time_ns: sequence as i64,
                key: b"customer-1".to_vec(),
                payload: Vec::new(),
                metadata: BTreeMap::new(),
            })
            .unwrap(),
        );
        coordinator.append(request).await.unwrap();
    }
    let (commit, position, page) = owner
        .read_owned_range_cursor_page(&assignment, Some("cursor-1"), None, None, None, 32)
        .await
        .unwrap();
    assert_eq!(commit, CommitPosition::new(34));
    assert_eq!(position, Some(RangePosition::new(1)));
    assert_eq!(page.len(), 32);
    assert_eq!(page.first().unwrap().cursor, "cursor-2");
    assert_eq!(page.last().unwrap().cursor, "cursor-33");
    let (_, _, next) = owner
        .read_owned_range_cursor_page(
            &assignment,
            None,
            Some(RangePosition::new(33)),
            Some(commit),
            None,
            32,
        )
        .await
        .unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].cursor, "cursor-34");
    let (_, tail_start, tail) = owner
        .read_owned_range_cursor_page(&assignment, None, None, None, Some(2), 32)
        .await
        .unwrap();
    assert_eq!(tail_start, Some(RangePosition::new(32)));
    assert_eq!(
        tail.iter()
            .map(|frame| frame.cursor.as_str())
            .collect::<Vec<_>>(),
        vec!["cursor-33", "cursor-34"]
    );
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
