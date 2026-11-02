use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use tempfile::TempDir;
use uuid::Uuid;
use whitewater::{
    active_range::{
        repair_replica, AppendIdentity, CommitPosition, MajorityAppendCoordinator,
        MajorityAppendErrorCode, OwnerMoveControl, OwnerMoveError, OwnerMoveEvidence,
        OwnerMoveExecutor, OwnershipEpoch, RangeGeneration, RangePosition, ReplicaAppendAccepted,
        ReplicaAppendErrorCode, ReplicaAppendRequest, ReplicaAppendService, ReplicaCommitAccepted,
        ReplicaCommitRequest, ReplicaDiscardPendingRequest, ReplicaDiscardPendingResponse,
        ReplicaTransport, ReplicaTransportError, StorageNodeId,
    },
    codec::{decode_record, encode_record},
    control::{Command, ControlController, RangeOwnerMovePlan},
    domain::StoredRecord,
    storage::{FileLogStore, LogStore},
};

#[derive(Clone)]
struct DirectTransport {
    services: Arc<BTreeMap<StorageNodeId, Arc<ReplicaAppendService>>>,
    append_down: Arc<BTreeSet<StorageNodeId>>,
    commit_down: Arc<BTreeSet<StorageNodeId>>,
    corrupt_digest: Option<StorageNodeId>,
    commit_pause: Option<CommitPause>,
}

#[derive(Clone)]
struct CommitPause {
    node: StorageNodeId,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl ReplicaTransport for DirectTransport {
    async fn append(
        &self,
        replica: &StorageNodeId,
        request: ReplicaAppendRequest,
    ) -> Result<ReplicaAppendAccepted, ReplicaTransportError> {
        if self.append_down.contains(replica) {
            return Err(ReplicaTransportError::transport("append unavailable", true));
        }
        let mut accepted = self.services[replica]
            .append(request)
            .await
            .map_err(ReplicaTransportError::replica)?;
        if self.corrupt_digest.as_ref() == Some(replica) {
            accepted.frame_digest[0] ^= 0xff;
        }
        Ok(accepted)
    }

    async fn append_batch(
        &self,
        replica: &StorageNodeId,
        request: whitewater::active_range::ReplicaBatchAppendRequest,
    ) -> Result<Vec<ReplicaAppendAccepted>, ReplicaTransportError> {
        if self.append_down.contains(replica) {
            return Err(ReplicaTransportError::transport("append unavailable", true));
        }
        let mut accepted = self.services[replica]
            .import_committed_batch(request)
            .await
            .map_err(ReplicaTransportError::replica)?;
        if self.corrupt_digest.as_ref() == Some(replica) {
            accepted[0].frame_digest[0] ^= 0xff;
        }
        Ok(accepted)
    }

    async fn commit(
        &self,
        replica: &StorageNodeId,
        request: ReplicaCommitRequest,
    ) -> Result<ReplicaCommitAccepted, ReplicaTransportError> {
        if self.commit_down.contains(replica) {
            return Err(ReplicaTransportError::transport("commit unavailable", true));
        }
        if let Some(pause) = self
            .commit_pause
            .as_ref()
            .filter(|pause| &pause.node == replica)
        {
            pause.entered.notify_one();
            pause.release.notified().await;
        }
        self.services[replica]
            .commit(request)
            .await
            .map_err(ReplicaTransportError::replica)
    }

    async fn discard_pending(
        &self,
        replica: &StorageNodeId,
        request: ReplicaDiscardPendingRequest,
    ) -> Result<ReplicaDiscardPendingResponse, ReplicaTransportError> {
        self.services[replica]
            .discard_pending(request)
            .await
            .map_err(ReplicaTransportError::replica)
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
    owner: StorageNodeId,
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
            .execute_commands_with_request_id(
                vec![
                    Command::CreateSpace {
                        name: "orders".to_owned(),
                    },
                    Command::CreateFeed {
                        name: "orders.events".to_owned(),
                        encrypted: false,
                    },
                ],
                Uuid::from_u128(600),
            )
            .await
            .unwrap();
        let feed_id = serde_json::from_value(execution.results[1].data["feed_id"].clone()).unwrap();
        let assignment = control.active_range_assignment(feed_id).await.unwrap();
        let owner = assignment.owner.clone();
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
            append_owner: owner.clone(),
            expected_position: RangePosition::new(1),
            identity: AppendIdentity {
                writer_session_id: writer,
                writer_epoch: 1,
                sequence: 1,
            },
            cursor: "cursor-1".to_owned(),
            frame: encode_record(&record).unwrap().into(),
            pending_effect: None,
        };
        Self {
            _catalog: catalog,
            _replicas: replicas,
            control,
            services,
            request,
            owner,
        }
    }

    fn non_owner_nodes(&self) -> Vec<StorageNodeId> {
        self.services
            .keys()
            .filter(|node| **node != self.owner)
            .cloned()
            .collect()
    }

    fn non_owner_names(&self) -> Vec<String> {
        self.non_owner_nodes()
            .iter()
            .map(|node| node.as_str().to_owned())
            .collect()
    }

    fn coordinator(
        &self,
        append_down: &[&str],
        commit_down: &[&str],
        corrupt_digest: Option<&str>,
    ) -> MajorityAppendCoordinator {
        self.coordinator_with_pause(append_down, commit_down, corrupt_digest, None)
    }

    fn coordinator_with_pause(
        &self,
        append_down: &[&str],
        commit_down: &[&str],
        corrupt_digest: Option<&str>,
        commit_pause: Option<CommitPause>,
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
            commit_pause,
        };
        MajorityAppendCoordinator::new(
            self.services[&self.owner].clone(),
            self.control.clone(),
            Arc::new(transport),
        )
    }
}

#[tokio::test]
async fn stale_epoch_stamp_on_the_current_owner_is_re_stamped_not_fatal() {
    let fixture = Fixture::new().await;
    let followers = fixture.non_owner_nodes();
    // Ownership CASes epoch 1 -> 2 onto a different replica; every store
    // adopts the newer epoch on its next fencing check.
    fixture
        .control
        .execute_commands(vec![Command::RecoverActiveRangeOwnership {
            feed: "orders.events".to_owned(),
            expected_owner: fixture.owner.clone(),
            expected_epoch: fixture.request.ownership_epoch,
            new_owner: followers[0].clone(),
        }])
        .await
        .unwrap();

    let transport = DirectTransport {
        services: Arc::new(fixture.services.clone()),
        append_down: Arc::new(BTreeSet::new()),
        commit_down: Arc::new(BTreeSet::new()),
        corrupt_digest: None,
        commit_pause: None,
    };
    let coordinator = MajorityAppendCoordinator::new(
        fixture.services[&followers[0]].clone(),
        fixture.control.clone(),
        Arc::new(transport),
    );

    // The caller routed to the right (new) owner but still carries the
    // pre-cutover epoch — catalog propagation lag or a stamp pinned at
    // Writer-session open. Refusing fatally would wedge the session on
    // routing residue; the current owner must re-stamp the authoritative
    // epoch and land the frame.
    let mut request = fixture.request.clone();
    request.append_owner = followers[0].clone();
    assert_eq!(request.ownership_epoch, OwnershipEpoch::new(1));
    let result = coordinator.append(request).await.unwrap();
    assert_eq!(result.position, RangePosition::new(1));
}

#[tokio::test]
async fn divergent_committed_replica_vetoes_instead_of_forging_divergence() {
    let fixture = Fixture::new().await;
    let followers = fixture.non_owner_nodes();

    // Stage a divergent committed occupant at position 1 on one follower:
    // same Writer identity, different record bytes — the history a
    // superseded owner could have committed before losing ownership.
    let mut divergent = fixture.request.clone();
    divergent.cursor = "cursor-divergent".to_owned();
    let mut divergent_record = decode_record(&fixture.request.frame).unwrap();
    divergent_record.message_id = Uuid::from_u128(702);
    divergent_record.payload = b"diverged".to_vec();
    divergent.frame = encode_record(&divergent_record).unwrap().into();
    let landed = fixture.services[&followers[0]]
        .append(divergent)
        .await
        .unwrap();
    fixture.services[&followers[0]]
        .commit(ReplicaCommitRequest {
            feed_id: fixture.request.feed_id,
            range_id: fixture.request.range_id,
            generation: fixture.request.generation,
            ownership_epoch: fixture.request.ownership_epoch,
            append_owner: fixture.owner.clone(),
            commit_position: CommitPosition::new(1),
            frame_digest: landed.frame_digest,
        })
        .await
        .unwrap();

    // The owner's durable pair used to out-vote the divergent replica and
    // commit a second frame at position 1 — forging committed divergence no
    // recovery can reconcile. The fork evidence must veto the append as a
    // retryable refusal so the Writer rides out the ownership cutover.
    let error = fixture
        .coordinator(&[], &[], None)
        .append(fixture.request.clone())
        .await
        .unwrap_err();
    assert!(error.retryable);
    assert_eq!(error.code, MajorityAppendErrorCode::NoDurableMajority);

    // The diverged latch keeps the owner unhealthy under this epoch so
    // recovery moves ownership onto the evidence holder instead of a clean
    // quorum append elsewhere clearing the mark while the fork persists.
    let status = fixture.services[&fixture.owner]
        .recovery_status(fixture.request.feed_id)
        .await
        .unwrap();
    assert!(!status.healthy);

    // The evidence replica's committed frame is untouched.
    let divergent_status = fixture.services[&followers[0]]
        .recovery_status(fixture.request.feed_id)
        .await
        .unwrap();
    assert_eq!(divergent_status.committed, CommitPosition::new(1));
    assert_eq!(divergent_status.appended, RangePosition::new(1));
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
    let probe = Fixture::new().await;
    for unavailable in probe.non_owner_names() {
        let fixture = Fixture::new().await;
        let result = fixture
            .coordinator(&[&unavailable], &[], None)
            .append(fixture.request.clone())
            .await
            .unwrap();
        assert_eq!(result.durable_replicas.len(), 2);
        assert_eq!(result.commit_evidence.len(), 2);
        assert!(!result
            .durable_replicas
            .contains(&StorageNodeId::try_new(unavailable.as_str()).unwrap()));
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
async fn split_freeze_waits_for_in_flight_majority_commit_before_truncating_owner() {
    let fixture = Fixture::new().await;
    let assignment = fixture
        .control
        .active_range_assignment(fixture.request.feed_id)
        .await
        .unwrap();
    let pause = CommitPause {
        node: fixture.non_owner_nodes().remove(0),
        entered: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let coordinator = Arc::new(fixture.coordinator_with_pause(&[], &[], None, Some(pause.clone())));
    let pending = tokio::spawn({
        let coordinator = coordinator.clone();
        let request = fixture.request.clone();
        async move { coordinator.append(request).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), pause.entered.notified())
        .await
        .unwrap();
    let freeze = coordinator.freeze_for_split(&assignment);
    tokio::pin!(freeze);
    tokio::select! {
        biased;
        result = &mut freeze => panic!("split froze before in-flight append committed: {result:?}"),
        _ = tokio::task::yield_now() => {},
    }
    pause.release.notify_one();
    let appended = pending.await.unwrap().unwrap();
    let committed = freeze.await.unwrap();
    assert_eq!(committed.value(), appended.position.value());
    assert_eq!(
        fixture.services[&assignment.owner]
            .recovery_status_for_assignment(&assignment)
            .await
            .unwrap()
            .committed,
        committed
    );
    assert_eq!(
        fixture.services[&assignment.owner]
            .read_staged_committed(&assignment, None, 10)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn follower_move_freeze_discards_only_the_uncommitted_owner_tail() {
    let fixture = Fixture::new().await;
    let down: Vec<String> = fixture.non_owner_names();
    let owner = fixture.coordinator(&[down[0].as_str(), down[1].as_str()], &[], None);
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
    let target_name = fixture.non_owner_names().remove(0);
    let target = fixture.services[&StorageNodeId::try_new(target_name.as_str()).unwrap()].clone();
    let owner = Arc::new(fixture.coordinator(&[], &[target_name.as_str()], None));
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
    request.frame = encode_record(&record).unwrap().into();
    let transport = DirectTransport {
        services: Arc::new(fixture.services.clone()),
        append_down: Arc::new(BTreeSet::new()),
        commit_down: Arc::new(BTreeSet::new()),
        corrupt_digest: None,
        commit_pause: None,
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
    let target_name = fixture.non_owner_names().remove(0);
    let target = fixture.services[&StorageNodeId::try_new(target_name.as_str()).unwrap()].clone();
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
    conflicting.frame = encode_record(&record).unwrap().into();
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

fn paged_request(fixture: &Fixture, sequence: u64) -> ReplicaAppendRequest {
    let mut request = fixture.request.clone();
    let record = StoredRecord {
        message_id: Uuid::from_u128(700 + sequence as u128),
        producer_id: request.identity.writer_session_id,
        producer_sequence: sequence,
        event_time_ns: sequence as i64,
        ingest_time_ns: sequence as i64 + 1,
        key: b"customer-1".to_vec(),
        payload: format!("value-{sequence}").into_bytes(),
        metadata: BTreeMap::new(),
    };
    request.expected_position = RangePosition::new(sequence);
    request.identity.sequence = sequence;
    request.cursor = format!("cursor-{sequence}");
    request.frame = encode_record(&record).unwrap().into();
    request
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
    let follower_name = fixture.non_owner_names().remove(0);
    let follower =
        fixture.services[&StorageNodeId::try_new(follower_name.as_str()).unwrap()].clone();
    let (empty_boundary, empty) = owner
        .read_owned_range_page(&assignment, None, None, 32)
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
        .read_owned_range_page(&assignment, None, None, 32)
        .await
        .unwrap();
    assert_eq!(boundary, CommitPosition::new(1));
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].cursor, fixture.request.cursor);
    let (_, exhausted) = owner
        .read_owned_range_page(&assignment, Some(RangePosition::new(1)), Some(boundary), 32)
        .await
        .unwrap();
    assert!(exhausted.is_empty());
    let (_, resolved, _, suffix) = owner
        .read_owned_range_cursor_page(
            &assignment,
            whitewater::active_range::RangeReadSpec {
                cursor: Some(&fixture.request.cursor),
                ..Default::default()
            },
            None,
            32,
        )
        .await
        .unwrap();
    assert_eq!(resolved, Some(RangePosition::new(1)));
    assert!(suffix.is_empty());
    let (_, start, _, tail) = owner
        .read_owned_range_cursor_page(
            &assignment,
            whitewater::active_range::RangeReadSpec {
                tail_count: Some(1),
                ..Default::default()
            },
            None,
            32,
        )
        .await
        .unwrap();
    assert_eq!(start, Some(RangePosition::new(0)));
    assert_eq!(tail[0].cursor, fixture.request.cursor);
    assert_eq!(
        owner
            .read_owned_range_cursor_page(
                &assignment,
                whitewater::active_range::RangeReadSpec {
                    cursor: Some("unknown"),
                    ..Default::default()
                },
                None,
                32,
            )
            .await
            .unwrap_err()
            .code,
        ReplicaAppendErrorCode::PositionConflict
    );
    for sequence in 2..=5_u64 {
        fixture
            .coordinator(&[], &[], None)
            .append(paged_request(&fixture, sequence))
            .await
            .unwrap();
    }
    let (boundary, page) = owner
        .read_owned_range_page(&assignment, None, None, 2)
        .await
        .unwrap();
    assert_eq!(boundary, CommitPosition::new(5));
    assert_eq!(
        page.iter()
            .map(|frame| frame.cursor.as_str())
            .collect::<Vec<_>>(),
        vec!["cursor-1", "cursor-2"]
    );
    let (boundary, page) = owner
        .read_owned_range_page(&assignment, Some(RangePosition::new(2)), Some(boundary), 2)
        .await
        .unwrap();
    assert_eq!(boundary, CommitPosition::new(5));
    assert_eq!(
        page.iter()
            .map(|frame| frame.cursor.as_str())
            .collect::<Vec<_>>(),
        vec!["cursor-3", "cursor-4"]
    );
    let (_, page) = owner
        .read_owned_range_page(&assignment, Some(RangePosition::new(4)), Some(boundary), 2)
        .await
        .unwrap();
    assert_eq!(
        page.iter()
            .map(|frame| frame.cursor.as_str())
            .collect::<Vec<_>>(),
        vec!["cursor-5"]
    );
    assert_eq!(
        owner
            .read_owned_range_page(&assignment, None, Some(CommitPosition::new(6)), 2)
            .await
            .unwrap_err()
            .code,
        ReplicaAppendErrorCode::StorageFailure
    );
    assert_eq!(
        follower
            .read_owned_range_page(&assignment, None, None, 32)
            .await
            .unwrap_err()
            .code,
        ReplicaAppendErrorCode::NotCurrentOwner
    );
}

#[tokio::test]
async fn read_evidence_exposes_owner_data_loss_instead_of_claiming_empty_history() {
    let fixture = Fixture::new().await;
    let assignment = fixture
        .control
        .active_range_assignment(fixture.request.feed_id)
        .await
        .unwrap();
    fixture
        .coordinator(&[], &[], None)
        .append(fixture.request.clone())
        .await
        .unwrap();
    let owner = fixture.services[&assignment.owner].clone();
    let follower_name = fixture.non_owner_names().remove(0);
    let follower =
        fixture.services[&StorageNodeId::try_new(follower_name.as_str()).unwrap()].clone();
    let boundary = CommitPosition::new(1);
    let original = owner
        .read_replica_evidence(&assignment, boundary)
        .await
        .unwrap();
    let corroborated = follower
        .read_replica_evidence(&assignment, boundary)
        .await
        .unwrap();
    assert_eq!(original.committed, boundary);
    assert_eq!(original.digest, corroborated.digest);
    owner
        .quarantine_for_repair(assignment.feed_id)
        .await
        .unwrap();
    let lost = owner
        .read_replica_evidence(&assignment, CommitPosition::new(0))
        .await
        .unwrap();
    let surviving = follower
        .read_replica_evidence(&assignment, CommitPosition::new(0))
        .await
        .unwrap();
    assert_eq!(lost.committed, CommitPosition::new(0));
    assert!(surviving.committed > lost.committed);
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
        request.frame = encode_record(&StoredRecord {
            message_id: Uuid::from_u128(700 + sequence as u128),
            producer_id: fixture.request.identity.writer_session_id,
            producer_sequence: sequence,
            event_time_ns: sequence as i64,
            ingest_time_ns: sequence as i64,
            key: b"customer-1".to_vec(),
            payload: Vec::new(),
            metadata: BTreeMap::new(),
        })
        .unwrap()
        .into();
        coordinator.append(request).await.unwrap();
    }
    let (commit, position, _, page) = owner
        .read_owned_range_cursor_page(
            &assignment,
            whitewater::active_range::RangeReadSpec {
                cursor: Some("cursor-1"),
                ..Default::default()
            },
            None,
            32,
        )
        .await
        .unwrap();
    assert_eq!(commit, CommitPosition::new(34));
    assert_eq!(position, Some(RangePosition::new(1)));
    assert_eq!(page.len(), 32);
    assert_eq!(page.first().unwrap().cursor, "cursor-2");
    assert_eq!(page.last().unwrap().cursor, "cursor-33");
    let (_, _, _, next) = owner
        .read_owned_range_cursor_page(
            &assignment,
            whitewater::active_range::RangeReadSpec {
                after: Some(RangePosition::new(33)),
                ..Default::default()
            },
            Some(commit),
            32,
        )
        .await
        .unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].cursor, "cursor-34");
    let (_, tail_start, _, tail) = owner
        .read_owned_range_cursor_page(
            &assignment,
            whitewater::active_range::RangeReadSpec {
                tail_count: Some(2),
                ..Default::default()
            },
            None,
            32,
        )
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
async fn ingest_threshold_probe_resolves_the_fenced_committed_boundary() {
    let fixture = Fixture::new().await;
    let assignment = fixture
        .control
        .active_range_assignment(fixture.request.feed_id)
        .await
        .unwrap();
    let owner = fixture.services[&assignment.owner].clone();
    let follower_name = fixture.non_owner_names().remove(0);
    let follower =
        fixture.services[&StorageNodeId::try_new(follower_name.as_str()).unwrap()].clone();
    fixture
        .coordinator(&[], &[], None)
        .append(fixture.request.clone())
        .await
        .unwrap();
    for sequence in 2..=5_u64 {
        fixture
            .coordinator(&[], &[], None)
            .append(paged_request(&fixture, sequence))
            .await
            .unwrap();
    }
    // Committed ingest times are 2,3,4,5,6 (record sequence + 1); the
    // threshold 5 lands on the frame at position 3.
    let (boundary, start, probe, frames) = owner
        .read_owned_range_cursor_page(
            &assignment,
            whitewater::active_range::RangeReadSpec {
                ingest_threshold: Some(5),
                ..Default::default()
            },
            None,
            32,
        )
        .await
        .unwrap();
    assert_eq!(boundary, CommitPosition::new(5));
    assert_eq!(start, Some(RangePosition::new(3)));
    assert!(frames.is_empty());
    match probe.unwrap() {
        whitewater::active_range::IngestBoundary::Ordered { position, cursor } => {
            assert_eq!(position, Some(RangePosition::new(3)));
            assert_eq!(cursor, "cursor-3");
        }
        other => panic!("expected an ordered boundary, got {other:?}"),
    }
    // A threshold at-or-before the head resolves to the range start.
    let (_, start, probe, _) = owner
        .read_owned_range_cursor_page(
            &assignment,
            whitewater::active_range::RangeReadSpec {
                ingest_threshold: Some(2),
                ..Default::default()
            },
            None,
            32,
        )
        .await
        .unwrap();
    assert_eq!(start, None);
    match probe.unwrap() {
        whitewater::active_range::IngestBoundary::Ordered { position, cursor } => {
            assert_eq!(position, None);
            assert!(cursor.is_empty());
        }
        other => panic!("expected an ordered boundary, got {other:?}"),
    }
    // The probe cannot be combined with another read mode.
    assert_eq!(
        owner
            .read_owned_range_cursor_page(
                &assignment,
                whitewater::active_range::RangeReadSpec {
                    cursor: Some("cursor-1"),
                    ingest_threshold: Some(5),
                    ..Default::default()
                },
                None,
                32
            )
            .await
            .unwrap_err()
            .code,
        ReplicaAppendErrorCode::InvalidFrame
    );
    // A non-owner is fenced before the index is consulted.
    assert_eq!(
        follower
            .read_owned_range_cursor_page(
                &assignment,
                whitewater::active_range::RangeReadSpec {
                    ingest_threshold: Some(5),
                    ..Default::default()
                },
                None,
                32,
            )
            .await
            .unwrap_err()
            .code,
        ReplicaAppendErrorCode::NotCurrentOwner
    );
}

#[tokio::test]
async fn one_healthy_replica_never_returns_success() {
    let fixture = Fixture::new().await;
    let error = fixture
        .coordinator(
            &[
                fixture.non_owner_names()[0].as_str(),
                fixture.non_owner_names()[1].as_str(),
            ],
            &[],
            None,
        )
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
        .coordinator(
            &[],
            &[
                fixture.non_owner_names()[0].as_str(),
                fixture.non_owner_names()[1].as_str(),
            ],
            None,
        )
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
        .coordinator(&[], &[], Some(fixture.non_owner_names()[0].as_str()))
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

fn pending_request(request: &mut ReplicaAppendRequest, effect_id: Uuid, subscription_id: Uuid) {
    request.pending_effect = Some(whitewater::active_range::PendingEffectRef {
        effect_id,
        subscription_id,
    });
}

/// Re-encode `request` at a new writer sequence and position so the frame
/// identity matches the record bytes.
fn resequence(request: &mut ReplicaAppendRequest, sequence: u64, position: u64) {
    let record = StoredRecord {
        message_id: Uuid::from_u128(1_000 + sequence as u128),
        producer_id: request.identity.writer_session_id,
        producer_sequence: sequence,
        event_time_ns: sequence as i64,
        ingest_time_ns: sequence as i64 + 1,
        key: b"customer-1".to_vec(),
        payload: format!("payload-{sequence}").into_bytes(),
        metadata: BTreeMap::new(),
    };
    request.identity.sequence = sequence;
    request.expected_position = RangePosition::new(position);
    request.cursor = format!("cursor-{sequence}");
    request.frame = encode_record(&record).unwrap().into();
}

#[tokio::test]
async fn pending_appends_replicate_durably_without_becoming_committed() {
    let fixture = Fixture::new().await;
    let coordinator = fixture.coordinator(&[], &[], None);
    let mut request = fixture.request.clone();
    pending_request(&mut request, Uuid::from_u128(800), Uuid::from_u128(801));

    let result = coordinator.append(request.clone()).await.unwrap();
    assert_eq!(result.position, RangePosition::new(1));
    // No commit evidence: the release phase commits the tail later.
    assert!(result.commit_evidence.is_empty());
    assert!(!result.durable_replicas.is_empty());

    // Every replica holds the frame durably but uncommitted.
    let assignment = fixture
        .control
        .active_range_assignment(fixture.request.feed_id)
        .await
        .unwrap();
    for service in fixture.services.values() {
        let status = service
            .recovery_status_for_assignment(&assignment)
            .await
            .unwrap();
        assert_eq!(status.appended, RangePosition::new(1));
        assert_eq!(status.committed, CommitPosition::new(0));
        assert_eq!(
            service
                .pending_effect(&assignment)
                .await
                .unwrap()
                .unwrap()
                .effect_id,
            Uuid::from_u128(800)
        );
        assert!(service
            .read_staged_committed(&assignment, None, 10)
            .await
            .unwrap()
            .is_empty());
    }
}

#[tokio::test]
async fn pending_gate_refuses_a_foreign_effect_and_reports_the_occupant() {
    let fixture = Fixture::new().await;
    let coordinator = fixture.coordinator(&[], &[], None);

    let mut first = fixture.request.clone();
    pending_request(&mut first, Uuid::from_u128(810), Uuid::from_u128(811));
    coordinator.append(first).await.unwrap();

    let mut second = fixture.request.clone();
    resequence(&mut second, 2, 2);
    pending_request(&mut second, Uuid::from_u128(812), Uuid::from_u128(811));
    let error = coordinator.append(second).await.unwrap_err();
    assert_eq!(error.code, MajorityAppendErrorCode::PendingEffectOccupied);
    assert_eq!(
        error.pending_effect.unwrap().effect_id,
        Uuid::from_u128(810)
    );

    // A plain append is fenced behind the held tail the same way.
    let mut plain = fixture.request.clone();
    resequence(&mut plain, 2, 2);
    let error = coordinator.append(plain).await.unwrap_err();
    assert_eq!(error.code, MajorityAppendErrorCode::PendingEffectOccupied);
}

#[tokio::test]
async fn commit_pending_effect_releases_the_tail_across_replicas() {
    let fixture = Fixture::new().await;
    let coordinator = fixture.coordinator(&[], &[], None);
    let effect = Uuid::from_u128(820);
    let subscription = Uuid::from_u128(821);

    let mut request = fixture.request.clone();
    pending_request(&mut request, effect, subscription);
    coordinator.append(request.clone()).await.unwrap();

    let assignment = fixture
        .control
        .active_range_assignment(fixture.request.feed_id)
        .await
        .unwrap();
    let released = coordinator
        .commit_pending_effect(whitewater::active_range::PendingReleaseRequest {
            feed_id: request.feed_id,
            range_id: request.range_id,
            generation: request.generation,
            ownership_epoch: request.ownership_epoch,
            append_owner: fixture.owner.clone(),
            effect_id: effect,
            subscription_id: subscription,
        })
        .await
        .unwrap();
    assert_eq!(
        released,
        whitewater::active_range::PendingReleaseOutcome::Released {
            commit_position: CommitPosition::new(1),
        }
    );

    // The released tail is committed on the quorum and readable; markers
    // are gone everywhere.
    for service in fixture.services.values() {
        let status = service
            .recovery_status_for_assignment(&assignment)
            .await
            .unwrap();
        assert_eq!(status.committed, CommitPosition::new(1));
        assert_eq!(service.pending_effect(&assignment).await.unwrap(), None);
        assert_eq!(
            service
                .read_staged_committed(&assignment, None, 10)
                .await
                .unwrap()
                .len(),
            1
        );
    }
}

#[tokio::test]
async fn commit_pending_effect_is_idempotent_and_fences_wrong_effects() {
    let fixture = Fixture::new().await;
    let coordinator = fixture.coordinator(&[], &[], None);
    let effect = Uuid::from_u128(830);
    let subscription = Uuid::from_u128(831);

    let mut request = fixture.request.clone();
    pending_request(&mut request, effect, subscription);
    coordinator.append(request.clone()).await.unwrap();

    let release = |effect_id: Uuid| whitewater::active_range::PendingReleaseRequest {
        feed_id: request.feed_id,
        range_id: request.range_id,
        generation: request.generation,
        ownership_epoch: request.ownership_epoch,
        append_owner: fixture.owner.clone(),
        effect_id,
        subscription_id: subscription,
    };

    // A different effect cannot release a tail it does not own.
    let outcome = coordinator
        .commit_pending_effect(release(Uuid::from_u128(832)))
        .await
        .unwrap();
    match outcome {
        whitewater::active_range::PendingReleaseOutcome::Occupied { occupant } => {
            assert_eq!(occupant.effect_id, effect);
        }
        other => panic!("expected Occupied, got {other:?}"),
    }

    // The owning effect releases; a retried release then reports the tail
    // already resolved rather than erroring.
    coordinator
        .commit_pending_effect(release(effect))
        .await
        .unwrap();
    let outcome = coordinator
        .commit_pending_effect(release(effect))
        .await
        .unwrap();
    match outcome {
        whitewater::active_range::PendingReleaseOutcome::NotPending { committed } => {
            assert_eq!(committed, 1);
        }
        other => panic!("expected NotPending, got {other:?}"),
    }
}

#[tokio::test]
async fn release_is_unambiguous_when_one_follower_is_down() {
    let fixture = Fixture::new().await;
    // One follower cannot take the deferred commit; the release still
    // reaches quorum through the other follower.
    let down = fixture.non_owner_names();
    let down_refs = down.iter().take(1).map(String::as_str).collect::<Vec<_>>();
    let coordinator = fixture.coordinator(&[], &down_refs, None);
    let effect = Uuid::from_u128(840);
    let subscription = Uuid::from_u128(841);

    let mut request = fixture.request.clone();
    pending_request(&mut request, effect, subscription);
    coordinator.append(request.clone()).await.unwrap();

    let outcome = coordinator
        .commit_pending_effect(whitewater::active_range::PendingReleaseRequest {
            feed_id: request.feed_id,
            range_id: request.range_id,
            generation: request.generation,
            ownership_epoch: request.ownership_epoch,
            append_owner: fixture.owner.clone(),
            effect_id: effect,
            subscription_id: subscription,
        })
        .await
        .unwrap();
    assert_eq!(
        outcome,
        whitewater::active_range::PendingReleaseOutcome::Released {
            commit_position: CommitPosition::new(1),
        }
    );
}

fn discard_release(
    request: &ReplicaAppendRequest,
    owner: &StorageNodeId,
    effect_id: Uuid,
    subscription_id: Uuid,
) -> whitewater::active_range::PendingReleaseRequest {
    whitewater::active_range::PendingReleaseRequest {
        feed_id: request.feed_id,
        range_id: request.range_id,
        generation: request.generation,
        ownership_epoch: request.ownership_epoch,
        append_owner: owner.clone(),
        effect_id,
        subscription_id,
    }
}

#[tokio::test]
async fn discard_pending_effect_truncates_the_tail_across_replicas() {
    let fixture = Fixture::new().await;
    let coordinator = fixture.coordinator(&[], &[], None);
    let effect = Uuid::from_u128(850);
    let subscription = Uuid::from_u128(851);

    let mut request = fixture.request.clone();
    pending_request(&mut request, effect, subscription);
    coordinator.append(request.clone()).await.unwrap();

    let assignment = fixture
        .control
        .active_range_assignment(request.feed_id)
        .await
        .unwrap();
    let outcome = coordinator
        .discard_pending_effect(discard_release(
            &request,
            &fixture.owner,
            effect,
            subscription,
        ))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        whitewater::active_range::PendingDiscardOutcome::Discarded { dropped_frames: 1 }
    );

    // Owner and both followers dropped the marker and the uncommitted
    // frame; nothing became visible anywhere.
    for service in fixture.services.values() {
        assert_eq!(service.pending_effect(&assignment).await.unwrap(), None);
        assert_eq!(
            service
                .read_staged_committed(&assignment, None, 10)
                .await
                .unwrap()
                .len(),
            0
        );
    }
}

#[tokio::test]
async fn discard_pending_effect_is_idempotent_and_fences_wrong_effects() {
    let fixture = Fixture::new().await;
    let coordinator = fixture.coordinator(&[], &[], None);
    let effect = Uuid::from_u128(860);
    let subscription = Uuid::from_u128(861);

    let mut request = fixture.request.clone();
    pending_request(&mut request, effect, subscription);
    coordinator.append(request.clone()).await.unwrap();

    // A different effect cannot discard a tail it does not own.
    let outcome = coordinator
        .discard_pending_effect(discard_release(
            &request,
            &fixture.owner,
            Uuid::from_u128(862),
            subscription,
        ))
        .await
        .unwrap();
    match outcome {
        whitewater::active_range::PendingDiscardOutcome::Occupied(marker) => {
            assert_eq!(marker.effect_id, effect);
        }
        other => panic!("expected Occupied, got {other:?}"),
    }

    // The owning effect discards; a retried discard reports the tail
    // already resolved rather than erroring.
    coordinator
        .discard_pending_effect(discard_release(
            &request,
            &fixture.owner,
            effect,
            subscription,
        ))
        .await
        .unwrap();
    let outcome = coordinator
        .discard_pending_effect(discard_release(
            &request,
            &fixture.owner,
            effect,
            subscription,
        ))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        whitewater::active_range::PendingDiscardOutcome::NotPending
    );
}

#[tokio::test]
async fn discard_pending_effect_heals_a_follower_that_missed_the_first_discard() {
    let fixture = Fixture::new().await;
    let coordinator = fixture.coordinator(&[], &[], None);
    let effect = Uuid::from_u128(870);
    let subscription = Uuid::from_u128(871);
    let assignment = fixture
        .control
        .active_range_assignment(fixture.request.feed_id)
        .await
        .unwrap();
    let follower = fixture
        .non_owner_nodes()
        .into_iter()
        .next()
        .expect("follower");

    // The follower accepted a pending frame whose discard broadcast it
    // then missed: the owner never held the marker at all.
    let mut request = fixture.request.clone();
    pending_request(&mut request, effect, subscription);
    fixture.services[&follower]
        .append(request.clone())
        .await
        .unwrap();
    assert!(fixture.services[&follower]
        .pending_effect(&assignment)
        .await
        .unwrap()
        .is_some());

    // The owner's local marker is absent, but the broadcast still reaches
    // the follower and heals its stale tail.
    let outcome = coordinator
        .discard_pending_effect(discard_release(
            &request,
            &fixture.owner,
            effect,
            subscription,
        ))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        whitewater::active_range::PendingDiscardOutcome::NotPending
    );
    assert_eq!(
        fixture.services[&follower]
            .pending_effect(&assignment)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        fixture.services[&follower]
            .read_staged_committed(&assignment, None, 10)
            .await
            .unwrap()
            .len(),
        0
    );
}

#[tokio::test]
async fn discard_pending_effect_fences_a_stale_ownership_epoch() {
    let fixture = Fixture::new().await;
    let coordinator = fixture.coordinator(&[], &[], None);
    let effect = Uuid::from_u128(880);
    let subscription = Uuid::from_u128(881);

    let mut request = fixture.request.clone();
    pending_request(&mut request, effect, subscription);
    coordinator.append(request.clone()).await.unwrap();

    let mut stale = discard_release(&request, &fixture.owner, effect, subscription);
    stale.ownership_epoch = OwnershipEpoch::new(9);
    let error = coordinator.discard_pending_effect(stale).await.unwrap_err();
    assert_eq!(error.code, MajorityAppendErrorCode::NotCurrentOwner);
}

#[tokio::test]
async fn retried_append_repushes_the_deduplicated_position_to_lagging_followers() {
    let fixture = Fixture::new().await;
    // The owner stored the frame; both followers missed the original push.
    fixture.services[&fixture.owner]
        .append(fixture.request.clone())
        .await
        .unwrap();
    // The retry deduplicates on the owner at position 1: replicas must be
    // offered that position rather than the post-allocation tip.
    let coordinator = fixture.coordinator(&[], &[], None);
    let result = coordinator.append(fixture.request.clone()).await.unwrap();
    assert_eq!(result.position.value(), 1);
    assert!(result.deduplicated);
    assert_eq!(result.durable_replicas.len(), 3);
    for node in fixture.non_owner_nodes() {
        let status = fixture.services[&node]
            .recovery_status(fixture.request.feed_id)
            .await
            .unwrap();
        assert!(status.healthy);
        assert_eq!(status.committed.value(), 1);
        assert_eq!(status.appended.value(), 1);
    }
}

#[tokio::test]
async fn position_gap_repushes_the_missing_owner_frames_before_the_current_one() {
    let fixture = Fixture::new().await;
    // Frame 1 landed only on the owner; the followers never stored it.
    fixture.services[&fixture.owner]
        .append(fixture.request.clone())
        .await
        .unwrap();
    // Frame 2 is a fresh append: the followers are a position behind, so
    // the coordinator must re-drive position 1 before frame 2 can land.
    let mut second = fixture.request.clone();
    second.identity.sequence = 2;
    second.cursor = "cursor-2".to_owned();
    second.frame = encode_record(&StoredRecord {
        message_id: Uuid::from_u128(702),
        producer_id: second.identity.writer_session_id,
        producer_sequence: 2,
        event_time_ns: 3,
        ingest_time_ns: 4,
        key: b"customer-2".to_vec(),
        payload: b"shipped".to_vec(),
        metadata: BTreeMap::new(),
    })
    .unwrap()
    .into();
    let coordinator = fixture.coordinator(&[], &[], None);
    let result = coordinator.append(second).await.unwrap();
    assert_eq!(result.position.value(), 2);
    assert!(!result.deduplicated);
    assert_eq!(result.durable_replicas.len(), 3);
    for node in fixture.non_owner_nodes() {
        let status = fixture.services[&node]
            .recovery_status(fixture.request.feed_id)
            .await
            .unwrap();
        assert_eq!(status.appended.value(), 2);
        assert_eq!(status.committed.value(), 2);
    }
}

#[tokio::test]
async fn validated_owner_frame_truncates_the_superseded_uncommitted_tail() {
    let fixture = Fixture::new().await;
    let coordinator = fixture.coordinator(&[], &[], None);
    coordinator.append(fixture.request.clone()).await.unwrap();
    let replica = fixture.non_owner_nodes()[0].clone();
    // The replica holds an old-epoch uncommitted orphan at position 2,
    // left behind by the previous owner's abandoned tail.
    let mut orphan = fixture.request.clone();
    orphan.identity.sequence = 2;
    orphan.expected_position = RangePosition::new(2);
    orphan.cursor = "cursor-2".to_owned();
    orphan.frame = encode_record(&StoredRecord {
        message_id: Uuid::from_u128(703),
        producer_id: orphan.identity.writer_session_id,
        producer_sequence: 2,
        event_time_ns: 5,
        ingest_time_ns: 6,
        key: b"customer-3".to_vec(),
        payload: b"orphaned".to_vec(),
        metadata: BTreeMap::new(),
    })
    .unwrap()
    .into();
    fixture.services[&replica].append(orphan).await.unwrap();
    // Ownership moves to the replica under a newer epoch.
    fixture
        .control
        .execute_commands(vec![Command::RecoverActiveRangeOwnership {
            feed: "orders.events".to_owned(),
            expected_owner: fixture.owner.clone(),
            expected_epoch: OwnershipEpoch::new(1),
            new_owner: replica.clone(),
        }])
        .await
        .unwrap();
    // The new owner's epoch-2 frame claims the orphaned position: the
    // placement-validated frame is authoritative for the uncommitted tail,
    // so the append drops the orphan rather than reporting a permanent
    // conflict.
    let mut successor = fixture.request.clone();
    successor.ownership_epoch = OwnershipEpoch::new(2);
    successor.append_owner = replica.clone();
    successor.identity.writer_session_id = Uuid::from_u128(704);
    successor.expected_position = RangePosition::new(2);
    successor.cursor = "cursor-successor".to_owned();
    successor.frame = encode_record(&StoredRecord {
        message_id: Uuid::from_u128(705),
        producer_id: successor.identity.writer_session_id,
        producer_sequence: 1,
        event_time_ns: 7,
        ingest_time_ns: 8,
        key: b"customer-4".to_vec(),
        payload: b"adopted".to_vec(),
        metadata: BTreeMap::new(),
    })
    .unwrap()
    .into();
    let accepted = fixture.services[&replica].append(successor).await.unwrap();
    assert_eq!(accepted.position.value(), 2);
    assert!(!accepted.deduplicated);
    let status = fixture.services[&replica]
        .recovery_status(fixture.request.feed_id)
        .await
        .unwrap();
    assert_eq!(status.appended.value(), 2);
    assert_eq!(status.committed.value(), 1);
}

#[tokio::test]
async fn resequenced_retry_dedup_pushes_the_stored_frame_to_replicas() {
    // A retried request shares its request-derived Cursor with the original
    // append but can carry a freshly allocated or healed sequence — the
    // exact shape the API hands the owner after a stale-sequence refusal.
    // The owner dedups it to the stored position; the coordinator must then
    // push the canonical stored frame, because offering the request's
    // divergent identity at that position makes every replica refuse a
    // committed occupant and wedges the range.
    let fixture = Fixture::new().await;
    let coordinator = fixture.coordinator(&[], &[], None);
    let mut records = vec![fixture.request.clone()];
    for sequence in 2..=3_u64 {
        let mut request = fixture.request.clone();
        request.cursor = format!("cursor-{sequence}");
        request.identity.sequence = sequence;
        let mut record = decode_record(&fixture.request.frame).unwrap();
        record.message_id = Uuid::from_u128(700 + sequence as u128);
        record.producer_sequence = sequence;
        request.frame = encode_record(&record).unwrap().into();
        records.push(request);
    }
    for request in &records {
        coordinator.append(request.clone()).await.unwrap();
    }

    let mut retry = records[0].clone();
    retry.identity.sequence = 9;
    let mut record = decode_record(&retry.frame).unwrap();
    record.producer_sequence = 9;
    retry.frame = encode_record(&record).unwrap().into();
    let result = coordinator.append(retry).await.unwrap();
    assert!(result.deduplicated);
    assert_eq!(result.position.value(), 1);
    assert_eq!(result.durable_replicas.len(), 3);

    for service in fixture.services.values() {
        let status = service
            .recovery_status(fixture.request.feed_id)
            .await
            .unwrap();
        assert_eq!(status.appended.value(), 3);
        assert_eq!(status.committed.value(), 3);
    }
}

#[tokio::test]
async fn committed_append_kicks_the_index_apply_boundary() {
    // Every replica that advances its committed watermark must trigger the
    // deterministic Index apply for that Feed: the committed log carries
    // the intents, and each replica materializes them at the same boundary.
    let fixture = Fixture::new().await;
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    struct CountingSource {
        feed_id: Uuid,
        reads: Arc<std::sync::atomic::AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl whitewater::state_store::FeedReplaySource for CountingSource {
        async fn source_exists(&self, feed_id: Uuid) -> bool {
            feed_id == self.feed_id
        }
        async fn read_committed_records(
            &self,
            feed_id: Uuid,
            _after: Option<&str>,
            _limit: usize,
        ) -> Result<
            Vec<whitewater::state_store::ReplaySourceRecord>,
            whitewater::state_store::StateStoreReplayError,
        > {
            assert_eq!(feed_id, self.feed_id);
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(vec![])
        }
    }
    // An Active generation on this Feed is what the commit path serves.
    let index_id = {
        fixture
            .control
            .execute_commands(vec![
                Command::DefineIndex {
                    name: "orders.events.by.key".to_owned(),
                    feed: "orders.events".to_owned(),
                    fields: vec![whitewater::control::IndexFieldSpec::Key],
                    unique: false,
                    collation: whitewater::control::IndexCollation::Binary,
                    consistency: whitewater::control::IndexConsistency::Eventual,
                },
                Command::RebuildIndex {
                    name: "orders.events.by.key".to_owned(),
                },
            ])
            .await
            .unwrap();
        let declared = fixture
            .control
            .declared_index_by_name("orders.events.by.key")
            .await
            .unwrap();
        fixture
            .control
            .execute_commands(vec![Command::CompleteIndexBuild {
                index_id: declared.index_id,
                generation: 1,
                applied_cursor: "genesis".to_owned(),
            }])
            .await
            .unwrap();
        declared.index_id
    };
    let _ = index_id;
    let index_dir = tempfile::TempDir::new().unwrap();
    let index = Arc::new(whitewater::index::FjallIndexStore::open(index_dir.path()).unwrap());
    let driver = Arc::new(whitewater::index_build::IndexBuildDriver::new(
        fixture.control.clone(),
        Arc::new(CountingSource {
            feed_id: fixture.request.feed_id,
            reads: reads.clone(),
        }),
        index,
    ));
    let gate = Arc::new(whitewater::index_build::IndexCommitApply::new(driver));
    for service in fixture.services.values() {
        service.set_index_apply(gate.clone());
    }
    let coordinator = fixture.coordinator(&[], &[], None);
    let accepted = coordinator.append(fixture.request.clone()).await.unwrap();
    assert_eq!(accepted.position.value(), 1);
    // The owner committed plus at least one follower commit each kick the
    // gate; the apply pass itself is async, so poll for the side effect.
    for _ in 0..200 {
        if reads.load(std::sync::atomic::Ordering::SeqCst) > 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("commit never triggered the deterministic Index apply boundary");
}

#[tokio::test]
async fn owner_minted_sequences_stay_contiguous_and_retries_deduplicate() {
    let fixture = Fixture::new().await;
    let coordinator = fixture.coordinator(&[], &[], None);
    let writer = Uuid::from_u128(700);
    let encode = |sequence: u64, marker: u128, payload: &'static [u8]| {
        Ok(Bytes::from(
            encode_record(&StoredRecord {
                message_id: Uuid::from_u128(marker),
                producer_id: writer,
                producer_sequence: sequence,
                event_time_ns: 1,
                ingest_time_ns: 2,
                key: b"customer-1".to_vec(),
                payload: payload.to_vec(),
                metadata: BTreeMap::new(),
            })
            .unwrap(),
        ))
    };
    // The request's sequence and frame are placeholders — the owner mints
    // the contiguous number under the range's append lock before encoding.
    let mut first = fixture.request.clone();
    first.cursor = "mint-1".to_owned();
    first.identity.sequence = 0;
    first.frame = bytes::Bytes::new();
    let first = coordinator
        .append_minted(first, |sequence| encode(sequence, 701, b"one"))
        .await
        .unwrap();
    assert_eq!(first.position.value(), 1);

    let mut second = fixture.request.clone();
    second.cursor = "mint-2".to_owned();
    second.identity.sequence = 0;
    second.frame = bytes::Bytes::new();
    let second = coordinator
        .append_minted(second, |sequence| encode(sequence, 702, b"two"))
        .await
        .unwrap();
    assert_eq!(second.position.value(), 2);

    let frames = fixture.services[&fixture.owner]
        .export_committed(fixture.request.feed_id, None, 10)
        .await
        .unwrap();
    assert_eq!(frames[0].identity.sequence, 1);
    assert_eq!(frames[1].identity.sequence, 2);
    // The stored frame's producer_sequence is the minted number — the
    // identity and record never disagree.
    assert_eq!(
        decode_record(&frames[0].frame).unwrap().producer_sequence,
        1
    );
    assert_eq!(
        decode_record(&frames[1].frame).unwrap().producer_sequence,
        2
    );

    // A retry of the first request deduplicates on its request-derived
    // Cursor: the minted sequence is consumed against the original position
    // rather than appending a duplicate.
    let mut retry = fixture.request.clone();
    retry.cursor = "mint-1".to_owned();
    retry.identity.sequence = 0;
    retry.frame = bytes::Bytes::new();
    let retried = coordinator
        .append_minted(retry, |sequence| encode(sequence, 701, b"one"))
        .await
        .unwrap();
    assert!(retried.deduplicated);
    assert_eq!(retried.position, first.position);
    let frames = fixture.services[&fixture.owner]
        .export_committed(fixture.request.feed_id, None, 10)
        .await
        .unwrap();
    assert_eq!(
        frames.len(),
        2,
        "a retried minted append must not duplicate"
    );
}

#[tokio::test]
async fn minted_sequences_parallelize_across_ranges_on_one_node() {
    // Two Feeds' ranges share this owner Node; minted appends to each run
    // under independent range locks so neither waits on the other's quorum
    // round trip. Concurrency proves the locks are per-range; the committed
    // streams prove each mint stayed contiguous within its own range.
    let fixture = Fixture::new().await;
    let created = fixture
        .control
        .execute_commands_with_request_id(
            vec![Command::CreateFeed {
                name: "orders.second".to_owned(),
                encrypted: false,
            }],
            Uuid::from_u128(601),
        )
        .await
        .unwrap();
    let second_feed_id =
        serde_json::from_value(created.results[0].data["feed_id"].clone()).unwrap();
    let second_assignment = fixture
        .control
        .active_range_assignment(second_feed_id)
        .await
        .unwrap();
    let coordinator = fixture.coordinator(&[], &[], None);

    let encode_for = |writer: Uuid, marker: u128, payload: &'static [u8]| {
        move |sequence: u64| {
            Ok(Bytes::from(
                encode_record(&StoredRecord {
                    message_id: Uuid::from_u128(marker),
                    producer_id: writer,
                    producer_sequence: sequence,
                    event_time_ns: 1,
                    ingest_time_ns: 2,
                    key: b"customer-1".to_vec(),
                    payload: payload.to_vec(),
                    metadata: BTreeMap::new(),
                })
                .unwrap(),
            ))
        }
    };
    // A mint goes through the range's current owner; placement may put the
    // second Feed on a different Node, which is itself the point.
    let second_coordinator = MajorityAppendCoordinator::new(
        fixture.services[&second_assignment.owner].clone(),
        fixture.control.clone(),
        Arc::new(DirectTransport {
            services: Arc::new(fixture.services.clone()),
            append_down: Arc::new(BTreeSet::new()),
            commit_down: Arc::new(BTreeSet::new()),
            corrupt_digest: None,
            commit_pause: None,
        }),
    );
    let mut a = fixture.request.clone();
    a.cursor = "mint-a".to_owned();
    a.identity.sequence = 0;
    a.frame = bytes::Bytes::new();
    let mut b = a.clone();
    b.feed_id = second_feed_id;
    b.range_id = second_assignment.range_id;
    b.generation = second_assignment.generation;
    b.append_owner = second_assignment.owner.clone();
    b.cursor = "mint-b".to_owned();
    let writer = fixture.request.identity.writer_session_id;
    let (ra, rb) = tokio::join!(
        coordinator.append_minted(a, encode_for(writer, 801, b"a")),
        second_coordinator.append_minted(b, encode_for(writer, 802, b"b")),
    );
    assert_eq!(ra.unwrap().position.value(), 1);
    assert_eq!(rb.unwrap().position.value(), 1);
}

/// DirectTransport variant that measures per-follower in-flight
/// `append_batch` concurrency and records the position order each
/// follower observes — the evidence windowed sends provide.
#[derive(Clone)]
struct WindowedTransport {
    services: Arc<BTreeMap<StorageNodeId, Arc<ReplicaAppendService>>>,
    delay: std::time::Duration,
    // (current in flight, high-water mark) per follower.
    in_flight: Arc<std::sync::Mutex<BTreeMap<StorageNodeId, (usize, usize)>>>,
    // First-frame positions in the order each follower received them.
    received: Arc<std::sync::Mutex<BTreeMap<StorageNodeId, Vec<u64>>>>,
    // Pause one designated send inside the transport to hold the lane.
    pause_position: Option<u64>,
    pause_entered: Arc<tokio::sync::Notify>,
    pause_release: Arc<tokio::sync::Notify>,
}

impl WindowedTransport {
    fn new(fixture: &Fixture, delay: std::time::Duration) -> Self {
        Self {
            services: Arc::new(fixture.services.clone()),
            delay,
            in_flight: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            received: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            pause_position: None,
            pause_entered: Arc::new(tokio::sync::Notify::new()),
            pause_release: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn max_in_flight(&self, node: &StorageNodeId) -> usize {
        self.in_flight
            .lock()
            .unwrap()
            .get(node)
            .map(|entry| entry.1)
            .unwrap_or(0)
    }

    fn received(&self, node: &StorageNodeId) -> Vec<u64> {
        self.received
            .lock()
            .unwrap()
            .get(node)
            .cloned()
            .unwrap_or_default()
    }
}

#[async_trait]
impl ReplicaTransport for WindowedTransport {
    async fn append(
        &self,
        replica: &StorageNodeId,
        request: ReplicaAppendRequest,
    ) -> Result<ReplicaAppendAccepted, ReplicaTransportError> {
        self.services[replica]
            .append(request)
            .await
            .map_err(ReplicaTransportError::replica)
    }

    async fn append_batch(
        &self,
        replica: &StorageNodeId,
        request: whitewater::active_range::ReplicaBatchAppendRequest,
    ) -> Result<Vec<ReplicaAppendAccepted>, ReplicaTransportError> {
        {
            let mut in_flight = self.in_flight.lock().unwrap();
            let entry = in_flight.entry(replica.clone()).or_default();
            entry.0 += 1;
            entry.1 = entry.1.max(entry.0);
        }
        if let Some(position) = request
            .frames
            .first()
            .and_then(|frame| frame.expected_position)
        {
            self.received
                .lock()
                .unwrap()
                .entry(replica.clone())
                .or_default()
                .push(position.value());
        }
        if self.pause_position.is_some()
            && request
                .frames
                .iter()
                .any(|frame| frame.expected_position.map(|p| p.value()) == self.pause_position)
        {
            self.pause_entered.notify_one();
            self.pause_release.notified().await;
        }
        tokio::time::sleep(self.delay).await;
        let result = self.services[replica]
            .import_committed_batch(request)
            .await
            .map_err(ReplicaTransportError::replica);
        self.in_flight.lock().unwrap().get_mut(replica).unwrap().0 -= 1;
        result
    }

    async fn commit(
        &self,
        replica: &StorageNodeId,
        request: ReplicaCommitRequest,
    ) -> Result<ReplicaCommitAccepted, ReplicaTransportError> {
        self.services[replica]
            .commit(request)
            .await
            .map_err(ReplicaTransportError::replica)
    }

    async fn discard_pending(
        &self,
        replica: &StorageNodeId,
        request: ReplicaDiscardPendingRequest,
    ) -> Result<ReplicaDiscardPendingResponse, ReplicaTransportError> {
        self.services[replica]
            .discard_pending(request)
            .await
            .map_err(ReplicaTransportError::replica)
    }
}

/// One-frame batch request for a distinct writer, so concurrent rounds
/// never wedge on per-writer sequence order.
fn windowed_request(fixture: &Fixture, index: u64) -> ReplicaAppendRequest {
    let mut request = fixture.request.clone();
    let writer = Uuid::from_u128(10_000 + index as u128);
    let record = StoredRecord {
        message_id: Uuid::from_u128(20_000 + index as u128),
        producer_id: writer,
        producer_sequence: 1,
        event_time_ns: index as i64,
        ingest_time_ns: index as i64 + 1,
        key: b"customer-1".to_vec(),
        payload: format!("payload-{index}").into_bytes(),
        metadata: BTreeMap::new(),
    };
    request.identity = AppendIdentity {
        writer_session_id: writer,
        writer_epoch: 1,
        sequence: 1,
    };
    request.cursor = format!("window-{index}");
    request.frame = encode_record(&record).unwrap().into();
    request
}

#[tokio::test]
async fn concurrent_submissions_merge_into_fewer_rounds() {
    // Several client batches queued on one Range's append lock fold
    // into fewer quorum rounds, each caller receiving exactly its own
    // result slice in request order.
    let fixture = Fixture::new().await;
    let transport = WindowedTransport::new(&fixture, std::time::Duration::from_millis(40));
    let coordinator = MajorityAppendCoordinator::new(
        fixture.services[&fixture.owner].clone(),
        fixture.control.clone(),
        Arc::new(transport.clone()),
    );
    const SUBMISSIONS: u64 = 64;
    let mut handles = Vec::new();
    for submission in 0..SUBMISSIONS {
        let coordinator = coordinator.clone();
        let request = windowed_request(&fixture, submission + 1);
        handles.push(tokio::spawn(async move {
            coordinator.append_batch(vec![request]).await
        }));
    }
    let mut positions = Vec::new();
    for handle in handles {
        let results = handle.await.unwrap().unwrap();
        assert_eq!(results.len(), 1, "each submission got its own slice");
        positions.push(results[0].position.value());
    }
    // Every submission landed a distinct contiguous position; the
    // follower saw fewer round heads than submissions.
    positions.sort_unstable();
    assert_eq!(positions, (1..=SUBMISSIONS).collect::<Vec<u64>>());
    for follower in fixture.non_owner_nodes() {
        // 64 submissions queue ahead of the 16-round in-flight window;
        // drivers merge them into far fewer quorum rounds.
        assert!(
            transport.received(&follower).len() < SUBMISSIONS as usize / 4,
            "submissions should merge into far fewer quorum rounds"
        );
    }
}

#[tokio::test]
async fn replica_sends_pipeline_up_to_the_per_follower_window() {
    let fixture = Fixture::new().await;
    let transport = WindowedTransport::new(&fixture, std::time::Duration::from_millis(30));
    let coordinator = MajorityAppendCoordinator::new(
        fixture.services[&fixture.owner].clone(),
        fixture.control.clone(),
        Arc::new(transport.clone()),
    );
    let mut handles = Vec::new();
    for index in 0..8_u64 {
        let coordinator = coordinator.clone();
        let request = windowed_request(&fixture, index);
        handles.push(tokio::spawn(async move {
            coordinator.append_batch(vec![request]).await
        }));
    }
    for handle in handles {
        handle.await.unwrap().unwrap();
    }
    for follower in fixture.non_owner_nodes() {
        let max = transport.max_in_flight(&follower);
        assert!(max > 1, "lane {follower} never overlapped sends");
        assert!(max <= 4, "lane {follower} exceeded the send window: {max}");
        let received = transport.received(&follower);
        // Group commit merges queued submissions into fewer rounds, so
        // the follower may see fewer than 8 round heads — what must hold
        // is that heads start at 1 and arrive strictly in mint order.
        assert!(!received.is_empty());
        assert_eq!(received[0], 1);
        for pair in received.windows(2) {
            assert!(
                pair[1] > pair[0],
                "follower {follower} observed positions out of mint order: {received:?}"
            );
        }
    }
}

#[tokio::test]
async fn poisoned_submission_is_isolated_on_solo_rerun() {
    // A merged round that fails on a non-retryable, per-frame refusal
    // re-runs each submission alone: the bad one keeps failing while
    // the innocent ones land.
    let fixture = Fixture::new().await;
    let transport = WindowedTransport::new(&fixture, std::time::Duration::from_millis(40));
    let coordinator = MajorityAppendCoordinator::new(
        fixture.services[&fixture.owner].clone(),
        fixture.control.clone(),
        Arc::new(transport.clone()),
    );
    // Land writer W sequence 1 normally, then a conflicting submission
    // replays the same writer+sequence with different frame bytes.
    let mut poison = windowed_request(&fixture, 1);
    let writer = poison.identity.writer_session_id;
    let landed = coordinator
        .append_batch(vec![poison.clone()])
        .await
        .unwrap();
    assert_eq!(landed.len(), 1);
    let record = StoredRecord {
        message_id: Uuid::from_u128(999_999),
        producer_id: writer,
        producer_sequence: 1,
        event_time_ns: 1,
        ingest_time_ns: 2,
        key: b"customer-1".to_vec(),
        payload: b"forged".to_vec(),
        metadata: BTreeMap::new(),
    };
    poison.frame = encode_record(&record).unwrap().into();
    poison.cursor = "forged-cursor".to_owned();
    let innocent = windowed_request(&fixture, 2);
    let (poisoned, clean) = tokio::join!(
        coordinator.append_batch(vec![poison]),
        coordinator.append_batch(vec![innocent]),
    );
    assert!(poisoned.is_err(), "the conflicting submission must fail");
    let results = clean.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].position.value(), 2);
}

#[tokio::test]
async fn submissions_over_the_byte_budget_split_rounds() {
    // Submissions that together exceed MAX_GROUP_ROUND_BYTES (16 MiB)
    // must not merge into one quorum round.
    let fixture = Fixture::new().await;
    let transport = WindowedTransport::new(&fixture, std::time::Duration::from_millis(40));
    let coordinator = MajorityAppendCoordinator::new(
        fixture.services[&fixture.owner].clone(),
        fixture.control.clone(),
        Arc::new(transport.clone()),
    );
    let big = |index: u64| {
        let mut request = windowed_request(&fixture, index);
        let record = StoredRecord {
            message_id: Uuid::from_u128(3_000 + index as u128),
            producer_id: request.identity.writer_session_id,
            producer_sequence: 1,
            event_time_ns: index as i64,
            ingest_time_ns: index as i64 + 1,
            key: b"customer-1".to_vec(),
            payload: vec![b'x'; 9 * 1024 * 1024],
            metadata: BTreeMap::new(),
        };
        request.frame = encode_record(&record).unwrap().into();
        request
    };
    let (first, second) = tokio::join!(
        coordinator.append_batch(vec![big(1)]),
        coordinator.append_batch(vec![big(2)]),
    );
    first.unwrap();
    second.unwrap();
    for follower in fixture.non_owner_nodes() {
        assert_eq!(
            transport.received(&follower).len(),
            2,
            "submissions over the 16 MiB byte budget must run separately"
        );
    }
}

/// Drops one designated batch send to one follower, then records every
/// repair `append_batch` call so the test can count them.
struct DropOnceTransport {
    services: Arc<BTreeMap<StorageNodeId, Arc<ReplicaAppendService>>>,
    dropped: Arc<std::sync::Mutex<std::collections::HashSet<u64>>>,
    repair_batches: Arc<std::sync::Mutex<Vec<usize>>>,
    single_appends: Arc<std::sync::Mutex<usize>>,
    drop_at: u64,
    victim: StorageNodeId,
}

impl DropOnceTransport {
    fn new(fixture: &Fixture, victim: StorageNodeId, drop_at: u64) -> Self {
        Self {
            services: Arc::new(fixture.services.clone()),
            dropped: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
            repair_batches: Arc::new(std::sync::Mutex::new(Vec::new())),
            single_appends: Arc::new(std::sync::Mutex::new(0)),
            drop_at,
            victim,
        }
    }
}

#[async_trait]
impl ReplicaTransport for DropOnceTransport {
    async fn append(
        &self,
        replica: &StorageNodeId,
        request: ReplicaAppendRequest,
    ) -> Result<ReplicaAppendAccepted, ReplicaTransportError> {
        *self.single_appends.lock().unwrap() += 1;
        self.services[replica]
            .append(request)
            .await
            .map_err(ReplicaTransportError::replica)
    }

    async fn append_batch(
        &self,
        replica: &StorageNodeId,
        request: whitewater::active_range::ReplicaBatchAppendRequest,
    ) -> Result<Vec<ReplicaAppendAccepted>, ReplicaTransportError> {
        let first = request
            .frames
            .first()
            .and_then(|frame| frame.expected_position)
            .map(|p| p.value())
            .unwrap_or(0);
        if replica == &self.victim
            && first == self.drop_at
            && self.dropped.lock().unwrap().insert(first)
        {
            // Pretend the send landed; the follower never saw it.
            return Ok(request
                .frames
                .iter()
                .map(|frame| ReplicaAppendAccepted {
                    position: frame.expected_position.unwrap(),
                    message_id: uuid::Uuid::new_v4(),
                    cursor: frame.cursor.clone(),
                    frame_digest: [0; 32],
                    deduplicated: false,
                    committed: false,
                    durable: true,
                })
                .collect());
        }
        if replica == &self.victim {
            self.repair_batches
                .lock()
                .unwrap()
                .push(request.frames.len());
        }
        self.services[replica]
            .import_committed_batch(request)
            .await
            .map_err(ReplicaTransportError::replica)
    }

    async fn commit(
        &self,
        replica: &StorageNodeId,
        request: ReplicaCommitRequest,
    ) -> Result<ReplicaCommitAccepted, ReplicaTransportError> {
        self.services[replica]
            .commit(request)
            .await
            .map_err(ReplicaTransportError::replica)
    }

    async fn discard_pending(
        &self,
        replica: &StorageNodeId,
        request: ReplicaDiscardPendingRequest,
    ) -> Result<ReplicaDiscardPendingResponse, ReplicaTransportError> {
        self.services[replica]
            .discard_pending(request)
            .await
            .map_err(ReplicaTransportError::replica)
    }
}

#[tokio::test]
async fn dropped_round_is_repaired_with_batch_sends_not_single_appends() {
    let fixture = Fixture::new().await;
    let victim = fixture.non_owner_nodes()[0].clone();
    // Round 1 lands positions 1..2 for the victim; pretend round 2's
    // send (position 2) was dropped so round 3 hits a PositionGap and
    // fill_replica_gap re-drives it.
    let transport = DropOnceTransport::new(&fixture, victim.clone(), 2);
    let repair_batches = transport.repair_batches.clone();
    let single_appends = transport.single_appends.clone();
    let coordinator = MajorityAppendCoordinator::new(
        fixture.services[&fixture.owner].clone(),
        fixture.control.clone(),
        Arc::new(transport),
    );
    coordinator
        .append_batch(vec![
            windowed_request(&fixture, 1),
            windowed_request(&fixture, 2),
            windowed_request(&fixture, 3),
        ])
        .await
        .expect("round with the dropped send still commits");
    coordinator
        .append_batch(vec![windowed_request(&fixture, 4)])
        .await
        .expect("the next round repairs the gap and commits");
    let batches = repair_batches.lock().unwrap().clone();
    assert_eq!(
        *single_appends.lock().unwrap(),
        0,
        "gap repair must not fall back to per-frame appends"
    );
    // batches[0] is the merged round (3 frames); the repair is the
    // single follow-up batch carrying only the dropped position.
    assert_eq!(
        batches.as_slice(),
        &[3, 1],
        "the dropped round is repaired with ONE batch send, got {batches:?}"
    );
}

#[tokio::test]
async fn cancelled_driver_still_completes_the_group_round() {
    // Cancelling the driving submission's caller mid-round must not
    // abort the quorum round: every other merged submission still gets
    // its reply and lands contiguous positions.
    let fixture = Fixture::new().await;
    let transport = WindowedTransport::new(&fixture, std::time::Duration::from_millis(60));
    let coordinator = MajorityAppendCoordinator::new(
        fixture.services[&fixture.owner].clone(),
        fixture.control.clone(),
        Arc::new(transport.clone()),
    );
    // Driver: cancelled once the send delay has it inside the round.
    let driver_request = windowed_request(&fixture, 1);
    let driver = {
        let coordinator = coordinator.clone();
        tokio::spawn(async move { coordinator.append_batch(vec![driver_request]).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    // Passengers queue behind the append lock and merge into the round.
    let passenger_requests: Vec<_> = (2..=4_u64)
        .map(|index| windowed_request(&fixture, index))
        .collect();
    let mut passengers = Vec::new();
    for request in passenger_requests {
        let coordinator = coordinator.clone();
        passengers.push(tokio::spawn(async move {
            coordinator.append_batch(vec![request]).await
        }));
    }
    driver.abort();
    let mut positions = Vec::new();
    for handle in passengers {
        let results = handle.await.unwrap().unwrap();
        positions.push(results[0].position.value());
    }
    positions.sort_unstable();
    assert!(positions.len() >= 3);
    for pair in positions.windows(2) {
        assert_eq!(pair[1], pair[0] + 1, "merged positions stay contiguous");
    }
}

#[tokio::test]
async fn pending_effect_submissions_never_merge() {
    // A pending-effect submission always runs alone: merging it into a
    // group round would let an unrelated batch ride the effect fence.
    let fixture = Fixture::new().await;
    let transport = WindowedTransport::new(&fixture, std::time::Duration::from_millis(40));
    let coordinator = MajorityAppendCoordinator::new(
        fixture.services[&fixture.owner].clone(),
        fixture.control.clone(),
        Arc::new(transport.clone()),
    );
    let mut effect = windowed_request(&fixture, 1);
    pending_request(&mut effect, Uuid::from_u128(700), Uuid::from_u128(701));
    let plain = windowed_request(&fixture, 2);
    let (effect_results, plain_results) = tokio::join!(
        coordinator.append_batch(vec![effect]),
        coordinator.append_batch(vec![plain]),
    );
    effect_results.unwrap();
    // The effect holds the tail, so the plain submission is refused
    // retryable — proof it never merged around the effect's fence.
    let error = plain_results.unwrap_err();
    assert_eq!(error.code, MajorityAppendErrorCode::PendingEffectOccupied);
    assert!(error.retryable);
    for follower in fixture.non_owner_nodes() {
        assert_eq!(
            transport.received(&follower).len(),
            1,
            "only the pending-effect submission should land a round"
        );
    }
}

#[tokio::test]
async fn pending_effect_send_fences_the_whole_follower_lane() {
    let fixture = Fixture::new().await;
    let mut transport = WindowedTransport::new(&fixture, std::time::Duration::from_millis(5));
    transport.pause_position = Some(1);
    let pause_entered = transport.pause_entered.clone();
    let pause_release = transport.pause_release.clone();
    let coordinator = MajorityAppendCoordinator::new(
        fixture.services[&fixture.owner].clone(),
        fixture.control.clone(),
        Arc::new(transport.clone()),
    );

    // First round carries the pending-effect tail hold; its send pauses
    // inside the transport while holding every lane permit.
    let mut pending = windowed_request(&fixture, 1);
    pending_request(&mut pending, Uuid::from_u128(900), Uuid::from_u128(901));
    let pending_send = {
        let coordinator = coordinator.clone();
        tokio::spawn(async move { coordinator.append_batch(vec![pending]).await })
    };
    pause_entered.notified().await;

    // A second round on the same effect may append locally, but its send
    // must queue behind the fenced lane — never overlap the pending one.
    let mut second = windowed_request(&fixture, 2);
    pending_request(&mut second, Uuid::from_u128(900), Uuid::from_u128(901));
    let second_send = {
        let coordinator = coordinator.clone();
        tokio::spawn(async move { coordinator.append_batch(vec![second]).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    for follower in fixture.non_owner_nodes() {
        assert_eq!(
            transport.received(&follower).len(),
            1,
            "a second send overlapped the fenced pending-effect lane"
        );
    }
    pause_release.notify_waiters();
    pending_send.await.unwrap().unwrap();
    second_send.await.unwrap().unwrap();
    for follower in fixture.non_owner_nodes() {
        assert_eq!(transport.received(&follower).len(), 2);
    }
}
