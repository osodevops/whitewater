use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use finnstream::{
    active_range::{
        AppendIdentity, CommitPosition, DrainCommandAuthority, DrainMoveDriver,
        FollowerMoveControl, FollowerMoveCopyResult, FollowerMoveExecutor, LocalDrainDriver,
        MajorityAppendCoordinator, OwnerMoveControl, OwnerMoveEvidence, RangeGeneration,
        RangePosition, ReplicaAppendAccepted, ReplicaAppendRequest, ReplicaAppendService,
        ReplicaCommitAccepted, ReplicaCommitRequest, ReplicaTransport, ReplicaTransportError,
        StorageDrainExecutor, StorageDrainStatus, StorageDrainSupervisor, StorageNodeId,
    },
    codec::encode_record,
    control::{Command, ControlController, RangeMovePlan, RangeOwnerMovePlan, ReaderStart},
    domain::StoredRecord,
    storage::{FileLogStore, LogStore},
};
use tempfile::TempDir;
use uuid::Uuid;

fn node(value: &str) -> StorageNodeId {
    StorageNodeId::try_new(value).unwrap()
}

struct UnusedTransport;

#[async_trait]
impl ReplicaTransport for UnusedTransport {
    async fn append(
        &self,
        _replica: &StorageNodeId,
        _request: ReplicaAppendRequest,
    ) -> Result<ReplicaAppendAccepted, ReplicaTransportError> {
        Err(ReplicaTransportError {
            message: "transport is not used during drain".to_owned(),
            retryable: false,
        })
    }

    async fn commit(
        &self,
        _replica: &StorageNodeId,
        _request: ReplicaCommitRequest,
    ) -> Result<ReplicaCommitAccepted, ReplicaTransportError> {
        Err(ReplicaTransportError {
            message: "transport is not used during drain".to_owned(),
            retryable: false,
        })
    }
}

struct LocalDrainAuthority(Arc<ControlController>);

#[async_trait]
impl DrainCommandAuthority for LocalDrainAuthority {
    async fn execute(&self, command: Command) -> Result<serde_json::Value, String> {
        self.0
            .execute_commands(vec![command])
            .await
            .map(|execution| execution.results[0].data.clone())
            .map_err(|error| error.to_string())
    }
}

struct LocalMoveControl(Arc<ControlController>);

#[async_trait]
impl FollowerMoveControl for LocalMoveControl {
    async fn record_ready(
        &self,
        feed: &str,
        plan: &RangeMovePlan,
        copied: &FollowerMoveCopyResult,
    ) -> Result<(), String> {
        self.0
            .execute_commands(vec![Command::RecordFollowerMoveCatchUp {
                feed: feed.to_owned(),
                plan_id: plan.plan_id,
                source_commit: copied.source_commit,
                target_commit: copied.target_commit,
                checksum_verified: copied.ready,
            }])
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    async fn activate(&self, feed: &str, plan: &RangeMovePlan) -> Result<(), String> {
        self.0
            .execute_commands(vec![Command::ActivateFollowerMove {
                feed: feed.to_owned(),
                plan_id: plan.plan_id,
            }])
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

#[async_trait]
impl OwnerMoveControl for LocalMoveControl {
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

struct Fixture {
    _directory: TempDir,
    control: Arc<ControlController>,
    services: BTreeMap<StorageNodeId, Arc<ReplicaAppendService>>,
    executor: StorageDrainExecutor,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_spare_node(true).await
    }

    async fn with_spare_node(spare: bool) -> Self {
        let directory = TempDir::new().unwrap();
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(directory.path().join("legacy")).unwrap());
        let control = Arc::new(
            ControlController::open_with_storage_nodes(
                directory.path().join("catalog.json"),
                store,
                vec![node("storage-1"), node("storage-2"), node("storage-3")],
            )
            .unwrap(),
        );
        if spare {
            control
                .execute("REGISTER STORAGE NODE storage-4 AT http://storage-4:7070;")
                .await
                .unwrap();
        }
        let nodes = if spare {
            vec!["storage-1", "storage-2", "storage-3", "storage-4"]
        } else {
            vec!["storage-1", "storage-2", "storage-3"]
        };
        let services = nodes
            .into_iter()
            .map(|value| {
                let node = node(value);
                let service = Arc::new(ReplicaAppendService::new(
                    directory.path().join(value),
                    node.clone(),
                    control.clone(),
                ));
                (node, service)
            })
            .collect::<BTreeMap<_, _>>();
        let executor = Self::executor(control.clone());
        Self {
            _directory: directory,
            control,
            services,
            executor,
        }
    }

    fn executor(control: Arc<ControlController>) -> StorageDrainExecutor {
        StorageDrainExecutor::new(
            control.clone(),
            Arc::new(LocalDrainAuthority(control.clone())),
            Arc::new(LocalMoveControl(control.clone())),
            Arc::new(LocalMoveControl(control.clone())),
            Arc::new(UnusedTransport),
        )
    }

    fn supervisor(&self) -> StorageDrainSupervisor {
        StorageDrainSupervisor::new(
            self.control.clone(),
            Arc::new(LocalDrainDriver::new(
                Self::executor(self.control.clone()),
                Arc::new(self.services.clone()),
            )),
        )
    }

    async fn create_feed_and_subscription(&self) {
        self.control
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
        self.control
            .execute_commands(vec![Command::CreateSubscription {
                name: "orders.billing".to_owned(),
                feed: "orders.events".to_owned(),
                start: ReaderStart::Beginning,
            }])
            .await
            .unwrap();
    }

    async fn seed_committed(&self, feed_id: Uuid) {
        let assignment = self.control.active_range_assignment(feed_id).await.unwrap();
        for sequence in 1..=3_u64 {
            let record = StoredRecord {
                message_id: Uuid::from_u128(9_000 + sequence as u128),
                producer_id: Uuid::from_u128(900),
                producer_sequence: sequence,
                event_time_ns: sequence as i64,
                ingest_time_ns: sequence as i64,
                key: b"order-1".to_vec(),
                payload: format!("event-{sequence}").into_bytes(),
                metadata: BTreeMap::new(),
            };
            let request = ReplicaAppendRequest {
                feed_id,
                range_id: assignment.range_id,
                generation: RangeGeneration::new(1),
                ownership_epoch: assignment.ownership_epoch,
                append_owner: assignment.owner.clone(),
                expected_position: RangePosition::new(sequence),
                identity: AppendIdentity {
                    writer_session_id: record.producer_id,
                    writer_epoch: 1,
                    sequence,
                },
                cursor: format!("cursor-{sequence}"),
                frame_base64: STANDARD.encode(encode_record(&record).unwrap()),
            };
            for replica in assignment.replicas.iter() {
                let service = self.services[replica].clone();
                let accepted = service.append(request.clone()).await.unwrap();
                service
                    .commit(ReplicaCommitRequest {
                        feed_id,
                        range_id: assignment.range_id,
                        generation: assignment.generation,
                        ownership_epoch: assignment.ownership_epoch,
                        append_owner: assignment.owner.clone(),
                        commit_position: CommitPosition::new(sequence),
                        frame_digest: accepted.frame_digest,
                    })
                    .await
                    .unwrap();
            }
        }
    }

    async fn feed_id(&self) -> Uuid {
        self.control
            .active_feed_by_name("orders.events")
            .await
            .unwrap()
            .feed_id
    }
}

#[tokio::test]
async fn drain_executor_vacates_a_follower_and_unblocks_retire() {
    let fixture = Fixture::new().await;
    fixture.create_feed_and_subscription().await;
    let feed_id = fixture.feed_id().await;
    fixture.seed_committed(feed_id).await;
    let original = fixture
        .control
        .active_range_assignment(feed_id)
        .await
        .unwrap();
    let drained = original
        .replicas
        .iter()
        .find(|node| **node != original.owner)
        .unwrap()
        .clone();

    fixture
        .control
        .execute(&format!("DRAIN STORAGE NODE {drained};"))
        .await
        .unwrap();
    let report = fixture
        .executor
        .drain(&drained, &fixture.services)
        .await
        .unwrap();
    assert!(report.completed_moves >= 1);
    assert!(report.unplannable.is_empty());
    assert!(report.ready_to_retire);

    let current = fixture
        .control
        .active_range_assignment(feed_id)
        .await
        .unwrap();
    assert!(!current.replicas.contains(&drained));
    assert_eq!(current.owner, original.owner);
    let replacement = current
        .replicas
        .iter()
        .find(|node| !original.replicas.contains(node))
        .unwrap()
        .clone();
    assert_eq!(
        fixture.services[&replacement]
            .read_committed(feed_id, None, 10)
            .await
            .unwrap()
            .len(),
        3
    );
    fixture
        .control
        .execute(&format!("RETIRE STORAGE NODE {drained};"))
        .await
        .unwrap();
}

#[tokio::test]
async fn drain_executor_vacates_an_owner_through_ordered_moves() {
    let fixture = Fixture::new().await;
    fixture.create_feed_and_subscription().await;
    let feed_id = fixture.feed_id().await;
    fixture.seed_committed(feed_id).await;
    let original = fixture
        .control
        .active_range_assignment(feed_id)
        .await
        .unwrap();
    let drained = original.owner.clone();

    fixture
        .control
        .execute(&format!("DRAIN STORAGE NODE {drained};"))
        .await
        .unwrap();
    let report = fixture
        .executor
        .drain(&drained, &fixture.services)
        .await
        .unwrap();
    // Owner move off the drained Node, then follower move replacing it.
    assert!(report.completed_moves >= 2);
    assert!(report.ready_to_retire);

    let current = fixture
        .control
        .active_range_assignment(feed_id)
        .await
        .unwrap();
    assert!(!current.replicas.contains(&drained));
    assert_ne!(current.owner, drained);
    assert_eq!(current.replicas.as_array().len(), 3);
    fixture
        .control
        .execute(&format!("RETIRE STORAGE NODE {drained};"))
        .await
        .unwrap();
}

#[tokio::test]
async fn drain_executor_vacates_subscription_progress_references() {
    let fixture = Fixture::new().await;
    fixture.create_feed_and_subscription().await;
    let subscription = fixture
        .control
        .active_subscription_by_name("orders.billing")
        .await
        .unwrap();
    let progress = fixture
        .control
        .active_subscription_progress_assignment_by_id(subscription.subscription_id)
        .await
        .unwrap();
    let drained = progress.owner.clone();

    fixture
        .control
        .execute(&format!("DRAIN STORAGE NODE {drained};"))
        .await
        .unwrap();
    let report = fixture
        .executor
        .drain(&drained, &fixture.services)
        .await
        .unwrap();
    assert!(report.ready_to_retire);

    let current = fixture
        .control
        .active_subscription_progress_assignment_by_id(subscription.subscription_id)
        .await
        .unwrap();
    assert!(!current.replicas.contains(&drained));
    assert_ne!(current.owner, drained);
    fixture
        .control
        .execute(&format!("RETIRE STORAGE NODE {drained};"))
        .await
        .unwrap();
}

/// Fails `apply` the first `failures` calls, then delegates to the inner
/// driver; proves a mid-drain failure leaves a retriable plan instead of a
/// wedged Node.
struct FlakyDriver {
    inner: LocalDrainDriver,
    failures: usize,
    calls: AtomicUsize,
}

#[async_trait]
impl DrainMoveDriver for FlakyDriver {
    async fn apply(&self, command: Command) -> Result<(), String> {
        if self.calls.fetch_add(1, Ordering::SeqCst) < self.failures {
            return Err("injected movement failure".to_owned());
        }
        self.inner.apply(command).await
    }
}

#[tokio::test]
async fn supervisor_drains_marked_nodes_and_reports_safe_to_remove() {
    let fixture = Fixture::new().await;
    fixture.create_feed_and_subscription().await;
    let feed_id = fixture.feed_id().await;
    fixture.seed_committed(feed_id).await;
    let original = fixture
        .control
        .active_range_assignment(feed_id)
        .await
        .unwrap();
    let drained = original
        .replicas
        .iter()
        .find(|node| **node != original.owner)
        .unwrap()
        .clone();
    fixture
        .control
        .execute(&format!("DRAIN STORAGE NODE {drained};"))
        .await
        .unwrap();

    let outcomes = fixture.supervisor().tick().await;
    assert_eq!(outcomes.len(), 1);
    let StorageDrainStatus::Drained(report) = &outcomes[0].status else {
        panic!("expected a drain report: {:?}", outcomes[0].status)
    };
    assert_eq!(report.node, drained);
    assert!(report.completed_moves >= 1);
    assert!(report.ready_to_retire);

    let current = fixture
        .control
        .active_range_assignment(feed_id)
        .await
        .unwrap();
    assert!(!current.replicas.contains(&drained));
    fixture
        .control
        .execute(&format!("RETIRE STORAGE NODE {drained};"))
        .await
        .unwrap();
    assert!(fixture.control.draining_storage_nodes().await.is_empty());
}

#[tokio::test]
async fn supervisor_drains_owner_and_progress_references_together() {
    let fixture = Fixture::new().await;
    fixture.create_feed_and_subscription().await;
    let feed_id = fixture.feed_id().await;
    fixture.seed_committed(feed_id).await;
    let drained = fixture
        .control
        .active_range_assignment(feed_id)
        .await
        .unwrap()
        .owner;
    fixture
        .control
        .execute(&format!("DRAIN STORAGE NODE {drained};"))
        .await
        .unwrap();

    let outcomes = fixture.supervisor().tick().await;
    let StorageDrainStatus::Drained(report) = &outcomes[0].status else {
        panic!("expected a drain report: {:?}", outcomes[0].status)
    };
    assert!(report.completed_moves >= 2);
    assert!(report.ready_to_retire);
    let current = fixture
        .control
        .active_range_assignment(feed_id)
        .await
        .unwrap();
    assert!(!current.replicas.contains(&drained));
    assert_ne!(current.owner, drained);
}

#[tokio::test]
async fn supervisor_reports_unplannable_when_no_replacement_exists() {
    let fixture = Fixture::with_spare_node(false).await;
    fixture.create_feed_and_subscription().await;
    let feed_id = fixture.feed_id().await;
    let drained = fixture
        .control
        .active_range_assignment(feed_id)
        .await
        .unwrap()
        .owner;
    fixture
        .control
        .execute(&format!("DRAIN STORAGE NODE {drained};"))
        .await
        .unwrap();

    let outcomes = fixture.supervisor().tick().await;
    let StorageDrainStatus::Drained(report) = &outcomes[0].status else {
        panic!("expected a drain report: {:?}", outcomes[0].status)
    };
    // Ownership can always move onto a surviving replica; the follower and
    // progress-replica replacements that need spare capacity stay unplannable.
    assert!(!report.unplannable.is_empty());
    assert!(!report.ready_to_retire);
    fixture
        .control
        .execute(&format!("RETIRE STORAGE NODE {drained};"))
        .await
        .unwrap_err();
}

#[tokio::test]
async fn supervisor_retries_after_driver_failure() {
    let fixture = Fixture::new().await;
    fixture.create_feed_and_subscription().await;
    let feed_id = fixture.feed_id().await;
    fixture.seed_committed(feed_id).await;
    let drained = fixture
        .control
        .active_range_assignment(feed_id)
        .await
        .unwrap()
        .owner;
    fixture
        .control
        .execute(&format!("DRAIN STORAGE NODE {drained};"))
        .await
        .unwrap();

    let supervisor = StorageDrainSupervisor::new(
        fixture.control.clone(),
        Arc::new(FlakyDriver {
            inner: LocalDrainDriver::new(
                Fixture::executor(fixture.control.clone()),
                Arc::new(fixture.services.clone()),
            ),
            failures: 1,
            calls: AtomicUsize::new(0),
        }),
    );
    let first = supervisor.tick().await;
    assert!(matches!(first[0].status, StorageDrainStatus::Failed(_)));
    let second = supervisor.tick().await;
    let StorageDrainStatus::Drained(report) = &second[0].status else {
        panic!("expected a drain report: {:?}", second[0].status)
    };
    assert!(report.ready_to_retire);
}

#[tokio::test]
async fn supervisor_throttles_movement_while_the_budget_is_exhausted() {
    let fixture = Fixture::new().await;
    fixture.create_feed_and_subscription().await;
    let feed_id = fixture.feed_id().await;
    fixture.seed_committed(feed_id).await;
    let original = fixture
        .control
        .active_range_assignment(feed_id)
        .await
        .unwrap();

    // A pending plan that was prepared but never finalized (for example a
    // driver that crashed mid-move) occupies the movement budget.
    let pending_removed = original
        .replicas
        .iter()
        .find(|node| **node != original.owner)
        .unwrap()
        .clone();
    let pending_replacement = fixture
        .services
        .keys()
        .find(|node| !original.replicas.contains(node))
        .unwrap()
        .clone();
    let prepared = fixture
        .control
        .execute_commands(vec![Command::PrepareFollowerMove {
            feed: "orders.events".to_owned(),
            range_id: original.range_id,
            removed_replica: pending_removed,
            replacement_replica: pending_replacement.clone(),
        }])
        .await
        .unwrap();
    let pending_plan: RangeMovePlan =
        serde_json::from_value(prepared.results[0].data.clone()).unwrap();
    assert_eq!(fixture.control.pending_move_plan_count().await, 1);

    let drained = original.owner.clone();
    fixture
        .control
        .execute(&format!("DRAIN STORAGE NODE {drained};"))
        .await
        .unwrap();
    let supervisor = fixture.supervisor().with_move_budget(1);
    let outcomes = supervisor.tick().await;
    assert_eq!(outcomes.len(), 1);
    assert!(matches!(
        outcomes[0].status,
        StorageDrainStatus::Throttled { pending_plans: 1 }
    ));

    // Completing the wedged plan frees the budget; the drain proceeds on the
    // next tick without manual re-planning.
    let coordinator = Arc::new(MajorityAppendCoordinator::new(
        fixture.services[&pending_plan.source_assignment.owner].clone(),
        fixture.control.clone(),
        Arc::new(UnusedTransport),
    ));
    FollowerMoveExecutor::new(
        fixture.control.clone(),
        coordinator,
        fixture.services[&pending_plan.source_assignment.owner].clone(),
        fixture.services[&pending_replacement].clone(),
    )
    .finalize(
        &LocalMoveControl(fixture.control.clone()),
        "orders.events",
        &pending_plan,
        32,
    )
    .await
    .unwrap();
    assert_eq!(fixture.control.pending_move_plan_count().await, 0);

    let outcomes = supervisor.tick().await;
    let StorageDrainStatus::Drained(report) = &outcomes[0].status else {
        panic!("expected a drain report: {:?}", outcomes[0].status)
    };
    assert!(report.ready_to_retire);
}
