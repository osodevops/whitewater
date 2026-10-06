use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use finnstream::{
    active_range::{RangeId, ReplicaSet, StorageNodeId},
    control::ControlController,
    reader::{
        FjallReaderProgressStore, FjallSubscriptionProgressReplica, ReaderDeliveryMutation,
        ReaderDeliveryReceipt, ReaderPacingController, ReaderPressureSample, ReaderProgressEngine,
        ReaderProgressError, SubscriptionCommitEvidence, SubscriptionPrepareVote,
        SubscriptionProgressAssignment, SubscriptionProgressCoordinator, SubscriptionProgressError,
        SubscriptionProgressMutation, SubscriptionProgressTransport,
    },
    storage::{FileLogStore, LogStore},
};
use tempfile::TempDir;
use uuid::Uuid;

struct TestProgressTransport {
    stores: BTreeMap<StorageNodeId, FjallSubscriptionProgressReplica>,
    prepare_down: Mutex<BTreeSet<StorageNodeId>>,
    commit_down: Mutex<BTreeSet<StorageNodeId>>,
    corrupt_vote: Mutex<Option<StorageNodeId>>,
}

impl TestProgressTransport {
    fn new(directories: &[TempDir; 3], nodes: &[StorageNodeId; 3]) -> Self {
        Self {
            stores: nodes
                .iter()
                .cloned()
                .zip(directories.iter().map(|directory| {
                    FjallSubscriptionProgressReplica::open(directory.path()).unwrap()
                }))
                .collect(),
            prepare_down: Mutex::new(BTreeSet::new()),
            commit_down: Mutex::new(BTreeSet::new()),
            corrupt_vote: Mutex::new(None),
        }
    }
}

impl SubscriptionProgressTransport for TestProgressTransport {
    fn prepare(
        &self,
        node: &StorageNodeId,
        mutation: SubscriptionProgressMutation,
    ) -> Result<SubscriptionPrepareVote, SubscriptionProgressError> {
        if self.prepare_down.lock().unwrap().contains(node) {
            return Err(SubscriptionProgressError::Unavailable);
        }
        let store = self
            .stores
            .get(node)
            .ok_or(SubscriptionProgressError::InvalidAssignment)?;
        let mut vote = store.prepare(mutation)?;
        if self.corrupt_vote.lock().unwrap().as_ref() == Some(node) {
            vote.digest[0] ^= 1;
        }
        Ok(vote)
    }

    fn commit(
        &self,
        node: &StorageNodeId,
        evidence: SubscriptionCommitEvidence,
    ) -> Result<SubscriptionProgressMutation, SubscriptionProgressError> {
        if self.commit_down.lock().unwrap().contains(node) {
            return Err(SubscriptionProgressError::Unavailable);
        }
        self.stores
            .get(node)
            .ok_or(SubscriptionProgressError::InvalidAssignment)?
            .commit_with_quorum(evidence)
    }
}

fn progress_nodes() -> [StorageNodeId; 3] {
    ["storage-a", "storage-b", "storage-c"].map(|name| StorageNodeId::try_new(name).unwrap())
}

#[tokio::test]
async fn subscription_coordinator_uses_private_catalog_placement_and_refuses_legacy_missing_placement(
) {
    let directory = TempDir::new().unwrap();
    let store: Arc<dyn LogStore> =
        Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
    let nodes = progress_nodes();
    let controller = ControlController::open_with_storage_nodes(
        directory.path().join("catalog.json"),
        store,
        nodes.to_vec(),
    )
    .unwrap();
    controller.execute("CREATE SPACE orders; CREATE FEED orders.events; CREATE SUBSCRIPTION orders.billing FROM orders.events;").await.unwrap();
    let subscription = controller
        .active_subscription_by_name("orders.billing")
        .await
        .unwrap();
    let dirs = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let transport = Arc::new(TestProgressTransport::new(&dirs, &nodes));
    let coordinator = SubscriptionProgressCoordinator::for_subscription(
        &controller,
        subscription.subscription_id,
        transport.clone(),
    )
    .await
    .unwrap();
    let mutation = SubscriptionProgressMutation {
        subscription_id: subscription.subscription_id,
        feed_id: subscription.feed_id,
        ownership_epoch: 1,
        sequence: 1,
        request_id: Uuid::from_u128(500),
        expected_cursor: None,
        cursor: "rf1_page".to_owned(),
        positions: BTreeMap::from([(
            RangeId::from_uuid(Uuid::from_u128(501)),
            "record".to_owned(),
        )]),
    };
    let committed = tokio::task::spawn_blocking(move || coordinator.apply(mutation))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(committed.len(), 3);

    let mut legacy: serde_json::Value =
        serde_json::from_slice(&controller.snapshot_bytes().await.unwrap()).unwrap();
    legacy
        .as_object_mut()
        .unwrap()
        .remove("subscription_progress_assignments");
    let legacy_dir = TempDir::new().unwrap();
    let legacy_store: Arc<dyn LogStore> =
        Arc::new(FileLogStore::open(legacy_dir.path().join("data")).unwrap());
    let legacy_controller = ControlController::open_with_storage_nodes(
        legacy_dir.path().join("catalog.json"),
        legacy_store,
        nodes.to_vec(),
    )
    .unwrap();
    legacy_controller
        .install_snapshot_bytes(&serde_json::to_vec(&legacy).unwrap())
        .await
        .unwrap();
    assert!(matches!(
        SubscriptionProgressCoordinator::for_subscription(
            &legacy_controller,
            subscription.subscription_id,
            transport,
        )
        .await,
        Err(SubscriptionProgressError::InvalidAssignment)
    ));
}

#[test]
fn subscription_progress_coordinator_requires_matching_prepare_and_commit_majorities() {
    let dirs = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = progress_nodes();
    let transport = Arc::new(TestProgressTransport::new(&dirs, &nodes));
    let subscription_id = Uuid::from_u128(201);
    let assignment = SubscriptionProgressAssignment::try_new(
        subscription_id,
        nodes[0].clone(),
        ReplicaSet::try_new(nodes.clone()).unwrap(),
        1,
    )
    .unwrap();
    let coordinator = SubscriptionProgressCoordinator::new(assignment, transport.clone());
    let mutation = SubscriptionProgressMutation {
        subscription_id,
        feed_id: Uuid::from_u128(202),
        ownership_epoch: 1,
        sequence: 1,
        request_id: Uuid::from_u128(203),
        expected_cursor: None,
        cursor: "rf1_a".to_owned(),
        positions: BTreeMap::from([(
            RangeId::from_uuid(Uuid::from_u128(204)),
            "event-a".to_owned(),
        )]),
    };
    transport
        .prepare_down
        .lock()
        .unwrap()
        .extend([nodes[1].clone(), nodes[2].clone()]);
    assert!(matches!(
        coordinator.apply(mutation.clone()),
        Err(SubscriptionProgressError::NoQuorum)
    ));
    assert!(transport.stores[&nodes[0]]
        .local_committed(subscription_id)
        .unwrap()
        .is_none());
    transport.prepare_down.lock().unwrap().remove(&nodes[1]);
    assert_eq!(coordinator.apply(mutation.clone()).unwrap().len(), 2);
    assert_eq!(
        transport.stores[&nodes[0]]
            .local_committed(subscription_id)
            .unwrap(),
        Some(mutation.clone())
    );
    assert_eq!(
        transport.stores[&nodes[1]]
            .local_committed(subscription_id)
            .unwrap(),
        Some(mutation.clone())
    );
    assert!(transport.stores[&nodes[2]]
        .local_committed(subscription_id)
        .unwrap()
        .is_none());
    let mut next = mutation.clone();
    next.sequence = 2;
    next.request_id = Uuid::from_u128(205);
    next.expected_cursor = Some(mutation.cursor);
    next.cursor = "rf1_b".to_owned();
    transport
        .commit_down
        .lock()
        .unwrap()
        .insert(nodes[1].clone());
    assert!(matches!(
        coordinator.apply(next.clone()),
        Err(SubscriptionProgressError::AmbiguousCommit)
    ));
    assert_eq!(
        transport.stores[&nodes[0]]
            .local_committed(subscription_id)
            .unwrap(),
        Some(next.clone())
    );
    assert_ne!(
        transport.stores[&nodes[1]]
            .local_committed(subscription_id)
            .unwrap(),
        Some(next.clone())
    );
    transport.commit_down.lock().unwrap().remove(&nodes[1]);
    assert_eq!(coordinator.apply(next.clone()).unwrap().len(), 2);
    assert_eq!(
        transport.stores[&nodes[1]]
            .local_committed(subscription_id)
            .unwrap(),
        Some(next)
    );
}

#[test]
fn subscription_progress_coordinator_refuses_contradictory_replica_evidence() {
    let dirs = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = progress_nodes();
    let transport = Arc::new(TestProgressTransport::new(&dirs, &nodes));
    let subscription_id = Uuid::from_u128(301);
    let assignment = SubscriptionProgressAssignment::try_new(
        subscription_id,
        nodes[0].clone(),
        ReplicaSet::try_new(nodes.clone()).unwrap(),
        1,
    )
    .unwrap();
    let coordinator = SubscriptionProgressCoordinator::new(assignment, transport.clone());
    let mutation = SubscriptionProgressMutation {
        subscription_id,
        feed_id: Uuid::from_u128(302),
        ownership_epoch: 1,
        sequence: 1,
        request_id: Uuid::from_u128(303),
        expected_cursor: None,
        cursor: "rf1_a".to_owned(),
        positions: BTreeMap::from([(
            RangeId::from_uuid(Uuid::from_u128(304)),
            "event-a".to_owned(),
        )]),
    };
    *transport.corrupt_vote.lock().unwrap() = Some(nodes[1].clone());
    assert!(matches!(
        coordinator.apply(mutation.clone()),
        Err(SubscriptionProgressError::Conflict)
    ));
    for node in &nodes {
        assert!(transport.stores[node]
            .local_committed(subscription_id)
            .unwrap()
            .is_none());
    }
    let stale = SubscriptionProgressMutation {
        ownership_epoch: 2,
        ..mutation
    };
    assert!(matches!(
        coordinator.apply(stale),
        Err(SubscriptionProgressError::InvalidAssignment)
    ));
}

#[test]
fn prepared_subscription_progress_remains_invisible_after_replica_restart() {
    let first_dir = TempDir::new().unwrap();
    let second_dir = TempDir::new().unwrap();
    let first = FjallSubscriptionProgressReplica::open(first_dir.path()).unwrap();
    let second = FjallSubscriptionProgressReplica::open(second_dir.path()).unwrap();
    let subscription_id = Uuid::from_u128(101);
    let mutation = SubscriptionProgressMutation {
        subscription_id,
        feed_id: Uuid::from_u128(102),
        ownership_epoch: 1,
        sequence: 1,
        request_id: Uuid::from_u128(103),
        expected_cursor: None,
        cursor: "rf1_progress".to_owned(),
        positions: BTreeMap::from([(
            RangeId::from_uuid(Uuid::from_u128(104)),
            "event-1".to_owned(),
        )]),
    };
    let vote = first.prepare(mutation.clone()).unwrap();
    assert_eq!(
        vote.digest,
        second.prepare(mutation.clone()).unwrap().digest
    );
    assert!(first.local_committed(subscription_id).unwrap().is_none());
    drop(first);
    let reopened = FjallSubscriptionProgressReplica::open(first_dir.path()).unwrap();
    assert!(reopened.local_committed(subscription_id).unwrap().is_none());
    assert_eq!(
        reopened.prepare(mutation.clone()).unwrap().digest,
        vote.digest
    );
    let mut conflicting = mutation.clone();
    conflicting.cursor = "another-page".to_owned();
    assert!(matches!(
        reopened.prepare(conflicting),
        Err(SubscriptionProgressError::Conflict)
    ));
    assert!(second.local_committed(subscription_id).unwrap().is_none());
}

fn delivery(
    reader_id: Uuid,
    epoch: u64,
    expected_cursor: Option<&str>,
    request_id: Uuid,
    cursor: &str,
    positions: &[(RangeId, &str)],
    records: &[&str],
) -> ReaderDeliveryMutation {
    ReaderDeliveryMutation {
        reader_id,
        epoch,
        expected_cursor: expected_cursor.map(str::to_owned),
        positions: positions
            .iter()
            .map(|(range, cursor)| (*range, (*cursor).to_owned()))
            .collect::<BTreeMap<_, _>>(),
        receipt: ReaderDeliveryReceipt {
            request_id,
            cursor: cursor.to_owned(),
            records: records.iter().map(|record| (*record).to_owned()).collect(),
        },
    }
}

#[test]
fn independent_reader_progress_survives_restart_and_only_acknowledged_work_resumes() {
    let directory = TempDir::new().unwrap();
    let store = FjallReaderProgressStore::open(directory.path()).unwrap();
    let progress: &dyn ReaderProgressEngine = &store;
    let feed = Uuid::from_u128(1);
    let audit = Uuid::from_u128(2);
    let analytics = Uuid::from_u128(3);
    let left = RangeId::from_uuid(Uuid::from_u128(4));
    let right = RangeId::from_uuid(Uuid::from_u128(5));
    let same_request_id = Uuid::from_u128(6);
    let audit_page = delivery(
        audit,
        1,
        None,
        same_request_id,
        "rf1_audit_1",
        &[(left, "a-left-1"), (right, "a-right-1")],
        &["a-left-1", "a-right-1"],
    );
    let analytics_page = delivery(
        analytics,
        1,
        None,
        same_request_id,
        "rf1_analytics_1",
        &[(left, "b-left-1"), (right, "")],
        &["b-left-1"],
    );
    progress.open_session(audit, feed, 1).unwrap();
    progress.open_session(analytics, feed, 1).unwrap();
    let (_, first_receipt) = progress.deliver(audit_page.clone()).unwrap();
    let (_, analytics_receipt) = progress.deliver(analytics_page).unwrap();
    assert_ne!(first_receipt, analytics_receipt);
    assert_eq!(
        progress.deliver(audit_page.clone()).unwrap().1,
        first_receipt
    );

    let mut conflicting = audit_page.clone();
    conflicting.receipt.records = vec!["different-event".to_owned()];
    assert!(matches!(
        progress.deliver(conflicting),
        Err(ReaderProgressError::ConflictingDelivery)
    ));
    let acknowledged = progress.acknowledge(audit, 1, "rf1_audit_1").unwrap();
    assert_eq!(
        progress.acknowledge(audit, 1, "rf1_audit_1").unwrap(),
        acknowledged
    );
    assert!(progress
        .get(analytics)
        .unwrap()
        .unwrap()
        .acknowledged
        .is_empty());

    let unacknowledged_page = delivery(
        audit,
        1,
        Some("rf1_audit_1"),
        Uuid::from_u128(7),
        "rf1_audit_2",
        &[(left, "a-left-2"), (right, "a-right-1")],
        &["a-left-2"],
    );
    progress.deliver(unacknowledged_page).unwrap();
    assert!(matches!(
        progress.acknowledge(audit, 1, "not-delivered"),
        Err(ReaderProgressError::InvalidAcknowledgement)
    ));
    drop(store);

    let reopened = FjallReaderProgressStore::open(directory.path()).unwrap();
    let progress: &dyn ReaderProgressEngine = &reopened;
    let audit_resumed = progress.open_session(audit, feed, 2).unwrap();
    let analytics_resumed = progress.open_session(analytics, feed, 2).unwrap();
    assert_eq!(audit_resumed.delivered, acknowledged.acknowledged);
    assert_eq!(
        audit_resumed.acknowledged_cursor.as_deref(),
        Some("rf1_audit_1")
    );
    assert_eq!(
        audit_resumed.delivered_cursor.as_deref(),
        Some("rf1_audit_1")
    );
    assert!(audit_resumed.last_delivery.is_none());
    assert!(analytics_resumed.delivered.is_empty());
    assert!(analytics_resumed.acknowledged.is_empty());
    assert_eq!(analytics_resumed.delivered_cursor, None);
    assert!(matches!(
        progress.acknowledge(audit, 1, "rf1_audit_2"),
        Err(ReaderProgressError::StaleEpoch)
    ));
    assert!(matches!(
        progress.open_session(audit, Uuid::from_u128(99), 2),
        Err(ReaderProgressError::WrongFeed)
    ));
    assert!(progress.get(Uuid::from_u128(100)).unwrap().is_none());

    let replay = delivery(
        audit,
        2,
        Some("rf1_audit_1"),
        Uuid::from_u128(7),
        "rf1_audit_2_replayed",
        &[(left, "a-left-2"), (right, "a-right-1")],
        &["a-left-2"],
    );
    let (_, receipt) = progress.deliver(replay.clone()).unwrap();
    assert_eq!(progress.deliver(replay).unwrap().1, receipt);
    assert_eq!(
        progress
            .acknowledge(audit, 2, &receipt.cursor)
            .unwrap()
            .acknowledged[&left],
        "a-left-2"
    );
    assert!(progress
        .get(analytics)
        .unwrap()
        .unwrap()
        .acknowledged
        .is_empty());
}

#[test]
fn bounded_progress_and_pacing_do_not_advance_on_exhaustion() {
    let directory = TempDir::new().unwrap();
    let store = FjallReaderProgressStore::open(directory.path()).unwrap();
    let progress: &dyn ReaderProgressEngine = &store;
    let reader = Uuid::from_u128(11);
    let range = RangeId::from_uuid(Uuid::from_u128(12));
    progress
        .open_session(reader, Uuid::from_u128(13), 1)
        .unwrap();
    let mut oversized = delivery(
        reader,
        1,
        None,
        Uuid::from_u128(14),
        "rf1_oversized",
        &[(range, "event")],
        &["event"],
    );
    oversized.receipt.records = vec!["event".to_owned(); 1_025];
    assert!(matches!(
        progress.deliver(oversized),
        Err(ReaderProgressError::TooLarge)
    ));
    assert!(progress.get(reader).unwrap().unwrap().delivered.is_empty());

    let mut fast = ReaderPacingController::new(2, 64, 1_024, Duration::from_millis(100));
    let mut slow = ReaderPacingController::new(2, 64, 1_024, Duration::from_millis(100));
    let healthy = ReaderPressureSample {
        backlog_records: 100,
        average_record_bytes: 16,
        acknowledgement_latency: Duration::from_millis(10),
        unacknowledged_bytes: 0,
        node_pressure: 0.1,
        replica_ready: true,
    };
    for _ in 0..9 {
        fast.observe(healthy, 64);
    }
    let normal = fast.observe(healthy, 64);
    let pressured = slow.observe(
        ReaderPressureSample {
            node_pressure: 0.9,
            ..healthy
        },
        64,
    );
    assert!(normal.max_records > pressured.max_records);
    assert!(fast.observe(healthy, 3).max_records <= 3);
    let blocked = fast.observe(
        ReaderPressureSample {
            unacknowledged_bytes: 1_024,
            ..healthy
        },
        64,
    );
    assert_eq!(blocked.max_records, 0);
    assert!(blocked.retry_after > Duration::ZERO);
    let unavailable = fast.observe(
        ReaderPressureSample {
            replica_ready: false,
            ..healthy
        },
        64,
    );
    assert_eq!(unavailable.retry_after, Duration::from_millis(200));
    let too_large = fast.observe(
        ReaderPressureSample {
            average_record_bytes: 2_048,
            ..healthy
        },
        64,
    );
    assert!(too_large.record_exceeds_budget);
    assert_eq!(too_large.max_records, 0);
    assert!(progress
        .get(reader)
        .unwrap()
        .unwrap()
        .acknowledged
        .is_empty());
}
