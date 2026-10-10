use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use finnstream::{
    active_range::{KeyToken, RangeId, RangePosition, ReplicaSet, StorageNodeId},
    control::{Command, ControlController},
    reader::{
        translate_merge_reader_frontier, translate_split_reader_frontier, FjallReaderProgressStore,
        FjallSubscriptionProgressReplica, ReaderDeliveryMutation, ReaderDeliveryReceipt,
        ReaderFrontierTranslationError, ReaderLineageEntry, ReaderPacingController,
        ReaderPressureSample, ReaderProgressEngine, ReaderProgressError,
        SubscriptionCommitEvidence, SubscriptionPrepareVote, SubscriptionProgressAssignment,
        SubscriptionProgressCoordinator, SubscriptionProgressError, SubscriptionProgressInspection,
        SubscriptionProgressMutation, SubscriptionProgressTransport, SubscriptionReplicaReply,
    },
    storage::{FileLogStore, LogStore},
};
use tempfile::TempDir;
use uuid::Uuid;

#[test]
fn split_and_merge_frontiers_keep_acknowledged_history_without_skipping() {
    let left = RangeId::from_uuid(Uuid::from_u128(401));
    let right = RangeId::from_uuid(Uuid::from_u128(402));
    let split_at = KeyToken::from_bytes([0x80; 16]);
    let rows = (1..=4)
        .map(|position| ReaderLineageEntry {
            range_id: left,
            position: RangePosition::new(position),
            cursor: format!("cursor-{position}"),
            key_token: KeyToken::from_bytes([if position % 2 == 0 { 0xf0 } else { 0x10 }; 16]),
            ingest_time_ns: position as i64,
            message_id: Uuid::from_u128(position as u128),
        })
        .collect::<Vec<_>>();
    let split =
        translate_split_reader_frontier(&rows, left, left, right, split_at, "cursor-2").unwrap();
    assert_eq!(split[&left], "cursor-1");
    assert_eq!(split[&right], "cursor-2");
    let left_rows = rows
        .iter()
        .filter(|row| row.position.value() % 2 == 1)
        .enumerate()
        .map(|(index, row)| ReaderLineageEntry {
            range_id: left,
            position: RangePosition::new(index as u64 + 1),
            ..row.clone()
        })
        .collect::<Vec<_>>();
    let right_rows = rows
        .iter()
        .filter(|row| row.position.value() % 2 == 0)
        .enumerate()
        .map(|(index, row)| ReaderLineageEntry {
            range_id: right,
            position: RangePosition::new(index as u64 + 1),
            ..row.clone()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        translate_merge_reader_frontier(&left_rows, &right_rows, left, right, "cursor-3", "")
            .unwrap(),
        "cursor-1"
    );
    assert_eq!(
        translate_merge_reader_frontier(
            &left_rows,
            &right_rows,
            left,
            right,
            "cursor-3",
            "cursor-2"
        )
        .unwrap(),
        "cursor-3"
    );
    assert!(matches!(
        translate_split_reader_frontier(&rows, left, left, right, split_at, "unknown"),
        Err(ReaderFrontierTranslationError::MissingCursor)
    ));
    let mut missing = rows.clone();
    missing.remove(1);
    assert!(matches!(
        translate_split_reader_frontier(&missing, left, left, right, split_at, "cursor-3"),
        Err(ReaderFrontierTranslationError::InvalidHistory)
    ));
}

#[test]
fn frontier_translation_rejects_ambiguous_and_oversized_source_history() {
    let left = RangeId::from_uuid(Uuid::from_u128(501));
    let right = RangeId::from_uuid(Uuid::from_u128(502));
    let split_at = KeyToken::from_bytes([0x80; 16]);
    let rows = (1..=2)
        .map(|position| ReaderLineageEntry {
            range_id: left,
            position: RangePosition::new(position),
            cursor: format!("source-{position}"),
            key_token: KeyToken::from_bytes([position as u8; 16]),
            ingest_time_ns: position as i64,
            message_id: Uuid::from_u128(position as u128),
        })
        .collect::<Vec<_>>();
    let empty = translate_split_reader_frontier(&rows, left, left, right, split_at, "").unwrap();
    assert!(empty.values().all(String::is_empty));
    let left_rows = rows
        .iter()
        .map(|row| ReaderLineageEntry {
            range_id: left,
            ..row.clone()
        })
        .collect::<Vec<_>>();
    let right_rows = rows
        .iter()
        .map(|row| ReaderLineageEntry {
            range_id: right,
            cursor: format!("other-{}", row.position.value()),
            ingest_time_ns: row.ingest_time_ns + 10,
            ..row.clone()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        translate_merge_reader_frontier(&left_rows, &right_rows, left, right, "", "").unwrap(),
        ""
    );

    let mut duplicated = rows.clone();
    duplicated[1].cursor = duplicated[0].cursor.clone();
    assert!(matches!(
        translate_split_reader_frontier(&duplicated, left, left, right, split_at, ""),
        Err(ReaderFrontierTranslationError::InvalidHistory)
    ));
    let mut gap = rows.clone();
    gap[1].position = RangePosition::new(3);
    assert!(matches!(
        translate_split_reader_frontier(&gap, left, left, right, split_at, ""),
        Err(ReaderFrontierTranslationError::InvalidHistory)
    ));
    let mut wrong_range = rows.clone();
    wrong_range[0].range_id = right;
    assert!(matches!(
        translate_split_reader_frontier(&wrong_range, left, left, right, split_at, ""),
        Err(ReaderFrontierTranslationError::InvalidHistory)
    ));
    let mut oversized_cursor = rows.clone();
    oversized_cursor[0].cursor = "x".repeat(257);
    assert!(matches!(
        translate_split_reader_frontier(&oversized_cursor, left, left, right, split_at, ""),
        Err(ReaderFrontierTranslationError::InvalidHistory)
    ));
    let oversized_history = (1..=10_001_u64)
        .map(|position| ReaderLineageEntry {
            range_id: left,
            position: RangePosition::new(position),
            cursor: format!("large-{position}"),
            key_token: KeyToken::from_bytes([0x10; 16]),
            ingest_time_ns: position as i64,
            message_id: Uuid::from_u128(position as u128),
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        translate_split_reader_frontier(&oversized_history, left, left, right, split_at, ""),
        Err(ReaderFrontierTranslationError::InvalidHistory)
    ));
    assert!(matches!(
        translate_split_reader_frontier(&rows, left, left, left, split_at, ""),
        Err(ReaderFrontierTranslationError::InvalidSources)
    ));
    assert!(matches!(
        translate_merge_reader_frontier(&left_rows, &right_rows, left, left, "", ""),
        Err(ReaderFrontierTranslationError::InvalidSources)
    ));
    let mut conflicting_right = right_rows.clone();
    conflicting_right[0].cursor = left_rows[0].cursor.clone();
    assert!(matches!(
        translate_merge_reader_frontier(&left_rows, &conflicting_right, left, right, "", ""),
        Err(ReaderFrontierTranslationError::InvalidHistory)
    ));
}

struct TestProgressTransport {
    stores: BTreeMap<StorageNodeId, Arc<FjallSubscriptionProgressReplica>>,
    prepare_down: Mutex<BTreeSet<StorageNodeId>>,
    commit_down: Mutex<BTreeSet<StorageNodeId>>,
    read_down: Mutex<BTreeSet<StorageNodeId>>,
    inspect_down: Mutex<BTreeSet<StorageNodeId>>,
    adopt_down: Mutex<BTreeSet<StorageNodeId>>,
    feed_ids: Mutex<BTreeMap<Uuid, Uuid>>,
    read_override: Mutex<BTreeMap<StorageNodeId, Option<SubscriptionProgressMutation>>>,
    wrong_prepare_node: Mutex<Option<StorageNodeId>>,
    wrong_commit_node: Mutex<Option<StorageNodeId>>,
    wrong_read_node: Mutex<Option<StorageNodeId>>,
    corrupt_vote: Mutex<Option<StorageNodeId>>,
}

impl TestProgressTransport {
    fn new(directories: &[TempDir; 3], nodes: &[StorageNodeId; 3]) -> Self {
        Self {
            stores: nodes
                .iter()
                .cloned()
                .zip(directories.iter().map(|directory| {
                    Arc::new(FjallSubscriptionProgressReplica::open(directory.path()).unwrap())
                }))
                .collect(),
            prepare_down: Mutex::new(BTreeSet::new()),
            commit_down: Mutex::new(BTreeSet::new()),
            read_down: Mutex::new(BTreeSet::new()),
            inspect_down: Mutex::new(BTreeSet::new()),
            adopt_down: Mutex::new(BTreeSet::new()),
            feed_ids: Mutex::new(BTreeMap::new()),
            read_override: Mutex::new(BTreeMap::new()),
            wrong_prepare_node: Mutex::new(None),
            wrong_commit_node: Mutex::new(None),
            wrong_read_node: Mutex::new(None),
            corrupt_vote: Mutex::new(None),
        }
    }

    fn claimed_node(
        override_node: &Mutex<Option<StorageNodeId>>,
        node: &StorageNodeId,
    ) -> StorageNodeId {
        override_node
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| node.clone())
    }
}

#[async_trait::async_trait]
impl SubscriptionProgressTransport for TestProgressTransport {
    async fn prepare(
        &self,
        node: &StorageNodeId,
        mutation: SubscriptionProgressMutation,
    ) -> Result<SubscriptionReplicaReply<SubscriptionPrepareVote>, SubscriptionProgressError> {
        if self.prepare_down.lock().unwrap().contains(node) {
            return Err(SubscriptionProgressError::Unavailable);
        }
        let store = self
            .stores
            .get(node)
            .cloned()
            .ok_or(SubscriptionProgressError::InvalidAssignment)?;
        let subscription_id = mutation.subscription_id;
        let ownership_epoch = mutation.ownership_epoch;
        self.feed_ids
            .lock()
            .unwrap()
            .insert(subscription_id, mutation.feed_id);
        let mut vote = tokio::task::spawn_blocking(move || store.prepare(mutation))
            .await
            .map_err(|_| SubscriptionProgressError::Unavailable)??;
        if self.corrupt_vote.lock().unwrap().as_ref() == Some(node) {
            vote.digest[0] ^= 1;
        }
        Ok(SubscriptionReplicaReply {
            replica: Self::claimed_node(&self.wrong_prepare_node, node),
            subscription_id,
            ownership_epoch,
            result: vote,
        })
    }

    async fn commit(
        &self,
        node: &StorageNodeId,
        evidence: SubscriptionCommitEvidence,
    ) -> Result<SubscriptionReplicaReply<SubscriptionProgressMutation>, SubscriptionProgressError>
    {
        if self.commit_down.lock().unwrap().contains(node) {
            return Err(SubscriptionProgressError::Unavailable);
        }
        let store = self
            .stores
            .get(node)
            .cloned()
            .ok_or(SubscriptionProgressError::InvalidAssignment)?;
        let result = tokio::task::spawn_blocking(move || store.commit_with_quorum(evidence))
            .await
            .map_err(|_| SubscriptionProgressError::Unavailable)??;
        Ok(SubscriptionReplicaReply {
            replica: Self::claimed_node(&self.wrong_commit_node, node),
            subscription_id: result.subscription_id,
            ownership_epoch: result.ownership_epoch,
            result,
        })
    }

    async fn committed(
        &self,
        node: &StorageNodeId,
        subscription_id: Uuid,
        ownership_epoch: u64,
    ) -> Result<
        SubscriptionReplicaReply<Option<SubscriptionProgressMutation>>,
        SubscriptionProgressError,
    > {
        if self.read_down.lock().unwrap().contains(node) {
            return Err(SubscriptionProgressError::Unavailable);
        }
        let override_value = self.read_override.lock().unwrap().get(node).cloned();
        let result = if let Some(value) = override_value {
            value
        } else {
            let store = self
                .stores
                .get(node)
                .cloned()
                .ok_or(SubscriptionProgressError::InvalidAssignment)?;
            tokio::task::spawn_blocking(move || store.local_committed(subscription_id))
                .await
                .map_err(|_| SubscriptionProgressError::Unavailable)??
        };
        Ok(SubscriptionReplicaReply {
            replica: Self::claimed_node(&self.wrong_read_node, node),
            subscription_id,
            ownership_epoch,
            result,
        })
    }

    async fn inspect(
        &self,
        node: &StorageNodeId,
        subscription_id: Uuid,
        ownership_epoch: u64,
    ) -> Result<SubscriptionReplicaReply<SubscriptionProgressInspection>, SubscriptionProgressError>
    {
        if self.inspect_down.lock().unwrap().contains(node) {
            return Err(SubscriptionProgressError::Unavailable);
        }
        let store = self
            .stores
            .get(node)
            .cloned()
            .ok_or(SubscriptionProgressError::InvalidAssignment)?;
        let result = tokio::task::spawn_blocking(move || store.local_state(subscription_id))
            .await
            .map_err(|_| SubscriptionProgressError::Unavailable)??;
        Ok(SubscriptionReplicaReply {
            replica: node.clone(),
            subscription_id,
            ownership_epoch,
            result,
        })
    }

    async fn adopt(
        &self,
        node: &StorageNodeId,
        _owner: &StorageNodeId,
        subscription_id: Uuid,
        ownership_epoch: u64,
        committed: Option<SubscriptionProgressMutation>,
    ) -> Result<
        SubscriptionReplicaReply<Option<SubscriptionProgressMutation>>,
        SubscriptionProgressError,
    > {
        if self.adopt_down.lock().unwrap().contains(node) {
            return Err(SubscriptionProgressError::Unavailable);
        }
        let feed_id = committed
            .as_ref()
            .map(|mutation| mutation.feed_id)
            .or_else(|| self.feed_ids.lock().unwrap().get(&subscription_id).copied())
            .ok_or(SubscriptionProgressError::InvalidAssignment)?;
        let store = self
            .stores
            .get(node)
            .cloned()
            .ok_or(SubscriptionProgressError::InvalidAssignment)?;
        let result = tokio::task::spawn_blocking(move || {
            store.adopt_recovered(subscription_id, feed_id, ownership_epoch, committed)
        })
        .await
        .map_err(|_| SubscriptionProgressError::Unavailable)??;
        Ok(SubscriptionReplicaReply {
            replica: node.clone(),
            subscription_id,
            ownership_epoch,
            result,
        })
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
    let committed = coordinator.apply(mutation).await.unwrap();
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

#[tokio::test]
async fn quorum_subscription_read_fails_closed_on_loss_disagreement_and_ambiguous_retry() {
    let dirs = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = progress_nodes();
    let transport = Arc::new(TestProgressTransport::new(&dirs, &nodes));
    let subscription_id = Uuid::from_u128(801);
    let assignment = SubscriptionProgressAssignment::try_new(
        subscription_id,
        nodes[0].clone(),
        ReplicaSet::try_new(nodes.clone()).unwrap(),
        1,
    )
    .unwrap();
    let coordinator = SubscriptionProgressCoordinator::new(assignment.clone(), transport.clone());
    assert!(coordinator.read_committed().await.unwrap().is_none());
    transport.read_down.lock().unwrap().insert(nodes[0].clone());
    assert!(matches!(
        coordinator.read_committed().await,
        Err(SubscriptionProgressError::NoQuorum)
    ));
    transport.read_down.lock().unwrap().clear();
    let mutation = SubscriptionProgressMutation {
        subscription_id,
        feed_id: Uuid::from_u128(802),
        ownership_epoch: 1,
        sequence: 1,
        request_id: Uuid::from_u128(803),
        expected_cursor: None,
        cursor: "rf1_a".to_owned(),
        positions: BTreeMap::from([(
            RangeId::from_uuid(Uuid::from_u128(804)),
            "event-a".to_owned(),
        )]),
    };
    coordinator.apply(mutation.clone()).await.unwrap();
    assert_eq!(
        coordinator.read_committed().await.unwrap(),
        Some(mutation.clone())
    );
    transport.read_down.lock().unwrap().insert(nodes[2].clone());
    assert_eq!(
        coordinator.read_committed().await.unwrap(),
        Some(mutation.clone())
    );
    transport.read_down.lock().unwrap().clear();
    transport.read_down.lock().unwrap().insert(nodes[0].clone());
    assert_eq!(
        coordinator.read_committed().await.unwrap(),
        Some(mutation.clone())
    );
    transport.read_down.lock().unwrap().insert(nodes[1].clone());
    assert!(matches!(
        coordinator.read_committed().await,
        Err(SubscriptionProgressError::NoQuorum)
    ));
    transport.read_down.lock().unwrap().clear();
    transport
        .read_override
        .lock()
        .unwrap()
        .insert(nodes[2].clone(), None);
    assert!(matches!(
        coordinator.read_committed().await,
        Err(SubscriptionProgressError::Conflict)
    ));
    transport.read_override.lock().unwrap().clear();
    let mut stale = mutation.clone();
    stale.ownership_epoch = 2;
    transport
        .read_override
        .lock()
        .unwrap()
        .insert(nodes[2].clone(), Some(stale));
    assert!(matches!(
        coordinator.read_committed().await,
        Err(SubscriptionProgressError::StaleEpoch)
    ));
    transport.read_override.lock().unwrap().clear();
    drop(coordinator);
    drop(transport);
    let transport = Arc::new(TestProgressTransport::new(&dirs, &nodes));
    let coordinator = SubscriptionProgressCoordinator::new(assignment, transport.clone());
    assert_eq!(
        coordinator.read_committed().await.unwrap(),
        Some(mutation.clone())
    );
    let mut next = mutation.clone();
    next.sequence = 2;
    next.request_id = Uuid::from_u128(805);
    next.expected_cursor = Some(mutation.cursor);
    next.cursor = "rf1_b".to_owned();
    transport
        .commit_down
        .lock()
        .unwrap()
        .extend([nodes[1].clone(), nodes[2].clone()]);
    assert!(matches!(
        coordinator.apply(next.clone()).await,
        Err(SubscriptionProgressError::AmbiguousCommit)
    ));
    assert!(matches!(
        coordinator.read_committed().await,
        Err(SubscriptionProgressError::Conflict)
    ));
    transport.commit_down.lock().unwrap().clear();
    coordinator.apply(next.clone()).await.unwrap();
    assert_eq!(coordinator.read_committed().await.unwrap(), Some(next));
}

#[tokio::test]
async fn ambiguous_subscription_retry_reconciles_after_restart_without_guessing() {
    let dirs = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = progress_nodes();
    let transport = Arc::new(TestProgressTransport::new(&dirs, &nodes));
    let subscription_id = Uuid::from_u128(1201);
    let assignment = SubscriptionProgressAssignment::try_new(
        subscription_id,
        nodes[0].clone(),
        ReplicaSet::try_new(nodes.clone()).unwrap(),
        1,
    )
    .unwrap();
    let coordinator = SubscriptionProgressCoordinator::new(assignment.clone(), transport.clone());
    let first = SubscriptionProgressMutation {
        subscription_id,
        feed_id: Uuid::from_u128(1202),
        ownership_epoch: 1,
        sequence: 1,
        request_id: Uuid::from_u128(1203),
        expected_cursor: None,
        cursor: "cursor-1".to_owned(),
        positions: BTreeMap::from([(
            RangeId::from_uuid(Uuid::from_u128(1204)),
            "cursor-1".to_owned(),
        )]),
    };
    coordinator.apply(first.clone()).await.unwrap();
    let mut next = first.clone();
    next.sequence = 2;
    next.request_id = Uuid::from_u128(1205);
    next.expected_cursor = Some(first.cursor);
    next.cursor = "cursor-2".to_owned();
    transport
        .commit_down
        .lock()
        .unwrap()
        .extend([nodes[1].clone(), nodes[2].clone()]);
    assert!(matches!(
        coordinator.apply(next.clone()).await,
        Err(SubscriptionProgressError::AmbiguousCommit)
    ));
    drop(coordinator);
    drop(transport);
    let transport = Arc::new(TestProgressTransport::new(&dirs, &nodes));
    let coordinator = SubscriptionProgressCoordinator::new(assignment, transport.clone());
    transport.read_down.lock().unwrap().insert(nodes[0].clone());
    assert!(matches!(
        coordinator.reconcile_retry(next.clone()).await,
        Err(SubscriptionProgressError::NoQuorum)
    ));
    transport.read_down.lock().unwrap().clear();
    let mut conflicting = next.clone();
    conflicting.cursor = "other-cursor".to_owned();
    transport
        .read_override
        .lock()
        .unwrap()
        .insert(nodes[2].clone(), Some(conflicting));
    assert!(matches!(
        coordinator.reconcile_retry(next.clone()).await,
        Err(SubscriptionProgressError::Conflict)
    ));
    transport.read_override.lock().unwrap().clear();
    transport
        .commit_down
        .lock()
        .unwrap()
        .insert(nodes[2].clone());
    assert!(matches!(
        coordinator.reconcile_retry(next.clone()).await,
        Err(SubscriptionProgressError::AmbiguousCommit)
    ));
    transport.commit_down.lock().unwrap().clear();
    assert_eq!(
        coordinator.reconcile_retry(next.clone()).await.unwrap(),
        next
    );
    assert_eq!(
        coordinator.read_committed().await.unwrap(),
        Some(next.clone())
    );
    let mut wrong_id = next.clone();
    wrong_id.request_id = Uuid::from_u128(1206);
    assert!(matches!(
        coordinator.reconcile_retry(wrong_id).await,
        Err(SubscriptionProgressError::Conflict)
    ));
    assert_eq!(coordinator.read_committed().await.unwrap(), Some(next));
}

#[tokio::test]
async fn progress_coordinator_refuses_replies_claiming_another_replica() {
    let dirs = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = progress_nodes();
    let transport = Arc::new(TestProgressTransport::new(&dirs, &nodes));
    let subscription_id = Uuid::from_u128(981);
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
        feed_id: Uuid::from_u128(982),
        ownership_epoch: 1,
        sequence: 1,
        request_id: Uuid::from_u128(983),
        expected_cursor: None,
        cursor: "rf1_a".to_owned(),
        positions: BTreeMap::from([(
            RangeId::from_uuid(Uuid::from_u128(984)),
            "event-a".to_owned(),
        )]),
    };
    *transport.wrong_prepare_node.lock().unwrap() = Some(nodes[1].clone());
    assert!(matches!(
        coordinator.apply(mutation.clone()).await,
        Err(SubscriptionProgressError::InvalidAssignment)
    ));
    assert!(transport.stores[&nodes[0]]
        .local_committed(subscription_id)
        .unwrap()
        .is_none());
    *transport.wrong_prepare_node.lock().unwrap() = None;
    *transport.wrong_commit_node.lock().unwrap() = Some(nodes[1].clone());
    assert!(matches!(
        coordinator.apply(mutation.clone()).await,
        Err(SubscriptionProgressError::InvalidAssignment)
    ));
    assert!(matches!(
        coordinator.read_committed().await,
        Err(SubscriptionProgressError::Conflict)
    ));
    *transport.wrong_commit_node.lock().unwrap() = None;
    coordinator.apply(mutation.clone()).await.unwrap();
    *transport.wrong_read_node.lock().unwrap() = Some(nodes[1].clone());
    assert!(matches!(
        coordinator.read_committed().await,
        Err(SubscriptionProgressError::InvalidAssignment)
    ));
    *transport.wrong_read_node.lock().unwrap() = None;
    assert_eq!(coordinator.read_committed().await.unwrap(), Some(mutation));
}

#[tokio::test]
async fn subscription_progress_coordinator_requires_matching_prepare_and_commit_majorities() {
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
        coordinator.apply(mutation.clone()).await,
        Err(SubscriptionProgressError::NoQuorum)
    ));
    assert!(transport.stores[&nodes[0]]
        .local_committed(subscription_id)
        .unwrap()
        .is_none());
    transport.prepare_down.lock().unwrap().remove(&nodes[1]);
    assert_eq!(coordinator.apply(mutation.clone()).await.unwrap().len(), 2);
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
        coordinator.apply(next.clone()).await,
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
    assert_eq!(coordinator.apply(next.clone()).await.unwrap().len(), 2);
    assert_eq!(
        transport.stores[&nodes[1]]
            .local_committed(subscription_id)
            .unwrap(),
        Some(next)
    );
}

#[tokio::test]
async fn subscription_progress_coordinator_refuses_contradictory_replica_evidence() {
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
        coordinator.apply(mutation.clone()).await,
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
        coordinator.apply(stale).await,
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

fn progress_mutation(
    subscription_id: Uuid,
    feed_id: Uuid,
    epoch: u64,
    sequence: u64,
) -> SubscriptionProgressMutation {
    SubscriptionProgressMutation {
        subscription_id,
        feed_id,
        ownership_epoch: epoch,
        sequence,
        request_id: Uuid::from_u128(5_000 + sequence as u128),
        expected_cursor: (sequence > 1).then(|| format!("rf1_{}", sequence - 1)),
        cursor: format!("rf1_{sequence}"),
        positions: BTreeMap::from([(
            RangeId::from_uuid(Uuid::from_u128(6_000 + sequence as u128)),
            format!("position-{sequence}"),
        )]),
    }
}

fn plant_committed(
    transport: &TestProgressTransport,
    node: &StorageNodeId,
    mutation: &SubscriptionProgressMutation,
) {
    let store = &transport.stores[node];
    let vote = store.prepare(mutation.clone()).unwrap();
    store
        .commit_with_quorum(
            SubscriptionCommitEvidence::new(
                mutation.subscription_id,
                mutation.request_id,
                [
                    (StorageNodeId::try_new("witness-a").unwrap(), vote.digest),
                    (StorageNodeId::try_new("witness-b").unwrap(), vote.digest),
                ],
            )
            .unwrap(),
        )
        .unwrap();
}

fn progress_controller(directory: &TempDir, nodes: &[StorageNodeId]) -> ControlController {
    let store: Arc<dyn LogStore> =
        Arc::new(FileLogStore::open(directory.path().join("data")).unwrap());
    ControlController::open_with_storage_nodes(
        directory.path().join("catalog.json"),
        store,
        nodes.to_vec(),
    )
    .unwrap()
}

#[tokio::test]
async fn subscription_progress_recovery_moves_owner_preserves_committed_and_fences_stale_epoch() {
    let directory = TempDir::new().unwrap();
    let nodes = progress_nodes();
    let controller = progress_controller(&directory, &nodes);
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
    let owner = coordinator.assignment().owner.clone();
    let first = progress_mutation(subscription.subscription_id, subscription.feed_id, 1, 1);
    coordinator.apply(first.clone()).await.unwrap();

    for set in [
        &transport.prepare_down,
        &transport.commit_down,
        &transport.read_down,
        &transport.inspect_down,
        &transport.adopt_down,
    ] {
        set.lock().unwrap().insert(owner.clone());
    }

    let outcome = coordinator.recover_lost_owner(&controller).await.unwrap();
    assert_eq!(outcome.assignment.ownership_epoch, 2);
    assert_ne!(outcome.assignment.owner, owner);
    assert_eq!(outcome.adopted.len(), 2);
    assert!(outcome.adopted.contains(&outcome.assignment.owner));
    let recovered_committed = outcome.committed.clone().unwrap();
    assert_eq!(recovered_committed.ownership_epoch, 2);
    assert_eq!(recovered_committed.cursor, first.cursor);
    assert_eq!(recovered_committed.request_id, first.request_id);

    let recovered = SubscriptionProgressCoordinator::for_subscription(
        &controller,
        subscription.subscription_id,
        transport.clone(),
    )
    .await
    .unwrap();
    assert_eq!(recovered.assignment(), &outcome.assignment);

    let mut stale_write =
        progress_mutation(subscription.subscription_id, subscription.feed_id, 1, 2);
    stale_write.request_id = Uuid::from_u128(5_500);
    assert!(matches!(
        coordinator.apply(stale_write).await,
        Err(SubscriptionProgressError::StaleEpoch)
            | Err(SubscriptionProgressError::InvalidAssignment)
    ));
    assert!(matches!(
        coordinator.read_committed().await,
        Err(SubscriptionProgressError::StaleEpoch)
            | Err(SubscriptionProgressError::InvalidAssignment)
            | Err(SubscriptionProgressError::NoQuorum)
    ));

    let next = progress_mutation(subscription.subscription_id, subscription.feed_id, 2, 2);
    recovered.apply(next.clone()).await.unwrap();
    assert_eq!(recovered.read_committed().await.unwrap(), Some(next));

    for set in [
        &transport.prepare_down,
        &transport.commit_down,
        &transport.read_down,
        &transport.inspect_down,
        &transport.adopt_down,
    ] {
        set.lock().unwrap().clear();
    }
    let healed = recovered.synchronize_placement().await.unwrap();
    assert_eq!(healed.adopted.len(), 3);
    let owner_state = transport.stores[&owner]
        .local_state(subscription.subscription_id)
        .unwrap();
    assert_eq!(owner_state.committed, healed.committed);
    assert_eq!(owner_state.committed.unwrap().ownership_epoch, 2);

    let third = progress_mutation(subscription.subscription_id, subscription.feed_id, 2, 3);
    recovered.apply(third.clone()).await.unwrap();
    assert_eq!(recovered.read_committed().await.unwrap(), Some(third));
}

#[tokio::test]
async fn subscription_progress_recovery_fails_closed_on_insufficient_or_contradictory_evidence() {
    let directory = TempDir::new().unwrap();
    let nodes = progress_nodes();
    let controller = progress_controller(&directory, &nodes);
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
    transport
        .feed_ids
        .lock()
        .unwrap()
        .insert(subscription.subscription_id, subscription.feed_id);
    let assignment = SubscriptionProgressAssignment::try_new(
        subscription.subscription_id,
        nodes[2].clone(),
        ReplicaSet::try_new(nodes.clone()).unwrap(),
        1,
    )
    .unwrap();
    let coordinator = SubscriptionProgressCoordinator::new(assignment, transport.clone());
    transport
        .inspect_down
        .lock()
        .unwrap()
        .insert(nodes[2].clone());
    transport
        .adopt_down
        .lock()
        .unwrap()
        .insert(nodes[2].clone());

    let empty = coordinator.recover_lost_owner(&controller).await.unwrap();
    assert_eq!(empty.assignment.ownership_epoch, 2);
    assert!(empty.committed.is_none());
    assert_eq!(empty.adopted.len(), 2);

    let mut fork_left = progress_mutation(subscription.subscription_id, subscription.feed_id, 2, 1);
    fork_left.cursor = "rf1_left".to_owned();
    let mut fork_right = fork_left.clone();
    fork_right.request_id = Uuid::from_u128(5_999);
    fork_right.cursor = "rf1_right".to_owned();
    plant_committed(&transport, &nodes[0], &fork_left);
    plant_committed(&transport, &nodes[1], &fork_right);
    assert!(matches!(
        coordinator.recover_lost_owner(&controller).await,
        Err(SubscriptionProgressError::Conflict)
    ));
    let current = SubscriptionProgressCoordinator::for_subscription(
        &controller,
        subscription.subscription_id,
        transport.clone(),
    )
    .await
    .unwrap();
    assert_eq!(current.assignment().ownership_epoch, 2);

    transport
        .inspect_down
        .lock()
        .unwrap()
        .insert(nodes[0].clone());
    assert!(matches!(
        coordinator.recover_lost_owner(&controller).await,
        Err(SubscriptionProgressError::NoQuorum)
    ));
}

#[tokio::test]
async fn subscription_progress_recovery_adopts_latest_committed_under_raced_move() {
    let directory = TempDir::new().unwrap();
    let nodes = progress_nodes();
    let controller = progress_controller(&directory, &nodes);
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
    transport
        .feed_ids
        .lock()
        .unwrap()
        .insert(subscription.subscription_id, subscription.feed_id);
    let assignment = SubscriptionProgressAssignment::try_new(
        subscription.subscription_id,
        nodes[2].clone(),
        ReplicaSet::try_new(nodes.clone()).unwrap(),
        1,
    )
    .unwrap();
    let coordinator = SubscriptionProgressCoordinator::new(assignment, transport.clone());

    for sequence in 1..=5 {
        plant_committed(
            &transport,
            &nodes[0],
            &progress_mutation(
                subscription.subscription_id,
                subscription.feed_id,
                1,
                sequence,
            ),
        );
    }
    for sequence in 1..=3 {
        plant_committed(
            &transport,
            &nodes[1],
            &progress_mutation(
                subscription.subscription_id,
                subscription.feed_id,
                1,
                sequence,
            ),
        );
    }
    transport
        .inspect_down
        .lock()
        .unwrap()
        .insert(nodes[2].clone());
    transport
        .adopt_down
        .lock()
        .unwrap()
        .insert(nodes[2].clone());

    controller
        .execute_commands(vec![Command::RecoverSubscriptionProgressOwner {
            subscription_id: subscription.subscription_id,
            expected_ownership_epoch: 1,
            new_owner: nodes[1].clone(),
        }])
        .await
        .unwrap();

    let outcome = coordinator.recover_lost_owner(&controller).await.unwrap();
    assert_eq!(outcome.assignment.ownership_epoch, 2);
    assert_eq!(outcome.assignment.owner, nodes[1]);
    let committed = outcome.committed.unwrap();
    assert_eq!(committed.sequence, 5);
    assert_eq!(committed.ownership_epoch, 2);
    assert_eq!(outcome.adopted.len(), 2);
    let behind = transport.stores[&nodes[1]]
        .local_state(subscription.subscription_id)
        .unwrap();
    assert_eq!(behind.committed, Some(committed));
}
