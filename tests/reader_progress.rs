use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use tempfile::TempDir;
use uuid::Uuid;
use whitewater::{
    active_range::{KeyToken, RangeId, RangePosition, ReplicaSet, StorageNodeId},
    control::{Command, ControlController},
    reader::{
        translate_merge_reader_frontier, translate_merge_subscription_frontier,
        translate_split_reader_frontier, translate_split_subscription_frontier,
        FjallReaderProgressStore, FjallSubscriptionProgressReplica, ReaderDeliveryMutation,
        ReaderDeliveryReceipt, ReaderFrontierTranslationError, ReaderLineageEntry,
        ReaderPacingController, ReaderPressureSample, ReaderProgressEngine, ReaderProgressError,
        SubscriptionCommitEvidence, SubscriptionMemberState, SubscriptionPlacementAuthority,
        SubscriptionPrepareVote, SubscriptionProgressAssignment, SubscriptionProgressCoordinator,
        SubscriptionProgressError, SubscriptionProgressInspection, SubscriptionProgressMutation,
        SubscriptionProgressTransport, SubscriptionReplicaReply,
    },
    storage::{FileLogStore, LogStore},
};

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

#[test]
fn split_subscription_frontier_rekeys_positions_and_quarantine() {
    let source = RangeId::from_uuid(Uuid::from_u128(601));
    let right = RangeId::from_uuid(Uuid::from_u128(602));
    let other = RangeId::from_uuid(Uuid::from_u128(603));
    let split_at = KeyToken::from_bytes([0x80; 16]);
    // Odd positions fall below the split token (left child keeps `source`),
    // even positions move to `right`.
    let rows = (1..=4)
        .map(|position| ReaderLineageEntry {
            range_id: source,
            position: RangePosition::new(position),
            cursor: format!("cursor-{position}"),
            key_token: KeyToken::from_bytes([if position % 2 == 0 { 0xf0 } else { 0x10 }; 16]),
            ingest_time_ns: position as i64,
            message_id: Uuid::from_u128(position as u128),
        })
        .collect::<Vec<_>>();
    let positions = BTreeMap::from([
        (source, "cursor-3".to_string()),
        (other, "other-cursor".to_string()),
    ]);
    let quarantined = BTreeMap::from([
        (
            source,
            vec![
                "cursor-1".to_string(),
                "cursor-4".to_string(),
                "outside-lineage".to_string(),
            ],
        ),
        (other, vec!["other-quarantine".to_string()]),
    ]);
    let (positions, quarantined, _skipped) = translate_split_subscription_frontier(
        &rows,
        source,
        right,
        split_at,
        &positions,
        &quarantined,
        &BTreeMap::new(),
    )
    .unwrap();
    // Acknowledged cursor-3 acks left through position 3 and right through
    // position 2; untouched ranges pass through unchanged.
    assert_eq!(positions[&source], "cursor-3");
    assert_eq!(positions[&right], "cursor-2");
    assert_eq!(positions[&other], "other-cursor");
    assert_eq!(positions.len(), 3);
    assert_eq!(
        quarantined[&source],
        vec!["cursor-1".to_string(), "outside-lineage".to_string()]
    );
    assert_eq!(quarantined[&right], vec!["cursor-4".to_string()]);
    assert_eq!(quarantined[&other], vec!["other-quarantine".to_string()]);
}

#[test]
fn merge_subscription_frontier_rekeys_positions_and_quarantine() {
    let left = RangeId::from_uuid(Uuid::from_u128(701));
    let right = RangeId::from_uuid(Uuid::from_u128(702));
    let other = RangeId::from_uuid(Uuid::from_u128(703));
    let build = |range_id: RangeId, positions: &[u64]| {
        positions
            .iter()
            .enumerate()
            .map(|(index, position)| ReaderLineageEntry {
                range_id,
                position: RangePosition::new(index as u64 + 1),
                cursor: format!("cursor-{position}"),
                key_token: KeyToken::from_bytes([0x10; 16]),
                ingest_time_ns: *position as i64,
                message_id: Uuid::from_u128(*position as u128),
            })
            .collect::<Vec<_>>()
    };
    let left_rows = build(left, &[1, 3]);
    let right_rows = build(right, &[2, 4]);
    let positions = BTreeMap::from([
        (left, "cursor-1".to_string()),
        (right, "cursor-2".to_string()),
        (other, "other-cursor".to_string()),
    ]);
    let quarantined = BTreeMap::from([
        (left, vec!["left-quarantine".to_string()]),
        (
            right,
            vec![
                "right-quarantine-a".to_string(),
                "right-quarantine-b".to_string(),
            ],
        ),
        (other, vec!["other-quarantine".to_string()]),
    ]);
    let (positions, quarantined, _skipped) = translate_merge_subscription_frontier(
        &left_rows,
        &right_rows,
        left,
        right,
        &positions,
        &quarantined,
        &BTreeMap::new(),
    )
    .unwrap();
    // Ingest ordering interleaves cursor-1 (left pos 1), cursor-2 (right pos
    // 1), then cursor-3 (left pos 2) which sits beyond the acknowledged left
    // boundary, so the merged frontier stops at cursor-2.
    assert_eq!(positions[&left], "cursor-2");
    assert_eq!(positions[&other], "other-cursor");
    assert!(!positions.contains_key(&right));
    assert_eq!(
        quarantined[&left],
        vec![
            "left-quarantine".to_string(),
            "right-quarantine-a".to_string(),
            "right-quarantine-b".to_string()
        ]
    );
    assert!(!quarantined.contains_key(&right));
    assert_eq!(quarantined[&other], vec!["other-quarantine".to_string()]);
}

#[test]
fn subscription_frontier_translation_rejects_missing_ranges_and_stale_lineage() {
    let left = RangeId::from_uuid(Uuid::from_u128(801));
    let right = RangeId::from_uuid(Uuid::from_u128(802));
    let split_at = KeyToken::from_bytes([0x80; 16]);
    let rows = (1..=2)
        .map(|position| ReaderLineageEntry {
            range_id: left,
            position: RangePosition::new(position),
            cursor: format!("cursor-{position}"),
            key_token: KeyToken::from_bytes([0x10; 16]),
            ingest_time_ns: position as i64,
            message_id: Uuid::from_u128(position as u128),
        })
        .collect::<Vec<_>>();
    let no_positions: BTreeMap<RangeId, String> = BTreeMap::new();
    let no_quarantine: BTreeMap<RangeId, Vec<String>> = BTreeMap::new();
    // A frontier that never covered the splitting range cannot be guessed.
    assert!(matches!(
        translate_split_subscription_frontier(
            &rows,
            left,
            right,
            split_at,
            &no_positions,
            &no_quarantine,
            &no_quarantine
        ),
        Err(ReaderFrontierTranslationError::InvalidSources)
    ));
    assert!(matches!(
        translate_merge_subscription_frontier(
            &rows,
            &rows,
            left,
            right,
            &no_positions,
            &no_quarantine,
            &no_quarantine
        ),
        Err(ReaderFrontierTranslationError::InvalidSources)
    ));
    // A Cursor absent from the frozen lineage must not translate.
    let stale = BTreeMap::from([(left, "not-in-history".to_string())]);
    assert!(matches!(
        translate_split_subscription_frontier(
            &rows,
            left,
            right,
            split_at,
            &stale,
            &no_quarantine,
            &no_quarantine
        ),
        Err(ReaderFrontierTranslationError::MissingCursor)
    ));
    // Gapped or duplicated lineage stays a hard refusal through the
    // Subscription wrapper, identical to the Reader path.
    let mut gapped = rows.clone();
    gapped[1].position = RangePosition::new(3);
    let frontier = BTreeMap::from([(left, "cursor-1".to_string())]);
    assert!(matches!(
        translate_split_subscription_frontier(
            &gapped,
            left,
            right,
            split_at,
            &frontier,
            &no_quarantine,
            &no_quarantine
        ),
        Err(ReaderFrontierTranslationError::InvalidHistory)
    ));
    let mut duplicated = rows.clone();
    duplicated[1].cursor = duplicated[0].cursor.clone();
    let merge_frontier = BTreeMap::from([(left, "cursor-1".to_string()), (right, String::new())]);
    assert!(matches!(
        translate_merge_subscription_frontier(
            &duplicated,
            &rows,
            left,
            right,
            &merge_frontier,
            &no_quarantine,
            &no_quarantine
        ),
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
        members: SubscriptionMemberState,
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
            store.adopt_recovered(
                subscription_id,
                feed_id,
                ownership_epoch,
                committed,
                members,
            )
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

#[tokio::test]
async fn owner_rejected_prepare_does_not_reserve_follower_slots() {
    let dirs = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let nodes = progress_nodes();
    let transport = Arc::new(TestProgressTransport::new(&dirs, &nodes));
    let subscription_id = Uuid::from_u128(9100);
    let assignment = SubscriptionProgressAssignment::try_new(
        subscription_id,
        nodes[0].clone(),
        ReplicaSet::try_new(nodes.clone()).unwrap(),
        1,
    )
    .unwrap();
    let coordinator = SubscriptionProgressCoordinator::new(assignment, transport.clone());
    let accepted = progress_mutation(subscription_id, Uuid::from_u128(9101), 1, 1);
    transport.stores[&nodes[0]]
        .prepare(accepted.clone())
        .unwrap();
    let mut rejected = accepted.clone();
    rejected.request_id = Uuid::from_u128(9102);
    rejected.cursor = "rf1_rejected".to_owned();
    assert!(matches!(
        coordinator.apply(rejected).await,
        Err(SubscriptionProgressError::Sequence)
    ));
    for node in &nodes[1..] {
        assert!(
            transport.stores[node]
                .local_state(subscription_id)
                .unwrap()
                .prepared
                .is_none(),
            "an owner-rejected request reserved a follower slot"
        );
    }
    coordinator.apply(accepted.clone()).await.unwrap();
    for node in &nodes {
        assert_eq!(
            transport.stores[node]
                .local_committed(subscription_id)
                .unwrap(),
            Some(accepted.clone())
        );
    }
}

#[tokio::test]
async fn unavailable_or_invalid_owner_vote_never_prepares_followers() {
    for corrupt in [false, true] {
        let dirs = [
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
        ];
        let nodes = progress_nodes();
        let transport = Arc::new(TestProgressTransport::new(&dirs, &nodes));
        let subscription_id = Uuid::from_u128(9200);
        let assignment = SubscriptionProgressAssignment::try_new(
            subscription_id,
            nodes[0].clone(),
            ReplicaSet::try_new(nodes.clone()).unwrap(),
            1,
        )
        .unwrap();
        let coordinator = SubscriptionProgressCoordinator::new(assignment, transport.clone());
        if corrupt {
            *transport.corrupt_vote.lock().unwrap() = Some(nodes[0].clone());
        } else {
            transport
                .prepare_down
                .lock()
                .unwrap()
                .insert(nodes[0].clone());
        }
        assert!(coordinator
            .apply(progress_mutation(
                subscription_id,
                Uuid::from_u128(9201),
                1,
                1
            ))
            .await
            .is_err());
        for node in &nodes[1..] {
            assert!(transport.stores[node]
                .local_state(subscription_id)
                .unwrap()
                .prepared
                .is_none());
        }
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
        feed_id: subscription.source.feed_id().unwrap(),
        ownership_epoch: 1,
        sequence: 1,
        request_id: Uuid::from_u128(500),
        member_progress: BTreeMap::new(),
        member_feed: None,
        expected_cursor: None,
        cursor: "rf1_page".to_owned(),
        positions: BTreeMap::from([(
            RangeId::from_uuid(Uuid::from_u128(501)),
            "record".to_owned(),
        )]),

        quarantined: BTreeMap::new(),
        quarantine_add: BTreeMap::new(),
        quarantine_remove: BTreeMap::new(),
        skipped: BTreeMap::new(),
        skip_add: BTreeMap::new(),
        tick: 0,
        lease_ops: Vec::new(),
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
        member_progress: BTreeMap::new(),
        member_feed: None,
        expected_cursor: None,
        cursor: "rf1_a".to_owned(),
        positions: BTreeMap::from([(
            RangeId::from_uuid(Uuid::from_u128(804)),
            "event-a".to_owned(),
        )]),

        quarantined: BTreeMap::new(),
        quarantine_add: BTreeMap::new(),
        quarantine_remove: BTreeMap::new(),
        skipped: BTreeMap::new(),
        skip_add: BTreeMap::new(),
        tick: 0,
        lease_ops: Vec::new(),
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
    // A replica holding no committed row is a lagging witness rather than a
    // contradiction; the two agreeing committed votes still decide.
    transport
        .read_override
        .lock()
        .unwrap()
        .insert(nodes[2].clone(), None);
    assert_eq!(
        coordinator.read_committed().await.unwrap(),
        Some(mutation.clone())
    );
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
    // The quorum-evidenced commit reached one replica: that higher committed
    // sequence is the live frontier while the two lagging replicas converge.
    assert_eq!(
        coordinator.read_committed().await.unwrap(),
        Some(next.clone())
    );
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
        member_progress: BTreeMap::new(),
        member_feed: None,
        expected_cursor: None,
        cursor: "cursor-1".to_owned(),
        positions: BTreeMap::from([(
            RangeId::from_uuid(Uuid::from_u128(1204)),
            "cursor-1".to_owned(),
        )]),

        quarantined: BTreeMap::new(),
        quarantine_add: BTreeMap::new(),
        quarantine_remove: BTreeMap::new(),
        skipped: BTreeMap::new(),
        skip_add: BTreeMap::new(),
        tick: 0,
        lease_ops: Vec::new(),
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
    // The mutation quorum-commits on the two reachable replicas; the third
    // is a lagging witness, so reconciliation confirms the committed value.
    assert_eq!(
        coordinator.reconcile_retry(next.clone()).await.unwrap(),
        next
    );
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
        member_progress: BTreeMap::new(),
        member_feed: None,
        expected_cursor: None,
        cursor: "rf1_a".to_owned(),
        positions: BTreeMap::from([(
            RangeId::from_uuid(Uuid::from_u128(984)),
            "event-a".to_owned(),
        )]),

        quarantined: BTreeMap::new(),
        quarantine_add: BTreeMap::new(),
        quarantine_remove: BTreeMap::new(),
        skipped: BTreeMap::new(),
        skip_add: BTreeMap::new(),
        tick: 0,
        lease_ops: Vec::new(),
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
    // Replicas that only prepared (or stayed empty) are lagging witnesses;
    // the one committed vote is the live frontier.
    assert_eq!(
        coordinator.read_committed().await.unwrap(),
        Some(mutation.clone())
    );
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
        member_progress: BTreeMap::new(),
        member_feed: None,
        expected_cursor: None,
        cursor: "rf1_a".to_owned(),
        positions: BTreeMap::from([(
            RangeId::from_uuid(Uuid::from_u128(204)),
            "event-a".to_owned(),
        )]),

        quarantined: BTreeMap::new(),
        quarantine_add: BTreeMap::new(),
        quarantine_remove: BTreeMap::new(),
        skipped: BTreeMap::new(),
        skip_add: BTreeMap::new(),
        tick: 0,
        lease_ops: Vec::new(),
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
        member_progress: BTreeMap::new(),
        member_feed: None,
        expected_cursor: None,
        cursor: "rf1_a".to_owned(),
        positions: BTreeMap::from([(
            RangeId::from_uuid(Uuid::from_u128(304)),
            "event-a".to_owned(),
        )]),

        quarantined: BTreeMap::new(),
        quarantine_add: BTreeMap::new(),
        quarantine_remove: BTreeMap::new(),
        skipped: BTreeMap::new(),
        skip_add: BTreeMap::new(),
        tick: 0,
        lease_ops: Vec::new(),
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
        member_progress: BTreeMap::new(),
        member_feed: None,
        expected_cursor: None,
        cursor: "rf1_progress".to_owned(),
        positions: BTreeMap::from([(
            RangeId::from_uuid(Uuid::from_u128(104)),
            "event-1".to_owned(),
        )]),

        quarantined: BTreeMap::new(),
        quarantine_add: BTreeMap::new(),
        quarantine_remove: BTreeMap::new(),
        skipped: BTreeMap::new(),
        skip_add: BTreeMap::new(),
        tick: 0,
        lease_ops: Vec::new(),
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
        member_progress: BTreeMap::new(),
        member_feed: None,
        expected_cursor: (sequence > 1).then(|| format!("rf1_{}", sequence - 1)),
        cursor: format!("rf1_{sequence}"),
        positions: BTreeMap::from([(
            RangeId::from_uuid(Uuid::from_u128(6_000 + sequence as u128)),
            format!("position-{sequence}"),
        )]),
        quarantined: BTreeMap::new(),
        quarantine_add: BTreeMap::new(),
        quarantine_remove: BTreeMap::new(),
        skipped: BTreeMap::new(),
        skip_add: BTreeMap::new(),
        tick: 0,
        lease_ops: Vec::new(),
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
    let first = progress_mutation(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
        1,
        1,
    );
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

    let mut stale_write = progress_mutation(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
        1,
        2,
    );
    stale_write.request_id = Uuid::from_u128(5_500);
    let direct_read = coordinator.read_committed().await;
    let replica_states = coordinator
        .replica_states()
        .await
        .into_iter()
        .map(|(node, state)| {
            (
                node,
                state.map(|inspection| {
                    (
                        inspection
                            .committed
                            .map(|mutation| (mutation.sequence, mutation.ownership_epoch)),
                        inspection
                            .prepared
                            .map(|mutation| (mutation.sequence, mutation.ownership_epoch)),
                    )
                }),
            )
        })
        .collect::<Vec<_>>();
    let stale_result = coordinator.apply(stale_write).await;
    let replica_rows = nodes
        .iter()
        .map(|node| {
            (
                node.clone(),
                transport.stores[node]
                    .local_state(subscription.subscription_id)
                    .map(|state| {
                        (
                            state
                                .committed
                                .map(|mutation| (mutation.sequence, mutation.ownership_epoch)),
                            state
                                .prepared
                                .map(|mutation| (mutation.sequence, mutation.ownership_epoch)),
                        )
                    }),
            )
        })
        .collect::<Vec<_>>();
    assert!(
        matches!(
            stale_result,
            Err(SubscriptionProgressError::StaleEpoch)
                | Err(SubscriptionProgressError::InvalidAssignment)
        ),
        "stale write must be fenced, got {stale_result:?} (direct read {direct_read:?}) \
         with replica rows {replica_rows:?} and states {replica_states:?}"
    );
    assert!(matches!(
        coordinator.read_committed().await,
        Err(SubscriptionProgressError::StaleEpoch)
            | Err(SubscriptionProgressError::InvalidAssignment)
            | Err(SubscriptionProgressError::NoQuorum)
    ));

    let next = progress_mutation(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
        2,
        2,
    );
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

    let third = progress_mutation(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
        2,
        3,
    );
    recovered.apply(third.clone()).await.unwrap();
    assert_eq!(recovered.read_committed().await.unwrap(), Some(third));
}

#[tokio::test]
async fn subscription_progress_handoff_owner_vacates_a_healthy_owner_and_adopts_all() {
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
    let first = progress_mutation(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
        1,
        1,
    );
    coordinator.apply(first.clone()).await.unwrap();

    // The drained owner stays healthy and reachable: the caller chooses the
    // target, so a drain can never route ownership back to the leaving Node.
    let target = nodes.iter().find(|node| **node != owner).unwrap().clone();
    let outsider = StorageNodeId::try_new("progress-outsider").unwrap();
    assert!(matches!(
        coordinator.handoff_owner(&controller, &outsider).await,
        Err(SubscriptionProgressError::InvalidAssignment)
    ));

    let outcome = coordinator
        .handoff_owner(&controller, &target)
        .await
        .unwrap();
    assert_eq!(outcome.assignment.owner, target);
    assert_eq!(outcome.assignment.ownership_epoch, 2);
    assert_eq!(outcome.adopted.len(), 3);
    let recovered = outcome.committed.clone().unwrap();
    assert_eq!(recovered.ownership_epoch, 2);
    assert_eq!(recovered.cursor, first.cursor);
    assert_eq!(recovered.request_id, first.request_id);

    // Every replica — including the still-healthy old owner — carries the
    // re-stamped frontier, so reads at the new epoch hold quorum.
    for node in &nodes {
        let state = transport.stores[node]
            .local_state(subscription.subscription_id)
            .unwrap();
        assert_eq!(
            state.committed.as_ref().unwrap().ownership_epoch,
            2,
            "{node} was not adopted"
        );
    }

    // A retry on the stale coordinator reports Unavailable: the CAS's
    // expected epoch already moved, and the outcome is unknowable from this
    // vantage. Rebuilding on the live placement converges the same handoff
    // through adoption at the live epoch — no second CAS, no epoch bump.
    assert!(matches!(
        coordinator.handoff_owner(&controller, &target).await,
        Err(SubscriptionProgressError::Unavailable)
    ));
    let fresh = SubscriptionProgressCoordinator::for_subscription(
        &controller,
        subscription.subscription_id,
        transport.clone(),
    )
    .await
    .unwrap();
    let retried = fresh.handoff_owner(&controller, &target).await.unwrap();
    assert_eq!(retried.assignment.ownership_epoch, 2);
    assert_eq!(retried.assignment.owner, target);
    assert_eq!(retried.adopted.len(), 3);
}

#[tokio::test]
async fn subscription_progress_handoff_owner_retries_converge_after_an_ambiguous_cas() {
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
    let first = progress_mutation(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
        1,
        1,
    );
    coordinator.apply(first.clone()).await.unwrap();

    // The placement CAS commits but the new owner cannot be adopted: the step
    // reports failure while the catalog already moved.
    let target = nodes.iter().find(|node| **node != owner).unwrap().clone();
    transport.adopt_down.lock().unwrap().insert(target.clone());
    assert!(matches!(
        coordinator.handoff_owner(&controller, &target).await,
        Err(SubscriptionProgressError::NoQuorum)
    ));
    transport.adopt_down.lock().unwrap().clear();

    // Retrying with a coordinator rebuilt on the landed placement converges
    // the replicas at epoch 2 — no second CAS, no skipped epoch.
    let healed = SubscriptionProgressCoordinator::for_subscription(
        &controller,
        subscription.subscription_id,
        transport.clone(),
    )
    .await
    .unwrap();
    let outcome = healed.handoff_owner(&controller, &target).await.unwrap();
    assert_eq!(outcome.assignment.ownership_epoch, 2);
    assert_eq!(outcome.assignment.owner, target);
    assert_eq!(outcome.adopted.len(), 3);
    assert_eq!(
        healed.read_committed().await.unwrap().unwrap().cursor,
        first.cursor
    );
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
    transport.feed_ids.lock().unwrap().insert(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
    );
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

    let mut fork_left = progress_mutation(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
        2,
        1,
    );
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
    transport.feed_ids.lock().unwrap().insert(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
    );
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
                subscription.source.feed_id().unwrap(),
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
                subscription.source.feed_id().unwrap(),
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

    // The racing move's CAS already committed: this coordinator still holds
    // epoch 1, so its own CAS is refused and reports Unavailable rather than
    // adopting possibly-superseded content at the winner's epoch.
    assert!(matches!(
        coordinator.recover_lost_owner(&controller).await,
        Err(SubscriptionProgressError::Unavailable)
    ));
    // A coordinator rebuilt on the live placement converges by adopting the
    // latest committed frontier at the live epoch — no second CAS.
    let fresh = SubscriptionProgressCoordinator::for_subscription(
        &controller,
        subscription.subscription_id,
        transport.clone(),
    )
    .await
    .unwrap();
    let outcome = fresh.synchronize_placement().await.unwrap();
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

#[tokio::test]
async fn subscription_progress_replica_move_seeds_replacement_and_fences_stale_epoch() {
    let directory = TempDir::new().unwrap();
    let nodes = progress_nodes();
    let extra = StorageNodeId::try_new("storage-d").unwrap();
    let mut all_nodes = nodes.to_vec();
    all_nodes.push(extra.clone());
    let controller = progress_controller(&directory, &all_nodes);
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
    let extra_dir = TempDir::new().unwrap();
    let mut transport = TestProgressTransport::new(&dirs, &nodes);
    transport.stores.insert(
        extra.clone(),
        Arc::new(FjallSubscriptionProgressReplica::open(extra_dir.path()).unwrap()),
    );
    let transport = Arc::new(transport);
    let coordinator = SubscriptionProgressCoordinator::for_subscription(
        &controller,
        subscription.subscription_id,
        transport.clone(),
    )
    .await
    .unwrap();
    let owner = coordinator.assignment().owner.clone();
    let replaced = coordinator
        .assignment()
        .replicas
        .iter()
        .find(|node| *node != &owner)
        .cloned()
        .unwrap();
    let spare = all_nodes
        .iter()
        .find(|node| !coordinator.assignment().replicas.contains(node))
        .cloned()
        .unwrap();
    let first = progress_mutation(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
        1,
        1,
    );
    coordinator.apply(first.clone()).await.unwrap();

    // the owner slot cannot be drained through a replica move.
    assert!(matches!(
        coordinator.move_replica(&controller, &owner, &spare).await,
        Err(SubscriptionProgressError::InvalidAssignment)
    ));
    // a member cannot be swapped for an existing member.
    assert!(matches!(
        coordinator
            .move_replica(&controller, &replaced, &owner)
            .await,
        Err(SubscriptionProgressError::InvalidAssignment)
    ));

    let outcome = coordinator
        .move_replica(&controller, &replaced, &spare)
        .await
        .unwrap();
    assert_eq!(outcome.assignment.ownership_epoch, 2);
    assert_eq!(outcome.assignment.owner, owner);
    assert!(outcome.assignment.replicas.contains(&spare));
    assert!(!outcome.assignment.replicas.contains(&replaced));
    assert_eq!(outcome.adopted.len(), 3);
    let recovered = outcome.committed.clone().unwrap();
    assert_eq!(recovered.ownership_epoch, 2);
    assert_eq!(recovered.cursor, first.cursor);
    assert_eq!(recovered.request_id, first.request_id);

    // the replacement was seeded with the recovered committed value.
    let spare_state = transport.stores[&spare]
        .local_state(subscription.subscription_id)
        .unwrap();
    assert_eq!(spare_state.committed, Some(recovered.clone()));
    // the drained member kept its old-epoch row and received no writes.
    let drained_state = transport.stores[&replaced]
        .local_state(subscription.subscription_id)
        .unwrap();
    let drained_committed = drained_state.committed.unwrap();
    assert_eq!(drained_committed.ownership_epoch, 1);
    assert_eq!(drained_committed.cursor, first.cursor);

    // the drained member can no longer serve the subscription: a fresh
    // coordinator binds the moved placement and continues progress.
    let moved = SubscriptionProgressCoordinator::for_subscription(
        &controller,
        subscription.subscription_id,
        transport.clone(),
    )
    .await
    .unwrap();
    assert_eq!(moved.assignment(), &outcome.assignment);
    let next = progress_mutation(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
        2,
        2,
    );
    moved.apply(next.clone()).await.unwrap();
    assert_eq!(moved.read_committed().await.unwrap(), Some(next.clone()));
    assert_eq!(
        transport.stores[&spare]
            .local_state(subscription.subscription_id)
            .unwrap()
            .committed,
        Some(next)
    );

    // the old coordinator is fenced by the higher ownership epoch.
    let stale = progress_mutation(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
        1,
        2,
    );
    assert!(matches!(
        coordinator.apply(stale).await,
        Err(SubscriptionProgressError::StaleEpoch)
            | Err(SubscriptionProgressError::InvalidAssignment)
            | Err(SubscriptionProgressError::NoQuorum)
    ));

    // the seeded value survives a replacement-Node restart.
    drop(moved);
    drop(coordinator);
    drop(transport);
    let spare_dir = if spare == extra {
        &extra_dir
    } else {
        &dirs[all_nodes.iter().position(|node| *node == spare).unwrap()]
    };
    let reopened = FjallSubscriptionProgressReplica::open(spare_dir.path()).unwrap();
    assert_eq!(
        reopened
            .local_state(subscription.subscription_id)
            .unwrap()
            .committed
            .unwrap()
            .sequence,
        2
    );
}

#[tokio::test]
async fn subscription_progress_replica_move_drains_unavailable_member() {
    let directory = TempDir::new().unwrap();
    let nodes = progress_nodes();
    let extra = StorageNodeId::try_new("storage-d").unwrap();
    let mut all_nodes = nodes.to_vec();
    all_nodes.push(extra.clone());
    let controller = progress_controller(&directory, &all_nodes);
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
    let extra_dir = TempDir::new().unwrap();
    let mut transport = TestProgressTransport::new(&dirs, &nodes);
    transport.stores.insert(
        extra.clone(),
        Arc::new(FjallSubscriptionProgressReplica::open(extra_dir.path()).unwrap()),
    );
    let transport = Arc::new(transport);
    let coordinator = SubscriptionProgressCoordinator::for_subscription(
        &controller,
        subscription.subscription_id,
        transport.clone(),
    )
    .await
    .unwrap();
    let owner = coordinator.assignment().owner.clone();
    let replaced = coordinator
        .assignment()
        .replicas
        .iter()
        .find(|node| *node != &owner)
        .cloned()
        .unwrap();
    let spare = all_nodes
        .iter()
        .find(|node| !coordinator.assignment().replicas.contains(node))
        .cloned()
        .unwrap();
    let first = progress_mutation(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
        1,
        1,
    );
    coordinator.apply(first.clone()).await.unwrap();

    // the member being drained is completely unreachable; quorum evidence
    // from the two survivors still authorizes the move.
    for set in [
        &transport.prepare_down,
        &transport.commit_down,
        &transport.read_down,
        &transport.inspect_down,
        &transport.adopt_down,
    ] {
        set.lock().unwrap().insert(replaced.clone());
    }

    let outcome = coordinator
        .move_replica(&controller, &replaced, &spare)
        .await
        .unwrap();
    assert_eq!(outcome.assignment.ownership_epoch, 2);
    assert!(outcome.assignment.replicas.contains(&spare));
    assert!(!outcome.assignment.replicas.contains(&replaced));
    assert_eq!(outcome.adopted.len(), 3);
    assert!(!outcome.adopted.contains(&replaced));
    let committed = outcome.committed.unwrap();
    assert_eq!(committed.cursor, first.cursor);
    assert_eq!(committed.ownership_epoch, 2);

    let spare_state = transport.stores[&spare]
        .local_state(subscription.subscription_id)
        .unwrap();
    assert_eq!(spare_state.committed, Some(committed));
}

fn lease_mutation(
    base: &SubscriptionProgressMutation,
    sequence: u64,
    tick: u64,
    lease_ops: Vec<whitewater::reader::SubscriptionLeaseOp>,
) -> SubscriptionProgressMutation {
    let mut mutation = progress_mutation(
        base.subscription_id,
        base.feed_id,
        base.ownership_epoch,
        sequence,
    );
    mutation.expected_cursor = Some(base.cursor.clone());
    mutation.cursor = base.cursor.clone();
    mutation.positions = base.positions.clone();
    mutation.tick = tick;
    mutation.lease_ops = lease_ops;
    mutation
}

#[tokio::test]
async fn subscription_member_leases_replicate_survive_restart_and_recover() {
    use whitewater::reader::SubscriptionLeaseOp;

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
    let member = Uuid::from_u128(11_001);
    let other = Uuid::from_u128(11_002);
    let work = Uuid::from_u128(11_003);

    let frontier = progress_mutation(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
        1,
        1,
    );
    coordinator.apply(frontier.clone()).await.unwrap();

    // join and claim commit as one quorum mutation on every replica.
    let join = lease_mutation(
        &frontier,
        2,
        10,
        vec![
            SubscriptionLeaseOp::Join {
                member_id: member,
                member_epoch: 1,
            },
            SubscriptionLeaseOp::Claim {
                work_id: work,
                member_id: member,
                member_epoch: 1,
                lease_ticks: 30,
            },
        ],
    );
    coordinator.apply(join.clone()).await.unwrap();
    for node in &nodes {
        let state = transport.stores[node]
            .local_state(subscription.subscription_id)
            .unwrap();
        assert_eq!(state.members.member_epochs.get(&member), Some(&1));
        let lease = state.members.leases.get(&work).unwrap();
        assert_eq!(
            (lease.member_id, lease.lease_epoch, lease.expires_at_tick),
            (member, 1, 40)
        );
        assert_eq!(state.committed.as_ref().unwrap().cursor, frontier.cursor);
    }

    // a stale member epoch cannot take the work while the lease is live.
    let steal = lease_mutation(
        &frontier,
        3,
        20,
        vec![
            SubscriptionLeaseOp::Join {
                member_id: other,
                member_epoch: 1,
            },
            SubscriptionLeaseOp::Claim {
                work_id: work,
                member_id: other,
                member_epoch: 1,
                lease_ticks: 10,
            },
        ],
    );
    assert!(matches!(
        coordinator.apply(steal).await,
        Err(SubscriptionProgressError::Lease(
            whitewater::reader::SubscriptionLeaseError::Busy
        ))
    ));

    // after expiry the other member takes over at a higher lease epoch.
    let mut join_only = lease_mutation(
        &frontier,
        3,
        20,
        vec![SubscriptionLeaseOp::Join {
            member_id: other,
            member_epoch: 1,
        }],
    );
    join_only.request_id = Uuid::from_u128(12_003);
    coordinator.apply(join_only.clone()).await.unwrap();
    let mut takeover = lease_mutation(
        &frontier,
        4,
        45,
        vec![SubscriptionLeaseOp::Claim {
            work_id: work,
            member_id: other,
            member_epoch: 1,
            lease_ticks: 20,
        }],
    );
    takeover.request_id = Uuid::from_u128(12_004);
    coordinator.apply(takeover.clone()).await.unwrap();
    for node in &nodes {
        let state = transport.stores[node]
            .local_state(subscription.subscription_id)
            .unwrap();
        let lease = state.members.leases.get(&work).unwrap();
        assert_eq!(
            (lease.member_id, lease.lease_epoch, lease.expires_at_tick),
            (other, 2, 65)
        );
    }

    // the owner dies; member state recovers with committed progress.
    let owner = coordinator.assignment().owner.clone();
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
    let lease = outcome.members.leases.get(&work).unwrap();
    assert_eq!((lease.member_id, lease.lease_epoch), (other, 2));
    assert_eq!(outcome.members.member_epochs.get(&other), Some(&1));
    // adopted replicas carry the recovered member snapshot under the new epoch.
    for node in &outcome.adopted {
        let state = transport.stores[node]
            .local_state(subscription.subscription_id)
            .unwrap();
        assert_eq!(state.members, outcome.members);
        assert_eq!(state.committed.as_ref().unwrap().ownership_epoch, 2);
    }

    // the recovered set continues committing lease transitions.
    let recovered = SubscriptionProgressCoordinator::for_subscription(
        &controller,
        subscription.subscription_id,
        transport.clone(),
    )
    .await
    .unwrap();
    let mut release = lease_mutation(&takeover, 5, 50, vec![]);
    release.ownership_epoch = 2;
    release.request_id = Uuid::from_u128(12_005);
    release.lease_ops = vec![SubscriptionLeaseOp::Release {
        work_id: work,
        member_id: other,
        member_epoch: 1,
        lease_epoch: 2,
    }];
    recovered.apply(release.clone()).await.unwrap();
    for node in &outcome.adopted {
        let state = transport.stores[node]
            .local_state(subscription.subscription_id)
            .unwrap();
        assert!(state.members.leases.is_empty());
    }

    // member state survives a full replica restart.
    drop(recovered);
    drop(coordinator);
    drop(transport);
    let transport = Arc::new(TestProgressTransport::new(&dirs, &nodes));
    for node in &nodes {
        let state = transport.stores[node]
            .local_state(subscription.subscription_id)
            .unwrap();
        if *node == owner {
            // the fenced-off owner never learned the release: it still holds
            // its epoch-1 committed value with the old lease materialized.
            let committed = state.committed.as_ref().unwrap();
            assert_eq!(committed.ownership_epoch, 1);
            assert_eq!(committed.sequence, 4);
            assert!(state.members.leases.contains_key(&work));
        } else {
            let committed = state.committed.as_ref().unwrap();
            assert_eq!(committed.ownership_epoch, 2);
            assert_eq!(committed.sequence, 5);
            assert_eq!(state.members.member_epochs.get(&member), Some(&1));
            assert!(state.members.leases.is_empty());
        }
    }
}

#[tokio::test]
async fn subscription_member_api_fences_acks_by_quorum_leases() {
    use whitewater::reader::{
        SubscriptionLeaseOp, SubscriptionQuarantineDelta, SubscriptionWorkLease,
    };

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
    let member = Uuid::from_u128(21_001);
    let work = Uuid::from_u128(21_002);

    // member operations need an established committed frontier.
    assert!(matches!(
        coordinator
            .apply_member_ops(
                Uuid::from_u128(22_001),
                1,
                vec![SubscriptionLeaseOp::Join {
                    member_id: member,
                    member_epoch: 1,
                }]
            )
            .await,
        Err(SubscriptionProgressError::Conflict)
    ));

    let frontier = progress_mutation(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
        1,
        1,
    );
    coordinator.apply(frontier.clone()).await.unwrap();

    coordinator
        .apply_member_ops(
            Uuid::from_u128(22_002),
            10,
            vec![SubscriptionLeaseOp::Join {
                member_id: member,
                member_epoch: 1,
            }],
        )
        .await
        .unwrap();
    coordinator
        .apply_member_ops(
            Uuid::from_u128(22_003),
            10,
            vec![SubscriptionLeaseOp::Claim {
                work_id: work,
                member_id: member,
                member_epoch: 1,
                lease_ticks: 30,
            }],
        )
        .await
        .unwrap();
    let members = coordinator.member_state().await.unwrap();
    let lease = members.leases.get(&work).unwrap();
    let grant = SubscriptionWorkLease {
        work_id: work,
        member_id: member,
        member_epoch: 1,
        lease_epoch: lease.lease_epoch,
        expires_at_tick: lease.expires_at_tick,
        exhausted_applied: false,
    };
    assert_eq!(grant.expires_at_tick, 40);
    assert!(coordinator.can_member_ack(&grant, 20).await.unwrap());

    // a stale member epoch or stale lease epoch cannot acknowledge.
    let wrong_member = SubscriptionWorkLease {
        member_epoch: 2,
        ..grant.clone()
    };
    assert!(!coordinator.can_member_ack(&wrong_member, 20).await.unwrap());
    assert!(matches!(
        coordinator
            .acknowledge(
                &wrong_member,
                20,
                Uuid::from_u128(22_004),
                "rf1_acked".to_owned(),
                frontier.positions.clone(),
                SubscriptionQuarantineDelta::default()
            )
            .await,
        Err(SubscriptionProgressError::Lease(
            whitewater::reader::SubscriptionLeaseError::StaleLease
        ))
    ));
    let wrong_lease = SubscriptionWorkLease {
        lease_epoch: grant.lease_epoch + 1,
        ..grant.clone()
    };
    assert!(matches!(
        coordinator
            .acknowledge(
                &wrong_lease,
                20,
                Uuid::from_u128(22_005),
                "rf1_acked".to_owned(),
                frontier.positions.clone(),
                SubscriptionQuarantineDelta::default()
            )
            .await,
        Err(SubscriptionProgressError::Lease(
            whitewater::reader::SubscriptionLeaseError::StaleLease
        ))
    ));

    // a valid acknowledgement moves the frontier and releases the lease
    // atomically on every replica.
    let acked = coordinator
        .acknowledge(
            &grant,
            20,
            Uuid::from_u128(22_006),
            "rf1_acked".to_owned(),
            BTreeMap::from([(
                RangeId::from_uuid(Uuid::from_u128(23_001)),
                "ack-pos".to_owned(),
            )]),
            SubscriptionQuarantineDelta::default(),
        )
        .await
        .unwrap();
    assert_eq!(acked.cursor, "rf1_acked");
    for node in &nodes {
        let state = transport.stores[node]
            .local_state(subscription.subscription_id)
            .unwrap();
        assert!(state.members.leases.is_empty());
        assert_eq!(state.committed.as_ref().unwrap().cursor, "rf1_acked");
    }
    // after release the grant no longer authorizes anything.
    assert!(!coordinator.can_member_ack(&grant, 21).await.unwrap());
    // an expired lease can never acknowledge even if it still exists.
    let reclaimed = coordinator
        .apply_member_ops(
            Uuid::from_u128(22_007),
            30,
            vec![SubscriptionLeaseOp::Claim {
                work_id: work,
                member_id: member,
                member_epoch: 1,
                lease_ticks: 5,
            }],
        )
        .await
        .unwrap();
    assert_eq!(reclaimed.sequence, acked.sequence + 1);
    let members = coordinator.member_state().await.unwrap();
    let renewed_grant = SubscriptionWorkLease {
        work_id: work,
        member_id: member,
        member_epoch: 1,
        lease_epoch: members.leases[&work].lease_epoch,
        expires_at_tick: members.leases[&work].expires_at_tick,
        exhausted_applied: false,
    };
    assert!(!coordinator
        .can_member_ack(&renewed_grant, 36)
        .await
        .unwrap());
}

#[tokio::test]
async fn owner_available_reports_progress_owner_liveness() {
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
    controller
        .execute("CREATE SPACE orders; CREATE FEED orders.events; CREATE SUBSCRIPTION orders.billing FROM orders.events;")
        .await
        .unwrap();
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

    assert!(coordinator.owner_available().await);
    transport
        .inspect_down
        .lock()
        .unwrap()
        .insert(coordinator.assignment().owner.clone());
    assert!(!coordinator.owner_available().await);
}

#[tokio::test]
async fn epoch_stale_replica_rows_are_lagging_witnesses_for_reads_and_writes() {
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
    for sequence in 1..=2 {
        let mutation = progress_mutation(
            subscription.subscription_id,
            subscription.source.feed_id().unwrap(),
            1,
            sequence,
        );
        for node in &nodes {
            plant_committed(&transport, node, &mutation);
        }
    }
    // Recovery advances the catalog to epoch 2 and adopts the recovered
    // frontier at the new epoch. Leave one replica un-adopted so its row
    // stays fenced at ownership epoch 1. The recovery owner is the first
    // replica holding the recovered frontier, so lag the last member.
    let lagging = coordinator
        .assignment()
        .replicas
        .as_array()
        .last()
        .unwrap()
        .clone();
    transport.adopt_down.lock().unwrap().insert(lagging.clone());
    let outcome = coordinator.recover_lost_owner(&controller).await.unwrap();
    assert_eq!(outcome.assignment.ownership_epoch, 2);
    let recovered = SubscriptionProgressCoordinator::for_subscription(
        &controller,
        subscription.subscription_id,
        transport.clone(),
    )
    .await
    .unwrap();
    assert_eq!(recovered.assignment().ownership_epoch, 2);

    // The epoch-stale row is a lagging witness, not a contradiction: reads
    // return the current frontier and member mutations still commit through
    // the current replicas.
    let committed = recovered.read_committed().await.unwrap().unwrap();
    assert_eq!(committed.sequence, 2);
    assert_eq!(committed.ownership_epoch, 2);
    recovered.member_state().await.unwrap();
    let applied = recovered
        .apply_member_ops(
            Uuid::from_u128(9_001),
            1,
            vec![whitewater::reader::SubscriptionLeaseOp::Join {
                member_id: Uuid::from_u128(9_002),
                member_epoch: 1,
            }],
        )
        .await
        .unwrap();
    assert_eq!(applied.sequence, 3);

    // Retry reconciliation likewise treats the stale row as behind rather
    // than as a conflicting fork.
    let mut retry = applied.clone();
    retry.sequence = applied.sequence.checked_add(1).unwrap();
    retry.request_id = Uuid::from_u128(9_003);
    retry.expected_cursor = Some(applied.cursor.clone());
    retry.cursor = "rf1_4".to_owned();
    retry.lease_ops = vec![whitewater::reader::SubscriptionLeaseOp::Claim {
        work_id: Uuid::from_u128(9_004),
        member_id: Uuid::from_u128(9_002),
        member_epoch: 1,
        lease_ticks: 10,
    }];
    recovered.reconcile_retry(retry).await.unwrap();

    // The lagging replica was never admitted into the epoch-2 mutations and
    // remains fenced at epoch 1 until a future adoption heals it.
    let stale = transport.stores[&lagging]
        .local_committed(subscription.subscription_id)
        .unwrap()
        .unwrap();
    assert_eq!(stale.sequence, 2);
    assert_eq!(stale.ownership_epoch, 1);
}

#[tokio::test]
async fn synchronize_placement_heals_replicas_stranded_behind_a_committed_move() {
    // Live-fault regression: a placement CAS commits before adoption runs.
    // When adoption misses replicas their rows stay stamped at the prior
    // epoch and every committed read fences them as stale, so quorum never
    // forms and member operations wedge permanently. Re-adopting the
    // recovered frontier at the live epoch must heal without committing
    // another ownership move.
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
    let mutation = progress_mutation(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
        1,
        1,
    );
    coordinator.apply(mutation.clone()).await.unwrap();

    for node in &nodes {
        transport.adopt_down.lock().unwrap().insert(node.clone());
    }
    assert!(matches!(
        coordinator.recover_lost_owner(&controller).await,
        Err(SubscriptionProgressError::NoQuorum)
    ));
    let moved = controller
        .active_subscription_progress_assignment_by_id(subscription.subscription_id)
        .await
        .unwrap();
    assert_eq!(moved.ownership_epoch, 2);
    let stranded = SubscriptionProgressCoordinator::new(moved.clone(), transport.clone());
    assert!(matches!(
        stranded.read_committed().await,
        Err(SubscriptionProgressError::NoQuorum)
    ));

    transport.adopt_down.lock().unwrap().clear();
    let outcome = stranded.synchronize_placement().await.unwrap();
    assert_eq!(outcome.assignment.ownership_epoch, 2);
    let committed = stranded.read_committed().await.unwrap().unwrap();
    assert_eq!(committed.sequence, 1);
    assert_eq!(committed.ownership_epoch, 2);
    for node in &nodes {
        assert_eq!(
            transport.stores[node]
                .local_committed(subscription.subscription_id)
                .unwrap()
                .map(|mutation| mutation.ownership_epoch),
            Some(2)
        );
    }
}

#[tokio::test]
async fn recovery_then_member_ops_when_a_survivor_lags_one_sequence() {
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
    let survivors: Vec<StorageNodeId> = nodes
        .iter()
        .filter(|node| **node != owner)
        .cloned()
        .collect();
    // One survivor lags exactly one committed sequence behind the other.
    let first = progress_mutation(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
        1,
        1,
    );
    let second = progress_mutation(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
        1,
        2,
    );
    plant_committed(&transport, &survivors[0], &first);
    plant_committed(&transport, &survivors[1], &first);
    plant_committed(&transport, &survivors[1], &second);
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
    assert_eq!(outcome.committed.as_ref().unwrap().sequence, 2);
    let recovered = SubscriptionProgressCoordinator::for_subscription(
        &controller,
        subscription.subscription_id,
        transport.clone(),
    )
    .await
    .unwrap();
    let applied = recovered
        .apply_member_ops(
            Uuid::from_u128(7_777),
            1,
            vec![whitewater::reader::SubscriptionLeaseOp::Join {
                member_id: Uuid::from_u128(8_888),
                member_epoch: 1,
            }],
        )
        .await
        .unwrap();
    assert_eq!(applied.sequence, 3);
}

#[tokio::test]
async fn subscription_acks_carry_a_bounded_quarantine_set() {
    use whitewater::reader::{
        SubscriptionLeaseOp, SubscriptionQuarantineDelta, SubscriptionWorkLease,
    };

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
    let member = Uuid::from_u128(31_001);
    let work = Uuid::from_u128(31_002);

    let frontier = progress_mutation(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
        1,
        1,
    );
    coordinator.apply(frontier.clone()).await.unwrap();
    let range = *frontier.positions.keys().next().unwrap();
    coordinator
        .apply_member_ops(
            Uuid::from_u128(32_001),
            10,
            vec![SubscriptionLeaseOp::Join {
                member_id: member,
                member_epoch: 1,
            }],
        )
        .await
        .unwrap();
    coordinator
        .apply_member_ops(
            Uuid::from_u128(32_002),
            10,
            vec![SubscriptionLeaseOp::Claim {
                work_id: work,
                member_id: member,
                member_epoch: 1,
                lease_ticks: 30,
            }],
        )
        .await
        .unwrap();
    let members = coordinator.member_state().await.unwrap();
    let lease = members.leases.get(&work).unwrap();
    let grant = SubscriptionWorkLease {
        work_id: work,
        member_id: member,
        member_epoch: 1,
        lease_epoch: lease.lease_epoch,
        expires_at_tick: lease.expires_at_tick,
        exhausted_applied: false,
    };

    // An ack may only quarantine positions inside the ranges it commits;
    // naming a foreign range conflicts rather than writing false evidence.
    let foreign = RangeId::from_uuid(Uuid::from_u128(33_999));
    assert!(matches!(
        coordinator
            .acknowledge(
                &grant,
                20,
                Uuid::from_u128(32_003),
                "rf1_q".to_owned(),
                frontier.positions.clone(),
                SubscriptionQuarantineDelta {
                    add: BTreeMap::from([(foreign, vec!["poison-1".to_owned()])]),
                    remove: BTreeMap::new(),
                }
            )
            .await,
        Err(SubscriptionProgressError::Conflict)
    ));

    // Quarantined positions ride the committed mutation on every replica.
    let acked = coordinator
        .acknowledge(
            &grant,
            20,
            Uuid::from_u128(32_004),
            "rf1_q".to_owned(),
            frontier.positions.clone(),
            SubscriptionQuarantineDelta {
                add: BTreeMap::from([(range, vec!["poison-1".to_owned(), "poison-2".to_owned()])]),
                remove: BTreeMap::new(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        acked.quarantined[&range],
        vec!["poison-1".to_owned(), "poison-2".to_owned()]
    );
    for node in &nodes {
        let state = transport.stores[node]
            .local_state(subscription.subscription_id)
            .unwrap();
        assert_eq!(
            state.committed.as_ref().unwrap().quarantined[&range].len(),
            2
        );
    }

    // A later ack releases one quarantined position while the frontier
    // keeps moving; the cumulative set shrinks but stays durable.
    coordinator
        .apply_member_ops(
            Uuid::from_u128(32_005),
            30,
            vec![SubscriptionLeaseOp::Claim {
                work_id: work,
                member_id: member,
                member_epoch: 1,
                lease_ticks: 30,
            }],
        )
        .await
        .unwrap();
    let members = coordinator.member_state().await.unwrap();
    let lease = members.leases.get(&work).unwrap();
    let grant = SubscriptionWorkLease {
        lease_epoch: lease.lease_epoch,
        expires_at_tick: lease.expires_at_tick,
        exhausted_applied: false,
        ..grant
    };
    let acked = coordinator
        .acknowledge(
            &grant,
            40,
            Uuid::from_u128(32_006),
            "rf1_q2".to_owned(),
            frontier.positions.clone(),
            SubscriptionQuarantineDelta {
                add: BTreeMap::new(),
                remove: BTreeMap::from([(range, vec!["poison-1".to_owned()])]),
            },
        )
        .await
        .unwrap();
    assert_eq!(acked.quarantined[&range], vec!["poison-2".to_owned()]);
}

/// A placement authority that fails a bounded number of CAS attempts before
/// delegating to the real catalog — exercises the inconclusive-error branch
/// of `commit_owner_move` where the outcome is unknowable to the caller.
struct FlakyPlacementAuthority {
    inner: ControlController,
    fail_first: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl SubscriptionPlacementAuthority for FlakyPlacementAuthority {
    async fn execute_placement_commands(
        &self,
        commands: Vec<Command>,
    ) -> Result<(), SubscriptionProgressError> {
        if self
            .fail_first
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
            > 0
        {
            return Err(SubscriptionProgressError::Unavailable);
        }
        self.inner.execute_placement_commands(commands).await
    }

    async fn progress_assignment(
        &self,
        subscription_id: Uuid,
    ) -> Option<SubscriptionProgressAssignment> {
        self.inner.progress_assignment(subscription_id).await
    }
}

#[tokio::test]
async fn subscription_progress_recovery_reports_unavailable_on_failed_cas_then_converges() {
    // Regression: a failed placement CAS used to drive adoption of the
    // attempt's recovered state at whatever epoch the catalog reported —
    // fencing replicas on content a racing recovery already wrote at that
    // epoch. The CAS error must surface as Unavailable so the caller rebuilds
    // on the live placement; a following attempt converges cleanly.
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
    let first = progress_mutation(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
        1,
        1,
    );
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

    // The first CAS attempt fails; recovery must report Unavailable rather
    // than adopting recovered content at an unknown catalog epoch.
    let authority = FlakyPlacementAuthority {
        inner: controller,
        fail_first: std::sync::atomic::AtomicUsize::new(1),
    };
    let result = coordinator.recover_lost_owner(&authority).await;
    assert!(matches!(
        result,
        Err(SubscriptionProgressError::Unavailable)
    ));
    assert_eq!(
        authority
            .inner
            .active_subscription_progress_assignment_by_id(subscription.subscription_id)
            .await
            .unwrap()
            .ownership_epoch,
        1,
        "a failed CAS must not advance the ownership epoch"
    );

    // The retried attempt commits normally and adopts the recovered frontier.
    let outcome = coordinator.recover_lost_owner(&authority).await.unwrap();
    assert_eq!(outcome.assignment.ownership_epoch, 2);
    assert_ne!(outcome.assignment.owner, owner);
    assert!(outcome.adopted.contains(&outcome.assignment.owner));
}

#[tokio::test]
async fn subscription_progress_recovery_lost_cas_race_returns_unavailable() {
    // When a racing recovery wins the CAS, the losing attempt must report
    // Unavailable — not adopt its own recovered state at the winner's epoch,
    // which would conflict with the winner's already-adopted content.
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
    let first = progress_mutation(
        subscription.subscription_id,
        subscription.source.feed_id().unwrap(),
        1,
        1,
    );
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

    // The racing CAS lands first: the live placement is already epoch 2 with
    // a surviving owner while this coordinator still holds epoch 1.
    let winner = controller
        .active_subscription_progress_assignment_by_id(subscription.subscription_id)
        .await
        .unwrap()
        .replicas
        .iter()
        .find(|node| *node != &owner)
        .unwrap()
        .clone();
    controller
        .execute_placement_commands(vec![Command::RecoverSubscriptionProgressOwner {
            subscription_id: subscription.subscription_id,
            expected_ownership_epoch: 1,
            new_owner: winner.clone(),
        }])
        .await
        .unwrap();

    // The losing attempt's CAS is refused (expected epoch 1 is stale); it
    // must report Unavailable instead of adopting stale content at epoch 2.
    let result = coordinator.recover_lost_owner(&controller).await;
    assert!(
        matches!(result, Err(SubscriptionProgressError::Unavailable)),
        "losing CAS must report Unavailable, got {result:?}"
    );

    // The retried attempt on the live placement converges: the winner's owner
    // answers, the coordinator heals stale rows through adoption.
    let recovered = SubscriptionProgressCoordinator::for_subscription(
        &controller,
        subscription.subscription_id,
        transport.clone(),
    )
    .await
    .unwrap();
    assert_eq!(recovered.assignment().ownership_epoch, 2);
    let healed = recovered.synchronize_placement().await.unwrap();
    assert_eq!(healed.assignment.owner, winner);
    assert_eq!(healed.assignment.ownership_epoch, 2);
    assert_eq!(healed.committed.unwrap().cursor, first.cursor);
}

#[tokio::test]
async fn control_lists_every_active_subscription_progress_placement() {
    let directory = TempDir::new().unwrap();
    let nodes = progress_nodes();
    let controller = progress_controller(&directory, &nodes);
    controller.execute("CREATE SPACE orders; CREATE FEED orders.events; CREATE SUBSCRIPTION orders.billing FROM orders.events; CREATE SUBSCRIPTION orders.shipping FROM orders.events;").await.unwrap();
    let assignments = controller.active_subscription_progress_assignments().await;
    assert_eq!(assignments.len(), 2);
    assert!(assignments
        .iter()
        .all(|assignment| assignment.ownership_epoch == 1));
}

/// A sequence-1 Domain-source row: one frontier entry per attached member
/// Feed under the bookkeeping aggregate Cursor, like first-member join
/// or Pipe bootstrap commits for a Domain-scoped Subscription.
fn domain_progress_mutation(
    subscription_id: Uuid,
    domain_id: Uuid,
    members: &[(Uuid, &str)],
) -> SubscriptionProgressMutation {
    use whitewater::reader::MemberFeedProgress;
    SubscriptionProgressMutation {
        subscription_id,
        feed_id: domain_id,
        ownership_epoch: 1,
        sequence: 1,
        request_id: Uuid::from_u128(71_001),
        member_progress: members
            .iter()
            .enumerate()
            .map(|(index, (feed, cursor))| {
                (
                    *feed,
                    MemberFeedProgress {
                        cursor: cursor.to_string(),
                        positions: BTreeMap::from([(
                            RangeId::from_uuid(Uuid::from_u128(31_000 + index as u128)),
                            format!("pos-{cursor}"),
                        )]),
                        quarantined: BTreeMap::new(),
                        skipped: BTreeMap::new(),
                    },
                )
            })
            .collect(),
        member_feed: None,
        expected_cursor: None,
        cursor: "domain".to_owned(),
        positions: BTreeMap::new(),
        quarantined: BTreeMap::new(),
        quarantine_add: BTreeMap::new(),
        quarantine_remove: BTreeMap::new(),
        skipped: BTreeMap::new(),
        skip_add: BTreeMap::new(),
        tick: 0,
        lease_ops: Vec::new(),
    }
}

async fn domain_subscription(
    controller: &ControlController,
) -> whitewater::control::SubscriptionDefinition {
    controller
        .execute(
            "CREATE DOMAIN orders; CREATE FEED orders.alpha; CREATE FEED orders.beta; \
             CREATE SUBSCRIPTION orders.all FROM DOMAIN orders;",
        )
        .await
        .unwrap();
    controller
        .active_subscription_by_name("orders.all")
        .await
        .unwrap()
}

#[tokio::test]
async fn domain_subscription_member_frontiers_advance_independently() {
    use whitewater::reader::{
        MemberFeedProgress, SubscriptionLeaseOp, SubscriptionQuarantineDelta, SubscriptionWorkLease,
    };

    let directory = TempDir::new().unwrap();
    let nodes = progress_nodes();
    let controller = progress_controller(&directory, &nodes);
    let subscription = domain_subscription(&controller).await;
    let domain_id = subscription.source.domain_id().unwrap();
    let alpha = controller
        .active_feed_by_name("orders.alpha")
        .await
        .unwrap()
        .feed_id;
    let beta = controller
        .active_feed_by_name("orders.beta")
        .await
        .unwrap()
        .feed_id;
    // The catalog tracked both Feeds as attached members at declaration.
    let memberships = controller
        .subscription_feed_memberships(subscription.subscription_id)
        .await;
    assert_eq!(
        memberships
            .values()
            .map(|member| member.state)
            .collect::<Vec<_>>(),
        vec![
            whitewater::control::SubscriptionFeedState::Active,
            whitewater::control::SubscriptionFeedState::Active
        ]
    );

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

    coordinator
        .apply(domain_progress_mutation(
            subscription.subscription_id,
            domain_id,
            &[(alpha, "alpha_0"), (beta, "beta_0")],
        ))
        .await
        .unwrap();

    let member = Uuid::from_u128(33_001);
    let work = Uuid::from_u128(33_002);
    coordinator
        .apply_member_ops(
            Uuid::from_u128(33_010),
            10,
            vec![SubscriptionLeaseOp::Join {
                member_id: member,
                member_epoch: 1,
            }],
        )
        .await
        .unwrap();
    coordinator
        .apply_member_ops(
            Uuid::from_u128(33_011),
            10,
            vec![SubscriptionLeaseOp::Claim {
                work_id: work,
                member_id: member,
                member_epoch: 1,
                lease_ticks: 30,
            }],
        )
        .await
        .unwrap();
    let members = coordinator.member_state().await.unwrap();
    let grant = SubscriptionWorkLease {
        work_id: work,
        member_id: member,
        member_epoch: 1,
        lease_epoch: members.leases[&work].lease_epoch,
        expires_at_tick: members.leases[&work].expires_at_tick,
        exhausted_applied: false,
    };

    // Acknowledging alpha moves only its entry; beta's frontier — and the
    // row's aggregate Cursor — stay exactly as committed. A quarantine
    // delta lands on alpha's evidence only.
    let alpha_range = RangeId::from_uuid(Uuid::from_u128(41_001));
    let acked = coordinator
        .acknowledge_member(
            &grant,
            20,
            Uuid::from_u128(33_012),
            alpha,
            "alpha_1".to_owned(),
            BTreeMap::from([(alpha_range, "alpha-p1".to_owned())]),
            SubscriptionQuarantineDelta {
                add: BTreeMap::from([(alpha_range, vec!["poison-1".to_owned()])]),
                remove: BTreeMap::new(),
            },
        )
        .await
        .unwrap();
    assert_eq!(acked.member_feed, Some(alpha));
    assert_eq!(acked.cursor, "alpha_1");
    assert_eq!(acked.member_progress[&alpha].cursor, "alpha_1");
    assert_eq!(acked.member_progress[&beta].cursor, "beta_0");
    assert_eq!(
        acked.member_progress[&alpha].quarantined[&alpha_range],
        vec!["poison-1".to_owned()]
    );
    assert!(acked.member_progress[&beta].quarantined.is_empty());
    for node in &nodes {
        let committed = transport.stores[node]
            .local_committed(subscription.subscription_id)
            .unwrap()
            .unwrap();
        assert_eq!(committed.member_progress[&beta].cursor, "beta_0");
    }

    // Re-claim, then acknowledge beta: its CAS rode beta's own committed
    // entry Cursor, so alpha's movement never rewrote beta's chain.
    coordinator
        .apply_member_ops(
            Uuid::from_u128(33_013),
            21,
            vec![SubscriptionLeaseOp::Claim {
                work_id: work,
                member_id: member,
                member_epoch: 1,
                lease_ticks: 30,
            }],
        )
        .await
        .unwrap();
    let members = coordinator.member_state().await.unwrap();
    let grant = SubscriptionWorkLease {
        work_id: work,
        member_id: member,
        member_epoch: 1,
        lease_epoch: members.leases[&work].lease_epoch,
        expires_at_tick: members.leases[&work].expires_at_tick,
        exhausted_applied: false,
    };
    let acked_b = coordinator
        .acknowledge_member(
            &grant,
            40,
            Uuid::from_u128(33_014),
            beta,
            "beta_1".to_owned(),
            BTreeMap::from([(
                RangeId::from_uuid(Uuid::from_u128(41_002)),
                "bp1".to_owned(),
            )]),
            SubscriptionQuarantineDelta::default(),
        )
        .await
        .unwrap();
    assert_eq!(acked_b.member_progress[&alpha].cursor, "alpha_1");
    assert_eq!(acked_b.member_progress[&beta].cursor, "beta_1");
    assert_eq!(
        acked_b.member_progress[&alpha].quarantined[&alpha_range],
        vec!["poison-1".to_owned()]
    );

    // The synthesized member view scopes evidence to the named Feed only:
    // beta sees no quarantine and none of alpha's delta.
    let row = coordinator.read_committed().await.unwrap().unwrap();
    let view = row.member_view(beta).unwrap();
    assert_eq!(view.feed_id, beta);
    assert_eq!(view.cursor, "beta_1");
    assert!(view.quarantined.is_empty());
    assert!(view.quarantine_add.is_empty());
    let alpha_view = row.member_view(alpha).unwrap();
    assert_eq!(
        alpha_view.quarantined[&alpha_range],
        vec!["poison-1".to_owned()]
    );
    assert!(row.member_view(Uuid::from_u128(999)).is_none());

    // attach_member_feeds adds a discovered Feed without moving anything.
    let gamma = Uuid::from_u128(42_000);
    coordinator
        .attach_member_feeds(
            Uuid::from_u128(33_015),
            41,
            BTreeMap::from([(
                gamma,
                MemberFeedProgress {
                    cursor: "beginning".to_owned(),
                    positions: BTreeMap::from([(
                        RangeId::from_uuid(Uuid::from_u128(41_003)),
                        String::new(),
                    )]),
                    quarantined: BTreeMap::new(),
                    skipped: BTreeMap::new(),
                },
            )]),
        )
        .await
        .unwrap();
    let row = coordinator.read_committed().await.unwrap().unwrap();
    assert!(row.member_progress.contains_key(&gamma));
    assert_eq!(row.member_progress[&alpha].cursor, "alpha_1");
    assert_eq!(row.member_feed, None);
    // Re-attaching a tracked Feed can never silently rewind it.
    assert!(matches!(
        coordinator
            .attach_member_feeds(
                Uuid::from_u128(33_016),
                42,
                BTreeMap::from([(
                    gamma,
                    MemberFeedProgress {
                        cursor: "beginning".to_owned(),
                        positions: BTreeMap::new(),
                        quarantined: BTreeMap::new(),
                        skipped: BTreeMap::new(),
                    },
                )]),
            )
            .await,
        Err(SubscriptionProgressError::Conflict)
    ));
}

#[tokio::test]
async fn domain_progress_validation_fences_delta_and_drift_violations() {
    use whitewater::reader::MemberFeedProgress;

    let directory = TempDir::new().unwrap();
    let nodes = progress_nodes();
    let controller = progress_controller(&directory, &nodes);
    let subscription = domain_subscription(&controller).await;
    let domain_id = subscription.source.domain_id().unwrap();
    let alpha = Uuid::from_u128(51_001);
    let beta = Uuid::from_u128(51_002);
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
    let seed = domain_progress_mutation(
        subscription.subscription_id,
        domain_id,
        &[(alpha, "alpha_0"), (beta, "beta_0")],
    );
    coordinator.apply(seed.clone()).await.unwrap();

    let entry = |cursor: &str| MemberFeedProgress {
        cursor: cursor.to_owned(),
        positions: BTreeMap::from([(RangeId::from_uuid(Uuid::from_u128(52_000)), "p".to_owned())]),
        quarantined: BTreeMap::new(),
        skipped: BTreeMap::new(),
    };
    #[allow(clippy::too_many_arguments)]
    fn base(
        subscription_id: Uuid,
        domain_id: Uuid,
        member_feed: Option<Uuid>,
        member_progress: BTreeMap<Uuid, MemberFeedProgress>,
        cursor: &str,
        expected: &str,
        request_id: Uuid,
    ) -> SubscriptionProgressMutation {
        SubscriptionProgressMutation {
            subscription_id,
            feed_id: domain_id,
            ownership_epoch: 1,
            sequence: 2,
            request_id,
            member_progress,
            member_feed,
            expected_cursor: Some(expected.to_owned()),
            cursor: cursor.to_owned(),
            positions: BTreeMap::from([(
                RangeId::from_uuid(Uuid::from_u128(52_000)),
                "p".to_owned(),
            )]),
            quarantined: BTreeMap::new(),
            quarantine_add: BTreeMap::new(),
            quarantine_remove: BTreeMap::new(),
            skipped: BTreeMap::new(),
            skip_add: BTreeMap::new(),
            tick: 1,
            lease_ops: Vec::new(),
        }
    }
    let sid = subscription.subscription_id;

    // (a) named entry whose top-level delta does not restate it exactly.
    let forged = base(
        sid,
        domain_id,
        Some(alpha),
        BTreeMap::from([
            (alpha, entry("alpha_X")),
            (beta, seed.member_progress[&beta].clone()),
        ]),
        "alpha_Y",
        "alpha_0",
        Uuid::from_u128(53_001),
    );
    assert!(matches!(
        coordinator.apply(forged).await,
        Err(SubscriptionProgressError::Conflict)
    ));

    // (b) a drifted non-named entry cannot ride a valid alpha delta.
    let forged = base(
        sid,
        domain_id,
        Some(alpha),
        BTreeMap::from([(alpha, entry("alpha_1")), (beta, entry("beta_X"))]),
        "alpha_1",
        "alpha_0",
        Uuid::from_u128(53_002),
    );
    assert!(matches!(
        coordinator.apply(forged).await,
        Err(SubscriptionProgressError::Conflict)
    ));

    // (c) a pure bookkeeping/attach mutation carries no per-Feed evidence.
    let mut forged = base(
        sid,
        domain_id,
        None,
        seed.member_progress.clone(),
        "domain",
        "domain",
        Uuid::from_u128(53_003),
    );
    forged.quarantine_add = BTreeMap::from([(
        RangeId::from_uuid(Uuid::from_u128(52_000)),
        vec!["x".to_owned()],
    )]);
    assert!(matches!(
        coordinator.apply(forged).await,
        Err(SubscriptionProgressError::Conflict)
    ));

    // (d) a member Feed outside the committed map cannot acknowledge.
    let forged = base(
        sid,
        domain_id,
        Some(Uuid::from_u128(51_999)),
        seed.member_progress.clone(),
        "ghost",
        "domain",
        Uuid::from_u128(53_004),
    );
    assert!(matches!(
        coordinator.apply(forged).await,
        Err(SubscriptionProgressError::Conflict)
    ));

    // (e) the CAS rides the named entry's Cursor: expecting beta's Cursor
    // while advancing alpha conflicts.
    let forged = base(
        sid,
        domain_id,
        Some(alpha),
        BTreeMap::from([
            (alpha, entry("alpha_1")),
            (beta, seed.member_progress[&beta].clone()),
        ]),
        "alpha_1",
        "beta_0",
        Uuid::from_u128(53_005),
    );
    assert!(matches!(
        coordinator.apply(forged).await,
        Err(SubscriptionProgressError::Conflict)
    ));

    // And the honest sequence-2 alpha delta commits on every replica.
    let honest = base(
        sid,
        domain_id,
        Some(alpha),
        BTreeMap::from([
            (alpha, entry("alpha_1")),
            (beta, seed.member_progress[&beta].clone()),
        ]),
        "alpha_1",
        "alpha_0",
        Uuid::from_u128(53_006),
    );
    coordinator.apply(honest.clone()).await.unwrap();
    // Replaying the identical mutation is an idempotent no-op.
    coordinator.apply(honest.clone()).await.unwrap();
    // The same request identity carrying different content conflicts.
    let mut divergent = honest.clone();
    divergent.cursor = "alpha_2".to_owned();
    assert!(matches!(
        coordinator.apply(divergent).await,
        Err(SubscriptionProgressError::Conflict)
    ));
}

#[test]
fn member_view_scopes_delta_evidence_to_the_named_feed() {
    use whitewater::reader::MemberFeedProgress;
    let alpha = Uuid::from_u128(61_001);
    let beta = Uuid::from_u128(61_002);
    let range = RangeId::from_uuid(Uuid::from_u128(61_003));
    let row = SubscriptionProgressMutation {
        subscription_id: Uuid::from_u128(61_000),
        feed_id: Uuid::from_u128(61_004),
        ownership_epoch: 1,
        sequence: 2,
        request_id: Uuid::from_u128(61_005),
        member_progress: BTreeMap::from([
            (
                alpha,
                MemberFeedProgress {
                    cursor: "a1".to_owned(),
                    positions: BTreeMap::from([(range, "ap".to_owned())]),
                    quarantined: BTreeMap::from([(range, vec!["aq".to_owned()])]),
                    skipped: BTreeMap::new(),
                },
            ),
            (
                beta,
                MemberFeedProgress {
                    cursor: "b1".to_owned(),
                    positions: BTreeMap::from([(range, "bp".to_owned())]),
                    quarantined: BTreeMap::new(),
                    skipped: BTreeMap::from([(range, vec!["bs".to_owned()])]),
                },
            ),
        ]),
        member_feed: Some(alpha),
        expected_cursor: Some("a0".to_owned()),
        cursor: "a1".to_owned(),
        positions: BTreeMap::from([(range, "ap".to_owned())]),
        quarantined: BTreeMap::from([(range, vec!["aq".to_owned()])]),
        quarantine_add: BTreeMap::from([(range, vec!["aq".to_owned()])]),
        quarantine_remove: BTreeMap::new(),
        skipped: BTreeMap::new(),
        skip_add: BTreeMap::new(),
        tick: 5,
        lease_ops: Vec::new(),
    };
    // alpha's view carries the row's delta (the last mutation named it);
    // beta's view carries its own entry and none of alpha's delta.
    let alpha_view = row.member_view(alpha).unwrap();
    assert_eq!(alpha_view.feed_id, alpha);
    assert_eq!(alpha_view.cursor, "a1");
    assert_eq!(alpha_view.quarantine_add[&range], vec!["aq".to_owned()]);
    let beta_view = row.member_view(beta).unwrap();
    assert_eq!(beta_view.cursor, "b1");
    assert_eq!(beta_view.skipped[&range], vec!["bs".to_owned()]);
    assert!(beta_view.quarantine_add.is_empty());
    assert!(beta_view.quarantined.is_empty());
    assert!(row.member_view(Uuid::from_u128(61_999)).is_none());
    // expected_frontier_cursor: aggregate for exact/lease mutations,
    // the entry's Cursor for member-Feed deltas, None when unattached.
    assert_eq!(row.expected_frontier_cursor(None), Some("a1"));
    assert_eq!(row.expected_frontier_cursor(Some(alpha)), Some("a1"));
    assert_eq!(row.expected_frontier_cursor(Some(beta)), Some("b1"));
    assert_eq!(
        row.expected_frontier_cursor(Some(Uuid::from_u128(61_999))),
        None
    );
}
