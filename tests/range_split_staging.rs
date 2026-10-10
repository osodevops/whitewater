use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use finnstream::{
    active_range::{
        abort_frozen_split, freeze_and_stage_final_boundary, orchestrate_split_cutover,
        stage_candidate_ranges, stage_merged_range, stage_right_range, AppendIdentity,
        CommitPosition, FrozenSplitBoundary, KeyToken, RangePosition, ReplicaAppendErrorCode,
        ReplicaAppendRequest, ReplicaAppendService, ReplicaCommitRequest, SplitCutoverControl,
        StorageNodeId,
    },
    codec::encode_record,
    control::{
        Command, ControlController, RangeMergePlan, RangeSplitPlan, ReaderFrontierTranslation,
    },
    domain::StoredRecord,
    storage::{FileLogStore, LogStore},
};
use tempfile::TempDir;
use uuid::Uuid;

fn node(value: &str) -> StorageNodeId {
    StorageNodeId::try_new(value).unwrap()
}

struct LocalSplitControl {
    control: Arc<ControlController>,
    reader_translations: Vec<ReaderFrontierTranslation>,
}

#[async_trait]
impl SplitCutoverControl for LocalSplitControl {
    async fn mark_ready(
        &self,
        feed: &str,
        plan: &RangeSplitPlan,
        boundary: &FrozenSplitBoundary,
    ) -> Result<(), String> {
        self.control
            .execute_commands(vec![Command::RecordActiveRangeSplitCatchUp {
                feed: feed.to_owned(),
                plan_id: plan.plan_id,
                source_commit: boundary.final_commit,
                source_scanned_through: boundary.staging.source_commit,
                right_commit: boundary.staging.right.target_commit,
                checksum_verified: true,
            }])
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    async fn activate(
        &self,
        feed: &str,
        plan: &RangeSplitPlan,
        boundary: &FrozenSplitBoundary,
    ) -> Result<(), String> {
        if !self.reader_translations.is_empty()
            && self
                .control
                .execute_commands(vec![Command::ActivateActiveRangeSplit {
                    feed: feed.to_owned(),
                    plan_id: plan.plan_id,
                    left_writer_sequences: boundary.staging.left.writer_sequences.clone(),
                    right_writer_sequences: boundary.staging.right.writer_sequences.clone(),
                    reader_translations: Vec::new(),
                }])
                .await
                .is_ok()
        {
            return Err("cutover ignored the Reader translation requirement".to_owned());
        }
        if let Some(first) = self.reader_translations.first() {
            let mut stale = first.clone();
            stale.expected_delivered_cursor = Some("changed-during-staging".to_owned());
            if self
                .control
                .execute_commands(vec![Command::ActivateActiveRangeSplit {
                    feed: feed.to_owned(),
                    plan_id: plan.plan_id,
                    left_writer_sequences: boundary.staging.left.writer_sequences.clone(),
                    right_writer_sequences: boundary.staging.right.writer_sequences.clone(),
                    reader_translations: vec![stale],
                }])
                .await
                .is_ok()
            {
                return Err("cutover accepted stale Reader frontier evidence".to_owned());
            }
        }
        self.control
            .execute_commands(vec![Command::ActivateActiveRangeSplit {
                feed: feed.to_owned(),
                plan_id: plan.plan_id,
                left_writer_sequences: boundary.staging.left.writer_sequences.clone(),
                right_writer_sequences: boundary.staging.right.writer_sequences.clone(),
                reader_translations: self.reader_translations.clone(),
            }])
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

#[tokio::test]
async fn committed_right_hand_records_stage_identically_on_all_replicas() {
    let directory = TempDir::new().unwrap();
    let legacy: Arc<dyn LogStore> =
        Arc::new(FileLogStore::open(directory.path().join("legacy")).unwrap());
    let control = Arc::new(
        ControlController::open_with_storage_nodes(
            directory.path().join("catalog.json"),
            legacy,
            vec![node("storage-1"), node("storage-2"), node("storage-3")],
        )
        .unwrap(),
    );
    let created = control
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
    let feed_id = serde_json::from_value(created.results[1].data["feed_id"].clone()).unwrap();
    let source_assignment = control.active_range_assignment(feed_id).await.unwrap();
    let services = ["storage-1", "storage-2", "storage-3"]
        .into_iter()
        .map(|value| {
            let id = node(value);
            let service = Arc::new(ReplicaAppendService::new(
                directory.path().join(value),
                id.clone(),
                control.clone(),
            ));
            (id, service)
        })
        .collect::<BTreeMap<_, _>>();
    let source = services[&source_assignment.owner].clone();
    let split_at = KeyToken::from_bytes([0x80; 16]);
    let mut left_key = Vec::new();
    let mut right_key = Vec::new();
    for value in 0..10_000_u64 {
        let key = format!("key-{value}").into_bytes();
        if KeyToken::from_key(&key) < split_at && left_key.is_empty() {
            left_key = key;
        } else if KeyToken::from_key(&key) >= split_at && right_key.is_empty() {
            right_key = key;
        }
        if !left_key.is_empty() && !right_key.is_empty() {
            break;
        }
    }
    let post_freeze_key = right_key.clone();
    let writer = Uuid::from_u128(900);
    for (index, key) in [left_key.clone(), right_key.clone(), left_key, right_key]
        .into_iter()
        .enumerate()
    {
        let sequence = index as u64 + 1;
        let record = StoredRecord {
            message_id: Uuid::from_u128(1_000 + sequence as u128),
            producer_id: writer,
            producer_sequence: sequence,
            event_time_ns: sequence as i64,
            ingest_time_ns: sequence as i64,
            key,
            payload: format!("value-{sequence}").into_bytes(),
            metadata: BTreeMap::new(),
        };
        let accepted = source
            .append(ReplicaAppendRequest {
                feed_id,
                range_id: source_assignment.range_id,
                generation: source_assignment.generation,
                ownership_epoch: source_assignment.ownership_epoch,
                append_owner: source_assignment.owner.clone(),
                expected_position: RangePosition::new(sequence),
                identity: AppendIdentity {
                    writer_session_id: writer,
                    writer_epoch: 1,
                    sequence,
                },
                cursor: format!("cursor-{sequence}"),
                frame_base64: STANDARD.encode(encode_record(&record).unwrap()),
            })
            .await
            .unwrap();
        source
            .commit(ReplicaCommitRequest {
                feed_id,
                range_id: source_assignment.range_id,
                generation: source_assignment.generation,
                ownership_epoch: source_assignment.ownership_epoch,
                append_owner: source_assignment.owner.clone(),
                commit_position: CommitPosition::new(sequence),
                frame_digest: accepted.frame_digest,
            })
            .await
            .unwrap();
    }
    control
        .execute("CREATE READER audit FROM orders.events START AT BEGINNING;")
        .await
        .unwrap();
    let reader = control.active_reader_by_name("audit").await.unwrap();
    control
        .execute_commands(vec![Command::OpenReaderSession {
            reader: "audit".to_owned(),
            capacity: 4,
        }])
        .await
        .unwrap();
    let acknowledged = BTreeMap::from([(source_assignment.range_id, "cursor-2".to_owned())]);
    let delivered = BTreeMap::from([(source_assignment.range_id, "cursor-4".to_owned())]);
    control
        .execute_commands(vec![Command::RecordReaderFrontier {
            reader: "audit".to_owned(),
            session_epoch: 1,
            cursor: "ack-token".to_owned(),
            positions: acknowledged.clone(),
            expected_cursor: None,
            fence_delivery: true,
            fetch_request_id: Some(Uuid::from_u128(8001)),
        }])
        .await
        .unwrap();
    control
        .execute_commands(vec![Command::AcknowledgeReader {
            reader: "audit".to_owned(),
            session_epoch: 1,
            cursor: "ack-token".to_owned(),
        }])
        .await
        .unwrap();
    control
        .execute_commands(vec![Command::RecordReaderFrontier {
            reader: "audit".to_owned(),
            session_epoch: 1,
            cursor: "unack-token".to_owned(),
            positions: delivered.clone(),
            expected_cursor: Some("ack-token".to_owned()),
            fence_delivery: true,
            fetch_request_id: Some(Uuid::from_u128(8002)),
        }])
        .await
        .unwrap();
    let prepared = control
        .execute_commands(vec![Command::PrepareActiveRangeSplit {
            feed: "orders.events".to_owned(),
            split_at,
        }])
        .await
        .unwrap();
    let plan: RangeSplitPlan = serde_json::from_value(prepared.results[0].data.clone()).unwrap();
    let staged = stage_right_range(&plan, source, &services, 1)
        .await
        .unwrap();
    assert_eq!(staged.source_scanned_through, CommitPosition::new(4));
    assert_eq!(staged.target_commit, CommitPosition::new(2));
    assert_eq!(staged.transferred_records, 2);
    assert_eq!(staged.replicas_verified.len(), 3);
    let candidate = stage_candidate_ranges(
        &plan,
        services[&source_assignment.owner].clone(),
        &services,
        2,
    )
    .await
    .unwrap();
    assert_eq!(candidate.source_commit, CommitPosition::new(4));
    assert_eq!(candidate.left.target_commit, CommitPosition::new(2));
    assert_eq!(candidate.right.target_commit, CommitPosition::new(2));
    assert_eq!(
        candidate.left.source_scanned_through,
        candidate.right.source_scanned_through
    );
    let source_service = services[&source_assignment.owner].clone();
    let frozen = freeze_and_stage_final_boundary(
        &plan,
        &source_assignment,
        source_service.clone(),
        &services,
        2,
    )
    .await
    .unwrap();
    assert_eq!(frozen.final_commit, CommitPosition::new(4));
    assert!(
        source_service
            .generation_is_frozen(source_assignment.range_id, source_assignment.generation)
            .await
    );
    let fifth = StoredRecord {
        message_id: Uuid::from_u128(1_005),
        producer_id: writer,
        producer_sequence: 5,
        event_time_ns: 5,
        ingest_time_ns: 5,
        key: post_freeze_key,
        payload: b"value-5".to_vec(),
        metadata: BTreeMap::new(),
    };
    let frozen_error = source_service
        .append(ReplicaAppendRequest {
            feed_id,
            range_id: source_assignment.range_id,
            generation: source_assignment.generation,
            ownership_epoch: source_assignment.ownership_epoch,
            append_owner: source_assignment.owner.clone(),
            expected_position: RangePosition::new(5),
            identity: AppendIdentity {
                writer_session_id: writer,
                writer_epoch: 1,
                sequence: 5,
            },
            cursor: "cursor-5".to_owned(),
            frame_base64: STANDARD.encode(encode_record(&fifth).unwrap()),
        })
        .await
        .unwrap_err();
    assert_eq!(frozen_error.code, ReplicaAppendErrorCode::RangeFrozen);
    abort_frozen_split(&source_service, &frozen).await;
    assert!(
        !source_service
            .generation_is_frozen(source_assignment.range_id, source_assignment.generation)
            .await
    );
    for replica in plan.right_assignment.replicas.iter() {
        let records = services[replica]
            .read_staged_committed(&plan.right_assignment, None, 10)
            .await
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].identity.sequence, 2);
        assert_eq!(records[1].identity.sequence, 4);
        assert_eq!(
            records[0].frame,
            services[&plan.right_assignment.owner]
                .read_staged_committed(&plan.right_assignment, None, 10)
                .await
                .unwrap()[0]
                .frame
        );
    }
    let left_assignment = plan.left_assignment.as_ref().unwrap();
    for replica in left_assignment.replicas.iter() {
        let records = services[replica]
            .read_staged_committed(left_assignment, None, 10)
            .await
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].identity.sequence, 1);
        assert_eq!(records[1].identity.sequence, 3);
    }
    assert_eq!(
        control
            .active_range_map(feed_id)
            .await
            .unwrap()
            .routes()
            .len(),
        1
    );
    let cutover_control = LocalSplitControl {
        control: control.clone(),
        reader_translations: vec![ReaderFrontierTranslation {
            reader_id: reader.reader_id,
            expected_session_epoch: 1,
            expected_acknowledged_cursor: Some("ack-token".to_owned()),
            expected_delivered_cursor: Some("unack-token".to_owned()),
            expected_acknowledged: acknowledged,
            expected_delivered: delivered,
            expected_has_frontier: true,
            acknowledged: BTreeMap::from([
                (source_assignment.range_id, "cursor-1".to_owned()),
                (plan.right_assignment.range_id, "cursor-2".to_owned()),
            ]),
        }],
    };
    let final_boundary = orchestrate_split_cutover(
        &cutover_control,
        "orders.events",
        &plan,
        &source_assignment,
        source_service.clone(),
        &services,
        2,
    )
    .await
    .unwrap();
    assert_eq!(final_boundary.final_commit, CommitPosition::new(4));
    let after_split = control.active_reader_by_name("audit").await.unwrap();
    assert_eq!(after_split.session_epoch, 2);
    assert!(!after_split.session_active);
    assert!(control
        .execute_commands(vec![Command::AcknowledgeReader {
            reader: "audit".to_owned(),
            session_epoch: 1,
            cursor: "unack-token".to_owned(),
        }])
        .await
        .is_err());
    let after_split_state: serde_json::Value =
        serde_json::from_slice(&control.snapshot_bytes().await.unwrap()).unwrap();
    let split_progress =
        &after_split_state["reader_frontiers"][reader.reader_id.to_string()]["acknowledged"];
    assert_eq!(
        split_progress[source_assignment.range_id.to_string()],
        "cursor-1"
    );
    assert_eq!(
        split_progress[plan.right_assignment.range_id.to_string()],
        "cursor-2"
    );
    assert_eq!(
        after_split_state["reader_frontiers"][reader.reader_id.to_string()]["delivered"],
        *split_progress
    );
    assert_eq!(
        control
            .active_range_map(feed_id)
            .await
            .unwrap()
            .routes()
            .len(),
        2
    );
    let merged = source_service
        .read_committed(feed_id, None, 10)
        .await
        .unwrap();
    assert_eq!(merged.len(), 4);
    assert_eq!(
        merged
            .iter()
            .map(|item| item.identity.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    let assignments = control.active_range_assignments_for_feed(feed_id).await;
    let merge_prepared = control
        .execute_commands(vec![Command::PrepareActiveRangeMerge {
            feed: "orders.events".to_owned(),
            left_range_id: assignments[0].range_id,
            right_range_id: assignments[1].range_id,
        }])
        .await
        .unwrap();
    let merge_plan: RangeMergePlan =
        serde_json::from_value(merge_prepared.results[0].data.clone()).unwrap();
    let merge_staged = stage_merged_range(
        &merge_plan,
        &assignments[0],
        &assignments[1],
        services[&assignments[0].owner].clone(),
        services[&assignments[1].owner].clone(),
        &services,
    )
    .await
    .unwrap();
    assert_eq!(merge_staged.transferred_records, 4);
    control
        .execute_commands(vec![Command::RecordActiveRangeMergeStaging {
            feed: "orders.events".to_owned(),
            plan_id: merge_plan.plan_id,
            left_commit: merge_staged.left_commit,
            right_commit: merge_staged.right_commit,
            merged_commit: merge_staged.merged_commit,
            checksum_verified: true,
        }])
        .await
        .unwrap();
    control
        .execute_commands(vec![Command::ActivateActiveRangeMerge {
            feed: "orders.events".to_owned(),
            plan_id: merge_plan.plan_id,
            writer_sequences: merge_staged.writer_sequences.clone(),
            reader_translations: vec![ReaderFrontierTranslation {
                reader_id: reader.reader_id,
                expected_session_epoch: 2,
                expected_acknowledged_cursor: Some("ack-token".to_owned()),
                expected_delivered_cursor: Some("ack-token".to_owned()),
                expected_acknowledged: BTreeMap::from([
                    (source_assignment.range_id, "cursor-1".to_owned()),
                    (plan.right_assignment.range_id, "cursor-2".to_owned()),
                ]),
                expected_delivered: BTreeMap::from([
                    (source_assignment.range_id, "cursor-1".to_owned()),
                    (plan.right_assignment.range_id, "cursor-2".to_owned()),
                ]),
                expected_has_frontier: true,
                acknowledged: BTreeMap::from([(merge_plan.left_range_id, "cursor-2".to_owned())]),
            }],
        }])
        .await
        .unwrap();
    assert_eq!(
        control
            .active_range_map(feed_id)
            .await
            .unwrap()
            .routes()
            .len(),
        1
    );
    let merged_assignment = control.active_range_assignment(feed_id).await.unwrap();
    let restored_store: Arc<dyn LogStore> =
        Arc::new(FileLogStore::open(directory.path().join("reopened-legacy")).unwrap());
    let restored = ControlController::open_with_storage_nodes(
        directory.path().join("restored-catalog.json"),
        restored_store,
        vec![node("storage-1"), node("storage-2"), node("storage-3")],
    )
    .unwrap();
    restored
        .install_snapshot_bytes(&control.snapshot_bytes().await.unwrap())
        .await
        .unwrap();
    let restored_reader = restored.active_reader_by_name("audit").await.unwrap();
    assert_eq!(restored_reader.session_epoch, 3);
    let restored_state: serde_json::Value =
        serde_json::from_slice(&restored.snapshot_bytes().await.unwrap()).unwrap();
    let merged_progress =
        &restored_state["reader_frontiers"][reader.reader_id.to_string()]["acknowledged"];
    assert_eq!(
        merged_progress[merged_assignment.range_id.to_string()],
        "cursor-2"
    );
    assert_eq!(merged_progress.as_object().unwrap().len(), 1);
    let reopened = restored
        .execute_commands(vec![Command::OpenReaderSession {
            reader: "audit".to_owned(),
            capacity: 4,
        }])
        .await
        .unwrap();
    assert_eq!(reopened.results[0].data["session_epoch"], 4);
    restored
        .execute_commands(vec![Command::RecordReaderFrontier {
            reader: "audit".to_owned(),
            session_epoch: 4,
            cursor: "after-merge".to_owned(),
            positions: BTreeMap::from([(merged_assignment.range_id, "cursor-4".to_owned())]),
            expected_cursor: Some("ack-token".to_owned()),
            fence_delivery: true,
            fetch_request_id: Some(Uuid::from_u128(8003)),
        }])
        .await
        .unwrap();
    let final_records = services[&merged_assignment.owner]
        .read_committed(feed_id, None, 10)
        .await
        .unwrap();
    assert_eq!(final_records.len(), 4);
}

#[tokio::test]
async fn merge_of_branched_writer_history_deduplicates_retries_by_frame_digest() {
    let directory = TempDir::new().unwrap();
    let legacy: Arc<dyn LogStore> =
        Arc::new(FileLogStore::open(directory.path().join("legacy")).unwrap());
    let control = Arc::new(
        ControlController::open_with_storage_nodes(
            directory.path().join("catalog.json"),
            legacy,
            vec![node("storage-1"), node("storage-2"), node("storage-3")],
        )
        .unwrap(),
    );
    let created = control
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
    let feed_id = serde_json::from_value(created.results[1].data["feed_id"].clone()).unwrap();
    let source_assignment = control.active_range_assignment(feed_id).await.unwrap();
    let services = ["storage-1", "storage-2", "storage-3"]
        .into_iter()
        .map(|value| {
            let id = node(value);
            let service = Arc::new(ReplicaAppendService::new(
                directory.path().join(value),
                id.clone(),
                control.clone(),
            ));
            (id, service)
        })
        .collect::<BTreeMap<_, _>>();
    let source = services[&source_assignment.owner].clone();
    let split_at = KeyToken::from_bytes([0x80; 16]);
    let mut left_key = Vec::new();
    let mut right_key = Vec::new();
    for value in 0..10_000_u64 {
        let key = format!("key-{value}").into_bytes();
        if KeyToken::from_key(&key) < split_at && left_key.is_empty() {
            left_key = key;
        } else if KeyToken::from_key(&key) >= split_at && right_key.is_empty() {
            right_key = key;
        }
        if !left_key.is_empty() && !right_key.is_empty() {
            break;
        }
    }
    let writer = Uuid::from_u128(901);
    let build_frame = |sequence: u64, key: &[u8], payload: &str, message_id: u128| {
        let record = StoredRecord {
            message_id: Uuid::from_u128(message_id),
            producer_id: writer,
            producer_sequence: sequence,
            event_time_ns: sequence as i64,
            ingest_time_ns: sequence as i64,
            key: key.to_vec(),
            payload: payload.as_bytes().to_vec(),
            metadata: BTreeMap::new(),
        };
        STANDARD.encode(encode_record(&record).unwrap())
    };
    for (index, key) in [
        left_key.clone(),
        right_key.clone(),
        left_key.clone(),
        right_key.clone(),
    ]
    .into_iter()
    .enumerate()
    {
        let sequence = index as u64 + 1;
        let accepted = source
            .append(ReplicaAppendRequest {
                feed_id,
                range_id: source_assignment.range_id,
                generation: source_assignment.generation,
                ownership_epoch: source_assignment.ownership_epoch,
                append_owner: source_assignment.owner.clone(),
                expected_position: RangePosition::new(sequence),
                identity: AppendIdentity {
                    writer_session_id: writer,
                    writer_epoch: 1,
                    sequence,
                },
                cursor: format!("cursor-{sequence}"),
                frame_base64: build_frame(
                    sequence,
                    &key,
                    &format!("value-{sequence}"),
                    1_000 + sequence as u128,
                ),
            })
            .await
            .unwrap();
        source
            .commit(ReplicaCommitRequest {
                feed_id,
                range_id: source_assignment.range_id,
                generation: source_assignment.generation,
                ownership_epoch: source_assignment.ownership_epoch,
                append_owner: source_assignment.owner.clone(),
                commit_position: CommitPosition::new(sequence),
                frame_digest: accepted.frame_digest,
            })
            .await
            .unwrap();
    }
    let prepared = control
        .execute_commands(vec![Command::PrepareActiveRangeSplit {
            feed: "orders.events".to_owned(),
            split_at,
        }])
        .await
        .unwrap();
    let plan: RangeSplitPlan = serde_json::from_value(prepared.results[0].data.clone()).unwrap();
    let source_service = services[&source_assignment.owner].clone();
    let cutover_control = LocalSplitControl {
        control: control.clone(),
        reader_translations: Vec::new(),
    };
    orchestrate_split_cutover(
        &cutover_control,
        "orders.events",
        &plan,
        &source_assignment,
        source_service.clone(),
        &services,
        2,
    )
    .await
    .unwrap();
    let assignments = control.active_range_assignments_for_feed(feed_id).await;
    assert_eq!(assignments.len(), 2);
    // The same writer continues on both children, which allocate sequences
    // independently: the left child issues sequence 4 while the right issues
    // sequence 5, so sequence 4 exists with different records in both ranges -
    // the overlap that previously refused this merge.
    let mut branched = Vec::new();
    for (index, assignment) in assignments.iter().enumerate() {
        let sequence = if index == 0 { 4 } else { 5 };
        let frame = build_frame(
            sequence,
            if index == 0 { &left_key } else { &right_key },
            &format!("branched-{index}"),
            2_000 + index as u128,
        );
        let owner_service = services[&assignment.owner].clone();
        let accepted = owner_service
            .append(ReplicaAppendRequest {
                feed_id,
                range_id: assignment.range_id,
                generation: assignment.generation,
                ownership_epoch: assignment.ownership_epoch,
                append_owner: assignment.owner.clone(),
                expected_position: RangePosition::new(3),
                identity: AppendIdentity {
                    writer_session_id: writer,
                    writer_epoch: 1,
                    sequence,
                },
                cursor: format!("branched-cursor-{index}"),
                frame_base64: frame.clone(),
            })
            .await
            .unwrap();
        owner_service
            .commit(ReplicaCommitRequest {
                feed_id,
                range_id: assignment.range_id,
                generation: assignment.generation,
                ownership_epoch: assignment.ownership_epoch,
                append_owner: assignment.owner.clone(),
                commit_position: CommitPosition::new(3),
                frame_digest: accepted.frame_digest,
            })
            .await
            .unwrap();
        branched.push((accepted, frame));
    }
    let merge_prepared = control
        .execute_commands(vec![Command::PrepareActiveRangeMerge {
            feed: "orders.events".to_owned(),
            left_range_id: assignments[0].range_id,
            right_range_id: assignments[1].range_id,
        }])
        .await
        .unwrap();
    let merge_plan: RangeMergePlan =
        serde_json::from_value(merge_prepared.results[0].data.clone()).unwrap();
    let merge_staged = stage_merged_range(
        &merge_plan,
        &assignments[0],
        &assignments[1],
        services[&assignments[0].owner].clone(),
        services[&assignments[1].owner].clone(),
        &services,
    )
    .await
    .unwrap();
    assert_eq!(merge_staged.transferred_records, 6);
    control
        .execute_commands(vec![Command::RecordActiveRangeMergeStaging {
            feed: "orders.events".to_owned(),
            plan_id: merge_plan.plan_id,
            left_commit: merge_staged.left_commit,
            right_commit: merge_staged.right_commit,
            merged_commit: merge_staged.merged_commit,
            checksum_verified: true,
        }])
        .await
        .unwrap();
    control
        .execute_commands(vec![Command::ActivateActiveRangeMerge {
            feed: "orders.events".to_owned(),
            plan_id: merge_plan.plan_id,
            writer_sequences: merge_staged.writer_sequences.clone(),
            reader_translations: Vec::new(),
        }])
        .await
        .unwrap();
    let merged_assignment = control.active_range_assignment(feed_id).await.unwrap();
    let merged = services[&merged_assignment.owner]
        .read_committed(feed_id, None, 10)
        .await
        .unwrap();
    assert_eq!(merged.len(), 6);
    assert_eq!(
        merged
            .iter()
            .map(|frame| frame.cursor.clone())
            .collect::<Vec<_>>(),
        vec![
            "cursor-1",
            "cursor-2",
            "cursor-3",
            "cursor-4",
            "branched-cursor-0",
            "branched-cursor-1"
        ]
    );
    // A retried branch append resolves by frame digest to its original result.
    for (index, (accepted, frame)) in branched.iter().enumerate() {
        let replay = services[&merged_assignment.owner]
            .append(ReplicaAppendRequest {
                feed_id,
                range_id: merged_assignment.range_id,
                generation: merged_assignment.generation,
                ownership_epoch: merged_assignment.ownership_epoch,
                append_owner: merged_assignment.owner.clone(),
                expected_position: RangePosition::new(7),
                identity: AppendIdentity {
                    writer_session_id: writer,
                    writer_epoch: 1,
                    sequence: if index == 0 { 4 } else { 5 },
                },
                cursor: format!("branched-cursor-{index}"),
                frame_base64: frame.clone(),
            })
            .await
            .unwrap();
        assert!(replay.deduplicated);
        assert_eq!(replay.frame_digest, accepted.frame_digest);
        assert_eq!(replay.cursor, accepted.cursor);
        assert_eq!(replay.message_id, accepted.message_id);
    }
    // A conflicting frame at the writer's latest merged sequence is refused,
    // and a frame below it is reported stale rather than silently appended.
    let conflict = services[&merged_assignment.owner]
        .append(ReplicaAppendRequest {
            feed_id,
            range_id: merged_assignment.range_id,
            generation: merged_assignment.generation,
            ownership_epoch: merged_assignment.ownership_epoch,
            append_owner: merged_assignment.owner.clone(),
            expected_position: RangePosition::new(7),
            identity: AppendIdentity {
                writer_session_id: writer,
                writer_epoch: 1,
                sequence: 5,
            },
            cursor: "conflicting-cursor".to_owned(),
            frame_base64: build_frame(5, &left_key, "tampered", 9_999),
        })
        .await
        .unwrap_err();
    assert_eq!(
        conflict.code,
        ReplicaAppendErrorCode::WriterSequenceConflict
    );
    let stale = services[&merged_assignment.owner]
        .append(ReplicaAppendRequest {
            feed_id,
            range_id: merged_assignment.range_id,
            generation: merged_assignment.generation,
            ownership_epoch: merged_assignment.ownership_epoch,
            append_owner: merged_assignment.owner.clone(),
            expected_position: RangePosition::new(7),
            identity: AppendIdentity {
                writer_session_id: writer,
                writer_epoch: 1,
                sequence: 4,
            },
            cursor: "stale-cursor".to_owned(),
            frame_base64: build_frame(4, &left_key, "tampered", 9_998),
        })
        .await
        .unwrap_err();
    assert_eq!(stale.code, ReplicaAppendErrorCode::WriterSequenceConflict);
    assert!(stale.message.contains("stale"));
    // New appends continue above the merged writer maximum.
    let sixth = services[&merged_assignment.owner]
        .append(ReplicaAppendRequest {
            feed_id,
            range_id: merged_assignment.range_id,
            generation: merged_assignment.generation,
            ownership_epoch: merged_assignment.ownership_epoch,
            append_owner: merged_assignment.owner.clone(),
            expected_position: RangePosition::new(7),
            identity: AppendIdentity {
                writer_session_id: writer,
                writer_epoch: 1,
                sequence: 6,
            },
            cursor: "cursor-6".to_owned(),
            frame_base64: build_frame(6, &left_key, "value-6", 1_006),
        })
        .await
        .unwrap();
    assert_eq!(sixth.position, RangePosition::new(7));
    assert!(!sixth.deduplicated);
    // Restart preserves the digest index for every branch.
    drop(source);
    drop(source_service);
    drop(services);
    let reopened_services = ["storage-1", "storage-2", "storage-3"]
        .into_iter()
        .map(|value| {
            let id = node(value);
            let service = Arc::new(ReplicaAppendService::new(
                directory.path().join(value),
                id.clone(),
                control.clone(),
            ));
            (id, service)
        })
        .collect::<BTreeMap<_, _>>();
    let (accepted, frame) = &branched[1];
    let replay = reopened_services[&merged_assignment.owner]
        .append(ReplicaAppendRequest {
            feed_id,
            range_id: merged_assignment.range_id,
            generation: merged_assignment.generation,
            ownership_epoch: merged_assignment.ownership_epoch,
            append_owner: merged_assignment.owner.clone(),
            expected_position: RangePosition::new(8),
            identity: AppendIdentity {
                writer_session_id: writer,
                writer_epoch: 1,
                sequence: 5,
            },
            cursor: "branched-cursor-1".to_owned(),
            frame_base64: frame.clone(),
        })
        .await
        .unwrap();
    assert!(replay.deduplicated);
    assert_eq!(replay.cursor, accepted.cursor);
}
