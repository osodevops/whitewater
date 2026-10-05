use std::{collections::BTreeMap, fs, sync::Arc};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use finnstream::{
    active_range::{
        copy_follower_move, repair_replica, AppendIdentity, CommitPosition, FollowerMoveControl,
        FollowerMoveCopyResult, FollowerMoveExecutor, MajorityAppendCoordinator, OwnershipEpoch,
        RangeGeneration, RangePosition, ReplicaAppendAccepted, ReplicaAppendRequest,
        ReplicaAppendService, ReplicaCommitAccepted, ReplicaCommitRequest, ReplicaTransport,
        ReplicaTransportError, StorageNodeId,
    },
    codec::encode_record,
    control::{Command, ControlController, RangeMovePlan},
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
            message: "transport is not used during follower movement".to_owned(),
            retryable: false,
        })
    }

    async fn commit(
        &self,
        _replica: &StorageNodeId,
        _request: ReplicaCommitRequest,
    ) -> Result<ReplicaCommitAccepted, ReplicaTransportError> {
        Err(ReplicaTransportError {
            message: "transport is not used during follower movement".to_owned(),
            retryable: false,
        })
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

struct UncertainReadinessControl;

#[async_trait]
impl FollowerMoveControl for UncertainReadinessControl {
    async fn record_ready(
        &self,
        _feed: &str,
        _plan: &RangeMovePlan,
        _copied: &FollowerMoveCopyResult,
    ) -> Result<(), String> {
        Err("readiness response was lost".to_owned())
    }

    async fn activate(&self, _feed: &str, _plan: &RangeMovePlan) -> Result<(), String> {
        Err("activation must not run without readiness".to_owned())
    }
}

struct AmbiguousMoveControl(Arc<ControlController>);

#[async_trait]
impl FollowerMoveControl for AmbiguousMoveControl {
    async fn record_ready(
        &self,
        feed: &str,
        plan: &RangeMovePlan,
        copied: &FollowerMoveCopyResult,
    ) -> Result<(), String> {
        LocalMoveControl(self.0.clone())
            .record_ready(feed, plan, copied)
            .await
    }

    async fn activate(&self, _feed: &str, _plan: &RangeMovePlan) -> Result<(), String> {
        Err("activation response was lost".to_owned())
    }
}

#[tokio::test]
async fn missing_committed_frames_are_verified_transferred_and_dedup_rebuilt() {
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
    let assignment = control.active_range_assignment(feed_id).await.unwrap();
    let source = Arc::new(ReplicaAppendService::new(
        directory.path().join("source"),
        node("storage-1"),
        control.clone(),
    ));
    let target = Arc::new(ReplicaAppendService::new(
        directory.path().join("target"),
        node("storage-2"),
        control,
    ));
    let writer = Uuid::from_u128(500);
    for sequence in 1..=3_u64 {
        let record = StoredRecord {
            message_id: Uuid::from_u128(600 + sequence as u128),
            producer_id: writer,
            producer_sequence: sequence,
            event_time_ns: sequence as i64,
            ingest_time_ns: sequence as i64,
            key: b"key".to_vec(),
            payload: format!("value-{sequence}").into_bytes(),
            metadata: BTreeMap::new(),
        };
        let request = ReplicaAppendRequest {
            feed_id,
            range_id: assignment.range_id,
            generation: RangeGeneration::new(1),
            ownership_epoch: OwnershipEpoch::new(1),
            append_owner: assignment.owner.clone(),
            expected_position: RangePosition::new(sequence),
            identity: AppendIdentity {
                writer_session_id: writer,
                writer_epoch: 1,
                sequence,
            },
            cursor: format!("cursor-{sequence}"),
            frame_base64: STANDARD.encode(encode_record(&record).unwrap()),
        };
        let accepted = source.append(request.clone()).await.unwrap();
        source
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
        if sequence == 1 {
            let accepted = target.append(request).await.unwrap();
            target
                .commit(ReplicaCommitRequest {
                    feed_id,
                    range_id: assignment.range_id,
                    generation: assignment.generation,
                    ownership_epoch: assignment.ownership_epoch,
                    append_owner: assignment.owner.clone(),
                    commit_position: CommitPosition::new(1),
                    frame_digest: accepted.frame_digest,
                })
                .await
                .unwrap();
        }
    }
    let progress = repair_replica(&assignment, source, target.clone(), 1)
        .await
        .unwrap();
    assert!(progress.ready);
    assert_eq!(progress.transferred_records, 2);
    assert_eq!(progress.target_committed, CommitPosition::new(3));
    assert_eq!(
        target
            .read_committed(feed_id, None, 10)
            .await
            .unwrap()
            .len(),
        3
    );
}

#[tokio::test]
async fn corrupt_replica_directory_is_quarantined_before_rebuild() {
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
    let assignment = control.active_range_assignment(feed_id).await.unwrap();
    let source = Arc::new(ReplicaAppendService::new(
        directory.path().join("source"),
        node("storage-1"),
        control.clone(),
    ));
    let target_root = directory.path().join("target");
    let target = Arc::new(ReplicaAppendService::new(
        &target_root,
        node("storage-2"),
        control.clone(),
    ));
    let writer = Uuid::from_u128(700);
    let record = StoredRecord {
        message_id: Uuid::from_u128(701),
        producer_id: writer,
        producer_sequence: 1,
        event_time_ns: 1,
        ingest_time_ns: 1,
        key: b"key".to_vec(),
        payload: b"value".to_vec(),
        metadata: BTreeMap::new(),
    };
    let request = ReplicaAppendRequest {
        feed_id,
        range_id: assignment.range_id,
        generation: assignment.generation,
        ownership_epoch: assignment.ownership_epoch,
        append_owner: assignment.owner.clone(),
        expected_position: RangePosition::new(1),
        identity: AppendIdentity {
            writer_session_id: writer,
            writer_epoch: 1,
            sequence: 1,
        },
        cursor: "cursor-1".to_owned(),
        frame_base64: STANDARD.encode(encode_record(&record).unwrap()),
    };
    for service in [&source, &target] {
        let accepted = service.append(request.clone()).await.unwrap();
        service
            .commit(ReplicaCommitRequest {
                feed_id,
                range_id: assignment.range_id,
                generation: assignment.generation,
                ownership_epoch: assignment.ownership_epoch,
                append_owner: assignment.owner.clone(),
                commit_position: CommitPosition::new(1),
                frame_digest: accepted.frame_digest,
            })
            .await
            .unwrap();
    }
    drop(target);
    let segment = target_root
        .join(feed_id.to_string())
        .join(assignment.range_id.to_string())
        .join("generation-1")
        .join("segment-000000.log");
    let mut bytes = fs::read(&segment).unwrap();
    let corrupt_index = bytes.len() - 5;
    bytes[corrupt_index] ^= 0xff;
    fs::write(&segment, bytes).unwrap();
    let target = Arc::new(ReplicaAppendService::new(
        &target_root,
        node("storage-2"),
        control,
    ));
    let progress = repair_replica(&assignment, source, target, 10)
        .await
        .unwrap();
    assert!(progress.ready);
    assert!(progress.quarantined_corruption);
    assert_eq!(progress.transferred_records, 1);
}

#[tokio::test]
async fn follower_move_copies_committed_frames_before_swapping_rf3() {
    let directory = TempDir::new().unwrap();
    let legacy: Arc<dyn LogStore> =
        Arc::new(FileLogStore::open(directory.path().join("legacy")).unwrap());
    let control = Arc::new(
        ControlController::open_with_storage_nodes(
            directory.path().join("catalog.json"),
            legacy,
            ["storage-1", "storage-2", "storage-3", "storage-4"]
                .into_iter()
                .map(node)
                .collect(),
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
    let original = control.active_range_assignment(feed_id).await.unwrap();
    let services = ["storage-1", "storage-2", "storage-3", "storage-4"]
        .into_iter()
        .map(|value| {
            (
                node(value),
                Arc::new(ReplicaAppendService::new(
                    directory.path().join(value),
                    node(value),
                    control.clone(),
                )),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let source = services[&original.owner].clone();
    for sequence in 1..=3_u64 {
        let record = StoredRecord {
            message_id: Uuid::from_u128(900 + sequence as u128),
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
            range_id: original.range_id,
            generation: original.generation,
            ownership_epoch: original.ownership_epoch,
            append_owner: original.owner.clone(),
            expected_position: RangePosition::new(sequence),
            identity: AppendIdentity {
                writer_session_id: record.producer_id,
                writer_epoch: 1,
                sequence,
            },
            cursor: format!("cursor-{sequence}"),
            frame_base64: STANDARD.encode(encode_record(&record).unwrap()),
        };
        for node in original.replicas.iter() {
            let accepted = services[node].append(request.clone()).await.unwrap();
            services[node]
                .commit(ReplicaCommitRequest {
                    feed_id,
                    range_id: original.range_id,
                    generation: original.generation,
                    ownership_epoch: original.ownership_epoch,
                    append_owner: original.owner.clone(),
                    commit_position: CommitPosition::new(sequence),
                    frame_digest: accepted.frame_digest,
                })
                .await
                .unwrap();
        }
    }
    let prepared = control
        .execute_commands(vec![Command::PrepareFollowerMove {
            feed: "orders.events".to_owned(),
            range_id: original.range_id,
            removed_replica: node("storage-3"),
            replacement_replica: node("storage-4"),
        }])
        .await
        .unwrap();
    let plan: RangeMovePlan = serde_json::from_value(prepared.results[0].data.clone()).unwrap();
    let target = services[&node("storage-4")].clone();
    let copied = copy_follower_move(&plan, &source, &target, 1)
        .await
        .unwrap();
    assert!(copied.ready);
    assert_eq!(copied.transferred_records, 3);
    assert_eq!(copied.target_commit, CommitPosition::new(3));
    assert_eq!(
        copy_follower_move(&plan, &source, &target, 2)
            .await
            .unwrap()
            .transferred_records,
        0
    );
    assert_eq!(
        control.active_range_assignment(feed_id).await.unwrap(),
        original
    );
    let delta = StoredRecord {
        message_id: Uuid::from_u128(904),
        producer_id: Uuid::from_u128(900),
        producer_sequence: 4,
        event_time_ns: 4,
        ingest_time_ns: 4,
        key: b"order-1".to_vec(),
        payload: b"event-4".to_vec(),
        metadata: BTreeMap::new(),
    };
    let delta_request = ReplicaAppendRequest {
        feed_id,
        range_id: original.range_id,
        generation: original.generation,
        ownership_epoch: original.ownership_epoch,
        append_owner: original.owner.clone(),
        expected_position: RangePosition::new(4),
        identity: AppendIdentity {
            writer_session_id: delta.producer_id,
            writer_epoch: 1,
            sequence: 4,
        },
        cursor: "cursor-4".to_owned(),
        frame_base64: STANDARD.encode(encode_record(&delta).unwrap()),
    };
    for node in original.replicas.iter() {
        let accepted = services[node].append(delta_request.clone()).await.unwrap();
        services[node]
            .commit(ReplicaCommitRequest {
                feed_id,
                range_id: original.range_id,
                generation: original.generation,
                ownership_epoch: original.ownership_epoch,
                append_owner: original.owner.clone(),
                commit_position: CommitPosition::new(4),
                frame_digest: accepted.frame_digest,
            })
            .await
            .unwrap();
    }
    let coordinator = Arc::new(MajorityAppendCoordinator::new(
        source.clone(),
        control.clone(),
        Arc::new(UnusedTransport),
    ));
    let failed_executor = FollowerMoveExecutor::new(
        control.clone(),
        coordinator.clone(),
        source.clone(),
        services[&node("storage-2")].clone(),
    );
    assert!(failed_executor
        .finalize(
            &LocalMoveControl(control.clone()),
            "orders.events",
            &plan,
            1
        )
        .await
        .is_err());
    assert!(
        !source
            .generation_is_frozen(original.range_id, original.generation)
            .await
    );
    assert_eq!(
        control.active_range_assignment(feed_id).await.unwrap(),
        original
    );
    let executor =
        FollowerMoveExecutor::new(control.clone(), coordinator, source.clone(), target.clone());
    assert!(executor
        .finalize(&UncertainReadinessControl, "orders.events", &plan, 1)
        .await
        .is_err());
    assert!(
        source
            .generation_is_frozen(original.range_id, original.generation)
            .await
    );
    assert_eq!(
        control.active_range_assignment(feed_id).await.unwrap(),
        original
    );
    source
        .unfreeze_generation(original.range_id, original.generation)
        .await;
    assert!(executor
        .finalize(
            &AmbiguousMoveControl(control.clone()),
            "orders.events",
            &plan,
            2
        )
        .await
        .is_err());
    assert!(
        source
            .generation_is_frozen(original.range_id, original.generation)
            .await
    );
    assert_eq!(
        control.active_range_assignment(feed_id).await.unwrap(),
        original
    );
    source
        .unfreeze_generation(original.range_id, original.generation)
        .await;
    let final_copy = executor
        .finalize(
            &LocalMoveControl(control.clone()),
            "orders.events",
            &plan,
            1,
        )
        .await
        .unwrap();
    assert!(final_copy.ready);
    assert_eq!(final_copy.transferred_records, 0);
    assert_eq!(final_copy.target_commit, CommitPosition::new(4));
    assert!(
        !source
            .generation_is_frozen(original.range_id, original.generation)
            .await
    );
    let current = control.active_range_assignment(feed_id).await.unwrap();
    assert!(!current.replicas.contains(&node("storage-3")));
    assert!(current.replicas.contains(&node("storage-4")));
    let next_record = StoredRecord {
        message_id: Uuid::from_u128(905),
        producer_id: Uuid::from_u128(900),
        producer_sequence: 5,
        event_time_ns: 5,
        ingest_time_ns: 5,
        key: b"order-1".to_vec(),
        payload: b"event-5".to_vec(),
        metadata: BTreeMap::new(),
    };
    let next_request = ReplicaAppendRequest {
        feed_id,
        range_id: current.range_id,
        generation: current.generation,
        ownership_epoch: current.ownership_epoch,
        append_owner: current.owner.clone(),
        expected_position: RangePosition::new(5),
        identity: AppendIdentity {
            writer_session_id: next_record.producer_id,
            writer_epoch: 1,
            sequence: 5,
        },
        cursor: "cursor-5".to_owned(),
        frame_base64: STANDARD.encode(encode_record(&next_record).unwrap()),
    };
    assert!(services[&node("storage-3")]
        .append(next_request.clone())
        .await
        .is_err());
    for node in current.replicas.iter() {
        let accepted = services[node].append(next_request.clone()).await.unwrap();
        services[node]
            .commit(ReplicaCommitRequest {
                feed_id,
                range_id: current.range_id,
                generation: current.generation,
                ownership_epoch: current.ownership_epoch,
                append_owner: current.owner.clone(),
                commit_position: CommitPosition::new(5),
                frame_digest: accepted.frame_digest,
            })
            .await
            .unwrap();
    }
    for sequence in 6..=11_u64 {
        let mut record = next_record.clone();
        record.message_id = Uuid::from_u128(900 + sequence as u128);
        record.producer_sequence = sequence;
        record.event_time_ns = sequence as i64;
        record.ingest_time_ns = sequence as i64;
        record.payload = format!("event-{sequence}").into_bytes();
        let mut request = next_request.clone();
        request.expected_position = RangePosition::new(sequence);
        request.identity.sequence = sequence;
        request.cursor = format!("cursor-{sequence}");
        request.frame_base64 = STANDARD.encode(encode_record(&record).unwrap());
        for node in current.replicas.iter() {
            let accepted = services[node].append(request.clone()).await.unwrap();
            services[node]
                .commit(ReplicaCommitRequest {
                    feed_id,
                    range_id: current.range_id,
                    generation: current.generation,
                    ownership_epoch: current.ownership_epoch,
                    append_owner: current.owner.clone(),
                    commit_position: CommitPosition::new(sequence),
                    frame_digest: accepted.frame_digest,
                })
                .await
                .unwrap();
        }
    }
    let records = target.read_committed(feed_id, None, 20).await.unwrap();
    assert_eq!(records.len(), 11);
    assert_eq!(records[3].cursor, "cursor-4");
    assert_eq!(records[4].cursor, "cursor-5");
    assert_eq!(
        records[2].frame,
        source.read_committed(feed_id, None, 10).await.unwrap()[2].frame
    );
    let restarted = ReplicaAppendService::new(
        directory.path().join("storage-4"),
        node("storage-4"),
        control,
    );
    let (frames, status) = tokio::join!(
        restarted.read_committed(feed_id, None, 20),
        restarted.recovery_status_for_assignment(&current),
    );
    assert_eq!(frames.unwrap().len(), 11);
    assert_eq!(status.unwrap().committed, CommitPosition::new(11));
}
