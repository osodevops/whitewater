use std::{collections::BTreeMap, sync::Arc};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use finnstream::{
    active_range::{
        AppendIdentity, RangeGeneration, RangeId, RangePosition, ReplicaAppendErrorCode,
        ReplicaAppendRequest, ReplicaAppendService, StorageNodeId,
    },
    codec::encode_record,
    control::{Command, ControlController},
    domain::StoredRecord,
    storage::{FileLogStore, LogStore},
};
use tempfile::TempDir;
use uuid::Uuid;

struct Fixture {
    _catalog_directory: TempDir,
    replica_directory: TempDir,
    controller: Arc<ControlController>,
    feed_id: Uuid,
    range_id: RangeId,
    owner: StorageNodeId,
    replicas: Vec<StorageNodeId>,
}

impl Fixture {
    async fn new() -> Self {
        let catalog_directory = TempDir::new().unwrap();
        let replica_directory = TempDir::new().unwrap();
        let store: Arc<dyn LogStore> =
            Arc::new(FileLogStore::open(catalog_directory.path().join("legacy-data")).unwrap());
        let controller = Arc::new(
            ControlController::open_with_storage_nodes(
                catalog_directory.path().join("catalog.json"),
                store,
                ["storage-1", "storage-2", "storage-3"]
                    .into_iter()
                    .map(|node| StorageNodeId::try_new(node).unwrap())
                    .collect(),
            )
            .unwrap(),
        );
        let execution = controller
            .execute_commands(vec![
                Command::CreateSpace {
                    name: "orders".to_owned(),
                },
                Command::CreateFeed {
                    name: "orders.created".to_owned(),
                },
            ])
            .await
            .unwrap();
        let feed_id = serde_json::from_value(execution.results[1].data["feed_id"].clone()).unwrap();
        let placement = controller.active_range_assignment(feed_id).await.unwrap();
        Self {
            _catalog_directory: catalog_directory,
            replica_directory,
            controller,
            feed_id,
            range_id: placement.range_id,
            owner: placement.owner.clone(),
            replicas: placement.replicas.iter().cloned().collect(),
        }
    }

    fn service(&self, local: &str) -> ReplicaAppendService {
        ReplicaAppendService::new(
            self.replica_directory.path().join(local),
            StorageNodeId::try_new(local).unwrap(),
            self.controller.clone(),
        )
    }

    fn request(&self, position: u64, sequence: u64, payload: &[u8]) -> ReplicaAppendRequest {
        let writer_session_id = Uuid::from_u128(900);
        let record = StoredRecord {
            message_id: Uuid::from_u128(1_000 + sequence as u128),
            producer_id: writer_session_id,
            producer_sequence: sequence,
            event_time_ns: sequence as i64,
            ingest_time_ns: sequence as i64 + 1,
            key: b"customer-1".to_vec(),
            payload: payload.to_vec(),
            metadata: BTreeMap::new(),
        };
        ReplicaAppendRequest {
            feed_id: self.feed_id,
            range_id: self.range_id,
            generation: RangeGeneration::new(1),
            ownership_epoch: finnstream::active_range::OwnershipEpoch::new(1),
            append_owner: self.owner.clone(),
            expected_position: RangePosition::new(position),
            identity: AppendIdentity {
                writer_session_id,
                writer_epoch: 1,
                sequence,
            },
            cursor: format!("cursor-{sequence}"),
            frame_base64: STANDARD.encode(encode_record(&record).unwrap()),
        }
    }
}

#[tokio::test]
async fn replica_persists_the_exact_frame_and_identical_retry_is_idempotent() {
    let fixture = Fixture::new().await;
    let service = fixture.service("storage-2");
    let request = fixture.request(1, 1, b"one");
    let expected_digest = blake3::hash(&STANDARD.decode(&request.frame_base64).unwrap());

    let accepted = service.append(request.clone()).await.unwrap();
    assert_eq!(accepted.position, RangePosition::new(1));
    assert_eq!(accepted.frame_digest, *expected_digest.as_bytes());
    assert!(!accepted.deduplicated);
    assert!(accepted.durable);

    let retry = service.append(request).await.unwrap();
    assert_eq!(retry.position, accepted.position);
    assert_eq!(retry.frame_digest, accepted.frame_digest);
    assert!(retry.deduplicated);
}

#[tokio::test]
async fn all_assigned_replicas_persist_identical_checksummed_bytes() {
    let fixture = Fixture::new().await;
    let request = fixture.request(1, 1, b"identical");
    let expected_digest =
        *blake3::hash(&STANDARD.decode(&request.frame_base64).unwrap()).as_bytes();

    for replica in fixture.replicas.clone() {
        let accepted = fixture
            .service(replica.as_str())
            .append(request.clone())
            .await
            .unwrap();
        assert_eq!(accepted.position, RangePosition::new(1));
        assert_eq!(accepted.frame_digest, expected_digest);
        assert!(accepted.durable);
    }
}

#[tokio::test]
async fn uncommitted_frames_are_invisible_until_commit_position_is_durable() {
    let fixture = Fixture::new().await;
    let service = fixture.service("storage-2");
    let request = fixture.request(1, 1, b"committed-only");
    let accepted = service.append(request.clone()).await.unwrap();
    assert!(service
        .read_committed(fixture.feed_id, None, 10)
        .await
        .unwrap()
        .is_empty());
    service
        .commit(finnstream::active_range::ReplicaCommitRequest {
            feed_id: fixture.feed_id,
            range_id: fixture.range_id,
            generation: request.generation,
            ownership_epoch: request.ownership_epoch,
            append_owner: request.append_owner,
            commit_position: finnstream::active_range::CommitPosition::new(1),
            frame_digest: accepted.frame_digest,
        })
        .await
        .unwrap();
    let records = service
        .read_committed(fixture.feed_id, None, 10)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].cursor, "cursor-1");
    assert!(service
        .read_committed(fixture.feed_id, Some("cursor-1"), 10)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn replica_rejects_position_gaps_and_conflicting_bytes() {
    let fixture = Fixture::new().await;
    let service = fixture.service("storage-2");
    service.append(fixture.request(1, 1, b"one")).await.unwrap();

    let gap = service
        .append(fixture.request(3, 2, b"three"))
        .await
        .unwrap_err();
    assert_eq!(gap.code, ReplicaAppendErrorCode::PositionGap);
    let conflict = service
        .append(fixture.request(1, 2, b"different"))
        .await
        .unwrap_err();
    assert_eq!(conflict.code, ReplicaAppendErrorCode::PositionConflict);
}

#[tokio::test]
async fn replica_rejects_invalid_frame_checksum_before_storage_mutation() {
    let fixture = Fixture::new().await;
    let service = fixture.service("storage-2");
    let mut request = fixture.request(1, 1, b"one");
    let mut frame = STANDARD.decode(&request.frame_base64).unwrap();
    let last = frame.len() - 1;
    frame[last] ^= 0xff;
    request.frame_base64 = STANDARD.encode(frame);

    let error = service.append(request).await.unwrap_err();
    assert_eq!(error.code, ReplicaAppendErrorCode::InvalidFrame);
    let accepted = service.append(fixture.request(1, 1, b"one")).await.unwrap();
    assert_eq!(accepted.position, RangePosition::new(1));
}

#[tokio::test]
async fn replica_rejects_wrong_range_generation_epoch_owner_and_receiver() {
    let fixture = Fixture::new().await;
    let service = fixture.service("storage-2");

    let mut wrong_range = fixture.request(1, 1, b"one");
    wrong_range.range_id = RangeId::new();
    assert_eq!(
        service.append(wrong_range).await.unwrap_err().code,
        ReplicaAppendErrorCode::WrongRange
    );

    let mut wrong_generation = fixture.request(1, 1, b"one");
    wrong_generation.generation = RangeGeneration::new(2);
    assert_eq!(
        service.append(wrong_generation).await.unwrap_err().code,
        ReplicaAppendErrorCode::WrongGeneration
    );

    let mut stale_epoch = fixture.request(1, 1, b"one");
    stale_epoch.ownership_epoch = finnstream::active_range::OwnershipEpoch::new(0);
    assert_eq!(
        service.append(stale_epoch).await.unwrap_err().code,
        ReplicaAppendErrorCode::StaleOwnershipEpoch
    );

    let mut wrong_owner = fixture.request(1, 1, b"one");
    wrong_owner.append_owner = fixture
        .replicas
        .iter()
        .find(|replica| **replica != fixture.owner)
        .unwrap()
        .clone();
    assert_eq!(
        service.append(wrong_owner).await.unwrap_err().code,
        ReplicaAppendErrorCode::NotCurrentOwner
    );

    let non_replica = fixture.service("storage-9");
    assert_eq!(
        non_replica
            .append(fixture.request(1, 1, b"one"))
            .await
            .unwrap_err()
            .code,
        ReplicaAppendErrorCode::ReceiverNotReplica
    );
}

#[tokio::test]
async fn replica_rejects_unknown_feed_and_oversized_or_invalid_base64() {
    let fixture = Fixture::new().await;
    let service = fixture.service("storage-2");
    let mut unknown_feed = fixture.request(1, 1, b"one");
    unknown_feed.feed_id = Uuid::new_v4();
    assert_eq!(
        service.append(unknown_feed).await.unwrap_err().code,
        ReplicaAppendErrorCode::AssignmentNotFound
    );

    let mut invalid = fixture.request(1, 1, b"one");
    invalid.frame_base64 = "not-base64".to_owned();
    assert_eq!(
        service.append(invalid).await.unwrap_err().code,
        ReplicaAppendErrorCode::InvalidFrameEncoding
    );

    let mut oversized = fixture.request(1, 1, b"one");
    oversized.frame_base64 =
        "A".repeat(finnstream::active_range::MAX_REPLICA_FRAME_BASE64_BYTES + 1);
    assert_eq!(
        service.append(oversized).await.unwrap_err().code,
        ReplicaAppendErrorCode::FrameTooLarge
    );
}
