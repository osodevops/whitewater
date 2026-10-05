use std::{collections::BTreeMap, time::Duration};

use finnstream::{
    active_range::RangeId,
    reader::{
        FjallReaderProgressStore, ReaderDeliveryMutation, ReaderDeliveryReceipt,
        ReaderPacingController, ReaderPressureSample, ReaderProgressEngine, ReaderProgressError,
    },
};
use tempfile::TempDir;
use uuid::Uuid;

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
