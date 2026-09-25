use std::collections::BTreeMap;

use finnstream::{
    domain::AppendInput,
    storage::{FileLogStore, LogStore},
};
use tempfile::TempDir;
use uuid::Uuid;

#[tokio::test]
async fn records_and_deduplication_survive_restart() {
    let directory = TempDir::new().unwrap();
    let producer_id = Uuid::new_v4();
    let input = AppendInput {
        message_id: Uuid::new_v4(),
        producer_id,
        producer_sequence: 1,
        event_time_ns: 10,
        ingest_time_ns: 11,
        key: b"account-1".to_vec(),
        payload: b"credited".to_vec(),
        metadata: BTreeMap::new(),
    };
    let store = FileLogStore::open(directory.path()).unwrap();
    let original = store.append("/ledger/events", input.clone()).await.unwrap();
    drop(store);

    let reopened = FileLogStore::open(directory.path()).unwrap();
    let duplicate = reopened.append("/ledger/events", input).await.unwrap();
    assert!(duplicate.deduplicated);
    assert_eq!(duplicate.cursor, original.cursor);
    assert_eq!(
        reopened
            .read("/ledger/events", None, 10)
            .await
            .unwrap()
            .len(),
        1
    );
}
