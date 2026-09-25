use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendInput {
    pub message_id: Uuid,
    pub producer_id: Uuid,
    pub producer_sequence: u64,
    pub event_time_ns: i64,
    pub ingest_time_ns: i64,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
    pub metadata: BTreeMap<String, Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredRecord {
    pub message_id: Uuid,
    pub producer_id: Uuid,
    pub producer_sequence: u64,
    pub event_time_ns: i64,
    pub ingest_time_ns: i64,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
    pub metadata: BTreeMap<String, Vec<u8>>,
}

impl From<AppendInput> for StoredRecord {
    fn from(value: AppendInput) -> Self {
        Self {
            message_id: value.message_id,
            producer_id: value.producer_id,
            producer_sequence: value.producer_sequence,
            event_time_ns: value.event_time_ns,
            ingest_time_ns: value.ingest_time_ns,
            key: value.key,
            payload: value.payload,
            metadata: value.metadata,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendResult {
    pub cursor: String,
    pub message_id: Uuid,
    pub deduplicated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CursorRecord {
    pub cursor: String,
    pub record: StoredRecord,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamDescription {
    pub name: String,
    pub records: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageStats {
    pub stream_count: u64,
    pub record_count: u64,
    pub bytes_on_disk: u64,
    pub safe_to_remove: bool,
}
