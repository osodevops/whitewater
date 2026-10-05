use std::{collections::BTreeMap, path::Path, time::Duration};

use fjall::Readable;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::active_range::RangeId;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReaderRetryPolicy {
    pub max_attempts: usize,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl ReaderRetryPolicy {
    pub fn delay(&self, attempt: usize, server_retry_after_ms: u64) -> Duration {
        let multiplier = 1_u32
            .checked_shl(attempt.min(20) as u32)
            .unwrap_or(u32::MAX);
        self.base_delay
            .saturating_mul(multiplier)
            .max(Duration::from_millis(server_retry_after_ms))
            .min(self.max_delay)
    }
}

impl Default for ReaderRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ReaderPressureSample {
    pub backlog_records: u64,
    pub average_record_bytes: usize,
    pub acknowledgement_latency: Duration,
    pub unacknowledged_bytes: usize,
    pub node_pressure: f64,
    pub replica_ready: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReaderPacingPlan {
    pub max_records: usize,
    pub max_bytes: usize,
    pub retry_after: Duration,
    pub record_exceeds_budget: bool,
}

pub struct ReaderPacingController {
    min_records: usize,
    max_records: usize,
    max_bytes: usize,
    target_ack_latency: Duration,
    current_records: usize,
    healthy_samples: usize,
}

impl ReaderPacingController {
    pub fn new(
        min_records: usize,
        max_records: usize,
        max_bytes: usize,
        target_ack_latency: Duration,
    ) -> Self {
        let min_records = min_records.max(1);
        let max_records = max_records.max(min_records);
        Self {
            min_records,
            max_records,
            max_bytes: max_bytes.max(1),
            target_ack_latency,
            current_records: min_records,
            healthy_samples: 0,
        }
    }

    pub fn observe(
        &mut self,
        sample: ReaderPressureSample,
        client_capacity: usize,
    ) -> ReaderPacingPlan {
        let pressured = !sample.replica_ready
            || !sample.node_pressure.is_finite()
            || sample.node_pressure.clamp(0.0, 1.0) >= 0.75
            || sample.acknowledgement_latency > self.target_ack_latency
            || sample.unacknowledged_bytes >= self.max_bytes;
        if pressured {
            self.current_records = self.current_records.div_ceil(2).max(self.min_records);
            self.healthy_samples = 0;
        } else if sample.backlog_records > self.current_records as u64 {
            self.healthy_samples = self.healthy_samples.saturating_add(1);
            if self.healthy_samples >= 3 {
                self.current_records = self
                    .current_records
                    .saturating_add((self.current_records / 8).max(1))
                    .min(self.max_records);
                self.healthy_samples = 0;
            }
        } else {
            self.healthy_samples = 0;
        }
        let max_bytes = self.max_bytes.saturating_sub(sample.unacknowledged_bytes);
        let by_bytes = max_bytes / sample.average_record_bytes.max(1);
        ReaderPacingPlan {
            max_records: self.current_records.min(client_capacity).min(by_bytes),
            max_bytes,
            record_exceeds_budget: sample.average_record_bytes > self.max_bytes,
            retry_after: if !sample.replica_ready {
                Duration::from_millis(200)
            } else if pressured || by_bytes == 0 {
                Duration::from_millis(50)
            } else {
                Duration::ZERO
            },
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReaderDeliveryReceipt {
    pub request_id: Uuid,
    pub cursor: String,
    pub records: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReaderDeliveryMutation {
    pub reader_id: Uuid,
    pub epoch: u64,
    pub expected_cursor: Option<String>,
    pub positions: BTreeMap<RangeId, String>,
    pub receipt: ReaderDeliveryReceipt,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReaderProgressSnapshot {
    pub reader_id: Uuid,
    pub feed_id: Uuid,
    pub epoch: u64,
    pub delivered: BTreeMap<RangeId, String>,
    pub acknowledged: BTreeMap<RangeId, String>,
    pub delivered_cursor: Option<String>,
    pub acknowledged_cursor: Option<String>,
    pub last_delivery: Option<ReaderDeliveryReceipt>,
}

#[derive(Debug, Error)]
pub enum ReaderProgressError {
    #[error(transparent)]
    Engine(#[from] fjall::Error),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
    #[error("Reader progress belongs to a different Feed")]
    WrongFeed,
    #[error("Reader session epoch is stale or inactive")]
    StaleEpoch,
    #[error("Reader delivery has a conflicting Cursor, request ID, or frontier")]
    ConflictingDelivery,
    #[error("Reader acknowledgement does not match the latest delivered Cursor")]
    InvalidAcknowledgement,
    #[error("Reader progress exceeds the bounded local storage budget")]
    TooLarge,
    #[error("Reader progress has a corrupt primary identity")]
    CorruptPrimary,
}

pub trait ReaderProgressEngine {
    fn get(&self, reader_id: Uuid) -> Result<Option<ReaderProgressSnapshot>, ReaderProgressError>;
    fn open_session(
        &self,
        reader_id: Uuid,
        feed_id: Uuid,
        epoch: u64,
    ) -> Result<ReaderProgressSnapshot, ReaderProgressError>;
    fn deliver(
        &self,
        mutation: ReaderDeliveryMutation,
    ) -> Result<(ReaderProgressSnapshot, ReaderDeliveryReceipt), ReaderProgressError>;
    fn acknowledge(
        &self,
        reader_id: Uuid,
        epoch: u64,
        cursor: &str,
    ) -> Result<ReaderProgressSnapshot, ReaderProgressError>;
}

pub struct FjallReaderProgressStore {
    db: fjall::SingleWriterTxDatabase,
    progress: fjall::SingleWriterTxKeyspace,
}

impl FjallReaderProgressStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ReaderProgressError> {
        let db = fjall::SingleWriterTxDatabase::builder(path).open()?;
        let progress = db.keyspace("reader_progress", fjall::KeyspaceCreateOptions::default)?;
        Ok(Self { db, progress })
    }

    pub fn get(
        &self,
        reader_id: Uuid,
    ) -> Result<Option<ReaderProgressSnapshot>, ReaderProgressError> {
        self.db
            .read_tx()
            .get(&self.progress, reader_id.as_bytes())?
            .map(|bytes| {
                serde_json::from_slice::<ReaderProgressSnapshot>(&bytes).map_err(Into::into)
            })
            .transpose()
            .and_then(|row| match row {
                Some(value) if value.reader_id != reader_id => {
                    Err(ReaderProgressError::CorruptPrimary)
                }
                other => Ok(other),
            })
    }

    pub fn open_session(
        &self,
        reader_id: Uuid,
        feed_id: Uuid,
        epoch: u64,
    ) -> Result<ReaderProgressSnapshot, ReaderProgressError> {
        if epoch == 0 {
            return Err(ReaderProgressError::StaleEpoch);
        }
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let previous = tx
            .get(&self.progress, reader_id.as_bytes())?
            .map(|bytes| serde_json::from_slice::<ReaderProgressSnapshot>(&bytes))
            .transpose()?;
        let row = match previous {
            Some(mut row) => {
                if row.reader_id != reader_id {
                    return Err(ReaderProgressError::CorruptPrimary);
                }
                if row.feed_id != feed_id {
                    return Err(ReaderProgressError::WrongFeed);
                }
                if epoch < row.epoch {
                    return Err(ReaderProgressError::StaleEpoch);
                }
                if epoch == row.epoch {
                    return Ok(row);
                }
                row.epoch = epoch;
                row.delivered = row.acknowledged.clone();
                row.delivered_cursor = row.acknowledged_cursor.clone();
                row.last_delivery = None;
                row
            }
            None => ReaderProgressSnapshot {
                reader_id,
                feed_id,
                epoch,
                delivered: BTreeMap::new(),
                acknowledged: BTreeMap::new(),
                delivered_cursor: None,
                acknowledged_cursor: None,
                last_delivery: None,
            },
        };
        Self::persist(&mut tx, &self.progress, &row)?;
        tx.commit()?;
        Ok(row)
    }

    pub fn deliver(
        &self,
        mutation: ReaderDeliveryMutation,
    ) -> Result<(ReaderProgressSnapshot, ReaderDeliveryReceipt), ReaderProgressError> {
        let ReaderDeliveryMutation {
            reader_id,
            epoch,
            expected_cursor,
            positions,
            receipt,
        } = mutation;
        if receipt.cursor.is_empty()
            || receipt.cursor.len() > 256
            || positions.is_empty()
            || positions.len() > 128
            || positions.values().any(|value| value.len() > 256)
            || receipt.records.is_empty()
            || receipt.records.len() > 1_024
            || receipt
                .records
                .iter()
                .any(|value| value.is_empty() || value.len() > 256)
        {
            return Err(ReaderProgressError::TooLarge);
        }
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let mut row = self.load_for_update(&mut tx, reader_id, epoch)?;
        if row
            .last_delivery
            .as_ref()
            .is_some_and(|previous| previous.request_id == receipt.request_id)
        {
            return if row.last_delivery.as_ref() == Some(&receipt) && row.delivered == positions {
                Ok((row, receipt))
            } else {
                Err(ReaderProgressError::ConflictingDelivery)
            };
        }
        if row.delivered_cursor != expected_cursor {
            return Err(ReaderProgressError::ConflictingDelivery);
        }
        row.delivered = positions;
        row.delivered_cursor = Some(receipt.cursor.clone());
        row.last_delivery = Some(receipt.clone());
        Self::persist(&mut tx, &self.progress, &row)?;
        tx.commit()?;
        Ok((row, receipt))
    }

    pub fn acknowledge(
        &self,
        reader_id: Uuid,
        epoch: u64,
        cursor: &str,
    ) -> Result<ReaderProgressSnapshot, ReaderProgressError> {
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let mut row = self.load_for_update(&mut tx, reader_id, epoch)?;
        if row.acknowledged_cursor.as_deref() == Some(cursor) {
            return Ok(row);
        }
        if row.delivered_cursor.as_deref() != Some(cursor) {
            return Err(ReaderProgressError::InvalidAcknowledgement);
        }
        row.acknowledged = row.delivered.clone();
        row.acknowledged_cursor = Some(cursor.to_owned());
        Self::persist(&mut tx, &self.progress, &row)?;
        tx.commit()?;
        Ok(row)
    }

    fn load_for_update(
        &self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        reader_id: Uuid,
        epoch: u64,
    ) -> Result<ReaderProgressSnapshot, ReaderProgressError> {
        let row = tx
            .get(&self.progress, reader_id.as_bytes())?
            .ok_or(ReaderProgressError::StaleEpoch)?;
        let row: ReaderProgressSnapshot = serde_json::from_slice(&row)?;
        if row.reader_id != reader_id {
            return Err(ReaderProgressError::CorruptPrimary);
        }
        if row.epoch != epoch {
            return Err(ReaderProgressError::StaleEpoch);
        }
        Ok(row)
    }

    fn persist(
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        keyspace: &fjall::SingleWriterTxKeyspace,
        row: &ReaderProgressSnapshot,
    ) -> Result<(), ReaderProgressError> {
        let bytes = serde_json::to_vec(row)?;
        if bytes.len() > 256 * 1024 {
            return Err(ReaderProgressError::TooLarge);
        }
        tx.insert(keyspace, row.reader_id.as_bytes(), bytes);
        Ok(())
    }
}

impl ReaderProgressEngine for FjallReaderProgressStore {
    fn get(&self, reader_id: Uuid) -> Result<Option<ReaderProgressSnapshot>, ReaderProgressError> {
        FjallReaderProgressStore::get(self, reader_id)
    }

    fn open_session(
        &self,
        reader_id: Uuid,
        feed_id: Uuid,
        epoch: u64,
    ) -> Result<ReaderProgressSnapshot, ReaderProgressError> {
        FjallReaderProgressStore::open_session(self, reader_id, feed_id, epoch)
    }

    fn deliver(
        &self,
        mutation: ReaderDeliveryMutation,
    ) -> Result<(ReaderProgressSnapshot, ReaderDeliveryReceipt), ReaderProgressError> {
        FjallReaderProgressStore::deliver(self, mutation)
    }

    fn acknowledge(
        &self,
        reader_id: Uuid,
        epoch: u64,
        cursor: &str,
    ) -> Result<ReaderProgressSnapshot, ReaderProgressError> {
        FjallReaderProgressStore::acknowledge(self, reader_id, epoch, cursor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_backoff_is_bounded_and_respects_server_delay() {
        let policy = ReaderRetryPolicy::default();
        assert_eq!(policy.delay(0, 0), Duration::from_millis(100));
        assert_eq!(policy.delay(2, 0), Duration::from_millis(400));
        assert_eq!(policy.delay(1, 750), Duration::from_millis(750));
        assert_eq!(policy.delay(20, 0), Duration::from_secs(5));
    }

    #[test]
    fn pacing_grows_cautiously_and_isolates_a_slow_reader() {
        let mut fast = ReaderPacingController::new(2, 64, 1024, Duration::from_millis(100));
        let mut slow = ReaderPacingController::new(2, 64, 1024, Duration::from_millis(100));
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
        let fast_plan = fast.observe(healthy, 64);
        assert!(fast_plan.max_records > 2 && fast_plan.max_records <= 64);
        assert_eq!(fast.observe(healthy, 3).max_records, 3);
        let pressured = ReaderPressureSample {
            acknowledgement_latency: Duration::from_millis(200),
            node_pressure: 0.9,
            ..healthy
        };
        assert_eq!(slow.observe(pressured, 64).max_records, 2);
        assert!(fast.observe(healthy, 64).max_records > slow.observe(pressured, 64).max_records);
        let blocked = ReaderPressureSample {
            unacknowledged_bytes: 1024,
            ..healthy
        };
        let blocked_plan = fast.observe(blocked, 64);
        assert_eq!(blocked_plan.max_records, 0);
        assert!(blocked_plan.retry_after > Duration::ZERO);
        let unavailable = fast.observe(
            ReaderPressureSample {
                replica_ready: false,
                ..healthy
            },
            64,
        );
        assert_eq!(unavailable.retry_after, Duration::from_millis(200));
        let oversized = fast.observe(
            ReaderPressureSample {
                average_record_bytes: 2048,
                ..healthy
            },
            64,
        );
        assert!(oversized.record_exceeds_budget);
        assert_eq!(oversized.max_records, 0);
    }

    #[test]
    fn local_progress_is_independent_durable_and_replays_one_fetch_identity() {
        let directory = tempfile::TempDir::new().unwrap();
        let store = FjallReaderProgressStore::open(directory.path()).unwrap();
        let first = Uuid::from_u128(1);
        let second = Uuid::from_u128(2);
        let feed = Uuid::from_u128(3);
        let range = RangeId::from_uuid(Uuid::from_u128(4));
        let request = Uuid::from_u128(5);
        store.open_session(first, feed, 1).unwrap();
        store.open_session(second, feed, 1).unwrap();
        let positions = BTreeMap::from([(range, "record-1".to_owned())]);
        let mutation = ReaderDeliveryMutation {
            reader_id: first,
            epoch: 1,
            expected_cursor: None,
            positions: positions.clone(),
            receipt: ReaderDeliveryReceipt {
                request_id: request,
                cursor: "rf1_first".to_owned(),
                records: vec!["record-1".to_owned()],
            },
        };
        let (_, receipt) = store.deliver(mutation.clone()).unwrap();
        let (_, retry) = store.deliver(mutation.clone()).unwrap();
        assert_eq!(receipt, retry);
        let mut conflicting = mutation.clone();
        conflicting.receipt.cursor = "rf1_other".to_owned();
        assert!(matches!(
            store.deliver(conflicting),
            Err(ReaderProgressError::ConflictingDelivery)
        ));
        let mut stale = mutation.clone();
        stale.receipt.request_id = Uuid::from_u128(6);
        assert!(matches!(
            store.deliver(stale),
            Err(ReaderProgressError::ConflictingDelivery)
        ));
        let mut other_reader = mutation;
        other_reader.reader_id = second;
        other_reader.receipt.request_id = request;
        other_reader.receipt.cursor = "rf1_second".to_owned();
        store.deliver(other_reader).unwrap();
        let acknowledged = store.acknowledge(first, 1, "rf1_first").unwrap();
        assert_eq!(
            store.acknowledge(first, 1, "rf1_first").unwrap(),
            acknowledged
        );
        let engine: &dyn ReaderProgressEngine = &store;
        assert!(engine.get(second).unwrap().unwrap().acknowledged.is_empty());
        drop(store);
        let reopened = FjallReaderProgressStore::open(directory.path()).unwrap();
        let resumed = reopened.open_session(first, feed, 2).unwrap();
        assert_eq!(resumed.delivered, positions);
        assert_eq!(resumed.acknowledged_cursor.as_deref(), Some("rf1_first"));
        assert!(resumed.last_delivery.is_none());
        let unacknowledged = reopened.open_session(second, feed, 2).unwrap();
        assert!(unacknowledged.delivered.is_empty());
        assert_eq!(unacknowledged.delivered_cursor, None);
        assert!(matches!(
            reopened.acknowledge(first, 1, "rf1_first"),
            Err(ReaderProgressError::StaleEpoch)
        ));
        assert!(matches!(
            reopened.open_session(first, Uuid::from_u128(9), 2),
            Err(ReaderProgressError::WrongFeed)
        ));
    }

    #[test]
    fn progress_budget_rejects_oversized_results_without_advancing() {
        let directory = tempfile::TempDir::new().unwrap();
        let store = FjallReaderProgressStore::open(directory.path()).unwrap();
        let reader = Uuid::from_u128(1);
        store.open_session(reader, Uuid::from_u128(2), 1).unwrap();
        let range = RangeId::from_uuid(Uuid::from_u128(3));
        assert!(matches!(
            store.deliver(ReaderDeliveryMutation {
                reader_id: reader,
                epoch: 1,
                expected_cursor: None,
                positions: BTreeMap::from([(range, "record".to_owned())]),
                receipt: ReaderDeliveryReceipt {
                    request_id: Uuid::from_u128(4),
                    cursor: "rf1_page".to_owned(),
                    records: vec!["record".to_owned(); 1_025],
                },
            }),
            Err(ReaderProgressError::TooLarge)
        ));
        assert_eq!(store.get(reader).unwrap().unwrap().delivered_cursor, None);
    }
}
