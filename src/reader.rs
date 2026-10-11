use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::Duration,
};

use fjall::Readable;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::active_range::{KeyToken, RangeId, RangePosition};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReaderLineageEntry {
    pub range_id: RangeId,
    pub position: RangePosition,
    pub cursor: String,
    pub key_token: KeyToken,
    pub ingest_time_ns: i64,
    pub message_id: Uuid,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ReaderFrontierTranslationError {
    #[error("Reader frontier source history is incomplete, duplicated, or exceeds the 10,000-record transition budget")]
    InvalidHistory,
    #[error("Reader acknowledged Cursor is missing from the frozen committed source history")]
    MissingCursor,
    #[error("Reader split or merge source ranges are invalid")]
    InvalidSources,
}

fn acknowledged_source_position(
    entries: &[ReaderLineageEntry],
    range_id: RangeId,
    cursor: &str,
) -> Result<u64, ReaderFrontierTranslationError> {
    if entries.len() > 10_000 {
        return Err(ReaderFrontierTranslationError::InvalidHistory);
    }
    let mut seen = BTreeSet::new();
    let mut acknowledged = (!cursor.is_empty()).then_some(None);
    for (index, item) in entries.iter().enumerate() {
        if item.range_id != range_id
            || item.position.value() != index as u64 + 1
            || item.cursor.is_empty()
            || item.cursor.len() > 256
            || !seen.insert(item.cursor.as_str())
        {
            return Err(ReaderFrontierTranslationError::InvalidHistory);
        }
        if item.cursor == cursor {
            acknowledged = Some(Some(item.position.value()));
        }
    }
    match acknowledged {
        None => Ok(0),
        Some(Some(position)) => Ok(position),
        Some(None) => Err(ReaderFrontierTranslationError::MissingCursor),
    }
}

pub fn translate_split_reader_frontier(
    entries: &[ReaderLineageEntry],
    source: RangeId,
    left: RangeId,
    right: RangeId,
    split_at: KeyToken,
    acknowledged_cursor: &str,
) -> Result<BTreeMap<RangeId, String>, ReaderFrontierTranslationError> {
    if source != left || left == right {
        return Err(ReaderFrontierTranslationError::InvalidSources);
    }
    let position = acknowledged_source_position(entries, source, acknowledged_cursor)?;
    let mut translated = BTreeMap::from([(left, String::new()), (right, String::new())]);
    for entry in entries.iter().take(position as usize) {
        let range = if entry.key_token < split_at {
            left
        } else {
            right
        };
        translated.insert(range, entry.cursor.clone());
    }
    Ok(translated)
}

pub fn translate_merge_reader_frontier(
    left_entries: &[ReaderLineageEntry],
    right_entries: &[ReaderLineageEntry],
    left: RangeId,
    right: RangeId,
    left_cursor: &str,
    right_cursor: &str,
) -> Result<String, ReaderFrontierTranslationError> {
    if left == right {
        return Err(ReaderFrontierTranslationError::InvalidSources);
    }
    let left_position = acknowledged_source_position(left_entries, left, left_cursor)?;
    let right_position = acknowledged_source_position(right_entries, right, right_cursor)?;
    let mut merged = left_entries.iter().chain(right_entries).collect::<Vec<_>>();
    let mut cursors = BTreeSet::new();
    if merged
        .iter()
        .any(|entry| !cursors.insert(entry.cursor.as_str()))
    {
        return Err(ReaderFrontierTranslationError::InvalidHistory);
    }
    merged.sort_by_key(|entry| (entry.ingest_time_ns, entry.message_id));
    let mut translated = String::new();
    for entry in merged {
        let boundary = if entry.range_id == left {
            left_position
        } else {
            right_position
        };
        if entry.position.value() > boundary {
            break;
        }
        translated = entry.cursor.clone();
    }
    Ok(translated)
}

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
pub struct SubscriptionWorkLease {
    pub work_id: Uuid,
    pub member_id: Uuid,
    pub member_epoch: u64,
    pub lease_epoch: u64,
    pub expires_at_tick: u64,
}

pub const SUBSCRIPTION_LEASE_MAX_WORK: usize = 256;
pub const SUBSCRIPTION_LEASE_MAX_MEMBER_WORK: usize = 32;
pub const SUBSCRIPTION_LEASE_MAX_OPS: usize = 16;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionMemberState {
    pub member_epochs: BTreeMap<Uuid, u64>,
    pub leases: BTreeMap<Uuid, SubscriptionWorkLease>,
    pub last_tick: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum SubscriptionLeaseOp {
    Join {
        member_id: Uuid,
        member_epoch: u64,
    },
    Claim {
        work_id: Uuid,
        member_id: Uuid,
        member_epoch: u64,
        lease_ticks: u64,
    },
    Renew {
        work_id: Uuid,
        member_id: Uuid,
        member_epoch: u64,
        lease_epoch: u64,
        lease_ticks: u64,
    },
    Release {
        work_id: Uuid,
        member_id: Uuid,
        member_epoch: u64,
        lease_epoch: u64,
    },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SubscriptionLeaseError {
    #[error("work is currently leased to another member")]
    Busy,
    #[error("member lease is stale or expired")]
    StaleLease,
    #[error("lease budget is exhausted")]
    Capacity,
    #[error("lease clock moved backwards or counter overflowed")]
    InvalidClock,
}

pub struct SubscriptionLeaseTracker {
    leases: BTreeMap<Uuid, SubscriptionWorkLease>,
    member_epochs: BTreeMap<Uuid, u64>,
    last_tick: u64,
    max_work: usize,
    max_members: usize,
}

impl SubscriptionLeaseTracker {
    pub fn new(max_work: usize) -> Self {
        let max_work = max_work.clamp(1, 65_536);
        Self {
            leases: BTreeMap::new(),
            member_epochs: BTreeMap::new(),
            last_tick: 0,
            max_work,
            max_members: max_work.saturating_mul(4).min(65_536),
        }
    }

    pub fn fence_member(
        &mut self,
        member_id: Uuid,
        new_epoch: u64,
    ) -> Result<(), SubscriptionLeaseError> {
        if new_epoch == 0
            || self
                .member_epochs
                .get(&member_id)
                .is_some_and(|current| *current >= new_epoch)
        {
            return Err(SubscriptionLeaseError::StaleLease);
        }
        if !self.member_epochs.contains_key(&member_id)
            && self.member_epochs.len() >= self.max_members
        {
            return Err(SubscriptionLeaseError::Capacity);
        }
        self.member_epochs.insert(member_id, new_epoch);
        for lease in self
            .leases
            .values_mut()
            .filter(|lease| lease.member_id == member_id)
        {
            lease.expires_at_tick = self.last_tick;
        }
        Ok(())
    }

    pub fn claim(
        &mut self,
        work_id: Uuid,
        member_id: Uuid,
        member_epoch: u64,
        now_tick: u64,
        lease_ticks: u64,
        max_member_work: usize,
    ) -> Result<SubscriptionWorkLease, SubscriptionLeaseError> {
        self.check_tick(now_tick, lease_ticks)?;
        if self.member_epochs.get(&member_id) != Some(&member_epoch) {
            return Err(SubscriptionLeaseError::StaleLease);
        }
        if max_member_work == 0 {
            return Err(SubscriptionLeaseError::Capacity);
        }
        if let Some(existing) = self.leases.get(&work_id) {
            if now_tick < existing.expires_at_tick {
                if existing.member_id == member_id && existing.member_epoch == member_epoch {
                    return Ok(existing.clone());
                }
                return Err(SubscriptionLeaseError::Busy);
            }
        } else if self.leases.len() >= self.max_work {
            return Err(SubscriptionLeaseError::Capacity);
        }
        let active_for_member = self
            .leases
            .values()
            .filter(|lease| {
                lease.member_id == member_id
                    && lease.member_epoch == member_epoch
                    && now_tick < lease.expires_at_tick
            })
            .count();
        if active_for_member >= max_member_work {
            return Err(SubscriptionLeaseError::Capacity);
        }
        let lease_epoch = self.leases.get(&work_id).map_or(Ok(1), |previous| {
            previous
                .lease_epoch
                .checked_add(1)
                .ok_or(SubscriptionLeaseError::InvalidClock)
        })?;
        let expires_at_tick = now_tick
            .checked_add(lease_ticks)
            .ok_or(SubscriptionLeaseError::InvalidClock)?;
        let granted = SubscriptionWorkLease {
            work_id,
            member_id,
            member_epoch,
            lease_epoch,
            expires_at_tick,
        };
        self.leases.insert(work_id, granted.clone());
        self.last_tick = now_tick;
        Ok(granted)
    }

    pub fn renew(
        &mut self,
        grant: &SubscriptionWorkLease,
        now_tick: u64,
        lease_ticks: u64,
    ) -> Result<SubscriptionWorkLease, SubscriptionLeaseError> {
        self.check_tick(now_tick, lease_ticks)?;
        if !self.can_ack(grant, now_tick) {
            return Err(SubscriptionLeaseError::StaleLease);
        }
        let expires_at_tick = now_tick
            .checked_add(lease_ticks)
            .ok_or(SubscriptionLeaseError::InvalidClock)?;
        let current = self
            .leases
            .get_mut(&grant.work_id)
            .ok_or(SubscriptionLeaseError::StaleLease)?;
        current.expires_at_tick = expires_at_tick;
        self.last_tick = now_tick;
        Ok(current.clone())
    }

    pub fn can_ack(&self, grant: &SubscriptionWorkLease, now_tick: u64) -> bool {
        now_tick >= self.last_tick
            && self.member_epochs.get(&grant.member_id) == Some(&grant.member_epoch)
            && self.leases.get(&grant.work_id).is_some_and(|current| {
                current.member_id == grant.member_id
                    && current.member_epoch == grant.member_epoch
                    && current.lease_epoch == grant.lease_epoch
                    && now_tick < current.expires_at_tick
            })
    }

    pub fn from_state(state: SubscriptionMemberState, max_work: usize) -> Self {
        let mut tracker = Self::new(max_work);
        tracker.leases = state.leases;
        tracker.member_epochs = state.member_epochs;
        tracker.last_tick = state.last_tick;
        tracker
    }

    pub fn materialize(&self) -> SubscriptionMemberState {
        SubscriptionMemberState {
            member_epochs: self.member_epochs.clone(),
            leases: self.leases.clone(),
            last_tick: self.last_tick,
        }
    }

    pub fn release(&mut self, grant: &SubscriptionWorkLease) -> Result<(), SubscriptionLeaseError> {
        match self.leases.get(&grant.work_id) {
            Some(current)
                if current.member_id == grant.member_id
                    && current.member_epoch == grant.member_epoch
                    && current.lease_epoch == grant.lease_epoch =>
            {
                self.leases.remove(&grant.work_id);
                Ok(())
            }
            _ => Err(SubscriptionLeaseError::StaleLease),
        }
    }

    pub fn apply_op(
        &mut self,
        op: &SubscriptionLeaseOp,
        now_tick: u64,
    ) -> Result<(), SubscriptionLeaseError> {
        match op {
            SubscriptionLeaseOp::Join {
                member_id,
                member_epoch,
            } => self.fence_member(*member_id, *member_epoch),
            SubscriptionLeaseOp::Claim {
                work_id,
                member_id,
                member_epoch,
                lease_ticks,
            } => self
                .claim(
                    *work_id,
                    *member_id,
                    *member_epoch,
                    now_tick,
                    *lease_ticks,
                    SUBSCRIPTION_LEASE_MAX_MEMBER_WORK,
                )
                .map(|_| ()),
            SubscriptionLeaseOp::Renew {
                work_id,
                member_id,
                member_epoch,
                lease_epoch,
                lease_ticks,
            } => self
                .renew(
                    &SubscriptionWorkLease {
                        work_id: *work_id,
                        member_id: *member_id,
                        member_epoch: *member_epoch,
                        lease_epoch: *lease_epoch,
                        expires_at_tick: 0,
                    },
                    now_tick,
                    *lease_ticks,
                )
                .map(|_| ()),
            SubscriptionLeaseOp::Release {
                work_id,
                member_id,
                member_epoch,
                lease_epoch,
            } => self.release(&SubscriptionWorkLease {
                work_id: *work_id,
                member_id: *member_id,
                member_epoch: *member_epoch,
                lease_epoch: *lease_epoch,
                expires_at_tick: 0,
            }),
        }
    }

    fn check_tick(&self, now_tick: u64, lease_ticks: u64) -> Result<(), SubscriptionLeaseError> {
        if now_tick < self.last_tick
            || lease_ticks == 0
            || now_tick.checked_add(lease_ticks).is_none()
        {
            Err(SubscriptionLeaseError::InvalidClock)
        } else {
            Ok(())
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionProgressMutation {
    pub subscription_id: Uuid,
    pub feed_id: Uuid,
    pub ownership_epoch: u64,
    pub sequence: u64,
    pub request_id: Uuid,
    pub expected_cursor: Option<String>,
    pub cursor: String,
    pub positions: BTreeMap<RangeId, String>,
    #[serde(default)]
    pub tick: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lease_ops: Vec<SubscriptionLeaseOp>,
}

/// A replica answering with one of these errors is behind the current
/// placement (still applying a committed catalog change or holding an older
/// row) rather than contradicting quorum state. Such replies are treated as
/// lagging witnesses and skipped so a single stale replica cannot wedge
/// otherwise healthy progress.
fn lagging_witness(error: &SubscriptionProgressError) -> bool {
    matches!(
        error,
        SubscriptionProgressError::Unavailable
            | SubscriptionProgressError::StaleEpoch
            | SubscriptionProgressError::InvalidAssignment
            | SubscriptionProgressError::Sequence
    )
}

/// Prepare-stage refusals that identify a lagging replica. `Lease`
/// rejections are included because a replica whose member state is behind
/// validates against stale leases; current replicas fence identically, so
/// skipping the stale vote cannot admit a mutation that quorum would reject.
fn lagging_prepare(error: &SubscriptionProgressError) -> bool {
    lagging_witness(error) || matches!(error, SubscriptionProgressError::Lease(_))
}

fn same_progress_identity(
    left: &SubscriptionProgressMutation,
    right: &SubscriptionProgressMutation,
) -> bool {
    left.subscription_id == right.subscription_id
        && left.feed_id == right.feed_id
        && left.sequence == right.sequence
        && left.request_id == right.request_id
        && left.expected_cursor == right.expected_cursor
        && left.cursor == right.cursor
        && left.positions == right.positions
        && left.tick == right.tick
        && left.lease_ops == right.lease_ops
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SubscriptionProgressReplicaRow {
    subscription_id: Uuid,
    feed_id: Uuid,
    ownership_epoch: u64,
    committed: Option<SubscriptionProgressMutation>,
    prepared: Option<SubscriptionProgressMutation>,
    #[serde(default)]
    members: SubscriptionMemberState,
}

#[derive(Debug, Error)]
pub enum SubscriptionProgressError {
    #[error(transparent)]
    Engine(#[from] fjall::Error),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
    #[error("Subscription progress has a conflicting identity, request, or Cursor")]
    Conflict,
    #[error("Subscription progress ownership changed; reconcile before retrying")]
    StaleEpoch,
    #[error("Subscription progress sequence has a gap or overflow")]
    Sequence,
    #[error("Subscription progress exceeds its bounded storage budget")]
    TooLarge,
    #[error("Subscription progress has no valid two-replica commit evidence")]
    NoQuorum,
    #[error("Subscription member lease was rejected: {0}")]
    Lease(#[from] SubscriptionLeaseError),
    #[error("Subscription progress replica placement or owner is invalid")]
    InvalidAssignment,
    #[error("Subscription progress replica is unavailable; retry the same request identity")]
    Unavailable,
    #[error("Subscription progress commit result is ambiguous; retry the same request identity")]
    AmbiguousCommit,
}

impl SubscriptionProgressError {
    /// Wire-stable identifier carried by internal replica responses so a
    /// calling coordinator can distinguish lagging replicas from genuine
    /// progress conflicts instead of collapsing every failure into one
    /// variant.
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Engine(_) => "engine",
            Self::Serialization(_) => "serialization",
            Self::Conflict => "conflict",
            Self::StaleEpoch => "stale_epoch",
            Self::Sequence => "sequence",
            Self::TooLarge => "too_large",
            Self::NoQuorum => "no_quorum",
            Self::Lease(inner) => inner.code(),
            Self::InvalidAssignment => "invalid_assignment",
            Self::Unavailable => "unavailable",
            Self::AmbiguousCommit => "ambiguous_commit",
        }
    }

    pub(crate) fn from_code(code: &str) -> Option<Self> {
        Some(match code {
            "conflict" => Self::Conflict,
            "stale_epoch" => Self::StaleEpoch,
            "sequence" => Self::Sequence,
            "too_large" => Self::TooLarge,
            "no_quorum" => Self::NoQuorum,
            "invalid_assignment" => Self::InvalidAssignment,
            "unavailable" => Self::Unavailable,
            "ambiguous_commit" => Self::AmbiguousCommit,
            other => Self::Lease(SubscriptionLeaseError::from_code(other)?),
        })
    }
}

impl SubscriptionLeaseError {
    fn code(&self) -> &'static str {
        match self {
            Self::Busy => "lease.busy",
            Self::StaleLease => "lease.stale_lease",
            Self::Capacity => "lease.capacity",
            Self::InvalidClock => "lease.invalid_clock",
        }
    }

    fn from_code(code: &str) -> Option<Self> {
        Some(match code {
            "lease.busy" => Self::Busy,
            "lease.stale_lease" => Self::StaleLease,
            "lease.capacity" => Self::Capacity,
            "lease.invalid_clock" => Self::InvalidClock,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubscriptionPrepareVote {
    pub subscription_id: Uuid,
    pub request_id: Uuid,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionReplicaReply<T> {
    pub replica: crate::active_range::StorageNodeId,
    pub subscription_id: Uuid,
    pub ownership_epoch: u64,
    pub result: T,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionCommitEvidence {
    votes: [(crate::active_range::StorageNodeId, [u8; 32]); 2],
    subscription_id: Uuid,
    request_id: Uuid,
}

impl SubscriptionCommitEvidence {
    pub fn new(
        subscription_id: Uuid,
        request_id: Uuid,
        votes: [(crate::active_range::StorageNodeId, [u8; 32]); 2],
    ) -> Result<Self, SubscriptionProgressError> {
        if votes[0].0 == votes[1].0 {
            return Err(SubscriptionProgressError::NoQuorum);
        }
        Ok(Self {
            votes,
            subscription_id,
            request_id,
        })
    }

    /// The committed mutation's request identity, for fault filtering and
    /// diagnostics that must not see quorum votes.
    pub fn request_id(&self) -> Uuid {
        self.request_id
    }
}

pub struct FjallSubscriptionProgressReplica {
    db: fjall::SingleWriterTxDatabase,
    progress: fjall::SingleWriterTxKeyspace,
}

impl FjallSubscriptionProgressReplica {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SubscriptionProgressError> {
        let db = fjall::SingleWriterTxDatabase::builder(path).open()?;
        let progress = db.keyspace(
            "subscription_progress",
            fjall::KeyspaceCreateOptions::default,
        )?;
        Ok(Self { db, progress })
    }

    pub fn local_committed(
        &self,
        subscription_id: Uuid,
    ) -> Result<Option<SubscriptionProgressMutation>, SubscriptionProgressError> {
        let row = self
            .db
            .read_tx()
            .get(&self.progress, subscription_id.as_bytes())?
            .map(|bytes| serde_json::from_slice::<SubscriptionProgressReplicaRow>(&bytes))
            .transpose()?;
        match row {
            Some(row)
                if row.subscription_id != subscription_id
                    || row.committed.as_ref().is_some_and(|committed| {
                        committed.subscription_id != subscription_id
                            || committed.feed_id != row.feed_id
                            || committed.ownership_epoch != row.ownership_epoch
                    }) =>
            {
                Err(SubscriptionProgressError::Conflict)
            }
            Some(row) => Ok(row.committed),
            None => Ok(None),
        }
    }

    pub fn local_state(
        &self,
        subscription_id: Uuid,
    ) -> Result<SubscriptionProgressInspection, SubscriptionProgressError> {
        let row = self
            .db
            .read_tx()
            .get(&self.progress, subscription_id.as_bytes())?
            .map(|bytes| serde_json::from_slice::<SubscriptionProgressReplicaRow>(&bytes))
            .transpose()?;
        match row {
            Some(row)
                if row.subscription_id != subscription_id
                    || row.committed.as_ref().is_some_and(|committed| {
                        committed.subscription_id != subscription_id
                            || committed.feed_id != row.feed_id
                            || committed.ownership_epoch != row.ownership_epoch
                    })
                    || row.prepared.as_ref().is_some_and(|prepared| {
                        prepared.subscription_id != subscription_id
                            || prepared.feed_id != row.feed_id
                            || prepared.ownership_epoch != row.ownership_epoch
                    }) =>
            {
                Err(SubscriptionProgressError::Conflict)
            }
            Some(row) => Ok(SubscriptionProgressInspection {
                committed: row.committed,
                prepared: row.prepared,
                members: row.members,
            }),
            None => Ok(SubscriptionProgressInspection {
                committed: None,
                prepared: None,
                members: SubscriptionMemberState::default(),
            }),
        }
    }

    pub fn adopt_recovered(
        &self,
        subscription_id: Uuid,
        feed_id: Uuid,
        ownership_epoch: u64,
        committed: Option<SubscriptionProgressMutation>,
        members: SubscriptionMemberState,
    ) -> Result<Option<SubscriptionProgressMutation>, SubscriptionProgressError> {
        if ownership_epoch == 0 {
            return Err(SubscriptionProgressError::StaleEpoch);
        }
        if committed.is_none() && members != SubscriptionMemberState::default() {
            return Err(SubscriptionProgressError::Conflict);
        }
        if let Some(mutation) = &committed {
            if mutation.subscription_id != subscription_id
                || mutation.feed_id != feed_id
                || mutation.ownership_epoch != ownership_epoch
            {
                return Err(SubscriptionProgressError::Conflict);
            }
            if mutation.sequence == 0
                || mutation.cursor.is_empty()
                || mutation.cursor.len() > 256
                || mutation.positions.is_empty()
                || mutation.positions.len() > 128
                || mutation.positions.values().any(|value| value.len() > 256)
                || mutation.expected_cursor.as_ref().is_some_and(|expected| {
                    expected.is_empty()
                        || expected.len() > 256
                        || (expected == &mutation.cursor && mutation.lease_ops.is_empty())
                })
                || mutation.lease_ops.len() > SUBSCRIPTION_LEASE_MAX_OPS
                || serde_json::to_vec(mutation)?.len() > 256 * 1024
            {
                return Err(SubscriptionProgressError::TooLarge);
            }
        }
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let previous = tx
            .get(&self.progress, subscription_id.as_bytes())?
            .map(|bytes| serde_json::from_slice::<SubscriptionProgressReplicaRow>(&bytes))
            .transpose()?;
        let mut row = previous.unwrap_or(SubscriptionProgressReplicaRow {
            subscription_id,
            feed_id,
            ownership_epoch: 0,
            committed: None,
            prepared: None,
            members: SubscriptionMemberState::default(),
        });
        if row.subscription_id != subscription_id || row.feed_id != feed_id {
            return Err(SubscriptionProgressError::Conflict);
        }
        if row.ownership_epoch > ownership_epoch {
            return Err(SubscriptionProgressError::StaleEpoch);
        }
        if row.ownership_epoch == ownership_epoch {
            return if row.committed == committed && row.members == members {
                Ok(row.committed)
            } else {
                Err(SubscriptionProgressError::Conflict)
            };
        }
        if let Some(existing) = &row.committed {
            let Some(recovered) = &committed else {
                return Err(SubscriptionProgressError::Conflict);
            };
            if recovered.sequence < existing.sequence
                || (recovered.sequence == existing.sequence
                    && !same_progress_identity(existing, recovered))
                || (recovered.sequence == existing.sequence + 1
                    && recovered.expected_cursor.as_deref() != Some(existing.cursor.as_str()))
            {
                return Err(SubscriptionProgressError::Conflict);
            }
        }
        row.ownership_epoch = ownership_epoch;
        row.committed = committed;
        row.members = members;
        row.prepared = None;
        let encoded = serde_json::to_vec(&row)?;
        if encoded.len() > 256 * 1024 {
            return Err(SubscriptionProgressError::TooLarge);
        }
        tx.insert(&self.progress, row.subscription_id.as_bytes(), encoded);
        tx.commit()?;
        Ok(row.committed)
    }

    pub fn prepare(
        &self,
        mutation: SubscriptionProgressMutation,
    ) -> Result<SubscriptionPrepareVote, SubscriptionProgressError> {
        if mutation.ownership_epoch == 0
            || mutation.sequence == 0
            || mutation.cursor.is_empty()
            || mutation.cursor.len() > 256
            || mutation.positions.is_empty()
            || mutation.positions.len() > 128
            || mutation.positions.values().any(|value| value.len() > 256)
            || mutation.lease_ops.len() > SUBSCRIPTION_LEASE_MAX_OPS
            || mutation.expected_cursor.as_ref().is_some_and(|expected| {
                expected.is_empty()
                    || expected.len() > 256
                    || (expected == &mutation.cursor && mutation.lease_ops.is_empty())
            })
        {
            return Err(SubscriptionProgressError::TooLarge);
        }
        let bytes = serde_json::to_vec(&mutation)?;
        if bytes.len() > 256 * 1024 {
            return Err(SubscriptionProgressError::TooLarge);
        }
        let vote = SubscriptionPrepareVote {
            subscription_id: mutation.subscription_id,
            request_id: mutation.request_id,
            digest: *blake3::hash(&bytes).as_bytes(),
        };
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let previous = tx
            .get(&self.progress, mutation.subscription_id.as_bytes())?
            .map(|bytes| serde_json::from_slice::<SubscriptionProgressReplicaRow>(&bytes))
            .transpose()?;
        let mut row = match previous {
            Some(row) => row,
            None => SubscriptionProgressReplicaRow {
                subscription_id: mutation.subscription_id,
                feed_id: mutation.feed_id,
                ownership_epoch: mutation.ownership_epoch,
                committed: None,
                prepared: None,
                members: SubscriptionMemberState::default(),
            },
        };
        if row.subscription_id != mutation.subscription_id || row.feed_id != mutation.feed_id {
            return Err(SubscriptionProgressError::Conflict);
        }
        if row.ownership_epoch != mutation.ownership_epoch {
            return Err(SubscriptionProgressError::StaleEpoch);
        }
        if let Some(committed) = &row.committed {
            if committed.request_id == mutation.request_id {
                return if committed == &mutation {
                    Ok(vote)
                } else {
                    Err(SubscriptionProgressError::Conflict)
                };
            }
        }
        if let Some(prepared) = &row.prepared {
            if prepared == &mutation {
                return Ok(vote);
            }
            if prepared.request_id == mutation.request_id {
                return Err(SubscriptionProgressError::Conflict);
            }
            // A residue from an abandoned vote: this replica is behind the
            // quorum's committed trail, so it answers as a lagging witness
            // rather than contradicting the mutation.
            return Err(SubscriptionProgressError::Sequence);
        }
        let prior_sequence = row.committed.as_ref().map_or(0, |prior| prior.sequence);
        if prior_sequence.checked_add(1) != Some(mutation.sequence) {
            return Err(SubscriptionProgressError::Sequence);
        }
        if row.committed.as_ref().map(|prior| prior.cursor.as_str())
            != mutation.expected_cursor.as_deref()
        {
            return Err(SubscriptionProgressError::Conflict);
        }
        if !mutation.lease_ops.is_empty() {
            let mut tracker = SubscriptionLeaseTracker::from_state(
                row.members.clone(),
                SUBSCRIPTION_LEASE_MAX_WORK,
            );
            for op in &mutation.lease_ops {
                tracker
                    .apply_op(op, mutation.tick)
                    .map_err(SubscriptionProgressError::Lease)?;
            }
        }
        row.prepared = Some(mutation);
        let encoded = serde_json::to_vec(&row)?;
        if encoded.len() > 256 * 1024 {
            return Err(SubscriptionProgressError::TooLarge);
        }
        tx.insert(&self.progress, row.subscription_id.as_bytes(), encoded);
        tx.commit()?;
        Ok(vote)
    }

    pub fn commit_with_quorum(
        &self,
        evidence: SubscriptionCommitEvidence,
    ) -> Result<SubscriptionProgressMutation, SubscriptionProgressError> {
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let bytes = tx
            .get(&self.progress, evidence.subscription_id.as_bytes())?
            .ok_or(SubscriptionProgressError::Conflict)?;
        let mut row: SubscriptionProgressReplicaRow = serde_json::from_slice(&bytes)?;
        if row.subscription_id != evidence.subscription_id {
            return Err(SubscriptionProgressError::Conflict);
        }
        let candidate = row
            .prepared
            .as_ref()
            .filter(|value| value.request_id == evidence.request_id)
            .or_else(|| {
                row.committed
                    .as_ref()
                    .filter(|value| value.request_id == evidence.request_id)
            })
            .ok_or(SubscriptionProgressError::Conflict)?;
        let digest = *blake3::hash(&serde_json::to_vec(candidate)?).as_bytes();
        if evidence.request_id != candidate.request_id
            || evidence.votes[0].0 == evidence.votes[1].0
            || evidence.votes.iter().any(|(_, value)| *value != digest)
        {
            return Err(SubscriptionProgressError::NoQuorum);
        }
        if row
            .prepared
            .as_ref()
            .is_none_or(|value| value.request_id != evidence.request_id)
        {
            return row.committed.ok_or(SubscriptionProgressError::Conflict);
        }
        let committed = row
            .prepared
            .take()
            .ok_or(SubscriptionProgressError::Conflict)?;
        if !committed.lease_ops.is_empty() {
            let mut tracker = SubscriptionLeaseTracker::from_state(
                row.members.clone(),
                SUBSCRIPTION_LEASE_MAX_WORK,
            );
            for op in &committed.lease_ops {
                tracker
                    .apply_op(op, committed.tick)
                    .map_err(|_| SubscriptionProgressError::Conflict)?;
            }
            row.members = tracker.materialize();
        }
        row.committed = Some(committed.clone());
        tx.insert(
            &self.progress,
            row.subscription_id.as_bytes(),
            serde_json::to_vec(&row)?,
        );
        tx.commit()?;
        Ok(committed)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionPrepareRequest {
    pub owner: crate::active_range::StorageNodeId,
    pub receiver: crate::active_range::StorageNodeId,
    pub mutation: SubscriptionProgressMutation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionCommitRequest {
    pub owner: crate::active_range::StorageNodeId,
    pub receiver: crate::active_range::StorageNodeId,
    pub subscription_id: Uuid,
    pub ownership_epoch: u64,
    pub evidence: SubscriptionCommitEvidence,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionCommittedReadRequest {
    pub owner: crate::active_range::StorageNodeId,
    pub receiver: crate::active_range::StorageNodeId,
    pub subscription_id: Uuid,
    pub ownership_epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionProgressInspection {
    pub committed: Option<SubscriptionProgressMutation>,
    pub prepared: Option<SubscriptionProgressMutation>,
    #[serde(default)]
    pub members: SubscriptionMemberState,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionProgressAdoptRequest {
    pub owner: crate::active_range::StorageNodeId,
    pub receiver: crate::active_range::StorageNodeId,
    pub subscription_id: Uuid,
    pub ownership_epoch: u64,
    pub committed: Option<SubscriptionProgressMutation>,
    #[serde(default)]
    pub members: SubscriptionMemberState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionRecoveryOutcome {
    pub assignment: SubscriptionProgressAssignment,
    pub committed: Option<SubscriptionProgressMutation>,
    pub members: SubscriptionMemberState,
    pub adopted: Vec<crate::active_range::StorageNodeId>,
}

pub struct SubscriptionProgressReplicaService {
    local_node: crate::active_range::StorageNodeId,
    control: std::sync::Arc<crate::control::ControlController>,
    replica: std::sync::Arc<FjallSubscriptionProgressReplica>,
}

impl SubscriptionProgressReplicaService {
    pub fn new(
        local_node: crate::active_range::StorageNodeId,
        control: std::sync::Arc<crate::control::ControlController>,
        replica: std::sync::Arc<FjallSubscriptionProgressReplica>,
    ) -> Self {
        Self {
            local_node,
            control,
            replica,
        }
    }

    async fn check_placement(
        &self,
        subscription_id: Uuid,
        owner: &crate::active_range::StorageNodeId,
        receiver: &crate::active_range::StorageNodeId,
        epoch: u64,
    ) -> Result<SubscriptionProgressAssignment, SubscriptionProgressError> {
        let assignment = self
            .control
            .active_subscription_progress_assignment_by_id(subscription_id)
            .await
            .ok_or(SubscriptionProgressError::InvalidAssignment)?;
        if assignment.owner != *owner
            || epoch != assignment.ownership_epoch
            || *receiver != self.local_node
            || !assignment.replicas.contains(receiver)
        {
            return Err(SubscriptionProgressError::InvalidAssignment);
        }
        Ok(assignment)
    }

    pub async fn prepare(
        &self,
        request: SubscriptionPrepareRequest,
    ) -> Result<SubscriptionReplicaReply<SubscriptionPrepareVote>, SubscriptionProgressError> {
        let assignment = self
            .check_placement(
                request.mutation.subscription_id,
                &request.owner,
                &request.receiver,
                request.mutation.ownership_epoch,
            )
            .await?;
        if self
            .control
            .active_subscription_feed_by_id(request.mutation.subscription_id)
            .await
            != Some(request.mutation.feed_id)
        {
            return Err(SubscriptionProgressError::Conflict);
        }
        let replica = self.replica.clone();
        let result = tokio::task::spawn_blocking(move || replica.prepare(request.mutation))
            .await
            .map_err(|_| SubscriptionProgressError::Unavailable)??;
        Ok(SubscriptionReplicaReply {
            replica: self.local_node.clone(),
            subscription_id: assignment.subscription_id,
            ownership_epoch: assignment.ownership_epoch,
            result,
        })
    }

    pub async fn commit(
        &self,
        request: SubscriptionCommitRequest,
    ) -> Result<SubscriptionReplicaReply<SubscriptionProgressMutation>, SubscriptionProgressError>
    {
        let assignment = self
            .check_placement(
                request.subscription_id,
                &request.owner,
                &request.receiver,
                request.ownership_epoch,
            )
            .await?;
        if request.evidence.subscription_id != request.subscription_id
            || request
                .evidence
                .votes
                .iter()
                .any(|(node, _)| !assignment.replicas.contains(node))
            || !request
                .evidence
                .votes
                .iter()
                .any(|(node, _)| node == &assignment.owner)
        {
            return Err(SubscriptionProgressError::InvalidAssignment);
        }
        let replica = self.replica.clone();
        let result =
            tokio::task::spawn_blocking(move || replica.commit_with_quorum(request.evidence))
                .await
                .map_err(|_| SubscriptionProgressError::Unavailable)??;
        Ok(SubscriptionReplicaReply {
            replica: self.local_node.clone(),
            subscription_id: assignment.subscription_id,
            ownership_epoch: assignment.ownership_epoch,
            result,
        })
    }

    pub async fn committed(
        &self,
        request: SubscriptionCommittedReadRequest,
    ) -> Result<
        SubscriptionReplicaReply<Option<SubscriptionProgressMutation>>,
        SubscriptionProgressError,
    > {
        let assignment = self
            .check_placement(
                request.subscription_id,
                &request.owner,
                &request.receiver,
                request.ownership_epoch,
            )
            .await?;
        let feed_id = self
            .control
            .active_subscription_feed_by_id(request.subscription_id)
            .await
            .ok_or(SubscriptionProgressError::InvalidAssignment)?;
        let replica = self.replica.clone();
        let subscription_id = request.subscription_id;
        let committed =
            tokio::task::spawn_blocking(move || replica.local_committed(subscription_id))
                .await
                .map_err(|_| SubscriptionProgressError::Unavailable)??;
        if let Some(progress) = &committed {
            if progress.subscription_id != subscription_id || progress.feed_id != feed_id {
                return Err(SubscriptionProgressError::Conflict);
            }
            if progress.ownership_epoch != request.ownership_epoch {
                return Err(SubscriptionProgressError::StaleEpoch);
            }
        }
        Ok(SubscriptionReplicaReply {
            replica: self.local_node.clone(),
            subscription_id: assignment.subscription_id,
            ownership_epoch: assignment.ownership_epoch,
            result: committed,
        })
    }

    pub async fn inspect(
        &self,
        request: SubscriptionCommittedReadRequest,
    ) -> Result<SubscriptionReplicaReply<SubscriptionProgressInspection>, SubscriptionProgressError>
    {
        let assignment = self
            .check_placement(
                request.subscription_id,
                &request.owner,
                &request.receiver,
                request.ownership_epoch,
            )
            .await?;
        let replica = self.replica.clone();
        let subscription_id = request.subscription_id;
        let inspection = tokio::task::spawn_blocking(move || replica.local_state(subscription_id))
            .await
            .map_err(|_| SubscriptionProgressError::Unavailable)??;
        Ok(SubscriptionReplicaReply {
            replica: self.local_node.clone(),
            subscription_id: assignment.subscription_id,
            ownership_epoch: assignment.ownership_epoch,
            result: inspection,
        })
    }

    pub async fn adopt(
        &self,
        request: SubscriptionProgressAdoptRequest,
    ) -> Result<
        SubscriptionReplicaReply<Option<SubscriptionProgressMutation>>,
        SubscriptionProgressError,
    > {
        let assignment = self
            .check_placement(
                request.subscription_id,
                &request.owner,
                &request.receiver,
                request.ownership_epoch,
            )
            .await?;
        let feed_id = self
            .control
            .active_subscription_feed_by_id(request.subscription_id)
            .await
            .ok_or(SubscriptionProgressError::InvalidAssignment)?;
        let replica = self.replica.clone();
        let subscription_id = request.subscription_id;
        let ownership_epoch = request.ownership_epoch;
        let committed = request.committed;
        let members = request.members;
        let result = tokio::task::spawn_blocking(move || {
            replica.adopt_recovered(
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
            replica: self.local_node.clone(),
            subscription_id: assignment.subscription_id,
            ownership_epoch: assignment.ownership_epoch,
            result,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscriptionProgressAssignment {
    pub subscription_id: Uuid,
    pub owner: crate::active_range::StorageNodeId,
    pub replicas: crate::active_range::ReplicaSet,
    pub ownership_epoch: u64,
}

impl SubscriptionProgressAssignment {
    pub fn try_new(
        subscription_id: Uuid,
        owner: crate::active_range::StorageNodeId,
        replicas: crate::active_range::ReplicaSet,
        ownership_epoch: u64,
    ) -> Result<Self, SubscriptionProgressError> {
        if ownership_epoch == 0 || !replicas.contains(&owner) {
            return Err(SubscriptionProgressError::InvalidAssignment);
        }
        Ok(Self {
            subscription_id,
            owner,
            replicas,
            ownership_epoch,
        })
    }
}

#[async_trait::async_trait]
pub trait SubscriptionProgressTransport: Send + Sync {
    async fn prepare(
        &self,
        replica: &crate::active_range::StorageNodeId,
        mutation: SubscriptionProgressMutation,
    ) -> Result<SubscriptionReplicaReply<SubscriptionPrepareVote>, SubscriptionProgressError>;

    async fn commit(
        &self,
        replica: &crate::active_range::StorageNodeId,
        evidence: SubscriptionCommitEvidence,
    ) -> Result<SubscriptionReplicaReply<SubscriptionProgressMutation>, SubscriptionProgressError>;

    async fn committed(
        &self,
        replica: &crate::active_range::StorageNodeId,
        subscription_id: Uuid,
        ownership_epoch: u64,
    ) -> Result<
        SubscriptionReplicaReply<Option<SubscriptionProgressMutation>>,
        SubscriptionProgressError,
    >;

    async fn inspect(
        &self,
        replica: &crate::active_range::StorageNodeId,
        subscription_id: Uuid,
        ownership_epoch: u64,
    ) -> Result<SubscriptionReplicaReply<SubscriptionProgressInspection>, SubscriptionProgressError>;

    async fn adopt(
        &self,
        replica: &crate::active_range::StorageNodeId,
        owner: &crate::active_range::StorageNodeId,
        subscription_id: Uuid,
        ownership_epoch: u64,
        committed: Option<SubscriptionProgressMutation>,
        members: SubscriptionMemberState,
    ) -> Result<
        SubscriptionReplicaReply<Option<SubscriptionProgressMutation>>,
        SubscriptionProgressError,
    >;
}

pub struct HttpSubscriptionProgressTransport {
    assignment: SubscriptionProgressAssignment,
    endpoints: crate::internal_plane::InternalEndpoints,
    key: Option<String>,
    client: reqwest::Client,
}

impl HttpSubscriptionProgressTransport {
    pub fn new(
        assignment: SubscriptionProgressAssignment,
        endpoints: impl Into<crate::internal_plane::InternalEndpoints>,
        key: String,
        timeout: Duration,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            assignment,
            endpoints: endpoints.into(),
            key: Some(key),
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(1))
                .timeout(timeout)
                .build()?,
        })
    }

    pub fn new_mtls(
        assignment: SubscriptionProgressAssignment,
        endpoints: impl Into<crate::internal_plane::InternalEndpoints>,
        ca_pem: &[u8],
        identity_pem: &[u8],
        timeout: Duration,
    ) -> Result<Self, SubscriptionProgressError> {
        let endpoints = endpoints.into();
        for (node, endpoint) in endpoints.configured() {
            let url = reqwest::Url::parse(endpoint)
                .map_err(|_| SubscriptionProgressError::InvalidAssignment)?;
            if url.scheme() != "https" || url.host_str() != Some(node.as_str()) {
                return Err(SubscriptionProgressError::InvalidAssignment);
            }
        }
        let ca = reqwest::Certificate::from_pem(ca_pem)
            .map_err(|_| SubscriptionProgressError::InvalidAssignment)?;
        let identity = reqwest::Identity::from_pem(identity_pem)
            .map_err(|_| SubscriptionProgressError::InvalidAssignment)?;
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(1))
            .timeout(timeout)
            .https_only(true)
            .tls_built_in_root_certs(false)
            .add_root_certificate(ca)
            .identity(identity)
            .build()
            .map_err(|_| SubscriptionProgressError::InvalidAssignment)?;
        Ok(Self {
            assignment,
            endpoints,
            key: None,
            client,
        })
    }

    async fn send<R: Serialize, T: serde::de::DeserializeOwned>(
        &self,
        node: &crate::active_range::StorageNodeId,
        path: &str,
        body: &R,
    ) -> Result<T, SubscriptionProgressError> {
        if !self.assignment.replicas.contains(node) {
            return Err(SubscriptionProgressError::InvalidAssignment);
        }
        let endpoint = self
            .endpoints
            .resolve(node)
            .await
            .ok_or(SubscriptionProgressError::Unavailable)?;
        let mut request = self
            .client
            .post(format!("{}{path}", endpoint.trim_end_matches('/')))
            .json(body);
        if let Some(key) = &self.key {
            request = request.header("x-whitewater-control-key", key);
        }
        let mut response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(node = %node, path, %error, "Subscription progress request failed");
                return Err(SubscriptionProgressError::Unavailable);
            }
        };
        if !response.status().is_success() {
            let status = response.status();
            let body = response.bytes().await.unwrap_or_default();
            tracing::warn!(node = %node, path, %status, "Subscription progress replica refused");
            let code = body
                .get(..4096)
                .and_then(|head| serde_json::from_slice::<serde_json::Value>(head).ok())
                .and_then(|value| {
                    value
                        .get("code")
                        .and_then(|code| code.as_str())
                        .map(str::to_owned)
                });
            if let Some(error) = code
                .as_deref()
                .and_then(SubscriptionProgressError::from_code)
            {
                return Err(error);
            }
            return Err(if status == reqwest::StatusCode::CONFLICT {
                SubscriptionProgressError::Conflict
            } else {
                SubscriptionProgressError::Unavailable
            });
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| SubscriptionProgressError::Unavailable)?
        {
            if bytes
                .len()
                .checked_add(chunk.len())
                .is_none_or(|size| size > 512 * 1024)
            {
                return Err(SubscriptionProgressError::TooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| SubscriptionProgressError::Unavailable)
    }
}

#[async_trait::async_trait]
impl SubscriptionProgressTransport for HttpSubscriptionProgressTransport {
    async fn prepare(
        &self,
        replica: &crate::active_range::StorageNodeId,
        mutation: SubscriptionProgressMutation,
    ) -> Result<SubscriptionReplicaReply<SubscriptionPrepareVote>, SubscriptionProgressError> {
        if mutation.subscription_id != self.assignment.subscription_id
            || mutation.ownership_epoch != self.assignment.ownership_epoch
        {
            return Err(SubscriptionProgressError::InvalidAssignment);
        }
        self.send(
            replica,
            "/internal/subscription-progress/prepare",
            &SubscriptionPrepareRequest {
                owner: self.assignment.owner.clone(),
                receiver: replica.clone(),
                mutation,
            },
        )
        .await
    }

    async fn commit(
        &self,
        replica: &crate::active_range::StorageNodeId,
        evidence: SubscriptionCommitEvidence,
    ) -> Result<SubscriptionReplicaReply<SubscriptionProgressMutation>, SubscriptionProgressError>
    {
        if evidence.subscription_id != self.assignment.subscription_id {
            return Err(SubscriptionProgressError::InvalidAssignment);
        }
        self.send(
            replica,
            "/internal/subscription-progress/commit",
            &SubscriptionCommitRequest {
                owner: self.assignment.owner.clone(),
                receiver: replica.clone(),
                subscription_id: evidence.subscription_id,
                ownership_epoch: self.assignment.ownership_epoch,
                evidence,
            },
        )
        .await
    }

    async fn committed(
        &self,
        replica: &crate::active_range::StorageNodeId,
        subscription_id: Uuid,
        ownership_epoch: u64,
    ) -> Result<
        SubscriptionReplicaReply<Option<SubscriptionProgressMutation>>,
        SubscriptionProgressError,
    > {
        if subscription_id != self.assignment.subscription_id
            || ownership_epoch != self.assignment.ownership_epoch
        {
            return Err(SubscriptionProgressError::InvalidAssignment);
        }
        self.send(
            replica,
            "/internal/subscription-progress/committed",
            &SubscriptionCommittedReadRequest {
                owner: self.assignment.owner.clone(),
                receiver: replica.clone(),
                subscription_id,
                ownership_epoch,
            },
        )
        .await
    }

    async fn inspect(
        &self,
        replica: &crate::active_range::StorageNodeId,
        subscription_id: Uuid,
        ownership_epoch: u64,
    ) -> Result<SubscriptionReplicaReply<SubscriptionProgressInspection>, SubscriptionProgressError>
    {
        if subscription_id != self.assignment.subscription_id
            || ownership_epoch != self.assignment.ownership_epoch
        {
            return Err(SubscriptionProgressError::InvalidAssignment);
        }
        self.send(
            replica,
            "/internal/subscription-progress/inspect",
            &SubscriptionCommittedReadRequest {
                owner: self.assignment.owner.clone(),
                receiver: replica.clone(),
                subscription_id,
                ownership_epoch,
            },
        )
        .await
    }

    async fn adopt(
        &self,
        replica: &crate::active_range::StorageNodeId,
        owner: &crate::active_range::StorageNodeId,
        subscription_id: Uuid,
        ownership_epoch: u64,
        committed: Option<SubscriptionProgressMutation>,
        members: SubscriptionMemberState,
    ) -> Result<
        SubscriptionReplicaReply<Option<SubscriptionProgressMutation>>,
        SubscriptionProgressError,
    > {
        if subscription_id != self.assignment.subscription_id
            || committed.as_ref().is_some_and(|mutation| {
                mutation.subscription_id != subscription_id
                    || mutation.ownership_epoch != ownership_epoch
            })
        {
            return Err(SubscriptionProgressError::InvalidAssignment);
        }
        self.send(
            replica,
            "/internal/subscription-progress/adopt",
            &SubscriptionProgressAdoptRequest {
                owner: owner.clone(),
                receiver: replica.clone(),
                subscription_id,
                ownership_epoch,
                committed,
                members,
            },
        )
        .await
    }
}

/// Executes Subscription progress placement commands through the
/// configured placement authority: the replicated Control Plane when one is
/// configured, or the local catalog for `local_prototype` Nodes. Recovery and
/// replica movement must mutate placement through this authority so the new
/// ownership epoch is replicated to every Node's catalog instead of landing
/// only on the Node that happened to serve the request.
#[async_trait::async_trait]
pub trait SubscriptionPlacementAuthority: Send + Sync {
    async fn execute_placement_commands(
        &self,
        commands: Vec<crate::control::Command>,
    ) -> Result<(), SubscriptionProgressError>;

    async fn progress_assignment(
        &self,
        subscription_id: Uuid,
    ) -> Option<SubscriptionProgressAssignment>;
}

#[async_trait::async_trait]
impl SubscriptionPlacementAuthority for crate::control::ControlController {
    async fn execute_placement_commands(
        &self,
        commands: Vec<crate::control::Command>,
    ) -> Result<(), SubscriptionProgressError> {
        self.execute_commands(commands)
            .await
            .map(|_| ())
            .map_err(|_| SubscriptionProgressError::Unavailable)
    }

    async fn progress_assignment(
        &self,
        subscription_id: Uuid,
    ) -> Option<SubscriptionProgressAssignment> {
        self.active_subscription_progress_assignment_by_id(subscription_id)
            .await
    }
}

#[async_trait::async_trait]
impl SubscriptionPlacementAuthority for crate::control_plane::ControlPlane {
    async fn execute_placement_commands(
        &self,
        commands: Vec<crate::control::Command>,
    ) -> Result<(), SubscriptionProgressError> {
        self.execute_commands(commands)
            .await
            .map(|_| ())
            .map_err(|_| SubscriptionProgressError::Unavailable)
    }

    async fn progress_assignment(
        &self,
        subscription_id: Uuid,
    ) -> Option<SubscriptionProgressAssignment> {
        self.controller()
            .active_subscription_progress_assignment_by_id(subscription_id)
            .await
    }
}

pub struct SubscriptionProgressCoordinator {
    assignment: SubscriptionProgressAssignment,
    transport: std::sync::Arc<dyn SubscriptionProgressTransport>,
}

impl SubscriptionProgressCoordinator {
    pub async fn for_subscription(
        control: &crate::control::ControlController,
        subscription_id: Uuid,
        transport: std::sync::Arc<dyn SubscriptionProgressTransport>,
    ) -> Result<Self, SubscriptionProgressError> {
        let assignment = control
            .active_subscription_progress_assignment_by_id(subscription_id)
            .await
            .ok_or(SubscriptionProgressError::InvalidAssignment)?;
        Ok(Self::new(assignment, transport))
    }

    pub fn new(
        assignment: SubscriptionProgressAssignment,
        transport: std::sync::Arc<dyn SubscriptionProgressTransport>,
    ) -> Self {
        Self {
            assignment,
            transport,
        }
    }

    pub async fn read_committed(
        &self,
    ) -> Result<Option<SubscriptionProgressMutation>, SubscriptionProgressError> {
        let mut observed: Option<SubscriptionProgressMutation> = None;
        let mut votes = 0;
        let mut lagged = false;
        let replies = futures_util::future::join_all(self.assignment.replicas.iter().map(|node| {
            self.transport.committed(
                node,
                self.assignment.subscription_id,
                self.assignment.ownership_epoch,
            )
        }))
        .await;
        for (node, reply) in self.assignment.replicas.iter().zip(replies) {
            match reply {
                Ok(reply) => {
                    if reply.replica != *node
                        || reply.subscription_id != self.assignment.subscription_id
                        || reply.ownership_epoch != self.assignment.ownership_epoch
                    {
                        return Err(SubscriptionProgressError::InvalidAssignment);
                    }
                    let progress = reply.result;
                    if let Some(mutation) = progress.as_ref() {
                        if mutation.subscription_id != self.assignment.subscription_id {
                            return Err(SubscriptionProgressError::Conflict);
                        }
                        if mutation.ownership_epoch != self.assignment.ownership_epoch {
                            if mutation.ownership_epoch > self.assignment.ownership_epoch {
                                // The replica knows a newer placement; this
                                // coordinator's assignment is stale.
                                return Err(SubscriptionProgressError::StaleEpoch);
                            }
                            // The replica still carries the prior epoch's row;
                            // it is a lagging witness, not a contradiction.
                            lagged = true;
                            continue;
                        }
                    }
                    votes += 1;
                    if let Some(candidate) = progress {
                        match &observed {
                            Some(current) if !same_progress_identity(current, &candidate) => {
                                match candidate.sequence.cmp(&current.sequence) {
                                    std::cmp::Ordering::Greater => observed = Some(candidate),
                                    std::cmp::Ordering::Equal => {
                                        return Err(SubscriptionProgressError::Conflict)
                                    }
                                    std::cmp::Ordering::Less => {}
                                }
                            }
                            Some(_) => {}
                            None => observed = Some(candidate),
                        }
                    }
                }
                Err(error) if lagging_witness(&error) => lagged = true,
                Err(error) => return Err(error),
            }
        }
        if votes < 2 || (observed.is_none() && (votes < 3 || lagged)) {
            return Err(SubscriptionProgressError::NoQuorum);
        }
        Ok(observed)
    }

    pub async fn reconcile_retry(
        &self,
        mutation: SubscriptionProgressMutation,
    ) -> Result<SubscriptionProgressMutation, SubscriptionProgressError> {
        if mutation.subscription_id != self.assignment.subscription_id
            || mutation.ownership_epoch != self.assignment.ownership_epoch
        {
            return Err(SubscriptionProgressError::InvalidAssignment);
        }
        let mut reachable = 0;
        let mut owner_seen = false;
        let mut owner_committed = false;
        let mut matching = 0;
        let replies = futures_util::future::join_all(self.assignment.replicas.iter().map(|node| {
            self.transport
                .committed(node, mutation.subscription_id, mutation.ownership_epoch)
        }))
        .await;
        for (node, reply) in self.assignment.replicas.iter().zip(replies) {
            match reply {
                Ok(reply) => {
                    if reply.replica != *node
                        || reply.subscription_id != mutation.subscription_id
                        || reply.ownership_epoch != mutation.ownership_epoch
                    {
                        return Err(SubscriptionProgressError::InvalidAssignment);
                    }
                    reachable += 1;
                    if node == &self.assignment.owner {
                        owner_seen = true;
                    }
                    match reply.result {
                        Some(current) if current == mutation => {
                            matching += 1;
                            if node == &self.assignment.owner {
                                owner_committed = true;
                            }
                        }
                        // A replica holding an older sequence or no committed
                        // row is a lagging witness; quorum evidence decides,
                        // and adoption brings the replica forward.
                        Some(current) if current.sequence < mutation.sequence => {}
                        None => {}
                        _ => return Err(SubscriptionProgressError::Conflict),
                    }
                }
                Err(error) if lagging_witness(&error) => {}
                Err(error) => return Err(error),
            }
        }
        if reachable < 2 || !owner_seen {
            return Err(SubscriptionProgressError::NoQuorum);
        }
        if owner_committed && matching >= 2 {
            match self.read_committed().await {
                Ok(Some(committed)) if committed == mutation => return Ok(mutation),
                Err(SubscriptionProgressError::Conflict) => {}
                Err(error) => return Err(error),
                _ => return Err(SubscriptionProgressError::AmbiguousCommit),
            }
        }
        self.apply(mutation.clone()).await?;
        match self.read_committed().await {
            Ok(Some(committed)) if committed == mutation => Ok(mutation),
            Err(SubscriptionProgressError::Conflict) | Ok(_) => {
                Err(SubscriptionProgressError::AmbiguousCommit)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn apply(
        &self,
        mutation: SubscriptionProgressMutation,
    ) -> Result<Vec<crate::active_range::StorageNodeId>, SubscriptionProgressError> {
        if mutation.subscription_id != self.assignment.subscription_id
            || mutation.ownership_epoch != self.assignment.ownership_epoch
        {
            return Err(SubscriptionProgressError::InvalidAssignment);
        }
        let digest = *blake3::hash(&serde_json::to_vec(&mutation)?).as_bytes();
        let mut prepared = Vec::with_capacity(3);
        let mut first_refusal: Option<SubscriptionProgressError> = None;
        let replies = futures_util::future::join_all(
            self.assignment
                .replicas
                .iter()
                .map(|node| self.transport.prepare(node, mutation.clone())),
        )
        .await;
        for (node, reply) in self.assignment.replicas.iter().zip(replies) {
            match reply {
                Ok(reply) => {
                    if reply.replica != *node
                        || reply.subscription_id != self.assignment.subscription_id
                        || reply.ownership_epoch != self.assignment.ownership_epoch
                    {
                        return Err(SubscriptionProgressError::InvalidAssignment);
                    }
                    let vote = reply.result;
                    if vote.subscription_id != mutation.subscription_id
                        || vote.request_id != mutation.request_id
                        || vote.digest != digest
                    {
                        return Err(SubscriptionProgressError::Conflict);
                    }
                    prepared.push((node.clone(), vote.digest));
                }
                Err(error) if lagging_prepare(&error) => {
                    if !matches!(error, SubscriptionProgressError::Unavailable)
                        && first_refusal.is_none()
                    {
                        first_refusal = Some(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        if prepared.len() < 2
            || !prepared
                .iter()
                .any(|(node, _)| node == &self.assignment.owner)
        {
            // Quorum failed: surface the most informative replica refusal
            // (a lease fence or stale epoch) when one exists rather than a
            // bare quorum shortage.
            return Err(first_refusal.unwrap_or(SubscriptionProgressError::NoQuorum));
        }
        let other = prepared
            .iter()
            .find(|(node, _)| node != &self.assignment.owner)
            .ok_or(SubscriptionProgressError::NoQuorum)?;
        let evidence = SubscriptionCommitEvidence {
            subscription_id: mutation.subscription_id,
            request_id: mutation.request_id,
            votes: [
                (self.assignment.owner.clone(), digest),
                (other.0.clone(), other.1),
            ],
        };
        let mut committed = Vec::with_capacity(3);
        let replies = futures_util::future::join_all(
            prepared
                .iter()
                .map(|(node, _)| self.transport.commit(node, evidence.clone())),
        )
        .await;
        for ((node, _), reply) in prepared.iter().zip(replies) {
            match reply {
                Ok(reply)
                    if reply.replica != *node
                        || reply.subscription_id != self.assignment.subscription_id
                        || reply.ownership_epoch != self.assignment.ownership_epoch =>
                {
                    return Err(SubscriptionProgressError::InvalidAssignment);
                }
                Ok(reply) if reply.result == mutation => committed.push(node.clone()),
                Ok(_) => return Err(SubscriptionProgressError::Conflict),
                Err(error) if lagging_witness(&error) => {}
                Err(error) => return Err(error),
            }
        }
        if committed.len() < 2 || !committed.contains(&self.assignment.owner) {
            return Err(SubscriptionProgressError::AmbiguousCommit);
        }
        Ok(committed)
    }

    pub fn assignment(&self) -> &SubscriptionProgressAssignment {
        &self.assignment
    }

    pub(crate) async fn inspect_evidence(
        &self,
    ) -> Result<
        Vec<(
            crate::active_range::StorageNodeId,
            SubscriptionProgressInspection,
        )>,
        SubscriptionProgressError,
    > {
        let mut evidence = Vec::with_capacity(3);
        let replies = futures_util::future::join_all(self.assignment.replicas.iter().map(|node| {
            self.transport.inspect(
                node,
                self.assignment.subscription_id,
                self.assignment.ownership_epoch,
            )
        }))
        .await;
        for (node, reply) in self.assignment.replicas.iter().zip(replies) {
            match reply {
                Ok(reply) => {
                    if reply.replica != *node
                        || reply.subscription_id != self.assignment.subscription_id
                        || reply.ownership_epoch != self.assignment.ownership_epoch
                    {
                        return Err(SubscriptionProgressError::InvalidAssignment);
                    }
                    for mutation in [
                        reply.result.committed.as_ref(),
                        reply.result.prepared.as_ref(),
                    ]
                    .into_iter()
                    .flatten()
                    {
                        if mutation.subscription_id != self.assignment.subscription_id {
                            return Err(SubscriptionProgressError::Conflict);
                        }
                    }
                    evidence.push((node.clone(), reply.result));
                }
                Err(error) if lagging_witness(&error) => {}
                Err(error) => return Err(error),
            }
        }
        if evidence.len() < 2 {
            return Err(SubscriptionProgressError::NoQuorum);
        }
        Ok(evidence)
    }

    fn recovered_committed(
        evidence: &[(
            crate::active_range::StorageNodeId,
            SubscriptionProgressInspection,
        )],
    ) -> Result<
        (
            Option<SubscriptionProgressMutation>,
            SubscriptionMemberState,
        ),
        SubscriptionProgressError,
    > {
        let mut best: Option<(&SubscriptionProgressMutation, &SubscriptionMemberState)> = None;
        for (_, inspection) in evidence {
            let Some(candidate) = &inspection.committed else {
                continue;
            };
            match best {
                None => best = Some((candidate, &inspection.members)),
                Some((current, members)) if same_progress_identity(current, candidate) => {
                    if *members != inspection.members {
                        return Err(SubscriptionProgressError::Conflict);
                    }
                }
                Some((current, _)) => match candidate.sequence.cmp(&current.sequence) {
                    std::cmp::Ordering::Greater => {
                        best = Some((candidate, &inspection.members));
                    }
                    std::cmp::Ordering::Equal => {
                        return Err(SubscriptionProgressError::Conflict);
                    }
                    std::cmp::Ordering::Less => {}
                },
            }
        }
        Ok(best.map_or_else(
            || (None, SubscriptionMemberState::default()),
            |(mutation, members)| (Some(mutation.clone()), members.clone()),
        ))
    }

    fn recovery_owner(
        &self,
        evidence: &[(
            crate::active_range::StorageNodeId,
            SubscriptionProgressInspection,
        )],
        recovered: Option<&SubscriptionProgressMutation>,
    ) -> Result<crate::active_range::StorageNodeId, SubscriptionProgressError> {
        let reachable = |node: &crate::active_range::StorageNodeId| {
            evidence.iter().any(|(member, _)| member == node)
        };
        let holds = |node: &crate::active_range::StorageNodeId| {
            evidence.iter().any(|(member, inspection)| {
                member == node
                    && match (inspection.committed.as_ref(), recovered) {
                        (Some(committed), Some(recovered)) => {
                            same_progress_identity(committed, recovered)
                        }
                        (None, None) => true,
                        _ => false,
                    }
            })
        };
        self.assignment
            .replicas
            .iter()
            .find(|node| reachable(node) && holds(node))
            .or_else(|| self.assignment.replicas.iter().find(|node| reachable(node)))
            .cloned()
            .ok_or(SubscriptionProgressError::NoQuorum)
    }

    async fn adopt_all(
        &self,
        members: &[crate::active_range::StorageNodeId],
        owner: &crate::active_range::StorageNodeId,
        ownership_epoch: u64,
        committed: Option<SubscriptionProgressMutation>,
        member_state: &SubscriptionMemberState,
    ) -> Result<Vec<crate::active_range::StorageNodeId>, SubscriptionProgressError> {
        // A replica whose catalog has not yet applied a just-committed
        // placement change rejects the new epoch; give the replicated catalog
        // a bounded window to converge before counting the replica as
        // unavailable. Members are adopted in parallel.
        let results = futures_util::future::join_all(members.iter().map(|node| async {
            let mut attempts = 0u8;
            loop {
                match self
                    .transport
                    .adopt(
                        node,
                        owner,
                        self.assignment.subscription_id,
                        ownership_epoch,
                        committed.clone(),
                        member_state.clone(),
                    )
                    .await
                {
                    Ok(reply) => {
                        if reply.replica != *node
                            || reply.subscription_id != self.assignment.subscription_id
                            || reply.ownership_epoch != ownership_epoch
                        {
                            break Some(Err(SubscriptionProgressError::InvalidAssignment));
                        }
                        if reply.result != committed {
                            break Some(Err(SubscriptionProgressError::Conflict));
                        }
                        break Some(Ok(()));
                    }
                    Err(SubscriptionProgressError::Unavailable) => break None,
                    Err(error) if lagging_witness(&error) => {
                        attempts += 1;
                        if attempts >= 5 {
                            break None;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                    }
                    Err(error) => break Some(Err(error)),
                }
            }
        }))
        .await;
        let mut adopted = Vec::with_capacity(3);
        for (node, result) in members.iter().zip(results) {
            match result {
                Some(Ok(())) => adopted.push(node.clone()),
                Some(Err(error)) => return Err(error),
                None => {}
            }
        }
        if adopted.len() < 2 || !adopted.contains(owner) {
            return Err(SubscriptionProgressError::NoQuorum);
        }
        Ok(adopted)
    }

    pub async fn owner_available(&self) -> bool {
        self.transport
            .inspect(
                &self.assignment.owner,
                self.assignment.subscription_id,
                self.assignment.ownership_epoch,
            )
            .await
            .is_ok()
    }

    pub async fn recover_lost_owner<A: SubscriptionPlacementAuthority + ?Sized>(
        &self,
        control: &A,
    ) -> Result<SubscriptionRecoveryOutcome, SubscriptionProgressError> {
        let next_epoch = self
            .assignment
            .ownership_epoch
            .checked_add(1)
            .ok_or(SubscriptionProgressError::Sequence)?;
        let evidence = self.inspect_evidence().await?;
        let (recovered, member_state) = Self::recovered_committed(&evidence)?;
        let proposed_owner = self.recovery_owner(&evidence, recovered.as_ref())?;
        let (owner, ownership_epoch) = match control
            .execute_placement_commands(vec![
                crate::control::Command::RecoverSubscriptionProgressOwner {
                    subscription_id: self.assignment.subscription_id,
                    expected_ownership_epoch: self.assignment.ownership_epoch,
                    new_owner: proposed_owner.clone(),
                },
            ])
            .await
        {
            Ok(_) => (proposed_owner, next_epoch),
            Err(_) => {
                let current = control
                    .progress_assignment(self.assignment.subscription_id)
                    .await
                    .ok_or(SubscriptionProgressError::InvalidAssignment)?;
                if current.ownership_epoch <= self.assignment.ownership_epoch {
                    return Err(SubscriptionProgressError::Unavailable);
                }
                (current.owner.clone(), current.ownership_epoch)
            }
        };
        let committed = recovered.map(|mut mutation| {
            mutation.ownership_epoch = ownership_epoch;
            mutation
        });
        let adopted = self
            .adopt_all(
                self.assignment.replicas.as_array(),
                &owner,
                ownership_epoch,
                committed.clone(),
                &member_state,
            )
            .await?;
        Ok(SubscriptionRecoveryOutcome {
            assignment: SubscriptionProgressAssignment {
                subscription_id: self.assignment.subscription_id,
                owner,
                replicas: self.assignment.replicas.clone(),
                ownership_epoch,
            },
            committed,
            members: member_state,
            adopted,
        })
    }

    pub async fn move_replica<A: SubscriptionPlacementAuthority + ?Sized>(
        &self,
        control: &A,
        replaced: &crate::active_range::StorageNodeId,
        replacement: &crate::active_range::StorageNodeId,
    ) -> Result<SubscriptionRecoveryOutcome, SubscriptionProgressError> {
        if !self.assignment.replicas.contains(replaced)
            || self.assignment.replicas.contains(replacement)
            || *replaced == self.assignment.owner
        {
            return Err(SubscriptionProgressError::InvalidAssignment);
        }
        let next_epoch = self
            .assignment
            .ownership_epoch
            .checked_add(1)
            .ok_or(SubscriptionProgressError::Sequence)?;
        let mut swapped = self.assignment.replicas.as_array().clone();
        for slot in swapped.iter_mut() {
            if *slot == *replaced {
                *slot = replacement.clone();
            }
        }
        let new_replicas = crate::active_range::ReplicaSet::try_new(swapped)
            .map_err(|_| SubscriptionProgressError::InvalidAssignment)?;
        let evidence = self.inspect_evidence().await?;
        let (recovered, member_state) = Self::recovered_committed(&evidence)?;
        let (replicas, owner, ownership_epoch) = match control
            .execute_placement_commands(vec![
                crate::control::Command::MoveSubscriptionProgressReplica {
                    subscription_id: self.assignment.subscription_id,
                    expected_ownership_epoch: self.assignment.ownership_epoch,
                    replaced: replaced.clone(),
                    replacement: replacement.clone(),
                },
            ])
            .await
        {
            Ok(_) => (new_replicas, self.assignment.owner.clone(), next_epoch),
            Err(_) => {
                let current = control
                    .progress_assignment(self.assignment.subscription_id)
                    .await
                    .ok_or(SubscriptionProgressError::InvalidAssignment)?;
                if current.ownership_epoch <= self.assignment.ownership_epoch {
                    return Err(SubscriptionProgressError::Unavailable);
                }
                (
                    current.replicas.clone(),
                    current.owner,
                    current.ownership_epoch,
                )
            }
        };
        let committed = recovered.map(|mut mutation| {
            mutation.ownership_epoch = ownership_epoch;
            mutation
        });
        let adopted = self
            .adopt_all(
                replicas.as_array(),
                &owner,
                ownership_epoch,
                committed.clone(),
                &member_state,
            )
            .await?;
        Ok(SubscriptionRecoveryOutcome {
            assignment: SubscriptionProgressAssignment {
                subscription_id: self.assignment.subscription_id,
                owner,
                replicas,
                ownership_epoch,
            },
            committed,
            members: member_state,
            adopted,
        })
    }

    pub async fn synchronize_placement(
        &self,
    ) -> Result<SubscriptionRecoveryOutcome, SubscriptionProgressError> {
        let evidence = self.inspect_evidence().await?;
        let (recovered, member_state) = Self::recovered_committed(&evidence)?;
        let committed = recovered.map(|mut mutation| {
            mutation.ownership_epoch = self.assignment.ownership_epoch;
            mutation
        });
        let adopted = self
            .adopt_all(
                self.assignment.replicas.as_array(),
                &self.assignment.owner.clone(),
                self.assignment.ownership_epoch,
                committed.clone(),
                &member_state,
            )
            .await?;
        Ok(SubscriptionRecoveryOutcome {
            assignment: self.assignment.clone(),
            committed,
            members: member_state,
            adopted,
        })
    }

    pub async fn member_state(&self) -> Result<SubscriptionMemberState, SubscriptionProgressError> {
        let evidence = self.inspect_evidence().await?;
        let (_, members) = Self::recovered_committed(&evidence)?;
        Ok(members)
    }

    pub async fn can_member_ack(
        &self,
        grant: &SubscriptionWorkLease,
        now_tick: u64,
    ) -> Result<bool, SubscriptionProgressError> {
        let members = self.member_state().await?;
        let tracker = SubscriptionLeaseTracker::from_state(members, SUBSCRIPTION_LEASE_MAX_WORK);
        Ok(tracker.can_ack(grant, now_tick))
    }

    pub async fn apply_member_ops(
        &self,
        request_id: Uuid,
        tick: u64,
        lease_ops: Vec<SubscriptionLeaseOp>,
    ) -> Result<SubscriptionProgressMutation, SubscriptionProgressError> {
        let current = self
            .read_committed()
            .await?
            .ok_or(SubscriptionProgressError::Conflict)?;
        let mut mutation = current.clone();
        mutation.sequence = current
            .sequence
            .checked_add(1)
            .ok_or(SubscriptionProgressError::Sequence)?;
        mutation.request_id = request_id;
        mutation.expected_cursor = Some(current.cursor.clone());
        mutation.tick = tick;
        mutation.lease_ops = lease_ops;
        self.apply(mutation.clone()).await?;
        Ok(mutation)
    }

    pub async fn acknowledge(
        &self,
        grant: &SubscriptionWorkLease,
        tick: u64,
        request_id: Uuid,
        cursor: String,
        positions: BTreeMap<RangeId, String>,
    ) -> Result<SubscriptionProgressMutation, SubscriptionProgressError> {
        let current = self
            .read_committed()
            .await?
            .ok_or(SubscriptionProgressError::Conflict)?;
        let mutation = SubscriptionProgressMutation {
            subscription_id: current.subscription_id,
            feed_id: current.feed_id,
            ownership_epoch: current.ownership_epoch,
            sequence: current
                .sequence
                .checked_add(1)
                .ok_or(SubscriptionProgressError::Sequence)?,
            request_id,
            expected_cursor: Some(current.cursor.clone()),
            cursor,
            positions,
            tick,
            lease_ops: vec![SubscriptionLeaseOp::Release {
                work_id: grant.work_id,
                member_id: grant.member_id,
                member_epoch: grant.member_epoch,
                lease_epoch: grant.lease_epoch,
            }],
        };
        self.apply(mutation.clone()).await?;
        Ok(mutation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn subscription_replica_service_fences_placement_before_mutating_fjall() {
        use crate::{
            active_range::StorageNodeId,
            control::ControlController,
            storage::{FileLogStore, LogStore},
        };

        let directory = tempfile::TempDir::new().unwrap();
        let nodes = ["storage-a", "storage-b", "storage-c"]
            .map(|value| StorageNodeId::try_new(value).unwrap());
        let store: std::sync::Arc<dyn LogStore> =
            std::sync::Arc::new(FileLogStore::open(directory.path().join("feed-data")).unwrap());
        let control = std::sync::Arc::new(
            ControlController::open_with_storage_nodes(
                directory.path().join("catalog.json"),
                store,
                nodes.to_vec(),
            )
            .unwrap(),
        );
        control.execute("CREATE SPACE orders; CREATE FEED orders.events; CREATE SUBSCRIPTION orders.billing FROM orders.events;").await.unwrap();
        let subscription = control
            .active_subscription_by_name("orders.billing")
            .await
            .unwrap();
        let assignment = control
            .active_subscription_progress_assignment_by_id(subscription.subscription_id)
            .await
            .unwrap();
        let follower = assignment
            .replicas
            .iter()
            .find(|node| *node != &assignment.owner)
            .unwrap()
            .clone();
        let path = directory.path().join("subscription-progress");
        let replica = std::sync::Arc::new(FjallSubscriptionProgressReplica::open(&path).unwrap());
        let service = SubscriptionProgressReplicaService::new(
            assignment.owner.clone(),
            control,
            replica.clone(),
        );
        let mutation = SubscriptionProgressMutation {
            subscription_id: subscription.subscription_id,
            feed_id: subscription.feed_id,
            ownership_epoch: 1,
            sequence: 1,
            request_id: Uuid::from_u128(700),
            expected_cursor: None,
            cursor: "rf1_event".to_owned(),
            positions: BTreeMap::from([(
                RangeId::from_uuid(Uuid::from_u128(701)),
                "event".to_owned(),
            )]),

            tick: 0,
            lease_ops: Vec::new(),
        };
        let valid = SubscriptionPrepareRequest {
            owner: assignment.owner.clone(),
            receiver: assignment.owner.clone(),
            mutation: mutation.clone(),
        };
        let mut wrong_receiver = valid.clone();
        wrong_receiver.receiver = follower.clone();
        assert!(matches!(
            service.prepare(wrong_receiver).await,
            Err(SubscriptionProgressError::InvalidAssignment)
        ));
        let mut wrong_epoch = valid.clone();
        wrong_epoch.mutation.ownership_epoch = 2;
        assert!(matches!(
            service.prepare(wrong_epoch).await,
            Err(SubscriptionProgressError::InvalidAssignment)
        ));
        assert!(replica
            .local_committed(subscription.subscription_id)
            .unwrap()
            .is_none());
        let read = SubscriptionCommittedReadRequest {
            owner: assignment.owner.clone(),
            receiver: assignment.owner.clone(),
            subscription_id: subscription.subscription_id,
            ownership_epoch: 1,
        };
        let mut wrong_read = read.clone();
        wrong_read.receiver = follower.clone();
        assert!(matches!(
            service.committed(wrong_read).await,
            Err(SubscriptionProgressError::InvalidAssignment)
        ));
        let mut wrong_feed = valid.clone();
        wrong_feed.mutation.feed_id = Uuid::from_u128(702);
        assert!(matches!(
            service.prepare(wrong_feed).await,
            Err(SubscriptionProgressError::Conflict)
        ));
        let vote = service.prepare(valid).await.unwrap();
        assert_eq!(vote.replica, assignment.owner);
        assert!(service
            .committed(read.clone())
            .await
            .unwrap()
            .result
            .is_none());
        let invalid = SubscriptionCommitRequest {
            owner: assignment.owner.clone(),
            receiver: assignment.owner.clone(),
            subscription_id: subscription.subscription_id,
            ownership_epoch: 1,
            evidence: SubscriptionCommitEvidence {
                subscription_id: subscription.subscription_id,
                request_id: mutation.request_id,
                votes: [
                    (assignment.owner.clone(), vote.result.digest),
                    (
                        StorageNodeId::try_new("foreign-node").unwrap(),
                        vote.result.digest,
                    ),
                ],
            },
        };
        assert!(matches!(
            service.commit(invalid.clone()).await,
            Err(SubscriptionProgressError::InvalidAssignment)
        ));
        assert!(replica
            .local_committed(subscription.subscription_id)
            .unwrap()
            .is_none());
        let mut accepted = invalid;
        accepted.evidence.votes[1].0 = follower;
        assert_eq!(service.commit(accepted).await.unwrap().result, mutation);
        assert_eq!(
            service.committed(read).await.unwrap().result,
            Some(mutation.clone())
        );
        drop(service);
        drop(replica);
        assert_eq!(
            FjallSubscriptionProgressReplica::open(path)
                .unwrap()
                .local_committed(subscription.subscription_id)
                .unwrap(),
            Some(mutation)
        );
    }

    #[test]
    fn subscription_progress_never_exposes_prepared_state_and_requires_distinct_votes() {
        use crate::active_range::StorageNodeId;

        let a = tempfile::TempDir::new().unwrap();
        let b = tempfile::TempDir::new().unwrap();
        let c = tempfile::TempDir::new().unwrap();
        let first = FjallSubscriptionProgressReplica::open(a.path()).unwrap();
        let second = FjallSubscriptionProgressReplica::open(b.path()).unwrap();
        let third = FjallSubscriptionProgressReplica::open(c.path()).unwrap();
        let subscription_id = Uuid::from_u128(1);
        let mutation = SubscriptionProgressMutation {
            subscription_id,
            feed_id: Uuid::from_u128(2),
            ownership_epoch: 1,
            sequence: 1,
            request_id: Uuid::from_u128(3),
            expected_cursor: None,
            cursor: "rf1_one".to_owned(),
            positions: BTreeMap::from([(
                RangeId::from_uuid(Uuid::from_u128(4)),
                "record-one".to_owned(),
            )]),

            tick: 0,
            lease_ops: Vec::new(),
        };
        let first_vote = first.prepare(mutation.clone()).unwrap();
        let second_vote = second.prepare(mutation.clone()).unwrap();
        assert_eq!(first_vote.digest, second_vote.digest);
        assert_eq!(
            first.prepare(mutation.clone()).unwrap().digest,
            first_vote.digest
        );
        assert!(first.local_committed(subscription_id).unwrap().is_none());
        assert!(second.local_committed(subscription_id).unwrap().is_none());
        assert!(third.local_committed(subscription_id).unwrap().is_none());
        let node_a = StorageNodeId::try_new("storage-a").unwrap();
        let node_b = StorageNodeId::try_new("storage-b").unwrap();
        assert!(matches!(
            first.commit_with_quorum(SubscriptionCommitEvidence {
                subscription_id,
                request_id: mutation.request_id,
                votes: [
                    (node_a.clone(), first_vote.digest),
                    (node_a.clone(), second_vote.digest)
                ],
            }),
            Err(SubscriptionProgressError::NoQuorum)
        ));
        assert!(first.local_committed(subscription_id).unwrap().is_none());
        let proof = || SubscriptionCommitEvidence {
            subscription_id,
            request_id: mutation.request_id,
            votes: [
                (node_a.clone(), first_vote.digest),
                (node_b.clone(), second_vote.digest),
            ],
        };
        assert_eq!(first.commit_with_quorum(proof()).unwrap(), mutation);
        assert_eq!(first.commit_with_quorum(proof()).unwrap(), mutation);
        assert_eq!(second.commit_with_quorum(proof()).unwrap(), mutation);
        assert!(third.local_committed(subscription_id).unwrap().is_none());
        drop(first);
        let reopened = FjallSubscriptionProgressReplica::open(a.path()).unwrap();
        assert_eq!(
            reopened.local_committed(subscription_id).unwrap(),
            Some(mutation.clone())
        );
        assert_eq!(
            reopened.prepare(mutation.clone()).unwrap().digest,
            first_vote.digest
        );
        let mut next = mutation.clone();
        next.sequence = 2;
        next.request_id = Uuid::from_u128(5);
        next.expected_cursor = Some(mutation.cursor.clone());
        next.cursor = "rf1_two".to_owned();
        reopened.prepare(next.clone()).unwrap();
        assert_eq!(reopened.commit_with_quorum(proof()).unwrap(), mutation);
        assert_eq!(
            reopened.local_committed(subscription_id).unwrap(),
            Some(mutation.clone())
        );
        let mut conflicting = next.clone();
        conflicting.request_id = Uuid::from_u128(6);
        assert!(matches!(
            reopened.prepare(conflicting),
            Err(SubscriptionProgressError::Sequence)
        ));
        let mut conflicting_request = next.clone();
        conflicting_request.cursor = "rf1_fork".to_owned();
        assert!(matches!(
            reopened.prepare(conflicting_request),
            Err(SubscriptionProgressError::Conflict)
        ));
        let mut future_epoch = next;
        future_epoch.ownership_epoch = 2;
        assert!(matches!(
            reopened.prepare(future_epoch),
            Err(SubscriptionProgressError::StaleEpoch)
        ));
    }

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
    fn subscription_work_leases_fence_stale_members_and_limit_capacity() {
        let mut leases = SubscriptionLeaseTracker::new(2);
        let first_member = Uuid::from_u128(1);
        let second_member = Uuid::from_u128(2);
        let first_work = Uuid::from_u128(3);
        let second_work = Uuid::from_u128(4);
        assert_eq!(
            leases
                .claim(first_work, first_member, 1, 100, 20, 1)
                .unwrap_err(),
            SubscriptionLeaseError::StaleLease
        );
        leases.fence_member(first_member, 1).unwrap();
        leases.fence_member(second_member, 1).unwrap();
        let first = leases
            .claim(first_work, first_member, 1, 100, 20, 1)
            .unwrap();
        assert_eq!(
            leases
                .claim(first_work, first_member, 1, 100, 20, 1)
                .unwrap(),
            first
        );
        assert_eq!(
            leases
                .claim(first_work, second_member, 1, 101, 20, 1)
                .unwrap_err(),
            SubscriptionLeaseError::Busy
        );
        assert_eq!(
            leases
                .claim(second_work, first_member, 1, 101, 20, 1)
                .unwrap_err(),
            SubscriptionLeaseError::Capacity
        );
        let second = leases
            .claim(second_work, second_member, 1, 102, 20, 1)
            .unwrap();
        assert!(leases.can_ack(&first, 103));
        assert!(leases.can_ack(&second, 103));
        leases.fence_member(first_member, 2).unwrap();
        assert!(!leases.can_ack(&first, 103));
        assert_eq!(
            leases
                .claim(first_work, first_member, 1, 103, 20, 1)
                .unwrap_err(),
            SubscriptionLeaseError::StaleLease
        );
        let renewed_session = leases
            .claim(first_work, first_member, 2, 103, 20, 1)
            .unwrap();
        assert!(renewed_session.lease_epoch > first.lease_epoch);
        assert!(!leases.can_ack(&first, 104));
        assert_eq!(
            leases.renew(&first, 104, 20).unwrap_err(),
            SubscriptionLeaseError::StaleLease
        );
        assert_eq!(
            leases
                .claim(first_work, second_member, 1, 110, 20, 1)
                .unwrap_err(),
            SubscriptionLeaseError::Busy
        );
        let replacement = leases
            .claim(first_work, second_member, 1, 123, 20, 2)
            .unwrap();
        assert!(replacement.lease_epoch > renewed_session.lease_epoch);
        assert!(!leases.can_ack(&renewed_session, 124));
        assert!(!leases.can_ack(&second, 124));
        assert_eq!(
            leases
                .claim(Uuid::from_u128(5), first_member, 2, 125, 20, 1)
                .unwrap_err(),
            SubscriptionLeaseError::Capacity
        );
        assert_eq!(
            leases
                .claim(first_work, second_member, 1, 120, 20, 2)
                .unwrap_err(),
            SubscriptionLeaseError::InvalidClock
        );
        assert_eq!(
            leases
                .claim(first_work, second_member, 1, u64::MAX, 2, 2)
                .unwrap_err(),
            SubscriptionLeaseError::InvalidClock
        );
        assert_eq!(SubscriptionLeaseTracker::new(usize::MAX).max_work, 65_536);
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

    fn subscription_progress_mutation(
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
            request_id: Uuid::from_u128(sequence as u128 + 9_000),
            expected_cursor: (sequence > 1).then(|| format!("rf1_{}", sequence - 1)),
            cursor: format!("rf1_{sequence}"),
            positions: BTreeMap::from([(
                RangeId::from_uuid(Uuid::from_u128(7_000 + sequence as u128)),
                format!("position-{sequence}"),
            )]),
            tick: 0,
            lease_ops: Vec::new(),
        }
    }

    #[test]
    fn subscription_lease_ops_commit_atomically_and_fence_members() {
        let directory = tempfile::TempDir::new().unwrap();
        let replica = FjallSubscriptionProgressReplica::open(directory.path()).unwrap();
        let subscription_id = Uuid::from_u128(610);
        let feed_id = Uuid::from_u128(611);
        let member = Uuid::from_u128(612);
        let other = Uuid::from_u128(613);
        let work = Uuid::from_u128(614);

        let mut join = subscription_progress_mutation(subscription_id, feed_id, 1, 1);
        join.lease_ops = vec![SubscriptionLeaseOp::Join {
            member_id: member,
            member_epoch: 1,
        }];
        commit_direct(&replica, &join);
        let state = replica.local_state(subscription_id).unwrap();
        assert_eq!(state.members.member_epochs.get(&member), Some(&1));
        assert!(state.members.leases.is_empty());

        // a lease-only mutation leaves the frontier unchanged.
        let mut claim = subscription_progress_mutation(subscription_id, feed_id, 1, 2);
        claim.expected_cursor = Some(join.cursor.clone());
        claim.cursor = join.cursor.clone();
        claim.tick = 10;
        claim.lease_ops = vec![SubscriptionLeaseOp::Claim {
            work_id: work,
            member_id: member,
            member_epoch: 1,
            lease_ticks: 30,
        }];
        commit_direct(&replica, &claim);
        let state = replica.local_state(subscription_id).unwrap();
        let granted = state.members.leases.get(&work).unwrap();
        assert_eq!(
            (granted.member_id, granted.member_epoch, granted.lease_epoch),
            (member, 1, 1)
        );
        assert_eq!(granted.expires_at_tick, 40);
        assert_eq!(state.members.last_tick, 10);
        assert_eq!(state.committed.as_ref().unwrap().cursor, join.cursor);

        // a member that never joined cannot claim.
        let mut outsider = subscription_progress_mutation(subscription_id, feed_id, 1, 3);
        outsider.expected_cursor = Some(join.cursor.clone());
        outsider.cursor = join.cursor.clone();
        outsider.tick = 20;
        outsider.lease_ops = vec![SubscriptionLeaseOp::Claim {
            work_id: Uuid::from_u128(615),
            member_id: other,
            member_epoch: 1,
            lease_ticks: 10,
        }];
        assert!(matches!(
            replica.prepare(outsider),
            Err(SubscriptionProgressError::Lease(
                SubscriptionLeaseError::StaleLease
            ))
        ));

        // the live lease cannot be stolen before expiry.
        let mut join_other = subscription_progress_mutation(subscription_id, feed_id, 1, 3);
        join_other.expected_cursor = Some(join.cursor.clone());
        join_other.cursor = join.cursor.clone();
        join_other.tick = 20;
        join_other.lease_ops = vec![
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
        ];
        assert!(matches!(
            replica.prepare(join_other.clone()),
            Err(SubscriptionProgressError::Lease(
                SubscriptionLeaseError::Busy
            ))
        ));

        // after expiry the other member takes over at a higher lease epoch.
        let mut takeover = join_other.clone();
        takeover.tick = 45;
        commit_direct(&replica, &takeover);
        let state = replica.local_state(subscription_id).unwrap();
        let held = state.members.leases.get(&work).unwrap();
        assert_eq!(
            (held.member_id, held.member_epoch, held.lease_epoch),
            (other, 1, 2)
        );
        assert_eq!(held.expires_at_tick, 55);

        // lease clocks never move backwards.
        let mut rewind = subscription_progress_mutation(subscription_id, feed_id, 1, 4);
        rewind.expected_cursor = Some(join.cursor.clone());
        rewind.cursor = join.cursor.clone();
        rewind.tick = 30;
        rewind.lease_ops = vec![SubscriptionLeaseOp::Renew {
            work_id: work,
            member_id: other,
            member_epoch: 1,
            lease_epoch: 2,
            lease_ticks: 10,
        }];
        assert!(matches!(
            replica.prepare(rewind),
            Err(SubscriptionProgressError::Lease(
                SubscriptionLeaseError::InvalidClock
            ))
        ));

        // renew requires the exact lease epoch.
        let mut wrong_epoch = subscription_progress_mutation(subscription_id, feed_id, 1, 4);
        wrong_epoch.expected_cursor = Some(join.cursor.clone());
        wrong_epoch.cursor = join.cursor.clone();
        wrong_epoch.tick = 50;
        wrong_epoch.lease_ops = vec![SubscriptionLeaseOp::Renew {
            work_id: work,
            member_id: other,
            member_epoch: 1,
            lease_epoch: 1,
            lease_ticks: 10,
        }];
        assert!(matches!(
            replica.prepare(wrong_epoch),
            Err(SubscriptionProgressError::Lease(
                SubscriptionLeaseError::StaleLease
            ))
        ));

        // release removes the lease at its exact epoch.
        let mut release = subscription_progress_mutation(subscription_id, feed_id, 1, 4);
        release.expected_cursor = Some(join.cursor.clone());
        release.cursor = join.cursor.clone();
        release.tick = 50;
        release.lease_ops = vec![SubscriptionLeaseOp::Release {
            work_id: work,
            member_id: other,
            member_epoch: 1,
            lease_epoch: 2,
        }];
        commit_direct(&replica, &release);
        assert!(replica
            .local_state(subscription_id)
            .unwrap()
            .members
            .leases
            .is_empty());

        // member fencing expires that member's remaining leases.
        let mut reclaim = subscription_progress_mutation(subscription_id, feed_id, 1, 5);
        reclaim.expected_cursor = Some(join.cursor.clone());
        reclaim.cursor = join.cursor.clone();
        reclaim.tick = 60;
        reclaim.lease_ops = vec![
            SubscriptionLeaseOp::Claim {
                work_id: work,
                member_id: member,
                member_epoch: 1,
                lease_ticks: 30,
            },
            SubscriptionLeaseOp::Join {
                member_id: member,
                member_epoch: 2,
            },
        ];
        commit_direct(&replica, &reclaim);
        let state = replica.local_state(subscription_id).unwrap();
        assert_eq!(state.members.member_epochs.get(&member), Some(&2));
        let expired = state.members.leases.get(&work).unwrap();
        assert_eq!(expired.lease_epoch, 1);
        assert!(expired.expires_at_tick <= state.members.last_tick);
        let tracker = SubscriptionLeaseTracker::from_state(
            state.members.clone(),
            SUBSCRIPTION_LEASE_MAX_WORK,
        );
        assert!(!tracker.can_ack(expired, 61));

        // member state survives a replica restart.
        drop(replica);
        let reopened = FjallSubscriptionProgressReplica::open(directory.path()).unwrap();
        let state = reopened.local_state(subscription_id).unwrap();
        assert_eq!(state.members.member_epochs.get(&member), Some(&2));
        assert!(state.members.leases.contains_key(&work));

        // an adopt under a higher epoch installs the member snapshot.
        let adopted_dir = tempfile::TempDir::new().unwrap();
        let adopted = FjallSubscriptionProgressReplica::open(adopted_dir.path()).unwrap();
        let mut recovered_committed = state.committed.clone().unwrap();
        recovered_committed.ownership_epoch = 2;
        adopted
            .adopt_recovered(
                subscription_id,
                feed_id,
                2,
                Some(recovered_committed),
                state.members.clone(),
            )
            .unwrap();
        let adopted_state = adopted.local_state(subscription_id).unwrap();
        assert_eq!(adopted_state.members, state.members);
        // the same committed value with a different member snapshot is a fork.
        assert!(matches!(
            adopted.adopt_recovered(
                subscription_id,
                feed_id,
                2,
                adopted_state.committed.clone(),
                SubscriptionMemberState::default()
            ),
            Err(SubscriptionProgressError::Conflict)
        ));
    }

    #[test]
    fn subscription_lease_ops_stay_invisible_until_committed() {
        let directory = tempfile::TempDir::new().unwrap();
        let replica = FjallSubscriptionProgressReplica::open(directory.path()).unwrap();
        let subscription_id = Uuid::from_u128(620);
        let feed_id = Uuid::from_u128(621);
        let member = Uuid::from_u128(622);
        let work = Uuid::from_u128(623);

        let first = subscription_progress_mutation(subscription_id, feed_id, 1, 1);
        commit_direct(&replica, &first);

        let mut claim = subscription_progress_mutation(subscription_id, feed_id, 1, 2);
        claim.expected_cursor = Some(first.cursor.clone());
        claim.cursor = first.cursor.clone();
        claim.tick = 5;
        claim.lease_ops = vec![
            SubscriptionLeaseOp::Join {
                member_id: member,
                member_epoch: 1,
            },
            SubscriptionLeaseOp::Claim {
                work_id: work,
                member_id: member,
                member_epoch: 1,
                lease_ticks: 20,
            },
        ];
        replica.prepare(claim).unwrap();
        let state = replica.local_state(subscription_id).unwrap();
        assert!(state.prepared.is_some());
        assert!(state.members.member_epochs.is_empty());
        assert!(state.members.leases.is_empty());

        drop(replica);
        let reopened = FjallSubscriptionProgressReplica::open(directory.path()).unwrap();
        let state = reopened.local_state(subscription_id).unwrap();
        assert!(state.prepared.is_some());
        assert!(state.members.member_epochs.is_empty());
    }

    #[test]
    fn subscription_lease_ops_obey_capacity_and_bounded_fan_out() {
        let directory = tempfile::TempDir::new().unwrap();
        let replica = FjallSubscriptionProgressReplica::open(directory.path()).unwrap();
        let subscription_id = Uuid::from_u128(630);
        let feed_id = Uuid::from_u128(631);
        let member = Uuid::from_u128(632);

        let mut fanout = subscription_progress_mutation(subscription_id, feed_id, 1, 1);
        fanout.lease_ops = (0..SUBSCRIPTION_LEASE_MAX_OPS as u128 + 1)
            .map(|index| SubscriptionLeaseOp::Join {
                member_id: Uuid::from_u128(7_000 + index),
                member_epoch: 1,
            })
            .collect();
        assert!(matches!(
            replica.prepare(fanout),
            Err(SubscriptionProgressError::TooLarge)
        ));

        let mut join = subscription_progress_mutation(subscription_id, feed_id, 1, 1);
        join.lease_ops = vec![SubscriptionLeaseOp::Join {
            member_id: member,
            member_epoch: 1,
        }];
        commit_direct(&replica, &join);

        let mut exact = subscription_progress_mutation(subscription_id, feed_id, 1, 2);
        exact.expected_cursor = Some(join.cursor.clone());
        exact.cursor = join.cursor.clone();
        exact.tick = 5;
        exact.lease_ops = (0..SUBSCRIPTION_LEASE_MAX_OPS as u128)
            .map(|index| SubscriptionLeaseOp::Claim {
                work_id: Uuid::from_u128(10_000 + index),
                member_id: member,
                member_epoch: 1,
                lease_ticks: 10,
            })
            .collect();
        commit_direct(&replica, &exact);
        let state = replica.local_state(subscription_id).unwrap();
        assert_eq!(state.members.leases.len(), SUBSCRIPTION_LEASE_MAX_OPS);
    }

    fn commit_direct(
        replica: &FjallSubscriptionProgressReplica,
        mutation: &SubscriptionProgressMutation,
    ) {
        replica.prepare(mutation.clone()).unwrap();
        let digest = *blake3::hash(&serde_json::to_vec(mutation).unwrap()).as_bytes();
        replica
            .commit_with_quorum(
                SubscriptionCommitEvidence::new(
                    mutation.subscription_id,
                    mutation.request_id,
                    [
                        (
                            crate::active_range::StorageNodeId::try_new("storage-a").unwrap(),
                            digest,
                        ),
                        (
                            crate::active_range::StorageNodeId::try_new("storage-b").unwrap(),
                            digest,
                        ),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
    }

    #[test]
    fn subscription_progress_adopt_recovered_fences_epochs_and_preserves_progress() {
        let directory = tempfile::TempDir::new().unwrap();
        let replica = FjallSubscriptionProgressReplica::open(directory.path()).unwrap();
        let subscription_id = Uuid::from_u128(600);
        let feed_id = Uuid::from_u128(601);
        let first = subscription_progress_mutation(subscription_id, feed_id, 1, 1);
        commit_direct(&replica, &first);

        assert!(matches!(
            replica.adopt_recovered(
                subscription_id,
                feed_id,
                0,
                None,
                SubscriptionMemberState::default()
            ),
            Err(SubscriptionProgressError::StaleEpoch)
        ));
        let mut adopted_first = first.clone();
        adopted_first.ownership_epoch = 2;
        assert_eq!(
            replica
                .adopt_recovered(
                    subscription_id,
                    feed_id,
                    2,
                    Some(adopted_first.clone()),
                    SubscriptionMemberState::default()
                )
                .unwrap(),
            Some(adopted_first.clone())
        );
        assert_eq!(
            replica
                .adopt_recovered(
                    subscription_id,
                    feed_id,
                    2,
                    Some(adopted_first.clone()),
                    SubscriptionMemberState::default()
                )
                .unwrap(),
            Some(adopted_first.clone())
        );
        assert!(matches!(
            replica.adopt_recovered(
                subscription_id,
                feed_id,
                2,
                None,
                SubscriptionMemberState::default()
            ),
            Err(SubscriptionProgressError::Conflict)
        ));
        let mut divergent = subscription_progress_mutation(subscription_id, feed_id, 2, 2);
        divergent.request_id = Uuid::from_u128(6_666);
        assert!(matches!(
            replica.adopt_recovered(
                subscription_id,
                feed_id,
                2,
                Some(divergent),
                SubscriptionMemberState::default()
            ),
            Err(SubscriptionProgressError::Conflict)
        ));

        let second = subscription_progress_mutation(subscription_id, feed_id, 3, 2);
        assert_eq!(
            replica
                .adopt_recovered(
                    subscription_id,
                    feed_id,
                    3,
                    Some(second.clone()),
                    SubscriptionMemberState::default()
                )
                .unwrap(),
            Some(second.clone())
        );
        let mut regressed = subscription_progress_mutation(subscription_id, feed_id, 4, 1);
        regressed.ownership_epoch = 4;
        assert!(matches!(
            replica.adopt_recovered(
                subscription_id,
                feed_id,
                4,
                Some(regressed),
                SubscriptionMemberState::default()
            ),
            Err(SubscriptionProgressError::Conflict)
        ));
        let mut forked = subscription_progress_mutation(subscription_id, feed_id, 4, 2);
        forked.request_id = Uuid::from_u128(6_667);
        assert!(matches!(
            replica.adopt_recovered(
                subscription_id,
                feed_id,
                4,
                Some(forked),
                SubscriptionMemberState::default()
            ),
            Err(SubscriptionProgressError::Conflict)
        ));
        let mut unchained = subscription_progress_mutation(subscription_id, feed_id, 4, 3);
        unchained.expected_cursor = Some("rf1_unknown".to_owned());
        assert!(matches!(
            replica.adopt_recovered(
                subscription_id,
                feed_id,
                4,
                Some(unchained),
                SubscriptionMemberState::default()
            ),
            Err(SubscriptionProgressError::Conflict)
        ));

        let pending = subscription_progress_mutation(subscription_id, feed_id, 3, 3);
        replica.prepare(pending.clone()).unwrap();
        let third = subscription_progress_mutation(subscription_id, feed_id, 5, 3);
        assert_eq!(
            replica
                .adopt_recovered(
                    subscription_id,
                    feed_id,
                    5,
                    Some(third.clone()),
                    SubscriptionMemberState::default()
                )
                .unwrap(),
            Some(third.clone())
        );
        let state = replica.local_state(subscription_id).unwrap();
        assert_eq!(state.committed, Some(third.clone()));
        assert!(state.prepared.is_none());
        assert!(matches!(
            replica.prepare(subscription_progress_mutation(
                subscription_id,
                feed_id,
                4,
                4
            )),
            Err(SubscriptionProgressError::StaleEpoch)
        ));

        let empty_subscription = Uuid::from_u128(602);
        assert_eq!(
            replica
                .adopt_recovered(
                    empty_subscription,
                    feed_id,
                    2,
                    None,
                    SubscriptionMemberState::default()
                )
                .unwrap(),
            None
        );
        replica
            .prepare(subscription_progress_mutation(
                empty_subscription,
                feed_id,
                2,
                1,
            ))
            .unwrap();
        assert!(matches!(
            replica.adopt_recovered(
                subscription_id,
                Uuid::from_u128(999),
                6,
                None,
                SubscriptionMemberState::default()
            ),
            Err(SubscriptionProgressError::Conflict)
        ));
        assert!(matches!(
            replica.adopt_recovered(
                Uuid::from_u128(999),
                feed_id,
                6,
                None,
                SubscriptionMemberState::default()
            ),
            Ok(None)
        ));

        drop(replica);
        let reopened = FjallSubscriptionProgressReplica::open(directory.path()).unwrap();
        assert_eq!(
            reopened.local_committed(subscription_id).unwrap(),
            Some(third)
        );
    }

    #[tokio::test]
    async fn subscription_progress_service_fences_inspect_and_adopt_placement() {
        use crate::{
            active_range::StorageNodeId,
            control::ControlController,
            storage::{FileLogStore, LogStore},
        };

        let directory = tempfile::TempDir::new().unwrap();
        let nodes = ["storage-a", "storage-b", "storage-c"]
            .map(|value| StorageNodeId::try_new(value).unwrap());
        let store: std::sync::Arc<dyn LogStore> =
            std::sync::Arc::new(FileLogStore::open(directory.path().join("feed-data")).unwrap());
        let control = std::sync::Arc::new(
            ControlController::open_with_storage_nodes(
                directory.path().join("catalog.json"),
                store,
                nodes.to_vec(),
            )
            .unwrap(),
        );
        control.execute("CREATE SPACE orders; CREATE FEED orders.events; CREATE SUBSCRIPTION orders.billing FROM orders.events;").await.unwrap();
        let subscription = control
            .active_subscription_by_name("orders.billing")
            .await
            .unwrap();
        let assignment = control
            .active_subscription_progress_assignment_by_id(subscription.subscription_id)
            .await
            .unwrap();
        let path = directory.path().join("subscription-progress-adopt");
        let replica = std::sync::Arc::new(FjallSubscriptionProgressReplica::open(&path).unwrap());
        let service = SubscriptionProgressReplicaService::new(
            assignment.owner.clone(),
            control.clone(),
            replica.clone(),
        );
        let inspect = SubscriptionCommittedReadRequest {
            owner: assignment.owner.clone(),
            receiver: assignment.owner.clone(),
            subscription_id: subscription.subscription_id,
            ownership_epoch: 2,
        };
        assert!(matches!(
            service.inspect(inspect.clone()).await,
            Err(SubscriptionProgressError::InvalidAssignment)
        ));
        let adopt = SubscriptionProgressAdoptRequest {
            owner: assignment.owner.clone(),
            receiver: assignment.owner.clone(),
            subscription_id: subscription.subscription_id,
            ownership_epoch: 2,
            committed: None,
            members: SubscriptionMemberState::default(),
        };
        assert!(matches!(
            service.adopt(adopt.clone()).await,
            Err(SubscriptionProgressError::InvalidAssignment)
        ));

        let survivor = assignment
            .replicas
            .iter()
            .find(|node| *node != &assignment.owner)
            .unwrap()
            .clone();
        control
            .execute_commands(vec![
                crate::control::Command::RecoverSubscriptionProgressOwner {
                    subscription_id: subscription.subscription_id,
                    expected_ownership_epoch: 1,
                    new_owner: survivor.clone(),
                },
            ])
            .await
            .unwrap();
        assert!(matches!(
            service.inspect(inspect).await,
            Err(SubscriptionProgressError::InvalidAssignment)
        ));
        let mut stale_adopt = adopt.clone();
        stale_adopt.owner = survivor.clone();
        stale_adopt.ownership_epoch = 1;
        assert!(matches!(
            service.adopt(stale_adopt).await,
            Err(SubscriptionProgressError::InvalidAssignment)
        ));
        let adopted_committed = SubscriptionProgressMutation {
            subscription_id: subscription.subscription_id,
            feed_id: subscription.feed_id,
            ownership_epoch: 2,
            sequence: 1,
            request_id: Uuid::from_u128(9_999),
            expected_cursor: None,
            cursor: "rf1_recovered".to_owned(),
            positions: BTreeMap::from([(
                RangeId::from_uuid(Uuid::from_u128(9_998)),
                "position".to_owned(),
            )]),

            tick: 0,
            lease_ops: Vec::new(),
        };
        let adopt = SubscriptionProgressAdoptRequest {
            owner: survivor.clone(),
            receiver: assignment.owner.clone(),
            subscription_id: subscription.subscription_id,
            ownership_epoch: 2,
            committed: Some(adopted_committed.clone()),
            members: SubscriptionMemberState::default(),
        };
        assert_eq!(
            service.adopt(adopt).await.unwrap().result,
            Some(adopted_committed.clone())
        );
        let inspection = service
            .inspect(SubscriptionCommittedReadRequest {
                owner: survivor,
                receiver: assignment.owner.clone(),
                subscription_id: subscription.subscription_id,
                ownership_epoch: 2,
            })
            .await
            .unwrap()
            .result;
        assert_eq!(inspection.committed, Some(adopted_committed));
        assert!(inspection.prepared.is_none());
    }
}
