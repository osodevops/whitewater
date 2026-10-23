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

/// The `(positions, quarantine, skipped)` triple a cutover translation
/// restates on the post-transition range map.
pub type TranslatedSubscriptionFrontier = (
    BTreeMap<RangeId, String>,
    BTreeMap<RangeId, Vec<String>>,
    BTreeMap<RangeId, Vec<String>>,
);

/// Restate a committed Subscription frontier's per-range resume positions and
/// RangeId-keyed quarantine evidence across a committed split. The left child
/// retains the source RangeId, `right` is new, and positions are source
/// Cursor literals that the children inherit unchanged. Quarantined Cursors
/// follow their frame's KeyToken to the owning child; a Cursor absent from the
/// bounded lineage stays under the surviving left id, which can at worst lose
/// replay dedup evidence and never skips records.
pub fn translate_split_subscription_frontier(
    entries: &[ReaderLineageEntry],
    source: RangeId,
    right: RangeId,
    split_at: KeyToken,
    positions: &BTreeMap<RangeId, String>,
    quarantined: &BTreeMap<RangeId, Vec<String>>,
    skipped: &BTreeMap<RangeId, Vec<String>>,
) -> Result<TranslatedSubscriptionFrontier, ReaderFrontierTranslationError> {
    let source_cursor = positions
        .get(&source)
        .ok_or(ReaderFrontierTranslationError::InvalidSources)?;
    let children =
        translate_split_reader_frontier(entries, source, source, right, split_at, source_cursor)?;
    let mut translated = positions.clone();
    translated.extend(children);
    let mut translated_quarantined = quarantined.clone();
    if let Some(cursors) = translated_quarantined.remove(&source) {
        let tokens: BTreeMap<&str, KeyToken> = entries
            .iter()
            .map(|entry| (entry.cursor.as_str(), entry.key_token))
            .collect();
        for cursor in cursors {
            let owner = match tokens.get(cursor.as_str()) {
                Some(token) if *token >= split_at => right,
                _ => source,
            };
            translated_quarantined
                .entry(owner)
                .or_default()
                .push(cursor);
        }
    }
    let mut translated_skipped = skipped.clone();
    if let Some(cursors) = translated_skipped.remove(&source) {
        let tokens: BTreeMap<&str, KeyToken> = entries
            .iter()
            .map(|entry| (entry.cursor.as_str(), entry.key_token))
            .collect();
        for cursor in cursors {
            let owner = match tokens.get(cursor.as_str()) {
                Some(token) if *token >= split_at => right,
                _ => source,
            };
            translated_skipped.entry(owner).or_default().push(cursor);
        }
    }
    Ok((translated, translated_quarantined, translated_skipped))
}

/// Restate a committed Subscription frontier across a committed merge. The
/// merged range keeps the left RangeId and stores source Cursor literals, so
/// the right child's positions and quarantine evidence re-key to it verbatim.
pub fn translate_merge_subscription_frontier(
    left_entries: &[ReaderLineageEntry],
    right_entries: &[ReaderLineageEntry],
    left: RangeId,
    right: RangeId,
    positions: &BTreeMap<RangeId, String>,
    quarantined: &BTreeMap<RangeId, Vec<String>>,
    skipped: &BTreeMap<RangeId, Vec<String>>,
) -> Result<TranslatedSubscriptionFrontier, ReaderFrontierTranslationError> {
    let left_cursor = positions
        .get(&left)
        .ok_or(ReaderFrontierTranslationError::InvalidSources)?;
    let right_cursor = positions
        .get(&right)
        .ok_or(ReaderFrontierTranslationError::InvalidSources)?;
    let merged = translate_merge_reader_frontier(
        left_entries,
        right_entries,
        left,
        right,
        left_cursor,
        right_cursor,
    )?;
    let mut translated = positions.clone();
    translated.remove(&right);
    translated.insert(left, merged);
    let mut translated_quarantined = quarantined.clone();
    if let Some(cursors) = translated_quarantined.remove(&right) {
        translated_quarantined
            .entry(left)
            .or_default()
            .extend(cursors);
    }
    let mut translated_skipped = skipped.clone();
    if let Some(cursors) = translated_skipped.remove(&right) {
        translated_skipped.entry(left).or_default().extend(cursors);
    }
    Ok((translated, translated_quarantined, translated_skipped))
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

    /// Grant `initial` records immediately: a session opens trusted at
    /// its declared capacity and pressure halves it, while recovery
    /// back up still requires the sustained healthy samples.
    pub fn with_initial_grant(mut self, initial: usize) -> Self {
        self.current_records = initial.clamp(self.min_records, self.max_records);
        self
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

struct ReaderPacingEntry {
    controller: ReaderPacingController,
    session_capacity: usize,
    last_fetch_at: Option<std::time::Instant>,
    last_ack_latency: Duration,
    unacknowledged_bytes: usize,
    average_record_bytes: usize,
    backlog_hint: bool,
}

/// Node-local advisory pacing state per ReaderId. Delivery correctness
/// never depends on it — the fetch path still fails closed on real
/// errors — it only shapes the effective page bound and the
/// `recommended_capacity`/`retry_after_ms` advice a Reader receives, so
/// a slow or unacknowledging Reader throttles itself without squeezing
/// the capacity honest Readers observe. A session opens trusted at its
/// declared capacity; pressure halves the grant and only sustained
/// healthy, acknowledged full pages grow it back. State is intentionally local
/// to the serving Node (like demand telemetry): an ingress change
/// resets advice to the cautious minimum rather than migrating
/// heuristics through consensus.
#[derive(Clone, Default)]
pub struct ReaderPacingTable {
    inner: std::sync::Arc<std::sync::Mutex<BTreeMap<Uuid, ReaderPacingEntry>>>,
}

impl ReaderPacingTable {
    const MAX_ENTRIES: usize = 8192;
    const MIN_RECORDS: usize = 8;
    const TARGET_ACK_LATENCY: Duration = Duration::from_millis(250);

    /// One pacing observation per fetch: the plan bounds this page and
    /// fills the response advice. A poisoned or saturated table fails
    /// open to the client's own capacity — pacing must never become a
    /// correctness dependency.
    pub fn plan(
        &self,
        reader_id: Uuid,
        session_capacity: usize,
        client_capacity: usize,
        node_pressure: f64,
        max_bytes: usize,
    ) -> ReaderPacingPlan {
        let fallback = ReaderPacingPlan {
            max_records: client_capacity,
            max_bytes,
            retry_after: Duration::ZERO,
            record_exceeds_budget: false,
        };
        let Ok(mut map) = self.inner.lock() else {
            return fallback;
        };
        if !map.contains_key(&reader_id) {
            if map.len() >= Self::MAX_ENTRIES {
                return fallback;
            }
            let capacity = session_capacity.max(1);
            map.insert(
                reader_id,
                ReaderPacingEntry {
                    controller: ReaderPacingController::new(
                        Self::MIN_RECORDS.min(capacity),
                        capacity,
                        max_bytes,
                        Self::TARGET_ACK_LATENCY,
                    )
                    .with_initial_grant(capacity),
                    session_capacity: capacity,
                    last_fetch_at: None,
                    last_ack_latency: Duration::ZERO,
                    unacknowledged_bytes: 0,
                    average_record_bytes: 1,
                    backlog_hint: false,
                },
            );
        }
        let Some(entry) = map.get_mut(&reader_id) else {
            return fallback;
        };
        // A reopened session may declare a different capacity: re-
        // baseline the controller to its bounds while keeping the
        // reader-level signals (unacknowledged bytes, sizes, latency)
        // that describe delivered work regardless of session epoch.
        let capacity = session_capacity.max(1);
        if entry.session_capacity != capacity {
            entry.session_capacity = capacity;
            entry.controller = ReaderPacingController::new(
                Self::MIN_RECORDS.min(capacity),
                capacity,
                max_bytes,
                Self::TARGET_ACK_LATENCY,
            )
            .with_initial_grant(capacity);
        }
        entry.controller.observe(
            ReaderPressureSample {
                // A page that filled its bound is the honest signal that
                // more history waits behind it; the merge does not price
                // an exact remaining count.
                backlog_records: if entry.backlog_hint { u64::MAX } else { 0 },
                average_record_bytes: entry.average_record_bytes,
                acknowledgement_latency: entry.last_ack_latency,
                unacknowledged_bytes: entry.unacknowledged_bytes,
                node_pressure,
                // The fetch path already fails closed when replicas
                // cannot corroborate, so reaching the plan means the
                // data path was served.
                replica_ready: true,
            },
            client_capacity,
        )
    }

    /// Record a served page: byte volume feeds the unacknowledged-byte
    /// budget until the next ack, the filled-page flag is the backlog
    /// hint, and the fetch timestamp starts the ack-latency sample.
    pub fn record_page(
        &self,
        reader_id: Uuid,
        page_records: usize,
        page_bytes: usize,
        effective_limit: usize,
    ) {
        let Ok(mut map) = self.inner.lock() else {
            return;
        };
        let Some(entry) = map.get_mut(&reader_id) else {
            return;
        };
        entry.last_fetch_at = Some(std::time::Instant::now());
        entry.unacknowledged_bytes = entry.unacknowledged_bytes.saturating_add(page_bytes);
        if page_records > 0 {
            entry.average_record_bytes = (page_bytes / page_records).max(1);
        }
        entry.backlog_hint = effective_limit > 0 && page_records >= effective_limit;
    }

    /// An ack retires the delivered byte budget and completes the
    /// fetch→ack latency sample. Acks landing on a Node that never
    /// served a page have no state to update.
    pub fn observe_ack(&self, reader_id: Uuid) {
        let Ok(mut map) = self.inner.lock() else {
            return;
        };
        let Some(entry) = map.get_mut(&reader_id) else {
            return;
        };
        entry.unacknowledged_bytes = 0;
        if let Some(fetch_at) = entry.last_fetch_at.take() {
            entry.last_ack_latency = fetch_at.elapsed();
        }
    }

    /// Session close drops the entry; bounded capacity plus removal on
    /// close keeps the table proportional to live Readers.
    pub fn remove(&self, reader_id: Uuid) {
        if let Ok(mut map) = self.inner.lock() {
            map.remove(&reader_id);
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
    /// Set once this claim's declared exhausted-delivery policy fired; the
    /// flag resets whenever the lease is reclaimed or transferred because a
    /// new grant builds a fresh `SubscriptionWorkLease`.
    #[serde(default)]
    pub exhausted_applied: bool,
    /// The Key window this claim pinned, if any: `None` keeps the legacy
    /// unfiltered delivery contract while `Some(window)` scopes fetches to
    /// the lease's coverage and requires delivery evidence at ack.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_window: Option<u16>,
}

/// One incremental lease handoff: the work item, the current holder's
/// fencing evidence, and the target member + lease duration.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscriptionLeaseTransfer {
    pub work_id: Uuid,
    pub from_member_id: Uuid,
    pub from_lease_epoch: u64,
    pub to_member_id: Uuid,
    pub to_member_epoch: u64,
    pub lease_ticks: u64,
}

pub const SUBSCRIPTION_LEASE_MAX_WORK: usize = 256;
pub const SUBSCRIPTION_LEASE_MAX_MEMBER_WORK: usize = 32;
pub const SUBSCRIPTION_LEASE_MAX_OPS: usize = 16;
/// Cumulative quarantined positions a Subscription frontier may carry;
/// evidence stays explainable instead of growing an unbounded audit row.
pub const SUBSCRIPTION_QUARANTINE_MAX_POSITIONS: usize = 1024;
/// Cumulative positions a declared retry budget permanently passed over;
/// bounded evidence like quarantine, but never releasable — skip is a final
/// disposition, while explicit SEEK remains the replay mechanism.
pub const SUBSCRIPTION_SKIP_MAX_POSITIONS: usize = 1024;
/// Per-Key work subdivision: a lease that pins a Key window exclusively
/// serves the records whose `KeyToken` maps into its coverage, and every
/// window no live windowed lease claims is dealt across those leases in
/// `work_id` order — a pure function of committed member state, identical
/// on every replica, so coverage needs no assignment protocol and never
/// rebalances pinned work.
pub const SUBSCRIPTION_WORK_WINDOWS: u16 = 256;
/// Positions a frontier moved past while leaving them owed to another
/// Key window — transient re-delivery evidence that drains when the
/// covering lease consumes them, bounded like quarantine evidence.
pub const SUBSCRIPTION_DEFERRED_MAX_POSITIONS: usize = 4096;

/// The quarantine deltas one acknowledgement applies to the cumulative
/// frontier set: positions newly declared delivered-but-not-processed, and
/// positions released once resolved.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SubscriptionQuarantineDelta {
    pub add: BTreeMap<RangeId, Vec<String>>,
    pub remove: BTreeMap<RangeId, Vec<String>>,
}

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
        /// Optional Key window the claim pins; absent keeps unfiltered
        /// delivery for that lease.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key_window: Option<u16>,
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
    /// Moves one work lease to another joined member — the incremental
    /// handoff primitive: one item at a time, never a global rebalance.
    Transfer {
        work_id: Uuid,
        from_member_id: Uuid,
        from_lease_epoch: u64,
        to_member_id: Uuid,
        to_member_epoch: u64,
        lease_ticks: u64,
    },
    /// Marks that the claimed work's declared retry budget was exceeded and
    /// the fetch-time exhausted policy already committed once under this
    /// grant, so subsequent fetches under the same lease deliver normally.
    ApplyExhausted {
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
    #[error("key window is out of range")]
    InvalidWindow,
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

    #[allow(clippy::too_many_arguments)]
    pub fn claim(
        &mut self,
        work_id: Uuid,
        member_id: Uuid,
        member_epoch: u64,
        now_tick: u64,
        lease_ticks: u64,
        max_member_work: usize,
        key_window: Option<u16>,
    ) -> Result<SubscriptionWorkLease, SubscriptionLeaseError> {
        self.check_tick(now_tick, lease_ticks)?;
        if self.member_epochs.get(&member_id) != Some(&member_epoch) {
            return Err(SubscriptionLeaseError::StaleLease);
        }
        if max_member_work == 0 {
            return Err(SubscriptionLeaseError::Capacity);
        }
        if key_window.is_some_and(|window| window >= SUBSCRIPTION_WORK_WINDOWS) {
            return Err(SubscriptionLeaseError::InvalidWindow);
        }
        if key_window.is_some_and(|window| {
            self.leases.values().any(|lease| {
                lease.work_id != work_id
                    && lease.key_window == Some(window)
                    && now_tick < lease.expires_at_tick
            })
        }) {
            return Err(SubscriptionLeaseError::Busy);
        }
        if let Some(existing) = self.leases.get(&work_id) {
            if now_tick < existing.expires_at_tick {
                if existing.member_id == member_id && existing.member_epoch == member_epoch {
                    // A re-claim is only the same work when the pinned
                    // window matches; a different window on live work is
                    // a different claim competing for the work_id.
                    if existing.key_window == key_window {
                        return Ok(existing.clone());
                    }
                    return Err(SubscriptionLeaseError::Busy);
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
            exhausted_applied: false,
            key_window,
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

    /// Transfers one work lease to another joined member. The stored
    /// grant must match the caller's source evidence (holder + lease
    /// epoch); the grant's `lease_epoch` advances so the previous holder
    /// can never acknowledge it, while the target can immediately.
    /// Replaying an already-applied transfer returns the moved grant.
    pub fn transfer(
        &mut self,
        transfer: &SubscriptionLeaseTransfer,
        now_tick: u64,
        max_member_work: usize,
    ) -> Result<SubscriptionWorkLease, SubscriptionLeaseError> {
        self.check_tick(now_tick, transfer.lease_ticks)?;
        if self.member_epochs.get(&transfer.to_member_id) != Some(&transfer.to_member_epoch) {
            return Err(SubscriptionLeaseError::StaleLease);
        }
        if max_member_work == 0 {
            return Err(SubscriptionLeaseError::Capacity);
        }
        let existing = self
            .leases
            .get(&transfer.work_id)
            .ok_or(SubscriptionLeaseError::StaleLease)?;
        if existing.member_id == transfer.to_member_id
            && existing.member_epoch == transfer.to_member_epoch
        {
            return Ok(existing.clone());
        }
        if existing.member_id != transfer.from_member_id
            || existing.lease_epoch != transfer.from_lease_epoch
        {
            return Err(SubscriptionLeaseError::StaleLease);
        }
        let active_for_target = self
            .leases
            .values()
            .filter(|lease| {
                lease.member_id == transfer.to_member_id
                    && lease.member_epoch == transfer.to_member_epoch
                    && now_tick < lease.expires_at_tick
            })
            .count();
        if active_for_target >= max_member_work {
            return Err(SubscriptionLeaseError::Capacity);
        }
        let lease_epoch = existing
            .lease_epoch
            .checked_add(1)
            .ok_or(SubscriptionLeaseError::InvalidClock)?;
        let expires_at_tick = now_tick
            .checked_add(transfer.lease_ticks)
            .ok_or(SubscriptionLeaseError::InvalidClock)?;
        let grant = SubscriptionWorkLease {
            work_id: transfer.work_id,
            member_id: transfer.to_member_id,
            member_epoch: transfer.to_member_epoch,
            lease_epoch,
            expires_at_tick,
            exhausted_applied: false,
            // The pinned window moves with the work: the recipient takes
            // over the same coverage the previous holder served.
            key_window: self
                .leases
                .get(&transfer.work_id)
                .and_then(|lease| lease.key_window),
        };
        self.leases.insert(transfer.work_id, grant.clone());
        self.last_tick = now_tick;
        Ok(grant)
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
                key_window,
            } => self
                .claim(
                    *work_id,
                    *member_id,
                    *member_epoch,
                    now_tick,
                    *lease_ticks,
                    SUBSCRIPTION_LEASE_MAX_MEMBER_WORK,
                    *key_window,
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
                        exhausted_applied: false,
                        key_window: None,
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
                exhausted_applied: false,
                key_window: None,
            }),
            SubscriptionLeaseOp::Transfer {
                work_id,
                from_member_id,
                from_lease_epoch,
                to_member_id,
                to_member_epoch,
                lease_ticks,
            } => self
                .transfer(
                    &SubscriptionLeaseTransfer {
                        work_id: *work_id,
                        from_member_id: *from_member_id,
                        from_lease_epoch: *from_lease_epoch,
                        to_member_id: *to_member_id,
                        to_member_epoch: *to_member_epoch,
                        lease_ticks: *lease_ticks,
                    },
                    now_tick,
                    SUBSCRIPTION_LEASE_MAX_MEMBER_WORK,
                )
                .map(|_| ()),
            SubscriptionLeaseOp::ApplyExhausted {
                work_id,
                member_id,
                member_epoch,
                lease_epoch,
            } => self.apply_exhausted(
                &SubscriptionWorkLease {
                    work_id: *work_id,
                    member_id: *member_id,
                    member_epoch: *member_epoch,
                    lease_epoch: *lease_epoch,
                    expires_at_tick: 0,
                    exhausted_applied: false,
                    key_window: None,
                },
                now_tick,
            ),
        }
    }

    /// Flags the live grant matching the caller's evidence so one exhausted
    /// disposition commits per claim epoch. The op replays harmlessly — a
    /// grant already marked returns success — but a stale holder or expired
    /// lease is fenced like every other member mutation.
    fn apply_exhausted(
        &mut self,
        grant: &SubscriptionWorkLease,
        now_tick: u64,
    ) -> Result<(), SubscriptionLeaseError> {
        if now_tick < self.last_tick {
            return Err(SubscriptionLeaseError::InvalidClock);
        }
        match self.leases.get_mut(&grant.work_id) {
            Some(current)
                if current.member_id == grant.member_id
                    && current.member_epoch == grant.member_epoch
                    && current.lease_epoch == grant.lease_epoch
                    && now_tick < current.expires_at_tick =>
            {
                current.exhausted_applied = true;
                self.last_tick = now_tick;
                Ok(())
            }
            _ => Err(SubscriptionLeaseError::StaleLease),
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

/// One member Feed's independent frontier inside a Domain-source
/// Subscription's committed progress. No cross-Feed order is promised, so
/// each Feed keeps its own Cursor, positions, and durable
/// quarantine/skip evidence; the enclosing mutation's top-level delta
/// fields describe whichever Feed `member_feed` names.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MemberFeedProgress {
    pub cursor: String,
    pub positions: BTreeMap<RangeId, String>,
    /// Cumulative positions members quarantined on this Feed.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub quarantined: BTreeMap<RangeId, Vec<String>>,
    /// Cumulative positions permanently skipped on this Feed.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub skipped: BTreeMap<RangeId, Vec<String>>,
    /// Positions this Feed's frontier passed while leaving them owed to
    /// another member Key window — drained when the covering lease
    /// consumes them.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub deferred: BTreeMap<RangeId, Vec<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionProgressMutation {
    pub subscription_id: Uuid,
    /// The progress scope identity: the source Feed for exact-Feed
    /// Subscriptions, the bound Domain for Domain-source ones — the row
    /// key never changes across a Subscription's lifetime.
    pub feed_id: Uuid,
    pub ownership_epoch: u64,
    pub sequence: u64,
    pub request_id: Uuid,
    /// Compare-and-set guard on the frontier this mutation advances: the
    /// committed row's `cursor` for exact-Feed rows, the committed
    /// `member_progress[member_feed].cursor` for a member-Feed delta, and
    /// `None` when the touched entry is being attached fresh.
    pub expected_cursor: Option<String>,
    pub cursor: String,
    pub positions: BTreeMap<RangeId, String>,
    /// Domain-source Subscriptions only: the full per-Feed frontier map
    /// this commit carries forward. Entries the mutation does not name
    /// must equal the committed row verbatim; new entries attach a
    /// discovered member Feed, and dropped entries detach one.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub member_progress: BTreeMap<Uuid, MemberFeedProgress>,
    /// The member Feed whose frontier this mutation advances. Present
    /// whenever `member_progress` is non-empty and the mutation moves a
    /// Feed's cursor; `None` marks a pure attach/detach or lease-only
    /// mutation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member_feed: Option<Uuid>,
    #[serde(default)]
    pub tick: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lease_ops: Vec<SubscriptionLeaseOp>,
    /// Cumulative positions members acknowledged as delivered but
    /// explicitly not processed — durable poison/quarantine evidence a
    /// later SEEK can reprocess.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub quarantined: BTreeMap<RangeId, Vec<String>>,
    /// The acknowledgement deltas that produced `quarantined`; kept so an
    /// identical request replays and a changed one conflicts rather than
    /// silently restating a different intent.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub quarantine_add: BTreeMap<RangeId, Vec<String>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub quarantine_remove: BTreeMap<RangeId, Vec<String>>,
    /// Cumulative positions a declared retry budget permanently passed over
    /// — final-disposition evidence carried through seeks and cutovers.
    /// Unlike `quarantined`, no member operation releases skipped positions.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub skipped: BTreeMap<RangeId, Vec<String>>,
    /// The positions this mutation newly skipped; kept so an identical
    /// request replays and a changed one conflicts rather than restating.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub skip_add: BTreeMap<RangeId, Vec<String>>,
    /// Cumulative positions the frontier passed while leaving them owed
    /// to other member Key windows — transient owed-work evidence that
    /// drains when the covering lease consumes them.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub deferred: BTreeMap<RangeId, Vec<String>>,
    /// The positions this mutation newly deferred (a filtered page's
    /// withheld records) and newly cleared (previously-deferred positions
    /// the ack consumed); replay evidence like the quarantine deltas.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub deferred_add: BTreeMap<RangeId, Vec<String>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub deferred_remove: BTreeMap<RangeId, Vec<String>>,
}

/// The deferred-position deltas one filtered-window acknowledgement
/// applies to the cumulative owed set: positions the page withheld for
/// other windows (`add`) and previously-deferred positions it consumed
/// (`remove`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SubscriptionDeferredDelta {
    pub add: BTreeMap<RangeId, Vec<String>>,
    pub remove: BTreeMap<RangeId, Vec<String>>,
}

/// Deterministic Key → work-window mapping shared by member fetch
/// filtering and coverage derivation.
pub fn subscription_work_window(key: &[u8]) -> u16 {
    let token = crate::active_range::routing::KeyToken::from_key(key).as_bytes();
    (u64::from_be_bytes([
        token[0], token[1], token[2], token[3], token[4], token[5], token[6], token[7],
    ]) % SUBSCRIPTION_WORK_WINDOWS as u64) as u16
}

/// Total exclusive work-window coverage across the live windowed leases:
/// a claimed window belongs to its lease and every window no lease claims
/// is dealt round-robin across the live windowed leases in `work_id`
/// order, so the full Key space is always covered exactly once. Unwindowed
/// leases hold no coverage — they keep the legacy unfiltered contract.
/// The result is a pure function of committed member state: identical on
/// every replica, no assignment protocol, and pinned windows never move.
pub fn subscription_work_coverage(
    leases: &BTreeMap<Uuid, SubscriptionWorkLease>,
    now_tick: u64,
) -> BTreeMap<Uuid, BTreeSet<u16>> {
    let windowed: Vec<Uuid> = leases
        .iter()
        .filter(|(_, lease)| now_tick < lease.expires_at_tick && lease.key_window.is_some())
        .map(|(work_id, _)| *work_id)
        .collect();
    let claimed: BTreeSet<u16> = leases
        .values()
        .filter(|lease| now_tick < lease.expires_at_tick)
        .filter_map(|lease| lease.key_window)
        .collect();
    let mut coverage: BTreeMap<Uuid, BTreeSet<u16>> = windowed
        .iter()
        .map(|work_id| {
            (
                *work_id,
                leases
                    .get(work_id)
                    .and_then(|lease| lease.key_window)
                    .into_iter()
                    .collect::<BTreeSet<u16>>(),
            )
        })
        .collect();
    let n = windowed.len();
    for window in 0..SUBSCRIPTION_WORK_WINDOWS {
        if claimed.contains(&window) {
            continue;
        }
        if let Some(work_id) = windowed.get(window as usize % n.max(1)) {
            coverage.entry(*work_id).or_default().insert(window);
        }
    }
    coverage
}

/// The per-Feed frontier entries a Domain-source mutation may carry:
/// mirrors the catalog's `MAX_SUBSCRIPTION_MEMBER_FEEDS` bound.
const MEMBER_PROGRESS_MAX_FEEDS: usize = 4096;

/// Per-entry bounds for a member-Feed frontier, mirroring the top-level
/// frontier bounds so a Domain row cannot smuggle oversized state past
/// the legacy checks.
fn member_progress_bounds_ok(entry: &MemberFeedProgress) -> bool {
    !entry.cursor.is_empty()
        && entry.cursor.len() <= 256
        && !entry.positions.is_empty()
        && entry.positions.len() <= 128
        && entry.positions.values().all(|value| value.len() <= 256)
        && entry.quarantined.len() <= 128
        && entry
            .quarantined
            .values()
            .flatten()
            .all(|value| !value.is_empty() && value.len() <= 256)
        && entry.quarantined.values().map(Vec::len).sum::<usize>()
            <= SUBSCRIPTION_QUARANTINE_MAX_POSITIONS
        && entry
            .quarantined
            .keys()
            .all(|range| entry.positions.contains_key(range))
        && entry.skipped.len() <= 128
        && entry
            .skipped
            .values()
            .flatten()
            .all(|value| !value.is_empty() && value.len() <= 256)
        && entry.skipped.values().map(Vec::len).sum::<usize>() <= SUBSCRIPTION_SKIP_MAX_POSITIONS
        && entry
            .skipped
            .keys()
            .all(|range| entry.positions.contains_key(range))
        && entry.deferred.len() <= 128
        && entry
            .deferred
            .values()
            .flatten()
            .all(|value| !value.is_empty() && value.len() <= 256)
        && entry.deferred.values().map(Vec::len).sum::<usize>()
            <= SUBSCRIPTION_DEFERRED_MAX_POSITIONS
        && entry
            .deferred
            .keys()
            .all(|range| entry.positions.contains_key(range))
}

/// Validate the Domain-source member-Feed rules on a proposed mutation:
/// every entry stays inside the per-Feed bounds, the named member Feed's
/// entry restates the top-level delta exactly, entries the mutation does
/// not name copy the committed row forward verbatim, and a pure
/// attach/detach mutation (`member_feed` unset) leaves the aggregate
/// delta fields untouched and carries no per-Feed evidence.
fn member_progress_valid(
    mutation: &SubscriptionProgressMutation,
    committed: Option<&SubscriptionProgressMutation>,
) -> Result<(), SubscriptionProgressError> {
    if mutation.member_progress.is_empty() && mutation.member_feed.is_none() {
        return Ok(());
    }
    if mutation.member_progress.is_empty()
        || mutation.member_progress.len() > MEMBER_PROGRESS_MAX_FEEDS
        || !mutation
            .member_progress
            .values()
            .all(member_progress_bounds_ok)
    {
        return Err(SubscriptionProgressError::TooLarge);
    }
    match mutation.member_feed {
        Some(feed) => {
            let Some(entry) = mutation.member_progress.get(&feed) else {
                return Err(SubscriptionProgressError::Conflict);
            };
            if entry.cursor != mutation.cursor
                || entry.positions != mutation.positions
                || entry.quarantined != mutation.quarantined
                || entry.deferred != mutation.deferred
                || entry.skipped != mutation.skipped
            {
                return Err(SubscriptionProgressError::Conflict);
            }
        }
        None => {
            if !mutation.quarantine_add.is_empty()
                || !mutation.quarantine_remove.is_empty()
                || !mutation.skip_add.is_empty()
                || !mutation.deferred_add.is_empty()
                || !mutation.deferred_remove.is_empty()
            {
                return Err(SubscriptionProgressError::Conflict);
            }
            if let Some(committed) = committed {
                if committed.cursor != mutation.cursor
                    || committed.positions != mutation.positions
                    || committed.quarantined != mutation.quarantined
                    || committed.skipped != mutation.skipped
                    || committed.deferred != mutation.deferred
                {
                    return Err(SubscriptionProgressError::Conflict);
                }
            }
        }
    }
    if let Some(committed) = committed {
        for (feed, prior) in &committed.member_progress {
            if Some(*feed) == mutation.member_feed {
                continue;
            }
            match mutation.member_progress.get(feed) {
                Some(next) if next == prior => {}
                _ => return Err(SubscriptionProgressError::Conflict),
            }
        }
    }
    Ok(())
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

/// Decode a replica refusal into the wire-stable progress error when the
/// response body carries a recognized `code`; fall back to the HTTP status
/// so uncoded bodies still classify as conflict versus unavailability.
///
/// The JSON head is bounded to 4 KiB so an oversized error page cannot make
/// the coordinator parse unbounded input.
fn refusal_error(status: reqwest::StatusCode, body: &[u8]) -> SubscriptionProgressError {
    let head = &body[..body.len().min(4096)];
    let code = serde_json::from_slice::<serde_json::Value>(head)
        .ok()
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
        return error;
    }
    if status == reqwest::StatusCode::CONFLICT {
        SubscriptionProgressError::Conflict
    } else {
        SubscriptionProgressError::Unavailable
    }
}

impl SubscriptionProgressMutation {
    /// The frontier Cursor a subsequent mutation must CAS on for the
    /// member Feed it touches: the entry's Cursor for a named member
    /// Feed (`None` while the entry is unattached), the row's aggregate
    /// Cursor for exact-Feed and pure bookkeeping mutations.
    pub fn expected_frontier_cursor(&self, member_feed: Option<Uuid>) -> Option<&str> {
        match member_feed {
            Some(feed) => self
                .member_progress
                .get(&feed)
                .map(|entry| entry.cursor.as_str()),
            None => Some(self.cursor.as_str()),
        }
    }

    /// A synthesized per-Feed frontier view of a Domain-scoped row:
    /// page fetching, evidence, and record-source resolution all key off
    /// `feed_id`/`cursor`/`positions`, so callers can treat one member
    /// Feed exactly like an exact-Feed row. The view carries the row's
    /// per-Feed delta maps only when this row's last mutation named the
    /// same Feed, so a replayed request never surfaces another Feed's
    /// evidence.
    pub fn member_view(&self, feed: Uuid) -> Option<Self> {
        let entry = self.member_progress.get(&feed)?;
        let delta_is_mine = self.member_feed == Some(feed);
        Some(Self {
            feed_id: feed,
            cursor: entry.cursor.clone(),
            positions: entry.positions.clone(),
            quarantined: entry.quarantined.clone(),
            skipped: entry.skipped.clone(),
            quarantine_add: if delta_is_mine {
                self.quarantine_add.clone()
            } else {
                BTreeMap::new()
            },
            quarantine_remove: if delta_is_mine {
                self.quarantine_remove.clone()
            } else {
                BTreeMap::new()
            },
            skip_add: if delta_is_mine {
                self.skip_add.clone()
            } else {
                BTreeMap::new()
            },
            deferred: entry.deferred.clone(),
            deferred_add: if delta_is_mine {
                self.deferred_add.clone()
            } else {
                BTreeMap::new()
            },
            deferred_remove: if delta_is_mine {
                self.deferred_remove.clone()
            } else {
                BTreeMap::new()
            },
            member_progress: BTreeMap::new(),
            member_feed: None,
            ..self.clone()
        })
    }
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
        && left.member_progress == right.member_progress
        && left.member_feed == right.member_feed
        && left.tick == right.tick
        && left.lease_ops == right.lease_ops
        && left.quarantined == right.quarantined
        && left.quarantine_add == right.quarantine_add
        && left.quarantine_remove == right.quarantine_remove
        && left.skipped == right.skipped
        && left.skip_add == right.skip_add
        && left.deferred == right.deferred
        && left.deferred_add == right.deferred_add
        && left.deferred_remove == right.deferred_remove
}

/// `ownership_epoch` stamps which assignment issued a mutation; it fences
/// writes but is not decision content - `same_progress_identity` already
/// excludes it, and quorum digests must too or a stored decision and its
/// epoch-moved replay can never match commit evidence.
fn progress_decision_digest(
    mutation: &SubscriptionProgressMutation,
) -> Result<[u8; 32], SubscriptionProgressError> {
    let canonical = SubscriptionProgressMutation {
        ownership_epoch: 0,
        ..mutation.clone()
    };
    let bytes = serde_json::to_vec(&canonical)?;
    Ok(*blake3::hash(&bytes).as_bytes())
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
            Self::InvalidWindow => "lease.invalid_window",
            Self::InvalidClock => "lease.invalid_clock",
        }
    }

    fn from_code(code: &str) -> Option<Self> {
        Some(match code {
            "lease.busy" => Self::Busy,
            "lease.stale_lease" => Self::StaleLease,
            "lease.capacity" => Self::Capacity,
            "lease.invalid_window" => Self::InvalidWindow,
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
                            || committed.ownership_epoch > row.ownership_epoch
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
                            || committed.ownership_epoch > row.ownership_epoch
                    })
                    || row.prepared.as_ref().is_some_and(|prepared| {
                        prepared.subscription_id != subscription_id
                            || prepared.feed_id != row.feed_id
                            || prepared.ownership_epoch > row.ownership_epoch
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
            // The stamped epoch is the decision's history, not its scope: a
            // committed row adopted forward keeps the epoch that made it.
            if mutation.subscription_id != subscription_id
                || mutation.feed_id != feed_id
                || mutation.ownership_epoch > ownership_epoch
            {
                return Err(SubscriptionProgressError::Conflict);
            }
            if mutation.sequence == 0
                || mutation.cursor.is_empty()
                || mutation.cursor.len() > 256
                || (mutation.positions.is_empty() && mutation.member_progress.is_empty())
                || mutation.positions.len() > 128
                || mutation.positions.values().any(|value| value.len() > 256)
                || mutation.quarantined.len() > 128
                || mutation
                    .quarantined
                    .values()
                    .flatten()
                    .any(|value| value.is_empty() || value.len() > 256)
                || mutation.quarantined.values().map(Vec::len).sum::<usize>()
                    > SUBSCRIPTION_QUARANTINE_MAX_POSITIONS
                || !mutation
                    .quarantined
                    .keys()
                    .all(|range| mutation.positions.contains_key(range))
                || mutation.skipped.len() > 128
                || mutation
                    .skipped
                    .values()
                    .flatten()
                    .any(|value| value.is_empty() || value.len() > 256)
                || mutation.skipped.values().map(Vec::len).sum::<usize>()
                    > SUBSCRIPTION_SKIP_MAX_POSITIONS
                || !mutation
                    .skipped
                    .keys()
                    .all(|range| mutation.positions.contains_key(range))
                || mutation.deferred.len() > 128
                || mutation
                    .deferred
                    .values()
                    .flatten()
                    .any(|value| value.is_empty() || value.len() > 256)
                || mutation.deferred.values().map(Vec::len).sum::<usize>()
                    > SUBSCRIPTION_DEFERRED_MAX_POSITIONS
                || !mutation
                    .deferred
                    .keys()
                    .all(|range| mutation.positions.contains_key(range))
                || mutation.expected_cursor.as_ref().is_some_and(|expected| {
                    expected.is_empty()
                        || expected.len() > 256
                        || (expected == &mutation.cursor
                            && mutation.lease_ops.is_empty()
                            && (mutation.member_feed.is_some()
                                || mutation.member_progress.is_empty()))
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
            let same_committed = match (&row.committed, &committed) {
                (None, None) => true,
                (Some(existing), Some(candidate)) => same_progress_identity(existing, candidate),
                _ => false,
            };
            return if same_committed && row.members == members {
                Ok(row.committed)
            } else {
                Err(SubscriptionProgressError::Conflict)
            };
        }
        if let Some(recovered) = &committed {
            member_progress_valid(recovered, row.committed.as_ref())?;
        }
        if let Some(existing) = &row.committed {
            let Some(recovered) = &committed else {
                return Err(SubscriptionProgressError::Conflict);
            };
            if recovered.sequence < existing.sequence
                || (recovered.sequence == existing.sequence
                    && !same_progress_identity(existing, recovered))
                || (recovered.sequence == existing.sequence + 1
                    && recovered.expected_cursor.as_deref()
                        != existing.expected_frontier_cursor(recovered.member_feed))
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
            || (mutation.positions.is_empty() && mutation.member_progress.is_empty())
            || mutation.positions.len() > 128
            || mutation.positions.values().any(|value| value.len() > 256)
            || mutation.quarantined.len() > 128
            || mutation
                .quarantined
                .values()
                .flatten()
                .any(|value| value.is_empty() || value.len() > 256)
            || mutation.quarantined.values().map(Vec::len).sum::<usize>()
                > SUBSCRIPTION_QUARANTINE_MAX_POSITIONS
            || !mutation
                .quarantined
                .keys()
                .all(|range| mutation.positions.contains_key(range))
            || mutation.skipped.len() > 128
            || mutation
                .skipped
                .values()
                .flatten()
                .any(|value| value.is_empty() || value.len() > 256)
            || mutation.skipped.values().map(Vec::len).sum::<usize>()
                > SUBSCRIPTION_SKIP_MAX_POSITIONS
            || !mutation
                .skipped
                .keys()
                .all(|range| mutation.positions.contains_key(range))
            || mutation.deferred.len() > 128
            || mutation
                .deferred
                .values()
                .flatten()
                .any(|value| value.is_empty() || value.len() > 256)
            || mutation.deferred.values().map(Vec::len).sum::<usize>()
                > SUBSCRIPTION_DEFERRED_MAX_POSITIONS
            || !mutation
                .deferred
                .keys()
                .all(|range| mutation.positions.contains_key(range))
            || mutation.lease_ops.len() > SUBSCRIPTION_LEASE_MAX_OPS
            || mutation.expected_cursor.as_ref().is_some_and(|expected| {
                expected.is_empty()
                    || expected.len() > 256
                    || (expected == &mutation.cursor
                        && mutation.lease_ops.is_empty()
                        && (mutation.member_feed.is_some() || mutation.member_progress.is_empty()))
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
            digest: progress_decision_digest(&mutation)?,
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
        // Fencing is directional: the placement check upstream already
        // proved the request epoch is the authoritative assignment, so a
        // row fenced behind it ratchets forward on install below rather
        // than wedging the mutation chain while adoption catches up. A
        // mutation stamped under an older epoch than the row dies here as
        // a stale write.
        if row.ownership_epoch > mutation.ownership_epoch {
            return Err(SubscriptionProgressError::StaleEpoch);
        }
        if let Some(committed) = &row.committed {
            if committed.request_id == mutation.request_id {
                return if same_progress_identity(committed, &mutation) {
                    Ok(vote)
                } else {
                    Err(SubscriptionProgressError::Conflict)
                };
            }
        }
        if let Some(prepared) = &row.prepared {
            if same_progress_identity(prepared, &mutation) {
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
        member_progress_valid(&mutation, row.committed.as_ref())?;
        if row
            .committed
            .as_ref()
            .and_then(|prior| prior.expected_frontier_cursor(mutation.member_feed))
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
        row.ownership_epoch = mutation.ownership_epoch;
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
        let digest = progress_decision_digest(candidate)?;
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
            tracing::warn!(
                subscription = %evidence.subscription_id,
                evidence_request = %evidence.request_id,
                prepared_request = ?row.prepared.as_ref().map(|value| value.request_id),
                "Subscription progress commit found no matching prepared vote"
            );
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
            .active_subscription_progress_scope_by_id(request.mutation.subscription_id)
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
            .active_subscription_progress_scope_by_id(request.subscription_id)
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
            if progress.ownership_epoch > request.ownership_epoch {
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
            .active_subscription_progress_scope_by_id(request.subscription_id)
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
            let error = refusal_error(status, &body);
            tracing::warn!(node = %node, path, %status, code = error.code(), "Subscription progress replica refused");
            return Err(error);
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
            .map_err(|error| {
                tracing::warn!(%error, "Subscription progress placement command failed");
                SubscriptionProgressError::Unavailable
            })
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
            .map_err(|error| {
                tracing::warn!(%error, "Subscription progress placement command failed");
                SubscriptionProgressError::Unavailable
            })
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

#[derive(Clone)]
pub struct SubscriptionProgressCoordinator {
    assignment: SubscriptionProgressAssignment,
    transport: std::sync::Arc<dyn SubscriptionProgressTransport>,
}

/// The one-shot exhausted-delivery decision `member.fetch` commits when a
/// claim's durable lease epoch exceeds the declared retry budget: the page's
/// ending Cursor and positions plus the evidence set it joins.
#[derive(Clone, Debug)]
pub struct ExhaustedDelivery {
    /// The last exhausted record's Cursor — the committed frontier's new head.
    pub cursor: String,
    /// Per-range positions advanced past the exhausted page.
    pub positions: BTreeMap<RangeId, String>,
    /// The exhausted positions keyed by Active Range, routed to the
    /// `quarantined` or `skipped` evidence set by `disposition`.
    pub exhausted: BTreeMap<RangeId, Vec<String>>,
    /// Whether the evidence parks for release (quarantine) or is final (skip).
    pub disposition: crate::control::SubscriptionExhaustedDisposition,
    /// Positions the exhausted page passed that belong to other member
    /// Key windows — owed work to defer rather than park, empty for
    /// unwindowed leases.
    pub deferred_add: BTreeMap<RangeId, Vec<String>>,
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
                        if mutation.ownership_epoch > self.assignment.ownership_epoch {
                            // The replica knows a newer placement; this
                            // coordinator's assignment is stale.
                            return Err(SubscriptionProgressError::StaleEpoch);
                        }
                        // A decision committed under a prior epoch stays
                        // committed: the stamped epoch is history, and the
                        // reply envelope's row epoch already fenced the read.
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
                        Some(current) if same_progress_identity(&current, &mutation) => {
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
                Ok(Some(committed)) if same_progress_identity(&committed, &mutation) => {
                    return Ok(mutation)
                }
                Err(SubscriptionProgressError::Conflict) => {}
                Err(error) => return Err(error),
                _ => return Err(SubscriptionProgressError::AmbiguousCommit),
            }
        }
        self.apply(mutation.clone()).await?;
        match self.read_committed().await {
            Ok(Some(committed)) if same_progress_identity(&committed, &mutation) => Ok(mutation),
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
        let digest = progress_decision_digest(&mutation)?;
        let owner = &self.assignment.owner;
        let owner_reply = match self.transport.prepare(owner, mutation.clone()).await {
            Ok(reply) => reply,
            Err(SubscriptionProgressError::Unavailable) => {
                // An unreachable owner cannot arbitrate this request. Read the
                // committed trail so a stale assignment still surfaces the
                // fencing signal (e.g. a newer epoch) instead of a bare
                // quorum shortage.
                return Err(match self.read_committed().await {
                    Err(error) => error,
                    Ok(_) => SubscriptionProgressError::NoQuorum,
                });
            }
            Err(error) => return Err(error),
        };
        if owner_reply.replica != *owner
            || owner_reply.subscription_id != self.assignment.subscription_id
            || owner_reply.ownership_epoch != self.assignment.ownership_epoch
        {
            return Err(SubscriptionProgressError::InvalidAssignment);
        }
        if owner_reply.result.subscription_id != mutation.subscription_id
            || owner_reply.result.request_id != mutation.request_id
            || owner_reply.result.digest != digest
        {
            tracing::warn!(
                subscription = %mutation.subscription_id,
                vote_request = %owner_reply.result.request_id,
                mutation_request = %mutation.request_id,
                "Subscription progress owner prepare vote mismatched"
            );
            return Err(SubscriptionProgressError::Conflict);
        }
        let mut prepared = vec![(owner.clone(), digest)];
        let mut first_refusal: Option<SubscriptionProgressError> = None;
        let followers: Vec<_> = self
            .assignment
            .replicas
            .iter()
            .filter(|node| *node != owner)
            .collect();
        let replies = futures_util::future::join_all(
            followers
                .iter()
                .map(|node| self.transport.prepare(node, mutation.clone())),
        )
        .await;
        for (node, reply) in followers.into_iter().zip(replies) {
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
                        tracing::warn!(
                            %node,
                            subscription = %mutation.subscription_id,
                            vote_request = %vote.request_id,
                            mutation_request = %mutation.request_id,
                            "Subscription progress follower prepare vote mismatched"
                        );
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
                Ok(reply) if same_progress_identity(&reply.result, &mutation) => {
                    committed.push(node.clone())
                }
                Ok(reply) => {
                    tracing::warn!(
                        %node,
                        subscription = %mutation.subscription_id,
                        committed_seq = reply.result.sequence,
                        committed_request = %reply.result.request_id,
                        mutation_seq = mutation.sequence,
                        mutation_request = %mutation.request_id,
                        "Subscription progress commit reply mismatched mutation"
                    );
                    return Err(SubscriptionProgressError::Conflict);
                }
                Err(error) if lagging_witness(&error) => {}
                Err(error) => return Err(error),
            }
        }
        if committed.len() < 2 || !committed.contains(&self.assignment.owner) {
            return Err(SubscriptionProgressError::AmbiguousCommit);
        }
        Ok(committed)
    }

    /// Settles a stranded two-phase vote. A coordinator that dies between a
    /// quorum's prepare votes and its commit leaves an uncommitted residue
    /// that foreign mutations cannot supersede: replicas answer every later
    /// mutation as a lagging witness forever. When a quorum of replicas
    /// holds the identical prepared row extending the committed tip, that
    /// decision was already chosen — this finishes its commit rather than
    /// letting the residue wedge the chain until an ownership epoch ratchet
    /// happens to clear it.
    ///
    /// A residue held by a lone replica or split into divergent contents is
    /// not a chosen decision and is left for adoption-driven recovery.
    pub async fn settle_stranded_prepare(
        &self,
    ) -> Result<Option<SubscriptionProgressMutation>, SubscriptionProgressError> {
        let evidence = self.inspect_evidence().await?;
        let mut residue: Option<(
            SubscriptionProgressMutation,
            Vec<crate::active_range::StorageNodeId>,
        )> = None;
        let mut divergent = false;
        for (node, inspection) in &evidence {
            let Some(prepared) = &inspection.prepared else {
                continue;
            };
            match &mut residue {
                Some((existing, holders)) if same_progress_identity(existing, prepared) => {
                    holders.push(node.clone());
                }
                Some(_) => divergent = true,
                None => residue = Some((prepared.clone(), vec![node.clone()])),
            }
        }
        let Some((residue, holders)) = residue else {
            return Ok(None);
        };
        if divergent || holders.len() < 2 {
            return Ok(None);
        }
        // A prepared vote only ever extends the committed tip; a residue at
        // any other sequence contradicts the accept rule and is left for
        // epoch-fenced recovery rather than committed here.
        if evidence.iter().any(|(node, inspection)| {
            holders.contains(node)
                && inspection
                    .committed
                    .as_ref()
                    .is_some_and(|committed| residue.sequence != committed.sequence + 1)
        }) {
            return Ok(None);
        }
        let digest = progress_decision_digest(&residue)?;
        let first = holders
            .iter()
            .find(|node| **node == self.assignment.owner)
            .unwrap_or(&holders[0]);
        let second = holders
            .iter()
            .find(|node| *node != first)
            .expect("a residue quorum holds at least two distinct replicas");
        let commit_evidence = SubscriptionCommitEvidence {
            subscription_id: self.assignment.subscription_id,
            request_id: residue.request_id,
            votes: [(first.clone(), digest), (second.clone(), digest)],
        };
        let mut committed = 0usize;
        let replies = futures_util::future::join_all(
            holders
                .iter()
                .map(|node| self.transport.commit(node, commit_evidence.clone())),
        )
        .await;
        for reply in replies {
            match reply {
                Ok(reply) if same_progress_identity(&reply.result, &residue) => {
                    committed += 1;
                }
                Ok(_) => return Err(SubscriptionProgressError::Conflict),
                Err(error) if lagging_witness(&error) => {}
                Err(error) => return Err(error),
            }
        }
        if committed < 2 {
            return Err(SubscriptionProgressError::AmbiguousCommit);
        }
        Ok(Some(residue))
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

    /// Per-replica inspection for operations visibility. Unlike
    /// `inspect_evidence`, an unreachable, stale, or divergent replica does
    /// not fail the whole read: each assigned replica reports its own outcome
    /// so operators can see exactly which copy is behind before recovery.
    pub async fn replica_states(
        &self,
    ) -> Vec<(
        crate::active_range::StorageNodeId,
        Result<SubscriptionProgressInspection, SubscriptionProgressError>,
    )> {
        let replies = futures_util::future::join_all(self.assignment.replicas.iter().map(|node| {
            self.transport.inspect(
                node,
                self.assignment.subscription_id,
                self.assignment.ownership_epoch,
            )
        }))
        .await;
        self.assignment
            .replicas
            .iter()
            .cloned()
            .zip(replies)
            .map(|(node, reply)| {
                let result = reply.and_then(|reply| {
                    if reply.replica != node
                        || reply.subscription_id != self.assignment.subscription_id
                    {
                        return Err(SubscriptionProgressError::InvalidAssignment);
                    }
                    Ok(reply.result)
                });
                (node, result)
            })
            .collect()
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
        let stage = |stage: &'static str| {
            move |error: SubscriptionProgressError| {
                tracing::warn!(
                    %error,
                    stage,
                    subscription = %self.assignment.subscription_id,
                    owner = %self.assignment.owner,
                    ownership_epoch = self.assignment.ownership_epoch,
                    "Subscription progress owner recovery attempt failed"
                );
                error
            }
        };
        let evidence = self
            .inspect_evidence()
            .await
            .map_err(stage("inspect_evidence"))?;
        let (recovered, member_state) =
            Self::recovered_committed(&evidence).map_err(stage("recovered_committed"))?;
        let proposed_owner = self
            .recovery_owner(&evidence, recovered.as_ref())
            .map_err(stage("recovery_owner"))?;
        self.commit_owner_move(control, proposed_owner, recovered, member_state)
            .await
            .map_err(stage("commit_owner_move"))
    }

    /// Moves Subscription progress ownership to a caller-chosen replica while
    /// the current owner may still be reachable. Storage drain uses this to
    /// vacate a specific healthy Node; `recover_lost_owner` derives its own
    /// target because it runs after owner loss. A placement that already
    /// moved — a committed CAS from an earlier attempt or a racing mover —
    /// converges through `synchronize_placement` rather than erroring, so a
    /// retried handoff still adopts replicas at the live epoch.
    pub async fn handoff_owner<A: SubscriptionPlacementAuthority + ?Sized>(
        &self,
        control: &A,
        new_owner: &crate::active_range::StorageNodeId,
    ) -> Result<SubscriptionRecoveryOutcome, SubscriptionProgressError> {
        if !self.assignment.replicas.contains(new_owner) {
            return Err(SubscriptionProgressError::InvalidAssignment);
        }
        if *new_owner == self.assignment.owner {
            return self.synchronize_placement().await;
        }
        let evidence = self.inspect_evidence().await?;
        let (recovered, member_state) = Self::recovered_committed(&evidence)?;
        self.commit_owner_move(control, new_owner.clone(), recovered, member_state)
            .await
    }

    async fn commit_owner_move<A: SubscriptionPlacementAuthority + ?Sized>(
        &self,
        control: &A,
        proposed_owner: crate::active_range::StorageNodeId,
        recovered: Option<SubscriptionProgressMutation>,
        member_state: SubscriptionMemberState,
    ) -> Result<SubscriptionRecoveryOutcome, SubscriptionProgressError> {
        let next_epoch = self
            .assignment
            .ownership_epoch
            .checked_add(1)
            .ok_or(SubscriptionProgressError::Sequence)?;
        // A failed CAS is inconclusive: the placement may have committed
        // ambiguously, or a racing recovery already advanced the epoch.
        // Adopting this attempt's recovered state at whichever epoch the
        // catalog now holds would fence replicas on content a racing adoption
        // already replaced at that epoch, so report Unavailable and let the
        // caller rebuild on the live placement: the next attempt converges
        // through inspection and adoption instead of guessing.
        control
            .execute_placement_commands(vec![
                crate::control::Command::RecoverSubscriptionProgressOwner {
                    subscription_id: self.assignment.subscription_id,
                    expected_ownership_epoch: self.assignment.ownership_epoch,
                    new_owner: proposed_owner.clone(),
                },
            ])
            .await
            .map_err(|_| SubscriptionProgressError::Unavailable)?;
        let committed = recovered.map(|mut mutation| {
            mutation.ownership_epoch = next_epoch;
            mutation
        });
        let adopted = self
            .adopt_all(
                self.assignment.replicas.as_array(),
                &proposed_owner,
                next_epoch,
                committed.clone(),
                &member_state,
            )
            .await?;
        Ok(SubscriptionRecoveryOutcome {
            assignment: SubscriptionProgressAssignment {
                subscription_id: self.assignment.subscription_id,
                owner: proposed_owner,
                replicas: self.assignment.replicas.clone(),
                ownership_epoch: next_epoch,
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
            && self.assignment.replicas.contains(replacement)
        {
            // An earlier attempt's placement CAS already landed; converge by
            // re-adopting the recovered state at the live epoch.
            return self.synchronize_placement().await;
        }
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
        // Same inconclusive-error rule as the owner move: a failed CAS may
        // have committed ambiguously or lost to a racing move, and adopting
        // this attempt's recovered state at the catalog's newer epoch would
        // fence replicas on superseded content.
        control
            .execute_placement_commands(vec![
                crate::control::Command::MoveSubscriptionProgressReplica {
                    subscription_id: self.assignment.subscription_id,
                    expected_ownership_epoch: self.assignment.ownership_epoch,
                    replaced: replaced.clone(),
                    replacement: replacement.clone(),
                },
            ])
            .await
            .map_err(|_| SubscriptionProgressError::Unavailable)?;
        let committed = recovered.map(|mut mutation| {
            mutation.ownership_epoch = next_epoch;
            mutation
        });
        let adopted = self
            .adopt_all(
                new_replicas.as_array(),
                &self.assignment.owner,
                next_epoch,
                committed.clone(),
                &member_state,
            )
            .await?;
        Ok(SubscriptionRecoveryOutcome {
            assignment: SubscriptionProgressAssignment {
                subscription_id: self.assignment.subscription_id,
                owner: self.assignment.owner.clone(),
                replicas: new_replicas,
                ownership_epoch: next_epoch,
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
        // A lease-only mutation carries no member-Feed delta on Domain
        // rows and no frontier evidence anywhere: the top-level frontier
        // fields copy forward verbatim and stale deltas must not restate.
        mutation.quarantine_add = BTreeMap::new();
        mutation.quarantine_remove = BTreeMap::new();
        mutation.skip_add = BTreeMap::new();
        mutation.deferred_add = BTreeMap::new();
        mutation.deferred_remove = BTreeMap::new();
        if !mutation.member_progress.is_empty() {
            mutation.member_feed = None;
        }
        self.apply(mutation.clone()).await?;
        Ok(mutation)
    }

    /// Attach member-Feed frontier entries to a Domain-scoped row without
    /// moving any existing frontier: the mutation copies every committed
    /// field forward and only adds the named entries, so a member Feed
    /// discovered after the first join gains progress durably without a
    /// lease op or a frontier CAS on unrelated Feeds.
    pub async fn attach_member_feeds(
        &self,
        request_id: Uuid,
        tick: u64,
        additions: BTreeMap<Uuid, MemberFeedProgress>,
    ) -> Result<SubscriptionProgressMutation, SubscriptionProgressError> {
        if additions.is_empty() {
            return Err(SubscriptionProgressError::Conflict);
        }
        let current = self
            .read_committed()
            .await?
            .ok_or(SubscriptionProgressError::Conflict)?;
        // An attach must never overwrite a committed frontier:
        // reattaching an already-tracked Feed would silently rewind its
        // acknowledged positions.
        if additions
            .keys()
            .any(|feed| current.member_progress.contains_key(feed))
        {
            return Err(SubscriptionProgressError::Conflict);
        }
        let mut mutation = current.clone();
        mutation.sequence = current
            .sequence
            .checked_add(1)
            .ok_or(SubscriptionProgressError::Sequence)?;
        mutation.request_id = request_id;
        mutation.expected_cursor = Some(current.cursor.clone());
        mutation.tick = tick;
        mutation.member_feed = None;
        mutation.quarantine_add = BTreeMap::new();
        mutation.quarantine_remove = BTreeMap::new();
        mutation.skip_add = BTreeMap::new();
        mutation.deferred_add = BTreeMap::new();
        mutation.deferred_remove = BTreeMap::new();
        mutation.lease_ops = Vec::new();
        mutation.member_progress.extend(additions);
        self.apply(mutation.clone()).await?;
        Ok(mutation)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn acknowledge(
        &self,
        grant: &SubscriptionWorkLease,
        tick: u64,
        request_id: Uuid,
        cursor: String,
        positions: BTreeMap<RangeId, String>,
        quarantine: SubscriptionQuarantineDelta,
        deferred: SubscriptionDeferredDelta,
    ) -> Result<SubscriptionProgressMutation, SubscriptionProgressError> {
        self.acknowledge_inner(
            grant, tick, request_id, None, cursor, positions, quarantine, deferred,
        )
        .await
    }

    /// Acknowledge delivered positions on one member Feed of a
    /// Domain-source Subscription: the frontier CAS rides that Feed's
    /// committed entry Cursor while every other member-Feed frontier
    /// copies forward untouched.
    #[allow(clippy::too_many_arguments)]
    pub async fn acknowledge_member(
        &self,
        grant: &SubscriptionWorkLease,
        tick: u64,
        request_id: Uuid,
        member_feed: Uuid,
        cursor: String,
        positions: BTreeMap<RangeId, String>,
        quarantine: SubscriptionQuarantineDelta,
        deferred: SubscriptionDeferredDelta,
    ) -> Result<SubscriptionProgressMutation, SubscriptionProgressError> {
        self.acknowledge_inner(
            grant,
            tick,
            request_id,
            Some(member_feed),
            cursor,
            positions,
            quarantine,
            deferred,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn acknowledge_inner(
        &self,
        grant: &SubscriptionWorkLease,
        tick: u64,
        request_id: Uuid,
        member_feed: Option<Uuid>,
        cursor: String,
        positions: BTreeMap<RangeId, String>,
        quarantine: SubscriptionQuarantineDelta,
        deferred: SubscriptionDeferredDelta,
    ) -> Result<SubscriptionProgressMutation, SubscriptionProgressError> {
        let SubscriptionQuarantineDelta {
            add: quarantine_add,
            remove: quarantine_remove,
        } = quarantine;
        let SubscriptionDeferredDelta {
            add: deferred_add,
            remove: deferred_remove,
        } = deferred;
        let current = self
            .read_committed()
            .await?
            .ok_or(SubscriptionProgressError::Conflict)?;
        // Quarantine deltas fold into the cumulative set carried forward
        // from the committed frontier; only positions in ranges this ack
        // covers may be quarantined, so evidence cannot name progress the
        // member did not commit.
        if !quarantine_add
            .keys()
            .all(|range| positions.contains_key(range))
        {
            return Err(SubscriptionProgressError::Conflict);
        }
        // For a member-Feed acknowledgement the cumulative sets and the
        // CAS live on that Feed's entry; every other entry copies forward.
        let prior_entry = member_feed.map(|feed| {
            current
                .member_progress
                .get(&feed)
                .cloned()
                .ok_or(SubscriptionProgressError::Conflict)
        });
        let prior_entry = match prior_entry {
            Some(entry) => Some(entry?),
            None => None,
        };
        let mut quarantined = prior_entry
            .as_ref()
            .map(|entry| entry.quarantined.clone())
            .unwrap_or_else(|| current.quarantined.clone());
        let skipped = prior_entry
            .as_ref()
            .map(|entry| entry.skipped.clone())
            .unwrap_or_else(|| current.skipped.clone());
        for (range, cursors) in &quarantine_remove {
            if let Some(held) = quarantined.get_mut(range) {
                held.retain(|cursor| !cursors.contains(cursor));
                if held.is_empty() {
                    quarantined.remove(range);
                }
            }
        }
        for (range, cursors) in &quarantine_add {
            let held = quarantined.entry(*range).or_default();
            for cursor in cursors {
                if !held.contains(cursor) {
                    held.push(cursor.clone());
                }
            }
        }
        if quarantined.values().map(Vec::len).sum::<usize>() > SUBSCRIPTION_QUARANTINE_MAX_POSITIONS
        {
            return Err(SubscriptionProgressError::TooLarge);
        }
        // Deferred owed positions fold exactly like quarantine: withheld
        // evidence adds to the owed set while consumed evidence releases
        // it — and removal may only name positions actually owed.
        let mut deferred = prior_entry
            .as_ref()
            .map(|entry| entry.deferred.clone())
            .unwrap_or_else(|| current.deferred.clone());
        if !deferred_remove.iter().all(|(range, cursors)| {
            cursors.iter().all(|cursor| {
                deferred
                    .get(range)
                    .is_some_and(|held| held.contains(cursor))
            })
        }) {
            return Err(SubscriptionProgressError::Conflict);
        }
        for (range, cursors) in &deferred_remove {
            if let Some(held) = deferred.get_mut(range) {
                held.retain(|cursor| !cursors.contains(cursor));
                if held.is_empty() {
                    deferred.remove(range);
                }
            }
        }
        for (range, cursors) in &deferred_add {
            let held = deferred.entry(*range).or_default();
            for cursor in cursors {
                if !held.contains(cursor) {
                    held.push(cursor.clone());
                }
            }
        }
        if deferred.values().map(Vec::len).sum::<usize>() > SUBSCRIPTION_DEFERRED_MAX_POSITIONS {
            return Err(SubscriptionProgressError::TooLarge);
        }
        let mut member_progress = current.member_progress.clone();
        if let Some(feed) = member_feed {
            member_progress.insert(
                feed,
                MemberFeedProgress {
                    cursor: cursor.clone(),
                    positions: positions.clone(),
                    quarantined: quarantined.clone(),
                    skipped: skipped.clone(),
                    deferred: deferred.clone(),
                },
            );
        }
        let mutation = SubscriptionProgressMutation {
            subscription_id: current.subscription_id,
            feed_id: current.feed_id,
            ownership_epoch: current.ownership_epoch,
            sequence: current
                .sequence
                .checked_add(1)
                .ok_or(SubscriptionProgressError::Sequence)?,
            request_id,
            expected_cursor: prior_entry
                .as_ref()
                .map(|entry| entry.cursor.clone())
                .or_else(|| Some(current.cursor.clone())),
            cursor,
            positions,
            member_progress,
            member_feed,
            quarantined,
            quarantine_add,
            quarantine_remove,
            skipped,
            skip_add: BTreeMap::new(),
            deferred,
            deferred_add,
            deferred_remove,
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

    /// Commits the declared exhausted-delivery disposition for a claim that
    /// exceeded its Subscription's retry budget: `Quarantine` parks the
    /// positions in the releasable evidence set while `Skip` records them as
    /// permanently passed over; either way the frontier advances past them
    /// exactly once per claim because the mutation rides an `ApplyExhausted`
    /// lease op fencing the live grant. The lease stays held so the member
    /// continues on the next page without re-claiming.
    pub async fn apply_exhausted_delivery(
        &self,
        grant: &SubscriptionWorkLease,
        tick: u64,
        request_id: Uuid,
        member_feed: Option<Uuid>,
        decision: ExhaustedDelivery,
    ) -> Result<SubscriptionProgressMutation, SubscriptionProgressError> {
        let ExhaustedDelivery {
            cursor,
            positions,
            exhausted,
            disposition,
            deferred_add,
        } = decision;
        let current = self
            .read_committed()
            .await?
            .ok_or(SubscriptionProgressError::Conflict)?;
        if !exhausted.keys().all(|range| positions.contains_key(range)) {
            return Err(SubscriptionProgressError::Conflict);
        }
        // On a Domain row the disposition merges into the member Feed's
        // entry and the CAS rides that entry's committed Cursor.
        let prior_entry = match member_feed {
            Some(feed) => Some(
                current
                    .member_progress
                    .get(&feed)
                    .cloned()
                    .ok_or(SubscriptionProgressError::Conflict)?,
            ),
            None => None,
        };
        let mut skipped = prior_entry
            .as_ref()
            .map(|entry| entry.skipped.clone())
            .unwrap_or_else(|| current.skipped.clone());
        let mut quarantined = prior_entry
            .as_ref()
            .map(|entry| entry.quarantined.clone())
            .unwrap_or_else(|| current.quarantined.clone());
        // A position the exhausted disposition quarantines or skips is a
        // decided delivery, so it stops being owed to a Key window.
        let mut deferred = prior_entry
            .as_ref()
            .map(|entry| entry.deferred.clone())
            .unwrap_or_else(|| current.deferred.clone());
        let mut deferred_remove: BTreeMap<RangeId, Vec<String>> = BTreeMap::new();
        for (range, cursors) in &exhausted {
            if let Some(held) = deferred.get_mut(range) {
                let removed: Vec<String> = cursors
                    .iter()
                    .filter(|cursor| held.contains(*cursor))
                    .cloned()
                    .collect();
                if !removed.is_empty() {
                    held.retain(|cursor| !removed.contains(cursor));
                    deferred_remove.insert(*range, removed);
                }
                if held.is_empty() {
                    deferred.remove(range);
                }
            }
        }
        let mut skip_add = BTreeMap::new();
        let mut quarantine_add = BTreeMap::new();
        match disposition {
            crate::control::SubscriptionExhaustedDisposition::Quarantine => {
                for (range, cursors) in &exhausted {
                    let held = quarantined.entry(*range).or_default();
                    for cursor in cursors {
                        if !held.contains(cursor) {
                            held.push(cursor.clone());
                        }
                    }
                }
                if quarantined.values().map(Vec::len).sum::<usize>()
                    > SUBSCRIPTION_QUARANTINE_MAX_POSITIONS
                {
                    return Err(SubscriptionProgressError::TooLarge);
                }
                quarantine_add = exhausted;
            }
            crate::control::SubscriptionExhaustedDisposition::Skip => {
                for (range, cursors) in &exhausted {
                    let held = skipped.entry(*range).or_default();
                    for cursor in cursors {
                        if !held.contains(cursor) {
                            held.push(cursor.clone());
                        }
                    }
                }
                if skipped.values().map(Vec::len).sum::<usize>() > SUBSCRIPTION_SKIP_MAX_POSITIONS {
                    return Err(SubscriptionProgressError::TooLarge);
                }
                skip_add = exhausted;
            }
        }
        for (range, cursors) in &deferred_add {
            let held = deferred.entry(*range).or_default();
            for cursor in cursors {
                if !held.contains(cursor) {
                    held.push(cursor.clone());
                }
            }
        }
        if deferred.values().map(Vec::len).sum::<usize>() > SUBSCRIPTION_DEFERRED_MAX_POSITIONS {
            return Err(SubscriptionProgressError::TooLarge);
        }
        let mut member_progress = current.member_progress.clone();
        if let Some(feed) = member_feed {
            member_progress.insert(
                feed,
                MemberFeedProgress {
                    cursor: cursor.clone(),
                    positions: positions.clone(),
                    quarantined: quarantined.clone(),
                    skipped: skipped.clone(),
                    deferred: deferred.clone(),
                },
            );
        }
        let mutation = SubscriptionProgressMutation {
            subscription_id: current.subscription_id,
            feed_id: current.feed_id,
            ownership_epoch: current.ownership_epoch,
            sequence: current
                .sequence
                .checked_add(1)
                .ok_or(SubscriptionProgressError::Sequence)?,
            request_id,
            expected_cursor: prior_entry
                .as_ref()
                .map(|entry| entry.cursor.clone())
                .or_else(|| Some(current.cursor.clone())),
            cursor,
            positions,
            member_progress,
            member_feed,
            quarantined,
            quarantine_add,
            quarantine_remove: BTreeMap::new(),
            skipped,
            skip_add,
            deferred,
            deferred_add,
            deferred_remove,
            tick,
            lease_ops: vec![SubscriptionLeaseOp::ApplyExhausted {
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

    #[test]
    fn refusal_error_decodes_wire_codes_regardless_of_body_size() {
        // Regression: the transport previously truncated the refusal body
        // with `body.get(..4096)`, which returns `None` for any body shorter
        // than 4 KiB and collapsed every coded refusal (lease fences, stale
        // epochs) into a bare `Conflict`.
        let busy = br#"{"code":"lease.busy","error":"leased"}"#;
        let decoded = refusal_error(reqwest::StatusCode::CONFLICT, busy);
        assert!(matches!(
            decoded,
            SubscriptionProgressError::Lease(SubscriptionLeaseError::Busy)
        ));
        assert_eq!(decoded.code(), "lease.busy");

        let stale = br#"{"code":"stale_epoch","error":"old"}"#;
        assert_eq!(
            refusal_error(reqwest::StatusCode::CONFLICT, stale).code(),
            "stale_epoch"
        );
        let sequence = br#"{"code":"sequence","error":"gap"}"#;
        assert_eq!(
            refusal_error(reqwest::StatusCode::CONFLICT, sequence).code(),
            "sequence"
        );
        // The wire code wins over status when both are present.
        assert_eq!(
            refusal_error(reqwest::StatusCode::SERVICE_UNAVAILABLE, busy).code(),
            "lease.busy"
        );
        assert_eq!(
            refusal_error(reqwest::StatusCode::CONFLICT, b"plain refusal").code(),
            "conflict"
        );
        assert_eq!(
            refusal_error(reqwest::StatusCode::CONFLICT, b"{}").code(),
            "conflict"
        );
        assert_eq!(
            refusal_error(reqwest::StatusCode::BAD_GATEWAY, b"{}").code(),
            "unavailable"
        );
        // Bodies beyond the 4 KiB inspection head fall back to status.
        let mut oversized = br#"{"code":"lease.busy","pad":""#.to_vec();
        oversized.extend(std::iter::repeat_n(b' ', 5000));
        oversized.extend_from_slice(b"}");
        assert_eq!(
            refusal_error(reqwest::StatusCode::CONFLICT, &oversized).code(),
            "conflict"
        );
    }

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
            feed_id: subscription.source.feed_id().unwrap(),
            ownership_epoch: 1,
            sequence: 1,
            request_id: Uuid::from_u128(700),
            member_progress: BTreeMap::new(),
            member_feed: None,
            expected_cursor: None,
            cursor: "rf1_event".to_owned(),
            positions: BTreeMap::from([(
                RangeId::from_uuid(Uuid::from_u128(701)),
                "event".to_owned(),
            )]),

            quarantined: BTreeMap::new(),
            quarantine_add: BTreeMap::new(),
            quarantine_remove: BTreeMap::new(),
            skipped: BTreeMap::new(),
            skip_add: BTreeMap::new(),
            deferred: BTreeMap::new(),
            deferred_add: BTreeMap::new(),
            deferred_remove: BTreeMap::new(),
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
            member_progress: BTreeMap::new(),
            member_feed: None,
            expected_cursor: None,
            cursor: "rf1_one".to_owned(),
            positions: BTreeMap::from([(
                RangeId::from_uuid(Uuid::from_u128(4)),
                "record-one".to_owned(),
            )]),

            quarantined: BTreeMap::new(),
            quarantine_add: BTreeMap::new(),
            quarantine_remove: BTreeMap::new(),
            skipped: BTreeMap::new(),
            skip_add: BTreeMap::new(),
            deferred: BTreeMap::new(),
            deferred_add: BTreeMap::new(),
            deferred_remove: BTreeMap::new(),
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
        let next_vote = reopened.prepare(next.clone()).unwrap();
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
        // The fence is directional: the placement check upstream proves the
        // authoritative epoch, so the identical decision restamped under an
        // adopted epoch re-votes instead of wedging. Once the row fence
        // ratchets forward, a mutation stamped behind it still dies.
        let mut future_epoch = next.clone();
        future_epoch.ownership_epoch = 2;
        assert_eq!(
            reopened.prepare(future_epoch).unwrap().digest,
            next_vote.digest
        );
        reopened
            .adopt_recovered(
                subscription_id,
                mutation.feed_id,
                2,
                Some(mutation.clone()),
                SubscriptionMemberState::default(),
            )
            .unwrap();
        assert!(matches!(
            reopened.prepare(next),
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
    fn pacing_table_isolates_readers_and_resets_on_close() {
        let table = ReaderPacingTable::default();
        let slow = Uuid::from_u128(1);
        let healthy = Uuid::from_u128(2);
        // A session opens trusted at its declared capacity.
        let warm = table.plan(slow, 64, 64, 0.0, 1024);
        assert_eq!(warm.max_records, 64);
        // A slow Reader that never acks fills its delivered-byte budget;
        // the plan defers delivery entirely once the budget saturates.
        table.record_page(slow, 64, 2048, 64);
        let deferred = table.plan(slow, 64, 64, 0.0, 1024);
        assert_eq!(deferred.max_records, 0);
        assert!(deferred.retry_after > Duration::ZERO);
        // The saturated Reader does not squeeze the healthy one.
        let unaffected = table.plan(healthy, 64, 64, 0.0, 1024);
        assert_eq!(unaffected.max_records, 64);
        assert_eq!(unaffected.retry_after, Duration::ZERO);
        // An ack retires the byte budget; the halved grant reopens flow
        // cautiously rather than bouncing straight back to capacity.
        table.observe_ack(slow);
        let reopened = table.plan(slow, 64, 64, 0.0, 1024);
        assert!(reopened.max_records < warm.max_records && reopened.max_records > 0);
        table.remove(slow);
        // Removal drops every signal: the next plan is a fresh trusted grant.
        assert_eq!(table.plan(slow, 64, 64, 0.0, 1024).max_records, 64);
    }

    #[test]
    fn pacing_table_grows_back_only_on_sustained_full_pages() {
        let table = ReaderPacingTable::default();
        let reader = Uuid::from_u128(3);
        // The optimistic grant still defers when the byte budget is gone.
        let first = table.plan(reader, 64, 64, 0.0, 1 << 20);
        assert_eq!(first.max_records, 64);
        table.record_page(reader, 64, 2 << 20, 64);
        let deferred = table.plan(reader, 64, 64, 0.0, 1 << 20);
        assert_eq!(deferred.max_records, 0);
        assert!(deferred.retry_after > Duration::ZERO);
        // After the ack the grant starts halved, then grows only on
        // sustained full, acknowledged pages.
        table.observe_ack(reader);
        let halved = table.plan(reader, 64, 64, 0.0, 1 << 20);
        assert_eq!(halved.max_records, 32);
        let mut plan = halved;
        for _ in 0..12 {
            table.record_page(
                reader,
                plan.max_records.max(1),
                100,
                plan.max_records.max(1),
            );
            table.observe_ack(reader);
            plan = table.plan(reader, 64, 64, 0.0, 1 << 20);
        }
        assert!(plan.max_records > halved.max_records && plan.max_records <= 64);
        // Reopening with a different capacity re-baselines the grant to
        // the new session's declared bound.
        let rebased = table.plan(reader, 16, 16, 0.0, 1 << 20);
        assert_eq!(rebased.max_records, 16);
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
                .claim(first_work, first_member, 1, 100, 20, 1, None)
                .unwrap_err(),
            SubscriptionLeaseError::StaleLease
        );
        leases.fence_member(first_member, 1).unwrap();
        leases.fence_member(second_member, 1).unwrap();
        let first = leases
            .claim(first_work, first_member, 1, 100, 20, 1, None)
            .unwrap();
        assert_eq!(
            leases
                .claim(first_work, first_member, 1, 100, 20, 1, None)
                .unwrap(),
            first
        );
        assert_eq!(
            leases
                .claim(first_work, second_member, 1, 101, 20, 1, None)
                .unwrap_err(),
            SubscriptionLeaseError::Busy
        );
        assert_eq!(
            leases
                .claim(second_work, first_member, 1, 101, 20, 1, None)
                .unwrap_err(),
            SubscriptionLeaseError::Capacity
        );
        let second = leases
            .claim(second_work, second_member, 1, 102, 20, 1, None)
            .unwrap();
        assert!(leases.can_ack(&first, 103));
        assert!(leases.can_ack(&second, 103));
        leases.fence_member(first_member, 2).unwrap();
        assert!(!leases.can_ack(&first, 103));
        assert_eq!(
            leases
                .claim(first_work, first_member, 1, 103, 20, 1, None)
                .unwrap_err(),
            SubscriptionLeaseError::StaleLease
        );
        let renewed_session = leases
            .claim(first_work, first_member, 2, 103, 20, 1, None)
            .unwrap();
        assert!(renewed_session.lease_epoch > first.lease_epoch);
        assert!(!leases.can_ack(&first, 104));
        assert_eq!(
            leases.renew(&first, 104, 20).unwrap_err(),
            SubscriptionLeaseError::StaleLease
        );
        assert_eq!(
            leases
                .claim(first_work, second_member, 1, 110, 20, 1, None)
                .unwrap_err(),
            SubscriptionLeaseError::Busy
        );
        let replacement = leases
            .claim(first_work, second_member, 1, 123, 20, 2, None)
            .unwrap();
        assert!(replacement.lease_epoch > renewed_session.lease_epoch);
        assert!(!leases.can_ack(&renewed_session, 124));
        assert!(!leases.can_ack(&second, 124));
        assert_eq!(
            leases
                .claim(Uuid::from_u128(5), first_member, 2, 125, 20, 1, None)
                .unwrap_err(),
            SubscriptionLeaseError::Capacity
        );
        assert_eq!(
            leases
                .claim(first_work, second_member, 1, 120, 20, 2, None)
                .unwrap_err(),
            SubscriptionLeaseError::InvalidClock
        );
        assert_eq!(
            leases
                .claim(first_work, second_member, 1, u64::MAX, 2, 2, None)
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
            member_progress: BTreeMap::new(),
            member_feed: None,
            expected_cursor: (sequence > 1).then(|| format!("rf1_{}", sequence - 1)),
            cursor: format!("rf1_{sequence}"),
            positions: BTreeMap::from([(
                RangeId::from_uuid(Uuid::from_u128(7_000 + sequence as u128)),
                format!("position-{sequence}"),
            )]),
            quarantined: BTreeMap::new(),
            quarantine_add: BTreeMap::new(),
            quarantine_remove: BTreeMap::new(),
            skipped: BTreeMap::new(),
            skip_add: BTreeMap::new(),
            deferred: BTreeMap::new(),
            deferred_add: BTreeMap::new(),
            deferred_remove: BTreeMap::new(),
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
            key_window: None,
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
            key_window: None,
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
                key_window: None,
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
                key_window: None,
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
                key_window: None,
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
                key_window: None,
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
        let digest = progress_decision_digest(mutation).unwrap();
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

    #[test]
    fn subscription_progress_replays_identical_decisions_across_epoch_moves() {
        // A decision committed under epoch 1 keeps its stamp through the
        // epoch-2 adoption; the identical replay under the new epoch answers
        // the same vote instead of conflicting on the stamp, and commit
        // evidence still matches the stored candidate.
        let directory = tempfile::TempDir::new().unwrap();
        let replica = FjallSubscriptionProgressReplica::open(directory.path()).unwrap();
        let subscription_id = Uuid::from_u128(720);
        let feed_id = Uuid::from_u128(721);
        let committed = subscription_progress_mutation(subscription_id, feed_id, 1, 1);
        commit_direct(&replica, &committed);

        replica
            .adopt_recovered(
                subscription_id,
                feed_id,
                2,
                Some(committed.clone()),
                SubscriptionMemberState::default(),
            )
            .unwrap();

        let mut replayed = committed.clone();
        replayed.ownership_epoch = 2;
        let vote = replica.prepare(replayed.clone()).unwrap();
        assert_eq!(vote.request_id, committed.request_id);
        assert_eq!(
            replica.local_committed(subscription_id).unwrap(),
            Some(committed)
        );
        commit_direct(&replica, &replayed);
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
            feed_id: subscription.source.feed_id().unwrap(),
            ownership_epoch: 2,
            sequence: 1,
            request_id: Uuid::from_u128(9_999),
            member_progress: BTreeMap::new(),
            member_feed: None,
            expected_cursor: None,
            cursor: "rf1_recovered".to_owned(),
            positions: BTreeMap::from([(
                RangeId::from_uuid(Uuid::from_u128(9_998)),
                "position".to_owned(),
            )]),

            quarantined: BTreeMap::new(),
            quarantine_add: BTreeMap::new(),
            quarantine_remove: BTreeMap::new(),
            skipped: BTreeMap::new(),
            skip_add: BTreeMap::new(),
            deferred: BTreeMap::new(),
            deferred_add: BTreeMap::new(),
            deferred_remove: BTreeMap::new(),
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

    #[test]
    fn subscription_lease_transfer_moves_work_incrementally_and_fences_the_source() {
        let mut tracker = SubscriptionLeaseTracker::new(16);
        let alice = Uuid::from_u128(1);
        let bob = Uuid::from_u128(2);
        let work = Uuid::from_u128(9);
        tracker.fence_member(alice, 1).unwrap();
        tracker.fence_member(bob, 1).unwrap();
        let grant = tracker.claim(work, alice, 1, 10, 30, 4, None).unwrap();
        assert!(tracker.can_ack(&grant, 10));

        // Stale source evidence and unjoined targets refuse.
        assert_eq!(
            tracker.transfer(
                &SubscriptionLeaseTransfer {
                    work_id: work,
                    from_member_id: alice,
                    from_lease_epoch: 9,
                    to_member_id: bob,
                    to_member_epoch: 1,
                    lease_ticks: 30
                },
                10,
                4
            ),
            Err(SubscriptionLeaseError::StaleLease)
        );
        assert_eq!(
            tracker.transfer(
                &SubscriptionLeaseTransfer {
                    work_id: work,
                    from_member_id: alice,
                    from_lease_epoch: 1,
                    to_member_id: Uuid::from_u128(3),
                    to_member_epoch: 1,
                    lease_ticks: 30
                },
                10,
                4
            ),
            Err(SubscriptionLeaseError::StaleLease)
        );

        // The transfer bumps lease_epoch: the old grant can never
        // acknowledge, the target's new grant can.
        let moved = tracker
            .transfer(
                &SubscriptionLeaseTransfer {
                    work_id: work,
                    from_member_id: alice,
                    from_lease_epoch: 1,
                    to_member_id: bob,
                    to_member_epoch: 1,
                    lease_ticks: 30,
                },
                11,
                4,
            )
            .unwrap();
        assert_eq!(
            (moved.member_id, moved.member_epoch, moved.lease_epoch),
            (bob, 1, 2)
        );
        assert_eq!(moved.expires_at_tick, 41);
        assert!(!tracker.can_ack(&grant, 11));
        assert!(tracker.can_ack(&moved, 11));

        // A replayed transfer returns the moved grant rather than
        // demanding now-stale source evidence.
        let replayed = tracker
            .transfer(
                &SubscriptionLeaseTransfer {
                    work_id: work,
                    from_member_id: alice,
                    from_lease_epoch: 1,
                    to_member_id: bob,
                    to_member_epoch: 1,
                    lease_ticks: 30,
                },
                11,
                4,
            )
            .unwrap();
        assert_eq!(replayed, moved);

        // Transfer respects the target's per-member bound.
        for extra in 0..3u128 {
            tracker
                .claim(Uuid::from_u128(100 + extra), bob, 1, 12, 30, 4, None)
                .unwrap();
        }
        let overflow = Uuid::from_u128(200);
        let held = tracker.claim(overflow, alice, 1, 12, 30, 4, None).unwrap();
        assert_eq!(
            tracker.transfer(
                &SubscriptionLeaseTransfer {
                    work_id: overflow,
                    from_member_id: alice,
                    from_lease_epoch: held.lease_epoch,
                    to_member_id: bob,
                    to_member_epoch: 1,
                    lease_ticks: 30
                },
                13,
                4
            ),
            Err(SubscriptionLeaseError::Capacity)
        );
    }

    #[test]
    fn subscription_lease_transfer_commits_through_the_member_journal() {
        let directory = tempfile::TempDir::new().unwrap();
        let replica = FjallSubscriptionProgressReplica::open(directory.path()).unwrap();
        let subscription_id = Uuid::from_u128(620);
        let feed_id = Uuid::from_u128(621);
        let holder = Uuid::from_u128(622);
        let target = Uuid::from_u128(623);
        let work = Uuid::from_u128(624);

        // Both members join; the holder claims the work.
        let mut join = subscription_progress_mutation(subscription_id, feed_id, 1, 1);
        join.lease_ops = vec![
            SubscriptionLeaseOp::Join {
                member_id: holder,
                member_epoch: 1,
            },
            SubscriptionLeaseOp::Join {
                member_id: target,
                member_epoch: 1,
            },
            SubscriptionLeaseOp::Claim {
                work_id: work,
                member_id: holder,
                member_epoch: 1,
                key_window: None,
                lease_ticks: 30,
            },
        ];
        commit_direct(&replica, &join);

        // Incremental transfer replicates as one journal mutation.
        let mut transfer = subscription_progress_mutation(subscription_id, feed_id, 1, 2);
        transfer.lease_ops = vec![SubscriptionLeaseOp::Transfer {
            work_id: work,
            from_member_id: holder,
            from_lease_epoch: 1,
            to_member_id: target,
            to_member_epoch: 1,
            lease_ticks: 40,
        }];
        commit_direct(&replica, &transfer);
        let state = replica.local_state(subscription_id).unwrap();
        let moved = state.members.leases.get(&work).unwrap();
        assert_eq!(
            (moved.member_id, moved.member_epoch, moved.lease_epoch),
            (target, 1, 2)
        );

        // The fenced source cannot release or ack the moved lease.
        let mut stale_release = subscription_progress_mutation(subscription_id, feed_id, 1, 3);
        stale_release.tick = 30;
        stale_release.lease_ops = vec![SubscriptionLeaseOp::Release {
            work_id: work,
            member_id: holder,
            member_epoch: 1,
            lease_epoch: 1,
        }];
        assert!(matches!(
            replica.prepare(stale_release),
            Err(SubscriptionProgressError::Lease(
                SubscriptionLeaseError::StaleLease
            ))
        ));

        // A stale transfer — evidence predates the moved grant — refuses.
        let mut stale_transfer = subscription_progress_mutation(subscription_id, feed_id, 1, 3);
        stale_transfer.tick = 30;
        stale_transfer.lease_ops = vec![SubscriptionLeaseOp::Transfer {
            work_id: work,
            from_member_id: holder,
            from_lease_epoch: 1,
            to_member_id: holder,
            to_member_epoch: 1,
            lease_ticks: 10,
        }];
        assert!(replica.prepare(stale_transfer).is_err());
    }

    #[test]
    fn apply_exhausted_marks_the_live_grant_once_and_fences_stale_evidence() {
        let directory = tempfile::TempDir::new().unwrap();
        let replica = FjallSubscriptionProgressReplica::open(directory.path()).unwrap();
        let subscription_id = Uuid::from_u128(640);
        let feed_id = Uuid::from_u128(641);
        let member = Uuid::from_u128(642);
        let work = Uuid::from_u128(643);

        let mut join = subscription_progress_mutation(subscription_id, feed_id, 1, 1);
        join.lease_ops = vec![SubscriptionLeaseOp::Join {
            member_id: member,
            member_epoch: 1,
        }];
        commit_direct(&replica, &join);

        let mut claim = subscription_progress_mutation(subscription_id, feed_id, 1, 2);
        claim.expected_cursor = Some(join.cursor.clone());
        claim.cursor = join.cursor.clone();
        claim.tick = 10;
        claim.lease_ops = vec![SubscriptionLeaseOp::Claim {
            work_id: work,
            member_id: member,
            member_epoch: 1,
            key_window: None,
            lease_ticks: 30,
        }];
        commit_direct(&replica, &claim);
        assert!(
            !replica
                .local_state(subscription_id)
                .unwrap()
                .members
                .leases
                .get(&work)
                .unwrap()
                .exhausted_applied
        );

        // The policy marker flags the live grant at the same tick the fetch
        // validated against, so a lease expiring on the boundary still
        // admits the disposition the member already earned.
        let mut exhausted = subscription_progress_mutation(subscription_id, feed_id, 1, 3);
        exhausted.expected_cursor = Some(join.cursor.clone());
        exhausted.cursor = "rf1_head".to_owned();
        exhausted.positions = BTreeMap::from([(
            RangeId::from_uuid(Uuid::from_u128(7_002)),
            "position-2".to_owned(),
        )]);
        exhausted.tick = 10;
        exhausted.skip_add = BTreeMap::from([(
            RangeId::from_uuid(Uuid::from_u128(7_002)),
            vec!["poison-1".to_owned()],
        )]);
        exhausted.skipped = exhausted.skip_add.clone();
        exhausted.lease_ops = vec![SubscriptionLeaseOp::ApplyExhausted {
            work_id: work,
            member_id: member,
            member_epoch: 1,
            lease_epoch: 1,
        }];
        commit_direct(&replica, &exhausted);
        let state = replica.local_state(subscription_id).unwrap();
        assert!(state.members.leases.get(&work).unwrap().exhausted_applied);
        assert_eq!(
            state.committed.as_ref().unwrap().skipped[&RangeId::from_uuid(Uuid::from_u128(7_002))],
            vec!["poison-1".to_owned()]
        );

        // A stale epoch on the marker op refuses; a replayed marked grant is
        // fine because re-claiming builds a fresh unmarked lease.
        let mut stale = subscription_progress_mutation(subscription_id, feed_id, 1, 4);
        stale.expected_cursor = Some("rf1_head".to_owned());
        stale.cursor = "rf1_head".to_owned();
        stale.positions = exhausted.positions.clone();
        stale.tick = 15;
        stale.lease_ops = vec![SubscriptionLeaseOp::ApplyExhausted {
            work_id: work,
            member_id: member,
            member_epoch: 1,
            lease_epoch: 9,
        }];
        assert!(matches!(
            replica.prepare(stale),
            Err(SubscriptionProgressError::Lease(
                SubscriptionLeaseError::StaleLease
            ))
        ));

        // After expiry a reclaim rebuilds the grant unmarked at epoch + 1.
        let mut reclaim = subscription_progress_mutation(subscription_id, feed_id, 1, 4);
        reclaim.expected_cursor = Some("rf1_head".to_owned());
        reclaim.cursor = "rf1_head".to_owned();
        reclaim.positions = exhausted.positions.clone();
        reclaim.tick = 50;
        reclaim.lease_ops = vec![SubscriptionLeaseOp::Claim {
            work_id: work,
            member_id: member,
            member_epoch: 1,
            key_window: None,
            lease_ticks: 10,
        }];
        commit_direct(&replica, &reclaim);
        let state = replica.local_state(subscription_id).unwrap();
        let grant = state.members.leases.get(&work).unwrap();
        assert_eq!(grant.lease_epoch, 2);
        assert!(!grant.exhausted_applied);
    }

    #[test]
    fn subscription_skip_evidence_stays_bounded_and_durable() {
        let directory = tempfile::TempDir::new().unwrap();
        let replica = FjallSubscriptionProgressReplica::open(directory.path()).unwrap();
        let subscription_id = Uuid::from_u128(650);
        let feed_id = Uuid::from_u128(651);
        let range = RangeId::from_uuid(Uuid::from_u128(7_001));

        let mut first = subscription_progress_mutation(subscription_id, feed_id, 1, 1);
        first.positions = BTreeMap::from([(range, "position-1".to_owned())]);
        commit_direct(&replica, &first);

        // skipped keys must name ranges the mutation covers.
        let mut off_range = subscription_progress_mutation(subscription_id, feed_id, 1, 2);
        off_range.expected_cursor = Some(first.cursor.clone());
        off_range.positions = first.positions.clone();
        off_range.skip_add = BTreeMap::from([(
            RangeId::from_uuid(Uuid::from_u128(9_999)),
            vec!["gone".to_owned()],
        )]);
        off_range.skipped = off_range.skip_add.clone();
        assert!(matches!(
            replica.prepare(off_range),
            Err(SubscriptionProgressError::TooLarge)
        ));

        // The cumulative set stays inside its bounded evidence budget.
        let mut over = subscription_progress_mutation(subscription_id, feed_id, 1, 2);
        over.expected_cursor = Some(first.cursor.clone());
        over.positions = first.positions.clone();
        over.skipped = BTreeMap::from([(
            range,
            (0..SUBSCRIPTION_SKIP_MAX_POSITIONS + 1)
                .map(|index| format!("skip-{index}"))
                .collect(),
        )]);
        assert!(matches!(
            replica.prepare(over),
            Err(SubscriptionProgressError::TooLarge)
        ));

        let mut second = subscription_progress_mutation(subscription_id, feed_id, 1, 2);
        second.expected_cursor = Some(first.cursor.clone());
        second.positions = first.positions.clone();
        second.skip_add = BTreeMap::from([(range, vec!["poison".to_owned()])]);
        second.skipped = second.skip_add.clone();
        commit_direct(&replica, &second);
        let state = replica.local_state(subscription_id).unwrap();
        assert_eq!(
            state.committed.as_ref().unwrap().skipped[&range],
            vec!["poison".to_owned()]
        );
        drop(replica);
        let reopened = FjallSubscriptionProgressReplica::open(directory.path()).unwrap();
        assert_eq!(
            reopened
                .local_state(subscription_id)
                .unwrap()
                .committed
                .as_ref()
                .unwrap()
                .skipped[&range],
            vec!["poison".to_owned()]
        );
    }

    #[test]
    fn subscription_skip_evidence_translates_across_split_and_merge() {
        let source = RangeId::from_uuid(Uuid::from_u128(660));
        let right = RangeId::from_uuid(Uuid::from_u128(661));
        let entries = vec![
            ReaderLineageEntry {
                range_id: source,
                position: RangePosition::new(1),
                cursor: "left-record".to_owned(),
                key_token: KeyToken::from_bytes([0x10; 16]),
                ingest_time_ns: 1,
                message_id: Uuid::from_u128(1),
            },
            ReaderLineageEntry {
                range_id: source,
                position: RangePosition::new(2),
                cursor: "right-record".to_owned(),
                key_token: KeyToken::from_bytes([0x90; 16]),
                ingest_time_ns: 2,
                message_id: Uuid::from_u128(2),
            },
        ];
        let positions = BTreeMap::from([(source, "right-record".to_owned())]);
        let quarantined = BTreeMap::new();
        let skipped = BTreeMap::from([(
            source,
            vec!["left-record".to_owned(), "right-record".to_owned()],
        )]);
        let (positions, quarantined, skipped) = translate_split_subscription_frontier(
            &entries,
            source,
            right,
            KeyToken::from_bytes([0x80; 16]),
            &positions,
            &quarantined,
            &skipped,
        )
        .unwrap();
        assert_eq!(skipped[&source], vec!["left-record".to_owned()]);
        assert_eq!(skipped[&right], vec!["right-record".to_owned()]);
        assert!(quarantined.is_empty());
        assert!(positions.contains_key(&right));

        // merge re-keys the retired right child's evidence onto the left.
        let left_entries = vec![
            ReaderLineageEntry {
                range_id: source,
                position: RangePosition::new(1),
                cursor: "left-record".to_owned(),
                key_token: KeyToken::from_bytes([0x10; 16]),
                ingest_time_ns: 1,
                message_id: Uuid::from_u128(1),
            },
            ReaderLineageEntry {
                range_id: source,
                position: RangePosition::new(2),
                cursor: "left-ack".to_owned(),
                key_token: KeyToken::from_bytes([0x10; 16]),
                ingest_time_ns: 3,
                message_id: Uuid::from_u128(3),
            },
        ];
        let right_entries = vec![ReaderLineageEntry {
            range_id: right,
            position: RangePosition::new(1),
            cursor: "right-record".to_owned(),
            key_token: KeyToken::from_bytes([0x90; 16]),
            ingest_time_ns: 2,
            message_id: Uuid::from_u128(2),
        }];
        let positions = BTreeMap::from([
            (source, "left-ack".to_owned()),
            (right, "right-record".to_owned()),
        ]);
        let skipped = BTreeMap::from([
            (source, vec!["left-record".to_owned()]),
            (right, vec!["right-record".to_owned()]),
        ]);
        let (_, _, skipped) = translate_merge_subscription_frontier(
            &left_entries,
            &right_entries,
            source,
            right,
            &positions,
            &BTreeMap::new(),
            &skipped,
        )
        .unwrap();
        assert_eq!(
            skipped[&source],
            vec!["left-record".to_owned(), "right-record".to_owned()]
        );
        assert!(!skipped.contains_key(&right));
    }
    #[test]
    fn subscription_work_windows_cover_the_key_space_exactly_once() {
        // Two windowed leases each pin a window; the remaining windows
        // deal deterministically across them so every Key maps to
        // exactly one live lease.
        let mut tracker = SubscriptionLeaseTracker::new(16);
        let alice = Uuid::from_u128(1);
        let bob = Uuid::from_u128(2);
        tracker.fence_member(alice, 1).unwrap();
        tracker.fence_member(bob, 1).unwrap();
        let work_a = Uuid::from_u128(9);
        let work_b = Uuid::from_u128(10);
        tracker.claim(work_a, alice, 1, 10, 30, 4, Some(7)).unwrap();

        // The same pinned window cannot be claimed twice while live.
        let work_c = Uuid::from_u128(11);
        assert_eq!(
            tracker.claim(work_c, alice, 1, 10, 30, 4, Some(7)),
            Err(SubscriptionLeaseError::Busy)
        );
        // Out-of-range windows refuse.
        assert_eq!(
            tracker.claim(work_c, alice, 1, 10, 30, 4, Some(SUBSCRIPTION_WORK_WINDOWS)),
            Err(SubscriptionLeaseError::InvalidWindow)
        );
        tracker.claim(work_b, bob, 1, 10, 30, 4, Some(11)).unwrap();

        let coverage = subscription_work_coverage(&tracker.leases, 10);
        let mut union: BTreeSet<u16> = BTreeSet::new();
        for (work, windows) in &coverage {
            assert!(windows.contains(&tracker.leases.get(work).unwrap().key_window.unwrap()));
            for window in windows {
                assert!(union.insert(*window));
            }
        }
        assert_eq!(union.len(), SUBSCRIPTION_WORK_WINDOWS as usize);
        // Deterministic: the same committed state reproduces it exactly.
        assert_eq!(coverage, subscription_work_coverage(&tracker.leases, 10));
        // An unwindowed lease holds no coverage — the legacy contract.
        let work_d = Uuid::from_u128(12);
        tracker.claim(work_d, alice, 1, 10, 30, 4, None).unwrap();
        assert!(!subscription_work_coverage(&tracker.leases, 10).contains_key(&work_d));
    }

    #[test]
    fn subscription_work_window_reclaim_replays_and_conflicts() {
        let mut tracker = SubscriptionLeaseTracker::new(16);
        let alice = Uuid::from_u128(1);
        tracker.fence_member(alice, 1).unwrap();
        let work = Uuid::from_u128(9);
        let grant = tracker.claim(work, alice, 1, 10, 30, 4, Some(3)).unwrap();
        // Same member re-claiming the same window replays the lease.
        assert_eq!(tracker.claim(work, alice, 1, 11, 30, 4, Some(3)), Ok(grant));
        // Same work_id under a different window is a competing claim.
        assert_eq!(
            tracker.claim(work, alice, 1, 11, 30, 4, Some(4)),
            Err(SubscriptionLeaseError::Busy)
        );
        // Expiry frees the pinned window for another claimant.
        let other = Uuid::from_u128(10);
        assert_eq!(
            tracker.claim(other, alice, 1, 41, 30, 4, Some(3)),
            Ok(tracker.leases.get(&other).unwrap().clone())
        );
    }
}
