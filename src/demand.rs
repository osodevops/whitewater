use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use crate::active_range::{KeyToken, RangeId};

use serde::{Deserialize, Serialize};

#[derive(Clone, Default)]
pub struct DemandMetrics {
    inner: Arc<DemandCounters>,
}

#[derive(Default)]
struct DemandCounters {
    requests_total: AtomicU64,
    requests_in_flight: AtomicU64,
    appends_total: AtomicU64,
    append_bytes_total: AtomicU64,
    reads_total: AtomicU64,
    records_read_total: AtomicU64,
    range_appends: Mutex<BTreeMap<RangeId, RangeAppendWindow>>,
    range_totals: Mutex<BTreeMap<RangeId, RangeAppendWindow>>,
    hot_keys: Mutex<BTreeMap<RangeId, (HotKeyEvidence, std::time::Instant)>>,
    pipe_drives: Mutex<BTreeMap<uuid::Uuid, PipeDriveTelemetry>>,
    subscription_traffic: Mutex<BTreeMap<uuid::Uuid, SubscriptionTrafficTelemetry>>,
    reader_traffic: Mutex<BTreeMap<uuid::Uuid, ReaderTrafficTelemetry>>,
    index_queries: Mutex<BTreeMap<uuid::Uuid, IndexQueryTelemetry>>,
    range_reads: Mutex<BTreeMap<RangeId, RangeReadTotals>>,
    range_replication: Mutex<BTreeMap<RangeId, RangeReplicationTotals>>,
    feed_moves: Mutex<BTreeMap<uuid::Uuid, FeedMoveTotals>>,
}

#[derive(Default)]
struct RangeAppendWindow {
    records: u64,
    bytes: u64,
    key_counts: BTreeMap<KeyToken, KeyDemand>,
    untracked_records: u64,
    untracked_bytes: u64,
}

/// Bounded per-Key append demand within one range window.
#[derive(Clone, Copy, Default)]
struct KeyDemand {
    records: u64,
    bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangePressureSample {
    pub range_id: RangeId,
    pub records: u64,
    pub bytes: u64,
    pub split_token: Option<KeyToken>,
    /// Highest-demand Keys observed in the window, ordered by record share.
    /// Bounded so per-range fan-out stays small on the internal wire.
    #[serde(default)]
    pub top_keys: Vec<KeyPressureShare>,
    /// Smallest KeyToken above the dominant Key seen in the window: the
    /// isolation boundary that would put the hot Key at a range edge.
    #[serde(default)]
    pub dominant_next_token: Option<KeyToken>,
    /// Demand from Keys beyond the bounded per-Key tracking budget; kept
    /// separate so dominant-share evidence never silently under-counts.
    #[serde(default)]
    pub untracked_records: u64,
    #[serde(default)]
    pub untracked_bytes: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct KeyPressureShare {
    pub token: KeyToken,
    pub records: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct DemandSnapshot {
    pub requests_total: u64,
    pub requests_in_flight: u64,
    pub appends_total: u64,
    pub append_bytes_total: u64,
    pub reads_total: u64,
    pub records_read_total: u64,
}

pub struct RequestGuard {
    metrics: DemandMetrics,
}

impl DemandMetrics {
    pub fn begin_request(&self) -> RequestGuard {
        self.inner.requests_total.fetch_add(1, Ordering::Relaxed);
        self.inner
            .requests_in_flight
            .fetch_add(1, Ordering::Relaxed);
        RequestGuard {
            metrics: self.clone(),
        }
    }

    pub fn record_append(&self, bytes: usize) {
        self.inner.appends_total.fetch_add(1, Ordering::Relaxed);
        self.inner
            .append_bytes_total
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn record_range_append(&self, range_id: RangeId, key: &[u8], bytes: usize) {
        let token = KeyToken::from_key(key);
        for counters in [&self.inner.range_appends, &self.inner.range_totals] {
            if let Ok(mut ranges) = counters.lock() {
                let window = ranges.entry(range_id).or_default();
                window.records = window.records.saturating_add(1);
                window.bytes = window.bytes.saturating_add(bytes as u64);
                if !window.key_counts.contains_key(&token)
                    && window.key_counts.len() >= HOT_KEY_TRACKED_KEYS
                {
                    window.untracked_records = window.untracked_records.saturating_add(1);
                    window.untracked_bytes = window.untracked_bytes.saturating_add(bytes as u64);
                } else {
                    let demand = window.key_counts.entry(token).or_default();
                    demand.records = demand.records.saturating_add(1);
                    demand.bytes = demand.bytes.saturating_add(bytes as u64);
                }
            }
        }
    }

    pub fn take_range_pressure_samples(&self) -> Vec<RangePressureSample> {
        let Ok(mut ranges) = self.inner.range_appends.lock() else {
            return Vec::new();
        };
        std::mem::take(&mut *ranges)
            .into_iter()
            .map(|(range_id, window)| pressure_sample(range_id, &window))
            .collect()
    }

    pub fn range_pressure_totals(&self) -> Vec<RangePressureSample> {
        let Ok(ranges) = self.inner.range_totals.lock() else {
            return Vec::new();
        };
        ranges
            .iter()
            .map(|(range_id, window)| pressure_sample(*range_id, window))
            .collect()
    }

    /// Register a sustained hot-Key flag for a range; called by the
    /// autoscaler when a `HotKeyTracker` flags dominant demand.
    pub fn set_hot_key(&self, evidence: HotKeyEvidence) {
        if let Ok(mut hot) = self.inner.hot_keys.lock() {
            hot.insert(evidence.range_id, (evidence, std::time::Instant::now()));
        }
    }

    /// Lift the hot-Key flag for a range after sustained sub-threshold share.
    pub fn clear_hot_key(&self, range_id: RangeId) {
        if let Ok(mut hot) = self.inner.hot_keys.lock() {
            hot.remove(&range_id);
        }
    }

    /// Currently flagged hot Keys across all ranges on this Node. Flags are
    /// refreshed by every dominant window; a flag whose range goes idle
    /// expires after `HOT_KEY_EVIDENCE_TTL` rather than reporting stale
    /// dominance forever.
    pub fn hot_keys(&self) -> Vec<HotKeyEvidence> {
        self.inner
            .hot_keys
            .lock()
            .map(|hot| {
                hot.values()
                    .filter(|(_, seen)| seen.elapsed() <= HOT_KEY_EVIDENCE_TTL)
                    .map(|(evidence, _)| *evidence)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Flagged evidence for one range, if any and still fresh.
    pub fn hot_key(&self, range_id: RangeId) -> Option<HotKeyEvidence> {
        self.inner.hot_keys.lock().ok().and_then(|hot| {
            hot.get(&range_id).and_then(|(evidence, seen)| {
                (seen.elapsed() <= HOT_KEY_EVIDENCE_TTL).then_some(*evidence)
            })
        })
    }

    /// Whether `key`'s token is the flagged hot Key for `range_id`.
    pub fn is_hot_key(&self, range_id: RangeId, key: &[u8]) -> bool {
        self.hot_key(range_id)
            .is_some_and(|hot| hot.token == KeyToken::from_key(key))
    }

    /// Fold one finished drive cycle into the Pipe's Node-local
    /// attribution row; counters saturate rather than wrap so a
    /// long-lived Node never reports negative demand.
    pub fn record_pipe_drive(&self, pipe_id: uuid::Uuid, sample: PipeDriveSample) {
        let Ok(mut drives) = self.inner.pipe_drives.lock() else {
            return;
        };
        let telemetry = drives.entry(pipe_id).or_default();
        telemetry.drives = telemetry.drives.saturating_add(1);
        match &sample.result {
            PipeDriveResult::Applied => {
                telemetry.applied = telemetry.applied.saturating_add(1);
                telemetry.last_error = None;
            }
            PipeDriveResult::Idle => {
                telemetry.idle = telemetry.idle.saturating_add(1);
                telemetry.last_error = None;
            }
            PipeDriveResult::Failed(message) => {
                telemetry.failed = telemetry.failed.saturating_add(1);
                telemetry.last_error = Some(message.clone());
            }
        }
        telemetry.outputs = telemetry.outputs.saturating_add(sample.outputs);
        telemetry.state_mutations = telemetry
            .state_mutations
            .saturating_add(sample.state_mutations);
        telemetry.records_fetched = telemetry.records_fetched.saturating_add(sample.records);
        telemetry.fetch_bytes = telemetry.fetch_bytes.saturating_add(sample.fetch_bytes);
        telemetry.records_refused = telemetry.records_refused.saturating_add(sample.refused);
        telemetry.emissions = telemetry.emissions.saturating_add(sample.emissions);
        telemetry.drive_wall_ns_total =
            telemetry.drive_wall_ns_total.saturating_add(sample.wall_ns);
        telemetry.last_drive_wall_ns = sample.wall_ns;
        telemetry.last_page_records = sample.records;
        telemetry.last_page_limit = sample.page_limit;
        telemetry.pane_rows_last = sample.pane_rows;
        telemetry.pane_bytes_last = sample.pane_bytes;
        telemetry.last_drive_at_ns = Some(sample.sampled_at_ns);
    }

    /// A consistent snapshot of per-Pipe attribution rows on this Node.
    pub fn pipe_drive_telemetry(&self) -> BTreeMap<uuid::Uuid, PipeDriveTelemetry> {
        self.inner
            .pipe_drives
            .lock()
            .map(|drives| drives.clone())
            .unwrap_or_default()
    }

    /// Drop attribution rows for Pipes that no longer exist so a
    /// churned declaration cannot grow the map unboundedly.
    pub fn prune_pipe_drives(&self, live: &std::collections::BTreeSet<uuid::Uuid>) {
        if let Ok(mut drives) = self.inner.pipe_drives.lock() {
            drives.retain(|pipe_id, _| live.contains(pipe_id));
        }
    }

    /// Fold one successfully served Subscription member/admin request into
    /// the durable Node-local attribution row. Replayed request identities
    /// count too: the request still cost a quorum read even when it applied
    /// no new mutation.
    pub fn record_subscription_op(
        &self,
        subscription_id: uuid::Uuid,
        sample: SubscriptionTrafficSample,
    ) {
        let Ok(mut traffic) = self.inner.subscription_traffic.lock() else {
            return;
        };
        let telemetry = traffic.entry(subscription_id).or_default();
        telemetry.requests = telemetry.requests.saturating_add(1);
        match sample.op {
            SubscriptionTrafficOp::Join => telemetry.joins = telemetry.joins.saturating_add(1),
            SubscriptionTrafficOp::Claim => telemetry.claims = telemetry.claims.saturating_add(1),
            SubscriptionTrafficOp::Renew => telemetry.renews = telemetry.renews.saturating_add(1),
            SubscriptionTrafficOp::Release => {
                telemetry.releases = telemetry.releases.saturating_add(1)
            }
            SubscriptionTrafficOp::Transfer => {
                telemetry.transfers = telemetry.transfers.saturating_add(1)
            }
            SubscriptionTrafficOp::Fetch => {
                telemetry.fetches = telemetry.fetches.saturating_add(1);
                telemetry.fetch_records = telemetry.fetch_records.saturating_add(sample.records);
            }
            SubscriptionTrafficOp::Ack => {
                telemetry.acks = telemetry.acks.saturating_add(1);
                telemetry.ack_ranges = telemetry.ack_ranges.saturating_add(sample.ranges);
                telemetry.quarantined_records = telemetry
                    .quarantined_records
                    .saturating_add(sample.quarantine_add);
                telemetry.released_quarantined_records = telemetry
                    .released_quarantined_records
                    .saturating_add(sample.quarantine_remove);
            }
            SubscriptionTrafficOp::Seek => telemetry.seeks = telemetry.seeks.saturating_add(1),
        }
        telemetry.members_active = sample.members;
        telemetry.last_op = Some(sample.op.label());
        telemetry.last_op_at_ns = Some(sample.sampled_at_ns);
    }

    /// A consistent snapshot of per-Subscription attribution rows on this
    /// Node.
    pub fn subscription_traffic(&self) -> BTreeMap<uuid::Uuid, SubscriptionTrafficTelemetry> {
        self.inner
            .subscription_traffic
            .lock()
            .map(|traffic| traffic.clone())
            .unwrap_or_default()
    }

    /// Drop attribution rows for Subscriptions that no longer exist.
    pub fn prune_subscription_traffic(&self, live: &std::collections::BTreeSet<uuid::Uuid>) {
        if let Ok(mut traffic) = self.inner.subscription_traffic.lock() {
            traffic.retain(|subscription_id, _| live.contains(subscription_id));
        }
    }

    /// Fold one successfully served Reader session request into the
    /// Node-local attribution row. A replayed fetch identity counts too:
    /// the replay still costs a committed-frontier read.
    pub fn record_reader_op(&self, reader_id: uuid::Uuid, sample: ReaderTrafficSample) {
        let Ok(mut traffic) = self.inner.reader_traffic.lock() else {
            return;
        };
        let telemetry = traffic.entry(reader_id).or_default();
        telemetry.requests = telemetry.requests.saturating_add(1);
        match sample.op {
            ReaderTrafficOp::Open => telemetry.opens = telemetry.opens.saturating_add(1),
            ReaderTrafficOp::Fetch => {
                telemetry.fetches = telemetry.fetches.saturating_add(1);
                telemetry.fetch_records = telemetry.fetch_records.saturating_add(sample.records);
            }
            ReaderTrafficOp::Ack => telemetry.acks = telemetry.acks.saturating_add(1),
            ReaderTrafficOp::Close => telemetry.closes = telemetry.closes.saturating_add(1),
        }
        telemetry.last_op = Some(sample.op.label());
        telemetry.last_op_at_ns = Some(sample.sampled_at_ns);
    }

    /// A consistent snapshot of per-Reader attribution rows on this Node.
    pub fn reader_traffic(&self) -> BTreeMap<uuid::Uuid, ReaderTrafficTelemetry> {
        self.inner
            .reader_traffic
            .lock()
            .map(|traffic| traffic.clone())
            .unwrap_or_default()
    }

    /// Drop attribution rows for Readers that no longer exist.
    pub fn prune_reader_traffic(&self, live: &std::collections::BTreeSet<uuid::Uuid>) {
        if let Ok(mut traffic) = self.inner.reader_traffic.lock() {
            traffic.retain(|reader_id, _| live.contains(reader_id));
        }
    }

    /// Fold one Index query this Node materially served into its
    /// Node-local attribution row — whether the request arrived publicly
    /// or peer-forwarded, the answering replica did the posting scan.
    pub fn record_index_query(&self, index_id: uuid::Uuid, sample: IndexQuerySample) {
        let Ok(mut queries) = self.inner.index_queries.lock() else {
            return;
        };
        let telemetry = queries.entry(index_id).or_default();
        telemetry.queries = telemetry.queries.saturating_add(1);
        match sample.mode {
            IndexQueryMode::Exact => {
                telemetry.exact_queries = telemetry.exact_queries.saturating_add(1)
            }
            IndexQueryMode::Prefix => {
                telemetry.prefix_queries = telemetry.prefix_queries.saturating_add(1)
            }
            IndexQueryMode::Range => {
                telemetry.range_queries = telemetry.range_queries.saturating_add(1)
            }
        }
        telemetry.rows_returned = telemetry.rows_returned.saturating_add(sample.rows);
        if sample.truncated {
            telemetry.truncated_queries = telemetry.truncated_queries.saturating_add(1);
        }
        if sample.behind {
            telemetry.served_behind = telemetry.served_behind.saturating_add(1);
        }
        telemetry.last_mode = Some(sample.mode.label());
        telemetry.last_query_at_ns = Some(sample.sampled_at_ns);
    }

    /// A consistent snapshot of per-Index attribution rows on this Node.
    pub fn index_query_telemetry(&self) -> BTreeMap<uuid::Uuid, IndexQueryTelemetry> {
        self.inner
            .index_queries
            .lock()
            .map(|queries| queries.clone())
            .unwrap_or_default()
    }

    /// Drop attribution rows for Indexes that no longer exist.
    pub fn prune_index_queries(&self, live: &std::collections::BTreeSet<uuid::Uuid>) {
        if let Ok(mut queries) = self.inner.index_queries.lock() {
            queries.retain(|index_id, _| live.contains(index_id));
        }
    }

    /// Fold one committed range page this replica served into its
    /// cumulative read-attribution counter.
    pub fn record_range_read(&self, range_id: RangeId, records: usize, bytes: u64) {
        if let Ok(mut reads) = self.inner.range_reads.lock() {
            let totals = reads.entry(range_id).or_default();
            totals.pages = totals.pages.saturating_add(1);
            totals.records = totals.records.saturating_add(records as u64);
            totals.bytes = totals.bytes.saturating_add(bytes);
        }
    }

    /// Cumulative served-read rows keyed by RangeId on this Node.
    pub fn range_read_totals(&self) -> BTreeMap<RangeId, RangeReadTotals> {
        self.inner
            .range_reads
            .lock()
            .map(|reads| reads.clone())
            .unwrap_or_default()
    }

    /// Fold one replica-append frame this follower accepted into its
    /// cumulative replication-attribution counter. `wire_bytes` is the
    /// encoded frame size — what replication actually moved on the wire.
    pub fn record_range_replication(&self, range_id: RangeId, wire_bytes: usize) {
        if let Ok(mut replication) = self.inner.range_replication.lock() {
            let totals = replication.entry(range_id).or_default();
            totals.frames = totals.frames.saturating_add(1);
            totals.wire_bytes = totals.wire_bytes.saturating_add(wire_bytes as u64);
        }
    }

    /// Fold one accepted replica commit into the range's replication
    /// counter — a quorum write with no frame payload.
    pub fn record_range_replication_commit(&self, range_id: RangeId) {
        if let Ok(mut replication) = self.inner.range_replication.lock() {
            let totals = replication.entry(range_id).or_default();
            totals.commits = totals.commits.saturating_add(1);
        }
    }

    /// Cumulative replication-receive rows keyed by RangeId on this Node.
    pub fn range_replication_totals(&self) -> BTreeMap<RangeId, RangeReplicationTotals> {
        self.inner
            .range_replication
            .lock()
            .map(|replication| replication.clone())
            .unwrap_or_default()
    }

    /// Fold one completed ownership/follower move into the Feed's
    /// cumulative movement-attribution counter. `skipped_records` counts
    /// positions the copy resumed past — evidence the move did not have to
    /// re-transfer.
    pub fn record_feed_move(
        &self,
        feed_id: uuid::Uuid,
        records: u64,
        bytes: u64,
        skipped_records: u64,
    ) {
        if let Ok(mut moves) = self.inner.feed_moves.lock() {
            let totals = moves.entry(feed_id).or_default();
            totals.moves = totals.moves.saturating_add(1);
            totals.records = totals.records.saturating_add(records);
            totals.bytes = totals.bytes.saturating_add(bytes);
            totals.skipped_records = totals.skipped_records.saturating_add(skipped_records);
        }
    }

    /// Cumulative movement rows keyed by FeedId on this Node.
    pub fn feed_move_totals(&self) -> BTreeMap<uuid::Uuid, FeedMoveTotals> {
        self.inner
            .feed_moves
            .lock()
            .map(|moves| moves.clone())
            .unwrap_or_default()
    }

    /// Release Feed/Range attribution rows whose logical resource no longer
    /// exists in the catalog, so Feed/Range churn cannot grow the maps
    /// without bound.
    pub fn prune_feed_demand(
        &self,
        live_feeds: &std::collections::BTreeSet<uuid::Uuid>,
        live_ranges: &std::collections::BTreeSet<RangeId>,
    ) {
        if let Ok(mut reads) = self.inner.range_reads.lock() {
            reads.retain(|range_id, _| live_ranges.contains(range_id));
        }
        if let Ok(mut replication) = self.inner.range_replication.lock() {
            replication.retain(|range_id, _| live_ranges.contains(range_id));
        }
        if let Ok(mut totals) = self.inner.range_totals.lock() {
            totals.retain(|range_id, _| live_ranges.contains(range_id));
        }
        if let Ok(mut moves) = self.inner.feed_moves.lock() {
            moves.retain(|feed_id, _| live_feeds.contains(feed_id));
        }
    }

    pub fn record_read(&self, records: usize) {
        self.inner.reads_total.fetch_add(1, Ordering::Relaxed);
        self.inner
            .records_read_total
            .fetch_add(records as u64, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> DemandSnapshot {
        DemandSnapshot {
            requests_total: self.inner.requests_total.load(Ordering::Relaxed),
            requests_in_flight: self.inner.requests_in_flight.load(Ordering::Relaxed),
            appends_total: self.inner.appends_total.load(Ordering::Relaxed),
            append_bytes_total: self.inner.append_bytes_total.load(Ordering::Relaxed),
            reads_total: self.inner.reads_total.load(Ordering::Relaxed),
            records_read_total: self.inner.records_read_total.load(Ordering::Relaxed),
        }
    }
}

/// One Pipe drive cycle's measured cost on this Node: input folded, the
/// outputs/state mutations the committed effect declared, records
/// diverted into window sentinel rows (`LIMIT` refusals and lateness
/// quarantine), emissions, the fetched page's window-pane state
/// estimate, and wall time. `page_limit` records the bound the page was
/// fetched under so a saturated page marks the Pipe as backfilling.
#[derive(Clone, Debug, Default)]
pub struct PipeDriveSample {
    pub result: PipeDriveResult,
    pub outputs: u64,
    pub state_mutations: u64,
    pub records: u64,
    pub fetch_bytes: u64,
    pub refused: u64,
    pub emissions: u64,
    pub pane_rows: u64,
    pub pane_bytes: u64,
    pub page_limit: u64,
    pub wall_ns: u64,
    pub sampled_at_ns: i64,
}

/// How one drive cycle ended: committed an effect, found the input
/// caught up, or failed closed (message kept for the last-error field).
#[derive(Clone, Debug)]
pub enum PipeDriveResult {
    Applied,
    Idle,
    Failed(String),
}

impl Default for PipeDriveResult {
    fn default() -> Self {
        Self::Idle
    }
}

/// Per-Pipe drive attribution on this Node since process start — the
/// honest scope, because a compute-lease move restarts driving on the
/// new owner rather than pretending counters are portable. Domain
/// rollups sum these rows; an operator reading `pane_*_last` gets the
/// serving replica's most recent window-state estimate.
#[derive(Clone, Debug, Default, Serialize)]
pub struct PipeDriveTelemetry {
    pub drives: u64,
    pub applied: u64,
    pub idle: u64,
    pub failed: u64,
    pub outputs: u64,
    pub state_mutations: u64,
    pub records_fetched: u64,
    pub fetch_bytes: u64,
    pub records_refused: u64,
    pub emissions: u64,
    pub pane_rows_last: u64,
    pub pane_bytes_last: u64,
    pub last_page_records: u64,
    pub last_page_limit: u64,
    pub drive_wall_ns_total: u64,
    pub last_drive_wall_ns: u64,
    pub last_drive_at_ns: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// One served member/admin request against a Subscription — the op plus
/// the evidence that rides it (delivered records, acked ranges, quarantine
/// deltas, and the member gauge observed when it committed).
pub struct SubscriptionTrafficSample {
    pub op: SubscriptionTrafficOp,
    pub records: u64,
    pub ranges: u64,
    pub quarantine_add: u64,
    pub quarantine_remove: u64,
    pub members: u64,
    pub sampled_at_ns: i64,
}

/// The member/admin verbs a Subscription serves.
#[derive(Clone, Copy, Debug)]
pub enum SubscriptionTrafficOp {
    Join,
    Claim,
    Renew,
    Release,
    Transfer,
    Fetch,
    Ack,
    Seek,
}

impl SubscriptionTrafficOp {
    pub fn label(self) -> &'static str {
        match self {
            Self::Join => "join",
            Self::Claim => "claim",
            Self::Renew => "renew",
            Self::Release => "release",
            Self::Transfer => "transfer",
            Self::Fetch => "fetch",
            Self::Ack => "ack",
            Self::Seek => "seek",
        }
    }
}

/// Per-Subscription member-traffic attribution on this Node since process
/// start — the honest scope, because progress-placement movement serves
/// members on the new owner rather than pretending counters are portable.
/// `requests` counts every successfully served member/admin request,
/// including request-id replays that apply no new mutation.
#[derive(Clone, Debug, Default, Serialize)]
pub struct SubscriptionTrafficTelemetry {
    pub requests: u64,
    pub joins: u64,
    pub claims: u64,
    pub renews: u64,
    pub releases: u64,
    pub transfers: u64,
    pub fetches: u64,
    pub fetch_records: u64,
    pub acks: u64,
    pub ack_ranges: u64,
    pub quarantined_records: u64,
    pub released_quarantined_records: u64,
    pub seeks: u64,
    pub members_active: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_op: Option<&'static str>,
    pub last_op_at_ns: Option<i64>,
}

/// One served Reader session request — the op plus the evidence that
/// rides it (delivered records on a fetch).
pub struct ReaderTrafficSample {
    pub op: ReaderTrafficOp,
    pub records: u64,
    pub sampled_at_ns: i64,
}

/// The session verbs a named Reader serves.
#[derive(Clone, Copy, Debug)]
pub enum ReaderTrafficOp {
    Open,
    Fetch,
    Ack,
    Close,
}

impl ReaderTrafficOp {
    pub fn label(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Fetch => "fetch",
            Self::Ack => "ack",
            Self::Close => "close",
        }
    }
}

/// Per-Reader session-traffic attribution on this Node since process
/// start — the honest scope, because an open session may fetch through
/// any ingress and each request is attributed where it was served.
/// `requests` counts every successfully served session request, including
/// fetch replays that return the original page.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ReaderTrafficTelemetry {
    pub requests: u64,
    pub opens: u64,
    pub fetches: u64,
    pub fetch_records: u64,
    pub acks: u64,
    pub closes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_op: Option<&'static str>,
    pub last_op_at_ns: Option<i64>,
}

/// One Index query the serving Node answered — mode plus the evidence
/// that rides it (rows returned, whether the limit truncated, and whether
/// the postings lagged the activation frontier on a non-strict read).
pub struct IndexQuerySample {
    pub mode: IndexQueryMode,
    pub rows: u64,
    pub truncated: bool,
    pub behind: bool,
    pub sampled_at_ns: i64,
}

/// The Index read shapes the wire contract exposes.
#[derive(Clone, Copy, Debug)]
pub enum IndexQueryMode {
    Exact,
    Prefix,
    Range,
}

impl IndexQueryMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Prefix => "prefix",
            Self::Range => "range",
        }
    }
}

/// Cumulative committed-read service on one Active Range on this Node —
/// the storing replica that answered, not the ingress the client reached.
#[derive(Clone, Debug, Default, Serialize)]
pub struct RangeReadTotals {
    pub pages: u64,
    pub records: u64,
    /// Committed frame bytes served — the replica's read egress evidence.
    pub bytes: u64,
}

/// Cumulative replica-append receive service on one Active Range on this
/// follower Node. `wire_bytes` counts encoded frame bytes — what the
/// replication protocol actually moved to this replica.
#[derive(Clone, Debug, Default, Serialize)]
pub struct RangeReplicationTotals {
    pub frames: u64,
    pub wire_bytes: u64,
    /// Accepted replica commits — frame-free quorum writes.
    pub commits: u64,
}

/// Cumulative ownership/follower movement evidence for one Feed on this
/// Node — records and bytes the completed move transferred.
#[derive(Clone, Debug, Default, Serialize)]
pub struct FeedMoveTotals {
    pub moves: u64,
    pub records: u64,
    pub bytes: u64,
    /// Committed positions staged copies resumed past instead of
    /// re-transferring.
    pub skipped_records: u64,
}

/// Per-Index query attribution on this Node since process start — the
/// honest scope, because a peer-forwarded read counts on the replica that
/// served it rather than the Node the client happened to reach.
/// `served_behind` counts non-strict reads answered from postings that
/// still lagged the activation frontier — evidence operators need when a
/// build looks slow.
#[derive(Clone, Debug, Default, Serialize)]
pub struct IndexQueryTelemetry {
    pub queries: u64,
    pub exact_queries: u64,
    pub prefix_queries: u64,
    pub range_queries: u64,
    pub rows_returned: u64,
    pub truncated_queries: u64,
    pub served_behind: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_mode: Option<&'static str>,
    pub last_query_at_ns: Option<i64>,
}

/// Per-range bounded distinct-Key tracking budget. Windows holding more
/// distinct Keys funnel the overflow into untracked demand so hot-Key share
/// evidence stays honest without unbounded memory.
const HOT_KEY_TRACKED_KEYS: usize = 512;

/// Top-Key evidence carried per pressure sample.
const HOT_KEY_SAMPLE_TOP: usize = 8;

/// A Key must hold at least this share of a range window's records to be a
/// dominant-key candidate (basis points; 50%).
pub const HOT_KEY_DOMINANT_SHARE_BPS: u32 = 5_000;

/// A flagged Key must fall below this share for the full clear window before
/// its isolation is lifted (basis points; 30%) — hysteresis keeps flags from
/// flapping on a key hovering near the dominant threshold.
pub const HOT_KEY_CLEAR_SHARE_BPS: u32 = 3_000;

/// A window must hold at least this many records before its dominance share
/// counts as evidence either way: a nearly idle range has no hot Key, and a
/// sparse window should neither flag nor clear one.
pub const HOT_KEY_MIN_WINDOW_RECORDS: u64 = 10;

/// A hot-Key flag expires after this much idle time: flag clearing requires
/// fresh windows, so a range that simply stops receiving appends would
/// otherwise keep a stale flag forever.
pub const HOT_KEY_EVIDENCE_TTL: std::time::Duration = std::time::Duration::from_secs(120);

/// Sustained per-Key dominance evidence for one Active Range.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotKeyEvidence {
    pub range_id: RangeId,
    /// Token of the flagged Key. The raw Key bytes never leave the append
    /// path; operators see the stable token, not application data.
    pub token: KeyToken,
    /// Share of the most recent window's records, in basis points.
    pub share_bps: u32,
    /// Range window records behind the latest evidence refresh.
    pub window_records: u64,
    /// Latest window bytes attributed to the flagged Key.
    pub window_bytes: u64,
    /// Smallest observed token above the flagged Key, when another Key has
    /// been seen — the boundary an isolating split would need.
    pub next_token: Option<KeyToken>,
}

/// Transition emitted when a range's hot-Key state changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HotKeyObservation {
    /// No state change.
    Unchanged,
    /// Dominance was sustained long enough to flag the Key.
    Flagged(HotKeyEvidence),
    /// The flagged Key fell below the clear share for the full clear window.
    Cleared(RangeId),
}

/// Per-range detector deciding when a single Key dominates append demand.
///
/// Detection needs the dominant Key to hold `HOT_KEY_DOMINANT_SHARE_BPS` of
/// window records for `sustained_samples` consecutive windows; clearing needs
/// the flagged Key below `HOT_KEY_CLEAR_SHARE_BPS` for `clear_samples`
/// consecutive windows. Both directions require consecutive evidence so a
/// single burst neither flags nor unflags a Key.
pub struct HotKeyTracker {
    sustained_samples: u32,
    clear_samples: u32,
    candidate: Option<KeyToken>,
    candidate_evidence: u32,
    flagged: Option<HotKeyEvidence>,
    below_clear: u32,
}

impl HotKeyTracker {
    pub fn new(sustained_samples: u32, clear_samples: u32) -> Self {
        Self {
            sustained_samples: sustained_samples.max(1),
            clear_samples: clear_samples.max(1),
            candidate: None,
            candidate_evidence: 0,
            flagged: None,
            below_clear: 0,
        }
    }

    pub fn flagged(&self) -> Option<HotKeyEvidence> {
        self.flagged
    }

    pub fn observe(&mut self, sample: &RangePressureSample) -> HotKeyObservation {
        if sample.records < HOT_KEY_MIN_WINDOW_RECORDS {
            return HotKeyObservation::Unchanged;
        }
        let top = sample.top_keys.first();
        let dominant_share = top
            .map(|top| share_bps(top.records, sample.records))
            .unwrap_or(0);

        if let Some(current) = &mut self.flagged {
            if top.map(|top| top.token) == Some(current.token) {
                current.share_bps = dominant_share;
                current.window_records = sample.records;
                current.window_bytes = top.map(|top| top.bytes).unwrap_or(0);
                current.next_token = sample.dominant_next_token;
                self.below_clear = 0;
            } else {
                let flagged_share = sample
                    .top_keys
                    .iter()
                    .find(|key| key.token == current.token)
                    .map_or(0, |key| share_bps(key.records, sample.records));
                // Keep reported evidence honest while the flag decays.
                current.share_bps = flagged_share;
                current.window_records = sample.records;
                self.below_clear = if flagged_share >= HOT_KEY_CLEAR_SHARE_BPS {
                    0
                } else {
                    self.below_clear.saturating_add(1)
                };
            }
            if self.below_clear >= self.clear_samples {
                if let Some(cleared) = self.flagged.take() {
                    self.below_clear = 0;
                    return HotKeyObservation::Cleared(cleared.range_id);
                }
            }
            return HotKeyObservation::Unchanged;
        }

        if dominant_share >= HOT_KEY_DOMINANT_SHARE_BPS {
            let Some(top) = top else {
                return HotKeyObservation::Unchanged;
            };
            let token = top.token;
            if self.candidate == Some(token) {
                self.candidate_evidence = self.candidate_evidence.saturating_add(1);
            } else {
                self.candidate = Some(token);
                self.candidate_evidence = 1;
            }
            if self.candidate_evidence >= self.sustained_samples {
                let evidence = HotKeyEvidence {
                    range_id: sample.range_id,
                    token,
                    share_bps: dominant_share,
                    window_records: sample.records,
                    window_bytes: top.bytes,
                    next_token: sample.dominant_next_token,
                };
                self.flagged = Some(evidence);
                self.candidate = None;
                self.candidate_evidence = 0;
                self.below_clear = 0;
                return HotKeyObservation::Flagged(evidence);
            }
        } else {
            self.candidate = None;
            self.candidate_evidence = 0;
        }
        HotKeyObservation::Unchanged
    }
}

/// Pick the split boundary that isolates a flagged hot Key inside `bounds`.
///
/// An interior hot token becomes the split point itself, so the hot Key lands
/// on the right child's lower edge and a subsequent split just above it can
/// carve the Key into its own range. A hot token already sitting at the range
/// start needs the next observed token above it; `None` means no other Key
/// has been seen and nothing remains to isolate — a strictly ordered single
/// Key is never presented as parallelizable.
pub fn isolation_split_boundary(
    bounds: &crate::active_range::KeyRange,
    hot: &HotKeyEvidence,
) -> Option<KeyToken> {
    let interior = |token: KeyToken| {
        token > bounds.start && bounds.end_exclusive.is_none_or(|end| token < end)
    };
    if interior(hot.token) {
        Some(hot.token)
    } else {
        hot.next_token.filter(|upper| interior(*upper))
    }
}

fn share_bps(part: u64, total: u64) -> u32 {
    if total == 0 {
        return 0;
    }
    (part.saturating_mul(10_000) / total).min(10_000) as u32
}

/// Build a pressure sample from one range's append window: the split token
/// is the record-weighted median token (half of window records land on each
/// side), and the top-Key evidence rides alongside for hot-Key detection.
fn pressure_sample(range_id: RangeId, window: &RangeAppendWindow) -> RangePressureSample {
    let mut top_keys: Vec<KeyPressureShare> = window
        .key_counts
        .iter()
        .map(|(token, demand)| KeyPressureShare {
            token: *token,
            records: demand.records,
            bytes: demand.bytes,
        })
        .collect();
    top_keys.sort_by(|left, right| {
        right
            .records
            .cmp(&left.records)
            .then_with(|| left.token.cmp(&right.token))
    });
    top_keys.truncate(HOT_KEY_SAMPLE_TOP);
    let dominant_next_token = top_keys.first().and_then(|top| {
        window
            .key_counts
            .range((
                std::ops::Bound::Excluded(top.token),
                std::ops::Bound::Unbounded,
            ))
            .next()
            .map(|(token, _)| *token)
    });
    RangePressureSample {
        range_id,
        records: window.records,
        bytes: window.bytes,
        split_token: weighted_median(&window.key_counts, window.records),
        top_keys,
        dominant_next_token,
        untracked_records: window.untracked_records,
        untracked_bytes: window.untracked_bytes,
    }
}

/// Record-weighted median KeyToken: the smallest token accumulating at least
/// half the window's records. Splitting there keeps append load balanced even
/// when a few Keys dominate, which an unweighted token median cannot see.
fn weighted_median(key_counts: &BTreeMap<KeyToken, KeyDemand>, records: u64) -> Option<KeyToken> {
    let half = records.div_ceil(2);
    let mut accumulated = 0_u64;
    for (token, demand) in key_counts {
        accumulated = accumulated.saturating_add(demand.records);
        if accumulated >= half {
            return Some(*token);
        }
    }
    None
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        self.metrics
            .inner
            .requests_in_flight
            .fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_guard_and_counters_track_pressure() {
        let metrics = DemandMetrics::default();
        {
            let _guard = metrics.begin_request();
            metrics.record_append(128);
            metrics.record_read(3);
            assert_eq!(metrics.snapshot().requests_in_flight, 1);
        }
        assert_eq!(
            metrics.snapshot(),
            DemandSnapshot {
                requests_total: 1,
                requests_in_flight: 0,
                appends_total: 1,
                append_bytes_total: 128,
                reads_total: 1,
                records_read_total: 3,
            }
        );
    }

    #[test]
    fn range_pressure_samples_are_windowed_and_choose_observed_median() {
        let metrics = DemandMetrics::default();
        let range_id = RangeId::from_uuid(uuid::Uuid::from_u128(1));
        metrics.record_range_append(range_id, b"key-a", 10);
        metrics.record_range_append(range_id, b"key-b", 20);
        metrics.record_range_append(range_id, b"key-c", 30);
        let samples = metrics.take_range_pressure_samples();
        assert_eq!(samples[0].records, 3);
        assert_eq!(samples[0].bytes, 60);
        assert!(samples[0].split_token.is_some());
        assert!(metrics.take_range_pressure_samples().is_empty());
    }

    fn range_id(id: u128) -> RangeId {
        RangeId::from_uuid(uuid::Uuid::from_u128(id))
    }

    #[test]
    fn pressure_sample_reports_dominant_key_share_and_next_token() {
        let metrics = DemandMetrics::default();
        let range_id = range_id(7);
        for _ in 0..9 {
            metrics.record_range_append(range_id, b"hot", 100);
        }
        metrics.record_range_append(range_id, b"cold", 10);
        let sample = &metrics.take_range_pressure_samples()[0];
        let hot_token = KeyToken::from_key(b"hot");
        let cold_token = KeyToken::from_key(b"cold");
        assert_eq!(sample.top_keys[0].token, hot_token);
        assert_eq!(sample.top_keys[0].records, 9);
        assert_eq!(sample.top_keys.len(), 2);
        // The next observed token above the hot Key is the isolation edge.
        let expected_next = [hot_token, cold_token]
            .into_iter()
            .find(|token| *token > hot_token);
        assert_eq!(sample.dominant_next_token, expected_next);
        // The weighted median lands on the hot Key itself since it holds
        // at least half the window's records.
        assert_eq!(sample.split_token, Some(hot_token));
    }

    #[test]
    fn pressure_sample_bounds_distinct_keys_and_reports_untracked() {
        let metrics = DemandMetrics::default();
        let range_id = range_id(8);
        for index in 0..(HOT_KEY_TRACKED_KEYS + 10) {
            let key = format!("key-{index:04}");
            metrics.record_range_append(range_id, key.as_bytes(), 1);
        }
        let sample = &metrics.take_range_pressure_samples()[0];
        assert_eq!(sample.records, (HOT_KEY_TRACKED_KEYS + 10) as u64);
        assert_eq!(sample.top_keys.len(), HOT_KEY_SAMPLE_TOP);
        let tracked: u64 = sample.top_keys.iter().map(|key| key.records).sum();
        assert!(tracked <= HOT_KEY_SAMPLE_TOP as u64);
        assert_eq!(sample.untracked_records, 10);
        assert_eq!(sample.untracked_bytes, 10);
    }

    #[test]
    fn hot_key_tracker_requires_sustained_dominance() {
        let range_id = range_id(9);
        let metrics = DemandMetrics::default();
        let mut tracker = HotKeyTracker::new(2, 3);
        // A single dominant window is a burst, not a flag.
        for _ in 0..9 {
            metrics.record_range_append(range_id, b"hot", 10);
        }
        metrics.record_range_append(range_id, b"cold", 10);
        let sample = metrics.take_range_pressure_samples().remove(0);
        assert_eq!(tracker.observe(&sample), HotKeyObservation::Unchanged);
        assert!(tracker.flagged().is_none());
        // A second consecutive dominant window flags the Key.
        for _ in 0..8 {
            metrics.record_range_append(range_id, b"hot", 10);
        }
        metrics.record_range_append(range_id, b"cold-a", 10);
        metrics.record_range_append(range_id, b"cold-b", 10);
        let sample = metrics.take_range_pressure_samples().remove(0);
        let HotKeyObservation::Flagged(evidence) = tracker.observe(&sample) else {
            panic!("sustained dominance should flag the hot Key");
        };
        assert_eq!(evidence.token, KeyToken::from_key(b"hot"));
        assert_eq!(evidence.range_id, range_id);
        assert_eq!(evidence.share_bps, 8_000);
        assert!(tracker.flagged().is_some());
    }

    #[test]
    fn hot_key_tracker_clears_only_after_sustained_sub_threshold_share() {
        let range_id = range_id(10);
        let metrics = DemandMetrics::default();
        let mut tracker = HotKeyTracker::new(2, 2);
        for _ in 0..2 {
            for _ in 0..9 {
                metrics.record_range_append(range_id, b"hot", 10);
            }
            metrics.record_range_append(range_id, b"cold", 10);
            tracker.observe(&metrics.take_range_pressure_samples().remove(0));
        }
        assert!(tracker.flagged().is_some());
        // One sub-threshold window is tolerated by hysteresis.
        for _ in 0..5 {
            metrics.record_range_append(range_id, b"cold-a", 10);
            metrics.record_range_append(range_id, b"cold-b", 10);
        }
        metrics.record_range_append(range_id, b"hot", 10);
        let sample = metrics.take_range_pressure_samples().remove(0);
        assert_eq!(tracker.observe(&sample), HotKeyObservation::Unchanged);
        assert!(tracker.flagged().is_some());
        // A second consecutive sub-threshold window clears the flag.
        for _ in 0..5 {
            metrics.record_range_append(range_id, b"cold-a", 10);
            metrics.record_range_append(range_id, b"cold-b", 10);
        }
        metrics.record_range_append(range_id, b"hot", 10);
        let sample = metrics.take_range_pressure_samples().remove(0);
        assert_eq!(
            tracker.observe(&sample),
            HotKeyObservation::Cleared(range_id)
        );
        assert!(tracker.flagged().is_none());
    }

    #[test]
    fn hot_key_tracker_ignores_low_volume_windows() {
        let range_id = range_id(13);
        let metrics = DemandMetrics::default();
        let mut tracker = HotKeyTracker::new(2, 2);
        // A 100%-share window below the record floor is not evidence.
        for _ in 0..4 {
            metrics.record_range_append(range_id, b"hot", 10);
        }
        let sample = metrics.take_range_pressure_samples().remove(0);
        assert_eq!(tracker.observe(&sample), HotKeyObservation::Unchanged);
        assert!(tracker.flagged().is_none());
        // It also does not reset accumulated evidence: resume dominance and
        // the first floor-passing window still counts toward the flag.
        for _ in 0..9 {
            metrics.record_range_append(range_id, b"hot", 10);
        }
        metrics.record_range_append(range_id, b"cold", 10);
        tracker.observe(&metrics.take_range_pressure_samples().remove(0));
        for _ in 0..4 {
            metrics.record_range_append(range_id, b"hot", 10);
        }
        tracker.observe(&metrics.take_range_pressure_samples().remove(0));
        for _ in 0..9 {
            metrics.record_range_append(range_id, b"hot", 10);
        }
        metrics.record_range_append(range_id, b"cold", 10);
        let sample = metrics.take_range_pressure_samples().remove(0);
        assert!(matches!(
            tracker.observe(&sample),
            HotKeyObservation::Flagged(_)
        ));
    }

    #[test]
    fn isolation_boundary_carves_hot_key_to_range_edge() {
        use crate::active_range::KeyRange;
        let low = KeyToken::from_bytes([0x10; 16]);
        let hot = KeyToken::from_bytes([0x80; 16]);
        let high = KeyToken::from_bytes([0xf0; 16]);
        let evidence = |token, next_token| HotKeyEvidence {
            range_id: range_id(11),
            token,
            share_bps: 9_000,
            window_records: 100,
            window_bytes: 1_000,
            next_token,
        };
        let whole = KeyRange {
            start: KeyToken::MIN,
            end_exclusive: None,
        };
        // Interior hot Key: split at the Key itself so it becomes the right
        // child's lower edge.
        assert_eq!(
            isolation_split_boundary(&whole, &evidence(hot, Some(high))),
            Some(hot)
        );
        // Hot Key already at the range start: split at the next observed
        // token so the left child holds only the hot Key's edge.
        let starts_at_hot = KeyRange {
            start: hot,
            end_exclusive: None,
        };
        assert_eq!(
            isolation_split_boundary(&starts_at_hot, &evidence(hot, Some(high))),
            Some(high)
        );
        // No other Key has ever been observed: nothing exists to isolate.
        assert_eq!(
            isolation_split_boundary(&starts_at_hot, &evidence(hot, None)),
            None
        );
        // A next token outside the bounds is not a valid boundary.
        let narrow = KeyRange {
            start: low,
            end_exclusive: Some(high),
        };
        assert_eq!(
            isolation_split_boundary(&narrow, &evidence(low, None)),
            None
        );
    }

    #[test]
    fn hot_key_registry_round_trips_flags() {
        let metrics = DemandMetrics::default();
        let range_id = range_id(12);
        let evidence = HotKeyEvidence {
            range_id,
            token: KeyToken::from_key(b"hot"),
            share_bps: 8_500,
            window_records: 100,
            window_bytes: 1_000,
            next_token: None,
        };
        assert!(!metrics.is_hot_key(range_id, b"hot"));
        metrics.set_hot_key(evidence);
        assert!(metrics.is_hot_key(range_id, b"hot"));
        assert!(!metrics.is_hot_key(range_id, b"other"));
        assert_eq!(metrics.hot_keys(), vec![evidence]);
        metrics.clear_hot_key(range_id);
        assert!(!metrics.is_hot_key(range_id, b"hot"));
        assert!(metrics.hot_keys().is_empty());
    }

    #[test]
    fn pipe_drive_telemetry_aggregates_outcomes_and_prunes_dropped_pipes() {
        let metrics = DemandMetrics::default();
        let pipe = uuid::Uuid::from_u128(7_001);
        let dropped = uuid::Uuid::from_u128(7_002);
        let sample = |result, records| PipeDriveSample {
            result,
            outputs: 2,
            state_mutations: 1,
            records,
            fetch_bytes: 512,
            refused: 3,
            emissions: 4,
            pane_rows: 9,
            pane_bytes: 340,
            page_limit: 16,
            wall_ns: 2_000,
            sampled_at_ns: 42,
        };
        metrics.record_pipe_drive(pipe, sample(PipeDriveResult::Applied, 5));
        metrics.record_pipe_drive(pipe, sample(PipeDriveResult::Idle, 0));
        metrics.record_pipe_drive(
            pipe,
            sample(PipeDriveResult::Failed("quorum lost".to_owned()), 0),
        );
        metrics.record_pipe_drive(dropped, sample(PipeDriveResult::Idle, 1));

        let telemetry = metrics.pipe_drive_telemetry();
        let row = telemetry.get(&pipe).unwrap();
        assert_eq!(row.drives, 3);
        assert_eq!(row.applied, 1);
        assert_eq!(row.idle, 1);
        assert_eq!(row.failed, 1);
        assert_eq!(row.outputs, 6);
        assert_eq!(row.state_mutations, 3);
        assert_eq!(row.records_fetched, 5);
        assert_eq!(row.fetch_bytes, 1_536);
        assert_eq!(row.records_refused, 9);
        assert_eq!(row.emissions, 12);
        assert_eq!(row.pane_rows_last, 9);
        assert_eq!(row.pane_bytes_last, 340);
        assert_eq!(row.last_page_records, 0);
        assert_eq!(row.last_page_limit, 16);
        assert_eq!(row.drive_wall_ns_total, 6_000);
        assert_eq!(row.last_drive_wall_ns, 2_000);
        assert_eq!(row.last_drive_at_ns, Some(42));
        assert_eq!(row.last_error.as_deref(), Some("quorum lost"));

        // A successful cycle clears the last error.
        metrics.record_pipe_drive(pipe, sample(PipeDriveResult::Idle, 0));
        assert_eq!(
            metrics
                .pipe_drive_telemetry()
                .get(&pipe)
                .unwrap()
                .last_error,
            None
        );

        // Dropped Pipes leave no attribution residue.
        let live = std::collections::BTreeSet::from([pipe]);
        metrics.prune_pipe_drives(&live);
        let telemetry = metrics.pipe_drive_telemetry();
        assert!(telemetry.contains_key(&pipe));
        assert!(!telemetry.contains_key(&dropped));
    }

    #[test]
    fn subscription_traffic_aggregates_ops_and_prunes_dropped_subscriptions() {
        let metrics = DemandMetrics::default();
        let subscription = uuid::Uuid::from_u128(8_001);
        let dropped = uuid::Uuid::from_u128(8_002);
        let sample = |op, records, ranges, add, remove, members| SubscriptionTrafficSample {
            op,
            records,
            ranges,
            quarantine_add: add,
            quarantine_remove: remove,
            members,
            sampled_at_ns: 77,
        };
        metrics.record_subscription_op(
            subscription,
            sample(SubscriptionTrafficOp::Join, 0, 0, 0, 0, 1),
        );
        metrics.record_subscription_op(
            subscription,
            sample(SubscriptionTrafficOp::Claim, 0, 0, 0, 0, 1),
        );
        metrics.record_subscription_op(
            subscription,
            sample(SubscriptionTrafficOp::Fetch, 12, 0, 0, 0, 1),
        );
        metrics.record_subscription_op(
            subscription,
            sample(SubscriptionTrafficOp::Fetch, 8, 0, 0, 0, 1),
        );
        metrics.record_subscription_op(
            subscription,
            sample(SubscriptionTrafficOp::Ack, 0, 2, 3, 1, 1),
        );
        metrics.record_subscription_op(
            subscription,
            sample(SubscriptionTrafficOp::Seek, 0, 0, 0, 0, 0),
        );
        metrics.record_subscription_op(dropped, sample(SubscriptionTrafficOp::Join, 0, 0, 0, 0, 1));

        let telemetry = metrics.subscription_traffic();
        let row = telemetry.get(&subscription).unwrap();
        assert_eq!(row.requests, 6);
        assert_eq!(row.joins, 1);
        assert_eq!(row.claims, 1);
        assert_eq!(row.fetches, 2);
        assert_eq!(row.fetch_records, 20);
        assert_eq!(row.acks, 1);
        assert_eq!(row.ack_ranges, 2);
        assert_eq!(row.quarantined_records, 3);
        assert_eq!(row.released_quarantined_records, 1);
        assert_eq!(row.seeks, 1);
        assert_eq!(row.members_active, 0);
        assert_eq!(row.last_op, Some("seek"));
        assert_eq!(row.last_op_at_ns, Some(77));

        // Dropped Subscriptions leave no attribution residue.
        let live = std::collections::BTreeSet::from([subscription]);
        metrics.prune_subscription_traffic(&live);
        let telemetry = metrics.subscription_traffic();
        assert!(telemetry.contains_key(&subscription));
        assert!(!telemetry.contains_key(&dropped));
    }

    #[test]
    fn reader_traffic_aggregates_ops_and_prunes_dropped_readers() {
        let metrics = DemandMetrics::default();
        let reader = uuid::Uuid::from_u128(7_001);
        let dropped = uuid::Uuid::from_u128(7_002);
        let sample = |op, records| crate::demand::ReaderTrafficSample {
            op,
            records,
            sampled_at_ns: 55,
        };
        metrics.record_reader_op(reader, sample(ReaderTrafficOp::Open, 0));
        metrics.record_reader_op(reader, sample(ReaderTrafficOp::Fetch, 12));
        metrics.record_reader_op(reader, sample(ReaderTrafficOp::Fetch, 8));
        metrics.record_reader_op(reader, sample(ReaderTrafficOp::Ack, 0));
        metrics.record_reader_op(reader, sample(ReaderTrafficOp::Close, 0));
        metrics.record_reader_op(dropped, sample(ReaderTrafficOp::Open, 0));

        let telemetry = metrics.reader_traffic();
        let row = telemetry.get(&reader).unwrap();
        assert_eq!(row.requests, 5);
        assert_eq!(row.opens, 1);
        assert_eq!(row.fetches, 2);
        assert_eq!(row.fetch_records, 20);
        assert_eq!(row.acks, 1);
        assert_eq!(row.closes, 1);
        assert_eq!(row.last_op, Some("close"));
        assert_eq!(row.last_op_at_ns, Some(55));

        // Dropped Readers leave no attribution residue.
        let live = std::collections::BTreeSet::from([reader]);
        metrics.prune_reader_traffic(&live);
        let telemetry = metrics.reader_traffic();
        assert!(telemetry.contains_key(&reader));
        assert!(!telemetry.contains_key(&dropped));
    }

    #[test]
    fn index_query_telemetry_aggregates_served_reads_and_prunes_dropped_indexes() {
        let metrics = DemandMetrics::default();
        let index = uuid::Uuid::from_u128(9_001);
        let dropped = uuid::Uuid::from_u128(9_002);
        let sample = |mode, rows, truncated, behind| IndexQuerySample {
            mode,
            rows,
            truncated,
            behind,
            sampled_at_ns: 55,
        };
        metrics.record_index_query(index, sample(IndexQueryMode::Exact, 1, false, false));
        metrics.record_index_query(index, sample(IndexQueryMode::Exact, 0, false, true));
        metrics.record_index_query(index, sample(IndexQueryMode::Prefix, 7, false, false));
        metrics.record_index_query(index, sample(IndexQueryMode::Range, 256, true, false));
        metrics.record_index_query(dropped, sample(IndexQueryMode::Exact, 2, false, false));

        let telemetry = metrics.index_query_telemetry();
        let row = telemetry.get(&index).unwrap();
        assert_eq!(row.queries, 4);
        assert_eq!(row.exact_queries, 2);
        assert_eq!(row.prefix_queries, 1);
        assert_eq!(row.range_queries, 1);
        assert_eq!(row.rows_returned, 264);
        assert_eq!(row.truncated_queries, 1);
        assert_eq!(row.served_behind, 1);
        assert_eq!(row.last_mode, Some("range"));
        assert_eq!(row.last_query_at_ns, Some(55));

        // Dropped Indexes leave no attribution residue.
        let live = std::collections::BTreeSet::from([index]);
        metrics.prune_index_queries(&live);
        let telemetry = metrics.index_query_telemetry();
        assert!(telemetry.contains_key(&index));
        assert!(!telemetry.contains_key(&dropped));
    }

    #[test]
    fn range_read_totals_accumulate_served_pages_per_range() {
        let metrics = DemandMetrics::default();
        let first = range_id(11);
        let second = range_id(12);
        metrics.record_range_read(first, 8, 400);
        metrics.record_range_read(first, 4, 200);
        metrics.record_range_read(second, 1, 48);

        let totals = metrics.range_read_totals();
        assert_eq!(totals.get(&first).unwrap().pages, 2);
        assert_eq!(totals.get(&first).unwrap().records, 12);
        assert_eq!(totals.get(&first).unwrap().bytes, 600);
        assert_eq!(totals.get(&second).unwrap().records, 1);
        assert_eq!(totals.get(&second).unwrap().bytes, 48);
    }

    #[test]
    fn replication_and_move_totals_accumulate_per_range_and_feed() {
        let metrics = DemandMetrics::default();
        let range = range_id(21);
        let feed = uuid::Uuid::from_u128(9_101);
        metrics.record_range_replication(range, 512);
        metrics.record_range_replication(range, 300);
        metrics.record_range_replication_commit(range);
        metrics.record_range_read(range, 3, 96);
        metrics.record_range_append(range, b"key", 96);
        metrics.record_feed_move(feed, 1_000, 48_000, 200);
        metrics.record_feed_move(feed, 12, 600, 0);

        let replication = metrics.range_replication_totals();
        assert_eq!(replication.get(&range).unwrap().frames, 2);
        assert_eq!(replication.get(&range).unwrap().wire_bytes, 812);
        assert_eq!(replication.get(&range).unwrap().commits, 1);
        let moves = metrics.feed_move_totals();
        assert_eq!(moves.get(&feed).unwrap().moves, 2);
        assert_eq!(moves.get(&feed).unwrap().records, 1_012);
        assert_eq!(moves.get(&feed).unwrap().bytes, 48_600);
        assert_eq!(moves.get(&feed).unwrap().skipped_records, 200);

        metrics.prune_feed_demand(
            &std::collections::BTreeSet::new(),
            &std::collections::BTreeSet::new(),
        );
        assert!(metrics.range_replication_totals().is_empty());
        assert!(metrics.feed_move_totals().is_empty());
        assert!(metrics.range_read_totals().is_empty());
        assert!(metrics.range_pressure_totals().is_empty());
    }
}
