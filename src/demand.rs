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
}
