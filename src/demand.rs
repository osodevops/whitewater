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
}

#[derive(Default)]
struct RangeAppendWindow {
    records: u64,
    bytes: u64,
    key_tokens: Vec<KeyToken>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangePressureSample {
    pub range_id: RangeId,
    pub records: u64,
    pub bytes: u64,
    pub split_token: Option<KeyToken>,
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
                if window.key_tokens.len() < 4096 {
                    window.key_tokens.push(token);
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
            .map(|(range_id, mut window)| {
                window.key_tokens.sort_unstable();
                let split_token = window
                    .key_tokens
                    .get(window.key_tokens.len().saturating_sub(1) / 2)
                    .copied();
                RangePressureSample {
                    range_id,
                    records: window.records,
                    bytes: window.bytes,
                    split_token,
                }
            })
            .collect()
    }

    pub fn range_pressure_totals(&self) -> Vec<RangePressureSample> {
        let Ok(ranges) = self.inner.range_totals.lock() else {
            return Vec::new();
        };
        ranges
            .iter()
            .map(|(range_id, window)| {
                let mut tokens = window.key_tokens.clone();
                tokens.sort_unstable();
                let split_token = tokens.get(tokens.len().saturating_sub(1) / 2).copied();
                RangePressureSample {
                    range_id: *range_id,
                    records: window.records,
                    bytes: window.bytes,
                    split_token,
                }
            })
            .collect()
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
}
