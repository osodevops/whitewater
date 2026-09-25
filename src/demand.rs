use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use serde::Serialize;

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
}
