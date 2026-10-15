use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

use crate::control::RangeCommitProof;

use super::RangeId;

/// Persists a committed-frontier proof durably so it survives loss of
/// the Range's local copies; implemented by the catalog/Control Plane
/// wiring so the data plane stays transport-agnostic.
#[async_trait]
pub trait CommitProofSink: Send + Sync {
    async fn record(&self, proof: RangeCommitProof) -> Result<(), String>;
}

/// Coalesces per-append committed-frontier proofs into best-effort
/// catalog reports. Appends commit at line rate; the Control Plane only
/// needs the *latest* quorum-committed frontier per Range, so reporting
/// keeps one pending proof per Range and drains asynchronously. A failed
/// report is dropped rather than retried — proofs are monotonic, so the
/// next successful append re-proves the Range at a higher position, and
/// a missing proof degrades detection to the live per-read corroboration
/// that already exists rather than fabricating certainty.
/// Minimum spacing between drained proof batches per reporter. Each
/// catalog report commits a Control Plane entry, so proofs are reported
/// at this interval rather than at append rate; the map always keeps the
/// newest proof per Range, so a busy Range emits its freshest frontier.
const FLUSH_INTERVAL: Duration = Duration::from_millis(500);

pub struct CommitProofReporter {
    sink: Arc<dyn CommitProofSink>,
    pending: Mutex<BTreeMap<RangeId, RangeCommitProof>>,
    flushing: AtomicBool,
}

impl CommitProofReporter {
    pub fn new(sink: Arc<dyn CommitProofSink>) -> Arc<Self> {
        Arc::new(Self {
            sink,
            pending: Mutex::new(BTreeMap::new()),
            flushing: AtomicBool::new(false),
        })
    }

    /// Queue `proof` for reporting, replacing any older pending proof for
    /// the same Range. Returns without waiting for the catalog write;
    /// reporting happens on a spawned drain that coalesces bursts.
    pub fn report(self: &Arc<Self>, proof: RangeCommitProof) {
        if let Ok(mut pending) = self.pending.try_lock() {
            insert_newest(&mut pending, proof);
        } else {
            // A drain holds the lock; defer the insert to a spawn that
            // re-triggers the drain so the proof cannot strand behind
            // the drain task's final empty-check.
            let this = self.clone();
            tokio::spawn(async move {
                let mut pending = this.pending.lock().await;
                insert_newest(&mut pending, proof);
                drop(pending);
                this.spawn_drain();
            });
            return;
        }
        self.spawn_drain();
    }

    fn spawn_drain(self: &Arc<Self>) {
        if self.flushing.swap(true, Ordering::AcqRel) {
            return;
        }
        let this = self.clone();
        tokio::spawn(this.drain());
    }

    async fn drain(self: Arc<Self>) {
        loop {
            let batch = {
                let mut pending = self.pending.lock().await;
                std::mem::take(&mut *pending)
            };
            if batch.is_empty() {
                break;
            }
            for (_, proof) in batch {
                if let Err(error) = self.sink.record(proof.clone()).await {
                    tracing::warn!(
                        range_id = %proof.range_id,
                        position = proof.position.value(),
                        %error,
                        "committed-frontier proof report failed"
                    );
                }
            }
            // Bound catalog writes: a sustained append burst produces
            // one drained batch per interval, not one entry per commit.
            sleep(FLUSH_INTERVAL).await;
        }
        self.flushing.store(false, Ordering::Release);
        // A report that landed between the final take and this store saw
        // flushing=true and skipped its own drain; recheck closes that
        // race without leaving pending proofs stranded.
        if !self.pending.lock().await.is_empty() {
            self.spawn_drain();
        }
    }
}

/// Keep only the newest proof per Range so a delayed older report can
/// never mask a newer committed frontier.
fn insert_newest(pending: &mut BTreeMap<RangeId, RangeCommitProof>, proof: RangeCommitProof) {
    match pending.get(&proof.range_id) {
        Some(existing) if existing.position >= proof.position => {}
        _ => {
            pending.insert(proof.range_id, proof);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use super::*;
    use crate::active_range::CommitPosition;
    use crate::control::RangeCommitProof;
    use uuid::Uuid;

    #[derive(Default)]
    struct CollectSink {
        proofs: Mutex<Vec<RangeCommitProof>>,
        attempts: AtomicUsize,
        fail: AtomicBool,
    }

    #[async_trait]
    impl CommitProofSink for CollectSink {
        async fn record(&self, proof: RangeCommitProof) -> Result<(), String> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if self.fail.load(Ordering::SeqCst) {
                return Err("catalog unavailable".to_owned());
            }
            self.proofs.lock().await.push(proof);
            Ok(())
        }
    }

    fn proof(range_id: RangeId, position: u64) -> RangeCommitProof {
        RangeCommitProof {
            feed_id: Uuid::from_u128(7),
            range_id,
            position: CommitPosition::new(position),
            frame_digest: [position as u8; 32],
            recorded_at_ns: 0,
            evidence: vec![],
            reporter: crate::active_range::StorageNodeId::try_new("storage-1").unwrap(),
        }
    }

    async fn wait_for(sink: &CollectSink, count: usize) {
        for _ in 0..200 {
            if sink.proofs.lock().await.len() >= count {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn reports_coalesce_and_deliver_the_newest_frontier() {
        let sink = Arc::new(CollectSink::default());
        let reporter = CommitProofReporter::new(sink.clone());
        let range = RangeId::from_uuid(Uuid::from_u128(42));
        for position in 1..=5 {
            reporter.report(proof(range, position));
        }
        // Drain the spawned tasks deterministically.
        for _ in 0..200 {
            tokio::task::yield_now().await;
            if !sink.proofs.lock().await.is_empty() {
                break;
            }
        }
        wait_for(&sink, 1).await;
        // Whatever drained mid-burst, the final recorded proof is the
        // newest position and positions only ever move forward.
        let proofs = sink.proofs.lock().await;
        assert_eq!(proofs.last().unwrap().position, CommitPosition::new(5));
        assert!(proofs
            .windows(2)
            .all(|pair| pair[0].position < pair[1].position));
    }

    #[tokio::test]
    async fn failed_reports_drop_and_later_proofs_still_deliver() {
        let sink = Arc::new(CollectSink::default());
        let reporter = CommitProofReporter::new(sink.clone());
        let range = RangeId::from_uuid(Uuid::from_u128(43));
        sink.fail.store(true, Ordering::SeqCst);
        reporter.report(proof(range, 1));
        for _ in 0..200 {
            tokio::task::yield_now().await;
            if sink.attempts.load(Ordering::SeqCst) > 0 {
                break;
            }
        }
        assert!(sink.proofs.lock().await.is_empty());

        sink.fail.store(false, Ordering::SeqCst);
        reporter.report(proof(range, 2));
        wait_for(&sink, 1).await;
        assert_eq!(sink.proofs.lock().await[0].position, CommitPosition::new(2));
    }

    #[tokio::test]
    async fn pending_proofs_are_tracked_per_range() {
        let sink = Arc::new(CollectSink::default());
        let reporter = CommitProofReporter::new(sink.clone());
        let left = RangeId::from_uuid(Uuid::from_u128(44));
        let right = RangeId::from_uuid(Uuid::from_u128(45));
        reporter.report(proof(left, 3));
        reporter.report(proof(right, 7));
        wait_for(&sink, 2).await;
        let positions: Vec<u64> = sink
            .proofs
            .lock()
            .await
            .iter()
            .map(|proof| proof.position.value())
            .collect();
        assert_eq!(positions.len(), 2);
        assert!(positions.contains(&3) && positions.contains(&7));
    }
}
