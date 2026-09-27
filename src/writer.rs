use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct WriterServerFeedback {
    pub recommended_batch_count: usize,
    pub recommended_batch_bytes: usize,
    pub pressure: f64,
    pub retry_after_ms: u64,
    pub max_frame_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchPlan {
    pub max_count: usize,
    pub max_bytes: usize,
    pub linger: Duration,
}

#[derive(Clone, Debug)]
pub struct AdaptiveBatchPolicy {
    min_count: usize,
    max_count: usize,
    min_bytes: usize,
    max_bytes: usize,
    target_latency: Duration,
    average_message_bytes: f64,
    retry_rate: f64,
}

impl AdaptiveBatchPolicy {
    pub fn new(
        min_count: usize,
        max_count: usize,
        min_bytes: usize,
        max_bytes: usize,
        target_latency: Duration,
    ) -> Self {
        Self {
            min_count: min_count.max(1),
            max_count: max_count.max(min_count.max(1)),
            min_bytes: min_bytes.max(1),
            max_bytes: max_bytes.max(min_bytes.max(1)),
            target_latency,
            average_message_bytes: min_bytes.max(1) as f64,
            retry_rate: 0.0,
        }
    }

    pub fn observe_message(&mut self, bytes: usize) {
        self.average_message_bytes = self.average_message_bytes * 0.8 + bytes.max(1) as f64 * 0.2;
    }

    pub fn observe_retry(&mut self, retried: bool) {
        let sample = if retried { 1.0 } else { 0.0 };
        self.retry_rate = self.retry_rate * 0.9 + sample * 0.1;
    }

    pub fn plan(&self, feedback: Option<&WriterServerFeedback>) -> BatchPlan {
        let pressure = feedback.map_or(0.0, |value| value.pressure.clamp(0.0, 1.0));
        let retry_penalty = (1.0 - self.retry_rate.clamp(0.0, 0.9)).max(0.1);
        let pressure_penalty = (1.0 - pressure * 0.8).max(0.2);
        let server_bytes = feedback
            .map(|value| value.recommended_batch_bytes.min(value.max_frame_bytes))
            .unwrap_or(self.max_bytes);
        let max_bytes = ((server_bytes as f64 * pressure_penalty * retry_penalty) as usize)
            .clamp(self.min_bytes, self.max_bytes);
        let size_count = (max_bytes as f64 / self.average_message_bytes.max(1.0)) as usize;
        let server_count = feedback
            .map(|value| value.recommended_batch_count)
            .unwrap_or(self.max_count);
        let max_count = size_count
            .min(server_count)
            .clamp(self.min_count, self.max_count);
        let retry_delay = feedback.map_or(0, |value| value.retry_after_ms);
        BatchPlan {
            max_count,
            max_bytes,
            linger: self.target_latency.max(Duration::from_millis(retry_delay)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_batching_adapts_to_message_size_pressure_and_retry_feedback() {
        let mut policy = AdaptiveBatchPolicy::new(1, 100, 1024, 1_000_000, Duration::from_millis(5));
        for _ in 0..10 {
            policy.observe_message(10_000);
        }
        let normal = policy.plan(None);
        assert!(normal.max_count < 100);
        let feedback = WriterServerFeedback {
            recommended_batch_count: 20,
            recommended_batch_bytes: 100_000,
            pressure: 0.8,
            retry_after_ms: 50,
            max_frame_bytes: 64_000,
        };
        policy.observe_retry(true);
        let pressured = policy.plan(Some(&feedback));
        assert!(pressured.max_count <= 20);
        assert!(pressured.max_bytes <= 64_000);
        assert_eq!(pressured.linger, Duration::from_millis(50));
    }
}
