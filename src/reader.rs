use std::time::Duration;

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
}
