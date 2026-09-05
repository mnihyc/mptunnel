//! Diagnostic native-byte counters. No admission, rate model or liveness authority.

use crate::protocol::{NativeDeliverySnapshot, PathMetricDirection};
use crate::runtime::identity::random_u64;
use std::time::Instant;

#[derive(Debug, Default)]
pub(in crate::runtime) struct NativeDeliveryTracker {
    epoch: Option<u64>,
    started_at: Option<Instant>,
    previous_bytes: Option<u64>,
}

impl NativeDeliveryTracker {
    pub(in crate::runtime) fn observe(
        &mut self,
        acked_bytes: Option<u64>,
        direction: PathMetricDirection,
        now: Instant,
    ) -> Option<NativeDeliverySnapshot> {
        let acked_bytes = acked_bytes?;
        if self.started_at.is_none()
            || self
                .previous_bytes
                .is_some_and(|previous| acked_bytes < previous)
        {
            // Counter replacement/wrap begins a distinct diagnostic clock.
            // Missing entropy disables this optional metric, never the path.
            self.epoch = random_u64().ok();
            self.started_at = Some(now);
        }
        self.previous_bytes = Some(acked_bytes);
        Some(NativeDeliverySnapshot {
            epoch: self.epoch?,
            sampled_at_us: now
                .checked_duration_since(self.started_at?)?
                .as_micros()
                .try_into()
                .ok()?,
            acked_bytes,
            direction,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn counters_keep_idle_zero_and_missing_observation_distinct() {
        let mut tracker = NativeDeliveryTracker::default();
        let now = Instant::now();
        let direction = PathMetricDirection::ServerToClient;
        assert_eq!(tracker.observe(None, direction, now), None);
        let first = tracker.observe(Some(0), direction, now).unwrap();
        let idle = tracker
            .observe(Some(0), direction, now + Duration::from_secs(1))
            .unwrap();
        assert_eq!(idle.epoch, first.epoch);
        assert_eq!(idle.acked_bytes, 0);
        assert_eq!(idle.sampled_at_us - first.sampled_at_us, 1_000_000);
        assert_eq!(
            tracker.observe(None, direction, now + Duration::from_secs(2)),
            None
        );
        let data = tracker
            .observe(Some(1_250_000), direction, now + Duration::from_secs(3))
            .unwrap();
        assert_eq!(data.epoch, idle.epoch);
        assert_eq!(data.acked_bytes - idle.acked_bytes, 1_250_000);
        let reset = tracker
            .observe(Some(0), direction, now + Duration::from_secs(4))
            .unwrap();
        assert_eq!(reset.sampled_at_us, 0);
        assert_eq!(reset.direction, direction);
    }
}
