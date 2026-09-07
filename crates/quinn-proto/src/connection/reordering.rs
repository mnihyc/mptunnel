//! Native loss tolerance learned from unambiguous late-original delivery.
//!
//! RFC 9002 §6.1 permits adaptive absolute time thresholds. Late delivery
//! changes this detector, not congestion ownership: one original cannot undo
//! the loss response for other packets. Aging follows RFC 8985 §6.2.

use crate::{Duration, Instant, TIMER_GRANULARITY};

const RECOVERY_PERSISTENCE: u8 = 16;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Reordering {
    seen: bool,
    observed_excess: Duration,
    recovery_start: Option<Instant>,
    recovering: bool,
    remaining_recoveries: u8,
}

impl Reordering {
    pub(super) fn packet_threshold_enabled(&self) -> bool {
        !self.seen
    }

    pub(super) fn loss_delay(&self, ordinary: Duration, current_rtt: Duration) -> Duration {
        if self.observed_excess.is_zero() {
            ordinary
        } else {
            ordinary.max(current_rtt.saturating_add(self.observed_excess))
        }
    }

    pub(super) fn on_late_original(&mut self, now: Instant, sent: Instant, current_rtt: Duration) {
        // The caller matches only current-lineage, unexpired retained originals.
        // Retain excess delay, not the common RTT component of an old queue.
        // Recompose with the current baseline at loss detection. A genuinely
        // large differential tail remains evidence; there is no RTT-sized cap.
        self.seen = true;
        self.observed_excess = self.observed_excess.max(
            now.saturating_duration_since(sent)
                .saturating_sub(current_rtt)
                .saturating_add(TIMER_GRANULARITY),
        );
        self.remaining_recoveries = RECOVERY_PERSISTENCE;
    }

    pub(super) fn on_loss(&mut self, now: Instant, sent: Instant) {
        if self.recovery_start.is_none_or(|start| sent > start) {
            self.recovery_start = Some(now);
            self.recovering = true;
        }
    }

    pub(super) fn on_ack(&mut self, sent: Instant) {
        if self.recovering && self.recovery_start.is_some_and(|start| sent > start) {
            self.recovering = false;
            self.remaining_recoveries = self.remaining_recoveries.saturating_sub(1);
            if self.remaining_recoveries == 0 {
                self.observed_excess = Duration::ZERO;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_proof_preserves_native_deadline_and_packet_detector() {
        let now = Instant::now();
        let mut state = Reordering::default();
        state.on_loss(now, now - Duration::from_millis(100));
        state.on_ack(now + Duration::from_millis(1));
        assert!(state.packet_threshold_enabled());
        let ordinary = Duration::from_micros(112_500);
        assert_eq!(state.loss_delay(ordinary, Duration::from_millis(100)), ordinary);
        // No evidence must preserve even an explicitly shorter native policy.
        // MPP's default D0 is already >= RTT; this protects the owner boundary.
        let short = Duration::from_millis(50);
        assert_eq!(state.loss_delay(short, Duration::from_millis(100)), short);
    }

    #[test]
    fn learned_deadline_covers_the_original_that_disproved_loss() {
        let start = Instant::now();
        let mut state = Reordering::default();
        let elapsed = Duration::from_millis(180);
        let base = Duration::from_millis(100);
        state.on_late_original(start + elapsed, start, base);
        assert_eq!(state.loss_delay(Duration::from_micros(112_500), base), elapsed + TIMER_GRANULARITY);
        assert!(!state.packet_threshold_enabled());
        // A burst of younger originals cannot inflate or erase the observed fact.
        for n in 0..100 {
            state.on_late_original(start + elapsed, start + Duration::from_micros(n), base);
        }
        assert_eq!(state.loss_delay(Duration::ZERO, base), elapsed + TIMER_GRANULARITY);
        assert_eq!(state.loss_delay(Duration::from_millis(250), base), Duration::from_millis(250));
    }

    #[test]
    fn adaptation_ages_by_completed_recoveries_not_loss_batches() {
        let start = Instant::now();
        let mut state = Reordering::default();
        let base = Duration::from_millis(100);
        state.on_late_original(start + Duration::from_millis(180), start, base);
        for n in 0..16 {
            let sent = start + Duration::from_secs(n + 1);
            let loss_at = sent + Duration::from_millis(100);
            state.on_loss(loss_at, sent);
            state.on_loss(loss_at + Duration::from_millis(1), sent);
            state.on_ack(sent);
            assert_eq!(state.remaining_recoveries, 16 - n as u8);
            assert!(state.observed_excess > Duration::ZERO);
            state.on_ack(loss_at + Duration::from_millis(1));
            state.on_ack(loss_at + Duration::from_millis(2));
        }
        assert_eq!(state.observed_excess, Duration::ZERO);
        assert_eq!(state.loss_delay(Duration::from_micros(112_500), base), Duration::from_micros(112_500));
        assert!(!state.packet_threshold_enabled());
    }

    #[test]
    fn common_queue_delay_does_not_become_future_reordering_delay() {
        let start = Instant::now();
        let elapsed = Duration::from_micros(2_014_429);
        let queued_base = Duration::from_micros(1_905_151);
        let recovered_base = Duration::from_micros(54_912);
        let mut state = Reordering::default();
        state.on_late_original(start + elapsed, start, queued_base);
        let expected = recovered_base + (elapsed - queued_base) + TIMER_GRANULARITY;
        assert_eq!(state.loss_delay(recovered_base.mul_f32(1.125), recovered_base), expected);
    }
}
