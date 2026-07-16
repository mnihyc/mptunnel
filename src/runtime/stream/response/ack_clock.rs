//! Connection-level acknowledged-goodput samples for response paths.
//!
//! Data ACK timing informs path completion estimates. It never creates a
//! congestion window or replaces TCP/QUIC congestion control and recovery.

use super::attachment::ResponseStreamOutputs;
use crate::model::capacity::{PATH_OPEN_SCORE_BYTES, PathRateSample};
use crate::model::path::CarrierPathKey;
use crate::protocol::UnderlayProtocol;
use std::collections::HashMap;
use std::time::{Duration, Instant};

// Product ACKs can arrive in callback bursts after sitting in a control queue.
// Integrate across the burst instead of treating each callback as a new clock.
pub(super) const RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED: Duration = Duration::from_millis(100);
const RESPONSE_ACK_CLOCK_GOODPUT_MAX_ELAPSED: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ResponseAckClockRateEvidence {
    pending_bytes: u64,
    pending_first_sent_at: Instant,
    pending_last_sent_at: Instant,
    previous_window_acked_at: Option<Instant>,
    goodput_last_acked_at: Option<Instant>,
    goodput_bytes: u64,
    goodput_elapsed: Duration,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) enum ResponseAckClockRateEvidenceUpdate {
    Pending,
    Window,
}

impl ResponseAckClockRateEvidence {
    pub(super) fn new(first_sent_at: Instant) -> Self {
        Self {
            pending_bytes: 0,
            pending_first_sent_at: first_sent_at,
            pending_last_sent_at: first_sent_at,
            previous_window_acked_at: None,
            goodput_last_acked_at: None,
            goodput_bytes: 0,
            goodput_elapsed: Duration::ZERO,
        }
    }

    fn record_goodput_progress(&mut self, bytes: u64, acked_at: Instant) {
        let Some(previous_acked_at) = self.goodput_last_acked_at.replace(acked_at) else {
            // The first Data ACK can include arbitrary assignment residence.
            return;
        };
        let elapsed = acked_at.saturating_duration_since(previous_acked_at);
        if elapsed > RESPONSE_ACK_CLOCK_GOODPUT_MAX_ELAPSED {
            self.goodput_bytes = 0;
            self.goodput_elapsed = Duration::ZERO;
            return;
        }
        self.goodput_bytes = self.goodput_bytes.saturating_add(bytes);
        self.goodput_elapsed = self.goodput_elapsed.saturating_add(elapsed);
        while self.goodput_elapsed > RESPONSE_ACK_CLOCK_GOODPUT_MAX_ELAPSED {
            self.goodput_bytes = self.goodput_bytes.div_ceil(2);
            self.goodput_elapsed /= 2;
        }
    }

    pub(super) fn goodput_sample(&self) -> Option<PathRateSample> {
        (self.goodput_elapsed >= RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED)
            .then(|| PathRateSample::new(self.goodput_bytes, self.goodput_elapsed))
            .flatten()
    }

    #[cfg(test)]
    pub(super) fn observe(
        &mut self,
        bytes: u64,
        first_sent_at: Instant,
        last_sent_at: Instant,
        acked_at: Instant,
    ) -> ResponseAckClockRateEvidenceUpdate {
        self.observe_with_fresh_bytes(bytes, bytes, first_sent_at, last_sent_at, acked_at)
    }

    pub(super) fn observe_with_fresh_bytes(
        &mut self,
        bytes: u64,
        _fresh_bytes: u64,
        first_sent_at: Instant,
        last_sent_at: Instant,
        acked_at: Instant,
    ) -> ResponseAckClockRateEvidenceUpdate {
        self.record_goodput_progress(bytes, acked_at);
        if self.pending_bytes == 0 {
            self.pending_first_sent_at = first_sent_at;
            self.pending_last_sent_at = last_sent_at;
        } else {
            self.pending_first_sent_at = self.pending_first_sent_at.min(first_sent_at);
            self.pending_last_sent_at = self.pending_last_sent_at.max(last_sent_at);
        }
        self.pending_bytes = self.pending_bytes.saturating_add(bytes);
        if self.pending_bytes < PATH_OPEN_SCORE_BYTES as u64 {
            return ResponseAckClockRateEvidenceUpdate::Pending;
        }

        let sample_started_at = self
            .previous_window_acked_at
            .unwrap_or(self.pending_first_sent_at);
        let ack_clocked = self.previous_window_acked_at.is_none()
            || self.pending_last_sent_at <= sample_started_at;
        self.pending_bytes = 0;
        self.previous_window_acked_at = Some(acked_at);
        if ack_clocked {
            ResponseAckClockRateEvidenceUpdate::Window
        } else {
            ResponseAckClockRateEvidenceUpdate::Pending
        }
    }
}

/// Applies exact Data ACK samples while the caller holds the output lock.
pub(super) fn apply_response_ack_clock_release_samples(
    outputs: &mut ResponseStreamOutputs,
    path_samples: HashMap<(CarrierPathKey, u64), (u64, u64, Instant, Instant)>,
    now: Instant,
) {
    for ((key, output_incarnation), (bytes, _, first_sent_at, last_sent_at)) in path_samples {
        let Some(entry) = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == key && entry.incarnation == output_incarnation)
        else {
            continue;
        };

        match entry.key.underlay {
            UnderlayProtocol::Tcp => {
                let evidence = entry
                    .tcp_product_rate_evidence
                    .get_or_insert_with(|| ResponseAckClockRateEvidence::new(first_sent_at));
                evidence.observe_with_fresh_bytes(bytes, bytes, first_sent_at, last_sent_at, now);
                if let Some(sample) = evidence.goodput_sample() {
                    let rate_bps = sample.rate_bps();
                    entry.tcp_ack_clock_rate_bps = Some(rate_bps);
                    entry.product_progress_rate_bps = Some(rate_bps);
                    entry.delivery_rate_bps = Some(rate_bps);
                }
            }
            UnderlayProtocol::Udp => {
                let Some(sample) =
                    PathRateSample::new(bytes, now.saturating_duration_since(first_sent_at))
                else {
                    continue;
                };
                let sample_bps = sample.rate_bps();
                let carrier_app_limited = entry
                    .local_path_metrics
                    .is_some_and(|metrics| metrics.metrics.app_limited);
                entry.product_progress_rate_bps = Some(match entry.product_progress_rate_bps {
                    Some(previous) if carrier_app_limited => previous.max(sample_bps),
                    Some(previous) => previous.mul_add(0.75, sample_bps * 0.25),
                    None => sample_bps,
                });
            }
        }
    }
}

#[cfg(test)]
#[path = "ack_clock_test.rs"]
mod tests;
