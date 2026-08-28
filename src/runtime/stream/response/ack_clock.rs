//! Connection-level acknowledged-goodput samples for response paths.
//!
//! Data ACK timing informs path completion estimates. It never creates a
//! congestion window or replaces TCP/QUIC congestion control and recovery.

use super::attachment::{
    ResponseProductRateEpoch, ResponseStreamOutputEntry, ResponseStreamOutputs,
};
use super::evidence::server_output_local_path_metrics;
use crate::model::capacity::{PATH_OPEN_SCORE_BYTES, PathRateSample};
use crate::model::path::CarrierPathKey;
use crate::model::timing::transport_rate_sample_freshness_horizon;
use crate::protocol::UnderlayProtocol;
use crate::runtime::path::model::default_path_srtt_ms;
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
    goodput_sample_count: u32,
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
            goodput_sample_count: 0,
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
            self.goodput_sample_count = 0;
            self.goodput_elapsed = Duration::ZERO;
            return;
        }
        self.goodput_bytes = self.goodput_bytes.saturating_add(bytes);
        self.goodput_sample_count = self.goodput_sample_count.saturating_add(1);
        self.goodput_elapsed = self.goodput_elapsed.saturating_add(elapsed);
        while self.goodput_elapsed > RESPONSE_ACK_CLOCK_GOODPUT_MAX_ELAPSED {
            self.goodput_bytes = self.goodput_bytes.div_ceil(2);
            self.goodput_sample_count = self.goodput_sample_count.div_ceil(2);
            self.goodput_elapsed /= 2;
        }
    }

    pub(super) fn goodput_sample(&self) -> Option<PathRateSample> {
        (self.goodput_elapsed >= RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED)
            .then(|| PathRateSample::new(self.goodput_bytes, self.goodput_elapsed))
            .flatten()
    }

    fn goodput_epoch_sample(&self) -> Option<(PathRateSample, u32)> {
        self.goodput_sample()
            .map(|sample| (sample, self.goodput_sample_count))
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

fn response_product_rate_freshness_horizon(entry: &ResponseStreamOutputEntry) -> Duration {
    // Capture the local carrier timing visible in this exact Data-ACK
    // transaction. The resulting epoch stores an absolute deadline, so later
    // transport-shape polls cannot rewrite its authority.
    let (srtt, rttvar) = server_output_local_path_metrics(entry).map_or_else(
        || {
            let srtt = Duration::from_secs_f64(
                entry
                    .srtt_ms
                    .unwrap_or_else(default_path_srtt_ms)
                    .max(0.001)
                    / 1000.0,
            );
            (srtt, srtt / 8)
        },
        |metrics| {
            (
                Duration::from_micros(u64::from(metrics.metrics.srtt_us.max(1))),
                Duration::from_micros(u64::from(metrics.metrics.rttvar_us)),
            )
        },
    );
    transport_rate_sample_freshness_horizon(srtt, rttvar)
}

fn install_response_product_rate_epoch(
    entry: &mut ResponseStreamOutputEntry,
    rate_bps: f64,
    sample_count: u32,
    sample_bytes: u64,
    now: Instant,
) {
    let freshness_horizon = response_product_rate_freshness_horizon(entry);
    entry.product_rate_epoch =
        ResponseProductRateEpoch::new(rate_bps, sample_count, sample_bytes, now, freshness_horizon);
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
                if entry
                    .product_rate_epoch
                    .is_some_and(|epoch| epoch.fresh_rate_at(now).is_none())
                {
                    // The first post-expiry ACK only seeds the new ACK clock;
                    // no byte or elapsed-time evidence crosses epochs.
                    entry.product_rate_epoch = None;
                    entry.tcp_product_rate_evidence =
                        Some(ResponseAckClockRateEvidence::new(first_sent_at));
                }
                let evidence = entry
                    .tcp_product_rate_evidence
                    .get_or_insert_with(|| ResponseAckClockRateEvidence::new(first_sent_at));
                evidence.observe_with_fresh_bytes(bytes, bytes, first_sent_at, last_sent_at, now);
                if let Some((sample, sample_count)) = evidence.goodput_epoch_sample() {
                    install_response_product_rate_epoch(
                        entry,
                        sample.rate_bps(),
                        sample_count,
                        sample.bytes(),
                        now,
                    );
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
                let previous_fresh_epoch = entry
                    .product_rate_epoch
                    .filter(|epoch| epoch.fresh_rate_at(now).is_some());
                let rate_bps = match previous_fresh_epoch {
                    Some(previous) if carrier_app_limited => previous.rate_bps.max(sample_bps),
                    Some(previous) => previous.rate_bps.mul_add(0.75, sample_bps * 0.25),
                    None => sample_bps,
                };
                let sample_count =
                    previous_fresh_epoch.map_or(1, |epoch| epoch.sample_count.saturating_add(1));
                let sample_bytes = previous_fresh_epoch
                    .map_or(bytes, |epoch| epoch.sample_bytes.saturating_add(bytes));
                install_response_product_rate_epoch(
                    entry,
                    rate_bps,
                    sample_count,
                    sample_bytes,
                    now,
                );
            }
        }
    }
}

#[cfg(test)]
#[path = "tests_ack_clock.rs"]
mod tests;
