//! Request-direction product ACK and delivery-rate evidence.
//!
//! Carrier services attribute exact bytes and timestamps. This model advances
//! bounded evidence without reading sockets, queues, or mutable path state.

use super::ack_clock::reliable_data_ack_rate_coverage_floor_bytes;
use super::capacity::{
    PATH_OPEN_SCORE_BYTES, PathRateSample, product_delivery_samples_override_startup_prior,
};
use crate::mux::MuxLimits;
use crate::protocol::UnderlayProtocol;
use std::time::Instant;

#[derive(Debug, Clone, Copy)]
pub(crate) struct RequestPathRateEvidence {
    exact_attributed_bytes: u64,
    pending_bytes: u64,
    pending_first_sent_at: Instant,
    pending_latest_sent_at: Instant,
    previous_window_acked_at: Option<Instant>,
    successor_boundary_epoch: Option<(Instant, Instant)>,
}

pub(crate) enum RequestPathRateEvidenceUpdate {
    Pending,
    Proven {
        sample: Option<PathRateSample>,
        first_window: bool,
    },
}

/// Per-output Product goodput proven by one exact request Data-ACK epoch.
///
/// Expiry is frozen when the ACK is observed. The retained scalar remains
/// diagnostic history; numeric authority always goes through `fresh_rate_at`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct RequestProductRateEpoch {
    pub(crate) rate_bps: f64,
    pub(crate) delivery_samples: u32,
    pub(crate) observed_at: Instant,
    pub(crate) expires_at: Instant,
}

impl RequestProductRateEpoch {
    pub(crate) fn new(
        rate_bps: f64,
        delivery_samples: u32,
        observed_at: Instant,
        freshness_horizon: std::time::Duration,
    ) -> Option<Self> {
        (rate_bps.is_finite() && rate_bps > 0.0)
            .then(|| observed_at.checked_add(freshness_horizon))
            .flatten()
            .map(|expires_at| Self {
                rate_bps,
                delivery_samples,
                observed_at,
                expires_at,
            })
    }

    pub(crate) fn fresh_rate_at(self, now: Instant) -> Option<f64> {
        (self.observed_at <= now && now < self.expires_at).then_some(self.rate_bps)
    }

    /// Returns achieved Product completion service only after the exact
    /// request-output epoch has crossed its established sample boundary.
    /// `fresh_rate_at` intentionally remains available to the producer for
    /// raw EWMA history and diagnostics; scheduler authority uses this view.
    pub(crate) fn qualified_completion_rate_at(self, now: Instant) -> Option<f64> {
        product_delivery_samples_override_startup_prior(self.delivery_samples)
            .then(|| self.fresh_rate_at(now))
            .flatten()
    }

    #[cfg(test)]
    pub(crate) fn for_test(rate_bps: f64, delivery_samples: u32) -> Self {
        let observed_at = Instant::now() - std::time::Duration::from_secs(1);
        Self::new(
            rate_bps,
            delivery_samples,
            observed_at,
            std::time::Duration::from_secs(60 * 60),
        )
        .expect("valid test Product rate epoch")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RequestOwnerAckProgress<I> {
    pub(crate) instance: I,
    pub(crate) bytes: usize,
}

impl RequestPathRateEvidence {
    pub(crate) fn new(first_sent_at: Instant) -> Self {
        Self {
            exact_attributed_bytes: 0,
            pending_bytes: 0,
            pending_first_sent_at: first_sent_at,
            pending_latest_sent_at: first_sent_at,
            previous_window_acked_at: None,
            successor_boundary_epoch: None,
        }
    }

    pub(crate) fn observe(
        &mut self,
        bytes: u64,
        first_sent_at: Instant,
        latest_sent_at: Instant,
        acked_at: Instant,
        coverage_floor_bytes: u64,
        require_post_boundary_send: bool,
    ) -> RequestPathRateEvidenceUpdate {
        self.exact_attributed_bytes = self.exact_attributed_bytes.saturating_add(bytes);
        if self.pending_bytes == 0 {
            self.pending_first_sent_at = first_sent_at;
            self.pending_latest_sent_at = latest_sent_at;
        } else {
            self.pending_first_sent_at = self.pending_first_sent_at.min(first_sent_at);
            self.pending_latest_sent_at = self.pending_latest_sent_at.max(latest_sent_at);
        }
        self.pending_bytes = self.pending_bytes.saturating_add(bytes);
        let coverage_floor_bytes = coverage_floor_bytes.max(PATH_OPEN_SCORE_BYTES as u64);
        if self.pending_bytes < coverage_floor_bytes {
            return RequestPathRateEvidenceUpdate::Pending;
        }

        let sample_bytes = self.pending_bytes;
        let first_window = self.previous_window_acked_at.is_none();
        let sample_started_at = self
            .previous_window_acked_at
            .unwrap_or(self.pending_first_sent_at);
        // A later staged window is causal only when every sampled byte was sent
        // at or after the ACK that starts the interval. Charging the full
        // ACK-to-ACK gap is conservative when the sender was briefly idle.
        let ack_clocked = first_window
            || !require_post_boundary_send
            || self.pending_first_sent_at >= sample_started_at;
        self.pending_bytes = 0;
        self.previous_window_acked_at = Some(acked_at);
        let ack_elapsed = acked_at.saturating_duration_since(sample_started_at);
        let send_elapsed = self
            .pending_latest_sent_at
            .saturating_duration_since(self.pending_first_sent_at);
        // Product ACKs can arrive in compressed batches. Use the slower of the
        // send and ACK clocks so ACK timing alone cannot claim a rate above the
        // observed sender rate.
        let sample = ack_clocked
            .then(|| PathRateSample::new(sample_bytes, ack_elapsed.max(send_elapsed)))
            .flatten();
        RequestPathRateEvidenceUpdate::Proven {
            sample,
            first_window,
        }
    }

    #[cfg(test)]
    pub(crate) fn has_exact_path_provenance(&self) -> bool {
        self.exact_attributed_bytes >= PATH_OPEN_SCORE_BYTES as u64
    }

    pub(crate) fn seed_ack_boundary(&mut self, acked_at: Instant) {
        self.pending_bytes = 0;
        self.previous_window_acked_at = Some(acked_at);
    }

    /// Starts acquisition after one expired published Product epoch exactly
    /// once. Repeated sub-floor ACKs retain their pending bytes until the
    /// unchanged coverage floor is reached; a later distinct expired epoch
    /// receives its own boundary reset.
    pub(crate) fn seed_successor_epoch_boundary(&mut self, epoch: RequestProductRateEpoch) {
        let identity = (epoch.observed_at, epoch.expires_at);
        if self.successor_boundary_epoch == Some(identity) {
            return;
        }
        self.seed_ack_boundary(epoch.expires_at);
        self.successor_boundary_epoch = Some(identity);
    }
}

pub(crate) fn request_path_rate_coverage_floor_bytes(
    underlay: UnderlayProtocol,
    measurement_target: Option<u64>,
    mux_limits: MuxLimits,
) -> u64 {
    match underlay {
        UnderlayProtocol::Tcp => measurement_target
            .unwrap_or_else(|| reliable_data_ack_rate_coverage_floor_bytes(mux_limits)),
        UnderlayProtocol::Udp => PATH_OPEN_SCORE_BYTES as u64,
    }
}

#[cfg(test)]
#[path = "tests_request_evidence.rs"]
mod tests;
