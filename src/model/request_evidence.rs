//! Request-direction product ACK and delivery-rate evidence.
//!
//! Carrier services attribute exact bytes and timestamps. This model advances
//! bounded evidence without reading sockets, queues, or mutable path state.

use super::ack_clock::reliable_ack_clock_measurement_rate_coverage_floor_bytes;
use super::capacity::{PATH_OPEN_SCORE_BYTES, PathRateSample};
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
}

pub(crate) enum RequestPathRateEvidenceUpdate {
    Pending,
    Proven {
        sample: Option<PathRateSample>,
        first_window: bool,
    },
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RequestPerFlowRateModel {
    pub(crate) rate_bps: f64,
    pub(crate) delivery_samples: u32,
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

    pub(crate) fn has_exact_path_provenance(&self) -> bool {
        self.exact_attributed_bytes >= PATH_OPEN_SCORE_BYTES as u64
    }

    pub(crate) fn seed_ack_boundary(&mut self, acked_at: Instant) {
        self.pending_bytes = 0;
        self.previous_window_acked_at = Some(acked_at);
    }
}

pub(crate) fn request_path_rate_coverage_floor_bytes(
    underlay: UnderlayProtocol,
    measurement_target: Option<u64>,
    mux_limits: MuxLimits,
) -> u64 {
    match underlay {
        UnderlayProtocol::Tcp => measurement_target.unwrap_or_else(|| {
            reliable_ack_clock_measurement_rate_coverage_floor_bytes(mux_limits)
        }),
        UnderlayProtocol::Udp => PATH_OPEN_SCORE_BYTES as u64,
    }
}

#[cfg(test)]
#[path = "request_evidence_test.rs"]
mod tests;
