//! Connection-level request read-ahead ownership.
//!
//! TCP and QUIC provide different evidence, but both grow this one bounded
//! product window after their feedback becomes authoritative.

use crate::model::admission::bulk_scheduling_window_bytes;
use crate::model::capacity::{
    QUIC_TIMER_GRANULARITY, reliable_path_startup_sample_limit_bytes, reliable_relay_buffer_len,
};
use crate::model::request_evidence::RequestWindowGrowthEvidence;
use crate::mux::MuxLimits;
use std::time::{Duration, Instant};

/// Connection-level product read-ahead authority for one request stream.
///
/// TCP and QUIC supply different evidence below this owner. Both grow the same
/// bounded product window only after their evidence becomes product-authoritative.
#[derive(Debug)]
pub(in crate::runtime) struct RequestOutstandingWindow {
    product_limit_bytes: usize,
    growth_epoch_at: Instant,
    acked_in_epoch: usize,
}

impl RequestOutstandingWindow {
    pub(in crate::runtime) fn new() -> Self {
        Self::new_at(Instant::now())
    }

    fn new_at(now: Instant) -> Self {
        Self {
            product_limit_bytes: 0,
            growth_epoch_at: now,
            acked_in_epoch: 0,
        }
    }

    pub(in crate::runtime) fn limit_bytes(
        &mut self,
        lane: crate::scheduler::TrafficClass,
        payload_bytes: usize,
        mux_limits: MuxLimits,
    ) -> usize {
        self.limit_bytes_at(lane, payload_bytes, mux_limits, Instant::now())
    }

    /// Applies one connection-level ACK decision without reading path state.
    pub(in crate::runtime) fn apply_growth_evidence(
        &mut self,
        evidence: RequestWindowGrowthEvidence,
        lane: crate::scheduler::TrafficClass,
        mux_limits: MuxLimits,
    ) {
        match evidence {
            RequestWindowGrowthEvidence::None => {}
            RequestWindowGrowthEvidence::AckCredits {
                bytes,
                growth_interval,
                observed_at,
            } => self.record_acked_at(bytes, lane, growth_interval, mux_limits, observed_at),
        }
    }

    fn limit_bytes_at(
        &mut self,
        lane: crate::scheduler::TrafficClass,
        payload_bytes: usize,
        mux_limits: MuxLimits,
        now: Instant,
    ) -> usize {
        let resource_ceiling = request_outstanding_resource_ceiling(mux_limits);
        let startup_reservoir = if lane.is_bulk() {
            bulk_scheduling_window_bytes(payload_bytes, mux_limits)
        } else {
            // Flow classification expects one full source queue; a smaller
            // bound would make latency probing a stop-and-wait prerequisite.
            reliable_relay_buffer_len(mux_limits)
        }
        .min(resource_ceiling)
        .max(1);
        let lane_demoted = !lane.is_bulk() && self.product_limit_bytes > startup_reservoir;
        if lane_demoted || self.product_limit_bytes < startup_reservoir {
            self.product_limit_bytes = startup_reservoir;
            self.growth_epoch_at = now;
            self.acked_in_epoch = 0;
        }
        self.product_limit_bytes.min(resource_ceiling).max(1)
    }

    #[allow(clippy::too_many_arguments)]
    fn record_acked_at(
        &mut self,
        released_bytes: usize,
        lane: crate::scheduler::TrafficClass,
        growth_interval: Duration,
        mux_limits: MuxLimits,
        now: Instant,
    ) {
        if released_bytes == 0 || !lane.is_bulk() {
            return;
        }
        let resource_ceiling = request_outstanding_resource_ceiling(mux_limits);
        if self.product_limit_bytes == 0 || self.product_limit_bytes >= resource_ceiling {
            return;
        }
        let growth_interval = growth_interval.max(QUIC_TIMER_GRANULARITY);
        if now.saturating_duration_since(self.growth_epoch_at) > growth_interval {
            self.growth_epoch_at = now;
            self.acked_in_epoch = 0;
            return;
        }
        self.acked_in_epoch = self.acked_in_epoch.saturating_add(released_bytes);
        let durable_product_floor =
            usize::try_from(reliable_path_startup_sample_limit_bytes(mux_limits))
                .unwrap_or(usize::MAX)
                .min(self.product_limit_bytes);
        let growth_threshold = self
            .product_limit_bytes
            .div_ceil(2)
            .max(durable_product_floor)
            .max(1);
        if self.acked_in_epoch < growth_threshold {
            return;
        }
        self.product_limit_bytes = self
            .product_limit_bytes
            .saturating_mul(2)
            .min(resource_ceiling)
            .max(1);
        self.growth_epoch_at = now;
        self.acked_in_epoch = 0;
    }
}

fn request_outstanding_resource_ceiling(mux_limits: MuxLimits) -> usize {
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    mux_limits
        .max_repair_bytes
        .min(mux_limits.max_path_flight_bytes)
        .min(stream_window)
        .max(1)
}

#[cfg(test)]
#[path = "flow_control_test.rs"]
mod tests;
