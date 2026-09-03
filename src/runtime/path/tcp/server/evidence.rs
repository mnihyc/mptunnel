//! Transport evidence and typed capacity epochs for one server TCP carrier.
//!
//! Native metrics, path proofs, and capacity receipts share an exact socket
//! and registration lifetime; keeping them together prevents cross-path proof
//! publication.

use super::super::metrics::TcpMetricPublisher;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, RELIABLE_INITIAL_WINDOW_PACKETS,
    reliable_capacity_measurement_session_limit_bytes,
};
use crate::model::timing::transport_rate_sample_freshness_horizon;
use crate::mux::MuxLimits;
use crate::protocol::path_capacity::CapacityReceiveTracker;
use crate::protocol::{
    Frame, PATH_METRICS_MAX_RATE_VALID_FOR_US, PathId, PathMetricDirection, PathMetrics,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::model::metric_epoch_now;
use crate::runtime::path::proof::{PathProofTracker, path_proof_ack_frame};
use crate::runtime::path::server_context::ServerPathContext;
use crate::runtime::path::{
    CarrierDeliveryRateSample, CarrierNativeWindowSample, ServerCarrierPathRegistration,
};
use std::time::{Duration, Instant};

pub(in crate::runtime::path::tcp) struct ServerTcpEvidenceState {
    tcp_metrics: Option<TcpMetricPublisher>,
    local_metrics: Option<PathMetrics>,
    native_window_sample: Option<CarrierNativeWindowSample>,
    delivery_rate_sample: Option<CarrierDeliveryRateSample>,
    native_drain_observed: bool,
    sender_refresh_pending: bool,
    path_proofs: PathProofTracker,
    request_capacity_receive: CapacityReceiveTracker,
}

impl ServerTcpEvidenceState {
    pub(in crate::runtime::path::tcp) fn new(
        tcp_metrics: Option<TcpMetricPublisher>,
        local_metrics: Option<PathMetrics>,
        mux_limits: MuxLimits,
    ) -> Self {
        let observed_at = Instant::now();
        Self {
            tcp_metrics,
            local_metrics,
            native_window_sample: local_metrics.and_then(|metrics| {
                CarrierNativeWindowSample::from_path_metrics_at(metrics, observed_at)
            }),
            delivery_rate_sample: None,
            native_drain_observed: false,
            sender_refresh_pending: false,
            path_proofs: PathProofTracker::from_limits(mux_limits),
            request_capacity_receive: CapacityReceiveTracker::new(
                reliable_capacity_measurement_session_limit_bytes(mux_limits),
            ),
        }
    }

    pub(in crate::runtime::path::tcp) fn observe_periodic(
        &mut self,
        context: &ServerPathContext,
        path_registration: &ServerCarrierPathRegistration,
        path_id: PathId,
    ) {
        self.observe_sender(context, path_registration, path_id, false);
    }

    /// Kernel ACK progress does not wake the carrier actor. Poll only while a
    /// prior write can leave scheduler-visible native sender debt behind.
    pub(in crate::runtime::path::tcp) fn next_sender_observation_at(&self) -> Option<Instant> {
        if !self.sender_refresh_pending {
            return None;
        }
        self.tcp_metrics
            .as_ref()
            .map(TcpMetricPublisher::next_observation_at)
    }

    /// Publishes the same-socket state reached by a completed writer handoff.
    /// Writer debt is released only after this observation, so scheduling never
    /// sees a pre-write idle sample between the private batch and TCP queues.
    pub(in crate::runtime::path::tcp) fn observe_after_write(
        &mut self,
        context: &ServerPathContext,
        path_registration: &ServerCarrierPathRegistration,
        path_id: PathId,
    ) {
        self.sender_refresh_pending = self.tcp_metrics.is_some();
        self.observe_sender(context, path_registration, path_id, true);
    }

    fn observe_sender(
        &mut self,
        context: &ServerPathContext,
        path_registration: &ServerCarrierPathRegistration,
        path_id: PathId,
        force: bool,
    ) {
        let Some(observation) = self.tcp_metrics.as_mut().and_then(|publisher| {
            publisher.maybe_observe(path_id, PathMetricDirection::ServerToClient, force)
        }) else {
            return;
        };
        #[cfg(feature = "lab-diagnostics")]
        {
            if observation.retransmission_advanced() == Some(true) {
                lab_diagnostic(
                    "tcp_native_retransmission",
                    format_args!(
                        "session_id={} path_index={} path_instance_id={} direction={:?}",
                        path_registration.session_id().0,
                        path_id.0,
                        path_registration.path_instance_id().as_u64(),
                        observation.direction(),
                    ),
                );
            }
            lab_diagnostic(
                "tcp_sender_metrics",
                format_args!(
                    "session_id={} path_index={} path_instance_id={} direction={:?} newly_acked_bytes={} acked_bytes_since_epoch={} retransmission_advanced={:?} delivery_rate_mbps={:.3} pacing_rate_mbps={:.3} bytes_in_flight={} inflight_limit={} queue_bytes={} app_limited={}",
                    path_registration.session_id().0,
                    path_id.0,
                    path_registration.path_instance_id().as_u64(),
                    observation.direction(),
                    observation.newly_acked_bytes().unwrap_or(0),
                    observation.acked_bytes_since_epoch().unwrap_or(0),
                    observation.retransmission_advanced(),
                    observation.delivery_rate_bps().unwrap_or(0) as f64 / 1_000_000.0,
                    observation.pacing_rate_bps().unwrap_or(0) as f64 / 1_000_000.0,
                    observation.bytes_in_flight().unwrap_or(0),
                    observation.inflight_limit_bytes().unwrap_or(0),
                    observation.queue_bytes().unwrap_or(0),
                    observation.app_limited().unwrap_or(true),
                ),
            );
        }
        self.sender_refresh_pending = observation
            .bytes_in_flight()
            .is_some_and(|bytes_in_flight| bytes_in_flight > 0)
            || observation
                .queue_bytes()
                .is_some_and(|queue_bytes| queue_bytes > 0);
        let observed_at = Instant::now();
        self.observe_native_window_sample_at(observation, observed_at);
        self.observe_delivery_rate_sample_at(observation, observed_at);
        self.native_drain_observed = observation.has_native_drain_evidence();
        let Some(mut metrics) = merge_local_tcp_metrics(self.local_metrics, observation) else {
            return;
        };
        if let Some(sample) = self.delivery_rate_sample {
            project_tcp_delivery_rate_sample(&mut metrics, sample, observed_at);
        }
        self.local_metrics = Some(metrics);
        context
            .reliable_streams
            .record_local_path_metrics_with_native_evidence(
                path_registration,
                metrics,
                self.native_drain_observed,
                None,
                self.native_window_sample,
                self.delivery_rate_sample,
            );
    }

    fn observe_native_window_sample_at(
        &mut self,
        observation: super::super::metrics::TcpNativeObservation,
        observed_at: Instant,
    ) {
        let Some(inflight_limit_bytes) = observation.inflight_limit_bytes() else {
            // A partial RTT/queue/rate poll may update retained diagnostics,
            // but absence of a native window cannot refresh its authority.
            return;
        };
        let srtt_us = observation
            .srtt_us()
            .or_else(|| self.local_metrics.map(|metrics| metrics.srtt_us))
            .unwrap_or(1)
            .max(1);
        let rttvar_us = observation
            .rttvar_us()
            .or_else(|| self.local_metrics.map(|metrics| metrics.rttvar_us))
            .unwrap_or(srtt_us / 8);
        self.native_window_sample = CarrierNativeWindowSample::new(
            inflight_limit_bytes,
            observed_at,
            transport_rate_sample_freshness_horizon(
                Duration::from_micros(u64::from(srtt_us)),
                Duration::from_micros(u64::from(rttvar_us)),
            ),
        );
    }

    fn observe_delivery_rate_sample_at(
        &mut self,
        observation: super::super::metrics::TcpNativeObservation,
        observed_at: Instant,
    ) {
        if observation.app_limited() != Some(false) {
            return;
        }
        let Some(sample_bytes) = observation.newly_acked_bytes().filter(|bytes| *bytes > 0) else {
            return;
        };
        let Some(delivery_rate_bps) = observation.delivery_rate_bps() else {
            return;
        };
        let srtt_us = observation
            .srtt_us()
            .or_else(|| self.local_metrics.map(|metrics| metrics.srtt_us))
            .unwrap_or(1)
            .max(1);
        let rttvar_us = observation
            .rttvar_us()
            .or_else(|| self.local_metrics.map(|metrics| metrics.rttvar_us))
            .unwrap_or(srtt_us / 8);
        let freshness_horizon = transport_rate_sample_freshness_horizon(
            Duration::from_micros(u64::from(srtt_us)),
            Duration::from_micros(u64::from(rttvar_us)),
        );
        let previous = self
            .delivery_rate_sample
            .filter(|sample| observed_at < sample.expires_at);
        let expires_at = observed_at
            .checked_add(freshness_horizon)
            .unwrap_or(observed_at);
        self.delivery_rate_sample = Some(CarrierDeliveryRateSample {
            delivery_rate_bps,
            pacing_rate_bps: observation.pacing_rate_bps().filter(|rate| *rate > 0),
            sample_count: previous.map_or(1, |sample| sample.sample_count.saturating_add(1)),
            sample_bytes: previous.map_or(sample_bytes, |sample| {
                sample.sample_bytes.saturating_add(sample_bytes)
            }),
            delivery_window_covered: observation.delivery_window_covered()
                || previous.is_some_and(|sample| sample.delivery_window_covered),
            observed_at,
            expires_at,
        });
    }

    pub(in crate::runtime::path::tcp) fn record_sent_frame(&mut self, frame: &Frame) {
        self.path_proofs.record_sent_frame(frame);
    }

    pub(in crate::runtime::path::tcp) fn handle_path_proof_data(
        &self,
        path_id: PathId,
        proof_id: u64,
        payload_bytes: usize,
    ) -> Frame {
        path_proof_ack_frame(path_id, proof_id, payload_bytes)
    }

    pub(in crate::runtime::path::tcp) fn handle_path_proof_ack(
        &mut self,
        context: &ServerPathContext,
        path_registration: &ServerCarrierPathRegistration,
        path_id: PathId,
        proof_id: u64,
        payload_bytes: u32,
    ) {
        if let Some(observation) = self
            .path_proofs
            .acknowledge(path_id, proof_id, payload_bytes)
        {
            // Validation establishes carrier liveness only. Native TCP telemetry
            // and product ACKs retain ownership of transport scheduling evidence.
            context
                .reliable_streams
                .record_path_proof_success(path_registration, observation);
        }
    }

    pub(in crate::runtime::path::tcp) fn handle_request_capacity_data(
        &mut self,
        measurement_id: u64,
        payload_bytes: usize,
    ) -> Result<(), RuntimeError> {
        self.request_capacity_receive
            .record_data(measurement_id, payload_bytes)?;
        Ok(())
    }

    pub(in crate::runtime::path::tcp) fn handle_request_capacity_finish(
        &mut self,
        path_id: PathId,
        measurement_id: u64,
        payload_bytes: u64,
    ) -> Result<Frame, RuntimeError> {
        let received_payload_bytes = self
            .request_capacity_receive
            .finish(measurement_id, payload_bytes)?;
        Ok(Frame::PathCapacityReceipt {
            path_id,
            measurement_id,
            received_payload_bytes,
        })
    }

    pub(in crate::runtime::path::tcp) fn record_peer_metrics(
        &self,
        context: &ServerPathContext,
        path_registration: &ServerCarrierPathRegistration,
        metrics: PathMetrics,
    ) {
        context
            .reliable_streams
            .record_peer_path_metrics(path_registration, metrics);
    }

    pub(in crate::runtime::path::tcp) fn cancel_for_path_drain(&mut self) {
        self.sender_refresh_pending = false;
        self.path_proofs.cancel_for_path_drain();
        self.request_capacity_receive.cancel_for_path_drain();
    }

    pub(in crate::runtime::path::tcp) fn is_idle(&self) -> bool {
        self.path_proofs.is_idle() && self.request_capacity_receive.is_idle()
    }
}

fn project_tcp_delivery_rate_sample(
    metrics: &mut PathMetrics,
    sample: CarrierDeliveryRateSample,
    now: Instant,
) {
    let qualified_epoch =
        sample.sample_count > 0 && sample.sample_bytes > 0 && sample.observed_at <= now;
    metrics.delivery_rate_bps = sample.delivery_rate_bps.max(1);
    metrics.pacing_rate_bps = sample
        .pacing_rate_bps
        .unwrap_or(sample.delivery_rate_bps)
        .max(1);
    metrics.pacing_rate_observed = qualified_epoch && sample.pacing_rate_bps.is_some();
    metrics.metric_age_us = u32::try_from(
        now.saturating_duration_since(sample.observed_at)
            .as_micros(),
    )
    .unwrap_or(u32::MAX);
    metrics.rate_valid_for_us = if qualified_epoch {
        u64::try_from(sample.expires_at.saturating_duration_since(now).as_micros())
            .unwrap_or(u64::MAX)
            .min(PATH_METRICS_MAX_RATE_VALID_FOR_US)
    } else {
        0
    };
    metrics.rate_observed = qualified_epoch;
    let byte_confidence =
        (sample.sample_bytes as f64 / PATH_OPEN_SCORE_BYTES.max(1) as f64).clamp(0.0, 1.0);
    let count_confidence = (f64::from(sample.sample_count)
        / f64::from(RELIABLE_INITIAL_WINDOW_PACKETS as u32))
    .clamp(0.0, 1.0);
    metrics.confidence_ppm = (byte_confidence.min(count_confidence) * 1_000_000.0).round() as u32;
    metrics.app_limited = false;
}

fn merge_local_tcp_metrics(
    current: Option<PathMetrics>,
    observation: super::super::metrics::TcpNativeObservation,
) -> Option<PathMetrics> {
    if let Some(metrics) = observation.complete_path_metrics() {
        return Some(metrics);
    }
    if !observation.has_transport_shape() {
        return None;
    }
    let mut metrics = current?;
    observation.apply_transport_shape(&mut metrics);
    metrics.metric_epoch = metric_epoch_now();
    metrics.metric_age_us = 0;
    // A partial shape observation is not a new rate epoch. The retained typed
    // sample, when present, is projected immediately by the caller with its
    // original deadline.
    metrics.rate_valid_for_us = 0;
    metrics.rate_observed = false;
    metrics.pacing_rate_observed = false;
    metrics.pacing_rate_bps = metrics.delivery_rate_bps;
    Some(metrics)
}

#[cfg(test)]
#[path = "tests_evidence.rs"]
mod tests;
