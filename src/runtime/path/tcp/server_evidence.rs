//! Transport evidence and typed capacity epochs for one server TCP carrier.
//!
//! Native metrics, path proofs, and capacity receipts share an exact socket
//! and registration lifetime; keeping them together prevents cross-path proof
//! publication.

use super::metrics::TcpMetricPublisher;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::reliable_capacity_measurement_session_limit_bytes;
use crate::mux::MuxLimits;
use crate::protocol::path_capacity::CapacityReceiveTracker;
use crate::protocol::{Frame, PathId, PathMetricDirection, PathMetrics, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ServerCarrierPathRegistration;
use crate::runtime::path::model::metric_epoch_now;
use crate::runtime::path::proof::{PathProofTracker, path_proof_ack_frame, path_proof_metrics};
use crate::runtime::path::server_context::ServerPathContext;
use std::time::Instant;

pub(super) struct ServerTcpEvidenceState {
    tcp_metrics: Option<TcpMetricPublisher>,
    local_metrics: Option<PathMetrics>,
    native_drain_observed: bool,
    sender_refresh_pending: bool,
    path_proofs: PathProofTracker,
    request_capacity_receive: CapacityReceiveTracker,
}

impl ServerTcpEvidenceState {
    pub(super) fn new(
        tcp_metrics: Option<TcpMetricPublisher>,
        local_metrics: Option<PathMetrics>,
        mux_limits: MuxLimits,
    ) -> Self {
        Self {
            tcp_metrics,
            local_metrics,
            native_drain_observed: false,
            sender_refresh_pending: false,
            path_proofs: PathProofTracker::default(),
            request_capacity_receive: CapacityReceiveTracker::new(
                reliable_capacity_measurement_session_limit_bytes(mux_limits),
            ),
        }
    }

    pub(super) fn observe_periodic(
        &mut self,
        context: &ServerPathContext,
        path_registration: &ServerCarrierPathRegistration,
        path_id: PathId,
    ) {
        self.observe_sender(context, path_registration, path_id, false);
    }

    /// Kernel ACK progress does not wake the carrier actor. Poll only while a
    /// prior write can leave scheduler-visible native sender debt behind.
    pub(super) fn next_sender_observation_at(&self) -> Option<Instant> {
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
    pub(super) fn observe_after_write(
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
        self.native_drain_observed = observation.has_native_drain_evidence();
        let Some(metrics) = merge_local_tcp_metrics(self.local_metrics, observation) else {
            return;
        };
        self.local_metrics = Some(metrics);
        context.reliable_streams.record_local_path_metrics(
            path_registration,
            metrics,
            self.native_drain_observed,
        );
    }

    pub(super) fn record_sent_frame(&mut self, frame: &Frame) {
        self.path_proofs.record_sent_frame(frame);
    }

    pub(super) fn handle_path_proof_data(
        &self,
        path_id: PathId,
        proof_id: u64,
        payload_bytes: usize,
    ) -> Frame {
        path_proof_ack_frame(path_id, proof_id, payload_bytes)
    }

    pub(super) fn handle_path_proof_ack(
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
            && let Some(metrics) = path_proof_metrics(
                path_id,
                UnderlayProtocol::Tcp,
                PathMetricDirection::ServerToClient,
                observation,
            )
        {
            self.local_metrics = Some(metrics);
            context.reliable_streams.record_local_path_metrics(
                path_registration,
                metrics,
                self.native_drain_observed,
            );
        }
    }

    pub(super) fn handle_request_capacity_data(
        &mut self,
        measurement_id: u64,
        payload_bytes: usize,
    ) -> Result<(), RuntimeError> {
        self.request_capacity_receive
            .record_data(measurement_id, payload_bytes)?;
        Ok(())
    }

    pub(super) fn handle_request_capacity_finish(
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

    pub(super) fn record_peer_metrics(
        &self,
        context: &ServerPathContext,
        path_registration: &ServerCarrierPathRegistration,
        metrics: PathMetrics,
    ) {
        context
            .reliable_streams
            .record_peer_path_metrics(path_registration, metrics);
    }
}

fn merge_local_tcp_metrics(
    current: Option<PathMetrics>,
    observation: super::metrics::TcpNativeObservation,
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
    Some(metrics)
}

#[cfg(test)]
#[path = "server_evidence_test.rs"]
mod tests;
