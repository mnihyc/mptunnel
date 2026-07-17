//! Transport evidence and typed capacity epochs for one server TCP carrier.
//!
//! Native metrics, path proofs, and capacity receipts share an exact socket
//! and registration lifetime; keeping them together prevents cross-path proof
//! publication.

use super::metrics::TcpMetricPublisher;
use crate::model::capacity::reliable_capacity_measurement_session_limit_bytes;
use crate::mux::MuxLimits;
use crate::protocol::path_capacity::CapacityReceiveTracker;
use crate::protocol::{Frame, PathId, PathMetricDirection, PathMetrics, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ServerCarrierPathRegistration;
use crate::runtime::path::proof::{PathProofTracker, path_proof_ack_frame, path_proof_metrics};
use crate::runtime::path::server_context::ServerPathContext;
use std::time::Instant;

pub(super) struct ServerTcpEvidenceState {
    tcp_metrics: Option<TcpMetricPublisher>,
    local_metrics: Option<PathMetrics>,
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
        self.sender_refresh_pending = observation
            .flight()
            .is_some_and(|(bytes_in_flight, _, _)| bytes_in_flight > 0)
            || observation
                .queue_bytes()
                .is_some_and(|queue_bytes| queue_bytes > 0);
        if let Some(metrics) = observation.complete_path_metrics() {
            self.local_metrics = Some(metrics);
            context
                .reliable_streams
                .record_local_path_metrics(path_registration, metrics);
        }
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
            context
                .reliable_streams
                .record_local_path_metrics(path_registration, metrics);
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
