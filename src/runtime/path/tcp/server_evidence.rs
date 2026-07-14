//! Transport evidence and typed capacity epochs for one server TCP carrier.
//!
//! Native metrics, path proofs, and capacity receipts share an exact socket
//! and registration lifetime; keeping them together prevents cross-path proof
//! publication.

use super::metrics::TcpMetricPublisher;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{
    TcpCapacityProofCandidate, reliable_capacity_calibration_session_limit_bytes,
};
use crate::mux::MuxLimits;
use crate::protocol::path_capacity::CapacityReceiveTracker;
use crate::protocol::{
    Frame, PathId, PathMetricDirection, PathMetrics, SessionId, UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{TcpCapacityProbeCommand, TcpCapacityProbeOwner};
use crate::runtime::path::proof::{PathProofTracker, path_proof_ack_frame, path_proof_metrics};
use crate::runtime::path::server_context::ServerPathContext;
use crate::runtime::path::tcp::capacity::{
    response_tcp_capacity_receipt_metrics, tcp_capacity_proof_validity,
    tcp_capacity_receipt_rate_bps,
};
use crate::runtime::stream::{ServerCarrierPathInstanceId, ServerCarrierPathRegistration};
use std::time::Instant;

pub(super) enum ServerTcpEvidenceOutcome {
    Handled,
    SkipCommandPoll,
}

struct PendingTcpCapacityProbe {
    probe: TcpCapacityProbeCommand,
    started_at: Instant,
}

pub(super) struct ServerTcpEvidenceState {
    // Release the typed carrier/session lease before exact-socket telemetry.
    response_capacity_probe: Option<PendingTcpCapacityProbe>,
    tcp_metrics: Option<TcpMetricPublisher>,
    startup_metrics: Option<PathMetrics>,
    local_metrics: Option<PathMetrics>,
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
            response_capacity_probe: None,
            tcp_metrics,
            startup_metrics: local_metrics,
            local_metrics,
            path_proofs: PathProofTracker::default(),
            request_capacity_receive: CapacityReceiveTracker::new(
                reliable_capacity_calibration_session_limit_bytes(mux_limits),
            ),
        }
    }

    pub(super) fn startup_metrics(&self) -> Option<PathMetrics> {
        self.startup_metrics
    }

    pub(super) fn response_probe_deadline(&self) -> Option<tokio::time::Instant> {
        self.response_capacity_probe
            .as_ref()
            .map(|pending| tokio::time::Instant::from_std(pending.probe.expires_at))
    }

    pub(super) fn has_response_probe(&self) -> bool {
        self.response_capacity_probe.is_some()
    }

    pub(super) fn publish_response_probe(
        &mut self,
        probe: TcpCapacityProbeCommand,
        started_at: Instant,
    ) {
        self.response_capacity_probe = Some(PendingTcpCapacityProbe { probe, started_at });
    }

    pub(super) fn log_response_probe_timeout(
        &self,
        session_id: SessionId,
        path_id: PathId,
        path_instance_id: ServerCarrierPathInstanceId,
    ) {
        #[cfg(not(feature = "lab-diagnostics"))]
        let _ = (session_id, path_id, path_instance_id);
        #[cfg(feature = "lab-diagnostics")]
        if let Some(pending) = self.response_capacity_probe.as_ref() {
            lab_diagnostic(
                "response_tcp_capacity_probe",
                format_args!(
                    "phase=rejected reason=receipt_timeout session_id={} path_id={} path_instance_id={} calibration_id={}",
                    session_id.0,
                    path_id.0,
                    path_instance_id.as_u64(),
                    pending.probe.calibration_id,
                ),
            );
        }
    }

    pub(super) fn observe_periodic(
        &mut self,
        context: &ServerPathContext,
        path_registration: &ServerCarrierPathRegistration,
        path_id: PathId,
    ) {
        if let Some(metrics) = self.tcp_metrics.as_mut().and_then(|publisher| {
            publisher.maybe_observe(path_id, PathMetricDirection::ServerToClient, false)
        }) {
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

    pub(super) fn handle_response_capacity_receipt(
        &mut self,
        context: &ServerPathContext,
        path_registration: &ServerCarrierPathRegistration,
        session_id: SessionId,
        path_id: PathId,
        calibration_id: u64,
        received_payload_bytes: u64,
    ) -> Result<ServerTcpEvidenceOutcome, RuntimeError> {
        #[cfg(not(feature = "lab-diagnostics"))]
        let _ = session_id;
        let Some(pending) = self.response_capacity_probe.take() else {
            return Err(RuntimeError::Protocol(
                "TCP capacity receipt has no active epoch",
            ));
        };
        let TcpCapacityProbeOwner::Response { path_instance_id } = pending.probe.owner else {
            return Err(RuntimeError::Protocol(
                "server TCP path received a request-owned capacity receipt",
            ));
        };
        if path_instance_id != path_registration.path_instance_id()
            || pending.probe.calibration_id != calibration_id
            || pending.probe.train_payload_bytes != received_payload_bytes
        {
            return Err(RuntimeError::Protocol(
                "TCP capacity receipt does not match active epoch",
            ));
        }
        if Instant::now() >= pending.probe.expires_at {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "response_tcp_capacity_probe",
                format_args!(
                    "phase=rejected reason=expired_before_receipt session_id={} path_id={} path_instance_id={} calibration_id={}",
                    session_id.0,
                    path_id.0,
                    path_registration.path_instance_id().as_u64(),
                    calibration_id,
                ),
            );
            return Ok(ServerTcpEvidenceOutcome::SkipCommandPoll);
        }
        let elapsed = pending.started_at.elapsed();
        let receipt_rate_bps = tcp_capacity_receipt_rate_bps(received_payload_bytes, elapsed)
            .ok_or(RuntimeError::Protocol(
                "TCP capacity receipt has invalid timing",
            ))?;
        let native_metrics = self.tcp_metrics.as_mut().and_then(|publisher| {
            publisher.maybe_observe(path_id, PathMetricDirection::ServerToClient, true)
        });
        #[cfg(feature = "lab-diagnostics")]
        let kernel_delivery_rate_bps =
            native_metrics.map_or(0, |metrics| metrics.delivery_rate_bps);
        #[cfg(feature = "lab-diagnostics")]
        let kernel_pacing_rate_bps = native_metrics.map_or(0, |metrics| metrics.pacing_rate_bps);
        let metrics = response_tcp_capacity_receipt_metrics(
            path_id,
            received_payload_bytes,
            receipt_rate_bps,
            self.local_metrics,
            native_metrics,
        );
        let rate_bps = metrics.delivery_rate_bps;
        let accepted_at = Instant::now();
        let validity = tcp_capacity_proof_validity(metrics);
        let candidate = TcpCapacityProofCandidate {
            token: calibration_id,
            train_bytes: pending.probe.train_payload_bytes,
            received_bytes: received_payload_bytes,
            rate_sample_bytes: received_payload_bytes,
            proof_elapsed: elapsed,
            receipt_rate_bps,
            rate_bps,
            accepted_at,
            expires_at: accepted_at.checked_add(validity).unwrap_or(accepted_at),
        };
        // Release carrier and session discovery ownership before proof
        // publication wakes the sender.
        drop(pending);
        if !context.reliable_streams.record_local_tcp_capacity_proof(
            path_registration,
            metrics,
            candidate,
        ) {
            return Err(RuntimeError::Protocol(
                "TCP capacity proof publication was rejected",
            ));
        }
        self.local_metrics = Some(metrics);
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "response_tcp_capacity_probe",
            format_args!(
                "phase=confirmed session_id={} path_id={} path_instance_id={} calibration_id={} train_bytes={} elapsed_ms={} receipt_rate_mbps={:.3} published_rate_mbps={:.3} kernel_delivery_rate_mbps={:.3} kernel_pacing_rate_mbps={:.3} srtt_ms={:.3} inflight_limit_bytes={} app_limited={}",
                session_id.0,
                path_id.0,
                path_registration.path_instance_id().as_u64(),
                calibration_id,
                received_payload_bytes,
                elapsed.as_millis(),
                receipt_rate_bps as f64 / 1_000_000.0,
                rate_bps as f64 / 1_000_000.0,
                kernel_delivery_rate_bps as f64 / 1_000_000.0,
                kernel_pacing_rate_bps as f64 / 1_000_000.0,
                metrics.srtt_us as f64 / 1_000.0,
                metrics.inflight_limit_bytes,
                metrics.app_limited,
            ),
        );
        Ok(ServerTcpEvidenceOutcome::Handled)
    }

    pub(super) fn handle_request_capacity_data(
        &mut self,
        calibration_id: u64,
        payload_bytes: usize,
    ) -> Result<(), RuntimeError> {
        self.request_capacity_receive
            .record_data(calibration_id, payload_bytes)?;
        Ok(())
    }

    pub(super) fn handle_request_capacity_finish(
        &mut self,
        path_id: PathId,
        calibration_id: u64,
        payload_bytes: u64,
    ) -> Result<Frame, RuntimeError> {
        let received_payload_bytes = self
            .request_capacity_receive
            .finish(calibration_id, payload_bytes)?;
        Ok(Frame::PathCapacityReceipt {
            path_id,
            calibration_id,
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
            .record_path_metrics(path_registration, metrics);
    }
}
