use super::response_admission::{
    ResponseSenderPathTarget, reliable_quic_capacity_calibration_session_limit_bytes,
    server_output_has_bulk_rate_evidence_with_limits,
};
use super::{CarrierPathKey, ResponseStreamBinding, ServerCarrierPathInstanceId};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::protocol::{StreamOpenRole, UnderlayProtocol};
use crate::scheduler::FlowLane;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// Owns admission of carrier-only QUIC receipt probes. Product offsets and TCP
// ACK-clock calibration stay outside because they produce different proof.
static NEXT_RESPONSE_QUIC_CAPACITY_CALIBRATION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuicCapacityAdmissionState {
    Provisional,
    Admitted,
    Committed,
}

/// Rolls back only before carrier admission. Once frames own queue capacity,
/// cleanup may release serialization but must never refill discovery budget.
struct QuicCapacityAdmissionGuard<'a> {
    binding: &'a ResponseStreamBinding,
    path: CarrierPathKey,
    path_instance_id: ServerCarrierPathInstanceId,
    token: u64,
    ticket: super::QuicCapacityProbeCommandTicket,
    state: QuicCapacityAdmissionState,
}

impl QuicCapacityAdmissionGuard<'_> {
    fn mark_admitted(&mut self) {
        self.state = QuicCapacityAdmissionState::Admitted;
    }

    fn mark_committed(&mut self) {
        self.state = QuicCapacityAdmissionState::Committed;
    }
}

impl Drop for QuicCapacityAdmissionGuard<'_> {
    fn drop(&mut self) {
        match self.state {
            QuicCapacityAdmissionState::Provisional => {
                self.ticket.cancel();
                self.binding
                    .lane_tracker
                    .rollback_quic_capacity_calibration(
                        self.binding.session_id,
                        self.binding.binding_instance_id,
                        self.path,
                        self.path_instance_id,
                        self.token,
                    );
            }
            QuicCapacityAdmissionState::Admitted => {
                // The queue item remains charged, but a failed ownership lease
                // must prevent it from starting a now-unpublishable epoch.
                self.ticket.cancel();
                self.binding.lane_tracker.cancel_quic_capacity_calibration(
                    self.binding.session_id,
                    self.binding.binding_instance_id,
                    self.path,
                    self.path_instance_id,
                    "lease_commit_failed",
                );
            }
            QuicCapacityAdmissionState::Committed => {}
        }
    }
}

#[derive(Debug, Clone, Copy)]
/// QUIC capacity calibration consumes carrier bandwidth but no product offset.
/// Receiver-confirmed token receipt is authority; native carrier ACK telemetry
/// remains diagnostic timing evidence.
pub(in crate::runtime) struct ResponseQuicCapacityCalibrationRequest {
    pub(in crate::runtime) expected_planner_generation: u64,
    pub(in crate::runtime) expected_lane_generation: u64,
    pub(in crate::runtime) expected_model_generation: u64,
    pub(in crate::runtime) target: CarrierPathKey,
    pub(in crate::runtime) target_path_instance_id: ServerCarrierPathInstanceId,
    pub(in crate::runtime) target_incarnation: u64,
    pub(in crate::runtime) target_pending_bytes: u64,
    pub(in crate::runtime) train_bytes: usize,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) sample_floor_bytes: u64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) accounting_slack_bytes: u64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) fresh_strict_window_bytes: u64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) carrier_window_bytes: u64,
    pub(in crate::runtime) proof_validity: Duration,
    pub(in crate::runtime) lease: Duration,
}

impl ResponseStreamBinding {
    pub(in crate::runtime) fn try_start_quic_capacity_calibration(
        &self,
        target: &ResponseSenderPathTarget,
        request: ResponseQuicCapacityCalibrationRequest,
    ) -> bool {
        self.try_start_quic_capacity_calibration_with_lease(target, request, |lease| lease)
    }

    pub(super) fn try_start_quic_capacity_calibration_with_lease(
        &self,
        target: &ResponseSenderPathTarget,
        request: ResponseQuicCapacityCalibrationRequest,
        lease_after_admission: impl FnOnce(Duration) -> Duration,
    ) -> bool {
        let session_envelope = usize::try_from(
            reliable_quic_capacity_calibration_session_limit_bytes(self.mux_limits),
        )
        .unwrap_or(usize::MAX);
        if !self.response_stream_open.load(Ordering::Acquire)
            || request.target != target.key
            || request.target_path_instance_id != target.path_instance_id
            || request.target_incarnation != target.incarnation
            || request.train_bytes == 0
            || request.train_bytes > session_envelope
            || request.proof_validity.is_zero()
            || request.lease.is_zero()
        {
            return false;
        }
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        if !self.response_stream_open.load(Ordering::Acquire)
            || self.response_model_generation.load(Ordering::Acquire)
                != request.expected_model_generation
        {
            return false;
        }
        {
            let state = self
                .subflow_set
                .lock()
                .expect("server reliable stream subflow set lock");
            if state.planner_generation != request.expected_planner_generation {
                return false;
            }
        }
        // A QUIC train is connection-wide optional traffic. Exact Validation
        // identity plus the captured idle queue value isolates one carrier
        // epoch and fences stale proposals; neither is product-byte ownership.
        let target_is_exact_udp_validation = outputs.entries.iter().any(|entry| {
            entry.key == request.target
                && entry.path_instance_id == request.target_path_instance_id
                && entry.incarnation == request.target_incarnation
                && entry.commands.same_channel(&target.commands)
                && entry.role == StreamOpenRole::Validation
                && entry.key.underlay == UnderlayProtocol::Udp
                && !entry.commands.is_closed()
                && entry.commands.pending_bytes() == request.target_pending_bytes
                && !server_output_has_bulk_rate_evidence_with_limits(entry, self.mux_limits)
        });
        if !target_is_exact_udp_validation {
            return false;
        }
        if !target.commands.can_enqueue_lane_now(FlowLane::Throughput) {
            return false;
        }
        let calibration_id =
            NEXT_RESPONSE_QUIC_CAPACITY_CALIBRATION_ID.fetch_add(1, Ordering::Relaxed);
        let command_ticket = super::QuicCapacityProbeCommandTicket::new();
        #[cfg(feature = "lab-diagnostics")]
        let attempt_ordinal = target.quic_capacity_calibration_attempts.saturating_add(1);
        #[cfg(feature = "lab-diagnostics")]
        let selection = if attempt_ordinal == 1 {
            "fresh"
        } else {
            "retry"
        };
        #[cfg(feature = "lab-diagnostics")]
        let frame_count = request
            .train_bytes
            .div_ceil(self.mux_limits.max_payload_bytes.max(1));
        if !self.lane_tracker.try_reserve_quic_capacity_calibration(
            self.session_id,
            request.expected_lane_generation,
            self.binding_instance_id,
            request.target,
            request.target_path_instance_id,
            request.train_bytes as u64,
            request.sample_floor_bytes,
            request.accounting_slack_bytes,
            request.carrier_window_bytes,
            request.fresh_strict_window_bytes,
            request.proof_validity,
            session_envelope as u64,
            calibration_id,
            command_ticket.clone(),
        ) {
            return false;
        }
        let mut admission = QuicCapacityAdmissionGuard {
            binding: self,
            path: request.target,
            path_instance_id: request.target_path_instance_id,
            token: calibration_id,
            ticket: command_ticket.clone(),
            state: QuicCapacityAdmissionState::Provisional,
        };

        let Some(probe_expires_at) = Instant::now().checked_add(request.lease) else {
            return false;
        };
        let probe = super::QuicCapacityProbeCommand {
            owner: super::QuicCapacityProbeOwner::Response {
                binding_instance_id: self.binding_instance_id,
                path_instance_id: request.target_path_instance_id,
            },
            path_id: request.target.path_id,
            calibration_id,
            train_payload_bytes: request.train_bytes as u64,
            sample_floor_bytes: request.sample_floor_bytes,
            warmup_carrier_bytes: request.carrier_window_bytes,
            required_timed_carrier_bytes: request.fresh_strict_window_bytes,
            proof_validity: request.proof_validity,
            expires_at: probe_expires_at,
            ticket: command_ticket,
            cancel_on_drop: true,
        };
        if target
            .commands
            .try_enqueue_quic_capacity_probe(probe)
            .is_err()
        {
            return false;
        }
        admission.mark_admitted();
        // The carrier allocates and encodes only after this one typed command is
        // admitted, so failed reservations no longer build a throwaway train.
        let lease = lease_after_admission(request.lease);
        let Some(expires_at) = Instant::now().checked_add(lease) else {
            return false;
        };
        if !self.lane_tracker.commit_quic_capacity_calibration(
            self.session_id,
            self.binding_instance_id,
            request.target,
            request.target_path_instance_id,
            expires_at,
            calibration_id,
        ) {
            return false;
        }
        admission.mark_committed();
        // Publish after command admission so a new planner cannot reuse the
        // pre-calibration pending-byte/model snapshot.
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
        #[cfg(feature = "lab-diagnostics")]
        let session_spent_bytes = self
            .lane_tracker
            .response_scheduling_snapshot(self.session_id)
            .quic_capacity_calibration_spent_bytes;
        drop(outputs);
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "response_quic_capacity_calibration",
            format_args!(
                "phase=started session_id={} binding_instance_id={} path_id={} path_instance_id={} incarnation={} calibration_id={} attempt_ordinal={} selection={} train_bytes={} sample_floor_bytes={} accounting_slack_bytes={} fresh_strict_window_bytes={} carrier_window_bytes={} frame_count={} proof_validity_ms={} lease_ms={} lease_committed={} session_spent_bytes={} session_limit_bytes={}",
                self.session_id.0,
                self.binding_instance_id,
                request.target.path_id.0,
                request.target_path_instance_id.0,
                request.target_incarnation,
                calibration_id,
                attempt_ordinal,
                selection,
                request.train_bytes,
                request.sample_floor_bytes,
                request.accounting_slack_bytes,
                request.fresh_strict_window_bytes,
                request.carrier_window_bytes,
                frame_count,
                request.proof_validity.as_millis(),
                request.lease.as_millis(),
                true,
                session_spent_bytes,
                session_envelope,
            ),
        );
        self.notify_update();
        true
    }
}
