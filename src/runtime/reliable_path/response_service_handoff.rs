use super::response_admission::{
    ResponseDispatchTarget, ResponseSenderPathTarget,
    server_bulk_output_snapshot_with_command_pending, server_output_fresh_quic_capacity_proof,
    server_output_has_bulk_rate_evidence_with_limits, server_output_quic_capacity_proof_marker,
};
use super::response_placement::{
    ResponseRateScope, ResponseServiceHandoffMode, response_rate_fair_share_bps,
    response_service_handoff_mode,
};
use super::{
    CarrierPathKey, QuicCapacityProofCandidate, ResponseStreamBinding, ServerCarrierPathInstanceId,
    quic_capacity_proof_pin_matches_marker, valid_quic_capacity_proof_candidate_at,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::protocol::{Frame, StreamOpenRole, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::multipath_model::FlowSubflowSet;
use crate::runtime::relay_striping::reliable_stream_frame_extent;
use crate::scheduler::FlowLane;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

// Owns atomic mutation of persistent response Service at a clear product
// frontier. Sender service ranks candidates; this module only revalidates and
// commits.
#[derive(Debug, Clone, Copy)]
/// Exact clear-frontier whole-flow handoff. It changes persistent response
/// Service ownership; it never authorizes adjacent cross-family Subflow bytes.
pub(in crate::runtime) struct ResponseServiceHandoffRequest {
    pub(in crate::runtime) expected_planner_generation: u64,
    pub(in crate::runtime) expected_lane_generation: u64,
    pub(in crate::runtime) expected_model_generation: u64,
    pub(in crate::runtime) handoff_frontier: u64,
    pub(in crate::runtime) service: CarrierPathKey,
    pub(in crate::runtime) service_path_instance_id: ServerCarrierPathInstanceId,
    pub(in crate::runtime) service_incarnation: u64,
    pub(in crate::runtime) target: CarrierPathKey,
    pub(in crate::runtime) target_path_instance_id: ServerCarrierPathInstanceId,
    pub(in crate::runtime) target_incarnation: u64,
    pub(in crate::runtime) mode: ResponseServiceHandoffMode,
    /// Shared queue pressure may fall after ranking, but it may not grow beyond
    /// the byte-credit envelope that admitted this frame.
    pub(in crate::runtime) target_command_pending_limit_bytes: u64,
    pub(in crate::runtime) capacity_proof: Option<QuicCapacityProofCandidate>,
}

#[derive(Debug, Clone, Copy)]
/// Bounded pause of fresh OwnerData assignment for one response binding while
/// already-owned ranges reach the STREAM_ACK frontier. Offset-free source
/// staging remains sender-service state, outside this transaction.
pub(in crate::runtime) struct ResponseServiceHandoffDrainRequest {
    pub(in crate::runtime) expected_planner_generation: u64,
    pub(in crate::runtime) expected_lane_generation: u64,
    pub(in crate::runtime) expected_model_generation: u64,
    pub(in crate::runtime) service: CarrierPathKey,
    pub(in crate::runtime) service_path_instance_id: ServerCarrierPathInstanceId,
    pub(in crate::runtime) service_incarnation: u64,
    pub(in crate::runtime) target: CarrierPathKey,
    pub(in crate::runtime) target_path_instance_id: ServerCarrierPathInstanceId,
    pub(in crate::runtime) target_incarnation: u64,
    pub(in crate::runtime) mode: ResponseServiceHandoffMode,
    pub(in crate::runtime) capacity_proof: Option<QuicCapacityProofCandidate>,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) outstanding_owner_bytes: u64,
    pub(in crate::runtime) lease: Duration,
}

impl ResponseStreamBinding {
    pub(in crate::runtime) fn binding_instance_id(&self) -> u64 {
        self.binding_instance_id
    }

    pub(in crate::runtime) fn response_service_handoff_open(&self) -> bool {
        self.response_service_handoff_open.load(Ordering::Acquire)
    }

    pub(in crate::runtime) fn response_service_handoff_drain_active(&self) -> bool {
        self.lane_tracker
            .response_scheduling_snapshot(self.session_id)
            .response_service_handoff_drain
            .is_some_and(|reservation| reservation.binding_instance_id == self.binding_instance_id)
    }

    pub(in crate::runtime) fn cancel_response_service_handoff_drain(
        &self,
        reason: &'static str,
    ) -> bool {
        let cleared = self
            .lane_tracker
            .clear_response_service_handoff_drain_for_binding(
                self.session_id,
                self.binding_instance_id,
            );
        if cleared {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "response_service_handoff",
                format_args!(
                    "phase=drain_cancelled session_id={} binding_instance_id={} reason={}",
                    self.session_id.0, self.binding_instance_id, reason,
                ),
            );
            #[cfg(not(feature = "lab-diagnostics"))]
            let _ = reason;
            self.notify_update();
        }
        cleared
    }

    pub(in crate::runtime) fn try_start_response_service_handoff_drain(
        &self,
        service: &ResponseSenderPathTarget,
        target: &ResponseSenderPathTarget,
        lane: FlowLane,
        request: ResponseServiceHandoffDrainRequest,
    ) -> bool {
        let now = Instant::now();
        let Some(expires_at) = now.checked_add(request.lease) else {
            return false;
        };
        if request.lease.is_zero()
            || !lane.is_bulk()
            || !self.response_stream_open.load(Ordering::Acquire)
            || !self.response_service_handoff_open.load(Ordering::Acquire)
            || self
                .response_service_handoff_drain_attempted
                .load(Ordering::Acquire)
            || request.service != service.key
            || request.service_path_instance_id != service.path_instance_id
            || request.service_incarnation != service.incarnation
            || request.target != target.key
            || request.target_path_instance_id != target.path_instance_id
            || request.target_incarnation != target.incarnation
            || request.capacity_proof.is_some_and(|proof| {
                target.key.underlay != UnderlayProtocol::Udp
                    || !valid_quic_capacity_proof_candidate_at(proof, now)
            })
            || request.service.underlay == request.target.underlay
            || !service.is_active
            || !service.has_bulk_rate_evidence
            || target.is_active
            || !target.has_bulk_rate_evidence
            || target.attachment_role != StreamOpenRole::Validation
            || target.owner_data_in_flight_bytes != 0
            || target.snapshot.product_bytes_in_flight != 0
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
            || outputs.active_ack_clock_calibration.is_some()
            || outputs.ack_clock_calibrations.values().any(|calibration| {
                !calibration.proven && !calibration.retired && calibration.spent_bytes > 0
            })
        {
            return false;
        }
        {
            let state = self
                .subflow_set
                .lock()
                .expect("server reliable stream subflow set lock");
            if state.planner_generation != request.expected_planner_generation
                || state
                    .set
                    .as_ref()
                    .and_then(FlowSubflowSet::startup_owner_key)
                    .is_some()
            {
                return false;
            }
        }
        let service_index = outputs.entries.iter().position(|entry| {
            entry.key == request.service
                && entry.path_instance_id == request.service_path_instance_id
                && entry.incarnation == request.service_incarnation
                && entry.commands.same_channel(&service.commands)
                && !entry.commands.is_closed()
                && server_output_has_bulk_rate_evidence_with_limits(entry, self.mux_limits)
        });
        let target_index = outputs.entries.iter().position(|entry| {
            entry.key == request.target
                && entry.path_instance_id == request.target_path_instance_id
                && entry.incarnation == request.target_incarnation
                && entry.commands.same_channel(&target.commands)
                && entry.role == StreamOpenRole::Validation
                && entry.owner_data_in_flight_bytes == 0
                && entry.bytes_in_flight == 0
                && !entry.commands.is_closed()
                && server_output_has_bulk_rate_evidence_with_limits(entry, self.mux_limits)
        });
        let (Some(service_index), Some(target_index)) = (service_index, target_index) else {
            return false;
        };
        if server_output_fresh_quic_capacity_proof(&outputs.entries[target_index])
            != request.capacity_proof
        {
            return false;
        }
        let service_model = server_bulk_output_snapshot_with_command_pending(
            &outputs.entries[service_index],
            self.session_id,
            lane,
            &self.lane_tracker,
            self.mux_limits,
            now,
            outputs.entries[service_index].commands.pending_bytes(),
        );
        let target_model = server_bulk_output_snapshot_with_command_pending(
            &outputs.entries[target_index],
            self.session_id,
            lane,
            &self.lane_tracker,
            self.mux_limits,
            now,
            outputs.entries[target_index].commands.pending_bytes(),
        );
        let service_snapshot = service_model.path;
        let target_snapshot = target_model.path;
        #[cfg(feature = "lab-diagnostics")]
        let service_bulk_flows = service_snapshot
            .active_flows
            .saturating_sub(service_snapshot.active_latency_sensitive_flows)
            .max(1);
        #[cfg(feature = "lab-diagnostics")]
        let target_bulk_flows = target_snapshot
            .active_flows
            .saturating_sub(target_snapshot.active_latency_sensitive_flows)
            .saturating_add(1)
            .max(1);
        let service_share_bps =
            response_rate_fair_share_bps(service_snapshot, service_model.rate_scope, false);
        let target_share_bps =
            response_rate_fair_share_bps(target_snapshot, target_model.rate_scope, true);
        let family_loads = self.response_scheduling_snapshot().service_family_loads;
        let handoff_mode = response_service_handoff_mode(
            request.service.underlay,
            service_share_bps,
            request.target.underlay,
            target_share_bps,
            family_loads,
        );
        if target_snapshot.active_latency_sensitive_flows != 0
            || target_snapshot.session_active_latency_sensitive_flows != 0
            || handoff_mode != Some(request.mode)
        {
            return false;
        }
        let lead = self
            .ordered_data_owner
            .lock()
            .expect("server reliable stream ordered data owner lock");
        if *lead != Some(request.service) {
            return false;
        }
        if self
            .response_service_handoff_drain_attempted
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        if !self
            .lane_tracker
            .try_reserve_response_service_handoff_drain(
                self.session_id,
                request.expected_lane_generation,
                self.binding_instance_id,
                request.service,
                request.service_path_instance_id,
                request.service_incarnation,
                request.target,
                request.target_path_instance_id,
                request.target_incarnation,
                request.capacity_proof,
                expires_at,
            )
        {
            self.response_service_handoff_drain_attempted
                .store(false, Ordering::Release);
            return false;
        }
        drop(lead);
        drop(outputs);
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "response_service_handoff",
            format_args!(
                "phase=drain_started session_id={} binding_instance_id={} handoff_mode={:?} capacity_proof_authority={} capacity_proof_token={} from_underlay={:?} from_path_id={} from_path_instance_id={} from_incarnation={} to_underlay={:?} to_path_id={} to_path_instance_id={} to_incarnation={} outstanding_owner_bytes={} lease_ms={} from_rate_bps={} from_bulk_flows={} from_share_bps={} to_rate_bps={} to_bulk_flows={} to_share_bps={} from_rate_mbps={:.3} from_share_mbps={:.3} to_rate_mbps={:.3} to_share_mbps={:.3}",
                self.session_id.0,
                self.binding_instance_id,
                request.mode,
                if request.capacity_proof.is_some() {
                    "exact_receipt"
                } else {
                    "generic_carrier"
                },
                request.capacity_proof.map_or(0, |proof| proof.token),
                request.service.underlay,
                request.service.path_id.0,
                request.service_path_instance_id.0,
                request.service_incarnation,
                request.target.underlay,
                request.target.path_id.0,
                request.target_path_instance_id.0,
                request.target_incarnation,
                request.outstanding_owner_bytes,
                request.lease.as_millis(),
                service_snapshot.delivery_rate_bps.round() as u64,
                service_bulk_flows,
                service_share_bps.round() as u64,
                target_snapshot.delivery_rate_bps.round() as u64,
                target_bulk_flows,
                target_share_bps.round() as u64,
                service_snapshot.delivery_rate_bps / 1_000_000.0,
                service_share_bps / 1_000_000.0,
                target_snapshot.delivery_rate_bps / 1_000_000.0,
                target_share_bps / 1_000_000.0,
            ),
        );
        self.notify_update();
        true
    }

    #[cfg(test)]
    pub(in crate::runtime) fn try_enqueue_response_service_handoff(
        &self,
        target: &ResponseSenderPathTarget,
        frame: &Frame,
        lane: FlowLane,
        request: ResponseServiceHandoffRequest,
    ) -> Result<(), RuntimeError> {
        self.try_enqueue_response_service_handoff_for_dispatch(&target.into(), frame, lane, request)
    }

    pub(in crate::runtime) fn try_enqueue_response_service_handoff_for_dispatch(
        &self,
        target: &ResponseDispatchTarget,
        frame: &Frame,
        lane: FlowLane,
        request: ResponseServiceHandoffRequest,
    ) -> Result<(), RuntimeError> {
        let Some((offset, _, _)) = reliable_stream_frame_extent(frame) else {
            return Err(RuntimeError::SenderServiceBlocked);
        };
        if !self.response_stream_open.load(Ordering::Acquire)
            || request.target != target.key
            || request.target_path_instance_id != target.path_instance_id
            || request.target_incarnation != target.incarnation
            || request.service.underlay == request.target.underlay
            || request.handoff_frontier != offset
            || !lane.is_bulk()
            || !self.response_service_handoff_open.load(Ordering::Acquire)
        {
            return Err(RuntimeError::SenderServiceBlocked);
        }

        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        if !self.response_stream_open.load(Ordering::Acquire)
            || self.response_model_generation.load(Ordering::Acquire)
                != request.expected_model_generation
        {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        {
            let state = self
                .subflow_set
                .lock()
                .expect("server reliable stream subflow set lock");
            if state.planner_generation != request.expected_planner_generation
                || state
                    .set
                    .as_ref()
                    .and_then(FlowSubflowSet::startup_owner_key)
                    .is_some()
            {
                return Err(RuntimeError::SenderServiceBlocked);
            }
        }
        if outputs.active_ack_clock_calibration.is_some()
            || outputs.ack_clock_calibrations.values().any(|calibration| {
                !calibration.proven && !calibration.retired && calibration.spent_bytes > 0
            })
            || outputs
                .entries
                .iter()
                .any(|entry| entry.owner_data_in_flight_bytes > 0)
        {
            return Err(RuntimeError::SenderServiceBlocked);
        }

        let scheduling = self
            .lane_tracker
            .response_scheduling_snapshot(self.session_id);
        let drain = scheduling
            .response_service_handoff_drain
            .filter(|reservation| reservation.binding_instance_id == self.binding_instance_id);
        if drain.is_some_and(|reservation| {
            reservation.service != request.service
                || reservation.service_path_instance_id != request.service_path_instance_id
                || reservation.service_incarnation != request.service_incarnation
                || reservation.target != request.target
                || reservation.target_path_instance_id != request.target_path_instance_id
                || reservation.target_incarnation != request.target_incarnation
                || reservation.capacity_proof != request.capacity_proof
        }) {
            drop(outputs);
            self.cancel_response_service_handoff_drain("reservation_identity_changed");
            return Err(RuntimeError::SenderServiceBlocked);
        }

        let service_index = outputs.entries.iter().position(|entry| {
            entry.key == request.service
                && entry.path_instance_id == request.service_path_instance_id
                && entry.incarnation == request.service_incarnation
                && !entry.commands.is_closed()
        });
        let target_index = outputs.entries.iter().position(|entry| {
            entry.key == request.target
                && entry.path_instance_id == request.target_path_instance_id
                && entry.incarnation == request.target_incarnation
                && entry.commands.same_channel(&target.commands)
                && entry.role == StreamOpenRole::Validation
                && entry.owner_data_in_flight_bytes == 0
                && entry.bytes_in_flight == 0
                && !entry.commands.is_closed()
        });
        let (Some(service_index), Some(target_index)) = (service_index, target_index) else {
            drop(outputs);
            if drain.is_some() {
                self.cancel_response_service_handoff_drain("reserved_path_changed");
            }
            return Err(RuntimeError::SenderServiceBlocked);
        };
        let now = Instant::now();
        let service_entry = &outputs.entries[service_index];
        let target_entry = &outputs.entries[target_index];
        if target_entry.commands.pending_bytes() > request.target_command_pending_limit_bytes {
            drop(outputs);
            if drain.is_some() {
                self.cancel_response_service_handoff_drain("target_credit_regressed");
            }
            return Err(RuntimeError::SenderServiceBlocked);
        }
        let ordinary_target_proof = server_output_fresh_quic_capacity_proof(target_entry);
        let target_proof_marker = server_output_quic_capacity_proof_marker(target_entry);
        let pinned_proof = drain.and_then(|reservation| reservation.capacity_proof);
        let pinned_marker_matches = pinned_proof.is_none_or(|proof| {
            quic_capacity_proof_pin_matches_marker(proof, target_proof_marker, now)
        });
        if !server_output_has_bulk_rate_evidence_with_limits(service_entry, self.mux_limits)
            || (drain.is_none()
                && (!server_output_has_bulk_rate_evidence_with_limits(
                    target_entry,
                    self.mux_limits,
                ) || request.capacity_proof != ordinary_target_proof))
            || (drain.is_some()
                && (request.capacity_proof != pinned_proof || !pinned_marker_matches))
        {
            drop(outputs);
            if drain.is_some() {
                self.cancel_response_service_handoff_drain("capacity_proof_changed");
            }
            return Err(RuntimeError::SenderServiceBlocked);
        }
        let service_model = server_bulk_output_snapshot_with_command_pending(
            service_entry,
            self.session_id,
            lane,
            &self.lane_tracker,
            self.mux_limits,
            now,
            service_entry.commands.pending_bytes(),
        );
        let mut target_model = server_bulk_output_snapshot_with_command_pending(
            target_entry,
            self.session_id,
            lane,
            &self.lane_tracker,
            self.mux_limits,
            now,
            target_entry.commands.pending_bytes(),
        );
        if let Some(proof) = pinned_proof {
            target_model.path.delivery_rate_bps = proof.rate_bps.max(1) as f64;
            target_model.rate_scope = ResponseRateScope::PathCapacity;
        }
        let service_snapshot = service_model.path;
        let target_snapshot = target_model.path;
        #[cfg(feature = "lab-diagnostics")]
        let service_bulk_flows = service_snapshot
            .active_flows
            .saturating_sub(service_snapshot.active_latency_sensitive_flows)
            .max(1);
        #[cfg(feature = "lab-diagnostics")]
        let target_bulk_flows = target_snapshot
            .active_flows
            .saturating_sub(target_snapshot.active_latency_sensitive_flows)
            .saturating_add(1)
            .max(1);
        let service_share_bps =
            response_rate_fair_share_bps(service_snapshot, service_model.rate_scope, false);
        let target_share_bps =
            response_rate_fair_share_bps(target_snapshot, target_model.rate_scope, true);
        let handoff_mode = response_service_handoff_mode(
            request.service.underlay,
            service_share_bps,
            request.target.underlay,
            target_share_bps,
            scheduling.service_family_loads,
        );
        if target_snapshot.active_latency_sensitive_flows != 0
            || target_snapshot.session_active_latency_sensitive_flows != 0
            || handoff_mode != Some(request.mode)
        {
            drop(outputs);
            if drain.is_some() {
                self.cancel_response_service_handoff_drain("target_capacity_regressed");
            }
            return Err(RuntimeError::SenderServiceBlocked);
        }
        #[cfg(feature = "lab-diagnostics")]
        let handoff_fair_share = (
            service_snapshot.delivery_rate_bps,
            service_bulk_flows,
            service_share_bps,
            target_snapshot.delivery_rate_bps,
            target_bulk_flows,
            target_share_bps,
            service_entry.commands.pending_bytes(),
            target_entry.commands.pending_bytes(),
        );
        {
            let flights = self
                .flights
                .lock()
                .expect("server reliable stream flight lock");
            if flights
                .values()
                .flatten()
                .any(|flight| flight.kind.is_ordering_owner())
            {
                return Err(RuntimeError::SenderServiceBlocked);
            }
        }
        {
            let ordering = self
                .ack_ordering
                .lock()
                .expect("server response ACK ordering lock");
            if ordering.contiguous_frontier != request.handoff_frontier
                || !ordering.acked_holes.is_empty()
            {
                return Err(RuntimeError::SenderServiceBlocked);
            }
        }
        let mut lead = self
            .ordered_data_owner
            .lock()
            .expect("server reliable stream ordered data owner lock");
        if !self.response_stream_open.load(Ordering::Acquire) || *lead != Some(request.service) {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        if !self.lane_tracker.try_move_response_service_handoff(
            self.session_id,
            request.expected_lane_generation,
            self.binding_instance_id,
            request.service,
            request.service_path_instance_id,
            request.service_incarnation,
            request.target,
            request.target_path_instance_id,
            request.target_incarnation,
            request.capacity_proof,
            lane,
        ) {
            drop(lead);
            drop(outputs);
            if drain.is_some() {
                self.cancel_response_service_handoff_drain("atomic_move_rejected");
            }
            return Err(RuntimeError::SenderServiceBlocked);
        }
        *lead = Some(request.target);
        if let Err(err) = target
            .commands
            .try_enqueue_stream_ordered_frame(frame.clone(), lane)
        {
            *lead = Some(request.service);
            drop(lead);
            self.lane_tracker.rollback_response_service_handoff(
                self.session_id,
                request.service,
                request.target,
                lane,
            );
            return Err(err);
        }

        self.response_service_handoff_open
            .store(false, Ordering::Release);
        self.response_flow_registration
            .commit_reserved_service_move(request.service, request.target, lane);
        self.reset_subflow_set_with_outputs(&mut outputs);
        self.record_validated_owner_flight_with_outputs(&mut outputs, target_index, frame);
        drop(lead);
        drop(outputs);
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "response_service_handoff",
            format_args!(
                "phase=committed session_id={} binding_instance_id={} handoff_mode={:?} capacity_proof_authority={} capacity_proof_token={} from_underlay={:?} from_path_id={} from_path_instance_id={} from_incarnation={} to_underlay={:?} to_path_id={} to_path_instance_id={} to_incarnation={} frontier={} owner_flight_bytes=0 acked_hole_bytes=0 from_pending_bytes={} to_pending_bytes={} from_rate_bps={} from_bulk_flows={} from_share_bps={} to_rate_bps={} to_bulk_flows={} to_share_bps={} from_rate_mbps={:.3} from_share_mbps={:.3} to_rate_mbps={:.3} to_share_mbps={:.3}",
                self.session_id.0,
                self.binding_instance_id,
                request.mode,
                if request.capacity_proof.is_some() {
                    "exact_receipt"
                } else {
                    "generic_carrier"
                },
                request.capacity_proof.map_or(0, |proof| proof.token),
                request.service.underlay,
                request.service.path_id.0,
                request.service_path_instance_id.0,
                request.service_incarnation,
                request.target.underlay,
                request.target.path_id.0,
                request.target_path_instance_id.0,
                request.target_incarnation,
                request.handoff_frontier,
                handoff_fair_share.6,
                handoff_fair_share.7,
                handoff_fair_share.0.round() as u64,
                handoff_fair_share.1,
                handoff_fair_share.2.round() as u64,
                handoff_fair_share.3.round() as u64,
                handoff_fair_share.4,
                handoff_fair_share.5.round() as u64,
                handoff_fair_share.0 / 1_000_000.0,
                handoff_fair_share.2 / 1_000_000.0,
                handoff_fair_share.3 / 1_000_000.0,
                handoff_fair_share.5 / 1_000_000.0,
            ),
        );
        self.notify_update();
        Ok(())
    }
}
