//! Response Service handoff reservation and atomic ownership commit.
//!
//! One algorithm keeps bounded drain validation and final binding/output/queue
//! commit together. The session coordinator owns expiry and serializes the
//! stored operation slot with Service-load movement.

use super::attachment::{ResponseDispatchTarget, ResponseSenderPathTarget};
use super::evidence::{
    server_output_fresh_quic_capacity_proof, server_output_quic_capacity_proof_marker,
};
use super::quic_capacity::{
    quic_capacity_proof_pin_matches_marker, valid_quic_capacity_proof_candidate_at,
};
use super::session::{ResponseServiceHandoffDrainReservation, ServerPathLaneTracker};
use super::snapshot::server_bulk_output_snapshot_with_command_pending;
use super::subflow::server_output_has_bulk_rate_evidence_with_limits;
use super::{
    ResponseServiceHandoffDrainRequest, ResponseServiceHandoffRequest, ResponseStreamBinding,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::QuicCapacityProofCandidate;
use crate::model::multipath::FlowSubflowSet;
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey};
use crate::model::response::{response_rate_fair_share_bps, response_service_handoff_mode};
use crate::protocol::frame::reliable_stream_frame_extent;
use crate::protocol::{Frame, SessionId, StreamOpenRole, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::scheduler::{FlowLane, PathRateScope};
use std::sync::atomic::Ordering;
use std::time::Instant;

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
            || request.service != service.observation.key
            || request.service_path_instance_id != service.observation.path_instance_id
            || request.service_incarnation != service.observation.incarnation
            || request.target != target.observation.key
            || request.target_path_instance_id != target.observation.path_instance_id
            || request.target_incarnation != target.observation.incarnation
            || request.capacity_proof.is_some_and(|proof| {
                target.observation.key.underlay != UnderlayProtocol::Udp
                    || !valid_quic_capacity_proof_candidate_at(proof, now)
            })
            || request.service.underlay == request.target.underlay
            || !service.observation.is_service
            || !service.observation.has_bulk_rate_evidence
            || target.observation.is_service
            || !target.observation.has_bulk_rate_evidence
            || target.observation.attachment_role != StreamOpenRole::Validation
            || target.observation.owner_data_in_flight_bytes != 0
            || target.observation.snapshot.product_bytes_in_flight != 0
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
                && !entry.commands.is_closed()
                && server_output_has_bulk_rate_evidence_with_limits(entry, self.mux_limits)
        });
        let target_index = outputs.entries.iter().position(|entry| {
            entry.key == request.target
                && entry.path_instance_id == request.target_path_instance_id
                && entry.incarnation == request.target_incarnation
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
            outputs.product_queue_bytes,
            self.session_id,
            lane,
            &self.lane_tracker,
            self.mux_limits,
            outputs.entries[service_index].commands.pending_bytes(),
        );
        let target_model = server_bulk_output_snapshot_with_command_pending(
            &outputs.entries[target_index],
            outputs.product_queue_bytes,
            self.session_id,
            lane,
            &self.lane_tracker,
            self.mux_limits,
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
            response_rate_fair_share_bps(service_snapshot, service_snapshot.rate_scope, false);
        let target_share_bps =
            response_rate_fair_share_bps(target_snapshot, target_snapshot.rate_scope, true);
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
                request.service_path_instance_id.as_u64(),
                request.service_incarnation,
                request.target.underlay,
                request.target.path_id.0,
                request.target_path_instance_id.as_u64(),
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
}

impl ResponseStreamBinding {
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
        let target_commands = outputs.entries[target_index].commands.clone();
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
            outputs.product_queue_bytes,
            self.session_id,
            lane,
            &self.lane_tracker,
            self.mux_limits,
            service_entry.commands.pending_bytes(),
        );
        let mut target_model = server_bulk_output_snapshot_with_command_pending(
            target_entry,
            outputs.product_queue_bytes,
            self.session_id,
            lane,
            &self.lane_tracker,
            self.mux_limits,
            target_entry.commands.pending_bytes(),
        );
        if let Some(proof) = pinned_proof {
            target_model.path.delivery_rate_bps = proof.rate_bps.max(1) as f64;
            target_model.path.rate_scope = PathRateScope::PathCapacity;
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
            response_rate_fair_share_bps(service_snapshot, service_snapshot.rate_scope, false);
        let target_share_bps =
            response_rate_fair_share_bps(target_snapshot, target_snapshot.rate_scope, true);
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
        if let Err(err) = target_commands.try_enqueue_stream_ordered_frame(frame.clone(), lane) {
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
                request.service_path_instance_id.as_u64(),
                request.service_incarnation,
                request.target.underlay,
                request.target.path_id.0,
                request.target_path_instance_id.as_u64(),
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

impl ServerPathLaneTracker {
    #[cfg(test)]
    pub(super) fn set_response_service_handoff_drain_expiry_for_test(
        &self,
        session_id: SessionId,
        current: ResponseServiceHandoffDrainReservation,
        expires_at: Instant,
    ) -> bool {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let Some(reservation) = state
            .session_mut(session_id)
            .and_then(|session| session.response_service_handoff_drain_mut())
        else {
            return false;
        };
        if *reservation != current {
            return false;
        }
        reservation.expires_at = expires_at;
        true
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn try_reserve_response_service_handoff_drain(
        &self,
        session_id: SessionId,
        expected_generation: u64,
        binding_instance_id: u64,
        service: CarrierPathKey,
        service_path_instance_id: CarrierPathInstanceId,
        service_incarnation: u64,
        target: CarrierPathKey,
        target_path_instance_id: CarrierPathInstanceId,
        target_incarnation: u64,
        capacity_proof: Option<QuicCapacityProofCandidate>,
        expires_at: Instant,
    ) -> bool {
        let now = Instant::now();
        if service == target
            || service.underlay == target.underlay
            || expires_at <= now
            || capacity_proof.is_some_and(|proof| {
                target.underlay != UnderlayProtocol::Udp
                    || !valid_quic_capacity_proof_candidate_at(proof, now)
            })
        {
            return false;
        }
        let mut state = self.state.lock().expect("server path lane tracker lock");
        if expires_at <= Instant::now() {
            return false;
        }
        let generation = state.generation(session_id);
        // The tracker serializes a decision made from the matching generation;
        // family/rate policy belongs to the sender snapshot that ranked it.
        if generation != expected_generation {
            return false;
        }
        let session = state.session_mut_or_default(session_id);
        if !session.reserve_response_service_handoff_drain(ResponseServiceHandoffDrainReservation {
            binding_instance_id,
            service,
            service_path_instance_id,
            service_incarnation,
            target,
            target_path_instance_id,
            target_incarnation,
            capacity_proof,
            expires_at,
        }) {
            return false;
        }
        session.bump_generation();
        true
    }

    pub(super) fn clear_response_service_handoff_drain_for_binding(
        &self,
        session_id: SessionId,
        binding_instance_id: u64,
    ) -> bool {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let matches = state
            .session(session_id)
            .and_then(|session| session.response_service_handoff_drain())
            .is_some_and(|reservation| reservation.binding_instance_id == binding_instance_id);
        if matches {
            state
                .session_mut(session_id)
                .and_then(|session| session.take_response_service_handoff_drain());
            state.bump_generation(session_id);
        }
        matches
    }

    pub(super) fn clear_response_service_handoff_drain_for_path(
        &self,
        session_id: SessionId,
        binding_instance_id: u64,
        path: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
    ) -> bool {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let matches = state
            .session(session_id)
            .and_then(|session| session.response_service_handoff_drain())
            .is_some_and(|reservation| {
                reservation.binding_instance_id == binding_instance_id
                    && ((reservation.service == path
                        && reservation.service_path_instance_id == path_instance_id)
                        || (reservation.target == path
                            && reservation.target_path_instance_id == path_instance_id))
            });
        if matches {
            state
                .session_mut(session_id)
                .and_then(|session| session.take_response_service_handoff_drain());
            state.bump_generation(session_id);
        }
        matches
    }

    pub(super) fn try_move_response_service_handoff(
        &self,
        session_id: SessionId,
        expected_generation: u64,
        binding_instance_id: u64,
        from: CarrierPathKey,
        from_path_instance_id: CarrierPathInstanceId,
        from_incarnation: u64,
        to: CarrierPathKey,
        to_path_instance_id: CarrierPathInstanceId,
        to_incarnation: u64,
        capacity_proof: Option<QuicCapacityProofCandidate>,
        lane: FlowLane,
    ) -> bool {
        if from.underlay == to.underlay || from == to {
            return false;
        }
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let generation = state.generation(session_id);
        if generation != expected_generation || state.quic_capacity_calibration_reserved(session_id)
        {
            return false;
        }
        // Generation equality proves the caller ranked the same family-load
        // state. This mutation layer owns serialization, not placement policy.
        let now = Instant::now();
        let drain_expired = state
            .session(session_id)
            .and_then(|session| session.response_service_handoff_drain())
            .is_some_and(|reservation| now >= reservation.expires_at);
        if drain_expired {
            state
                .session_mut(session_id)
                .and_then(|session| session.take_response_service_handoff_drain());
            state.bump_generation(session_id);
            return false;
        }
        let drain = state
            .session(session_id)
            .and_then(|session| session.response_service_handoff_drain())
            .copied();
        // A direct move has no transaction lease to retain receipt authority,
        // so freshness must still hold at this final serialized mutation. An
        // exact drain instead owns the frozen proof until its own bounded lease.
        if drain.is_none()
            && capacity_proof.is_some_and(|proof| {
                to.underlay != UnderlayProtocol::Udp
                    || !valid_quic_capacity_proof_candidate_at(proof, now)
            })
        {
            return false;
        }
        let drain_matches = drain.is_none_or(|reservation| {
            reservation.binding_instance_id == binding_instance_id
                && reservation.service == from
                && reservation.service_path_instance_id == from_path_instance_id
                && reservation.service_incarnation == from_incarnation
                && reservation.target == to
                && reservation.target_path_instance_id == to_path_instance_id
                && reservation.target_incarnation == to_incarnation
                && reservation.capacity_proof == capacity_proof
        });
        if !drain_matches {
            return false;
        }
        let moved = state
            .session_mut(session_id)
            .is_some_and(|session| session.load_mut().move_response_service(from, to, lane));
        if !moved {
            return false;
        }
        let session = state
            .session_mut(session_id)
            .expect("moved response Service session");
        session.take_response_service_handoff_drain();
        session.bump_generation();
        true
    }

    pub(super) fn rollback_response_service_handoff(
        &self,
        session_id: SessionId,
        from: CarrierPathKey,
        to: CarrierPathKey,
        lane: FlowLane,
    ) {
        self.move_response_service(session_id, to, from, lane);
    }
}

#[cfg(test)]
#[path = "handoff_test.rs"]
mod tests;

#[cfg(test)]
#[path = "handoff_commit_test.rs"]
mod commit_tests;
