//! Session-wide response Service handoff drain and ownership mutation.
//!
//! Drain serialization, expiry, and atomic Service-load movement use the one
//! central tracker mutex. Binding/output/queue commit lives in
//! `response_handoff_commit`.

use super::response_admission::server_output_has_bulk_rate_evidence_with_limits;
use super::response_evidence::server_output_fresh_quic_capacity_proof;
use super::response_quic_capacity::valid_quic_capacity_proof_candidate_at;
use super::response_session::{ServerPathLaneTracker, ServerPathLaneTrackerState};
use super::response_snapshot::server_bulk_output_snapshot_with_command_pending;
use super::response_topology::ResponseSenderPathTarget;
use super::{ResponseStreamBinding, ServerCarrierPathInstanceId};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::multipath::FlowSubflowSet;
use crate::model::path::CarrierPathKey;
use crate::protocol::{SessionId, StreamOpenRole, UnderlayProtocol};
use crate::runtime::path::quic::metrics::QuicCapacityProofCandidate;
use crate::runtime::stream::response_placement::{
    ResponseServiceHandoffMode, response_rate_fair_share_bps, response_service_handoff_mode,
};
use crate::scheduler::FlowLane;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ResponseServiceHandoffDrainReservation {
    pub(in crate::runtime) binding_instance_id: u64,
    pub(in crate::runtime) service: CarrierPathKey,
    pub(in crate::runtime) service_path_instance_id: ServerCarrierPathInstanceId,
    pub(in crate::runtime) service_incarnation: u64,
    pub(in crate::runtime) target: CarrierPathKey,
    pub(in crate::runtime) target_path_instance_id: ServerCarrierPathInstanceId,
    pub(in crate::runtime) target_incarnation: u64,
    /// Pins one fresh receipt only for this bounded handoff transaction.
    pub(in crate::runtime) capacity_proof: Option<QuicCapacityProofCandidate>,
    pub(in crate::runtime) expires_at: Instant,
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
}

impl ServerPathLaneTrackerState {
    pub(super) fn response_service_handoff_drain_reserved(&self, session_id: SessionId) -> bool {
        self.response_service_handoff_drains
            .contains_key(&session_id)
    }

    pub(super) fn response_service_handoff_drain(
        &self,
        session_id: SessionId,
    ) -> Option<ResponseServiceHandoffDrainReservation> {
        self.response_service_handoff_drains
            .get(&session_id)
            .copied()
    }

    pub(super) fn clear_response_service_handoff_drain_for_reclaim(
        &mut self,
        session_id: SessionId,
    ) {
        self.response_service_handoff_drains.remove(&session_id);
    }

    pub(super) fn expire_response_service_handoff_drain_at(
        &mut self,
        session_id: SessionId,
        now: Instant,
    ) {
        let drain_expired = self
            .response_service_handoff_drains
            .get(&session_id)
            .is_some_and(|reservation| now >= reservation.expires_at);
        if drain_expired {
            let reservation = self.response_service_handoff_drains.remove(&session_id);
            self.bump_generation(session_id);
            #[cfg(feature = "lab-diagnostics")]
            if let Some(reservation) = reservation {
                lab_diagnostic(
                    "response_service_handoff",
                    format_args!(
                        "phase=drain_cancelled session_id={} binding_instance_id={} reason=timeout from_underlay={:?} from_path_id={} to_underlay={:?} to_path_id={}",
                        session_id.0,
                        reservation.binding_instance_id,
                        reservation.service.underlay,
                        reservation.service.path_id.0,
                        reservation.target.underlay,
                        reservation.target.path_id.0,
                    ),
                );
            }
            #[cfg(not(feature = "lab-diagnostics"))]
            let _ = reservation;
        }
    }
}

impl ServerPathLaneTracker {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn try_reserve_response_service_handoff_drain(
        &self,
        session_id: SessionId,
        expected_generation: u64,
        binding_instance_id: u64,
        service: CarrierPathKey,
        service_path_instance_id: ServerCarrierPathInstanceId,
        service_incarnation: u64,
        target: CarrierPathKey,
        target_path_instance_id: ServerCarrierPathInstanceId,
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
        let generation = state
            .session_generations
            .get(&session_id)
            .copied()
            .unwrap_or(0);
        // The tracker serializes a decision made from the matching generation;
        // family/rate policy belongs to the sender snapshot that ranked it.
        if generation != expected_generation
            || state.tcp_capacity_probe_reservations.contains(&session_id)
            || state.quic_capacity_calibration_reserved(session_id)
            || state
                .response_service_handoff_drains
                .contains_key(&session_id)
        {
            return false;
        }
        state.response_service_handoff_drains.insert(
            session_id,
            ResponseServiceHandoffDrainReservation {
                binding_instance_id,
                service,
                service_path_instance_id,
                service_incarnation,
                target,
                target_path_instance_id,
                target_incarnation,
                capacity_proof,
                expires_at,
            },
        );
        state.bump_generation(session_id);
        true
    }

    pub(super) fn clear_response_service_handoff_drain_for_binding(
        &self,
        session_id: SessionId,
        binding_instance_id: u64,
    ) -> bool {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let matches = state
            .response_service_handoff_drains
            .get(&session_id)
            .is_some_and(|reservation| reservation.binding_instance_id == binding_instance_id);
        if matches {
            state.response_service_handoff_drains.remove(&session_id);
            state.bump_generation(session_id);
        }
        matches
    }

    pub(super) fn clear_response_service_handoff_drain_for_path(
        &self,
        session_id: SessionId,
        binding_instance_id: u64,
        path: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
    ) -> bool {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let matches = state
            .response_service_handoff_drains
            .get(&session_id)
            .is_some_and(|reservation| {
                reservation.binding_instance_id == binding_instance_id
                    && ((reservation.service == path
                        && reservation.service_path_instance_id == path_instance_id)
                        || (reservation.target == path
                            && reservation.target_path_instance_id == path_instance_id))
            });
        if matches {
            state.response_service_handoff_drains.remove(&session_id);
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
        from_path_instance_id: ServerCarrierPathInstanceId,
        from_incarnation: u64,
        to: CarrierPathKey,
        to_path_instance_id: ServerCarrierPathInstanceId,
        to_incarnation: u64,
        capacity_proof: Option<QuicCapacityProofCandidate>,
        lane: FlowLane,
    ) -> bool {
        if from.underlay == to.underlay || from == to {
            return false;
        }
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let generation = state
            .session_generations
            .get(&session_id)
            .copied()
            .unwrap_or(0);
        if generation != expected_generation || state.quic_capacity_calibration_reserved(session_id)
        {
            return false;
        }
        // Generation equality proves the caller ranked the same family-load
        // state. This mutation layer owns serialization, not placement policy.
        let now = Instant::now();
        let drain_expired = state
            .response_service_handoff_drains
            .get(&session_id)
            .is_some_and(|reservation| now >= reservation.expires_at);
        if drain_expired {
            state.response_service_handoff_drains.remove(&session_id);
            state.bump_generation(session_id);
            return false;
        }
        let drain = state.response_service_handoff_drains.get(&session_id);
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
        if !state.move_response_service(session_id, from, to, lane) {
            return false;
        }
        state.response_service_handoff_drains.remove(&session_id);
        state.bump_generation(session_id);
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
