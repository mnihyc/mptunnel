use super::ServerCarrierPathInstanceId;
use super::response_load::{ServerPathLaneLoad, ServerPathLoadKey};
use super::response_quic_capacity::{
    ServerQuicCapacityCalibrationPathKey, ServerQuicCapacityCalibrationReservation,
    finish_quic_capacity_session_reclamation, valid_quic_capacity_proof_candidate_at,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::path::CarrierPathKey;
use crate::protocol::{SessionId, UnderlayProtocol};
use crate::runtime::path::quic::metrics::QuicCapacityProofCandidate;
use crate::scheduler::FlowLane;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::Instant;

// Session coordination owns one state mutex, generations, and probe leases.
// `response_load` owns counter semantics through that same mutex. Neither layer
// ranks paths, estimates durable transport evidence, or owns product bytes.

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::runtime) struct ResponseServiceFamilyLoads {
    pub(super) tcp: u32,
    pub(super) udp: u32,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ResponseSessionSchedulingSnapshot {
    pub(in crate::runtime) generation: u64,
    pub(in crate::runtime) active_response_flows: u32,
    pub(in crate::runtime) service_family_loads: ResponseServiceFamilyLoads,
    pub(in crate::runtime) tcp_capacity_probe_reserved: bool,
    pub(in crate::runtime) quic_capacity_calibration_reserved: bool,
    pub(in crate::runtime) quic_capacity_calibration_spent_bytes: u64,
    pub(in crate::runtime) response_service_handoff_drain:
        Option<ResponseServiceHandoffDrainReservation>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ServerResponsePathSchedulingSnapshot {
    pub(super) path_load: ServerPathLaneLoad,
    pub(super) session_load: ServerPathLaneLoad,
    pub(super) quic_capacity_calibration_attempts: u8,
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

impl ResponseServiceFamilyLoads {
    #[cfg(test)]
    pub(in crate::runtime) fn new(tcp: u32, udp: u32) -> Self {
        Self { tcp, udp }
    }

    pub(in crate::runtime) fn for_underlay(self, underlay: UnderlayProtocol) -> u32 {
        match underlay {
            UnderlayProtocol::Tcp => self.tcp,
            UnderlayProtocol::Udp => self.udp,
        }
    }

    pub(in crate::runtime) fn needs_diversification(self) -> bool {
        self.tcp.abs_diff(self.udp) >= 2
    }
}

#[derive(Debug, Default)]
/// Per-session lane load snapshot for response attachments and Service owners.
///
/// The tracker informs scheduling and diagnostics only. It is not a path queue
/// and cannot reorder product frames after sender-service admission.
pub(in crate::runtime) struct ServerPathLaneTracker {
    pub(super) state: Mutex<ServerPathLaneTrackerState>,
}

#[derive(Debug, Default)]
pub(super) struct ServerPathLaneTrackerState {
    // Attachment load describes control-plane Active roles. Response Service
    // load is separate because a clear-frontier handoff changes product
    // ownership without rewriting the request-side attachment role.
    pub(super) loads: HashMap<ServerPathLoadKey, ServerPathLaneLoad>,
    pub(super) response_service_loads: HashMap<ServerPathLoadKey, ServerPathLaneLoad>,
    pub(super) response_service_session_loads: HashMap<SessionId, ServerPathLaneLoad>,
    pub(super) response_service_family_loads: HashMap<SessionId, ResponseServiceFamilyLoads>,
    // One active train serializes admission. Attempts belong to exact carrier
    // instances, while spent bytes are a non-refilling session envelope.
    pub(super) quic_capacity_calibrations:
        HashMap<SessionId, ServerQuicCapacityCalibrationReservation>,
    pub(super) quic_capacity_calibration_attempts:
        HashMap<ServerQuicCapacityCalibrationPathKey, u8>,
    pub(super) quic_capacity_calibration_bytes: HashMap<SessionId, u64>,
    pub(super) tcp_capacity_probe_reservations: HashSet<SessionId>,
    // A drain is only a session-wide intent: it pauses fresh data on the exact
    // binding but does not move Service ownership until clear-frontier commit.
    pub(super) response_service_handoff_drains:
        HashMap<SessionId, ResponseServiceHandoffDrainReservation>,
    pub(super) realtime_flows: HashMap<SessionId, u32>,
    pub(super) active_response_flows: HashMap<SessionId, u32>,
    pub(super) session_references: HashMap<SessionId, u32>,
    pub(super) session_generations: HashMap<SessionId, u64>,
}

impl ServerPathLaneTrackerState {
    pub(super) fn bump_generation(&mut self, session_id: SessionId) {
        let generation = self.session_generations.entry(session_id).or_default();
        *generation = generation.wrapping_add(1);
    }

    fn response_path_scheduling_snapshot(
        &self,
        session_id: SessionId,
        path: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
        session_load: ServerPathLaneLoad,
    ) -> ServerResponsePathSchedulingSnapshot {
        let path_load = self
            .response_service_loads
            .get(&ServerPathLoadKey { session_id, path })
            .copied()
            .unwrap_or_default();
        let quic_capacity_calibration_attempts =
            self.quic_capacity_calibration_attempts_for_path(session_id, path, path_instance_id);
        ServerResponsePathSchedulingSnapshot {
            path_load,
            session_load,
            quic_capacity_calibration_attempts,
        }
    }

    pub(super) fn maybe_reclaim_session(&mut self, session_id: SessionId) {
        let has_references = self
            .session_references
            .get(&session_id)
            .is_some_and(|count| *count > 0);
        let has_realtime = self
            .realtime_flows
            .get(&session_id)
            .is_some_and(|count| *count > 0);
        let has_active_response_flows = self
            .active_response_flows
            .get(&session_id)
            .is_some_and(|count| *count > 0);
        let has_loads = self.loads.keys().any(|key| key.session_id == session_id)
            || self
                .response_service_loads
                .keys()
                .any(|key| key.session_id == session_id);
        let proof_publication_in_progress =
            self.quic_capacity_proof_publication_in_progress(session_id);
        let tcp_capacity_probe_in_progress =
            self.tcp_capacity_probe_reservations.contains(&session_id);
        if !has_references
            && !has_realtime
            && !has_active_response_flows
            && !has_loads
            && !proof_publication_in_progress
            && !tcp_capacity_probe_in_progress
        {
            let capacity_reservation = self.take_quic_capacity_session_reclamation(session_id);
            self.tcp_capacity_probe_reservations.remove(&session_id);
            self.response_service_handoff_drains.remove(&session_id);
            self.response_service_session_loads.remove(&session_id);
            self.response_service_family_loads.remove(&session_id);
            self.session_generations.remove(&session_id);
            finish_quic_capacity_session_reclamation(session_id, capacity_reservation);
        }
    }
}

impl ServerPathLaneTracker {
    pub(in crate::runtime::stream) fn attach_session(&self, session_id: SessionId) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let references = state.session_references.entry(session_id).or_default();
        *references = references.saturating_add(1);
    }

    pub(in crate::runtime::stream) fn detach_session(&self, session_id: SessionId) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        if let Some(references) = state.session_references.get_mut(&session_id) {
            *references = references.saturating_sub(1);
            if *references == 0 {
                state.session_references.remove(&session_id);
            }
        }
        state.maybe_reclaim_session(session_id);
    }

    #[cfg(test)]
    pub(super) fn generation(&self, session_id: SessionId) -> u64 {
        self.state
            .lock()
            .expect("server path lane tracker lock")
            .session_generations
            .get(&session_id)
            .copied()
            .unwrap_or(0)
    }

    pub(super) fn response_scheduling_snapshot(
        &self,
        session_id: SessionId,
    ) -> ResponseSessionSchedulingSnapshot {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let now = Instant::now();
        state.expire_quic_capacity_calibration_at(session_id, now);
        let drain_expired = state
            .response_service_handoff_drains
            .get(&session_id)
            .is_some_and(|reservation| now >= reservation.expires_at);
        if drain_expired {
            let reservation = state.response_service_handoff_drains.remove(&session_id);
            state.bump_generation(session_id);
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
        let generation = state
            .session_generations
            .get(&session_id)
            .copied()
            .unwrap_or(0);
        let active_response_flows = state
            .active_response_flows
            .get(&session_id)
            .copied()
            .unwrap_or(0);
        let service_family_loads = state
            .response_service_family_loads
            .get(&session_id)
            .copied()
            .unwrap_or_default();
        ResponseSessionSchedulingSnapshot {
            generation,
            active_response_flows,
            service_family_loads,
            tcp_capacity_probe_reserved: state
                .tcp_capacity_probe_reservations
                .contains(&session_id),
            quic_capacity_calibration_reserved: state
                .quic_capacity_calibration_reserved(session_id),
            quic_capacity_calibration_spent_bytes: state
                .quic_capacity_calibration_spent_bytes(session_id),
            response_service_handoff_drain: state
                .response_service_handoff_drains
                .get(&session_id)
                .copied(),
        }
    }

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

    pub(super) fn with_matching_generation<R>(
        &self,
        session_id: SessionId,
        expected_generation: u64,
        apply: impl FnOnce() -> R,
    ) -> Option<R> {
        let state = self.state.lock().expect("server path lane tracker lock");
        let generation = state
            .session_generations
            .get(&session_id)
            .copied()
            .unwrap_or(0);
        if generation != expected_generation {
            return None;
        }
        let result = apply();
        drop(state);
        Some(result)
    }

    pub(super) fn with_matching_generation_and_min_active_response_flows<R>(
        &self,
        session_id: SessionId,
        expected_generation: u64,
        minimum_active_response_flows: u32,
        apply: impl FnOnce() -> R,
    ) -> Option<R> {
        let state = self.state.lock().expect("server path lane tracker lock");
        let generation = state
            .session_generations
            .get(&session_id)
            .copied()
            .unwrap_or(0);
        let active_response_flows = state
            .active_response_flows
            .get(&session_id)
            .copied()
            .unwrap_or(0);
        if generation != expected_generation
            || active_response_flows < minimum_active_response_flows
        {
            return None;
        }
        let result = apply();
        drop(state);
        Some(result)
    }

    pub(super) fn response_path_scheduling_snapshot(
        &self,
        session_id: SessionId,
        path: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
    ) -> ServerResponsePathSchedulingSnapshot {
        let state = self.state.lock().expect("server path lane tracker lock");
        let session_load = state.response_service_session_load(session_id);
        state.response_path_scheduling_snapshot(session_id, path, path_instance_id, session_load)
    }

    /// Reads one target set under one lock so load and attempt budgets share an epoch.
    pub(super) fn response_path_scheduling_snapshots(
        &self,
        session_id: SessionId,
        paths: impl IntoIterator<Item = (CarrierPathKey, ServerCarrierPathInstanceId)>,
    ) -> Vec<ServerResponsePathSchedulingSnapshot> {
        let state = self.state.lock().expect("server path lane tracker lock");
        let session_load = state.response_service_session_load(session_id);
        paths
            .into_iter()
            .map(|(path, path_instance_id)| {
                state.response_path_scheduling_snapshot(
                    session_id,
                    path,
                    path_instance_id,
                    session_load,
                )
            })
            .collect()
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

#[cfg(test)]
#[path = "response_session_test.rs"]
mod tests;
