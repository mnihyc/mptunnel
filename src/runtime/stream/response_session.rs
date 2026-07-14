use super::ServerCarrierPathInstanceId;
use super::response_handoff::ResponseServiceHandoffDrainReservation;
use super::response_load::{ServerPathLaneLoad, ServerPathLoadKey};
use super::response_quic_capacity::{
    ServerQuicCapacityCalibrationPathKey, ServerQuicCapacityCalibrationReservation,
    finish_quic_capacity_session_reclamation,
};
use crate::model::path::CarrierPathKey;
use crate::protocol::{SessionId, UnderlayProtocol};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
#[cfg(test)]
use std::sync::MutexGuard;
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

#[cfg(test)]
pub(super) struct ServerPathLaneTrackerStateLockForTest<'a> {
    _guard: MutexGuard<'a, ServerPathLaneTrackerState>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ServerSessionRetentionSnapshot {
    pub(super) references: u32,
    pub(super) generation: u64,
    pub(super) attachment_path_count: usize,
    pub(super) service_path_count: usize,
    pub(super) realtime_flows: u32,
    pub(super) active_response_flows: u32,
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
            self.clear_response_service_handoff_drain_for_reclaim(session_id);
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

    #[cfg(test)]
    pub(super) fn hold_state_lock_for_test(&self) -> ServerPathLaneTrackerStateLockForTest<'_> {
        ServerPathLaneTrackerStateLockForTest {
            _guard: self.state.lock().expect("server path lane tracker lock"),
        }
    }

    #[cfg(test)]
    pub(super) fn retention_snapshot_for_test(
        &self,
        session_id: SessionId,
    ) -> Option<ServerSessionRetentionSnapshot> {
        let state = self.state.lock().expect("server path lane tracker lock");
        let references = state.session_references.get(&session_id).copied();
        let generation = state.session_generations.get(&session_id).copied();
        let attachment_path_count = state
            .loads
            .keys()
            .filter(|key| key.session_id == session_id)
            .count();
        let service_path_count = state
            .response_service_loads
            .keys()
            .filter(|key| key.session_id == session_id)
            .count();
        let realtime_flows = state.realtime_flows.get(&session_id).copied();
        let active_response_flows = state.active_response_flows.get(&session_id).copied();
        let retained = references.is_some()
            || generation.is_some()
            || attachment_path_count > 0
            || service_path_count > 0
            || realtime_flows.is_some()
            || active_response_flows.is_some();
        retained.then_some(ServerSessionRetentionSnapshot {
            references: references.unwrap_or(0),
            generation: generation.unwrap_or(0),
            attachment_path_count,
            service_path_count,
            realtime_flows: realtime_flows.unwrap_or(0),
            active_response_flows: active_response_flows.unwrap_or(0),
        })
    }

    pub(super) fn response_scheduling_snapshot(
        &self,
        session_id: SessionId,
    ) -> ResponseSessionSchedulingSnapshot {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let now = Instant::now();
        state.expire_quic_capacity_calibration_at(session_id, now);
        state.expire_response_service_handoff_drain_at(session_id, now);
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
            response_service_handoff_drain: state.response_service_handoff_drain(session_id),
        }
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
}

#[cfg(test)]
#[path = "response_session_test.rs"]
mod tests;
