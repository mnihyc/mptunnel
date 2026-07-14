use super::handoff::ResponseServiceHandoffDrainReservation;
use super::load::{ResponseSessionLoadState, ServerPathLaneLoad};
use super::quic_capacity::{
    ResponseQuicCapacityHistory, ServerQuicCapacityCalibrationPhase,
    ServerQuicCapacityCalibrationReservation, finish_quic_capacity_session_reclamation,
};
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey};
use crate::protocol::{SessionId, UnderlayProtocol};
use std::collections::HashMap;
use std::sync::Mutex;
#[cfg(test)]
use std::sync::MutexGuard;
use std::time::Instant;

// Session coordination owns one state mutex, generations, and probe leases.
// `load` owns counter semantics through that same mutex. Neither layer
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
    sessions: HashMap<SessionId, ResponseSessionState>,
}

/// One typed slot makes mutually exclusive session transactions structural;
/// concrete variants retain owner-specific cancellation and reclaim behavior.
#[derive(Debug)]
pub(super) enum ResponseSessionOperation {
    TcpCapacityProbe,
    QuicCapacityCalibration(ServerQuicCapacityCalibrationReservation),
    ResponseServiceHandoffDrain(ResponseServiceHandoffDrainReservation),
}

/// Co-locates one session's coordination state for one hash lookup under the
/// tracker mutex while load and transport owners retain typed payloads.
#[derive(Debug, Default)]
pub(super) struct ResponseSessionState {
    references: u32,
    generation: u64,
    load: ResponseSessionLoadState,
    active_operation: Option<ResponseSessionOperation>,
    quic_history: ResponseQuicCapacityHistory,
}

impl ResponseSessionState {
    #[cfg(test)]
    pub(super) fn references(&self) -> u32 {
        self.references
    }

    pub(super) fn attach_reference(&mut self) {
        self.references = self.references.saturating_add(1);
    }

    pub(super) fn detach_reference(&mut self) {
        self.references = self.references.saturating_sub(1);
    }

    pub(super) fn generation(&self) -> u64 {
        self.generation
    }

    pub(super) fn bump_generation(&mut self) {
        self.generation = self.generation.wrapping_add(1);
    }

    pub(super) fn load(&self) -> &ResponseSessionLoadState {
        &self.load
    }

    pub(super) fn load_mut(&mut self) -> &mut ResponseSessionLoadState {
        &mut self.load
    }

    pub(super) fn tcp_capacity_probe_reserved(&self) -> bool {
        matches!(
            self.active_operation,
            Some(ResponseSessionOperation::TcpCapacityProbe)
        )
    }

    pub(super) fn reserve_tcp_capacity_probe(&mut self) -> bool {
        if self.active_operation.is_some() {
            return false;
        }
        self.active_operation = Some(ResponseSessionOperation::TcpCapacityProbe);
        true
    }

    pub(super) fn clear_tcp_capacity_probe(&mut self) -> bool {
        if !self.tcp_capacity_probe_reserved() {
            return false;
        }
        self.active_operation = None;
        true
    }

    pub(super) fn quic_capacity_calibration(
        &self,
    ) -> Option<&ServerQuicCapacityCalibrationReservation> {
        match self.active_operation.as_ref() {
            Some(ResponseSessionOperation::QuicCapacityCalibration(reservation)) => {
                Some(reservation)
            }
            _ => None,
        }
    }

    pub(super) fn quic_capacity_calibration_mut(
        &mut self,
    ) -> Option<&mut ServerQuicCapacityCalibrationReservation> {
        match self.active_operation.as_mut() {
            Some(ResponseSessionOperation::QuicCapacityCalibration(reservation)) => {
                Some(reservation)
            }
            _ => None,
        }
    }

    pub(super) fn reserve_quic_capacity_calibration(
        &mut self,
        reservation: ServerQuicCapacityCalibrationReservation,
    ) -> bool {
        if self.active_operation.is_some() {
            return false;
        }
        self.active_operation = Some(ResponseSessionOperation::QuicCapacityCalibration(
            reservation,
        ));
        true
    }

    pub(super) fn take_quic_capacity_calibration(
        &mut self,
    ) -> Option<ServerQuicCapacityCalibrationReservation> {
        if !matches!(
            self.active_operation,
            Some(ResponseSessionOperation::QuicCapacityCalibration(_))
        ) {
            return None;
        }
        match self.active_operation.take() {
            Some(ResponseSessionOperation::QuicCapacityCalibration(reservation)) => {
                Some(reservation)
            }
            _ => unreachable!("checked QUIC capacity operation"),
        }
    }

    pub(super) fn response_service_handoff_drain(
        &self,
    ) -> Option<&ResponseServiceHandoffDrainReservation> {
        match self.active_operation.as_ref() {
            Some(ResponseSessionOperation::ResponseServiceHandoffDrain(reservation)) => {
                Some(reservation)
            }
            _ => None,
        }
    }

    #[cfg(test)]
    pub(super) fn response_service_handoff_drain_mut(
        &mut self,
    ) -> Option<&mut ResponseServiceHandoffDrainReservation> {
        match self.active_operation.as_mut() {
            Some(ResponseSessionOperation::ResponseServiceHandoffDrain(reservation)) => {
                Some(reservation)
            }
            _ => None,
        }
    }

    pub(super) fn reserve_response_service_handoff_drain(
        &mut self,
        reservation: ResponseServiceHandoffDrainReservation,
    ) -> bool {
        if self.active_operation.is_some() {
            return false;
        }
        self.active_operation = Some(ResponseSessionOperation::ResponseServiceHandoffDrain(
            reservation,
        ));
        true
    }

    pub(super) fn take_response_service_handoff_drain(
        &mut self,
    ) -> Option<ResponseServiceHandoffDrainReservation> {
        if !matches!(
            self.active_operation,
            Some(ResponseSessionOperation::ResponseServiceHandoffDrain(_))
        ) {
            return None;
        }
        match self.active_operation.take() {
            Some(ResponseSessionOperation::ResponseServiceHandoffDrain(reservation)) => {
                Some(reservation)
            }
            _ => unreachable!("checked response Service handoff operation"),
        }
    }

    pub(super) fn quic_history(&self) -> &ResponseQuicCapacityHistory {
        &self.quic_history
    }

    pub(super) fn quic_history_mut(&mut self) -> &mut ResponseQuicCapacityHistory {
        &mut self.quic_history
    }

    fn blocks_session_reclamation(&self) -> bool {
        self.references > 0
            || self.load.blocks_session_reclamation()
            || match self.active_operation.as_ref() {
                Some(ResponseSessionOperation::TcpCapacityProbe) => true,
                Some(ResponseSessionOperation::QuicCapacityCalibration(reservation)) => matches!(
                    reservation.phase,
                    ServerQuicCapacityCalibrationPhase::ProofPublishing { .. }
                ),
                Some(ResponseSessionOperation::ResponseServiceHandoffDrain(_)) | None => false,
            }
    }

    fn into_reclaimed_quic_capacity_reservation(
        self,
    ) -> Option<ServerQuicCapacityCalibrationReservation> {
        match self.active_operation {
            Some(ResponseSessionOperation::QuicCapacityCalibration(reservation)) => {
                Some(reservation)
            }
            _ => None,
        }
    }
}

impl ServerPathLaneTrackerState {
    pub(super) fn session(&self, session_id: SessionId) -> Option<&ResponseSessionState> {
        self.sessions.get(&session_id)
    }

    pub(super) fn session_mut(
        &mut self,
        session_id: SessionId,
    ) -> Option<&mut ResponseSessionState> {
        self.sessions.get_mut(&session_id)
    }

    pub(super) fn session_mut_or_default(
        &mut self,
        session_id: SessionId,
    ) -> &mut ResponseSessionState {
        self.sessions.entry(session_id).or_default()
    }

    pub(super) fn generation(&self, session_id: SessionId) -> u64 {
        self.session(session_id)
            .map(ResponseSessionState::generation)
            .unwrap_or(0)
    }

    pub(super) fn bump_generation(&mut self, session_id: SessionId) {
        self.session_mut_or_default(session_id).bump_generation();
    }

    fn response_path_scheduling_snapshot(
        &self,
        session_id: SessionId,
        path: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
        session_load: ServerPathLaneLoad,
    ) -> ServerResponsePathSchedulingSnapshot {
        let path_load = self
            .session(session_id)
            .map(|session| session.load().response_service_path_load(path))
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
        let should_reclaim = self
            .session(session_id)
            .is_some_and(|session| !session.blocks_session_reclamation());
        if !should_reclaim {
            return;
        }
        let session = self
            .sessions
            .remove(&session_id)
            .expect("reclaimable response session");
        finish_quic_capacity_session_reclamation(
            session_id,
            session.into_reclaimed_quic_capacity_reservation(),
        );
    }
}

impl ServerPathLaneTracker {
    pub(in crate::runtime::stream) fn attach_session(&self, session_id: SessionId) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        state.session_mut_or_default(session_id).attach_reference();
    }

    pub(in crate::runtime::stream) fn detach_session(&self, session_id: SessionId) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        if let Some(session) = state.session_mut(session_id) {
            session.detach_reference();
        }
        state.maybe_reclaim_session(session_id);
    }

    #[cfg(test)]
    pub(super) fn generation(&self, session_id: SessionId) -> u64 {
        self.state
            .lock()
            .expect("server path lane tracker lock")
            .generation(session_id)
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
        state
            .session(session_id)
            .map(|session| ServerSessionRetentionSnapshot {
                references: session.references(),
                generation: session.generation(),
                attachment_path_count: session.load().attachment_path_count(),
                service_path_count: session.load().service_path_count(),
                realtime_flows: session.load().realtime_flows(),
                active_response_flows: session.load().active_response_flows(),
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
        let session = state.session(session_id);
        let generation = session.map(ResponseSessionState::generation).unwrap_or(0);
        let active_response_flows = session
            .map(|session| session.load().active_response_flows())
            .unwrap_or(0);
        let service_family_loads = session
            .map(|session| session.load().service_family_loads())
            .unwrap_or_default();
        ResponseSessionSchedulingSnapshot {
            generation,
            active_response_flows,
            service_family_loads,
            tcp_capacity_probe_reserved: session
                .is_some_and(ResponseSessionState::tcp_capacity_probe_reserved),
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
        let generation = state.generation(session_id);
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
        let generation = state.generation(session_id);
        let active_response_flows = state
            .session(session_id)
            .map(|session| session.load().active_response_flows())
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
        path_instance_id: CarrierPathInstanceId,
    ) -> ServerResponsePathSchedulingSnapshot {
        let state = self.state.lock().expect("server path lane tracker lock");
        let session_load = state
            .session(session_id)
            .map(|session| session.load().response_service_session_load())
            .unwrap_or_default();
        state.response_path_scheduling_snapshot(session_id, path, path_instance_id, session_load)
    }

    /// Reads one target set under one lock so load and attempt budgets share an epoch.
    pub(super) fn response_path_scheduling_snapshots(
        &self,
        session_id: SessionId,
        paths: impl IntoIterator<Item = (CarrierPathKey, CarrierPathInstanceId)>,
    ) -> Vec<ServerResponsePathSchedulingSnapshot> {
        let state = self.state.lock().expect("server path lane tracker lock");
        let session_load = state
            .session(session_id)
            .map(|session| session.load().response_service_session_load())
            .unwrap_or_default();
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
#[path = "session_test.rs"]
mod tests;
