//! Per-response-session state and exclusive operation coordination.
//!
//! This owner defines every schema stored under its one mutex and retires its
//! operation slots. Load, TCP/QUIC, and handoff services own their concrete
//! admission and commit algorithms against this state.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::QuicCapacityProofCandidate;
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey};
use crate::model::response::ResponseServiceFamilyLoads;
use crate::protocol::SessionId;
use crate::runtime::path::commands::CapacityProbeCommandTicket;
use crate::scheduler::FlowLane;
use std::collections::HashMap;
#[cfg(test)]
use std::sync::MutexGuard;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// Session coordination owns one state mutex, generations, counter invariants,
// and operation-slot lifecycle. It never ranks paths, estimates durable
// transport evidence, or owns product bytes.

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ServerPathLaneLoad {
    pub(super) active_flows: u32,
    pub(super) active_latency_sensitive_flows: u32,
}

impl ServerPathLaneLoad {
    fn add(&mut self, lane: FlowLane) {
        self.active_flows = self.active_flows.saturating_add(1);
        if lane.is_latency_sensitive() {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
    }

    fn remove(&mut self, lane: FlowLane) {
        self.active_flows = self.active_flows.saturating_sub(1);
        if lane.is_latency_sensitive() {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_sub(1);
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct ResponseSessionLoadState {
    attachment_paths: HashMap<CarrierPathKey, ServerPathLaneLoad>,
    service_paths: HashMap<CarrierPathKey, ServerPathLaneLoad>,
    service_total: ServerPathLaneLoad,
    service_family: ResponseServiceFamilyLoads,
    realtime_flows: u32,
    active_response_flows: u32,
}

impl ResponseSessionLoadState {
    pub(super) fn blocks_session_reclamation(&self) -> bool {
        self.realtime_flows > 0
            || self.active_response_flows > 0
            || !self.attachment_paths.is_empty()
            || !self.service_paths.is_empty()
    }

    #[cfg(test)]
    pub(super) fn attachment_path_count(&self) -> usize {
        self.attachment_paths.len()
    }

    #[cfg(test)]
    pub(super) fn service_path_count(&self) -> usize {
        self.service_paths.len()
    }

    #[cfg(test)]
    pub(super) fn realtime_flows(&self) -> u32 {
        self.realtime_flows
    }

    pub(super) fn active_response_flows(&self) -> u32 {
        self.active_response_flows
    }

    pub(super) fn service_family_loads(&self) -> ResponseServiceFamilyLoads {
        self.service_family
    }

    #[cfg(test)]
    pub(super) fn attachment_path_load(&self, path: CarrierPathKey) -> ServerPathLaneLoad {
        self.attachment_paths
            .get(&path)
            .copied()
            .unwrap_or_default()
    }

    pub(super) fn response_service_path_load(&self, path: CarrierPathKey) -> ServerPathLaneLoad {
        self.service_paths.get(&path).copied().unwrap_or_default()
    }

    pub(super) fn attach(&mut self, path: CarrierPathKey, lane: FlowLane) {
        self.attachment_paths.entry(path).or_default().add(lane);
    }

    pub(super) fn detach(&mut self, path: CarrierPathKey, lane: FlowLane) -> bool {
        let Some(load) = self.attachment_paths.get_mut(&path) else {
            return false;
        };
        load.remove(lane);
        if load.active_flows == 0 {
            self.attachment_paths.remove(&path);
        }
        true
    }

    pub(super) fn change_attachment_lanes(
        &mut self,
        paths: &[CarrierPathKey],
        from: FlowLane,
        to: FlowLane,
    ) -> bool {
        let mut changed = false;
        for path in paths {
            if let Some(load) = self.attachment_paths.get_mut(path) {
                load.remove(from);
                load.add(to);
                changed = true;
            }
        }
        changed
    }

    pub(super) fn attach_realtime_flow(&mut self) {
        self.realtime_flows = self.realtime_flows.saturating_add(1);
    }

    pub(super) fn detach_realtime_flow(&mut self) -> bool {
        if self.realtime_flows == 0 {
            return false;
        }
        self.realtime_flows = self.realtime_flows.saturating_sub(1);
        true
    }

    pub(super) fn set_response_flow_active(&mut self, active: bool) -> bool {
        if active {
            self.active_response_flows = self.active_response_flows.saturating_add(1);
            return true;
        }
        if self.active_response_flows == 0 {
            return false;
        }
        self.active_response_flows = self.active_response_flows.saturating_sub(1);
        true
    }

    pub(super) fn add_response_service(&mut self, path: CarrierPathKey, lane: FlowLane) {
        self.service_paths.entry(path).or_default().add(lane);
        self.service_total.add(lane);
        self.service_family
            .saturating_add_one_for_underlay(path.underlay);
    }

    pub(super) fn remove_response_service(&mut self, path: CarrierPathKey, lane: FlowLane) -> bool {
        let Some(path_load) = self.service_paths.get_mut(&path) else {
            return false;
        };
        if path_load.active_flows == 0 {
            return false;
        }
        path_load.remove(lane);
        if path_load.active_flows == 0 {
            self.service_paths.remove(&path);
        }

        self.service_total.remove(lane);
        self.service_family
            .saturating_remove_one_for_underlay(path.underlay);
        true
    }

    pub(super) fn move_response_service(
        &mut self,
        from: CarrierPathKey,
        to: CarrierPathKey,
        lane: FlowLane,
    ) -> bool {
        if from == to {
            return self.service_paths.contains_key(&from);
        }
        if !self.remove_response_service(from, lane) {
            return false;
        }
        self.add_response_service(to, lane);
        true
    }

    pub(super) fn change_response_service_lane(
        &mut self,
        path: CarrierPathKey,
        from: FlowLane,
        to: FlowLane,
    ) -> bool {
        let Some(load) = self.service_paths.get_mut(&path) else {
            return false;
        };
        load.remove(from);
        load.add(to);
        self.service_total.remove(from);
        self.service_total.add(to);
        true
    }

    pub(super) fn response_service_session_load(&self) -> ServerPathLaneLoad {
        let mut session_load = self.service_total;
        session_load.active_flows = session_load
            .active_flows
            .saturating_add(self.realtime_flows);
        session_load.active_latency_sensitive_flows = session_load
            .active_latency_sensitive_flows
            .saturating_add(self.realtime_flows);
        session_load
    }

    #[cfg(test)]
    pub(super) fn attachment_session_load(&self) -> ServerPathLaneLoad {
        let mut total = self.attachment_paths.values().fold(
            ServerPathLaneLoad::default(),
            |mut total, load| {
                total.active_flows = total.active_flows.saturating_add(load.active_flows);
                total.active_latency_sensitive_flows = total
                    .active_latency_sensitive_flows
                    .saturating_add(load.active_latency_sensitive_flows);
                total
            },
        );
        total.active_flows = total.active_flows.saturating_add(self.realtime_flows);
        total.active_latency_sensitive_flows = total
            .active_latency_sensitive_flows
            .saturating_add(self.realtime_flows);
        total
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ServerQuicCapacityCalibrationPhase {
    Provisional,
    Active {
        expires_at: Instant,
    },
    // Publication is synchronous but crosses registry and binding locks. Keep
    // serialization held until the accepted marker is visible everywhere.
    ProofAccepted {
        candidate: QuicCapacityProofCandidate,
    },
    // Once committed, carrier proof is irrevocable. Keep session serialization
    // until the registry has published it to every current response binding.
    ProofPublishing {
        candidate: QuicCapacityProofCandidate,
    },
}

#[derive(Debug, Clone)]
pub(super) struct ServerQuicCapacityCalibrationReservation {
    pub(super) binding_instance_id: u64,
    pub(super) path: CarrierPathKey,
    pub(super) path_instance_id: CarrierPathInstanceId,
    pub(super) phase: ServerQuicCapacityCalibrationPhase,
    pub(super) train_bytes: u64,
    pub(super) sample_floor_bytes: u64,
    pub(super) accounting_slack_bytes: u64,
    pub(super) warmup_bytes: u64,
    pub(super) required_proof_bytes: u64,
    pub(super) proof_validity: Duration,
    pub(super) token: u64,
    pub(super) command_ticket: CapacityProbeCommandTicket,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ServerQuicCapacityCalibrationPathKey {
    path: CarrierPathKey,
    path_instance_id: CarrierPathInstanceId,
}

#[derive(Debug, Default)]
pub(super) struct ResponseQuicCapacityHistory {
    attempts: HashMap<ServerQuicCapacityCalibrationPathKey, u8>,
    spent_bytes: u64,
}

impl ResponseQuicCapacityHistory {
    pub(super) fn attempts_for_path(
        &self,
        path: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
    ) -> u8 {
        self.attempts
            .get(&ServerQuicCapacityCalibrationPathKey {
                path,
                path_instance_id,
            })
            .copied()
            .unwrap_or(0)
    }

    pub(super) fn set_attempts_for_path(
        &mut self,
        path: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
        attempts: u8,
    ) {
        let key = ServerQuicCapacityCalibrationPathKey {
            path,
            path_instance_id,
        };
        if attempts == 0 {
            self.attempts.remove(&key);
        } else {
            self.attempts.insert(key, attempts);
        }
    }

    pub(super) fn remove_attempts_for_path(
        &mut self,
        path: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
    ) -> bool {
        self.attempts
            .remove(&ServerQuicCapacityCalibrationPathKey {
                path,
                path_instance_id,
            })
            .is_some()
    }

    pub(super) fn spent_bytes(&self) -> u64 {
        self.spent_bytes
    }

    pub(super) fn set_spent_bytes(&mut self, spent_bytes: u64) {
        self.spent_bytes = spent_bytes;
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self) -> Option<ServerQuicCapacityHistorySnapshot> {
        (!self.attempts.is_empty() || self.spent_bytes > 0).then_some(
            ServerQuicCapacityHistorySnapshot {
                attempt_entry_count: self.attempts.len(),
                spent_bytes: (self.spent_bytes > 0).then_some(self.spent_bytes),
            },
        )
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ServerQuicCapacityHistorySnapshot {
    pub(super) attempt_entry_count: usize,
    pub(super) spent_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ResponseServiceHandoffDrainReservation {
    pub(in crate::runtime) binding_instance_id: u64,
    pub(in crate::runtime) service: CarrierPathKey,
    pub(in crate::runtime) service_path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime) service_incarnation: u64,
    pub(in crate::runtime) target: CarrierPathKey,
    pub(in crate::runtime) target_path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime) target_incarnation: u64,
    /// Pins one fresh receipt only for this bounded handoff transaction.
    pub(in crate::runtime) capacity_proof: Option<QuicCapacityProofCandidate>,
    pub(in crate::runtime) expires_at: Instant,
}

#[cfg(feature = "lab-diagnostics")]
pub(super) fn record_quic_capacity_lifecycle(
    phase: &'static str,
    reason: &'static str,
    session_id: SessionId,
    reservation: ServerQuicCapacityCalibrationReservation,
    candidate: Option<QuicCapacityProofCandidate>,
) {
    let candidate = candidate.or(match reservation.phase {
        ServerQuicCapacityCalibrationPhase::ProofAccepted { candidate }
        | ServerQuicCapacityCalibrationPhase::ProofPublishing { candidate } => Some(candidate),
        _ => None,
    });
    let candidate_value = |value: Option<String>| value.unwrap_or_else(|| "unknown".to_string());
    lab_diagnostic(
        "response_quic_capacity_calibration",
        format_args!(
            "phase={} reason={} session_id={} binding_instance_id={} underlay={:?} path_id={} path_instance_id={} calibration_id={} train_bytes={} sample_floor_bytes={} accounting_slack_bytes={} warmup_bytes={} required_proof_bytes={} proof_validity_ms={} written_bytes={} written_data_frame_count={} receipt_confirmed={} received_bytes={} proof_elapsed_us={} rate_bps={}",
            phase,
            reason,
            session_id.0,
            reservation.binding_instance_id,
            reservation.path.underlay,
            reservation.path.path_id.0,
            reservation.path_instance_id.as_u64(),
            reservation.token,
            reservation.train_bytes,
            reservation.sample_floor_bytes,
            reservation.accounting_slack_bytes,
            reservation.warmup_bytes,
            reservation.required_proof_bytes,
            reservation.proof_validity.as_millis(),
            candidate_value(candidate.map(|proof| proof.written_bytes.to_string())),
            candidate_value(candidate.map(|proof| proof.written_data_frame_count.to_string())),
            candidate_value(candidate.map(|proof| proof.receipt_confirmed.to_string())),
            candidate_value(candidate.map(|proof| proof.received_bytes.to_string())),
            candidate_value(candidate.map(|proof| proof.proof_elapsed.as_micros().to_string())),
            candidate_value(candidate.map(|proof| proof.rate_bps.to_string())),
        ),
    );
}

fn finish_quic_capacity_session_reclamation(
    session_id: SessionId,
    reservation: Option<ServerQuicCapacityCalibrationReservation>,
) {
    if let Some(reservation) = reservation.as_ref() {
        reservation.command_ticket.cancel();
    }
    #[cfg(feature = "lab-diagnostics")]
    if let Some(reservation) = reservation {
        record_quic_capacity_lifecycle(
            "cancelled",
            "session_reclaimed",
            session_id,
            reservation,
            None,
        );
    }
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = (session_id, reservation);
}

#[derive(Debug, Default)]
/// Per-session lane load snapshot for response attachments and Service owners.
///
/// The tracker informs scheduling and diagnostics only. It is not a path queue
/// and cannot reorder product frames after sender-service admission.
pub(in crate::runtime) struct ServerPathLaneTracker {
    pub(super) state: Mutex<ServerPathLaneTrackerState>,
}

/// Holds the session-wide TCP discovery slot until its carrier command is
/// received, cancelled, or dropped after failure.
#[derive(Debug)]
pub(in crate::runtime) struct TcpCapacityProbeSessionLease {
    tracker: Arc<ServerPathLaneTracker>,
    session_id: SessionId,
}

impl Drop for TcpCapacityProbeSessionLease {
    fn drop(&mut self) {
        let mut state = self
            .tracker
            .state
            .lock()
            .expect("server path lane tracker lock");
        let released = state.session_mut(self.session_id).is_some_and(|session| {
            if !session.clear_tcp_capacity_probe() {
                return false;
            }
            session.bump_generation();
            true
        });
        if released {
            state.maybe_reclaim_session(self.session_id);
        }
    }
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

    pub(super) fn quic_capacity_calibration_reserved(&self, session_id: SessionId) -> bool {
        self.session(session_id)
            .and_then(|session| session.quic_capacity_calibration())
            .is_some()
    }

    fn quic_capacity_calibration_removal_reason_at(
        &self,
        session_id: SessionId,
        now: Instant,
    ) -> Option<(&'static str, &'static str)> {
        self.session(session_id)
            .and_then(|session| session.quic_capacity_calibration())
            .and_then(|reservation| {
                if !reservation.command_ticket.is_current()
                    && !matches!(
                        reservation.phase,
                        ServerQuicCapacityCalibrationPhase::ProofPublishing { .. }
                    )
                {
                    return Some(("cancelled", "command_invalidated"));
                }
                match reservation.phase {
                    ServerQuicCapacityCalibrationPhase::Active { expires_at }
                        if now >= expires_at =>
                    {
                        Some(("expired", "lease_elapsed"))
                    }
                    _ => None,
                }
            })
    }

    pub(super) fn quic_capacity_calibration_requires_maintenance_at(
        &self,
        session_id: SessionId,
        now: Instant,
    ) -> bool {
        self.quic_capacity_calibration_removal_reason_at(session_id, now)
            .is_some()
    }

    pub(super) fn expire_quic_capacity_calibration_at(
        &mut self,
        session_id: SessionId,
        now: Instant,
    ) {
        let capacity_removal = self.quic_capacity_calibration_removal_reason_at(session_id, now);
        if let Some((phase, reason)) = capacity_removal {
            let reservation = self
                .session_mut(session_id)
                .and_then(|session| session.take_quic_capacity_calibration());
            if let Some(reservation) = reservation.as_ref() {
                reservation.command_ticket.cancel();
            }
            self.bump_generation(session_id);
            #[cfg(feature = "lab-diagnostics")]
            if let Some(reservation) = reservation {
                record_quic_capacity_lifecycle(phase, reason, session_id, reservation, None);
            }
            #[cfg(not(feature = "lab-diagnostics"))]
            let _ = (reservation, phase, reason);
        }
    }

    pub(super) fn response_service_handoff_drain(
        &self,
        session_id: SessionId,
    ) -> Option<ResponseServiceHandoffDrainReservation> {
        self.session(session_id)
            .and_then(|session| session.response_service_handoff_drain())
            .copied()
    }

    pub(super) fn response_service_handoff_drain_requires_maintenance_at(
        &self,
        session_id: SessionId,
        now: Instant,
    ) -> bool {
        self.session(session_id)
            .and_then(|session| session.response_service_handoff_drain())
            .is_some_and(|reservation| now >= reservation.expires_at)
    }

    pub(super) fn expire_response_service_handoff_drain_at(
        &mut self,
        session_id: SessionId,
        now: Instant,
    ) {
        let drain_expired =
            self.response_service_handoff_drain_requires_maintenance_at(session_id, now);
        if drain_expired {
            let reservation = self
                .session_mut(session_id)
                .and_then(|session| session.take_response_service_handoff_drain());
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
    pub(in crate::runtime) fn try_reserve_tcp_capacity_probe(
        self: &Arc<Self>,
        session_id: SessionId,
        expected_generation: u64,
    ) -> Option<TcpCapacityProbeSessionLease> {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        if state.generation(session_id) != expected_generation {
            return None;
        }
        let session = state.session_mut_or_default(session_id);
        if !session.reserve_tcp_capacity_probe() {
            return None;
        }
        session.bump_generation();
        Some(TcpCapacityProbeSessionLease {
            tracker: Arc::clone(self),
            session_id,
        })
    }

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

    /// Expiry is an apply transition. Snapshot readers only report that this
    /// maintenance is due so readiness cannot mutate session generations.
    pub(super) fn maintain_response_session_operations(&self, session_id: SessionId) -> bool {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let now = Instant::now();
        let maintenance_due = state
            .quic_capacity_calibration_requires_maintenance_at(session_id, now)
            || state.response_service_handoff_drain_requires_maintenance_at(session_id, now);
        if !maintenance_due {
            return false;
        }
        state.expire_quic_capacity_calibration_at(session_id, now);
        state.expire_response_service_handoff_drain_at(session_id, now);
        state.maybe_reclaim_session(session_id);
        true
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
}

#[cfg(test)]
#[path = "session_test.rs"]
mod tests;
