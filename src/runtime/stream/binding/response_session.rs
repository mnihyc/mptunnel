#[cfg(test)]
use super::QuicCapacityProbeCommandResolution;
use super::{
    CarrierPathKey, MAX_RESPONSE_QUIC_CAPACITY_CALIBRATION_ATTEMPTS_PER_PATH,
    PATH_OPEN_SCORE_BYTES, QUIC_TIMER_GRANULARITY, QuicCapacityProbeCommandTicket,
    ServerCarrierPathInstanceId,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::protocol::{SessionId, UnderlayProtocol};
use crate::runtime::path::quic::metrics::QuicCapacityProofCandidate;
use crate::scheduler::FlowLane;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// Session-wide response coordination owns load counts, generations, and probe
// leases. It never ranks paths, estimates durable transport evidence, or owns
// product bytes; an accepted proof is held only while its publication commits.

fn response_lane_is_latency_sensitive(lane: FlowLane) -> bool {
    matches!(
        lane,
        FlowLane::Control | FlowLane::Latency | FlowLane::RealtimeDatagram
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct ServerPathLoadKey {
    pub(super) session_id: SessionId,
    pub(super) path: CarrierPathKey,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ServerPathLaneLoad {
    pub(super) active_flows: u32,
    pub(super) active_latency_sensitive_flows: u32,
}

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
    pub(super) path_instance_id: ServerCarrierPathInstanceId,
    pub(super) phase: ServerQuicCapacityCalibrationPhase,
    pub(super) train_bytes: u64,
    pub(super) sample_floor_bytes: u64,
    pub(super) accounting_slack_bytes: u64,
    pub(super) warmup_bytes: u64,
    pub(super) required_proof_bytes: u64,
    pub(super) proof_validity: Duration,
    pub(super) token: u64,
    pub(super) command_ticket: QuicCapacityProbeCommandTicket,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::stream) struct ServerQuicCapacityProofTicket {
    pub(super) session_id: SessionId,
    pub(super) binding_instance_id: u64,
    pub(super) path: CarrierPathKey,
    pub(super) path_instance_id: ServerCarrierPathInstanceId,
    pub(super) candidate: QuicCapacityProofCandidate,
}

pub(super) fn valid_quic_capacity_geometry(
    train_bytes: u64,
    sample_floor_bytes: u64,
    accounting_slack_bytes: u64,
    warmup_bytes: u64,
    required_proof_bytes: u64,
) -> bool {
    let expected_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor_bytes / 8);
    let expected_required = sample_floor_bytes.checked_sub(accounting_slack_bytes);
    let expected_train = warmup_bytes
        .checked_add(required_proof_bytes)
        .map(|bytes| bytes.max(sample_floor_bytes));
    train_bytes > 0
        && sample_floor_bytes > 0
        && required_proof_bytes > 0
        && accounting_slack_bytes == expected_slack
        && expected_required == Some(required_proof_bytes)
        && expected_train == Some(train_bytes)
}

pub(in crate::runtime) fn quic_capacity_receipt_rate_bps(
    train_bytes: u64,
    proof_elapsed: Duration,
) -> Option<u64> {
    if train_bytes == 0 || proof_elapsed.is_zero() {
        return None;
    }
    let rate = train_bytes as f64 * 8.0 / proof_elapsed.max(QUIC_TIMER_GRANULARITY).as_secs_f64();
    rate.is_finite()
        .then_some(rate.round().max(1.0).min(u64::MAX as f64) as u64)
}

pub(in crate::runtime) fn well_formed_quic_capacity_proof_candidate(
    proof: QuicCapacityProofCandidate,
) -> bool {
    valid_quic_capacity_geometry(
        proof.train_bytes,
        proof.sample_floor_bytes,
        proof.accounting_slack_bytes,
        proof.warmup_bytes,
        proof.required_proof_bytes,
    ) && proof.receipt_confirmed
        && proof.written_bytes == proof.train_bytes
        && proof.written_data_frame_count > 0
        && proof.received_bytes == proof.train_bytes
        && !proof.proof_elapsed.is_zero()
        && quic_capacity_receipt_rate_bps(proof.train_bytes, proof.proof_elapsed)
            .is_some_and(|raw_rate| proof.rate_bps > 0 && proof.rate_bps <= raw_rate)
        && !proof.proof_validity.is_zero()
        && proof.accepted_at.checked_add(proof.proof_validity) == Some(proof.expires_at)
}

pub(in crate::runtime) fn valid_quic_capacity_proof_candidate_at(
    proof: QuicCapacityProofCandidate,
    now: Instant,
) -> bool {
    well_formed_quic_capacity_proof_candidate(proof) && proof.expires_at > now
}

pub(in crate::runtime) fn quic_capacity_proof_pin_matches_marker(
    pinned: QuicCapacityProofCandidate,
    marker: Option<QuicCapacityProofCandidate>,
    now: Instant,
) -> bool {
    match marker {
        Some(marker) => marker == pinned,
        // A generic metric refresh may prune an expired marker. Absence before
        // its fixed deadline is invalidation, not ordinary expiry.
        None => now >= pinned.expires_at,
    }
}

#[cfg(feature = "lab-diagnostics")]
fn record_quic_capacity_lifecycle(
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
            reservation.path_instance_id.0,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct ServerQuicCapacityCalibrationPathKey {
    pub(super) session_id: SessionId,
    pub(super) path: CarrierPathKey,
    pub(super) path_instance_id: ServerCarrierPathInstanceId,
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

impl ServerPathLaneLoad {
    fn add(&mut self, lane: FlowLane) {
        self.active_flows = self.active_flows.saturating_add(1);
        if response_lane_is_latency_sensitive(lane) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
    }

    fn remove(&mut self, lane: FlowLane) {
        self.active_flows = self.active_flows.saturating_sub(1);
        if response_lane_is_latency_sensitive(lane) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_sub(1);
        }
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

/// Holds the one session-wide TCP carrier-discovery slot until the typed
/// command is dropped after receipt, failure, or cancellation.
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
        if state
            .tcp_capacity_probe_reservations
            .remove(&self.session_id)
        {
            state.bump_generation(self.session_id);
            state.maybe_reclaim_session(self.session_id);
        }
    }
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
    fn bump_generation(&mut self, session_id: SessionId) {
        let generation = self.session_generations.entry(session_id).or_default();
        *generation = generation.wrapping_add(1);
    }

    fn add_response_service(
        &mut self,
        session_id: SessionId,
        path: CarrierPathKey,
        lane: FlowLane,
    ) {
        self.response_service_loads
            .entry(ServerPathLoadKey { session_id, path })
            .or_default()
            .add(lane);
        self.response_service_session_loads
            .entry(session_id)
            .or_default()
            .add(lane);
        let family = self
            .response_service_family_loads
            .entry(session_id)
            .or_default();
        match path.underlay {
            UnderlayProtocol::Tcp => family.tcp = family.tcp.saturating_add(1),
            UnderlayProtocol::Udp => family.udp = family.udp.saturating_add(1),
        }
    }

    fn remove_response_service(
        &mut self,
        session_id: SessionId,
        path: CarrierPathKey,
        lane: FlowLane,
    ) -> bool {
        let key = ServerPathLoadKey { session_id, path };
        let Some(path_load) = self.response_service_loads.get_mut(&key) else {
            return false;
        };
        if path_load.active_flows == 0 {
            return false;
        }
        path_load.remove(lane);
        if path_load.active_flows == 0 {
            self.response_service_loads.remove(&key);
        }

        if let Some(session_load) = self.response_service_session_loads.get_mut(&session_id) {
            session_load.remove(lane);
            if session_load.active_flows == 0 {
                self.response_service_session_loads.remove(&session_id);
            }
        }
        if let Some(family) = self.response_service_family_loads.get_mut(&session_id) {
            match path.underlay {
                UnderlayProtocol::Tcp => family.tcp = family.tcp.saturating_sub(1),
                UnderlayProtocol::Udp => family.udp = family.udp.saturating_sub(1),
            }
            if family.tcp == 0 && family.udp == 0 {
                self.response_service_family_loads.remove(&session_id);
            }
        }
        true
    }

    fn move_response_service(
        &mut self,
        session_id: SessionId,
        from: CarrierPathKey,
        to: CarrierPathKey,
        lane: FlowLane,
    ) -> bool {
        if from == to {
            return self
                .response_service_loads
                .contains_key(&ServerPathLoadKey {
                    session_id,
                    path: from,
                });
        }
        if !self.remove_response_service(session_id, from, lane) {
            return false;
        }
        self.add_response_service(session_id, to, lane);
        true
    }

    fn response_service_session_load(&self, session_id: SessionId) -> ServerPathLaneLoad {
        let mut session_load = self
            .response_service_session_loads
            .get(&session_id)
            .copied()
            .unwrap_or_default();
        let realtime_flows = self.realtime_flows.get(&session_id).copied().unwrap_or(0);
        session_load.active_flows = session_load.active_flows.saturating_add(realtime_flows);
        session_load.active_latency_sensitive_flows = session_load
            .active_latency_sensitive_flows
            .saturating_add(realtime_flows);
        session_load
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
        let quic_capacity_calibration_attempts = self
            .quic_capacity_calibration_attempts
            .get(&ServerQuicCapacityCalibrationPathKey {
                session_id,
                path,
                path_instance_id,
            })
            .copied()
            .unwrap_or(0);
        ServerResponsePathSchedulingSnapshot {
            path_load,
            session_load,
            quic_capacity_calibration_attempts,
        }
    }

    fn maybe_reclaim_session(&mut self, session_id: SessionId) {
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
        let proof_publication_in_progress = self
            .quic_capacity_calibrations
            .get(&session_id)
            .is_some_and(|reservation| {
                matches!(
                    reservation.phase,
                    ServerQuicCapacityCalibrationPhase::ProofPublishing { .. }
                )
            });
        let tcp_capacity_probe_in_progress =
            self.tcp_capacity_probe_reservations.contains(&session_id);
        if !has_references
            && !has_realtime
            && !has_active_response_flows
            && !has_loads
            && !proof_publication_in_progress
            && !tcp_capacity_probe_in_progress
        {
            let capacity_reservation = self.quic_capacity_calibrations.remove(&session_id);
            self.quic_capacity_calibration_attempts
                .retain(|key, _| key.session_id != session_id);
            self.quic_capacity_calibration_bytes.remove(&session_id);
            self.tcp_capacity_probe_reservations.remove(&session_id);
            self.response_service_handoff_drains.remove(&session_id);
            self.response_service_session_loads.remove(&session_id);
            self.response_service_family_loads.remove(&session_id);
            self.session_generations.remove(&session_id);
            if let Some(reservation) = capacity_reservation.as_ref() {
                reservation.command_ticket.cancel();
            }
            #[cfg(feature = "lab-diagnostics")]
            if let Some(reservation) = capacity_reservation {
                record_quic_capacity_lifecycle(
                    "cancelled",
                    "session_reclaimed",
                    session_id,
                    reservation,
                    None,
                );
            }
            #[cfg(not(feature = "lab-diagnostics"))]
            let _ = capacity_reservation;
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
    #[allow(clippy::too_many_arguments)]
    pub(super) fn try_reserve_test_quic_capacity_calibration(
        &self,
        session_id: SessionId,
        expected_generation: u64,
        binding_instance_id: u64,
        path: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
        train_bytes: u64,
        session_byte_limit: u64,
        token: u64,
    ) -> bool {
        self.try_reserve_quic_capacity_calibration(
            session_id,
            expected_generation,
            binding_instance_id,
            path,
            path_instance_id,
            train_bytes,
            train_bytes,
            (PATH_OPEN_SCORE_BYTES as u64).min(train_bytes / 8),
            0,
            train_bytes.saturating_sub((PATH_OPEN_SCORE_BYTES as u64).min(train_bytes / 8)),
            Duration::from_secs(1),
            session_byte_limit,
            token,
            QuicCapacityProbeCommandTicket::new(),
        )
    }

    #[cfg(test)]
    pub(super) fn commit_test_quic_capacity_calibration(
        &self,
        session_id: SessionId,
        binding_instance_id: u64,
        path: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
        lease: Duration,
        token: u64,
    ) -> bool {
        Instant::now().checked_add(lease).is_some_and(|expires_at| {
            self.commit_quic_capacity_calibration(
                session_id,
                binding_instance_id,
                path,
                path_instance_id,
                expires_at,
                token,
            )
        })
    }

    #[cfg(test)]
    pub(super) fn complete_test_quic_capacity_calibration(
        &self,
        session_id: SessionId,
        binding_instance_id: u64,
        path: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
    ) -> bool {
        let reservation = self
            .state
            .lock()
            .expect("server path lane tracker lock")
            .quic_capacity_calibrations
            .get(&session_id)
            .cloned();
        let Some(reservation) = reservation.filter(|reservation| {
            reservation.binding_instance_id == binding_instance_id
                && reservation.path == path
                && reservation.path_instance_id == path_instance_id
        }) else {
            return false;
        };
        let accepted_at = Instant::now();
        let candidate = QuicCapacityProofCandidate {
            token: reservation.token,
            train_bytes: reservation.train_bytes,
            sample_floor_bytes: reservation.sample_floor_bytes,
            accounting_slack_bytes: reservation.accounting_slack_bytes,
            warmup_bytes: reservation.warmup_bytes,
            required_proof_bytes: reservation.required_proof_bytes,
            written_bytes: reservation.train_bytes,
            written_data_frame_count: 1,
            receipt_confirmed: true,
            received_bytes: reservation.train_bytes,
            proof_elapsed: Duration::from_millis(1),
            rate_bps: reservation.train_bytes.saturating_mul(8_000),
            accepted_at,
            expires_at: accepted_at + Duration::from_secs(1),
            proof_validity: reservation.proof_validity,
        };
        let Some(ticket) =
            self.try_accept_quic_capacity_proof(session_id, path, path_instance_id, candidate)
        else {
            return false;
        };
        self.commit_quic_capacity_proof(ticket)
            .and_then(|_| self.finish_quic_capacity_proof_publication(ticket))
            .is_some()
    }

    #[cfg(test)]
    pub(super) fn generation_and_active_response_flows(&self, session_id: SessionId) -> (u64, u32) {
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
        (generation, active_response_flows)
    }

    pub(super) fn response_scheduling_snapshot(
        &self,
        session_id: SessionId,
    ) -> ResponseSessionSchedulingSnapshot {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let now = Instant::now();
        let capacity_removal =
            state
                .quic_capacity_calibrations
                .get(&session_id)
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
                });
        if let Some((phase, reason)) = capacity_removal {
            let reservation = state.quic_capacity_calibrations.remove(&session_id);
            if let Some(reservation) = reservation.as_ref() {
                reservation.command_ticket.cancel();
            }
            state.bump_generation(session_id);
            #[cfg(feature = "lab-diagnostics")]
            if let Some(reservation) = reservation {
                record_quic_capacity_lifecycle(phase, reason, session_id, reservation, None);
            }
            #[cfg(not(feature = "lab-diagnostics"))]
            let _ = (reservation, phase, reason);
        }
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
                .quic_capacity_calibrations
                .contains_key(&session_id),
            quic_capacity_calibration_spent_bytes: state
                .quic_capacity_calibration_bytes
                .get(&session_id)
                .copied()
                .unwrap_or(0),
            response_service_handoff_drain: state
                .response_service_handoff_drains
                .get(&session_id)
                .copied(),
        }
    }

    pub(in crate::runtime) fn try_reserve_tcp_capacity_probe(
        self: &Arc<Self>,
        session_id: SessionId,
        expected_generation: u64,
    ) -> Option<TcpCapacityProbeSessionLease> {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let generation = state
            .session_generations
            .get(&session_id)
            .copied()
            .unwrap_or(0);
        if generation != expected_generation
            || state.tcp_capacity_probe_reservations.contains(&session_id)
            || state.quic_capacity_calibrations.contains_key(&session_id)
            || state
                .response_service_handoff_drains
                .contains_key(&session_id)
        {
            return None;
        }
        state.tcp_capacity_probe_reservations.insert(session_id);
        state.bump_generation(session_id);
        Some(TcpCapacityProbeSessionLease {
            tracker: Arc::clone(self),
            session_id,
        })
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
            || state.quic_capacity_calibrations.contains_key(&session_id)
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

    pub(super) fn try_reserve_quic_capacity_calibration(
        &self,
        session_id: SessionId,
        expected_generation: u64,
        binding_instance_id: u64,
        path: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
        train_bytes: u64,
        sample_floor_bytes: u64,
        accounting_slack_bytes: u64,
        warmup_bytes: u64,
        required_proof_bytes: u64,
        proof_validity: Duration,
        session_byte_limit: u64,
        token: u64,
        command_ticket: QuicCapacityProbeCommandTicket,
    ) -> bool {
        if proof_validity.is_zero()
            || !valid_quic_capacity_geometry(
                train_bytes,
                sample_floor_bytes,
                accounting_slack_bytes,
                warmup_bytes,
                required_proof_bytes,
            )
        {
            return false;
        }
        let mut state = self.state.lock().expect("server path lane tracker lock");
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
        let attempt_key = ServerQuicCapacityCalibrationPathKey {
            session_id,
            path,
            path_instance_id,
        };
        let attempts = state
            .quic_capacity_calibration_attempts
            .get(&attempt_key)
            .copied()
            .unwrap_or(0);
        let spent_bytes = state
            .quic_capacity_calibration_bytes
            .get(&session_id)
            .copied()
            .unwrap_or(0);
        let Some(next_spent_bytes) = spent_bytes.checked_add(train_bytes) else {
            return false;
        };
        if generation != expected_generation
            || active_response_flows < 2
            || attempts >= MAX_RESPONSE_QUIC_CAPACITY_CALIBRATION_ATTEMPTS_PER_PATH
            || next_spent_bytes > session_byte_limit
            || state.tcp_capacity_probe_reservations.contains(&session_id)
            || state.quic_capacity_calibrations.contains_key(&session_id)
            || state
                .response_service_handoff_drains
                .contains_key(&session_id)
        {
            return false;
        }
        state.quic_capacity_calibrations.insert(
            session_id,
            ServerQuicCapacityCalibrationReservation {
                binding_instance_id,
                path,
                path_instance_id,
                phase: ServerQuicCapacityCalibrationPhase::Provisional,
                train_bytes,
                sample_floor_bytes,
                accounting_slack_bytes,
                warmup_bytes,
                required_proof_bytes,
                proof_validity,
                token,
                command_ticket,
            },
        );
        state
            .quic_capacity_calibration_attempts
            .insert(attempt_key, attempts.saturating_add(1));
        state
            .quic_capacity_calibration_bytes
            .insert(session_id, next_spent_bytes);
        state.bump_generation(session_id);
        true
    }

    pub(super) fn rollback_quic_capacity_calibration(
        &self,
        session_id: SessionId,
        binding_instance_id: u64,
        path: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
        token: u64,
    ) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let matches = state
            .quic_capacity_calibrations
            .get(&session_id)
            .is_some_and(|reservation| {
                reservation.binding_instance_id == binding_instance_id
                    && reservation.path == path
                    && reservation.path_instance_id == path_instance_id
                    && reservation.token == token
                    && reservation.phase == ServerQuicCapacityCalibrationPhase::Provisional
            });
        if matches {
            let reservation = state
                .quic_capacity_calibrations
                .remove(&session_id)
                .expect("matching QUIC capacity reservation");
            reservation.command_ticket.cancel();
            let attempt_key = ServerQuicCapacityCalibrationPathKey {
                session_id,
                path,
                path_instance_id,
            };
            if let Some(attempts) = state
                .quic_capacity_calibration_attempts
                .get_mut(&attempt_key)
            {
                *attempts = attempts.saturating_sub(1);
                if *attempts == 0 {
                    state
                        .quic_capacity_calibration_attempts
                        .remove(&attempt_key);
                }
            }
            if let Some(spent_bytes) = state.quic_capacity_calibration_bytes.get_mut(&session_id) {
                debug_assert!(*spent_bytes >= reservation.train_bytes);
                *spent_bytes -= reservation.train_bytes.min(*spent_bytes);
                if *spent_bytes == 0 {
                    state.quic_capacity_calibration_bytes.remove(&session_id);
                }
            }
            state.bump_generation(session_id);
            #[cfg(feature = "lab-diagnostics")]
            record_quic_capacity_lifecycle(
                "cancelled",
                "provisional_rollback",
                session_id,
                reservation,
                None,
            );
        }
    }

    pub(in crate::runtime::stream) fn try_accept_quic_capacity_proof(
        &self,
        session_id: SessionId,
        path: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
        candidate: QuicCapacityProofCandidate,
    ) -> Option<ServerQuicCapacityProofTicket> {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        // Deadlines must be sampled after lock acquisition; otherwise mutex
        // contention can make stale carrier evidence appear lease-valid.
        let now = Instant::now();
        let removal = state
            .quic_capacity_calibrations
            .get(&session_id)
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
                        Some(("expired", "lease_elapsed_before_proof"))
                    }
                    _ => None,
                }
            });
        if let Some((phase, reason)) = removal {
            let reservation = state
                .quic_capacity_calibrations
                .remove(&session_id)
                .expect("expired QUIC capacity calibration");
            reservation.command_ticket.cancel();
            state.bump_generation(session_id);
            #[cfg(feature = "lab-diagnostics")]
            record_quic_capacity_lifecycle(phase, reason, session_id, reservation, Some(candidate));
            #[cfg(not(feature = "lab-diagnostics"))]
            let _ = (reservation, phase, reason);
            return None;
        }

        let reservation = state.quic_capacity_calibrations.get_mut(&session_id)?;
        let active_expires_at = match reservation.phase {
            ServerQuicCapacityCalibrationPhase::Active { expires_at } => expires_at,
            ServerQuicCapacityCalibrationPhase::Provisional
            | ServerQuicCapacityCalibrationPhase::ProofAccepted { .. }
            | ServerQuicCapacityCalibrationPhase::ProofPublishing { .. } => return None,
        };
        let specification_matches = reservation.command_ticket.is_current()
            && reservation.path == path
            && reservation.path_instance_id == path_instance_id
            && reservation.token == candidate.token
            && reservation.train_bytes == candidate.train_bytes
            && reservation.sample_floor_bytes == candidate.sample_floor_bytes
            && reservation.accounting_slack_bytes == candidate.accounting_slack_bytes
            && reservation.warmup_bytes == candidate.warmup_bytes
            && reservation.required_proof_bytes == candidate.required_proof_bytes
            && reservation.proof_validity == candidate.proof_validity;
        let evidence_is_complete = well_formed_quic_capacity_proof_candidate(candidate)
            && candidate.accepted_at <= now
            && candidate.accepted_at < active_expires_at
            && candidate.expires_at > now;
        if !specification_matches || !evidence_is_complete {
            return None;
        }

        reservation.phase = ServerQuicCapacityCalibrationPhase::ProofAccepted { candidate };
        Some(ServerQuicCapacityProofTicket {
            session_id,
            binding_instance_id: reservation.binding_instance_id,
            path,
            path_instance_id,
            candidate,
        })
    }

    pub(in crate::runtime::stream) fn commit_quic_capacity_proof(
        &self,
        ticket: ServerQuicCapacityProofTicket,
    ) -> Option<u64> {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let reservation = state
            .quic_capacity_calibrations
            .get_mut(&ticket.session_id)
            .filter(|reservation| {
                reservation.command_ticket.is_current()
                    && reservation.binding_instance_id == ticket.binding_instance_id
                    && reservation.path == ticket.path
                    && reservation.path_instance_id == ticket.path_instance_id
                    && reservation.token == ticket.candidate.token
                    && reservation.phase
                        == ServerQuicCapacityCalibrationPhase::ProofAccepted {
                            candidate: ticket.candidate,
                        }
            })?;
        // The evidence transaction is now irrevocable, but its reservation
        // remains the session barrier until registry publication is complete.
        reservation.phase = ServerQuicCapacityCalibrationPhase::ProofPublishing {
            candidate: ticket.candidate,
        };
        Some(ticket.binding_instance_id)
    }

    pub(in crate::runtime::stream) fn finish_quic_capacity_proof_publication(
        &self,
        ticket: ServerQuicCapacityProofTicket,
    ) -> Option<u64> {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let matches = state
            .quic_capacity_calibrations
            .get(&ticket.session_id)
            .is_some_and(|reservation| {
                reservation.binding_instance_id == ticket.binding_instance_id
                    && reservation.path == ticket.path
                    && reservation.path_instance_id == ticket.path_instance_id
                    && reservation.token == ticket.candidate.token
                    && reservation.phase
                        == ServerQuicCapacityCalibrationPhase::ProofPublishing {
                            candidate: ticket.candidate,
                        }
            });
        if !matches {
            return None;
        }
        let reservation = state
            .quic_capacity_calibrations
            .remove(&ticket.session_id)
            .expect("matching published QUIC capacity proof");
        reservation.command_ticket.publish();
        // Publication releases serialization but never refunds attempts or bytes.
        state.bump_generation(ticket.session_id);
        #[cfg(feature = "lab-diagnostics")]
        record_quic_capacity_lifecycle(
            "completed",
            "exact_carrier_proof",
            ticket.session_id,
            reservation,
            Some(ticket.candidate),
        );
        #[cfg(not(feature = "lab-diagnostics"))]
        let _ = reservation;
        state.maybe_reclaim_session(ticket.session_id);
        Some(ticket.binding_instance_id)
    }

    pub(super) fn commit_quic_capacity_calibration(
        &self,
        session_id: SessionId,
        binding_instance_id: u64,
        path: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
        expires_at: Instant,
        token: u64,
    ) -> bool {
        if expires_at <= Instant::now() {
            return false;
        }
        let mut state = self.state.lock().expect("server path lane tracker lock");
        if expires_at <= Instant::now() {
            return false;
        }
        if let Some(reservation) = state
            .quic_capacity_calibrations
            .get_mut(&session_id)
            .filter(|reservation| {
                reservation.command_ticket.is_current()
                    && reservation.binding_instance_id == binding_instance_id
                    && reservation.path == path
                    && reservation.path_instance_id == path_instance_id
                    && reservation.token == token
                    && reservation.phase == ServerQuicCapacityCalibrationPhase::Provisional
            })
        {
            // The provisional lease serialized admission. Start the effective
            // lease only after the complete carrier train owns queue capacity.
            reservation.phase = ServerQuicCapacityCalibrationPhase::Active { expires_at };
            true
        } else {
            false
        }
    }

    pub(super) fn clear_quic_capacity_calibration(
        &self,
        session_id: SessionId,
        binding_instance_id: u64,
        path: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
    ) {
        self.cancel_quic_capacity_calibration(
            session_id,
            binding_instance_id,
            path,
            path_instance_id,
            "path_output_removed",
        );
    }

    pub(super) fn cancel_quic_capacity_calibration(
        &self,
        session_id: SessionId,
        binding_instance_id: u64,
        path: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
        reason: &'static str,
    ) -> bool {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let matches = state
            .quic_capacity_calibrations
            .get(&session_id)
            .is_some_and(|reservation| {
                reservation.binding_instance_id == binding_instance_id
                    && reservation.path == path
                    && reservation.path_instance_id == path_instance_id
                    && !matches!(
                        reservation.phase,
                        ServerQuicCapacityCalibrationPhase::ProofPublishing { .. }
                    )
            });
        if matches {
            let reservation = state.quic_capacity_calibrations.remove(&session_id);
            if let Some(reservation) = reservation.as_ref() {
                reservation.command_ticket.cancel();
            }
            state.bump_generation(session_id);
            #[cfg(feature = "lab-diagnostics")]
            if let Some(reservation) = reservation {
                record_quic_capacity_lifecycle("cancelled", reason, session_id, reservation, None);
            }
            #[cfg(not(feature = "lab-diagnostics"))]
            let _ = (reservation, reason);
        }
        matches
    }

    pub(super) fn clear_quic_capacity_calibration_for_binding(
        &self,
        session_id: SessionId,
        binding_instance_id: u64,
    ) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let matches = state
            .quic_capacity_calibrations
            .get(&session_id)
            .is_some_and(|reservation| {
                reservation.binding_instance_id == binding_instance_id
                    && !matches!(
                        reservation.phase,
                        ServerQuicCapacityCalibrationPhase::ProofPublishing { .. }
                    )
            });
        if matches {
            // Close may race carrier dequeue, so transmitted-byte and attempt
            // charges remain consumed even though session serialization clears.
            let reservation = state.quic_capacity_calibrations.remove(&session_id);
            if let Some(reservation) = reservation.as_ref() {
                reservation.command_ticket.cancel();
            }
            state.bump_generation(session_id);
            #[cfg(feature = "lab-diagnostics")]
            if let Some(reservation) = reservation {
                record_quic_capacity_lifecycle(
                    "cancelled",
                    "binding_closed",
                    session_id,
                    reservation,
                    None,
                );
            }
            #[cfg(not(feature = "lab-diagnostics"))]
            let _ = reservation;
        }
    }

    pub(in crate::runtime::stream) fn retire_quic_capacity_calibration_path_instance(
        &self,
        session_id: SessionId,
        path: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
    ) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let reservation_matches = state
            .quic_capacity_calibrations
            .get(&session_id)
            .is_some_and(|reservation| {
                reservation.path == path
                    && reservation.path_instance_id == path_instance_id
                    && !matches!(
                        reservation.phase,
                        ServerQuicCapacityCalibrationPhase::ProofPublishing { .. }
                    )
            });
        let retired_reservation = if reservation_matches {
            // An admitted train may already be on the retired carrier queue;
            // retirement releases serialization but never refunds session spend.
            state.quic_capacity_calibrations.remove(&session_id)
        } else {
            None
        };
        if let Some(reservation) = retired_reservation.as_ref() {
            reservation.command_ticket.cancel();
        }
        let attempts_removed = state
            .quic_capacity_calibration_attempts
            .remove(&ServerQuicCapacityCalibrationPathKey {
                session_id,
                path,
                path_instance_id,
            })
            .is_some();
        if reservation_matches || attempts_removed {
            state.bump_generation(session_id);
        }
        #[cfg(feature = "lab-diagnostics")]
        if let Some(reservation) = retired_reservation {
            record_quic_capacity_lifecycle(
                "retired",
                "carrier_instance_retired",
                session_id,
                reservation,
                None,
            );
        }
        #[cfg(not(feature = "lab-diagnostics"))]
        let _ = retired_reservation;
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

    pub(super) fn attach(&self, session_id: SessionId, path: CarrierPathKey, lane: FlowLane) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        state
            .loads
            .entry(ServerPathLoadKey { session_id, path })
            .or_default()
            .add(lane);
        state.bump_generation(session_id);
    }

    pub(super) fn detach(&self, session_id: SessionId, path: CarrierPathKey, lane: FlowLane) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let key = ServerPathLoadKey { session_id, path };
        let changed = if let Some(load) = state.loads.get_mut(&key) {
            load.remove(lane);
            if load.active_flows == 0 {
                state.loads.remove(&key);
            }
            true
        } else {
            false
        };
        if changed {
            state.bump_generation(session_id);
        }
        state.maybe_reclaim_session(session_id);
    }

    pub(super) fn change_lanes(
        &self,
        session_id: SessionId,
        paths: &[CarrierPathKey],
        from: FlowLane,
        to: FlowLane,
    ) {
        if from == to {
            return;
        }
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let mut changed = false;
        for path in paths {
            if let Some(load) = state.loads.get_mut(&ServerPathLoadKey {
                session_id,
                path: *path,
            }) {
                load.remove(from);
                load.add(to);
                changed = true;
            }
        }
        if changed {
            state.bump_generation(session_id);
        }
        state.maybe_reclaim_session(session_id);
    }

    pub(super) fn attach_realtime_flow(&self, session_id: SessionId) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let count = state.realtime_flows.entry(session_id).or_default();
        *count = count.saturating_add(1);
        state.bump_generation(session_id);
    }

    pub(super) fn set_response_flow_active(&self, session_id: SessionId, active: bool) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        if active {
            let count = state.active_response_flows.entry(session_id).or_default();
            *count = count.saturating_add(1);
            state.bump_generation(session_id);
            return;
        }

        let changed = if let Some(count) = state.active_response_flows.get_mut(&session_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.active_response_flows.remove(&session_id);
            }
            true
        } else {
            false
        };
        if changed {
            state.bump_generation(session_id);
        }
        state.maybe_reclaim_session(session_id);
    }

    pub(super) fn detach_realtime_flow(&self, session_id: SessionId) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let changed = if let Some(count) = state.realtime_flows.get_mut(&session_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.realtime_flows.remove(&session_id);
            }
            true
        } else {
            false
        };
        if changed {
            state.bump_generation(session_id);
        }
        state.maybe_reclaim_session(session_id);
    }

    #[cfg(test)]
    pub(super) fn snapshot(
        &self,
        session_id: SessionId,
        path: CarrierPathKey,
    ) -> ServerPathLaneLoad {
        self.state
            .lock()
            .expect("server path lane tracker lock")
            .loads
            .get(&ServerPathLoadKey { session_id, path })
            .copied()
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(super) fn session_snapshot(&self, session_id: SessionId) -> ServerPathLaneLoad {
        let state = self.state.lock().expect("server path lane tracker lock");
        let mut total = state
            .loads
            .iter()
            .filter(|(key, _)| key.session_id == session_id)
            .fold(ServerPathLaneLoad::default(), |mut total, (_, load)| {
                total.active_flows = total.active_flows.saturating_add(load.active_flows);
                total.active_latency_sensitive_flows = total
                    .active_latency_sensitive_flows
                    .saturating_add(load.active_latency_sensitive_flows);
                total
            });
        let realtime_flows = state.realtime_flows.get(&session_id).copied().unwrap_or(0);
        total.active_flows = total.active_flows.saturating_add(realtime_flows);
        total.active_latency_sensitive_flows = total
            .active_latency_sensitive_flows
            .saturating_add(realtime_flows);
        total
    }

    pub(super) fn response_service_snapshot(
        &self,
        session_id: SessionId,
        path: CarrierPathKey,
    ) -> ServerPathLaneLoad {
        self.state
            .lock()
            .expect("server path lane tracker lock")
            .response_service_loads
            .get(&ServerPathLoadKey { session_id, path })
            .copied()
            .unwrap_or_default()
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

    pub(super) fn attach_response_service(
        &self,
        session_id: SessionId,
        path: CarrierPathKey,
        lane: FlowLane,
    ) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        state.add_response_service(session_id, path, lane);
        state.bump_generation(session_id);
    }

    pub(super) fn detach_response_service(
        &self,
        session_id: SessionId,
        path: CarrierPathKey,
        lane: FlowLane,
    ) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let changed = state.remove_response_service(session_id, path, lane);
        if changed {
            state.bump_generation(session_id);
        }
        state.maybe_reclaim_session(session_id);
    }

    pub(super) fn move_response_service(
        &self,
        session_id: SessionId,
        from: CarrierPathKey,
        to: CarrierPathKey,
        lane: FlowLane,
    ) {
        if from == to {
            return;
        }
        let mut state = self.state.lock().expect("server path lane tracker lock");
        if state.move_response_service(session_id, from, to, lane) {
            state.bump_generation(session_id);
        }
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
        if generation != expected_generation
            || state.quic_capacity_calibrations.contains_key(&session_id)
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

    pub(super) fn change_response_service_lane(
        &self,
        session_id: SessionId,
        path: CarrierPathKey,
        from: FlowLane,
        to: FlowLane,
    ) {
        if from == to {
            return;
        }
        let mut state = self.state.lock().expect("server path lane tracker lock");
        if let Some(load) = state
            .response_service_loads
            .get_mut(&ServerPathLoadKey { session_id, path })
        {
            load.remove(from);
            load.add(to);
            if let Some(session_load) = state.response_service_session_loads.get_mut(&session_id) {
                session_load.remove(from);
                session_load.add(to);
            }
            state.bump_generation(session_id);
        }
    }
}

pub(super) struct ServerResponseFlowRegistration {
    lane_tracker: Arc<ServerPathLaneTracker>,
    session_id: SessionId,
    active: Mutex<bool>,
    service: Mutex<Option<(CarrierPathKey, FlowLane)>>,
}

impl ServerResponseFlowRegistration {
    pub(super) fn new(
        lane_tracker: Arc<ServerPathLaneTracker>,
        session_id: SessionId,
        service: CarrierPathKey,
        lane: FlowLane,
    ) -> Self {
        lane_tracker.attach_session(session_id);
        lane_tracker.attach_response_service(session_id, service, lane);
        Self {
            lane_tracker,
            session_id,
            active: Mutex::new(false),
            service: Mutex::new(Some((service, lane))),
        }
    }

    pub(super) fn set_active(&self, active: bool) {
        let mut current = self
            .active
            .lock()
            .expect("server response flow registration lock");
        if *current == active {
            return;
        }
        self.lane_tracker
            .set_response_flow_active(self.session_id, active);
        *current = active;
    }

    pub(super) fn set_service(&self, next: Option<(CarrierPathKey, FlowLane)>) {
        let mut current = self
            .service
            .lock()
            .expect("server response Service registration lock");
        if *current == next {
            return;
        }
        match (*current, next) {
            (Some((from, from_lane)), Some((to, to_lane))) if from == to => {
                self.lane_tracker.change_response_service_lane(
                    self.session_id,
                    from,
                    from_lane,
                    to_lane,
                );
            }
            (Some((from, from_lane)), Some((to, to_lane))) => {
                debug_assert_eq!(from_lane, to_lane);
                self.lane_tracker
                    .move_response_service(self.session_id, from, to, to_lane);
            }
            (Some((from, lane)), None) => {
                self.lane_tracker
                    .detach_response_service(self.session_id, from, lane);
            }
            (None, Some((to, lane))) => {
                self.lane_tracker
                    .attach_response_service(self.session_id, to, lane);
            }
            (None, None) => {}
        }
        *current = next;
    }

    pub(super) fn change_lane_if_present(&self, from: FlowLane, to: FlowLane) {
        if from == to {
            return;
        }
        let mut current = self
            .service
            .lock()
            .expect("server response Service registration lock");
        let Some((path, registered_lane)) = *current else {
            return;
        };
        debug_assert_eq!(registered_lane, from);
        self.lane_tracker
            .change_response_service_lane(self.session_id, path, registered_lane, to);
        *current = Some((path, to));
    }

    pub(super) fn commit_reserved_service_move(
        &self,
        from: CarrierPathKey,
        to: CarrierPathKey,
        lane: FlowLane,
    ) {
        let mut current = self
            .service
            .lock()
            .expect("server response Service registration lock");
        debug_assert_eq!(*current, Some((from, lane)));
        *current = Some((to, lane));
    }
}

impl Drop for ServerResponseFlowRegistration {
    fn drop(&mut self) {
        self.set_active(false);
        self.set_service(None);
        self.lane_tracker.detach_session(self.session_id);
    }
}

pub(in crate::runtime) struct ServerRealtimeFlowRegistration {
    lane_tracker: Arc<ServerPathLaneTracker>,
    session_id: SessionId,
}

impl ServerRealtimeFlowRegistration {
    pub(in crate::runtime::stream) fn new(
        lane_tracker: Arc<ServerPathLaneTracker>,
        session_id: SessionId,
    ) -> Self {
        lane_tracker.attach_realtime_flow(session_id);
        Self {
            lane_tracker,
            session_id,
        }
    }
}

impl Drop for ServerRealtimeFlowRegistration {
    fn drop(&mut self) {
        self.lane_tracker.detach_realtime_flow(self.session_id);
    }
}

#[cfg(test)]
mod tests;
