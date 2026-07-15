//! Session-wide QUIC capacity reservation and proof lifecycle.
//!
//! This owner validates and commits one carrier-discovery transaction through
//! receipt and publication. The session coordinator owns its exclusive slot
//! and expiry; binding queue admission remains in `quic_admission`.

use super::MAX_RESPONSE_QUIC_CAPACITY_CALIBRATION_ATTEMPTS_PER_PATH;
#[cfg(test)]
use super::session::ServerQuicCapacityHistorySnapshot;
#[cfg(feature = "lab-diagnostics")]
use super::session::record_quic_capacity_lifecycle;
use super::session::{
    ServerPathLaneTracker, ServerPathLaneTrackerState, ServerQuicCapacityCalibrationPhase,
    ServerQuicCapacityCalibrationReservation,
};
use crate::model::capacity::{
    QuicCapacityProofCandidate, quic_capacity_receipt_rate_bps, valid_quic_capacity_proof_geometry,
};
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey};
use crate::protocol::SessionId;
use crate::runtime::path::commands::CapacityProbeCommandTicket;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::stream) struct ServerQuicCapacityProofTicket {
    pub(super) session_id: SessionId,
    pub(super) binding_instance_id: u64,
    pub(super) path: CarrierPathKey,
    pub(super) path_instance_id: CarrierPathInstanceId,
    pub(super) candidate: QuicCapacityProofCandidate,
}

pub(in crate::runtime) fn well_formed_quic_capacity_proof_candidate(
    proof: QuicCapacityProofCandidate,
) -> bool {
    valid_quic_capacity_proof_geometry(
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

impl ServerPathLaneTrackerState {
    pub(super) fn quic_capacity_calibration_attempts_for_path(
        &self,
        session_id: SessionId,
        path: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
    ) -> u8 {
        self.session(session_id)
            .map(|session| {
                session
                    .quic_history()
                    .attempts_for_path(path, path_instance_id)
            })
            .unwrap_or(0)
    }

    pub(super) fn quic_capacity_calibration_spent_bytes(&self, session_id: SessionId) -> u64 {
        self.session(session_id)
            .map(|session| session.quic_history().spent_bytes())
            .unwrap_or(0)
    }
}

impl ServerPathLaneTracker {
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(super) fn set_quic_capacity_active_expiry_for_test(
        &self,
        session_id: SessionId,
        binding_instance_id: u64,
        path: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
        token: u64,
        expires_at: Instant,
    ) -> bool {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let Some(reservation) = state
            .session_mut(session_id)
            .and_then(|session| session.quic_capacity_calibration_mut())
            .filter(|reservation| {
                reservation.binding_instance_id == binding_instance_id
                    && reservation.path == path
                    && reservation.path_instance_id == path_instance_id
                    && reservation.token == token
                    && matches!(
                        reservation.phase,
                        ServerQuicCapacityCalibrationPhase::Active { .. }
                    )
            })
        else {
            return false;
        };
        reservation.phase = ServerQuicCapacityCalibrationPhase::Active { expires_at };
        true
    }

    #[cfg(test)]
    pub(super) fn quic_capacity_history_snapshot_for_test(
        &self,
        session_id: SessionId,
    ) -> Option<ServerQuicCapacityHistorySnapshot> {
        let state = self.state.lock().expect("server path lane tracker lock");
        state
            .session(session_id)
            .and_then(|session| session.quic_history().snapshot())
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(super) fn try_reserve_test_quic_capacity_calibration(
        &self,
        session_id: SessionId,
        expected_generation: u64,
        binding_instance_id: u64,
        path: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
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
            CapacityProbeCommandTicket::new(),
        )
    }

    #[cfg(test)]
    pub(super) fn commit_test_quic_capacity_calibration(
        &self,
        session_id: SessionId,
        binding_instance_id: u64,
        path: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
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
        path_instance_id: CarrierPathInstanceId,
    ) -> bool {
        let reservation = self
            .state
            .lock()
            .expect("server path lane tracker lock")
            .session(session_id)
            .and_then(|session| session.quic_capacity_calibration())
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

    pub(super) fn try_reserve_quic_capacity_calibration(
        &self,
        session_id: SessionId,
        expected_generation: u64,
        binding_instance_id: u64,
        path: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
        train_bytes: u64,
        sample_floor_bytes: u64,
        accounting_slack_bytes: u64,
        warmup_bytes: u64,
        required_proof_bytes: u64,
        proof_validity: Duration,
        session_byte_limit: u64,
        token: u64,
        command_ticket: CapacityProbeCommandTicket,
    ) -> bool {
        if proof_validity.is_zero()
            || !valid_quic_capacity_proof_geometry(
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
        let generation = state.generation(session_id);
        let active_response_flows = state
            .session(session_id)
            .map(|session| session.load().active_response_flows())
            .unwrap_or(0);
        let attempts = state
            .session(session_id)
            .map(|session| {
                session
                    .quic_history()
                    .attempts_for_path(path, path_instance_id)
            })
            .unwrap_or(0);
        let spent_bytes = state
            .session(session_id)
            .map(|session| session.quic_history().spent_bytes())
            .unwrap_or(0);
        let Some(next_spent_bytes) = spent_bytes.checked_add(train_bytes) else {
            return false;
        };
        if generation != expected_generation
            || active_response_flows < 2
            || attempts >= MAX_RESPONSE_QUIC_CAPACITY_CALIBRATION_ATTEMPTS_PER_PATH
            || next_spent_bytes > session_byte_limit
        {
            return false;
        }
        let session = state.session_mut_or_default(session_id);
        if !session.reserve_quic_capacity_calibration(ServerQuicCapacityCalibrationReservation {
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
        }) {
            return false;
        }
        session.quic_history_mut().set_attempts_for_path(
            path,
            path_instance_id,
            attempts.saturating_add(1),
        );
        session.quic_history_mut().set_spent_bytes(next_spent_bytes);
        session.bump_generation();
        true
    }

    pub(super) fn rollback_quic_capacity_calibration(
        &self,
        session_id: SessionId,
        binding_instance_id: u64,
        path: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
        token: u64,
    ) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let matches = state
            .session(session_id)
            .and_then(|session| session.quic_capacity_calibration())
            .is_some_and(|reservation| {
                reservation.binding_instance_id == binding_instance_id
                    && reservation.path == path
                    && reservation.path_instance_id == path_instance_id
                    && reservation.token == token
                    && reservation.phase == ServerQuicCapacityCalibrationPhase::Provisional
            });
        if matches {
            let reservation = state
                .session_mut(session_id)
                .and_then(|session| session.take_quic_capacity_calibration())
                .expect("matching QUIC capacity reservation");
            reservation.command_ticket.cancel();
            let session = state
                .session_mut(session_id)
                .expect("matching QUIC capacity session");
            let attempts = session
                .quic_history()
                .attempts_for_path(path, path_instance_id);
            session.quic_history_mut().set_attempts_for_path(
                path,
                path_instance_id,
                attempts.saturating_sub(1),
            );
            let spent_bytes = session.quic_history().spent_bytes();
            debug_assert!(spent_bytes >= reservation.train_bytes);
            session.quic_history_mut().set_spent_bytes(
                spent_bytes.saturating_sub(reservation.train_bytes.min(spent_bytes)),
            );
            session.bump_generation();
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
        path_instance_id: CarrierPathInstanceId,
        candidate: QuicCapacityProofCandidate,
    ) -> Option<ServerQuicCapacityProofTicket> {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        // Deadlines must be sampled after lock acquisition; otherwise mutex
        // contention can make stale carrier evidence appear lease-valid.
        let now = Instant::now();
        let removal = state
            .session(session_id)
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
                        Some(("expired", "lease_elapsed_before_proof"))
                    }
                    _ => None,
                }
            });
        if let Some((phase, reason)) = removal {
            let reservation = state
                .session_mut(session_id)
                .and_then(|session| session.take_quic_capacity_calibration())
                .expect("expired QUIC capacity calibration");
            reservation.command_ticket.cancel();
            state.bump_generation(session_id);
            #[cfg(feature = "lab-diagnostics")]
            record_quic_capacity_lifecycle(phase, reason, session_id, reservation, Some(candidate));
            #[cfg(not(feature = "lab-diagnostics"))]
            let _ = (reservation, phase, reason);
            return None;
        }

        let reservation = state
            .session_mut(session_id)?
            .quic_capacity_calibration_mut()?;
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
            .session_mut(ticket.session_id)?
            .quic_capacity_calibration_mut()
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
            .session(ticket.session_id)
            .and_then(|session| session.quic_capacity_calibration())
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
            .session_mut(ticket.session_id)
            .and_then(|session| session.take_quic_capacity_calibration())
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
        path_instance_id: CarrierPathInstanceId,
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
            .session_mut(session_id)
            .and_then(|session| session.quic_capacity_calibration_mut())
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
        path_instance_id: CarrierPathInstanceId,
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
        path_instance_id: CarrierPathInstanceId,
        reason: &'static str,
    ) -> bool {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let matches = state
            .session(session_id)
            .and_then(|session| session.quic_capacity_calibration())
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
            let reservation = state
                .session_mut(session_id)
                .and_then(|session| session.take_quic_capacity_calibration());
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
            .session(session_id)
            .and_then(|session| session.quic_capacity_calibration())
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
            let reservation = state
                .session_mut(session_id)
                .and_then(|session| session.take_quic_capacity_calibration());
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
        path_instance_id: CarrierPathInstanceId,
    ) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let reservation_matches = state
            .session(session_id)
            .and_then(|session| session.quic_capacity_calibration())
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
            state
                .session_mut(session_id)
                .and_then(|session| session.take_quic_capacity_calibration())
        } else {
            None
        };
        if let Some(reservation) = retired_reservation.as_ref() {
            reservation.command_ticket.cancel();
        }
        let attempts_removed = state.session_mut(session_id).is_some_and(|session| {
            session
                .quic_history_mut()
                .remove_attempts_for_path(path, path_instance_id)
        });
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
}

#[cfg(test)]
#[path = "quic_capacity_test.rs"]
mod tests;
