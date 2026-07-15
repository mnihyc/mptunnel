//! Shared client-path evidence and carrier-neutral capacity budgets.
//!
//! One health lock makes mixed TCP/QUIC observations coherent. Each carrier
//! module owns its reservation, proof, and rollback transaction.

use super::commands::CapacityProbeCommandTicket;
use super::model::{
    ClientPathObservation, PathDeliveryStats, UdpDatagramPathObservation,
    path_observation_is_idle_for_probe, path_record_failure_cooldown,
    path_records_have_schedulable_alternative,
};
use super::proof::PathProofObservation;
use super::quic::metrics::UdpPathMetrics;
use super::quic::{
    RequestQuicCapacityProbeLease, RequestQuicCapacityProbeSession,
    RequestQuicCapacityProductHandoffState, RequestQuicCapacityReconciliationQuery,
    RequestQuicCapacityRecord,
};
use super::set::ClientPathContext;
use super::tcp::capacity::{
    RequestTcpCapacityProbeLease, RequestTcpCapacityProbeSession, RequestTcpCapacityProofQuery,
    RequestTcpCapacityRecord,
};
use super::tcp::metrics::TcpNativeObservation;
#[cfg(test)]
use super::*;
use crate::model::capacity::{
    BBR_MAX_SEND_QUANTUM_BYTES, PATH_OPEN_SCORE_BYTES, PathRateSample, TcpCapacityProofCandidate,
    reliable_capacity_calibration_session_limit_bytes,
};
use crate::model::path::{RelayPathInstance, RelayPathKey};
use crate::protocol::{StreamId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::scheduler::{FlowLane, PathState as SchedulerPathState};
use std::collections::{HashMap, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU32, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

/// One lock domain for coherent health, load, and carrier budget composition.
#[derive(Debug)]
pub(in crate::runtime) struct ClientPathState {
    health: Mutex<ClientPathHealth>,
    next_reliable_stream_id: Mutex<u64>,
    active_tcp_service_request_bulk_flows: AtomicU32,
    request_quic_capacity_probe: RequestQuicCapacityProbeSession,
    request_tcp_capacity_probe: RequestTcpCapacityProbeSession,
}

impl ClientPathState {
    pub(in crate::runtime) fn new(health: ClientPathHealth) -> Arc<Self> {
        let tcp_path_count = health.tcp.len();
        let udp_path_count = health.udp.len();
        Arc::new(Self {
            health: Mutex::new(health),
            next_reliable_stream_id: Mutex::new(0),
            active_tcp_service_request_bulk_flows: AtomicU32::new(0),
            request_quic_capacity_probe: RequestQuicCapacityProbeSession::new(udp_path_count),
            request_tcp_capacity_probe: RequestTcpCapacityProbeSession::new(tcp_path_count),
        })
    }

    pub(in crate::runtime) fn health(&self) -> &Mutex<ClientPathHealth> {
        &self.health
    }

    pub(in crate::runtime::path) fn request_tcp_capacity_probe_session(
        &self,
    ) -> &RequestTcpCapacityProbeSession {
        &self.request_tcp_capacity_probe
    }

    pub(in crate::runtime::path) fn request_quic_capacity_probe_session(
        &self,
    ) -> &RequestQuicCapacityProbeSession {
        &self.request_quic_capacity_probe
    }

    fn release_relay_path_load(&self, key: RelayPathKey, lane: FlowLane) {
        let mut health = self.health.lock().expect("client path health lock");
        let records = match key.underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        if let Some(record) = records.get_mut(key.index) {
            record.release_load(lane);
        }
    }
}

#[derive(Debug)]
pub(in crate::runtime::path) struct RequestCapacityProbeBudget {
    spent_bytes: AtomicU64,
    path_spent_bytes: Box<[AtomicU64]>,
    candidate_share_bytes: AtomicU64,
}

impl RequestCapacityProbeBudget {
    pub(in crate::runtime::path) fn new(path_count: usize) -> Self {
        Self {
            spent_bytes: AtomicU64::new(0),
            path_spent_bytes: (0..path_count)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            candidate_share_bytes: AtomicU64::new(0),
        }
    }

    pub(in crate::runtime::path) fn remaining_bytes(&self, limit: u64) -> u64 {
        limit.saturating_sub(self.spent_bytes.load(Ordering::Acquire))
    }

    pub(in crate::runtime::path) fn effective_candidate_share_bytes(
        &self,
        proposed_path_limit: u64,
        session_limit: u64,
    ) -> u64 {
        let frozen_limit = self.candidate_share_bytes.load(Ordering::Acquire);
        if frozen_limit == 0 {
            proposed_path_limit.min(session_limit)
        } else {
            frozen_limit
        }
    }

    pub(in crate::runtime::path) fn path_remaining_bytes(
        &self,
        path_index: usize,
        proposed_path_limit: u64,
        session_limit: u64,
    ) -> u64 {
        let Some(spent) = self.path_spent_bytes.get(path_index) else {
            return 0;
        };
        let frozen_limit = self.candidate_share_bytes.load(Ordering::Acquire);
        let path_limit = if frozen_limit == 0 {
            proposed_path_limit.min(session_limit)
        } else {
            frozen_limit
        };
        path_limit.saturating_sub(spent.load(Ordering::Acquire))
    }

    pub(in crate::runtime::path) fn try_reserve(
        &self,
        path_index: usize,
        bytes: u64,
        proposed_path_limit: u64,
        session_limit: u64,
    ) -> bool {
        let Some(path_spent) = self.path_spent_bytes.get(path_index) else {
            return false;
        };
        let proposed_path_limit = proposed_path_limit.min(session_limit);
        if proposed_path_limit == 0 {
            return false;
        }
        let path_limit = match self.candidate_share_bytes.compare_exchange(
            0,
            proposed_path_limit,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => proposed_path_limit,
            Err(frozen_limit) => frozen_limit,
        };
        if path_spent
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |spent| {
                spent.checked_add(bytes).filter(|next| *next <= path_limit)
            })
            .is_err()
        {
            return false;
        }
        if self
            .spent_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |spent| {
                spent
                    .checked_add(bytes)
                    .filter(|next| *next <= session_limit)
            })
            .is_err()
        {
            path_spent.fetch_sub(bytes, Ordering::AcqRel);
            return false;
        }
        true
    }

    pub(in crate::runtime::path) fn refund(&self, path_index: usize, bytes: u64) {
        if let Some(path_spent) = self.path_spent_bytes.get(path_index) {
            path_spent.fetch_sub(bytes, Ordering::AcqRel);
            self.spent_bytes.fetch_sub(bytes, Ordering::AcqRel);
        }
    }
}

/// One logical flow may spend one candidate share per protocol. The session
/// retains the full envelope so later flows can still discover other paths.
#[derive(Debug, Default)]
pub(in crate::runtime) struct RequestCapacityProbeCampaignBudget {
    spent_bytes: AtomicU64,
    limit_bytes: AtomicU64,
}

impl RequestCapacityProbeCampaignBudget {
    pub(in crate::runtime) fn remaining_bytes(&self, proposed_limit: u64) -> u64 {
        let frozen_limit = self.limit_bytes.load(Ordering::Acquire);
        let limit = if frozen_limit == 0 {
            proposed_limit
        } else {
            frozen_limit
        };
        limit.saturating_sub(self.spent_bytes.load(Ordering::Acquire))
    }

    pub(in crate::runtime::path) fn try_reserve(&self, bytes: u64, proposed_limit: u64) -> bool {
        if proposed_limit == 0 {
            return false;
        }
        let limit = match self.limit_bytes.compare_exchange(
            0,
            proposed_limit,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => proposed_limit,
            Err(frozen_limit) => frozen_limit,
        };
        self.spent_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |spent| {
                spent.checked_add(bytes).filter(|next| *next <= limit)
            })
            .is_ok()
    }

    pub(in crate::runtime::path) fn refund(&self, bytes: u64) {
        self.spent_bytes.fetch_sub(bytes, Ordering::AcqRel);
    }
}

/// Cancellation-safe ownership of one logical flow's shared path load.
///
/// Initial Active opens acquire a lease before I/O and transfer it with the
/// attachment. Passive validation acquires one only after unique product bytes
/// commit. Dropping any transaction owner rolls the scheduler load back.
pub(in crate::runtime) struct RelayPathLoadLease {
    state: Arc<ClientPathState>,
    key: RelayPathKey,
    lane: FlowLane,
}

impl RelayPathLoadLease {
    pub(super) fn new(state: Arc<ClientPathState>, key: RelayPathKey, lane: FlowLane) -> Self {
        Self { state, key, lane }
    }

    pub(in crate::runtime) fn set_recorded_lane(&mut self, lane: FlowLane) {
        self.lane = lane;
    }

    pub(in crate::runtime) fn key(&self) -> RelayPathKey {
        self.key
    }
}

impl Drop for RelayPathLoadLease {
    fn drop(&mut self) {
        self.state.release_relay_path_load(self.key, self.lane);
    }
}

#[derive(Debug)]
struct ReliableTcpRequestBulkFlowRegistrationState {
    path_state: Arc<ClientPathState>,
    counted: Mutex<bool>,
}

#[derive(Clone, Debug)]
pub(in crate::runtime) struct ReliableTcpRequestBulkFlowRegistration {
    state: Arc<ReliableTcpRequestBulkFlowRegistrationState>,
}

impl ReliableTcpRequestBulkFlowRegistration {
    pub(in crate::runtime) fn update(
        &self,
        request_bulk_active: bool,
        service_underlay: Option<UnderlayProtocol>,
    ) {
        let counted = request_bulk_active && service_underlay == Some(UnderlayProtocol::Tcp);
        let mut current = self
            .state
            .counted
            .lock()
            .expect("TCP-Service request bulk flow registration lock");
        if *current == counted {
            return;
        }
        if counted {
            self.state
                .path_state
                .active_tcp_service_request_bulk_flows
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    count.checked_add(1)
                })
                .expect("TCP-Service request bulk flow count overflow");
        } else {
            self.state
                .path_state
                .active_tcp_service_request_bulk_flows
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    count.checked_sub(1)
                })
                .expect("TCP-Service request bulk flow registration is unbalanced");
        }
        *current = counted;
    }
}

impl Drop for ReliableTcpRequestBulkFlowRegistrationState {
    fn drop(&mut self) {
        let counted = self
            .counted
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *counted {
            self.path_state
                .active_tcp_service_request_bulk_flows
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    count.checked_sub(1)
                })
                .expect("TCP-Service request bulk flow registration is unbalanced");
        }
    }
}

#[derive(Debug)]
pub(in crate::runtime) struct ClientPathHealth {
    pub(in crate::runtime) tcp: Vec<ClientPathHealthRecord>,
    pub(in crate::runtime) udp: Vec<ClientPathHealthRecord>,
}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct ClientPathHealthRecord {
    pub(in crate::runtime) state: SchedulerPathState,
    pub(in crate::runtime) manual_disabled: bool,
    pub(in crate::runtime) consecutive_failures: u32,
    pub(in crate::runtime) measured_srtt_ms: Option<f64>,
    pub(in crate::runtime) measured_jitter_ms: Option<f64>,
    pub(in crate::runtime) measured_rate_bps: Option<f64>,
    pub(in crate::runtime) measured_loss_rate: Option<f64>,
    pub(in crate::runtime) measured_mtu_payload_bytes: Option<usize>,
    pub(in crate::runtime) delivery_samples: u32,
    // Reliable product rate is separate from generic/datagram path goodput.
    pub(in crate::runtime) product_delivery_rate_bps: Option<f64>,
    pub(in crate::runtime) product_delivery_sample_bytes: u64,
    pub(in crate::runtime) datagram_feedback_samples: u32,
    pub(in crate::runtime) last_delivery_at: Option<Instant>,
    pub(in crate::runtime) failed_until: Option<Instant>,
    pub(in crate::runtime) active_flows: u32,
    pub(in crate::runtime) active_latency_sensitive_flows: u32,
    pub(in crate::runtime) relay_bytes_in_flight: u64,
    pub(in crate::runtime) relay_queue_bytes: u64,
    pub(in crate::runtime) carrier_srtt_ms: Option<f64>,
    pub(in crate::runtime) carrier_rttvar_ms: Option<f64>,
    pub(in crate::runtime) carrier_delivery_rate_bps: Option<f64>,
    pub(in crate::runtime) carrier_bytes_in_flight: u64,
    pub(in crate::runtime) carrier_queue_bytes: u64,
    pub(in crate::runtime) carrier_inflight_limit_bytes: u64,
    pub(in crate::runtime) carrier_delivery_samples: u32,
    pub(in crate::runtime) carrier_delivery_sample_bytes: u64,
    pub(in crate::runtime) carrier_last_delivery_at: Option<Instant>,
    pub(in crate::runtime) carrier_app_limited: bool,
    pub(in crate::runtime) carrier_ack_derived_data_seen: bool,
    pub(in crate::runtime::path) tcp_capacity: RequestTcpCapacityRecord,
    pub(in crate::runtime::path) quic_capacity: RequestQuicCapacityRecord,
    pub(in crate::runtime) path_proof_success: bool,
    path_proof_generation: u64,
    path_proof_valid_after: Instant,
    successful_path_proofs: HashMap<u64, SuccessfulPathProof>,
    successful_path_proof_order: VecDeque<u64>,
    successful_path_proof_limit: usize,
}

#[derive(Debug, Clone, Copy)]
struct RequestQuicCapacityReconciliationObservation {
    target: RelayPathInstance,
    token: u64,
    carrier_proven: bool,
    handoff: RequestQuicCapacityProductHandoffState,
}

/// One lock-coherent carrier-authority view for request reconciliation.
/// Controllers consume only exact transaction identities from this snapshot.
pub(in crate::runtime) struct RequestCapacityReconciliationView {
    observed_at: Instant,
    tcp_proofs: HashMap<RelayPathInstance, TcpCapacityProofCandidate>,
    quic: Option<RequestQuicCapacityReconciliationObservation>,
}

impl RequestCapacityReconciliationView {
    pub(in crate::runtime) fn observed_at(&self) -> Instant {
        self.observed_at
    }

    pub(in crate::runtime) fn tcp_proof(
        &self,
        target: RelayPathInstance,
    ) -> Option<TcpCapacityProofCandidate> {
        self.tcp_proofs.get(&target).copied()
    }

    pub(in crate::runtime) fn quic_carrier_proven(
        &self,
        target: RelayPathInstance,
        token: u64,
    ) -> bool {
        self.quic.is_some_and(|observation| {
            observation.target == target && observation.token == token && observation.carrier_proven
        })
    }

    pub(in crate::runtime) fn quic_handoff_state(
        &self,
        target: RelayPathInstance,
        token: u64,
    ) -> RequestQuicCapacityProductHandoffState {
        self.quic
            .filter(|observation| observation.target == target && observation.token == token)
            .map_or(
                RequestQuicCapacityProductHandoffState::Absent,
                |observation| observation.handoff,
            )
    }
}

#[derive(Debug, Clone, Copy)]
struct SuccessfulPathProof {
    proof_id: u64,
    sent_at: Instant,
    acked_at: Instant,
}

impl Default for ClientPathHealthRecord {
    fn default() -> Self {
        Self {
            state: SchedulerPathState::Active,
            manual_disabled: false,
            consecutive_failures: 0,
            measured_srtt_ms: None,
            measured_jitter_ms: None,
            measured_rate_bps: None,
            measured_loss_rate: None,
            measured_mtu_payload_bytes: None,
            delivery_samples: 0,
            product_delivery_rate_bps: None,
            product_delivery_sample_bytes: 0,
            datagram_feedback_samples: 0,
            last_delivery_at: None,
            failed_until: None,
            active_flows: 0,
            active_latency_sensitive_flows: 0,
            relay_bytes_in_flight: 0,
            relay_queue_bytes: 0,
            carrier_srtt_ms: None,
            carrier_rttvar_ms: None,
            carrier_delivery_rate_bps: None,
            carrier_bytes_in_flight: 0,
            carrier_queue_bytes: 0,
            carrier_inflight_limit_bytes: 0,
            carrier_delivery_samples: 0,
            carrier_delivery_sample_bytes: 0,
            carrier_last_delivery_at: None,
            carrier_app_limited: true,
            carrier_ack_derived_data_seen: false,
            tcp_capacity: RequestTcpCapacityRecord::default(),
            quic_capacity: RequestQuicCapacityRecord::default(),
            path_proof_success: false,
            path_proof_generation: 0,
            path_proof_valid_after: Instant::now(),
            successful_path_proofs: HashMap::new(),
            successful_path_proof_order: VecDeque::new(),
            successful_path_proof_limit: 1,
        }
    }
}

impl ClientPathHealthRecord {
    pub(super) fn with_path_proof_limit(limit: usize) -> Self {
        Self {
            successful_path_proof_limit: limit.max(1),
            ..Self::default()
        }
    }

    pub(super) fn path_proof_generation(&self) -> u64 {
        self.path_proof_generation
    }

    pub(super) fn successful_path_proof_acked_at(
        &self,
        proof_id: u64,
        attached_at: Instant,
        now: Instant,
    ) -> Option<Instant> {
        self.successful_path_proofs
            .get(&proof_id)
            .filter(|proof| {
                proof.proof_id == proof_id && proof.sent_at >= attached_at && proof.acked_at <= now
            })
            .map(|proof| proof.acked_at)
    }

    pub(in crate::runtime) fn mark_tcp_transport_state(
        &mut self,
        observation: TcpNativeObservation,
    ) {
        if self.manual_disabled {
            return;
        }
        self.mark_liveness_success();
        // Same-socket native evidence updates only capabilities the host exposed.
        // Delivery rate stays inside typed proofs and product ACK authority.
        if let Some((srtt_us, rttvar_us)) = observation.rtt() {
            self.carrier_srtt_ms = Some(f64::from(srtt_us.max(1)) / 1_000.0);
            self.carrier_rttvar_ms = Some(f64::from(rttvar_us) / 1_000.0);
        }
        if let Some((bytes_in_flight, inflight_limit_bytes, _)) = observation.flight() {
            self.carrier_bytes_in_flight = bytes_in_flight;
            self.carrier_inflight_limit_bytes = inflight_limit_bytes;
        }
        if let Some(queue_bytes) = observation.queue_bytes() {
            self.carrier_queue_bytes = queue_bytes;
        }
        if let Some(loss_ppm) = observation.loss_ppm() {
            self.measured_loss_rate = Some(f64::from(loss_ppm) / 1_000_000.0);
        }
    }

    fn has_durable_native_carrier_window(&self) -> bool {
        self.carrier_delivery_rate_bps.is_some()
            && self.carrier_ack_derived_data_seen
            && self.carrier_delivery_samples > 0
            && !self.carrier_app_limited
            && self.carrier_delivery_sample_bytes
                >= self
                    .carrier_inflight_limit_bytes
                    .max(BBR_MAX_SEND_QUANTUM_BYTES as u64)
                    .max(PATH_OPEN_SCORE_BYTES as u64)
    }
}

impl ClientPathHealthRecord {
    /// Applies time-driven lifecycle transitions. Keeping this separate from
    /// observation prevents diagnostics and ranking reads from canceling work.
    pub(in crate::runtime) fn maintain(&mut self, now: Instant) {
        self.tcp_capacity.maintain(now);
        self.quic_capacity.maintain(now);
        if self.state == SchedulerPathState::Failed
            && self.failed_until.is_some_and(|deadline| now >= deadline)
        {
            self.state = SchedulerPathState::Suspect;
            self.failed_until = None;
        }
    }

    /// Returns a time-indexed immutable view. Deadline projection keeps a pure
    /// diagnostic view truthful even when its owner has not run maintenance yet.
    pub(in crate::runtime) fn observation_at(&self, now: Instant) -> ClientPathObservation {
        if self.manual_disabled {
            return ClientPathObservation {
                state: SchedulerPathState::Failed,
                manual_disabled: true,
                measured_srtt_ms: self.measured_srtt_ms,
                measured_jitter_ms: self.measured_jitter_ms,
                measured_rate_bps: self.measured_rate_bps,
                measured_loss_rate: self.measured_loss_rate,
                measured_mtu_payload_bytes: self.measured_mtu_payload_bytes,
                delivery_samples: self.delivery_samples,
                product_delivery_rate_bps: self.product_delivery_rate_bps,
                product_delivery_sample_bytes: self.product_delivery_sample_bytes,
                datagram_feedback_samples: self.datagram_feedback_samples,
                last_delivery_at: self.last_delivery_at,
                active_flows: self.active_flows,
                active_latency_sensitive_flows: self.active_latency_sensitive_flows,
                relay_bytes_in_flight: self.relay_bytes_in_flight,
                relay_queue_bytes: self.relay_queue_bytes,
                carrier_srtt_ms: self.carrier_srtt_ms,
                carrier_rttvar_ms: self.carrier_rttvar_ms,
                carrier_delivery_rate_bps: self.carrier_delivery_rate_bps,
                carrier_bytes_in_flight: self.carrier_bytes_in_flight,
                carrier_queue_bytes: self.carrier_queue_bytes,
                carrier_inflight_limit_bytes: self.carrier_inflight_limit_bytes,
                carrier_delivery_samples: self.carrier_delivery_samples,
                carrier_delivery_sample_bytes: self.carrier_delivery_sample_bytes,
                carrier_last_delivery_at: self.carrier_last_delivery_at,
                carrier_app_limited: self.carrier_app_limited,
                carrier_ack_derived_data_seen: self.carrier_ack_derived_data_seen,
                explicit_carrier_capacity_proof: false,
                quic_capacity_product_handoff_complete: false,
                quic_capacity_rate_prior_fresh: false,
                path_proof_success: self.path_proof_success,
            };
        }
        let state = if self.state == SchedulerPathState::Failed
            && self.failed_until.is_some_and(|deadline| now >= deadline)
        {
            SchedulerPathState::Suspect
        } else {
            self.state
        };
        let tcp_proof = self.tcp_capacity.proof_candidate_at(now);
        let quic_capacity = self
            .quic_capacity
            .observation_at(now, self.has_durable_native_carrier_window());
        let quic_proof = quic_capacity.proof;
        let handoff_capacity_prior = quic_capacity.handoff_prior;
        let proof_rate_bps = tcp_proof
            .map(|proof| proof.rate_bps as f64)
            .or_else(|| quic_proof.map(|proof| proof.rate_bps as f64));
        let proof_sample_bytes = tcp_proof
            .map(|proof| proof.rate_sample_bytes)
            .or_else(|| quic_proof.map(|proof| proof.rate_sample_bytes));
        let proof_accepted_at = tcp_proof
            .map(|proof| proof.accepted_at)
            .or_else(|| quic_proof.map(|proof| proof.accepted_at));
        let explicit_carrier_capacity_proof = proof_rate_bps.is_some();
        ClientPathObservation {
            state,
            manual_disabled: false,
            measured_srtt_ms: self.measured_srtt_ms,
            measured_jitter_ms: self.measured_jitter_ms,
            measured_rate_bps: self.measured_rate_bps,
            measured_loss_rate: self.measured_loss_rate,
            measured_mtu_payload_bytes: self.measured_mtu_payload_bytes,
            delivery_samples: self.delivery_samples,
            product_delivery_rate_bps: self.product_delivery_rate_bps,
            product_delivery_sample_bytes: self.product_delivery_sample_bytes,
            datagram_feedback_samples: self.datagram_feedback_samples,
            last_delivery_at: self.last_delivery_at,
            active_flows: self.active_flows,
            active_latency_sensitive_flows: self.active_latency_sensitive_flows,
            relay_bytes_in_flight: self.relay_bytes_in_flight,
            relay_queue_bytes: self.relay_queue_bytes,
            carrier_srtt_ms: self.carrier_srtt_ms,
            carrier_rttvar_ms: self.carrier_rttvar_ms,
            carrier_delivery_rate_bps: proof_rate_bps
                .or_else(|| handoff_capacity_prior.map(|handoff| handoff.rate_bps as f64))
                .or(self.carrier_delivery_rate_bps),
            carrier_bytes_in_flight: self.carrier_bytes_in_flight,
            carrier_queue_bytes: self.carrier_queue_bytes,
            carrier_inflight_limit_bytes: self.carrier_inflight_limit_bytes,
            carrier_delivery_samples: if explicit_carrier_capacity_proof
                || handoff_capacity_prior.is_some()
            {
                self.carrier_delivery_samples.max(1)
            } else {
                self.carrier_delivery_samples
            },
            carrier_delivery_sample_bytes: proof_sample_bytes
                .or_else(|| handoff_capacity_prior.map(|handoff| handoff.rate_sample_bytes))
                .map_or(self.carrier_delivery_sample_bytes, |sample_bytes| {
                    self.carrier_delivery_sample_bytes.max(sample_bytes)
                }),
            carrier_last_delivery_at: proof_accepted_at
                .or_else(|| handoff_capacity_prior.map(|handoff| handoff.accepted_at))
                .or(self.carrier_last_delivery_at),
            carrier_app_limited: !explicit_carrier_capacity_proof
                && handoff_capacity_prior.is_none()
                && self.carrier_app_limited,
            carrier_ack_derived_data_seen: explicit_carrier_capacity_proof
                || handoff_capacity_prior.is_some()
                || self.carrier_ack_derived_data_seen,
            explicit_carrier_capacity_proof,
            quic_capacity_product_handoff_complete: quic_capacity.handoff_complete,
            quic_capacity_rate_prior_fresh: handoff_capacity_prior.is_some(),
            path_proof_success: self.path_proof_success,
        }
    }

    pub(in crate::runtime) fn mark_success(&mut self, elapsed: Duration) {
        if self.manual_disabled {
            return;
        }
        self.mark_liveness_success();
        let sample_ms = elapsed.as_secs_f64() * 1000.0;
        self.measured_srtt_ms = Some(match self.measured_srtt_ms {
            Some(previous) => previous.mul_add(0.875, sample_ms * 0.125),
            None => sample_ms,
        });
    }

    pub(in crate::runtime) fn mark_path_proof_success(
        &mut self,
        observation: PathProofObservation,
    ) {
        if self.manual_disabled || observation.sent_at < self.path_proof_valid_after {
            return;
        }
        self.mark_success(observation.elapsed);
        self.path_proof_success = true;
        let proof = SuccessfulPathProof {
            proof_id: observation.proof_id,
            sent_at: observation.sent_at,
            acked_at: Instant::now(),
        };
        if self
            .successful_path_proofs
            .insert(observation.proof_id, proof)
            .is_none()
        {
            self.successful_path_proof_order
                .push_back(observation.proof_id);
        }
        while self.successful_path_proofs.len() > self.successful_path_proof_limit {
            if let Some(proof_id) = self.successful_path_proof_order.pop_front() {
                self.successful_path_proofs.remove(&proof_id);
            }
        }
    }

    pub(in crate::runtime) fn invalidate_path_proofs(&mut self) {
        self.path_proof_success = false;
        self.successful_path_proofs.clear();
        self.successful_path_proof_order.clear();
        self.path_proof_generation = self.path_proof_generation.wrapping_add(1);
        self.path_proof_valid_after = Instant::now();
    }

    pub(in crate::runtime) fn mark_liveness_success(&mut self) {
        if self.manual_disabled {
            return;
        }
        self.state = SchedulerPathState::Active;
        self.consecutive_failures = 0;
        self.failed_until = None;
    }

    pub(in crate::runtime) fn mark_open_success(&mut self, _elapsed: Duration, lane: FlowLane) {
        self.mark_liveness_success();
        self.active_flows = self.active_flows.saturating_add(1);
        if lane.is_latency_sensitive() {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
    }

    pub(in crate::runtime) fn reserve_load(&mut self, lane: FlowLane) {
        self.active_flows = self.active_flows.saturating_add(1);
        if lane.is_latency_sensitive() {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
    }

    pub(in crate::runtime) fn mark_reserved_open_success(&mut self, _elapsed: Duration) {
        self.mark_liveness_success();
    }

    pub(in crate::runtime) fn release_load(&mut self, lane: FlowLane) {
        self.active_flows = self.active_flows.saturating_sub(1);
        if lane.is_latency_sensitive() {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_sub(1);
        }
    }

    pub(in crate::runtime) fn change_lane_load(&mut self, from: FlowLane, to: FlowLane) {
        if from.is_latency_sensitive() && !to.is_latency_sensitive() {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_sub(1);
        } else if !from.is_latency_sensitive() && to.is_latency_sensitive() {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
    }

    pub(in crate::runtime) fn mark_delivery(&mut self, sample: PathRateSample) {
        if self.manual_disabled {
            return;
        }
        self.state = SchedulerPathState::Active;
        self.consecutive_failures = 0;
        self.failed_until = None;
        self.delivery_samples = self.delivery_samples.saturating_add(1);
        self.last_delivery_at = Some(Instant::now());
        let sample_bps = sample.rate_bps();
        self.measured_rate_bps = Some(match self.measured_rate_bps {
            Some(previous) => previous.mul_add(0.75, sample_bps * 0.25),
            None => sample_bps,
        });
    }

    pub(in crate::runtime) fn mark_product_delivery(&mut self, sample: PathRateSample) {
        if self.manual_disabled {
            return;
        }
        let sample_bps = sample.rate_bps();
        self.product_delivery_rate_bps = Some(match self.product_delivery_rate_bps {
            Some(previous) => previous.mul_add(0.75, sample_bps * 0.25),
            None => sample_bps,
        });
        self.product_delivery_sample_bytes = self
            .product_delivery_sample_bytes
            .saturating_add(sample.bytes());
        self.mark_delivery(sample);
    }

    pub(in crate::runtime) fn mark_product_delivery_replacing_rate(
        &mut self,
        sample: PathRateSample,
    ) {
        if self.manual_disabled {
            return;
        }
        self.product_delivery_sample_bytes = self
            .product_delivery_sample_bytes
            .saturating_add(sample.bytes());
        self.product_delivery_rate_bps = Some(sample.rate_bps());
        self.state = SchedulerPathState::Active;
        self.consecutive_failures = 0;
        self.failed_until = None;
        self.delivery_samples = self.delivery_samples.saturating_add(1);
        self.last_delivery_at = Some(Instant::now());
        self.measured_rate_bps = Some(sample.rate_bps());
    }

    pub(in crate::runtime) fn mark_udp_datagram_feedback(
        &mut self,
        observation: UdpDatagramPathObservation,
    ) {
        self.mark_success(observation.rtt);
        if let Some(sample) = observation.rate_sample {
            self.mark_delivery(sample);
            self.datagram_feedback_samples = self.datagram_feedback_samples.saturating_add(1);
        }
        let sample_jitter_ms = observation.jitter.as_secs_f64() * 1000.0;
        self.measured_jitter_ms = Some(match self.measured_jitter_ms {
            Some(previous) => previous.mul_add(0.875, sample_jitter_ms * 0.125),
            None => sample_jitter_ms,
        });
        self.measured_loss_rate = Some(match self.measured_loss_rate {
            Some(previous) => previous.mul_add(0.875, observation.loss_rate * 0.125),
            None => observation.loss_rate,
        });
    }

    pub(in crate::runtime) fn mark_udp_mtu(&mut self, payload_bytes: usize) {
        self.measured_mtu_payload_bytes = Some(payload_bytes);
    }

    pub(in crate::runtime) fn mark_quic_path_metrics(&mut self, metrics: UdpPathMetrics) {
        if self.manual_disabled {
            return;
        }
        self.state = SchedulerPathState::Active;
        self.consecutive_failures = 0;
        self.failed_until = None;
        if metrics.min_rtt_observed {
            self.carrier_srtt_ms = Some(metrics.srtt.as_secs_f64() * 1000.0);
            self.carrier_rttvar_ms = Some(metrics.rttvar.as_secs_f64() * 1000.0);
        }
        self.carrier_delivery_rate_bps =
            (metrics.delivery_sample_count > 0).then_some(metrics.delivery_rate_bps.max(1.0));
        self.carrier_delivery_samples =
            u32::try_from(metrics.delivery_sample_count).unwrap_or(u32::MAX);
        self.carrier_delivery_sample_bytes = metrics.delivery_sample_bytes;
        self.carrier_last_delivery_at = metrics.last_delivery_sample_at;
        if metrics.ack_derived_data_seen {
            self.carrier_ack_derived_data_seen = true;
        }
        self.carrier_bytes_in_flight = metrics.bytes_in_flight as u64;
        self.carrier_queue_bytes = metrics
            .pending_bytes
            .saturating_sub(metrics.bytes_in_flight) as u64;
        self.carrier_inflight_limit_bytes = metrics.inflight_hi as u64;
        self.carrier_app_limited = metrics.app_limited;
    }

    pub(in crate::runtime) fn mark_failure(
        &mut self,
        now: Instant,
        has_schedulable_alternative: bool,
    ) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.relay_bytes_in_flight = 0;
        self.relay_queue_bytes = 0;
        self.invalidate_path_proofs();
        if self.consecutive_failures == 1 || !has_schedulable_alternative {
            self.state = SchedulerPathState::Suspect;
            self.failed_until = None;
        } else {
            self.state = SchedulerPathState::Failed;
            self.failed_until = Some(now + path_record_failure_cooldown(self));
        }
    }

    pub(in crate::runtime) fn mark_data_plane_failure(
        &mut self,
        now: Instant,
        has_schedulable_alternative: bool,
    ) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.relay_bytes_in_flight = 0;
        self.relay_queue_bytes = 0;
        // Product and native carrier evidence belongs to the failed association.
        self.product_delivery_rate_bps = None;
        self.product_delivery_sample_bytes = 0;
        self.carrier_delivery_rate_bps = None;
        self.carrier_bytes_in_flight = 0;
        self.carrier_queue_bytes = 0;
        self.carrier_inflight_limit_bytes = 0;
        self.carrier_delivery_samples = 0;
        self.carrier_delivery_sample_bytes = 0;
        self.carrier_last_delivery_at = None;
        self.carrier_app_limited = true;
        self.carrier_ack_derived_data_seen = false;
        self.tcp_capacity.reset_after_data_plane_failure();
        self.quic_capacity.reset_after_data_plane_failure();
        self.invalidate_path_proofs();
        if has_schedulable_alternative {
            self.state = SchedulerPathState::Failed;
            self.failed_until = Some(now + path_record_failure_cooldown(self));
        } else {
            self.state = SchedulerPathState::Suspect;
            self.failed_until = None;
        }
    }

    pub(in crate::runtime) fn record_relay_send(&mut self, bytes: usize) {
        self.relay_bytes_in_flight = self.relay_bytes_in_flight.saturating_add(bytes as u64);
    }

    pub(in crate::runtime) fn release_relay_inflight(&mut self, bytes: usize) {
        self.relay_bytes_in_flight = self.relay_bytes_in_flight.saturating_sub(bytes as u64);
    }
}

impl ClientPathContext {
    pub(in crate::runtime) fn health(&self) -> &Mutex<ClientPathHealth> {
        self.state.health()
    }

    pub(in crate::runtime) fn retire_request_quic_capacity_probe_token(&self, token: u64) {
        self.state.retire_request_quic_capacity_probe_token(token);
    }

    pub(in crate::runtime) fn request_quic_capacity_probe_remaining_bytes(&self) -> u64 {
        self.state.request_quic_capacity_probe_remaining_bytes(
            reliable_capacity_calibration_session_limit_bytes(self.mux_limits),
        )
    }

    pub(in crate::runtime) fn request_tcp_capacity_probe_remaining_bytes(&self) -> u64 {
        self.state.request_tcp_capacity_probe_remaining_bytes(
            reliable_capacity_calibration_session_limit_bytes(self.mux_limits),
        )
    }

    pub(in crate::runtime) fn request_quic_capacity_probe_candidate_share_bytes(
        &self,
        proposed_path_limit: u64,
    ) -> u64 {
        self.state
            .request_quic_capacity_probe_candidate_share_bytes(
                proposed_path_limit,
                reliable_capacity_calibration_session_limit_bytes(self.mux_limits),
            )
    }

    pub(in crate::runtime) fn request_tcp_capacity_probe_candidate_share_bytes(
        &self,
        proposed_path_limit: u64,
    ) -> u64 {
        self.state.request_tcp_capacity_probe_candidate_share_bytes(
            proposed_path_limit,
            reliable_capacity_calibration_session_limit_bytes(self.mux_limits),
        )
    }

    pub(in crate::runtime) fn request_quic_capacity_probe_path_remaining_bytes(
        &self,
        path_index: usize,
        path_limit: u64,
    ) -> u64 {
        self.state.request_quic_capacity_probe_path_remaining_bytes(
            path_index,
            path_limit,
            reliable_capacity_calibration_session_limit_bytes(self.mux_limits),
        )
    }

    pub(in crate::runtime) fn request_tcp_capacity_probe_path_remaining_bytes(
        &self,
        path_index: usize,
        path_limit: u64,
    ) -> u64 {
        self.state.request_tcp_capacity_probe_path_remaining_bytes(
            path_index,
            path_limit,
            reliable_capacity_calibration_session_limit_bytes(self.mux_limits),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::runtime) fn try_reserve_request_tcp_capacity_probe(
        &self,
        stream_id: StreamId,
        path_index: usize,
        path_instance: RelayPathInstance,
        token: u64,
        train_bytes: u64,
        path_limit_bytes: u64,
        campaign: Arc<RequestCapacityProbeCampaignBudget>,
        required_timed_bytes: u64,
        valid_after: Instant,
        expires_at: Instant,
        ticket: CapacityProbeCommandTicket,
    ) -> Option<RequestTcpCapacityProbeLease> {
        self.state.try_reserve_request_tcp_capacity_probe(
            stream_id,
            path_index,
            path_instance,
            token,
            train_bytes,
            path_limit_bytes,
            reliable_capacity_calibration_session_limit_bytes(self.mux_limits),
            campaign,
            required_timed_bytes,
            valid_after,
            expires_at,
            ticket,
        )
    }

    pub(in crate::runtime) fn request_capacity_reconciliation_view(
        &self,
        stream_id: StreamId,
        tcp_queries: impl Iterator<Item = RequestTcpCapacityProofQuery>,
        quic_query: Option<RequestQuicCapacityReconciliationQuery>,
        now: Instant,
    ) -> RequestCapacityReconciliationView {
        let mut tcp_queries = tcp_queries.peekable();
        if tcp_queries.peek().is_none() && quic_query.is_none() {
            return RequestCapacityReconciliationView {
                observed_at: now,
                tcp_proofs: HashMap::new(),
                quic: None,
            };
        }
        let health = self.state.health.lock().expect("client path health lock");
        let tcp_proofs = tcp_queries
            .filter_map(|query| {
                health
                    .tcp
                    .get(query.target.key.index)
                    .and_then(|record| {
                        record.tcp_capacity.exact_proof_candidate_at(
                            stream_id,
                            query.target,
                            query.token,
                            now,
                        )
                    })
                    .map(|proof| (query.target, proof))
            })
            .collect();
        let quic = quic_query.map(|query| {
            let reconciliation = health.udp.get(query.target.key.index).map(|record| {
                record
                    .quic_capacity
                    .reconciliation_at(stream_id, query.target, query.token, now)
            });
            RequestQuicCapacityReconciliationObservation {
                target: query.target,
                token: query.token,
                carrier_proven: reconciliation.is_some_and(|view| view.carrier_proven),
                handoff: reconciliation
                    .map_or(RequestQuicCapacityProductHandoffState::Absent, |view| {
                        view.handoff
                    }),
            }
        });
        RequestCapacityReconciliationView {
            observed_at: now,
            tcp_proofs,
            quic,
        }
    }

    pub(in crate::runtime) fn try_reserve_request_quic_capacity_probe(
        &self,
        stream_id: StreamId,
        path_index: usize,
        path_instance: RelayPathInstance,
        token: u64,
        train_bytes: u64,
        path_limit_bytes: u64,
        campaign: Arc<RequestCapacityProbeCampaignBudget>,
        valid_after: Instant,
        expires_at: Instant,
        proof_validity: Duration,
        ticket: CapacityProbeCommandTicket,
    ) -> Option<RequestQuicCapacityProbeLease> {
        self.state.try_reserve_request_quic_capacity_probe(
            stream_id,
            path_index,
            path_instance,
            token,
            train_bytes,
            path_limit_bytes,
            reliable_capacity_calibration_session_limit_bytes(self.mux_limits),
            campaign,
            valid_after,
            expires_at,
            proof_validity,
            ticket,
        )
    }

    #[cfg(test)]
    pub(in crate::runtime) fn request_quic_capacity_probe_proven_at(
        &self,
        path_index: usize,
        token: u64,
        now: Instant,
    ) -> bool {
        self.state
            .request_quic_capacity_probe_proven_at(path_index, token, now)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn request_quic_capacity_product_handoff_state_at(
        &self,
        path_index: usize,
        token: u64,
        now: Instant,
    ) -> RequestQuicCapacityProductHandoffState {
        self.state
            .request_quic_capacity_product_handoff_state_at(path_index, token, now)
    }

    pub(in crate::runtime) fn allocate_reliable_stream_id(&self) -> Result<StreamId, RuntimeError> {
        let mut next = self
            .state
            .next_reliable_stream_id
            .lock()
            .expect("client reliable stream ID lock");
        let stream_id = StreamId(*next);
        *next = next
            .checked_add(1)
            .ok_or(RuntimeError::Protocol("reliable stream ID overflow"))?;
        Ok(stream_id)
    }

    pub(in crate::runtime) fn reliable_tcp_request_bulk_flow_registration(
        &self,
    ) -> ReliableTcpRequestBulkFlowRegistration {
        ReliableTcpRequestBulkFlowRegistration {
            state: Arc::new(ReliableTcpRequestBulkFlowRegistrationState {
                path_state: self.state.clone(),
                counted: Mutex::new(false),
            }),
        }
    }

    pub(in crate::runtime) fn active_tcp_service_request_bulk_flows(&self) -> u32 {
        self.state
            .active_tcp_service_request_bulk_flows
            .load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn reserve_tcp_path_load(&self, index: usize, lane: FlowLane) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.reserve_load(lane);
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn reserve_udp_stream_path_load(&self, index: usize, lane: FlowLane) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.reserve_load(lane);
        }
    }

    /// Returns the only owner of the newly published scheduler load.
    pub(in crate::runtime) fn reserve_relay_path_load(
        &self,
        key: RelayPathKey,
        lane: FlowLane,
    ) -> Option<RelayPathLoadLease> {
        let mut health = self.state.health.lock().expect("client path health lock");
        let records = match key.underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        records.get_mut(key.index)?.reserve_load(lane);
        drop(health);
        Some(RelayPathLoadLease::new(self.state.clone(), key, lane))
    }

    pub(in crate::runtime) fn mark_tcp_path_open_success(
        &self,
        index: usize,
        elapsed: Duration,
        lane: FlowLane,
    ) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.mark_open_success(elapsed, lane);
        }
    }

    pub(in crate::runtime) fn mark_tcp_path_reserved_open_success(
        &self,
        index: usize,
        elapsed: Duration,
    ) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.mark_reserved_open_success(elapsed);
        }
    }

    pub(in crate::runtime) fn mark_tcp_path_probe_success(&self, index: usize, elapsed: Duration) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.mark_success(elapsed);
        }
    }

    pub(in crate::runtime) fn should_probe_tcp_path(&self, index: usize) -> bool {
        let now = Instant::now();
        self.state
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
            .is_some_and(|record| {
                record.maintain(now);
                path_observation_is_idle_for_probe(record.observation_at(now))
            })
    }

    pub(in crate::runtime) fn release_tcp_path_load(&self, index: usize, lane: FlowLane) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.release_load(lane);
        }
    }

    pub(in crate::runtime) fn mark_udp_stream_reserved_open_success(
        &self,
        index: usize,
        elapsed: Duration,
        accepted: bool,
    ) {
        if !accepted {
            return;
        }
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_reserved_open_success(elapsed);
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn release_udp_stream_path_load(&self, index: usize, lane: FlowLane) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.release_load(lane);
        }
    }

    pub(in crate::runtime) fn mark_relay_path_failure(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
    ) {
        match underlay {
            UnderlayProtocol::Tcp => self.mark_tcp_path_failure(index),
            UnderlayProtocol::Udp => self.mark_udp_path_failure(index),
        }
    }

    pub(in crate::runtime) fn mark_relay_path_data_plane_failure(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
    ) {
        match underlay {
            UnderlayProtocol::Tcp => self.mark_tcp_path_data_plane_failure(index),
            UnderlayProtocol::Udp => self.mark_udp_path_data_plane_failure(index),
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn release_relay_path_load(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        lane: FlowLane,
    ) {
        self.state
            .release_relay_path_load(RelayPathKey { underlay, index }, lane);
    }

    pub(in crate::runtime) fn record_relay_path_send(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        bytes: usize,
    ) {
        if bytes == 0 {
            return;
        }
        let mut health = self.state.health.lock().expect("client path health lock");
        let records = match underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        if let Some(current) = records.get_mut(index) {
            current.record_relay_send(bytes);
        }
    }

    pub(in crate::runtime) fn release_relay_path_inflight(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        bytes: usize,
    ) {
        if bytes == 0 {
            return;
        }
        let mut health = self.state.health.lock().expect("client path health lock");
        let records = match underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        if let Some(current) = records.get_mut(index) {
            current.release_relay_inflight(bytes);
        }
    }

    pub(in crate::runtime) fn record_relay_path_product_ack(
        &self,
        stream_id: StreamId,
        path_instance: RelayPathInstance,
        bytes: usize,
        sent_at: Instant,
        acked_at: Instant,
    ) {
        if bytes == 0 || path_instance.key.underlay != UnderlayProtocol::Udp {
            return;
        }
        self.state.record_request_quic_capacity_product_ack(
            stream_id,
            path_instance,
            bytes,
            sent_at,
            acked_at,
        );
    }

    pub(in crate::runtime) fn change_relay_path_lane_load(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        from: FlowLane,
        to: FlowLane,
    ) {
        if from == to {
            return;
        }
        let mut health = self.state.health.lock().expect("client path health lock");
        match underlay {
            UnderlayProtocol::Tcp => {
                if let Some(current) = health.tcp.get_mut(index) {
                    current.change_lane_load(from, to);
                }
            }
            UnderlayProtocol::Udp => {
                if let Some(current) = health.udp.get_mut(index) {
                    current.change_lane_load(from, to);
                }
            }
        }
    }

    pub(in crate::runtime) fn mark_relay_path_delivery(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        stats: PathDeliveryStats,
    ) {
        match underlay {
            UnderlayProtocol::Tcp => self.mark_tcp_path_delivery(index, stats),
            UnderlayProtocol::Udp => self.mark_udp_reliable_path_delivery(index, stats),
        }
    }

    pub(in crate::runtime) fn mark_relay_path_rate_sample(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        sample: PathRateSample,
    ) {
        let mut health = self.state.health.lock().expect("client path health lock");
        let records = match underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        if let Some(current) = records.get_mut(index) {
            current.mark_product_delivery(sample);
        }
    }

    pub(in crate::runtime) fn mark_relay_path_ack_clock_rate_sample(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        sample: PathRateSample,
        replace_startup_rate: bool,
    ) {
        let mut health = self.state.health.lock().expect("client path health lock");
        let records = match underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        if let Some(current) = records.get_mut(index) {
            if replace_startup_rate {
                current.mark_product_delivery_replacing_rate(sample);
            } else {
                current.mark_product_delivery(sample);
            }
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_relay_path_proof_observation(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        observation: PathProofObservation,
    ) {
        let mut health = self.state.health.lock().expect("client path health lock");
        let records = match underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        if let Some(current) = records.get_mut(index) {
            current.mark_path_proof_success(observation);
        }
    }

    pub(in crate::runtime) fn mark_tcp_path_delivery(
        &self,
        index: usize,
        stats: PathDeliveryStats,
    ) {
        let Some(sample) = stats.rate_sample() else {
            return;
        };
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.mark_product_delivery(sample);
        }
    }

    pub(in crate::runtime) fn mark_tcp_path_failure(&self, index: usize) {
        let now = Instant::now();
        let mut health = self.state.health.lock().expect("client path health lock");
        let has_schedulable_alternative =
            path_records_have_schedulable_alternative(&mut health.tcp, index, now);
        if let Some(current) = health.tcp.get_mut(index) {
            current.mark_failure(now, has_schedulable_alternative);
        }
    }

    pub(in crate::runtime) fn mark_tcp_path_data_plane_failure(&self, index: usize) {
        let now = Instant::now();
        let mut health = self.state.health.lock().expect("client path health lock");
        let has_schedulable_alternative =
            path_records_have_schedulable_alternative(&mut health.tcp, index, now);
        if let Some(current) = health.tcp.get_mut(index) {
            current.mark_data_plane_failure(now, has_schedulable_alternative);
        }
    }

    pub(in crate::runtime) fn mark_udp_path_open_success(&self, index: usize, elapsed: Duration) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_open_success(elapsed, FlowLane::RealtimeDatagram);
        }
    }

    pub(in crate::runtime) fn mark_udp_path_probe_success(&self, index: usize, elapsed: Duration) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_success(elapsed);
        }
    }

    pub(in crate::runtime) fn should_probe_udp_path(&self, index: usize) -> bool {
        let now = Instant::now();
        self.state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
            .is_some_and(|record| {
                record.maintain(now);
                path_observation_is_idle_for_probe(record.observation_at(now))
            })
    }

    pub(in crate::runtime) fn release_udp_path_load(&self, index: usize) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.release_load(FlowLane::RealtimeDatagram);
        }
    }

    fn mark_udp_reliable_path_delivery(&self, index: usize, stats: PathDeliveryStats) {
        let Some(sample) = stats.rate_sample() else {
            return;
        };
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_product_delivery(sample);
        }
    }

    pub(in crate::runtime) fn mark_udp_datagram_path_delivery(
        &self,
        index: usize,
        stats: PathDeliveryStats,
    ) {
        let Some(sample) = stats.rate_sample() else {
            return;
        };
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            // Datagram goodput ranks datagram paths but never proves reliable
            // product ownership or unlocks ordered-stream overlap.
            current.mark_delivery(sample);
        }
    }

    pub(in crate::runtime) fn mark_udp_path_feedback(
        &self,
        index: usize,
        observation: UdpDatagramPathObservation,
    ) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_udp_datagram_feedback(observation);
        }
    }

    pub(in crate::runtime) fn mark_udp_path_mtu(&self, index: usize, payload_bytes: usize) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_udp_mtu(payload_bytes);
        }
    }

    pub(in crate::runtime) fn mark_udp_path_failure(&self, index: usize) {
        let now = Instant::now();
        let mut health = self.state.health.lock().expect("client path health lock");
        let has_schedulable_alternative =
            path_records_have_schedulable_alternative(&mut health.udp, index, now);
        if let Some(current) = health.udp.get_mut(index) {
            current.mark_failure(now, has_schedulable_alternative);
        }
    }

    pub(in crate::runtime) fn mark_udp_path_data_plane_failure(&self, index: usize) {
        let now = Instant::now();
        let mut health = self.state.health.lock().expect("client path health lock");
        let has_schedulable_alternative =
            path_records_have_schedulable_alternative(&mut health.udp, index, now);
        if let Some(current) = health.udp.get_mut(index) {
            current.mark_data_plane_failure(now, has_schedulable_alternative);
        }
    }
}

#[cfg(test)]
#[path = "state_test.rs"]
mod tests;
