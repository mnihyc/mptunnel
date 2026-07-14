//! Mutable client-path evidence and capacity transactions.
//!
//! Reservations, proof publication, and lease rollback share this owner so
//! senders and carriers cannot observe a partially committed probe.

use super::commands::*;
use super::model::*;
use super::proof::PathProofObservation;
use super::quic::metrics::{QuicCapacityProofCandidate, UdpPathMetrics};
use super::set::ClientPathContext;
use super::tcp::metrics::*;
use crate::model::capacity::*;
use crate::model::path::*;
use crate::runtime::error::RuntimeError;
use crate::runtime::prelude::*;
use crate::runtime::relay::control::reliable_relay_expects_interactive_response;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

/// One transaction owner for mutable client-path evidence and probe budgets.
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
struct RequestCapacityProbeBudget {
    spent_bytes: AtomicU64,
    path_spent_bytes: Box<[AtomicU64]>,
    candidate_share_bytes: AtomicU64,
}

impl RequestCapacityProbeBudget {
    fn new(path_count: usize) -> Self {
        Self {
            spent_bytes: AtomicU64::new(0),
            path_spent_bytes: (0..path_count)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            candidate_share_bytes: AtomicU64::new(0),
        }
    }

    fn remaining_bytes(&self, limit: u64) -> u64 {
        limit.saturating_sub(self.spent_bytes.load(Ordering::Acquire))
    }

    fn effective_candidate_share_bytes(&self, proposed_path_limit: u64, session_limit: u64) -> u64 {
        let frozen_limit = self.candidate_share_bytes.load(Ordering::Acquire);
        if frozen_limit == 0 {
            proposed_path_limit.min(session_limit)
        } else {
            frozen_limit
        }
    }

    fn path_remaining_bytes(
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

    fn try_reserve(
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

    fn refund(&self, path_index: usize, bytes: u64) {
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

    fn try_reserve(&self, bytes: u64, proposed_limit: u64) -> bool {
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

    fn refund(&self, bytes: u64) {
        self.spent_bytes.fetch_sub(bytes, Ordering::AcqRel);
    }
}

#[derive(Debug)]
struct RequestQuicCapacityProbeSession {
    active_token: AtomicU64,
    budget: RequestCapacityProbeBudget,
}

impl RequestQuicCapacityProbeSession {
    fn new(path_count: usize) -> Self {
        Self {
            active_token: AtomicU64::new(0),
            budget: RequestCapacityProbeBudget::new(path_count),
        }
    }
}

#[derive(Debug)]
struct RequestTcpCapacityProbeSession {
    budget: RequestCapacityProbeBudget,
}

impl RequestTcpCapacityProbeSession {
    fn new(path_count: usize) -> Self {
        Self {
            budget: RequestCapacityProbeBudget::new(path_count),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestTcpCapacityProbeSpendState {
    Reserved = 0,
    Committed = 1,
    Refund = 2,
}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct RequestTcpCapacityProbeLease {
    state: Arc<RequestTcpCapacityProbeLeaseState>,
}

#[derive(Debug)]
struct RequestTcpCapacityProbeLeaseState {
    path_state: Arc<ClientPathState>,
    campaign: Arc<RequestCapacityProbeCampaignBudget>,
    path_index: usize,
    token: u64,
    bytes: u64,
    spend_state: AtomicU8,
    ticket: QuicCapacityProbeCommandTicket,
}

impl RequestTcpCapacityProbeLease {
    pub(in crate::runtime) fn commit(&self) -> bool {
        match self.state.spend_state.compare_exchange(
            RequestTcpCapacityProbeSpendState::Reserved as u8,
            RequestTcpCapacityProbeSpendState::Committed as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => true,
            Err(state) => state == RequestTcpCapacityProbeSpendState::Committed as u8,
        }
    }

    pub(in crate::runtime) fn refund_if_unwritten(&self) {
        // Planning reserves the session envelope before queueing. A carrier
        // that proves it wrote nothing returns that reservation without
        // reopening the bounded per-path attempt policy. Refund is terminal so
        // a fast carrier cannot have this decision overwritten by a later
        // planner commit after queue admission.
        self.state.spend_state.store(
            RequestTcpCapacityProbeSpendState::Refund as u8,
            Ordering::Release,
        );
    }

    pub(in crate::runtime) fn is_current(&self) -> bool {
        self.state.ticket.is_current()
    }

    pub(in crate::runtime) fn is_published(&self) -> bool {
        self.state.ticket.resolution() == QuicCapacityProbeCommandResolution::Published
    }

    pub(in crate::runtime) fn cancel(&self) -> bool {
        self.state.ticket.cancel()
    }

    pub(in crate::runtime) async fn cancelled(&self) {
        self.state.ticket.cancelled().await;
    }
}

impl Drop for RequestTcpCapacityProbeLeaseState {
    fn drop(&mut self) {
        self.ticket.cancel();
        if let Some(record) = self
            .path_state
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(self.path_index)
        {
            if record
                .request_tcp_capacity_probe
                .as_ref()
                .is_some_and(|reservation| reservation.token == self.token)
            {
                record.request_tcp_capacity_probe = None;
            }
            if record
                .tcp_capacity_proof
                .is_some_and(|proof| proof.candidate.token == self.token)
            {
                record.tcp_capacity_proof = None;
            }
        }
        if self.spend_state.load(Ordering::Acquire)
            != RequestTcpCapacityProbeSpendState::Committed as u8
        {
            self.path_state
                .request_tcp_capacity_probe
                .budget
                .refund(self.path_index, self.bytes);
            self.campaign.refund(self.bytes);
        }
    }
}

#[derive(Debug)]
pub(in crate::runtime) struct RequestQuicCapacityProbeLease {
    path_state: Arc<ClientPathState>,
    campaign: Arc<RequestCapacityProbeCampaignBudget>,
    path_index: usize,
    token: u64,
    bytes: u64,
    committed: bool,
}

impl RequestQuicCapacityProbeLease {
    pub(in crate::runtime) fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for RequestQuicCapacityProbeLease {
    fn drop(&mut self) {
        if let Some(record) = self
            .path_state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(self.path_index)
        {
            if record
                .request_quic_capacity_probe
                .as_ref()
                .is_some_and(|reservation| reservation.token == self.token)
                && let Some(reservation) = record.request_quic_capacity_probe.take()
            {
                reservation.ticket.cancel();
            }
            if record
                .request_quic_capacity_product_handoff
                .is_some_and(|handoff| handoff.token == self.token && !handoff.complete)
            {
                record.request_quic_capacity_product_handoff = None;
                if record
                    .quic_capacity_proof
                    .is_some_and(|proof| proof.candidate.token == self.token)
                {
                    record.quic_capacity_proof = None;
                }
            }
        }
        if !self.committed {
            self.path_state
                .request_quic_capacity_probe
                .budget
                .refund(self.path_index, self.bytes);
            self.campaign.refund(self.bytes);
        }
        let _ = self
            .path_state
            .request_quic_capacity_probe
            .active_token
            .compare_exchange(self.token, 0, Ordering::AcqRel, Ordering::Acquire);
    }
}

/// Cancellation-safe ownership of one logical flow's shared path load.
///
/// Validation attachment alone is not demand. The lease starts only when the
/// flow commits unique product bytes and releases the shared scheduler load if
/// that path, relay task, or enqueue attempt is dropped.
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
    request_tcp_capacity_probe: Option<RequestTcpCapacityProbeReservation>,
    tcp_capacity_proof: Option<RequestTcpCapacityProof>,
    request_quic_capacity_probe: Option<RequestQuicCapacityProbeReservation>,
    quic_capacity_proof: Option<RequestQuicCapacityProof>,
    request_quic_capacity_product_handoff: Option<RequestQuicCapacityProductHandoff>,
    pub(in crate::runtime) path_proof_success: bool,
    path_proof_generation: u64,
    path_proof_valid_after: Instant,
    successful_path_proofs: HashMap<u64, SuccessfulPathProof>,
    successful_path_proof_order: VecDeque<u64>,
    successful_path_proof_limit: usize,
}

#[derive(Debug, Clone)]
struct RequestTcpCapacityProbeReservation {
    stream_id: StreamId,
    path_instance: RelayPathInstance,
    token: u64,
    valid_after: Instant,
    expires_at: Instant,
    train_bytes: u64,
    required_timed_bytes: u64,
    ticket: QuicCapacityProbeCommandTicket,
}

#[derive(Debug, Clone, Copy)]
struct RequestTcpCapacityProof {
    stream_id: StreamId,
    path_instance: RelayPathInstance,
    candidate: TcpCapacityProofCandidate,
}

#[derive(Debug, Clone)]
struct RequestQuicCapacityProbeReservation {
    stream_id: StreamId,
    path_instance: RelayPathInstance,
    token: u64,
    valid_after: Instant,
    expires_at: Instant,
    publication_expires_at: Instant,
    train_bytes: u64,
    ticket: QuicCapacityProbeCommandTicket,
}

#[derive(Debug, Clone, Copy)]
struct RequestQuicCapacityProof {
    candidate: QuicCapacityProofCandidate,
    rate_bps: u64,
    rate_sample_bytes: u64,
}

/// Bridges one exact QUIC carrier proof into ordinary stream-ACK ownership.
/// Carrier bytes size the path; only post-proof product ACKs make it durable.
#[derive(Debug, Clone, Copy)]
struct RequestQuicCapacityProductHandoff {
    stream_id: StreamId,
    path_instance: RelayPathInstance,
    token: u64,
    acked_product_bytes: u64,
    required_product_sample_bytes: u64,
    rate_bps: u64,
    rate_sample_bytes: u64,
    accepted_at: Instant,
    expires_at: Instant,
    complete: bool,
    rate_prior_expires_at: Option<Instant>,
}

impl RequestQuicCapacityProductHandoff {
    fn record_product_ack(
        &mut self,
        stream_id: StreamId,
        path_instance: RelayPathInstance,
        bytes: usize,
        sent_at: Instant,
        acked_at: Instant,
    ) {
        if stream_id != self.stream_id
            || path_instance != self.path_instance
            || self.complete
            || sent_at < self.accepted_at
            || acked_at >= self.expires_at
        {
            return;
        }
        self.acked_product_bytes = self.acked_product_bytes.saturating_add(bytes as u64);
        if self.acked_product_bytes >= self.required_product_sample_bytes {
            self.complete = true;
            let proof_validity = self.expires_at.saturating_duration_since(self.accepted_at);
            self.rate_prior_expires_at = acked_at
                .checked_add(proof_validity)
                .or(Some(self.expires_at));
        }
    }

    fn rate_prior_fresh(&self, now: Instant) -> bool {
        self.complete
            && self
                .rate_prior_expires_at
                .is_some_and(|expires_at| now < expires_at)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum RequestQuicCapacityProductHandoffState {
    Absent,
    Pending,
    Complete,
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
            request_tcp_capacity_probe: None,
            tcp_capacity_proof: None,
            request_quic_capacity_probe: None,
            quic_capacity_proof: None,
            request_quic_capacity_product_handoff: None,
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
    ) -> Option<Instant> {
        self.successful_path_proofs
            .get(&proof_id)
            .filter(|proof| proof.proof_id == proof_id && proof.sent_at >= attached_at)
            .map(|proof| proof.acked_at)
    }

    pub(in crate::runtime) fn mark_tcp_transport_state(&mut self, metrics: PathMetrics) {
        if self.manual_disabled || metrics.underlay != UnderlayProtocol::Tcp {
            return;
        }
        self.mark_liveness_success();
        // Same-socket TCP_INFO owns transport state only. A small path proof
        // must not turn its native delivery estimate into path-rate authority.
        self.carrier_srtt_ms = Some(f64::from(metrics.srtt_us.max(1)) / 1_000.0);
        self.carrier_rttvar_ms = Some(f64::from(metrics.rttvar_us) / 1_000.0);
        self.carrier_bytes_in_flight = metrics.bytes_in_flight;
        self.carrier_queue_bytes = metrics.queue_bytes;
        self.carrier_inflight_limit_bytes = metrics.inflight_limit_bytes;
        self.measured_loss_rate = Some(f64::from(metrics.loss_ppm) / 1_000_000.0);
    }

    pub(in crate::runtime) fn accept_request_tcp_capacity_proof(
        &mut self,
        stream_id: StreamId,
        path_instance: RelayPathInstance,
        candidate: TcpCapacityProofCandidate,
        proof_metrics: PathMetrics,
        native_transport_state: Option<PathMetrics>,
        now: Instant,
    ) -> bool {
        let Some(reservation) = self.request_tcp_capacity_probe.as_ref() else {
            return false;
        };
        if path_instance.key.underlay != UnderlayProtocol::Tcp
            || proof_metrics.underlay != UnderlayProtocol::Tcp
            || proof_metrics.direction != PathMetricDirection::ClientToServer
            || proof_metrics.path_id.0 as usize != path_instance.key.index
            || native_transport_state.is_some_and(|metrics| {
                metrics.underlay != UnderlayProtocol::Tcp
                    || metrics.direction != PathMetricDirection::ClientToServer
                    || metrics.path_id.0 as usize != path_instance.key.index
            })
            || reservation.stream_id != stream_id
            || reservation.path_instance != path_instance
            || reservation.token != candidate.token
            || reservation.train_bytes != candidate.train_bytes
            || candidate.rate_bps != candidate.receipt_rate_bps
            || candidate.rate_sample_bytes < reservation.required_timed_bytes
            || candidate.rate_sample_bytes > candidate.train_bytes
            || candidate.accepted_at < reservation.valid_after
            || candidate.accepted_at >= reservation.expires_at
            || !valid_tcp_capacity_proof_candidate_at(candidate, now)
        {
            return false;
        }
        let reservation = self
            .request_tcp_capacity_probe
            .take()
            .expect("validated request TCP capacity reservation");
        // Publication wins cancellation before proof becomes visible. The health
        // lock keeps readers from observing the transaction between those steps.
        if !reservation.ticket.publish() {
            return false;
        }
        self.tcp_capacity_proof = Some(RequestTcpCapacityProof {
            stream_id,
            path_instance,
            candidate,
        });
        // RTT, queue, and cwnd remain current socket diagnostics. Carrier rate
        // authority stays inside the expiring typed proof until product ACKs
        // replace it; an offset-free train must not become a cross-stream prior.
        if let Some(native_transport_state) = native_transport_state {
            self.mark_tcp_transport_state(native_transport_state);
        }
        true
    }

    pub(in crate::runtime) fn accept_request_quic_capacity_proof(
        &mut self,
        candidate: QuicCapacityProofCandidate,
        probe: quic_transport::MeasurementMetrics,
        now: Instant,
    ) -> Option<(u64, u64, bool)> {
        let reservation = self.request_quic_capacity_probe.as_ref()?;
        if candidate.token != reservation.token
            || probe.token != candidate.token
            || candidate.train_bytes != reservation.train_bytes
            || candidate.accepted_at < reservation.valid_after
            || candidate.accepted_at >= reservation.expires_at
            || now >= candidate.expires_at
        {
            return None;
        }
        let reservation = self.request_quic_capacity_probe.take()?;
        // Publish the command transaction before exposing its proof. A
        // concurrent sender cancellation must leave neither state visible.
        if !reservation.ticket.publish() {
            return None;
        }
        let native_rate = probe
            .timed_measurement_ack_elapsed
            .filter(|elapsed| !elapsed.is_zero())
            .filter(|_| {
                probe.timed_measurement_acked_carrier_bytes >= probe.required_timed_carrier_bytes
            })
            .map(|elapsed| {
                (probe.timed_measurement_acked_carrier_bytes as f64 * 8.0 / elapsed.as_secs_f64())
                    .round()
                    .max(1.0) as u64
            });
        let native_tail_rate = native_rate.is_some();
        let rate_bps = native_rate
            .unwrap_or(candidate.rate_bps)
            .max(candidate.rate_bps);
        let rate_sample_bytes = native_rate
            .map(|_| probe.timed_measurement_acked_carrier_bytes)
            .unwrap_or(candidate.train_bytes);
        self.quic_capacity_proof = Some(RequestQuicCapacityProof {
            candidate,
            rate_bps,
            rate_sample_bytes,
        });
        self.request_quic_capacity_product_handoff = Some(RequestQuicCapacityProductHandoff {
            stream_id: reservation.stream_id,
            path_instance: reservation.path_instance,
            token: candidate.token,
            acked_product_bytes: 0,
            required_product_sample_bytes: candidate.required_proof_bytes,
            rate_bps,
            rate_sample_bytes,
            accepted_at: candidate.accepted_at,
            expires_at: candidate.expires_at,
            complete: false,
            rate_prior_expires_at: None,
        });
        Some((rate_bps, rate_sample_bytes, native_tail_rate))
    }

    fn record_request_quic_capacity_product_ack(
        &mut self,
        stream_id: StreamId,
        path_instance: RelayPathInstance,
        bytes: usize,
        sent_at: Instant,
        acked_at: Instant,
    ) -> Option<u64> {
        if let Some(handoff) = self.request_quic_capacity_product_handoff.as_mut() {
            handoff.record_product_ack(stream_id, path_instance, bytes, sent_at, acked_at);
            return handoff.complete.then_some(handoff.token);
        }
        None
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

    fn request_quic_capacity_product_handoff_state(
        &self,
        token: u64,
    ) -> RequestQuicCapacityProductHandoffState {
        match self.request_quic_capacity_product_handoff {
            Some(handoff) if handoff.token == token && handoff.complete => {
                RequestQuicCapacityProductHandoffState::Complete
            }
            Some(handoff) if handoff.token == token => {
                RequestQuicCapacityProductHandoffState::Pending
            }
            _ => RequestQuicCapacityProductHandoffState::Absent,
        }
    }
}

impl ClientPathHealthRecord {
    pub(in crate::runtime) fn observe(&mut self, now: Instant) -> ClientPathObservation {
        if self
            .request_tcp_capacity_probe
            .as_ref()
            .is_some_and(|reservation| now >= reservation.expires_at)
            && let Some(reservation) = self.request_tcp_capacity_probe.take()
        {
            reservation.ticket.cancel();
        }
        if self
            .tcp_capacity_proof
            .is_some_and(|proof| now >= proof.candidate.expires_at)
        {
            self.tcp_capacity_proof = None;
        }
        if self
            .request_quic_capacity_probe
            .as_ref()
            .is_some_and(|reservation| {
                !reservation.ticket.is_current() || now >= reservation.publication_expires_at
            })
            && let Some(reservation) = self.request_quic_capacity_probe.take()
        {
            reservation.ticket.cancel();
        }
        if let Some(expired_token) = self
            .quic_capacity_proof
            .and_then(|proof| (now >= proof.candidate.expires_at).then_some(proof.candidate.token))
        {
            self.quic_capacity_proof = None;
            if self
                .request_quic_capacity_product_handoff
                .is_some_and(|handoff| handoff.token == expired_token && !handoff.complete)
            {
                self.request_quic_capacity_product_handoff = None;
            }
        }
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
        if self.state == SchedulerPathState::Failed
            && self.failed_until.is_some_and(|deadline| now >= deadline)
        {
            self.state = SchedulerPathState::Suspect;
            self.failed_until = None;
        }
        let tcp_proof = self.tcp_capacity_proof;
        let quic_proof = self.quic_capacity_proof;
        let complete_handoff = self
            .request_quic_capacity_product_handoff
            .filter(|handoff| handoff.complete);
        // Once stream ACKs complete the handoff, prefer their product model
        // until a full native carrier window supplies a replacement estimate.
        let handoff_capacity_prior = complete_handoff.filter(|handoff| {
            quic_proof.is_none()
                && handoff.rate_prior_fresh(now)
                && !self.has_durable_native_carrier_window()
        });
        let proof_rate_bps = tcp_proof
            .map(|proof| proof.candidate.rate_bps as f64)
            .or_else(|| quic_proof.map(|proof| proof.rate_bps as f64));
        let proof_sample_bytes = tcp_proof
            .map(|proof| proof.candidate.rate_sample_bytes)
            .or_else(|| quic_proof.map(|proof| proof.rate_sample_bytes));
        let proof_accepted_at = tcp_proof
            .map(|proof| proof.candidate.accepted_at)
            .or_else(|| quic_proof.map(|proof| proof.candidate.accepted_at));
        let explicit_carrier_capacity_proof = proof_rate_bps.is_some();
        ClientPathObservation {
            state: self.state,
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
            quic_capacity_product_handoff_complete: complete_handoff.is_some(),
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
        if reliable_relay_expects_interactive_response(lane) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
    }

    pub(in crate::runtime) fn reserve_load(&mut self, lane: FlowLane) {
        self.active_flows = self.active_flows.saturating_add(1);
        if reliable_relay_expects_interactive_response(lane) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
    }

    pub(in crate::runtime) fn mark_reserved_open_success(&mut self, _elapsed: Duration) {
        self.mark_liveness_success();
    }

    pub(in crate::runtime) fn release_load(&mut self, lane: FlowLane) {
        self.active_flows = self.active_flows.saturating_sub(1);
        if reliable_relay_expects_interactive_response(lane) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_sub(1);
        }
    }

    pub(in crate::runtime) fn change_lane_load(&mut self, from: FlowLane, to: FlowLane) {
        if reliable_relay_expects_interactive_response(from)
            && !reliable_relay_expects_interactive_response(to)
        {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_sub(1);
        } else if !reliable_relay_expects_interactive_response(from)
            && reliable_relay_expects_interactive_response(to)
        {
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
        if let Some(reservation) = self.request_tcp_capacity_probe.take() {
            reservation.ticket.cancel();
        }
        self.tcp_capacity_proof = None;
        if let Some(reservation) = self.request_quic_capacity_probe.take() {
            reservation.ticket.cancel();
        }
        self.quic_capacity_proof = None;
        self.request_quic_capacity_product_handoff = None;
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

    pub(in crate::runtime) fn request_quic_capacity_probe_remaining_bytes(&self) -> u64 {
        self.state
            .request_quic_capacity_probe
            .budget
            .remaining_bytes(reliable_capacity_calibration_session_limit_bytes(
                self.mux_limits,
            ))
    }

    pub(in crate::runtime) fn request_tcp_capacity_probe_remaining_bytes(&self) -> u64 {
        self.state
            .request_tcp_capacity_probe
            .budget
            .remaining_bytes(reliable_capacity_calibration_session_limit_bytes(
                self.mux_limits,
            ))
    }

    pub(in crate::runtime) fn request_quic_capacity_probe_candidate_share_bytes(
        &self,
        proposed_path_limit: u64,
    ) -> u64 {
        self.state
            .request_quic_capacity_probe
            .budget
            .effective_candidate_share_bytes(
                proposed_path_limit,
                reliable_capacity_calibration_session_limit_bytes(self.mux_limits),
            )
    }

    pub(in crate::runtime) fn request_tcp_capacity_probe_candidate_share_bytes(
        &self,
        proposed_path_limit: u64,
    ) -> u64 {
        self.state
            .request_tcp_capacity_probe
            .budget
            .effective_candidate_share_bytes(
                proposed_path_limit,
                reliable_capacity_calibration_session_limit_bytes(self.mux_limits),
            )
    }

    pub(in crate::runtime) fn request_quic_capacity_probe_path_remaining_bytes(
        &self,
        path_index: usize,
        path_limit: u64,
    ) -> u64 {
        self.state
            .request_quic_capacity_probe
            .budget
            .path_remaining_bytes(
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
        self.state
            .request_tcp_capacity_probe
            .budget
            .path_remaining_bytes(
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
        ticket: QuicCapacityProbeCommandTicket,
    ) -> Option<RequestTcpCapacityProbeLease> {
        let now = Instant::now();
        if path_instance.key.underlay != UnderlayProtocol::Tcp
            || path_instance.key.index != path_index
            || token == 0
            || train_bytes < PATH_OPEN_SCORE_BYTES as u64
            || required_timed_bytes < PATH_OPEN_SCORE_BYTES as u64
            || required_timed_bytes > train_bytes
            || expires_at <= now
        {
            return None;
        }
        let mut health = self.state.health.lock().expect("client path health lock");
        let Some(record) = health.tcp.get_mut(path_index) else {
            return None;
        };
        let _ = record.observe(now);
        // Offset-free trains on distinct TCP sockets have independent carrier
        // ordering. The health record remains the exact per-path transaction
        // owner while the session counter bounds their cumulative byte cost.
        if record.request_tcp_capacity_probe.is_some() || record.tcp_capacity_proof.is_some() {
            return None;
        }
        let session_limit = reliable_capacity_calibration_session_limit_bytes(self.mux_limits);
        let campaign_limit = self
            .state
            .request_tcp_capacity_probe
            .budget
            .effective_candidate_share_bytes(path_limit_bytes, session_limit);
        if !campaign.try_reserve(train_bytes, campaign_limit) {
            return None;
        }
        if !self.state.request_tcp_capacity_probe.budget.try_reserve(
            path_index,
            train_bytes,
            path_limit_bytes,
            session_limit,
        ) {
            campaign.refund(train_bytes);
            return None;
        }
        record.request_tcp_capacity_probe = Some(RequestTcpCapacityProbeReservation {
            stream_id,
            path_instance,
            token,
            valid_after,
            expires_at,
            train_bytes,
            required_timed_bytes,
            ticket: ticket.clone(),
        });
        drop(health);
        Some(RequestTcpCapacityProbeLease {
            state: Arc::new(RequestTcpCapacityProbeLeaseState {
                path_state: self.state.clone(),
                campaign,
                path_index,
                token,
                bytes: train_bytes,
                spend_state: AtomicU8::new(RequestTcpCapacityProbeSpendState::Reserved as u8),
                ticket,
            }),
        })
    }

    pub(in crate::runtime) fn request_tcp_capacity_probe_proof(
        &self,
        stream_id: StreamId,
        path_index: usize,
        path_instance: RelayPathInstance,
        token: u64,
    ) -> Option<TcpCapacityProofCandidate> {
        let now = Instant::now();
        self.state
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(path_index)
            .and_then(|record| {
                let _ = record.observe(now);
                record
                    .tcp_capacity_proof
                    .filter(|proof| {
                        proof.stream_id == stream_id
                            && proof.path_instance == path_instance
                            && proof.candidate.token == token
                    })
                    .map(|proof| proof.candidate)
            })
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
        ticket: QuicCapacityProbeCommandTicket,
    ) -> Option<RequestQuicCapacityProbeLease> {
        let now = Instant::now();
        let publication_expires_at = expires_at.checked_add(proof_validity)?;
        if path_instance.key.underlay != UnderlayProtocol::Udp
            || path_instance.key.index != path_index
            || token == 0
            || train_bytes == 0
            || proof_validity.is_zero()
            || expires_at <= now
        {
            return None;
        }
        let mut health = self.state.health.lock().expect("client path health lock");
        let active_token = self
            .state
            .request_quic_capacity_probe
            .active_token
            .load(Ordering::Acquire);
        if active_token != 0 {
            let transaction_live = health.udp.iter_mut().any(|record| {
                let _ = record.observe(now);
                if record
                    .request_quic_capacity_probe
                    .as_ref()
                    .is_some_and(|reservation| {
                        reservation.token == active_token && !reservation.ticket.is_current()
                    })
                {
                    record.request_quic_capacity_probe = None;
                }
                record
                    .request_quic_capacity_probe
                    .as_ref()
                    .is_some_and(|reservation| reservation.token == active_token)
                    || record
                        .request_quic_capacity_product_handoff
                        .is_some_and(|handoff| handoff.token == active_token && !handoff.complete)
            });
            if !transaction_live {
                let _ = self
                    .state
                    .request_quic_capacity_probe
                    .active_token
                    .compare_exchange(active_token, 0, Ordering::AcqRel, Ordering::Acquire);
            }
        }
        if self
            .state
            .request_quic_capacity_probe
            .active_token
            .compare_exchange(0, token, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return None;
        }
        let session_limit = reliable_capacity_calibration_session_limit_bytes(self.mux_limits);
        let campaign_limit = self
            .state
            .request_quic_capacity_probe
            .budget
            .effective_candidate_share_bytes(path_limit_bytes, session_limit);
        if !campaign.try_reserve(train_bytes, campaign_limit) {
            let _ = self
                .state
                .request_quic_capacity_probe
                .active_token
                .compare_exchange(token, 0, Ordering::AcqRel, Ordering::Acquire);
            return None;
        }
        if !self.state.request_quic_capacity_probe.budget.try_reserve(
            path_index,
            train_bytes,
            path_limit_bytes,
            session_limit,
        ) {
            campaign.refund(train_bytes);
            let _ = self
                .state
                .request_quic_capacity_probe
                .active_token
                .compare_exchange(token, 0, Ordering::AcqRel, Ordering::Acquire);
            return None;
        }
        let Some(record) = health.udp.get_mut(path_index) else {
            self.state
                .request_quic_capacity_probe
                .budget
                .refund(path_index, train_bytes);
            campaign.refund(train_bytes);
            let _ = self
                .state
                .request_quic_capacity_probe
                .active_token
                .compare_exchange(token, 0, Ordering::AcqRel, Ordering::Acquire);
            return None;
        };
        if record.request_quic_capacity_probe.is_some() {
            self.state
                .request_quic_capacity_probe
                .budget
                .refund(path_index, train_bytes);
            campaign.refund(train_bytes);
            let _ = self
                .state
                .request_quic_capacity_probe
                .active_token
                .compare_exchange(token, 0, Ordering::AcqRel, Ordering::Acquire);
            return None;
        }
        record.request_quic_capacity_probe = Some(RequestQuicCapacityProbeReservation {
            stream_id,
            path_instance,
            token,
            valid_after,
            expires_at,
            publication_expires_at,
            train_bytes,
            ticket,
        });
        drop(health);
        Some(RequestQuicCapacityProbeLease {
            path_state: self.state.clone(),
            campaign,
            path_index,
            token,
            bytes: train_bytes,
            committed: false,
        })
    }

    pub(in crate::runtime) fn request_quic_capacity_probe_proven(
        &self,
        path_index: usize,
        token: u64,
    ) -> bool {
        let now = Instant::now();
        self.state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(path_index)
            .is_some_and(|record| {
                let _ = record.observe(now);
                record.quic_capacity_proof.is_some_and(|proof| {
                    proof.candidate.token == token && now < proof.candidate.expires_at
                })
            })
    }

    pub(in crate::runtime) fn request_quic_capacity_product_handoff_state(
        &self,
        path_index: usize,
        token: u64,
    ) -> RequestQuicCapacityProductHandoffState {
        let now = Instant::now();
        let state = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(path_index)
            .map_or(RequestQuicCapacityProductHandoffState::Absent, |record| {
                let _ = record.observe(now);
                record.request_quic_capacity_product_handoff_state(token)
            });
        if state != RequestQuicCapacityProductHandoffState::Pending {
            let _ = self
                .state
                .request_quic_capacity_probe
                .active_token
                .compare_exchange(token, 0, Ordering::AcqRel, Ordering::Acquire);
        }
        state
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
        self.state
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
            .is_some_and(|record| {
                path_observation_is_idle_for_probe(record.observe(Instant::now()))
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
        if self
            .state
            .request_quic_capacity_probe
            .active_token
            .load(Ordering::Acquire)
            == 0
        {
            // Every QUIC owner ACK reaches this hook. Without an active probe
            // transaction there is no handoff state, so avoid the health lock.
            return;
        }
        let completed_token = {
            let mut health = self.state.health.lock().expect("client path health lock");
            health
                .udp
                .get_mut(path_instance.key.index)
                .and_then(|current| {
                    current.record_request_quic_capacity_product_ack(
                        stream_id,
                        path_instance,
                        bytes,
                        sent_at,
                        acked_at,
                    )
                })
        };
        if let Some(token) = completed_token {
            let _ = self
                .state
                .request_quic_capacity_probe
                .active_token
                .compare_exchange(token, 0, Ordering::AcqRel, Ordering::Acquire);
        }
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
        self.state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
            .is_some_and(|record| {
                path_observation_is_idle_for_probe(record.observe(Instant::now()))
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
