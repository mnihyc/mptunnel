//! TCP capacity measurement and receipt-proof ownership.
//!
//! One TCP path owns one exact reservation-to-proof transaction. This module
//! also converts typed receiver receipts and optional native snapshots into
//! capacity evidence; socket capture and polling remain in `metrics`.

use super::metrics::TcpNativeObservation;
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, RELIABLE_INITIAL_RTT, TRANSPORT_TIMER_GRANULARITY,
    TcpCapacityProofCandidate, valid_tcp_capacity_proof_candidate_at,
};
use crate::model::path::RelayPathInstance;
use crate::protocol::{PathId, PathMetricDirection, PathMetrics, StreamId, UnderlayProtocol};
use crate::runtime::path::commands::{CapacityProbeCommandResolution, CapacityProbeCommandTicket};
use crate::runtime::path::health::ClientPathHealthRecord;
use crate::runtime::path::model::metric_epoch_now;
use crate::runtime::path::state::{
    ClientPathState, RequestCapacityProbeBudget, RequestCapacityProbeCampaignBudget,
};
use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub(in crate::runtime::path) struct RequestTcpCapacityProbeSession {
    budget: RequestCapacityProbeBudget,
}

impl RequestTcpCapacityProbeSession {
    pub(in crate::runtime::path) fn new(path_count: usize) -> Self {
        Self {
            budget: RequestCapacityProbeBudget::new(path_count),
        }
    }

    fn remaining_bytes(&self, session_limit: u64) -> u64 {
        self.budget.remaining_bytes(session_limit)
    }

    fn candidate_share_bytes(&self, proposed_path_limit: u64, session_limit: u64) -> u64 {
        self.budget
            .effective_candidate_share_bytes(proposed_path_limit, session_limit)
    }

    fn path_remaining_bytes(&self, path_index: usize, path_limit: u64, session_limit: u64) -> u64 {
        self.budget
            .path_remaining_bytes(path_index, path_limit, session_limit)
    }

    fn try_reserve(
        &self,
        path_index: usize,
        bytes: u64,
        path_limit: u64,
        session_limit: u64,
    ) -> bool {
        self.budget
            .try_reserve(path_index, bytes, path_limit, session_limit)
    }

    fn refund(&self, path_index: usize, bytes: u64) {
        self.budget.refund(path_index, bytes);
    }
}

#[derive(Debug, Clone, Default)]
pub(in crate::runtime::path) struct RequestTcpCapacityRecord {
    reservation: Option<RequestTcpCapacityProbeReservation>,
    proof: Option<RequestTcpCapacityProof>,
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
    ticket: CapacityProbeCommandTicket,
}

#[derive(Debug, Clone, Copy)]
struct RequestTcpCapacityProof {
    stream_id: StreamId,
    path_instance: RelayPathInstance,
    candidate: TcpCapacityProofCandidate,
}

struct RequestTcpCapacityProofEvidence {
    path_id: PathId,
    candidate: TcpCapacityProofCandidate,
    proof_metrics: PathMetrics,
    native_transport_state: Option<TcpNativeObservation>,
    observed_at: Instant,
}

impl RequestTcpCapacityRecord {
    fn is_idle(&self) -> bool {
        self.reservation.is_none() && self.proof.is_none()
    }

    #[allow(clippy::too_many_arguments)]
    fn reserve(
        &mut self,
        stream_id: StreamId,
        path_instance: RelayPathInstance,
        token: u64,
        train_bytes: u64,
        required_timed_bytes: u64,
        valid_after: Instant,
        expires_at: Instant,
        ticket: CapacityProbeCommandTicket,
    ) {
        debug_assert!(self.is_idle());
        self.reservation = Some(RequestTcpCapacityProbeReservation {
            stream_id,
            path_instance,
            token,
            valid_after,
            expires_at,
            train_bytes,
            required_timed_bytes,
            ticket,
        });
    }

    pub(in crate::runtime::path) fn maintain(&mut self, now: Instant) {
        if self
            .reservation
            .as_ref()
            .is_some_and(|reservation| now >= reservation.expires_at)
            && let Some(reservation) = self.reservation.take()
        {
            reservation.ticket.cancel();
        }
        if self
            .proof
            .is_some_and(|proof| now >= proof.candidate.expires_at)
        {
            self.proof = None;
        }
    }

    fn clear_token(&mut self, token: u64) {
        if self
            .reservation
            .as_ref()
            .is_some_and(|reservation| reservation.token == token)
        {
            self.reservation = None;
        }
        if self
            .proof
            .is_some_and(|proof| proof.candidate.token == token)
        {
            self.proof = None;
        }
    }

    pub(in crate::runtime::path) fn reset_after_data_plane_failure(&mut self) {
        if let Some(reservation) = self.reservation.take() {
            reservation.ticket.cancel();
        }
        self.proof = None;
    }

    pub(in crate::runtime::path) fn proof_candidate_at(
        &self,
        now: Instant,
    ) -> Option<TcpCapacityProofCandidate> {
        self.proof
            .filter(|proof| proof.candidate.accepted_at <= now && now < proof.candidate.expires_at)
            .map(|proof| proof.candidate)
    }

    pub(in crate::runtime::path) fn exact_proof_candidate_at(
        &self,
        stream_id: StreamId,
        path_instance: RelayPathInstance,
        token: u64,
        now: Instant,
    ) -> Option<TcpCapacityProofCandidate> {
        self.proof
            .filter(|proof| {
                proof.stream_id == stream_id
                    && proof.path_instance == path_instance
                    && proof.candidate.token == token
                    && proof.candidate.accepted_at <= now
                    && now < proof.candidate.expires_at
            })
            .map(|proof| proof.candidate)
    }

    fn accept_proof(
        &mut self,
        stream_id: StreamId,
        path_instance: RelayPathInstance,
        evidence: RequestTcpCapacityProofEvidence,
    ) -> bool {
        let RequestTcpCapacityProofEvidence {
            path_id,
            candidate,
            proof_metrics,
            native_transport_state,
            observed_at,
        } = evidence;
        let Some(reservation) = self.reservation.as_ref() else {
            return false;
        };
        if path_instance.key.underlay != UnderlayProtocol::Tcp
            || proof_metrics.underlay != UnderlayProtocol::Tcp
            || proof_metrics.direction != PathMetricDirection::ClientToServer
            || proof_metrics.path_id != path_id
            || native_transport_state.is_some_and(|observation| {
                observation.direction() != PathMetricDirection::ClientToServer
                    || observation.path_id() != path_id
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
            || !valid_tcp_capacity_proof_candidate_at(candidate, observed_at)
        {
            return false;
        }
        let reservation = self
            .reservation
            .take()
            .expect("validated request TCP capacity reservation");
        // Publication wins cancellation before proof becomes visible. The path
        // health lock keeps readers from seeing the transaction between steps.
        if !reservation.ticket.publish() {
            return false;
        }
        self.proof = Some(RequestTcpCapacityProof {
            stream_id,
            path_instance,
            candidate,
        });
        true
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
    ticket: CapacityProbeCommandTicket,
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
        // A carrier that proves it wrote nothing returns the planning budget.
        // Refund is terminal so a later planner commit cannot reverse it.
        self.state.spend_state.store(
            RequestTcpCapacityProbeSpendState::Refund as u8,
            Ordering::Release,
        );
    }

    pub(in crate::runtime) fn is_current(&self) -> bool {
        self.state.ticket.is_current()
    }

    pub(in crate::runtime) fn is_published(&self) -> bool {
        self.state.ticket.resolution() == CapacityProbeCommandResolution::Published
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
            .health()
            .lock()
            .expect("client path health lock")
            .tcp_record_mut(self.path_index)
        {
            record.tcp_capacity.clear_token(self.token);
        }
        if self.spend_state.load(Ordering::Acquire)
            != RequestTcpCapacityProbeSpendState::Committed as u8
        {
            self.path_state
                .request_tcp_capacity_probe_session()
                .refund(self.path_index, self.bytes);
            self.campaign.refund(self.bytes);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct RequestTcpCapacityProofQuery {
    pub(in crate::runtime) target: RelayPathInstance,
    pub(in crate::runtime) token: u64,
}

impl ClientPathHealthRecord {
    pub(in crate::runtime) fn accept_request_tcp_capacity_proof(
        &mut self,
        stream_id: StreamId,
        path_instance: RelayPathInstance,
        candidate: TcpCapacityProofCandidate,
        proof_metrics: PathMetrics,
        native_transport_state: Option<TcpNativeObservation>,
        now: Instant,
    ) -> bool {
        let Some(path_id) = self.wire_path_id() else {
            return false;
        };
        if !self.tcp_capacity.accept_proof(
            stream_id,
            path_instance,
            RequestTcpCapacityProofEvidence {
                path_id,
                candidate,
                proof_metrics,
                native_transport_state,
                observed_at: now,
            },
        ) {
            return false;
        }
        // The receipt owns this explicit measurement epoch. Same-socket native
        // telemetry is retained separately as TCP transport state and never
        // becomes an MPP Data ACK.
        if let Some(native_transport_state) = native_transport_state {
            self.mark_tcp_transport_state(path_instance.path_instance_id, native_transport_state);
        }
        true
    }
}

impl ClientPathState {
    pub(in crate::runtime::path) fn request_tcp_capacity_probe_remaining_bytes(
        &self,
        session_limit: u64,
    ) -> u64 {
        self.request_tcp_capacity_probe_session()
            .remaining_bytes(session_limit)
    }

    pub(in crate::runtime::path) fn request_tcp_capacity_probe_candidate_share_bytes(
        &self,
        proposed_path_limit: u64,
        session_limit: u64,
    ) -> u64 {
        self.request_tcp_capacity_probe_session()
            .candidate_share_bytes(proposed_path_limit, session_limit)
    }

    pub(in crate::runtime::path) fn request_tcp_capacity_probe_path_remaining_bytes(
        &self,
        path_index: usize,
        path_limit: u64,
        session_limit: u64,
    ) -> u64 {
        self.request_tcp_capacity_probe_session()
            .path_remaining_bytes(path_index, path_limit, session_limit)
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::runtime::path) fn try_reserve_request_tcp_capacity_probe(
        self: &Arc<Self>,
        stream_id: StreamId,
        path_index: usize,
        path_instance: RelayPathInstance,
        token: u64,
        train_bytes: u64,
        path_limit_bytes: u64,
        session_limit: u64,
        campaign: Arc<RequestCapacityProbeCampaignBudget>,
        required_timed_bytes: u64,
        valid_after: Instant,
        expires_at: Instant,
        ticket: CapacityProbeCommandTicket,
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
        let mut health = self.health().lock().expect("client path health lock");
        let record = health.tcp_record_mut(path_index)?;
        record.maintain(now);
        let record = health.tcp_record_mut(path_index)?;
        // Distinct TCP sockets have independent ordering. The path capsule owns
        // exact identity while the session budget bounds their cumulative cost.
        if !record.tcp_capacity.is_idle() {
            return None;
        }
        let session = self.request_tcp_capacity_probe_session();
        let campaign_limit = session.candidate_share_bytes(path_limit_bytes, session_limit);
        if !campaign.try_reserve(train_bytes, campaign_limit) {
            return None;
        }
        if !session.try_reserve(path_index, train_bytes, path_limit_bytes, session_limit) {
            campaign.refund(train_bytes);
            return None;
        }
        record.tcp_capacity.reserve(
            stream_id,
            path_instance,
            token,
            train_bytes,
            required_timed_bytes,
            valid_after,
            expires_at,
            ticket.clone(),
        );
        drop(health);
        Some(RequestTcpCapacityProbeLease {
            state: Arc::new(RequestTcpCapacityProbeLeaseState {
                path_state: self.clone(),
                campaign,
                path_index,
                token,
                bytes: train_bytes,
                spend_state: AtomicU8::new(RequestTcpCapacityProbeSpendState::Reserved as u8),
                ticket,
            }),
        })
    }
}

pub(in crate::runtime) fn tcp_capacity_receipt_rate_bps(
    sample_bytes: u64,
    elapsed: Duration,
) -> Option<u64> {
    if sample_bytes == 0 || elapsed.is_zero() {
        return None;
    }
    let rate = sample_bytes as f64 * 8.0 / elapsed.max(TRANSPORT_TIMER_GRANULARITY).as_secs_f64();
    rate.is_finite()
        .then_some(rate.round().clamp(1.0, u64::MAX as f64) as u64)
}

pub(in crate::runtime) fn tcp_capacity_proof_validity(metrics: PathMetrics) -> Duration {
    Duration::from_micros(u64::from(metrics.srtt_us.max(1)))
        .saturating_mul(4)
        .clamp(Duration::from_secs(1), Duration::from_secs(5))
}

pub(in crate::runtime) fn request_tcp_capacity_receipt_metrics(
    path_id: PathId,
    received_bytes: u64,
    receipt_rate_bps: u64,
    baseline: Option<PathMetrics>,
    native: Option<TcpNativeObservation>,
) -> PathMetrics {
    // A cold request train may be below the real BDP. Its full receiver receipt
    // is the conservative rate seed; product ACKs replace it after product admission.
    tcp_capacity_receipt_metrics(
        path_id,
        PathMetricDirection::ClientToServer,
        received_bytes,
        receipt_rate_bps,
        baseline,
        native,
    )
}

fn tcp_capacity_receipt_metrics(
    path_id: PathId,
    direction: PathMetricDirection,
    received_bytes: u64,
    receipt_rate_bps: u64,
    baseline: Option<PathMetrics>,
    native: Option<TcpNativeObservation>,
) -> PathMetrics {
    let mut metrics = baseline.unwrap_or_else(|| portable_tcp_receipt_metrics(path_id, direction));
    if let Some(native) = native {
        native.apply_transport_shape(&mut metrics);
        metrics.metric_epoch = metric_epoch_now();
        metrics.metric_age_us = 0;
    }
    let rate_bps = receipt_rate_bps.max(1);
    metrics.path_id = path_id;
    metrics.underlay = UnderlayProtocol::Tcp;
    metrics.direction = direction;
    metrics.delivery_rate_bps = rate_bps;
    metrics.pacing_rate_bps = rate_bps;
    metrics.has_ack_derived_data_sample = true;
    metrics.data_sample_count = metrics.data_sample_count.max(1);
    metrics.data_sample_bytes = metrics.data_sample_bytes.max(received_bytes);
    metrics.confidence_ppm = 1_000_000;
    metrics.app_limited = native
        .and_then(TcpNativeObservation::app_limited)
        .unwrap_or(true);
    if !native.is_some_and(TcpNativeObservation::has_flight) {
        // A configured startup prior is not native congestion evidence. Keep
        // cwnd unknown so receipt-rate BDP, not an initial-window hint, bounds
        // portable high-bandwidth admission.
        metrics.inflight_limit_bytes = 0;
        metrics.inflight_hi_bytes = 0;
    }
    metrics
}

fn portable_tcp_receipt_metrics(path_id: PathId, direction: PathMetricDirection) -> PathMetrics {
    // This is path shape, not rate evidence. The typed receipt installed by the
    // caller supplies rate while this conservative prior supplies RFC-like RTT
    // and initial-window geometry when the host has no native socket counters.
    let initial_rtt_us = u32::try_from(RELIABLE_INITIAL_RTT.as_micros()).unwrap_or(u32::MAX);
    PathMetrics {
        path_id,
        underlay: UnderlayProtocol::Tcp,
        direction,
        metric_epoch: metric_epoch_now(),
        metric_age_us: 0,
        srtt_us: initial_rtt_us,
        rttvar_us: initial_rtt_us / 2,
        jitter_us: initial_rtt_us / 2,
        delivery_rate_bps: 1,
        pacing_rate_bps: 1,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: false,
        ecn_observed: false,
        bytes_in_flight: 0,
        queue_bytes: 0,
        inflight_limit_bytes: PATH_OPEN_SCORE_BYTES as u64,
        inflight_hi_bytes: PATH_OPEN_SCORE_BYTES as u64,
        confidence_ppm: 0,
        app_limited: true,
        has_ack_derived_data_sample: false,
        data_sample_count: 0,
        data_sample_bytes: 0,
    }
}

#[cfg(test)]
#[path = "tests_capacity.rs"]
mod tests;
