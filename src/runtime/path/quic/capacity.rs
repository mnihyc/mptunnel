//! QUIC capacity transaction, framing, and receipt ownership.
//!
//! One session-wide token serializes native measurement and its product-ACK
//! admission. This module alone translates MPP capacity commands into carrier
//! epochs; generic QUIC transport remains unaware of product ownership.

use super::io::UdpPathSendStream;
use crate::model::capacity::{
    QUIC_MAX_ACK_DELAY, QUIC_TIMER_GRANULARITY, QuicCapacityProofCandidate,
};
use crate::model::path::RelayPathInstance;
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{Frame, PathId, StreamId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{CapacityProbeCommandTicket, QuicCapacityProbeCommand};
use crate::runtime::path::health::ClientPathHealthRecord;
use crate::runtime::path::state::{
    ClientPathState, RequestCapacityProbeBudget, RequestCapacityProbeCampaignBudget,
};
use crate::transport::quic as quic_transport;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

const QUIC_CAPACITY_RECORD_PAYLOAD_BYTES: usize = 64 * 1024;

// Request transaction ownership stays above Quinn mechanics so one exact
// carrier proof and its product admission cannot be observed independently.
#[derive(Debug)]
pub(in crate::runtime::path) struct RequestQuicCapacityProbeSession {
    active_token: AtomicU64,
    budget: RequestCapacityProbeBudget,
}

impl RequestQuicCapacityProbeSession {
    pub(in crate::runtime::path) fn new(path_count: usize) -> Self {
        Self {
            active_token: AtomicU64::new(0),
            budget: RequestCapacityProbeBudget::new(path_count),
        }
    }

    fn active_token(&self) -> u64 {
        self.active_token.load(Ordering::Acquire)
    }

    fn claim(&self, token: u64) -> bool {
        self.active_token
            .compare_exchange(0, token, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn retire(&self, token: u64) {
        let _ = self
            .active_token
            .compare_exchange(token, 0, Ordering::AcqRel, Ordering::Acquire);
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
pub(in crate::runtime::path) struct RequestQuicCapacityRecord {
    reservation: Option<RequestQuicCapacityProbeReservation>,
    proof: Option<RequestQuicCapacityProof>,
    product_admission: Option<RequestQuicCapacityProductAdmission>,
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
    ticket: CapacityProbeCommandTicket,
}

#[derive(Debug, Clone, Copy)]
struct RequestQuicCapacityProof {
    candidate: QuicCapacityProofCandidate,
    rate_bps: u64,
    rate_sample_bytes: u64,
}

/// Bridges one exact QUIC carrier proof into ordinary product-ACK ownership.
#[derive(Debug, Clone, Copy)]
struct RequestQuicCapacityProductAdmission {
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
    completed_at: Option<Instant>,
    rate_prior_expires_at: Option<Instant>,
}

impl RequestQuicCapacityProductAdmission {
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
            self.completed_at.get_or_insert(acked_at);
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
pub(in crate::runtime) enum RequestQuicCapacityProductAdmissionState {
    Absent,
    Pending,
    Complete,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct RequestQuicCapacityReconciliationQuery {
    pub(in crate::runtime) target: RelayPathInstance,
    pub(in crate::runtime) token: u64,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime::path) struct RequestQuicCapacityEvidence {
    pub(in crate::runtime::path) rate_bps: u64,
    pub(in crate::runtime::path) rate_sample_bytes: u64,
    pub(in crate::runtime::path) accepted_at: Instant,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime::path) struct RequestQuicCapacityObservation {
    pub(in crate::runtime::path) proof: Option<RequestQuicCapacityEvidence>,
    pub(in crate::runtime::path) product_admission_prior: Option<RequestQuicCapacityEvidence>,
    pub(in crate::runtime::path) product_admission_complete: bool,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime::path) struct RequestQuicCapacityReconciliation {
    pub(in crate::runtime::path) carrier_proven: bool,
    pub(in crate::runtime::path) product_admission: RequestQuicCapacityProductAdmissionState,
}

impl RequestQuicCapacityRecord {
    fn transaction_live(&self, token: u64) -> bool {
        self.reservation
            .as_ref()
            .is_some_and(|reservation| reservation.token == token)
            || self.product_admission.is_some_and(|product_admission| {
                product_admission.token == token && !product_admission.complete
            })
    }

    fn has_reservation(&self) -> bool {
        self.reservation.is_some()
    }

    #[allow(clippy::too_many_arguments)]
    fn reserve(
        &mut self,
        stream_id: StreamId,
        path_instance: RelayPathInstance,
        token: u64,
        valid_after: Instant,
        expires_at: Instant,
        publication_expires_at: Instant,
        train_bytes: u64,
        ticket: CapacityProbeCommandTicket,
    ) {
        debug_assert!(self.reservation.is_none());
        self.reservation = Some(RequestQuicCapacityProbeReservation {
            stream_id,
            path_instance,
            token,
            valid_after,
            expires_at,
            publication_expires_at,
            train_bytes,
            ticket,
        });
    }

    pub(in crate::runtime::path) fn maintain(&mut self, now: Instant) {
        if self.reservation.as_ref().is_some_and(|reservation| {
            !reservation.ticket.is_current() || now >= reservation.publication_expires_at
        }) && let Some(reservation) = self.reservation.take()
        {
            reservation.ticket.cancel();
        }
        if let Some(expired_token) = self
            .proof
            .and_then(|proof| (now >= proof.candidate.expires_at).then_some(proof.candidate.token))
        {
            self.proof = None;
            if self.product_admission.is_some_and(|product_admission| {
                product_admission.token == expired_token && !product_admission.complete
            }) {
                self.product_admission = None;
            }
        }
    }

    pub(in crate::runtime::path) fn reset_after_data_plane_failure(&mut self) {
        if let Some(reservation) = self.reservation.take() {
            reservation.ticket.cancel();
        }
        self.proof = None;
        self.product_admission = None;
    }

    fn rollback_token(&mut self, token: u64) {
        if self
            .reservation
            .as_ref()
            .is_some_and(|reservation| reservation.token == token)
            && let Some(reservation) = self.reservation.take()
        {
            reservation.ticket.cancel();
        }
        if self.product_admission.is_some_and(|product_admission| {
            product_admission.token == token && !product_admission.complete
        }) {
            self.product_admission = None;
            if self
                .proof
                .is_some_and(|proof| proof.candidate.token == token)
            {
                self.proof = None;
            }
        }
    }

    fn accept_proof(
        &mut self,
        candidate: QuicCapacityProofCandidate,
        probe: quic_transport::MeasurementMetrics,
        now: Instant,
    ) -> Option<(u64, u64, bool)> {
        let reservation = self.reservation.as_ref()?;
        if candidate.token != reservation.token
            || probe.token != candidate.token
            || candidate.train_bytes != reservation.train_bytes
            || candidate.accepted_at < reservation.valid_after
            || candidate.accepted_at >= reservation.expires_at
            || now >= candidate.expires_at
        {
            return None;
        }
        let reservation = self.reservation.take()?;
        // Publication wins cancellation before proof and product admission become visible.
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
        self.proof = Some(RequestQuicCapacityProof {
            candidate,
            rate_bps,
            rate_sample_bytes,
        });
        self.product_admission = Some(RequestQuicCapacityProductAdmission {
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
            completed_at: None,
            rate_prior_expires_at: None,
        });
        Some((rate_bps, rate_sample_bytes, native_tail_rate))
    }

    fn record_product_ack(
        &mut self,
        stream_id: StreamId,
        path_instance: RelayPathInstance,
        bytes: usize,
        sent_at: Instant,
        acked_at: Instant,
    ) -> Option<u64> {
        let product_admission = self.product_admission.as_mut()?;
        product_admission.record_product_ack(stream_id, path_instance, bytes, sent_at, acked_at);
        product_admission
            .complete
            .then_some(product_admission.token)
    }

    pub(in crate::runtime::path) fn observation_at(
        &self,
        now: Instant,
        has_durable_native_window: bool,
    ) -> RequestQuicCapacityObservation {
        let proof = self
            .proof
            .filter(|proof| proof.candidate.accepted_at <= now && now < proof.candidate.expires_at)
            .map(|proof| RequestQuicCapacityEvidence {
                rate_bps: proof.rate_bps,
                rate_sample_bytes: proof.rate_sample_bytes,
                accepted_at: proof.candidate.accepted_at,
            });
        let completed_admission = self.product_admission.filter(|product_admission| {
            product_admission.complete && product_admission.completed_at.is_some_and(|at| at <= now)
        });
        let product_admission_prior = completed_admission
            .filter(|product_admission| {
                proof.is_none()
                    && product_admission.rate_prior_fresh(now)
                    && !has_durable_native_window
            })
            .map(|product_admission| RequestQuicCapacityEvidence {
                rate_bps: product_admission.rate_bps,
                rate_sample_bytes: product_admission.rate_sample_bytes,
                accepted_at: product_admission.accepted_at,
            });
        RequestQuicCapacityObservation {
            proof,
            product_admission_prior,
            product_admission_complete: completed_admission.is_some(),
        }
    }

    pub(in crate::runtime::path) fn reconciliation_at(
        &self,
        stream_id: StreamId,
        path_instance: RelayPathInstance,
        token: u64,
        now: Instant,
    ) -> RequestQuicCapacityReconciliation {
        let exact_admission = self.product_admission.filter(|product_admission| {
            product_admission.stream_id == stream_id
                && product_admission.path_instance == path_instance
                && product_admission.token == token
                && product_admission.accepted_at <= now
        });
        let carrier_proven = self.proof.is_some_and(|proof| {
            proof.candidate.token == token
                && proof.candidate.accepted_at <= now
                && now < proof.candidate.expires_at
        }) && exact_admission.is_some();
        let product_admission = match exact_admission {
            Some(product_admission)
                if product_admission.complete
                    && product_admission.completed_at.is_some_and(|at| at <= now) =>
            {
                RequestQuicCapacityProductAdmissionState::Complete
            }
            Some(product_admission) if now < product_admission.expires_at => {
                RequestQuicCapacityProductAdmissionState::Pending
            }
            _ => RequestQuicCapacityProductAdmissionState::Absent,
        };
        RequestQuicCapacityReconciliation {
            carrier_proven,
            product_admission,
        }
    }

    #[cfg(test)]
    fn product_admission_state(&self, token: u64) -> RequestQuicCapacityProductAdmissionState {
        match self.product_admission {
            Some(product_admission)
                if product_admission.token == token && product_admission.complete =>
            {
                RequestQuicCapacityProductAdmissionState::Complete
            }
            Some(product_admission) if product_admission.token == token => {
                RequestQuicCapacityProductAdmissionState::Pending
            }
            _ => RequestQuicCapacityProductAdmissionState::Absent,
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
            .health()
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(self.path_index)
        {
            record.quic_capacity.rollback_token(self.token);
        }
        if !self.committed {
            self.path_state
                .request_quic_capacity_probe_session()
                .refund(self.path_index, self.bytes);
            self.campaign.refund(self.bytes);
        }
        self.path_state
            .request_quic_capacity_probe_session()
            .retire(self.token);
    }
}

impl ClientPathHealthRecord {
    pub(in crate::runtime) fn accept_request_quic_capacity_proof(
        &mut self,
        candidate: QuicCapacityProofCandidate,
        probe: quic_transport::MeasurementMetrics,
        now: Instant,
    ) -> Option<(u64, u64, bool)> {
        self.quic_capacity.accept_proof(candidate, probe, now)
    }
}

impl ClientPathState {
    pub(in crate::runtime::path) fn retire_request_quic_capacity_probe_token(&self, token: u64) {
        self.request_quic_capacity_probe_session().retire(token);
    }

    pub(in crate::runtime::path) fn request_quic_capacity_probe_remaining_bytes(
        &self,
        session_limit: u64,
    ) -> u64 {
        self.request_quic_capacity_probe_session()
            .remaining_bytes(session_limit)
    }

    pub(in crate::runtime::path) fn request_quic_capacity_probe_candidate_share_bytes(
        &self,
        proposed_path_limit: u64,
        session_limit: u64,
    ) -> u64 {
        self.request_quic_capacity_probe_session()
            .candidate_share_bytes(proposed_path_limit, session_limit)
    }

    pub(in crate::runtime::path) fn request_quic_capacity_probe_path_remaining_bytes(
        &self,
        path_index: usize,
        path_limit: u64,
        session_limit: u64,
    ) -> u64 {
        self.request_quic_capacity_probe_session()
            .path_remaining_bytes(path_index, path_limit, session_limit)
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::runtime::path) fn try_reserve_request_quic_capacity_probe(
        self: &Arc<Self>,
        stream_id: StreamId,
        path_index: usize,
        path_instance: RelayPathInstance,
        token: u64,
        train_bytes: u64,
        path_limit_bytes: u64,
        session_limit: u64,
        campaign: Arc<RequestCapacityProbeCampaignBudget>,
        valid_after: Instant,
        expires_at: Instant,
        proof_validity: Duration,
        ticket: CapacityProbeCommandTicket,
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
        let mut health = self.health().lock().expect("client path health lock");
        let session = self.request_quic_capacity_probe_session();
        let active_token = session.active_token();
        if active_token != 0 {
            let transaction_live = health.udp.iter_mut().any(|record| {
                record.maintain(now);
                record.quic_capacity.transaction_live(active_token)
            });
            if !transaction_live {
                session.retire(active_token);
            }
        }
        if !session.claim(token) {
            return None;
        }
        let campaign_limit = session.candidate_share_bytes(path_limit_bytes, session_limit);
        if !campaign.try_reserve(train_bytes, campaign_limit) {
            session.retire(token);
            return None;
        }
        if !session.try_reserve(path_index, train_bytes, path_limit_bytes, session_limit) {
            campaign.refund(train_bytes);
            session.retire(token);
            return None;
        }
        let Some(record) = health.udp.get_mut(path_index) else {
            session.refund(path_index, train_bytes);
            campaign.refund(train_bytes);
            session.retire(token);
            return None;
        };
        if record.quic_capacity.has_reservation() {
            session.refund(path_index, train_bytes);
            campaign.refund(train_bytes);
            session.retire(token);
            return None;
        }
        record.quic_capacity.reserve(
            stream_id,
            path_instance,
            token,
            valid_after,
            expires_at,
            publication_expires_at,
            train_bytes,
            ticket,
        );
        drop(health);
        Some(RequestQuicCapacityProbeLease {
            path_state: self.clone(),
            campaign,
            path_index,
            token,
            bytes: train_bytes,
            committed: false,
        })
    }

    pub(in crate::runtime::path) fn record_request_quic_capacity_product_ack(
        &self,
        stream_id: StreamId,
        path_instance: RelayPathInstance,
        bytes: usize,
        sent_at: Instant,
        acked_at: Instant,
    ) {
        let session = self.request_quic_capacity_probe_session();
        if session.active_token() == 0 {
            // Ordinary QUIC ACKs bypass the health lock when no product admission exists.
            return;
        }
        let completed_token = {
            let mut health = self.health().lock().expect("client path health lock");
            health
                .udp
                .get_mut(path_instance.key.index)
                .and_then(|record| {
                    record.quic_capacity.record_product_ack(
                        stream_id,
                        path_instance,
                        bytes,
                        sent_at,
                        acked_at,
                    )
                })
        };
        if let Some(token) = completed_token {
            session.retire(token);
        }
    }
}

// Carrier I/O below consumes the frozen transaction command but never mutates
// the request controller or shared product-range ownership.
pub(super) async fn udp_path_write_capacity_probe(
    send: &mut UdpPathSendStream,
    probe: &QuicCapacityProbeCommand,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
) -> Result<(), RuntimeError> {
    // Only this entry point can turn a carrier-neutral command into a QUIC ACK
    // epoch; ordinary frame batching must never absorb capacity payloads.
    let chunk_bytes = mux_limits
        .max_payload_bytes
        .min(codec_limits.max_payload_bytes.max(1))
        .min(QUIC_CAPACITY_RECORD_PAYLOAD_BYTES);
    let train_payload_bytes = usize::try_from(probe.train_payload_bytes).map_err(|_| {
        RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::InvalidMeasurement)
    })?;
    if chunk_bytes == 0 || train_payload_bytes == 0 {
        return Err(RuntimeError::QuicCarrier(
            quic_transport::QuicCarrierError::InvalidMeasurement,
        ));
    }

    let mut epoch = quic_transport::begin_measurement(
        send.transport_stream_mut(),
        quic_transport::MeasurementSpec {
            token: probe.measurement_id,
            train_payload_bytes: probe.train_payload_bytes,
            sample_floor_bytes: probe.sample_floor_bytes,
            warmup_carrier_bytes: probe.warmup_carrier_bytes,
            required_timed_carrier_bytes: probe.required_timed_carrier_bytes,
            retention: probe.proof_validity,
            expires_at: probe.expires_at,
        },
    )
    .await?;
    let zero_block = bytes::Bytes::from(vec![0_u8; chunk_bytes.min(train_payload_bytes)]);
    let mut remaining = train_payload_bytes;
    while remaining > 0 {
        let payload_bytes = remaining.min(zero_block.len());
        epoch
            .write_data(
                &Frame::PathCapacityData {
                    path_id: probe.path_id,
                    measurement_id: probe.measurement_id,
                    payload: zero_block.slice(..payload_bytes),
                },
                codec_limits,
            )
            .await?;
        remaining -= payload_bytes;
    }
    epoch
        .finish(
            &Frame::PathCapacityFinish {
                path_id: probe.path_id,
                measurement_id: probe.measurement_id,
                payload_bytes: probe.train_payload_bytes,
            },
            codec_limits,
        )
        .await?;
    Ok(())
}

pub(super) async fn udp_path_write_capacity_receipt(
    send: &mut UdpPathSendStream,
    path_id: PathId,
    measurement_id: u64,
    received_payload_bytes: u64,
    codec_limits: CodecLimits,
) -> Result<(), RuntimeError> {
    // Let QUIC emit the delayed terminal ACK before the application receipt.
    // Otherwise the receipt may overtake transport telemetry and make identical
    // probes alternate between native rate and cold-start average.
    tokio::time::sleep(QUIC_MAX_ACK_DELAY.saturating_add(QUIC_TIMER_GRANULARITY)).await;
    quic_transport::write_measurement_control(
        send.transport_stream_mut(),
        &Frame::PathCapacityReceipt {
            path_id,
            measurement_id,
            received_payload_bytes,
        },
        codec_limits,
    )
    .await?;
    Ok(())
}

pub(super) fn quic_capacity_command_drop_reason(
    probe: &QuicCapacityProbeCommand,
    now: Instant,
) -> Option<&'static str> {
    if !probe.ticket.is_current() {
        Some("ownership_invalidated")
    } else if now >= probe.expires_at {
        Some("deadline_elapsed_before_start")
    } else {
        None
    }
}

pub(super) fn quic_capacity_start_rejection_reason(err: &RuntimeError) -> Option<&'static str> {
    match err {
        RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::InvalidMeasurement) => {
            Some("invalid_specification")
        }
        RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::MeasurementBusy) => {
            Some("carrier_epoch_busy")
        }
        RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::MeasurementNotIdle) => {
            Some("carrier_not_idle")
        }
        RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::MeasurementExpired) => {
            Some("carrier_deadline_elapsed")
        }
        _ => None,
    }
}

#[cfg(test)]
#[path = "capacity_test.rs"]
mod tests;
