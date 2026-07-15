//! Carrier-neutral path health and evidence lifecycle.
//!
//! One record combines liveness, load, product delivery, native carrier
//! observations, and proof epochs so scheduling observes one coherent state.

use super::model::{
    ClientPathObservation, UdpDatagramPathObservation, path_record_failure_cooldown,
};
use super::proof::PathProofObservation;
use super::quic::metrics::UdpPathMetrics;
use super::quic::{RequestQuicCapacityProductHandoffState, RequestQuicCapacityRecord};
use super::tcp::capacity::RequestTcpCapacityRecord;
use super::tcp::metrics::TcpNativeObservation;
use crate::model::capacity::{
    BBR_MAX_SEND_QUANTUM_BYTES, PATH_OPEN_SCORE_BYTES, PathRateSample, TcpCapacityProofCandidate,
};
use crate::model::path::RelayPathInstance;
use crate::scheduler::{FlowLane, PathState as SchedulerPathState};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

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
pub(super) struct RequestQuicCapacityReconciliationObservation {
    pub(super) target: RelayPathInstance,
    pub(super) token: u64,
    pub(super) carrier_proven: bool,
    pub(super) handoff: RequestQuicCapacityProductHandoffState,
}

/// One lock-coherent carrier-authority view for request reconciliation.
/// Controllers consume only exact transaction identities from this snapshot.
pub(in crate::runtime) struct RequestCapacityReconciliationView {
    pub(super) observed_at: Instant,
    pub(super) tcp_proofs: HashMap<RelayPathInstance, TcpCapacityProofCandidate>,
    pub(super) quic: Option<RequestQuicCapacityReconciliationObservation>,
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

    /// Applies time-driven lifecycle transitions. Observation remains pure.
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

    pub(in crate::runtime) fn observation_at(&self, now: Instant) -> ClientPathObservation {
        if self.manual_disabled {
            return ClientPathObservation {
                state: SchedulerPathState::Failed,
                manual_disabled: true,
                measured_srtt_ms: self.measured_srtt_ms,
                measured_jitter_ms: self.measured_jitter_ms,
                measured_rate_bps: self.measured_rate_bps,
                measured_loss_rate: self.measured_loss_rate,
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

    pub(in crate::runtime) fn mark_quic_path_metrics(&mut self, metrics: UdpPathMetrics) {
        if self.manual_disabled {
            return;
        }
        self.state = SchedulerPathState::Active;
        self.consecutive_failures = 0;
        self.failed_until = None;
        if metrics.rtt_observed {
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

#[cfg(test)]
#[path = "health_test.rs"]
mod tests;
