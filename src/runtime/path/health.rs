//! Carrier-neutral path health and evidence lifecycle.
//!
//! One record combines liveness, load, product delivery, native carrier
//! observations, and proof epochs so scheduling observes one coherent state.

use super::model::{
    ClientPathObservation, UdpDatagramPathObservation, path_record_failure_cooldown,
};
use super::proof::PathProofObservation;
use super::quic::metrics::UdpPathMetrics;
use super::tcp::capacity::RequestTcpCapacityRecord;
use super::tcp::metrics::TcpNativeObservation;
use crate::model::capacity::{PathRateSample, RELIABLE_INITIAL_RTT, TcpCapacityProofCandidate};
use crate::model::path::{CarrierPathInstanceId, RelayPathInstance, RelayPathKey};
use crate::model::timing::transport_rate_sample_freshness_horizon;
use crate::protocol::{PathId, PathUsage, UnderlayProtocol};
use crate::scheduler::{PathState as SchedulerPathState, TrafficClass};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub(in crate::runtime) struct ClientPathHealth {
    /// Configured bounded-pool TCP members. Their stable indices are positional.
    pub(in crate::runtime) tcp: Vec<ClientPathHealthRecord>,
    pub(in crate::runtime) udp: Vec<ClientPathHealthRecord>,
}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct ClientPathHealthRecord {
    pub(in crate::runtime) state: SchedulerPathState,
    pub(in crate::runtime) manual_disabled: bool,
    data_plane_failure_instance_id: Option<CarrierPathInstanceId>,
    wire_path_id: Option<PathId>,
    pub(in crate::runtime) peer_usage: Option<PathUsage>,
    path_instance_id: Option<CarrierPathInstanceId>,
    peer_usage_sequence: Option<u64>,
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
    pub(in crate::runtime) delivery_rate_expires_at: Option<Instant>,
    pub(in crate::runtime) failed_until: Option<Instant>,
    pub(in crate::runtime) active_flows: u32,
    pub(in crate::runtime) active_latency_sensitive_flows: u32,
    pub(in crate::runtime) relay_bytes_in_flight: u64,
    pub(in crate::runtime) relay_queue_bytes: u64,
    pub(in crate::runtime) carrier_srtt_ms: Option<f64>,
    pub(in crate::runtime) carrier_rttvar_ms: Option<f64>,
    pub(in crate::runtime) carrier_loss_rate: Option<f64>,
    pub(in crate::runtime) carrier_ecn_rate: Option<f64>,
    pub(in crate::runtime) carrier_delivery_rate_bps: Option<f64>,
    pub(in crate::runtime) carrier_pacing_rate_bps: Option<f64>,
    pub(in crate::runtime) carrier_bytes_in_flight: u64,
    pub(in crate::runtime) carrier_bytes_in_flight_observed: bool,
    pub(in crate::runtime) carrier_queue_bytes: u64,
    pub(in crate::runtime) carrier_queue_bytes_observed: bool,
    pub(in crate::runtime) carrier_inflight_limit_bytes: u64,
    pub(in crate::runtime) native_drain_observed: bool,
    pub(in crate::runtime) carrier_delivery_samples: u32,
    pub(in crate::runtime) carrier_delivery_sample_bytes: u64,
    pub(in crate::runtime) carrier_delivery_window_covered: bool,
    pub(in crate::runtime) carrier_last_delivery_at: Option<Instant>,
    pub(in crate::runtime) carrier_bulk_proof_expires_at: Option<Instant>,
    pub(in crate::runtime) carrier_app_limited: bool,
    pub(in crate::runtime) carrier_ack_derived_data_seen: bool,
    pub(in crate::runtime::path) tcp_capacity: RequestTcpCapacityRecord,
    pub(in crate::runtime) path_proof_success: bool,
    path_proof_generation: u64,
    path_proof_valid_after: Instant,
    successful_path_proofs: HashMap<u64, SuccessfulPathProof>,
    successful_path_proof_order: VecDeque<u64>,
    successful_path_proof_limit: usize,
}

/// Raw rate evidence retained for diagnostics after its placement authority
/// expires. Every field is copied from the health record without applying a
/// freshness policy or fabricating evidence that the source does not expose.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::runtime) struct ClientPathRateDiagnostics {
    pub(in crate::runtime) measured_rate_bps: Option<f64>,
    pub(in crate::runtime) product_delivery_rate_bps: Option<f64>,
    pub(in crate::runtime) delivery_samples: u32,
    pub(in crate::runtime) product_delivery_sample_bytes: u64,
    pub(in crate::runtime) datagram_feedback_samples: u32,
    pub(in crate::runtime) last_delivery_at: Option<Instant>,
    pub(in crate::runtime) delivery_rate_expires_at: Option<Instant>,
    pub(in crate::runtime) carrier_delivery_rate_bps: Option<f64>,
    pub(in crate::runtime) carrier_pacing_rate_bps: Option<f64>,
    pub(in crate::runtime) carrier_delivery_samples: u32,
    pub(in crate::runtime) carrier_delivery_sample_bytes: u64,
    pub(in crate::runtime) carrier_delivery_window_covered: bool,
    pub(in crate::runtime) carrier_last_delivery_at: Option<Instant>,
    pub(in crate::runtime) carrier_bulk_proof_expires_at: Option<Instant>,
    pub(in crate::runtime) carrier_app_limited: bool,
    pub(in crate::runtime) carrier_ack_derived_data_seen: bool,
}

/// Non-queue facts that decide whether one configured carrier may provide
/// ordinary Product service. Rate, RTT, loss, proof, load, queue, and flight
/// evidence are intentionally excluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(test)]
pub(in crate::runtime) struct ClientPathEligibilityFingerprint {
    state: SchedulerPathState,
    manual_disabled: bool,
    path_instance_id: Option<CarrierPathInstanceId>,
    data_plane_failure_instance_id: Option<CarrierPathInstanceId>,
    peer_usage: Option<PathUsage>,
}

/// One lock-coherent carrier-authority view for request reconciliation.
/// Controllers consume only exact transaction identities from this snapshot.
pub(in crate::runtime) struct RequestCapacityReconciliationView {
    pub(super) observed_at: Instant,
    pub(super) tcp_proofs: HashMap<RelayPathInstance, TcpCapacityProofCandidate>,
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
            data_plane_failure_instance_id: None,
            wire_path_id: None,
            peer_usage: None,
            path_instance_id: None,
            peer_usage_sequence: None,
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
            delivery_rate_expires_at: None,
            failed_until: None,
            active_flows: 0,
            active_latency_sensitive_flows: 0,
            relay_bytes_in_flight: 0,
            relay_queue_bytes: 0,
            carrier_srtt_ms: None,
            carrier_rttvar_ms: None,
            carrier_loss_rate: None,
            carrier_ecn_rate: None,
            carrier_delivery_rate_bps: None,
            carrier_pacing_rate_bps: None,
            carrier_bytes_in_flight: 0,
            carrier_bytes_in_flight_observed: false,
            carrier_queue_bytes: 0,
            carrier_queue_bytes_observed: false,
            carrier_inflight_limit_bytes: 0,
            native_drain_observed: false,
            carrier_delivery_samples: 0,
            carrier_delivery_sample_bytes: 0,
            carrier_delivery_window_covered: false,
            carrier_last_delivery_at: None,
            carrier_bulk_proof_expires_at: None,
            carrier_app_limited: true,
            carrier_ack_derived_data_seen: false,
            tcp_capacity: RequestTcpCapacityRecord::default(),
            path_proof_success: false,
            path_proof_generation: 0,
            path_proof_valid_after: Instant::now(),
            successful_path_proofs: HashMap::new(),
            successful_path_proof_order: VecDeque::new(),
            successful_path_proof_limit: 1,
        }
    }
}

impl ClientPathHealth {
    pub(in crate::runtime) fn new(
        tcp: Vec<ClientPathHealthRecord>,
        udp: Vec<ClientPathHealthRecord>,
    ) -> Self {
        Self { tcp, udp }
    }

    pub(in crate::runtime) fn is_product_quiescent(&self) -> bool {
        self.tcp_records()
            .chain(&self.udp)
            .all(ClientPathHealthRecord::has_no_product_work)
    }

    pub(in crate::runtime) fn tcp_record(&self, index: usize) -> Option<&ClientPathHealthRecord> {
        self.tcp.get(index)
    }

    pub(in crate::runtime) fn tcp_record_mut(
        &mut self,
        index: usize,
    ) -> Option<&mut ClientPathHealthRecord> {
        self.tcp.get_mut(index)
    }

    pub(in crate::runtime) fn path_record(
        &self,
        key: RelayPathKey,
    ) -> Option<&ClientPathHealthRecord> {
        match key.underlay {
            UnderlayProtocol::Tcp => self.tcp_record(key.index),
            UnderlayProtocol::Udp => self.udp.get(key.index),
        }
    }

    pub(in crate::runtime) fn path_record_mut(
        &mut self,
        key: RelayPathKey,
    ) -> Option<&mut ClientPathHealthRecord> {
        match key.underlay {
            UnderlayProtocol::Tcp => self.tcp_record_mut(key.index),
            UnderlayProtocol::Udp => self.udp.get_mut(key.index),
        }
    }

    pub(in crate::runtime) fn tcp_records(&self) -> impl Iterator<Item = &ClientPathHealthRecord> {
        self.tcp.iter()
    }

    pub(in crate::runtime) fn tcp_records_mut(
        &mut self,
    ) -> impl Iterator<Item = &mut ClientPathHealthRecord> {
        self.tcp.iter_mut()
    }

    pub(in crate::runtime) fn tcp_records_with_indices(
        &self,
    ) -> impl Iterator<Item = (usize, &ClientPathHealthRecord)> {
        self.tcp.iter().enumerate()
    }

    pub(in crate::runtime) fn tcp_records_have_schedulable_alternative(
        &self,
        excluded_index: usize,
        now: Instant,
    ) -> bool {
        self.tcp_records_with_indices().any(|(index, record)| {
            index != excluded_index
                && !matches!(
                    record.observation_at(now).state,
                    SchedulerPathState::Failed | SchedulerPathState::Draining
                )
        })
    }
}

impl ClientPathHealthRecord {
    pub(in crate::runtime) fn rate_diagnostics(&self) -> ClientPathRateDiagnostics {
        ClientPathRateDiagnostics {
            measured_rate_bps: self.measured_rate_bps,
            product_delivery_rate_bps: self.product_delivery_rate_bps,
            delivery_samples: self.delivery_samples,
            product_delivery_sample_bytes: self.product_delivery_sample_bytes,
            datagram_feedback_samples: self.datagram_feedback_samples,
            last_delivery_at: self.last_delivery_at,
            delivery_rate_expires_at: self.delivery_rate_expires_at,
            carrier_delivery_rate_bps: self.carrier_delivery_rate_bps,
            carrier_pacing_rate_bps: self.carrier_pacing_rate_bps,
            carrier_delivery_samples: self.carrier_delivery_samples,
            carrier_delivery_sample_bytes: self.carrier_delivery_sample_bytes,
            carrier_delivery_window_covered: self.carrier_delivery_window_covered,
            carrier_last_delivery_at: self.carrier_last_delivery_at,
            carrier_bulk_proof_expires_at: self.carrier_bulk_proof_expires_at,
            carrier_app_limited: self.carrier_app_limited,
            carrier_ack_derived_data_seen: self.carrier_ack_derived_data_seen,
        }
    }

    pub(in crate::runtime) fn wire_path_id(&self) -> Option<PathId> {
        self.wire_path_id
    }

    pub(in crate::runtime) fn path_instance_id(&self) -> Option<CarrierPathInstanceId> {
        self.path_instance_id
    }

    #[cfg(test)]
    pub(in crate::runtime) fn eligibility_fingerprint(&self) -> ClientPathEligibilityFingerprint {
        ClientPathEligibilityFingerprint {
            state: self.state,
            manual_disabled: self.manual_disabled,
            path_instance_id: self.path_instance_id,
            data_plane_failure_instance_id: self.data_plane_failure_instance_id,
            peer_usage: self.peer_usage,
        }
    }

    pub(in crate::runtime) fn install_peer_usage(
        &mut self,
        path_instance_id: CarrierPathInstanceId,
        sequence: u64,
        usage: PathUsage,
    ) {
        if self.path_instance_id != Some(path_instance_id) {
            self.clear_physical_carrier_state();
        }
        self.path_instance_id = Some(path_instance_id);
        self.peer_usage_sequence = Some(sequence);
        self.peer_usage = Some(usage);
        self.mark_liveness_success();
    }

    pub(in crate::runtime) fn install_tcp_peer_usage(
        &mut self,
        wire_path_id: PathId,
        path_instance_id: CarrierPathInstanceId,
        sequence: u64,
        usage: PathUsage,
    ) {
        self.install_peer_usage(path_instance_id, sequence, usage);
        self.wire_path_id = Some(wire_path_id);
    }

    pub(in crate::runtime) fn begin_planned_retirement(&mut self) {
        if self.path_instance_id.is_some() && self.state != SchedulerPathState::Draining {
            self.state = SchedulerPathState::Draining;
            self.failed_until = None;
            self.invalidate_path_proofs();
        }
    }

    pub(in crate::runtime) fn begin_planned_instance_retirement(
        &mut self,
        path_instance_id: CarrierPathInstanceId,
    ) -> bool {
        if self.path_instance_id != Some(path_instance_id) {
            return false;
        }
        self.begin_planned_retirement();
        true
    }

    pub(in crate::runtime) fn retire_planned_instance(
        &mut self,
        path_instance_id: CarrierPathInstanceId,
    ) -> bool {
        if self.path_instance_id != Some(path_instance_id) {
            return false;
        }
        self.clear_physical_carrier_state();
        true
    }

    pub(in crate::runtime) fn has_physical_carrier(&self) -> bool {
        self.path_instance_id.is_some()
    }

    pub(in crate::runtime) fn has_live_authenticated_carrier(&self) -> bool {
        self.path_instance_id.is_some()
            && self.data_plane_failure_instance_id != self.path_instance_id
    }

    pub(in crate::runtime) fn accepts_endpoint_failure_for_expected_owner(
        &self,
        expected_path_instance_id: Option<CarrierPathInstanceId>,
    ) -> bool {
        match expected_path_instance_id {
            Some(expected) => {
                self.path_instance_id == Some(expected)
                    && self.data_plane_failure_instance_id != Some(expected)
            }
            None => self.path_instance_id.is_none(),
        }
    }

    /// Whether this exact authenticated physical instance may become a
    /// Product owner. This is lifecycle state only; sampled telemetry is not
    /// part of the commit decision.
    pub(in crate::runtime) fn accepts_product_commit(
        &self,
        path_instance_id: CarrierPathInstanceId,
    ) -> bool {
        !self.manual_disabled
            && self.path_instance_id == Some(path_instance_id)
            && self.data_plane_failure_instance_id != Some(path_instance_id)
            && !matches!(
                self.state,
                SchedulerPathState::Failed | SchedulerPathState::Draining
            )
    }

    /// Product admission owns these counters from before carrier I/O until the
    /// exact logical attachment releases them. Planned physical replacement
    /// may therefore use this lock-coherent boundary without an idle timer.
    pub(in crate::runtime) fn is_product_quiescent_for_instance(
        &self,
        path_instance_id: CarrierPathInstanceId,
    ) -> bool {
        self.path_instance_id == Some(path_instance_id)
            && self.data_plane_failure_instance_id != Some(path_instance_id)
            && !self.manual_disabled
            && matches!(
                self.state,
                SchedulerPathState::Active | SchedulerPathState::Suspect
            )
            && self.active_flows == 0
            && self.relay_bytes_in_flight == 0
            && self.relay_queue_bytes == 0
    }

    fn has_no_product_work(&self) -> bool {
        self.active_flows == 0 && self.relay_bytes_in_flight == 0 && self.relay_queue_bytes == 0
    }

    pub(in crate::runtime) fn update_peer_usage(
        &mut self,
        path_instance_id: CarrierPathInstanceId,
        sequence: u64,
        usage: PathUsage,
    ) -> bool {
        if self.path_instance_id != Some(path_instance_id) {
            return false;
        }
        if self
            .peer_usage_sequence
            .is_some_and(|current| sequence <= current)
        {
            return false;
        }
        self.peer_usage_sequence = Some(sequence);
        self.peer_usage = Some(usage);
        true
    }

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

    fn rate_sample_freshness_horizon(&self) -> Duration {
        let srtt = Duration::from_secs_f64(
            self.carrier_srtt_ms
                .or(self.measured_srtt_ms)
                .unwrap_or(RELIABLE_INITIAL_RTT.as_secs_f64() * 1_000.0)
                .max(0.001)
                / 1_000.0,
        );
        let rttvar = self
            .carrier_rttvar_ms
            .or(self.measured_jitter_ms)
            .map(|rttvar_ms| Duration::from_secs_f64(rttvar_ms.max(0.0) / 1_000.0))
            .unwrap_or_default()
            .max(srtt / 8);
        transport_rate_sample_freshness_horizon(srtt, rttvar)
    }

    fn delivery_rate_evidence_is_fresh_at(&self, now: Instant) -> bool {
        self.last_delivery_at
            .zip(self.delivery_rate_expires_at)
            .is_some_and(|(sample_at, expires_at)| sample_at <= now && now < expires_at)
    }

    fn carrier_rate_evidence_is_fresh_at(&self, now: Instant) -> bool {
        self.carrier_bulk_proof_expires_at
            .zip(self.carrier_last_delivery_at)
            .is_some_and(|(expires_at, sample_at)| sample_at <= now && now < expires_at)
    }

    fn clear_delivery_rate_evidence(&mut self) {
        self.measured_rate_bps = None;
        self.delivery_samples = 0;
        self.product_delivery_rate_bps = None;
        self.product_delivery_sample_bytes = 0;
        self.datagram_feedback_samples = 0;
        self.last_delivery_at = None;
        self.delivery_rate_expires_at = None;
    }

    fn clear_native_delivery_rate_evidence(&mut self) {
        self.carrier_delivery_rate_bps = None;
        self.carrier_pacing_rate_bps = None;
        self.carrier_delivery_samples = 0;
        self.carrier_delivery_sample_bytes = 0;
        self.carrier_delivery_window_covered = false;
        self.carrier_last_delivery_at = None;
        self.carrier_bulk_proof_expires_at = None;
        self.carrier_app_limited = true;
        self.carrier_ack_derived_data_seen = false;
    }

    pub(in crate::runtime) fn mark_tcp_transport_state(
        &mut self,
        path_instance_id: CarrierPathInstanceId,
        observation: TcpNativeObservation,
    ) -> bool {
        self.mark_tcp_transport_state_at(path_instance_id, observation, Instant::now())
    }

    fn mark_tcp_transport_state_at(
        &mut self,
        path_instance_id: CarrierPathInstanceId,
        observation: TcpNativeObservation,
        now: Instant,
    ) -> bool {
        if !self.accepts_native_carrier_observation(path_instance_id) {
            return false;
        }
        self.mark_liveness_success();
        self.native_drain_observed = observation.has_native_drain_evidence();
        // TCP owns congestion and delivery measurement. MPP retains these
        // same-socket samples for ranking without turning them into Data ACKs.
        if let Some(srtt_us) = observation.srtt_us() {
            self.carrier_srtt_ms = Some(f64::from(srtt_us.max(1)) / 1_000.0);
        }
        if let Some(rttvar_us) = observation.rttvar_us() {
            self.carrier_rttvar_ms = Some(f64::from(rttvar_us) / 1_000.0);
        }
        self.carrier_bytes_in_flight_observed = observation.bytes_in_flight().is_some();
        self.carrier_queue_bytes_observed = observation.queue_bytes().is_some();
        if let Some(bytes_in_flight) = observation.bytes_in_flight() {
            self.carrier_bytes_in_flight = bytes_in_flight;
        }
        if let Some(inflight_limit_bytes) = observation.inflight_limit_bytes() {
            self.carrier_inflight_limit_bytes = inflight_limit_bytes;
        }
        if let Some(queue_bytes) = observation.queue_bytes() {
            self.carrier_queue_bytes = queue_bytes;
        }
        self.carrier_loss_rate = observation
            .loss_ppm()
            .map(|loss_ppm| f64::from(loss_ppm) / 1_000_000.0);
        if observation.app_limited() == Some(false)
            && let Some(newly_acked_bytes) =
                observation.newly_acked_bytes().filter(|bytes| *bytes > 0)
            && let Some(delivery_rate_bps) = observation.delivery_rate_bps()
        {
            if self.carrier_last_delivery_at.is_some()
                && !self.carrier_rate_evidence_is_fresh_at(now)
            {
                // A fresh ACK epoch must earn its own coverage and confidence;
                // one post-idle ACK cannot revive accumulated stale capacity.
                self.clear_native_delivery_rate_evidence();
            }
            self.carrier_delivery_rate_bps = Some(delivery_rate_bps as f64);
            self.carrier_app_limited = false;
            // Pacing is an optional member of this exact native-delivery
            // epoch. A new qualifying sample that omits it must not relabel a
            // prior epoch's pacing value with the new sample time and expiry.
            self.carrier_pacing_rate_bps = observation
                .pacing_rate_bps()
                .filter(|rate| *rate > 0)
                .map(|pacing_rate_bps| pacing_rate_bps as f64);
            self.carrier_delivery_samples = self.carrier_delivery_samples.saturating_add(1);
            self.carrier_delivery_sample_bytes = self
                .carrier_delivery_sample_bytes
                .saturating_add(newly_acked_bytes);
            self.carrier_delivery_window_covered |= observation.delivery_window_covered();
            self.carrier_last_delivery_at = Some(now);
            self.carrier_bulk_proof_expires_at = Some(
                now.checked_add(self.rate_sample_freshness_horizon())
                    .unwrap_or(now),
            );
        }
        true
    }

    /// Applies time-driven lifecycle transitions. Observation remains pure.
    pub(in crate::runtime) fn maintain(&mut self, now: Instant) {
        self.tcp_capacity.maintain(now);
        if self.state == SchedulerPathState::Failed
            && self.failed_until.is_some_and(|deadline| now >= deadline)
        {
            self.state = SchedulerPathState::Suspect;
            self.failed_until = None;
        }
    }

    pub(in crate::runtime) fn observation_at(&self, now: Instant) -> ClientPathObservation {
        let state = if self.manual_disabled {
            SchedulerPathState::Failed
        } else if self.state == SchedulerPathState::Failed
            && self.failed_until.is_some_and(|deadline| now >= deadline)
        {
            SchedulerPathState::Suspect
        } else {
            self.state
        };
        let tcp_proof = (!self.manual_disabled)
            .then(|| self.tcp_capacity.proof_candidate_at(now))
            .flatten();
        let proof_rate_bps = tcp_proof.map(|proof| proof.rate_bps as f64);
        let proof_sample_bytes = tcp_proof.map(|proof| proof.rate_sample_bytes);
        let proof_accepted_at = tcp_proof.map(|proof| proof.accepted_at);
        let proof_expires_at = tcp_proof.map(|proof| proof.expires_at);
        let explicit_carrier_capacity_proof = proof_rate_bps.is_some();
        let delivery_rate_fresh = self.delivery_rate_evidence_is_fresh_at(now);
        let carrier_rate_fresh = self.carrier_rate_evidence_is_fresh_at(now);
        let fresh_carrier_rate = carrier_rate_fresh
            .then_some(self.carrier_delivery_rate_bps)
            .flatten();
        let fresh_carrier_pacing = carrier_rate_fresh
            .then_some(self.carrier_pacing_rate_bps)
            .flatten();
        let fresh_carrier_samples = if carrier_rate_fresh {
            self.carrier_delivery_samples
        } else {
            0
        };
        let fresh_carrier_sample_bytes = if carrier_rate_fresh {
            self.carrier_delivery_sample_bytes
        } else {
            0
        };
        ClientPathObservation {
            state,
            manual_disabled: self.manual_disabled,
            wire_path_id: self.wire_path_id,
            peer_usage: self.peer_usage,
            measured_srtt_ms: self.measured_srtt_ms,
            measured_jitter_ms: self.measured_jitter_ms,
            measured_rate_bps: delivery_rate_fresh
                .then_some(self.measured_rate_bps)
                .flatten(),
            measured_loss_rate: self.measured_loss_rate,
            delivery_samples: if delivery_rate_fresh {
                self.delivery_samples
            } else {
                0
            },
            product_delivery_rate_bps: delivery_rate_fresh
                .then_some(self.product_delivery_rate_bps)
                .flatten(),
            product_delivery_sample_bytes: if delivery_rate_fresh {
                self.product_delivery_sample_bytes
            } else {
                0
            },
            datagram_feedback_samples: if delivery_rate_fresh {
                self.datagram_feedback_samples
            } else {
                0
            },
            last_delivery_at: delivery_rate_fresh
                .then_some(self.last_delivery_at)
                .flatten(),
            delivery_rate_expires_at: delivery_rate_fresh
                .then_some(self.delivery_rate_expires_at)
                .flatten(),
            active_flows: self.active_flows,
            active_latency_sensitive_flows: self.active_latency_sensitive_flows,
            relay_bytes_in_flight: self.relay_bytes_in_flight,
            relay_queue_bytes: self.relay_queue_bytes,
            carrier_srtt_ms: self.carrier_srtt_ms,
            carrier_rttvar_ms: self.carrier_rttvar_ms,
            carrier_loss_rate: self.carrier_loss_rate,
            carrier_ecn_rate: self.carrier_ecn_rate,
            carrier_delivery_rate_bps: proof_rate_bps.or(fresh_carrier_rate),
            carrier_pacing_rate_bps: fresh_carrier_pacing,
            carrier_bytes_in_flight: self.carrier_bytes_in_flight,
            carrier_bytes_in_flight_observed: self.carrier_bytes_in_flight_observed,
            carrier_queue_bytes: self.carrier_queue_bytes,
            carrier_queue_bytes_observed: self.carrier_queue_bytes_observed,
            carrier_inflight_limit_bytes: self.carrier_inflight_limit_bytes,
            carrier_delivery_samples: if explicit_carrier_capacity_proof {
                fresh_carrier_samples.max(1)
            } else {
                fresh_carrier_samples
            },
            carrier_delivery_sample_bytes: proof_sample_bytes
                .map_or(fresh_carrier_sample_bytes, |sample_bytes| {
                    fresh_carrier_sample_bytes.max(sample_bytes)
                }),
            carrier_delivery_window_covered: carrier_rate_fresh
                && self.carrier_delivery_window_covered,
            carrier_last_delivery_at: proof_accepted_at.or_else(|| {
                carrier_rate_fresh
                    .then_some(self.carrier_last_delivery_at)
                    .flatten()
            }),
            carrier_bulk_proof_expires_at: proof_expires_at.or_else(|| {
                carrier_rate_fresh
                    .then_some(self.carrier_bulk_proof_expires_at)
                    .flatten()
                    .filter(|expires_at| *expires_at > now)
            }),
            carrier_app_limited: !explicit_carrier_capacity_proof
                && (!carrier_rate_fresh || self.carrier_app_limited),
            carrier_ack_derived_data_seen: explicit_carrier_capacity_proof
                || (carrier_rate_fresh && self.carrier_ack_derived_data_seen),
            explicit_carrier_capacity_proof,
            path_proof_success: self.path_proof_success,
        }
    }

    /// Returns evidence only while it still belongs to the requested physical
    /// carrier. A stable configured path may publish a replacement instance at
    /// any time, so attachment owners must never infer this identity from its
    /// current record after opening.
    pub(in crate::runtime) fn observation_for_instance_at(
        &self,
        path_instance_id: CarrierPathInstanceId,
        now: Instant,
    ) -> Option<ClientPathObservation> {
        (self.path_instance_id == Some(path_instance_id)
            && self.data_plane_failure_instance_id != Some(path_instance_id))
        .then(|| self.observation_at(now))
    }

    pub(in crate::runtime) fn mark_success(&mut self, elapsed: Duration) {
        if self.manual_disabled {
            return;
        }
        self.record_success(elapsed);
    }

    pub(in crate::runtime) fn mark_success_for_instance(
        &mut self,
        path_instance_id: CarrierPathInstanceId,
        elapsed: Duration,
    ) -> bool {
        if !self.accepts_liveness_sample(path_instance_id) {
            return false;
        }
        self.mark_success(elapsed);
        true
    }

    fn record_success(&mut self, elapsed: Duration) {
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

    #[cfg(test)]
    pub(in crate::runtime) fn mark_open_success(&mut self, _elapsed: Duration, lane: TrafficClass) {
        if self.manual_disabled {
            return;
        }
        self.mark_liveness_success();
        self.active_flows = self.active_flows.saturating_add(1);
        if lane.is_latency_sensitive() {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_open_success_for_instance(
        &mut self,
        path_instance_id: CarrierPathInstanceId,
        elapsed: Duration,
        lane: TrafficClass,
    ) -> bool {
        if !self.accepts_liveness_sample(path_instance_id) {
            return false;
        }
        self.mark_open_success(elapsed, lane);
        true
    }

    pub(in crate::runtime) fn reserve_load(&mut self, lane: TrafficClass, now: Instant) -> bool {
        // Selection may precede an asynchronous open. Revalidate at the
        // reservation commit point so a concurrent disable or failure cannot
        // publish load onto a path that is no longer schedulable.
        self.maintain(now);
        if self.manual_disabled
            || !matches!(
                self.state,
                SchedulerPathState::Active | SchedulerPathState::Suspect
            )
        {
            return false;
        }
        self.active_flows = self.active_flows.saturating_add(1);
        if lane.is_latency_sensitive() {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
        true
    }

    pub(in crate::runtime) fn mark_reserved_open_success(&mut self, _elapsed: Duration) {
        self.mark_liveness_success();
    }

    pub(in crate::runtime) fn mark_reserved_open_success_for_instance(
        &mut self,
        path_instance_id: CarrierPathInstanceId,
        elapsed: Duration,
    ) -> bool {
        if !self.accepts_liveness_sample(path_instance_id) {
            return false;
        }
        self.mark_reserved_open_success(elapsed);
        true
    }

    pub(in crate::runtime) fn release_load(&mut self, lane: TrafficClass) {
        self.active_flows = self.active_flows.saturating_sub(1);
        if lane.is_latency_sensitive() {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_sub(1);
        }
    }

    pub(in crate::runtime) fn change_lane_load(&mut self, from: TrafficClass, to: TrafficClass) {
        if from.is_latency_sensitive() && !to.is_latency_sensitive() {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_sub(1);
        } else if !from.is_latency_sensitive() && to.is_latency_sensitive() {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
    }

    fn prepare_delivery_rate_sample(&mut self, now: Instant) {
        if self.last_delivery_at.is_some() && !self.delivery_rate_evidence_is_fresh_at(now) {
            // Product and generic samples share one delivery epoch. Reset every
            // provenance counter together so a small post-idle sample cannot
            // revive an old rate, byte floor, or confidence count.
            self.clear_delivery_rate_evidence();
        }
    }

    fn mark_delivery_at(&mut self, sample: PathRateSample, now: Instant) {
        self.mark_delivery_at_with_expiry(sample, now, None);
    }

    fn mark_delivery_at_with_expiry(
        &mut self,
        sample: PathRateSample,
        now: Instant,
        explicit_expires_at: Option<Instant>,
    ) {
        self.mark_liveness_success();
        self.delivery_samples = self.delivery_samples.saturating_add(1);
        self.last_delivery_at = Some(now);
        self.delivery_rate_expires_at =
            explicit_expires_at.or_else(|| now.checked_add(self.rate_sample_freshness_horizon()));
        let sample_bps = sample.rate_bps();
        self.measured_rate_bps = Some(match self.measured_rate_bps {
            Some(previous) => previous.mul_add(0.75, sample_bps * 0.25),
            None => sample_bps,
        });
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_delivery(&mut self, sample: PathRateSample) {
        if self.manual_disabled {
            return;
        }
        let now = Instant::now();
        self.prepare_delivery_rate_sample(now);
        self.mark_delivery_at(sample, now);
    }

    pub(in crate::runtime) fn mark_product_delivery(&mut self, sample: PathRateSample) {
        if self.manual_disabled {
            return;
        }
        let now = Instant::now();
        self.prepare_delivery_rate_sample(now);
        let sample_bps = sample.rate_bps();
        self.product_delivery_rate_bps = Some(match self.product_delivery_rate_bps {
            Some(previous) => previous.mul_add(0.75, sample_bps * 0.25),
            None => sample_bps,
        });
        self.product_delivery_sample_bytes = self
            .product_delivery_sample_bytes
            .saturating_add(sample.bytes());
        self.mark_delivery_at(sample, now);
    }

    pub(in crate::runtime) fn mark_product_delivery_for_instance(
        &mut self,
        path_instance_id: CarrierPathInstanceId,
        sample: PathRateSample,
    ) {
        if self.accepts_native_carrier_observation(path_instance_id) {
            self.mark_product_delivery(sample);
        }
    }

    pub(in crate::runtime) fn mark_product_delivery_replacing_rate(
        &mut self,
        sample: PathRateSample,
    ) {
        if self.manual_disabled {
            return;
        }
        let now = Instant::now();
        self.prepare_delivery_rate_sample(now);
        self.product_delivery_sample_bytes = self
            .product_delivery_sample_bytes
            .saturating_add(sample.bytes());
        self.product_delivery_rate_bps = Some(sample.rate_bps());
        self.mark_liveness_success();
        self.delivery_samples = self.delivery_samples.saturating_add(1);
        self.last_delivery_at = Some(now);
        self.delivery_rate_expires_at = now.checked_add(self.rate_sample_freshness_horizon());
        self.measured_rate_bps = Some(sample.rate_bps());
    }

    pub(in crate::runtime) fn mark_product_delivery_replacing_rate_for_instance(
        &mut self,
        path_instance_id: CarrierPathInstanceId,
        sample: PathRateSample,
    ) {
        if self.accepts_native_carrier_observation(path_instance_id) {
            self.mark_product_delivery_replacing_rate(sample);
        }
    }

    pub(in crate::runtime) fn mark_udp_datagram_feedback(
        &mut self,
        observation: UdpDatagramPathObservation,
    ) {
        self.mark_udp_datagram_feedback_at(observation, Instant::now());
    }

    pub(in crate::runtime) fn mark_udp_datagram_feedback_at(
        &mut self,
        observation: UdpDatagramPathObservation,
        now: Instant,
    ) {
        self.mark_success(observation.rtt);
        if let Some(sample) = observation.rate_sample {
            self.prepare_delivery_rate_sample(now);
            self.mark_delivery_at_with_expiry(sample, now, observation.rate_sample_expires_at);
            self.datagram_feedback_samples = self.datagram_feedback_samples.saturating_add(1);
        }
        let sample_jitter_ms = observation.jitter.as_secs_f64() * 1000.0;
        self.measured_jitter_ms = Some(match self.measured_jitter_ms {
            Some(previous) => previous.mul_add(0.875, sample_jitter_ms * 0.125),
            None => sample_jitter_ms,
        });
        if let Some(loss_rate) = observation.loss_rate {
            self.measured_loss_rate = Some(match self.measured_loss_rate {
                Some(previous) => previous.mul_add(0.875, loss_rate * 0.125),
                None => loss_rate,
            });
        }
    }

    pub(in crate::runtime) fn mark_udp_datagram_feedback_for_instance(
        &mut self,
        path_instance_id: CarrierPathInstanceId,
        observation: UdpDatagramPathObservation,
    ) -> bool {
        if !self.accepts_liveness_sample(path_instance_id) {
            return false;
        }
        self.mark_udp_datagram_feedback(observation);
        true
    }

    pub(in crate::runtime) fn mark_quic_path_metrics(
        &mut self,
        path_instance_id: CarrierPathInstanceId,
        metrics: UdpPathMetrics,
    ) {
        if !self.accepts_liveness_sample(path_instance_id) {
            return;
        }
        self.mark_liveness_success();
        if metrics.rtt_observed {
            self.carrier_srtt_ms = Some(metrics.srtt.as_secs_f64() * 1000.0);
            self.carrier_rttvar_ms = Some(metrics.rttvar.as_secs_f64() * 1000.0);
        }
        if let Some(loss_ppm) = metrics.loss_ppm {
            // `Some(0)` is an observed zero. `None` is an unavailable partial
            // capability and must not erase an earlier same-instance sample.
            self.carrier_loss_rate = Some(f64::from(loss_ppm) / 1_000_000.0);
        }
        if let Some(ecn_ppm) = metrics.ecn_ppm {
            // ECN has the same partial-capability semantics as native loss:
            // retain a same-instance observation across unavailable polls.
            self.carrier_ecn_rate = Some(f64::from(ecn_ppm) / 1_000_000.0);
        }
        match (
            metrics.last_delivery_sample_at,
            metrics.bulk_proof_expires_at,
        ) {
            (None, _) => {
                // Quinn starts a new transport-evidence epoch on path change.
                // Old-path diagnostics must not be relabelled as the new path.
                self.clear_native_delivery_rate_evidence();
            }
            (Some(observed_at), Some(expires_at)) => {
                if self.carrier_last_delivery_at != Some(observed_at) {
                    // Only a new qualified ACK epoch replaces this bundle.
                    // Shape polls may change current Quinn pacing while
                    // retaining the old sample timestamp and deadline; those
                    // values must not be relabelled as same-epoch evidence.
                    self.carrier_delivery_rate_bps = (metrics.delivery_sample_count > 0)
                        .then_some(metrics.delivery_rate_bps.max(1.0));
                    self.carrier_pacing_rate_bps = (metrics.delivery_sample_count > 0)
                        .then_some(metrics.pacing_rate_bps.max(1.0));
                    self.carrier_delivery_samples =
                        u32::try_from(metrics.delivery_sample_count).unwrap_or(u32::MAX);
                    self.carrier_delivery_sample_bytes = metrics.delivery_sample_bytes;
                    self.carrier_last_delivery_at = Some(observed_at);
                    self.carrier_bulk_proof_expires_at = Some(expires_at);
                }
            }
            (Some(_), None) => {
                // Expiry revokes authority, not diagnostic provenance. The
                // retained deadline prevents this raw rate from falling back
                // into startup or current scheduling state.
            }
        }
        if metrics.ack_derived_data_seen {
            self.carrier_ack_derived_data_seen = true;
        }
        self.carrier_bytes_in_flight = metrics.bytes_in_flight as u64;
        self.carrier_bytes_in_flight_observed = true;
        self.carrier_queue_bytes = metrics
            .pending_bytes
            .saturating_sub(metrics.bytes_in_flight) as u64;
        self.carrier_queue_bytes_observed = true;
        self.carrier_inflight_limit_bytes = metrics.inflight_hi as u64;
        self.carrier_app_limited = metrics.app_limited;
    }

    fn accepts_native_carrier_observation(&self, path_instance_id: CarrierPathInstanceId) -> bool {
        !self.manual_disabled
            && self.path_instance_id == Some(path_instance_id)
            && self.data_plane_failure_instance_id != Some(path_instance_id)
    }

    fn accepts_liveness_sample(&self, path_instance_id: CarrierPathInstanceId) -> bool {
        self.state != SchedulerPathState::Draining
            && self.accepts_native_carrier_observation(path_instance_id)
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
        path_instance_id: CarrierPathInstanceId,
        now: Instant,
        has_schedulable_alternative: bool,
    ) -> bool {
        if self.path_instance_id != Some(path_instance_id)
            || self.data_plane_failure_instance_id == Some(path_instance_id)
        {
            return false;
        }
        self.data_plane_failure_instance_id = Some(path_instance_id);
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.relay_bytes_in_flight = 0;
        self.relay_queue_bytes = 0;
        // Product and native carrier evidence belongs to the failed association.
        self.product_delivery_rate_bps = None;
        self.product_delivery_sample_bytes = 0;
        self.clear_native_carrier_state();
        self.tcp_capacity.reset_after_data_plane_failure();
        self.invalidate_path_proofs();
        if has_schedulable_alternative {
            self.state = SchedulerPathState::Failed;
            self.failed_until = Some(now + path_record_failure_cooldown(self));
        } else {
            self.state = SchedulerPathState::Suspect;
            self.failed_until = None;
        }
        true
    }

    fn clear_native_carrier_state(&mut self) {
        self.carrier_srtt_ms = None;
        self.carrier_rttvar_ms = None;
        self.carrier_loss_rate = None;
        self.carrier_ecn_rate = None;
        self.carrier_bytes_in_flight = 0;
        self.carrier_bytes_in_flight_observed = false;
        self.carrier_queue_bytes = 0;
        self.carrier_queue_bytes_observed = false;
        self.carrier_inflight_limit_bytes = 0;
        self.native_drain_observed = false;
        self.clear_native_delivery_rate_evidence();
    }

    fn clear_physical_carrier_state(&mut self) {
        self.data_plane_failure_instance_id = None;
        self.wire_path_id = None;
        self.peer_usage = None;
        self.path_instance_id = None;
        self.peer_usage_sequence = None;
        self.consecutive_failures = 0;
        self.measured_srtt_ms = None;
        self.measured_jitter_ms = None;
        self.measured_rate_bps = None;
        self.measured_loss_rate = None;
        self.delivery_samples = 0;
        self.product_delivery_rate_bps = None;
        self.product_delivery_sample_bytes = 0;
        self.datagram_feedback_samples = 0;
        self.last_delivery_at = None;
        self.delivery_rate_expires_at = None;
        self.failed_until = None;
        self.relay_bytes_in_flight = 0;
        self.relay_queue_bytes = 0;
        self.clear_native_carrier_state();
        self.tcp_capacity.reset_after_data_plane_failure();
        self.invalidate_path_proofs();
        self.state = if self.manual_disabled {
            SchedulerPathState::Failed
        } else {
            SchedulerPathState::Suspect
        };
    }

    pub(in crate::runtime) fn record_relay_send(
        &mut self,
        path_instance_id: CarrierPathInstanceId,
        bytes: usize,
    ) {
        if !self.accepts_native_carrier_observation(path_instance_id) {
            return;
        }
        self.relay_bytes_in_flight = self.relay_bytes_in_flight.saturating_add(bytes as u64);
    }

    pub(in crate::runtime) fn release_relay_inflight(
        &mut self,
        path_instance_id: CarrierPathInstanceId,
        bytes: usize,
    ) -> bool {
        if self.path_instance_id != Some(path_instance_id) {
            return false;
        }
        self.relay_bytes_in_flight = self.relay_bytes_in_flight.saturating_sub(bytes as u64);
        self.is_product_quiescent_for_instance(path_instance_id)
    }
}

#[cfg(test)]
#[path = "tests_health.rs"]
mod tests;
