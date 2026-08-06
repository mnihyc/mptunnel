use crate::model::admission::BulkPathCandidate;
use crate::model::capacity::{
    MAX_PRODUCT_DATAGRAM_PAYLOAD_BYTES, MAX_RELIABLE_SERVICE_QUANTUM_BYTES, PATH_OPEN_SCORE_BYTES,
    PathRateSample, QUIC_PERSISTENT_CONGESTION_THRESHOLD, QUIC_TIMER_GRANULARITY,
    RELIABLE_INITIAL_RTT, RELIABLE_INITIAL_WINDOW_PACKETS, UDP_BASELINE_PACKET_PAYLOAD_BYTES,
    adaptive_reliable_relay_inflight_bytes, product_delivery_samples_override_startup_prior,
};
use crate::model::path::{CarrierPathInstanceId, RelayPathKey};
use crate::model::timing::{
    quic_bulk_proof_freshness_horizon, transport_pto_from_ms, transport_pto_from_snapshot,
};
use crate::mux::MuxLimits;
use crate::protocol::{PathId, PathMetricDirection, PathMetrics, PathUsage, UnderlayProtocol};
use crate::runtime::path::health::ClientPathHealthRecord;
use crate::scheduler::{
    self, PathRateScope, PathSnapshot, PathState as SchedulerPathState, TrafficClass,
};
use crate::transport::{PathSpec, RateHint};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Ranked UDP path value shared with the product datagram association.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::runtime) struct UdpPathCandidate {
    pub(in crate::runtime) path_index: usize,
    pub(in crate::runtime) eta_ms: f64,
}

/// One layer-3 tunnel attachment on an exact authenticated carrier lifetime.
///
/// The packet service owns attachment readiness. Path health owns only the
/// carrier identity and observations, so callers pass the ready set into the
/// immutable candidate projection instead of letting it infer readiness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(in crate::runtime) struct PacketPathAttachment {
    pub(in crate::runtime) key: RelayPathKey,
    pub(in crate::runtime) path_instance_id: CarrierPathInstanceId,
}

/// Packet-plane load paired with one ready exact attachment for one coherent
/// selection. It never reads or mutates Product flow accounting.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct PacketPathSelectionInput {
    pub(in crate::runtime) attachment: PacketPathAttachment,
    pub(in crate::runtime) active_flows: u32,
}

/// One immutable carrier candidate for layer-3 packet affinity.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct PacketPathCandidate {
    pub(in crate::runtime) attachment: PacketPathAttachment,
    pub(in crate::runtime) snapshot: PathSnapshot,
    pub(in crate::runtime) eta_ms: f64,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct UdpPathRuntimeModel {
    pub(in crate::runtime) pacing_rate_bps: f64,
    pub(in crate::runtime) response_timeout: Duration,
    pub(in crate::runtime) max_payload_bytes: usize,
}

impl UdpPathRuntimeModel {
    pub(in crate::runtime) fn from_snapshot(
        snapshot: PathSnapshot,
        ttl_ms: u32,
        max_payload_bytes: usize,
    ) -> Self {
        let ttl_timeout = Duration::from_millis(u64::from(ttl_ms));
        let pto = transport_pto_from_snapshot(Some(snapshot));
        let pacing_payload_bytes = max_payload_bytes.clamp(1, UDP_BASELINE_PACKET_PAYLOAD_BYTES);
        let loss_backoff = datagram_loss_backoff(snapshot, pacing_payload_bytes);
        let min_pacing_rate_bps = datagram_min_pacing_rate_bps(pacing_payload_bytes, pto);
        let pacing_rate_bps = (snapshot.delivery_rate_bps * loss_backoff).max(min_pacing_rate_bps);
        let timeout_loss_gain = 1.0 + snapshot.loss_rate.clamp(0.0, 1.0);
        let response_timeout = pto.mul_f64(timeout_loss_gain).min(ttl_timeout);
        Self {
            pacing_rate_bps,
            response_timeout,
            max_payload_bytes,
        }
    }

    pub(in crate::runtime) fn accepts_payload(self, payload_bytes: usize) -> bool {
        payload_bytes <= self.max_payload_bytes
    }

    pub(in crate::runtime) fn pacing_interval(self, payload_bytes: usize) -> Duration {
        if payload_bytes == 0 {
            return Duration::ZERO;
        }
        Duration::from_secs_f64(payload_bytes as f64 * 8.0 / self.pacing_rate_bps)
    }
}

fn datagram_loss_backoff(snapshot: PathSnapshot, payload_bytes: usize) -> f64 {
    let loss = snapshot.loss_rate.clamp(0.0, 1.0);
    let min_progress = adaptive_transport_byte_floor_factor(
        payload_bytes.max(1) as f64,
        path_bdp_floor_bytes(snapshot),
    );
    (1.0 - loss).max(min_progress)
}

fn datagram_min_pacing_rate_bps(payload_bytes: usize, pto: Duration) -> f64 {
    let payload_bits = payload_bytes.max(1) as f64 * 8.0;
    payload_bits / pto.max(QUIC_TIMER_GRANULARITY).as_secs_f64()
}

fn path_bdp_floor_bytes(path: PathSnapshot) -> f64 {
    let rate = path.delivery_rate_bps.max(path.pacing_rate_bps).max(1.0);
    rate / 8.0 * path.srtt_ms.max(1.0) / 1000.0
}

fn adaptive_transport_byte_floor_factor(minimum_bytes: f64, model_bytes: f64) -> f64 {
    minimum_bytes.max(1.0) / model_bytes.max(minimum_bytes).max(1.0)
}

pub(in crate::runtime) fn path_record_failure_cooldown(
    record: &ClientPathHealthRecord,
) -> Duration {
    let srtt_ms = record
        .carrier_srtt_ms
        .or(record.measured_srtt_ms)
        .unwrap_or(RELIABLE_INITIAL_RTT.as_secs_f64() * 1000.0);
    let rttvar_ms = record
        .carrier_rttvar_ms
        .or(record.measured_jitter_ms)
        .unwrap_or(srtt_ms / 2.0);
    let pto = transport_pto_from_ms(srtt_ms, rttvar_ms);
    let failure_exponent = record
        .consecutive_failures
        .saturating_sub(1)
        .min(QUIC_PERSISTENT_CONGESTION_THRESHOLD);
    pto.saturating_mul(2_u32.saturating_pow(failure_exponent))
}

pub(in crate::runtime) fn udp_datagram_payload_limit_bytes(
    path: &PathSpec,
    max_payload_bytes: usize,
) -> usize {
    let protocol_ceiling = max_payload_bytes.clamp(1, MAX_PRODUCT_DATAGRAM_PAYLOAD_BYTES);
    path.metadata
        .max_datagram_payload_bytes
        .unwrap_or(protocol_ceiling)
        .clamp(1, protocol_ceiling)
}

pub(in crate::runtime) fn health_observations(
    records: &[ClientPathHealthRecord],
    now: Instant,
) -> Vec<ClientPathObservation> {
    records
        .iter()
        .map(|record| record.observation_at(now))
        .collect()
}

pub(in crate::runtime) fn path_records_have_schedulable_alternative(
    records: &[ClientPathHealthRecord],
    failed_index: usize,
    now: Instant,
) -> bool {
    records.iter().enumerate().any(|(index, record)| {
        index != failed_index
            && !matches!(
                record.observation_at(now).state,
                SchedulerPathState::Failed | SchedulerPathState::Draining
            )
    })
}

pub(in crate::runtime) fn path_observation_is_idle_for_probe(
    observation: ClientPathObservation,
) -> bool {
    observation.active_flows == 0
}

pub(in crate::runtime) fn apply_bulk_latency_isolation(
    observations: &mut [ClientPathObservation],
    lane: TrafficClass,
    mux_limits: MuxLimits,
) {
    if lane != TrafficClass::Throughput {
        return;
    }
    if !observations
        .iter()
        .any(|observation| observation.measured_rate_bps.is_some())
    {
        return;
    }
    let isolation_bytes =
        adaptive_reliable_relay_inflight_bytes(None, TrafficClass::Latency, mux_limits) as u64;
    for observation in observations {
        let latency_flows = u64::from(observation.active_latency_sensitive_flows);
        observation.relay_queue_bytes = observation
            .relay_queue_bytes
            .saturating_add(latency_flows.saturating_mul(isolation_bytes));
    }
}

pub(in crate::runtime) fn endpoint_only_reliable_startup_should_preserve_configured_order(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: TrafficClass,
) -> bool {
    lane.is_latency_sensitive()
        && paths.iter().all(path_is_endpoint_only)
        && !endpoint_only_startup_has_latency_sensitive_load(observations)
        && !endpoint_only_startup_has_bulk_load(observations)
}

pub(in crate::runtime) fn endpoint_only_reliable_startup_should_spread_latency_load(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: TrafficClass,
) -> bool {
    lane.is_latency_sensitive()
        && paths.iter().all(path_is_endpoint_only)
        && endpoint_only_startup_has_latency_sensitive_load(observations)
        && !endpoint_only_startup_has_bulk_load(observations)
}

pub(in crate::runtime) fn endpoint_only_startup_has_latency_sensitive_load(
    observations: &[ClientPathObservation],
) -> bool {
    observations
        .iter()
        .any(|observation| observation.active_latency_sensitive_flows > 0)
}

pub(in crate::runtime) fn endpoint_only_startup_has_any_load(
    observations: &[ClientPathObservation],
) -> bool {
    observations
        .iter()
        .any(|observation| observation.active_flows > 0)
}

pub(in crate::runtime) fn endpoint_only_startup_has_bulk_load(
    observations: &[ClientPathObservation],
) -> bool {
    observations
        .iter()
        .any(|observation| observation.active_flows > observation.active_latency_sensitive_flows)
}

pub(in crate::runtime) fn endpoint_only_reliable_startup_should_spread_bulk_load(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: TrafficClass,
) -> bool {
    lane.is_latency_sensitive()
        && paths.iter().all(path_is_endpoint_only)
        && endpoint_only_startup_has_any_load(observations)
        && endpoint_only_startup_has_bulk_load(observations)
        && !endpoint_only_startup_has_latency_sensitive_load(observations)
}

pub(in crate::runtime) fn reliable_stream_startup_path_scores(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: TrafficClass,
    payload_bytes: usize,
) -> Vec<(usize, f64)> {
    if endpoint_only_reliable_startup_should_preserve_configured_order(paths, observations, lane) {
        return configured_order_path_scores(paths, observations, lane, payload_bytes);
    }
    if endpoint_only_reliable_startup_should_spread_latency_load(paths, observations, lane) {
        return endpoint_only_reliable_startup_path_scores(
            paths,
            observations,
            lane,
            payload_bytes,
        );
    }
    ordered_path_scores(paths, observations, lane, payload_bytes)
}

fn reliable_stream_mixed_startup_path_scores(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: TrafficClass,
    payload_bytes: usize,
) -> Vec<(usize, f64)> {
    if endpoint_only_reliable_startup_should_preserve_configured_order(paths, observations, lane)
        || endpoint_only_reliable_startup_should_spread_latency_load(paths, observations, lane)
        || endpoint_only_reliable_startup_should_spread_bulk_load(paths, observations, lane)
    {
        return endpoint_only_reliable_startup_path_scores(
            paths,
            observations,
            lane,
            payload_bytes,
        );
    }
    ordered_path_scores(paths, observations, lane, payload_bytes)
}

pub(in crate::runtime) fn endpoint_only_reliable_startup_path_scores(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: TrafficClass,
    payload_bytes: usize,
) -> Vec<(usize, f64)> {
    let observations = observations
        .iter()
        .copied()
        .map(endpoint_only_startup_observation_for_scoring)
        .collect::<Vec<_>>();
    ordered_path_scores(paths, &observations, lane, payload_bytes)
}

fn endpoint_only_startup_observation_for_scoring(
    observation: ClientPathObservation,
) -> ClientPathObservation {
    if observation_has_sender_delivery_evidence(observation) {
        return observation;
    }
    let validated_timing = observation_has_validated_rtt_evidence(observation).then_some((
        observation.measured_srtt_ms,
        observation.measured_jitter_ms,
        observation.carrier_srtt_ms,
        observation.carrier_rttvar_ms,
    ));
    ClientPathObservation {
        // Path validation qualifies timing independently from delivery rate.
        // Keep capacity unknown until sender delivery evidence exists.
        measured_srtt_ms: validated_timing.and_then(|timing| timing.0),
        measured_jitter_ms: validated_timing.and_then(|timing| timing.1),
        measured_rate_bps: None,
        measured_loss_rate: None,
        delivery_samples: 0,
        product_delivery_rate_bps: None,
        product_delivery_sample_bytes: 0,
        last_delivery_at: None,
        carrier_srtt_ms: validated_timing.and_then(|timing| timing.2),
        carrier_rttvar_ms: validated_timing.and_then(|timing| timing.3),
        carrier_delivery_rate_bps: None,
        carrier_pacing_rate_bps: None,
        carrier_delivery_samples: 0,
        carrier_delivery_sample_bytes: 0,
        carrier_last_delivery_at: None,
        ..observation
    }
}

pub(in crate::runtime) fn path_is_endpoint_only(path: &PathSpec) -> bool {
    let policy = path.metadata.policy;
    path.metadata.initial_srtt_ms.is_none()
        && path.metadata.initial_jitter_ms.is_none()
        && path.metadata.initial_rate == RateHint::Unknown
        && !policy.expensive
        && policy.bulk_allowed
        && !policy.probe_only
        && !policy.no_udp
}

pub(in crate::runtime) fn configured_order_path_indices(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: TrafficClass,
    payload_bytes: usize,
) -> Vec<usize> {
    configured_order_path_scores(paths, observations, lane, payload_bytes)
        .into_iter()
        .map(|(index, _)| index)
        .collect()
}

pub(in crate::runtime) fn configured_order_path_scores(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: TrafficClass,
    payload_bytes: usize,
) -> Vec<(usize, f64)> {
    let mut scores = paths
        .iter()
        .enumerate()
        .filter_map(|(index, path)| {
            let observation = observations.get(index).copied().unwrap_or_default();
            let snapshot = path_snapshot(path, index, observation);
            scheduler::score_path(snapshot, lane, payload_bytes)
                .map(|score| (index, score.eta_ms, scheduler::path_is_backup(snapshot)))
        })
        .collect::<Vec<_>>();
    if scores.iter().any(|(_, _, backup)| !backup) {
        scores.retain(|(_, _, backup)| !backup);
    }
    scores
        .into_iter()
        .map(|(index, eta_ms, _)| (index, eta_ms))
        .collect()
}

pub(in crate::runtime) fn ordered_path_scores_for_ttl(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: TrafficClass,
    payload_bytes: usize,
    ttl_ms: u32,
) -> Vec<(usize, f64)> {
    let scores = ordered_path_scores(paths, observations, lane, payload_bytes);
    let freshness_budget_ms = f64::from(ttl_ms);
    scores
        .iter()
        .copied()
        .filter(|(_, eta_ms)| *eta_ms <= freshness_budget_ms)
        .collect::<Vec<_>>()
}

pub(in crate::runtime) fn ordered_path_scores(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: TrafficClass,
    payload_bytes: usize,
) -> Vec<(usize, f64)> {
    let mut scores = paths
        .iter()
        .enumerate()
        .filter_map(|(index, path)| {
            let observation = observations.get(index).copied().unwrap_or_default();
            let snapshot = path_snapshot(path, index, observation);
            scheduler::score_path(snapshot, lane, payload_bytes)
                .map(|score| (index, score.eta_ms, scheduler::path_is_backup(snapshot)))
        })
        .collect::<Vec<_>>();
    scores.sort_by(|left, right| {
        left.2.cmp(&right.2).then_with(|| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| left.0.cmp(&right.0))
        })
    });
    scores
        .into_iter()
        .map(|(index, eta_ms, _)| (index, eta_ms))
        .collect()
}

pub(in crate::runtime) fn reliable_stream_path_candidates(
    tcp_paths: &[PathSpec],
    tcp_observations: &[ClientPathObservation],
    udp_paths: &[PathSpec],
    udp_observations: &[ClientPathObservation],
    lane: TrafficClass,
    payload_bytes: usize,
) -> Vec<BulkPathCandidate> {
    let tcp_scores =
        reliable_stream_mixed_startup_path_scores(tcp_paths, tcp_observations, lane, payload_bytes);
    let udp_scores =
        reliable_stream_mixed_startup_path_scores(udp_paths, udp_observations, lane, payload_bytes);

    let mut candidates = tcp_scores
        .iter()
        .filter_map(|(index, eta_ms)| {
            let path = tcp_paths.get(*index)?;
            let observation = tcp_observations.get(*index).copied().unwrap_or_default();
            let snapshot = path_snapshot(path, *index, observation);
            path_can_be_auto_discovered_for_lane(path, observation, lane).then_some(
                bulk_path_candidate(
                    RelayPathKey {
                        underlay: UnderlayProtocol::Tcp,
                        index: *index,
                    },
                    *eta_ms,
                    path,
                    observation,
                    snapshot,
                ),
            )
        })
        .chain(udp_scores.iter().filter_map(|(index, eta_ms)| {
            let path = udp_paths.get(*index)?;
            let observation = udp_observations.get(*index).copied().unwrap_or_default();
            let snapshot = path_snapshot(path, *index, observation);
            let eta_ms = *eta_ms
                + if lane == TrafficClass::Throughput {
                    udp_reliable_stream_loss_reinjection_penalty_ms(snapshot, payload_bytes)
                } else {
                    0.0
                };
            path_can_be_auto_discovered_for_lane(path, observation, lane).then_some(
                bulk_path_candidate(
                    RelayPathKey {
                        underlay: UnderlayProtocol::Udp,
                        index: *index,
                    },
                    eta_ms,
                    path,
                    observation,
                    snapshot,
                ),
            )
        }))
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        candidates = tcp_scores
            .iter()
            .filter_map(|(index, eta_ms)| {
                let path = tcp_paths.get(*index)?;
                let observation = tcp_observations.get(*index).copied().unwrap_or_default();
                let snapshot = path_snapshot(path, *index, observation);
                path_can_be_recovery_candidate_for_lane(path, observation, lane).then_some(
                    bulk_path_candidate(
                        RelayPathKey {
                            underlay: UnderlayProtocol::Tcp,
                            index: *index,
                        },
                        *eta_ms,
                        path,
                        observation,
                        snapshot,
                    ),
                )
            })
            .chain(udp_scores.iter().filter_map(|(index, eta_ms)| {
                let path = udp_paths.get(*index)?;
                let observation = udp_observations.get(*index).copied().unwrap_or_default();
                let snapshot = path_snapshot(path, *index, observation);
                let eta_ms = *eta_ms
                    + if lane == TrafficClass::Throughput {
                        udp_reliable_stream_loss_reinjection_penalty_ms(snapshot, payload_bytes)
                    } else {
                        0.0
                    };
                path_can_be_recovery_candidate_for_lane(path, observation, lane).then_some(
                    bulk_path_candidate(
                        RelayPathKey {
                            underlay: UnderlayProtocol::Udp,
                            index: *index,
                        },
                        eta_ms,
                        path,
                        observation,
                        snapshot,
                    ),
                )
            }))
            .collect();
    }
    candidates
}

pub(in crate::runtime) fn bulk_path_candidate(
    key: RelayPathKey,
    eta_ms: f64,
    path: &PathSpec,
    observation: ClientPathObservation,
    snapshot: PathSnapshot,
) -> BulkPathCandidate {
    BulkPathCandidate {
        key,
        eta_ms,
        has_liveness_evidence: bulk_candidate_has_liveness_evidence(path, observation),
        has_path_proof_evidence: bulk_candidate_has_path_proof_evidence(observation),
        has_ack_data_evidence: bulk_candidate_has_ack_data_evidence(path, observation),
        has_bulk_rate_evidence: bulk_candidate_has_bulk_rate_evidence(path, observation),
        has_sender_delivery_evidence: bulk_candidate_has_sender_delivery_evidence(
            path,
            observation,
        ),
        snapshot,
    }
}

pub(in crate::runtime) fn path_snapshot(
    path: &PathSpec,
    index: usize,
    observation: ClientPathObservation,
) -> PathSnapshot {
    path_snapshot_with_id(
        path,
        observation.wire_path_id.unwrap_or(PathId(index as u16)),
        observation,
    )
}

/// Projects carrier evidence for the independent layer-3 packet plane.
///
/// Product data-level queues, flow ownership, and goodput samples must not
/// become packet-plane state. Native carrier observations and common path
/// timing/loss remain shared because they describe the physical output that
/// every data plane uses.
pub(in crate::runtime) fn packet_path_snapshot(
    path: &PathSpec,
    index: usize,
    observation: ClientPathObservation,
) -> PathSnapshot {
    path_snapshot(
        path,
        index,
        ClientPathObservation {
            measured_rate_bps: None,
            delivery_samples: 0,
            product_delivery_rate_bps: None,
            product_delivery_sample_bytes: 0,
            datagram_feedback_samples: 0,
            last_delivery_at: None,
            active_flows: 0,
            active_latency_sensitive_flows: 0,
            relay_bytes_in_flight: 0,
            relay_queue_bytes: 0,
            ..observation
        },
    )
}

pub(in crate::runtime) fn path_snapshot_with_id(
    path: &PathSpec,
    path_id: PathId,
    observation: ClientPathObservation,
) -> PathSnapshot {
    let hinted_delivery_rate_bps = match path.metadata.initial_rate {
        RateHint::Unknown => default_path_rate_bps(),
        RateHint::Unlimited => 1_000_000_000_000.0,
        RateHint::BitsPerSecond(rate) => rate.max(1) as f64,
    };
    let product_delivery_samples = reliable_product_delivery_samples(path, observation);
    let product_progress_rate_bps = (product_delivery_samples > 0
        && observation.product_delivery_sample_bytes > 0)
        .then_some(observation.product_delivery_rate_bps)
        .flatten();
    let has_durable_product_progress =
        client_path_observation_has_durable_product_progress(path, observation);
    let carrier_capacity_rate_bps = observation.carrier_delivery_rate_bps.filter(|_| {
        path.underlay != UnderlayProtocol::Tcp
            || bulk_candidate_has_native_carrier_rate_evidence(path, observation)
    });
    let (delivery_rate_bps, rate_scope) = if let Some(rate) = carrier_capacity_rate_bps {
        (rate, PathRateScope::PathCapacity)
    } else if let Some(rate) = product_progress_rate_bps {
        if path.underlay == UnderlayProtocol::Tcp
            && !product_delivery_samples_override_startup_prior(product_delivery_samples)
            && hinted_delivery_rate_bps > rate
        {
            (hinted_delivery_rate_bps, PathRateScope::PathCapacity)
        } else {
            (rate, PathRateScope::PerFlowGoodput)
        }
    } else if let Some(rate) = observation.measured_rate_bps {
        (rate, PathRateScope::PathCapacity)
    } else {
        (hinted_delivery_rate_bps, PathRateScope::PathCapacity)
    };
    let delivery_rate_bps = delivery_rate_bps.max(1.0);
    let srtt_ms = path_model_srtt_ms(path, observation);
    let jitter_ms = observation
        .carrier_rttvar_ms
        .or(observation.measured_jitter_ms)
        .unwrap_or_else(|| f64::from(path.metadata.initial_jitter_ms.unwrap_or(0)));
    let confidence = path_model_confidence(observation);
    // Payload completion uses delivered service. Native pacing is retained in
    // the carrier observation, but must not inflate capacity through the
    // scheduler's current max(delivery, pacing) projection.
    let pacing_rate_bps = delivery_rate_bps;
    PathSnapshot {
        id: path_id,
        underlay: path.underlay,
        state: observation.state,
        policy: path.metadata.policy,
        peer_usage: observation.peer_usage,
        srtt_ms,
        jitter_ms,
        delivery_rate_bps,
        rate_scope,
        carrier_delivery_rate_bps: carrier_capacity_rate_bps,
        product_progress_rate_bps,
        has_durable_product_progress,
        loss_rate: observation.measured_loss_rate.unwrap_or(0.0),
        queue_bytes: observation.carrier_queue_bytes,
        data_level_queue_bytes: observation.relay_queue_bytes,
        bytes_in_flight: observation.carrier_bytes_in_flight,
        data_level_bytes_in_flight: observation.relay_bytes_in_flight,
        active_flows: observation.active_flows,
        active_latency_sensitive_flows: observation.active_latency_sensitive_flows,
        session_active_latency_sensitive_flows: observation.active_latency_sensitive_flows,
        pacing_rate_bps,
        carrier_inflight_limit_bytes: observation.carrier_inflight_limit_bytes,
        data_level_limit_bytes: 0,
        confidence,
        // App-limited qualifies the rate sample that produced this model; it
        // is not an instantaneous idle test. Outstanding product or carrier
        // flight must not silently promote a lower-bound rate to capacity.
        app_limited: observation.carrier_app_limited,
    }
}

pub(in crate::runtime) fn path_startup_snapshot(path: &PathSpec, path_id: PathId) -> PathSnapshot {
    path_snapshot_with_id(
        path,
        path_id,
        ClientPathObservation {
            state: SchedulerPathState::Active,
            carrier_app_limited: path.metadata.initial_rate == RateHint::Unknown,
            ..ClientPathObservation::default()
        },
    )
}

pub(in crate::runtime) fn path_startup_metrics(
    path: &PathSpec,
    path_id: PathId,
    direction: PathMetricDirection,
) -> PathMetrics {
    let observation = ClientPathObservation {
        state: SchedulerPathState::Active,
        carrier_app_limited: path.metadata.initial_rate == RateHint::Unknown,
        ..ClientPathObservation::default()
    };
    path_metrics_from_snapshot(
        path_snapshot(path, usize::from(path_id.0), observation),
        observation,
        direction,
    )
}

pub(in crate::runtime) fn path_metrics_from_snapshot(
    snapshot: PathSnapshot,
    observation: ClientPathObservation,
    direction: PathMetricDirection,
) -> PathMetrics {
    let carrier_data_sample_count = if snapshot.underlay == UnderlayProtocol::Udp {
        observation.carrier_delivery_samples
    } else {
        0
    };
    let data_sample_count = observation
        .delivery_samples
        .saturating_add(carrier_data_sample_count);
    let has_ack_derived_data_sample =
        data_sample_count > 0 || observation.carrier_ack_derived_data_seen;
    PathMetrics {
        path_id: snapshot.id,
        underlay: snapshot.underlay,
        direction,
        metric_epoch: metric_epoch_now(),
        metric_age_us: 0,
        srtt_us: millis_to_micros_u32(snapshot.srtt_ms),
        rttvar_us: millis_to_micros_u32(snapshot.jitter_ms.max(0.0)),
        jitter_us: millis_to_micros_u32(snapshot.jitter_ms.max(0.0)),
        delivery_rate_bps: snapshot.delivery_rate_bps.max(1.0).round() as u64,
        pacing_rate_bps: observation
            .carrier_pacing_rate_bps
            .unwrap_or(snapshot.pacing_rate_bps)
            .max(1.0)
            .round() as u64,
        loss_ppm: (snapshot.loss_rate.clamp(0.0, 1.0) * 1_000_000.0).round() as u32,
        ecn_ppm: 0,
        loss_observed: observation.delivery_samples > 0 || observation.carrier_delivery_samples > 0,
        ecn_observed: false,
        bytes_in_flight: snapshot.bytes_in_flight,
        queue_bytes: snapshot.queue_bytes,
        inflight_limit_bytes: snapshot.carrier_inflight_limit_bytes,
        inflight_hi_bytes: snapshot.carrier_inflight_limit_bytes,
        confidence_ppm: ratio_to_ppm(snapshot.confidence),
        app_limited: snapshot.app_limited,
        has_ack_derived_data_sample,
        data_sample_count,
        data_sample_bytes: if snapshot.underlay == UnderlayProtocol::Udp {
            observation.carrier_delivery_sample_bytes
        } else {
            0
        },
    }
}

pub(in crate::runtime) fn metric_epoch_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

pub(in crate::runtime) fn ratio_to_ppm(value: f64) -> u32 {
    (value.clamp(0.0, 1.0) * 1_000_000.0).round() as u32
}

fn millis_to_micros_u32(ms: f64) -> u32 {
    let micros = (ms.max(0.0) * 1000.0).round();
    micros.clamp(0.0, f64::from(u32::MAX)) as u32
}

pub(in crate::runtime) fn path_model_srtt_ms(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> f64 {
    observation
        .carrier_srtt_ms
        .or(observation.measured_srtt_ms)
        .unwrap_or_else(|| {
            path.metadata
                .initial_srtt_ms
                .map_or_else(default_path_srtt_ms, f64::from)
        })
}

pub(in crate::runtime) fn path_model_confidence(observation: ClientPathObservation) -> f64 {
    if observation.explicit_carrier_capacity_proof
        && observation.carrier_delivery_rate_bps.is_some()
    {
        // One fresh fenced train represents a full multi-packet sample, not
        // one ordinary ACK. Product admission durability is modeled separately.
        return 1.0;
    }
    let delivery_confidence = (f64::from(
        observation
            .delivery_samples
            .saturating_add(observation.carrier_delivery_samples),
    ) / RELIABLE_INITIAL_WINDOW_PACKETS as f64)
        .clamp(0.0, 1.0);
    let rtt_confidence = if observation
        .carrier_srtt_ms
        .or(observation.measured_srtt_ms)
        .is_some()
    {
        1.0 / RELIABLE_INITIAL_WINDOW_PACKETS as f64
    } else {
        0.0
    };
    (delivery_confidence + rtt_confidence).clamp(0.0, 1.0)
}

pub(in crate::runtime) fn udp_path_has_realtime_model(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> bool {
    observation.measured_srtt_ms.is_some()
        || observation.carrier_srtt_ms.is_some()
        || observation.measured_jitter_ms.is_some()
        || observation.carrier_rttvar_ms.is_some()
        || observation.measured_rate_bps.is_some()
        || observation.carrier_delivery_rate_bps.is_some()
        || observation.measured_loss_rate.is_some()
        || path.metadata.initial_srtt_ms.is_some()
        || path.metadata.initial_jitter_ms.is_some()
        || path.metadata.initial_rate != RateHint::Unknown
}

pub(in crate::runtime) fn udp_observation_has_datagram_feedback(
    observation: &ClientPathObservation,
) -> bool {
    observation.measured_jitter_ms.is_some()
        || observation.measured_loss_rate.is_some()
        || observation.measured_rate_bps.is_some()
        || observation.carrier_delivery_rate_bps.is_some()
}

pub(in crate::runtime) fn path_can_be_auto_discovered(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> bool {
    observation.state == SchedulerPathState::Active && path_allows_automatic_bulk_use(path)
}

pub(in crate::runtime) fn path_allows_automatic_bulk_use(path: &PathSpec) -> bool {
    // Operator cost/role policy is transport-independent. Automatic capacity
    // discovery may measure only paths that are also eligible to carry bulk.
    let policy = path.metadata.policy;
    !policy.expensive && !policy.backup && !policy.probe_only && policy.bulk_allowed
}

fn path_can_be_auto_discovered_for_lane(
    path: &PathSpec,
    observation: ClientPathObservation,
    lane: TrafficClass,
) -> bool {
    observation.state == SchedulerPathState::Active
        && !path.metadata.policy.expensive
        && !path.metadata.policy.backup
        && !path.metadata.policy.probe_only
        && (lane != TrafficClass::Throughput || path.metadata.policy.bulk_allowed)
}

fn path_can_be_recovery_candidate_for_lane(
    path: &PathSpec,
    observation: ClientPathObservation,
    lane: TrafficClass,
) -> bool {
    !matches!(
        observation.state,
        SchedulerPathState::Failed | SchedulerPathState::Draining
    ) && !path.metadata.policy.probe_only
        && (lane != TrafficClass::Throughput || path.metadata.policy.bulk_allowed)
}

pub(in crate::runtime) fn bulk_candidate_has_liveness_evidence(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> bool {
    observation.path_proof_success
        || observation.measured_srtt_ms.is_some()
        || observation.carrier_srtt_ms.is_some()
        || observation.measured_jitter_ms.is_some()
        || observation.carrier_rttvar_ms.is_some()
        || observation.measured_rate_bps.is_some()
        || observation.carrier_delivery_rate_bps.is_some()
        || observation.measured_loss_rate.is_some()
        || path.metadata.initial_srtt_ms.is_some()
        || path.metadata.initial_jitter_ms.is_some()
        || path.metadata.initial_rate != RateHint::Unknown
}

pub(in crate::runtime) fn bulk_candidate_has_path_proof_evidence(
    observation: ClientPathObservation,
) -> bool {
    observation.path_proof_success
}

pub(in crate::runtime) fn bulk_candidate_has_ack_data_evidence(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> bool {
    reliable_product_delivery_samples(path, observation) > 0
        || observation.last_delivery_at.is_some()
        || (path.underlay == UnderlayProtocol::Udp
            && (observation.carrier_delivery_samples > 0
                || observation.carrier_last_delivery_at.is_some()
                || observation.carrier_ack_derived_data_seen))
}

pub(in crate::runtime) fn bulk_candidate_has_bulk_rate_evidence(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> bool {
    let product_rate = client_path_observation_has_durable_product_progress(path, observation);
    product_rate || bulk_candidate_has_native_carrier_rate_evidence(path, observation)
}

pub(in crate::runtime) fn bulk_candidate_has_native_carrier_rate_evidence(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> bool {
    (observation.explicit_carrier_capacity_proof && observation.carrier_delivery_rate_bps.is_some())
        || carrier_has_durable_delivery_window(path, observation)
}

pub(in crate::runtime) fn bulk_candidate_has_fresh_native_carrier_rate_evidence(
    path: &PathSpec,
    observation: ClientPathObservation,
    valid_after: Instant,
    now: Instant,
) -> bool {
    let Some(sample_at) = observation.carrier_last_delivery_at else {
        return false;
    };
    if sample_at < valid_after || sample_at > now {
        return false;
    }
    let proof_is_fresh = match path.underlay {
        UnderlayProtocol::Udp => observation
            .carrier_bulk_proof_expires_at
            .is_some_and(|expires_at| now < expires_at),
        UnderlayProtocol::Tcp => {
            let srtt = Duration::from_secs_f64(
                observation
                    .carrier_srtt_ms
                    .unwrap_or(RELIABLE_INITIAL_RTT.as_secs_f64() * 1000.0)
                    .max(0.001)
                    / 1000.0,
            );
            let rttvar = observation
                .carrier_rttvar_ms
                .map(|rttvar_ms| Duration::from_secs_f64(rttvar_ms.max(0.001) / 1000.0))
                .unwrap_or(srtt / 2);
            now.saturating_duration_since(sample_at)
                < quic_bulk_proof_freshness_horizon(srtt, rttvar)
        }
    };
    bulk_candidate_has_native_carrier_rate_evidence(path, observation) && proof_is_fresh
}

fn carrier_has_durable_delivery_window(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> bool {
    observation.carrier_delivery_rate_bps.is_some()
        && match path.underlay {
            UnderlayProtocol::Tcp => {
                observation.carrier_delivery_window_covered && !observation.carrier_app_limited
            }
            UnderlayProtocol::Udp => {
                observation.carrier_ack_derived_data_seen
                    && observation.carrier_bulk_proof_expires_at.is_some()
            }
        }
        && observation.carrier_delivery_samples > 0
        && observation.carrier_delivery_sample_bytes >= MAX_RELIABLE_SERVICE_QUANTUM_BYTES as u64
}

fn client_path_observation_has_durable_product_progress(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> bool {
    reliable_product_delivery_samples(path, observation) > 0
        && observation.product_delivery_sample_bytes >= product_delivery_sample_floor_bytes()
}

pub(in crate::runtime) fn bulk_candidate_has_sender_delivery_evidence(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> bool {
    bulk_candidate_has_ack_data_evidence(path, observation)
        || bulk_candidate_has_bulk_rate_evidence(path, observation)
}

fn observation_has_sender_delivery_evidence(observation: ClientPathObservation) -> bool {
    observation.delivery_samples > 0
        || observation.carrier_delivery_samples > 0
        || observation.last_delivery_at.is_some()
        || observation.carrier_last_delivery_at.is_some()
        || observation.carrier_ack_derived_data_seen
        || observation.measured_rate_bps.is_some()
        || (observation.carrier_delivery_rate_bps.is_some() && !observation.carrier_app_limited)
}

fn product_delivery_sample_floor_bytes() -> u64 {
    // Product ACK coverage must not chase a TCP cwnd that grows by the same
    // ACKed bytes. One full service quantum is stable product evidence; native
    // TCP/QUIC state independently owns the live congestion window.
    (MAX_RELIABLE_SERVICE_QUANTUM_BYTES as u64).max(PATH_OPEN_SCORE_BYTES as u64)
}

fn reliable_product_delivery_samples(path: &PathSpec, observation: ClientPathObservation) -> u32 {
    match path.underlay {
        UnderlayProtocol::Udp => observation
            .delivery_samples
            .saturating_sub(observation.datagram_feedback_samples),
        UnderlayProtocol::Tcp => observation.delivery_samples,
    }
}

pub(in crate::runtime) fn udp_reliable_stream_loss_reinjection_penalty_ms(
    snapshot: scheduler::PathSnapshot,
    payload_bytes: usize,
) -> f64 {
    let loss = snapshot.loss_rate.clamp(0.0, 1.0);
    if loss <= f64::EPSILON {
        return 0.0;
    }
    let fragment_count = (payload_bytes as f64 / UDP_BASELINE_PACKET_PAYLOAD_BYTES as f64)
        .ceil()
        .max(1.0);
    let bdp_bytes = path_bdp_floor_bytes(snapshot).max(UDP_BASELINE_PACKET_PAYLOAD_BYTES as f64);
    let progress_floor = (UDP_BASELINE_PACKET_PAYLOAD_BYTES as f64 / bdp_bytes).min(1.0);
    let expected_reinjections = fragment_count * loss / (1.0 - loss).max(progress_floor);
    let reinjection_rtt_ms = transport_pto_from_snapshot(Some(snapshot)).as_secs_f64() * 1000.0;
    expected_reinjections * reinjection_rtt_ms
}

pub(in crate::runtime) fn default_path_srtt_ms() -> f64 {
    RELIABLE_INITIAL_RTT.as_secs_f64() * 1000.0
}

pub(in crate::runtime) fn default_path_rate_bps() -> f64 {
    PATH_OPEN_SCORE_BYTES as f64 * 8.0 / RELIABLE_INITIAL_RTT.as_secs_f64()
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ClientPathObservation {
    pub(in crate::runtime) state: SchedulerPathState,
    pub(in crate::runtime) manual_disabled: bool,
    pub(in crate::runtime) wire_path_id: Option<PathId>,
    pub(in crate::runtime) peer_usage: Option<PathUsage>,
    pub(in crate::runtime) measured_srtt_ms: Option<f64>,
    pub(in crate::runtime) measured_jitter_ms: Option<f64>,
    pub(in crate::runtime) measured_rate_bps: Option<f64>,
    pub(in crate::runtime) measured_loss_rate: Option<f64>,
    pub(in crate::runtime) delivery_samples: u32,
    pub(in crate::runtime) product_delivery_rate_bps: Option<f64>,
    pub(in crate::runtime) product_delivery_sample_bytes: u64,
    pub(in crate::runtime) datagram_feedback_samples: u32,
    pub(in crate::runtime) last_delivery_at: Option<Instant>,
    pub(in crate::runtime) active_flows: u32,
    pub(in crate::runtime) active_latency_sensitive_flows: u32,
    pub(in crate::runtime) relay_bytes_in_flight: u64,
    pub(in crate::runtime) relay_queue_bytes: u64,
    pub(in crate::runtime) carrier_srtt_ms: Option<f64>,
    pub(in crate::runtime) carrier_rttvar_ms: Option<f64>,
    pub(in crate::runtime) carrier_delivery_rate_bps: Option<f64>,
    pub(in crate::runtime) carrier_pacing_rate_bps: Option<f64>,
    pub(in crate::runtime) carrier_bytes_in_flight: u64,
    pub(in crate::runtime) carrier_queue_bytes: u64,
    pub(in crate::runtime) carrier_inflight_limit_bytes: u64,
    pub(in crate::runtime) carrier_delivery_samples: u32,
    pub(in crate::runtime) carrier_delivery_sample_bytes: u64,
    pub(in crate::runtime) carrier_delivery_window_covered: bool,
    pub(in crate::runtime) carrier_last_delivery_at: Option<Instant>,
    pub(in crate::runtime) carrier_bulk_proof_expires_at: Option<Instant>,
    pub(in crate::runtime) carrier_app_limited: bool,
    pub(in crate::runtime) carrier_ack_derived_data_seen: bool,
    pub(in crate::runtime) explicit_carrier_capacity_proof: bool,
    pub(in crate::runtime) path_proof_success: bool,
}

impl Default for ClientPathObservation {
    fn default() -> Self {
        Self {
            state: SchedulerPathState::Suspect,
            manual_disabled: false,
            wire_path_id: None,
            peer_usage: None,
            measured_srtt_ms: None,
            measured_jitter_ms: None,
            measured_rate_bps: None,
            measured_loss_rate: None,
            delivery_samples: 0,
            product_delivery_rate_bps: None,
            product_delivery_sample_bytes: 0,
            datagram_feedback_samples: 0,
            last_delivery_at: None,
            active_flows: 0,
            active_latency_sensitive_flows: 0,
            relay_bytes_in_flight: 0,
            relay_queue_bytes: 0,
            carrier_srtt_ms: None,
            carrier_rttvar_ms: None,
            carrier_delivery_rate_bps: None,
            carrier_pacing_rate_bps: None,
            carrier_bytes_in_flight: 0,
            carrier_queue_bytes: 0,
            carrier_inflight_limit_bytes: 0,
            carrier_delivery_samples: 0,
            carrier_delivery_sample_bytes: 0,
            carrier_delivery_window_covered: false,
            carrier_last_delivery_at: None,
            carrier_bulk_proof_expires_at: None,
            carrier_app_limited: true,
            carrier_ack_derived_data_seen: false,
            explicit_carrier_capacity_proof: false,
            path_proof_success: false,
        }
    }
}

pub(super) fn reliable_reservation_should_use_endpoint_only_startup_order(
    tcp_paths: &[PathSpec],
    tcp_observations: &[ClientPathObservation],
    udp_paths: &[PathSpec],
    udp_observations: &[ClientPathObservation],
) -> bool {
    (!tcp_paths.is_empty() || !udp_paths.is_empty())
        && tcp_paths.iter().chain(udp_paths).all(path_is_endpoint_only)
        && !paths_have_sender_delivery_evidence(tcp_paths, tcp_observations)
        && !paths_have_sender_delivery_evidence(udp_paths, udp_observations)
        && !tcp_observations
            .iter()
            .any(|observation| observation.path_proof_success)
        && !udp_observations
            .iter()
            .any(|observation| observation.path_proof_success)
}

fn observation_has_validated_rtt_evidence(observation: ClientPathObservation) -> bool {
    observation.path_proof_success
        && (observation.measured_srtt_ms.is_some() || observation.carrier_srtt_ms.is_some())
}

fn paths_have_sender_delivery_evidence(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
) -> bool {
    paths.iter().enumerate().any(|(index, path)| {
        bulk_candidate_has_sender_delivery_evidence(
            path,
            observations.get(index).copied().unwrap_or_default(),
        )
    })
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct UdpDatagramPathObservation {
    pub(in crate::runtime) rtt: Duration,
    pub(in crate::runtime) jitter: Duration,
    pub(in crate::runtime) loss_rate: f64,
    pub(in crate::runtime) rate_sample: Option<PathRateSample>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(in crate::runtime) struct PathDeliveryStats {
    pub(in crate::runtime) payload_bytes: u64,
    pub(in crate::runtime) first_payload_at: Option<Instant>,
    pub(in crate::runtime) last_payload_at: Option<Instant>,
}

impl PathDeliveryStats {
    pub(in crate::runtime) fn record_payload_bytes(&mut self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let now = Instant::now();
        self.payload_bytes = self.payload_bytes.saturating_add(bytes as u64);
        if self.first_payload_at.is_none() {
            self.first_payload_at = Some(now);
        }
        self.last_payload_at = Some(now);
    }

    pub(in crate::runtime) fn rate_sample(self) -> Option<PathRateSample> {
        let first = self.first_payload_at?;
        let last = self.last_payload_at.unwrap_or(first);
        PathRateSample::new(self.payload_bytes, last.duration_since(first))
    }
}

#[cfg(test)]
#[path = "tests_model.rs"]
mod tests;
