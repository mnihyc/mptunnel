use super::bulk_admission::BulkPathCandidate;
use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) struct UdpPathRuntimeModel {
    pub(super) pacing_rate_bps: f64,
    pub(super) response_timeout: Duration,
    pub(super) mtu_payload_bytes: usize,
    pub(super) mtu_is_measured: bool,
    pub(super) mtu_probe_ceiling_payload_bytes: usize,
}

impl UdpPathRuntimeModel {
    pub(super) fn from_snapshot(
        snapshot: PathSnapshot,
        ttl_ms: u32,
        mtu_payload_bytes: usize,
        mtu_is_measured: bool,
        mtu_probe_ceiling_payload_bytes: usize,
    ) -> Self {
        let loss_backoff = (1.0 - snapshot.loss_rate.clamp(0.0, 1.0)).clamp(0.25, 1.0);
        let pacing_rate_bps = (snapshot.delivery_rate_bps * UDP_BBR_PACING_GAIN * loss_backoff)
            .max(UDP_MIN_PACING_RATE_BPS);
        let timeout_loss_gain = 1.0 + snapshot.loss_rate.clamp(0.0, 1.0);
        let model_timeout = Duration::from_secs_f64(
            (((snapshot.srtt_ms + snapshot.jitter_ms.mul_add(4.0, 25.0)) * timeout_loss_gain)
                / 1000.0)
                .max(UDP_MIN_RESPONSE_TIMEOUT.as_secs_f64()),
        );
        let ttl_timeout = Duration::from_millis(u64::from(ttl_ms));
        let response_timeout = model_timeout.min(UDP_MAX_RESPONSE_TIMEOUT).min(ttl_timeout);
        Self {
            pacing_rate_bps,
            response_timeout,
            mtu_payload_bytes,
            mtu_is_measured,
            mtu_probe_ceiling_payload_bytes,
        }
    }

    pub(super) fn accepts_or_can_probe(self, payload_bytes: usize) -> bool {
        payload_bytes <= self.mtu_payload_bytes
            || (!self.mtu_is_measured && payload_bytes <= self.mtu_probe_ceiling_payload_bytes)
    }

    pub(super) fn pacing_interval(self, payload_bytes: usize) -> Duration {
        if payload_bytes == 0 {
            return Duration::ZERO;
        }
        Duration::from_secs_f64(payload_bytes as f64 * 8.0 / self.pacing_rate_bps)
    }
}

pub(super) fn udp_mtu_payload_bytes(
    path: &PathSpec,
    observation: ClientPathObservation,
    max_payload_bytes: usize,
) -> usize {
    let seeded = observation
        .measured_mtu_payload_bytes
        .or(path.metadata.initial_mtu_payload_bytes)
        .unwrap_or(UDP_DEFAULT_MTU_PAYLOAD_BYTES);
    seeded.clamp(
        UDP_MIN_MTU_PAYLOAD_BYTES,
        udp_probe_ceiling_payload_bytes(max_payload_bytes),
    )
}

pub(super) fn udp_probe_ceiling_payload_bytes(max_payload_bytes: usize) -> usize {
    max_payload_bytes.clamp(UDP_MIN_MTU_PAYLOAD_BYTES, UDP_MAX_MTU_PAYLOAD_BYTES)
}

pub(super) fn health_observations(
    records: &mut [ClientPathHealthRecord],
) -> Vec<ClientPathObservation> {
    let now = Instant::now();
    records
        .iter_mut()
        .map(|record| record.observe(now))
        .collect()
}

pub(super) fn path_records_have_schedulable_alternative(
    records: &mut [ClientPathHealthRecord],
    failed_index: usize,
    now: Instant,
) -> bool {
    records.iter_mut().enumerate().any(|(index, record)| {
        index != failed_index
            && !matches!(
                record.observe(now).state,
                SchedulerPathState::Failed | SchedulerPathState::Draining
            )
    })
}

pub(super) fn path_observation_is_idle_for_probe(observation: ClientPathObservation) -> bool {
    observation.active_flows == 0
}

pub(super) fn apply_tcp_bulk_isolation(
    observations: &mut [ClientPathObservation],
    lane: FlowLane,
    mux_limits: MuxLimits,
) {
    if !matches!(lane, FlowLane::Throughput | FlowLane::Background) {
        return;
    }
    if !observations
        .iter()
        .any(|observation| observation.measured_rate_bps.is_some())
    {
        return;
    }
    let isolation_bytes =
        adaptive_reliable_relay_inflight_bytes(None, FlowLane::Latency, mux_limits) as u64;
    for observation in observations {
        let latency_flows = u64::from(observation.active_latency_sensitive_flows);
        observation.relay_queue_bytes = observation
            .relay_queue_bytes
            .saturating_add(latency_flows.saturating_mul(isolation_bytes));
    }
}

pub(super) fn reliable_stream_latency_startup_should_use_configured_order(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
) -> bool {
    reliable_relay_expects_interactive_response(lane)
        && paths.iter().all(path_is_endpoint_only)
        && (!endpoint_only_startup_has_latency_sensitive_load(observations)
            || endpoint_only_startup_has_bulk_load(observations))
}

pub(super) fn reliable_stream_latency_startup_should_use_load_balanced_order(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
) -> bool {
    reliable_relay_expects_interactive_response(lane)
        && paths.iter().all(path_is_endpoint_only)
        && endpoint_only_startup_has_latency_sensitive_load(observations)
        && !endpoint_only_startup_has_bulk_load(observations)
}

pub(super) fn endpoint_only_startup_has_latency_sensitive_load(
    observations: &[ClientPathObservation],
) -> bool {
    observations
        .iter()
        .any(|observation| observation.active_latency_sensitive_flows > 0)
}

pub(super) fn endpoint_only_startup_has_any_load(observations: &[ClientPathObservation]) -> bool {
    observations
        .iter()
        .any(|observation| observation.active_flows > 0)
}

pub(super) fn endpoint_only_startup_has_bulk_load(observations: &[ClientPathObservation]) -> bool {
    observations
        .iter()
        .any(|observation| observation.active_flows > observation.active_latency_sensitive_flows)
}

pub(super) fn endpoint_only_tcp_startup_should_spread_bulk_load(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
    active_udp_work: bool,
) -> bool {
    reliable_relay_expects_interactive_response(lane)
        && paths.iter().all(path_is_endpoint_only)
        && endpoint_only_startup_has_any_load(observations)
        && endpoint_only_startup_has_bulk_load(observations)
        && !endpoint_only_startup_has_latency_sensitive_load(observations)
        && !active_udp_work
}

pub(super) fn ordered_reliable_path_indices(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<usize> {
    reliable_stream_startup_path_scores(paths, observations, lane, payload_bytes)
        .into_iter()
        .map(|(index, _)| index)
        .collect()
}

pub(super) fn reliable_stream_startup_path_scores(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<(usize, f64)> {
    if reliable_stream_latency_startup_should_use_configured_order(paths, observations, lane) {
        return configured_order_path_scores(paths, observations, lane, payload_bytes);
    }
    if reliable_stream_latency_startup_should_use_load_balanced_order(paths, observations, lane) {
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
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<(usize, f64)> {
    if reliable_stream_latency_startup_should_use_configured_order(paths, observations, lane)
        || reliable_stream_latency_startup_should_use_load_balanced_order(paths, observations, lane)
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

pub(super) fn endpoint_only_reliable_startup_path_scores(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<(usize, f64)> {
    let observations = observations
        .iter()
        .copied()
        .map(|observation| ClientPathObservation {
            measured_srtt_ms: None,
            measured_jitter_ms: None,
            measured_rate_bps: None,
            measured_loss_rate: None,
            measured_mtu_payload_bytes: observation.measured_mtu_payload_bytes,
            delivery_samples: 0,
            last_delivery_at: None,
            carrier_srtt_ms: None,
            carrier_rttvar_ms: None,
            carrier_delivery_rate_bps: None,
            carrier_delivery_samples: 0,
            carrier_last_delivery_at: None,
            ..observation
        })
        .collect::<Vec<_>>();
    ordered_path_scores(paths, &observations, lane, payload_bytes)
}

pub(super) fn path_is_endpoint_only(path: &PathSpec) -> bool {
    path.metadata.initial_srtt_ms.is_none()
        && path.metadata.initial_jitter_ms.is_none()
        && path.metadata.initial_rate == RateHint::Unknown
        && path.metadata.capabilities == crate::protocol::PathCapabilities::default()
}

pub(super) fn path_has_configured_performance_hint(path: &PathSpec) -> bool {
    path.metadata.initial_srtt_ms.is_some()
        || path.metadata.initial_jitter_ms.is_some()
        || path.metadata.initial_rate != RateHint::Unknown
}

pub(super) fn configured_order_path_indices(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<usize> {
    configured_order_path_scores(paths, observations, lane, payload_bytes)
        .into_iter()
        .map(|(index, _)| index)
        .collect()
}

pub(super) fn configured_order_path_scores(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<(usize, f64)> {
    paths
        .iter()
        .enumerate()
        .filter_map(|(index, path)| {
            let observation = observations.get(index).copied().unwrap_or_default();
            scheduler::score_path(
                path_snapshot(path, index, observation),
                lane,
                payload_bytes,
                SchedulerPolicy::default(),
            )
            .map(|score| (index, score.eta_ms))
        })
        .collect()
}

pub(super) fn ordered_path_scores_for_ttl(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
    payload_bytes: usize,
    ttl_ms: u32,
) -> Vec<(usize, f64)> {
    let scores = ordered_path_scores(paths, observations, lane, payload_bytes);
    let freshness_budget_ms = f64::from(ttl_ms) * UDP_DATAGRAM_MIN_TTL_FIT_RATIO;
    scores
        .iter()
        .copied()
        .filter(|(_, eta_ms)| *eta_ms <= freshness_budget_ms)
        .collect::<Vec<_>>()
}

pub(super) fn ordered_path_scores(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<(usize, f64)> {
    let mut scores = paths
        .iter()
        .enumerate()
        .filter_map(|(index, path)| {
            let observation = observations.get(index).copied().unwrap_or_default();
            scheduler::score_path(
                path_snapshot(path, index, observation),
                lane,
                payload_bytes,
                SchedulerPolicy::default(),
            )
            .map(|score| (index, score.eta_ms))
        })
        .collect::<Vec<_>>();
    scores.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    scores
}

pub(super) fn reliable_stream_path_candidates(
    tcp_paths: &[PathSpec],
    tcp_observations: &[ClientPathObservation],
    udp_paths: &[PathSpec],
    udp_observations: &[ClientPathObservation],
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<BulkPathCandidate> {
    let active_udp_work = endpoint_only_startup_has_any_load(udp_observations);
    let tcp_scores = if endpoint_only_tcp_startup_should_spread_bulk_load(
        tcp_paths,
        tcp_observations,
        lane,
        active_udp_work,
    ) {
        endpoint_only_reliable_startup_path_scores(tcp_paths, tcp_observations, lane, payload_bytes)
    } else {
        reliable_stream_mixed_startup_path_scores(tcp_paths, tcp_observations, lane, payload_bytes)
    };
    let udp_scores =
        reliable_stream_mixed_startup_path_scores(udp_paths, udp_observations, lane, payload_bytes);

    let mut candidates = tcp_scores
        .into_iter()
        .filter_map(|(index, eta_ms)| {
            let path = tcp_paths.get(index)?;
            let observation = tcp_observations.get(index).copied().unwrap_or_default();
            let snapshot = path_snapshot(path, index, observation);
            path_can_be_auto_discovered_for_lane(path, observation, lane).then_some(
                BulkPathCandidate {
                    key: RelayPathKey {
                        underlay: UnderlayProtocol::Tcp,
                        index,
                    },
                    eta_ms: eta_ms
                        + reliable_stream_initial_lane_protection_penalty(snapshot, lane),
                    has_evidence: bulk_candidate_has_evidence(path, observation),
                    has_sender_delivery_evidence: bulk_candidate_has_sender_delivery_evidence(
                        observation,
                    ),
                    has_configured_performance_hint: path_has_configured_performance_hint(path),
                    snapshot,
                },
            )
        })
        .chain(udp_scores.into_iter().filter_map(|(index, eta_ms)| {
            let path = udp_paths.get(index)?;
            let observation = udp_observations.get(index).copied().unwrap_or_default();
            let snapshot = path_snapshot(path, index, observation);
            let eta_ms = eta_ms
                + if matches!(lane, FlowLane::Throughput | FlowLane::Background) {
                    udp_reliable_stream_loss_repair_penalty_ms(snapshot, payload_bytes)
                } else {
                    0.0
                }
                + reliable_stream_initial_lane_protection_penalty(snapshot, lane);
            path_can_be_auto_discovered_for_lane(path, observation, lane).then_some(
                BulkPathCandidate {
                    key: RelayPathKey {
                        underlay: UnderlayProtocol::Udp,
                        index,
                    },
                    eta_ms,
                    has_evidence: bulk_candidate_has_evidence(path, observation),
                    has_sender_delivery_evidence: bulk_candidate_has_sender_delivery_evidence(
                        observation,
                    ),
                    has_configured_performance_hint: path_has_configured_performance_hint(path),
                    snapshot,
                },
            )
        }))
        .collect::<Vec<_>>();
    retain_safe_mixed_latency_startup_candidates(&mut candidates, lane);
    candidates
}

pub(super) fn reliable_stream_initial_underlay_order(
    lane: FlowLane,
    underlay: UnderlayProtocol,
) -> u8 {
    match (lane, underlay) {
        (
            FlowLane::Throughput | FlowLane::Background | FlowLane::RealtimeDatagram,
            UnderlayProtocol::Udp,
        ) => 0,
        (
            FlowLane::Throughput | FlowLane::Background | FlowLane::RealtimeDatagram,
            UnderlayProtocol::Tcp,
        ) => 1,
        (_, UnderlayProtocol::Tcp) => 0,
        (_, UnderlayProtocol::Udp) => 1,
    }
}

fn reliable_stream_initial_lane_protection_penalty(snapshot: PathSnapshot, lane: FlowLane) -> f64 {
    if snapshot.underlay == UnderlayProtocol::Udp
        && matches!(lane, FlowLane::Control | FlowLane::Latency)
        && snapshot.active_latency_sensitive_flows > 0
    {
        snapshot.srtt_ms.max(1.0)
    } else {
        0.0
    }
}

pub(super) fn path_snapshot(
    path: &PathSpec,
    index: usize,
    observation: ClientPathObservation,
) -> PathSnapshot {
    let hinted_delivery_rate_bps = match path.metadata.initial_rate {
        RateHint::Unknown => default_path_rate_bps(path.underlay),
        RateHint::Unlimited => 1_000_000_000_000.0,
        RateHint::BitsPerSecond(rate) => rate.max(1) as f64,
    };
    let delivery_rate_bps = observation
        .carrier_delivery_rate_bps
        .or(observation.measured_rate_bps)
        .unwrap_or(hinted_delivery_rate_bps)
        .max(1.0);
    let srtt_ms = path_model_srtt_ms(path, observation);
    let jitter_ms = observation
        .carrier_rttvar_ms
        .or(observation.measured_jitter_ms)
        .unwrap_or_else(|| f64::from(path.metadata.initial_jitter_ms.unwrap_or(0)));
    let confidence = path_model_confidence(observation);
    let bdp_bytes = (delivery_rate_bps / 8.0 * srtt_ms.max(1.0) / 1000.0)
        .ceil()
        .max(PATH_OPEN_SCORE_BYTES as f64) as u64;
    let pacing_rate_bps = delivery_rate_bps
        * (1.0
            - observation
                .measured_loss_rate
                .unwrap_or(0.0)
                .clamp(0.0, 0.75)
                * 0.5)
            .clamp(0.25, 1.0);
    let inflight_limit_bytes = if observation.carrier_inflight_limit_bytes > 0 {
        observation.carrier_inflight_limit_bytes
    } else {
        bdp_bytes
            .saturating_mul(2)
            .max(PATH_OPEN_SCORE_BYTES as u64)
    };
    PathSnapshot {
        id: PathId(index as u16),
        underlay: path.underlay,
        state: observation.state,
        flags: path.metadata.capabilities.into(),
        srtt_ms,
        jitter_ms,
        delivery_rate_bps,
        product_progress_rate_bps: None,
        loss_rate: observation.measured_loss_rate.unwrap_or(0.0),
        queue_bytes: observation.carrier_queue_bytes,
        product_queue_bytes: observation.relay_queue_bytes,
        bytes_in_flight: observation
            .relay_bytes_in_flight
            .saturating_add(observation.carrier_bytes_in_flight),
        product_bytes_in_flight: observation.relay_bytes_in_flight,
        active_flows: observation.active_flows,
        active_latency_sensitive_flows: observation.active_latency_sensitive_flows,
        session_active_latency_sensitive_flows: observation.active_latency_sensitive_flows,
        pacing_rate_bps,
        inflight_limit_bytes,
        confidence,
        app_limited: observation.relay_bytes_in_flight == 0
            && observation.carrier_bytes_in_flight == 0
            && observation.carrier_app_limited,
    }
}

pub(super) fn path_metrics_from_snapshot(
    snapshot: PathSnapshot,
    observation: ClientPathObservation,
    direction: PathMetricDirection,
) -> PathMetrics {
    let data_sample_count = observation
        .delivery_samples
        .saturating_add(observation.carrier_delivery_samples);
    PathMetrics {
        path_id: snapshot.id,
        underlay: snapshot.underlay,
        direction,
        metric_epoch: metric_epoch_now(),
        metric_age_us: 0,
        min_rtt_us: millis_to_micros_u32(snapshot.srtt_ms),
        srtt_us: millis_to_micros_u32(snapshot.srtt_ms),
        rttvar_us: millis_to_micros_u32(snapshot.jitter_ms.max(0.0)),
        jitter_us: millis_to_micros_u32(snapshot.jitter_ms.max(0.0)),
        delivery_rate_bps: snapshot.delivery_rate_bps.max(1.0).round() as u64,
        pacing_rate_bps: snapshot.pacing_rate_bps.max(1.0).round() as u64,
        loss_ppm: (snapshot.loss_rate.clamp(0.0, 1.0) * 1_000_000.0).round() as u32,
        ecn_ppm: 0,
        bytes_in_flight: snapshot.bytes_in_flight,
        queue_bytes: snapshot.queue_bytes,
        inflight_limit_bytes: snapshot.inflight_limit_bytes,
        inflight_hi_bytes: snapshot.inflight_limit_bytes,
        confidence_ppm: ratio_to_ppm(snapshot.confidence),
        app_limited: snapshot.app_limited,
        has_ack_derived_data_sample: data_sample_count > 0,
        data_sample_count,
    }
}

pub(super) fn metric_epoch_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

pub(super) fn ratio_to_ppm(value: f64) -> u32 {
    (value.clamp(0.0, 1.0) * 1_000_000.0).round() as u32
}

fn millis_to_micros_u32(ms: f64) -> u32 {
    let micros = (ms.max(0.0) * 1000.0).round();
    micros.clamp(0.0, f64::from(u32::MAX)) as u32
}

pub(super) fn path_model_srtt_ms(path: &PathSpec, observation: ClientPathObservation) -> f64 {
    observation
        .carrier_srtt_ms
        .or(observation.measured_srtt_ms)
        .unwrap_or_else(|| {
            path.metadata
                .initial_srtt_ms
                .map_or_else(|| default_path_srtt_ms(path.underlay), f64::from)
        })
}

pub(super) fn path_model_confidence(observation: ClientPathObservation) -> f64 {
    let delivery_confidence = (f64::from(
        observation
            .delivery_samples
            .saturating_add(observation.carrier_delivery_samples),
    ) / 8.0)
        .clamp(0.0, 1.0);
    let rtt_confidence = if observation
        .carrier_srtt_ms
        .or(observation.measured_srtt_ms)
        .is_some()
    {
        0.35
    } else {
        0.0
    };
    let freshness_confidence = observation
        .last_delivery_at
        .into_iter()
        .chain(observation.carrier_last_delivery_at)
        .max()
        .map(|seen| {
            let age = Instant::now().saturating_duration_since(seen).as_secs_f64();
            (1.0 - age / 30.0).clamp(0.0, 1.0) * 0.25
        })
        .unwrap_or(0.0);
    (delivery_confidence + rtt_confidence + freshness_confidence).clamp(0.1, 1.0)
}

pub(super) fn udp_path_has_realtime_model(
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

pub(super) fn udp_observation_has_datagram_feedback(observation: &ClientPathObservation) -> bool {
    observation.measured_jitter_ms.is_some()
        || observation.measured_loss_rate.is_some()
        || observation.measured_rate_bps.is_some()
        || observation.carrier_delivery_rate_bps.is_some()
        || observation.measured_mtu_payload_bytes.is_some()
}

pub(super) fn path_can_be_auto_discovered(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> bool {
    observation.state == SchedulerPathState::Active
        && !path.metadata.capabilities.expensive
        && !path.metadata.capabilities.backup
        && !path.metadata.capabilities.probe_only
        && path.metadata.capabilities.bulk_allowed
}

fn path_can_be_auto_discovered_for_lane(
    path: &PathSpec,
    observation: ClientPathObservation,
    lane: FlowLane,
) -> bool {
    observation.state == SchedulerPathState::Active
        && !path.metadata.capabilities.expensive
        && !path.metadata.capabilities.backup
        && !path.metadata.capabilities.probe_only
        && (!matches!(lane, FlowLane::Throughput | FlowLane::Background)
            || path.metadata.capabilities.bulk_allowed)
}

pub(super) fn bulk_candidate_has_evidence(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> bool {
    observation.delivery_samples > 0
        || observation.carrier_delivery_samples > 0
        || observation.last_delivery_at.is_some()
        || observation.carrier_last_delivery_at.is_some()
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

pub(super) fn bulk_candidate_has_delivery_evidence(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> bool {
    observation.delivery_samples > 0
        || observation.carrier_delivery_samples > 0
        || observation.last_delivery_at.is_some()
        || observation.carrier_last_delivery_at.is_some()
        || observation.measured_rate_bps.is_some()
        || observation.carrier_delivery_rate_bps.is_some()
        || path.metadata.initial_rate != RateHint::Unknown
}

pub(super) fn bulk_candidate_has_sender_delivery_evidence(
    observation: ClientPathObservation,
) -> bool {
    observation.delivery_samples > 0
        || observation.carrier_delivery_samples > 0
        || observation.last_delivery_at.is_some()
        || observation.carrier_last_delivery_at.is_some()
        || observation.carrier_delivery_rate_bps.is_some()
}

pub(super) fn bulk_candidate_has_active_bulk_work(candidate: &BulkPathCandidate) -> bool {
    candidate.snapshot.active_flows > candidate.snapshot.active_latency_sensitive_flows
}

pub(super) fn bulk_candidates_span_underlays(candidates: &[BulkPathCandidate]) -> bool {
    let Some(first) = candidates.first() else {
        return false;
    };
    candidates
        .iter()
        .any(|candidate| candidate.key.underlay != first.key.underlay)
}

fn retain_safe_mixed_latency_startup_candidates(
    candidates: &mut Vec<BulkPathCandidate>,
    lane: FlowLane,
) {
    if matches!(lane, FlowLane::Throughput | FlowLane::Background)
        || !bulk_candidates_span_underlays(candidates)
    {
        return;
    }
    if candidates
        .iter()
        .any(|candidate| candidate.has_sender_delivery_evidence)
        || candidates
            .iter()
            .any(|candidate| candidate.has_configured_performance_hint)
    {
        return;
    }
    if candidates
        .iter()
        .any(|candidate| candidate.key.underlay == UnderlayProtocol::Tcp)
    {
        candidates.retain(|candidate| candidate.key.underlay == UnderlayProtocol::Tcp);
    }
}

pub(super) fn carrier_diverse_bulk_validation_order(
    candidates: Vec<BulkPathCandidate>,
) -> Vec<BulkPathCandidate> {
    if !bulk_candidates_span_underlays(&candidates) {
        return candidates;
    }
    let mut remaining = candidates;
    let mut ordered = Vec::with_capacity(remaining.len());
    for underlay in [UnderlayProtocol::Udp, UnderlayProtocol::Tcp] {
        if let Some(position) = remaining
            .iter()
            .position(|candidate| candidate.key.underlay == underlay)
        {
            ordered.push(remaining.remove(position));
        }
    }
    ordered.extend(remaining);
    ordered
}

pub(super) fn udp_reliable_stream_loss_repair_penalty_ms(
    snapshot: scheduler::PathSnapshot,
    payload_bytes: usize,
) -> f64 {
    let loss = snapshot.loss_rate.clamp(0.0, 0.75);
    if loss <= f64::EPSILON {
        return 0.0;
    }
    let fragment_count = (payload_bytes as f64 / UDP_DEFAULT_MTU_PAYLOAD_BYTES as f64)
        .ceil()
        .max(1.0);
    let expected_repairs = fragment_count * loss / (1.0 - loss).max(0.01);
    let repair_rtt_ms = snapshot.srtt_ms + snapshot.jitter_ms.max(0.0) * 4.0;
    expected_repairs * repair_rtt_ms
}

pub(super) fn default_path_srtt_ms(underlay: UnderlayProtocol) -> f64 {
    match underlay {
        UnderlayProtocol::Tcp | UnderlayProtocol::Udp => 50.0,
    }
}

pub(super) fn default_path_rate_bps(underlay: UnderlayProtocol) -> f64 {
    match underlay {
        UnderlayProtocol::Tcp | UnderlayProtocol::Udp => 100_000_000.0,
    }
}
