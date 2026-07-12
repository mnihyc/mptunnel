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
        let ttl_timeout = Duration::from_millis(u64::from(ttl_ms));
        let pto = transport_pto_from_snapshot(Some(snapshot));
        let loss_backoff = datagram_loss_backoff(snapshot, mtu_payload_bytes);
        let min_pacing_rate_bps = datagram_min_pacing_rate_bps(mtu_payload_bytes, pto);
        let pacing_rate_bps = (snapshot.delivery_rate_bps * loss_backoff).max(min_pacing_rate_bps);
        let timeout_loss_gain = 1.0 + snapshot.loss_rate.clamp(0.0, 1.0);
        let response_timeout = pto.mul_f64(timeout_loss_gain).min(ttl_timeout);
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

pub(super) fn transport_pto_from_ms(srtt_ms: f64, rttvar_ms: f64) -> Duration {
    let srtt = Duration::from_secs_f64(srtt_ms.max(0.0) / 1000.0);
    let rttvar = Duration::from_secs_f64(rttvar_ms.max(0.0) / 1000.0);
    srtt + (rttvar * 4).max(QUIC_TIMER_GRANULARITY) + QUIC_MAX_ACK_DELAY
}

pub(super) fn quic_bulk_proof_freshness_horizon(srtt: Duration, rttvar: Duration) -> Duration {
    // A rate proof loses placement rights at the same three-PTO boundary where
    // QUIC declares persistent congestion; reachability evidence is separate.
    transport_pto_from_ms(srtt.as_secs_f64() * 1000.0, rttvar.as_secs_f64() * 1000.0)
        .saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD)
}

pub(super) fn transport_pto_from_snapshot(path: Option<PathSnapshot>) -> Duration {
    path.map(|path| {
        let srtt_ms = path.srtt_ms.max(1.0);
        let rttvar_ms = path.jitter_ms.max(srtt_ms / 8.0);
        transport_pto_from_ms(srtt_ms, rttvar_ms)
    })
    .unwrap_or_else(default_transport_pto)
}

pub(super) fn path_open_pto(path: Option<PathSnapshot>, rtt_is_observed: bool) -> Duration {
    let path_pto = transport_pto_from_snapshot(path);
    if rtt_is_observed {
        path_pto
    } else {
        path_pto.max(default_transport_pto())
    }
}

pub(super) fn active_path_open_timeout(
    path: Option<PathSnapshot>,
    rtt_is_observed: bool,
) -> Duration {
    path_open_pto(path, rtt_is_observed).saturating_mul(active_path_open_pto_multiplier(path))
}

pub(super) fn active_path_open_pto_multiplier(path: Option<PathSnapshot>) -> u32 {
    active_path_open_serialized_exchanges(path)
        .saturating_sub(1)
        .saturating_add(persistent_congestion_pto_backoff_multiplier())
}

pub(super) fn persistent_congestion_pto_backoff_multiplier() -> u32 {
    (0..QUIC_PERSISTENT_CONGESTION_THRESHOLD).fold(0_u32, |total, exponent| {
        total.saturating_add(1_u32.checked_shl(exponent).unwrap_or(u32::MAX))
    })
}

pub(super) fn active_path_open_serialized_exchanges(path: Option<PathSnapshot>) -> u32 {
    match path.map(|snapshot| snapshot.underlay) {
        Some(UnderlayProtocol::Udp) => 2,
        Some(UnderlayProtocol::Tcp) | None => 3,
    }
}

pub(super) fn default_transport_pto() -> Duration {
    transport_pto_from_ms(
        RELIABLE_INITIAL_RTT.as_secs_f64() * 1000.0,
        RELIABLE_INITIAL_RTT.as_secs_f64() * 1000.0 / 2.0,
    )
}

pub(super) fn path_record_failure_cooldown(record: &ClientPathHealthRecord) -> Duration {
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

pub(super) fn apply_bulk_latency_isolation(
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

pub(super) fn endpoint_only_reliable_startup_should_preserve_configured_order(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
) -> bool {
    reliable_relay_expects_interactive_response(lane)
        && paths.iter().all(path_is_endpoint_only)
        && !endpoint_only_startup_has_latency_sensitive_load(observations)
        && !endpoint_only_startup_has_bulk_load(observations)
}

pub(super) fn endpoint_only_reliable_startup_should_spread_latency_load(
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

pub(super) fn endpoint_only_reliable_startup_should_spread_bulk_load(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
) -> bool {
    reliable_relay_expects_interactive_response(lane)
        && paths.iter().all(path_is_endpoint_only)
        && endpoint_only_startup_has_any_load(observations)
        && endpoint_only_startup_has_bulk_load(observations)
        && !endpoint_only_startup_has_latency_sensitive_load(observations)
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
    lane: FlowLane,
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

pub(super) fn endpoint_only_reliable_startup_path_scores(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
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
    ClientPathObservation {
        measured_srtt_ms: None,
        measured_jitter_ms: None,
        measured_rate_bps: None,
        measured_loss_rate: None,
        measured_mtu_payload_bytes: observation.measured_mtu_payload_bytes,
        delivery_samples: 0,
        product_delivery_sample_bytes: 0,
        last_delivery_at: None,
        carrier_srtt_ms: None,
        carrier_rttvar_ms: None,
        carrier_delivery_rate_bps: None,
        carrier_delivery_samples: 0,
        carrier_delivery_sample_bytes: 0,
        carrier_last_delivery_at: None,
        ..observation
    }
}

pub(super) fn path_is_endpoint_only(path: &PathSpec) -> bool {
    path.metadata.initial_srtt_ms.is_none()
        && path.metadata.initial_jitter_ms.is_none()
        && path.metadata.initial_rate == RateHint::Unknown
        && path.metadata.capabilities == crate::protocol::PathCapabilities::default()
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
    let freshness_budget_ms = f64::from(ttl_ms);
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
                + if matches!(lane, FlowLane::Throughput | FlowLane::Background) {
                    udp_reliable_stream_loss_repair_penalty_ms(snapshot, payload_bytes)
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
                    + if matches!(lane, FlowLane::Throughput | FlowLane::Background) {
                        udp_reliable_stream_loss_repair_penalty_ms(snapshot, payload_bytes)
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

pub(super) fn bulk_path_candidate(
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
    let product_progress_rate_bps = (reliable_product_delivery_samples(path, observation) > 0
        && observation.product_delivery_sample_bytes > 0)
        .then_some(observation.measured_rate_bps)
        .flatten();
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
    let loss = observation
        .measured_loss_rate
        .unwrap_or(0.0)
        .clamp(0.0, 1.0);
    let min_progress =
        adaptive_transport_byte_floor_factor(PATH_OPEN_SCORE_BYTES as f64, bdp_bytes.max(1) as f64);
    let pacing_rate_bps = delivery_rate_bps * (1.0 - loss).max(min_progress);
    let inflight_limit_bytes = if observation.carrier_inflight_limit_bytes > 0 {
        observation.carrier_inflight_limit_bytes
    } else {
        ((bdp_bytes as f64) * BBR_DEFAULT_CWND_GAIN).ceil() as u64
    };
    let inflight_limit_bytes = inflight_limit_bytes.max(PATH_OPEN_SCORE_BYTES as u64);
    PathSnapshot {
        id: PathId(index as u16),
        underlay: path.underlay,
        state: observation.state,
        flags: path.metadata.capabilities.into(),
        srtt_ms,
        min_rtt_ms: srtt_ms,
        jitter_ms,
        delivery_rate_bps,
        product_progress_rate_bps,
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

pub(super) fn path_startup_snapshot(path: &PathSpec, index: usize) -> PathSnapshot {
    path_snapshot(
        path,
        index,
        ClientPathObservation {
            state: SchedulerPathState::Active,
            carrier_app_limited: path.metadata.initial_rate == RateHint::Unknown,
            ..ClientPathObservation::default()
        },
    )
}

pub(super) fn path_startup_metrics(
    path: &PathSpec,
    index: usize,
    direction: PathMetricDirection,
) -> PathMetrics {
    let observation = ClientPathObservation {
        state: SchedulerPathState::Active,
        carrier_app_limited: path.metadata.initial_rate == RateHint::Unknown,
        ..ClientPathObservation::default()
    };
    path_metrics_from_snapshot(
        path_snapshot(path, index, observation),
        observation,
        direction,
    )
}

pub(super) fn path_metrics_from_snapshot(
    snapshot: PathSnapshot,
    observation: ClientPathObservation,
    direction: PathMetricDirection,
) -> PathMetrics {
    let data_sample_count = observation
        .delivery_samples
        .saturating_add(observation.carrier_delivery_samples);
    let has_ack_derived_data_sample =
        data_sample_count > 0 || observation.carrier_ack_derived_data_seen;
    PathMetrics {
        path_id: snapshot.id,
        underlay: snapshot.underlay,
        direction,
        metric_epoch: metric_epoch_now(),
        metric_age_us: 0,
        min_rtt_us: millis_to_micros_u32(snapshot.min_rtt_ms),
        srtt_us: millis_to_micros_u32(snapshot.srtt_ms),
        rttvar_us: millis_to_micros_u32(snapshot.jitter_ms.max(0.0)),
        jitter_us: millis_to_micros_u32(snapshot.jitter_ms.max(0.0)),
        delivery_rate_bps: snapshot.delivery_rate_bps.max(1.0).round() as u64,
        pacing_rate_bps: snapshot.pacing_rate_bps.max(1.0).round() as u64,
        loss_ppm: (snapshot.loss_rate.clamp(0.0, 1.0) * 1_000_000.0).round() as u32,
        ecn_ppm: 0,
        loss_observed: observation.delivery_samples > 0 || observation.carrier_delivery_samples > 0,
        ecn_observed: false,
        bytes_in_flight: snapshot.bytes_in_flight,
        queue_bytes: snapshot.queue_bytes,
        inflight_limit_bytes: snapshot.inflight_limit_bytes,
        inflight_hi_bytes: snapshot.inflight_limit_bytes,
        confidence_ppm: ratio_to_ppm(snapshot.confidence),
        app_limited: snapshot.app_limited,
        has_ack_derived_data_sample,
        data_sample_count,
        data_sample_bytes: observation.carrier_delivery_sample_bytes,
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

pub(super) fn path_within_adaptive_lead_hysteresis(
    old_eta_ms: f64,
    old_snapshot: PathSnapshot,
    best_eta_ms: f64,
    best_snapshot: PathSnapshot,
    payload_bytes: usize,
) -> bool {
    let jitter_hysteresis_ms = old_snapshot.jitter_ms.max(best_snapshot.jitter_ms);
    let queue_hysteresis_bytes = payload_bytes as u64;
    old_eta_ms <= best_eta_ms + jitter_hysteresis_ms
        && old_snapshot.queue_bytes <= best_snapshot.queue_bytes + queue_hysteresis_bytes
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

fn path_can_be_recovery_candidate_for_lane(
    path: &PathSpec,
    observation: ClientPathObservation,
    lane: FlowLane,
) -> bool {
    !matches!(
        observation.state,
        SchedulerPathState::Failed | SchedulerPathState::Draining
    ) && !path.metadata.capabilities.probe_only
        && (!matches!(lane, FlowLane::Throughput | FlowLane::Background)
            || path.metadata.capabilities.bulk_allowed)
}

pub(super) fn bulk_candidate_has_liveness_evidence(
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

pub(super) fn bulk_candidate_has_path_proof_evidence(observation: ClientPathObservation) -> bool {
    observation.path_proof_success
}

pub(super) fn bulk_candidate_has_ack_data_evidence(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> bool {
    reliable_product_delivery_samples(path, observation) > 0
        || observation.carrier_delivery_samples > 0
        || observation.last_delivery_at.is_some()
        || observation.carrier_last_delivery_at.is_some()
        || observation.carrier_ack_derived_data_seen
}

pub(super) fn bulk_candidate_has_bulk_rate_evidence(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> bool {
    let product_rate = observation.measured_rate_bps.is_some()
        && reliable_product_delivery_samples(path, observation) > 0
        && observation.product_delivery_sample_bytes
            >= client_path_observation_bulk_sample_floor_bytes(observation);
    product_rate
        || (observation.carrier_delivery_rate_bps.is_some()
            && !observation.carrier_app_limited
            && observation.carrier_delivery_sample_bytes
                >= client_path_observation_bulk_sample_floor_bytes(observation))
}

pub(super) fn bulk_candidate_has_sender_delivery_evidence(
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

fn client_path_observation_bulk_sample_floor_bytes(observation: ClientPathObservation) -> u64 {
    observation
        .carrier_inflight_limit_bytes
        .max(BBR_MAX_SEND_QUANTUM_BYTES as u64)
        .max(PATH_OPEN_SCORE_BYTES as u64)
}

fn reliable_product_delivery_samples(path: &PathSpec, observation: ClientPathObservation) -> u32 {
    match path.underlay {
        UnderlayProtocol::Udp => observation
            .delivery_samples
            .saturating_sub(observation.datagram_feedback_samples),
        UnderlayProtocol::Tcp => observation.delivery_samples,
    }
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

pub(super) fn carrier_diverse_bulk_validation_order(
    candidates: Vec<BulkPathCandidate>,
) -> Vec<BulkPathCandidate> {
    if !bulk_candidates_span_underlays(&candidates) {
        return candidates;
    }
    let mut ordered = Vec::with_capacity(candidates.len());
    let mut remaining = Vec::new();
    let mut saw_tcp = false;
    let mut saw_udp = false;
    for candidate in candidates {
        let first_for_underlay = match candidate.key.underlay {
            UnderlayProtocol::Tcp if !saw_tcp => {
                saw_tcp = true;
                true
            }
            UnderlayProtocol::Udp if !saw_udp => {
                saw_udp = true;
                true
            }
            _ => false,
        };
        if first_for_underlay {
            ordered.push(candidate);
        } else {
            remaining.push(candidate);
        }
    }
    ordered.extend(remaining);
    ordered
}

pub(super) fn udp_reliable_stream_loss_repair_penalty_ms(
    snapshot: scheduler::PathSnapshot,
    payload_bytes: usize,
) -> f64 {
    let loss = snapshot.loss_rate.clamp(0.0, 1.0);
    if loss <= f64::EPSILON {
        return 0.0;
    }
    let fragment_count = (payload_bytes as f64 / UDP_DEFAULT_MTU_PAYLOAD_BYTES as f64)
        .ceil()
        .max(1.0);
    let bdp_bytes = path_bdp_floor_bytes(snapshot).max(UDP_DEFAULT_MTU_PAYLOAD_BYTES as f64);
    let progress_floor = (UDP_DEFAULT_MTU_PAYLOAD_BYTES as f64 / bdp_bytes).min(1.0);
    let expected_repairs = fragment_count * loss / (1.0 - loss).max(progress_floor);
    let repair_rtt_ms = transport_pto_from_snapshot(Some(snapshot)).as_secs_f64() * 1000.0;
    expected_repairs * repair_rtt_ms
}

pub(super) fn default_path_srtt_ms(underlay: UnderlayProtocol) -> f64 {
    let _ = underlay;
    RELIABLE_INITIAL_RTT.as_secs_f64() * 1000.0
}

pub(super) fn default_path_rate_bps(underlay: UnderlayProtocol) -> f64 {
    let _ = underlay;
    PATH_OPEN_SCORE_BYTES as f64 * 8.0 / RELIABLE_INITIAL_RTT.as_secs_f64()
}
