use super::*;
use crate::config::ResourceLimits;
use crate::protocol::PathMetricDirection;
#[cfg(feature = "lab-diagnostics")]
use crate::runtime::path::quic::metrics::QuicAckPollDiagnostics;
use crate::runtime::path::quic::metrics::UdpPathMetrics;

#[test]
fn mixed_bulk_striping_includes_unmeasured_endpoint_only_udp() {
    let tcp_path = "tcp://127.0.0.1:10142"
        .parse::<PathSpec>()
        .expect("tcp path");
    let udp_path = "udp://127.0.0.1:10143"
        .parse::<PathSpec>()
        .expect("udp path");
    let context = ClientPathContext::new(
        vec![tcp_path, udp_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_udp_path_probe_success(0, Duration::from_millis(1));

    assert!(
        context
            .ordered_reliable_bulk_striping_path_keys(
                MuxLimits::default().max_reliable_relay_chunk_bytes
            )
            .contains(&RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: 0,
            })
    );
}

#[test]
fn measured_udp_delivery_rate_updates_next_datagram_order() {
    let hinted_slow_path = "udp://127.0.0.1:10019?srtt-ms=20&rate-mbps=10"
        .parse::<PathSpec>()
        .expect("hinted slow path");
    let hinted_fast_path = "udp://127.0.0.1:10020?srtt-ms=20&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("hinted fast path");
    let context = ClientPathContext::new(
        vec![hinted_slow_path, hinted_fast_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        udp_candidate_indices(&context, 1024 * 1024, DEFAULT_SOCKS5_UDP_TTL_MS)
            .first()
            .copied(),
        Some(1)
    );

    context.mark_udp_datagram_path_delivery(
        0,
        PathDeliveryStats {
            payload_bytes: 1024 * 1024,
            first_payload_at: Some(Instant::now()),
            last_payload_at: Some(Instant::now() + Duration::from_millis(10)),
        },
    );

    assert_eq!(
        udp_candidate_indices(&context, 1024 * 1024, DEFAULT_SOCKS5_UDP_TTL_MS)
            .first()
            .copied(),
        Some(0)
    );
    let observation = context
        .health()
        .lock()
        .expect("client path health lock")
        .udp[0]
        .observation_at(Instant::now());
    let reliable_snapshot = path_snapshot(&context.udp_paths[0], 0, observation);
    assert!(observation.measured_rate_bps.is_some());
    assert!(observation.product_delivery_rate_bps.is_none());
    assert_eq!(observation.product_delivery_sample_bytes, 0);
    assert!(reliable_snapshot.product_progress_rate_bps.is_none());
    assert!(!reliable_snapshot.has_durable_product_progress);
}

#[test]
fn udp_datagram_feedback_updates_scheduler_health() {
    let lagging_path = "udp://127.0.0.1:10021?srtt-ms=250&rate-mbps=1"
        .parse::<PathSpec>()
        .expect("lagging path");
    let observed_path = "udp://127.0.0.1:10022?srtt-ms=250&rate-mbps=1"
        .parse::<PathSpec>()
        .expect("observed path");
    let context = ClientPathContext::new(
        vec![lagging_path, observed_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_udp_path_feedback(
        1,
        UdpDatagramPathObservation {
            rtt: Duration::from_millis(8),
            jitter: Duration::from_millis(1),
            loss_rate: 0.02,
            rate_sample: PathRateSample::new(1024 * 1024, Duration::from_millis(20)),
        },
    );

    assert_eq!(
        udp_candidate_indices(&context, 4096, DEFAULT_SOCKS5_UDP_TTL_MS)
            .first()
            .copied(),
        Some(1)
    );
    let health = context.health().lock().expect("health lock");
    assert_eq!(health.udp[1].state, SchedulerPathState::Active);
    assert!(health.udp[1].measured_srtt_ms.is_some());
    assert!(health.udp[1].measured_jitter_ms.is_some());
    assert!(health.udp[1].measured_rate_bps.is_some());
    assert_eq!(health.udp[1].measured_loss_rate, Some(0.02));
}

#[test]
fn realtime_udp_datagram_feedback_beats_probe_only_paths() {
    let feedback_path = "udp://127.0.0.1:10144"
        .parse::<PathSpec>()
        .expect("feedback path");
    let probe_only_path = "udp://127.0.0.1:10145"
        .parse::<PathSpec>()
        .expect("probe-only path");
    let context = ClientPathContext::new(
        vec![feedback_path, probe_only_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_udp_path_feedback(
        0,
        UdpDatagramPathObservation {
            rtt: Duration::from_millis(40),
            jitter: Duration::from_millis(4),
            loss_rate: 0.0,
            rate_sample: PathRateSample::new(1024 * 1024, Duration::from_millis(10)),
        },
    );
    context.mark_udp_path_probe_success(1, Duration::from_millis(1));

    let association = UdpDatagramClientAssociation::new(context.clone());
    let candidates = context.ordered_udp_path_candidates_for_ttl(512, DEFAULT_SOCKS5_UDP_TTL_MS);
    assert_eq!(
        association.select_path_candidate(
            &candidates,
            &HashSet::new(),
            512,
            DEFAULT_SOCKS5_UDP_TTL_MS,
        ),
        Some(0)
    );
    assert_eq!(
        association.select_path_candidate(
            &candidates,
            &HashSet::from([0]),
            512,
            DEFAULT_SOCKS5_UDP_TTL_MS,
        ),
        Some(1)
    );
}

#[test]
fn endpoint_only_udp_datagram_uses_measured_eta_after_feedback() {
    let first_path = "udp://127.0.0.1:10146"
        .parse::<PathSpec>()
        .expect("first path");
    let second_path = "udp://127.0.0.1:10147"
        .parse::<PathSpec>()
        .expect("second path");
    let context = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_udp_path_feedback(
        0,
        UdpDatagramPathObservation {
            rtt: Duration::from_millis(80),
            jitter: Duration::from_millis(4),
            loss_rate: 0.0,
            rate_sample: PathRateSample::new(1024 * 1024, Duration::from_millis(20)),
        },
    );
    context.mark_udp_path_feedback(
        1,
        UdpDatagramPathObservation {
            rtt: Duration::from_millis(20),
            jitter: Duration::from_millis(2),
            loss_rate: 0.0,
            rate_sample: PathRateSample::new(1024 * 1024, Duration::from_millis(10)),
        },
    );

    let association = UdpDatagramClientAssociation::new(context.clone());
    let candidates = context.ordered_udp_path_candidates_for_ttl(512, DEFAULT_SOCKS5_UDP_TTL_MS);
    assert_eq!(
        association.select_path_candidate(
            &candidates,
            &HashSet::new(),
            512,
            DEFAULT_SOCKS5_UDP_TTL_MS,
        ),
        Some(1),
        "endpoint-only startup order must stop dominating after path-scoped datagram feedback exists"
    );
}

#[test]
fn mixed_datagram_underlay_uses_tcp_when_tcp_eta_is_better() {
    let slow_udp = "udp://127.0.0.1:10148?srtt-ms=90&rate-mbps=20"
        .parse::<PathSpec>()
        .expect("udp path");
    let fast_tcp = "tcp://127.0.0.1:10149?srtt-ms=15&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("tcp path");
    let context = ClientPathContext::new(
        vec![slow_udp, fast_tcp],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        DatagramClientAssociation::select_underlay(&context, 512, DEFAULT_SOCKS5_UDP_TTL_MS),
        Some(UnderlayProtocol::Tcp),
        "datagram underlay selection must be ETA/evidence driven, not UDP-first"
    );
}

#[test]
fn mixed_datagram_underlay_uses_udp_when_udp_eta_is_better() {
    let fast_udp = "udp://127.0.0.1:10150?srtt-ms=15&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("udp path");
    let slow_tcp = "tcp://127.0.0.1:10151?srtt-ms=90&rate-mbps=20"
        .parse::<PathSpec>()
        .expect("tcp path");
    let context = ClientPathContext::new(
        vec![fast_udp, slow_tcp],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        DatagramClientAssociation::select_underlay(&context, 512, DEFAULT_SOCKS5_UDP_TTL_MS),
        Some(UnderlayProtocol::Udp)
    );
}

#[test]
fn udp_freshness_filter_rejects_paths_that_cannot_fit_ttl() {
    let high_latency_path = "udp://127.0.0.1:10023?srtt-ms=1000&rate-mbps=1"
        .parse::<PathSpec>()
        .expect("high latency path");
    let context = ClientPathContext::new(
        vec![high_latency_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert!(udp_candidate_indices(&context, 1024, 10).is_empty());
}

#[test]
fn realtime_udp_prefers_measured_model_before_unmeasured_startup_paths() {
    let first_path = "udp://127.0.0.1:10024"
        .parse::<PathSpec>()
        .expect("first path");
    let second_path = "udp://127.0.0.1:10025"
        .parse::<PathSpec>()
        .expect("second path");
    let context = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        udp_candidate_indices(&context, 512, DEFAULT_SOCKS5_UDP_TTL_MS),
        vec![0]
    );

    context.mark_udp_path_probe_success(0, Duration::from_millis(20));

    assert_eq!(
        udp_candidate_indices(&context, 512, DEFAULT_SOCKS5_UDP_TTL_MS),
        vec![0]
    );
}

#[test]
fn udp_association_suppression_prefers_survivor_without_dead_ending() {
    let blackhole_path = "udp://127.0.0.1:10026?srtt-ms=5&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("blackhole path");
    let survivor_path = "udp://127.0.0.1:10027?srtt-ms=20&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("survivor path");
    let context = ClientPathContext::new(
        vec![blackhole_path, survivor_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("ctx");
    let mut association = UdpDatagramClientAssociation::new(context);
    let candidates = [
        UdpPathCandidate {
            path_index: 0,
            eta_ms: 5.0,
        },
        UdpPathCandidate {
            path_index: 1,
            eta_ms: 20.0,
        },
    ];

    assert_eq!(
        association.select_path_candidate(&candidates, &HashSet::new(), 512, 1000),
        Some(0)
    );

    association.suppress_path_after_timeout(0, Duration::from_millis(250), 1000);
    assert_eq!(
        association.select_path_candidate(&candidates, &HashSet::new(), 512, 1000),
        Some(1)
    );

    association.suppress_path_after_timeout(1, Duration::from_millis(250), 1000);
    assert_eq!(
        association.select_path_candidate(&candidates, &HashSet::new(), 512, 1000),
        Some(0)
    );
}

#[test]
fn udp_association_keeps_successful_path_within_hysteresis_until_suppressed() {
    let steady_path = "udp://127.0.0.1:10031?srtt-ms=20&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("steady path");
    let peer_path = "udp://127.0.0.1:10032?srtt-ms=20&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("peer path");
    let context = ClientPathContext::new(
        vec![steady_path, peer_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("ctx");
    let mut association = UdpDatagramClientAssociation::new(context);
    let candidates = [
        UdpPathCandidate {
            path_index: 1,
            eta_ms: 20.0,
        },
        UdpPathCandidate {
            path_index: 0,
            eta_ms: 20.0,
        },
    ];

    association.last_successful_path = Some(0);
    assert_eq!(
        association.select_path_candidate(&candidates, &HashSet::new(), 512, 1000),
        Some(0)
    );

    association.suppress_path_after_timeout(0, Duration::from_millis(250), 1000);
    assert_eq!(
        association.select_path_candidate(&candidates, &HashSet::new(), 512, 1000),
        Some(1)
    );
}

#[test]
fn udp_association_last_successful_path_is_only_hysteresis_hint() {
    let stale_path = "udp://127.0.0.1:10033?srtt-ms=20&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("stale path");
    let better_path = "udp://127.0.0.1:10034?srtt-ms=20&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("better path");
    let context = ClientPathContext::new(
        vec![stale_path, better_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("ctx");
    let mut association = UdpDatagramClientAssociation::new(context.clone());

    context.mark_udp_path_feedback(
        0,
        UdpDatagramPathObservation {
            rtt: Duration::from_millis(180),
            jitter: Duration::from_millis(5),
            loss_rate: 0.25,
            rate_sample: PathRateSample::new(512, Duration::from_millis(180)),
        },
    );
    context.mark_udp_path_feedback(
        1,
        UdpDatagramPathObservation {
            rtt: Duration::from_millis(25),
            jitter: Duration::from_millis(2),
            loss_rate: 0.0,
            rate_sample: PathRateSample::new(512, Duration::from_millis(25)),
        },
    );

    association.last_successful_path = Some(0);
    let candidates = context.ordered_udp_path_candidates_for_ttl(512, DEFAULT_SOCKS5_UDP_TTL_MS);

    assert_eq!(
        association.select_path_candidate(&candidates, &HashSet::new(), 512, 1000),
        Some(1),
        "last successful realtime datagram path is a hysteresis hint, not a sticky override over a substantially better live candidate"
    );
}

#[test]
fn datagram_response_timeout_is_terminal_product_expiry() {
    assert!(!datagram_underlay_error_is_retryable(
        &RuntimeError::DatagramResponseTimedOut
    ));
    assert!(!tcp_datagram_error_is_path_retryable(
        &RuntimeError::DatagramResponseTimedOut
    ));
    assert!(!udp_datagram_error_is_path_retryable(
        &RuntimeError::DatagramResponseTimedOut
    ));

    assert!(runtime_error_is_datagram_response_timeout(
        &RuntimeError::DatagramResponseTimedOut
    ));
    assert!(!runtime_error_is_datagram_response_timeout(
        &RuntimeError::Protocol("other datagram protocol error")
    ));
    assert!(datagram_underlay_error_is_retryable(
        &RuntimeError::PathOpenTimedOut
    ));
    assert!(tcp_datagram_error_is_path_retryable(
        &RuntimeError::PathOpenTimedOut
    ));
}

#[test]
fn unacked_datagram_timeout_retries_unattempted_alternative_before_product_feedback() {
    assert_eq!(
        datagram_timeout_action(false, true),
        DatagramTimeoutAction::RetryAlternative
    );
    assert_eq!(
        datagram_timeout_action(true, true),
        DatagramTimeoutAction::TerminalProductExpiry
    );
    assert_eq!(
        datagram_timeout_action(false, false),
        DatagramTimeoutAction::TerminalProductExpiry
    );
}

#[test]
fn tcp_datagram_open_budget_is_ttl_bounded_and_reserves_an_alternative() {
    let ttl = Duration::from_millis(DEFAULT_SOCKS5_UDP_TTL_MS.into());
    let initial_pto = path_open_pto(None, false);
    assert_eq!(
        tcp_datagram_path_open_timeout(None, false, ttl),
        initial_pto
            .saturating_mul(active_path_open_pto_multiplier(None))
            .min(ttl)
    );
    assert_eq!(
        tcp_datagram_path_open_timeout(None, true, ttl),
        initial_pto
            .saturating_mul(active_path_open_serialized_exchanges(None))
            .min(ttl / 2)
    );

    let tight_ttl = Duration::from_millis(250);
    assert_eq!(
        tcp_datagram_path_open_timeout(None, false, tight_ttl),
        tight_ttl
    );
    assert_eq!(
        tcp_datagram_path_open_timeout(None, true, tight_ttl),
        tight_ttl / 2
    );
}

#[test]
fn fresh_tcp_datagram_carrier_keeps_initial_pto_floor_after_live_probe() {
    let path = "tcp://127.0.0.1:10130?srtt-ms=20&jitter-ms=1&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("TCP path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    context.mark_tcp_path_probe_success(0, Duration::from_millis(20));
    assert!(context.reliable_path_rtt_is_observed(key));
    assert!(path_open_pto(context.tcp_path_snapshot(0), true) < default_transport_pto());

    let ttl = Duration::from_secs(30);
    assert_eq!(
        tcp_datagram_path_open_timeout(context.tcp_path_snapshot(0), true, ttl),
        path_open_pto(context.tcp_path_snapshot(0), false).saturating_mul(
            active_path_open_serialized_exchanges(context.tcp_path_snapshot(0))
        ),
    );
    assert_eq!(
        tcp_datagram_path_open_timeout(context.tcp_path_snapshot(0), false, ttl),
        path_open_pto(context.tcp_path_snapshot(0), false).saturating_mul(
            active_path_open_pto_multiplier(context.tcp_path_snapshot(0))
        ),
    );
}

#[test]
fn udp_path_open_timeout_uses_adaptive_multipath_startup_budget() {
    let mut model = UdpPathRuntimeModel {
        pacing_rate_bps: 1.0,
        response_timeout: Duration::from_millis(300),
        max_payload_bytes: MAX_PRODUCT_DATAGRAM_PAYLOAD_BYTES,
    };

    assert_eq!(
        udp_datagram_path_open_timeout(false, false, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        UDP_PATH_HANDSHAKE_TIMEOUT
    );
    assert_eq!(
        udp_datagram_path_open_timeout(true, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        Duration::from_millis(300)
    );
    assert_eq!(
        udp_datagram_path_open_timeout(false, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        Duration::from_millis(300)
    );

    model.response_timeout = Duration::from_millis(1);
    assert_eq!(
        udp_datagram_path_open_timeout(true, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        QUIC_TIMER_GRANULARITY
    );
    assert_eq!(
        udp_datagram_path_open_timeout(false, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        QUIC_TIMER_GRANULARITY
    );

    model.response_timeout = Duration::from_millis(65);
    assert_eq!(
        udp_datagram_path_open_timeout(false, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        Duration::from_millis(65)
    );

    model.response_timeout = UDP_PATH_HANDSHAKE_TIMEOUT + Duration::from_secs(1);
    assert_eq!(
        udp_datagram_path_open_timeout(true, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        UDP_PATH_HANDSHAKE_TIMEOUT
    );
    assert_eq!(
        udp_datagram_path_open_timeout(false, false, model, 250),
        Duration::from_millis(250)
    );
}

#[test]
fn udp_first_datagram_response_uses_startup_budget_for_cold_association() {
    let mut model = UdpPathRuntimeModel {
        pacing_rate_bps: 1.0,
        response_timeout: Duration::from_millis(65),
        max_payload_bytes: MAX_PRODUCT_DATAGRAM_PAYLOAD_BYTES,
    };

    assert_eq!(
        udp_datagram_first_response_timeout(false, false, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        Duration::from_millis(65)
    );
    assert_eq!(
        udp_datagram_first_response_timeout(true, false, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        Duration::from_millis(65)
    );
    assert_eq!(
        udp_datagram_first_response_timeout(false, true, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        Duration::from_millis(65)
    );

    model.response_timeout = Duration::from_millis(300);
    assert_eq!(
        udp_datagram_first_response_timeout(false, false, false, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        UDP_PATH_HANDSHAKE_TIMEOUT
    );
}

#[test]
fn udp_runtime_model_backs_off_response_timeout_after_loss() {
    let stable = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 80.0, 30_000_000.0);
    let mut lossy = stable;
    lossy.loss_rate = 0.5;

    let stable_model = UdpPathRuntimeModel::from_snapshot(
        stable,
        DEFAULT_SOCKS5_UDP_TTL_MS,
        MAX_PRODUCT_DATAGRAM_PAYLOAD_BYTES,
    );
    let lossy_model = UdpPathRuntimeModel::from_snapshot(
        lossy,
        DEFAULT_SOCKS5_UDP_TTL_MS,
        MAX_PRODUCT_DATAGRAM_PAYLOAD_BYTES,
    );

    assert!(lossy_model.response_timeout > stable_model.response_timeout);
    assert!(
        lossy_model.response_timeout <= Duration::from_millis(DEFAULT_SOCKS5_UDP_TTL_MS.into())
    );
    assert!(lossy_model.pacing_rate_bps < stable_model.pacing_rate_bps);
}

#[test]
fn udp_response_budget_tracks_live_loss_model() {
    let path = "udp://127.0.0.1:10036?srtt-ms=80&rate-mbps=30"
        .parse::<PathSpec>()
        .expect("path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
    let stable_model = context
        .udp_path_runtime_model(0, DEFAULT_SOCKS5_UDP_TTL_MS)
        .expect("stable model");
    let stable_budget =
        datagram_response_deadline_budget(stable_model.response_timeout, DEFAULT_SOCKS5_UDP_TTL_MS);
    assert!(stable_budget >= QUIC_TIMER_GRANULARITY);

    context.mark_udp_path_feedback(
        0,
        UdpDatagramPathObservation {
            rtt: Duration::from_millis(120),
            jitter: Duration::from_millis(0),
            loss_rate: 1.0,
            rate_sample: None,
        },
    );

    let lossy_model = context
        .udp_path_runtime_model(0, DEFAULT_SOCKS5_UDP_TTL_MS)
        .expect("lossy model");
    let lossy_budget =
        datagram_response_deadline_budget(lossy_model.response_timeout, DEFAULT_SOCKS5_UDP_TTL_MS);
    assert!(lossy_budget > stable_budget);
    assert!(lossy_budget > Duration::from_millis(500));
}

#[test]
fn datagram_response_deadline_budget_scales_from_ttl_slack_and_response_model() {
    let high_rtt_timeout = Duration::from_millis(900);
    let budget = datagram_response_deadline_budget(high_rtt_timeout, DEFAULT_SOCKS5_UDP_TTL_MS);
    let low_rtt_budget =
        datagram_response_deadline_budget(Duration::from_millis(50), DEFAULT_SOCKS5_UDP_TTL_MS);
    assert!(budget > low_rtt_budget);
    assert!(budget < Duration::from_millis(DEFAULT_SOCKS5_UDP_TTL_MS.into()));

    let tight_ttl_budget = datagram_response_deadline_budget(high_rtt_timeout, 1_000);
    assert_eq!(tight_ttl_budget, Duration::from_millis(1_000));
}

#[test]
fn tcp_datagram_response_timeout_uses_tcp_rto_budget() {
    let mut high_rtt_tcp =
        PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 250.0, 200_000_000.0);
    high_rtt_tcp.jitter_ms = 20.0;

    let startup_timeout =
        tcp_datagram_response_timeout(high_rtt_tcp, None, None, DEFAULT_SOCKS5_UDP_TTL_MS);
    assert!(startup_timeout >= transport_pto_from_snapshot(Some(high_rtt_tcp)));
    assert!(startup_timeout <= Duration::from_millis(DEFAULT_SOCKS5_UDP_TTL_MS.into()));

    let tight_ttl_timeout = tcp_datagram_response_timeout(high_rtt_tcp, None, None, 500);
    assert!(tight_ttl_timeout <= Duration::from_millis(500));

    let observed_timeout = tcp_datagram_response_timeout(
        high_rtt_tcp,
        Some(Duration::from_millis(300)),
        Some(Duration::from_millis(80)),
        DEFAULT_SOCKS5_UDP_TTL_MS,
    );
    assert_eq!(observed_timeout, Duration::from_millis(645));
}

#[test]
fn udp_edge_lane_limit_scales_with_realtime_response_model() {
    let low_latency = "udp://127.0.0.1:10184?srtt-ms=20&jitter-ms=0&rate-mbps=30"
        .parse::<PathSpec>()
        .expect("low-latency path");
    let high_rtt = "udp://127.0.0.1:10185?srtt-ms=180&jitter-ms=20&rate-mbps=300"
        .parse::<PathSpec>()
        .expect("high-rtt path");
    let low_context =
        ClientPathContext::new(vec![low_latency], security(), ResourceLimits::default())
            .expect("low context");
    let high_context = ClientPathContext::new(
        vec![high_rtt.clone()],
        security(),
        ResourceLimits::default(),
    )
    .expect("high context");

    let low_limit = udp_edge_lane_limit(&low_context);
    let high_limit = udp_edge_lane_limit(&high_context);
    assert!(low_limit >= QUIC_PERSISTENT_CONGESTION_THRESHOLD as usize);
    assert!(high_limit >= low_limit);
    assert!(high_limit <= udp_edge_queue_slots(&high_context));

    let capped_resources = ResourceLimits {
        max_datagram_queue_bytes: ResourceLimits::default().max_payload_bytes * 3,
        ..ResourceLimits::default()
    };
    let capped_context = ClientPathContext::new(vec![high_rtt], security(), capped_resources)
        .expect("capped context");
    assert_eq!(udp_edge_lane_limit(&capped_context), 3);
}

#[test]
fn udp_edge_lane_startup_ramps_after_success_feedback() {
    let paths = vec![
        "udp://127.0.0.1:10180".parse().expect("first path"),
        "udp://127.0.0.1:10181".parse().expect("second path"),
        "udp://127.0.0.1:10182".parse().expect("third path"),
    ];
    let context =
        ClientPathContext::new(paths, security(), ResourceLimits::default()).expect("context");

    assert!(udp_edge_lane_limit(&context) > udp_edge_startup_lane_limit(&context));
    assert_eq!(udp_edge_startup_lane_limit(&context), 2);
    assert!(udp_edge_lane_spawn_allowed(0, 0, &context));
    assert!(udp_edge_lane_spawn_allowed(1, 0, &context));
    assert!(!udp_edge_lane_spawn_allowed(2, 0, &context));
    assert!(udp_edge_lane_spawn_allowed(2, 1, &context));
}

#[test]
fn udp_edge_lane_startup_uses_tcp_datagram_carriers_too() {
    let paths = vec![
        "tcp://127.0.0.1:10186?srtt-ms=20&jitter-ms=0&rate-mbps=30"
            .parse()
            .expect("first tcp path"),
        "tcp://127.0.0.1:10187?srtt-ms=25&jitter-ms=0&rate-mbps=30"
            .parse()
            .expect("second tcp path"),
    ];
    let context =
        ClientPathContext::new(paths, security(), ResourceLimits::default()).expect("context");

    assert!(udp_edge_lane_limit(&context) > 1);
    assert_eq!(udp_edge_startup_lane_limit(&context), 2);
    assert!(udp_edge_lane_spawn_allowed(0, 0, &context));
    assert!(udp_edge_lane_spawn_allowed(1, 0, &context));
    assert!(!udp_edge_lane_spawn_allowed(2, 0, &context));
}

#[test]
fn udp_edge_route_hint_avoids_pending_lane_carrier_when_alternative_exists() {
    let paths = vec![
        "udp://127.0.0.1:10188?srtt-ms=20&jitter-ms=0&rate-mbps=30"
            .parse()
            .expect("first udp path"),
        "tcp://127.0.0.1:10189?srtt-ms=25&jitter-ms=0&rate-mbps=30"
            .parse()
            .expect("tcp path"),
        "udp://127.0.0.1:10190?srtt-ms=30&jitter-ms=0&rate-mbps=30"
            .parse()
            .expect("second udp path"),
    ];
    let context =
        ClientPathContext::new(paths, security(), ResourceLimits::default()).expect("context");
    let first = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let second = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };

    assert_eq!(
        udp_edge_route_hint(
            &context,
            512,
            DEFAULT_SOCKS5_UDP_TTL_MS,
            std::iter::once(first),
        ),
        Some(second)
    );
}

#[test]
fn udp_edge_lane_startup_respects_queue_capacity() {
    let path = "udp://127.0.0.1:10183".parse().expect("path");
    let resources = ResourceLimits {
        max_datagram_queue_bytes: ResourceLimits::default().max_payload_bytes,
        ..ResourceLimits::default()
    };
    let context = ClientPathContext::new(vec![path], security(), resources).expect("context");

    assert_eq!(udp_edge_queue_slots(&context), 1);
    assert_eq!(udp_edge_startup_lane_limit(&context), 1);
    assert!(udp_edge_lane_spawn_allowed(0, 0, &context));
    assert!(!udp_edge_lane_spawn_allowed(1, 0, &context));
    assert!(udp_edge_lane_spawn_allowed(1, 1, &context));
}

#[test]
fn active_tcp_load_spreads_new_streams_and_releases_on_close() {
    let first_path = "tcp://127.0.0.1:10021?srtt-ms=10&rate-mbps=10"
        .parse::<PathSpec>()
        .expect("first path");
    let second_path = "tcp://127.0.0.1:10022?srtt-ms=10&rate-mbps=10"
        .parse::<PathSpec>()
        .expect("second path");
    let context = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_tcp_path_open_success(0, Duration::from_millis(1), FlowLane::Latency);
    assert_eq!(
        context
            .ordered_tcp_path_indices(FlowLane::Latency, 512)
            .first()
            .copied(),
        Some(1)
    );

    context.release_tcp_path_load(0, FlowLane::Latency);
    assert_eq!(
        context
            .ordered_tcp_path_indices(FlowLane::Latency, 512)
            .first()
            .copied(),
        Some(0)
    );
    let health = context.health().lock().expect("health lock");
    assert_eq!(health.tcp[0].active_flows, 0);
    assert_eq!(health.tcp[0].relay_bytes_in_flight, 0);
}

#[test]
fn active_interactive_tcp_flow_pushes_bulk_to_other_path() {
    let low_latency_path = "tcp://127.0.0.1:10123?srtt-ms=20&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("low latency path");
    let bulk_candidate_path = "tcp://127.0.0.1:10124?srtt-ms=180&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("bulk candidate path");
    let context = ClientPathContext::new(
        vec![low_latency_path, bulk_candidate_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), FlowLane::Latency);
    context.mark_tcp_path_delivery(
        1,
        PathDeliveryStats {
            payload_bytes: 4 * 1024 * 1024,
            first_payload_at: Some(Instant::now()),
            last_payload_at: Some(Instant::now() + Duration::from_millis(40)),
        },
    );

    assert_eq!(
        context
            .ordered_tcp_path_indices(FlowLane::Throughput, 4 * 1024 * 1024)
            .first()
            .copied(),
        Some(1)
    );
    assert_eq!(
        context
            .ordered_tcp_path_indices(FlowLane::Latency, PATH_OPEN_SCORE_BYTES)
            .first()
            .copied(),
        Some(0)
    );
}

#[test]
fn endpoint_only_tcp_startup_preserves_configured_order_on_equal_scores() {
    let first_path = "tcp://127.0.0.1:10121"
        .parse::<PathSpec>()
        .expect("first path");
    let second_path = "tcp://127.0.0.1:10122"
        .parse::<PathSpec>()
        .expect("second path");
    let context = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        context
            .ordered_tcp_path_indices(FlowLane::Latency, PATH_OPEN_SCORE_BYTES)
            .first()
            .copied(),
        Some(0)
    );
}

#[test]
fn endpoint_only_tcp_realtime_datagrams_preserve_configured_order() {
    let first_path = "tcp://127.0.0.1:10135"
        .parse::<PathSpec>()
        .expect("first path");
    let second_path = "tcp://127.0.0.1:10136"
        .parse::<PathSpec>()
        .expect("second path");
    let context = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_tcp_path_failure(0);
    context.mark_tcp_path_probe_success(1, Duration::from_millis(1));

    assert_eq!(
        context.ordered_tcp_path_indices(FlowLane::RealtimeDatagram, PATH_OPEN_SCORE_BYTES),
        vec![0, 1]
    );

    context.mark_tcp_path_failure(0);
    assert_eq!(
        context.ordered_tcp_path_indices(FlowLane::RealtimeDatagram, PATH_OPEN_SCORE_BYTES),
        vec![1]
    );
}

#[test]
fn endpoint_only_tcp_startup_validates_order_before_noisy_probe_scores() {
    let first_path = "tcp://127.0.0.1:10125"
        .parse::<PathSpec>()
        .expect("first path");
    let second_path = "tcp://127.0.0.1:10126"
        .parse::<PathSpec>()
        .expect("second path");
    let context = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_tcp_path_failure(0);
    context.mark_tcp_path_probe_success(1, Duration::from_millis(1));

    assert_eq!(
        context.ordered_tcp_path_indices(FlowLane::Latency, PATH_OPEN_SCORE_BYTES),
        vec![0, 1]
    );

    context.mark_tcp_path_failure(0);
    assert_eq!(
        context.ordered_tcp_path_indices(FlowLane::Latency, PATH_OPEN_SCORE_BYTES),
        vec![1]
    );
}

#[test]
fn endpoint_only_tcp_interactive_opens_spread_active_load_without_probe_noise() {
    let low_latency_path = "tcp://127.0.0.1:10129"
        .parse::<PathSpec>()
        .expect("low latency path");
    let high_latency_path = "tcp://127.0.0.1:10130"
        .parse::<PathSpec>()
        .expect("high latency path");
    let poor_path = "tcp://127.0.0.1:10131"
        .parse::<PathSpec>()
        .expect("poor path");
    let context = ClientPathContext::new(
        vec![low_latency_path, high_latency_path, poor_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), FlowLane::Latency);
    context.mark_tcp_path_probe_success(2, Duration::from_millis(1));

    assert_eq!(
        context.ordered_tcp_path_indices(FlowLane::Latency, PATH_OPEN_SCORE_BYTES),
        vec![1, 2, 0]
    );
}

#[test]
fn endpoint_only_tcp_open_reservations_spread_concurrent_streams_without_probe_noise() {
    let first_path = "tcp://127.0.0.1:10162"
        .parse::<PathSpec>()
        .expect("first path");
    let second_path = "tcp://127.0.0.1:10163"
        .parse::<PathSpec>()
        .expect("second path");
    let probe_noisy_path = "tcp://127.0.0.1:10164"
        .parse::<PathSpec>()
        .expect("probe noisy path");
    let context = ClientPathContext::new(
        vec![first_path, second_path, probe_noisy_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_tcp_path_probe_success(2, Duration::from_millis(1));

    let first = context
        .reserve_reliable_stream_path(FlowLane::Latency, PATH_OPEN_SCORE_BYTES, &[])
        .expect("first reservation");
    let second = context
        .reserve_reliable_stream_path(FlowLane::Latency, PATH_OPEN_SCORE_BYTES, &[])
        .expect("second reservation");

    assert_eq!(first.key().underlay, UnderlayProtocol::Tcp);
    assert_eq!(first.key().index, 0);
    assert_eq!(second.key().underlay, UnderlayProtocol::Tcp);
    assert_eq!(second.key().index, 1);

    context.mark_tcp_path_reserved_open_success(first.key().index, Duration::from_millis(20));
    context.mark_tcp_path_reserved_open_success(second.key().index, Duration::from_millis(80));
    drop(first);
    drop(second);

    let health = context.health().lock().expect("health lock");
    assert_eq!(health.tcp[0].active_flows, 0);
    assert_eq!(health.tcp[1].active_flows, 0);
    assert_eq!(health.tcp[0].relay_bytes_in_flight, 0);
    assert_eq!(health.tcp[1].relay_bytes_in_flight, 0);
}

#[test]
fn endpoint_only_tcp_bulk_load_spreads_replacement_without_realtime_work() {
    let first_path = "tcp://127.0.0.1:10166"
        .parse::<PathSpec>()
        .expect("first path");
    let second_path = "tcp://127.0.0.1:10167"
        .parse::<PathSpec>()
        .expect("second path");
    let context = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), FlowLane::Latency);
    context.change_relay_path_lane_load(
        UnderlayProtocol::Tcp,
        0,
        FlowLane::Latency,
        FlowLane::Throughput,
    );

    let reserved = context
        .reserve_reliable_stream_path(FlowLane::Latency, PATH_OPEN_SCORE_BYTES, &[])
        .expect("interactive reservation");
    assert_eq!(reserved.key().underlay, UnderlayProtocol::Tcp);
    assert_eq!(reserved.key().index, 1);
}

#[test]
fn endpoint_only_tcp_bulk_load_keeps_new_interactive_streams_latency_first_with_realtime_work() {
    let first_path = "tcp://127.0.0.1:10168"
        .parse::<PathSpec>()
        .expect("first path");
    let second_path = "tcp://127.0.0.1:10169"
        .parse::<PathSpec>()
        .expect("second path");
    let udp_path = "udp://127.0.0.1:10170"
        .parse::<PathSpec>()
        .expect("udp path");
    let context = ClientPathContext::new(
        vec![first_path, second_path, udp_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), FlowLane::Latency);
    context.change_relay_path_lane_load(
        UnderlayProtocol::Tcp,
        0,
        FlowLane::Latency,
        FlowLane::Throughput,
    );
    context.mark_udp_path_open_success(0, Duration::from_millis(30));

    let reserved = context
        .reserve_reliable_stream_path(FlowLane::Latency, PATH_OPEN_SCORE_BYTES, &[])
        .expect("interactive reservation");
    assert_eq!(reserved.key().underlay, UnderlayProtocol::Tcp);
    assert_eq!(reserved.key().index, 1);
}

#[test]
fn endpoint_only_tcp_bulk_and_interactive_load_keep_new_interactive_streams_latency_first() {
    let first_path = "tcp://127.0.0.1:10171"
        .parse::<PathSpec>()
        .expect("first path");
    let second_path = "tcp://127.0.0.1:10172"
        .parse::<PathSpec>()
        .expect("second path");
    let context = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), FlowLane::Latency);
    context.change_relay_path_lane_load(
        UnderlayProtocol::Tcp,
        0,
        FlowLane::Latency,
        FlowLane::Throughput,
    );
    context.mark_tcp_path_open_success(0, Duration::from_millis(20), FlowLane::Latency);

    let reserved = context
        .reserve_reliable_stream_path(FlowLane::Latency, PATH_OPEN_SCORE_BYTES, &[])
        .expect("interactive reservation");
    assert_eq!(reserved.key().underlay, UnderlayProtocol::Tcp);
    assert_eq!(reserved.key().index, 1);
}

#[test]
fn hinted_tcp_startup_uses_configured_metrics_before_order() {
    let high_latency_path = "tcp://127.0.0.1:10127?srtt-ms=200&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("high latency path");
    let low_latency_path = "tcp://127.0.0.1:10128?srtt-ms=10&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("low latency path");
    let context = ClientPathContext::new(
        vec![high_latency_path, low_latency_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        context
            .ordered_tcp_path_indices(FlowLane::Latency, PATH_OPEN_SCORE_BYTES)
            .first()
            .copied(),
        Some(1)
    );
}

#[test]
fn quic_path_metrics_feed_path_model_without_fake_bulk_evidence() {
    let path = "udp://127.0.0.1:10129"
        .parse::<PathSpec>()
        .expect("udp path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    let now = Instant::now();

    {
        let mut health = context.health().lock().expect("health lock");
        health.udp[0].mark_quic_path_metrics(UdpPathMetrics {
            direction: PathMetricDirection::ClientToServer,
            srtt: Duration::from_millis(42),
            rttvar: Duration::from_millis(7),
            rtt_observed: true,
            delivery_rate_bps: 300_000_000.0,
            pacing_rate_bps: 300_000_000.0,
            inflight_hi: 512 * 1024,
            bytes_in_flight: 48 * 1024,
            pending_bytes: 64 * 1024,
            loss_ppm: None,
            ecn_ppm: None,
            app_limited: true,
            ack_derived_data_seen: false,
            delivery_sample_count: 0,
            delivery_sample_bytes: 0,
            last_delivery_sample_at: None,
            bulk_proof_expires_at: None,
            latest_delivery_sample_bytes: 0,
            latest_delivery_sample_count: 0,
            latest_carrier_ack_elapsed: None,
            latest_rate_sample_elapsed: None,
            capacity_proof_candidate: None,
            capacity_probe: None,
            #[cfg(feature = "lab-diagnostics")]
            ack_poll: QuicAckPollDiagnostics::default(),
        });
    }

    let snapshot = context.udp_path_snapshot(0).expect("snapshot");
    assert_eq!(snapshot.srtt_ms, 42.0);
    assert_eq!(snapshot.jitter_ms, 7.0);
    assert_eq!(snapshot.bytes_in_flight, 48 * 1024);
    assert_eq!(snapshot.queue_bytes, 16 * 1024);
    assert_eq!(snapshot.inflight_limit_bytes, 512 * 1024);
    assert!(
        !context.relay_path_has_bulk_model_evidence(UnderlayProtocol::Udp, 0),
        "carrier RTT/liveness alone must not promote a UDP path for ordinary bulk"
    );

    {
        let mut health = context.health().lock().expect("health lock");
        health.udp[0].mark_quic_path_metrics(UdpPathMetrics {
            direction: PathMetricDirection::ClientToServer,
            srtt: Duration::from_millis(42),
            rttvar: Duration::from_millis(7),
            rtt_observed: true,
            delivery_rate_bps: 300_000_000.0,
            pacing_rate_bps: 300_000_000.0,
            inflight_hi: 512 * 1024,
            bytes_in_flight: 48 * 1024,
            pending_bytes: 64 * 1024,
            loss_ppm: Some(0),
            ecn_ppm: None,
            app_limited: false,
            ack_derived_data_seen: true,
            delivery_sample_count: 2,
            delivery_sample_bytes: 512 * 1024,
            last_delivery_sample_at: Some(now),
            bulk_proof_expires_at: None,
            latest_delivery_sample_bytes: 512 * 1024,
            latest_delivery_sample_count: 2,
            latest_carrier_ack_elapsed: Some(Duration::from_millis(20)),
            latest_rate_sample_elapsed: Some(Duration::from_millis(20)),
            capacity_proof_candidate: None,
            capacity_probe: None,
            #[cfg(feature = "lab-diagnostics")]
            ack_poll: QuicAckPollDiagnostics::default(),
        });
    }

    let snapshot = context.udp_path_snapshot(0).expect("snapshot");
    assert_eq!(snapshot.delivery_rate_bps, 300_000_000.0);
    assert!(
        context.relay_path_has_bulk_model_evidence(UnderlayProtocol::Udp, 0),
        "ACK-derived carrier delivery samples should become bulk model evidence"
    );
}

#[test]
fn active_udp_load_spreads_new_associations_and_releases_on_close() {
    let first_path = "udp://127.0.0.1:10031?srtt-ms=10&rate-mbps=10"
        .parse::<PathSpec>()
        .expect("first path");
    let second_path = "udp://127.0.0.1:10032?srtt-ms=10&rate-mbps=10"
        .parse::<PathSpec>()
        .expect("second path");
    let context = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_udp_path_probe_success(1, Duration::from_millis(1));
    context.mark_udp_path_open_success(0, Duration::from_millis(1));
    assert_eq!(
        udp_candidate_indices(&context, 512, DEFAULT_SOCKS5_UDP_TTL_MS)
            .first()
            .copied(),
        Some(1)
    );

    context.release_udp_path_load(0);
    assert!(udp_candidate_indices(&context, 512, DEFAULT_SOCKS5_UDP_TTL_MS).contains(&0));
    let health = context.health().lock().expect("health lock");
    assert_eq!(health.udp[0].active_flows, 0);
    assert_eq!(health.udp[0].relay_bytes_in_flight, 0);
}
