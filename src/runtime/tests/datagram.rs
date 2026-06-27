use super::*;

#[test]
fn mixed_auto_bulk_discovery_does_not_attach_unmeasured_endpoint_only_udp() {
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
            .ordered_reliable_auto_bulk_discovery_path_keys(
                Some(0),
                None,
                MuxLimits::default().max_tcp_path_inflight_bytes,
            )
            .is_empty()
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

    context.mark_udp_path_delivery(
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
}

#[test]
fn udp_datagram_feedback_updates_scheduler_health() {
    let stale_path = "udp://127.0.0.1:10021?srtt-ms=250&rate-mbps=1"
        .parse::<PathSpec>()
        .expect("stale path");
    let observed_path = "udp://127.0.0.1:10022?srtt-ms=250&rate-mbps=1"
        .parse::<PathSpec>()
        .expect("observed path");
    let context = ClientPathContext::new(
        vec![stale_path, observed_path],
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
    let health = context.health.lock().expect("health lock");
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
    context.mark_udp_path_feedback(
        1,
        UdpDatagramPathObservation {
            rtt: Duration::from_millis(20),
            jitter: Duration::from_millis(2),
            loss_rate: 0.0,
            rate_sample: PathRateSample::new(1024 * 1024, Duration::from_millis(10)),
        },
    );

    let association = UdpDatagramClientAssociation::new(context.clone()).expect("assoc");
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
    let mut association = UdpDatagramClientAssociation::new(context).expect("assoc");
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
fn udp_association_sticks_to_successful_path_until_suppressed() {
    let steady_path = "udp://127.0.0.1:10031?srtt-ms=20&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("steady path");
    let lower_eta_path = "udp://127.0.0.1:10032?srtt-ms=5&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("lower eta path");
    let context = ClientPathContext::new(
        vec![steady_path, lower_eta_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("ctx");
    let mut association = UdpDatagramClientAssociation::new(context).expect("assoc");
    let candidates = [
        UdpPathCandidate {
            path_index: 1,
            eta_ms: 5.0,
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
fn udp_acked_timeout_migration_requires_validated_alternative() {
    let proven_path = "udp://127.0.0.1:10033"
        .parse::<PathSpec>()
        .expect("proven path");
    let endpoint_only_alternative = "udp://127.0.0.1:10034"
        .parse::<PathSpec>()
        .expect("endpoint-only alternative");
    let hinted_alternative = "udp://127.0.0.1:10035?srtt-ms=80&rate-mbps=30"
        .parse::<PathSpec>()
        .expect("hinted alternative");
    let context = ClientPathContext::new(
        vec![proven_path, endpoint_only_alternative, hinted_alternative],
        security(),
        ResourceLimits::default(),
    )
    .expect("ctx");
    context.mark_udp_path_feedback(
        0,
        UdpDatagramPathObservation {
            rtt: Duration::from_millis(40),
            jitter: Duration::from_millis(4),
            loss_rate: 0.0,
            rate_sample: None,
        },
    );
    let association = UdpDatagramClientAssociation::new(context).expect("assoc");
    let attempted = HashSet::from([0]);

    assert!(!association.has_validated_udp_retry_alternative(
        &[
            UdpPathCandidate {
                path_index: 0,
                eta_ms: 40.0,
            },
            UdpPathCandidate {
                path_index: 1,
                eta_ms: 80.0,
            },
        ],
        &attempted,
        0,
    ));
    assert!(association.has_validated_udp_retry_alternative(
        &[
            UdpPathCandidate {
                path_index: 0,
                eta_ms: 40.0,
            },
            UdpPathCandidate {
                path_index: 2,
                eta_ms: 80.0,
            },
        ],
        &attempted,
        0,
    ));
}

#[test]
fn udp_path_open_timeout_uses_adaptive_multipath_startup_budget() {
    let mut model = UdpPathRuntimeModel {
        pacing_rate_bps: UDP_MIN_PACING_RATE_BPS,
        response_timeout: Duration::from_millis(300),
        mtu_payload_bytes: UDP_DEFAULT_MTU_PAYLOAD_BYTES,
        mtu_is_measured: false,
        mtu_probe_ceiling_payload_bytes: UDP_MAX_MTU_PAYLOAD_BYTES,
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
        UDP_PATH_HANDSHAKE_TIMEOUT
    );

    model.response_timeout = Duration::from_millis(1);
    assert_eq!(
        udp_datagram_path_open_timeout(true, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        UDP_MIN_RESPONSE_TIMEOUT
    );
    assert_eq!(
        udp_datagram_path_open_timeout(false, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        UDP_MIN_RESPONSE_TIMEOUT
    );

    model.response_timeout = Duration::from_millis(65);
    assert_eq!(
        udp_datagram_path_open_timeout(false, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        Duration::from_millis(520)
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
fn udp_runtime_model_backs_off_response_timeout_after_loss() {
    let stable = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 80.0, 30_000_000.0);
    let mut lossy = stable;
    lossy.loss_rate = 0.5;

    let stable_model = UdpPathRuntimeModel::from_snapshot(
        stable,
        DEFAULT_SOCKS5_UDP_TTL_MS,
        UDP_DEFAULT_MTU_PAYLOAD_BYTES,
        true,
        UDP_MAX_MTU_PAYLOAD_BYTES,
    );
    let lossy_model = UdpPathRuntimeModel::from_snapshot(
        lossy,
        DEFAULT_SOCKS5_UDP_TTL_MS,
        UDP_DEFAULT_MTU_PAYLOAD_BYTES,
        true,
        UDP_MAX_MTU_PAYLOAD_BYTES,
    );

    assert!(lossy_model.response_timeout > stable_model.response_timeout);
    assert!(lossy_model.response_timeout <= UDP_MAX_RESPONSE_TIMEOUT);
    assert!(lossy_model.pacing_rate_bps < stable_model.pacing_rate_bps);
}

#[test]
fn udp_association_retry_budget_tracks_live_loss_model() {
    let path = "udp://127.0.0.1:10036?srtt-ms=80&rate-mbps=30"
        .parse::<PathSpec>()
        .expect("path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
    let association = UdpDatagramClientAssociation::new(context.clone()).expect("assoc");
    let stable_budget = association.adaptive_retry_budget(512, DEFAULT_SOCKS5_UDP_TTL_MS);
    assert!(stable_budget >= UDP_MIN_RETRY_BUDGET);
    assert!(stable_budget <= UDP_MAX_RETRY_BUDGET);

    context.mark_udp_path_feedback(
        0,
        UdpDatagramPathObservation {
            rtt: Duration::from_millis(120),
            jitter: Duration::from_millis(0),
            loss_rate: 1.0,
            rate_sample: None,
        },
    );

    let lossy_budget = association.adaptive_retry_budget(512, DEFAULT_SOCKS5_UDP_TTL_MS);
    assert!(lossy_budget > stable_budget);
    assert!(lossy_budget <= UDP_MAX_RETRY_BUDGET);
}

#[test]
fn tcp_datagram_response_timeout_uses_tcp_rto_budget() {
    let mut high_rtt_tcp =
        PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 250.0, 200_000_000.0);
    high_rtt_tcp.jitter_ms = 20.0;

    let startup_timeout =
        tcp_datagram_response_timeout(high_rtt_tcp, None, None, DEFAULT_SOCKS5_UDP_TTL_MS);
    assert!(startup_timeout > UDP_MAX_RESPONSE_TIMEOUT);
    assert!(startup_timeout <= Duration::from_millis(DEFAULT_SOCKS5_UDP_TTL_MS.into()));

    let tight_ttl_timeout = tcp_datagram_response_timeout(high_rtt_tcp, None, None, 500);
    assert!(tight_ttl_timeout <= Duration::from_millis(450));

    let observed_timeout = tcp_datagram_response_timeout(
        high_rtt_tcp,
        Some(Duration::from_millis(300)),
        Some(Duration::from_millis(80)),
        DEFAULT_SOCKS5_UDP_TTL_MS,
    );
    assert!(observed_timeout >= Duration::from_millis(900));
    assert!(observed_timeout <= startup_timeout);
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

    assert_eq!(udp_edge_lane_limit(&low_context), 2);
    assert!(udp_edge_lane_limit(&high_context) > udp_edge_lane_limit(&low_context));

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

    context.mark_tcp_path_open_success(0, Duration::from_millis(1), TrafficClass::Interactive);
    assert_eq!(
        context
            .ordered_tcp_path_indices(TrafficClass::Interactive, 512)
            .first()
            .copied(),
        Some(1)
    );

    context.release_tcp_path_load(0, TrafficClass::Interactive);
    assert_eq!(
        context
            .ordered_tcp_path_indices(TrafficClass::Interactive, 512)
            .first()
            .copied(),
        Some(0)
    );
    let health = context.health.lock().expect("health lock");
    assert_eq!(health.tcp[0].active_flows, 0);
    assert_eq!(health.tcp[0].load_bytes, 0);
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

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), TrafficClass::Interactive);
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
            .ordered_tcp_path_indices(TrafficClass::Bulk, 4 * 1024 * 1024)
            .first()
            .copied(),
        Some(1)
    );
    assert_eq!(
        context
            .ordered_tcp_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES)
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
            .ordered_tcp_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES)
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
        context.ordered_tcp_path_indices(TrafficClass::RealtimeDatagram, PATH_OPEN_SCORE_BYTES),
        vec![0, 1]
    );

    context.mark_tcp_path_failure(0);
    assert_eq!(
        context.ordered_tcp_path_indices(TrafficClass::RealtimeDatagram, PATH_OPEN_SCORE_BYTES),
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
        context.ordered_tcp_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES),
        vec![0, 1]
    );

    context.mark_tcp_path_failure(0);
    assert_eq!(
        context.ordered_tcp_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES),
        vec![1]
    );
}

#[test]
fn endpoint_only_tcp_interactive_opens_stay_latency_first_under_active_flow() {
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

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), TrafficClass::Interactive);
    context.mark_tcp_path_probe_success(2, Duration::from_millis(1));

    assert_eq!(
        context.ordered_tcp_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES),
        vec![0, 1, 2]
    );
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
            .ordered_tcp_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES)
            .first()
            .copied(),
        Some(1)
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
    assert_eq!(
        udp_candidate_indices(&context, 512, DEFAULT_SOCKS5_UDP_TTL_MS)
            .first()
            .copied(),
        Some(0)
    );
    let health = context.health.lock().expect("health lock");
    assert_eq!(health.udp[0].active_flows, 0);
    assert_eq!(health.udp[0].load_bytes, 0);
}
