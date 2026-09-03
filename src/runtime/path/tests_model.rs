use super::*;

#[test]
fn configured_order_startup_excludes_backup_while_available_path_is_schedulable() {
    let paths = [
        "tcp://127.0.0.1:10000".parse::<PathSpec>().expect("path"),
        "tcp://127.0.0.1:10001".parse::<PathSpec>().expect("path"),
    ];
    let observations = [
        ClientPathObservation {
            state: SchedulerPathState::Active,
            peer_usage: Some(crate::protocol::PathUsage::Backup),
            ..ClientPathObservation::default()
        },
        ClientPathObservation {
            state: SchedulerPathState::Active,
            peer_usage: Some(crate::protocol::PathUsage::Available),
            ..ClientPathObservation::default()
        },
    ];

    assert_eq!(
        configured_order_path_indices(&paths, &observations, TrafficClass::Latency, 1),
        vec![1]
    );
}

#[test]
fn automatic_bulk_use_honors_every_operator_policy() {
    let allowed = "tcp://127.0.0.1:10000"
        .parse::<PathSpec>()
        .expect("allowed path");
    let active = ClientPathObservation {
        state: SchedulerPathState::Active,
        ..ClientPathObservation::default()
    };
    assert!(path_allows_automatic_bulk_use(&allowed));
    assert!(path_can_be_auto_discovered(&allowed, active));
    for query in [
        "expensive=true",
        "backup=true",
        "control-only=true",
        "allow-bulk=false",
    ] {
        let path = format!("quic://127.0.0.1:10002?{query}")
            .parse::<PathSpec>()
            .expect("policy path");
        assert!(
            !path_allows_automatic_bulk_use(&path),
            "{query} must block automatic bulk use"
        );
        assert!(
            !path_can_be_auto_discovered(&path, active),
            "{query} must block automatic discovery"
        );
    }

    assert!(!path_can_be_auto_discovered(
        &allowed,
        ClientPathObservation {
            state: SchedulerPathState::Suspect,
            ..active
        }
    ));
}

#[test]
fn path_snapshot_preserves_rate_provenance() {
    let tcp = "tcp://127.0.0.1:10000?initial-rate-mbps=400"
        .parse::<PathSpec>()
        .expect("TCP path");
    let now = Instant::now();
    let product = ClientPathObservation {
        measured_rate_bps: Some(100_000_000.0),
        product_delivery_rate_bps: Some(120_000_000.0),
        product_delivery_sample_bytes: 1024 * 1024,
        product_delivery_samples: 1,
        product_last_delivery_at: Some(now),
        product_delivery_rate_expires_at: Some(now + Duration::from_secs(1)),
        ..ClientPathObservation::default()
    };
    let provisional_snapshot = path_snapshot(&tcp, 0, product);
    assert_eq!(provisional_snapshot.delivery_rate_bps, 400_000_000.0);
    assert_eq!(provisional_snapshot.rate_scope, PathRateScope::PathCapacity);
    assert_eq!(
        path_metrics_from_snapshot_at(
            provisional_snapshot,
            product,
            PathMetricDirection::ClientToServer,
            now,
        )
        .rate_valid_for_us,
        0,
        "a provisional Product sample must not lend its deadline to the selected startup prior"
    );

    let mature_product = ClientPathObservation {
        product_delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
        ..product
    };
    let product_snapshot = path_snapshot(&tcp, 0, mature_product);
    assert_eq!(product_snapshot.delivery_rate_bps, 400_000_000.0);
    assert_eq!(product_snapshot.rate_scope, PathRateScope::PathCapacity);
    assert_eq!(
        product_snapshot.product_progress_rate_bps,
        Some(120_000_000.0),
        "the mature exact Product interval remains visible as a historical lower bound",
    );
    assert!(product_snapshot.has_durable_product_progress);
    assert_eq!(
        path_metrics_from_snapshot_at(
            product_snapshot,
            mature_product,
            PathMetricDirection::ClientToServer,
            now,
        )
        .rate_valid_for_us,
        0,
        "a lower Product bound cannot lend its deadline to the selected configured baseline",
    );

    let carrier = ClientPathObservation {
        carrier_delivery_rate_bps: Some(500_000_000.0),
        carrier_inflight_limit_bytes: 512 * 1024,
        carrier_delivery_samples: 1,
        carrier_delivery_sample_bytes: 512 * 1024,
        carrier_delivery_window_covered: true,
        carrier_last_delivery_at: Some(now),
        carrier_bulk_proof_expires_at: Some(now + Duration::from_secs(2)),
        carrier_app_limited: false,
        ..mature_product
    };
    let carrier_snapshot = path_snapshot(&tcp, 0, carrier);
    assert_eq!(carrier_snapshot.delivery_rate_bps, 500_000_000.0);
    assert_eq!(carrier_snapshot.rate_scope, PathRateScope::PathCapacity);
    assert_eq!(
        path_metrics_from_snapshot_at(
            carrier_snapshot,
            carrier,
            PathMetricDirection::ClientToServer,
            now,
        )
        .rate_valid_for_us,
        2_000_000
    );

    let generic = ClientPathObservation {
        measured_rate_bps: Some(90_000_000.0),
        delivery_samples: 1,
        delivery_sample_bytes: 32 * 1024,
        last_delivery_at: Some(now),
        delivery_rate_expires_at: Some(now + Duration::from_secs(3)),
        ..ClientPathObservation::default()
    };
    assert_eq!(
        path_snapshot(&tcp, 0, generic).rate_scope,
        PathRateScope::PathCapacity
    );
    assert_eq!(
        path_metrics_from_snapshot_at(
            path_snapshot(&tcp, 0, generic),
            generic,
            PathMetricDirection::ClientToServer,
            now,
        )
        .rate_valid_for_us,
        3_000_000
    );
    assert_eq!(
        path_snapshot(&tcp, 0, ClientPathObservation::default()).rate_scope,
        PathRateScope::PathCapacity
    );
}

#[test]
fn path_snapshot_uses_current_native_underfill_not_rate_epoch_provenance() {
    let udp = "quic://127.0.0.1:10000"
        .parse::<PathSpec>()
        .expect("UDP path");
    let snapshot = path_snapshot(
        &udp,
        0,
        ClientPathObservation {
            carrier_delivery_rate_bps: Some(3_000_000.0),
            carrier_app_limited: false,
            carrier_current_app_limited: Some(true),
            carrier_bytes_in_flight: 64 * 1024,
            relay_bytes_in_flight: 256 * 1024,
            ..ClientPathObservation::default()
        },
    );

    assert!(
        snapshot.app_limited,
        "live local underfill must remain visible independently of the retained rate epoch"
    );
}

#[test]
fn native_carrier_evidence_is_post_attachment_fresh_and_ack_derived() {
    let udp = "quic://127.0.0.1:10000"
        .parse::<PathSpec>()
        .expect("UDP path");
    let now = Instant::now();
    let valid_after = now - Duration::from_millis(10);
    let evidence = ClientPathObservation {
        carrier_srtt_ms: Some(20.0),
        carrier_rttvar_ms: Some(2.0),
        carrier_delivery_rate_bps: Some(500_000_000.0),
        carrier_inflight_limit_bytes: 4 * 1024 * 1024,
        carrier_delivery_samples: 1,
        carrier_delivery_sample_bytes: 4 * 1024 * 1024,
        carrier_last_delivery_at: Some(now - Duration::from_millis(1)),
        carrier_bulk_proof_expires_at: Some(now + Duration::from_secs(2)),
        carrier_app_limited: false,
        carrier_current_app_limited: Some(true),
        carrier_ack_derived_data_seen: true,
        ..ClientPathObservation::default()
    };
    assert!(bulk_candidate_has_fresh_native_carrier_rate_evidence(
        &udp,
        evidence,
        valid_after,
        now,
    ));

    let before_attachment = ClientPathObservation {
        carrier_last_delivery_at: Some(valid_after - Duration::from_millis(1)),
        ..evidence
    };
    assert!(!bulk_candidate_has_fresh_native_carrier_rate_evidence(
        &udp,
        before_attachment,
        valid_after,
        now,
    ));
    let future = ClientPathObservation {
        carrier_last_delivery_at: Some(now + Duration::from_millis(1)),
        ..evidence
    };
    assert!(!bulk_candidate_has_fresh_native_carrier_rate_evidence(
        &udp,
        future,
        valid_after,
        now,
    ));
    let stale = ClientPathObservation {
        carrier_last_delivery_at: Some(now - Duration::from_secs(10)),
        carrier_bulk_proof_expires_at: Some(now - Duration::from_millis(1)),
        ..evidence
    };
    assert!(!bulk_candidate_has_fresh_native_carrier_rate_evidence(
        &udp,
        stale,
        now - Duration::from_secs(20),
        now,
    ));
}

#[test]
fn passive_tcp_carrier_rate_never_claims_mpp_data_ack_evidence() {
    let tcp = "tcp://127.0.0.1:10000"
        .parse::<PathSpec>()
        .expect("TCP path");
    let now = Instant::now();
    let observation = ClientPathObservation {
        state: SchedulerPathState::Active,
        carrier_srtt_ms: Some(180.0),
        carrier_rttvar_ms: Some(10.0),
        carrier_delivery_rate_bps: Some(200_000_000.0),
        carrier_pacing_rate_bps: Some(250_000_000.0),
        carrier_inflight_limit_bytes: 512 * 1024,
        carrier_delivery_samples: 2,
        carrier_delivery_sample_bytes: 1024 * 1024,
        carrier_delivery_window_covered: true,
        carrier_last_delivery_at: Some(now),
        carrier_bulk_proof_expires_at: Some(now + Duration::from_secs(1)),
        carrier_app_limited: false,
        carrier_ack_derived_data_seen: false,
        ..ClientPathObservation::default()
    };

    assert!(bulk_candidate_has_native_carrier_rate_evidence(
        &tcp,
        observation
    ));
    assert!(!bulk_candidate_has_ack_data_evidence(&tcp, observation));
    let snapshot = path_snapshot(&tcp, 0, observation);
    assert_eq!(snapshot.delivery_rate_bps, 200_000_000.0);
    assert_eq!(snapshot.pacing_rate_bps, 200_000_000.0);
    let metrics =
        path_metrics_from_snapshot(snapshot, observation, PathMetricDirection::ClientToServer);
    assert!(!metrics.has_ack_derived_data_sample);
    assert_eq!(metrics.data_sample_count, 0);
    assert_eq!(metrics.data_sample_bytes, 0);
    assert_eq!(metrics.delivery_rate_bps, 200_000_000);
    assert_eq!(metrics.pacing_rate_bps, 250_000_000);
    assert!(metrics.pacing_rate_observed);
    assert!(metrics.rate_valid_for_us > 0);

    let preliminary = ClientPathObservation {
        carrier_delivery_window_covered: false,
        ..observation
    };
    assert!(!bulk_candidate_has_native_carrier_rate_evidence(
        &tcp,
        preliminary
    ));
    assert_ne!(
        path_snapshot(&tcp, 0, preliminary).delivery_rate_bps,
        200_000_000.0
    );
}

#[test]
fn tcp_native_peer_metrics_preserve_fresh_and_stale_rate_provenance_without_product_ack() {
    let tcp = "tcp://127.0.0.1:10000"
        .parse::<PathSpec>()
        .expect("TCP path");
    let now = Instant::now();
    let observation = ClientPathObservation {
        carrier_delivery_rate_bps: Some(200_000_000.0),
        carrier_pacing_rate_bps: Some(250_000_000.0),
        carrier_delivery_samples: 1,
        carrier_delivery_sample_bytes: MAX_RELIABLE_SERVICE_QUANTUM_BYTES as u64,
        carrier_delivery_window_covered: true,
        carrier_last_delivery_at: Some(now - Duration::from_micros(7)),
        carrier_bulk_proof_expires_at: Some(now + Duration::from_micros(1)),
        carrier_app_limited: false,
        carrier_current_app_limited: Some(true),
        ..ClientPathObservation::default()
    };
    let snapshot = path_snapshot(&tcp, 0, observation);
    let near_expiry = path_metrics_from_snapshot_at(
        snapshot,
        observation,
        PathMetricDirection::ClientToServer,
        now,
    );
    assert_eq!(near_expiry.metric_age_us, 7);
    assert_eq!(near_expiry.rate_valid_for_us, 1);
    assert!(near_expiry.rate_observed);
    assert!(near_expiry.pacing_rate_observed);
    assert_eq!(near_expiry.pacing_rate_bps, 250_000_000);
    assert!(snapshot.app_limited);
    assert!(
        !near_expiry.app_limited,
        "peer metrics must carry the immutable non-app-limited rate-epoch provenance, not live local underfill"
    );
    assert!(!near_expiry.has_ack_derived_data_sample);
    assert_eq!(near_expiry.data_sample_count, 0);
    assert_eq!(near_expiry.data_sample_bytes, 0);

    let expired = path_metrics_from_snapshot_at(
        snapshot,
        observation,
        PathMetricDirection::ClientToServer,
        now + Duration::from_micros(1),
    );
    assert_eq!(expired.rate_valid_for_us, 0);
    assert!(expired.rate_observed);
    assert!(expired.pacing_rate_observed);
    assert_eq!(expired.pacing_rate_bps, 250_000_000);
    assert!(!expired.has_ack_derived_data_sample);

    let startup = path_startup_metrics(&tcp, PathId(0), PathMetricDirection::ClientToServer);
    assert_eq!(startup.rate_valid_for_us, 0);
    assert!(!startup.rate_observed);
    assert!(!startup.pacing_rate_observed);

    let overlong = ClientPathObservation {
        carrier_bulk_proof_expires_at: now.checked_add(Duration::from_micros(
            crate::protocol::PATH_METRICS_MAX_RATE_VALID_FOR_US + 1,
        )),
        ..observation
    };
    let capped = path_metrics_from_snapshot_at(
        path_snapshot(&tcp, 0, overlong),
        overlong,
        PathMetricDirection::ClientToServer,
        now,
    );
    assert_eq!(
        capped.rate_valid_for_us,
        crate::protocol::PATH_METRICS_MAX_RATE_VALID_FOR_US
    );
}

#[test]
fn explicit_capacity_proof_does_not_inherit_the_grown_native_sample_floor() {
    let tcp = "tcp://127.0.0.1:10000"
        .parse::<PathSpec>()
        .expect("TCP path");
    let proof = ClientPathObservation {
        carrier_delivery_rate_bps: Some(117_000_000.0),
        carrier_inflight_limit_bytes: 16 * 1024 * 1024,
        carrier_delivery_samples: 1,
        carrier_delivery_sample_bytes: 247_544,
        carrier_app_limited: false,
        carrier_ack_derived_data_seen: true,
        explicit_carrier_capacity_proof: true,
        ..ClientPathObservation::default()
    };

    assert!(bulk_candidate_has_native_carrier_rate_evidence(&tcp, proof));
    assert_eq!(path_model_confidence(proof), 1.0);
    let mut snapshot = path_snapshot(&tcp, 0, proof);
    let mux_limits = crate::mux::MuxLimits::default();
    snapshot.data_level_limit_bytes =
        crate::model::capacity::reliable_bulk_product_windows(mux_limits)
            .per_output_product_limit_bytes;
    assert!(!snapshot.has_durable_product_progress);
    assert_eq!(
        crate::model::admission::bulk_original_data_assignment_authority(
            snapshot,
            MAX_RELIABLE_SERVICE_QUANTUM_BYTES,
            mux_limits,
            crate::model::admission::BulkCandidatePosition::AdditionalPath,
            snapshot.has_durable_product_progress,
        )
        .assignment_limit_bytes,
        crate::model::capacity::reliable_bulk_unproven_exploration_limit_bytes(
            snapshot, mux_limits,
        ),
        "a native-only capacity proof cannot qualify Product P",
    );
    assert!(
        !bulk_candidate_has_native_carrier_rate_evidence(
            &tcp,
            ClientPathObservation {
                explicit_carrier_capacity_proof: false,
                ..proof
            }
        ),
        "generic rolling ACK evidence still requires coverage of its live carrier window"
    );
    assert!(
        path_model_confidence(ClientPathObservation {
            explicit_carrier_capacity_proof: false,
            ..proof
        }) < 1.0,
        "one ordinary aggregate sample must not inherit capacity-train confidence"
    );
}

#[test]
fn product_delivery_evidence_does_not_chase_a_growing_tcp_cwnd() {
    let tcp = "tcp://127.0.0.1:10000"
        .parse::<PathSpec>()
        .expect("TCP path");
    let product = ClientPathObservation {
        product_delivery_rate_bps: Some(3_000_000.0),
        product_delivery_sample_bytes: MAX_RELIABLE_SERVICE_QUANTUM_BYTES as u64,
        product_delivery_samples: 1,
        carrier_inflight_limit_bytes: 16 * 1024 * 1024,
        ..ClientPathObservation::default()
    };

    assert!(bulk_candidate_has_bulk_rate_evidence(&tcp, product));
    assert!(path_snapshot(&tcp, 0, product).has_durable_product_progress);
}

#[test]
fn global_product_rate_requires_the_exact_service_quantum_boundary() {
    let udp = "quic://127.0.0.1:10000?initial-rate-mbps=25"
        .parse::<PathSpec>()
        .expect("QUIC path");
    let sample_floor = MAX_RELIABLE_SERVICE_QUANTUM_BYTES as u64;
    let raw_rate = 900_000_000.0;
    let boundary_minus_one = ClientPathObservation {
        product_delivery_rate_bps: Some(raw_rate),
        product_delivery_sample_bytes: sample_floor - 1,
        product_delivery_samples: 1,
        ..ClientPathObservation::default()
    };

    let partial = path_snapshot(&udp, 0, boundary_minus_one);
    assert_eq!(partial.delivery_rate_bps, 25_000_000.0);
    assert_eq!(partial.rate_scope, PathRateScope::PathCapacity);
    assert_eq!(partial.product_progress_rate_bps, None);
    assert!(!partial.has_durable_product_progress);

    let qualified = path_snapshot(
        &udp,
        0,
        ClientPathObservation {
            product_delivery_sample_bytes: sample_floor,
            ..boundary_minus_one
        },
    );
    assert_eq!(qualified.delivery_rate_bps, raw_rate);
    assert_eq!(qualified.rate_scope, PathRateScope::PerFlowGoodput);
    assert_eq!(qualified.product_progress_rate_bps, Some(raw_rate));
    assert!(qualified.has_durable_product_progress);
}

#[test]
fn stale_rate_evidence_cannot_win_scheduler_or_capacity_selection() {
    let paths = [
        "tcp://127.0.0.1:10000".parse::<PathSpec>().expect("path"),
        "tcp://127.0.0.1:10001".parse::<PathSpec>().expect("path"),
    ];
    let now = Instant::now();
    let stale_at = now - Duration::from_secs(60);
    let mut stale_record = ClientPathHealthRecord::default();
    stale_record.measured_srtt_ms = Some(20.0);
    stale_record.measured_jitter_ms = Some(5.0);
    stale_record.measured_rate_bps = Some(1_000_000_000.0);
    stale_record.delivery_samples = 100;
    stale_record.product_delivery_rate_bps = Some(900_000_000.0);
    stale_record.product_delivery_samples = 100;
    stale_record.product_delivery_sample_bytes = 8 * 1024 * 1024;
    stale_record.last_delivery_at = Some(stale_at);
    stale_record.delivery_rate_expires_at = Some(stale_at + Duration::from_secs(1));
    stale_record.product_last_delivery_at = Some(stale_at);
    stale_record.product_delivery_rate_expires_at = Some(stale_at + Duration::from_secs(1));
    stale_record.carrier_srtt_ms = Some(20.0);
    stale_record.carrier_rttvar_ms = Some(5.0);
    stale_record.carrier_delivery_rate_bps = Some(1_100_000_000.0);
    stale_record.carrier_delivery_samples = 100;
    stale_record.carrier_delivery_sample_bytes = 8 * 1024 * 1024;
    stale_record.carrier_delivery_window_covered = true;
    stale_record.carrier_last_delivery_at = Some(stale_at);
    stale_record.carrier_app_limited = false;

    let mut fresh_record = ClientPathHealthRecord::default();
    fresh_record.measured_srtt_ms = Some(20.0);
    fresh_record.measured_jitter_ms = Some(5.0);
    fresh_record.measured_rate_bps = Some(10_000_000.0);
    fresh_record.delivery_samples = RELIABLE_INITIAL_WINDOW_PACKETS as u32;
    fresh_record.product_delivery_rate_bps = Some(10_000_000.0);
    fresh_record.product_delivery_samples = RELIABLE_INITIAL_WINDOW_PACKETS as u32;
    fresh_record.product_delivery_sample_bytes = 1024 * 1024;
    fresh_record.last_delivery_at = Some(now - Duration::from_millis(1));
    fresh_record.delivery_rate_expires_at = Some(now + Duration::from_secs(1));
    fresh_record.product_last_delivery_at = Some(now - Duration::from_millis(1));
    fresh_record.product_delivery_rate_expires_at = Some(now + Duration::from_secs(1));
    let observations = [
        stale_record.observation_at(now),
        fresh_record.observation_at(now),
    ];

    assert_eq!(observations[0].measured_rate_bps, None);
    assert_eq!(observations[0].product_delivery_rate_bps, None);
    assert_eq!(observations[0].carrier_delivery_rate_bps, None);
    assert!(!bulk_candidate_has_bulk_rate_evidence(
        &paths[0],
        observations[0]
    ));
    assert!(bulk_candidate_has_bulk_rate_evidence(
        &paths[1],
        observations[1]
    ));
    assert_eq!(
        path_snapshot(&paths[0], 0, observations[0]).delivery_rate_bps,
        default_path_rate_bps()
    );
    assert_eq!(
        path_snapshot(&paths[1], 1, observations[1]).delivery_rate_bps,
        10_000_000.0
    );
    assert_eq!(
        ordered_path_scores(&paths, &observations, TrafficClass::Throughput, 64 * 1024,)[0].0,
        1,
        "expired gigabit evidence must not outrank current measured delivery"
    );
}
