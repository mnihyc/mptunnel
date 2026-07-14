use super::*;

#[test]
fn automatic_bulk_use_honors_every_operator_capability() {
    let allowed = "tcp://127.0.0.1:10000"
        .parse::<PathSpec>()
        .expect("allowed path");
    let active = ClientPathObservation {
        state: SchedulerPathState::Active,
        ..ClientPathObservation::default()
    };
    assert!(path_allows_automatic_bulk_use(&allowed));
    assert!(path_can_be_auto_discovered(&allowed, active));
    let low_latency = "udp://127.0.0.1:10001?low-latency=true"
        .parse::<PathSpec>()
        .expect("low-latency path");
    assert!(path_allows_automatic_bulk_use(&low_latency));

    for query in [
        "expensive=true",
        "backup=true",
        "probe-only=true",
        "bulk=false",
    ] {
        let path = format!("udp://127.0.0.1:10002?{query}")
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
    let tcp = "tcp://127.0.0.1:10000?rate-mbps=400"
        .parse::<PathSpec>()
        .expect("TCP path");
    let product = ClientPathObservation {
        measured_rate_bps: Some(100_000_000.0),
        product_delivery_rate_bps: Some(120_000_000.0),
        product_delivery_sample_bytes: 1024 * 1024,
        delivery_samples: 1,
        ..ClientPathObservation::default()
    };
    let provisional_snapshot = path_snapshot(&tcp, 0, product);
    assert_eq!(provisional_snapshot.delivery_rate_bps, 400_000_000.0);
    assert_eq!(provisional_snapshot.rate_scope, PathRateScope::PathCapacity);

    let mature_product = ClientPathObservation {
        delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
        ..product
    };
    let product_snapshot = path_snapshot(&tcp, 0, mature_product);
    assert_eq!(product_snapshot.delivery_rate_bps, 120_000_000.0);
    assert_eq!(product_snapshot.rate_scope, PathRateScope::PerFlowGoodput);

    let carrier = ClientPathObservation {
        carrier_delivery_rate_bps: Some(500_000_000.0),
        ..mature_product
    };
    let carrier_snapshot = path_snapshot(&tcp, 0, carrier);
    assert_eq!(carrier_snapshot.delivery_rate_bps, 500_000_000.0);
    assert_eq!(carrier_snapshot.rate_scope, PathRateScope::PathCapacity);

    let generic = ClientPathObservation {
        measured_rate_bps: Some(90_000_000.0),
        ..ClientPathObservation::default()
    };
    assert_eq!(
        path_snapshot(&tcp, 0, generic).rate_scope,
        PathRateScope::PathCapacity
    );
    assert_eq!(
        path_snapshot(&tcp, 0, ClientPathObservation::default()).rate_scope,
        PathRateScope::PathCapacity
    );
}

#[test]
fn native_carrier_evidence_is_post_attachment_fresh_and_ack_derived() {
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
        carrier_app_limited: false,
        carrier_ack_derived_data_seen: true,
        ..ClientPathObservation::default()
    };
    assert!(bulk_candidate_has_fresh_native_carrier_rate_evidence(
        evidence,
        valid_after,
        now,
    ));

    let before_attachment = ClientPathObservation {
        carrier_last_delivery_at: Some(valid_after - Duration::from_millis(1)),
        ..evidence
    };
    assert!(!bulk_candidate_has_fresh_native_carrier_rate_evidence(
        before_attachment,
        valid_after,
        now,
    ));
    let future = ClientPathObservation {
        carrier_last_delivery_at: Some(now + Duration::from_millis(1)),
        ..evidence
    };
    assert!(!bulk_candidate_has_fresh_native_carrier_rate_evidence(
        future,
        valid_after,
        now,
    ));
    let stale = ClientPathObservation {
        carrier_last_delivery_at: Some(now - Duration::from_secs(10)),
        ..evidence
    };
    assert!(!bulk_candidate_has_fresh_native_carrier_rate_evidence(
        stale,
        now - Duration::from_secs(20),
        now,
    ));
}

#[test]
fn explicit_capacity_proof_does_not_inherit_the_grown_native_sample_floor() {
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

    assert!(bulk_candidate_has_native_carrier_rate_evidence(proof));
    assert_eq!(path_model_confidence(proof), 1.0);
    assert!(
        !bulk_candidate_has_native_carrier_rate_evidence(ClientPathObservation {
            explicit_carrier_capacity_proof: false,
            ..proof
        }),
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
