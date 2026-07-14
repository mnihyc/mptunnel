use super::*;
use crate::model::capacity::{BBR_MAX_SEND_QUANTUM_BYTES, RELIABLE_INITIAL_WINDOW_PACKETS};
use crate::protocol::PathId;

#[test]
fn request_quic_capacity_geometry_models_the_competing_service_rate_pipe() {
    let mux_limits = MuxLimits::default();
    let mut candidate = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, 1_000_000.0);
    candidate.inflight_limit_bytes = 262_144;

    let geometry = request_quic_capacity_calibration_geometry(
        candidate,
        100_000_000.0,
        mux_limits,
        reliable_capacity_calibration_session_limit_bytes(mux_limits),
    )
    .expect("the competing pipe should fit the default session envelope");

    assert_eq!(geometry.warmup_carrier_bytes, 4_500_000);
    assert_eq!(geometry.desired_warmup_carrier_bytes, 4_500_000);
    assert_eq!(geometry.service_rate_bps, 100_000_000);
    assert_eq!(geometry.candidate_carrier_flight_bytes, 0);
    assert_eq!(
        geometry.train_bytes,
        geometry
            .warmup_carrier_bytes
            .saturating_add(geometry.required_timed_carrier_bytes)
            .saturating_add(geometry.timing_slack_bytes),
        "the strict window retains a full callback-batching guard after cold-start warmup"
    );
    assert_eq!(
        geometry.accounting_slack_bytes,
        PATH_OPEN_SCORE_BYTES as u64
    );
}

#[test]
fn request_tcp_capacity_geometry_requires_mature_service_and_full_pipe() {
    let mux_limits = MuxLimits::default();
    let mut candidate = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 180.0, 1_000_000.0);
    candidate.inflight_limit_bytes = 32 * 1024 * 1024;
    let mature_service = RequestPerFlowRateModel {
        rate_bps: 100_000_000.0,
        delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
    };
    let envelope = reliable_capacity_calibration_session_limit_bytes(mux_limits);

    let geometry =
        request_tcp_capacity_calibration_geometry(candidate, mature_service, mux_limits, envelope)
            .expect("the exact competing pipe fits the default carrier budget");
    assert_eq!(geometry.warmup_carrier_bytes, 4_500_000);
    assert_eq!(geometry.required_timed_carrier_bytes, 247_544);
    assert_eq!(
        geometry.timing_slack_bytes,
        BBR_MAX_SEND_QUANTUM_BYTES as u64
    );
    let measurement_bytes = geometry
        .timing_slack_bytes
        .checked_add(geometry.required_timed_carrier_bytes)
        .expect("measurement sizing fits the carrier envelope");
    assert_eq!(measurement_bytes, 313_080);
    assert!(measurement_bytes >= geometry.sample_floor_bytes);
    assert_eq!(
        geometry.warmup_carrier_bytes + measurement_bytes,
        geometry.train_bytes,
        "the full receipt sample uses the existing bounded train"
    );
    assert_eq!(geometry.train_bytes, 4_813_080);
    assert_eq!(geometry.service_rate_bps, 100_000_000);
    assert!(
        request_tcp_capacity_calibration_geometry(
            candidate,
            RequestPerFlowRateModel {
                delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32 - 1,
                ..mature_service
            },
            mux_limits,
            envelope,
        )
        .is_none(),
        "a startup/path-capacity prior must never size request TCP calibration"
    );
    assert!(
        request_tcp_capacity_calibration_geometry(
            candidate,
            mature_service,
            mux_limits,
            geometry.train_bytes - 1,
        )
        .is_none(),
        "TCP must skip rather than truncate below its complete warmup and ACK span"
    );
}

#[test]
fn request_capacity_candidate_share_is_fixed_by_eligible_topology() {
    let mux_limits = MuxLimits::default();
    let session_limit = reliable_capacity_calibration_session_limit_bytes(mux_limits);

    assert_eq!(
        request_capacity_stable_candidate_share_bytes(mux_limits, 4),
        session_limit / 4
    );
    assert_eq!(
        request_capacity_stable_candidate_share_bytes(mux_limits, 2),
        session_limit / 2
    );
    assert_eq!(
        request_capacity_stable_candidate_share_bytes(mux_limits, 0),
        session_limit,
        "zero is a defensive no-candidate input, not a zero-byte divisor"
    );
}

#[test]
fn request_tcp_capacity_receipt_admission_ignores_only_stale_control_flight() {
    let mut candidate = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 180.0, 1_000_000.0);
    candidate.bytes_in_flight = 1_448;
    assert!(
        request_tcp_capacity_candidate_can_start_receipt(candidate),
        "a full typed receipt safely includes stale control delay"
    );

    candidate.product_bytes_in_flight = 1;
    assert!(!request_tcp_capacity_candidate_can_start_receipt(candidate));
    candidate.product_bytes_in_flight = 0;
    candidate.queue_bytes = 1;
    assert!(!request_tcp_capacity_candidate_can_start_receipt(candidate));
    candidate.queue_bytes = 0;
    candidate.active_latency_sensitive_flows = 1;
    assert!(!request_tcp_capacity_candidate_can_start_receipt(candidate));
}

#[test]
fn request_tcp_capacity_lease_is_derived_from_growth_service_and_recovery() {
    let candidate = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 180.0, 1_000_000.0);
    let train_bytes = 4_813_080;
    let service_rate_bps = 100_000_000;
    let pto = transport_pto_from_snapshot(Some(candidate));
    let growth = pto.saturating_mul(request_quic_capacity_slow_start_rounds(train_bytes));
    let service_transfer =
        Duration::from_secs_f64(train_bytes as f64 * 8.0 / service_rate_bps as f64);
    let expected = pto
        .saturating_add(growth.max(service_transfer))
        .saturating_add(pto)
        .max(Duration::from_secs(1));

    assert_eq!(
        request_tcp_capacity_calibration_lease(candidate, train_bytes, service_rate_bps),
        expected,
        "every cold growth round owns one recovery-capable candidate PTO"
    );
}

#[test]
fn request_quic_capacity_geometry_excludes_candidate_product_flight() {
    let mux_limits = MuxLimits::default();
    let envelope = reliable_capacity_calibration_session_limit_bytes(mux_limits);
    let mut candidate = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, 1_000_000.0);
    candidate.inflight_limit_bytes = 262_144;
    candidate.product_bytes_in_flight = 3_000_000;
    candidate.bytes_in_flight = 3_500_000;

    let first =
        request_quic_capacity_calibration_geometry(candidate, 1_000_000.0, mux_limits, envelope)
            .expect("the native carrier flight should fit");
    assert_eq!(first.candidate_carrier_flight_bytes, 500_000);
    assert_eq!(first.warmup_carrier_bytes, 500_000);

    candidate.product_bytes_in_flight = 7_000_000;
    candidate.bytes_in_flight = 7_500_000;
    let more_product =
        request_quic_capacity_calibration_geometry(candidate, 1_000_000.0, mux_limits, envelope)
            .expect("product debt must not alter carrier geometry");
    assert_eq!(more_product, first);

    candidate.bytes_in_flight = 7_750_000;
    let more_carrier =
        request_quic_capacity_calibration_geometry(candidate, 1_000_000.0, mux_limits, envelope)
            .expect("the larger native carrier flight should fit");
    assert_eq!(more_carrier.candidate_carrier_flight_bytes, 750_000);
    assert_eq!(more_carrier.warmup_carrier_bytes, 750_000);
}

#[test]
fn request_quic_capacity_geometry_requires_valid_rate_and_budget() {
    let mux_limits = MuxLimits::default();
    let candidate = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, 1_000_000.0);

    assert!(
        request_quic_capacity_calibration_geometry(
            candidate,
            f64::NAN,
            mux_limits,
            reliable_capacity_calibration_session_limit_bytes(mux_limits),
        )
        .is_none(),
        "an invalid carrier rate must not size capacity traffic"
    );

    let bounded = request_quic_capacity_calibration_geometry(
        candidate,
        2_000_000_000.0,
        mux_limits,
        reliable_capacity_calibration_session_limit_bytes(mux_limits),
    )
    .expect("a bounded train can still test capacity below a larger competing pipe");
    assert_eq!(
        bounded.train_bytes,
        reliable_capacity_calibration_session_limit_bytes(mux_limits)
    );
    assert!(bounded.desired_warmup_carrier_bytes > bounded.warmup_carrier_bytes);

    let mut carrier_loaded = candidate;
    carrier_loaded.bytes_in_flight = reliable_capacity_calibration_session_limit_bytes(mux_limits);
    assert!(
        request_quic_capacity_calibration_geometry(
            carrier_loaded,
            100_000_000.0,
            mux_limits,
            reliable_capacity_calibration_session_limit_bytes(mux_limits),
        )
        .is_none(),
        "the session envelope must not clamp below native carrier flight"
    );
}

#[test]
fn request_quic_capacity_lease_covers_cold_congestion_window_growth() {
    let mut candidate = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, 1_000_000.0);
    candidate.jitter_ms = 90.0;
    let train_bytes = 8_553_080;
    let rounds = request_quic_capacity_slow_start_rounds(train_bytes);
    let pto = transport_pto_from_snapshot(Some(candidate));
    let modeled_round_trip = Duration::from_millis(180).max(pto.div_f64(BBR_DEFAULT_CWND_GAIN));

    assert_eq!(rounds, 10);
    assert!(
        request_quic_capacity_calibration_lease(candidate, train_bytes)
            >= modeled_round_trip
                .saturating_mul(rounds)
                .saturating_add(pto),
        "a competing-pipe train must not inherit the smaller startup-sample deadline"
    );
}
