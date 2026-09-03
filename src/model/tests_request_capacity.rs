//! Request capacity model tests.

use super::*;
use crate::model::capacity::{MAX_RELIABLE_SERVICE_QUANTUM_BYTES, RELIABLE_INITIAL_WINDOW_PACKETS};
use crate::protocol::PathId;

#[test]
fn request_tcp_capacity_geometry_requires_mature_service_and_full_pipe() {
    let mux_limits = MuxLimits::default();
    let mut candidate = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 180.0, 1_000_000.0);
    candidate.carrier_inflight_limit_bytes = 32 * 1024 * 1024;
    let mature_service =
        RequestProductRateEpoch::for_test(100_000_000.0, RELIABLE_INITIAL_WINDOW_PACKETS as u32);
    let envelope = reliable_capacity_measurement_session_limit_bytes(mux_limits);

    let geometry =
        request_tcp_capacity_measurement_geometry(candidate, mature_service, mux_limits, envelope)
            .expect("the exact competing pipe fits the default carrier budget");
    assert_eq!(geometry.warmup_carrier_bytes, 4_500_000);
    assert_eq!(geometry.required_timed_carrier_bytes, 247_544);
    assert_eq!(
        geometry.timing_slack_bytes,
        MAX_RELIABLE_SERVICE_QUANTUM_BYTES as u64
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
    assert_eq!(geometry.reference_rate_bps, 100_000_000);
    assert!(
        request_tcp_capacity_measurement_geometry(
            candidate,
            RequestProductRateEpoch {
                delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32 - 1,
                ..mature_service
            },
            mux_limits,
            envelope,
        )
        .is_none(),
        "a startup/path-capacity prior must never size request TCP measurement"
    );
    assert!(
        request_tcp_capacity_measurement_geometry(
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
    let session_limit = reliable_capacity_measurement_session_limit_bytes(mux_limits);

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

    candidate.data_level_bytes_in_flight = 1;
    assert!(!request_tcp_capacity_candidate_can_start_receipt(candidate));
    candidate.data_level_bytes_in_flight = 0;
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
    let reference_rate_bps = 100_000_000;
    let pto = transport_pto_from_snapshot(Some(candidate));
    let growth = pto.saturating_mul(request_capacity_slow_start_rounds(train_bytes));
    let reference_transfer =
        Duration::from_secs_f64(train_bytes as f64 * 8.0 / reference_rate_bps as f64);
    let expected = pto
        .saturating_add(growth.max(reference_transfer))
        .saturating_add(pto)
        .max(Duration::from_secs(1));

    assert_eq!(
        request_tcp_capacity_measurement_lease(candidate, train_bytes, reference_rate_bps),
        expected,
        "every cold growth round owns one recovery-capable candidate PTO"
    );
}
