use super::*;
use crate::model::capacity::reliable_capacity_calibration_session_limit_bytes;
use crate::model::path::CarrierPathKey;
use crate::protocol::{PathId, UnderlayProtocol};
use crate::runtime::sender::response::test_support::response_target;

#[test]
fn quic_capacity_calibration_requires_reachable_underloaded_family() {
    let service = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let mut udp = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    udp.has_bulk_rate_evidence = false;

    assert_eq!(
        select_response_quic_capacity_calibration_target(
            &[service.clone(), udp.clone()],
            FlowLane::Throughput,
            Some(service.key),
            ResponseServiceFamilyLoads::new(2, 0),
            MuxLimits::default(),
            reliable_capacity_calibration_session_limit_bytes(MuxLimits::default()),
        )
        .map(|target| target.key),
        Some(udp.key),
        "a native QUIC sample may break the proof cycle without product offsets"
    );
    assert!(
        select_response_quic_capacity_calibration_target(
            &[service.clone(), udp.clone()],
            FlowLane::Throughput,
            Some(service.key),
            ResponseServiceFamilyLoads::new(1, 1),
            MuxLimits::default(),
            reliable_capacity_calibration_session_limit_bytes(MuxLimits::default()),
        )
        .is_none(),
        "balanced Service families need no optional carrier calibration"
    );
    udp.has_sender_evidence = false;
    assert!(
        select_response_quic_capacity_calibration_target(
            &[service, udp],
            FlowLane::Throughput,
            Some(CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(0),
            }),
            ResponseServiceFamilyLoads::new(2, 0),
            MuxLimits::default(),
            reliable_capacity_calibration_session_limit_bytes(MuxLimits::default()),
        )
        .is_none(),
        "capacity traffic must not replace path reachability proof"
    );
}

#[test]
fn quic_capacity_calibration_prefers_fresh_path_before_retry() {
    let mux_limits = MuxLimits::default();
    let service = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let mut retry = response_target(1, UnderlayProtocol::Udp, 1.0, 0, 16 * 1024 * 1024, false);
    retry.has_bulk_rate_evidence = false;
    retry.quic_capacity_calibration_attempts = 1;
    let mut fresh = response_target(2, UnderlayProtocol::Udp, 100.0, 0, 16 * 1024 * 1024, false);
    fresh.has_bulk_rate_evidence = false;

    let selected = select_response_quic_capacity_calibration_target(
        &[service.clone(), retry, fresh.clone()],
        FlowLane::Throughput,
        Some(service.key),
        ResponseServiceFamilyLoads::new(2, 0),
        mux_limits,
        reliable_capacity_calibration_session_limit_bytes(mux_limits),
    )
    .expect("at least one reachable UDP path should remain probeable");

    assert_eq!(
        selected.key, fresh.key,
        "an unattempted path must be sampled before a lower-ETA retry"
    );
}

#[test]
fn quic_capacity_calibration_filters_path_at_attempt_limit() {
    let mux_limits = MuxLimits::default();
    let service = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let mut exhausted = response_target(1, UnderlayProtocol::Udp, 1.0, 0, 16 * 1024 * 1024, false);
    exhausted.has_bulk_rate_evidence = false;
    exhausted.quic_capacity_calibration_attempts =
        MAX_RESPONSE_QUIC_CAPACITY_CALIBRATION_ATTEMPTS_PER_PATH;

    assert!(
        select_response_quic_capacity_calibration_target(
            &[service.clone(), exhausted],
            FlowLane::Throughput,
            Some(service.key),
            ResponseServiceFamilyLoads::new(2, 0),
            mux_limits,
            reliable_capacity_calibration_session_limit_bytes(mux_limits),
        )
        .is_none(),
        "a path must not exceed its exact-path calibration attempt limit"
    );
}

#[test]
fn quic_capacity_calibration_uses_smaller_retry_when_fresh_train_does_not_fit() {
    let mux_limits = MuxLimits::default();
    let session_limit = reliable_capacity_calibration_session_limit_bytes(mux_limits);
    let service = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let mut retry = response_target(1, UnderlayProtocol::Udp, 50.0, 0, 1, false);
    retry.has_bulk_rate_evidence = false;
    retry.quic_capacity_calibration_attempts = 1;
    let mut fresh = response_target(2, UnderlayProtocol::Udp, 1.0, 0, session_limit, false);
    fresh.has_bulk_rate_evidence = false;

    let retry_train = response_quic_capacity_calibration_train_bytes(&retry, mux_limits) as u64;
    let fresh_train = response_quic_capacity_calibration_train_bytes(&fresh, mux_limits) as u64;
    assert!(
        !response_quic_capacity_calibration_geometry(&fresh, mux_limits).fits_session_envelope,
        "a clamped train cannot silently change its frozen warmup/proof geometry"
    );
    assert!(
        retry_train < fresh_train,
        "the test needs a retry train that fits below the fresh path's live window"
    );

    let selected = select_response_quic_capacity_calibration_target(
        &[service.clone(), retry.clone(), fresh],
        FlowLane::Throughput,
        Some(service.key),
        ResponseServiceFamilyLoads::new(2, 0),
        mux_limits,
        retry_train,
    )
    .expect("the smaller retry should fit the remaining session envelope");

    assert_eq!(
        selected.key, retry.key,
        "a too-large fresh train must not hide a retry that still fits the remaining budget"
    );
}

#[test]
fn quic_capacity_retry_fills_live_window_plus_fresh_proof_window() {
    let mux_limits = MuxLimits::default();
    let mut udp = response_target(3, UnderlayProtocol::Udp, 390.0, 0, 798_666, false);
    udp.snapshot.pacing_rate_bps = 5_530_000.0;
    udp.snapshot.delivery_rate_bps = 153_000.0;

    assert_eq!(
        response_quic_capacity_calibration_train_bytes(&udp, mux_limits),
        1_111_746,
        "the grown window needs one strict-proof window plus one pacing guard"
    );
    assert!(
        response_quic_capacity_calibration_lease(&udp, 1_111_746)
            >= transport_pto_from_snapshot(Some(udp.snapshot)),
        "the admitted train lease must cover at least one recovery horizon"
    );

    udp.snapshot.inflight_limit_bytes = u64::MAX;
    assert!(
        !response_quic_capacity_calibration_geometry(&udp, mux_limits).fits_session_envelope,
        "a live window larger than the resource envelope is ineligible, not repeatedly reservable"
    );
    assert_eq!(
        response_quic_capacity_calibration_train_bytes(&udp, mux_limits) as u64,
        reliable_capacity_calibration_session_limit_bytes(mux_limits),
        "a single train cannot exceed the session carrier envelope"
    );
}
