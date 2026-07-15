use super::*;
use crate::model::capacity::reliable_capacity_calibration_session_limit_bytes;
use crate::model::path::CarrierPathKey;
use crate::protocol::{Frame, PathId, SessionId, StreamOpenRole, UnderlayProtocol};
use crate::runtime::path::commands::{
    ReliablePathCommand, reliable_path_command_channels, try_recv_reliable_path_command,
};
use crate::runtime::sender::response::test_support::response_target;
use crate::runtime::stream::response::ServerPathLaneTracker;
use std::sync::Arc;

#[test]
fn sender_quic_geometry_is_accepted_by_the_binding_transaction() {
    let mux_limits = MuxLimits::default();
    let session_id = SessionId(911);
    let tracker = Arc::new(ServerPathLaneTracker::default());
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        service_commands,
        FlowLane::Throughput,
        mux_limits,
        tracker.clone(),
    );
    let (second_commands, _second_receivers) = reliable_path_command_channels(8);
    let _second_flow = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(2),
        second_commands,
        FlowLane::Throughput,
        mux_limits,
        tracker,
    );
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (candidate_commands, mut candidate_receivers) = reliable_path_command_channels(8);
    binding.attach(
        candidate.underlay,
        candidate.path_id,
        candidate_commands,
        FlowLane::Throughput,
        StreamOpenRole::Validation,
        mux_limits.max_payload_bytes,
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));

    let (planner_generation, _) = binding.subflow_state_snapshot();
    let scheduling = binding.response_scheduling_snapshot();
    let model_generation = binding.response_model_generation();
    let target = binding
        .sender_path_targets(FlowLane::Throughput, 64 * 1024)
        .into_iter()
        .find(|target| target.observation.key == candidate)
        .expect("UDP Validation target");
    let geometry = response_quic_capacity_calibration_geometry(&target, mux_limits);
    assert!(
        geometry.train_bytes as u64
            > geometry
                .carrier_window_bytes
                .saturating_add(geometry.fresh_strict_window_bytes),
        "the regression requires sender timing guard bytes"
    );
    assert!(binding.try_start_quic_capacity_calibration(
        &target,
        ResponseQuicCapacityCalibrationRequest {
            expected_planner_generation: planner_generation,
            expected_lane_generation: scheduling.generation,
            expected_model_generation: model_generation,
            target: candidate,
            target_path_instance_id: target.observation.path_instance_id,
            target_incarnation: target.observation.incarnation,
            target_pending_bytes: target.observation.command_pending_bytes,
            train_bytes: geometry.train_bytes,
            sample_floor_bytes: geometry.sample_floor_bytes,
            accounting_slack_bytes: geometry.accounting_slack_bytes,
            fresh_strict_window_bytes: geometry.fresh_strict_window_bytes,
            carrier_window_bytes: geometry.carrier_window_bytes,
            proof_validity: response_quic_capacity_proof_validity(&target),
            lease: response_quic_capacity_calibration_lease(&target, geometry.train_bytes),
        },
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_receivers),
        Some(ReliablePathCommand::SendQuicCapacityProbe(_))
    ));
}

#[test]
fn quic_capacity_calibration_requires_reachable_underloaded_family() {
    let service = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let mut udp = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    udp.observation.has_bulk_rate_evidence = false;

    assert_eq!(
        select_response_quic_capacity_calibration_target(
            &[service.clone(), udp.clone()],
            FlowLane::Throughput,
            Some(service.observation.key),
            ResponseServiceFamilyLoads::new(2, 0),
            MuxLimits::default(),
            reliable_capacity_calibration_session_limit_bytes(MuxLimits::default()),
        )
        .map(|target| target.observation.key),
        Some(udp.observation.key),
        "a native QUIC sample may break the proof cycle without product offsets"
    );
    assert!(
        select_response_quic_capacity_calibration_target(
            &[service.clone(), udp.clone()],
            FlowLane::Throughput,
            Some(service.observation.key),
            ResponseServiceFamilyLoads::new(1, 1),
            MuxLimits::default(),
            reliable_capacity_calibration_session_limit_bytes(MuxLimits::default()),
        )
        .is_none(),
        "balanced Service families need no optional carrier calibration"
    );
    udp.observation.has_sender_evidence = false;
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
    retry.observation.has_bulk_rate_evidence = false;
    retry.quic_capacity_calibration_attempts = 1;
    let mut fresh = response_target(2, UnderlayProtocol::Udp, 100.0, 0, 16 * 1024 * 1024, false);
    fresh.observation.has_bulk_rate_evidence = false;

    let selected = select_response_quic_capacity_calibration_target(
        &[service.clone(), retry, fresh.clone()],
        FlowLane::Throughput,
        Some(service.observation.key),
        ResponseServiceFamilyLoads::new(2, 0),
        mux_limits,
        reliable_capacity_calibration_session_limit_bytes(mux_limits),
    )
    .expect("at least one reachable UDP path should remain probeable");

    assert_eq!(
        selected.observation.key, fresh.observation.key,
        "an unattempted path must be sampled before a lower-ETA retry"
    );
}

#[test]
fn quic_capacity_calibration_filters_path_at_attempt_limit() {
    let mux_limits = MuxLimits::default();
    let service = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let mut exhausted = response_target(1, UnderlayProtocol::Udp, 1.0, 0, 16 * 1024 * 1024, false);
    exhausted.observation.has_bulk_rate_evidence = false;
    exhausted.quic_capacity_calibration_attempts =
        MAX_RESPONSE_QUIC_CAPACITY_CALIBRATION_ATTEMPTS_PER_PATH;

    assert!(
        select_response_quic_capacity_calibration_target(
            &[service.clone(), exhausted],
            FlowLane::Throughput,
            Some(service.observation.key),
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
    retry.observation.has_bulk_rate_evidence = false;
    retry.quic_capacity_calibration_attempts = 1;
    let mut fresh = response_target(2, UnderlayProtocol::Udp, 1.0, 0, session_limit, false);
    fresh.observation.has_bulk_rate_evidence = false;

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
        Some(service.observation.key),
        ResponseServiceFamilyLoads::new(2, 0),
        mux_limits,
        retry_train,
    )
    .expect("the smaller retry should fit the remaining session envelope");

    assert_eq!(
        selected.observation.key, retry.observation.key,
        "a too-large fresh train must not hide a retry that still fits the remaining budget"
    );
}

#[test]
fn quic_capacity_retry_fills_live_window_plus_fresh_proof_window() {
    let mux_limits = MuxLimits::default();
    let mut udp = response_target(3, UnderlayProtocol::Udp, 390.0, 0, 798_666, false);
    udp.observation.snapshot.pacing_rate_bps = 5_530_000.0;
    udp.observation.snapshot.delivery_rate_bps = 153_000.0;

    assert_eq!(
        response_quic_capacity_calibration_train_bytes(&udp, mux_limits),
        1_111_746,
        "the grown window needs one strict-proof window plus one pacing guard"
    );
    assert!(
        response_quic_capacity_calibration_lease(&udp, 1_111_746)
            >= transport_pto_from_snapshot(Some(udp.observation.snapshot)),
        "the admitted train lease must cover at least one recovery horizon"
    );

    udp.observation.snapshot.inflight_limit_bytes = u64::MAX;
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
