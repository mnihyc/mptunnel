use super::super::ResponseStreamBinding;
use super::super::evidence::ServerPathMetricsSource;
use super::super::session::ServerPathLaneTracker;
use super::super::test_support::mark_test_quic_output_carrier_bulk_proven;
use super::ResponseQuicCapacityCalibrationRequest;
use crate::model::capacity::PATH_OPEN_SCORE_BYTES;
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::{Frame, PathId, SessionId, StreamOpenRole, UnderlayProtocol};
use crate::runtime::path::commands::{
    ReliablePathCommand, reliable_path_command_channels, try_recv_reliable_path_command,
};
use crate::scheduler::FlowLane;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[test]
fn quic_capacity_calibration_uses_carrier_bytes_without_product_flight() {
    let mux_limits = MuxLimits::default();
    let session_id = SessionId(510);
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
        .find(|target| target.key == candidate)
        .expect("UDP Validation target");
    let train_bytes = mux_limits
        .max_payload_bytes
        .saturating_add(PATH_OPEN_SCORE_BYTES);
    let sample_floor_bytes = train_bytes as u64;
    let accounting_slack_bytes = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor_bytes / 8);
    let required_proof_bytes = sample_floor_bytes - accounting_slack_bytes;
    assert!(binding.try_start_quic_capacity_calibration(
        &target,
        ResponseQuicCapacityCalibrationRequest {
            expected_planner_generation: planner_generation,
            expected_lane_generation: scheduling.generation,
            expected_model_generation: model_generation,
            target: candidate,
            target_path_instance_id: target.path_instance_id,
            target_incarnation: target.incarnation,
            target_pending_bytes: target.command_pending_bytes,
            train_bytes,
            sample_floor_bytes,
            accounting_slack_bytes,
            fresh_strict_window_bytes: required_proof_bytes,
            carrier_window_bytes: 0,
            lease: Duration::from_secs(1),
            proof_validity: Duration::from_secs(3),
        },
    ));
    let probe = match try_recv_reliable_path_command(&mut candidate_receivers)
        .expect("capacity probe command")
    {
        ReliablePathCommand::SendQuicCapacityProbe(probe) => probe,
        _ => panic!("expected typed QUIC capacity probe"),
    };
    assert_ne!(probe.calibration_id, 0);
    assert_eq!(probe.path_id, candidate.path_id);
    assert_eq!(probe.train_payload_bytes, train_bytes as u64);
    assert_eq!(probe.sample_floor_bytes, sample_floor_bytes);
    assert_eq!(probe.warmup_carrier_bytes, 0);
    assert_eq!(probe.required_timed_carrier_bytes, required_proof_bytes);
    assert!(probe.expires_at > Instant::now());
    assert!(
        binding
            .flights
            .lock()
            .expect("test response flight lock")
            .is_empty(),
        "carrier capacity bytes must not enter product ownership"
    );
    assert_eq!(
        binding.ordered_data_owner(),
        Some(CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        })
    );
    assert!(
        binding
            .response_scheduling_snapshot()
            .quic_capacity_calibration_reserved
    );
    let generic_bulk_metrics = {
        let mut entry = binding
            .outputs
            .lock()
            .expect("test response outputs lock")
            .entries
            .iter()
            .find(|entry| entry.key == candidate)
            .expect("UDP capacity candidate")
            .clone();
        mark_test_quic_output_carrier_bulk_proven(&mut entry, mux_limits);
        entry
            .local_path_metrics
            .expect("generic local bulk metrics")
            .metrics
    };
    binding.update_path_metrics(
        candidate,
        generic_bulk_metrics,
        ServerPathMetricsSource::LocalSender,
    );
    assert!(
        binding
            .response_scheduling_snapshot()
            .quic_capacity_calibration_reserved,
        "generic path metrics cannot complete a token-owned capacity train"
    );
}

#[test]
fn quic_capacity_lease_deadline_is_created_after_admission_and_failure_propagates() {
    let mux_limits = MuxLimits::default();
    let session_id = SessionId(519);
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
    let candidate_queue = candidate_commands.clone();
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
    let target = binding
        .sender_path_targets(FlowLane::Throughput, 64 * 1024)
        .into_iter()
        .find(|target| target.key == candidate)
        .expect("UDP Validation target");
    let pending_before_train = candidate_queue.pending_bytes();
    let train_bytes = mux_limits.max_payload_bytes / 2;
    let sample_floor_bytes = train_bytes as u64;
    let accounting_slack_bytes = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor_bytes / 8);
    let required_proof_bytes = sample_floor_bytes - accounting_slack_bytes;
    let mut deadline_observed_admitted_train = false;
    assert!(!binding.try_start_quic_capacity_calibration_with_lease(
        &target,
        ResponseQuicCapacityCalibrationRequest {
            expected_planner_generation: planner_generation,
            expected_lane_generation: scheduling.generation,
            expected_model_generation: binding.response_model_generation(),
            target: candidate,
            target_path_instance_id: target.path_instance_id,
            target_incarnation: target.incarnation,
            target_pending_bytes: target.command_pending_bytes,
            train_bytes,
            sample_floor_bytes,
            accounting_slack_bytes,
            fresh_strict_window_bytes: required_proof_bytes,
            carrier_window_bytes: 0,
            lease: Duration::from_secs(1),
            proof_validity: Duration::from_secs(3),
        },
        |_| {
            deadline_observed_admitted_train =
                candidate_queue.pending_bytes() > pending_before_train;
            Duration::ZERO
        },
    ));
    assert!(deadline_observed_admitted_train);
    let after_failed_commit = binding.response_scheduling_snapshot();
    assert!(!after_failed_commit.quic_capacity_calibration_reserved);
    assert_eq!(
        after_failed_commit.quic_capacity_calibration_spent_bytes, train_bytes as u64,
        "an admitted train remains charged even when its lease cannot commit"
    );
    assert_eq!(
        binding
            .lane_tracker
            .response_path_scheduling_snapshot(session_id, candidate, target.path_instance_id,)
            .quic_capacity_calibration_attempts,
        1
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_receivers),
        Some(ReliablePathCommand::SendQuicCapacityProbe(_))
    ));
}
