use super::super::ResponseStreamBinding;
use super::super::ack_clock::ResponseAckClockCalibrationState;
use super::super::attachment::{ResponseDispatchTarget, ResponseStreamAttachOutcome};
use super::super::session::ServerPathLaneTracker;
use super::super::subflow::ResponseSubflowAdmissionRequest;
use super::super::test_support::{
    mark_test_response_output_bulk_proven, stream_data_frame, stream_data_frame_at,
    test_ack_clock_rate_sample,
};
use super::{ResponseAckClockCalibrationRequest, ResponseOwnerEnqueueAdmission};
use crate::model::ack_clock::{
    reliable_ack_clock_calibration_ceiling_bytes, reliable_ack_clock_calibration_limit_bytes,
};
use crate::model::capacity::{PATH_OPEN_SCORE_BYTES, reliable_bulk_carrier_feed_quantum_bytes};
use crate::model::multipath::{PathAdmissionDecision, SubflowAdmissionInput};
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::{Frame, PathId, SessionId, StreamOpenRole, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommand, reliable_path_command_channels, reliable_path_command_pending_bytes,
    try_recv_reliable_path_command,
};
use crate::scheduler::FlowLane;
use std::sync::{Arc, mpsc as std_mpsc};
use std::time::Duration;

#[test]
fn service_admission_publishes_queue_flight_and_owner_as_one_transaction() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = PATH_OPEN_SCORE_BYTES;
    let session_id = SessionId(189);
    let tracker = Arc::new(ServerPathLaneTracker::default());
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        service.underlay,
        service.path_id,
        service_commands.clone(),
        FlowLane::Throughput,
        mux_limits,
        tracker.clone(),
    );
    let (candidate_commands, mut candidate_receivers) = reliable_path_command_channels(1);
    assert_eq!(
        binding.attach(
            candidate.underlay,
            candidate.path_id,
            candidate_commands.clone(),
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let proof = try_recv_reliable_path_command(&mut candidate_receivers)
        .expect("candidate path proof is queued");
    assert!(matches!(
        &proof,
        ReliablePathCommand::SendFrame(Frame::PathProofData { .. })
    ));
    candidate_receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&proof));

    binding.detach(service, &service_commands);
    assert_eq!(binding.ordered_data_owner(), None);
    assert_eq!(
        tracker
            .response_service_snapshot(session_id, service)
            .active_flows,
        0
    );
    assert_eq!(
        tracker
            .response_service_snapshot(session_id, candidate)
            .active_flows,
        0
    );

    let target = binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.observation.key == candidate)
        .expect("candidate target remains attached");
    let identity = (target.observation.key, target.observation.incarnation);
    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        outputs.ack_clock_calibrations.insert(
            identity,
            ResponseAckClockCalibrationState::new(
                reliable_ack_clock_calibration_limit_bytes(mux_limits),
                reliable_ack_clock_calibration_ceiling_bytes(mux_limits),
            ),
        );
        outputs.active_ack_clock_calibration = Some(identity);
    }
    let frame = stream_data_frame(payload_bytes);
    let unchanged_planner_generation = binding.subflow_state_snapshot().0;
    let unchanged_lane_generation = binding.lane_generation();

    let mut stale_target = target.clone();
    stale_target.observation.incarnation = stale_target.observation.incarnation.wrapping_add(1);
    assert!(matches!(
        binding.try_enqueue_owner_frame_for_target(
            &stale_target,
            &frame,
            FlowLane::Throughput,
            ResponseOwnerEnqueueAdmission::Service,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert_eq!(binding.ordered_data_owner(), None);
    assert_eq!(
        binding.subflow_state_snapshot().0,
        unchanged_planner_generation
    );
    assert_eq!(binding.lane_generation(), unchanged_lane_generation);
    assert!(
        binding
            .owner_flight_keys_overlapping_frame(&frame)
            .is_empty()
    );
    {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        assert_eq!(outputs.active_ack_clock_calibration, Some(identity));
        assert_eq!(
            outputs
                .ack_clock_calibrations
                .get(&identity)
                .expect("stale target preserves calibration")
                .spent_bytes,
            0
        );
    }
    assert!(try_recv_reliable_path_command(&mut candidate_receivers).is_none());

    candidate_commands
        .try_enqueue_stream_ordered_frame(
            stream_data_frame_at(payload_bytes as u64, payload_bytes),
            FlowLane::Throughput,
        )
        .expect("fill candidate command queue");
    assert!(matches!(
        binding.try_enqueue_owner_frame_for_target(
            &target,
            &frame,
            FlowLane::Throughput,
            ResponseOwnerEnqueueAdmission::Service,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert_eq!(binding.ordered_data_owner(), None);
    assert_eq!(
        binding.subflow_state_snapshot().0,
        unchanged_planner_generation
    );
    assert_eq!(binding.lane_generation(), unchanged_lane_generation);
    assert!(
        binding
            .owner_flight_keys_overlapping_frame(&frame)
            .is_empty()
    );
    {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == candidate)
            .expect("candidate output remains attached");
        assert_eq!(entry.owner_data_in_flight_bytes, 0);
        assert_eq!(entry.bytes_in_flight, 0);
        assert_eq!(outputs.active_ack_clock_calibration, Some(identity));
        assert!(outputs.ack_clock_calibrations.contains_key(&identity));
    }
    assert_eq!(
        tracker
            .response_service_snapshot(session_id, candidate)
            .active_flows,
        0
    );
    let filler = try_recv_reliable_path_command(&mut candidate_receivers)
        .expect("only the queue filler remains after blocked admission");
    candidate_receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&filler));
    assert!(try_recv_reliable_path_command(&mut candidate_receivers).is_none());

    binding
        .try_enqueue_owner_frame_for_target(
            &target,
            &frame,
            FlowLane::Throughput,
            ResponseOwnerEnqueueAdmission::Service,
        )
        .expect("live Service target commits");
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert_eq!(binding.ordered_data_owner(), Some(candidate));
    assert_ne!(
        binding.subflow_state_snapshot().0,
        unchanged_planner_generation
    );
    assert_ne!(binding.lane_generation(), unchanged_lane_generation);
    assert_eq!(
        tracker
            .response_service_snapshot(session_id, candidate)
            .active_flows,
        1
    );
    assert_eq!(
        binding.owner_flight_keys_overlapping_frame(&frame),
        vec![candidate]
    );
    {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == candidate)
            .expect("candidate output remains attached");
        assert_eq!(entry.owner_data_in_flight_bytes, payload_bytes as u64);
        assert_eq!(entry.bytes_in_flight, payload_bytes as u64);
        assert_eq!(outputs.active_ack_clock_calibration, None);
        assert!(!outputs.ack_clock_calibrations.contains_key(&identity));
    }
}

#[test]
fn tcp_calibration_commit_fences_generations_and_rolls_back_blocked_enqueue() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = PATH_OPEN_SCORE_BYTES;
    let session_id = SessionId(190);
    let tracker = Arc::new(ServerPathLaneTracker::default());
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (service_commands, mut service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        service.underlay,
        service.path_id,
        service_commands.clone(),
        FlowLane::Throughput,
        mux_limits,
        tracker.clone(),
    );
    let (second_commands, _second_receivers) = reliable_path_command_channels(8);
    let mut second_flow = Some(ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(9),
        second_commands,
        FlowLane::Throughput,
        mux_limits,
        tracker.clone(),
    ));
    assert_eq!(binding.lane_generation_and_active_response_flows().1, 2);

    let (candidate_commands, mut candidate_receivers) = reliable_path_command_channels(1);
    assert_eq!(
        binding.attach(
            candidate.underlay,
            candidate.path_id,
            candidate_commands.clone(),
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));

    let (service_incarnation, candidate_incarnation) = {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        for entry in &mut outputs.entries {
            if entry.key == service || entry.key == candidate {
                mark_test_response_output_bulk_proven(entry, mux_limits);
            }
        }
        let service_incarnation = outputs
            .entries
            .iter()
            .find(|entry| entry.key == service)
            .expect("service output")
            .incarnation;
        let candidate_incarnation = outputs
            .entries
            .iter()
            .find(|entry| entry.key == candidate)
            .expect("candidate output")
            .incarnation;
        outputs.ack_clock_calibrations.insert(
            (candidate, candidate_incarnation),
            ResponseAckClockCalibrationState::new(
                reliable_ack_clock_calibration_limit_bytes(mux_limits),
                reliable_ack_clock_calibration_ceiling_bytes(mux_limits),
            ),
        );
        (service_incarnation, candidate_incarnation)
    };
    assert_eq!(
        binding
            .commit_subflow_owner_admission(
                service,
                payload_bytes,
                0,
                Duration::ZERO,
                SubflowAdmissionInput {
                    key: candidate,
                    bulk_rate_proven: true,
                    startup_owner_allowed: false,
                    frontier_clear: true,
                    completion_improves: true,
                    observed_goodput_non_degrading: true,
                    read_gap: Duration::ZERO,
                    owner_bytes: payload_bytes,
                    optional_overhead_bytes: 0,
                },
            )
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
    let target = binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.observation.key == candidate)
        .expect("candidate target");
    let request_for = |binding: &ResponseStreamBinding| {
        let targets = binding.sender_path_targets(FlowLane::Throughput, payload_bytes);
        let pending_bytes = |key| {
            targets
                .iter()
                .find(|target| target.observation.key == key)
                .expect("calibration target remains attached")
                .observation
                .command_pending_bytes
        };
        let (expected_planner_generation, _) = binding.subflow_state_snapshot();
        ResponseAckClockCalibrationRequest {
            expected_planner_generation,
            expected_lane_generation: binding.lane_generation(),
            expected_model_generation: binding.response_model_generation(),
            service,
            service_incarnation,
            service_pending_bytes: pending_bytes(service),
            target_pending_bytes: pending_bytes(candidate),
            limit_bytes: reliable_ack_clock_calibration_limit_bytes(mux_limits),
            requires_active_response_start: true,
        }
    };
    let frame = stream_data_frame(payload_bytes);

    let stale_model = request_for(&binding);
    binding.set_output_product_model_for_test(candidate, 500_000_000.0, 10.0);
    assert!(matches!(
        binding.try_enqueue_owner_frame_for_target(
            &target,
            &frame,
            FlowLane::Throughput,
            ResponseOwnerEnqueueAdmission::AckClockCalibration(stale_model),
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));

    let stale = request_for(&binding);
    binding.invalidate_subflow_plan();
    assert!(matches!(
        binding.try_enqueue_owner_frame_for_target(
            &target,
            &frame,
            FlowLane::Throughput,
            ResponseOwnerEnqueueAdmission::AckClockCalibration(stale),
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));

    let stale_lane = request_for(&binding);
    drop(second_flow.take());
    assert_eq!(binding.lane_generation_and_active_response_flows().1, 1);
    assert!(matches!(
        binding.try_enqueue_owner_frame_for_target(
            &target,
            &frame,
            FlowLane::Throughput,
            ResponseOwnerEnqueueAdmission::AckClockCalibration(stale_lane),
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
    second_flow = Some(ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(9),
        replacement_commands,
        FlowLane::Throughput,
        mux_limits,
        tracker,
    ));
    assert_eq!(binding.lane_generation_and_active_response_flows().1, 2);

    let stale_stage = request_for(&binding);
    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let calibration = outputs
            .ack_clock_calibrations
            .get_mut(&(candidate, candidate_incarnation))
            .expect("candidate calibration state");
        calibration.spent_bytes = calibration.credit_limit_bytes;
        let stage_authorized_at = calibration.stage_authorized_at;
        let sample =
            test_ack_clock_rate_sample(calibration.stage_rate_coverage_floor_bytes, 10_000_000.0);
        assert!(calibration.record_ack_clock_sample(
            sample,
            stage_authorized_at,
            stage_authorized_at + Duration::from_millis(1),
        ));
    }
    assert!(matches!(
        binding.try_enqueue_owner_frame_for_target(
            &target,
            &frame,
            FlowLane::Throughput,
            ResponseOwnerEnqueueAdmission::AckClockCalibration(stale_stage),
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        outputs.ack_clock_calibrations.insert(
            (candidate, candidate_incarnation),
            ResponseAckClockCalibrationState::new(
                reliable_ack_clock_calibration_limit_bytes(mux_limits),
                reliable_ack_clock_calibration_ceiling_bytes(mux_limits),
            ),
        );
    }

    let stale_target_pending = request_for(&binding);
    candidate_commands
        .try_enqueue_stream_ordered_frame(
            stream_data_frame_at(payload_bytes as u64, payload_bytes),
            FlowLane::Throughput,
        )
        .expect("change candidate pending bytes");
    let candidate_pending_command = try_recv_reliable_path_command(&mut candidate_receivers)
        .expect("drain candidate queue without releasing pending bytes");
    let candidate_pending_bytes = reliable_path_command_pending_bytes(&candidate_pending_command);
    assert!(matches!(
        binding.try_enqueue_owner_frame_for_target(
            &target,
            &frame,
            FlowLane::Throughput,
            ResponseOwnerEnqueueAdmission::AckClockCalibration(stale_target_pending),
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    candidate_receivers.release_pending_command_bytes(candidate_pending_bytes);

    let stale_service_pending = request_for(&binding);
    service_commands
        .try_enqueue_stream_ordered_frame(
            stream_data_frame_at(payload_bytes as u64, payload_bytes),
            FlowLane::Throughput,
        )
        .expect("change service pending bytes");
    let service_pending_command = try_recv_reliable_path_command(&mut service_receivers)
        .expect("drain service queue without releasing pending bytes");
    let service_pending_bytes = reliable_path_command_pending_bytes(&service_pending_command);
    assert!(matches!(
        binding.try_enqueue_owner_frame_for_target(
            &target,
            &frame,
            FlowLane::Throughput,
            ResponseOwnerEnqueueAdmission::AckClockCalibration(stale_service_pending),
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    service_receivers.release_pending_command_bytes(service_pending_bytes);

    candidate_commands
        .try_enqueue_stream_ordered_frame(
            stream_data_frame_at(payload_bytes as u64, payload_bytes),
            FlowLane::Throughput,
        )
        .expect("fill candidate queue");
    let fresh = request_for(&binding);
    assert!(matches!(
        binding.try_enqueue_owner_frame_for_target(
            &target,
            &frame,
            FlowLane::Throughput,
            ResponseOwnerEnqueueAdmission::AckClockCalibration(fresh),
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        assert_eq!(
            outputs
                .ack_clock_calibrations
                .get(&(candidate, candidate_incarnation))
                .expect("candidate calibration state")
                .spent_bytes,
            0,
            "blocked enqueue restores cumulative calibration credit"
        );
        assert_eq!(outputs.active_ack_clock_calibration, None);
    }
    assert!(try_recv_reliable_path_command(&mut candidate_receivers).is_some());

    binding
        .try_enqueue_owner_frame_for_target(
            &target,
            &frame,
            FlowLane::Throughput,
            ResponseOwnerEnqueueAdmission::AckClockCalibration(request_for(&binding)),
        )
        .expect("fresh exact calibration reservation enqueues");
    {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        assert_eq!(
            outputs
                .ack_clock_calibrations
                .get(&(candidate, candidate_incarnation))
                .expect("candidate calibration state")
                .spent_bytes,
            payload_bytes as u64
        );
        assert_eq!(
            outputs.active_ack_clock_calibration,
            Some((candidate, candidate_incarnation))
        );
    }

    let detached = request_for(&binding);
    binding.detach(candidate, &candidate_commands);
    assert!(matches!(
        binding.try_enqueue_owner_frame_for_target(
            &target,
            &frame,
            FlowLane::Throughput,
            ResponseOwnerEnqueueAdmission::AckClockCalibration(detached),
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(
        binding
            .outputs
            .lock()
            .expect("test response outputs lock")
            .ack_clock_calibrations
            .get(&(candidate, candidate_incarnation))
            .is_none(),
        "detach removes exact-incarnation calibration state"
    );
    drop(second_flow);
}

#[test]
fn subflow_reservation_and_enqueue_linearize_before_topology_reset() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let unrelated = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(2),
    };
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(91),
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let (candidate_commands, mut candidate_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            candidate.underlay,
            candidate.path_id,
            candidate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
    let (unrelated_commands, mut unrelated_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            unrelated.underlay,
            unrelated.path_id,
            unrelated_commands.clone(),
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut unrelated_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));

    let target = binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.observation.key == candidate)
        .expect("candidate output is attached");
    let (planner_generation, _) = binding.subflow_state_snapshot();
    let request = ResponseSubflowAdmissionRequest {
        expected_planner_generation: planner_generation,
        expected_lane_generation: binding.lane_generation(),
        service,
        startup_owner_credit_bytes: payload_bytes,
        optional_overhead_budget_bytes: 0,
        max_read_gap_budget: Duration::ZERO,
        input: SubflowAdmissionInput {
            key: candidate,
            bulk_rate_proven: false,
            startup_owner_allowed: true,
            frontier_clear: true,
            completion_improves: false,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: payload_bytes,
            optional_overhead_bytes: 0,
        },
    };
    let frame = stream_data_frame(payload_bytes);
    let frame_for_sender = frame.clone();
    let binding_for_sender = binding.clone();
    let (reserved_tx, reserved_rx) = std_mpsc::sync_channel(0);
    let (resume_tx, resume_rx) = std_mpsc::sync_channel(0);
    let sender = std::thread::spawn(move || {
        binding_for_sender.try_enqueue_owner_frame_for_target_inner(
            &ResponseDispatchTarget::from(&target),
            &frame_for_sender,
            FlowLane::Throughput,
            ResponseOwnerEnqueueAdmission::SubflowAdmission(request),
            || {
                reserved_tx
                    .send(())
                    .expect("reservation observer remains live");
                resume_rx.recv().expect("reservation test resumes enqueue");
            },
        )
    });
    reserved_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("Subflow reservation reaches the pre-enqueue barrier");

    let outputs_locked_across_reservation = matches!(
        binding.outputs.try_lock(),
        Err(std::sync::TryLockError::WouldBlock)
    );
    let binding_for_detach = binding.clone();
    let (detach_started_tx, detach_started_rx) = std_mpsc::sync_channel(0);
    let (detach_done_tx, detach_done_rx) = std_mpsc::channel();
    let detacher = std::thread::spawn(move || {
        detach_started_tx
            .send(())
            .expect("detach observer remains live");
        binding_for_detach.detach(unrelated, &unrelated_commands);
        detach_done_tx
            .send(())
            .expect("detach completion observer remains live");
    });
    detach_started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("detach attempt starts while enqueue is paused");
    let generation_while_paused = binding.subflow_state_snapshot().0;
    let detach_completed_while_paused = detach_done_rx
        .recv_timeout(Duration::from_millis(50))
        .is_ok();

    resume_tx
        .send(())
        .expect("paused reservation remains ready to enqueue");
    let reservation_epoch = sender
        .join()
        .expect("sender thread does not panic")
        .expect("generation-fenced reservation enqueues");
    detacher.join().expect("detach thread does not panic");

    assert!(
        outputs_locked_across_reservation,
        "outputs must remain locked from Subflow reservation through owner enqueue"
    );
    assert_eq!(generation_while_paused, planner_generation);
    assert!(
        !detach_completed_while_paused,
        "topology reset must not linearize between reservation and enqueue"
    );
    assert!(reservation_epoch.is_some());
    assert_ne!(binding.subflow_state_snapshot().0, planner_generation);
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert_eq!(
        binding.owner_flight_keys_overlapping_frame(&frame),
        vec![candidate],
        "owner flight must be recorded before the topology reset"
    );
}
