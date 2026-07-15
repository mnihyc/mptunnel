use super::super::admission::*;
use super::super::dispatch::*;
use super::super::planner::*;
use super::super::tcp_capacity::response_ack_clock_calibration_blocks_generic_owner;
use super::super::test_support::response_target;
use super::*;
use crate::config::MppPerformanceConfig;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::*;
use crate::model::ack_clock::*;
use crate::model::admission::*;
use crate::model::capacity::*;
use crate::model::multipath::*;
use crate::model::path::*;
use crate::model::response::*;
use crate::mux::MuxLimits;
use crate::protocol::*;
use crate::runtime::path::commands::*;
use crate::runtime::sender::*;
use crate::runtime::stream::response::*;
use crate::runtime::stream::*;
use crate::scheduler::*;
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

struct ResponseServiceHandoffDrainFixture {
    binding: Arc<ResponseStreamBinding>,
    other_binding: Arc<ResponseStreamBinding>,
    stream: ReliablePathStream,
    other_stream: ReliablePathStream,
    service: CarrierPathKey,
    target: CarrierPathKey,
    other_service: CarrierPathKey,
    _service_receivers: ReliablePathCommandReceivers,
    target_receivers: ReliablePathCommandReceivers,
    _other_service_receivers: ReliablePathCommandReceivers,
}

fn response_service_handoff_drain_fixture() -> ResponseServiceHandoffDrainFixture {
    response_service_handoff_drain_fixture_with_other_service(UnderlayProtocol::Tcp)
}

fn response_service_handoff_drain_fixture_with_other_service(
    other_underlay: UnderlayProtocol,
) -> ResponseServiceHandoffDrainFixture {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let session_id = SessionId(192);
    let tracker = Arc::new(ServerPathLaneTracker::default());
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let target = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let other_service = CarrierPathKey {
        underlay: other_underlay,
        path_id: if other_underlay == UnderlayProtocol::Udp {
            target.path_id
        } else {
            PathId(2)
        },
    };
    let (service_commands, service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
        mux_limits,
        tracker.clone(),
    );
    let (other_service_commands, other_service_receivers) = reliable_path_command_channels(8);
    let other_binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        other_service.underlay,
        other_service.path_id,
        other_service_commands,
        FlowLane::Throughput,
        mux_limits,
        tracker,
    );
    let (target_commands, mut target_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            target.underlay,
            target.path_id,
            target_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut target_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
    binding.mark_output_bulk_proven_for_test(service);
    other_binding.mark_output_bulk_proven_for_test(other_service);
    let sample_bytes = reliable_subflow_startup_sample_limit_bytes(mux_limits);
    binding.update_path_metrics(
        target,
        PathMetrics {
            path_id: target.path_id,
            underlay: target.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 10_000,
            srtt_us: 12_000,
            rttvar_us: 1_000,
            jitter_us: 1_000,
            delivery_rate_bps: 500_000_000,
            pacing_rate_bps: 500_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: sample_bytes,
            inflight_hi_bytes: sample_bytes,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
            data_sample_bytes: sample_bytes,
        },
        ServerPathMetricsSource::LocalSender,
    );
    let expected_family_loads = if other_underlay == UnderlayProtocol::Udp {
        ResponseServiceFamilyLoads::new(1, 1)
    } else {
        ResponseServiceFamilyLoads::new(2, 0)
    };
    assert_eq!(
        binding.response_scheduling_snapshot().service_family_loads,
        expected_family_loads
    );

    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(192),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: service.underlay,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    let (_other_frames_tx, other_frames_rx) = mpsc::channel(1);
    let other_stream = ReliablePathStream {
        stream_id: StreamId(193),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: other_service.underlay,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(other_binding.clone()),
        frames: other_frames_rx,
    };
    ResponseServiceHandoffDrainFixture {
        binding,
        other_binding,
        stream,
        other_stream,
        service,
        target,
        other_service,
        _service_receivers: service_receivers,
        target_receivers,
        _other_service_receivers: other_service_receivers,
    }
}

struct ResponseCalibrationDispatchFixture {
    binding: Arc<ResponseStreamBinding>,
    stream: ReliablePathStream,
    service: CarrierPathKey,
    candidate: CarrierPathKey,
    candidate_commands: ReliablePathCommandSender,
    service_receivers: ReliablePathCommandReceivers,
    candidate_receivers: ReliablePathCommandReceivers,
    second_binding: Option<Arc<ResponseStreamBinding>>,
}

fn response_calibration_dispatch_fixture(
    candidate_capacity: usize,
) -> ResponseCalibrationDispatchFixture {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let session_id = SessionId(191);
    let tracker = Arc::new(ServerPathLaneTracker::default());
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (service_commands, service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
        mux_limits,
        tracker.clone(),
    );
    let (second_commands, _second_receivers) = reliable_path_command_channels(8);
    let second_binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(9),
        second_commands,
        FlowLane::Throughput,
        mux_limits,
        tracker,
    );
    let (candidate_commands, mut candidate_receivers) =
        reliable_path_command_channels(candidate_capacity);
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
    binding.mark_output_bulk_proven_for_test(service);
    binding.mark_output_bulk_proven_for_test(candidate);
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
    let stage_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
    binding.install_tcp_ack_clock_calibration_for_test(
        candidate,
        stage_limit - 4032,
        stage_limit,
        reliable_ack_clock_calibration_ceiling_bytes(mux_limits),
        true,
    );
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(191),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    ResponseCalibrationDispatchFixture {
        binding,
        stream,
        service,
        candidate,
        candidate_commands,
        service_receivers,
        candidate_receivers,
        second_binding: Some(second_binding),
    }
}

fn install_slow_fresh_response_calibration(fixture: &ResponseCalibrationDispatchFixture) {
    fixture
        .binding
        .set_output_product_model_for_test(fixture.service, 47_429_000.0, 333.0);
    fixture
        .binding
        .set_output_product_model_for_test(fixture.candidate, 1_342_000.0, 891.787);
    fixture.binding.install_tcp_ack_clock_calibration_for_test(
        fixture.candidate,
        0,
        299_176,
        reliable_ack_clock_calibration_ceiling_bytes(MuxLimits::default()),
        false,
    );
}

fn response_calibration_retirement_request(
    fixture: &ResponseCalibrationDispatchFixture,
) -> ResponseAckClockCalibrationRetirementRequest {
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let targets = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes);
    let service = targets
        .iter()
        .find(|target| target.observation.key == fixture.service)
        .expect("Service target");
    let candidate = targets
        .iter()
        .find(|target| target.observation.key == fixture.candidate)
        .expect("calibration target");
    let (expected_planner_generation, _) = fixture.binding.subflow_state_snapshot();
    let expected_lane_generation = fixture
        .binding
        .lane_generation_and_active_response_flows()
        .0;
    ResponseAckClockCalibrationRetirementRequest {
        expected_planner_generation,
        expected_lane_generation,
        expected_model_generation: fixture.binding.response_model_generation(),
        service: service.observation.key,
        service_incarnation: service.observation.incarnation,
        service_pending_bytes: service.observation.command_pending_bytes,
        target: candidate.observation.key,
        target_incarnation: candidate.observation.incarnation,
        target_pending_bytes: candidate.observation.command_pending_bytes,
        limit_bytes: candidate.ack_clock_calibration_credit_limit_bytes,
    }
}

#[test]
fn tcp_ack_clock_calibration_retirement_releases_binding_fences() {
    let fixture = response_calibration_dispatch_fixture(8);
    install_slow_fresh_response_calibration(&fixture);
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let generation_before = fixture.binding.subflow_state_snapshot().0;

    let plan = plan_response_data_dispatch(&fixture.stream, FlowLane::Throughput, 0, payload_bytes)
        .expect("Service remains available after retiring unsafe exploration");

    assert_eq!(plan.primary_key(), Some(fixture.service));
    assert_ne!(
        fixture.binding.subflow_state_snapshot().0,
        generation_before
    );
    let candidate = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.observation.key == fixture.candidate)
        .expect("retired candidate remains attached");
    assert_eq!(candidate.ack_clock_calibration_spent_bytes, 0);
    assert_eq!(candidate.ack_clock_calibration_credit_limit_bytes, 0);
    assert_eq!(candidate.ack_clock_calibration_max_limit_bytes, 0);
    assert!(!candidate.ack_clock_calibration_active);
    assert!(!response_ack_clock_calibration_blocks_generic_owner(
        &candidate
    ));
}

#[test]
fn tcp_ack_clock_calibration_retirement_ignores_repair_only_carrier_debt() {
    let fixture = response_calibration_dispatch_fixture(1);
    install_slow_fresh_response_calibration(&fixture);
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let repair = Frame::StreamData {
        stream_id: fixture.stream.stream_id,
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from_static(b"repair-only"),
    };
    fixture
        .candidate_commands
        .try_enqueue_stream_ordered_frame(repair.clone(), FlowLane::Throughput)
        .expect("fill the candidate lane with RepairData");
    fixture
        .binding
        .record_repair_flight(fixture.candidate, &repair);

    let plan = plan_response_data_dispatch(
        &fixture.stream,
        FlowLane::Throughput,
        reliable_stream_frame_accounted_bytes(&repair) as u64,
        payload_bytes,
    )
    .expect("RepairData must not preserve a unique-owner calibration fence");

    assert_eq!(plan.primary_key(), Some(fixture.service));
    let candidate = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.observation.key == fixture.candidate)
        .expect("candidate remains attached");
    assert_eq!(candidate.observation.owner_data_in_flight_bytes, 0);
    assert!(candidate.observation.snapshot.product_bytes_in_flight > 0);
    assert_eq!(candidate.ack_clock_calibration_credit_limit_bytes, 0);
    assert_eq!(candidate.ack_clock_calibration_max_limit_bytes, 0);
    assert!(!response_ack_clock_calibration_blocks_generic_owner(
        &candidate
    ));
}

#[test]
fn tcp_ack_clock_calibration_retirement_refuses_exact_owner_flight() {
    let fixture = response_calibration_dispatch_fixture(8);
    install_slow_fresh_response_calibration(&fixture);
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let candidate = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.observation.key == fixture.candidate)
        .expect("fresh calibration candidate");
    fixture.binding.record_owner_flight_for_target(
        &candidate,
        &Frame::StreamData {
            stream_id: fixture.stream.stream_id,
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"stale-owner"),
        },
    );

    let plan = plan_response_data_dispatch(&fixture.stream, FlowLane::Throughput, 0, payload_bytes)
        .expect("stale calibration state must fall back without erasing exact flight");

    assert_eq!(plan.primary_key(), Some(fixture.service));
    let candidate = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.observation.key == fixture.candidate)
        .expect("candidate remains attached");
    assert!(candidate.ack_clock_calibration_credit_limit_bytes > 0);
    assert!(candidate.ack_clock_calibration_max_limit_bytes > 0);
    assert!(response_ack_clock_calibration_blocks_generic_owner(
        &candidate
    ));
}

#[test]
fn tcp_ack_clock_calibration_retirement_rejects_stale_path_model() {
    let fixture = response_calibration_dispatch_fixture(8);
    install_slow_fresh_response_calibration(&fixture);
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let request = response_calibration_retirement_request(&fixture);
    fixture
        .binding
        .set_output_product_model_for_test(fixture.candidate, 500_000_000.0, 10.0);

    assert!(
        !fixture
            .binding
            .try_retire_tcp_ack_clock_calibration(request)
    );
    let candidate = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.observation.key == fixture.candidate)
        .expect("candidate remains attached");
    assert!(candidate.ack_clock_calibration_credit_limit_bytes > 0);
}

#[test]
fn tcp_ack_clock_calibration_retirement_rejects_stale_pending_snapshots() {
    let mut fixture = response_calibration_dispatch_fixture(8);
    install_slow_fresh_response_calibration(&fixture);
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());

    let stale_candidate = response_calibration_retirement_request(&fixture);
    fixture
        .candidate_commands
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: fixture.stream.stream_id,
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"candidate-pending"),
            },
            FlowLane::Throughput,
        )
        .expect("change candidate pending bytes");
    let candidate_command = try_recv_reliable_path_command(&mut fixture.candidate_receivers)
        .expect("drain candidate queue without releasing pending bytes");
    let candidate_pending_bytes = reliable_path_command_pending_bytes(&candidate_command);
    assert!(
        !fixture
            .binding
            .try_retire_tcp_ack_clock_calibration(stale_candidate)
    );
    fixture
        .candidate_receivers
        .release_pending_command_bytes(candidate_pending_bytes);

    let stale_service = response_calibration_retirement_request(&fixture);
    let service = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.observation.key == fixture.service)
        .expect("Service target");
    service
        .commands
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: fixture.stream.stream_id,
                offset: payload_bytes as u64,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"service-pending"),
            },
            FlowLane::Throughput,
        )
        .expect("change Service pending bytes");
    let service_command = try_recv_reliable_path_command(&mut fixture.service_receivers)
        .expect("drain Service queue without releasing pending bytes");
    let service_pending_bytes = reliable_path_command_pending_bytes(&service_command);
    assert!(
        !fixture
            .binding
            .try_retire_tcp_ack_clock_calibration(stale_service)
    );
    fixture
        .service_receivers
        .release_pending_command_bytes(service_pending_bytes);

    assert!(
        fixture.binding.try_retire_tcp_ack_clock_calibration(
            response_calibration_retirement_request(&fixture)
        )
    );
}

#[tokio::test]
async fn tcp_response_calibration_dispatch_restores_credit_after_exact_remainder() {
    let mux_limits = MuxLimits::default();
    let normal_payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut fixture = response_calibration_dispatch_fixture(8);
    let mut sender = ServerResponseSenderService::new(SessionId(191), fixture.stream.stream_id);
    sender.enqueue_data_for_lane(
        Bytes::from(vec![0x5a; normal_payload_bytes]),
        FlowLane::Throughput,
    );
    let mut send_stream = ReliableSendStream::new_with_initial_max_offset(
        fixture.stream.stream_id,
        mux_limits,
        u64::MAX,
    );

    let dispatch = sender
        .dispatch_next(
            &fixture.stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
        )
        .expect("the exact residual remains spendable");

    assert_eq!(dispatch.selected_path, Some(fixture.candidate));
    assert_eq!(dispatch.payload_bytes, 4032);
    assert_eq!(fixture.binding.ordered_data_owner(), Some(fixture.service));
    assert!(try_recv_reliable_path_command(&mut fixture.service_receivers).is_none());
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.candidate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
            if payload.len() == 4032
    ));
    let target = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, normal_payload_bytes)
        .into_iter()
        .find(|target| target.observation.key == fixture.candidate)
        .expect("calibration target");
    assert_eq!(
        target.ack_clock_calibration_spent_bytes,
        target.ack_clock_calibration_credit_limit_bytes
    );
    assert_eq!(sender.data_bytes(), normal_payload_bytes - 4032);

    fixture
        .binding
        .release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: 4032,
        }]);
    let drained = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, normal_payload_bytes)
        .into_iter()
        .find(|target| target.observation.key == fixture.candidate)
        .expect("drained calibration target");
    assert!(drained.ack_clock_calibration_active);
    assert!(
        drained.ack_clock_calibration_credit_limit_bytes
            > drained.ack_clock_calibration_spent_bytes,
        "exact drain restores bounded credit when no representative strict window was reachable"
    );
    assert!(
        drained.ack_clock_calibration_credit_limit_bytes
            <= drained.ack_clock_calibration_max_limit_bytes
    );
}

#[tokio::test]
async fn active_tcp_calibration_continues_after_another_response_flow_closes() {
    let mux_limits = MuxLimits::default();
    let normal_payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut fixture = response_calibration_dispatch_fixture(8);
    drop(fixture.second_binding.take());
    assert_eq!(
        fixture
            .binding
            .lane_generation_and_active_response_flows()
            .1,
        1
    );
    let mut sender = ServerResponseSenderService::new(SessionId(191), fixture.stream.stream_id);
    sender.enqueue_data_for_lane(
        Bytes::from(vec![0x5a; normal_payload_bytes]),
        FlowLane::Throughput,
    );
    let mut send_stream = ReliableSendStream::new_with_initial_max_offset(
        fixture.stream.stream_id,
        mux_limits,
        u64::MAX,
    );

    let dispatch = sender
        .dispatch_next(
            &fixture.stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
        )
        .expect("an exact active calibration may finish after the start gate closes");

    assert_eq!(dispatch.selected_path, Some(fixture.candidate));
    assert_eq!(dispatch.payload_bytes, 4032);
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.candidate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
            if payload.len() == 4032
    ));
}

#[tokio::test]
async fn tcp_response_calibration_dispatch_treats_pending_flight_as_one_debt() {
    let mux_limits = MuxLimits::default();
    let normal_payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let stage_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
    let committed = stage_limit - 4032;
    let mut fixture = response_calibration_dispatch_fixture(8);
    let overlap = Frame::StreamData {
        stream_id: fixture.stream.stream_id,
        offset: normal_payload_bytes as u64,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x5a; committed as usize]),
    };
    fixture
        .binding
        .record_owner_flight(fixture.candidate, &overlap);
    fixture
        .candidate_commands
        .try_enqueue_stream_ordered_frame(overlap, FlowLane::Throughput)
        .expect("mirror the assigned product flight in the carrier queue");

    let mut sender = ServerResponseSenderService::new(SessionId(191), fixture.stream.stream_id);
    sender.enqueue_data_for_lane(
        Bytes::from(vec![0x5a; normal_payload_bytes]),
        FlowLane::Throughput,
    );
    let mut send_stream = ReliableSendStream::new_with_initial_max_offset(
        fixture.stream.stream_id,
        mux_limits,
        u64::MAX,
    );
    let dispatch = sender
        .dispatch_next(
            &fixture.stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
        )
        .expect("overlapping ledger and queue views leave the residual spendable");

    assert_eq!(dispatch.selected_path, Some(fixture.candidate));
    assert_eq!(dispatch.payload_bytes, 4032);
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.candidate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
            if payload.len() == committed as usize
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.candidate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
            if payload.len() == 4032
    ));
    let target = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, normal_payload_bytes)
        .into_iter()
        .find(|target| target.observation.key == fixture.candidate)
        .expect("calibration target");
    assert_eq!(
        target.ack_clock_calibration_spent_bytes,
        target.ack_clock_calibration_credit_limit_bytes
    );
}

#[tokio::test]
async fn blocked_tcp_calibration_remainder_keeps_normal_service_chunk() {
    let mux_limits = MuxLimits::default();
    let normal_payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut fixture = response_calibration_dispatch_fixture(1);
    fixture
        .candidate_commands
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: fixture.stream.stream_id,
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"blocked"),
            },
            FlowLane::Throughput,
        )
        .expect("fill exact calibration candidate queue");
    let mut sender = ServerResponseSenderService::new(SessionId(191), fixture.stream.stream_id);
    sender.enqueue_data_for_lane(
        Bytes::from(vec![0x5a; normal_payload_bytes]),
        FlowLane::Throughput,
    );
    let mut send_stream = ReliableSendStream::new_with_initial_max_offset(
        fixture.stream.stream_id,
        mux_limits,
        u64::MAX,
    );

    let dispatch = sender
        .dispatch_next(
            &fixture.stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
        )
        .expect("blocked calibration falls back to normal Service emission");

    assert_eq!(dispatch.selected_path, Some(fixture.service));
    assert_eq!(dispatch.payload_bytes, normal_payload_bytes);
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.service_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
            if payload.len() == normal_payload_bytes
    ));
    assert_eq!(
        fixture
            .binding
            .active_tcp_ack_clock_calibration_remaining_bytes(),
        Some(4032),
        "Service fallback must not spend or repeatedly fragment the candidate's residual credit"
    );
}

fn client_data_frame_for_test(stream_id: StreamId, offset: u64, payload_bytes: usize) -> Frame {
    Frame::StreamData {
        stream_id,
        offset,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x5a; payload_bytes]),
    }
}

#[test]
fn measured_cross_family_path_handoff_allows_diversification_or_two_x_gain() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    service.observation.snapshot.active_flows = 2;
    let udp = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

    let selected = select_response_service_handoff_target(
        &[service.clone(), udp.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        0,
        ResponseServiceFamilyLoads::new(2, 0),
        4096,
        None,
    )
    .expect("measured underloaded family should receive one whole flow");
    assert_eq!(selected.target.observation.key, udp.observation.key);
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);
    assert_eq!(
        selected
            .service_handoff_commit()
            .map(|commit| commit.handoff_frontier),
        Some(4096)
    );

    assert!(
        select_response_service_handoff_target(
            &[service.clone(), udp.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.observation.key),
            1,
            ResponseServiceFamilyLoads::new(2, 0),
            4096,
            None,
        )
        .is_none(),
        "any unresolved product tail blocks carrier-family handoff"
    );
    let balanced_gain = select_response_service_handoff_target(
        &[service, udp],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        }),
        0,
        ResponseServiceFamilyLoads::new(1, 1),
        4096,
        None,
    )
    .expect("a balanced family may still move one flow for a two-fold projected gain");
    assert_eq!(balanced_gain.admission().role, PathRuntimeRole::Service);
}

#[test]
fn busy_shared_target_carrier_is_pressure_not_binding_debt() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    service.observation.snapshot.rate_scope = PathRateScope::PerFlowGoodput;
    service.observation.snapshot.delivery_rate_bps = 1_000_000.0;
    let mut udp = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let (udp_commands, _udp_receivers) = reliable_path_command_channels(8);
    udp.commands = udp_commands;
    udp.observation.snapshot.delivery_rate_bps = 100_000_000.0;
    udp.observation.snapshot.active_flows = 1;
    udp.commands
        .try_enqueue_stream_ordered_frame(
            client_data_frame_for_test(StreamId(999), 0, 1),
            FlowLane::Throughput,
        )
        .expect("shared target carrier accepts unrelated queued work");
    udp.observation.command_pending_bytes = udp.commands.pending_bytes();
    udp.observation.snapshot.queue_bytes = udp.observation.command_pending_bytes;
    udp.observation.snapshot.bytes_in_flight = 1;
    assert!(udp.observation.command_pending_bytes > 0);
    assert_eq!(udp.observation.owner_data_in_flight_bytes, 0);
    assert_eq!(udp.observation.snapshot.product_bytes_in_flight, 0);

    let selected = select_response_service_handoff_target(
        &[service.clone(), udp.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        0,
        ResponseServiceFamilyLoads::new(1, 1),
        4096,
        None,
    )
    .expect("another binding's carrier pressure must not masquerade as this binding's debt");
    assert_eq!(selected.target.observation.key, udp.observation.key);
}

#[test]
fn response_service_handoff_drain_blocks_only_its_own_binding() {
    let fixture = response_service_handoff_drain_fixture();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());

    assert!(matches!(
        plan_response_data_dispatch_with_ordered_debt_impl(
            &fixture.stream,
            FlowLane::Throughput,
            payload_bytes as u64,
            payload_bytes,
            payload_bytes,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    let mut sender = ServerResponseSenderService::new(SessionId(191), fixture.stream.stream_id);
    sender.enqueue_data_for_lane(Bytes::from(vec![0x5a; payload_bytes]), FlowLane::Throughput);
    assert!(
        sender.drain_allows_bounded_source_staging(&fixture.stream, true),
        "a drain blocks offset assignment, not bounded raw target read-ahead"
    );
    let reservation = fixture
        .binding
        .response_scheduling_snapshot()
        .response_service_handoff_drain
        .expect("the eligible flow should own the session drain intent");
    assert_eq!(
        reservation.binding_instance_id,
        fixture.binding.binding_instance_id()
    );
    assert_eq!(reservation.service, fixture.service);
    assert_eq!(reservation.target, fixture.target);
    assert!(matches!(
        plan_response_data_dispatch_with_ordered_debt_impl(
            &fixture.stream,
            FlowLane::Throughput,
            payload_bytes as u64,
            payload_bytes,
            payload_bytes,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));

    let other_plan = plan_response_data_dispatch(
        &fixture.other_stream,
        FlowLane::Throughput,
        0,
        payload_bytes,
    )
    .expect("another binding must keep planning ordinary OwnerData");
    assert_eq!(other_plan.primary_key(), Some(fixture.other_service));
    assert_eq!(
        fixture.other_binding.ordered_data_owner(),
        Some(fixture.other_service),
        "a session drain is serialization for handoff, not a session-wide data pause"
    );
}

#[test]
fn response_readiness_preview_does_not_start_handoff_drain() {
    let fixture = response_service_handoff_drain_fixture();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let before = fixture.binding.response_scheduling_snapshot();
    assert!(before.response_service_handoff_drain.is_none());

    for _ in 0..2 {
        assert!(preview_response_data_payload_with_ordered_debt(
            &fixture.stream,
            FlowLane::Throughput,
            payload_bytes as u64,
            payload_bytes,
            payload_bytes,
        ));
    }

    let after_preview = fixture.binding.response_scheduling_snapshot();
    assert_eq!(after_preview.generation, before.generation);
    assert!(after_preview.response_service_handoff_drain.is_none());
    assert!(matches!(
        plan_response_data_dispatch_with_ordered_debt_impl(
            &fixture.stream,
            FlowLane::Throughput,
            payload_bytes as u64,
            payload_bytes,
            payload_bytes,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(
        fixture
            .binding
            .response_scheduling_snapshot()
            .response_service_handoff_drain
            .is_some()
    );
}

#[tokio::test]
async fn response_service_handoff_drain_holds_raw_offset_until_frontier_commit() {
    let mut fixture = response_service_handoff_drain_fixture();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let frontier = payload_bytes as u64;
    let service_target = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.observation.key == fixture.service)
        .expect("TCP Service target");
    let old_owner_frame = Frame::StreamData {
        stream_id: fixture.stream.stream_id,
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x61; payload_bytes]),
    };
    fixture
        .binding
        .record_owner_flight_for_target(&service_target, &old_owner_frame);

    for _ in 0..2 {
        assert!(matches!(
            plan_response_data_dispatch_with_ordered_debt_impl(
                &fixture.stream,
                FlowLane::Throughput,
                frontier,
                payload_bytes,
                payload_bytes,
            ),
            Err(RuntimeError::SenderServiceBlocked)
        ));
    }
    assert_eq!(fixture.binding.ordered_data_owner(), Some(fixture.service));
    assert!(
        try_recv_reliable_path_command(&mut fixture.target_receivers).is_none(),
        "the paused raw payload must not consume its offset or enter the target queue"
    );

    fixture.binding.release_normalized_acked_ranges(&[
        OffsetRange::new(0, frontier).expect("old owner ACK range")
    ]);
    let plan = plan_response_data_dispatch_with_ordered_debt_impl(
        &fixture.stream,
        FlowLane::Throughput,
        frontier,
        payload_bytes,
        0,
    )
    .expect("the identical raw offset should become the clear-frontier handoff frame");
    assert_eq!(plan.primary_key(), Some(fixture.target));
    assert!(matches!(
        &plan.primary,
        ResponseDataDispatchTarget::Switchable {
            intent: ResponseDataDispatchIntent::ServiceHandoff(commit),
            ..
        } if commit.handoff_frontier == frontier
    ));

    let handoff_frame = Frame::StreamData {
        stream_id: fixture.stream.stream_id,
        offset: frontier,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x62; payload_bytes]),
    };
    let outcome = emit_planned_response_data_frame(
        &fixture.stream,
        plan,
        handoff_frame,
        FlowLane::Throughput,
    )
    .await
    .expect("the first post-drain raw payload should atomically move Service");
    assert_eq!(outcome.selected_path, Some(fixture.target));
    assert_eq!(fixture.binding.ordered_data_owner(), Some(fixture.target));
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.target_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { offset, .. }))
            if offset == frontier
    ));
}

#[tokio::test]
async fn balanced_performance_override_commits_full_handoff_transaction() {
    let mut fixture =
        response_service_handoff_drain_fixture_with_other_service(UnderlayProtocol::Udp);
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let frontier = payload_bytes as u64;
    fixture
        .binding
        .set_output_product_model_for_test(fixture.service, 1_000_000.0, 20.0);
    let service_target = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.observation.key == fixture.service)
        .expect("slow TCP Service target");
    fixture.binding.record_owner_flight_for_target(
        &service_target,
        &client_data_frame_for_test(fixture.stream.stream_id, 0, payload_bytes),
    );

    assert!(matches!(
        plan_response_data_dispatch_with_ordered_debt_impl(
            &fixture.stream,
            FlowLane::Throughput,
            frontier,
            payload_bytes,
            payload_bytes,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    fixture.binding.release_normalized_acked_ranges(&[
        OffsetRange::new(0, frontier).expect("old owner ACK range")
    ]);
    let plan = plan_response_data_dispatch_with_ordered_debt_impl(
        &fixture.stream,
        FlowLane::Throughput,
        frontier,
        payload_bytes,
        0,
    )
    .expect("balanced slow TCP should move to the measured fast QUIC carrier");
    assert!(matches!(
        &plan.primary,
        ResponseDataDispatchTarget::Switchable {
            intent: ResponseDataDispatchIntent::ServiceHandoff(commit),
            ..
        } if commit.mode == ResponseServiceHandoffMode::PerformanceOverride
    ));

    let outcome = emit_planned_response_data_frame(
        &fixture.stream,
        plan,
        client_data_frame_for_test(fixture.stream.stream_id, frontier, payload_bytes),
        FlowLane::Throughput,
    )
    .await
    .expect("the balanced performance override should commit atomically");
    assert_eq!(outcome.selected_path, Some(fixture.target));
    assert_eq!(fixture.binding.ordered_data_owner(), Some(fixture.target));
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.target_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { offset, .. }))
            if offset == frontier
    ));
}

#[tokio::test]
async fn handoff_commit_rejects_shared_queue_growth_beyond_ranked_credit() {
    let fixture = response_service_handoff_drain_fixture();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let frontier = payload_bytes as u64;
    let service_target = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.observation.key == fixture.service)
        .expect("TCP Service target");
    fixture.binding.record_owner_flight_for_target(
        &service_target,
        &client_data_frame_for_test(fixture.stream.stream_id, 0, payload_bytes),
    );
    assert!(matches!(
        plan_response_data_dispatch_with_ordered_debt_impl(
            &fixture.stream,
            FlowLane::Throughput,
            frontier,
            payload_bytes,
            payload_bytes,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    fixture.binding.release_normalized_acked_ranges(&[
        OffsetRange::new(0, frontier).expect("old owner ACK range")
    ]);
    let plan = plan_response_data_dispatch_with_ordered_debt_impl(
        &fixture.stream,
        FlowLane::Throughput,
        frontier,
        payload_bytes,
        0,
    )
    .expect("clear frontier should produce a bounded handoff commit");
    let (target_commands, pending_limit) = match &plan.primary {
        ResponseDataDispatchTarget::Switchable {
            target,
            intent: ResponseDataDispatchIntent::ServiceHandoff(commit),
            ..
        } => (
            target.commands.clone(),
            commit.target_command_pending_limit_bytes,
        ),
        _ => panic!("expected switchable handoff plan"),
    };
    let excess_bytes =
        usize::try_from(pending_limit.saturating_add(1)).expect("test credit fits process memory");
    target_commands
        .try_enqueue_stream_ordered_frame(
            client_data_frame_for_test(StreamId(999), 0, excess_bytes),
            FlowLane::Throughput,
        )
        .expect("unrelated shared work races with the planned commit");

    let result = emit_planned_response_data_frame(
        &fixture.stream,
        plan,
        client_data_frame_for_test(fixture.stream.stream_id, frontier, payload_bytes),
        FlowLane::Throughput,
    )
    .await;
    assert!(matches!(result, Err(RuntimeError::SenderServiceBlocked)));
    assert_eq!(fixture.binding.ordered_data_owner(), Some(fixture.service));
    assert!(
        fixture
            .binding
            .response_scheduling_snapshot()
            .response_service_handoff_drain
            .is_none(),
        "a credit-regressed transaction must release its session reservation"
    );
}

#[test]
fn service_handoff_rejects_lower_projected_fair_share() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    service.observation.snapshot.active_flows = 1;
    service.observation.snapshot.delivery_rate_bps = 500_000_000.0;
    let mut udp = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    udp.observation.snapshot.delivery_rate_bps = 100_000_000.0;

    assert!(
        select_response_service_handoff_target(
            &[service.clone(), udp],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.observation.key),
            0,
            ResponseServiceFamilyLoads::new(2, 0),
            4096,
            None,
        )
        .is_none(),
        "low RTT cannot justify a sticky move to a much slower carrier"
    );
}

#[test]
fn generic_evidence_drain_clears_unpinned_expired_receipt_marker() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    service.observation.snapshot.delivery_rate_bps = 1_000_000.0;
    let mut target = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let now = Instant::now();
    let accepted_at = now
        .checked_sub(Duration::from_secs(2))
        .expect("test clock supports short subtraction");
    target.quic_capacity_proof = Some(QuicCapacityProofCandidate {
        token: 8,
        train_bytes: 1024,
        sample_floor_bytes: 1024,
        accounting_slack_bytes: 128,
        warmup_bytes: 128,
        required_proof_bytes: 896,
        written_bytes: 1024,
        written_data_frame_count: 1,
        receipt_confirmed: true,
        received_bytes: 1024,
        proof_elapsed: Duration::from_millis(1),
        rate_bps: 8_192_000,
        accepted_at,
        expires_at: accepted_at + Duration::from_secs(1),
        proof_validity: Duration::from_secs(1),
    });
    target.observation.has_bulk_rate_evidence = true;
    let reservation = ResponseServiceHandoffDrainReservation {
        binding_instance_id: 8,
        service: service.observation.key,
        service_path_instance_id: service.observation.path_instance_id,
        service_incarnation: service.observation.incarnation,
        target: target.observation.key,
        target_path_instance_id: target.observation.path_instance_id,
        target_incarnation: target.observation.incarnation,
        capacity_proof: None,
        expires_at: now + Duration::from_secs(1),
    };

    let effective = response_service_handoff_target_view(
        &target,
        service.observation.key,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        Some(reservation),
        now,
    )
    .expect("the exact generic-evidence drain target");
    assert!(effective.observation.has_bulk_rate_evidence);
    assert_eq!(effective.quic_capacity_proof, None);
    assert!(response_service_handoff_drain_matches_candidate(
        reservation.binding_instance_id,
        reservation,
        &ResponseServiceHandoffCandidate {
            service,
            target: effective,
            mode: ResponseServiceHandoffMode::Diversification,
        },
    ));
}

#[cfg(feature = "lab-diagnostics")]
#[test]
fn service_handoff_diagnostic_distinguishes_frontier_and_expired_receipt() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    service.observation.snapshot.delivery_rate_bps = 1_000_000.0;
    let mut udp = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let now = Instant::now();
    let accepted_at = now
        .checked_sub(Duration::from_secs(2))
        .expect("test clock supports short subtraction");
    udp.quic_capacity_proof = Some(QuicCapacityProofCandidate {
        token: 7,
        train_bytes: 1024,
        sample_floor_bytes: 1024,
        accounting_slack_bytes: 128,
        warmup_bytes: 128,
        required_proof_bytes: 896,
        written_bytes: 1024,
        written_data_frame_count: 1,
        receipt_confirmed: true,
        received_bytes: 1024,
        proof_elapsed: Duration::from_millis(1),
        rate_bps: 8_192_000,
        accepted_at,
        expires_at: accepted_at + Duration::from_secs(1),
        proof_validity: Duration::from_secs(1),
    });
    udp.observation.has_bulk_rate_evidence = false;
    let targets = [service.clone(), udp.clone()];
    let expired = response_service_handoff_diagnostics::evaluate_response_service_handoff(
        &targets,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        0,
        ResponseServiceFamilyLoads::new(2, 0),
        None,
        true,
        false,
        false,
        false,
        now,
    );
    assert_eq!(expired.first_failed_gate, "target_proof_expired");

    let proof = udp.quic_capacity_proof.expect("raw expired marker");
    let reservation = ResponseServiceHandoffDrainReservation {
        binding_instance_id: 7,
        service: service.observation.key,
        service_path_instance_id: service.observation.path_instance_id,
        service_incarnation: service.observation.incarnation,
        target: udp.observation.key,
        target_path_instance_id: udp.observation.path_instance_id,
        target_incarnation: udp.observation.incarnation,
        capacity_proof: Some(proof),
        expires_at: now + Duration::from_secs(1),
    };
    let effective =
        response_service_handoff_diagnostics::response_service_handoff_diagnostic_target_view(
            &service,
            &udp,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            Some(reservation),
            now,
        )
        .expect("diagnostic must retain the exact bounded proof view");
    assert!(
        !udp.observation.has_bulk_rate_evidence,
        "raw marker is expired"
    );
    assert!(
        effective.observation.has_bulk_rate_evidence,
        "pinned view remains authoritative"
    );
    assert_eq!(effective.quic_capacity_proof, Some(proof));
    assert_eq!(
        effective.observation.snapshot.delivery_rate_bps,
        proof.rate_bps as f64
    );
    assert_eq!(
        effective.observation.snapshot.rate_scope,
        PathRateScope::PathCapacity,
        "the pinned QUIC receipt rate and its capacity scope are one snapshot authority"
    );
    assert!(response_service_handoff_preserves_fair_share(
        &service.observation,
        &effective.observation,
    ));

    udp.observation.has_bulk_rate_evidence = true;
    udp.quic_capacity_proof = udp
        .quic_capacity_proof
        .map(|proof| QuicCapacityProofCandidate {
            accepted_at: now,
            expires_at: now + Duration::from_secs(1),
            ..proof
        });
    let targets = [service.clone(), udp];
    let blocked_frontier = response_service_handoff_diagnostics::evaluate_response_service_handoff(
        &targets,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        payload_bytes,
        ResponseServiceFamilyLoads::new(2, 0),
        None,
        true,
        false,
        false,
        false,
        now,
    );
    assert_eq!(blocked_frontier.first_failed_gate, "frontier_not_clear");
    assert!(response_service_handoff_preserves_fair_share(
        &blocked_frontier
            .service
            .expect("diagnostic Service")
            .observation,
        &blocked_frontier
            .target
            .expect("diagnostic target")
            .observation,
    ));
}

#[cfg(feature = "lab-diagnostics")]
#[test]
fn family_or_gain_diagnostic_ignores_shared_carrier_churn() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(0, UnderlayProtocol::Tcp, 20.0, 0, 16 * 1024 * 1024, true);
    let udp = response_target(1, UnderlayProtocol::Udp, 180.0, 0, 16 * 1024 * 1024, false);
    let mut targets = [service, udp];
    let service_family_loads = ResponseServiceFamilyLoads::new(1, 1);
    let now = Instant::now();
    let signature = |targets: &[ResponseSenderPathTarget]| {
        let evaluation = response_service_handoff_diagnostics::evaluate_response_service_handoff(
            targets,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(targets[0].observation.key),
            0,
            service_family_loads,
            None,
            true,
            false,
            false,
            false,
            now,
        );
        assert_eq!(evaluation.first_failed_gate, "family_or_gain");
        response_service_handoff_diagnostics::response_service_handoff_evaluation_signature(
            evaluation,
            service_family_loads,
        )
    };

    let before = signature(&targets);
    targets[1].observation.snapshot.bytes_in_flight = payload_bytes as u64;
    targets[1].observation.snapshot.queue_bytes = payload_bytes as u64;
    targets[1].observation.eta_ms = 1_000_000.0;
    let after = signature(&targets);

    assert_eq!(
        before, after,
        "shared carrier pressure cannot change a family/gain policy decision"
    );
}

#[test]
fn quic_ack_data_seen_path_does_not_own_unique_data_without_bulk_rate_proof() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let (active_commands, _active_rx) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(81),
        UnderlayProtocol::Udp,
        PathId(0),
        active_commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let validation_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (validation_commands, _validation_rx) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            validation_key.underlay,
            validation_key.path_id,
            validation_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.update_path_metrics(
        validation_key,
        PathMetrics {
            path_id: validation_key.path_id,
            underlay: validation_key.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 5_000,
            srtt_us: 5_000,
            rttvar_us: 500,
            jitter_us: 500,
            delivery_rate_bps: 1_000_000,
            pacing_rate_bps: 1_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: payload_bytes as u64,
            inflight_hi_bytes: payload_bytes as u64,
            confidence_ppm: 1,
            app_limited: true,
            has_ack_derived_data_sample: true,
            data_sample_count: 0,
            data_sample_bytes: 0,
        },
        ServerPathMetricsSource::LocalSender,
    );
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(7),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frames_rx,
    };

    let plan = plan_response_data_dispatch(&stream, FlowLane::Throughput, 0, payload_bytes)
        .expect("active owner should remain dispatchable");

    assert_eq!(
        plan.primary_key(),
        Some(CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        }),
        "ACK-data evidence cannot create Subflow OwnerData before the candidate has bulk-rate evidence"
    );
    assert_eq!(
        plan.primary_role(),
        PathRuntimeRole::Service,
        "ACK-data-only paths must not become Service owners"
    );
}
