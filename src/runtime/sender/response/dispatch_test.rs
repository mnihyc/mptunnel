use super::super::multipath::*;
use super::super::planner::*;
use super::*;
use crate::model::capacity::*;
use crate::model::multipath::*;
use crate::model::path::*;
use crate::mux::MuxLimits;
use crate::protocol::*;
use crate::runtime::path::commands::*;
use crate::runtime::sender::*;
use crate::runtime::stream::response::*;
use crate::runtime::stream::*;
use crate::scheduler::*;
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;

#[test]
fn response_dispatch_values_drop_snapshot_and_impossible_commit_state() {
    let ranked = std::mem::size_of::<ResponseSenderPathTarget>();
    let selected = std::mem::size_of::<ResponseSelectedDataTarget>();
    let dispatch = std::mem::size_of::<ResponseDispatchTarget>();
    let plan = std::mem::size_of::<ResponseDataDispatchTarget>();

    assert!(dispatch < ranked, "dispatch={dispatch} ranked={ranked}");
    assert!(
        selected <= 576,
        "selection must retain only one transition record: {selected} bytes"
    );
    assert!(
        plan <= 192,
        "the per-frame plan must retain one compact dispatch intent: {plan} bytes"
    );
}

#[test]
fn path_failure_repair_stream_data_uses_data_queue_when_priority_is_full() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let stream_id = StreamId(71);
    let repair_frame = Frame::StreamData {
        stream_id,
        offset: 1024,
        payload: Bytes::from(vec![7_u8; payload_bytes]),
    };
    let active_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (active_commands, _active_rx) = reliable_path_command_channels(1);
    active_commands
        .try_enqueue_admitted_frame(
            Frame::StreamAck {
                stream_id,
                complete: false,
                ranges: Vec::new(),
            },
            FlowLane::Control,
        )
        .expect("fill active priority queue");
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(71),
        active_key.underlay,
        active_key.path_id,
        active_commands,
        FlowLane::Throughput,
        mux_limits,
    );
    binding.record_owner_flight(active_key, &repair_frame);

    let (survivor_commands, _survivor_rx) = reliable_path_command_channels(1);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(1),
            survivor_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frames_rx,
    };

    assert!(
        response_frame_has_carrier_credit(
            &path_stream,
            &repair_frame,
            FlowLane::Latency,
            CarrierEmitMode::Classified,
            Some(RelaySendCause::PathFailureRepair),
        ),
        "RepairData is product-critical stream data: carrier priority queues may be full, but an open stream-data queue must still admit failover repair"
    );
}

#[test]
fn mixed_dispatch_plan_does_not_carry_udp_product_duplicate_when_primary_is_tcp() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let (active_commands, _active_rx) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(79),
        UnderlayProtocol::Tcp,
        PathId(0),
        active_commands,
        FlowLane::Throughput,
        mux_limits,
    );
    binding.update_path_metrics(
        CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        },
        PathMetrics {
            path_id: PathId(0),
            underlay: UnderlayProtocol::Tcp,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            srtt_us: 50_000,
            rttvar_us: 1_000,
            jitter_us: 1_000,
            delivery_rate_bps: 200_000_000,
            pacing_rate_bps: 200_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: payload_bytes as u64,
            inflight_hi_bytes: payload_bytes as u64,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: 4,
            data_sample_bytes: payload_bytes as u64,
        },
        ServerPathMetricsSource::LocalSender,
    );
    let (validation_commands, _validation_rx) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(1),
            validation_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.update_path_metrics(
        CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        },
        PathMetrics {
            path_id: PathId(1),
            underlay: UnderlayProtocol::Udp,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            srtt_us: 20_000,
            rttvar_us: 1_000,
            jitter_us: 1_000,
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
            confidence_ppm: 1_000_000,
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
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frames_rx,
    };

    let plan = plan_response_data_dispatch(&stream, FlowLane::Throughput, 0, payload_bytes)
        .expect("TCP primary remains dispatchable");

    assert_eq!(
        plan.primary_key(),
        Some(CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        })
    );
}

#[tokio::test]
async fn stale_service_plan_cannot_enqueue_owner_data_after_repair_role_change() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let active = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let validation = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (active_commands, _active_rx) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(77),
        active.underlay,
        active.path_id,
        active_commands.clone(),
        FlowLane::Throughput,
        mux_limits,
    );
    let (validation_commands, mut validation_rx) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            validation.underlay,
            validation.path_id,
            validation_commands.clone(),
            FlowLane::Throughput,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    while try_recv_reliable_path_command(&mut validation_rx).is_some() {}
    binding.detach(active, &active_commands);
    assert_eq!(binding.ordered_data_owner(), None);

    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(77),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    let plan = plan_response_data_dispatch(&stream, FlowLane::Throughput, 0, payload_bytes)
        .expect("liveness survivor may become the frontier-clear Service");
    assert_eq!(plan.primary_key(), Some(validation));
    assert_eq!(plan.primary_admission(), PathAdmission::Service);
    assert_eq!(
        binding.attach(
            validation.underlay,
            validation.path_id,
            validation_commands,
            FlowLane::Throughput,
            StreamOpenRole::Repair,
        ),
        ResponseStreamAttachOutcome::RoleChanged
    );
    let frame = Frame::StreamData {
        stream_id: StreamId(77),
        offset: 0,
        payload: Bytes::from(vec![0x77; payload_bytes]),
    };

    assert!(matches!(
        emit_planned_response_data_frame(&stream, plan, frame, FlowLane::Throughput),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(
        try_recv_reliable_path_command(&mut validation_rx).is_none(),
        "a stale Service plan must not enqueue STREAM_DATA on a Repair attachment"
    );
    let target = binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.observation.key == validation)
        .expect("Repair output remains attached");
    assert_eq!(target.observation.attachment_role, StreamOpenRole::Repair);
    assert_eq!(target.observation.snapshot.product_bytes_in_flight, 0);
    assert_eq!(target.observation.command_pending_bytes, 0);
    assert_eq!(binding.ordered_data_owner(), None);
}

#[tokio::test]
async fn passive_attach_preserves_one_bounded_exact_service_plan() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (service_commands, mut service_rx) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(109),
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(109),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: service.underlay,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    let plan = plan_response_data_dispatch(&stream, FlowLane::Throughput, 0, payload_bytes)
        .expect("live Service has a bounded owner plan");
    assert_eq!(plan.primary_key(), Some(service));
    assert_eq!(plan.primary_admission(), PathAdmission::Service);
    let planner_generation = binding.subflow_state_snapshot().0;

    let repair = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let (repair_commands, mut repair_rx) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            repair.underlay,
            repair.path_id,
            repair_commands,
            FlowLane::Throughput,
            StreamOpenRole::Repair,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert_ne!(binding.subflow_state_snapshot().0, planner_generation);

    let frame = Frame::StreamData {
        stream_id: StreamId(109),
        offset: 0,
        payload: Bytes::from(vec![0x6d; payload_bytes]),
    };
    let outcome =
        emit_planned_response_data_frame(&stream, plan, frame.clone(), FlowLane::Throughput)
            .expect("passive growth does not revoke the exact live Service quantum");
    assert_eq!(outcome.selected_path, Some(service));
    assert!(matches!(
        try_recv_reliable_path_command(&mut service_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(try_recv_reliable_path_command(&mut repair_rx).is_none());
    assert_eq!(
        binding.owner_flight_keys_overlapping_frame(&frame),
        vec![service]
    );
}

#[tokio::test]
async fn quic_probe_path_does_not_receive_product_duplicate_data() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let (active_commands, mut active_rx) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(78),
        UnderlayProtocol::Udp,
        PathId(0),
        active_commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let (validation_commands, mut validation_rx) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(1),
            validation_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    while try_recv_reliable_path_command(&mut validation_rx).is_some() {}
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(7),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    let plan = plan_response_data_dispatch(&stream, FlowLane::Throughput, 0, payload_bytes)
        .expect("active path should remain dispatchable");
    let frame = Frame::StreamData {
        stream_id: StreamId(7),
        offset: 0,
        payload: Bytes::from(vec![7_u8; payload_bytes]),
    };

    let outcome =
        emit_planned_response_data_frame(&stream, plan, frame.clone(), FlowLane::Throughput)
            .expect("primary data should emit");

    assert_eq!(
        outcome.selected_path,
        Some(CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        })
    );
    assert!(matches!(
        recv_reliable_path_command(&mut active_rx).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(
        try_recv_reliable_path_command(&mut validation_rx).is_none(),
        "Probe paths must not receive product STREAM_DATA"
    );
    let lower = binding.lower_flights_before_offset(payload_bytes as u64);
    assert!(
        lower.is_empty(),
        "plain unacked OwnerData stays in the flight ledger but is not ACK-hole ordering debt"
    );
}

#[tokio::test]
async fn response_owner_data_keeps_fifo_order_across_lane_changes() {
    let (commands, mut receiver) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(108),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        FlowLane::Throughput,
    );
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(108),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: 4,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    let target = binding
        .sender_path_targets(FlowLane::Throughput, 4)
        .into_iter()
        .next()
        .expect("binding has service target");

    let bulk_first = ResponseDataDispatchPlan {
        primary: ResponseDataDispatchTarget::Switchable {
            target: target.clone().into(),
            intent: ResponseDataDispatchIntent::Service,
        },
    };
    let latency_second = ResponseDataDispatchPlan {
        primary: ResponseDataDispatchTarget::Switchable {
            target: target.into(),
            intent: ResponseDataDispatchIntent::Service,
        },
    };

    emit_planned_response_data_frame(
        &stream,
        bulk_first,
        Frame::StreamData {
            stream_id: StreamId(108),
            offset: 0,
            payload: Bytes::from_static(b"aaaa"),
        },
        FlowLane::Throughput,
    )
    .expect("bulk owner data should enqueue");
    emit_planned_response_data_frame(
        &stream,
        latency_second,
        Frame::StreamData {
            stream_id: StreamId(108),
            offset: 4,
            payload: Bytes::from_static(b"bbbb"),
        },
        FlowLane::Latency,
    )
    .expect("latency owner data should enqueue");

    assert!(matches!(
        recv_reliable_path_command(&mut receiver).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut receiver).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 4,
            ..
        }))
    ));
}

#[tokio::test]
async fn one_flow_response_bounds_app_limited_sampling_before_service_resumes() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let lane_tracker = Arc::new(ServerPathLaneTracker::default());
    let (active_commands, mut active_rx) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        SessionId(88),
        UnderlayProtocol::Udp,
        PathId(0),
        active_commands,
        FlowLane::Throughput,
        mux_limits,
        lane_tracker,
    );
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    binding.update_path_metrics(
        service,
        PathMetrics {
            path_id: service.path_id,
            underlay: service.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            srtt_us: 50_000,
            rttvar_us: 1_000,
            jitter_us: 1_000,
            delivery_rate_bps: 200_000_000,
            pacing_rate_bps: 200_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: (payload_bytes * 8) as u64,
            inflight_hi_bytes: (payload_bytes * 8) as u64,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: 4,
            data_sample_bytes: (payload_bytes * 8) as u64,
        },
        ServerPathMetricsSource::LocalSender,
    );
    let optional = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (optional_commands, mut optional_rx) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            optional.underlay,
            optional.path_id,
            optional_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.update_path_metrics(
        optional,
        PathMetrics {
            path_id: optional.path_id,
            underlay: optional.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            srtt_us: 5_000,
            rttvar_us: 500,
            jitter_us: 500,
            delivery_rate_bps: 500_000_000,
            pacing_rate_bps: 500_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: (payload_bytes * 8) as u64,
            inflight_hi_bytes: (payload_bytes * 8) as u64,
            confidence_ppm: 900_000,
            app_limited: true,
            has_ack_derived_data_sample: true,
            data_sample_count: 0,
            data_sample_bytes: 0,
        },
        ServerPathMetricsSource::LocalSender,
    );
    binding.update_path_metrics(
        optional,
        PathMetrics {
            path_id: optional.path_id,
            underlay: optional.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            srtt_us: 5_000,
            rttvar_us: 500,
            jitter_us: 500,
            delivery_rate_bps: 500_000_000,
            pacing_rate_bps: 500_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: (payload_bytes * 8) as u64,
            inflight_hi_bytes: (payload_bytes * 8) as u64,
            confidence_ppm: 0,
            app_limited: false,
            has_ack_derived_data_sample: false,
            data_sample_count: 0,
            data_sample_bytes: 0,
        },
        ServerPathMetricsSource::PeerHint,
    );

    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(88),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    let startup_limit =
        usize::try_from(reliable_subflow_startup_sample_limit_bytes(mux_limits)).unwrap();
    assert_eq!(startup_limit % payload_bytes, 0);
    for quantum in 0..(startup_limit / payload_bytes) {
        let offset = (quantum * payload_bytes) as u64;
        let plan =
            plan_response_data_dispatch(&stream, FlowLane::Throughput, offset, payload_bytes)
                .expect("bounded Validation sampling should be dispatchable");
        assert_eq!(plan.primary_key(), Some(optional));
        assert_eq!(plan.primary_admission(), PathAdmission::Subflow);

        let frame = Frame::StreamData {
            stream_id: StreamId(88),
            offset,
            payload: Bytes::from(vec![9_u8; payload_bytes]),
        };
        let outcome = emit_planned_response_data_frame(&stream, plan, frame, FlowLane::Throughput)
            .expect("bounded startup Subflow OwnerData should emit");
        assert_eq!(outcome.selected_path, Some(optional));
        assert!(try_recv_reliable_path_command(&mut optional_rx).is_some());
        assert_eq!(
            binding.ordered_data_owner(),
            Some(service),
            "startup sampling must not migrate Service ownership"
        );
    }

    let service_offset = startup_limit as u64;
    let plan =
        plan_response_data_dispatch(&stream, FlowLane::Throughput, service_offset, payload_bytes)
            .expect("Service should resume after the startup sample cap");
    assert_eq!(plan.primary_key(), Some(service));
    assert_eq!(plan.primary_admission(), PathAdmission::Service);
    let frame = Frame::StreamData {
        stream_id: StreamId(88),
        offset: service_offset,
        payload: Bytes::from(vec![7_u8; payload_bytes]),
    };
    let outcome = emit_planned_response_data_frame(&stream, plan, frame, FlowLane::Throughput)
        .expect("Service OwnerData should emit after bounded sampling");
    assert_eq!(outcome.selected_path, Some(service));
    assert!(try_recv_reliable_path_command(&mut active_rx).is_some());
}

#[tokio::test]
async fn blocked_path_queue_rolls_back_unemitted_startup_credit() {
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
    let (service_commands, _service_rx) = reliable_path_command_channels(1);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(89),
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(1);
    assert_eq!(
        binding.attach(
            candidate.underlay,
            candidate.path_id,
            candidate_commands.clone(),
            FlowLane::Throughput,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_rx),
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
        input: SubflowAdmissionInput {
            key: candidate,
            bulk_rate_proven: false,
            startup_owner_allowed: true,
            frontier_clear: true,
            completion_improves: false,
            observed_goodput_non_degrading: true,
            owner_bytes: payload_bytes,
        },
    };
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(89),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: service.underlay,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    let frame = Frame::StreamData {
        stream_id: StreamId(89),
        offset: 0,
        payload: Bytes::from(vec![5_u8; payload_bytes]),
    };
    candidate_commands
        .try_enqueue_stream_ordered_frame(frame.clone(), FlowLane::Throughput)
        .expect("fill the candidate data queue after planning");
    let blocked = emit_planned_response_data_frame(
        &stream,
        ResponseDataDispatchPlan {
            primary: ResponseDataDispatchTarget::Switchable {
                target: target.clone().into(),
                intent: ResponseDataDispatchIntent::SubflowAdmission(request),
            },
        },
        frame.clone(),
        FlowLane::Throughput,
    );
    assert!(matches!(blocked, Err(RuntimeError::SenderServiceBlocked)));
    assert!(try_recv_reliable_path_command(&mut candidate_rx).is_some());

    let emitted = emit_planned_response_data_frame(
        &stream,
        ResponseDataDispatchPlan {
            primary: ResponseDataDispatchTarget::Switchable {
                target: target.into(),
                intent: ResponseDataDispatchIntent::SubflowAdmission(request),
            },
        },
        frame,
        FlowLane::Throughput,
    )
    .expect("the rolled-back startup quantum remains admissible");
    assert_eq!(emitted.selected_path, Some(candidate));
}

#[tokio::test]
async fn stale_passive_topology_plan_blocks_subflow_reservation_and_enqueue() {
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
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(90),
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            candidate.underlay,
            candidate.path_id,
            candidate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_rx),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
    let target = binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.observation.key == candidate)
        .expect("candidate output is attached");
    let (stale_planner_generation, _) = binding.subflow_state_snapshot();
    let lane_generation = binding.lane_generation();
    let input = SubflowAdmissionInput {
        key: candidate,
        bulk_rate_proven: false,
        startup_owner_allowed: true,
        frontier_clear: true,
        completion_improves: false,
        observed_goodput_non_degrading: true,
        owner_bytes: payload_bytes,
    };
    let stale_request = ResponseSubflowAdmissionRequest {
        expected_planner_generation: stale_planner_generation,
        expected_lane_generation: lane_generation,
        service,
        startup_owner_credit_bytes: payload_bytes,
        input,
    };
    let (unrelated_commands, _unrelated_rx) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            unrelated.underlay,
            unrelated.path_id,
            unrelated_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (fresh_planner_generation, _) = binding.subflow_state_snapshot();
    assert_ne!(fresh_planner_generation, stale_planner_generation);

    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(90),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: service.underlay,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    let frame = Frame::StreamData {
        stream_id: StreamId(90),
        offset: 0,
        payload: Bytes::from(vec![0x55; payload_bytes]),
    };
    let stale = emit_planned_response_data_frame(
        &stream,
        ResponseDataDispatchPlan {
            primary: ResponseDataDispatchTarget::Switchable {
                target: target.clone().into(),
                intent: ResponseDataDispatchIntent::SubflowAdmission(stale_request),
            },
        },
        frame.clone(),
        FlowLane::Throughput,
    );
    assert!(matches!(stale, Err(RuntimeError::SenderServiceBlocked)));
    assert!(
        try_recv_reliable_path_command(&mut candidate_rx).is_none(),
        "planner invalidation must fence both reservation and owner enqueue"
    );

    let fresh = emit_planned_response_data_frame(
        &stream,
        ResponseDataDispatchPlan {
            primary: ResponseDataDispatchTarget::Switchable {
                target: target.into(),
                intent: ResponseDataDispatchIntent::SubflowAdmission(
                    ResponseSubflowAdmissionRequest {
                        expected_planner_generation: fresh_planner_generation,
                        ..stale_request
                    },
                ),
            },
        },
        frame,
        FlowLane::Throughput,
    )
    .expect("fresh generation may reserve and enqueue the startup quantum");
    assert_eq!(fresh.selected_path, Some(candidate));
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
}

#[tokio::test]
async fn quic_ack_data_path_does_not_own_range_under_lower_owner_debt() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let active_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let (active_commands, _active_rx) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(88),
        active_key.underlay,
        active_key.path_id,
        active_commands,
        FlowLane::Throughput,
        mux_limits,
    );
    binding.set_ordered_data_owner(active_key);
    let active_frame = Frame::StreamData {
        stream_id: StreamId(7),
        offset: 0,
        payload: Bytes::from(vec![3_u8; payload_bytes]),
    };
    binding.record_owner_flight(active_key, &active_frame);

    let (ack_data_path_commands, mut ack_data_path_rx) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(1),
            ack_data_path_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let ack_data_path_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    binding.update_path_metrics(
        ack_data_path_key,
        PathMetrics {
            path_id: ack_data_path_key.path_id,
            underlay: ack_data_path_key.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            srtt_us: 20_000,
            rttvar_us: 1_000,
            jitter_us: 1_000,
            delivery_rate_bps: default_path_rate_bps().round() as u64,
            pacing_rate_bps: default_path_rate_bps().round() as u64,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: payload_bytes as u64,
            inflight_hi_bytes: payload_bytes as u64,
            confidence_ppm: 0,
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
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    let plan = plan_response_data_dispatch(
        &stream,
        FlowLane::Throughput,
        payload_bytes as u64,
        payload_bytes,
    )
    .expect("active owner should remain dispatchable");
    assert_eq!(plan.primary_key(), Some(active_key));
    assert_eq!(
        plan.primary_admission(),
        PathAdmission::Service,
        "validation paths must not receive unique owner data while lower bytes are unresolved"
    );

    let service_frame = Frame::StreamData {
        stream_id: StreamId(7),
        offset: payload_bytes as u64,
        payload: Bytes::from(vec![4_u8; payload_bytes]),
    };
    let outcome =
        emit_planned_response_data_frame(&stream, plan, service_frame, FlowLane::Throughput)
            .expect("service owner data should emit");

    assert_eq!(outcome.selected_path, Some(active_key));
    assert_eq!(
        binding.ordered_data_owner(),
        Some(active_key),
        "service owner remains the ordinary lead"
    );
    while let Some(_command) = try_recv_reliable_path_command(&mut ack_data_path_rx) {}
    let lower = binding.lower_flights_before_offset((payload_bytes * 2) as u64);
    assert!(!lower.iter().any(|flight| flight.key == ack_data_path_key));
}
