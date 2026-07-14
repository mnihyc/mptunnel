use super::*;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{
    lab_assert_server_sender_service_balanced, lab_diag_test_guard,
    lab_sender_service_counts_for_test,
};
use crate::runtime::stream::response::{ResponseStreamAttachOutcome, ResponseStreamBinding};

#[test]
fn persistent_response_repair_is_cancelled_when_output_incarnation_detaches() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(7),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(84),
        key.underlay,
        key.path_id,
        commands.clone(),
        FlowLane::Throughput,
    );
    let target = binding
        .sender_path_targets(FlowLane::Throughput, 64)
        .into_iter()
        .next()
        .expect("initial response output");
    let (_frames_tx, frames) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id: StreamId(84),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: key.underlay,
        max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames,
    };
    let mut sender = ServerResponseSenderService::new(SessionId(84), StreamId(84));
    sender.enqueue_critical_repair_frame_with_cause(
        Frame::StreamData {
            stream_id: StreamId(84),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(&[0x5e; 64]),
        },
        RelaySendCause::persistent_server_ack_gap_repair(
            ServerRepairOutputIdentity {
                key,
                incarnation: target.incarnation,
            },
            target.snapshot,
            FlowLane::Throughput,
        ),
    );

    binding.detach(key, &commands);
    assert_eq!(
        sender.discard_stale_persistent_ack_gap_repairs(&path_stream),
        64
    );
    assert!(sender.is_empty());
}

#[test]
fn response_repair_extra_budget_is_cumulative_not_per_event() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(91);
    let mut sender = ServerResponseSenderService::new_with_performance(
        SessionId(91),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );
    let startup_floor = sender_extra_traffic_startup_floor_bytes(mux_limits);
    let repair_payload = Bytes::from(vec![0x55; startup_floor]);

    assert_eq!(
        sender.repair_extra_budget_remaining(mux_limits),
        startup_floor
    );
    assert!(
        sender
            .enqueue_repair_frame_with_priority(
                Frame::StreamData {
                    stream_id,
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: repair_payload.clone(),
                },
                mux_limits,
                false,
            )
            .is_some(),
        "startup repair floor should be spendable once"
    );
    assert_eq!(sender.repair_extra_budget_remaining(mux_limits), 0);
    assert!(
        sender
            .enqueue_repair_frame_with_priority(
                Frame::StreamData {
                    stream_id,
                    offset: startup_floor as u64,
                    flags: StreamFlags::NONE,
                    payload: repair_payload.clone(),
                },
                mux_limits,
                false,
            )
            .is_none(),
        "repair budget must be cumulative, not refreshed for every tail/ACK event"
    );

    let earned_data_bytes = startup_floor.saturating_mul(100);
    sender.record_owner_progress_for_test(earned_data_bytes);

    assert!(
        sender.repair_extra_budget_remaining(mux_limits) >= startup_floor,
        "ACK-released owner progress earns more bounded extra repair budget"
    );
    assert!(
        sender
            .enqueue_repair_frame_with_priority(
                Frame::StreamData {
                    stream_id,
                    offset: (startup_floor * 2) as u64,
                    flags: StreamFlags::NONE,
                    payload: repair_payload,
                },
                mux_limits,
                false,
            )
            .is_some()
    );
}

#[test]
fn response_source_read_budget_is_separate_from_repair_cache_retention() {
    let stream_id = StreamId(93);
    let mux_limits = MuxLimits {
        max_repair_bytes: 4096,
        max_payload_bytes: 4096,
        max_stream_window_bytes: 64 * 1024,
        max_path_flight_bytes: 4096,
        ..MuxLimits::default()
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
    send_stream
        .send_data(
            Bytes::from(vec![0x5a; mux_limits.max_repair_bytes]),
            StreamFlags::NONE,
        )
        .expect("seed retained unacked OwnerData");
    assert_eq!(send_stream.repair_bytes(), mux_limits.max_repair_bytes);

    let sender_queue = ReliableRelaySenderQueue::default();
    assert!(
        reliable_relay_can_read_into_sender_queue(
            &send_stream,
            &sender_queue,
            mux_limits,
            mux_limits.max_repair_bytes,
        ),
        "repair cache retention is unacked OwnerData memory, not already-queued source bytes"
    );
    assert_eq!(
        reliable_relay_sender_queue_read_budget(
            &send_stream,
            &sender_queue,
            mux_limits,
            mux_limits.max_repair_bytes,
            mux_limits.max_repair_bytes,
        ),
        mux_limits.max_repair_bytes,
        "bounded product-source reads may continue while dispatch waits for repair-cache ACK release"
    );
}

#[test]
fn mixed_response_dispatch_payload_is_bounded_by_remaining_repair_capacity() {
    let stream_id = StreamId(98);
    let mux_limits = MuxLimits {
        max_payload_bytes: 4096,
        max_repair_bytes: 4096,
        max_path_flight_bytes: 4096,
        max_reliable_relay_chunk_bytes: 4096,
        ..MuxLimits::default()
    };
    let (commands, _active_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(98),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let (validation_commands, _validation_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(1),
            validation_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            4096,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: 4096,
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
    send_stream
        .send_data(Bytes::from(vec![0x5a; 3072]), StreamFlags::NONE)
        .expect("seed retained OwnerData");

    assert_eq!(
        response_dispatch_payload_bytes(
            &path_stream,
            &send_stream,
            FlowLane::Throughput,
            mux_limits,
            4096,
        ),
        Some(1024),
    );
    send_stream
        .send_data(Bytes::from(vec![0x5a; 1024]), StreamFlags::NONE)
        .expect("fill repair cache");
    assert_eq!(
        response_dispatch_payload_bytes(
            &path_stream,
            &send_stream,
            FlowLane::Throughput,
            mux_limits,
            4096,
        ),
        None,
    );
}

#[test]
fn coupled_response_dispatch_keeps_the_authoritative_send_stream_check() {
    let stream_id = StreamId(97);
    let mux_limits = MuxLimits {
        max_payload_bytes: 4096,
        max_repair_bytes: 4096,
        max_path_flight_bytes: 4096,
        max_reliable_relay_chunk_bytes: 4096,
        ..MuxLimits::default()
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(97),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: 4096,
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
    send_stream
        .send_data(Bytes::from(vec![0x5a; 4096]), StreamFlags::NONE)
        .expect("fill repair cache");

    assert_eq!(
        response_dispatch_payload_bytes(
            &path_stream,
            &send_stream,
            FlowLane::Throughput,
            mux_limits,
            4096,
        ),
        Some(4096),
        "coupled paths retain the existing send-stream repair-capacity boundary"
    );
}

#[tokio::test]
async fn formerly_mixed_response_retains_repair_preflight_after_family_detach() {
    let stream_id = StreamId(96);
    let mux_limits = MuxLimits {
        max_payload_bytes: 4096,
        max_repair_bytes: 4096,
        max_path_flight_bytes: 4096,
        max_reliable_relay_chunk_bytes: 4096,
        ..MuxLimits::default()
    };
    let (commands, _active_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(96),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let udp_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (udp_commands, _udp_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            udp_key.underlay,
            udp_key.path_id,
            udp_commands.clone(),
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            4096,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert!(binding.has_live_mixed_owner_underlays());
    binding.detach(udp_key, &udp_commands);
    assert!(!binding.has_live_mixed_owner_underlays());
    assert!(binding.may_have_mixed_owner_underlays());

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: 4096,
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
    send_stream
        .send_data(Bytes::from(vec![0x5a; 3072]), StreamFlags::NONE)
        .expect("seed retained OwnerData");
    let mut sender = ServerResponseSenderService::new(SessionId(96), stream_id);
    sender.enqueue_data_for_lane(Bytes::from(vec![0x33; 4096]), FlowLane::Throughput);

    let first = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
        )
        .expect("formerly mixed raw bytes dispatch within remaining repair capacity");
    assert_eq!(first.payload_bytes, 1024);
    assert_eq!(send_stream.repair_bytes(), 4096);
    assert_eq!(sender.data_bytes(), 3072);
    assert!(matches!(
        sender.dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert_eq!(sender.data_bytes(), 3072);
}

#[tokio::test]
async fn mixed_response_dispatch_waits_retryably_when_repair_cache_is_full() {
    let stream_id = StreamId(99);
    let mux_limits = MuxLimits {
        max_payload_bytes: 4096,
        max_repair_bytes: 4096,
        max_path_flight_bytes: 4096,
        max_reliable_relay_chunk_bytes: 4096,
        ..MuxLimits::default()
    };
    let (commands, _active_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(99),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let (validation_commands, _validation_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(1),
            validation_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            4096,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: 4096,
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
    send_stream
        .send_data(Bytes::from(vec![0x5a; 4096]), StreamFlags::NONE)
        .expect("fill repair cache");
    let blocked_offset = send_stream.next_offset();
    let mut sender = ServerResponseSenderService::new(SessionId(99), stream_id);
    sender.enqueue_data_for_lane(Bytes::from_static(b"next"), FlowLane::Throughput);

    assert!(matches!(
        sender.dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert_eq!(send_stream.next_offset(), blocked_offset);
    assert_eq!(sender.data_bytes(), 4, "blocked raw bytes remain queued");

    send_stream.apply_ack(&[OffsetRange {
        start: 0,
        end: blocked_offset,
    }]);
    sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
        )
        .expect("ACK release restores dispatch capacity");
    assert_eq!(sender.data_bytes(), 0);
}

#[test]
fn response_repair_extra_budget_accumulates_until_useful_attempt() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(92);
    let mut sender = ServerResponseSenderService::new_with_performance(
        SessionId(92),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );
    let startup_floor = sender_extra_traffic_startup_floor_bytes(mux_limits);
    let min_attempt = sender_repair_minimum_useful_attempt_bytes(mux_limits);

    assert!(sender.repair_extra_event_budget_remaining(mux_limits) >= min_attempt);
    assert!(
        sender
            .enqueue_repair_frame_with_priority(
                Frame::StreamData {
                    stream_id,
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from(vec![0x44; startup_floor]),
                },
                mux_limits,
                false,
            )
            .is_some()
    );

    sender.record_owner_progress_for_test(startup_floor);
    assert!(
        sender.repair_extra_budget_remaining(mux_limits) > 0,
        "ACK-released owner progress earns fractional repair budget"
    );
    assert_eq!(
        sender.repair_extra_event_budget_remaining(mux_limits),
        0,
        "tiny earned repair crumbs should accumulate instead of emitting high-overhead repair frames"
    );

    sender.record_owner_progress_for_test(min_attempt.saturating_mul(100));
    assert!(
        sender.repair_extra_event_budget_remaining(mux_limits) >= min_attempt,
        "once enough owner bytes make ACK progress, repair can spend a useful attempt"
    );
}

#[tokio::test]
async fn response_owner_dispatch_does_not_earn_repair_budget_before_ack_progress() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(96);
    let startup_floor = sender_extra_traffic_startup_floor_bytes(mux_limits);
    let (commands, _receivers) = reliable_path_command_channels(8);
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            mux_limits,
        ),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
    let mut sender = ServerResponseSenderService::new_with_performance(
        SessionId(96),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );

    sender
        .extra_traffic
        .record_optional(ExtraTrafficKind::Repair, startup_floor);
    assert_eq!(sender.repair_extra_budget_remaining(mux_limits), 0);

    sender.enqueue_data_for_lane(
        Bytes::from(vec![0x96; startup_floor.saturating_mul(100)]),
        FlowLane::Throughput,
    );
    sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
        )
        .expect("owner dispatch should not be blocked by exhausted repair budget");

    assert_eq!(
        sender.repair_extra_budget_remaining(mux_limits),
        0,
        "emitted OwnerData must not earn optional repair budget until ordered ACK progress releases it"
    );
}

#[cfg(feature = "lab-diagnostics")]
#[tokio::test]
async fn fixed_output_owner_data_records_sender_service_decision_for_conformance() {
    let _guard = lab_diag_test_guard();
    let mux_limits = MuxLimits::default();
    let session_id = SessionId(97);
    let stream_id = StreamId(97);
    let (commands, _receivers) = reliable_path_command_channels(8);
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            mux_limits,
        ),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
    let mut sender = ServerResponseSenderService::new(session_id, stream_id);

    sender.enqueue_data_for_lane(Bytes::from_static(b"owner"), FlowLane::Throughput);
    sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
        )
        .expect("fixed output OwnerData dispatch should succeed");

    assert_eq!(
        lab_sender_service_counts_for_test(session_id.0, stream_id.0),
        (1, 1),
        "fixed output OwnerData must be accounted as a sender-service decision"
    );
    lab_assert_server_sender_service_balanced(session_id.0, stream_id.0);
}

#[test]
fn response_critical_repair_closes_tail_after_optional_budget_exhaustion() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(94);
    let mut sender = ServerResponseSenderService::new_with_performance(
        SessionId(94),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );
    let startup_floor = sender_extra_traffic_startup_floor_bytes(mux_limits);
    let frame = Frame::StreamData {
        stream_id,
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x44; startup_floor]),
    };
    assert!(
        sender
            .enqueue_repair_frame_with_priority(frame, mux_limits, false)
            .is_some()
    );

    let closure_frame = Frame::StreamData {
        stream_id,
        offset: startup_floor as u64,
        flags: StreamFlags::NONE,
        payload: Bytes::from_static(b"tail"),
    };
    assert!(
        sender
            .enqueue_repair_frame_with_priority(closure_frame.clone(), mux_limits, false)
            .is_none(),
        "ordinary optional repair budget should be exhausted"
    );

    sender.enqueue_critical_repair_frame(closure_frame);
    assert_eq!(sender.repair_extra_budget_remaining(mux_limits), 0);
}

#[test]
fn response_critical_tail_repair_is_idempotent_while_range_is_queued() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(96);
    let mut sender = ServerResponseSenderService::new_with_performance(
        SessionId(96),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );
    let first = Frame::StreamData {
        stream_id,
        offset: 128,
        flags: StreamFlags::NONE,
        payload: Bytes::from_static(&[0x44; 64]),
    };
    let duplicate = first.clone();

    assert!(sender.enqueue_critical_tail_repair_frame(first).is_some());
    let bytes_after_first = sender.bytes();
    let budget_after_first = sender.repair_extra_budget_remaining(mux_limits);

    assert!(
        sender
            .enqueue_critical_tail_repair_frame(duplicate)
            .is_none(),
        "final-tail RepairData is a one pending repair per offset range, not a repeatable owner-data substitute"
    );
    assert_eq!(sender.bytes(), bytes_after_first);
    assert_eq!(
        sender.repair_extra_budget_remaining(mux_limits),
        budget_after_first
    );
}
