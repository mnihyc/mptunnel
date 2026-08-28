use super::*;
use crate::protocol::{Frame, PathId, StreamId, UnderlayProtocol};
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, reliable_path_command_channels,
    try_recv_reliable_path_command,
};
use crate::runtime::stream::ReliablePathStreamOutput;
use bytes::Bytes;
use tokio::sync::mpsc;

struct FixedFixture {
    stream: ReliablePathStream,
    commands: crate::runtime::path::commands::ReliablePathCommandSender,
    receivers: ReliablePathCommandReceivers,
}

fn fixed_fixture(queue_capacity: usize, limits: MuxLimits) -> FixedFixture {
    let (commands, receivers) = reliable_path_command_channels(queue_capacity);
    let (_frames_tx, frames) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(31),
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: limits.max_payload_bytes,
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands.clone(),
            limits,
        ),
        frames: frames.into(),
    };
    FixedFixture {
        stream,
        commands,
        receivers,
    }
}

fn reinjection_frame(offset: u64, payload_bytes: usize) -> Frame {
    Frame::StreamData {
        stream_id: StreamId(31),
        offset,
        payload: Bytes::from(vec![0x4a; payload_bytes]),
    }
}

#[test]
fn optional_reinjection_budget_is_cumulative_and_data_ack_funded() {
    let limits = MuxLimits::default();
    let performance = MppPerformanceConfig {
        optional_reinjection_budget_percent: 1,
    };
    let mut sender =
        ServerResponseSenderService::new_with_performance(SessionId(31), StreamId(31), performance);
    let startup_floor = sender_optional_reinjection_startup_floor_bytes(limits);

    assert!(
        sender
            .enqueue_reinjection_frame_with_priority(
                reinjection_frame(0, startup_floor),
                limits,
                false,
            )
            .is_some()
    );
    assert_eq!(sender.reinjection_extra_budget_remaining(limits), 0);
    assert!(
        sender
            .enqueue_reinjection_frame_with_priority(
                reinjection_frame(startup_floor as u64, 1),
                limits,
                false,
            )
            .is_none()
    );

    sender.record_delivered_data(100_000);
    assert_eq!(sender.reinjection_extra_budget_remaining(limits), 1_000);
}

#[test]
fn dispatching_data_commits_mux_offset_queue_and_carrier_work() {
    let limits = MuxLimits::default();
    let mut fixture = fixed_fixture(4, limits);
    let mut sender = ServerResponseSenderService::new(SessionId(31), StreamId(31));
    let mut send_stream = ReliableSendStream::new(StreamId(31), limits);
    sender.enqueue_data_for_lane(Bytes::from_static(b"response"), TrafficClass::Throughput);

    let dispatch = sender
        .dispatch_next(
            &fixture.stream,
            &mut send_stream,
            TrafficClass::Throughput,
            limits,
        )
        .expect("dispatch response data");

    assert_eq!(dispatch.lane, ReliableWorkClass::Data);
    assert_eq!(dispatch.payload_bytes, 8);
    assert_eq!(send_stream.next_offset(), 8);
    assert!(sender.is_empty());
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 0,
            payload,
            ..
        })) if payload == Bytes::from_static(b"response")
    ));
}

#[test]
fn unavailable_carrier_does_not_advance_mux_or_consume_data() {
    let limits = MuxLimits::default();
    let fixture = fixed_fixture(1, limits);
    fixture
        .commands
        .try_enqueue_stream_ordered_frame(reinjection_frame(4096, 1024), TrafficClass::Throughput)
        .expect("fill carrier queue");
    let mut sender = ServerResponseSenderService::new(SessionId(31), StreamId(31));
    let mut send_stream = ReliableSendStream::new(StreamId(31), limits);
    sender.enqueue_data_for_lane(Bytes::from_static(b"blocked"), TrafficClass::Throughput);

    assert!(matches!(
        sender.dispatch_next(
            &fixture.stream,
            &mut send_stream,
            TrafficClass::Throughput,
            limits,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert_eq!(send_stream.next_offset(), 0);
    assert_eq!(sender.data_bytes(), 7);
}

#[test]
fn critical_reinjection_preempts_later_original_data() {
    let limits = MuxLimits::default();
    let mut fixture = fixed_fixture(4, limits);
    let mut sender = ServerResponseSenderService::new(SessionId(31), StreamId(31));
    let mut send_stream = ReliableSendStream::new(StreamId(31), limits);
    sender.enqueue_data_for_lane(Bytes::from_static(b"new"), TrafficClass::Throughput);
    sender.enqueue_critical_reinjection_frame_with_cause(
        reinjection_frame(100, 3),
        RelaySendCause::AckGapReinjection,
    );

    let dispatch = sender
        .dispatch_next(
            &fixture.stream,
            &mut send_stream,
            TrafficClass::Throughput,
            limits,
        )
        .expect("critical reinjection dispatch");

    assert_eq!(dispatch.lane, ReliableWorkClass::Reinjection);
    assert_eq!(send_stream.next_offset(), 0);
    assert_eq!(sender.data_bytes(), 3);
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 100,
            ..
        }))
    ));
}

#[test]
fn large_queued_payload_is_dispatched_in_bounded_chunks() {
    let limits = MuxLimits::default();
    let fixture = fixed_fixture(8, limits);
    let mut sender = ServerResponseSenderService::new(SessionId(31), StreamId(31));
    let mut send_stream = ReliableSendStream::new(StreamId(31), limits);
    let total = limits.max_payload_bytes + 4096;
    sender.enqueue_data_for_lane(Bytes::from(vec![0x33; total]), TrafficClass::Throughput);

    let dispatch = sender
        .dispatch_next(
            &fixture.stream,
            &mut send_stream,
            TrafficClass::Throughput,
            limits,
        )
        .expect("dispatch bounded prefix");

    assert!(dispatch.payload_bytes <= limits.max_payload_bytes);
    assert_eq!(sender.data_bytes(), total - dispatch.payload_bytes);
    assert_eq!(send_stream.next_offset(), dispatch.payload_bytes as u64);
}

#[test]
fn published_product_queue_is_shared_stream_state() {
    let limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(4);
    let binding = crate::runtime::stream::response::ResponseStreamBinding::new(
        SessionId(31),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        TrafficClass::Throughput,
    );
    let (_frames_tx, frames) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(31),
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: limits.max_payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames.into(),
    };
    let mut sender = ServerResponseSenderService::new(SessionId(31), StreamId(31));
    sender.enqueue_data_for_lane(Bytes::from_static(b"queued"), TrafficClass::Throughput);

    sender.publish_queue_bytes(&stream);

    let targets = binding.sender_path_targets(TrafficClass::Throughput, 6);
    assert_eq!(targets[0].observation.snapshot.data_level_queue_bytes, 6);
}

#[test]
fn stale_output_recovery_admits_disjoint_ranges_and_retries_exact_ranges() {
    let limits = MuxLimits {
        max_payload_bytes: 4096,
        max_repair_bytes: 8192,
        max_path_flight_bytes: 4096,
        max_reliable_relay_chunk_bytes: 4096,
        ..MuxLimits::default()
    };
    let initial = crate::model::path::CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let alternate = crate::model::path::CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (initial_commands, _initial_receivers) = reliable_path_command_channels(8);
    let binding = crate::runtime::stream::response::ResponseStreamBinding::new_with_limits(
        SessionId(31),
        initial.underlay,
        initial.path_id,
        initial_commands,
        TrafficClass::Throughput,
        limits,
    );
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    binding.attach(
        alternate.underlay,
        alternate.path_id,
        alternate_commands,
        TrafficClass::Throughput,
    );
    let identity = binding
        .sender_path_targets(TrafficClass::Throughput, 1)
        .into_iter()
        .find(|target| target.observation.key == initial)
        .map(|target| ServerReinjectionOutputIdentity {
            key: initial,
            incarnation: target.observation.incarnation,
        })
        .expect("initial output identity");
    let (_frames_tx, frames) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(31),
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: limits.max_payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames.into(),
    };
    let mut send_stream = ReliableSendStream::new(StreamId(31), limits);
    let first = send_stream
        .send_data(Bytes::from(vec![0x31; 4096]))
        .expect("assign first response range");
    let second = send_stream
        .send_data(Bytes::from(vec![0x32; 4096]))
        .expect("assign second response range");
    binding.record_original_flight(initial, &first);
    binding.record_original_flight(initial, &second);
    assert!(binding.mark_output_stale(identity));

    let mut sender = ServerResponseSenderService::new(SessionId(31), StreamId(31));
    assert!(
        sender
            .drive_stale_output_recovery(&stream, &send_stream, limits)
            .queued
    );
    assert_eq!(sender.bytes(), 4096);
    let first_dispatch = sender
        .dispatch_next(&stream, &mut send_stream, TrafficClass::Throughput, limits)
        .expect("dispatch first exact-range recovery");
    assert_eq!(first_dispatch.selected_path, Some(alternate));
    assert_eq!(sender.bytes(), 0);
    let recovery = sender.drive_stale_output_recovery(&stream, &send_stream, limits);
    assert!(
        recovery.retry_deadline.is_some(),
        "a current recovery copy exposes its exact-range retry deadline"
    );
    assert!(
        recovery.queued,
        "a current copy cannot delay a disjoint never-attempted range"
    );
    assert_eq!(sender.bytes(), 4096);
    let second_dispatch = sender
        .dispatch_next(&stream, &mut send_stream, TrafficClass::Throughput, limits)
        .expect("dispatch second exact-range recovery");
    assert_eq!(second_dispatch.selected_path, Some(alternate));

    binding.age_reinjected_flights_for_test(Duration::from_secs(2));
    assert!(
        sender
            .drive_stale_output_recovery(&stream, &send_stream, limits)
            .queued,
        "an unacknowledged exact range is eligible after its recovery interval"
    );
    let retry_dispatch = sender
        .dispatch_next(&stream, &mut send_stream, TrafficClass::Throughput, limits)
        .expect("retry exact range on the surviving output");
    assert_eq!(
        retry_dispatch.selected_path,
        Some(alternate),
        "retry may reuse the same live survivor while remaining distinct from the stale owner"
    );
}
