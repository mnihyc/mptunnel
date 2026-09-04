use super::*;
use crate::model::capacity::reliable_path_startup_sample_limit_bytes;
use crate::model::path::CarrierPathKey;
use crate::protocol::{Frame, OffsetRange, PathId, PathUsage, StreamId, UnderlayProtocol};
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, reliable_path_command_channels,
    reliable_path_command_pending_bytes, try_recv_reliable_path_command,
};
use crate::runtime::stream::ReliablePathStreamOutput;
use crate::runtime::stream::response::{ResponseStreamAttachOutcome, ResponseStreamBinding};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;

struct FixedFixture {
    stream: ReliablePathStream,
    commands: crate::runtime::path::commands::ReliablePathCommandSender,
    receivers: ReliablePathCommandReceivers,
}

struct ResponseAcquisitionFixture {
    limits: MuxLimits,
    binding: Arc<ResponseStreamBinding>,
    stream: ReliablePathStream,
    owner: CarrierPathKey,
    first_additional: CarrierPathKey,
    second_additional: CarrierPathKey,
    owner_commands: crate::runtime::path::commands::ReliablePathCommandSender,
    first_commands: crate::runtime::path::commands::ReliablePathCommandSender,
    second_commands: crate::runtime::path::commands::ReliablePathCommandSender,
    owner_receivers: ReliablePathCommandReceivers,
    first_receivers: ReliablePathCommandReceivers,
    second_receivers: ReliablePathCommandReceivers,
}

fn response_acquisition_fixture(queue_capacity: usize) -> ResponseAcquisitionFixture {
    let limits = MuxLimits::default();
    let owner = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let first_additional = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let second_additional = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(2),
    };
    let (owner_commands, owner_receivers) = reliable_path_command_channels(queue_capacity);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(31),
        owner.underlay,
        owner.path_id,
        owner_commands.clone(),
        TrafficClass::Throughput,
        limits,
    );
    let (first_commands, first_receivers) = reliable_path_command_channels(queue_capacity);
    assert_eq!(
        binding.attach(
            first_additional.underlay,
            first_additional.path_id,
            first_commands.clone(),
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached,
    );
    let (second_commands, second_receivers) = reliable_path_command_channels(queue_capacity);
    assert_eq!(
        binding.attach(
            second_additional.underlay,
            second_additional.path_id,
            second_commands.clone(),
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached,
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
    ResponseAcquisitionFixture {
        limits,
        binding,
        stream,
        owner,
        first_additional,
        second_additional,
        owner_commands,
        first_commands,
        second_commands,
        owner_receivers,
        first_receivers,
        second_receivers,
    }
}

fn establish_qualified_response_owner(
    fixture: &ResponseAcquisitionFixture,
    send_stream: &mut ReliableSendStream,
) {
    let floor = reliable_path_startup_sample_limit_bytes(fixture.limits);
    let floor_bytes = usize::try_from(floor).expect("response qualification floor fits usize");
    let qualification = send_stream
        .send_data(Bytes::from(vec![0x41; floor_bytes]))
        .expect("assign exact response qualification range");
    fixture
        .binding
        .record_original_flight(fixture.owner, &qualification);
    let exact_ack = [OffsetRange {
        start: 0,
        end: floor,
    }];
    fixture.binding.release_normalized_acked_ranges(&exact_ack);
    send_stream
        .apply_ack(&exact_ack)
        .expect("release exact response qualification range");

    let retained_frontier = send_stream
        .send_data(Bytes::from_static(b"f"))
        .expect("retain one live ordinary-owner byte");
    fixture
        .binding
        .record_original_flight(fixture.owner, &retained_frontier);
    fixture
        .binding
        .set_output_product_model_for_test(fixture.owner, 500_000_000.0, 1.0);
}

fn fill_response_data_queue(
    commands: &crate::runtime::path::commands::ReliablePathCommandSender,
    offset: u64,
) {
    commands
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: StreamId(999),
                offset,
                payload: Bytes::from_static(b"blocked"),
            },
            TrafficClass::Throughput,
        )
        .expect("fill exact response carrier data queue");
}

#[test]
fn stale_quic_requalification_backpressure_is_target_local_to_ready_tcp_data() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(32);
    let tcp = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let quic = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (tcp_commands, mut tcp_receivers) = reliable_path_command_channels(4);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(32),
        tcp.underlay,
        tcp.path_id,
        tcp_commands,
        TrafficClass::Throughput,
        limits,
    );
    let (quic_commands, mut quic_receivers) = reliable_path_command_channels(1);
    assert_eq!(
        binding.attach(
            quic.underlay,
            quic.path_id,
            quic_commands.clone(),
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached,
    );
    let (_frames_tx, frames) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: limits.max_payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames.into(),
    };

    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let retained = send_stream
        .send_data(Bytes::from_static(b"retained"))
        .expect("retain one exact Product range for requalification");
    binding.record_original_flight(tcp, &retained);
    let quic_target = binding
        .sender_path_targets(TrafficClass::Throughput, 1)
        .into_iter()
        .find(|target| target.observation.key == quic)
        .expect("attached QUIC response target");
    assert!(binding.mark_output_stale(
        ServerReinjectionOutputIdentity {
            key: quic,
            incarnation: quic_target.observation.incarnation,
        },
        TrafficClass::Throughput,
    ));
    quic_commands
        .try_enqueue_reinjection_frame(
            Frame::StreamData {
                stream_id: StreamId(999),
                offset: 0,
                payload: Bytes::from_static(b"fill-stale-quic"),
            },
            TrafficClass::Throughput,
        )
        .expect("fill only the stale QUIC reinjection queue");

    let mut sender = ServerResponseSenderService::new(SessionId(32), stream_id);
    sender.enqueue_data_for_lane(
        Bytes::from_static(b"ordinary-tcp"),
        TrafficClass::Throughput,
    );
    let requalification = sender
        .try_send_requalification_probe(
            &path_stream,
            &send_stream,
            TrafficClass::Throughput,
            limits,
        )
        .expect("target-local QUIC requalification result");
    assert!(
        requalification.is_capacity_blocked(),
        "target-local QUIC requalification backpressure must not request a global sender retry"
    );

    let data_ack_outstanding_bytes = send_stream.reinjection_bytes();
    let dispatch = sender
        .dispatch_next_with_data_ack_outstanding(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
            limits,
            data_ack_outstanding_bytes,
        )
        .expect("independent healthy TCP writer remains work-conserving");
    assert_eq!(dispatch.selected_path, Some(tcp));
    assert!(matches!(
        try_recv_reliable_path_command(&mut tcp_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
            if payload == Bytes::from_static(b"ordinary-tcp")
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut quic_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            stream_id: StreamId(999),
            ..
        }))
    ));
}

fn mark_response_output_backup(binding: &ResponseStreamBinding, key: CarrierPathKey) {
    let target = binding
        .sender_path_targets(TrafficClass::Throughput, 1)
        .into_iter()
        .find(|target| target.observation.key == key)
        .expect("attached response backup target");
    assert!(binding.update_peer_path_usage_for_test(
        key,
        target.observation.path_instance_id,
        1,
        PathUsage::Backup,
    ));
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
fn response_reinjection_accounting_tracks_data_ack_and_accepted_product_work() {
    let limits = MuxLimits::default();
    let performance = MppPerformanceConfig {
        optional_reinjection_budget_percent: 1,
    };
    let mut sender =
        ServerResponseSenderService::new_with_performance(SessionId(31), StreamId(31), performance);
    let startup_floor = sender_optional_reinjection_startup_floor_bytes(limits);

    sender.enqueue_reinjection_frame_with_priority(reinjection_frame(0, startup_floor), false);
    sender
        .enqueue_reinjection_frame_with_priority(reinjection_frame(startup_floor as u64, 1), false);
    assert_eq!(sender.bytes(), startup_floor.saturating_add(1));
    assert_eq!(
        sender.optional_reinjection.reinjected_bytes(),
        startup_floor.saturating_add(1) as u64,
    );

    sender.record_delivered_data(100_000);
    assert_eq!(sender.optional_reinjection.delivered_data_bytes(), 100_000);
}

#[test]
fn response_reinjection_final_enqueue_is_percentage_invariant_and_exactly_accounted() {
    let limits = MuxLimits::default();
    let startup_floor = sender_optional_reinjection_startup_floor_bytes(limits);
    let delivered_bytes = 1_000_000usize;
    let payload_bytes = sender_reinjection_minimum_useful_attempt_bytes(limits);
    let default_percent = MppPerformanceConfig::default().optional_reinjection_budget_percent;
    let mut observations = Vec::new();

    for percent in [0, default_percent, 200] {
        let mut sender = ServerResponseSenderService::new_with_performance(
            SessionId(31),
            StreamId(31),
            MppPerformanceConfig {
                optional_reinjection_budget_percent: percent,
            },
        );
        sender.record_delivered_data(delivered_bytes);
        sender.record_reinjection_for_test(startup_floor);
        assert_eq!(
            sender.optional_reinjection.delivered_data_bytes(),
            delivered_bytes as u64,
        );
        assert_eq!(
            sender.optional_reinjection.reinjected_bytes(),
            startup_floor as u64,
        );

        sender.enqueue_reinjection_frame_with_priority(reinjection_frame(0, payload_bytes), false);
        observations.push((
            percent,
            sender.bytes(),
            sender.optional_reinjection.reinjected_bytes(),
        ));
    }

    for (percent, queued_bytes, reinjected_bytes) in observations {
        assert_eq!(
            queued_bytes, payload_bytes,
            "percentage {percent} must not alter the admitted Product extent",
        );
        assert_eq!(
            reinjected_bytes,
            startup_floor.saturating_add(payload_bytes) as u64,
            "percentage {percent} must preserve exact accepted-Product-work accounting",
        );
    }
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
fn unresolved_return_plan_splits_trace_quantum_and_ack_does_not_refill_it() {
    let fixture = response_acquisition_fixture(8);
    fixture
        .binding
        .install_unresolved_response_startup_for_test(
            58_400,
            3,
            PathUsage::Available,
            fixture.owner,
        );
    let mut sender = ServerResponseSenderService::new(SessionId(31), StreamId(31));
    let mut send_stream = ReliableSendStream::new(StreamId(31), fixture.limits);

    for (proposal, expected) in [(208usize, 208usize), (14_600, 14_600), (65_536, 43_592)] {
        sender.enqueue_data_for_lane(Bytes::from(vec![0x51; proposal]), TrafficClass::Throughput);
        let dispatch = sender
            .dispatch_next(
                &fixture.stream,
                &mut send_stream,
                TrafficClass::Throughput,
                fixture.limits,
            )
            .expect("dispatch the startup-bounded trace quantum");
        assert_eq!(dispatch.payload_bytes, expected);
    }

    assert_eq!(send_stream.next_offset(), 58_400);
    assert_eq!(sender.data_bytes(), 65_536 - 43_592);
    assert!(
        !sender.front_has_carrier_credit_at_frontier(
            &fixture.stream,
            &send_stream,
            TrafficClass::Throughput,
            fixture.limits,
            send_stream.reinjection_bytes(),
            ReliableDataAckFrontierState::Live,
        ),
        "readiness preview must observe the same exhausted prefix as apply",
    );
    assert!(matches!(
        sender.dispatch_next(
            &fixture.stream,
            &mut send_stream,
            TrafficClass::Throughput,
            fixture.limits,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));

    send_stream
        .apply_ack(&[OffsetRange {
            start: 0,
            end: 58_400,
        }])
        .expect("ack the entire tentative prefix");
    assert_eq!(send_stream.next_offset(), 58_400);
    assert!(
        !sender.front_has_carrier_credit_at_frontier(
            &fixture.stream,
            &send_stream,
            TrafficClass::Throughput,
            fixture.limits,
            0,
            ReliableDataAckFrontierState::Live,
        ),
        "Data ACK cannot refund a cumulative startup coordinate",
    );
    assert!(matches!(
        sender.dispatch_next(
            &fixture.stream,
            &mut send_stream,
            TrafficClass::Throughput,
            fixture.limits,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
}

#[test]
fn canonical_singleton_preserves_the_exact_trace_dispatch_sequence() {
    let fixture = response_acquisition_fixture(8);
    let mut sender = ServerResponseSenderService::new(SessionId(31), StreamId(31));
    let mut send_stream = ReliableSendStream::new(StreamId(31), fixture.limits);

    for proposal in [208usize, 14_600, 65_536] {
        sender.enqueue_data_for_lane(Bytes::from(vec![0x52; proposal]), TrafficClass::Throughput);
        let dispatch = sender
            .dispatch_next(
                &fixture.stream,
                &mut send_stream,
                TrafficClass::Throughput,
                fixture.limits,
            )
            .expect("canonical singleton retains ordinary dispatch");
        assert_eq!(dispatch.payload_bytes, proposal);
    }

    assert_eq!(send_stream.next_offset(), 208 + 14_600 + 65_536);
    assert_eq!(sender.data_bytes(), 0);
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
fn ordinary_ecf_retains_the_frontier_owner_within_hysteresis() {
    let mut fixture = response_acquisition_fixture(1);
    let mut send_stream = ReliableSendStream::new(StreamId(31), fixture.limits);
    establish_qualified_response_owner(&fixture, &mut send_stream);
    fill_response_data_queue(&fixture.first_commands, 10_000);
    fixture.binding.set_output_product_model_for_test(
        fixture.second_additional,
        600_000_000.0,
        0.5,
    );
    let mut sender = ServerResponseSenderService::new(SessionId(31), StreamId(31));
    sender.enqueue_data_for_lane(Bytes::from_static(b"next"), TrafficClass::Throughput);
    let outstanding = send_stream.reinjection_bytes();

    let dispatch = sender
        .dispatch_next_with_data_ack_outstanding(
            &fixture.stream,
            &mut send_stream,
            TrafficClass::Throughput,
            fixture.limits,
            outstanding,
        )
        .expect("ordinary ECF retains the live frontier owner");

    assert_eq!(
        dispatch.selected_path,
        Some(fixture.owner),
        "a small modeled advantage does not override frontier hysteresis",
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.owner_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(try_recv_reliable_path_command(&mut fixture.second_receivers).is_none());
}

#[test]
fn ordinary_ecf_uses_backup_after_regular_outputs_are_blocked() {
    let mut fixture = response_acquisition_fixture(1);
    let mut send_stream = ReliableSendStream::new(StreamId(31), fixture.limits);
    establish_qualified_response_owner(&fixture, &mut send_stream);
    mark_response_output_backup(&fixture.binding, fixture.second_additional);
    fill_response_data_queue(&fixture.owner_commands, 20_000);
    fill_response_data_queue(&fixture.first_commands, 30_000);
    let outstanding = send_stream.reinjection_bytes();
    let mut sender = ServerResponseSenderService::new(SessionId(31), StreamId(31));
    sender.enqueue_data_for_lane(Bytes::from_static(b"next"), TrafficClass::Throughput);

    let dispatch = sender
        .dispatch_next_with_data_ack_outstanding(
            &fixture.stream,
            &mut send_stream,
            TrafficClass::Throughput,
            fixture.limits,
            outstanding,
        )
        .expect("backup remains the ordinary work-conserving fallback");
    assert_eq!(
        dispatch.selected_path,
        Some(fixture.second_additional),
        "a zero-commit Regular pass permits ordinary Backup placement",
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.second_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
            if payload == Bytes::from_static(b"next")
    ));
}

#[test]
fn blocked_additional_outputs_keep_a_qualified_owner_work_conserving() {
    let mut fixture = response_acquisition_fixture(1);
    let mut send_stream = ReliableSendStream::new(StreamId(31), fixture.limits);
    establish_qualified_response_owner(&fixture, &mut send_stream);
    fill_response_data_queue(&fixture.first_commands, 40_000);
    fill_response_data_queue(&fixture.second_commands, 50_000);
    let outstanding = send_stream.reinjection_bytes();
    let mut sender = ServerResponseSenderService::new(SessionId(31), StreamId(31));
    sender.enqueue_data_for_lane(Bytes::from_static(b"next"), TrafficClass::Throughput);

    let dispatch = sender
        .dispatch_next_with_data_ack_outstanding(
            &fixture.stream,
            &mut send_stream,
            TrafficClass::Throughput,
            fixture.limits,
            outstanding,
        )
        .expect("blocked alternatives must not stall an independently usable qualified owner");

    assert_eq!(dispatch.selected_path, Some(fixture.owner));
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.owner_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
}

#[test]
fn response_acquisition_does_not_bypass_the_ordinary_ecf_owner() {
    let mut fixture = response_acquisition_fixture(4);
    let mut send_stream = ReliableSendStream::new(StreamId(31), fixture.limits);
    establish_qualified_response_owner(&fixture, &mut send_stream);
    fixture
        .binding
        .set_output_historical_product_model_for_test(fixture.first_additional, 100_000.0, 500.0);
    mark_response_output_backup(&fixture.binding, fixture.second_additional);
    let outstanding = send_stream.reinjection_bytes();
    let mut sender = ServerResponseSenderService::new(SessionId(31), StreamId(31));
    sender.enqueue_data_for_lane(Bytes::from_static(b"next"), TrafficClass::Throughput);

    let dispatch = sender
        .dispatch_next_with_data_ack_outstanding(
            &fixture.stream,
            &mut send_stream,
            TrafficClass::Throughput,
            fixture.limits,
            outstanding,
        )
        .expect("ordinary ECF owner has Product and writer authority");

    assert_eq!(
        dispatch.selected_path,
        Some(fixture.owner),
        "an unqualified, materially slower additional output must not acquire Product ahead of the qualified current ECF owner",
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.owner_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
            if payload == Bytes::from_static(b"next")
    ));
    assert!(try_recv_reliable_path_command(&mut fixture.first_receivers).is_none());
}

#[test]
fn unmeasured_response_output_cannot_override_the_qualified_ecf_owner() {
    let mut fixture = response_acquisition_fixture(4);
    let mut send_stream = ReliableSendStream::new(StreamId(31), fixture.limits);
    establish_qualified_response_owner(&fixture, &mut send_stream);
    // Keep the first additional output wholly unmeasured and unqualified. Its
    // lack of Product evidence is not authority to bypass ordinary completion
    // ordering with a unique low-sequence acquisition quantum.
    mark_response_output_backup(&fixture.binding, fixture.second_additional);
    let outstanding = send_stream.reinjection_bytes();
    let mut sender = ServerResponseSenderService::new(SessionId(31), StreamId(31));
    sender.enqueue_data_for_lane(Bytes::from_static(b"next"), TrafficClass::Throughput);

    let dispatch = sender
        .dispatch_next_with_data_ack_outstanding(
            &fixture.stream,
            &mut send_stream,
            TrafficClass::Throughput,
            fixture.limits,
            outstanding,
        )
        .expect("qualified ordinary ECF owner has Product and writer authority");

    assert_eq!(
        dispatch.selected_path,
        Some(fixture.owner),
        "missing rate evidence cannot grant a second arbiter authority over the ordinary ECF result",
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.owner_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
            if payload == Bytes::from_static(b"next")
    ));
    assert!(try_recv_reliable_path_command(&mut fixture.first_receivers).is_none());
}

#[test]
fn ordinary_ecf_keeps_the_selected_unqualified_output_while_it_remains_best() {
    let mut fixture = response_acquisition_fixture(4);
    fixture
        .binding
        .set_output_product_model_for_test(fixture.owner, 5_000_000.0, 500.0);
    fixture
        .binding
        .set_output_product_model_for_test(fixture.first_additional, 500_000_000.0, 1.0);
    mark_response_output_backup(&fixture.binding, fixture.second_additional);
    let mut sender = ServerResponseSenderService::new(SessionId(31), StreamId(31));
    let mut send_stream = ReliableSendStream::new(StreamId(31), fixture.limits);

    sender.enqueue_data_for_lane(Bytes::from_static(b"first"), TrafficClass::Throughput);
    let first = sender
        .dispatch_next_with_data_ack_outstanding(
            &fixture.stream,
            &mut send_stream,
            TrafficClass::Throughput,
            fixture.limits,
            0,
        )
        .expect("ordinary completion placement establishes the first owner");
    assert_eq!(
        first.selected_path,
        Some(fixture.first_additional),
        "acquisition must not preempt ordinary first-owner selection",
    );

    sender.enqueue_data_for_lane(Bytes::from_static(b"second"), TrafficClass::Throughput);
    let outstanding = send_stream.reinjection_bytes();
    let second = sender
        .dispatch_next_with_data_ack_outstanding(
            &fixture.stream,
            &mut send_stream,
            TrafficClass::Throughput,
            fixture.limits,
            outstanding,
        )
        .expect("ordinary ECF keeps the lower-completion output");
    assert_eq!(
        second.selected_path,
        Some(fixture.first_additional),
        "qualification state does not override ordinary completion placement",
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.first_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.first_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(try_recv_reliable_path_command(&mut fixture.owner_receivers).is_none());
}

#[test]
fn frontier_hysteresis_is_not_overridden_by_additional_output_qualification() {
    let mut fixture = response_acquisition_fixture(4);
    let mut send_stream = ReliableSendStream::new(StreamId(31), fixture.limits);
    establish_qualified_response_owner(&fixture, &mut send_stream);
    fixture
        .binding
        .set_output_product_model_for_test(fixture.first_additional, 600_000_000.0, 0.5);
    mark_response_output_backup(&fixture.binding, fixture.second_additional);
    let mut sender = ServerResponseSenderService::new(SessionId(31), StreamId(31));

    for payload in [Bytes::from_static(b"one"), Bytes::from_static(b"two")] {
        sender.enqueue_data_for_lane(payload, TrafficClass::Throughput);
        let outstanding = send_stream.reinjection_bytes();
        let dispatch = sender
            .dispatch_next_with_data_ack_outstanding(
                &fixture.stream,
                &mut send_stream,
                TrafficClass::Throughput,
                fixture.limits,
                outstanding,
            )
            .expect("ordinary frontier owner remains schedulable");
        assert_eq!(
            dispatch.selected_path,
            Some(fixture.owner),
            "qualification demand is not a second placement policy",
        );
    }

    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.owner_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.owner_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(try_recv_reliable_path_command(&mut fixture.first_receivers).is_none());
}

#[test]
fn critical_reinjection_preempts_later_original_data() {
    let limits = MuxLimits::default();
    let mut fixture = fixed_fixture(4, limits);
    let mut sender = ServerResponseSenderService::new(SessionId(31), StreamId(31));
    let mut send_stream = ReliableSendStream::new(StreamId(31), limits);
    send_stream
        .send_data(Bytes::from_static(b"old"))
        .expect("establish retransmittable Product debt");
    sender.enqueue_data_for_lane(Bytes::from_static(b"new"), TrafficClass::Throughput);
    sender.enqueue_critical_reinjection_frame_with_cause(
        reinjection_frame(0, 3),
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
    assert_eq!(send_stream.next_offset(), 3);
    assert_eq!(sender.data_bytes(), 3);
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 0,
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
fn stale_output_recovery_releases_k_only_by_data_ack_and_never_retries_same_target() {
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
        // This fixture exercises target-reserve lifecycle, not Native QUIC
        // authority. A bare UDP output has no activation-scoped Native stamp
        // and is intentionally rejected at final Apply.
        underlay: UnderlayProtocol::Tcp,
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
    let (alternate_commands, mut alternate_receivers) = reliable_path_command_channels(8);
    let alternate_commands_for_backlog = alternate_commands.clone();
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
    assert!(binding.mark_output_stale(identity, TrafficClass::Throughput));

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
        !recovery.queued && recovery.blocked_for_carrier_capacity,
        "one live accepted copy consumes the target-wide emergency reserve across disjoint stale ranges",
    );
    assert_eq!(sender.bytes(), 0);
    let first_command = try_recv_reliable_path_command(&mut alternate_receivers)
        .expect("receive first recovery command");
    assert!(matches!(
        first_command,
        ReliablePathCommand::SendFrame(Frame::StreamData { .. })
    ));
    alternate_receivers
        .release_pending_command_bytes(reliable_path_command_pending_bytes(&first_command));
    let first_ack = [OffsetRange {
        start: 0,
        end: 4096,
    }];
    binding.release_normalized_acked_ranges(&first_ack);
    let _ = send_stream.apply_ack(&first_ack);

    assert!(
        sender
            .drive_stale_output_recovery(&stream, &send_stream, limits)
            .queued,
        "Data ACK and native queue release restore service for the next disjoint range",
    );
    assert_eq!(sender.bytes(), 4096);
    let second_dispatch = sender
        .dispatch_next(&stream, &mut send_stream, TrafficClass::Throughput, limits)
        .expect("dispatch second exact-range recovery");
    assert_eq!(second_dispatch.selected_path, Some(alternate));
    let second_command = try_recv_reliable_path_command(&mut alternate_receivers)
        .expect("receive second recovery command");
    assert!(matches!(
        second_command,
        ReliablePathCommand::SendFrame(Frame::StreamData { .. })
    ));
    alternate_commands_for_backlog
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: StreamId(999),
                offset: 0,
                payload: Bytes::from(vec![0x77; 4096]),
            },
            TrafficClass::Throughput,
        )
        .expect("queue unrelated native work above the modeled recovery window");

    binding.age_reinjected_flights_for_test(Duration::from_secs(2));
    let native_backlog = sender.drive_stale_output_recovery(&stream, &send_stream, limits);
    assert!(
        !native_backlog.queued && native_backlog.blocked_for_carrier_capacity,
        "deadline expiry cannot renew recovery while the same copy remains in native backlog",
    );
    alternate_receivers
        .release_pending_command_bytes(reliable_path_command_pending_bytes(&second_command));
    let unrelated_command = try_recv_reliable_path_command(&mut alternate_receivers)
        .expect("receive unrelated native backlog");
    alternate_receivers
        .release_pending_command_bytes(reliable_path_command_pending_bytes(&unrelated_command));
    let expired_retry = sender.drive_stale_output_recovery(&stream, &send_stream, limits);
    assert!(
        !expired_retry.queued && expired_retry.blocked_for_carrier_capacity,
        "timer and unrelated native-backlog release cannot renew the same target's Product authority; blocked={} deadline={:?} queued_bytes={}",
        expired_retry.blocked_for_carrier_capacity,
        expired_retry.retry_deadline,
        sender.bytes(),
    );
}

#[test]
fn stale_output_recovery_falls_through_exhausted_target_reserve() {
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
    let fast_exhausted = crate::model::path::CarrierPathKey {
        // Keep this reserve-fallthrough fixture carrier-neutral. Bare UDP
        // test outputs intentionally fail the separate Native-authority fence.
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let fallback = crate::model::path::CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(2),
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
    let (fast_commands, mut fast_receivers) = reliable_path_command_channels(8);
    let (fallback_commands, mut fallback_receivers) = reliable_path_command_channels(8);
    binding.attach(
        fast_exhausted.underlay,
        fast_exhausted.path_id,
        fast_commands,
        TrafficClass::Throughput,
    );
    binding.attach(
        fallback.underlay,
        fallback.path_id,
        fallback_commands,
        TrafficClass::Throughput,
    );
    binding.mark_output_path_proven_for_test(fast_exhausted);
    binding.mark_output_path_proven_for_test(fallback);
    binding.set_output_product_model_for_test(fast_exhausted, 500_000_000.0, 5.0);
    binding.set_output_product_model_for_test(fallback, 20_000_000.0, 50.0);

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
    let first_failed = send_stream
        .send_data(Bytes::from(vec![0x31; 4096]))
        .expect("assign first failed response range");
    let second_failed = send_stream
        .send_data(Bytes::from(vec![0x32; 4096]))
        .expect("assign second failed response range");
    binding.record_original_flight(initial, &first_failed);
    binding.record_original_flight(initial, &second_failed);
    binding.record_reinjected_flight(fast_exhausted, &reinjection_frame(8192, 4096));
    assert!(binding.mark_output_stale(identity, TrafficClass::Throughput));

    let mut sender = ServerResponseSenderService::new(SessionId(31), StreamId(31));
    let recovery = sender.drive_stale_output_recovery(&stream, &send_stream, limits);
    assert!(
        recovery.queued,
        "recovery must skip the fastest exhausted survivor and keep searching regular alternates",
    );
    assert!(
        sender.front_has_carrier_credit_at_frontier(
            &stream,
            &send_stream,
            TrafficClass::Throughput,
            limits,
            0,
            ReliableDataAckFrontierState::Live,
        ),
        "front readiness must exclude its own queued reinjection bytes from exact-target reserve revalidation",
    );

    let dispatch = sender
        .dispatch_next(&stream, &mut send_stream, TrafficClass::Throughput, limits)
        .expect("dispatch fallback stale-output recovery");
    assert_eq!(dispatch.selected_path, Some(fallback));
    assert_eq!(sender.stale_response_recovery_generation(), 1);
    assert!(try_recv_reliable_path_command(&mut fast_receivers).is_none());
    assert!(matches!(
        try_recv_reliable_path_command(&mut fallback_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 0,
            payload,
            ..
        })) if payload.len() == 4096
    ));

    let exhausted = sender.drive_stale_output_recovery(&stream, &send_stream, limits);
    assert!(
        !exhausted.queued && exhausted.blocked_for_carrier_capacity,
        "once both regular alternates hold live recovery reserve, reevaluation must block instead of spinning",
    );
    assert!(exhausted.retry_deadline.is_some());
}
