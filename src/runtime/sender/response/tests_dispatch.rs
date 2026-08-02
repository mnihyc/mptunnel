use super::*;
use crate::mux::MuxLimits;
use crate::protocol::{Frame, PathId, SessionId, StreamId, UnderlayProtocol};
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, reliable_path_command_channels,
    try_recv_reliable_path_command,
};
use crate::runtime::sender::response::multipath::plan_response_data_dispatch;
use crate::runtime::stream::response::{ResponseStreamAttachOutcome, ResponseStreamBinding};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

fn data_frame(offset: u64, payload_bytes: usize) -> Frame {
    Frame::StreamData {
        stream_id: StreamId(23),
        offset,
        payload: Bytes::from(vec![0x5a; payload_bytes]),
    }
}

fn stream_with_output(output: ReliablePathStreamOutput) -> ReliablePathStream {
    let (_frames_tx, frames) = mpsc::channel(1);
    ReliablePathStream {
        stream_id: StreamId(23),
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
        output,
        frames: frames.into(),
    }
}

struct SwitchableFixture {
    binding: Arc<ResponseStreamBinding>,
    stream: ReliablePathStream,
    initial: crate::model::path::CarrierPathKey,
    initial_receivers: ReliablePathCommandReceivers,
}

fn switchable_fixture() -> SwitchableFixture {
    let initial = crate::model::path::CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (commands, initial_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(23),
        initial.underlay,
        initial.path_id,
        commands,
        TrafficClass::Throughput,
    );
    let stream = stream_with_output(ReliablePathStreamOutput::Switchable(binding.clone()));
    SwitchableFixture {
        binding,
        stream,
        initial,
        initial_receivers,
    }
}

fn assert_data_command(receivers: &mut ReliablePathCommandReceivers, offset: u64) {
    assert!(matches!(
        try_recv_reliable_path_command(receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: actual,
            ..
        })) if actual == offset
    ));
}

fn output_identity(
    binding: &ResponseStreamBinding,
    key: crate::model::path::CarrierPathKey,
) -> (crate::model::path::CarrierPathKey, u64) {
    binding
        .sender_path_targets(TrafficClass::Throughput, 1)
        .into_iter()
        .find(|target| target.observation.key == key)
        .map(|target| (key, target.observation.incarnation))
        .expect("attached response output")
}

#[test]
fn fixed_data_commit_records_flight_before_publishing_command() {
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let stream = stream_with_output(ReliablePathStreamOutput::fixed(
        UnderlayProtocol::Tcp,
        PathId(4),
        commands,
        MuxLimits::default(),
    ));
    let frame = data_frame(0, 2048);
    let plan = plan_response_data_dispatch(&stream, TrafficClass::Throughput, 0, 2048)
        .expect("fixed output has queue credit");

    let selected_path =
        emit_planned_response_data_frame(&stream, plan, frame, TrafficClass::Throughput)
            .expect("fixed data commit");

    assert_eq!(selected_path.map(|key| key.path_id), Some(PathId(4)));
    assert_data_command(&mut receivers, 0);
}

#[test]
fn fixed_latency_data_dispatch_overtakes_queued_bulk() {
    let (commands, mut receivers) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(data_frame(4096, 1024), TrafficClass::Throughput)
        .expect("queue bulk response data");
    let stream = stream_with_output(ReliablePathStreamOutput::fixed(
        UnderlayProtocol::Tcp,
        PathId(4),
        commands,
        MuxLimits::default(),
    ));
    let frame = data_frame(0, 128);
    let plan = plan_response_data_dispatch(&stream, TrafficClass::Latency, 0, 128)
        .expect("latency response has priority capacity");

    emit_planned_response_data_frame(&stream, plan, frame, TrafficClass::Latency)
        .expect("dispatch latency response data");

    assert_data_command(&mut receivers, 0);
    assert_data_command(&mut receivers, 4096);
}

#[test]
fn switchable_data_commit_publishes_exact_range_and_command() {
    let mut fixture = switchable_fixture();
    let frame = data_frame(4096, 1536);
    let plan = plan_response_data_dispatch(&fixture.stream, TrafficClass::Throughput, 4096, 1536)
        .expect("initial response path has queue credit");

    let selected_path = emit_planned_response_data_frame(
        &fixture.stream,
        plan,
        frame.clone(),
        TrafficClass::Throughput,
    )
    .expect("switchable data commit");

    assert_eq!(selected_path, Some(fixture.initial));
    assert_eq!(
        fixture
            .binding
            .original_flight_outputs_overlapping_frame(&frame),
        vec![output_identity(&fixture.binding, fixture.initial)]
    );
    assert_data_command(&mut fixture.initial_receivers, 4096);
}

#[test]
fn stale_model_generation_rejects_without_carrier_publication() {
    let mut fixture = switchable_fixture();
    let frame = data_frame(0, 1024);
    let plan = plan_response_data_dispatch(&fixture.stream, TrafficClass::Throughput, 0, 1024)
        .expect("observe initial model");
    let (commands, _receivers) = reliable_path_command_channels(1);
    assert_eq!(
        fixture.binding.attach(
            UnderlayProtocol::Udp,
            PathId(9),
            commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached,
    );

    assert!(matches!(
        emit_planned_response_data_frame(
            &fixture.stream,
            plan,
            frame.clone(),
            TrafficClass::Throughput,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(
        fixture
            .binding
            .original_flight_outputs_overlapping_frame(&frame)
            .is_empty()
    );
    assert!(try_recv_reliable_path_command(&mut fixture.initial_receivers).is_none());
}

#[test]
fn closed_carrier_queue_cannot_overtake_ordered_detach() {
    let fixture = switchable_fixture();
    let alternate = crate::model::path::CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(9),
    };
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    fixture.binding.attach(
        alternate.underlay,
        alternate.path_id,
        alternate_commands,
        TrafficClass::Throughput,
    );

    let original = data_frame(0, 1024);
    fixture
        .binding
        .record_original_flight(fixture.initial, &original);
    let initial_identity = output_identity(&fixture.binding, fixture.initial);
    let next = data_frame(1024, 1024);
    let initial_target = fixture
        .binding
        .sender_path_targets(TrafficClass::Throughput, 1024)
        .into_iter()
        .find(|target| target.observation.key == fixture.initial)
        .expect("initial response target");
    let plan = ResponseDataDispatchTarget::Switchable {
        target: ResponseDispatchTarget::from(&initial_target),
        expected_model_generation: fixture.binding.response_model_generation(),
    };

    drop(fixture.initial_receivers);
    assert!(matches!(
        emit_planned_response_data_frame(&fixture.stream, plan, next, TrafficClass::Throughput,),
        Err(RuntimeError::SenderServiceBlocked)
    ));

    assert!(
        fixture
            .binding
            .has_output_incarnation(initial_identity.0, initial_identity.1),
        "dispatch must leave ownership for the carrier's ordered detach event"
    );
    assert!(
        fixture
            .binding
            .uncovered_failed_original_ranges()
            .is_empty(),
        "queue closure alone must not convert accepted ACK order into failed work"
    );
}

#[test]
fn reinjection_prefers_a_path_without_the_original_range() {
    let mut fixture = switchable_fixture();
    let alternate = crate::model::path::CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (alternate_commands, mut alternate_receivers) = reliable_path_command_channels(8);
    fixture.binding.attach(
        alternate.underlay,
        alternate.path_id,
        alternate_commands,
        TrafficClass::Throughput,
    );
    fixture.binding.mark_output_path_proven_for_test(alternate);
    let frame = data_frame(0, 4096);
    fixture
        .binding
        .record_original_flight(fixture.initial, &frame);

    let selected = emit_response_frame_from_sender_service(
        &fixture.stream,
        frame.clone(),
        TrafficClass::Throughput,
        CarrierEmitMode::Classified,
        "tail_reinjection",
        Some(RelaySendCause::TailReinjection),
    )
    .expect("alternate accepts reinjection");

    assert_eq!(selected, Some(alternate));
    let mut outputs = fixture.binding.flight_outputs_overlapping_frame(&frame);
    outputs.sort_by_key(|(key, _)| key.path_id.0);
    assert_eq!(
        outputs,
        vec![
            output_identity(&fixture.binding, fixture.initial),
            output_identity(&fixture.binding, alternate),
        ]
    );
    assert_data_command(&mut alternate_receivers, 0);
    assert!(try_recv_reliable_path_command(&mut fixture.initial_receivers).is_none());
}

#[test]
fn aged_repair_history_on_every_alternate_does_not_block_ack_gap_retry() {
    let mut fixture = switchable_fixture();
    let first_alternate = crate::model::path::CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let second_alternate = crate::model::path::CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(2),
    };
    let (first_commands, mut first_receivers) = reliable_path_command_channels(8);
    let (second_commands, mut second_receivers) = reliable_path_command_channels(8);
    fixture.binding.attach(
        first_alternate.underlay,
        first_alternate.path_id,
        first_commands,
        TrafficClass::Throughput,
    );
    fixture.binding.attach(
        second_alternate.underlay,
        second_alternate.path_id,
        second_commands,
        TrafficClass::Throughput,
    );
    fixture
        .binding
        .mark_output_path_proven_for_test(first_alternate);
    fixture
        .binding
        .mark_output_path_proven_for_test(second_alternate);

    let frame = data_frame(0, 4096);
    fixture
        .binding
        .record_original_flight(fixture.initial, &frame);
    fixture
        .binding
        .record_reinjected_flight(first_alternate, &frame);
    fixture
        .binding
        .record_reinjected_flight(second_alternate, &frame);
    fixture
        .binding
        .age_reinjected_flights_for_test(Duration::from_secs(1));
    assert!(
        !fixture
            .binding
            .has_recent_reinjection_overlap(&frame, Duration::from_millis(100),)
    );

    let selected = emit_response_frame_from_sender_service(
        &fixture.stream,
        frame,
        TrafficClass::Throughput,
        CarrierEmitMode::Classified,
        "ack_gap_reinjection",
        Some(RelaySendCause::AckGapReinjection),
    )
    .expect("aged repair history must leave an alternate eligible");

    assert_eq!(selected, Some(first_alternate));
    assert_data_command(&mut first_receivers, 0);
    assert!(try_recv_reliable_path_command(&mut second_receivers).is_none());
    assert!(try_recv_reliable_path_command(&mut fixture.initial_receivers).is_none());
}

#[test]
fn control_frame_uses_the_same_live_target_validation() {
    let mut fixture = switchable_fixture();
    let frame = Frame::StreamMaxData {
        stream_id: StreamId(23),
        max_offset: 64 * 1024,
    };

    assert_eq!(
        emit_response_control_frame(&fixture.stream, frame).expect("emit control"),
        Some(fixture.initial)
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.initial_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamMaxData { .. }))
    ));
}
