use super::*;
use crate::mux::MuxLimits;
use crate::protocol::{Frame, PathId, SessionId, StreamId, UnderlayProtocol};
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, reliable_path_command_channels,
    try_recv_reliable_path_command, try_recv_reliable_path_priority_command,
};
use crate::runtime::sender::response::multipath::plan_response_data_dispatch;
use crate::runtime::stream::response::{ResponseStreamAttachOutcome, ResponseStreamBinding};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use bytes::Bytes;
use std::sync::Arc;
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
        frames,
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
            .original_flight_keys_overlapping_frame(&frame),
        vec![fixture.initial]
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
            .original_flight_keys_overlapping_frame(&frame)
            .is_empty()
    );
    assert!(try_recv_reliable_path_command(&mut fixture.initial_receivers).is_none());
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
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut alternate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
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
    let mut keys = fixture.binding.flight_keys_overlapping_frame(&frame);
    keys.sort_by_key(|key| key.path_id.0);
    assert_eq!(keys, vec![fixture.initial, alternate]);
    assert_data_command(&mut alternate_receivers, 0);
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
