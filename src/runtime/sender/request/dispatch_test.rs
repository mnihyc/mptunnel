use super::emit_request_frame_with_mode;
use crate::mux::MuxLimits;
use crate::protocol::{Frame, PathId, SessionId, StreamId, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommand, reliable_path_command_channels, try_recv_reliable_path_command,
    try_recv_reliable_path_priority_command,
};
use crate::runtime::sender::work::CarrierEmitMode;
use crate::runtime::stream::response::ResponseStreamBinding;
use crate::runtime::stream::{
    FixedReliablePathOutput, ReliablePathStreamHandle, ReliablePathStreamOutput,
};
use crate::scheduler::FlowLane;

fn request_handle(output: ReliablePathStreamOutput) -> ReliablePathStreamHandle {
    ReliablePathStreamHandle {
        stream_id: StreamId(7),
        max_offset: 64 * 1024,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: 16 * 1024,
        output,
    }
}

#[test]
fn request_dispatch_preserves_classified_and_stream_ordered_queues() {
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let fixed = FixedReliablePathOutput::new(
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        MuxLimits::default(),
    );
    let handle = request_handle(ReliablePathStreamOutput::Fixed(fixed));

    emit_request_frame_with_mode(
        &handle,
        Frame::Ping { nonce: 1 },
        FlowLane::Control,
        CarrierEmitMode::Classified,
    )
    .expect("classified control uses the priority queue");
    assert!(matches!(
        emit_request_frame_with_mode(
            &handle,
            Frame::Ping { nonce: 2 },
            FlowLane::Control,
            CarrierEmitMode::Classified,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    emit_request_frame_with_mode(
        &handle,
        Frame::Ping { nonce: 3 },
        FlowLane::Control,
        CarrierEmitMode::StreamOrdered,
    )
    .expect("stream-ordered control uses the data queue");

    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: 1 }))
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: 3 }))
    ));
}

#[test]
fn request_dispatch_rejects_switchable_response_output() {
    let (commands, _receivers) = reliable_path_command_channels(1);
    let binding = ResponseStreamBinding::new(
        SessionId(9),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        FlowLane::Throughput,
    );
    let handle = request_handle(ReliablePathStreamOutput::Switchable(binding));

    assert!(matches!(
        emit_request_frame_with_mode(
            &handle,
            Frame::Ping { nonce: 1 },
            FlowLane::Control,
            CarrierEmitMode::Classified,
        ),
        Err(RuntimeError::Protocol("request relay path is not fixed"))
    ));
}
