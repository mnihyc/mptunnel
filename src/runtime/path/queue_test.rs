use super::{
    ReliablePathCommand, recv_reliable_path_command, reliable_path_command_channels,
    reliable_path_command_pending_bytes, reliable_path_effective_frame_lane,
    reliable_path_frame_uses_priority_queue, try_recv_reliable_path_command,
};
use crate::protocol::{
    DatagramFlowId, Frame, PathId, PathUsage, ResetReason, StreamDemandHint, StreamId, TargetAddr,
};
use crate::scheduler::TrafficClass;
use bytes::Bytes;

#[test]
fn control_and_ack_frames_never_use_throughput_lane() {
    let priority_frames = [
        (
            Frame::OpenStream {
                stream_id: StreamId(1),
                target: TargetAddr::Domain {
                    host: "example.test".to_string(),
                    port: 443,
                },
                demand: StreamDemandHint::throughput(),
            },
            TrafficClass::Control,
        ),
        (
            Frame::PathStatus {
                path_id: PathId(1),
                sequence: 4,
                usage: PathUsage::Backup,
            },
            TrafficClass::Control,
        ),
        (
            Frame::StreamAck {
                stream_id: StreamId(1),
                complete: false,
                ranges: vec![],
            },
            TrafficClass::Control,
        ),
        (
            Frame::StreamMaxData {
                stream_id: StreamId(1),
                max_offset: 1024,
            },
            TrafficClass::Control,
        ),
        (
            Frame::StreamFin {
                stream_id: StreamId(1),
                final_offset: 64,
            },
            TrafficClass::Control,
        ),
        (
            Frame::StreamReset {
                stream_id: StreamId(1),
                reason: ResetReason::RemoteClosed,
            },
            TrafficClass::Control,
        ),
        (
            Frame::StreamDetach {
                stream_id: StreamId(1),
            },
            TrafficClass::Control,
        ),
        (
            Frame::DatagramFeedback {
                flow_id: DatagramFlowId(1),
                received: vec![],
            },
            TrafficClass::RealtimeDatagram,
        ),
        (
            Frame::DatagramClose {
                flow_id: DatagramFlowId(1),
            },
            TrafficClass::Control,
        ),
    ];

    for (frame, expected_lane) in priority_frames {
        let effective_lane = reliable_path_effective_frame_lane(&frame, TrafficClass::Throughput);
        assert_eq!(effective_lane, expected_lane);
        assert!(reliable_path_frame_uses_priority_queue(effective_lane));
    }
}

#[tokio::test]
async fn terminal_reset_and_close_uses_one_ordered_queue_item() {
    let stream_id = StreamId(2);
    let (commands, mut receivers) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id,
                offset: 0,
                payload: Bytes::from_static(b"bulk"),
            },
            TrafficClass::Throughput,
        )
        .expect("fill ordered data queue");

    let (terminal_result, first) = tokio::join!(
        commands.send_stream_ordered_reset_and_close(
            stream_id,
            ResetReason::Refused,
            TrafficClass::Throughput,
        ),
        recv_reliable_path_command(&mut receivers),
    );
    assert!(
        terminal_result.is_ok(),
        "queue terminal transaction after prior data"
    );
    let first = first.expect("prior ordered data");
    assert!(matches!(
        &first,
        ReliablePathCommand::SendFrame(Frame::StreamData {
            stream_id: data_stream_id,
            ..
        }) if *data_stream_id == stream_id
    ));
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&first));

    let terminal = recv_reliable_path_command(&mut receivers)
        .await
        .expect("single terminal transaction");
    assert!(matches!(
        &terminal,
        ReliablePathCommand::ResetAndCloseStream {
            stream_id: reset_stream_id,
            reason: ResetReason::Refused,
        } if *reset_stream_id == stream_id
    ));
    assert_eq!(
        commands.pending_bytes(),
        u64::try_from(reliable_path_command_pending_bytes(&terminal))
            .expect("terminal pacing debt fits queue metrics")
    );
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&terminal));
    assert_eq!(commands.pending_bytes(), 0);
    assert!(try_recv_reliable_path_command(&mut receivers).is_none());
}

#[tokio::test]
async fn cancelling_waiting_terminal_reset_releases_queue_debt() {
    let stream_id = StreamId(3);
    let (commands, mut receivers) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id,
                offset: 0,
                payload: Bytes::from_static(b"bulk"),
            },
            TrafficClass::Throughput,
        )
        .expect("fill ordered data queue");
    let queued_bytes = commands.pending_bytes();
    let terminal_bytes = u64::try_from(reliable_path_command_pending_bytes(
        &ReliablePathCommand::ResetAndCloseStream {
            stream_id,
            reason: ResetReason::Refused,
        },
    ))
    .expect("terminal pacing debt fits queue metrics");

    let mut terminal_send = Box::pin(commands.send_stream_ordered_reset_and_close(
        stream_id,
        ResetReason::Refused,
        TrafficClass::Throughput,
    ));
    tokio::select! {
        biased;
        _ = terminal_send.as_mut() => panic!("full queue admitted terminal transaction"),
        _ = std::future::ready(()) => {}
    }
    assert_eq!(
        commands.pending_bytes(),
        queued_bytes.saturating_add(terminal_bytes)
    );

    drop(terminal_send);
    assert_eq!(commands.pending_bytes(), queued_bytes);
    let queued = recv_reliable_path_command(&mut receivers)
        .await
        .expect("prior ordered data");
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&queued));
    assert_eq!(commands.pending_bytes(), 0);
}
