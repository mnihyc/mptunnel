use super::{
    ClientTcpOpenCancellation, ClientTcpPathStreamState, ClientTcpPendingOpen,
    handle_client_tcp_stream_detach, remove_matching_client_tcp_open,
    route_client_tcp_lifecycle_frame, route_client_tcp_stream_frame,
};
use crate::protocol::{Frame, StreamId};
use crate::runtime::path::commands::{
    ClientTcpOpenAttemptId, ReliablePathCommand, recv_reliable_path_command,
    reliable_path_command_channels,
};
use crate::runtime::recent_ids::RecentIdCache;
use bytes::Bytes;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

#[tokio::test]
async fn canceled_tcp_open_queues_generation_scoped_cancellation() {
    let stream_id = StreamId(91);
    let attempt_id = ClientTcpOpenAttemptId(17);
    let (commands, mut receivers) = reliable_path_command_channels(4);
    drop(ClientTcpOpenCancellation::new(
        commands, stream_id, attempt_id,
    ));

    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(1),
            recv_reliable_path_command(&mut receivers),
        )
        .await
        .expect("detach command deadline"),
        Some(ReliablePathCommand::CancelTcpOpen {
            stream_id: id,
            attempt_id: id_attempt,
        }) if id == stream_id && id_attempt == attempt_id
    ));
}

#[tokio::test]
async fn tcp_detach_distinguishes_pending_refusal_from_live_retirement() {
    let pending_id = StreamId(95);
    let (pending_frames, pending_frame_rx) = mpsc::channel(1);
    let (session_commands, _session_receivers) = reliable_path_command_channels(1);
    let (response, response_rx) = oneshot::channel();
    let mut streams = HashMap::from([(
        pending_id,
        ClientTcpPathStreamState {
            open_attempt_id: ClientTcpOpenAttemptId(30),
            frames: pending_frames,
            pending_open: Some(ClientTcpPendingOpen {
                response,
                frames: Some(pending_frame_rx),
                session_commands,
                lane: crate::scheduler::TrafficClass::Throughput,
                open_deadline: tokio::time::Instant::now() + Duration::from_secs(1),
            }),
        },
    )]);
    let mut closed = RecentIdCache::new(8);

    handle_client_tcp_stream_detach(&mut streams, &mut closed, pending_id).await;

    assert!(matches!(
        response_rx.await.expect("pending TCP open response"),
        crate::runtime::path::commands::ClientTcpOpenResponse::FailedAfterOpen(
            crate::runtime::error::RuntimeError::ReliablePathAttachmentRefused
        )
    ));
    assert!(closed.contains(&pending_id));

    let live_id = StreamId(96);
    let (live_frames, mut live_frame_rx) = mpsc::channel(1);
    streams.insert(
        live_id,
        ClientTcpPathStreamState {
            open_attempt_id: ClientTcpOpenAttemptId(31),
            frames: live_frames,
            pending_open: None,
        },
    );
    handle_client_tcp_stream_detach(&mut streams, &mut closed, live_id).await;
    assert!(matches!(
        live_frame_rx.recv().await,
        Some(Err(
            crate::runtime::error::RuntimeError::ReliablePathRetired
        ))
    ));
    assert!(closed.contains(&live_id));
}

#[test]
fn stale_tcp_open_cancellation_cannot_remove_current_generation() {
    let stream_id = StreamId(92);
    let current_attempt = ClientTcpOpenAttemptId(23);
    let (frames, _frame_rx) = mpsc::channel(1);
    let mut streams = HashMap::from([(
        stream_id,
        ClientTcpPathStreamState {
            open_attempt_id: current_attempt,
            frames,
            pending_open: None,
        },
    )]);

    assert!(
        remove_matching_client_tcp_open(&mut streams, stream_id, ClientTcpOpenAttemptId(22),)
            .is_none()
    );
    assert_eq!(
        streams.get(&stream_id).map(|state| state.open_attempt_id),
        Some(current_attempt),
        "cleanup from an older opener must preserve the current stream owner"
    );
    assert!(remove_matching_client_tcp_open(&mut streams, stream_id, current_attempt).is_some());
    assert!(!streams.contains_key(&stream_id));
}

#[tokio::test]
async fn client_tcp_path_ignores_late_frames_for_recently_closed_stream() {
    let stream_id = StreamId(7);
    let (frames_tx, frames_rx) = mpsc::channel(1);
    let mut streams = HashMap::new();
    streams.insert(
        stream_id,
        ClientTcpPathStreamState {
            open_attempt_id: ClientTcpOpenAttemptId(1),
            frames: frames_tx,
            pending_open: None,
        },
    );
    let mut closed_streams = RecentIdCache::new(8);
    drop(frames_rx);

    route_client_tcp_lifecycle_frame(
        &mut streams,
        &mut closed_streams,
        stream_id,
        Frame::StreamFin {
            stream_id,
            final_offset: 0,
        },
    )
    .await
    .expect("receiver close should mark stream drained");
    assert!(!streams.contains_key(&stream_id));
    assert!(closed_streams.contains(&stream_id));

    route_client_tcp_stream_frame(
        &mut streams,
        &mut closed_streams,
        stream_id,
        Frame::StreamAck {
            stream_id,
            complete: true,
            ranges: Vec::new(),
        },
    )
    .await
    .expect("late frame for closed stream should be ignored");

    let unknown = StreamId(99);
    route_client_tcp_stream_frame(
        &mut streams,
        &mut closed_streams,
        unknown,
        Frame::StreamFin {
            stream_id: unknown,
            final_offset: 0,
        },
    )
    .await
    .expect("unknown product stream frame should be dropped at product layer");
    assert!(closed_streams.contains(&unknown));
}

#[tokio::test]
async fn client_tcp_path_routes_inflight_receive_frames_to_live_stream() {
    let stream_id = StreamId(70);
    let (frames_tx, mut frames_rx) = mpsc::channel(4);
    let mut streams = HashMap::new();
    streams.insert(
        stream_id,
        ClientTcpPathStreamState {
            open_attempt_id: ClientTcpOpenAttemptId(2),
            frames: frames_tx,
            pending_open: None,
        },
    );
    let mut closed_streams = RecentIdCache::new(8);

    route_client_tcp_stream_frame(
        &mut streams,
        &mut closed_streams,
        stream_id,
        Frame::StreamData {
            stream_id,
            offset: 0,
            payload: Bytes::from_static(b"inflight"),
        },
    )
    .await
    .expect("live stream should route in-flight data");

    match frames_rx
        .recv()
        .await
        .expect("in-flight frame")
        .expect("frame")
    {
        Frame::StreamData {
            stream_id: routed,
            payload,
            ..
        } => {
            assert_eq!(routed, stream_id);
            assert_eq!(&payload[..], b"inflight");
        }
        other => panic!("expected routed stream data, got {other:?}"),
    }
    assert!(
        streams.contains_key(&stream_id),
        "routing a frame must preserve the live stream owner"
    );

    route_client_tcp_lifecycle_frame(
        &mut streams,
        &mut closed_streams,
        stream_id,
        Frame::StreamFin {
            stream_id,
            final_offset: 8,
        },
    )
    .await
    .expect("FIN routes before cleanup");
    assert!(
        streams.contains_key(&stream_id),
        "FIN must preserve the attachment until the relay explicitly closes it"
    );
    assert!(!closed_streams.contains(&stream_id));
    assert!(matches!(
        frames_rx.recv().await.expect("routed FIN").expect("frame"),
        Frame::StreamFin {
            final_offset: 8,
            ..
        }
    ));

    route_client_tcp_stream_frame(
        &mut streams,
        &mut closed_streams,
        stream_id,
        Frame::StreamData {
            stream_id,
            offset: 4,
            payload: Bytes::from_static(b"tail"),
        },
    )
    .await
    .expect("repair below the final offset still routes");
    assert!(matches!(
        frames_rx
            .recv()
            .await
            .expect("routed post-FIN repair")
            .expect("frame"),
        Frame::StreamData { offset: 4, payload, .. } if &payload[..] == b"tail"
    ));
}

#[tokio::test]
async fn client_tcp_idle_writer_routes_both_requalification_frames() {
    let stream_id = StreamId(71);
    let (frames_tx, mut frames_rx) = mpsc::channel(4);
    let mut streams = HashMap::from([(
        stream_id,
        ClientTcpPathStreamState {
            open_attempt_id: ClientTcpOpenAttemptId(3),
            frames: frames_tx,
            pending_open: None,
        },
    )]);
    let mut closed_streams = RecentIdCache::new(8);
    let probe = Frame::StreamRequalifyData {
        stream_id,
        probe_id: 51,
        offset: 4096,
        payload: Bytes::from_static(b"idle-probe"),
    };
    let ack = Frame::StreamRequalifyAck {
        stream_id,
        probe_id: 52,
        offset: 8192,
        payload_bytes: 1024,
    };

    for frame in [probe.clone(), ack.clone()] {
        let stream_id = match &frame {
            Frame::StreamRequalifyData { stream_id, .. }
            | Frame::StreamRequalifyAck { stream_id, .. } => *stream_id,
            _ => unreachable!("test frames are requalification frames"),
        };
        route_client_tcp_stream_frame(&mut streams, &mut closed_streams, stream_id, frame)
            .await
            .expect("route requalification frame");
    }
    assert_eq!(
        frames_rx.recv().await.expect("probe route").expect("probe"),
        probe,
    );
    assert_eq!(
        frames_rx.recv().await.expect("ACK route").expect("ACK"),
        ack
    );
}
