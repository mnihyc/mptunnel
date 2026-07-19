use super::{
    ClientTcpOpenCancellation, ClientTcpPathStreamState, remove_matching_client_tcp_open,
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
