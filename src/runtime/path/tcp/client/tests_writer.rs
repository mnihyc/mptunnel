use super::{
    ClientTcpWriteFrameRoute, ensure_client_tcp_transaction_closed,
    release_client_tcp_writer_transaction_charge, try_route_client_tcp_frame_during_write,
};
use crate::protocol::frame::reliable_path_frame_pacing_bytes;
use crate::protocol::{CloseReason, Frame, OffsetRange, ResetReason, StreamId};
use crate::runtime::path::commands::{
    ClientTcpOpenAttemptId, reliable_path_command_channels, reliable_path_command_pending_bytes,
    try_recv_reliable_path_command,
};
use crate::runtime::path::tcp::client::datagram::ClientTcpDatagramState;
use crate::runtime::path::tcp::client::stream::ClientTcpPathStreamState;
use crate::runtime::recent_ids::RecentIdCache;
use crate::scheduler::TrafficClass;
use bytes::Bytes;
use std::collections::HashMap;
use tokio::sync::mpsc;

#[test]
fn client_tcp_pending_frame_charge_survives_until_writer_commit_or_receiver_drop() {
    let frame = Frame::StreamData {
        stream_id: StreamId(80),
        offset: 0,
        payload: Bytes::from_static(b"protected writer batch"),
    };
    let expected_bytes = reliable_path_frame_pacing_bytes(&frame);
    let (commands, mut receivers) = reliable_path_command_channels(2);
    commands
        .try_reserve_admitted_frame(frame.clone(), TrafficClass::Throughput)
        .expect("reserve client TCP frame")
        .commit();
    let command = try_recv_reliable_path_command(&mut receivers).expect("dequeue client frame");
    let mut writer_pending_bytes = reliable_path_command_pending_bytes(&command);
    assert_eq!(writer_pending_bytes, expected_bytes);
    assert_eq!(commands.pending_bytes(), expected_bytes as u64);
    assert_eq!(commands.writer_pending_bytes(), expected_bytes as u64);

    release_client_tcp_writer_transaction_charge(&receivers, &mut writer_pending_bytes);
    assert_eq!(commands.pending_bytes(), 0);
    assert_eq!(commands.writer_pending_bytes(), 0);

    commands
        .try_reserve_admitted_frame(frame, TrafficClass::Throughput)
        .expect("reserve failed-write client TCP frame")
        .commit();
    let failed_command =
        try_recv_reliable_path_command(&mut receivers).expect("dequeue failed-write frame");
    let failed_writer_pending_bytes = reliable_path_command_pending_bytes(&failed_command);
    assert_eq!(failed_writer_pending_bytes, expected_bytes);
    assert_eq!(commands.pending_bytes(), expected_bytes as u64);
    assert_eq!(commands.writer_pending_bytes(), expected_bytes as u64);

    drop(receivers);
    assert_eq!(commands.pending_bytes(), 0);
    assert_eq!(commands.writer_pending_bytes(), 0);
}

#[test]
fn client_tcp_stale_probe_normal_exit_requires_prior_transaction_commit() {
    let frame = Frame::Ping { nonce: 80 };
    assert!(matches!(
        ensure_client_tcp_transaction_closed(std::slice::from_ref(&frame), 64),
        Err(crate::runtime::error::RuntimeError::Protocol(
            "client TCP writer returned with an uncommitted transaction"
        ))
    ));
    assert!(ensure_client_tcp_transaction_closed(&[], 64).is_err());
    assert!(ensure_client_tcp_transaction_closed(&[], 0).is_ok());
}

#[test]
fn tcp_write_interlock_routes_ready_feedback_and_stops_at_backpressure() {
    let stream_id = StreamId(81);
    let (frames, mut frame_rx) = mpsc::channel(1);
    let mut streams = HashMap::from([(
        stream_id,
        ClientTcpPathStreamState {
            open_attempt_id: ClientTcpOpenAttemptId(3),
            frames,
            pending_open: None,
        },
    )]);
    let mut closed_streams = RecentIdCache::new(4);
    let mut datagrams = ClientTcpDatagramState::new(4, 4);
    let ack = Frame::StreamAck {
        stream_id,
        complete: false,
        ranges: vec![OffsetRange { start: 0, end: 64 }],
    };

    assert!(matches!(
        try_route_client_tcp_frame_during_write(
            ack.clone(),
            &mut streams,
            &mut closed_streams,
            &mut datagrams,
        ),
        Ok(ClientTcpWriteFrameRoute::Routed)
    ));
    assert!(matches!(
        frame_rx.try_recv().expect("routed ACK"),
        Ok(frame) if frame == ack
    ));

    streams
        .get(&stream_id)
        .expect("stream")
        .frames
        .try_send(Ok(Frame::Ping { nonce: 1 }))
        .expect("fill delivery queue");
    let response = Frame::StreamData {
        stream_id,
        offset: 0,
        payload: Bytes::from_static(b"response"),
    };
    assert!(matches!(
        try_route_client_tcp_frame_during_write(
            response.clone(),
            &mut streams,
            &mut closed_streams,
            &mut datagrams,
        ),
        Ok(ClientTcpWriteFrameRoute::Barrier(frame)) if frame == response
    ));
    assert!(matches!(
        try_route_client_tcp_frame_during_write(
            Frame::SessionClose {
                reason: CloseReason::Normal,
            },
            &mut streams,
            &mut closed_streams,
            &mut datagrams,
        ),
        Ok(ClientTcpWriteFrameRoute::Barrier(
            Frame::SessionClose { .. }
        ))
    ));

    assert!(matches!(
        frame_rx.try_recv().expect("queued frame"),
        Ok(Frame::Ping { nonce: 1 })
    ));
    let fin = Frame::StreamFin {
        stream_id,
        final_offset: 8,
    };
    assert!(matches!(
        try_route_client_tcp_frame_during_write(
            fin.clone(),
            &mut streams,
            &mut closed_streams,
            &mut datagrams,
        ),
        Ok(ClientTcpWriteFrameRoute::Routed)
    ));
    assert!(matches!(
        frame_rx.try_recv().expect("routed FIN"),
        Ok(frame) if frame == fin
    ));
    assert!(
        streams.contains_key(&stream_id),
        "FIN must not retire the route needed by final-offset repair"
    );
    assert!(!closed_streams.contains(&stream_id));

    let repair = Frame::StreamData {
        stream_id,
        offset: 4,
        payload: Bytes::from_static(b"tail"),
    };
    assert!(matches!(
        try_route_client_tcp_frame_during_write(
            repair.clone(),
            &mut streams,
            &mut closed_streams,
            &mut datagrams,
        ),
        Ok(ClientTcpWriteFrameRoute::Routed)
    ));
    assert!(matches!(
        frame_rx.try_recv().expect("post-FIN repair"),
        Ok(frame) if frame == repair
    ));

    let reset = Frame::StreamReset {
        stream_id,
        reason: ResetReason::RemoteClosed,
    };
    assert!(matches!(
        try_route_client_tcp_frame_during_write(
            reset.clone(),
            &mut streams,
            &mut closed_streams,
            &mut datagrams,
        ),
        Ok(ClientTcpWriteFrameRoute::Routed)
    ));
    assert!(matches!(
        frame_rx.try_recv().expect("routed reset"),
        Ok(frame) if frame == reset
    ));
    assert!(!streams.contains_key(&stream_id));
    assert!(closed_streams.contains(&stream_id));
}
