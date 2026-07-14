use super::super::server_evidence::ServerTcpEvidenceState;
use super::super::server_writer::ServerTcpWriter;
use super::{
    ServerTcpPathAdmission, ServerTcpPathEvent, ServerTcpPathSession, ServerTcpSessionDisposition,
    recv_server_tcp_path_event,
};
use crate::config::{
    DEFAULT_OUTBOUND_CONNECT_TIMEOUT, MppPerformanceConfig, ResourceLimits, SecurityConfig,
    SharedSecret,
};
use crate::outbound::{DnsConfig, OutboundConfig};
use crate::protocol::{
    Frame, PathCapabilities, PathId, ResetReason, SessionId, StreamDemandHint, StreamFlags,
    StreamId, StreamOpenRole, TargetAddr, UnderlayProtocol,
};
use crate::runtime::node::server::{ServerIdentityRuntime, new_identity_runtime};
use crate::runtime::path::commands::{recv_reliable_path_command, reliable_path_command_channels};
use crate::scheduler::FlowLane;
use crate::transport::encrypted::{EncryptedFramedStream, PeerRole};
use bytes::Bytes;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

#[tokio::test]
async fn server_tcp_path_input_frame_bypasses_queued_bulk_output() {
    let (tx, mut commands_rx) = reliable_path_command_channels(1);
    tx.try_enqueue_admitted_frame(
        Frame::StreamData {
            stream_id: StreamId(10),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"bulk"),
        },
        FlowLane::Throughput,
    )
    .expect("fill bulk output command queue");
    let (frame_tx, mut path_frames) = mpsc::channel(1);
    frame_tx
        .send(Ok(Frame::Ping { nonce: 7 }))
        .await
        .expect("queue inbound ping");

    match recv_server_tcp_path_event(&mut path_frames, &mut commands_rx)
        .await
        .expect("server path event")
        .expect("event")
    {
        ServerTcpPathEvent::Frame(Frame::Ping { nonce }) => assert_eq!(nonce, 7),
        _ => panic!("expected inbound frame before queued bulk output"),
    }
}

#[tokio::test]
async fn server_tcp_terminal_reset_precedes_detach_and_preserves_shared_session() {
    let security = SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("test shared secret"),
    );
    let ServerIdentityRuntime {
        paths: context,
        reliable_relay: _reliable_relay,
    } = new_identity_runtime(
        Vec::new(),
        OutboundConfig::Direct,
        DnsConfig::default(),
        DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        security,
        MppPerformanceConfig::default(),
        ResourceLimits::default(),
    );
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test TCP carrier");
    let client_socket = TcpStream::connect(listener.local_addr().expect("test carrier address"))
        .await
        .expect("connect test TCP carrier");
    let (server_socket, _) = listener.accept().await.expect("accept test TCP carrier");
    let mut client_framed = EncryptedFramedStream::with_cipher_suite(
        client_socket,
        context.security.secret.as_bytes(),
        PeerRole::Client,
        context.codec_limits,
        context.security.cipher,
    )
    .expect("client encrypted carrier");
    let mut server_framed = EncryptedFramedStream::with_cipher_suite(
        server_socket,
        context.security.secret.as_bytes(),
        PeerRole::Server,
        context.codec_limits,
        context.security.cipher,
    )
    .expect("server encrypted carrier");
    client_framed
        .write_frame(&Frame::Ping { nonce: 1 })
        .await
        .expect("initialize encrypted carrier");
    client_framed.flush().await.expect("flush carrier opener");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), server_framed.read_frame())
            .await
            .expect("carrier opener timeout")
            .expect("read carrier opener"),
        Frame::Ping { nonce: 1 }
    );
    let (_server_reader, server_writer) = server_framed.split().expect("split server carrier");

    let session_id = SessionId(202);
    let path_id = PathId(0);
    let path_registration =
        context
            .reliable_streams
            .register_carrier_path(session_id, UnderlayProtocol::Tcp, path_id);
    let (commands_tx, commands_rx) = reliable_path_command_channels(8);
    let commands_for_streams = commands_tx.clone();
    let (_path_frames_tx, path_frames) = mpsc::channel(1);
    let evidence = ServerTcpEvidenceState::new(None, None, context.mux_limits);
    let mut session = ServerTcpPathSession::new(ServerTcpPathAdmission {
        context,
        session_id,
        path_id,
        path_capabilities: PathCapabilities::default(),
        path_registration,
        writer: ServerTcpWriter::new(server_writer),
        path_frames,
        commands_tx,
        commands_rx,
        evidence,
    });
    let terminal_stream_id = StreamId(301);
    let sibling_stream_id = StreamId(302);
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let open_context = session.context.clone();
    let open_registration = session.path_registration.clone();
    let open_commands = session.commands_tx.clone();
    for stream_id in [terminal_stream_id, sibling_stream_id] {
        assert!(
            session
                .streams
                .open(
                    &open_context,
                    &open_registration,
                    &open_commands,
                    session_id,
                    path_id,
                    PathCapabilities::default(),
                    None,
                    stream_id,
                    target.clone(),
                    StreamDemandHint::throughput(),
                    StreamOpenRole::Active,
                )
                .await
                .expect("open shared TCP response stream")
                .is_none()
        );
    }

    commands_for_streams
        .send_stream_ordered_reset_and_close(
            terminal_stream_id,
            ResetReason::Refused,
            FlowLane::Throughput,
        )
        .await
        .expect("queue terminal TCP command");
    let terminal = recv_reliable_path_command(&mut session.commands_rx)
        .await
        .expect("dequeue terminal TCP command");
    assert!(matches!(
        session
            .drain_commands(terminal)
            .await
            .expect("write terminal TCP command"),
        ServerTcpSessionDisposition::Continue
    ));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), client_framed.read_frame())
            .await
            .expect("terminal TCP reset timeout")
            .expect("read terminal TCP reset"),
        Frame::StreamReset {
            stream_id: terminal_stream_id,
            reason: ResetReason::Refused,
        }
    );
    assert!(
        !session.streams.is_empty(),
        "terminal detach must preserve the sibling stream on the shared TCP session"
    );
    assert_eq!(commands_for_streams.pending_bytes(), 0);

    let sibling_frame = Frame::StreamMaxData {
        stream_id: sibling_stream_id,
        max_offset: 4096,
    };
    commands_for_streams
        .send_stream_ordered_frame(sibling_frame.clone(), FlowLane::Throughput)
        .await
        .expect("queue sibling TCP output");
    let sibling_command = recv_reliable_path_command(&mut session.commands_rx)
        .await
        .expect("dequeue sibling TCP output");
    assert!(matches!(
        session
            .drain_commands(sibling_command)
            .await
            .expect("write sibling TCP output"),
        ServerTcpSessionDisposition::Continue
    ));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), client_framed.read_frame())
            .await
            .expect("sibling TCP output timeout")
            .expect("read sibling TCP output"),
        sibling_frame
    );

    let detach_context = session.context.clone();
    let detach_commands = session.commands_tx.clone();
    session.streams.detach(
        &detach_context,
        &detach_commands,
        session_id,
        path_id,
        sibling_stream_id,
    );
    assert!(
        session.streams.is_empty(),
        "the terminal command must have removed only its own TCP attachment"
    );
}
