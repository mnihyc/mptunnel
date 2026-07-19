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
    Frame, PathId, PathUsage, ResetReason, SessionId, StreamDemandHint, StreamId, TargetAddr,
    UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::node::server::{ServerIdentityRuntime, new_identity_runtime};
use crate::runtime::path::commands::{recv_reliable_path_command, reliable_path_command_channels};
use crate::runtime::path::{PathProofObservation, ServerLocalPathProperties};
use crate::runtime::peer_status::PeerStatusBroker;
use crate::scheduler::TrafficClass;
use crate::transport::encrypted::{EncryptedFramedStream, EncryptedFramedTransportError, PeerRole};
use bytes::Bytes;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

#[tokio::test]
async fn server_tcp_path_input_frame_bypasses_queued_bulk_output() {
    let (tx, mut commands_rx) = reliable_path_command_channels(1);
    tx.try_enqueue_admitted_frame(
        Frame::StreamData {
            stream_id: StreamId(10),
            offset: 0,
            payload: Bytes::from_static(b"bulk"),
        },
        TrafficClass::Throughput,
    )
    .expect("fill bulk output command queue");
    let (frame_tx, mut path_frames) = mpsc::channel(1);
    let broker = PeerStatusBroker::new(false);
    let mut peer_status = broker.register(SessionId(1));
    frame_tx
        .send(Ok(Frame::Ping { nonce: 7 }))
        .await
        .expect("queue inbound ping");

    match recv_server_tcp_path_event(
        &mut path_frames,
        &mut commands_rx,
        &mut peer_status,
        Some(std::time::Instant::now()),
    )
    .await
    .expect("server path event")
    .expect("event")
    {
        ServerTcpPathEvent::Frame(Frame::Ping { nonce }) => assert_eq!(nonce, 7),
        _ => panic!("expected inbound frame before queued bulk output"),
    }
}

#[tokio::test]
async fn server_tcp_peer_eof_terminates_carrier_cleanly() {
    let (_commands_tx, mut commands_rx) = reliable_path_command_channels(1);
    let (frame_tx, mut path_frames) = mpsc::channel(1);
    let broker = PeerStatusBroker::new(false);
    let mut peer_status = broker.register(SessionId(1));
    frame_tx
        .send(Err(EncryptedFramedTransportError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "peer closed",
        ))))
        .await
        .expect("queue peer close");

    assert!(
        recv_server_tcp_path_event(&mut path_frames, &mut commands_rx, &mut peer_status, None)
            .await
            .expect("normal peer close")
            .is_none()
    );
}

#[tokio::test]
async fn server_tcp_native_sender_deadline_wakes_an_idle_actor() {
    let (_commands_tx, mut commands_rx) = reliable_path_command_channels(1);
    let (_frame_tx, mut path_frames) = mpsc::channel(1);
    let broker = PeerStatusBroker::new(false);
    let mut peer_status = broker.register(SessionId(1));
    let deadline = std::time::Instant::now();

    let event = tokio::time::timeout(
        Duration::from_secs(1),
        recv_server_tcp_path_event(
            &mut path_frames,
            &mut commands_rx,
            &mut peer_status,
            Some(deadline),
        ),
    )
    .await
    .expect("native sender observation deadline")
    .expect("server path event")
    .expect("open carrier");

    assert!(matches!(event, ServerTcpPathEvent::SenderObservationDue));
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
    server_framed
        .write_frame(&Frame::Pong { nonce: 1 })
        .await
        .expect("confirm encrypted carrier");
    server_framed
        .flush()
        .await
        .expect("flush carrier confirmation");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), client_framed.read_frame())
            .await
            .expect("carrier confirmation timeout")
            .expect("read carrier confirmation"),
        Frame::Pong { nonce: 1 }
    );
    let (_server_reader, server_writer) = server_framed.split().expect("split server carrier");

    let session_id = SessionId(202);
    let path_id = PathId(0);
    let path_registration = context.reliable_streams.register_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        path_id,
        ServerLocalPathProperties::default(),
    );
    let proof_elapsed = Duration::from_millis(1);
    context.reliable_streams.record_path_proof_success(
        &path_registration,
        PathProofObservation {
            proof_id: 1,
            elapsed: proof_elapsed,
            sent_at: Instant::now()
                .checked_sub(proof_elapsed)
                .expect("test validation instant"),
        },
    );
    let (commands_tx, commands_rx) = reliable_path_command_channels(8);
    let commands_for_streams = commands_tx.clone();
    let (_path_frames_tx, path_frames) = mpsc::channel(1);
    let evidence = ServerTcpEvidenceState::new(None, None, context.mux_limits);
    let peer_status = context.peer_status.register(session_id);
    let mut session = ServerTcpPathSession::new(ServerTcpPathAdmission {
        context,
        session_id,
        path_id,
        path_registration,
        writer: ServerTcpWriter::new(server_writer),
        path_frames,
        commands_tx,
        commands_rx,
        evidence,
        peer_status,
    });
    session
        .handle_frame(Frame::PathStatus {
            path_id,
            sequence: 1,
            usage: PathUsage::Backup,
        })
        .await
        .expect("record exact TCP path usage advertisement");
    assert!(matches!(
        session
            .handle_frame(Frame::PathStatus {
                path_id: PathId(path_id.0 + 1),
                sequence: 2,
                usage: PathUsage::Available,
            })
            .await,
        Err(RuntimeError::Protocol(
            "TCP path usage advertisement path mismatch"
        ))
    ));
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
                    stream_id,
                    target.clone(),
                    StreamDemandHint::throughput(),
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
            TrafficClass::Throughput,
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
    assert_eq!(commands_for_streams.writer_pending_bytes(), 0);

    let sibling_frame = Frame::StreamMaxData {
        stream_id: sibling_stream_id,
        max_offset: 4096,
    };
    commands_for_streams
        .send_stream_ordered_frame(sibling_frame.clone(), TrafficClass::Throughput)
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
    assert_eq!(commands_for_streams.writer_pending_bytes(), 0);

    let detach_context = session.context.clone();
    session
        .streams
        .detach(
            &detach_context,
            &session.path_registration,
            sibling_stream_id,
        )
        .expect("detach sibling TCP stream");
    assert!(
        session.streams.is_empty(),
        "the terminal command must have removed only its own TCP attachment"
    );
}
