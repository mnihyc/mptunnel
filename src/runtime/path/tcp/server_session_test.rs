use super::super::server_evidence::ServerTcpEvidenceState;
use super::super::server_writer::ServerTcpWriter;
use super::{
    ServerTcpPathAdmission, ServerTcpPathEvent, ServerTcpPathSession, ServerTcpSessionDisposition,
    recv_server_tcp_path_event,
};
use crate::config::{
    DEFAULT_OUTBOUND_CONNECT_TIMEOUT, MppPerformanceConfig, ResourceLimits, ServerSecurityConfig,
    SharedSecret,
};
use crate::outbound::OutboundConfig;
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
use crate::transport::encrypted::{EncryptedFramedStream, EncryptedFramedTransportError};
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

async fn server_tcp_test_session(
    session_id: SessionId,
    path_id: PathId,
) -> (
    ServerTcpPathSession,
    EncryptedFramedStream<TcpStream>,
    crate::runtime::path::commands::ReliablePathCommandSender,
    mpsc::Sender<Result<Frame, EncryptedFramedTransportError>>,
    crate::runtime::relay::ServerReliableRelayService,
) {
    let security = ServerSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("test shared secret"),
    );
    let ServerIdentityRuntime {
        paths: context,
        reliable_relay,
    } = new_identity_runtime(
        Vec::new(),
        OutboundConfig::Direct,
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
    let client_tls = crate::transport::encrypted::test_client_tls_config();
    let server_tls = &context.tls;
    let (client_framed, server_framed) = tokio::join!(
        EncryptedFramedStream::connect(client_socket, &client_tls, context.codec_limits),
        EncryptedFramedStream::accept(server_socket, server_tls, context.codec_limits),
    );
    let mut client_framed = client_framed.expect("client TLS carrier");
    let mut server_framed = server_framed.expect("server TLS carrier");
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

    let path_registration = context.reliable_streams.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        path_id,
        ServerLocalPathProperties::default(),
    );
    let (commands_tx, commands_rx) = reliable_path_command_channels(8);
    let commands = commands_tx.clone();
    let (path_frames_tx, path_frames) = mpsc::channel(1);
    let evidence = ServerTcpEvidenceState::new(None, None, context.mux_limits);
    let peer_status = context.peer_status.register(session_id);
    (
        ServerTcpPathSession::new(ServerTcpPathAdmission {
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
        }),
        client_framed,
        commands,
        path_frames_tx,
        reliable_relay,
    )
}

#[tokio::test]
async fn server_tcp_terminal_reset_precedes_detach_and_preserves_shared_session() {
    let session_id = SessionId(202);
    let path_id = PathId(0);
    let (mut session, mut client_framed, commands_for_streams, _path_frames_tx, _reliable_relay) =
        server_tcp_test_session(session_id, path_id).await;
    let proof_elapsed = Duration::from_millis(1);
    session.context.reliable_streams.record_path_proof_success(
        &session.path_registration,
        PathProofObservation {
            proof_id: 1,
            elapsed: proof_elapsed,
            sent_at: Instant::now()
                .checked_sub(proof_elapsed)
                .expect("test validation instant"),
        },
    );
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

#[tokio::test]
async fn server_tcp_path_close_is_the_aggregate_responder_suffix() {
    let session_id = SessionId(203);
    let path_id = PathId(0);
    let (session, mut client_framed, commands, path_frames, _reliable_relay) =
        server_tcp_test_session(session_id, path_id).await;
    commands
        .try_enqueue_admitted_frame(
            Frame::PathProofData {
                path_id,
                proof_id: 1,
                payload: Bytes::from_static(b"proof"),
            },
            TrafficClass::Control,
        )
        .expect("queue proof before path drain");
    let final_control = Frame::Pong { nonce: 99 };
    commands
        .try_enqueue_admitted_frame(final_control.clone(), TrafficClass::Control)
        .expect("queue product control before path drain");
    path_frames
        .send(Ok(Frame::PathDrain { path_id }))
        .await
        .expect("deliver ordered PATH_DRAIN");

    tokio::time::timeout(Duration::from_secs(5), session.run())
        .await
        .expect("aggregate TCP path drain timeout")
        .expect("complete aggregate TCP path drain");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), client_framed.read_frame())
            .await
            .expect("final TCP control timeout")
            .expect("read final TCP control"),
        final_control,
        "ordinary accepted responder work must survive the drain fence"
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), client_framed.read_frame())
            .await
            .expect("TCP PATH_CLOSE timeout")
            .expect("read TCP PATH_CLOSE"),
        Frame::PathClose {
            path_id,
            reason: crate::protocol::CloseReason::Normal,
        },
        "measurement work is canceled and PATH_CLOSE is serialized last"
    );
}
