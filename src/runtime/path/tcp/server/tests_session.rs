use super::super::datagram::ServerTcpDatagramState;
use super::super::evidence::ServerTcpEvidenceState;
use super::super::writer::ServerTcpWriter;
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
use crate::runtime::path::{
    AcceptedServerDatagramFlow, PathProofObservation, ServerDatagramOpenError,
    ServerDatagramOpenRequest, ServerDatagramPort, ServerDatagramPortBackend,
    ServerLocalPathProperties, ServerTargetAdmission,
};
use crate::runtime::peer_status::PeerStatusBroker;
use crate::scheduler::TrafficClass;
use crate::transport::encrypted::{EncryptedFramedStream, EncryptedFramedTransportError};
use bytes::Bytes;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

struct PolicyDenyDatagramBackend;

impl ServerDatagramPortBackend for PolicyDenyDatagramBackend {
    fn open<'a>(
        &'a self,
        request: ServerDatagramOpenRequest,
    ) -> Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<AcceptedServerDatagramFlow, ServerDatagramOpenError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let error = match request.target.port() {
                81 => return Err(ServerDatagramOpenError::new(RuntimeError::RouteRejected)),
                82 => return Err(ServerDatagramOpenError::new(RuntimeError::RouteDropped)),
                83 => return Err(ServerDatagramOpenError::capacity()),
                _ => RuntimeError::Protocol("unexpected test datagram target"),
            };
            Err(ServerDatagramOpenError::new(error))
        })
    }
}

#[derive(Default)]
struct ScriptedDatagramBackend {
    opens: Mutex<HashMap<u16, usize>>,
    feedback: Arc<std::sync::atomic::AtomicUsize>,
}

impl ScriptedDatagramBackend {
    fn opens_for(&self, port: u16) -> usize {
        self.opens
            .lock()
            .expect("scripted datagram opens")
            .get(&port)
            .copied()
            .unwrap_or(0)
    }
}

impl ServerDatagramPortBackend for ScriptedDatagramBackend {
    fn open<'a>(
        &'a self,
        request: ServerDatagramOpenRequest,
    ) -> Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<AcceptedServerDatagramFlow, ServerDatagramOpenError>,
                > + Send
                + 'a,
        >,
    > {
        *self
            .opens
            .lock()
            .expect("scripted datagram opens")
            .entry(request.target.port())
            .or_default() += 1;
        let feedback = self.feedback.clone();
        Box::pin(async move {
            match request.target.port() {
                81 => Err(ServerDatagramOpenError::new(RuntimeError::RouteRejected)),
                82 => Err(ServerDatagramOpenError::new(RuntimeError::RouteDropped)),
                _ => {
                    let (requests, mut receiver) = mpsc::channel(8);
                    tokio::spawn(async move {
                        while let Some(message) = receiver.recv().await {
                            match message {
                                crate::runtime::path::ServerDatagramWorkerMessage::Request {
                                    admission,
                                    ..
                                } => {
                                    let _ = admission.send(Ok(
                                        crate::runtime::path::ServerDatagramSendOutcome::Accepted,
                                    ));
                                }
                                crate::runtime::path::ServerDatagramWorkerMessage::ResponseFeedback { .. } => {
                                    feedback.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                }
                                crate::runtime::path::ServerDatagramWorkerMessage::Attach {
                                    attached,
                                    ..
                                } => {
                                    let _ = attached.send(());
                                }
                            }
                        }
                    });
                    Ok(AcceptedServerDatagramFlow::holding(
                        request.flow_id,
                        requests,
                        request.commands,
                        Arc::new(()),
                        (),
                    ))
                }
            }
        })
    }
}

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
        recv_server_tcp_path_event(&mut path_frames, &mut commands_rx, &mut peer_status, None,)
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
    server_tcp_test_session_with_mode(session_id, path_id, crate::config::ForwardingMode::L4).await
}

async fn server_tcp_test_session_with_mode(
    session_id: SessionId,
    path_id: PathId,
    forwarding_mode: crate::config::ForwardingMode,
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
        paths: mut context,
        reliable_relay,
    } = new_identity_runtime(
        Vec::new(),
        OutboundConfig::Direct,
        DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        security,
        MppPerformanceConfig::default(),
        ResourceLimits::default(),
    );
    let reliable_relay = reliable_relay.expect("L4 test server has a reliable relay");
    context.forwarding_mode = forwarding_mode;
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
    let mut client_framed = client_framed.expect("client protected carrier");
    let mut server_framed = server_framed.expect("server protected carrier");
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
async fn server_tcp_l3_mode_rejects_l4_forwarding_opens() {
    let (mut session, mut client, _commands, _path_frames, _relay) =
        server_tcp_test_session_with_mode(
            SessionId(207),
            PathId(0),
            crate::config::ForwardingMode::L3,
        )
        .await;
    let stream_id = StreamId(1);
    session
        .handle_frame(Frame::OpenStream {
            stream_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
            demand: StreamDemandHint::Latency,
        })
        .await
        .expect("reject L4 stream open in L3 mode");
    assert_eq!(
        client.read_frame().await.expect("stream rejection"),
        Frame::StreamDetach { stream_id }
    );

    let flow_id = crate::protocol::DatagramFlowId(2);
    session
        .handle_frame(Frame::OpenDatagramFlow {
            flow_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 53))),
        })
        .await
        .expect("reject L4 datagram open in L3 mode");
    assert_eq!(
        client.read_frame().await.expect("datagram rejection"),
        Frame::DatagramClose { flow_id }
    );
}

#[tokio::test]
async fn server_tcp_route_denials_are_flow_local_and_drop_is_silent() {
    let session_id = SessionId(208);
    let path_id = PathId(0);
    let (mut session, mut client, commands, _path_frames, _relay) =
        server_tcp_test_session(session_id, path_id).await;
    session.context.reliable_streams = session
        .context
        .reliable_streams
        .clone()
        .with_target_admission(Arc::new(|_, target| {
            Ok(match target.port() {
                81 => ServerTargetAdmission::Reject,
                82 => ServerTargetAdmission::Drop,
                _ => ServerTargetAdmission::Allow,
            })
        }));

    let sibling_stream_id = StreamId(80);
    session
        .handle_frame(Frame::OpenStream {
            stream_id: sibling_stream_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
            demand: StreamDemandHint::Latency,
        })
        .await
        .expect("open allowed sibling stream");

    let rejected_stream_id = StreamId(81);
    session
        .handle_frame(Frame::OpenStream {
            stream_id: rejected_stream_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 81))),
            demand: StreamDemandHint::Latency,
        })
        .await
        .expect("reject one logical stream");
    assert_eq!(
        client.read_frame().await.expect("stream refusal"),
        Frame::StreamDetach {
            stream_id: rejected_stream_id,
        }
    );

    let dropped_stream_id = StreamId(82);
    session
        .handle_frame(Frame::OpenStream {
            stream_id: dropped_stream_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 82))),
            demand: StreamDemandHint::Latency,
        })
        .await
        .expect("silently drop one logical stream");
    assert!(
        tokio::time::timeout(Duration::from_millis(50), client.read_frame())
            .await
            .is_err(),
        "route drop must not emit an MPP response",
    );
    assert_eq!(
        session
            .context
            .reliable_streams
            .management_snapshot()
            .active_streams,
        1,
        "denied opens must preserve the allowed sibling logical stream",
    );

    let sibling_frame = Frame::StreamMaxData {
        stream_id: sibling_stream_id,
        max_offset: 4096,
    };
    commands
        .send_stream_ordered_frame(sibling_frame.clone(), TrafficClass::Latency)
        .await
        .expect("queue sibling output after route denials");
    let command = recv_reliable_path_command(&mut session.commands_rx)
        .await
        .expect("dequeue sibling output");
    assert!(matches!(
        session
            .drain_commands(command)
            .await
            .expect("write sibling output after route denials"),
        ServerTcpSessionDisposition::Continue
    ));
    assert_eq!(
        client.read_frame().await.expect("sibling output"),
        sibling_frame,
        "the shared TCP carrier must remain usable after both denial kinds",
    );
}

#[tokio::test]
async fn server_tcp_datagram_route_denials_are_flow_local_and_drop_is_silent() {
    let session_id = SessionId(209);
    let path_id = PathId(0);
    let (mut session, mut client, _commands, _path_frames, _relay) =
        server_tcp_test_session(session_id, path_id).await;
    session.context.datagrams = Some(ServerDatagramPort::new(Arc::new(PolicyDenyDatagramBackend)));

    let rejected_flow_id = crate::protocol::DatagramFlowId(81);
    session
        .handle_frame(Frame::OpenDatagramFlow {
            flow_id: rejected_flow_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 81))),
        })
        .await
        .expect("reject one TCP-carried datagram flow");
    assert_eq!(
        client.read_frame().await.expect("datagram refusal"),
        Frame::DatagramClose {
            flow_id: rejected_flow_id,
        }
    );

    let dropped_flow_id = crate::protocol::DatagramFlowId(82);
    session
        .handle_frame(Frame::OpenDatagramFlow {
            flow_id: dropped_flow_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 82))),
        })
        .await
        .expect("silently drop one TCP-carried datagram flow");
    assert!(
        tokio::time::timeout(Duration::from_millis(50), client.read_frame())
            .await
            .is_err(),
        "datagram drop must not emit an MPP response",
    );

    let saturated_flow_id = crate::protocol::DatagramFlowId(83);
    for _ in 0..2 {
        session
            .handle_frame(Frame::OpenDatagramFlow {
                flow_id: saturated_flow_id,
                target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 83))),
            })
            .await
            .expect("map shared registry saturation to a flow-local close");
        assert_eq!(
            client.read_frame().await.expect("registry capacity close"),
            Frame::DatagramClose {
                flow_id: saturated_flow_id,
            },
        );
    }

    session
        .handle_frame(Frame::Ping { nonce: 209 })
        .await
        .expect("use shared TCP carrier after datagram denials");
    assert_eq!(
        client.read_frame().await.expect("carrier pong"),
        Frame::Pong { nonce: 209 },
        "a denied datagram flow must not tear down its shared TCP carrier",
    );
}

#[tokio::test]
async fn server_tcp_datagram_tombstones_are_deterministic_bounded_and_flow_local() {
    let session_id = SessionId(210);
    let path_id = PathId(0);
    let (mut session, mut client, _commands, _path_frames, _relay) =
        server_tcp_test_session(session_id, path_id).await;
    session.context.max_udp_flows_per_session = 2;
    session.datagrams = ServerTcpDatagramState::new(2);
    let backend = Arc::new(ScriptedDatagramBackend::default());
    session.context.datagrams = Some(ServerDatagramPort::new(backend.clone()));

    let reject_id = crate::protocol::DatagramFlowId(81);
    let reject_open = Frame::OpenDatagramFlow {
        flow_id: reject_id,
        target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 81))),
    };
    session
        .handle_frame(reject_open.clone())
        .await
        .expect("reject datagram open");
    assert_eq!(
        client.read_frame().await.expect("initial rejection"),
        Frame::DatagramClose { flow_id: reject_id },
    );
    session
        .handle_frame(Frame::DatagramData {
            flow_id: reject_id,
            datagram_id: crate::protocol::DatagramId(1),
            ttl_ms: 0,
            payload: Bytes::from_static(b"late-rejected"),
        })
        .await
        .expect("ignore in-flight rejected data");
    session
        .handle_frame(Frame::DatagramFeedback {
            flow_id: reject_id,
            received: vec![crate::protocol::OffsetRange::new(0, 1).expect("feedback range")],
        })
        .await
        .expect("ignore in-flight rejected feedback");
    session
        .handle_frame(reject_open.clone())
        .await
        .expect("repeat rejected open from tombstone");
    assert_eq!(
        client.read_frame().await.expect("repeated rejection"),
        Frame::DatagramClose { flow_id: reject_id },
    );
    assert_eq!(
        backend.opens_for(81),
        1,
        "reject tombstone must bypass policy"
    );

    let drop_id = crate::protocol::DatagramFlowId(82);
    let drop_open = Frame::OpenDatagramFlow {
        flow_id: drop_id,
        target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 82))),
    };
    session
        .handle_frame(drop_open.clone())
        .await
        .expect("drop datagram open");
    session
        .handle_frame(Frame::DatagramData {
            flow_id: drop_id,
            datagram_id: crate::protocol::DatagramId(2),
            ttl_ms: 0,
            payload: Bytes::from_static(b"late-dropped"),
        })
        .await
        .expect("ignore in-flight dropped data");
    session
        .handle_frame(Frame::DatagramFeedback {
            flow_id: drop_id,
            received: Vec::new(),
        })
        .await
        .expect("ignore in-flight dropped feedback");
    session
        .handle_frame(drop_open)
        .await
        .expect("repeat silent drop from tombstone");
    assert!(
        tokio::time::timeout(Duration::from_millis(50), client.read_frame())
            .await
            .is_err(),
        "initial and repeated drop must stay silent",
    );
    assert_eq!(
        backend.opens_for(82),
        1,
        "drop tombstone must bypass policy"
    );
    assert_eq!(session.datagrams.tombstone_count(), 2);

    for (flow, port) in [(1, 90), (2, 91)] {
        session
            .handle_frame(Frame::OpenDatagramFlow {
                flow_id: crate::protocol::DatagramFlowId(flow),
                target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], port))),
            })
            .await
            .expect("open accepted sibling flow");
    }
    let accepted_id = crate::protocol::DatagramFlowId(1);
    session
        .handle_frame(Frame::DatagramData {
            flow_id: accepted_id,
            datagram_id: crate::protocol::DatagramId(3),
            ttl_ms: 1_000,
            payload: Bytes::from_static(b"accepted"),
        })
        .await
        .expect("send accepted sibling data");
    assert!(matches!(
        client.read_frame().await.expect("accepted feedback"),
        Frame::DatagramFeedback { flow_id, .. } if flow_id == accepted_id
    ));
    session
        .handle_frame(Frame::DatagramFeedback {
            flow_id: accepted_id,
            received: Vec::new(),
        })
        .await
        .expect("route accepted sibling feedback");
    tokio::time::timeout(Duration::from_secs(1), async {
        while backend.feedback.load(std::sync::atomic::Ordering::Relaxed) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("accepted sibling feedback routing timeout");

    let capacity_id = crate::protocol::DatagramFlowId(3);
    let capacity_open = Frame::OpenDatagramFlow {
        flow_id: capacity_id,
        target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 92))),
    };
    session
        .handle_frame(capacity_open.clone())
        .await
        .expect("reject at accepted-flow capacity");
    assert_eq!(
        client.read_frame().await.expect("capacity rejection"),
        Frame::DatagramClose {
            flow_id: capacity_id,
        },
    );
    assert_eq!(backend.opens_for(92), 0);
    assert_eq!(
        session.datagrams.tombstone_count(),
        2,
        "tombstones have an independent bound and do not consume accepted slots",
    );

    session
        .handle_frame(Frame::DatagramClose {
            flow_id: accepted_id,
        })
        .await
        .expect("close one accepted flow");
    session
        .handle_frame(capacity_open)
        .await
        .expect("repeat capacity tombstone after capacity becomes available");
    assert_eq!(
        client
            .read_frame()
            .await
            .expect("repeated capacity rejection"),
        Frame::DatagramClose {
            flow_id: capacity_id,
        },
    );
    assert_eq!(backend.opens_for(92), 0);

    session
        .handle_frame(reject_open)
        .await
        .expect("re-evaluate LRU-evicted rejection");
    assert_eq!(
        client.read_frame().await.expect("re-evaluated rejection"),
        Frame::DatagramClose { flow_id: reject_id },
    );
    assert_eq!(backend.opens_for(81), 2, "oldest tombstone must be evicted");
    assert_eq!(session.datagrams.tombstone_count(), 2);

    let unknown_id = crate::protocol::DatagramFlowId(999);
    session
        .handle_frame(Frame::DatagramData {
            flow_id: unknown_id,
            datagram_id: crate::protocol::DatagramId(4),
            ttl_ms: 0,
            payload: Bytes::from_static(b"unknown"),
        })
        .await
        .expect("ignore unknown data");
    session
        .handle_frame(Frame::DatagramFeedback {
            flow_id: unknown_id,
            received: Vec::new(),
        })
        .await
        .expect("ignore unknown feedback");
    session
        .handle_frame(Frame::DatagramClose {
            flow_id: capacity_id,
        })
        .await
        .expect("close capacity tombstone");
    assert_eq!(session.datagrams.tombstone_count(), 1);

    session
        .handle_frame(Frame::Ping { nonce: 210 })
        .await
        .expect("use carrier after stale datagram frames");
    assert_eq!(
        client.read_frame().await.expect("carrier pong"),
        Frame::Pong { nonce: 210 },
    );
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
async fn server_tcp_attachment_refusal_is_stream_local_during_ordered_detach() {
    let session_id = SessionId(205);
    let path_id = PathId(0);
    let (mut session, _client_framed, _commands, _path_frames, _reliable_relay) =
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
    let stream_id = StreamId(303);
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let context = session.context.clone();
    let registration = session.path_registration.clone();
    let commands = session.commands_tx.clone();
    assert!(
        session
            .streams
            .open(
                &context,
                &registration,
                &commands,
                session_id,
                stream_id,
                target.clone(),
                StreamDemandHint::throughput(),
            )
            .await
            .expect("open TCP response stream")
            .is_none()
    );

    session
        .streams
        .detach(&context, &registration, stream_id)
        .expect("begin ordered TCP attachment detach");
    assert_eq!(
        session
            .streams
            .open(
                &context,
                &registration,
                &commands,
                session_id,
                stream_id,
                target,
                StreamDemandHint::throughput(),
            )
            .await
            .expect("retry TCP attachment while detach is pending"),
        Some(Frame::StreamDetach { stream_id }),
        "an attachment-local refusal must not reset the logical stream",
    );
    assert_eq!(
        context
            .reliable_streams
            .management_snapshot()
            .active_streams,
        1,
        "the existing logical stream must survive attachment refusal",
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
