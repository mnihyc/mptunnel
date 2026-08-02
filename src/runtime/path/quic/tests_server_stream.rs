use super::{
    ServerUdpReliableOutputDetachGuard, ServerUdpReliableStreamContext,
    ServerUdpReliableStreamLoop, handle_server_udp_reliable_stream,
    run_server_udp_reliable_stream_loop,
};
use crate::config::{
    ClientSecurityConfig, DEFAULT_OUTBOUND_CONNECT_TIMEOUT, MppPerformanceConfig, ResourceLimits,
    ServerSecurityConfig, SharedSecret,
};
use crate::model::capacity::MAX_RELIABLE_SERVICE_QUANTUM_BYTES;
use crate::model::path::next_carrier_path_instance_id;
use crate::mux::MuxLimits;
use crate::outbound::OutboundConfig;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{
    Frame, PathId, ResetReason, SessionId, StreamId, TargetAddr, UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::node::server::{ServerIdentityRuntime, new_identity_runtime};
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, ReliablePathCommandSender,
    recv_reliable_path_command, reliable_path_command_channels,
    reliable_path_command_pending_bytes,
};
use crate::runtime::path::proof::PathProofTracker;
use crate::runtime::path::quic::client::ClientUdpPathSessionRuntime;
use crate::runtime::path::quic::client_stream::run_client_udp_stream;
use crate::runtime::path::quic::io::{
    UdpPathConnection, UdpPathEndpoint, UdpPathRecvStream, UdpPathSendStream,
    udp_path_finish_stream, udp_path_max_stream_payload_bytes, udp_path_read_frame,
    udp_path_write_frame,
};
use crate::runtime::path::quic::server_writer::drain_server_udp_reliable_commands;
use crate::runtime::path::server_context::ServerPathContext;
use crate::runtime::path::{
    ClientPathHealth, ClientPathHealthRecord, ClientPathState, PathProofObservation,
    ServerCarrierPathRegistration, ServerLocalPathProperties, ServerStreamOpenOutcome,
    ServerStreamOpenRequest, ServerStreamPathAttachment,
};
use crate::runtime::peer_status::{PeerStatusBroker, PeerStatusSnapshotSource};
use crate::runtime::stream::{
    AcceptedServerReliableStream, ReliablePathStreamOutput, ServerReliableStreamRegistry,
};
use crate::scheduler::TrafficClass;
use crate::transport::{
    CarrierEndpoint as PathEndpoint, CarrierPathIdentity, CarrierSocket, CarrierSocketRequest,
    PathBinding, PathMetadata, PathSpec, SystemCarrierNetworkProvider,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

struct ServerUdpTerminalWriterFixture {
    context: ServerPathContext,
    session_id: SessionId,
    path_id: PathId,
    stream_id: StreamId,
    target: TargetAddr,
    _path_registration: ServerCarrierPathRegistration,
    commands_tx: ReliablePathCommandSender,
    commands_rx: Option<ReliablePathCommandReceivers>,
    accepted: AcceptedServerReliableStream,
    server_send: Option<UdpPathSendStream>,
    client_recv: Option<UdpPathRecvStream>,
    client_send: Option<UdpPathSendStream>,
    server_recv: Option<UdpPathRecvStream>,
    _server_endpoint: UdpPathEndpoint,
    _client_endpoint: UdpPathEndpoint,
    _server_connection: UdpPathConnection,
    _client_connection: UdpPathConnection,
}

impl ServerUdpTerminalWriterFixture {
    async fn open(stream_id: StreamId) -> Self {
        let shared_secret = SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("test shared secret");
        let security = ServerSecurityConfig::for_test(shared_secret.clone());
        let client_security = ClientSecurityConfig::for_test(shared_secret);
        let ServerIdentityRuntime {
            paths: mut context,
            reliable_relay: _,
        } = new_identity_runtime(
            Vec::new(),
            OutboundConfig::Direct,
            DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
            security,
            MppPerformanceConfig::default(),
            ResourceLimits::default(),
        );
        let (registry, mut accepted_rx) =
            ServerReliableStreamRegistry::new_accepting(context.mux_limits.max_streams);
        context.reliable_streams = registry.path_port();
        let session_id = SessionId(401);
        let path_id = PathId(0);
        let path_registration = context.reliable_streams.register_test_carrier_path(
            session_id,
            UnderlayProtocol::Udp,
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
        let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
        let outcome = context
            .reliable_streams
            .open_or_attach(ServerStreamOpenRequest {
                session_id,
                stream_id,
                target: target.clone(),
                lane: TrafficClass::Throughput,
                attachment: ServerStreamPathAttachment {
                    path_registration: path_registration.clone(),
                    commands: commands_tx.clone(),
                    max_frame_payload_bytes: udp_path_max_stream_payload_bytes(
                        context.codec_limits,
                        context.mux_limits,
                    ),
                },
                mux_limits: context.mux_limits,
            })
            .await
            .expect("open server QUIC response stream");
        assert_eq!(outcome, ServerStreamOpenOutcome::New);
        let accepted = accepted_rx
            .recv()
            .await
            .expect("receive accepted server QUIC response stream");

        let reserved =
            std::net::UdpSocket::bind("127.0.0.1:0").expect("reserve server QUIC address");
        let reserved_addr = reserved.local_addr().expect("reserved QUIC address");
        drop(reserved);
        let bind_path = PathSpec {
            underlay: UnderlayProtocol::Udp,
            endpoint: PathEndpoint::single("127.0.0.1", reserved_addr.port())
                .expect("server carrier endpoint"),
            binding: PathBinding::default(),
            metadata: PathMetadata::default(),
        };
        let server_endpoint = UdpPathEndpoint::bind_server(&bind_path, &context)
            .await
            .expect("bind server QUIC endpoint");
        let server_addr = server_endpoint.local_addr().expect("server QUIC address");
        let client_path = PathSpec {
            underlay: UnderlayProtocol::Udp,
            endpoint: PathEndpoint::single(server_addr.ip().to_string(), server_addr.port())
                .expect("client carrier endpoint"),
            binding: PathBinding::default(),
            metadata: PathMetadata::default(),
        };
        let client_runtime = ClientUdpPathSessionRuntime {
            paths: Arc::new(vec![client_path.clone()]),
            config_index: 0,
            path_index: 0,
            carrier_identity: CarrierPathIdentity {
                group_ordinal: 0,
                path_ordinal: 0,
            },
            session_id,
            candidate_selector: crate::transport::quic::QuicCandidateSelector::derive(
                client_security.credential.id().as_str(),
                client_security.credential.secret().as_bytes(),
            ),
            security: Arc::new(vec![client_security]),
            tls: Arc::new(vec![crate::transport::encrypted::test_client_tls_config()]),
            codec_limits: context.codec_limits,
            mux_limits: context.mux_limits,
            stream_frame_queue: 8,
            state: ClientPathState::new(ClientPathHealth::new(Vec::new(), Vec::new())),
            carrier_network: Arc::new(SystemCarrierNetworkProvider),
            peer_status: PeerStatusBroker::new(false),
            peer_status_snapshot: PeerStatusSnapshotSource::new(|| Some(Vec::new())),
            authenticated_carriers: crate::runtime::path::AuthenticatedCarrierInventory::default(),
        };
        let client_carrier = CarrierSocket::system(CarrierSocketRequest {
            path: &client_path,
            identity: CarrierPathIdentity {
                group_ordinal: 0,
                path_ordinal: 0,
            },
            remote_addr: server_addr,
        })
        .expect("create client QUIC carrier");
        let client_endpoint = UdpPathEndpoint::bind_client(client_carrier, &client_runtime)
            .await
            .expect("bind client QUIC endpoint");
        let (client_connection, server_connection) =
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(
                    client_endpoint.connect(server_addr),
                    server_endpoint.accept(),
                )
            })
            .await
            .expect("QUIC connection timeout");
        let client_connection = client_connection.expect("connect QUIC carrier");
        let server_connection = server_connection.expect("accept QUIC carrier");
        let (mut client_send, client_recv) = client_connection
            .open_bi()
            .await
            .expect("open client QUIC stream");
        udp_path_write_frame(
            &mut client_send,
            &Frame::Ping { nonce: 1 },
            context.codec_limits,
        )
        .await
        .expect("publish client QUIC stream");
        let (server_send, server_recv) =
            tokio::time::timeout(Duration::from_secs(5), server_connection.accept_bi())
                .await
                .expect("server QUIC stream timeout")
                .expect("accept server QUIC stream");

        Self {
            context,
            session_id,
            path_id,
            stream_id,
            target,
            _path_registration: path_registration,
            commands_tx,
            commands_rx: Some(commands_rx),
            accepted,
            server_send: Some(server_send),
            client_recv: Some(client_recv),
            client_send: Some(client_send),
            server_recv: Some(server_recv),
            _server_endpoint: server_endpoint,
            _client_endpoint: client_endpoint,
            _server_connection: server_connection,
            _client_connection: client_connection,
        }
    }

    fn attached_output_count(&self) -> usize {
        let ReliablePathStreamOutput::Switchable(binding) = &self.accepted.stream().output else {
            panic!("expected switchable server response output");
        };
        binding
            .sender_path_targets(TrafficClass::Throughput, MAX_RELIABLE_SERVICE_QUANTUM_BYTES)
            .len()
    }
}

#[tokio::test]
async fn reliable_output_guard_detaches_on_abnormal_stream_exit() {
    let (registry, mut accepted_rx) =
        ServerReliableStreamRegistry::new_accepting(ResourceLimits::default().max_streams);
    let streams = registry.path_port();
    let session_id = SessionId(201);
    let stream_id = StreamId(301);
    let path_id = PathId(0);
    let path_registration = streams.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Udp,
        path_id,
        ServerLocalPathProperties::default(),
    );
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let (commands, _receivers) = reliable_path_command_channels(8);
    let outcome = streams
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target,
            lane: TrafficClass::Throughput,
            attachment: ServerStreamPathAttachment {
                path_registration: path_registration.clone(),
                commands,
                max_frame_payload_bytes: udp_path_max_stream_payload_bytes(
                    CodecLimits::default(),
                    MuxLimits::default(),
                ),
            },
            mux_limits: MuxLimits::default(),
        })
        .await
        .expect("open UDP response stream");
    assert_eq!(outcome, ServerStreamOpenOutcome::New);
    let mut accepted = accepted_rx
        .recv()
        .await
        .expect("receive accepted UDP response stream");
    let ReliablePathStreamOutput::Switchable(binding) = &accepted.stream().output else {
        panic!("expected switchable response output");
    };
    let binding = binding.clone();
    assert_eq!(
        binding
            .sender_path_targets(TrafficClass::Throughput, MAX_RELIABLE_SERVICE_QUANTUM_BYTES)
            .len(),
        1
    );

    drop(ServerUdpReliableOutputDetachGuard {
        streams,
        path_registration,
        stream_id,
    });

    let mut stream = accepted.take_stream();
    assert!(
        tokio::time::timeout(Duration::from_millis(25), stream.recv_frame())
            .await
            .is_err(),
        "detach lifecycle should apply before waiting for the next product frame"
    );

    assert!(
        binding
            .sender_path_targets(TrafficClass::Throughput, MAX_RELIABLE_SERVICE_QUANTUM_BYTES)
            .is_empty(),
        "every server QUIC stream exit must detach its response output"
    );
}

#[tokio::test]
async fn server_quic_terminal_writer_flushes_reset_then_detaches_and_finishes_stream() {
    let stream_id = StreamId(402);
    let mut fixture = ServerUdpTerminalWriterFixture::open(stream_id).await;
    assert_eq!(fixture.attached_output_count(), 1);
    fixture
        .commands_tx
        .send_stream_ordered_reset_and_close(
            stream_id,
            ResetReason::Refused,
            TrafficClass::Throughput,
        )
        .await
        .expect("queue server QUIC terminal command");
    let command = recv_reliable_path_command(
        fixture
            .commands_rx
            .as_mut()
            .expect("server QUIC command receivers"),
    )
    .await
    .expect("dequeue server QUIC terminal command");
    let mut pending_frames = Vec::new();
    let mut path_proofs = PathProofTracker::default();
    let (_carrier_frames_tx, mut carrier_frames) = mpsc::channel(1);
    let mut deferred_input = None;

    let should_close = drain_server_udp_reliable_commands(
        command,
        fixture
            .commands_rx
            .as_mut()
            .expect("server QUIC command receivers"),
        fixture.server_send.as_mut().expect("server QUIC sender"),
        &fixture.context,
        fixture.stream_id,
        fixture.path_id,
        &fixture._path_registration,
        &mut pending_frames,
        &mut path_proofs,
        &mut carrier_frames,
        &mut deferred_input,
    )
    .await
    .expect("write server QUIC terminal command");

    assert!(
        should_close,
        "a matching terminal command finishes its stream"
    );
    assert!(pending_frames.is_empty());
    assert_eq!(fixture.commands_tx.pending_bytes(), 0);
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(5),
            udp_path_read_frame(
                fixture.client_recv.as_mut().expect("client QUIC receiver"),
                fixture.context.codec_limits,
            ),
        )
        .await
        .expect("server QUIC reset timeout")
        .expect("read server QUIC terminal reset"),
        Frame::StreamReset {
            stream_id,
            reason: ResetReason::Refused,
        }
    );
    let ReliablePathStreamOutput::Switchable(binding) = &fixture.accepted.stream().output else {
        panic!("expected switchable server response output");
    };
    let binding = binding.clone();
    let mut product_stream = fixture.accepted.take_stream();
    assert!(
        tokio::time::timeout(Duration::from_millis(25), product_stream.recv_frame())
            .await
            .is_err(),
        "terminal detach should apply before waiting for another product frame"
    );
    assert_eq!(
        binding
            .sender_path_targets(TrafficClass::Throughput, MAX_RELIABLE_SERVICE_QUANTUM_BYTES)
            .len(),
        0,
        "the matching terminal writer detaches its exact response output"
    );
    assert!(
        tokio::time::timeout(
            Duration::from_secs(5),
            udp_path_read_frame(
                fixture.client_recv.as_mut().expect("client QUIC receiver"),
                fixture.context.codec_limits,
            ),
        )
        .await
        .expect("server QUIC finish timeout")
        .is_err(),
        "the matching terminal writer finishes the QUIC stream after its reset"
    );
}

#[tokio::test]
async fn server_quic_mismatched_terminal_releases_debt_and_guard_fails_closed() {
    let stream_id = StreamId(403);
    let mismatched_stream_id = StreamId(404);
    let mut fixture = ServerUdpTerminalWriterFixture::open(stream_id).await;
    fixture
        .commands_tx
        .send_stream_ordered_reset_and_close(
            mismatched_stream_id,
            ResetReason::Refused,
            TrafficClass::Throughput,
        )
        .await
        .expect("queue mismatched server QUIC terminal command");
    let command = recv_reliable_path_command(
        fixture
            .commands_rx
            .as_mut()
            .expect("server QUIC command receivers"),
    )
    .await
    .expect("dequeue mismatched server QUIC terminal command");
    let command_debt = reliable_path_command_pending_bytes(&command) as u64;
    assert_eq!(fixture.commands_tx.pending_bytes(), command_debt);
    let output_guard = ServerUdpReliableOutputDetachGuard {
        streams: fixture.context.reliable_streams.clone(),
        path_registration: fixture._path_registration.clone(),
        stream_id: fixture.stream_id,
    };
    let mut server_send = fixture.server_send.take().expect("server QUIC sender");
    let mut pending_frames = Vec::new();
    let mut path_proofs = PathProofTracker::default();
    let (_carrier_frames_tx, mut carrier_frames) = mpsc::channel(1);
    let mut deferred_input = None;

    let error = drain_server_udp_reliable_commands(
        command,
        fixture
            .commands_rx
            .as_mut()
            .expect("server QUIC command receivers"),
        &mut server_send,
        &fixture.context,
        fixture.stream_id,
        fixture.path_id,
        &fixture._path_registration,
        &mut pending_frames,
        &mut path_proofs,
        &mut carrier_frames,
        &mut deferred_input,
    )
    .await
    .expect_err("a stream-local QUIC writer must reject another stream's terminal command");

    assert!(matches!(
        error,
        RuntimeError::Protocol("server QUIC terminal command stream does not match writer")
    ));
    assert_eq!(fixture.commands_tx.pending_bytes(), 0);
    assert!(pending_frames.is_empty());
    let ReliablePathStreamOutput::Switchable(binding) = &fixture.accepted.stream().output else {
        panic!("expected switchable server response output");
    };
    let binding = binding.clone();
    drop(output_guard);
    drop(server_send);
    let mut product_stream = fixture.accepted.take_stream();
    assert!(
        tokio::time::timeout(Duration::from_millis(25), product_stream.recv_frame())
            .await
            .is_err(),
        "guard detach should apply before waiting for another product frame"
    );
    assert_eq!(
        binding
            .sender_path_targets(TrafficClass::Throughput, MAX_RELIABLE_SERVICE_QUANTUM_BYTES)
            .len(),
        0,
        "the enclosing stream guard detaches the actual attachment on writer failure"
    );
    assert!(
        tokio::time::timeout(
            Duration::from_secs(5),
            udp_path_read_frame(
                fixture.client_recv.as_mut().expect("client QUIC receiver"),
                fixture.context.codec_limits,
            ),
        )
        .await
        .expect("failed server QUIC stream close timeout")
        .is_err(),
        "dropping the failed stream-local writer must fail the QUIC stream closed"
    );
}

#[tokio::test]
async fn client_quic_terminal_input_keeps_feedback_writer_until_owner_close() {
    let stream_id = StreamId(405);
    let mut fixture = ServerUdpTerminalWriterFixture::open(stream_id).await;
    let client_send = fixture.client_send.take().expect("client QUIC sender");
    let client_recv = fixture.client_recv.take().expect("client QUIC receiver");
    let mut server_send = fixture.server_send.take().expect("server QUIC sender");
    let mut server_recv = fixture.server_recv.take().expect("server QUIC receiver");
    let (commands_tx, commands_rx) = reliable_path_command_channels(8);
    let (frames_tx, mut frames_rx) = mpsc::channel(8);
    let state = ClientPathState::new(ClientPathHealth::new(
        Vec::new(),
        vec![ClientPathHealthRecord::default()],
    ));
    let codec_limits = fixture.context.codec_limits;
    let mux_limits = fixture.context.mux_limits;
    let actor = tokio::spawn(run_client_udp_stream(
        client_send,
        client_recv,
        stream_id,
        0,
        next_carrier_path_instance_id(),
        codec_limits,
        mux_limits,
        8,
        state,
        commands_rx,
        frames_tx,
    ));

    assert_eq!(
        udp_path_read_frame(&mut server_recv, codec_limits)
            .await
            .expect("read client opener"),
        Frame::Ping { nonce: 1 },
    );
    udp_path_write_frame(
        &mut server_send,
        &Frame::StreamFin {
            stream_id,
            final_offset: 0,
        },
        codec_limits,
    )
    .await
    .expect("write terminal product FIN");
    udp_path_finish_stream(&mut server_send)
        .await
        .expect("finish server send half");
    assert!(matches!(
        frames_rx.recv().await,
        Some(Ok(Frame::StreamFin {
            stream_id: received_stream_id,
            final_offset: 0,
        })) if received_stream_id == stream_id
    ));

    tokio::time::sleep(Duration::from_millis(50)).await;
    commands_tx
        .send_control(ReliablePathCommand::SendFrame(Frame::StreamAck {
            stream_id,
            complete: true,
            ranges: Vec::new(),
        }))
        .await
        .expect("client writer remains after peer send-half finish");
    assert!(matches!(
        udp_path_read_frame(&mut server_recv, codec_limits).await,
        Ok(Frame::StreamAck {
            stream_id: ack_stream_id,
            complete: true,
            ..
        }) if ack_stream_id == stream_id
    ));
    commands_tx
        .send_stream_ordered_close(stream_id, TrafficClass::Throughput)
        .await
        .expect("close retained client writer");
    tokio::time::timeout(Duration::from_secs(5), actor)
        .await
        .expect("client actor close timeout")
        .expect("client actor join");
    assert!(!fixture._client_connection.is_closed());
}

#[tokio::test]
async fn client_quic_clean_eof_reports_attachment_failure_then_allows_detach() {
    let stream_id = StreamId(407);
    let mut fixture = ServerUdpTerminalWriterFixture::open(stream_id).await;
    let client_send = fixture.client_send.take().expect("client QUIC sender");
    let client_recv = fixture.client_recv.take().expect("client QUIC receiver");
    let mut server_send = fixture.server_send.take().expect("server QUIC sender");
    let mut server_recv = fixture.server_recv.take().expect("server QUIC receiver");
    let (commands_tx, commands_rx) = reliable_path_command_channels(8);
    let (frames_tx, mut frames_rx) = mpsc::channel(8);
    let state = ClientPathState::new(ClientPathHealth::new(
        Vec::new(),
        vec![ClientPathHealthRecord::default()],
    ));
    let codec_limits = fixture.context.codec_limits;
    let actor = tokio::spawn(run_client_udp_stream(
        client_send,
        client_recv,
        stream_id,
        0,
        next_carrier_path_instance_id(),
        codec_limits,
        fixture.context.mux_limits,
        8,
        state,
        commands_rx,
        frames_tx,
    ));

    assert_eq!(
        udp_path_read_frame(&mut server_recv, codec_limits)
            .await
            .expect("read client opener"),
        Frame::Ping { nonce: 1 },
    );
    udp_path_finish_stream(&mut server_send)
        .await
        .expect("finish server send half");
    assert!(matches!(
        frames_rx.recv().await,
        Some(Err(RuntimeError::ReliablePathSessionClosed))
    ));

    commands_tx
        .send_stream_ordered_frame(Frame::StreamDetach { stream_id }, TrafficClass::Throughput)
        .await
        .expect("queue detach after peer clean EOF");
    commands_tx
        .send_stream_ordered_close(stream_id, TrafficClass::Throughput)
        .await
        .expect("close retained client writer");
    assert_eq!(
        udp_path_read_frame(&mut server_recv, codec_limits)
            .await
            .expect("read client detach"),
        Frame::StreamDetach { stream_id },
    );
    assert!(matches!(
        udp_path_read_frame(&mut server_recv, codec_limits).await,
        Err(RuntimeError::QuicCarrier(
            crate::transport::quic::QuicCarrierError::StreamFinished
        ))
    ));
    tokio::time::timeout(Duration::from_secs(5), actor)
        .await
        .expect("client actor close timeout")
        .expect("client actor join");
    assert!(!fixture._client_connection.is_closed());
}

#[tokio::test]
async fn server_quic_duplicate_refusal_preserves_live_attachment() {
    let stream_id = StreamId(409);
    let fixture = ServerUdpTerminalWriterFixture::open(stream_id).await;
    assert_eq!(fixture.attached_output_count(), 1);

    let (mut client_send, mut client_recv) = fixture
        ._client_connection
        .open_bi()
        .await
        .expect("open duplicate client QUIC stream");
    udp_path_write_frame(
        &mut client_send,
        &Frame::OpenStream {
            stream_id,
            target: fixture.target.clone(),
            demand: crate::protocol::StreamDemandHint::throughput(),
        },
        fixture.context.codec_limits,
    )
    .await
    .expect("publish duplicate QUIC attachment");
    let (server_send, server_recv) = tokio::time::timeout(
        Duration::from_secs(5),
        fixture._server_connection.accept_bi(),
    )
    .await
    .expect("duplicate QUIC stream timeout")
    .expect("accept duplicate QUIC stream");

    handle_server_udp_reliable_stream(
        server_send,
        server_recv,
        fixture.context.clone(),
        ServerUdpReliableStreamContext {
            session_id: fixture.session_id,
            path_id: fixture.path_id,
            path_registration: fixture._path_registration.clone(),
            stream_id,
            target: fixture.target.clone(),
            lane: TrafficClass::Throughput,
        },
    )
    .await
    .expect("refuse duplicate QUIC attachment");
    assert_eq!(
        udp_path_read_frame(&mut client_recv, fixture.context.codec_limits)
            .await
            .expect("read duplicate QUIC attachment refusal"),
        Frame::StreamDetach { stream_id },
    );
    assert_eq!(
        fixture.attached_output_count(),
        1,
        "refusing a duplicate must preserve the existing live attachment",
    );
}

#[tokio::test]
async fn server_quic_attachment_refusal_is_stream_local_during_ordered_detach() {
    let stream_id = StreamId(408);
    let fixture = ServerUdpTerminalWriterFixture::open(stream_id).await;
    fixture
        .context
        .reliable_streams
        .detach_path(&fixture._path_registration, stream_id)
        .expect("begin ordered QUIC attachment detach");

    let (mut client_send, mut client_recv) = fixture
        ._client_connection
        .open_bi()
        .await
        .expect("open retried client QUIC stream");
    udp_path_write_frame(
        &mut client_send,
        &Frame::OpenStream {
            stream_id,
            target: fixture.target.clone(),
            demand: crate::protocol::StreamDemandHint::throughput(),
        },
        fixture.context.codec_limits,
    )
    .await
    .expect("publish retried QUIC attachment");
    let (server_send, server_recv) = tokio::time::timeout(
        Duration::from_secs(5),
        fixture._server_connection.accept_bi(),
    )
    .await
    .expect("retried QUIC stream timeout")
    .expect("accept retried QUIC stream");

    handle_server_udp_reliable_stream(
        server_send,
        server_recv,
        fixture.context.clone(),
        ServerUdpReliableStreamContext {
            session_id: fixture.session_id,
            path_id: fixture.path_id,
            path_registration: fixture._path_registration.clone(),
            stream_id,
            target: fixture.target.clone(),
            lane: TrafficClass::Throughput,
        },
    )
    .await
    .expect("refuse retried QUIC attachment");
    assert_eq!(
        udp_path_read_frame(&mut client_recv, fixture.context.codec_limits)
            .await
            .expect("read QUIC attachment refusal"),
        Frame::StreamDetach { stream_id },
        "an attachment-local refusal must not reset the logical stream",
    );
    assert!(matches!(
        udp_path_read_frame(&mut client_recv, fixture.context.codec_limits).await,
        Err(RuntimeError::QuicCarrier(
            crate::transport::quic::QuicCarrierError::StreamFinished
        ))
    ));
    assert_eq!(
        fixture
            .context
            .reliable_streams
            .management_snapshot()
            .active_streams,
        1,
        "the existing logical stream must survive attachment refusal",
    );
}

#[tokio::test]
async fn server_quic_ordered_close_drains_peer_until_stream_detach() {
    let stream_id = StreamId(406);
    let mut fixture = ServerUdpTerminalWriterFixture::open(stream_id).await;
    let mut product_stream = fixture.accepted.take_stream();
    let server_send = fixture.server_send.take().expect("server QUIC sender");
    let server_recv = fixture.server_recv.take().expect("server QUIC receiver");
    let commands_rx = fixture
        .commands_rx
        .take()
        .expect("server QUIC command receivers");
    let context = fixture.context.clone();
    let path_registration = fixture._path_registration.clone();
    let commands_tx = fixture.commands_tx.clone();
    let target = fixture.target.clone();
    fixture
        .commands_tx
        .send_stream_ordered_frame(
            Frame::StreamFin {
                stream_id,
                final_offset: 0,
            },
            TrafficClass::Throughput,
        )
        .await
        .expect("queue server product FIN");
    fixture
        .commands_tx
        .send_stream_ordered_close(stream_id, TrafficClass::Throughput)
        .await
        .expect("queue server ordered close");
    let actor = tokio::spawn(run_server_udp_reliable_stream_loop(
        server_send,
        server_recv,
        ServerUdpReliableStreamLoop {
            context,
            session_id: fixture.session_id,
            path_id: fixture.path_id,
            path_registration,
            stream_id,
            target,
            commands_tx,
            commands_rx,
            path_proofs: PathProofTracker::default(),
        },
    ));

    let client_recv = fixture.client_recv.as_mut().expect("client QUIC receiver");
    let first = udp_path_read_frame(client_recv, fixture.context.codec_limits)
        .await
        .expect("read server terminal response");
    let terminal = if first == (Frame::Pong { nonce: 1 }) {
        udp_path_read_frame(client_recv, fixture.context.codec_limits)
            .await
            .expect("read server product FIN")
    } else {
        first
    };
    assert!(matches!(
        terminal,
        Frame::StreamFin {
            stream_id: received_stream_id,
            final_offset: 0,
        } if received_stream_id == stream_id
    ));
    assert!(matches!(
        udp_path_read_frame(client_recv, fixture.context.codec_limits).await,
        Err(RuntimeError::QuicCarrier(
            crate::transport::quic::QuicCarrierError::StreamFinished
        ))
    ));

    let client_send = fixture.client_send.as_mut().expect("client QUIC sender");
    udp_path_write_frame(
        client_send,
        &Frame::StreamAck {
            stream_id,
            complete: true,
            ranges: Vec::new(),
        },
        fixture.context.codec_limits,
    )
    .await
    .expect("write final feedback after server send-half finish");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), product_stream.recv_frame()).await,
        Ok(Ok(Frame::StreamAck {
            stream_id: ack_stream_id,
            complete: true,
            ..
        })) if ack_stream_id == stream_id
    ));
    udp_path_write_frame(
        client_send,
        &Frame::StreamDetach { stream_id },
        fixture.context.codec_limits,
    )
    .await
    .expect("detach server terminal drain");
    udp_path_finish_stream(client_send)
        .await
        .expect("finish client send half");

    tokio::time::timeout(Duration::from_secs(5), actor)
        .await
        .expect("server terminal drain timeout")
        .expect("server actor join")
        .expect("server actor result");
    let ReliablePathStreamOutput::Switchable(binding) = &product_stream.output else {
        panic!("expected switchable server response output");
    };
    assert!(
        binding
            .sender_path_targets(TrafficClass::Throughput, MAX_RELIABLE_SERVICE_QUANTUM_BYTES)
            .is_empty()
    );
    assert!(!fixture._server_connection.is_closed());
}
