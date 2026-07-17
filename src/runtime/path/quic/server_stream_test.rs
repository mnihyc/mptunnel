use super::ServerUdpReliableOutputDetachGuard;
use crate::config::{
    DEFAULT_OUTBOUND_CONNECT_TIMEOUT, MppPerformanceConfig, ResourceLimits, SecurityConfig,
    SharedSecret,
};
use crate::model::capacity::MAX_RELIABLE_SERVICE_QUANTUM_BYTES;
use crate::mux::MuxLimits;
use crate::outbound::{DnsConfig, OutboundConfig};
use crate::protocol::codec::CodecLimits;
use crate::protocol::{
    Frame, PathId, ResetReason, SessionId, StreamId, TargetAddr, UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::node::server::{ServerIdentityRuntime, new_identity_runtime};
use crate::runtime::path::commands::{
    ReliablePathCommandReceivers, ReliablePathCommandSender, recv_reliable_path_command,
    reliable_path_command_channels, reliable_path_command_pending_bytes,
};
use crate::runtime::path::proof::PathProofTracker;
use crate::runtime::path::quic::client::ClientUdpPathSessionRuntime;
use crate::runtime::path::quic::io::{
    UdpPathConnection, UdpPathEndpoint, UdpPathRecvStream, UdpPathSendStream,
    udp_path_max_stream_payload_bytes, udp_path_read_frame, udp_path_write_frame,
};
use crate::runtime::path::quic::server_writer::drain_server_udp_reliable_commands;
use crate::runtime::path::server_context::ServerPathContext;
use crate::runtime::path::{
    ClientPathHealth, ClientPathState, ServerCarrierPathRegistration, ServerLocalPathProperties,
    ServerStreamOpenOutcome, ServerStreamOpenRequest, ServerStreamPathAttachment,
};
use crate::runtime::peer_status::{PeerStatusBroker, PeerStatusSnapshotSource};
use crate::runtime::stream::{
    AcceptedServerReliableStream, ReliablePathStreamOutput, ServerReliableStreamRegistry,
};
use crate::scheduler::TrafficClass;
use crate::transport::{
    CarrierPathIdentity, CarrierSocket, CarrierSocketRequest, Endpoint as PathEndpoint,
    PathBinding, PathMetadata, PathSpec, SystemCarrierNetworkProvider,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

struct ServerUdpTerminalWriterFixture {
    context: ServerPathContext,
    session_id: SessionId,
    path_id: PathId,
    stream_id: StreamId,
    _path_registration: ServerCarrierPathRegistration,
    commands_tx: ReliablePathCommandSender,
    commands_rx: ReliablePathCommandReceivers,
    accepted: AcceptedServerReliableStream,
    server_send: Option<UdpPathSendStream>,
    client_recv: UdpPathRecvStream,
    _client_send: UdpPathSendStream,
    _server_recv: UdpPathRecvStream,
    _server_endpoint: UdpPathEndpoint,
    _client_endpoint: UdpPathEndpoint,
    _server_connection: UdpPathConnection,
    _client_connection: UdpPathConnection,
}

impl ServerUdpTerminalWriterFixture {
    async fn open(stream_id: StreamId) -> Self {
        let security = SecurityConfig::encrypted(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
                .expect("test shared secret"),
        );
        let ServerIdentityRuntime {
            paths: mut context,
            reliable_relay: _,
        } = new_identity_runtime(
            Vec::new(),
            OutboundConfig::Direct,
            DnsConfig::default(),
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
        let path_registration = context.reliable_streams.register_carrier_path(
            session_id,
            UnderlayProtocol::Udp,
            path_id,
            ServerLocalPathProperties::default(),
        );
        let (commands_tx, commands_rx) = reliable_path_command_channels(8);
        let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
        let outcome = context
            .reliable_streams
            .open_or_attach(ServerStreamOpenRequest {
                session_id,
                stream_id,
                target,
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

        let bind_path = PathSpec {
            underlay: UnderlayProtocol::Udp,
            endpoint: PathEndpoint {
                host: "127.0.0.1".to_string(),
                port: 0,
            },
            binding: PathBinding::default(),
            metadata: PathMetadata::default(),
        };
        let server_endpoint = UdpPathEndpoint::bind_server(&bind_path, &context)
            .await
            .expect("bind server QUIC endpoint");
        let server_addr = server_endpoint.local_addr().expect("server QUIC address");
        let client_path = PathSpec {
            underlay: UnderlayProtocol::Udp,
            endpoint: PathEndpoint {
                host: server_addr.ip().to_string(),
                port: server_addr.port(),
            },
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
            security: Arc::new(vec![context.security.clone()]),
            codec_limits: context.codec_limits,
            mux_limits: context.mux_limits,
            stream_frame_queue: 8,
            state: ClientPathState::new(ClientPathHealth {
                tcp: Vec::new(),
                udp: Vec::new(),
            }),
            carrier_network: Arc::new(SystemCarrierNetworkProvider),
            peer_status: PeerStatusBroker::new(false),
            peer_status_snapshot: PeerStatusSnapshotSource::new(Vec::new),
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
            _path_registration: path_registration,
            commands_tx,
            commands_rx,
            accepted,
            server_send: Some(server_send),
            client_recv,
            _client_send: client_send,
            _server_recv: server_recv,
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
    let path_registration = streams.register_carrier_path(
        session_id,
        UnderlayProtocol::Udp,
        path_id,
        ServerLocalPathProperties::default(),
    );
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let (commands, _receivers) = reliable_path_command_channels(8);
    let commands_for_guard = commands.clone();
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
    let accepted = accepted_rx
        .recv()
        .await
        .expect("receive accepted UDP response stream");
    let stream = accepted.stream();
    let ReliablePathStreamOutput::Switchable(binding) = &stream.output else {
        panic!("expected switchable response output");
    };
    assert_eq!(
        binding
            .sender_path_targets(TrafficClass::Throughput, MAX_RELIABLE_SERVICE_QUANTUM_BYTES)
            .len(),
        1
    );

    drop(ServerUdpReliableOutputDetachGuard {
        streams,
        session_id,
        stream_id,
        path_id,
        commands: commands_for_guard,
    });

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
    let command = recv_reliable_path_command(&mut fixture.commands_rx)
        .await
        .expect("dequeue server QUIC terminal command");
    let mut pending_frames = Vec::new();
    let mut path_proofs = PathProofTracker::default();
    let (_carrier_frames_tx, mut carrier_frames) = mpsc::channel(1);
    let mut deferred_input = None;

    let should_close = drain_server_udp_reliable_commands(
        command,
        &mut fixture.commands_rx,
        fixture.server_send.as_mut().expect("server QUIC sender"),
        &fixture.context,
        fixture.session_id,
        fixture.stream_id,
        fixture.path_id,
        &fixture._path_registration,
        &fixture.commands_tx,
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
            udp_path_read_frame(&mut fixture.client_recv, fixture.context.codec_limits),
        )
        .await
        .expect("server QUIC reset timeout")
        .expect("read server QUIC terminal reset"),
        Frame::StreamReset {
            stream_id,
            reason: ResetReason::Refused,
        }
    );
    assert_eq!(
        fixture.attached_output_count(),
        0,
        "the matching terminal writer detaches its exact response output"
    );
    assert!(
        tokio::time::timeout(
            Duration::from_secs(5),
            udp_path_read_frame(&mut fixture.client_recv, fixture.context.codec_limits),
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
    let command = recv_reliable_path_command(&mut fixture.commands_rx)
        .await
        .expect("dequeue mismatched server QUIC terminal command");
    let command_debt = reliable_path_command_pending_bytes(&command) as u64;
    assert_eq!(fixture.commands_tx.pending_bytes(), command_debt);
    let output_guard = ServerUdpReliableOutputDetachGuard {
        streams: fixture.context.reliable_streams.clone(),
        session_id: fixture.session_id,
        stream_id: fixture.stream_id,
        path_id: fixture.path_id,
        commands: fixture.commands_tx.clone(),
    };
    let mut server_send = fixture.server_send.take().expect("server QUIC sender");
    let mut pending_frames = Vec::new();
    let mut path_proofs = PathProofTracker::default();
    let (_carrier_frames_tx, mut carrier_frames) = mpsc::channel(1);
    let mut deferred_input = None;

    let error = drain_server_udp_reliable_commands(
        command,
        &mut fixture.commands_rx,
        &mut server_send,
        &fixture.context,
        fixture.session_id,
        fixture.stream_id,
        fixture.path_id,
        &fixture._path_registration,
        &fixture.commands_tx,
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
    drop(output_guard);
    drop(server_send);
    assert_eq!(
        fixture.attached_output_count(),
        0,
        "the enclosing stream guard detaches the actual attachment on writer failure"
    );
    assert!(
        tokio::time::timeout(
            Duration::from_secs(5),
            udp_path_read_frame(&mut fixture.client_recv, fixture.context.codec_limits),
        )
        .await
        .expect("failed server QUIC stream close timeout")
        .is_err(),
        "dropping the failed stream-local writer must fail the QUIC stream closed"
    );
}
