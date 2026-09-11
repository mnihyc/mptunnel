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
    Frame, OffsetRange, PathId, PathUsage, ResetReason, SessionId, StreamAttachmentPhase,
    StreamDemandHint, StreamId, StreamReturnPlan, TargetAddr, UnderlayProtocol,
};
use crate::runtime::ReliableSendStream;
use crate::runtime::error::RuntimeError;
use crate::runtime::node::server::{ServerIdentityRuntime, new_identity_runtime};
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, ReliablePathCommandSender,
    recv_reliable_path_command, reliable_path_command_channels,
    reliable_path_command_pending_bytes, try_recv_reliable_path_priority_command,
};
use crate::runtime::path::proof::PathProofTracker;
use crate::runtime::path::quic::client::ClientUdpCarrierReconciliation;
use crate::runtime::path::quic::client::ClientUdpPathSessionRuntime;
use crate::runtime::path::quic::client_stream::run_client_udp_stream;
use crate::runtime::path::quic::io::{
    UdpPathConnection, UdpPathEndpoint, UdpPathRecvStream, UdpPathSendStream,
    udp_path_finish_stream, udp_path_max_stream_payload_bytes, udp_path_read_frame,
    udp_path_write_frame,
};
use crate::runtime::path::quic::server::handle_server_udp_bidi_stream;
use crate::runtime::path::quic::server_writer::{
    drain_one_server_udp_command_while_input_deferred, drain_server_udp_reliable_commands,
};
use crate::runtime::path::server_context::ServerPathContext;
use crate::runtime::path::{
    AcceptedServerDatagramFlow, ClientPathHealth, ClientPathHealthRecord, ClientPathState,
    PathProofObservation, ServerCarrierPathRegistration, ServerDatagramOpenError,
    ServerDatagramOpenRequest, ServerDatagramPort, ServerDatagramPortBackend,
    ServerDatagramTombstone, ServerDatagramTombstoneCache, ServerLocalPathProperties,
    ServerStreamOpenOutcome, ServerStreamOpenRequest, ServerStreamPathAttachment,
    ServerTargetAdmission,
};
use crate::runtime::peer_status::{PeerStatusBroker, PeerStatusSnapshotSource};
use crate::runtime::sender::{
    PreparedResponseSource, ResponseProductState, ServerReinjectionOutputIdentity,
    ServerResponseSenderService, SharedResponseProduct, publish_prepared_response_work,
};
use crate::runtime::stream::{
    AcceptedServerReliableStream, ReliablePathStream, ReliablePathStreamOutput,
    ServerReliableStreamRegistry,
};
use crate::scheduler::TrafficClass;
use crate::transport::{
    CarrierEndpoint as PathEndpoint, CarrierPathIdentity, CarrierSocket, CarrierSocketRequest,
    PathBinding, PathMetadata, PathSpec, SystemCarrierNetworkProvider,
};
use bytes::Bytes;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

struct PolicyDenyQuicDatagramBackend;

impl ServerDatagramPortBackend for PolicyDenyQuicDatagramBackend {
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
struct ScriptedQuicDatagramBackend {
    opens: Mutex<HashMap<u16, usize>>,
    feedback: Arc<std::sync::atomic::AtomicUsize>,
    block_first_reject: std::sync::atomic::AtomicBool,
    first_reject_entered: tokio::sync::Notify,
    release_first_reject: tokio::sync::Notify,
}

impl ScriptedQuicDatagramBackend {
    fn opens_for(&self, port: u16) -> usize {
        self.opens
            .lock()
            .expect("scripted QUIC datagram opens")
            .get(&port)
            .copied()
            .unwrap_or(0)
    }
}

impl ServerDatagramPortBackend for ScriptedQuicDatagramBackend {
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
            .expect("scripted QUIC datagram opens")
            .entry(request.target.port())
            .or_default() += 1;
        let port = request.target.port();
        let block_first_reject = port == 81
            && self
                .block_first_reject
                .load(std::sync::atomic::Ordering::Relaxed);
        let feedback = self.feedback.clone();
        Box::pin(async move {
            if block_first_reject {
                self.first_reject_entered.notify_one();
                self.release_first_reject.notified().await;
            }
            match port {
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
                        Arc::new(std::sync::atomic::AtomicBool::new(false)),
                        crate::runtime::path::ServerSessionRetirement::active_for_test(),
                        (),
                    ))
                }
            }
        })
    }
}

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
        Self::open_with_max_streams(stream_id, None).await
    }

    async fn open_with_max_streams(stream_id: StreamId, max_streams: Option<usize>) -> Self {
        Self::open_with_accept_receiver(stream_id, max_streams)
            .await
            .0
    }

    async fn open_with_accept_receiver(
        stream_id: StreamId,
        max_streams: Option<usize>,
    ) -> (Self, mpsc::UnboundedReceiver<AcceptedServerReliableStream>) {
        Self::open_with_native_authority(stream_id, max_streams, false).await
    }

    async fn open_with_native_authority(
        stream_id: StreamId,
        max_streams: Option<usize>,
        bind_native_authority: bool,
    ) -> (Self, mpsc::UnboundedReceiver<AcceptedServerReliableStream>) {
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
        if let Some(max_streams) = max_streams {
            context.mux_limits.max_streams = max_streams;
            context.max_udp_flows_per_session = max_streams;
        }
        let mut client_mux_limits = context.mux_limits;
        if max_streams.is_some() {
            client_mux_limits.max_streams = MuxLimits::default().max_streams;
        }
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
        let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));

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
            mux_limits: client_mux_limits,
            stream_frame_queue: 8,
            state: ClientPathState::new(ClientPathHealth::new(Vec::new(), Vec::new())),
            carrier_network: Arc::new(SystemCarrierNetworkProvider),
            peer_status: PeerStatusBroker::new(false),
            peer_status_snapshot: PeerStatusSnapshotSource::new(|| Some(Vec::new())),
            authenticated_carriers: crate::runtime::path::AuthenticatedCarrierInventory::default(),
            ip_tunnels: crate::runtime::tun_l3::ClientIpTunnelHub::default(),
            reconciliation: ClientUdpCarrierReconciliation::new(),
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

        let (commands_tx, commands_rx) = reliable_path_command_channels(8);
        let commands_tx = if bind_native_authority {
            let scope = crate::model::carrier_rate_authority::CarrierRateAuthorityScope::new(
                path_registration.path_instance_id(),
                crate::protocol::PathMetricDirection::ServerToClient,
            );
            let authority = server_connection
                .bind_native_rate_authority(
                    scope,
                    ServerLocalPathProperties::default().startup_rate_prior,
                )
                .await
                .expect("bind the actual protected QUIC writer authority");
            let shape = authority
                .refresh_scheduling_shape(scope)
                .expect("read actual QUIC scheduling shape");
            assert!(
                authority
                    .commit_if_current(shape.stamp(), || {
                        context
                            .reliable_streams
                            .stage_native_scheduling_shape(&path_registration, shape)
                    })
                    .expect("stage exact current QUIC scheduling shape")
            );
            commands_tx.with_native_rate_authority(authority)
        } else {
            commands_tx
        };
        let outcome = context
            .reliable_streams
            .open_or_attach(ServerStreamOpenRequest {
                session_id,
                stream_id,
                target: target.clone(),
                initial_demand: StreamDemandHint::Throughput,
                return_plan: Default::default(),
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
        assert_eq!(
            outcome,
            ServerStreamOpenOutcome::New(TrafficClass::Throughput)
        );
        let accepted = accepted_rx
            .recv()
            .await
            .expect("receive accepted server QUIC response stream");

        (
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
            },
            accepted_rx,
        )
    }

    fn attached_output_count(&self) -> usize {
        let ReliablePathStreamOutput::Switchable(binding) = &self.accepted.stream().output else {
            panic!("expected switchable server response output");
        };
        binding
            .sender_path_targets(TrafficClass::Throughput, MAX_RELIABLE_SERVICE_QUANTUM_BYTES)
            .len()
    }

    async fn drain_zero_credit_admission(&mut self) {
        let admission = recv_reliable_path_command(
            self.commands_rx
                .as_mut()
                .expect("server QUIC command receivers"),
        )
        .await
        .expect("dequeue zero-credit QUIC carrier admission");
        assert!(matches!(
            &admission,
            ReliablePathCommand::SendFrame(Frame::StreamMaxData {
                stream_id,
                max_offset: 0,
            }) if *stream_id == self.stream_id
        ));
        let mut path_proofs = PathProofTracker::default();
        assert!(
            !drain_one_server_udp_command_while_input_deferred(
                admission,
                self.commands_rx
                    .as_mut()
                    .expect("server QUIC command receivers"),
                self.server_send.as_mut().expect("server QUIC sender"),
                &self.context,
                self.stream_id,
                &self._path_registration,
                &mut path_proofs,
            )
            .await
            .expect("write zero-credit QUIC carrier admission")
        );
        assert_eq!(
            udp_path_read_frame(
                self.client_recv.as_mut().expect("client QUIC receiver"),
                self.context.codec_limits,
            )
            .await
            .expect("read zero-credit QUIC carrier admission"),
            Frame::StreamMaxData {
                stream_id: self.stream_id,
                max_offset: 0,
            }
        );
    }

    async fn publish_latency_source(
        &mut self,
        payload: Bytes,
    ) -> (SharedResponseProduct, ReliablePathStream) {
        let mut stream = self.accepted.take_stream();
        let ReliablePathStreamOutput::Switchable(binding) = &stream.output else {
            panic!("shared response binding");
        };
        let binding = binding.clone();
        let limits = self.context.mux_limits;
        let lane = TrafficClass::Latency;
        let quantum = crate::model::capacity::adaptive_reliable_relay_chunk_bytes_with_frame_limit(
            None,
            lane,
            limits,
            udp_path_max_stream_payload_bytes(self.context.codec_limits, limits),
        );
        assert!(!payload.is_empty() && payload.len() <= quantum);
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(self.stream_id, limits, 0);
        self.context
            .reliable_streams
            .route_frame(
                &self._path_registration,
                self.stream_id,
                Frame::StreamMaxData {
                    stream_id: self.stream_id,
                    max_offset: limits.max_stream_window_bytes,
                },
            )
            .await
            .expect("route actual response receive credit");
        let Frame::StreamMaxData { max_offset, .. } =
            stream.recv_frame().await.expect("receive routed credit")
        else {
            panic!("expected response receive credit");
        };
        send_stream.update_max_offset(max_offset);
        assert!(send_stream.send_credit_bytes() >= payload.len());
        let owner = SharedResponseProduct::new(
            ResponseProductState {
                sender: ServerResponseSenderService::new(self.session_id, self.stream_id),
                send_stream,
                last_send_ack: Default::default(),
                prepared: PreparedResponseSource::new(lane, quantum),
            },
            binding,
        );
        {
            let mut state = owner.lock();
            state.sender.enqueue_data_for_lane(payload, lane);
            publish_prepared_response_work(&mut state, &owner, lane, quantum, true);
        }
        (owner, stream)
    }

    async fn drain_normal_command(
        &mut self,
        command: ReliablePathCommand,
    ) -> Result<bool, RuntimeError> {
        let mut pending = Vec::new();
        let mut proofs = PathProofTracker::default();
        let (_input_tx, mut input_rx) = mpsc::channel(1);
        let mut deferred = None;
        drain_server_udp_reliable_commands(
            command,
            self.commands_rx.as_mut().unwrap(),
            self.server_send.as_mut().unwrap(),
            &self.context,
            self.stream_id,
            self.path_id,
            &self._path_registration,
            &mut pending,
            &mut proofs,
            &mut input_rx,
            &mut deferred,
        )
        .await
    }

    async fn finish_prepared_write(&mut self, owner: &SharedResponseProduct, payload: &Bytes) {
        // Run the same connection-owned cadence service as the real server,
        // scoped to this completion. It owns retryable Native refresh/stage/
        // fanout, including central stamp changes caused by actual control I/O.
        // Dropping these futures releases the service; no spawned task leaks.
        let metrics = crate::runtime::path::quic::metrics::run_server_quic_path_metrics(
            self.context.clone(),
            self._path_registration.clone(),
            self._server_connection.clone(),
        );
        let service = async {
            loop {
                let claimed = {
                    let state = owner.lock();
                    assert!(state.prepared.pending_error.is_none());
                    if state.send_stream.next_offset() == 0 {
                        assert_eq!(state.sender.data_bytes(), payload.len());
                        assert_eq!(state.send_stream.reinjection_bytes(), 0);
                    }
                    state.send_stream.next_offset()
                };
                if claimed != 0 {
                    assert_eq!(claimed, payload.len() as u64);
                    break;
                }
                let targets = owner
                    .binding()
                    .sender_path_targets(TrafficClass::Latency, payload.len());
                assert_eq!(targets.len(), 1);
                assert_eq!(targets[0].observation.original_data_in_flight_bytes, 0);
                assert_eq!(self.commands_tx.pending_bytes(), 0);
                assert_eq!(self.commands_tx.writer_pending_bytes(), 0);
                let notice = recv_reliable_path_command(self.commands_rx.as_mut().unwrap())
                    .await
                    .expect("actual Native cadence wakes the existing parked notice");
                assert!(matches!(&notice, ReliablePathCommand::PreparedOriginal(_)));
                assert!(!self.drain_normal_command(notice).await.unwrap());
            }
            assert_eq!(
                udp_path_read_frame(
                    self.client_recv.as_mut().unwrap(),
                    self.context.codec_limits
                )
                .await
                .unwrap(),
                Frame::StreamData {
                    stream_id: self.stream_id,
                    offset: 0,
                    payload: payload.clone(),
                }
            );
        };
        tokio::pin!(metrics, service);
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                () = &mut service => {}
                () = &mut metrics => panic!("Native metrics service ended before response completion"),
            }
        })
        .await
        .expect("unchanged response completion guard includes Native retries and peer read");
    }
}

#[tokio::test]
async fn server_quic_prepared_latency_waits_for_deferred_probe_then_writes_prefix() {
    let stream_id = StreamId(406);
    let (mut fixture, _accepted_rx) =
        ServerUdpTerminalWriterFixture::open_with_native_authority(stream_id, None, true).await;
    fixture.drain_zero_credit_admission().await;
    let payload = Bytes::from_static(b"unclaimed latency response");
    let (owner, _stream) = fixture.publish_latency_source(payload.clone()).await;
    let instance = fixture._path_registration.path_instance_id();
    let ready = fixture
        .commands_rx
        .as_mut()
        .unwrap()
        .writer_ready_boundary(instance)
        .unwrap()
        .receipt();
    let notice = try_recv_reliable_path_priority_command(fixture.commands_rx.as_mut().unwrap())
        .expect("actual latency publisher uses the priority lane");
    assert!(matches!(&notice, ReliablePathCommand::PreparedOriginal(_)));
    assert_eq!(reliable_path_command_pending_bytes(&notice), 0);
    let probe = Frame::StreamRequalifyData {
        stream_id,
        probe_id: 24,
        offset: 8192,
        payload: Bytes::from(vec![0x5c; 256]),
    };
    let mut deferred: Option<Result<Frame, RuntimeError>> = Some(Ok(probe.clone()));
    let mut proofs = PathProofTracker::default();
    let continued = drain_one_server_udp_command_while_input_deferred(
        notice,
        fixture.commands_rx.as_mut().unwrap(),
        fixture.server_send.as_mut().unwrap(),
        &fixture.context,
        stream_id,
        &fixture._path_registration,
        &mut proofs,
    )
    .await;
    {
        let state = owner.lock();
        assert_eq!(state.sender.data_bytes(), payload.len());
        assert_eq!(state.send_stream.next_offset(), 0);
        assert_eq!(state.send_stream.reinjection_bytes(), 0);
    }
    assert_eq!(fixture.commands_tx.pending_bytes(), 0);
    assert_eq!(fixture.commands_tx.writer_pending_bytes(), 0);
    assert!(
        !ready.is_current(),
        "retained input occupies the actual writer"
    );
    assert!(matches!(&deferred, Some(Ok(frame)) if frame == &probe));
    assert!(
        matches!(continued, Ok(false)),
        "legal priority source must park, not terminate QUIC: {continued:?}"
    );
    assert!(
        try_recv_reliable_path_priority_command(fixture.commands_rx.as_mut().unwrap()).is_none(),
        "unchanged occupied boundary must not requeue the notice"
    );

    // Preserve the existing exact-ACK escape while this same input slot is full.
    let ack = Frame::StreamRequalifyAck {
        stream_id,
        probe_id: 23,
        offset: 4096,
        payload_bytes: 256,
    };
    fixture
        .commands_tx
        .try_enqueue_admitted_frame(ack.clone(), TrafficClass::Control)
        .unwrap();
    let command =
        try_recv_reliable_path_priority_command(fixture.commands_rx.as_mut().unwrap()).unwrap();
    assert!(
        !drain_one_server_udp_command_while_input_deferred(
            command,
            fixture.commands_rx.as_mut().unwrap(),
            fixture.server_send.as_mut().unwrap(),
            &fixture.context,
            stream_id,
            &fixture._path_registration,
            &mut proofs,
        )
        .await
        .unwrap()
    );
    assert_eq!(
        udp_path_read_frame(
            fixture.client_recv.as_mut().unwrap(),
            fixture.context.codec_limits
        )
        .await
        .unwrap(),
        ack
    );
    assert!(matches!(deferred.take(), Some(Ok(frame)) if frame == probe));
    fixture
        .commands_rx
        .as_mut()
        .unwrap()
        .writer_ready_boundary(instance)
        .unwrap();
    let notice = try_recv_reliable_path_priority_command(fixture.commands_rx.as_mut().unwrap())
        .expect("actual Ready publication wakes the parked source");
    assert!(matches!(&notice, ReliablePathCommand::PreparedOriginal(_)));
    assert!(!fixture.drain_normal_command(notice).await.unwrap());
    fixture.finish_prepared_write(&owner, &payload).await;
    let state = owner.lock();
    assert_eq!(state.sender.data_bytes(), 0);
    assert_eq!(state.send_stream.next_offset(), payload.len() as u64);
    assert_eq!(state.send_stream.reinjection_bytes(), payload.len());
    assert_eq!(fixture.commands_tx.pending_bytes(), 0);
    assert_eq!(fixture.commands_tx.writer_pending_bytes(), 0);
}

#[tokio::test]
async fn server_quic_prepared_latency_busy_preserves_idle_writer_epoch() {
    use futures::FutureExt;

    let stream_id = StreamId(407);
    let (mut fixture, _accepted_rx) =
        ServerUdpTerminalWriterFixture::open_with_native_authority(stream_id, None, true).await;
    fixture.drain_zero_credit_admission().await;
    let payload = Bytes::from_static(b"response after Product unlock");
    let (owner, _stream) = fixture.publish_latency_source(payload.clone()).await;
    let instance = fixture._path_registration.path_instance_id();
    let ready = fixture
        .commands_rx
        .as_mut()
        .unwrap()
        .writer_ready_boundary(instance)
        .unwrap()
        .receipt();
    let command =
        try_recv_reliable_path_priority_command(fixture.commands_rx.as_mut().unwrap()).unwrap();
    assert!(matches!(&command, ReliablePathCommand::PreparedOriginal(_)));
    let state = owner.lock();
    // Poll only the real metadata drain. Busy must return without an await or
    // I/O while Product is owned; this is not a wall-clock scheduling assertion.
    assert!(matches!(
        fixture.drain_normal_command(command).now_or_never(),
        Some(Ok(false))
    ));
    assert!(
        ready.is_current(),
        "metadata refusal is not writer occupancy"
    );
    assert_eq!(state.sender.data_bytes(), payload.len());
    assert_eq!(state.send_stream.next_offset(), 0);
    assert_eq!(state.send_stream.reinjection_bytes(), 0);
    assert!(
        try_recv_reliable_path_priority_command(fixture.commands_rx.as_mut().unwrap()).is_none()
    );
    drop(state);
    let notice = try_recv_reliable_path_priority_command(fixture.commands_rx.as_mut().unwrap())
        .expect("Product unlock wakes the parked wait and requeues the same weak notice");
    assert!(ready.is_current());
    assert!(!fixture.drain_normal_command(notice).await.unwrap());
    fixture.finish_prepared_write(&owner, &payload).await;
    assert!(
        !ready.is_current(),
        "actual claim/write consumes the idle epoch"
    );
    let state = owner.lock();
    assert_eq!(state.sender.data_bytes(), 0);
    assert_eq!(state.send_stream.next_offset(), payload.len() as u64);
    assert_eq!(fixture.commands_tx.pending_bytes(), 0);
    assert_eq!(fixture.commands_tx.writer_pending_bytes(), 0);
}

#[tokio::test]
async fn server_quic_prepared_repair_successors_cross_split_writer_before_copy_ack() {
    use crate::model::capacity::{
        adaptive_reliable_relay_reinjection_bytes, reliable_path_startup_sample_limit_bytes,
    };
    use crate::model::path::CarrierPathKey;
    use crate::model::timing::reliable_data_retransmission_interval;

    let stream_id = StreamId(408);
    let (mut fixture, _accepted_rx) =
        ServerUdpTerminalWriterFixture::open_with_native_authority(stream_id, None, true).await;
    fixture.drain_zero_credit_admission().await;
    let mut product_stream = fixture.accepted.take_stream();
    let ReliablePathStreamOutput::Switchable(binding) = &product_stream.output else {
        panic!("shared response binding");
    };
    let binding = binding.clone();
    let lane = TrafficClass::Throughput;
    let limits = fixture.context.mux_limits;
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receiver) = reliable_path_command_channels(8);
    binding.attach(
        original_key.underlay,
        original_key.path_id,
        original_commands,
        lane,
    );
    let targets = binding.sender_path_targets(lane, 1);
    let repair_target = targets
        .iter()
        .find(|target| target.observation.key.underlay == UnderlayProtocol::Udp)
        .unwrap();
    let repair_identity = ServerReinjectionOutputIdentity {
        key: repair_target.observation.key,
        incarnation: repair_target.observation.incarnation,
    };
    // Three retained fixture frames fit one default startup sample. No rate,
    // capacity, qualification receipt, or Native stamp is fabricated.
    let payload_bytes = targets
        .iter()
        .map(|target| {
            adaptive_reliable_relay_reinjection_bytes(
                Some(target.observation.snapshot),
                lane,
                limits,
            )
        })
        .min()
        .unwrap()
        .min(usize::try_from(reliable_path_startup_sample_limit_bytes(limits)).unwrap() / 3)
        .min(udp_path_max_stream_payload_bytes(
            fixture.context.codec_limits,
            limits,
        ));
    assert!(payload_bytes > 0);
    let original_interval = targets
        .iter()
        .find(|target| target.observation.key == original_key)
        .map(|target| {
            reliable_data_retransmission_interval(
                Some(original_key.underlay),
                Some(target.observation.snapshot),
            )
        })
        .unwrap();
    let mut send_stream = ReliableSendStream::new_with_initial_max_offset(stream_id, limits, 0);
    fixture
        .context
        .reliable_streams
        .route_frame(
            &fixture._path_registration,
            stream_id,
            Frame::StreamMaxData {
                stream_id,
                max_offset: limits.max_stream_window_bytes,
            },
        )
        .await
        .unwrap();
    let Frame::StreamMaxData { max_offset, .. } = product_stream.recv_frame().await.unwrap() else {
        panic!("actual response receive credit");
    };
    send_stream.update_max_offset(max_offset);
    let mut frames = Vec::new();
    for index in 0..3 {
        let frame = send_stream
            .send_data(Bytes::from(vec![0x70 + index; payload_bytes]))
            .unwrap();
        binding.record_original_flight(original_key, &frame);
        frames.push(frame);
    }
    binding.record_reinjected_flight(repair_identity.key, &frames[0]);
    binding.age_original_flights_for_test(original_interval + original_interval);
    let owner = SharedResponseProduct::new(
        ResponseProductState {
            sender: ServerResponseSenderService::new(fixture.session_id, stream_id),
            send_stream,
            last_send_ack: Default::default(),
            prepared: PreparedResponseSource::new(lane, payload_bytes),
        },
        binding,
    );
    {
        let mut state = owner.lock();
        publish_prepared_response_work(&mut state, &owner, lane, payload_bytes, true);
    }
    let initial_credit = owner.lock().send_stream.send_credit_bytes();
    let repair_commands = fixture
        .commands_rx
        .as_mut()
        .unwrap()
        .take_repair_receiver(stream_id);

    // Reuse the real connection's second HTTP/3 stream for the existing repair
    // writer. Consume its opening fixture frame before entering the repair loop.
    let (mut peer_repair_send, mut peer_repair_recv) =
        fixture._client_connection.open_bi().await.unwrap();
    udp_path_write_frame(
        &mut peer_repair_send,
        &Frame::Ping { nonce: 408 },
        fixture.context.codec_limits,
    )
    .await
    .unwrap();
    let (repair_send, mut repair_recv) = tokio::time::timeout(
        Duration::from_secs(5),
        fixture._server_connection.accept_bi(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        udp_path_read_frame(&mut repair_recv, fixture.context.codec_limits)
            .await
            .unwrap(),
        Frame::Ping { nonce: 408 }
    );
    let codec_limits = fixture.context.codec_limits;
    let repair = super::super::repair::run_repair_channel(
        repair_send,
        repair_recv,
        repair_commands,
        stream_id,
        codec_limits,
        |_| async { panic!("the peer sends no Product acknowledgement or repair data") },
    );
    let metrics = crate::runtime::path::quic::metrics::run_server_quic_path_metrics(
        fixture.context.clone(),
        fixture._path_registration.clone(),
        fixture._server_connection.clone(),
    );
    let ordinary = async {
        loop {
            fixture
                .commands_rx
                .as_mut()
                .unwrap()
                .writer_ready_boundary(fixture._path_registration.path_instance_id());
            let command = recv_reliable_path_command(fixture.commands_rx.as_mut().unwrap())
                .await
                .expect("ordinary writer remains owned");
            assert!(!fixture.drain_normal_command(command).await.unwrap());
        }
    };
    let received = async {
        // Current Native service can split a retained frame into smaller
        // writes. Verify its exact byte ranges without prescribing packet or
        // Product frame size to the real writer.
        let expected = frames[1..]
            .iter()
            .flat_map(|frame| {
                let Frame::StreamData { payload, .. } = frame else {
                    unreachable!("retained fixture data")
                };
                payload.iter().copied()
            })
            .collect::<Vec<_>>();
        let mut received_bytes = 0;
        let mut service_frames = 0;
        while received_bytes < expected.len() {
            let Frame::StreamData {
                stream_id: received_stream,
                offset,
                payload,
            } = udp_path_read_frame(&mut peer_repair_recv, codec_limits)
                .await
                .unwrap()
            else {
                panic!("repair writer must deliver retained data")
            };
            assert_eq!(received_stream, stream_id);
            assert_eq!(offset, (payload_bytes + received_bytes) as u64);
            assert!(!payload.is_empty());
            let end = received_bytes + payload.len();
            assert!(
                end <= expected.len(),
                "repair cannot exceed retained history"
            );
            assert!(
                payload.as_ref() == &expected[received_bytes..end],
                "repair payload must match the exact retained subrange"
            );
            received_bytes = end;
            service_frames += 1;
            assert_eq!(owner.lock().send_stream.data_ack_frontier(), 0);
        }
        assert!(service_frames >= 2);
    };
    tokio::pin!(repair, metrics, ordinary, received);
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            () = &mut received => {}
            result = &mut repair => panic!("repair writer ended before both frames: {result:?}"),
            () = &mut metrics => panic!("Native metrics ended before both frames"),
            () = &mut ordinary => unreachable!("ordinary writer loop remains owned"),
        }
    })
    .await
    .expect("existing completion guard includes both actual writer handoffs and peer reads");
    let state = owner.lock();
    assert_eq!(state.send_stream.data_ack_frontier(), 0);
    assert_eq!(state.send_stream.next_offset(), (3 * payload_bytes) as u64);
    assert_eq!(state.send_stream.send_credit_bytes(), initial_credit);
    assert_eq!(state.send_stream.reinjection_bytes(), 3 * payload_bytes);
    assert!(state.sender.is_empty());
    assert_eq!(
        owner
            .binding()
            .accepted_reinjected_data_in_flight_bytes_at(repair_identity),
        3 * payload_bytes
    );
}

#[tokio::test]
async fn server_quic_drains_exact_ack_without_consuming_deferred_probe_slot() {
    let stream_id = StreamId(405);
    let mut fixture = ServerUdpTerminalWriterFixture::open(stream_id).await;
    let ack = Frame::StreamRequalifyAck {
        stream_id,
        probe_id: 23,
        offset: 4096,
        payload_bytes: 256,
    };
    fixture
        .commands_tx
        .try_enqueue_admitted_frame(ack.clone(), TrafficClass::Control)
        .expect("queue exact requalification ACK");
    let initial_grant = recv_reliable_path_command(
        fixture
            .commands_rx
            .as_mut()
            .expect("server QUIC command receivers"),
    )
    .await
    .expect("dequeue pre-admission receive grant");
    let deferred_input: Option<Result<Frame, RuntimeError>> =
        Some(Ok(Frame::StreamRequalifyData {
            stream_id,
            probe_id: 24,
            offset: 8192,
            payload: Bytes::from(vec![0x5c; 256]),
        }));
    let mut path_proofs = PathProofTracker::default();

    assert!(
        !drain_one_server_udp_command_while_input_deferred(
            initial_grant,
            fixture
                .commands_rx
                .as_mut()
                .expect("server QUIC command receivers"),
            fixture.server_send.as_mut().expect("server QUIC sender"),
            &fixture.context,
            stream_id,
            &fixture._path_registration,
            &mut path_proofs,
        )
        .await
        .expect("drain receive grant while probe is retained")
    );
    assert_eq!(
        udp_path_read_frame(
            fixture.client_recv.as_mut().expect("client QUIC receiver"),
            fixture.context.codec_limits,
        )
        .await
        .expect("read pre-admission receive grant"),
        Frame::StreamMaxData {
            stream_id,
            max_offset: 0,
        }
    );
    let ack_command = recv_reliable_path_command(
        fixture
            .commands_rx
            .as_mut()
            .expect("server QUIC command receivers"),
    )
    .await
    .expect("dequeue exact ACK command");
    assert!(
        !drain_one_server_udp_command_while_input_deferred(
            ack_command,
            fixture
                .commands_rx
                .as_mut()
                .expect("server QUIC command receivers"),
            fixture.server_send.as_mut().expect("server QUIC sender"),
            &fixture.context,
            stream_id,
            &fixture._path_registration,
            &mut path_proofs,
        )
        .await
        .expect("drain exact ACK while probe is retained")
    );
    assert!(matches!(
        deferred_input,
        Some(Ok(Frame::StreamRequalifyData { probe_id: 24, .. }))
    ));
    assert_eq!(
        udp_path_read_frame(
            fixture.client_recv.as_mut().expect("client QUIC receiver"),
            fixture.context.codec_limits,
        )
        .await
        .expect("read exact requalification ACK"),
        ack
    );
}

#[tokio::test]
async fn server_quic_live_attachment_requalifies_without_replacement() {
    tokio::time::timeout(Duration::from_secs(1), async {
        let stream_id = StreamId(415);
        let mut fixture = ServerUdpTerminalWriterFixture::open(stream_id).await;
        fixture.drain_zero_credit_admission().await;
        let binding = match &fixture.accepted.stream().output {
            ReliablePathStreamOutput::Switchable(binding) => binding.clone(),
            _ => panic!("expected switchable server response output"),
        };
        let initial = binding
            .sender_path_targets(TrafficClass::Throughput, 64)
            .into_iter()
            .find(|target| target.observation.key.underlay == UnderlayProtocol::Udp)
            .expect("live QUIC response attachment")
            .observation;
        let identity = ServerReinjectionOutputIdentity {
            key: initial.key,
            incarnation: initial.incarnation,
        };
        assert_eq!(
            initial.path_instance_id,
            fixture._path_registration.path_instance_id()
        );
        assert!(!binding.output_is_stale(identity), "starts Qualified");
        let connection_owner = fixture._server_connection.write_activity_notify();

        let (alternate, alternate_rx) = reliable_path_command_channels(8);
        let _ = binding.attach(
            UnderlayProtocol::Tcp,
            PathId(1),
            alternate,
            TrafficClass::Throughput,
        );
        let mut send_stream = ReliableSendStream::new(stream_id, fixture.context.mux_limits);
        let old = send_stream
            .send_data(Bytes::from_static(b"qualified-before-stale"))
            .expect("prepare old original data");
        binding.record_original_flight(initial.key, &old);
        assert!(binding.mark_output_stale(identity, TrafficClass::Throughput));
        assert!(binding.output_is_stale(identity), "becomes Stale");
        drop(alternate_rx);
        assert_eq!(
            binding
                .try_enqueue_response_requalification_probe(
                    &send_stream,
                    TrafficClass::Throughput,
                    64,
                )
                .expect("queue response probe")
                .published_payload_bytes(),
            Some(22),
        );
        assert!(
            binding.response_requalification_deadline().is_some(),
            "becomes Requalifying"
        );

        let mut product_stream = fixture.accepted.take_stream();
        let actor = tokio::spawn(run_server_udp_reliable_stream_loop(
            fixture.server_send.take().expect("server QUIC sender"),
            fixture.server_recv.take().expect("server QUIC receiver"),
            ServerUdpReliableStreamLoop {
                context: fixture.context.clone(),
                session_id: fixture.session_id,
                path_id: fixture.path_id,
                path_registration: fixture._path_registration.clone(),
                stream_id,
                target: fixture.target.clone(),
                commands_tx: fixture.commands_tx.clone(),
                commands_rx: fixture.commands_rx.take().expect("server QUIC commands"),
                path_proofs: PathProofTracker::default(),
            },
        ));
        let client_recv = fixture.client_recv.as_mut().expect("client QUIC receiver");
        let (probe_id, offset, payload_bytes) = loop {
            let frame = udp_path_read_frame(client_recv, fixture.context.codec_limits)
                .await
                .expect("read response probe");
            if let Frame::StreamRequalifyData {
                probe_id,
                offset,
                payload,
                ..
            } = frame
            {
                assert_eq!(payload, Bytes::from_static(b"qualified-before-stale"));
                break (probe_id, offset, payload.len() as u32);
            }
        };
        let client_send = fixture.client_send.as_mut().expect("client QUIC sender");
        udp_path_write_frame(
            client_send,
            &Frame::StreamRequalifyAck {
                stream_id,
                probe_id,
                offset,
                payload_bytes,
            },
            fixture.context.codec_limits,
        )
        .await
        .expect("return exact response probe ACK");
        while binding.response_requalification_deadline().is_some() {
            tokio::task::yield_now().await;
        }
        assert!(
            !binding.output_is_stale(identity),
            "Acquiring admits fresh original data"
        );
        let old_range = OffsetRange::new(0, payload_bytes as u64).expect("old range");
        assert!(
            binding
                .release_normalized_acked_ranges(&[old_range])
                .path_progress_outputs
                .is_empty()
        );

        let fresh = send_stream
            .send_data(Bytes::from_static(b"fresh-after-probe"))
            .expect("prepare fresh original data");
        let Frame::StreamData {
            offset,
            ref payload,
            ..
        } = fresh
        else {
            panic!("expected STREAM_DATA");
        };
        let fresh_range =
            OffsetRange::new(offset, offset + payload.len() as u64).expect("fresh range");
        binding.record_original_flight(initial.key, &fresh);
        fixture
            .commands_tx
            .send_stream_ordered_frame(fresh.clone(), TrafficClass::Throughput)
            .await
            .expect("send fresh data on same attachment");
        while udp_path_read_frame(client_recv, fixture.context.codec_limits)
            .await
            .expect("read fresh data")
            != fresh
        {}
        udp_path_write_frame(
            client_send,
            &Frame::StreamAck {
                stream_id,
                scope_start: None,
                ranges: vec![fresh_range],
            },
            fixture.context.codec_limits,
        )
        .await
        .expect("return fresh ACK");
        let Frame::StreamAck { ranges, .. } =
            product_stream.recv_frame().await.expect("route fresh ACK")
        else {
            panic!("expected routed fresh STREAM_ACK");
        };
        assert_eq!(
            binding
                .release_normalized_acked_ranges(&ranges)
                .path_progress_outputs
                .as_slice(),
            &[identity]
        );
        assert!(!binding.output_is_stale(identity), "restores Qualified");
        let final_state = binding
            .sender_path_targets(TrafficClass::Throughput, 64)
            .into_iter()
            .find(|target| target.observation.key == identity.key)
            .expect("same live QUIC attachment")
            .observation;
        assert_eq!(final_state.path_instance_id, initial.path_instance_id);
        assert_eq!(final_state.incarnation, initial.incarnation);
        assert!(Arc::ptr_eq(
            &connection_owner,
            &fixture._server_connection.write_activity_notify()
        ));
        assert!(!fixture._server_connection.is_closed());
        actor.abort();
        let _ = actor.await;
    })
    .await
    .expect("live same-carrier requalification must finish within one second");
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
            initial_demand: StreamDemandHint::Throughput,
            return_plan: Default::default(),
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
    assert_eq!(
        outcome,
        ServerStreamOpenOutcome::New(TrafficClass::Throughput)
    );
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
    fixture.drain_zero_credit_admission().await;
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
    fixture.drain_zero_credit_admission().await;
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
async fn client_quic_closed_product_recipient_preserves_ordered_terminal_writer() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let stream_id = StreamId(406);
        let mut fixture = ServerUdpTerminalWriterFixture::open(stream_id).await;
        let mut server_send = fixture.server_send.take().expect("server sender");
        let mut server_recv = fixture.server_recv.take().expect("server receiver");
        let (commands_tx, commands_rx) = reliable_path_command_channels(8);
        let (frames_tx, frames_rx) = mpsc::channel(1);
        let state = ClientPathState::new(ClientPathHealth::new(
            Vec::new(),
            vec![ClientPathHealthRecord::default()],
        ));
        let limits = fixture.context.codec_limits;
        drop(frames_rx);
        let actor = tokio::spawn(run_client_udp_stream(
            fixture.client_send.take().expect("client sender"),
            fixture.client_recv.take().expect("client receiver"),
            stream_id,
            0,
            next_carrier_path_instance_id(),
            limits,
            fixture.context.mux_limits,
            8,
            state,
            commands_rx,
            frames_tx,
        ));
        assert_eq!(
            udp_path_read_frame(&mut server_recv, limits).await.unwrap(),
            Frame::Ping { nonce: 1 }
        );
        udp_path_write_frame(
            &mut server_send,
            &Frame::StreamAck {
                stream_id,
                scope_start: None,
                ranges: Vec::new(),
            },
            limits,
        )
        .await
        .unwrap();
        // Same-native-stream ordering makes Pong proof that the late ACK was
        // processed after Product retirement without cancelling the writer.
        udp_path_write_frame(&mut server_send, &Frame::Ping { nonce: 406 }, limits)
            .await
            .unwrap();
        assert_eq!(
            udp_path_read_frame(&mut server_recv, limits)
                .await
                .expect("retired Product input must preserve the independent writer"),
            Frame::Pong { nonce: 406 }
        );
        let terminal = [
            Frame::StreamFin {
                stream_id,
                final_offset: 0,
            },
            Frame::StreamDetach { stream_id },
        ];
        for frame in &terminal {
            commands_tx
                .send_stream_ordered_frame(frame.clone(), TrafficClass::Throughput)
                .await
                .unwrap();
        }
        commands_tx
            .send_stream_ordered_close(stream_id, TrafficClass::Throughput)
            .await
            .unwrap();
        for frame in terminal {
            assert_eq!(
                udp_path_read_frame(&mut server_recv, limits).await.unwrap(),
                frame
            );
        }
        actor.await.unwrap();
        assert!(!fixture._client_connection.is_closed());
    })
    .await
    .expect("retired input and ordered terminal writer must complete");
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
            scope_start: None,
            ranges: Vec::new(),
        }))
        .await
        .expect("client writer remains after peer send-half finish");
    assert!(matches!(
        udp_path_read_frame(&mut server_recv, codec_limits).await,
        Ok(Frame::StreamAck {
            stream_id: ack_stream_id,
            scope_start: None,
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
async fn client_quic_idle_writer_routes_both_requalification_frames() {
    let stream_id = StreamId(410);
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
    let probe = Frame::StreamRequalifyData {
        stream_id,
        probe_id: 41,
        offset: 1024,
        payload: Bytes::from_static(b"idle-probe"),
    };
    let ack = Frame::StreamRequalifyAck {
        stream_id,
        probe_id: 42,
        offset: 2048,
        payload_bytes: 512,
    };
    for frame in [&probe, &ack] {
        udp_path_write_frame(&mut server_send, frame, codec_limits)
            .await
            .expect("write requalification frame while client writer is idle");
    }
    assert_eq!(
        frames_rx
            .recv()
            .await
            .expect("probe route")
            .expect("probe frame"),
        probe,
    );
    assert_eq!(
        frames_rx
            .recv()
            .await
            .expect("ACK route")
            .expect("ACK frame"),
        ack,
    );

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
            return_plan: Default::default(),
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
            initial_demand: StreamDemandHint::Throughput,
            return_plan: Default::default(),
            native_rate_authority: None,
            repair_bindings: Default::default(),
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
async fn late_startup_after_final_refuses_only_the_quic_attachment() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let sibling_id = StreamId(1490);
        let stream_id = StreamId(1491);
        let (mut fixture, mut accepted_rx) =
            ServerUdpTerminalWriterFixture::open_with_accept_receiver(sibling_id, None).await;
        fixture.drain_zero_credit_admission().await;
        let opening = fixture.context.reliable_streams.register_test_carrier_path(
            fixture.session_id,
            UnderlayProtocol::Tcp,
            PathId(1),
            ServerLocalPathProperties::default(),
        );
        let (opening_commands, _opening_receiver) = reliable_path_command_channels(8);
        let plan = StreamReturnPlan {
            trigger_bytes: 58_400,
            candidate_total: 2,
            candidate_tier: PathUsage::Available,
            phase: StreamAttachmentPhase::Create,
            candidate_ordinal: 0,
        };
        fixture
            .context
            .reliable_streams
            .open_or_attach(ServerStreamOpenRequest {
                session_id: fixture.session_id,
                stream_id,
                target: fixture.target.clone(),
                initial_demand: StreamDemandHint::Throughput,
                return_plan: plan,
                attachment: ServerStreamPathAttachment {
                    path_registration: opening.clone(),
                    commands: opening_commands,
                    max_frame_payload_bytes: fixture.context.mux_limits.max_payload_bytes,
                },
                mux_limits: fixture.context.mux_limits,
            })
            .await
            .expect("CREATE accepted on carrier A");
        let _opening_owner = accepted_rx.recv().await.expect("one new logical owner");
        let sibling_actor = tokio::spawn(run_server_udp_reliable_stream_loop(
            fixture.server_send.take().expect("sibling sender"),
            fixture.server_recv.take().expect("sibling receiver"),
            ServerUdpReliableStreamLoop {
                context: fixture.context.clone(),
                session_id: fixture.session_id,
                path_id: fixture.path_id,
                path_registration: fixture._path_registration.clone(),
                stream_id: sibling_id,
                target: fixture.target.clone(),
                commands_tx: fixture.commands_tx.clone(),
                commands_rx: fixture.commands_rx.take().expect("sibling commands"),
                path_proofs: PathProofTracker::default(),
            },
        ));
        let (mut delayed_send, mut delayed_recv) = fixture
            ._client_connection
            .open_bi()
            .await
            .expect("open candidate B");
        udp_path_write_frame(
            &mut delayed_send,
            &Frame::OpenStream {
                stream_id,
                target: fixture.target.clone(),
                demand: StreamDemandHint::Throughput,
                return_plan: StreamReturnPlan {
                    phase: StreamAttachmentPhase::Startup,
                    candidate_ordinal: 1,
                    ..plan
                },
            },
            fixture.context.codec_limits,
        )
        .await
        .expect("STARTUP enters B before local timeout");
        // Delay B's native request consumption, not its phase construction.
        // The locally failed ordinal is omitted by FINAL arriving through A.
        fixture
            .context
            .reliable_streams
            .route_frame(
                &opening,
                stream_id,
                Frame::StreamReturnPlanFinal {
                    stream_id,
                    retained_ordinals: vec![0],
                },
            )
            .await
            .expect("FINAL arrives on surviving carrier A");
        let (send, recv) = fixture
            ._server_connection
            .accept_bi()
            .await
            .expect("consume delayed B request");
        handle_server_udp_bidi_stream(
            send,
            recv,
            fixture.context.clone(),
            fixture.session_id,
            fixture.path_id,
            fixture._path_registration.clone(),
        )
        .await
        .expect("obsolete enrollment is attachment-local");
        assert_eq!(
            udp_path_read_frame(&mut delayed_recv, fixture.context.codec_limits)
                .await
                .expect("refusal"),
            Frame::StreamDetach { stream_id }
        );
        assert!(matches!(
            udp_path_read_frame(&mut delayed_recv, fixture.context.codec_limits).await,
            Err(RuntimeError::QuicCarrier(
                crate::transport::quic::QuicCarrierError::StreamFinished
            ))
        ));
        assert_eq!(
            fixture.attached_output_count(),
            1,
            "sibling output remains enrolled"
        );
        let data = Frame::StreamData {
            stream_id: sibling_id,
            offset: 0,
            payload: Bytes::from_static(b"sibling"),
        };
        fixture
            .commands_tx
            .send_stream_ordered_frame(data.clone(), TrafficClass::Throughput)
            .await
            .expect("sibling still sends");
        while udp_path_read_frame(
            fixture.client_recv.as_mut().expect("sibling receiver"),
            fixture.context.codec_limits,
        )
        .await
        .expect("sibling payload survives obsolete enrollment")
            != data
        {}

        let (mut ordinary_send, mut ordinary_recv) = fixture
            ._client_connection
            .open_bi()
            .await
            .expect("new Ordinary request");
        udp_path_write_frame(
            &mut ordinary_send,
            &Frame::OpenStream {
                stream_id,
                target: fixture.target.clone(),
                demand: StreamDemandHint::Throughput,
                return_plan: StreamReturnPlan {
                    phase: StreamAttachmentPhase::Ordinary,
                    ..plan
                },
            },
            fixture.context.codec_limits,
        )
        .await
        .expect("Ordinary is explicit, not promoted late STARTUP");
        let (send, recv) = fixture
            ._server_connection
            .accept_bi()
            .await
            .expect("accept Ordinary request");
        let ordinary_actor = tokio::spawn(handle_server_udp_bidi_stream(
            send,
            recv,
            fixture.context.clone(),
            fixture.session_id,
            fixture.path_id,
            fixture._path_registration.clone(),
        ));
        assert_eq!(
            udp_path_read_frame(&mut ordinary_recv, fixture.context.codec_limits)
                .await
                .expect("Ordinary admission"),
            Frame::StreamMaxData {
                stream_id,
                max_offset: 0
            }
        );
        let snapshot = fixture.context.reliable_streams.management_snapshot();
        assert_eq!(
            snapshot.active_streams, 2,
            "no Product reset or replacement target"
        );
        assert_eq!(
            snapshot.paths.len(),
            2,
            "physical carriers remain registered"
        );
        assert!(!fixture._client_connection.is_closed());
        ordinary_actor.abort();
        let _ = ordinary_actor.await;
        sibling_actor.abort();
        let _ = sibling_actor.await;
    })
    .await
    .expect("late STARTUP scope test completes without a carrier stall");
}

#[tokio::test]
async fn server_quic_restart_reset_preserves_fresh_sibling_on_same_carrier() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let fresh_id = StreamId(489);
        let lost_id = StreamId(488);
        // This new registry has admitted a fresh STARTUP stream, but has no
        // retained state for the client's pre-restart logical stream.
        let mut fixture = ServerUdpTerminalWriterFixture::open(fresh_id).await;
        fixture.drain_zero_credit_admission().await;
        let mut product_stream = fixture.accepted.take_stream();
        let actor = tokio::spawn(run_server_udp_reliable_stream_loop(
            fixture.server_send.take().expect("fresh sibling sender"),
            fixture.server_recv.take().expect("fresh sibling receiver"),
            ServerUdpReliableStreamLoop {
                context: fixture.context.clone(),
                session_id: fixture.session_id,
                path_id: fixture.path_id,
                path_registration: fixture._path_registration.clone(),
                stream_id: fresh_id,
                target: fixture.target.clone(),
                commands_tx: fixture.commands_tx.clone(),
                commands_rx: fixture.commands_rx.take().expect("fresh sibling commands"),
                path_proofs: PathProofTracker::default(),
            },
        ));
        for phase in [
            StreamAttachmentPhase::Ordinary,
            StreamAttachmentPhase::Startup,
        ] {
            let (mut send, mut recv) = fixture
                ._client_connection
                .open_bi()
                .await
                .expect("open lost stream recovery request");
            udp_path_write_frame(
                &mut send,
                &Frame::OpenStream {
                    stream_id: lost_id,
                    target: fixture.target.clone(),
                    demand: StreamDemandHint::Throughput,
                    return_plan: StreamReturnPlan {
                        phase,
                        ..Default::default()
                    },
                },
                fixture.context.codec_limits,
            )
            .await
            .expect("publish recovery or delayed startup");
            let (server_send, server_recv) = fixture
                ._server_connection
                .accept_bi()
                .await
                .expect("accept lost stream request");
            handle_server_udp_bidi_stream(
                server_send,
                server_recv,
                fixture.context.clone(),
                fixture.session_id,
                fixture.path_id,
                fixture._path_registration.clone(),
            )
            .await
            .expect("lost logical stream is terminal without failing its carrier");
            assert_eq!(
                udp_path_read_frame(&mut recv, fixture.context.codec_limits)
                    .await
                    .expect("read lost stream terminal response"),
                Frame::StreamReset {
                    stream_id: lost_id,
                    reason: ResetReason::RemoteClosed,
                }
            );
            assert!(matches!(
                udp_path_read_frame(&mut recv, fixture.context.codec_limits).await,
                Err(RuntimeError::QuicCarrier(
                    crate::transport::quic::QuicCarrierError::StreamFinished
                ))
            ));
            let snapshot = fixture.context.reliable_streams.management_snapshot();
            assert_eq!(snapshot.active_streams, 1, "no replacement target owner");
            assert_eq!(snapshot.paths.len(), 1, "same carrier remains registered");
            assert!(!fixture._client_connection.is_closed());
        }

        let payload = Bytes::from_static(b"fresh-sibling-after-restart");
        let data = Frame::StreamData {
            stream_id: fresh_id,
            offset: 0,
            payload: payload.clone(),
        };
        fixture
            .commands_tx
            .send_stream_ordered_frame(data.clone(), TrafficClass::Throughput)
            .await
            .expect("send fresh sibling payload after stale recovery");
        let client_recv = fixture
            .client_recv
            .as_mut()
            .expect("fresh sibling receiver");
        while udp_path_read_frame(client_recv, fixture.context.codec_limits)
            .await
            .expect("fresh sibling continues receiving")
            != data
        {}
        let ack = Frame::StreamAck {
            stream_id: fresh_id,
            scope_start: None,
            ranges: vec![
                OffsetRange::new(0, payload.len() as u64).expect("fresh sibling payload range"),
            ],
        };
        udp_path_write_frame(
            fixture.client_send.as_mut().expect("fresh sibling sender"),
            &ack,
            fixture.context.codec_limits,
        )
        .await
        .expect("acknowledge fresh sibling on same carrier");
        assert_eq!(
            product_stream
                .recv_frame()
                .await
                .expect("fresh sibling ACK"),
            ack
        );
        assert!(!fixture._server_connection.is_closed());
        actor.abort();
        let _ = actor.await;
    })
    .await
    .expect("lost QUIC stream and healthy sibling settle promptly");
}

#[tokio::test]
async fn server_quic_route_denials_are_stream_local_and_drop_has_no_response() {
    let stream_id = StreamId(413);
    let mut fixture = ServerUdpTerminalWriterFixture::open(stream_id).await;
    fixture.context.reliable_streams = fixture
        .context
        .reliable_streams
        .clone()
        .with_target_admission(Arc::new(|_, _, target| {
            Ok(match target.port() {
                81 => ServerTargetAdmission::Reject,
                82 => ServerTargetAdmission::Drop,
                _ => ServerTargetAdmission::Allow,
            })
        }));

    let rejected_stream_id = StreamId(481);
    let (mut reject_send, mut reject_recv) = fixture
        ._client_connection
        .open_bi()
        .await
        .expect("open rejected QUIC Product stream");
    udp_path_write_frame(
        &mut reject_send,
        &Frame::OpenStream {
            stream_id: rejected_stream_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 81))),
            demand: StreamDemandHint::Latency,
            return_plan: Default::default(),
        },
        fixture.context.codec_limits,
    )
    .await
    .expect("publish rejected QUIC Product open");
    let (server_send, server_recv) = tokio::time::timeout(
        Duration::from_secs(5),
        fixture._server_connection.accept_bi(),
    )
    .await
    .expect("rejected QUIC Product stream timeout")
    .expect("accept rejected QUIC Product stream");
    handle_server_udp_bidi_stream(
        server_send,
        server_recv,
        fixture.context.clone(),
        fixture.session_id,
        fixture.path_id,
        fixture._path_registration.clone(),
    )
    .await
    .expect("reject QUIC Product stream locally");
    assert_eq!(
        udp_path_read_frame(&mut reject_recv, fixture.context.codec_limits)
            .await
            .expect("read QUIC Product refusal"),
        Frame::StreamDetach {
            stream_id: rejected_stream_id,
        },
    );

    let dropped_stream_id = StreamId(482);
    let (mut drop_send, mut drop_recv) = fixture
        ._client_connection
        .open_bi()
        .await
        .expect("open dropped QUIC Product stream");
    udp_path_write_frame(
        &mut drop_send,
        &Frame::OpenStream {
            stream_id: dropped_stream_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 82))),
            demand: StreamDemandHint::Latency,
            return_plan: Default::default(),
        },
        fixture.context.codec_limits,
    )
    .await
    .expect("publish dropped QUIC Product open");
    let (server_send, server_recv) = tokio::time::timeout(
        Duration::from_secs(5),
        fixture._server_connection.accept_bi(),
    )
    .await
    .expect("dropped QUIC Product stream timeout")
    .expect("accept dropped QUIC Product stream");
    handle_server_udp_bidi_stream(
        server_send,
        server_recv,
        fixture.context.clone(),
        fixture.session_id,
        fixture.path_id,
        fixture._path_registration.clone(),
    )
    .await
    .expect("silently drop QUIC Product stream");
    let dropped_response = udp_path_read_frame(&mut drop_recv, fixture.context.codec_limits).await;
    assert!(
        matches!(
            &dropped_response,
            Err(RuntimeError::QuicCarrier(
                crate::transport::quic::QuicCarrierError::H3Stream(
                    h3::error::StreamError::RemoteTerminate { code }
                )
            )) if *code == h3::error::Code::H3_REQUEST_CANCELLED
        ),
        "route drop must cancel the request without an HTTP/MPP response: {dropped_response:?}",
    );
    assert_eq!(
        fixture.attached_output_count(),
        1,
        "both denial kinds must preserve the existing logical sibling",
    );
    assert!(!fixture._client_connection.is_closed());
}

#[tokio::test]
async fn server_quic_datagram_route_denials_are_flow_local_and_drop_has_no_frame() {
    let stream_id = StreamId(412);
    let mut fixture = ServerUdpTerminalWriterFixture::open(stream_id).await;
    fixture.context.datagrams = Some(ServerDatagramPort::new(Arc::new(
        PolicyDenyQuicDatagramBackend,
    )));

    let rejected_flow_id = crate::protocol::DatagramFlowId(81);
    let (mut reject_send, mut reject_recv) = fixture
        ._client_connection
        .open_bi()
        .await
        .expect("open rejected QUIC datagram stream");
    udp_path_write_frame(
        &mut reject_send,
        &Frame::OpenDatagramFlow {
            flow_id: rejected_flow_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 81))),
        },
        fixture.context.codec_limits,
    )
    .await
    .expect("publish rejected QUIC datagram open");
    let (server_send, server_recv) = tokio::time::timeout(
        Duration::from_secs(5),
        fixture._server_connection.accept_bi(),
    )
    .await
    .expect("rejected QUIC datagram stream timeout")
    .expect("accept rejected QUIC datagram stream");
    let reject_actor = tokio::spawn(handle_server_udp_bidi_stream(
        server_send,
        server_recv,
        fixture.context.clone(),
        fixture.session_id,
        fixture.path_id,
        fixture._path_registration.clone(),
    ));
    assert_eq!(
        udp_path_read_frame(&mut reject_recv, fixture.context.codec_limits)
            .await
            .expect("read QUIC datagram refusal"),
        Frame::DatagramClose {
            flow_id: rejected_flow_id,
        },
    );
    udp_path_write_frame(
        &mut reject_send,
        &Frame::DatagramClose {
            flow_id: rejected_flow_id,
        },
        fixture.context.codec_limits,
    )
    .await
    .expect("close rejected QUIC datagram stream");
    udp_path_finish_stream(&mut reject_send)
        .await
        .expect("finish rejected QUIC datagram sender");
    tokio::time::timeout(Duration::from_secs(5), reject_actor)
        .await
        .expect("rejected QUIC datagram actor timeout")
        .expect("rejected QUIC datagram actor join")
        .expect("rejected QUIC datagram actor result");

    let saturated_flow_id = crate::protocol::DatagramFlowId(83);
    let (mut saturated_send, mut saturated_recv) = fixture
        ._client_connection
        .open_bi()
        .await
        .expect("open registry-saturated QUIC datagram stream");
    udp_path_write_frame(
        &mut saturated_send,
        &Frame::OpenDatagramFlow {
            flow_id: saturated_flow_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 83))),
        },
        fixture.context.codec_limits,
    )
    .await
    .expect("publish registry-saturated QUIC datagram open");
    udp_path_write_frame(
        &mut saturated_send,
        &Frame::OpenDatagramFlow {
            flow_id: saturated_flow_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 83))),
        },
        fixture.context.codec_limits,
    )
    .await
    .expect("queue repeated registry-saturated QUIC datagram open");
    let (server_send, server_recv) = tokio::time::timeout(
        Duration::from_secs(5),
        fixture._server_connection.accept_bi(),
    )
    .await
    .expect("registry-saturated QUIC datagram stream timeout")
    .expect("accept registry-saturated QUIC datagram stream");
    let saturated_actor = tokio::spawn(handle_server_udp_bidi_stream(
        server_send,
        server_recv,
        fixture.context.clone(),
        fixture.session_id,
        fixture.path_id,
        fixture._path_registration.clone(),
    ));
    for _ in 0..2 {
        assert_eq!(
            udp_path_read_frame(&mut saturated_recv, fixture.context.codec_limits)
                .await
                .expect("registry capacity close"),
            Frame::DatagramClose {
                flow_id: saturated_flow_id,
            },
        );
    }
    udp_path_write_frame(
        &mut saturated_send,
        &Frame::DatagramClose {
            flow_id: saturated_flow_id,
        },
        fixture.context.codec_limits,
    )
    .await
    .expect("close registry-saturated QUIC datagram flow");
    udp_path_finish_stream(&mut saturated_send)
        .await
        .expect("finish registry-saturated QUIC datagram sender");
    tokio::time::timeout(Duration::from_secs(5), saturated_actor)
        .await
        .expect("registry-saturated QUIC datagram actor timeout")
        .expect("registry-saturated QUIC datagram actor join")
        .expect("registry-saturated QUIC datagram actor result");

    let dropped_flow_id = crate::protocol::DatagramFlowId(82);
    let (mut drop_send, mut drop_recv) = fixture
        ._client_connection
        .open_bi()
        .await
        .expect("open dropped QUIC datagram stream");
    udp_path_write_frame(
        &mut drop_send,
        &Frame::OpenDatagramFlow {
            flow_id: dropped_flow_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 82))),
        },
        fixture.context.codec_limits,
    )
    .await
    .expect("publish dropped QUIC datagram open");
    udp_path_finish_stream(&mut drop_send)
        .await
        .expect("finish dropped QUIC datagram sender");
    let (server_send, server_recv) = tokio::time::timeout(
        Duration::from_secs(5),
        fixture._server_connection.accept_bi(),
    )
    .await
    .expect("dropped QUIC datagram stream timeout")
    .expect("accept dropped QUIC datagram stream");
    handle_server_udp_bidi_stream(
        server_send,
        server_recv,
        fixture.context.clone(),
        fixture.session_id,
        fixture.path_id,
        fixture._path_registration.clone(),
    )
    .await
    .expect("silently drop QUIC datagram flow");
    let dropped_response = udp_path_read_frame(&mut drop_recv, fixture.context.codec_limits).await;
    assert!(
        matches!(
            &dropped_response,
            Err(RuntimeError::QuicCarrier(
                crate::transport::quic::QuicCarrierError::H3Stream(
                    h3::error::StreamError::RemoteTerminate { code }
                )
            )) if *code == h3::error::Code::H3_REQUEST_CANCELLED
        ),
        "route drop must finish without an MPP response frame: {dropped_response:?}"
    );

    let (mut ping_send, mut ping_recv) = fixture
        ._client_connection
        .open_bi()
        .await
        .expect("open sibling QUIC stream after datagram denials");
    udp_path_write_frame(
        &mut ping_send,
        &Frame::Ping { nonce: 412 },
        fixture.context.codec_limits,
    )
    .await
    .expect("publish sibling QUIC ping");
    let (server_send, server_recv) = tokio::time::timeout(
        Duration::from_secs(5),
        fixture._server_connection.accept_bi(),
    )
    .await
    .expect("sibling QUIC stream timeout")
    .expect("accept sibling QUIC stream");
    handle_server_udp_bidi_stream(
        server_send,
        server_recv,
        fixture.context.clone(),
        fixture.session_id,
        fixture.path_id,
        fixture._path_registration.clone(),
    )
    .await
    .expect("serve sibling QUIC ping after datagram denials");
    assert_eq!(
        udp_path_read_frame(&mut ping_recv, fixture.context.codec_limits)
            .await
            .expect("read sibling QUIC pong"),
        Frame::Pong { nonce: 412 },
    );
    assert_eq!(
        fixture.attached_output_count(),
        1,
        "denied datagram flows must preserve the existing reliable sibling",
    );
    assert!(!fixture._client_connection.is_closed());
}

#[test]
fn server_quic_datagram_tombstones_are_bounded_lru_state() {
    let mut tombstones = ServerDatagramTombstoneCache::new(2);
    let reject = crate::protocol::DatagramFlowId(81);
    let drop = crate::protocol::DatagramFlowId(82);
    let capacity = crate::protocol::DatagramFlowId(83);
    assert_eq!(
        tombstones.insert_with_eviction(reject, ServerDatagramTombstone::Reject),
        None,
    );
    assert_eq!(
        tombstones.insert_with_eviction(drop, ServerDatagramTombstone::Drop),
        None,
    );
    assert_eq!(tombstones.len(), 2);
    assert_eq!(
        tombstones.get(reject),
        Some(ServerDatagramTombstone::Reject),
        "repeated opens refresh the retained decision",
    );
    assert_eq!(
        tombstones.insert_with_eviction(capacity, ServerDatagramTombstone::CapacityReject),
        Some(drop),
    );
    assert_eq!(tombstones.len(), 2);
    assert_eq!(tombstones.get(drop), None, "the LRU decision is evicted");
    assert_eq!(
        tombstones.get(reject),
        Some(ServerDatagramTombstone::Reject),
    );
    assert_eq!(
        tombstones.get(capacity),
        Some(ServerDatagramTombstone::CapacityReject),
    );
}

#[tokio::test]
async fn server_quic_datagram_tombstones_ignore_in_flight_frames_and_preserve_carrier() {
    let stream_id = StreamId(413);
    let mut fixture =
        ServerUdpTerminalWriterFixture::open_with_max_streams(stream_id, Some(2)).await;
    let backend = Arc::new(ScriptedQuicDatagramBackend::default());
    fixture.context.datagrams = Some(ServerDatagramPort::new(backend.clone()));

    let (mut client_send, mut client_recv) = fixture
        ._client_connection
        .open_bi()
        .await
        .expect("open QUIC datagram stream");
    let reject_id = crate::protocol::DatagramFlowId(81);
    let reject_open = Frame::OpenDatagramFlow {
        flow_id: reject_id,
        target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 81))),
    };
    for frame in [
        reject_open.clone(),
        Frame::DatagramData {
            flow_id: reject_id,
            datagram_id: crate::protocol::DatagramId(1),
            ttl_ms: 1,
            payload: Bytes::from_static(b"late-rejected"),
        },
        Frame::DatagramFeedback {
            flow_id: reject_id,
            received: vec![crate::protocol::OffsetRange::new(0, 1).expect("feedback range")],
        },
        reject_open,
    ] {
        udp_path_write_frame(&mut client_send, &frame, fixture.context.codec_limits)
            .await
            .expect("queue rejected flow traffic");
    }
    let (server_send, server_recv) = tokio::time::timeout(
        Duration::from_secs(5),
        fixture._server_connection.accept_bi(),
    )
    .await
    .expect("QUIC datagram stream timeout")
    .expect("accept QUIC datagram stream");
    let actor = tokio::spawn(handle_server_udp_bidi_stream(
        server_send,
        server_recv,
        fixture.context.clone(),
        fixture.session_id,
        fixture.path_id,
        fixture._path_registration.clone(),
    ));
    for _ in 0..2 {
        assert_eq!(
            udp_path_read_frame(&mut client_recv, fixture.context.codec_limits)
                .await
                .expect("read repeated rejection"),
            Frame::DatagramClose { flow_id: reject_id },
        );
    }
    assert_eq!(backend.opens_for(81), 1);

    let drop_id = crate::protocol::DatagramFlowId(82);
    let drop_open = Frame::OpenDatagramFlow {
        flow_id: drop_id,
        target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 82))),
    };
    for frame in [
        drop_open.clone(),
        Frame::DatagramData {
            flow_id: drop_id,
            datagram_id: crate::protocol::DatagramId(2),
            ttl_ms: 1,
            payload: Bytes::from_static(b"late-dropped"),
        },
        Frame::DatagramFeedback {
            flow_id: drop_id,
            received: Vec::new(),
        },
        drop_open,
    ] {
        udp_path_write_frame(&mut client_send, &frame, fixture.context.codec_limits)
            .await
            .expect("queue silently dropped flow traffic");
    }
    assert!(
        tokio::time::timeout(
            Duration::from_millis(50),
            udp_path_read_frame(&mut client_recv, fixture.context.codec_limits),
        )
        .await
        .is_err(),
        "initial and repeated drop must not emit a response",
    );
    assert_eq!(backend.opens_for(82), 1);

    for (flow, port) in [(1_u64, 90_u16), (2, 91)] {
        udp_path_write_frame(
            &mut client_send,
            &Frame::OpenDatagramFlow {
                flow_id: crate::protocol::DatagramFlowId(flow),
                target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], port))),
            },
            fixture.context.codec_limits,
        )
        .await
        .expect("open accepted QUIC datagram sibling");
    }

    let capacity_id = crate::protocol::DatagramFlowId(3);
    let capacity_open = Frame::OpenDatagramFlow {
        flow_id: capacity_id,
        target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 92))),
    };
    for frame in [
        capacity_open.clone(),
        Frame::DatagramData {
            flow_id: capacity_id,
            datagram_id: crate::protocol::DatagramId(3),
            ttl_ms: 1,
            payload: Bytes::from_static(b"late-capacity"),
        },
        Frame::DatagramFeedback {
            flow_id: capacity_id,
            received: Vec::new(),
        },
        capacity_open,
    ] {
        udp_path_write_frame(&mut client_send, &frame, fixture.context.codec_limits)
            .await
            .expect("queue capacity-rejected flow traffic");
    }
    for _ in 0..2 {
        assert_eq!(
            udp_path_read_frame(&mut client_recv, fixture.context.codec_limits)
                .await
                .expect("read repeated capacity rejection"),
            Frame::DatagramClose {
                flow_id: capacity_id,
            },
        );
    }
    assert_eq!(
        backend.opens_for(92),
        0,
        "accepted-flow capacity is checked before target admission",
    );

    let next_capacity_id = crate::protocol::DatagramFlowId(4);
    udp_path_write_frame(
        &mut client_send,
        &Frame::OpenDatagramFlow {
            flow_id: next_capacity_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 93))),
        },
        fixture.context.codec_limits,
    )
    .await
    .expect("advance the bounded denial cache");
    assert_eq!(
        udp_path_read_frame(&mut client_recv, fixture.context.codec_limits)
            .await
            .expect("read next capacity rejection"),
        Frame::DatagramClose {
            flow_id: next_capacity_id,
        },
    );
    assert_eq!(backend.opens_for(93), 0);

    // DROP was the runtime LRU victim. Its transport association must become
    // terminal too: a duplicate OPEN plus late native/reliable traffic stays
    // silent and can never fall through to target admission.
    for frame in [
        Frame::OpenDatagramFlow {
            flow_id: drop_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 82))),
        },
        Frame::DatagramData {
            flow_id: drop_id,
            datagram_id: crate::protocol::DatagramId(5),
            ttl_ms: 1_000,
            payload: Bytes::from_static(b"late-evicted-drop"),
        },
        Frame::DatagramFeedback {
            flow_id: drop_id,
            received: Vec::new(),
        },
    ] {
        udp_path_write_frame(&mut client_send, &frame, fixture.context.codec_limits)
            .await
            .expect("send traffic for an evicted denial");
    }
    assert!(
        tokio::time::timeout(
            Duration::from_millis(50),
            udp_path_read_frame(&mut client_recv, fixture.context.codec_limits),
        )
        .await
        .is_err(),
        "an evicted denial must stay terminal and silent",
    );
    assert_eq!(
        backend.opens_for(82),
        1,
        "an evicted denial must not be admitted again",
    );

    let accepted_id = crate::protocol::DatagramFlowId(1);
    udp_path_write_frame(
        &mut client_send,
        &Frame::DatagramData {
            flow_id: accepted_id,
            datagram_id: crate::protocol::DatagramId(4),
            ttl_ms: 1_000,
            payload: Bytes::from_static(b"accepted"),
        },
        fixture.context.codec_limits,
    )
    .await
    .expect("send accepted sibling datagram");
    assert!(matches!(
        udp_path_read_frame(&mut client_recv, fixture.context.codec_limits)
            .await
            .expect("accepted sibling feedback"),
        Frame::DatagramFeedback { flow_id, .. } if flow_id == accepted_id
    ));
    udp_path_write_frame(
        &mut client_send,
        &Frame::DatagramFeedback {
            flow_id: accepted_id,
            received: Vec::new(),
        },
        fixture.context.codec_limits,
    )
    .await
    .expect("send accepted sibling response feedback");
    tokio::time::timeout(Duration::from_secs(1), async {
        while backend.feedback.load(std::sync::atomic::Ordering::Relaxed) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("accepted sibling feedback routing timeout");

    for flow_id in [
        capacity_id,
        next_capacity_id,
        drop_id,
        accepted_id,
        crate::protocol::DatagramFlowId(2),
    ] {
        udp_path_write_frame(
            &mut client_send,
            &Frame::DatagramClose { flow_id },
            fixture.context.codec_limits,
        )
        .await
        .expect("close accepted flow or tombstone");
    }
    udp_path_finish_stream(&mut client_send)
        .await
        .expect("finish QUIC datagram sender");
    tokio::time::timeout(Duration::from_secs(5), actor)
        .await
        .expect("QUIC datagram actor timeout")
        .expect("QUIC datagram actor join")
        .expect("QUIC datagram actor result");

    let (mut ping_send, mut ping_recv) = fixture
        ._client_connection
        .open_bi()
        .await
        .expect("open sibling QUIC stream after tombstone traffic");
    udp_path_write_frame(
        &mut ping_send,
        &Frame::Ping { nonce: 413 },
        fixture.context.codec_limits,
    )
    .await
    .expect("publish sibling QUIC ping");
    let (server_send, server_recv) = tokio::time::timeout(
        Duration::from_secs(5),
        fixture._server_connection.accept_bi(),
    )
    .await
    .expect("sibling QUIC stream timeout")
    .expect("accept sibling QUIC stream");
    handle_server_udp_bidi_stream(
        server_send,
        server_recv,
        fixture.context.clone(),
        fixture.session_id,
        fixture.path_id,
        fixture._path_registration.clone(),
    )
    .await
    .expect("serve sibling QUIC ping");
    assert_eq!(
        udp_path_read_frame(&mut ping_recv, fixture.context.codec_limits)
            .await
            .expect("read sibling QUIC pong"),
        Frame::Pong { nonce: 413 },
    );
    assert!(!fixture._client_connection.is_closed());
}

#[tokio::test]
async fn server_quic_burst_denials_free_provisional_capacity_for_later_allowed_flow() {
    let stream_id = StreamId(414);
    let mut fixture =
        ServerUdpTerminalWriterFixture::open_with_max_streams(stream_id, Some(2)).await;
    let backend = Arc::new(ScriptedQuicDatagramBackend::default());
    backend
        .block_first_reject
        .store(true, std::sync::atomic::Ordering::Relaxed);
    fixture.context.datagrams = Some(ServerDatagramPort::new(backend.clone()));

    let (mut client_send, mut client_recv) = fixture
        ._client_connection
        .open_bi()
        .await
        .expect("open burst QUIC datagram stream");
    let rejected = crate::protocol::DatagramFlowId(81);
    let dropped = crate::protocol::DatagramFlowId(82);
    let allowed = crate::protocol::DatagramFlowId(90);
    for frame in [
        Frame::OpenDatagramFlow {
            flow_id: rejected,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 81))),
        },
        Frame::OpenDatagramFlow {
            flow_id: dropped,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 82))),
        },
        Frame::OpenDatagramFlow {
            flow_id: allowed,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 90))),
        },
        Frame::DatagramData {
            flow_id: allowed,
            datagram_id: crate::protocol::DatagramId(1),
            ttl_ms: 1_000,
            payload: Bytes::from_static(b"later-allowed"),
        },
        Frame::DatagramFeedback {
            flow_id: allowed,
            received: Vec::new(),
        },
    ] {
        udp_path_write_frame(&mut client_send, &frame, fixture.context.codec_limits)
            .await
            .expect("queue burst datagram frame");
    }
    let (server_send, server_recv) = tokio::time::timeout(
        Duration::from_secs(5),
        fixture._server_connection.accept_bi(),
    )
    .await
    .expect("burst QUIC datagram stream timeout")
    .expect("accept burst QUIC datagram stream");
    let actor = tokio::spawn(handle_server_udp_bidi_stream(
        server_send,
        server_recv,
        fixture.context.clone(),
        fixture.session_id,
        fixture.path_id,
        fixture._path_registration.clone(),
    ));
    tokio::time::timeout(
        Duration::from_secs(1),
        backend.first_reject_entered.notified(),
    )
    .await
    .expect("first denial did not block");
    // The reader runs independently while policy admission is blocked, so it
    // observes all three OPENs against max=2 before runtime resolves either
    // earlier denial.
    tokio::time::sleep(Duration::from_millis(50)).await;
    backend.release_first_reject.notify_one();

    assert_eq!(
        udp_path_read_frame(&mut client_recv, fixture.context.codec_limits)
            .await
            .expect("read first burst rejection"),
        Frame::DatagramClose { flow_id: rejected },
    );
    assert!(matches!(
        udp_path_read_frame(&mut client_recv, fixture.context.codec_limits)
            .await
            .expect("read later allowed feedback"),
        Frame::DatagramFeedback { flow_id, .. } if flow_id == allowed
    ));
    assert_eq!(backend.opens_for(81), 1);
    assert_eq!(backend.opens_for(82), 1);
    assert_eq!(backend.opens_for(90), 1);
    tokio::time::timeout(Duration::from_secs(1), async {
        while backend.feedback.load(std::sync::atomic::Ordering::Relaxed) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("later allowed feedback routing timeout");

    for flow_id in [rejected, dropped, allowed] {
        udp_path_write_frame(
            &mut client_send,
            &Frame::DatagramClose { flow_id },
            fixture.context.codec_limits,
        )
        .await
        .expect("close burst flow");
    }
    udp_path_finish_stream(&mut client_send)
        .await
        .expect("finish burst datagram sender");
    tokio::time::timeout(Duration::from_secs(5), actor)
        .await
        .expect("burst datagram actor timeout")
        .expect("burst datagram actor join")
        .expect("burst datagram actor result");

    let (mut ping_send, mut ping_recv) = fixture
        ._client_connection
        .open_bi()
        .await
        .expect("open sibling after burst denials");
    udp_path_write_frame(
        &mut ping_send,
        &Frame::Ping { nonce: 414 },
        fixture.context.codec_limits,
    )
    .await
    .expect("publish sibling ping after burst denials");
    let (server_send, server_recv) = tokio::time::timeout(
        Duration::from_secs(5),
        fixture._server_connection.accept_bi(),
    )
    .await
    .expect("sibling stream timeout")
    .expect("accept sibling stream");
    handle_server_udp_bidi_stream(
        server_send,
        server_recv,
        fixture.context.clone(),
        fixture.session_id,
        fixture.path_id,
        fixture._path_registration.clone(),
    )
    .await
    .expect("serve sibling ping after burst denials");
    assert_eq!(
        udp_path_read_frame(&mut ping_recv, fixture.context.codec_limits)
            .await
            .expect("read sibling pong"),
        Frame::Pong { nonce: 414 },
    );
    assert!(!fixture._client_connection.is_closed());
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
            return_plan: Default::default(),
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
            initial_demand: StreamDemandHint::Throughput,
            return_plan: Default::default(),
            native_rate_authority: None,
            repair_bindings: Default::default(),
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
    fixture.drain_zero_credit_admission().await;
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
            scope_start: None,
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
            scope_start: None,
            ..
        })) if ack_stream_id == stream_id
    ));
    for frame in [
        Frame::StreamRequalifyData {
            stream_id,
            probe_id: 71,
            offset: 0,
            payload: Bytes::from_static(b"late-probe"),
        },
        Frame::StreamRequalifyAck {
            stream_id,
            probe_id: 72,
            offset: 0,
            payload_bytes: 10,
        },
    ] {
        udp_path_write_frame(client_send, &frame, fixture.context.codec_limits)
            .await
            .expect("terminal drain tolerates delayed requalification frame");
    }
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
