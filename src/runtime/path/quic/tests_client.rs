use super::*;
use crate::config::{
    ClientSecurityConfig, MppPerformanceConfig, ResourceLimits, ServerSecurityConfig, SharedSecret,
};
use crate::outbound::OutboundConfig;
use crate::protocol::{CloseReason, Frame, StreamDemandHint, TargetAddr};
use crate::runtime::path::ServerCarrierPathRegistration;
use crate::runtime::path::health::{ClientPathHealth, ClientPathHealthRecord};
use crate::runtime::path::quic::server::accept_server_udp_path_handshake_for_test;
use crate::runtime::path::server_context::{ServerLocalPath, ServerPathContext};
use crate::runtime::peer_status::{PeerStatusBroker, PeerStatusSnapshotSource};
use crate::transport::encrypted::test_client_tls_config;
use crate::transport::{
    CarrierEndpoint, CarrierPathIdentity, PathBinding, PathMetadata, SystemCarrierNetworkProvider,
};
use std::net::SocketAddr;

#[test]
fn session_close_is_terminal_for_quic_open() {
    assert_eq!(
        client_udp_error_disposition(&RuntimeError::RemoteClosed(
            crate::protocol::CloseReason::PolicyRejected,
        )),
        ClientUdpErrorDisposition::Session,
    );
}

#[test]
fn carrier_local_path_close_remains_retryable_for_quic_open() {
    assert!(
        crate::runtime::relay::open::udp_stream_open_error_is_path_retryable(
            &RuntimeError::RemotePathClosed(crate::protocol::CloseReason::PolicyRejected)
        )
    );
}

#[test]
fn closed_native_transport_cannot_override_session_terminal() {
    assert!(!client_udp_native_close_authorizes_retry(
        true,
        &RuntimeError::RemoteClosed(crate::protocol::CloseReason::PolicyRejected),
    ));
}

#[test]
fn closed_native_transport_still_authorizes_carrier_local_retry() {
    assert!(client_udp_native_close_authorizes_retry(
        true,
        &RuntimeError::RemotePathClosed(crate::protocol::CloseReason::ProtocolError),
    ));
}

struct ClientOpenRaceFixture {
    session: ClientUdpPathSessionHandle,
    server_endpoint: UdpPathEndpoint,
    server_local_path: ServerLocalPath,
    server_context: ServerPathContext,
}

struct AcceptedTestCarrier {
    connection: UdpPathConnection,
    registration: ServerCarrierPathRegistration,
    _control_send: UdpPathSendStream,
    _control_recv: UdpPathRecvStream,
}

impl ClientOpenRaceFixture {
    async fn new() -> Self {
        let shared_secret = SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("test shared secret");
        let security = ServerSecurityConfig::for_test(shared_secret.clone());
        let client_security = ClientSecurityConfig::for_test(shared_secret);
        let crate::runtime::node::server::ServerIdentityRuntime {
            paths: server_context,
            ..
        } = crate::runtime::node::server::new_identity_runtime(
            Vec::new(),
            OutboundConfig::Direct,
            crate::config::DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
            security,
            MppPerformanceConfig::default(),
            ResourceLimits::default(),
        );
        let reserved =
            std::net::UdpSocket::bind("127.0.0.1:0").expect("reserve server QUIC address");
        let reserved_addr = reserved.local_addr().expect("reserved QUIC address");
        drop(reserved);
        let server_path = PathSpec {
            underlay: UnderlayProtocol::Udp,
            endpoint: CarrierEndpoint::single("127.0.0.1", reserved_addr.port())
                .expect("server carrier endpoint"),
            binding: PathBinding::default(),
            metadata: PathMetadata::default(),
        };
        let server_endpoint = UdpPathEndpoint::bind_server(&server_path, &server_context)
            .await
            .expect("bind server QUIC endpoint");
        let server_addr = server_endpoint.local_addr().expect("server QUIC address");
        let client_path = PathSpec {
            underlay: UnderlayProtocol::Udp,
            endpoint: CarrierEndpoint::single(server_addr.ip().to_string(), server_addr.port())
                .expect("client carrier endpoint"),
            binding: PathBinding::default(),
            metadata: PathMetadata::default(),
        };
        let runtime = ClientUdpPathSessionRuntime {
            paths: Arc::new(vec![client_path]),
            config_index: 0,
            path_index: 0,
            carrier_identity: CarrierPathIdentity {
                group_ordinal: 0,
                path_ordinal: 0,
            },
            session_id: SessionId(941),
            candidate_selector: QuicCandidateSelector::derive(
                client_security.credential.id().as_str(),
                client_security.credential.secret().as_bytes(),
            ),
            security: Arc::new(vec![client_security]),
            tls: Arc::new(vec![test_client_tls_config()]),
            codec_limits: server_context.codec_limits,
            mux_limits: server_context.mux_limits,
            stream_frame_queue: 8,
            state: ClientPathState::new(ClientPathHealth::new(
                Vec::new(),
                vec![ClientPathHealthRecord::default()],
            )),
            carrier_network: Arc::new(SystemCarrierNetworkProvider),
            peer_status: PeerStatusBroker::new(false),
            peer_status_snapshot: PeerStatusSnapshotSource::new(|| Some(Vec::new())),
            authenticated_carriers: crate::runtime::path::AuthenticatedCarrierInventory::default(),
            ip_tunnels: crate::runtime::tun_l3::ClientIpTunnelHub::default(),
        };
        Self {
            session: ClientUdpPathSessionHandle::new(runtime),
            server_endpoint,
            server_local_path: ServerLocalPath::new(0, server_path),
            server_context,
        }
    }

    fn spawn_server_accept(
        &self,
    ) -> tokio::task::JoinHandle<Result<AcceptedTestCarrier, RuntimeError>> {
        let endpoint = self.server_endpoint.clone();
        let local_path = self.server_local_path.clone();
        let context = self.server_context.clone();
        tokio::spawn(async move {
            let connection = endpoint
                .accept()
                .await
                .ok_or(RuntimeError::Protocol("test QUIC endpoint closed"))?;
            let (registration, control_send, control_recv) =
                accept_server_udp_path_handshake_for_test(&connection, &local_path, &context)
                    .await?;
            Ok(AcceptedTestCarrier {
                connection,
                registration,
                _control_send: control_send,
                _control_recv: control_recv,
            })
        })
    }

    async fn establish_current(&self) -> AcceptedTestCarrier {
        let accepted = self.spawn_server_accept();
        self.session
            .prepare_connection(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .expect("prepare client QUIC connection");
        tokio::time::timeout(Duration::from_secs(5), accepted)
            .await
            .expect("server QUIC handshake timeout")
            .expect("server QUIC handshake join")
            .expect("accept authenticated client QUIC carrier")
    }
}

#[tokio::test]
async fn server_quic_observed_ingress_captures_the_real_authenticated_carrier_peer() {
    let fixture = ClientOpenRaceFixture::new().await;
    let accepted = fixture.establish_current().await;
    let client_bind = fixture
        .session
        .connection
        .lock()
        .await
        .as_ref()
        .expect("established client QUIC carrier")
        .endpoint
        .local_addr()
        .expect("client QUIC endpoint");
    assert!(client_bind.ip().is_unspecified());
    let expected_peer = SocketAddr::new(
        fixture
            .server_endpoint
            .local_addr()
            .expect("server QUIC endpoint")
            .ip(),
        client_bind.port(),
    );
    assert_eq!(accepted.connection.remote_address(), expected_peer);

    let ingress = accepted
        .registration
        .mpp_ingress()
        .expect("QUIC observed ingress");
    assert_eq!(ingress.peer(), expected_peer);
    assert_eq!(ingress.session_id(), SessionId(941));
    assert_eq!(ingress.underlay(), UnderlayProtocol::Udp);
    assert_eq!(ingress.configured_path(), None);
    assert_eq!(ingress.path_id(), PathId(0));
    assert_eq!(
        ingress.path_instance_id(),
        accepted.registration.path_instance_id()
    );
}

async fn current_client_carrier(
    session: &ClientUdpPathSessionHandle,
) -> Option<ClientUdpCarrierInstance> {
    session
        .connection
        .lock()
        .await
        .as_ref()
        .map(|connection| connection.carrier.clone())
}

async fn read_test_stream_open(
    connection: &UdpPathConnection,
    stream_id: StreamId,
    limits: CodecLimits,
) -> Result<(UdpPathSendStream, UdpPathRecvStream), RuntimeError> {
    let (send, mut recv) = connection.accept_bi().await?;
    assert!(matches!(
        udp_path_read_frame(&mut recv, limits).await?,
        Frame::OpenStream {
            stream_id: opened,
            ..
        } if opened == stream_id
    ));
    assert!(matches!(
        udp_path_read_frame(&mut recv, limits).await?,
        Frame::StreamMaxData {
            stream_id: opened,
            ..
        } if opened == stream_id
    ));
    Ok((send, recv))
}

async fn fail_test_stream_with_carrier_lifetime_loss(
    connection: UdpPathConnection,
    stream_id: StreamId,
    limits: CodecLimits,
) -> Result<(), RuntimeError> {
    let (_send, _recv) = read_test_stream_open(&connection, stream_id, limits).await?;
    connection.close();
    Ok(())
}

async fn accept_test_stream(
    connection: UdpPathConnection,
    stream_id: StreamId,
    limits: CodecLimits,
) -> Result<(), RuntimeError> {
    let (mut send, _recv) = read_test_stream_open(&connection, stream_id, limits).await?;
    udp_path_write_frame(
        &mut send,
        &Frame::StreamMaxData {
            stream_id,
            max_offset: 65_536,
        },
        limits,
    )
    .await
}

async fn accept_test_datagram_request(
    connection: UdpPathConnection,
    limits: CodecLimits,
) -> Result<(), RuntimeError> {
    let (mut send, _recv) = connection.accept_bi().await?;
    udp_path_write_frame(&mut send, &Frame::SessionReady, limits).await
}

fn install_open_failure_pause(
    session: &ClientUdpPathSessionHandle,
) -> (
    tokio::sync::mpsc::UnboundedReceiver<CarrierPathInstanceId>,
    Arc<tokio::sync::Notify>,
) {
    let (reached, receiver) = tokio::sync::mpsc::unbounded_channel();
    let resume = Arc::new(tokio::sync::Notify::new());
    session.set_retryable_open_failure_hook(Some(ClientUdpRetryableOpenFailureTestHook {
        reached,
        resume: resume.clone(),
    }));
    (receiver, resume)
}

fn install_accepted_open_pause(
    session: &ClientUdpPathSessionHandle,
) -> (
    tokio::sync::mpsc::UnboundedReceiver<(ClientUdpAcceptedOpenKind, CarrierPathInstanceId)>,
    Arc<tokio::sync::Notify>,
) {
    let (reached, receiver) = tokio::sync::mpsc::unbounded_channel();
    let resume = Arc::new(tokio::sync::Notify::new());
    session.set_accepted_open_hook(Some(ClientUdpAcceptedOpenTestHook {
        reached,
        resume: resume.clone(),
    }));
    (receiver, resume)
}

fn spawn_test_open(
    session: &ClientUdpPathSessionHandle,
    stream_id: StreamId,
) -> tokio::task::JoinHandle<Result<OpenedReliableCarrierStream, RuntimeError>> {
    let session = session.clone();
    tokio::spawn(async move {
        session
            .open_stream(
                stream_id,
                TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
                TrafficClass::Latency,
                StreamDemandHint::Latency,
                tokio::time::Instant::now() + Duration::from_secs(10),
                65_536,
            )
            .await
    })
}

fn spawn_test_datagram_open(
    session: &ClientUdpPathSessionHandle,
) -> tokio::task::JoinHandle<Result<ClientUdpDatagramStream, RuntimeError>> {
    let session = session.clone();
    tokio::spawn(async move {
        session
            .open_datagram_stream(tokio::time::Instant::now() + Duration::from_secs(10))
            .await
    })
}

#[test]
fn address_retry_uses_rfc_delay_for_normal_budgets() {
    assert_eq!(
        quic_address_attempt_delay(Duration::from_secs(4), 3),
        QUIC_ADDRESS_ATTEMPT_DELAY
    );
}

#[test]
fn address_retry_fits_alternates_inside_short_budget() {
    assert_eq!(
        quic_address_attempt_delay(Duration::from_millis(120), 3),
        Duration::from_millis(30)
    );
    assert_eq!(
        quic_address_attempt_delay(Duration::from_nanos(1), 1),
        Duration::ZERO
    );
}

#[test]
fn stream_open_path_status_uses_carrier_instance_and_sequence_fences() {
    let state = ClientPathState::new(ClientPathHealth::new(
        Vec::new(),
        vec![ClientPathHealthRecord::default()],
    ));
    let old_instance = next_carrier_path_instance_id();
    state.install_peer_path_usage(
        UnderlayProtocol::Udp,
        0,
        old_instance,
        4,
        PathUsage::Available,
    );

    assert!(
        apply_client_udp_path_status(
            &state,
            0,
            old_instance,
            PathId(0),
            PathId(0),
            5,
            PathUsage::Backup,
        )
        .expect("matching stream-open PATH_STATUS")
    );
    assert!(
        !apply_client_udp_path_status(
            &state,
            0,
            old_instance,
            PathId(0),
            PathId(0),
            4,
            PathUsage::Available,
        )
        .expect("stale stream-open PATH_STATUS")
    );

    let replacement_instance = next_carrier_path_instance_id();
    state.install_peer_path_usage(
        UnderlayProtocol::Udp,
        0,
        replacement_instance,
        0,
        PathUsage::Available,
    );
    assert!(
        !apply_client_udp_path_status(
            &state,
            0,
            old_instance,
            PathId(0),
            PathId(0),
            99,
            PathUsage::Backup,
        )
        .expect("prior carrier stream-open PATH_STATUS")
    );
    assert_eq!(
        state.peer_path_usage(UnderlayProtocol::Udp, 0),
        Some(PathUsage::Available),
    );
}

#[test]
fn stream_open_path_status_rejects_a_different_wire_path() {
    let state = ClientPathState::new(ClientPathHealth::new(
        Vec::new(),
        vec![ClientPathHealthRecord::default()],
    ));
    let instance = next_carrier_path_instance_id();
    state.install_peer_path_usage(UnderlayProtocol::Udp, 0, instance, 0, PathUsage::Available);

    assert!(
        apply_client_udp_path_status(
            &state,
            0,
            instance,
            PathId(0),
            PathId(1),
            1,
            PathUsage::Backup,
        )
        .is_err()
    );
}

#[tokio::test]
async fn stale_carrier_lifetime_open_failure_keeps_the_replacement_connection() {
    let fixture = ClientOpenRaceFixture::new().await;
    let first = fixture.establish_current().await;
    let first_carrier = current_client_carrier(&fixture.session)
        .await
        .expect("first client QUIC carrier");
    let stream_id = StreamId(942);
    let rejected = tokio::spawn(fail_test_stream_with_carrier_lifetime_loss(
        first.connection.clone(),
        stream_id,
        fixture.server_context.codec_limits,
    ));
    let (mut failure_reached, resume_failure) = install_open_failure_pause(&fixture.session);
    let mut opening = spawn_test_open(&fixture.session, stream_id);
    let failed_instance = tokio::time::timeout(Duration::from_secs(5), failure_reached.recv())
        .await
        .expect("real QUIC open failure hook timeout")
        .expect("real QUIC open failure hook closed");
    assert_eq!(failed_instance, first_carrier.path_instance_id);
    tokio::time::timeout(Duration::from_secs(5), rejected)
        .await
        .expect("server rejection timeout")
        .expect("server rejection join")
        .expect("publish carrier-lifetime loss");

    // A different exact owner retires the failed carrier and publishes its
    // successor while the first open's error callback is deliberately stale.
    fixture
        .session
        .drop_failed_connection_instance(first_carrier.path_instance_id)
        .await;
    drop(first);
    let replacement = fixture.establish_current().await;
    let replacement_carrier = current_client_carrier(&fixture.session)
        .await
        .expect("replacement client QUIC carrier");
    assert_ne!(
        replacement_carrier.path_instance_id,
        first_carrier.path_instance_id
    );
    let accepted = tokio::spawn(accept_test_stream(
        replacement.connection.clone(),
        stream_id,
        fixture.server_context.codec_limits,
    ));
    let replacement_lifetime = replacement_carrier.connection.clone();

    resume_failure.notify_one();
    tokio::select! {
        biased;
        result = &mut opening => {
            let opened = result
                .expect("stale-failure open task join")
                .expect("stale failure retries on the already-published replacement");
            assert_eq!(opened.path_instance_id, replacement_carrier.path_instance_id);
        }
        () = replacement_lifetime.wait_closed() => {
            panic!(
                "old released HEAD let a stale open failure retire replacement instance {:?}",
                replacement_carrier.path_instance_id,
            );
        }
        () = tokio::time::sleep(Duration::from_secs(5)) => {
            panic!("stale-failure retry neither completed nor reported replacement retirement");
        }
    }
    tokio::time::timeout(Duration::from_secs(5), accepted)
        .await
        .expect("replacement stream accept timeout")
        .expect("replacement stream accept join")
        .expect("accept retry on replacement carrier");
    assert_eq!(
        current_client_carrier(&fixture.session)
            .await
            .expect("replacement remains installed")
            .path_instance_id,
        replacement_carrier.path_instance_id,
    );
}

#[tokio::test]
async fn current_carrier_lifetime_open_failure_retires_and_retries() {
    let fixture = ClientOpenRaceFixture::new().await;
    let first = fixture.establish_current().await;
    let first_carrier = current_client_carrier(&fixture.session)
        .await
        .expect("first client QUIC carrier");
    let stream_id = StreamId(943);
    let rejected = tokio::spawn(fail_test_stream_with_carrier_lifetime_loss(
        first.connection.clone(),
        stream_id,
        fixture.server_context.codec_limits,
    ));
    let (mut failure_reached, resume_failure) = install_open_failure_pause(&fixture.session);
    let opening = spawn_test_open(&fixture.session, stream_id);
    let failed_instance = tokio::time::timeout(Duration::from_secs(5), failure_reached.recv())
        .await
        .expect("real QUIC open failure hook timeout")
        .expect("real QUIC open failure hook closed");
    assert_eq!(failed_instance, first_carrier.path_instance_id);
    tokio::time::timeout(Duration::from_secs(5), rejected)
        .await
        .expect("server rejection timeout")
        .expect("server rejection join")
        .expect("publish carrier-lifetime loss");
    drop(first);

    // No successor exists yet: the callback is current and must retire this
    // exact carrier before the ordinary retry establishes another one.
    let successor_accept = fixture.spawn_server_accept();
    resume_failure.notify_one();
    let successor = tokio::time::timeout(Duration::from_secs(5), successor_accept)
        .await
        .expect("successor carrier handshake timeout")
        .expect("successor carrier handshake join")
        .expect("accept successor carrier");
    let accepted = tokio::spawn(accept_test_stream(
        successor.connection.clone(),
        stream_id,
        fixture.server_context.codec_limits,
    ));
    let opened = tokio::time::timeout(Duration::from_secs(5), opening)
        .await
        .expect("current-failure retry timeout")
        .expect("current-failure open task join")
        .expect("current failure retries successfully");
    tokio::time::timeout(Duration::from_secs(5), accepted)
        .await
        .expect("successor stream accept timeout")
        .expect("successor stream accept join")
        .expect("accept retry on successor carrier");
    assert_ne!(opened.path_instance_id, first_carrier.path_instance_id);
    assert!(first_carrier.connection.is_closed());
    assert_eq!(
        current_client_carrier(&fixture.session)
            .await
            .expect("successor remains installed")
            .path_instance_id,
        opened.path_instance_id,
    );
}

#[tokio::test]
async fn reliable_accepted_n_take_first_retries_on_real_n_plus_one() {
    let fixture = ClientOpenRaceFixture::new().await;
    let first = fixture.establish_current().await;
    let first_carrier = current_client_carrier(&fixture.session)
        .await
        .expect("first client QUIC carrier");
    let stream_id = StreamId(944);
    let accepted_n = tokio::spawn(accept_test_stream(
        first.connection.clone(),
        stream_id,
        fixture.server_context.codec_limits,
    ));
    let (mut accepted_reached, resume_accepted) = install_accepted_open_pause(&fixture.session);
    let opening = spawn_test_open(&fixture.session, stream_id);
    let (kind, accepted_instance) =
        tokio::time::timeout(Duration::from_secs(5), accepted_reached.recv())
            .await
            .expect("reliable Accepted-N hook timeout")
            .expect("reliable Accepted-N hook closed");
    assert_eq!(kind, ClientUdpAcceptedOpenKind::Reliable);
    assert_eq!(accepted_instance, first_carrier.path_instance_id);
    tokio::time::timeout(Duration::from_secs(5), accepted_n)
        .await
        .expect("server Accepted-N response timeout")
        .expect("server Accepted-N response join")
        .expect("server accepts N reliable Product stream");

    // A real physical owner closes and takes N, then publishes N+1 through
    // the same handle while N's already accepted value is paused.
    first_carrier.connection.close();
    drop(first);
    let successor_accept = fixture.spawn_server_accept();
    fixture
        .session
        .prepare_connection(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .expect("publish successor QUIC carrier");
    let successor = tokio::time::timeout(Duration::from_secs(5), successor_accept)
        .await
        .expect("successor handshake timeout")
        .expect("successor handshake join")
        .expect("accept successor carrier");
    let successor_carrier = current_client_carrier(&fixture.session)
        .await
        .expect("successor client QUIC carrier");
    assert_ne!(
        successor_carrier.path_instance_id,
        first_carrier.path_instance_id
    );
    let accepted_n_plus_one = tokio::spawn(accept_test_stream(
        successor.connection.clone(),
        stream_id,
        fixture.server_context.codec_limits,
    ));

    resume_accepted.notify_one();
    let opened = tokio::time::timeout(Duration::from_secs(5), opening)
        .await
        .expect("reliable Accepted-N settlement timeout")
        .expect("reliable Accepted-N task join")
        .expect("take-first settlement retries within the released bound");
    assert_eq!(
        opened.path_instance_id, successor_carrier.path_instance_id,
        "a take-first Accepted N must never escape as the Product owner",
    );
    tokio::time::timeout(Duration::from_secs(5), accepted_n_plus_one)
        .await
        .expect("server N+1 stream accept timeout")
        .expect("server N+1 stream accept join")
        .expect("accept bounded retry on N+1");
}

#[tokio::test]
async fn reliable_accepted_n_commit_first_remains_n_until_subsequent_take() {
    let fixture = ClientOpenRaceFixture::new().await;
    let first = fixture.establish_current().await;
    let first_carrier = current_client_carrier(&fixture.session)
        .await
        .expect("first client QUIC carrier");
    let stream_id = StreamId(945);
    let accepted_n = tokio::spawn(accept_test_stream(
        first.connection.clone(),
        stream_id,
        fixture.server_context.codec_limits,
    ));
    let (mut accepted_reached, resume_accepted) = install_accepted_open_pause(&fixture.session);
    let opening = spawn_test_open(&fixture.session, stream_id);
    let (kind, accepted_instance) =
        tokio::time::timeout(Duration::from_secs(5), accepted_reached.recv())
            .await
            .expect("reliable commit-first hook timeout")
            .expect("reliable commit-first hook closed");
    assert_eq!(kind, ClientUdpAcceptedOpenKind::Reliable);
    assert_eq!(accepted_instance, first_carrier.path_instance_id);
    tokio::time::timeout(Duration::from_secs(5), accepted_n)
        .await
        .expect("server commit-first response timeout")
        .expect("server commit-first response join")
        .expect("server accepts N reliable Product stream");

    resume_accepted.notify_one();
    let opened = tokio::time::timeout(Duration::from_secs(5), opening)
        .await
        .expect("reliable commit-first settlement timeout")
        .expect("reliable commit-first task join")
        .expect("commit-first returns the accepted Product owner");
    assert_eq!(opened.path_instance_id, first_carrier.path_instance_id);

    // Only after N was returned do we close/take it and publish N+1. That
    // later lifecycle event cannot retroactively invalidate the committed N.
    first_carrier.connection.close();
    drop(first);
    let successor_accept = fixture.spawn_server_accept();
    fixture
        .session
        .prepare_connection(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .expect("publish post-commit successor");
    let _successor = tokio::time::timeout(Duration::from_secs(5), successor_accept)
        .await
        .expect("post-commit successor timeout")
        .expect("post-commit successor join")
        .expect("accept post-commit successor");
    assert_ne!(
        current_client_carrier(&fixture.session)
            .await
            .expect("post-commit successor remains current")
            .path_instance_id,
        opened.path_instance_id,
    );
}

#[tokio::test]
async fn datagram_accepted_n_take_first_retries_on_real_n_plus_one() {
    let fixture = ClientOpenRaceFixture::new().await;
    let first = fixture.establish_current().await;
    let first_carrier = current_client_carrier(&fixture.session)
        .await
        .expect("first client QUIC carrier");
    let accepted_n = tokio::spawn(accept_test_datagram_request(
        first.connection.clone(),
        fixture.server_context.codec_limits,
    ));
    let (mut accepted_reached, resume_accepted) = install_accepted_open_pause(&fixture.session);
    let opening = spawn_test_datagram_open(&fixture.session);
    let (kind, accepted_instance) =
        tokio::time::timeout(Duration::from_secs(5), accepted_reached.recv())
            .await
            .expect("datagram Accepted-N hook timeout")
            .expect("datagram Accepted-N hook closed");
    assert_eq!(kind, ClientUdpAcceptedOpenKind::Datagram);
    assert_eq!(accepted_instance, first_carrier.path_instance_id);
    tokio::time::timeout(Duration::from_secs(5), accepted_n)
        .await
        .expect("server accepts N datagram request timeout")
        .expect("server accepts N datagram request join")
        .expect("server accepts N HTTP/3 Product request");

    first_carrier.connection.close();
    drop(first);
    let successor_accept = fixture.spawn_server_accept();
    fixture
        .session
        .prepare_connection(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .expect("publish successor QUIC carrier");
    let successor = tokio::time::timeout(Duration::from_secs(5), successor_accept)
        .await
        .expect("successor datagram handshake timeout")
        .expect("successor datagram handshake join")
        .expect("accept successor datagram carrier");
    let successor_carrier = current_client_carrier(&fixture.session)
        .await
        .expect("successor datagram client carrier");
    let accepted_n_plus_one = tokio::spawn(accept_test_datagram_request(
        successor.connection.clone(),
        fixture.server_context.codec_limits,
    ));

    resume_accepted.notify_one();
    let opened = tokio::time::timeout(Duration::from_secs(5), opening)
        .await
        .expect("datagram Accepted-N settlement timeout")
        .expect("datagram Accepted-N task join")
        .expect("take-first datagram settlement retries within the released bound");
    assert_eq!(
        opened.path_instance_id, successor_carrier.path_instance_id,
        "a take-first datagram request on N must not escape",
    );
    tokio::time::timeout(Duration::from_secs(5), accepted_n_plus_one)
        .await
        .expect("server N+1 datagram accept timeout")
        .expect("server N+1 datagram accept join")
        .expect("accept bounded datagram retry on N+1");
}

#[tokio::test]
async fn datagram_accepted_n_commit_first_remains_n_until_subsequent_take() {
    let fixture = ClientOpenRaceFixture::new().await;
    let first = fixture.establish_current().await;
    let first_carrier = current_client_carrier(&fixture.session)
        .await
        .expect("first client QUIC carrier");
    let accepted_n = tokio::spawn(accept_test_datagram_request(
        first.connection.clone(),
        fixture.server_context.codec_limits,
    ));
    let (mut accepted_reached, resume_accepted) = install_accepted_open_pause(&fixture.session);
    let opening = spawn_test_datagram_open(&fixture.session);
    let (kind, accepted_instance) =
        tokio::time::timeout(Duration::from_secs(5), accepted_reached.recv())
            .await
            .expect("datagram commit-first hook timeout")
            .expect("datagram commit-first hook closed");
    assert_eq!(kind, ClientUdpAcceptedOpenKind::Datagram);
    assert_eq!(accepted_instance, first_carrier.path_instance_id);
    tokio::time::timeout(Duration::from_secs(5), accepted_n)
        .await
        .expect("server datagram commit-first timeout")
        .expect("server datagram commit-first join")
        .expect("server accepts N HTTP/3 Product request");

    resume_accepted.notify_one();
    let opened = tokio::time::timeout(Duration::from_secs(5), opening)
        .await
        .expect("datagram commit-first settlement timeout")
        .expect("datagram commit-first task join")
        .expect("datagram commit-first returns the accepted owner");
    assert_eq!(opened.path_instance_id, first_carrier.path_instance_id);

    first_carrier.connection.close();
    drop(first);
    let successor_accept = fixture.spawn_server_accept();
    fixture
        .session
        .prepare_connection(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .expect("publish post-commit datagram successor");
    let _successor = tokio::time::timeout(Duration::from_secs(5), successor_accept)
        .await
        .expect("post-commit datagram successor timeout")
        .expect("post-commit datagram successor join")
        .expect("accept post-commit datagram successor");
    assert_ne!(
        current_client_carrier(&fixture.session)
            .await
            .expect("post-commit datagram successor remains current")
            .path_instance_id,
        opened.path_instance_id,
    );
}

#[test]
fn product_error_disposition_separates_session_carrier_and_operation_authority() {
    assert_eq!(
        client_udp_error_disposition(&RuntimeError::RemoteClosed(CloseReason::Normal)),
        ClientUdpErrorDisposition::Session,
    );
    for carrier in [
        RuntimeError::QuicCarrier(QuicCarrierError::Io(std::io::Error::other(
            "established socket failed",
        ))),
        RuntimeError::QuicCarrier(QuicCarrierError::Connection(
            quinn::ConnectionError::LocallyClosed,
        )),
        RuntimeError::QuicCarrier(QuicCarrierError::Write(quinn::WriteError::ConnectionLost(
            quinn::ConnectionError::LocallyClosed,
        ))),
        RuntimeError::QuicCarrier(QuicCarrierError::Read(quinn::ReadError::ConnectionLost(
            quinn::ConnectionError::LocallyClosed,
        ))),
        RuntimeError::QuicCarrier(QuicCarrierError::H3DriverClosed),
        RuntimeError::QuicCarrier(QuicCarrierError::NativeDatagram(
            quinn::SendDatagramError::ConnectionLost(quinn::ConnectionError::LocallyClosed),
        )),
    ] {
        assert_eq!(
            client_udp_error_disposition(&carrier),
            ClientUdpErrorDisposition::CarrierLifetime,
            "physical carrier source lacked carrier authority: {carrier}",
        );
    }

    for operation in [
        RuntimeError::RemotePathClosed(CloseReason::Normal),
        RuntimeError::PathOpenTimedOut,
        RuntimeError::PathHeartbeatTimeout,
        RuntimeError::ReliablePathSessionClosed,
        RuntimeError::ReliablePathRetired,
        RuntimeError::Protocol("Product response shape"),
        RuntimeError::QuicCarrier(QuicCarrierError::Write(quinn::WriteError::Stopped(
            quinn::VarInt::from_u32(1),
        ))),
        RuntimeError::QuicCarrier(QuicCarrierError::Write(quinn::WriteError::ClosedStream)),
        RuntimeError::QuicCarrier(QuicCarrierError::Read(quinn::ReadError::Reset(
            quinn::VarInt::from_u32(1),
        ))),
        RuntimeError::QuicCarrier(QuicCarrierError::Read(quinn::ReadError::ClosedStream)),
        RuntimeError::QuicCarrier(QuicCarrierError::H3StreamFinished),
        RuntimeError::QuicCarrier(QuicCarrierError::StreamFinished),
        RuntimeError::QuicCarrier(QuicCarrierError::UnexpectedEnd),
        RuntimeError::QuicCarrier(QuicCarrierError::H3Status(http::StatusCode::NOT_FOUND)),
        RuntimeError::QuicCarrier(QuicCarrierError::H3Role("Product request role")),
        RuntimeError::QuicCarrier(QuicCarrierError::H3DatagramNotNegotiated),
        RuntimeError::QuicCarrier(QuicCarrierError::NativeDatagram(
            quinn::SendDatagramError::TooLarge,
        )),
        RuntimeError::QuicCarrier(QuicCarrierError::NativeDatagramUnavailable),
        RuntimeError::QuicCarrier(QuicCarrierError::NativeDatagramTooLarge),
    ] {
        assert_eq!(
            client_udp_error_disposition(&operation),
            ClientUdpErrorDisposition::Operation,
            "operation-local source gained carrier authority: {operation}",
        );
    }
    assert_eq!(MAX_CLIENT_UDP_EXACT_OPEN_ATTEMPTS, 2);
}

#[tokio::test]
async fn session_and_live_operation_settlement_do_not_take_or_fail_the_carrier() {
    let fixture = ClientOpenRaceFixture::new().await;
    let first = fixture.establish_current().await;
    let current = current_client_carrier(&fixture.session)
        .await
        .expect("current client QUIC carrier");
    let before = fixture
        .session
        .runtime
        .state
        .health()
        .lock()
        .expect("health before typed settlement")
        .udp[0]
        .eligibility_fingerprint();

    assert_eq!(
        fixture
            .session
            .settle_established_error(
                current.path_instance_id,
                &RuntimeError::RemoteClosed(CloseReason::Normal),
            )
            .await,
        ClientUdpErrorDisposition::Session,
    );
    assert_eq!(
        fixture
            .session
            .settle_established_error(
                current.path_instance_id,
                &RuntimeError::Protocol("Product response shape"),
            )
            .await,
        ClientUdpErrorDisposition::Operation,
    );
    assert_eq!(
        fixture
            .session
            .settle_established_error(
                current.path_instance_id,
                &RuntimeError::QuicCarrier(
                    QuicCarrierError::H3Status(http::StatusCode::NOT_FOUND,)
                ),
            )
            .await,
        ClientUdpErrorDisposition::Operation,
    );
    assert_eq!(
        current_client_carrier(&fixture.session)
            .await
            .expect("typed non-carrier errors preserve owner")
            .path_instance_id,
        current.path_instance_id,
    );
    assert_eq!(
        fixture
            .session
            .runtime
            .state
            .health()
            .lock()
            .expect("health after typed settlement")
            .udp[0]
            .eligibility_fingerprint(),
        before,
    );
    drop(first);
}

#[tokio::test]
async fn carrier_lifetime_settlement_takes_and_fails_only_the_exact_owner() {
    let fixture = ClientOpenRaceFixture::new().await;
    let first = fixture.establish_current().await;
    let current = current_client_carrier(&fixture.session)
        .await
        .expect("current client QUIC carrier");
    assert_eq!(
        fixture
            .session
            .settle_established_error(
                current.path_instance_id,
                &RuntimeError::QuicCarrier(QuicCarrierError::H3DriverClosed),
            )
            .await,
        ClientUdpErrorDisposition::CarrierLifetime,
    );
    assert!(current_client_carrier(&fixture.session).await.is_none());
    let health = fixture
        .session
        .runtime
        .state
        .health()
        .lock()
        .expect("health after carrier settlement");
    assert_eq!(
        health.udp[0].path_instance_id(),
        Some(current.path_instance_id)
    );
    assert_eq!(health.udp[0].consecutive_failures, 1);
    drop(health);
    drop(first);
}
