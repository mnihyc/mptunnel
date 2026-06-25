use crate::config::{AppConfig, ClientConfig, CommandConfig, ResourceLimits, SecurityConfig};
use crate::ingress::IngressConfig;
use crate::ingress::http_connect::{self, HttpConnectError, HttpStatus};
use crate::ingress::socks5::{self, Socks5Error, Socks5Reply};
use crate::mux::MuxLimits;
use crate::mux::datagram::{DatagramError, DatagramFlow};
use crate::mux::stream::{ReliableRecvStream, ReliableSendStream, StreamError};
use crate::outbound::{self, OutboundConfig, TargetProtocol};
use crate::protocol::auth::{AuthError, SessionAuthenticator};
use crate::protocol::codec::CodecLimits;
use crate::protocol::{
    AuthNonce, CloseReason, DatagramFlowId, Frame, IngressKind, OutboundPolicy, PathCapabilities,
    PathId, ResetReason, SessionId, StreamFlags, StreamId, TargetAddr, TrafficClass,
    UnderlayProtocol,
};
use crate::transport::encrypted::{EncryptedFramedStream, EncryptedFramedTransportError, PeerRole};
use crate::transport::encrypted_udp::{EncryptedUdpSocket, EncryptedUdpTransportError};
use crate::transport::tcp::{self, TcpConnectOptions, TcpTransportError};
use crate::transport::udp::{self, UdpTransportError};
use crate::transport::{PathSpec, PathSpecParseError};
use bytes::Bytes;
use std::future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};

const MAX_HTTP_CONNECT_HEADER_BYTES: usize = 64 * 1024;

pub async fn run(config: AppConfig) -> Result<(), RuntimeError> {
    match config.command {
        CommandConfig::Client(client) => {
            run_client(client, config.security, config.resources).await
        }
        CommandConfig::Server(server) => {
            run_server(
                server.bind_paths,
                server.outbound,
                config.security,
                config.resources,
            )
            .await
        }
    }
}

async fn run_client(
    client: ClientConfig,
    security: SecurityConfig,
    resources: ResourceLimits,
) -> Result<(), RuntimeError> {
    let context = ClientPathContext::new(client.paths, security, resources)?;
    match client.ingress {
        IngressConfig::Socks5 { listen } => {
            let listener = TcpListener::bind(listen).await?;
            loop {
                let (stream, _) = listener.accept().await?;
                let context = context.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_socks5_client_stream(stream, context).await {
                        eprintln!("warning: SOCKS5 client handler failed: {err}");
                    }
                });
            }
        }
        IngressConfig::HttpConnect { listen } => {
            let listener = TcpListener::bind(listen).await?;
            loop {
                let (stream, _) = listener.accept().await?;
                let context = context.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_http_connect_client_stream(stream, context).await {
                        eprintln!("warning: HTTP CONNECT client handler failed: {err}");
                    }
                });
            }
        }
        IngressConfig::TunL4(_) => Err(RuntimeError::UnsupportedIngress("tun-l4")),
    }
}

async fn run_server(
    bind_paths: Vec<PathSpec>,
    outbound: OutboundConfig,
    security: SecurityConfig,
    resources: ResourceLimits,
) -> Result<(), RuntimeError> {
    let context = ServerPathContext {
        outbound,
        codec_limits: resources.into(),
        mux_limits: resources.into(),
        security,
    };
    for path in bind_paths {
        match path.underlay {
            UnderlayProtocol::Tcp => {
                let listener = tcp::bind_listener(&path).await?;
                let context = context.clone();
                tokio::spawn(async move {
                    if let Err(err) = run_server_tcp_listener(listener, context).await {
                        eprintln!("warning: TCP server listener failed: {err}");
                    }
                });
            }
            UnderlayProtocol::Udp => {
                let socket = udp::bind_socket(&path).await?;
                let context = context.clone();
                tokio::spawn(async move {
                    if let Err(err) = run_server_udp_listener(socket, context).await {
                        eprintln!("warning: UDP server listener failed: {err}");
                    }
                });
            }
        }
    }
    future::pending::<()>().await;
    Ok(())
}

async fn run_server_tcp_listener(
    listener: TcpListener,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    loop {
        let (stream, _) = listener.accept().await?;
        let context = context.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_server_path(stream, context).await {
                eprintln!("warning: server path handler failed: {err}");
            }
        });
    }
}

async fn run_server_udp_listener(
    socket: UdpSocket,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    let socket = Arc::new(socket);
    loop {
        if let Err(err) =
            handle_server_udp_datagram_shared_path_session(socket.clone(), context.clone()).await
        {
            eprintln!("warning: UDP server path session failed: {err}");
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClientPathContext {
    tcp_path: Option<PathSpec>,
    udp_path: Option<PathSpec>,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    security: SecurityConfig,
}

impl ClientPathContext {
    pub fn new(
        paths: Vec<PathSpec>,
        security: SecurityConfig,
        resources: ResourceLimits,
    ) -> Result<Self, RuntimeError> {
        Ok(Self {
            tcp_path: first_tcp_path(&paths).ok().cloned(),
            udp_path: first_udp_path(&paths).cloned(),
            codec_limits: resources.into(),
            mux_limits: resources.into(),
            security,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ServerPathContext {
    outbound: OutboundConfig,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    security: SecurityConfig,
}

pub async fn handle_socks5_client_stream<S>(
    mut stream: S,
    context: ClientPathContext,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let auth = read_socks5_auth(&mut stream).await?;
    if !auth.supports_no_auth() {
        stream
            .write_all(&socks5::no_acceptable_methods_response())
            .await?;
        return Err(RuntimeError::Socks5(Socks5Error::UnsupportedCommand(0)));
    }
    stream.write_all(&socks5::no_auth_response()).await?;
    let request = read_socks5_command(&mut stream).await?;
    match request.command {
        socks5::Socks5Command::Connect => {
            let remote = match open_remote_stream(
                &context,
                request.target,
                IngressKind::Socks5,
                TrafficClass::Interactive,
            )
            .await
            {
                Ok(remote) => remote,
                Err(err) => {
                    stream
                        .write_all(&socks5::connect_reply(
                            Socks5Reply::GeneralFailure,
                            SocketAddr::from(([0, 0, 0, 0], 0)),
                        ))
                        .await?;
                    return Err(err);
                }
            };
            stream
                .write_all(&socks5::connect_reply(
                    Socks5Reply::Succeeded,
                    SocketAddr::from(([0, 0, 0, 0], 0)),
                ))
                .await?;
            stream.flush().await?;
            relay_tcp_stream(
                stream,
                remote.framed,
                remote.stream_id,
                context.mux_limits,
                remote.max_offset,
            )
            .await
        }
        socks5::Socks5Command::UdpAssociate => {
            handle_socks5_udp_associate(
                &mut stream,
                context,
                socks5::UdpAssociateRequest {
                    client_endpoint: request.target,
                },
            )
            .await
        }
    }
}

pub async fn handle_http_connect_client_stream<S>(
    mut stream: S,
    context: ClientPathContext,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request = read_http_connect(&mut stream).await?;
    let remote = match open_remote_stream(
        &context,
        request.target,
        IngressKind::HttpConnect,
        TrafficClass::Interactive,
    )
    .await
    {
        Ok(remote) => remote,
        Err(err) => {
            stream
                .write_all(http_connect::error_response(HttpStatus::BadGateway))
                .await?;
            return Err(err);
        }
    };
    stream.write_all(http_connect::success_response()).await?;
    stream.flush().await?;
    relay_tcp_stream(
        stream,
        remote.framed,
        remote.stream_id,
        context.mux_limits,
        remote.max_offset,
    )
    .await
}

struct OpenedRemoteStream {
    framed: EncryptedFramedStream<TcpStream>,
    stream_id: StreamId,
    max_offset: u64,
}

async fn open_remote_stream(
    context: &ClientPathContext,
    target: TargetAddr,
    ingress: IngressKind,
    class: TrafficClass,
) -> Result<OpenedRemoteStream, RuntimeError> {
    let path = context.tcp_path.as_ref().ok_or(RuntimeError::NoTcpPath)?;
    let tcp_stream = tcp::connect_path(path, TcpConnectOptions::default()).await?;
    let mut framed = EncryptedFramedStream::new(
        tcp_stream,
        context.security.secret.as_bytes(),
        PeerRole::Client,
        context.codec_limits,
    );
    let session_id = random_session_id()?;
    let path_id = PathId(0);
    let authenticator = SessionAuthenticator::new(context.security.secret.as_bytes())?;
    let session_nonce = random_nonce()?;
    let session_tag = authenticator.session_auth_tag(session_id, session_nonce);
    let path_nonce = random_nonce()?;
    let capabilities = PathCapabilities::default();
    let path_tag = authenticator.path_join_tag(
        session_id,
        path_id,
        UnderlayProtocol::Tcp,
        path_nonce,
        capabilities,
    );

    framed
        .write_frame(&Frame::SessionHello { session_id })
        .await?;
    framed
        .write_frame(&Frame::SessionAuth {
            session_id,
            nonce: session_nonce,
            auth_tag: session_tag,
        })
        .await?;
    framed
        .write_frame(&Frame::PathJoin {
            session_id,
            path_id,
            underlay: UnderlayProtocol::Tcp,
            nonce: path_nonce,
            capabilities,
            auth_tag: path_tag,
        })
        .await?;
    let stream_id = StreamId(0);
    framed
        .write_frame(&Frame::OpenStream {
            stream_id,
            target,
            ingress,
            outbound: OutboundPolicy::Direct,
            class,
        })
        .await?;
    framed.flush().await?;
    let mut session_ready = false;
    loop {
        match framed.read_frame().await? {
            Frame::SessionReady => session_ready = true,
            Frame::PathStatus { .. } => {}
            Frame::StreamMaxData {
                stream_id: accepted_stream_id,
                max_offset,
            } if accepted_stream_id == stream_id && session_ready => {
                return Ok(OpenedRemoteStream {
                    framed,
                    stream_id,
                    max_offset,
                });
            }
            Frame::StreamReset {
                stream_id: reset_stream_id,
                reason,
            } if reset_stream_id == stream_id => {
                return Err(RuntimeError::RemoteReset(reason));
            }
            Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
            _ => {
                return Err(RuntimeError::Protocol(
                    "unexpected frame while opening stream",
                ));
            }
        }
    }
}

const DEFAULT_SOCKS5_UDP_TTL_MS: u32 = 30_000;

async fn handle_socks5_udp_associate<S>(
    stream: &mut S,
    context: ClientPathContext,
    request: socks5::UdpAssociateRequest,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let path = context.udp_path.as_ref().ok_or(RuntimeError::NoUdpPath)?;
    let client_endpoint = request.client_endpoint;
    let relay_socket = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let relay_addr = relay_socket.local_addr()?;
    stream
        .write_all(&socks5::connect_reply(Socks5Reply::Succeeded, relay_addr))
        .await?;
    stream.flush().await?;

    let mut packet = vec![0u8; local_udp_buffer_len(context.mux_limits)];
    let mut control_probe = [0u8; 1];
    let mut udp_session: Option<UdpDatagramClientSession> = None;
    loop {
        tokio::select! {
            read = stream.read(&mut control_probe) => {
                let read = read?;
                if read == 0 {
                    if let Some(session) = udp_session.as_mut() {
                        session.close().await?;
                    }
                    return Ok(());
                }
                return Err(RuntimeError::Protocol("unexpected data on SOCKS5 UDP control stream"));
            }
            received = relay_socket.recv_from(&mut packet) => {
                let (len, peer) = received?;
                if !socks5_udp_peer_allowed(&client_endpoint, peer) {
                    return Err(RuntimeError::Protocol("SOCKS5 UDP peer does not match association"));
                }
                let (datagram, consumed) = socks5::parse_udp_datagram(&packet[..len])?;
                if consumed != len {
                    return Err(RuntimeError::Protocol("trailing SOCKS5 UDP datagram bytes"));
                }
                let target = datagram.target.clone();
                if udp_session.is_none() {
                    udp_session = Some(UdpDatagramClientSession::open(
                        path,
                        context.security.clone(),
                        context.codec_limits,
                        context.mux_limits,
                    )
                    .await?);
                }
                let response = udp_session
                    .as_mut()
                    .ok_or(RuntimeError::Protocol("missing UDP datagram session"))?
                    .send_to(target.clone(), datagram.payload, DEFAULT_SOCKS5_UDP_TTL_MS)
                    .await?;
                let response_packet = socks5::udp_datagram(&target, &response)?;
                relay_socket.send_to(&response_packet, peer).await?;
            }
        }
    }
}

fn local_udp_buffer_len(mux_limits: MuxLimits) -> usize {
    mux_limits
        .max_payload_bytes
        .saturating_add(512)
        .clamp(512, 65_535)
}

fn socks5_udp_peer_allowed(client_endpoint: &TargetAddr, peer: SocketAddr) -> bool {
    match client_endpoint {
        TargetAddr::Ip(addr) => {
            let ip_matches = addr.ip().is_unspecified() || addr.ip() == peer.ip();
            let port_matches = addr.port() == 0 || addr.port() == peer.port();
            ip_matches && port_matches
        }
        TargetAddr::Domain { port, .. } => *port == 0 || *port == peer.port(),
    }
}

async fn handle_server_path(
    stream: TcpStream,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    let mut framed = EncryptedFramedStream::new(
        stream,
        context.security.secret.as_bytes(),
        PeerRole::Server,
        context.codec_limits,
    );
    let session_id = match framed.read_frame().await? {
        Frame::SessionHello { session_id } => session_id,
        _ => return Err(RuntimeError::Protocol("expected SESSION_HELLO")),
    };
    let authenticator = SessionAuthenticator::new(context.security.secret.as_bytes())?;
    match framed.read_frame().await? {
        Frame::SessionAuth {
            session_id: auth_session_id,
            nonce,
            auth_tag,
        } if auth_session_id == session_id
            && authenticator.verify_session_auth(session_id, nonce, auth_tag) => {}
        _ => return Err(RuntimeError::Protocol("invalid SESSION_AUTH")),
    }
    let path_id = match framed.read_frame().await? {
        Frame::PathJoin {
            session_id: join_session_id,
            path_id,
            underlay,
            nonce,
            capabilities,
            auth_tag,
        } if join_session_id == session_id
            && underlay == UnderlayProtocol::Tcp
            && authenticator.verify_path_join(
                session_id,
                path_id,
                underlay,
                nonce,
                capabilities,
                auth_tag,
            ) =>
        {
            path_id
        }
        _ => return Err(RuntimeError::Protocol("invalid PATH_JOIN")),
    };
    let (stream_id, target) = match framed.read_frame().await? {
        Frame::OpenStream {
            stream_id, target, ..
        } => {
            outbound::validate_target(&target)?;
            context.outbound.ensure_supports(TargetProtocol::Tcp)?;
            (stream_id, target)
        }
        _ => return Err(RuntimeError::Protocol("expected OPEN_STREAM")),
    };
    let outbound_stream =
        match outbound::connect_tcp(&context.outbound, &target, Duration::from_secs(10)).await {
            Ok(stream) => stream,
            Err(err) => {
                framed
                    .write_frame(&Frame::StreamReset {
                        stream_id,
                        reason: ResetReason::Refused,
                    })
                    .await?;
                framed.flush().await?;
                return Err(RuntimeError::OutboundConnect(err));
            }
        };
    framed.write_frame(&Frame::SessionReady).await?;
    framed
        .write_frame(&Frame::PathStatus {
            path_id,
            status: crate::protocol::PathStatus::Active,
            capabilities: PathCapabilities::default(),
        })
        .await?;
    framed
        .write_frame(&Frame::StreamMaxData {
            stream_id,
            max_offset: context.mux_limits.max_stream_window_bytes,
        })
        .await?;
    framed.flush().await?;
    relay_tcp_stream(
        outbound_stream,
        framed,
        stream_id,
        context.mux_limits,
        context.mux_limits.max_stream_window_bytes,
    )
    .await
}

async fn relay_tcp_stream<S>(
    mut local: S,
    mut framed: EncryptedFramedStream<TcpStream>,
    stream_id: StreamId,
    mux_limits: MuxLimits,
    initial_max_offset: u64,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut send_stream = ReliableSendStream::new(stream_id, mux_limits);
    send_stream.update_max_offset(initial_max_offset);
    let mut recv_stream = ReliableRecvStream::new(stream_id, mux_limits);
    let chunk_size = mux_limits.max_payload_bytes.clamp(1, 16 * 1024);
    let mut buf = vec![0u8; chunk_size];
    let mut local_open = true;
    let mut remote_open = true;

    loop {
        if !local_open && !remote_open && send_stream.repair_bytes() == 0 {
            break;
        }

        tokio::select! {
            read = local.read(&mut buf), if local_open => {
                let read = read?;
                if read == 0 {
                    framed.write_frame(&Frame::StreamFin { stream_id }).await?;
                    framed.flush().await?;
                    local_open = false;
                } else {
                    let frame = send_stream.send_data(
                        Bytes::copy_from_slice(&buf[..read]),
                        StreamFlags::NONE,
                    )?;
                    framed.write_frame(&frame).await?;
                    framed.flush().await?;
                }
            }
            frame = framed.read_frame(), if remote_open || send_stream.repair_bytes() > 0 => {
                match frame? {
                    Frame::StreamData {
                        stream_id: received_stream_id,
                        offset,
                        flags,
                        payload,
                    } if received_stream_id == stream_id && remote_open => {
                        let outcome = recv_stream.receive_data(offset, payload, flags)?;
                        for chunk in outcome.delivered {
                            local.write_all(&chunk).await?;
                        }
                        framed.write_frame(&recv_stream.ack_frame()).await?;
                        framed.flush().await?;
                        if outcome.fin {
                            local.shutdown().await?;
                            remote_open = false;
                        }
                    }
                    Frame::StreamAck {
                        stream_id: ack_stream_id,
                        ranges,
                    } if ack_stream_id == stream_id => {
                        send_stream.apply_ack(&ranges);
                    }
                    Frame::StreamMaxData {
                        stream_id: max_stream_id,
                        max_offset,
                    } if max_stream_id == stream_id => {
                        send_stream.update_max_offset(max_offset);
                    }
                    Frame::StreamFin { stream_id: fin_stream_id } if fin_stream_id == stream_id => {
                        local.shutdown().await?;
                        remote_open = false;
                    }
                    Frame::StreamReset {
                        stream_id: reset_stream_id,
                        reason,
                    } if reset_stream_id == stream_id => return Err(RuntimeError::RemoteReset(reason)),
                    Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
                    _ => return Err(RuntimeError::Protocol("unexpected stream relay frame")),
                }
            }
            else => break,
        }
    }

    Ok(())
}

pub async fn client_udp_datagram_round_trip(
    path: &PathSpec,
    security: SecurityConfig,
    resources: ResourceLimits,
    target: TargetAddr,
    payload: Bytes,
    ttl_ms: u32,
) -> Result<Bytes, RuntimeError> {
    client_udp_datagram_round_trip_with_limits(
        path,
        security,
        resources.into(),
        resources.into(),
        target,
        payload,
        ttl_ms,
    )
    .await
}

async fn client_udp_datagram_round_trip_with_limits(
    path: &PathSpec,
    security: SecurityConfig,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    target: TargetAddr,
    payload: Bytes,
    ttl_ms: u32,
) -> Result<Bytes, RuntimeError> {
    let mut session =
        UdpDatagramClientSession::open(path, security, codec_limits, mux_limits).await?;
    let response = session.send_to(target, payload, ttl_ms).await?;
    session.close().await?;
    Ok(response)
}

struct UdpDatagramClientSession {
    encrypted: EncryptedUdpSocket,
    buffer: Vec<u8>,
    flows: Vec<UdpDatagramClientFlow>,
    next_flow_id: u64,
    mux_limits: MuxLimits,
}

struct UdpDatagramClientFlow {
    target: TargetAddr,
    flow: DatagramFlow,
    flow_id: DatagramFlowId,
}

impl UdpDatagramClientSession {
    async fn open(
        path: &PathSpec,
        security: SecurityConfig,
        codec_limits: CodecLimits,
        mux_limits: MuxLimits,
    ) -> Result<Self, RuntimeError> {
        let socket =
            udp::connect_path(path, crate::transport::udp::UdpConnectOptions::default()).await?;
        let mut encrypted = EncryptedUdpSocket::new(
            socket,
            security.secret.as_bytes(),
            PeerRole::Client,
            codec_limits,
        );
        let session_id = random_session_id()?;
        let path_id = PathId(0);
        let authenticator = SessionAuthenticator::new(security.secret.as_bytes())?;
        let session_nonce = random_nonce()?;
        let session_tag = authenticator.session_auth_tag(session_id, session_nonce);
        let path_nonce = random_nonce()?;
        let capabilities = PathCapabilities::default();
        let path_tag = authenticator.path_join_tag(
            session_id,
            path_id,
            UnderlayProtocol::Udp,
            path_nonce,
            capabilities,
        );

        encrypted
            .send_frame(&Frame::SessionHello { session_id })
            .await?;
        encrypted
            .send_frame(&Frame::SessionAuth {
                session_id,
                nonce: session_nonce,
                auth_tag: session_tag,
            })
            .await?;
        encrypted
            .send_frame(&Frame::PathJoin {
                session_id,
                path_id,
                underlay: UnderlayProtocol::Udp,
                nonce: path_nonce,
                capabilities,
                auth_tag: path_tag,
            })
            .await?;

        let mut buffer = vec![0u8; encrypted.max_datagram_bytes()?];
        let mut session_ready = false;
        let mut path_active = false;
        while !session_ready || !path_active {
            match encrypted.recv_frame(&mut buffer).await? {
                Frame::SessionReady => session_ready = true,
                Frame::PathStatus { .. } => path_active = true,
                Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
                _ => return Err(RuntimeError::Protocol("unexpected UDP handshake frame")),
            }
        }

        Ok(Self {
            encrypted,
            buffer,
            flows: Vec::new(),
            next_flow_id: 0,
            mux_limits,
        })
    }

    async fn send_to(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        ttl_ms: u32,
    ) -> Result<Bytes, RuntimeError> {
        let flow_id = self.ensure_flow(target).await?;
        let flow = self
            .flows
            .iter_mut()
            .find(|flow| flow.flow_id == flow_id)
            .ok_or(RuntimeError::Protocol("missing UDP datagram flow"))?;
        flow.flow.enqueue(0, ttl_ms, payload)?;
        let frame = self
            .flows
            .iter_mut()
            .find(|flow| flow.flow_id == flow_id)
            .ok_or(RuntimeError::Protocol("missing UDP datagram flow"))?
            .flow
            .pop_frame(0)
            .ok_or(RuntimeError::Protocol("datagram expired before send"))?;
        self.encrypted.send_frame(&frame).await?;

        match self.encrypted.recv_frame(&mut self.buffer).await? {
            Frame::DatagramData {
                flow_id: response_flow_id,
                payload,
                ..
            } if response_flow_id == flow_id => Ok(payload),
            Frame::DatagramClose {
                flow_id: closed_flow_id,
            } if closed_flow_id == flow_id => Err(RuntimeError::Protocol("datagram flow closed")),
            Frame::SessionClose { reason } => Err(RuntimeError::RemoteClosed(reason)),
            _ => Err(RuntimeError::Protocol("unexpected UDP datagram frame")),
        }
    }

    async fn ensure_flow(&mut self, target: TargetAddr) -> Result<DatagramFlowId, RuntimeError> {
        if let Some(flow) = self.flows.iter().find(|flow| flow.target == target) {
            return Ok(flow.flow_id);
        }
        let flow_id = DatagramFlowId(self.next_flow_id);
        self.next_flow_id = self
            .next_flow_id
            .checked_add(1)
            .ok_or(RuntimeError::Protocol("UDP datagram flow id overflow"))?;
        self.encrypted
            .send_frame(&Frame::OpenDatagramFlow {
                flow_id,
                target: target.clone(),
                ingress: IngressKind::TunUdp,
                outbound: OutboundPolicy::Direct,
                class: TrafficClass::RealtimeDatagram,
            })
            .await?;
        self.flows.push(UdpDatagramClientFlow {
            target,
            flow: DatagramFlow::new(flow_id, self.mux_limits),
            flow_id,
        });
        Ok(flow_id)
    }

    async fn close(&mut self) -> Result<(), RuntimeError> {
        for flow in &self.flows {
            self.encrypted
                .send_frame(&Frame::DatagramClose {
                    flow_id: flow.flow_id,
                })
                .await?;
        }
        self.flows.clear();
        Ok(())
    }
}

pub async fn handle_server_udp_datagram_path_session(
    socket: UdpSocket,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    let encrypted = EncryptedUdpSocket::new(
        socket,
        context.security.secret.as_bytes(),
        PeerRole::Server,
        context.codec_limits,
    );
    handle_server_udp_datagram_path(encrypted, context).await
}

async fn handle_server_udp_datagram_shared_path_session(
    socket: Arc<UdpSocket>,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    let encrypted = EncryptedUdpSocket::from_shared(
        socket,
        context.security.secret.as_bytes(),
        PeerRole::Server,
        context.codec_limits,
    );
    handle_server_udp_datagram_path(encrypted, context).await
}

struct ServerUdpDatagramFlow {
    flow_id: DatagramFlowId,
    outbound_socket: UdpSocket,
}

async fn handle_server_udp_datagram_path(
    mut encrypted: EncryptedUdpSocket,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    let mut buffer = vec![0u8; encrypted.max_datagram_bytes()?];
    let (session_id, peer) = match encrypted.recv_frame_from(&mut buffer).await? {
        (Frame::SessionHello { session_id }, peer) => (session_id, peer),
        _ => return Err(RuntimeError::Protocol("expected UDP SESSION_HELLO")),
    };
    let authenticator = SessionAuthenticator::new(context.security.secret.as_bytes())?;
    match recv_udp_frame_from_peer(&mut encrypted, &mut buffer, peer).await? {
        Frame::SessionAuth {
            session_id: auth_session_id,
            nonce,
            auth_tag,
        } if auth_session_id == session_id
            && authenticator.verify_session_auth(session_id, nonce, auth_tag) => {}
        _ => return Err(RuntimeError::Protocol("invalid UDP SESSION_AUTH")),
    }
    let path_id = match recv_udp_frame_from_peer(&mut encrypted, &mut buffer, peer).await? {
        Frame::PathJoin {
            session_id: join_session_id,
            path_id,
            underlay,
            nonce,
            capabilities,
            auth_tag,
        } if join_session_id == session_id
            && underlay == UnderlayProtocol::Udp
            && authenticator.verify_path_join(
                session_id,
                path_id,
                underlay,
                nonce,
                capabilities,
                auth_tag,
            ) =>
        {
            path_id
        }
        _ => return Err(RuntimeError::Protocol("invalid UDP PATH_JOIN")),
    };
    encrypted.send_frame_to(&Frame::SessionReady, peer).await?;
    encrypted
        .send_frame_to(
            &Frame::PathStatus {
                path_id,
                status: crate::protocol::PathStatus::Active,
                capabilities: PathCapabilities::default(),
            },
            peer,
        )
        .await?;

    let mut flows: Vec<ServerUdpDatagramFlow> = Vec::new();
    loop {
        match recv_udp_frame_from_peer(&mut encrypted, &mut buffer, peer).await? {
            Frame::OpenDatagramFlow {
                flow_id, target, ..
            } => {
                if flows.iter().any(|flow| flow.flow_id == flow_id) {
                    return Err(RuntimeError::Protocol("duplicate UDP datagram flow"));
                }
                outbound::validate_target(&target)?;
                context.outbound.ensure_supports(TargetProtocol::Udp)?;
                let outbound_socket = match outbound::connect_udp(
                    &context.outbound,
                    &target,
                    Duration::from_secs(10),
                )
                .await
                {
                    Ok(socket) => socket,
                    Err(err) => {
                        encrypted
                            .send_frame_to(&Frame::DatagramClose { flow_id }, peer)
                            .await?;
                        return Err(RuntimeError::OutboundConnect(err));
                    }
                };
                flows.push(ServerUdpDatagramFlow {
                    flow_id,
                    outbound_socket,
                });
            }
            Frame::DatagramData {
                flow_id,
                ttl_ms,
                payload,
                ..
            } => {
                if ttl_ms == 0 {
                    return Err(RuntimeError::Protocol("expired datagram received"));
                }
                let flow = flows
                    .iter_mut()
                    .find(|flow| flow.flow_id == flow_id)
                    .ok_or(RuntimeError::Protocol("unknown UDP datagram flow"))?;
                flow.outbound_socket.send(&payload).await?;
                let mut response = vec![0u8; context.mux_limits.max_payload_bytes.min(64 * 1024)];
                let len = tokio::time::timeout(
                    Duration::from_secs(1),
                    flow.outbound_socket.recv(&mut response),
                )
                .await
                .map_err(|_| RuntimeError::Protocol("UDP outbound response timed out"))??;
                response.truncate(len);
                let mut response_flow = DatagramFlow::new(flow_id, context.mux_limits);
                response_flow.enqueue(0, ttl_ms, Bytes::from(response))?;
                let frame = response_flow
                    .pop_frame(0)
                    .ok_or(RuntimeError::Protocol("UDP response expired before send"))?;
                encrypted.send_frame_to(&frame, peer).await?;
            }
            Frame::DatagramClose { flow_id } => {
                flows.retain(|flow| flow.flow_id != flow_id);
                if flows.is_empty() {
                    return Ok(());
                }
            }
            Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
            _ => return Err(RuntimeError::Protocol("unexpected UDP datagram path frame")),
        }
    }
}

async fn recv_udp_frame_from_peer(
    encrypted: &mut EncryptedUdpSocket,
    buffer: &mut [u8],
    expected_peer: SocketAddr,
) -> Result<Frame, RuntimeError> {
    let (frame, peer) = encrypted.recv_frame_from(buffer).await?;
    if peer != expected_peer {
        return Err(RuntimeError::Protocol(
            "UDP frame arrived from unexpected peer",
        ));
    }
    Ok(frame)
}

async fn read_socks5_auth<S>(stream: &mut S) -> Result<socks5::AuthRequest, RuntimeError>
where
    S: AsyncRead + Unpin,
{
    let mut prefix = [0u8; 2];
    stream.read_exact(&mut prefix).await?;
    let method_count = prefix[1] as usize;
    let mut request = Vec::with_capacity(2 + method_count);
    request.extend_from_slice(&prefix);
    request.resize(2 + method_count, 0);
    stream.read_exact(&mut request[2..]).await?;
    let (auth, consumed) = socks5::parse_auth_request(&request)?;
    if consumed != request.len() {
        return Err(RuntimeError::Protocol("trailing SOCKS5 auth bytes"));
    }
    Ok(auth)
}

async fn read_socks5_command<S>(stream: &mut S) -> Result<socks5::CommandRequest, RuntimeError>
where
    S: AsyncRead + Unpin,
{
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).await?;
    let remaining = match prefix[3] {
        0x01 => 4 + 2,
        0x04 => 16 + 2,
        0x03 => {
            let mut len = [0u8; 1];
            stream.read_exact(&mut len).await?;
            let host_len = len[0] as usize;
            let mut request = Vec::with_capacity(5 + host_len + 2);
            request.extend_from_slice(&prefix);
            request.push(len[0]);
            request.resize(5 + host_len + 2, 0);
            stream.read_exact(&mut request[5..]).await?;
            let (command, consumed) = socks5::parse_command_request(&request)?;
            if consumed != request.len() {
                return Err(RuntimeError::Protocol("trailing SOCKS5 command bytes"));
            }
            return Ok(command);
        }
        _ => {
            return Err(RuntimeError::Socks5(Socks5Error::UnsupportedAddressType(
                prefix[3],
            )));
        }
    };
    let mut request = Vec::with_capacity(4 + remaining);
    request.extend_from_slice(&prefix);
    request.resize(4 + remaining, 0);
    stream.read_exact(&mut request[4..]).await?;
    let (command, consumed) = socks5::parse_command_request(&request)?;
    if consumed != request.len() {
        return Err(RuntimeError::Protocol("trailing SOCKS5 command bytes"));
    }
    Ok(command)
}

async fn read_http_connect<S>(stream: &mut S) -> Result<http_connect::ConnectRequest, RuntimeError>
where
    S: AsyncRead + Unpin,
{
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        if buf.len() >= MAX_HTTP_CONNECT_HEADER_BYTES {
            return Err(RuntimeError::HttpConnect(HttpConnectError::HeaderTooLarge));
        }
        stream.read_exact(&mut byte).await?;
        buf.push(byte[0]);
        if buf.ends_with(b"\r\n\r\n") {
            return Ok(http_connect::parse_connect_request(&buf)?);
        }
    }
}

fn first_tcp_path(paths: &[PathSpec]) -> Result<&PathSpec, RuntimeError> {
    paths
        .iter()
        .find(|path| path.underlay == UnderlayProtocol::Tcp)
        .ok_or(RuntimeError::NoTcpPath)
}

fn first_udp_path(paths: &[PathSpec]) -> Option<&PathSpec> {
    paths
        .iter()
        .find(|path| path.underlay == UnderlayProtocol::Udp)
}

fn random_session_id() -> Result<SessionId, RuntimeError> {
    let mut bytes = [0u8; 8];
    getrandom::getrandom(&mut bytes).map_err(RuntimeError::Random)?;
    Ok(SessionId(u64::from_be_bytes(bytes)))
}

fn random_nonce() -> Result<AuthNonce, RuntimeError> {
    let mut bytes = [0u8; 16];
    getrandom::getrandom(&mut bytes).map_err(RuntimeError::Random)?;
    Ok(AuthNonce(bytes))
}

#[derive(Debug)]
pub enum RuntimeError {
    Io(std::io::Error),
    Tcp(TcpTransportError),
    Udp(UdpTransportError),
    Encrypted(EncryptedFramedTransportError),
    EncryptedUdp(EncryptedUdpTransportError),
    Auth(AuthError),
    Random(getrandom::Error),
    Socks5(Socks5Error),
    HttpConnect(HttpConnectError),
    Outbound(outbound::OutboundError),
    OutboundConnect(outbound::OutboundConnectError),
    Stream(StreamError),
    Datagram(DatagramError),
    PathSpec(PathSpecParseError),
    NoTcpPath,
    NoUdpPath,
    UnsupportedIngress(&'static str),
    RemoteReset(ResetReason),
    RemoteClosed(CloseReason),
    Protocol(&'static str),
}

impl From<std::io::Error> for RuntimeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<TcpTransportError> for RuntimeError {
    fn from(value: TcpTransportError) -> Self {
        Self::Tcp(value)
    }
}

impl From<UdpTransportError> for RuntimeError {
    fn from(value: UdpTransportError) -> Self {
        Self::Udp(value)
    }
}

impl From<EncryptedFramedTransportError> for RuntimeError {
    fn from(value: EncryptedFramedTransportError) -> Self {
        Self::Encrypted(value)
    }
}

impl From<EncryptedUdpTransportError> for RuntimeError {
    fn from(value: EncryptedUdpTransportError) -> Self {
        Self::EncryptedUdp(value)
    }
}

impl From<AuthError> for RuntimeError {
    fn from(value: AuthError) -> Self {
        Self::Auth(value)
    }
}

impl From<Socks5Error> for RuntimeError {
    fn from(value: Socks5Error) -> Self {
        Self::Socks5(value)
    }
}

impl From<HttpConnectError> for RuntimeError {
    fn from(value: HttpConnectError) -> Self {
        Self::HttpConnect(value)
    }
}

impl From<outbound::OutboundError> for RuntimeError {
    fn from(value: outbound::OutboundError) -> Self {
        Self::Outbound(value)
    }
}

impl From<outbound::OutboundConnectError> for RuntimeError {
    fn from(value: outbound::OutboundConnectError) -> Self {
        Self::OutboundConnect(value)
    }
}

impl From<StreamError> for RuntimeError {
    fn from(value: StreamError) -> Self {
        Self::Stream(value)
    }
}

impl From<DatagramError> for RuntimeError {
    fn from(value: DatagramError) -> Self {
        Self::Datagram(value)
    }
}

impl From<PathSpecParseError> for RuntimeError {
    fn from(value: PathSpecParseError) -> Self {
        Self::PathSpec(value)
    }
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Tcp(err) => write!(f, "{err}"),
            Self::Udp(err) => write!(f, "{err}"),
            Self::Encrypted(err) => write!(f, "{err}"),
            Self::EncryptedUdp(err) => write!(f, "{err}"),
            Self::Auth(err) => write!(f, "{err}"),
            Self::Random(err) => write!(f, "random source failed: {err}"),
            Self::Socks5(err) => write!(f, "{err}"),
            Self::HttpConnect(err) => write!(f, "{err}"),
            Self::Outbound(err) => write!(f, "{err}"),
            Self::OutboundConnect(err) => write!(f, "{err}"),
            Self::Stream(err) => write!(f, "{err}"),
            Self::Datagram(err) => write!(f, "{err}"),
            Self::PathSpec(err) => write!(f, "{err}"),
            Self::NoTcpPath => write!(f, "runtime operation requires at least one TCP path"),
            Self::NoUdpPath => write!(f, "runtime operation requires at least one UDP path"),
            Self::UnsupportedIngress(ingress) => {
                write!(f, "{ingress} runtime is not implemented yet")
            }
            Self::RemoteReset(reason) => write!(f, "remote reset stream: {reason:?}"),
            Self::RemoteClosed(reason) => write!(f, "remote closed session: {reason:?}"),
            Self::Protocol(message) => write!(f, "protocol error: {message}"),
        }
    }
}

impl std::error::Error for RuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::Tcp(err) => Some(err),
            Self::Udp(err) => Some(err),
            Self::Encrypted(err) => Some(err),
            Self::EncryptedUdp(err) => Some(err),
            Self::Auth(err) => Some(err),
            Self::Random(_) => None,
            Self::Socks5(err) => Some(err),
            Self::HttpConnect(err) => Some(err),
            Self::Outbound(err) => Some(err),
            Self::OutboundConnect(err) => Some(err),
            Self::Stream(err) => Some(err),
            Self::Datagram(err) => Some(err),
            Self::PathSpec(err) => Some(err),
            Self::NoTcpPath
            | Self::NoUdpPath
            | Self::UnsupportedIngress(_)
            | Self::RemoteReset(_)
            | Self::RemoteClosed(_)
            | Self::Protocol(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SharedSecret;
    use crate::transport::tcp::bind_listener;
    use tokio::io::duplex;

    fn security() -> SecurityConfig {
        SecurityConfig::encrypted(SharedSecret::new(b"0123456789abcdef".to_vec()).expect("secret"))
    }

    async fn reserve_tcp_path() -> PathSpec {
        let probe = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("reserve port");
        let port = probe.local_addr().expect("reserved addr").port();
        drop(probe);
        format!("tcp://127.0.0.1:{port}").parse().expect("path")
    }

    async fn spawn_echo_target() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("target bind");
        let addr = listener.local_addr().expect("target addr");
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("target accept");
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.expect("target read");
            assert_eq!(&buf, b"ping");
            stream.write_all(b"pong").await.expect("target write");
            stream.shutdown().await.expect("target shutdown");
        });
        (addr, handle)
    }

    async fn spawn_udp_echo_target() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        spawn_udp_echo_target_count(1).await
    }

    async fn spawn_udp_echo_target_count(
        count: usize,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let socket = UdpSocket::bind("127.0.0.1:0").await.expect("target bind");
        let addr = socket.local_addr().expect("target addr");
        let handle = tokio::spawn(async move {
            let mut buf = [0u8; 16];
            for _ in 0..count {
                let (len, peer) = socket.recv_from(&mut buf).await.expect("target recv");
                assert_eq!(&buf[..len], b"ping");
                socket.send_to(b"pong", peer).await.expect("target send");
            }
        });
        (addr, handle)
    }

    async fn spawn_server_path(
        outbound: OutboundConfig,
    ) -> (PathSpec, tokio::task::JoinHandle<Result<(), RuntimeError>>) {
        let path = reserve_tcp_path().await;
        let listener = bind_listener(&path).await.expect("bind");
        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            handle_server_path(
                stream,
                ServerPathContext {
                    outbound,
                    codec_limits: CodecLimits::default(),
                    mux_limits: ResourceLimits::default().into(),
                    security: security(),
                },
            )
            .await
        });
        (path, handle)
    }

    async fn reserve_udp_path() -> PathSpec {
        let probe = UdpSocket::bind("127.0.0.1:0").await.expect("reserve port");
        let port = probe.local_addr().expect("reserved addr").port();
        drop(probe);
        format!("udp://127.0.0.1:{port}").parse().expect("path")
    }

    #[tokio::test]
    async fn socks5_ingress_relays_tcp_payload_over_encrypted_internal_stream() {
        let (target_addr, target) = spawn_echo_target().await;
        let (path, server_path) = spawn_server_path(OutboundConfig::Direct).await;
        let context =
            ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
        let (mut client, server) = duplex(4096);
        let handler = tokio::spawn(handle_socks5_client_stream(server, context));

        client
            .write_all(&[0x05, 0x01, 0x00])
            .await
            .expect("auth request");
        let mut auth_response = [0u8; 2];
        client
            .read_exact(&mut auth_response)
            .await
            .expect("auth response");
        assert_eq!(auth_response, [0x05, 0x00]);

        let mut connect = vec![0x05, 0x01, 0x00, 0x01];
        match target_addr {
            SocketAddr::V4(addr) => {
                connect.extend_from_slice(&addr.ip().octets());
                connect.extend_from_slice(&addr.port().to_be_bytes());
            }
            SocketAddr::V6(_) => panic!("expected IPv4 test target"),
        }
        client.write_all(&connect).await.expect("connect");

        let mut response = [0u8; 10];
        client.read_exact(&mut response).await.expect("reply");
        assert_eq!(response[1], Socks5Reply::Succeeded as u8);

        client.write_all(b"ping").await.expect("payload write");
        client.shutdown().await.expect("client shutdown");
        let mut payload = [0u8; 4];
        client.read_exact(&mut payload).await.expect("payload read");
        assert_eq!(&payload, b"pong");

        handler.await.expect("join").expect("handler");
        server_path
            .await
            .expect("server join")
            .expect("server path");
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn http_connect_ingress_relays_tcp_payload_over_encrypted_internal_stream() {
        let (target_addr, target) = spawn_echo_target().await;
        let (path, server_path) = spawn_server_path(OutboundConfig::Direct).await;
        let context =
            ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
        let (mut client, server) = duplex(4096);
        let handler = tokio::spawn(handle_http_connect_client_stream(server, context));

        client
            .write_all(
                format!("CONNECT {target_addr} HTTP/1.1\r\nHost: {target_addr}\r\n\r\n").as_bytes(),
            )
            .await
            .expect("request");
        let mut response = vec![0u8; http_connect::success_response().len()];
        client.read_exact(&mut response).await.expect("response");
        assert_eq!(response, http_connect::success_response());

        client.write_all(b"ping").await.expect("payload write");
        client.shutdown().await.expect("client shutdown");
        let mut payload = [0u8; 4];
        client.read_exact(&mut payload).await.expect("payload read");
        assert_eq!(&payload, b"pong");

        handler.await.expect("join").expect("handler");
        server_path
            .await
            .expect("server join")
            .expect("server path");
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn encrypted_udp_datagram_path_relays_direct_udp_target() {
        let (target_addr, target) = spawn_udp_echo_target().await;
        let path = reserve_udp_path().await;
        let socket = udp::bind_socket(&path).await.expect("bind udp path");
        let server = tokio::spawn(handle_server_udp_datagram_path_session(
            socket,
            ServerPathContext {
                outbound: OutboundConfig::Direct,
                codec_limits: CodecLimits::default(),
                mux_limits: ResourceLimits::default().into(),
                security: security(),
            },
        ));

        let response = client_udp_datagram_round_trip(
            &path,
            security(),
            ResourceLimits::default(),
            TargetAddr::Ip(target_addr),
            Bytes::from_static(b"ping"),
            1000,
        )
        .await
        .expect("round trip");

        assert_eq!(response, Bytes::from_static(b"pong"));
        server.await.expect("server join").expect("server");
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn server_runtime_binds_udp_path_and_relays_direct_udp_datagram() {
        let (target_addr, target) = spawn_udp_echo_target().await;
        let path = reserve_udp_path().await;
        let server = tokio::spawn(run_server(
            vec![path.clone()],
            OutboundConfig::Direct,
            security(),
            ResourceLimits::default(),
        ));
        tokio::time::sleep(Duration::from_millis(10)).await;

        let response = client_udp_datagram_round_trip(
            &path,
            security(),
            ResourceLimits::default(),
            TargetAddr::Ip(target_addr),
            Bytes::from_static(b"ping"),
            1000,
        )
        .await
        .expect("round trip");

        assert_eq!(response, Bytes::from_static(b"pong"));
        server.abort();
        let _ = server.await;
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn socks5_udp_associate_relays_datagram_over_encrypted_udp_path() {
        let (target_addr, target) = spawn_udp_echo_target_count(2).await;
        let path = reserve_udp_path().await;
        let socket = udp::bind_socket(&path).await.expect("bind udp path");
        let server = tokio::spawn(handle_server_udp_datagram_path_session(
            socket,
            ServerPathContext {
                outbound: OutboundConfig::Direct,
                codec_limits: CodecLimits::default(),
                mux_limits: ResourceLimits::default().into(),
                security: security(),
            },
        ));
        let context =
            ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
        let (mut control_client, control_server) = duplex(4096);
        let handler = tokio::spawn(handle_socks5_client_stream(control_server, context));

        control_client
            .write_all(&[0x05, 0x01, 0x00])
            .await
            .expect("auth request");
        let mut auth_response = [0u8; 2];
        control_client
            .read_exact(&mut auth_response)
            .await
            .expect("auth response");
        assert_eq!(auth_response, [0x05, 0x00]);

        control_client
            .write_all(&[0x05, 0x03, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
            .await
            .expect("udp associate");
        let mut associate_response = [0u8; 10];
        control_client
            .read_exact(&mut associate_response)
            .await
            .expect("associate response");
        assert_eq!(associate_response[0], 0x05);
        assert_eq!(associate_response[1], Socks5Reply::Succeeded as u8);
        assert_eq!(associate_response[3], 0x01);
        let relay_addr = SocketAddr::from((
            [
                associate_response[4],
                associate_response[5],
                associate_response[6],
                associate_response[7],
            ],
            u16::from_be_bytes([associate_response[8], associate_response[9]]),
        ));

        let udp_client = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("udp client bind");
        let request =
            socks5::udp_datagram(&TargetAddr::Ip(target_addr), b"ping").expect("udp request");
        for _ in 0..2 {
            udp_client
                .send_to(&request, relay_addr)
                .await
                .expect("send udp request");
            let mut response = [0u8; 128];
            let (len, _) = udp_client
                .recv_from(&mut response)
                .await
                .expect("recv udp response");
            let (datagram, consumed) =
                socks5::parse_udp_datagram(&response[..len]).expect("datagram");
            assert_eq!(consumed, len);
            assert_eq!(datagram.target, TargetAddr::Ip(target_addr));
            assert_eq!(datagram.payload, Bytes::from_static(b"pong"));
        }
        control_client.shutdown().await.expect("control shutdown");

        handler.await.expect("handler join").expect("handler");
        server.await.expect("server join").expect("server");
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn server_verifies_auth_sequence_and_rejects_wrong_secret() {
        let path = reserve_tcp_path().await;
        let listener = bind_listener(&path).await.expect("bind");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            handle_server_path(
                stream,
                ServerPathContext {
                    outbound: OutboundConfig::Direct,
                    codec_limits: CodecLimits::default(),
                    mux_limits: ResourceLimits::default().into(),
                    security: SecurityConfig::encrypted(
                        SharedSecret::new(b"fedcba9876543210".to_vec()).expect("secret"),
                    ),
                },
            )
            .await
        });

        let stream = tcp::connect_path(&path, TcpConnectOptions::default())
            .await
            .expect("connect");
        let mut client = EncryptedFramedStream::new(
            stream,
            b"0123456789abcdef",
            PeerRole::Client,
            CodecLimits::default(),
        );
        client
            .write_frame(&Frame::SessionHello {
                session_id: SessionId(1),
            })
            .await
            .expect("write");
        client.flush().await.expect("flush");

        assert!(matches!(
            server.await.expect("join"),
            Err(RuntimeError::Encrypted(
                EncryptedFramedTransportError::Crypto
            ))
        ));
    }
}
