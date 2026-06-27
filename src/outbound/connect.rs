use super::{dns, http_connect, socks5};
use crate::ingress::socks5 as socks5_udp;
use crate::protocol::TargetAddr;
use crate::transport::Endpoint;
use crate::transport::tcp::{self, TcpConnectOptions, TcpTransportError};
use crate::transport::udp::{self, UdpConnectOptions, UdpTransportError};
use dns::DnsConfig;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};

const MAX_HTTP_CONNECT_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_SOCKS5_UDP_PACKET_BYTES: usize = 65_535;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundConfig {
    Direct,
    BindSourceIp(IpAddr),
    Socks5 { proxy: Endpoint },
    HttpConnect { proxy: Endpoint },
    HttpConnectUdp { proxy: Endpoint },
}

impl OutboundConfig {
    pub fn supports_udp_targets(&self) -> bool {
        matches!(
            self,
            Self::Direct
                | Self::BindSourceIp(_)
                | Self::Socks5 { .. }
                | Self::HttpConnectUdp { .. }
        )
    }

    pub fn ensure_supports(&self, target_protocol: TargetProtocol) -> Result<(), OutboundError> {
        match target_protocol {
            TargetProtocol::Tcp => Ok(()),
            TargetProtocol::Udp if self.supports_udp_targets() => Ok(()),
            TargetProtocol::Udp => Err(OutboundError::UdpNotSupported),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetProtocol {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundError {
    UdpNotSupported,
    DomainTooLong,
    InvalidTargetPort,
}

impl std::fmt::Display for OutboundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UdpNotSupported => write!(f, "outbound policy does not support UDP targets"),
            Self::DomainTooLong => write!(f, "target domain is too long"),
            Self::InvalidTargetPort => write!(f, "target port must be greater than zero"),
        }
    }
}

impl std::error::Error for OutboundError {}

pub fn validate_target(target: &TargetAddr) -> Result<(), OutboundError> {
    if target.port() == 0 {
        return Err(OutboundError::InvalidTargetPort);
    }
    if let TargetAddr::Domain { host, .. } = target
        && host.len() > u8::MAX as usize
    {
        return Err(OutboundError::DomainTooLong);
    }
    Ok(())
}

pub async fn connect_tcp(
    config: &OutboundConfig,
    dns: &DnsConfig,
    target: &TargetAddr,
    timeout: Duration,
) -> Result<TcpStream, OutboundConnectError> {
    config.ensure_supports(TargetProtocol::Tcp)?;
    validate_target(target)?;
    match config {
        OutboundConfig::Direct => connect_direct_tcp(target, dns, None, timeout).await,
        OutboundConfig::BindSourceIp(ip) => {
            connect_direct_tcp(target, dns, Some(*ip), timeout).await
        }
        OutboundConfig::Socks5 { proxy } => connect_socks5_tcp(proxy, target, timeout).await,
        OutboundConfig::HttpConnect { proxy } | OutboundConfig::HttpConnectUdp { proxy } => {
            connect_http_connect_tcp(proxy, target, timeout).await
        }
    }
}

pub async fn connect_udp(
    config: &OutboundConfig,
    dns: &DnsConfig,
    target: &TargetAddr,
    timeout: Duration,
) -> Result<OutboundUdpSocket, OutboundConnectError> {
    config.ensure_supports(TargetProtocol::Udp)?;
    validate_target(target)?;
    match config {
        OutboundConfig::Direct => connect_direct_udp(target, dns, None, timeout)
            .await
            .map(OutboundUdpSocket::Direct),
        OutboundConfig::BindSourceIp(ip) => connect_direct_udp(target, dns, Some(*ip), timeout)
            .await
            .map(OutboundUdpSocket::Direct),
        OutboundConfig::Socks5 { proxy } => connect_socks5_udp(proxy, target, timeout)
            .await
            .map(OutboundUdpSocket::Socks5),
        OutboundConfig::HttpConnectUdp { proxy } => {
            connect_http_connect_udp(proxy, target, timeout)
                .await
                .map(OutboundUdpSocket::HttpConnectUdp)
        }
        OutboundConfig::HttpConnect { .. } => Err(OutboundError::UdpNotSupported.into()),
    }
}

#[derive(Debug)]
pub enum OutboundUdpSocket {
    Direct(UdpSocket),
    Socks5(Socks5UdpAssociation),
    HttpConnectUdp(HttpConnectUdpAssociation),
}

impl OutboundUdpSocket {
    pub async fn send(&mut self, payload: &[u8]) -> Result<usize, OutboundConnectError> {
        match self {
            Self::Direct(socket) => Ok(socket.send(payload).await?),
            Self::Socks5(association) => association.send(payload).await,
            Self::HttpConnectUdp(association) => association.send(payload).await,
        }
    }

    pub async fn recv(&mut self, buffer: &mut [u8]) -> Result<usize, OutboundConnectError> {
        match self {
            Self::Direct(socket) => Ok(socket.recv(buffer).await?),
            Self::Socks5(association) => association.recv(buffer).await,
            Self::HttpConnectUdp(association) => association.recv(buffer).await,
        }
    }
}

#[derive(Debug)]
pub struct Socks5UdpAssociation {
    _control: TcpStream,
    relay: UdpSocket,
    target: TargetAddr,
    recv_buffer: Vec<u8>,
}

#[derive(Debug)]
pub struct HttpConnectUdpAssociation {
    stream: TcpStream,
}

impl HttpConnectUdpAssociation {
    async fn send(&mut self, payload: &[u8]) -> Result<usize, OutboundConnectError> {
        let capsule = http_connect::datagram_capsule(payload)?;
        self.stream.write_all(&capsule).await?;
        Ok(payload.len())
    }

    async fn recv(&mut self, buffer: &mut [u8]) -> Result<usize, OutboundConnectError> {
        let payload = http_connect::read_datagram_capsule(&mut self.stream).await?;
        if payload.len() > buffer.len() {
            return Err(OutboundConnectError::UdpReceiveBufferTooSmall {
                actual: payload.len(),
                limit: buffer.len(),
            });
        }
        buffer[..payload.len()].copy_from_slice(&payload);
        Ok(payload.len())
    }
}

impl Socks5UdpAssociation {
    async fn send(&mut self, payload: &[u8]) -> Result<usize, OutboundConnectError> {
        let packet = socks5_udp::udp_datagram(&self.target, payload)
            .map_err(OutboundConnectError::Socks5UdpPacket)?;
        self.relay.send(&packet).await?;
        Ok(payload.len())
    }

    async fn recv(&mut self, buffer: &mut [u8]) -> Result<usize, OutboundConnectError> {
        let len = self.relay.recv(&mut self.recv_buffer).await?;
        let (datagram, consumed) = socks5_udp::parse_udp_datagram(&self.recv_buffer[..len])
            .map_err(OutboundConnectError::Socks5UdpPacket)?;
        if consumed != len {
            return Err(OutboundConnectError::InvalidProxyResponse);
        }
        if !socks5_udp_response_target_allowed(&self.target, &datagram.target) {
            return Err(OutboundConnectError::UdpRelayTargetMismatch {
                expected: self.target.clone(),
                actual: datagram.target,
            });
        }
        if datagram.payload.len() > buffer.len() {
            return Err(OutboundConnectError::UdpReceiveBufferTooSmall {
                actual: datagram.payload.len(),
                limit: buffer.len(),
            });
        }
        buffer[..datagram.payload.len()].copy_from_slice(&datagram.payload);
        Ok(datagram.payload.len())
    }
}

async fn connect_direct_tcp(
    target: &TargetAddr,
    dns: &DnsConfig,
    source_ip: Option<IpAddr>,
    timeout: Duration,
) -> Result<TcpStream, OutboundConnectError> {
    let addrs = resolve_target(target, dns).await?;
    let mut last_error = None;
    for addr in addrs {
        if let Some(source_ip) = source_ip
            && source_ip.is_ipv4() != addr.is_ipv4()
        {
            continue;
        }
        match tcp::connect_addr(
            addr,
            TcpConnectOptions {
                source_ip,
                timeout,
                ..TcpConnectOptions::default()
            },
        )
        .await
        {
            Ok(stream) => return Ok(stream),
            Err(err) => last_error = Some(err),
        }
    }
    Err(last_error
        .unwrap_or(TcpTransportError::NoCompatibleAddress)
        .into())
}

async fn connect_direct_udp(
    target: &TargetAddr,
    dns: &DnsConfig,
    source_ip: Option<IpAddr>,
    timeout: Duration,
) -> Result<UdpSocket, OutboundConnectError> {
    let addrs = resolve_target(target, dns).await?;
    let mut last_error = None;
    for addr in addrs {
        if let Some(source_ip) = source_ip
            && source_ip.is_ipv4() != addr.is_ipv4()
        {
            continue;
        }
        match udp::connect_addr(addr, UdpConnectOptions { source_ip, timeout }).await {
            Ok(socket) => return Ok(socket),
            Err(err) => last_error = Some(err),
        }
    }
    Err(last_error
        .unwrap_or(UdpTransportError::NoCompatibleAddress)
        .into())
}

async fn connect_socks5_tcp(
    proxy: &Endpoint,
    target: &TargetAddr,
    timeout: Duration,
) -> Result<TcpStream, OutboundConnectError> {
    let mut stream = tcp::connect_endpoint(
        proxy,
        TcpConnectOptions {
            timeout,
            ..TcpConnectOptions::default()
        },
    )
    .await?;
    negotiate_socks5_no_auth(&mut stream).await?;
    let request = socks5::connect_request(target)?;
    stream.write_all(&request).await?;
    let reply = read_socks5_reply(&mut stream).await?;
    if reply.status != 0x00 {
        return Err(OutboundConnectError::ProxyRejected(reply.status as u16));
    }
    Ok(stream)
}

async fn connect_socks5_udp(
    proxy: &Endpoint,
    target: &TargetAddr,
    timeout: Duration,
) -> Result<Socks5UdpAssociation, OutboundConnectError> {
    let mut control = tcp::connect_endpoint(
        proxy,
        TcpConnectOptions {
            timeout,
            ..TcpConnectOptions::default()
        },
    )
    .await?;
    let control_peer = control.peer_addr()?;
    negotiate_socks5_no_auth(&mut control).await?;
    let request = socks5::udp_associate_request(socks5_udp_client_endpoint(control.local_addr()?))?;
    control.write_all(&request).await?;
    let reply = read_socks5_reply(&mut control).await?;
    if reply.status != 0x00 {
        return Err(OutboundConnectError::ProxyRejected(reply.status as u16));
    }
    let relay = relay_endpoint_from_socks5_bind(&reply.bind, control_peer)?;
    let relay = udp::connect_endpoint(
        &relay,
        UdpConnectOptions {
            timeout,
            ..UdpConnectOptions::default()
        },
    )
    .await?;
    Ok(Socks5UdpAssociation {
        _control: control,
        relay,
        target: target.clone(),
        recv_buffer: vec![0u8; MAX_SOCKS5_UDP_PACKET_BYTES],
    })
}

async fn connect_http_connect_tcp(
    proxy: &Endpoint,
    target: &TargetAddr,
    timeout: Duration,
) -> Result<TcpStream, OutboundConnectError> {
    let mut stream = tcp::connect_endpoint(
        proxy,
        TcpConnectOptions {
            timeout,
            ..TcpConnectOptions::default()
        },
    )
    .await?;
    let request = http_connect::connect_request(target, None)?;
    stream.write_all(&request).await?;
    let response = read_http_proxy_response(&mut stream).await?;
    let response = http_connect::parse_connect_response(&response)?;
    if response.status != 200 {
        return Err(OutboundConnectError::ProxyRejected(response.status));
    }
    Ok(stream)
}

async fn connect_http_connect_udp(
    proxy: &Endpoint,
    target: &TargetAddr,
    timeout: Duration,
) -> Result<HttpConnectUdpAssociation, OutboundConnectError> {
    let mut stream = tcp::connect_endpoint(
        proxy,
        TcpConnectOptions {
            timeout,
            ..TcpConnectOptions::default()
        },
    )
    .await?;
    let request = http_connect::connect_udp_request(proxy, target)?;
    stream.write_all(&request).await?;
    let response = read_http_proxy_response(&mut stream).await?;
    http_connect::parse_connect_udp_response(&response)?;
    Ok(HttpConnectUdpAssociation { stream })
}

async fn read_http_proxy_response(stream: &mut TcpStream) -> Result<Vec<u8>, OutboundConnectError> {
    let mut response = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        if response.len() >= MAX_HTTP_CONNECT_RESPONSE_BYTES {
            return Err(OutboundConnectError::InvalidProxyResponse);
        }
        stream.read_exact(&mut byte).await?;
        response.push(byte[0]);
        if response.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    Ok(response)
}

async fn resolve_target(
    target: &TargetAddr,
    dns: &DnsConfig,
) -> Result<Vec<SocketAddr>, OutboundConnectError> {
    match target {
        TargetAddr::Ip(addr) => Ok(vec![*addr]),
        TargetAddr::Domain { host, port } => Ok(dns::resolve_socket_addrs(host, *port, dns).await?),
    }
}

async fn negotiate_socks5_no_auth(stream: &mut TcpStream) -> Result<(), OutboundConnectError> {
    stream.write_all(&socks5::no_auth_greeting()).await?;
    let mut method = [0u8; 2];
    stream.read_exact(&mut method).await?;
    let method = socks5::parse_method_selection(&method)?;
    if method.method != 0x00 {
        return Err(OutboundConnectError::ProxyAuthRejected(method.method));
    }
    Ok(())
}

async fn read_socks5_reply(
    stream: &mut TcpStream,
) -> Result<socks5::Socks5ConnectReply, OutboundConnectError> {
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).await?;
    if prefix[0] != 0x05 || prefix[2] != 0x00 {
        return Err(OutboundConnectError::InvalidProxyResponse);
    }
    let mut reply = Vec::with_capacity(4 + 255 + 2);
    reply.extend_from_slice(&prefix);
    match prefix[3] {
        0x01 => {
            reply.resize(4 + 4 + 2, 0);
            stream.read_exact(&mut reply[4..]).await?;
        }
        0x04 => {
            reply.resize(4 + 16 + 2, 0);
            stream.read_exact(&mut reply[4..]).await?;
        }
        0x03 => {
            let mut len = [0u8; 1];
            stream.read_exact(&mut len).await?;
            reply.push(len[0]);
            let rest_len = len[0] as usize + 2;
            let start = reply.len();
            reply.resize(start + rest_len, 0);
            stream.read_exact(&mut reply[start..]).await?;
        }
        _ => return Err(OutboundConnectError::InvalidProxyResponse),
    }
    let parsed = socks5::parse_connect_reply(&reply)?;
    if parsed.consumed != reply.len() {
        return Err(OutboundConnectError::InvalidProxyResponse);
    }
    Ok(parsed)
}

fn socks5_udp_client_endpoint(control_local: SocketAddr) -> SocketAddr {
    let ip = if control_local.is_ipv4() {
        IpAddr::V4(Ipv4Addr::UNSPECIFIED)
    } else {
        IpAddr::V6(Ipv6Addr::UNSPECIFIED)
    };
    SocketAddr::new(ip, 0)
}

fn relay_endpoint_from_socks5_bind(
    bind: &TargetAddr,
    control_peer: SocketAddr,
) -> Result<Endpoint, OutboundConnectError> {
    match bind {
        TargetAddr::Domain { host, port } => Ok(Endpoint::new(host.clone(), *port)?),
        TargetAddr::Ip(addr) => {
            let ip = if addr.ip().is_unspecified() {
                control_peer.ip()
            } else {
                addr.ip()
            };
            Ok(Endpoint::new(ip.to_string(), addr.port())?)
        }
    }
}

fn socks5_udp_response_target_allowed(expected: &TargetAddr, actual: &TargetAddr) -> bool {
    match expected {
        TargetAddr::Ip(_) => actual == expected,
        TargetAddr::Domain { port, .. } => actual.port() == *port,
    }
}

#[derive(Debug)]
pub enum OutboundConnectError {
    Policy(OutboundError),
    Endpoint(crate::transport::EndpointParseError),
    Tcp(TcpTransportError),
    Udp(UdpTransportError),
    Io(std::io::Error),
    Dns(dns::DnsError),
    Socks5Client(socks5::Socks5ClientError),
    HttpConnectClient(http_connect::HttpConnectClientError),
    ProxyAuthRejected(u8),
    ProxyRejected(u16),
    Socks5UdpPacket(socks5_udp::Socks5Error),
    UdpRelayTargetMismatch {
        expected: TargetAddr,
        actual: TargetAddr,
    },
    UdpReceiveBufferTooSmall {
        actual: usize,
        limit: usize,
    },
    InvalidProxyResponse,
}

impl From<OutboundError> for OutboundConnectError {
    fn from(value: OutboundError) -> Self {
        Self::Policy(value)
    }
}

impl From<crate::transport::EndpointParseError> for OutboundConnectError {
    fn from(value: crate::transport::EndpointParseError) -> Self {
        Self::Endpoint(value)
    }
}

impl From<TcpTransportError> for OutboundConnectError {
    fn from(value: TcpTransportError) -> Self {
        Self::Tcp(value)
    }
}

impl From<UdpTransportError> for OutboundConnectError {
    fn from(value: UdpTransportError) -> Self {
        Self::Udp(value)
    }
}

impl From<std::io::Error> for OutboundConnectError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<dns::DnsError> for OutboundConnectError {
    fn from(value: dns::DnsError) -> Self {
        Self::Dns(value)
    }
}

impl From<socks5::Socks5ClientError> for OutboundConnectError {
    fn from(value: socks5::Socks5ClientError) -> Self {
        Self::Socks5Client(value)
    }
}

impl From<http_connect::HttpConnectClientError> for OutboundConnectError {
    fn from(value: http_connect::HttpConnectClientError) -> Self {
        Self::HttpConnectClient(value)
    }
}

impl std::fmt::Display for OutboundConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Policy(err) => write!(f, "{err}"),
            Self::Endpoint(err) => write!(f, "{err}"),
            Self::Tcp(err) => write!(f, "{err}"),
            Self::Udp(err) => write!(f, "{err}"),
            Self::Io(err) => write!(f, "{err}"),
            Self::Dns(err) => write!(f, "{err}"),
            Self::Socks5Client(err) => write!(f, "{err}"),
            Self::HttpConnectClient(err) => write!(f, "{err}"),
            Self::ProxyAuthRejected(method) => {
                write!(f, "SOCKS5 proxy selected unsupported auth method {method}")
            }
            Self::ProxyRejected(status) => {
                write!(f, "upstream proxy rejected CONNECT with {status}")
            }
            Self::Socks5UdpPacket(err) => write!(f, "{err}"),
            Self::UdpRelayTargetMismatch { expected, actual } => {
                write!(
                    f,
                    "SOCKS5 UDP relay returned packet for {}, expected {}",
                    actual.authority(),
                    expected.authority()
                )
            }
            Self::UdpReceiveBufferTooSmall { actual, limit } => {
                write!(
                    f,
                    "UDP receive buffer is too small: packet payload is {actual} bytes, buffer is {limit} bytes"
                )
            }
            Self::InvalidProxyResponse => write!(f, "invalid upstream proxy response"),
        }
    }
}

impl std::error::Error for OutboundConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Policy(err) => Some(err),
            Self::Endpoint(err) => Some(err),
            Self::Tcp(err) => Some(err),
            Self::Udp(err) => Some(err),
            Self::Io(err) => Some(err),
            Self::Dns(err) => Some(err),
            Self::Socks5Client(err) => Some(err),
            Self::HttpConnectClient(err) => Some(err),
            Self::Socks5UdpPacket(err) => Some(err),
            Self::ProxyAuthRejected(_)
            | Self::ProxyRejected(_)
            | Self::UdpRelayTargetMismatch { .. }
            | Self::UdpReceiveBufferTooSmall { .. }
            | Self::InvalidProxyResponse => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingress::socks5 as ingress_socks5;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, UdpSocket};

    #[test]
    fn outbound_support_matrix_matches_protocol_semantics() {
        assert!(
            OutboundConfig::Direct
                .ensure_supports(TargetProtocol::Tcp)
                .is_ok()
        );
        assert!(
            OutboundConfig::Direct
                .ensure_supports(TargetProtocol::Udp)
                .is_ok()
        );
        assert!(
            OutboundConfig::HttpConnect {
                proxy: "127.0.0.1:8080".parse().expect("proxy")
            }
            .ensure_supports(TargetProtocol::Tcp)
            .is_ok()
        );
        assert_eq!(
            OutboundConfig::HttpConnect {
                proxy: "127.0.0.1:8080".parse().expect("proxy")
            }
            .ensure_supports(TargetProtocol::Udp),
            Err(OutboundError::UdpNotSupported)
        );
        assert!(
            OutboundConfig::HttpConnectUdp {
                proxy: "127.0.0.1:8080".parse().expect("proxy")
            }
            .ensure_supports(TargetProtocol::Tcp)
            .is_ok()
        );
        assert!(
            OutboundConfig::HttpConnectUdp {
                proxy: "127.0.0.1:8080".parse().expect("proxy")
            }
            .ensure_supports(TargetProtocol::Udp)
            .is_ok()
        );
    }

    #[tokio::test]
    async fn direct_tcp_outbound_connects_to_target() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.expect("read");
            assert_eq!(&buf, b"ping");
            stream.write_all(b"pong").await.expect("write");
        });

        let mut stream = connect_tcp(
            &OutboundConfig::Direct,
            &DnsConfig::default(),
            &TargetAddr::Ip(addr),
            Duration::from_secs(1),
        )
        .await
        .expect("connect");
        stream.write_all(b"ping").await.expect("write");
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.expect("read");

        assert_eq!(&buf, b"pong");
        server.await.expect("server");
    }

    #[tokio::test]
    async fn direct_udp_outbound_connects_to_target() {
        let target = UdpSocket::bind("127.0.0.1:0").await.expect("target");
        let target_addr = target.local_addr().expect("target addr");
        let server = tokio::spawn(async move {
            let mut buf = [0u8; 16];
            let (len, peer) = target.recv_from(&mut buf).await.expect("recv");
            assert_eq!(&buf[..len], b"ping");
            target.send_to(b"pong", peer).await.expect("send");
        });

        let mut socket = connect_udp(
            &OutboundConfig::Direct,
            &DnsConfig::default(),
            &TargetAddr::Ip(target_addr),
            Duration::from_secs(1),
        )
        .await
        .expect("connect");
        socket.send(b"ping").await.expect("send");
        let mut buf = [0u8; 16];
        let len = socket.recv(&mut buf).await.expect("recv");

        assert_eq!(&buf[..len], b"pong");
        server.await.expect("server");
    }

    #[tokio::test]
    async fn socks5_udp_outbound_builds_udp_association() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let proxy: Endpoint = listener
            .local_addr()
            .expect("addr")
            .to_string()
            .parse()
            .expect("proxy");
        let target = TargetAddr::Domain {
            host: "example.com".to_string(),
            port: 53,
        };
        let expected_target = target.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut greeting = [0u8; 3];
            stream.read_exact(&mut greeting).await.expect("greeting");
            assert_eq!(greeting, socks5::no_auth_greeting());
            stream.write_all(&[0x05, 0x00]).await.expect("method");

            let mut request = [0u8; 10];
            stream.read_exact(&mut request).await.expect("request");
            assert_eq!(
                request.as_slice(),
                socks5::udp_associate_request("0.0.0.0:0".parse().expect("addr"))
                    .expect("expected request")
            );

            let relay = UdpSocket::bind("127.0.0.1:0").await.expect("relay bind");
            let relay_addr = relay.local_addr().expect("relay addr");
            stream
                .write_all(&ingress_socks5::connect_reply(
                    ingress_socks5::Socks5Reply::Succeeded,
                    relay_addr,
                ))
                .await
                .expect("reply");

            let mut packet = [0u8; 512];
            let (len, peer) = relay.recv_from(&mut packet).await.expect("relay recv");
            let (datagram, consumed) =
                ingress_socks5::parse_udp_datagram(&packet[..len]).expect("udp packet");
            assert_eq!(consumed, len);
            assert_eq!(datagram.target, expected_target);
            assert_eq!(&datagram.payload[..], b"ping");

            let response_target = TargetAddr::Ip("127.0.0.1:53".parse().expect("response target"));
            let response =
                ingress_socks5::udp_datagram(&response_target, b"pong").expect("response packet");
            relay.send_to(&response, peer).await.expect("relay send");
        });

        let mut socket = connect_udp(
            &OutboundConfig::Socks5 { proxy },
            &DnsConfig::default(),
            &target,
            Duration::from_secs(1),
        )
        .await
        .expect("connect");
        socket.send(b"ping").await.expect("send");
        let mut buf = [0u8; 16];
        let len = socket.recv(&mut buf).await.expect("recv");

        assert_eq!(&buf[..len], b"pong");
        server.await.expect("server");
    }

    #[tokio::test]
    async fn socks5_tcp_outbound_builds_connect_tunnel() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let proxy: Endpoint = listener
            .local_addr()
            .expect("addr")
            .to_string()
            .parse()
            .expect("proxy");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut greeting = [0u8; 3];
            stream.read_exact(&mut greeting).await.expect("greeting");
            assert_eq!(greeting, socks5::no_auth_greeting());
            stream.write_all(&[0x05, 0x00]).await.expect("method");

            let mut request = vec![0u8; 5 + 11 + 2];
            stream.read_exact(&mut request).await.expect("request");
            assert_eq!(
                request,
                socks5::connect_request(&TargetAddr::Domain {
                    host: "example.com".to_string(),
                    port: 443,
                })
                .expect("expected request")
            );
            stream
                .write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0])
                .await
                .expect("reply");

            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.expect("payload read");
            assert_eq!(&buf, b"ping");
            stream.write_all(b"pong").await.expect("payload write");
        });

        let mut stream = connect_tcp(
            &OutboundConfig::Socks5 { proxy },
            &DnsConfig::default(),
            &TargetAddr::Domain {
                host: "example.com".to_string(),
                port: 443,
            },
            Duration::from_secs(1),
        )
        .await
        .expect("connect");
        stream.write_all(b"ping").await.expect("payload write");
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.expect("payload read");

        assert_eq!(&buf, b"pong");
        server.await.expect("server");
    }

    #[tokio::test]
    async fn http_connect_tcp_outbound_builds_connect_tunnel() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let proxy: Endpoint = listener
            .local_addr()
            .expect("addr")
            .to_string()
            .parse()
            .expect("proxy");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            loop {
                stream.read_exact(&mut byte).await.expect("request byte");
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            assert_eq!(
                request,
                http_connect::connect_request(
                    &TargetAddr::Domain {
                        host: "example.com".to_string(),
                        port: 443,
                    },
                    None,
                )
                .expect("expected request")
            );
            stream
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .expect("reply");

            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.expect("payload read");
            assert_eq!(&buf, b"ping");
            stream.write_all(b"pong").await.expect("payload write");
        });

        let mut stream = connect_tcp(
            &OutboundConfig::HttpConnect { proxy },
            &DnsConfig::default(),
            &TargetAddr::Domain {
                host: "example.com".to_string(),
                port: 443,
            },
            Duration::from_secs(1),
        )
        .await
        .expect("connect");
        stream.write_all(b"ping").await.expect("payload write");
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.expect("payload read");

        assert_eq!(&buf, b"pong");
        server.await.expect("server");
    }

    #[tokio::test]
    async fn http_connect_udp_outbound_builds_capsule_tunnel() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let proxy: Endpoint = listener
            .local_addr()
            .expect("addr")
            .to_string()
            .parse()
            .expect("proxy");
        let target = TargetAddr::Domain {
            host: "example.com".to_string(),
            port: 443,
        };
        let expected_request = http_connect::connect_udp_request(&proxy, &target).expect("request");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            loop {
                stream.read_exact(&mut byte).await.expect("request byte");
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            assert_eq!(request, expected_request);
            stream
                .write_all(
                    b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: connect-udp\r\nCapsule-Protocol: ?1\r\n\r\n",
                )
                .await
                .expect("reply");

            let payload = http_connect::read_datagram_capsule(&mut stream)
                .await
                .expect("capsule");
            assert_eq!(&payload, b"ping");
            let response = http_connect::datagram_capsule(b"pong").expect("response");
            stream.write_all(&response).await.expect("response write");
        });

        let mut socket = connect_udp(
            &OutboundConfig::HttpConnectUdp { proxy },
            &DnsConfig::default(),
            &target,
            Duration::from_secs(1),
        )
        .await
        .expect("connect");
        socket.send(b"ping").await.expect("send");
        let mut buf = [0u8; 16];
        let len = socket.recv(&mut buf).await.expect("recv");

        assert_eq!(&buf[..len], b"pong");
        server.await.expect("server");
    }
}
