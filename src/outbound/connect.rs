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
    /// Direct target dialing with the OS-selected source address.
    Direct,
    /// Direct target dialing with an operator-selected source IP.
    BindSourceIp(IpAddr),
    /// Upstream SOCKS5 proxy egress.
    Socks5 { proxy: Endpoint },
    /// Upstream HTTP CONNECT egress for TCP targets.
    HttpConnect { proxy: Endpoint },
    /// Upstream HTTP CONNECT-UDP egress for UDP targets.
    HttpConnectUdp { proxy: Endpoint },
    /// Egress balancer that tries members in configured order.
    Sequence { members: Vec<OutboundRouteMember> },
    /// Egress balancer that rotates the starting member per flow.
    Random { members: Vec<OutboundRouteMember> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundRouteMember {
    /// Leaf egress config. Nested route groups are rejected by the connector.
    pub config: Box<OutboundConfig>,
    /// DNS policy scoped to this exact egress member.
    pub dns: DnsConfig,
    /// Target connect timeout scoped to this exact egress member.
    pub connect_timeout: Duration,
}

impl OutboundConfig {
    pub fn supports_udp_targets(&self) -> bool {
        matches!(
            self,
            Self::Direct
                | Self::BindSourceIp(_)
                | Self::Socks5 { .. }
                | Self::HttpConnectUdp { .. }
        ) || match self {
            Self::Sequence { members } | Self::Random { members } => members
                .iter()
                .any(|member| member.config.supports_udp_targets()),
            _ => false,
        }
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
    NoRouteMembers,
    NestedRouteGroup,
    DomainTooLong,
    InvalidTargetPort,
}

impl std::fmt::Display for OutboundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UdpNotSupported => write!(f, "outbound policy does not support UDP targets"),
            Self::NoRouteMembers => write!(f, "outbound route group has no members"),
            Self::NestedRouteGroup => write!(f, "outbound route groups cannot be nested"),
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
    if let OutboundConfig::Sequence { members } = config {
        return connect_tcp_route_members(members, target, RouteMemberOrder::Sequence).await;
    }
    if let OutboundConfig::Random { members } = config {
        return connect_tcp_route_members(members, target, RouteMemberOrder::Random).await;
    }
    connect_tcp_leaf(config, dns, target, timeout).await
}

async fn connect_tcp_leaf(
    config: &OutboundConfig,
    dns: &DnsConfig,
    target: &TargetAddr,
    timeout: Duration,
) -> Result<TcpStream, OutboundConnectError> {
    match config {
        OutboundConfig::Direct => connect_direct_tcp(target, dns, None, timeout).await,
        OutboundConfig::BindSourceIp(ip) => {
            connect_direct_tcp(target, dns, Some(*ip), timeout).await
        }
        OutboundConfig::Socks5 { proxy } => connect_socks5_tcp(proxy, target, timeout).await,
        OutboundConfig::HttpConnect { proxy } | OutboundConfig::HttpConnectUdp { proxy } => {
            connect_http_connect_tcp(proxy, target, timeout).await
        }
        OutboundConfig::Sequence { .. } | OutboundConfig::Random { .. } => {
            Err(OutboundError::NestedRouteGroup.into())
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
    if let OutboundConfig::Sequence { members } = config {
        return connect_udp_route_members(members, target, RouteMemberOrder::Sequence).await;
    }
    if let OutboundConfig::Random { members } = config {
        return connect_udp_route_members(members, target, RouteMemberOrder::Random).await;
    }
    connect_udp_leaf(config, dns, target, timeout).await
}

async fn connect_udp_leaf(
    config: &OutboundConfig,
    dns: &DnsConfig,
    target: &TargetAddr,
    timeout: Duration,
) -> Result<OutboundUdpSocket, OutboundConnectError> {
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
        OutboundConfig::Sequence { .. } | OutboundConfig::Random { .. } => {
            Err(OutboundError::NestedRouteGroup.into())
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum RouteMemberOrder {
    Sequence,
    Random,
}

async fn connect_tcp_route_members(
    members: &[OutboundRouteMember],
    target: &TargetAddr,
    order: RouteMemberOrder,
) -> Result<TcpStream, OutboundConnectError> {
    let mut last_error = None;
    for index in route_member_indices(members.len(), order) {
        let member = &members[index];
        match connect_tcp_leaf(&member.config, &member.dns, target, member.connect_timeout).await {
            Ok(stream) => return Ok(stream),
            Err(err) => last_error = Some(err),
        }
    }
    Err(last_error.unwrap_or(OutboundError::NoRouteMembers.into()))
}

async fn connect_udp_route_members(
    members: &[OutboundRouteMember],
    target: &TargetAddr,
    order: RouteMemberOrder,
) -> Result<OutboundUdpSocket, OutboundConnectError> {
    let mut last_error = None;
    for index in route_member_indices(members.len(), order) {
        let member = &members[index];
        if !member.config.supports_udp_targets() {
            continue;
        }
        match connect_udp_leaf(&member.config, &member.dns, target, member.connect_timeout).await {
            Ok(socket) => return Ok(socket),
            Err(err) => last_error = Some(err),
        }
    }
    Err(last_error.unwrap_or(OutboundError::NoRouteMembers.into()))
}

fn route_member_indices(len: usize, order: RouteMemberOrder) -> Vec<usize> {
    if len == 0 {
        return Vec::new();
    }
    let start = match order {
        RouteMemberOrder::Sequence => 0,
        RouteMemberOrder::Random => random_start_index(len),
    };
    (0..len).map(|offset| (start + offset) % len).collect()
}

fn random_start_index(len: usize) -> usize {
    let mut bytes = [0u8; 8];
    if getrandom::getrandom(&mut bytes).is_err() {
        return 0;
    }
    (u64::from_le_bytes(bytes) as usize) % len
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
        match http_connect::read_datagram_capsule_into(&mut self.stream, buffer).await {
            Ok(len) => Ok(len),
            Err(http_connect::HttpConnectClientError::DatagramPayloadTooLarge {
                actual,
                limit,
            }) if limit == buffer.len() => {
                Err(OutboundConnectError::UdpReceiveBufferTooSmall { actual, limit })
            }
            Err(err) => Err(err.into()),
        }
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
        let datagram = socks5_udp::parse_udp_datagram_parts(&self.recv_buffer[..len])
            .map_err(OutboundConnectError::Socks5UdpPacket)?;
        if datagram.consumed != len {
            return Err(OutboundConnectError::InvalidProxyResponse);
        }
        if !socks5_udp_response_target_allowed(&self.target, &datagram.target) {
            return Err(OutboundConnectError::UdpRelayTargetMismatch {
                expected: self.target.clone(),
                actual: datagram.target,
            });
        }
        let payload = &self.recv_buffer[datagram.payload_offset..len];
        if payload.len() > buffer.len() {
            return Err(OutboundConnectError::UdpReceiveBufferTooSmall {
                actual: payload.len(),
                limit: buffer.len(),
            });
        }
        buffer[..payload.len()].copy_from_slice(payload);
        Ok(payload.len())
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
#[path = "connect_test.rs"]
mod tests;
