use super::{
    destination::{DestinationAuthorization, DestinationAuthorizationError, DestinationAuthorizer},
    http_connect, socks5,
};
use crate::dns::{DnsGeneration, DnsRuntimeError};
use crate::ingress::socks5 as socks5_udp;
use crate::product::{AuthorizedDomainTarget, AuthorizedTarget, DnsPlanId, Network};
use crate::protocol::TargetAddr;
use crate::transport::Endpoint;
use crate::transport::tcp::{self, TcpConnectOptions, TcpTransportError};
use crate::transport::udp::{self, UdpConnectOptions, UdpTransportError};
use crate::transport::{
    NativeEgressPurpose, NativeSocketConfigurator, SystemNativeSocketConfigurator,
};
use rustls::pki_types::{CertificateDer, ServerName};
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpStream, UdpSocket};
use tokio_rustls::client::TlsStream;

const MAX_HTTP_CONNECT_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_SOCKS5_UDP_PACKET_BYTES: usize = 65_535;

#[derive(Clone, PartialEq, Eq)]
pub struct ProxyCredentials {
    username: String,
    password: String,
}

impl ProxyCredentials {
    pub fn new(username: String, password: String) -> Result<Self, OutboundError> {
        if username.is_empty() {
            return Err(OutboundError::ProxyUsernameEmpty);
        }
        if password.is_empty() {
            return Err(OutboundError::ProxyPasswordEmpty);
        }
        if username.len() > u8::MAX as usize {
            return Err(OutboundError::ProxyUsernameTooLong);
        }
        if password.len() > u8::MAX as usize {
            return Err(OutboundError::ProxyPasswordTooLong);
        }
        if username.contains(':') || username.bytes().any(|byte| byte.is_ascii_control()) {
            return Err(OutboundError::InvalidProxyUsername);
        }
        if password.bytes().any(|byte| byte.is_ascii_control()) {
            return Err(OutboundError::InvalidProxyPassword);
        }
        Ok(Self { username, password })
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub(crate) fn password(&self) -> &str {
        &self.password
    }
}

impl std::fmt::Debug for ProxyCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyCredentials")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyConfig {
    endpoint: Endpoint,
    credentials: Option<ProxyCredentials>,
}

impl ProxyConfig {
    pub fn new(endpoint: Endpoint, credentials: Option<ProxyCredentials>) -> Self {
        Self {
            endpoint,
            credentials,
        }
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub fn credentials(&self) -> Option<&ProxyCredentials> {
        self.credentials.as_ref()
    }
}

#[derive(Clone)]
pub struct HttpsProxyConfig {
    proxy: ProxyConfig,
    tls_server_name: String,
    extra_root_certificates: Vec<CertificateDer<'static>>,
    tls_config: Arc<rustls::ClientConfig>,
}

impl HttpsProxyConfig {
    pub fn new(
        proxy: ProxyConfig,
        tls_server_name: Option<String>,
        extra_root_certificates: Vec<CertificateDer<'static>>,
    ) -> Result<Self, OutboundError> {
        let tls_server_name = tls_server_name.unwrap_or_else(|| proxy.endpoint.host.clone());
        ServerName::try_from(tls_server_name.clone())
            .map_err(|_| OutboundError::InvalidTlsServerName)?;
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        for certificate in &extra_root_certificates {
            roots
                .add(certificate.clone())
                .map_err(|_| OutboundError::InvalidTlsRootCertificate)?;
        }
        let mut tls_config = rustls::ClientConfig::builder_with_protocol_versions(&[
            &rustls::version::TLS13,
            &rustls::version::TLS12,
        ])
        .with_root_certificates(roots)
        .with_no_client_auth();
        tls_config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Self {
            proxy,
            tls_server_name,
            extra_root_certificates,
            tls_config: Arc::new(tls_config),
        })
    }

    pub fn proxy(&self) -> &ProxyConfig {
        &self.proxy
    }

    pub fn tls_server_name(&self) -> &str {
        &self.tls_server_name
    }
}

impl std::fmt::Debug for HttpsProxyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpsProxyConfig")
            .field("proxy", &self.proxy)
            .field("tls_server_name", &self.tls_server_name)
            .field(
                "extra_root_certificates",
                &self.extra_root_certificates.len(),
            )
            .finish()
    }
}

impl PartialEq for HttpsProxyConfig {
    fn eq(&self, other: &Self) -> bool {
        self.proxy == other.proxy
            && self.tls_server_name == other.tls_server_name
            && self.extra_root_certificates == other.extra_root_certificates
    }
}

impl Eq for HttpsProxyConfig {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundConfig {
    /// Direct target dialing with the OS-selected source address.
    Direct,
    /// Direct target dialing with an operator-selected source IP.
    BindSourceIp(IpAddr),
    /// Direct target dialing with independently selected source addresses for
    /// each explicitly enabled IP family.
    BindSourceIps {
        ipv4: Option<Ipv4Addr>,
        ipv6: Option<Ipv6Addr>,
    },
    /// Upstream SOCKS5 proxy egress.
    Socks5(ProxyConfig),
    /// Upstream HTTP CONNECT egress for TCP targets.
    HttpConnect(ProxyConfig),
    /// Upstream HTTP CONNECT carried over WebPKI-authenticated TLS.
    HttpsConnect(Box<HttpsProxyConfig>),
}

impl OutboundConfig {
    pub const fn supports_tcp_targets(&self) -> bool {
        true
    }

    pub fn supports_udp_targets(&self) -> bool {
        matches!(
            self,
            Self::Direct | Self::BindSourceIp(_) | Self::BindSourceIps { .. } | Self::Socks5(_)
        )
    }

    pub fn ensure_supports(&self, target_protocol: TargetProtocol) -> Result<(), OutboundError> {
        match target_protocol {
            TargetProtocol::Tcp => Ok(()),
            TargetProtocol::Udp if self.supports_udp_targets() => Ok(()),
            TargetProtocol::Udp => Err(OutboundError::UdpNotSupported),
        }
    }

    /// Native target sockets need an IP target. Proxy protocols can carry a
    /// domain in their own request and delegate target resolution upstream.
    ///
    /// Resolution for an IP-only leaf uses the selected Product DNS policy; its
    /// upstream transport may be system, direct, or routed.
    pub const fn requires_ip_target(&self) -> bool {
        matches!(
            self,
            Self::Direct | Self::BindSourceIp(_) | Self::BindSourceIps { .. }
        )
    }

    pub(crate) const fn source_binding_for(&self, remote: IpAddr) -> DirectSourceBinding {
        match (self, remote) {
            (Self::Direct, _) => DirectSourceBinding::Default,
            (Self::BindSourceIp(source), remote) if source.is_ipv4() == remote.is_ipv4() => {
                DirectSourceBinding::Bound(*source)
            }
            (Self::BindSourceIp(_), _) => DirectSourceBinding::Ineligible,
            (
                Self::BindSourceIps {
                    ipv4: Some(source), ..
                },
                IpAddr::V4(_),
            ) => DirectSourceBinding::Bound(IpAddr::V4(*source)),
            (
                Self::BindSourceIps {
                    ipv6: Some(source), ..
                },
                IpAddr::V6(_),
            ) => DirectSourceBinding::Bound(IpAddr::V6(*source)),
            (Self::BindSourceIps { .. }, _) => DirectSourceBinding::Ineligible,
            (Self::Socks5(_) | Self::HttpConnect(_) | Self::HttpsConnect(_), _) => {
                DirectSourceBinding::Default
            }
        }
    }

    pub(crate) const fn supports_ip_family(&self, remote: IpAddr) -> bool {
        !matches!(
            self.source_binding_for(remote),
            DirectSourceBinding::Ineligible
        )
    }

    /// Returns the process-owned native proxy control endpoint, when this leaf
    /// uses one. Runtime lifecycle code can pre-resolve this inventory before
    /// publishing full-tunnel policy; target endpoints remain flow-scoped.
    pub fn native_proxy_endpoint(&self) -> Option<&Endpoint> {
        match self {
            Self::Socks5(proxy) | Self::HttpConnect(proxy) => Some(proxy.endpoint()),
            Self::HttpsConnect(proxy) => Some(proxy.proxy().endpoint()),
            Self::Direct | Self::BindSourceIp(_) | Self::BindSourceIps { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirectSourceBinding {
    Ineligible,
    Default,
    Bound(IpAddr),
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
    InvalidDomain,
    InvalidTargetPort,
    ProxyUsernameEmpty,
    ProxyPasswordEmpty,
    ProxyUsernameTooLong,
    ProxyPasswordTooLong,
    InvalidProxyUsername,
    InvalidProxyPassword,
    InvalidTlsServerName,
    InvalidTlsRootCertificate,
    InvalidProxyHeaderValue,
    ProxyRequestTooLarge,
}

impl std::fmt::Display for OutboundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UdpNotSupported => write!(f, "outbound policy does not support UDP targets"),
            Self::DomainTooLong => write!(f, "target domain is too long"),
            Self::InvalidDomain => write!(f, "target domain contains an invalid character"),
            Self::InvalidTargetPort => write!(f, "target port must be greater than zero"),
            Self::ProxyUsernameEmpty => write!(f, "upstream proxy username must not be empty"),
            Self::ProxyPasswordEmpty => write!(f, "upstream proxy password must not be empty"),
            Self::ProxyUsernameTooLong => {
                write!(f, "upstream proxy username must fit in 255 bytes")
            }
            Self::ProxyPasswordTooLong => {
                write!(f, "upstream proxy password must fit in 255 bytes")
            }
            Self::InvalidProxyUsername => {
                write!(f, "upstream proxy username contains an invalid character")
            }
            Self::InvalidProxyPassword => {
                write!(f, "upstream proxy password contains an invalid character")
            }
            Self::InvalidTlsServerName => write!(f, "HTTPS proxy TLS server name is invalid"),
            Self::InvalidTlsRootCertificate => {
                write!(f, "HTTPS proxy TLS root certificate is invalid")
            }
            Self::InvalidProxyHeaderValue => {
                write!(f, "upstream proxy request contains an invalid header value")
            }
            Self::ProxyRequestTooLarge => {
                write!(f, "upstream proxy request exceeds the 16384-byte limit")
            }
        }
    }
}

impl std::error::Error for OutboundError {}

pub fn validate_target(target: &TargetAddr) -> Result<(), OutboundError> {
    if target.port() == 0 {
        return Err(OutboundError::InvalidTargetPort);
    }
    if let TargetAddr::Domain { host, .. } = target {
        if host.len() > u8::MAX as usize {
            return Err(OutboundError::DomainTooLong);
        }
        if host.is_empty() || host.bytes().any(|byte| byte <= b' ' || byte == 0x7f) {
            return Err(OutboundError::InvalidDomain);
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum DnsResolutionContext<'a> {
    Product {
        generation: &'a DnsGeneration,
        plan: Option<&'a DnsPlanId>,
    },
    /// Opening this connector must not consult the resolver it carries.
    LiteralOnly,
}

pub(crate) enum ConnectorTarget<'a> {
    Domain(&'a AuthorizedDomainTarget),
    Resolved(&'a [AuthorizedTarget]),
}

impl DnsResolutionContext<'_> {
    async fn resolve_socket_addrs(
        &self,
        host: &str,
        port: u16,
    ) -> Result<Vec<SocketAddr>, OutboundConnectError> {
        if let Ok(address) = host.parse::<IpAddr>() {
            return Ok(vec![SocketAddr::new(address, port)]);
        }
        match self {
            Self::Product { generation, plan } => Ok(generation
                .resolve_socket_addrs_for_plan(*plan, host, port)
                .await?),
            Self::LiteralOnly => Err(OutboundConnectError::DnsDependentProxyEndpoint(
                host.to_string(),
            )),
        }
    }
}

pub async fn connect_tcp(
    config: &OutboundConfig,
    dns: &DnsGeneration,
    dns_plan: Option<&DnsPlanId>,
    destination_policy: &dyn DestinationAuthorizer,
    target: &TargetAddr,
    timeout: Duration,
) -> Result<OutboundTcpStream, OutboundConnectError> {
    connect_tcp_with_configurator(
        config,
        dns,
        dns_plan,
        destination_policy,
        target,
        timeout,
        &SystemNativeSocketConfigurator,
    )
    .await
}

pub async fn connect_tcp_with_configurator(
    config: &OutboundConfig,
    dns: &DnsGeneration,
    dns_plan: Option<&DnsPlanId>,
    destination_policy: &dyn DestinationAuthorizer,
    target: &TargetAddr,
    timeout: Duration,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<OutboundTcpStream, OutboundConnectError> {
    config.ensure_supports(TargetProtocol::Tcp)?;
    validate_target(target)?;
    let deadline = tokio::time::Instant::now() + timeout;
    let authorization = destination_policy.begin(Network::Tcp, target)?;
    if authorization.target().ip().is_some()
        || config.requires_ip_target()
        || authorization.requires_post_resolution()
    {
        let authorized = resolve_authorization_before(
            dns,
            dns_plan,
            destination_policy,
            authorization,
            deadline,
        )
        .await?;
        connect_tcp_target_with_configurator(
            config,
            dns,
            dns_plan,
            ConnectorTarget::Resolved(&authorized),
            deadline,
            configurator,
        )
        .await
    } else {
        let domain = destination_policy.authorize_domain(authorization)?;
        connect_tcp_target_with_configurator(
            config,
            dns,
            dns_plan,
            ConnectorTarget::Domain(&domain),
            deadline,
            configurator,
        )
        .await
    }
}

pub(crate) async fn connect_tcp_target_with_configurator(
    config: &OutboundConfig,
    dns: &DnsGeneration,
    dns_plan: Option<&DnsPlanId>,
    target: ConnectorTarget<'_>,
    deadline: tokio::time::Instant,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<OutboundTcpStream, OutboundConnectError> {
    config.ensure_supports(TargetProtocol::Tcp)?;
    let dns = DnsResolutionContext::Product {
        generation: dns,
        plan: dns_plan,
    };
    match target {
        ConnectorTarget::Resolved(authorized) => {
            let addresses = authorized_socket_addrs(authorized, Network::Tcp)?;
            connect_tcp_leaf_to_addresses(config, &dns, &addresses, deadline, configurator).await
        }
        ConnectorTarget::Domain(domain) => {
            let target = super::destination::protocol_target_addr(domain.flow().target());
            validate_target(&target)?;
            match config {
                OutboundConfig::Direct
                | OutboundConfig::BindSourceIp(_)
                | OutboundConfig::BindSourceIps { .. } => {
                    Err(OutboundConnectError::TargetResolutionRequired)
                }
                OutboundConfig::Socks5(proxy) => {
                    connect_socks5_tcp_one(proxy, &target, &dns, deadline, configurator)
                        .await
                        .map(OutboundTcpStream::Plain)
                }
                OutboundConfig::HttpConnect(proxy) => {
                    connect_http_connect_tcp_one(proxy, &target, &dns, deadline, configurator)
                        .await
                        .map(OutboundTcpStream::Plain)
                }
                OutboundConfig::HttpsConnect(proxy) => {
                    connect_https_connect_tcp_one(proxy, &target, &dns, deadline, configurator)
                        .await
                }
            }
        }
    }
}

async fn connect_tcp_leaf_to_addresses(
    config: &OutboundConfig,
    dns: &DnsResolutionContext<'_>,
    addresses: &[SocketAddr],
    deadline: tokio::time::Instant,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<OutboundTcpStream, OutboundConnectError> {
    match config {
        OutboundConfig::Direct
        | OutboundConfig::BindSourceIp(_)
        | OutboundConfig::BindSourceIps { .. } => {
            connect_direct_tcp(config, addresses, deadline, configurator)
                .await
                .map(OutboundTcpStream::Plain)
        }
        OutboundConfig::Socks5(proxy) => {
            connect_socks5_tcp(proxy, addresses, dns, deadline, configurator)
                .await
                .map(OutboundTcpStream::Plain)
        }
        OutboundConfig::HttpConnect(proxy) => {
            connect_http_connect_tcp(proxy, addresses, dns, deadline, configurator)
                .await
                .map(OutboundTcpStream::Plain)
        }
        OutboundConfig::HttpsConnect(proxy) => {
            connect_https_connect_tcp(proxy, addresses, dns, deadline, configurator).await
        }
    }
}

/// Opens one literal target through a DNS-independent Product leaf.
///
/// This is intentionally unable to resolve a proxy control hostname: routed
/// DNS is compiled before its resolver generation exists, and consulting that
/// generation here would recurse.
pub(crate) async fn connect_tcp_literal_target_with_configurator(
    config: &OutboundConfig,
    target: SocketAddr,
    timeout: Duration,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<OutboundTcpStream, OutboundConnectError> {
    config.ensure_supports(TargetProtocol::Tcp)?;
    if timeout.is_zero() {
        return Err(OutboundConnectError::ConnectTimeout);
    }
    connect_tcp_leaf_to_addresses(
        config,
        &DnsResolutionContext::LiteralOnly,
        &[target],
        tokio::time::Instant::now() + timeout,
        configurator,
    )
    .await
}

pub async fn connect_udp(
    config: &OutboundConfig,
    dns: &DnsGeneration,
    dns_plan: Option<&DnsPlanId>,
    destination_policy: &dyn DestinationAuthorizer,
    target: &TargetAddr,
    timeout: Duration,
) -> Result<OutboundUdpSocket, OutboundConnectError> {
    connect_udp_with_configurator(
        config,
        dns,
        dns_plan,
        destination_policy,
        target,
        timeout,
        &SystemNativeSocketConfigurator,
    )
    .await
}

pub async fn connect_udp_with_configurator(
    config: &OutboundConfig,
    dns: &DnsGeneration,
    dns_plan: Option<&DnsPlanId>,
    destination_policy: &dyn DestinationAuthorizer,
    target: &TargetAddr,
    timeout: Duration,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<OutboundUdpSocket, OutboundConnectError> {
    config.ensure_supports(TargetProtocol::Udp)?;
    validate_target(target)?;
    let deadline = tokio::time::Instant::now() + timeout;
    let authorization = destination_policy.begin(Network::Udp, target)?;
    if authorization.target().ip().is_some()
        || config.requires_ip_target()
        || authorization.requires_post_resolution()
    {
        let authorized = resolve_authorization_before(
            dns,
            dns_plan,
            destination_policy,
            authorization,
            deadline,
        )
        .await?;
        connect_udp_target_with_configurator(
            config,
            dns,
            dns_plan,
            ConnectorTarget::Resolved(&authorized),
            deadline,
            configurator,
        )
        .await
    } else {
        let domain = destination_policy.authorize_domain(authorization)?;
        connect_udp_target_with_configurator(
            config,
            dns,
            dns_plan,
            ConnectorTarget::Domain(&domain),
            deadline,
            configurator,
        )
        .await
    }
}

pub(crate) async fn connect_udp_target_with_configurator(
    config: &OutboundConfig,
    dns: &DnsGeneration,
    dns_plan: Option<&DnsPlanId>,
    target: ConnectorTarget<'_>,
    deadline: tokio::time::Instant,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<OutboundUdpSocket, OutboundConnectError> {
    config.ensure_supports(TargetProtocol::Udp)?;
    let dns = DnsResolutionContext::Product {
        generation: dns,
        plan: dns_plan,
    };
    match target {
        ConnectorTarget::Resolved(authorized) => {
            let addresses = authorized_socket_addrs(authorized, Network::Udp)?;
            match config {
                OutboundConfig::Direct
                | OutboundConfig::BindSourceIp(_)
                | OutboundConfig::BindSourceIps { .. } => {
                    connect_direct_udp(config, &addresses, deadline, configurator)
                        .await
                        .map(OutboundUdpSocket::Direct)
                }
                OutboundConfig::Socks5(proxy) => {
                    connect_socks5_udp(proxy, &addresses, &dns, deadline, configurator)
                        .await
                        .map(OutboundUdpSocket::Socks5)
                }
                OutboundConfig::HttpConnect(_) | OutboundConfig::HttpsConnect(_) => {
                    Err(OutboundError::UdpNotSupported.into())
                }
            }
        }
        ConnectorTarget::Domain(domain) => match config {
            OutboundConfig::Direct
            | OutboundConfig::BindSourceIp(_)
            | OutboundConfig::BindSourceIps { .. } => {
                Err(OutboundConnectError::TargetResolutionRequired)
            }
            OutboundConfig::Socks5(proxy) => {
                let target = super::destination::protocol_target_addr(domain.flow().target());
                validate_target(&target)?;
                connect_socks5_udp_one(proxy, &target, &dns, deadline, configurator)
                    .await
                    .map(OutboundUdpSocket::Socks5)
            }
            OutboundConfig::HttpConnect(_) | OutboundConfig::HttpsConnect(_) => {
                Err(OutboundError::UdpNotSupported.into())
            }
        },
    }
}

#[derive(Debug)]
pub enum OutboundTcpStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for OutboundTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buffer),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buffer),
        }
    }
}

impl AsyncWrite for OutboundTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buffer),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buffer),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Plain(stream) => stream.is_write_vectored(),
            Self::Tls(stream) => stream.is_write_vectored(),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[std::io::IoSlice<'_>],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write_vectored(cx, buffers),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write_vectored(cx, buffers),
        }
    }
}

#[derive(Debug)]
pub enum OutboundUdpSocket {
    Direct(UdpSocket),
    Socks5(Socks5UdpAssociation),
}

impl OutboundUdpSocket {
    pub async fn send(&mut self, payload: &[u8]) -> Result<usize, OutboundConnectError> {
        match self {
            Self::Direct(socket) => Ok(socket.send(payload).await?),
            Self::Socks5(association) => association.send(payload).await,
        }
    }

    pub async fn recv(&mut self, buffer: &mut [u8]) -> Result<usize, OutboundConnectError> {
        match self {
            Self::Direct(socket) => Ok(socket.recv(buffer).await?),
            Self::Socks5(association) => association.recv(buffer).await,
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

impl Socks5UdpAssociation {
    pub(crate) async fn send(&mut self, payload: &[u8]) -> Result<usize, OutboundConnectError> {
        let packet = socks5_udp::udp_datagram(&self.target, payload)
            .map_err(OutboundConnectError::Socks5UdpPacket)?;
        self.relay.send(&packet).await?;
        Ok(payload.len())
    }

    pub(crate) async fn recv(&mut self, buffer: &mut [u8]) -> Result<usize, OutboundConnectError> {
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
    config: &OutboundConfig,
    addresses: &[SocketAddr],
    deadline: tokio::time::Instant,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<TcpStream, OutboundConnectError> {
    let mut ipv4 = Vec::new();
    let mut ipv6 = Vec::new();
    let mut default = Vec::new();
    for address in addresses.iter().copied() {
        match config.source_binding_for(address.ip()) {
            DirectSourceBinding::Ineligible => {}
            DirectSourceBinding::Default => default.push(address),
            DirectSourceBinding::Bound(IpAddr::V4(source)) => ipv4.push((address, source)),
            DirectSourceBinding::Bound(IpAddr::V6(source)) => ipv6.push((address, source)),
        }
    }
    if !default.is_empty() {
        return connect_direct_tcp_addresses(&default, None, deadline, configurator).await;
    }
    let ipv4_source = ipv4.first().map(|(_, source)| IpAddr::V4(*source));
    let ipv6_source = ipv6.first().map(|(_, source)| IpAddr::V6(*source));
    let ipv4 = ipv4
        .into_iter()
        .map(|(address, _)| address)
        .collect::<Vec<_>>();
    let ipv6 = ipv6
        .into_iter()
        .map(|(address, _)| address)
        .collect::<Vec<_>>();
    match (ipv4_source, ipv6_source) {
        (Some(source), None) => {
            connect_direct_tcp_addresses(&ipv4, Some(source), deadline, configurator).await
        }
        (None, Some(source)) => {
            connect_direct_tcp_addresses(&ipv6, Some(source), deadline, configurator).await
        }
        (Some(ipv4_source), Some(ipv6_source)) => {
            tokio::select! {
                result = connect_direct_tcp_addresses(
                    &ipv4,
                    Some(ipv4_source),
                    deadline,
                    configurator,
                ) => match result {
                    Ok(stream) => Ok(stream),
                    Err(_) => connect_direct_tcp_addresses(
                        &ipv6,
                        Some(ipv6_source),
                        deadline,
                        configurator,
                    ).await,
                },
                result = connect_direct_tcp_addresses(
                    &ipv6,
                    Some(ipv6_source),
                    deadline,
                    configurator,
                ) => match result {
                    Ok(stream) => Ok(stream),
                    Err(_) => connect_direct_tcp_addresses(
                        &ipv4,
                        Some(ipv4_source),
                        deadline,
                        configurator,
                    ).await,
                },
            }
        }
        (None, None) => Err(TcpTransportError::NoCompatibleAddress.into()),
    }
}

async fn connect_direct_tcp_addresses(
    addresses: &[SocketAddr],
    source_ip: Option<IpAddr>,
    deadline: tokio::time::Instant,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<TcpStream, OutboundConnectError> {
    let timeout = remaining_timeout(deadline)?;
    operation_before(
        deadline,
        tcp::connect_addrs_with_configurator(
            addresses.to_vec(),
            TcpConnectOptions {
                source_ip,
                timeout,
                ..TcpConnectOptions::default()
            },
            NativeEgressPurpose::Target,
            configurator,
        ),
    )
    .await?
    .map_err(Into::into)
}

async fn connect_direct_udp(
    config: &OutboundConfig,
    addresses: &[SocketAddr],
    deadline: tokio::time::Instant,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<UdpSocket, OutboundConnectError> {
    let mut last_error = None;
    for addr in addresses.iter().copied() {
        let source_ip = match config.source_binding_for(addr.ip()) {
            DirectSourceBinding::Ineligible => continue,
            DirectSourceBinding::Default => None,
            DirectSourceBinding::Bound(source_ip) => Some(source_ip),
        };
        let timeout = remaining_timeout(deadline)?;
        match operation_before(
            deadline,
            udp::connect_addr_with_configurator(
                addr,
                UdpConnectOptions { source_ip, timeout },
                NativeEgressPurpose::Target,
                configurator,
            ),
        )
        .await?
        {
            Ok(socket) => return Ok(socket),
            Err(err) => last_error = Some(err),
        }
    }
    Err(last_error
        .unwrap_or(UdpTransportError::NoCompatibleAddress)
        .into())
}

async fn connect_socks5_tcp(
    proxy: &ProxyConfig,
    addresses: &[SocketAddr],
    dns: &DnsResolutionContext<'_>,
    deadline: tokio::time::Instant,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<TcpStream, OutboundConnectError> {
    let mut last_error = None;
    for address in addresses.iter().copied() {
        let target = TargetAddr::Ip(address);
        match connect_socks5_tcp_one(proxy, &target, dns, deadline, configurator).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or(OutboundConnectError::NoAuthorizedAddresses))
}

async fn connect_socks5_tcp_one(
    proxy: &ProxyConfig,
    target: &TargetAddr,
    dns: &DnsResolutionContext<'_>,
    deadline: tokio::time::Instant,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<TcpStream, OutboundConnectError> {
    let timeout = remaining_timeout(deadline)?;
    let mut stream = proxy_transaction_before(
        deadline,
        connect_tcp_endpoint(
            proxy.endpoint(),
            dns,
            TcpConnectOptions {
                timeout,
                ..TcpConnectOptions::default()
            },
            configurator,
        ),
    )
    .await?;
    let reply = proxy_transaction_before(deadline, async {
        negotiate_socks5(&mut stream, proxy.credentials()).await?;
        let request = socks5::connect_request(target)?;
        stream.write_all(&request).await?;
        read_socks5_reply(&mut stream).await
    })
    .await?;
    if reply.status != 0x00 {
        return Err(OutboundConnectError::ProxyRejected(reply.status as u16));
    }
    Ok(stream)
}

async fn connect_socks5_udp(
    proxy: &ProxyConfig,
    addresses: &[SocketAddr],
    dns: &DnsResolutionContext<'_>,
    deadline: tokio::time::Instant,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<Socks5UdpAssociation, OutboundConnectError> {
    let mut last_error = None;
    for address in addresses.iter().copied() {
        let target = TargetAddr::Ip(address);
        match connect_socks5_udp_one(proxy, &target, dns, deadline, configurator).await {
            Ok(association) => return Ok(association),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or(OutboundConnectError::NoAuthorizedAddresses))
}

async fn connect_socks5_udp_one(
    proxy: &ProxyConfig,
    target: &TargetAddr,
    dns: &DnsResolutionContext<'_>,
    deadline: tokio::time::Instant,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<Socks5UdpAssociation, OutboundConnectError> {
    let timeout = remaining_timeout(deadline)?;
    let mut control = proxy_transaction_before(
        deadline,
        connect_tcp_endpoint(
            proxy.endpoint(),
            dns,
            TcpConnectOptions {
                timeout,
                ..TcpConnectOptions::default()
            },
            configurator,
        ),
    )
    .await?;
    let control_peer = control.peer_addr()?;
    let client_udp_endpoint = socks5_udp_client_endpoint(control.local_addr()?);
    let reply = proxy_transaction_before(deadline, async {
        negotiate_socks5(&mut control, proxy.credentials()).await?;
        let request = socks5::udp_associate_request(client_udp_endpoint)?;
        control.write_all(&request).await?;
        read_socks5_reply(&mut control).await
    })
    .await?;
    if reply.status != 0x00 {
        return Err(OutboundConnectError::ProxyRejected(reply.status as u16));
    }
    let relay = relay_endpoint_from_socks5_bind(&reply.bind, control_peer)?;
    let relay = proxy_transaction_before(
        deadline,
        connect_udp_endpoint(
            &relay,
            dns,
            UdpConnectOptions {
                timeout,
                ..UdpConnectOptions::default()
            },
            configurator,
        ),
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
    proxy: &ProxyConfig,
    addresses: &[SocketAddr],
    dns: &DnsResolutionContext<'_>,
    deadline: tokio::time::Instant,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<TcpStream, OutboundConnectError> {
    let mut last_error = None;
    for address in addresses.iter().copied() {
        let target = TargetAddr::Ip(address);
        match connect_http_connect_tcp_one(proxy, &target, dns, deadline, configurator).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or(OutboundConnectError::NoAuthorizedAddresses))
}

async fn connect_http_connect_tcp_one(
    proxy: &ProxyConfig,
    target: &TargetAddr,
    dns: &DnsResolutionContext<'_>,
    deadline: tokio::time::Instant,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<TcpStream, OutboundConnectError> {
    let timeout = remaining_timeout(deadline)?;
    let mut stream = proxy_transaction_before(
        deadline,
        connect_tcp_endpoint(
            proxy.endpoint(),
            dns,
            TcpConnectOptions {
                timeout,
                ..TcpConnectOptions::default()
            },
            configurator,
        ),
    )
    .await?;
    let response = proxy_transaction_before(deadline, async {
        let request = http_connect::connect_request(target, None, proxy.credentials())?;
        stream.write_all(&request).await?;
        let response = read_http_proxy_response(&mut stream).await?;
        http_connect::parse_connect_response(&response).map_err(Into::into)
    })
    .await?;
    if response.status != 200 {
        return Err(OutboundConnectError::ProxyRejected(response.status));
    }
    Ok(stream)
}

async fn connect_https_connect_tcp(
    proxy: &HttpsProxyConfig,
    addresses: &[SocketAddr],
    dns: &DnsResolutionContext<'_>,
    deadline: tokio::time::Instant,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<OutboundTcpStream, OutboundConnectError> {
    let mut last_error = None;
    for address in addresses.iter().copied() {
        let target = TargetAddr::Ip(address);
        match connect_https_connect_tcp_one(proxy, &target, dns, deadline, configurator).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or(OutboundConnectError::NoAuthorizedAddresses))
}

async fn connect_https_connect_tcp_one(
    proxy: &HttpsProxyConfig,
    target: &TargetAddr,
    dns: &DnsResolutionContext<'_>,
    deadline: tokio::time::Instant,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<OutboundTcpStream, OutboundConnectError> {
    let timeout = remaining_timeout(deadline)?;
    let stream = proxy_transaction_before(
        deadline,
        connect_tcp_endpoint(
            proxy.proxy().endpoint(),
            dns,
            TcpConnectOptions {
                timeout,
                ..TcpConnectOptions::default()
            },
            configurator,
        ),
    )
    .await?;
    let server_name = ServerName::try_from(proxy.tls_server_name().to_string())
        .map_err(|_| OutboundError::InvalidTlsServerName)?;
    let (stream, response) = proxy_transaction_before(deadline, async {
        let mut stream = tokio_rustls::TlsConnector::from(proxy.tls_config.clone())
            .connect(server_name, stream)
            .await?;
        let request = http_connect::connect_request(target, None, proxy.proxy().credentials())?;
        stream.write_all(&request).await?;
        let response = read_http_proxy_response(&mut stream).await?;
        let response = http_connect::parse_connect_response(&response)?;
        Ok((stream, response))
    })
    .await?;
    if response.status != 200 {
        return Err(OutboundConnectError::ProxyRejected(response.status));
    }
    Ok(OutboundTcpStream::Tls(Box::new(stream)))
}

async fn proxy_transaction_before<T>(
    deadline: tokio::time::Instant,
    handshake: impl Future<Output = Result<T, OutboundConnectError>>,
) -> Result<T, OutboundConnectError> {
    tokio::time::timeout_at(deadline, handshake)
        .await
        .map_err(|_| OutboundConnectError::ProxyTimeout)?
}

async fn read_http_proxy_response<S>(stream: &mut S) -> Result<Vec<u8>, OutboundConnectError>
where
    S: AsyncRead + Unpin,
{
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

pub(crate) async fn resolve_authorization_before(
    dns: &DnsGeneration,
    dns_plan: Option<&DnsPlanId>,
    destination_policy: &dyn DestinationAuthorizer,
    authorization: DestinationAuthorization,
    deadline: tokio::time::Instant,
) -> Result<Vec<AuthorizedTarget>, OutboundConnectError> {
    let dns = DnsResolutionContext::Product {
        generation: dns,
        plan: dns_plan,
    };
    let target = authorization.target();
    let addresses = match target.ip() {
        Some(address) => vec![address],
        None => {
            let host = target
                .domain()
                .expect("non-IP Product target has a domain")
                .as_str();
            operation_before(
                deadline,
                dns.resolve_socket_addrs(host, target.port().get()),
            )
            .await??
            .into_iter()
            .map(|address| address.ip())
            .collect()
        }
    };
    destination_policy
        .authorize_addresses(authorization, &addresses)
        .map_err(Into::into)
}

pub(crate) async fn resolve_authorized_domain_before(
    dns: &DnsGeneration,
    dns_plan: Option<&DnsPlanId>,
    destination_policy: &dyn DestinationAuthorizer,
    domain: &AuthorizedDomainTarget,
    deadline: tokio::time::Instant,
) -> Result<Vec<AuthorizedTarget>, OutboundConnectError> {
    let target = domain.flow().target();
    let Some(host) = target.domain() else {
        return Err(DestinationAuthorizationError::TargetChanged.into());
    };
    let dns = DnsResolutionContext::Product {
        generation: dns,
        plan: dns_plan,
    };
    let addresses = operation_before(
        deadline,
        dns.resolve_socket_addrs(host.as_str(), target.port().get()),
    )
    .await??
    .into_iter()
    .map(|address| address.ip())
    .collect::<Vec<_>>();
    destination_policy
        .authorize_domain_addresses(domain, &addresses)
        .map_err(Into::into)
}

fn authorized_socket_addrs(
    authorized: &[AuthorizedTarget],
    network: Network,
) -> Result<Vec<SocketAddr>, OutboundConnectError> {
    let Some(first) = authorized.first() else {
        return Err(OutboundConnectError::NoAuthorizedAddresses);
    };
    let generation = first.acl_generation();
    let flow = first.flow();
    if flow.network() != network {
        return Err(DestinationAuthorizationError::TargetChanged.into());
    }
    authorized
        .iter()
        .map(|target| {
            if target.acl_generation() != generation || target.flow() != flow {
                return Err(DestinationAuthorizationError::TargetChanged.into());
            }
            Ok(SocketAddr::new(
                target.address(),
                target.flow().target().port().get(),
            ))
        })
        .collect()
}

fn remaining_timeout(deadline: tokio::time::Instant) -> Result<Duration, OutboundConnectError> {
    let now = tokio::time::Instant::now();
    if now >= deadline {
        return Err(OutboundConnectError::ConnectTimeout);
    }
    Ok(deadline.duration_since(now))
}

async fn operation_before<T, E>(
    deadline: tokio::time::Instant,
    operation: impl Future<Output = Result<T, E>>,
) -> Result<Result<T, E>, OutboundConnectError> {
    tokio::time::timeout_at(deadline, operation)
        .await
        .map_err(|_| OutboundConnectError::ConnectTimeout)
}

async fn connect_tcp_endpoint(
    endpoint: &Endpoint,
    dns: &DnsResolutionContext<'_>,
    options: TcpConnectOptions,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<TcpStream, OutboundConnectError> {
    let addrs = dns
        .resolve_socket_addrs(&endpoint.host, endpoint.port)
        .await?;
    Ok(tcp::connect_addrs_with_configurator(
        addrs,
        options,
        NativeEgressPurpose::Proxy,
        configurator,
    )
    .await?)
}

async fn connect_udp_endpoint(
    endpoint: &Endpoint,
    dns: &DnsResolutionContext<'_>,
    options: UdpConnectOptions,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<UdpSocket, OutboundConnectError> {
    let addrs = dns
        .resolve_socket_addrs(&endpoint.host, endpoint.port)
        .await?;
    let mut last_error = None;
    for addr in addrs {
        if let Some(source_ip) = options.source_ip
            && source_ip.is_ipv4() != addr.is_ipv4()
        {
            continue;
        }
        match udp::connect_addr_with_configurator(
            addr,
            options,
            NativeEgressPurpose::Proxy,
            configurator,
        )
        .await
        {
            Ok(socket) => return Ok(socket),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error
        .unwrap_or(UdpTransportError::NoCompatibleAddress)
        .into())
}

async fn negotiate_socks5(
    stream: &mut TcpStream,
    credentials: Option<&ProxyCredentials>,
) -> Result<(), OutboundConnectError> {
    let greeting = if credentials.is_some() {
        socks5::username_password_greeting()
    } else {
        socks5::no_auth_greeting()
    };
    stream.write_all(&greeting).await?;
    let mut method = [0u8; 2];
    stream.read_exact(&mut method).await?;
    let method = socks5::parse_method_selection(&method)?;
    let expected_method = if credentials.is_some() { 0x02 } else { 0x00 };
    if method.method != expected_method {
        return Err(OutboundConnectError::ProxyAuthRejected(method.method));
    }
    if let Some(credentials) = credentials {
        stream
            .write_all(&socks5::username_password_request(credentials))
            .await?;
        let mut reply = [0u8; 2];
        stream.read_exact(&mut reply).await?;
        socks5::parse_username_password_reply(&reply)?;
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
    DestinationAuthorization(DestinationAuthorizationError),
    Endpoint(crate::transport::EndpointParseError),
    Tcp(TcpTransportError),
    Udp(UdpTransportError),
    Io(std::io::Error),
    Dns(DnsRuntimeError),
    Socks5Client(socks5::Socks5ClientError),
    HttpConnectClient(http_connect::HttpConnectClientError),
    ProxyAuthRejected(u8),
    ConnectTimeout,
    ProxyTimeout,
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
    NoAuthorizedAddresses,
    TargetResolutionRequired,
    DnsDependentProxyEndpoint(String),
}

impl From<OutboundError> for OutboundConnectError {
    fn from(value: OutboundError) -> Self {
        Self::Policy(value)
    }
}

impl From<DestinationAuthorizationError> for OutboundConnectError {
    fn from(value: DestinationAuthorizationError) -> Self {
        Self::DestinationAuthorization(value)
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

impl From<DnsRuntimeError> for OutboundConnectError {
    fn from(value: DnsRuntimeError) -> Self {
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
            Self::DestinationAuthorization(err) => write!(f, "{err}"),
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
            Self::ConnectTimeout => {
                write!(f, "outbound DNS and connection transaction timed out")
            }
            Self::ProxyTimeout => {
                write!(f, "upstream proxy connection transaction timed out")
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
            Self::NoAuthorizedAddresses => {
                write!(
                    f,
                    "destination authorization returned no connector addresses"
                )
            }
            Self::TargetResolutionRequired => {
                write!(f, "native target connector requires resolved addresses")
            }
            Self::DnsDependentProxyEndpoint(host) => write!(
                f,
                "DNS-routed proxy control endpoint {host:?} is not a literal IP"
            ),
        }
    }
}

impl std::error::Error for OutboundConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Policy(err) => Some(err),
            Self::DestinationAuthorization(err) => Some(err),
            Self::Endpoint(err) => Some(err),
            Self::Tcp(err) => Some(err),
            Self::Udp(err) => Some(err),
            Self::Io(err) => Some(err),
            Self::Dns(err) => Some(err),
            Self::Socks5Client(err) => Some(err),
            Self::HttpConnectClient(err) => Some(err),
            Self::Socks5UdpPacket(err) => Some(err),
            Self::ProxyAuthRejected(_)
            | Self::ConnectTimeout
            | Self::ProxyTimeout
            | Self::ProxyRejected(_)
            | Self::UdpRelayTargetMismatch { .. }
            | Self::UdpReceiveBufferTooSmall { .. }
            | Self::InvalidProxyResponse
            | Self::NoAuthorizedAddresses
            | Self::TargetResolutionRequired
            | Self::DnsDependentProxyEndpoint(_) => None,
        }
    }
}

#[cfg(test)]
#[path = "tests_connect.rs"]
mod tests;
