//! Host-owned carrier resolution and raw socket construction.
//!
//! Resolution and socket routing must share one path identity: mobile VPN and
//! multi-network hosts need to resolve and connect outside the product tunnel.
//! TCP and QUIC still own their distinct address-attempt and handshake policy.

use crate::protocol::UnderlayProtocol;
use crate::transport::PathSpec;
#[cfg(target_os = "linux")]
use crate::transport::native_egress::LinuxSocketMarker;
use crate::transport::native_egress::{
    HostSocketProtectionRequest, HostSocketProtector, HostSocketPurpose, protect_socket,
};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;

/// Stable position of one path within the process configuration generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CarrierPathIdentity {
    /// Client path-group position; zero for a standalone client.
    pub group_ordinal: usize,
    /// Position in that group's configured path list, before carrier filtering.
    pub path_ordinal: usize,
}

/// Concrete path context supplied before a carrier endpoint is resolved.
#[derive(Debug, Clone, Copy)]
pub struct CarrierResolutionRequest<'a> {
    pub path: &'a PathSpec,
    pub identity: CarrierPathIdentity,
    /// Concrete port selected once for this physical carrier establishment.
    ///
    /// Every address returned for this request must retain this exact port.
    pub remote_port: u16,
}

impl CarrierResolutionRequest<'_> {
    pub fn validate(&self) -> io::Result<()> {
        if self.path.endpoint.ports().contains(self.remote_port) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "selected carrier port {} is outside configured endpoint {}",
                    self.remote_port,
                    self.path.endpoint.authority()
                ),
            ))
        }
    }
}

pub type CarrierResolutionFuture<'a> =
    Pin<Box<dyn Future<Output = io::Result<Vec<SocketAddr>>> + Send + 'a>>;

/// Concrete path context supplied before the transport connects a carrier.
#[derive(Debug, Clone, Copy)]
pub struct CarrierSocketRequest<'a> {
    pub path: &'a PathSpec,
    pub identity: CarrierPathIdentity,
    pub remote_addr: SocketAddr,
}

/// Host integrations own network selection while transports retain connect semantics.
pub trait CarrierNetworkProvider: Send + Sync + 'static {
    /// Resolves through the same native network that will own the carrier sockets.
    ///
    /// Answers retain resolver preference order and the requested endpoint port.
    /// The future must tolerate cancellation when the path-open deadline expires.
    fn resolve<'a>(&'a self, request: CarrierResolutionRequest<'a>) -> CarrierResolutionFuture<'a>;

    /// Creates one socket on the native network used for this path's resolution.
    fn create_socket(&self, request: CarrierSocketRequest<'_>) -> io::Result<CarrierSocket>;
}

/// Alternates address families while preserving resolver preference and each
/// family's order. TCP and QUIC own separate races over this neutral ordering.
pub(crate) fn interleave_socket_addr_families(addrs: Vec<SocketAddr>) -> Vec<SocketAddr> {
    let Some(first) = addrs.first() else {
        return addrs;
    };
    let preferred_is_v4 = first.is_ipv4();
    let mut preferred = VecDeque::new();
    let mut alternate = VecDeque::new();
    for addr in addrs {
        if addr.is_ipv4() == preferred_is_v4 {
            preferred.push_back(addr);
        } else {
            alternate.push_back(addr);
        }
    }
    let mut ordered = Vec::with_capacity(preferred.len() + alternate.len());
    while !preferred.is_empty() || !alternate.is_empty() {
        if let Some(addr) = preferred.pop_front() {
            ordered.push(addr);
        }
        if let Some(addr) = alternate.pop_front() {
            ordered.push(addr);
        }
    }
    ordered
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemCarrierNetworkProvider;

impl CarrierNetworkProvider for SystemCarrierNetworkProvider {
    fn resolve<'a>(&'a self, request: CarrierResolutionRequest<'a>) -> CarrierResolutionFuture<'a> {
        Box::pin(async move {
            request.validate()?;
            tokio::net::lookup_host((request.path.endpoint.host.as_str(), request.remote_port))
                .await
                .map(|addrs| addrs.collect())
        })
    }

    fn create_socket(&self, request: CarrierSocketRequest<'_>) -> io::Result<CarrierSocket> {
        CarrierSocket::system(request)
    }
}

/// Applies one host VPN protection callback to every carrier socket created by
/// an underlying resolver/network selector.
///
/// Resolution remains owned by `inner`; protection runs once after socket
/// creation/source binding and before TCP connect or QUIC's first UDP send.
#[derive(Clone)]
pub struct ProtectedCarrierNetworkProvider {
    inner: Arc<dyn CarrierNetworkProvider>,
    protector: Arc<dyn HostSocketProtector>,
}

impl ProtectedCarrierNetworkProvider {
    pub fn new(
        inner: Arc<dyn CarrierNetworkProvider>,
        protector: Arc<dyn HostSocketProtector>,
    ) -> Self {
        Self { inner, protector }
    }

    pub fn inner(&self) -> &Arc<dyn CarrierNetworkProvider> {
        &self.inner
    }

    pub fn protector(&self) -> &Arc<dyn HostSocketProtector> {
        &self.protector
    }
}

impl std::fmt::Debug for ProtectedCarrierNetworkProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProtectedCarrierNetworkProvider")
            .finish_non_exhaustive()
    }
}

impl CarrierNetworkProvider for ProtectedCarrierNetworkProvider {
    fn resolve<'a>(&'a self, request: CarrierResolutionRequest<'a>) -> CarrierResolutionFuture<'a> {
        self.inner.resolve(request)
    }

    fn create_socket(&self, request: CarrierSocketRequest<'_>) -> io::Result<CarrierSocket> {
        let socket = self.inner.create_socket(request)?;
        protect_carrier_socket(self.protector.as_ref(), &socket, request)?;
        Ok(socket)
    }
}

fn protect_carrier_socket(
    protector: &dyn HostSocketProtector,
    socket: &CarrierSocket,
    request: CarrierSocketRequest<'_>,
) -> io::Result<()> {
    protect_socket(
        protector,
        socket,
        HostSocketProtectionRequest {
            remote_addr: request.remote_addr,
            purpose: HostSocketPurpose::MppCarrier {
                underlay: request.path.underlay,
                group_ordinal: request.identity.group_ordinal,
                path_ordinal: request.identity.path_ordinal,
            },
        },
    )
}

/// One carrier path resolved before host VPN policy is published.
///
/// The full path is retained deliberately: an identity alone is not enough to
/// authorize reuse after a configuration generation changes its endpoint or
/// source binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedCarrierPath {
    identity: CarrierPathIdentity,
    path: PathSpec,
    addresses: Vec<IpAddr>,
}

impl PreparedCarrierPath {
    pub fn new(
        identity: CarrierPathIdentity,
        path: PathSpec,
        addresses: impl IntoIterator<Item = SocketAddr>,
    ) -> io::Result<Self> {
        let mut addresses = addresses.into_iter().collect::<Vec<_>>();
        addresses.retain(|address| {
            path.endpoint.ports().contains(address.port())
                && path
                    .binding
                    .source_ip
                    .is_none_or(|source| source.is_ipv4() == address.is_ipv4())
        });
        let mut unique = Vec::with_capacity(addresses.len());
        for address in addresses {
            let address = address.ip();
            if !unique.contains(&address) {
                unique.push(address);
            }
        }
        let addresses = unique;
        if addresses.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "prepared carrier {} has no compatible address",
                    path.endpoint.authority()
                ),
            ));
        }
        Ok(Self {
            identity,
            path,
            addresses,
        })
    }

    pub fn identity(&self) -> CarrierPathIdentity {
        self.identity
    }

    pub fn path(&self) -> &PathSpec {
        &self.path
    }

    pub fn addresses(&self) -> &[IpAddr] {
        &self.addresses
    }
}

/// Immutable, generation-scoped carrier DNS snapshot.
///
/// A managed full-tunnel generation resolves every MPP carrier before
/// publishing host routes. Runtime reconnects then use this snapshot instead
/// of opening a resolver dependency through the tunnel they are responsible
/// for establishing.
#[derive(Debug, Clone)]
pub struct PreparedCarrierNetworkProvider {
    paths: Arc<[PreparedCarrierPath]>,
}

impl PreparedCarrierNetworkProvider {
    pub fn new(mut paths: Vec<PreparedCarrierPath>) -> io::Result<Self> {
        paths
            .sort_unstable_by_key(|path| (path.identity.group_ordinal, path.identity.path_ordinal));
        if paths
            .windows(2)
            .any(|pair| pair[0].identity == pair[1].identity)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "prepared carrier path identities must be unique",
            ));
        }
        Ok(Self {
            paths: paths.into(),
        })
    }

    pub fn paths(&self) -> &[PreparedCarrierPath] {
        &self.paths
    }

    pub fn endpoint_addresses(&self) -> Vec<IpAddr> {
        let mut addresses = self
            .paths
            .iter()
            .flat_map(|path| path.addresses.iter().copied())
            .collect::<Vec<_>>();
        addresses.sort_unstable();
        addresses.dedup();
        addresses
    }
}

impl CarrierNetworkProvider for PreparedCarrierNetworkProvider {
    fn resolve<'a>(&'a self, request: CarrierResolutionRequest<'a>) -> CarrierResolutionFuture<'a> {
        Box::pin(async move {
            request.validate()?;
            self.paths
                .iter()
                .find(|prepared| {
                    prepared.identity == request.identity && prepared.path == *request.path
                })
                .map(|prepared| {
                    prepared
                        .addresses
                        .iter()
                        .map(|address| SocketAddr::new(*address, request.remote_port))
                        .collect()
                })
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!(
                            "carrier path {} was not resolved before VPN publication",
                            request.path.endpoint.authority()
                        ),
                    )
                })
        })
    }

    fn create_socket(&self, request: CarrierSocketRequest<'_>) -> io::Result<CarrierSocket> {
        CarrierSocket::system(request)
    }
}

/// Rejects provider answers that violate one-establishment port selection.
pub(crate) fn validate_carrier_resolution_port(
    addresses: Vec<SocketAddr>,
    remote_port: u16,
) -> io::Result<Vec<SocketAddr>> {
    if addresses
        .iter()
        .all(|address| address.port() == remote_port)
    {
        Ok(addresses)
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("carrier resolver returned an address outside selected port {remote_port}"),
        ))
    }
}

/// Linux carrier wrapper that applies the native-main routing mark before
/// TCP connect or QUIC's first UDP send.
#[cfg(target_os = "linux")]
#[derive(Clone)]
pub struct LinuxMarkedCarrierNetworkProvider {
    inner: Arc<dyn CarrierNetworkProvider>,
    marker: LinuxSocketMarker,
}

#[cfg(target_os = "linux")]
impl LinuxMarkedCarrierNetworkProvider {
    pub fn new(
        inner: Arc<dyn CarrierNetworkProvider>,
        mark: crate::platform::LinuxSocketMark,
    ) -> Self {
        Self {
            inner,
            marker: LinuxSocketMarker::new(mark),
        }
    }

    pub fn mark(&self) -> crate::platform::LinuxSocketMark {
        self.marker.mark()
    }
}

#[cfg(target_os = "linux")]
impl CarrierNetworkProvider for LinuxMarkedCarrierNetworkProvider {
    fn resolve<'a>(&'a self, request: CarrierResolutionRequest<'a>) -> CarrierResolutionFuture<'a> {
        self.inner.resolve(request)
    }

    fn create_socket(&self, request: CarrierSocketRequest<'_>) -> io::Result<CarrierSocket> {
        let socket = self.inner.create_socket(request)?;
        protect_carrier_socket(&self.marker, &socket, request)?;
        Ok(socket)
    }
}

/// An unconnected, nonblocking carrier socket whose raw handle can be protected
/// or assigned to a platform network before runtime starts the connection.
#[derive(Debug)]
pub struct CarrierSocket {
    socket: Socket,
    underlay: UnderlayProtocol,
}

impl CarrierSocket {
    pub fn system(request: CarrierSocketRequest<'_>) -> io::Result<Self> {
        let domain = if request.remote_addr.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };
        let (socket_type, protocol) = match request.path.underlay {
            UnderlayProtocol::Tcp => (Type::STREAM, Protocol::TCP),
            UnderlayProtocol::Udp => (Type::DGRAM, Protocol::UDP),
        };
        let socket = Socket::new(domain, socket_type, Some(protocol))?;
        socket.set_nonblocking(true)?;

        let source_ip = validated_source_ip(request.path.binding.source_ip, request.remote_addr)?;
        if let Some(source_ip) = source_ip {
            socket.bind(&SockAddr::from(SocketAddr::new(source_ip, 0)))?;
        } else if request.path.underlay == UnderlayProtocol::Udp {
            // QUIC needs a bound UDP socket before handing ownership to its endpoint.
            socket.bind(&SockAddr::from(wildcard_addr(request.remote_addr)))?;
        }

        Ok(Self {
            socket,
            underlay: request.path.underlay,
        })
    }

    pub(crate) fn into_tcp_socket(self) -> io::Result<std::net::TcpStream> {
        self.ensure_underlay(UnderlayProtocol::Tcp)?;
        Ok(self.socket.into())
    }

    pub(crate) fn into_udp_socket(self) -> io::Result<std::net::UdpSocket> {
        self.ensure_underlay(UnderlayProtocol::Udp)?;
        Ok(self.socket.into())
    }

    fn ensure_underlay(&self, expected: UnderlayProtocol) -> io::Result<()> {
        if self.underlay == expected {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "carrier socket underlay does not match transport",
            ))
        }
    }
}

fn validated_source_ip(
    source_ip: Option<IpAddr>,
    remote_addr: SocketAddr,
) -> io::Result<Option<IpAddr>> {
    let Some(source_ip) = source_ip else {
        return Ok(None);
    };
    if source_ip.is_ipv4() != remote_addr.is_ipv4() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "source-address and resolved remote address use different IP families",
        ));
    }
    Ok(Some(source_ip))
}

fn wildcard_addr(remote_addr: SocketAddr) -> SocketAddr {
    if remote_addr.is_ipv4() {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
    } else {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0)
    }
}

#[cfg(unix)]
impl std::os::fd::AsFd for CarrierSocket {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        std::os::fd::AsFd::as_fd(&self.socket)
    }
}

#[cfg(unix)]
impl std::os::fd::AsRawFd for CarrierSocket {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        std::os::fd::AsRawFd::as_raw_fd(&self.socket)
    }
}

#[cfg(windows)]
impl std::os::windows::io::AsSocket for CarrierSocket {
    fn as_socket(&self) -> std::os::windows::io::BorrowedSocket<'_> {
        std::os::windows::io::AsSocket::as_socket(&self.socket)
    }
}

#[cfg(windows)]
impl std::os::windows::io::AsRawSocket for CarrierSocket {
    fn as_raw_socket(&self) -> std::os::windows::io::RawSocket {
        std::os::windows::io::AsRawSocket::as_raw_socket(&self.socket)
    }
}

#[cfg(test)]
#[path = "tests_carrier_network.rs"]
mod tests;
