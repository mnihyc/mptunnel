use crate::protocol::UnderlayProtocol;
use crate::transport::PathSpec;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// Concrete path context supplied before the transport connects a carrier.
#[derive(Debug, Clone, Copy)]
pub struct CarrierSocketRequest<'a> {
    pub path: &'a PathSpec,
    /// Position in the configured path list, stable across carrier families.
    pub config_ordinal: usize,
    pub remote_addr: SocketAddr,
}

/// Host integrations own OS routing while transports retain connect semantics.
pub trait CarrierSocketProvider: Send + Sync + 'static {
    fn create(&self, request: CarrierSocketRequest<'_>) -> io::Result<CarrierSocket>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemCarrierSocketProvider;

impl CarrierSocketProvider for SystemCarrierSocketProvider {
    fn create(&self, request: CarrierSocketRequest<'_>) -> io::Result<CarrierSocket> {
        CarrierSocket::system(request)
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
            "source-ip and resolved remote address use different IP families",
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
impl std::os::fd::AsRawFd for CarrierSocket {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        std::os::fd::AsRawFd::as_raw_fd(&self.socket)
    }
}

#[cfg(windows)]
impl std::os::windows::io::AsRawSocket for CarrierSocket {
    fn as_raw_socket(&self) -> std::os::windows::io::RawSocket {
        std::os::windows::io::AsRawSocket::as_raw_socket(&self.socket)
    }
}

#[cfg(test)]
#[path = "carrier_socket_test.rs"]
mod tests;
