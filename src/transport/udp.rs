use crate::protocol::UnderlayProtocol;
use crate::transport::{
    Endpoint, NativeEgressPurpose, NativeSocketConfigurator, NativeSocketRequest, PathSpec,
    SystemNativeSocketConfigurator,
};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;
use tokio::net::{UdpSocket, lookup_host};
use tokio::time::timeout;

// Raw UDP socket setup only. The QUIC carrier consumes UDP below mptunnel,
// while application DatagramData forwarding lives in runtime target workers;
// neither protocol's scheduling or reliability policy belongs here.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UdpConnectOptions {
    pub source_ip: Option<IpAddr>,
    pub timeout: Duration,
}

impl Default for UdpConnectOptions {
    fn default() -> Self {
        Self {
            source_ip: None,
            timeout: Duration::from_secs(10),
        }
    }
}

pub async fn connect_path(
    path: &PathSpec,
    options: UdpConnectOptions,
) -> Result<UdpSocket, UdpTransportError> {
    if path.underlay != UnderlayProtocol::Udp {
        return Err(UdpTransportError::WrongUnderlay(path.underlay));
    }
    let port = path.endpoint.ports().select().map_err(|error| {
        UdpTransportError::Io(std::io::Error::other(format!(
            "could not select a carrier port for {}: {error}",
            path.endpoint.authority()
        )))
    })?;
    connect_endpoint(
        &Endpoint::new(path.endpoint.host.clone(), port)
            .expect("selected carrier port and parsed host are valid"),
        options,
    )
    .await
}

pub async fn connect_endpoint(
    endpoint: &Endpoint,
    options: UdpConnectOptions,
) -> Result<UdpSocket, UdpTransportError> {
    connect_endpoint_with_configurator(
        endpoint,
        options,
        NativeEgressPurpose::Target,
        &SystemNativeSocketConfigurator,
    )
    .await
}

pub async fn connect_endpoint_with_configurator(
    endpoint: &Endpoint,
    options: UdpConnectOptions,
    purpose: NativeEgressPurpose,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<UdpSocket, UdpTransportError> {
    let addrs = resolve_endpoint(endpoint).await?;
    let mut last_error = None;
    for addr in addrs {
        if let Some(source_ip) = options.source_ip
            && source_ip.is_ipv4() != addr.is_ipv4()
        {
            continue;
        }
        match connect_addr_with_configurator(addr, options, purpose, configurator).await {
            Ok(socket) => return Ok(socket),
            Err(err) => last_error = Some(err),
        }
    }
    Err(last_error.unwrap_or(UdpTransportError::NoCompatibleAddress))
}

pub async fn bind_socket(path: &PathSpec) -> Result<UdpSocket, UdpTransportError> {
    if path.underlay != UnderlayProtocol::Udp {
        return Err(UdpTransportError::WrongUnderlay(path.underlay));
    }
    if !path.endpoint.ports().is_single() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "server carrier paths require one listener port; forward any advertised port range to that listener",
        )
        .into());
    }
    Ok(UdpSocket::bind(path.endpoint.first_endpoint().authority()).await?)
}

async fn resolve_endpoint(endpoint: &Endpoint) -> Result<Vec<SocketAddr>, UdpTransportError> {
    let addrs = lookup_host((endpoint.host.as_str(), endpoint.port))
        .await?
        .collect::<Vec<_>>();
    if addrs.is_empty() {
        Err(UdpTransportError::ResolutionEmpty(endpoint.authority()))
    } else {
        Ok(addrs)
    }
}

pub async fn connect_addr(
    addr: SocketAddr,
    options: UdpConnectOptions,
) -> Result<UdpSocket, UdpTransportError> {
    connect_addr_with_configurator(
        addr,
        options,
        NativeEgressPurpose::Target,
        &SystemNativeSocketConfigurator,
    )
    .await
}

pub async fn connect_addr_with_configurator(
    addr: SocketAddr,
    options: UdpConnectOptions,
    purpose: NativeEgressPurpose,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<UdpSocket, UdpTransportError> {
    let local_addr = match options.source_ip {
        Some(source_ip) => SocketAddr::new(source_ip, 0),
        None if addr.is_ipv4() => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        None => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
    };
    let socket = std::net::UdpSocket::bind(local_addr)?;
    socket.set_nonblocking(true)?;
    configurator.configure_udp(
        &socket,
        NativeSocketRequest {
            remote_addr: addr,
            purpose,
        },
    )?;
    let socket = UdpSocket::from_std(socket)?;
    timeout(options.timeout, socket.connect(addr))
        .await
        .map_err(|_| UdpTransportError::ConnectTimedOut(addr))??;
    Ok(socket)
}

#[derive(Debug)]
pub enum UdpTransportError {
    WrongUnderlay(UnderlayProtocol),
    ResolutionEmpty(String),
    NoCompatibleAddress,
    ConnectTimedOut(SocketAddr),
    Io(std::io::Error),
}

impl From<std::io::Error> for UdpTransportError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl std::fmt::Display for UdpTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongUnderlay(underlay) => {
                write!(f, "UDP transport cannot use {underlay:?} path")
            }
            Self::ResolutionEmpty(authority) => {
                write!(f, "no socket addresses resolved for {authority}")
            }
            Self::NoCompatibleAddress => {
                write!(f, "no resolved address is compatible with source binding")
            }
            Self::ConnectTimedOut(addr) => write!(f, "UDP connect to {addr} timed out"),
            Self::Io(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for UdpTransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

#[cfg(test)]
#[path = "tests_udp.rs"]
mod tests;
