//! TCP listener and connection establishment.
//!
//! Configured client paths resolve and create sockets through the host carrier
//! network; TCP alone owns its sequential address attempts and connect timeout.

use crate::protocol::UnderlayProtocol;
use crate::transport::{
    CarrierNetworkProvider, CarrierPathIdentity, CarrierResolutionRequest, CarrierSocketRequest,
    Endpoint, PathSpec, SystemCarrierNetworkProvider,
};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use tokio::net::{TcpListener, TcpSocket, TcpStream, lookup_host};
use tokio::time::timeout;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpConnectOptions {
    pub source_ip: Option<IpAddr>,
    pub timeout: Duration,
    pub nodelay: bool,
}

impl Default for TcpConnectOptions {
    fn default() -> Self {
        Self {
            source_ip: None,
            timeout: Duration::from_secs(10),
            nodelay: true,
        }
    }
}

pub async fn connect_path(
    path: &PathSpec,
    options: TcpConnectOptions,
) -> Result<TcpStream, TcpTransportError> {
    connect_path_with_provider(
        path,
        CarrierPathIdentity {
            group_ordinal: 0,
            path_ordinal: 0,
        },
        options,
        &SystemCarrierNetworkProvider,
    )
    .await
}

/// Connects a configured carrier through its host-selected network.
pub async fn connect_path_with_provider(
    path: &PathSpec,
    identity: CarrierPathIdentity,
    options: TcpConnectOptions,
    provider: &dyn CarrierNetworkProvider,
) -> Result<TcpStream, TcpTransportError> {
    if path.underlay != UnderlayProtocol::Tcp {
        return Err(TcpTransportError::WrongUnderlay(path.underlay));
    }
    let mut effective_path = path.clone();
    effective_path.binding.source_ip = match (path.binding.source_ip, options.source_ip) {
        (Some(configured), Some(requested)) if configured != requested => {
            return Err(TcpTransportError::ConflictingSourceBinding);
        }
        (configured, requested) => configured.or(requested),
    };
    let addrs = provider
        .resolve(CarrierResolutionRequest {
            path: &effective_path,
            identity,
        })
        .await?;
    if addrs.is_empty() {
        return Err(TcpTransportError::ResolutionEmpty(
            effective_path.endpoint.authority(),
        ));
    }
    let mut last_error = None;
    for addr in addrs {
        if effective_path
            .binding
            .source_ip
            .is_some_and(|source_ip| source_ip.is_ipv4() != addr.is_ipv4())
        {
            continue;
        }
        match connect_carrier_addr(&effective_path, identity, addr, options, provider).await {
            Ok(stream) => return Ok(stream),
            Err(err) => last_error = Some(err),
        }
    }
    Err(last_error.unwrap_or(TcpTransportError::NoCompatibleAddress))
}

pub async fn connect_endpoint(
    endpoint: &Endpoint,
    options: TcpConnectOptions,
) -> Result<TcpStream, TcpTransportError> {
    let addrs = resolve_endpoint(endpoint).await?;
    let mut last_error = None;
    for addr in addrs {
        if let Some(source_ip) = options.source_ip
            && source_ip.is_ipv4() != addr.is_ipv4()
        {
            continue;
        }
        match connect_addr(addr, options).await {
            Ok(stream) => return Ok(stream),
            Err(err) => last_error = Some(err),
        }
    }
    Err(last_error.unwrap_or(TcpTransportError::NoCompatibleAddress))
}

pub async fn bind_listener(path: &PathSpec) -> Result<TcpListener, TcpTransportError> {
    if path.underlay != UnderlayProtocol::Tcp {
        return Err(TcpTransportError::WrongUnderlay(path.underlay));
    }
    let listener = TcpListener::bind(path.endpoint.authority()).await?;
    Ok(listener)
}

async fn resolve_endpoint(endpoint: &Endpoint) -> Result<Vec<SocketAddr>, TcpTransportError> {
    let addrs = lookup_host((endpoint.host.as_str(), endpoint.port))
        .await?
        .collect::<Vec<_>>();
    if addrs.is_empty() {
        Err(TcpTransportError::ResolutionEmpty(endpoint.authority()))
    } else {
        Ok(addrs)
    }
}

pub async fn connect_addr(
    addr: SocketAddr,
    options: TcpConnectOptions,
) -> Result<TcpStream, TcpTransportError> {
    let connect = async {
        match options.source_ip {
            Some(source_ip) => {
                let socket = if addr.is_ipv4() {
                    TcpSocket::new_v4()?
                } else {
                    TcpSocket::new_v6()?
                };
                socket.bind(SocketAddr::new(source_ip, 0))?;
                socket.connect(addr).await
            }
            None => TcpStream::connect(addr).await,
        }
    };
    let stream = timeout(options.timeout, connect)
        .await
        .map_err(|_| TcpTransportError::ConnectTimedOut(addr))?
        .map_err(TcpTransportError::Io)?;
    stream.set_nodelay(options.nodelay)?;
    Ok(stream)
}

async fn connect_carrier_addr(
    path: &PathSpec,
    identity: CarrierPathIdentity,
    addr: SocketAddr,
    options: TcpConnectOptions,
    provider: &dyn CarrierNetworkProvider,
) -> Result<TcpStream, TcpTransportError> {
    let carrier = provider.create_socket(CarrierSocketRequest {
        path,
        identity,
        remote_addr: addr,
    })?;
    let socket = TcpSocket::from_std_stream(carrier.into_tcp_socket()?);
    let stream = timeout(options.timeout, socket.connect(addr))
        .await
        .map_err(|_| TcpTransportError::ConnectTimedOut(addr))?
        .map_err(TcpTransportError::Io)?;
    stream.set_nodelay(options.nodelay)?;
    Ok(stream)
}

#[derive(Debug)]
pub enum TcpTransportError {
    WrongUnderlay(UnderlayProtocol),
    ResolutionEmpty(String),
    NoCompatibleAddress,
    ConflictingSourceBinding,
    ConnectTimedOut(SocketAddr),
    Io(std::io::Error),
}

impl From<std::io::Error> for TcpTransportError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl std::fmt::Display for TcpTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongUnderlay(underlay) => {
                write!(f, "TCP transport cannot use {underlay:?} path")
            }
            Self::ResolutionEmpty(authority) => {
                write!(f, "no socket addresses resolved for {authority}")
            }
            Self::NoCompatibleAddress => {
                write!(f, "no resolved address is compatible with source binding")
            }
            Self::ConflictingSourceBinding => {
                write!(
                    f,
                    "path and connect options specify different source addresses"
                )
            }
            Self::ConnectTimedOut(addr) => write!(f, "TCP connect to {addr} timed out"),
            Self::Io(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for TcpTransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

#[cfg(test)]
#[path = "tcp_test.rs"]
mod tests;
