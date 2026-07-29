//! TCP listener and connection establishment.
//!
//! Configured client paths resolve and create sockets through the host carrier
//! network; TCP alone owns its staggered address race and absolute connect timeout.

use crate::protocol::UnderlayProtocol;
use crate::transport::{
    CarrierNetworkProvider, CarrierPathIdentity, CarrierResolutionRequest, CarrierSocketRequest,
    Endpoint, NativeEgressPurpose, NativeSocketConfigurator, NativeSocketRequest, PathSpec,
    SystemCarrierNetworkProvider, SystemNativeSocketConfigurator, interleave_socket_addr_families,
    validate_carrier_resolution_port,
};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use tokio::net::{TcpListener, TcpSocket, TcpStream, lookup_host};
use tokio::time::{Instant, sleep_until, timeout_at};

// RFC 8305's default bounds a blackholed family without starting every DNS
// answer at once. The remaining deadline shortens this for small budgets.
const TCP_ADDRESS_ATTEMPT_DELAY: Duration = Duration::from_millis(250);

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
    let effective_path = match (path.binding.source_ip, options.source_ip) {
        (Some(configured), Some(requested)) if configured != requested => {
            return Err(TcpTransportError::ConflictingSourceBinding);
        }
        (None, Some(requested)) => {
            let mut overridden_path = path.clone();
            overridden_path.binding.source_ip = Some(requested);
            Cow::Owned(overridden_path)
        }
        _ => Cow::Borrowed(path),
    };
    let deadline = Instant::now() + options.timeout;
    let authority = effective_path.endpoint.authority();
    let remote_port = effective_path.endpoint.ports().select().map_err(|error| {
        TcpTransportError::Io(std::io::Error::other(format!(
            "could not select a carrier port for {authority}: {error}"
        )))
    })?;
    let addrs = timeout_at(
        deadline,
        provider.resolve(CarrierResolutionRequest {
            path: effective_path.as_ref(),
            identity,
            remote_port,
        }),
    )
    .await
    .map_err(|_| TcpTransportError::ResolutionTimedOut(authority.clone()))??;
    let addrs = validate_carrier_resolution_port(addrs, remote_port)?;
    if addrs.is_empty() {
        return Err(TcpTransportError::ResolutionEmpty(authority));
    }
    let addrs = compatible_tcp_socket_addrs(addrs, effective_path.binding.source_ip);
    if addrs.is_empty() {
        return Err(TcpTransportError::NoCompatibleAddress);
    }
    race_tcp_address_attempts(addrs, deadline, |addr, deadline| {
        connect_carrier_addr_before(
            effective_path.as_ref(),
            identity,
            addr,
            options,
            deadline,
            provider,
        )
    })
    .await
}

pub async fn connect_endpoint(
    endpoint: &Endpoint,
    options: TcpConnectOptions,
) -> Result<TcpStream, TcpTransportError> {
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
    options: TcpConnectOptions,
    purpose: NativeEgressPurpose,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<TcpStream, TcpTransportError> {
    let deadline = Instant::now() + options.timeout;
    let addrs = resolve_endpoint(endpoint, deadline).await?;
    connect_addrs_before(addrs, options, purpose, configurator, deadline).await
}

/// Races an already-resolved address set with the same family filtering and
/// RFC 8305 staggering used by endpoint dialing.
pub async fn connect_addrs(
    addrs: Vec<SocketAddr>,
    options: TcpConnectOptions,
) -> Result<TcpStream, TcpTransportError> {
    connect_addrs_with_configurator(
        addrs,
        options,
        NativeEgressPurpose::Target,
        &SystemNativeSocketConfigurator,
    )
    .await
}

pub async fn connect_addrs_with_configurator(
    addrs: Vec<SocketAddr>,
    options: TcpConnectOptions,
    purpose: NativeEgressPurpose,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<TcpStream, TcpTransportError> {
    connect_addrs_before(
        addrs,
        options,
        purpose,
        configurator,
        Instant::now() + options.timeout,
    )
    .await
}

async fn connect_addrs_before(
    addrs: Vec<SocketAddr>,
    options: TcpConnectOptions,
    purpose: NativeEgressPurpose,
    configurator: &dyn NativeSocketConfigurator,
    deadline: Instant,
) -> Result<TcpStream, TcpTransportError> {
    let addrs = compatible_tcp_socket_addrs(addrs, options.source_ip);
    if addrs.is_empty() {
        return Err(TcpTransportError::NoCompatibleAddress);
    }
    race_tcp_address_attempts(addrs, deadline, |addr, deadline| {
        connect_addr_before(addr, options, purpose, configurator, deadline)
    })
    .await
}

pub async fn bind_listener(path: &PathSpec) -> Result<TcpListener, TcpTransportError> {
    if path.underlay != UnderlayProtocol::Tcp {
        return Err(TcpTransportError::WrongUnderlay(path.underlay));
    }
    if !path.endpoint.ports().is_single() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "server carrier paths require one listener port; forward any advertised port range to that listener",
        )
        .into());
    }
    let listener = TcpListener::bind(path.endpoint.first_endpoint().authority()).await?;
    Ok(listener)
}

async fn resolve_endpoint(
    endpoint: &Endpoint,
    deadline: Instant,
) -> Result<Vec<SocketAddr>, TcpTransportError> {
    let authority = endpoint.authority();
    let addrs = timeout_at(
        deadline,
        lookup_host((endpoint.host.as_str(), endpoint.port)),
    )
    .await
    .map_err(|_| TcpTransportError::ResolutionTimedOut(authority.clone()))??
    .collect::<Vec<_>>();
    if addrs.is_empty() {
        Err(TcpTransportError::ResolutionEmpty(authority))
    } else {
        Ok(addrs)
    }
}

pub async fn connect_addr(
    addr: SocketAddr,
    options: TcpConnectOptions,
) -> Result<TcpStream, TcpTransportError> {
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
    options: TcpConnectOptions,
    purpose: NativeEgressPurpose,
    configurator: &dyn NativeSocketConfigurator,
) -> Result<TcpStream, TcpTransportError> {
    connect_addr_before(
        addr,
        options,
        purpose,
        configurator,
        Instant::now() + options.timeout,
    )
    .await
}

async fn connect_addr_before(
    addr: SocketAddr,
    options: TcpConnectOptions,
    purpose: NativeEgressPurpose,
    configurator: &dyn NativeSocketConfigurator,
    deadline: Instant,
) -> Result<TcpStream, TcpTransportError> {
    let connect = async {
        let socket = if addr.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };
        if let Some(source_ip) = options.source_ip {
            socket.bind(SocketAddr::new(source_ip, 0))?;
        }
        configurator.configure_tcp(
            &socket,
            NativeSocketRequest {
                remote_addr: addr,
                purpose,
            },
        )?;
        socket.connect(addr).await
    };
    let stream = timeout_at(deadline, connect)
        .await
        .map_err(|_| TcpTransportError::ConnectTimedOut(addr))?
        .map_err(TcpTransportError::Io)?;
    stream.set_nodelay(options.nodelay)?;
    Ok(stream)
}

async fn connect_carrier_addr_before(
    path: &PathSpec,
    identity: CarrierPathIdentity,
    addr: SocketAddr,
    options: TcpConnectOptions,
    deadline: Instant,
    provider: &dyn CarrierNetworkProvider,
) -> Result<TcpStream, TcpTransportError> {
    let carrier = provider.create_socket(CarrierSocketRequest {
        path,
        identity,
        remote_addr: addr,
    })?;
    let socket = TcpSocket::from_std_stream(carrier.into_tcp_socket()?);
    let stream = timeout_at(deadline, socket.connect(addr))
        .await
        .map_err(|_| TcpTransportError::ConnectTimedOut(addr))?
        .map_err(TcpTransportError::Io)?;
    stream.set_nodelay(options.nodelay)?;
    Ok(stream)
}

fn compatible_tcp_socket_addrs(
    resolved: impl IntoIterator<Item = SocketAddr>,
    source_ip: Option<IpAddr>,
) -> Vec<SocketAddr> {
    let mut compatible = Vec::new();
    for addr in resolved {
        if source_ip.is_some_and(|source| source.is_ipv4() != addr.is_ipv4())
            || compatible.contains(&addr)
        {
            continue;
        }
        compatible.push(addr);
    }
    compatible
}

fn tcp_address_attempt_delay(remaining: Duration, unstarted: usize) -> Duration {
    debug_assert!(unstarted > 0 && unstarted < u32::MAX as usize);
    (remaining / (unstarted as u32 + 1)).min(TCP_ADDRESS_ATTEMPT_DELAY)
}

fn next_tcp_address_attempt_at(deadline: Instant, unstarted: usize) -> Instant {
    let now = Instant::now();
    now + tcp_address_attempt_delay(deadline.saturating_duration_since(now), unstarted)
}

async fn race_tcp_address_attempts<T, F, Fut>(
    addrs: Vec<SocketAddr>,
    deadline: Instant,
    mut connect: F,
) -> Result<T, TcpTransportError>
where
    F: FnMut(SocketAddr, Instant) -> Fut,
    Fut: Future<Output = Result<T, TcpTransportError>>,
{
    let mut unstarted = interleave_socket_addr_families(addrs)
        .into_iter()
        .collect::<VecDeque<_>>();
    let first = unstarted
        .pop_front()
        .expect("compatible address set is non-empty");
    let mut last_started = first;
    let mut attempts = FuturesUnordered::new();
    attempts.push(connect(first, deadline));
    let mut next_attempt_at =
        (!unstarted.is_empty()).then(|| next_tcp_address_attempt_at(deadline, unstarted.len()));
    let mut last_error = None;

    loop {
        let completed = if unstarted.is_empty() {
            tokio::select! {
                completed = attempts.next() => completed,
                _ = sleep_until(deadline) => {
                    return Err(last_error.unwrap_or(
                        TcpTransportError::ConnectTimedOut(last_started)
                    ));
                }
            }
        } else {
            tokio::select! {
                biased;
                completed = attempts.next() => completed,
                _ = sleep_until(next_attempt_at.expect("unstarted address has launch time")) => {
                    if Instant::now() >= deadline {
                        return Err(last_error.unwrap_or(
                            TcpTransportError::ConnectTimedOut(last_started)
                        ));
                    }
                    last_started = unstarted.pop_front().expect("address availability checked");
                    attempts.push(connect(last_started, deadline));
                    next_attempt_at = (!unstarted.is_empty())
                        .then(|| next_tcp_address_attempt_at(deadline, unstarted.len()));
                    continue;
                }
                _ = sleep_until(deadline) => {
                    return Err(last_error.unwrap_or(
                        TcpTransportError::ConnectTimedOut(last_started)
                    ));
                }
            }
        };

        match completed {
            Some(Ok(stream)) => return Ok(stream),
            Some(Err(err)) => {
                last_error = Some(err);
                if attempts.is_empty()
                    && let Some(addr) = unstarted.pop_front()
                {
                    last_started = addr;
                    attempts.push(connect(addr, deadline));
                    next_attempt_at = (!unstarted.is_empty())
                        .then(|| next_tcp_address_attempt_at(deadline, unstarted.len()));
                }
            }
            None => {
                return Err(last_error.unwrap_or(TcpTransportError::NoCompatibleAddress));
            }
        }
    }
}

#[derive(Debug)]
pub enum TcpTransportError {
    WrongUnderlay(UnderlayProtocol),
    ResolutionEmpty(String),
    ResolutionTimedOut(String),
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
            Self::ResolutionTimedOut(authority) => {
                write!(f, "TCP resolution for {authority} timed out")
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
