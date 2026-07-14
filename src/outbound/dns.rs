use hickory_resolver::Resolver;
use hickory_resolver::config::{
    ConnectionConfig, LookupIpStrategy, NameServerConfig, ProtocolConfig, ResolverConfig,
    ResolverOpts,
};
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use tokio::net::lookup_host;

pub const DEFAULT_OUTBOUND_DNS_TIMEOUT_MS: u64 = 5_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsConfig {
    pub resolvers: Vec<SocketAddr>,
    pub strategy: DnsIpStrategy,
    pub timeout: Duration,
}

impl Default for DnsConfig {
    fn default() -> Self {
        Self {
            resolvers: Vec::new(),
            strategy: DnsIpStrategy::Ipv4ThenIpv6,
            timeout: Duration::from_millis(DEFAULT_OUTBOUND_DNS_TIMEOUT_MS),
        }
    }
}

impl DnsConfig {
    pub fn uses_system_resolver(&self) -> bool {
        self.resolvers.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DnsIpStrategy {
    Ipv4ThenIpv6,
    Ipv6ThenIpv4,
    Ipv4Only,
    Ipv6Only,
    Ipv4AndIpv6,
    Ipv6AndIpv4,
}

impl DnsIpStrategy {
    fn into_hickory(self) -> LookupIpStrategy {
        match self {
            Self::Ipv4ThenIpv6 => LookupIpStrategy::Ipv4thenIpv6,
            Self::Ipv6ThenIpv4 => LookupIpStrategy::Ipv6thenIpv4,
            Self::Ipv4Only => LookupIpStrategy::Ipv4Only,
            Self::Ipv6Only => LookupIpStrategy::Ipv6Only,
            Self::Ipv4AndIpv6 => LookupIpStrategy::Ipv4AndIpv6,
            Self::Ipv6AndIpv4 => LookupIpStrategy::Ipv6AndIpv4,
        }
    }
}

pub async fn resolve_socket_addrs(
    host: &str,
    port: u16,
    config: &DnsConfig,
) -> Result<Vec<SocketAddr>, DnsError> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }
    if config.uses_system_resolver() {
        return resolve_with_system(host, port).await;
    }
    let resolver = custom_resolver(config)?;
    let lookup = tokio::time::timeout(config.timeout, resolver.lookup_ip(host))
        .await
        .map_err(|_| DnsError::Timeout)?
        .map_err(|err| DnsError::Lookup(err.to_string()))?;
    let addrs = lookup
        .iter()
        .map(|ip| SocketAddr::new(ip, port))
        .collect::<Vec<_>>();
    if addrs.is_empty() {
        Err(DnsError::Empty(host.to_string()))
    } else {
        Ok(addrs)
    }
}

async fn resolve_with_system(host: &str, port: u16) -> Result<Vec<SocketAddr>, DnsError> {
    let addrs = lookup_host((host, port)).await?.collect::<Vec<_>>();
    if addrs.is_empty() {
        Err(DnsError::Empty(format!("{host}:{port}")))
    } else {
        Ok(addrs)
    }
}

fn custom_resolver(config: &DnsConfig) -> Result<Resolver<TokioRuntimeProvider>, DnsError> {
    let name_servers = config
        .resolvers
        .iter()
        .copied()
        .map(name_server_config)
        .collect::<Vec<_>>();
    let resolver_config = ResolverConfig::from_parts(None, Vec::new(), name_servers);
    let mut options = ResolverOpts::default();
    options.ip_strategy = config.strategy.into_hickory();
    options.timeout = config.timeout;
    Resolver::builder_with_config(resolver_config, TokioRuntimeProvider::default())
        .with_options(options)
        .build()
        .map_err(|err| DnsError::Lookup(err.to_string()))
}

fn name_server_config(addr: SocketAddr) -> NameServerConfig {
    let mut udp = ConnectionConfig::new(ProtocolConfig::Udp);
    udp.port = addr.port();
    let mut tcp = ConnectionConfig::new(ProtocolConfig::Tcp);
    tcp.port = addr.port();
    NameServerConfig::new(addr.ip(), true, vec![udp, tcp])
}

#[derive(Debug)]
pub enum DnsError {
    Io(std::io::Error),
    Timeout,
    Lookup(String),
    Empty(String),
}

impl From<std::io::Error> for DnsError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl std::fmt::Display for DnsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Timeout => write!(f, "DNS lookup timed out"),
            Self::Lookup(err) => write!(f, "DNS lookup failed: {err}"),
            Self::Empty(authority) => write!(f, "no DNS records resolved for {authority}"),
        }
    }
}

impl std::error::Error for DnsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::Timeout | Self::Lookup(_) | Self::Empty(_) => None,
        }
    }
}

#[cfg(test)]
#[path = "dns_test.rs"]
mod tests;
