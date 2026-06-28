use crate::config::{
    AppConfig, CipherSuite, ClientConfig, CommandConfig, DEFAULT_AUTH_FRESHNESS_WINDOW_SECONDS,
    DEFAULT_DATAGRAM_QUEUE_BYTES, DEFAULT_MAX_TCP_RELAY_CHUNK_BYTES,
    DEFAULT_PATH_PROBE_INTERVAL_MS, DEFAULT_PATH_PROBE_TIMEOUT_MS, DEFAULT_REORDER_BYTES,
    DEFAULT_REPAIR_BYTES, DEFAULT_RESTART_BACKOFF_MS, DEFAULT_RESTART_MAX_BACKOFF_MS,
    DEFAULT_STREAM_WINDOW_BYTES, DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL_MS,
    DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT_MS, DEFAULT_TCP_PATH_INFLIGHT_BYTES, ResourceLimits,
    SecurityConfig, ServerConfig, ServiceConfig, SharedSecret,
};
use crate::ingress::tun::{DEFAULT_TUN_DNS_TTL_MS, DEFAULT_TUN_MTU, TunL4Config};
use crate::ingress::{IngressConfig, ProxyAuthConfig};
use crate::outbound::{DnsConfig, DnsIpStrategy, OutboundConfig};
use crate::transport::{Endpoint, PathSpec};
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

#[derive(Debug, Parser)]
#[command(name = "mptunnel")]
#[command(about = "Encrypted multipath proxy and tunnel")]
#[command(version)]
pub struct Cli {
    #[arg(long, global = true, env = "MPTUNNEL_LOG", default_value = "info")]
    pub log_level: String,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_SECURITY",
        value_enum,
        default_value_t = SecurityArg::Encrypted
    )]
    pub security: SecurityArg,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_I_UNDERSTAND_THIS_IS_INSECURE",
        default_value_t = false
    )]
    pub i_understand_this_is_insecure: bool,

    #[arg(long, global = true, env = "MPTUNNEL_SECRET")]
    pub secret: Option<String>,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_CIPHER",
        value_enum,
        default_value_t = CipherArg::Aes256Gcm
    )]
    pub cipher: CipherArg,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_AUTH_FRESHNESS_WINDOW_SECONDS",
        default_value_t = DEFAULT_AUTH_FRESHNESS_WINDOW_SECONDS
    )]
    pub auth_freshness_window_seconds: u64,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_CHECK_CONFIG",
        default_value_t = false
    )]
    pub check_config: bool,

    #[command(flatten)]
    pub resources: ResourceArgs,

    #[command(flatten)]
    pub service: ServiceArgs,

    #[command(subcommand)]
    pub command: Command,
}

impl Cli {
    pub fn into_config(self) -> Result<AppConfig, CliConfigError> {
        let security = self.security.into_config(
            self.i_understand_this_is_insecure,
            self.secret,
            self.cipher,
            self.auth_freshness_window_seconds,
        )?;
        let command = match self.command {
            Command::Client(args) => CommandConfig::Client(args.into_config()?),
            Command::Server(args) => CommandConfig::Server(args.into_config()?),
            Command::Platform(_) => return Err(CliConfigError::PlatformCommandNotRuntimeConfig),
        };
        let config = AppConfig {
            log_level: self.log_level,
            check_config: self.check_config,
            service: self.service.into_config(),
            resources: self.resources.into_limits(),
            security,
            command,
        };
        config.validate()?;
        Ok(config)
    }
}

#[derive(Debug, Args)]
pub struct ServiceArgs {
    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_SERVICE_MODE",
        default_value_t = false
    )]
    pub service_mode: bool,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_SUPERVISE",
        default_value_t = false
    )]
    pub supervise: bool,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_RESTART_BACKOFF_MS",
        default_value_t = DEFAULT_RESTART_BACKOFF_MS
    )]
    pub restart_backoff_ms: u64,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_RESTART_MAX_BACKOFF_MS",
        default_value_t = DEFAULT_RESTART_MAX_BACKOFF_MS
    )]
    pub restart_max_backoff_ms: u64,

    #[arg(long, global = true, env = "MPTUNNEL_MAX_RESTARTS")]
    pub max_restarts: Option<u32>,
}

impl ServiceArgs {
    fn into_config(self) -> ServiceConfig {
        ServiceConfig {
            service_mode: self.service_mode,
            supervise: self.supervise,
            restart_backoff: Duration::from_millis(self.restart_backoff_ms),
            restart_max_backoff: Duration::from_millis(self.restart_max_backoff_ms),
            max_restarts: self.max_restarts,
        }
    }
}

#[derive(Debug, Args)]
pub struct ResourceArgs {
    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_FRAME_BYTES",
        default_value_t = 1_048_576
    )]
    pub max_frame_bytes: usize,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_PAYLOAD_BYTES",
        default_value_t = 1_048_512
    )]
    pub max_payload_bytes: usize,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_ACK_RANGES",
        default_value_t = 256
    )]
    pub max_ack_ranges: usize,

    #[arg(long, global = true, env = "MPTUNNEL_MAX_PATHS", default_value_t = 64)]
    pub max_paths: usize,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_STREAMS",
        default_value_t = 65_536
    )]
    pub max_streams: usize,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_STREAM_WINDOW_BYTES",
        default_value_t = DEFAULT_STREAM_WINDOW_BYTES
    )]
    pub max_stream_window_bytes: u64,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_REPAIR_BYTES",
        default_value_t = DEFAULT_REPAIR_BYTES
    )]
    pub max_repair_bytes: usize,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_REORDER_BYTES",
        default_value_t = DEFAULT_REORDER_BYTES
    )]
    pub max_reorder_bytes: usize,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_DATAGRAM_QUEUE_BYTES",
        default_value_t = DEFAULT_DATAGRAM_QUEUE_BYTES
    )]
    pub max_datagram_queue_bytes: usize,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_TCP_PATH_INFLIGHT_BYTES",
        default_value_t = DEFAULT_TCP_PATH_INFLIGHT_BYTES
    )]
    pub max_tcp_path_inflight_bytes: usize,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_TCP_RELAY_CHUNK_BYTES",
        default_value_t = DEFAULT_MAX_TCP_RELAY_CHUNK_BYTES
    )]
    pub max_tcp_relay_chunk_bytes: usize,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_TCP_PATH_HEARTBEAT_INTERVAL_MS",
        default_value_t = DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL_MS
    )]
    pub tcp_path_heartbeat_interval_ms: u64,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_TCP_PATH_HEARTBEAT_TIMEOUT_MS",
        default_value_t = DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT_MS
    )]
    pub tcp_path_heartbeat_timeout_ms: u64,
}

impl ResourceArgs {
    fn into_limits(self) -> ResourceLimits {
        ResourceLimits {
            max_frame_bytes: self.max_frame_bytes,
            max_payload_bytes: self.max_payload_bytes,
            max_ack_ranges: self.max_ack_ranges,
            max_paths: self.max_paths,
            max_streams: self.max_streams,
            max_stream_window_bytes: self.max_stream_window_bytes,
            max_repair_bytes: self.max_repair_bytes,
            max_reorder_bytes: self.max_reorder_bytes,
            max_datagram_queue_bytes: self.max_datagram_queue_bytes,
            max_tcp_path_inflight_bytes: self.max_tcp_path_inflight_bytes,
            max_tcp_relay_chunk_bytes: self.max_tcp_relay_chunk_bytes,
            tcp_path_heartbeat_interval: Duration::from_millis(self.tcp_path_heartbeat_interval_ms),
            tcp_path_heartbeat_timeout: Duration::from_millis(self.tcp_path_heartbeat_timeout_ms),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SecurityArg {
    Encrypted,
    PlaintextLab,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CipherArg {
    #[value(name = "aes-256-gcm", alias = "aes256-gcm")]
    Aes256Gcm,
    Chacha20Poly1305,
}

impl From<CipherArg> for CipherSuite {
    fn from(value: CipherArg) -> Self {
        match value {
            CipherArg::Aes256Gcm => Self::Aes256Gcm,
            CipherArg::Chacha20Poly1305 => Self::Chacha20Poly1305,
        }
    }
}

impl SecurityArg {
    fn into_config(
        self,
        acknowledged: bool,
        secret: Option<String>,
        cipher: CipherArg,
        auth_freshness_window_seconds: u64,
    ) -> Result<SecurityConfig, CliConfigError> {
        if matches!(self, Self::PlaintextLab) && !acknowledged {
            return Err(CliConfigError::PlaintextNotAcknowledged);
        }
        let secret = SharedSecret::new(
            secret
                .ok_or(CliConfigError::Security(
                    crate::config::SecurityPolicyError::MissingSecret,
                ))?
                .into_bytes(),
        )?;
        let cipher = cipher.into();
        let auth_freshness_window = Duration::from_secs(auth_freshness_window_seconds);
        match self {
            Self::Encrypted => Ok(SecurityConfig::encrypted_with_cipher(secret, cipher)
                .with_auth_freshness_window(auth_freshness_window)),
            Self::PlaintextLab => Ok(SecurityConfig::plaintext_lab_with_cipher(secret, cipher)
                .with_auth_freshness_window(auth_freshness_window)),
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the local proxy/TUN ingress side.
    Client(ClientArgs),
    /// Run the remote path listener and outbound connector side.
    Server(ServerArgs),
    /// Print platform, TUN, service, and release-target information.
    Platform(PlatformArgs),
}

#[derive(Debug, Args)]
pub struct PlatformArgs {}

#[derive(Debug, Args)]
pub struct ClientArgs {
    #[arg(long = "listen", env = "MPTUNNEL_LISTEN", value_delimiter = ',')]
    pub listen: Vec<SocketAddr>,

    #[arg(
        long = "socks5-listen",
        env = "MPTUNNEL_SOCKS5_LISTEN",
        value_delimiter = ','
    )]
    pub socks5_listen: Vec<SocketAddr>,

    #[arg(
        long = "http-listen",
        env = "MPTUNNEL_HTTP_LISTEN",
        value_delimiter = ','
    )]
    pub http_listen: Vec<SocketAddr>,

    #[arg(long, env = "MPTUNNEL_PROXY_USERNAME")]
    pub proxy_username: Option<String>,

    #[arg(long, env = "MPTUNNEL_PROXY_PASSWORD")]
    pub proxy_password: Option<String>,

    #[arg(long, env = "MPTUNNEL_TUN", default_value_t = false)]
    pub tun: bool,

    #[arg(long, env = "MPTUNNEL_TUN_NAME")]
    pub tun_name: Option<String>,

    #[arg(long, env = "MPTUNNEL_TUN_IPV4")]
    pub tun_ipv4: Option<Ipv4Addr>,

    #[arg(long, env = "MPTUNNEL_TUN_DISABLE_IPV4", default_value_t = false)]
    pub tun_disable_ipv4: bool,

    #[arg(long, env = "MPTUNNEL_TUN_IPV4_PREFIX", default_value_t = crate::ingress::tun::DEFAULT_TUN_IPV4_PREFIX)]
    pub tun_ipv4_prefix: u8,

    #[arg(long, env = "MPTUNNEL_TUN_IPV4_GATEWAY")]
    pub tun_ipv4_gateway: Option<Ipv4Addr>,

    #[arg(long, env = "MPTUNNEL_TUN_IPV6")]
    pub tun_ipv6: Option<Ipv6Addr>,

    #[arg(long, env = "MPTUNNEL_TUN_IPV6_PREFIX", default_value_t = 64)]
    pub tun_ipv6_prefix: u8,

    #[arg(long, env = "MPTUNNEL_TUN_MTU", default_value_t = DEFAULT_TUN_MTU)]
    pub tun_mtu: u16,

    #[arg(long, env = "MPTUNNEL_TUN_DISABLE_ICMP", default_value_t = false)]
    pub tun_disable_icmp: bool,

    #[arg(
        long = "tun-dns-resolver",
        env = "MPTUNNEL_TUN_DNS_RESOLVERS",
        value_delimiter = ','
    )]
    pub tun_dns_resolvers: Vec<SocketAddr>,

    #[arg(long, env = "MPTUNNEL_TUN_DNS_TTL_MS", default_value_t = DEFAULT_TUN_DNS_TTL_MS)]
    pub tun_dns_ttl_ms: u32,

    #[arg(
        long = "path",
        env = "MPTUNNEL_PATHS",
        value_delimiter = ',',
        required = true
    )]
    pub paths: Vec<PathSpec>,

    #[arg(
        long,
        env = "MPTUNNEL_PATH_PROBE_INTERVAL_MS",
        default_value_t = DEFAULT_PATH_PROBE_INTERVAL_MS
    )]
    pub path_probe_interval_ms: u64,

    #[arg(
        long,
        env = "MPTUNNEL_PATH_PROBE_TIMEOUT_MS",
        default_value_t = DEFAULT_PATH_PROBE_TIMEOUT_MS
    )]
    pub path_probe_timeout_ms: u64,
}

impl ClientArgs {
    fn into_config(self) -> Result<ClientConfig, CliConfigError> {
        let socks5_listen = combined_socks5_listen(&self);
        let http_connect_enabled = !self.http_listen.is_empty();
        let tun_enabled = tun_requested(&self);
        let socks5_enabled = !socks5_listen.is_empty() || (!http_connect_enabled && !tun_enabled);

        let mut ingresses = Vec::with_capacity(3);
        if socks5_enabled {
            ingresses.push(IngressConfig::Socks5 {
                listen: listen_or_default(socks5_listen, 1080),
            });
        }
        if http_connect_enabled {
            ingresses.push(IngressConfig::HttpConnect {
                listen: self.http_listen.clone(),
            });
        }
        if tun_enabled {
            let tun_ipv4 = if self.tun_disable_ipv4 {
                if self.tun_ipv4.is_some() || self.tun_ipv4_gateway.is_some() {
                    return Err(CliConfigError::TunIpv4DisabledWithIpv4Options);
                }
                None
            } else {
                Some(
                    self.tun_ipv4
                        .unwrap_or(crate::ingress::tun::DEFAULT_TUN_IPV4),
                )
            };
            ingresses.push(IngressConfig::TunL4(TunL4Config {
                name: self.tun_name.clone(),
                ipv4: tun_ipv4,
                ipv4_prefix: self.tun_ipv4_prefix,
                ipv4_gateway: self.tun_ipv4_gateway,
                ipv6: self.tun_ipv6,
                ipv6_prefix: self.tun_ipv6_prefix,
                mtu: self.tun_mtu,
                enable_icmp: !self.tun_disable_icmp,
                dns_resolvers: self.tun_dns_resolvers.clone(),
                dns_ttl_ms: self.tun_dns_ttl_ms,
            }));
        }
        Ok(ClientConfig {
            ingresses,
            proxy_auth: proxy_auth_config(self.proxy_username, self.proxy_password)?,
            paths: self.paths,
            path_probe_interval: Duration::from_millis(self.path_probe_interval_ms),
            path_probe_timeout: Duration::from_millis(self.path_probe_timeout_ms),
        })
    }
}

fn proxy_auth_config(
    username: Option<String>,
    password: Option<String>,
) -> Result<ProxyAuthConfig, CliConfigError> {
    match (username, password) {
        (Some(username), Some(password)) => Ok(ProxyAuthConfig::required(username, password)),
        (None, None) => Ok(ProxyAuthConfig::disabled()),
        (Some(_), None) => Err(CliConfigError::ProxyPasswordRequired),
        (None, Some(_)) => Err(CliConfigError::ProxyUsernameRequired),
    }
}

fn combined_socks5_listen(args: &ClientArgs) -> Vec<SocketAddr> {
    let mut listen = args.listen.clone();
    listen.extend(args.socks5_listen.iter().copied());
    listen
}

fn listen_or_default(listen: Vec<SocketAddr>, port: u16) -> Vec<SocketAddr> {
    if listen.is_empty() {
        vec![SocketAddr::from(([127, 0, 0, 1], port))]
    } else {
        listen
    }
}

fn tun_requested(args: &ClientArgs) -> bool {
    args.tun
        || args.tun_name.is_some()
        || args.tun_ipv4.is_some()
        || args.tun_disable_ipv4
        || args.tun_ipv4_prefix != crate::ingress::tun::DEFAULT_TUN_IPV4_PREFIX
        || args.tun_ipv4_gateway.is_some()
        || args.tun_ipv6.is_some()
        || args.tun_ipv6_prefix != 64
        || args.tun_mtu != DEFAULT_TUN_MTU
        || args.tun_disable_icmp
        || !args.tun_dns_resolvers.is_empty()
        || args.tun_dns_ttl_ms != DEFAULT_TUN_DNS_TTL_MS
}

#[derive(Debug, Args)]
pub struct ServerArgs {
    #[arg(
        long = "bind-path",
        env = "MPTUNNEL_BIND_PATHS",
        value_delimiter = ',',
        required = true
    )]
    pub bind_paths: Vec<PathSpec>,

    #[arg(long, env = "MPTUNNEL_OUTBOUND", value_enum, default_value_t = OutboundArg::Direct)]
    pub outbound: OutboundArg,

    #[arg(long, env = "MPTUNNEL_OUTBOUND_BIND_IP")]
    pub outbound_bind_ip: Option<IpAddr>,

    #[arg(long, env = "MPTUNNEL_UPSTREAM_SOCKS5")]
    pub upstream_socks5: Option<Endpoint>,

    #[arg(long, env = "MPTUNNEL_UPSTREAM_HTTP")]
    pub upstream_http: Option<Endpoint>,

    #[arg(
        long = "outbound-dns-resolver",
        env = "MPTUNNEL_OUTBOUND_DNS_RESOLVERS",
        value_delimiter = ','
    )]
    pub outbound_dns_resolvers: Vec<SocketAddr>,

    #[arg(
        long,
        env = "MPTUNNEL_OUTBOUND_DNS_STRATEGY",
        value_enum,
        default_value_t = DnsStrategyArg::Ipv4ThenIpv6
    )]
    pub outbound_dns_strategy: DnsStrategyArg,

    #[arg(
        long,
        env = "MPTUNNEL_OUTBOUND_DNS_TIMEOUT_MS",
        default_value_t = crate::outbound::dns::DEFAULT_OUTBOUND_DNS_TIMEOUT_MS
    )]
    pub outbound_dns_timeout_ms: u64,
}

impl ServerArgs {
    fn into_config(self) -> Result<ServerConfig, CliConfigError> {
        let outbound = match self.outbound {
            OutboundArg::Direct => OutboundConfig::Direct,
            OutboundArg::Bind => OutboundConfig::BindSourceIp(
                self.outbound_bind_ip
                    .ok_or(CliConfigError::MissingOutboundBindIp)?,
            ),
            OutboundArg::Socks5 => OutboundConfig::Socks5 {
                proxy: self
                    .upstream_socks5
                    .ok_or(CliConfigError::MissingUpstreamSocks5)?,
            },
            OutboundArg::HttpConnect => OutboundConfig::HttpConnect {
                proxy: self
                    .upstream_http
                    .ok_or(CliConfigError::MissingUpstreamHttp)?,
            },
            OutboundArg::HttpConnectUdp => OutboundConfig::HttpConnectUdp {
                proxy: self
                    .upstream_http
                    .ok_or(CliConfigError::MissingUpstreamHttp)?,
            },
        };
        Ok(ServerConfig {
            bind_paths: self.bind_paths,
            outbound,
            outbound_dns: DnsConfig {
                resolvers: self.outbound_dns_resolvers,
                strategy: self.outbound_dns_strategy.into(),
                timeout: Duration::from_millis(self.outbound_dns_timeout_ms),
            },
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum DnsStrategyArg {
    Ipv4ThenIpv6,
    Ipv6ThenIpv4,
    Ipv4Only,
    Ipv6Only,
    Ipv4AndIpv6,
    Ipv6AndIpv4,
}

impl From<DnsStrategyArg> for DnsIpStrategy {
    fn from(value: DnsStrategyArg) -> Self {
        match value {
            DnsStrategyArg::Ipv4ThenIpv6 => Self::Ipv4ThenIpv6,
            DnsStrategyArg::Ipv6ThenIpv4 => Self::Ipv6ThenIpv4,
            DnsStrategyArg::Ipv4Only => Self::Ipv4Only,
            DnsStrategyArg::Ipv6Only => Self::Ipv6Only,
            DnsStrategyArg::Ipv4AndIpv6 => Self::Ipv4AndIpv6,
            DnsStrategyArg::Ipv6AndIpv4 => Self::Ipv6AndIpv4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutboundArg {
    Direct,
    Bind,
    Socks5,
    HttpConnect,
    HttpConnectUdp,
}

#[derive(Debug)]
pub enum CliConfigError {
    Config(crate::config::ConfigError),
    Security(crate::config::SecurityPolicyError),
    PlaintextNotAcknowledged,
    MissingOutboundBindIp,
    MissingUpstreamSocks5,
    MissingUpstreamHttp,
    TunIpv4DisabledWithIpv4Options,
    ProxyUsernameRequired,
    ProxyPasswordRequired,
    PlatformCommandNotRuntimeConfig,
}

impl From<crate::config::ConfigError> for CliConfigError {
    fn from(value: crate::config::ConfigError) -> Self {
        Self::Config(value)
    }
}

impl From<crate::config::SecurityPolicyError> for CliConfigError {
    fn from(value: crate::config::SecurityPolicyError) -> Self {
        Self::Security(value)
    }
}

impl std::fmt::Display for CliConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(err) => write!(f, "{err}"),
            Self::Security(err) => write!(f, "{err}"),
            Self::PlaintextNotAcknowledged => write!(
                f,
                "plaintext lab mode requires --i-understand-this-is-insecure"
            ),
            Self::MissingOutboundBindIp => {
                write!(f, "--outbound bind requires --outbound-bind-ip")
            }
            Self::MissingUpstreamSocks5 => {
                write!(f, "--outbound socks5 requires --upstream-socks5")
            }
            Self::MissingUpstreamHttp => {
                write!(
                    f,
                    "--outbound http-connect/http-connect-udp requires --upstream-http"
                )
            }
            Self::TunIpv4DisabledWithIpv4Options => {
                write!(
                    f,
                    "--tun-disable-ipv4 cannot be combined with --tun-ipv4 or --tun-ipv4-gateway"
                )
            }
            Self::ProxyUsernameRequired => {
                write!(f, "--proxy-password requires --proxy-username")
            }
            Self::ProxyPasswordRequired => {
                write!(f, "--proxy-username requires --proxy-password")
            }
            Self::PlatformCommandNotRuntimeConfig => {
                write!(f, "platform command does not build a runtime config")
            }
        }
    }
}

impl std::error::Error for CliConfigError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CipherSuite, CommandConfig, TransportSecurity};
    use clap::Parser;

    #[test]
    fn client_cli_builds_default_socks_config() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--check-config",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "client",
            "--path",
            "tcp://127.0.0.1:443",
            "--path",
            "udp://127.0.0.1:443",
        ])
        .expect("parse cli");
        let config = cli.into_config().expect("config");

        assert_eq!(config.security.transport, TransportSecurity::Encrypted);
        assert_eq!(config.security.cipher, CipherSuite::Aes256Gcm);
        assert!(config.check_config);
        assert_eq!(config.service, ServiceConfig::default());
        assert_eq!(config.resources, ResourceLimits::default());
        match config.command {
            CommandConfig::Client(client) => {
                assert_eq!(client.paths.len(), 2);
                assert_eq!(
                    client.ingresses,
                    vec![IngressConfig::Socks5 {
                        listen: vec!["127.0.0.1:1080".parse().expect("listen")]
                    }]
                );
                assert_eq!(
                    client.path_probe_interval,
                    crate::config::DEFAULT_PATH_PROBE_INTERVAL
                );
                assert_eq!(
                    client.path_probe_timeout,
                    crate::config::DEFAULT_PATH_PROBE_TIMEOUT
                );
            }
            CommandConfig::Server(_) => panic!("expected client config"),
        }
    }

    #[test]
    fn client_proxy_cli_accepts_multiple_dual_stack_listen_addresses() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "client",
            "--http-listen",
            "127.0.0.1:8080",
            "--http-listen",
            "[::1]:8080",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");
        let config = cli.into_config().expect("config");

        let CommandConfig::Client(client) = config.command else {
            panic!("expected client config");
        };
        assert_eq!(
            client.ingresses,
            vec![IngressConfig::HttpConnect {
                listen: vec![
                    "127.0.0.1:8080".parse().expect("ipv4 listen"),
                    "[::1]:8080".parse().expect("ipv6 listen"),
                ]
            }]
        );
    }

    #[test]
    fn client_cli_accepts_multiple_proxy_ingresses() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "client",
            "--socks5-listen",
            "127.0.0.1:1080",
            "--http-listen",
            "127.0.0.1:8080",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");
        let config = cli.into_config().expect("config");

        let CommandConfig::Client(client) = config.command else {
            panic!("expected client config");
        };
        assert_eq!(
            client.ingresses,
            vec![
                IngressConfig::Socks5 {
                    listen: vec!["127.0.0.1:1080".parse().expect("socks listen")]
                },
                IngressConfig::HttpConnect {
                    listen: vec!["127.0.0.1:8080".parse().expect("http listen")]
                },
            ]
        );
    }

    #[test]
    fn client_cli_parses_optional_proxy_auth() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "client",
            "--proxy-username",
            "operator",
            "--proxy-password",
            "secret",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");
        let config = cli.into_config().expect("config");

        let CommandConfig::Client(client) = config.command else {
            panic!("expected client config");
        };
        assert!(client.proxy_auth.is_required());
        assert!(client.proxy_auth.verify("operator", "secret"));
        assert!(!client.proxy_auth.verify("operator", "wrong"));

        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "client",
            "--proxy-username",
            "operator",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");
        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::ProxyPasswordRequired)
        ));
    }

    #[test]
    fn client_cli_treats_listen_as_socks5_shorthand_with_http_ingress() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "client",
            "--listen",
            "127.0.0.1:1080",
            "--http-listen",
            "127.0.0.1:8080",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");
        let config = cli.into_config().expect("config");

        let CommandConfig::Client(client) = config.command else {
            panic!("expected client config");
        };
        assert_eq!(
            client.ingresses,
            vec![
                IngressConfig::Socks5 {
                    listen: vec!["127.0.0.1:1080".parse().expect("socks listen")]
                },
                IngressConfig::HttpConnect {
                    listen: vec!["127.0.0.1:8080".parse().expect("http listen")]
                },
            ]
        );
    }

    #[test]
    fn cipher_cli_can_select_chacha20_poly1305() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "--cipher",
            "chacha20-poly1305",
            "client",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");
        let config = cli.into_config().expect("config");

        assert_eq!(config.security.cipher, CipherSuite::Chacha20Poly1305);
    }

    #[test]
    fn service_supervisor_cli_is_parsed_and_validated() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "--service-mode",
            "--supervise",
            "--restart-backoff-ms",
            "500",
            "--restart-max-backoff-ms",
            "5000",
            "--max-restarts",
            "3",
            "client",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");
        let config = cli.into_config().expect("config");

        assert!(config.service.service_mode);
        assert!(config.service.supervise);
        assert_eq!(config.service.restart_backoff, Duration::from_millis(500));
        assert_eq!(
            config.service.restart_max_backoff,
            Duration::from_millis(5000)
        );
        assert_eq!(config.service.max_restarts, Some(3));

        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "--restart-backoff-ms",
            "5000",
            "--restart-max-backoff-ms",
            "500",
            "client",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");
        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Config(
                crate::config::ConfigError::RestartMaxBackoffTooSmall
            ))
        ));
    }

    #[test]
    fn platform_command_does_not_require_runtime_secret() {
        let cli = Cli::try_parse_from(["mptunnel", "platform"]).expect("parse cli");
        assert!(matches!(cli.command, Command::Platform(_)));
    }

    #[test]
    fn plaintext_lab_mode_requires_acknowledgement() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--security",
            "plaintext-lab",
            "client",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::PlaintextNotAcknowledged)
        ));
    }

    #[test]
    fn secret_is_required() {
        let cli = Cli::try_parse_from(["mptunnel", "client", "--path", "tcp://127.0.0.1:443"])
            .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Security(
                crate::config::SecurityPolicyError::MissingSecret
            ))
        ));
    }

    #[test]
    fn server_outbound_requires_matching_parameters() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "server",
            "--bind-path",
            "tcp://0.0.0.0:443",
            "--outbound",
            "socks5",
        ])
        .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::MissingUpstreamSocks5)
        ));

        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "server",
            "--bind-path",
            "udp://0.0.0.0:443",
            "--outbound",
            "http-connect-udp",
        ])
        .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::MissingUpstreamHttp)
        ));
    }

    #[test]
    fn server_http_connect_udp_outbound_is_parsed() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "server",
            "--bind-path",
            "udp://0.0.0.0:443",
            "--outbound",
            "http-connect-udp",
            "--upstream-http",
            "127.0.0.1:8080",
        ])
        .expect("parse cli");
        let config = cli.into_config().expect("config");

        let CommandConfig::Server(server) = config.command else {
            panic!("expected server config");
        };
        assert_eq!(
            server.outbound,
            OutboundConfig::HttpConnectUdp {
                proxy: "127.0.0.1:8080".parse().expect("proxy")
            }
        );
    }

    #[test]
    fn server_outbound_dns_is_parsed() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "server",
            "--bind-path",
            "tcp://0.0.0.0:443",
            "--outbound-dns-resolver",
            "1.1.1.1:53",
            "--outbound-dns-resolver",
            "[2606:4700:4700::1111]:53",
            "--outbound-dns-strategy",
            "ipv6-then-ipv4",
            "--outbound-dns-timeout-ms",
            "1500",
        ])
        .expect("parse cli");
        let config = cli.into_config().expect("config");

        let CommandConfig::Server(server) = config.command else {
            panic!("expected server config");
        };
        assert_eq!(
            server.outbound_dns.resolvers,
            vec![
                "1.1.1.1:53".parse().expect("resolver"),
                "[2606:4700:4700::1111]:53".parse().expect("resolver"),
            ]
        );
        assert_eq!(server.outbound_dns.strategy, DnsIpStrategy::Ipv6ThenIpv4);
        assert_eq!(server.outbound_dns.timeout, Duration::from_millis(1500));

        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "server",
            "--bind-path",
            "tcp://0.0.0.0:443",
            "--outbound-dns-resolver",
            "1.1.1.1:0",
        ])
        .expect("parse cli");
        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Config(
                crate::config::ConfigError::OutboundDnsResolverPortZero
            ))
        ));
    }

    #[test]
    fn tun_l4_cli_parses_dual_stack_and_dns() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "client",
            "--tun-name",
            "mptun0",
            "--tun-ipv6",
            "fd00::1",
            "--tun-dns-resolver",
            "1.1.1.1:53",
            "--tun-dns-resolver",
            "[2606:4700:4700::1111]:53",
            "--path",
            "tcp://127.0.0.1:443",
            "--path",
            "udp://127.0.0.1:443",
        ])
        .expect("parse cli");
        let config = cli.into_config().expect("config");

        let CommandConfig::Client(client) = config.command else {
            panic!("expected client config");
        };
        let [IngressConfig::TunL4(tun)] = client.ingresses.as_slice() else {
            panic!("expected TUN L4 ingress");
        };
        assert_eq!(tun.name.as_deref(), Some("mptun0"));
        assert_eq!(tun.ipv4, Some(crate::ingress::tun::DEFAULT_TUN_IPV4));
        assert_eq!(tun.ipv6, Some("fd00::1".parse().expect("ipv6")));
        assert_eq!(
            tun.dns_resolvers,
            vec![
                "1.1.1.1:53".parse().expect("resolver"),
                "[2606:4700:4700::1111]:53".parse().expect("resolver"),
            ]
        );
        assert!(tun.enable_icmp);
        assert!(
            client
                .paths
                .iter()
                .any(|path| { path.underlay == crate::protocol::UnderlayProtocol::Tcp })
        );
        assert!(
            client
                .paths
                .iter()
                .any(|path| { path.underlay == crate::protocol::UnderlayProtocol::Udp })
        );
    }

    #[test]
    fn tun_l4_cli_supports_ipv6_only() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "client",
            "--tun",
            "--tun-disable-ipv4",
            "--tun-ipv6",
            "fd00::1",
            "--path",
            "tcp://127.0.0.1:443",
            "--path",
            "udp://127.0.0.1:443",
        ])
        .expect("parse cli");
        let config = cli.into_config().expect("config");

        let CommandConfig::Client(client) = config.command else {
            panic!("expected client config");
        };
        let [IngressConfig::TunL4(tun)] = client.ingresses.as_slice() else {
            panic!("expected TUN L4 ingress");
        };
        assert_eq!(tun.ipv4, None);
        assert_eq!(tun.ipv6, Some("fd00::1".parse().expect("ipv6")));
    }

    #[test]
    fn tun_l4_validation_accepts_single_underlay_and_rejects_ipv4_flags() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "client",
            "--tun",
            "--tun-disable-ipv4",
            "--tun-ipv4",
            "10.88.0.2",
            "--tun-ipv6",
            "fd00::1",
            "--path",
            "tcp://127.0.0.1:443",
            "--path",
            "udp://127.0.0.1:443",
        ])
        .expect("parse cli");
        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::TunIpv4DisabledWithIpv4Options)
        ));

        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "client",
            "--tun",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");
        cli.into_config().expect("TCP-only TUN config");

        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "client",
            "--tun",
            "--path",
            "udp://127.0.0.1:443",
        ])
        .expect("parse cli");
        cli.into_config().expect("UDP-only TUN config");

        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "client",
            "--tun",
            "--tun-dns-resolver",
            "1.1.1.1:0",
            "--path",
            "tcp://127.0.0.1:443",
            "--path",
            "udp://127.0.0.1:443",
        ])
        .expect("parse cli");
        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Config(
                crate::config::ConfigError::TunDnsResolverPortZero
            ))
        ));
    }

    #[test]
    fn resource_limits_are_validated() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "--max-frame-bytes",
            "128",
            "--max-payload-bytes",
            "128",
            "client",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Config(
                crate::config::ConfigError::PayloadLimitExceedsFrameLimit
            ))
        ));
    }

    #[test]
    fn mux_memory_limits_are_validated() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "--max-payload-bytes",
            "1024",
            "--max-repair-bytes",
            "512",
            "client",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Config(
                crate::config::ConfigError::RepairLimitTooSmall
            ))
        ));

        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "--max-payload-bytes",
            "1024",
            "--max-repair-bytes",
            "4096",
            "--max-tcp-relay-chunk-bytes",
            "1024",
            "--max-tcp-path-inflight-bytes",
            "512",
            "client",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Config(
                crate::config::ConfigError::TcpPathInflightLimitTooSmall
            ))
        ));

        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "--max-payload-bytes",
            "1024",
            "--max-repair-bytes",
            "4096",
            "--max-tcp-relay-chunk-bytes",
            "1024",
            "--max-tcp-path-inflight-bytes",
            "8192",
            "client",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Config(
                crate::config::ConfigError::TcpPathInflightLimitExceedsRepairLimit
            ))
        ));

        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "--max-tcp-relay-chunk-bytes",
            "0",
            "client",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Config(
                crate::config::ConfigError::MaxTcpRelayChunkBytesZero
            ))
        ));

        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "--max-payload-bytes",
            "1024",
            "--max-tcp-relay-chunk-bytes",
            "2048",
            "client",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Config(
                crate::config::ConfigError::MaxTcpRelayChunkExceedsPayloadLimit
            ))
        ));
    }

    #[test]
    fn tcp_path_heartbeat_timing_is_validated() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "--tcp-path-heartbeat-interval-ms",
            "0",
            "client",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Config(
                crate::config::ConfigError::TcpPathHeartbeatIntervalZero
            ))
        ));

        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "--tcp-path-heartbeat-timeout-ms",
            "0",
            "client",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Config(
                crate::config::ConfigError::TcpPathHeartbeatTimeoutZero
            ))
        ));

        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "--tcp-path-heartbeat-interval-ms",
            "2000",
            "--tcp-path-heartbeat-timeout-ms",
            "1000",
            "client",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Config(
                crate::config::ConfigError::TcpPathHeartbeatTimeoutTooSmall
            ))
        ));
    }

    #[test]
    fn path_probe_timing_is_validated() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "client",
            "--path-probe-interval-ms",
            "0",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Config(
                crate::config::ConfigError::PathProbeIntervalZero
            ))
        ));

        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "client",
            "--path-probe-timeout-ms",
            "0",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Config(
                crate::config::ConfigError::PathProbeTimeoutZero
            ))
        ));
    }
}
