use crate::config::{
    AppConfig, CipherSuite, ClientConfig, ClientPathConfig, CommandConfig,
    DEFAULT_AUTH_FRESHNESS_WINDOW_SECONDS, DEFAULT_DATAGRAM_QUEUE_BYTES,
    DEFAULT_EXTRA_TRAFFIC_HINT_PERCENT, DEFAULT_MAX_QUIC_CONCURRENT_BIDI_STREAMS,
    DEFAULT_MAX_RELIABLE_RELAY_CHUNK_BYTES, DEFAULT_OUTBOUND_CONNECT_TIMEOUT_MS,
    DEFAULT_PATH_FLIGHT_BYTES, DEFAULT_PATH_PROBE_INTERVAL_MS, DEFAULT_PATH_PROBE_TIMEOUT_MS,
    DEFAULT_REORDER_BYTES, DEFAULT_REPAIR_BYTES, DEFAULT_RESTART_BACKOFF_MS,
    DEFAULT_RESTART_MAX_BACKOFF_MS, DEFAULT_STREAM_WINDOW_BYTES,
    DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL_MS, DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT_MS,
    LocalIngressConfig, ManagementConfig, MppPerformanceConfig, ResourceLimits, SecurityConfig,
    ServerConfig, ServiceConfig, SharedSecret,
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

    #[command(flatten)]
    pub management: ManagementArgs,

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
            Command::Client(args) => CommandConfig::Client(args.into_config(security.clone())?),
            Command::Server(args) => CommandConfig::Server(args.into_config(security.clone())?),
            Command::Platform(_) => return Err(CliConfigError::PlatformCommandNotRuntimeConfig),
        };
        let config = AppConfig {
            log_level: self.log_level,
            check_config: self.check_config,
            service: self.service.into_config(),
            resources: self.resources.into_limits(),
            management: self.management.into_config(),
            security,
            command,
        };
        config.validate()?;
        Ok(config)
    }
}

#[derive(Debug, Args)]
pub struct ManagementArgs {
    #[arg(
        long = "management-listen",
        global = true,
        env = "MPTUNNEL_MANAGEMENT_LISTEN",
        value_delimiter = ','
    )]
    pub listen: Vec<SocketAddr>,

    #[arg(
        long = "management-token",
        global = true,
        env = "MPTUNNEL_MANAGEMENT_TOKEN"
    )]
    pub token: Option<String>,
}

impl ManagementArgs {
    fn into_config(self) -> ManagementConfig {
        ManagementConfig {
            listen: self.listen,
            token: self.token,
        }
    }
}

#[derive(Debug, Args)]
pub struct ServiceArgs {
    /// Declares service intent; does not register with a native service manager.
    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_SERVICE_MODE",
        default_value_t = false
    )]
    pub service_mode: bool,

    /// Restarts failed runtime generations inside the current process.
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
        env = "MPTUNNEL_MAX_QUIC_CONCURRENT_BIDI_STREAMS",
        default_value_t = DEFAULT_MAX_QUIC_CONCURRENT_BIDI_STREAMS
    )]
    pub max_quic_concurrent_bidi_streams: usize,

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
        env = "MPTUNNEL_MAX_PATH_FLIGHT_BYTES",
        default_value_t = DEFAULT_PATH_FLIGHT_BYTES
    )]
    pub max_path_flight_bytes: usize,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_RELIABLE_RELAY_CHUNK_BYTES",
        default_value_t = DEFAULT_MAX_RELIABLE_RELAY_CHUNK_BYTES
    )]
    pub max_reliable_relay_chunk_bytes: usize,

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
            max_quic_concurrent_bidi_streams: self.max_quic_concurrent_bidi_streams,
            max_stream_window_bytes: self.max_stream_window_bytes,
            max_repair_bytes: self.max_repair_bytes,
            max_reorder_bytes: self.max_reorder_bytes,
            max_datagram_queue_bytes: self.max_datagram_queue_bytes,
            max_path_flight_bytes: self.max_path_flight_bytes,
            max_reliable_relay_chunk_bytes: self.max_reliable_relay_chunk_bytes,
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

    #[arg(
        long = "extra-traffic-hint-percent",
        env = "MPTUNNEL_EXTRA_TRAFFIC_HINT_PERCENT",
        default_value_t = DEFAULT_EXTRA_TRAFFIC_HINT_PERCENT
    )]
    pub extra_traffic_hint_percent: u16,
}

impl ClientArgs {
    fn into_config(self, security: SecurityConfig) -> Result<ClientConfig, CliConfigError> {
        let socks5_listen = combined_socks5_listen(&self);
        let http_connect_enabled = !self.http_listen.is_empty();
        let tun_enabled = tun_requested(&self);
        let socks5_enabled = !socks5_listen.is_empty() || (!http_connect_enabled && !tun_enabled);
        let proxy_auth = proxy_auth_config(self.proxy_username, self.proxy_password)?;

        let mut ingresses = Vec::with_capacity(3);
        if socks5_enabled {
            ingresses.push(LocalIngressConfig {
                tag: None,
                config: IngressConfig::Socks5 {
                    listen: listen_or_default(socks5_listen, 1080),
                    proxy_auth: proxy_auth.clone(),
                },
            });
        }
        if http_connect_enabled {
            ingresses.push(LocalIngressConfig {
                tag: None,
                config: IngressConfig::HttpConnect {
                    listen: self.http_listen.clone(),
                    proxy_auth: proxy_auth.clone(),
                },
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
            ingresses.push(LocalIngressConfig {
                tag: None,
                config: IngressConfig::TunL4(TunL4Config {
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
                }),
            });
        }
        Ok(ClientConfig {
            route_target: None,
            ingresses,
            paths: self
                .paths
                .into_iter()
                .map(|spec| ClientPathConfig {
                    spec,
                    security: security.clone(),
                })
                .collect(),
            security,
            path_probe_interval: Duration::from_millis(self.path_probe_interval_ms),
            path_probe_timeout: Duration::from_millis(self.path_probe_timeout_ms),
            performance: MppPerformanceConfig {
                extra_traffic_hint_percent: self.extra_traffic_hint_percent,
            },
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

    #[arg(
        long,
        env = "MPTUNNEL_OUTBOUND_CONNECT_TIMEOUT_MS",
        default_value_t = DEFAULT_OUTBOUND_CONNECT_TIMEOUT_MS
    )]
    pub outbound_connect_timeout_ms: u64,

    #[arg(
        long = "extra-traffic-hint-percent",
        env = "MPTUNNEL_EXTRA_TRAFFIC_HINT_PERCENT",
        default_value_t = DEFAULT_EXTRA_TRAFFIC_HINT_PERCENT
    )]
    pub extra_traffic_hint_percent: u16,
}

impl ServerArgs {
    fn into_config(self, security: SecurityConfig) -> Result<ServerConfig, CliConfigError> {
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
            tag: None,
            route_target: None,
            bind_paths: self.bind_paths,
            security,
            outbound,
            outbound_dns: DnsConfig {
                resolvers: self.outbound_dns_resolvers,
                strategy: self.outbound_dns_strategy.into(),
                timeout: Duration::from_millis(self.outbound_dns_timeout_ms),
            },
            outbound_connect_timeout: Duration::from_millis(self.outbound_connect_timeout_ms),
            performance: MppPerformanceConfig {
                extra_traffic_hint_percent: self.extra_traffic_hint_percent,
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
#[path = "cli_test.rs"]
mod tests;
