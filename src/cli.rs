use crate::config::{
    AppConfig, ClientPathConfig, ClientSecurityConfig, CommandConfig,
    DEFAULT_AUTH_FRESHNESS_WINDOW_SECONDS, DEFAULT_DATAGRAM_QUEUE_BYTES,
    DEFAULT_EXTRA_TRAFFIC_HINT_PERCENT, DEFAULT_MAX_QUIC_CONCURRENT_BIDI_STREAMS,
    DEFAULT_MAX_REINJECTION_CACHE_CHUNKS, DEFAULT_MAX_RELIABLE_RELAY_CHUNK_BYTES,
    DEFAULT_MAX_REORDER_BUFFER_CHUNKS, DEFAULT_MAX_RETAINED_RECEIVE_RANGES,
    DEFAULT_MPP_TLS_SERVER_NAME, DEFAULT_OUTBOUND_CONNECT_TIMEOUT_MS, DEFAULT_PATH_FLIGHT_BYTES,
    DEFAULT_PATH_PROBE_INTERVAL_MS, DEFAULT_PATH_PROBE_TIMEOUT_MS,
    DEFAULT_QUIC_PATH_IDLE_TIMEOUT_MS, DEFAULT_QUIC_PATH_KEEP_ALIVE_INTERVAL_MS,
    DEFAULT_REORDER_BYTES, DEFAULT_REPAIR_BYTES, DEFAULT_RESTART_BACKOFF_MS,
    DEFAULT_RESTART_MAX_BACKOFF_MS, DEFAULT_SESSION_RETENTION_TIMEOUT_MS,
    DEFAULT_STREAM_WINDOW_BYTES, DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL_MS,
    DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT_MS, DnsPolicyConfig, EgressRef, LocalIngressConfig,
    LogFormat, LogLevel, LoggingConfig, ManagementConfig, MppInboundConfig, MppOutboundConfig,
    MppPerformanceConfig, NamedPathConfig, NodeConfig, OutboundLeafConfig, ProductPolicyConfig,
    ResourceLimits, ServerDestinationAclConfig, ServerSecurityConfig, ServiceConfig, SessionConfig,
    SharedSecret, normalize_secret_bytes, read_secret_environment, read_secret_file,
};
use crate::ingress::tun::{
    DEFAULT_TUN_DNS_TTL_MS, DEFAULT_TUN_MTU, ManagedVpnConfig, ManagedVpnPlatformConfig,
    TunHostConfig, TunL4Config,
};
use crate::ingress::{
    DEFAULT_TCP_FORWARD_MAX_CONNECTIONS, DEFAULT_UDP_FORWARD_DATAGRAM_TTL_MS,
    DEFAULT_UDP_FORWARD_IDLE_TIMEOUT_MS, DEFAULT_UDP_FORWARD_MAX_ASSOCIATIONS, IngressConfig,
    LocalIngressAdmissionConfig, LocalProxyUser, PortForwardTarget, ProxyAuthConfig,
    ProxyAuthConfigError, TcpForwardConfig, UdpForwardConfig,
};
use crate::outbound::{HttpsProxyConfig, OutboundConfig, ProxyConfig, ProxyCredentials};
use crate::platform::RouteMode;
use crate::product::{
    CredentialCatalog, CredentialId, CredentialRecord, DnsIpStrategy, DnsPlanId, DnsPlanLimits,
    DnsPlanSpec, DnsPolicySpec, DnsSecurityPolicy, DnsUpstreamEndpoint, DnsUpstreamId,
    DnsUpstreamSpec, DomainName, EgressAction, InboundId, InitialDemand, OutboundId, PrincipalId,
    ProtocolTarget, RouteAction, RouteMatchSpec, RouteRuleSpec, RuleId,
};
use crate::transport::encrypted::{SharedTransportSecret, TcpClientTlsConfig, TcpServerTlsConfig};
use crate::transport::{Endpoint, PathSpec};
use clap::{Args, Parser, Subcommand, ValueEnum};
use ipnet::IpNet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

const DEFAULT_SIMPLE_DNS_TIMEOUT_MS: u64 = 5_000;

#[derive(Debug, Parser)]
#[command(name = "mptunnel")]
#[command(about = "Encrypted multipath proxy and tunnel")]
#[command(version)]
pub struct Cli {
    /// Load the complete runtime configuration from a TOML file.
    #[arg(short = 'c', long = "config", global = true, value_name = "FILE")]
    pub config_file: Option<PathBuf>,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_LOG_LEVEL",
        value_enum,
        default_value = "info"
    )]
    pub log_level: LogLevelArg,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_LOG_FORMAT",
        value_enum,
        default_value = "text"
    )]
    pub log_format: LogFormatArg,

    /// Append process records to a file in addition to any console sink.
    #[arg(long, global = true, env = "MPTUNNEL_LOG_FILE", value_name = "FILE")]
    pub log_file: Option<PathBuf>,

    /// Disable the standard-error log sink.
    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_LOG_NO_CONSOLE",
        default_value_t = false
    )]
    pub log_no_console: bool,

    /// Emit sanitized Product flow-open and flow-close records.
    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_LOG_FLOW_EVENTS",
        default_value_t = false
    )]
    pub log_flow_events: bool,

    /// Named MPP credential selected by the simple client/server profile.
    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_CREDENTIAL_ID",
        default_value = "default"
    )]
    pub credential_id: String,

    /// Principal identity assigned to a simple server credential or
    /// route-explain input.
    #[arg(
        long = "principal-id",
        global = true,
        env = "MPTUNNEL_PRINCIPAL_ID",
        default_value = "default-user"
    )]
    pub principal_id: String,

    /// Read the MPP credential key from this file.
    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_CREDENTIAL_SECRET_FILE",
        value_name = "FILE",
        conflicts_with_all = ["credential_secret_env", "credential_secret_stdin"]
    )]
    pub credential_secret_file: Option<PathBuf>,

    /// Read the MPP credential key from the named environment variable.
    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_CREDENTIAL_SECRET_ENV",
        value_name = "NAME",
        conflicts_with_all = ["credential_secret_file", "credential_secret_stdin"]
    )]
    pub credential_secret_env: Option<String>,

    /// Read the MPP credential key from standard input until EOF.
    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_CREDENTIAL_SECRET_STDIN",
        default_value_t = false,
        conflicts_with_all = ["credential_secret_file", "credential_secret_env"]
    )]
    pub credential_secret_stdin: bool,

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
    pub session: SessionArgs,

    #[command(flatten)]
    pub management: ManagementArgs,

    #[command(subcommand)]
    pub command: Command,
}

impl Cli {
    pub(crate) fn logging_config(&self) -> LoggingConfig {
        LoggingConfig {
            level: self.log_level.into(),
            format: self.log_format.into(),
            console: !self.log_no_console,
            file: self.log_file.clone(),
            flow_events: self.log_flow_events,
        }
    }

    pub fn into_config(self) -> Result<AppConfig, CliConfigError> {
        if !matches!(&self.command, Command::Client(_) | Command::Server(_)) {
            return Err(CliConfigError::OperationalCommandNotRuntimeConfig);
        }
        let credential_id = CredentialId::parse(&self.credential_id)
            .map_err(|error| CliConfigError::Credential(error.to_string()))?;
        let principal = PrincipalId::parse(&self.principal_id)
            .map_err(|error| CliConfigError::Credential(error.to_string()))?;
        let secret = SharedSecret::new(self.resolve_credential_secret()?)?;
        let credential =
            CredentialRecord::new(credential_id.clone(), principal, secret, None, false, 0)
                .map_err(|error| CliConfigError::Credential(error.to_string()))?;
        let catalog = CredentialCatalog::compile([credential])
            .map_err(|error| CliConfigError::Credential(error.to_string()))?;
        let logging = self.logging_config();
        let command = match self.command {
            Command::Client(args) => {
                let security = ClientSecurityConfig::new(
                    catalog
                        .credential(&credential_id)
                        .map_err(|error| CliConfigError::Credential(error.to_string()))?,
                )
                .with_auth_freshness_window(Duration::from_secs(
                    self.auth_freshness_window_seconds,
                ));
                CommandConfig::Node((*args).into_config(security)?)
            }
            Command::Server(args) => {
                let security = ServerSecurityConfig::new(
                    catalog
                        .authority(std::slice::from_ref(&credential_id))
                        .map_err(|error| CliConfigError::Credential(error.to_string()))?,
                )
                .with_auth_freshness_window(Duration::from_secs(
                    self.auth_freshness_window_seconds,
                ));
                CommandConfig::Node((*args).into_config(security)?)
            }
            Command::Platform(_)
            | Command::Status(_)
            | Command::Doctor(_)
            | Command::Route(_)
            | Command::Dns(_) => {
                return Err(CliConfigError::OperationalCommandNotRuntimeConfig);
            }
        };
        let config = AppConfig {
            logging,
            check_config: self.check_config,
            service: self.service.into_config(),
            session: self.session.into_config(),
            resources: self.resources.into_limits(),
            admission: crate::product::ProductAdmissionConfig::default(),
            management: self.management.into_config()?,
            command,
        };
        config.validate()?;
        Ok(config)
    }

    fn resolve_credential_secret(&self) -> Result<Vec<u8>, CliConfigError> {
        let mut value = match (
            self.credential_secret_file.as_deref(),
            self.credential_secret_env.as_deref(),
            self.credential_secret_stdin,
        ) {
            (Some(path), None, false) => read_secret_file(path, "credential")?,
            (None, Some(name), false) => read_secret_environment(name, "credential")?,
            (None, None, true) => {
                use std::io::Read;
                let mut bytes = Vec::new();
                std::io::stdin().read_to_end(&mut bytes).map_err(|error| {
                    CliConfigError::CredentialSecret(format!(
                        "failed to read credential secret from stdin: {error}"
                    ))
                })?;
                normalize_secret_bytes(&mut bytes);
                bytes
            }
            (None, None, false) => {
                return Err(CliConfigError::Security(
                    crate::config::SecurityPolicyError::MissingSecret,
                ));
            }
            _ => {
                return Err(CliConfigError::CredentialSecret(
                    "exactly one credential secret reference is required".to_string(),
                ));
            }
        };
        normalize_secret_bytes(&mut value);
        Ok(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LogLevelArg {
    Off,
    Error,
    Warn,
    Info,
}

impl From<LogLevelArg> for LogLevel {
    fn from(value: LogLevelArg) -> Self {
        match value {
            LogLevelArg::Off => Self::Off,
            LogLevelArg::Error => Self::Error,
            LogLevelArg::Warn => Self::Warn,
            LogLevelArg::Info => Self::Info,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LogFormatArg {
    Text,
    Json,
}

impl From<LogFormatArg> for LogFormat {
    fn from(value: LogFormatArg) -> Self {
        match value {
            LogFormatArg::Text => Self::Text,
            LogFormatArg::Json => Self::Json,
        }
    }
}

fn secret_utf8(bytes: Vec<u8>, purpose: &'static str) -> Result<String, CliConfigError> {
    String::from_utf8(bytes).map_err(|_| {
        CliConfigError::SecretMaterial(format!("{purpose} secret material is not valid UTF-8"))
    })
}

#[derive(Debug, Args)]
pub struct SessionArgs {
    /// Bound carrierless stream retention and graceful TCP carrier retirement.
    #[arg(
        long = "session-retention-timeout-ms",
        global = true,
        env = "MPTUNNEL_SESSION_RETENTION_TIMEOUT_MS",
        default_value_t = DEFAULT_SESSION_RETENTION_TIMEOUT_MS
    )]
    pub retention_timeout_ms: u64,
}

impl SessionArgs {
    fn into_config(self) -> SessionConfig {
        SessionConfig {
            retention_timeout: Duration::from_millis(self.retention_timeout_ms),
        }
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
    pub management_listen: Vec<SocketAddr>,

    #[arg(
        long = "management-token-file",
        global = true,
        env = "MPTUNNEL_MANAGEMENT_TOKEN_FILE",
        value_name = "FILE",
        conflicts_with = "management_token_env"
    )]
    pub management_token_file: Option<PathBuf>,

    #[arg(
        long = "management-token-env",
        global = true,
        env = "MPTUNNEL_MANAGEMENT_TOKEN_ENV",
        value_name = "NAME",
        conflicts_with = "management_token_file"
    )]
    pub management_token_env: Option<String>,

    #[arg(
        long = "management-dashboard",
        global = true,
        env = "MPTUNNEL_MANAGEMENT_DASHBOARD",
        default_value_t = false
    )]
    pub dashboard: bool,

    #[arg(
        long = "management-allow-peer-diagnostics",
        global = true,
        env = "MPTUNNEL_MANAGEMENT_ALLOW_PEER_DIAGNOSTICS",
        default_value_t = false
    )]
    pub allow_peer_diagnostics: bool,
}

impl ManagementArgs {
    pub(crate) fn resolve_token(&self) -> Result<Option<String>, CliConfigError> {
        match (
            self.management_token_file.as_deref(),
            self.management_token_env.as_deref(),
        ) {
            (Some(path), None) => Ok(Some(secret_utf8(
                read_secret_file(path, "management token")?,
                "management token",
            )?)),
            (None, Some(name)) => Ok(Some(secret_utf8(
                read_secret_environment(name, "management token")?,
                "management token",
            )?)),
            (None, None) => Ok(None),
            (Some(_), Some(_)) => Err(CliConfigError::SecretMaterial(
                "exactly one management token reference is allowed".to_string(),
            )),
        }
    }

    fn into_config(self) -> Result<ManagementConfig, CliConfigError> {
        let token = self.resolve_token()?;
        Ok(ManagementConfig {
            listen: self.management_listen,
            token,
            dashboard: self.dashboard,
            allow_peer_diagnostics: self.allow_peer_diagnostics,
        })
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
        env = "MPTUNNEL_MAX_REINJECTION_CACHE_CHUNKS",
        default_value_t = DEFAULT_MAX_REINJECTION_CACHE_CHUNKS
    )]
    pub max_reinjection_cache_chunks: usize,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_REORDER_BUFFER_CHUNKS",
        default_value_t = DEFAULT_MAX_REORDER_BUFFER_CHUNKS
    )]
    pub max_reorder_buffer_chunks: usize,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_RETAINED_RECEIVE_RANGES",
        default_value_t = DEFAULT_MAX_RETAINED_RECEIVE_RANGES
    )]
    pub max_retained_receive_ranges: usize,

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

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_QUIC_PATH_KEEP_ALIVE_INTERVAL_MS",
        default_value_t = DEFAULT_QUIC_PATH_KEEP_ALIVE_INTERVAL_MS
    )]
    pub quic_path_keep_alive_interval_ms: u64,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_QUIC_PATH_IDLE_TIMEOUT_MS",
        default_value_t = DEFAULT_QUIC_PATH_IDLE_TIMEOUT_MS
    )]
    pub quic_path_idle_timeout_ms: u64,
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
            max_reinjection_cache_chunks: self.max_reinjection_cache_chunks,
            max_reorder_buffer_chunks: self.max_reorder_buffer_chunks,
            max_retained_receive_ranges: self.max_retained_receive_ranges,
            max_datagram_queue_bytes: self.max_datagram_queue_bytes,
            max_path_flight_bytes: self.max_path_flight_bytes,
            max_reliable_relay_chunk_bytes: self.max_reliable_relay_chunk_bytes,
            tcp_path_heartbeat_interval: Duration::from_millis(self.tcp_path_heartbeat_interval_ms),
            tcp_path_heartbeat_timeout: Duration::from_millis(self.tcp_path_heartbeat_timeout_ms),
            quic_path_keep_alive_interval: Duration::from_millis(
                self.quic_path_keep_alive_interval_ms,
            ),
            quic_path_idle_timeout: Duration::from_millis(self.quic_path_idle_timeout_ms),
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the local proxy/TUN ingress side.
    Client(Box<ClientArgs>),
    /// Run the remote path listener and outbound connector side.
    Server(Box<ServerArgs>),
    /// Print platform, TUN, service, and release-target information.
    Platform(PlatformArgs),
    /// Print the current authenticated runtime status.
    Status(ManagementClientArgs),
    /// Validate configuration, host capability, and configured endpoint reachability.
    Doctor(DoctorArgs),
    /// Explain offline Product routing decisions.
    Route(Box<RouteArgs>),
    /// Inspect or operate the authenticated Product DNS runtime.
    Dns(DnsArgs),
}

#[derive(Debug, Args)]
pub struct PlatformArgs {}

impl Command {
    pub const fn is_operational(&self) -> bool {
        matches!(
            self,
            Self::Platform(_) | Self::Status(_) | Self::Doctor(_) | Self::Route(_) | Self::Dns(_)
        )
    }
}

#[derive(Debug, Args)]
pub struct ManagementClientArgs {
    /// Loopback management API address.
    #[arg(long, global = true)]
    pub address: Option<SocketAddr>,
}

#[derive(Debug, Args)]
pub struct DoctorArgs {
    /// Check this loopback management API address instead of the first configured listener.
    #[arg(long)]
    pub management_address: Option<SocketAddr>,
}

#[derive(Debug, Args)]
pub struct RouteArgs {
    #[command(subcommand)]
    pub command: RouteCommand,
}

#[derive(Debug, Subcommand)]
pub enum RouteCommand {
    /// Explain one pre- or post-resolution Product routing decision.
    Explain(RouteExplainArgs),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RouteNetworkArg {
    Tcp,
    Udp,
}

impl From<RouteNetworkArg> for crate::product::Network {
    fn from(value: RouteNetworkArg) -> Self {
        match value {
            RouteNetworkArg::Tcp => Self::Tcp,
            RouteNetworkArg::Udp => Self::Udp,
        }
    }
}

#[derive(Debug, Args)]
pub struct RouteExplainArgs {
    /// Destination authority with a mandatory port.
    #[arg(long, value_parser = parse_route_target)]
    pub target: ProtocolTarget,

    #[arg(long, value_enum)]
    pub network: RouteNetworkArg,

    /// Original source address and port.
    #[arg(long)]
    pub source: SocketAddr,

    #[arg(long)]
    pub inbound: InboundId,

    /// Explain post-resolution routing with this selected destination address.
    #[arg(long)]
    pub resolved_ip: Option<IpAddr>,
}

fn parse_route_target(value: &str) -> Result<ProtocolTarget, String> {
    ProtocolTarget::parse_authority(value).map_err(|error| error.to_string())
}

#[derive(Debug, Args)]
pub struct DnsArgs {
    /// Loopback management API address.
    #[arg(long, global = true)]
    pub address: Option<SocketAddr>,

    #[command(subcommand)]
    pub command: DnsCommand,
}

#[derive(Debug, Subcommand)]
pub enum DnsCommand {
    /// Print DNS cache, upstream, and FakeDNS status.
    Status,
    /// Explain DNS policy selection without issuing a query.
    Explain(DnsExplainArgs),
    /// Issue one explicit typed DNS query through the configured runtime.
    Query(DnsQueryArgs),
    /// Flush one plan cache or all DNS caches.
    Flush(DnsFlushArgs),
}

#[derive(Debug, Args)]
pub struct DnsExplainArgs {
    pub domain: DomainName,
}

#[derive(Debug, Args)]
pub struct DnsQueryArgs {
    pub domain: DomainName,

    /// DNS record type, for example A, AAAA, HTTPS, MX, SRV, or TXT.
    #[arg(long = "type", default_value = "A")]
    pub record_type: String,
}

#[derive(Debug, Args)]
pub struct DnsFlushArgs {
    /// Flush only this DNS plan; omit to flush every DNS plan.
    #[arg(long = "dns-plan")]
    pub dns_plan: Option<DnsPlanId>,
}

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

    /// One simple fixed-target TCP listener. Use config.toml for multiple forwards.
    #[arg(long = "tcp-forward-listen", env = "MPTUNNEL_TCP_FORWARD_LISTEN")]
    pub tcp_forward_listen: Option<SocketAddr>,

    #[arg(long = "tcp-forward-target", env = "MPTUNNEL_TCP_FORWARD_TARGET")]
    pub tcp_forward_target: Option<PortForwardTarget>,

    #[arg(
        long = "tcp-forward-max-connections",
        env = "MPTUNNEL_TCP_FORWARD_MAX_CONNECTIONS"
    )]
    pub tcp_forward_max_connections: Option<usize>,

    /// One simple fixed-target UDP listener. Use config.toml for multiple forwards.
    #[arg(long = "udp-forward-listen", env = "MPTUNNEL_UDP_FORWARD_LISTEN")]
    pub udp_forward_listen: Option<SocketAddr>,

    #[arg(long = "udp-forward-target", env = "MPTUNNEL_UDP_FORWARD_TARGET")]
    pub udp_forward_target: Option<PortForwardTarget>,

    #[arg(
        long = "udp-forward-max-associations",
        env = "MPTUNNEL_UDP_FORWARD_MAX_ASSOCIATIONS"
    )]
    pub udp_forward_max_associations: Option<usize>,

    #[arg(
        long = "udp-forward-idle-timeout-ms",
        env = "MPTUNNEL_UDP_FORWARD_IDLE_TIMEOUT_MS"
    )]
    pub udp_forward_idle_timeout_ms: Option<u64>,

    #[arg(
        long = "udp-forward-datagram-ttl-ms",
        env = "MPTUNNEL_UDP_FORWARD_DATAGRAM_TTL_MS"
    )]
    pub udp_forward_datagram_ttl_ms: Option<u64>,

    #[arg(long, env = "MPTUNNEL_PROXY_USERNAME")]
    pub proxy_username: Option<String>,

    #[arg(
        long,
        env = "MPTUNNEL_PROXY_PASSWORD_FILE",
        value_name = "FILE",
        conflicts_with = "proxy_password_env"
    )]
    pub proxy_password_file: Option<PathBuf>,

    #[arg(
        long,
        env = "MPTUNNEL_PROXY_PASSWORD_ENV",
        value_name = "NAME",
        conflicts_with = "proxy_password_file"
    )]
    pub proxy_password_env: Option<String>,

    #[arg(long, env = "MPTUNNEL_TUN", default_value_t = false)]
    pub tun: bool,

    #[arg(long, env = "MPTUNNEL_TUN_INTERFACE_NAME")]
    pub tun_interface_name: Option<String>,

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

    /// Let MPTUNNEL own VPN routes and DNS on a supported host.
    ///
    /// Android `VpnService` integrations remain host-provided and do not use
    /// this process-managed mode.
    #[arg(long = "tun-vpn-mode", env = "MPTUNNEL_TUN_VPN_MODE", value_enum)]
    pub tun_vpn_mode: Option<TunVpnModeArg>,

    /// Prefix captured by a managed split VPN. Repeat for multiple prefixes.
    #[arg(
        long = "tun-include-cidr",
        env = "MPTUNNEL_TUN_INCLUDE_CIDRS",
        value_delimiter = ',',
        requires = "tun_vpn_mode"
    )]
    pub tun_include_cidrs: Vec<IpNet>,

    /// Prefix bypassed by a managed full or split VPN.
    #[arg(
        long = "tun-exclude-cidr",
        env = "MPTUNNEL_TUN_EXCLUDE_CIDRS",
        value_delimiter = ',',
        requires = "tun_vpn_mode"
    )]
    pub tun_exclude_cidrs: Vec<IpNet>,

    /// Keep discovered local-LAN prefixes outside the managed VPN.
    #[arg(
        long = "tun-local-lan",
        env = "MPTUNNEL_TUN_LOCAL_LAN",
        default_value_t = false,
        requires = "tun_vpn_mode"
    )]
    pub tun_local_lan: bool,

    /// System-facing local DNS address captured by the managed VPN.
    #[arg(
        long = "tun-dns-capture-server",
        env = "MPTUNNEL_TUN_DNS_CAPTURE_SERVERS",
        value_delimiter = ',',
        requires = "tun_vpn_mode"
    )]
    pub tun_dns_capture_servers: Vec<IpAddr>,

    /// Literal bootstrap endpoint for the managed VPN's DNS-over-TLS upstream.
    #[arg(
        long = "tun-dns-dot-bootstrap",
        env = "MPTUNNEL_TUN_DNS_DOT_BOOTSTRAP",
        requires_all = ["tun_vpn_mode", "tun_dns_dot_server_name"],
        conflicts_with = "tun_dns_resolvers"
    )]
    pub tun_dns_dot_bootstrap: Option<SocketAddr>,

    /// Authenticated TLS name for the managed VPN's DNS-over-TLS upstream.
    #[arg(
        long = "tun-dns-dot-server-name",
        env = "MPTUNNEL_TUN_DNS_DOT_SERVER_NAME",
        requires_all = ["tun_vpn_mode", "tun_dns_dot_bootstrap"],
        conflicts_with = "tun_dns_resolvers"
    )]
    pub tun_dns_dot_server_name: Option<String>,

    /// Outbound carrier endpoint: tcp://host:PORT[-END] or udp://host:PORT[-END].
    #[arg(
        long = "path",
        env = "MPTUNNEL_PATHS",
        value_delimiter = ',',
        required = true
    )]
    pub paths: Vec<PathSpec>,

    /// Pinned MPP TLS identity (default: mptunnel.example).
    #[arg(long = "tls-server-name", env = "MPTUNNEL_TLS_SERVER_NAME")]
    pub tls_server_name: Option<String>,

    #[arg(
        long = "tls-pinned-certificate",
        env = "MPTUNNEL_TLS_PINNED_CERTIFICATE",
        value_name = "PEM_FILE"
    )]
    pub tls_pinned_certificate: Option<PathBuf>,

    /// Optional endpoint-wide 32-byte raw transport secret. This is not an
    /// MPP client credential and must match the server value.
    #[arg(
        long = "transport-secret-file",
        env = "MPTUNNEL_TRANSPORT_SECRET_FILE",
        value_name = "FILE"
    )]
    pub transport_secret_file: Option<PathBuf>,

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TunVpnModeArg {
    Full,
    Split,
}

impl ClientArgs {
    fn into_config(self, security: ClientSecurityConfig) -> Result<NodeConfig, CliConfigError> {
        let socks5_listen = combined_socks5_listen(&self);
        let http_connect_enabled = !self.http_listen.is_empty();
        let tun_enabled = tun_requested(&self);
        let tcp_forward_requested = self.tcp_forward_listen.is_some()
            || self.tcp_forward_target.is_some()
            || self.tcp_forward_max_connections.is_some();
        let udp_forward_requested = self.udp_forward_listen.is_some()
            || self.udp_forward_target.is_some()
            || self.udp_forward_max_associations.is_some()
            || self.udp_forward_idle_timeout_ms.is_some()
            || self.udp_forward_datagram_ttl_ms.is_some();
        if tcp_forward_requested
            && (self.tcp_forward_listen.is_none() || self.tcp_forward_target.is_none())
        {
            return Err(CliConfigError::PortForward(
                "--tcp-forward-listen and --tcp-forward-target must be set together".to_string(),
            ));
        }
        if udp_forward_requested
            && (self.udp_forward_listen.is_none() || self.udp_forward_target.is_none())
        {
            return Err(CliConfigError::PortForward(
                "--udp-forward-listen and --udp-forward-target must be set together".to_string(),
            ));
        }
        let socks5_enabled = !socks5_listen.is_empty()
            || (!http_connect_enabled
                && !tun_enabled
                && !tcp_forward_requested
                && !udp_forward_requested);
        let tls = client_tls_from_cli(
            self.tls_server_name,
            self.tls_pinned_certificate,
            self.transport_secret_file,
        )?;
        let proxy_auth = proxy_auth_config(
            self.proxy_username,
            self.proxy_password_file.as_deref(),
            self.proxy_password_env.as_deref(),
        )?;
        if self.tun_vpn_mode.is_some() && !self.tun_dns_resolvers.is_empty() {
            return Err(CliConfigError::ManagedVpn(
                "--tun-dns-resolver is external/manual only and cannot be combined with --tun-vpn-mode"
                    .to_string(),
            ));
        }
        let dns_policy = match (
            self.tun_vpn_mode,
            self.tun_dns_dot_bootstrap,
            self.tun_dns_dot_server_name.as_deref(),
        ) {
            (Some(_), Some(bootstrap), Some(server_name)) => {
                managed_dot_dns_policy(bootstrap, server_name)?
            }
            (Some(_), None, None) => return Err(CliConfigError::ManagedDnsDotRequired),
            (Some(_), None, Some(_)) | (Some(_), Some(_), None) => {
                return Err(CliConfigError::ManagedDnsDotPairRequired);
            }
            (None, Some(_), _) | (None, _, Some(_)) => {
                return Err(CliConfigError::ManagedDnsDotRequiresVpnMode);
            }
            (None, None, None) if tun_enabled && !self.tun_dns_resolvers.is_empty() => {
                simple_dns_policy(
                    DnsModeArg::Servers,
                    self.tun_dns_resolvers.clone(),
                    DnsIpStrategy::Ipv4AndIpv6,
                    Duration::from_millis(DEFAULT_SIMPLE_DNS_TIMEOUT_MS),
                )?
                .0
            }
            (None, None, None) => DnsPolicyConfig::default(),
        };

        let mut ingresses = Vec::with_capacity(5);
        if socks5_enabled {
            ingresses.push(LocalIngressConfig {
                name: "socks5".to_string(),
                config: IngressConfig::Socks5 {
                    listen: listen_or_default(socks5_listen, 1080),
                    proxy_auth: proxy_auth.clone(),
                    admission: LocalIngressAdmissionConfig::default(),
                },
            });
        }
        if http_connect_enabled {
            ingresses.push(LocalIngressConfig {
                name: "http".to_string(),
                config: IngressConfig::HttpConnect {
                    listen: self.http_listen.clone(),
                    proxy_auth: proxy_auth.clone(),
                    admission: LocalIngressAdmissionConfig::default(),
                },
            });
        }
        if let (Some(listen), Some(target)) = (self.tcp_forward_listen, self.tcp_forward_target) {
            let config = TcpForwardConfig::new(
                vec![listen],
                target,
                self.tcp_forward_max_connections
                    .unwrap_or(DEFAULT_TCP_FORWARD_MAX_CONNECTIONS),
            )
            .map_err(|error| CliConfigError::PortForward(error.to_string()))?;
            ingresses.push(LocalIngressConfig {
                name: "tcp-forward".to_string(),
                config: IngressConfig::TcpForward(config),
            });
        }
        if let (Some(listen), Some(target)) = (self.udp_forward_listen, self.udp_forward_target) {
            let config = UdpForwardConfig::new(
                vec![listen],
                target,
                self.udp_forward_max_associations
                    .unwrap_or(DEFAULT_UDP_FORWARD_MAX_ASSOCIATIONS),
                Duration::from_millis(
                    self.udp_forward_idle_timeout_ms
                        .unwrap_or(DEFAULT_UDP_FORWARD_IDLE_TIMEOUT_MS),
                ),
                Duration::from_millis(
                    self.udp_forward_datagram_ttl_ms
                        .unwrap_or(DEFAULT_UDP_FORWARD_DATAGRAM_TTL_MS),
                ),
            )
            .map_err(|error| CliConfigError::PortForward(error.to_string()))?;
            ingresses.push(LocalIngressConfig {
                name: "udp-forward".to_string(),
                config: IngressConfig::UdpForward(config),
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
            let host = match self.tun_vpn_mode {
                None => TunHostConfig::External,
                Some(TunVpnModeArg::Full) => {
                    if !self.tun_include_cidrs.is_empty() {
                        return Err(CliConfigError::ManagedVpn(
                            "--tun-vpn-mode full cannot be combined with --tun-include-cidr"
                                .to_string(),
                        ));
                    }
                    TunHostConfig::Managed(ManagedVpnConfig {
                        route_mode: RouteMode::Full,
                        excludes: self.tun_exclude_cidrs.clone(),
                        local_lan: self.tun_local_lan,
                        dns_capture_servers: self.tun_dns_capture_servers.clone(),
                        platform: ManagedVpnPlatformConfig::default(),
                    })
                }
                Some(TunVpnModeArg::Split) => TunHostConfig::Managed(ManagedVpnConfig {
                    route_mode: RouteMode::Split(self.tun_include_cidrs.clone()),
                    excludes: self.tun_exclude_cidrs.clone(),
                    local_lan: self.tun_local_lan,
                    dns_capture_servers: self.tun_dns_capture_servers.clone(),
                    platform: ManagedVpnPlatformConfig::default(),
                }),
            };
            ingresses.push(LocalIngressConfig {
                name: "tun".to_string(),
                config: IngressConfig::TunL4(TunL4Config {
                    interface_name: self.tun_interface_name.clone(),
                    ipv4: tun_ipv4,
                    ipv4_prefix: self.tun_ipv4_prefix,
                    ipv4_gateway: self.tun_ipv4_gateway,
                    ipv6: self.tun_ipv6,
                    ipv6_prefix: self.tun_ipv6_prefix,
                    mtu: self.tun_mtu,
                    enable_icmp: !self.tun_disable_icmp,
                    dns_resolvers: self.tun_dns_resolvers.clone(),
                    dns_ttl_ms: self.tun_dns_ttl_ms,
                    host,
                }),
            });
        }
        let id = OutboundId::parse("cli-mpp")
            .map_err(|error| CliConfigError::ProductPolicy(error.to_string()))?;
        let outbound = MppOutboundConfig {
            paths: self
                .paths
                .into_iter()
                .enumerate()
                .map(|(index, spec)| ClientPathConfig {
                    name: format!("path-{}", index + 1),
                    tls: tls.clone(),
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
        };
        let policy = ProductPolicyConfig {
            generation: 1,
            routes: vec![RouteRuleSpec::new(
                RuleId::parse("default")
                    .map_err(|error| CliConfigError::ProductPolicy(error.to_string()))?,
                RouteMatchSpec::default(),
                RouteAction::new(
                    EgressAction::Outbound(id.clone()),
                    None,
                    InitialDemand::Automatic,
                ),
            )],
            destination_acl: Vec::new(),
        };
        Ok(NodeConfig {
            forwarding_mode: crate::config::ForwardingMode::L4,
            outbounds: vec![OutboundLeafConfig::Mpp {
                id,
                config: Box::new(outbound),
            }],
            gateway_balancers: Vec::new(),
            local_ingresses: ingresses,
            tun_l3_ingresses: Vec::new(),
            product_policy: Some(policy),
            dns_policy,
            servers: Vec::new(),
        })
    }
}

fn proxy_auth_config(
    username: Option<String>,
    password_file: Option<&Path>,
    password_environment: Option<&str>,
) -> Result<ProxyAuthConfig, CliConfigError> {
    match (username, password_file, password_environment) {
        (Some(username), Some(path), None) => {
            let password = secret_utf8(
                read_secret_file(path, "local proxy password")?,
                "local proxy password",
            )?;
            let principal = PrincipalId::parse(&username)
                .map_err(|error| CliConfigError::ProxyAuth(error.to_string()))?;
            let user = LocalProxyUser::new(username.clone(), principal, username, password)
                .map_err(CliConfigError::ProxyAuthConfig)?;
            ProxyAuthConfig::required([user]).map_err(CliConfigError::ProxyAuthConfig)
        }
        (Some(username), None, Some(name)) => {
            let password = secret_utf8(
                read_secret_environment(name, "local proxy password")?,
                "local proxy password",
            )?;
            let principal = PrincipalId::parse(&username)
                .map_err(|error| CliConfigError::ProxyAuth(error.to_string()))?;
            let user = LocalProxyUser::new(username.clone(), principal, username, password)
                .map_err(CliConfigError::ProxyAuthConfig)?;
            ProxyAuthConfig::required([user]).map_err(CliConfigError::ProxyAuthConfig)
        }
        (None, None, None) => Ok(ProxyAuthConfig::disabled()),
        (Some(_), None, None) => Err(CliConfigError::ProxyPasswordRequired),
        (None, Some(_), None) | (None, None, Some(_)) => Err(CliConfigError::ProxyUsernameRequired),
        _ => Err(CliConfigError::ProxyAuth(
            "exactly one local proxy password reference is required".to_string(),
        )),
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
        || args.tun_interface_name.is_some()
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
        || args.tun_vpn_mode.is_some()
        || !args.tun_include_cidrs.is_empty()
        || !args.tun_exclude_cidrs.is_empty()
        || args.tun_local_lan
        || !args.tun_dns_capture_servers.is_empty()
        || args.tun_dns_dot_bootstrap.is_some()
        || args.tun_dns_dot_server_name.is_some()
}

#[derive(Debug, Args)]
pub struct ServerArgs {
    /// Server carrier listener using one fixed PORT.
    #[arg(
        long = "bind-path",
        env = "MPTUNNEL_BIND_PATHS",
        value_delimiter = ',',
        required = true
    )]
    pub bind_paths: Vec<PathSpec>,

    #[arg(
        long = "tls-certificate-chain",
        env = "MPTUNNEL_TLS_CERTIFICATE_CHAIN",
        value_name = "PEM_FILE"
    )]
    pub tls_certificate_chain: Option<PathBuf>,

    #[arg(
        long = "tls-private-key",
        env = "MPTUNNEL_TLS_PRIVATE_KEY",
        value_name = "PEM_FILE"
    )]
    pub tls_private_key: Option<PathBuf>,

    /// Optional endpoint-wide 32-byte raw transport secret. This is not an
    /// MPP client credential and must match the client value.
    #[arg(
        long = "transport-secret-file",
        env = "MPTUNNEL_TRANSPORT_SECRET_FILE",
        value_name = "FILE"
    )]
    pub transport_secret_file: Option<PathBuf>,

    #[arg(
        long = "outbound-protocol",
        env = "MPTUNNEL_OUTBOUND_PROTOCOL",
        value_enum,
        default_value_t = OutboundArg::Direct
    )]
    pub outbound_protocol: OutboundArg,

    #[arg(long, env = "MPTUNNEL_OUTBOUND_BIND_IP")]
    pub outbound_bind_ip: Option<IpAddr>,

    #[arg(long, env = "MPTUNNEL_UPSTREAM_SOCKS5")]
    pub upstream_socks5: Option<Endpoint>,

    #[arg(long, env = "MPTUNNEL_UPSTREAM_HTTP")]
    pub upstream_http: Option<Endpoint>,

    #[arg(long, env = "MPTUNNEL_UPSTREAM_PROXY_USERNAME")]
    pub upstream_proxy_username: Option<String>,

    #[arg(
        long,
        env = "MPTUNNEL_UPSTREAM_PROXY_PASSWORD_FILE",
        value_name = "FILE",
        conflicts_with = "upstream_proxy_password_env"
    )]
    pub upstream_proxy_password_file: Option<PathBuf>,

    #[arg(
        long,
        env = "MPTUNNEL_UPSTREAM_PROXY_PASSWORD_ENV",
        value_name = "NAME",
        conflicts_with = "upstream_proxy_password_file"
    )]
    pub upstream_proxy_password_env: Option<String>,

    #[arg(long, env = "MPTUNNEL_UPSTREAM_HTTP_TLS_SERVER_NAME")]
    pub upstream_http_tls_server_name: Option<String>,

    #[arg(
        long = "outbound-dns-resolver",
        env = "MPTUNNEL_OUTBOUND_DNS_RESOLVERS",
        value_delimiter = ','
    )]
    pub outbound_dns_resolvers: Vec<SocketAddr>,

    #[arg(
        long,
        env = "MPTUNNEL_OUTBOUND_DNS_MODE",
        value_enum,
        default_value_t = DnsModeArg::System
    )]
    pub outbound_dns_mode: DnsModeArg,

    #[arg(
        long,
        env = "MPTUNNEL_OUTBOUND_DNS_STRATEGY",
        value_enum,
        default_value_t = DnsStrategyArg::Ipv4AndIpv6
    )]
    pub outbound_dns_strategy: DnsStrategyArg,

    #[arg(
        long,
        env = "MPTUNNEL_OUTBOUND_DNS_TIMEOUT_MS",
        default_value_t = DEFAULT_SIMPLE_DNS_TIMEOUT_MS
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
    fn into_config(self, security: ServerSecurityConfig) -> Result<NodeConfig, CliConfigError> {
        let tls = server_tls_from_cli(
            self.tls_certificate_chain,
            self.tls_private_key,
            self.transport_secret_file,
            security.auth_freshness_window,
            security.max_pending_authentications,
        )?;
        let credentials = match (
            self.upstream_proxy_username,
            self.upstream_proxy_password_file.as_deref(),
            self.upstream_proxy_password_env.as_deref(),
        ) {
            (Some(username), Some(path), None) => {
                let password = secret_utf8(
                    read_secret_file(path, "upstream proxy password")?,
                    "upstream proxy password",
                )?;
                Some(ProxyCredentials::new(username, password).map_err(CliConfigError::Outbound)?)
            }
            (Some(username), None, Some(name)) => {
                let password = secret_utf8(
                    read_secret_environment(name, "upstream proxy password")?,
                    "upstream proxy password",
                )?;
                Some(ProxyCredentials::new(username, password).map_err(CliConfigError::Outbound)?)
            }
            (None, None, None) => None,
            (Some(_), None, None) => {
                return Err(CliConfigError::UpstreamProxyPasswordRequired);
            }
            (None, Some(_), None) | (None, None, Some(_)) => {
                return Err(CliConfigError::UpstreamProxyUsernameRequired);
            }
            _ => {
                return Err(CliConfigError::UpstreamProxyAuth(
                    "exactly one upstream proxy password reference is required".to_string(),
                ));
            }
        };
        if matches!(
            self.outbound_protocol,
            OutboundArg::Direct | OutboundArg::Bind
        ) && credentials.is_some()
        {
            return Err(CliConfigError::UpstreamProxyAuthWithoutProxy);
        }
        if self.outbound_protocol != OutboundArg::HttpsConnect
            && self.upstream_http_tls_server_name.is_some()
        {
            return Err(CliConfigError::UpstreamTlsNameWithoutHttps);
        }
        let outbound = match self.outbound_protocol {
            OutboundArg::Direct => OutboundConfig::Direct,
            OutboundArg::Bind => OutboundConfig::BindSourceIp(
                self.outbound_bind_ip
                    .ok_or(CliConfigError::MissingOutboundBindIp)?,
            ),
            OutboundArg::Socks5 => OutboundConfig::Socks5(ProxyConfig::new(
                self.upstream_socks5
                    .ok_or(CliConfigError::MissingUpstreamSocks5)?,
                credentials.clone(),
            )),
            OutboundArg::HttpConnect => OutboundConfig::HttpConnect(ProxyConfig::new(
                self.upstream_http
                    .ok_or(CliConfigError::MissingUpstreamHttp)?,
                credentials.clone(),
            )),
            OutboundArg::HttpsConnect => OutboundConfig::HttpsConnect(Box::new(
                HttpsProxyConfig::new(
                    ProxyConfig::new(
                        self.upstream_http
                            .ok_or(CliConfigError::MissingUpstreamHttp)?,
                        credentials.clone(),
                    ),
                    self.upstream_http_tls_server_name,
                    Vec::new(),
                )
                .map_err(CliConfigError::Outbound)?,
            )),
        };
        let id = OutboundId::parse("cli-egress")
            .map_err(|error| CliConfigError::ProductPolicy(error.to_string()))?;
        let (dns_policy, dns_plan) = simple_dns_policy(
            self.outbound_dns_mode,
            self.outbound_dns_resolvers,
            self.outbound_dns_strategy.into(),
            Duration::from_millis(self.outbound_dns_timeout_ms),
        )?;
        let server = MppInboundConfig {
            name: "cli-mpp-inbound".to_string(),
            egress: EgressRef::Outbound(id.clone()),
            dns_plan: Some(dns_plan),
            paths: self
                .bind_paths
                .into_iter()
                .enumerate()
                .map(|(index, spec)| NamedPathConfig {
                    name: format!("path-{}", index + 1),
                    spec,
                })
                .collect(),
            security,
            tls,
            destination_acl: ServerDestinationAclConfig::default(),
            performance: MppPerformanceConfig {
                extra_traffic_hint_percent: self.extra_traffic_hint_percent,
            },
            tun_l3: None,
        };
        Ok(NodeConfig {
            forwarding_mode: crate::config::ForwardingMode::L4,
            outbounds: vec![OutboundLeafConfig::Local {
                id,
                config: outbound,
                connect_timeout: Duration::from_millis(self.outbound_connect_timeout_ms),
            }],
            gateway_balancers: Vec::new(),
            local_ingresses: Vec::new(),
            tun_l3_ingresses: Vec::new(),
            product_policy: None,
            dns_policy,
            servers: vec![server],
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum DnsModeArg {
    System,
    Servers,
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

fn simple_dns_policy(
    mode: DnsModeArg,
    resolvers: Vec<SocketAddr>,
    strategy: DnsIpStrategy,
    timeout: Duration,
) -> Result<(DnsPolicyConfig, DnsPlanId), CliConfigError> {
    let plan_id =
        DnsPlanId::parse("default").map_err(|error| CliConfigError::Dns(error.to_string()))?;
    let upstreams = match mode {
        DnsModeArg::System if resolvers.is_empty() => vec![DnsUpstreamSpec::direct(
            DnsUpstreamId::parse("system")
                .map_err(|error| CliConfigError::Dns(error.to_string()))?,
            DnsUpstreamEndpoint::System,
        )],
        DnsModeArg::System => {
            return Err(CliConfigError::Dns(
                "--outbound-dns-mode system cannot be combined with --outbound-dns-resolver"
                    .to_string(),
            ));
        }
        DnsModeArg::Servers if resolvers.is_empty() => {
            return Err(CliConfigError::Dns(
                "--outbound-dns-mode servers requires at least one --outbound-dns-resolver"
                    .to_string(),
            ));
        }
        DnsModeArg::Servers => resolvers
            .into_iter()
            .enumerate()
            .map(|(index, bootstrap)| {
                Ok(DnsUpstreamSpec::direct(
                    DnsUpstreamId::parse(&format!("resolver-{}", index + 1))
                        .map_err(|error| CliConfigError::Dns(error.to_string()))?,
                    DnsUpstreamEndpoint::UdpTcp { bootstrap },
                ))
            })
            .collect::<Result<Vec<_>, CliConfigError>>()?,
    };
    let limits = DnsPlanLimits {
        lookup_timeout: timeout,
        ..DnsPlanLimits::default()
    };
    let plan = DnsPlanSpec {
        id: plan_id.clone(),
        upstreams: upstreams
            .iter()
            .map(|upstream| upstream.id.clone())
            .collect(),
        ip_strategy: strategy,
        security: crate::product::DnsSecurityPolicy::AllowPlaintext,
        upstream_strategy: crate::product::DnsUpstreamStrategy::Ordered,
        expected_cidrs: Vec::new(),
        limits,
    };
    let config = DnsPolicyConfig {
        generation: 1,
        spec: DnsPolicySpec {
            upstreams,
            outbound_capabilities: Vec::new(),
            plans: vec![plan],
            rules: Vec::new(),
            hosts: Vec::new(),
            fake_dns: None,
            default_plan: plan_id.clone(),
        },
    };
    config
        .compile()
        .map_err(|error| CliConfigError::Dns(error.to_string()))?;
    Ok((config, plan_id))
}

fn managed_dot_dns_policy(
    bootstrap: SocketAddr,
    server_name: &str,
) -> Result<DnsPolicyConfig, CliConfigError> {
    let plan_id =
        DnsPlanId::parse("default").map_err(|error| CliConfigError::Dns(error.to_string()))?;
    let upstream_id = DnsUpstreamId::parse("managed-dot")
        .map_err(|error| CliConfigError::Dns(error.to_string()))?;
    let server_name =
        DomainName::parse(server_name).map_err(|error| CliConfigError::Dns(error.to_string()))?;
    let upstream = DnsUpstreamSpec::direct(
        upstream_id.clone(),
        DnsUpstreamEndpoint::Tls {
            bootstrap,
            server_name,
        },
    );
    let config = DnsPolicyConfig {
        generation: 1,
        spec: DnsPolicySpec {
            upstreams: vec![upstream],
            outbound_capabilities: Vec::new(),
            plans: vec![DnsPlanSpec {
                id: plan_id.clone(),
                upstreams: vec![upstream_id],
                ip_strategy: DnsIpStrategy::Ipv4AndIpv6,
                security: DnsSecurityPolicy::RequireEncrypted,
                upstream_strategy: crate::product::DnsUpstreamStrategy::Ordered,
                expected_cidrs: Vec::new(),
                limits: DnsPlanLimits::default(),
            }],
            rules: Vec::new(),
            hosts: Vec::new(),
            fake_dns: None,
            default_plan: plan_id,
        },
    };
    config
        .compile()
        .map_err(|error| CliConfigError::Dns(error.to_string()))?;
    Ok(config)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutboundArg {
    Direct,
    Bind,
    Socks5,
    HttpConnect,
    HttpsConnect,
}

fn client_tls_from_cli(
    server_name: Option<String>,
    pinned_certificate: Option<PathBuf>,
    transport_secret_file: Option<PathBuf>,
) -> Result<TcpClientTlsConfig, CliConfigError> {
    let tls = match pinned_certificate {
        None => Err(CliConfigError::MppTls(
            "MPP client paths require --tls-pinned-certificate".to_string(),
        )),
        Some(path) => {
            let certificates = crate::config::load_certificates(Path::new("."), &path)
                .map_err(|error| CliConfigError::MppTls(error.to_string()))?;
            let [pinned_leaf] = certificates.as_slice() else {
                return Err(CliConfigError::MppTls(
                    "--tls-pinned-certificate must contain exactly one certificate".to_string(),
                ));
            };
            TcpClientTlsConfig::new(
                server_name.unwrap_or_else(|| DEFAULT_MPP_TLS_SERVER_NAME.to_string()),
                pinned_leaf.clone(),
            )
            .map_err(|error| CliConfigError::MppTls(error.to_string()))
        }
    }?;
    Ok(match load_cli_transport_secret(transport_secret_file)? {
        Some(secret) => tls.with_shared_transport_secret(secret),
        None => tls,
    })
}

fn server_tls_from_cli(
    certificate_chain: Option<PathBuf>,
    private_key: Option<PathBuf>,
    transport_secret_file: Option<PathBuf>,
    freshness_window: Duration,
    max_pending_authentications: usize,
) -> Result<TcpServerTlsConfig, CliConfigError> {
    let tls = match (certificate_chain, private_key) {
        (None, _) => Err(CliConfigError::MppTls(
            "MPP server paths require --tls-certificate-chain".to_string(),
        )),
        (_, None) => Err(CliConfigError::MppTls(
            "MPP server paths require --tls-private-key".to_string(),
        )),
        (Some(chain_path), Some(key_path)) => {
            let chain = crate::config::load_certificates(Path::new("."), &chain_path)
                .map_err(|error| CliConfigError::MppTls(error.to_string()))?;
            let key = crate::config::load_private_key(Path::new("."), &key_path)
                .map_err(|error| CliConfigError::MppTls(error.to_string()))?;
            TcpServerTlsConfig::new(chain, key)
                .map_err(|error| CliConfigError::MppTls(error.to_string()))
        }
    }?;
    Ok(match load_cli_transport_secret(transport_secret_file)? {
        Some(secret) => {
            tls.with_shared_transport_secret(secret, freshness_window, max_pending_authentications)
        }
        None => tls,
    })
}

fn load_cli_transport_secret(
    transport_secret_file: Option<PathBuf>,
) -> Result<Option<SharedTransportSecret>, CliConfigError> {
    let Some(path) = transport_secret_file else {
        return Ok(None);
    };
    let bytes = std::fs::read(&path).map_err(|error| {
        CliConfigError::MppTransportSecret(format!(
            "failed to read shared transport secret {}: {error}",
            path.display()
        ))
    })?;
    let bytes: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        CliConfigError::MppTransportSecret(format!(
            "shared transport secret {} must contain exactly 32 raw bytes, found {}",
            path.display(),
            bytes.len()
        ))
    })?;
    Ok(Some(SharedTransportSecret::new(bytes)))
}

#[derive(Debug)]
pub enum CliConfigError {
    Config(crate::config::ConfigError),
    Security(crate::config::SecurityPolicyError),
    Credential(String),
    CredentialSecret(String),
    SecretMaterial(String),
    ProxyAuth(String),
    ProxyAuthConfig(ProxyAuthConfigError),
    MissingOutboundBindIp,
    MissingUpstreamSocks5,
    MissingUpstreamHttp,
    Outbound(crate::outbound::OutboundError),
    UpstreamProxyUsernameRequired,
    UpstreamProxyPasswordRequired,
    UpstreamProxyAuth(String),
    UpstreamProxyAuthWithoutProxy,
    UpstreamTlsNameWithoutHttps,
    TunIpv4DisabledWithIpv4Options,
    ManagedVpn(String),
    ManagedDnsDotRequired,
    ManagedDnsDotPairRequired,
    ManagedDnsDotRequiresVpnMode,
    ProxyUsernameRequired,
    ProxyPasswordRequired,
    MppTls(String),
    MppTransportSecret(String),
    PortForward(String),
    ProductPolicy(String),
    Dns(String),
    OperationalCommandNotRuntimeConfig,
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

impl From<crate::config::SecretMaterialError> for CliConfigError {
    fn from(value: crate::config::SecretMaterialError) -> Self {
        Self::SecretMaterial(value.to_string())
    }
}

impl std::fmt::Display for CliConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(err) => write!(f, "{err}"),
            Self::Security(err) => write!(f, "{err}"),
            Self::Credential(error) => write!(f, "invalid credential: {error}"),
            Self::CredentialSecret(error) => write!(f, "{error}"),
            Self::SecretMaterial(error) => write!(f, "{error}"),
            Self::ProxyAuth(error) => write!(f, "invalid local proxy authentication: {error}"),
            Self::ProxyAuthConfig(error) => {
                write!(f, "invalid local proxy authentication: {error}")
            }
            Self::MissingOutboundBindIp => {
                write!(f, "--outbound bind requires --outbound-bind-ip")
            }
            Self::MissingUpstreamSocks5 => {
                write!(f, "--outbound socks5 requires --upstream-socks5")
            }
            Self::MissingUpstreamHttp => {
                write!(
                    f,
                    "--outbound http-connect/https-connect requires --upstream-http"
                )
            }
            Self::Outbound(err) => write!(f, "{err}"),
            Self::UpstreamProxyUsernameRequired => {
                write!(
                    f,
                    "--upstream-proxy-password-file/--upstream-proxy-password-env requires --upstream-proxy-username"
                )
            }
            Self::UpstreamProxyPasswordRequired => {
                write!(
                    f,
                    "--upstream-proxy-username requires --upstream-proxy-password-file or --upstream-proxy-password-env"
                )
            }
            Self::UpstreamProxyAuth(error) => {
                write!(f, "invalid upstream proxy authentication: {error}")
            }
            Self::UpstreamProxyAuthWithoutProxy => {
                write!(f, "upstream proxy credentials require a proxied outbound")
            }
            Self::UpstreamTlsNameWithoutHttps => {
                write!(
                    f,
                    "--upstream-http-tls-server-name requires --outbound https-connect"
                )
            }
            Self::TunIpv4DisabledWithIpv4Options => {
                write!(
                    f,
                    "--tun-disable-ipv4 cannot be combined with --tun-ipv4 or --tun-ipv4-gateway"
                )
            }
            Self::ManagedVpn(error) => {
                write!(f, "invalid managed VPN configuration: {error}")
            }
            Self::ManagedDnsDotRequired => write!(
                f,
                "--tun-vpn-mode requires --tun-dns-dot-bootstrap and --tun-dns-dot-server-name"
            ),
            Self::ManagedDnsDotPairRequired => write!(
                f,
                "--tun-dns-dot-bootstrap and --tun-dns-dot-server-name must be set together"
            ),
            Self::ManagedDnsDotRequiresVpnMode => {
                write!(f, "managed DNS-over-TLS options require --tun-vpn-mode")
            }
            Self::ProxyUsernameRequired => {
                write!(
                    f,
                    "--proxy-password-file/--proxy-password-env requires --proxy-username"
                )
            }
            Self::ProxyPasswordRequired => {
                write!(
                    f,
                    "--proxy-username requires --proxy-password-file or --proxy-password-env"
                )
            }
            Self::MppTls(message) => write!(f, "{message}"),
            Self::MppTransportSecret(message) => write!(f, "{message}"),
            Self::PortForward(message) => write!(f, "invalid port-forward inbound: {message}"),
            Self::ProductPolicy(message) => write!(f, "{message}"),
            Self::Dns(message) => write!(f, "invalid DNS configuration: {message}"),
            Self::OperationalCommandNotRuntimeConfig => {
                write!(f, "operational commands do not build a runtime config")
            }
        }
    }
}

impl std::error::Error for CliConfigError {}

#[cfg(test)]
#[path = "tests_cli.rs"]
mod tests;
