use super::security::{
    CipherSuite, EncryptionMode, SecurityPolicyError, SharedSecret, TransportIntegrity,
    TransportSecurity, validate_transport_security,
};
use crate::ingress::{IngressConfig, ProxyAuthConfig};
use crate::outbound::{DnsConfig, OutboundConfig};
use crate::protocol::codec::CodecLimits;
use crate::transport::PathSpec;
use std::net::SocketAddr;
use std::time::Duration;

pub const DEFAULT_PATH_PROBE_INTERVAL_MS: u64 = 10_000;
pub const DEFAULT_PATH_PROBE_TIMEOUT_MS: u64 = 2_000;
pub const DEFAULT_PATH_PROBE_INTERVAL: Duration =
    Duration::from_millis(DEFAULT_PATH_PROBE_INTERVAL_MS);
pub const DEFAULT_PATH_PROBE_TIMEOUT: Duration =
    Duration::from_millis(DEFAULT_PATH_PROBE_TIMEOUT_MS);
pub const DEFAULT_STREAM_WINDOW_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_REPAIR_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_REORDER_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_DATAGRAM_QUEUE_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_PATH_FLIGHT_BYTES: usize = DEFAULT_REPAIR_BYTES;
pub const DEFAULT_MAX_RELIABLE_RELAY_CHUNK_BYTES: usize = 512 * 1024;
pub const DEFAULT_MAX_STREAMS: usize = 65_536;
pub const DEFAULT_MAX_QUIC_CONCURRENT_BIDI_STREAMS: usize = DEFAULT_MAX_STREAMS;
pub const DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL_MS: u64 = 10_000;
pub const DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL: Duration =
    Duration::from_millis(DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL_MS);
pub const DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT: Duration =
    Duration::from_millis(DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT_MS);
pub const DEFAULT_RESTART_BACKOFF_MS: u64 = 1_000;
pub const DEFAULT_RESTART_MAX_BACKOFF_MS: u64 = 30_000;
pub const DEFAULT_RESTART_BACKOFF: Duration = Duration::from_millis(DEFAULT_RESTART_BACKOFF_MS);
pub const DEFAULT_RESTART_MAX_BACKOFF: Duration =
    Duration::from_millis(DEFAULT_RESTART_MAX_BACKOFF_MS);
pub const DEFAULT_AUTH_FRESHNESS_WINDOW_SECONDS: u64 = 300;
pub const DEFAULT_AUTH_FRESHNESS_WINDOW: Duration =
    Duration::from_secs(DEFAULT_AUTH_FRESHNESS_WINDOW_SECONDS);
pub const DEFAULT_OUTBOUND_CONNECT_TIMEOUT_MS: u64 = 10_000;
pub const DEFAULT_OUTBOUND_CONNECT_TIMEOUT: Duration =
    Duration::from_millis(DEFAULT_OUTBOUND_CONNECT_TIMEOUT_MS);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppConfig {
    /// Process-level logging/check behavior. It does not own protocol state.
    pub log_level: String,
    pub check_config: bool,
    /// Process supervision behavior, separate from data-plane ownership.
    pub service: ServiceConfig,
    /// Runtime envelopes shared by product streams, datagram flows, and carriers.
    pub resources: ResourceLimits,
    /// Observation/control plane. It must not become a hidden data-plane owner.
    pub management: ManagementConfig,
    /// Representative process security for CLI/default checks. MPP peer secrets
    /// are scoped to each MPP inbound/outbound/path where configured.
    pub security: SecurityConfig,
    /// Role-free runtime graph: client, server, or a node containing both.
    pub command: CommandConfig,
}

impl AppConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_transport_security(
            self.security.mode,
            self.security.transport,
            self.security.integrity,
            &self.security.secret,
        )?;
        if self.security.auth_freshness_window.is_zero() {
            return Err(ConfigError::AuthFreshnessWindowZero);
        }
        self.service.validate()?;
        self.resources.validate()?;
        self.management.validate()?;
        match &self.command {
            CommandConfig::Client(client) => validate_client_config(client, self.resources)?,
            CommandConfig::Server(server) => validate_server_config(server, self.resources)?,
            CommandConfig::Node(node) => {
                if node.clients.is_empty() && node.servers.is_empty() {
                    return Err(ConfigError::NoRuntimeServices);
                }
                for client in &node.clients {
                    validate_client_config(client, self.resources)?;
                }
                for server in &node.servers {
                    validate_server_config(server, self.resources)?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ManagementConfig {
    pub listen: Vec<SocketAddr>,
    pub token: Option<String>,
}

impl ManagementConfig {
    pub fn enabled(&self) -> bool {
        !self.listen.is_empty()
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.listen.iter().any(|addr| addr.port() == 0) {
            return Err(ConfigError::ManagementListenPortZero);
        }
        if self.token.as_ref().is_some_and(|token| token.is_empty()) {
            return Err(ConfigError::ManagementTokenEmpty);
        }
        Ok(())
    }
}

pub const DEFAULT_EXTRA_TRAFFIC_HINT_PERCENT: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MppPerformanceConfig {
    /// Operator hint for adaptive duplicate/probe/repair overhead, in percent.
    ///
    /// 1 means the sender treats roughly 1% extra transport traffic as
    /// acceptable when runtime evidence shows that additional repair/probing can
    /// reduce stalls. 100 permits full duplication in pathological cases, and
    /// values above 100 bias toward redundant repair under severe instability.
    /// This is a hint, not a production hard limit.
    pub extra_traffic_hint_percent: u16,
}

impl Default for MppPerformanceConfig {
    fn default() -> Self {
        Self {
            extra_traffic_hint_percent: DEFAULT_EXTRA_TRAFFIC_HINT_PERCENT,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServiceConfig {
    pub service_mode: bool,
    pub supervise: bool,
    pub restart_backoff: Duration,
    pub restart_max_backoff: Duration,
    pub max_restarts: Option<u32>,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            service_mode: false,
            supervise: false,
            restart_backoff: DEFAULT_RESTART_BACKOFF,
            restart_max_backoff: DEFAULT_RESTART_MAX_BACKOFF,
            max_restarts: None,
        }
    }
}

impl ServiceConfig {
    pub fn validate(self) -> Result<(), ConfigError> {
        if self.restart_backoff.is_zero() {
            return Err(ConfigError::RestartBackoffZero);
        }
        if self.restart_max_backoff.is_zero() {
            return Err(ConfigError::RestartMaxBackoffZero);
        }
        if self.restart_max_backoff < self.restart_backoff {
            return Err(ConfigError::RestartMaxBackoffTooSmall);
        }
        if self.max_restarts == Some(0) {
            return Err(ConfigError::RestartLimitZero);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceLimits {
    pub max_frame_bytes: usize,
    pub max_payload_bytes: usize,
    pub max_ack_ranges: usize,
    pub max_paths: usize,
    pub max_streams: usize,
    pub max_quic_concurrent_bidi_streams: usize,
    pub max_stream_window_bytes: u64,
    pub max_repair_bytes: usize,
    pub max_reorder_bytes: usize,
    pub max_datagram_queue_bytes: usize,
    pub max_path_flight_bytes: usize,
    pub max_reliable_relay_chunk_bytes: usize,
    pub tcp_path_heartbeat_interval: Duration,
    pub tcp_path_heartbeat_timeout: Duration,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1_048_576,
            max_payload_bytes: 1_048_512,
            max_ack_ranges: 256,
            max_paths: 64,
            max_streams: DEFAULT_MAX_STREAMS,
            max_quic_concurrent_bidi_streams: DEFAULT_MAX_QUIC_CONCURRENT_BIDI_STREAMS,
            max_stream_window_bytes: DEFAULT_STREAM_WINDOW_BYTES,
            max_repair_bytes: DEFAULT_REPAIR_BYTES,
            max_reorder_bytes: DEFAULT_REORDER_BYTES,
            max_datagram_queue_bytes: DEFAULT_DATAGRAM_QUEUE_BYTES,
            max_path_flight_bytes: DEFAULT_PATH_FLIGHT_BYTES,
            max_reliable_relay_chunk_bytes: DEFAULT_MAX_RELIABLE_RELAY_CHUNK_BYTES,
            tcp_path_heartbeat_interval: DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
            tcp_path_heartbeat_timeout: DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
        }
    }
}

impl ResourceLimits {
    pub fn validate(self) -> Result<(), ConfigError> {
        if self.max_frame_bytes < 64 {
            return Err(ConfigError::FrameLimitTooSmall);
        }
        if self.max_payload_bytes > self.max_frame_bytes.saturating_sub(16) {
            return Err(ConfigError::PayloadLimitExceedsFrameLimit);
        }
        if self.max_ack_ranges == 0 {
            return Err(ConfigError::AckRangeLimitZero);
        }
        if self.max_paths == 0 {
            return Err(ConfigError::PathLimitZero);
        }
        if self.max_paths > u16::MAX as usize {
            return Err(ConfigError::PathLimitTooLarge);
        }
        if self.max_streams == 0 {
            return Err(ConfigError::StreamLimitZero);
        }
        if self.max_quic_concurrent_bidi_streams == 0 {
            return Err(ConfigError::QuicBidiStreamLimitZero);
        }
        if self.max_stream_window_bytes == 0 {
            return Err(ConfigError::StreamWindowLimitZero);
        }
        if self.max_repair_bytes < self.max_payload_bytes {
            return Err(ConfigError::RepairLimitTooSmall);
        }
        if self.max_reorder_bytes < self.max_payload_bytes {
            return Err(ConfigError::ReorderLimitTooSmall);
        }
        if self.max_datagram_queue_bytes < self.max_payload_bytes {
            return Err(ConfigError::DatagramQueueLimitTooSmall);
        }
        if self.max_reliable_relay_chunk_bytes == 0 {
            return Err(ConfigError::MaxReliableRelayChunkBytesZero);
        }
        if self.max_reliable_relay_chunk_bytes > self.max_payload_bytes {
            return Err(ConfigError::MaxReliableRelayChunkExceedsPayloadLimit);
        }
        if self.max_path_flight_bytes < self.max_reliable_relay_chunk_bytes {
            return Err(ConfigError::PathFlightLimitTooSmall);
        }
        if self.max_path_flight_bytes > self.max_repair_bytes {
            return Err(ConfigError::PathFlightLimitExceedsRepairLimit);
        }
        if self.tcp_path_heartbeat_interval.is_zero() {
            return Err(ConfigError::TcpPathHeartbeatIntervalZero);
        }
        if self.tcp_path_heartbeat_timeout.is_zero() {
            return Err(ConfigError::TcpPathHeartbeatTimeoutZero);
        }
        if self.tcp_path_heartbeat_timeout < self.tcp_path_heartbeat_interval {
            return Err(ConfigError::TcpPathHeartbeatTimeoutTooSmall);
        }
        Ok(())
    }
}

impl From<ResourceLimits> for CodecLimits {
    fn from(value: ResourceLimits) -> Self {
        Self {
            max_frame_bytes: value.max_frame_bytes,
            max_payload_bytes: value.max_payload_bytes,
            max_ack_ranges: value.max_ack_ranges,
            max_host_bytes: 255,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityConfig {
    pub mode: EncryptionMode,
    pub transport: TransportSecurity,
    pub integrity: TransportIntegrity,
    pub cipher: CipherSuite,
    pub secret: SharedSecret,
    pub auth_freshness_window: Duration,
}

impl SecurityConfig {
    pub fn encrypted(secret: SharedSecret) -> Self {
        Self::encrypted_with_cipher(secret, CipherSuite::default())
    }

    pub fn encrypted_with_cipher(secret: SharedSecret, cipher: CipherSuite) -> Self {
        Self {
            mode: EncryptionMode::Required,
            transport: TransportSecurity::Encrypted,
            integrity: TransportIntegrity::Authenticated,
            cipher,
            secret,
            auth_freshness_window: DEFAULT_AUTH_FRESHNESS_WINDOW,
        }
    }

    pub fn plaintext_lab(secret: SharedSecret) -> Self {
        Self::plaintext_lab_with_cipher(secret, CipherSuite::default())
    }

    pub fn plaintext_lab_with_cipher(secret: SharedSecret, cipher: CipherSuite) -> Self {
        Self {
            mode: EncryptionMode::AllowPlaintextLab,
            transport: TransportSecurity::Plaintext,
            integrity: TransportIntegrity::Authenticated,
            cipher,
            secret,
            auth_freshness_window: DEFAULT_AUTH_FRESHNESS_WINDOW,
        }
    }

    pub fn warning(&self) -> Option<&'static str> {
        self.mode.plaintext_warning()
    }

    pub fn with_auth_freshness_window(mut self, value: Duration) -> Self {
        self.auth_freshness_window = value;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandConfig {
    Client(ClientConfig),
    Server(ServerConfig),
    Node(NodeConfig),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeConfig {
    pub clients: Vec<ClientConfig>,
    pub servers: Vec<ServerConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteTargetKind {
    Outbound,
    Balancer,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteTarget {
    pub kind: RouteTargetKind,
    pub tag: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    /// Route selected by local inbounds: one MPP outbound tag or MPP balancer tag.
    pub route_target: Option<RouteTarget>,
    /// Local SOCKS5/HTTP/TUN ingress surfaces owned by this client service.
    pub ingresses: Vec<LocalIngressConfig>,
    /// Representative security for process-level validation; live path security
    /// is stored per `ClientPathConfig`.
    pub security: SecurityConfig,
    /// Candidate MPP carrier paths. Each path owns its own peer security.
    pub paths: Vec<ClientPathConfig>,
    pub path_probe_interval: Duration,
    pub path_probe_timeout: Duration,
    /// MPP sender behavior for this outbound path group.
    pub performance: MppPerformanceConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalIngressConfig {
    pub tag: Option<String>,
    pub config: IngressConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientPathConfig {
    /// One configured carrier path for an MPP outbound.
    pub spec: PathSpec,
    /// Security scoped to this path's MPP peer relationship.
    pub security: SecurityConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    /// Display/routing tag for this MPP inbound.
    pub tag: Option<String>,
    /// Egress outbound or egress balancer selected for accepted MPP flows.
    pub route_target: Option<RouteTarget>,
    /// Carrier listen/bind paths owned by this MPP inbound.
    pub bind_paths: Vec<PathSpec>,
    /// Security scoped to peers that join this MPP inbound.
    pub security: SecurityConfig,
    /// Resolved egress behavior for accepted target TCP/UDP flows.
    pub outbound: OutboundConfig,
    /// DNS policy owned by the selected egress behavior.
    pub outbound_dns: DnsConfig,
    /// Target connect timeout owned by the selected egress behavior.
    pub outbound_connect_timeout: Duration,
    /// MPP sender behavior for streams accepted by this inbound path group.
    pub performance: MppPerformanceConfig,
}

fn validate_client_config(
    client: &ClientConfig,
    resources: ResourceLimits,
) -> Result<(), ConfigError> {
    if client.paths.is_empty() {
        return Err(ConfigError::NoPaths);
    }
    if client.ingresses.is_empty() {
        return Err(ConfigError::NoIngresses);
    }
    for ingress in &client.ingresses {
        validate_ingress(&ingress.config)?;
        if let IngressConfig::TunL4(tun) = &ingress.config {
            validate_tun_l4(tun)?;
        }
    }
    validate_security_config(&client.security)?;
    for ingress in &client.ingresses {
        match &ingress.config {
            IngressConfig::Socks5 { proxy_auth, .. }
            | IngressConfig::HttpConnect { proxy_auth, .. } => {
                validate_proxy_auth(proxy_auth)?;
            }
            IngressConfig::TunL4(_) => {}
        }
    }
    if client.paths.len() > resources.max_paths {
        return Err(ConfigError::TooManyPaths {
            actual: client.paths.len(),
            limit: resources.max_paths,
        });
    }
    if client.path_probe_interval.is_zero() {
        return Err(ConfigError::PathProbeIntervalZero);
    }
    if client.path_probe_timeout.is_zero() {
        return Err(ConfigError::PathProbeTimeoutZero);
    }
    Ok(())
}

fn validate_server_config(
    server: &ServerConfig,
    resources: ResourceLimits,
) -> Result<(), ConfigError> {
    if server.bind_paths.is_empty() {
        return Err(ConfigError::NoPaths);
    }
    if server.bind_paths.len() > resources.max_paths {
        return Err(ConfigError::TooManyPaths {
            actual: server.bind_paths.len(),
            limit: resources.max_paths,
        });
    }
    validate_security_config(&server.security)?;
    server.outbound_dns.validate()?;
    if server.outbound_connect_timeout.is_zero() {
        return Err(ConfigError::OutboundConnectTimeoutZero);
    }
    Ok(())
}

fn validate_security_config(security: &SecurityConfig) -> Result<(), ConfigError> {
    validate_transport_security(
        security.mode,
        security.transport,
        security.integrity,
        &security.secret,
    )?;
    if security.auth_freshness_window.is_zero() {
        return Err(ConfigError::AuthFreshnessWindowZero);
    }
    Ok(())
}

impl DnsConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.timeout.is_zero() {
            return Err(ConfigError::OutboundDnsTimeoutZero);
        }
        if self.resolvers.iter().any(|resolver| resolver.port() == 0) {
            return Err(ConfigError::OutboundDnsResolverPortZero);
        }
        Ok(())
    }
}

fn validate_ingress(ingress: &IngressConfig) -> Result<(), ConfigError> {
    match ingress {
        IngressConfig::Socks5 { listen, .. } | IngressConfig::HttpConnect { listen, .. } => {
            if listen.is_empty() {
                return Err(ConfigError::NoListenAddresses);
            }
        }
        IngressConfig::TunL4(_) => {}
    }
    Ok(())
}

fn validate_tun_l4(tun: &crate::ingress::tun::TunL4Config) -> Result<(), ConfigError> {
    if tun.ipv4.is_none() && tun.ipv6.is_none() {
        return Err(ConfigError::TunAddressRequired);
    }
    if tun.ipv4_prefix > 32 {
        return Err(ConfigError::TunIpv4PrefixInvalid);
    }
    if tun.ipv6_prefix > 128 {
        return Err(ConfigError::TunIpv6PrefixInvalid);
    }
    if tun.mtu < 576 {
        return Err(ConfigError::TunMtuTooSmall);
    }
    if tun.ipv6.is_some() && tun.mtu < 1280 {
        return Err(ConfigError::TunIpv6MtuTooSmall);
    }
    if tun.dns_ttl_ms == 0 {
        return Err(ConfigError::TunDnsTtlZero);
    }
    if tun
        .dns_resolvers
        .iter()
        .any(|resolver| resolver.port() == 0)
    {
        return Err(ConfigError::TunDnsResolverPortZero);
    }
    Ok(())
}

fn validate_proxy_auth(auth: &ProxyAuthConfig) -> Result<(), ConfigError> {
    let Some(credentials) = auth.credentials() else {
        return Ok(());
    };
    if credentials.username().is_empty() {
        return Err(ConfigError::ProxyAuthUsernameEmpty);
    }
    if credentials.password().is_empty() {
        return Err(ConfigError::ProxyAuthPasswordEmpty);
    }
    if credentials.username().len() > u8::MAX as usize {
        return Err(ConfigError::ProxyAuthUsernameTooLong);
    }
    if credentials.password().len() > u8::MAX as usize {
        return Err(ConfigError::ProxyAuthPasswordTooLong);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    Security(SecurityPolicyError),
    AuthFreshnessWindowZero,
    NoPaths,
    FrameLimitTooSmall,
    PayloadLimitExceedsFrameLimit,
    AckRangeLimitZero,
    PathLimitZero,
    PathLimitTooLarge,
    StreamLimitZero,
    QuicBidiStreamLimitZero,
    StreamWindowLimitZero,
    RepairLimitTooSmall,
    ReorderLimitTooSmall,
    DatagramQueueLimitTooSmall,
    MaxReliableRelayChunkBytesZero,
    MaxReliableRelayChunkExceedsPayloadLimit,
    PathFlightLimitTooSmall,
    PathFlightLimitExceedsRepairLimit,
    TcpPathHeartbeatIntervalZero,
    TcpPathHeartbeatTimeoutZero,
    TcpPathHeartbeatTimeoutTooSmall,
    RestartBackoffZero,
    RestartMaxBackoffZero,
    RestartMaxBackoffTooSmall,
    RestartLimitZero,
    NoIngresses,
    NoListenAddresses,
    TooManyPaths { actual: usize, limit: usize },
    PathProbeIntervalZero,
    PathProbeTimeoutZero,
    TunAddressRequired,
    TunIpv4PrefixInvalid,
    TunIpv6PrefixInvalid,
    TunMtuTooSmall,
    TunIpv6MtuTooSmall,
    TunDnsTtlZero,
    TunDnsResolverPortZero,
    OutboundDnsTimeoutZero,
    OutboundDnsResolverPortZero,
    OutboundConnectTimeoutZero,
    ProxyAuthUsernameEmpty,
    ProxyAuthPasswordEmpty,
    ProxyAuthUsernameTooLong,
    ProxyAuthPasswordTooLong,
    ManagementListenPortZero,
    ManagementTokenEmpty,
    NoRuntimeServices,
}

impl From<SecurityPolicyError> for ConfigError {
    fn from(value: SecurityPolicyError) -> Self {
        Self::Security(value)
    }
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Security(err) => write!(f, "{err}"),
            Self::AuthFreshnessWindowZero => {
                write!(f, "auth freshness window must be greater than zero")
            }
            Self::NoPaths => write!(f, "at least one TCP or UDP path is required"),
            Self::FrameLimitTooSmall => write!(f, "max frame bytes must be at least 64"),
            Self::PayloadLimitExceedsFrameLimit => {
                write!(f, "max payload bytes must fit inside max frame bytes")
            }
            Self::AckRangeLimitZero => write!(f, "max ack ranges must be greater than zero"),
            Self::PathLimitZero => write!(f, "max paths must be greater than zero"),
            Self::PathLimitTooLarge => write!(f, "max paths must fit in protocol path IDs"),
            Self::StreamLimitZero => write!(f, "max streams must be greater than zero"),
            Self::QuicBidiStreamLimitZero => {
                write!(
                    f,
                    "max QUIC concurrent bidirectional streams must be greater than zero"
                )
            }
            Self::StreamWindowLimitZero => {
                write!(f, "max stream window bytes must be greater than zero")
            }
            Self::RepairLimitTooSmall => {
                write!(f, "max repair bytes must be at least max payload bytes")
            }
            Self::ReorderLimitTooSmall => {
                write!(f, "max reorder bytes must be at least max payload bytes")
            }
            Self::DatagramQueueLimitTooSmall => {
                write!(
                    f,
                    "max datagram queue bytes must be at least max payload bytes"
                )
            }
            Self::MaxReliableRelayChunkBytesZero => {
                write!(
                    f,
                    "max reliable relay chunk bytes must be greater than zero"
                )
            }
            Self::MaxReliableRelayChunkExceedsPayloadLimit => {
                write!(
                    f,
                    "max reliable relay chunk bytes must be no greater than max payload bytes"
                )
            }
            Self::PathFlightLimitTooSmall => {
                write!(f, "max path flight bytes must be at least one relay chunk")
            }
            Self::PathFlightLimitExceedsRepairLimit => {
                write!(
                    f,
                    "max path flight bytes must be no greater than max repair bytes"
                )
            }
            Self::TcpPathHeartbeatIntervalZero => {
                write!(f, "TCP path heartbeat interval must be greater than zero")
            }
            Self::TcpPathHeartbeatTimeoutZero => {
                write!(f, "TCP path heartbeat timeout must be greater than zero")
            }
            Self::TcpPathHeartbeatTimeoutTooSmall => {
                write!(
                    f,
                    "TCP path heartbeat timeout must be at least the heartbeat interval"
                )
            }
            Self::RestartBackoffZero => write!(f, "restart backoff must be greater than zero"),
            Self::RestartMaxBackoffZero => {
                write!(f, "maximum restart backoff must be greater than zero")
            }
            Self::RestartMaxBackoffTooSmall => {
                write!(
                    f,
                    "maximum restart backoff must be at least the initial restart backoff"
                )
            }
            Self::RestartLimitZero => write!(f, "max restarts must be greater than zero"),
            Self::NoIngresses => write!(f, "at least one client ingress is required"),
            Self::NoListenAddresses => {
                write!(f, "proxy ingress requires at least one listen address")
            }
            Self::TooManyPaths { actual, limit } => {
                write!(f, "{actual} paths configured, limit is {limit}")
            }
            Self::PathProbeIntervalZero => {
                write!(f, "path probe interval must be greater than zero")
            }
            Self::PathProbeTimeoutZero => {
                write!(f, "path probe timeout must be greater than zero")
            }
            Self::TunAddressRequired => write!(f, "TUN L4 ingress requires IPv4 or IPv6 address"),
            Self::TunIpv4PrefixInvalid => write!(f, "TUN IPv4 prefix must be in 0..=32"),
            Self::TunIpv6PrefixInvalid => write!(f, "TUN IPv6 prefix must be in 0..=128"),
            Self::TunMtuTooSmall => write!(f, "TUN MTU must be at least 576 bytes"),
            Self::TunIpv6MtuTooSmall => write!(f, "TUN IPv6 MTU must be at least 1280 bytes"),
            Self::TunDnsTtlZero => write!(f, "TUN DNS TTL must be greater than zero"),
            Self::TunDnsResolverPortZero => write!(f, "TUN DNS resolver port must be nonzero"),
            Self::OutboundDnsTimeoutZero => {
                write!(f, "outbound DNS timeout must be greater than zero")
            }
            Self::OutboundDnsResolverPortZero => {
                write!(f, "outbound DNS resolver port must be nonzero")
            }
            Self::OutboundConnectTimeoutZero => {
                write!(f, "outbound connect timeout must be greater than zero")
            }
            Self::ProxyAuthUsernameEmpty => write!(f, "proxy auth username must not be empty"),
            Self::ProxyAuthPasswordEmpty => write!(f, "proxy auth password must not be empty"),
            Self::ProxyAuthUsernameTooLong => {
                write!(f, "proxy auth username must fit in 255 bytes")
            }
            Self::ProxyAuthPasswordTooLong => {
                write!(f, "proxy auth password must fit in 255 bytes")
            }
            Self::ManagementListenPortZero => {
                write!(f, "management API listen port must be nonzero")
            }
            Self::ManagementTokenEmpty => {
                write!(f, "management API token must not be empty")
            }
            Self::NoRuntimeServices => {
                write!(
                    f,
                    "config must define at least one inbound or path listener"
                )
            }
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn udp_paths_reject_unknown_query_parameters() {
        let default_path = "udp://127.0.0.1:443"
            .parse::<PathSpec>()
            .expect("default udp path parses");

        assert_eq!(
            default_path.underlay,
            crate::protocol::UnderlayProtocol::Udp
        );
        assert!(
            "udp://127.0.0.1:443?unsupported=true"
                .parse::<PathSpec>()
                .is_err()
        );
        assert!(
            "udp://127.0.0.1:443?profile=experimental"
                .parse::<PathSpec>()
                .is_err()
        );
    }
}
