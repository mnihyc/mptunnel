pub mod security;

use crate::ingress::IngressConfig;
use crate::outbound::{DnsConfig, OutboundConfig};
use crate::protocol::codec::CodecLimits;
use crate::transport::PathSpec;
use security::validate_transport_security;
pub use security::{
    EncryptionMode, SecurityPolicyError, SharedSecret, TransportIntegrity, TransportSecurity,
};
use std::time::Duration;

pub const DEFAULT_PATH_PROBE_INTERVAL_MS: u64 = 10_000;
pub const DEFAULT_PATH_PROBE_TIMEOUT_MS: u64 = 2_000;
pub const DEFAULT_PATH_PROBE_INTERVAL: Duration =
    Duration::from_millis(DEFAULT_PATH_PROBE_INTERVAL_MS);
pub const DEFAULT_PATH_PROBE_TIMEOUT: Duration =
    Duration::from_millis(DEFAULT_PATH_PROBE_TIMEOUT_MS);
pub const DEFAULT_TCP_PATH_INFLIGHT_BYTES: usize = 4 * 1024 * 1024;
pub const DEFAULT_MAX_TCP_RELAY_CHUNK_BYTES: usize = 256 * 1024;
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppConfig {
    pub log_level: String,
    pub check_config: bool,
    pub service: ServiceConfig,
    pub resources: ResourceLimits,
    pub security: SecurityConfig,
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
        self.service.validate()?;
        self.resources.validate()?;
        match &self.command {
            CommandConfig::Client(client) => {
                if client.paths.is_empty() {
                    return Err(ConfigError::NoPaths);
                }
                validate_ingress(&client.ingress)?;
                if client.paths.len() > self.resources.max_paths {
                    return Err(ConfigError::TooManyPaths {
                        actual: client.paths.len(),
                        limit: self.resources.max_paths,
                    });
                }
                if client.path_probe_interval.is_zero() {
                    return Err(ConfigError::PathProbeIntervalZero);
                }
                if client.path_probe_timeout.is_zero() {
                    return Err(ConfigError::PathProbeTimeoutZero);
                }
                if let IngressConfig::TunL4(tun) = &client.ingress {
                    validate_tun_l4(tun)?;
                }
            }
            CommandConfig::Server(server) => {
                if server.bind_paths.is_empty() {
                    return Err(ConfigError::NoPaths);
                }
                if server.bind_paths.len() > self.resources.max_paths {
                    return Err(ConfigError::TooManyPaths {
                        actual: server.bind_paths.len(),
                        limit: self.resources.max_paths,
                    });
                }
                server.outbound_dns.validate()?;
            }
        }
        Ok(())
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
    pub max_stream_window_bytes: u64,
    pub max_repair_bytes: usize,
    pub max_reorder_bytes: usize,
    pub max_datagram_queue_bytes: usize,
    pub max_tcp_path_inflight_bytes: usize,
    pub max_tcp_relay_chunk_bytes: usize,
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
            max_streams: 65_536,
            max_stream_window_bytes: 16 * 1024 * 1024,
            max_repair_bytes: 16 * 1024 * 1024,
            max_reorder_bytes: 16 * 1024 * 1024,
            max_datagram_queue_bytes: 4 * 1024 * 1024,
            max_tcp_path_inflight_bytes: DEFAULT_TCP_PATH_INFLIGHT_BYTES,
            max_tcp_relay_chunk_bytes: DEFAULT_MAX_TCP_RELAY_CHUNK_BYTES,
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
        if self.max_tcp_relay_chunk_bytes == 0 {
            return Err(ConfigError::MaxTcpRelayChunkBytesZero);
        }
        if self.max_tcp_relay_chunk_bytes > self.max_payload_bytes {
            return Err(ConfigError::MaxTcpRelayChunkExceedsPayloadLimit);
        }
        if self.max_tcp_path_inflight_bytes < self.max_tcp_relay_chunk_bytes {
            return Err(ConfigError::TcpPathInflightLimitTooSmall);
        }
        if self.max_tcp_path_inflight_bytes > self.max_repair_bytes {
            return Err(ConfigError::TcpPathInflightLimitExceedsRepairLimit);
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
    pub secret: SharedSecret,
}

impl SecurityConfig {
    pub fn encrypted(secret: SharedSecret) -> Self {
        Self {
            mode: EncryptionMode::Required,
            transport: TransportSecurity::Encrypted,
            integrity: TransportIntegrity::Authenticated,
            secret,
        }
    }

    pub fn plaintext_lab(secret: SharedSecret) -> Self {
        Self {
            mode: EncryptionMode::AllowPlaintextLab,
            transport: TransportSecurity::Plaintext,
            integrity: TransportIntegrity::Authenticated,
            secret,
        }
    }

    pub fn warning(&self) -> Option<&'static str> {
        self.mode.plaintext_warning()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandConfig {
    Client(ClientConfig),
    Server(ServerConfig),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    pub ingress: IngressConfig,
    pub paths: Vec<PathSpec>,
    pub path_probe_interval: Duration,
    pub path_probe_timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    pub bind_paths: Vec<PathSpec>,
    pub outbound: OutboundConfig,
    pub outbound_dns: DnsConfig,
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
        IngressConfig::Socks5 { listen } | IngressConfig::HttpConnect { listen } => {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    Security(SecurityPolicyError),
    NoPaths,
    FrameLimitTooSmall,
    PayloadLimitExceedsFrameLimit,
    AckRangeLimitZero,
    PathLimitZero,
    PathLimitTooLarge,
    StreamLimitZero,
    StreamWindowLimitZero,
    RepairLimitTooSmall,
    ReorderLimitTooSmall,
    DatagramQueueLimitTooSmall,
    MaxTcpRelayChunkBytesZero,
    MaxTcpRelayChunkExceedsPayloadLimit,
    TcpPathInflightLimitTooSmall,
    TcpPathInflightLimitExceedsRepairLimit,
    TcpPathHeartbeatIntervalZero,
    TcpPathHeartbeatTimeoutZero,
    TcpPathHeartbeatTimeoutTooSmall,
    RestartBackoffZero,
    RestartMaxBackoffZero,
    RestartMaxBackoffTooSmall,
    RestartLimitZero,
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
            Self::NoPaths => write!(f, "at least one TCP or UDP path is required"),
            Self::FrameLimitTooSmall => write!(f, "max frame bytes must be at least 64"),
            Self::PayloadLimitExceedsFrameLimit => {
                write!(f, "max payload bytes must fit inside max frame bytes")
            }
            Self::AckRangeLimitZero => write!(f, "max ack ranges must be greater than zero"),
            Self::PathLimitZero => write!(f, "max paths must be greater than zero"),
            Self::PathLimitTooLarge => write!(f, "max paths must fit in protocol path IDs"),
            Self::StreamLimitZero => write!(f, "max streams must be greater than zero"),
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
            Self::MaxTcpRelayChunkBytesZero => {
                write!(f, "max TCP relay chunk bytes must be greater than zero")
            }
            Self::MaxTcpRelayChunkExceedsPayloadLimit => {
                write!(
                    f,
                    "max TCP relay chunk bytes must be no greater than max payload bytes"
                )
            }
            Self::TcpPathInflightLimitTooSmall => {
                write!(
                    f,
                    "max TCP path inflight bytes must be at least one relay chunk"
                )
            }
            Self::TcpPathInflightLimitExceedsRepairLimit => {
                write!(
                    f,
                    "max TCP path inflight bytes must be no greater than max repair bytes"
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
        }
    }
}

impl std::error::Error for ConfigError {}
