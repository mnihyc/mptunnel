pub mod security;

use crate::ingress::IngressConfig;
use crate::outbound::OutboundConfig;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{TargetAddr, TrafficClass};
use crate::transport::PathSpec;
use security::validate_transport_security;
pub use security::{
    EncryptionMode, SecurityPolicyError, SharedSecret, TransportIntegrity, TransportSecurity,
};
use std::collections::BTreeSet;
use std::time::Duration;

pub const DEFAULT_PATH_PROBE_INTERVAL_MS: u64 = 10_000;
pub const DEFAULT_PATH_PROBE_TIMEOUT_MS: u64 = 2_000;
pub const DEFAULT_PATH_PROBE_INTERVAL: Duration =
    Duration::from_millis(DEFAULT_PATH_PROBE_INTERVAL_MS);
pub const DEFAULT_PATH_PROBE_TIMEOUT: Duration =
    Duration::from_millis(DEFAULT_PATH_PROBE_TIMEOUT_MS);
pub const DEFAULT_TCP_PATH_INFLIGHT_BYTES: usize = 4 * 1024 * 1024;
const RELAY_CHUNK_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppConfig {
    pub log_level: String,
    pub check_config: bool,
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
        self.resources.validate()?;
        match &self.command {
            CommandConfig::Client(client) => {
                if client.paths.is_empty() {
                    return Err(ConfigError::NoPaths);
                }
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
                client.traffic_policy.validate()?;
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
            }
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
        if self.max_tcp_path_inflight_bytes < self.max_payload_bytes.min(RELAY_CHUNK_BYTES) {
            return Err(ConfigError::TcpPathInflightLimitTooSmall);
        }
        if self.max_tcp_path_inflight_bytes > self.max_repair_bytes {
            return Err(ConfigError::TcpPathInflightLimitExceedsRepairLimit);
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
    pub traffic_policy: TrafficPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrafficPolicy {
    pub default_tcp_class: TrafficClass,
    pub tcp_port_rules: Vec<TcpPortClassRule>,
}

impl Default for TrafficPolicy {
    fn default() -> Self {
        Self {
            default_tcp_class: TrafficClass::Interactive,
            tcp_port_rules: Vec::new(),
        }
    }
}

impl TrafficPolicy {
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_tcp_class(self.default_tcp_class)?;
        let mut ports = BTreeSet::new();
        for rule in &self.tcp_port_rules {
            if rule.port == 0 {
                return Err(ConfigError::TcpClassRulePortZero);
            }
            validate_tcp_class(rule.class)?;
            if !ports.insert(rule.port) {
                return Err(ConfigError::DuplicateTcpClassRule { port: rule.port });
            }
        }
        Ok(())
    }

    pub fn classify_tcp_target(&self, target: &TargetAddr) -> TrafficClass {
        let port = target.port();
        self.tcp_port_rules
            .iter()
            .find_map(|rule| (rule.port == port).then_some(rule.class))
            .unwrap_or(self.default_tcp_class)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpPortClassRule {
    pub port: u16,
    pub class: TrafficClass,
}

fn validate_tcp_class(class: TrafficClass) -> Result<(), ConfigError> {
    if class == TrafficClass::RealtimeDatagram {
        Err(ConfigError::TcpPolicyUsesDatagramClass)
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    pub bind_paths: Vec<PathSpec>,
    pub outbound: OutboundConfig,
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
    TcpPathInflightLimitTooSmall,
    TcpPathInflightLimitExceedsRepairLimit,
    TooManyPaths { actual: usize, limit: usize },
    PathProbeIntervalZero,
    PathProbeTimeoutZero,
    TcpPolicyUsesDatagramClass,
    TcpClassRulePortZero,
    DuplicateTcpClassRule { port: u16 },
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
            Self::TooManyPaths { actual, limit } => {
                write!(f, "{actual} paths configured, limit is {limit}")
            }
            Self::PathProbeIntervalZero => {
                write!(f, "path probe interval must be greater than zero")
            }
            Self::PathProbeTimeoutZero => {
                write!(f, "path probe timeout must be greater than zero")
            }
            Self::TcpPolicyUsesDatagramClass => {
                write!(f, "TCP traffic policy cannot use realtime datagram class")
            }
            Self::TcpClassRulePortZero => {
                write!(f, "TCP traffic class rule port must be in 1..=65535")
            }
            Self::DuplicateTcpClassRule { port } => {
                write!(f, "duplicate TCP traffic class rule for port {port}")
            }
        }
    }
}

impl std::error::Error for ConfigError {}
