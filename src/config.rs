mod file;
mod model;
mod secret;
mod store;

pub use crate::performance::{
    DEFAULT_DATAGRAM_QUEUE_BYTES, DEFAULT_EXTRA_TRAFFIC_HINT_PERCENT,
    DEFAULT_MAX_QUIC_CONCURRENT_BIDI_STREAMS, DEFAULT_MAX_REINJECTION_CACHE_CHUNKS,
    DEFAULT_MAX_RELIABLE_RELAY_CHUNK_BYTES, DEFAULT_MAX_REORDER_BUFFER_CHUNKS,
    DEFAULT_MAX_RETAINED_RECEIVE_RANGES, DEFAULT_MAX_STREAMS, DEFAULT_PATH_FLIGHT_BYTES,
    DEFAULT_QUIC_PATH_IDLE_TIMEOUT, DEFAULT_QUIC_PATH_IDLE_TIMEOUT_MS,
    DEFAULT_QUIC_PATH_KEEP_ALIVE_INTERVAL, DEFAULT_QUIC_PATH_KEEP_ALIVE_INTERVAL_MS,
    DEFAULT_REORDER_BYTES, DEFAULT_REPAIR_BYTES, DEFAULT_STREAM_WINDOW_BYTES,
    DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL, DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL_MS,
    DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT, DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT_MS,
    MppPerformanceConfig, ResourceLimitError, ResourceLimits,
};
pub use crate::product::{
    CredentialAdmissionError, CredentialAuthority, CredentialCatalog, CredentialCatalogError,
    CredentialRecord, PrincipalPermit, ProductAdmissionConfig, ProductAdmissionConfigError,
    SecurityPolicyError, SharedSecret,
};
pub use file::{ConfigFileError, DEFAULT_CONFIG_PATH, load_config_toml, load_config_toml_str};
pub(crate) use file::{load_certificates, load_private_key};
pub use model::{
    AppConfig, ClientPathConfig, ClientSecurityConfig, CommandConfig, ConfigError,
    DEFAULT_AUTH_FRESHNESS_WINDOW, DEFAULT_AUTH_FRESHNESS_WINDOW_SECONDS,
    DEFAULT_AUTHENTICATION_TIMEOUT, DEFAULT_AUTHENTICATION_TIMEOUT_MS,
    DEFAULT_MAX_PENDING_AUTHENTICATIONS, DEFAULT_MPP_TLS_SERVER_NAME,
    DEFAULT_OUTBOUND_CONNECT_TIMEOUT, DEFAULT_OUTBOUND_CONNECT_TIMEOUT_MS,
    DEFAULT_PATH_PROBE_INTERVAL, DEFAULT_PATH_PROBE_INTERVAL_MS, DEFAULT_PATH_PROBE_TIMEOUT,
    DEFAULT_PATH_PROBE_TIMEOUT_MS, DEFAULT_RESTART_BACKOFF, DEFAULT_RESTART_BACKOFF_MS,
    DEFAULT_RESTART_MAX_BACKOFF, DEFAULT_RESTART_MAX_BACKOFF_MS, DEFAULT_SESSION_RETENTION_TIMEOUT,
    DEFAULT_SESSION_RETENTION_TIMEOUT_MS, DnsPolicyConfig, EgressRef, GatewayBalancerConfig,
    LocalIngressConfig, LogFormat, LogLevel, LoggingConfig, ManagementConfig, MppInboundConfig,
    MppOutboundConfig, NamedPathConfig, NodeConfig, OutboundLeafConfig, ProductPolicyConfig,
    ServerDestinationAclConfig, ServerSecurityConfig, ServiceConfig, SessionConfig,
};
pub(crate) use secret::{
    SecretMaterialError, normalize_secret_bytes, read_secret_environment, read_secret_file,
};
pub use store::{
    CanonicalConfigStore, CommittedConfig, ConfigRecoveryConflict, ConfigRevision,
    ConfigRevisionParseError, ConfigStoreError, ValidatedConfigCandidate,
};
pub(crate) use store::{canonical_config_owned_paths, paths_equivalent};
