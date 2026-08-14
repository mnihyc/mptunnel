use super::secret::{MaterialSource, MaterialSourceError};
use super::{
    AppConfig, ClientPathConfig, ClientSecurityConfig, CommandConfig, ConfigError,
    DEFAULT_AUTH_FRESHNESS_WINDOW_SECONDS, DEFAULT_AUTHENTICATION_TIMEOUT_MS,
    DEFAULT_MAX_PENDING_AUTHENTICATIONS, DEFAULT_MPP_TLS_SERVER_NAME,
    DEFAULT_OUTBOUND_CONNECT_TIMEOUT_MS, DEFAULT_PATH_PROBE_INTERVAL_MS,
    DEFAULT_PATH_PROBE_TIMEOUT_MS, DEFAULT_RESTART_BACKOFF_MS, DEFAULT_RESTART_MAX_BACKOFF_MS,
    DEFAULT_SESSION_RETENTION_TIMEOUT_MS, DnsPolicyConfig, ForwardingMode, GatewayBalancerConfig,
    LocalIngressConfig, LogFormat, LogLevel, LoggingConfig, ManagementConfig, MppInboundConfig,
    MppOutboundConfig, MppPerformanceConfig, NamedPathConfig, NamedTunL3Config, NodeConfig,
    OutboundLeafConfig, ProductAdmissionConfig, ProductPolicyConfig, ResourceLimits,
    SecurityPolicyError, ServerSecurityConfig, ServiceConfig, SessionConfig, SharedSecret,
};
use crate::ingress::tun::{
    DEFAULT_TUN_DNS_TTL_MS, DEFAULT_TUN_IPV4, DEFAULT_TUN_IPV4_PREFIX, DEFAULT_TUN_MTU,
    ManagedVpnConfig, ManagedVpnPlatformConfig, TunHostConfig, TunL4Config,
};
use crate::ingress::{
    DEFAULT_TCP_FORWARD_MAX_CONNECTIONS, DEFAULT_UDP_FORWARD_DATAGRAM_TTL_MS,
    DEFAULT_UDP_FORWARD_IDLE_TIMEOUT_MS, DEFAULT_UDP_FORWARD_MAX_ASSOCIATIONS, IngressConfig,
    LocalIngressAdmissionConfig, LocalIngressAdmissionConfigError, LocalProxyUser,
    MixedForwardConfig, PortForwardTarget, ProxyAuthConfig, ProxyAuthConfigError, TcpForwardConfig,
    TunL3IngressConfig, UdpForwardConfig,
};
use crate::outbound::{
    HttpsProxyConfig, OutboundConfig, OutboundError, ProxyConfig, ProxyCredentials,
};
use crate::platform::{
    DEFAULT_LINUX_CAPTURE_RULE_PRIORITY, DEFAULT_LINUX_NATIVE_RULE_PRIORITY,
    DEFAULT_LINUX_ROUTE_TABLE, DEFAULT_LINUX_SOCKET_MARK, LinuxPolicyConfig, LinuxSocketMark,
    RouteMode,
};
use crate::product::{
    BalancerId, CompiledRuleSetRegistry, CredentialCatalog, CredentialId, CredentialRecord,
    DnsEgressSpec, DnsIpStrategy, DnsOutboundCapabilitySpec, DnsOverrideRecordId,
    DnsOverrideRecordSpec, DnsPlanLimits, DnsPlanSpec, DnsPolicySpec, DnsRuleId, DnsRuleMatch,
    DnsRuleSpec, DnsSecurityPolicy, DnsSyntheticCaptureId, DnsSyntheticCaptureSpec,
    DnsUpstreamEndpoint, DnsUpstreamId, DnsUpstreamSpec, DnsUpstreamStrategy, DomainName,
    EgressAction, GatewayBalancer, GatewayBalancerSpec, GatewayHealthPolicy, GatewayMemberMode,
    GatewayMemberSpec, GatewayProbePolicy, GatewayStickinessKey, GatewayStickinessPolicy,
    GatewayStrategy, InboundId, InitialDemand, MAX_RULE_SET_ENVELOPE_BYTES, Network, OutboundId,
    PortRange, PrincipalId, ProtocolTarget, RouteAction, RouteMatchSpec, RouteRuleSpec, RouteStage,
    RuleId, RuleSetId, RuleSetPublisher, RuleSetPublisherCatalog, RuleSetPublisherId,
    TunL3AddressPlan, TunL3AllocationSpec, TunL3ServerSpec, VerifiedRuleSet,
};
use crate::transport::encrypted::{SharedTransportSecret, TcpClientTlsConfig, TcpServerTlsConfig};
use crate::transport::{EndpointParseError, PathSpecParseError};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const DEFAULT_CONFIG_PATH: &str = "config.toml";

// Runtime generations are internal identities rather than operator policy.
// One non-zero identity is shared by every component compiled from a document.
static NEXT_CONFIG_GENERATION: AtomicU64 = AtomicU64::new(1);

fn next_config_generation() -> Result<u64, ConfigFileError> {
    NEXT_CONFIG_GENERATION
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |generation| {
            generation.checked_add(1)
        })
        .map_err(|_| ConfigFileError::GenerationExhausted)
}

pub fn load_config_toml(path: impl AsRef<Path>) -> Result<AppConfig, ConfigFileError> {
    let path = path.as_ref();
    let contents = std::fs::read_to_string(path).map_err(ConfigFileError::Io)?;
    load_config_toml_str_at(&contents, path.parent().unwrap_or_else(|| Path::new(".")))
}

pub fn load_config_toml_str(contents: &str) -> Result<AppConfig, ConfigFileError> {
    load_config_toml_str_at(contents, Path::new("."))
}

pub(super) fn load_config_toml_str_at(
    contents: &str,
    material_base: &Path,
) -> Result<AppConfig, ConfigFileError> {
    let file = toml::from_str::<FileConfig>(contents)
        .map_err(|error| ConfigFileError::Toml(TomlConfigError::new(contents, &error)))?;
    file.into_config(material_base)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TomlConfigError {
    line: Option<usize>,
    column: Option<usize>,
    unknown_field: Option<String>,
}

impl TomlConfigError {
    fn new(contents: &str, error: &toml::de::Error) -> Self {
        let (line, column) = error
            .span()
            .map(|span| toml_line_column(contents, span.start))
            .map_or((None, None), |(line, column)| (Some(line), Some(column)));
        Self {
            line,
            column,
            unknown_field: toml_unknown_field(error.message()),
        }
    }
}

impl std::fmt::Display for TomlConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.unknown_field, self.line, self.column) {
            (Some(field), Some(line), Some(column)) => write!(
                formatter,
                "configuration document contains unknown field {field:?} at line {line}, column {column}"
            ),
            (Some(field), _, _) => {
                write!(
                    formatter,
                    "configuration document contains unknown field {field:?}"
                )
            }
            (None, Some(line), Some(column)) => write!(
                formatter,
                "configuration document is invalid at line {line}, column {column}"
            ),
            (None, _, _) => formatter.write_str("configuration document is invalid"),
        }
    }
}

impl std::error::Error for TomlConfigError {}

fn toml_line_column(contents: &str, offset: usize) -> (usize, usize) {
    let offset = offset.min(contents.len());
    let prefix = contents.get(..offset).unwrap_or(contents);
    let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
    let column = prefix
        .rsplit('\n')
        .next()
        .map_or(1, |tail| tail.chars().count() + 1);
    (line, column)
}

fn toml_unknown_field(message: &str) -> Option<String> {
    let field = message.strip_prefix("unknown field `")?.split_once('`')?.0;
    if field.is_empty() || field.len() > 128 || field.chars().any(char::is_control) {
        return None;
    }
    Some(field.to_string())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    #[serde(default)]
    logging: LoggingFileConfig,
    #[serde(default)]
    check_config: bool,
    #[serde(default)]
    service: ServiceFileConfig,
    #[serde(default)]
    session: SessionFileConfig,
    #[serde(default)]
    resources: ResourceFileConfig,
    #[serde(default)]
    admission: ProductAdmissionFileConfig,
    #[serde(default)]
    management: ManagementFileConfig,
    #[serde(default)]
    credentials: Vec<CredentialFileConfig>,
    #[serde(default)]
    local_users: Vec<LocalUserFileConfig>,
    #[serde(default)]
    dns: DnsFileConfig,
    #[serde(default)]
    inbounds: Vec<InboundFileConfig>,
    #[serde(default)]
    outbounds: Vec<OutboundFileConfig>,
    routing: Option<RoutingFileConfig>,
}

impl FileConfig {
    fn into_config(self, material_base: &Path) -> Result<AppConfig, ConfigFileError> {
        if self.inbounds.is_empty() {
            return Err(ConfigFileError::NoRuntimeServices);
        }
        let forwarding_mode = infer_forwarding_mode(&self.inbounds)?;
        let generation = next_config_generation()?;
        self.admission.validate_for_mode(forwarding_mode)?;
        let credential_catalog = parse_credential_catalog(self.credentials, material_base)?;
        let local_user_catalog = LocalUserCatalog::compile(self.local_users, material_base)?;
        let mut parsed_outbounds =
            parse_outbounds(self.outbounds, material_base, &credential_catalog)?;
        let dns_policy = self.dns.into_config(generation, &parsed_outbounds)?;
        let product_policy = match (forwarding_mode, self.routing) {
            (ForwardingMode::L4, Some(routing)) => {
                let RoutingFileConfig {
                    balancers,
                    rule_set_publishers,
                    rule_sets,
                    rules,
                } = routing;
                let rule_sets =
                    compile_rule_set_registry(rule_set_publishers, rule_sets, material_base)?;
                apply_routing(generation, balancers, &mut parsed_outbounds)?;
                let local_inbound_names = configured_routed_inbound_names(&self.inbounds)?;
                compile_product_policy(
                    generation,
                    rules,
                    &rule_sets,
                    &local_inbound_names,
                    &parsed_outbounds,
                )?
            }
            (ForwardingMode::L4, None) => {
                return Err(ConfigFileError::L4RoutingSectionRequired);
            }
            (ForwardingMode::L3, Some(_)) => {
                return Err(ConfigFileError::L3RoutingSectionForbidden);
            }
            (ForwardingMode::L3, None) => None,
        };
        let (outbounds, gateway_balancers, local_ingresses, tun_l3_ingresses, servers) =
            build_node_services(
                self.inbounds,
                parsed_outbounds,
                material_base,
                &credential_catalog,
                &local_user_catalog,
            )?;
        let config = AppConfig {
            logging: self.logging.into_config(material_base)?,
            check_config: self.check_config,
            service: self.service.into_config(),
            session: self.session.into_config(),
            resources: self.resources.into_limits(),
            admission: self.admission.into_config(forwarding_mode),
            management: self.management.into_config(material_base)?,
            command: CommandConfig::Node(NodeConfig {
                forwarding_mode,
                outbounds,
                gateway_balancers,
                local_ingresses,
                tun_l3_ingresses,
                product_policy,
                dns_policy,
                servers,
            }),
        };
        config.validate()?;
        Ok(config)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalUserFileConfig {
    name: String,
    principal_id: String,
    username: String,
    password: MaterialSource,
}

impl std::fmt::Debug for LocalUserFileConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalUserFileConfig")
            .field("name", &self.name)
            .field("principal_id", &self.principal_id)
            .field("username", &"<redacted>")
            .field("password", &self.password)
            .finish()
    }
}

struct LocalUserCatalog {
    users: HashMap<String, LocalProxyUser>,
}

impl LocalUserCatalog {
    fn compile(
        values: Vec<LocalUserFileConfig>,
        material_base: &Path,
    ) -> Result<Self, ConfigFileError> {
        let mut users = HashMap::with_capacity(values.len());
        for value in values {
            let name = canonical_config_name(&value.name)?;
            let principal = PrincipalId::parse(&value.principal_id)
                .map_err(|error| ConfigFileError::LocalUser(error.to_string()))?;
            let password = value
                .password
                .resolve(material_base, "local proxy user password")
                .map_err(ConfigFileError::MaterialSource)?
                .into_utf8("local proxy user password")
                .map_err(ConfigFileError::MaterialSource)?;
            let user = LocalProxyUser::new(name.clone(), principal, value.username, password)
                .map_err(ConfigFileError::ProxyAuth)?;
            if users.insert(name.clone(), user.clone()).is_some() {
                return Err(ConfigFileError::LocalUser(format!(
                    "duplicate local user name {name:?}"
                )));
            }
        }
        Ok(Self { users })
    }

    fn auth_for(&self, ids: Vec<String>) -> Result<ProxyAuthConfig, ConfigFileError> {
        if ids.is_empty() {
            return Ok(ProxyAuthConfig::disabled());
        }
        let mut selected = Vec::with_capacity(ids.len());
        for name in ids {
            let name = canonical_config_name(&name)?;
            let user = self.users.get(&name).cloned().ok_or_else(|| {
                ConfigFileError::LocalUser(format!(
                    "local inbound references missing local user {name:?}"
                ))
            })?;
            selected.push(user);
        }
        ProxyAuthConfig::required(selected).map_err(ConfigFileError::ProxyAuth)
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionFileConfig {
    retention_timeout_ms: Option<u64>,
}

impl SessionFileConfig {
    fn into_config(self) -> SessionConfig {
        SessionConfig {
            retention_timeout: Duration::from_millis(
                self.retention_timeout_ms
                    .unwrap_or(DEFAULT_SESSION_RETENTION_TIMEOUT_MS),
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum LogLevelFileValue {
    Off,
    Error,
    Warn,
    #[default]
    Info,
}

impl From<LogLevelFileValue> for LogLevel {
    fn from(value: LogLevelFileValue) -> Self {
        match value {
            LogLevelFileValue::Off => Self::Off,
            LogLevelFileValue::Error => Self::Error,
            LogLevelFileValue::Warn => Self::Warn,
            LogLevelFileValue::Info => Self::Info,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum LogFormatFileValue {
    #[default]
    Text,
    Json,
}

impl From<LogFormatFileValue> for LogFormat {
    fn from(value: LogFormatFileValue) -> Self {
        match value {
            LogFormatFileValue::Text => Self::Text,
            LogFormatFileValue::Json => Self::Json,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct LoggingFileConfig {
    level: LogLevelFileValue,
    format: LogFormatFileValue,
    console: bool,
    file: Option<PathBuf>,
    flow_events: bool,
}

impl Default for LoggingFileConfig {
    fn default() -> Self {
        Self {
            level: LogLevelFileValue::Info,
            format: LogFormatFileValue::Text,
            console: true,
            file: None,
            flow_events: false,
        }
    }
}

impl LoggingFileConfig {
    fn into_config(self, material_base: &Path) -> Result<LoggingConfig, ConfigFileError> {
        if self
            .file
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        {
            return Err(ConfigError::LoggingFilePathEmpty.into());
        }
        Ok(LoggingConfig {
            level: self.level.into(),
            format: self.format.into(),
            console: self.console,
            file: self
                .file
                .map(|configured| material_path(material_base, &configured)),
            flow_events: self.flow_events,
        })
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServiceFileConfig {
    #[serde(default)]
    supervise: bool,
    restart_backoff_ms: Option<u64>,
    restart_max_backoff_ms: Option<u64>,
    max_restarts: Option<u32>,
}

impl ServiceFileConfig {
    fn into_config(self) -> ServiceConfig {
        ServiceConfig {
            supervise: self.supervise,
            restart_backoff: Duration::from_millis(
                self.restart_backoff_ms
                    .unwrap_or(DEFAULT_RESTART_BACKOFF_MS),
            ),
            restart_max_backoff: Duration::from_millis(
                self.restart_max_backoff_ms
                    .unwrap_or(DEFAULT_RESTART_MAX_BACKOFF_MS),
            ),
            max_restarts: self.max_restarts,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagementFileConfig {
    #[serde(default)]
    listen: Vec<SocketAddr>,
    token: Option<MaterialSource>,
    #[serde(default)]
    dashboard: bool,
    #[serde(default)]
    allow_peer_diagnostics: bool,
}

impl ManagementFileConfig {
    fn into_config(self, material_base: &Path) -> Result<ManagementConfig, ConfigFileError> {
        Ok(ManagementConfig {
            listen: self.listen,
            token: self
                .token
                .map(|reference| {
                    reference
                        .resolve(material_base, "management token")
                        .and_then(|material| material.into_utf8("management token"))
                        .map_err(ConfigFileError::MaterialSource)
                })
                .transpose()?,
            dashboard: self.dashboard,
            allow_peer_diagnostics: self.allow_peer_diagnostics,
        })
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceFileConfig {
    max_frame_bytes: Option<usize>,
    max_payload_bytes: Option<usize>,
    max_ack_ranges: Option<usize>,
    max_paths: Option<usize>,
    max_streams: Option<usize>,
    max_quic_concurrent_bidi_streams: Option<usize>,
    max_stream_window_bytes: Option<u64>,
    max_repair_bytes: Option<usize>,
    max_reorder_bytes: Option<usize>,
    max_reinjection_cache_chunks: Option<usize>,
    max_reorder_buffer_chunks: Option<usize>,
    max_retained_receive_ranges: Option<usize>,
    max_datagram_queue_bytes: Option<usize>,
    max_path_flight_bytes: Option<usize>,
    max_reliable_relay_chunk_bytes: Option<usize>,
    tcp_path_heartbeat_interval_ms: Option<u64>,
    tcp_path_heartbeat_timeout_ms: Option<u64>,
    quic_path_keep_alive_interval_ms: Option<u64>,
    quic_path_idle_timeout_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProductAdmissionFileConfig {
    max_live_flows: Option<usize>,
    max_concurrent_work: Option<usize>,
    max_live_flows_per_principal: Option<usize>,
    max_live_flows_per_outbound: Option<usize>,
    max_connects_per_outbound: Option<usize>,
    max_live_flows_per_target: Option<usize>,
    max_connects_per_target: Option<usize>,
    max_dns_work: Option<usize>,
}

impl ProductAdmissionFileConfig {
    fn validate_for_mode(&self, forwarding_mode: ForwardingMode) -> Result<(), ConfigFileError> {
        if forwarding_mode != ForwardingMode::L3 {
            return Ok(());
        }
        let l4_field = [
            ("max_live_flows", self.max_live_flows.is_some()),
            ("max_concurrent_work", self.max_concurrent_work.is_some()),
            (
                "max_live_flows_per_principal",
                self.max_live_flows_per_principal.is_some(),
            ),
            (
                "max_live_flows_per_outbound",
                self.max_live_flows_per_outbound.is_some(),
            ),
            (
                "max_connects_per_outbound",
                self.max_connects_per_outbound.is_some(),
            ),
            (
                "max_live_flows_per_target",
                self.max_live_flows_per_target.is_some(),
            ),
            (
                "max_connects_per_target",
                self.max_connects_per_target.is_some(),
            ),
        ]
        .into_iter()
        .find_map(|(field, configured)| configured.then_some(field));
        if let Some(field) = l4_field {
            return Err(ConfigFileError::L3AdmissionField(field));
        }
        Ok(())
    }

    fn into_config(self, forwarding_mode: ForwardingMode) -> ProductAdmissionConfig {
        let defaults = ProductAdmissionConfig::default();
        if forwarding_mode == ForwardingMode::L3 {
            let max_dns_work = self.max_dns_work.unwrap_or(defaults.max_dns_work);
            // ProductAdmission retains one shared accounting implementation.
            // Keep its unused L4 dimensions minimally valid and make the
            // shared concurrent-work ceiling identical to the only L3 knob,
            // so no hidden L4 default can further restrict DNS work.
            return ProductAdmissionConfig {
                max_live_flows: 1,
                max_concurrent_work: max_dns_work,
                max_live_flows_per_principal: 1,
                max_live_flows_per_outbound: 1,
                max_connects_per_outbound: 1,
                max_live_flows_per_target: 1,
                max_connects_per_target: 1,
                max_dns_work,
            };
        }
        ProductAdmissionConfig {
            max_live_flows: self.max_live_flows.unwrap_or(defaults.max_live_flows),
            max_concurrent_work: self
                .max_concurrent_work
                .unwrap_or(defaults.max_concurrent_work),
            max_live_flows_per_principal: self
                .max_live_flows_per_principal
                .unwrap_or(defaults.max_live_flows_per_principal),
            max_live_flows_per_outbound: self
                .max_live_flows_per_outbound
                .unwrap_or(defaults.max_live_flows_per_outbound),
            max_connects_per_outbound: self
                .max_connects_per_outbound
                .unwrap_or(defaults.max_connects_per_outbound),
            max_live_flows_per_target: self
                .max_live_flows_per_target
                .unwrap_or(defaults.max_live_flows_per_target),
            max_connects_per_target: self
                .max_connects_per_target
                .unwrap_or(defaults.max_connects_per_target),
            max_dns_work: self.max_dns_work.unwrap_or(defaults.max_dns_work),
        }
    }
}

impl ResourceFileConfig {
    fn into_limits(self) -> ResourceLimits {
        let defaults = ResourceLimits::default();
        let max_frame_bytes = self.max_frame_bytes.unwrap_or(defaults.max_frame_bytes);
        let max_payload_bytes = self.max_payload_bytes.unwrap_or_else(|| {
            defaults
                .max_payload_bytes
                .min(max_frame_bytes.saturating_sub(16))
                .max(1)
        });
        let max_repair_bytes = self
            .max_repair_bytes
            .unwrap_or(defaults.max_repair_bytes.max(max_payload_bytes));
        let max_reorder_bytes = self
            .max_reorder_bytes
            .unwrap_or(defaults.max_reorder_bytes.max(max_payload_bytes));
        let max_datagram_queue_bytes = self
            .max_datagram_queue_bytes
            .unwrap_or(defaults.max_datagram_queue_bytes.max(max_payload_bytes));
        let max_reliable_relay_chunk_bytes =
            self.max_reliable_relay_chunk_bytes.unwrap_or_else(|| {
                defaults
                    .max_reliable_relay_chunk_bytes
                    .min(max_payload_bytes)
                    .max(1)
            });
        let max_path_flight_bytes = self.max_path_flight_bytes.unwrap_or(max_repair_bytes);
        ResourceLimits {
            max_frame_bytes,
            max_payload_bytes,
            max_ack_ranges: self.max_ack_ranges.unwrap_or(defaults.max_ack_ranges),
            max_paths: self.max_paths.unwrap_or(defaults.max_paths),
            max_streams: self.max_streams.unwrap_or(defaults.max_streams),
            max_quic_concurrent_bidi_streams: self
                .max_quic_concurrent_bidi_streams
                .unwrap_or(defaults.max_quic_concurrent_bidi_streams),
            max_stream_window_bytes: self
                .max_stream_window_bytes
                .unwrap_or(defaults.max_stream_window_bytes),
            max_repair_bytes,
            max_reorder_bytes,
            max_reinjection_cache_chunks: self
                .max_reinjection_cache_chunks
                .unwrap_or(defaults.max_reinjection_cache_chunks),
            max_reorder_buffer_chunks: self
                .max_reorder_buffer_chunks
                .unwrap_or(defaults.max_reorder_buffer_chunks),
            max_retained_receive_ranges: self
                .max_retained_receive_ranges
                .unwrap_or(defaults.max_retained_receive_ranges),
            max_datagram_queue_bytes,
            max_path_flight_bytes,
            max_reliable_relay_chunk_bytes,
            tcp_path_heartbeat_interval: Duration::from_millis(
                self.tcp_path_heartbeat_interval_ms
                    .unwrap_or(defaults.tcp_path_heartbeat_interval.as_millis() as u64),
            ),
            tcp_path_heartbeat_timeout: Duration::from_millis(
                self.tcp_path_heartbeat_timeout_ms
                    .unwrap_or(defaults.tcp_path_heartbeat_timeout.as_millis() as u64),
            ),
            quic_path_keep_alive_interval: Duration::from_millis(
                self.quic_path_keep_alive_interval_ms
                    .unwrap_or(defaults.quic_path_keep_alive_interval.as_millis() as u64),
            ),
            quic_path_idle_timeout: Duration::from_millis(
                self.quic_path_idle_timeout_ms
                    .unwrap_or(defaults.quic_path_idle_timeout.as_millis() as u64),
            ),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialFileConfig {
    credential_id: String,
    principal_id: String,
    secret: MaterialSource,
    expires_at_unix_secs: Option<u64>,
    #[serde(default)]
    revoked: bool,
    #[serde(default)]
    revocation_grace_seconds: u64,
}

impl std::fmt::Debug for CredentialFileConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CredentialFileConfig")
            .field("credential_id", &self.credential_id)
            .field("principal_id", &self.principal_id)
            .field("secret", &self.secret)
            .field("expires_at_unix_secs", &self.expires_at_unix_secs)
            .field("revoked", &self.revoked)
            .field("revocation_grace_seconds", &self.revocation_grace_seconds)
            .finish()
    }
}

fn parse_credential_catalog(
    values: Vec<CredentialFileConfig>,
    material_base: &Path,
) -> Result<CredentialCatalog, ConfigFileError> {
    let records = values
        .into_iter()
        .map(|value| {
            let id = CredentialId::parse(&value.credential_id)
                .map_err(|error| ConfigFileError::Credential(error.to_string()))?;
            let principal = PrincipalId::parse(&value.principal_id)
                .map_err(|error| ConfigFileError::Credential(error.to_string()))?;
            let secret = value
                .secret
                .resolve(material_base, "credential")
                .map_err(ConfigFileError::MaterialSource)?
                .into_bytes();
            CredentialRecord::new(
                id,
                principal,
                SharedSecret::new(secret)?,
                value.expires_at_unix_secs,
                value.revoked,
                value.revocation_grace_seconds,
            )
            .map_err(|error| ConfigFileError::Credential(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    CredentialCatalog::compile(records)
        .map_err(|error| ConfigFileError::Credential(error.to_string()))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SecurityFileConfig {
    credential_id: Option<String>,
    #[serde(default)]
    credential_ids: Vec<String>,
    auth_freshness_window_seconds: Option<u64>,
    authentication_timeout_ms: Option<u64>,
    max_pending_authentications: Option<usize>,
    tls_server_name: Option<String>,
    tls_pinned_certificate: Option<MaterialSource>,
    tls_certificate_chain: Option<MaterialSource>,
    tls_private_key: Option<MaterialSource>,
    transport_secret: Option<MaterialSource>,
}

impl SecurityFileConfig {
    fn client_auth_config(
        &self,
        catalog: &CredentialCatalog,
    ) -> Result<ClientSecurityConfig, ConfigFileError> {
        if self.authentication_timeout_ms.is_some() || self.max_pending_authentications.is_some() {
            return Err(ConfigFileError::Credential(
                "MPP outbound security cannot set inbound-only authentication_timeout_ms or max_pending_authentications"
                    .to_string(),
            ));
        }
        if !self.credential_ids.is_empty() {
            return Err(ConfigFileError::Credential(
                "MPP outbound security accepts exactly one credential_id".to_string(),
            ));
        }
        let id = CredentialId::parse(self.credential_id.as_deref().ok_or_else(|| {
            ConfigFileError::Credential("MPP outbound security requires credential_id".to_string())
        })?)
        .map_err(|error| ConfigFileError::Credential(error.to_string()))?;
        let credential = catalog
            .credential(&id)
            .map_err(|error| ConfigFileError::Credential(error.to_string()))?;
        if credential.revoked() {
            return Err(ConfigFileError::Credential(format!(
                "MPP outbound credential {id} is revoked"
            )));
        }
        let now_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| {
                ConfigFileError::Credential("system clock is before the Unix epoch".to_string())
            })?
            .as_secs();
        if credential
            .expires_at_unix_secs()
            .is_some_and(|expires_at| now_unix_secs >= expires_at)
        {
            return Err(ConfigFileError::Credential(format!(
                "MPP outbound credential {id} is expired"
            )));
        }
        let auth_freshness_window = Duration::from_secs(
            self.auth_freshness_window_seconds
                .unwrap_or(DEFAULT_AUTH_FRESHNESS_WINDOW_SECONDS),
        );
        Ok(ClientSecurityConfig::new(credential).with_auth_freshness_window(auth_freshness_window))
    }

    fn server_auth_config(
        &self,
        catalog: &CredentialCatalog,
    ) -> Result<ServerSecurityConfig, ConfigFileError> {
        if self.credential_id.is_some() {
            return Err(ConfigFileError::Credential(
                "MPP inbound security requires credential_ids, not credential_id".to_string(),
            ));
        }
        let ids = self
            .credential_ids
            .iter()
            .map(|id| {
                CredentialId::parse(id)
                    .map_err(|error| ConfigFileError::Credential(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let authority = catalog
            .authority(&ids)
            .map_err(|error| ConfigFileError::Credential(error.to_string()))?;
        let now_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| {
                ConfigFileError::Credential("system clock is before the Unix epoch".to_string())
            })?
            .as_secs();
        if !authority.credentials().iter().any(|credential| {
            !credential.revoked()
                && credential
                    .expires_at_unix_secs()
                    .is_none_or(|expiry| now_unix_secs < expiry)
        }) {
            return Err(ConfigFileError::Credential(
                "MPP inbound requires at least one active, unexpired credential".to_string(),
            ));
        }
        Ok(ServerSecurityConfig::new(authority)
            .with_auth_freshness_window(Duration::from_secs(
                self.auth_freshness_window_seconds
                    .unwrap_or(DEFAULT_AUTH_FRESHNESS_WINDOW_SECONDS),
            ))
            .with_authentication_timeout(Duration::from_millis(
                self.authentication_timeout_ms
                    .unwrap_or(DEFAULT_AUTHENTICATION_TIMEOUT_MS),
            ))
            .with_max_pending_authentications(
                self.max_pending_authentications
                    .unwrap_or(DEFAULT_MAX_PENDING_AUTHENTICATIONS),
            ))
    }

    fn client_tls(&self, material_base: &Path) -> Result<TcpClientTlsConfig, ConfigFileError> {
        if self.tls_certificate_chain.is_some() || self.tls_private_key.is_some() {
            return Err(ConfigFileError::MppTlsRoleMismatch(
                "client MPP security cannot contain server-private transport material",
            ));
        }
        let tls = match self.tls_pinned_certificate.as_ref() {
            None => Err(ConfigFileError::MppTlsFieldRequired(
                "tls_pinned_certificate",
            )),
            Some(source) => {
                let certificates =
                    load_certificates_from_source(material_base, source, "pinned TLS certificate")?;
                let [pinned_leaf] = certificates.as_slice() else {
                    return Err(ConfigFileError::MppTlsMaterial(
                        "tls_pinned_certificate must contain exactly one certificate".to_string(),
                    ));
                };
                TcpClientTlsConfig::new(
                    self.tls_server_name
                        .as_deref()
                        .unwrap_or(DEFAULT_MPP_TLS_SERVER_NAME),
                    pinned_leaf.clone(),
                )
                .map_err(|error| ConfigFileError::MppTlsMaterial(error.to_string()))
            }
        }?;
        match self.transport_secret.as_ref() {
            None => Ok(tls),
            Some(source) => Ok(
                tls.with_shared_transport_secret(load_shared_transport_secret(
                    material_base,
                    source,
                )?),
            ),
        }
    }

    fn server_tls(
        &self,
        material_base: &Path,
        security: &ServerSecurityConfig,
    ) -> Result<TcpServerTlsConfig, ConfigFileError> {
        if self.tls_server_name.is_some() || self.tls_pinned_certificate.is_some() {
            return Err(ConfigFileError::MppTlsRoleMismatch(
                "server MPP security cannot contain client-side transport identity material",
            ));
        }
        let tls = match (
            self.tls_certificate_chain.as_ref(),
            self.tls_private_key.as_ref(),
        ) {
            (None, _) => Err(ConfigFileError::MppTlsFieldRequired(
                "tls_certificate_chain",
            )),
            (_, None) => Err(ConfigFileError::MppTlsFieldRequired("tls_private_key")),
            (Some(chain_source), Some(key_source)) => {
                let certificate_chain = load_certificates_from_source(
                    material_base,
                    chain_source,
                    "TLS certificate chain",
                )?;
                let private_key =
                    load_private_key_from_source(material_base, key_source, "TLS private key")?;
                TcpServerTlsConfig::new(certificate_chain, private_key)
                    .map_err(|error| ConfigFileError::MppTlsMaterial(error.to_string()))
            }
        }?;
        match self.transport_secret.as_ref() {
            None => Ok(tls),
            Some(source) => Ok(tls.with_shared_transport_secret(
                load_shared_transport_secret(material_base, source)?,
                security.auth_freshness_window,
                security.max_pending_authentications,
            )),
        }
    }
}

fn load_shared_transport_secret(
    base: &Path,
    source: &MaterialSource,
) -> Result<SharedTransportSecret, ConfigFileError> {
    let bytes = source
        .resolve(base, "shared transport secret")
        .map_err(ConfigFileError::MaterialSource)?
        .into_bytes();
    let bytes = bytes.try_into().map_err(|bytes: Vec<u8>| {
        ConfigFileError::MppTransportSecret(format!(
            "shared transport secret must contain exactly 32 bytes, found {}",
            bytes.len()
        ))
    })?;
    Ok(SharedTransportSecret::new(bytes))
}

fn material_path(base: &Path, configured: &Path) -> PathBuf {
    if configured.is_absolute() {
        configured.to_path_buf()
    } else {
        base.join(configured)
    }
}

pub(crate) fn load_certificates(
    base: &Path,
    configured: &Path,
) -> Result<Vec<CertificateDer<'static>>, ConfigFileError> {
    let path = material_path(base, configured);
    let bytes = std::fs::read(&path).map_err(|error| {
        ConfigFileError::MppTlsMaterial(format!(
            "failed to read TLS certificate file {}: {error}",
            configured.display()
        ))
    })?;
    parse_certificates(&bytes, "TLS certificate file")
}

fn load_certificates_from_source(
    base: &Path,
    source: &MaterialSource,
    purpose: &'static str,
) -> Result<Vec<CertificateDer<'static>>, ConfigFileError> {
    let bytes = source
        .resolve(base, purpose)
        .map_err(ConfigFileError::MaterialSource)?
        .into_bytes();
    parse_certificates(&bytes, purpose)
}

fn parse_certificates(
    bytes: &[u8],
    purpose: &'static str,
) -> Result<Vec<CertificateDer<'static>>, ConfigFileError> {
    let certificates = CertificateDer::pem_slice_iter(bytes)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            ConfigFileError::MppTlsMaterial(format!("failed to parse {purpose}: {error}"))
        })?;
    if certificates.is_empty() {
        return Err(ConfigFileError::MppTlsMaterial(format!(
            "{purpose} contains no certificates"
        )));
    }
    Ok(certificates)
}

pub(crate) fn load_private_key(
    base: &Path,
    configured: &Path,
) -> Result<PrivateKeyDer<'static>, ConfigFileError> {
    let path = material_path(base, configured);
    let bytes = std::fs::read(&path).map_err(|error| {
        ConfigFileError::MppTlsMaterial(format!(
            "failed to read TLS private-key file {}: {error}",
            configured.display()
        ))
    })?;
    parse_private_key(&bytes, "TLS private-key file")
}

fn load_private_key_from_source(
    base: &Path,
    source: &MaterialSource,
    purpose: &'static str,
) -> Result<PrivateKeyDer<'static>, ConfigFileError> {
    let bytes = source
        .resolve(base, purpose)
        .map_err(ConfigFileError::MaterialSource)?
        .into_bytes();
    parse_private_key(&bytes, purpose)
}

fn parse_private_key(
    bytes: &[u8],
    purpose: &'static str,
) -> Result<PrivateKeyDer<'static>, ConfigFileError> {
    let mut keys = PrivateKeyDer::pem_slice_iter(bytes)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            ConfigFileError::MppTlsMaterial(format!("failed to parse {purpose}: {error}"))
        })?
        .into_iter();
    let key = keys.next().ok_or_else(|| {
        ConfigFileError::MppTlsMaterial(format!("{purpose} contains no private key"))
    })?;
    if keys.next().is_some() {
        return Err(ConfigFileError::MppTlsMaterial(format!(
            "{purpose} must contain exactly one private key"
        )));
    }
    Ok(key)
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct MppPerformanceFileConfig {
    extra_traffic_hint_percent: Option<u16>,
}

impl MppPerformanceFileConfig {
    fn into_config(self) -> MppPerformanceConfig {
        MppPerformanceConfig {
            extra_traffic_hint_percent: self
                .extra_traffic_hint_percent
                .unwrap_or(MppPerformanceConfig::default().extra_traffic_hint_percent),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "protocol", rename_all = "kebab-case", deny_unknown_fields)]
enum InboundFileConfig {
    Socks5 {
        name: String,
        #[serde(default)]
        listen: Vec<SocketAddr>,
        #[serde(default)]
        local_users: Vec<String>,
        #[serde(default)]
        admission: LocalIngressAdmissionFileConfig,
    },
    HttpConnect {
        name: String,
        #[serde(default)]
        listen: Vec<SocketAddr>,
        #[serde(default)]
        local_users: Vec<String>,
        #[serde(default)]
        admission: LocalIngressAdmissionFileConfig,
    },
    Mixed {
        name: String,
        #[serde(default)]
        listen: Vec<SocketAddr>,
        #[serde(default)]
        local_users: Vec<String>,
        #[serde(default)]
        admission: LocalIngressAdmissionFileConfig,
    },
    TcpForward {
        name: String,
        listen: Vec<SocketAddr>,
        target: String,
        max_connections: Option<u32>,
    },
    UdpForward {
        name: String,
        listen: Vec<SocketAddr>,
        target: String,
        max_associations: Option<u32>,
        idle_timeout_ms: Option<u64>,
        datagram_ttl_ms: Option<u64>,
    },
    MixedForward {
        name: String,
        listen: Vec<SocketAddr>,
        target: String,
        max_connections: Option<u32>,
        max_associations: Option<u32>,
        idle_timeout_ms: Option<u64>,
        datagram_ttl_ms: Option<u64>,
    },
    Tun {
        name: String,
        interface_name: Option<String>,
        ipv4: Option<Ipv4Addr>,
        #[serde(default)]
        disable_ipv4: bool,
        ipv4_prefix: Option<u8>,
        ipv4_gateway: Option<Ipv4Addr>,
        ipv6: Option<Ipv6Addr>,
        ipv6_prefix: Option<u8>,
        mtu: Option<u16>,
        #[serde(default)]
        disable_icmp: bool,
        #[serde(default)]
        dns_redirects: Vec<SocketAddr>,
        dns_ttl_ms: Option<u32>,
        #[serde(default)]
        host: TunHostFileConfig,
    },
    TunL3 {
        name: String,
        outbound: String,
        interface_name: Option<String>,
    },
    Mpp {
        name: String,
        security: Box<SecurityFileConfig>,
        #[serde(default)]
        performance: MppPerformanceFileConfig,
        #[serde(default)]
        paths: Vec<MppPathFileConfig>,
        #[serde(default)]
        peer_diagnostics_principal_ids: PeerDiagnosticsPrincipalSelectorFileValue,
    },
    MppL3 {
        name: String,
        security: Box<SecurityFileConfig>,
        #[serde(default)]
        paths: Vec<MppPathFileConfig>,
        #[serde(default)]
        peer_diagnostics_principal_ids: PeerDiagnosticsPrincipalSelectorFileValue,
        tun_l3: TunL3ServerFileConfig,
    },
}

fn infer_forwarding_mode(
    inbounds: &[InboundFileConfig],
) -> Result<ForwardingMode, ConfigFileError> {
    let has_l3 = inbounds.iter().any(|inbound| {
        matches!(
            inbound,
            InboundFileConfig::TunL3 { .. } | InboundFileConfig::MppL3 { .. }
        )
    });
    let has_l4 = inbounds.iter().any(|inbound| {
        !matches!(
            inbound,
            InboundFileConfig::TunL3 { .. } | InboundFileConfig::MppL3 { .. }
        )
    });
    match (has_l4, has_l3) {
        (true, false) => Ok(ForwardingMode::L4),
        (false, true) => Ok(ForwardingMode::L3),
        (true, true) => Err(ConfigFileError::MixedForwardingFamilies),
        (false, false) => Err(ConfigFileError::NoRuntimeServices),
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum PeerDiagnosticsPrincipalSelectorFileValue {
    Scalar(String),
    Concrete(Vec<String>),
}

impl Default for PeerDiagnosticsPrincipalSelectorFileValue {
    fn default() -> Self {
        Self::Concrete(Vec::new())
    }
}

impl PeerDiagnosticsPrincipalSelectorFileValue {
    fn into_policy(
        self,
        inbound: &str,
        authority: &crate::product::CredentialAuthority,
    ) -> Result<crate::config::PeerDiagnosticsPrincipalPolicy, ConfigFileError> {
        let values = match self {
            Self::Scalar(value) if value == "*" => {
                return Ok(crate::config::PeerDiagnosticsPrincipalPolicy::All);
            }
            Self::Scalar(value) => {
                return Err(ConfigFileError::PeerDiagnostics(format!(
                    "MPP inbound {inbound:?} peer_diagnostics_principal_ids scalar must be exactly \"*\", got {value:?}"
                )));
            }
            Self::Concrete(values) if values.iter().any(|value| value == "*") => {
                return Err(ConfigFileError::PeerDiagnostics(format!(
                    "MPP inbound {inbound:?} peer diagnostics wildcard must be the scalar \"*\" and cannot be mixed with concrete principals"
                )));
            }
            Self::Concrete(values) => values,
        };

        let now_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| {
                ConfigFileError::PeerDiagnostics(
                    "system clock is before the Unix epoch".to_string(),
                )
            })?
            .as_secs();
        let active_principals = authority
            .credentials()
            .into_iter()
            .filter(|credential| {
                !credential.revoked()
                    && credential
                        .expires_at_unix_secs()
                        .is_none_or(|expiry| now_unix_secs < expiry)
            })
            .map(|credential| credential.principal().clone())
            .collect::<HashSet<_>>();
        let mut selected = Vec::with_capacity(values.len());
        let mut unique = HashSet::with_capacity(values.len());
        for value in values {
            let principal = PrincipalId::parse(&value)
                .map_err(|error| ConfigFileError::PeerDiagnostics(error.to_string()))?;
            if !unique.insert(principal.clone()) {
                return Err(ConfigFileError::PeerDiagnostics(format!(
                    "MPP inbound {inbound:?} repeats peer diagnostics principal {principal}"
                )));
            }
            if !active_principals.contains(&principal) {
                return Err(ConfigFileError::PeerDiagnostics(format!(
                    "MPP inbound {inbound:?} peer diagnostics principal {principal} has no active, unrevoked, unexpired accepted credential"
                )));
            }
            selected.push(principal);
        }
        Ok(crate::config::PeerDiagnosticsPrincipalPolicy::selected(
            selected,
        ))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TunL3ServerFileConfig {
    interface_name: Option<String>,
    ipv4_pool: Option<String>,
    ipv4: Option<Ipv4Addr>,
    ipv6_pool: Option<String>,
    ipv6: Option<Ipv6Addr>,
    mtu: Option<u16>,
    #[serde(default)]
    allocations: Vec<TunL3AllocationFileConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TunL3AllocationFileConfig {
    principal_id: String,
    ipv4: Option<Ipv4Addr>,
    ipv6: Option<Ipv6Addr>,
    #[serde(default)]
    allowed_ips: Vec<String>,
}

impl TunL3ServerFileConfig {
    fn into_plan(
        self,
        authority: &crate::product::CredentialAuthority,
    ) -> Result<TunL3AddressPlan, ConfigFileError> {
        let now_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| {
                ConfigFileError::TunL3("system clock is before the Unix epoch".to_string())
            })?
            .as_secs();
        let active_principals = authority
            .credentials()
            .into_iter()
            .filter(|credential| {
                !credential.revoked()
                    && credential
                        .expires_at_unix_secs()
                        .is_none_or(|expiry| now_unix_secs < expiry)
            })
            .map(|credential| credential.principal().clone())
            .collect::<HashSet<_>>();
        let ipv4_pool = self
            .ipv4_pool
            .map(|value| {
                value
                    .parse::<ipnet::Ipv4Net>()
                    .map_err(|error| ConfigFileError::TunL3(error.to_string()))
            })
            .transpose()?;
        let ipv6_pool = self
            .ipv6_pool
            .map(|value| {
                value
                    .parse::<ipnet::Ipv6Net>()
                    .map_err(|error| ConfigFileError::TunL3(error.to_string()))
            })
            .transpose()?;
        let allocations = self
            .allocations
            .into_iter()
            .map(|allocation| {
                let principal_id = PrincipalId::parse(&allocation.principal_id)
                    .map_err(|error| ConfigFileError::TunL3(error.to_string()))?;
                if !active_principals.contains(&principal_id) {
                    return Err(ConfigFileError::TunL3(format!(
                        "TUN-L3 allocation principal {principal_id} has no active, unrevoked, unexpired accepted credential"
                    )));
                }
                let allowed_ips = allocation
                    .allowed_ips
                    .into_iter()
                    .map(|value| {
                        value
                            .parse::<ipnet::IpNet>()
                            .map_err(|error| ConfigFileError::TunL3(error.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(TunL3AllocationSpec {
                    principal_id,
                    ipv4: allocation.ipv4,
                    ipv6: allocation.ipv6,
                    allowed_ips,
                })
            })
            .collect::<Result<Vec<_>, ConfigFileError>>()?;
        TunL3AddressPlan::compile(
            TunL3ServerSpec {
                interface_name: self.interface_name,
                ipv4_pool,
                ipv4: self.ipv4,
                ipv6_pool,
                ipv6: self.ipv6,
                mtu: self.mtu.unwrap_or(DEFAULT_TUN_MTU),
                allocations,
            },
            authority,
        )
        .map_err(|error| ConfigFileError::TunL3(error.to_string()))
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalIngressAdmissionFileConfig {
    max_connections: Option<usize>,
    max_connections_per_source: Option<usize>,
    max_connections_per_principal: Option<usize>,
    handshake_timeout_ms: Option<u64>,
}

impl LocalIngressAdmissionFileConfig {
    fn into_config(self) -> Result<LocalIngressAdmissionConfig, ConfigFileError> {
        let defaults = LocalIngressAdmissionConfig::default();
        LocalIngressAdmissionConfig::new(
            self.max_connections.unwrap_or(defaults.max_connections()),
            self.max_connections_per_source
                .unwrap_or(defaults.max_connections_per_source()),
            self.max_connections_per_principal
                .unwrap_or(defaults.max_connections_per_principal()),
            Duration::from_millis(
                self.handshake_timeout_ms
                    .unwrap_or(defaults.handshake_timeout().as_millis() as u64),
            ),
        )
        .map_err(ConfigFileError::LocalAdmission)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OutboundProxyAuthFileConfig {
    username: Option<String>,
    password: Option<MaterialSource>,
}

impl std::fmt::Debug for OutboundProxyAuthFileConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyAuthFileConfig")
            .field("username", &self.username)
            .field("password", &self.password)
            .finish()
    }
}

impl OutboundProxyAuthFileConfig {
    fn into_outbound_credentials(
        self,
        material_base: &Path,
    ) -> Result<ProxyCredentials, ConfigFileError> {
        let username = self
            .username
            .ok_or(ConfigFileError::ProxyUsernameRequired)?;
        let password = self
            .password
            .ok_or(ConfigFileError::ProxyPasswordRequired)?
            .resolve(material_base, "upstream proxy password")
            .map_err(ConfigFileError::MaterialSource)?
            .into_utf8("upstream proxy password")
            .map_err(ConfigFileError::MaterialSource)?;
        ProxyCredentials::new(username, password).map_err(ConfigFileError::Outbound)
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct TunFileConfig {
    interface_name: Option<String>,
    ipv4: Option<Ipv4Addr>,
    #[serde(default)]
    disable_ipv4: bool,
    ipv4_prefix: Option<u8>,
    ipv4_gateway: Option<Ipv4Addr>,
    ipv6: Option<Ipv6Addr>,
    ipv6_prefix: Option<u8>,
    mtu: Option<u16>,
    #[serde(default)]
    disable_icmp: bool,
    #[serde(default)]
    dns_redirects: Vec<SocketAddr>,
    dns_ttl_ms: Option<u32>,
    #[serde(default)]
    host: TunHostFileConfig,
}

impl TunFileConfig {
    fn into_config(self) -> Result<TunL4Config, ConfigFileError> {
        let ipv4 = if self.disable_ipv4 {
            if self.ipv4.is_some() || self.ipv4_gateway.is_some() {
                return Err(ConfigFileError::TunIpv4DisabledWithIpv4Options);
            }
            None
        } else {
            Some(self.ipv4.unwrap_or(DEFAULT_TUN_IPV4))
        };
        Ok(TunL4Config {
            interface_name: self.interface_name,
            ipv4,
            ipv4_prefix: self.ipv4_prefix.unwrap_or(DEFAULT_TUN_IPV4_PREFIX),
            ipv4_gateway: self.ipv4_gateway,
            ipv6: self.ipv6,
            ipv6_prefix: self.ipv6_prefix.unwrap_or(64),
            mtu: self.mtu.unwrap_or(DEFAULT_TUN_MTU),
            enable_icmp: !self.disable_icmp,
            dns_resolvers: self.dns_redirects,
            dns_ttl_ms: self.dns_ttl_ms.unwrap_or(DEFAULT_TUN_DNS_TTL_MS),
            host: self.host.into_config()?,
        })
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
enum TunHostFileConfig {
    #[default]
    External,
    Managed {
        #[serde(default)]
        route_mode: ManagedVpnRouteModeFileValue,
        #[serde(default)]
        include_cidrs: Vec<String>,
        #[serde(default)]
        exclude_cidrs: Vec<String>,
        #[serde(default)]
        local_lan: bool,
        #[serde(default)]
        dns_listeners: Vec<IpAddr>,
        linux: Option<ManagedVpnLinuxFileConfig>,
    },
}

impl TunHostFileConfig {
    fn into_config(self) -> Result<TunHostConfig, ConfigFileError> {
        let Self::Managed {
            route_mode,
            include_cidrs,
            exclude_cidrs,
            local_lan,
            dns_listeners,
            linux,
        } = self
        else {
            return Ok(TunHostConfig::External);
        };
        if matches!(route_mode, ManagedVpnRouteModeFileValue::Full) && !include_cidrs.is_empty() {
            return Err(ConfigFileError::ManagedVpnValue(
                "managed full VPN cannot set include_cidrs".to_string(),
            ));
        }
        let includes = parse_managed_vpn_cidrs(include_cidrs, "include_cidrs")?;
        let excludes = parse_managed_vpn_cidrs(exclude_cidrs, "exclude_cidrs")?;
        let route_mode = match route_mode {
            ManagedVpnRouteModeFileValue::Full => RouteMode::Full,
            ManagedVpnRouteModeFileValue::Split => RouteMode::Split(includes),
        };
        let linux = linux
            .map(ManagedVpnLinuxFileConfig::into_config)
            .transpose()?;
        Ok(TunHostConfig::Managed(ManagedVpnConfig {
            route_mode,
            excludes,
            local_lan,
            dns_capture_servers: dns_listeners,
            platform: ManagedVpnPlatformConfig { linux },
        }))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagedVpnLinuxFileConfig {
    route_table: Option<u32>,
    native_rule_priority: Option<u32>,
    capture_rule_priority: Option<u32>,
    socket_mark: Option<u32>,
}

impl ManagedVpnLinuxFileConfig {
    fn into_config(self) -> Result<LinuxPolicyConfig, ConfigFileError> {
        let mark =
            LinuxSocketMark::new(self.socket_mark.unwrap_or(DEFAULT_LINUX_SOCKET_MARK.get()))
                .map_err(|error| ConfigFileError::ManagedVpnValue(error.to_string()))?;
        LinuxPolicyConfig::new(
            self.route_table.unwrap_or(DEFAULT_LINUX_ROUTE_TABLE),
            self.native_rule_priority
                .unwrap_or(DEFAULT_LINUX_NATIVE_RULE_PRIORITY),
            self.capture_rule_priority
                .unwrap_or(DEFAULT_LINUX_CAPTURE_RULE_PRIORITY),
            mark,
        )
        .map_err(|error| ConfigFileError::ManagedVpnValue(error.to_string()))
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum ManagedVpnRouteModeFileValue {
    #[default]
    Full,
    Split,
}

fn parse_managed_vpn_cidrs(
    values: Vec<String>,
    field: &'static str,
) -> Result<Vec<ipnet::IpNet>, ConfigFileError> {
    values
        .into_iter()
        .map(|value| {
            value.parse::<ipnet::IpNet>().map_err(|error| {
                ConfigFileError::ManagedVpnValue(format!(
                    "invalid managed VPN {field} value {value:?}: {error}"
                ))
            })
        })
        .collect()
}

fn listen_or_default(listen: Vec<SocketAddr>, port: u16) -> Vec<SocketAddr> {
    if listen.is_empty() {
        vec![SocketAddr::from(([127, 0, 0, 1], port))]
    } else {
        listen
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MppPathFileConfig {
    name: String,
    endpoint: String,
}

fn parse_named_path_specs(
    values: Vec<MppPathFileConfig>,
) -> Result<Vec<NamedPathConfig>, ConfigFileError> {
    values
        .into_iter()
        .map(|value| {
            Ok(NamedPathConfig {
                name: canonical_config_name(&value.name)?,
                spec: value.endpoint.parse().map_err(ConfigFileError::PathSpec)?,
            })
        })
        .collect()
}

#[derive(Debug, Deserialize)]
#[serde(tag = "protocol", rename_all = "kebab-case", deny_unknown_fields)]
enum OutboundFileConfig {
    Mpp {
        name: String,
        security: SecurityFileConfig,
        #[serde(default)]
        allow_peer_diagnostics: bool,
        performance: Option<MppPerformanceFileConfig>,
        #[serde(default)]
        paths: Vec<MppPathFileConfig>,
        path_probe_interval_ms: Option<u64>,
        path_probe_timeout_ms: Option<u64>,
    },
    Direct {
        name: String,
        bind_ip: Option<IpAddr>,
        bind_ipv4: Option<Ipv4Addr>,
        bind_ipv6: Option<Ipv6Addr>,
        connect_timeout_ms: Option<u64>,
    },
    Socks5 {
        name: String,
        endpoint: Option<String>,
        auth: Option<OutboundProxyAuthFileConfig>,
        connect_timeout_ms: Option<u64>,
    },
    HttpConnect {
        name: String,
        endpoint: Option<String>,
        auth: Option<OutboundProxyAuthFileConfig>,
        connect_timeout_ms: Option<u64>,
    },
    HttpsConnect {
        name: String,
        endpoint: Option<String>,
        auth: Option<OutboundProxyAuthFileConfig>,
        tls_server_name: Option<String>,
        tls_ca_certificate: Option<MaterialSource>,
        connect_timeout_ms: Option<u64>,
    },
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutingFileConfig {
    #[serde(default)]
    balancers: Vec<RoutingBalancerFileConfig>,
    #[serde(default)]
    rule_set_publishers: Vec<RoutingRuleSetPublisherFileConfig>,
    #[serde(default)]
    rule_sets: Vec<RoutingRuleSetFileConfig>,
    #[serde(default)]
    rules: Vec<RoutingRuleFileConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutingRuleSetPublisherFileConfig {
    publisher_id: String,
    ed25519_public_key: MaterialSource,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutingRuleSetFileConfig {
    rule_set_id: String,
    publisher_id: String,
    minimum_revision: u64,
    file: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutingBalancerFileConfig {
    name: String,
    strategy: RoutingStrategyFileValue,
    #[serde(default)]
    members: Vec<RoutingBalancerMemberFileConfig>,
    #[serde(default)]
    health: RoutingBalancerHealthFileConfig,
    stickiness: Option<RoutingBalancerStickinessFileConfig>,
    manual_outbound: Option<String>,
    probe: Option<RoutingBalancerProbeFileConfig>,
    freshness_ttl_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutingBalancerMemberFileConfig {
    outbound: String,
    #[serde(default = "default_gateway_member_weight")]
    weight: u32,
    #[serde(default)]
    mode: RoutingBalancerMemberModeFileValue,
}

const fn default_gateway_member_weight() -> u32 {
    1
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutingBalancerHealthFileConfig {
    failure_threshold: Option<u32>,
    recovery_threshold: Option<u32>,
    initial_backoff_ms: Option<u64>,
    maximum_backoff_ms: Option<u64>,
}

impl RoutingBalancerHealthFileConfig {
    fn into_config(self) -> GatewayHealthPolicy {
        let default = GatewayHealthPolicy::default();
        GatewayHealthPolicy {
            failure_threshold: self.failure_threshold.unwrap_or(default.failure_threshold),
            recovery_threshold: self
                .recovery_threshold
                .unwrap_or(default.recovery_threshold),
            initial_backoff: Duration::from_millis(
                self.initial_backoff_ms
                    .unwrap_or(default.initial_backoff.as_millis() as u64),
            ),
            maximum_backoff: Duration::from_millis(
                self.maximum_backoff_ms
                    .unwrap_or(default.maximum_backoff.as_millis() as u64),
            ),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutingBalancerStickinessFileConfig {
    key: RoutingBalancerStickinessKeyFileValue,
    ttl_ms: u64,
    capacity: usize,
}

impl RoutingBalancerStickinessFileConfig {
    fn into_config(self) -> (GatewayStickinessPolicy, GatewayStickinessKey) {
        (
            GatewayStickinessPolicy {
                ttl: Duration::from_millis(self.ttl_ms),
                capacity: self.capacity,
            },
            match self.key {
                RoutingBalancerStickinessKeyFileValue::Destination => {
                    GatewayStickinessKey::Destination
                }
                RoutingBalancerStickinessKeyFileValue::Principal => GatewayStickinessKey::Principal,
            },
        )
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutingBalancerProbeFileConfig {
    target: String,
    interval_ms: u64,
    timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RoutingBalancerStickinessKeyFileValue {
    Destination,
    Principal,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RoutingBalancerMemberModeFileValue {
    #[default]
    Enabled,
    Draining,
    Disabled,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RoutingStrategyFileValue {
    Manual,
    OrderedFailover,
    RoundRobin,
    Random,
    WeightedRandom,
    LeastLatency,
    LeastLoad,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutingRuleFileConfig {
    name: String,
    #[serde(default)]
    domain_exact: Vec<String>,
    #[serde(default)]
    domain_suffix: Vec<String>,
    #[serde(default)]
    domain_keyword: Vec<String>,
    #[serde(default)]
    domain_regex: Vec<String>,
    #[serde(default)]
    domain_rule_set_ids: Vec<String>,
    #[serde(default)]
    destination_cidrs: Vec<String>,
    #[serde(default)]
    destination_rule_set_ids: Vec<String>,
    #[serde(default)]
    source_cidrs: Vec<String>,
    #[serde(default)]
    destination_ports: Vec<RoutingPortFileValue>,
    #[serde(default)]
    source_ports: Vec<RoutingPortFileValue>,
    #[serde(default)]
    networks: Vec<RoutingNetworkFileValue>,
    #[serde(default)]
    inbounds: RoutingIdentitySelectorFileValue,
    #[serde(default)]
    principal_ids: RoutingIdentitySelectorFileValue,
    #[serde(default)]
    stages: Vec<RoutingStageFileValue>,
    decision: Option<RoutingDecisionFileValue>,
    outbound: Option<String>,
    balancer: Option<String>,
    dns_policy: Option<String>,
    initial_demand: Option<RoutingInitialDemandFileValue>,
    explanation: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum RoutingPortFileValue {
    Single(u16),
    Range(String),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum RoutingIdentitySelectorFileValue {
    Scalar(String),
    Concrete(Vec<String>),
}

impl Default for RoutingIdentitySelectorFileValue {
    fn default() -> Self {
        Self::Concrete(Vec::new())
    }
}

impl RoutingIdentitySelectorFileValue {
    fn into_concrete(self, field: &'static str) -> Result<Vec<String>, ConfigFileError> {
        match self {
            Self::Scalar(value) if value == "*" => Ok(Vec::new()),
            Self::Scalar(value) => Err(ConfigFileError::RoutingValue(format!(
                "routing {field} scalar must be exactly \"*\", got {value:?}"
            ))),
            Self::Concrete(values) if values.iter().any(|value| value == "*") => {
                Err(ConfigFileError::RoutingValue(format!(
                    "routing {field} wildcard must be the scalar \"*\" and cannot be mixed with concrete values"
                )))
            }
            Self::Concrete(values) => Ok(values),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RoutingNetworkFileValue {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RoutingStageFileValue {
    PreResolution,
    PostResolution,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RoutingDecisionFileValue {
    Allow,
    AllowRestricted,
    Reject,
    Drop,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RoutingInitialDemandFileValue {
    #[default]
    Automatic,
    Throughput,
}

#[derive(Debug, Clone)]
struct ParsedOutbounds {
    leaves: HashMap<String, OutboundLeafConfig>,
    balancers: HashMap<String, GatewayBalancerConfig>,
    order: Vec<String>,
    balancer_order: Vec<String>,
    explicit_mpp_performance: HashSet<String>,
}

fn parse_outbounds(
    values: Vec<OutboundFileConfig>,
    material_base: &Path,
    credential_catalog: &CredentialCatalog,
) -> Result<ParsedOutbounds, ConfigFileError> {
    let mut parsed = ParsedOutbounds {
        leaves: HashMap::new(),
        balancers: HashMap::new(),
        order: Vec::new(),
        balancer_order: Vec::new(),
        explicit_mpp_performance: HashSet::new(),
    };
    for value in values {
        match value {
            OutboundFileConfig::Mpp {
                name,
                security,
                allow_peer_diagnostics,
                performance,
                paths,
                path_probe_interval_ms,
                path_probe_timeout_ms,
            } => {
                let name = canonical_config_name(&name)?;
                insert_outbound_name(&parsed, &name)?;
                let explicit_performance = performance.is_some();
                let named_paths = parse_named_path_specs(paths)?;
                if named_paths.is_empty() {
                    return Err(ConfigFileError::MppOutboundRequiresPath(name));
                }
                let security_config = security.client_auth_config(credential_catalog)?;
                let tls = security.client_tls(material_base)?;
                let paths = named_paths
                    .into_iter()
                    .map(|path| ClientPathConfig {
                        name: path.name,
                        tls: tls.clone(),
                        spec: path.spec,
                        security: security_config.clone(),
                    })
                    .collect();
                let id = OutboundId::parse(&name)
                    .map_err(|error| ConfigFileError::RoutingValue(error.to_string()))?;
                parsed.order.push(name.clone());
                if explicit_performance {
                    parsed.explicit_mpp_performance.insert(name.clone());
                }
                parsed.leaves.insert(
                    name,
                    OutboundLeafConfig::Mpp {
                        id,
                        config: Box::new(MppOutboundConfig {
                            security: security_config,
                            paths,
                            path_probe_interval: Duration::from_millis(
                                path_probe_interval_ms.unwrap_or(DEFAULT_PATH_PROBE_INTERVAL_MS),
                            ),
                            path_probe_timeout: Duration::from_millis(
                                path_probe_timeout_ms.unwrap_or(DEFAULT_PATH_PROBE_TIMEOUT_MS),
                            ),
                            allow_peer_diagnostics,
                            performance: performance.unwrap_or_default().into_config(),
                        }),
                    },
                );
            }
            OutboundFileConfig::Direct {
                name,
                bind_ip,
                bind_ipv4,
                bind_ipv6,
                connect_timeout_ms,
            } => {
                let name = canonical_config_name(&name)?;
                insert_outbound_name(&parsed, &name)?;
                let config = match (bind_ip, bind_ipv4, bind_ipv6) {
                    (Some(_), Some(_), _) | (Some(_), _, Some(_)) => {
                        return Err(ConfigFileError::DirectBindFieldConflict);
                    }
                    (Some(ip), None, None) => OutboundConfig::BindSourceIp(ip),
                    (None, None, None) => OutboundConfig::Direct,
                    (None, ipv4, ipv6) => OutboundConfig::BindSourceIps { ipv4, ipv6 },
                };
                let id = OutboundId::parse(&name)
                    .map_err(|error| ConfigFileError::RoutingValue(error.to_string()))?;
                parsed.order.push(name.clone());
                parsed.leaves.insert(
                    name,
                    OutboundLeafConfig::Local {
                        id,
                        config,
                        connect_timeout: outbound_connect_timeout(connect_timeout_ms),
                    },
                );
            }
            OutboundFileConfig::Socks5 {
                name,
                endpoint,
                auth,
                connect_timeout_ms,
            } => {
                let name = canonical_config_name(&name)?;
                insert_outbound_name(&parsed, &name)?;
                let id = OutboundId::parse(&name)
                    .map_err(|error| ConfigFileError::RoutingValue(error.to_string()))?;
                parsed.order.push(name.clone());
                parsed.leaves.insert(
                    name,
                    OutboundLeafConfig::Local {
                        id,
                        config: OutboundConfig::Socks5(ProxyConfig::new(
                            endpoint
                                .ok_or(ConfigFileError::MissingOutboundEndpoint)?
                                .parse()
                                .map_err(ConfigFileError::Endpoint)?,
                            auth.map(|auth| auth.into_outbound_credentials(material_base))
                                .transpose()?,
                        )),
                        connect_timeout: outbound_connect_timeout(connect_timeout_ms),
                    },
                );
            }
            OutboundFileConfig::HttpConnect {
                name,
                endpoint,
                auth,
                connect_timeout_ms,
            } => {
                let name = canonical_config_name(&name)?;
                insert_outbound_name(&parsed, &name)?;
                let id = OutboundId::parse(&name)
                    .map_err(|error| ConfigFileError::RoutingValue(error.to_string()))?;
                parsed.order.push(name.clone());
                parsed.leaves.insert(
                    name,
                    OutboundLeafConfig::Local {
                        id,
                        config: OutboundConfig::HttpConnect(ProxyConfig::new(
                            endpoint
                                .ok_or(ConfigFileError::MissingOutboundEndpoint)?
                                .parse()
                                .map_err(ConfigFileError::Endpoint)?,
                            auth.map(|auth| auth.into_outbound_credentials(material_base))
                                .transpose()?,
                        )),
                        connect_timeout: outbound_connect_timeout(connect_timeout_ms),
                    },
                );
            }
            OutboundFileConfig::HttpsConnect {
                name,
                endpoint,
                auth,
                tls_server_name,
                tls_ca_certificate,
                connect_timeout_ms,
            } => {
                let name = canonical_config_name(&name)?;
                insert_outbound_name(&parsed, &name)?;
                let proxy = ProxyConfig::new(
                    endpoint
                        .ok_or(ConfigFileError::MissingOutboundEndpoint)?
                        .parse()
                        .map_err(ConfigFileError::Endpoint)?,
                    auth.map(|auth| auth.into_outbound_credentials(material_base))
                        .transpose()?,
                );
                let root_certificates = tls_ca_certificate
                    .as_ref()
                    .map(|source| {
                        load_certificates_from_source(
                            material_base,
                            source,
                            "HTTPS proxy CA certificate",
                        )
                    })
                    .transpose()?
                    .unwrap_or_default();
                let id = OutboundId::parse(&name)
                    .map_err(|error| ConfigFileError::RoutingValue(error.to_string()))?;
                parsed.order.push(name.clone());
                parsed.leaves.insert(
                    name,
                    OutboundLeafConfig::Local {
                        id,
                        config: OutboundConfig::HttpsConnect(Box::new(
                            HttpsProxyConfig::new(proxy, tls_server_name, root_certificates)
                                .map_err(ConfigFileError::Outbound)?,
                        )),
                        connect_timeout: outbound_connect_timeout(connect_timeout_ms),
                    },
                );
            }
        }
    }
    Ok(parsed)
}

fn apply_routing(
    generation: u64,
    balancers: Vec<RoutingBalancerFileConfig>,
    parsed: &mut ParsedOutbounds,
) -> Result<(), ConfigFileError> {
    for balancer in balancers {
        let name = canonical_config_name(&balancer.name)?;
        insert_balancer_name(parsed, &name)?;
        if balancer.members.is_empty() {
            return Err(ConfigFileError::RoutingBalancerRequiresMembers(name));
        }
        let strategy = match balancer.strategy {
            RoutingStrategyFileValue::Manual => GatewayStrategy::Manual,
            RoutingStrategyFileValue::OrderedFailover => GatewayStrategy::OrderedFailover,
            RoutingStrategyFileValue::RoundRobin => GatewayStrategy::RoundRobin,
            RoutingStrategyFileValue::Random => GatewayStrategy::Random,
            RoutingStrategyFileValue::WeightedRandom => GatewayStrategy::WeightedRandom,
            RoutingStrategyFileValue::LeastLatency => GatewayStrategy::LeastLatency,
            RoutingStrategyFileValue::LeastLoad => GatewayStrategy::LeastLoad,
        };
        let mut members = Vec::with_capacity(balancer.members.len());
        for member in balancer.members {
            let name = canonical_config_name(&member.outbound)?;
            let leaf = parsed
                .leaves
                .get(&name)
                .ok_or_else(|| ConfigFileError::MissingOutboundName(name.clone()))?;
            let mode = match member.mode {
                RoutingBalancerMemberModeFileValue::Enabled => GatewayMemberMode::Enabled,
                RoutingBalancerMemberModeFileValue::Draining => GatewayMemberMode::Draining,
                RoutingBalancerMemberModeFileValue::Disabled => GatewayMemberMode::Disabled,
            };
            members.push(
                GatewayMemberSpec::new(leaf.id().clone(), member.weight, leaf.networks())
                    .with_mode(mode),
            );
        }
        let mut spec = GatewayBalancerSpec::new(strategy, members);
        spec.health = balancer.health.into_config();
        if let Some((stickiness, key)) = balancer
            .stickiness
            .map(RoutingBalancerStickinessFileConfig::into_config)
        {
            spec.stickiness = Some(stickiness);
            spec.stickiness_key = key;
        }
        spec.manual_member = balancer
            .manual_outbound
            .map(|outbound| {
                let outbound = canonical_config_name(&outbound)?;
                OutboundId::parse(&outbound)
                    .map_err(|error| ConfigFileError::RoutingValue(error.to_string()))
            })
            .transpose()?;
        spec.probe = balancer
            .probe
            .map(|probe| {
                Ok::<_, ConfigFileError>(GatewayProbePolicy {
                    target: ProtocolTarget::parse_authority(&probe.target)
                        .map_err(|error| ConfigFileError::RoutingValue(error.to_string()))?,
                    interval: Duration::from_millis(probe.interval_ms),
                    timeout: Duration::from_millis(probe.timeout_ms),
                })
            })
            .transpose()?;
        if let Some(freshness_ttl_ms) = balancer.freshness_ttl_ms {
            spec.freshness_ttl = Duration::from_millis(freshness_ttl_ms);
        }
        GatewayBalancer::compile(generation, spec.clone())
            .map_err(|error| ConfigFileError::RoutingValue(error.to_string()))?;
        let id = BalancerId::parse(&name)
            .map_err(|error| ConfigFileError::RoutingValue(error.to_string()))?;
        parsed.balancer_order.push(name.clone());
        parsed.balancers.insert(
            name,
            GatewayBalancerConfig {
                id,
                generation,
                spec,
            },
        );
    }
    Ok(())
}

fn canonical_config_name(name: &str) -> Result<String, ConfigFileError> {
    if name.trim().is_empty() {
        return Err(ConfigFileError::EmptyName);
    }
    let canonical = RuleId::parse(name)
        .map_err(|error| ConfigFileError::RoutingValue(error.to_string()))?
        .as_str()
        .to_string();
    if canonical != name {
        return Err(ConfigFileError::NonCanonicalName(name.to_string()));
    }
    Ok(canonical)
}

fn insert_outbound_name(parsed: &ParsedOutbounds, name: &str) -> Result<(), ConfigFileError> {
    if parsed.leaves.contains_key(name) {
        return Err(ConfigFileError::DuplicateOutboundName(name.to_string()));
    }
    Ok(())
}

fn insert_balancer_name(parsed: &ParsedOutbounds, name: &str) -> Result<(), ConfigFileError> {
    if parsed.balancers.contains_key(name) {
        return Err(ConfigFileError::DuplicateBalancerName(name.to_string()));
    }
    Ok(())
}

fn configured_routed_inbound_names(
    inbounds: &[InboundFileConfig],
) -> Result<HashSet<String>, ConfigFileError> {
    let mut names = HashSet::new();
    for inbound in inbounds {
        let name = match inbound {
            InboundFileConfig::Socks5 { name, .. }
            | InboundFileConfig::HttpConnect { name, .. }
            | InboundFileConfig::Mixed { name, .. }
            | InboundFileConfig::TcpForward { name, .. }
            | InboundFileConfig::UdpForward { name, .. }
            | InboundFileConfig::MixedForward { name, .. }
            | InboundFileConfig::Tun { name, .. }
            | InboundFileConfig::Mpp { name, .. } => name,
            InboundFileConfig::TunL3 { .. } | InboundFileConfig::MppL3 { .. } => continue,
        };
        let name = canonical_config_name(name)?;
        InboundId::parse(&name)
            .map_err(|error| ConfigFileError::RoutingValue(error.to_string()))?;
        if !names.insert(name.clone()) {
            return Err(ConfigFileError::DuplicateInboundName(name));
        }
    }
    Ok(names)
}

fn compile_rule_set_registry(
    publishers: Vec<RoutingRuleSetPublisherFileConfig>,
    rule_sets: Vec<RoutingRuleSetFileConfig>,
    material_base: &Path,
) -> Result<CompiledRuleSetRegistry, ConfigFileError> {
    let publishers = publishers
        .into_iter()
        .map(|publisher| {
            let id = RuleSetPublisherId::parse(&publisher.publisher_id)
                .map_err(|error| ConfigFileError::RuleSet(error.to_string()))?;
            let public_key = publisher
                .ed25519_public_key
                .resolve(material_base, "Ed25519 rule-set publisher public key")
                .map_err(ConfigFileError::MaterialSource)?
                .into_bytes();
            let public_key: [u8; 32] = public_key.try_into().map_err(|_| {
                ConfigFileError::RuleSet(format!(
                    "publisher {id} Ed25519 public key must contain exactly 32 bytes"
                ))
            })?;
            Ok::<_, ConfigFileError>(RuleSetPublisher::new(id, public_key))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let publishers = RuleSetPublisherCatalog::compile(publishers)
        .map_err(|error| ConfigFileError::RuleSet(error.to_string()))?;

    let now_unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ConfigFileError::RuleSet("system clock is before the Unix epoch".to_string()))?
        .as_secs();
    let mut verified = Vec::with_capacity(rule_sets.len());
    for rule_set in rule_sets {
        let expected_id = RuleSetId::parse(&rule_set.rule_set_id)
            .map_err(|error| ConfigFileError::RuleSet(error.to_string()))?;
        let expected_publisher = RuleSetPublisherId::parse(&rule_set.publisher_id)
            .map_err(|error| ConfigFileError::RuleSet(error.to_string()))?;
        if rule_set.minimum_revision == 0 {
            return Err(ConfigFileError::RuleSet(format!(
                "rule set {expected_id} minimum_revision must be non-zero"
            )));
        }
        let path = if rule_set.file.is_absolute() {
            rule_set.file
        } else {
            material_base.join(rule_set.file)
        };
        let file = std::fs::File::open(&path).map_err(ConfigFileError::Io)?;
        let mut artifact = Vec::new();
        file.take((MAX_RULE_SET_ENVELOPE_BYTES + 1) as u64)
            .read_to_end(&mut artifact)
            .map_err(ConfigFileError::Io)?;
        if artifact.len() > MAX_RULE_SET_ENVELOPE_BYTES {
            return Err(ConfigFileError::RuleSet(format!(
                "rule set {expected_id} artifact exceeds {MAX_RULE_SET_ENVELOPE_BYTES} bytes"
            )));
        }
        let artifact = VerifiedRuleSet::verify_json(&artifact, &publishers, now_unix_secs)
            .map_err(|error| {
                ConfigFileError::RuleSet(format!(
                    "rule set {expected_id} failed verification: {error}"
                ))
            })?;
        if artifact.id() != &expected_id {
            return Err(ConfigFileError::RuleSet(format!(
                "rule set file for {expected_id} contains signed ID {}",
                artifact.id()
            )));
        }
        if artifact.publisher() != &expected_publisher {
            return Err(ConfigFileError::RuleSet(format!(
                "rule set {expected_id} is signed by {}, expected {expected_publisher}",
                artifact.publisher()
            )));
        }
        if artifact.revision() < rule_set.minimum_revision {
            return Err(ConfigFileError::RuleSet(format!(
                "rule set {expected_id} revision {} is below configured minimum {}",
                artifact.revision(),
                rule_set.minimum_revision
            )));
        }
        verified.push(artifact);
    }
    CompiledRuleSetRegistry::compile(verified)
        .map_err(|error| ConfigFileError::RuleSet(error.to_string()))
}

fn compile_product_policy(
    generation: u64,
    rules: Vec<RoutingRuleFileConfig>,
    rule_sets: &CompiledRuleSetRegistry,
    local_inbounds: &HashSet<String>,
    outbounds: &ParsedOutbounds,
) -> Result<Option<ProductPolicyConfig>, ConfigFileError> {
    if rules.is_empty() && local_inbounds.is_empty() {
        return Ok(None);
    }

    let rules = rules
        .into_iter()
        .map(|rule| compile_product_route_rule(rule, rule_sets, local_inbounds, outbounds))
        .collect::<Result<Vec<_>, _>>()?;
    let policy = ProductPolicyConfig {
        generation,
        routes: rules,
    };
    policy
        .compile()
        .map_err(|error| ConfigFileError::RoutingPolicy(error.to_string()))?;
    Ok(Some(policy))
}

fn compile_product_route_rule(
    rule: RoutingRuleFileConfig,
    rule_sets: &CompiledRuleSetRegistry,
    local_inbounds: &HashSet<String>,
    outbounds: &ParsedOutbounds,
) -> Result<RouteRuleSpec, ConfigFileError> {
    let name = canonical_config_name(&rule.name)?;
    let id =
        RuleId::parse(&name).map_err(|error| ConfigFileError::RoutingValue(error.to_string()))?;
    let inbounds = rule
        .inbounds
        .into_concrete("inbounds")?
        .into_iter()
        .map(|name| {
            let name = canonical_config_name(&name)?;
            let inbound = InboundId::parse(&name)
                .map_err(|error| ConfigFileError::RoutingValue(error.to_string()))?;
            if !local_inbounds.contains(inbound.as_str()) {
                return Err(ConfigFileError::RoutingRuleMissingInbound {
                    rule: id.as_str().to_string(),
                    inbound: name,
                });
            }
            Ok(inbound)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let matcher = RouteMatchSpec {
        domain_exact: parse_route_values(rule.domain_exact, DomainName::parse)?,
        domain_suffix: parse_route_values(rule.domain_suffix, DomainName::parse)?,
        domain_keyword: rule.domain_keyword,
        domain_regex: rule.domain_regex,
        domain_rule_sets: resolve_route_rule_sets(
            rule.domain_rule_set_ids,
            rule_sets,
            &id,
            "domain_rule_set_ids",
        )?,
        destination_cidrs: parse_route_values(rule.destination_cidrs, |value| {
            value.parse::<ipnet::IpNet>()
        })?,
        destination_rule_sets: resolve_route_rule_sets(
            rule.destination_rule_set_ids,
            rule_sets,
            &id,
            "destination_rule_set_ids",
        )?,
        source_cidrs: parse_route_values(rule.source_cidrs, |value| value.parse::<ipnet::IpNet>())?,
        destination_ports: rule
            .destination_ports
            .into_iter()
            .map(|value| parse_route_port(value, false))
            .collect::<Result<Vec<_>, _>>()?,
        source_ports: rule
            .source_ports
            .into_iter()
            .map(|value| parse_route_port(value, true))
            .collect::<Result<Vec<_>, _>>()?,
        networks: rule
            .networks
            .into_iter()
            .map(|network| match network {
                RoutingNetworkFileValue::Tcp => Network::Tcp,
                RoutingNetworkFileValue::Udp => Network::Udp,
            })
            .collect(),
        inbounds,
        principals: parse_route_values(
            rule.principal_ids.into_concrete("principal_ids")?,
            PrincipalId::parse,
        )?,
        stages: rule
            .stages
            .into_iter()
            .map(|stage| match stage {
                RoutingStageFileValue::PreResolution => RouteStage::PreResolution,
                RoutingStageFileValue::PostResolution => RouteStage::PostResolution,
            })
            .collect(),
    };
    // Selecting one egress is the ordinary allow form used by mature proxy
    // configurations. Only exceptional terminal or restricted behavior needs
    // an explicit decision.
    let decision = rule.decision.unwrap_or(RoutingDecisionFileValue::Allow);
    let action = match decision {
        decision
        @ (RoutingDecisionFileValue::Allow | RoutingDecisionFileValue::AllowRestricted) => {
            let egress = match (rule.outbound, rule.balancer) {
                (Some(name), None) => {
                    let name = canonical_config_name(&name)?;
                    if !outbounds.leaves.contains_key(&name) {
                        return Err(ConfigFileError::RoutingRuleMissingOutbound {
                            rule: id.as_str().to_string(),
                            outbound: name,
                        });
                    }
                    EgressAction::Outbound(
                        OutboundId::parse(&name)
                            .map_err(|error| ConfigFileError::RoutingValue(error.to_string()))?,
                    )
                }
                (None, Some(name)) => {
                    let name = canonical_config_name(&name)?;
                    if !outbounds.balancers.contains_key(&name) {
                        return Err(ConfigFileError::RoutingRuleMissingBalancer {
                            rule: id.as_str().to_string(),
                            balancer: name,
                        });
                    }
                    EgressAction::Balancer(
                        BalancerId::parse(&name)
                            .map_err(|error| ConfigFileError::RoutingValue(error.to_string()))?,
                    )
                }
                (None, None) | (Some(_), Some(_)) => {
                    return Err(ConfigFileError::RoutingPolicy(format!(
                        "routing rule {} with an allow decision requires exactly one of outbound or balancer",
                        id.as_str()
                    )));
                }
            };
            let dns_plan = rule
                .dns_policy
                .map(|plan| {
                    let plan = canonical_config_name(&plan)?;
                    crate::product::DnsPlanId::parse(&plan)
                        .map_err(|error| ConfigFileError::DnsValue(error.to_string()))
                })
                .transpose()?;
            let initial_demand = match rule
                .initial_demand
                .unwrap_or(RoutingInitialDemandFileValue::Automatic)
            {
                RoutingInitialDemandFileValue::Automatic => InitialDemand::Automatic,
                RoutingInitialDemandFileValue::Throughput => InitialDemand::Throughput,
            };
            match decision {
                RoutingDecisionFileValue::Allow => {
                    RouteAction::allow(egress, dns_plan, initial_demand)
                }
                RoutingDecisionFileValue::AllowRestricted => {
                    RouteAction::allow_restricted(egress, dns_plan, initial_demand)
                }
                RoutingDecisionFileValue::Reject | RoutingDecisionFileValue::Drop => {
                    unreachable!("allow decision pattern")
                }
            }
        }
        decision @ (RoutingDecisionFileValue::Reject | RoutingDecisionFileValue::Drop) => {
            if rule.outbound.is_some()
                || rule.balancer.is_some()
                || rule.dns_policy.is_some()
                || rule.initial_demand.is_some()
            {
                return Err(ConfigFileError::RoutingPolicy(format!(
                    "routing rule {} with a reject/drop decision forbids outbound, balancer, dns_policy, and initial_demand",
                    id.as_str()
                )));
            }
            match decision {
                RoutingDecisionFileValue::Reject => RouteAction::reject(),
                RoutingDecisionFileValue::Drop => RouteAction::drop(),
                RoutingDecisionFileValue::Allow | RoutingDecisionFileValue::AllowRestricted => {
                    unreachable!("terminal pattern")
                }
            }
        }
    };
    Ok(RouteRuleSpec {
        id,
        matcher,
        action,
        explanation: rule.explanation,
    })
}

fn resolve_route_rule_sets(
    values: Vec<String>,
    rule_sets: &CompiledRuleSetRegistry,
    rule_id: &RuleId,
    field: &'static str,
) -> Result<Vec<Arc<VerifiedRuleSet>>, ConfigFileError> {
    values
        .into_iter()
        .map(|value| {
            let id = RuleSetId::parse(&value)
                .map_err(|error| ConfigFileError::RuleSet(error.to_string()))?;
            rule_sets.resolve(&id).ok_or_else(|| {
                ConfigFileError::RuleSet(format!(
                    "routing rule {rule_id} field {field} references missing rule set {id}"
                ))
            })
        })
        .collect()
}

fn parse_route_values<T, E>(
    values: Vec<String>,
    parse: impl Fn(&str) -> Result<T, E>,
) -> Result<Vec<T>, ConfigFileError>
where
    E: std::fmt::Display,
{
    values
        .into_iter()
        .map(|value| {
            parse(&value).map_err(|error| ConfigFileError::RoutingValue(error.to_string()))
        })
        .collect()
}

fn parse_route_port(
    value: RoutingPortFileValue,
    allow_zero: bool,
) -> Result<PortRange, ConfigFileError> {
    let (start, end) = match value {
        RoutingPortFileValue::Single(port) => (port, port),
        RoutingPortFileValue::Range(value) => {
            let (start, end) = value.split_once('-').ok_or_else(|| {
                ConfigFileError::RoutingValue(format!(
                    "routing port range {value:?} must be START-END"
                ))
            })?;
            let start = start.parse::<u16>().map_err(|_| {
                ConfigFileError::RoutingValue(format!("invalid routing port {start:?}"))
            })?;
            let end = end.parse::<u16>().map_err(|_| {
                ConfigFileError::RoutingValue(format!("invalid routing port {end:?}"))
            })?;
            (start, end)
        }
    };
    if !allow_zero && start == 0 {
        return Err(ConfigFileError::RoutingValue(
            "destination routing ports must be non-zero".to_string(),
        ));
    }
    PortRange::new(start, end).map_err(|error| ConfigFileError::RoutingValue(error.to_string()))
}

type BuiltNodeServices = (
    Vec<OutboundLeafConfig>,
    Vec<GatewayBalancerConfig>,
    Vec<LocalIngressConfig>,
    Vec<NamedTunL3Config>,
    Vec<MppInboundConfig>,
);

fn build_node_services(
    inbounds: Vec<InboundFileConfig>,
    outbounds: ParsedOutbounds,
    material_base: &Path,
    credential_catalog: &CredentialCatalog,
    local_user_catalog: &LocalUserCatalog,
) -> Result<BuiltNodeServices, ConfigFileError> {
    let mut inbound_names = HashSet::new();
    let mut local_ingresses = Vec::new();
    let mut tun_l3_ingresses = Vec::new();
    let mut servers = Vec::new();

    for inbound in inbounds {
        match inbound {
            InboundFileConfig::Socks5 {
                name,
                listen,
                local_users,
                admission,
            } => {
                let name = canonical_config_name(&name)?;
                validate_unique_inbound_name(&name, &mut inbound_names)?;
                local_ingresses.push(LocalIngressConfig {
                    name,
                    config: IngressConfig::Socks5 {
                        listen: listen_or_default(listen, 1080),
                        proxy_auth: local_user_catalog.auth_for(local_users)?,
                        admission: admission.into_config()?,
                    },
                });
            }
            InboundFileConfig::HttpConnect {
                name,
                listen,
                local_users,
                admission,
            } => {
                let name = canonical_config_name(&name)?;
                validate_unique_inbound_name(&name, &mut inbound_names)?;
                local_ingresses.push(LocalIngressConfig {
                    name,
                    config: IngressConfig::HttpConnect {
                        listen: listen_or_default(listen, 8080),
                        proxy_auth: local_user_catalog.auth_for(local_users)?,
                        admission: admission.into_config()?,
                    },
                });
            }
            InboundFileConfig::Mixed {
                name,
                listen,
                local_users,
                admission,
            } => {
                let name = canonical_config_name(&name)?;
                validate_unique_inbound_name(&name, &mut inbound_names)?;
                local_ingresses.push(LocalIngressConfig {
                    name,
                    config: IngressConfig::Mixed {
                        listen: listen_or_default(listen, 1080),
                        proxy_auth: local_user_catalog.auth_for(local_users)?,
                        admission: admission.into_config()?,
                    },
                });
            }
            InboundFileConfig::TcpForward {
                name,
                listen,
                target,
                max_connections,
            } => {
                let name = canonical_config_name(&name)?;
                validate_unique_inbound_name(&name, &mut inbound_names)?;
                let target = PortForwardTarget::parse(&target)
                    .map_err(|error| ConfigFileError::PortForward(error.to_string()))?;
                let max_connections = max_connections
                    .map(|limit| limit as usize)
                    .unwrap_or(DEFAULT_TCP_FORWARD_MAX_CONNECTIONS);
                let config = TcpForwardConfig::new(listen, target, max_connections)
                    .map_err(|error| ConfigFileError::PortForward(error.to_string()))?;
                local_ingresses.push(LocalIngressConfig {
                    name,
                    config: IngressConfig::TcpForward(config),
                });
            }
            InboundFileConfig::UdpForward {
                name,
                listen,
                target,
                max_associations,
                idle_timeout_ms,
                datagram_ttl_ms,
            } => {
                let name = canonical_config_name(&name)?;
                validate_unique_inbound_name(&name, &mut inbound_names)?;
                let target = PortForwardTarget::parse(&target)
                    .map_err(|error| ConfigFileError::PortForward(error.to_string()))?;
                let max_associations = max_associations
                    .map(|limit| limit as usize)
                    .unwrap_or(DEFAULT_UDP_FORWARD_MAX_ASSOCIATIONS);
                let idle_timeout = Duration::from_millis(
                    idle_timeout_ms.unwrap_or(DEFAULT_UDP_FORWARD_IDLE_TIMEOUT_MS),
                );
                let datagram_ttl = Duration::from_millis(
                    datagram_ttl_ms.unwrap_or(DEFAULT_UDP_FORWARD_DATAGRAM_TTL_MS),
                );
                let config = UdpForwardConfig::new(
                    listen,
                    target,
                    max_associations,
                    idle_timeout,
                    datagram_ttl,
                )
                .map_err(|error| ConfigFileError::PortForward(error.to_string()))?;
                local_ingresses.push(LocalIngressConfig {
                    name,
                    config: IngressConfig::UdpForward(config),
                });
            }
            InboundFileConfig::MixedForward {
                name,
                listen,
                target,
                max_connections,
                max_associations,
                idle_timeout_ms,
                datagram_ttl_ms,
            } => {
                let name = canonical_config_name(&name)?;
                validate_unique_inbound_name(&name, &mut inbound_names)?;
                let target = PortForwardTarget::parse(&target)
                    .map_err(|error| ConfigFileError::PortForward(error.to_string()))?;
                let max_connections = max_connections
                    .map(|limit| limit as usize)
                    .unwrap_or(DEFAULT_TCP_FORWARD_MAX_CONNECTIONS);
                let max_associations = max_associations
                    .map(|limit| limit as usize)
                    .unwrap_or(DEFAULT_UDP_FORWARD_MAX_ASSOCIATIONS);
                let idle_timeout = Duration::from_millis(
                    idle_timeout_ms.unwrap_or(DEFAULT_UDP_FORWARD_IDLE_TIMEOUT_MS),
                );
                let datagram_ttl = Duration::from_millis(
                    datagram_ttl_ms.unwrap_or(DEFAULT_UDP_FORWARD_DATAGRAM_TTL_MS),
                );
                let config = MixedForwardConfig::new(
                    listen,
                    target,
                    max_connections,
                    max_associations,
                    idle_timeout,
                    datagram_ttl,
                )
                .map_err(|error| ConfigFileError::PortForward(error.to_string()))?;
                local_ingresses.push(LocalIngressConfig {
                    name,
                    config: IngressConfig::MixedForward(config),
                });
            }
            InboundFileConfig::Tun {
                name,
                interface_name,
                ipv4,
                disable_ipv4,
                ipv4_prefix,
                ipv4_gateway,
                ipv6,
                ipv6_prefix,
                mtu,
                disable_icmp,
                dns_redirects,
                dns_ttl_ms,
                host,
            } => {
                let name = canonical_config_name(&name)?;
                validate_unique_inbound_name(&name, &mut inbound_names)?;
                local_ingresses.push(LocalIngressConfig {
                    name,
                    config: IngressConfig::TunL4(
                        TunFileConfig {
                            interface_name,
                            ipv4,
                            disable_ipv4,
                            ipv4_prefix,
                            ipv4_gateway,
                            ipv6,
                            ipv6_prefix,
                            mtu,
                            disable_icmp,
                            dns_redirects,
                            dns_ttl_ms,
                            host,
                        }
                        .into_config()?,
                    ),
                });
            }
            InboundFileConfig::TunL3 {
                name,
                outbound,
                interface_name,
            } => {
                let name = canonical_config_name(&name)?;
                validate_unique_inbound_name(&name, &mut inbound_names)?;
                let outbound = canonical_config_name(&outbound)?;
                if outbounds.explicit_mpp_performance.contains(&outbound) {
                    return Err(ConfigFileError::TunL3OutboundPerformance(outbound));
                }
                let outbound = OutboundId::parse(&outbound)
                    .map_err(|error| ConfigFileError::TunL3(error.to_string()))?;
                tun_l3_ingresses.push(NamedTunL3Config {
                    name,
                    config: TunL3IngressConfig {
                        outbound,
                        interface_name,
                    },
                });
            }
            InboundFileConfig::Mpp {
                name,
                security,
                performance,
                paths,
                peer_diagnostics_principal_ids,
            } => {
                let name = canonical_config_name(&name)?;
                validate_unique_inbound_name(&name, &mut inbound_names)?;
                let paths = parse_named_path_specs(paths)?;
                if paths.is_empty() {
                    return Err(ConfigFileError::MppInboundRequiresPath);
                }
                let server_security = security.server_auth_config(credential_catalog)?;
                let tls = security.server_tls(material_base, &server_security)?;
                let peer_diagnostics_principals = peer_diagnostics_principal_ids
                    .into_policy(&name, &server_security.credential_authority)?;
                servers.push(MppInboundConfig {
                    name,
                    paths,
                    security: server_security,
                    tls,
                    performance: performance.into_config(),
                    peer_diagnostics_principals,
                    tun_l3: None,
                });
            }
            InboundFileConfig::MppL3 {
                name,
                security,
                paths,
                peer_diagnostics_principal_ids,
                tun_l3,
            } => {
                let name = canonical_config_name(&name)?;
                validate_unique_inbound_name(&name, &mut inbound_names)?;
                let paths = parse_named_path_specs(paths)?;
                if paths.is_empty() {
                    return Err(ConfigFileError::MppInboundRequiresPath);
                }
                let server_security = security.server_auth_config(credential_catalog)?;
                let tls = security.server_tls(material_base, &server_security)?;
                let peer_diagnostics_principals = peer_diagnostics_principal_ids
                    .into_policy(&name, &server_security.credential_authority)?;
                let tun_l3 = tun_l3.into_plan(&server_security.credential_authority)?;
                servers.push(MppInboundConfig {
                    name,
                    paths,
                    security: server_security,
                    tls,
                    performance: MppPerformanceConfig::default(),
                    peer_diagnostics_principals,
                    tun_l3: Some(tun_l3),
                });
            }
        }
    }

    let leaves = outbounds
        .order
        .iter()
        .map(|name| {
            outbounds
                .leaves
                .get(name)
                .cloned()
                .ok_or_else(|| ConfigFileError::MissingOutboundName(name.clone()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let gateway_balancers = outbounds
        .balancer_order
        .iter()
        .map(|name| {
            outbounds
                .balancers
                .get(name)
                .cloned()
                .ok_or_else(|| ConfigFileError::MissingBalancerName(name.clone()))
        })
        .collect::<Result<Vec<_>, _>>()?;

    if local_ingresses.is_empty() && tun_l3_ingresses.is_empty() && servers.is_empty() {
        return Err(ConfigFileError::NoRuntimeServices);
    }
    Ok((
        leaves,
        gateway_balancers,
        local_ingresses,
        tun_l3_ingresses,
        servers,
    ))
}

fn validate_unique_inbound_name(
    name: &str,
    seen: &mut HashSet<String>,
) -> Result<(), ConfigFileError> {
    if !seen.insert(name.to_string()) {
        return Err(ConfigFileError::DuplicateInboundName(name.to_string()));
    }
    Ok(())
}

fn outbound_connect_timeout(value: Option<u64>) -> Duration {
    Duration::from_millis(value.unwrap_or(DEFAULT_OUTBOUND_CONNECT_TIMEOUT_MS))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DnsFileConfig {
    #[serde(default = "default_dns_servers")]
    servers: Vec<DnsServerFileConfig>,
    #[serde(default = "default_dns_policies")]
    policies: Vec<DnsPolicyFileConfig>,
    #[serde(default)]
    rules: Vec<DnsRuleFileConfig>,
    #[serde(default)]
    override_records: Vec<DnsOverrideRecordFileConfig>,
    #[serde(default)]
    synthetic_capture: Vec<DnsSyntheticCaptureFileConfig>,
    #[serde(default = "default_dns_policy_name")]
    default: String,
}

impl Default for DnsFileConfig {
    fn default() -> Self {
        Self {
            servers: default_dns_servers(),
            policies: default_dns_policies(),
            rules: Vec::new(),
            override_records: Vec::new(),
            synthetic_capture: Vec::new(),
            default: default_dns_policy_name(),
        }
    }
}

fn default_dns_policy_name() -> String {
    "default".to_string()
}

fn default_dns_servers() -> Vec<DnsServerFileConfig> {
    vec![DnsServerFileConfig {
        name: "system".to_string(),
        protocol: DnsProtocolFileValue::System,
        address: None,
        tls_name: None,
        path: None,
        outbound: None,
    }]
}

fn default_dns_policies() -> Vec<DnsPolicyFileConfig> {
    vec![DnsPolicyFileConfig {
        name: default_dns_policy_name(),
        servers: vec!["system".to_string()],
        family: DnsFamilyFileValue::default(),
        security: DnsSecurityFileValue::default(),
        strategy: DnsServerStrategyFileValue::default(),
        fallback_ms: None,
        answer_cidrs: Vec::new(),
        query: DnsQueryFileConfig::default(),
        cache: DnsCacheFileConfig::default(),
        override_records: Vec::new(),
        synthetic_capture: None,
    }]
}

impl DnsFileConfig {
    fn into_config(
        self,
        generation: u64,
        outbounds: &ParsedOutbounds,
    ) -> Result<DnsPolicyConfig, ConfigFileError> {
        let upstreams = self
            .servers
            .into_iter()
            .map(DnsServerFileConfig::into_spec)
            .collect::<Result<Vec<_>, _>>()?;
        let plans = self
            .policies
            .into_iter()
            .map(DnsPolicyFileConfig::into_spec)
            .collect::<Result<Vec<_>, _>>()?;
        let rules = self
            .rules
            .into_iter()
            .map(DnsRuleFileConfig::into_spec)
            .collect::<Result<Vec<_>, _>>()?;
        let override_records = self
            .override_records
            .into_iter()
            .map(DnsOverrideRecordFileConfig::into_spec)
            .collect::<Result<Vec<_>, _>>()?;
        let synthetic_captures = self
            .synthetic_capture
            .into_iter()
            .map(DnsSyntheticCaptureFileConfig::into_spec)
            .collect::<Result<Vec<_>, _>>()?;
        let mut outbound_capabilities = outbounds
            .leaves
            .values()
            .map(|leaf| match leaf {
                OutboundLeafConfig::Local { id, config, .. } => match config {
                    OutboundConfig::Direct
                    | OutboundConfig::BindSourceIp(_)
                    | OutboundConfig::BindSourceIps { .. } => {
                        DnsOutboundCapabilitySpec::new(id.clone(), leaf.networks(), true)
                    }
                    OutboundConfig::Socks5(_)
                    | OutboundConfig::HttpConnect(_)
                    | OutboundConfig::HttpsConnect(_) => {
                        let dns_independent = config
                            .native_proxy_endpoint()
                            .is_some_and(|endpoint| endpoint.host.parse::<IpAddr>().is_ok());
                        // Routed DNS currently has a TCP stream connector for
                        // proxy leaves. Do not over-advertise their unrelated
                        // UDP target capability.
                        DnsOutboundCapabilitySpec::new(
                            id.clone(),
                            crate::product::NetworkSet::TCP,
                            dns_independent,
                        )
                    }
                },
                OutboundLeafConfig::Mpp { id, config } => {
                    let dns_independent = config
                        .paths
                        .iter()
                        .all(|path| path.spec.endpoint.host.parse::<IpAddr>().is_ok());
                    DnsOutboundCapabilitySpec::new(
                        id.clone(),
                        crate::product::NetworkSet::TCP,
                        dns_independent,
                    )
                }
            })
            .collect::<Vec<_>>();
        outbound_capabilities
            .sort_by(|left, right| left.outbound.as_str().cmp(right.outbound.as_str()));
        let spec = DnsPolicySpec {
            upstreams,
            outbound_capabilities,
            plans,
            rules,
            override_records,
            synthetic_captures,
            default_plan: {
                let name = canonical_config_name(&self.default)?;
                crate::product::DnsPlanId::parse(&name)
                    .map_err(|error| ConfigFileError::DnsValue(error.to_string()))?
            },
        };
        let config = DnsPolicyConfig { generation, spec };
        config
            .compile()
            .map_err(|error| ConfigFileError::DnsPolicy(error.to_string()))?;
        Ok(config)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DnsSyntheticCaptureFileConfig {
    name: String,
    ipv4_pool: Option<ipnet::Ipv4Net>,
    ipv6_pool: Option<ipnet::Ipv6Net>,
    capacity: usize,
    answer_ttl_seconds: u64,
    recovery_ttl_seconds: u64,
}

impl DnsSyntheticCaptureFileConfig {
    fn into_spec(self) -> Result<DnsSyntheticCaptureSpec, ConfigFileError> {
        let name = canonical_config_name(&self.name)?;
        Ok(DnsSyntheticCaptureSpec {
            id: DnsSyntheticCaptureId::parse(&name)
                .map_err(|error| ConfigFileError::DnsValue(error.to_string()))?,
            ipv4_pool: self.ipv4_pool,
            ipv6_pool: self.ipv6_pool,
            max_entries: self.capacity,
            answer_ttl: Duration::from_secs(self.answer_ttl_seconds),
            recovery_ttl: Duration::from_secs(self.recovery_ttl_seconds),
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DnsServerFileConfig {
    name: String,
    protocol: DnsProtocolFileValue,
    address: Option<SocketAddr>,
    tls_name: Option<String>,
    path: Option<String>,
    outbound: Option<String>,
}

impl DnsServerFileConfig {
    fn into_spec(self) -> Result<DnsUpstreamSpec, ConfigFileError> {
        let Self {
            name,
            protocol,
            address,
            tls_name,
            path,
            outbound,
        } = self;
        let name = canonical_config_name(&name)?;
        let id = DnsUpstreamId::parse(&name)
            .map_err(|error| ConfigFileError::DnsValue(error.to_string()))?;
        if matches!(
            protocol,
            DnsProtocolFileValue::Udp | DnsProtocolFileValue::Tcp | DnsProtocolFileValue::UdpTcp
        ) && (tls_name.is_some() || path.is_some())
        {
            return Err(ConfigFileError::DnsValue(format!(
                "DNS server {} protocol does not accept a TLS name or HTTP path",
                id.as_str()
            )));
        }
        if matches!(
            protocol,
            DnsProtocolFileValue::Dot | DnsProtocolFileValue::Doq
        ) && path.is_some()
        {
            return Err(ConfigFileError::DnsValue(format!(
                "DNS server {} protocol does not accept an HTTP path",
                id.as_str()
            )));
        }
        let endpoint = match protocol {
            DnsProtocolFileValue::System => {
                if address.is_some() || tls_name.is_some() || path.is_some() || outbound.is_some() {
                    return Err(ConfigFileError::DnsValue(
                        "system DNS server cannot set address, tls_name, path, or outbound"
                            .to_string(),
                    ));
                }
                DnsUpstreamEndpoint::System
            }
            DnsProtocolFileValue::Udp => DnsUpstreamEndpoint::Udp {
                bootstrap: required_dns_server_address(address, &id)?,
            },
            DnsProtocolFileValue::Tcp => DnsUpstreamEndpoint::Tcp {
                bootstrap: required_dns_server_address(address, &id)?,
            },
            DnsProtocolFileValue::UdpTcp => DnsUpstreamEndpoint::UdpTcp {
                bootstrap: required_dns_server_address(address, &id)?,
            },
            DnsProtocolFileValue::Dot => DnsUpstreamEndpoint::Tls {
                bootstrap: required_dns_server_address(address, &id)?,
                server_name: required_dns_tls_name(tls_name, &id)?,
            },
            DnsProtocolFileValue::Doh => DnsUpstreamEndpoint::Https {
                bootstrap: required_dns_server_address(address, &id)?,
                server_name: required_dns_tls_name(tls_name, &id)?,
                path: path.ok_or_else(|| {
                    ConfigFileError::DnsValue(format!("DoH server {} requires path", id.as_str()))
                })?,
            },
            DnsProtocolFileValue::Doq => DnsUpstreamEndpoint::Quic {
                bootstrap: required_dns_server_address(address, &id)?,
                server_name: required_dns_tls_name(tls_name, &id)?,
            },
        };
        let egress = outbound
            .map(|outbound| {
                let outbound = canonical_config_name(&outbound)?;
                OutboundId::parse(&outbound)
                    .map(DnsEgressSpec::Outbound)
                    .map_err(|error| ConfigFileError::DnsValue(error.to_string()))
            })
            .transpose()?
            .unwrap_or(DnsEgressSpec::Direct);
        Ok(DnsUpstreamSpec {
            id,
            endpoint,
            egress,
        })
    }
}

fn required_dns_server_address(
    address: Option<SocketAddr>,
    id: &DnsUpstreamId,
) -> Result<SocketAddr, ConfigFileError> {
    address.ok_or_else(|| {
        ConfigFileError::DnsValue(format!(
            "DNS server {} requires a literal IP socket address",
            id.as_str()
        ))
    })
}

fn required_dns_tls_name(
    tls_name: Option<String>,
    id: &DnsUpstreamId,
) -> Result<DomainName, ConfigFileError> {
    let tls_name = tls_name.ok_or_else(|| {
        ConfigFileError::DnsValue(format!(
            "encrypted DNS server {} requires tls_name",
            id.as_str()
        ))
    })?;
    DomainName::parse(&tls_name).map_err(|error| ConfigFileError::DnsValue(error.to_string()))
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum DnsProtocolFileValue {
    System,
    Udp,
    Tcp,
    UdpTcp,
    Dot,
    Doh,
    Doq,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DnsPolicyFileConfig {
    name: String,
    servers: Vec<String>,
    #[serde(default)]
    family: DnsFamilyFileValue,
    #[serde(default)]
    security: DnsSecurityFileValue,
    #[serde(default)]
    strategy: DnsServerStrategyFileValue,
    fallback_ms: Option<u64>,
    #[serde(default)]
    answer_cidrs: Vec<String>,
    #[serde(default)]
    query: DnsQueryFileConfig,
    #[serde(default)]
    cache: DnsCacheFileConfig,
    #[serde(default)]
    override_records: Vec<String>,
    synthetic_capture: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct DnsQueryFileConfig {
    timeout_ms: Option<u64>,
    inflight: Option<usize>,
    answers: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct DnsCacheFileConfig {
    entries: Option<usize>,
    positive_ttl_ms: Option<u64>,
    negative_ttl_ms: Option<u64>,
    stale_ms: Option<u64>,
    prefetch_ms: Option<u64>,
}

impl DnsPolicyFileConfig {
    fn into_spec(self) -> Result<DnsPlanSpec, ConfigFileError> {
        let name = canonical_config_name(&self.name)?;
        let id = crate::product::DnsPlanId::parse(&name)
            .map_err(|error| ConfigFileError::DnsValue(error.to_string()))?;
        let defaults = DnsPlanLimits::default();
        let upstream_strategy = match (self.strategy, self.fallback_ms) {
            (DnsServerStrategyFileValue::Ordered, None) => DnsUpstreamStrategy::Ordered,
            (DnsServerStrategyFileValue::Ordered, Some(_)) => {
                return Err(ConfigFileError::DnsValue(format!(
                    "ordered DNS policy {} cannot set fallback_ms",
                    id.as_str()
                )));
            }
            (DnsServerStrategyFileValue::Race, Some(delay_ms)) => DnsUpstreamStrategy::Race {
                fallback_delay: Duration::from_millis(delay_ms),
            },
            (DnsServerStrategyFileValue::Race, None) => {
                return Err(ConfigFileError::DnsValue(format!(
                    "racing DNS policy {} requires fallback_ms",
                    id.as_str()
                )));
            }
        };
        Ok(DnsPlanSpec {
            id: id.clone(),
            upstreams: self
                .servers
                .into_iter()
                .map(|value| {
                    let value = canonical_config_name(&value)?;
                    DnsUpstreamId::parse(&value)
                        .map_err(|error| ConfigFileError::DnsValue(error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?,
            ip_strategy: self.family.into(),
            security: self.security.into(),
            upstream_strategy,
            expected_cidrs: self
                .answer_cidrs
                .into_iter()
                .map(|value| {
                    value.parse::<ipnet::IpNet>().map_err(|error| {
                        ConfigFileError::DnsValue(format!(
                            "DNS policy {} answer CIDR {value:?} is invalid: {error}",
                            id.as_str()
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
            override_records: self
                .override_records
                .into_iter()
                .map(|value| {
                    let value = canonical_config_name(&value)?;
                    DnsOverrideRecordId::parse(&value)
                        .map_err(|error| ConfigFileError::DnsValue(error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?,
            synthetic_capture: self
                .synthetic_capture
                .map(|value| {
                    let value = canonical_config_name(&value)?;
                    DnsSyntheticCaptureId::parse(&value)
                        .map_err(|error| ConfigFileError::DnsValue(error.to_string()))
                })
                .transpose()?,
            limits: DnsPlanLimits {
                lookup_timeout: Duration::from_millis(
                    self.query
                        .timeout_ms
                        .unwrap_or(defaults.lookup_timeout.as_millis() as u64),
                ),
                cache_capacity: self.cache.entries.unwrap_or(defaults.cache_capacity),
                max_inflight: self.query.inflight.unwrap_or(defaults.max_inflight),
                max_answers: self.query.answers.unwrap_or(defaults.max_answers),
                positive_ttl_cap: Duration::from_millis(
                    self.cache
                        .positive_ttl_ms
                        .unwrap_or(defaults.positive_ttl_cap.as_millis() as u64),
                ),
                negative_ttl_cap: Duration::from_millis(
                    self.cache
                        .negative_ttl_ms
                        .unwrap_or(defaults.negative_ttl_cap.as_millis() as u64),
                ),
                stale_if_error: Duration::from_millis(
                    self.cache
                        .stale_ms
                        .unwrap_or(defaults.stale_if_error.as_millis() as u64),
                ),
                prefetch_max: Duration::from_millis(
                    self.cache
                        .prefetch_ms
                        .unwrap_or(defaults.prefetch_max.as_millis() as u64),
                ),
            },
        })
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum DnsServerStrategyFileValue {
    #[default]
    Ordered,
    Race,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DnsOverrideRecordFileConfig {
    name: String,
    domain: String,
    addresses: Vec<IpAddr>,
}

impl DnsOverrideRecordFileConfig {
    fn into_spec(self) -> Result<DnsOverrideRecordSpec, ConfigFileError> {
        let name = canonical_config_name(&self.name)?;
        Ok(DnsOverrideRecordSpec {
            id: DnsOverrideRecordId::parse(&name)
                .map_err(|error| ConfigFileError::DnsValue(error.to_string()))?,
            domain: DomainName::parse(&self.domain)
                .map_err(|error| ConfigFileError::DnsValue(error.to_string()))?,
            addresses: self.addresses,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DnsRuleFileConfig {
    name: String,
    exact: Option<String>,
    suffix: Option<String>,
    policy: String,
    explanation: Option<String>,
}

impl DnsRuleFileConfig {
    fn into_spec(self) -> Result<DnsRuleSpec, ConfigFileError> {
        let name = canonical_config_name(&self.name)?;
        let dns_policy = canonical_config_name(&self.policy)?;
        let matcher = match (self.exact, self.suffix) {
            (Some(exact), None) => DnsRuleMatch::Exact(
                DomainName::parse(&exact)
                    .map_err(|error| ConfigFileError::DnsValue(error.to_string()))?,
            ),
            (None, Some(suffix)) => DnsRuleMatch::Suffix(
                DomainName::parse(&suffix)
                    .map_err(|error| ConfigFileError::DnsValue(error.to_string()))?,
            ),
            _ => {
                return Err(ConfigFileError::DnsValue(
                    "DNS rule requires exactly one of exact or suffix".to_string(),
                ));
            }
        };
        Ok(DnsRuleSpec {
            id: DnsRuleId::parse(&name)
                .map_err(|error| ConfigFileError::DnsValue(error.to_string()))?,
            matcher,
            plan: crate::product::DnsPlanId::parse(&dns_policy)
                .map_err(|error| ConfigFileError::DnsValue(error.to_string()))?,
            explanation: self.explanation,
        })
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum DnsFamilyFileValue {
    Ipv4ThenIpv6,
    Ipv6ThenIpv4,
    Ipv4Only,
    Ipv6Only,
    #[default]
    Ipv4AndIpv6,
    Ipv6AndIpv4,
}

impl From<DnsFamilyFileValue> for DnsIpStrategy {
    fn from(value: DnsFamilyFileValue) -> Self {
        match value {
            DnsFamilyFileValue::Ipv4ThenIpv6 => Self::Ipv4ThenIpv6,
            DnsFamilyFileValue::Ipv6ThenIpv4 => Self::Ipv6ThenIpv4,
            DnsFamilyFileValue::Ipv4Only => Self::Ipv4Only,
            DnsFamilyFileValue::Ipv6Only => Self::Ipv6Only,
            DnsFamilyFileValue::Ipv4AndIpv6 => Self::Ipv4AndIpv6,
            DnsFamilyFileValue::Ipv6AndIpv4 => Self::Ipv6AndIpv4,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum DnsSecurityFileValue {
    #[default]
    AllowPlaintext,
    RequireEncrypted,
}

impl From<DnsSecurityFileValue> for DnsSecurityPolicy {
    fn from(value: DnsSecurityFileValue) -> Self {
        match value {
            DnsSecurityFileValue::AllowPlaintext => Self::AllowPlaintext,
            DnsSecurityFileValue::RequireEncrypted => Self::RequireEncrypted,
        }
    }
}

#[derive(Debug)]
pub enum ConfigFileError {
    Io(std::io::Error),
    Toml(TomlConfigError),
    Config(ConfigError),
    Security(SecurityPolicyError),
    Credential(String),
    LocalUser(String),
    ProxyAuth(ProxyAuthConfigError),
    LocalAdmission(LocalIngressAdmissionConfigError),
    MaterialSource(MaterialSourceError),
    Endpoint(EndpointParseError),
    Outbound(OutboundError),
    PathSpec(PathSpecParseError),
    NoRuntimeServices,
    GenerationExhausted,
    MixedForwardingFamilies,
    EmptyName,
    NonCanonicalName(String),
    DuplicateInboundName(String),
    DuplicateOutboundName(String),
    DuplicateBalancerName(String),
    MissingOutboundName(String),
    MissingBalancerName(String),
    MppInboundRequiresPath,
    MppOutboundRequiresPath(String),
    L4RoutingSectionRequired,
    L3RoutingSectionForbidden,
    L3AdmissionField(&'static str),
    TunL3OutboundPerformance(String),
    RoutingBalancerRequiresMembers(String),
    RoutingPolicy(String),
    RoutingValue(String),
    RuleSet(String),
    DnsPolicy(String),
    DnsValue(String),
    DirectBindFieldConflict,
    RoutingRuleMissingInbound { rule: String, inbound: String },
    RoutingRuleMissingOutbound { rule: String, outbound: String },
    RoutingRuleMissingBalancer { rule: String, balancer: String },
    MissingOutboundEndpoint,
    TunIpv4DisabledWithIpv4Options,
    ManagedVpnValue(String),
    TunL3(String),
    PeerDiagnostics(String),
    ProxyUsernameRequired,
    ProxyPasswordRequired,
    PortForward(String),
    MppTlsFieldRequired(&'static str),
    MppTlsRoleMismatch(&'static str),
    MppTlsMaterial(String),
    MppTransportSecret(String),
}

impl From<ConfigError> for ConfigFileError {
    fn from(value: ConfigError) -> Self {
        Self::Config(value)
    }
}

impl From<SecurityPolicyError> for ConfigFileError {
    fn from(value: SecurityPolicyError) -> Self {
        Self::Security(value)
    }
}

impl std::fmt::Display for ConfigFileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Toml(err) => write!(f, "{err}"),
            Self::Config(err) => write!(f, "{err}"),
            Self::Security(err) => write!(f, "{err}"),
            Self::Credential(error) => write!(f, "invalid credential catalog: {error}"),
            Self::LocalUser(error) => write!(f, "invalid local user catalog: {error}"),
            Self::ProxyAuth(error) => write!(f, "invalid local proxy authentication: {error}"),
            Self::LocalAdmission(error) => write!(f, "invalid local proxy admission: {error}"),
            Self::MaterialSource(error) => write!(f, "{error}"),
            Self::Endpoint(err) => write!(f, "{err}"),
            Self::Outbound(err) => write!(f, "{err}"),
            Self::PathSpec(err) => write!(f, "{err}"),
            Self::NoRuntimeServices => {
                write!(f, "config must define at least one [[inbounds]] entry")
            }
            Self::GenerationExhausted => {
                write!(f, "runtime configuration generation space is exhausted")
            }
            Self::MixedForwardingFamilies => write!(
                f,
                "one config cannot mix L4 inbounds with tun-l3 or mpp-l3 inbounds"
            ),
            Self::EmptyName => write!(f, "configured resource names must not be empty"),
            Self::NonCanonicalName(name) => write!(
                f,
                "configured resource name {name:?} must be canonical lowercase ASCII"
            ),
            Self::DuplicateInboundName(name) => {
                write!(f, "duplicate inbound name {name:?}")
            }
            Self::DuplicateOutboundName(name) => {
                write!(f, "duplicate outbound name {name:?}")
            }
            Self::DuplicateBalancerName(name) => {
                write!(f, "duplicate balancer name {name:?}")
            }
            Self::MissingOutboundName(name) => {
                write!(f, "outbound name {name:?} does not exist")
            }
            Self::MissingBalancerName(name) => {
                write!(f, "balancer name {name:?} does not exist")
            }
            Self::MppInboundRequiresPath => {
                write!(f, "MPP inbound requires at least one named path")
            }
            Self::MppOutboundRequiresPath(name) => {
                write!(f, "MPP outbound {name:?} requires at least one named path")
            }
            Self::L4RoutingSectionRequired => {
                write!(f, "L4 inbounds require an explicit [routing] section")
            }
            Self::L3RoutingSectionForbidden => {
                write!(
                    f,
                    "tun-l3 and mpp-l3 inbounds forbid the [routing] section, even when empty"
                )
            }
            Self::L3AdmissionField(field) => write!(
                f,
                "tun-l3 and mpp-l3 accept only admission.max_dns_work; admission.{field} is L4-only"
            ),
            Self::TunL3OutboundPerformance(name) => write!(
                f,
                "TUN-L3 outbound {name:?} must not configure the L4 MPP performance table"
            ),
            Self::RoutingBalancerRequiresMembers(name) => {
                write!(
                    f,
                    "routing balancer {name:?} requires at least one outbound member"
                )
            }
            Self::RoutingPolicy(error) => write!(f, "{error}"),
            Self::RoutingValue(error) => write!(f, "invalid routing value: {error}"),
            Self::RuleSet(error) => write!(f, "invalid routing rule set: {error}"),
            Self::DnsPolicy(error) => write!(f, "invalid DNS policy: {error}"),
            Self::DnsValue(error) => write!(f, "invalid DNS value: {error}"),
            Self::DirectBindFieldConflict => write!(
                f,
                "direct outbound bind_ip cannot be combined with bind_ipv4 or bind_ipv6"
            ),
            Self::RoutingRuleMissingInbound { rule, inbound } => write!(
                f,
                "routing rule {rule:?} references missing local inbound {inbound:?}"
            ),
            Self::RoutingRuleMissingOutbound { rule, outbound } => write!(
                f,
                "routing rule {rule:?} references missing MPP outbound {outbound:?}"
            ),
            Self::RoutingRuleMissingBalancer { rule, balancer } => write!(
                f,
                "routing rule {rule:?} references missing MPP balancer {balancer:?}"
            ),
            Self::MissingOutboundEndpoint => {
                write!(f, "proxied outbound requires endpoint")
            }
            Self::TunIpv4DisabledWithIpv4Options => {
                write!(
                    f,
                    "TUN disable_ipv4 cannot be combined with ipv4 or ipv4_gateway"
                )
            }
            Self::ManagedVpnValue(error) => {
                write!(f, "invalid managed VPN configuration: {error}")
            }
            Self::TunL3(error) => write!(f, "invalid TUN-L3 configuration: {error}"),
            Self::PeerDiagnostics(error) => {
                write!(f, "invalid peer diagnostics configuration: {error}")
            }
            Self::ProxyUsernameRequired => write!(f, "proxy auth password requires username"),
            Self::ProxyPasswordRequired => write!(f, "proxy auth username requires password"),
            Self::PortForward(error) => write!(f, "invalid port-forward inbound: {error}"),
            Self::MppTlsFieldRequired(field) => {
                write!(f, "MPP TLS security requires {field}")
            }
            Self::MppTlsRoleMismatch(message) => write!(f, "{message}"),
            Self::MppTlsMaterial(message) => write!(f, "{message}"),
            Self::MppTransportSecret(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for ConfigFileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::Toml(_) => None,
            Self::Config(err) => Some(err),
            Self::Security(err) => Some(err),
            Self::Credential(_) | Self::LocalUser(_) => None,
            Self::ProxyAuth(error) => Some(error),
            Self::LocalAdmission(error) => Some(error),
            Self::MaterialSource(error) => Some(error),
            Self::Endpoint(err) => Some(err),
            Self::Outbound(err) => Some(err),
            Self::PathSpec(err) => Some(err),
            Self::NoRuntimeServices
            | Self::GenerationExhausted
            | Self::MixedForwardingFamilies
            | Self::EmptyName
            | Self::NonCanonicalName(_)
            | Self::DuplicateInboundName(_)
            | Self::DuplicateOutboundName(_)
            | Self::DuplicateBalancerName(_)
            | Self::MissingOutboundName(_)
            | Self::MissingBalancerName(_)
            | Self::MppInboundRequiresPath
            | Self::MppOutboundRequiresPath(_)
            | Self::L4RoutingSectionRequired
            | Self::L3RoutingSectionForbidden
            | Self::L3AdmissionField(_)
            | Self::TunL3OutboundPerformance(_)
            | Self::RoutingBalancerRequiresMembers(_)
            | Self::RoutingPolicy(_)
            | Self::RoutingValue(_)
            | Self::RuleSet(_)
            | Self::DnsPolicy(_)
            | Self::DnsValue(_)
            | Self::DirectBindFieldConflict
            | Self::RoutingRuleMissingInbound { .. }
            | Self::RoutingRuleMissingOutbound { .. }
            | Self::RoutingRuleMissingBalancer { .. }
            | Self::MissingOutboundEndpoint
            | Self::TunIpv4DisabledWithIpv4Options
            | Self::ManagedVpnValue(_)
            | Self::TunL3(_)
            | Self::PeerDiagnostics(_)
            | Self::ProxyUsernameRequired
            | Self::ProxyPasswordRequired
            | Self::PortForward(_)
            | Self::MppTlsFieldRequired(_)
            | Self::MppTlsRoleMismatch(_)
            | Self::MppTlsMaterial(_)
            | Self::MppTransportSecret(_) => None,
        }
    }
}

#[cfg(test)]
#[path = "tests_file.rs"]
mod tests;
