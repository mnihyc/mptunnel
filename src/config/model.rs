use crate::ingress::IngressConfig;
use crate::outbound::OutboundConfig;
use crate::performance::{MppPerformanceConfig, ResourceLimitError, ResourceLimits};
use crate::product::{
    AclError, AclRuleSpec, BalancerId, CompiledDnsPolicy, CredentialAuthority, CredentialRecord,
    DestinationAcl, DnsPlanId, DnsPlanSpec, DnsPolicySpec, DnsUpstreamEndpoint, DnsUpstreamId,
    DnsUpstreamSpec, EgressAction, GatewayBalancer, GatewayBalancerSpec, InboundId, NetworkSet,
    OutboundId, ProductAdmissionConfig, ProductAdmissionConfigError, ProductPolicyCompileError,
    ProductPolicyGeneration, RouteRuleSpec, SecurityPolicyError,
};
#[cfg(test)]
use crate::product::{CredentialCatalog, CredentialId, PrincipalId, SharedSecret};
use crate::transport::PathSpec;
use crate::transport::encrypted::{TcpClientTlsConfig, TcpServerTlsConfig};
use ipnet::IpNet;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub const DEFAULT_PATH_PROBE_INTERVAL_MS: u64 = 10_000;
pub const DEFAULT_PATH_PROBE_TIMEOUT_MS: u64 = 2_000;
pub const DEFAULT_PATH_PROBE_INTERVAL: Duration =
    Duration::from_millis(DEFAULT_PATH_PROBE_INTERVAL_MS);
pub const DEFAULT_PATH_PROBE_TIMEOUT: Duration =
    Duration::from_millis(DEFAULT_PATH_PROBE_TIMEOUT_MS);
pub const DEFAULT_RESTART_BACKOFF_MS: u64 = 1_000;
pub const DEFAULT_RESTART_MAX_BACKOFF_MS: u64 = 30_000;
pub const DEFAULT_RESTART_BACKOFF: Duration = Duration::from_millis(DEFAULT_RESTART_BACKOFF_MS);
pub const DEFAULT_RESTART_MAX_BACKOFF: Duration =
    Duration::from_millis(DEFAULT_RESTART_MAX_BACKOFF_MS);
pub const DEFAULT_AUTH_FRESHNESS_WINDOW_SECONDS: u64 = 300;
pub const DEFAULT_AUTH_FRESHNESS_WINDOW: Duration =
    Duration::from_secs(DEFAULT_AUTH_FRESHNESS_WINDOW_SECONDS);
pub const DEFAULT_AUTHENTICATION_TIMEOUT_MS: u64 = 10_000;
pub const DEFAULT_AUTHENTICATION_TIMEOUT: Duration =
    Duration::from_millis(DEFAULT_AUTHENTICATION_TIMEOUT_MS);
pub const DEFAULT_MAX_PENDING_AUTHENTICATIONS: usize = 128;
pub const DEFAULT_OUTBOUND_CONNECT_TIMEOUT_MS: u64 = 10_000;
pub const DEFAULT_OUTBOUND_CONNECT_TIMEOUT: Duration =
    Duration::from_millis(DEFAULT_OUTBOUND_CONNECT_TIMEOUT_MS);
pub const DEFAULT_SESSION_RETENTION_TIMEOUT_MS: u64 = 300_000;
pub const DEFAULT_SESSION_RETENTION_TIMEOUT: Duration =
    Duration::from_millis(DEFAULT_SESSION_RETENTION_TIMEOUT_MS);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum LogLevel {
    Off = 0,
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
    Trace = 5,
}

impl LogLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LogFormat {
    #[default]
    Text,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoggingConfig {
    /// Minimum process-event severity written to configured sinks.
    pub level: LogLevel,
    /// One stable record encoding shared by the console and file sinks.
    pub format: LogFormat,
    /// Write records to standard error.
    pub console: bool,
    /// Append records to this file. TOML paths are resolved beside the
    /// canonical configuration document.
    pub file: Option<PathBuf>,
    /// Emit sanitized Product flow-open and flow-close records. This is
    /// opt-in so normal forwarding never performs connection-log I/O.
    pub flow_events: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            format: LogFormat::Text,
            console: true,
            file: None,
            flow_events: false,
        }
    }
}

impl LoggingConfig {
    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        if self
            .file
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        {
            return Err(ConfigError::LoggingFilePathEmpty);
        }
        if self.level != LogLevel::Off && !self.console && self.file.is_none() {
            return Err(ConfigError::LoggingSinkRequired);
        }
        if self.flow_events && self.level < LogLevel::Info {
            return Err(ConfigError::FlowEventsRequireInfo);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppConfig {
    /// Process-level logging/check behavior. It does not own protocol state.
    pub logging: LoggingConfig,
    pub check_config: bool,
    /// Process supervision behavior, separate from data-plane ownership.
    pub service: ServiceConfig,
    /// Logical MPP session lifetime across a break-before-make handover.
    pub session: SessionConfig,
    /// Runtime envelopes shared by product streams, datagram flows, and carriers.
    pub resources: ResourceLimits,
    /// Product flow/open/DNS admission, independent of Core transport budgets.
    pub admission: ProductAdmissionConfig,
    /// Observation/control plane. It must not become a hidden data-plane owner.
    pub management: ManagementConfig,
    /// Role-free runtime graph: client, server, or a node containing both.
    pub command: CommandConfig,
}

impl AppConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.logging.validate()?;
        self.service.validate()?;
        self.session.validate()?;
        self.resources.validate()?;
        self.admission.validate()?;
        self.management.validate()?;
        let CommandConfig::Node(node) = &self.command;
        validate_node_config(node, self.resources)?;
        Ok(())
    }
}

fn validate_node_config(node: &NodeConfig, resources: ResourceLimits) -> Result<(), ConfigError> {
    if node.local_ingresses.is_empty() && node.servers.is_empty() {
        return Err(ConfigError::NoRuntimeServices);
    }
    validate_inbound_names(&node.local_ingresses, &node.servers)?;

    let mut leaf_networks = HashMap::with_capacity(node.outbounds.len());
    for leaf in &node.outbounds {
        if leaf_networks
            .insert(leaf.id().clone(), leaf.networks())
            .is_some()
        {
            return Err(ConfigError::ProductPolicy(format!(
                "duplicate outbound {}",
                leaf.id().as_str()
            )));
        }
        match leaf {
            OutboundLeafConfig::Mpp { config, .. } => {
                validate_mpp_outbound(config, resources)?;
            }
            OutboundLeafConfig::Local {
                connect_timeout, ..
            } => {
                if connect_timeout.is_zero() {
                    return Err(ConfigError::OutboundConnectTimeoutZero);
                }
            }
        }
    }

    let dns_policy = node
        .dns_policy
        .compile()
        .map_err(|error| ConfigError::DnsPolicy(error.to_string()))?;
    validate_gateway_balancers(&leaf_networks, &node.gateway_balancers)?;
    for server in &node.servers {
        validate_mpp_inbound(server, resources)?;
        if let Some(plan) = &server.dns_plan
            && dns_policy.plan(plan).is_none()
        {
            return Err(ConfigError::DnsPolicy(format!(
                "MPP inbound references missing DNS plan {}",
                plan.as_str()
            )));
        }
        validate_egress(&server.egress, &leaf_networks, &node.gateway_balancers)?;
        validate_mpp_inbound_egress(&server.egress, &node.gateway_balancers, &node.outbounds)?;
    }
    validate_local_ingresses(&node.local_ingresses)?;
    validate_fake_dns_tun_routes(&node.local_ingresses, &dns_policy)?;
    match (&node.product_policy, node.local_ingresses.is_empty()) {
        (Some(policy), _) => {
            policy
                .compile()
                .map_err(|error| ConfigError::ProductPolicy(error.to_string()))?;
            validate_product_policy_targets(policy, &leaf_networks, &node.gateway_balancers)?;
            validate_product_policy_dns_plans(policy, &dns_policy)?;
        }
        (None, false) => return Err(ConfigError::LocalIngressRoutingRequired),
        (None, true) => {}
    }
    Ok(())
}

fn validate_fake_dns_tun_routes(
    ingresses: &[LocalIngressConfig],
    dns_policy: &CompiledDnsPolicy,
) -> Result<(), ConfigError> {
    let Some(fake_dns) = dns_policy.fake_dns() else {
        return Ok(());
    };
    let pools = fake_dns
        .ipv4_pool
        .map(IpNet::V4)
        .into_iter()
        .chain(fake_dns.ipv6_pool.map(IpNet::V6));
    for ingress in ingresses {
        let IngressConfig::TunL4(tun) = &ingress.config else {
            continue;
        };
        let Some(managed) = tun.managed_vpn() else {
            continue;
        };
        for pool in pools.clone() {
            if managed
                .excludes
                .iter()
                .any(|excluded| ip_nets_overlap(*excluded, pool))
            {
                return Err(ConfigError::DnsPolicy(format!(
                    "FakeDNS pool {pool} overlaps a managed VPN exclude"
                )));
            }
            if let crate::platform::RouteMode::Split(includes) = &managed.route_mode
                && !includes
                    .iter()
                    .any(|included| ip_net_contains(*included, pool))
            {
                return Err(ConfigError::DnsPolicy(format!(
                    "managed split VPN does not capture FakeDNS pool {pool}"
                )));
            }
        }
    }
    Ok(())
}

fn ip_net_contains(outer: IpNet, inner: IpNet) -> bool {
    outer.addr().is_ipv4() == inner.addr().is_ipv4()
        && outer.prefix_len() <= inner.prefix_len()
        && outer.contains(&inner.addr())
}

fn ip_nets_overlap(left: IpNet, right: IpNet) -> bool {
    left.addr().is_ipv4() == right.addr().is_ipv4()
        && (left.contains(&right.addr()) || right.contains(&left.addr()))
}

fn validate_product_policy_dns_plans(
    policy: &ProductPolicyConfig,
    dns_policy: &CompiledDnsPolicy,
) -> Result<(), ConfigError> {
    for rule in &policy.routes {
        if let Some(plan) = rule.action.dns_plan()
            && dns_policy.plan(plan).is_none()
        {
            return Err(ConfigError::ProductPolicy(format!(
                "route {} references missing DNS plan {}",
                rule.id.as_str(),
                plan.as_str()
            )));
        }
    }
    Ok(())
}

fn validate_mpp_inbound_egress(
    egress: &EgressRef,
    balancers: &[GatewayBalancerConfig],
    outbounds: &[OutboundLeafConfig],
) -> Result<(), ConfigError> {
    let member_ids = match egress {
        EgressRef::Outbound(outbound) => vec![outbound.clone()],
        EgressRef::Balancer(selected) => balancers
            .iter()
            .find(|balancer| &balancer.id == selected)
            .map(|balancer| {
                balancer
                    .spec
                    .members
                    .iter()
                    .map(|member| member.id.clone())
                    .collect()
            })
            .unwrap_or_default(),
    };
    if member_ids.iter().any(|id| {
        outbounds.iter().any(
            |outbound| matches!(outbound, OutboundLeafConfig::Mpp { id: leaf, .. } if leaf == id),
        )
    }) {
        return Err(ConfigError::ProductPolicy(
            "MPP inbound egress cannot select an MPP outbound".to_string(),
        ));
    }
    Ok(())
}

fn validate_gateway_balancers(
    leaf_networks: &HashMap<OutboundId, NetworkSet>,
    balancers: &[GatewayBalancerConfig],
) -> Result<(), ConfigError> {
    let mut balancer_ids = HashSet::with_capacity(balancers.len());
    for config in balancers {
        if !balancer_ids.insert(config.id.clone()) {
            return Err(ConfigError::ProductPolicy(format!(
                "duplicate MPP balancer {}",
                config.id.as_str()
            )));
        }
        GatewayBalancer::compile(config.generation, config.spec.clone())
            .map_err(|error| ConfigError::ProductPolicy(error.to_string()))?;
        for member in &config.spec.members {
            let Some(networks) = leaf_networks.get(&member.id) else {
                return Err(ConfigError::ProductPolicy(format!(
                    "balancer {} references missing outbound {}",
                    config.id.as_str(),
                    member.id.as_str()
                )));
            };
            if member.networks != *networks {
                return Err(ConfigError::ProductPolicy(format!(
                    "balancer {} member {} capability metadata does not match its outbound",
                    config.id.as_str(),
                    member.id.as_str()
                )));
            }
        }
    }
    Ok(())
}

fn validate_egress(
    egress: &EgressRef,
    leaves: &HashMap<OutboundId, NetworkSet>,
    balancers: &[GatewayBalancerConfig],
) -> Result<(), ConfigError> {
    match egress {
        EgressRef::Outbound(outbound) => {
            if !leaves.contains_key(outbound) {
                return Err(ConfigError::ProductPolicy(format!(
                    "egress references missing outbound {}",
                    outbound.as_str()
                )));
            }
        }
        EgressRef::Balancer(selected) => {
            if !balancers.iter().any(|balancer| &balancer.id == selected) {
                return Err(ConfigError::ProductPolicy(format!(
                    "egress references missing balancer {}",
                    selected.as_str()
                )));
            }
        }
    }
    Ok(())
}

fn validate_product_policy_targets(
    policy: &ProductPolicyConfig,
    leaves: &HashMap<OutboundId, NetworkSet>,
    balancers: &[GatewayBalancerConfig],
) -> Result<(), ConfigError> {
    for rule in &policy.routes {
        match rule.action.egress() {
            EgressAction::Outbound(id) if !leaves.contains_key(id) => {
                return Err(ConfigError::ProductPolicy(format!(
                    "route {} references missing outbound {}",
                    rule.id.as_str(),
                    id.as_str()
                )));
            }
            EgressAction::Balancer(id) if !balancers.iter().any(|balancer| &balancer.id == id) => {
                return Err(ConfigError::ProductPolicy(format!(
                    "route {} references missing balancer {}",
                    rule.id.as_str(),
                    id.as_str()
                )));
            }
            EgressAction::Direct => {
                return Err(ConfigError::ProductPolicy(format!(
                    "route {} must select a configured direct outbound",
                    rule.id.as_str()
                )));
            }
            _ => {}
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionConfig {
    /// Maximum interval an established logical stream may have no carrier.
    /// Healthy idle streams with an authenticated carrier do not consume it.
    pub retention_timeout: Duration,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            retention_timeout: DEFAULT_SESSION_RETENTION_TIMEOUT,
        }
    }
}

impl SessionConfig {
    pub fn validate(self) -> Result<(), ConfigError> {
        if self.retention_timeout.is_zero() {
            return Err(ConfigError::SessionRetentionTimeoutZero);
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Default)]
pub struct ManagementConfig {
    pub listen: Vec<SocketAddr>,
    pub token: Option<String>,
    /// Serves the embedded operator UI on the management listener.
    pub dashboard: bool,
    /// Allows an authenticated MPP peer to request a sanitized path snapshot.
    pub allow_peer_diagnostics: bool,
}

impl std::fmt::Debug for ManagementConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ManagementConfig")
            .field("listen", &self.listen)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("dashboard", &self.dashboard)
            .field("allow_peer_diagnostics", &self.allow_peer_diagnostics)
            .finish()
    }
}

impl ManagementConfig {
    pub fn http_enabled(&self) -> bool {
        !self.listen.is_empty()
    }

    pub fn peer_diagnostics_enabled(&self) -> bool {
        self.allow_peer_diagnostics
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.listen.iter().any(|addr| addr.port() == 0) {
            return Err(ConfigError::ManagementListenPortZero);
        }
        if self.token.as_ref().is_some_and(|token| token.is_empty()) {
            return Err(ConfigError::ManagementTokenEmpty);
        }
        if self.token.as_ref().is_some_and(|token| {
            !(16..=256).contains(&token.len()) || !token.bytes().all(|byte| byte.is_ascii_graphic())
        }) {
            return Err(ConfigError::ManagementTokenInvalid);
        }
        if self.dashboard && !self.http_enabled() {
            return Err(ConfigError::ManagementDashboardWithoutListener);
        }
        if self.http_enabled() && self.token.is_none() {
            return Err(ConfigError::ManagementListenerRequiresToken);
        }
        if self.listen.iter().any(|addr| !addr.ip().is_loopback()) {
            return Err(ConfigError::ManagementListenerMustBeLoopback);
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientSecurityConfig {
    /// One named application credential selected by this MPP outbound.
    pub credential: Arc<CredentialRecord>,
    pub auth_freshness_window: Duration,
}

impl ClientSecurityConfig {
    pub fn new(credential: Arc<CredentialRecord>) -> Self {
        Self {
            credential,
            auth_freshness_window: DEFAULT_AUTH_FRESHNESS_WINDOW,
        }
    }

    pub fn with_auth_freshness_window(mut self, value: Duration) -> Self {
        self.auth_freshness_window = value;
        self
    }

    #[cfg(test)]
    pub(crate) fn for_test(secret: SharedSecret) -> Self {
        let record = CredentialRecord::new(
            CredentialId::parse("test-credential").expect("static test credential ID"),
            PrincipalId::parse("test-peer").expect("static test principal"),
            secret,
            None,
            false,
            0,
        )
        .expect("static test credential");
        let catalog = CredentialCatalog::compile([record]).expect("test credential catalog");
        Self::new(
            catalog
                .credential(
                    &CredentialId::parse("test-credential").expect("static test credential ID"),
                )
                .expect("test client credential"),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerSecurityConfig {
    /// Immutable Product credential set accepted by this MPP inbound.
    pub credential_authority: CredentialAuthority,
    pub auth_freshness_window: Duration,
    /// Absolute bound covering application authentication and admission after
    /// carrier accept. It is not consulted after the path is registered.
    pub authentication_timeout: Duration,
    /// Endpoint-local cap on unauthenticated TCP/QUIC carrier tasks.
    pub max_pending_authentications: usize,
}

impl ServerSecurityConfig {
    pub fn new(credential_authority: CredentialAuthority) -> Self {
        Self {
            credential_authority,
            auth_freshness_window: DEFAULT_AUTH_FRESHNESS_WINDOW,
            authentication_timeout: DEFAULT_AUTHENTICATION_TIMEOUT,
            max_pending_authentications: DEFAULT_MAX_PENDING_AUTHENTICATIONS,
        }
    }

    pub fn with_auth_freshness_window(mut self, value: Duration) -> Self {
        self.auth_freshness_window = value;
        self
    }

    pub fn with_authentication_timeout(mut self, value: Duration) -> Self {
        self.authentication_timeout = value;
        self
    }

    pub fn with_max_pending_authentications(mut self, value: usize) -> Self {
        self.max_pending_authentications = value;
        self
    }

    #[cfg(test)]
    pub(crate) fn for_test(secret: SharedSecret) -> Self {
        let id = CredentialId::parse("test-credential").expect("static test credential ID");
        let record = CredentialRecord::new(
            id.clone(),
            PrincipalId::parse("test-peer").expect("static test principal"),
            secret,
            None,
            false,
            0,
        )
        .expect("static test credential");
        let catalog = CredentialCatalog::compile([record]).expect("test credential catalog");
        Self::new(catalog.authority(&[id]).expect("test credential authority"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandConfig {
    Node(NodeConfig),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeConfig {
    /// One canonical namespace for MPP and native outbound leaves.
    pub outbounds: Vec<OutboundLeafConfig>,
    /// Product balancers over compatible leaf outbounds. They never
    /// merge MPP carrier paths or own Core scheduling state.
    pub gateway_balancers: Vec<GatewayBalancerConfig>,
    /// Product-owned local ingress surfaces. They are intentionally not owned
    /// by any one carrier/path group because routing selects that group per
    /// normalized flow.
    pub local_ingresses: Vec<LocalIngressConfig>,
    /// Immutable new-flow policy generation for local SOCKS/HTTP/TUN traffic.
    pub product_policy: Option<ProductPolicyConfig>,
    /// Immutable named split-DNS policy used whenever this node needs address
    /// evidence. Upstream transport may be system, direct, or routed.
    pub dns_policy: DnsPolicyConfig,
    pub servers: Vec<MppInboundConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayBalancerConfig {
    pub id: BalancerId,
    pub generation: u64,
    pub spec: GatewayBalancerSpec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductPolicyConfig {
    pub generation: u64,
    pub routes: Vec<RouteRuleSpec>,
    pub destination_acl: Vec<AclRuleSpec>,
}

impl ProductPolicyConfig {
    pub fn compile(&self) -> Result<ProductPolicyGeneration, ProductPolicyCompileError> {
        ProductPolicyGeneration::compile(
            self.generation,
            self.routes.clone(),
            self.destination_acl.clone(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsPolicyConfig {
    pub generation: u64,
    pub spec: DnsPolicySpec,
}

impl DnsPolicyConfig {
    /// Explicit named system resolution for the simple proxy/server profile.
    /// Managed full-VPN validation rejects this policy before publishing host
    /// routes, so the convenience default cannot become an implicit DNS leak.
    pub fn system_default() -> Self {
        let upstream = DnsUpstreamId::parse("system").expect("static DNS upstream ID");
        let plan = DnsPlanId::parse("default").expect("static DNS plan ID");
        Self {
            generation: 1,
            spec: DnsPolicySpec {
                upstreams: vec![DnsUpstreamSpec::direct(
                    upstream.clone(),
                    DnsUpstreamEndpoint::System,
                )],
                outbound_capabilities: Vec::new(),
                plans: vec![DnsPlanSpec::new(plan.clone(), vec![upstream])],
                rules: Vec::new(),
                hosts: Vec::new(),
                fake_dns: None,
                default_plan: plan,
            },
        }
    }

    pub fn compile(&self) -> Result<CompiledDnsPolicy, crate::product::DnsCompileError> {
        CompiledDnsPolicy::compile(self.generation, self.spec.clone())
    }
}

impl Default for DnsPolicyConfig {
    fn default() -> Self {
        Self::system_default()
    }
}

/// Immutable destination-authorization generation for flows accepted by one
/// MPP server inbound. An empty rule list retains Product's safe restricted-IP
/// default; only an explicit `AllowRestricted` rule can opt a scoped target in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerDestinationAclConfig {
    pub generation: u64,
    pub rules: Vec<AclRuleSpec>,
}

impl Default for ServerDestinationAclConfig {
    fn default() -> Self {
        Self {
            generation: 1,
            rules: Vec::new(),
        }
    }
}

impl ServerDestinationAclConfig {
    pub fn compile(&self) -> Result<DestinationAcl, AclError> {
        DestinationAcl::compile(self.generation, self.rules.clone())
    }
}

/// Typed reference from one MPP inbound to its native Product egress.
///
/// Configuration text is parsed once into this enum so an outbound and a
/// balancer never share an untyped string discriminator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EgressRef {
    Outbound(OutboundId),
    Balancer(BalancerId),
}

impl EgressRef {
    pub fn name(&self) -> &str {
        match self {
            Self::Outbound(name) => name.as_str(),
            Self::Balancer(name) => name.as_str(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MppOutboundConfig {
    /// Representative security for process-level validation; live path security
    /// is stored per `ClientPathConfig`.
    pub security: ClientSecurityConfig,
    /// Candidate MPP carrier paths. Each path owns its own peer security.
    pub paths: Vec<ClientPathConfig>,
    pub path_probe_interval: Duration,
    pub path_probe_timeout: Duration,
    /// MPP sender behavior for this outbound path group.
    pub performance: MppPerformanceConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundLeafConfig {
    Mpp {
        id: OutboundId,
        config: Box<MppOutboundConfig>,
    },
    Local {
        id: OutboundId,
        config: OutboundConfig,
        connect_timeout: Duration,
    },
}

impl OutboundLeafConfig {
    pub const fn id(&self) -> &OutboundId {
        match self {
            Self::Mpp { id, .. } | Self::Local { id, .. } => id,
        }
    }

    pub fn networks(&self) -> crate::product::NetworkSet {
        match self {
            Self::Mpp { .. } => crate::product::NetworkSet::TCP_UDP,
            Self::Local { config, .. } if config.supports_udp_targets() => {
                crate::product::NetworkSet::TCP_UDP
            }
            Self::Local { .. } => crate::product::NetworkSet::TCP,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalIngressConfig {
    /// Required canonical operator-assigned name used by routing and
    /// management. It is not a protocol identity.
    pub name: String,
    pub config: IngressConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientPathConfig {
    /// Stable Product name of one configured carrier path. Core scheduling
    /// continues to use protocol path identities and never this name.
    pub name: String,
    /// One configured carrier path for an MPP outbound.
    pub spec: PathSpec,
    /// Security scoped to this path's MPP peer relationship.
    pub security: ClientSecurityConfig,
    /// Independently pinned carrier TLS identity. TCP and QUIC consume the same
    /// Product-configured identity; application credentials never derive it.
    pub tls: TcpClientTlsConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedPathConfig {
    /// Stable Product name used for management and presentation only.
    pub name: String,
    pub spec: PathSpec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MppInboundConfig {
    /// Required canonical operator-assigned name used by Product policy and
    /// management. It never enters the MPP wire protocol.
    pub name: String,
    /// Egress outbound or egress balancer selected for accepted MPP flows.
    pub egress: EgressRef,
    /// Optional DNS plan for target resolution before native egress.
    pub dns_plan: Option<crate::product::DnsPlanId>,
    /// Named carrier listen/bind paths owned by this MPP inbound.
    pub paths: Vec<NamedPathConfig>,
    /// Security scoped to peers that join this MPP inbound.
    pub security: ServerSecurityConfig,
    /// TLS identity shared by every TCP and QUIC listener in this MPP inbound.
    pub tls: TcpServerTlsConfig,
    /// Immutable Product destination authorization for accepted TCP/UDP flows.
    pub destination_acl: ServerDestinationAclConfig,
    /// MPP sender behavior for streams accepted by this inbound path group.
    pub performance: MppPerformanceConfig,
}

fn validate_mpp_outbound(
    client: &MppOutboundConfig,
    resources: ResourceLimits,
) -> Result<(), ConfigError> {
    if client.paths.is_empty() {
        return Err(ConfigError::NoPaths);
    }
    validate_path_names(client.paths.iter().map(|path| path.name.as_str()))?;
    validate_client_security_config(&client.security)?;
    if client.paths.len() > resources.max_paths {
        return Err(ConfigError::TooManyPaths {
            actual: client.paths.len(),
            limit: resources.max_paths,
        });
    }
    if client.paths.iter().any(|path| {
        path.spec.underlay == crate::protocol::UnderlayProtocol::Udp
            && path.tls.quic_server_name_text().is_none()
    }) {
        return Err(ConfigError::QuicTlsServerNameRequiresDns);
    }
    if client.path_probe_interval.is_zero() {
        return Err(ConfigError::PathProbeIntervalZero);
    }
    if client.path_probe_timeout.is_zero() {
        return Err(ConfigError::PathProbeTimeoutZero);
    }
    Ok(())
}

fn validate_local_ingresses(ingresses: &[LocalIngressConfig]) -> Result<(), ConfigError> {
    let managed_tun_count = ingresses
        .iter()
        .filter(|ingress| {
            matches!(
                &ingress.config,
                IngressConfig::TunL4(tun) if tun.managed_vpn().is_some()
            )
        })
        .count();
    if managed_tun_count > 1 {
        return Err(ConfigError::MultipleManagedTunInbounds {
            actual: managed_tun_count,
        });
    }
    for ingress in ingresses {
        validate_ingress(&ingress.config)?;
        if let IngressConfig::TunL4(tun) = &ingress.config {
            validate_tun_l4(tun)?;
        }
    }
    Ok(())
}

fn validate_inbound_names(
    local_ingresses: &[LocalIngressConfig],
    mpp_inbounds: &[MppInboundConfig],
) -> Result<(), ConfigError> {
    let mut seen = HashSet::with_capacity(local_ingresses.len() + mpp_inbounds.len());
    for name in local_ingresses
        .iter()
        .map(|inbound| inbound.name.as_str())
        .chain(mpp_inbounds.iter().map(|inbound| inbound.name.as_str()))
    {
        let canonical = InboundId::parse(name).map_err(|_| ConfigError::InboundNameInvalid)?;
        if canonical.as_str() != name {
            return Err(ConfigError::InboundNameInvalid);
        }
        if !seen.insert(name) {
            return Err(ConfigError::DuplicateInboundName(name.to_string()));
        }
    }
    Ok(())
}

fn validate_mpp_inbound(
    server: &MppInboundConfig,
    resources: ResourceLimits,
) -> Result<(), ConfigError> {
    if server.paths.is_empty() {
        return Err(ConfigError::NoPaths);
    }
    validate_path_names(server.paths.iter().map(|path| path.name.as_str()))?;
    if server.paths.len() > resources.max_paths {
        return Err(ConfigError::TooManyPaths {
            actual: server.paths.len(),
            limit: resources.max_paths,
        });
    }
    if server
        .paths
        .iter()
        .any(|path| path.spec.binding.source_ip.is_some())
    {
        return Err(ConfigError::ServerPathSourceBinding);
    }
    if server
        .paths
        .iter()
        .any(|path| !path.spec.endpoint.ports().is_single())
    {
        return Err(ConfigError::ServerPathPortRange);
    }
    validate_server_security_config(&server.security)?;
    server
        .destination_acl
        .compile()
        .map_err(|error| ConfigError::ServerDestinationAcl(error.to_string()))?;
    Ok(())
}

fn validate_path_names<'a>(names: impl IntoIterator<Item = &'a str>) -> Result<(), ConfigError> {
    let mut seen = HashSet::new();
    for name in names {
        let canonical =
            crate::product::RuleId::parse(name).map_err(|_| ConfigError::PathNameInvalid)?;
        if canonical.as_str() != name {
            return Err(ConfigError::PathNameInvalid);
        }
        if !seen.insert(name) {
            return Err(ConfigError::DuplicatePathName(name.to_string()));
        }
    }
    Ok(())
}

fn validate_client_security_config(security: &ClientSecurityConfig) -> Result<(), ConfigError> {
    if security.auth_freshness_window.is_zero() {
        return Err(ConfigError::AuthFreshnessWindowZero);
    }
    Ok(())
}

fn validate_server_security_config(security: &ServerSecurityConfig) -> Result<(), ConfigError> {
    if security.auth_freshness_window.is_zero() {
        return Err(ConfigError::AuthFreshnessWindowZero);
    }
    if security.authentication_timeout.is_zero() {
        return Err(ConfigError::AuthenticationTimeoutZero);
    }
    if security.max_pending_authentications == 0 {
        return Err(ConfigError::MaxPendingAuthenticationsZero);
    }
    Ok(())
}

fn validate_ingress(ingress: &IngressConfig) -> Result<(), ConfigError> {
    match ingress {
        IngressConfig::Socks5 { listen, .. } | IngressConfig::HttpConnect { listen, .. } => {
            if listen.is_empty() {
                return Err(ConfigError::NoListenAddresses);
            }
        }
        IngressConfig::TcpForward(config) => {
            if config.listen().is_empty() {
                return Err(ConfigError::NoListenAddresses);
            }
        }
        IngressConfig::UdpForward(config) => {
            if config.listen().is_empty() {
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
    tun.compile_managed_vpn()
        .map_err(|error| ConfigError::ManagedVpn(error.to_string()))?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    Security(SecurityPolicyError),
    ProductAdmission(ProductAdmissionConfigError),
    LoggingFilePathEmpty,
    LoggingSinkRequired,
    FlowEventsRequireInfo,
    AuthFreshnessWindowZero,
    AuthenticationTimeoutZero,
    MaxPendingAuthenticationsZero,
    NoPaths,
    FrameLimitTooSmall,
    PayloadLimitExceedsFrameLimit,
    AckRangeLimitZero,
    PathLimitZero,
    PathLimitTooLarge,
    StreamLimitZero,
    QuicBidiStreamLimitZero,
    StreamWindowLimitZero,
    ReinjectionLimitTooSmall,
    ReorderLimitTooSmall,
    ReinjectionCacheChunkLimitZero,
    ReorderBufferChunkLimitZero,
    RetainedReceiveRangeLimitZero,
    DatagramQueueLimitTooSmall,
    MaxReliableRelayChunkBytesZero,
    MaxReliableRelayChunkExceedsPayloadLimit,
    PathFlightLimitTooSmall,
    PathFlightLimitExceedsReinjectionLimit,
    TcpPathHeartbeatIntervalZero,
    TcpPathHeartbeatTimeoutZero,
    TcpPathHeartbeatTimeoutTooSmall,
    QuicPathKeepAliveIntervalZero,
    QuicPathIdleTimeoutZero,
    QuicPathIdleTimeoutTooSmall,
    QuicPathIdleTimeoutTooLarge,
    RestartBackoffZero,
    RestartMaxBackoffZero,
    RestartMaxBackoffTooSmall,
    RestartLimitZero,
    SessionRetentionTimeoutZero,
    NoIngresses,
    NoListenAddresses,
    TooManyPaths { actual: usize, limit: usize },
    PathNameInvalid,
    DuplicatePathName(String),
    PathProbeIntervalZero,
    PathProbeTimeoutZero,
    QuicTlsServerNameRequiresDns,
    ServerPathSourceBinding,
    ServerPathPortRange,
    TunAddressRequired,
    TunIpv4PrefixInvalid,
    TunIpv6PrefixInvalid,
    TunMtuTooSmall,
    TunIpv6MtuTooSmall,
    TunDnsTtlZero,
    TunDnsResolverPortZero,
    ManagedVpn(String),
    MultipleManagedTunInbounds { actual: usize },
    DnsPolicy(String),
    OutboundConnectTimeoutZero,
    InboundNameInvalid,
    DuplicateInboundName(String),
    LocalIngressRoutingRequired,
    ProductPolicy(String),
    ServerDestinationAcl(String),
    ManagementListenPortZero,
    ManagementTokenEmpty,
    ManagementTokenInvalid,
    ManagementDashboardWithoutListener,
    ManagementListenerRequiresToken,
    ManagementListenerMustBeLoopback,
    NoRuntimeServices,
}

impl From<ResourceLimitError> for ConfigError {
    fn from(value: ResourceLimitError) -> Self {
        match value {
            ResourceLimitError::FrameLimitTooSmall => Self::FrameLimitTooSmall,
            ResourceLimitError::PayloadLimitExceedsFrameLimit => {
                Self::PayloadLimitExceedsFrameLimit
            }
            ResourceLimitError::AckRangeLimitZero => Self::AckRangeLimitZero,
            ResourceLimitError::PathLimitZero => Self::PathLimitZero,
            ResourceLimitError::PathLimitTooLarge => Self::PathLimitTooLarge,
            ResourceLimitError::StreamLimitZero => Self::StreamLimitZero,
            ResourceLimitError::QuicBidiStreamLimitZero => Self::QuicBidiStreamLimitZero,
            ResourceLimitError::StreamWindowLimitZero => Self::StreamWindowLimitZero,
            ResourceLimitError::ReinjectionLimitTooSmall => Self::ReinjectionLimitTooSmall,
            ResourceLimitError::ReorderLimitTooSmall => Self::ReorderLimitTooSmall,
            ResourceLimitError::ReinjectionCacheChunkLimitZero => {
                Self::ReinjectionCacheChunkLimitZero
            }
            ResourceLimitError::ReorderBufferChunkLimitZero => Self::ReorderBufferChunkLimitZero,
            ResourceLimitError::RetainedReceiveRangeLimitZero => {
                Self::RetainedReceiveRangeLimitZero
            }
            ResourceLimitError::DatagramQueueLimitTooSmall => Self::DatagramQueueLimitTooSmall,
            ResourceLimitError::MaxReliableRelayChunkBytesZero => {
                Self::MaxReliableRelayChunkBytesZero
            }
            ResourceLimitError::MaxReliableRelayChunkExceedsPayloadLimit => {
                Self::MaxReliableRelayChunkExceedsPayloadLimit
            }
            ResourceLimitError::PathFlightLimitTooSmall => Self::PathFlightLimitTooSmall,
            ResourceLimitError::PathFlightLimitExceedsReinjectionLimit => {
                Self::PathFlightLimitExceedsReinjectionLimit
            }
            ResourceLimitError::TcpPathHeartbeatIntervalZero => Self::TcpPathHeartbeatIntervalZero,
            ResourceLimitError::TcpPathHeartbeatTimeoutZero => Self::TcpPathHeartbeatTimeoutZero,
            ResourceLimitError::TcpPathHeartbeatTimeoutTooSmall => {
                Self::TcpPathHeartbeatTimeoutTooSmall
            }
            ResourceLimitError::QuicPathKeepAliveIntervalZero => {
                Self::QuicPathKeepAliveIntervalZero
            }
            ResourceLimitError::QuicPathIdleTimeoutZero => Self::QuicPathIdleTimeoutZero,
            ResourceLimitError::QuicPathIdleTimeoutTooSmall => Self::QuicPathIdleTimeoutTooSmall,
            ResourceLimitError::QuicPathIdleTimeoutTooLarge => Self::QuicPathIdleTimeoutTooLarge,
        }
    }
}

impl From<SecurityPolicyError> for ConfigError {
    fn from(value: SecurityPolicyError) -> Self {
        Self::Security(value)
    }
}

impl From<ProductAdmissionConfigError> for ConfigError {
    fn from(value: ProductAdmissionConfigError) -> Self {
        Self::ProductAdmission(value)
    }
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Security(err) => write!(f, "{err}"),
            Self::ProductAdmission(err) => write!(f, "{err}"),
            Self::LoggingFilePathEmpty => write!(f, "logging file path must not be empty"),
            Self::LoggingSinkRequired => {
                write!(f, "enabled logging requires the console or file sink")
            }
            Self::FlowEventsRequireInfo => {
                write!(
                    f,
                    "flow-event logging requires log level info, debug, or trace"
                )
            }
            Self::AuthFreshnessWindowZero => {
                write!(f, "auth freshness window must be greater than zero")
            }
            Self::AuthenticationTimeoutZero => {
                write!(f, "authentication timeout must be greater than zero")
            }
            Self::MaxPendingAuthenticationsZero => {
                write!(
                    f,
                    "maximum pending authentications must be greater than zero"
                )
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
            Self::ReinjectionLimitTooSmall => {
                write!(
                    f,
                    "max reinjection bytes must be at least max payload bytes"
                )
            }
            Self::ReorderLimitTooSmall => {
                write!(f, "max reorder bytes must be at least max payload bytes")
            }
            Self::ReinjectionCacheChunkLimitZero => {
                write!(f, "max reinjection cache chunks must be greater than zero")
            }
            Self::ReorderBufferChunkLimitZero => {
                write!(f, "max reorder buffer chunks must be greater than zero")
            }
            Self::RetainedReceiveRangeLimitZero => {
                write!(f, "max retained receive ranges must be greater than zero")
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
            Self::PathFlightLimitExceedsReinjectionLimit => {
                write!(
                    f,
                    "max path flight bytes must be no greater than max reinjection bytes"
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
            Self::QuicPathKeepAliveIntervalZero => {
                write!(f, "QUIC path keep-alive interval must be greater than zero")
            }
            Self::QuicPathIdleTimeoutZero => {
                write!(f, "QUIC path idle timeout must be greater than zero")
            }
            Self::QuicPathIdleTimeoutTooSmall => {
                write!(
                    f,
                    "QUIC path idle timeout must exceed its keep-alive interval"
                )
            }
            Self::QuicPathIdleTimeoutTooLarge => {
                write!(f, "QUIC path idle timeout exceeds the protocol timer range")
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
            Self::SessionRetentionTimeoutZero => {
                write!(f, "session retention timeout must be greater than zero")
            }
            Self::NoIngresses => write!(f, "at least one client ingress is required"),
            Self::NoListenAddresses => {
                write!(f, "proxy ingress requires at least one listen address")
            }
            Self::TooManyPaths { actual, limit } => {
                write!(f, "{actual} paths configured, limit is {limit}")
            }
            Self::PathNameInvalid => {
                write!(f, "path name must be canonical Product name text")
            }
            Self::DuplicatePathName(name) => {
                write!(f, "duplicate path name {name:?}")
            }
            Self::PathProbeIntervalZero => {
                write!(f, "path probe interval must be greater than zero")
            }
            Self::PathProbeTimeoutZero => {
                write!(f, "path probe timeout must be greater than zero")
            }
            Self::QuicTlsServerNameRequiresDns => write!(
                f,
                "QUIC paths require a DNS TLS server name because HTTP/3 authority is bound to SNI; carrier endpoints may still use IP addresses"
            ),
            Self::ServerPathSourceBinding => {
                write!(f, "source-ip is valid only for client carrier paths")
            }
            Self::ServerPathPortRange => write!(
                f,
                "server carrier paths require one listener port; forward any advertised port range to that listener"
            ),
            Self::TunAddressRequired => write!(f, "TUN L4 ingress requires IPv4 or IPv6 address"),
            Self::TunIpv4PrefixInvalid => write!(f, "TUN IPv4 prefix must be in 0..=32"),
            Self::TunIpv6PrefixInvalid => write!(f, "TUN IPv6 prefix must be in 0..=128"),
            Self::TunMtuTooSmall => write!(f, "TUN MTU must be at least 576 bytes"),
            Self::TunIpv6MtuTooSmall => write!(f, "TUN IPv6 MTU must be at least 1280 bytes"),
            Self::TunDnsTtlZero => write!(f, "TUN DNS TTL must be greater than zero"),
            Self::TunDnsResolverPortZero => write!(f, "TUN DNS resolver port must be nonzero"),
            Self::ManagedVpn(error) => {
                write!(f, "invalid managed VPN configuration: {error}")
            }
            Self::MultipleManagedTunInbounds { actual } => write!(
                f,
                "node config defines {actual} managed TUN inbounds; at most one may own host VPN state"
            ),
            Self::DnsPolicy(error) => write!(f, "invalid DNS policy: {error}"),
            Self::OutboundConnectTimeoutZero => {
                write!(f, "outbound connect timeout must be greater than zero")
            }
            Self::InboundNameInvalid => {
                write!(f, "inbound name must be canonical Product name text")
            }
            Self::DuplicateInboundName(name) => {
                write!(f, "duplicate inbound name {name:?}")
            }
            Self::LocalIngressRoutingRequired => {
                write!(
                    f,
                    "local inbounds require a compiled Product routing policy"
                )
            }
            Self::ProductPolicy(error) => write!(f, "{error}"),
            Self::ServerDestinationAcl(error) => {
                write!(f, "invalid server destination ACL policy: {error}")
            }
            Self::ManagementListenPortZero => {
                write!(f, "management API listen port must be nonzero")
            }
            Self::ManagementTokenEmpty => {
                write!(f, "management API token must not be empty")
            }
            Self::ManagementTokenInvalid => write!(
                f,
                "management API token must contain 16-256 visible ASCII characters"
            ),
            Self::ManagementDashboardWithoutListener => {
                write!(
                    f,
                    "management dashboard requires at least one listen address"
                )
            }
            Self::ManagementListenerRequiresToken => {
                write!(f, "management API listeners require a token")
            }
            Self::ManagementListenerMustBeLoopback => {
                write!(f, "management API listeners must use loopback addresses")
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
#[path = "model_test.rs"]
mod tests;
