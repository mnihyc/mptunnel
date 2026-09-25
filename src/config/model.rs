use crate::ingress::IngressConfig;
use crate::outbound::OutboundConfig;
use crate::performance::{MppPerformanceConfig, ResourceLimitError, ResourceLimits};
use crate::product::{
    BalancerId, CompiledDnsPolicy, CredentialAuthority, CredentialRecord, DnsActivation,
    DnsCompileError, DnsEgressSpec, DnsPlanId, DnsPlanSpec, DnsPolicySpec, DnsUpstreamEndpoint,
    DnsUpstreamId, DnsUpstreamSpec, EgressAction, GatewayBalancer, GatewayBalancerSpec, InboundId,
    Network, NetworkSet, OutboundId, PrincipalId, ProductAdmissionConfig,
    ProductAdmissionConfigError, ProductPolicyCompileError, ProductPolicyGeneration, RouteRuleSpec,
    SecurityPolicyError, TargetResolutionMode,
};
#[cfg(test)]
use crate::product::{CredentialCatalog, CredentialId, SharedSecret};
use crate::transport::PathSpec;
use crate::transport::encrypted::{TcpClientTlsConfig, TcpServerTlsConfig};
use crate::webhook::{EventKind, WebhookConfig, WebhookRule, WebhookTarget};
use ipnet::IpNet;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
pub const DEFAULT_MPP_TLS_SERVER_NAME: &str = "mptunnel.example";
pub const DEFAULT_AUTH_FRESHNESS_WINDOW: Duration =
    Duration::from_secs(DEFAULT_AUTH_FRESHNESS_WINDOW_SECONDS);
pub const DEFAULT_AUTHENTICATION_TIMEOUT_MS: u64 = 10_000;
pub const DEFAULT_AUTHENTICATION_TIMEOUT: Duration =
    Duration::from_millis(DEFAULT_AUTHENTICATION_TIMEOUT_MS);
pub const DEFAULT_MAX_PENDING_AUTHENTICATIONS: usize = 4_096;
pub const DEFAULT_OUTBOUND_CONNECT_TIMEOUT_MS: u64 = 10_000;
pub const DEFAULT_OUTBOUND_CONNECT_TIMEOUT: Duration =
    Duration::from_millis(DEFAULT_OUTBOUND_CONNECT_TIMEOUT_MS);
pub const DEFAULT_SESSION_RETENTION_TIMEOUT_MS: u64 = 300_000;
pub const DEFAULT_SESSION_RETENTION_TIMEOUT: Duration =
    Duration::from_millis(DEFAULT_SESSION_RETENTION_TIMEOUT_MS);
pub const DEFAULT_PRODUCT_FLOW_IDLE_TIMEOUT_SECONDS: u64 = 300;
pub const DEFAULT_PRODUCT_FLOW_IDLE_TIMEOUT: Option<Duration> = Some(Duration::from_secs(
    DEFAULT_PRODUCT_FLOW_IDLE_TIMEOUT_SECONDS,
));

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum LogLevel {
    Off = 0,
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
}

impl LogLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
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
    /// Process-event verbosity threshold written to configured sinks.
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
    /// Product TCP/UDP lifetime policy, independent of carrier/session liveness.
    pub flow: ProductFlowConfig,
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
        self.flow.validate()?;
        self.resources.validate()?;
        self.admission.validate()?;
        self.management.validate()?;
        let CommandConfig::Node(node) = &self.command;
        validate_node_config(node, self.resources)?;
        Ok(())
    }
}

fn validate_node_config(node: &NodeConfig, resources: ResourceLimits) -> Result<(), ConfigError> {
    validate_forwarding_mode(node)?;
    if node.local_ingresses.is_empty()
        && node.tun_l3_ingresses.is_empty()
        && node.servers.is_empty()
    {
        return Err(ConfigError::NoRuntimeServices);
    }
    validate_inbound_names(&node.local_ingresses, &node.tun_l3_ingresses, &node.servers)?;
    validate_packet_device_names(node)?;

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

    validate_tun_l3_ingresses(&node.tun_l3_ingresses, &node.outbounds)?;

    let route_dns_plans = node
        .product_policy
        .iter()
        .flat_map(|policy| &policy.routes)
        .filter_map(|rule| rule.action.dns_plan());
    let (dns_policy, dns_activation) = node
        .dns_policy
        .compile_active(route_dns_plans)
        .map_err(|error| ConfigError::DnsPolicy(error.to_string()))?;
    validate_gateway_balancers(&leaf_networks, &node.gateway_balancers)?;
    for server in &node.servers {
        validate_mpp_inbound(server, resources)?;
    }
    validate_local_ingresses(&node.local_ingresses)?;
    validate_webhooks(node)?;
    validate_synthetic_capture_tun_routes(&node.local_ingresses, &dns_policy, &dns_activation)?;
    // Do not approximate cross-policy synthetic-capture reachability here:
    // domain and principal selectors can make active policies disjoint. The
    // TUN runtime binds every recovered lease to its DNS generation, policy,
    // and capture, then rejects a route selecting different provenance.
    let has_l4_inbound = !node.local_ingresses.is_empty()
        || node.servers.iter().any(|server| server.tun_l3.is_none());
    match (&node.product_policy, has_l4_inbound) {
        (Some(policy), _) => {
            policy
                .compile()
                .map_err(|error| ConfigError::ProductPolicy(error.to_string()))?;
            validate_product_policy_targets(policy, &leaf_networks, &node.gateway_balancers)?;
            validate_product_policy_dns_plans(policy, &dns_policy)?;
            validate_product_policy_reachability(
                policy,
                node,
                &leaf_networks,
                &node.gateway_balancers,
            )?;
        }
        (None, true) => return Err(ConfigError::LocalIngressRoutingRequired),
        (None, false) => {}
    }
    Ok(())
}

fn validate_webhooks(node: &NodeConfig) -> Result<(), ConfigError> {
    node.webhooks
        .validate()
        .map_err(|error| ConfigError::Webhook(error.to_string()))?;
    if !node.webhooks.is_enabled() {
        return Ok(());
    }

    let outbound_by_name: HashMap<_, _> = node
        .outbounds
        .iter()
        .map(|outbound| (outbound.id().as_str(), outbound))
        .collect();
    let inbound_names: HashSet<_> = node
        .servers
        .iter()
        .map(|server| server.name.as_str())
        .collect();
    let balancers: HashMap<_, _> = node
        .gateway_balancers
        .iter()
        .map(|balancer| (balancer.id.as_str(), balancer))
        .collect();

    for rule in &node.webhooks.rules {
        validate_webhook_target(node, rule, &outbound_by_name, &balancers)?;
        let matcher = &rule.when;
        for name in &matcher.outbounds {
            if !outbound_by_name.contains_key(name.as_str()) {
                return Err(ConfigError::Webhook(format!(
                    "rule {:?} references unknown source outbound {name:?}",
                    rule.name
                )));
            }
        }
        for name in &matcher.inbounds {
            if !inbound_names.contains(name.as_str()) {
                return Err(ConfigError::Webhook(format!(
                    "rule {:?} references unknown MPP source inbound {name:?}",
                    rule.name
                )));
            }
        }
        for name in &matcher.balancers {
            if !balancers.contains_key(name.as_str()) {
                return Err(ConfigError::Webhook(format!(
                    "rule {:?} references unknown source balancer {name:?}",
                    rule.name
                )));
            }
        }
        validate_webhook_source_capabilities(node, rule, &outbound_by_name, &balancers)?;
        validate_webhook_template_context(rule)?;
        validate_webhook_dependency_cycle(node, rule, &outbound_by_name, &balancers)?;
    }
    Ok(())
}

fn webhook_domain_host(host: &str) -> Option<crate::product::DomainName> {
    if host.parse::<IpAddr>().is_ok() {
        return None;
    }
    crate::product::DomainName::parse(host).ok()
}

fn webhook_target_resolves_domain_locally(target: &WebhookTarget, node: &NodeConfig) -> bool {
    if target.target_resolution == TargetResolutionMode::FullResolve {
        return true;
    }
    match &target.egress {
        EgressRef::Outbound(id) => node
            .outbounds
            .iter()
            .find(|outbound| outbound.id() == id)
            .is_some_and(|outbound| match outbound {
                OutboundLeafConfig::Local { config, .. } => config.requires_ip_target(),
                OutboundLeafConfig::Mpp { .. } => false,
            }),
        EgressRef::Balancer(id) => node
            .gateway_balancers
            .iter()
            .find(|balancer| balancer.id == *id)
            .is_some_and(|balancer| {
                balancer.spec.members.iter().any(|member| {
                    node.outbounds
                        .iter()
                        .find(|outbound| outbound.id() == &member.id)
                        .is_some_and(|outbound| match outbound {
                            OutboundLeafConfig::Local { config, .. } => config.requires_ip_target(),
                            OutboundLeafConfig::Mpp { .. } => false,
                        })
                })
            }),
    }
}

fn validate_webhook_target<'a>(
    node: &NodeConfig,
    rule: &WebhookRule,
    outbounds: &HashMap<&'a str, &'a OutboundLeafConfig>,
    balancers: &HashMap<&str, &GatewayBalancerConfig>,
) -> Result<(), ConfigError> {
    match &rule.target.egress {
        EgressRef::Outbound(id) => {
            let Some(outbound) = outbounds.get(id.as_str()) else {
                return Err(ConfigError::Webhook(format!(
                    "rule {:?} selects unknown outbound {id}",
                    rule.name
                )));
            };
            if !outbound.networks().contains(Network::Tcp) {
                return Err(ConfigError::Webhook(format!(
                    "rule {:?} outbound {id} does not support TCP",
                    rule.name
                )));
            }
            if node.forwarding_mode == ForwardingMode::L3
                && matches!(outbound, OutboundLeafConfig::Mpp { .. })
            {
                return Err(ConfigError::Webhook(format!(
                    "L3 webhook rule {:?} cannot use an MPP outbound; select a native TCP outbound",
                    rule.name
                )));
            }
        }
        EgressRef::Balancer(id) => {
            let Some(balancer) = balancers.get(id.as_str()) else {
                return Err(ConfigError::Webhook(format!(
                    "rule {:?} selects unknown balancer {id}",
                    rule.name
                )));
            };
            if node.forwarding_mode == ForwardingMode::L3
                || !balancer
                    .spec
                    .members
                    .iter()
                    .any(|member| member.networks.contains(Network::Tcp))
            {
                return Err(ConfigError::Webhook(format!(
                    "rule {:?} balancer {id} has no supported TCP egress",
                    rule.name
                )));
            }
        }
    }
    if rule.target.target_resolution == TargetResolutionMode::RouteOnly {
        return Err(ConfigError::Webhook(format!(
            "rule {:?} cannot use route-only target resolution",
            rule.name
        )));
    }
    if let Some(policy) = &rule.target.dns_policy
        && !node
            .dns_policy
            .spec
            .plans
            .iter()
            .any(|plan| &plan.id == policy)
    {
        return Err(ConfigError::Webhook(format!(
            "rule {:?} references unknown DNS policy {policy}",
            rule.name
        )));
    }
    let size = rule.target.compiled_material_bytes();
    if size > 64 * 1024 {
        return Err(ConfigError::Webhook(format!(
            "rule {:?} target templates and headers exceed 64 KiB ({size} bytes)",
            rule.name
        )));
    }
    Ok(())
}

fn validate_webhook_source_capabilities(
    node: &NodeConfig,
    rule: &WebhookRule,
    outbounds: &HashMap<&str, &OutboundLeafConfig>,
    balancers: &HashMap<&str, &GatewayBalancerConfig>,
) -> Result<(), ConfigError> {
    let matcher = &rule.when;
    let known_paths: HashSet<_> = node
        .outbounds
        .iter()
        .filter_map(|outbound| match outbound {
            OutboundLeafConfig::Mpp { id, config } => Some(
                config
                    .paths
                    .iter()
                    .map(move |path| (id.as_str(), path.name.as_str())),
            ),
            OutboundLeafConfig::Local { .. } => None,
        })
        .flatten()
        .collect();
    for path in &matcher.paths {
        let found = known_paths.iter().any(|(outbound, configured_path)| {
            configured_path == path
                && (matcher.outbounds.is_empty()
                    || matcher.outbounds.iter().any(|name| name == outbound))
        });
        if !found {
            return Err(ConfigError::Webhook(format!(
                "rule {:?} references unknown MPP path {path:?} for its selected outbound set",
                rule.name
            )));
        }
    }
    validate_webhook_source_shape(matcher, &rule.name)?;
    validate_webhook_source_outbound_types(matcher, &rule.name, outbounds)?;
    for name in &matcher.balancers {
        if !balancers.contains_key(name.as_str()) {
            return Err(ConfigError::Webhook(format!(
                "rule {:?} references unknown balancer source {name:?}",
                rule.name
            )));
        }
    }
    Ok(())
}

fn validate_webhook_source_outbound_types(
    matcher: &crate::webhook::EventMatcher,
    rule_name: &str,
    outbounds: &HashMap<&str, &OutboundLeafConfig>,
) -> Result<(), ConfigError> {
    let requires_mpp = matcher.branches.iter().any(|branch| {
        branch
            .events
            .iter()
            .any(|event| is_path_event(*event) || is_carrier_or_session_event(*event))
    });
    if requires_mpp {
        for name in &matcher.outbounds {
            if !matches!(
                outbounds.get(name.as_str()),
                Some(OutboundLeafConfig::Mpp { .. })
            ) {
                return Err(ConfigError::Webhook(format!(
                    "rule {rule_name:?} path, carrier, and session events require MPP source outbound {name:?}"
                )));
            }
        }
    }
    Ok(())
}

fn validate_webhook_source_shape(
    matcher: &crate::webhook::EventMatcher,
    rule_name: &str,
) -> Result<(), ConfigError> {
    let events = matcher
        .branches
        .iter()
        .flat_map(|branch| branch.events.iter().copied())
        .collect::<Vec<_>>();
    let any = |predicate: fn(EventKind) -> bool| events.iter().any(|event| predicate(*event));
    let every = |predicate: fn(EventKind) -> bool| events.iter().all(|event| predicate(*event));

    if any(is_path_event) && (!matcher.inbounds.is_empty() || !matcher.balancers.is_empty()) {
        return Err(ConfigError::Webhook(format!(
            "rule {rule_name:?} path events cannot use inbound or balancer source selectors"
        )));
    }
    if !matcher.paths.is_empty() && !every(|event| is_path_event(event) || is_carrier_event(event))
    {
        return Err(ConfigError::Webhook(format!(
            "rule {rule_name:?} paths selector requires only path or client carrier events"
        )));
    }
    if !matcher.inbounds.is_empty()
        && !every(|event| is_carrier_event(event) || is_session_event(event))
    {
        return Err(ConfigError::Webhook(format!(
            "rule {rule_name:?} inbounds selector requires only server carrier or session events"
        )));
    }
    if !matcher.inbounds.is_empty() && (!matcher.outbounds.is_empty() || !matcher.paths.is_empty())
    {
        return Err(ConfigError::Webhook(format!(
            "rule {rule_name:?} cannot combine inbound and outbound/path source selectors"
        )));
    }
    if !matcher.balancers.is_empty() && !every(is_balancer_event) {
        return Err(ConfigError::Webhook(format!(
            "rule {rule_name:?} balancers selector requires only balancer events"
        )));
    }
    if any(is_balancer_event) && (!matcher.paths.is_empty() || !matcher.inbounds.is_empty()) {
        return Err(ConfigError::Webhook(format!(
            "rule {rule_name:?} balancer events cannot use path or inbound source selectors"
        )));
    }
    if !matcher.outbounds.is_empty()
        && !every(|event| {
            is_path_event(event) || is_carrier_or_session_event(event) || is_balancer_event(event)
        })
    {
        return Err(ConfigError::Webhook(format!(
            "rule {rule_name:?} outbounds selector is not valid for every selected event source"
        )));
    }
    if !matcher.transports.is_empty()
        && !every(|event| is_path_event(event) || is_carrier_event(event))
    {
        return Err(ConfigError::Webhook(format!(
            "rule {rule_name:?} transports selector requires only path or carrier events"
        )));
    }
    Ok(())
}

fn is_path_event(event: EventKind) -> bool {
    matches!(
        event,
        EventKind::PathStateChanged
            | EventKind::PathPolicyChanged
            | EventKind::PathProbeCompleted
            | EventKind::PathInterval
    )
}

fn is_carrier_or_session_event(event: EventKind) -> bool {
    is_carrier_event(event) || is_session_event(event)
}

fn is_carrier_event(event: EventKind) -> bool {
    matches!(
        event,
        EventKind::CarrierStateChanged
            | EventKind::CarrierPolicyChanged
            | EventKind::CarrierAddressChanged
    )
}

fn is_session_event(event: EventKind) -> bool {
    matches!(
        event,
        EventKind::SessionStateChanged | EventKind::SessionPeerAddressesChanged
    )
}

fn is_balancer_event(event: EventKind) -> bool {
    matches!(
        event,
        EventKind::BalancerMemberChanged | EventKind::BalancerProbeCompleted
    )
}

fn validate_webhook_template_context(rule: &WebhookRule) -> Result<(), ConfigError> {
    let fields = rule.target.template_fields();
    for field in fields {
        if !template_field_is_known(field) {
            return Err(ConfigError::Webhook(format!(
                "rule {:?} references unknown template field {field:?}",
                rule.name
            )));
        }
        for branch in &rule.when.branches {
            if branch
                .events
                .iter()
                .any(|event| !template_field_available(field, *event, &rule.when))
            {
                return Err(ConfigError::Webhook(format!(
                    "rule {:?} template field {field:?} is unavailable for one or more selected event types",
                    rule.name
                )));
            }
        }
    }
    Ok(())
}

fn template_field_available(
    field: &str,
    event: EventKind,
    matcher: &crate::webhook::EventMatcher,
) -> bool {
    let path_event = is_path_event(event);
    let carrier_event = matches!(
        event,
        EventKind::CarrierStateChanged
            | EventKind::CarrierPolicyChanged
            | EventKind::CarrierAddressChanged
    );
    let session_event = matches!(
        event,
        EventKind::SessionStateChanged | EventKind::SessionPeerAddressesChanged
    );
    let client_carrier = carrier_event
        && matcher.inbounds.is_empty()
        && (!matcher.outbounds.is_empty() || !matcher.paths.is_empty());
    let client_session =
        session_event && matcher.inbounds.is_empty() && !matcher.outbounds.is_empty();
    let server_event = (carrier_event || session_event) && !matcher.inbounds.is_empty();
    let balancer_event = is_balancer_event(event);
    let probe_event = matches!(
        event,
        EventKind::PathProbeCompleted | EventKind::BalancerProbeCompleted
    );
    if field == "schema_version" || field.starts_with("event.") || field.starts_with("subject.") {
        return true;
    }
    match field.split_once('.') {
        Some(("path", "name" | "outbound")) => path_event || client_carrier,
        Some(("path", _)) => path_event,
        Some(("outbound", "name")) => path_event || client_carrier || client_session,
        Some(("inbound", "name")) => server_event,
        Some(("carrier", "address_revision")) => event == EventKind::CarrierAddressChanged,
        Some(("carrier", "listen_path")) => carrier_event && server_event,
        Some(("carrier", "peer_usage")) => {
            matches!(
                event,
                EventKind::CarrierStateChanged | EventKind::CarrierPolicyChanged
            )
        }
        Some(("carrier", _)) => carrier_event,
        Some(("session", "id")) => session_event || carrier_event,
        Some(("session", "peer_ips")) => event == EventKind::SessionPeerAddressesChanged,
        Some(("session", "ready_carriers")) => event == EventKind::SessionStateChanged,
        Some(("session", "lifetime")) => session_event && server_event,
        Some(("session", _)) => session_event,
        Some(("probe", "id" | "started_at" | "completed_at" | "duration_s" | "state_at_start")) => {
            event == EventKind::PathProbeCompleted
        }
        Some(("probe", "elapsed_s")) => event == EventKind::BalancerProbeCompleted,
        Some(("probe", "trigger" | "outcome" | "applied")) => probe_event,
        Some(("probe", _)) => false,
        Some(("change", "from")) => matches!(
            event,
            EventKind::PathStateChanged
                | EventKind::PathPolicyChanged
                | EventKind::CarrierStateChanged
                | EventKind::CarrierPolicyChanged
                | EventKind::SessionStateChanged
                | EventKind::NodeStateChanged
                | EventKind::BalancerMemberChanged
        ),
        Some(("change", "to")) => matches!(
            event,
            EventKind::PathStateChanged
                | EventKind::PathPolicyChanged
                | EventKind::CarrierStateChanged
                | EventKind::CarrierPolicyChanged
                | EventKind::SessionStateChanged
                | EventKind::NodeStateChanged
                | EventKind::BalancerMemberChanged
        ),
        Some(("change", "before" | "after")) => matches!(
            event,
            EventKind::CarrierAddressChanged | EventKind::SessionPeerAddressesChanged
        ),
        Some(("change", "components")) => matches!(
            event,
            EventKind::CarrierAddressChanged | EventKind::SessionPeerAddressesChanged
        ),
        Some(("change", "skipped_revisions" | "coalesced")) => {
            event == EventKind::CarrierAddressChanged
        }
        Some(("change", "field")) => matches!(
            event,
            EventKind::CarrierPolicyChanged | EventKind::BalancerMemberChanged
        ),
        Some(("change", "cause")) => event == EventKind::BalancerMemberChanged,
        Some(("change", "reason")) => server_event,
        Some(("change", _)) => false,
        Some(("node", "state")) => event == EventKind::NodeStateChanged,
        Some(("balancer", "name")) => balancer_event,
        Some(("member", "outbound")) => balancer_event,
        _ => false,
    }
}

fn template_field_is_known(field: &str) -> bool {
    let allowed = match field.split_once('.') {
        Some(("event", leaf)) => matches!(
            leaf,
            "id" | "type"
                | "occurred_at"
                | "observed_at"
                | "process_boot_id"
                | "configuration_generation"
                | "subject_id"
                | "subject_sequence"
                | "reason"
                | "initial"
        ),
        Some(("subject", leaf)) => matches!(leaf, "id" | "sequence"),
        Some(("outbound", leaf)) => leaf == "name",
        Some(("inbound", leaf)) => leaf == "name",
        Some(("path", leaf)) => matches!(
            leaf,
            "name"
                | "outbound"
                | "state"
                | "transports"
                | "local_ips"
                | "ready_carriers"
                | "draining_carriers"
                | "policy"
                | "last_ready_at"
                | "interval_s"
                | "metrics"
        ),
        Some(("carrier", leaf)) => {
            matches!(
                leaf,
                "id" | "instance"
                    | "path_id"
                    | "configured_slot"
                    | "state"
                    | "transport"
                    | "listen_path"
                    | "address_revision"
                    | "peer_usage"
            ) || matches!(leaf, "local.ip" | "local.port" | "peer.ip" | "peer.port")
        }
        Some(("session", leaf)) => {
            matches!(leaf, "id" | "lifetime" | "peer_ips" | "ready_carriers")
        }
        Some(("probe", leaf)) => matches!(
            leaf,
            "id" | "trigger"
                | "started_at"
                | "completed_at"
                | "duration_s"
                | "state_at_start"
                | "outcome"
                | "applied"
                | "elapsed_s"
        ),
        Some(("change", leaf)) => {
            matches!(
                leaf,
                "from"
                    | "to"
                    | "before"
                    | "after"
                    | "components"
                    | "skipped_revisions"
                    | "coalesced"
                    | "field"
                    | "cause"
                    | "reason"
            ) || matches!(
                leaf,
                "before.ip" | "before.port" | "after.ip" | "after.port"
            )
        }
        Some(("node", leaf)) => leaf == "state",
        Some(("balancer", leaf)) => leaf == "name",
        Some(("member", leaf)) => leaf == "outbound",
        _ => field == "schema_version",
    };
    if !allowed {
        return false;
    }
    if field.starts_with("carrier.local.") || field.starts_with("carrier.peer.") {
        return matches!(field.rsplit('.').next(), Some("ip" | "port"));
    }
    field == "schema_version"
        || field.split('.').next().is_some_and(|root| {
            matches!(
                root,
                "event"
                    | "subject"
                    | "outbound"
                    | "inbound"
                    | "path"
                    | "carrier"
                    | "session"
                    | "probe"
                    | "change"
                    | "node"
                    | "balancer"
                    | "member"
            )
        })
}

fn validate_webhook_dependency_cycle(
    node: &NodeConfig,
    rule: &WebhookRule,
    outbounds: &HashMap<&str, &OutboundLeafConfig>,
    balancers: &HashMap<&str, &GatewayBalancerConfig>,
) -> Result<(), ConfigError> {
    let transition_driven = rule.when.branches.iter().any(|branch| {
        branch
            .events
            .iter()
            .any(|event| *event != EventKind::PathInterval)
    });
    if !transition_driven {
        return Ok(());
    }
    let observes_client_outbound = rule.when.branches.iter().any(|branch| {
        branch.events.iter().any(|event| {
            matches!(
                event,
                EventKind::PathStateChanged
                    | EventKind::PathPolicyChanged
                    | EventKind::PathProbeCompleted
                    | EventKind::CarrierStateChanged
                    | EventKind::CarrierPolicyChanged
                    | EventKind::CarrierAddressChanged
                    | EventKind::SessionStateChanged
                    | EventKind::SessionPeerAddressesChanged
            )
        })
    }) && rule.when.inbounds.is_empty();
    let observed_outbounds: HashSet<&str> = if !observes_client_outbound {
        HashSet::new()
    } else if rule.when.outbounds.is_empty() {
        node.outbounds
            .iter()
            .filter_map(|leaf| match leaf {
                OutboundLeafConfig::Mpp { id, .. } => Some(id.as_str()),
                OutboundLeafConfig::Local { .. } => None,
            })
            .collect()
    } else {
        rule.when.outbounds.iter().map(String::as_str).collect()
    };
    let compiled_dns = node
        .dns_policy
        .compile()
        .map_err(|error| ConfigError::DnsPolicy(error.to_string()))?;
    let mut dependencies: HashSet<&str> = HashSet::new();
    let mut pending_outbounds = Vec::new();
    let mut dependency_balancers: HashSet<&str> = HashSet::new();
    match &rule.target.egress {
        EgressRef::Outbound(id) => {
            dependencies.insert(id.as_str());
            pending_outbounds.push(id.as_str());
        }
        EgressRef::Balancer(id) => {
            dependency_balancers.insert(id.as_str());
            if let Some(balancer) = balancers.get(id.as_str()) {
                for member in &balancer.spec.members {
                    dependencies.insert(member.id.as_str());
                    pending_outbounds.push(member.id.as_str());
                }
            }
        }
    }

    let url_domain = webhook_domain_host(&rule.target.url.host);
    let mut pending_dns_plans = Vec::new();
    if let Some(domain) = &url_domain
        && webhook_target_resolves_domain_locally(&rule.target, node)
    {
        if let Some(plan) = &rule.target.dns_policy {
            pending_dns_plans.push(plan.clone());
        } else {
            pending_dns_plans.push(compiled_dns.select(domain).plan().id().clone());
        }
    }
    let mut visited_outbounds = HashSet::new();
    let mut visited_dns_plans = HashSet::new();
    while !pending_outbounds.is_empty() || !pending_dns_plans.is_empty() {
        while let Some(outbound_name) = pending_outbounds.pop() {
            if !visited_outbounds.insert(outbound_name) {
                continue;
            }
            if let Some(OutboundLeafConfig::Mpp { config, .. }) = outbounds.get(outbound_name) {
                for path in &config.paths {
                    if let Some(domain) = webhook_domain_host(&path.spec.endpoint.host) {
                        pending_dns_plans.push(compiled_dns.select(&domain).plan().id().clone());
                    }
                }
            }
        }
        while let Some(plan_id) = pending_dns_plans.pop() {
            if !visited_dns_plans.insert(plan_id.clone()) {
                continue;
            }
            let Some(plan) = node
                .dns_policy
                .spec
                .plans
                .iter()
                .find(|plan| plan.id == plan_id)
            else {
                continue;
            };
            let upstream_ids: HashSet<_> = plan.upstreams.iter().collect();
            for upstream in &node.dns_policy.spec.upstreams {
                if upstream_ids.contains(&upstream.id)
                    && let DnsEgressSpec::Outbound(id) = &upstream.egress
                {
                    dependencies.insert(id.as_str());
                    pending_outbounds.push(id.as_str());
                }
            }
        }
    }
    if let Some(cycle) = observed_outbounds
        .intersection(&dependencies)
        .copied()
        .next()
    {
        return Err(ConfigError::Webhook(format!(
            "rule {:?} observes MPP outbound {cycle:?} used by its own delivery or DNS dependency; choose an independent selector",
            rule.name
        )));
    }
    let observes_balancers = rule
        .when
        .branches
        .iter()
        .any(|branch| branch.events.iter().any(|event| is_balancer_event(*event)));
    let observed_balancers = if !observes_balancers {
        Vec::new()
    } else if rule.when.balancers.is_empty() {
        node.gateway_balancers
            .iter()
            .map(|balancer| balancer.id.as_str())
            .collect()
    } else {
        rule.when.balancers.iter().map(String::as_str).collect()
    };
    if observed_balancers
        .iter()
        .any(|name| dependency_balancers.contains(name))
    {
        return Err(ConfigError::Webhook(format!(
            "rule {:?} observes a balancer used by its own delivery dependency; choose an independent selector",
            rule.name
        )));
    }
    Ok(())
}

fn validate_synthetic_capture_tun_routes(
    ingresses: &[LocalIngressConfig],
    dns_policy: &CompiledDnsPolicy,
    activation: &crate::product::DnsActivation,
) -> Result<(), ConfigError> {
    let pools = dns_policy
        .synthetic_captures_for_activation(activation)
        .flat_map(|capture| {
            capture
                .ipv4_pool
                .map(IpNet::V4)
                .into_iter()
                .chain(capture.ipv6_pool.map(IpNet::V6))
        })
        .collect::<Vec<_>>();
    for ingress in ingresses {
        let IngressConfig::TunL4(tun) = &ingress.config else {
            continue;
        };
        let Some(managed) = tun.managed_vpn() else {
            continue;
        };
        for pool in pools.iter().copied() {
            if managed
                .excludes
                .iter()
                .any(|excluded| ip_nets_overlap(*excluded, pool))
            {
                return Err(ConfigError::DnsPolicy(format!(
                    "DNS synthetic-capture pool {pool} overlaps a managed VPN exclude"
                )));
            }
            if let crate::platform::RouteMode::Split(includes) = &managed.route_mode
                && !includes
                    .iter()
                    .any(|included| ip_net_contains(*included, pool))
            {
                return Err(ConfigError::DnsPolicy(format!(
                    "managed split VPN does not capture DNS synthetic-capture pool {pool}"
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
                "route {} references missing DNS policy {}",
                rule.id.as_str(),
                plan.as_str()
            )));
        }
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

fn validate_product_policy_targets(
    policy: &ProductPolicyConfig,
    leaves: &HashMap<OutboundId, NetworkSet>,
    balancers: &[GatewayBalancerConfig],
) -> Result<(), ConfigError> {
    for rule in &policy.routes {
        match rule.action.egress() {
            Some(EgressAction::Outbound(id)) if !leaves.contains_key(id) => {
                return Err(ConfigError::ProductPolicy(format!(
                    "route {} references missing outbound {}",
                    rule.id.as_str(),
                    id.as_str()
                )));
            }
            Some(EgressAction::Balancer(id))
                if !balancers.iter().any(|balancer| &balancer.id == id) =>
            {
                return Err(ConfigError::ProductPolicy(format!(
                    "route {} references missing balancer {}",
                    rule.id.as_str(),
                    id.as_str()
                )));
            }
            Some(EgressAction::Direct) => {
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

struct L4InboundReachability {
    id: InboundId,
    is_mpp: bool,
    has_source: bool,
    networks: NetworkSet,
    principals: HashSet<PrincipalId>,
}

fn validate_product_policy_reachability(
    policy: &ProductPolicyConfig,
    node: &NodeConfig,
    leaf_networks: &HashMap<OutboundId, NetworkSet>,
    balancers: &[GatewayBalancerConfig],
) -> Result<(), ConfigError> {
    let inbounds = l4_inbound_reachability(node)?;
    for rule in &policy.routes {
        for selected in &rule.matcher.inbounds {
            if !inbounds.iter().any(|inbound| &inbound.id == selected) {
                return Err(ConfigError::ProductPolicy(format!(
                    "route {} references unavailable L4 inbound {}",
                    rule.id, selected
                )));
            }
        }
        for principal in &rule.matcher.principals {
            if !inbounds.iter().any(|inbound| {
                route_selects_inbound(rule, inbound)
                    && route_can_match_inbound_network(rule, inbound)
                    && route_can_match_inbound_source(rule, inbound)
                    && inbound.principals.contains(principal)
            }) {
                return Err(ConfigError::ProductPolicy(format!(
                    "route {} principal {} is not reachable on any selected L4 inbound",
                    rule.id, principal
                )));
            }
        }

        if !inbounds
            .iter()
            .any(|inbound| route_can_match_inbound(rule, inbound))
        {
            return Err(ConfigError::ProductPolicy(format!(
                "route {} cannot match any configured L4 inbound after applying its inbound, principal, network, and source selectors",
                rule.id
            )));
        }

        let Some(egress) = rule.action.egress() else {
            continue;
        };
        let reachable_networks = inbounds
            .iter()
            .filter(|inbound| route_can_match_inbound(rule, inbound))
            .fold(NetworkSet::NONE, |networks, inbound| {
                networks.union(route_inbound_networks(rule, inbound))
            });
        let egress_networks = match egress {
            EgressAction::Outbound(id) => {
                leaf_networks.get(id).copied().unwrap_or(NetworkSet::NONE)
            }
            EgressAction::Balancer(id) => balancers
                .iter()
                .find(|balancer| &balancer.id == id)
                .map(|balancer| {
                    balancer
                        .spec
                        .members
                        .iter()
                        .fold(NetworkSet::NONE, |networks, member| {
                            networks.union(member.networks)
                        })
                })
                .unwrap_or(NetworkSet::NONE),
            EgressAction::Direct => NetworkSet::TCP_UDP,
        };
        for network in [Network::Tcp, Network::Udp] {
            if reachable_networks.contains(network) && !egress_networks.contains(network) {
                return Err(ConfigError::ProductPolicy(format!(
                    "route {} can match {network} but its selected egress does not support {network}",
                    rule.id
                )));
            }
        }

        if inbounds
            .iter()
            .any(|inbound| inbound.is_mpp && route_can_match_inbound(rule, inbound))
            && route_egress_contains_mpp(egress, node, balancers)
        {
            return Err(ConfigError::ProductPolicy(format!(
                "route {} is reachable from an MPP inbound and cannot select an MPP outbound",
                rule.id
            )));
        }
    }
    Ok(())
}

fn l4_inbound_reachability(node: &NodeConfig) -> Result<Vec<L4InboundReachability>, ConfigError> {
    let anonymous = PrincipalId::parse("anonymous")
        .map_err(|error| ConfigError::ProductPolicy(error.to_string()))?;
    let mut inbounds = Vec::with_capacity(node.local_ingresses.len() + node.servers.len());
    for ingress in &node.local_ingresses {
        let (networks, proxy_auth) = match &ingress.config {
            IngressConfig::Socks5 { proxy_auth, .. } | IngressConfig::Mixed { proxy_auth, .. } => {
                (NetworkSet::TCP_UDP, Some(proxy_auth))
            }
            IngressConfig::HttpConnect { proxy_auth, .. } => (NetworkSet::TCP, Some(proxy_auth)),
            IngressConfig::TcpForward(_) => (NetworkSet::TCP, None),
            IngressConfig::UdpForward(_) => (NetworkSet::UDP, None),
            IngressConfig::MixedForward(_) | IngressConfig::TunL4(_) => (NetworkSet::TCP_UDP, None),
        };
        let principals = match proxy_auth {
            Some(auth) if auth.is_required() => auth.principals().cloned().collect(),
            Some(_) | None => HashSet::from([anonymous.clone()]),
        };
        inbounds.push(L4InboundReachability {
            id: InboundId::parse(&ingress.name)
                .map_err(|error| ConfigError::ProductPolicy(error.to_string()))?,
            is_mpp: false,
            has_source: true,
            networks,
            principals,
        });
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ConfigError::ProductPolicy("system clock is before Unix epoch".to_string()))?
        .as_secs();
    for server in node.servers.iter().filter(|server| server.tun_l3.is_none()) {
        let principals = server
            .security
            .credential_authority
            .credentials()
            .into_iter()
            .filter(|credential| {
                !credential.revoked()
                    && credential
                        .expires_at_unix_secs()
                        .is_none_or(|expiry| now < expiry)
            })
            .map(|credential| credential.principal().clone())
            .collect();
        inbounds.push(L4InboundReachability {
            id: InboundId::parse(&server.name)
                .map_err(|error| ConfigError::ProductPolicy(error.to_string()))?,
            is_mpp: true,
            has_source: false,
            networks: NetworkSet::TCP_UDP,
            principals,
        });
    }
    Ok(inbounds)
}

fn route_selects_inbound(rule: &RouteRuleSpec, inbound: &L4InboundReachability) -> bool {
    rule.matcher.inbounds.is_empty() || rule.matcher.inbounds.contains(&inbound.id)
}

fn route_can_match_inbound_source(rule: &RouteRuleSpec, inbound: &L4InboundReachability) -> bool {
    inbound.has_source
        || (rule.matcher.source_cidrs.is_empty() && rule.matcher.source_ports.is_empty())
}

fn route_can_match_inbound_network(rule: &RouteRuleSpec, inbound: &L4InboundReachability) -> bool {
    rule.matcher.networks.is_empty()
        || rule
            .matcher
            .networks
            .iter()
            .any(|network| inbound.networks.contains(*network))
}

fn route_can_match_inbound(rule: &RouteRuleSpec, inbound: &L4InboundReachability) -> bool {
    route_selects_inbound(rule, inbound)
        && route_can_match_inbound_source(rule, inbound)
        && route_can_match_inbound_network(rule, inbound)
        && (rule.matcher.principals.is_empty()
            || rule
                .matcher
                .principals
                .iter()
                .any(|principal| inbound.principals.contains(principal)))
        && !inbound.principals.is_empty()
}

fn route_inbound_networks(rule: &RouteRuleSpec, inbound: &L4InboundReachability) -> NetworkSet {
    if rule.matcher.networks.is_empty() {
        inbound.networks
    } else {
        rule.matcher
            .networks
            .iter()
            .filter(|network| inbound.networks.contains(**network))
            .fold(NetworkSet::NONE, |networks, network| {
                networks.union((*network).into())
            })
    }
}

fn route_egress_contains_mpp(
    egress: &EgressAction,
    node: &NodeConfig,
    balancers: &[GatewayBalancerConfig],
) -> bool {
    let is_mpp = |id: &OutboundId| {
        node.outbounds.iter().any(
            |outbound| matches!(outbound, OutboundLeafConfig::Mpp { id: leaf, .. } if leaf == id),
        )
    };
    match egress {
        EgressAction::Outbound(id) => is_mpp(id),
        EgressAction::Balancer(id) => balancers
            .iter()
            .find(|balancer| &balancer.id == id)
            .is_some_and(|balancer| {
                balancer
                    .spec
                    .members
                    .iter()
                    .any(|member| is_mpp(&member.id))
            }),
        EgressAction::Direct => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionConfig {
    /// Absolute Product resource-lifetime ceiling for carrierless stream
    /// retention and graceful TCP carrier retirement. Healthy idle streams do
    /// not consume it.
    pub retention_timeout: Duration,
}

/// Established Product flow lifetime, independent of carrier and session
/// liveness. `None` explicitly disables payload-idle retirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductFlowConfig {
    pub idle_timeout: Option<Duration>,
}

impl Default for ProductFlowConfig {
    fn default() -> Self {
        Self {
            idle_timeout: DEFAULT_PRODUCT_FLOW_IDLE_TIMEOUT,
        }
    }
}

impl ProductFlowConfig {
    pub fn validate(self) -> Result<(), ConfigError> {
        if self.idle_timeout.is_some_and(|timeout| timeout.is_zero()) {
            return Err(ConfigError::ProductFlowIdleTimeoutZero);
        }
        Ok(())
    }
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
    pub supervise: bool,
    pub restart_backoff: Duration,
    pub restart_max_backoff: Duration,
    pub max_restarts: Option<u32>,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
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
    /// Endpoint-local cap applied independently to active authentication work
    /// and silent TCP Noise rejection retention.
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

/// Generation-scoped forwarding family.
///
/// L4 retains the Product stream/datagram forwarding graph. L3 selects the
/// raw authenticated IP-packet service. The two families are deliberately
/// exclusive so transport policy can evolve independently.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ForwardingMode {
    #[default]
    L4,
    L3,
}

impl ForwardingMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::L4 => "l4",
            Self::L3 => "l3",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeConfig {
    /// Selects one forwarding family for this runtime generation. TOML derives
    /// it from the inbound protocols; the simple CLI constructs L4 directly.
    pub forwarding_mode: ForwardingMode,
    /// One canonical namespace for MPP and native outbound leaves.
    pub outbounds: Vec<OutboundLeafConfig>,
    /// Product balancers over compatible leaf outbounds. They never
    /// merge MPP carrier paths or own Core scheduling state.
    pub gateway_balancers: Vec<GatewayBalancerConfig>,
    /// Product-owned local ingress surfaces. They are intentionally not owned
    /// by any one carrier/path group because routing selects that group per
    /// normalized flow.
    pub local_ingresses: Vec<LocalIngressConfig>,
    /// Raw layer-3 packet services. These bind directly to one MPP outbound
    /// and never pass through Product's L4 routing or DNS policy.
    pub tun_l3_ingresses: Vec<NamedTunL3Config>,
    /// Immutable new-flow policy generation for local SOCKS/HTTP/TUN traffic.
    pub product_policy: Option<ProductPolicyConfig>,
    /// Immutable named split-DNS policy used whenever this node needs address
    /// evidence. Upstream transport may be system, direct, or routed.
    pub dns_policy: DnsPolicyConfig,
    /// Optional bounded event publisher and outbound notification rules.
    pub webhooks: WebhookConfig,
    pub servers: Vec<MppInboundConfig>,
}

/// Selector-reachable runtime graph compiled from one fully validated node.
///
/// Catalog entries outside these sets remain part of configuration validation,
/// but runtime and managed-VPN preparation must not instantiate or publish
/// host state for them.
#[derive(Debug)]
pub(crate) struct ActiveNodeGraph {
    pub(crate) dns_policy: CompiledDnsPolicy,
    pub(crate) dns_activation: DnsActivation,
    active_outbounds: HashSet<OutboundId>,
    active_balancers: HashSet<BalancerId>,
}

impl ActiveNodeGraph {
    pub(crate) fn contains_outbound(&self, outbound: &OutboundId) -> bool {
        self.active_outbounds.contains(outbound)
    }

    pub(crate) fn contains_balancer(&self, balancer: &BalancerId) -> bool {
        self.active_balancers.contains(balancer)
    }
}

impl NodeConfig {
    /// Compile the active DNS and egress closure without mutating the complete
    /// configuration catalog. Callers use this only after `AppConfig::validate`
    /// has checked every definition, including inactive ones.
    pub(crate) fn compile_active_graph(&self) -> Result<ActiveNodeGraph, DnsCompileError> {
        let mut dns_roots = self
            .product_policy
            .iter()
            .flat_map(|policy| &policy.routes)
            .filter_map(|rule| rule.action.dns_plan().cloned())
            .collect::<Vec<_>>();
        if self.webhooks.is_enabled() {
            let compiled_for_selection = self.dns_policy.compile()?;
            for rule in &self.webhooks.rules {
                if let Some(domain) = webhook_domain_host(&rule.target.url.host)
                    && webhook_target_resolves_domain_locally(&rule.target, self)
                {
                    if let Some(plan) = &rule.target.dns_policy {
                        dns_roots.push(plan.clone());
                    } else {
                        dns_roots.push(compiled_for_selection.select(&domain).plan().id().clone());
                    }
                }

                let mut selected_outbounds = Vec::new();
                match &rule.target.egress {
                    EgressRef::Outbound(id) => selected_outbounds.push(id.as_str()),
                    EgressRef::Balancer(id) => {
                        if let Some(balancer) = self
                            .gateway_balancers
                            .iter()
                            .find(|balancer| balancer.id == *id)
                        {
                            selected_outbounds.extend(
                                balancer
                                    .spec
                                    .members
                                    .iter()
                                    .map(|member| member.id.as_str()),
                            );
                        }
                    }
                }
                for outbound_name in selected_outbounds {
                    if let Some(OutboundLeafConfig::Mpp { config, .. }) = self
                        .outbounds
                        .iter()
                        .find(|outbound| outbound.id().as_str() == outbound_name)
                    {
                        for path in &config.paths {
                            if let Some(domain) = webhook_domain_host(&path.spec.endpoint.host) {
                                dns_roots.push(
                                    compiled_for_selection.select(&domain).plan().id().clone(),
                                );
                            }
                        }
                    }
                }
            }
        }
        let (dns_policy, dns_activation) = self.dns_policy.compile_active(dns_roots.iter())?;

        let mut active_outbounds = HashSet::new();
        let mut active_balancers = HashSet::new();
        if let Some(policy) = &self.product_policy {
            for egress in policy.routes.iter().filter_map(|rule| rule.action.egress()) {
                match egress {
                    EgressAction::Outbound(id) => {
                        active_outbounds.insert(id.clone());
                    }
                    EgressAction::Balancer(id) => {
                        active_balancers.insert(id.clone());
                    }
                    EgressAction::Direct => {}
                }
            }
        }
        for rule in &self.webhooks.rules {
            match &rule.target.egress {
                EgressRef::Outbound(id) => {
                    active_outbounds.insert(id.clone());
                }
                EgressRef::Balancer(id) => {
                    active_balancers.insert(id.clone());
                }
            }
        }
        for upstream in dns_policy.upstreams_for_activation(&dns_activation) {
            if let DnsEgressSpec::Outbound(id) = upstream.egress() {
                active_outbounds.insert(id.clone());
            }
        }
        active_outbounds.extend(
            self.tun_l3_ingresses
                .iter()
                .map(|ingress| ingress.config.outbound.clone()),
        );
        for balancer in self
            .gateway_balancers
            .iter()
            .filter(|balancer| active_balancers.contains(&balancer.id))
        {
            active_outbounds.extend(balancer.spec.members.iter().map(|member| member.id.clone()));
        }

        Ok(ActiveNodeGraph {
            dns_policy,
            dns_activation,
            active_outbounds,
            active_balancers,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayBalancerConfig {
    /// The operator-facing `[[balancers]].name`, compiled once into a typed
    /// reference. It is not a generated ordinal or an MPP wire identifier.
    pub id: BalancerId,
    pub generation: u64,
    pub spec: GatewayBalancerSpec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductPolicyConfig {
    pub generation: u64,
    pub routes: Vec<RouteRuleSpec>,
}

impl ProductPolicyConfig {
    pub fn compile(&self) -> Result<ProductPolicyGeneration, ProductPolicyCompileError> {
        ProductPolicyGeneration::compile(self.generation, self.routes.clone())
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
        let upstream = DnsUpstreamId::parse("system").expect("static DNS server ID");
        let plan = DnsPlanId::parse("default").expect("static DNS policy ID");
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
                override_records: Vec::new(),
                synthetic_captures: Vec::new(),
                default_plan: plan,
            },
        }
    }

    pub fn compile(&self) -> Result<CompiledDnsPolicy, crate::product::DnsCompileError> {
        CompiledDnsPolicy::compile(self.generation, self.spec.clone())
    }

    /// Compile the complete catalog, then freeze the selector-reachable DNS
    /// closure for this runtime generation. Split-DNS default/rule roots are
    /// intrinsic; routing contributes only its explicit policy selectors.
    pub fn compile_active<'a>(
        &self,
        route_plans: impl IntoIterator<Item = &'a crate::product::DnsPlanId>,
    ) -> Result<(CompiledDnsPolicy, crate::product::DnsActivation), crate::product::DnsCompileError>
    {
        let compiled = self.compile()?;
        let activation = compiled.activate(route_plans)?;
        Ok((compiled, activation))
    }
}

impl Default for DnsPolicyConfig {
    fn default() -> Self {
        Self::system_default()
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
    /// Permits this MPP peer to request a sanitized local path snapshot.
    /// The management-global override may also enable it process-wide.
    pub allow_peer_diagnostics: bool,
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
pub struct NamedTunL3Config {
    pub name: String,
    pub config: crate::ingress::TunL3IngressConfig,
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
    /// Independently pinned QUIC identity and TLS-fallback TCP identity.
    /// Optional shared transport protection never derives from an MPP client
    /// credential.
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
    /// Named carrier listen/bind paths owned by this MPP inbound.
    pub paths: Vec<NamedPathConfig>,
    /// Security scoped to peers that join this MPP inbound.
    pub security: ServerSecurityConfig,
    /// QUIC identity and TLS-fallback TCP identity shared by this MPP inbound.
    pub tls: TcpServerTlsConfig,
    /// MPP sender behavior for streams accepted by this inbound path group.
    pub performance: MppPerformanceConfig,
    /// Principals allowed to request a sanitized path snapshot from this
    /// inbound. Process-wide management policy can still override this gate.
    pub peer_diagnostics_principals: PeerDiagnosticsPrincipalPolicy,
    /// Optional first-class layer-3 packet service for authenticated peers.
    /// Host routing, DNS, firewall, and NAT remain external policy.
    pub tun_l3: Option<crate::product::TunL3AddressPlan>,
}

/// Endpoint-local authorization for incoming MPP peer diagnostics.
///
/// `All` deliberately remains open to principals admitted by later authority
/// publications. A concrete list is frozen with the configuration generation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum PeerDiagnosticsPrincipalPolicy {
    All,
    #[default]
    Deny,
    Selected(Vec<PrincipalId>),
}

impl PeerDiagnosticsPrincipalPolicy {
    pub fn selected(principals: Vec<PrincipalId>) -> Self {
        if principals.is_empty() {
            Self::Deny
        } else {
            Self::Selected(principals)
        }
    }

    pub const fn has_authorized_principals(&self) -> bool {
        !matches!(self, Self::Deny)
    }

    pub fn allows(&self, principal: &PrincipalId) -> bool {
        match self {
            Self::All => true,
            Self::Deny => false,
            Self::Selected(principals) => principals.contains(principal),
        }
    }
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
    let carrier_slots = client.paths.iter().fold(0usize, |total, path| {
        total.saturating_add(
            path.spec
                .tcp_carrier_range()
                .map_or(1, |range| usize::from(range.max())),
        )
    });
    if carrier_slots > resources.max_paths {
        return Err(ConfigError::TooManyPaths {
            actual: carrier_slots,
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
    tun_l3_ingresses: &[NamedTunL3Config],
    mpp_inbounds: &[MppInboundConfig],
) -> Result<(), ConfigError> {
    let mut seen =
        HashSet::with_capacity(local_ingresses.len() + tun_l3_ingresses.len() + mpp_inbounds.len());
    for name in local_ingresses
        .iter()
        .map(|inbound| inbound.name.as_str())
        .chain(tun_l3_ingresses.iter().map(|inbound| inbound.name.as_str()))
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

fn validate_tun_l3_ingresses(
    ingresses: &[NamedTunL3Config],
    outbounds: &[OutboundLeafConfig],
) -> Result<(), ConfigError> {
    let mut selected = HashSet::with_capacity(ingresses.len());
    for ingress in ingresses {
        let outbound = &ingress.config.outbound;
        let Some(leaf) = outbounds.iter().find(|leaf| leaf.id() == outbound) else {
            return Err(ConfigError::TunL3OutboundMissing(
                outbound.as_str().to_string(),
            ));
        };
        if !matches!(leaf, OutboundLeafConfig::Mpp { .. }) {
            return Err(ConfigError::TunL3OutboundNotMpp(
                outbound.as_str().to_string(),
            ));
        }
        if !selected.insert(outbound) {
            return Err(ConfigError::TunL3OutboundBoundTwice(
                outbound.as_str().to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_packet_device_names(node: &NodeConfig) -> Result<(), ConfigError> {
    let tun_l4_names = node.local_ingresses.iter().filter_map(|ingress| {
        if let IngressConfig::TunL4(tun) = &ingress.config {
            tun.interface_name.as_deref()
        } else {
            None
        }
    });
    let tun_l3_client_names = node
        .tun_l3_ingresses
        .iter()
        .filter_map(|ingress| ingress.config.interface_name.as_deref());
    let tun_l3_server_names = node
        .servers
        .iter()
        .filter_map(|server| server.tun_l3.as_ref()?.interface_name());
    let mut seen = HashSet::new();
    for name in tun_l4_names
        .chain(tun_l3_client_names)
        .chain(tun_l3_server_names)
    {
        if name.trim().is_empty() {
            return Err(ConfigError::PacketDeviceNameEmpty);
        }
        if !seen.insert(name) {
            return Err(ConfigError::DuplicatePacketDeviceName(name.to_string()));
        }
    }
    Ok(())
}

fn validate_forwarding_mode(node: &NodeConfig) -> Result<(), ConfigError> {
    match node.forwarding_mode {
        ForwardingMode::L4 => {
            if !node.tun_l3_ingresses.is_empty()
                || node.servers.iter().any(|server| server.tun_l3.is_some())
            {
                return Err(ConfigError::L4ContainsTunL3);
            }
        }
        ForwardingMode::L3 => {
            if node.product_policy.is_some() {
                return Err(ConfigError::L3ContainsRouting);
            }
            if let Some(inbound) = node.local_ingresses.first() {
                return Err(ConfigError::L3ContainsL4Inbound(inbound.name.clone()));
            }
            if let Some(server) = node.servers.iter().find(|server| server.tun_l3.is_none()) {
                return Err(ConfigError::L3ServerMissingTunL3(server.name.clone()));
            }
            if node.tun_l3_ingresses.is_empty() && node.servers.is_empty() {
                return Err(ConfigError::L3ServiceRequired);
            }
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
        .any(|path| path.spec.metadata.max_datagram_payload_bytes.is_some())
    {
        return Err(ConfigError::ServerPathMaxDatagramPayload);
    }
    if server
        .paths
        .iter()
        .any(|path| path.spec.metadata.tcp_carriers.is_some())
    {
        return Err(ConfigError::ServerTcpCarrierRange);
    }
    if server
        .paths
        .iter()
        .any(|path| path.spec.metadata.port_hop_interval_ms.is_some())
    {
        return Err(ConfigError::ServerPathPortRotation);
    }
    if server
        .paths
        .iter()
        .any(|path| !path.spec.endpoint.ports().is_single())
    {
        return Err(ConfigError::ServerPathPortRange);
    }
    validate_server_security_config(&server.security)?;
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
    if security.auth_freshness_window.subsec_nanos() != 0 {
        return Err(ConfigError::AuthFreshnessWindowSubsecond);
    }
    Ok(())
}

fn validate_server_security_config(security: &ServerSecurityConfig) -> Result<(), ConfigError> {
    if security.auth_freshness_window.is_zero() {
        return Err(ConfigError::AuthFreshnessWindowZero);
    }
    if security.auth_freshness_window.subsec_nanos() != 0 {
        return Err(ConfigError::AuthFreshnessWindowSubsecond);
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
        IngressConfig::Socks5 { listen, .. }
        | IngressConfig::HttpConnect { listen, .. }
        | IngressConfig::Mixed { listen, .. } => {
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
        IngressConfig::MixedForward(config) => {
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
    AuthFreshnessWindowSubsecond,
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
    ProductFlowIdleTimeoutZero,
    NoIngresses,
    NoListenAddresses,
    TooManyPaths { actual: usize, limit: usize },
    PathNameInvalid,
    DuplicatePathName(String),
    PathProbeIntervalZero,
    PathProbeTimeoutZero,
    QuicTlsServerNameRequiresDns,
    ServerPathSourceBinding,
    ServerPathMaxDatagramPayload,
    ServerPathPortRotation,
    ServerPathPortRange,
    ServerTcpCarrierRange,
    TunAddressRequired,
    TunIpv4PrefixInvalid,
    TunIpv6PrefixInvalid,
    TunMtuTooSmall,
    TunIpv6MtuTooSmall,
    TunDnsTtlZero,
    TunDnsResolverPortZero,
    ManagedVpn(String),
    MultipleManagedTunInbounds { actual: usize },
    L4ContainsTunL3,
    L3ContainsRouting,
    L3ContainsL4Inbound(String),
    L3ServerMissingTunL3(String),
    L3ServiceRequired,
    TunL3OutboundMissing(String),
    TunL3OutboundNotMpp(String),
    TunL3OutboundBoundTwice(String),
    PacketDeviceNameEmpty,
    DuplicatePacketDeviceName(String),
    DnsPolicy(String),
    OutboundConnectTimeoutZero,
    InboundNameInvalid,
    DuplicateInboundName(String),
    LocalIngressRoutingRequired,
    ProductPolicy(String),
    Webhook(String),
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
                write!(f, "flow-event logging requires at least log level info")
            }
            Self::AuthFreshnessWindowZero => {
                write!(f, "auth freshness window must be greater than zero")
            }
            Self::AuthFreshnessWindowSubsecond => write!(
                f,
                "auth freshness window must use whole seconds because authentication timestamps use Unix seconds"
            ),
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
                write!(f, "max repair bytes must be at least max payload bytes")
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
            Self::ProductFlowIdleTimeoutZero => {
                write!(
                    f,
                    "Product flow idle timeout must be disabled or greater than zero"
                )
            }
            Self::NoIngresses => write!(f, "at least one client ingress is required"),
            Self::NoListenAddresses => {
                write!(f, "proxy ingress requires at least one listen address")
            }
            Self::TooManyPaths { actual, limit } => {
                write!(f, "{actual} paths configured, limit is {limit}")
            }
            Self::PathNameInvalid => {
                write!(f, "path name must use canonical configuration-name text")
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
                write!(f, "source-address is valid only for client carrier paths")
            }
            Self::ServerPathMaxDatagramPayload => write!(
                f,
                "max-datagram-payload-bytes is valid only for client QUIC carrier paths"
            ),
            Self::ServerPathPortRotation => write!(
                f,
                "port-rotation-interval-s is valid only for client carrier paths"
            ),
            Self::ServerPathPortRange => write!(
                f,
                "server carrier paths require one listener port; forward any advertised port range to that listener"
            ),
            Self::ServerTcpCarrierRange => write!(
                f,
                "max-tcp-carriers is client endpoint policy and cannot configure a server listener"
            ),
            Self::TunAddressRequired => write!(f, "TUN L4 ingress requires IPv4 or IPv6 address"),
            Self::TunIpv4PrefixInvalid => write!(f, "TUN IPv4 prefix must be in 0..=32"),
            Self::TunIpv6PrefixInvalid => write!(f, "TUN IPv6 prefix must be in 0..=128"),
            Self::TunMtuTooSmall => write!(f, "TUN MTU must be at least 576 bytes"),
            Self::TunIpv6MtuTooSmall => write!(f, "TUN IPv6 MTU must be at least 1280 bytes"),
            Self::TunDnsTtlZero => write!(f, "TUN DNS TTL must be greater than zero"),
            Self::TunDnsResolverPortZero => write!(f, "TUN DNS redirect port must be nonzero"),
            Self::ManagedVpn(error) => {
                write!(f, "invalid managed VPN configuration: {error}")
            }
            Self::MultipleManagedTunInbounds { actual } => write!(
                f,
                "node config defines {actual} managed TUN inbounds; at most one may own host VPN state"
            ),
            Self::L4ContainsTunL3 => write!(
                f,
                "forwarding mode l4 cannot contain a TUN-L3 client or server service"
            ),
            Self::L3ContainsRouting => {
                write!(f, "forwarding mode l3 cannot contain an L4 routing policy")
            }
            Self::L3ContainsL4Inbound(name) => {
                write!(f, "forwarding mode l3 cannot contain L4 inbound {name:?}")
            }
            Self::L3ServerMissingTunL3(name) => write!(
                f,
                "forwarding mode l3 requires MPP inbound {name:?} to define its TUN-L3 service"
            ),
            Self::L3ServiceRequired => write!(
                f,
                "forwarding mode l3 requires at least one TUN-L3 client or server service"
            ),
            Self::TunL3OutboundMissing(name) => {
                write!(f, "TUN-L3 references missing outbound {name:?}")
            }
            Self::TunL3OutboundNotMpp(name) => {
                write!(f, "TUN-L3 outbound {name:?} must use protocol mpp")
            }
            Self::TunL3OutboundBoundTwice(name) => write!(
                f,
                "MPP outbound {name:?} is bound by more than one TUN-L3 ingress"
            ),
            Self::PacketDeviceNameEmpty => {
                write!(f, "packet-device interface name must not be empty")
            }
            Self::DuplicatePacketDeviceName(name) => {
                write!(
                    f,
                    "packet-device interface name {name:?} is configured more than once"
                )
            }
            Self::DnsPolicy(error) => write!(f, "invalid DNS policy: {error}"),
            Self::OutboundConnectTimeoutZero => {
                write!(f, "outbound connect timeout must be greater than zero")
            }
            Self::InboundNameInvalid => {
                write!(f, "inbound name must use canonical configuration-name text")
            }
            Self::DuplicateInboundName(name) => {
                write!(f, "duplicate inbound name {name:?}")
            }
            Self::LocalIngressRoutingRequired => {
                write!(f, "local inbounds require a compiled routing policy")
            }
            Self::ProductPolicy(error) => write!(f, "{error}"),
            Self::Webhook(error) => write!(f, "invalid webhook configuration: {error}"),
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
#[path = "tests_model.rs"]
mod tests;
