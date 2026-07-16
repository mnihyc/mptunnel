use super::{
    AppConfig, CipherSuite, ClientConfig, ClientPathConfig, CommandConfig, ConfigError,
    DEFAULT_AUTH_FRESHNESS_WINDOW_SECONDS, DEFAULT_OUTBOUND_CONNECT_TIMEOUT_MS,
    DEFAULT_PATH_PROBE_INTERVAL_MS, DEFAULT_PATH_PROBE_TIMEOUT_MS, DEFAULT_RESTART_BACKOFF_MS,
    DEFAULT_RESTART_MAX_BACKOFF_MS, LocalIngressConfig, ManagementConfig, MppPerformanceConfig,
    NodeConfig, ResourceLimits, RouteTarget, RouteTargetKind, SecurityConfig, SecurityPolicyError,
    ServerConfig, ServiceConfig, SharedSecret,
};
use crate::ingress::tun::{
    DEFAULT_TUN_DNS_TTL_MS, DEFAULT_TUN_IPV4, DEFAULT_TUN_IPV4_PREFIX, DEFAULT_TUN_MTU, TunL4Config,
};
use crate::ingress::{IngressConfig, ProxyAuthConfig};
use crate::outbound::{DnsConfig, DnsIpStrategy, OutboundConfig, OutboundRouteMember};
use crate::transport::{EndpointParseError, PathSpec, PathSpecParseError};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::time::Duration;

pub const DEFAULT_CONFIG_PATH: &str = "config.toml";

pub fn load_config_toml(path: impl AsRef<Path>) -> Result<AppConfig, ConfigFileError> {
    let contents = std::fs::read_to_string(path.as_ref()).map_err(ConfigFileError::Io)?;
    load_config_toml_str(&contents)
}

pub fn load_config_toml_str(contents: &str) -> Result<AppConfig, ConfigFileError> {
    let file = toml::from_str::<FileConfig>(contents).map_err(ConfigFileError::Toml)?;
    file.into_config()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    #[serde(default = "default_log_level")]
    log_level: String,
    #[serde(default)]
    check_config: bool,
    #[serde(default)]
    service: ServiceFileConfig,
    #[serde(default)]
    resources: ResourceFileConfig,
    #[serde(default)]
    management: ManagementFileConfig,
    #[serde(default)]
    inbounds: Vec<InboundFileConfig>,
    #[serde(default)]
    outbounds: Vec<OutboundFileConfig>,
    #[serde(default)]
    routing: RoutingFileConfig,
}

impl FileConfig {
    fn into_config(self) -> Result<AppConfig, ConfigFileError> {
        if self.inbounds.is_empty() {
            return Err(ConfigFileError::NoRuntimeServices);
        }
        let mut parsed_outbounds = parse_outbounds(self.outbounds)?;
        apply_routing(self.routing, &mut parsed_outbounds)?;
        let (clients, servers) = build_node_services(self.inbounds, parsed_outbounds)?;
        let representative_security = clients
            .first()
            .map(|client| client.security.clone())
            .or_else(|| servers.first().map(|server| server.security.clone()))
            .ok_or(ConfigFileError::NoRuntimeServices)?;
        let config = AppConfig {
            log_level: self.log_level,
            check_config: self.check_config,
            service: self.service.into_config(),
            resources: self.resources.into_limits(),
            management: self.management.into_config(),
            security: representative_security,
            command: CommandConfig::Node(NodeConfig { clients, servers }),
        };
        config.validate()?;
        Ok(config)
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServiceFileConfig {
    #[serde(default)]
    service_mode: bool,
    #[serde(default)]
    supervise: bool,
    restart_backoff_ms: Option<u64>,
    restart_max_backoff_ms: Option<u64>,
    max_restarts: Option<u32>,
}

impl ServiceFileConfig {
    fn into_config(self) -> ServiceConfig {
        ServiceConfig {
            service_mode: self.service_mode,
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
    token: Option<String>,
    #[serde(default)]
    dashboard: bool,
    #[serde(default)]
    allow_peer_diagnostics: bool,
}

impl ManagementFileConfig {
    fn into_config(self) -> ManagementConfig {
        ManagementConfig {
            listen: self.listen,
            token: self.token,
            dashboard: self.dashboard,
            allow_peer_diagnostics: self.allow_peer_diagnostics,
        }
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
    max_datagram_queue_bytes: Option<usize>,
    max_path_flight_bytes: Option<usize>,
    max_reliable_relay_chunk_bytes: Option<usize>,
    tcp_path_heartbeat_interval_ms: Option<u64>,
    tcp_path_heartbeat_timeout_ms: Option<u64>,
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
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SecurityFileConfig {
    secret: Option<String>,
    #[serde(default)]
    cipher: CipherFileValue,
    auth_freshness_window_seconds: Option<u64>,
}

impl SecurityFileConfig {
    fn into_config(self) -> Result<SecurityConfig, ConfigFileError> {
        let secret = SharedSecret::new(
            self.secret
                .ok_or(ConfigFileError::Security(
                    SecurityPolicyError::MissingSecret,
                ))?
                .into_bytes(),
        )?;
        let auth_freshness_window = Duration::from_secs(
            self.auth_freshness_window_seconds
                .unwrap_or(DEFAULT_AUTH_FRESHNESS_WINDOW_SECONDS),
        );
        let cipher = self.cipher.into();
        let config = SecurityConfig::encrypted_with_cipher(secret, cipher)
            .with_auth_freshness_window(auth_freshness_window);
        Ok(config)
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum CipherFileValue {
    #[default]
    #[serde(rename = "aes-256-gcm", alias = "aes256-gcm")]
    Aes256Gcm,
    Chacha20Poly1305,
}

impl From<CipherFileValue> for CipherSuite {
    fn from(value: CipherFileValue) -> Self {
        match value {
            CipherFileValue::Aes256Gcm => Self::Aes256Gcm,
            CipherFileValue::Chacha20Poly1305 => Self::Chacha20Poly1305,
        }
    }
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

    fn is_explicit(self) -> bool {
        self.extra_traffic_hint_percent.is_some()
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "protocol", rename_all = "kebab-case", deny_unknown_fields)]
enum InboundFileConfig {
    Socks5 {
        tag: Option<String>,
        #[serde(default)]
        listen: Vec<SocketAddr>,
        outbound: Option<String>,
        balancer: Option<String>,
        auth: Option<ProxyAuthFileConfig>,
    },
    #[serde(rename = "http", alias = "http-connect")]
    HttpConnect {
        tag: Option<String>,
        #[serde(default)]
        listen: Vec<SocketAddr>,
        outbound: Option<String>,
        balancer: Option<String>,
        auth: Option<ProxyAuthFileConfig>,
    },
    Tun {
        tag: Option<String>,
        outbound: Option<String>,
        balancer: Option<String>,
        name: Option<String>,
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
        dns_resolvers: Vec<SocketAddr>,
        dns_ttl_ms: Option<u32>,
    },
    Mpp {
        tag: Option<String>,
        security: SecurityFileConfig,
        #[serde(default)]
        performance: MppPerformanceFileConfig,
        #[serde(default)]
        endpoints: Vec<String>,
        outbound: Option<String>,
        balancer: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProxyAuthFileConfig {
    username: Option<String>,
    password: Option<String>,
}

impl ProxyAuthFileConfig {
    fn into_config(self) -> Result<ProxyAuthConfig, ConfigFileError> {
        match (self.username, self.password) {
            (Some(username), Some(password)) => Ok(ProxyAuthConfig::required(username, password)),
            (None, None) => Ok(ProxyAuthConfig::disabled()),
            (Some(_), None) => Err(ConfigFileError::ProxyPasswordRequired),
            (None, Some(_)) => Err(ConfigFileError::ProxyUsernameRequired),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct TunFileConfig {
    name: Option<String>,
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
    dns_resolvers: Vec<SocketAddr>,
    dns_ttl_ms: Option<u32>,
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
            name: self.name,
            ipv4,
            ipv4_prefix: self.ipv4_prefix.unwrap_or(DEFAULT_TUN_IPV4_PREFIX),
            ipv4_gateway: self.ipv4_gateway,
            ipv6: self.ipv6,
            ipv6_prefix: self.ipv6_prefix.unwrap_or(64),
            mtu: self.mtu.unwrap_or(DEFAULT_TUN_MTU),
            enable_icmp: !self.disable_icmp,
            dns_resolvers: self.dns_resolvers,
            dns_ttl_ms: self.dns_ttl_ms.unwrap_or(DEFAULT_TUN_DNS_TTL_MS),
        })
    }
}

fn listen_or_default(listen: Vec<SocketAddr>, port: u16) -> Vec<SocketAddr> {
    if listen.is_empty() {
        vec![SocketAddr::from(([127, 0, 0, 1], port))]
    } else {
        listen
    }
}

fn parse_path_specs(values: Vec<String>) -> Result<Vec<PathSpec>, ConfigFileError> {
    values
        .into_iter()
        .map(|value| value.parse().map_err(ConfigFileError::PathSpec))
        .collect()
}

#[derive(Debug, Deserialize)]
#[serde(tag = "protocol", rename_all = "kebab-case", deny_unknown_fields)]
enum OutboundFileConfig {
    Mpp {
        tag: Option<String>,
        security: SecurityFileConfig,
        #[serde(default)]
        performance: MppPerformanceFileConfig,
        #[serde(default)]
        endpoints: Vec<String>,
        path_probe_interval_ms: Option<u64>,
        path_probe_timeout_ms: Option<u64>,
    },
    Direct {
        tag: Option<String>,
        bind_ip: Option<IpAddr>,
        connect_timeout_ms: Option<u64>,
        #[serde(default)]
        dns: DnsFileConfig,
    },
    Socks5 {
        tag: Option<String>,
        proxy: Option<String>,
        connect_timeout_ms: Option<u64>,
        #[serde(default)]
        dns: DnsFileConfig,
    },
    HttpConnect {
        tag: Option<String>,
        proxy: Option<String>,
        connect_timeout_ms: Option<u64>,
        #[serde(default)]
        dns: DnsFileConfig,
    },
    HttpConnectUdp {
        tag: Option<String>,
        proxy: Option<String>,
        connect_timeout_ms: Option<u64>,
        #[serde(default)]
        dns: DnsFileConfig,
    },
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutingFileConfig {
    #[serde(default)]
    balancers: Vec<RoutingBalancerFileConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutingBalancerFileConfig {
    tag: String,
    strategy: RoutingStrategyFileValue,
    #[serde(default)]
    outbounds: Vec<String>,
    #[serde(default)]
    performance: MppPerformanceFileConfig,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RoutingStrategyFileValue {
    Sequence,
    Random,
    CombinedMpp,
}

#[derive(Debug, Clone)]
struct ParsedOutbounds {
    mpp: HashMap<String, ParsedMppOutbound>,
    egress: HashMap<String, ParsedEgressOutbound>,
    mpp_balancers: HashMap<String, ParsedMppOutbound>,
    egress_balancers: HashMap<String, ParsedEgressOutbound>,
    mpp_order: Vec<String>,
    egress_order: Vec<String>,
    mpp_balancer_order: Vec<String>,
    egress_balancer_order: Vec<String>,
}

#[derive(Debug, Clone)]
struct ParsedMppOutbound {
    security: SecurityConfig,
    paths: Vec<ClientPathConfig>,
    path_probe_interval: Duration,
    path_probe_timeout: Duration,
    performance: MppPerformanceConfig,
}

#[derive(Debug, Clone)]
struct ParsedEgressOutbound {
    outbound: OutboundConfig,
    dns: DnsConfig,
    connect_timeout: Duration,
}

fn parse_outbounds(values: Vec<OutboundFileConfig>) -> Result<ParsedOutbounds, ConfigFileError> {
    let mut parsed = ParsedOutbounds {
        mpp: HashMap::new(),
        egress: HashMap::new(),
        mpp_balancers: HashMap::new(),
        egress_balancers: HashMap::new(),
        mpp_order: Vec::new(),
        egress_order: Vec::new(),
        mpp_balancer_order: Vec::new(),
        egress_balancer_order: Vec::new(),
    };
    for (index, value) in values.into_iter().enumerate() {
        match value {
            OutboundFileConfig::Mpp {
                tag,
                security,
                performance,
                endpoints,
                path_probe_interval_ms,
                path_probe_timeout_ms,
            } => {
                let tag = outbound_tag(tag, "mpp", index)?;
                insert_config_tag(&parsed, &tag)?;
                let security = security.into_config()?;
                let specs = parse_path_specs(endpoints)?;
                if specs.is_empty() {
                    return Err(ConfigFileError::MppOutboundRequiresEndpoint(tag));
                }
                let paths = specs
                    .into_iter()
                    .map(|spec| ClientPathConfig {
                        spec,
                        security: security.clone(),
                    })
                    .collect();
                parsed.mpp_order.push(tag.clone());
                parsed.mpp.insert(
                    tag,
                    ParsedMppOutbound {
                        security,
                        paths,
                        path_probe_interval: Duration::from_millis(
                            path_probe_interval_ms.unwrap_or(DEFAULT_PATH_PROBE_INTERVAL_MS),
                        ),
                        path_probe_timeout: Duration::from_millis(
                            path_probe_timeout_ms.unwrap_or(DEFAULT_PATH_PROBE_TIMEOUT_MS),
                        ),
                        performance: performance.into_config(),
                    },
                );
            }
            OutboundFileConfig::Direct {
                tag,
                bind_ip,
                connect_timeout_ms,
                dns,
            } => {
                let tag = outbound_tag(tag, "direct", index)?;
                insert_config_tag(&parsed, &tag)?;
                parsed.egress_order.push(tag.clone());
                parsed.egress.insert(
                    tag,
                    ParsedEgressOutbound {
                        outbound: match bind_ip {
                            Some(ip) => OutboundConfig::BindSourceIp(ip),
                            None => OutboundConfig::Direct,
                        },
                        dns: dns.into_config(),
                        connect_timeout: outbound_connect_timeout(connect_timeout_ms),
                    },
                );
            }
            OutboundFileConfig::Socks5 {
                tag,
                proxy,
                connect_timeout_ms,
                dns,
            } => {
                let tag = outbound_tag(tag, "socks5", index)?;
                insert_config_tag(&parsed, &tag)?;
                parsed.egress_order.push(tag.clone());
                parsed.egress.insert(
                    tag,
                    ParsedEgressOutbound {
                        outbound: OutboundConfig::Socks5 {
                            proxy: proxy
                                .ok_or(ConfigFileError::MissingOutboundProxy)?
                                .parse()
                                .map_err(ConfigFileError::Endpoint)?,
                        },
                        dns: dns.into_config(),
                        connect_timeout: outbound_connect_timeout(connect_timeout_ms),
                    },
                );
            }
            OutboundFileConfig::HttpConnect {
                tag,
                proxy,
                connect_timeout_ms,
                dns,
            } => {
                let tag = outbound_tag(tag, "http-connect", index)?;
                insert_config_tag(&parsed, &tag)?;
                parsed.egress_order.push(tag.clone());
                parsed.egress.insert(
                    tag,
                    ParsedEgressOutbound {
                        outbound: OutboundConfig::HttpConnect {
                            proxy: proxy
                                .ok_or(ConfigFileError::MissingOutboundProxy)?
                                .parse()
                                .map_err(ConfigFileError::Endpoint)?,
                        },
                        dns: dns.into_config(),
                        connect_timeout: outbound_connect_timeout(connect_timeout_ms),
                    },
                );
            }
            OutboundFileConfig::HttpConnectUdp {
                tag,
                proxy,
                connect_timeout_ms,
                dns,
            } => {
                let tag = outbound_tag(tag, "http-connect-udp", index)?;
                insert_config_tag(&parsed, &tag)?;
                parsed.egress_order.push(tag.clone());
                parsed.egress.insert(
                    tag,
                    ParsedEgressOutbound {
                        outbound: OutboundConfig::HttpConnectUdp {
                            proxy: proxy
                                .ok_or(ConfigFileError::MissingOutboundProxy)?
                                .parse()
                                .map_err(ConfigFileError::Endpoint)?,
                        },
                        dns: dns.into_config(),
                        connect_timeout: outbound_connect_timeout(connect_timeout_ms),
                    },
                );
            }
        }
    }
    Ok(parsed)
}

fn apply_routing(
    routing: RoutingFileConfig,
    parsed: &mut ParsedOutbounds,
) -> Result<(), ConfigFileError> {
    for balancer in routing.balancers {
        validate_tag(&balancer.tag)?;
        insert_config_tag(parsed, &balancer.tag)?;
        if balancer.outbounds.is_empty() {
            return Err(ConfigFileError::RoutingBalancerRequiresMembers(
                balancer.tag,
            ));
        }
        match balancer.strategy {
            RoutingStrategyFileValue::CombinedMpp => {
                let first_tag = balancer
                    .outbounds
                    .first()
                    .ok_or_else(|| {
                        ConfigFileError::RoutingBalancerRequiresMembers(balancer.tag.clone())
                    })?
                    .clone();
                let first = parsed.mpp.get(&first_tag).ok_or_else(|| {
                    routing_member_wrong_protocol(parsed, &balancer.tag, &first_tag, "mpp")
                })?;
                let mut paths = Vec::new();
                paths.extend(first.paths.clone());
                let path_probe_interval = first.path_probe_interval;
                let path_probe_timeout = first.path_probe_timeout;
                let performance = if balancer.performance.is_explicit() {
                    balancer.performance.into_config()
                } else {
                    first.performance
                };
                for tag in balancer.outbounds.iter().skip(1) {
                    let member = parsed.mpp.get(tag).ok_or_else(|| {
                        routing_member_wrong_protocol(parsed, &balancer.tag, tag, "mpp")
                    })?;
                    if member.path_probe_interval != path_probe_interval
                        || member.path_probe_timeout != path_probe_timeout
                    {
                        return Err(ConfigFileError::CombinedMppProbePolicyMismatch(
                            balancer.tag,
                        ));
                    }
                    if !balancer.performance.is_explicit() && member.performance != performance {
                        return Err(ConfigFileError::CombinedMppPerformancePolicyMismatch(
                            balancer.tag,
                        ));
                    }
                    paths.extend(member.paths.clone());
                }
                parsed.mpp_balancer_order.push(balancer.tag.clone());
                parsed.mpp_balancers.insert(
                    balancer.tag,
                    ParsedMppOutbound {
                        security: first.security.clone(),
                        paths,
                        path_probe_interval,
                        path_probe_timeout,
                        performance,
                    },
                );
            }
            RoutingStrategyFileValue::Sequence | RoutingStrategyFileValue::Random => {
                let mut members = Vec::with_capacity(balancer.outbounds.len());
                for tag in &balancer.outbounds {
                    let member = parsed.egress.get(tag).ok_or_else(|| {
                        routing_member_wrong_protocol(parsed, &balancer.tag, tag, "egress")
                    })?;
                    members.push(OutboundRouteMember {
                        config: Box::new(member.outbound.clone()),
                        dns: member.dns.clone(),
                        connect_timeout: member.connect_timeout,
                    });
                }
                parsed.egress_balancer_order.push(balancer.tag.clone());
                parsed.egress_balancers.insert(
                    balancer.tag,
                    ParsedEgressOutbound {
                        outbound: match balancer.strategy {
                            RoutingStrategyFileValue::Sequence => {
                                OutboundConfig::Sequence { members }
                            }
                            RoutingStrategyFileValue::Random => OutboundConfig::Random { members },
                            RoutingStrategyFileValue::CombinedMpp => unreachable!(),
                        },
                        dns: DnsConfig::default(),
                        connect_timeout: Duration::from_millis(DEFAULT_OUTBOUND_CONNECT_TIMEOUT_MS),
                    },
                );
            }
        }
    }
    Ok(())
}

fn routing_member_wrong_protocol(
    parsed: &ParsedOutbounds,
    balancer: &str,
    member: &str,
    expected: &'static str,
) -> ConfigFileError {
    if parsed.mpp.contains_key(member)
        || parsed.egress.contains_key(member)
        || parsed.mpp_balancers.contains_key(member)
        || parsed.egress_balancers.contains_key(member)
    {
        ConfigFileError::RoutingBalancerMemberWrongProtocol {
            balancer: balancer.to_string(),
            member: member.to_string(),
            expected,
        }
    } else {
        ConfigFileError::MissingOutboundTag(member.to_string())
    }
}

fn outbound_tag(
    tag: Option<String>,
    protocol: &'static str,
    index: usize,
) -> Result<String, ConfigFileError> {
    let tag = tag.unwrap_or_else(|| {
        if index == 0 {
            protocol.to_string()
        } else {
            format!("{protocol}-{index}")
        }
    });
    validate_tag(&tag)?;
    Ok(tag)
}

fn validate_tag(tag: &str) -> Result<(), ConfigFileError> {
    if tag.trim().is_empty() {
        Err(ConfigFileError::EmptyTag)
    } else {
        Ok(())
    }
}

fn insert_config_tag(parsed: &ParsedOutbounds, tag: &str) -> Result<(), ConfigFileError> {
    if parsed.mpp.contains_key(tag)
        || parsed.egress.contains_key(tag)
        || parsed.mpp_balancers.contains_key(tag)
        || parsed.egress_balancers.contains_key(tag)
    {
        return Err(ConfigFileError::DuplicateTag(tag.to_string()));
    }
    Ok(())
}

fn build_node_services(
    inbounds: Vec<InboundFileConfig>,
    outbounds: ParsedOutbounds,
) -> Result<(Vec<ClientConfig>, Vec<ServerConfig>), ConfigFileError> {
    let mut client_groups: HashMap<String, Vec<LocalIngressConfig>> = HashMap::new();
    let mut client_targets: HashMap<String, ResolvedMppTarget> = HashMap::new();
    let mut inbound_tags = HashSet::new();
    let mut client_order = Vec::new();
    let mut servers = Vec::new();

    for inbound in inbounds {
        match inbound {
            InboundFileConfig::Socks5 {
                tag,
                listen,
                outbound,
                balancer,
                auth,
            } => {
                validate_optional_tag(tag.as_deref())?;
                validate_unique_inbound_tag(tag.as_deref(), &mut inbound_tags)?;
                let target = resolve_mpp_target(outbound, balancer, &outbounds)?;
                push_client_ingress(
                    &mut client_groups,
                    &mut client_targets,
                    &mut client_order,
                    target,
                    LocalIngressConfig {
                        tag,
                        config: IngressConfig::Socks5 {
                            listen: listen_or_default(listen, 1080),
                            proxy_auth: proxy_auth_or_disabled(auth)?,
                        },
                    },
                );
            }
            InboundFileConfig::HttpConnect {
                tag,
                listen,
                outbound,
                balancer,
                auth,
            } => {
                validate_optional_tag(tag.as_deref())?;
                validate_unique_inbound_tag(tag.as_deref(), &mut inbound_tags)?;
                let target = resolve_mpp_target(outbound, balancer, &outbounds)?;
                push_client_ingress(
                    &mut client_groups,
                    &mut client_targets,
                    &mut client_order,
                    target,
                    LocalIngressConfig {
                        tag,
                        config: IngressConfig::HttpConnect {
                            listen: listen_or_default(listen, 8080),
                            proxy_auth: proxy_auth_or_disabled(auth)?,
                        },
                    },
                );
            }
            InboundFileConfig::Tun {
                tag,
                outbound,
                balancer,
                name,
                ipv4,
                disable_ipv4,
                ipv4_prefix,
                ipv4_gateway,
                ipv6,
                ipv6_prefix,
                mtu,
                disable_icmp,
                dns_resolvers,
                dns_ttl_ms,
            } => {
                validate_optional_tag(tag.as_deref())?;
                validate_unique_inbound_tag(tag.as_deref(), &mut inbound_tags)?;
                let target = resolve_mpp_target(outbound, balancer, &outbounds)?;
                push_client_ingress(
                    &mut client_groups,
                    &mut client_targets,
                    &mut client_order,
                    target,
                    LocalIngressConfig {
                        tag,
                        config: IngressConfig::TunL4(
                            TunFileConfig {
                                name,
                                ipv4,
                                disable_ipv4,
                                ipv4_prefix,
                                ipv4_gateway,
                                ipv6,
                                ipv6_prefix,
                                mtu,
                                disable_icmp,
                                dns_resolvers,
                                dns_ttl_ms,
                            }
                            .into_config()?,
                        ),
                    },
                );
            }
            InboundFileConfig::Mpp {
                tag,
                security,
                performance,
                endpoints,
                outbound,
                balancer,
            } => {
                validate_optional_tag(tag.as_deref())?;
                validate_unique_inbound_tag(tag.as_deref(), &mut inbound_tags)?;
                let egress = resolve_egress_target(outbound, balancer, &outbounds)?;
                let paths = parse_path_specs(endpoints)?;
                if paths.is_empty() {
                    return Err(ConfigFileError::MppInboundRequiresEndpoint);
                }
                servers.push(ServerConfig {
                    tag,
                    route_target: Some(egress.target),
                    bind_paths: paths,
                    security: security.into_config()?,
                    outbound: egress.config.outbound.clone(),
                    outbound_dns: egress.config.dns.clone(),
                    outbound_connect_timeout: egress.config.connect_timeout,
                    performance: performance.into_config(),
                });
            }
        }
    }

    let mut clients = Vec::with_capacity(client_order.len());
    for key in client_order {
        let mpp = client_targets
            .remove(&key)
            .ok_or_else(|| ConfigFileError::MissingOutboundTag(key.clone()))?;
        clients.push(ClientConfig {
            route_target: Some(mpp.target),
            ingresses: client_groups.remove(&key).unwrap_or_default(),
            security: mpp.config.security.clone(),
            paths: mpp.config.paths.clone(),
            path_probe_interval: mpp.config.path_probe_interval,
            path_probe_timeout: mpp.config.path_probe_timeout,
            performance: mpp.config.performance,
        });
    }

    if clients.is_empty() && servers.is_empty() {
        return Err(ConfigFileError::NoRuntimeServices);
    }
    Ok((clients, servers))
}

#[derive(Debug, Clone)]
struct ResolvedMppTarget {
    key: String,
    config: ParsedMppOutbound,
    target: RouteTarget,
}

fn mpp_outbound_target(tag: &str, config: &ParsedMppOutbound) -> ResolvedMppTarget {
    ResolvedMppTarget {
        key: format!("outbound:{tag}"),
        config: config.clone(),
        target: RouteTarget {
            kind: RouteTargetKind::Outbound,
            tag: tag.to_string(),
        },
    }
}

fn mpp_balancer_target(tag: &str, config: &ParsedMppOutbound) -> ResolvedMppTarget {
    ResolvedMppTarget {
        key: format!("balancer:{tag}"),
        config: config.clone(),
        target: RouteTarget {
            kind: RouteTargetKind::Balancer,
            tag: tag.to_string(),
        },
    }
}

fn resolve_mpp_target(
    outbound: Option<String>,
    balancer: Option<String>,
    outbounds: &ParsedOutbounds,
) -> Result<ResolvedMppTarget, ConfigFileError> {
    match (outbound, balancer) {
        (Some(_), Some(_)) => Err(ConfigFileError::InboundTargetConflict),
        (Some(tag), None) => {
            validate_tag(&tag)?;
            if let Some(config) = outbounds.mpp.get(&tag) {
                return Ok(mpp_outbound_target(&tag, config));
            }
            if outbounds.mpp_balancers.contains_key(&tag)
                || outbounds.egress_balancers.contains_key(&tag)
            {
                return Err(ConfigFileError::OutboundFieldReferencesBalancer(tag));
            }
            if outbounds.egress.contains_key(&tag) {
                return Err(ConfigFileError::OutboundTagWrongProtocol {
                    tag,
                    expected: "mpp",
                });
            }
            Err(ConfigFileError::MissingOutboundTag(tag))
        }
        (None, Some(tag)) => {
            validate_tag(&tag)?;
            if let Some(config) = outbounds.mpp_balancers.get(&tag) {
                return Ok(mpp_balancer_target(&tag, config));
            }
            if outbounds.mpp.contains_key(&tag) || outbounds.egress.contains_key(&tag) {
                return Err(ConfigFileError::BalancerFieldReferencesOutbound(tag));
            }
            if outbounds.egress_balancers.contains_key(&tag) {
                return Err(ConfigFileError::BalancerTagWrongProtocol {
                    tag,
                    expected: "combined-mpp",
                });
            }
            Err(ConfigFileError::MissingBalancerTag(tag))
        }
        (None, None) => resolve_default_mpp_target(outbounds),
    }
}

fn resolve_default_mpp_target(
    outbounds: &ParsedOutbounds,
) -> Result<ResolvedMppTarget, ConfigFileError> {
    let mut candidates = Vec::new();
    for tag in &outbounds.mpp_order {
        let config = outbounds
            .mpp
            .get(tag)
            .ok_or_else(|| ConfigFileError::MissingOutboundTag(tag.clone()))?;
        candidates.push(mpp_outbound_target(tag, config));
    }
    for tag in &outbounds.mpp_balancer_order {
        let config = outbounds
            .mpp_balancers
            .get(tag)
            .ok_or_else(|| ConfigFileError::MissingBalancerTag(tag.clone()))?;
        candidates.push(mpp_balancer_target(tag, config));
    }
    match candidates.as_slice() {
        [target] => Ok(target.clone()),
        [] => Err(ConfigFileError::LocalInboundRequiresMppOutbound),
        _ => Err(ConfigFileError::MultipleDefaultMppTargets),
    }
}

fn resolve_egress_target(
    outbound: Option<String>,
    balancer: Option<String>,
    outbounds: &ParsedOutbounds,
) -> Result<ResolvedEgressTarget, ConfigFileError> {
    match (outbound, balancer) {
        (Some(_), Some(_)) => Err(ConfigFileError::InboundTargetConflict),
        (Some(tag), None) => {
            validate_tag(&tag)?;
            if let Some(config) = outbounds.egress.get(&tag) {
                return Ok(egress_outbound_target(&tag, config));
            }
            if outbounds.mpp_balancers.contains_key(&tag)
                || outbounds.egress_balancers.contains_key(&tag)
            {
                return Err(ConfigFileError::OutboundFieldReferencesBalancer(tag));
            }
            if outbounds.mpp.contains_key(&tag) {
                return Err(ConfigFileError::OutboundTagWrongProtocol {
                    tag,
                    expected: "direct, socks5, http-connect, or http-connect-udp",
                });
            }
            Err(ConfigFileError::MissingOutboundTag(tag))
        }
        (None, Some(tag)) => {
            validate_tag(&tag)?;
            if let Some(config) = outbounds.egress_balancers.get(&tag) {
                return Ok(egress_balancer_target(&tag, config));
            }
            if outbounds.mpp.contains_key(&tag) || outbounds.egress.contains_key(&tag) {
                return Err(ConfigFileError::BalancerFieldReferencesOutbound(tag));
            }
            if outbounds.mpp_balancers.contains_key(&tag) {
                return Err(ConfigFileError::BalancerTagWrongProtocol {
                    tag,
                    expected: "sequence or random",
                });
            }
            Err(ConfigFileError::MissingBalancerTag(tag))
        }
        (None, None) => resolve_default_egress_target(outbounds),
    }
}

fn resolve_default_egress_target(
    outbounds: &ParsedOutbounds,
) -> Result<ResolvedEgressTarget, ConfigFileError> {
    let mut candidates = Vec::new();
    for tag in &outbounds.egress_order {
        let config = outbounds
            .egress
            .get(tag)
            .ok_or_else(|| ConfigFileError::MissingOutboundTag(tag.clone()))?;
        candidates.push(egress_outbound_target(tag, config));
    }
    for tag in &outbounds.egress_balancer_order {
        let config = outbounds
            .egress_balancers
            .get(tag)
            .ok_or_else(|| ConfigFileError::MissingBalancerTag(tag.clone()))?;
        candidates.push(egress_balancer_target(tag, config));
    }
    match candidates.as_slice() {
        [target] => Ok(target.clone()),
        [] => Err(ConfigFileError::MppInboundRequiresEgressOutbound),
        _ => Err(ConfigFileError::MultipleDefaultEgressTargets),
    }
}

#[derive(Debug, Clone)]
struct ResolvedEgressTarget {
    config: ParsedEgressOutbound,
    target: RouteTarget,
}

fn egress_outbound_target(tag: &str, config: &ParsedEgressOutbound) -> ResolvedEgressTarget {
    ResolvedEgressTarget {
        config: config.clone(),
        target: RouteTarget {
            kind: RouteTargetKind::Outbound,
            tag: tag.to_string(),
        },
    }
}

fn egress_balancer_target(tag: &str, config: &ParsedEgressOutbound) -> ResolvedEgressTarget {
    ResolvedEgressTarget {
        config: config.clone(),
        target: RouteTarget {
            kind: RouteTargetKind::Balancer,
            tag: tag.to_string(),
        },
    }
}

fn validate_optional_tag(tag: Option<&str>) -> Result<(), ConfigFileError> {
    if let Some(tag) = tag {
        validate_tag(tag)?;
    }
    Ok(())
}

fn validate_unique_inbound_tag(
    tag: Option<&str>,
    seen: &mut HashSet<String>,
) -> Result<(), ConfigFileError> {
    let Some(tag) = tag else {
        return Ok(());
    };
    if !seen.insert(tag.to_string()) {
        return Err(ConfigFileError::DuplicateInboundTag(tag.to_string()));
    }
    Ok(())
}

fn proxy_auth_or_disabled(
    auth: Option<ProxyAuthFileConfig>,
) -> Result<ProxyAuthConfig, ConfigFileError> {
    match auth {
        Some(auth) => auth.into_config(),
        None => Ok(ProxyAuthConfig::disabled()),
    }
}

fn outbound_connect_timeout(value: Option<u64>) -> Duration {
    Duration::from_millis(value.unwrap_or(DEFAULT_OUTBOUND_CONNECT_TIMEOUT_MS))
}

fn push_client_ingress(
    groups: &mut HashMap<String, Vec<LocalIngressConfig>>,
    targets: &mut HashMap<String, ResolvedMppTarget>,
    order: &mut Vec<String>,
    target: ResolvedMppTarget,
    ingress: LocalIngressConfig,
) {
    let key = target.key.clone();
    if !groups.contains_key(&key) {
        order.push(key.clone());
        targets.insert(key.clone(), target);
    }
    groups.entry(key).or_default().push(ingress);
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct DnsFileConfig {
    #[serde(default)]
    resolvers: Vec<SocketAddr>,
    #[serde(default)]
    strategy: DnsStrategyFileValue,
    timeout_ms: Option<u64>,
}

impl DnsFileConfig {
    fn into_config(self) -> DnsConfig {
        let defaults = DnsConfig::default();
        DnsConfig {
            resolvers: self.resolvers,
            strategy: self.strategy.into(),
            timeout: Duration::from_millis(
                self.timeout_ms
                    .unwrap_or(defaults.timeout.as_millis() as u64),
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum DnsStrategyFileValue {
    #[default]
    #[serde(alias = "ipv4_then_ipv6")]
    Ipv4ThenIpv6,
    #[serde(alias = "ipv6_then_ipv4")]
    Ipv6ThenIpv4,
    #[serde(alias = "ipv4_only")]
    Ipv4Only,
    #[serde(alias = "ipv6_only")]
    Ipv6Only,
    #[serde(alias = "ipv4_and_ipv6")]
    Ipv4AndIpv6,
    #[serde(alias = "ipv6_and_ipv4")]
    Ipv6AndIpv4,
}

impl From<DnsStrategyFileValue> for DnsIpStrategy {
    fn from(value: DnsStrategyFileValue) -> Self {
        match value {
            DnsStrategyFileValue::Ipv4ThenIpv6 => Self::Ipv4ThenIpv6,
            DnsStrategyFileValue::Ipv6ThenIpv4 => Self::Ipv6ThenIpv4,
            DnsStrategyFileValue::Ipv4Only => Self::Ipv4Only,
            DnsStrategyFileValue::Ipv6Only => Self::Ipv6Only,
            DnsStrategyFileValue::Ipv4AndIpv6 => Self::Ipv4AndIpv6,
            DnsStrategyFileValue::Ipv6AndIpv4 => Self::Ipv6AndIpv4,
        }
    }
}

#[derive(Debug)]
pub enum ConfigFileError {
    Io(std::io::Error),
    Toml(toml::de::Error),
    Config(ConfigError),
    Security(SecurityPolicyError),
    Endpoint(EndpointParseError),
    PathSpec(PathSpecParseError),
    NoRuntimeServices,
    EmptyTag,
    DuplicateTag(String),
    DuplicateInboundTag(String),
    MissingOutboundTag(String),
    MissingBalancerTag(String),
    OutboundTagWrongProtocol {
        tag: String,
        expected: &'static str,
    },
    BalancerTagWrongProtocol {
        tag: String,
        expected: &'static str,
    },
    InboundTargetConflict,
    OutboundFieldReferencesBalancer(String),
    BalancerFieldReferencesOutbound(String),
    LocalInboundRequiresMppOutbound,
    MppInboundRequiresEgressOutbound,
    MultipleDefaultMppTargets,
    MultipleDefaultEgressTargets,
    MppInboundRequiresEndpoint,
    MppOutboundRequiresEndpoint(String),
    RoutingBalancerRequiresMembers(String),
    RoutingBalancerMemberWrongProtocol {
        balancer: String,
        member: String,
        expected: &'static str,
    },
    CombinedMppProbePolicyMismatch(String),
    CombinedMppPerformancePolicyMismatch(String),
    MissingOutboundProxy,
    TunIpv4DisabledWithIpv4Options,
    ProxyUsernameRequired,
    ProxyPasswordRequired,
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
            Self::Endpoint(err) => write!(f, "{err}"),
            Self::PathSpec(err) => write!(f, "{err}"),
            Self::NoRuntimeServices => {
                write!(f, "config must define at least one [[inbounds]] entry")
            }
            Self::EmptyTag => write!(f, "inbound and outbound tags must not be empty"),
            Self::DuplicateTag(tag) => write!(f, "duplicate outbound or balancer tag {tag:?}"),
            Self::DuplicateInboundTag(tag) => write!(f, "duplicate inbound tag {tag:?}"),
            Self::MissingOutboundTag(tag) => write!(f, "outbound tag {tag:?} does not exist"),
            Self::MissingBalancerTag(tag) => {
                write!(f, "routing balancer tag {tag:?} does not exist")
            }
            Self::OutboundTagWrongProtocol { tag, expected } => {
                write!(f, "outbound tag {tag:?} must use protocol {expected}")
            }
            Self::BalancerTagWrongProtocol { tag, expected } => {
                write!(
                    f,
                    "routing balancer tag {tag:?} must use strategy {expected}"
                )
            }
            Self::InboundTargetConflict => {
                write!(f, "inbound must set at most one of outbound or balancer")
            }
            Self::OutboundFieldReferencesBalancer(tag) => write!(
                f,
                "inbound outbound field references routing balancer {tag:?}; use balancer instead"
            ),
            Self::BalancerFieldReferencesOutbound(tag) => write!(
                f,
                "inbound balancer field references outbound {tag:?}; use outbound instead"
            ),
            Self::LocalInboundRequiresMppOutbound => {
                write!(
                    f,
                    "SOCKS5, HTTP, and TUN inbounds require an MPP outbound or combined-mpp balancer"
                )
            }
            Self::MppInboundRequiresEgressOutbound => write!(
                f,
                "MPP inbounds require an egress outbound or egress balancer with protocol direct, socks5, http-connect, or http-connect-udp"
            ),
            Self::MultipleDefaultMppTargets => write!(
                f,
                "local inbound outbound or balancer tag is required when multiple MPP targets exist"
            ),
            Self::MultipleDefaultEgressTargets => write!(
                f,
                "MPP inbound outbound or balancer tag is required when multiple egress targets exist"
            ),
            Self::MppInboundRequiresEndpoint => {
                write!(f, "MPP inbound requires at least one endpoint")
            }
            Self::MppOutboundRequiresEndpoint(tag) => {
                write!(f, "MPP outbound {tag:?} requires at least one endpoint")
            }
            Self::RoutingBalancerRequiresMembers(tag) => {
                write!(
                    f,
                    "routing balancer {tag:?} requires at least one outbound member"
                )
            }
            Self::RoutingBalancerMemberWrongProtocol {
                balancer,
                member,
                expected,
            } => write!(
                f,
                "routing balancer {balancer:?} member {member:?} must be an {expected} outbound tag"
            ),
            Self::CombinedMppProbePolicyMismatch(tag) => write!(
                f,
                "combined MPP balancer {tag:?} requires member MPP outbounds to use the same path probe timing"
            ),
            Self::CombinedMppPerformancePolicyMismatch(tag) => write!(
                f,
                "combined MPP balancer {tag:?} requires member MPP outbounds to use the same performance policy unless the balancer defines performance"
            ),
            Self::MissingOutboundProxy => write!(f, "proxied outbound requires proxy"),
            Self::TunIpv4DisabledWithIpv4Options => {
                write!(
                    f,
                    "TUN disable_ipv4 cannot be combined with ipv4 or ipv4_gateway"
                )
            }
            Self::ProxyUsernameRequired => write!(f, "proxy auth password requires username"),
            Self::ProxyPasswordRequired => write!(f, "proxy auth username requires password"),
        }
    }
}

impl std::error::Error for ConfigFileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::Toml(err) => Some(err),
            Self::Config(err) => Some(err),
            Self::Security(err) => Some(err),
            Self::Endpoint(err) => Some(err),
            Self::PathSpec(err) => Some(err),
            Self::NoRuntimeServices
            | Self::EmptyTag
            | Self::DuplicateTag(_)
            | Self::DuplicateInboundTag(_)
            | Self::MissingOutboundTag(_)
            | Self::MissingBalancerTag(_)
            | Self::OutboundTagWrongProtocol { .. }
            | Self::BalancerTagWrongProtocol { .. }
            | Self::InboundTargetConflict
            | Self::OutboundFieldReferencesBalancer(_)
            | Self::BalancerFieldReferencesOutbound(_)
            | Self::LocalInboundRequiresMppOutbound
            | Self::MppInboundRequiresEgressOutbound
            | Self::MultipleDefaultMppTargets
            | Self::MultipleDefaultEgressTargets
            | Self::MppInboundRequiresEndpoint
            | Self::MppOutboundRequiresEndpoint(_)
            | Self::RoutingBalancerRequiresMembers(_)
            | Self::RoutingBalancerMemberWrongProtocol { .. }
            | Self::CombinedMppProbePolicyMismatch(_)
            | Self::CombinedMppPerformancePolicyMismatch(_)
            | Self::MissingOutboundProxy
            | Self::TunIpv4DisabledWithIpv4Options
            | Self::ProxyUsernameRequired
            | Self::ProxyPasswordRequired => None,
        }
    }
}

#[cfg(test)]
#[path = "file_test.rs"]
mod tests;
