//! Host-independent compilation of one process-managed VPN generation.
//!
//! Linux and Windows share this inventory compiler so carrier identities,
//! native proxy bypasses, and pre-publication DNS rules cannot drift between
//! platform adapters. No host inspection or mutation occurs here.

use crate::config::{NodeConfig, OutboundLeafConfig};
use crate::ingress::IngressConfig;
use crate::ingress::tun::{
    DEFAULT_MANAGED_TUN_NAME, ManagedVpnCompileError, ManagedVpnPlatformConfig,
};
use crate::platform::{ManagedVpnConfig, RouteMode};
use crate::product::{CompiledDnsPolicy, DnsCompileError, DnsEgressSpec, DomainName};
use crate::transport::{CarrierPathIdentity, Endpoint, PathSpec};
use std::collections::BTreeSet;
use std::fmt;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

pub(crate) const MAX_PREPARED_CARRIER_PATHS: usize = 128;
pub(crate) const MAX_NATIVE_ENDPOINTS: usize = 128;

/// One bounded deadline shared by all native endpoint resolution performed
/// before a generation publishes capture routes.
pub(crate) const MANAGED_VPN_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedVpnCarrierPath {
    pub(crate) identity: CarrierPathIdentity,
    pub(crate) path: PathSpec,
}

/// Portable desired state and complete native endpoint inventory for one
/// process-managed generation.
#[derive(Debug, Clone)]
pub(crate) struct ManagedVpnGenerationSpec {
    pub(crate) managed: ManagedVpnConfig,
    pub(crate) managed_tun_count: usize,
    pub(crate) interface_name: String,
    pub(crate) ingress_index: usize,
    pub(crate) ingress_name: String,
    pub(crate) platform: ManagedVpnPlatformConfig,
    pub(crate) carrier_paths: Vec<ManagedVpnCarrierPath>,
    pub(crate) native_proxy_endpoints: Vec<Endpoint>,
    pub(crate) prepublication_domains: Vec<DomainName>,
    pub(crate) dns_policy: Arc<CompiledDnsPolicy>,
    pub(crate) resolution_timeout: Duration,
}

/// Compiles the managed-VPN portion of one validated node without touching
/// the host, opening a socket, or resolving a name.
pub(crate) fn compile_managed_vpn_generation_spec(
    node: &NodeConfig,
) -> Result<Option<ManagedVpnGenerationSpec>, ManagedVpnGenerationSpecError> {
    let mut managed_tun_count = 0_usize;
    let mut managed_tun = None;
    for (ingress_index, ingress) in node.local_ingresses.iter().enumerate() {
        let IngressConfig::TunL4(tun) = &ingress.config else {
            continue;
        };
        if tun.managed_vpn().is_some() {
            managed_tun_count = managed_tun_count.saturating_add(1);
            managed_tun.get_or_insert((ingress_index, ingress.name.as_str(), tun));
        }
    }
    if managed_tun_count == 0 {
        return Ok(None);
    }
    if managed_tun_count != 1 {
        return Err(ManagedVpnGenerationSpecError::MultipleManagedTunInbounds {
            actual: managed_tun_count,
        });
    }

    let (ingress_index, ingress_name, tun) =
        managed_tun.expect("one managed TUN was counted and retained");
    let managed = tun
        .compile_managed_vpn()
        .map_err(|source| ManagedVpnGenerationSpecError::ManagedTun {
            ingress_index,
            ingress_name: ingress_name.to_owned(),
            source,
        })?
        .expect("a managed TUN compiles to a platform VPN config");
    let interface_name = tun
        .interface_name
        .clone()
        .unwrap_or_else(|| DEFAULT_MANAGED_TUN_NAME.to_owned());
    let platform = tun
        .managed_vpn()
        .expect("the managed TUN was selected above")
        .platform
        .clone();

    let carrier_path_count = node
        .outbounds
        .iter()
        .filter_map(|outbound| match outbound {
            OutboundLeafConfig::Mpp {
                id,
                config: outbound,
            } if outbound.paths.is_empty() => Some(Err(
                ManagedVpnGenerationSpecError::MppOutboundWithoutCarrierPaths {
                    outbound: id.as_str().to_owned(),
                },
            )),
            OutboundLeafConfig::Mpp {
                config: outbound, ..
            } => Some(Ok(outbound.paths.len())),
            OutboundLeafConfig::Local { .. } => None,
        })
        .try_fold(0_usize, |total, count| {
            count.map(|count| total.saturating_add(count))
        })?;
    if carrier_path_count > MAX_PREPARED_CARRIER_PATHS {
        return Err(ManagedVpnGenerationSpecError::TooManyCarrierPaths {
            actual: carrier_path_count,
            maximum: MAX_PREPARED_CARRIER_PATHS,
        });
    }

    let mut carrier_paths = Vec::with_capacity(carrier_path_count);
    let mut prepublication_domains = BTreeSet::new();
    let mut group_ordinal = 0_usize;
    for outbound in &node.outbounds {
        let OutboundLeafConfig::Mpp {
            id,
            config: outbound,
        } = outbound
        else {
            continue;
        };
        for (path_ordinal, path) in outbound.paths.iter().enumerate() {
            if path.spec.endpoint.host.trim().is_empty() {
                return Err(ManagedVpnGenerationSpecError::InvalidCarrierEndpoint {
                    outbound: id.as_str().to_owned(),
                    path_ordinal,
                    endpoint: path.spec.endpoint.authority(),
                });
            }
            if path.spec.endpoint.host.parse::<IpAddr>().is_err() {
                let domain = DomainName::parse(&path.spec.endpoint.host).map_err(|_| {
                    ManagedVpnGenerationSpecError::InvalidCarrierEndpoint {
                        outbound: id.as_str().to_owned(),
                        path_ordinal,
                        endpoint: path.spec.endpoint.authority(),
                    }
                })?;
                prepublication_domains.insert(domain);
            }
            carrier_paths.push(ManagedVpnCarrierPath {
                identity: CarrierPathIdentity {
                    group_ordinal,
                    path_ordinal,
                },
                path: path.spec.clone(),
            });
        }
        group_ordinal = group_ordinal
            .checked_add(1)
            .expect("MPP group count cannot exceed outbound vector length");
    }

    let mut native_proxy_endpoints = Vec::new();
    for outbound in &node.outbounds {
        let OutboundLeafConfig::Local { config, .. } = outbound else {
            continue;
        };
        let Some(endpoint) = config.native_proxy_endpoint() else {
            continue;
        };
        if endpoint.host.parse::<IpAddr>().is_err() {
            let domain = DomainName::parse(&endpoint.host).map_err(|_| {
                ManagedVpnGenerationSpecError::InvalidNativeEndpoint {
                    endpoint: endpoint.authority(),
                }
            })?;
            prepublication_domains.insert(domain);
        }
        if !native_proxy_endpoints.contains(endpoint) {
            native_proxy_endpoints.push(endpoint.clone());
        }
    }
    if native_proxy_endpoints.len() > MAX_NATIVE_ENDPOINTS {
        return Err(ManagedVpnGenerationSpecError::TooManyNativeEndpoints {
            actual: native_proxy_endpoints.len(),
            maximum: MAX_NATIVE_ENDPOINTS,
        });
    }

    let dns_policy = Arc::new(
        node.dns_policy
            .compile()
            .map_err(ManagedVpnGenerationSpecError::DnsPolicy)?,
    );
    validate_managed_vpn_dns(&managed, &dns_policy)?;
    for domain in &prepublication_domains {
        let selection = dns_policy.select(domain);
        for upstream_id in selection.plan().upstreams() {
            let upstream = dns_policy.upstream(upstream_id).ok_or_else(|| {
                ManagedVpnGenerationSpecError::DnsPolicyInvariant {
                    message: format!(
                        "pre-publication plan {} lost upstream {upstream_id}",
                        selection.plan().id()
                    ),
                }
            })?;
            if let DnsEgressSpec::Outbound(outbound) = upstream.egress() {
                return Err(
                    ManagedVpnGenerationSpecError::PreCarrierDnsEgressUnsupported {
                        upstream: upstream.id().as_str().to_owned(),
                        outbound: outbound.as_str().to_owned(),
                    },
                );
            }
        }
    }

    Ok(Some(ManagedVpnGenerationSpec {
        managed,
        managed_tun_count,
        interface_name,
        ingress_index,
        ingress_name: ingress_name.to_owned(),
        platform,
        carrier_paths,
        native_proxy_endpoints,
        prepublication_domains: prepublication_domains.into_iter().collect(),
        dns_policy,
        resolution_timeout: MANAGED_VPN_RESOLUTION_TIMEOUT,
    }))
}

pub(crate) fn validate_managed_vpn_dns(
    managed: &ManagedVpnConfig,
    dns_policy: &CompiledDnsPolicy,
) -> Result<(), ManagedVpnGenerationSpecError> {
    if dns_policy.uses_system_resolution() {
        return Err(ManagedVpnGenerationSpecError::SystemDnsUnsupported);
    }
    if !dns_policy.is_encrypted_only() {
        return Err(ManagedVpnGenerationSpecError::EncryptedDnsRequired);
    }
    if managed.dns().is_none() && matches!(managed.route_mode(), RouteMode::Full) {
        return Err(ManagedVpnGenerationSpecError::FullTunnelDnsCaptureRequired);
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) enum ManagedVpnGenerationSpecError {
    MultipleManagedTunInbounds {
        actual: usize,
    },
    ManagedTun {
        ingress_index: usize,
        ingress_name: String,
        source: ManagedVpnCompileError,
    },
    MppOutboundWithoutCarrierPaths {
        outbound: String,
    },
    InvalidCarrierEndpoint {
        outbound: String,
        path_ordinal: usize,
        endpoint: String,
    },
    InvalidNativeEndpoint {
        endpoint: String,
    },
    TooManyCarrierPaths {
        actual: usize,
        maximum: usize,
    },
    TooManyNativeEndpoints {
        actual: usize,
        maximum: usize,
    },
    DnsPolicy(DnsCompileError),
    DnsPolicyInvariant {
        message: String,
    },
    PreCarrierDnsEgressUnsupported {
        upstream: String,
        outbound: String,
    },
    SystemDnsUnsupported,
    EncryptedDnsRequired,
    FullTunnelDnsCaptureRequired,
}

impl fmt::Display for ManagedVpnGenerationSpecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MultipleManagedTunInbounds { actual } => write!(
                formatter,
                "managed VPN generation requires exactly one managed TUN ingress; found {actual}"
            ),
            Self::ManagedTun {
                ingress_index,
                ingress_name,
                source,
            } => write!(
                formatter,
                "managed TUN inbound {ingress_name} at index {ingress_index} is invalid: {source}"
            ),
            Self::MppOutboundWithoutCarrierPaths { outbound } => write!(
                formatter,
                "managed VPN cannot prepare MPP outbound {outbound}: it has no carrier paths"
            ),
            Self::InvalidCarrierEndpoint {
                outbound,
                path_ordinal,
                endpoint,
            } => write!(
                formatter,
                "managed VPN MPP outbound {outbound} path {path_ordinal} has invalid endpoint {endpoint}"
            ),
            Self::InvalidNativeEndpoint { endpoint } => write!(
                formatter,
                "managed VPN native proxy has invalid endpoint {endpoint}"
            ),
            Self::TooManyCarrierPaths { actual, maximum } => write!(
                formatter,
                "managed VPN generation has {actual} carrier paths; maximum is {maximum}"
            ),
            Self::TooManyNativeEndpoints { actual, maximum } => write!(
                formatter,
                "managed VPN generation has {actual} unique native proxy endpoints; maximum is {maximum}"
            ),
            Self::DnsPolicy(error) => {
                write!(formatter, "managed VPN DNS policy is invalid: {error}")
            }
            Self::DnsPolicyInvariant { message } => {
                write!(formatter, "managed VPN DNS policy invariant failed: {message}")
            }
            Self::PreCarrierDnsEgressUnsupported { upstream, outbound } => write!(
                formatter,
                "managed VPN DNS upstream {upstream} selects outbound {outbound}, which is unavailable before carrier bootstrap"
            ),
            Self::SystemDnsUnsupported => formatter.write_str(
                "managed VPN cannot use system DNS because it creates a recursive tunnel dependency",
            ),
            Self::EncryptedDnsRequired => {
                formatter.write_str("managed VPN requires encrypted-only DNS plans")
            }
            Self::FullTunnelDnsCaptureRequired => {
                formatter.write_str("full managed VPN requires host DNS capture")
            }
        }
    }
}

impl std::error::Error for ManagedVpnGenerationSpecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ManagedTun { source, .. } => Some(source),
            Self::DnsPolicy(source) => Some(source),
            Self::MultipleManagedTunInbounds { .. }
            | Self::MppOutboundWithoutCarrierPaths { .. }
            | Self::InvalidCarrierEndpoint { .. }
            | Self::InvalidNativeEndpoint { .. }
            | Self::TooManyCarrierPaths { .. }
            | Self::TooManyNativeEndpoints { .. }
            | Self::DnsPolicyInvariant { .. }
            | Self::PreCarrierDnsEgressUnsupported { .. }
            | Self::SystemDnsUnsupported
            | Self::EncryptedDnsRequired
            | Self::FullTunnelDnsCaptureRequired => None,
        }
    }
}
