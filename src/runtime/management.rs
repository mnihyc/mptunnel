//! Management composition and endpoint-local path controls.
//!
//! The sampler publishes immutable typed snapshots, while `http` owns browser
//! transport details. Peer diagnostic exchange remains on authenticated MPP
//! carrier control channels and is only initiated here on an explicit request.

mod config;
mod control;
mod dns;
mod gateway;
mod http;
mod projection;
mod schema;
mod snapshot;

#[cfg(test)]
use self::http::{ManagementRequest, management_auth_ok};
use self::schema::ManagementSnapshot;
use self::snapshot::ManagementState;
#[cfg(test)]
use super::*;
use crate::config::{LocalIngressConfig, ManagementConfig, OutboundLeafConfig};
use crate::dns::DnsGeneration;
use crate::ingress::IngressConfig;
use crate::outbound::OutboundConfig;
use crate::product::{Network, OutboundId, ProductAdmission};
use crate::runtime::config_control::RuntimeConfigControl;
use crate::runtime::error::RuntimeError;
use crate::runtime::outbound_registry::GatewayRuntimeControl;
use crate::runtime::path::{ClientPathContext, ServerPathContext};
use crate::runtime::readiness::{RequiredServiceReadiness, RuntimeGenerationControl};
use crate::runtime::telemetry::RuntimeTelemetry;
use std::sync::Arc;
use std::time::Duration;

const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

#[allow(
    clippy::too_many_arguments,
    reason = "the generation composition boundary explicitly transfers each independent Product owner"
)]
pub(super) async fn spawn_node_management_services(
    config: ManagementConfig,
    clients: Vec<ClientPathContext>,
    servers: Vec<ServerPathContext>,
    inventory: ProductRuntimeInventory,
    product_telemetry: RuntimeTelemetry,
    config_control: Option<RuntimeConfigControl>,
    gateway_control: Option<GatewayRuntimeControl>,
    dns: Option<DnsGeneration>,
    product_admission: ProductAdmission,
    generation: RuntimeGenerationControl,
    readiness: RequiredServiceReadiness,
    services: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) -> Result<(), RuntimeError> {
    let state = ManagementState::new("node");
    let target = ManagementTarget {
        clients,
        servers,
        inventory,
        product_telemetry,
        state,
        config_control,
        gateway_control,
        dns,
        product_admission,
        generation,
    };
    spawn_management_services(config, target, readiness, services).await
}

async fn spawn_management_services(
    config: ManagementConfig,
    target: ManagementTarget,
    readiness: RequiredServiceReadiness,
    services: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) -> Result<(), RuntimeError> {
    target.refresh_sample_snapshot();
    http::spawn_listeners(config, target.clone(), readiness, services).await?;
    services.spawn(run_sampler(target));
    Ok(())
}

async fn run_sampler(target: ManagementTarget) -> Result<(), RuntimeError> {
    let start = tokio::time::Instant::now() + SAMPLE_INTERVAL;
    let mut ticker = tokio::time::interval_at(start, SAMPLE_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        target.refresh_sample_snapshot();
    }
}

#[derive(Clone)]
struct ManagementTarget {
    clients: Vec<ClientPathContext>,
    servers: Vec<ServerPathContext>,
    inventory: ProductRuntimeInventory,
    product_telemetry: RuntimeTelemetry,
    state: ManagementState,
    config_control: Option<RuntimeConfigControl>,
    gateway_control: Option<GatewayRuntimeControl>,
    dns: Option<DnsGeneration>,
    product_admission: ProductAdmission,
    generation: RuntimeGenerationControl,
}

#[derive(Debug, Clone, Default)]
pub(super) struct ProductRuntimeInventory {
    local_inbounds: Arc<Vec<ProductInboundInventory>>,
    outbounds: Arc<Vec<ProductOutboundInventory>>,
}

#[derive(Debug, Clone)]
struct ProductInboundInventory {
    name: String,
    protocol: &'static str,
    listen: Vec<String>,
    interface_name: Option<String>,
    target: Option<String>,
    auth_required: bool,
}

#[derive(Debug, Clone)]
struct ProductOutboundInventory {
    id: OutboundId,
    protocol: &'static str,
    networks: Vec<Network>,
}

impl ProductRuntimeInventory {
    pub(super) fn from_config(
        local_inbounds: &[LocalIngressConfig],
        outbounds: &[OutboundLeafConfig],
    ) -> Self {
        let local_inbounds = local_inbounds
            .iter()
            .map(|inbound| {
                let (protocol, listen, interface_name, target, auth_required) =
                    match &inbound.config {
                        IngressConfig::Socks5 {
                            listen, proxy_auth, ..
                        } => (
                            "socks5",
                            listen.iter().map(ToString::to_string).collect(),
                            None,
                            None,
                            proxy_auth.is_required(),
                        ),
                        IngressConfig::HttpConnect {
                            listen, proxy_auth, ..
                        } => (
                            "http-connect",
                            listen.iter().map(ToString::to_string).collect(),
                            None,
                            None,
                            proxy_auth.is_required(),
                        ),
                        IngressConfig::TcpForward(config) => (
                            "tcp-forward",
                            config.listen().iter().map(ToString::to_string).collect(),
                            None,
                            Some(config.target().to_string()),
                            false,
                        ),
                        IngressConfig::UdpForward(config) => (
                            "udp-forward",
                            config.listen().iter().map(ToString::to_string).collect(),
                            None,
                            Some(config.target().to_string()),
                            false,
                        ),
                        IngressConfig::TunL4(tun) => {
                            ("tun", Vec::new(), tun.interface_name.clone(), None, false)
                        }
                    };
                ProductInboundInventory {
                    name: inbound.name.clone(),
                    protocol,
                    listen,
                    interface_name,
                    target,
                    auth_required,
                }
            })
            .collect();
        let outbounds = outbounds
            .iter()
            .map(|outbound| match outbound {
                OutboundLeafConfig::Mpp { id, .. } => ProductOutboundInventory {
                    id: id.clone(),
                    protocol: "mpp",
                    networks: vec![Network::Tcp, Network::Udp],
                },
                OutboundLeafConfig::Local { id, config, .. } => ProductOutboundInventory {
                    id: id.clone(),
                    protocol: match config {
                        OutboundConfig::Direct => "direct",
                        OutboundConfig::BindSourceIp(_) => "bind-source",
                        OutboundConfig::Socks5(_) => "socks5",
                        OutboundConfig::HttpConnect(_) => "http-connect",
                        OutboundConfig::HttpsConnect(_) => "https-connect",
                    },
                    networks: if config.supports_udp_targets() {
                        vec![Network::Tcp, Network::Udp]
                    } else {
                        vec![Network::Tcp]
                    },
                },
            })
            .collect();
        Self {
            local_inbounds: Arc::new(local_inbounds),
            outbounds: Arc::new(outbounds),
        }
    }
}

impl ManagementTarget {
    fn state(&self) -> &ManagementState {
        &self.state
    }

    fn refresh_sample_snapshot(&self) {
        self.state().refresh(self, true);
    }

    fn refresh_current_snapshot(&self) {
        self.state().refresh(self, false);
    }

    fn snapshot(&self) -> Arc<ManagementSnapshot> {
        self.state().snapshot()
    }

    fn config_control(&self) -> Option<&RuntimeConfigControl> {
        self.config_control.as_ref()
    }

    fn generation(&self) -> &RuntimeGenerationControl {
        &self.generation
    }
}

#[cfg(test)]
#[path = "management_test.rs"]
mod tests;
