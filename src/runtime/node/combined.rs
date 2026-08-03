//! Combined-node composition for multiple client and server identities.

use super::{client, server};
use crate::config::{ManagementConfig, NodeConfig, OutboundLeafConfig, SessionConfig};
use crate::performance::ResourceLimits;
use crate::platform::PacketDeviceProvider;
use crate::product::{ProductAdmission, ProductAdmissionConfig};
use crate::runtime::config_control::RuntimeConfigControl;
use crate::runtime::error::RuntimeError;
use crate::runtime::management::{ProductRuntimeInventory, spawn_node_management_services};
use crate::runtime::outbound_registry::{RuntimeOutboundLeaf, RuntimeOutboundRegistryShell};
use crate::runtime::path::ClientPathRuntimeOptions;
use crate::runtime::product_policy::ClientIngressRouter;
use crate::runtime::readiness::{RuntimeGenerationControl, RuntimeReadinessBarrier};
use crate::runtime::telemetry::{RuntimeTelemetry, active_flow_detail_capacity};
use crate::transport::NativeSocketConfigurator;
use std::sync::Arc;

pub(super) struct NodeRuntimeEnvironment {
    pub(super) resources: ResourceLimits,
    pub(super) admission: ProductAdmissionConfig,
    pub(super) session: SessionConfig,
    pub(super) management: ManagementConfig,
    pub(super) packet_devices: Arc<dyn PacketDeviceProvider>,
    pub(super) carrier_network: super::GenerationCarrierNetwork,
    pub(super) native_sockets: Arc<dyn NativeSocketConfigurator>,
    pub(super) config_control: Option<RuntimeConfigControl>,
    pub(super) generation: RuntimeGenerationControl,
}

pub(super) async fn run(
    node: NodeConfig,
    environment: NodeRuntimeEnvironment,
) -> Result<crate::runtime::readiness::RuntimeGenerationStopReason, RuntimeError> {
    let NodeRuntimeEnvironment {
        resources,
        admission,
        session,
        management,
        packet_devices,
        carrier_network,
        native_sockets,
        config_control,
        generation,
    } = environment;
    let runtime_carrier_network = carrier_network.provider.clone();
    let readiness = RuntimeReadinessBarrier::new(generation.clone());
    let NodeConfig {
        outbounds,
        gateway_balancers,
        local_ingresses,
        product_policy,
        dns_policy,
        servers,
    } = node;
    let product_inventory = ProductRuntimeInventory::from_config(&local_ingresses, &outbounds);
    let product_telemetry =
        RuntimeTelemetry::generation_owner(active_flow_detail_capacity(resources.max_streams));
    let mut services = tokio::task::JoinSet::new();
    let mut path_probe_services = Vec::new();
    let mut runtime_leaves = Vec::with_capacity(outbounds.len());
    let mut client_contexts = Vec::new();
    let mut path_group_ordinal = 0;
    for outbound in outbounds {
        match outbound {
            OutboundLeafConfig::Mpp { id, config } => {
                let context = client::new_path_context(
                    &config,
                    resources,
                    id.clone(),
                    ClientPathRuntimeOptions {
                        session_retention_timeout: session.retention_timeout,
                        path_group_ordinal,
                        carrier_network: runtime_carrier_network.clone(),
                        allow_peer_diagnostics: management.peer_diagnostics_enabled(),
                    },
                    product_telemetry.clone(),
                )?;
                path_group_ordinal += 1;
                path_probe_services.push((
                    context.clone(),
                    config.path_probe_interval,
                    config.path_probe_timeout,
                ));
                runtime_leaves.push(RuntimeOutboundLeaf::Mpp {
                    id,
                    context: context.clone(),
                    performance: config.performance,
                });
                client_contexts.push(context);
            }
            OutboundLeafConfig::Local {
                id,
                config,
                connect_timeout,
            } => {
                runtime_leaves.push(RuntimeOutboundLeaf::Local {
                    id,
                    config,
                    connect_timeout,
                    native_sockets: native_sockets.clone(),
                });
            }
        }
    }
    let product_admission = ProductAdmission::new(admission)
        .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?;
    let outbound_shell = RuntimeOutboundRegistryShell::compile(runtime_leaves, &gateway_balancers)?
        .with_product_admission(product_admission.clone())
        .with_product_telemetry(product_telemetry.clone());
    let compiled_dns = dns_policy
        .compile()
        .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?;
    let dns_factory = outbound_shell.dns_backend_factory(native_sockets);
    let dns = crate::dns::DnsGeneration::compile_with_factory_and_admission(
        Arc::new(compiled_dns),
        &dns_factory,
        product_admission,
    )
    .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?;
    carrier_network.install_product_dns(dns.clone())?;
    let outbound_registry = outbound_shell.with_dns(dns);
    let gateway_control = {
        let control = outbound_registry.gateway_control();
        (!control.is_empty()).then_some(control)
    };
    outbound_registry.spawn_gateway_probe_services(&mut services);

    if !local_ingresses.is_empty() {
        let policy = product_policy.as_ref().ok_or(RuntimeError::Protocol(
            "node local inbounds are missing routing policy",
        ))?;
        let router = ClientIngressRouter::new(policy, outbound_registry.clone())?;
        if let Err(error) = client::spawn_ingresses(
            local_ingresses,
            resources.into(),
            router,
            packet_devices.clone(),
            &readiness,
            &mut services,
        )
        .await
        {
            super::retire_runtime_services(&mut services).await;
            return Err(error);
        }
    }

    let mut server_contexts = Vec::with_capacity(servers.len());
    for server_config in servers {
        let server_readiness = readiness.require("MPP server listeners");
        let destination_acl = match server_config.destination_acl.compile() {
            Ok(destination_acl) => destination_acl,
            Err(error) => {
                super::retire_runtime_services(&mut services).await;
                return Err(RuntimeError::ProductPolicy(error.to_string()));
            }
        };
        let inbound_id = match crate::product::InboundId::parse(&server_config.name) {
            Ok(inbound_id) => inbound_id,
            Err(error) => {
                super::retire_runtime_services(&mut services).await;
                return Err(RuntimeError::ProductPolicy(error.to_string()));
            }
        };
        let destination_policy = Arc::new(crate::outbound::ServerDestinationPolicy::for_inbound(
            destination_acl,
            inbound_id,
        ));
        let runtime = match server::new_identity_runtime_with_metadata(
            server_config.name,
            server_config.paths,
            outbound_registry.clone(),
            server_config.egress,
            server_config.dns_plan,
            destination_policy,
            server_config.security,
            server_config.tls,
            server_config.performance,
            resources,
            product_telemetry.clone(),
            session.retention_timeout,
            management.peer_diagnostics_enabled(),
        ) {
            Ok(runtime) => runtime,
            Err(error) => {
                super::retire_runtime_services(&mut services).await;
                return Err(error);
            }
        };
        let bound = match server::bind_paths(&runtime.paths).await {
            Ok(bound) => bound,
            Err(error) => {
                super::retire_runtime_services(&mut services).await;
                return Err(error);
            }
        };
        let server::ServerIdentityRuntime {
            paths,
            reliable_relay,
        } = runtime;
        services.spawn(reliable_relay.run());
        server::spawn_listeners(bound, paths.clone(), &mut services);
        server_readiness.ready();
        server_contexts.push(paths);
    }

    if management.http_enabled() {
        let management_readiness = readiness.require("management listeners");
        if let Err(error) = spawn_node_management_services(
            management,
            client_contexts,
            server_contexts,
            product_inventory,
            product_telemetry,
            config_control.clone(),
            gateway_control,
            Some(outbound_registry.dns().clone()),
            outbound_registry.product_admission().clone(),
            generation.clone(),
            management_readiness,
            &mut services,
        )
        .await
        {
            super::retire_runtime_services(&mut services).await;
            return Err(error);
        }
    }

    if services.is_empty() {
        return Err(RuntimeError::Protocol("node has no runtime services"));
    }
    for (context, interval, timeout) in path_probe_services {
        client::spawn_path_probe_service(context, interval, timeout, &mut services);
    }
    readiness.seal();
    let startup = tokio::select! {
        biased;
        service = services.join_next() => {
            super::map_runtime_service_result(
                service,
                "node service exited during startup",
                "node has no runtime services",
            )
            .map(Some)
        }
        stop = generation.wait_for_stop() => Ok(Some(stop)),
        ready = generation.wait_until_ready() => {
            ready
                .map(|()| None)
                .map_err(|_| RuntimeError::Protocol(
                    "runtime generation failed before reaching readiness",
                ))
        }
    };
    match startup {
        Ok(None) => {}
        Ok(Some(stop)) => {
            generation.wait_for_retirement_authorization().await;
            super::retire_runtime_services(&mut services).await;
            return Ok(stop);
        }
        Err(error) => {
            super::retire_runtime_services(&mut services).await;
            return Err(error);
        }
    }
    super::supervise_runtime_services(
        services,
        &generation,
        "node service exited",
        "node has no runtime services",
    )
    .await
}
