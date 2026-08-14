//! Combined-node composition for multiple client and server identities.

use super::{client, server};
use crate::config::{
    ActiveNodeGraph, ForwardingMode, ManagementConfig, NodeConfig, OutboundLeafConfig,
    SessionConfig,
};
use crate::performance::ResourceLimits;
use crate::platform::{PacketDeviceConfig, PacketDeviceProvider};
use crate::product::{ProductAdmission, ProductAdmissionConfig};
use crate::runtime::config_control::RuntimeConfigControl;
use crate::runtime::error::RuntimeError;
use crate::runtime::management::{
    ProductRuntimeInventory, TunL3RuntimeInventory, spawn_node_management_services,
};
use crate::runtime::outbound_registry::{RuntimeOutboundLeaf, RuntimeOutboundRegistryShell};
use crate::runtime::path::ClientPathRuntimeOptions;
use crate::runtime::product_policy::ClientIngressRouter;
use crate::runtime::readiness::{RuntimeGenerationControl, RuntimeReadinessBarrier};
use crate::runtime::telemetry::{RuntimeTelemetry, active_flow_detail_capacity};
use crate::transport::NativeSocketConfigurator;
use std::collections::HashSet;
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
    pub(super) product_telemetry: Option<RuntimeTelemetry>,
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
        product_telemetry,
    } = environment;
    let runtime_carrier_network = carrier_network.provider.clone();
    let readiness = RuntimeReadinessBarrier::new(generation.clone());
    let active = node
        .compile_active_graph()
        .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?;
    let NodeConfig {
        forwarding_mode,
        mut outbounds,
        mut gateway_balancers,
        local_ingresses,
        tun_l3_ingresses,
        product_policy,
        dns_policy: _,
        servers,
    } = node;
    outbounds.retain(|outbound| active.contains_outbound(outbound.id()));
    gateway_balancers.retain(|balancer| active.contains_balancer(&balancer.id));
    let ActiveNodeGraph {
        dns_policy: compiled_dns,
        dns_activation,
        ..
    } = active;
    let product_inventory = ProductRuntimeInventory::from_config(&local_ingresses, &outbounds);
    let tun_l3_inventory = match forwarding_mode {
        ForwardingMode::L4 => TunL3RuntimeInventory::default(),
        ForwardingMode::L3 => TunL3RuntimeInventory::from_config(&tun_l3_ingresses, &servers),
    };
    let tun_l3_outbounds = match forwarding_mode {
        ForwardingMode::L4 => HashSet::new(),
        ForwardingMode::L3 => tun_l3_ingresses
            .iter()
            .map(|ingress| ingress.config.outbound.clone())
            .collect::<HashSet<_>>(),
    };
    let product_telemetry = product_telemetry.unwrap_or_else(|| {
        RuntimeTelemetry::generation_owner(active_flow_detail_capacity(resources.max_streams))
    });
    let mut services = tokio::task::JoinSet::new();
    let mut path_probe_services = Vec::new();
    let mut runtime_leaves = Vec::with_capacity(outbounds.len());
    let mut client_contexts = Vec::new();
    let mut tun_l3_contexts = Vec::new();
    let mut path_group_ordinal = 0;
    for outbound in outbounds {
        match outbound {
            OutboundLeafConfig::Mpp { id, config } => {
                let outbound_path_group_ordinal = path_group_ordinal;
                let context = client::new_path_context(
                    &config,
                    resources,
                    id.clone(),
                    ClientPathRuntimeOptions {
                        session_retention_timeout: session.retention_timeout,
                        path_group_ordinal: outbound_path_group_ordinal,
                        carrier_network: runtime_carrier_network.clone(),
                        allow_peer_diagnostics: management.peer_diagnostics_enabled()
                            || config.allow_peer_diagnostics,
                    },
                    product_telemetry.clone(),
                )?;
                path_group_ordinal += 1;
                path_probe_services.push((
                    context.clone(),
                    config.path_probe_interval,
                    config.path_probe_timeout,
                ));
                if tun_l3_outbounds.contains(&id) {
                    // One MPP outbound owns one path context and probe
                    // lifecycle within a runtime generation, independent of
                    // the selected forwarding family.
                    tun_l3_contexts.push(context.clone());
                }
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
    let dns_factory = outbound_shell.dns_backend_factory(native_sockets);
    let dns = crate::dns::DnsGeneration::compile_active_with_factory_and_admission(
        Arc::new(compiled_dns),
        &dns_factory,
        product_admission,
        &dns_activation,
    )
    .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?;
    carrier_network.install_product_dns(dns.clone())?;
    let outbound_registry = outbound_shell.with_dns(dns);
    let gateway_control = {
        let control = outbound_registry.gateway_control();
        (!control.is_empty()).then_some(control)
    };
    outbound_registry.spawn_gateway_probe_services(&mut services);

    let has_l4_server = servers.iter().any(|server| server.tun_l3.is_none());
    let router = if !local_ingresses.is_empty() || has_l4_server {
        let policy = product_policy.as_ref().ok_or(RuntimeError::Protocol(
            "node L4 inbounds are missing routing policy",
        ))?;
        Some(ClientIngressRouter::new(policy, outbound_registry.clone())?)
    } else {
        None
    };
    if !local_ingresses.is_empty()
        && let Err(error) = client::spawn_ingresses(
            local_ingresses,
            resources.into(),
            router
                .clone()
                .expect("local L4 inbounds require the shared router"),
            packet_devices.clone(),
            &readiness,
            &mut services,
        )
        .await
    {
        super::retire_runtime_services(&mut services).await;
        return Err(error);
    }

    for ingress in tun_l3_ingresses {
        let context = tun_l3_contexts
            .iter()
            .find(|context| context.outbound.as_ref() == Some(&ingress.config.outbound))
            .cloned()
            .ok_or_else(|| RuntimeError::OutboundUnavailable(ingress.config.outbound.clone()))?;
        let ingress_readiness = readiness.require("TUN-L3 packet ingress");
        services.spawn(crate::runtime::tun_l3::run_client_tun_l3(
            ingress.name,
            ingress.config,
            context,
            packet_devices.clone(),
            ingress_readiness,
        ));
    }

    let mut server_contexts = Vec::with_capacity(servers.len());
    for server_config in servers {
        let server_readiness = readiness.require("MPP server listeners");
        let server_router = if server_config.tun_l3.is_none() {
            router.clone()
        } else {
            None
        };
        let runtime = match server::new_identity_runtime_with_metadata(
            server_config.name,
            server_config.paths,
            outbound_registry.clone(),
            server_router,
            server_config.security,
            server_config.tls,
            server_config.performance,
            resources,
            product_telemetry.clone(),
            session.retention_timeout,
            management.peer_diagnostics_enabled(),
            server_config.peer_diagnostics_principals,
            forwarding_mode,
            server_config.tun_l3,
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
        if let Some(ip_tunnel) = paths.take_ip_tunnel_device() {
            let device = match packet_devices.open(&PacketDeviceConfig {
                interface_name: ip_tunnel.interface_name(),
                ipv4: ip_tunnel.ipv4(),
                ipv4_prefix: 32,
                ipv4_gateway: None,
                ipv6: ip_tunnel.ipv6(),
                ipv6_prefix: 128,
                mtu: ip_tunnel.mtu(),
            }) {
                Ok(device) => device,
                Err(error) => {
                    super::retire_runtime_services(&mut services).await;
                    return Err(RuntimeError::TunDevice(error));
                }
            };
            services.spawn(crate::runtime::tun_l3::run_server_tun_l3(
                paths.name.clone(),
                ip_tunnel,
                device,
            ));
        }
        if let Some(reliable_relay) = reliable_relay {
            services.spawn(reliable_relay.run());
        }
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
            tun_l3_inventory,
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
