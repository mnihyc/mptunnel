//! Combined-node composition for multiple client and server identities.

use super::{client, server};
use crate::config::{ManagementConfig, NodeConfig, ResourceLimits};
use crate::runtime::error::RuntimeError;
use crate::runtime::management::spawn_node_management_services;
use crate::runtime::packet_device::PacketDeviceProvider;
use crate::transport::CarrierNetworkProvider;
use std::sync::Arc;

pub(super) async fn run(
    node: NodeConfig,
    resources: ResourceLimits,
    management: ManagementConfig,
    packet_devices: Arc<dyn PacketDeviceProvider>,
    carrier_network: Arc<dyn CarrierNetworkProvider>,
) -> Result<(), RuntimeError> {
    let mut services = tokio::task::JoinSet::new();
    let mut path_probe_services = Vec::with_capacity(node.clients.len());

    let mut client_contexts = Vec::with_capacity(node.clients.len());
    for (path_group_ordinal, client_config) in node.clients.into_iter().enumerate() {
        let context = client::new_path_context(
            &client_config,
            resources,
            path_group_ordinal,
            carrier_network.clone(),
        )?;
        path_probe_services.push((
            context.clone(),
            client_config.path_probe_interval,
            client_config.path_probe_timeout,
        ));
        client::spawn_ingresses(
            client_config.ingresses,
            context.clone(),
            client_config.performance,
            packet_devices.clone(),
            &mut services,
        );
        client_contexts.push(context);
    }

    let mut server_contexts = Vec::with_capacity(node.servers.len());
    for server_config in node.servers {
        let runtime = server::new_identity_runtime_with_metadata(
            server_config.tag,
            server_config.route_target,
            server_config.bind_paths,
            server_config.outbound,
            server_config.outbound_dns,
            server_config.outbound_connect_timeout,
            server_config.security,
            server_config.performance,
            resources,
        );
        let bound = server::bind_paths(&runtime.paths).await?;
        let server::ServerIdentityRuntime {
            paths,
            reliable_relay,
        } = runtime;
        services.spawn(reliable_relay.run());
        server::spawn_listeners(bound, paths.clone(), &mut services);
        server_contexts.push(paths);
    }

    if management.enabled() {
        spawn_node_management_services(management, client_contexts, server_contexts, &mut services);
    }

    if services.is_empty() {
        return Err(RuntimeError::Protocol("node has no runtime services"));
    }
    for (context, interval, timeout) in path_probe_services {
        client::spawn_path_probe_service(context, interval, timeout, &mut services);
    }
    super::supervise_runtime_services(
        services,
        "node service exited",
        "node has no runtime services",
    )
    .await
}
