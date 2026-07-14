//! Combined-node composition for multiple client and server identities.

use super::{client, server};
use crate::config::{ManagementConfig, NodeConfig, ResourceLimits};
use crate::runtime::error::RuntimeError;
use crate::runtime::management::run_node_management_api;
use crate::runtime::packet_device::PacketDeviceProvider;
use std::sync::Arc;

pub(super) async fn run(
    node: NodeConfig,
    resources: ResourceLimits,
    management: ManagementConfig,
    packet_devices: Arc<dyn PacketDeviceProvider>,
) -> Result<(), RuntimeError> {
    let mut services = tokio::task::JoinSet::new();

    let mut client_contexts = Vec::with_capacity(node.clients.len());
    for client_config in node.clients {
        let context = client::new_path_context(&client_config, resources)?;
        client::start_path_probes(
            context.clone(),
            client_config.path_probe_interval,
            client_config.path_probe_timeout,
        );
        client::spawn_ingresses(
            client_config.ingresses,
            context.clone(),
            packet_devices.clone(),
            &mut services,
        );
        client_contexts.push(context);
    }

    let mut server_contexts = Vec::with_capacity(node.servers.len());
    for server_config in node.servers {
        let runtime = server::new_identity_runtime_with_metadata(
            server_config.tag.clone(),
            server_config.route_target.clone(),
            server_config.bind_paths.clone(),
            server_config.outbound,
            server_config.outbound_dns,
            server_config.outbound_connect_timeout,
            server_config.security,
            server_config.performance,
            resources,
        );
        let bound = server::bind_paths(server_config.bind_paths, &runtime.paths).await?;
        let server::ServerIdentityRuntime {
            paths,
            reliable_relay,
        } = runtime;
        services.spawn(reliable_relay.run());
        server::spawn_listeners(bound, paths.clone(), &mut services);
        server_contexts.push(paths);
    }

    if management.enabled() {
        services.spawn(async move {
            run_node_management_api(management, client_contexts, server_contexts).await
        });
    }

    wait_for_service(services).await
}

async fn wait_for_service(
    mut services: tokio::task::JoinSet<Result<(), RuntimeError>>,
) -> Result<(), RuntimeError> {
    if let Some(result) = services.join_next().await {
        match result {
            Ok(Ok(())) => Err(RuntimeError::Protocol("node service exited")),
            Ok(Err(err)) => Err(err),
            Err(err) => Err(RuntimeError::TaskJoin(err)),
        }
    } else {
        Err(RuntimeError::Protocol("node has no runtime services"))
    }
}
