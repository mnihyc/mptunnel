//! Process composition for client, server, and combined-node roles.
//!
//! Node code starts long-lived services. Carrier state and scheduling policy
//! stay in their owning `path`, `stream`, and `sender` modules.

mod client;
mod combined;
pub(super) mod server;

#[cfg(test)]
pub(in crate::runtime) use client::probe_paths;

use crate::config::{AppConfig, CommandConfig};
use crate::runtime::error::RuntimeError;
use crate::runtime::packet_device::{PacketDeviceProvider, SystemPacketDeviceProvider};
use crate::transport::{CarrierSocketProvider, SystemCarrierSocketProvider};
use std::sync::Arc;
use tokio::task::JoinSet;

pub async fn run(config: AppConfig) -> Result<(), RuntimeError> {
    run_with_host_providers(
        config,
        Arc::new(SystemPacketDeviceProvider),
        Arc::new(SystemCarrierSocketProvider),
    )
    .await
}

/// Runs a process with host-controlled packet-device construction.
///
/// Hosts that only customize packet-device construction use this entry point.
/// Full-tunnel mobile VPNs must also protect carrier sockets through
/// [`run_with_host_providers`].
pub async fn run_with_packet_device_provider(
    config: AppConfig,
    packet_devices: Arc<dyn PacketDeviceProvider>,
) -> Result<(), RuntimeError> {
    run_with_host_providers(
        config,
        packet_devices,
        Arc::new(SystemCarrierSocketProvider),
    )
    .await
}

/// Runs a process with both packet-device and carrier-network host adapters.
pub async fn run_with_host_providers(
    config: AppConfig,
    packet_devices: Arc<dyn PacketDeviceProvider>,
    carrier_sockets: Arc<dyn CarrierSocketProvider>,
) -> Result<(), RuntimeError> {
    match config.command {
        CommandConfig::Client(client) => {
            client::run(
                client,
                config.resources,
                config.management,
                packet_devices,
                carrier_sockets,
            )
            .await
        }
        CommandConfig::Server(server) => {
            server::run(
                server.bind_paths,
                server.outbound,
                server.outbound_dns,
                server.outbound_connect_timeout,
                server.security,
                server.performance,
                config.resources,
                config.management,
            )
            .await
        }
        CommandConfig::Node(node) => {
            combined::run(
                node,
                config.resources,
                config.management,
                packet_devices,
                carrier_sockets,
            )
            .await
        }
    }
}

/// Returns the first service result after retiring the rest of this runtime generation.
pub(super) async fn supervise_runtime_services(
    mut services: JoinSet<Result<(), RuntimeError>>,
    exited_message: &'static str,
    empty_message: &'static str,
) -> Result<(), RuntimeError> {
    let result = match services.join_next().await {
        Some(Ok(Ok(()))) => Err(RuntimeError::Protocol(exited_message)),
        Some(Ok(Err(err))) => Err(err),
        Some(Err(err)) => Err(RuntimeError::TaskJoin(err)),
        None => Err(RuntimeError::Protocol(empty_message)),
    };

    // A restart must not overlap services that still hold the prior generation's state.
    services.abort_all();
    while services.join_next().await.is_some() {}
    result
}
