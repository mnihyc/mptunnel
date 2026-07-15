//! Client process lifecycle and background path liveness probes.

use crate::config::{
    ClientConfig, LocalIngressConfig, ManagementConfig, MppPerformanceConfig, ResourceLimits,
};
use crate::ingress::{IngressConfig, ProxyAuthConfig};
use crate::protocol::UnderlayProtocol;
use crate::runtime::error::RuntimeError;
use crate::runtime::ingress_runtime::{
    probe_tcp_client_path, probe_udp_client_path, run_http_connect_client_ingress,
    run_socks5_client_ingress,
};
use crate::runtime::management::spawn_client_management_services;
use crate::runtime::packet_device::PacketDeviceProvider;
use crate::runtime::path::ClientPathContext;
use crate::runtime::tun_l4::run_tun_l4_client;
use crate::transport::CarrierNetworkProvider;
use std::sync::Arc;
use std::time::Duration;

pub(super) async fn run(
    client: ClientConfig,
    resources: ResourceLimits,
    management: ManagementConfig,
    packet_devices: Arc<dyn PacketDeviceProvider>,
    carrier_network: Arc<dyn CarrierNetworkProvider>,
) -> Result<(), RuntimeError> {
    let path_probe_interval = client.path_probe_interval;
    let path_probe_timeout = client.path_probe_timeout;
    // Product sender policy follows the configured path group without becoming
    // carrier-state ownership in `ClientPathContext`.
    let performance = client.performance;
    let context = new_path_context(&client, resources, 0, carrier_network)?;
    let mut services = tokio::task::JoinSet::new();
    if management.enabled() {
        spawn_client_management_services(management, context.clone(), &mut services);
    }
    spawn_ingresses(
        client.ingresses,
        context.clone(),
        performance,
        packet_devices,
        &mut services,
    );
    if services.is_empty() {
        return Err(RuntimeError::Protocol("client has no ingress tasks"));
    }
    spawn_path_probe_service(
        context,
        path_probe_interval,
        path_probe_timeout,
        &mut services,
    );
    super::supervise_runtime_services(
        services,
        "client ingress exited",
        "client has no ingress tasks",
    )
    .await
}

pub(super) fn new_path_context(
    client: &ClientConfig,
    resources: ResourceLimits,
    path_group_ordinal: usize,
    carrier_network: Arc<dyn CarrierNetworkProvider>,
) -> Result<ClientPathContext, RuntimeError> {
    ClientPathContext::new_with_carrier_network(
        client.paths.clone(),
        resources,
        ProxyAuthConfig::disabled(),
        client.route_target.clone(),
        client.ingresses.clone(),
        path_group_ordinal,
        carrier_network,
    )
}

pub(super) fn spawn_ingresses(
    ingresses: Vec<LocalIngressConfig>,
    context: ClientPathContext,
    performance: MppPerformanceConfig,
    packet_devices: Arc<dyn PacketDeviceProvider>,
    tasks: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) {
    for ingress in ingresses {
        let context = context.clone();
        match ingress.config {
            IngressConfig::Socks5 { listen, proxy_auth } => {
                tasks.spawn(async move {
                    run_socks5_client_ingress(listen, context, proxy_auth, performance).await
                });
            }
            IngressConfig::HttpConnect { listen, proxy_auth } => {
                tasks.spawn(async move {
                    run_http_connect_client_ingress(listen, context, proxy_auth, performance).await
                });
            }
            IngressConfig::TunL4(tun) => {
                let packet_devices = packet_devices.clone();
                tasks.spawn(async move {
                    let device = packet_devices.open(&tun).map_err(RuntimeError::TunDevice)?;
                    run_tun_l4_client(tun, context, performance, device).await
                });
            }
        }
    }
}

pub(super) fn spawn_path_probe_service(
    context: ClientPathContext,
    interval: Duration,
    timeout: Duration,
    services: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) {
    // Probes share node supervision so a restart cannot update stale path state.
    services.spawn(run_path_probes(context, interval, timeout));
}

async fn run_path_probes(
    context: ClientPathContext,
    interval: Duration,
    timeout: Duration,
) -> Result<(), RuntimeError> {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        probe_paths(&context, timeout).await;
    }
}

pub(in crate::runtime) async fn probe_paths(context: &ClientPathContext, timeout: Duration) {
    let mut probes = tokio::task::JoinSet::new();
    for path_index in 0..context.tcp_paths.len() {
        if !context.should_probe_tcp_path(path_index) {
            continue;
        }
        let context = context.clone();
        probes.spawn(async move {
            (
                UnderlayProtocol::Tcp,
                path_index,
                probe_tcp_client_path(&context, path_index, timeout).await,
            )
        });
    }
    for path_index in 0..context.udp_paths.len() {
        if !context.should_probe_udp_path(path_index) {
            continue;
        }
        let context = context.clone();
        probes.spawn(async move {
            (
                UnderlayProtocol::Udp,
                path_index,
                probe_udp_client_path(&context, path_index, timeout).await,
            )
        });
    }

    while let Some(result) = probes.join_next().await {
        match result {
            Ok((UnderlayProtocol::Tcp, path_index, Ok(elapsed))) => {
                context.mark_tcp_path_probe_success(path_index, elapsed);
            }
            Ok((UnderlayProtocol::Tcp, path_index, Err(_))) => {
                context.mark_tcp_path_failure(path_index);
            }
            Ok((UnderlayProtocol::Udp, path_index, Ok(elapsed))) => {
                context.mark_udp_path_probe_success(path_index, elapsed);
            }
            Ok((UnderlayProtocol::Udp, path_index, Err(_))) => {
                context.mark_udp_path_failure(path_index);
            }
            Err(err) => eprintln!("warning: path probe task failed: {err}"),
        }
    }
}
