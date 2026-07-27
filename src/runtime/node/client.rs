//! Client process lifecycle and background path liveness probes.

use crate::config::{LocalIngressConfig, MppOutboundConfig, RouteTarget};
use crate::ingress::IngressConfig;
use crate::mux::MuxLimits;
use crate::performance::ResourceLimits;
use crate::platform::{PacketDeviceConfig, PacketDeviceProvider};
use crate::protocol::UnderlayProtocol;
use crate::runtime::error::RuntimeError;
use crate::runtime::ingress_runtime::{
    probe_tcp_client_path, probe_udp_client_path, run_http_connect_client_ingress,
    run_socks5_client_ingress, run_tcp_forward_client_ingress, run_udp_forward_client_ingress,
};
use crate::runtime::path::{ClientPathContext, ClientPathRuntimeOptions};
use crate::runtime::product_policy::ClientIngressRouter;
use crate::runtime::readiness::RuntimeReadinessBarrier;
use crate::runtime::telemetry::RuntimeTelemetry;
use crate::runtime::tun_l4::run_tun_l4_client;
use std::sync::Arc;
use std::time::Duration;

pub(super) fn new_path_context(
    client: &MppOutboundConfig,
    resources: ResourceLimits,
    route_target: RouteTarget,
    runtime: ClientPathRuntimeOptions,
    telemetry: RuntimeTelemetry,
) -> Result<ClientPathContext, RuntimeError> {
    ClientPathContext::new_with_runtime_options_and_telemetry(
        client.paths.clone(),
        resources,
        Some(route_target),
        runtime,
        telemetry,
    )
}

pub(super) fn spawn_ingresses(
    ingresses: Vec<LocalIngressConfig>,
    mux_limits: MuxLimits,
    router: ClientIngressRouter,
    packet_devices: Arc<dyn PacketDeviceProvider>,
    readiness: &RuntimeReadinessBarrier,
    tasks: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) {
    for ingress in ingresses {
        let router = router.clone();
        let inbound = ingress
            .tag
            .as_deref()
            .ok_or(RuntimeError::Protocol(
                "local inbound is missing its routing tag",
            ))
            .and_then(|tag| {
                crate::product::InboundId::parse(tag)
                    .map_err(|_| RuntimeError::Protocol("local inbound has an invalid routing tag"))
            });
        match ingress.config {
            IngressConfig::Socks5 {
                listen,
                proxy_auth,
                admission,
            } => {
                let ingress_readiness = readiness.require("SOCKS5 ingress listeners");
                tasks.spawn(async move {
                    run_socks5_client_ingress(
                        listen,
                        mux_limits,
                        router,
                        inbound?,
                        proxy_auth,
                        admission,
                        ingress_readiness,
                    )
                    .await
                });
            }
            IngressConfig::HttpConnect {
                listen,
                proxy_auth,
                admission,
            } => {
                let ingress_readiness = readiness.require("HTTP CONNECT ingress listeners");
                tasks.spawn(async move {
                    run_http_connect_client_ingress(
                        listen,
                        router,
                        inbound?,
                        proxy_auth,
                        admission,
                        ingress_readiness,
                    )
                    .await
                });
            }
            IngressConfig::TcpForward(config) => {
                let ingress_readiness = readiness.require("TCP port-forward ingress listeners");
                tasks.spawn(async move {
                    run_tcp_forward_client_ingress(config, router, inbound?, ingress_readiness)
                        .await
                });
            }
            IngressConfig::UdpForward(config) => {
                let ingress_readiness = readiness.require("UDP port-forward ingress listeners");
                tasks.spawn(async move {
                    run_udp_forward_client_ingress(
                        config,
                        mux_limits,
                        router,
                        inbound?,
                        ingress_readiness,
                    )
                    .await
                });
            }
            IngressConfig::TunL4(tun) => {
                let packet_devices = packet_devices.clone();
                let ingress_readiness = readiness.require("TUN packet stack");
                tasks.spawn(async move {
                    let device = packet_devices
                        .open(&PacketDeviceConfig {
                            name: tun.name.as_deref(),
                            ipv4: tun.ipv4,
                            ipv4_prefix: tun.ipv4_prefix,
                            ipv4_gateway: tun.ipv4_gateway,
                            ipv6: tun.ipv6,
                            ipv6_prefix: tun.ipv6_prefix,
                            mtu: tun.mtu,
                        })
                        .map_err(RuntimeError::TunDevice)?;
                    run_tun_l4_client(tun, mux_limits, router, inbound?, device, ingress_readiness)
                        .await
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
            Err(err) => crate::observability::process_event!(
                Warn,
                "path",
                "probe_task_failed",
                "path probe task failed: {err}"
            ),
        }
    }
}
