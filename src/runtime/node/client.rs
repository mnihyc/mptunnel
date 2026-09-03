//! Client process lifecycle and background path liveness probes.

use crate::config::{LocalIngressConfig, MppOutboundConfig, ProductFlowConfig};
use crate::ingress::IngressConfig;
use crate::model::path::CarrierPathInstanceId;
use crate::mux::MuxLimits;
use crate::performance::ResourceLimits;
use crate::platform::{PacketDeviceConfig, PacketDeviceProvider};
use crate::product::OutboundId;
use crate::runtime::error::RuntimeError;
#[cfg(test)]
use crate::runtime::ingress_runtime::probe_tcp_client_path;
use crate::runtime::ingress_runtime::{
    probe_udp_client_path, spawn_http_connect_client_ingress, spawn_mixed_client_ingress,
    spawn_socks5_client_ingress, spawn_tcp_forward_client_ingress,
    spawn_udp_forward_client_ingress,
};
use crate::runtime::path::tcp::group::ClientTcpMemberRetry;
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
    outbound: OutboundId,
    runtime: ClientPathRuntimeOptions,
    telemetry: RuntimeTelemetry,
) -> Result<ClientPathContext, RuntimeError> {
    ClientPathContext::new_with_runtime_options_and_telemetry(
        client.paths.clone(),
        resources,
        Some(outbound),
        runtime,
        telemetry,
    )
}

pub(super) async fn spawn_ingresses(
    ingresses: Vec<LocalIngressConfig>,
    mux_limits: MuxLimits,
    router: ClientIngressRouter,
    packet_devices: Arc<dyn PacketDeviceProvider>,
    flow: ProductFlowConfig,
    readiness: &RuntimeReadinessBarrier,
    tasks: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) -> Result<(), RuntimeError> {
    for ingress in ingresses {
        let router = router.clone();
        let inbound = crate::product::InboundId::parse(&ingress.name)
            .map_err(|_| RuntimeError::Protocol("local inbound has an invalid name"))?;
        match ingress.config {
            IngressConfig::Socks5 {
                listen,
                proxy_auth,
                admission,
            } => {
                let ingress_readiness = readiness.require("SOCKS5 ingress listeners");
                spawn_socks5_client_ingress(
                    listen,
                    mux_limits,
                    router,
                    inbound,
                    proxy_auth,
                    admission,
                    flow.idle_timeout,
                    ingress_readiness,
                    tasks,
                )
                .await?;
            }
            IngressConfig::HttpConnect {
                listen,
                proxy_auth,
                admission,
            } => {
                let ingress_readiness = readiness.require("HTTP CONNECT ingress listeners");
                spawn_http_connect_client_ingress(
                    listen,
                    router,
                    inbound,
                    proxy_auth,
                    admission,
                    flow.idle_timeout,
                    ingress_readiness,
                    tasks,
                )
                .await?;
            }
            IngressConfig::Mixed {
                listen,
                proxy_auth,
                admission,
            } => {
                let ingress_readiness = readiness.require("mixed proxy ingress listeners");
                spawn_mixed_client_ingress(
                    listen,
                    mux_limits,
                    router,
                    inbound,
                    proxy_auth,
                    admission,
                    flow.idle_timeout,
                    ingress_readiness,
                    tasks,
                )
                .await?;
            }
            IngressConfig::TcpForward(config) => {
                let ingress_readiness = readiness.require("TCP port-forward ingress listeners");
                spawn_tcp_forward_client_ingress(
                    config,
                    router,
                    inbound,
                    flow.idle_timeout,
                    ingress_readiness,
                    tasks,
                )
                .await?;
            }
            IngressConfig::UdpForward(config) => {
                let ingress_readiness = readiness.require("UDP port-forward ingress listeners");
                spawn_udp_forward_client_ingress(
                    config,
                    mux_limits,
                    router,
                    inbound,
                    flow.idle_timeout,
                    ingress_readiness,
                    tasks,
                )
                .await?;
            }
            IngressConfig::MixedForward(config) => {
                let tcp_readiness = readiness.require("mixed-forward TCP ingress listeners");
                let udp_readiness = readiness.require("mixed-forward UDP ingress listeners");
                let (tcp, udp) = config.into_configs();
                spawn_tcp_forward_client_ingress(
                    tcp,
                    router.clone(),
                    inbound.clone(),
                    flow.idle_timeout,
                    tcp_readiness,
                    tasks,
                )
                .await?;
                spawn_udp_forward_client_ingress(
                    udp,
                    mux_limits,
                    router,
                    inbound,
                    flow.idle_timeout,
                    udp_readiness,
                    tasks,
                )
                .await?;
            }
            IngressConfig::TunL4(tun) => {
                let packet_devices = packet_devices.clone();
                let ingress_readiness = readiness.require("TUN packet stack");
                tasks.spawn(async move {
                    let device = packet_devices
                        .open(&PacketDeviceConfig {
                            interface_name: tun.interface_name.as_deref(),
                            ipv4: tun.ipv4,
                            ipv4_prefix: tun.ipv4_prefix,
                            ipv4_gateway: tun.ipv4_gateway,
                            ipv6: tun.ipv6,
                            ipv6_prefix: tun.ipv6_prefix,
                            mtu: tun.mtu,
                        })
                        .map_err(RuntimeError::TunDevice)?;
                    run_tun_l4_client(
                        tun,
                        mux_limits,
                        router,
                        inbound,
                        device,
                        flow.idle_timeout,
                        ingress_readiness,
                    )
                    .await
                });
            }
        }
    }
    Ok(())
}

pub(super) fn spawn_path_probe_service(
    context: ClientPathContext,
    interval: Duration,
    timeout: Duration,
    services: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) {
    services.spawn(run_path_probe_service(context, interval, timeout));
}

pub(in crate::runtime) async fn run_path_probe_service(
    context: ClientPathContext,
    interval: Duration,
    probe_timeout: Duration,
) -> Result<(), RuntimeError> {
    let groups = context.tcp_carrier_groups.clone();
    let mut changes = groups.subscribe();
    let mut udp_changes = context.udp_carrier_reconciliation.subscribe();
    let now = tokio::time::Instant::now();
    let mut retry = vec![ClientTcpMemberRetry::new(now); context.tcp_sessions.len()];
    let mut measurements = tokio::task::JoinSet::new();

    // Preserve immediate UDP measurement while the same service establishes
    // the bounded TCP carrier target.
    {
        let context = context.clone();
        measurements.spawn(async move {
            probe_selected_paths(&context, probe_timeout, TcpProbeSelection::None).await;
        });
    }

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;

    groups.reconcile(&context, interval, &mut retry).await;
    reconcile_udp_carrier_owners(&context).await;

    loop {
        let maintenance_at = match (
            groups.next_maintenance_at(&context, &retry),
            next_udp_carrier_reconciliation_at(&context),
        ) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
        let maintenance_timer = async {
            match maintenance_at {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(maintenance_timer);
        tokio::select! {
            changed = changes.changed() => {
                changed.expect("TCP carrier group sender lives with path context");
                groups
                    .reconcile(&context, interval, &mut retry)
                    .await;
            }
            changed = udp_changes.changed() => {
                changed.expect("QUIC owner reconciliation sender lives with path context");
                reconcile_udp_carrier_owners(&context).await;
            }
            _ = &mut maintenance_timer => {
                groups
                    .reconcile(&context, interval, &mut retry)
                    .await;
                reconcile_udp_carrier_owners(&context).await;
            }
            _ = ticker.tick() => {
                if measurements.is_empty() {
                    let context = context.clone();
                    measurements.spawn(async move {
                        probe_selected_paths(&context, probe_timeout, TcpProbeSelection::None).await;
                    });
                }
                groups
                    .reconcile(&context, interval, &mut retry)
                    .await;
            }
            measurement = measurements.join_next(), if !measurements.is_empty() => {
                if let Some(Err(error)) = measurement {
                    crate::observability::process_event!(
                        Warn,
                        "path",
                        "probe_batch_failed",
                        "path measurement batch failed: {error}"
                    );
                }
            }
        }
    }
}

fn next_udp_carrier_reconciliation_at(context: &ClientPathContext) -> Option<tokio::time::Instant> {
    context
        .udp_sessions
        .iter()
        .filter_map(|session| session.reconciliation_deadline())
        .min()
}

/// Reconciles configured QUIC physical owners only. Optional measurement is
/// intentionally absent: active Product flows, probe eligibility, and the
/// periodic measurement ticker cannot suppress a missing owner.
async fn reconcile_udp_carrier_owners(context: &ClientPathContext) {
    if context.ensure_session_active().is_err() {
        return;
    }
    let now = tokio::time::Instant::now();
    let mut attempts = tokio::task::JoinSet::new();
    for session in context.udp_sessions.iter() {
        if session
            .reconciliation_deadline()
            .is_some_and(|not_before| not_before <= now)
        {
            let session = session.clone();
            attempts.spawn(async move { session.reconcile_connection_owner().await });
        }
    }
    while let Some(attempt) = attempts.join_next().await {
        if let Err(error) = attempt {
            crate::observability::process_event!(
                Warn,
                "quic",
                "owner_reconciliation_task_failed",
                "QUIC carrier-owner reconciliation task failed: {error}"
            );
        }
    }
}

#[cfg(test)]
pub(in crate::runtime) async fn probe_paths(context: &ClientPathContext, timeout: Duration) {
    probe_selected_paths(context, timeout, TcpProbeSelection::All).await;
}

#[derive(Clone, Copy)]
enum TcpProbeSelection {
    None,
    #[cfg(test)]
    All,
}

enum PathProbeResult {
    #[cfg(test)]
    Tcp,
    Udp {
        path_index: usize,
        expected_path_instance_id: Option<CarrierPathInstanceId>,
        result: Result<Option<(CarrierPathInstanceId, Duration)>, RuntimeError>,
    },
}

async fn probe_selected_paths(
    context: &ClientPathContext,
    timeout: Duration,
    tcp: TcpProbeSelection,
) {
    let mut probes = tokio::task::JoinSet::new();
    match tcp {
        TcpProbeSelection::None => {}
        #[cfg(test)]
        TcpProbeSelection::All => {
            for path_index in 0..context.tcp_paths.len() {
                if !context.should_probe_tcp_path(path_index) {
                    continue;
                }
                let context = context.clone();
                probes.spawn(async move {
                    probe_tcp_client_path(&context, path_index, timeout).await;
                    PathProbeResult::Tcp
                });
            }
        }
    }
    for path_index in 0..context.udp_paths.len() {
        let Some(expected_path_instance_id) = context.udp_path_probe_expected_instance(path_index)
        else {
            continue;
        };
        let context = context.clone();
        probes.spawn(async move {
            PathProbeResult::Udp {
                path_index,
                expected_path_instance_id,
                result: probe_udp_client_path(&context, path_index, timeout).await,
            }
        });
    }

    while let Some(result) = probes.join_next().await {
        match result {
            #[cfg(test)]
            Ok(PathProbeResult::Tcp) => {}
            Ok(PathProbeResult::Udp {
                path_index,
                result: Ok(Some((path_instance_id, elapsed))),
                ..
            }) => {
                context.mark_udp_path_probe_success_for_instance(
                    path_index,
                    path_instance_id,
                    elapsed,
                );
            }
            Ok(PathProbeResult::Udp {
                result: Ok(None), ..
            }) => {}
            Ok(PathProbeResult::Udp {
                path_index,
                expected_path_instance_id,
                result: Err(_),
            }) => {
                context.mark_udp_path_establishment_failure_if_current(
                    path_index,
                    expected_path_instance_id,
                );
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
