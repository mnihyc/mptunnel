//! Client process lifecycle and background path liveness probes.

use crate::config::{ClientConfig, LocalIngressConfig, ManagementConfig, ResourceLimits};
use crate::ingress::{IngressConfig, ProxyAuthConfig};
use crate::protocol::UnderlayProtocol;
use crate::runtime::error::RuntimeError;
use crate::runtime::ingress_runtime::{
    probe_tcp_client_path, probe_udp_client_path, run_http_connect_client_ingress,
    run_socks5_client_ingress,
};
use crate::runtime::management::run_client_management_api;
use crate::runtime::path::ClientPathContext;
use crate::runtime::tun_l4::run_tun_l4_client;
use std::time::Duration;

pub(super) async fn run(
    client: ClientConfig,
    resources: ResourceLimits,
    management: ManagementConfig,
) -> Result<(), RuntimeError> {
    let path_probe_interval = client.path_probe_interval;
    let path_probe_timeout = client.path_probe_timeout;
    let context = new_path_context(&client, resources)?;
    start_path_probes(context.clone(), path_probe_interval, path_probe_timeout);
    let mut ingresses = tokio::task::JoinSet::new();
    if management.enabled() {
        let context = context.clone();
        ingresses.spawn(async move { run_client_management_api(management, context).await });
    }
    spawn_ingresses(client.ingresses, context, &mut ingresses);
    wait_for_ingress(ingresses).await
}

pub(super) fn new_path_context(
    client: &ClientConfig,
    resources: ResourceLimits,
) -> Result<ClientPathContext, RuntimeError> {
    ClientPathContext::new_with_path_configs_and_target(
        client.paths.clone(),
        resources,
        ProxyAuthConfig::disabled(),
        client.route_target.clone(),
        client.ingresses.clone(),
    )
}

pub(super) fn spawn_ingresses(
    ingresses: Vec<LocalIngressConfig>,
    context: ClientPathContext,
    tasks: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) {
    for ingress in ingresses {
        let context = context.clone();
        match ingress.config {
            IngressConfig::Socks5 { listen, proxy_auth } => {
                tasks.spawn(
                    async move { run_socks5_client_ingress(listen, context, proxy_auth).await },
                );
            }
            IngressConfig::HttpConnect { listen, proxy_auth } => {
                tasks.spawn(async move {
                    run_http_connect_client_ingress(listen, context, proxy_auth).await
                });
            }
            IngressConfig::TunL4(tun) => {
                tasks.spawn(async move { run_tun_l4_client(tun, context).await });
            }
        }
    }
}

async fn wait_for_ingress(
    mut ingresses: tokio::task::JoinSet<Result<(), RuntimeError>>,
) -> Result<(), RuntimeError> {
    if let Some(result) = ingresses.join_next().await {
        match result {
            Ok(Ok(())) => Err(RuntimeError::Protocol("client ingress exited")),
            Ok(Err(err)) => Err(err),
            Err(err) => Err(RuntimeError::TaskJoin(err)),
        }
    } else {
        Err(RuntimeError::Protocol("client has no ingress tasks"))
    }
}

pub(super) fn start_path_probes(context: ClientPathContext, interval: Duration, timeout: Duration) {
    tokio::spawn(async move {
        run_path_probes(context, interval, timeout).await;
    });
}

async fn run_path_probes(context: ClientPathContext, interval: Duration, timeout: Duration) {
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
