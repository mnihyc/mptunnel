//! Server listener composition; carrier loops remain under `runtime::path`.

use crate::config::{
    ManagementConfig, MppPerformanceConfig, ResourceLimits, RouteTarget, SecurityConfig,
};
use crate::outbound::{DnsConfig, OutboundConfig};
use crate::protocol::UnderlayProtocol;
use crate::runtime::error::RuntimeError;
use crate::runtime::management::run_server_management_api;
use crate::runtime::path::ServerPathContext;
use crate::runtime::path::quic::io::UdpPathEndpoint;
use crate::runtime::path::quic::server::{bind_server_udp_endpoint, run_server_udp_listener};
use crate::runtime::path::tcp::server::handle_server_path;
use crate::runtime::recent_ids::{RecentIdCache, path_join_replay_cache_capacity};
use crate::runtime::relay::ServerReliableRelayService;
use crate::transport::{PathSpec, tcp};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;

/// Per-identity server services and the carrier context they compose.
pub(in crate::runtime) struct ServerIdentityRuntime {
    pub(in crate::runtime) paths: ServerPathContext,
    pub(in crate::runtime) reliable_relay: ServerReliableRelayService,
}

pub(super) async fn run(
    path_specs: Vec<PathSpec>,
    outbound: OutboundConfig,
    outbound_dns: DnsConfig,
    outbound_connect_timeout: Duration,
    security: SecurityConfig,
    performance: MppPerformanceConfig,
    resources: ResourceLimits,
    management: ManagementConfig,
) -> Result<(), RuntimeError> {
    let runtime = new_identity_runtime(
        path_specs.clone(),
        outbound,
        outbound_dns,
        outbound_connect_timeout,
        security,
        performance,
        resources,
    );
    let bound = bind_paths(path_specs, &runtime.paths).await?;
    let ServerIdentityRuntime {
        paths,
        reliable_relay,
    } = runtime;
    let mut services = tokio::task::JoinSet::new();
    services.spawn(reliable_relay.run());
    if management.enabled() {
        let paths = paths.clone();
        services.spawn(async move { run_server_management_api(management, paths).await });
    }
    spawn_listeners(bound, paths, &mut services);
    wait_for_service(services, "server service exited").await
}

pub(in crate::runtime) fn new_identity_runtime(
    server_paths: Vec<PathSpec>,
    outbound: OutboundConfig,
    outbound_dns: DnsConfig,
    outbound_connect_timeout: Duration,
    security: SecurityConfig,
    performance: MppPerformanceConfig,
    resources: ResourceLimits,
) -> ServerIdentityRuntime {
    new_identity_runtime_with_metadata(
        None,
        None,
        server_paths,
        outbound,
        outbound_dns,
        outbound_connect_timeout,
        security,
        performance,
        resources,
    )
}

pub(super) fn new_identity_runtime_with_metadata(
    tag: Option<String>,
    route_target: Option<RouteTarget>,
    server_paths: Vec<PathSpec>,
    outbound: OutboundConfig,
    outbound_dns: DnsConfig,
    outbound_connect_timeout: Duration,
    security: SecurityConfig,
    performance: MppPerformanceConfig,
    resources: ResourceLimits,
) -> ServerIdentityRuntime {
    let mux_limits = resources.into();
    let (reliable_streams, reliable_relay) = ServerReliableRelayService::new(
        outbound.clone(),
        outbound_dns.clone(),
        outbound_connect_timeout,
        performance,
        mux_limits,
    );
    let paths = ServerPathContext {
        tag,
        route_target,
        server_paths: Arc::new(server_paths),
        outbound,
        outbound_dns,
        outbound_connect_timeout,
        codec_limits: resources.into(),
        mux_limits,
        security,
        reliable_streams,
        path_join_replay: Arc::new(Mutex::new(RecentIdCache::new(
            path_join_replay_cache_capacity(resources.max_streams),
        ))),
        max_udp_flows_per_session: resources.max_streams,
    };
    ServerIdentityRuntime {
        paths,
        reliable_relay,
    }
}

pub(super) async fn bind_paths(
    bind_paths: Vec<PathSpec>,
    context: &ServerPathContext,
) -> Result<Vec<BoundServerPath>, RuntimeError> {
    let mut bound = Vec::with_capacity(bind_paths.len());
    for path in bind_paths {
        match path.underlay {
            UnderlayProtocol::Tcp => {
                let listener = tcp::bind_listener(&path).await?;
                bound.push(BoundServerPath::Tcp(listener));
            }
            UnderlayProtocol::Udp => {
                let endpoint = bind_server_udp_endpoint(&path, context).await?;
                bound.push(BoundServerPath::Udp(endpoint));
            }
        }
    }
    Ok(bound)
}

pub(super) fn spawn_listeners(
    bound: Vec<BoundServerPath>,
    context: ServerPathContext,
    listeners: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) {
    for bound_path in bound {
        match bound_path {
            BoundServerPath::Tcp(listener) => {
                let context = context.clone();
                listeners.spawn(async move { run_server_tcp_listener(listener, context).await });
            }
            BoundServerPath::Udp(endpoint) => {
                let context = context.clone();
                listeners.spawn(async move { run_server_udp_listener(endpoint, context).await });
            }
        }
    }
}

async fn wait_for_service(
    mut services: tokio::task::JoinSet<Result<(), RuntimeError>>,
    exited_message: &'static str,
) -> Result<(), RuntimeError> {
    if let Some(result) = services.join_next().await {
        match result {
            Ok(Ok(())) => Err(RuntimeError::Protocol(exited_message)),
            Ok(Err(err)) => Err(err),
            Err(err) => Err(RuntimeError::TaskJoin(err)),
        }
    } else {
        Err(RuntimeError::Protocol("server has no runtime services"))
    }
}

pub(super) enum BoundServerPath {
    Tcp(TcpListener),
    Udp(UdpPathEndpoint),
}

pub(super) async fn run_server_tcp_listener(
    listener: TcpListener,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    loop {
        let (stream, _) = listener.accept().await?;
        stream.set_nodelay(true)?;
        let context = context.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_server_path(stream, context).await {
                eprintln!("warning: server path handler failed: {err}");
            }
        });
    }
}
