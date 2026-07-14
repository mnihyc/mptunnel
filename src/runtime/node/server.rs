//! Server listener composition; carrier loops remain under `runtime::path`.

use crate::config::{
    ManagementConfig, MppPerformanceConfig, ResourceLimits, RouteTarget, SecurityConfig,
};
use crate::model::capacity::QUIC_PERSISTENT_CONGESTION_THRESHOLD;
use crate::mux::MuxLimits;
use crate::outbound::{DnsConfig, OutboundConfig};
use crate::protocol::codec::CodecLimits;
use crate::protocol::{
    AuthNonce, PathId, PathMetricDirection, PathMetrics, SessionId, UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::management::run_server_management_api;
use crate::runtime::path::model::path_startup_metrics;
use crate::runtime::path::quic::io::UdpPathEndpoint;
use crate::runtime::path::quic::server::{bind_server_udp_endpoint, run_server_udp_listener};
use crate::runtime::path::tcp::server::handle_server_path;
use crate::runtime::recent_ids::RecentIdCache;
use crate::runtime::stream::ServerReliableStreamRegistry;
use crate::transport::{PathSpec, tcp};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;

/// Immutable server configuration plus shared carrier and stream registries.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct ServerPathContext {
    pub(in crate::runtime) tag: Option<String>,
    pub(in crate::runtime) route_target: Option<RouteTarget>,
    pub(in crate::runtime) server_paths: Arc<Vec<PathSpec>>,
    pub(in crate::runtime) outbound: OutboundConfig,
    pub(in crate::runtime) outbound_dns: DnsConfig,
    pub(in crate::runtime) outbound_connect_timeout: Duration,
    pub(in crate::runtime) performance: MppPerformanceConfig,
    pub(in crate::runtime) codec_limits: CodecLimits,
    pub(in crate::runtime) mux_limits: MuxLimits,
    pub(in crate::runtime) security: SecurityConfig,
    pub(in crate::runtime) reliable_streams: Arc<ServerReliableStreamRegistry>,
    pub(in crate::runtime) path_join_replay: Arc<Mutex<RecentIdCache<PathJoinReplayKey>>>,
    pub(in crate::runtime) max_reliable_streams: usize,
    pub(in crate::runtime) max_udp_flows_per_session: usize,
}

impl ServerPathContext {
    pub(in crate::runtime) fn local_path_startup_metrics(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
    ) -> Option<PathMetrics> {
        let index = usize::from(path_id.0);
        let path = self.server_paths.get(index)?;
        (path.underlay == underlay)
            .then(|| path_startup_metrics(path, index, PathMetricDirection::ServerToClient))
    }

    pub(in crate::runtime) fn accept_path_join_nonce(
        &self,
        session_id: SessionId,
        path_id: PathId,
        underlay: UnderlayProtocol,
        nonce: AuthNonce,
    ) -> bool {
        let key = PathJoinReplayKey {
            session_id,
            path_id,
            underlay,
            nonce,
        };
        let mut replay = self.path_join_replay.lock().expect("path join replay lock");
        if replay.contains(&key) {
            return false;
        }
        replay.insert(key);
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(in crate::runtime) struct PathJoinReplayKey {
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) underlay: UnderlayProtocol,
    pub(in crate::runtime) nonce: AuthNonce,
}

pub(in crate::runtime) fn path_join_replay_cache_capacity(max_streams: usize) -> usize {
    // Replay retention scales with accepted session concurrency and the QUIC
    // persistent-congestion window rather than one lab-sized fixed cache.
    max_streams
        .max(1)
        .saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD as usize + 1)
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
    let context = new_path_context(
        path_specs.clone(),
        outbound,
        outbound_dns,
        outbound_connect_timeout,
        security,
        performance,
        resources,
    );
    let bound = bind_paths(path_specs, &context).await?;
    let mut listeners = tokio::task::JoinSet::new();
    if management.enabled() {
        let context = context.clone();
        listeners.spawn(async move { run_server_management_api(management, context).await });
    }
    spawn_listeners(bound, context, &mut listeners);
    wait_for_listener(listeners, "server listener exited").await
}

pub(super) fn new_path_context(
    server_paths: Vec<PathSpec>,
    outbound: OutboundConfig,
    outbound_dns: DnsConfig,
    outbound_connect_timeout: Duration,
    security: SecurityConfig,
    performance: MppPerformanceConfig,
    resources: ResourceLimits,
) -> ServerPathContext {
    new_path_context_with_identity(
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

pub(super) fn new_path_context_with_identity(
    tag: Option<String>,
    route_target: Option<RouteTarget>,
    server_paths: Vec<PathSpec>,
    outbound: OutboundConfig,
    outbound_dns: DnsConfig,
    outbound_connect_timeout: Duration,
    security: SecurityConfig,
    performance: MppPerformanceConfig,
    resources: ResourceLimits,
) -> ServerPathContext {
    ServerPathContext {
        tag,
        route_target,
        server_paths: Arc::new(server_paths),
        outbound,
        outbound_dns,
        outbound_connect_timeout,
        performance,
        codec_limits: resources.into(),
        mux_limits: resources.into(),
        security,
        reliable_streams: Arc::new(ServerReliableStreamRegistry::new(resources.max_streams)),
        path_join_replay: Arc::new(Mutex::new(RecentIdCache::new(
            path_join_replay_cache_capacity(resources.max_streams),
        ))),
        max_reliable_streams: resources.max_streams,
        max_udp_flows_per_session: resources.max_streams,
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

async fn wait_for_listener(
    mut listeners: tokio::task::JoinSet<Result<(), RuntimeError>>,
    exited_message: &'static str,
) -> Result<(), RuntimeError> {
    if let Some(result) = listeners.join_next().await {
        match result {
            Ok(Ok(())) => Err(RuntimeError::Protocol(exited_message)),
            Ok(Err(err)) => Err(err),
            Err(err) => Err(RuntimeError::TaskJoin(err)),
        }
    } else {
        Err(RuntimeError::Protocol("server has no listener tasks"))
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
