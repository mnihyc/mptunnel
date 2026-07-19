//! Server listener composition; carrier loops remain under `runtime::path`.

use crate::config::{
    ManagementConfig, MppPerformanceConfig, ResourceLimits, SecurityConfig, SessionConfig,
};
use crate::outbound::{self, DnsConfig, OutboundConfig, TargetProtocol};
use crate::protocol::UnderlayProtocol;
use crate::runtime::datagram::ServerDatagramService;
use crate::runtime::error::RuntimeError;
use crate::runtime::management::spawn_server_management_services;
use crate::runtime::path::quic::io::UdpPathEndpoint;
use crate::runtime::path::quic::server::{bind_server_udp_endpoint, run_server_udp_listener};
use crate::runtime::path::tcp::server::handle_server_path;
use crate::runtime::path::{ServerLocalPath, ServerPathContext};
use crate::runtime::recent_ids::{RecentIdCache, path_join_replay_cache_capacity};
use crate::runtime::relay::ServerReliableRelayService;
use crate::runtime::telemetry::{RuntimeTelemetry, active_flow_detail_capacity};
use crate::transport::{PathSpec, tcp};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;

/// Per-identity server services and the carrier context they compose.
pub(in crate::runtime) struct ServerIdentityRuntime {
    pub(in crate::runtime) paths: ServerPathContext,
    pub(in crate::runtime) reliable_relay: ServerReliableRelayService,
}

#[allow(clippy::too_many_arguments)]
pub(in crate::runtime) async fn run(
    path_specs: Vec<PathSpec>,
    outbound: OutboundConfig,
    outbound_dns: DnsConfig,
    outbound_connect_timeout: Duration,
    security: SecurityConfig,
    performance: MppPerformanceConfig,
    resources: ResourceLimits,
    session: SessionConfig,
    management: ManagementConfig,
) -> Result<(), RuntimeError> {
    let runtime = new_identity_runtime_with_metadata(
        None,
        path_specs,
        outbound,
        outbound_dns,
        outbound_connect_timeout,
        security,
        performance,
        resources,
        session.retention_timeout,
        management.peer_diagnostics_enabled(),
    );
    let bound = bind_paths(&runtime.paths).await?;
    let ServerIdentityRuntime {
        paths,
        reliable_relay,
    } = runtime;
    let mut services = tokio::task::JoinSet::new();
    services.spawn(reliable_relay.run());
    if management.http_enabled() {
        spawn_server_management_services(management, paths.clone(), &mut services);
    }
    spawn_listeners(bound, paths, &mut services);
    super::supervise_runtime_services(
        services,
        "server service exited",
        "server has no runtime services",
    )
    .await
}

#[cfg(test)]
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
        server_paths,
        outbound,
        outbound_dns,
        outbound_connect_timeout,
        security,
        performance,
        resources,
        SessionConfig::default().retention_timeout,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn new_identity_runtime_with_metadata(
    tag: Option<String>,
    server_paths: Vec<PathSpec>,
    outbound: OutboundConfig,
    outbound_dns: DnsConfig,
    outbound_connect_timeout: Duration,
    security: SecurityConfig,
    performance: MppPerformanceConfig,
    resources: ResourceLimits,
    session_retention_timeout: Duration,
    allow_peer_diagnostics: bool,
) -> ServerIdentityRuntime {
    let mux_limits = resources.into();
    let telemetry = RuntimeTelemetry::new(active_flow_detail_capacity(resources.max_streams));
    let (reliable_streams, reliable_relay) = ServerReliableRelayService::new(
        outbound.clone(),
        outbound_dns.clone(),
        outbound_connect_timeout,
        performance,
        mux_limits,
        session_retention_timeout,
        telemetry.clone(),
    );
    let stream_target_outbound = outbound.clone();
    let reliable_stream_port =
        reliable_streams
            .path_port()
            .with_target_admission(Arc::new(move |target| {
                outbound::validate_target(target)?;
                stream_target_outbound.ensure_supports(TargetProtocol::Tcp)?;
                Ok(())
            }));
    let datagram_port = ServerDatagramService::path_port(
        outbound,
        outbound_dns,
        outbound_connect_timeout,
        session_retention_timeout,
        mux_limits,
        reliable_stream_port.clone(),
        telemetry.clone(),
    );
    let paths = ServerPathContext {
        tag,
        server_paths: Arc::new(server_paths),
        codec_limits: resources.into(),
        mux_limits,
        security,
        reliable_streams: reliable_stream_port,
        datagrams: datagram_port,
        telemetry,
        peer_status: crate::runtime::peer_status::PeerStatusBroker::new(allow_peer_diagnostics),
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
    context: &ServerPathContext,
) -> Result<Vec<BoundServerPath>, RuntimeError> {
    let mut bound = Vec::with_capacity(context.server_paths.len());
    for (config_ordinal, path) in context.server_paths.iter().enumerate() {
        let local_path = ServerLocalPath::new(config_ordinal, path.clone());
        match path.underlay {
            UnderlayProtocol::Tcp => {
                let listener = tcp::bind_listener(path).await?;
                bound.push(BoundServerPath::Tcp {
                    listener,
                    local_path,
                });
            }
            UnderlayProtocol::Udp => {
                let endpoint = bind_server_udp_endpoint(path, context).await?;
                bound.push(BoundServerPath::Udp {
                    endpoint,
                    local_path,
                });
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
            BoundServerPath::Tcp {
                listener,
                local_path,
            } => {
                let context = context.clone();
                listeners.spawn(async move {
                    run_server_tcp_listener(listener, local_path, context).await
                });
            }
            BoundServerPath::Udp {
                endpoint,
                local_path,
            } => {
                let context = context.clone();
                listeners.spawn(async move {
                    run_server_udp_listener(endpoint, local_path, context).await
                });
            }
        }
    }
}

pub(super) enum BoundServerPath {
    Tcp {
        listener: TcpListener,
        local_path: ServerLocalPath,
    },
    Udp {
        endpoint: UdpPathEndpoint,
        local_path: ServerLocalPath,
    },
}

pub(super) async fn run_server_tcp_listener(
    listener: TcpListener,
    local_path: ServerLocalPath,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                stream.set_nodelay(true)?;
                let context = context.clone();
                let local_path = local_path.clone();
                connections.spawn(async move {
                    if let Err(err) = handle_server_path(stream, local_path, context).await {
                        eprintln!("warning: server path handler failed: {err}");
                    }
                });
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(err) = result {
                    eprintln!("warning: server path handler task failed: {err}");
                }
            }
        }
    }
}
