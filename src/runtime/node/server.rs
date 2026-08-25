//! Server listener composition; carrier loops remain under `runtime::path`.

use crate::config::{
    ForwardingMode, NamedPathConfig, PeerDiagnosticsPrincipalPolicy, ServerSecurityConfig,
};
#[cfg(test)]
use crate::config::{ManagementConfig, ProductPolicyConfig, SessionConfig};
use crate::outbound;
#[cfg(test)]
use crate::outbound::OutboundConfig;
use crate::performance::{MppPerformanceConfig, ResourceLimits};
use crate::product::InboundId;
#[cfg(test)]
use crate::product::{
    EgressAction, InitialDemand, OutboundId, RouteAction, RouteMatchSpec, RouteRuleSpec, RuleId,
};
use crate::protocol::UnderlayProtocol;
#[cfg(test)]
use crate::runtime::config_control::RuntimeConfigControl;
use crate::runtime::datagram::{ServerDatagramService, ServerDatagramServiceConfig};
use crate::runtime::error::RuntimeError;
#[cfg(test)]
use crate::runtime::management::spawn_node_management_services;
#[cfg(test)]
use crate::runtime::outbound_registry::RuntimeOutboundLeaf;
use crate::runtime::outbound_registry::RuntimeOutboundRegistry;
use crate::runtime::path::authentication::ProductCredentialAdmission;
use crate::runtime::path::quic::io::UdpPathEndpoint;
use crate::runtime::path::quic::server::{bind_server_udp_endpoint, run_server_udp_listener};
use crate::runtime::path::tcp::server::handle_server_path_with_authentication_slot;
use crate::runtime::path::{
    CredentialRetirementControl, ServerLocalPath, ServerPathContext, ServerTargetAdmission,
};
use crate::runtime::product_policy::{ClientIngressRouter, ClientPolicyDisposition, ClientRoute};
#[cfg(test)]
use crate::runtime::readiness::RuntimeReadinessBarrier;
use crate::runtime::recent_ids::{ExpiringReplayCache, path_join_replay_cache_capacity};
use crate::runtime::relay::{ServerReliableRelayContext, ServerReliableRelayService};
use crate::runtime::telemetry::RuntimeTelemetry;
#[cfg(test)]
use crate::runtime::telemetry::active_flow_detail_capacity;
use crate::runtime::tun_l3::ServerIpTunnelService;
#[cfg(test)]
use crate::transport::PathSpec;
use crate::transport::encrypted::TcpServerTlsConfig;
use crate::transport::tcp;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

/// Per-identity server services and the carrier context they compose.
pub(in crate::runtime) struct ServerIdentityRuntime {
    pub(in crate::runtime) paths: ServerPathContext,
    pub(in crate::runtime) reliable_relay: Option<ServerReliableRelayService>,
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(in crate::runtime) async fn run(
    path_specs: Vec<PathSpec>,
    outbound: OutboundConfig,
    outbound_connect_timeout: Duration,
    security: ServerSecurityConfig,
    tls: TcpServerTlsConfig,
    performance: MppPerformanceConfig,
    resources: ResourceLimits,
    session: SessionConfig,
    management: ManagementConfig,
    config_control: Option<RuntimeConfigControl>,
) -> Result<(), RuntimeError> {
    let id = OutboundId::parse("test-server-egress")
        .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?;
    let registry = RuntimeOutboundRegistry::compile(
        [RuntimeOutboundLeaf::Local {
            id: id.clone(),
            config: outbound,
            connect_timeout: outbound_connect_timeout,
            native_sockets: Arc::new(crate::transport::SystemNativeSocketConfigurator),
        }],
        &[],
        crate::runtime::outbound_registry::test_dns_generation(),
    )?;
    let product_admission = registry.product_admission().clone();
    let router = test_router(&registry, id.clone())?;
    let runtime = new_identity_runtime_with_metadata(
        "test-mpp-inbound".to_string(),
        path_specs
            .into_iter()
            .enumerate()
            .map(|(index, spec)| NamedPathConfig {
                name: format!("path-{}", index + 1),
                spec,
            })
            .collect(),
        registry,
        Some(router),
        security,
        tls,
        performance,
        resources,
        RuntimeTelemetry::generation_owner(active_flow_detail_capacity(resources.max_streams)),
        session.retention_timeout,
        management.peer_diagnostics_enabled(),
        PeerDiagnosticsPrincipalPolicy::Deny,
        ForwardingMode::L4,
        None,
    )?;
    let generation = config_control
        .as_ref()
        .map(RuntimeConfigControl::generation)
        .unwrap_or_default();
    let readiness = RuntimeReadinessBarrier::new(generation.clone());
    let server_readiness = readiness.require("MPP server listeners");
    let bound = bind_paths(&runtime.paths).await?;
    let ServerIdentityRuntime {
        paths,
        reliable_relay,
    } = runtime;
    let mut services = tokio::task::JoinSet::new();
    if let Some(reliable_relay) = reliable_relay {
        services.spawn(reliable_relay.run());
    }
    spawn_listeners(bound, paths.clone(), &mut services);
    server_readiness.ready();
    if management.http_enabled() {
        let management_readiness = readiness.require("management listeners");
        let product_telemetry = paths.telemetry.clone();
        if let Err(error) = spawn_node_management_services(
            management,
            Vec::new(),
            vec![paths],
            crate::runtime::management::ProductRuntimeInventory::default(),
            crate::runtime::management::TunL3RuntimeInventory::default(),
            product_telemetry,
            config_control,
            None,
            None,
            product_admission,
            generation.clone(),
            management_readiness,
            &mut services,
        )
        .await
        {
            super::retire_runtime_services(&mut services).await;
            return Err(error);
        }
    }
    readiness.seal();
    let result = super::supervise_runtime_services(
        services,
        &generation,
        "server service exited",
        "server has no runtime services",
    )
    .await
    .map(|_| ());
    if let Err(error) = &result {
        generation.mark_failed(error.to_string());
    }
    result
}

#[cfg(test)]
pub(in crate::runtime) fn new_identity_runtime(
    server_paths: Vec<PathSpec>,
    outbound: OutboundConfig,
    outbound_connect_timeout: Duration,
    security: ServerSecurityConfig,
    performance: MppPerformanceConfig,
    resources: ResourceLimits,
) -> ServerIdentityRuntime {
    let id = OutboundId::parse("test-server-egress").expect("static test outbound ID");
    let registry = RuntimeOutboundRegistry::compile(
        [RuntimeOutboundLeaf::Local {
            id: id.clone(),
            config: outbound,
            connect_timeout: outbound_connect_timeout,
            native_sockets: Arc::new(crate::transport::SystemNativeSocketConfigurator),
        }],
        &[],
        crate::runtime::outbound_registry::test_dns_generation(),
    )
    .expect("test outbound registry");
    let router = test_router(&registry, id).expect("test routing policy");
    new_identity_runtime_with_metadata(
        "test-mpp-inbound".to_string(),
        server_paths
            .into_iter()
            .enumerate()
            .map(|(index, spec)| NamedPathConfig {
                name: format!("path-{}", index + 1),
                spec,
            })
            .collect(),
        registry,
        Some(router),
        security,
        crate::transport::encrypted::test_server_tls_config(),
        performance,
        resources,
        RuntimeTelemetry::new(active_flow_detail_capacity(resources.max_streams)),
        SessionConfig::default().retention_timeout,
        false,
        PeerDiagnosticsPrincipalPolicy::Deny,
        ForwardingMode::L4,
        None,
    )
    .expect("test server identity runtime")
}

#[allow(clippy::too_many_arguments)]
pub(super) fn new_identity_runtime_with_metadata(
    name: String,
    configured_paths: Vec<NamedPathConfig>,
    _outbound_registry: RuntimeOutboundRegistry,
    router: Option<ClientIngressRouter>,
    security: ServerSecurityConfig,
    tls: TcpServerTlsConfig,
    performance: MppPerformanceConfig,
    resources: ResourceLimits,
    telemetry: RuntimeTelemetry,
    session_retention_timeout: Duration,
    global_allow_peer_diagnostics: bool,
    peer_diagnostics_principals: PeerDiagnosticsPrincipalPolicy,
    forwarding_mode: ForwardingMode,
    tun_l3: Option<crate::product::TunL3AddressPlan>,
) -> Result<ServerIdentityRuntime, RuntimeError> {
    let (configured_path_names, server_paths): (Vec<_>, Vec<_>) = configured_paths
        .into_iter()
        .map(|path| (path.name, path.spec))
        .unzip();
    let mux_limits = resources.into();
    let inbound =
        InboundId::parse(&name).map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?;
    let (reliable_stream_port, datagram_port, reliable_relay) = match (router, tun_l3.is_some()) {
        (Some(router), false) => {
            let (reliable_streams, reliable_relay) =
                ServerReliableRelayService::new(ServerReliableRelayContext {
                    router: router.clone(),
                    inbound: inbound.clone(),
                    performance,
                    mux_limits,
                    max_paths_per_session: resources.max_paths,
                    session_retention_timeout,
                    telemetry: telemetry.clone(),
                });
            let admission_router = router.clone();
            let admission_inbound = inbound.clone();
            let reliable_stream_port = reliable_streams.path_port().with_target_admission(
                Arc::new(move |permit, ingress, target| {
                    outbound::validate_target(target)?;
                    match admission_router.preflight_mpp_tcp_with_ingress(
                        target,
                        permit.principal().clone(),
                        admission_inbound.clone(),
                        ingress,
                    )? {
                        ClientRoute::Open(_) => Ok(ServerTargetAdmission::Allow),
                        ClientRoute::Deny(ClientPolicyDisposition::Reject) => {
                            Ok(ServerTargetAdmission::Reject)
                        }
                        ClientRoute::Deny(ClientPolicyDisposition::Drop) => {
                            Ok(ServerTargetAdmission::Drop)
                        }
                    }
                }),
            );
            let datagram_port = ServerDatagramService::path_port(ServerDatagramServiceConfig {
                router,
                inbound,
                session_retention_timeout,
                mux_limits,
                reliable_streams: reliable_stream_port.clone(),
                telemetry: telemetry.clone(),
            });
            (
                reliable_stream_port,
                Some(datagram_port),
                Some(reliable_relay),
            )
        }
        (None, true) => {
            let (reliable_streams, receiver) = crate::runtime::stream::ServerReliableStreamRegistry::new_accepting_with_limits_and_retention(
                mux_limits,
                resources.max_paths,
                session_retention_timeout,
            );
            drop(receiver);
            (reliable_streams.path_port(), None, None)
        }
        (None, false) => {
            return Err(RuntimeError::Protocol(
                "MPP L4 inbound is missing the shared routing policy",
            ));
        }
        (Some(_), true) => {
            return Err(RuntimeError::Protocol(
                "MPP L3 inbound must not construct L4 routing services",
            ));
        }
    };
    let (ip_tunnels, ip_tunnel_device) = tun_l3.map_or((None, None), |plan| {
        let (port, device) = ServerIpTunnelService::build(
            plan,
            reliable_stream_port.clone(),
            resources.max_paths,
            mux_limits.max_datagram_queue_bytes,
            session_retention_timeout,
        );
        (Some(port), Some(device))
    });
    let credential_admission = ProductCredentialAdmission::from_security(&security);
    let pending_authentications = Arc::new(Semaphore::new(security.max_pending_authentications));
    let silent_rejections = Arc::new(Semaphore::new(security.max_pending_authentications));
    let paths = ServerPathContext {
        name,
        forwarding_mode,
        configured_path_names: Arc::new(configured_path_names),
        server_paths: Arc::new(server_paths),
        codec_limits: resources.into(),
        mux_limits,
        security,
        credential_admission,
        credential_retirements: CredentialRetirementControl::new(),
        pending_authentications,
        silent_rejections,
        tls,
        reliable_streams: reliable_stream_port,
        datagrams: datagram_port,
        ip_tunnels,
        ip_tunnel_device: Arc::new(Mutex::new(ip_tunnel_device)),
        telemetry,
        peer_status: crate::runtime::peer_status::PeerStatusBroker::with_scoped_incoming(
            global_allow_peer_diagnostics,
            peer_diagnostics_principals.has_authorized_principals(),
        ),
        peer_diagnostics_principals,
        path_join_replay: Arc::new(Mutex::new(ExpiringReplayCache::new(
            path_join_replay_cache_capacity(resources.max_streams),
        ))),
        max_udp_flows_per_session: resources.max_streams,
        session_retention_timeout,
    };
    Ok(ServerIdentityRuntime {
        paths,
        reliable_relay,
    })
}

#[cfg(test)]
fn test_router(
    registry: &RuntimeOutboundRegistry,
    id: OutboundId,
) -> Result<ClientIngressRouter, RuntimeError> {
    let policy = ProductPolicyConfig {
        generation: 1,
        routes: vec![RouteRuleSpec::new(
            RuleId::parse("test-default")
                .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?,
            RouteMatchSpec::default(),
            RouteAction::allow_restricted(
                EgressAction::Outbound(id),
                None,
                InitialDemand::Automatic,
            ),
        )],
    };
    ClientIngressRouter::new(&policy, registry.clone())
}

#[cfg(test)]
#[test]
fn l3_identity_runtime_builds_packet_service_without_l4_relay() {
    let security = ServerSecurityConfig::for_test(
        crate::config::SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("test shared secret"),
    );
    let plan = crate::product::TunL3AddressPlan::compile(
        crate::product::TunL3ServerSpec {
            interface_name: Some("mptun-test".to_string()),
            ipv4_pool: Some("10.88.0.0/24".parse().expect("IPv4 pool")),
            ipv4: Some("10.88.0.1".parse().expect("server address")),
            ipv6_pool: None,
            ipv6: None,
            mtu: 1_400,
            allocations: vec![crate::product::TunL3AllocationSpec {
                principal_id: crate::product::PrincipalId::parse("test-peer")
                    .expect("test principal"),
                ipv4: Some("10.88.0.2".parse().expect("peer address")),
                ipv6: None,
                allowed_ips: Vec::new(),
            }],
        },
        &security.credential_authority,
    )
    .expect("TUN-L3 plan");
    let registry = RuntimeOutboundRegistry::compile(
        std::iter::empty::<RuntimeOutboundLeaf>(),
        &[],
        crate::runtime::outbound_registry::test_dns_generation(),
    )
    .expect("empty L3 outbound registry");
    let resources = ResourceLimits::default();
    let runtime = new_identity_runtime_with_metadata(
        "packet-server".to_string(),
        vec![NamedPathConfig {
            name: "path-1".to_string(),
            spec: "tcp://127.0.0.1:7443".parse().expect("server path"),
        }],
        registry,
        None,
        security,
        crate::transport::encrypted::test_server_tls_config(),
        MppPerformanceConfig::default(),
        resources,
        RuntimeTelemetry::new(active_flow_detail_capacity(resources.max_streams)),
        SessionConfig::default().retention_timeout,
        false,
        PeerDiagnosticsPrincipalPolicy::Deny,
        ForwardingMode::L3,
        Some(plan),
    )
    .expect("L3 server runtime");

    assert!(runtime.reliable_relay.is_none());
    assert!(runtime.paths.datagrams.is_none());
    assert!(runtime.paths.ip_tunnels.is_some());
}

pub(super) async fn bind_paths(
    context: &ServerPathContext,
) -> Result<Vec<BoundServerPath>, RuntimeError> {
    if context.server_paths.is_empty() {
        return Err(RuntimeError::Protocol("MPP server has no path listeners"));
    }
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
    for path in &bound {
        let (local_path, local_address, underlay) = match path {
            BoundServerPath::Tcp {
                listener,
                local_path,
            } => (local_path, listener.local_addr()?, UnderlayProtocol::Tcp),
            BoundServerPath::Udp {
                endpoint,
                local_path,
            } => (local_path, endpoint.local_addr()?, UnderlayProtocol::Udp),
        };
        let transport = crate::transport::encrypted::carrier_security_description(
            underlay,
            context.tls.shared_transport_secret_configured(),
        );
        let path_name = context
            .configured_path_names
            .get(local_path.config_ordinal())
            .map(String::as_str)
            .unwrap_or("unnamed");
        crate::observability::emit_lifecycle(
            crate::config::LogLevel::Info,
            "inbound",
            "listening",
            format_args!(
                "{}: MPP path {path_name} listening on {local_address} over {transport}",
                context.name
            ),
        );
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

#[allow(
    clippy::large_enum_variant,
    reason = "server paths are allocated only at startup; boxing would add allocation and indirection to avoid stack-only ownership"
)]
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
            biased;
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(err) = result {
                    crate::observability::process_event!(
                        Warn,
                        "path",
                        "server_handler_task_failed",
                        "server path handler task failed: {err}"
                    );
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                stream.set_nodelay(true)?;
                try_spawn_server_tcp_connection(
                    &mut connections,
                    stream,
                    local_path.clone(),
                    context.clone(),
                );
            }
        }
    }
}

pub(super) fn try_spawn_server_tcp_connection(
    connections: &mut tokio::task::JoinSet<()>,
    stream: tokio::net::TcpStream,
    local_path: ServerLocalPath,
    context: ServerPathContext,
) -> bool {
    let authentication_slot = match context.try_begin_authentication() {
        Ok(authentication_slot) => authentication_slot,
        Err(_) => return false,
    };
    connections.spawn(async move {
        if let Err(err) = handle_server_path_with_authentication_slot(
            stream,
            local_path,
            context,
            authentication_slot,
        )
        .await
        {
            crate::observability::process_event!(
                Warn,
                "path",
                "server_handler_failed",
                "server path handler failed: {err}"
            );
        }
    });
    true
}
