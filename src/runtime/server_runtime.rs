use super::*;

pub(super) async fn run_server(
    bind_paths: Vec<PathSpec>,
    outbound: OutboundConfig,
    outbound_dns: DnsConfig,
    security: SecurityConfig,
    resources: ResourceLimits,
    management: ManagementConfig,
) -> Result<(), RuntimeError> {
    let context = new_server_path_context(
        bind_paths.clone(),
        outbound,
        outbound_dns,
        security,
        resources,
    );
    let bound = bind_server_paths(bind_paths, &context).await?;
    let mut listeners = tokio::task::JoinSet::new();
    if management.enabled() {
        let context = context.clone();
        listeners.spawn(async move { run_server_management_api(management, context).await });
    }
    spawn_server_listeners(bound, context, &mut listeners);
    wait_server_tasks(listeners, "server listener exited").await
}

pub(super) fn new_server_path_context(
    server_paths: Vec<PathSpec>,
    outbound: OutboundConfig,
    outbound_dns: DnsConfig,
    security: SecurityConfig,
    resources: ResourceLimits,
) -> ServerPathContext {
    new_server_path_context_with_identity(
        None,
        None,
        server_paths,
        outbound,
        outbound_dns,
        security,
        resources,
    )
}

pub(super) fn new_server_path_context_with_identity(
    tag: Option<String>,
    route_target: Option<RouteTarget>,
    server_paths: Vec<PathSpec>,
    outbound: OutboundConfig,
    outbound_dns: DnsConfig,
    security: SecurityConfig,
    resources: ResourceLimits,
) -> ServerPathContext {
    ServerPathContext {
        tag,
        route_target,
        server_paths: Arc::new(server_paths),
        outbound,
        outbound_dns,
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

pub(super) async fn bind_server_paths(
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

pub(super) fn spawn_server_listeners(
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

pub(super) async fn wait_server_tasks(
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
        let context = context.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_server_path(stream, context).await {
                eprintln!("warning: server path handler failed: {err}");
            }
        });
    }
}
