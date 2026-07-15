//! Management sampling and HTTP control-plane services.
//!
//! Samplers are node-supervised siblings of listeners so restarts retire both.

#[cfg(test)]
use super::*;
use crate::config::{ManagementConfig, RouteTarget, RouteTargetKind};
use crate::ingress::IngressConfig;
use crate::protocol::{PathMetricDirection, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ServerStreamManagementSnapshot;
use crate::runtime::path::model::{path_record_failure_cooldown, path_snapshot};
use crate::runtime::path::{ClientPathContext, ClientPathHealthRecord, ServerPathContext};
use crate::scheduler::{PathSnapshot, PathState as SchedulerPathState};
use crate::transport::PathSpec;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const MANAGEMENT_REQUEST_LIMIT: usize = 64 * 1024;
const MANAGEMENT_TREND_CAPACITY: usize = 300;
const MANAGEMENT_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

pub(super) fn spawn_client_management_services(
    config: ManagementConfig,
    context: ClientPathContext,
    services: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) {
    let state = ManagementState::new("client");
    services.spawn(run_client_sampler(context.clone(), state.clone()));
    services.spawn(run_management_listeners(
        config,
        ManagementTarget::Client { context, state },
    ));
}

pub(super) fn spawn_server_management_services(
    config: ManagementConfig,
    context: ServerPathContext,
    services: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) {
    let state = ManagementState::new("server");
    services.spawn(run_server_sampler(context.clone(), state.clone()));
    services.spawn(run_management_listeners(
        config,
        ManagementTarget::Server { context, state },
    ));
}

pub(super) fn spawn_node_management_services(
    config: ManagementConfig,
    clients: Vec<ClientPathContext>,
    servers: Vec<ServerPathContext>,
    services: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) {
    let state = ManagementState::new("node");
    services.spawn(run_node_sampler(
        clients.clone(),
        servers.clone(),
        state.clone(),
    ));
    services.spawn(run_management_listeners(
        config,
        ManagementTarget::Node {
            clients,
            servers,
            state,
        },
    ));
}

async fn run_management_listeners(
    config: ManagementConfig,
    target: ManagementTarget,
) -> Result<(), RuntimeError> {
    let token = Arc::new(config.token);
    let mut listeners = tokio::task::JoinSet::new();
    for listen in config.listen {
        let listener = TcpListener::bind(listen).await?;
        let target = target.clone();
        let token = token.clone();
        listeners.spawn(run_management_listener(listener, target, token));
    }
    let result = if let Some(result) = listeners.join_next().await {
        match result {
            Ok(Ok(())) => Err(RuntimeError::Protocol("management listener exited")),
            Ok(Err(err)) => Err(err),
            Err(err) => Err(RuntimeError::TaskJoin(err)),
        }
    } else {
        Err(RuntimeError::Protocol(
            "management API has no listen addresses",
        ))
    };
    listeners.abort_all();
    while listeners.join_next().await.is_some() {}
    result
}

async fn run_management_listener(
    listener: TcpListener,
    target: ManagementTarget,
    token: Arc<Option<String>>,
) -> Result<(), RuntimeError> {
    let mut requests = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let target = target.clone();
                let token = token.clone();
                requests.spawn(async move {
                    if let Err(err) = handle_management_connection(stream, target, token).await {
                        eprintln!("warning: management API request failed: {err}");
                    }
                });
            }
            Some(result) = requests.join_next(), if !requests.is_empty() => {
                if let Err(err) = result {
                    eprintln!("warning: management API request task failed: {err}");
                }
            }
        }
    }
}

async fn handle_management_connection(
    mut stream: TcpStream,
    target: ManagementTarget,
    token: Arc<Option<String>>,
) -> Result<(), RuntimeError> {
    let request = read_management_request(&mut stream).await?;
    let path = request
        .path
        .split_once('?')
        .map_or(request.path.as_str(), |(path, _)| path);
    if path != "/healthz" && !management_auth_ok(&request, token.as_ref().as_ref()) {
        return write_json_response(
            &mut stream,
            401,
            "Unauthorized",
            json!({"error":"unauthorized"}),
        )
        .await;
    }

    let response = match (request.method.as_str(), path) {
        ("GET", "/healthz") => Ok(json!({"ok":true})),
        ("GET", "/status") => Ok(target.status_json()),
        ("GET", "/paths") => Ok(json!({"paths":target.paths_json()})),
        ("GET", "/traffic") => Ok(target.traffic_json()),
        ("GET", "/diagnostics") => Ok(target.diagnostics_json()),
        ("POST", "/control/path") => target.control_path_json(&request.body),
        _ => Err(ManagementHttpError::new(
            404,
            "Not Found",
            "unknown management endpoint",
        )),
    };

    match response {
        Ok(body) => write_json_response(&mut stream, 200, "OK", body).await,
        Err(err) => {
            write_json_response(
                &mut stream,
                err.status,
                err.reason,
                json!({"error":err.message}),
            )
            .await
        }
    }
}

async fn read_management_request(
    stream: &mut TcpStream,
) -> Result<ManagementRequest, RuntimeError> {
    let mut buffer = Vec::with_capacity(4096);
    let mut scratch = [0u8; 4096];
    let header_end = loop {
        if buffer.len() > MANAGEMENT_REQUEST_LIMIT {
            return Err(RuntimeError::Protocol("management request too large"));
        }
        if let Some(position) = find_header_end(&buffer) {
            break position;
        }
        let read = stream.read(&mut scratch).await?;
        if read == 0 {
            return Err(RuntimeError::Protocol("management request closed early"));
        }
        buffer.extend_from_slice(&scratch[..read]);
    };

    let header_bytes = &buffer[..header_end];
    let header_text = std::str::from_utf8(header_bytes)
        .map_err(|_| RuntimeError::Protocol("management request header is not UTF-8"))?;
    let mut lines = header_text.split("\r\n");
    let request_line = lines
        .next()
        .ok_or(RuntimeError::Protocol("management request line missing"))?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or(RuntimeError::Protocol("management request method missing"))?
        .to_string();
    let path = request_parts
        .next()
        .ok_or(RuntimeError::Protocol("management request path missing"))?
        .to_string();
    let mut headers = Vec::new();
    let mut content_length = 0usize;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim().to_string();
        if name == "content-length" {
            content_length = value
                .parse()
                .map_err(|_| RuntimeError::Protocol("invalid management content-length"))?;
        }
        headers.push((name, value));
    }
    if content_length > MANAGEMENT_REQUEST_LIMIT {
        return Err(RuntimeError::Protocol("management request body too large"));
    }
    let body_start = header_end + 4;
    while buffer.len().saturating_sub(body_start) < content_length {
        let read = stream.read(&mut scratch).await?;
        if read == 0 {
            return Err(RuntimeError::Protocol(
                "management request body closed early",
            ));
        }
        buffer.extend_from_slice(&scratch[..read]);
        if buffer.len() > MANAGEMENT_REQUEST_LIMIT {
            return Err(RuntimeError::Protocol("management request too large"));
        }
    }
    let body = buffer[body_start..body_start + content_length].to_vec();
    Ok(ManagementRequest {
        method,
        path,
        headers,
        body,
    })
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn management_auth_ok(request: &ManagementRequest, token: Option<&String>) -> bool {
    let Some(token) = token else {
        return true;
    };
    request.headers.iter().any(|(name, value)| {
        if name == "authorization" {
            value
                .strip_prefix("Bearer ")
                .is_some_and(|actual| constant_time_eq(actual.as_bytes(), token.as_bytes()))
        } else if name == "x-mptunnel-token" {
            constant_time_eq(value.as_bytes(), token.as_bytes())
        } else {
            false
        }
    })
}

fn constant_time_eq(expected: &[u8], actual: &[u8]) -> bool {
    let max_len = expected.len().max(actual.len());
    let mut diff = expected.len() ^ actual.len();
    for index in 0..max_len {
        let lhs = expected.get(index).copied().unwrap_or(0);
        let rhs = actual.get(index).copied().unwrap_or(0);
        diff |= usize::from(lhs ^ rhs);
    }
    diff == 0
}

async fn write_json_response(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    body: Value,
) -> Result<(), RuntimeError> {
    let body = serde_json::to_vec(&body)
        .map_err(|_| RuntimeError::Protocol("management response serialization failed"))?;
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(&body).await?;
    Ok(())
}

#[derive(Debug)]
struct ManagementRequest {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

#[derive(Debug)]
struct ManagementHttpError {
    status: u16,
    reason: &'static str,
    message: &'static str,
}

impl ManagementHttpError {
    fn new(status: u16, reason: &'static str, message: &'static str) -> Self {
        Self {
            status,
            reason,
            message,
        }
    }
}

#[derive(Clone)]
enum ManagementTarget {
    Client {
        context: ClientPathContext,
        state: ManagementState,
    },
    Server {
        context: ServerPathContext,
        state: ManagementState,
    },
    Node {
        clients: Vec<ClientPathContext>,
        servers: Vec<ServerPathContext>,
        state: ManagementState,
    },
}

impl ManagementTarget {
    fn status_json(&self) -> Value {
        match self {
            Self::Client { context, state } => {
                let (paths, summary) = client_path_statuses(context);
                json!({
                    "schema": "mptunnel.management.v1",
                    "role": state.role,
                    "route_target": route_target_json(context.route_target.as_ref()),
                    "inbounds": client_inbounds_json(context),
                    "uptime_ms": state.uptime_ms(),
                    "started_unix_ms": state.started_unix_ms,
                    "summary": summary,
                    "paths": paths,
                    "traffic_trends": state.trends(),
                    "controls": {
                        "path": {
                            "endpoint": "POST /control/path",
                            "states": ["active", "suspect", "failed", "disabled"]
                        }
                    }
                })
            }
            Self::Server { context, state } => {
                let paths = server_paths(context);
                let registry = context.reliable_streams.management_snapshot();
                let summary = server_summary(&registry);
                json!({
                    "schema": "mptunnel.management.v1",
                    "role": state.role,
                    "tag": context.tag,
                    "route_target": route_target_json(context.route_target.as_ref()),
                    "uptime_ms": state.uptime_ms(),
                    "started_unix_ms": state.started_unix_ms,
                    "summary": summary,
                    "paths": paths,
                    "path_metrics": server_path_metrics(&registry),
                    "traffic_trends": state.trends(),
                    "controls": {
                        "path": {
                            "supported": false,
                            "reason": "server path control is listener-level and not mutable through v1 management API"
                        }
                    }
                })
            }
            Self::Node {
                clients,
                servers,
                state,
            } => {
                let client_snapshots: Vec<_> = clients.iter().map(client_snapshot_json).collect();
                let server_snapshots: Vec<_> = servers.iter().map(server_snapshot_json).collect();
                let summary = node_summary(clients, servers);
                json!({
                    "schema": "mptunnel.management.v1",
                    "role": state.role,
                    "services": {
                        "mpp_outbounds": clients.len(),
                        "mpp_inbounds": servers.len(),
                        "has_local_inbounds": !clients.is_empty(),
                        "has_path_listeners": !servers.is_empty()
                    },
                    "uptime_ms": state.uptime_ms(),
                    "started_unix_ms": state.started_unix_ms,
                    "summary": summary,
                    "clients": client_snapshots,
                    "servers": server_snapshots,
                    "traffic_trends": state.trends(),
                    "controls": {
                        "path": {
                            "supported": !clients.is_empty(),
                            "endpoint": "POST /control/path",
                            "states": ["active", "suspect", "failed", "disabled"]
                        }
                    }
                })
            }
        }
    }

    fn paths_json(&self) -> Value {
        match self {
            Self::Client { context, .. } => json!(client_path_statuses(context).0),
            Self::Server { context, .. } => json!(server_paths(context)),
            Self::Node {
                clients, servers, ..
            } => json!({
                "clients": clients
                    .iter()
                    .map(|context| client_path_statuses(context).0)
                    .collect::<Vec<_>>(),
                "servers": servers.iter().map(server_paths).collect::<Vec<_>>(),
            }),
        }
    }

    fn traffic_json(&self) -> Value {
        match self {
            Self::Client { context, state } => {
                let (_, summary) = client_path_statuses(context);
                json!({"summary":summary,"traffic_trends":state.trends()})
            }
            Self::Server { context, state } => {
                let registry = context.reliable_streams.management_snapshot();
                json!({"summary":server_summary(&registry),"traffic_trends":state.trends()})
            }
            Self::Node {
                clients,
                servers,
                state,
            } => json!({
                "summary": node_summary(clients, servers),
                "clients": clients.iter().map(|context| {
                    let (_, summary) = client_path_statuses(context);
                    summary
                }).collect::<Vec<_>>(),
                "servers": servers.iter().map(|context| {
                    let registry = context.reliable_streams.management_snapshot();
                    server_summary(&registry)
                }).collect::<Vec<_>>(),
                "traffic_trends": state.trends()
            }),
        }
    }

    fn diagnostics_json(&self) -> Value {
        match self {
            Self::Client { context, state } => {
                let (paths, summary) = client_path_statuses(context);
                json!({
                    "role": state.role,
                    "route_target": route_target_json(context.route_target.as_ref()),
                    "inbounds": client_inbounds_json(context),
                    "summary": summary,
                    "paths": paths,
                    "traffic_trends": state.trends(),
                    "notes": [
                        "release diagnostics expose current runtime counters only",
                        "lab-only component timing requires the lab-diagnostics feature"
                    ]
                })
            }
            Self::Server { context, state } => {
                let registry = context.reliable_streams.management_snapshot();
                json!({
                    "role": state.role,
                    "tag": context.tag,
                    "route_target": route_target_json(context.route_target.as_ref()),
                    "summary": server_summary(&registry),
                    "paths": server_paths(context),
                    "path_metrics": server_path_metrics(&registry),
                    "traffic_trends": state.trends(),
                    "notes": [
                        "server response path metrics include source provenance",
                        "lab-only component timing requires the lab-diagnostics feature"
                    ]
                })
            }
            Self::Node {
                clients,
                servers,
                state,
            } => {
                let client_snapshots: Vec<_> = clients.iter().map(client_snapshot_json).collect();
                let server_snapshots: Vec<_> = servers.iter().map(server_snapshot_json).collect();
                json!({
                    "role": state.role,
                    "services": {
                        "mpp_outbounds": clients.len(),
                        "mpp_inbounds": servers.len(),
                        "has_local_inbounds": !clients.is_empty(),
                        "has_path_listeners": !servers.is_empty()
                    },
                    "summary": node_summary(clients, servers),
                    "clients": client_snapshots,
                    "servers": server_snapshots,
                    "traffic_trends": state.trends(),
                    "notes": [
                        "node diagnostics are self-contained across ingress and path-listener services",
                        "release diagnostics expose current runtime counters only",
                        "lab-only component timing requires the lab-diagnostics feature"
                    ]
                })
            }
        }
    }

    fn control_path_json(&self, body: &[u8]) -> Result<Value, ManagementHttpError> {
        let request = serde_json::from_slice::<PathControlRequest>(body).map_err(|_| {
            ManagementHttpError::new(400, "Bad Request", "invalid path control JSON body")
        })?;
        let context = match self {
            Self::Client { context, .. } => context,
            Self::Node { clients, .. } => select_control_client_context(clients, &request)?,
            Self::Server { .. } => {
                return Err(ManagementHttpError::new(
                    409,
                    "Conflict",
                    "path control requires inbound services with upstream mpp outbounds",
                ));
            }
        };
        let underlay = parse_underlay(&request.underlay)?;
        let state = parse_control_state(&request.state)?;
        set_client_path_state(context, underlay, request.index, state)?;
        Ok(json!({
            "applied": true,
            "underlay": underlay_name(underlay),
            "index": request.index,
            "state": request.state
        }))
    }
}

#[derive(Debug, Deserialize)]
struct PathControlRequest {
    #[serde(default)]
    client_index: Option<usize>,
    #[serde(default)]
    client_tag: Option<String>,
    underlay: String,
    index: usize,
    state: String,
}

fn select_control_client_context<'a>(
    clients: &'a [ClientPathContext],
    request: &PathControlRequest,
) -> Result<&'a ClientPathContext, ManagementHttpError> {
    if request.client_index.is_some() && request.client_tag.is_some() {
        return Err(ManagementHttpError::new(
            400,
            "Bad Request",
            "path control must set at most one of client_index or client_tag",
        ));
    }
    if let Some(tag) = request.client_tag.as_deref() {
        return clients
            .iter()
            .find(|context| {
                context
                    .route_target
                    .as_ref()
                    .is_some_and(|target| target.tag == tag)
            })
            .ok_or_else(|| {
                ManagementHttpError::new(
                    404,
                    "Not Found",
                    "client_tag does not match an MPP outbound or balancer",
                )
            });
    }
    clients
        .get(request.client_index.unwrap_or(0))
        .ok_or_else(|| {
            ManagementHttpError::new(
                409,
                "Conflict",
                "path control requires an existing mpp outbound",
            )
        })
}

#[derive(Debug, Clone, Copy)]
enum PathControlState {
    Active,
    Suspect,
    Failed,
    Disabled,
}

fn parse_underlay(value: &str) -> Result<UnderlayProtocol, ManagementHttpError> {
    match value {
        "tcp" => Ok(UnderlayProtocol::Tcp),
        "udp" => Ok(UnderlayProtocol::Udp),
        _ => Err(ManagementHttpError::new(
            400,
            "Bad Request",
            "underlay must be tcp or udp",
        )),
    }
}

fn parse_control_state(value: &str) -> Result<PathControlState, ManagementHttpError> {
    match value {
        "active" => Ok(PathControlState::Active),
        "suspect" => Ok(PathControlState::Suspect),
        "failed" => Ok(PathControlState::Failed),
        "disabled" => Ok(PathControlState::Disabled),
        _ => Err(ManagementHttpError::new(
            400,
            "Bad Request",
            "state must be active, suspect, failed, or disabled",
        )),
    }
}

fn set_client_path_state(
    context: &ClientPathContext,
    underlay: UnderlayProtocol,
    index: usize,
    state: PathControlState,
) -> Result<(), ManagementHttpError> {
    let mut health = context
        .health()
        .lock()
        .expect("client path health management lock");
    let records = match underlay {
        UnderlayProtocol::Tcp => &mut health.tcp,
        UnderlayProtocol::Udp => &mut health.udp,
    };
    let Some(record) = records.get_mut(index) else {
        return Err(ManagementHttpError::new(
            404,
            "Not Found",
            "path index does not exist",
        ));
    };
    match state {
        PathControlState::Active => {
            record.manual_disabled = false;
            record.mark_liveness_success();
        }
        PathControlState::Suspect => {
            record.manual_disabled = false;
            record.invalidate_path_proofs();
            record.state = SchedulerPathState::Suspect;
            record.failed_until = None;
        }
        PathControlState::Failed => {
            record.manual_disabled = false;
            record.invalidate_path_proofs();
            record.state = SchedulerPathState::Failed;
            record.failed_until = Some(Instant::now() + path_record_failure_cooldown(record));
        }
        PathControlState::Disabled => {
            record.manual_disabled = true;
            record.invalidate_path_proofs();
            record.state = SchedulerPathState::Failed;
            record.failed_until = None;
            record.relay_bytes_in_flight = 0;
            record.relay_queue_bytes = 0;
        }
    }
    Ok(())
}

async fn run_client_sampler(
    context: ClientPathContext,
    state: ManagementState,
) -> Result<(), RuntimeError> {
    let mut ticker = tokio::time::interval(MANAGEMENT_SAMPLE_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let (_, summary) = client_path_statuses(&context);
        state.push_sample(summary);
    }
}

async fn run_server_sampler(
    context: ServerPathContext,
    state: ManagementState,
) -> Result<(), RuntimeError> {
    let mut ticker = tokio::time::interval(MANAGEMENT_SAMPLE_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let registry = context.reliable_streams.management_snapshot();
        state.push_sample(server_summary(&registry));
    }
}

async fn run_node_sampler(
    clients: Vec<ClientPathContext>,
    servers: Vec<ServerPathContext>,
    state: ManagementState,
) -> Result<(), RuntimeError> {
    let mut ticker = tokio::time::interval(MANAGEMENT_SAMPLE_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        state.push_sample(node_summary(&clients, &servers));
    }
}

fn client_snapshot_json(context: &ClientPathContext) -> Value {
    let (paths, summary) = client_path_statuses(context);
    json!({
        "route_target": route_target_json(context.route_target.as_ref()),
        "inbounds": client_inbounds_json(context),
        "summary": summary,
        "paths": paths,
    })
}

fn client_inbounds_json(context: &ClientPathContext) -> Vec<Value> {
    context
        .ingresses
        .iter()
        .map(|ingress| match &ingress.config {
            IngressConfig::Socks5 { listen, proxy_auth } => json!({
                "tag": ingress.tag.as_deref(),
                "protocol": "socks5",
                "listen": listen.iter().map(ToString::to_string).collect::<Vec<_>>(),
                "auth_required": proxy_auth.is_required(),
            }),
            IngressConfig::HttpConnect { listen, proxy_auth } => json!({
                "tag": ingress.tag.as_deref(),
                "protocol": "http",
                "listen": listen.iter().map(ToString::to_string).collect::<Vec<_>>(),
                "auth_required": proxy_auth.is_required(),
            }),
            IngressConfig::TunL4(tun) => json!({
                "tag": ingress.tag.as_deref(),
                "protocol": "tun",
                "name": tun.name.as_deref(),
                "ipv4": tun.ipv4.map(|addr| addr.to_string()),
                "ipv4_prefix": tun.ipv4_prefix,
                "ipv4_gateway": tun.ipv4_gateway.map(|addr| addr.to_string()),
                "ipv6": tun.ipv6.map(|addr| addr.to_string()),
                "ipv6_prefix": tun.ipv6_prefix,
                "mtu": tun.mtu,
                "icmp_enabled": tun.enable_icmp,
                "dns_resolvers": tun
                    .dns_resolvers
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
                "dns_ttl_ms": tun.dns_ttl_ms,
            }),
        })
        .collect()
}

fn server_snapshot_json(context: &ServerPathContext) -> Value {
    let registry = context.reliable_streams.management_snapshot();
    json!({
        "tag": context.tag,
        "route_target": route_target_json(context.route_target.as_ref()),
        "summary": server_summary(&registry),
        "paths": server_paths(context),
        "path_metrics": server_path_metrics(&registry),
    })
}

fn route_target_json(target: Option<&RouteTarget>) -> Value {
    match target {
        Some(target) => json!({
            "kind": match target.kind {
                RouteTargetKind::Outbound => "outbound",
                RouteTargetKind::Balancer => "balancer",
            },
            "tag": target.tag,
        }),
        None => Value::Null,
    }
}

fn node_summary(clients: &[ClientPathContext], servers: &[ServerPathContext]) -> ManagementSummary {
    let mut summary = ManagementSummary::default();
    for context in clients {
        let (_, client_summary) = client_path_statuses(context);
        merge_summary(&mut summary, client_summary);
    }
    for context in servers {
        let registry = context.reliable_streams.management_snapshot();
        merge_summary(&mut summary, server_summary(&registry));
    }
    summary
}

fn merge_summary(total: &mut ManagementSummary, next: ManagementSummary) {
    total.path_count = total.path_count.saturating_add(next.path_count);
    total.active_paths = total.active_paths.saturating_add(next.active_paths);
    total.suspect_paths = total.suspect_paths.saturating_add(next.suspect_paths);
    total.failed_paths = total.failed_paths.saturating_add(next.failed_paths);
    total.disabled_paths = total.disabled_paths.saturating_add(next.disabled_paths);
    total.active_flows = total.active_flows.saturating_add(next.active_flows);
    total.active_latency_sensitive_flows = total
        .active_latency_sensitive_flows
        .saturating_add(next.active_latency_sensitive_flows);
    total.queue_bytes = total.queue_bytes.saturating_add(next.queue_bytes);
    total.bytes_in_flight = total.bytes_in_flight.saturating_add(next.bytes_in_flight);
    total.product_bytes_in_flight = total
        .product_bytes_in_flight
        .saturating_add(next.product_bytes_in_flight);
    total.delivery_rate_bps = total
        .delivery_rate_bps
        .saturating_add(next.delivery_rate_bps);
    total.pacing_rate_bps = total.pacing_rate_bps.saturating_add(next.pacing_rate_bps);
}

#[derive(Debug, Clone)]
struct ManagementState {
    role: &'static str,
    started: Instant,
    started_unix_ms: u64,
    trends: Arc<Mutex<VecDeque<ManagementTrendSample>>>,
}

impl ManagementState {
    fn new(role: &'static str) -> Self {
        Self {
            role,
            started: Instant::now(),
            started_unix_ms: unix_millis(),
            trends: Arc::new(Mutex::new(VecDeque::with_capacity(
                MANAGEMENT_TREND_CAPACITY,
            ))),
        }
    }

    fn uptime_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    fn push_sample(&self, summary: ManagementSummary) {
        let mut trends = self.trends.lock().expect("management trend lock");
        if trends.len() >= MANAGEMENT_TREND_CAPACITY {
            trends.pop_front();
        }
        trends.push_back(ManagementTrendSample {
            timestamp_unix_ms: unix_millis(),
            summary,
        });
    }

    fn trends(&self) -> Vec<ManagementTrendSample> {
        self.trends
            .lock()
            .expect("management trend lock")
            .iter()
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone, Default, Serialize)]
struct ManagementSummary {
    path_count: usize,
    active_paths: usize,
    suspect_paths: usize,
    failed_paths: usize,
    disabled_paths: usize,
    active_flows: u64,
    active_latency_sensitive_flows: u64,
    queue_bytes: u64,
    bytes_in_flight: u64,
    product_bytes_in_flight: u64,
    delivery_rate_bps: u64,
    pacing_rate_bps: u64,
}

#[derive(Debug, Clone, Serialize)]
struct ManagementTrendSample {
    timestamp_unix_ms: u64,
    summary: ManagementSummary,
}

#[derive(Debug, Clone, Serialize)]
struct ManagementPathStatus {
    underlay: &'static str,
    index: usize,
    endpoint: String,
    state: &'static str,
    manual_disabled: bool,
    flags: ManagementPathFlags,
    srtt_ms: f64,
    jitter_ms: f64,
    delivery_rate_bps: u64,
    pacing_rate_bps: u64,
    loss_rate: f64,
    queue_bytes: u64,
    bytes_in_flight: u64,
    product_bytes_in_flight: u64,
    inflight_limit_bytes: u64,
    confidence: f64,
    app_limited: bool,
    active_flows: u32,
    active_latency_sensitive_flows: u32,
    delivery_samples: u32,
    carrier_delivery_samples: u32,
    last_delivery_age_ms: Option<u64>,
    carrier_last_delivery_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct ManagementPathFlags {
    backup: bool,
    expensive: bool,
    low_latency: bool,
    bulk_allowed: bool,
    probe_only: bool,
    no_udp: bool,
}

fn client_path_statuses(
    context: &ClientPathContext,
) -> (Vec<ManagementPathStatus>, ManagementSummary) {
    let now = Instant::now();
    let mut summary = ManagementSummary::default();
    let mut health = context.health().lock().expect("client path health lock");
    let mut paths = Vec::with_capacity(context.tcp_paths.len() + context.udp_paths.len());
    paths.extend(client_path_status_set(
        &context.tcp_paths,
        &mut health.tcp,
        UnderlayProtocol::Tcp,
        now,
        &mut summary,
    ));
    paths.extend(client_path_status_set(
        &context.udp_paths,
        &mut health.udp,
        UnderlayProtocol::Udp,
        now,
        &mut summary,
    ));
    (paths, summary)
}

fn client_path_status_set(
    specs: &[PathSpec],
    records: &[ClientPathHealthRecord],
    underlay: UnderlayProtocol,
    now: Instant,
    summary: &mut ManagementSummary,
) -> Vec<ManagementPathStatus> {
    specs
        .iter()
        .enumerate()
        .map(|(index, spec)| {
            let observation = records
                .get(index)
                .map(|record| record.observation_at(now))
                .unwrap_or_default();
            let snapshot = path_snapshot(spec, index, observation);
            apply_summary(summary, snapshot, observation.manual_disabled);
            ManagementPathStatus {
                underlay: underlay_name(underlay),
                index,
                endpoint: path_endpoint(spec),
                state: path_state_name(snapshot.state),
                manual_disabled: observation.manual_disabled,
                flags: ManagementPathFlags {
                    backup: snapshot.flags.backup,
                    expensive: snapshot.flags.expensive,
                    low_latency: snapshot.flags.low_latency,
                    bulk_allowed: snapshot.flags.bulk_allowed,
                    probe_only: snapshot.flags.probe_only,
                    no_udp: snapshot.flags.no_udp,
                },
                srtt_ms: snapshot.srtt_ms,
                jitter_ms: snapshot.jitter_ms,
                delivery_rate_bps: snapshot.delivery_rate_bps.round() as u64,
                pacing_rate_bps: snapshot.pacing_rate_bps.round() as u64,
                loss_rate: snapshot.loss_rate,
                queue_bytes: snapshot.queue_bytes,
                bytes_in_flight: snapshot.bytes_in_flight,
                product_bytes_in_flight: snapshot.product_bytes_in_flight,
                inflight_limit_bytes: snapshot.inflight_limit_bytes,
                confidence: snapshot.confidence,
                app_limited: snapshot.app_limited,
                active_flows: snapshot.active_flows,
                active_latency_sensitive_flows: snapshot.active_latency_sensitive_flows,
                delivery_samples: observation.delivery_samples,
                carrier_delivery_samples: observation.carrier_delivery_samples,
                last_delivery_age_ms: age_ms(now, observation.last_delivery_at),
                carrier_last_delivery_age_ms: age_ms(now, observation.carrier_last_delivery_at),
            }
        })
        .collect()
}

fn apply_summary(summary: &mut ManagementSummary, snapshot: PathSnapshot, manual_disabled: bool) {
    summary.path_count += 1;
    match snapshot.state {
        SchedulerPathState::Active => summary.active_paths += 1,
        SchedulerPathState::Suspect | SchedulerPathState::Draining => summary.suspect_paths += 1,
        SchedulerPathState::Failed => summary.failed_paths += 1,
    }
    if manual_disabled {
        summary.disabled_paths += 1;
    }
    summary.active_flows = summary
        .active_flows
        .saturating_add(snapshot.active_flows as u64);
    summary.active_latency_sensitive_flows = summary
        .active_latency_sensitive_flows
        .saturating_add(snapshot.active_latency_sensitive_flows as u64);
    summary.queue_bytes = summary.queue_bytes.saturating_add(snapshot.queue_bytes);
    summary.bytes_in_flight = summary
        .bytes_in_flight
        .saturating_add(snapshot.bytes_in_flight);
    summary.product_bytes_in_flight = summary
        .product_bytes_in_flight
        .saturating_add(snapshot.product_bytes_in_flight);
    summary.delivery_rate_bps = summary
        .delivery_rate_bps
        .saturating_add(snapshot.delivery_rate_bps.round() as u64);
    summary.pacing_rate_bps = summary
        .pacing_rate_bps
        .saturating_add(snapshot.pacing_rate_bps.round() as u64);
}

#[derive(Debug, Clone, Serialize)]
struct ServerConfiguredPathStatus {
    underlay: &'static str,
    index: usize,
    endpoint: String,
    state: &'static str,
}

fn server_paths(context: &ServerPathContext) -> Vec<ServerConfiguredPathStatus> {
    context
        .server_paths
        .iter()
        .enumerate()
        .map(|(index, spec)| ServerConfiguredPathStatus {
            underlay: underlay_name(spec.underlay),
            index,
            endpoint: path_endpoint(spec),
            state: "listening",
        })
        .collect()
}

#[derive(Debug, Clone, Serialize)]
struct ServerMetricStatus {
    session_id: u64,
    underlay: &'static str,
    path_id: u16,
    source: &'static str,
    direction: &'static str,
    metric_epoch: u64,
    metric_age_us: u64,
    srtt_us: u32,
    rttvar_us: u32,
    jitter_us: u32,
    delivery_rate_bps: u64,
    pacing_rate_bps: u64,
    bytes_in_flight: u64,
    queue_bytes: u64,
    inflight_limit_bytes: u64,
    inflight_hi_bytes: u64,
    loss_ppm: u32,
    ecn_ppm: u32,
    loss_observed: bool,
    ecn_observed: bool,
    confidence_ppm: u32,
    app_limited: bool,
    has_ack_derived_data_sample: bool,
    data_sample_count: u32,
    data_sample_bytes: u64,
}

fn server_path_metrics(registry: &ServerStreamManagementSnapshot) -> Vec<ServerMetricStatus> {
    registry
        .path_metrics
        .iter()
        .map(|metric| ServerMetricStatus {
            session_id: metric.session_id.0,
            underlay: underlay_name(metric.underlay),
            path_id: metric.path_id.0,
            source: metric.source,
            direction: metric_direction_name(metric.metrics.direction),
            metric_epoch: metric.metrics.metric_epoch,
            metric_age_us: u64::from(metric.metrics.metric_age_us),
            srtt_us: metric.metrics.srtt_us,
            rttvar_us: metric.metrics.rttvar_us,
            jitter_us: metric.metrics.jitter_us,
            delivery_rate_bps: metric.metrics.delivery_rate_bps,
            pacing_rate_bps: metric.metrics.pacing_rate_bps,
            bytes_in_flight: metric.metrics.bytes_in_flight,
            queue_bytes: metric.metrics.queue_bytes,
            inflight_limit_bytes: metric.metrics.inflight_limit_bytes,
            inflight_hi_bytes: metric.metrics.inflight_hi_bytes,
            loss_ppm: metric.metrics.loss_ppm,
            ecn_ppm: metric.metrics.ecn_ppm,
            loss_observed: metric.metrics.loss_observed,
            ecn_observed: metric.metrics.ecn_observed,
            confidence_ppm: metric.metrics.confidence_ppm,
            app_limited: metric.metrics.app_limited,
            has_ack_derived_data_sample: metric.metrics.has_ack_derived_data_sample,
            data_sample_count: metric.metrics.data_sample_count,
            data_sample_bytes: metric.metrics.data_sample_bytes,
        })
        .collect()
}

fn server_summary(registry: &ServerStreamManagementSnapshot) -> ManagementSummary {
    let mut summary = ManagementSummary {
        active_flows: registry.active_streams as u64,
        path_count: registry.path_metrics.len(),
        ..ManagementSummary::default()
    };
    for metric in &registry.path_metrics {
        summary.delivery_rate_bps = summary
            .delivery_rate_bps
            .saturating_add(metric.metrics.delivery_rate_bps);
        summary.pacing_rate_bps = summary
            .pacing_rate_bps
            .saturating_add(metric.metrics.pacing_rate_bps);
        summary.bytes_in_flight = summary
            .bytes_in_flight
            .saturating_add(metric.metrics.bytes_in_flight);
        summary.queue_bytes = summary
            .queue_bytes
            .saturating_add(metric.metrics.queue_bytes);
    }
    summary
}

fn path_endpoint(spec: &PathSpec) -> String {
    format!(
        "{}://{}",
        underlay_name(spec.underlay),
        spec.endpoint.authority()
    )
}

fn underlay_name(underlay: UnderlayProtocol) -> &'static str {
    match underlay {
        UnderlayProtocol::Tcp => "tcp",
        UnderlayProtocol::Udp => "udp",
    }
}

fn path_state_name(state: SchedulerPathState) -> &'static str {
    match state {
        SchedulerPathState::Active => "active",
        SchedulerPathState::Suspect => "suspect",
        SchedulerPathState::Draining => "draining",
        SchedulerPathState::Failed => "failed",
    }
}

fn metric_direction_name(direction: PathMetricDirection) -> &'static str {
    match direction {
        PathMetricDirection::ClientToServer => "client_to_server",
        PathMetricDirection::ServerToClient => "server_to_client",
    }
}

fn age_ms(now: Instant, instant: Option<Instant>) -> Option<u64> {
    instant.map(|instant| now.saturating_duration_since(instant).as_millis() as u64)
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

#[cfg(test)]
#[path = "management_test.rs"]
mod tests;
