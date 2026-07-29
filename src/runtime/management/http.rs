//! Bounded HTTP/1 management transport and embedded dashboard assets.
//!
//! HTTP parsing, authentication, and browser policy stay here so runtime
//! snapshots and path controls do not acquire web-server responsibilities.

use super::ManagementTarget;
use super::schema::{
    ManagementControls, ManagementDiagnostics, ManagementFlowStatus, ManagementPathStatus,
    ManagementSessionStatus, ManagementSummary, ManagementTraffic, SCHEMA,
};
use crate::config::ConfigRevision;
use crate::config::ManagementConfig;
use crate::runtime::error::RuntimeError;
use crate::runtime::readiness::{RequiredServiceReadiness, RuntimeGenerationPhase};
use serde::Serialize;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

const HEADER_LIMIT: usize = 32 * 1024;
const BODY_LIMIT: usize = 4 * 1024 * 1024;
const REQUEST_LIMIT: usize = HEADER_LIMIT + BODY_LIMIT;
const HEADER_COUNT_LIMIT: usize = 64;
const CONNECTION_LIMIT: usize = 64;
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
const CONTENT_SECURITY_POLICY: &str = "default-src 'self'; connect-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";

const DASHBOARD_HTML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/assets/dashboard/index.html"
));
const DASHBOARD_CSS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/assets/dashboard/dashboard.css"
));
const DASHBOARD_JS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/assets/dashboard/dashboard.js"
));

struct HttpSettings {
    token: Option<String>,
    dashboard: bool,
}

pub(super) async fn spawn_listeners(
    config: ManagementConfig,
    target: ManagementTarget,
    readiness: RequiredServiceReadiness,
    services: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) -> Result<(), RuntimeError> {
    let settings = Arc::new(HttpSettings {
        token: config.token,
        dashboard: config.dashboard,
    });
    let capacity = Arc::new(Semaphore::new(CONNECTION_LIMIT));
    let mut bound = Vec::with_capacity(config.listen.len());
    for listen in config.listen {
        bound.push(TcpListener::bind(listen).await?);
    }
    if bound.is_empty() {
        return Err(RuntimeError::Protocol(
            "management API has no listen addresses",
        ));
    }
    for listener in bound {
        services.spawn(run_listener(
            listener,
            target.clone(),
            settings.clone(),
            capacity.clone(),
        ));
    }
    readiness.ready();
    Ok(())
}

async fn run_listener(
    listener: TcpListener,
    target: ManagementTarget,
    settings: Arc<HttpSettings>,
    capacity: Arc<Semaphore>,
) -> Result<(), RuntimeError> {
    let mut requests = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (mut stream, _) = accepted?;
                let Ok(permit) = capacity.clone().try_acquire_owned() else {
                    let _ = stream.try_write(
                        b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    );
                    continue;
                };
                let target = target.clone();
                let settings = settings.clone();
                requests.spawn(async move {
                    let _permit = permit;
                    match tokio::time::timeout(
                        CONNECTION_TIMEOUT,
                        handle_connection(&mut stream, target, settings),
                    )
                    .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            crate::observability::process_event!(
                                Warn,
                                "management",
                                "request_failed",
                                "management API request failed: {err}"
                            );
                        }
                        Err(_) => {
                            let _ = write_error(
                                &mut stream,
                                ManagementHttpError::new(
                                    408,
                                    "Request Timeout",
                                    "management request timed out",
                                ),
                            )
                            .await;
                        }
                    }
                });
            }
            Some(result) = requests.join_next(), if !requests.is_empty() => {
                if let Err(err) = result {
                    crate::observability::process_event!(
                        Warn,
                        "management",
                        "request_task_failed",
                        "management API request task failed: {err}"
                    );
                }
            }
        }
    }
}

async fn handle_connection(
    stream: &mut TcpStream,
    target: ManagementTarget,
    settings: Arc<HttpSettings>,
) -> Result<(), RuntimeError> {
    let request = match read_request(stream).await {
        Ok(request) => request,
        Err(err) => {
            write_error(stream, err).await?;
            return Ok(());
        }
    };
    let path = request.path_without_query();

    let public = public_path(path, settings.dashboard);
    if !public && !management_auth_ok(&request, settings.token.as_deref()) {
        write_error(
            stream,
            ManagementHttpError::new(401, "Unauthorized", "unauthorized"),
        )
        .await?;
        return Ok(());
    }

    match (request.method.as_str(), path) {
        ("GET", "/") if settings.dashboard => {
            write_static(stream, "text/html; charset=utf-8", DASHBOARD_HTML).await
        }
        ("GET", "/dashboard.css") if settings.dashboard => {
            write_static(stream, "text/css; charset=utf-8", DASHBOARD_CSS).await
        }
        ("GET", "/dashboard.js") if settings.dashboard => {
            write_static(stream, "text/javascript; charset=utf-8", DASHBOARD_JS).await
        }
        ("GET", "/api/v2/") => {
            write_json(
                stream,
                200,
                "OK",
                &json!({
                    "schema": SCHEMA,
                    "operations": {
                        "health": "GET /api/v2/health",
                        "liveness": "GET /api/v2/health/live",
                        "readiness": "GET /api/v2/health/ready",
                        "status": "GET /api/v2/status",
                        "paths": "GET /api/v2/paths",
                        "traffic": "GET /api/v2/traffic",
                        "sessions": "GET /api/v2/sessions",
                        "flows": "GET /api/v2/flows",
                        "diagnostics": "GET /api/v2/diagnostics",
                        "path_control": "POST /api/v2/actions/path",
                        "peer_diagnostics": "POST /api/v2/diagnostics/peer",
                        "config": "GET /api/v2/config",
                        "config_validate": "POST /api/v2/config/validate",
                        "config_apply": "POST /api/v2/config/apply",
                        "balancers": "GET /api/v2/balancers",
                        "balancer_actions": "POST /api/v2/balancers/actions",
                        "dns_status": "GET /api/v2/dns/status",
                        "dns_explain": "GET /api/v2/dns/explain?domain=<domain>",
                        "dns_query": "POST /api/v2/dns/query",
                        "dns_cache_flush": "POST /api/v2/dns/cache/flush"
                    },
                    "dashboard": settings.dashboard,
                    "authentication": if settings.token.is_some() { "bearer" } else { "none" }
                }),
            )
            .await
        }
        ("GET", "/api/v2/health") => {
            let (status, reason, response) = health_response(&target);
            write_json(stream, status, reason, &response).await
        }
        ("GET", "/api/v2/health/live") => {
            let (status, reason, response) = liveness_response(&target);
            write_json(stream, status, reason, &response).await
        }
        ("GET", "/api/v2/health/ready") => {
            let (status, reason, response) = readiness_response(&target);
            write_json(stream, status, reason, &response).await
        }
        ("GET", "/api/v2/config") => match target.config_status_json() {
            Ok(value) => write_json(stream, 200, "OK", &value).await,
            Err(err) => write_error(stream, err).await,
        },
        ("GET", "/api/v2/balancers") => match target.balancer_status_json() {
            Ok(value) => write_json(stream, 200, "OK", &value).await,
            Err(err) => write_error(stream, err).await,
        },
        ("POST", "/api/v2/balancers/actions") => {
            if let Err(err) = require_json(&request) {
                return write_error(stream, err).await;
            }
            match target.control_balancer_json(&request.body) {
                Ok(value) => write_json(stream, 200, "OK", &value).await,
                Err(err) => write_error(stream, err).await,
            }
        }
        ("GET", "/api/v2/dns/status") => match target.dns_status_json() {
            Ok(value) => write_json(stream, 200, "OK", &value).await,
            Err(err) => write_error(stream, err).await,
        },
        ("GET", "/api/v2/dns/explain") => {
            let domain = match required_single_query_parameter(&request, "domain") {
                Ok(domain) => domain,
                Err(err) => return write_error(stream, err).await,
            };
            match target.dns_explain_json(&domain) {
                Ok(value) => write_json(stream, 200, "OK", &value).await,
                Err(err) => write_error(stream, err).await,
            }
        }
        ("POST", "/api/v2/dns/query") => {
            if let Err(err) = require_json(&request) {
                return write_error(stream, err).await;
            }
            match target.dns_query_json(&request.body).await {
                Ok(value) => write_json(stream, 200, "OK", &value).await,
                Err(err) => write_error(stream, err).await,
            }
        }
        ("POST", "/api/v2/dns/cache/flush") => {
            if let Err(err) = require_json(&request) {
                return write_error(stream, err).await;
            }
            match target.dns_flush_json(&request.body) {
                Ok(value) => write_json(stream, 200, "OK", &value).await,
                Err(err) => write_error(stream, err).await,
            }
        }
        ("POST", "/api/v2/config/validate") => {
            if let Err(err) = require_toml(&request) {
                return write_error(stream, err).await;
            }
            match target.validate_config_document(&request.body) {
                Ok(value) => write_json(stream, 200, "OK", &value).await,
                Err(err) => write_error(stream, err).await,
            }
        }
        ("POST", "/api/v2/config/apply") => {
            if let Err(err) = require_toml(&request) {
                return write_error(stream, err).await;
            }
            let expected = match required_config_revision(&request) {
                Ok(expected) => expected,
                Err(err) => return write_error(stream, err).await,
            };
            match target.apply_config_document(expected, &request.body) {
                Ok(outcome) => {
                    let status = if outcome.pending_activation { 202 } else { 200 };
                    let reason = if outcome.pending_activation {
                        "Accepted"
                    } else {
                        "OK"
                    };
                    let written = write_json(stream, status, reason, &outcome.response).await;
                    // Persistence, not delivery of the acknowledgement, commits
                    // the desired state. A client that disconnects after the
                    // store transaction must not strand a pending generation.
                    if outcome.request_reload {
                        target.request_config_reload();
                    }
                    written
                }
                Err(err) => write_error(stream, err).await,
            }
        }
        ("GET", "/api/v2/status") => {
            let snapshot = target.snapshot();
            write_json(stream, 200, "OK", &*snapshot).await
        }
        ("GET", "/api/v2/paths") => {
            let snapshot = target.snapshot();
            write_json(
                stream,
                200,
                "OK",
                &PathsResponse {
                    schema: snapshot.schema,
                    generated_unix_ms: snapshot.generated_unix_ms,
                    paths: &snapshot.paths,
                },
            )
            .await
        }
        ("GET", "/api/v2/traffic") => {
            let snapshot = target.snapshot();
            write_json(
                stream,
                200,
                "OK",
                &TrafficResponse {
                    schema: snapshot.schema,
                    generated_unix_ms: snapshot.generated_unix_ms,
                    summary: &snapshot.summary,
                    traffic: &snapshot.traffic,
                },
            )
            .await
        }
        ("GET", "/api/v2/sessions") => {
            let snapshot = target.snapshot();
            write_json(
                stream,
                200,
                "OK",
                &SessionsResponse {
                    schema: snapshot.schema,
                    generated_unix_ms: snapshot.generated_unix_ms,
                    sessions: &snapshot.sessions,
                },
            )
            .await
        }
        ("GET", "/api/v2/flows") => {
            let snapshot = target.snapshot();
            write_json(
                stream,
                200,
                "OK",
                &FlowsResponse {
                    schema: snapshot.schema,
                    generated_unix_ms: snapshot.generated_unix_ms,
                    flows: &snapshot.flows,
                },
            )
            .await
        }
        ("GET", "/api/v2/diagnostics") => {
            let snapshot = target.snapshot();
            write_json(
                stream,
                200,
                "OK",
                &DiagnosticsResponse {
                    schema: snapshot.schema,
                    generated_unix_ms: snapshot.generated_unix_ms,
                    diagnostics: &snapshot.diagnostics,
                    controls: &snapshot.controls,
                    paths: &snapshot.paths,
                },
            )
            .await
        }
        ("POST", "/api/v2/actions/path") => {
            if let Err(err) = require_json(&request) {
                return write_error(stream, err).await;
            }
            match target.control_path_json(&request.body) {
                Ok(value) => write_json(stream, 200, "OK", &value).await,
                Err(err) => write_error(stream, err).await,
            }
        }
        ("POST", "/api/v2/diagnostics/peer") => {
            if let Err(err) = require_json(&request) {
                return write_error(stream, err).await;
            }
            match target.peer_diagnostics_json(&request.body).await {
                Ok(value) => write_json(stream, 200, "OK", &value).await,
                Err(err) => write_error(stream, err).await,
            }
        }
        ("OPTIONS", _) => {
            write_error(
                stream,
                ManagementHttpError::new(405, "Method Not Allowed", "CORS is not enabled"),
            )
            .await
        }
        (_, path) if known_path(path, settings.dashboard) => {
            write_error(
                stream,
                ManagementHttpError::new(405, "Method Not Allowed", "method not allowed"),
            )
            .await
        }
        _ => {
            write_error(
                stream,
                ManagementHttpError::new(404, "Not Found", "unknown management endpoint"),
            )
            .await
        }
    }
}

pub(super) fn health_response(target: &ManagementTarget) -> (u16, &'static str, serde_json::Value) {
    let health = health_assessment(target);
    if health.live {
        (200, "OK", health.value)
    } else {
        (503, "Service Unavailable", health.value)
    }
}

fn liveness_response(target: &ManagementTarget) -> (u16, &'static str, serde_json::Value) {
    health_response(target)
}

fn readiness_response(target: &ManagementTarget) -> (u16, &'static str, serde_json::Value) {
    let health = health_assessment(target);
    if health.ready {
        (200, "OK", health.value)
    } else {
        (503, "Service Unavailable", health.value)
    }
}

struct HealthAssessment {
    live: bool,
    ready: bool,
    value: serde_json::Value,
}

fn health_assessment(target: &ManagementTarget) -> HealthAssessment {
    target.refresh_current_snapshot();
    let generation = target
        .config_control()
        .map(|control| control.generation_status())
        .unwrap_or_else(|| target.generation().status());
    let snapshot = target.snapshot();
    let live = generation.phase != RuntimeGenerationPhase::Failed;
    let generation_ready = generation.phase == RuntimeGenerationPhase::Ready;
    let mut readiness_blockers = Vec::new();
    let mut degraded_reasons = Vec::new();
    if !generation_ready {
        readiness_blockers.push(format!("generation-{}", generation.phase.as_str()));
    }

    let connected_outbound_sessions = snapshot
        .sessions
        .iter()
        .filter(|session| {
            session.service == "mpp_outbound"
                && session.state == "connected"
                && session.carrier_count > 0
        })
        .count();
    if snapshot.services.mpp_outbounds > 0 {
        if connected_outbound_sessions == 0 {
            readiness_blockers.push("no-connected-mpp-outbound".to_string());
        } else if connected_outbound_sessions < snapshot.services.mpp_outbounds {
            degraded_reasons.push("some-mpp-outbounds-disconnected".to_string());
        }
    }

    let unavailable_balancers = snapshot
        .balancers
        .iter()
        .filter(|balancer| balancer.ready_members == 0)
        .map(|balancer| balancer.name.clone())
        .collect::<Vec<_>>();
    if !unavailable_balancers.is_empty() {
        readiness_blockers.push("balancer-without-ready-outbound".to_string());
    }
    if snapshot
        .balancers
        .iter()
        .any(|balancer| balancer.unavailable_members > 0 && balancer.ready_members > 0)
    {
        degraded_reasons.push("some-balancer-outbounds-unavailable".to_string());
    }

    let dns_snapshot = target.dns.as_ref().map(|dns| dns.runtime_snapshot());
    let failed_dns_plans = dns_snapshot
        .as_ref()
        .map(|dns| {
            dns.plans
                .iter()
                .filter(|plan| {
                    plan.queries > 0
                        && !plan.upstreams.is_empty()
                        && plan
                            .upstreams
                            .iter()
                            .all(|upstream| upstream.attempts > 0 && upstream.successes == 0)
                })
                .map(|plan| plan.plan.to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if !failed_dns_plans.is_empty() {
        degraded_reasons.push("dns-plan-without-successful-upstream".to_string());
    }

    if let Some(control) = target.config_control()
        && (control.store().revision() != control.store().active_revision()
            || control.runtime_revision() != control.store().active_revision())
    {
        degraded_reasons.push("configuration-activation-pending".to_string());
    }

    if generation_ready
        && snapshot.summary.configured_path_count > 0
        && snapshot.summary.suspect_paths + snapshot.summary.failed_paths > 0
    {
        degraded_reasons.push("some-carrier-paths-unhealthy".to_string());
    }
    let ready = generation_ready && readiness_blockers.is_empty();
    let degraded = generation_ready && (!ready || !degraded_reasons.is_empty());
    let status = match generation.phase {
        RuntimeGenerationPhase::Starting => "starting",
        RuntimeGenerationPhase::Stopping => "stopping",
        RuntimeGenerationPhase::Failed => "failed",
        RuntimeGenerationPhase::Ready if degraded => "degraded",
        RuntimeGenerationPhase::Ready => "healthy",
    };
    let mut response = json!({
        "schema": "mptunnel.health.v2",
        "status": status,
        "live": live,
        "ready": ready,
        "degraded": degraded,
        "phase": generation.phase.as_str(),
        "readiness_blockers": readiness_blockers,
        "degraded_reasons": degraded_reasons,
        "listeners": {
            "management": "accepting",
            "local_inbounds": snapshot.local_inbounds.len(),
            "mpp_path_listeners": snapshot.services.configured_path_listeners,
        },
        "sessions": {
            "mpp_outbounds": snapshot.services.mpp_outbounds,
            "connected_mpp_outbounds": connected_outbound_sessions,
            "authenticated": snapshot.sessions.iter()
                .filter(|session| session.carrier_count > 0)
                .count(),
        },
        "balancers": {
            "configured": snapshot.balancers.len(),
            "unavailable": unavailable_balancers,
        },
        "dns": dns_snapshot.as_ref().map(|dns| json!({
            "generation": dns.generation,
            "plans": dns.plans.len(),
            "failed_plans": failed_dns_plans,
        })),
    });
    let object = response
        .as_object_mut()
        .expect("health response starts as a JSON object");
    if let Some(failure) = generation.failure {
        object.insert(
            "failure".to_string(),
            serde_json::Value::String(failure.to_string()),
        );
    }
    if let Some(control) = target.config_control() {
        object.insert(
            "desired_revision".to_string(),
            serde_json::Value::String(control.store().revision().to_string()),
        );
        object.insert(
            "active_revision".to_string(),
            serde_json::Value::String(control.store().active_revision().to_string()),
        );
        object.insert(
            "runtime_revision".to_string(),
            serde_json::Value::String(control.runtime_revision().to_string()),
        );
    }
    HealthAssessment {
        live,
        ready,
        value: response,
    }
}

#[derive(Serialize)]
struct PathsResponse<'a> {
    schema: &'static str,
    generated_unix_ms: u64,
    paths: &'a [ManagementPathStatus],
}

#[derive(Serialize)]
struct TrafficResponse<'a> {
    schema: &'static str,
    generated_unix_ms: u64,
    summary: &'a ManagementSummary,
    traffic: &'a ManagementTraffic,
}

#[derive(Serialize)]
struct SessionsResponse<'a> {
    schema: &'static str,
    generated_unix_ms: u64,
    sessions: &'a [ManagementSessionStatus],
}

#[derive(Serialize)]
struct FlowsResponse<'a> {
    schema: &'static str,
    generated_unix_ms: u64,
    flows: &'a [ManagementFlowStatus],
}

#[derive(Serialize)]
struct DiagnosticsResponse<'a> {
    schema: &'static str,
    generated_unix_ms: u64,
    diagnostics: &'a ManagementDiagnostics,
    controls: &'a ManagementControls,
    paths: &'a [ManagementPathStatus],
}

fn known_path(path: &str, dashboard: bool) -> bool {
    matches!(
        path,
        "/api/v2/"
            | "/api/v2/health"
            | "/api/v2/health/live"
            | "/api/v2/health/ready"
            | "/api/v2/status"
            | "/api/v2/paths"
            | "/api/v2/traffic"
            | "/api/v2/sessions"
            | "/api/v2/flows"
            | "/api/v2/diagnostics"
            | "/api/v2/config"
            | "/api/v2/config/validate"
            | "/api/v2/config/apply"
            | "/api/v2/balancers"
            | "/api/v2/balancers/actions"
            | "/api/v2/dns/status"
            | "/api/v2/dns/explain"
            | "/api/v2/dns/query"
            | "/api/v2/dns/cache/flush"
            | "/api/v2/actions/path"
            | "/api/v2/diagnostics/peer"
    ) || dashboard && matches!(path, "/" | "/dashboard.css" | "/dashboard.js")
}

fn public_path(path: &str, dashboard: bool) -> bool {
    dashboard && matches!(path, "/" | "/dashboard.css" | "/dashboard.js")
}

fn require_json(request: &ManagementRequest) -> Result<(), ManagementHttpError> {
    let is_json = request.header("content-type").is_some_and(|value| {
        value
            .split(';')
            .next()
            .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
    });
    if is_json {
        Ok(())
    } else {
        Err(ManagementHttpError::new(
            415,
            "Unsupported Media Type",
            "management POST content-type must be application/json",
        ))
    }
}

fn require_toml(request: &ManagementRequest) -> Result<(), ManagementHttpError> {
    let is_toml = request.header("content-type").is_some_and(|value| {
        value
            .split(';')
            .next()
            .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/toml"))
    });
    if is_toml {
        Ok(())
    } else {
        Err(ManagementHttpError::new(
            415,
            "Unsupported Media Type",
            "configuration body content-type must be application/toml",
        ))
    }
}

fn required_single_query_parameter(
    request: &ManagementRequest,
    expected: &str,
) -> Result<String, ManagementHttpError> {
    let query = request
        .path
        .split_once('?')
        .map(|(_, query)| query)
        .ok_or_else(|| {
            ManagementHttpError::new(
                400,
                "Bad Request",
                format!("missing {expected} query parameter"),
            )
        })?;
    let mut found = None;
    for pair in query.split('&') {
        let (name, value) = pair.split_once('=').ok_or_else(|| {
            ManagementHttpError::new(400, "Bad Request", "malformed management query parameter")
        })?;
        let name = percent_encoding::percent_decode_str(name)
            .decode_utf8()
            .map_err(|_| {
                ManagementHttpError::new(400, "Bad Request", "query parameter is not UTF-8")
            })?;
        if name != expected {
            return Err(ManagementHttpError::new(
                400,
                "Bad Request",
                format!("unexpected query parameter {name}"),
            ));
        }
        if found.is_some() {
            return Err(ManagementHttpError::new(
                400,
                "Bad Request",
                format!("duplicate {expected} query parameter"),
            ));
        }
        let value = percent_encoding::percent_decode_str(value)
            .decode_utf8()
            .map_err(|_| {
                ManagementHttpError::new(400, "Bad Request", "query parameter is not UTF-8")
            })?;
        if value.is_empty() {
            return Err(ManagementHttpError::new(
                400,
                "Bad Request",
                format!("{expected} query parameter must not be empty"),
            ));
        }
        found = Some(value.into_owned());
    }
    found.ok_or_else(|| {
        ManagementHttpError::new(
            400,
            "Bad Request",
            format!("missing {expected} query parameter"),
        )
    })
}

fn required_config_revision(
    request: &ManagementRequest,
) -> Result<ConfigRevision, ManagementHttpError> {
    let value = request.header("if-match").ok_or_else(|| {
        ManagementHttpError::new(
            428,
            "Precondition Required",
            "configuration apply requires If-Match",
        )
    })?;
    let value = value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(value);
    value.parse().map_err(|_| {
        ManagementHttpError::new(
            400,
            "Bad Request",
            "If-Match must contain one configuration revision",
        )
    })
}

async fn read_request(stream: &mut TcpStream) -> Result<ManagementRequest, ManagementHttpError> {
    let mut buffer = Vec::with_capacity(4096);
    let mut scratch = [0u8; 4096];
    let header_len = loop {
        if buffer.len() > HEADER_LIMIT {
            return Err(ManagementHttpError::new(
                431,
                "Request Header Fields Too Large",
                "management request headers too large",
            ));
        }
        let mut headers = [httparse::EMPTY_HEADER; HEADER_COUNT_LIMIT];
        let mut parsed = httparse::Request::new(&mut headers);
        match parsed.parse(&buffer) {
            Ok(httparse::Status::Complete(header_len)) => break header_len,
            Ok(httparse::Status::Partial) => {}
            Err(_) => {
                return Err(ManagementHttpError::new(
                    400,
                    "Bad Request",
                    "malformed management request",
                ));
            }
        }
        let read = stream.read(&mut scratch).await.map_err(|_| {
            ManagementHttpError::new(400, "Bad Request", "management request read failed")
        })?;
        if read == 0 {
            return Err(ManagementHttpError::new(
                400,
                "Bad Request",
                "management request closed before headers completed",
            ));
        }
        buffer.extend_from_slice(&scratch[..read]);
    };

    let mut parsed_headers = [httparse::EMPTY_HEADER; HEADER_COUNT_LIMIT];
    let mut parsed = httparse::Request::new(&mut parsed_headers);
    match parsed.parse(&buffer[..header_len]) {
        Ok(httparse::Status::Complete(_)) => {}
        _ => {
            return Err(ManagementHttpError::new(
                400,
                "Bad Request",
                "malformed management request",
            ));
        }
    }
    let method = parsed
        .method
        .ok_or_else(|| {
            ManagementHttpError::new(400, "Bad Request", "management request method missing")
        })?
        .to_string();
    let path = parsed
        .path
        .ok_or_else(|| {
            ManagementHttpError::new(400, "Bad Request", "management request path missing")
        })?
        .to_string();
    if !path.starts_with('/') || path.contains('#') {
        return Err(ManagementHttpError::new(
            400,
            "Bad Request",
            "management request target must be origin-form",
        ));
    }
    if parsed.version.is_none() {
        return Err(ManagementHttpError::new(
            400,
            "Bad Request",
            "management HTTP version missing",
        ));
    }

    let mut headers = Vec::with_capacity(parsed.headers.len());
    let mut content_length = None;
    let mut authorization_seen = false;
    let mut content_type_seen = false;
    let mut if_match_seen = false;
    for header in parsed.headers.iter() {
        let name = header.name.to_ascii_lowercase();
        let value = std::str::from_utf8(header.value)
            .map_err(|_| {
                ManagementHttpError::new(
                    400,
                    "Bad Request",
                    "management request header is not UTF-8",
                )
            })?
            .trim()
            .to_string();
        if name == "transfer-encoding" {
            return Err(ManagementHttpError::new(
                400,
                "Bad Request",
                "transfer-encoding is not supported",
            ));
        }
        if name == "content-length" {
            if content_length.is_some() {
                return Err(ManagementHttpError::new(
                    400,
                    "Bad Request",
                    "duplicate content-length is not allowed",
                ));
            }
            content_length = Some(value.parse::<usize>().map_err(|_| {
                ManagementHttpError::new(400, "Bad Request", "invalid content-length")
            })?);
        }
        if name == "authorization" {
            if authorization_seen {
                return Err(ManagementHttpError::new(
                    400,
                    "Bad Request",
                    "duplicate authorization is not allowed",
                ));
            }
            authorization_seen = true;
        }
        if name == "content-type" {
            if content_type_seen {
                return Err(ManagementHttpError::new(
                    400,
                    "Bad Request",
                    "duplicate content-type is not allowed",
                ));
            }
            content_type_seen = true;
        }
        if name == "if-match" {
            if if_match_seen {
                return Err(ManagementHttpError::new(
                    400,
                    "Bad Request",
                    "duplicate if-match is not allowed",
                ));
            }
            if_match_seen = true;
        }
        headers.push((name, value));
    }
    let content_length = content_length.unwrap_or(0);
    if content_length > BODY_LIMIT || header_len.saturating_add(content_length) > REQUEST_LIMIT {
        return Err(ManagementHttpError::new(
            413,
            "Payload Too Large",
            "management request too large",
        ));
    }
    while buffer.len() < header_len + content_length {
        let read = stream.read(&mut scratch).await.map_err(|_| {
            ManagementHttpError::new(400, "Bad Request", "management request body read failed")
        })?;
        if read == 0 {
            return Err(ManagementHttpError::new(
                400,
                "Bad Request",
                "management request body closed early",
            ));
        }
        buffer.extend_from_slice(&scratch[..read]);
        if buffer.len() > REQUEST_LIMIT {
            return Err(ManagementHttpError::new(
                413,
                "Payload Too Large",
                "management request too large",
            ));
        }
    }
    if buffer.len() != header_len + content_length {
        return Err(ManagementHttpError::new(
            400,
            "Bad Request",
            "pipelined management requests are not supported",
        ));
    }
    Ok(ManagementRequest {
        method,
        path,
        headers,
        body: buffer[header_len..].to_vec(),
    })
}

pub(super) fn management_auth_ok(request: &ManagementRequest, token: Option<&str>) -> bool {
    let Some(token) = token else {
        return false;
    };
    request.headers.iter().any(|(name, value)| {
        if name == "authorization" {
            value.split_once(' ').is_some_and(|(scheme, actual)| {
                scheme.eq_ignore_ascii_case("bearer")
                    && !actual.is_empty()
                    && !actual.contains(' ')
                    && constant_time_eq(actual.as_bytes(), token.as_bytes())
            })
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

async fn write_json<T: Serialize>(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    body: &T,
) -> Result<(), RuntimeError> {
    let body = serde_json::to_vec(body)
        .map_err(|_| RuntimeError::Protocol("management response serialization failed"))?;
    write_response(stream, status, reason, "application/json", &body, true).await
}

async fn write_error(
    stream: &mut TcpStream,
    error: ManagementHttpError,
) -> Result<(), RuntimeError> {
    write_json(
        stream,
        error.status,
        error.reason,
        &json!({"error": error.message}),
    )
    .await
}

async fn write_static(
    stream: &mut TcpStream,
    content_type: &str,
    body: &str,
) -> Result<(), RuntimeError> {
    write_response(stream, 200, "OK", content_type, body.as_bytes(), false).await
}

async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    content_type: &str,
    body: &[u8],
    no_store: bool,
) -> Result<(), RuntimeError> {
    let cache_control = if no_store { "no-store" } else { "no-cache" };
    let mut header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: {cache_control}\r\nX-Content-Type-Options: nosniff\r\nX-Frame-Options: DENY\r\nReferrer-Policy: no-referrer\r\nPermissions-Policy: camera=(), microphone=(), geolocation=()\r\nContent-Security-Policy: {CONTENT_SECURITY_POLICY}\r\n",
        body.len(),
    );
    header.push_str("\r\n");
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body).await?;
    Ok(())
}

pub(super) struct ManagementRequest {
    pub(super) method: String,
    pub(super) path: String,
    pub(super) headers: Vec<(String, String)>,
    pub(super) body: Vec<u8>,
}

impl std::fmt::Debug for ManagementRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ManagementRequest")
            .field("method", &self.method)
            .field("path", &self.path)
            .field("header_count", &self.headers.len())
            .field("body_bytes", &self.body.len())
            .finish()
    }
}

impl ManagementRequest {
    fn path_without_query(&self) -> &str {
        self.path
            .split_once('?')
            .map_or(self.path.as_str(), |(path, _)| path)
    }

    fn header(&self, expected: &str) -> Option<&str> {
        self.headers
            .iter()
            .find_map(|(name, value)| (name == expected).then_some(value.as_str()))
    }
}

#[derive(Debug, Clone)]
pub(super) struct ManagementHttpError {
    pub(super) status: u16,
    pub(super) reason: &'static str,
    pub(super) message: String,
}

impl ManagementHttpError {
    pub(super) fn new(status: u16, reason: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            reason,
            message: message.into(),
        }
    }
}

#[cfg(test)]
#[path = "http_test.rs"]
mod tests;
