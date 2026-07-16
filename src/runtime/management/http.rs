//! Bounded HTTP/1 management transport and embedded dashboard assets.
//!
//! HTTP parsing, authentication, and browser policy stay here so runtime
//! snapshots and path controls do not acquire web-server responsibilities.

use super::ManagementTarget;
use super::schema::{
    ManagementControls, ManagementDiagnostics, ManagementFlowStatus, ManagementPathStatus,
    ManagementSessionStatus, ManagementSummary, ManagementTraffic,
};
use crate::config::ManagementConfig;
use crate::runtime::error::RuntimeError;
use serde::Serialize;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

const REQUEST_LIMIT: usize = 64 * 1024;
const HEADER_LIMIT: usize = 32 * 1024;
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

#[derive(Debug)]
struct HttpSettings {
    token: Option<String>,
    dashboard: bool,
}

pub(super) async fn run_listeners(
    config: ManagementConfig,
    target: ManagementTarget,
) -> Result<(), RuntimeError> {
    let settings = Arc::new(HttpSettings {
        token: config.token,
        dashboard: config.dashboard,
    });
    let capacity = Arc::new(Semaphore::new(CONNECTION_LIMIT));
    let mut listeners = tokio::task::JoinSet::new();
    for listen in config.listen {
        let listener = TcpListener::bind(listen).await?;
        listeners.spawn(run_listener(
            listener,
            target.clone(),
            settings.clone(),
            capacity.clone(),
        ));
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
                            eprintln!("warning: management API request failed: {err}");
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
                    eprintln!("warning: management API request task failed: {err}");
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

    let public = matches!(path, "/api/health")
        || settings.dashboard && matches!(path, "/" | "/dashboard.css" | "/dashboard.js");
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
        ("GET", "/api/") => {
            write_json(
                stream,
                200,
                "OK",
                &json!({
                    "schema": "mptunnel.management.v2",
                    "endpoints": {
                        "health": "GET /api/health",
                        "status": "GET /api/status",
                        "paths": "GET /api/paths",
                        "traffic": "GET /api/traffic",
                        "sessions": "GET /api/sessions",
                        "flows": "GET /api/flows",
                        "diagnostics": "GET /api/diagnostics",
                        "path_control": "POST /api/control/path",
                        "peer_diagnostics": "POST /api/diagnostics/peer"
                    },
                    "dashboard": settings.dashboard,
                    "authentication": if settings.token.is_some() { "bearer" } else { "none" }
                }),
            )
            .await
        }
        ("GET", "/api/health") => write_json(stream, 200, "OK", &json!({"ok": true})).await,
        ("GET", "/api/status") => {
            let snapshot = target.snapshot();
            write_json(stream, 200, "OK", &*snapshot).await
        }
        ("GET", "/api/paths") => {
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
        ("GET", "/api/traffic") => {
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
        ("GET", "/api/sessions") => {
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
        ("GET", "/api/flows") => {
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
        ("GET", "/api/diagnostics") => {
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
        ("POST", "/api/control/path") => {
            if let Err(err) = require_json(&request) {
                return write_error(stream, err).await;
            }
            match target.control_path_json(&request.body) {
                Ok(value) => write_json(stream, 200, "OK", &value).await,
                Err(err) => write_error(stream, err).await,
            }
        }
        ("POST", "/api/diagnostics/peer") => {
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
        "/api/"
            | "/api/health"
            | "/api/status"
            | "/api/paths"
            | "/api/traffic"
            | "/api/sessions"
            | "/api/flows"
            | "/api/diagnostics"
            | "/api/control/path"
            | "/api/diagnostics/peer"
    ) || dashboard && matches!(path, "/" | "/dashboard.css" | "/dashboard.js")
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
        headers.push((name, value));
    }
    let content_length = content_length.unwrap_or(0);
    if header_len.saturating_add(content_length) > REQUEST_LIMIT {
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

#[derive(Debug)]
pub(super) struct ManagementRequest {
    pub(super) method: String,
    pub(super) path: String,
    pub(super) headers: Vec<(String, String)>,
    pub(super) body: Vec<u8>,
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

#[derive(Debug, Clone, Copy)]
pub(super) struct ManagementHttpError {
    pub(super) status: u16,
    pub(super) reason: &'static str,
    pub(super) message: &'static str,
}

impl ManagementHttpError {
    pub(super) const fn new(status: u16, reason: &'static str, message: &'static str) -> Self {
        Self {
            status,
            reason,
            message,
        }
    }
}

#[cfg(test)]
#[path = "http_test.rs"]
mod tests;
