use super::*;
use crate::runtime::management::ProductRuntimeInventory;
use crate::runtime::management::snapshot::ManagementState;
use crate::runtime::readiness::{
    RuntimeGenerationControl, RuntimeGenerationPhase, RuntimeReadinessBarrier,
};
use crate::runtime::telemetry::RuntimeTelemetry;

async fn parse(raw: &[u8]) -> Result<ManagementRequest, ManagementHttpError> {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let mut client = TcpStream::connect(listener.local_addr().expect("address"))
        .await
        .expect("client");
    let (mut server, _) = listener.accept().await.expect("accept");
    client.write_all(raw).await.expect("request");
    client.shutdown().await.expect("shutdown");
    read_request(&mut server).await
}

#[tokio::test]
async fn parser_accepts_one_complete_origin_form_request() {
    let request = parse(
        b"POST /api/v1/diagnostics/peer?fresh=true HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}",
    )
    .await
    .expect("request");
    assert_eq!(request.method, "POST");
    assert_eq!(request.path_without_query(), "/api/v1/diagnostics/peer");
    assert_eq!(request.body, b"{}");
}

#[test]
fn dns_explain_accepts_exactly_one_nonempty_utf8_name_parameter() {
    let request = |path: &str| ManagementRequest {
        method: "GET".to_string(),
        path: path.to_string(),
        headers: Vec::new(),
        body: Vec::new(),
    };

    assert_eq!(
        required_single_query_parameter(
            &request("/api/v1/dns/explain?name=www%2Eexample%2Ecom"),
            "name"
        )
        .expect("one encoded name"),
        "www.example.com"
    );
    for path in [
        "/api/v1/dns/explain",
        "/api/v1/dns/explain?name=",
        "/api/v1/dns/explain?name=a.example&name=b.example",
        "/api/v1/dns/explain?name=a.example&plan=default",
        "/api/v1/dns/explain?name=%ff",
    ] {
        assert!(
            required_single_query_parameter(&request(path), "name").is_err(),
            "{path} must be rejected"
        );
    }
}

fn health_target(generation: RuntimeGenerationControl) -> ManagementTarget {
    ManagementTarget {
        clients: Vec::new(),
        servers: Vec::new(),
        inventory: ProductRuntimeInventory::default(),
        product_telemetry: RuntimeTelemetry::new(8),
        state: ManagementState::new("node"),
        config_control: None,
        gateway_control: None,
        dns: None,
        product_admission: crate::product::ProductAdmission::default(),
        generation,
    }
}

#[test]
fn management_api_is_one_authenticated_versioned_surface() {
    for path in [
        "/api/v1/",
        "/api/v1/health",
        "/api/v1/health/live",
        "/api/v1/health/ready",
        "/api/v1/status",
        "/api/v1/paths",
        "/api/v1/traffic",
        "/api/v1/sessions",
        "/api/v1/flows",
        "/api/v1/diagnostics",
        "/api/v1/actions/path",
        "/api/v1/diagnostics/peer",
        "/api/v1/config",
        "/api/v1/gateways",
        "/api/v1/dns/status",
    ] {
        assert!(known_path(path, false), "{path} must be registered");
        assert!(!public_path(path, true), "{path} must require bearer auth");
    }
    for legacy in [
        "/api/",
        "/api/health",
        "/api/status",
        "/api/paths",
        "/api/traffic",
        "/api/sessions",
        "/api/flows",
        "/api/diagnostics",
        "/api/control/path",
        "/api/diagnostics/peer",
    ] {
        assert!(
            !known_path(legacy, true),
            "{legacy} must not remain as an alias"
        );
    }
    for asset in ["/", "/dashboard.css", "/dashboard.js"] {
        assert!(public_path(asset, true));
        assert!(!public_path(asset, false));
    }
}

#[test]
fn health_distinguishes_process_liveness_from_traffic_readiness() {
    let generation = RuntimeGenerationControl::new();
    let target = health_target(generation.clone());

    let (status, reason, response) = health_response(&target);
    assert_eq!((status, reason), (200, "OK"));
    assert_eq!(response["live"], true);
    assert_eq!(response["ready"], false);
    assert_eq!(response["degraded"], false);
    assert_eq!(response["phase"], "starting");
    let (status, reason, _) = readiness_response(&target);
    assert_eq!((status, reason), (503, "Service Unavailable"));

    RuntimeReadinessBarrier::new(generation.clone()).seal();
    let (status, reason, response) = health_response(&target);
    assert_eq!((status, reason), (200, "OK"));
    assert_eq!(response["live"], true);
    assert_eq!(response["ready"], true);
    assert_eq!(response["degraded"], false);
    assert_eq!(response["status"], "healthy");
    assert_eq!(response["phase"], "ready");
    let (status, reason, _) = readiness_response(&target);
    assert_eq!((status, reason), (200, "OK"));

    generation.mark_stopping();
    let (status, reason, response) = health_response(&target);
    assert_eq!((status, reason), (200, "OK"));
    assert_eq!(response["live"], true);
    assert_eq!(response["ready"], false);
    assert_eq!(response["phase"], "stopping");
    let (status, reason, _) = readiness_response(&target);
    assert_eq!((status, reason), (503, "Service Unavailable"));
}

#[test]
fn failed_health_is_structured_and_never_ready() {
    let generation = RuntimeGenerationControl::new();
    generation.mark_failed("listener bind failed");
    let (status, reason, response) = health_response(&health_target(generation.clone()));

    assert_eq!((status, reason), (503, "Service Unavailable"));
    assert_eq!(response["schema"], "mptunnel.health.v1");
    assert_eq!(response["live"], false);
    assert_eq!(response["ready"], false);
    assert_eq!(response["phase"], "failed");
    assert_eq!(response["failure"], "listener bind failed");
    assert_eq!(generation.status().phase, RuntimeGenerationPhase::Failed);
}

#[tokio::test]
async fn management_bind_failure_fails_the_startup_barrier() {
    let occupied = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("occupied listener");
    let generation = RuntimeGenerationControl::new();
    let barrier = RuntimeReadinessBarrier::new(generation.clone());
    let readiness = barrier.require("management listeners");
    barrier.seal();
    let config = ManagementConfig {
        listen: vec![occupied.local_addr().expect("occupied address")],
        token: Some("0123456789abcdef".to_string()),
        dashboard: false,
        allow_peer_diagnostics: false,
    };

    let mut services = tokio::task::JoinSet::new();
    spawn_listeners(
        config,
        health_target(generation.clone()),
        readiness,
        &mut services,
    )
    .await
    .expect_err("occupied listener must fail");
    assert!(services.is_empty());
    assert_eq!(generation.status().phase, RuntimeGenerationPhase::Failed);
}

#[tokio::test]
async fn parser_rejects_ambiguous_security_headers() {
    for raw in [
        b"GET /api/v1/status HTTP/1.1\r\nAuthorization: Bearer first\r\nAuthorization: Bearer second\r\n\r\n".as_slice(),
        b"POST /api/v1/actions/path HTTP/1.1\r\nContent-Type: application/json\r\nContent-Type: text/plain\r\nContent-Length: 0\r\n\r\n".as_slice(),
        b"POST /api/v1/actions/path HTTP/1.1\r\nTransfer-Encoding: chunked\r\nContent-Length: 0\r\n\r\n".as_slice(),
        b"POST /api/v1/config/apply HTTP/1.1\r\nIf-Match: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\nIf-Match: sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\r\nContent-Length: 0\r\n\r\n".as_slice(),
    ] {
        assert_eq!(parse(raw).await.expect_err("ambiguous request").status, 400);
    }
}

#[test]
fn config_apply_requires_toml_and_one_valid_revision() {
    let revision = ConfigRevision::from_bytes(b"config");
    let quoted = ManagementRequest {
        method: "POST".to_string(),
        path: "/api/v1/config/apply".to_string(),
        headers: vec![
            ("content-type".to_string(), "application/toml".to_string()),
            ("if-match".to_string(), format!("\"{revision}\"")),
        ],
        body: Vec::new(),
    };
    assert!(require_toml(&quoted).is_ok());
    assert_eq!(
        required_config_revision(&quoted).expect("quoted revision"),
        revision
    );

    let mut missing = quoted;
    missing.headers.retain(|(name, _)| name != "if-match");
    assert_eq!(
        required_config_revision(&missing)
            .expect_err("missing precondition")
            .status,
        428
    );
}

#[tokio::test]
async fn parser_rejects_absolute_targets_and_pipelining() {
    let absolute = parse(b"GET http://localhost/api/v1/status HTTP/1.1\r\n\r\n")
        .await
        .expect_err("absolute target");
    assert_eq!(absolute.status, 400);

    let pipelined =
        parse(b"GET /api/v1/status HTTP/1.1\r\n\r\nGET /api/v1/status HTTP/1.1\r\n\r\n")
            .await
            .expect_err("pipelined request");
    assert_eq!(pipelined.status, 400);
}

#[tokio::test]
async fn parser_enforces_the_body_limit_independently_of_header_size() {
    let request = format!(
        "POST /api/v1/config/validate HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
        BODY_LIMIT + 1
    );
    let error = parse(request.as_bytes())
        .await
        .expect_err("oversized declared body");
    assert_eq!(error.status, 413);
}

#[test]
fn dashboard_auth_form_cannot_navigate_without_javascript() {
    assert!(DASHBOARD_HTML.contains(r#"<form id="auth-form" class="auth-form" method="dialog">"#));
    assert!(DASHBOARD_HTML.contains(r#"<link rel="icon" href="data:image/svg+xml,"#));
    assert!(CONTENT_SECURITY_POLICY.contains("form-action 'none'"));
}

#[test]
fn dashboard_auto_refresh_contract_is_bounded_and_includes_peer_status() {
    assert!(DASHBOARD_HTML.contains(r#"<label class="refresh-control" for="refresh-interval">"#));
    for option in [
        r#"<option value="1000">1 second</option>"#,
        r#"<option value="5000" selected>5 seconds</option>"#,
        r#"<option value="30000">30 seconds</option>"#,
        r#"<option value="0">Manual only</option>"#,
    ] {
        assert!(DASHBOARD_HTML.contains(option), "missing {option}");
    }
    assert!(DASHBOARD_JS.contains("const REFRESH_INTERVALS_MS = [0, 1000, 5000, 30000];"));
    assert!(DASHBOARD_JS.contains(r#"const HEALTH_ENDPOINT = "/api/v1/health";"#));
    assert!(DASHBOARD_JS.contains("state.health && !state.health.ready"));
    assert!(DASHBOARD_JS.contains("state.health && state.health.degraded"));
    assert!(DASHBOARD_JS.contains("await refreshDashboard(\"poll\");"));
    assert!(DASHBOARD_JS.contains("await requestPeerStatus(source, true);"));
    assert!(DASHBOARD_JS.contains("state.refreshTimer = window.setTimeout(async function ()"));
    assert!(DASHBOARD_JS.contains("state.refreshIntervalMs !== 0"));
}
