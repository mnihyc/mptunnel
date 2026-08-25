use super::*;
use crate::runtime::management::snapshot::ManagementState;
use crate::runtime::management::{ProductRuntimeInventory, TunL3RuntimeInventory};
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
        b"POST /api/v4/diagnostics/peer?fresh=true HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}",
    )
    .await
    .expect("request");
    assert_eq!(request.method, "POST");
    assert_eq!(request.path_without_query(), "/api/v4/diagnostics/peer");
    assert_eq!(request.body, b"{}");
}

#[test]
fn dns_explain_accepts_exactly_one_nonempty_utf8_domain_parameter() {
    let request = |path: &str| ManagementRequest {
        method: "GET".to_string(),
        path: path.to_string(),
        headers: Vec::new(),
        body: Vec::new(),
    };

    assert_eq!(
        required_single_query_parameter(
            &request("/api/v4/dns/explain?domain=www%2Eexample%2Ecom"),
            "domain"
        )
        .expect("one encoded domain"),
        "www.example.com"
    );
    for path in [
        "/api/v4/dns/explain",
        "/api/v4/dns/explain?domain=",
        "/api/v4/dns/explain?domain=a.example&domain=b.example",
        "/api/v4/dns/explain?domain=a.example&plan=default",
        "/api/v4/dns/explain?domain=%ff",
    ] {
        assert!(
            required_single_query_parameter(&request(path), "domain").is_err(),
            "{path} must be rejected"
        );
    }
}

fn health_target(generation: RuntimeGenerationControl) -> ManagementTarget {
    ManagementTarget {
        clients: Vec::new(),
        servers: Vec::new(),
        inventory: ProductRuntimeInventory::default(),
        tun_l3_inventory: TunL3RuntimeInventory::default(),
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
        "/api/v4/",
        "/api/v4/health",
        "/api/v4/health/live",
        "/api/v4/health/ready",
        "/api/v4/status",
        "/api/v4/paths",
        "/api/v4/traffic",
        "/api/v4/sessions",
        "/api/v4/flows",
        "/api/v4/diagnostics",
        "/api/v4/actions/path",
        "/api/v4/diagnostics/peer",
        "/api/v4/config",
        "/api/v4/balancers",
        "/api/v4/dns/status",
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
        "/api/v1/status",
        "/api/v2/status",
        "/api/v2/dns/status",
        "/api/v3/status",
        "/api/v3/dns/status",
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
fn health_reads_the_sampler_snapshot_without_recollecting_runtime_state() {
    let target = health_target(RuntimeGenerationControl::new());
    let before = target.snapshot();

    let _ = health_response(&target);

    let after = target.snapshot();
    assert!(Arc::ptr_eq(&before, &after));
}

#[test]
fn failed_health_is_structured_and_never_ready() {
    let generation = RuntimeGenerationControl::new();
    generation.mark_failed("listener bind failed");
    let (status, reason, response) = health_response(&health_target(generation.clone()));

    assert_eq!((status, reason), (503, "Service Unavailable"));
    assert_eq!(response["schema"], "mptunnel.health.v4");
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
        b"GET /api/v4/status HTTP/1.1\r\nAuthorization: Bearer first\r\nAuthorization: Bearer second\r\n\r\n".as_slice(),
        b"POST /api/v4/actions/path HTTP/1.1\r\nContent-Type: application/json\r\nContent-Type: text/plain\r\nContent-Length: 0\r\n\r\n".as_slice(),
        b"POST /api/v4/actions/path HTTP/1.1\r\nTransfer-Encoding: chunked\r\nContent-Length: 0\r\n\r\n".as_slice(),
        b"POST /api/v4/config/apply HTTP/1.1\r\nIf-Match: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\nIf-Match: sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\r\nContent-Length: 0\r\n\r\n".as_slice(),
    ] {
        assert_eq!(parse(raw).await.expect_err("ambiguous request").status, 400);
    }
}

#[test]
fn config_apply_requires_toml_and_one_valid_revision() {
    let revision = ConfigRevision::from_bytes(b"config");
    let quoted = ManagementRequest {
        method: "POST".to_string(),
        path: "/api/v4/config/apply".to_string(),
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
    let absolute = parse(b"GET http://localhost/api/v4/status HTTP/1.1\r\n\r\n")
        .await
        .expect_err("absolute target");
    assert_eq!(absolute.status, 400);

    let pipelined =
        parse(b"GET /api/v4/status HTTP/1.1\r\n\r\nGET /api/v4/status HTTP/1.1\r\n\r\n")
            .await
            .expect_err("pipelined request");
    assert_eq!(pipelined.status, 400);
}

#[tokio::test]
async fn parser_enforces_the_body_limit_independently_of_header_size() {
    let request = format!(
        "POST /api/v4/config/validate HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
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
    assert!(DASHBOARD_JS.contains(r#"const HEALTH_ENDPOINT = "/api/v4/health";"#));
    assert!(DASHBOARD_JS.contains("state.health && !state.health.ready"));
    assert!(DASHBOARD_JS.contains("state.health && state.health.degraded"));
    assert!(DASHBOARD_JS.contains("await refreshDashboard(\"poll\");"));
    assert!(DASHBOARD_JS.contains("await requestPeerStatus(source, true);"));
    assert!(DASHBOARD_JS.contains("function peerSurfaceVisible()"));
    assert!(DASHBOARD_JS.contains("state.refreshTimer = window.setTimeout(async function ()"));
    assert!(DASHBOARD_JS.contains("state.refreshIntervalMs !== 0"));

    for option in [
        r#"<option value="900000" selected>15 minutes</option>"#,
        r#"<option value="3600000">1 hour</option>"#,
        r#"<option value="21600000">6 hours</option>"#,
        r#"<option value="86400000">24 hours</option>"#,
        r#"<option value="0">Forever</option>"#,
    ] {
        assert!(DASHBOARD_HTML.contains(option), "missing {option}");
    }
    for overview_surface in [
        r#"id="sidebar-toggle""#,
        r#"id="overview-connections-body""#,
        r#"id="inbound-services-body""#,
        r#"id="tun-l3-services-body""#,
        r#"id="tun-l3-services-count""#,
        r#"id="admission-body""#,
        r#"id="overview-paths-body""#,
        r#"id="overview-peer-paths-body""#,
        r#"id="overview-peer-allow-badge""#,
        r#"id="chart-mode-speed""#,
        r#"id="chart-mode-total""#,
    ] {
        assert!(
            DASHBOARD_HTML.contains(overview_surface),
            "missing {overview_surface}"
        );
    }
    assert!(
        DASHBOARD_JS.contains("const CHART_WINDOWS_MS = [0, 900000, 3600000, 21600000, 86400000];")
    );
    assert!(DASHBOARD_JS.contains("function mergeChartSamples(samples)"));
    assert!(DASHBOARD_JS.contains("function trimChartSamples()"));
    assert!(DASHBOARD_JS.contains("state.chartSamples.splice(0, removeCount);"));
    assert!(DASHBOARD_JS.contains("state.trafficChartMode === \"speed\""));
    assert!(DASHBOARD_JS.contains("direction === \"server_to_client\""));
    assert!(DASHBOARD_JS.contains("formatRttMicros(path.rttvar_us)"));
    assert!(DASHBOARD_JS.contains("formatBytes(path.inflight_limit_bytes)"));
    assert!(DASHBOARD_JS.contains("formatDuration(finiteNumber(path.metric_age_us) / 1000)"));
    for (heading, expected_count) in [
        (
            r#"title="Configured path / endpoint / Path ID / physical carrier instance">Path</th>"#,
            2,
        ),
        (
            r#"title="Service name / service kind / MPP session (16-hex)">Service</th>"#,
            2,
        ),
        (
            r#"title="Queue / native carrier flight / MPP data flight / native limit">Flight</th>"#,
            2,
        ),
        (
            r#"title="Locally configured path / endpoint / peer Path ID / metric epoch">Path</th>"#,
            2,
        ),
        (
            r#"title="Queue / native carrier flight / native limit">Flight</th>"#,
            2,
        ),
    ] {
        assert_eq!(
            DASHBOARD_HTML.matches(heading).count(),
            expected_count,
            "missing or duplicated path-table heading contract: {heading}"
        );
    }
    assert!(DASHBOARD_HTML.contains(r#"<th scope="col">Source</th>"#));
    assert!(!DASHBOARD_HTML.contains("Queue / native / MPP / cap"));
    assert!(DASHBOARD_CSS.contains("min-width: 1540px;"));
    assert!(DASHBOARD_JS.contains("path.endpoint || \"Local mapping unavailable\""));
    assert!(DASHBOARD_JS.contains("formatIdentifier(path.path)"));
    assert!(DASHBOARD_JS.contains("function formatSessionId(value)"));
    assert!(
        DASHBOARD_JS.contains(r#"typeof value !== "string" || !/^(0|[1-9][0-9]*)$/.test(value)"#)
    );
    assert!(DASHBOARD_JS.contains("const sessionId = BigInt(String(value));"));
    assert!(DASHBOARD_JS.contains("sessionId.toString(16).padStart(16, \"0\")"));
    for visible_session_surface in [
        r#"flow.session_id ? formatSessionId(flow.session_id) : "Local""#,
        r#""cell-secondary cell-mono", formatSessionId(path.session_id)"#,
        r#"appendCell(row, "Session", formatSessionId(session.session_id), "cell-mono");"#,
        r#"" / Session " + formatSessionId(session.session_id)"#,
        r#"serviceLabel(selectedSession) + " / " + formatSessionId(selectedSession.session_id)"#,
        r#"serviceLabel(result) + " / " + formatSessionId(result.session_id) + " / ""#,
        r#"appendMetric(elements.peerResultSummary, "Session", formatSessionId(result.session_id));"#,
    ] {
        assert!(
            DASHBOARD_JS.contains(visible_session_surface),
            "missing hex session display surface: {visible_session_surface}"
        );
    }
    for raw_session_identity in [
        r#"return String(session.service) + ":" + String(session.service_name) + ":" + String(session.session_id);"#,
        r#"String(result.session_id) === String(session.session_id)"#,
        r#"String(state.peerResult.session_id) === String(selectedSession.session_id)"#,
        "session_id: session.session_id",
    ] {
        assert!(
            DASHBOARD_JS.contains(raw_session_identity),
            "session identity/control must retain the lossless API decimal: {raw_session_identity}"
        );
    }
    for filter_identity in [
        "session.session_id,\n        formatSessionId(session.session_id),",
        "flow.session_id,\n        formatSessionId(flow.session_id),",
    ] {
        assert!(
            DASHBOARD_JS.contains(filter_identity),
            "filters must match both raw and displayed session identity: {filter_identity}"
        );
    }
    assert_eq!(
        format!("{:016x}", 9_203_593_540_056_189_687_u64),
        "7fb9bb8fdc210af7"
    );
    assert!(
        DASHBOARD_JS.contains(r#"createElement("span", "cell-primary cell-mono", flow.source)"#)
    );
    assert!(!DASHBOARD_JS.contains("flow.source ||"));
    assert!(DASHBOARD_JS.contains("titleCase(flow.source_kind)"));
    assert!(DASHBOARD_JS.contains(r#"typeof flow.inbound !== "string""#));
    assert!(DASHBOARD_JS.contains("!Array.isArray(payload.flows)"));
    assert!(DASHBOARD_JS.contains(r#"flow.inbound_kind !== "local""#));
    assert!(DASHBOARD_JS.contains(r#"typeof flow.source !== "string""#));
    assert!(DASHBOARD_JS.contains(r#"flow.source_kind !== "local_peer""#));
    assert!(DASHBOARD_JS.contains(r#""cell-primary", flow.inbound"#));
    assert!(!DASHBOARD_JS.contains("flow.inbound ||"));
    let local_row = DASHBOARD_JS
        .split_once("function createPathRow(pathValue)")
        .expect("local path row renderer")
        .1
        .split_once("function renderPathRows")
        .expect("end of local path row renderer")
        .0;
    let peer_row = DASHBOARD_JS
        .split_once("function appendPeerPathRow(body, pathValue)")
        .expect("peer path row renderer")
        .1
        .split_once("function renderPeerPaths")
        .expect("end of peer path row renderer")
        .0;
    for (surface, labels) in [
        (
            local_row,
            &[
                "State", "Path", "Service", "Carrier", "Use", "Latency", "Rate", "Loss", "Flight",
                "Evidence", "Flows",
            ][..],
        ),
        (
            peer_row,
            &[
                "State", "Path", "Carrier", "Use", "Latency", "Rate", "Loss", "Flight", "Evidence",
            ][..],
        ),
    ] {
        let mut remainder = surface;
        for label in labels {
            let marker = format!(r#"appendCell(row, "{label}""#);
            remainder = remainder
                .split_once(&marker)
                .unwrap_or_else(|| panic!("missing or out-of-order data label: {label}"))
                .1;
        }
    }
    let peer = DASHBOARD_HTML
        .find("overview-peer-title")
        .expect("peer section");
    let connections = DASHBOARD_HTML
        .find("overview-connections-title")
        .expect("inbound connections section");
    let outbound = DASHBOARD_HTML
        .find("outbound-services-title")
        .expect("outbound section");
    assert!(peer < connections && connections < outbound);
    assert_eq!(
        DASHBOARD_HTML.matches("overview-connections-title").count(),
        2
    );
    assert!(!DASHBOARD_JS.contains("Refreshing runtime status"));
    assert!(DASHBOARD_JS.contains("window.localStorage.setItem(TOKEN_STORAGE_KEY, token);"));
    assert!(DASHBOARD_JS.contains("window.localStorage.setItem(NAVIGATION_STORAGE_KEY"));
}

#[test]
fn dashboard_charts_select_real_retained_samples_without_viewport_controls() {
    for surface in [
        r#"id="traffic-chart" tabindex="0""#,
        r#"id="flows-chart" tabindex="0""#,
        r#"id="traffic-chart-detail""#,
        r#"id="flows-chart-detail""#,
    ] {
        assert!(
            DASHBOARD_HTML.contains(surface),
            "missing chart surface: {surface}"
        );
    }
    for contract in [
        "function nearestChartSampleIndex(samples, timestamp)",
        "const timestamp = state.chartSamples[index].timestamp_unix_ms;",
        "state.selectedChartTimestamp = timestamp;",
        "state.chartSelectionTouched = index !== state.chartSamples.length - 1;",
        "state.chartSelectionTouched = false;",
        "(event.clientX - rect.left) * (layout.width / rect.width)",
        "formatChartTimestamp(sample.timestamp_unix_ms)",
        "sample.to_peer_bytes + \" B (\"",
        "sample.from_peer_bytes + \" B (\"",
        "const trafficHasData = trends.length > 0;",
        "let maximumValue = 0;",
        "maximumValue = Math.max(maximumValue",
    ] {
        assert!(
            DASHBOARD_JS.contains(contract),
            "missing chart contract: {contract}"
        );
    }
    assert!(!DASHBOARD_JS.contains("Math.max.apply"));
    assert!(!DASHBOARD_JS.contains("addEventListener(\"wheel\""));
    for rejected_viewport_model in [
        "chartViewport",
        "CHART_ZOOM",
        "zoomChart",
        "panChart",
        "resetChart",
        "is-panning",
    ] {
        assert!(
            !DASHBOARD_JS.contains(rejected_viewport_model)
                && !DASHBOARD_HTML.contains(rejected_viewport_model)
                && !DASHBOARD_CSS.contains(rejected_viewport_model),
            "chart viewport control leaked into the selection-only model: {rejected_viewport_model}"
        );
    }
    assert!(DASHBOARD_CSS.contains("touch-action: pan-y pinch-zoom;"));
    assert!(DASHBOARD_HTML.contains(r#"<p id="traffic-chart-detail" class="chart-detail">"#));
    assert!(DASHBOARD_HTML.contains(r#"<p id="flows-chart-detail" class="chart-detail">"#));
    let reconcile = DASHBOARD_JS
        .split_once("function reconcileChartSelection()")
        .expect("chart selection reconciliation")
        .1
        .split_once("function selectChartSample")
        .expect("end of chart selection reconciliation")
        .0;
    assert!(
        reconcile.contains("samples[index].timestamp_unix_ms !== state.selectedChartTimestamp")
    );
    assert!(reconcile.contains("samples[samples.length - 1].timestamp_unix_ms"));
    assert!(reconcile.contains("state.chartSelectionTouched = false;"));
    let interaction = DASHBOARD_JS
        .split_once("function bindChartInteraction(canvas)")
        .expect("chart interaction binding")
        .1
        .split_once("function trimChartSamples()")
        .expect("end of chart interaction binding")
        .0;
    assert!(interaction.contains(r#"event.key === "End""#));
    assert!(interaction.contains("selectChartSample(index, true);"));
    assert!(interaction.contains(r#"canvas.addEventListener("pointerleave""#));
    assert!(interaction.contains("selectChartSample(state.chartSamples.length - 1, false);"));
}
