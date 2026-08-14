use super::*;
use clap::Parser;
use std::ffi::OsString;
use std::fs;
use std::net::TcpListener;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const TOKEN: &str = "0123456789abcdef0123456789abcdef";

const ROUTING_CONFIG: &str = r#"
[[inbounds]]
name = "local-socks"
protocol = "socks5"
listen = ["127.0.0.1:1080"]

[[outbounds]]
name = "direct"
protocol = "direct"

[routing]

[[routing.rules]]
name = "resolved-private"
destination_cidrs = ["10.0.0.0/8"]
stages = ["post-resolution"]
decision = "allow-restricted"
outbound = "direct"
initial_demand = "throughput"
explanation = "explicit private-network access"

[[routing.rules]]
name = "default"
decision = "allow"
outbound = "direct"
explanation = "ordinary default"
"#;

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mptunnel-operations-test-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create operations test directory");
        Self(path)
    }

    fn write(&self, name: &str, value: impl AsRef<[u8]>) -> PathBuf {
        let path = self.0.join(name);
        fs::write(&path, value).expect("write operation test file");
        path
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn parse(args: Vec<OsString>) -> Cli {
    Cli::try_parse_from(args).expect("parse operational CLI")
}

fn operation_cli(directory: &TestDirectory, address: SocketAddr, tail: &[&str]) -> Cli {
    let token = directory.write("management-token.key", TOKEN);
    let mut args = vec![
        OsString::from("mptunnel"),
        OsString::from("--management-token-file"),
        token.into_os_string(),
    ];
    args.extend(tail.iter().map(OsString::from));
    args.extend([
        OsString::from("--address"),
        OsString::from(address.to_string()),
    ]);
    parse(args)
}

struct TestJsonServer {
    address: SocketAddr,
    requests: Arc<Mutex<Vec<Vec<u8>>>>,
    handle: std::thread::JoinHandle<()>,
}

fn spawn_json_server(responses: Vec<Vec<u8>>) -> TestJsonServer {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind operation test server");
    let address = listener.local_addr().expect("test server address");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let handle = std::thread::spawn(move || {
        for response in responses {
            let (mut stream, _) = listener.accept().expect("accept operation request");
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("request timeout");
            let request = read_test_request(&mut stream);
            captured.lock().expect("request capture").push(request);
            stream
                .write_all(&response)
                .expect("write operation response");
        }
    });
    TestJsonServer {
        address,
        requests,
        handle,
    }
}

fn read_test_request(stream: &mut TcpStream) -> Vec<u8> {
    let mut request = Vec::new();
    let mut expected = None;
    loop {
        if let Some(expected) = expected
            && request.len() >= expected
        {
            return request;
        }
        let mut chunk = [0_u8; 2048];
        let count = stream.read(&mut chunk).expect("read operation request");
        assert!(count > 0, "operation request truncated");
        request.extend_from_slice(&chunk[..count]);
        if let Some(header_end) = find_header_end(&request) {
            let header = std::str::from_utf8(&request[..header_end]).expect("request header");
            let body = header
                .lines()
                .find_map(|line| {
                    line.strip_prefix("Content-Length: ")
                        .map(|value| value.parse::<usize>().expect("content length"))
                })
                .unwrap_or(0);
            expected = Some(header_end + body);
        }
    }
}

fn json_response(status: u16, value: &Value) -> Vec<u8> {
    let body = serde_json::to_vec(value).expect("test JSON");
    format!(
        "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .bytes()
    .chain(body)
    .collect()
}

#[test]
fn status_authenticates_and_redacts_token_from_debug_and_output() {
    let directory = TestDirectory::new();
    let server = spawn_json_server(vec![json_response(200, &json!({"echo": TOKEN}))]);
    let cli = operation_cli(&directory, server.address, &["status"]);
    let mut output = Vec::new();

    execute(&cli, &mut output).expect("status command");
    server.handle.join().expect("status server");

    let request = server.requests.lock().expect("requests");
    let request = String::from_utf8_lossy(&request[0]);
    assert!(request.starts_with("GET /api/v4/status HTTP/1.1\r\n"));
    assert!(request.contains(&format!("Authorization: Bearer {TOKEN}\r\n")));
    let output = String::from_utf8(output).expect("status output");
    assert!(!output.contains(TOKEN));
    assert!(output.contains("<redacted>"));
    let client = ManagementClient::new(server.address, TOKEN.to_string()).expect("client");
    assert!(!format!("{client:?}").contains(TOKEN));

    let quoted_token = "0123456789abc\"def";
    let mut echoed = serde_json::Map::new();
    echoed.insert(
        format!("key-{quoted_token}"),
        Value::String(format!("value-{quoted_token}")),
    );
    let mut output = Vec::new();
    write_json(&mut output, &Value::Object(echoed), quoted_token)
        .expect("redacted escaped JSON token");
    let value: Value = serde_json::from_slice(&output).expect("redacted JSON");
    assert_eq!(value["key-<redacted>"], "value-<redacted>");
}

#[test]
fn status_can_use_the_canonical_configured_listener_and_token_reference() {
    let directory = TestDirectory::new();
    directory.write("management-token.key", TOKEN);
    let server = spawn_json_server(vec![json_response(200, &json!({"ready": true}))]);
    let config = directory.write(
        "config.toml",
        format!(
            r#"
[management]
listen = ["{}"]
token = {{ from = "file", path = "management-token.key" }}

{ROUTING_CONFIG}
"#,
            server.address
        ),
    );
    let cli = parse(vec![
        OsString::from("mptunnel"),
        OsString::from("--config"),
        config.into_os_string(),
        OsString::from("status"),
    ]);
    let mut output = Vec::new();

    execute(&cli, &mut output).expect("configured status command");
    server.handle.join().expect("configured status server");
    assert!(
        String::from_utf8(output)
            .expect("configured status output")
            .contains("\"ready\": true")
    );
    let requests = server.requests.lock().expect("requests");
    assert!(
        String::from_utf8_lossy(&requests[0])
            .contains(&format!("Authorization: Bearer {TOKEN}\r\n"))
    );
}

#[test]
fn management_client_rejects_oversized_and_ambiguous_responses() {
    let oversized = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        MANAGEMENT_BODY_LIMIT + 1
    )
    .into_bytes();
    let ambiguous = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nContent-Length: 2\r\n\r\n{}".to_vec();
    let server = spawn_json_server(vec![oversized, ambiguous]);
    let client = ManagementClient::new(server.address, TOKEN.to_string()).expect("client");

    assert!(matches!(
        client.get("/api/v4/status"),
        Err(OperationError::ManagementResponseBodyTooLarge)
    ));
    assert!(matches!(
        client.get("/api/v4/status"),
        Err(OperationError::AmbiguousManagementResponse)
    ));
    server.handle.join().expect("bounded response server");
}

#[test]
fn dns_commands_use_only_the_versioned_authenticated_contract() {
    let directory = TestDirectory::new();
    let responses = (0..4)
        .map(|_| json_response(200, &json!({"ok": true})))
        .collect();
    let server = spawn_json_server(responses);
    for command in [
        vec!["dns", "status"],
        vec!["dns", "explain", "WWW.Example."],
        vec!["dns", "query", "www.example", "--type", "AAAA"],
        vec!["dns", "flush", "--policy", "default"],
    ] {
        let cli = operation_cli(&directory, server.address, &command);
        execute(&cli, &mut Vec::new()).expect("DNS operation");
    }
    server.handle.join().expect("DNS server");

    let requests = server.requests.lock().expect("requests");
    let rendered = requests
        .iter()
        .map(|request| String::from_utf8_lossy(request).into_owned())
        .collect::<Vec<_>>();
    assert!(rendered[0].starts_with("GET /api/v4/dns/status HTTP/1.1\r\n"));
    assert!(rendered[1].starts_with("GET /api/v4/dns/explain?domain=www%2Eexample HTTP/1.1\r\n"));
    assert!(rendered[2].starts_with("POST /api/v4/dns/query HTTP/1.1\r\n"));
    assert!(rendered[2].ends_with(r#"{"domain":"www.example","type":"AAAA"}"#));
    assert!(rendered[3].starts_with("POST /api/v4/dns/cache/flush HTTP/1.1\r\n"));
    assert!(rendered[3].ends_with(r#"{"policy":"default"}"#));
    assert!(
        rendered
            .iter()
            .all(|request| request.contains(&format!("Authorization: Bearer {TOKEN}\r\n")))
    );
}

#[test]
fn route_explain_uses_the_canonical_pre_and_post_resolution_policy() {
    let directory = TestDirectory::new();
    let config = directory.write("config.toml", ROUTING_CONFIG);
    let base = vec![
        OsString::from("mptunnel"),
        OsString::from("--config"),
        config.into_os_string(),
        OsString::from("route"),
        OsString::from("explain"),
        OsString::from("--target"),
        OsString::from("service.example:443"),
        OsString::from("--network"),
        OsString::from("tcp"),
        OsString::from("--source"),
        OsString::from("198.51.100.9:42000"),
        OsString::from("--principal-id"),
        OsString::from("alice"),
        OsString::from("--inbound"),
        OsString::from("local-socks"),
    ];

    let mut pre = Vec::new();
    execute(&parse(base.clone()), &mut pre).expect("pre-resolution explanation");
    let pre = String::from_utf8(pre).expect("pre explanation");
    assert!(pre.contains("stage: pre-resolution"));
    assert!(pre.contains("rule: default"));
    assert!(pre.contains("decision: allow\n  egress: outbound\n  outbound: direct"));
    assert!(pre.contains("selector: default"));
    assert!(pre.contains("dns_rule: none"));
    assert!(pre.contains("source: 198.51.100.9:42000"));
    assert!(pre.contains("restricted_class: none"));
    assert!(pre.contains("outcome: not-evaluated"));
    assert!(pre.contains("id: resolved-private"));
    assert!(pre.contains("result: mismatch (destination IP)"));

    let mut post_args = base.clone();
    post_args.extend([OsString::from("--resolved-ip"), OsString::from("10.0.0.42")]);
    let mut post = Vec::new();
    execute(&parse(post_args), &mut post).expect("post-resolution explanation");
    let post = String::from_utf8(post).expect("post explanation");
    assert!(post.contains("stage: post-resolution"));
    assert!(post.contains("rule: resolved-private"));
    assert!(post.contains("initial_demand: throughput"));
    assert!(post.contains("decision: allow-restricted\n  egress: outbound\n  outbound: direct"));
    assert!(
        post.contains("resolution:\n  route_rule: default\n  policy: default\n  selector: default")
    );
    assert!(post.contains("restricted_class: private"));
    assert!(post.contains("restricted_authorized: true"));
    assert!(post.contains("outcome: authorized"));
    assert!(post.contains("authorization:\n  restricted_class: private\n  restricted_authorized: true\n  outcome: authorized\n  rule: resolved-private"));

    let mut denied_args = base;
    denied_args.extend([OsString::from("--resolved-ip"), OsString::from("127.0.0.1")]);
    let mut denied = Vec::new();
    execute(&parse(denied_args), &mut denied).expect("restricted denial explanation");
    let denied = String::from_utf8(denied).expect("denied explanation");
    assert!(denied.contains("decision: allow"));
    assert!(denied.contains("restricted_class: loopback"));
    assert!(denied.contains("restricted_authorized: false"));
    assert!(denied.contains("outcome: denied"));
    assert!(denied.contains("rule: default"));
}

#[test]
fn route_explain_skips_ip_family_ineligible_outbounds_and_balancers() {
    let directory = TestDirectory::new();
    let config = directory.write(
        "config.toml",
        r#"
[[inbounds]]
name = "local-socks"
protocol = "socks5"
listen = ["127.0.0.1:1080"]

[[outbounds]]
name = "ipv6-only"
protocol = "direct"
bind_ipv6 = "2001:db8::10"

[[outbounds]]
name = "dual-stack"
protocol = "direct"

[routing]

[[routing.balancers]]
name = "ipv6-only-balancer"
strategy = "ordered-failover"
members = [{ outbound = "ipv6-only" }]

[[routing.balancers]]
name = "mixed-family-balancer"
strategy = "ordered-failover"
members = [{ outbound = "ipv6-only" }, { outbound = "dual-stack" }]

[[routing.rules]]
name = "ipv6-only-outbound"
destination_cidrs = ["0.0.0.0/0"]
stages = ["post-resolution"]
decision = "allow"
outbound = "ipv6-only"

[[routing.rules]]
name = "ipv6-only-group"
destination_cidrs = ["0.0.0.0/0"]
stages = ["post-resolution"]
decision = "allow"
balancer = "ipv6-only-balancer"

[[routing.rules]]
name = "mixed-family-group"
destination_cidrs = ["0.0.0.0/0"]
stages = ["post-resolution"]
decision = "allow"
balancer = "mixed-family-balancer"

[[routing.rules]]
name = "default"
decision = "allow"
outbound = "dual-stack"
"#,
    );
    let cli = parse(vec![
        OsString::from("mptunnel"),
        OsString::from("--config"),
        config.into_os_string(),
        OsString::from("route"),
        OsString::from("explain"),
        OsString::from("--target"),
        OsString::from("service.example:443"),
        OsString::from("--network"),
        OsString::from("tcp"),
        OsString::from("--resolved-ip"),
        OsString::from("192.0.2.42"),
        OsString::from("--principal-id"),
        OsString::from("anonymous"),
        OsString::from("--inbound"),
        OsString::from("local-socks"),
    ]);
    let mut output = Vec::new();

    execute(&cli, &mut output).expect("IP-family-aware route explanation");
    let output = String::from_utf8(output).expect("UTF-8 route explanation");

    assert!(output.contains("stage: post-resolution"));
    assert!(
        output
            .contains("resolution:\n  route_rule: default\n  policy: default\n  selector: default")
    );
    assert!(output.contains(
        "selected:\n  rule: mixed-family-group\n  outcome: matched\n  decision: allow\n  egress: balancer\n  balancer: mixed-family-balancer"
    ));
    assert!(output.contains("  - id: ipv6-only-outbound\n    result: mismatch (egress IP family)"));
    assert!(output.contains("  - id: ipv6-only-group\n    result: mismatch (egress IP family)"));
    assert!(output.contains("  - id: mixed-family-group\n    result: selected"));
}

#[test]
fn route_explain_keeps_dns_policy_provenance_during_family_fallthrough() {
    let directory = TestDirectory::new();
    let config = directory.write(
        "config.toml",
        r#"
[[inbounds]]
name = "local-socks"
protocol = "socks5"
listen = ["127.0.0.1:1080"]

[[outbounds]]
name = "ipv4-only"
protocol = "direct"
bind_ipv4 = "198.51.100.2"

[[outbounds]]
name = "ipv6-only"
protocol = "direct"
bind_ipv6 = "2001:db8::2"

[[outbounds]]
name = "fallback"
protocol = "direct"

[dns]
default = "policy-a"

[[dns.servers]]
name = "system"
protocol = "system"

[[dns.policies]]
name = "policy-a"
servers = ["system"]

[[dns.policies]]
name = "policy-b"
servers = ["system"]

[[dns.rules]]
name = "activate-policy-b"
exact = "activate-b.example"
policy = "policy-b"

[routing]

[[routing.rules]]
name = "first-policy-a"
domain_exact = ["family-policy.example"]
outbound = "ipv4-only"
dns_policy = "policy-a"

[[routing.rules]]
name = "second-policy-b"
domain_exact = ["family-policy.example"]
outbound = "ipv6-only"
dns_policy = "policy-b"

[[routing.rules]]
name = "fallback"
outbound = "fallback"
"#,
    );
    let cli = parse(vec![
        OsString::from("mptunnel"),
        OsString::from("--config"),
        config.into_os_string(),
        OsString::from("route"),
        OsString::from("explain"),
        OsString::from("--target"),
        OsString::from("family-policy.example:443"),
        OsString::from("--network"),
        OsString::from("tcp"),
        OsString::from("--resolved-ip"),
        OsString::from("2001:4860:4860::8888"),
        OsString::from("--principal-id"),
        OsString::from("anonymous"),
        OsString::from("--inbound"),
        OsString::from("local-socks"),
    ]);
    let mut output = Vec::new();

    execute(&cli, &mut output).expect("DNS-provenance-aware route explanation");
    let output = String::from_utf8(output).expect("UTF-8 explanation");

    assert!(output.contains(
        "resolution:\n  route_rule: first-policy-a\n  policy: policy-a\n  selector: route"
    ));
    assert!(output.contains("selected:\n  rule: fallback"));
    assert!(output.contains("first-policy-a\n    result: mismatch (egress IP family)"));
    assert!(output.contains("second-policy-b\n    result: mismatch (DNS policy provenance)"));
}

#[test]
fn route_explain_marks_the_implicit_reject_without_inventing_a_rule_name() {
    use crate::product::{
        FlowContext, InboundId, PrincipalId, ProductPolicyGeneration, ProtocolTarget, RouteInput,
    };

    let dns = crate::config::DnsPolicyConfig::system_default()
        .compile()
        .expect("default DNS policy");
    let policy = ProductPolicyGeneration::compile(11, Vec::new()).expect("implicit reject policy");
    let flow = FlowContext::without_source(
        crate::product::Network::Tcp,
        ProtocolTarget::parse_authority("outside.example:443").expect("target"),
        PrincipalId::parse("anonymous").expect("principal"),
        InboundId::parse("local-socks").expect("inbound"),
    );
    let input = RouteInput::pre_resolution(&flow);
    let explanation = policy.routes().explain(input);
    let mut output = Vec::new();
    render_route_explanation(&dns, &policy, &flow, input, &explanation, &mut output)
        .expect("render implicit reject");
    let output = String::from_utf8(output).expect("UTF-8 explanation");

    assert!(output.contains("selected:\n  rule: none\n  outcome: unmatched"));
    assert!(output.contains("resolution:\n  route_rule: none"));
    assert!(output.contains("  - id: none\n    result: unmatched (implicit reject)"));
    assert!(!output.contains("rule: unmatched"));
    assert!(!output.contains("id: unmatched"));
}

#[test]
fn route_explain_reports_route_and_split_dns_selector_provenance_with_named_attachments() {
    use crate::product::{
        CompiledDnsPolicy, DnsOverrideRecordId, DnsOverrideRecordSpec, DnsPlanId, DnsPlanSpec,
        DnsPolicySpec, DnsRuleId, DnsRuleMatch, DnsRuleSpec, DnsSyntheticCaptureId,
        DnsSyntheticCaptureSpec, DnsUpstreamEndpoint, DnsUpstreamId, DnsUpstreamSpec, EgressAction,
        FlowContext, InboundId, InitialDemand, OutboundId, PrincipalId, ProductPolicyGeneration,
        ProtocolTarget, RouteAction, RouteInput, RouteMatchSpec, RouteRuleSpec, RuleId,
    };

    let upstream = DnsUpstreamId::parse("system").expect("upstream");
    let default = DnsPlanId::parse("default").expect("default policy");
    let split = DnsPlanId::parse("private").expect("split policy");
    let explicit = DnsPlanId::parse("route-selected").expect("route policy");
    let record = DnsOverrideRecordId::parse("private-api").expect("record");
    let capture = DnsSyntheticCaptureId::parse("private-capture").expect("capture");
    let mut private = DnsPlanSpec::new(split.clone(), vec![upstream.clone()]);
    private.override_records = vec![record.clone()];
    private.synthetic_capture = Some(capture.clone());
    let dns = CompiledDnsPolicy::compile(
        9,
        DnsPolicySpec {
            upstreams: vec![DnsUpstreamSpec::direct(
                upstream.clone(),
                DnsUpstreamEndpoint::System,
            )],
            outbound_capabilities: Vec::new(),
            plans: vec![
                DnsPlanSpec::new(default.clone(), vec![upstream.clone()]),
                private,
                DnsPlanSpec::new(explicit.clone(), vec![upstream]),
            ],
            rules: vec![
                DnsRuleSpec {
                    id: DnsRuleId::parse("private-exact").expect("exact rule"),
                    matcher: DnsRuleMatch::Exact(
                        crate::product::DomainName::parse("api.corp.example").expect("domain"),
                    ),
                    plan: split.clone(),
                    explanation: None,
                },
                DnsRuleSpec {
                    id: DnsRuleId::parse("private-suffix").expect("suffix rule"),
                    matcher: DnsRuleMatch::Suffix(
                        crate::product::DomainName::parse("corp.example").expect("domain"),
                    ),
                    plan: split,
                    explanation: None,
                },
            ],
            override_records: vec![DnsOverrideRecordSpec {
                id: record,
                domain: crate::product::DomainName::parse("api.corp.example").expect("domain"),
                addresses: vec!["10.0.0.8".parse().expect("address")],
            }],
            synthetic_captures: vec![DnsSyntheticCaptureSpec {
                id: capture,
                ipv4_pool: Some("198.18.0.0/24".parse().expect("pool")),
                ipv6_pool: None,
                max_entries: 32,
                answer_ttl: Duration::from_secs(30),
                recovery_ttl: Duration::from_secs(120),
            }],
            default_plan: default,
        },
    )
    .expect("DNS policy");

    let outbound = OutboundId::parse("direct").expect("outbound");
    let policy = ProductPolicyGeneration::compile(
        7,
        vec![
            RouteRuleSpec::new(
                RuleId::parse("route-dns").expect("route rule"),
                RouteMatchSpec {
                    domain_exact: vec![
                        crate::product::DomainName::parse("route.example").expect("domain"),
                    ],
                    ..RouteMatchSpec::default()
                },
                RouteAction::allow(
                    EgressAction::Outbound(outbound.clone()),
                    Some(explicit),
                    InitialDemand::Automatic,
                ),
            ),
            RouteRuleSpec::new(
                RuleId::parse("default").expect("default route"),
                RouteMatchSpec::default(),
                RouteAction::allow(
                    EgressAction::Outbound(outbound),
                    None,
                    InitialDemand::Automatic,
                ),
            ),
        ],
    )
    .expect("routing policy");

    let render = |target: &str| {
        let flow = FlowContext::without_source(
            crate::product::Network::Tcp,
            ProtocolTarget::parse_authority(target).expect("target"),
            PrincipalId::parse("peer").expect("principal"),
            InboundId::parse("mpp-server").expect("inbound"),
        );
        let input = RouteInput::pre_resolution(&flow);
        let explanation = policy.routes().explain(input);
        let mut output = Vec::new();
        render_route_explanation(&dns, &policy, &flow, input, &explanation, &mut output)
            .expect("render explanation");
        String::from_utf8(output).expect("UTF-8 explanation")
    };

    let exact = render("api.corp.example:443");
    assert!(exact.contains("selector: exact"));
    assert!(exact.contains("dns_rule: private-exact"));
    assert!(exact.contains("matched_domain: api.corp.example"));
    assert!(exact.contains("override_records: [private-api]"));
    assert!(exact.contains("synthetic_capture: private-capture"));
    assert!(exact.contains("source: none"));

    let suffix = render("www.corp.example:443");
    assert!(suffix.contains("selector: suffix"));
    assert!(suffix.contains("dns_rule: private-suffix"));
    assert!(suffix.contains("matched_domain: corp.example"));

    let route = render("route.example:443");
    assert!(route.contains("policy: route-selected"));
    assert!(route.contains("selector: route"));
    assert!(route.contains("dns_rule: none"));

    let fallback = render("public.example:443");
    assert!(fallback.contains("policy: default"));
    assert!(fallback.contains("selector: default"));
}

#[test]
fn doctor_has_stable_success_warning_and_failure_outcomes() {
    let directory = TestDirectory::new();
    let config = directory.write("config.toml", ROUTING_CONFIG);
    let valid = parse(vec![
        OsString::from("mptunnel"),
        OsString::from("--config"),
        config.into_os_string(),
        OsString::from("doctor"),
    ]);
    let mut output = Vec::new();
    execute(&valid, &mut output).expect("valid offline doctor");
    let output = String::from_utf8(output).expect("doctor output");
    assert!(output.contains("[PASS] config:"));
    assert!(output.contains("doctor: PASS"));

    let missing = parse(vec![
        OsString::from("mptunnel"),
        OsString::from("--config"),
        directory.0.join("missing.toml").into_os_string(),
        OsString::from("doctor"),
    ]);
    let mut output = Vec::new();
    assert!(matches!(
        execute(&missing, &mut output),
        Err(OperationError::DoctorFailed)
    ));
    let output = String::from_utf8(output).expect("doctor failure output");
    assert!(output.contains("[FAIL] config:"));
    assert!(output.contains("doctor: FAIL"));

    let mut warning = DoctorReport::default();
    warning.warn("endpoint", "temporarily unreachable");
    let mut output = Vec::new();
    warning.render(&mut output).expect("warning report");
    assert!(!warning.failed());
    assert!(
        String::from_utf8(output)
            .expect("warning output")
            .contains("doctor: WARN")
    );
}

#[test]
fn doctor_skips_host_dns_for_domains_and_preserves_literal_ip_probes() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind literal endpoint");
    let literal = listener.local_addr().expect("literal endpoint address");
    let results = probe_endpoints(vec![
        ProbeEndpoint {
            label: "domain endpoint".to_string(),
            authority: "localhost:443".to_string(),
            endpoint: Endpoint::new("localhost", 443).expect("domain endpoint"),
            connect: true,
            skip_connect: false,
        },
        ProbeEndpoint {
            label: "literal endpoint".to_string(),
            authority: literal.to_string(),
            endpoint: Endpoint::new(literal.ip().to_string(), literal.port())
                .expect("literal endpoint"),
            connect: true,
            skip_connect: false,
        },
        ProbeEndpoint {
            label: "ranged carrier endpoint".to_string(),
            authority: "192.0.2.10:20000-40000".to_string(),
            endpoint: Endpoint::new("192.0.2.10", 20000).expect("carrier endpoint"),
            connect: false,
            skip_connect: true,
        },
    ])
    .expect("doctor endpoint probes");

    assert!(matches!(
        &results[0].outcome,
        EndpointProbeOutcome::Skipped(message)
            if message.contains("host DNS and direct probing were skipped")
                && message.contains("runtime DNS and routing own resolution")
                && !message.contains("127.0.0.1")
    ));
    assert!(matches!(
        &results[1].outcome,
        EndpointProbeOutcome::Reachable(message)
            if message.contains(&literal.to_string())
    ));
    assert!(matches!(
        &results[2].outcome,
        EndpointProbeOutcome::Skipped(message)
            if message.contains("192.0.2.10:20000-40000")
                && message.contains("configured carrier selection")
    ));
}
