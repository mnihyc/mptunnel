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
generation = 7

[[routing.rules]]
name = "resolved-private"
destination_cidrs = ["192.0.2.0/24"]
stages = ["post-resolution"]
action = "outbound"
outbound = "direct"
initial_demand = "throughput"
explanation = "resolved documentation network"

[[routing.rules]]
name = "default"
action = "outbound"
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
    assert!(request.starts_with("GET /api/v3/status HTTP/1.1\r\n"));
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
        client.get("/api/v3/status"),
        Err(OperationError::ManagementResponseBodyTooLarge)
    ));
    assert!(matches!(
        client.get("/api/v3/status"),
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
    assert!(rendered[0].starts_with("GET /api/v3/dns/status HTTP/1.1\r\n"));
    assert!(rendered[1].starts_with("GET /api/v3/dns/explain?domain=www%2Eexample HTTP/1.1\r\n"));
    assert!(rendered[2].starts_with("POST /api/v3/dns/query HTTP/1.1\r\n"));
    assert!(rendered[2].ends_with(r#"{"domain":"www.example","type":"AAAA"}"#));
    assert!(rendered[3].starts_with("POST /api/v3/dns/cache/flush HTTP/1.1\r\n"));
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
    assert!(pre.contains("action: outbound\n  outbound: direct"));
    assert!(pre.contains("id: resolved-private"));
    assert!(pre.contains("result: mismatch (destination IP)"));

    let mut post_args = base;
    post_args.extend([
        OsString::from("--resolved-ip"),
        OsString::from("192.0.2.42"),
    ]);
    let mut post = Vec::new();
    execute(&parse(post_args), &mut post).expect("post-resolution explanation");
    let post = String::from_utf8(post).expect("post explanation");
    assert!(post.contains("stage: post-resolution"));
    assert!(post.contains("rule: resolved-private"));
    assert!(post.contains("initial_demand: throughput"));
    assert!(post.contains("action: outbound\n  outbound: direct"));
    assert!(post.contains("resolution:\n  rule: default\n  dns_policy: default (policy default)"));
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
