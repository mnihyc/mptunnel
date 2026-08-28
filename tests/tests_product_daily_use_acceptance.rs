#[path = "support/tests_support.rs"]
mod support;

use hickory_proto::op::{Message, MessageType, Query, ResponseCode};
use hickory_proto::rr::rdata::A;
use hickory_proto::rr::{Name, RData, RecordType};
use mptunnel::config::{CommandConfig, load_config_toml_str};
use mptunnel::dns::{
    DnsBackendFactory, DnsBackendResponse, DnsGeneration, DnsQueryBackend, DnsQuestion,
    DnsRecordBackendFuture, DnsRuntimeError,
};
use mptunnel::product::{CompiledDnsPlan, CompiledDnsUpstream, DnsSecurityPolicy, DomainName};
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use support::{
    MptunnelProcess, SocksTarget, TestDirectory, check_config, http_connect, http_request,
    join_thread, network_test_guard, socks5_connect, socks5_round_trip, socks5_udp_round_trip,
    spawn_blackhole_proxy, spawn_echo_http_connect_proxy, spawn_echo_socks5_proxy, spawn_tcp_echo,
    spawn_tcp_echo_connections, spawn_udp_echo_packets, udp_round_trip, unused_loopback_addr,
    unused_loopback_udp_addr, wait_for_ready_management, wait_for_tcp, wait_for_tcp_closed,
};

const OPERATOR_TOKEN: &str = "daily-use-operator-token";
const PROCESS_START_TIMEOUT: Duration = Duration::from_secs(10);
const LOCAL_DNS_TARGET: &str = "daily-use.test";
const DELEGATED_HTTP_TARGET: &str = "remote-resolution.invalid:443";

fn reject_runtime_config(
    socks: &[SocketAddr],
    management: SocketAddr,
    route_revision: u64,
) -> String {
    let socks = socks
        .iter()
        .map(|address| format!("\"{address}\""))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        r#"
[logging]
level = "warn"

[management]
listen = ["{management}"]
token = {{ from = "file", path = "operator-token.key" }}
dashboard = false
allow_peer_diagnostics = false

[[inbounds]]
name = "local-socks"
protocol = "socks5"
listen = [{socks}]

[routing]

[[routing.rules]]
name = "default-reject-{route_revision}"
decision = "reject"
"#
    )
}

#[allow(clippy::too_many_arguments)]
fn native_daily_use_config(
    http: SocketAddr,
    socks: SocketAddr,
    tcp_forward: SocketAddr,
    udp_forward: SocketAddr,
    management: SocketAddr,
    direct_tcp_target: SocketAddr,
    direct_udp_target: SocketAddr,
    http_proxy: SocketAddr,
) -> String {
    format!(
        r#"
[logging]
level = "error"

[management]
listen = ["{management}"]
token = {{ from = "file", path = "operator-token.key" }}
dashboard = false
allow_peer_diagnostics = false

[dns]
default = "local"

[[dns.servers]]
name = "system"
protocol = "system"

[[dns.policies]]
name = "local"
servers = ["system"]
family = "ipv4-only"
override_records = ["local-target"]

[[dns.override_records]]
name = "local-target"
domain = "{LOCAL_DNS_TARGET}"
addresses = ["127.0.0.1"]

[[inbounds]]
name = "local-http"
protocol = "http-connect"
listen = ["{http}"]

[[inbounds]]
name = "local-socks"
protocol = "socks5"
listen = ["{socks}"]

[[inbounds]]
name = "local-tcp-forward"
protocol = "tcp-forward"
listen = ["{tcp_forward}"]
target = "{DELEGATED_HTTP_TARGET}"
max_connections = 4

[[inbounds]]
name = "local-udp-forward"
protocol = "udp-forward"
listen = ["{udp_forward}"]
target = "{direct_udp_target}"
max_associations = 4
idle_timeout_s = 5
datagram_ttl_s = 3

[[outbounds]]
name = "direct-egress"
protocol = "direct"

[[outbounds]]
name = "http-egress"
protocol = "http-connect"
endpoint = "{http_proxy}"
connect_timeout_s = 2

[routing]

[[routing.rules]]
name = "tcp-forward-through-http"
inbounds = ["local-tcp-forward"]
domain_exact = ["remote-resolution.invalid"]
destination_ports = [443]
networks = ["tcp"]
outbound = "http-egress"

[[routing.rules]]
name = "allow-loopback-test-targets"
destination_cidrs = ["127.0.0.1/32"]
destination_ports = [{direct_tcp_port}, {direct_udp_port}]
networks = ["tcp", "udp"]
decision = "allow-restricted"
outbound = "direct-egress"

[[routing.rules]]
name = "default-direct"
outbound = "direct-egress"
"#,
        direct_tcp_port = direct_tcp_target.port(),
        direct_udp_port = direct_udp_target.port(),
    )
}

fn assert_check_config_ok(path: &Path) {
    let output = check_config(path);
    assert!(
        output.status.success(),
        "packaged config check failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn parse_json_log(contents: &str) -> Vec<serde_json::Value> {
    contents
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).expect("valid newline-delimited JSON log record"))
        .collect()
}

#[test]
fn packaged_no_args_restart_loads_the_edited_default_config() {
    let _network = network_test_guard();
    let directory = TestDirectory::new("default-config-restart");
    directory.write("operator-token.key", OPERATOR_TOKEN);
    let original_socks = unused_loopback_addr();
    let management = unused_loopback_addr();
    let config_path = directory.write(
        "config.toml",
        &reject_runtime_config(&[original_socks], management, 1),
    );

    let mut first = MptunnelProcess::spawn_without_args(
        directory.path(),
        directory.path().join("first.stderr"),
    );
    wait_for_tcp(
        &mut first,
        original_socks,
        PROCESS_START_TIMEOUT,
        "original no-args SOCKS5 listener",
    );
    first.stop();

    // Allocate the future listener only after the first process has stopped.
    // On Darwin, a released port-0 reservation can otherwise be selected as
    // an ephemeral source port by the readiness probe before the restart.
    let replacement_socks = unused_loopback_addr();
    fs::write(
        &config_path,
        reject_runtime_config(&[replacement_socks], management, 2),
    )
    .expect("edit default config between packaged process runs");

    let mut restarted = MptunnelProcess::spawn_without_args(
        directory.path(),
        directory.path().join("restarted.stderr"),
    );
    wait_for_tcp(
        &mut restarted,
        replacement_socks,
        PROCESS_START_TIMEOUT,
        "edited no-args SOCKS5 listener",
    );
    wait_for_tcp_closed(
        &mut restarted,
        original_socks,
        PROCESS_START_TIMEOUT,
        "stale no-args SOCKS5 listener",
    );
    let (_stream, reply) = socks5_connect(
        replacement_socks,
        SocksTarget::Domain("blocked.example", 443),
    )
    .expect("edited listener serves the restarted process");
    assert_eq!(reply, 0x02, "edited reject route must be active");
}

#[test]
fn packaged_management_api_validates_applies_persists_and_reloads_one_generation() {
    let _network = network_test_guard();
    let directory = TestDirectory::new("config-api");
    directory.write("operator-token.key", OPERATOR_TOKEN);
    let retained_socks = unused_loopback_addr();
    let retired_socks = unused_loopback_addr();
    let management = unused_loopback_addr();
    let original = reject_runtime_config(&[retained_socks, retired_socks], management, 1);
    let candidate = reject_runtime_config(&[retained_socks], management, 2)
        .replacen("dashboard = false", "dashboard = true", 1)
        .replacen(
            "[logging]\nlevel = \"warn\"",
            "[logging]\nlevel = \"info\"\nformat = \"json\"\nconsole = false\nfile = \"applied-runtime.jsonl\"",
            1,
        );
    let applied_log_path = directory.path().join("applied-runtime.jsonl");
    let config_path = directory.write("config.toml", &original);
    assert_check_config_ok(&config_path);

    let unknown = original.replacen("[management]", "[management]\nlegacy_unsafe_api = true", 1);
    let unknown_path = directory.write("unknown.toml", &unknown);
    let output = check_config(&unknown_path);
    assert!(!output.status.success(), "unknown TOML field was accepted");
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        error.contains("legacy_unsafe_api") && error.contains("unknown field"),
        "strict TOML failure did not identify the unknown field:\n{error}"
    );
    let toml_canary = "packaged-toml-value-canary";
    let invalid_value = original.replacen(
        "token = { from = \"file\", path = \"operator-token.key\" }",
        &format!("token = \"{toml_canary}\""),
        1,
    );
    let invalid_value_path = directory.write("invalid-value.toml", &invalid_value);
    let output = check_config(&invalid_value_path);
    assert!(!output.status.success(), "invalid TOML value was accepted");
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("configuration document is invalid"));
    assert!(error.contains("line") && error.contains("column"));
    assert!(error.contains(env!("CARGO_PKG_VERSION")));
    assert!(
        !error.contains(toml_canary),
        "fatal log leaked TOML value: {error}"
    );
    assert_eq!(
        error.lines().count(),
        1,
        "fatal error must be one log record"
    );
    assert!(
        !applied_log_path.exists(),
        "validation must not create a candidate log file"
    );

    let mut process =
        MptunnelProcess::spawn(&config_path, directory.path().join("mptunnel.stderr"));
    wait_for_ready_management(
        &mut process,
        management,
        OPERATOR_TOKEN,
        PROCESS_START_TIMEOUT,
    );
    wait_for_tcp(
        &mut process,
        retained_socks,
        PROCESS_START_TIMEOUT,
        "retained SOCKS5 listener",
    );
    wait_for_tcp(
        &mut process,
        retired_socks,
        PROCESS_START_TIMEOUT,
        "pre-reload SOCKS5 listener",
    );

    let unauthorized_health = http_request(management, "GET", "/api/v4/health", None, &[], &[])
        .expect("unauthenticated health response");
    assert_eq!(unauthorized_health.status, 401);
    let removed_legacy_health = http_request(
        management,
        "GET",
        "/api/health",
        Some(OPERATOR_TOKEN),
        &[],
        &[],
    )
    .expect("removed legacy health response");
    assert_eq!(removed_legacy_health.status, 404);
    let health = http_request(
        management,
        "GET",
        "/api/v4/health/ready",
        Some(OPERATOR_TOKEN),
        &[],
        &[],
    )
    .expect("authenticated readiness response");
    assert_eq!(health.status, 200);
    assert_eq!(health.json()["ready"], true);

    let unauthorized = http_request(management, "GET", "/api/v4/config", None, &[], &[])
        .expect("unauthenticated config status");
    assert_eq!(unauthorized.status, 401);

    let status = http_request(
        management,
        "GET",
        "/api/v4/config",
        Some(OPERATOR_TOKEN),
        &[],
        &[],
    )
    .expect("authenticated config status");
    assert_eq!(status.status, 200);
    let initial = status.json();
    let active_revision = initial["active_revision"]
        .as_str()
        .expect("active revision")
        .to_string();
    assert_eq!(initial["desired_revision"], initial["active_revision"]);
    assert_eq!(initial["runtime_revision"], initial["active_revision"]);
    assert!(initial["pending_revision"].is_null());

    let invalid_candidate = format!("unknown_daily_use_option = true\n{candidate}");
    let invalid = http_request(
        management,
        "POST",
        "/api/v4/config/validate",
        Some(OPERATOR_TOKEN),
        &[("Content-Type", "application/toml")],
        invalid_candidate.as_bytes(),
    )
    .expect("invalid candidate validation");
    assert_eq!(invalid.status, 422);
    assert!(
        String::from_utf8_lossy(&invalid.body).contains("unknown_daily_use_option"),
        "validation response should identify the invalid field"
    );
    fs::hard_link(&config_path, directory.path().join("config-hardlink.toml"))
        .expect("create canonical-config hard link");
    for store_owned_path in [
        "config.toml",
        "config.toml.mptunnel.last-good",
        "config.toml.mptunnel.pending",
        "config-hardlink.toml",
    ] {
        let store_collision = original.replacen(
            "[logging]\nlevel = \"warn\"",
            &format!("[logging]\nlevel = \"info\"\nconsole = false\nfile = {store_owned_path:?}"),
            1,
        );
        let collision = http_request(
            management,
            "POST",
            "/api/v4/config/validate",
            Some(OPERATOR_TOKEN),
            &[("Content-Type", "application/toml")],
            store_collision.as_bytes(),
        )
        .expect("store-owned log path validation");
        assert_eq!(collision.status, 422);
        assert!(
            String::from_utf8_lossy(&collision.body).contains("canonical configuration store"),
            "store-owned logging path rejection should be explicit for {store_owned_path}"
        );
    }

    let validated = http_request(
        management,
        "POST",
        "/api/v4/config/validate",
        Some(OPERATOR_TOKEN),
        &[("Content-Type", "application/toml")],
        candidate.as_bytes(),
    )
    .expect("candidate validation");
    assert_eq!(validated.status, 200);
    let candidate_revision = validated.json()["revision"]
        .as_str()
        .expect("candidate revision")
        .to_string();

    let missing_precondition = http_request(
        management,
        "POST",
        "/api/v4/config/apply",
        Some(OPERATOR_TOKEN),
        &[("Content-Type", "application/toml")],
        candidate.as_bytes(),
    )
    .expect("apply without precondition");
    assert_eq!(missing_precondition.status, 428);

    let stale = http_request(
        management,
        "POST",
        "/api/v4/config/apply",
        Some(OPERATOR_TOKEN),
        &[
            ("Content-Type", "application/toml"),
            (
                "If-Match",
                "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            ),
        ],
        candidate.as_bytes(),
    )
    .expect("stale apply");
    assert_eq!(stale.status, 412);
    assert!(
        !applied_log_path.exists(),
        "a stale apply must not create or open the candidate log file"
    );

    let applied = http_request(
        management,
        "POST",
        "/api/v4/config/apply",
        Some(OPERATOR_TOKEN),
        &[
            ("Content-Type", "application/toml"),
            ("If-Match", &active_revision),
        ],
        candidate.as_bytes(),
    )
    .expect("candidate apply");
    assert_eq!(applied.status, 202);
    let applied = applied.json();
    assert_eq!(applied["desired_revision"], candidate_revision);
    assert_eq!(applied["active_revision"], active_revision);
    assert_eq!(applied["pending_revision"], candidate_revision);
    assert_eq!(applied["activation"], "pending-generation-reload");

    wait_for_ready_management(
        &mut process,
        management,
        OPERATOR_TOKEN,
        PROCESS_START_TIMEOUT,
    );
    let deadline = Instant::now() + PROCESS_START_TIMEOUT;
    loop {
        process.assert_running("configuration activation");
        if let Ok(response) = http_request(
            management,
            "GET",
            "/api/v4/config",
            Some(OPERATOR_TOKEN),
            &[],
            &[],
        ) && response.status == 200
        {
            let status = response.json();
            if status["desired_revision"] == candidate_revision
                && status["active_revision"] == candidate_revision
                && status["runtime_revision"] == candidate_revision
                && status["pending_revision"].is_null()
            {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "candidate generation never became active; stderr:\n{}",
            process.log()
        );
        thread::sleep(Duration::from_millis(25));
    }

    wait_for_tcp(
        &mut process,
        retained_socks,
        PROCESS_START_TIMEOUT,
        "reloaded SOCKS5 listener",
    );
    wait_for_tcp_closed(
        &mut process,
        retired_socks,
        PROCESS_START_TIMEOUT,
        "retired SOCKS5 listener",
    );
    let (_stream, reply) =
        socks5_connect(retained_socks, SocksTarget::Domain("blocked.example", 443))
            .expect("reloaded SOCKS5 reject response");
    assert_eq!(
        reply, 0x02,
        "reject route must return connection-not-allowed"
    );
    let dashboard =
        http_request(management, "GET", "/", None, &[], &[]).expect("reloaded dashboard response");
    assert_eq!(dashboard.status, 200);
    assert!(
        String::from_utf8_lossy(&dashboard.body).contains("<title>mptunnel dashboard</title>"),
        "candidate dashboard setting must become observable after activation"
    );
    assert_eq!(
        fs::read_to_string(&config_path).expect("persisted canonical config"),
        candidate
    );
    let deadline = Instant::now() + PROCESS_START_TIMEOUT;
    loop {
        let contents = fs::read_to_string(&applied_log_path).unwrap_or_default();
        if contents.lines().any(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .is_ok_and(|record| record["event"] == "generation_activated")
        }) {
            break;
        }
        process.assert_running("runtime-applied logging configuration");
        assert!(
            Instant::now() < deadline,
            "runtime-applied file logger did not record activation:\n{contents}"
        );
        thread::sleep(Duration::from_millis(25));
    }

    let unusable_logging_candidate = candidate.replacen(
        "file = \"applied-runtime.jsonl\"",
        "file = \"missing-log-directory/mptunnel.jsonl\"",
        1,
    );
    let validated = http_request(
        management,
        "POST",
        "/api/v4/config/validate",
        Some(OPERATOR_TOKEN),
        &[("Content-Type", "application/toml")],
        unusable_logging_candidate.as_bytes(),
    )
    .expect("unusable logging candidate structural validation");
    assert_eq!(
        validated.status, 200,
        "side-effect-free validation should not probe or create the file sink"
    );
    let applied = http_request(
        management,
        "POST",
        "/api/v4/config/apply",
        Some(OPERATOR_TOKEN),
        &[
            ("Content-Type", "application/toml"),
            ("If-Match", &candidate_revision),
        ],
        unusable_logging_candidate.as_bytes(),
    )
    .expect("unusable logging candidate apply");
    assert_eq!(
        applied.status, 422,
        "an unusable live logging update must roll back without a reload"
    );
    assert_eq!(
        fs::read_to_string(&config_path).expect("preserved configuration"),
        candidate
    );
    let status = http_request(
        management,
        "GET",
        "/api/v4/config",
        Some(OPERATOR_TOKEN),
        &[],
        &[],
    )
    .expect("configuration status after rejected logging update")
    .json();
    assert_eq!(status["desired_revision"], candidate_revision);
    assert_eq!(status["active_revision"], candidate_revision);
    assert_eq!(status["runtime_revision"], candidate_revision);
    assert!(status["pending_revision"].is_null());

    let live_logging_candidate = candidate.replacen("console = false", "console = true", 1);
    let validated = http_request(
        management,
        "POST",
        "/api/v4/config/validate",
        Some(OPERATOR_TOKEN),
        &[("Content-Type", "application/toml")],
        live_logging_candidate.as_bytes(),
    )
    .expect("live logging candidate validation");
    assert_eq!(validated.status, 200);
    let live_revision = validated.json()["revision"]
        .as_str()
        .expect("live logging revision")
        .to_string();
    let applied = http_request(
        management,
        "POST",
        "/api/v4/config/apply",
        Some(OPERATOR_TOKEN),
        &[
            ("Content-Type", "application/toml"),
            ("If-Match", &candidate_revision),
        ],
        live_logging_candidate.as_bytes(),
    )
    .expect("live logging update");
    assert_eq!(
        applied.status,
        200,
        "logging-only apply was not live: body={}; client log:\n{}",
        String::from_utf8_lossy(&applied.body),
        process.log()
    );
    let applied = applied.json();
    assert_eq!(applied["activation"], "live-update");
    assert_eq!(applied["desired_revision"], live_revision);
    assert_eq!(applied["active_revision"], live_revision);
    assert!(applied["pending_revision"].is_null());
    process.assert_running("live logging update");
    wait_for_tcp(
        &mut process,
        retained_socks,
        PROCESS_START_TIMEOUT,
        "listener retained across live logging update",
    );
    assert_eq!(
        fs::read_to_string(&config_path).expect("live logging configuration"),
        live_logging_candidate
    );

    let rotated_log_path = directory.path().join("applied-runtime.rotated.jsonl");
    fs::rename(&applied_log_path, &rotated_log_path).expect("rotate active log descriptor");
    fs::create_dir(&applied_log_path).expect("make configured sink path unreopenable");
    let non_logging_candidate = live_logging_candidate.replacen(
        "name = \"default-reject-2\"",
        "name = \"default-reject-3\"",
        1,
    );
    let validated = http_request(
        management,
        "POST",
        "/api/v4/config/validate",
        Some(OPERATOR_TOKEN),
        &[("Content-Type", "application/toml")],
        non_logging_candidate.as_bytes(),
    )
    .expect("non-logging candidate validation");
    assert_eq!(validated.status, 200);
    let non_logging_revision = validated.json()["revision"]
        .as_str()
        .expect("non-logging candidate revision")
        .to_string();
    let applied = http_request(
        management,
        "POST",
        "/api/v4/config/apply",
        Some(OPERATOR_TOKEN),
        &[
            ("Content-Type", "application/toml"),
            ("If-Match", &live_revision),
        ],
        non_logging_candidate.as_bytes(),
    )
    .expect("non-logging update after host-owned log rotation");
    assert_eq!(applied.status, 202);
    assert_eq!(applied.json()["activation"], "pending-generation-reload");

    let deadline = Instant::now() + PROCESS_START_TIMEOUT;
    loop {
        process.assert_running("non-logging generation replacement");
        if let Ok(response) = http_request(
            management,
            "GET",
            "/api/v4/config",
            Some(OPERATOR_TOKEN),
            &[],
            &[],
        ) && response.status == 200
        {
            let status = response.json();
            if status["desired_revision"] == non_logging_revision
                && status["active_revision"] == non_logging_revision
                && status["runtime_revision"] == non_logging_revision
                && status["pending_revision"].is_null()
            {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "non-logging generation never became active; stderr:\n{}",
            process.log()
        );
        thread::sleep(Duration::from_millis(25));
    }
    assert_eq!(
        fs::read_to_string(&config_path).expect("non-logging configuration"),
        non_logging_candidate
    );
    wait_for_tcp(
        &mut process,
        retained_socks,
        PROCESS_START_TIMEOUT,
        "listener retained after non-logging generation replacement",
    );
    let rotated_contents =
        fs::read_to_string(&rotated_log_path).expect("read host-rotated active log");
    assert!(
        rotated_contents.lines().any(|line| {
            serde_json::from_str::<serde_json::Value>(line).is_ok_and(|record| {
                record["event"] == "generation_activated"
                    && record["message"]
                        .as_str()
                        .is_some_and(|message| message.contains(&non_logging_revision))
            })
        }),
        "an unchanged live logger must remain installed across an unrelated generation reload:\n{rotated_contents}"
    );
}

#[test]
fn packaged_daily_use_ingresses_reach_configured_native_egress() {
    let _network = network_test_guard();
    let directory = TestDirectory::new("daily-use-ingresses");
    let http = unused_loopback_addr();
    let socks = unused_loopback_addr();
    let tcp_forward = unused_loopback_addr();
    let udp_forward = unused_loopback_udp_addr();
    let management = unused_loopback_addr();
    let (direct_tcp_target, direct_tcp_task) = spawn_tcp_echo();
    let (direct_udp_target, direct_udp_task) = spawn_udp_echo_packets(2);
    let (http_proxy, http_proxy_task) = spawn_echo_http_connect_proxy(DELEGATED_HTTP_TARGET);
    directory.write("operator-token.key", OPERATOR_TOKEN);
    let config_path = directory.write(
        "native-daily-use.toml",
        &native_daily_use_config(
            http,
            socks,
            tcp_forward,
            udp_forward,
            management,
            direct_tcp_target,
            direct_udp_target,
            http_proxy,
        ),
    );
    assert_check_config_ok(&config_path);

    let mut process = MptunnelProcess::spawn(
        &config_path,
        directory.path().join("native-daily-use.stderr"),
    );
    wait_for_ready_management(
        &mut process,
        management,
        OPERATOR_TOKEN,
        PROCESS_START_TIMEOUT,
    );

    enum TcpIngress<'a> {
        HttpConnect {
            listener: SocketAddr,
            authority: &'a str,
        },
        FixedForward {
            listener: SocketAddr,
        },
    }
    let local_authority = format!("{LOCAL_DNS_TARGET}:{}", direct_tcp_target.port());
    let tcp_cases = [
        (
            "HTTP CONNECT to direct TCP with local DNS",
            TcpIngress::HttpConnect {
                listener: http,
                authority: local_authority.as_str(),
            },
        ),
        (
            "fixed TCP forward through HTTP CONNECT",
            TcpIngress::FixedForward {
                listener: tcp_forward,
            },
        ),
    ];
    for (name, case) in tcp_cases {
        let mut stream = match case {
            TcpIngress::HttpConnect {
                listener,
                authority,
            } => {
                let (stream, status) = http_connect(listener, authority)
                    .unwrap_or_else(|error| panic!("{name} failed to open: {error}"));
                assert_eq!(status, 200, "{name} returned HTTP status {status}");
                stream
            }
            TcpIngress::FixedForward { listener } => {
                TcpStream::connect_timeout(&listener, Duration::from_millis(500))
                    .unwrap_or_else(|error| panic!("{name} failed to open: {error}"))
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("set packaged TCP case read timeout");
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .expect("set packaged TCP case write timeout");
        stream
            .write_all(b"ping")
            .unwrap_or_else(|error| panic!("{name} request failed: {error}"));
        let mut response = [0_u8; 4];
        stream
            .read_exact(&mut response)
            .unwrap_or_else(|error| panic!("{name} response failed: {error}"));
        assert_eq!(&response, b"pong", "{name} returned the wrong payload");
    }

    enum UdpIngress {
        Socks5Associate {
            listener: SocketAddr,
            target: SocketAddr,
        },
        FixedForward {
            listener: SocketAddr,
        },
    }
    let udp_cases = [
        (
            "SOCKS5 UDP ASSOCIATE to direct UDP",
            UdpIngress::Socks5Associate {
                listener: socks,
                target: direct_udp_target,
            },
        ),
        (
            "fixed UDP forward to direct UDP",
            UdpIngress::FixedForward {
                listener: udp_forward,
            },
        ),
    ];
    for (name, case) in udp_cases {
        let result = match case {
            UdpIngress::Socks5Associate { listener, target } => socks5_udp_round_trip(
                listener,
                SocksTarget::Ipv4(Ipv4Addr::LOCALHOST, target.port()),
                b"ping",
                b"pong",
            ),
            UdpIngress::FixedForward { listener } => udp_round_trip(listener, b"ping", b"pong"),
        };
        result.unwrap_or_else(|error| panic!("{name} failed: {error}"));
    }

    process.assert_running("completed packaged daily-use ingress acceptance");
    join_thread(direct_tcp_task, "daily-use direct TCP target");
    join_thread(http_proxy_task, "daily-use HTTP CONNECT proxy");
    join_thread(direct_udp_task, "daily-use direct UDP target");
}

fn write_test_tls_material(directory: &TestDirectory) -> (std::path::PathBuf, std::path::PathBuf) {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["mptunnel.test".to_string()])
            .expect("generate acceptance-test TLS identity");
    (
        directory.write("server-cert.pem", &cert.pem()),
        directory.write("server-key.pem", &signing_key.serialize_pem()),
    )
}

fn toml_path(path: &Path) -> String {
    path.to_string_lossy()
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
}

fn mpp_server_config(
    mpp: SocketAddr,
    direct_target: SocketAddr,
    certificate: &Path,
    private_key: &Path,
) -> String {
    let certificate = toml_path(certificate);
    let private_key = toml_path(private_key);
    format!(
        r#"
[logging]
level = "error"

[[credentials]]
credential_id = "daily-use"
principal_id = "daily-use"
secret = {{ from = "file", path = "mpp-credential.key" }}

[[inbounds]]
name = "mpp-server"
protocol = "mpp"
paths = [{{ name = "path-1", endpoint = "tcp://{mpp}" }}]

[inbounds.security]
credential_ids = ["daily-use"]
tls_certificate_chain = {{ from = "file", path = "{certificate}" }}
tls_private_key = {{ from = "file", path = "{private_key}" }}

[[outbounds]]
name = "direct-egress"
protocol = "direct"

[routing]

[[routing.rules]]
name = "allow-loopback-test-target"
inbounds = ["mpp-server"]
principal_ids = ["daily-use"]
destination_cidrs = ["127.0.0.1/32"]
destination_ports = [{direct_target_port}]
networks = ["tcp"]
decision = "allow-restricted"
outbound = "direct-egress"
"#,
        direct_target_port = direct_target.port(),
    )
}

fn routing_client_config(
    socks: SocketAddr,
    management: SocketAddr,
    mpp: SocketAddr,
    failing_proxy: SocketAddr,
    working_proxy: SocketAddr,
    certificate: &Path,
) -> String {
    let certificate = toml_path(certificate);
    format!(
        r#"
[logging]
level = "debug"
format = "json"
console = false
file = "product-flows.jsonl"
flow_events = true

[management]
listen = ["{management}"]
token = {{ from = "file", path = "operator-token.key" }}

[[credentials]]
credential_id = "daily-use"
principal_id = "daily-use"
secret = {{ from = "file", path = "mpp-credential.key" }}

[[inbounds]]
name = "local-socks"
protocol = "socks5"
listen = ["{socks}"]

[[outbounds]]
name = "failed-proxy"
protocol = "socks5"
endpoint = "{failing_proxy}"
connect_timeout_s = 0.5

[[outbounds]]
name = "working-proxy"
protocol = "socks5"
endpoint = "{working_proxy}"
connect_timeout_s = 0.5

[[outbounds]]
name = "edge-mpp"
protocol = "mpp"
paths = [{{ name = "path-1", endpoint = "tcp://{mpp}" }}]
path_probe_interval_s = 0.1
path_probe_timeout_s = 1

[outbounds.security]
credential_id = "daily-use"
tls_server_name = "mptunnel.test"
tls_pinned_certificate = {{ from = "file", path = "{certificate}" }}

[[outbounds]]
name = "direct-default"
protocol = "direct"

[routing]

[[routing.balancers]]
name = "native-failover"
strategy = "ordered-failover"
members = [
  {{ outbound = "failed-proxy" }},
  {{ outbound = "working-proxy" }},
]

[routing.balancers.health]
failure_threshold = 1
recovery_threshold = 1
initial_backoff_s = 0.1
maximum_backoff_s = 0.1

[[routing.rules]]
name = "reject-blocked"
domain_exact = ["blocked.example"]
decision = "reject"

[[routing.rules]]
name = "native-proxy"
destination_cidrs = ["8.8.8.8/32"]
balancer = "native-failover"

[[routing.rules]]
name = "mpp-local-service"
domain_exact = ["localhost"]
networks = ["tcp"]
decision = "allow-restricted"
outbound = "edge-mpp"

[[routing.rules]]
name = "direct-default"
outbound = "direct-default"
"#
    )
}

fn wait_for_mpp_outbound_availability(
    process: &mut MptunnelProcess,
    management: SocketAddr,
    expected_available: bool,
    context: &str,
) {
    let deadline = Instant::now() + PROCESS_START_TIMEOUT;
    loop {
        process.assert_running(context);
        if let Ok(response) = http_request(
            management,
            "GET",
            "/api/v4/sessions",
            Some(OPERATOR_TOKEN),
            &[],
            &[],
        ) && response.status == 200
        {
            let sessions = response.json();
            let count = sessions["sessions"]
                .as_array()
                .and_then(|sessions| {
                    sessions.iter().find(|session| {
                        session["service"] == "mpp_outbound"
                            && session["service_name"] == "edge-mpp"
                    })
                })
                .and_then(|session| session["carrier_count"].as_u64())
                .and_then(|count| usize::try_from(count).ok());
            if count.is_some_and(|count| (count > 0) == expected_available) {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {context}; client stderr:\n{}",
            process.log()
        );
        thread::sleep(Duration::from_millis(25));
    }
}

#[test]
fn packaged_mpp_outage_rejects_new_flows_and_client_server_restarts_recover() {
    let _network = network_test_guard();
    let directory = TestDirectory::new("mpp-restart");
    let mpp = unused_loopback_addr();
    let socks = unused_loopback_addr();
    let management = unused_loopback_addr();
    let failed_proxy = unused_loopback_addr();
    let unused_proxy = unused_loopback_addr();
    let (direct_target, direct_task) = spawn_tcp_echo_connections(3);
    let (certificate, private_key) = write_test_tls_material(&directory);
    directory.write("mpp-credential.key", "0123456789abcdef0123456789abcdef");
    directory.write("operator-token.key", OPERATOR_TOKEN);
    let server_path = directory.write(
        "restart-server.toml",
        &mpp_server_config(mpp, direct_target, &certificate, &private_key),
    );
    let client_path = directory.write(
        "restart-client.toml",
        &routing_client_config(
            socks,
            management,
            mpp,
            failed_proxy,
            unused_proxy,
            &certificate,
        ),
    );
    assert_check_config_ok(&server_path);
    assert_check_config_ok(&client_path);

    let mut server = MptunnelProcess::spawn(
        &server_path,
        directory.path().join("restart-server-1.stderr"),
    );
    wait_for_tcp(
        &mut server,
        mpp,
        PROCESS_START_TIMEOUT,
        "initial MPP server listener",
    );
    let mut client = MptunnelProcess::spawn(
        &client_path,
        directory.path().join("restart-client-1.stderr"),
    );
    wait_for_tcp(
        &mut client,
        socks,
        PROCESS_START_TIMEOUT,
        "initial client SOCKS5 listener",
    );
    wait_for_ready_management(
        &mut client,
        management,
        OPERATOR_TOKEN,
        PROCESS_START_TIMEOUT,
    );
    wait_for_mpp_outbound_availability(
        &mut client,
        management,
        true,
        "initial authenticated carrier",
    );
    socks5_round_trip(
        socks,
        SocksTarget::Domain("localhost", direct_target.port()),
        b"ping",
        b"pong",
    )
    .expect("initial MPP Product flow");

    server.stop();
    wait_for_mpp_outbound_availability(
        &mut client,
        management,
        false,
        "authenticated carrier withdrawal after server stop",
    );
    let (_stream, unavailable) = socks5_connect(
        socks,
        SocksTarget::Domain("localhost", direct_target.port()),
    )
    .expect("offline SOCKS5 response");
    assert_eq!(
        unavailable, 0x03,
        "an offline MPP outbound must report network unreachable"
    );

    server = MptunnelProcess::spawn(
        &server_path,
        directory.path().join("restart-server-2.stderr"),
    );
    wait_for_tcp(
        &mut server,
        mpp,
        PROCESS_START_TIMEOUT,
        "restarted MPP server listener",
    );
    wait_for_mpp_outbound_availability(
        &mut client,
        management,
        true,
        "authenticated carrier after server restart",
    );
    socks5_round_trip(
        socks,
        SocksTarget::Domain("localhost", direct_target.port()),
        b"ping",
        b"pong",
    )
    .expect("Product flow after server restart");

    client.stop();
    client = MptunnelProcess::spawn(
        &client_path,
        directory.path().join("restart-client-2.stderr"),
    );
    wait_for_tcp(
        &mut client,
        socks,
        PROCESS_START_TIMEOUT,
        "restarted client SOCKS5 listener",
    );
    wait_for_ready_management(
        &mut client,
        management,
        OPERATOR_TOKEN,
        PROCESS_START_TIMEOUT,
    );
    wait_for_mpp_outbound_availability(
        &mut client,
        management,
        true,
        "authenticated carrier after client restart",
    );
    socks5_round_trip(
        socks,
        SocksTarget::Domain("localhost", direct_target.port()),
        b"ping",
        b"pong",
    )
    .expect("Product flow after client restart");

    client.assert_running("completed client restart acceptance");
    server.assert_running("completed server restart acceptance");
    join_thread(direct_task, "restart echo target");
}

#[test]
fn packaged_routing_exercises_reject_proxy_failover_mpp_and_direct_egress() {
    let _network = network_test_guard();
    let directory = TestDirectory::new("routing");
    let mpp = unused_loopback_addr();
    let socks = unused_loopback_addr();
    let management = unused_loopback_addr();
    let (failing_proxy, failing_task) = spawn_blackhole_proxy();
    let proxy_target = Ipv4Addr::new(8, 8, 8, 8);
    let (working_proxy, working_task) = spawn_echo_socks5_proxy(proxy_target, 443);
    let (direct_target, direct_task) = spawn_tcp_echo();
    let (certificate, private_key) = write_test_tls_material(&directory);
    directory.write("mpp-credential.key", "0123456789abcdef0123456789abcdef");
    directory.write("operator-token.key", OPERATOR_TOKEN);

    let server_path = directory.write(
        "server.toml",
        &mpp_server_config(mpp, direct_target, &certificate, &private_key),
    );
    let client_config = routing_client_config(
        socks,
        management,
        mpp,
        failing_proxy,
        working_proxy,
        &certificate,
    );
    let client_path = directory.write("client.toml", &client_config);
    let flow_log_path = directory.path().join("product-flows.jsonl");
    let next_flow_log_path = directory.path().join("product-flows-next.jsonl");
    assert_check_config_ok(&server_path);
    assert_check_config_ok(&client_path);
    assert!(
        !flow_log_path.exists(),
        "check-only validation must not create a configured log file"
    );

    let mut server = MptunnelProcess::spawn(&server_path, directory.path().join("server.stderr"));
    wait_for_tcp(
        &mut server,
        mpp,
        PROCESS_START_TIMEOUT,
        "MPP server listener",
    );
    let mut client = MptunnelProcess::spawn(&client_path, directory.path().join("client.stderr"));
    wait_for_tcp(
        &mut client,
        socks,
        PROCESS_START_TIMEOUT,
        "client SOCKS5 listener",
    );
    wait_for_ready_management(
        &mut client,
        management,
        OPERATOR_TOKEN,
        PROCESS_START_TIMEOUT,
    );

    let (_stream, rejected) = socks5_connect(socks, SocksTarget::Domain("blocked.example", 443))
        .expect("policy reject response");
    assert_eq!(rejected, 0x02);

    let (mut proxy_stream, proxy_reply) =
        socks5_connect(socks, SocksTarget::Ipv4(proxy_target, 443))
            .expect("ordered proxy failover connection");
    assert_eq!(proxy_reply, 0x00);
    let status = http_request(
        management,
        "GET",
        "/api/v4/config",
        Some(OPERATOR_TOKEN),
        &[],
        &[],
    )
    .expect("routing client config status")
    .json();
    let active_revision = status["active_revision"]
        .as_str()
        .expect("routing client active revision");
    let next_client_config = client_config.replacen(
        "file = \"product-flows.jsonl\"",
        "file = \"product-flows-next.jsonl\"",
        1,
    );
    let applied = http_request(
        management,
        "POST",
        "/api/v4/config/apply",
        Some(OPERATOR_TOKEN),
        &[
            ("Content-Type", "application/toml"),
            ("If-Match", active_revision),
        ],
        next_client_config.as_bytes(),
    )
    .expect("live flow-logger update");
    assert_eq!(
        applied.status,
        200,
        "flow-logger apply was not live: body={}; client log:\n{}",
        String::from_utf8_lossy(&applied.body),
        client.log()
    );
    assert_eq!(applied.json()["activation"], "live-update");
    proxy_stream
        .write_all(b"ping")
        .expect("send proxied payload after logging update");
    let mut proxy_response = [0_u8; 4];
    proxy_stream
        .read_exact(&mut proxy_response)
        .expect("receive proxied response after logging update");
    assert_eq!(&proxy_response, b"pong");
    drop(proxy_stream);
    join_thread(failing_task, "failing proxy");
    join_thread(working_task, "working proxy");

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let error = match socks5_round_trip(
            socks,
            SocksTarget::Domain("localhost", direct_target.port()),
            b"ping",
            b"pong",
        ) {
            Ok(()) => break,
            Err(error) => error,
        };
        client.assert_running("MPP client path establishment");
        server.assert_running("MPP server path establishment");
        assert!(
            Instant::now() < deadline,
            "MPP-to-direct route never became usable: {}; client stderr:\n{}\nserver stderr:\n{}",
            error,
            client.log(),
            server.log()
        );
        thread::sleep(Duration::from_millis(100));
    }
    join_thread(direct_task, "direct target");
    client.assert_running("completed routing acceptance");
    server.assert_running("completed routing acceptance");

    let startup_records =
        parse_json_log(&fs::read_to_string(&flow_log_path).expect("read initial process log"));
    assert!(
        startup_records.iter().any(|record| {
            record["component"] == "process"
                && record["event"] == "starting"
                && record["message"]
                    .as_str()
                    .is_some_and(|message| message.contains(env!("CARGO_PKG_VERSION")))
        }),
        "startup log must identify the running package version: {startup_records:#?}"
    );
    let client_config_path = client_path.to_string_lossy();
    let socks_listener = socks.to_string();
    for (component, event, detail) in [
        ("configuration", "loaded", client_config_path.as_ref()),
        ("outbound", "configured", "edge-mpp"),
        ("inbound", "listening", socks_listener.as_str()),
        ("process", "generation_ready", "runtime generation"),
    ] {
        assert!(
            startup_records.iter().any(|record| {
                record["component"] == component
                    && record["event"] == event
                    && record["message"]
                        .as_str()
                        .is_some_and(|message| message.contains(detail))
            }),
            "missing essential {component}.{event} lifecycle record: {startup_records:#?}"
        );
    }

    let deadline = Instant::now() + PROCESS_START_TIMEOUT;
    let (old_flow_records, new_flow_records, flow_records) = loop {
        client.assert_running("Product flow logging");
        let old_contents = fs::read_to_string(&flow_log_path).unwrap_or_default();
        let new_contents = fs::read_to_string(&next_flow_log_path).unwrap_or_default();
        let old_records = parse_json_log(&old_contents)
            .into_iter()
            .filter(|record| record["component"] == "flow")
            .collect::<Vec<_>>();
        let new_records = parse_json_log(&new_contents)
            .into_iter()
            .filter(|record| record["component"] == "flow")
            .collect::<Vec<_>>();
        let records = old_records
            .iter()
            .cloned()
            .chain(new_records.iter().cloned())
            .collect::<Vec<_>>();
        if records
            .iter()
            .filter(|record| record["event"] == "closed")
            .count()
            >= 2
        {
            break (old_records, new_records, records);
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for Product flow log records; old:\n{old_contents}\nnew:\n{new_contents}"
        );
        thread::sleep(Duration::from_millis(25));
    };
    assert!(
        new_flow_records
            .iter()
            .any(|record| record["event"] == "opened"),
        "new flows must use the live-updated logging sink: {new_flow_records:#?}"
    );
    let proxy_flow_id = old_flow_records
        .iter()
        .find(|record| {
            record["event"] == "opened"
                && record["target"] == "8.8.8.8:443"
                && record["outbound"] == "working-proxy"
                && record["balancer"] == "native-failover"
        })
        .and_then(|record| record["flow_id"].as_str())
        .expect("proxy-balancer flow-open record")
        .to_string();
    assert!(
        old_flow_records.iter().any(|record| {
            record["event"] == "closed"
                && record["flow_id"] == proxy_flow_id
                && record["outcome"] == "complete"
                && record["to_peer_bytes"]
                    .as_u64()
                    .is_some_and(|bytes| bytes >= 4)
                && record["from_peer_bytes"]
                    .as_u64()
                    .is_some_and(|bytes| bytes >= 4)
        }),
        "a flow opened before a live logging update must close in the same sink: {old_flow_records:#?}"
    );
    for record in &flow_records {
        let object = record.as_object().expect("flow record object");
        for forbidden in [
            "principal",
            "session_id",
            "display_id",
            "credential",
            "secret",
            "source_ip",
            "carrier",
        ] {
            assert!(
                !object.contains_key(forbidden),
                "flow record leaked forbidden field {forbidden}: {record}"
            );
        }
    }
    let flow_log = format!(
        "{}{}",
        fs::read_to_string(&flow_log_path).expect("read original Product flow log"),
        fs::read_to_string(&next_flow_log_path).expect("read updated Product flow log")
    );
    let debug_records = parse_json_log(&flow_log)
        .into_iter()
        .filter(|record| record["level"] == "debug")
        .collect::<Vec<_>>();
    let proxy_connection_id = debug_records
        .iter()
        .find(|record| {
            record["component"] == "balancer"
                && record["event"] == "selected"
                && record["outbound"] == "failed-proxy"
                && record["attempt"] == 1
        })
        .and_then(|record| record["connection_id"].as_str())
        .expect("first proxy-balancer debug attempt");
    let proxy_events = debug_records
        .iter()
        .filter(|record| record["connection_id"] == proxy_connection_id)
        .map(|record| {
            (
                record["component"].as_str().expect("debug component"),
                record["event"].as_str().expect("debug event"),
                record.get("outbound").and_then(serde_json::Value::as_str),
                record.get("attempt").and_then(serde_json::Value::as_u64),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        proxy_events,
        [
            ("inbound", "accepted", None, None),
            ("routing", "selected", None, None),
            ("balancer", "selected", Some("failed-proxy"), Some(1)),
            ("outbound", "connecting", Some("failed-proxy"), Some(1)),
            ("outbound", "failed", Some("failed-proxy"), Some(1)),
            ("balancer", "selected", Some("working-proxy"), Some(2)),
            ("outbound", "connecting", Some("working-proxy"), Some(2)),
            ("outbound", "connected", Some("working-proxy"), Some(2)),
            ("inbound", "established", None, None),
        ],
        "one stable connection ID must expose ordered inbound, routing, balancer-failover, and outbound states"
    );
    let mpp_connection_id = debug_records
        .iter()
        .find(|record| {
            record["component"] == "outbound"
                && record["event"] == "connected"
                && record["outbound"] == "edge-mpp"
                && record["network"] == "tcp"
                && record["attempt"] == 1
        })
        .and_then(|record| record["connection_id"].as_str())
        .expect("MPP outbound debug connection");
    for event in ["accepted", "established"] {
        assert!(
            debug_records.iter().any(|record| {
                record["connection_id"] == mpp_connection_id
                    && record["component"] == "inbound"
                    && record["event"] == event
            }),
            "MPP outbound state must correlate with inbound.{event}"
        );
    }
    assert!(
        !flow_log.contains("0123456789abcdef0123456789abcdef"),
        "Product flow log leaked credential material"
    );
}

fn split_dot_config() -> &'static str {
    r#"
[logging]
level = "error"

[dns]
default = "public"

[[dns.servers]]
name = "corp-dot"
protocol = "dot"
address = "9.9.9.9:853"
tls_name = "dns.quad9.net"

[[dns.servers]]
name = "public-dot"
protocol = "dot"
address = "1.1.1.1:853"
tls_name = "cloudflare-dns.com"

[[dns.policies]]
name = "corp"
servers = ["corp-dot"]
family = "ipv4-only"
security = "require-encrypted"
query = { timeout_s = 0.75 }

[[dns.policies]]
name = "public"
servers = ["public-dot"]
family = "ipv4-only"
security = "require-encrypted"
query = { timeout_s = 0.75 }

[[dns.rules]]
name = "corp-split"
suffix = "corp.example"
policy = "corp"
explanation = "private daily-use namespace"

[[inbounds]]
name = "local-socks"
protocol = "socks5"
listen = ["127.0.0.1:1080"]

[routing]

[[routing.rules]]
name = "default-reject"
decision = "reject"
"#
}

#[derive(Clone)]
struct StaticDnsBackend {
    address: IpAddr,
}

impl DnsQueryBackend for StaticDnsBackend {
    fn query(&self, question: DnsQuestion) -> DnsRecordBackendFuture {
        let address = self.address;
        Box::pin(async move {
            let mut message = Message::response(0, hickory_proto::op::OpCode::Query);
            let query = Query::query(
                Name::from_ascii(format!("{}.", question.domain())).expect("DNS name"),
                question.record_type(),
            );
            message.add_query(query.clone());
            let data = match (question.record_type(), address) {
                (RecordType::A, IpAddr::V4(address)) => RData::A(A(address)),
                (RecordType::AAAA, IpAddr::V6(address)) => {
                    RData::AAAA(hickory_proto::rr::rdata::AAAA(address))
                }
                _ => {
                    return Err(mptunnel::dns::DnsBackendError::NoRecords {
                        ttl: Some(Duration::from_secs(30)),
                    });
                }
            };
            message.add_answer(hickory_proto::rr::Record::from_rdata(
                query.name().clone(),
                30,
                data,
            ));
            Ok(DnsBackendResponse::new(
                message,
                Some(Duration::from_secs(30)),
            ))
        })
    }
}

struct StaticDnsFactory {
    answers: HashMap<String, IpAddr>,
}

impl DnsBackendFactory for StaticDnsFactory {
    fn build_backend(
        &self,
        _plan: &CompiledDnsPlan,
        upstream: &CompiledDnsUpstream,
    ) -> Result<Arc<dyn DnsQueryBackend>, DnsRuntimeError> {
        let address = self
            .answers
            .get(upstream.id().as_str())
            .copied()
            .ok_or_else(|| {
                DnsRuntimeError::PolicyInvariant(format!(
                    "acceptance backend missing for {}",
                    upstream.id()
                ))
            })?;
        Ok(Arc::new(StaticDnsBackend { address }))
    }
}

fn dns_wire_query(id: u16, name: &str) -> Vec<u8> {
    let mut request = Message::query();
    request.metadata.id = id;
    request.metadata.recursion_desired = true;
    request.add_query(Query::query(
        Name::from_ascii(name).expect("DNS query name"),
        RecordType::A,
    ));
    request.to_vec().expect("DNS wire query")
}

#[tokio::test(flavor = "current_thread")]
async fn strict_toml_drives_split_dot_selection_and_local_dns_capture_boundary() {
    let directory = TestDirectory::new("split-dns");
    let config_path = directory.write("split-dot.toml", split_dot_config());
    assert_check_config_ok(&config_path);

    let config = load_config_toml_str(split_dot_config()).expect("strict split-DoT config");
    let CommandConfig::Node(node) = config.command;
    let policy = Arc::new(node.dns_policy.compile().expect("compiled DNS policy"));
    assert!(policy.is_encrypted_only());
    assert!(!policy.uses_system_resolution());
    assert!(
        policy
            .plans()
            .all(|plan| plan.security() == DnsSecurityPolicy::RequireEncrypted)
    );
    let runtime = DnsGeneration::compile_with_factory(
        policy,
        &StaticDnsFactory {
            answers: HashMap::from([
                (
                    "corp-dot".to_string(),
                    "10.42.0.53".parse().expect("corp answer"),
                ),
                (
                    "public-dot".to_string(),
                    "198.51.100.53".parse().expect("public answer"),
                ),
            ]),
        },
    )
    .expect("split DNS generation");
    let generation = runtime.generation();

    let corp = runtime
        .resolve(&DomainName::parse("service.corp.example").expect("corp domain"))
        .await
        .expect("corp split lookup");
    assert_eq!(
        corp.addresses().as_ref(),
        &["10.42.0.53".parse::<IpAddr>().expect("corp answer")]
    );
    assert_eq!(corp.metadata().generation(), generation);
    assert_eq!(corp.metadata().plan().as_str(), "corp");
    assert_eq!(
        corp.metadata().rule().map(|rule| rule.as_str()),
        Some("corp-split")
    );
    assert_eq!(
        corp.metadata().explanation(),
        Some("private daily-use namespace")
    );

    let public = runtime
        .resolve(&DomainName::parse("www.example.net").expect("public domain"))
        .await
        .expect("public lookup");
    assert_eq!(
        public.addresses().as_ref(),
        &["198.51.100.53".parse::<IpAddr>().expect("public answer")]
    );
    assert_eq!(public.metadata().plan().as_str(), "public");
    assert!(public.metadata().rule().is_none());

    let response = runtime
        .answer_wire_query(
            &dns_wire_query(0x7711, "service.corp.example."),
            Duration::from_secs(45),
            1232,
        )
        .await
        .expect("captured DNS response");
    let response = Message::from_vec(&response).expect("decode captured DNS response");
    assert_eq!(response.metadata.id, 0x7711);
    assert_eq!(response.metadata.message_type, MessageType::Response);
    assert_eq!(response.metadata.response_code, ResponseCode::NoError);
    assert_eq!(response.answers.len(), 1);
    assert!(matches!(
        &response.answers[0].data,
        RData::A(A(address))
            if *address == "10.42.0.53".parse::<Ipv4Addr>().expect("corp IPv4")
    ));
}
