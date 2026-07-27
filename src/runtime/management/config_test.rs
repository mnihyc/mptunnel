use super::*;
use crate::config::{CanonicalConfigStore, ClientSecurityConfig, ResourceLimits, SharedSecret};
use crate::runtime::config_control::RuntimeConfigControl;
use crate::runtime::management::ProductRuntimeInventory;
use crate::runtime::management::http::health_response;
use crate::runtime::management::snapshot::ManagementState;
use crate::runtime::path::ClientPathContext;
use crate::runtime::readiness::RuntimeReadinessBarrier;
use crate::runtime::telemetry::RuntimeTelemetry;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const TEST_CREDENTIAL_FILE: &str = "edge-credential.key";
const TEST_MANAGEMENT_TOKEN_FILE: &str = "management-token.key";
const TEST_CERTIFICATE_FILE: &str = "edge-certificate.pem";

const CONFIG_A: &str = r#"
[[credentials]]
id = "edge-client"
principal = "edge-peer"
secret = { from = "file", path = "edge-credential.key" }

[[inbounds]]
tag = "local-socks"
protocol = "socks5"
listen = ["127.0.0.1:1080"]

[[outbounds]]
tag = "edge"
protocol = "mpp"
endpoints = ["udp://127.0.0.1:7443"]

[outbounds.security]
credential = "edge-client"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "edge-certificate.pem"

[routing]

[[routing.rules]]
id = "default"
action = "outbound"
target = "edge"
"#;

const CONFIG_B: &str = r#"
[[credentials]]
id = "edge-client"
principal = "edge-peer"
secret = { from = "file", path = "edge-credential.key" }

[[inbounds]]
tag = "local-socks"
protocol = "socks5"
listen = ["127.0.0.1:1081"]

[[outbounds]]
tag = "edge"
protocol = "mpp"
endpoints = ["udp://127.0.0.1:7443"]

[outbounds.security]
credential = "edge-client"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "edge-certificate.pem"

[routing]

[[routing.rules]]
id = "default"
action = "outbound"
target = "edge"
"#;

const CONFIG_WITH_CHANGED_MANAGEMENT: &str = r#"
[management]
listen = ["127.0.0.1:7600"]
token = { from = "file", path = "management-token.key" }

[[credentials]]
id = "edge-client"
principal = "edge-peer"
secret = { from = "file", path = "edge-credential.key" }

[[inbounds]]
tag = "local-socks"
protocol = "socks5"
listen = ["127.0.0.1:1081"]

[[outbounds]]
tag = "edge"
protocol = "mpp"
endpoints = ["udp://127.0.0.1:7443"]

[outbounds.security]
credential = "edge-client"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "edge-certificate.pem"

[routing]

[[routing.rules]]
id = "default"
action = "outbound"
target = "edge"
"#;

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mptunnel-management-config-test-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create isolated management config test directory");
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn target() -> (
    TestDirectory,
    PathBuf,
    ManagementTarget,
    RuntimeConfigControl,
) {
    target_from_document(CONFIG_A)
}

fn target_from_document(
    document: &str,
) -> (
    TestDirectory,
    PathBuf,
    ManagementTarget,
    RuntimeConfigControl,
) {
    let directory = TestDirectory::new();
    let path = directory.0.join("config.toml");
    fs::write(
        directory.0.join(TEST_CREDENTIAL_FILE),
        b"0123456789abcdef0123456789abcdef",
    )
    .expect("write referenced test credential");
    fs::write(
        directory.0.join(TEST_MANAGEMENT_TOKEN_FILE),
        b"different-operator-token",
    )
    .expect("write referenced management token");
    let rcgen::CertifiedKey { cert, .. } =
        rcgen::generate_simple_self_signed(vec!["mptunnel.test".to_string()])
            .expect("generate management config test certificate");
    fs::write(directory.0.join(TEST_CERTIFICATE_FILE), cert.pem())
        .expect("write management config test certificate");
    fs::write(&path, document).expect("write canonical config");
    let (store, _) = CanonicalConfigStore::open(path.clone()).expect("open canonical config");
    let control = RuntimeConfigControl::new(Arc::new(store));
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new(
        vec!["udp://127.0.0.1:7443".parse().expect("path")],
        security,
        ResourceLimits::default(),
    )
    .expect("client context");
    let target = ManagementTarget {
        clients: vec![context],
        servers: Vec::new(),
        inventory: ProductRuntimeInventory::default(),
        product_telemetry: RuntimeTelemetry::new(8),
        state: ManagementState::new("node"),
        config_control: Some(control.clone()),
        gateway_control: None,
        dns: None,
        product_admission: crate::product::ProductAdmission::default(),
        generation: control.generation(),
    };
    (directory, path, target, control)
}

#[test]
fn health_reports_desired_active_and_running_revisions() {
    let (_directory, _path, target, control) = target();
    let running = control.runtime_revision();
    RuntimeReadinessBarrier::new(control.generation()).seal();

    let (status, _, initial) = health_response(&target);
    assert_eq!(status, 200);
    assert_eq!(initial["desired_revision"], running.to_string());
    assert_eq!(initial["active_revision"], running.to_string());
    assert_eq!(initial["runtime_revision"], running.to_string());
    assert_eq!(initial["live"], true);
    assert_eq!(initial["ready"], false);
    assert_eq!(initial["degraded"], true);
    assert_eq!(
        initial["readiness_blockers"][0],
        "no-connected-mpp-outbound"
    );

    let outcome = target
        .apply_config_document(running, CONFIG_B.as_bytes())
        .expect("persist candidate");
    assert!(outcome.reload);
    let desired = control.store().revision();
    assert_ne!(desired, running);

    let (status, _, pending) = health_response(&target);
    assert_eq!(status, 200);
    assert_eq!(pending["desired_revision"], desired.to_string());
    assert_eq!(pending["active_revision"], running.to_string());
    assert_eq!(pending["runtime_revision"], running.to_string());
    assert!(
        pending["degraded_reasons"]
            .as_array()
            .expect("degraded reasons")
            .iter()
            .any(|reason| reason == "configuration-activation-pending")
    );
}

#[tokio::test]
async fn apply_is_cas_persistent_and_requests_a_generation_reload() {
    let (_directory, path, target, control) = target();
    let revision = control.store().revision();
    let status = target.config_status_json().expect("config status");
    assert_eq!(status["desired_revision"], revision.to_string());

    let outcome = target
        .apply_config_document(revision, CONFIG_B.as_bytes())
        .expect("apply valid candidate");
    assert!(outcome.reload);
    assert_eq!(outcome.response["state"], "persisted");
    assert_eq!(
        fs::read_to_string(path).expect("read committed config"),
        CONFIG_B
    );

    let error = target
        .apply_config_document(revision, CONFIG_A.as_bytes())
        .expect_err("stale revision must fail");
    assert_eq!(error.status, 412);

    let waiter_control = control.clone();
    let waiter = tokio::spawn(async move {
        waiter_control.wait_for_reload().await;
    });
    tokio::task::yield_now().await;
    target.request_config_reload();
    tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("reload notification")
        .expect("reload waiter task");
}

#[test]
fn api_cannot_replace_its_management_authentication_channel() {
    let (_directory, path, target, control) = target();
    let revision = control.store().revision();

    let error = target
        .apply_config_document(revision, CONFIG_WITH_CHANGED_MANAGEMENT.as_bytes())
        .expect_err("management lockout-sensitive fields require local restart");

    assert_eq!(error.status, 409);
    assert_eq!(
        fs::read_to_string(path).expect("read preserved config"),
        CONFIG_A
    );
}

#[test]
fn api_allows_generation_scoped_management_flags() {
    let active = format!(
        r#"
[management]
listen = ["127.0.0.1:7600"]
token = {{ from = "file", path = "{TEST_MANAGEMENT_TOKEN_FILE}" }}
dashboard = false
allow_peer_diagnostics = false
{CONFIG_A}
"#
    );
    let candidate = format!(
        r#"
[management]
listen = ["127.0.0.1:7600"]
token = {{ from = "file", path = "{TEST_MANAGEMENT_TOKEN_FILE}" }}
dashboard = true
allow_peer_diagnostics = true
{CONFIG_A}
"#
    );
    let (_directory, path, target, control) = target_from_document(&active);
    let revision = control.store().revision();

    let outcome = target
        .apply_config_document(revision, candidate.as_bytes())
        .expect("generation-scoped management flags may reload");

    assert!(outcome.reload);
    assert_eq!(outcome.response["state"], "persisted");
    assert_eq!(
        fs::read_to_string(path).expect("read committed config"),
        candidate
    );
}
