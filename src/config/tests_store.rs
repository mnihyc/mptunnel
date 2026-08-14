use super::{CanonicalConfigStore, ConfigRecoveryConflict, ConfigRevision, ConfigStoreError};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

const CONFIG_A: &str = r#"
[[credentials]]
credential_id = "home"
principal_id = "home"
secret = { from = "file", path = "credential.key" }

[[inbounds]]
name = "local-socks"
protocol = "socks5"
listen = ["127.0.0.1:1080"]

[[outbounds]]
name = "edge"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "quic://127.0.0.1:7443" }]

[outbounds.security]
credential_id = "home"
tls_server_name = "mptunnel.test"
tls_pinned_certificate = { from = "file", path = "certificate.pem" }

[routing]

[[routing.rules]]
name = "default"
action = "outbound"
outbound = "edge"
"#;

const CONFIG_B: &str = r#"
[[credentials]]
credential_id = "home"
principal_id = "home"
secret = { from = "file", path = "credential.key" }

[[inbounds]]
name = "local-socks"
protocol = "socks5"
listen = ["127.0.0.1:1081"]

[[outbounds]]
name = "edge"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "quic://127.0.0.1:7443" }]

[outbounds.security]
credential_id = "home"
tls_server_name = "mptunnel.test"
tls_pinned_certificate = { from = "file", path = "certificate.pem" }

[routing]

[[routing.rules]]
name = "default"
action = "outbound"
outbound = "edge"
"#;

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mptunnel-config-store-test-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create isolated config-store test directory");
        fs::write(
            path.join("credential.key"),
            b"0123456789abcdef0123456789abcdef",
        )
        .expect("write config-store test credential");
        let rcgen::CertifiedKey { cert, .. } =
            rcgen::generate_simple_self_signed(vec!["mptunnel.test".to_string()])
                .expect("generate config-store test certificate");
        fs::write(path.join("certificate.pem"), cert.pem())
            .expect("write config-store test certificate");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn open_store() -> (TestDirectory, PathBuf, CanonicalConfigStore) {
    let directory = TestDirectory::new();
    let path = directory.path().join("config.toml");
    fs::write(&path, CONFIG_A).expect("write initial config");
    let (store, _) = CanonicalConfigStore::open(path.clone()).expect("open config store");
    (directory, path, store)
}

#[test]
fn revision_round_trip_is_stable_and_content_addressed() {
    let revision = ConfigRevision::from_bytes(CONFIG_A.as_bytes());
    let rendered = revision.to_string();

    assert_eq!(rendered.parse::<ConfigRevision>(), Ok(revision));
    assert_ne!(revision, ConfigRevision::from_bytes(CONFIG_B.as_bytes()));
}

#[test]
fn config_store_error_stays_below_clippys_large_result_threshold() {
    assert!(
        std::mem::size_of::<ConfigStoreError>() <= 128,
        "ConfigStoreError grew to {} bytes",
        std::mem::size_of::<ConfigStoreError>()
    );
}

#[test]
fn boxed_recovery_conflict_preserves_structured_diagnostics() {
    let active = ConfigRevision::from_bytes(CONFIG_A.as_bytes());
    let desired = ConfigRevision::from_bytes(CONFIG_B.as_bytes());
    let current = ConfigRevision::from_bytes(b"unexpected current document");
    let last_good = ConfigRevision::from_bytes(b"unexpected last-good document");
    let error = ConfigStoreError::RecoveryConflict(Box::new(ConfigRecoveryConflict {
        active,
        desired,
        current,
        last_good: Some(last_good),
    }));

    let rendered = error.to_string();
    assert!(rendered.contains(&active.to_string()));
    assert!(rendered.contains(&desired.to_string()));
    assert!(rendered.contains(&current.to_string()));
    assert!(rendered.contains(&last_good.to_string()));
    assert!(std::error::Error::source(&error).is_none());

    let ConfigStoreError::RecoveryConflict(conflict) = error else {
        panic!("expected recovery conflict");
    };
    assert_eq!(
        *conflict,
        ConfigRecoveryConflict {
            active,
            desired,
            current,
            last_good: Some(last_good),
        }
    );
}

#[test]
fn invalid_candidate_never_changes_the_file() {
    let (_directory, path, store) = open_store();
    let before = fs::read(&path).expect("read initial config");

    let invalid_log_level = format!("[logging]\nlevel = \"warning\"\n{CONFIG_B}");
    for document in [b"not valid = [".as_slice(), invalid_log_level.as_bytes()] {
        let error = store
            .validate_candidate(document)
            .expect_err("invalid candidate must fail before persistence");

        assert!(matches!(error, ConfigStoreError::Config(_)));
        assert_eq!(fs::read(&path).expect("read preserved config"), before);
    }
}

#[test]
fn stale_revision_is_rejected_without_overwriting_current_document() {
    let (_directory, path, store) = open_store();
    let revision_a = store.revision();
    let candidate_b = store
        .validate_candidate(CONFIG_B)
        .expect("validate replacement");
    let committed = store
        .replace(revision_a, candidate_b)
        .expect("commit replacement");
    assert!(committed.changed);

    let candidate_a = store
        .validate_candidate(CONFIG_A)
        .expect("validate stale replacement");
    let error = store
        .replace(revision_a, candidate_a)
        .expect_err("stale compare-and-swap must fail");

    assert!(matches!(error, ConfigStoreError::RevisionConflict { .. }));
    assert_eq!(
        fs::read_to_string(path).expect("read current config"),
        CONFIG_B
    );
}

#[test]
fn external_manual_edit_is_preserved_and_reported() {
    let (_directory, path, store) = open_store();
    let known = store.revision();
    fs::write(&path, CONFIG_B).expect("simulate manual edit");
    let candidate = store
        .validate_candidate(CONFIG_A)
        .expect("validate candidate");

    let error = store
        .replace(known, candidate)
        .expect_err("store must not overwrite a manual edit");

    assert!(matches!(
        error,
        ConfigStoreError::ExternalModification { .. }
    ));
    assert_eq!(
        fs::read_to_string(path).expect("read preserved manual edit"),
        CONFIG_B
    );
}

#[test]
fn identical_candidate_is_an_idempotent_no_op() {
    let (_directory, path, store) = open_store();
    let initial = store.revision();
    let candidate = store
        .validate_candidate(CONFIG_A)
        .expect("validate same document");

    let committed = store
        .replace(initial, candidate)
        .expect("idempotent commit");

    assert!(!committed.changed);
    assert_eq!(committed.revision, initial);
    assert_eq!(
        fs::read_to_string(path).expect("read unchanged document"),
        CONFIG_A
    );
}

#[test]
fn candidate_remains_pending_until_explicit_readiness_commit() {
    let (_directory, path, store) = open_store();
    let active = store.active_revision();
    let candidate = store
        .validate_candidate(CONFIG_B)
        .expect("validate replacement");
    let desired = candidate.revision();

    store.replace(active, candidate).expect("stage replacement");

    assert_eq!(store.active_revision(), active);
    assert_eq!(store.revision(), desired);
    assert_eq!(store.pending_revision(), Some(desired));
    assert!(store.pending_path.exists());
    assert_eq!(
        fs::read_to_string(path).expect("read desired document"),
        CONFIG_B
    );
}

#[test]
fn failed_activation_rolls_back_canonical_document_and_revision() {
    let (_directory, path, store) = open_store();
    let active = store.active_revision();
    let candidate = store
        .validate_candidate(CONFIG_B)
        .expect("validate replacement");
    store.replace(active, candidate).expect("stage replacement");

    let rolled_back = store.rollback_pending().expect("rollback candidate");

    assert!(rolled_back.changed);
    assert_eq!(rolled_back.revision, active);
    assert_eq!(store.revision(), active);
    assert_eq!(store.active_revision(), active);
    assert_eq!(store.pending_revision(), None);
    assert!(!store.pending_path.exists());
    assert_eq!(
        fs::read_to_string(path).expect("read restored document"),
        CONFIG_A
    );
}

#[test]
fn successful_activation_commits_last_good_and_survives_reopen() {
    let (_directory, path, store) = open_store();
    let candidate = store
        .validate_candidate(CONFIG_B)
        .expect("validate replacement");
    let desired = candidate.revision();
    store
        .replace(store.revision(), candidate)
        .expect("stage replacement");

    store
        .activate_desired(desired)
        .expect("commit ready generation");

    assert_eq!(store.active_revision(), desired);
    assert_eq!(store.pending_revision(), None);
    assert_eq!(
        fs::read_to_string(&store.last_good_path).expect("read last-good document"),
        CONFIG_B
    );
    assert!(!store.pending_path.exists());
    drop(store);

    let (reopened, _) = CanonicalConfigStore::open(path).expect("reopen committed store");
    assert_eq!(reopened.revision(), desired);
    assert_eq!(reopened.active_revision(), desired);
}

#[test]
fn interrupted_pending_activation_restores_last_good_on_open() {
    let (_directory, path, store) = open_store();
    let active = store.active_revision();
    let candidate = store
        .validate_candidate(CONFIG_B)
        .expect("validate replacement");
    store.replace(active, candidate).expect("stage replacement");
    assert!(store.pending_path.exists());
    drop(store);

    let (recovered, _) = CanonicalConfigStore::open(path.clone()).expect("recover store");

    assert_eq!(recovered.revision(), active);
    assert_eq!(
        fs::read_to_string(path).expect("read recovered canonical document"),
        CONFIG_A
    );
    assert!(!recovered.pending_path.exists());
}

#[test]
fn interrupted_activation_commit_keeps_new_last_good_on_open() {
    let (_directory, path, store) = open_store();
    let candidate = store
        .validate_candidate(CONFIG_B)
        .expect("validate replacement");
    let desired = candidate.revision();
    store
        .replace(store.revision(), candidate)
        .expect("stage replacement");
    fs::write(&store.last_good_path, CONFIG_B)
        .expect("simulate committed last-good before journal deletion");
    drop(store);

    let (recovered, _) = CanonicalConfigStore::open(path.clone()).expect("recover committed store");

    assert_eq!(recovered.revision(), desired);
    assert_eq!(
        fs::read_to_string(path).expect("read committed canonical document"),
        CONFIG_B
    );
    assert!(!recovered.pending_path.exists());
}

#[test]
fn second_candidate_is_rejected_while_activation_is_pending() {
    let (_directory, _path, store) = open_store();
    let active = store.revision();
    let candidate = store
        .validate_candidate(CONFIG_B)
        .expect("validate replacement");
    store.replace(active, candidate).expect("stage replacement");
    let retry = store.validate_candidate(CONFIG_A).expect("validate retry");

    let error = store
        .replace(store.revision(), retry)
        .expect_err("overlapping activation must fail");

    assert!(matches!(error, ConfigStoreError::ActivationPending { .. }));
}

#[test]
fn debug_output_never_contains_configuration_material() {
    let (_directory, _path, store) = open_store();
    let debug = format!("{store:?}");

    assert!(!debug.contains("0123456789abcdef"));
    assert!(!debug.contains("127.0.0.1:7443"));
    assert!(debug.contains(&store.revision().to_string()));
}
