use super::*;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

const CONFIG_A: &str = r#"
[[credentials]]
credential_id = "app-test"
principal_id = "app-test"
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
credential_id = "app-test"
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
credential_id = "app-test"
principal_id = "app-test"
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
credential_id = "app-test"
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
            "mptunnel-app-test-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create isolated app test directory");
        let rcgen::CertifiedKey { cert, .. } =
            rcgen::generate_simple_self_signed(vec!["mptunnel.test".to_string()])
                .expect("generate app-test TLS identity");
        fs::write(path.join("certificate.pem"), cert.pem()).expect("write app-test certificate");
        fs::write(
            path.join("credential.key"),
            b"0123456789abcdef0123456789abcdef",
        )
        .expect("write app-test credential");
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

struct DropNotice(Option<oneshot::Sender<()>>);

impl Drop for DropNotice {
    fn drop(&mut self) {
        if let Some(notice) = self.0.take() {
            let _ = notice.send(());
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VpnFaultEvent {
    Publish,
    Unpublish,
    RuntimeStopped,
    Cleanup,
}

#[derive(Debug, Default)]
struct VpnFaultState {
    events: Vec<VpnFaultEvent>,
    publish_fails: bool,
    unpublish_blocked: bool,
    cleanup_fails: bool,
}

#[derive(Debug, Clone, Default)]
struct FaultInjectedVpnLifecycle {
    state: Arc<Mutex<VpnFaultState>>,
}

impl FaultInjectedVpnLifecycle {
    fn set_publish_fails(&self, value: bool) {
        self.state.lock().expect("VPN fault state").publish_fails = value;
    }

    fn set_unpublish_blocked(&self, value: bool) {
        self.state
            .lock()
            .expect("VPN fault state")
            .unpublish_blocked = value;
    }

    fn set_cleanup_fails(&self, value: bool) {
        self.state.lock().expect("VPN fault state").cleanup_fails = value;
    }

    fn record(&self, event: VpnFaultEvent) {
        self.state
            .lock()
            .expect("VPN fault state")
            .events
            .push(event);
    }

    fn events(&self) -> Vec<VpnFaultEvent> {
        self.state.lock().expect("VPN fault state").events.clone()
    }

    async fn wait_for(&self, event: VpnFaultEvent) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !self.events().contains(&event) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("VPN lifecycle event");
    }
}

impl crate::platform::VpnGenerationLifecycle for FaultInjectedVpnLifecycle {
    async fn publish_when_worker_ready(
        &mut self,
        _timeout: Duration,
    ) -> Result<(), crate::platform::VpnGenerationError> {
        self.record(VpnFaultEvent::Publish);
        if self.state.lock().expect("VPN fault state").publish_fails {
            Err(vpn_fault(crate::platform::VpnGenerationStage::Publish))
        } else {
            Ok(())
        }
    }

    async fn unpublish(
        &mut self,
        _attempts: std::num::NonZeroUsize,
        _retry_delay: Duration,
    ) -> Result<(), crate::platform::VpnGenerationError> {
        self.record(VpnFaultEvent::Unpublish);
        if self
            .state
            .lock()
            .expect("VPN fault state")
            .unpublish_blocked
        {
            Err(vpn_fault(crate::platform::VpnGenerationStage::Unpublish))
        } else {
            Ok(())
        }
    }

    async fn cleanup_after_worker_stopped(
        &mut self,
        _attempts: std::num::NonZeroUsize,
        _retry_delay: Duration,
    ) -> Result<(), crate::platform::VpnGenerationError> {
        self.record(VpnFaultEvent::Cleanup);
        if self.state.lock().expect("VPN fault state").cleanup_fails {
            Err(vpn_fault(crate::platform::VpnGenerationStage::Cleanup))
        } else {
            Ok(())
        }
    }
}

fn vpn_fault(stage: crate::platform::VpnGenerationStage) -> crate::platform::VpnGenerationError {
    crate::platform::VpnGenerationError::adapter(
        crate::platform::VpnPlatform::Linux,
        stage,
        "injected lifecycle fault",
    )
}

fn test_managed_vpn_policy() -> ManagedVpnLifecyclePolicy {
    ManagedVpnLifecyclePolicy {
        ready_timeout: Duration::from_millis(100),
        shutdown_attempts: std::num::NonZeroUsize::new(1).expect("one attempt"),
        retry_delay: Duration::from_millis(1),
    }
}

async fn fault_runtime(
    generation: crate::runtime::RuntimeGenerationControl,
    lifecycle: FaultInjectedVpnLifecycle,
) -> crate::runtime::RuntimeGenerationOutcome {
    generation.wait_for_retirement_authorization().await;
    let reason = generation.wait_for_stop().await;
    lifecycle.record(VpnFaultEvent::RuntimeStopped);
    stop_outcome(reason)
}

fn open_pending_store() -> (
    TestDirectory,
    Arc<CanonicalConfigStore>,
    crate::config::ConfigRevision,
    crate::config::ConfigRevision,
) {
    let directory = TestDirectory::new();
    let path = directory.path().join("config.toml");
    fs::write(&path, CONFIG_A).expect("write active config");
    let (store, _) = CanonicalConfigStore::open(path).expect("open canonical config");
    let active = store.active_revision();
    let candidate = store
        .validate_candidate(CONFIG_B)
        .expect("validate candidate config");
    let desired = candidate.revision();
    store
        .replace(active, candidate)
        .expect("stage candidate config");
    (directory, Arc::new(store), active, desired)
}

fn stop_outcome(
    reason: crate::runtime::RuntimeGenerationStopReason,
) -> crate::runtime::RuntimeGenerationOutcome {
    match reason {
        crate::runtime::RuntimeGenerationStopReason::ReloadRequested => {
            crate::runtime::RuntimeGenerationOutcome::ReloadRequested
        }
        crate::runtime::RuntimeGenerationStopReason::ShutdownRequested => {
            crate::runtime::RuntimeGenerationOutcome::ShutdownRequested
        }
    }
}

#[test]
fn restart_backoff_doubles_until_max() {
    assert_eq!(
        next_restart_backoff(Duration::from_millis(100), Duration::from_millis(1_000)),
        Duration::from_millis(200)
    );
    assert_eq!(
        next_restart_backoff(Duration::from_millis(800), Duration::from_millis(1_000)),
        Duration::from_millis(1_000)
    );
}

#[test]
fn process_composition_uses_only_the_platform_neutral_vpn_boundary() {
    let source = include_str!("app.rs");
    for forbidden in [
        "PreparedLinuxVpn",
        "LinuxVpnGeneration",
        "LinuxVpnPrepare",
        "LinuxVpnPublish",
        "LinuxVpnShutdown",
        "compile_linux_vpn",
        "prepare_linux_vpn",
        "#[cfg(target_os = \"linux\")]",
    ] {
        assert!(
            !source.contains(forbidden),
            "generic app composition must not contain {forbidden}"
        );
    }
    assert!(source.contains("PreparedVpnGeneration"));
    assert!(source.contains("validate_vpn_generation"));
}

#[test]
fn config_file_invocation_defaults_to_config_toml_without_args() {
    let args = vec![OsString::from("mptunnel")];
    assert_eq!(
        config_file_from_args(&args).expect("args"),
        Some(ConfigFileInvocation {
            path: PathBuf::from(DEFAULT_CONFIG_PATH),
            check_config: None,
        })
    );
}

#[test]
fn config_file_invocation_preserves_check_config_override() {
    let args = vec![
        OsString::from("mptunnel"),
        OsString::from("--config"),
        OsString::from("edge.toml"),
        OsString::from("--check-config"),
    ];
    assert_eq!(
        config_file_from_args(&args).expect("args"),
        Some(ConfigFileInvocation {
            path: PathBuf::from("edge.toml"),
            check_config: Some(true),
        })
    );
}

#[test]
fn config_file_invocation_accepts_false_check_config_override() {
    let args = vec![
        OsString::from("mptunnel"),
        OsString::from("--check-config=false"),
        OsString::from("--config=client.toml"),
    ];
    assert_eq!(
        config_file_from_args(&args).expect("args"),
        Some(ConfigFileInvocation {
            path: PathBuf::from("client.toml"),
            check_config: Some(false),
        })
    );
}

#[test]
fn config_file_version_and_help_are_cli_meta_actions() {
    for flag in ["--version", "-V", "--help", "-h"] {
        let args = vec![
            OsString::from("mptunnel"),
            OsString::from("--config"),
            OsString::from("edge.toml"),
            OsString::from(flag),
        ];
        assert!(config_file_meta_action_requested(&args), "flag={flag}");
    }

    let runtime_args = vec![
        OsString::from("mptunnel"),
        OsString::from("--config=edge.toml"),
    ];
    assert!(!config_file_meta_action_requested(&runtime_args));
}

#[test]
fn explicit_operational_commands_bypass_only_the_config_runtime_shortcut() {
    for command in ["platform", "status", "doctor", "route", "dns"] {
        let args = vec![
            OsString::from("mptunnel"),
            OsString::from("--config"),
            OsString::from("edge.toml"),
            OsString::from(command),
        ];
        assert!(
            operational_command_requested(&args),
            "explicit {command} command must enter the CLI parser"
        );
    }

    let config_named_status = vec![
        OsString::from("mptunnel"),
        OsString::from("--config"),
        OsString::from("status"),
    ];
    assert!(!operational_command_requested(&config_named_status));
    assert_eq!(
        config_file_from_args(&config_named_status).expect("config invocation"),
        Some(ConfigFileInvocation {
            path: PathBuf::from("status"),
            check_config: None,
        })
    );

    let no_args = vec![OsString::from("mptunnel")];
    assert!(!operational_command_requested(&no_args));
    assert_eq!(
        config_file_from_args(&no_args).expect("default config"),
        Some(ConfigFileInvocation {
            path: PathBuf::from(DEFAULT_CONFIG_PATH),
            check_config: None,
        })
    );
}

#[test]
fn operational_commands_apply_the_global_logging_contract() {
    let cli = Cli::try_parse_from(["mptunnel", "--log-no-console", "platform"])
        .expect("parse platform command");
    assert!(matches!(
        run(cli),
        Err(AppError::Config(CliConfigError::Config(
            crate::config::ConfigError::LoggingSinkRequired
        )))
    ));

    let directory = TestDirectory::new();
    let config_path = directory.path().join("config.toml");
    let last_good_path = directory.path().join("config.toml.mptunnel.last-good");
    let pending_path = directory.path().join("config.toml.mptunnel.pending");
    let hard_link_path = directory.path().join("config-hard-link.toml");
    fs::write(&config_path, "persistent-config").expect("write protected config");
    fs::hard_link(&config_path, &hard_link_path).expect("create protected config hard link");

    for log_path in [
        &config_path,
        &last_good_path,
        &pending_path,
        &hard_link_path,
    ] {
        let cli = Cli::try_parse_from([
            OsString::from("mptunnel"),
            OsString::from("--config"),
            config_path.as_os_str().to_owned(),
            OsString::from("--log-file"),
            log_path.as_os_str().to_owned(),
            OsString::from("platform"),
        ])
        .expect("parse protected operational log path");
        assert!(matches!(
            run(cli),
            Err(AppError::Logging(
                crate::observability::ConfigureError::ConfigStorePath { .. }
            ))
        ));
    }
    assert_eq!(
        fs::read_to_string(&config_path).expect("read protected config"),
        "persistent-config"
    );
    assert!(!last_good_path.exists());
    assert!(!pending_path.exists());
}

#[test]
fn config_file_invocation_rejects_unknown_and_duplicate_arguments() {
    let unknown = vec![
        OsString::from("mptunnel"),
        OsString::from("--config=config.toml"),
        OsString::from("--unknown"),
    ];
    assert!(matches!(
        config_file_from_args(&unknown),
        Err(AppError::UnsupportedConfigFileArgument)
    ));

    let duplicate = vec![
        OsString::from("mptunnel"),
        OsString::from("--config=a.toml"),
        OsString::from("-c"),
        OsString::from("b.toml"),
    ];
    assert!(matches!(
        config_file_from_args(&duplicate),
        Err(AppError::DuplicateConfigFileArgument)
    ));

    for legacy_boolean in ["1", "0", "yes", "no", "on", "off"] {
        let args = vec![
            OsString::from("mptunnel"),
            OsString::from("--config=config.toml"),
            OsString::from(format!("--check-config={legacy_boolean}")),
        ];
        assert!(
            matches!(
                config_file_from_args(&args),
                Err(AppError::InvalidCheckConfigFlag)
            ),
            "legacy boolean must be rejected: {legacy_boolean}"
        );
    }
}

#[tokio::test]
async fn process_shutdown_wrapper_preserves_operation_completion() {
    let result =
        run_process_until_shutdown(async { Ok::<(), &'static str>(()) }, ProcessShutdown::new())
            .await;
    assert!(result.is_ok());

    let result = run_process_until_shutdown(
        async { Err::<(), _>("operation failed") },
        ProcessShutdown::new(),
    )
    .await;
    assert!(matches!(
        result,
        Err(ProcessExitError::Operation("operation failed"))
    ));
}

#[tokio::test]
async fn process_signal_waits_for_cooperative_generation_retirement() {
    let shutdown = ProcessShutdown::new();
    let operation_shutdown = shutdown.clone();
    let (started_tx, started_rx) = oneshot::channel();
    let (retiring_tx, retiring_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let operation = async move {
        let _ = started_tx.send(());
        operation_shutdown.wait().await;
        let _ = retiring_tx.send(());
        release_rx.await.expect("release retirement");
        Ok::<(), &'static str>(())
    };
    let signal = async move {
        started_rx.await.expect("operation started");
        Ok(())
    };

    let runner = tokio::spawn(run_process_with_signal(
        operation,
        shutdown,
        signal,
        Duration::from_secs(1),
    ));
    retiring_rx
        .await
        .expect("operation observed cooperative shutdown");
    assert!(!runner.is_finished());
    release_tx
        .send(())
        .expect("authorize retirement completion");

    assert!(runner.await.expect("process wrapper task").is_ok());
}

#[tokio::test]
async fn process_shutdown_timeout_aborts_and_joins_stuck_operation() {
    let shutdown = ProcessShutdown::new();
    let operation_shutdown = shutdown.clone();
    let (started_tx, started_rx) = oneshot::channel();
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let operation = async move {
        let _drop_notice = DropNotice(Some(dropped_tx));
        let _ = started_tx.send(());
        operation_shutdown.wait().await;
        std::future::pending::<Result<(), &'static str>>().await
    };
    let signal = async move {
        started_rx.await.expect("operation started");
        Ok(())
    };

    let result =
        run_process_with_signal(operation, shutdown, signal, Duration::from_millis(10)).await;

    assert!(matches!(
        result,
        Err(ProcessExitError::ShutdownTimeout(timeout))
            if timeout == Duration::from_millis(10)
    ));
    dropped_rx.await.expect("aborted operation was joined");
}

#[tokio::test]
async fn process_timeout_waits_for_unpublication_before_aborting_runtime() {
    let shutdown = ProcessShutdown::new();
    shutdown.protect_published_vpn_runtime();
    let operation_shutdown = shutdown.clone();
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (dropped_tx, mut dropped_rx) = oneshot::channel();
    let operation = async move {
        let _drop_notice = DropNotice(Some(dropped_tx));
        let _ = started_tx.send(());
        operation_shutdown.wait().await;
        release_rx.await.expect("safe host unpublication");
        operation_shutdown.release_published_vpn_runtime();
        std::future::pending::<Result<(), &'static str>>().await
    };
    let signal = async move {
        started_rx.await.expect("operation started");
        Ok(())
    };

    let runner = tokio::spawn(run_process_with_signal(
        operation,
        shutdown,
        signal,
        Duration::from_millis(10),
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!runner.is_finished());
    assert_eq!(
        dropped_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty),
        "published packet runtime was dropped at the generic teardown timeout"
    );

    release_tx.send(()).expect("release safe retirement");
    assert!(matches!(
        runner.await.expect("process wrapper"),
        Err(ProcessExitError::ShutdownTimeout(timeout))
            if timeout == Duration::from_millis(10)
    ));
    dropped_rx.await.expect("completed operation was dropped");
}

#[tokio::test]
async fn pending_config_activates_only_after_generation_readiness() {
    let (_directory, store, active, desired) = open_pending_store();
    let control = crate::runtime::RuntimeConfigControl::new(store.clone());
    let shutdown = ProcessShutdown::new();
    let runtime_control = control.clone();
    let driver = tokio::spawn(drive_canonical_generation(
        async move { stop_outcome(runtime_control.wait_for_stop().await) },
        control.clone(),
        shutdown,
    ));

    tokio::task::yield_now().await;
    assert_eq!(store.active_revision(), active);
    assert_eq!(store.pending_revision(), Some(desired));

    control.signal_ready_for_test();
    tokio::time::timeout(Duration::from_secs(1), async {
        while store.active_revision() != desired {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ready generation activation");
    assert_eq!(store.pending_revision(), None);

    control.request_shutdown();
    let terminal = driver.await.expect("generation driver");
    assert!(matches!(
        terminal,
        CanonicalGenerationTerminal::Runtime {
            outcome: crate::runtime::RuntimeGenerationOutcome::ShutdownRequested,
            activated: true,
        }
    ));
}

#[test]
fn next_generation_preserves_the_single_canonical_store_owner() {
    let (_directory, store, _active, _desired) = open_pending_store();
    let first = crate::runtime::RuntimeConfigControl::new(store);
    let second = first.next_generation();

    assert!(std::ptr::eq(first.store(), second.store()));
    assert_eq!(second.runtime_revision(), first.store().revision());
}

#[test]
fn supervised_restart_reopens_disk_into_a_fresh_canonical_store() {
    let directory = TestDirectory::new();
    let path = directory.path().join("config.toml");
    fs::write(&path, CONFIG_A).expect("write initial supervised config");
    let invocation = ConfigFileInvocation {
        path: path.clone(),
        check_config: None,
    };
    let first = open_config_file_generation(&invocation).expect("open initial generation");
    let first_revision = first.config_control.runtime_revision();

    fs::write(&path, CONFIG_B).expect("externally edit supervised config");
    let reopened =
        reopen_supervised_config_file_generation(&invocation).expect("reopen edited generation");
    assert_ne!(reopened.config_control.runtime_revision(), first_revision);
    assert!(
        !std::ptr::eq(
            first.config_control.store(),
            reopened.config_control.store()
        ),
        "supervised restart reused the stale canonical store"
    );
    assert_eq!(
        reopened.config,
        crate::config::load_config_toml(&path).expect("parse expected edited config")
    );

    fs::write(&path, "unknown_supervised_field = true\n").expect("write invalid external edit");
    let invalid = reopen_supervised_config_file_generation(&invocation)
        .expect_err("invalid external edit must stop supervision");
    let invalid = invalid.to_string();
    assert!(invalid.contains(&path.display().to_string()));
    assert!(invalid.contains("unknown_supervised_field"));

    fs::remove_file(&path).expect("remove externally edited config");
    let missing = reopen_supervised_config_file_generation(&invocation)
        .expect_err("missing external config must stop supervision");
    let (missing_path, source) = match missing {
        AppError::SupervisedConfigReopen { path, source } => (path, source),
        other => panic!("missing config produced the wrong app error: {other:?}"),
    };
    assert_eq!(missing_path, path);
    let source = match *source {
        AppError::ConfigStore(source) => source,
        other => panic!("missing config produced the wrong reopen source: {other:?}"),
    };
    let source = match *source {
        ConfigStoreError::Io(source) => source,
        other => panic!("missing config produced the wrong store error: {other:?}"),
    };
    assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
}

#[tokio::test]
async fn failed_candidate_is_never_activated_and_rolls_back_to_last_good() {
    let (_directory, store, active, desired) = open_pending_store();
    let control = crate::runtime::RuntimeConfigControl::new(store.clone());

    let terminal = drive_canonical_generation(
        async {
            crate::runtime::RuntimeGenerationOutcome::Failed(
                crate::runtime::RuntimeError::Protocol("candidate startup failed"),
            )
        },
        control.clone(),
        ProcessShutdown::new(),
    )
    .await;

    assert!(matches!(
        terminal,
        CanonicalGenerationTerminal::Runtime {
            outcome: crate::runtime::RuntimeGenerationOutcome::Failed(_),
            activated: false,
        }
    ));
    assert_eq!(store.active_revision(), active);
    assert_eq!(store.pending_revision(), Some(desired));

    let restored = rollback_failed_candidate(&control, desired)
        .expect("rollback succeeds")
        .expect("candidate was pending");
    assert!(restored.changed);
    assert_eq!(restored.revision, active);
    assert_eq!(store.revision(), active);
    assert_eq!(store.active_revision(), active);
    assert_eq!(store.pending_revision(), None);
    assert_eq!(
        fs::read_to_string(store.path()).expect("read restored canonical config"),
        CONFIG_A
    );
}

#[tokio::test]
async fn terminal_runtime_failure_wins_over_simultaneous_readiness() {
    let (_directory, store, active, desired) = open_pending_store();
    let control = crate::runtime::RuntimeConfigControl::new(store.clone());
    control.signal_ready_for_test();

    let terminal = drive_canonical_generation(
        async {
            crate::runtime::RuntimeGenerationOutcome::Failed(
                crate::runtime::RuntimeError::Protocol("terminal startup failure"),
            )
        },
        control,
        ProcessShutdown::new(),
    )
    .await;

    assert!(matches!(
        terminal,
        CanonicalGenerationTerminal::Runtime {
            outcome: crate::runtime::RuntimeGenerationOutcome::Failed(_),
            activated: false,
        }
    ));
    assert_eq!(store.active_revision(), active);
    assert_eq!(store.pending_revision(), Some(desired));
}

#[tokio::test]
async fn managed_vpn_start_failure_cleans_inert_prepare_without_publication() {
    let generation = crate::runtime::RuntimeGenerationControl::new();
    generation.defer_retirement();
    let lifecycle = FaultInjectedVpnLifecycle::default();
    let runtime_lifecycle = lifecycle.clone();
    let shutdown = ProcessShutdown::new();

    let terminal = drive_managed_runtime_generation_with_policy(
        async move {
            runtime_lifecycle.record(VpnFaultEvent::RuntimeStopped);
            crate::runtime::RuntimeGenerationOutcome::Failed(
                crate::runtime::RuntimeError::Protocol("injected start failure"),
            )
        },
        generation,
        None,
        shutdown.clone(),
        lifecycle.clone(),
        test_managed_vpn_policy(),
    )
    .await
    .expect("safe inert cleanup");

    assert!(matches!(
        terminal.outcome,
        crate::runtime::RuntimeGenerationOutcome::Failed(_)
    ));
    assert!(!terminal.activated);
    assert_eq!(
        lifecycle.events(),
        vec![
            VpnFaultEvent::RuntimeStopped,
            VpnFaultEvent::Unpublish,
            VpnFaultEvent::Cleanup,
        ]
    );
    assert!(!shutdown.must_preserve_published_vpn_runtime());
}

#[tokio::test]
async fn managed_vpn_readiness_failure_retires_without_publishing() {
    let generation = crate::runtime::RuntimeGenerationControl::new();
    generation.defer_retirement();
    let lifecycle = FaultInjectedVpnLifecycle::default();
    let runtime_generation = generation.clone();
    let runtime_lifecycle = lifecycle.clone();
    let shutdown = ProcessShutdown::new();
    let driver = tokio::spawn(drive_managed_runtime_generation_with_policy(
        fault_runtime(runtime_generation, runtime_lifecycle),
        generation.clone(),
        None,
        shutdown.clone(),
        lifecycle.clone(),
        test_managed_vpn_policy(),
    ));

    generation.mark_failed("injected readiness failure");
    let terminal = driver
        .await
        .expect("managed driver")
        .expect("safe readiness retirement");
    assert!(matches!(
        terminal.outcome,
        crate::runtime::RuntimeGenerationOutcome::ShutdownRequested
    ));
    assert!(!terminal.activated);
    assert_eq!(
        lifecycle.events(),
        vec![
            VpnFaultEvent::Unpublish,
            VpnFaultEvent::RuntimeStopped,
            VpnFaultEvent::Cleanup,
        ]
    );
    assert!(!shutdown.must_preserve_published_vpn_runtime());
}

#[tokio::test]
async fn managed_vpn_publish_failure_retires_before_returning_error() {
    let (_directory, store, _active, _desired) = open_pending_store();
    let control = crate::runtime::RuntimeConfigControl::new(store);
    let generation = control.generation();
    generation.defer_retirement();
    let lifecycle = FaultInjectedVpnLifecycle::default();
    lifecycle.set_publish_fails(true);
    let shutdown = ProcessShutdown::new();
    control.signal_ready_for_test();

    let error = drive_managed_runtime_generation_with_policy(
        fault_runtime(generation.clone(), lifecycle.clone()),
        generation,
        None,
        shutdown.clone(),
        lifecycle.clone(),
        test_managed_vpn_policy(),
    )
    .await
    .expect_err("injected publication failure");

    assert!(matches!(error, AppError::VpnGeneration(_)));
    assert_eq!(
        lifecycle.events(),
        vec![
            VpnFaultEvent::Publish,
            VpnFaultEvent::Unpublish,
            VpnFaultEvent::RuntimeStopped,
            VpnFaultEvent::Cleanup,
        ]
    );
    assert!(!shutdown.must_preserve_published_vpn_runtime());
}

#[tokio::test]
async fn managed_vpn_activation_failure_unpublishes_before_runtime_stop() {
    let (_directory, store, _active, desired) = open_pending_store();
    let control = crate::runtime::RuntimeConfigControl::new(store.clone());
    let generation = control.generation();
    generation.defer_retirement();
    let lifecycle = FaultInjectedVpnLifecycle::default();
    let shutdown = ProcessShutdown::new();

    let restored = store
        .rollback_pending()
        .expect("inject activation conflict");
    assert_ne!(restored.revision, desired);
    control.signal_ready_for_test();
    let error = drive_managed_runtime_generation_with_policy(
        fault_runtime(generation.clone(), lifecycle.clone()),
        generation,
        Some(control),
        shutdown.clone(),
        lifecycle.clone(),
        test_managed_vpn_policy(),
    )
    .await
    .expect_err("activation conflict");

    assert!(matches!(error, AppError::ConfigStore(_)));
    assert_eq!(
        lifecycle.events(),
        vec![
            VpnFaultEvent::Publish,
            VpnFaultEvent::Unpublish,
            VpnFaultEvent::RuntimeStopped,
            VpnFaultEvent::Cleanup,
        ]
    );
    assert!(!shutdown.must_preserve_published_vpn_runtime());
}

#[tokio::test]
async fn reload_retirement_keeps_runtime_alive_while_unpublish_is_blocked() {
    let (_directory, store, _active, _desired) = open_pending_store();
    let control = crate::runtime::RuntimeConfigControl::new(store);
    let generation = control.generation();
    generation.defer_retirement();
    let lifecycle = FaultInjectedVpnLifecycle::default();
    lifecycle.set_unpublish_blocked(true);
    let shutdown = ProcessShutdown::new();
    let driver = tokio::spawn(drive_managed_runtime_generation_with_policy(
        fault_runtime(generation.clone(), lifecycle.clone()),
        generation.clone(),
        None,
        shutdown.clone(),
        lifecycle.clone(),
        test_managed_vpn_policy(),
    ));

    control.signal_ready_for_test();
    lifecycle.wait_for(VpnFaultEvent::Publish).await;
    generation.request_reload();
    lifecycle.wait_for(VpnFaultEvent::Unpublish).await;
    assert!(!driver.is_finished());
    assert!(
        !lifecycle.events().contains(&VpnFaultEvent::RuntimeStopped),
        "packet runtime stopped while host publication remained"
    );
    assert!(shutdown.must_preserve_published_vpn_runtime());

    lifecycle.set_unpublish_blocked(false);
    let terminal = driver
        .await
        .expect("managed driver")
        .expect("reload retirement");
    assert!(matches!(
        terminal.outcome,
        crate::runtime::RuntimeGenerationOutcome::ReloadRequested
    ));
    let events = lifecycle.events();
    let last_unpublish = events
        .iter()
        .rposition(|event| *event == VpnFaultEvent::Unpublish)
        .expect("unpublication");
    let runtime_stop = events
        .iter()
        .position(|event| *event == VpnFaultEvent::RuntimeStopped)
        .expect("runtime stop");
    let cleanup = events
        .iter()
        .position(|event| *event == VpnFaultEvent::Cleanup)
        .expect("cleanup");
    assert!(last_unpublish < runtime_stop && runtime_stop < cleanup);
    assert!(!shutdown.must_preserve_published_vpn_runtime());
}

#[tokio::test]
async fn cleanup_failure_occurs_only_after_unpublish_and_runtime_stop() {
    let (_directory, store, _active, _desired) = open_pending_store();
    let control = crate::runtime::RuntimeConfigControl::new(store);
    let generation = control.generation();
    generation.defer_retirement();
    let lifecycle = FaultInjectedVpnLifecycle::default();
    lifecycle.set_cleanup_fails(true);
    let shutdown = ProcessShutdown::new();
    let driver = tokio::spawn(drive_managed_runtime_generation_with_policy(
        fault_runtime(generation.clone(), lifecycle.clone()),
        generation.clone(),
        None,
        shutdown.clone(),
        lifecycle.clone(),
        test_managed_vpn_policy(),
    ));

    control.signal_ready_for_test();
    lifecycle.wait_for(VpnFaultEvent::Publish).await;
    generation.request_shutdown();
    let error = driver
        .await
        .expect("managed driver")
        .expect_err("injected cleanup failure");
    assert!(matches!(
        error,
        AppError::VpnGeneration(error)
            if matches!(
                *error,
                crate::platform::VpnGenerationError::Adapter {
                    stage: crate::platform::VpnGenerationStage::Cleanup,
                    ..
                }
            )
    ));
    assert_eq!(
        lifecycle.events(),
        vec![
            VpnFaultEvent::Publish,
            VpnFaultEvent::Unpublish,
            VpnFaultEvent::RuntimeStopped,
            VpnFaultEvent::Cleanup,
        ]
    );
    assert!(!shutdown.must_preserve_published_vpn_runtime());
}
