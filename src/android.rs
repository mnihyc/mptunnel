//! Android JNI host for one embedded MPTUNNEL generation.
//!
//! The Java/Kotlin facade supplies only an app-private root, an opaque profile
//! id, a path-free TOML template, and material contents. This module owns the
//! private files, runtime thread, listener-readiness barrier, and synchronous
//! `VpnService.protect(int)` callback.

use crate::config::load_config_toml;
use crate::platform::SystemPacketDeviceProvider;
use crate::runtime::{
    RuntimeHostControl, RuntimeHostPhase, RuntimeHostStats, run_with_vpn_host_providers_and_control,
};
use crate::transport::{
    HostSocketHandle, HostSocketProtectionRequest, HostSocketProtector,
    SystemCarrierNetworkProvider,
};
use jni::errors::ThrowRuntimeExAndDefault;
use jni::objects::{Global, JByteArray, JClass, JObject, JObjectArray, JString};
use jni::sys::{jboolean, jlong};
use jni::{Env, EnvUnowned, JValue, JavaVM, jni_mangle, jni_sig, jni_str};
use serde::Serialize;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const PROFILE_DIRECTORY: &str = "mptunnel";
const CONFIG_FILE: &str = "config.toml";
const CREDENTIAL_FILE: &str = "credential.key";
const CERTIFICATE_FILE: &str = "pinned-certificate.pem";
const TRANSPORT_SECRET_FILE: &str = "transport-secret.key";
const LOCAL_PROXY_PASSWORD_FILE: &str = "local-proxy-password.key";
const CREDENTIAL_TOKEN: &str = "@mptunnel-profile-credential@";
const CERTIFICATE_TOKEN: &str = "@mptunnel-profile-certificate@";
const TRANSPORT_SECRET_TOKEN: &str = "@mptunnel-profile-transport-secret@";
const LOCAL_PROXY_PASSWORD_TOKEN: &str = "@mptunnel-local-proxy-password@";
const MATERIAL_COUNT: usize = 4;
const MAX_CONFIG_BYTES: usize = 2 * 1024 * 1024;
const MAX_MATERIAL_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum AndroidRuntimePhase {
    Stopped,
    Starting,
    Ready,
    Stopping,
    Failed,
}

impl AndroidRuntimePhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Stopped => "stopped",
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Stopping => "stopping",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug)]
struct SharedStatus {
    state: Mutex<StatusSnapshot>,
    changed: Condvar,
}

#[derive(Debug, Clone)]
struct StatusSnapshot {
    phase: AndroidRuntimePhase,
    error: Option<String>,
}

impl SharedStatus {
    fn starting() -> Self {
        Self {
            state: Mutex::new(StatusSnapshot {
                phase: AndroidRuntimePhase::Starting,
                error: None,
            }),
            changed: Condvar::new(),
        }
    }

    fn snapshot(&self) -> StatusSnapshot {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn set(&self, phase: AndroidRuntimePhase, error: Option<String>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.phase = phase;
        state.error = error;
        self.changed.notify_all();
    }

    /// Publishes listener readiness only while startup still owns the state.
    /// A concurrent stop, timeout, or terminal worker result always wins.
    fn transition_to_ready(&self) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.phase != AndroidRuntimePhase::Starting {
            return false;
        }
        state.phase = AndroidRuntimePhase::Ready;
        state.error = None;
        self.changed.notify_all();
        true
    }

    /// Atomically enters cooperative teardown without overwriting a terminal
    /// result published by the runtime worker.
    fn transition_to_stopping(&self) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(
            state.phase,
            AndroidRuntimePhase::Starting | AndroidRuntimePhase::Ready
        ) {
            return false;
        }
        state.phase = AndroidRuntimePhase::Stopping;
        state.error = None;
        self.changed.notify_all();
        true
    }
}

#[derive(Debug)]
struct ActiveRuntime {
    control: RuntimeHostControl,
    status: Arc<SharedStatus>,
    worker: Option<JoinHandle<()>>,
    profile_directory: PathBuf,
}

#[derive(Debug, Default)]
struct AndroidBridge {
    active: Option<ActiveRuntime>,
    start_reserved: bool,
    last_error: Option<String>,
    last_stats: RuntimeHostStats,
}

fn bridge() -> &'static Mutex<AndroidBridge> {
    static BRIDGE: OnceLock<Mutex<AndroidBridge>> = OnceLock::new();
    BRIDGE.get_or_init(|| Mutex::new(AndroidBridge::default()))
}

#[derive(Debug)]
struct AndroidSocketProtector {
    callback: Global<JObject<'static>>,
}

impl HostSocketProtector for AndroidSocketProtector {
    fn protect(
        &self,
        socket: HostSocketHandle<'_>,
        _request: HostSocketProtectionRequest,
    ) -> io::Result<()> {
        let fd = socket.as_raw_fd();
        let protected = JavaVM::singleton()
            .map_err(jni_io_error)?
            .attach_current_thread(|env| -> Result<bool, jni::errors::Error> {
                env.call_method(
                    self.callback.as_obj(),
                    jni_str!("protect"),
                    jni_sig!((fd: jint) -> jboolean),
                    &[JValue::Int(fd)],
                )?
                .into_bool()
            })
            .map_err(jni_io_error)?;
        if protected {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "VpnService.protect rejected an MPTUNNEL egress socket",
            ))
        }
    }
}

fn jni_io_error(error: jni::errors::Error) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, error.to_string())
}

#[derive(Debug)]
struct AndroidBridgeError(String);

impl std::fmt::Display for AndroidBridgeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for AndroidBridgeError {}

impl From<io::Error> for AndroidBridgeError {
    fn from(value: io::Error) -> Self {
        Self(value.to_string())
    }
}

impl From<jni::errors::Error> for AndroidBridgeError {
    fn from(value: jni::errors::Error) -> Self {
        Self(value.to_string())
    }
}

fn bridge_error(message: impl Into<String>) -> AndroidBridgeError {
    AndroidBridgeError(message.into())
}

fn validate_profile_id(profile_id: &str) -> Result<(), AndroidBridgeError> {
    if !(1..=80).contains(&profile_id.len())
        || !profile_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(bridge_error(
            "profile id must contain 1..80 ASCII letters, digits, '-' or '_'",
        ));
    }
    Ok(())
}

fn private_profile_directory(
    no_backup_root: &str,
    profile_id: &str,
) -> Result<PathBuf, AndroidBridgeError> {
    validate_profile_id(profile_id)?;
    let root = Path::new(no_backup_root);
    if !root.is_absolute()
        || root
            .components()
            .any(|part| matches!(part, Component::ParentDir))
    {
        return Err(bridge_error(
            "noBackupFilesDir must be an absolute normalized path",
        ));
    }
    Ok(root.join(PROFILE_DIRECTORY).join(profile_id))
}

fn materialize_profile(
    profile_directory: &Path,
    config_template: &str,
    materials: [&[u8]; MATERIAL_COUNT],
) -> Result<PathBuf, AndroidBridgeError> {
    if config_template.len() > MAX_CONFIG_BYTES {
        return Err(bridge_error("configuration template exceeds 2 MiB"));
    }
    for (name, material) in [
        ("credential", materials[0]),
        ("pinned certificate", materials[1]),
    ] {
        if material.is_empty() {
            return Err(bridge_error(format!("{name} content is empty")));
        }
        if material.len() > MAX_MATERIAL_BYTES {
            return Err(bridge_error(format!("{name} content exceeds 1 MiB")));
        }
    }
    for (name, material) in [
        ("transport secret", materials[2]),
        ("local proxy password", materials[3]),
    ] {
        if material.len() > MAX_MATERIAL_BYTES {
            return Err(bridge_error(format!("{name} content exceeds 1 MiB")));
        }
    }
    if !materials[2].is_empty() && materials[2].len() != 32 {
        return Err(bridge_error(
            "transport secret must contain exactly 32 raw bytes",
        ));
    }
    for token in [CREDENTIAL_TOKEN, CERTIFICATE_TOKEN] {
        if config_template.matches(token).count() != 1 {
            return Err(bridge_error(format!(
                "configuration template must contain reserved token {token:?} exactly once"
            )));
        }
    }
    for (token, material) in [
        (TRANSPORT_SECRET_TOKEN, materials[2]),
        (LOCAL_PROXY_PASSWORD_TOKEN, materials[3]),
    ] {
        let expected = usize::from(!material.is_empty());
        if config_template.matches(token).count() != expected {
            return Err(bridge_error(format!(
                "reserved token {token:?} must occur exactly once when its material is present and zero times otherwise"
            )));
        }
    }

    fs::create_dir_all(profile_directory)?;
    set_private_directory_permissions(profile_directory)?;
    let config = config_template
        .replace(CREDENTIAL_TOKEN, CREDENTIAL_FILE)
        .replace(CERTIFICATE_TOKEN, CERTIFICATE_FILE)
        .replace(TRANSPORT_SECRET_TOKEN, TRANSPORT_SECRET_FILE)
        .replace(LOCAL_PROXY_PASSWORD_TOKEN, LOCAL_PROXY_PASSWORD_FILE);
    if config.contains("@mptunnel-") {
        return Err(bridge_error(
            "configuration template contains an unresolved MPTUNNEL token",
        ));
    }
    let document: toml::Value =
        toml::from_str(&config).map_err(|error| bridge_error(error.to_string()))?;
    let mut allowed_files = vec![CREDENTIAL_FILE, CERTIFICATE_FILE];
    if !materials[2].is_empty() {
        allowed_files.push(TRANSPORT_SECRET_FILE);
    }
    if !materials[3].is_empty() {
        allowed_files.push(LOCAL_PROXY_PASSWORD_FILE);
    }
    validate_confined_file_references(&document, &allowed_files)?;
    atomic_private_write(&profile_directory.join(CREDENTIAL_FILE), materials[0])?;
    atomic_private_write(&profile_directory.join(CERTIFICATE_FILE), materials[1])?;
    if !materials[2].is_empty() {
        atomic_private_write(&profile_directory.join(TRANSPORT_SECRET_FILE), materials[2])?;
    }
    if !materials[3].is_empty() {
        atomic_private_write(
            &profile_directory.join(LOCAL_PROXY_PASSWORD_FILE),
            materials[3],
        )?;
    }
    let config_path = profile_directory.join(CONFIG_FILE);
    atomic_private_write(&config_path, config.as_bytes())?;
    Ok(config_path)
}

fn validate_confined_file_references(
    value: &toml::Value,
    allowed_files: &[&str],
) -> Result<(), AndroidBridgeError> {
    match value {
        toml::Value::Array(values) => {
            for value in values {
                validate_confined_file_references(value, allowed_files)?;
            }
        }
        toml::Value::Table(table) => {
            if table.get("from").and_then(toml::Value::as_str) == Some("file") {
                let path = table
                    .get("path")
                    .and_then(toml::Value::as_str)
                    .ok_or_else(|| bridge_error("file material reference is missing its path"))?;
                validate_private_material_basename(path, allowed_files)?;
            }
            for (key, value) in table {
                if key == "file" {
                    return Err(bridge_error(
                        "Android templates may not configure host filesystem outputs or rule-set files",
                    ));
                }
                if key.ends_with("_file") {
                    let path = value.as_str().ok_or_else(|| {
                        bridge_error(format!("material field {key:?} must be a string"))
                    })?;
                    validate_private_material_basename(path, allowed_files)?;
                }
                validate_confined_file_references(value, allowed_files)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_private_material_basename(
    path: &str,
    allowed_files: &[&str],
) -> Result<(), AndroidBridgeError> {
    if !allowed_files.contains(&path) {
        return Err(bridge_error(
            "Android templates may reference only bridge-managed material tokens",
        ));
    }
    Ok(())
}

fn atomic_private_write(path: &Path, contents: &[u8]) -> io::Result<()> {
    let temporary = path.with_extension("tmp");
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    file.write_all(contents)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, path)?;
    Ok(())
}

fn set_private_directory_permissions(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Some(parent) = path.parent() {
            fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
        }
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn clean_profile_materials(profile_directory: &Path) {
    for file in [
        CONFIG_FILE,
        CREDENTIAL_FILE,
        CERTIFICATE_FILE,
        TRANSPORT_SECRET_FILE,
        LOCAL_PROXY_PASSWORD_FILE,
    ] {
        let _ = fs::remove_file(profile_directory.join(file));
        let _ = fs::remove_file(profile_directory.join(file).with_extension("tmp"));
    }
    let _ = fs::remove_dir(profile_directory);
}

fn reserve_start() -> Result<(), AndroidBridgeError> {
    let mut global = bridge()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    reap_finished_runtime(&mut global);
    if global.active.is_some() || global.start_reserved {
        return Err(bridge_error("an MPTUNNEL runtime is already active"));
    }
    global.start_reserved = true;
    Ok(())
}

fn cancel_start_reservation() {
    bridge()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .start_reserved = false;
}

fn duration_from_java(value: jlong, name: &str) -> Result<Duration, AndroidBridgeError> {
    if !(1..=120_000).contains(&value) {
        return Err(bridge_error(format!(
            "{name} must be between 1 and 120000 milliseconds"
        )));
    }
    Ok(Duration::from_millis(value as u64))
}

fn start_runtime(
    profile_directory: PathBuf,
    config_path: PathBuf,
    protector: AndroidSocketProtector,
    ready_timeout: Duration,
) -> Result<bool, AndroidBridgeError> {
    let config = load_config_toml(&config_path).map_err(|error| bridge_error(error.to_string()))?;
    // Configuration loading eagerly resolves every referenced material into
    // owned runtime values. Remove plaintext files before the worker starts;
    // the live generation no longer reads them.
    clean_profile_materials(&profile_directory);
    let control = RuntimeHostControl::for_config(&config);
    let status = Arc::new(SharedStatus::starting());

    {
        let mut global = bridge()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !global.start_reserved || global.active.is_some() {
            global.start_reserved = false;
            return Err(bridge_error("MPTUNNEL start reservation was lost"));
        }
        let worker_control = control.clone();
        let worker_status = status.clone();
        let worker = match std::thread::Builder::new()
            .name("mptunnel-android".to_string())
            .spawn(move || {
                run_runtime_thread(config, protector, worker_control, worker_status);
            }) {
            Ok(worker) => worker,
            Err(error) => {
                global.start_reserved = false;
                return Err(bridge_error(format!(
                    "failed to spawn runtime thread: {error}"
                )));
            }
        };
        global.active = Some(ActiveRuntime {
            control: control.clone(),
            status: status.clone(),
            worker: Some(worker),
            profile_directory: profile_directory.clone(),
        });
        global.start_reserved = false;
        global.last_error = None;
    }

    let deadline = Instant::now() + ready_timeout;
    let mut snapshot = status
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    loop {
        match snapshot.phase {
            AndroidRuntimePhase::Ready => return Ok(true),
            AndroidRuntimePhase::Failed | AndroidRuntimePhase::Stopped => {
                return Err(bridge_error(snapshot.error.clone().unwrap_or_else(|| {
                    "runtime stopped before listener readiness".to_string()
                })));
            }
            AndroidRuntimePhase::Starting | AndroidRuntimePhase::Stopping => {}
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            drop(snapshot);
            control.request_shutdown();
            status.transition_to_stopping();
            return Err(bridge_error("runtime listener readiness timed out"));
        }
        let (next, _) = status
            .changed
            .wait_timeout(snapshot, remaining)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        snapshot = next;
    }
}

fn run_runtime_thread(
    config: crate::config::AppConfig,
    protector: AndroidSocketProtector,
    control: RuntimeHostControl,
    status: Arc<SharedStatus>,
) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("mptunnel-worker")
        .build();
    let result = match runtime {
        Ok(runtime) => runtime.block_on(async {
            let ready_control = control.clone();
            let ready_status = status.clone();
            let readiness = tokio::spawn(async move {
                match ready_control.wait_until_ready().await {
                    Ok(()) => {
                        ready_status.transition_to_ready();
                    }
                    Err(error) if ready_control.phase() != RuntimeHostPhase::Stopping => {
                        ready_status.set(AndroidRuntimePhase::Failed, Some(error.to_string()));
                    }
                    Err(_) => {}
                }
            });
            let result = run_with_vpn_host_providers_and_control(
                config,
                Arc::new(SystemPacketDeviceProvider),
                Arc::new(SystemCarrierNetworkProvider),
                Arc::new(protector),
                control.clone(),
            )
            .await;
            readiness.abort();
            result
        }),
        Err(error) => Err(crate::runtime::RuntimeError::Io(error)),
    };

    match result {
        Ok(()) => status.set(AndroidRuntimePhase::Stopped, None),
        Err(error) => status.set(AndroidRuntimePhase::Failed, Some(error.to_string())),
    }
}

fn reap_finished_runtime(global: &mut AndroidBridge) {
    let finished = global
        .active
        .as_ref()
        .and_then(|active| active.worker.as_ref())
        .is_some_and(JoinHandle::is_finished);
    if !finished {
        return;
    }
    let Some(mut active) = global.active.take() else {
        return;
    };
    global.last_stats = active.control.stats();
    let snapshot = active.status.snapshot();
    global.last_error = snapshot.error;
    if let Some(worker) = active.worker.take() {
        let _ = worker.join();
    }
    clean_profile_materials(&active.profile_directory);
}

fn stop_runtime(timeout: Duration) -> bool {
    let (control, status) = {
        let mut global = bridge()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reap_finished_runtime(&mut global);
        let Some(active) = global.active.as_ref() else {
            return true;
        };
        active.status.transition_to_stopping();
        active.control.request_shutdown();
        (active.control.clone(), active.status.clone())
    };

    let deadline = Instant::now() + timeout;
    let mut snapshot = status
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    while !matches!(
        snapshot.phase,
        AndroidRuntimePhase::Stopped | AndroidRuntimePhase::Failed
    ) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return false;
        }
        let (next, wait) = status
            .changed
            .wait_timeout(snapshot, remaining)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        snapshot = next;
        if wait.timed_out()
            && !matches!(
                snapshot.phase,
                AndroidRuntimePhase::Stopped | AndroidRuntimePhase::Failed
            )
        {
            return false;
        }
    }
    drop(snapshot);
    let mut global = bridge()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(mut active) = global.active.take() else {
        return true;
    };
    global.last_stats = control.stats();
    global.last_error = active.status.snapshot().error;
    let joined = active
        .worker
        .take()
        .is_none_or(|worker| worker.join().is_ok());
    clean_profile_materials(&active.profile_directory);
    joined
}

fn jstring(env: &Env<'_>, value: &JString<'_>, name: &str) -> Result<String, AndroidBridgeError> {
    if value.is_null() {
        return Err(bridge_error(format!("{name} must not be null")));
    }
    value.try_to_string(env).map_err(AndroidBridgeError::from)
}

fn material_array(
    env: &mut Env<'_>,
    values: &JObjectArray<'_, JByteArray<'_>>,
) -> Result<[Vec<u8>; MATERIAL_COUNT], AndroidBridgeError> {
    if values.len(env)? != MATERIAL_COUNT {
        return Err(bridge_error(
            "materials must contain exactly four byte arrays",
        ));
    }
    let mut result: [Vec<u8>; MATERIAL_COUNT] = std::array::from_fn(|_| Vec::new());
    for (index, slot) in result.iter_mut().enumerate() {
        let value = values.get_element(env, index)?;
        *slot = env.convert_byte_array(&value)?;
    }
    Ok(result)
}

#[derive(Serialize)]
struct AndroidStats<'a> {
    state: &'a str,
    error: Option<&'a str>,
    #[serde(flatten)]
    runtime: RuntimeHostStats,
}

fn state_and_stats() -> (StatusSnapshot, RuntimeHostStats) {
    let mut global = bridge()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    reap_finished_runtime(&mut global);
    match &global.active {
        Some(active) => (active.status.snapshot(), active.control.stats()),
        None if global.last_error.is_some() => (
            StatusSnapshot {
                phase: AndroidRuntimePhase::Failed,
                error: global.last_error.clone(),
            },
            global.last_stats,
        ),
        None => (
            StatusSnapshot {
                phase: AndroidRuntimePhase::Stopped,
                error: None,
            },
            global.last_stats,
        ),
    }
}

#[jni_mangle("com.v2ray.ang.mpp.MptunnelNative", "nativeStart")]
pub fn native_start<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    no_backup_root: JString<'caller>,
    profile_id: JString<'caller>,
    config_template: JString<'caller>,
    materials: JObjectArray<'caller, JByteArray<'caller>>,
    protector: JObject<'caller>,
    ready_timeout_ms: jlong,
) -> jboolean {
    unowned_env
        .with_env(|env| -> Result<jboolean, AndroidBridgeError> {
            if protector.is_null() {
                return Err(bridge_error("socket protector must not be null"));
            }
            let root = jstring(env, &no_backup_root, "noBackupFilesDir")?;
            let profile_id = jstring(env, &profile_id, "profile id")?;
            let template = jstring(env, &config_template, "configuration template")?;
            let values = material_array(env, &materials)?;
            let profile_directory = private_profile_directory(&root, &profile_id)?;
            reserve_start()?;
            let result = (|| {
                clean_profile_materials(&profile_directory);
                let config_path = materialize_profile(
                    &profile_directory,
                    &template,
                    [&values[0], &values[1], &values[2], &values[3]],
                )?;
                let callback = env.new_global_ref(&protector)?;
                start_runtime(
                    profile_directory.clone(),
                    config_path,
                    AndroidSocketProtector { callback },
                    duration_from_java(ready_timeout_ms, "ready timeout")?,
                )
            })();
            if result.is_err() {
                cancel_start_reservation();
                clean_profile_materials(&profile_directory);
            }
            result
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[jni_mangle("com.v2ray.ang.mpp.MptunnelNative", "nativeStop")]
pub fn native_stop<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    timeout_ms: jlong,
) -> jboolean {
    unowned_env
        .with_env(|_env| -> Result<jboolean, AndroidBridgeError> {
            Ok(jboolean::from(stop_runtime(duration_from_java(
                timeout_ms,
                "stop timeout",
            )?)))
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[jni_mangle("com.v2ray.ang.mpp.MptunnelNative", "nativeIsRunning")]
pub fn native_is_running<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
) -> jboolean {
    unowned_env
        .with_env(|_env| -> Result<jboolean, AndroidBridgeError> {
            Ok(jboolean::from(
                state_and_stats().0.phase == AndroidRuntimePhase::Ready,
            ))
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[jni_mangle("com.v2ray.ang.mpp.MptunnelNative", "nativeState")]
pub fn native_state<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
) -> JString<'caller> {
    unowned_env
        .with_env(|env| -> Result<JString<'caller>, AndroidBridgeError> {
            Ok(env.new_string(state_and_stats().0.phase.as_str())?)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[jni_mangle("com.v2ray.ang.mpp.MptunnelNative", "nativeVersion")]
pub fn native_version<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
) -> JString<'caller> {
    unowned_env
        .with_env(|env| -> Result<JString<'caller>, AndroidBridgeError> {
            Ok(env.new_string(env!("CARGO_PKG_VERSION"))?)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[jni_mangle("com.v2ray.ang.mpp.MptunnelNative", "nativeStatsJson")]
pub fn native_stats_json<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
) -> JString<'caller> {
    unowned_env
        .with_env(|env| -> Result<JString<'caller>, AndroidBridgeError> {
            let (status, runtime) = state_and_stats();
            let json = serde_json::to_string(&AndroidStats {
                state: status.phase.as_str(),
                error: status.error.as_deref(),
                runtime,
            })
            .map_err(|error| bridge_error(error.to_string()))?;
            Ok(env.new_string(json)?)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[jni_mangle("com.v2ray.ang.mpp.MptunnelNative", "nativeDeleteProfile")]
pub fn native_delete_profile<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    no_backup_root: JString<'caller>,
    profile_id: JString<'caller>,
) -> jboolean {
    unowned_env
        .with_env(|env| -> Result<jboolean, AndroidBridgeError> {
            let root = jstring(env, &no_backup_root, "noBackupFilesDir")?;
            let profile_id = jstring(env, &profile_id, "profile id")?;
            let directory = private_profile_directory(&root, &profile_id)?;
            {
                let global = bridge()
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if global.active.as_ref().is_some_and(|active| {
                    active.profile_directory.as_os_str() == directory.as_os_str()
                }) {
                    return Err(bridge_error("cannot delete the active profile"));
                }
            }
            clean_profile_materials(&directory);
            Ok(jboolean::from(!directory.exists()))
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE_TEMPLATE: &str = r#"
[[credentials]]
credential_id = "profile"
principal_id = "profile"
secret = { from = "file", path = "@mptunnel-profile-credential@" }

[[inbounds]]
name = "local"
protocol = "mixed"
listen = ["127.0.0.1:1080"]

[[outbounds]]
name = "remote"
protocol = "mpp"
paths = [{ name = "primary", endpoint = "tcp://127.0.0.1:7443" }]

[outbounds.security]
credential_id = "profile"
tls_pinned_certificate_file = "@mptunnel-profile-certificate@"

[routing]
[[routing.rules]]
name = "default"
action = "outbound"
outbound = "remote"
"#;

    fn test_directory(name: &str) -> PathBuf {
        std::env::temp_dir()
            .join(format!(
                "mptunnel-android-{name}-{}-{}",
                std::process::id(),
                std::thread::current().name().unwrap_or("test")
            ))
            .join(PROFILE_DIRECTORY)
            .join("profile")
    }

    fn materialize(
        directory: &Path,
        template: &str,
        transport: &[u8],
        proxy_password: &[u8],
    ) -> Result<PathBuf, AndroidBridgeError> {
        materialize_profile(
            directory,
            template,
            [b"credential", b"certificate", transport, proxy_password],
        )
    }

    #[test]
    fn substitutes_fixed_relative_material_names_and_cleans_files() {
        let directory = test_directory("substitution");
        clean_profile_materials(&directory);
        let config = materialize(&directory, BASE_TEMPLATE, &[], &[]).expect("materialize");
        let contents = fs::read_to_string(&config).expect("read generated config");
        assert!(contents.contains(r#"path = "credential.key""#));
        assert!(contents.contains(r#"tls_pinned_certificate_file = "pinned-certificate.pem""#));
        assert!(!contents.contains("@mptunnel-"));
        assert!(directory.join(CREDENTIAL_FILE).is_file());
        assert!(directory.join(CERTIFICATE_FILE).is_file());
        assert!(!directory.join(TRANSPORT_SECRET_FILE).exists());
        clean_profile_materials(&directory);
        assert!(!directory.exists());
        let _ = fs::remove_dir(directory.parent().expect("profile parent"));
        let _ = fs::remove_dir(
            directory
                .parent()
                .and_then(Path::parent)
                .expect("temporary root"),
        );
    }

    #[test]
    fn optional_token_presence_must_match_material_presence() {
        let directory = test_directory("optional-presence");
        clean_profile_materials(&directory);
        let with_transport_token =
            format!("{BASE_TEMPLATE}\ntransport_secret_file = {TRANSPORT_SECRET_TOKEN:?}\n");
        assert!(materialize(&directory, &with_transport_token, &[], &[]).is_err());
        assert!(materialize(&directory, BASE_TEMPLATE, &[7; 32], &[]).is_err());

        let with_password = format!(
            "[[local_users]]\nusername = \"user\"\npassword = {{ from = \"file\", path = {LOCAL_PROXY_PASSWORD_TOKEN:?} }}\n{BASE_TEMPLATE}"
        );
        materialize(&directory, &with_password, &[], b"password").expect("optional password");
        assert!(directory.join(LOCAL_PROXY_PASSWORD_FILE).is_file());
        clean_profile_materials(&directory);
    }

    #[test]
    fn rejects_duplicate_tokens_unresolved_tokens_and_external_paths() {
        let directory = test_directory("rejection");
        clean_profile_materials(&directory);
        let duplicate = BASE_TEMPLATE.replace(
            CREDENTIAL_TOKEN,
            &format!("{CREDENTIAL_TOKEN}{CREDENTIAL_TOKEN}"),
        );
        assert!(materialize(&directory, &duplicate, &[], &[]).is_err());

        let unresolved = format!("{BASE_TEMPLATE}\nunknown = \"@mptunnel-unknown@\"\n");
        assert!(materialize(&directory, &unresolved, &[], &[]).is_err());

        let external = format!("{BASE_TEMPLATE}\n[management]\ntoken_file = \"/tmp/token\"\n");
        assert!(materialize(&directory, &external, &[], &[]).is_err());

        let logging_file = BASE_TEMPLATE.replace(
            "[[credentials]]",
            "[logging]\nfile = \"/tmp/mptunnel.log\"\n\n[[credentials]]",
        );
        assert!(materialize(&directory, &logging_file, &[], &[]).is_err());

        let rule_set_file = format!(
            "{BASE_TEMPLATE}\n[[routing.rule_sets]]\nname = \"external\"\nfile = \"/tmp/rules.json\"\n"
        );
        assert!(materialize(&directory, &rule_set_file, &[], &[]).is_err());
        clean_profile_materials(&directory);
    }

    #[test]
    fn validates_profile_identity_and_private_root_shape() {
        assert!(validate_profile_id("profile_01-test").is_ok());
        assert!(validate_profile_id("../escape").is_err());
        assert!(private_profile_directory("relative", "profile").is_err());
        assert!(private_profile_directory("/private/no-backup", "../escape").is_err());
        assert_eq!(
            private_profile_directory("/private/no-backup", "profile").expect("private path"),
            Path::new("/private/no-backup/mptunnel/profile")
        );
    }

    #[test]
    fn stopping_transition_preserves_terminal_status_atomically() {
        for terminal in [AndroidRuntimePhase::Stopped, AndroidRuntimePhase::Failed] {
            let status = SharedStatus::starting();
            let error = (terminal == AndroidRuntimePhase::Failed).then(|| "failure".to_string());
            status.set(terminal, error.clone());
            assert!(!status.transition_to_stopping());
            let snapshot = status.snapshot();
            assert_eq!(snapshot.phase, terminal);
            assert_eq!(snapshot.error, error);
        }

        let status = SharedStatus::starting();
        assert!(status.transition_to_stopping());
        assert_eq!(status.snapshot().phase, AndroidRuntimePhase::Stopping);
        assert!(!status.transition_to_stopping());
    }

    #[test]
    fn readiness_transition_cannot_overwrite_stopping_or_terminal_status() {
        for phase in [
            AndroidRuntimePhase::Stopping,
            AndroidRuntimePhase::Stopped,
            AndroidRuntimePhase::Failed,
        ] {
            let status = SharedStatus::starting();
            status.set(phase, None);
            assert!(!status.transition_to_ready());
            assert_eq!(status.snapshot().phase, phase);
        }

        let status = SharedStatus::starting();
        assert!(status.transition_to_ready());
        assert_eq!(status.snapshot().phase, AndroidRuntimePhase::Ready);
        assert!(!status.transition_to_ready());
    }

    #[test]
    fn concurrent_stop_always_wins_over_readiness() {
        for _ in 0..128 {
            let status = Arc::new(SharedStatus::starting());
            let barrier = Arc::new(std::sync::Barrier::new(3));
            let ready_status = status.clone();
            let ready_barrier = barrier.clone();
            let ready = std::thread::spawn(move || {
                ready_barrier.wait();
                ready_status.transition_to_ready();
            });
            let stopping_status = status.clone();
            let stopping_barrier = barrier.clone();
            let stopping = std::thread::spawn(move || {
                stopping_barrier.wait();
                stopping_status.transition_to_stopping();
            });
            barrier.wait();
            ready.join().expect("readiness transition");
            stopping.join().expect("stopping transition");
            assert_eq!(status.snapshot().phase, AndroidRuntimePhase::Stopping);
        }
    }

    #[test]
    fn concurrent_failure_cannot_be_lost_to_stopping_transition() {
        for _ in 0..128 {
            let status = Arc::new(SharedStatus::starting());
            let barrier = Arc::new(std::sync::Barrier::new(3));
            let failing_status = status.clone();
            let failing_barrier = barrier.clone();
            let failing = std::thread::spawn(move || {
                failing_barrier.wait();
                failing_status.set(AndroidRuntimePhase::Failed, Some("failure".to_string()));
            });
            let stopping_status = status.clone();
            let stopping_barrier = barrier.clone();
            let stopping = std::thread::spawn(move || {
                stopping_barrier.wait();
                stopping_status.transition_to_stopping();
            });
            barrier.wait();
            failing.join().expect("failure publisher");
            stopping.join().expect("stopping transition");
            let snapshot = status.snapshot();
            assert_eq!(snapshot.phase, AndroidRuntimePhase::Failed);
            assert_eq!(snapshot.error.as_deref(), Some("failure"));
        }
    }
}
