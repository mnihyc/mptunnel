//! Android JNI host for one embedded MPTUNNEL generation.
//!
//! The Java/Kotlin facade supplies one finalized, self-contained TOML document.
//! This module rejects host-file material references before compiling that
//! document entirely in memory, then owns the runtime thread,
//! listener-readiness barrier, and synchronous `VpnService.protect(int)`
//! callback.

use crate::config::load_config_toml_str;
use crate::platform::SystemPacketDeviceProvider;
use crate::runtime::{
    RuntimeHostControl, RuntimeHostPhase, RuntimeHostStats, run_with_vpn_host_providers_and_control,
};
use crate::transport::{
    HostSocketHandle, HostSocketProtectionRequest, HostSocketProtector,
    SystemCarrierNetworkProvider,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use jni::errors::ThrowRuntimeExAndDefault;
use jni::objects::{Global, JClass, JObject, JString};
use jni::sys::{jboolean, jlong};
use jni::{Env, EnvUnowned, JValue, JavaVM, jni_mangle, jni_sig, jni_str};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use toml_edit::{Array, ArrayOfTables, DocumentMut, InlineTable, Item, Key, Table, Value};

const MAX_CONFIG_BYTES: usize = 2 * 1024 * 1024;
const MANAGED_PLACEHOLDER_PREFIX: &str = "@mptunnel-";
const MAX_MATERIAL_BYTES: usize = 1024 * 1024;
const SOCKS_PORT_TOKEN: &str = "@mptunnel-socks-port@";
const LOCAL_USER_DEFINITION_MARKER: &str = "# @mptunnel-local-user-definition@";
const LOCAL_USER_BINDING_MARKER: &str = "# @mptunnel-local-user-binding@";
const LEGACY_CREDENTIAL_TOKEN: &str = "@mptunnel-profile-credential@";
const LEGACY_CERTIFICATE_TOKEN: &str = "@mptunnel-profile-certificate@";
const LEGACY_TRANSPORT_SECRET_TOKEN: &str = "@mptunnel-profile-transport-secret@";
const MANAGED_CREDENTIAL_ID: &str = "credential";
const MANAGED_PINNED_CERTIFICATE_ID: &str = "pinned-certificate";
const MANAGED_TRANSPORT_SECRET_ID: &str = "transport-secret";
const ANDROID_LOCAL_USER_ID: &str = "v2rayng-local";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EditorProjection {
    schema_version: u8,
    paths: Vec<EditorPath>,
    advanced: Option<EditorAdvanced>,
    credential_id: String,
    principal_id: String,
    tls_server_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EditorPath {
    name: String,
    endpoint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EditorAdvanced {
    path_probe_interval_ms: i64,
    path_probe_timeout_ms: i64,
    extra_traffic_hint_percent: i64,
    auth_freshness_window_seconds: i64,
    session_retention_timeout_ms: i64,
    tcp_heartbeat_interval_ms: i64,
    tcp_heartbeat_timeout_ms: i64,
    quic_keep_alive_interval_ms: i64,
    quic_idle_timeout_ms: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FinalizeBindings {
    schema_version: u8,
    socks_port: u16,
    credential_base64: String,
    pinned_certificate_base64: String,
    transport_secret_base64: Option<String>,
    local_auth: Option<FinalizeLocalAuth>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FinalizeLocalAuth {
    username: String,
    password_base64: String,
}

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

fn compile_inline_config(contents: &str) -> Result<crate::config::AppConfig, AndroidBridgeError> {
    if contents.len() > MAX_CONFIG_BYTES {
        return Err(bridge_error("configuration document exceeds 2 MiB"));
    }
    let document = toml::from_str::<toml::Value>(contents)
        .map_err(|_| bridge_error("configuration document is invalid"))?;
    validate_inline_document(&document)?;
    load_config_toml_str(contents).map_err(|error| bridge_error(error.to_string()))
}

fn validate_inline_document(value: &toml::Value) -> Result<(), AndroidBridgeError> {
    match value {
        toml::Value::Array(values) => {
            for value in values {
                validate_inline_document(value)?;
            }
        }
        toml::Value::Table(table) => {
            if matches!(
                table.get("from").and_then(toml::Value::as_str),
                Some("file" | "env" | "environment" | "managed")
            ) {
                return Err(bridge_error(
                    "Android configurations may not contain external or unresolved managed material",
                ));
            }
            for (key, value) in table {
                if key == "file" {
                    return Err(bridge_error(
                        "Android configurations may not configure logging output or rule-set files",
                    ));
                }
                if key.ends_with("_file") {
                    return Err(bridge_error(format!(
                        "Android configurations may not use legacy file material field {key:?}"
                    )));
                }
                validate_inline_document(value)?;
            }
        }
        toml::Value::String(value) if value.contains(MANAGED_PLACEHOLDER_PREFIX) => {
            return Err(bridge_error(
                "configuration document contains an unresolved managed placeholder",
            ));
        }
        _ => {}
    }
    Ok(())
}

fn parse_editor_document(contents: &str) -> Result<DocumentMut, AndroidBridgeError> {
    if contents.len() > MAX_CONFIG_BYTES {
        return Err(bridge_error("configuration document exceeds 2 MiB"));
    }
    normalize_legacy_marker_lines(contents)
        .parse::<DocumentMut>()
        .map_err(|_| bridge_error("editable configuration document is invalid"))
}

fn normalize_legacy_marker_lines(contents: &str) -> String {
    let legacy_definition = LOCAL_USER_DEFINITION_MARKER.trim_start_matches("# ");
    let legacy_binding = LOCAL_USER_BINDING_MARKER.trim_start_matches("# ");
    let mut normalized = String::with_capacity(contents.len() + 4);
    for line in contents.split_inclusive('\n') {
        let body = line.trim_end_matches(['\r', '\n']);
        let ending = &line[body.len()..];
        match body.trim() {
            marker if marker == legacy_definition => {
                normalized.push_str(LOCAL_USER_DEFINITION_MARKER);
                normalized.push_str(ending);
            }
            marker if marker == legacy_binding => {
                normalized.push_str(LOCAL_USER_BINDING_MARKER);
                normalized.push_str(ending);
            }
            _ => normalized.push_str(line),
        }
    }
    normalized
}

fn mpp_outbound_index(document: &DocumentMut) -> Result<usize, AndroidBridgeError> {
    document
        .get("outbounds")
        .and_then(Item::as_array_of_tables)
        .ok_or_else(|| bridge_error("editable configuration has no outbound table"))?
        .iter()
        .position(|outbound| outbound.get("protocol").and_then(Item::as_str) == Some("mpp"))
        .ok_or_else(|| bridge_error("editable configuration has no MPP outbound"))
}

fn guided_mpp_outbound_index(document: &DocumentMut) -> Result<usize, AndroidBridgeError> {
    let outbounds = document
        .get("outbounds")
        .and_then(Item::as_array_of_tables)
        .ok_or_else(|| bridge_error("guided editor requires exactly one MPP outbound"))?;
    let mut matches = outbounds
        .iter()
        .enumerate()
        .filter_map(|(index, outbound)| {
            (outbound.get("protocol").and_then(Item::as_str) == Some("mpp")).then_some(index)
        });
    let index = matches
        .next()
        .ok_or_else(|| bridge_error("guided editor requires exactly one MPP outbound"))?;
    if matches.next().is_some() {
        return Err(bridge_error(
            "guided editor requires exactly one MPP outbound",
        ));
    }
    Ok(index)
}

fn mpp_outbound(document: &DocumentMut, index: usize) -> Result<&Table, AndroidBridgeError> {
    document
        .get("outbounds")
        .and_then(Item::as_array_of_tables)
        .and_then(|outbounds| outbounds.get(index))
        .ok_or_else(|| bridge_error("editable MPP outbound is unavailable"))
}

fn mpp_outbound_mut(
    document: &mut DocumentMut,
    index: usize,
) -> Result<&mut Table, AndroidBridgeError> {
    document
        .get_mut("outbounds")
        .and_then(Item::as_array_of_tables_mut)
        .and_then(|outbounds| outbounds.get_mut(index))
        .ok_or_else(|| bridge_error("editable MPP outbound is unavailable"))
}

fn child_table<'a>(table: &'a Table, key: &str) -> Result<&'a Table, AndroidBridgeError> {
    table
        .get(key)
        .and_then(Item::as_table)
        .ok_or_else(|| bridge_error(format!("editable MPP outbound requires [{key}]")))
}

fn referenced_credential_index(
    document: &DocumentMut,
    credential_id: &str,
) -> Result<usize, AndroidBridgeError> {
    let credentials = document
        .get("credentials")
        .and_then(Item::as_array_of_tables)
        .ok_or_else(|| bridge_error("editable configuration has no credential catalog"))?;
    let mut matches = credentials
        .iter()
        .enumerate()
        .filter_map(|(index, credential)| {
            (credential.get("credential_id").and_then(Item::as_str) == Some(credential_id))
                .then_some(index)
        });
    let index = matches
        .next()
        .ok_or_else(|| bridge_error("MPP outbound references an unknown credential"))?;
    if matches.next().is_some() {
        return Err(bridge_error(
            "MPP outbound credential reference is ambiguous",
        ));
    }
    Ok(index)
}

fn credential_table(document: &DocumentMut, index: usize) -> Result<&Table, AndroidBridgeError> {
    document
        .get("credentials")
        .and_then(Item::as_array_of_tables)
        .and_then(|credentials| credentials.get(index))
        .ok_or_else(|| bridge_error("editable credential is unavailable"))
}

fn credential_table_mut(
    document: &mut DocumentMut,
    index: usize,
) -> Result<&mut Table, AndroidBridgeError> {
    document
        .get_mut("credentials")
        .and_then(Item::as_array_of_tables_mut)
        .and_then(|credentials| credentials.get_mut(index))
        .ok_or_else(|| bridge_error("editable credential is unavailable"))
}

fn project_editor(contents: &str) -> Result<EditorProjection, AndroidBridgeError> {
    let document = parse_editor_document(contents)?;
    let outbound_index = guided_mpp_outbound_index(&document)?;
    let outbound = mpp_outbound(&document, outbound_index)?;
    let security = child_table(outbound, "security")?;
    let credential_id = required_string(security, "credential_id")?.to_string();
    let credential_index = referenced_credential_index(&document, &credential_id)?;
    let credential = credential_table(&document, credential_index)?;
    let principal_id = required_string(credential, "principal_id")?.to_string();
    let tls_server_name = security
        .get("tls_server_name")
        .map(|item| {
            item.as_str()
                .map(str::to_string)
                .ok_or_else(|| bridge_error("tls_server_name must be a string"))
        })
        .transpose()?
        .unwrap_or_else(|| crate::config::DEFAULT_MPP_TLS_SERVER_NAME.to_string());
    let paths = project_paths(outbound)?;
    let advanced = project_advanced(&document, outbound, security)?;
    Ok(EditorProjection {
        schema_version: 1,
        paths,
        advanced,
        credential_id,
        principal_id,
        tls_server_name,
    })
}

fn required_string<'a>(table: &'a Table, key: &str) -> Result<&'a str, AndroidBridgeError> {
    table
        .get(key)
        .and_then(Item::as_str)
        .ok_or_else(|| bridge_error(format!("editable field {key:?} must be a string")))
}

fn project_paths(outbound: &Table) -> Result<Vec<EditorPath>, AndroidBridgeError> {
    let paths = outbound
        .get("paths")
        .and_then(Item::as_array)
        .ok_or_else(|| bridge_error("editable MPP outbound requires a paths array"))?;
    paths
        .iter()
        .map(|value| {
            let path = value
                .as_inline_table()
                .ok_or_else(|| bridge_error("editable MPP path must be an inline table"))?;
            let name = path
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| bridge_error("editable MPP path requires a string name"))?;
            let endpoint = path
                .get("endpoint")
                .and_then(Value::as_str)
                .ok_or_else(|| bridge_error("editable MPP path requires a string endpoint"))?;
            Ok(EditorPath {
                name: name.to_string(),
                endpoint: endpoint.to_string(),
            })
        })
        .collect()
}

fn project_advanced(
    document: &DocumentMut,
    outbound: &Table,
    security: &Table,
) -> Result<Option<EditorAdvanced>, AndroidBridgeError> {
    let performance = outbound.get("performance").and_then(Item::as_table);
    let session = document.get("session").and_then(Item::as_table);
    let resources = document.get("resources").and_then(Item::as_table);
    let any_present = [
        outbound.get("path_probe_interval_ms"),
        outbound.get("path_probe_timeout_ms"),
        performance.and_then(|table| table.get("extra_traffic_hint_percent")),
        security.get("auth_freshness_window_seconds"),
        session.and_then(|table| table.get("retention_timeout_ms")),
        resources.and_then(|table| table.get("tcp_path_heartbeat_interval_ms")),
        resources.and_then(|table| table.get("tcp_path_heartbeat_timeout_ms")),
        resources.and_then(|table| table.get("quic_path_keep_alive_interval_ms")),
        resources.and_then(|table| table.get("quic_path_idle_timeout_ms")),
    ]
    .into_iter()
    .any(|item| item.is_some());
    if !any_present {
        return Ok(None);
    }
    Ok(Some(EditorAdvanced {
        path_probe_interval_ms: optional_integer(
            outbound,
            "path_probe_interval_ms",
            crate::config::DEFAULT_PATH_PROBE_INTERVAL_MS as i64,
        )?,
        path_probe_timeout_ms: optional_integer(
            outbound,
            "path_probe_timeout_ms",
            crate::config::DEFAULT_PATH_PROBE_TIMEOUT_MS as i64,
        )?,
        extra_traffic_hint_percent: optional_child_integer(
            performance,
            "extra_traffic_hint_percent",
            crate::config::DEFAULT_EXTRA_TRAFFIC_HINT_PERCENT as i64,
        )?,
        auth_freshness_window_seconds: optional_integer(
            security,
            "auth_freshness_window_seconds",
            crate::config::DEFAULT_AUTH_FRESHNESS_WINDOW_SECONDS as i64,
        )?,
        session_retention_timeout_ms: optional_child_integer(
            session,
            "retention_timeout_ms",
            crate::config::DEFAULT_SESSION_RETENTION_TIMEOUT_MS as i64,
        )?,
        tcp_heartbeat_interval_ms: optional_child_integer(
            resources,
            "tcp_path_heartbeat_interval_ms",
            crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL_MS as i64,
        )?,
        tcp_heartbeat_timeout_ms: optional_child_integer(
            resources,
            "tcp_path_heartbeat_timeout_ms",
            crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT_MS as i64,
        )?,
        quic_keep_alive_interval_ms: optional_child_integer(
            resources,
            "quic_path_keep_alive_interval_ms",
            crate::config::DEFAULT_QUIC_PATH_KEEP_ALIVE_INTERVAL_MS as i64,
        )?,
        quic_idle_timeout_ms: optional_child_integer(
            resources,
            "quic_path_idle_timeout_ms",
            crate::config::DEFAULT_QUIC_PATH_IDLE_TIMEOUT_MS as i64,
        )?,
    }))
}

fn optional_child_integer(
    table: Option<&Table>,
    key: &str,
    default: i64,
) -> Result<i64, AndroidBridgeError> {
    table.map_or(Ok(default), |table| optional_integer(table, key, default))
}

fn optional_integer(table: &Table, key: &str, default: i64) -> Result<i64, AndroidBridgeError> {
    table.get(key).map_or(Ok(default), |item| {
        item.as_integer()
            .ok_or_else(|| bridge_error(format!("editable field {key:?} must be an integer")))
    })
}

fn parse_projection_json(contents: &str) -> Result<EditorProjection, AndroidBridgeError> {
    let projection = serde_json::from_str::<EditorProjection>(contents)
        .map_err(|_| bridge_error("editor projection JSON is invalid"))?;
    if projection.schema_version != 1 {
        return Err(bridge_error("unsupported editor projection schema version"));
    }
    Ok(projection)
}

fn set_table_value(table: &mut Table, key: &str, mut value: Value) {
    if let Some(item) = table.get_mut(key) {
        if let Some(previous) = item.as_value() {
            *value.decor_mut() = previous.decor().clone();
        }
        *item = Item::Value(value);
    } else {
        table.insert(key, Item::Value(value));
    }
}

fn set_inline_value(table: &mut InlineTable, key: &str, mut value: Value) {
    if let Some(previous) = table.get_mut(key) {
        *value.decor_mut() = previous.decor().clone();
        *previous = value;
    } else {
        table.insert(key, value);
    }
}

fn ensure_child_table<'a>(
    table: &'a mut Table,
    key: &str,
) -> Result<&'a mut Table, AndroidBridgeError> {
    if !table.contains_key(key) {
        table.insert(key, Item::Table(Table::new()));
    }
    table
        .get_mut(key)
        .and_then(Item::as_table_mut)
        .ok_or_else(|| bridge_error(format!("editable field {key:?} must be a table")))
}

fn managed_source(id: &str) -> Value {
    let mut source = InlineTable::new();
    source.insert("from", Value::from("managed"));
    source.insert("id", Value::from(id));
    Value::InlineTable(source)
}

fn base64_source(value: &str) -> Value {
    let mut source = InlineTable::new();
    source.insert("from", Value::from("base64"));
    source.insert("value", Value::from(value));
    Value::InlineTable(source)
}

fn is_managed_source(value: &Value, id: &str) -> bool {
    value.as_inline_table().is_some_and(|source| {
        source.get("from").and_then(Value::as_str) == Some("managed")
            && source.get("id").and_then(Value::as_str) == Some(id)
    })
}

fn legacy_source_matches(value: &Value, expected_token: &str) -> bool {
    match value {
        Value::String(value) => value.value() == expected_token,
        Value::InlineTable(source) => {
            source.get("path").and_then(Value::as_str) == Some(expected_token)
                && matches!(
                    source.get("from").and_then(Value::as_str),
                    None | Some("file")
                )
        }
        _ => false,
    }
}

fn canonicalize_table_material(
    table: &mut Table,
    canonical_key: &str,
    legacy_key: Option<&str>,
    legacy_token: &str,
    managed_id: &str,
    insert_when_absent: bool,
) -> Result<(), AndroidBridgeError> {
    let canonical_known = table
        .get(canonical_key)
        .and_then(Item::as_value)
        .is_some_and(|value| {
            is_managed_source(value, managed_id) || legacy_source_matches(value, legacy_token)
        });
    let legacy_known = legacy_key
        .and_then(|key| table.get(key).and_then(Item::as_value))
        .is_some_and(|value| legacy_source_matches(value, legacy_token));
    if legacy_known && table.contains_key(canonical_key) {
        return Err(bridge_error(format!(
            "editable material field {canonical_key:?} is duplicated"
        )));
    }
    if canonical_known || legacy_known || (insert_when_absent && !table.contains_key(canonical_key))
    {
        let prior_decor = table
            .get(canonical_key)
            .and_then(Item::as_value)
            .or_else(|| legacy_key.and_then(|key| table.get(key).and_then(Item::as_value)))
            .map(|value| value.decor().clone());
        if let Some(legacy_key) = legacy_key
            && legacy_known
        {
            table.remove(legacy_key);
        }
        let mut replacement = managed_source(managed_id);
        if let Some(decor) = prior_decor {
            *replacement.decor_mut() = decor;
        }
        table.insert(canonical_key, Item::Value(replacement));
    }
    Ok(())
}

fn canonicalize_inline_material(
    table: &mut InlineTable,
    canonical_key: &str,
    legacy_key: &str,
    legacy_token: &str,
    managed_id: &str,
) -> Result<(), AndroidBridgeError> {
    let canonical_known = table.get(canonical_key).is_some_and(|value| {
        is_managed_source(value, managed_id) || legacy_source_matches(value, legacy_token)
    });
    let legacy_known = table
        .get(legacy_key)
        .is_some_and(|value| legacy_source_matches(value, legacy_token));
    if legacy_known && table.contains_key(canonical_key) {
        return Err(bridge_error(format!(
            "editable material field {canonical_key:?} is duplicated"
        )));
    }
    if canonical_known {
        set_inline_value(table, canonical_key, managed_source(managed_id));
    } else if legacy_known {
        let (legacy_key_format, previous) = table
            .remove_entry(legacy_key)
            .ok_or_else(|| bridge_error("legacy material field became unavailable"))?;
        let key = Key::new(canonical_key)
            .with_leaf_decor(legacy_key_format.leaf_decor().clone())
            .with_dotted_decor(legacy_key_format.dotted_decor().clone());
        let mut replacement = managed_source(managed_id);
        *replacement.decor_mut() = previous.decor().clone();
        table.insert_formatted(&key, replacement);
    }
    Ok(())
}

fn set_authoritative_managed(
    table: &mut Table,
    canonical_key: &str,
    legacy_key: &str,
    managed_id: &str,
) {
    let prior_decor = table
        .get(canonical_key)
        .and_then(Item::as_value)
        .or_else(|| table.get(legacy_key).and_then(Item::as_value))
        .map(|value| value.decor().clone());
    table.remove(legacy_key);
    let mut replacement = managed_source(managed_id);
    if let Some(decor) = prior_decor {
        *replacement.decor_mut() = decor;
    }
    set_table_value(table, canonical_key, replacement);
}

fn migrate_legacy_materials_in_item(item: &mut Item) -> Result<(), AndroidBridgeError> {
    match item {
        Item::None => Ok(()),
        Item::Value(value) => migrate_legacy_materials_in_value(value),
        Item::Table(table) => migrate_legacy_materials_in_table(table),
        Item::ArrayOfTables(tables) => {
            for table in tables.iter_mut() {
                migrate_legacy_materials_in_table(table)?;
            }
            Ok(())
        }
    }
}

fn migrate_legacy_materials_in_value(value: &mut Value) -> Result<(), AndroidBridgeError> {
    match value {
        Value::Array(array) => {
            for value in array.iter_mut() {
                migrate_legacy_materials_in_value(value)?;
            }
        }
        Value::InlineTable(table) => {
            canonicalize_inline_material(
                table,
                "secret",
                "secret_file",
                LEGACY_CREDENTIAL_TOKEN,
                MANAGED_CREDENTIAL_ID,
            )?;
            canonicalize_inline_material(
                table,
                "tls_pinned_certificate",
                "tls_pinned_certificate_file",
                LEGACY_CERTIFICATE_TOKEN,
                MANAGED_PINNED_CERTIFICATE_ID,
            )?;
            canonicalize_inline_material(
                table,
                "transport_secret",
                "transport_secret_file",
                LEGACY_TRANSPORT_SECRET_TOKEN,
                MANAGED_TRANSPORT_SECRET_ID,
            )?;
            for (_, value) in table.iter_mut() {
                migrate_legacy_materials_in_value(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn migrate_legacy_materials_in_table(table: &mut Table) -> Result<(), AndroidBridgeError> {
    canonicalize_table_material(
        table,
        "secret",
        Some("secret_file"),
        LEGACY_CREDENTIAL_TOKEN,
        MANAGED_CREDENTIAL_ID,
        false,
    )?;
    canonicalize_table_material(
        table,
        "tls_pinned_certificate",
        Some("tls_pinned_certificate_file"),
        LEGACY_CERTIFICATE_TOKEN,
        MANAGED_PINNED_CERTIFICATE_ID,
        false,
    )?;
    canonicalize_table_material(
        table,
        "transport_secret",
        Some("transport_secret_file"),
        LEGACY_TRANSPORT_SECRET_TOKEN,
        MANAGED_TRANSPORT_SECRET_ID,
        false,
    )?;
    for (_, item) in table.iter_mut() {
        migrate_legacy_materials_in_item(item)?;
    }
    Ok(())
}

fn managed_source_count_in_item(item: &Item, id: &str) -> usize {
    match item {
        Item::None => 0,
        Item::Value(value) => managed_source_count_in_value(value, id),
        Item::Table(table) => managed_source_count_in_table(table, id),
        Item::ArrayOfTables(tables) => tables
            .iter()
            .map(|table| managed_source_count_in_table(table, id))
            .sum(),
    }
}

fn managed_source_count_in_value(value: &Value, id: &str) -> usize {
    usize::from(is_managed_source(value, id))
        + match value {
            Value::Array(array) => array
                .iter()
                .map(|value| managed_source_count_in_value(value, id))
                .sum(),
            Value::InlineTable(table) => table
                .iter()
                .map(|(_, value)| managed_source_count_in_value(value, id))
                .sum(),
            _ => 0,
        }
}

fn managed_source_count_in_table(table: &Table, id: &str) -> usize {
    table
        .iter()
        .map(|(_, item)| managed_source_count_in_item(item, id))
        .sum()
}

#[derive(Clone, Copy)]
enum ManagedPinOwner {
    ArrayOfTablesTable(usize),
    ArrayOfTablesInline(usize),
    InlineArray(usize),
}

fn table_owns_managed_pin(security: &Table) -> bool {
    security
        .get("tls_pinned_certificate")
        .and_then(Item::as_value)
        .is_some_and(|value| is_managed_source(value, MANAGED_PINNED_CERTIFICATE_ID))
}

fn inline_table_owns_managed_pin(security: &InlineTable) -> bool {
    security
        .get("tls_pinned_certificate")
        .is_some_and(|value| is_managed_source(value, MANAGED_PINNED_CERTIFICATE_ID))
}

fn managed_pin_owners(document: &DocumentMut) -> Vec<ManagedPinOwner> {
    if let Some(outbounds) = document.get("outbounds").and_then(Item::as_array_of_tables) {
        return outbounds
            .iter()
            .enumerate()
            .filter(|(_, outbound)| outbound.get("protocol").and_then(Item::as_str) == Some("mpp"))
            .filter_map(|(index, outbound)| {
                let security = outbound.get("security")?;
                if security.as_table().is_some_and(table_owns_managed_pin) {
                    Some(ManagedPinOwner::ArrayOfTablesTable(index))
                } else if security
                    .as_value()
                    .and_then(Value::as_inline_table)
                    .is_some_and(inline_table_owns_managed_pin)
                {
                    Some(ManagedPinOwner::ArrayOfTablesInline(index))
                } else {
                    None
                }
            })
            .collect();
    }
    document
        .get("outbounds")
        .and_then(Item::as_array)
        .into_iter()
        .flat_map(Array::iter)
        .enumerate()
        .filter_map(|(index, outbound)| {
            let outbound = outbound.as_inline_table()?;
            if outbound.get("protocol").and_then(Value::as_str) != Some("mpp") {
                return None;
            }
            outbound
                .get("security")
                .and_then(Value::as_inline_table)
                .filter(|security| inline_table_owns_managed_pin(security))
                .map(|_| ManagedPinOwner::InlineArray(index))
        })
        .collect()
}

fn insert_missing_transport_marker(document: &mut DocumentMut) -> Result<(), AndroidBridgeError> {
    let owners = managed_pin_owners(document);
    let [owner] = owners.as_slice() else {
        return Err(bridge_error(
            "editable configuration must have exactly one MPP owner for managed pinned certificate material",
        ));
    };
    match *owner {
        ManagedPinOwner::ArrayOfTablesTable(index) => {
            let security = mpp_outbound_mut(document, index)?
                .get_mut("security")
                .and_then(Item::as_table_mut)
                .ok_or_else(|| bridge_error("managed MPP security table is unavailable"))?;
            if !security.contains_key("transport_secret")
                && !security.contains_key("transport_secret_file")
            {
                security.insert(
                    "transport_secret",
                    Item::Value(managed_source(MANAGED_TRANSPORT_SECRET_ID)),
                );
            }
        }
        ManagedPinOwner::ArrayOfTablesInline(index) => {
            let security = mpp_outbound_mut(document, index)?
                .get_mut("security")
                .and_then(Item::as_value_mut)
                .and_then(Value::as_inline_table_mut)
                .ok_or_else(|| bridge_error("managed inline MPP security is unavailable"))?;
            if !security.contains_key("transport_secret")
                && !security.contains_key("transport_secret_file")
            {
                security.insert(
                    "transport_secret",
                    managed_source(MANAGED_TRANSPORT_SECRET_ID),
                );
            }
        }
        ManagedPinOwner::InlineArray(index) => {
            let security = document
                .get_mut("outbounds")
                .and_then(Item::as_array_mut)
                .and_then(|outbounds| outbounds.get_mut(index))
                .and_then(Value::as_inline_table_mut)
                .and_then(|outbound| outbound.get_mut("security"))
                .and_then(Value::as_inline_table_mut)
                .ok_or_else(|| bridge_error("managed inline MPP security is unavailable"))?;
            if !security.contains_key("transport_secret")
                && !security.contains_key("transport_secret_file")
            {
                security.insert(
                    "transport_secret",
                    managed_source(MANAGED_TRANSPORT_SECRET_ID),
                );
            }
        }
    }
    Ok(())
}

fn migrate_v03_routing_actions(document: &mut DocumentMut) -> Result<(), AndroidBridgeError> {
    let Some(rules) = document
        .get_mut("routing")
        .and_then(Item::as_table_mut)
        .and_then(|routing| routing.get_mut("rules"))
        .and_then(Item::as_array_of_tables_mut)
    else {
        return Ok(());
    };

    for rule in rules.iter_mut() {
        let Some(action) = rule.get("action") else {
            continue;
        };
        if rule.contains_key("decision") {
            return Err(bridge_error(
                "editable routing rule cannot mix legacy action with decision",
            ));
        }
        let action = action
            .as_str()
            .ok_or_else(|| bridge_error("legacy routing action must be a string"))?;
        let outbound = routing_rule_string_reference(rule, "outbound")?;
        let balancer = routing_rule_string_reference(rule, "balancer")?;
        let decision = match action {
            "outbound" if outbound && !balancer => "allow",
            "balancer" if balancer && !outbound => "allow",
            "reject"
                if !outbound
                    && !balancer
                    && !rule.contains_key("dns_policy")
                    && !rule.contains_key("initial_demand") =>
            {
                "reject"
            }
            "drop"
                if !outbound
                    && !balancer
                    && !rule.contains_key("dns_policy")
                    && !rule.contains_key("initial_demand") =>
            {
                "drop"
            }
            "outbound" => {
                return Err(bridge_error(
                    "legacy outbound routing action requires outbound and forbids balancer",
                ));
            }
            "balancer" => {
                return Err(bridge_error(
                    "legacy balancer routing action requires balancer and forbids outbound",
                ));
            }
            "reject" | "drop" => {
                return Err(bridge_error(
                    "legacy terminal routing action forbids outbound, balancer, dns_policy, and initial_demand",
                ));
            }
            _ => return Err(bridge_error("legacy routing action is unsupported")),
        };

        let (action_key, action_item) = rule
            .remove_entry("action")
            .ok_or_else(|| bridge_error("legacy routing action became unavailable"))?;
        let action_value = action_item
            .as_value()
            .ok_or_else(|| bridge_error("legacy routing action must be a value"))?;
        let mut replacement = Value::from(decision);
        *replacement.decor_mut() = action_value.decor().clone();
        let decision_key = Key::new("decision")
            .with_leaf_decor(action_key.leaf_decor().clone())
            .with_dotted_decor(action_key.dotted_decor().clone());
        rule.insert_formatted(&decision_key, Item::Value(replacement));
    }
    Ok(())
}

fn routing_rule_string_reference(
    rule: &Table,
    key: &'static str,
) -> Result<bool, AndroidBridgeError> {
    match rule.get(key) {
        None => Ok(false),
        Some(value) if value.as_str().is_some() => Ok(true),
        Some(_) => Err(bridge_error(format!(
            "legacy routing reference {key:?} must be a string"
        ))),
    }
}

fn migrate_editor(contents: &str) -> Result<String, AndroidBridgeError> {
    let mut document = parse_editor_document(contents)?;
    migrate_v03_routing_actions(&mut document)?;
    migrate_legacy_materials_in_table(document.as_table_mut())?;
    if managed_source_count_in_table(document.as_table(), MANAGED_TRANSPORT_SECRET_ID) == 0 {
        insert_missing_transport_marker(&mut document)?;
    }
    Ok(document.to_string())
}

fn patch_paths(outbound: &mut Table, projected: &[EditorPath]) -> Result<(), AndroidBridgeError> {
    let existing = outbound
        .get("paths")
        .and_then(Item::as_array)
        .ok_or_else(|| bridge_error("editable MPP outbound requires a paths array"))?;
    let existing_values = existing.iter().cloned().collect::<Vec<_>>();
    let mut replacement = Array::new();
    *replacement.decor_mut() = existing.decor().clone();
    replacement.set_trailing_comma(existing.trailing_comma());
    replacement.set_trailing(existing.trailing().clone());
    let mut reserved_old_indices = HashSet::new();
    let reservations = projected
        .iter()
        .map(|projected_path| {
            let reservation = existing_values
                .iter()
                .enumerate()
                .find_map(|(old_index, value)| {
                    (!reserved_old_indices.contains(&old_index)
                        && value
                            .as_inline_table()
                            .and_then(|table| table.get("name"))
                            .and_then(Value::as_str)
                            == Some(projected_path.name.as_str()))
                    .then_some(old_index)
                });
            if let Some(old_index) = reservation {
                reserved_old_indices.insert(old_index);
            }
            reservation
        })
        .collect::<Vec<_>>();
    let mut claimed = HashSet::new();
    for (index, projected_path) in projected.iter().enumerate() {
        let positional_match = (index < existing_values.len()
            && !claimed.contains(&index)
            && !reserved_old_indices.contains(&index))
        .then_some(index);
        let old_index = reservations[index].or(positional_match);
        if let Some(old_index) = old_index {
            claimed.insert(old_index);
        }
        let mut path = old_index
            .and_then(|old_index| existing_values[old_index].as_inline_table().cloned())
            .unwrap_or_default();
        set_inline_value(&mut path, "name", Value::from(projected_path.name.as_str()));
        set_inline_value(
            &mut path,
            "endpoint",
            Value::from(projected_path.endpoint.as_str()),
        );
        let mut value = Value::InlineTable(path);
        if let Some(old_index) = old_index {
            *value.decor_mut() = existing_values[old_index].decor().clone();
        }
        replacement.push_formatted(value);
    }
    set_table_value(outbound, "paths", Value::Array(replacement));
    Ok(())
}

fn patch_advanced(
    document: &mut DocumentMut,
    outbound_index: usize,
    advanced: Option<&EditorAdvanced>,
) -> Result<(), AndroidBridgeError> {
    {
        let outbound = mpp_outbound_mut(document, outbound_index)?;
        match advanced {
            Some(advanced) => {
                set_table_value(
                    outbound,
                    "path_probe_interval_ms",
                    Value::from(advanced.path_probe_interval_ms),
                );
                set_table_value(
                    outbound,
                    "path_probe_timeout_ms",
                    Value::from(advanced.path_probe_timeout_ms),
                );
                let performance = ensure_child_table(outbound, "performance")?;
                set_table_value(
                    performance,
                    "extra_traffic_hint_percent",
                    Value::from(advanced.extra_traffic_hint_percent),
                );
                let security = ensure_child_table(outbound, "security")?;
                set_table_value(
                    security,
                    "auth_freshness_window_seconds",
                    Value::from(advanced.auth_freshness_window_seconds),
                );
            }
            None => {
                outbound.remove("path_probe_interval_ms");
                outbound.remove("path_probe_timeout_ms");
                if let Some(performance) =
                    outbound.get_mut("performance").and_then(Item::as_table_mut)
                {
                    performance.remove("extra_traffic_hint_percent");
                }
                if let Some(security) = outbound.get_mut("security").and_then(Item::as_table_mut) {
                    security.remove("auth_freshness_window_seconds");
                }
            }
        }
    }

    match advanced {
        Some(advanced) => {
            let session = ensure_child_table(document.as_table_mut(), "session")?;
            set_table_value(
                session,
                "retention_timeout_ms",
                Value::from(advanced.session_retention_timeout_ms),
            );
            let resources = ensure_child_table(document.as_table_mut(), "resources")?;
            set_table_value(
                resources,
                "tcp_path_heartbeat_interval_ms",
                Value::from(advanced.tcp_heartbeat_interval_ms),
            );
            set_table_value(
                resources,
                "tcp_path_heartbeat_timeout_ms",
                Value::from(advanced.tcp_heartbeat_timeout_ms),
            );
            set_table_value(
                resources,
                "quic_path_keep_alive_interval_ms",
                Value::from(advanced.quic_keep_alive_interval_ms),
            );
            set_table_value(
                resources,
                "quic_path_idle_timeout_ms",
                Value::from(advanced.quic_idle_timeout_ms),
            );
        }
        None => {
            if let Some(session) = document.get_mut("session").and_then(Item::as_table_mut) {
                session.remove("retention_timeout_ms");
            }
            if let Some(resources) = document.get_mut("resources").and_then(Item::as_table_mut) {
                for key in [
                    "tcp_path_heartbeat_interval_ms",
                    "tcp_path_heartbeat_timeout_ms",
                    "quic_path_keep_alive_interval_ms",
                    "quic_path_idle_timeout_ms",
                ] {
                    resources.remove(key);
                }
            }
        }
    }
    Ok(())
}

fn patch_editor(contents: &str, projection_json: &str) -> Result<String, AndroidBridgeError> {
    let projection = parse_projection_json(projection_json)?;
    let migrated = migrate_editor(contents)?;
    let mut document = parse_editor_document(&migrated)?;
    let outbound_index = guided_mpp_outbound_index(&document)?;
    let old_credential_id = {
        let outbound = mpp_outbound(&document, outbound_index)?;
        required_string(child_table(outbound, "security")?, "credential_id")?.to_string()
    };
    let credential_index = referenced_credential_index(&document, &old_credential_id)?;

    {
        let credential = credential_table_mut(&mut document, credential_index)?;
        set_table_value(
            credential,
            "credential_id",
            Value::from(projection.credential_id.as_str()),
        );
        set_table_value(
            credential,
            "principal_id",
            Value::from(projection.principal_id.as_str()),
        );
        set_authoritative_managed(credential, "secret", "secret_file", MANAGED_CREDENTIAL_ID);
    }
    {
        let outbound = mpp_outbound_mut(&mut document, outbound_index)?;
        patch_paths(outbound, &projection.paths)?;
        let security = ensure_child_table(outbound, "security")?;
        set_table_value(
            security,
            "credential_id",
            Value::from(projection.credential_id.as_str()),
        );
        set_table_value(
            security,
            "tls_server_name",
            Value::from(projection.tls_server_name.as_str()),
        );
        set_authoritative_managed(
            security,
            "tls_pinned_certificate",
            "tls_pinned_certificate_file",
            MANAGED_PINNED_CERTIFICATE_ID,
        );
        set_authoritative_managed(
            security,
            "transport_secret",
            "transport_secret_file",
            MANAGED_TRANSPORT_SECRET_ID,
        );
    }
    patch_advanced(&mut document, outbound_index, projection.advanced.as_ref())?;
    Ok(document.to_string())
}

fn parse_finalize_bindings(contents: &str) -> Result<FinalizeBindings, AndroidBridgeError> {
    if contents.len() > MAX_CONFIG_BYTES * 3 {
        return Err(bridge_error(
            "finalization bindings exceed the supported size",
        ));
    }
    let bindings = serde_json::from_str::<FinalizeBindings>(contents)
        .map_err(|_| bridge_error("finalization bindings JSON is invalid"))?;
    if bindings.schema_version != 1 {
        return Err(bridge_error(
            "unsupported finalization bindings schema version",
        ));
    }
    if bindings.socks_port == 0 {
        return Err(bridge_error("SOCKS port must be between 1 and 65535"));
    }
    validate_base64_material("credential", &bindings.credential_base64, None, false)?;
    validate_base64_material(
        "pinned certificate",
        &bindings.pinned_certificate_base64,
        None,
        true,
    )?;
    if let Some(value) = &bindings.transport_secret_base64 {
        validate_base64_material("transport secret", value, Some(32), false)?;
    }
    if let Some(local_auth) = &bindings.local_auth {
        if local_auth.username.trim().is_empty() {
            return Err(bridge_error("local proxy username must be nonempty"));
        }
        validate_base64_material(
            "local proxy password",
            &local_auth.password_base64,
            None,
            true,
        )?;
    }
    Ok(bindings)
}

fn validate_base64_material(
    purpose: &str,
    encoded: &str,
    exact_size: Option<usize>,
    require_utf8: bool,
) -> Result<(), AndroidBridgeError> {
    let maximum_encoded = MAX_MATERIAL_BYTES.saturating_mul(4) / 3 + 4;
    if encoded.is_empty() || encoded.len() > maximum_encoded {
        return Err(bridge_error(format!(
            "{purpose} material has an unsupported size"
        )));
    }
    let decoded = BASE64
        .decode(encoded.as_bytes())
        .map_err(|_| bridge_error(format!("{purpose} material is not strict Base64")))?;
    if decoded.is_empty()
        || decoded.len() > MAX_MATERIAL_BYTES
        || BASE64.encode(&decoded) != encoded
        || exact_size.is_some_and(|size| decoded.len() != size)
    {
        return Err(bridge_error(format!(
            "{purpose} material is not canonical or has an unsupported size"
        )));
    }
    if require_utf8 && std::str::from_utf8(&decoded).is_err() {
        return Err(bridge_error(format!(
            "{purpose} material must decode to UTF-8"
        )));
    }
    Ok(())
}

#[derive(Default)]
struct ManagedMaterialCounts {
    credential: usize,
    pinned_certificate: usize,
    transport_secret: usize,
}

enum ManagedMaterialEdit {
    NotManaged,
    Replace(Value),
    Remove,
}

fn managed_material_edit(
    value: &Value,
    bindings: &FinalizeBindings,
    counts: &mut ManagedMaterialCounts,
) -> Result<ManagedMaterialEdit, AndroidBridgeError> {
    let Some(source) = value.as_inline_table() else {
        return Ok(ManagedMaterialEdit::NotManaged);
    };
    if source.get("from").and_then(Value::as_str) != Some("managed") {
        return Ok(ManagedMaterialEdit::NotManaged);
    }
    if source.len() != 2 {
        return Err(bridge_error("managed material reference is malformed"));
    }
    let id = source
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| bridge_error("managed material reference is malformed"))?;
    match id {
        MANAGED_CREDENTIAL_ID => {
            counts.credential += 1;
            Ok(ManagedMaterialEdit::Replace(base64_source(
                &bindings.credential_base64,
            )))
        }
        MANAGED_PINNED_CERTIFICATE_ID => {
            counts.pinned_certificate += 1;
            Ok(ManagedMaterialEdit::Replace(base64_source(
                &bindings.pinned_certificate_base64,
            )))
        }
        MANAGED_TRANSPORT_SECRET_ID => {
            counts.transport_secret += 1;
            Ok(match &bindings.transport_secret_base64 {
                Some(value) => ManagedMaterialEdit::Replace(base64_source(value)),
                None => ManagedMaterialEdit::Remove,
            })
        }
        _ => Err(bridge_error(
            "managed material reference uses an unknown id",
        )),
    }
}

fn finalize_managed_materials_in_item(
    item: &mut Item,
    bindings: &FinalizeBindings,
    counts: &mut ManagedMaterialCounts,
) -> Result<(), AndroidBridgeError> {
    match item {
        Item::None => Ok(()),
        Item::Value(value) => finalize_managed_materials_in_value(value, bindings, counts, false),
        Item::Table(table) => finalize_managed_materials_in_table(table, bindings, counts),
        Item::ArrayOfTables(tables) => {
            for table in tables.iter_mut() {
                finalize_managed_materials_in_table(table, bindings, counts)?;
            }
            Ok(())
        }
    }
}

fn finalize_managed_materials_in_table(
    table: &mut Table,
    bindings: &FinalizeBindings,
    counts: &mut ManagedMaterialCounts,
) -> Result<(), AndroidBridgeError> {
    let keys = table
        .iter()
        .map(|(key, _)| key.to_string())
        .collect::<Vec<_>>();
    for key in keys {
        let edit = table
            .get(&key)
            .and_then(Item::as_value)
            .map_or(Ok(ManagedMaterialEdit::NotManaged), |value| {
                managed_material_edit(value, bindings, counts)
            })?;
        match edit {
            ManagedMaterialEdit::NotManaged => {}
            ManagedMaterialEdit::Replace(mut replacement) => {
                if let Some(previous) = table.get(&key).and_then(Item::as_value) {
                    *replacement.decor_mut() = previous.decor().clone();
                }
                table.insert(&key, Item::Value(replacement));
                continue;
            }
            ManagedMaterialEdit::Remove => {
                table.remove(&key);
                continue;
            }
        }
        if let Some(item) = table.get_mut(&key) {
            finalize_managed_materials_in_item(item, bindings, counts)?;
        }
    }
    Ok(())
}

fn finalize_managed_materials_in_inline_table(
    table: &mut InlineTable,
    bindings: &FinalizeBindings,
    counts: &mut ManagedMaterialCounts,
) -> Result<(), AndroidBridgeError> {
    let keys = table
        .iter()
        .map(|(key, _)| key.to_string())
        .collect::<Vec<_>>();
    for key in keys {
        let edit = table
            .get(&key)
            .map_or(Ok(ManagedMaterialEdit::NotManaged), |value| {
                managed_material_edit(value, bindings, counts)
            })?;
        match edit {
            ManagedMaterialEdit::NotManaged => {}
            ManagedMaterialEdit::Replace(mut replacement) => {
                if let Some(previous) = table.get(&key) {
                    *replacement.decor_mut() = previous.decor().clone();
                }
                table.insert(&key, replacement);
                continue;
            }
            ManagedMaterialEdit::Remove => {
                table.remove(&key);
                continue;
            }
        }
        if let Some(value) = table.get_mut(&key) {
            finalize_managed_materials_in_value(value, bindings, counts, false)?;
        }
    }
    Ok(())
}

fn finalize_managed_materials_in_value(
    value: &mut Value,
    bindings: &FinalizeBindings,
    counts: &mut ManagedMaterialCounts,
    removable: bool,
) -> Result<(), AndroidBridgeError> {
    match managed_material_edit(value, bindings, counts)? {
        ManagedMaterialEdit::NotManaged => {}
        ManagedMaterialEdit::Replace(mut replacement) => {
            *replacement.decor_mut() = value.decor().clone();
            *value = replacement;
            return Ok(());
        }
        ManagedMaterialEdit::Remove if removable => return Ok(()),
        ManagedMaterialEdit::Remove => {
            return Err(bridge_error(
                "optional managed transport material must be a named table field",
            ));
        }
    }
    match value {
        Value::Array(array) => {
            for value in array.iter_mut() {
                finalize_managed_materials_in_value(value, bindings, counts, false)?;
            }
        }
        Value::InlineTable(table) => {
            finalize_managed_materials_in_inline_table(table, bindings, counts)?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_managed_material_counts(
    counts: &ManagedMaterialCounts,
) -> Result<(), AndroidBridgeError> {
    if counts.credential != 1 || counts.pinned_certificate != 1 || counts.transport_secret != 1 {
        return Err(bridge_error(
            "editable configuration must contain exactly one of each required managed material reference",
        ));
    }
    Ok(())
}

fn finalize_socks_port(document: &mut DocumentMut, port: u16) -> Result<usize, AndroidBridgeError> {
    let inbounds = document
        .get_mut("inbounds")
        .and_then(Item::as_array_of_tables_mut)
        .ok_or_else(|| bridge_error("editable configuration has no inbound table"))?;
    let managed_listen = format!("127.0.0.1:{SOCKS_PORT_TOKEN}");
    let mut matches = Vec::new();
    for (inbound_index, inbound) in inbounds.iter().enumerate() {
        if inbound.get("protocol").and_then(Item::as_str) != Some("mixed") {
            continue;
        }
        let Some(listen) = inbound.get("listen").and_then(Item::as_array) else {
            continue;
        };
        for (listen_index, value) in listen.iter().enumerate() {
            if value.as_str() == Some(managed_listen.as_str()) {
                matches.push((inbound_index, listen_index));
            }
        }
    }
    let [(inbound_index, listen_index)] = matches.as_slice() else {
        return Err(bridge_error(
            "editable configuration must contain exactly one managed loopback mixed inbound",
        ));
    };
    let inbound = inbounds
        .get_mut(*inbound_index)
        .ok_or_else(|| bridge_error("managed mixed inbound is unavailable"))?;
    let listen = inbound
        .get_mut("listen")
        .and_then(Item::as_array_mut)
        .ok_or_else(|| bridge_error("managed mixed inbound listen field is unavailable"))?;
    listen.replace(*listen_index, format!("127.0.0.1:{port}"));
    Ok(*inbound_index)
}

fn add_local_auth(
    document: &mut DocumentMut,
    inbound_index: usize,
    local_auth: &FinalizeLocalAuth,
) -> Result<(), AndroidBridgeError> {
    if document
        .get("local_users")
        .and_then(Item::as_array_of_tables)
        .is_some_and(|users| {
            users
                .iter()
                .any(|user| user.get("name").and_then(Item::as_str) == Some(ANDROID_LOCAL_USER_ID))
        })
    {
        return Err(bridge_error(
            "editable configuration already defines the reserved Android local user",
        ));
    }
    if !document.contains_key("local_users") {
        document.insert("local_users", Item::ArrayOfTables(ArrayOfTables::new()));
    }
    let users = document
        .get_mut("local_users")
        .and_then(Item::as_array_of_tables_mut)
        .ok_or_else(|| bridge_error("local_users must be an array of tables"))?;
    let mut user = Table::new();
    user.insert("name", Item::Value(Value::from(ANDROID_LOCAL_USER_ID)));
    user.insert(
        "principal_id",
        Item::Value(Value::from(ANDROID_LOCAL_USER_ID)),
    );
    user.insert(
        "username",
        Item::Value(Value::from(local_auth.username.as_str())),
    );
    user.insert(
        "password",
        Item::Value(base64_source(&local_auth.password_base64)),
    );
    users.push(user);

    let inbound = document
        .get_mut("inbounds")
        .and_then(Item::as_array_of_tables_mut)
        .and_then(|inbounds| inbounds.get_mut(inbound_index))
        .ok_or_else(|| bridge_error("managed mixed inbound is unavailable"))?;
    if !inbound.contains_key("local_users") {
        inbound.insert("local_users", Item::Value(Value::Array(Array::new())));
    }
    let bindings = inbound
        .get_mut("local_users")
        .and_then(Item::as_array_mut)
        .ok_or_else(|| bridge_error("managed mixed inbound local_users must be an array"))?;
    if bindings
        .iter()
        .any(|value| value.as_str() == Some(ANDROID_LOCAL_USER_ID))
    {
        return Err(bridge_error(
            "managed mixed inbound already uses the reserved Android local user",
        ));
    }
    bindings.push(ANDROID_LOCAL_USER_ID);
    Ok(())
}

fn exact_marker_count(contents: &str, marker: &str) -> usize {
    contents
        .lines()
        .filter(|line| line.trim() == marker)
        .count()
}

fn remove_editor_marker_lines(contents: &str) -> String {
    let mut finalized = String::with_capacity(contents.len());
    for line in contents.split_inclusive('\n') {
        let body = line.trim_end_matches(['\r', '\n']);
        if matches!(
            body.trim(),
            LOCAL_USER_DEFINITION_MARKER | LOCAL_USER_BINDING_MARKER
        ) {
            continue;
        }
        finalized.push_str(line);
    }
    finalized
}

fn finalize_editor(contents: &str, bindings_json: &str) -> Result<String, AndroidBridgeError> {
    let bindings = parse_finalize_bindings(bindings_json)?;
    let migrated = migrate_editor(contents)?;
    if exact_marker_count(&migrated, LOCAL_USER_DEFINITION_MARKER) != 1
        || exact_marker_count(&migrated, LOCAL_USER_BINDING_MARKER) != 1
    {
        return Err(bridge_error(
            "editable configuration must contain exactly one of each local-auth marker",
        ));
    }
    let mut document = parse_editor_document(&migrated)?;
    let mut counts = ManagedMaterialCounts::default();
    finalize_managed_materials_in_table(document.as_table_mut(), &bindings, &mut counts)?;
    validate_managed_material_counts(&counts)?;
    let inbound_index = finalize_socks_port(&mut document, bindings.socks_port)?;
    if let Some(local_auth) = &bindings.local_auth {
        add_local_auth(&mut document, inbound_index, local_auth)?;
    }
    let finalized = remove_editor_marker_lines(&document.to_string());
    compile_inline_config(&finalized)?;
    Ok(finalized)
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
    config: crate::config::AppConfig,
    protector: AndroidSocketProtector,
    ready_timeout: Duration,
) -> Result<bool, AndroidBridgeError> {
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
    active
        .worker
        .take()
        .is_none_or(|worker| worker.join().is_ok())
}

fn jstring(env: &Env<'_>, value: &JString<'_>, name: &str) -> Result<String, AndroidBridgeError> {
    if value.is_null() {
        return Err(bridge_error(format!("{name} must not be null")));
    }
    value.try_to_string(env).map_err(AndroidBridgeError::from)
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

#[jni_mangle("com.v2ray.ang.mpp.MptunnelNative", "nativeProjectEditor")]
pub fn native_project_editor<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    config_toml: JString<'caller>,
) -> JString<'caller> {
    unowned_env
        .with_env(|env| -> Result<JString<'caller>, AndroidBridgeError> {
            let config_toml = jstring(env, &config_toml, "editable configuration document")?;
            let projection = project_editor(&config_toml)?;
            let json = serde_json::to_string(&projection)
                .map_err(|_| bridge_error("failed to encode editor projection"))?;
            Ok(env.new_string(json)?)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[jni_mangle("com.v2ray.ang.mpp.MptunnelNative", "nativePatchEditor")]
pub fn native_patch_editor<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    config_toml: JString<'caller>,
    projection_json: JString<'caller>,
) -> JString<'caller> {
    unowned_env
        .with_env(|env| -> Result<JString<'caller>, AndroidBridgeError> {
            let config_toml = jstring(env, &config_toml, "editable configuration document")?;
            let projection_json = jstring(env, &projection_json, "editor projection JSON")?;
            Ok(env.new_string(patch_editor(&config_toml, &projection_json)?)?)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[jni_mangle("com.v2ray.ang.mpp.MptunnelNative", "nativeMigrateEditor")]
pub fn native_migrate_editor<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    config_toml: JString<'caller>,
) -> JString<'caller> {
    unowned_env
        .with_env(|env| -> Result<JString<'caller>, AndroidBridgeError> {
            let config_toml = jstring(env, &config_toml, "editable configuration document")?;
            Ok(env.new_string(migrate_editor(&config_toml)?)?)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[jni_mangle("com.v2ray.ang.mpp.MptunnelNative", "nativeFinalizeEditor")]
pub fn native_finalize_editor<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    config_toml: JString<'caller>,
    bindings_json: JString<'caller>,
) -> JString<'caller> {
    unowned_env
        .with_env(|env| -> Result<JString<'caller>, AndroidBridgeError> {
            let config_toml = jstring(env, &config_toml, "editable configuration document")?;
            let bindings_json = jstring(env, &bindings_json, "finalization bindings JSON")?;
            Ok(env.new_string(finalize_editor(&config_toml, &bindings_json)?)?)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[jni_mangle("com.v2ray.ang.mpp.MptunnelNative", "nativeStart")]
pub fn native_start<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    config_toml: JString<'caller>,
    protector: JObject<'caller>,
    ready_timeout_ms: jlong,
) -> jboolean {
    unowned_env
        .with_env(|env| -> Result<jboolean, AndroidBridgeError> {
            if protector.is_null() {
                return Err(bridge_error("socket protector must not be null"));
            }
            let config_toml = jstring(env, &config_toml, "configuration document")?;
            reserve_start()?;
            let result = (|| {
                let config = compile_inline_config(&config_toml)?;
                let callback = env.new_global_ref(&protector)?;
                start_runtime(
                    config,
                    AndroidSocketProtector { callback },
                    duration_from_java(ready_timeout_ms, "ready timeout")?,
                )
            })();
            if result.is_err() {
                cancel_start_reservation();
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const INLINE_CONFIG: &str = r#"
[[inbounds]]
name = "local"
protocol = "mixed"
listen = ["127.0.0.1:1080"]

[[outbounds]]
name = "direct"
protocol = "direct"

[routing]
[[routing.rules]]
name = "default"
outbound = "direct"
"#;

    const EDITABLE_CONFIG: &str = r#"
[[credentials]]
credential_id = "profile"
principal_id = "profile"
secret = { from = "managed", id = "credential" }

# @mptunnel-local-user-definition@
[[inbounds]]
name = "local"
protocol = "mixed"
listen = ["127.0.0.1:@mptunnel-socks-port@"]
# @mptunnel-local-user-binding@

[[outbounds]]
name = "remote"
protocol = "mpp"
# preserve this path comment
paths = [{ name = "primary", endpoint = "tcp://127.0.0.1:7443", custom_path = "keep" }]
custom_outbound = "keep"
path_probe_interval_ms = 1234

[outbounds.security]
credential_id = "profile"
tls_server_name = "mptunnel.test"
tls_pinned_certificate = { from = "managed", id = "pinned-certificate" }
transport_secret = { from = "managed", id = "transport-secret" }

[routing]
[[routing.rules]]
name = "default"
outbound = "remote"
"#;

    const LEGACY_EDITABLE_CONFIG_WITHOUT_TRANSPORT: &str = r#"
[[credentials]]
credential_id = "profile"
principal_id = "profile"
secret = { from = "file", path = "@mptunnel-profile-credential@" }

@mptunnel-local-user-definition@
[[inbounds]]
name = "local"
protocol = "mixed"
listen = ["127.0.0.1:@mptunnel-socks-port@"]
@mptunnel-local-user-binding@

[[outbounds]]
name = "remote"
protocol = "mpp"
paths = [{ name = "primary", endpoint = "tcp://127.0.0.1:7443" }]
custom_outbound = "keep"

[outbounds.security]
credential_id = "profile"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "@mptunnel-profile-certificate@"

[routing]
[[routing.rules]]
name = "default"
outbound = "remote"
"#;

    const LEGACY_INLINE_SECURITY_CONFIG: &str = r#"
[[credentials]]
credential_id = "profile"
principal_id = "profile"
secret = { from = "file", path = "@mptunnel-profile-credential@" }

# @mptunnel-local-user-definition@
[[inbounds]]
name = "local"
protocol = "mixed"
listen = ["127.0.0.1:@mptunnel-socks-port@"]
# @mptunnel-local-user-binding@

[[outbounds]]
name = "remote"
protocol = "mpp"
paths = [{ name = "primary", endpoint = "tcp://127.0.0.1:7443" }]
# preserve inline security comment
security = { credential_id = "profile", tls_server_name = "mptunnel.test", tls_pinned_certificate_file = "@mptunnel-profile-certificate@", transport_secret_file = "@mptunnel-profile-transport-secret@", custom_security = "keep" }

[routing]
[[routing.rules]]
name = "default"
outbound = "remote"
"#;

    const LEGACY_INLINE_OUTBOUND_WITHOUT_TRANSPORT: &str = r#"
# preserve inline outbound comment
outbounds = [
  { name = "remote", protocol = "mpp", paths = [{ name = "primary", endpoint = "tcp://127.0.0.1:7443" }], security = { credential_id = "profile", tls_server_name = "mptunnel.test", tls_pinned_certificate_file = "@mptunnel-profile-certificate@", custom_security = "keep" }, custom_outbound = "keep" },
]

[[credentials]]
credential_id = "profile"
principal_id = "profile"
secret = { from = "file", path = "@mptunnel-profile-credential@" }

# @mptunnel-local-user-definition@
[[inbounds]]
name = "local"
protocol = "mixed"
listen = ["127.0.0.1:@mptunnel-socks-port@"]
# @mptunnel-local-user-binding@

[routing]
[[routing.rules]]
name = "default"
outbound = "remote"
"#;

    fn inline_document(contents: &str) -> toml::Value {
        toml::from_str(contents).expect("test TOML")
    }

    fn test_certificate_base64() -> String {
        let rcgen::CertifiedKey { cert, .. } =
            rcgen::generate_simple_self_signed(vec!["mptunnel.test".to_string()])
                .expect("test certificate");
        BASE64.encode(cert.pem().as_bytes())
    }

    fn test_bindings(transport: bool, local_auth: bool) -> String {
        json!({
            "schema_version": 1,
            "socks_port": 19080,
            "credential_base64": BASE64.encode([7_u8; 32]),
            "pinned_certificate_base64": test_certificate_base64(),
            "transport_secret_base64": transport.then(|| BASE64.encode([9_u8; 32])),
            "local_auth": local_auth.then(|| json!({
                "username": "android-user",
                "password_base64": BASE64.encode("android-password"),
            })),
        })
        .to_string()
    }

    fn runtime_editable_config() -> String {
        EDITABLE_CONFIG
            .replace(", custom_path = \"keep\"", "")
            .replace("custom_outbound = \"keep\"\n", "")
    }

    fn multi_mpp_editable_config() -> String {
        let certificate = test_certificate_base64();
        let second_credential = format!(
            r#"[[credentials]]
credential_id = "secondary-profile"
principal_id = "secondary-profile"
secret = {{ from = "base64", value = "{}" }}

{}"#,
            BASE64.encode([11_u8; 32]),
            LOCAL_USER_DEFINITION_MARKER,
        );
        let second_outbound = format!(
            r#"[[outbounds]]
name = "secondary"
protocol = "mpp"
paths = [{{ name = "secondary", endpoint = "tcp://127.0.0.1:7444" }}]

[outbounds.security]
credential_id = "secondary-profile"
tls_server_name = "mptunnel.test"
tls_pinned_certificate = {{ from = "base64", value = "{certificate}" }}

[routing]"#,
        );
        runtime_editable_config()
            .replace(LOCAL_USER_DEFINITION_MARKER, &second_credential)
            .replace("[routing]", &second_outbound)
    }

    #[test]
    fn v03_route_action_migration_is_strict_decor_preserving_and_idempotent() {
        let legacy = r#"[routing]

[[routing.rules]]
name = "direct"
action = "outbound" # preserve outbound decision comment
outbound = "edge"
custom = "keep"

[[routing.rules]]
name = "balanced"
action = "balancer"
balancer = "edges"

[[routing.rules]]
name = "rejected"
action = "reject"

[[routing.rules]]
name = "dropped"
action = "drop"
"#;
        let mut document = parse_editor_document(legacy).expect("legacy routing TOML");

        migrate_v03_routing_actions(&mut document).expect("v0.3 routing migration");
        let migrated = document.to_string();

        assert!(!migrated.contains("action ="));
        assert!(migrated.contains("decision = \"allow\" # preserve outbound decision comment"));
        assert!(migrated.contains("custom = \"keep\""));
        let rules = document["routing"]["rules"]
            .as_array_of_tables()
            .expect("routing rules");
        let decisions = rules
            .iter()
            .map(|rule| rule["decision"].as_str().expect("route decision"))
            .collect::<Vec<_>>();
        assert_eq!(decisions, ["allow", "allow", "reject", "drop"]);

        let mut second = parse_editor_document(&migrated).expect("migrated routing TOML");
        migrate_v03_routing_actions(&mut second).expect("idempotent routing migration");
        assert_eq!(second.to_string(), migrated);

        let current = r#"[routing]
[[routing.rules]]
name = "current"
decision = "allow"
outbound = "edge"
"#;
        let mut current_document = parse_editor_document(current).expect("current routing TOML");
        migrate_v03_routing_actions(&mut current_document).expect("current routing no-op");
        assert_eq!(current_document.to_string(), current);
    }

    #[test]
    fn v03_route_action_migration_rejects_dual_or_invalid_shapes() {
        for invalid in [
            r#"[routing]
[[routing.rules]]
name = "dual-dialect"
action = "outbound"
decision = "allow"
outbound = "edge"
"#,
            r#"[routing]
[[routing.rules]]
name = "missing-outbound"
action = "outbound"
"#,
            r#"[routing]
[[routing.rules]]
name = "mixed-egress"
action = "balancer"
outbound = "edge"
balancer = "edges"
"#,
            r#"[routing]
[[routing.rules]]
name = "terminal-egress"
action = "reject"
outbound = "edge"
"#,
            r#"[routing]
[[routing.rules]]
name = "terminal-policy"
action = "drop"
dns_policy = "private"
"#,
            r#"[routing]
[[routing.rules]]
name = "unknown"
action = "proxy"
outbound = "edge"
"#,
            r#"[routing]
[[routing.rules]]
name = "wrong-type"
action = 1
outbound = "edge"
"#,
            r#"[routing]
[[routing.rules]]
name = "wrong-reference-type"
action = "outbound"
outbound = 1
"#,
        ] {
            let mut document = parse_editor_document(invalid).expect("invalid-shape routing TOML");
            assert!(
                migrate_v03_routing_actions(&mut document).is_err(),
                "legacy routing migration guessed at invalid input:\n{invalid}"
            );
        }
    }

    #[test]
    fn released_mpp4_style_editor_finalizes_and_compiles_without_manual_edit() {
        let released = runtime_editable_config().replace(
            "name = \"default\"\noutbound = \"remote\"",
            "name = \"default\"\naction = \"outbound\"\noutbound = \"remote\"",
        );
        assert!(released.contains("action = \"outbound\""));

        let migrated = migrate_editor(&released).expect("released editor migration");
        assert!(!migrated.contains("action = \"outbound\""));
        assert!(migrated.contains("decision = \"allow\""));
        assert!(migrated.contains("# preserve this path comment"));
        assert_eq!(
            migrate_editor(&migrated).expect("idempotent released editor migration"),
            migrated
        );

        let finalized = finalize_editor(&released, &test_bindings(false, false))
            .expect("released mpp.4-style editor finalization");
        assert!(!finalized.contains("action = \"outbound\""));
        assert!(finalized.contains("decision = \"allow\""));
        compile_inline_config(&finalized).expect("finalized v0.4 core config");
    }

    #[test]
    fn migration_converts_only_known_legacy_tokens_and_inserts_optional_transport_marker() {
        let migrated = migrate_editor(LEGACY_EDITABLE_CONFIG_WITHOUT_TRANSPORT)
            .expect("legacy editor migration");
        assert!(migrated.contains(LOCAL_USER_DEFINITION_MARKER));
        assert!(migrated.contains(LOCAL_USER_BINDING_MARKER));
        assert!(!migrated.contains("tls_pinned_certificate_file"));
        assert!(!migrated.contains(LEGACY_CREDENTIAL_TOKEN));
        assert!(!migrated.contains(LEGACY_CERTIFICATE_TOKEN));
        assert!(migrated.contains("custom_outbound = \"keep\""));
        let document = parse_editor_document(&migrated).expect("migrated TOML");
        assert_eq!(
            managed_source_count_in_table(document.as_table(), MANAGED_CREDENTIAL_ID),
            1
        );
        assert_eq!(
            managed_source_count_in_table(document.as_table(), MANAGED_PINNED_CERTIFICATE_ID),
            1
        );
        assert_eq!(
            managed_source_count_in_table(document.as_table(), MANAGED_TRANSPORT_SECRET_ID),
            1
        );
    }

    #[test]
    fn migration_converts_exact_legacy_tokens_inside_inline_tables() {
        let migrated =
            migrate_editor(LEGACY_INLINE_SECURITY_CONFIG).expect("inline-table legacy migration");
        assert!(migrated.contains("# preserve inline security comment"));
        assert!(migrated.contains("custom_security = \"keep\""));
        assert!(!migrated.contains("tls_pinned_certificate_file"));
        assert!(!migrated.contains("transport_secret_file"));
        assert!(!migrated.contains(LEGACY_CERTIFICATE_TOKEN));
        assert!(!migrated.contains(LEGACY_TRANSPORT_SECRET_TOKEN));
        let document = parse_editor_document(&migrated).expect("migrated inline TOML");
        assert_eq!(
            managed_source_count_in_table(document.as_table(), MANAGED_PINNED_CERTIFICATE_ID),
            1
        );
        assert_eq!(
            managed_source_count_in_table(document.as_table(), MANAGED_TRANSPORT_SECRET_ID),
            1
        );
    }

    #[test]
    fn migration_inserts_absent_transport_into_exact_inline_mpp_outbound() {
        let migrated = migrate_editor(LEGACY_INLINE_OUTBOUND_WITHOUT_TRANSPORT)
            .expect("inline-outbound legacy migration");
        assert!(migrated.contains("# preserve inline outbound comment"));
        assert!(migrated.contains("custom_outbound = \"keep\""));
        assert!(migrated.contains("custom_security = \"keep\""));
        assert!(!migrated.contains("tls_pinned_certificate_file"));
        let document = parse_editor_document(&migrated).expect("migrated inline-outbound TOML");
        assert_eq!(
            managed_source_count_in_table(document.as_table(), MANAGED_PINNED_CERTIFICATE_ID),
            1
        );
        assert_eq!(
            managed_source_count_in_table(document.as_table(), MANAGED_TRANSPORT_SECRET_ID),
            1
        );
    }

    #[test]
    fn migration_inserts_transport_at_managed_pin_owner_not_first_mpp() {
        let legacy = LEGACY_EDITABLE_CONFIG_WITHOUT_TRANSPORT.replace(
            "[[outbounds]]\nname = \"remote\"",
            r#"[[outbounds]]
name = "unmanaged-first"
protocol = "mpp"
paths = [{ name = "unmanaged", endpoint = "tcp://127.0.0.1:7444" }]

[outbounds.security]
credential_id = "profile"
tls_server_name = "mptunnel.test"
tls_pinned_certificate = { from = "raw", value = "unmanaged" }

[[outbounds]]
name = "remote""#,
        );
        let migrated = migrate_editor(&legacy).expect("managed-pin owner migration");
        let document = parse_editor_document(&migrated).expect("migrated multi-MPP TOML");
        let outbounds = document
            .get("outbounds")
            .and_then(Item::as_array_of_tables)
            .expect("outbounds");
        let first_security = outbounds
            .get(0)
            .expect("first MPP")
            .get("security")
            .and_then(Item::as_table)
            .expect("first security");
        assert!(!first_security.contains_key("transport_secret"));
        let owner_security = outbounds
            .get(1)
            .expect("managed MPP")
            .get("security")
            .and_then(Item::as_table)
            .expect("managed owner security");
        assert!(
            owner_security
                .get("transport_secret")
                .and_then(Item::as_value)
                .is_some_and(|value| is_managed_source(value, MANAGED_TRANSPORT_SECRET_ID))
        );
    }

    #[test]
    fn projection_fills_partial_advanced_defaults_and_accepts_transient_values() {
        let projection = project_editor(EDITABLE_CONFIG).expect("editor projection");
        let advanced = projection.advanced.expect("partial advanced projection");
        assert_eq!(advanced.path_probe_interval_ms, 1234);
        assert_eq!(
            advanced.path_probe_timeout_ms,
            crate::config::DEFAULT_PATH_PROBE_TIMEOUT_MS as i64
        );
        assert_eq!(projection.paths[0].name, "primary");

        let transient = EditorProjection {
            schema_version: 1,
            paths: vec![
                EditorPath {
                    name: String::new(),
                    endpoint: String::new(),
                },
                EditorPath {
                    name: String::new(),
                    endpoint: "tcp://127.0.0.1:7444".to_string(),
                },
            ],
            advanced: Some(EditorAdvanced {
                path_probe_interval_ms: -1,
                path_probe_timeout_ms: 0,
                extra_traffic_hint_percent: -2,
                auth_freshness_window_seconds: -3,
                session_retention_timeout_ms: -4,
                tcp_heartbeat_interval_ms: -5,
                tcp_heartbeat_timeout_ms: -6,
                quic_keep_alive_interval_ms: -7,
                quic_idle_timeout_ms: -8,
            }),
            credential_id: String::new(),
            principal_id: String::new(),
            tls_server_name: String::new(),
        };
        let patched = patch_editor(
            EDITABLE_CONFIG,
            &serde_json::to_string(&transient).expect("projection JSON"),
        )
        .expect("transient structural patch");
        let projected = project_editor(&patched).expect("reproject transient document");
        assert_eq!(projected, transient);
    }

    #[test]
    fn guided_patch_preserves_unknowns_and_makes_material_bindings_authoritative() {
        let mut projection = project_editor(EDITABLE_CONFIG).expect("editor projection");
        projection.credential_id = "edited-credential".to_string();
        projection.principal_id = "edited-principal".to_string();
        projection.tls_server_name = "edited.example".to_string();
        projection.paths = vec![
            EditorPath {
                name: "backup".to_string(),
                endpoint: "quic://backup.example:7443".to_string(),
            },
            EditorPath {
                name: "primary".to_string(),
                endpoint: "tcp://primary.example:7443".to_string(),
            },
        ];
        let raw_materials = EDITABLE_CONFIG
            .replace(
                "secret = { from = \"managed\", id = \"credential\" }",
                "secret = { from = \"raw\", value = \"overridden\" }",
            )
            .replace(
                "tls_pinned_certificate = { from = \"managed\", id = \"pinned-certificate\" }",
                "tls_pinned_certificate = { from = \"raw\", value = \"overridden\" }",
            );
        let patched = patch_editor(
            &raw_materials,
            &serde_json::to_string(&projection).expect("projection JSON"),
        )
        .expect("guided editor patch");
        assert!(patched.contains("# preserve this path comment"));
        assert!(patched.contains("custom_outbound = \"keep\""));
        assert!(patched.contains("custom_path = \"keep\""));
        let document = parse_editor_document(&patched).expect("patched TOML");
        assert_eq!(
            managed_source_count_in_table(document.as_table(), MANAGED_CREDENTIAL_ID),
            1
        );
        assert_eq!(
            managed_source_count_in_table(document.as_table(), MANAGED_PINNED_CERTIFICATE_ID),
            1
        );
        assert_eq!(
            managed_source_count_in_table(document.as_table(), MANAGED_TRANSPORT_SECRET_ID),
            1
        );
        let paths = mpp_outbound(
            &document,
            mpp_outbound_index(&document).expect("MPP outbound"),
        )
        .expect("MPP outbound")
        .get("paths")
        .and_then(Item::as_array)
        .expect("patched paths");
        let primary = paths
            .iter()
            .find_map(|value| {
                let path = value.as_inline_table()?;
                (path.get("name").and_then(Value::as_str) == Some("primary")).then_some(path)
            })
            .expect("primary path");
        assert_eq!(
            primary.get("custom_path").and_then(Value::as_str),
            Some("keep")
        );
        assert_eq!(project_editor(&patched).expect("reproject"), projection);
    }

    #[test]
    fn finalizer_resolves_all_bindings_in_memory_and_adds_local_auth() {
        let finalized = finalize_editor(&runtime_editable_config(), &test_bindings(true, true))
            .expect("finalized editor document");
        assert!(!finalized.contains("from = \"managed\""));
        assert!(!finalized.contains(MANAGED_PLACEHOLDER_PREFIX));
        assert!(!finalized.contains(LOCAL_USER_DEFINITION_MARKER));
        assert!(!finalized.contains(LOCAL_USER_BINDING_MARKER));
        assert!(finalized.contains("127.0.0.1:19080"));
        assert!(finalized.contains("name = \"v2rayng-local\""));
        assert!(finalized.contains("username = \"android-user\""));
        let parsed = inline_document(&finalized);
        validate_inline_document(&parsed).expect("finalized Android confinement");
        compile_inline_config(&finalized).expect("finalized core config");
    }

    #[test]
    fn finalizer_removes_null_optional_transport_and_local_auth() {
        let finalized = finalize_editor(&runtime_editable_config(), &test_bindings(false, false))
            .expect("finalized editor document");
        let parsed = inline_document(&finalized);
        let security = parsed["outbounds"][0]["security"]
            .as_table()
            .expect("security table");
        assert!(!security.contains_key("transport_secret"));
        assert!(parsed.get("local_users").is_none());
        assert!(parsed["inbounds"][0].get("local_users").is_none());
    }

    #[test]
    fn finalizer_handles_multiple_mpp_outbounds_without_guided_projection() {
        let editable = multi_mpp_editable_config();
        assert!(project_editor(&editable).is_err());
        let projection = project_editor(EDITABLE_CONFIG).expect("single-MPP projection");
        assert!(
            patch_editor(
                &editable,
                &serde_json::to_string(&projection).expect("projection JSON")
            )
            .is_err()
        );
        assert!(project_editor(INLINE_CONFIG).is_err());

        let finalized = finalize_editor(&editable, &test_bindings(true, false))
            .expect("multi-MPP raw finalization");
        let parsed = inline_document(&finalized);
        assert_eq!(parsed["outbounds"].as_array().expect("outbounds").len(), 2);
        compile_inline_config(&finalized).expect("multi-MPP finalized core config");
    }

    #[test]
    fn finalizer_rejects_bad_cardinality_and_strict_base64_without_disclosure() {
        let duplicate = EDITABLE_CONFIG.replace(
            "custom_outbound = \"keep\"",
            "custom_outbound = { from = \"managed\", id = \"credential\" }",
        );
        let bindings = test_bindings(true, false);
        let error = finalize_editor(&duplicate, &bindings)
            .expect_err("duplicate managed credential")
            .to_string();
        assert!(!error.contains("BwcH"));
        assert!(!error.contains("android-password"));

        let missing = EDITABLE_CONFIG.replace(
            "tls_pinned_certificate = { from = \"managed\", id = \"pinned-certificate\" }\n",
            "",
        );
        assert!(finalize_editor(&missing, &bindings).is_err());
        let unknown = EDITABLE_CONFIG.replace("pinned-certificate", "unknown-material");
        assert!(finalize_editor(&unknown, &bindings).is_err());

        let malformed = json!({
            "schema_version": 1,
            "socks_port": 1080,
            "credential_base64": "not base64",
            "pinned_certificate_base64": test_certificate_base64(),
            "transport_secret_base64": null,
            "local_auth": null,
        })
        .to_string();
        let error = finalize_editor(EDITABLE_CONFIG, &malformed)
            .expect_err("strict Base64")
            .to_string();
        assert!(!error.contains("not base64"));
    }

    #[test]
    fn finalized_document_compiles_directly_from_memory() {
        let config = compile_inline_config(INLINE_CONFIG).expect("inline Android config");
        let crate::config::CommandConfig::Node(node) = config.command;
        assert_eq!(node.local_ingresses.len(), 1);
        assert_eq!(node.outbounds.len(), 1);
    }

    #[test]
    fn finalization_preserves_placeholder_like_comments_without_treating_them_as_values() {
        let editable = runtime_editable_config()
            .replace("# preserve this path comment", "# note: @mptunnel-custom");
        let finalized = finalize_editor(&editable, &test_bindings(false, false))
            .expect("comment-preserving finalization");
        assert!(finalized.contains("# note: @mptunnel-custom"));
        compile_inline_config(&finalized).expect("commented inline Android config");
    }

    #[test]
    fn confinement_accepts_only_self_contained_material_sources() {
        let document = inline_document(
            r#"
raw = { from = "raw", value = "text" }
implicit_raw = { value = "text" }
hex = { from = "hex", value = "00ff" }
base64 = { from = "base64", value = "AP8=" }
"#,
        );
        validate_inline_document(&document).expect("self-contained sources");
    }

    #[test]
    fn confinement_rejects_external_and_legacy_file_forms() {
        for forbidden in [
            r#"material = { from = "file", path = "secret.key" }"#,
            r#"material = { from = "env", var = "SECRET_FILE" }"#,
            r#"material = { from = "environment", variable = "SECRET" }"#,
            r#"material = { from = "managed", id = "credential" }"#,
            r#"transport_secret_file = "secret.key""#,
            r#"[logging]
file = "mptunnel.log""#,
            r#"[[routing.rule_sets]]
file = "rules.json""#,
        ] {
            let document = inline_document(forbidden);
            assert!(
                validate_inline_document(&document).is_err(),
                "forbidden Android form passed confinement: {forbidden}"
            );
        }
    }

    #[test]
    fn compilation_rejects_placeholders_oversize_and_unsanitized_parse_errors() {
        let placeholder = format!("{INLINE_CONFIG}\nvalue = \"@mptunnel-profile-secret@\"\n");
        assert!(compile_inline_config(&placeholder).is_err());
        assert!(compile_inline_config(&"x".repeat(MAX_CONFIG_BYTES + 1)).is_err());

        let canary = "secret-inline-canary";
        let invalid = format!("value = \"{canary}\"\nbroken = [");
        let error = compile_inline_config(&invalid)
            .expect_err("invalid TOML")
            .to_string();
        assert!(!error.contains(canary));
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
