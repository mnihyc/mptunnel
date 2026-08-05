//! Process-event logger for lifecycle, control, fault, and optional Product
//! flow boundaries.
//!
//! The data plane does not enqueue log records. Each call site has a fixed
//! burst limiter, messages are truncated before allocation can grow beyond the
//! record bound, and disabled records stop at one relaxed atomic read.
//! Sanitized flow lifecycle records are separately opt-in and never enter a
//! payload, packet, carrier, scheduler, or congestion-control path.

use crate::config::{CanonicalConfigStore, LogFormat, LogLevel, LoggingConfig};
use serde::Serialize;
use std::fmt;
use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;

const DEFAULT_WINDOW_MS: u64 = 10_000;
const DEFAULT_BURST: u32 = 4;
const MESSAGE_LIMIT: usize = 2_048;

static LEVEL_FILTER: AtomicU8 = AtomicU8::new(LogLevel::Info as u8);
static FLOW_EVENTS: AtomicBool = AtomicBool::new(false);
static LOGGER: OnceLock<Logger> = OnceLock::new();

struct Logger {
    output: RwLock<Arc<Output>>,
    emission: Mutex<()>,
}

struct Output {
    format: LogFormat,
    console: bool,
    file: Mutex<Option<FileSink>>,
}

struct FileSink {
    path: PathBuf,
    file: File,
}

pub(crate) struct PreparedLogger {
    level: LogLevel,
    flow_events: bool,
    output: Arc<Output>,
}

#[derive(Debug)]
pub enum ConfigureError {
    FileOpen {
        path: PathBuf,
        source: std::io::Error,
    },
    FileIdentity {
        path: PathBuf,
        source: std::io::Error,
    },
    ConfigStorePath {
        path: PathBuf,
    },
}

impl fmt::Display for ConfigureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FileOpen { path, source } => {
                write!(
                    formatter,
                    "failed to open log file {}: {source}",
                    path.display()
                )
            }
            Self::FileIdentity { path, source } => write!(
                formatter,
                "failed to verify log file identity for {}: {source}",
                path.display()
            ),
            Self::ConfigStorePath { path } => write!(
                formatter,
                "log file {} conflicts with the canonical configuration store",
                path.display()
            ),
        }
    }
}

impl std::error::Error for ConfigureError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::FileOpen { source, .. } | Self::FileIdentity { source, .. } => Some(source),
            Self::ConfigStorePath { .. } => None,
        }
    }
}

impl Logger {
    fn new() -> Self {
        Self {
            output: RwLock::new(Arc::new(Output {
                format: LogFormat::Text,
                console: true,
                file: Mutex::new(None),
            })),
            emission: Mutex::new(()),
        }
    }

    fn snapshot(&self) -> Arc<Output> {
        self.output
            .read()
            .expect("logger output lock poisoned")
            .clone()
    }

    fn install(&self, prepared: PreparedLogger) {
        let _emission = self.emission.lock().expect("logger emission lock poisoned");
        let previous = std::mem::replace(
            &mut *self.output.write().expect("logger output lock poisoned"),
            prepared.output,
        );
        LEVEL_FILTER.store(prepared.level as u8, Ordering::Relaxed);
        FLOW_EVENTS.store(prepared.flow_events, Ordering::Relaxed);
        previous.flush();
    }
}

impl Output {
    fn write(&self, line: &[u8]) {
        if self.console {
            let _ = std::io::stderr().lock().write_all(line);
        }
        let failed_path = {
            let mut file = self.file.lock().expect("logger file lock poisoned");
            let Some(sink) = file.as_mut() else {
                return;
            };
            if sink.file.write_all(line).is_ok() {
                return;
            }
            let failed_path = sink.path.clone();
            *file = None;
            failed_path
        };
        let message = format!(
            "log file {} failed during write and was disabled",
            failed_path.display()
        );
        let record = LogRecord {
            timestamp_unix_ms: unix_millis(),
            level: LogLevel::Error.as_str(),
            component: "logging",
            event: "file_disabled",
            message: &message,
            suppressed: 0,
        };
        let mut emergency = Vec::with_capacity(message.len().saturating_add(192));
        write_record(&mut emergency, self.format, &record);
        let _ = std::io::stderr().lock().write_all(&emergency);
    }

    fn flush(&self) {
        if let Some(sink) = self
            .file
            .lock()
            .expect("logger file lock poisoned")
            .as_mut()
        {
            let _ = sink.file.flush();
        }
    }
}

fn logger() -> &'static Logger {
    LOGGER.get_or_init(Logger::new)
}

pub(crate) fn prepare(config: &LoggingConfig) -> Result<PreparedLogger, ConfigureError> {
    let file = config
        .file
        .as_ref()
        .map(|path| {
            let mut options = OpenOptions::new();
            options.create(true).append(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            options
                .open(path)
                .map(|file| FileSink {
                    path: path.clone(),
                    file,
                })
                .map_err(|source| ConfigureError::FileOpen {
                    path: path.clone(),
                    source,
                })
        })
        .transpose()?;
    Ok(PreparedLogger {
        level: config.level,
        flow_events: config.flow_events,
        output: Arc::new(Output {
            format: config.format,
            console: config.console,
            file: Mutex::new(file),
        }),
    })
}

pub(crate) fn validate_store_path(
    config: &LoggingConfig,
    store: &CanonicalConfigStore,
) -> Result<(), ConfigureError> {
    validate_owned_paths(config, &store.owned_paths())
}

pub(crate) fn prepare_for_store(
    config: &LoggingConfig,
    store: &CanonicalConfigStore,
) -> Result<PreparedLogger, ConfigureError> {
    prepare_for_owned_paths(config, &store.owned_paths())
}

pub(crate) fn configure_for_owned_paths(
    config: &LoggingConfig,
    owned_paths: &[PathBuf; 3],
) -> Result<(), ConfigureError> {
    let owned_paths = owned_paths.each_ref().map(PathBuf::as_path);
    let prepared = prepare_for_owned_paths(config, &owned_paths)?;
    install(prepared);
    Ok(())
}

fn validate_owned_paths(
    config: &LoggingConfig,
    owned_paths: &[&Path],
) -> Result<(), ConfigureError> {
    if let Some(path) = config.file.as_ref()
        && owned_paths
            .iter()
            .any(|owned| crate::config::paths_equivalent(path, owned))
    {
        return Err(ConfigureError::ConfigStorePath { path: path.clone() });
    }
    Ok(())
}

fn prepare_for_owned_paths(
    config: &LoggingConfig,
    owned_paths: &[&Path],
) -> Result<PreparedLogger, ConfigureError> {
    validate_owned_paths(config, owned_paths)?;
    let prepared = prepare(config)?;
    if let Some(path) = config.file.as_ref() {
        let file = prepared
            .output
            .file
            .lock()
            .expect("prepared logger file lock poisoned");
        if let Some(sink) = file.as_ref() {
            let candidate =
                same_file::Handle::from_file(sink.file.try_clone().map_err(|source| {
                    ConfigureError::FileIdentity {
                        path: path.clone(),
                        source,
                    }
                })?)
                .map_err(|source| ConfigureError::FileIdentity {
                    path: path.clone(),
                    source,
                })?;
            for owned in owned_paths {
                match same_file::Handle::from_path(owned) {
                    Ok(handle) if handle == candidate => {
                        return Err(ConfigureError::ConfigStorePath { path: path.clone() });
                    }
                    Ok(_) => {}
                    Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
                    Err(source) => {
                        return Err(ConfigureError::FileIdentity {
                            path: path.clone(),
                            source,
                        });
                    }
                }
            }
        }
    }
    Ok(prepared)
}

pub(crate) fn install(prepared: PreparedLogger) {
    logger().install(prepared);
}

pub(crate) fn configure(config: &LoggingConfig) -> Result<(), ConfigureError> {
    let prepared = prepare(config)?;
    install(prepared);
    Ok(())
}

pub(crate) fn configure_for_store(
    config: &LoggingConfig,
    store: &CanonicalConfigStore,
) -> Result<(), ConfigureError> {
    let prepared = prepare_for_store(config, store)?;
    install(prepared);
    Ok(())
}

pub(crate) fn enabled(level: LogLevel) -> bool {
    level as u8 <= LEVEL_FILTER.load(Ordering::Relaxed)
}

pub(crate) fn flow_events_enabled() -> bool {
    FLOW_EVENTS.load(Ordering::Relaxed) && enabled(LogLevel::Info)
}

#[derive(Debug)]
pub(crate) struct RateLimiter {
    window_ms: u64,
    burst: u32,
    window_started_ms: AtomicU64,
    emitted: AtomicU32,
    suppressed: AtomicU64,
}

impl RateLimiter {
    pub(crate) const fn standard() -> Self {
        Self::with_policy(DEFAULT_WINDOW_MS, DEFAULT_BURST)
    }

    const fn with_policy(window_ms: u64, burst: u32) -> Self {
        Self {
            window_ms,
            burst,
            window_started_ms: AtomicU64::new(0),
            emitted: AtomicU32::new(0),
            suppressed: AtomicU64::new(0),
        }
    }

    fn admit(&self, now_ms: u64) -> Option<u64> {
        loop {
            let window_started = self.window_started_ms.load(Ordering::Relaxed);
            if now_ms.saturating_sub(window_started) >= self.window_ms {
                if self
                    .window_started_ms
                    .compare_exchange(window_started, now_ms, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    self.emitted.store(1, Ordering::Relaxed);
                    return Some(self.suppressed.swap(0, Ordering::Relaxed));
                }
                continue;
            }
            let emitted = self.emitted.load(Ordering::Relaxed);
            if emitted >= self.burst {
                let _ =
                    self.suppressed
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                            Some(value.saturating_add(1))
                        });
                return None;
            }
            if self
                .emitted
                .compare_exchange_weak(emitted, emitted + 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return Some(0);
            }
        }
    }
}

pub(crate) fn emit(
    level: LogLevel,
    component: &'static str,
    event: &'static str,
    limiter: &RateLimiter,
    arguments: fmt::Arguments<'_>,
) {
    if !enabled(level) {
        return;
    }
    let timestamp_unix_ms = unix_millis();
    let Some(suppressed) = limiter.admit(timestamp_unix_ms) else {
        return;
    };
    emit_record(
        level,
        component,
        event,
        timestamp_unix_ms,
        suppressed,
        arguments,
    );
}

/// Emits a bounded control-plane lifecycle record without fault-rate limiting.
///
/// Callers use this only for finite state transitions and configured inventory,
/// never from traffic, carrier retry, probe, or sampling loops.
pub(crate) fn emit_lifecycle(
    level: LogLevel,
    component: &'static str,
    event: &'static str,
    arguments: fmt::Arguments<'_>,
) {
    if !enabled(level) {
        return;
    }
    emit_record(level, component, event, unix_millis(), 0, arguments);
}

fn emit_record(
    level: LogLevel,
    component: &'static str,
    event: &'static str,
    timestamp_unix_ms: u64,
    suppressed: u64,
    arguments: fmt::Arguments<'_>,
) {
    let mut bounded = BoundedMessage::new(MESSAGE_LIMIT);
    let _ = bounded.write_fmt(arguments);
    let message = bound_message(redact_message(&bounded.finish()), MESSAGE_LIMIT);
    let record = LogRecord {
        timestamp_unix_ms,
        level: level.as_str(),
        component,
        event,
        message: &message,
        suppressed,
    };
    let logger = logger();
    let _emission = logger
        .emission
        .lock()
        .expect("logger emission lock poisoned");
    if !enabled(level) {
        return;
    }
    let output = logger.snapshot();
    let mut line = Vec::with_capacity(message.len().saturating_add(192));
    write_record(&mut line, output.format, &record);
    output.write(&line);
}

#[derive(Serialize)]
struct LogRecord<'a> {
    timestamp_unix_ms: u64,
    level: &'static str,
    component: &'static str,
    event: &'static str,
    message: &'a str,
    #[serde(skip_serializing_if = "is_zero")]
    suppressed: u64,
}

const fn is_zero(value: &u64) -> bool {
    *value == 0
}

fn write_record(output: &mut Vec<u8>, format: LogFormat, record: &LogRecord<'_>) {
    match format {
        LogFormat::Json => {
            let _ = serde_json::to_writer(&mut *output, record);
            output.push(b'\n');
        }
        LogFormat::Text => {
            write_text_header(
                output,
                record.timestamp_unix_ms,
                record.level,
                record.component,
                record.event,
            );
            let _ = write!(output, "{}", record.message);
            write_suppressed(output, record.suppressed);
            output.push(b'\n');
        }
    }
}

fn write_text_header(
    output: &mut Vec<u8>,
    timestamp_unix_ms: u64,
    level: &str,
    component: &str,
    event: &str,
) {
    let timestamp = readable_timestamp(timestamp_unix_ms);
    let _ = write!(
        output,
        "{timestamp} {:<5} {component}.{event}: ",
        text_level(level)
    );
}

fn text_level(level: &str) -> &str {
    match level {
        "error" => "ERROR",
        "warn" => "WARN",
        "info" => "INFO",
        _ => "UNKNOWN",
    }
}

fn write_suppressed(output: &mut Vec<u8>, suppressed: u64) {
    if suppressed == 0 {
        return;
    }
    let noun = if suppressed == 1 { "event" } else { "events" };
    let _ = write!(output, " ({suppressed} similar {noun} suppressed)");
}

#[derive(Serialize)]
struct FlowOpenRecord<'a> {
    timestamp_unix_ms: u64,
    level: &'static str,
    component: &'static str,
    event: &'static str,
    flow_id: String,
    origin: &'a str,
    network: &'a str,
    inbound: &'a str,
    target: &'a str,
    outbound: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    balancer: Option<&'a str>,
}

#[derive(Serialize)]
struct FlowCloseRecord<'a> {
    timestamp_unix_ms: u64,
    level: &'static str,
    component: &'static str,
    event: &'static str,
    flow_id: String,
    network: &'a str,
    outcome: &'a str,
    duration_ms: u64,
    to_peer_bytes: u64,
    to_peer_packets: u64,
    from_peer_bytes: u64,
    from_peer_packets: u64,
}

pub(crate) struct FlowLogToken {
    output: Arc<Output>,
}

impl fmt::Debug for FlowLogToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FlowLogToken")
    }
}

pub(crate) fn emit_flow_open(
    flow_id: u64,
    origin: &str,
    network: &str,
    inbound: &str,
    target: &str,
    outbound: &str,
    balancer: Option<&str>,
) -> Option<FlowLogToken> {
    let logger = logger();
    let _emission = logger
        .emission
        .lock()
        .expect("logger emission lock poisoned");
    if !flow_events_enabled() {
        return None;
    }
    let output = logger.snapshot();
    let record = FlowOpenRecord {
        timestamp_unix_ms: unix_millis(),
        level: LogLevel::Info.as_str(),
        component: "flow",
        event: "opened",
        flow_id: flow_id.to_string(),
        origin,
        network,
        inbound,
        target,
        outbound,
        balancer,
    };
    let mut line = Vec::with_capacity(320);
    match output.format {
        LogFormat::Json => {
            let _ = serde_json::to_writer(&mut line, &record);
            line.push(b'\n');
        }
        LogFormat::Text => {
            let inbound = quote_text_field(record.inbound);
            let target = quote_text_field(record.target);
            let outbound = quote_text_field(record.outbound);
            let balancer = record
                .balancer
                .map(quote_text_field)
                .unwrap_or_else(|| "null".to_string());
            write_text_header(
                &mut line,
                record.timestamp_unix_ms,
                record.level,
                record.component,
                record.event,
            );
            let _ = writeln!(
                line,
                "id={} origin={} network={} inbound={} destination={} outbound={} balancer={}",
                record.flow_id, record.origin, record.network, inbound, target, outbound, balancer,
            );
        }
    }
    output.write(&line);
    Some(FlowLogToken { output })
}

pub(crate) struct FlowIo {
    pub(crate) to_peer_bytes: u64,
    pub(crate) to_peer_packets: u64,
    pub(crate) from_peer_bytes: u64,
    pub(crate) from_peer_packets: u64,
}

pub(crate) fn emit_flow_close(
    token: &FlowLogToken,
    flow_id: u64,
    network: &str,
    outcome: &str,
    duration_ms: u64,
    io: FlowIo,
) {
    let logger = logger();
    let _emission = logger
        .emission
        .lock()
        .expect("logger emission lock poisoned");
    let output = &token.output;
    let record = FlowCloseRecord {
        timestamp_unix_ms: unix_millis(),
        level: LogLevel::Info.as_str(),
        component: "flow",
        event: "closed",
        flow_id: flow_id.to_string(),
        network,
        outcome,
        duration_ms,
        to_peer_bytes: io.to_peer_bytes,
        to_peer_packets: io.to_peer_packets,
        from_peer_bytes: io.from_peer_bytes,
        from_peer_packets: io.from_peer_packets,
    };
    let mut line = Vec::with_capacity(320);
    match output.format {
        LogFormat::Json => {
            let _ = serde_json::to_writer(&mut line, &record);
            line.push(b'\n');
        }
        LogFormat::Text => {
            write_text_header(
                &mut line,
                record.timestamp_unix_ms,
                record.level,
                record.component,
                record.event,
            );
            let _ = writeln!(
                line,
                "id={} network={} outcome={} duration_ms={} to_peer_bytes={} to_peer_packets={} from_peer_bytes={} from_peer_packets={}",
                record.flow_id,
                record.network,
                record.outcome,
                record.duration_ms,
                record.to_peer_bytes,
                record.to_peer_packets,
                record.from_peer_bytes,
                record.from_peer_packets,
            );
        }
    }
    output.write(&line);
}

fn quote_text_field(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"log-error\"".to_string())
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn readable_timestamp(timestamp_unix_ms: u64) -> String {
    let nanoseconds = i128::from(timestamp_unix_ms).saturating_mul(1_000_000);
    let Ok(timestamp) = OffsetDateTime::from_unix_timestamp_nanos(nanoseconds) else {
        return format!("unix:{timestamp_unix_ms}");
    };
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        timestamp.year(),
        u8::from(timestamp.month()),
        timestamp.day(),
        timestamp.hour(),
        timestamp.minute(),
        timestamp.second(),
        timestamp.millisecond(),
    )
}

struct BoundedMessage {
    value: String,
    limit: usize,
    truncated: bool,
}

impl BoundedMessage {
    fn new(limit: usize) -> Self {
        Self {
            value: String::with_capacity(limit.min(256)),
            limit,
            truncated: false,
        }
    }

    fn finish(mut self) -> String {
        if self.truncated && self.limit >= 3 {
            while self.value.len() > self.limit - 3 {
                self.value.pop();
            }
            self.value.push_str("...");
        }
        self.value
    }
}

impl fmt::Write for BoundedMessage {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let remaining = self.limit.saturating_sub(self.value.len());
        if value.len() <= remaining {
            self.value.push_str(value);
            return Ok(());
        }
        let mut boundary = remaining.min(value.len());
        while boundary > 0 && !value.is_char_boundary(boundary) {
            boundary -= 1;
        }
        self.value.push_str(&value[..boundary]);
        self.truncated = true;
        Ok(())
    }
}

fn redact_message(message: &str) -> String {
    let mut redacted = message
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    for key in [
        "authorization",
        "proxy-authorization",
        "proxy_authorization",
        "cookie",
        "set-cookie",
        "token",
        "management_token",
        "api_key",
        "api-key",
        "access_token",
        "refresh_token",
        "secret",
        "password",
        "credential_secret",
        "credential-secret",
        "transport_shared_secret",
        "private_key",
        "private-key",
    ] {
        redacted = redact_key_value(&redacted, key);
    }
    redacted
}

fn bound_message(mut message: String, limit: usize) -> String {
    if message.len() <= limit {
        return message;
    }
    let target = limit.saturating_sub(3);
    while message.len() > target {
        message.pop();
    }
    if limit >= 3 {
        message.push_str("...");
    }
    message
}

fn redact_key_value(value: &str, key: &str) -> String {
    let lower = value.to_ascii_lowercase();
    let mut output = String::with_capacity(value.len());
    let mut cursor = 0;
    while let Some(relative_start) = lower[cursor..].find(key) {
        let key_start = cursor + relative_start;
        let key_end = key_start + key.len();
        if key_start > 0 && is_sensitive_key_character(value.as_bytes()[key_start - 1]) {
            output.push_str(&value[cursor..key_end]);
            cursor = key_end;
            continue;
        }

        let bytes = value.as_bytes();
        let mut separator = key_end;
        if matches!(bytes.get(separator), Some(b'"' | b'\'')) {
            separator += 1;
        }
        while bytes.get(separator).is_some_and(u8::is_ascii_whitespace) {
            separator += 1;
        }
        if !matches!(bytes.get(separator), Some(b':' | b'=')) {
            output.push_str(&value[cursor..key_end]);
            cursor = key_end;
            continue;
        }

        let mut secret_start = separator + 1;
        while bytes.get(secret_start).is_some_and(u8::is_ascii_whitespace) {
            secret_start += 1;
        }
        output.push_str(&value[cursor..secret_start]);
        let Some(first) = bytes.get(secret_start).copied() else {
            cursor = secret_start;
            break;
        };
        let secret_end = if matches!(first, b'"' | b'\'') {
            output.push(char::from(first));
            secret_start += 1;
            quoted_secret_end(bytes, secret_start, first)
        } else if matches!(
            key,
            "authorization" | "proxy-authorization" | "proxy_authorization"
        ) {
            authorization_secret_end(value, secret_start)
        } else if matches!(key, "cookie" | "set-cookie") {
            value.len()
        } else {
            unquoted_secret_end(value, secret_start)
        };
        output.push_str("<redacted>");
        cursor = secret_end;
    }
    output.push_str(&value[cursor..]);
    output
}

fn is_sensitive_key_character(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
}

fn quoted_secret_end(bytes: &[u8], start: usize, quote: u8) -> usize {
    let mut escaped = false;
    for (offset, byte) in bytes[start..].iter().copied().enumerate() {
        if byte == quote && !escaped {
            return start + offset;
        }
        escaped = byte == b'\\' && !escaped;
        if byte != b'\\' {
            escaped = false;
        }
    }
    bytes.len()
}

fn authorization_secret_end(value: &str, start: usize) -> usize {
    let remaining = &value[start..];
    for scheme in ["basic", "bearer"] {
        if remaining
            .get(..scheme.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(scheme))
            && remaining
                .as_bytes()
                .get(scheme.len())
                .is_some_and(u8::is_ascii_whitespace)
        {
            let mut credential_start = start + scheme.len();
            while value
                .as_bytes()
                .get(credential_start)
                .is_some_and(u8::is_ascii_whitespace)
            {
                credential_start += 1;
            }
            return unquoted_secret_end(value, credential_start);
        }
    }
    unquoted_secret_end(value, start)
}

fn unquoted_secret_end(value: &str, start: usize) -> usize {
    value[start..]
        .char_indices()
        .find_map(|(offset, character)| {
            (character.is_whitespace() || matches!(character, ',' | ';' | ']' | '}' | '&'))
                .then_some(start + offset)
        })
        .unwrap_or(value.len())
}

macro_rules! process_event {
    ($level:ident, $component:literal, $event:literal, $($argument:tt)*) => {{
        static LIMITER: $crate::observability::RateLimiter =
            $crate::observability::RateLimiter::standard();
        if $crate::observability::enabled($crate::config::LogLevel::$level) {
            $crate::observability::emit(
                $crate::config::LogLevel::$level,
                $component,
                $event,
                &LIMITER,
                format_args!($($argument)*),
            );
        }
    }};
}

pub(crate) use process_event;

#[cfg(test)]
#[path = "tests_observability.rs"]
mod tests;
