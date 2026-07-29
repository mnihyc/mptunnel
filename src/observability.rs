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
            let message = serde_json::to_string(record.message)
                .unwrap_or_else(|_| "\"log-error\"".to_owned());
            let _ = writeln!(
                output,
                "timestamp_unix_ms={} level={} component={} event={} message={} suppressed={}",
                record.timestamp_unix_ms,
                record.level,
                record.component,
                record.event,
                message,
                record.suppressed,
            );
        }
    }
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
            let _ = writeln!(
                line,
                "timestamp_unix_ms={} level={} component={} event={} flow_id={} origin={} network={} inbound={} target={} outbound={} balancer={}",
                record.timestamp_unix_ms,
                record.level,
                record.component,
                record.event,
                record.flow_id,
                record.origin,
                record.network,
                inbound,
                target,
                outbound,
                balancer,
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
            let _ = writeln!(
                line,
                "timestamp_unix_ms={} level={} component={} event={} flow_id={} network={} outcome={} duration_ms={} to_peer_bytes={} to_peer_packets={} from_peer_bytes={} from_peer_packets={}",
                record.timestamp_unix_ms,
                record.level,
                record.component,
                record.event,
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
    let mut redacted = message.replace(['\r', '\n'], " ");
    for marker in [
        "bearer ",
        "authorization:",
        "authorization=",
        "token=",
        "token:",
        "secret=",
        "secret:",
        "password=",
        "password:",
        "credential_secret=",
        "credential-secret=",
    ] {
        redacted = redact_after_marker(&redacted, marker);
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

fn redact_after_marker(value: &str, marker: &str) -> String {
    let lower = value.to_ascii_lowercase();
    let mut output = String::with_capacity(value.len());
    let mut cursor = 0;
    while let Some(relative_start) = lower[cursor..].find(marker) {
        let marker_start = cursor + relative_start;
        let mut secret_start = marker_start + marker.len();
        while value.as_bytes().get(secret_start) == Some(&b' ') {
            secret_start += 1;
        }
        output.push_str(&value[cursor..secret_start]);
        if secret_start >= value.len() {
            cursor = secret_start;
            break;
        }
        let secret_end = value[secret_start..]
            .char_indices()
            .find_map(|(offset, character)| {
                (character.is_whitespace() || matches!(character, ',' | ';' | ']' | '}' | '&'))
                    .then_some(secret_start + offset)
            })
            .unwrap_or(value.len());
        output.push_str("<redacted>");
        cursor = secret_end;
    }
    output.push_str(&value[cursor..]);
    output
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
mod tests {
    use super::*;

    #[test]
    fn records_are_bounded_structured_and_redacted_in_both_formats() {
        let mut bounded = BoundedMessage::new(32);
        std::fmt::Write::write_fmt(
            &mut bounded,
            format_args!(
                "authorization: bearer very-secret-value\n{}",
                "x".repeat(128)
            ),
        )
        .expect("format bounded message");
        let message = bound_message(redact_message(&bounded.finish()), 32);
        assert!(message.len() <= 32);
        assert!(!message.contains('\n'));
        assert!(!message.contains("very-secret"));
        assert!(message.contains("<redacted>"));

        let record = LogRecord {
            timestamp_unix_ms: 7,
            level: "warn",
            component: "management",
            event: "request_failed",
            message: &message,
            suppressed: 3,
        };
        let mut json = Vec::new();
        write_record(&mut json, LogFormat::Json, &record);
        let parsed: serde_json::Value = serde_json::from_slice(&json).expect("one JSON record");
        assert_eq!(parsed["timestamp_unix_ms"], 7);
        assert_eq!(parsed["level"], "warn");
        assert_eq!(parsed["component"], "management");
        assert_eq!(parsed["event"], "request_failed");
        assert_eq!(parsed["suppressed"], 3);

        let mut text = Vec::new();
        write_record(&mut text, LogFormat::Text, &record);
        let text = String::from_utf8(text).expect("UTF-8 text record");
        assert!(text.starts_with("timestamp_unix_ms=7 level=warn"));
        assert_eq!(text.lines().count(), 1);
    }

    #[test]
    fn limiter_bounds_each_call_site_and_reports_suppressed_events() {
        let limiter = RateLimiter::with_policy(100, 2);
        assert_eq!(limiter.admit(1), Some(0));
        assert_eq!(limiter.admit(2), Some(0));
        assert_eq!(limiter.admit(3), None);
        assert_eq!(limiter.admit(4), None);
        assert_eq!(limiter.admit(101), Some(2));
    }
}
