//! Small process-event logger for lifecycle, control, and fault boundaries.
//!
//! The data plane does not enqueue log records. Each call site has a fixed
//! burst limiter, messages are truncated before allocation can grow beyond the
//! record bound, and records are written synchronously to stderr. The default
//! format is structured text; `MPTUNNEL_LOG_FORMAT=json` selects one JSON
//! object per line.

use serde::Serialize;
use std::fmt;
use std::fmt::Write as _;
use std::io::Write as _;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_WINDOW_MS: u64 = 10_000;
const DEFAULT_BURST: u32 = 4;
const MESSAGE_LIMIT: usize = 2_048;

static LEVEL_FILTER: AtomicU8 = AtomicU8::new(Level::Info as u8);
static OUTPUT_FORMAT: OnceLock<OutputFormat> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub(crate) enum Level {
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
    Trace = 5,
}

impl Level {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum OutputFormat {
    Text = 0,
    Json = 1,
}

impl OutputFormat {
    fn from_environment() -> Self {
        match std::env::var("MPTUNNEL_LOG_FORMAT") {
            Ok(value) if value.eq_ignore_ascii_case("json") => Self::Json,
            _ => Self::Text,
        }
    }
}

pub(crate) fn configure(level: &str) {
    let filter = match level {
        "off" => 0,
        "error" => Level::Error as u8,
        "warn" => Level::Warn as u8,
        "info" => Level::Info as u8,
        "debug" => Level::Debug as u8,
        "trace" => Level::Trace as u8,
        _ => Level::Info as u8,
    };
    LEVEL_FILTER.store(filter, Ordering::Relaxed);
    let _ = OUTPUT_FORMAT.get_or_init(OutputFormat::from_environment);
}

pub(crate) fn enabled(level: Level) -> bool {
    level as u8 <= LEVEL_FILTER.load(Ordering::Relaxed)
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
    level: Level,
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
    let format = *OUTPUT_FORMAT.get_or_init(OutputFormat::from_environment);
    let mut line = Vec::with_capacity(message.len().saturating_add(192));
    write_record(&mut line, format, &record);
    let _ = std::io::stderr().lock().write_all(&line);
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

fn write_record(output: &mut Vec<u8>, format: OutputFormat, record: &LogRecord<'_>) {
    match format {
        OutputFormat::Json => {
            let _ = serde_json::to_writer(&mut *output, record);
            output.push(b'\n');
        }
        OutputFormat::Text => {
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
        if $crate::observability::enabled($crate::observability::Level::$level) {
            $crate::observability::emit(
                $crate::observability::Level::$level,
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
        write_record(&mut json, OutputFormat::Json, &record);
        let parsed: serde_json::Value = serde_json::from_slice(&json).expect("one JSON record");
        assert_eq!(parsed["timestamp_unix_ms"], 7);
        assert_eq!(parsed["level"], "warn");
        assert_eq!(parsed["component"], "management");
        assert_eq!(parsed["event"], "request_failed");
        assert_eq!(parsed["suppressed"], 3);

        let mut text = Vec::new();
        write_record(&mut text, OutputFormat::Text, &record);
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
