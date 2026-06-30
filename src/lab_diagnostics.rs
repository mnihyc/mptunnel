use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub(crate) fn lab_diagnostic(event: &str, fields: fmt::Arguments<'_>) {
    if !env_flag("MPTUNNEL_LAB_DIAG") {
        return;
    }
    let stamp = lab_stamp();
    eprintln!(
        "mptunnel_lab_diag ts_unix_ms={} t_mono_ms={} seq={} pid={} role={} event={event} {fields}",
        stamp.unix_ms,
        stamp.mono_ms,
        next_diag_seq(),
        std::process::id(),
        lab_role(),
    );
}

pub(crate) fn lab_server_response_stream_data(
    session_id: u64,
    stream_id: u64,
    offset: u64,
    payload_bytes: usize,
) {
    if !env_flag("MPTUNNEL_LAB_DIAG") {
        return;
    }
    let counts = LAB_SENDER_SERVICE_COUNTS.get_or_init(Default::default);
    {
        let mut counts = counts.lock().expect("lab sender-service counts lock");
        counts
            .entry((session_id, stream_id))
            .or_default()
            .response_stream_data_frames += 1;
    }
    lab_diagnostic(
        "server_response_stream_data_frame",
        format_args!(
            "session_id={} stream_id={} offset={} payload_bytes={}",
            session_id, stream_id, offset, payload_bytes,
        ),
    );
}

pub(crate) fn lab_sender_service_decision(
    role: &'static str,
    session_id: Option<u64>,
    stream_id: u64,
    decision_kind: &'static str,
    frame_kind: &'static str,
    payload_bytes: usize,
    fields: fmt::Arguments<'_>,
) {
    if !env_flag("MPTUNNEL_LAB_DIAG") {
        return;
    }
    if role == "server"
        && decision_kind == "primary"
        && frame_kind == "stream_data"
        && let Some(session_id) = session_id
    {
        let counts = LAB_SENDER_SERVICE_COUNTS.get_or_init(Default::default);
        let mut counts = counts.lock().expect("lab sender-service counts lock");
        counts
            .entry((session_id, stream_id))
            .or_default()
            .sender_service_stream_data_decisions += 1;
    }
    lab_diagnostic(
        "sender_service_decision",
        format_args!(
            "role={} session_id={} stream_id={} decision_kind={} frame_kind={} payload_bytes={} {}",
            role,
            session_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
            stream_id,
            decision_kind,
            frame_kind,
            payload_bytes,
            fields,
        ),
    );
}

pub(crate) fn lab_assert_server_sender_service_balanced(session_id: u64, stream_id: u64) {
    if !env_flag("MPTUNNEL_LAB_DIAG") {
        return;
    }
    let counts = LAB_SENDER_SERVICE_COUNTS.get_or_init(Default::default);
    let counts = counts.lock().expect("lab sender-service counts lock");
    let Some(counts) = counts.get(&(session_id, stream_id)).copied() else {
        return;
    };
    lab_diagnostic(
        "sender_service_conformance",
        format_args!(
            "session_id={} stream_id={} server_response_stream_data_frames={} server_sender_service_stream_data_decisions={}",
            session_id,
            stream_id,
            counts.response_stream_data_frames,
            counts.sender_service_stream_data_decisions,
        ),
    );
    assert_eq!(
        counts.response_stream_data_frames, counts.sender_service_stream_data_decisions,
        "server response STREAM_DATA bypassed sender-service decisions for session {session_id} stream {stream_id}",
    );
}

pub(crate) fn lab_perf_record(component: &'static str, elapsed: Duration, bytes: usize) {
    if !env_flag("MPTUNNEL_LAB_PERF") {
        return;
    }
    let state = LAB_PERF_STATE.get_or_init(LabPerfState::new);
    let elapsed_us = elapsed.as_micros().max(1);
    let mut inner = state.inner.lock().expect("lab perf state lock");
    inner.record(component, elapsed_us, bytes as u128);
    if env_flag("MPTUNNEL_LAB_PERF_SAMPLES") {
        let stamp = lab_stamp();
        eprintln!(
            "mptunnel_lab_perf_sample ts_unix_ms={} t_mono_ms={} seq={} pid={} role={} component={} elapsed_us={} bytes={}",
            stamp.unix_ms,
            stamp.mono_ms,
            next_perf_seq(),
            std::process::id(),
            lab_role(),
            component,
            elapsed_us,
            bytes,
        );
    }
    if Instant::now() >= inner.next_flush_at {
        inner.flush("periodic", state.interval);
    }
}

pub(crate) fn lab_perf_flush(reason: &'static str) {
    if !env_flag("MPTUNNEL_LAB_PERF") {
        return;
    }
    let state = LAB_PERF_STATE.get_or_init(LabPerfState::new);
    state
        .inner
        .lock()
        .expect("lab perf state lock")
        .flush(reason, state.interval);
}

static LAB_PERF_STATE: OnceLock<LabPerfState> = OnceLock::new();
static LAB_STARTED_AT: OnceLock<Instant> = OnceLock::new();
static LAB_DIAG_SEQ: AtomicU64 = AtomicU64::new(1);
static LAB_PERF_SEQ: AtomicU64 = AtomicU64::new(1);
static LAB_SENDER_SERVICE_COUNTS: OnceLock<Mutex<HashMap<(u64, u64), LabSenderServiceCounts>>> =
    OnceLock::new();

#[derive(Clone, Copy, Default)]
struct LabSenderServiceCounts {
    response_stream_data_frames: u64,
    sender_service_stream_data_decisions: u64,
}

struct LabPerfState {
    interval: Duration,
    inner: Mutex<LabPerfInner>,
}

impl LabPerfState {
    fn new() -> Self {
        let interval = std::env::var("MPTUNNEL_LAB_PERF_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|millis| *millis > 0)
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_millis(1_000));
        Self {
            interval,
            inner: Mutex::new(LabPerfInner::new(interval)),
        }
    }
}

struct LabPerfInner {
    started_at: Instant,
    next_flush_at: Instant,
    metrics: BTreeMap<&'static str, LabPerfMetric>,
}

impl LabPerfInner {
    fn new(interval: Duration) -> Self {
        let started_at = Instant::now();
        Self {
            started_at,
            next_flush_at: started_at + interval,
            metrics: BTreeMap::new(),
        }
    }

    fn record(&mut self, component: &'static str, elapsed_us: u128, bytes: u128) {
        self.metrics
            .entry(component)
            .or_default()
            .record(elapsed_us, bytes);
    }

    fn flush(&mut self, reason: &'static str, interval: Duration) {
        if self.metrics.is_empty() {
            self.next_flush_at = Instant::now() + interval;
            return;
        }
        for (component, metric) in &mut self.metrics {
            if metric.interval_count == 0 {
                continue;
            }
            let stamp = lab_stamp();
            let interval_elapsed = interval.max(Duration::from_millis(1));
            let total_elapsed = self.started_at.elapsed().max(Duration::from_millis(1));
            eprintln!(
                "mptunnel_lab_perf ts_unix_ms={} t_mono_ms={} seq={} reason={reason} pid={} role={} component={} interval_ms={} interval_count={} interval_bytes={} interval_bytes_per_s={} interval_total_us={} interval_avg_us={} interval_max_us={} total_elapsed_ms={} total_count={} total_bytes={} total_bytes_per_s={} total_us={} total_avg_us={} total_max_us={}",
                stamp.unix_ms,
                stamp.mono_ms,
                next_perf_seq(),
                std::process::id(),
                lab_role(),
                component,
                duration_millis(interval_elapsed),
                metric.interval_count,
                metric.interval_bytes,
                bytes_per_second(metric.interval_bytes, interval_elapsed),
                metric.interval_total_us,
                metric.interval_avg_us(),
                metric.interval_max_us,
                duration_millis(total_elapsed),
                metric.total_count,
                metric.total_bytes,
                bytes_per_second(metric.total_bytes, total_elapsed),
                metric.total_us,
                metric.total_avg_us(),
                metric.total_max_us,
            );
            metric.reset_interval();
        }
        self.next_flush_at = Instant::now() + interval;
    }
}

#[derive(Default)]
struct LabPerfMetric {
    interval_count: u128,
    interval_bytes: u128,
    interval_total_us: u128,
    interval_max_us: u128,
    total_count: u128,
    total_bytes: u128,
    total_us: u128,
    total_max_us: u128,
}

impl LabPerfMetric {
    fn record(&mut self, elapsed_us: u128, bytes: u128) {
        self.interval_count = self.interval_count.saturating_add(1);
        self.interval_bytes = self.interval_bytes.saturating_add(bytes);
        self.interval_total_us = self.interval_total_us.saturating_add(elapsed_us);
        self.interval_max_us = self.interval_max_us.max(elapsed_us);
        self.total_count = self.total_count.saturating_add(1);
        self.total_bytes = self.total_bytes.saturating_add(bytes);
        self.total_us = self.total_us.saturating_add(elapsed_us);
        self.total_max_us = self.total_max_us.max(elapsed_us);
    }

    fn interval_avg_us(&self) -> u128 {
        average_us(self.interval_total_us, self.interval_count)
    }

    fn total_avg_us(&self) -> u128 {
        average_us(self.total_us, self.total_count)
    }

    fn reset_interval(&mut self) {
        self.interval_count = 0;
        self.interval_bytes = 0;
        self.interval_total_us = 0;
        self.interval_max_us = 0;
    }
}

fn average_us(total_us: u128, count: u128) -> u128 {
    total_us.checked_div(count).unwrap_or(0)
}

#[derive(Debug, Clone, Copy)]
struct LabStamp {
    unix_ms: u128,
    mono_ms: u128,
}

fn lab_stamp() -> LabStamp {
    let unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let mono_ms = LAB_STARTED_AT
        .get_or_init(Instant::now)
        .elapsed()
        .as_millis();
    LabStamp { unix_ms, mono_ms }
}

fn next_diag_seq() -> u64 {
    LAB_DIAG_SEQ.fetch_add(1, Ordering::Relaxed)
}

fn next_perf_seq() -> u64 {
    LAB_PERF_SEQ.fetch_add(1, Ordering::Relaxed)
}

fn lab_role() -> String {
    std::env::var("MPTUNNEL_LAB_ROLE").unwrap_or_else(|_| "unknown".to_string())
}

fn duration_millis(duration: Duration) -> u128 {
    duration.as_millis().max(1)
}

fn bytes_per_second(bytes: u128, duration: Duration) -> u128 {
    let nanos = duration.as_nanos().max(1);
    bytes
        .saturating_mul(1_000_000_000)
        .checked_div(nanos)
        .unwrap_or(0)
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}
