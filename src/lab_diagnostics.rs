use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

pub(crate) fn lab_diagnostic(event: &str, fields: fmt::Arguments<'_>) {
    if !env_flag("MPTUNNEL_LAB_DIAG") {
        return;
    }
    eprintln!("mptunnel_lab_diag event={event} {fields}");
}

pub(crate) fn lab_perf_record(component: &'static str, elapsed: Duration, bytes: usize) {
    if !env_flag("MPTUNNEL_LAB_PERF") {
        return;
    }
    let state = LAB_PERF_STATE.get_or_init(LabPerfState::new);
    let elapsed_us = elapsed.as_micros().max(1);
    let mut inner = state.inner.lock().expect("lab perf state lock");
    inner.record(component, elapsed_us, bytes as u128);
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
    next_flush_at: Instant,
    metrics: BTreeMap<&'static str, LabPerfMetric>,
}

impl LabPerfInner {
    fn new(interval: Duration) -> Self {
        Self {
            next_flush_at: Instant::now() + interval,
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
            eprintln!(
                "mptunnel_lab_perf reason={reason} pid={} component={} interval_count={} interval_bytes={} interval_total_us={} interval_avg_us={} interval_max_us={} total_count={} total_bytes={} total_us={} total_avg_us={} total_max_us={}",
                std::process::id(),
                component,
                metric.interval_count,
                metric.interval_bytes,
                metric.interval_total_us,
                metric.interval_avg_us(),
                metric.interval_max_us,
                metric.total_count,
                metric.total_bytes,
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

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}
