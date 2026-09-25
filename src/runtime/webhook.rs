//! Generation-scoped, bounded best-effort webhook delivery.
//!
//! Lifecycle owners only build a frozen JSON value after checking the
//! publisher's interest mask. `emit` is synchronous and uses `try_lock` plus
//! fixed queue limits; DNS, rendering, retries and network I/O happen only in
//! the separately supervised worker below.

pub(in crate::runtime) mod egress;
mod http;

use crate::config::WebhookConfig;
use crate::runtime::identity::random_u64;
use crate::runtime::outbound_registry::RuntimeOutboundRegistry;
use crate::runtime::readiness::RuntimeGenerationControl;
use crate::webhook::{EventKind, WebhookRule};
use serde_json::{Map, Value, json};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::{Notify, watch};
use tokio::task::{JoinHandle, JoinSet};

const MAX_EVENT_BYTES: usize = 16 * 1024;
const MAX_EVENT_NODES: usize = 8192;
const MAX_EVENT_DEPTH: usize = 32;
const MAX_RENDERED_BODY_BYTES: usize = 64 * 1024;
const MAX_RENDERED_HEADERS_BYTES: usize = 16 * 1024;
const MAX_RENDERED_URL_BYTES: usize = 8 * 1024;
const MAX_ATTEMPT_HEADER_BYTES: usize = 16 * 1024;
const EVENT_ID_PREFIX: &str = "mptunnel";

static PROCESS_EVENT_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Default)]
pub(crate) struct EventPublisher {
    shared: Option<Arc<Shared>>,
    #[cfg(test)]
    test_capture: Option<Arc<TestCapture>>,
}

impl EventPublisher {
    pub(crate) fn interested(&self, kind: EventKind) -> bool {
        let bit = 1u64 << (kind as usize % 64);
        if let Some(shared) = &self.shared {
            return shared.interested[kind as usize / 64].load(Ordering::Relaxed) & bit != 0;
        }
        #[cfg(test)]
        if let Some(capture) = &self.test_capture {
            return capture.interested[kind as usize / 64] & bit != 0;
        }
        false
    }

    /// Publishes a frozen event-specific namespace. The canonical envelope,
    /// event ID and observation timestamps are added here exactly once, so a
    /// retry retains identical IDs and bytes.
    pub(crate) fn emit(&self, kind: EventKind, subject: &str, data: Value) {
        let Some(shared) = &self.shared else {
            #[cfg(test)]
            if let Some(capture) = &self.test_capture
                && capture.interested[kind as usize / 64] & (1u64 << (kind as usize % 64)) != 0
            {
                capture.push(kind, subject, data);
            }
            return;
        };
        if !self.interested(kind) || shared.closed.load(Ordering::Acquire) {
            return;
        }
        let Some(event) = make_event(shared, kind, subject, data) else {
            shared
                .stats
                .oversized_events
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        enqueue_source(shared, event, false, None);
    }

    #[cfg(test)]
    pub(crate) fn test_capture(
        kinds: &[EventKind],
        max_events: usize,
    ) -> (Self, WebhookTestCaptureHandle) {
        let mut interested = [0u64; 1];
        for kind in kinds {
            interested[*kind as usize / 64] |= 1u64 << (*kind as usize % 64);
        }
        let capture = Arc::new(TestCapture {
            interested,
            max_events: max_events.min(256),
            entries: Mutex::new(VecDeque::with_capacity(max_events.min(256))),
            dropped: AtomicU64::new(0),
        });
        (
            Self {
                shared: None,
                test_capture: Some(capture.clone()),
            },
            WebhookTestCaptureHandle { capture },
        )
    }

    /// Registers a path-owned snapshot callback. The runtime invokes it only
    /// for configured interval rules, outside the queue lock. The returned
    /// guard releases the registration when that source owner retires.
    pub(crate) fn register_interval_snapshot(
        &self,
        subject: impl Into<Arc<str>>,
        provider: Arc<dyn Fn() -> Option<Value> + Send + Sync + 'static>,
    ) -> IntervalSnapshotRegistration {
        let Some(shared) = &self.shared else {
            return IntervalSnapshotRegistration::empty();
        };
        if !self.interested(EventKind::PathInterval) {
            return IntervalSnapshotRegistration::empty();
        }
        let id = shared.next_interval_source.fetch_add(1, Ordering::Relaxed);
        match shared.interval_sources.try_lock() {
            Ok(mut sources) => {
                sources.insert(
                    id,
                    IntervalSource {
                        subject: subject.into(),
                        provider,
                    },
                );
                IntervalSnapshotRegistration {
                    shared: Arc::downgrade(shared),
                    id: Some(id),
                }
            }
            Err(_) => {
                shared
                    .stats
                    .interval_sources_rejected
                    .fetch_add(1, Ordering::Relaxed);
                IntervalSnapshotRegistration::empty()
            }
        }
    }
}

#[cfg(test)]
struct TestCapture {
    interested: [u64; 1],
    max_events: usize,
    entries: Mutex<VecDeque<Value>>,
    dropped: AtomicU64,
}

#[cfg(test)]
impl TestCapture {
    fn push(&self, kind: EventKind, subject: &str, data: Value) {
        let Some(_) = bounded_json_bytes(&data) else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let event_id = format!(
            "test-{sequence:016x}",
            sequence = PROCESS_EVENT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        let sequence = data
            .get("subject_sequence")
            .or_else(|| data.pointer("/path/subject_sequence"))
            .cloned()
            .unwrap_or(Value::Null);
        let observed_at = utc_now();
        let mut event = Map::new();
        event.insert("id".into(), Value::String(event_id));
        event.insert("type".into(), Value::String(kind.as_str().to_owned()));
        event.insert("occurred_at".into(), Value::String(observed_at.clone()));
        event.insert("observed_at".into(), Value::String(observed_at));
        event.insert("subject_id".into(), Value::String(subject.to_owned()));
        event.insert("subject_sequence".into(), sequence.clone());
        if let Some(initial) = data
            .get("initial")
            .or_else(|| data.pointer("/event/initial"))
        {
            event.insert("initial".into(), initial.clone());
        }
        if let Some(reason) = data
            .get("reason")
            .or_else(|| data.pointer("/event/reason"))
            .or_else(|| data.pointer("/change/reason"))
            .or_else(|| data.pointer("/probe/reason"))
        {
            event.insert("reason".into(), reason.clone());
        }
        let mut envelope = Map::new();
        envelope.insert("schema_version".into(), Value::from(1));
        envelope.insert("event".into(), Value::Object(event));
        envelope.insert(
            "subject".into(),
            json!({ "id": subject, "sequence": sequence }),
        );
        if let Value::Object(fields) = data {
            for (key, value) in fields {
                if !matches!(key.as_str(), "event" | "schema_version" | "subject") {
                    envelope.insert(key, value);
                }
            }
        }
        let envelope = Value::Object(envelope);
        if bounded_json_bytes(&envelope).is_none() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let Ok(mut entries) = self.entries.try_lock() else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if self.max_events == 0 {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if entries.len() == self.max_events {
            entries.pop_front();
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        entries.push_back(envelope);
    }
}

#[cfg(test)]
pub(crate) struct WebhookTestCaptureHandle {
    capture: Arc<TestCapture>,
}

#[cfg(test)]
impl WebhookTestCaptureHandle {
    pub(crate) fn snapshot(&self) -> Vec<Value> {
        self.capture
            .entries
            .lock()
            .map(|entries| entries.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn dropped(&self) -> u64 {
        self.capture.dropped.load(Ordering::Relaxed)
    }
}

pub(crate) struct IntervalSnapshotRegistration {
    shared: Weak<Shared>,
    id: Option<u64>,
}

impl IntervalSnapshotRegistration {
    fn empty() -> Self {
        Self {
            shared: Weak::new(),
            id: None,
        }
    }
}

impl Drop for IntervalSnapshotRegistration {
    fn drop(&mut self) {
        let (Some(shared), Some(id)) = (self.shared.upgrade(), self.id.take()) else {
            return;
        };
        if let Ok(mut sources) = shared.interval_sources.try_lock() {
            sources.remove(&id);
        } else {
            // Retirement must not wait behind a snapshot. A stale weak source
            // is removed by the next interval sweep; its callback captures
            // only a Weak owner and therefore becomes inert immediately.
            if let Ok(mut pending) = shared.interval_remove_pending.try_lock() {
                pending.push(id);
            }
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, Default)]
pub(crate) struct WebhookRuntimeStats {
    pub enabled: bool,
    pub accepted_events: u64,
    pub source_drops: u64,
    pub oversized_events: u64,
    pub queue_drops: u64,
    pub expired_deliveries: u64,
    pub attempts: u64,
    pub delivered: u64,
    pub failed_deliveries: u64,
    pub cancelled_deliveries: u64,
    pub coalesced_intervals: u64,
    pub active_requests: usize,
    pub pending_deliveries: usize,
    pub pending_bytes: usize,
    pub interval_sources_rejected: u64,
    pub worker_failed: bool,
    pub rules: Vec<WebhookRuleStats>,
}

#[derive(Debug, Clone, serde::Serialize, Default)]
pub(crate) struct WebhookRuleStats {
    pub name: String,
    pub attempts: u64,
    pub delivered: u64,
    pub failed: u64,
    pub dropped: u64,
    pub expired: u64,
    pub cancelled: u64,
    pub coalesced: u64,
    pub last_result: Option<WebhookLastResult>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct WebhookLastResult {
    pub stage: String,
    pub status: Option<u16>,
    pub success: bool,
    pub attempt: u8,
    pub observed_at: String,
}

#[derive(Clone, Default)]
pub(crate) struct WebhookStatusHandle {
    stats: Option<Arc<StatsAtomic>>,
}

impl WebhookStatusHandle {
    pub(crate) fn snapshot(&self) -> WebhookRuntimeStats {
        self.stats
            .as_deref()
            .map_or_else(WebhookRuntimeStats::default, StatsAtomic::snapshot)
    }
}

struct StatsAtomic {
    enabled: bool,
    accepted_events: AtomicU64,
    source_drops: AtomicU64,
    oversized_events: AtomicU64,
    queue_drops: AtomicU64,
    expired_deliveries: AtomicU64,
    attempts: AtomicU64,
    delivered: AtomicU64,
    failed_deliveries: AtomicU64,
    cancelled_deliveries: AtomicU64,
    coalesced_intervals: AtomicU64,
    active_requests: AtomicUsize,
    pending_deliveries: AtomicUsize,
    pending_bytes: AtomicUsize,
    interval_sources_rejected: AtomicU64,
    worker_failed: AtomicBool,
    rules: Vec<RuleStatsAtomic>,
}

struct RuleStatsAtomic {
    name: String,
    attempts: AtomicU64,
    delivered: AtomicU64,
    failed: AtomicU64,
    dropped: AtomicU64,
    expired: AtomicU64,
    cancelled: AtomicU64,
    coalesced: AtomicU64,
    last_result: Mutex<Option<WebhookLastResult>>,
}

impl StatsAtomic {
    fn new(enabled: bool, rules: &[WebhookRule]) -> Self {
        Self {
            enabled,
            accepted_events: AtomicU64::new(0),
            source_drops: AtomicU64::new(0),
            oversized_events: AtomicU64::new(0),
            queue_drops: AtomicU64::new(0),
            expired_deliveries: AtomicU64::new(0),
            attempts: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
            failed_deliveries: AtomicU64::new(0),
            cancelled_deliveries: AtomicU64::new(0),
            coalesced_intervals: AtomicU64::new(0),
            active_requests: AtomicUsize::new(0),
            pending_deliveries: AtomicUsize::new(0),
            pending_bytes: AtomicUsize::new(0),
            interval_sources_rejected: AtomicU64::new(0),
            worker_failed: AtomicBool::new(false),
            rules: rules
                .iter()
                .map(|rule| RuleStatsAtomic {
                    name: rule.name.clone(),
                    attempts: AtomicU64::new(0),
                    delivered: AtomicU64::new(0),
                    failed: AtomicU64::new(0),
                    dropped: AtomicU64::new(0),
                    expired: AtomicU64::new(0),
                    cancelled: AtomicU64::new(0),
                    coalesced: AtomicU64::new(0),
                    last_result: Mutex::new(None),
                })
                .collect(),
        }
    }
}

impl StatsAtomic {
    fn snapshot(&self) -> WebhookRuntimeStats {
        WebhookRuntimeStats {
            enabled: self.enabled,
            accepted_events: self.accepted_events.load(Ordering::Relaxed),
            source_drops: self.source_drops.load(Ordering::Relaxed),
            oversized_events: self.oversized_events.load(Ordering::Relaxed),
            queue_drops: self.queue_drops.load(Ordering::Relaxed),
            expired_deliveries: self.expired_deliveries.load(Ordering::Relaxed),
            attempts: self.attempts.load(Ordering::Relaxed),
            delivered: self.delivered.load(Ordering::Relaxed),
            failed_deliveries: self.failed_deliveries.load(Ordering::Relaxed),
            cancelled_deliveries: self.cancelled_deliveries.load(Ordering::Relaxed),
            coalesced_intervals: self.coalesced_intervals.load(Ordering::Relaxed),
            active_requests: self.active_requests.load(Ordering::Relaxed),
            pending_deliveries: self.pending_deliveries.load(Ordering::Relaxed),
            pending_bytes: self.pending_bytes.load(Ordering::Relaxed),
            interval_sources_rejected: self.interval_sources_rejected.load(Ordering::Relaxed),
            worker_failed: self.worker_failed.load(Ordering::Relaxed),
            rules: self
                .rules
                .iter()
                .map(|rule| WebhookRuleStats {
                    name: rule.name.clone(),
                    attempts: rule.attempts.load(Ordering::Relaxed),
                    delivered: rule.delivered.load(Ordering::Relaxed),
                    failed: rule.failed.load(Ordering::Relaxed),
                    dropped: rule.dropped.load(Ordering::Relaxed),
                    expired: rule.expired.load(Ordering::Relaxed),
                    cancelled: rule.cancelled.load(Ordering::Relaxed),
                    coalesced: rule.coalesced.load(Ordering::Relaxed),
                    last_result: rule
                        .last_result
                        .lock()
                        .ok()
                        .and_then(|result| result.clone()),
                })
                .collect(),
        }
    }
}

/// Unique generation owner. Status handles and publishers can outlive it, but
/// Drop closes intake and aborts the observer worker so they cannot keep a
/// generation's dispatcher alive.
pub(crate) struct WebhookRuntime {
    shared: Option<Arc<Shared>>,
    publisher: EventPublisher,
    worker: Option<JoinHandle<()>>,
    shutdown: Option<watch::Sender<Option<tokio::time::Instant>>>,
    stats: Option<Arc<StatsAtomic>>,
}

impl WebhookRuntime {
    pub(crate) fn start(
        config: WebhookConfig,
        registry: RuntimeOutboundRegistry,
        generation: RuntimeGenerationControl,
    ) -> Result<Self, crate::runtime::RuntimeError> {
        config
            .validate()
            .map_err(|error| crate::runtime::RuntimeError::ProductPolicy(error.to_string()))?;
        if config.rules.is_empty() {
            return Ok(Self {
                shared: None,
                publisher: EventPublisher::default(),
                worker: None,
                shutdown: None,
                stats: None,
            });
        }
        let runtime_handle = tokio::runtime::Handle::try_current().map_err(|_| {
            crate::runtime::RuntimeError::Protocol("webhook runtime requires Tokio")
        })?;

        let boot_id = process_boot_id()?;
        let interested = interest_mask(&config);
        let stats = Arc::new(StatsAtomic::new(true, &config.rules));
        let prepared_tls = config
            .rules
            .iter()
            .map(|rule| {
                http::origin_tls_config(&rule.target).map_err(|_| {
                    crate::runtime::RuntimeError::ProductPolicy(
                        "invalid webhook origin TLS roots".to_string(),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (shutdown, shutdown_rx) = watch::channel(None);
        let shared = Arc::new(Shared::new(
            config,
            boot_id,
            registry.dns().generation(),
            interested,
            stats.clone(),
            prepared_tls,
        ));
        let publisher = EventPublisher {
            shared: Some(shared.clone()),
            #[cfg(test)]
            test_capture: None,
        };
        registry.attach_webhook_publisher(publisher.clone());
        let worker_shared = shared.clone();
        let worker = runtime_handle.spawn(async move {
            let result = run_worker(worker_shared.clone(), registry, generation, shutdown_rx).await;
            if result.is_err() {
                worker_shared
                    .stats
                    .worker_failed
                    .store(true, Ordering::Relaxed);
            }
        });
        Ok(Self {
            shared: Some(shared),
            publisher,
            worker: Some(worker),
            shutdown: Some(shutdown),
            stats: Some(stats),
        })
    }

    pub(crate) fn publisher(&self) -> EventPublisher {
        self.publisher.clone()
    }

    pub(crate) fn stats_handle(&self) -> WebhookStatusHandle {
        WebhookStatusHandle {
            stats: self.stats.clone(),
        }
    }

    /// Quiesces intake immediately, then gives queued work only the caller's
    /// absolute stop deadline. Callers compute that deadline from the original
    /// generation stop instant, before any retirement waits.
    pub(crate) async fn shutdown(&mut self, deadline: tokio::time::Instant) {
        if let Some(shared) = &self.shared {
            shared.closed.store(true, Ordering::Release);
            shared.changed.notify_one();
        }
        if let Some(shutdown) = &self.shutdown {
            shutdown.send_replace(Some(deadline));
        }
        let Some(mut worker) = self.worker.take() else {
            return;
        };
        if tokio::time::timeout_at(deadline, &mut worker)
            .await
            .is_err()
        {
            worker.abort();
            let _ = worker.await;
        }
    }
}

impl Drop for WebhookRuntime {
    fn drop(&mut self) {
        if let Some(shared) = &self.shared {
            shared.closed.store(true, Ordering::Release);
            shared.changed.notify_one();
        }
        if let Some(shutdown) = &self.shutdown {
            shutdown.send_replace(Some(tokio::time::Instant::now()));
        }
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
        if let Some(shared) = &self.shared {
            discard_queued(shared);
            shared.stats.active_requests.store(0, Ordering::Relaxed);
        }
    }
}

struct Shared {
    config: Arc<WebhookConfig>,
    registry_generation: u64,
    boot_id: u64,
    interested: [AtomicU64; 1],
    changed: Notify,
    closed: AtomicBool,
    activated: AtomicBool,
    state: Mutex<QueueState>,
    interval_sources: Mutex<HashMap<u64, IntervalSource>>,
    interval_remove_pending: Mutex<Vec<u64>>,
    next_interval_source: AtomicU64,
    stats: Arc<StatsAtomic>,
    prepared_tls: Vec<Option<Arc<rustls::ClientConfig>>>,
}

struct QueueState {
    sources: VecDeque<SourceEvent>,
    jobs: Vec<VecDeque<Delivery>>,
    used_count: usize,
    used_bytes: usize,
    fairness_cursor: usize,
}

struct SourceEvent {
    kind: EventKind,
    envelope: Arc<Value>,
    bytes: usize,
    received_at: tokio::time::Instant,
    only_rule: Option<usize>,
    coalesce_key: Option<(usize, Arc<str>)>,
}

#[derive(Clone)]
struct Delivery {
    rule: usize,
    envelope: Arc<Value>,
    event_id: String,
    delivery_id: String,
    deadline: tokio::time::Instant,
    attempt: u8,
    bytes: usize,
    coalesce_key: Option<Arc<str>>,
}

struct IntervalSource {
    subject: Arc<str>,
    provider: Arc<dyn Fn() -> Option<Value> + Send + Sync + 'static>,
}

/// Owns accounting for work that has left the shared queue but has not reached
/// a terminal result yet. Dropping the cancelled worker future runs this guard,
/// settling cancellation stats and all queue budgets even when
/// the normal shutdown branch cannot run before its deadline.
struct WorkerCleanup {
    shared: Arc<Shared>,
    retained: Vec<Option<Delivery>>,
}

impl WorkerCleanup {
    fn new(shared: Arc<Shared>, rules: usize) -> Self {
        Self {
            shared,
            retained: (0..rules).map(|_| None).collect(),
        }
    }

    fn track(&mut self, delivery: Delivery) {
        if let Some(retained) = self.retained.get_mut(delivery.rule) {
            *retained = Some(delivery);
        }
    }

    fn forget(&mut self, rule: usize) {
        if let Some(retained) = self.retained.get_mut(rule) {
            *retained = None;
        }
    }
}

impl Drop for WorkerCleanup {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::Release);
        for delivery in self.retained.iter_mut().filter_map(Option::take) {
            count_cancelled(&self.shared, delivery.rule);
        }

        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (index, jobs) in state.jobs.iter_mut().enumerate() {
            count_cancelled_many(&self.shared, index, jobs.len() as u64);
            jobs.clear();
        }
        state.sources.clear();
        state.used_count = 0;
        state.used_bytes = 0;
        update_queue_stats(&self.shared, &state);
        self.shared
            .stats
            .active_requests
            .store(0, Ordering::Relaxed);
    }
}

impl Shared {
    fn new(
        config: WebhookConfig,
        boot_id: u64,
        registry_generation: u64,
        interested: [u64; 1],
        stats: Arc<StatsAtomic>,
        prepared_tls: Vec<Option<Arc<rustls::ClientConfig>>>,
    ) -> Self {
        Self {
            state: Mutex::new(QueueState {
                sources: VecDeque::new(),
                jobs: (0..config.rules.len()).map(|_| VecDeque::new()).collect(),
                used_count: 0,
                used_bytes: 0,
                fairness_cursor: 0,
            }),
            config: Arc::new(config),
            registry_generation,
            boot_id,
            interested: [AtomicU64::new(interested[0])],
            changed: Notify::new(),
            closed: AtomicBool::new(false),
            activated: AtomicBool::new(false),
            interval_sources: Mutex::new(HashMap::new()),
            interval_remove_pending: Mutex::new(Vec::new()),
            next_interval_source: AtomicU64::new(1),
            stats,
            prepared_tls,
        }
    }
}

fn interest_mask(config: &WebhookConfig) -> [u64; 1] {
    let mut mask = [0u64; 1];
    for rule in &config.rules {
        for branch in &rule.when.branches {
            for kind in &branch.events {
                mask[*kind as usize / 64] |= 1u64 << (*kind as usize % 64);
            }
        }
    }
    mask
}

fn process_boot_id() -> Result<u64, crate::runtime::RuntimeError> {
    static BOOT_ID: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    if let Some(id) = BOOT_ID.get() {
        return Ok(*id);
    }
    let candidate = random_u64()?;
    let _ = BOOT_ID.set(candidate);
    BOOT_ID
        .get()
        .copied()
        .ok_or(crate::runtime::RuntimeError::Protocol(
            "webhook boot id unavailable",
        ))
}

fn make_event(shared: &Shared, kind: EventKind, subject: &str, data: Value) -> Option<SourceEvent> {
    let bounded = bounded_json_bytes(&data)?;
    if bounded.len().saturating_add(subject.len()) > MAX_EVENT_BYTES {
        return None;
    }
    let event_sequence = PROCESS_EVENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let event_id = format!(
        "{EVENT_ID_PREFIX}-{:016x}-{event_sequence:016x}",
        shared.boot_id
    );
    let now = utc_now();
    let subject_sequence = data
        .get("subject_sequence")
        .or_else(|| data.pointer("/path/subject_sequence"))
        .cloned()
        .unwrap_or(Value::Null);
    let initial = data
        .get("initial")
        .or_else(|| data.pointer("/event/initial"))
        .cloned();
    let reason = data
        .get("reason")
        .or_else(|| data.pointer("/event/reason"))
        .cloned()
        .or_else(|| data.pointer("/change/reason").cloned())
        .or_else(|| data.pointer("/probe/reason").cloned());
    let mut event = Map::new();
    event.insert("id".into(), Value::String(event_id.clone()));
    event.insert("type".into(), Value::String(kind.as_str().to_string()));
    event.insert("occurred_at".into(), Value::String(now.clone()));
    event.insert("observed_at".into(), Value::String(now));
    event.insert(
        "process_boot_id".into(),
        Value::String(format!("{:016x}", shared.boot_id)),
    );
    event.insert(
        "configuration_generation".into(),
        Value::from(shared.registry_generation),
    );
    event.insert("subject_id".into(), Value::String(subject.to_string()));
    event.insert("subject_sequence".into(), subject_sequence.clone());
    if let Some(reason) = reason {
        event.insert("reason".into(), reason);
    }
    if let Some(initial) = initial {
        event.insert("initial".into(), initial);
    }
    let mut envelope = Map::new();
    envelope.insert("schema_version".into(), Value::from(1));
    envelope.insert("event".into(), Value::Object(event));
    envelope.insert(
        "subject".into(),
        json!({ "id": subject, "sequence": subject_sequence }),
    );
    if let Value::Object(fields) = data {
        for (key, value) in fields {
            if !matches!(key.as_str(), "event" | "schema_version" | "subject") {
                envelope.insert(key, value);
            }
        }
    }
    let envelope = Value::Object(envelope);
    let encoded = bounded_json_bytes(&envelope)?;
    Some(SourceEvent {
        kind,
        bytes: encoded.len(),
        received_at: tokio::time::Instant::now(),
        envelope: Arc::new(envelope),
        only_rule: None,
        coalesce_key: None,
    })
}

fn utc_now() -> String {
    let timestamp = OffsetDateTime::now_utc();
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        timestamp.year(),
        u8::from(timestamp.month()),
        timestamp.day(),
        timestamp.hour(),
        timestamp.minute(),
        timestamp.second(),
        timestamp.millisecond()
    )
}

fn bounded_json_bytes(value: &Value) -> Option<Vec<u8>> {
    let mut nodes = 0usize;
    if !bounded_shape(value, 0, &mut nodes) {
        return None;
    }
    let mut output = CappedBytes::new(MAX_EVENT_BYTES);
    serde_json::to_writer(&mut output, value).ok()?;
    Some(output.bytes)
}

fn bounded_shape(value: &Value, depth: usize, nodes: &mut usize) -> bool {
    *nodes = nodes.saturating_add(1);
    if *nodes > MAX_EVENT_NODES || depth > MAX_EVENT_DEPTH {
        return false;
    }
    match value {
        Value::String(value) => value.len() <= MAX_EVENT_BYTES,
        Value::Array(values) => values
            .iter()
            .all(|value| bounded_shape(value, depth + 1, nodes)),
        Value::Object(values) => values.iter().all(|(key, value)| {
            key.len() <= MAX_EVENT_BYTES && bounded_shape(value, depth + 1, nodes)
        }),
        Value::Null | Value::Bool(_) | Value::Number(_) => true,
    }
}

struct CappedBytes {
    bytes: Vec<u8>,
    cap: usize,
}

impl CappedBytes {
    fn new(cap: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(cap.min(1024)),
            cap,
        }
    }
}

impl io::Write for CappedBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.bytes.len().saturating_add(bytes.len()) > self.cap {
            return Err(io::Error::other("bounded webhook JSON limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn enqueue_source(
    shared: &Shared,
    mut source: SourceEvent,
    priority: bool,
    only_rule: Option<usize>,
) {
    source.only_rule = only_rule;
    let state_result = if priority {
        shared.state.lock().map_err(|_| ())
    } else {
        shared.state.try_lock().map_err(|_| ())
    };
    let Ok(mut state) = state_result else {
        shared.stats.source_drops.fetch_add(1, Ordering::Relaxed);
        return;
    };
    // A producer may pass its first closed check and wait for this lock while
    // shutdown begins. Do not let it resurrect queue accounting after the
    // worker's terminal cleanup has run.
    if shared.closed.load(Ordering::Acquire) {
        shared.stats.source_drops.fetch_add(1, Ordering::Relaxed);
        return;
    }
    let (count, bytes) = queue_limits(&shared.config);
    // Replace an older interval sample before charging its successor. This
    // keeps coalescing effective even when the queue is at its configured cap.
    if let Some((rule, key)) = &source.coalesce_key {
        if let Some(position) = state.sources.iter().position(|queued| {
            queued
                .coalesce_key
                .as_ref()
                .is_some_and(|(queued_rule, queued_key)| queued_rule == rule && queued_key == key)
        }) && let Some(old) = state.sources.remove(position)
        {
            state.used_count = state.used_count.saturating_sub(1);
            state.used_bytes = state.used_bytes.saturating_sub(old.bytes);
            record_coalesced(shared, *rule);
        }
        if let Some(position) = state.jobs[*rule]
            .iter()
            .position(|queued| queued.coalesce_key.as_ref() == Some(key))
            && let Some(old) = state.jobs[*rule].remove(position)
        {
            state.used_count = state.used_count.saturating_sub(1);
            state.used_bytes = state.used_bytes.saturating_sub(old.bytes);
            record_coalesced(shared, *rule);
        }
    }
    while priority
        && (state.used_count >= count || state.used_bytes.saturating_add(source.bytes) > bytes)
    {
        let Some(dropped) = state.sources.pop_back() else {
            break;
        };
        state.used_count = state.used_count.saturating_sub(1);
        state.used_bytes = state.used_bytes.saturating_sub(dropped.bytes);
        shared.stats.source_drops.fetch_add(1, Ordering::Relaxed);
    }
    if state.used_count >= count || state.used_bytes.saturating_add(source.bytes) > bytes {
        shared.stats.source_drops.fetch_add(1, Ordering::Relaxed);
        update_queue_stats(shared, &state);
        return;
    }
    state.used_count += 1;
    state.used_bytes += source.bytes;
    if priority {
        state.sources.push_front(source);
    } else {
        state.sources.push_back(source);
    }
    shared.stats.accepted_events.fetch_add(1, Ordering::Relaxed);
    update_queue_stats(shared, &state);
    drop(state);
    shared.changed.notify_one();
}

fn record_coalesced(shared: &Shared, rule: usize) {
    shared
        .stats
        .coalesced_intervals
        .fetch_add(1, Ordering::Relaxed);
    if let Some(rule_stats) = shared.stats.rules.get(rule) {
        rule_stats.coalesced.fetch_add(1, Ordering::Relaxed);
    }
}

fn queue_limits(config: &WebhookConfig) -> (usize, usize) {
    (config.max_pending_deliveries, config.max_pending_bytes)
}

fn update_queue_stats(shared: &Shared, state: &QueueState) {
    shared
        .stats
        .pending_deliveries
        .store(state.used_count, Ordering::Relaxed);
    shared
        .stats
        .pending_bytes
        .store(state.used_bytes, Ordering::Relaxed);
}

async fn run_worker(
    shared: Arc<Shared>,
    registry: RuntimeOutboundRegistry,
    generation: RuntimeGenerationControl,
    mut shutdown: watch::Receiver<Option<tokio::time::Instant>>,
) -> Result<(), ()> {
    // This guard owns the accounting responsibility for every delivery moved
    // out of the queue and retained by this worker, including while a child
    // HTTP task is running or a retry waits. It runs synchronously when the
    // worker future is aborted at its hard shutdown deadline.
    let mut cleanup = WorkerCleanup::new(shared.clone(), shared.config.rules.len());
    if generation.wait_until_activated().await.is_err() {
        shared.closed.store(true, Ordering::Release);
        discard_queued(&shared);
        return Ok(());
    }
    if shared.interested[EventKind::NodeStateChanged as usize / 64].load(Ordering::Relaxed)
        & (1u64 << (EventKind::NodeStateChanged as usize % 64))
        != 0
        && let Some(source) = make_event(
            &shared,
            EventKind::NodeStateChanged,
            "node",
            json!({
                "node": { "state": "ready" },
                "change": { "from": "starting", "to": "ready", "reason": "activated" },
                "initial": true,
                "subject_sequence": 1
            }),
        )
    {
        enqueue_source(&shared, source, true, None);
    }
    // Activation is independent of whether this optional event fit within a
    // deliberately small queue; ready must never wedge generation startup.
    shared.activated.store(true, Ordering::Release);

    let interval_rules = shared
        .config
        .rules
        .iter()
        .enumerate()
        .filter_map(|(index, rule)| rule.interval.map(|interval| (index, interval)))
        .collect::<Vec<_>>();
    let mut next_intervals = interval_rules
        .iter()
        .map(|(rule, interval)| (*rule, tokio::time::Instant::now() + *interval, *interval))
        .collect::<Vec<_>>();
    let rule_count = shared.config.rules.len();
    let mut active = vec![false; rule_count];
    let mut retry_wait: Vec<Option<(Delivery, tokio::time::Instant)>> =
        (0..rule_count).map(|_| None).collect();
    let mut attempts = JoinSet::new();
    let mut in_flight = 0usize;
    let max_in_flight = shared
        .config
        .max_in_flight
        .min(shared.config.max_pending_deliveries)
        .max(1);
    let mut stop_deadline = *shutdown.borrow();

    loop {
        process_interval_removals(&shared);
        sweep_source_events(&shared);
        fanout_ready_sources(&shared);
        expire_waiting_retries(&shared, &mut active, &mut retry_wait, &mut cleanup);
        launch_ready_attempts(
            &shared,
            &registry,
            &mut active,
            &mut retry_wait,
            &mut attempts,
            &mut in_flight,
            &mut cleanup,
        );

        let no_jobs = queue_is_empty(&shared)
            && in_flight == 0
            && cleanup.retained.iter().all(Option::is_none)
            && retry_wait.iter().all(Option::is_none);
        if stop_deadline.is_some() && no_jobs {
            break;
        }
        if stop_deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
            attempts.abort_all();
            while attempts.join_next().await.is_some() {}
            break;
        }

        let next_tick = next_intervals.iter().map(|(_, due, _)| *due).min();
        let source_backlog = queue_has_source(&shared);
        let retry_deadline = next_retry_deadline(&retry_wait, in_flight < max_in_flight);
        tokio::select! {
            biased;
            changed = shutdown.changed(), if stop_deadline.is_none() => {
                if changed.is_err() { stop_deadline = Some(tokio::time::Instant::now()); }
                else { stop_deadline = *shutdown.borrow_and_update(); }
            }
            _ = wait_until(stop_deadline), if stop_deadline.is_some() => {
                attempts.abort_all();
                while attempts.join_next().await.is_some() {}
                break;
            }
            _ = wait_until(next_tick), if !next_intervals.is_empty() && stop_deadline.is_none() => {
                publish_due_intervals(&shared, &mut next_intervals);
            }
            joined = attempts.join_next(), if !attempts.is_empty() => {
                in_flight = in_flight.saturating_sub(1);
                if let Some(Ok(result)) = joined {
                    let rule = result.rule;
                    complete_attempt(&shared, result, &mut active, &mut retry_wait);
                    if retry_wait[rule].is_none() {
                        cleanup.forget(rule);
                    }
                } else if joined.is_some() {
                    shared.stats.worker_failed.store(true, Ordering::Relaxed);
                    shared.closed.store(true, Ordering::Release);
                    attempts.abort_all();
                    while attempts.join_next().await.is_some() {}
                    break;
                }
                shared.stats.active_requests.store(in_flight, Ordering::Relaxed);
            }
            _ = wait_until(retry_deadline) => {}
            _ = tokio::task::yield_now(), if source_backlog => {}
            _ = shared.changed.notified(), if stop_deadline.is_none() => {}
        }
    }
    Ok(())
}

async fn wait_until(deadline: Option<tokio::time::Instant>) {
    if let Some(deadline) = deadline {
        tokio::time::sleep_until(deadline).await;
    } else {
        std::future::pending::<()>().await;
    }
}

fn next_retry_deadline(
    retry_wait: &[Option<(Delivery, tokio::time::Instant)>],
    can_launch: bool,
) -> Option<tokio::time::Instant> {
    let now = tokio::time::Instant::now();
    retry_wait
        .iter()
        .filter_map(|retry| {
            retry.as_ref().and_then(|(delivery, due)| {
                let wake = if can_launch { *due } else { delivery.deadline };
                (wake > now).then_some(wake)
            })
        })
        .min()
}

fn process_interval_removals(shared: &Shared) {
    let Ok(mut pending) = shared.interval_remove_pending.try_lock() else {
        return;
    };
    if pending.is_empty() {
        return;
    }
    let ids = std::mem::take(&mut *pending);
    drop(pending);
    if let Ok(mut sources) = shared.interval_sources.try_lock() {
        for id in ids {
            sources.remove(&id);
        }
    } else if let Ok(mut pending) = shared.interval_remove_pending.try_lock() {
        pending.extend(ids);
    }
}

fn publish_due_intervals(
    shared: &Shared,
    next_intervals: &mut [(usize, tokio::time::Instant, Duration)],
) {
    let now = tokio::time::Instant::now();
    let due_rules = next_intervals
        .iter_mut()
        .filter_map(|(rule, due, interval)| {
            if *due > now {
                return None;
            }
            // Missed ticks are skipped; never burst to catch up.
            *due = now + *interval;
            Some((*rule, *interval))
        })
        .collect::<Vec<_>>();
    if due_rules.is_empty() {
        return;
    }
    let sources = match shared.interval_sources.try_lock() {
        Ok(sources) => sources
            .iter()
            .map(|(id, source)| (*id, source.subject.clone(), source.provider.clone()))
            .collect::<Vec<_>>(),
        Err(_) => return,
    };
    for (rule_index, interval) in due_rules {
        for (_, subject, provider) in &sources {
            let Some(snapshot) = provider() else {
                continue;
            };
            let mut path = match snapshot {
                Value::Object(fields) => fields,
                snapshot => Map::from_iter([("snapshot".to_owned(), snapshot)]),
            };
            path.insert("interval_s".to_owned(), json!(interval.as_secs_f64()));
            let data = json!({ "path": Value::Object(path) });
            let Some(mut event) = make_event(shared, EventKind::PathInterval, subject, data) else {
                shared
                    .stats
                    .oversized_events
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            };
            event.coalesce_key = Some((rule_index, subject.clone()));
            enqueue_source(shared, event, false, Some(rule_index));
        }
    }
}

fn sweep_source_events(shared: &Shared) {
    let source = {
        let Ok(mut state) = shared.state.lock() else {
            return;
        };
        let Some(source) = state.sources.pop_front() else {
            update_queue_stats(shared, &state);
            return;
        };
        // Keep the source charged while it is being matched and fanned out.
        // The first accepted delivery replaces this reservation atomically.
        update_queue_stats(shared, &state);
        source
    };
    let mut matching = Vec::new();
    if let Some(only_rule) = source.only_rule {
        if shared
            .config
            .rules
            .get(only_rule)
            .is_some_and(|rule| rule.matches(source.kind, &source.envelope))
        {
            matching.push(only_rule);
        }
    } else {
        for (index, rule) in shared.config.rules.iter().enumerate() {
            if rule.matches(source.kind, &source.envelope) {
                matching.push(index);
            }
        }
    }
    let Some(event_id) = source
        .envelope
        .pointer("/event/id")
        .and_then(Value::as_str)
        .map(str::to_owned)
    else {
        release_source(shared, &source);
        return;
    };
    // The in-progress source is a single bounded staging record per worker.
    // It retains only another Arc to the frozen envelope; the first admitted
    // delivery atomically replaces the source's queue reservation.
    let mut source_reserved = true;
    for rule_index in matching {
        let rule = &shared.config.rules[rule_index];
        let policy = &rule.delivery;
        let delivery_id = format!("{event_id}-r{rule_index}");
        let bytes = source.bytes.saturating_add(delivery_id.len());
        let delivery = Delivery {
            rule: rule_index,
            envelope: source.envelope.clone(),
            event_id: event_id.clone(),
            delivery_id,
            deadline: source.received_at + policy.max_age,
            attempt: 0,
            bytes,
            coalesce_key: source
                .coalesce_key
                .as_ref()
                .map(|(_, subject)| subject.clone()),
        };
        if let Ok(mut state) = shared.state.lock() {
            if let Some(key) = &delivery.coalesce_key
                && let Some(position) = state.jobs[rule_index]
                    .iter()
                    .position(|queued| queued.coalesce_key.as_ref() == Some(key))
                && let Some(old) = state.jobs[rule_index].remove(position)
            {
                state.used_count = state.used_count.saturating_sub(1);
                state.used_bytes = state.used_bytes.saturating_sub(old.bytes);
                record_coalesced(shared, rule_index);
            }
            let (max_count, max_bytes) = queue_limits(&shared.config);
            let base_count = state
                .used_count
                .saturating_sub(usize::from(source_reserved));
            let base_bytes =
                state
                    .used_bytes
                    .saturating_sub(if source_reserved { source.bytes } else { 0 });
            if base_count < max_count && base_bytes.saturating_add(delivery.bytes) <= max_bytes {
                if source_reserved {
                    state.used_count = base_count + 1;
                    state.used_bytes = base_bytes.saturating_add(delivery.bytes);
                    source_reserved = false;
                } else {
                    state.used_count += 1;
                    state.used_bytes += delivery.bytes;
                }
                state.jobs[rule_index].push_back(delivery);
                update_queue_stats(shared, &state);
                shared.changed.notify_one();
            } else {
                shared.stats.queue_drops.fetch_add(1, Ordering::Relaxed);
                if let Some(rule_stats) = shared.stats.rules.get(rule_index) {
                    rule_stats.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        } else {
            shared.stats.queue_drops.fetch_add(1, Ordering::Relaxed);
            if let Some(rule_stats) = shared.stats.rules.get(rule_index) {
                rule_stats.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    if source_reserved {
        release_source(shared, &source);
    }
}

fn release_source(shared: &Shared, source: &SourceEvent) {
    if let Ok(mut state) = shared.state.lock() {
        state.used_count = state.used_count.saturating_sub(1);
        state.used_bytes = state.used_bytes.saturating_sub(source.bytes);
        update_queue_stats(shared, &state);
    }
}

fn fanout_ready_sources(shared: &Shared) {
    // Source records already occupy the shared count/byte budget. Processing a
    // bounded number per loop keeps worker fairness with attempts and timers.
    for _ in 0..8 {
        if queue_has_source(shared) {
            sweep_source_events(shared);
        } else {
            break;
        }
    }
}

fn queue_has_source(shared: &Shared) -> bool {
    shared
        .state
        .lock()
        .is_ok_and(|state| !state.sources.is_empty())
}

fn queue_is_empty(shared: &Shared) -> bool {
    shared
        .state
        .lock()
        .is_ok_and(|state| state.sources.is_empty() && state.jobs.iter().all(VecDeque::is_empty))
}

fn discard_queued(shared: &Shared) {
    let mut state = shared
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.sources.clear();
    for (index, jobs) in state.jobs.iter_mut().enumerate() {
        count_cancelled_many(shared, index, jobs.len() as u64);
        jobs.clear();
    }
    state.used_count = 0;
    state.used_bytes = 0;
    update_queue_stats(shared, &state);
}

fn record_last_result(shared: &Shared, rule: usize, delivery: &Delivery, outcome: &AttemptOutcome) {
    let Some(rule_stats) = shared.stats.rules.get(rule) else {
        return;
    };
    if let Ok(mut last_result) = rule_stats.last_result.lock() {
        *last_result = Some(WebhookLastResult {
            stage: outcome.stage.to_owned(),
            status: outcome.status,
            success: outcome.success,
            attempt: delivery.attempt,
            observed_at: utc_now(),
        });
    }
}

fn record_expired(shared: &Shared, delivery: &Delivery) {
    release_delivery(shared, delivery);
    shared
        .stats
        .expired_deliveries
        .fetch_add(1, Ordering::Relaxed);
    if let Some(rule_stats) = shared.stats.rules.get(delivery.rule) {
        rule_stats.expired.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut last_result) = rule_stats.last_result.lock() {
            *last_result = Some(WebhookLastResult {
                stage: "expired".to_owned(),
                status: None,
                success: false,
                attempt: delivery.attempt,
                observed_at: utc_now(),
            });
        }
    }
}

fn expire_waiting_retries(
    shared: &Shared,
    active: &mut [bool],
    retry_wait: &mut [Option<(Delivery, tokio::time::Instant)>],
    cleanup: &mut WorkerCleanup,
) {
    let now = tokio::time::Instant::now();
    for (rule, retry) in retry_wait.iter_mut().enumerate() {
        if retry
            .as_ref()
            .is_some_and(|(delivery, due)| delivery.deadline <= now || *due >= delivery.deadline)
        {
            if let Some((delivery, _)) = retry.take() {
                record_expired(shared, &delivery);
            }
            cleanup.forget(rule);
            active[rule] = false;
        }
    }
}

fn launch_ready_attempts(
    shared: &Shared,
    registry: &RuntimeOutboundRegistry,
    active: &mut [bool],
    retry_wait: &mut [Option<(Delivery, tokio::time::Instant)>],
    attempts: &mut JoinSet<AttemptCompletion>,
    in_flight: &mut usize,
    cleanup: &mut WorkerCleanup,
) {
    let (limit, _) = queue_limits(&shared.config);
    let max_in_flight = shared.config.max_in_flight.min(limit).max(1);
    let rules = active.len();
    while *in_flight < max_in_flight && rules != 0 {
        let Ok(mut state) = shared.state.lock() else {
            return;
        };
        let mut selected = None;
        for offset in 0..rules {
            let rule = (state.fairness_cursor + offset) % rules;
            if active[rule] {
                if retry_wait[rule]
                    .as_ref()
                    .is_some_and(|(_, due)| *due <= tokio::time::Instant::now())
                {
                    selected = Some(rule);
                    break;
                }
                continue;
            }
            if !state.jobs[rule].is_empty() {
                selected = Some(rule);
                break;
            }
        }
        let Some(rule) = selected else {
            return;
        };
        state.fairness_cursor = (rule + 1) % rules;
        let delivery = if let Some((delivery, _)) = retry_wait[rule].take() {
            delivery
        } else if let Some(delivery) = state.jobs[rule].pop_front() {
            delivery
        } else {
            return;
        };
        if delivery.deadline <= tokio::time::Instant::now() {
            drop(state);
            record_expired(shared, &delivery);
            cleanup.forget(rule);
            active[rule] = false;
            continue;
        }
        active[rule] = true;
        drop(state);
        let registry = registry.clone();
        let rule_config = shared.config.rules[rule].clone();
        let envelope = delivery.envelope.clone();
        let tls = shared.prepared_tls[rule].clone();
        cleanup.track(delivery.clone());
        let shared_worker = Arc::new(AttemptShared {
            stats: shared.stats.clone(),
            registry: registry.clone(),
        });
        attempts.spawn(async move {
            run_attempt(shared_worker, rule, rule_config, delivery, envelope, tls).await
        });
        *in_flight += 1;
        shared
            .stats
            .active_requests
            .store(*in_flight, Ordering::Relaxed);
    }
}

struct AttemptShared {
    stats: Arc<StatsAtomic>,
    registry: RuntimeOutboundRegistry,
}

struct AttemptCompletion {
    rule: usize,
    delivery: Delivery,
    outcome: AttemptOutcome,
    expired: bool,
}

struct AttemptOutcome {
    status: Option<u16>,
    stage: &'static str,
    success: bool,
    retryable: bool,
    retry_after: Option<Duration>,
}

async fn run_attempt(
    shared: Arc<AttemptShared>,
    rule_index: usize,
    rule: WebhookRule,
    mut delivery: Delivery,
    envelope: Arc<Value>,
    tls: Option<Arc<rustls::ClientConfig>>,
) -> AttemptCompletion {
    if tokio::time::Instant::now() >= delivery.deadline {
        return AttemptCompletion {
            rule: rule_index,
            delivery,
            outcome: AttemptOutcome {
                status: None,
                stage: "expired",
                success: false,
                retryable: false,
                retry_after: None,
            },
            expired: true,
        };
    }
    delivery.attempt = delivery.attempt.saturating_add(1);
    shared.stats.attempts.fetch_add(1, Ordering::Relaxed);
    if let Some(rule_stats) = shared.stats.rules.get(rule_index) {
        rule_stats.attempts.fetch_add(1, Ordering::Relaxed);
    }
    let now = tokio::time::Instant::now();
    let attempt_deadline = (now + rule.delivery.timeout).min(delivery.deadline);
    let outcome = match tokio::time::timeout_at(
        attempt_deadline,
        http::deliver(
            &shared.registry,
            &rule.target,
            &delivery,
            &envelope,
            tls,
            attempt_deadline,
        ),
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(_) => AttemptOutcome {
            status: None,
            stage: "timeout",
            success: false,
            retryable: true,
            retry_after: None,
        },
    };
    let expired = tokio::time::Instant::now() >= delivery.deadline;
    AttemptCompletion {
        rule: rule_index,
        delivery,
        outcome,
        expired,
    }
}

fn complete_attempt(
    shared: &Shared,
    completion: AttemptCompletion,
    active: &mut [bool],
    retry_wait: &mut [Option<(Delivery, tokio::time::Instant)>],
) {
    let AttemptCompletion {
        rule,
        delivery,
        outcome,
        expired,
    } = completion;
    if expired {
        record_expired(shared, &delivery);
        active[rule] = false;
        return;
    }
    record_last_result(shared, rule, &delivery, &outcome);
    if outcome.success {
        shared.stats.delivered.fetch_add(1, Ordering::Relaxed);
        if let Some(rule_stats) = shared.stats.rules.get(rule) {
            rule_stats.delivered.fetch_add(1, Ordering::Relaxed);
        }
        active[rule] = false;
        release_delivery(shared, &delivery);
        return;
    }
    let policy = &shared.config.rules[rule].delivery;
    let can_retry = outcome.retryable
        && delivery.attempt < policy.max_attempts
        && tokio::time::Instant::now() < delivery.deadline;
    if can_retry {
        let backoff = retry_backoff(
            shared,
            &delivery,
            policy.initial_backoff,
            policy.max_backoff,
        );
        let wait = outcome
            .retry_after
            .map_or(backoff, |retry_after| backoff.max(retry_after));
        let now = tokio::time::Instant::now();
        if wait < delivery.deadline.saturating_duration_since(now) {
            let due = now + wait;
            retry_wait[rule] = Some((delivery, due));
            active[rule] = true;
            return;
        }
        record_expired(shared, &delivery);
        active[rule] = false;
        return;
    }
    shared
        .stats
        .failed_deliveries
        .fetch_add(1, Ordering::Relaxed);
    if let Some(rule_stats) = shared.stats.rules.get(rule) {
        rule_stats.failed.fetch_add(1, Ordering::Relaxed);
    }
    active[rule] = false;
    release_delivery(shared, &delivery);
}

fn retry_backoff(
    shared: &Shared,
    delivery: &Delivery,
    initial: Duration,
    max: Duration,
) -> Duration {
    let exponent = delivery.attempt.saturating_sub(1).min(16) as u32;
    let base = initial
        .checked_mul(1u32.checked_shl(exponent).unwrap_or(u32::MAX))
        .unwrap_or(max)
        .min(max);
    let seed = PROCESS_EVENT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ^ shared.boot_id
        ^ u64::from(delivery.attempt);
    let mut random = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    random = (random ^ (random >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    random = (random ^ (random >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    random ^= random >> 31;
    let factor = 0.5 + (random as f64 / u64::MAX as f64);
    base.mul_f64(factor).min(max)
}

fn release_delivery(shared: &Shared, delivery: &Delivery) {
    if let Ok(mut state) = shared.state.lock() {
        state.used_count = state.used_count.saturating_sub(1);
        state.used_bytes = state.used_bytes.saturating_sub(delivery.bytes);
        update_queue_stats(shared, &state);
    }
}

fn count_cancelled(shared: &Shared, rule: usize) {
    count_cancelled_many(shared, rule, 1);
}

fn count_cancelled_many(shared: &Shared, rule: usize, count: u64) {
    if count == 0 {
        return;
    }
    shared
        .stats
        .cancelled_deliveries
        .fetch_add(count, Ordering::Relaxed);
    if let Some(rule_stats) = shared.stats.rules.get(rule) {
        rule_stats.cancelled.fetch_add(count, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EgressRef;
    use crate::outbound::OutboundConfig;
    use crate::product::{OutboundId, TargetResolutionMode};
    use crate::webhook::{
        DeliveryPolicy, EventMatcher, EventMatcherBranch, WebhookBody, WebhookRule, WebhookTarget,
        WebhookUrl,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn configured_shared(policy: DeliveryPolicy, max_pending_deliveries: usize) -> Arc<Shared> {
        let rule = WebhookRule {
            name: "unit".to_owned(),
            when: EventMatcher {
                branches: vec![EventMatcherBranch {
                    events: vec![EventKind::PathStateChanged],
                    ..EventMatcherBranch::default()
                }],
                ..EventMatcher::default()
            },
            interval: None,
            target: WebhookTarget {
                url: WebhookUrl::parse("http://127.0.0.1/hook", Vec::new()).unwrap(),
                method: ::http::Method::POST,
                egress: EgressRef::Outbound(OutboundId::parse("test").unwrap()),
                dns_policy: None,
                target_resolution: TargetResolutionMode::FullResolve,
                headers: Vec::new(),
                body: WebhookBody::None,
                tls_roots: Vec::new(),
            },
            delivery: policy,
        };
        let config = WebhookConfig {
            max_in_flight: 1,
            max_pending_deliveries,
            max_pending_bytes: 64 * 1024,
            rules: vec![rule],
            ..WebhookConfig::default()
        };
        let stats = Arc::new(StatsAtomic::new(true, &config.rules));
        Arc::new(Shared::new(
            config,
            7,
            11,
            [1 << (EventKind::PathStateChanged as usize)],
            stats,
            vec![None],
        ))
    }

    fn enqueue_one_delivery(shared: &Shared) -> Delivery {
        let source = make_event(
            shared,
            EventKind::PathStateChanged,
            "path/test",
            json!({ "path": { "name": "test", "state": "up" }, "subject_sequence": 3 }),
        )
        .unwrap();
        enqueue_source(shared, source, false, None);
        sweep_source_events(shared);
        shared.state.lock().unwrap().jobs[0].pop_front().unwrap()
    }

    async fn read_request_body(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut request = Vec::with_capacity(2048);
        loop {
            let mut chunk = [0u8; 2048];
            let read = stream.read(&mut chunk).await.expect("read webhook request");
            assert_ne!(read, 0, "webhook request ended before the complete body");
            request.extend_from_slice(&chunk[..read]);
            let Some(headers_end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n")
            else {
                continue;
            };
            let headers = String::from_utf8_lossy(&request[..headers_end]);
            let content_length = headers
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .map(|(_, value)| value.trim().parse::<usize>().expect("content length"))
                .unwrap_or(0);
            if request.len() >= headers_end + 4 + content_length {
                return request;
            }
        }
    }

    #[tokio::test]
    async fn disabled_runtime_has_no_worker_queue_or_enabled_stats() {
        let registry = RuntimeOutboundRegistry::compile(
            std::iter::empty::<crate::runtime::outbound_registry::RuntimeOutboundLeaf>(),
            &[],
            crate::runtime::outbound_registry::test_dns_generation(),
        )
        .expect("empty outbound registry");
        let runtime = WebhookRuntime::start(
            WebhookConfig::default(),
            registry,
            RuntimeGenerationControl::new(),
        )
        .expect("disabled webhook runtime");
        assert!(runtime.worker.is_none());
        assert!(runtime.shared.is_none());
        assert!(!runtime.publisher().interested(EventKind::PathStateChanged));
        assert!(!runtime.stats_handle().snapshot().enabled);
    }

    #[test]
    fn queue_cap_is_shared_by_sources_and_fanout_deliveries() {
        let shared = configured_shared(DeliveryPolicy::default(), 1);
        for sequence in [1, 2] {
            let source = make_event(
                &shared,
                EventKind::PathStateChanged,
                "path/test",
                json!({ "path": { "name": "test" }, "subject_sequence": sequence }),
            )
            .unwrap();
            enqueue_source(&shared, source, false, None);
        }
        assert_eq!(shared.stats.pending_deliveries.load(Ordering::Relaxed), 1);
        assert_eq!(shared.stats.source_drops.load(Ordering::Relaxed), 1);
        sweep_source_events(&shared);
        assert_eq!(shared.stats.pending_deliveries.load(Ordering::Relaxed), 1);
        assert_eq!(shared.state.lock().unwrap().jobs[0].len(), 1);
    }

    #[test]
    fn bounded_source_batches_preserve_per_rule_fifo_until_drained() {
        let shared = configured_shared(DeliveryPolicy::default(), 32);
        for sequence in 1..=20 {
            let source = make_event(
                &shared,
                EventKind::PathStateChanged,
                "path/test",
                json!({
                    "path": { "name": "test" },
                    "subject_sequence": sequence
                }),
            )
            .unwrap();
            enqueue_source(&shared, source, false, None);
        }
        fanout_ready_sources(&shared);
        assert!(
            queue_has_source(&shared),
            "first batch is intentionally bounded"
        );
        for _ in 0..3 {
            if !queue_has_source(&shared) {
                break;
            }
            fanout_ready_sources(&shared);
        }
        assert!(
            !queue_has_source(&shared),
            "remaining source backlog is drained"
        );
        let state = shared.state.lock().unwrap();
        let sequences = state.jobs[0]
            .iter()
            .map(|delivery| {
                delivery
                    .envelope
                    .pointer("/event/subject_sequence")
                    .and_then(Value::as_u64)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(sequences, (1..=20).collect::<Vec<_>>());
    }

    #[test]
    fn interval_snapshot_places_fractional_period_and_subject_sequence_in_path() {
        let interval = Duration::from_millis(250);
        let mut shared = configured_shared(DeliveryPolicy::default(), 4);
        let shared_mut = Arc::get_mut(&mut shared).expect("unique shared runtime");
        let config = Arc::make_mut(&mut shared_mut.config);
        config.rules[0].interval = Some(interval);
        config.rules[0].when = EventMatcher {
            branches: vec![EventMatcherBranch {
                events: vec![EventKind::PathInterval],
                ..EventMatcherBranch::default()
            }],
            ..EventMatcher::default()
        };
        shared_mut.interval_sources.lock().unwrap().insert(
            1,
            IntervalSource {
                subject: Arc::from("path/test"),
                provider: Arc::new(|| {
                    Some(json!({
                        "name": "test",
                        "state": "up",
                        "subject_sequence": 17
                    }))
                }),
            },
        );

        let now = tokio::time::Instant::now();
        let mut due = [(0, now - Duration::from_millis(1), interval)];
        publish_due_intervals(&shared, &mut due);
        sweep_source_events(&shared);

        let state = shared.state.lock().unwrap();
        let delivery = state.jobs[0].front().expect("interval delivery");
        assert_eq!(delivery.envelope["path"]["interval_s"].as_f64(), Some(0.25));
        assert_eq!(delivery.envelope["path"]["subject_sequence"], 17);
        assert_eq!(delivery.envelope["event"]["subject_sequence"], 17);
        assert_eq!(delivery.envelope["subject"]["sequence"], 17);
    }

    #[tokio::test(start_paused = true)]
    async fn overdue_retry_has_no_ready_timer_while_global_concurrency_is_full() {
        let shared = configured_shared(DeliveryPolicy::default(), 4);
        let delivery = enqueue_one_delivery(&shared);
        let global_limit = 1;
        let in_flight = 1;
        tokio::time::advance(Duration::from_secs(1)).await;
        let retry_wait = vec![Some((delivery.clone(), tokio::time::Instant::now()))];
        let wake_at = next_retry_deadline(&retry_wait, in_flight < global_limit).unwrap();
        assert_eq!(wake_at, delivery.deadline);
        assert!(wake_at > tokio::time::Instant::now());

        let wake = tokio::spawn(wait_until(Some(wake_at)));
        for _ in 0..8 {
            tokio::task::yield_now().await;
            assert!(
                !wake.is_finished(),
                "an overdue retry must not spin its timer"
            );
        }
        tokio::time::advance(delivery.deadline - tokio::time::Instant::now()).await;
        wake.await.unwrap();
    }

    #[test]
    fn producer_data_cannot_replace_canonical_envelope_fields() {
        let shared = configured_shared(DeliveryPolicy::default(), 4);
        let source = make_event(
            &shared,
            EventKind::PathStateChanged,
            "path/expected",
            json!({
                "schema_version": 99,
                "event": {
                    "id": "forged-id",
                    "type": "forged.type",
                    "subject_id": "forged-subject",
                    "reason": "legacy-reason",
                    "initial": true
                },
                "subject": { "id": "forged-subject", "sequence": 99 },
                "path": { "subject_sequence": 17 }
            }),
        )
        .unwrap();
        assert_eq!(source.envelope["schema_version"], 1);
        assert_eq!(
            source.envelope["event"]["type"],
            EventKind::PathStateChanged.as_str()
        );
        assert_ne!(source.envelope["event"]["id"], "forged-id");
        assert_eq!(source.envelope["event"]["subject_id"], "path/expected");
        assert_eq!(source.envelope["event"]["subject_sequence"], 17);
        assert_eq!(source.envelope["event"]["initial"], true);
        assert_eq!(source.envelope["event"]["reason"], "legacy-reason");
        assert_eq!(source.envelope["subject"]["id"], "path/expected");
    }

    #[test]
    fn bounded_test_capture_preserves_the_same_canonical_namespaces() {
        let (publisher, capture) = EventPublisher::test_capture(&[EventKind::NodeStateChanged], 1);
        publisher.emit(
            EventKind::NodeStateChanged,
            "node",
            json!({
                "event": { "id": "forged", "type": "wrong", "initial": true },
                "schema_version": 77,
                "subject": { "id": "wrong" },
                "subject_sequence": 3
            }),
        );
        let entries = capture.snapshot();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["schema_version"], 1);
        assert_eq!(
            entries[0]["event"]["type"],
            EventKind::NodeStateChanged.as_str()
        );
        assert_ne!(entries[0]["event"]["id"], "forged");
        assert_eq!(entries[0]["event"]["initial"], true);
        assert_eq!(entries[0]["subject"]["id"], "node");
        assert_eq!(entries[0]["event"]["subject_sequence"], 3);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_keeps_event_and_delivery_ids_stable() {
        let policy = DeliveryPolicy {
            max_attempts: 2,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(1),
            ..DeliveryPolicy::default()
        };
        let shared = configured_shared(policy, 4);
        let mut delivery = enqueue_one_delivery(&shared);
        let original_event_id = delivery.event_id.clone();
        let original_delivery_id = delivery.delivery_id.clone();
        delivery.attempt = 1;
        let mut active = [true];
        let mut retry_wait = [None];
        complete_attempt(
            &shared,
            AttemptCompletion {
                rule: 0,
                delivery,
                outcome: AttemptOutcome {
                    status: Some(503),
                    stage: "http_status",
                    success: false,
                    retryable: true,
                    retry_after: None,
                },
                expired: false,
            },
            &mut active,
            &mut retry_wait,
        );
        let retry = retry_wait[0].as_ref().unwrap().0.clone();
        assert_eq!(retry.event_id, original_event_id);
        assert_eq!(retry.delivery_id, original_delivery_id);
        assert_eq!(retry.attempt, 1);
        assert!(active[0]);
        assert_eq!(shared.stats.failed_deliveries.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_expires_when_backoff_exceeds_delivery_age() {
        let policy = DeliveryPolicy {
            max_attempts: 2,
            max_age: Duration::from_secs(1),
            initial_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(2),
            ..DeliveryPolicy::default()
        };
        let shared = configured_shared(policy, 4);
        let mut delivery = enqueue_one_delivery(&shared);
        delivery.attempt = 1;
        let mut active = [true];
        let mut retry_wait = [None];
        complete_attempt(
            &shared,
            AttemptCompletion {
                rule: 0,
                delivery: delivery.clone(),
                outcome: AttemptOutcome {
                    status: Some(503),
                    stage: "http_status",
                    success: false,
                    retryable: true,
                    retry_after: None,
                },
                expired: false,
            },
            &mut active,
            &mut retry_wait,
        );
        assert!(retry_wait[0].is_none());
        assert_eq!(shared.stats.expired_deliveries.load(Ordering::Relaxed), 1);
        assert_eq!(shared.stats.failed_deliveries.load(Ordering::Relaxed), 0);
        assert_eq!(shared.stats.pending_deliveries.load(Ordering::Relaxed), 0);
        assert_eq!(shared.stats.rules[0].expired.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn huge_retry_after_expires_without_instant_overflow() {
        let policy = DeliveryPolicy {
            max_attempts: 2,
            ..DeliveryPolicy::default()
        };
        let shared = configured_shared(policy, 4);
        let mut delivery = enqueue_one_delivery(&shared);
        delivery.attempt = 1;
        let mut active = [true];
        let mut retry_wait = [None];
        complete_attempt(
            &shared,
            AttemptCompletion {
                rule: 0,
                delivery,
                outcome: AttemptOutcome {
                    status: Some(503),
                    stage: "http_status",
                    success: false,
                    retryable: true,
                    retry_after: Some(Duration::from_secs(u64::MAX)),
                },
                expired: false,
            },
            &mut active,
            &mut retry_wait,
        );
        assert!(retry_wait[0].is_none());
        assert_eq!(shared.stats.expired_deliveries.load(Ordering::Relaxed), 1);
        assert_eq!(shared.stats.pending_deliveries.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn retry_wait_releases_global_slot_for_the_next_rule() {
        let first_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind first callback");
        let first_address = first_listener.local_addr().unwrap();
        let first_callback = tokio::spawn(async move {
            let (mut stream, _) = first_listener
                .accept()
                .await
                .expect("accept first callback");
            let request = read_request_body(&mut stream).await;
            assert!(request.starts_with(b"POST /hook HTTP/1.1\r\n"));
            stream
                .write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\nRetry-After: 3\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .expect("write first callback response");
        });

        let second_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind second callback");
        let second_address = second_listener.local_addr().unwrap();
        let (second_seen_tx, second_seen_rx) = tokio::sync::oneshot::channel();
        let second_callback = tokio::spawn(async move {
            let (mut stream, _) = second_listener
                .accept()
                .await
                .expect("accept second callback");
            let request = read_request_body(&mut stream).await;
            assert!(request.starts_with(b"POST /hook HTTP/1.1\r\n"));
            second_seen_tx.send(()).expect("report second callback");
            stream
                .write_all(
                    b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .expect("write second callback response");
        });

        let outbound_id = OutboundId::parse("test-direct").unwrap();
        let registry = RuntimeOutboundRegistry::compile(
            [
                crate::runtime::outbound_registry::RuntimeOutboundLeaf::Local {
                    id: outbound_id.clone(),
                    config: OutboundConfig::Direct,
                    connect_timeout: Duration::from_secs(1),
                    native_sockets: Arc::new(crate::transport::SystemNativeSocketConfigurator),
                },
            ],
            &[],
            crate::runtime::outbound_registry::test_dns_generation(),
        )
        .expect("direct webhook registry");
        let matcher = EventMatcher {
            branches: vec![EventMatcherBranch {
                events: vec![EventKind::PathStateChanged],
                ..EventMatcherBranch::default()
            }],
            ..EventMatcher::default()
        };
        let make_target = |address: std::net::SocketAddr| WebhookTarget {
            url: WebhookUrl::parse(&format!("http://{address}/hook"), Vec::new()).unwrap(),
            method: ::http::Method::POST,
            egress: EgressRef::Outbound(outbound_id.clone()),
            dns_policy: None,
            target_resolution: TargetResolutionMode::FullResolve,
            headers: Vec::new(),
            body: WebhookBody::StandardEvent,
            tls_roots: Vec::new(),
        };
        let retry_policy = DeliveryPolicy {
            max_attempts: 2,
            initial_backoff: Duration::from_secs(3),
            max_backoff: Duration::from_secs(3),
            ..DeliveryPolicy::default()
        };
        let config = WebhookConfig {
            max_in_flight: 1,
            max_pending_deliveries: 8,
            max_pending_bytes: 64 * 1024,
            rules: vec![
                WebhookRule {
                    name: "first".to_owned(),
                    when: matcher.clone(),
                    interval: None,
                    target: make_target(first_address),
                    delivery: retry_policy,
                },
                WebhookRule {
                    name: "second".to_owned(),
                    when: matcher,
                    interval: None,
                    target: make_target(second_address),
                    delivery: DeliveryPolicy::default(),
                },
            ],
            ..WebhookConfig::default()
        };
        let generation = RuntimeGenerationControl::new();
        generation.mark_ready();
        let mut runtime =
            WebhookRuntime::start(config, registry, generation).expect("start webhook runtime");
        let publisher = runtime.publisher();
        publisher.emit(
            EventKind::PathStateChanged,
            "path/test",
            json!({
                "path": { "name": "test", "state": "up" },
                "change": { "from": "down", "to": "up" }
            }),
        );

        tokio::time::timeout(Duration::from_secs(2), second_seen_rx)
            .await
            .expect("next rule must run during the first rule's retry wait")
            .expect("second callback signal");
        tokio::time::timeout(Duration::from_secs(2), async {
            while runtime.stats_handle().snapshot().delivered == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second callback delivery stats");
        first_callback.await.expect("first callback task");
        second_callback.await.expect("second callback task");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let stats = runtime.stats_handle().snapshot();
                if stats.rules[0]
                    .last_result
                    .as_ref()
                    .is_some_and(|result| result.status == Some(503))
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first callback must enter its retry wait before shutdown");

        runtime.shutdown(tokio::time::Instant::now()).await;
        let stats = runtime.stats_handle().snapshot();
        assert_eq!(
            stats.attempts, 2,
            "the delayed retry was cancelled at shutdown"
        );
        assert_eq!(stats.delivered, 1);
        assert_eq!(stats.cancelled_deliveries, 1);
        assert_eq!(stats.pending_deliveries, 0);
        assert_eq!(stats.pending_bytes, 0);
        assert_eq!(stats.active_requests, 0);
    }

    #[tokio::test]
    async fn dropping_runtime_eventually_settles_retained_cancellation() {
        let shared = configured_shared(DeliveryPolicy::default(), 4);
        let delivery = enqueue_one_delivery(&shared);
        let worker_shared = shared.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let worker = tokio::spawn(async move {
            let mut cleanup = WorkerCleanup::new(worker_shared, 1);
            cleanup.track(delivery);
            started_tx.send(()).expect("report retained worker state");
            std::future::pending::<()>().await;
        });
        started_rx.await.expect("worker retained delivery");

        let status = WebhookStatusHandle {
            stats: Some(shared.stats.clone()),
        };
        let (shutdown, _shutdown_rx) = watch::channel(None);
        let runtime = WebhookRuntime {
            shared: Some(shared.clone()),
            publisher: EventPublisher::default(),
            worker: Some(worker),
            shutdown: Some(shutdown),
            stats: Some(shared.stats.clone()),
        };
        drop(runtime);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let stats = status.snapshot();
                if stats.cancelled_deliveries == 1
                    && stats.pending_deliveries == 0
                    && stats.pending_bytes == 0
                    && stats.active_requests == 0
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("status handle settles after the cancelled worker future is dropped");
    }
}
