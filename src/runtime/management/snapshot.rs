//! Cached management snapshots and telemetry history.
//!
//! One sampler aggregates product telemetry and publishes an immutable cache.
//! Browser refreshes therefore cannot add contention to carrier or flow hot
//! paths, and product totals remain separate from per-path evidence.

use super::ManagementTarget;
use super::projection::{NumericSummary, collect_snapshot, flow_status};
use super::schema::{
    ManagementControls, ManagementDiagnostics, ManagementFlowLifecycle, ManagementFlowStatus,
    ManagementIo, ManagementRates, ManagementServices, ManagementSessionStatus, ManagementSnapshot,
    ManagementTraffic, ManagementTrafficKind, ManagementTrendSample, NumericIo, NumericLifecycle,
    SCHEMA,
};
use crate::runtime::telemetry::RuntimeTelemetrySnapshot;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const TREND_CAPACITY: usize = 300;

#[derive(Debug, Clone)]
pub(super) struct ManagementState {
    role: &'static str,
    started: Instant,
    started_unix_ms: u64,
    cache: Arc<RwLock<Arc<ManagementSnapshot>>>,
    history: Arc<Mutex<TrendHistory>>,
    refresh: Arc<Mutex<()>>,
}

impl ManagementState {
    pub(super) fn new(role: &'static str) -> Self {
        let started = Instant::now();
        let started_unix_ms = unix_millis();
        let initial = ManagementSnapshot::empty(role, started_unix_ms);
        Self {
            role,
            started,
            started_unix_ms,
            cache: Arc::new(RwLock::new(Arc::new(initial))),
            history: Arc::new(Mutex::new(TrendHistory::default())),
            refresh: Arc::new(Mutex::new(())),
        }
    }

    pub(super) fn refresh(&self, target: &ManagementTarget, record_history: bool) {
        let _refresh = self
            .refresh
            .lock()
            .expect("management refresh lock poisoned");
        let collected_at = Instant::now();
        let mut snapshot = collect_snapshot(
            target,
            self.role,
            self.started_unix_ms,
            self.started.elapsed(),
            collected_at,
        );
        let mut history = self
            .history
            .lock()
            .expect("management trend history poisoned");
        let (rates, trends) = if record_history {
            history.record(
                collected_at,
                snapshot.generated_unix_ms,
                snapshot.traffic.total.numeric(),
                snapshot.summary.active_flows,
            )
        } else {
            history.current()
        };
        snapshot.traffic.rates = rates;
        snapshot.traffic.trends = trends;
        *self.cache.write().expect("management cache poisoned") = Arc::new(snapshot);
    }

    pub(super) fn snapshot(&self) -> Arc<ManagementSnapshot> {
        self.cache
            .read()
            .expect("management cache poisoned")
            .clone()
    }
}

impl ManagementSnapshot {
    fn empty(role: &'static str, started_unix_ms: u64) -> Self {
        Self {
            schema: SCHEMA,
            generated_unix_ms: started_unix_ms,
            role,
            started_unix_ms,
            uptime_ms: 0,
            services: ManagementServices::default(),
            local_inbounds: Vec::new(),
            summary: NumericSummary::default().finish(),
            traffic: TelemetryAggregate::default().traffic(),
            paths: Vec::new(),
            sessions: Vec::new(),
            flows: Vec::new(),
            diagnostics: ManagementDiagnostics::default(),
            controls: ManagementControls::default(),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct TelemetryAggregate {
    total: NumericIo,
    reliable_io: NumericIo,
    datagram_io: NumericIo,
    pub(super) reliable_flows: NumericLifecycle,
    pub(super) datagram_flows: NumericLifecycle,
    pub(super) active_flow_capacity: usize,
    pub(super) active_flow_overflow: u64,
    pub(super) active_flow_overflow_total: u64,
    pub(super) flows: Vec<ManagementFlowStatus>,
}

impl TelemetryAggregate {
    pub(super) fn add(
        &mut self,
        service: &'static str,
        service_index: usize,
        service_tag: Option<String>,
        snapshot: RuntimeTelemetrySnapshot,
        now: Instant,
    ) {
        self.total.add_snapshot(snapshot.io);
        self.reliable_io.add_snapshot(snapshot.reliable.io);
        self.datagram_io.add_snapshot(snapshot.datagram.io);
        self.reliable_flows.add_snapshot(snapshot.reliable.flows);
        self.datagram_flows.add_snapshot(snapshot.datagram.flows);
        self.active_flow_capacity = self
            .active_flow_capacity
            .saturating_add(snapshot.active_flow_capacity);
        self.active_flow_overflow = self
            .active_flow_overflow
            .saturating_add(snapshot.active_flow_record_overflow);
        self.active_flow_overflow_total = self
            .active_flow_overflow_total
            .saturating_add(snapshot.active_flow_record_overflow_total);
        self.flows.extend(
            snapshot
                .active_flows
                .into_iter()
                .map(|flow| flow_status(service, service_index, service_tag.clone(), flow, now)),
        );
    }

    pub(super) fn traffic(&self) -> ManagementTraffic {
        ManagementTraffic {
            total: ManagementIo::from_numeric(self.total),
            reliable: ManagementTrafficKind {
                io: ManagementIo::from_numeric(self.reliable_io),
                flows: ManagementFlowLifecycle::from_numeric(self.reliable_flows),
            },
            datagram: ManagementTrafficKind {
                io: ManagementIo::from_numeric(self.datagram_io),
                flows: ManagementFlowLifecycle::from_numeric(self.datagram_flows),
            },
            rates: ManagementRates::default(),
            trends: Vec::new(),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct SessionInventory {
    sessions: BTreeMap<(&'static str, usize, String), ManagementSessionStatus>,
}

impl SessionInventory {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn insert(
        &mut self,
        service: &'static str,
        service_index: usize,
        service_tag: Option<String>,
        session_id: String,
        state: &'static str,
        carrier_count: usize,
        reference_count: Option<u32>,
    ) {
        self.sessions.insert(
            (service, service_index, session_id.clone()),
            ManagementSessionStatus {
                service,
                service_index,
                service_tag,
                session_id,
                state,
                carrier_count,
                reference_count,
                active_reliable_flows: 0,
                active_datagram_flows: 0,
                active_flow_counts_complete: true,
            },
        );
    }

    pub(super) fn finish(
        mut self,
        flows: &[ManagementFlowStatus],
        active_flow_counts_complete: bool,
    ) -> Vec<ManagementSessionStatus> {
        for session in self.sessions.values_mut() {
            session.active_flow_counts_complete = active_flow_counts_complete;
        }
        for flow in flows {
            let Some(session_id) = flow.session_id.as_ref() else {
                continue;
            };
            let key = (flow.service, flow.service_index, session_id.clone());
            let session = self
                .sessions
                .entry(key)
                .or_insert_with(|| ManagementSessionStatus {
                    service: flow.service,
                    service_index: flow.service_index,
                    service_tag: flow.service_tag.clone(),
                    session_id: session_id.clone(),
                    state: "active",
                    carrier_count: 0,
                    reference_count: None,
                    active_reliable_flows: 0,
                    active_datagram_flows: 0,
                    active_flow_counts_complete,
                });
            match flow.flow_kind {
                "reliable" => {
                    session.active_reliable_flows = session.active_reliable_flows.saturating_add(1)
                }
                "datagram" => {
                    session.active_datagram_flows = session.active_datagram_flows.saturating_add(1)
                }
                _ => {}
            }
            if session.state != "connected" {
                session.state = "active";
            }
        }
        self.sessions.into_values().collect()
    }
}

#[derive(Debug, Default)]
struct TrendHistory {
    previous: Option<(Instant, NumericIo)>,
    samples: VecDeque<ManagementTrendSample>,
}

impl TrendHistory {
    fn current(&self) -> (ManagementRates, Vec<ManagementTrendSample>) {
        let rates = self
            .samples
            .back()
            .map_or_else(ManagementRates::default, |sample| ManagementRates {
                to_peer_bps: sample.to_peer_bps.clone(),
                from_peer_bps: sample.from_peer_bps.clone(),
            });
        (rates, self.samples.iter().cloned().collect())
    }

    fn record(
        &mut self,
        now: Instant,
        timestamp_unix_ms: u64,
        current: NumericIo,
        active_flows: u64,
    ) -> (ManagementRates, Vec<ManagementTrendSample>) {
        let (to_peer_bps, from_peer_bps) = self.previous.map_or((0, 0), |(at, previous)| {
            let elapsed = now.saturating_duration_since(at).as_secs_f64();
            if elapsed <= f64::EPSILON {
                return (0, 0);
            }
            let to_peer = current.to_peer_bytes.saturating_sub(previous.to_peer_bytes);
            let from_peer = current
                .from_peer_bytes
                .saturating_sub(previous.from_peer_bytes);
            (
                ((to_peer as f64 * 8.0) / elapsed).round() as u64,
                ((from_peer as f64 * 8.0) / elapsed).round() as u64,
            )
        });
        self.previous = Some((now, current));
        if self.samples.len() >= TREND_CAPACITY {
            self.samples.pop_front();
        }
        self.samples.push_back(ManagementTrendSample {
            timestamp_unix_ms,
            to_peer_bps: to_peer_bps.to_string(),
            from_peer_bps: from_peer_bps.to_string(),
            to_peer_bytes: current.to_peer_bytes.to_string(),
            from_peer_bytes: current.from_peer_bytes.to_string(),
            active_flows,
        });
        (
            ManagementRates {
                to_peer_bps: to_peer_bps.to_string(),
                from_peer_bps: from_peer_bps.to_string(),
            },
            self.samples.iter().cloned().collect(),
        )
    }
}

pub(super) fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(u64::MAX as u128) as u64
        })
}
