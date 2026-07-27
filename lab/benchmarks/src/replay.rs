//! Versioned deterministic observation-trace replay.
//!
//! This lives in the developer benchmark crate so trace parsing, fixtures, and
//! expected decisions never enter the shipped `mptunnel` binary. Scheduling
//! decisions still execute the production scoring primitives through
//! `mptunnel::simulator`.

use mptunnel::protocol::{PathId, PathUsage, UnderlayProtocol};
use mptunnel::scheduler::{PathSnapshot, PathState, TrafficClass};
use mptunnel::simulator::{FlowId, Simulator, VirtualPath};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

const TRACE_SCHEMA_VERSION: u32 = 1;
const REPORT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservationTrace {
    schema_version: u32,
    trace_id: String,
    initial_paths: Vec<TracePath>,
    events: Vec<TraceEvent>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct TracePath {
    id: u16,
    underlay: TraceUnderlay,
    state: TracePathState,
    srtt_ms: f64,
    jitter_ms: f64,
    delivery_rate_bps: f64,
    #[serde(default)]
    loss_rate: f64,
    #[serde(default = "default_confidence")]
    confidence: f64,
    #[serde(default)]
    queue_bytes: u64,
    #[serde(default)]
    bytes_in_flight: u64,
    #[serde(default)]
    backup: bool,
    #[serde(default)]
    expensive: bool,
    #[serde(default = "default_true")]
    bulk_allowed: bool,
    #[serde(default)]
    no_udp: bool,
    #[serde(default)]
    fail_at_ms: Option<f64>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TraceUnderlay {
    Tcp,
    Quic,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TracePathState {
    Active,
    Suspect,
    Draining,
    Failed,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum TraceEvent {
    Advance {
        at_ms: f64,
    },
    Route {
        at_ms: f64,
        flow_id: u64,
        lane: TraceLane,
        payload_bytes: usize,
        remaining_flow_bytes: usize,
        #[serde(default)]
        duplicate_eligible: bool,
    },
    Transfer {
        at_ms: f64,
        lane: TraceLane,
        total_bytes: usize,
        chunk_bytes: usize,
        #[serde(default)]
        reinjection_delay_ms: Option<f64>,
    },
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TraceLane {
    Control,
    Realtime,
    Interactive,
    Throughput,
    Background,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct ReplayReport {
    schema_version: u32,
    trace_schema_version: u32,
    trace_id: String,
    decisions: Vec<ReplayDecision>,
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ReplayDecision {
    Advance {
        event_index: usize,
        at_ms: f64,
    },
    Route {
        event_index: usize,
        at_ms: f64,
        flow_id: u64,
        path_id: u16,
        duplicate_path_id: Option<u16>,
        estimated_completion_ms: f64,
    },
    Transfer {
        event_index: usize,
        at_ms: f64,
        completion_ms: f64,
        reinjected_chunks: usize,
        failover_gap_ms: Option<f64>,
        path_bytes: Vec<ReplayPathBytes>,
    },
}

#[derive(Debug, Serialize, PartialEq)]
struct ReplayPathBytes {
    path_id: u16,
    bytes: usize,
}

pub fn replay_file(path: &Path) -> Result<ReplayReport, String> {
    let bytes = fs::read(path)
        .map_err(|error| format!("failed to read trace {}: {error}", path.display()))?;
    let trace: ObservationTrace = serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid trace {}: {error}", path.display()))?;
    replay(trace)
}

pub fn render_json(report: &ReplayReport) -> Result<String, String> {
    serde_json::to_string_pretty(report)
        .map_err(|error| format!("failed to serialize replay report: {error}"))
}

fn replay(trace: ObservationTrace) -> Result<ReplayReport, String> {
    validate_trace(&trace)?;
    let mut simulator = Simulator::new(
        trace
            .initial_paths
            .iter()
            .copied()
            .map(TracePath::into_virtual_path)
            .collect(),
    );
    let mut decisions = Vec::with_capacity(trace.events.len());
    let mut previous_at_ms = 0.0;

    for (event_index, event) in trace.events.into_iter().enumerate() {
        let at_ms = event.at_ms();
        if at_ms < previous_at_ms {
            return Err(format!(
                "event {event_index} timestamp {at_ms} precedes {previous_at_ms}"
            ));
        }
        previous_at_ms = at_ms;
        simulator.advance_to(at_ms);

        let decision = match event {
            TraceEvent::Advance { at_ms } => ReplayDecision::Advance { event_index, at_ms },
            TraceEvent::Route {
                at_ms,
                flow_id,
                lane,
                payload_bytes,
                remaining_flow_bytes,
                duplicate_eligible,
            } => {
                let send = simulator
                    .route_flow(
                        FlowId(flow_id),
                        lane.into_traffic_class(),
                        payload_bytes,
                        remaining_flow_bytes,
                        duplicate_eligible,
                    )
                    .ok_or_else(|| format!("event {event_index} has no schedulable path"))?;
                ReplayDecision::Route {
                    event_index,
                    at_ms,
                    flow_id,
                    path_id: send.path_id.0,
                    duplicate_path_id: send.duplicate_path_id.map(|path_id| path_id.0),
                    estimated_completion_ms: rounded(send.estimated_completion_ms),
                }
            }
            TraceEvent::Transfer {
                at_ms,
                lane,
                total_bytes,
                chunk_bytes,
                reinjection_delay_ms,
            } => {
                let lane = lane.into_traffic_class();
                let transfer = match reinjection_delay_ms {
                    Some(delay_ms) => simulator.schedule_transfer_with_reinjection(
                        lane,
                        total_bytes,
                        chunk_bytes,
                        delay_ms,
                    ),
                    None => simulator.schedule_transfer(lane, total_bytes, chunk_bytes),
                }
                .ok_or_else(|| format!("event {event_index} transfer could not be scheduled"))?;
                let path_bytes = transfer
                    .path_bytes()
                    .into_iter()
                    .map(|(path_id, bytes)| ReplayPathBytes {
                        path_id: path_id.0,
                        bytes,
                    })
                    .collect();
                ReplayDecision::Transfer {
                    event_index,
                    at_ms,
                    completion_ms: rounded(transfer.completion_ms),
                    reinjected_chunks: transfer.reinjected_chunks,
                    failover_gap_ms: transfer.failover_gap_ms.map(rounded),
                    path_bytes,
                }
            }
        };
        decisions.push(decision);
    }

    Ok(ReplayReport {
        schema_version: REPORT_SCHEMA_VERSION,
        trace_schema_version: trace.schema_version,
        trace_id: trace.trace_id,
        decisions,
    })
}

fn validate_trace(trace: &ObservationTrace) -> Result<(), String> {
    if trace.schema_version != TRACE_SCHEMA_VERSION {
        return Err(format!(
            "unsupported trace schema {}, expected {}",
            trace.schema_version, TRACE_SCHEMA_VERSION
        ));
    }
    if trace.trace_id.trim().is_empty() {
        return Err("trace_id must not be empty".to_string());
    }
    if trace.initial_paths.is_empty() {
        return Err("initial_paths must not be empty".to_string());
    }
    for (index, path) in trace.initial_paths.iter().enumerate() {
        path.validate(&format!("initial path {index}"))?;
    }
    for (index, event) in trace.events.iter().enumerate() {
        if !event.at_ms().is_finite() || event.at_ms() < 0.0 {
            return Err(format!(
                "event {index} at_ms must be finite and non-negative"
            ));
        }
    }
    Ok(())
}

impl TracePath {
    fn validate(self, owner: &str) -> Result<(), String> {
        for (name, value) in [
            ("srtt_ms", self.srtt_ms),
            ("jitter_ms", self.jitter_ms),
            ("delivery_rate_bps", self.delivery_rate_bps),
            ("loss_rate", self.loss_rate),
            ("confidence", self.confidence),
        ] {
            if !value.is_finite() {
                return Err(format!("{owner} {name} must be finite"));
            }
        }
        if self.srtt_ms < 0.0
            || self.jitter_ms < 0.0
            || self.delivery_rate_bps <= 0.0
            || !(0.0..=1.0).contains(&self.loss_rate)
            || !(0.0..=1.0).contains(&self.confidence)
            || self
                .fail_at_ms
                .is_some_and(|value| !value.is_finite() || value < 0.0)
        {
            return Err(format!("{owner} contains an out-of-range observation"));
        }
        Ok(())
    }

    fn into_virtual_path(self) -> VirtualPath {
        let mut snapshot = PathSnapshot::new(
            PathId(self.id),
            self.underlay.into_underlay_protocol(),
            self.srtt_ms,
            self.delivery_rate_bps,
        );
        snapshot.state = self.state.into_path_state();
        snapshot.jitter_ms = self.jitter_ms;
        snapshot.loss_rate = self.loss_rate;
        snapshot.confidence = self.confidence;
        snapshot.queue_bytes = self.queue_bytes;
        snapshot.bytes_in_flight = self.bytes_in_flight;
        snapshot.policy.backup = self.backup;
        snapshot.policy.expensive = self.expensive;
        snapshot.policy.bulk_allowed = self.bulk_allowed;
        snapshot.policy.no_udp = self.no_udp;
        snapshot.peer_usage = self.backup.then_some(PathUsage::Backup);
        VirtualPath {
            snapshot,
            fail_at_ms: self.fail_at_ms,
        }
    }
}

impl TraceEvent {
    fn at_ms(&self) -> f64 {
        match self {
            Self::Advance { at_ms } | Self::Route { at_ms, .. } | Self::Transfer { at_ms, .. } => {
                *at_ms
            }
        }
    }
}

impl TraceUnderlay {
    fn into_underlay_protocol(self) -> UnderlayProtocol {
        match self {
            Self::Tcp => UnderlayProtocol::Tcp,
            Self::Quic => UnderlayProtocol::Udp,
        }
    }
}

impl TracePathState {
    fn into_path_state(self) -> PathState {
        match self {
            Self::Active => PathState::Active,
            Self::Suspect => PathState::Suspect,
            Self::Draining => PathState::Draining,
            Self::Failed => PathState::Failed,
        }
    }
}

impl TraceLane {
    fn into_traffic_class(self) -> TrafficClass {
        match self {
            Self::Control => TrafficClass::Control,
            Self::Realtime => TrafficClass::RealtimeDatagram,
            Self::Interactive => TrafficClass::Latency,
            Self::Throughput => TrafficClass::Throughput,
            Self::Background => TrafficClass::Background,
        }
    }
}

fn default_confidence() -> f64 {
    1.0
}

fn default_true() -> bool {
    true
}

fn rounded(value: f64) -> f64 {
    (value * 1_000_000.0).round() / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_trace(json: &str) -> ObservationTrace {
        serde_json::from_str(json).expect("valid trace")
    }

    #[test]
    fn replay_is_deterministic_across_observation_and_failure_events() {
        let json = include_str!("../traces/scheduler-failover-v1.json");
        let expected = include_str!("../traces/scheduler-failover-v1.expected.json");

        let first = replay(parse_trace(json)).expect("first replay");
        let second = replay(parse_trace(json)).expect("second replay");

        assert_eq!(first, second);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                &render_json(&first).expect("rendered replay")
            )
            .expect("rendered replay JSON"),
            serde_json::from_str::<serde_json::Value>(expected).expect("expected replay JSON")
        );
        assert!(matches!(
            first.decisions[0],
            ReplayDecision::Route { path_id: 1, .. }
        ));
        assert!(matches!(
            first.decisions[2],
            ReplayDecision::Route { path_id: 2, .. }
        ));
    }

    #[test]
    fn replay_rejects_time_travel() {
        let trace = parse_trace(
            r#"{
              "schema_version": 1,
              "trace_id": "bad-time",
              "initial_paths": [
                {"id": 1, "underlay": "tcp", "state": "active",
                 "srtt_ms": 20.0, "jitter_ms": 0.0,
                 "delivery_rate_bps": 1000000.0}
              ],
              "events": [
                {"type": "advance", "at_ms": 2.0},
                {"type": "advance", "at_ms": 1.0}
              ]
            }"#,
        );

        assert!(replay(trace).unwrap_err().contains("precedes"));
    }
}
