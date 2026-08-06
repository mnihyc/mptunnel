//! Serialized management API schema and its value conversions.
//!
//! Keeping the browser-facing contract independent from runtime owners makes
//! it possible to evolve collection without coupling HTTP serialization to
//! carrier state.

use crate::model::path::PathPolicy;
use crate::protocol::{
    PathMetricDirection, PathUsage, PeerPathState, PeerStatusCode, UnderlayProtocol,
};
use crate::runtime::telemetry::{ProductFlowLifecycleSnapshot, ProductIoSnapshot};
use crate::scheduler::PathState as SchedulerPathState;
use serde::Serialize;

use super::gateway::ManagementBalancerStatus;

pub(super) const SCHEMA: &str = "mptunnel.management.v6";

#[derive(Debug, Clone, Serialize)]
pub(super) struct ManagementSnapshot {
    pub(super) schema: &'static str,
    pub(super) generated_unix_ms: u64,
    pub(super) role: &'static str,
    pub(super) started_unix_ms: u64,
    pub(super) uptime_ms: u64,
    pub(super) services: ManagementServices,
    pub(super) local_inbounds: Vec<ManagementIngressStatus>,
    pub(super) tun_l3_services: Vec<ManagementTunL3Status>,
    pub(super) outbounds: Vec<ManagementOutboundStatus>,
    pub(super) summary: ManagementSummary,
    pub(super) admission: ManagementAdmission,
    pub(super) traffic: ManagementTraffic,
    pub(super) balancers: Vec<ManagementBalancerStatus>,
    pub(super) paths: Vec<ManagementPathStatus>,
    pub(super) sessions: Vec<ManagementSessionStatus>,
    pub(super) flows: Vec<ManagementFlowStatus>,
    pub(super) diagnostics: ManagementDiagnostics,
    pub(super) controls: ManagementControls,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(super) struct ManagementAdmission {
    pub(super) owner_generation: String,
    pub(super) live_flows: usize,
    pub(super) concurrent_work: usize,
    pub(super) dns_work: usize,
    pub(super) tracked_principals: usize,
    pub(super) tracked_outbounds: usize,
    pub(super) tracked_targets: usize,
    pub(super) limits: ManagementAdmissionLimits,
    pub(super) rejections: ManagementAdmissionRejections,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(super) struct ManagementAdmissionLimits {
    pub(super) max_live_flows: usize,
    pub(super) max_concurrent_work: usize,
    pub(super) max_live_flows_per_principal: usize,
    pub(super) max_live_flows_per_outbound: usize,
    pub(super) max_connects_per_outbound: usize,
    pub(super) max_live_flows_per_target: usize,
    pub(super) max_connects_per_target: usize,
    pub(super) max_dns_work: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(super) struct ManagementAdmissionRejections {
    pub(super) global_live_flows: String,
    pub(super) principal_live_flows: String,
    pub(super) outbound_live_flows: String,
    pub(super) target_live_flows: String,
    pub(super) global_concurrent_work: String,
    pub(super) outbound_connects: String,
    pub(super) target_connects: String,
    pub(super) dns_work: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(super) struct ManagementServices {
    pub(super) mpp_outbounds: usize,
    pub(super) mpp_inbounds: usize,
    pub(super) local_inbounds: usize,
    pub(super) tun_l3_services: usize,
    pub(super) local_outbounds: usize,
    pub(super) outbounds: usize,
    pub(super) balancers: usize,
    pub(super) configured_path_listeners: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ManagementTunL3Status {
    pub(super) role: &'static str,
    pub(super) name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) interface_name: Option<String>,
    pub(super) mpp_binding: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) mtu: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) allocation_count: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ManagementOutboundStatus {
    pub(super) name: String,
    pub(super) protocol: &'static str,
    pub(super) networks: Vec<&'static str>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ManagementIngressStatus {
    pub(super) service_index: usize,
    pub(super) name: String,
    pub(super) protocol: &'static str,
    pub(super) listen: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) interface_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) target: Option<String>,
    pub(super) auth_required: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(super) struct ManagementSummary {
    pub(super) path_count: usize,
    pub(super) configured_path_count: usize,
    pub(super) active_paths: usize,
    pub(super) suspect_paths: usize,
    pub(super) failed_paths: usize,
    pub(super) disabled_paths: usize,
    pub(super) active_flows: u64,
    pub(super) active_reliable_flows: u64,
    pub(super) active_datagram_flows: u64,
    pub(super) queue_bytes: String,
    pub(super) bytes_in_flight: String,
    pub(super) data_level_bytes_in_flight: String,
    pub(super) path_delivery_rate_bps: String,
    pub(super) path_pacing_rate_bps: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(super) struct ManagementTraffic {
    pub(super) total: ManagementIo,
    pub(super) reliable: ManagementTrafficKind,
    pub(super) datagram: ManagementTrafficKind,
    pub(super) rates: ManagementRates,
    pub(super) trends: Vec<ManagementTrendSample>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(super) struct ManagementTrafficKind {
    pub(super) io: ManagementIo,
    pub(super) flows: ManagementFlowLifecycle,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ManagementIo {
    pub(super) to_peer_bytes: String,
    pub(super) to_peer_packets: String,
    pub(super) from_peer_bytes: String,
    pub(super) from_peer_packets: String,
}

impl Default for ManagementIo {
    fn default() -> Self {
        Self::from_numeric(NumericIo::default())
    }
}

impl ManagementIo {
    pub(super) fn from_numeric(io: NumericIo) -> Self {
        Self {
            to_peer_bytes: io.to_peer_bytes.to_string(),
            to_peer_packets: io.to_peer_packets.to_string(),
            from_peer_bytes: io.from_peer_bytes.to_string(),
            from_peer_packets: io.from_peer_packets.to_string(),
        }
    }

    pub(super) fn numeric(&self) -> NumericIo {
        NumericIo {
            to_peer_bytes: self.to_peer_bytes.parse().unwrap_or(0),
            to_peer_packets: self.to_peer_packets.parse().unwrap_or(0),
            from_peer_bytes: self.from_peer_bytes.parse().unwrap_or(0),
            from_peer_packets: self.from_peer_packets.parse().unwrap_or(0),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ManagementFlowLifecycle {
    pub(super) opened: String,
    pub(super) active: u64,
    pub(super) completed: String,
    pub(super) failed: String,
}

impl Default for ManagementFlowLifecycle {
    fn default() -> Self {
        Self::from_numeric(NumericLifecycle::default())
    }
}

impl ManagementFlowLifecycle {
    pub(super) fn from_numeric(flows: NumericLifecycle) -> Self {
        Self {
            opened: flows.opened.to_string(),
            active: flows.active,
            completed: flows.completed.to_string(),
            failed: flows.failed.to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ManagementRates {
    pub(super) to_peer_bps: String,
    pub(super) from_peer_bps: String,
}

impl Default for ManagementRates {
    fn default() -> Self {
        Self {
            to_peer_bps: "0".to_string(),
            from_peer_bps: "0".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ManagementTrendSample {
    pub(super) timestamp_unix_ms: u64,
    pub(super) to_peer_bps: String,
    pub(super) from_peer_bps: String,
    pub(super) to_peer_bytes: String,
    pub(super) from_peer_bytes: String,
    pub(super) active_flows: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ManagementPathStatus {
    pub(super) service: &'static str,
    pub(super) service_index: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) service_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) session_id: Option<String>,
    pub(super) path: String,
    pub(super) underlay: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) tcp_carrier_ordinal: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) tcp_carriers_max: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) path_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) path_instance_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) endpoint: Option<String>,
    pub(super) state: &'static str,
    pub(super) manual_disabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) usage: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) policy: Option<PathPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) source: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) direction: Option<&'static str>,
    pub(super) srtt_ms: f64,
    pub(super) jitter_ms: f64,
    pub(super) delivery_rate_bps: String,
    pub(super) pacing_rate_bps: String,
    pub(super) loss_ppm: u32,
    pub(super) ecn_ppm: u32,
    pub(super) queue_bytes: String,
    pub(super) bytes_in_flight: String,
    pub(super) data_level_bytes_in_flight: String,
    pub(super) inflight_limit_bytes: String,
    pub(super) confidence_ppm: u32,
    pub(super) app_limited: bool,
    pub(super) active_flows: u32,
    pub(super) active_latency_sensitive_flows: u32,
    pub(super) delivery_samples: u32,
    pub(super) data_sample_bytes: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) last_delivery_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ManagementSessionStatus {
    pub(super) service: &'static str,
    pub(super) service_index: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) service_name: Option<String>,
    pub(super) session_id: String,
    pub(super) state: &'static str,
    pub(super) carrier_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) reference_count: Option<u32>,
    pub(super) active_reliable_flows: u64,
    pub(super) active_datagram_flows: u64,
    pub(super) active_flow_counts_complete: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ManagementFlowStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) session_id: Option<String>,
    pub(super) flow_kind: &'static str,
    pub(super) flow_id: String,
    pub(super) network: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) inbound_kind: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) inbound: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) outbound: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) balancer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) target: Option<String>,
    pub(super) age_ms: u64,
    pub(super) idle_ms: u64,
    pub(super) io: ManagementIo,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(super) struct ManagementDiagnostics {
    pub(super) peer_diagnostics_allowed: bool,
    pub(super) peer_sessions: Vec<ManagementPeerSession>,
    pub(super) peer_results: Vec<ManagementPeerStatusResult>,
    pub(super) active_flow_detail_capacity: usize,
    pub(super) active_flow_detail_overflow: String,
    pub(super) active_flow_detail_overflow_total: String,
    pub(super) notes: Vec<&'static str>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ManagementPeerSession {
    pub(super) service: &'static str,
    pub(super) service_index: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) service_name: Option<String>,
    pub(super) session_id: String,
    pub(super) carrier_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ManagementPeerStatusResult {
    pub(super) service: &'static str,
    pub(super) service_index: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) service_name: Option<String>,
    pub(super) session_id: String,
    pub(super) request_id: String,
    pub(super) code: &'static str,
    pub(super) received_unix_ms: u64,
    pub(super) paths: Vec<ManagementPeerPathStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ManagementPeerPathStatus {
    pub(super) state: &'static str,
    pub(super) usage: &'static str,
    pub(super) path_id: String,
    pub(super) underlay: &'static str,
    pub(super) direction: &'static str,
    pub(super) metric_epoch: String,
    pub(super) metric_age_us: u32,
    pub(super) srtt_us: u32,
    pub(super) rttvar_us: u32,
    pub(super) jitter_us: u32,
    pub(super) delivery_rate_bps: String,
    pub(super) pacing_rate_bps: String,
    pub(super) loss_ppm: u32,
    pub(super) ecn_ppm: u32,
    pub(super) bytes_in_flight: String,
    pub(super) queue_bytes: String,
    pub(super) inflight_limit_bytes: String,
    pub(super) confidence_ppm: u32,
    pub(super) app_limited: bool,
    pub(super) data_sample_count: u32,
    pub(super) data_sample_bytes: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(super) struct ManagementControls {
    pub(super) path: ManagementControlStatus,
    pub(super) balancer: ManagementControlStatus,
    pub(super) peer_diagnostics: ManagementControlStatus,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(super) struct ManagementControlStatus {
    pub(super) supported: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) operation: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) reason: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct NumericIo {
    pub(super) to_peer_bytes: u64,
    pub(super) to_peer_packets: u64,
    pub(super) from_peer_bytes: u64,
    pub(super) from_peer_packets: u64,
}

impl NumericIo {
    pub(super) fn add_snapshot(&mut self, io: ProductIoSnapshot) {
        self.to_peer_bytes = self.to_peer_bytes.saturating_add(io.to_peer_bytes);
        self.to_peer_packets = self.to_peer_packets.saturating_add(io.to_peer_packets);
        self.from_peer_bytes = self.from_peer_bytes.saturating_add(io.from_peer_bytes);
        self.from_peer_packets = self.from_peer_packets.saturating_add(io.from_peer_packets);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct NumericLifecycle {
    pub(super) opened: u64,
    pub(super) active: u64,
    pub(super) completed: u64,
    pub(super) failed: u64,
}

impl NumericLifecycle {
    pub(super) fn add_snapshot(&mut self, flows: ProductFlowLifecycleSnapshot) {
        self.opened = self.opened.saturating_add(flows.opened);
        self.active = self.active.saturating_add(flows.active);
        self.completed = self.completed.saturating_add(flows.completed);
        self.failed = self.failed.saturating_add(flows.failed);
    }
}

pub(super) fn underlay_name(underlay: UnderlayProtocol) -> &'static str {
    match underlay {
        UnderlayProtocol::Tcp => "tcp",
        UnderlayProtocol::Udp => "udp",
    }
}

pub(super) fn path_usage_name(usage: PathUsage) -> &'static str {
    match usage {
        PathUsage::Available => "available",
        PathUsage::Backup => "backup",
    }
}

pub(super) fn peer_status_code_name(code: PeerStatusCode) -> &'static str {
    match code {
        PeerStatusCode::Ok => "ok",
        PeerStatusCode::Disabled => "disabled",
        PeerStatusCode::Unavailable => "unavailable",
    }
}

pub(super) fn peer_path_state_name(state: PeerPathState) -> &'static str {
    match state {
        PeerPathState::Active => "active",
        PeerPathState::Suspect => "suspect",
        PeerPathState::Draining => "draining",
        PeerPathState::Failed => "failed",
    }
}

pub(super) fn path_state_name(state: SchedulerPathState) -> &'static str {
    match state {
        SchedulerPathState::Active => "active",
        SchedulerPathState::Suspect => "suspect",
        SchedulerPathState::Draining => "draining",
        SchedulerPathState::Failed => "failed",
    }
}

pub(super) fn metric_direction_name(direction: PathMetricDirection) -> &'static str {
    match direction {
        PathMetricDirection::ClientToServer => "client_to_server",
        PathMetricDirection::ServerToClient => "server_to_client",
    }
}
