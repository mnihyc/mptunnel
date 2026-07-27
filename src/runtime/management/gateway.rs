//! Product gateway status projection and explicit operator actions.
//!
//! This module reads the gateway control plane once per management sample.
//! It never consults carrier schedulers or enters payload forwarding.

use super::ManagementTarget;
use super::http::ManagementHttpError;
use crate::product::{
    BalancerId, GatewayFreshnessStatus, GatewayHealthStatus, GatewayMemberMode,
    GatewayObservationSource, GatewaySelectionReason, GatewayStrategy, Network, OutboundId,
};
use crate::runtime::RuntimeError;
use crate::runtime::outbound_registry::{GatewayRuntimeControl, NamedGatewayRuntimeSnapshot};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub(super) const GATEWAY_SCHEMA: &str = "mptunnel.gateway.v1";

#[derive(Debug, Clone, Serialize)]
pub(super) struct ManagementGatewayStatus {
    pub(super) tag: String,
    pub(super) generation: String,
    pub(super) strategy: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) manual_member: Option<String>,
    pub(super) probe: Option<ManagementGatewayProbe>,
    pub(super) ready_members: usize,
    pub(super) draining_members: usize,
    pub(super) unavailable_members: usize,
    pub(super) active_flows: u64,
    pub(super) pending_flows: u64,
    pub(super) members: Vec<ManagementGatewayMemberStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ManagementGatewayProbe {
    pub(super) target: String,
    pub(super) interval_ms: u64,
    pub(super) timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ManagementGatewayMemberStatus {
    pub(super) tag: String,
    pub(super) networks: Vec<&'static str>,
    pub(super) mode: &'static str,
    pub(super) readiness: &'static str,
    pub(super) reason: &'static str,
    pub(super) health: &'static str,
    pub(super) freshness: &'static str,
    pub(super) probe_in_flight: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) cooldown_remaining_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) last_observation_age_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) last_observation_source: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) latency_ewma_us: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) latency_age_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) latency_source: Option<&'static str>,
    pub(super) active_flows: u32,
    pub(super) pending_flows: u32,
    pub(super) consecutive_failures: u32,
    pub(super) recovery_successes: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) last_error_age_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) last_selection_reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) last_selected_age_ms: Option<u64>,
    pub(super) counters: ManagementGatewayCounters,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ManagementGatewayCounters {
    pub(super) selections: String,
    pub(super) open_attempts: String,
    pub(super) open_successes: String,
    pub(super) open_failures: String,
    pub(super) flow_successes: String,
    pub(super) flow_failures: String,
    pub(super) probes: String,
    pub(super) probe_successes: String,
    pub(super) probe_failures: String,
    pub(super) ejections: String,
    pub(super) recoveries: String,
}

#[derive(Debug, Serialize)]
struct GatewayStatusResponse {
    schema: &'static str,
    gateways: Vec<ManagementGatewayStatus>,
}

impl ManagementTarget {
    pub(super) fn gateway_status_json(&self) -> Result<Value, ManagementHttpError> {
        let gateways = collect_gateway_statuses(self.gateway_control.as_ref())?;
        serde_json::to_value(GatewayStatusResponse {
            schema: GATEWAY_SCHEMA,
            gateways,
        })
        .map_err(|_| {
            ManagementHttpError::new(
                500,
                "Internal Server Error",
                "gateway status serialization failed",
            )
        })
    }

    pub(super) fn control_gateway_json(&self, body: &[u8]) -> Result<Value, ManagementHttpError> {
        let request = serde_json::from_slice::<GatewayControlRequest>(body).map_err(|_| {
            ManagementHttpError::new(400, "Bad Request", "invalid gateway control JSON body")
        })?;
        let control = self.gateway_control.as_ref().ok_or_else(|| {
            ManagementHttpError::new(
                409,
                "Conflict",
                "gateway control requires a configured balancer",
            )
        })?;
        let balancer = BalancerId::parse(&request.balancer).map_err(|error| {
            ManagementHttpError::new(400, "Bad Request", format!("invalid balancer: {error}"))
        })?;
        match request.action.as_str() {
            "enable-member" => set_member_mode(
                control,
                &balancer,
                required_member(&request)?,
                GatewayMemberMode::Enabled,
            )?,
            "drain-member" => set_member_mode(
                control,
                &balancer,
                required_member(&request)?,
                GatewayMemberMode::Draining,
            )?,
            "disable-member" => set_member_mode(
                control,
                &balancer,
                required_member(&request)?,
                GatewayMemberMode::Disabled,
            )?,
            "pin-member" => {
                let member = parse_member(required_member(&request)?)?;
                control
                    .set_manual_member(&balancer, Some(&member))
                    .map_err(map_gateway_control_error)?;
            }
            "automatic" => {
                reject_member(&request)?;
                control
                    .set_manual_member(&balancer, None)
                    .map_err(map_gateway_control_error)?;
            }
            _ => {
                return Err(ManagementHttpError::new(
                    400,
                    "Bad Request",
                    "action must be enable-member, drain-member, disable-member, pin-member, or automatic",
                ));
            }
        }
        self.refresh_current_snapshot();
        Ok(json!({
            "schema": GATEWAY_SCHEMA,
            "applied": true,
            "scope": "runtime-generation",
            "balancer": balancer.as_str(),
            "action": request.action,
            "member": request.member,
        }))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GatewayControlRequest {
    balancer: String,
    action: String,
    #[serde(default)]
    member: Option<String>,
}

pub(super) fn collect_gateway_statuses(
    control: Option<&GatewayRuntimeControl>,
) -> Result<Vec<ManagementGatewayStatus>, ManagementHttpError> {
    let Some(control) = control else {
        return Ok(Vec::new());
    };
    control
        .snapshots()
        .map_err(map_gateway_control_error)?
        .into_iter()
        .map(project_gateway)
        .collect()
}

fn project_gateway(
    snapshot: NamedGatewayRuntimeSnapshot,
) -> Result<ManagementGatewayStatus, ManagementHttpError> {
    let now = snapshot.runtime.now;
    let mut ready_members = 0;
    let mut draining_members = 0;
    let mut unavailable_members = 0;
    let mut active_flows = 0_u64;
    let mut pending_flows = 0_u64;
    let mut members = Vec::with_capacity(snapshot.runtime.members.len());
    for member in snapshot.runtime.members {
        let (readiness, reason) = readiness(member.mode, member.health);
        match readiness {
            "ready" => ready_members += 1,
            "draining" => draining_members += 1,
            _ => unavailable_members += 1,
        }
        active_flows = active_flows.saturating_add(u64::from(member.load.active_flows));
        pending_flows = pending_flows.saturating_add(u64::from(member.load.pending_flows));
        members.push(ManagementGatewayMemberStatus {
            tag: member.member.to_string(),
            networks: [Network::Tcp, Network::Udp]
                .into_iter()
                .filter(|network| member.networks.contains(*network))
                .map(network_name)
                .collect(),
            mode: member_mode_name(member.mode),
            readiness,
            reason,
            health: health_name(member.health),
            freshness: freshness_name(member.freshness),
            probe_in_flight: member.probe_in_flight,
            cooldown_remaining_ms: match member.health {
                GatewayHealthStatus::BackingOff { until } => {
                    Some(until.as_millis().saturating_sub(now.as_millis()))
                }
                _ => None,
            },
            last_observation_age_ms: age(now, member.last_observation),
            last_observation_source: member.last_observation_source.map(observation_source_name),
            latency_ewma_us: member
                .latency_ewma
                .map(|latency| latency.as_micros().to_string()),
            latency_age_ms: age(now, member.last_latency_observation),
            latency_source: member
                .last_latency_observation_source
                .map(observation_source_name),
            active_flows: member.load.active_flows,
            pending_flows: member.load.pending_flows,
            consecutive_failures: member.consecutive_failures,
            recovery_successes: member.recovery_successes,
            last_error: member.last_error,
            last_error_age_ms: age(now, member.last_error_at),
            last_selection_reason: member.last_selection_reason.map(selection_reason_name),
            last_selected_age_ms: age(now, member.last_selected_at),
            counters: ManagementGatewayCounters {
                selections: member.counters.selections.to_string(),
                open_attempts: member.counters.open_attempts.to_string(),
                open_successes: member.counters.open_successes.to_string(),
                open_failures: member.counters.open_failures.to_string(),
                flow_successes: member.counters.flow_successes.to_string(),
                flow_failures: member.counters.flow_failures.to_string(),
                probes: member.counters.probes.to_string(),
                probe_successes: member.counters.probe_successes.to_string(),
                probe_failures: member.counters.probe_failures.to_string(),
                ejections: member.counters.ejections.to_string(),
                recoveries: member.counters.recoveries.to_string(),
            },
        });
    }
    Ok(ManagementGatewayStatus {
        tag: snapshot.id.to_string(),
        generation: snapshot.runtime.generation.to_string(),
        strategy: strategy_name(snapshot.runtime.strategy),
        manual_member: snapshot
            .runtime
            .manual_member
            .map(|member| member.to_string()),
        probe: snapshot.runtime.probe.map(|probe| ManagementGatewayProbe {
            target: probe.target.authority(),
            interval_ms: bounded_millis(probe.interval),
            timeout_ms: bounded_millis(probe.timeout),
        }),
        ready_members,
        draining_members,
        unavailable_members,
        active_flows,
        pending_flows,
        members,
    })
}

fn required_member(request: &GatewayControlRequest) -> Result<&str, ManagementHttpError> {
    request.member.as_deref().ok_or_else(|| {
        ManagementHttpError::new(400, "Bad Request", "gateway action requires member")
    })
}

fn reject_member(request: &GatewayControlRequest) -> Result<(), ManagementHttpError> {
    if request.member.is_some() {
        return Err(ManagementHttpError::new(
            400,
            "Bad Request",
            "automatic action must not set member",
        ));
    }
    Ok(())
}

fn parse_member(value: &str) -> Result<OutboundId, ManagementHttpError> {
    OutboundId::parse(value).map_err(|error| {
        ManagementHttpError::new(400, "Bad Request", format!("invalid member: {error}"))
    })
}

fn set_member_mode(
    control: &GatewayRuntimeControl,
    balancer: &BalancerId,
    member: &str,
    mode: GatewayMemberMode,
) -> Result<(), ManagementHttpError> {
    let member = parse_member(member)?;
    control
        .set_member_mode(balancer, &member, mode)
        .map_err(map_gateway_control_error)
}

fn map_gateway_control_error(error: RuntimeError) -> ManagementHttpError {
    match error {
        RuntimeError::GatewayUnavailable(message) => {
            ManagementHttpError::new(404, "Not Found", message)
        }
        error => ManagementHttpError::new(409, "Conflict", error.to_string()),
    }
}

fn readiness(mode: GatewayMemberMode, health: GatewayHealthStatus) -> (&'static str, &'static str) {
    match mode {
        GatewayMemberMode::Draining => ("draining", "operator-drain"),
        GatewayMemberMode::Disabled => ("unavailable", "operator-disabled"),
        GatewayMemberMode::Enabled => match health {
            GatewayHealthStatus::Healthy => ("ready", "healthy"),
            GatewayHealthStatus::BackingOff { .. } => ("unavailable", "circuit-cooldown"),
            GatewayHealthStatus::RecoveryProbeEligible => ("unavailable", "recovery-probe-due"),
            GatewayHealthStatus::RecoveryProbeInFlight => {
                ("unavailable", "recovery-probe-in-flight")
            }
        },
    }
}

fn strategy_name(strategy: GatewayStrategy) -> &'static str {
    match strategy {
        GatewayStrategy::Manual => "manual",
        GatewayStrategy::OrderedFailover => "ordered-failover",
        GatewayStrategy::RoundRobin => "round-robin",
        GatewayStrategy::Random => "random",
        GatewayStrategy::WeightedRandom => "weighted-random",
        GatewayStrategy::LeastLatency => "least-latency",
        GatewayStrategy::LeastLoad => "least-load",
    }
}

fn member_mode_name(mode: GatewayMemberMode) -> &'static str {
    match mode {
        GatewayMemberMode::Enabled => "enabled",
        GatewayMemberMode::Draining => "draining",
        GatewayMemberMode::Disabled => "disabled",
    }
}

fn health_name(health: GatewayHealthStatus) -> &'static str {
    match health {
        GatewayHealthStatus::Healthy => "healthy",
        GatewayHealthStatus::BackingOff { .. } => "backing-off",
        GatewayHealthStatus::RecoveryProbeEligible => "recovery-probe-eligible",
        GatewayHealthStatus::RecoveryProbeInFlight => "recovery-probe-in-flight",
    }
}

fn freshness_name(freshness: GatewayFreshnessStatus) -> &'static str {
    match freshness {
        GatewayFreshnessStatus::NeverObserved => "never-observed",
        GatewayFreshnessStatus::Fresh { .. } => "fresh",
        GatewayFreshnessStatus::Stale { .. } => "stale",
    }
}

fn observation_source_name(source: GatewayObservationSource) -> &'static str {
    match source {
        GatewayObservationSource::ActiveProbe => "active-probe",
        GatewayObservationSource::PassiveOpen => "passive-open",
        GatewayObservationSource::PassiveFlow => "passive-flow",
    }
}

fn selection_reason_name(reason: GatewaySelectionReason) -> &'static str {
    match reason {
        GatewaySelectionReason::Manual => "manual",
        GatewaySelectionReason::OrderedFailover => "ordered-failover",
        GatewaySelectionReason::RoundRobin => "round-robin",
        GatewaySelectionReason::Random => "random",
        GatewaySelectionReason::WeightedRandom => "weighted-random",
        GatewaySelectionReason::LeastLatency => "least-latency",
        GatewaySelectionReason::LeastLoad => "least-load",
        GatewaySelectionReason::DestinationSticky => "destination-sticky",
        GatewaySelectionReason::PrincipalSticky => "principal-sticky",
        GatewaySelectionReason::AllUnhealthyRecoveryProbe => "all-unhealthy-recovery-probe",
        GatewaySelectionReason::AllUnhealthyDeferred { .. } => "all-unhealthy-deferred",
        GatewaySelectionReason::AllUnhealthyRecoveryInFlight => "all-unhealthy-recovery-in-flight",
    }
}

fn network_name(network: Network) -> &'static str {
    match network {
        Network::Tcp => "tcp",
        Network::Udp => "udp",
    }
}

fn age(
    now: crate::product::GatewayInstant,
    at: Option<crate::product::GatewayInstant>,
) -> Option<u64> {
    at.map(|at| now.as_millis().saturating_sub(at.as_millis()))
}

fn bounded_millis(duration: std::time::Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}
