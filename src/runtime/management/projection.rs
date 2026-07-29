//! Runtime-to-management-schema projection.
//!
//! Projection reads endpoint-local owners and produces one detached value
//! graph. The sampler publishes that graph, so HTTP never traverses runtime
//! locks or mutable carrier state.

use super::ManagementTarget;
use super::gateway::collect_balancer_statuses;
use super::schema::{
    ManagementAdmission, ManagementAdmissionLimits, ManagementAdmissionRejections,
    ManagementControlStatus, ManagementControls, ManagementDiagnostics, ManagementFlowStatus,
    ManagementIngressStatus, ManagementIo, ManagementOutboundStatus, ManagementPathStatus,
    ManagementPeerPathStatus, ManagementPeerSession, ManagementPeerStatusResult,
    ManagementServices, ManagementSnapshot, ManagementSummary, NumericIo, SCHEMA,
    metric_direction_name, path_state_name, path_usage_name, peer_path_state_name,
    peer_status_code_name, underlay_name,
};
use super::snapshot::{SessionInventory, TelemetryAggregate, unix_millis};
use crate::product::Network;
use crate::protocol::{PeerPathState, TargetAddr, UnderlayProtocol};
use crate::runtime::path::model::path_snapshot;
use crate::runtime::path::{ClientPathContext, ClientPathHealthRecord, ServerPathContext};
use crate::runtime::peer_status::{PeerStatusBroker, PeerStatusResult};
use crate::runtime::telemetry::{ActiveProductFlowSnapshot, ProductFlowId, ProductFlowOriginKind};
use crate::scheduler::{PathSnapshot, PathState as SchedulerPathState};
use crate::transport::PathSpec;
use std::collections::BTreeMap;
use std::time::{Duration, Instant, UNIX_EPOCH};

pub(super) fn collect_snapshot(
    target: &ManagementTarget,
    role: &'static str,
    started_unix_ms: u64,
    uptime: Duration,
    now: Instant,
) -> ManagementSnapshot {
    let mut services = ManagementServices::default();
    let mut summary = NumericSummary::default();
    let mut paths = Vec::new();
    let mut telemetry = TelemetryAggregate::default();
    let mut session_inventory = SessionInventory::default();
    let mut peer_sessions = Vec::new();
    let mut peer_results = Vec::new();

    services.mpp_outbounds = target.clients.len();
    services.mpp_inbounds = target.servers.len();
    services.local_inbounds = target.inventory.local_inbounds.len();
    services.outbounds = target.inventory.outbounds.len();
    services.local_outbounds = target
        .inventory
        .outbounds
        .iter()
        .filter(|outbound| outbound.protocol != "mpp")
        .count();
    services.configured_path_listeners = target
        .servers
        .iter()
        .map(|context| context.server_paths.len())
        .sum();
    let balancers = match collect_balancer_statuses(target.gateway_control.as_ref()) {
        Ok(balancers) => balancers,
        Err(error) => {
            crate::observability::process_event!(
                Warn,
                "management",
                "balancer_snapshot_unavailable",
                "balancer management snapshot unavailable: {error:?}"
            );
            Vec::new()
        }
    };
    services.balancers = balancers.len();
    for (index, context) in target.clients.iter().enumerate() {
        collect_client(
            context,
            index,
            &mut summary,
            &mut paths,
            &mut session_inventory,
            now,
        );
        let service_name = context
            .outbound
            .as_ref()
            .map(|outbound| outbound.as_str().to_string());
        peer_sessions.extend(service_peer_sessions(
            &context.peer_status,
            "mpp_outbound",
            index,
            service_name.clone(),
        ));
        peer_results.extend(latest_peer_results(
            &context.peer_status,
            "mpp_outbound",
            index,
            service_name,
        ));
    }
    for (index, context) in target.servers.iter().enumerate() {
        collect_server(
            context,
            index,
            &mut summary,
            &mut paths,
            &mut session_inventory,
            now,
        );
        peer_sessions.extend(service_peer_sessions(
            &context.peer_status,
            "mpp_inbound",
            index,
            Some(context.name.clone()),
        ));
        peer_results.extend(latest_peer_results(
            &context.peer_status,
            "mpp_inbound",
            index,
            Some(context.name.clone()),
        ));
    }
    telemetry.add(target.product_telemetry.snapshot(), now);
    let peer_diagnostics_allowed = target
        .clients
        .iter()
        .any(|context| context.peer_status.allows_incoming())
        || target
            .servers
            .iter()
            .any(|context| context.peer_status.allows_incoming());
    peer_sessions.sort_unstable_by(|left, right| {
        left.service
            .cmp(right.service)
            .then_with(|| left.service_index.cmp(&right.service_index))
            .then_with(|| left.session_id.cmp(&right.session_id))
    });
    peer_results.sort_unstable_by(|left, right| {
        left.service
            .cmp(right.service)
            .then_with(|| left.service_index.cmp(&right.service_index))
            .then_with(|| left.session_id.cmp(&right.session_id))
            .then_with(|| left.request_id.cmp(&right.request_id))
    });

    summary.active_flows = telemetry
        .reliable_flows
        .active
        .saturating_add(telemetry.datagram_flows.active);
    summary.active_reliable_flows = telemetry.reliable_flows.active;
    summary.active_datagram_flows = telemetry.datagram_flows.active;
    let summary = summary.finish();
    let controls = ManagementControls {
        path: ManagementControlStatus {
            supported: services.mpp_outbounds > 0,
            operation: (services.mpp_outbounds > 0).then_some("POST /api/v2/actions/path"),
            reason: (services.mpp_outbounds == 0)
                .then_some("path control requires a local inbound service with an MPP outbound"),
        },
        balancer: ManagementControlStatus {
            supported: target.gateway_control.is_some(),
            operation: target
                .gateway_control
                .is_some()
                .then_some("POST /api/v2/balancers/actions"),
            reason: target
                .gateway_control
                .is_none()
                .then_some("balancer control requires a configured Product balancer"),
        },
        peer_diagnostics: ManagementControlStatus {
            supported: !peer_sessions.is_empty(),
            operation: (!peer_sessions.is_empty()).then_some("POST /api/v2/diagnostics/peer"),
            reason: peer_sessions
                .is_empty()
                .then_some("no authenticated peer control carrier is currently available"),
        },
    };
    let diagnostics = ManagementDiagnostics {
        peer_diagnostics_allowed,
        peer_sessions,
        peer_results,
        active_flow_detail_capacity: telemetry.active_flow_capacity,
        active_flow_detail_overflow: telemetry.active_flow_overflow.to_string(),
        active_flow_detail_overflow_total: telemetry.active_flow_overflow_total.to_string(),
        notes: vec![
            "forwarded totals count logical product traffic, not carrier retransmission or reinjection",
            "path rates and flight are current carrier evidence, not forwarded traffic totals",
        ],
    };
    let traffic = telemetry.traffic();
    let mut flows = std::mem::take(&mut telemetry.flows);
    flows.sort_unstable_by(|left, right| {
        left.inbound
            .cmp(&right.inbound)
            .then_with(|| left.session_id.cmp(&right.session_id))
            .then_with(|| left.flow_id.cmp(&right.flow_id))
    });
    let sessions = session_inventory.finish(&flows, telemetry.active_flow_overflow == 0);
    let admission = management_admission(target.product_admission.snapshot());
    let local_inbounds = target
        .inventory
        .local_inbounds
        .iter()
        .enumerate()
        .map(|(service_index, inbound)| ManagementIngressStatus {
            service_index,
            name: inbound.name.clone(),
            protocol: inbound.protocol,
            listen: inbound.listen.clone(),
            interface_name: inbound.interface_name.clone(),
            target: inbound.target.clone(),
            auth_required: inbound.auth_required,
        })
        .collect();
    let outbounds = target
        .inventory
        .outbounds
        .iter()
        .map(|outbound| ManagementOutboundStatus {
            name: outbound.id.as_str().to_string(),
            protocol: outbound.protocol,
            networks: outbound
                .networks
                .iter()
                .map(|network| network_name(*network))
                .collect(),
        })
        .collect();

    ManagementSnapshot {
        schema: SCHEMA,
        generated_unix_ms: unix_millis(),
        role,
        started_unix_ms,
        uptime_ms: uptime.as_millis().min(u64::MAX as u128) as u64,
        services,
        local_inbounds,
        outbounds,
        summary,
        admission,
        traffic,
        balancers,
        paths,
        sessions,
        flows,
        diagnostics,
        controls,
    }
}

fn management_admission(snapshot: crate::product::ProductAdmissionSnapshot) -> ManagementAdmission {
    let limits = snapshot.limits;
    let rejections = snapshot.rejections;
    ManagementAdmission {
        owner_generation: snapshot.owner_generation.to_string(),
        live_flows: snapshot.live_flows,
        concurrent_work: snapshot.concurrent_work,
        dns_work: snapshot.dns_work,
        tracked_principals: snapshot.principals.len(),
        tracked_outbounds: snapshot.outbounds.len(),
        tracked_targets: snapshot.targets.len(),
        limits: ManagementAdmissionLimits {
            max_live_flows: limits.max_live_flows,
            max_concurrent_work: limits.max_concurrent_work,
            max_live_flows_per_principal: limits.max_live_flows_per_principal,
            max_live_flows_per_outbound: limits.max_live_flows_per_outbound,
            max_connects_per_outbound: limits.max_connects_per_outbound,
            max_live_flows_per_target: limits.max_live_flows_per_target,
            max_connects_per_target: limits.max_connects_per_target,
            max_dns_work: limits.max_dns_work,
        },
        rejections: ManagementAdmissionRejections {
            global_live_flows: rejections.global_live_flows.to_string(),
            principal_live_flows: rejections.principal_live_flows.to_string(),
            outbound_live_flows: rejections.outbound_live_flows.to_string(),
            target_live_flows: rejections.target_live_flows.to_string(),
            global_concurrent_work: rejections.global_concurrent_work.to_string(),
            outbound_connects: rejections.outbound_connects.to_string(),
            target_connects: rejections.target_connects.to_string(),
            dns_work: rejections.dns_work.to_string(),
        },
    }
}

fn service_peer_sessions(
    broker: &PeerStatusBroker,
    service: &'static str,
    service_index: usize,
    service_name: Option<String>,
) -> Vec<ManagementPeerSession> {
    broker
        .session_ids()
        .into_iter()
        .map(|session_id| ManagementPeerSession {
            service,
            service_index,
            service_name: service_name.clone(),
            session_id: session_id.0.to_string(),
            carrier_count: broker.carrier_count(session_id),
        })
        .collect()
}

fn latest_peer_results(
    broker: &PeerStatusBroker,
    service: &'static str,
    service_index: usize,
    service_name: Option<String>,
) -> Vec<ManagementPeerStatusResult> {
    broker
        .session_ids()
        .into_iter()
        .filter_map(|session_id| broker.latest(session_id))
        .map(|result| peer_status_result(result, service, service_index, service_name.clone()))
        .collect()
}

pub(super) fn peer_status_result(
    result: PeerStatusResult,
    service: &'static str,
    service_index: usize,
    service_name: Option<String>,
) -> ManagementPeerStatusResult {
    let received_unix_ms = result
        .received_at
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(u64::MAX as u128) as u64
        });
    ManagementPeerStatusResult {
        service,
        service_index,
        service_name,
        session_id: result.session_id.0.to_string(),
        request_id: result.request_id.to_string(),
        code: peer_status_code_name(result.code),
        received_unix_ms,
        paths: result
            .paths
            .into_iter()
            .map(|path| ManagementPeerPathStatus {
                state: peer_path_state_name(path.state),
                usage: path_usage_name(path.usage),
                path_id: path.metrics.path_id.0.to_string(),
                underlay: underlay_name(path.metrics.underlay),
                direction: metric_direction_name(path.metrics.direction),
                metric_epoch: path.metrics.metric_epoch.to_string(),
                metric_age_us: path.metrics.metric_age_us,
                srtt_us: path.metrics.srtt_us,
                rttvar_us: path.metrics.rttvar_us,
                jitter_us: path.metrics.jitter_us,
                delivery_rate_bps: path.metrics.delivery_rate_bps.to_string(),
                pacing_rate_bps: path.metrics.pacing_rate_bps.to_string(),
                loss_ppm: path.metrics.loss_ppm,
                ecn_ppm: path.metrics.ecn_ppm,
                bytes_in_flight: path.metrics.bytes_in_flight.to_string(),
                queue_bytes: path.metrics.queue_bytes.to_string(),
                inflight_limit_bytes: path.metrics.inflight_limit_bytes.to_string(),
                confidence_ppm: path.metrics.confidence_ppm,
                app_limited: path.metrics.app_limited,
                data_sample_count: path.metrics.data_sample_count,
                data_sample_bytes: path.metrics.data_sample_bytes.to_string(),
            })
            .collect(),
    }
}

#[derive(Debug, Default)]
pub(super) struct NumericSummary {
    path_count: usize,
    pub(super) configured_path_count: usize,
    active_paths: usize,
    suspect_paths: usize,
    failed_paths: usize,
    disabled_paths: usize,
    pub(super) active_flows: u64,
    pub(super) active_reliable_flows: u64,
    pub(super) active_datagram_flows: u64,
    queue_bytes: u64,
    bytes_in_flight: u64,
    data_level_bytes_in_flight: u64,
    path_delivery_rate_bps: u64,
    path_pacing_rate_bps: u64,
}

impl NumericSummary {
    fn add_path(&mut self, snapshot: PathSnapshot, manual_disabled: bool) {
        self.path_count = self.path_count.saturating_add(1);
        if manual_disabled {
            self.disabled_paths = self.disabled_paths.saturating_add(1);
        } else {
            match snapshot.state {
                SchedulerPathState::Active => {
                    self.active_paths = self.active_paths.saturating_add(1)
                }
                SchedulerPathState::Suspect | SchedulerPathState::Draining => {
                    self.suspect_paths = self.suspect_paths.saturating_add(1)
                }
                SchedulerPathState::Failed => {
                    self.failed_paths = self.failed_paths.saturating_add(1)
                }
            }
        }
        self.queue_bytes = self.queue_bytes.saturating_add(snapshot.queue_bytes);
        self.bytes_in_flight = self
            .bytes_in_flight
            .saturating_add(snapshot.bytes_in_flight);
        self.data_level_bytes_in_flight = self
            .data_level_bytes_in_flight
            .saturating_add(snapshot.data_level_bytes_in_flight);
        self.path_delivery_rate_bps = self
            .path_delivery_rate_bps
            .saturating_add(snapshot.delivery_rate_bps.round() as u64);
        self.path_pacing_rate_bps = self
            .path_pacing_rate_bps
            .saturating_add(snapshot.pacing_rate_bps.round() as u64);
    }

    fn add_server_path(
        &mut self,
        state: PeerPathState,
        metrics: Option<crate::protocol::PathMetrics>,
    ) {
        self.path_count = self.path_count.saturating_add(1);
        match state {
            PeerPathState::Active => self.active_paths = self.active_paths.saturating_add(1),
            PeerPathState::Suspect | PeerPathState::Draining => {
                self.suspect_paths = self.suspect_paths.saturating_add(1)
            }
            PeerPathState::Failed => self.failed_paths = self.failed_paths.saturating_add(1),
        }
        if let Some(metrics) = metrics {
            self.queue_bytes = self.queue_bytes.saturating_add(metrics.queue_bytes);
            self.bytes_in_flight = self.bytes_in_flight.saturating_add(metrics.bytes_in_flight);
            self.path_delivery_rate_bps = self
                .path_delivery_rate_bps
                .saturating_add(metrics.delivery_rate_bps);
            self.path_pacing_rate_bps = self
                .path_pacing_rate_bps
                .saturating_add(metrics.pacing_rate_bps);
        }
    }

    pub(super) fn finish(self) -> ManagementSummary {
        ManagementSummary {
            path_count: self.path_count,
            configured_path_count: self.configured_path_count,
            active_paths: self.active_paths,
            suspect_paths: self.suspect_paths,
            failed_paths: self.failed_paths,
            disabled_paths: self.disabled_paths,
            active_flows: self.active_flows,
            active_reliable_flows: self.active_reliable_flows,
            active_datagram_flows: self.active_datagram_flows,
            queue_bytes: self.queue_bytes.to_string(),
            bytes_in_flight: self.bytes_in_flight.to_string(),
            data_level_bytes_in_flight: self.data_level_bytes_in_flight.to_string(),
            path_delivery_rate_bps: self.path_delivery_rate_bps.to_string(),
            path_pacing_rate_bps: self.path_pacing_rate_bps.to_string(),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn collect_client(
    context: &ClientPathContext,
    service_index: usize,
    summary: &mut NumericSummary,
    paths: &mut Vec<ManagementPathStatus>,
    sessions: &mut SessionInventory,
    now: Instant,
) {
    let service_name = context
        .outbound
        .as_ref()
        .map(|outbound| outbound.as_str().to_string());
    let session_id = context.session_id.0.to_string();
    let carrier_count = context.peer_status.carrier_count(context.session_id);
    sessions.insert(
        "mpp_outbound",
        service_index,
        service_name.clone(),
        session_id,
        if carrier_count > 0 {
            "connected"
        } else {
            "idle"
        },
        carrier_count,
        None,
    );
    summary.configured_path_count = summary.configured_path_count.saturating_add(
        context
            .configured_tcp_endpoint_count()
            .saturating_add(context.udp_paths.len()),
    );
    let health = context.health().lock().expect("client path health lock");
    paths.extend(client_path_set(
        &context.tcp_path_names,
        &context.tcp_paths,
        &health.tcp,
        UnderlayProtocol::Tcp,
        service_index,
        service_name.clone(),
        context.session_id.0,
        summary,
        now,
        Some(context),
    ));
    paths.extend(client_path_set(
        &context.udp_path_names,
        &context.udp_paths,
        &health.udp,
        UnderlayProtocol::Udp,
        service_index,
        service_name,
        context.session_id.0,
        summary,
        now,
        None,
    ));
    drop(health);
}

#[allow(clippy::too_many_arguments)]
fn client_path_set(
    names: &[String],
    specs: &[PathSpec],
    records: &[ClientPathHealthRecord],
    underlay: UnderlayProtocol,
    service_index: usize,
    service_name: Option<String>,
    session_id: u64,
    summary: &mut NumericSummary,
    now: Instant,
    tcp_context: Option<&ClientPathContext>,
) -> Vec<ManagementPathStatus> {
    specs
        .iter()
        .enumerate()
        .filter_map(|(index, spec)| {
            if underlay == UnderlayProtocol::Tcp
                && records
                    .get(index)
                    .is_some_and(|record| !record.is_locally_eligible())
            {
                return None;
            }
            let path = names
                .get(index)
                .expect("client path names align with underlay-local path inventory")
                .clone();
            let observation = records
                .get(index)
                .map(|record| record.observation_at(now))
                .unwrap_or_default();
            let snapshot = path_snapshot(spec, index, observation);
            let tcp_endpoint = tcp_context.and_then(|context| context.tcp_endpoint_for_path(index));
            summary.add_path(snapshot, observation.manual_disabled);
            Some(ManagementPathStatus {
                service: "mpp_outbound",
                service_index,
                service_name: service_name.clone(),
                session_id: Some(session_id.to_string()),
                path,
                underlay: underlay_name(underlay),
                tcp_carrier_ordinal: tcp_context
                    .and_then(|context| context.tcp_member_ordinal(index))
                    .map(|ordinal| ordinal.saturating_add(1)),
                tcp_carriers_min: tcp_endpoint.map(|endpoint| endpoint.range.min()),
                tcp_carriers_max: tcp_endpoint.map(|endpoint| endpoint.range.max()),
                path_id: Some(snapshot.id.0.to_string()),
                path_instance_id: None,
                endpoint: Some(path_endpoint(spec)),
                state: if observation.manual_disabled {
                    "disabled"
                } else {
                    path_state_name(snapshot.state)
                },
                manual_disabled: observation.manual_disabled,
                usage: snapshot.peer_usage.map(path_usage_name),
                policy: Some(snapshot.policy),
                source: Some("local"),
                direction: Some("client_to_server"),
                srtt_ms: snapshot.srtt_ms,
                jitter_ms: snapshot.jitter_ms,
                delivery_rate_bps: (snapshot.delivery_rate_bps.round() as u64).to_string(),
                pacing_rate_bps: (snapshot.pacing_rate_bps.round() as u64).to_string(),
                loss_ppm: fraction_to_ppm(snapshot.loss_rate),
                ecn_ppm: 0,
                queue_bytes: snapshot.queue_bytes.to_string(),
                bytes_in_flight: snapshot.bytes_in_flight.to_string(),
                data_level_bytes_in_flight: snapshot.data_level_bytes_in_flight.to_string(),
                inflight_limit_bytes: snapshot.carrier_inflight_limit_bytes.to_string(),
                confidence_ppm: fraction_to_ppm(snapshot.confidence),
                app_limited: snapshot.app_limited,
                active_flows: snapshot.active_flows,
                active_latency_sensitive_flows: snapshot.active_latency_sensitive_flows,
                delivery_samples: observation
                    .delivery_samples
                    .saturating_add(observation.carrier_delivery_samples),
                data_sample_bytes: observation
                    .product_delivery_sample_bytes
                    .saturating_add(observation.carrier_delivery_sample_bytes)
                    .to_string(),
                last_delivery_age_ms: age_ms(
                    now,
                    observation
                        .last_delivery_at
                        .max(observation.carrier_last_delivery_at),
                ),
            })
        })
        .collect()
}

fn collect_server(
    context: &ServerPathContext,
    service_index: usize,
    summary: &mut NumericSummary,
    paths: &mut Vec<ManagementPathStatus>,
    sessions: &mut SessionInventory,
    _now: Instant,
) {
    summary.configured_path_count = summary
        .configured_path_count
        .saturating_add(context.server_paths.len());
    let service_name = Some(context.name.clone());
    for (index, spec) in context.server_paths.iter().enumerate() {
        let path = context
            .configured_path_names
            .get(index)
            .expect("server path names align with configured path inventory")
            .clone();
        paths.push(ManagementPathStatus {
            service: "mpp_inbound",
            service_index,
            service_name: service_name.clone(),
            session_id: None,
            path,
            underlay: underlay_name(spec.underlay),
            tcp_carrier_ordinal: None,
            tcp_carriers_min: None,
            tcp_carriers_max: None,
            path_id: None,
            path_instance_id: None,
            endpoint: Some(path_endpoint(spec)),
            state: "listening",
            manual_disabled: false,
            usage: None,
            policy: Some(spec.metadata.policy),
            source: Some("configured_listener"),
            direction: None,
            srtt_ms: 0.0,
            jitter_ms: 0.0,
            delivery_rate_bps: "0".to_string(),
            pacing_rate_bps: "0".to_string(),
            loss_ppm: 0,
            ecn_ppm: 0,
            queue_bytes: "0".to_string(),
            bytes_in_flight: "0".to_string(),
            data_level_bytes_in_flight: "0".to_string(),
            inflight_limit_bytes: "0".to_string(),
            confidence_ppm: 0,
            app_limited: false,
            active_flows: 0,
            active_latency_sensitive_flows: 0,
            delivery_samples: 0,
            data_sample_bytes: "0".to_string(),
            last_delivery_age_ms: None,
        });
    }
    let registry = context.reliable_streams.management_snapshot();
    let mut carrier_counts = BTreeMap::new();
    for path in &registry.paths {
        *carrier_counts.entry(path.session_id).or_insert(0usize) += 1;
    }
    for session in registry.sessions {
        let carrier_count = carrier_counts
            .get(&session.session_id)
            .copied()
            .unwrap_or(0);
        sessions.insert(
            "mpp_inbound",
            service_index,
            service_name.clone(),
            session.session_id.0.to_string(),
            if carrier_count > 0 {
                "connected"
            } else {
                "draining"
            },
            carrier_count,
            Some(session.reference_count),
        );
    }
    for path in registry.paths {
        summary.add_server_path(path.state, path.metrics);
        let metrics = path.metrics;
        let configured_path = context
            .configured_path_names
            .get(path.configured_index)
            .expect("server session path refers to configured path inventory")
            .clone();
        paths.push(ManagementPathStatus {
            service: "mpp_inbound",
            service_index,
            service_name: service_name.clone(),
            session_id: Some(path.session_id.0.to_string()),
            path: configured_path,
            underlay: underlay_name(path.underlay),
            tcp_carrier_ordinal: None,
            tcp_carriers_min: None,
            tcp_carriers_max: None,
            path_id: Some(path.path_id.0.to_string()),
            path_instance_id: Some(path.path_instance_id.as_u64().to_string()),
            endpoint: None,
            state: peer_path_state_name(path.state),
            manual_disabled: false,
            usage: path.usage.map(path_usage_name),
            policy: Some(path.policy),
            source: path.source,
            direction: metrics.map(|metrics| metric_direction_name(metrics.direction)),
            srtt_ms: metrics.map_or(0.0, |metrics| f64::from(metrics.srtt_us) / 1_000.0),
            jitter_ms: metrics.map_or(0.0, |metrics| f64::from(metrics.jitter_us) / 1_000.0),
            delivery_rate_bps: metrics
                .map_or(0, |metrics| metrics.delivery_rate_bps)
                .to_string(),
            pacing_rate_bps: metrics
                .map_or(0, |metrics| metrics.pacing_rate_bps)
                .to_string(),
            loss_ppm: metrics.map_or(0, |metrics| metrics.loss_ppm),
            ecn_ppm: metrics.map_or(0, |metrics| metrics.ecn_ppm),
            queue_bytes: metrics.map_or(0, |metrics| metrics.queue_bytes).to_string(),
            bytes_in_flight: metrics
                .map_or(0, |metrics| metrics.bytes_in_flight)
                .to_string(),
            data_level_bytes_in_flight: "0".to_string(),
            inflight_limit_bytes: metrics
                .map_or(0, |metrics| metrics.inflight_limit_bytes)
                .to_string(),
            confidence_ppm: metrics.map_or(0, |metrics| metrics.confidence_ppm),
            app_limited: metrics.is_none_or(|metrics| metrics.app_limited),
            active_flows: 0,
            active_latency_sensitive_flows: 0,
            delivery_samples: metrics.map_or(0, |metrics| metrics.data_sample_count),
            data_sample_bytes: metrics
                .map_or(0, |metrics| metrics.data_sample_bytes)
                .to_string(),
            last_delivery_age_ms: metrics.map(|metrics| u64::from(metrics.metric_age_us) / 1_000),
        });
    }
}

pub(super) fn flow_status(flow: ActiveProductFlowSnapshot, now: Instant) -> ManagementFlowStatus {
    let flow_kind = match flow.flow_id {
        ProductFlowId::Reliable(_) | ProductFlowId::NativeReliable => "reliable",
        ProductFlowId::Datagram(_) | ProductFlowId::NativeDatagram => "datagram",
    };
    let (inbound_kind, inbound) = flow.origin.as_ref().map_or((None, None), |origin| {
        let kind = match origin.kind {
            ProductFlowOriginKind::LocalInbound => "local",
            ProductFlowOriginKind::MppInbound => "mpp",
        };
        (Some(kind), Some(origin.inbound.as_str().to_string()))
    });
    let selection = flow.selection.as_ref();
    ManagementFlowStatus {
        session_id: flow.session_id.map(|id| id.0.to_string()),
        flow_kind,
        flow_id: flow.display_id.to_string(),
        network: network_name(flow.network),
        inbound_kind,
        inbound,
        outbound: selection.map(|selection| selection.outbound.as_str().to_string()),
        balancer: selection
            .and_then(|selection| selection.balancer.as_ref())
            .map(|balancer| balancer.as_str().to_string()),
        target: flow.target.as_ref().map(target_name),
        age_ms: now
            .saturating_duration_since(flow.started_at)
            .as_millis()
            .min(u64::MAX as u128) as u64,
        idle_ms: now
            .saturating_duration_since(flow.last_activity_at)
            .as_millis()
            .min(u64::MAX as u128) as u64,
        io: ManagementIo::from_numeric(NumericIo {
            to_peer_bytes: flow.io.to_peer_bytes,
            to_peer_packets: flow.io.to_peer_packets,
            from_peer_bytes: flow.io.from_peer_bytes,
            from_peer_packets: flow.io.from_peer_packets,
        }),
    }
}

const fn network_name(network: Network) -> &'static str {
    match network {
        Network::Tcp => "tcp",
        Network::Udp => "udp",
    }
}

fn target_name(target: &TargetAddr) -> String {
    target.authority()
}

fn path_endpoint(spec: &PathSpec) -> String {
    format!(
        "{}://{}",
        underlay_name(spec.underlay),
        spec.endpoint.authority()
    )
}

fn fraction_to_ppm(value: f64) -> u32 {
    (value.clamp(0.0, 1.0) * 1_000_000.0).round() as u32
}

fn age_ms(now: Instant, instant: Option<Instant>) -> Option<u64> {
    instant.map(|instant| {
        now.saturating_duration_since(instant)
            .as_millis()
            .min(u64::MAX as u128) as u64
    })
}
