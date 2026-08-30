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
    ManagementServices, ManagementSnapshot, ManagementSummary, ManagementTunL3Status, NumericIo,
    SCHEMA, metric_direction_name, path_state_name, path_usage_name, peer_path_state_name,
    peer_status_code_name, underlay_name,
};
use super::snapshot::{SessionInventory, TelemetryAggregate, unix_millis};
#[cfg(test)]
use crate::model::capacity::RELIABLE_INITIAL_RTT;
#[cfg(test)]
use crate::model::timing::transport_rate_sample_freshness_horizon;
use crate::product::Network;
use crate::protocol::{PathMetricDirection, PeerPathState, TargetAddr, UnderlayProtocol};
use crate::runtime::path::model::{ClientPathObservation, path_snapshot};
use crate::runtime::path::{
    AuthenticatedCarrierAvailability, ClientPathContext, ClientPathHealth, ClientPathHealthRecord,
    ClientPathRateDiagnostics, ServerCarrierPathStatusSnapshot, ServerPathContext,
};
use crate::runtime::peer_status::{PeerStatusBroker, PeerStatusResult};
use crate::runtime::telemetry::{
    ActiveProductFlowSnapshot, ProductFlowId, ProductFlowOriginKind, ProductFlowSourceKind,
};
use crate::scheduler::{PathRateScope, PathSnapshot, PathState as SchedulerPathState};
use crate::transport::{PathSpec, RateHint};
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
    services.tun_l3_services = target.tun_l3_inventory.services.len();
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
            PeerPathIdentitySource::Client(context),
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
            PeerPathIdentitySource::Server(context),
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
            operation: (services.mpp_outbounds > 0).then_some("POST /api/v4/actions/path"),
            reason: (services.mpp_outbounds == 0)
                .then_some("path control requires a local inbound service with an MPP outbound"),
        },
        balancer: ManagementControlStatus {
            supported: target.gateway_control.is_some(),
            operation: target
                .gateway_control
                .is_some()
                .then_some("POST /api/v4/balancers/actions"),
            reason: target
                .gateway_control
                .is_none()
                .then_some("balancer control requires a configured outbound balancer"),
        },
        peer_diagnostics: ManagementControlStatus {
            supported: !peer_sessions.is_empty(),
            operation: (!peer_sessions.is_empty()).then_some("POST /api/v4/diagnostics/peer"),
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
    let tun_l3_services = target
        .tun_l3_inventory
        .services
        .iter()
        .map(|service| ManagementTunL3Status {
            role: service.role.as_str(),
            name: service.name.clone(),
            interface_name: service.interface_name.clone(),
            mpp_binding: service.mpp_binding.clone(),
            mtu: service.mtu,
            allocation_count: service.allocation_count,
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
        tun_l3_services,
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
    identity_source: PeerPathIdentitySource<'_>,
) -> Vec<ManagementPeerStatusResult> {
    broker
        .session_ids()
        .into_iter()
        .filter_map(|session_id| broker.latest(session_id))
        .map(|result| {
            peer_status_result(
                result,
                service,
                service_index,
                service_name.clone(),
                identity_source,
            )
        })
        .collect()
}

#[derive(Clone, Copy)]
pub(super) enum PeerPathIdentitySource<'a> {
    Client(&'a ClientPathContext),
    Server(&'a ServerPathContext),
}

#[derive(Debug)]
struct LocalPeerPathIdentity {
    path: String,
    endpoint: String,
    port_hopping: bool,
    active_port: Option<u16>,
    active_port_retired: bool,
}

impl PeerPathIdentitySource<'_> {
    fn resolve(
        self,
        underlay: UnderlayProtocol,
        local_index: usize,
        active_port: Option<u16>,
        active_port_retired: bool,
    ) -> Option<LocalPeerPathIdentity> {
        match self {
            Self::Client(context) => {
                let (path, spec) = match underlay {
                    UnderlayProtocol::Tcp => (
                        context.tcp_path_name(local_index)?.to_string(),
                        context.tcp_path_spec(local_index)?,
                    ),
                    UnderlayProtocol::Udp => (
                        context.udp_path_name(local_index)?.to_string(),
                        context.udp_paths.get(local_index)?,
                    ),
                };
                Some(LocalPeerPathIdentity {
                    path,
                    endpoint: path_endpoint(spec),
                    port_hopping: spec.port_hop_interval().is_some(),
                    active_port,
                    active_port_retired,
                })
            }
            Self::Server(context) => {
                let path = context.configured_path_names.get(local_index)?.clone();
                let spec = context.server_paths.get(local_index)?;
                if spec.underlay != underlay {
                    return None;
                }
                Some(LocalPeerPathIdentity {
                    path,
                    endpoint: path_endpoint(spec),
                    port_hopping: spec.port_hop_interval().is_some(),
                    active_port: None,
                    active_port_retired: false,
                })
            }
        }
    }
}

pub(super) fn peer_status_result(
    result: PeerStatusResult,
    service: &'static str,
    service_index: usize,
    service_name: Option<String>,
    identity_source: PeerPathIdentitySource<'_>,
) -> ManagementPeerStatusResult {
    let PeerStatusResult {
        session_id,
        request_id,
        code,
        paths,
        local_paths,
        received_at,
    } = result;
    let received_unix_ms = received_at
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(u64::MAX as u128) as u64
        });
    ManagementPeerStatusResult {
        service,
        service_index,
        service_name,
        session_id: session_id.0.to_string(),
        request_id: request_id.to_string(),
        code: peer_status_code_name(code),
        received_unix_ms,
        paths: paths
            .into_iter()
            .map(|path| {
                let identity = local_paths
                    .get(&(path.metrics.underlay, path.metrics.path_id))
                    .copied()
                    .and_then(|local_path| {
                        identity_source.resolve(
                            path.metrics.underlay,
                            local_path.local_path_index,
                            local_path.active_port,
                            local_path.retired,
                        )
                    });
                let latency_observed = path.metrics.srtt_us > 0;
                ManagementPeerPathStatus {
                    state: peer_path_state_name(path.state),
                    usage: path_usage_name(path.usage),
                    usage_direction: opposite_metric_direction_name(path.metrics.direction),
                    path: identity.as_ref().map(|identity| identity.path.clone()),
                    endpoint: identity.as_ref().map(|identity| identity.endpoint.clone()),
                    port_hopping: identity
                        .as_ref()
                        .is_some_and(|identity| identity.port_hopping),
                    active_port: identity.as_ref().and_then(|identity| identity.active_port),
                    active_port_retired: identity
                        .as_ref()
                        .is_some_and(|identity| identity.active_port_retired),
                    path_id: path.metrics.path_id.0.to_string(),
                    underlay: underlay_name(path.metrics.underlay),
                    direction: metric_direction_name(path.metrics.direction),
                    metric_epoch: path.metrics.metric_epoch.to_string(),
                    metric_age_us: path.metrics.metric_age_us,
                    srtt_us: latency_observed.then_some(path.metrics.srtt_us),
                    rttvar_us: latency_observed.then_some(path.metrics.rttvar_us),
                    jitter_us: latency_observed.then_some(path.metrics.jitter_us),
                    latency_source: "peer_advisory",
                    delivery_rate_bps: Some(path.metrics.delivery_rate_bps.to_string()),
                    delivery_rate_observed: path.metrics.rate_observed,
                    delivery_rate_source: "peer_advisory",
                    delivery_rate_scope: "advisory",
                    pacing_rate_bps: path
                        .metrics
                        .pacing_rate_observed
                        .then(|| path.metrics.pacing_rate_bps.to_string()),
                    pacing_rate_source: path
                        .metrics
                        .pacing_rate_observed
                        .then_some("peer_advisory"),
                    loss_ppm: path.metrics.loss_observed.then_some(path.metrics.loss_ppm),
                    ecn_ppm: path.metrics.ecn_observed.then_some(path.metrics.ecn_ppm),
                    loss_observed: path.metrics.loss_observed,
                    ecn_observed: path.metrics.ecn_observed,
                    loss_source: path.metrics.loss_observed.then_some("peer_advisory"),
                    ecn_source: path.metrics.ecn_observed.then_some("peer_advisory"),
                    bytes_in_flight: path
                        .metrics
                        .bytes_in_flight_observed
                        .then(|| path.metrics.bytes_in_flight.to_string()),
                    queue_bytes: path
                        .metrics
                        .queue_observed
                        .then(|| path.metrics.queue_bytes.to_string()),
                    inflight_limit_bytes: (path.metrics.inflight_limit_bytes > 0)
                        .then(|| path.metrics.inflight_limit_bytes.to_string()),
                    confidence_ppm: Some(path.metrics.confidence_ppm),
                    app_limited: Some(path.metrics.app_limited),
                    ack_derived_data_observed: path.metrics.has_ack_derived_data_sample,
                    freshness_horizon_ms: path_metrics_freshness_horizon_ms(path.metrics),
                    metric_age_scope: "path_metrics",
                    data_sample_count: (path.metrics.data_sample_count > 0)
                        .then_some(path.metrics.data_sample_count),
                    data_sample_bytes: (path.metrics.data_sample_count > 0
                        || path.metrics.data_sample_bytes > 0)
                        .then(|| path.metrics.data_sample_bytes.to_string()),
                }
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
    path_pacing_rate_bps: Option<u64>,
}

impl NumericSummary {
    fn add_path(
        &mut self,
        snapshot: PathSnapshot,
        manual_disabled: bool,
        pacing_rate_bps: Option<u64>,
    ) {
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
        if let Some(pacing_rate_bps) = pacing_rate_bps {
            self.path_pacing_rate_bps = Some(
                self.path_pacing_rate_bps
                    .unwrap_or_default()
                    .saturating_add(pacing_rate_bps),
            );
        }
    }

    fn add_server_path(
        &mut self,
        state: PeerPathState,
        metrics: Option<crate::protocol::PathMetrics>,
        delivery_rate_bps: Option<u64>,
        pacing_rate_bps: Option<u64>,
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
            if metrics.queue_observed {
                self.queue_bytes = self.queue_bytes.saturating_add(metrics.queue_bytes);
            }
            if metrics.bytes_in_flight_observed {
                self.bytes_in_flight = self.bytes_in_flight.saturating_add(metrics.bytes_in_flight);
            }
        }
        self.path_delivery_rate_bps = self
            .path_delivery_rate_bps
            .saturating_add(delivery_rate_bps.unwrap_or(0));
        if let Some(pacing_rate_bps) = pacing_rate_bps {
            self.path_pacing_rate_bps = Some(
                self.path_pacing_rate_bps
                    .unwrap_or_default()
                    .saturating_add(pacing_rate_bps),
            );
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
            path_pacing_rate_bps: self.path_pacing_rate_bps.map(|rate| rate.to_string()),
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
    let authenticated_carriers = context.authenticated_carriers.snapshot();
    let state = match authenticated_carriers.availability() {
        AuthenticatedCarrierAvailability::AwaitingFirstCarrier => "connecting",
        AuthenticatedCarrierAvailability::Available => "connected",
        AuthenticatedCarrierAvailability::Offline => "offline",
    };
    sessions.insert(
        "mpp_outbound",
        service_index,
        service_name.clone(),
        session_id,
        state,
        authenticated_carriers.live_count,
        None,
    );
    summary.configured_path_count = summary.configured_path_count.saturating_add(
        context
            .configured_tcp_endpoint_count()
            .saturating_add(context.udp_paths.len()),
    );
    let health = context.health().lock().expect("client path health lock");
    paths.extend(client_tcp_path_set(
        context,
        &health,
        service_index,
        service_name.clone(),
        context.session_id.0,
        summary,
        now,
    ));
    paths.extend(client_path_set(
        context,
        &context.udp_path_names,
        &context.udp_paths,
        &health.udp,
        UnderlayProtocol::Udp,
        service_index,
        service_name,
        context.session_id.0,
        summary,
        now,
    ));
    drop(health);
}

#[allow(clippy::too_many_arguments)]
fn client_tcp_path_set(
    context: &ClientPathContext,
    health: &ClientPathHealth,
    service_index: usize,
    service_name: Option<String>,
    session_id: u64,
    summary: &mut NumericSummary,
    now: Instant,
) -> Vec<ManagementPathStatus> {
    health
        .tcp_records_with_indices()
        .filter_map(|(index, record)| {
            let spec = context.tcp_path_spec(index)?;
            let path = context.tcp_path_name(index)?.to_string();
            Some(client_path_status(
                path,
                spec,
                record,
                index,
                UnderlayProtocol::Tcp,
                service_index,
                service_name.clone(),
                session_id,
                summary,
                now,
                context,
            ))
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn client_path_set(
    context: &ClientPathContext,
    names: &[String],
    specs: &[PathSpec],
    records: &[ClientPathHealthRecord],
    underlay: UnderlayProtocol,
    service_index: usize,
    service_name: Option<String>,
    session_id: u64,
    summary: &mut NumericSummary,
    now: Instant,
) -> Vec<ManagementPathStatus> {
    assert_eq!(
        specs.len(),
        records.len(),
        "client path specifications and health records must align"
    );
    specs
        .iter()
        .zip(records)
        .enumerate()
        .map(|(index, (spec, record))| {
            let path = names
                .get(index)
                .expect("client path names align with underlay-local path inventory")
                .clone();
            client_path_status(
                path,
                spec,
                record,
                index,
                underlay,
                service_index,
                service_name.clone(),
                session_id,
                summary,
                now,
                context,
            )
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn client_path_status(
    path: String,
    spec: &PathSpec,
    record: &ClientPathHealthRecord,
    index: usize,
    underlay: UnderlayProtocol,
    service_index: usize,
    service_name: Option<String>,
    session_id: u64,
    summary: &mut NumericSummary,
    now: Instant,
    context: &ClientPathContext,
) -> ManagementPathStatus {
    let observation = record.observation_at(now);
    let rate_diagnostics = record.rate_diagnostics();
    let snapshot = path_snapshot(spec, index, observation);
    let tcp_endpoint = (underlay == UnderlayProtocol::Tcp)
        .then(|| context.tcp_endpoint_for_path(index))
        .flatten();
    let active_port_path_id = match underlay {
        UnderlayProtocol::Tcp => observation.wire_path_id,
        UnderlayProtocol::Udp => Some(snapshot.id),
    };
    let active_port = observed_client_active_port(context, underlay, active_port_path_id);
    let rate = client_rate_projection(spec, snapshot, observation, rate_diagnostics);
    let pacing = client_native_pacing_projection(rate, rate_diagnostics);
    let authoritative_pacing_rate_bps = client_authoritative_pacing_rate_bps(rate, pacing, now);
    let loss = observation
        .carrier_loss_rate
        .map(|loss| (loss, "native_carrier"))
        .or_else(|| {
            observation
                .measured_loss_rate
                .map(|loss| (loss, "mpp_datagram_feedback"))
        });
    summary.add_path(
        snapshot,
        observation.manual_disabled,
        authoritative_pacing_rate_bps,
    );
    ManagementPathStatus {
        service: "mpp_outbound",
        service_index,
        service_name,
        session_id: Some(session_id.to_string()),
        path,
        underlay: underlay_name(underlay),
        tcp_carrier_ordinal: (underlay == UnderlayProtocol::Tcp)
            .then(|| context.tcp_member_ordinal(index))
            .flatten()
            .map(|ordinal| ordinal.saturating_add(1)),
        max_tcp_carriers: tcp_endpoint.map(|endpoint| endpoint.range.max()),
        path_id: Some(snapshot.id.0.to_string()),
        path_instance_id: record
            .path_instance_id()
            .map(|instance| instance.as_u64().to_string()),
        endpoint: Some(path_endpoint(spec)),
        port_hopping: spec.port_hop_interval().is_some(),
        active_port,
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
        usage_direction: snapshot.peer_usage.map(|_| "client_to_server"),
        srtt_ms: Some(snapshot.srtt_ms),
        jitter_ms: observation
            .carrier_rttvar_ms
            .or(observation.measured_jitter_ms)
            .or_else(|| spec.metadata.initial_jitter_ms.map(f64::from)),
        latency_source: Some(client_latency_source(spec, observation)),
        delivery_rate_bps: Some((rate.delivery_rate_bps.round() as u64).to_string()),
        delivery_rate_observed: Some(rate.observed_at.is_some()),
        delivery_rate_source: Some(rate.source),
        delivery_rate_scope: Some(rate.scope),
        pacing_rate_bps: pacing.map(|(rate, _)| (rate.round() as u64).to_string()),
        pacing_rate_source: pacing.map(|_| "native_carrier"),
        loss_ppm: loss.map(|(loss, _)| fraction_to_ppm(loss)),
        ecn_ppm: None,
        loss_observed: Some(loss.is_some()),
        ecn_observed: Some(false),
        loss_source: loss.map(|(_, source)| source),
        ecn_source: None,
        queue_bytes: observation
            .carrier_queue_bytes_observed
            .then(|| snapshot.queue_bytes.to_string()),
        bytes_in_flight: observation
            .carrier_bytes_in_flight_observed
            .then(|| snapshot.bytes_in_flight.to_string()),
        data_level_bytes_in_flight: Some(snapshot.data_level_bytes_in_flight.to_string()),
        inflight_limit_bytes: (snapshot.carrier_inflight_limit_bytes > 0)
            .then(|| snapshot.carrier_inflight_limit_bytes.to_string()),
        confidence_ppm: Some(fraction_to_ppm(snapshot.confidence)),
        app_limited: rate.app_limited,
        active_flows: snapshot.active_flows,
        active_latency_sensitive_flows: snapshot.active_latency_sensitive_flows,
        delivery_samples: rate.samples,
        data_sample_bytes: rate.sample_bytes.map(|bytes| bytes.to_string()),
        last_delivery_age_ms: age_ms(now, rate.observed_at),
        pacing_age_ms: pacing.and_then(|(_, observed_at)| age_ms(now, Some(observed_at))),
        freshness_horizon_ms: rate.freshness_horizon_ms,
        metric_age_scope: rate.observed_at.map(|_| "delivery"),
        native_delivery_observed: Some(rate_diagnostics.carrier_delivery_samples > 0),
        product_delivery_observed: Some(rate_diagnostics.delivery_samples > 0),
        ack_derived_data_observed: Some(
            rate_diagnostics.delivery_samples > 0 || rate_diagnostics.carrier_ack_derived_data_seen,
        ),
    }
}

fn collect_server(
    context: &ServerPathContext,
    service_index: usize,
    summary: &mut NumericSummary,
    paths: &mut Vec<ManagementPathStatus>,
    sessions: &mut SessionInventory,
    now: Instant,
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
            max_tcp_carriers: None,
            path_id: None,
            path_instance_id: None,
            endpoint: Some(path_endpoint(spec)),
            port_hopping: false,
            active_port: None,
            state: "listening",
            manual_disabled: false,
            usage: None,
            policy: Some(spec.metadata.policy),
            source: Some("configured_listener"),
            direction: None,
            usage_direction: None,
            srtt_ms: None,
            jitter_ms: None,
            latency_source: None,
            delivery_rate_bps: None,
            delivery_rate_observed: None,
            delivery_rate_source: None,
            delivery_rate_scope: None,
            pacing_rate_bps: None,
            pacing_rate_source: None,
            loss_ppm: None,
            ecn_ppm: None,
            loss_observed: None,
            ecn_observed: None,
            loss_source: None,
            ecn_source: None,
            queue_bytes: None,
            bytes_in_flight: None,
            data_level_bytes_in_flight: None,
            inflight_limit_bytes: None,
            confidence_ppm: None,
            app_limited: None,
            active_flows: 0,
            active_latency_sensitive_flows: 0,
            delivery_samples: None,
            data_sample_bytes: None,
            last_delivery_age_ms: None,
            pacing_age_ms: None,
            freshness_horizon_ms: None,
            metric_age_scope: None,
            native_delivery_observed: None,
            product_delivery_observed: None,
            ack_derived_data_observed: None,
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
        let (summary_delivery_rate, summary_pacing_rate) =
            server_management_authoritative_rates(path, now);
        summary.add_server_path(
            path.state,
            path.metrics,
            summary_delivery_rate,
            summary_pacing_rate,
        );
        let metrics = path.metrics;
        let carrier_rate_sample = path.carrier_delivery_rate_sample;
        let metric_source = server_metric_source(path.source);
        let rate_source = if carrier_rate_sample.is_some() {
            Some("native_carrier")
        } else {
            server_rate_source(path.source, metrics)
        };
        let native_delivery_observed = carrier_rate_sample.is_some()
            || metrics.is_some_and(|metrics| {
                path.source == Some("local_sender") && path_metrics_has_rate_history(metrics)
            });
        let measured_at = metrics.is_some_and(path_metrics_has_rate_history)
            && (path.source == Some("peer_hint") || native_delivery_observed);
        let delivery_age_ms = carrier_rate_sample
            .map(|sample| age_ms(now, Some(sample.observed_at)).unwrap_or(0))
            .or_else(|| {
                metrics
                    .filter(|_| measured_at)
                    .map(|metrics| u64::from(metrics.metric_age_us) / 1_000)
            });
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
            max_tcp_carriers: None,
            path_id: Some(path.path_id.0.to_string()),
            path_instance_id: Some(path.path_instance_id.as_u64().to_string()),
            endpoint: None,
            port_hopping: false,
            active_port: None,
            state: peer_path_state_name(path.state),
            manual_disabled: false,
            usage: path.usage.map(path_usage_name),
            policy: Some(path.policy),
            source: path.source,
            direction: metrics.map(|metrics| metric_direction_name(metrics.direction)),
            usage_direction: path.usage.map(|_| "server_to_client"),
            srtt_ms: metrics.and_then(|metrics| {
                (metrics.srtt_us > 0).then(|| f64::from(metrics.srtt_us) / 1_000.0)
            }),
            jitter_ms: metrics.and_then(|metrics| {
                (metrics.srtt_us > 0).then(|| f64::from(metrics.jitter_us) / 1_000.0)
            }),
            latency_source: metrics.and(metric_source),
            delivery_rate_bps: carrier_rate_sample
                .map(|sample| sample.delivery_rate_bps.to_string())
                .or_else(|| metrics.map(|metrics| metrics.delivery_rate_bps.to_string())),
            delivery_rate_observed: metrics
                .map(|metrics| carrier_rate_sample.is_some() || metrics.rate_observed),
            delivery_rate_source: metrics.and(rate_source),
            delivery_rate_scope: metrics.map(|_| {
                if path.source == Some("peer_hint") {
                    "advisory"
                } else {
                    "path_capacity"
                }
            }),
            pacing_rate_bps: carrier_rate_sample
                .and_then(|sample| sample.pacing_rate_bps)
                .map(|rate| rate.to_string())
                .or_else(|| {
                    carrier_rate_sample
                        .is_none()
                        .then(|| {
                            metrics
                                .filter(|metrics| metrics.pacing_rate_observed)
                                .map(|metrics| metrics.pacing_rate_bps.to_string())
                        })
                        .flatten()
                }),
            pacing_rate_source: if let Some(sample) = carrier_rate_sample {
                sample.pacing_rate_bps.map(|_| "native_carrier")
            } else {
                metrics
                    .filter(|metrics| metrics.pacing_rate_observed)
                    .and(rate_source)
            },
            loss_ppm: metrics
                .filter(|metrics| metrics.loss_observed)
                .map(|metrics| metrics.loss_ppm),
            ecn_ppm: metrics
                .filter(|metrics| metrics.ecn_observed)
                .map(|metrics| metrics.ecn_ppm),
            loss_observed: metrics.map(|metrics| metrics.loss_observed),
            ecn_observed: metrics.map(|metrics| metrics.ecn_observed),
            loss_source: metrics
                .filter(|metrics| metrics.loss_observed)
                .and(metric_source),
            ecn_source: metrics
                .filter(|metrics| metrics.ecn_observed)
                .and(metric_source),
            queue_bytes: metrics
                .filter(|metrics| metrics.queue_observed)
                .map(|metrics| metrics.queue_bytes.to_string()),
            bytes_in_flight: metrics
                .filter(|metrics| metrics.bytes_in_flight_observed)
                .map(|metrics| metrics.bytes_in_flight.to_string()),
            data_level_bytes_in_flight: None,
            inflight_limit_bytes: metrics.and_then(|metrics| {
                (metrics.inflight_limit_bytes > 0).then(|| metrics.inflight_limit_bytes.to_string())
            }),
            confidence_ppm: metrics.map(|metrics| metrics.confidence_ppm),
            app_limited: metrics
                .filter(|_| native_delivery_observed || path.source == Some("peer_hint"))
                .map(|metrics| metrics.app_limited),
            active_flows: 0,
            active_latency_sensitive_flows: 0,
            delivery_samples: carrier_rate_sample
                .map(|sample| sample.sample_count)
                .or_else(|| {
                    metrics
                        .filter(|metrics| metrics.data_sample_count > 0)
                        .map(|metrics| metrics.data_sample_count)
                }),
            data_sample_bytes: carrier_rate_sample
                .map(|sample| sample.sample_bytes.to_string())
                .or_else(|| {
                    metrics
                        .filter(|metrics| {
                            metrics.data_sample_count > 0 || metrics.data_sample_bytes > 0
                        })
                        .map(|metrics| metrics.data_sample_bytes.to_string())
                }),
            last_delivery_age_ms: delivery_age_ms,
            pacing_age_ms: carrier_rate_sample.map_or_else(
                || {
                    metrics
                        .filter(|metrics| measured_at && metrics.pacing_rate_observed)
                        .map(|metrics| u64::from(metrics.metric_age_us) / 1_000)
                },
                |sample| sample.pacing_rate_bps.and(delivery_age_ms),
            ),
            freshness_horizon_ms: carrier_rate_sample
                .map(|sample| {
                    sample
                        .expires_at
                        .saturating_duration_since(sample.observed_at)
                        .as_millis()
                        .min(u64::MAX as u128) as u64
                })
                .or_else(|| {
                    metrics
                        .filter(|_| measured_at)
                        .and_then(path_metrics_freshness_horizon_ms)
                }),
            metric_age_scope: carrier_rate_sample
                .map(|_| "delivery")
                .or_else(|| measured_at.then_some("path_metrics")),
            native_delivery_observed: metrics.map(|_| native_delivery_observed),
            product_delivery_observed: None,
            ack_derived_data_observed: metrics.map(|metrics| metrics.has_ack_derived_data_sample),
        });
    }
}

pub(super) fn flow_status(
    flow: ActiveProductFlowSnapshot,
    now: Instant,
) -> Option<ManagementFlowStatus> {
    let origin = flow.origin.as_ref()?;
    let flow_kind = match flow.flow_id {
        ProductFlowId::Reliable(_) | ProductFlowId::NativeReliable => "reliable",
        ProductFlowId::Datagram(_) | ProductFlowId::NativeDatagram => "datagram",
    };
    let inbound_kind = match origin.kind {
        ProductFlowOriginKind::LocalInbound => "local",
        ProductFlowOriginKind::MppInbound => "mpp",
    };
    let source_kind = match origin.source.kind {
        ProductFlowSourceKind::LocalPeer => "local_peer",
        ProductFlowSourceKind::MppCarrierPeer => "mpp_carrier_peer",
    };
    let selection = flow.selection.as_ref();
    Some(ManagementFlowStatus {
        session_id: flow.session_id.map(|id| id.0.to_string()),
        flow_kind,
        flow_id: flow.display_id.to_string(),
        network: network_name(flow.network),
        inbound_kind,
        inbound: origin.inbound.as_str().to_string(),
        source_kind,
        source: origin.source.endpoint.to_string(),
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
    })
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

#[derive(Debug, Clone, Copy)]
struct ClientRateProjection {
    delivery_rate_bps: f64,
    source: &'static str,
    scope: &'static str,
    observed_at: Option<Instant>,
    expires_at: Option<Instant>,
    freshness_horizon_ms: Option<u64>,
    samples: Option<u32>,
    sample_bytes: Option<u64>,
    app_limited: Option<bool>,
}

fn client_rate_projection(
    spec: &PathSpec,
    snapshot: PathSnapshot,
    observation: ClientPathObservation,
    diagnostics: ClientPathRateDiagnostics,
) -> ClientRateProjection {
    if observation.explicit_carrier_capacity_proof
        && let Some(delivery_rate_bps) = observation.carrier_delivery_rate_bps
    {
        return ClientRateProjection {
            delivery_rate_bps,
            source: "capacity_proof",
            scope: "path_capacity",
            observed_at: observation.carrier_last_delivery_at,
            expires_at: observation.carrier_bulk_proof_expires_at,
            freshness_horizon_ms: client_rate_freshness_horizon_ms(
                observation.carrier_last_delivery_at,
                observation.carrier_bulk_proof_expires_at,
            ),
            samples: Some(observation.carrier_delivery_samples.max(1)),
            sample_bytes: Some(observation.carrier_delivery_sample_bytes),
            app_limited: Some(false),
        };
    }

    let carrier = diagnostics
        .carrier_delivery_rate_bps
        .filter(|_| diagnostics.carrier_delivery_samples > 0)
        .map(|delivery_rate_bps| ClientRateProjection {
            delivery_rate_bps,
            source: "native_carrier",
            scope: "path_capacity",
            observed_at: diagnostics.carrier_last_delivery_at,
            expires_at: diagnostics.carrier_bulk_proof_expires_at,
            freshness_horizon_ms: client_rate_freshness_horizon_ms(
                diagnostics.carrier_last_delivery_at,
                diagnostics.carrier_bulk_proof_expires_at,
            ),
            samples: Some(diagnostics.carrier_delivery_samples),
            sample_bytes: Some(diagnostics.carrier_delivery_sample_bytes),
            app_limited: Some(diagnostics.carrier_app_limited),
        });
    let product = diagnostics
        .product_delivery_rate_bps
        .filter(|_| diagnostics.delivery_samples > 0)
        .map(|delivery_rate_bps| ClientRateProjection {
            delivery_rate_bps,
            source: "product_goodput",
            scope: "per_flow_goodput",
            observed_at: diagnostics.last_delivery_at,
            expires_at: diagnostics.delivery_rate_expires_at,
            freshness_horizon_ms: client_rate_freshness_horizon_ms(
                diagnostics.last_delivery_at,
                diagnostics.delivery_rate_expires_at,
            ),
            samples: Some(diagnostics.delivery_samples),
            sample_bytes: Some(diagnostics.product_delivery_sample_bytes),
            app_limited: None,
        });
    let mpp = diagnostics
        .measured_rate_bps
        .map(|delivery_rate_bps| ClientRateProjection {
            delivery_rate_bps,
            source: if diagnostics.datagram_feedback_samples > 0 {
                "mpp_datagram_feedback"
            } else {
                "mpp_delivery"
            },
            scope: "path_capacity",
            observed_at: diagnostics.last_delivery_at,
            expires_at: diagnostics.delivery_rate_expires_at,
            freshness_horizon_ms: client_rate_freshness_horizon_ms(
                diagnostics.last_delivery_at,
                diagnostics.delivery_rate_expires_at,
            ),
            samples: (diagnostics.delivery_samples > 0).then_some(diagnostics.delivery_samples),
            sample_bytes: (diagnostics.product_delivery_sample_bytes > 0)
                .then_some(diagnostics.product_delivery_sample_bytes),
            app_limited: None,
        });
    let product_side = product.or(mpp);
    match (carrier, product_side) {
        (Some(carrier), Some(product)) => {
            if product.observed_at > carrier.observed_at {
                product
            } else {
                carrier
            }
        }
        (Some(carrier), None) => carrier,
        (None, Some(product)) => product,
        (None, None) => ClientRateProjection {
            delivery_rate_bps: snapshot.delivery_rate_bps,
            source: match spec.metadata.initial_rate {
                RateHint::Unknown => "scheduler_default",
                RateHint::Unlimited | RateHint::BitsPerSecond(_) => "configured_prior",
            },
            scope: rate_scope_name(snapshot.rate_scope),
            observed_at: None,
            expires_at: None,
            freshness_horizon_ms: None,
            samples: None,
            sample_bytes: None,
            app_limited: None,
        },
    }
}

fn client_native_pacing_projection(
    rate: ClientRateProjection,
    diagnostics: ClientPathRateDiagnostics,
) -> Option<(f64, Instant)> {
    let observed_at = diagnostics.carrier_last_delivery_at?;
    let expires_at = diagnostics.carrier_bulk_proof_expires_at?;
    if rate.source != "native_carrier"
        || rate.observed_at != Some(observed_at)
        || rate.expires_at != Some(expires_at)
    {
        return None;
    }
    diagnostics
        .carrier_pacing_rate_bps
        .map(|pacing_rate_bps| (pacing_rate_bps, observed_at))
}

fn client_authoritative_pacing_rate_bps(
    rate: ClientRateProjection,
    pacing: Option<(f64, Instant)>,
    now: Instant,
) -> Option<u64> {
    pacing
        .filter(|_| {
            rate.observed_at
                .zip(rate.expires_at)
                .is_some_and(|(observed_at, expires_at)| observed_at <= now && now < expires_at)
        })
        .map(|(rate, _)| rate.round() as u64)
}

fn client_rate_freshness_horizon_ms(
    observed_at: Option<Instant>,
    proof_expires_at: Option<Instant>,
) -> Option<u64> {
    let observed_at = observed_at?;
    Some(
        proof_expires_at?
            .saturating_duration_since(observed_at)
            .as_millis()
            .min(u64::MAX as u128) as u64,
    )
}

fn client_latency_source(spec: &PathSpec, observation: ClientPathObservation) -> &'static str {
    if observation.carrier_srtt_ms.is_some() || observation.carrier_rttvar_ms.is_some() {
        "native_carrier"
    } else if observation.measured_srtt_ms.is_some() || observation.measured_jitter_ms.is_some() {
        "mpp_feedback"
    } else if spec.metadata.initial_srtt_ms.is_some() || spec.metadata.initial_jitter_ms.is_some() {
        "configured_prior"
    } else {
        "scheduler_default"
    }
}

fn observed_client_active_port(
    context: &ClientPathContext,
    underlay: UnderlayProtocol,
    path_id: Option<crate::protocol::PathId>,
) -> Option<u16> {
    // The exact authenticated peer-status assignment is the common authority
    // for both tables. TCP make-before-break may keep two live PathIds in one
    // configured slot, while Quinn retains its canonical peer locator as the
    // mapped UDP socket migrates. Joining by the health record's real wire
    // PathId keeps both cases generation-coherent; an unconnected TCP slot's
    // synthetic display ID is deliberately not eligible.
    let path_id = path_id?;
    context
        .peer_status
        .live_path_active_port(context.session_id, underlay, path_id)
}

const fn rate_scope_name(scope: PathRateScope) -> &'static str {
    match scope {
        PathRateScope::PerFlowGoodput => "per_flow_goodput",
        PathRateScope::PathCapacity => "path_capacity",
    }
}

const fn opposite_metric_direction_name(direction: PathMetricDirection) -> &'static str {
    match direction {
        PathMetricDirection::ClientToServer => "server_to_client",
        PathMetricDirection::ServerToClient => "client_to_server",
    }
}

fn path_metrics_freshness_horizon_ms(metrics: crate::protocol::PathMetrics) -> Option<u64> {
    path_metrics_has_rate_history(metrics).then(|| {
        let lifetime_us =
            u128::from(metrics.metric_age_us).saturating_add(u128::from(metrics.rate_valid_for_us));
        lifetime_us
            .saturating_add(999)
            .checked_div(1_000)
            .unwrap_or_default()
            .min(u128::from(u64::MAX)) as u64
    })
}

fn server_management_authoritative_rates(
    path: ServerCarrierPathStatusSnapshot,
    now: Instant,
) -> (Option<u64>, Option<u64>) {
    if path.source == Some("startup") {
        return path.metrics.map_or((None, None), |metrics| {
            (
                Some(metrics.delivery_rate_bps),
                metrics
                    .pacing_rate_observed
                    .then_some(metrics.pacing_rate_bps),
            )
        });
    }
    if let Some(sample) = path.carrier_delivery_rate_sample {
        // A retained expired native sample remains useful in its row as a
        // stale diagnostic, but cannot inflate the live aggregate.
        return if sample.observed_at <= now && now < sample.expires_at {
            (Some(sample.delivery_rate_bps), sample.pacing_rate_bps)
        } else {
            (None, None)
        };
    }
    let Some(metrics) = path.metrics else {
        return (None, None);
    };
    let source_has_rate_evidence = matches!(path.source, Some("peer_hint" | "local_sender"))
        && path_metrics_has_rate_history(metrics);
    if source_has_rate_evidence && metrics.rate_valid_for_us > 0 {
        (
            Some(metrics.delivery_rate_bps),
            metrics
                .pacing_rate_observed
                .then_some(metrics.pacing_rate_bps),
        )
    } else {
        (None, None)
    }
}

#[cfg(test)]
pub(super) fn client_metric_freshness_horizon_ms(observation: ClientPathObservation) -> u64 {
    let srtt_ms = observation
        .carrier_srtt_ms
        .or(observation.measured_srtt_ms)
        .unwrap_or(RELIABLE_INITIAL_RTT.as_secs_f64() * 1_000.0)
        .max(0.001);
    let rttvar_ms = observation
        .carrier_rttvar_ms
        .or(observation.measured_jitter_ms)
        .unwrap_or_default()
        .max(0.0)
        .max(srtt_ms / 8.0);
    metric_freshness_horizon_from_ms(srtt_ms, rttvar_ms)
        .expect("positive client SRTT has a freshness horizon")
}

#[cfg(test)]
fn metric_freshness_horizon_from_ms(srtt_ms: f64, rttvar_ms: f64) -> Option<u64> {
    (srtt_ms.is_finite() && srtt_ms > 0.0 && rttvar_ms.is_finite() && rttvar_ms >= 0.0).then(|| {
        transport_rate_sample_freshness_horizon(
            Duration::from_secs_f64(srtt_ms / 1_000.0),
            Duration::from_secs_f64(rttvar_ms / 1_000.0),
        )
        .as_millis()
        .min(u64::MAX as u128) as u64
    })
}

fn server_metric_source(source: Option<&'static str>) -> Option<&'static str> {
    source.map(|source| match source {
        "startup" => "startup_prior",
        "peer_hint" => "peer_advisory",
        "local_sender" => "local_sender",
        source => source,
    })
}

fn server_rate_source(
    source: Option<&'static str>,
    metrics: Option<crate::protocol::PathMetrics>,
) -> Option<&'static str> {
    source.map(|source| match source {
        "startup" => "startup_prior",
        "peer_hint" => "peer_advisory",
        "local_sender" if metrics.is_some_and(path_metrics_has_rate_history) => "native_carrier",
        // TCP PathMetrics intentionally cannot classify native ACKs as
        // authenticated Product data, and startup priors currently share this
        // registry source. Preserve the exact owner provenance instead of
        // guessing either "native" or "startup" from an absent data flag.
        "local_sender" => "local_sender",
        source => source,
    })
}

fn path_metrics_has_rate_history(metrics: crate::protocol::PathMetrics) -> bool {
    metrics.rate_observed
}

fn age_ms(now: Instant, instant: Option<Instant>) -> Option<u64> {
    instant.map(|instant| {
        now.saturating_duration_since(instant)
            .as_millis()
            .min(u64::MAX as u128) as u64
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::path::{CarrierPathInstanceId, PathPolicy};
    use crate::protocol::{PathId, PathMetrics, PathUsage, SessionId};

    fn local_sender_status(metrics: PathMetrics) -> ServerCarrierPathStatusSnapshot {
        ServerCarrierPathStatusSnapshot {
            session_id: SessionId(1),
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(2),
            path_instance_id: CarrierPathInstanceId::from_raw(3),
            configured_index: 0,
            policy: PathPolicy::default(),
            state: PeerPathState::Active,
            usage: Some(PathUsage::Available),
            metrics: Some(metrics),
            carrier_delivery_rate_sample: None,
            source: Some("local_sender"),
        }
    }

    fn ack_seen_metrics() -> PathMetrics {
        PathMetrics {
            path_id: PathId(2),
            underlay: UnderlayProtocol::Udp,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: 1,
            metric_age_us: 0,
            rate_valid_for_us: 0,
            rate_observed: false,
            srtt_us: 20_000,
            rttvar_us: 2_000,
            jitter_us: 2_000,
            delivery_rate_bps: 800_000_000,
            pacing_rate_bps: 900_000_000,
            pacing_rate_observed: false,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight_observed: true,
            queue_observed: true,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: 512 * 1024,
            inflight_hi_bytes: 512 * 1024,
            confidence_ppm: 0,
            app_limited: true,
            has_ack_derived_data_sample: true,
            data_sample_count: 0,
            data_sample_bytes: 0,
        }
    }

    #[test]
    fn ack_reachability_without_sample_volume_is_not_native_rate_provenance_or_aggregate() {
        let now = Instant::now();
        let ack_seen = ack_seen_metrics();
        assert!(ack_seen.has_ack_derived_data_sample);
        assert!(!path_metrics_has_rate_history(ack_seen));
        assert_eq!(
            server_management_authoritative_rates(local_sender_status(ack_seen), now),
            (None, None)
        );
        assert_eq!(
            server_rate_source(Some("local_sender"), Some(ack_seen)),
            Some("local_sender")
        );

        let sampled = PathMetrics {
            rate_valid_for_us: 1,
            rate_observed: true,
            data_sample_count: 1,
            data_sample_bytes: 64 * 1024,
            ..ack_seen
        };
        assert_eq!(
            server_management_authoritative_rates(local_sender_status(sampled), now),
            (Some(800_000_000), None)
        );
        assert_eq!(
            server_rate_source(Some("local_sender"), Some(sampled)),
            Some("native_carrier")
        );

        let observed_pacing = PathMetrics {
            pacing_rate_observed: true,
            ..sampled
        };
        assert_eq!(
            server_management_authoritative_rates(local_sender_status(observed_pacing), now),
            (Some(800_000_000), Some(900_000_000))
        );

        let expired = PathMetrics {
            rate_valid_for_us: 0,
            ..observed_pacing
        };
        assert_eq!(
            server_management_authoritative_rates(local_sender_status(expired), now),
            (None, None)
        );
        assert_eq!(path_metrics_freshness_horizon_ms(expired), Some(0));

        let aged = PathMetrics {
            metric_age_us: 1_500,
            rate_valid_for_us: 2_001,
            ..observed_pacing
        };
        assert_eq!(path_metrics_freshness_horizon_ms(aged), Some(4));
    }

    #[test]
    fn client_aggregate_pacing_requires_a_fresh_same_epoch_native_observation() {
        let observed_at = Instant::now();
        let expires_at = observed_at + Duration::from_millis(100);
        let rate = ClientRateProjection {
            delivery_rate_bps: 100_000_000.0,
            source: "native_carrier",
            scope: "path_capacity",
            observed_at: Some(observed_at),
            expires_at: Some(expires_at),
            freshness_horizon_ms: Some(100),
            samples: Some(1),
            sample_bytes: Some(64 * 1024),
            app_limited: Some(false),
        };
        let pacing = Some((123_000_000.0, observed_at));

        assert_eq!(
            client_authoritative_pacing_rate_bps(
                rate,
                pacing,
                expires_at - Duration::from_nanos(1)
            ),
            Some(123_000_000)
        );
        assert_eq!(
            client_authoritative_pacing_rate_bps(rate, pacing, expires_at),
            None
        );
        assert_eq!(
            client_authoritative_pacing_rate_bps(rate, None, observed_at),
            None
        );

        let mut summary = NumericSummary::default();
        summary.add_path(
            PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 10.0, 50_000_000.0),
            false,
            None,
        );
        assert_eq!(summary.finish().path_pacing_rate_bps, None);

        let mut observed = NumericSummary::default();
        observed.add_path(
            PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 10.0, 50_000_000.0),
            false,
            Some(123_000_000),
        );
        observed.add_path(
            PathSnapshot::new(PathId(2), UnderlayProtocol::Tcp, 10.0, 50_000_000.0),
            false,
            None,
        );
        assert_eq!(
            observed.finish().path_pacing_rate_bps.as_deref(),
            Some("123000000")
        );
    }
}
