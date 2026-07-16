//! Runtime-to-management-schema projection.
//!
//! Projection reads endpoint-local owners and produces one detached value
//! graph. The sampler publishes that graph, so HTTP never traverses runtime
//! locks or mutable carrier state.

use super::ManagementTarget;
use super::schema::{
    ManagementControlStatus, ManagementControls, ManagementDiagnostics, ManagementFlowStatus,
    ManagementIngressStatus, ManagementIo, ManagementPathStatus, ManagementPeerPathStatus,
    ManagementPeerSession, ManagementPeerStatusResult, ManagementServices, ManagementSnapshot,
    ManagementSummary, NumericIo, SCHEMA, metric_direction_name, path_state_name, path_usage_name,
    peer_path_state_name, peer_status_code_name, underlay_name,
};
use super::snapshot::{SessionInventory, TelemetryAggregate, unix_millis};
use crate::ingress::IngressConfig;
use crate::protocol::{PeerPathState, TargetAddr, UnderlayProtocol};
use crate::runtime::path::model::path_snapshot;
use crate::runtime::path::{ClientPathContext, ClientPathHealthRecord, ServerPathContext};
use crate::runtime::peer_status::{PeerStatusBroker, PeerStatusResult};
use crate::runtime::telemetry::{ActiveProductFlowSnapshot, ProductFlowId};
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
    let mut local_inbounds = Vec::new();
    let mut telemetry = TelemetryAggregate::default();
    let mut session_inventory = SessionInventory::default();
    let peer_diagnostics_allowed;
    let mut peer_sessions = Vec::new();
    let mut peer_results = Vec::new();

    match target {
        ManagementTarget::Client { context, .. } => {
            services.mpp_outbounds = 1;
            services.local_inbounds = context.ingresses.len();
            collect_client(
                context,
                0,
                &mut summary,
                &mut paths,
                &mut telemetry,
                &mut session_inventory,
                &mut local_inbounds,
                now,
            );
            peer_diagnostics_allowed = context.peer_status.allows_incoming();
            let tag = context
                .route_target
                .as_ref()
                .map(|target| target.tag.clone());
            peer_sessions.extend(service_peer_sessions(
                &context.peer_status,
                "mpp_outbound",
                0,
                tag.clone(),
            ));
            peer_results.extend(latest_peer_results(
                &context.peer_status,
                "mpp_outbound",
                0,
                tag,
            ));
        }
        ManagementTarget::Server { context, .. } => {
            services.mpp_inbounds = 1;
            services.configured_path_listeners = context.server_paths.len();
            collect_server(
                context,
                0,
                &mut summary,
                &mut paths,
                &mut telemetry,
                &mut session_inventory,
                now,
            );
            peer_diagnostics_allowed = context.peer_status.allows_incoming();
            peer_sessions.extend(service_peer_sessions(
                &context.peer_status,
                "mpp_inbound",
                0,
                context.tag.clone(),
            ));
            peer_results.extend(latest_peer_results(
                &context.peer_status,
                "mpp_inbound",
                0,
                context.tag.clone(),
            ));
        }
        ManagementTarget::Node {
            clients, servers, ..
        } => {
            services.mpp_outbounds = clients.len();
            services.mpp_inbounds = servers.len();
            services.local_inbounds = clients.iter().map(|context| context.ingresses.len()).sum();
            services.configured_path_listeners = servers
                .iter()
                .map(|context| context.server_paths.len())
                .sum();
            for (index, context) in clients.iter().enumerate() {
                collect_client(
                    context,
                    index,
                    &mut summary,
                    &mut paths,
                    &mut telemetry,
                    &mut session_inventory,
                    &mut local_inbounds,
                    now,
                );
                let tag = context
                    .route_target
                    .as_ref()
                    .map(|target| target.tag.clone());
                peer_sessions.extend(service_peer_sessions(
                    &context.peer_status,
                    "mpp_outbound",
                    index,
                    tag.clone(),
                ));
                peer_results.extend(latest_peer_results(
                    &context.peer_status,
                    "mpp_outbound",
                    index,
                    tag,
                ));
            }
            for (index, context) in servers.iter().enumerate() {
                collect_server(
                    context,
                    index,
                    &mut summary,
                    &mut paths,
                    &mut telemetry,
                    &mut session_inventory,
                    now,
                );
                peer_sessions.extend(service_peer_sessions(
                    &context.peer_status,
                    "mpp_inbound",
                    index,
                    context.tag.clone(),
                ));
                peer_results.extend(latest_peer_results(
                    &context.peer_status,
                    "mpp_inbound",
                    index,
                    context.tag.clone(),
                ));
            }
            peer_diagnostics_allowed = clients
                .iter()
                .any(|context| context.peer_status.allows_incoming())
                || servers
                    .iter()
                    .any(|context| context.peer_status.allows_incoming());
        }
    }
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
            endpoint: (services.mpp_outbounds > 0).then_some("POST /api/control/path"),
            reason: (services.mpp_outbounds == 0)
                .then_some("path control requires a local inbound service with an MPP outbound"),
        },
        peer_diagnostics: ManagementControlStatus {
            supported: !peer_sessions.is_empty(),
            endpoint: (!peer_sessions.is_empty()).then_some("POST /api/diagnostics/peer"),
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
        left.service
            .cmp(right.service)
            .then_with(|| left.service_index.cmp(&right.service_index))
            .then_with(|| left.session_id.cmp(&right.session_id))
            .then_with(|| left.flow_id.cmp(&right.flow_id))
    });
    let sessions = session_inventory.finish(&flows, telemetry.active_flow_overflow == 0);

    ManagementSnapshot {
        schema: SCHEMA,
        generated_unix_ms: unix_millis(),
        role,
        started_unix_ms,
        uptime_ms: uptime.as_millis().min(u64::MAX as u128) as u64,
        services,
        local_inbounds,
        summary,
        traffic,
        paths,
        sessions,
        flows,
        diagnostics,
        controls,
    }
}

fn service_peer_sessions(
    broker: &PeerStatusBroker,
    service: &'static str,
    service_index: usize,
    service_tag: Option<String>,
) -> Vec<ManagementPeerSession> {
    broker
        .session_ids()
        .into_iter()
        .map(|session_id| ManagementPeerSession {
            service,
            service_index,
            service_tag: service_tag.clone(),
            session_id: session_id.0.to_string(),
            carrier_count: broker.carrier_count(session_id),
        })
        .collect()
}

fn latest_peer_results(
    broker: &PeerStatusBroker,
    service: &'static str,
    service_index: usize,
    service_tag: Option<String>,
) -> Vec<ManagementPeerStatusResult> {
    broker
        .session_ids()
        .into_iter()
        .filter_map(|session_id| broker.latest(session_id))
        .map(|result| peer_status_result(result, service, service_index, service_tag.clone()))
        .collect()
}

pub(super) fn peer_status_result(
    result: PeerStatusResult,
    service: &'static str,
    service_index: usize,
    service_tag: Option<String>,
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
        service_tag,
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
    telemetry: &mut TelemetryAggregate,
    sessions: &mut SessionInventory,
    local_inbounds: &mut Vec<ManagementIngressStatus>,
    now: Instant,
) {
    let tag = context
        .route_target
        .as_ref()
        .map(|target| target.tag.clone());
    let session_id = context.session_id.0.to_string();
    let carrier_count = context.peer_status.carrier_count(context.session_id);
    sessions.insert(
        "mpp_outbound",
        service_index,
        tag.clone(),
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
            .tcp_paths
            .len()
            .saturating_add(context.udp_paths.len()),
    );
    let health = context.health().lock().expect("client path health lock");
    local_inbounds.extend(context.ingresses.iter().map(|ingress| {
        let (protocol, listen, name, auth_required) = match &ingress.config {
            IngressConfig::Socks5 { listen, proxy_auth } => (
                "socks5",
                listen.iter().map(ToString::to_string).collect(),
                None,
                proxy_auth.is_required(),
            ),
            IngressConfig::HttpConnect { listen, proxy_auth } => (
                "http",
                listen.iter().map(ToString::to_string).collect(),
                None,
                proxy_auth.is_required(),
            ),
            IngressConfig::TunL4(tun) => ("tun", Vec::new(), tun.name.clone(), false),
        };
        ManagementIngressStatus {
            service_index,
            tag: ingress.tag.clone(),
            protocol,
            listen,
            name,
            auth_required,
        }
    }));
    paths.extend(client_path_set(
        &context.tcp_paths,
        &health.tcp,
        UnderlayProtocol::Tcp,
        service_index,
        tag.clone(),
        context.session_id.0,
        summary,
        now,
    ));
    paths.extend(client_path_set(
        &context.udp_paths,
        &health.udp,
        UnderlayProtocol::Udp,
        service_index,
        tag.clone(),
        context.session_id.0,
        summary,
        now,
    ));
    drop(health);
    telemetry.add(
        "mpp_outbound",
        service_index,
        tag,
        context.telemetry_snapshot(),
        now,
    );
}

#[allow(clippy::too_many_arguments)]
fn client_path_set(
    specs: &[PathSpec],
    records: &[ClientPathHealthRecord],
    underlay: UnderlayProtocol,
    service_index: usize,
    service_tag: Option<String>,
    session_id: u64,
    summary: &mut NumericSummary,
    now: Instant,
) -> Vec<ManagementPathStatus> {
    specs
        .iter()
        .enumerate()
        .map(|(index, spec)| {
            let observation = records
                .get(index)
                .map(|record| record.observation_at(now))
                .unwrap_or_default();
            let snapshot = path_snapshot(spec, index, observation);
            summary.add_path(snapshot, observation.manual_disabled);
            ManagementPathStatus {
                id: format!(
                    "outbound:{service_index}:{}:{index}",
                    underlay_name(underlay)
                ),
                service: "mpp_outbound",
                service_index,
                service_tag: service_tag.clone(),
                session_id: Some(session_id.to_string()),
                underlay: underlay_name(underlay),
                path_id: Some(snapshot.id.0.to_string()),
                path_instance_id: None,
                configured_index: Some(index),
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
            }
        })
        .collect()
}

fn collect_server(
    context: &ServerPathContext,
    service_index: usize,
    summary: &mut NumericSummary,
    paths: &mut Vec<ManagementPathStatus>,
    telemetry: &mut TelemetryAggregate,
    sessions: &mut SessionInventory,
    now: Instant,
) {
    summary.configured_path_count = summary
        .configured_path_count
        .saturating_add(context.server_paths.len());
    let tag = context.tag.clone();
    for (index, spec) in context.server_paths.iter().enumerate() {
        paths.push(ManagementPathStatus {
            id: format!("inbound:{service_index}:listener:{index}"),
            service: "mpp_inbound",
            service_index,
            service_tag: tag.clone(),
            session_id: None,
            underlay: underlay_name(spec.underlay),
            path_id: None,
            path_instance_id: None,
            configured_index: Some(index),
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
            tag.clone(),
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
        paths.push(ManagementPathStatus {
            id: format!(
                "inbound:{service_index}:session:{}:{}:{}:{}",
                path.session_id.0,
                underlay_name(path.underlay),
                path.path_id.0,
                path.path_instance_id.as_u64(),
            ),
            service: "mpp_inbound",
            service_index,
            service_tag: tag.clone(),
            session_id: Some(path.session_id.0.to_string()),
            underlay: underlay_name(path.underlay),
            path_id: Some(path.path_id.0.to_string()),
            path_instance_id: Some(path.path_instance_id.as_u64().to_string()),
            configured_index: Some(path.configured_index),
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
    telemetry.add(
        "mpp_inbound",
        service_index,
        tag,
        context.telemetry_snapshot(),
        now,
    );
}

pub(super) fn flow_status(
    service: &'static str,
    service_index: usize,
    service_tag: Option<String>,
    flow: ActiveProductFlowSnapshot,
    now: Instant,
) -> ManagementFlowStatus {
    let (flow_kind, flow_id) = match flow.flow_id {
        ProductFlowId::Reliable(id) => ("reliable", id.0.to_string()),
        ProductFlowId::Datagram(id) => ("datagram", id.0.to_string()),
    };
    ManagementFlowStatus {
        service,
        service_index,
        service_tag,
        session_id: flow.session_id.map(|id| id.0.to_string()),
        flow_kind,
        flow_id,
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
