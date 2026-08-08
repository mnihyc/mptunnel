//! Transport-neutral server packet ownership and dispatch.

use super::flow::PacketFlowTable;
use super::{IpPacketQueueBudget, IpPacketQueuePermit};
use crate::model::path::CarrierPathInstanceId;
use crate::model::tun_l3::{IpPacketFlowKey, parse_ip_packet};
use crate::product::{PrincipalId, TunL3AddressPlan, TunL3PeerAllocation};
use crate::protocol::{
    CloseReason, IpPacketId, IpTunnelId, PathId, PathMetricDirection, PeerPathState, SessionId,
    UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::{ServerCarrierPathRegistration, ServerStreamPort};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum IpTunnelPacketSendOutcome {
    Accepted,
    Full,
    Retired,
}

pub(in crate::runtime) trait ServerIpTunnelCarrier:
    std::fmt::Debug + Send + Sync
{
    fn try_send_packet(
        &self,
        tunnel_id: IpTunnelId,
        packet_id: IpPacketId,
        payload: Bytes,
        budget: &IpPacketQueueBudget,
    ) -> Result<IpTunnelPacketSendOutcome, RuntimeError>;

    fn close(&self, tunnel_id: IpTunnelId, reason: CloseReason);
}

pub(in crate::runtime) struct ServerIpTunnelOpenRequest<'a> {
    pub(in crate::runtime) tunnel_id: IpTunnelId,
    pub(in crate::runtime) path: &'a ServerCarrierPathRegistration,
    pub(in crate::runtime) carrier: Arc<dyn ServerIpTunnelCarrier>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ServerIpCarrierKey {
    session_id: SessionId,
    underlay: UnderlayProtocol,
    path_id: PathId,
    path_instance_id: CarrierPathInstanceId,
}

#[derive(Debug)]
struct ServerIpAttachment {
    key: ServerIpCarrierKey,
    config_ordinal: usize,
    backup: bool,
    startup_metrics: Option<crate::protocol::PathMetrics>,
    attachment_generation: u64,
    lifetime: Weak<()>,
    carrier: Arc<dyn ServerIpTunnelCarrier>,
}

#[derive(Debug)]
struct ServerLogicalIpTunnel {
    session_id: SessionId,
    tunnel_id: IpTunnelId,
    generation: u64,
    attachments: HashMap<ServerIpCarrierKey, ServerIpAttachment>,
    received_packet_ids: crate::runtime::recent_ids::RecentIdCache<IpPacketId>,
    flows: PacketFlowTable<ServerIpCarrierKey>,
}

#[derive(Debug, Default)]
struct ServerIpTunnelState {
    tunnels: HashMap<PrincipalId, ServerLogicalIpTunnel>,
    next_tunnel_generation: u64,
    next_attachment_generation: u64,
}

struct ServerIpTunnelInner {
    plan: TunL3AddressPlan,
    paths: ServerStreamPort,
    state: Mutex<ServerIpTunnelState>,
    device_packets: mpsc::UnboundedSender<BudgetedServerTunPacket>,
    device_packet_budget: IpPacketQueueBudget,
    carrier_packet_budget: IpPacketQueueBudget,
    recent_packet_capacity: usize,
    flow_capacity: usize,
    max_paths_per_tunnel: usize,
    next_packet_id: AtomicU64,
}

#[derive(Clone)]
pub(in crate::runtime) struct ServerIpTunnelPort {
    inner: Arc<ServerIpTunnelInner>,
}

impl std::fmt::Debug for ServerIpTunnelPort {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerIpTunnelPort")
            .field("interface_name", &self.inner.plan.interface_name())
            .finish_non_exhaustive()
    }
}

pub(in crate::runtime) struct ServerIpTunnelDevice {
    inner: Arc<ServerIpTunnelInner>,
    packets: mpsc::UnboundedReceiver<BudgetedServerTunPacket>,
}

pub(super) struct BudgetedServerTunPacket {
    pub(super) payload: Bytes,
    _budget: IpPacketQueuePermit,
}

pub(in crate::runtime) struct ServerIpTunnelOutput {
    inner: Arc<ServerIpTunnelInner>,
}

pub(in crate::runtime) struct AcceptedServerIpTunnel {
    inner: Arc<ServerIpTunnelInner>,
    principal: PrincipalId,
    session_id: SessionId,
    tunnel_id: IpTunnelId,
    carrier: ServerIpCarrierKey,
    tunnel_generation: u64,
    attachment_generation: u64,
    _lifetime: Arc<()>,
    allocation: TunL3PeerAllocation,
}

pub(in crate::runtime) struct ServerIpTunnelService;

impl ServerIpTunnelService {
    pub(in crate::runtime) fn build(
        plan: TunL3AddressPlan,
        paths: ServerStreamPort,
        max_paths_per_tunnel: usize,
        packet_queue_bytes: usize,
    ) -> (ServerIpTunnelPort, ServerIpTunnelDevice) {
        let packet_queue_bytes = packet_queue_bytes.max(1);
        let packet_queue = packet_queue_bytes
            .checked_div(usize::from(plan.mtu()))
            .unwrap_or(0)
            .max(1);
        let (device_packets, packets) = mpsc::unbounded_channel();
        let inner = Arc::new(ServerIpTunnelInner {
            plan,
            paths,
            state: Mutex::new(ServerIpTunnelState::default()),
            device_packets,
            device_packet_budget: IpPacketQueueBudget::new(packet_queue_bytes),
            carrier_packet_budget: IpPacketQueueBudget::new(packet_queue_bytes),
            recent_packet_capacity: packet_queue.saturating_mul(2),
            flow_capacity: packet_queue,
            max_paths_per_tunnel: max_paths_per_tunnel.max(1),
            next_packet_id: AtomicU64::new(1),
        });
        (
            ServerIpTunnelPort {
                inner: inner.clone(),
            },
            ServerIpTunnelDevice { inner, packets },
        )
    }
}

impl ServerIpTunnelPort {
    pub(in crate::runtime) fn open(
        &self,
        request: ServerIpTunnelOpenRequest<'_>,
    ) -> Result<AcceptedServerIpTunnel, RuntimeError> {
        let principal = request.path.principal_permit().principal().clone();
        let allocation = self.inner.plan.peer(&principal).cloned().ok_or_else(|| {
            RuntimeError::DestinationDenied("principal has no TUN-L3 allocation".into())
        })?;
        let carrier = ServerIpCarrierKey {
            session_id: request.path.session_id(),
            underlay: request.path.underlay(),
            path_id: request.path.path_id(),
            path_instance_id: request.path.path_instance_id(),
        };
        let lifetime = Arc::new(());
        let mut state = self.inner.state.lock().expect("server IP tunnel lock");
        let replace = state.tunnels.get(&principal).is_some_and(|tunnel| {
            tunnel.session_id != carrier.session_id || tunnel.tunnel_id != request.tunnel_id
        });
        if replace && let Some(previous) = state.tunnels.remove(&principal) {
            close_attachments(
                previous.attachments,
                previous.tunnel_id,
                CloseReason::PolicyRejected,
            );
        }
        let generation = if let Some(tunnel) = state.tunnels.get(&principal) {
            tunnel.generation
        } else {
            state.next_tunnel_generation = state.next_tunnel_generation.wrapping_add(1).max(1);
            let generation = state.next_tunnel_generation;
            state.tunnels.insert(
                principal.clone(),
                ServerLogicalIpTunnel {
                    session_id: carrier.session_id,
                    tunnel_id: request.tunnel_id,
                    generation,
                    attachments: HashMap::new(),
                    received_packet_ids: crate::runtime::recent_ids::RecentIdCache::new(
                        self.inner.recent_packet_capacity,
                    ),
                    flows: PacketFlowTable::new(self.inner.flow_capacity),
                },
            );
            generation
        };
        state.next_attachment_generation = state.next_attachment_generation.wrapping_add(1).max(1);
        let attachment_generation = state.next_attachment_generation;
        let startup_metrics = request.path.initial_metrics();
        let tunnel = state
            .tunnels
            .get_mut(&principal)
            .expect("server IP tunnel inserted");
        prune_dead_attachments(tunnel);
        if !tunnel.attachments.contains_key(&carrier)
            && tunnel.attachments.len() >= self.inner.max_paths_per_tunnel
        {
            return Err(RuntimeError::ProductPolicy(
                "TUN-L3 carrier attachment limit reached".into(),
            ));
        }
        if let Some(previous) = tunnel.attachments.insert(
            carrier,
            ServerIpAttachment {
                key: carrier,
                config_ordinal: request.path.local_config_ordinal(),
                backup: request.path.local_policy().backup,
                startup_metrics,
                attachment_generation,
                lifetime: Arc::downgrade(&lifetime),
                carrier: request.carrier,
            },
        ) {
            previous
                .carrier
                .close(request.tunnel_id, CloseReason::Normal);
        }
        drop(state);
        Ok(AcceptedServerIpTunnel {
            inner: self.inner.clone(),
            principal,
            session_id: carrier.session_id,
            tunnel_id: request.tunnel_id,
            carrier,
            tunnel_generation: generation,
            attachment_generation,
            _lifetime: lifetime,
            allocation,
        })
    }

    pub(in crate::runtime) fn plan(&self) -> &TunL3AddressPlan {
        &self.inner.plan
    }
}

impl AcceptedServerIpTunnel {
    pub(in crate::runtime) fn allocation(&self) -> &TunL3PeerAllocation {
        &self.allocation
    }

    pub(in crate::runtime) fn receive(
        &self,
        packet_id: IpPacketId,
        payload: Bytes,
    ) -> Result<bool, RuntimeError> {
        if payload.len() > usize::from(self.inner.plan.mtu()) {
            return Ok(false);
        }
        let metadata = match parse_ip_packet(&payload) {
            Ok(metadata) => metadata,
            Err(_) => return Ok(false),
        };
        if !self.allocation.owns(metadata.source) {
            return Ok(false);
        }
        let budget = match self.inner.device_packet_budget.try_reserve(payload.len()) {
            Ok(budget) => budget,
            Err(RuntimeError::SenderServiceBlocked) => return Ok(false),
            Err(error) => return Err(error),
        };
        let mut state = self.inner.state.lock().expect("server IP tunnel lock");
        let Some(tunnel) = state.tunnels.get_mut(&self.principal) else {
            return Ok(false);
        };
        if tunnel.session_id != self.session_id
            || tunnel.tunnel_id != self.tunnel_id
            || tunnel.generation != self.tunnel_generation
            || !tunnel
                .attachments
                .get(&self.carrier)
                .is_some_and(|attachment| {
                    attachment.attachment_generation == self.attachment_generation
                })
            || tunnel.received_packet_ids.contains(&packet_id)
        {
            return Ok(false);
        }
        tunnel.received_packet_ids.insert(packet_id);
        drop(state);
        self.inner
            .device_packets
            .send(BudgetedServerTunPacket {
                payload,
                _budget: budget,
            })
            .map(|()| true)
            .map_err(|_| RuntimeError::Protocol("TUN-L3 device packet sink closed"))
    }
}

impl Drop for AcceptedServerIpTunnel {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().expect("server IP tunnel lock");
        let Some(tunnel) = state.tunnels.get_mut(&self.principal) else {
            return;
        };
        if tunnel.session_id == self.session_id
            && tunnel.tunnel_id == self.tunnel_id
            && tunnel.generation == self.tunnel_generation
            && tunnel
                .attachments
                .get(&self.carrier)
                .is_some_and(|attachment| {
                    attachment.attachment_generation == self.attachment_generation
                })
        {
            tunnel.attachments.remove(&self.carrier);
            remove_server_carrier_bindings(tunnel, self.carrier);
        }
    }
}

impl ServerIpTunnelDevice {
    pub(in crate::runtime) fn interface_name(&self) -> Option<&str> {
        self.inner.plan.interface_name()
    }

    pub(in crate::runtime) fn ipv4(&self) -> Option<std::net::Ipv4Addr> {
        self.inner.plan.ipv4()
    }

    pub(in crate::runtime) fn ipv6(&self) -> Option<std::net::Ipv6Addr> {
        self.inner.plan.ipv6()
    }

    pub(in crate::runtime) fn mtu(&self) -> u16 {
        self.inner.plan.mtu()
    }

    #[cfg(test)]
    pub(in crate::runtime) async fn receive_from_peer(&mut self) -> Option<Bytes> {
        self.packets.recv().await.map(|packet| packet.payload)
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        ServerIpTunnelOutput,
        mpsc::UnboundedReceiver<BudgetedServerTunPacket>,
    ) {
        (ServerIpTunnelOutput { inner: self.inner }, self.packets)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn try_send_to_peer(
        &self,
        payload: Bytes,
    ) -> Result<bool, RuntimeError> {
        try_send_server_packet(&self.inner, payload)
    }
}

impl ServerIpTunnelOutput {
    pub(in crate::runtime) fn try_send_to_peer(
        &self,
        payload: Bytes,
    ) -> Result<bool, RuntimeError> {
        try_send_server_packet(&self.inner, payload)
    }
}

fn try_send_server_packet(
    inner: &ServerIpTunnelInner,
    payload: Bytes,
) -> Result<bool, RuntimeError> {
    if payload.len() > usize::from(inner.plan.mtu()) {
        return Ok(false);
    }
    let metadata = match parse_ip_packet(&payload) {
        Ok(metadata) => metadata,
        Err(_) => return Ok(false),
    };
    let Some(principal) = inner.plan.owner(metadata.destination).cloned() else {
        return Ok(false);
    };
    let packet_id = IpPacketId(inner.next_packet_id.fetch_add(1, Ordering::Relaxed));
    let mut state = inner.state.lock().expect("server IP tunnel lock");
    let Some(tunnel) = state.tunnels.get_mut(&principal) else {
        return Ok(false);
    };
    prune_dead_attachments(tunnel);
    for _ in 0..2 {
        let Some(carrier) =
            select_server_carrier(&inner.paths, tunnel, &metadata.flow_key, payload.len())
        else {
            return Ok(false);
        };
        let Some(attachment) = tunnel.attachments.get(&carrier) else {
            return Ok(false);
        };
        match attachment.carrier.try_send_packet(
            tunnel.tunnel_id,
            packet_id,
            payload.clone(),
            &inner.carrier_packet_budget,
        )? {
            IpTunnelPacketSendOutcome::Accepted => return Ok(true),
            IpTunnelPacketSendOutcome::Full => return Ok(false),
            IpTunnelPacketSendOutcome::Retired => {
                tunnel.attachments.remove(&carrier);
                remove_server_carrier_bindings(tunnel, carrier);
            }
        }
    }
    Ok(false)
}

impl std::fmt::Debug for ServerIpTunnelDevice {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerIpTunnelDevice")
            .field("interface_name", &self.interface_name())
            .field("mtu", &self.mtu())
            .finish_non_exhaustive()
    }
}

fn select_server_carrier(
    paths: &ServerStreamPort,
    tunnel: &mut ServerLogicalIpTunnel,
    flow: &IpPacketFlowKey,
    packet_bytes: usize,
) -> Option<ServerIpCarrierKey> {
    let now = Instant::now();
    let attachments = &tunnel.attachments;
    let current = tunnel
        .flows
        .current(flow, now, |carrier| attachments.contains_key(&carrier));
    if let Some(carrier) = current {
        return Some(carrier);
    }

    let statuses = paths.management_snapshot().paths;
    let mut candidates = tunnel
        .attachments
        .values()
        .filter_map(|attachment| {
            let status = statuses.iter().find(|status| {
                status.session_id == attachment.key.session_id
                    && status.underlay == attachment.key.underlay
                    && status.path_id == attachment.key.path_id
                    && status.path_instance_id == attachment.key.path_instance_id
            });
            let mut snapshot = server_packet_snapshot(status, attachment);
            snapshot.active_flows = tunnel.flows.active_load_for(attachment.key, flow);
            let score = crate::scheduler::score_path(
                snapshot,
                crate::scheduler::TrafficClass::RealtimeDatagram,
                packet_bytes,
            )?;
            Some((
                crate::scheduler::path_is_backup(snapshot),
                score.eta_ms,
                attachment.config_ordinal,
                attachment.key,
                crate::model::timing::transport_pto_from_snapshot(Some(snapshot)),
            ))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.total_cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
            .then_with(|| left.3.path_id.cmp(&right.3.path_id))
            .then_with(|| left.3.underlay.cmp(&right.3.underlay))
    });
    let (carrier, flowlet_timeout) = candidates
        .first()
        .map(|candidate| (candidate.3, candidate.4))?;
    tunnel
        .flows
        .bind(flow.clone(), carrier, now, flowlet_timeout);
    Some(carrier)
}

fn server_packet_snapshot(
    status: Option<&crate::runtime::path::ServerCarrierPathStatusSnapshot>,
    attachment: &ServerIpAttachment,
) -> crate::scheduler::PathSnapshot {
    // A peer hint describes the opposite direction. Packet dispatch consumes
    // only this endpoint's native sender state (or its configured startup
    // prior), keeping Product delivery evidence outside the packet plane.
    let metrics = status
        .and_then(|status| status.metrics)
        .filter(|metrics| metrics.direction == PathMetricDirection::ServerToClient);
    let startup_metrics = attachment
        .startup_metrics
        .filter(|metrics| metrics.direction == PathMetricDirection::ServerToClient);
    let rate = server_packet_delivery_rate(metrics, startup_metrics);
    let srtt_ms = metrics.or(startup_metrics).map_or_else(
        crate::runtime::path::model::default_path_srtt_ms,
        |metrics| f64::from(metrics.srtt_us) / 1_000.0,
    );
    let mut snapshot = crate::scheduler::PathSnapshot::new(
        attachment.key.path_id,
        attachment.key.underlay,
        srtt_ms,
        rate,
    );
    snapshot.policy = status.map_or_else(
        || crate::model::path::PathPolicy {
            backup: attachment.backup,
            ..crate::model::path::PathPolicy::default()
        },
        |status| status.policy,
    );
    snapshot.peer_usage = status.and_then(|status| status.usage);
    snapshot.state = match status.map(|status| status.state) {
        Some(PeerPathState::Active) | None => crate::scheduler::PathState::Active,
        Some(PeerPathState::Suspect) => crate::scheduler::PathState::Suspect,
        Some(PeerPathState::Draining) => crate::scheduler::PathState::Draining,
        Some(PeerPathState::Failed) => crate::scheduler::PathState::Failed,
    };
    if let Some(metrics) = metrics {
        snapshot.jitter_ms = f64::from(metrics.jitter_us) / 1_000.0;
        snapshot.loss_rate = if metrics.loss_observed {
            f64::from(metrics.loss_ppm) / 1_000_000.0
        } else {
            0.0
        };
        snapshot.queue_bytes = metrics.queue_bytes;
        snapshot.bytes_in_flight = metrics.bytes_in_flight;
        // Pacing is sender intent, not delivered capacity. As on the client,
        // packet completion ranking uses the ACK-derived rate or startup prior.
        snapshot.pacing_rate_bps = rate;
        snapshot.carrier_inflight_limit_bytes = metrics.inflight_limit_bytes;
        snapshot.confidence = f64::from(metrics.confidence_ppm) / 1_000_000.0;
        snapshot.app_limited = metrics.app_limited;
        snapshot.carrier_delivery_rate_bps = (metrics.has_ack_derived_data_sample
            && metrics.delivery_rate_bps > 0)
            .then_some(metrics.delivery_rate_bps as f64);
    }
    snapshot
}

pub(super) fn server_packet_delivery_rate(
    metrics: Option<crate::protocol::PathMetrics>,
    startup_metrics: Option<crate::protocol::PathMetrics>,
) -> f64 {
    metrics
        .filter(|metrics| metrics.has_ack_derived_data_sample && metrics.delivery_rate_bps > 0)
        .map(|metrics| metrics.delivery_rate_bps as f64)
        .or_else(|| startup_metrics.map(|metrics| metrics.delivery_rate_bps.max(1) as f64))
        .unwrap_or_else(crate::runtime::path::model::default_path_rate_bps)
}

fn prune_dead_attachments(tunnel: &mut ServerLogicalIpTunnel) {
    tunnel
        .attachments
        .retain(|_, attachment| attachment.lifetime.upgrade().is_some());
    let attachments = &tunnel.attachments;
    tunnel
        .flows
        .retain_carriers(|carrier| attachments.contains_key(&carrier));
}

fn remove_server_carrier_bindings(tunnel: &mut ServerLogicalIpTunnel, carrier: ServerIpCarrierKey) {
    tunnel.flows.remove_carrier(carrier);
}

fn close_attachments(
    attachments: HashMap<ServerIpCarrierKey, ServerIpAttachment>,
    tunnel_id: IpTunnelId,
    reason: CloseReason,
) {
    for attachment in attachments.into_values() {
        attachment.carrier.close(tunnel_id, reason);
    }
}
