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
use crate::runtime::path::{
    CarrierDeliveryRateSample, ServerCarrierPathRegistration, ServerRealtimeFlowLease,
    ServerStreamPort,
};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ServerIpTunnelNoAttachmentRetention {
    epoch: u64,
    deadline: tokio::time::Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ServerIpTunnelExpiry {
    principal: PrincipalId,
    session_id: SessionId,
    tunnel_id: IpTunnelId,
    tunnel_generation: u64,
    retention: ServerIpTunnelNoAttachmentRetention,
}

#[derive(Debug)]
struct ServerLogicalIpTunnel {
    session_id: SessionId,
    session_owner: ServerRealtimeFlowLease,
    tunnel_id: IpTunnelId,
    generation: u64,
    no_attachment_retention: Option<ServerIpTunnelNoAttachmentRetention>,
    attachments: HashMap<ServerIpCarrierKey, ServerIpAttachment>,
    received_packet_ids: crate::runtime::recent_ids::RecentIdCache<IpPacketId>,
    flows: PacketFlowTable<ServerIpCarrierKey>,
}

impl ServerLogicalIpTunnel {
    fn begin_no_attachment_retention(
        &mut self,
        next_retention_epoch: &AtomicU64,
        retention_timeout: Duration,
    ) -> Option<ServerIpTunnelNoAttachmentRetention> {
        debug_assert!(self.attachments.is_empty());
        if self.no_attachment_retention.is_some() {
            return None;
        }
        let retention = ServerIpTunnelNoAttachmentRetention {
            epoch: next_retention_epoch.fetch_add(1, Ordering::Relaxed),
            deadline: tokio::time::Instant::now() + retention_timeout,
        };
        self.no_attachment_retention = Some(retention);
        Some(retention)
    }
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
    session_retention_timeout: Duration,
    next_packet_id: AtomicU64,
    next_retention_epoch: AtomicU64,
    timer_runtime: Option<tokio::runtime::Handle>,
    #[cfg(test)]
    open_after_initial_retirement_check_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync + 'static>>>,
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
        session_retention_timeout: Duration,
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
            session_retention_timeout,
            next_packet_id: AtomicU64::new(1),
            next_retention_epoch: AtomicU64::new(1),
            timer_runtime: tokio::runtime::Handle::try_current().ok(),
            #[cfg(test)]
            open_after_initial_retirement_check_hook: Mutex::new(None),
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
        let session_id = request.path.session_id();
        // The logical tunnel survives attachment loss, so it must own the
        // authenticated session independently of every carrier attachment.
        // A temporary candidate also gives this opener the exact sticky fence;
        // insertion transfers it to a new logical owner, while reattachment to
        // an existing owner drops the redundant reference after `state`.
        let mut session_owner = Some(self.inner.paths.register_realtime_flow(session_id)?);
        let session_retirement = session_owner
            .as_ref()
            .expect("server IP tunnel session owner candidate")
            .retirement();
        if let Some(reason) = session_retirement.reason() {
            return Err(RuntimeError::RemoteClosed(reason));
        }
        #[cfg(test)]
        if let Some(hook) = self
            .inner
            .open_after_initial_retirement_check_hook
            .lock()
            .expect("server IP tunnel open hook lock")
            .clone()
        {
            hook();
        }
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
        // Principal takeover and the second sticky-fence read share the owner
        // lock. A stale opener that was retired while waiting cannot remove or
        // close the current principal incarnation.
        if let Some(reason) = session_retirement.reason() {
            return Err(RuntimeError::RemoteClosed(reason));
        }
        let replace = state.tunnels.get(&principal).is_some_and(|tunnel| {
            tunnel.session_id != carrier.session_id || tunnel.tunnel_id != request.tunnel_id
        });
        let replaced_tunnel = replace.then(|| state.tunnels.remove(&principal)).flatten();
        let generation = if let Some(tunnel) = state.tunnels.get(&principal) {
            tunnel.generation
        } else {
            state.next_tunnel_generation = state.next_tunnel_generation.wrapping_add(1).max(1);
            let generation = state.next_tunnel_generation;
            state.tunnels.insert(
                principal.clone(),
                ServerLogicalIpTunnel {
                    session_id: carrier.session_id,
                    session_owner: session_owner
                        .take()
                        .expect("new server IP tunnel takes session ownership"),
                    tunnel_id: request.tunnel_id,
                    generation,
                    no_attachment_retention: None,
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
        let was_carrierless = tunnel.attachments.is_empty();
        let replaced_attachment = tunnel.attachments.insert(
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
        );
        if was_carrierless {
            // Only a committed 0 -> 1 transition invalidates the absolute
            // carrierless epoch. Failed and duplicate opens never reach this
            // boundary and therefore cannot extend the deadline.
            tunnel.no_attachment_retention = None;
        }
        drop(state);
        // Existing logical tunnels already own the same authenticated session.
        // Release this opener's redundant reference without holding the TUN
        // owner lock; takeover similarly drops the displaced owner's lease
        // only through `replaced_tunnel` below.
        drop(session_owner);
        if let Some(previous) = replaced_attachment {
            previous
                .carrier
                .close(request.tunnel_id, CloseReason::Normal);
        }
        if let Some(previous) = replaced_tunnel {
            close_attachments(
                previous.attachments,
                previous.tunnel_id,
                CloseReason::PolicyRejected,
            );
        }
        if let Some(reason) = session_retirement.reason() {
            self.retire_session(session_id, reason);
            return Err(RuntimeError::RemoteClosed(reason));
        }
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

    #[cfg(test)]
    pub(super) fn set_open_after_initial_retirement_check_hook(
        &self,
        hook: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
    ) {
        *self
            .inner
            .open_after_initial_retirement_check_hook
            .lock()
            .expect("server IP tunnel open hook lock") = hook;
    }

    pub(in crate::runtime) fn retire_session(&self, session_id: SessionId, reason: CloseReason) {
        let retired = {
            let mut state = self.inner.state.lock().expect("server IP tunnel lock");
            let principals = state
                .tunnels
                .iter()
                .filter_map(|(principal, tunnel)| {
                    (tunnel.session_id == session_id).then_some(principal.clone())
                })
                .collect::<Vec<_>>();
            principals
                .into_iter()
                .filter_map(|principal| state.tunnels.remove(&principal))
                .collect::<Vec<_>>()
        };
        for tunnel in retired {
            close_attachments(tunnel.attachments, tunnel.tunnel_id, reason);
        }
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
        if tunnel.session_owner.is_retired()
            || tunnel.session_id != self.session_id
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

fn remove_server_ip_attachment(
    tunnel: &mut ServerLogicalIpTunnel,
    carrier: ServerIpCarrierKey,
    attachment_generation: u64,
    next_retention_epoch: &AtomicU64,
    retention_timeout: Duration,
) -> Option<ServerIpTunnelNoAttachmentRetention> {
    if tunnel
        .attachments
        .get(&carrier)
        .is_none_or(|attachment| attachment.attachment_generation != attachment_generation)
    {
        return None;
    }
    tunnel.attachments.remove(&carrier);
    remove_server_carrier_bindings(tunnel, carrier);
    if tunnel.attachments.is_empty() {
        tunnel.begin_no_attachment_retention(next_retention_epoch, retention_timeout)
    } else {
        None
    }
}

fn server_ip_tunnel_expiry(
    principal: &PrincipalId,
    tunnel: &ServerLogicalIpTunnel,
    retention: ServerIpTunnelNoAttachmentRetention,
) -> ServerIpTunnelExpiry {
    ServerIpTunnelExpiry {
        principal: principal.clone(),
        session_id: tunnel.session_id,
        tunnel_id: tunnel.tunnel_id,
        tunnel_generation: tunnel.generation,
        retention,
    }
}

fn spawn_server_ip_tunnel_expiry(inner: &Arc<ServerIpTunnelInner>, expiry: ServerIpTunnelExpiry) {
    let Some(runtime) = &inner.timer_runtime else {
        // Production packet services are constructed under their Tokio owner.
        // Synchronous model-only tests may construct a service without a timer
        // driver and do not exercise carrierless expiry.
        return;
    };
    let owner = Arc::downgrade(inner);
    runtime.spawn(async move {
        tokio::time::sleep_until(expiry.retention.deadline).await;
        let Some(inner) = owner.upgrade() else {
            return;
        };
        expire_server_ip_tunnel(&inner, &expiry);
    });
}

fn expire_server_ip_tunnel(inner: &ServerIpTunnelInner, expiry: &ServerIpTunnelExpiry) {
    let expired = {
        let mut state = inner.state.lock().expect("server IP tunnel lock");
        let exact_expired_owner = state.tunnels.get(&expiry.principal).is_some_and(|tunnel| {
            tunnel.session_id == expiry.session_id
                && tunnel.tunnel_id == expiry.tunnel_id
                && tunnel.generation == expiry.tunnel_generation
                && tunnel.attachments.is_empty()
                && tunnel.no_attachment_retention == Some(expiry.retention)
                && tokio::time::Instant::now() >= expiry.retention.deadline
        });
        exact_expired_owner
            .then(|| state.tunnels.remove(&expiry.principal))
            .flatten()
    };
    // Removing the complete logical tunnel transfers the retained session
    // lease out of the owner lock. Its tracker callback therefore cannot
    // participate in a TUN-lock/session-tracker lock cycle.
    drop(expired);
}

impl Drop for AcceptedServerIpTunnel {
    fn drop(&mut self) {
        let expiry = {
            let mut state = self.inner.state.lock().expect("server IP tunnel lock");
            let Some(tunnel) = state.tunnels.get_mut(&self.principal) else {
                return;
            };
            if tunnel.session_id != self.session_id
                || tunnel.tunnel_id != self.tunnel_id
                || tunnel.generation != self.tunnel_generation
            {
                return;
            }
            remove_server_ip_attachment(
                tunnel,
                self.carrier,
                self.attachment_generation,
                &self.inner.next_retention_epoch,
                self.inner.session_retention_timeout,
            )
            .map(|retention| server_ip_tunnel_expiry(&self.principal, tunnel, retention))
        };
        if let Some(expiry) = expiry {
            spawn_server_ip_tunnel_expiry(&self.inner, expiry);
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
    inner: &Arc<ServerIpTunnelInner>,
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
    let (result, expiry) = {
        let mut state = inner.state.lock().expect("server IP tunnel lock");
        let Some(tunnel) = state.tunnels.get_mut(&principal) else {
            return Ok(false);
        };
        if tunnel.session_owner.is_retired() {
            return Ok(false);
        }
        let mut expiry = if prune_dead_attachments(tunnel) {
            tunnel
                .begin_no_attachment_retention(
                    &inner.next_retention_epoch,
                    inner.session_retention_timeout,
                )
                .map(|retention| server_ip_tunnel_expiry(&principal, tunnel, retention))
        } else {
            None
        };
        let result = 'dispatch: {
            for _ in 0..2 {
                let Some(carrier) =
                    select_server_carrier(&inner.paths, tunnel, &metadata.flow_key, payload.len())
                else {
                    break 'dispatch Ok(false);
                };
                let Some(attachment) = tunnel.attachments.get(&carrier) else {
                    break 'dispatch Ok(false);
                };
                let attachment_generation = attachment.attachment_generation;
                let outcome = attachment.carrier.try_send_packet(
                    tunnel.tunnel_id,
                    packet_id,
                    payload.clone(),
                    &inner.carrier_packet_budget,
                );
                match outcome {
                    Ok(IpTunnelPacketSendOutcome::Accepted) => break 'dispatch Ok(true),
                    Ok(IpTunnelPacketSendOutcome::Full) => break 'dispatch Ok(false),
                    Ok(IpTunnelPacketSendOutcome::Retired) => {
                        if let Some(retention) = remove_server_ip_attachment(
                            tunnel,
                            carrier,
                            attachment_generation,
                            &inner.next_retention_epoch,
                            inner.session_retention_timeout,
                        ) {
                            debug_assert!(expiry.is_none());
                            expiry = Some(server_ip_tunnel_expiry(&principal, tunnel, retention));
                        }
                    }
                    Err(error) => break 'dispatch Err(error),
                }
            }
            Ok(false)
        };
        (result, expiry)
    };
    if let Some(expiry) = expiry {
        spawn_server_ip_tunnel_expiry(inner, expiry);
    }
    result
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
    server_packet_snapshot_at(status, attachment, Instant::now())
}

#[derive(Debug, Clone, Copy)]
struct ServerPacketRateAuthority {
    delivery_rate_bps: f64,
    pacing_rate_bps: f64,
    confidence: f64,
    app_limited: bool,
}

fn server_packet_snapshot_at(
    status: Option<&crate::runtime::path::ServerCarrierPathStatusSnapshot>,
    attachment: &ServerIpAttachment,
    now: Instant,
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
    let measured_rate = server_packet_rate_authority_at(
        attachment.key.underlay,
        metrics,
        status.and_then(|status| status.carrier_delivery_rate_sample),
        now,
    );
    let startup_rate = startup_metrics.map(|metrics| ServerPacketRateAuthority {
        delivery_rate_bps: metrics.delivery_rate_bps.max(1) as f64,
        pacing_rate_bps: if metrics.pacing_rate_observed {
            metrics.pacing_rate_bps
        } else {
            metrics.delivery_rate_bps
        }
        .max(1) as f64,
        confidence: f64::from(metrics.confidence_ppm) / 1_000_000.0,
        app_limited: metrics.app_limited,
    });
    let rate = measured_rate
        .or(startup_rate)
        .map_or_else(crate::runtime::path::model::default_path_rate_bps, |rate| {
            rate.delivery_rate_bps
        });
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
        snapshot.queue_bytes = if metrics.queue_observed {
            metrics.queue_bytes
        } else {
            0
        };
        snapshot.bytes_in_flight = if metrics.bytes_in_flight_observed {
            metrics.bytes_in_flight
        } else {
            0
        };
        // A nonzero native limit is independent congestion credit, not an
        // assertion that an exact queue or flight observation is available.
        snapshot.carrier_inflight_limit_bytes = metrics.inflight_limit_bytes;
    }
    if let Some(rate_authority) = measured_rate.or(startup_rate) {
        snapshot.pacing_rate_bps = rate_authority.pacing_rate_bps;
        snapshot.confidence = rate_authority.confidence;
        snapshot.app_limited = rate_authority.app_limited;
    }
    snapshot.carrier_delivery_rate_bps = measured_rate.map(|rate| rate.delivery_rate_bps);
    snapshot
}

fn server_packet_rate_authority_at(
    underlay: UnderlayProtocol,
    metrics: Option<crate::protocol::PathMetrics>,
    carrier_sample: Option<CarrierDeliveryRateSample>,
    now: Instant,
) -> Option<ServerPacketRateAuthority> {
    if let Some(sample) = carrier_sample {
        // Presence is authoritative: an expired sidecar cannot fall through
        // to retained PathMetrics and silently regain scheduling authority.
        if sample.delivery_rate_bps == 0 || sample.observed_at > now || now >= sample.expires_at {
            return None;
        }
        let delivery_rate_bps = sample.delivery_rate_bps as f64;
        return Some(ServerPacketRateAuthority {
            delivery_rate_bps,
            pacing_rate_bps: sample
                .pacing_rate_bps
                .filter(|rate| *rate > 0)
                .map_or(delivery_rate_bps, |rate| rate as f64),
            confidence: metrics.map_or(1.0, |metrics| {
                f64::from(metrics.confidence_ppm) / 1_000_000.0
            }),
            // A CarrierDeliveryRateSample is, by contract, a qualified
            // positive-ACK non-application-limited sample.
            app_limited: false,
        });
    }

    let metrics = metrics.filter(|metrics| {
        underlay == UnderlayProtocol::Udp
            && metrics.underlay == UnderlayProtocol::Udp
            && metrics.has_ack_derived_data_sample
            && metrics.rate_observed
            && metrics.data_sample_count > 0
            && metrics.data_sample_bytes > 0
            && metrics.delivery_rate_bps > 0
            && metrics.rate_valid_for_us > 0
    })?;
    Some(ServerPacketRateAuthority {
        delivery_rate_bps: metrics.delivery_rate_bps as f64,
        pacing_rate_bps: if metrics.pacing_rate_observed {
            metrics.pacing_rate_bps
        } else {
            metrics.delivery_rate_bps
        }
        .max(1) as f64,
        confidence: f64::from(metrics.confidence_ppm) / 1_000_000.0,
        app_limited: metrics.app_limited,
    })
}

#[cfg(test)]
pub(super) fn server_packet_delivery_rate(
    metrics: Option<crate::protocol::PathMetrics>,
    startup_metrics: Option<crate::protocol::PathMetrics>,
) -> f64 {
    let underlay = metrics
        .or(startup_metrics)
        .map_or(UnderlayProtocol::Udp, |metrics| metrics.underlay);
    server_packet_rate_authority_at(underlay, metrics, None, Instant::now())
        .map(|authority| authority.delivery_rate_bps)
        .or_else(|| startup_metrics.map(|metrics| metrics.delivery_rate_bps.max(1) as f64))
        .unwrap_or_else(crate::runtime::path::model::default_path_rate_bps)
}

fn prune_dead_attachments(tunnel: &mut ServerLogicalIpTunnel) -> bool {
    let had_attachments = !tunnel.attachments.is_empty();
    tunnel
        .attachments
        .retain(|_, attachment| attachment.lifetime.upgrade().is_some());
    let attachments = &tunnel.attachments;
    tunnel
        .flows
        .retain_carriers(|carrier| attachments.contains_key(&carrier));
    had_attachments && tunnel.attachments.is_empty()
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

#[cfg(test)]
mod packet_metric_authority_tests {
    use super::*;
    use crate::model::path::PathPolicy;
    use crate::protocol::PathMetrics;
    use crate::runtime::path::ServerCarrierPathStatusSnapshot;

    #[derive(Debug)]
    struct UnusedCarrier;

    impl ServerIpTunnelCarrier for UnusedCarrier {
        fn try_send_packet(
            &self,
            _tunnel_id: IpTunnelId,
            _packet_id: IpPacketId,
            _payload: Bytes,
            _budget: &IpPacketQueueBudget,
        ) -> Result<IpTunnelPacketSendOutcome, RuntimeError> {
            panic!("packet carrier is not used by snapshot tests")
        }

        fn close(&self, _tunnel_id: IpTunnelId, _reason: CloseReason) {}
    }

    fn packet_metrics(underlay: UnderlayProtocol) -> (PathMetrics, PathMetrics) {
        let startup = PathMetrics {
            path_id: PathId(7),
            underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: 1,
            metric_age_us: 0,
            rate_valid_for_us: 0,
            rate_observed: false,
            srtt_us: 20_000,
            rttvar_us: 2_000,
            jitter_us: 2_000,
            delivery_rate_bps: 5_000_000,
            pacing_rate_bps: 6_000_000,
            pacing_rate_observed: false,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight_observed: false,
            queue_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: 0,
            inflight_hi_bytes: 0,
            confidence_ppm: 100_000,
            app_limited: true,
            has_ack_derived_data_sample: false,
            data_sample_count: 0,
            data_sample_bytes: 0,
        };
        let live = PathMetrics {
            rate_valid_for_us: 1_000_000,
            rate_observed: true,
            delivery_rate_bps: 80_000_000,
            pacing_rate_bps: 90_000_000,
            pacing_rate_observed: true,
            confidence_ppm: 700_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: 8,
            data_sample_bytes: 128 * 1024,
            ..startup
        };
        (startup, live)
    }

    fn attachment(
        underlay: UnderlayProtocol,
        startup_metrics: PathMetrics,
    ) -> (ServerIpAttachment, Arc<()>) {
        let lifetime = Arc::new(());
        (
            ServerIpAttachment {
                key: ServerIpCarrierKey {
                    session_id: SessionId(11),
                    underlay,
                    path_id: PathId(7),
                    path_instance_id: CarrierPathInstanceId::from_raw(19),
                },
                config_ordinal: 0,
                backup: false,
                startup_metrics: Some(startup_metrics),
                attachment_generation: 1,
                lifetime: Arc::downgrade(&lifetime),
                carrier: Arc::new(UnusedCarrier),
            },
            lifetime,
        )
    }

    fn status(
        underlay: UnderlayProtocol,
        metrics: PathMetrics,
        carrier_delivery_rate_sample: Option<CarrierDeliveryRateSample>,
    ) -> ServerCarrierPathStatusSnapshot {
        ServerCarrierPathStatusSnapshot {
            session_id: SessionId(11),
            underlay,
            path_id: PathId(7),
            path_instance_id: CarrierPathInstanceId::from_raw(19),
            configured_index: 0,
            policy: PathPolicy::default(),
            state: PeerPathState::Active,
            usage: Some(crate::protocol::PathUsage::Available),
            metrics: Some(metrics),
            carrier_delivery_rate_sample,
            source: Some("local_sender"),
        }
    }

    fn assert_startup_rate_bundle(snapshot: crate::scheduler::PathSnapshot) {
        assert_eq!(snapshot.delivery_rate_bps, 5_000_000.0);
        assert_eq!(snapshot.pacing_rate_bps, 5_000_000.0);
        assert_eq!(snapshot.confidence, 0.1);
        assert!(snapshot.app_limited);
        assert_eq!(snapshot.carrier_delivery_rate_bps, None);
    }

    #[test]
    fn packet_snapshot_presence_flags_gate_retained_values_but_not_native_limit() {
        let (startup, mut live) = packet_metrics(UnderlayProtocol::Udp);
        live.queue_observed = false;
        live.queue_bytes = 41_000;
        live.bytes_in_flight_observed = false;
        live.bytes_in_flight = 73_000;
        live.inflight_limit_bytes = 256_000;
        let retained_status = status(UnderlayProtocol::Udp, live, None);
        let (attachment, _lifetime) = attachment(UnderlayProtocol::Udp, startup);

        let snapshot =
            server_packet_snapshot_at(Some(&retained_status), &attachment, Instant::now());
        assert_eq!(snapshot.queue_bytes, 0);
        assert_eq!(snapshot.bytes_in_flight, 0);
        assert_eq!(snapshot.carrier_inflight_limit_bytes, 256_000);

        let observed = status(
            UnderlayProtocol::Udp,
            PathMetrics {
                queue_observed: true,
                bytes_in_flight_observed: true,
                ..live
            },
            None,
        );
        let snapshot = server_packet_snapshot_at(Some(&observed), &attachment, Instant::now());
        assert_eq!(snapshot.queue_bytes, 41_000);
        assert_eq!(snapshot.bytes_in_flight, 73_000);
        assert_eq!(snapshot.carrier_inflight_limit_bytes, 256_000);
    }

    #[test]
    fn packet_snapshot_sidecar_deadline_is_authoritative_for_tcp_and_quic() {
        let now = Instant::now();
        let expires_at = now + Duration::from_millis(10);
        for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
            let (startup, live) = packet_metrics(underlay);
            let sample = CarrierDeliveryRateSample {
                delivery_rate_bps: 120_000_000,
                pacing_rate_bps: Some(140_000_000),
                sample_count: 3,
                sample_bytes: 192 * 1024,
                delivery_window_covered: true,
                observed_at: now,
                expires_at,
            };
            let status = status(underlay, live, Some(sample));
            let (attachment, _lifetime) = attachment(underlay, startup);

            let fresh = server_packet_snapshot_at(
                Some(&status),
                &attachment,
                expires_at - Duration::from_nanos(1),
            );
            assert_eq!(fresh.delivery_rate_bps, 120_000_000.0);
            assert_eq!(fresh.pacing_rate_bps, 140_000_000.0);
            assert_eq!(fresh.confidence, 0.7);
            assert!(!fresh.app_limited);
            assert_eq!(fresh.carrier_delivery_rate_bps, Some(120_000_000.0));

            // Retained live PathMetrics are deliberately still qualified. At
            // the exact deadline, an expired sidecar must not fall through.
            let expired = server_packet_snapshot_at(Some(&status), &attachment, expires_at);
            assert_startup_rate_bundle(expired);
        }
    }

    #[test]
    fn packet_snapshot_quic_remaining_budget_boundary_reverts_whole_rate_bundle() {
        let (startup, mut live) = packet_metrics(UnderlayProtocol::Udp);
        let (attachment, _lifetime) = attachment(UnderlayProtocol::Udp, startup);

        live.metric_age_us = u32::MAX;
        live.rate_valid_for_us = 1;
        let fresh_status = status(UnderlayProtocol::Udp, live, None);
        let fresh = server_packet_snapshot_at(Some(&fresh_status), &attachment, Instant::now());
        assert_eq!(fresh.delivery_rate_bps, 80_000_000.0);
        assert_eq!(fresh.pacing_rate_bps, 90_000_000.0);
        assert_eq!(fresh.confidence, 0.7);
        assert!(!fresh.app_limited);
        assert_eq!(fresh.carrier_delivery_rate_bps, Some(80_000_000.0));

        live.metric_age_us = 0;
        live.rate_valid_for_us = 0;
        let stale_status = status(UnderlayProtocol::Udp, live, None);
        let stale = server_packet_snapshot_at(Some(&stale_status), &attachment, Instant::now());
        assert_startup_rate_bundle(stale);
    }

    #[test]
    fn packet_snapshot_tcp_path_metrics_never_replace_missing_sidecar() {
        let (startup, live) = packet_metrics(UnderlayProtocol::Tcp);
        let status = status(UnderlayProtocol::Tcp, live, None);
        let (attachment, _lifetime) = attachment(UnderlayProtocol::Tcp, startup);

        assert_startup_rate_bundle(server_packet_snapshot_at(
            Some(&status),
            &attachment,
            Instant::now(),
        ));
    }

    #[test]
    fn packet_snapshot_ack_reachability_without_sample_volume_is_not_capacity() {
        let (startup, mut live) = packet_metrics(UnderlayProtocol::Udp);
        live.has_ack_derived_data_sample = true;
        live.data_sample_count = 0;
        live.data_sample_bytes = 0;
        live.delivery_rate_bps = 800_000_000;
        live.pacing_rate_bps = 900_000_000;
        let status = status(UnderlayProtocol::Udp, live, None);
        let (attachment, _lifetime) = attachment(UnderlayProtocol::Udp, startup);

        assert_startup_rate_bundle(server_packet_snapshot_at(
            Some(&status),
            &attachment,
            Instant::now(),
        ));
    }
}
