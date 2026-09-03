//! Transport-neutral server packet ownership and dispatch.

use super::flow::PacketFlowTable;
use super::{IpPacketQueueBudget, IpPacketQueuePermit};
use crate::model::carrier_rate_authority::{CarrierRateAuthorityBasis, CarrierRateAuthorityStamp};
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
        budget_permit: Option<IpPacketQueuePermit>,
        native_retention_limit_bytes: Option<u64>,
    ) -> Result<IpTunnelPacketSendOutcome, RuntimeError>;

    fn native_rate_authority(
        &self,
    ) -> Option<Arc<crate::runtime::path::authority::NativeCarrierRateAuthorityHandle>> {
        None
    }

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

impl ServerIpCarrierKey {
    fn path_identity(self) -> crate::runtime::path::ServerCarrierPathIdentity {
        crate::runtime::path::ServerCarrierPathIdentity {
            session_id: self.session_id,
            underlay: self.underlay,
            path_id: self.path_id,
            path_instance_id: self.path_instance_id,
        }
    }
}

#[derive(Debug, Clone)]
struct ServerIpAttachment {
    key: ServerIpCarrierKey,
    config_ordinal: usize,
    backup: bool,
    startup_metrics: Option<crate::protocol::PathMetrics>,
    attachment_generation: u64,
    lifetime: Weak<()>,
    apply_authority: crate::runtime::path::ServerCarrierPathApplyAuthority,
    native_rate_authority:
        Option<Arc<crate::runtime::path::authority::NativeCarrierRateAuthorityHandle>>,
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
    dispatch_generation: u64,
    no_attachment_retention: Option<ServerIpTunnelNoAttachmentRetention>,
    attachments: HashMap<ServerIpCarrierKey, ServerIpAttachment>,
    received_packet_ids: crate::runtime::recent_ids::RecentIdCache<IpPacketId>,
    flows: PacketFlowTable<ServerIpCarrierKey>,
}

impl ServerLogicalIpTunnel {
    fn advance_dispatch_generation(&mut self) {
        self.dispatch_generation = self.dispatch_generation.wrapping_add(1).max(1);
    }

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
        let underlay = request.path.underlay();
        let native_rate_authority = request.carrier.native_rate_authority();
        match (underlay, native_rate_authority.is_some()) {
            (UnderlayProtocol::Udp, true) | (UnderlayProtocol::Tcp, false) => {}
            (UnderlayProtocol::Udp, false) => {
                return Err(RuntimeError::Protocol(
                    "server UDP IP tunnel carrier missing native rate authority",
                ));
            }
            (UnderlayProtocol::Tcp, true) => {
                return Err(RuntimeError::Protocol(
                    "server TCP IP tunnel carrier exposed native rate authority",
                ));
            }
        }
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
            underlay,
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
                    dispatch_generation: 1,
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
        let apply_authority = request.path.apply_authority();
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
                apply_authority,
                native_rate_authority,
                carrier: request.carrier,
            },
        );
        tunnel.advance_dispatch_generation();
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
    tunnel.advance_dispatch_generation();
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
    for _ in 0..2 {
        let (capture, expiry) =
            capture_server_ip_dispatch(inner, &principal, &metadata.flow_key, Instant::now());
        if let Some(expiry) = expiry {
            spawn_server_ip_tunnel_expiry(inner, expiry);
        }
        let Some(capture) = capture else {
            return Ok(false);
        };
        let plan = match select_server_carrier(&inner.paths, capture, payload.len()) {
            ServerIpDispatchSelection::Selected(plan) => plan,
            ServerIpDispatchSelection::Unavailable => return Ok(false),
            ServerIpDispatchSelection::Stale => continue,
        };
        let budget_permit = if plan.attachment.key.underlay == UnderlayProtocol::Udp {
            match inner.carrier_packet_budget.try_reserve(payload.len()) {
                Ok(permit) => Some(permit),
                Err(RuntimeError::SenderServiceBlocked) => return Ok(false),
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        let apply = apply_server_ip_dispatch(
            inner,
            &principal,
            &metadata.flow_key,
            packet_id,
            payload.clone(),
            plan,
            budget_permit,
        )?;
        if let Some(expiry) = apply.expiry {
            spawn_server_ip_tunnel_expiry(inner, expiry);
        }
        match apply.outcome {
            ServerIpDispatchOutcome::Accepted => return Ok(true),
            ServerIpDispatchOutcome::Full => return Ok(false),
            ServerIpDispatchOutcome::Stale | ServerIpDispatchOutcome::Retired => continue,
        }
    }
    Ok(false)
}

#[derive(Debug)]
struct ServerIpDispatchCapture {
    tunnel_id: IpTunnelId,
    tunnel_generation: u64,
    dispatch_generation: u64,
    current: Option<ServerIpCarrierKey>,
    attachments: Vec<(ServerIpAttachment, u32)>,
}

#[derive(Debug, Clone, Copy)]
struct ServerIpDispatchBasis {
    tunnel_id: IpTunnelId,
    tunnel_generation: u64,
    dispatch_generation: u64,
    current: Option<ServerIpCarrierKey>,
}

impl From<&ServerIpDispatchCapture> for ServerIpDispatchBasis {
    fn from(capture: &ServerIpDispatchCapture) -> Self {
        Self {
            tunnel_id: capture.tunnel_id,
            tunnel_generation: capture.tunnel_generation,
            dispatch_generation: capture.dispatch_generation,
            current: capture.current,
        }
    }
}

#[derive(Debug, Clone)]
struct ServerIpDispatchPlan {
    tunnel_id: IpTunnelId,
    tunnel_generation: u64,
    dispatch_generation: u64,
    attachment: ServerIpAttachment,
    status: crate::runtime::path::ServerCarrierPathStatusSnapshot,
    eligibility_epoch: u64,
    native_stamp: Option<CarrierRateAuthorityStamp>,
    flowlet_timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServerIpDispatchOutcome {
    Accepted,
    Full,
    Retired,
    Stale,
}

#[derive(Debug)]
enum ServerIpDispatchSelection {
    Selected(ServerIpDispatchPlan),
    Unavailable,
    Stale,
}

#[derive(Debug)]
struct RankedServerIpDispatchPlan {
    current_mismatch: bool,
    backup: bool,
    eta_ms: f64,
    config_ordinal: usize,
    plan: ServerIpDispatchPlan,
}

#[derive(Debug)]
enum ServerIpCarrierEvaluation {
    Candidate(RankedServerIpDispatchPlan),
    Unavailable,
    Stale,
}

#[derive(Debug)]
struct ServerIpDispatchApply {
    outcome: ServerIpDispatchOutcome,
    expiry: Option<ServerIpTunnelExpiry>,
}

fn capture_server_ip_dispatch(
    inner: &Arc<ServerIpTunnelInner>,
    principal: &PrincipalId,
    flow: &IpPacketFlowKey,
    now: Instant,
) -> (
    Option<ServerIpDispatchCapture>,
    Option<ServerIpTunnelExpiry>,
) {
    let mut state = inner.state.lock().expect("server IP tunnel lock");
    let Some(tunnel) = state.tunnels.get_mut(principal) else {
        return (None, None);
    };
    if tunnel.session_owner.is_retired() {
        return (None, None);
    }
    let expiry = if prune_dead_attachments(tunnel) {
        tunnel
            .begin_no_attachment_retention(
                &inner.next_retention_epoch,
                inner.session_retention_timeout,
            )
            .map(|retention| server_ip_tunnel_expiry(principal, tunnel, retention))
    } else {
        None
    };
    if tunnel.attachments.is_empty() {
        return (None, expiry);
    }
    let attachments = &tunnel.attachments;
    let current = tunnel
        .flows
        .planned_current(flow, now, |carrier| attachments.contains_key(&carrier));
    let attachments = tunnel
        .attachments
        .values()
        .cloned()
        .map(|attachment| {
            let active_load = tunnel.flows.active_load_for(attachment.key, flow);
            (attachment, active_load)
        })
        .collect();
    (
        Some(ServerIpDispatchCapture {
            tunnel_id: tunnel.tunnel_id,
            tunnel_generation: tunnel.generation,
            dispatch_generation: tunnel.dispatch_generation,
            current,
            attachments,
        }),
        expiry,
    )
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
    capture: ServerIpDispatchCapture,
    packet_bytes: usize,
) -> ServerIpDispatchSelection {
    let basis = ServerIpDispatchBasis::from(&capture);
    let identities = capture
        .attachments
        .iter()
        .map(|(attachment, _)| attachment.key.path_identity())
        .collect::<Vec<_>>();
    let statuses = paths.carrier_path_statuses(&identities);
    let mut saw_stale = false;
    let mut candidates = capture
        .attachments
        .into_iter()
        .zip(statuses)
        .filter_map(|((attachment, active_load), status)| {
            match evaluate_server_carrier(&basis, attachment, active_load, status, packet_bytes) {
                ServerIpCarrierEvaluation::Candidate(candidate) => Some(candidate),
                ServerIpCarrierEvaluation::Stale => {
                    saw_stale = true;
                    None
                }
                ServerIpCarrierEvaluation::Unavailable => None,
            }
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        left.current_mismatch
            .cmp(&right.current_mismatch)
            .then_with(|| left.backup.cmp(&right.backup))
            .then_with(|| left.eta_ms.total_cmp(&right.eta_ms))
            .then_with(|| left.config_ordinal.cmp(&right.config_ordinal))
            .then_with(|| {
                left.plan
                    .attachment
                    .key
                    .path_id
                    .cmp(&right.plan.attachment.key.path_id)
            })
            .then_with(|| {
                left.plan
                    .attachment
                    .key
                    .underlay
                    .cmp(&right.plan.attachment.key.underlay)
            })
    });
    match candidates.into_iter().next() {
        Some(candidate) => ServerIpDispatchSelection::Selected(candidate.plan),
        None if saw_stale => ServerIpDispatchSelection::Stale,
        None => ServerIpDispatchSelection::Unavailable,
    }
}

fn evaluate_server_carrier(
    basis: &ServerIpDispatchBasis,
    attachment: ServerIpAttachment,
    active_load: u32,
    status: Option<crate::runtime::path::ServerCarrierPathStatusSnapshot>,
    packet_bytes: usize,
) -> ServerIpCarrierEvaluation {
    let Some(status) = status else {
        return ServerIpCarrierEvaluation::Stale;
    };
    if status.session_id != attachment.key.session_id
        || status.underlay != attachment.key.underlay
        || status.path_id != attachment.key.path_id
        || status.path_instance_id != attachment.key.path_instance_id
    {
        return ServerIpCarrierEvaluation::Stale;
    }
    let apply = attachment.apply_authority.snapshot();
    let Some(eligibility_epoch) = apply.eligibility_epoch else {
        return ServerIpCarrierEvaluation::Unavailable;
    };
    if status.eligibility_epoch != Some(eligibility_epoch)
        || status.native_scheduling_shape.map(|shape| shape.stamp())
            != apply.native_scheduling_shape.map(|shape| shape.stamp())
    {
        return ServerIpCarrierEvaluation::Stale;
    }
    let (mut snapshot, native_stamp) = match attachment.key.underlay {
        UnderlayProtocol::Udp => {
            let Some(authority) = attachment.native_rate_authority.as_ref() else {
                return ServerIpCarrierEvaluation::Unavailable;
            };
            let scope = crate::model::carrier_rate_authority::CarrierRateAuthorityScope::new(
                attachment.key.path_instance_id,
                PathMetricDirection::ServerToClient,
            );
            let live_shape = match authority.scheduling_shape_snapshot(scope) {
                Ok(shape) => shape,
                Err(error) if error.is_retryable_publication() => {
                    return ServerIpCarrierEvaluation::Stale;
                }
                Err(_) => return ServerIpCarrierEvaluation::Unavailable,
            };
            if apply.native_scheduling_shape.map(|shape| shape.stamp()) != Some(live_shape.stamp())
            {
                return ServerIpCarrierEvaluation::Stale;
            }
            (
                server_native_packet_snapshot(&status, &attachment, live_shape),
                Some(live_shape.stamp()),
            )
        }
        UnderlayProtocol::Tcp => {
            if attachment.native_rate_authority.is_some() {
                return ServerIpCarrierEvaluation::Unavailable;
            }
            (server_tcp_packet_snapshot(Some(&status), &attachment), None)
        }
    };
    snapshot.active_flows = active_load;
    let Some(score) = crate::scheduler::score_path(
        snapshot,
        crate::scheduler::TrafficClass::RealtimeDatagram,
        packet_bytes,
    ) else {
        return ServerIpCarrierEvaluation::Unavailable;
    };
    let flowlet_timeout = crate::model::timing::transport_pto_from_snapshot(Some(snapshot));
    ServerIpCarrierEvaluation::Candidate(RankedServerIpDispatchPlan {
        current_mismatch: basis.current != Some(attachment.key),
        backup: crate::scheduler::path_is_backup(snapshot),
        eta_ms: score.eta_ms,
        config_ordinal: attachment.config_ordinal,
        plan: ServerIpDispatchPlan {
            tunnel_id: basis.tunnel_id,
            tunnel_generation: basis.tunnel_generation,
            dispatch_generation: basis.dispatch_generation,
            attachment,
            status,
            eligibility_epoch,
            native_stamp,
            flowlet_timeout,
        },
    })
}

fn apply_server_ip_dispatch(
    inner: &Arc<ServerIpTunnelInner>,
    principal: &PrincipalId,
    flow: &IpPacketFlowKey,
    packet_id: IpPacketId,
    payload: Bytes,
    plan: ServerIpDispatchPlan,
    budget_permit: Option<IpPacketQueuePermit>,
) -> Result<ServerIpDispatchApply, RuntimeError> {
    let native_authority = plan.attachment.native_rate_authority.clone();
    let native_stamp = plan.native_stamp;
    let structural_apply = |current_authority_shape: Option<
        crate::runtime::path::authority::NativeCarrierSchedulingShapeSnapshot,
    >| {
        plan.attachment.apply_authority.commit_if_current(
            plan.eligibility_epoch,
            native_stamp,
            |_current_registry_shape| {
                let (native_retention_limit_bytes, flowlet_timeout) = match native_stamp {
                    Some(stamp) => {
                        let Some(shape) = current_authority_shape.filter(|shape| {
                            shape.stamp() == stamp
                                && shape.stamp().scope().carrier_instance_id()
                                    == plan.attachment.key.path_instance_id
                                && shape.stamp().scope().direction()
                                    == PathMetricDirection::ServerToClient
                        }) else {
                            return Ok(ServerIpDispatchApply {
                                outcome: ServerIpDispatchOutcome::Stale,
                                expiry: None,
                            });
                        };
                        let snapshot =
                            server_native_packet_snapshot(&plan.status, &plan.attachment, shape);
                        if crate::scheduler::score_path(
                            snapshot,
                            crate::scheduler::TrafficClass::RealtimeDatagram,
                            payload.len(),
                        )
                        .is_none()
                        {
                            return Ok(ServerIpDispatchApply {
                                outcome: ServerIpDispatchOutcome::Stale,
                                expiry: None,
                            });
                        }
                        (
                            Some(
                                shape
                                    .congestion_window()
                                    .max(u64::from(shape.current_mtu())),
                            ),
                            crate::model::timing::transport_pto_from_snapshot(Some(snapshot)),
                        )
                    }
                    None if current_authority_shape.is_none() => (None, plan.flowlet_timeout),
                    None => {
                        return Ok(ServerIpDispatchApply {
                            outcome: ServerIpDispatchOutcome::Stale,
                            expiry: None,
                        });
                    }
                };

                let mut state = inner.state.lock().expect("server IP tunnel lock");
                let Some(tunnel) = state.tunnels.get_mut(principal) else {
                    return Ok(ServerIpDispatchApply {
                        outcome: ServerIpDispatchOutcome::Stale,
                        expiry: None,
                    });
                };
                if tunnel.session_owner.is_retired()
                    || tunnel.tunnel_id != plan.tunnel_id
                    || tunnel.generation != plan.tunnel_generation
                    || tunnel.dispatch_generation != plan.dispatch_generation
                    || !tunnel
                        .attachments
                        .get(&plan.attachment.key)
                        .is_some_and(|attachment| {
                            attachment.attachment_generation
                                == plan.attachment.attachment_generation
                                && Arc::ptr_eq(&attachment.carrier, &plan.attachment.carrier)
                        })
                {
                    return Ok(ServerIpDispatchApply {
                        outcome: ServerIpDispatchOutcome::Stale,
                        expiry: None,
                    });
                }
                let outcome = plan.attachment.carrier.try_send_packet(
                    plan.tunnel_id,
                    packet_id,
                    payload,
                    budget_permit,
                    native_retention_limit_bytes,
                )?;
                match outcome {
                    IpTunnelPacketSendOutcome::Accepted => {
                        tunnel.flows.bind(
                            flow.clone(),
                            plan.attachment.key,
                            Instant::now(),
                            flowlet_timeout,
                        );
                        tunnel.advance_dispatch_generation();
                        Ok(ServerIpDispatchApply {
                            outcome: ServerIpDispatchOutcome::Accepted,
                            expiry: None,
                        })
                    }
                    IpTunnelPacketSendOutcome::Full => Ok(ServerIpDispatchApply {
                        outcome: ServerIpDispatchOutcome::Full,
                        expiry: None,
                    }),
                    IpTunnelPacketSendOutcome::Retired => {
                        let expiry = remove_server_ip_attachment(
                            tunnel,
                            plan.attachment.key,
                            plan.attachment.attachment_generation,
                            &inner.next_retention_epoch,
                            inner.session_retention_timeout,
                        )
                        .map(|retention| server_ip_tunnel_expiry(principal, tunnel, retention));
                        Ok(ServerIpDispatchApply {
                            outcome: ServerIpDispatchOutcome::Retired,
                            expiry,
                        })
                    }
                }
            },
        )
    };

    match (native_authority, native_stamp) {
        (Some(authority), Some(stamp)) => {
            match authority
                .commit_with_current_scheduling_shape(stamp, |shape| structural_apply(Some(shape)))
            {
                Ok(Some(result)) => result,
                Ok(None) | Err(_) => Ok(ServerIpDispatchApply {
                    outcome: ServerIpDispatchOutcome::Stale,
                    expiry: None,
                }),
            }
        }
        (None, None) => structural_apply(None).unwrap_or_else(|| {
            Ok(ServerIpDispatchApply {
                outcome: ServerIpDispatchOutcome::Stale,
                expiry: None,
            })
        }),
        _ => Ok(ServerIpDispatchApply {
            outcome: ServerIpDispatchOutcome::Stale,
            expiry: None,
        }),
    }
}

fn server_native_packet_snapshot(
    status: &crate::runtime::path::ServerCarrierPathStatusSnapshot,
    attachment: &ServerIpAttachment,
    shape: crate::runtime::path::authority::NativeCarrierSchedulingShapeSnapshot,
) -> crate::scheduler::PathSnapshot {
    let scope = shape.stamp().scope();
    let valid = attachment.key.underlay == UnderlayProtocol::Udp
        && status.underlay == UnderlayProtocol::Udp
        && status.path_instance_id == attachment.key.path_instance_id
        && scope.carrier_instance_id() == attachment.key.path_instance_id
        && scope.direction() == PathMetricDirection::ServerToClient
        && shape.rate_bps() > 0;
    let srtt_ms = if valid && !shape.srtt().is_zero() {
        shape.srtt().as_secs_f64() * 1_000.0
    } else {
        crate::runtime::path::model::default_path_srtt_ms()
    };
    let rate_bps = if valid {
        shape.rate_bps() as f64
    } else {
        crate::runtime::path::model::default_path_rate_bps()
    };
    let mut snapshot = crate::scheduler::PathSnapshot::new(
        attachment.key.path_id,
        attachment.key.underlay,
        srtt_ms,
        rate_bps.max(1.0),
    );
    snapshot.policy = status.policy;
    snapshot.peer_usage = status.usage;
    snapshot.state = if valid {
        match status.state {
            PeerPathState::Active => crate::scheduler::PathState::Active,
            PeerPathState::Suspect => crate::scheduler::PathState::Suspect,
            PeerPathState::Draining => crate::scheduler::PathState::Draining,
            PeerPathState::Failed => crate::scheduler::PathState::Failed,
        }
    } else {
        crate::scheduler::PathState::Failed
    };
    if valid {
        snapshot.jitter_ms = shape.rttvar().as_secs_f64() * 1_000.0;
        snapshot.pacing_rate_bps = shape
            .pacing_rate_bps()
            .map_or(shape.rate_bps() as f64, |rate| rate as f64)
            .max(1.0);
        snapshot.bytes_in_flight = shape.bytes_in_flight();
        snapshot.carrier_inflight_limit_bytes = shape
            .congestion_window()
            .max(u64::from(shape.current_mtu()));
        snapshot.app_limited = shape.app_limited();
        snapshot.carrier_delivery_rate_bps = Some(shape.rate_bps() as f64);
        snapshot.confidence = if shape.basis() == CarrierRateAuthorityBasis::NativeOperational {
            1.0
        } else {
            1.0 / f64::from(crate::model::capacity::RELIABLE_INITIAL_WINDOW_PACKETS as u32)
        };
    } else {
        snapshot.confidence = 0.0;
    }
    snapshot
}

fn server_tcp_packet_snapshot(
    status: Option<&crate::runtime::path::ServerCarrierPathStatusSnapshot>,
    attachment: &ServerIpAttachment,
) -> crate::scheduler::PathSnapshot {
    server_tcp_packet_snapshot_at(status, attachment, Instant::now())
}

#[derive(Debug, Clone, Copy)]
struct ServerTcpPacketRateAuthority {
    delivery_rate_bps: f64,
    pacing_rate_bps: f64,
    confidence: f64,
    app_limited: bool,
}

fn server_tcp_packet_snapshot_at(
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
    let measured_rate = server_tcp_packet_rate_authority_at(
        metrics,
        status.and_then(|status| status.carrier_delivery_rate_sample),
        now,
    );
    let startup_rate = startup_metrics.map(|metrics| ServerTcpPacketRateAuthority {
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

fn server_tcp_packet_rate_authority_at(
    metrics: Option<crate::protocol::PathMetrics>,
    carrier_sample: Option<CarrierDeliveryRateSample>,
    now: Instant,
) -> Option<ServerTcpPacketRateAuthority> {
    if let Some(sample) = carrier_sample {
        // Presence is authoritative: an expired sidecar cannot fall through
        // to retained PathMetrics and silently regain scheduling authority.
        if sample.delivery_rate_bps == 0 || sample.observed_at > now || now >= sample.expires_at {
            return None;
        }
        let delivery_rate_bps = sample.delivery_rate_bps as f64;
        return Some(ServerTcpPacketRateAuthority {
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

    None
}

fn prune_dead_attachments(tunnel: &mut ServerLogicalIpTunnel) -> bool {
    let previous_len = tunnel.attachments.len();
    tunnel
        .attachments
        .retain(|_, attachment| attachment.lifetime.upgrade().is_some());
    let attachments = &tunnel.attachments;
    tunnel
        .flows
        .retain_carriers(|carrier| attachments.contains_key(&carrier));
    if tunnel.attachments.len() != previous_len {
        tunnel.advance_dispatch_generation();
    }
    previous_len > 0 && tunnel.attachments.is_empty()
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
    use crate::config::{MppPerformanceConfig, ResourceLimits, ServerSecurityConfig, SharedSecret};
    use crate::model::carrier_rate_authority::CarrierRateAuthorityScope;
    use crate::model::path::PathPolicy;
    use crate::outbound::OutboundConfig;
    use crate::product::{TunL3AddressPlan, TunL3AllocationSpec, TunL3ServerSpec};
    use crate::protocol::PathMetrics;
    use crate::runtime::node::server::{ServerIdentityRuntime, new_identity_runtime};
    use crate::runtime::path::authority::{
        NativeCarrierRateAuthorityHandle, NativeCarrierSchedulingShapeSnapshot,
    };
    use crate::runtime::path::{
        ServerCarrierPathRegistration, ServerCarrierPathStatusSnapshot, ServerLocalPathProperties,
    };
    use std::net::Ipv4Addr;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    #[derive(Debug)]
    struct UnusedCarrier;

    impl ServerIpTunnelCarrier for UnusedCarrier {
        fn try_send_packet(
            &self,
            _tunnel_id: IpTunnelId,
            _packet_id: IpPacketId,
            _payload: Bytes,
            _budget_permit: Option<IpPacketQueuePermit>,
            _native_retention_limit_bytes: Option<u64>,
        ) -> Result<IpTunnelPacketSendOutcome, RuntimeError> {
            panic!("packet carrier is not used by snapshot tests")
        }

        fn close(&self, _tunnel_id: IpTunnelId, _reason: CloseReason) {}
    }

    #[derive(Debug)]
    struct NativeTestCarrier {
        authority: Arc<NativeCarrierRateAuthorityHandle>,
        sends: Arc<AtomicUsize>,
        retention_limits: Arc<Mutex<Vec<u64>>>,
    }

    impl ServerIpTunnelCarrier for NativeTestCarrier {
        fn try_send_packet(
            &self,
            _tunnel_id: IpTunnelId,
            _packet_id: IpPacketId,
            _payload: Bytes,
            budget_permit: Option<IpPacketQueuePermit>,
            native_retention_limit_bytes: Option<u64>,
        ) -> Result<IpTunnelPacketSendOutcome, RuntimeError> {
            assert!(
                budget_permit.is_some(),
                "Native UDP publication must transfer its reserved packet permit",
            );
            self.retention_limits
                .lock()
                .expect("native test retention limits")
                .push(
                    native_retention_limit_bytes
                        .expect("Native UDP publication must carry its exact window bound"),
                );
            self.sends.fetch_add(1, AtomicOrdering::Relaxed);
            Ok(IpTunnelPacketSendOutcome::Accepted)
        }

        fn native_rate_authority(&self) -> Option<Arc<NativeCarrierRateAuthorityHandle>> {
            Some(self.authority.clone())
        }

        fn close(&self, _tunnel_id: IpTunnelId, _reason: CloseReason) {}
    }

    struct NativeDispatchFixture {
        paths: ServerStreamPort,
        registration: ServerCarrierPathRegistration,
        _attachment: AcceptedServerIpTunnel,
        device: ServerIpTunnelDevice,
        authority: Arc<NativeCarrierRateAuthorityHandle>,
        scope: CarrierRateAuthorityScope,
        sends: Arc<AtomicUsize>,
        retention_limits: Arc<Mutex<Vec<u64>>>,
        principal: PrincipalId,
        payload: Bytes,
        flow: IpPacketFlowKey,
    }

    impl NativeDispatchFixture {
        fn new() -> (Self, NativeCarrierSchedulingShapeSnapshot) {
            let security = ServerSecurityConfig::for_test(
                SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
                    .expect("test secret"),
            );
            let ServerIdentityRuntime {
                paths: context,
                reliable_relay: _,
            } = new_identity_runtime(
                Vec::new(),
                OutboundConfig::Direct,
                crate::config::DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
                security.clone(),
                MppPerformanceConfig::default(),
                ResourceLimits::default(),
            );
            let address_plan = TunL3AddressPlan::compile(
                TunL3ServerSpec {
                    interface_name: Some("native-dispatch-test".to_owned()),
                    ipv4_pool: Some("10.88.0.0/24".parse().expect("test IPv4 pool")),
                    ipv4: Some(Ipv4Addr::new(10, 88, 0, 1)),
                    ipv6_pool: None,
                    ipv6: None,
                    mtu: 1_500,
                    allocations: vec![TunL3AllocationSpec {
                        principal_id: PrincipalId::parse("test-peer").expect("test principal"),
                        ipv4: Some(Ipv4Addr::new(10, 88, 0, 2)),
                        ipv6: None,
                        allowed_ips: Vec::new(),
                    }],
                },
                &security.credential_authority,
            )
            .expect("native dispatch address plan");
            let paths = context.reliable_streams.clone();
            let (port, device) = ServerIpTunnelService::build(
                address_plan,
                paths.clone(),
                4,
                16 * 1_500,
                context.session_retention_timeout,
            );
            let registration = paths.register_test_carrier_path(
                SessionId(91),
                UnderlayProtocol::Udp,
                PathId(7),
                ServerLocalPathProperties::default(),
            );
            let scope = CarrierRateAuthorityScope::new(
                registration.path_instance_id(),
                PathMetricDirection::ServerToClient,
            );
            let authority = NativeCarrierRateAuthorityHandle::from_observation_for_test(
                scope,
                8_000_000,
                1,
                7,
                Some(80_000_000),
            )
            .expect("native dispatch authority");
            let shape = authority
                .refresh_scheduling_shape_for_test(
                    scope,
                    1,
                    7,
                    Some(80_000_000),
                    Duration::from_millis(40),
                    Duration::from_millis(4),
                    256_000,
                    32_000,
                    1_400,
                    Some(90_000_000),
                    false,
                )
                .expect("initial native dispatch shape");
            assert!(paths.stage_native_scheduling_shape(&registration, shape));
            let sends = Arc::new(AtomicUsize::new(0));
            let retention_limits = Arc::new(Mutex::new(Vec::new()));
            let attachment = port
                .open(ServerIpTunnelOpenRequest {
                    tunnel_id: IpTunnelId(91),
                    path: &registration,
                    carrier: Arc::new(NativeTestCarrier {
                        authority: authority.clone(),
                        sends: sends.clone(),
                        retention_limits: retention_limits.clone(),
                    }),
                })
                .expect("open native UDP tunnel attachment");
            let payload = native_ipv4_packet([10, 88, 0, 1], [10, 88, 0, 2]);
            let flow = parse_ip_packet(&payload)
                .expect("native test packet")
                .flow_key;
            (
                Self {
                    paths,
                    registration,
                    _attachment: attachment,
                    device,
                    authority,
                    scope,
                    sends,
                    retention_limits,
                    principal: PrincipalId::parse("test-peer").expect("test principal"),
                    payload,
                    flow,
                },
                shape,
            )
        }

        fn plan(&self) -> ServerIpDispatchPlan {
            let (capture, expiry) = capture_server_ip_dispatch(
                &self.device.inner,
                &self.principal,
                &self.flow,
                Instant::now(),
            );
            assert!(expiry.is_none());
            match select_server_carrier(
                &self.paths,
                capture.expect("live native dispatch capture"),
                self.payload.len(),
            ) {
                ServerIpDispatchSelection::Selected(plan) => plan,
                other => panic!("expected live native dispatch plan, got {other:?}"),
            }
        }

        fn reserve(&self) -> (usize, IpPacketQueuePermit) {
            let available = self.device.inner.carrier_packet_budget.available_bytes();
            let permit = self
                .device
                .inner
                .carrier_packet_budget
                .try_reserve(self.payload.len())
                .expect("reserve native packet budget");
            assert!(
                self.device.inner.carrier_packet_budget.available_bytes() < available,
                "the race must occur after a real packet-budget reservation",
            );
            (available, permit)
        }

        fn apply(
            &self,
            plan: ServerIpDispatchPlan,
            permit: IpPacketQueuePermit,
        ) -> ServerIpDispatchApply {
            apply_server_ip_dispatch(
                &self.device.inner,
                &self.principal,
                &self.flow,
                IpPacketId(1),
                self.payload.clone(),
                plan,
                Some(permit),
            )
            .expect("native dispatch apply")
        }

        fn assert_stale_is_side_effect_free(
            &self,
            apply: ServerIpDispatchApply,
            available_before_reservation: usize,
        ) {
            assert_eq!(apply.outcome, ServerIpDispatchOutcome::Stale);
            assert!(apply.expiry.is_none());
            assert_eq!(
                self.device.inner.carrier_packet_budget.available_bytes(),
                available_before_reservation,
                "a rejected publication must refund its packet permit",
            );
            assert_eq!(self.sends.load(AtomicOrdering::Relaxed), 0);
            assert!(
                self.retention_limits
                    .lock()
                    .expect("native test retention limits")
                    .is_empty(),
                "a rejected publication must not reach the carrier",
            );
            let mut state = self.device.inner.state.lock().expect("server tunnel state");
            let tunnel = state
                .tunnels
                .get_mut(&self.principal)
                .expect("native test tunnel");
            assert_eq!(
                tunnel
                    .flows
                    .planned_current(&self.flow, Instant::now(), |_| true),
                None,
                "planning or rejected apply must not bind the packet flow",
            );
        }

        fn stage(&self, shape: NativeCarrierSchedulingShapeSnapshot) {
            assert!(
                self.paths
                    .stage_native_scheduling_shape(&self.registration, shape),
                "new Native generation must advance the registry shape",
            );
        }

        fn assert_fresh_rerank_succeeds_once(
            &self,
            expected_shape: NativeCarrierSchedulingShapeSnapshot,
        ) {
            let plan = self.plan();
            assert_eq!(plan.native_stamp, Some(expected_shape.stamp()));
            let carrier_key = plan.attachment.key;
            let (_available, permit) = self.reserve();
            let apply = self.apply(plan, permit);
            assert_eq!(apply.outcome, ServerIpDispatchOutcome::Accepted);
            assert!(apply.expiry.is_none());
            assert_eq!(self.sends.load(AtomicOrdering::Relaxed), 1);
            assert_eq!(
                *self
                    .retention_limits
                    .lock()
                    .expect("native test retention limits"),
                vec![
                    expected_shape
                        .congestion_window()
                        .max(u64::from(expected_shape.current_mtu()))
                ],
            );
            let mut state = self.device.inner.state.lock().expect("server tunnel state");
            let tunnel = state
                .tunnels
                .get_mut(&self.principal)
                .expect("native test tunnel");
            assert_eq!(
                tunnel
                    .flows
                    .planned_current(&self.flow, Instant::now(), |_| true),
                Some(carrier_key),
                "only the accepted fresh rerank binds the flow",
            );
        }
    }

    fn native_ipv4_packet(source: [u8; 4], destination: [u8; 4]) -> Bytes {
        let mut packet = vec![
            0x45,
            0,
            0,
            24,
            0,
            1,
            0,
            0,
            64,
            17,
            0,
            0,
            source[0],
            source[1],
            source[2],
            source[3],
            destination[0],
            destination[1],
            destination[2],
            destination[3],
            0x12,
            0x34,
            0,
            53,
        ];
        let mut sum = 0_u32;
        for chunk in packet[..20].chunks_exact(2) {
            sum += u32::from(u16::from_be_bytes([chunk[0], chunk[1]]));
        }
        while sum > u32::from(u16::MAX) {
            sum = (sum & u32::from(u16::MAX)) + (sum >> 16);
        }
        packet[10..12].copy_from_slice(&(!(sum as u16)).to_be_bytes());
        Bytes::from(packet)
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
                apply_authority: crate::runtime::path::ServerCarrierPathApplyAuthority::new(),
                native_rate_authority: None,
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
            eligibility_epoch: Some(1),
            native_scheduling_shape: None,
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
    fn native_udp_projection_uses_exact_shape_not_ack_metrics_or_rate_sidecar() {
        let (startup, mut adversarial) = packet_metrics(UnderlayProtocol::Udp);
        adversarial.srtt_us = 2_000_000;
        adversarial.rttvar_us = 1_000_000;
        adversarial.jitter_us = 900_000;
        adversarial.delivery_rate_bps = 900_000_000;
        adversarial.pacing_rate_bps = 1_000_000_000;
        adversarial.loss_ppm = 900_000;
        adversarial.loss_observed = true;
        adversarial.queue_bytes = 9_000_000;
        adversarial.queue_observed = true;
        adversarial.bytes_in_flight = 8_000_000;
        adversarial.bytes_in_flight_observed = true;
        adversarial.inflight_limit_bytes = 7_000_000;
        adversarial.confidence_ppm = 1;
        adversarial.app_limited = true;
        adversarial.has_ack_derived_data_sample = true;
        adversarial.data_sample_count = u32::MAX;
        adversarial.data_sample_bytes = u64::MAX;
        let now = Instant::now();
        let sidecar = CarrierDeliveryRateSample {
            delivery_rate_bps: 1_100_000_000,
            pacing_rate_bps: Some(1_200_000_000),
            sample_count: u32::MAX,
            sample_bytes: u64::MAX,
            delivery_window_covered: true,
            observed_at: now,
            expires_at: now + Duration::from_secs(60),
        };
        let mut status = status(UnderlayProtocol::Udp, adversarial, Some(sidecar));
        let (mut attachment, _lifetime) = attachment(UnderlayProtocol::Udp, startup);
        let scope = CarrierRateAuthorityScope::new(
            attachment.key.path_instance_id,
            PathMetricDirection::ServerToClient,
        );
        let authority = NativeCarrierRateAuthorityHandle::from_observation_for_test(
            scope,
            8_000_000,
            1,
            9,
            Some(83_000_000),
        )
        .expect("native projection authority");
        let shape = authority
            .refresh_scheduling_shape_for_test(
                scope,
                1,
                9,
                Some(83_000_000),
                Duration::from_millis(37),
                Duration::from_millis(6),
                333_000,
                12_000,
                1_400,
                Some(97_000_000),
                false,
            )
            .expect("exact native projection shape");
        attachment.native_rate_authority = Some(authority);
        status.native_scheduling_shape = Some(shape);

        let snapshot = server_native_packet_snapshot(&status, &attachment, shape);
        assert_eq!(snapshot.state, crate::scheduler::PathState::Active);
        assert_eq!(snapshot.delivery_rate_bps, 83_000_000.0);
        assert_eq!(snapshot.carrier_delivery_rate_bps, Some(83_000_000.0));
        assert_eq!(snapshot.pacing_rate_bps, 97_000_000.0);
        assert_eq!(snapshot.srtt_ms, 37.0);
        assert_eq!(snapshot.jitter_ms, 6.0);
        assert_eq!(snapshot.bytes_in_flight, 12_000);
        assert_eq!(snapshot.carrier_inflight_limit_bytes, 333_000);
        assert_eq!(snapshot.loss_rate, 0.0);
        assert_eq!(snapshot.queue_bytes, 0);
        assert_eq!(snapshot.product_progress_rate_bps, None);
        assert!(!snapshot.has_durable_product_progress);
        assert_eq!(snapshot.confidence, 1.0);
        assert!(!snapshot.app_limited);
    }

    #[test]
    fn native_udp_final_commit_uses_current_shape_for_window_and_flowlet_clock() {
        let (fixture, initial_shape) = NativeDispatchFixture::new();
        let plan = fixture.plan();
        assert_eq!(plan.native_stamp, Some(initial_shape.stamp()));
        let planning_timeout = plan.flowlet_timeout;

        // The authority cache is the local-sender source of truth. Reproduce
        // the ordinary cadence gap before its same-stamp projection reaches
        // the registry: window contracts while RTT/PTO grows.
        let current_shape = fixture
            .authority
            .refresh_scheduling_shape_for_test(
                fixture.scope,
                1,
                7,
                Some(80_000_000),
                Duration::from_millis(400),
                Duration::from_millis(100),
                64_000,
                32_000,
                1_400,
                Some(90_000_000),
                false,
            )
            .expect("refresh same-stamp final Native shape");
        assert_eq!(current_shape.stamp(), initial_shape.stamp());
        assert_ne!(current_shape, initial_shape);
        let current_snapshot =
            server_native_packet_snapshot(&plan.status, &plan.attachment, current_shape);
        let current_timeout =
            crate::model::timing::transport_pto_from_snapshot(Some(current_snapshot));
        assert!(current_timeout > planning_timeout);

        let carrier_key = plan.attachment.key;
        let (_available, permit) = fixture.reserve();
        let apply = fixture.apply(plan, permit);
        assert_eq!(apply.outcome, ServerIpDispatchOutcome::Accepted);
        assert_eq!(fixture.sends.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(
            *fixture
                .retention_limits
                .lock()
                .expect("native test retention limits"),
            vec![64_000],
            "queue retention must use the contracted current cwnd",
        );

        let mut state = fixture
            .device
            .inner
            .state
            .lock()
            .expect("server tunnel state");
        let tunnel = state
            .tunnels
            .get_mut(&fixture.principal)
            .expect("native test tunnel");
        let after_planning_timeout = Instant::now()
            .checked_add(planning_timeout + Duration::from_millis(100))
            .expect("flowlet observation instant");
        assert!(after_planning_timeout < Instant::now() + current_timeout);
        assert_eq!(
            tunnel
                .flows
                .planned_current(&fixture.flow, after_planning_timeout, |_| true),
            Some(carrier_key),
            "accepted flowlet must retain the final current PTO, not the stale planning PTO",
        );
    }

    #[test]
    fn structural_transition_after_status_capture_is_classified_stale() {
        let (fixture, _initial_shape) = NativeDispatchFixture::new();
        let (capture, expiry) = capture_server_ip_dispatch(
            &fixture.device.inner,
            &fixture.principal,
            &fixture.flow,
            Instant::now(),
        );
        assert!(expiry.is_none());
        let capture = capture.expect("live native dispatch capture");
        let basis = ServerIpDispatchBasis::from(&capture);
        let (attachment, active_load) = capture
            .attachments
            .into_iter()
            .next()
            .expect("native attachment");
        let status = fixture
            .paths
            .carrier_path_statuses(&[attachment.key.path_identity()])
            .into_iter()
            .next()
            .flatten();
        fixture.registration.set_state(PeerPathState::Suspect);

        assert!(matches!(
            evaluate_server_carrier(
                &basis,
                attachment,
                active_load,
                status,
                fixture.payload.len(),
            ),
            ServerIpCarrierEvaluation::Stale
        ));
    }

    #[test]
    fn native_udp_generation_race_refunds_then_fresh_rerank_publishes_once() {
        let (fixture, _initial_shape) = NativeDispatchFixture::new();
        let stale_plan = fixture.plan();
        let (available, permit) = fixture.reserve();
        fixture
            .authority
            .publish_observation_for_test(1, 7, Some(160_000_000))
            .expect("advance central generation after planning and reservation");

        let stale_apply = fixture.apply(stale_plan, permit);
        fixture.assert_stale_is_side_effect_free(stale_apply, available);

        let fresh_shape = fixture
            .authority
            .refresh_scheduling_shape_for_test(
                fixture.scope,
                1,
                7,
                Some(160_000_000),
                Duration::from_millis(32),
                Duration::from_millis(3),
                512_000,
                48_000,
                1_400,
                Some(170_000_000),
                false,
            )
            .expect("refresh exact newer-generation shape");
        fixture.stage(fresh_shape);
        fixture.assert_fresh_rerank_succeeds_once(fresh_shape);
    }

    #[test]
    fn native_udp_activation_race_refunds_then_fresh_rerank_publishes_once() {
        let (fixture, _initial_shape) = NativeDispatchFixture::new();
        let stale_plan = fixture.plan();
        let (available, permit) = fixture.reserve();
        fixture
            .authority
            .advance_transport_activation_for_test(2)
            .expect("replace native transport activation after reservation");

        let stale_apply = fixture.apply(stale_plan, permit);
        fixture.assert_stale_is_side_effect_free(stale_apply, available);

        fixture
            .authority
            .publish_observation_for_test(2, 3, Some(180_000_000))
            .expect("publish replacement activation");
        let fresh_shape = fixture
            .authority
            .refresh_scheduling_shape_for_test(
                fixture.scope,
                2,
                3,
                Some(180_000_000),
                Duration::from_millis(31),
                Duration::from_millis(3),
                640_000,
                56_000,
                1_400,
                Some(190_000_000),
                false,
            )
            .expect("refresh replacement-activation shape");
        fixture.stage(fresh_shape);
        fixture.assert_fresh_rerank_succeeds_once(fresh_shape);
    }

    #[test]
    fn native_udp_structural_epoch_race_refunds_and_cannot_publish() {
        let (fixture, shape) = NativeDispatchFixture::new();
        let stale_plan = fixture.plan();
        let stale_epoch = stale_plan.eligibility_epoch;
        let (available, permit) = fixture.reserve();
        fixture.registration.set_state(PeerPathState::Suspect);

        let stale_apply = fixture.apply(stale_plan, permit);
        fixture.assert_stale_is_side_effect_free(stale_apply, available);

        fixture.registration.set_state(PeerPathState::Active);
        let restored_plan = fixture.plan();
        assert_ne!(
            restored_plan.eligibility_epoch, stale_epoch,
            "the fresh plan must use the post-transition structural epoch",
        );
        drop(restored_plan);
        fixture.assert_fresh_rerank_succeeds_once(shape);
    }

    #[test]
    fn tcp_packet_snapshot_presence_flags_gate_retained_values_but_not_native_limit() {
        let (startup, mut live) = packet_metrics(UnderlayProtocol::Tcp);
        live.queue_observed = false;
        live.queue_bytes = 41_000;
        live.bytes_in_flight_observed = false;
        live.bytes_in_flight = 73_000;
        live.inflight_limit_bytes = 256_000;
        let retained_status = status(UnderlayProtocol::Tcp, live, None);
        let (attachment, _lifetime) = attachment(UnderlayProtocol::Tcp, startup);

        let snapshot =
            server_tcp_packet_snapshot_at(Some(&retained_status), &attachment, Instant::now());
        assert_eq!(snapshot.queue_bytes, 0);
        assert_eq!(snapshot.bytes_in_flight, 0);
        assert_eq!(snapshot.carrier_inflight_limit_bytes, 256_000);

        let observed = status(
            UnderlayProtocol::Tcp,
            PathMetrics {
                queue_observed: true,
                bytes_in_flight_observed: true,
                ..live
            },
            None,
        );
        let snapshot = server_tcp_packet_snapshot_at(Some(&observed), &attachment, Instant::now());
        assert_eq!(snapshot.queue_bytes, 41_000);
        assert_eq!(snapshot.bytes_in_flight, 73_000);
        assert_eq!(snapshot.carrier_inflight_limit_bytes, 256_000);
    }

    #[test]
    fn tcp_packet_snapshot_sidecar_deadline_is_authoritative() {
        let now = Instant::now();
        let expires_at = now + Duration::from_millis(10);
        let (startup, live) = packet_metrics(UnderlayProtocol::Tcp);
        let sample = CarrierDeliveryRateSample {
            delivery_rate_bps: 120_000_000,
            pacing_rate_bps: Some(140_000_000),
            sample_count: 3,
            sample_bytes: 192 * 1024,
            delivery_window_covered: true,
            observed_at: now,
            expires_at,
        };
        let status = status(UnderlayProtocol::Tcp, live, Some(sample));
        let (attachment, _lifetime) = attachment(UnderlayProtocol::Tcp, startup);

        let fresh = server_tcp_packet_snapshot_at(
            Some(&status),
            &attachment,
            expires_at - Duration::from_nanos(1),
        );
        assert_eq!(fresh.delivery_rate_bps, 120_000_000.0);
        assert_eq!(fresh.pacing_rate_bps, 140_000_000.0);
        assert_eq!(fresh.confidence, 0.7);
        assert!(!fresh.app_limited);
        assert_eq!(fresh.carrier_delivery_rate_bps, Some(120_000_000.0));

        // At the exact deadline, an expired sidecar must not fall through to
        // retained diagnostic PathMetrics.
        let expired = server_tcp_packet_snapshot_at(Some(&status), &attachment, expires_at);
        assert_startup_rate_bundle(expired);
    }

    #[test]
    fn tcp_path_metrics_never_replace_missing_sidecar() {
        let (startup, live) = packet_metrics(UnderlayProtocol::Tcp);
        let status = status(UnderlayProtocol::Tcp, live, None);
        let (attachment, _lifetime) = attachment(UnderlayProtocol::Tcp, startup);

        let snapshot = server_tcp_packet_snapshot_at(Some(&status), &attachment, Instant::now());
        assert_startup_rate_bundle(snapshot);
        assert_eq!(snapshot.carrier_delivery_rate_bps, None);
    }
}
