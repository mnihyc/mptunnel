//! Client packet-device ownership and transport-neutral carrier attachment.
//!
//! This service owns complete inner IP packets only. It neither enters the
//! Product router nor installs host routes, DNS, firewall policy, or NAT.

use super::flow::PacketFlowTable;
use crate::ingress::TunL3IngressConfig;
use crate::model::path::{CarrierPathInstanceId, RelayPathKey};
use crate::model::timing::{path_open_timeout, transport_pto_from_snapshot};
use crate::model::tun_l3::{IpPacketFlowKey, parse_ip_packet};
use crate::platform::{PacketDeviceConfig, PacketDeviceProvider};
use crate::protocol::{CloseReason, Frame, IpPacketId, IpTunnelId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::quic::ip_tunnel::{
    ClientUdpIpTunnelAttachment, ClientUdpIpTunnelOpenOutcome,
};
use crate::runtime::path::tcp::client::ClientTcpIpTunnelAttachment;
use crate::runtime::path::{ClientPathContext, PacketPathAttachment, PacketPathSelectionInput};
use crate::runtime::readiness::RequiredServiceReadiness;
use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tun_rs::async_framed::{BytesCodec, DeviceFramed};

const MIN_TUN_L3_MTU: usize = 576;
const LIFECYCLE_RECORDS_PER_CARRIER: usize = 4;

#[derive(Debug)]
pub(in crate::runtime) struct ClientIpTunnelEvent {
    pub(in crate::runtime) path: RelayPathKey,
    pub(in crate::runtime) path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime) frame: Frame,
}

#[derive(Debug, Clone, Default)]
pub(in crate::runtime) struct ClientIpTunnelHub {
    sink: Arc<Mutex<Option<ClientIpTunnelSink>>>,
}

#[derive(Debug, Clone)]
struct ClientIpTunnelSink {
    events: mpsc::UnboundedSender<ClientIpTunnelInput>,
    packet_budget: Arc<Semaphore>,
    minimum_packet_charge: usize,
    lifecycle_slots: Arc<Semaphore>,
}

struct ClientIpPacketBudget {
    _permit: OwnedSemaphorePermit,
}

enum ClientIpTunnelInput {
    Lifecycle {
        event: ClientIpTunnelEvent,
        _slot: OwnedSemaphorePermit,
    },
    Packet {
        event: ClientIpTunnelEvent,
        budget: ClientIpPacketBudget,
    },
    CarrierUpdate {
        update: ClientIpCarrierUpdate,
        processed: oneshot::Sender<()>,
        _slot: OwnedSemaphorePermit,
    },
}

struct BudgetedTunWrite {
    payload: Bytes,
    _budget: ClientIpPacketBudget,
}

pub(in crate::runtime) struct ClientIpTunnelHubRegistration {
    hub: ClientIpTunnelHub,
}

impl ClientIpTunnelHub {
    fn register(
        &self,
        sink: ClientIpTunnelSink,
    ) -> Result<ClientIpTunnelHubRegistration, RuntimeError> {
        let mut current = self.sink.lock().expect("client IP tunnel hub lock");
        if current.is_some() {
            return Err(RuntimeError::Protocol(
                "MPP outbound already owns a TUN-L3 ingress",
            ));
        }
        *current = Some(sink);
        Ok(ClientIpTunnelHubRegistration { hub: self.clone() })
    }

    pub(in crate::runtime) fn route(&self, event: ClientIpTunnelEvent) -> Result<(), RuntimeError> {
        let sink = self
            .sink
            .lock()
            .expect("client IP tunnel hub lock")
            .clone()
            .ok_or(RuntimeError::Protocol("TUN-L3 ingress is not active"))?;
        sink.route_event(event)
    }
}

impl ClientIpTunnelSink {
    fn new(
        events: mpsc::UnboundedSender<ClientIpTunnelInput>,
        packet_budget: usize,
        lifecycle_slots: usize,
    ) -> Self {
        let packet_budget = packet_budget.clamp(1, Semaphore::MAX_PERMITS);
        Self {
            events,
            packet_budget: Arc::new(Semaphore::new(packet_budget)),
            minimum_packet_charge: MIN_TUN_L3_MTU.min(packet_budget),
            lifecycle_slots: Arc::new(Semaphore::new(
                lifecycle_slots.clamp(1, Semaphore::MAX_PERMITS),
            )),
        }
    }

    fn route_event(&self, event: ClientIpTunnelEvent) -> Result<(), RuntimeError> {
        if let Frame::IpPacket { payload, .. } = &event.frame {
            let charge = payload.len().max(self.minimum_packet_charge);
            let permits = u32::try_from(charge).map_err(|_| RuntimeError::SenderServiceBlocked)?;
            let permit = self
                .packet_budget
                .clone()
                .try_acquire_many_owned(permits)
                .map_err(|_| RuntimeError::SenderServiceBlocked)?;
            self.events
                .send(ClientIpTunnelInput::Packet {
                    event,
                    budget: ClientIpPacketBudget { _permit: permit },
                })
                .map_err(|_| RuntimeError::Protocol("TUN-L3 packet event sink closed"))
        } else {
            let slot = self
                .lifecycle_slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| RuntimeError::SenderServiceBlocked)?;
            self.events
                .send(ClientIpTunnelInput::Lifecycle { event, _slot: slot })
                .map_err(|_| RuntimeError::Protocol("TUN-L3 lifecycle event sink closed"))
        }
    }

    async fn send_update(&self, update: ClientIpCarrierUpdate) -> Result<(), ()> {
        let slot = self
            .lifecycle_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ())?;
        let (processed, completion) = oneshot::channel();
        self.events
            .send(ClientIpTunnelInput::CarrierUpdate {
                update,
                processed,
                _slot: slot,
            })
            .map_err(|_| ())?;
        completion.await.map_err(|_| ())
    }
}

impl Drop for ClientIpTunnelHubRegistration {
    fn drop(&mut self) {
        self.hub
            .sink
            .lock()
            .expect("client IP tunnel hub lock")
            .take();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ClientIpCarrierKey {
    path: RelayPathKey,
    path_instance_id: CarrierPathInstanceId,
}

enum ClientIpCarrier {
    Tcp(Arc<ClientTcpIpTunnelAttachment>),
    Quic(Arc<ClientUdpIpTunnelAttachment>),
}

impl ClientIpCarrier {
    fn try_send(&self, packet_id: IpPacketId, payload: Bytes) -> Result<(), RuntimeError> {
        match self {
            Self::Tcp(attachment) => attachment.try_send(packet_id, payload),
            Self::Quic(attachment) => attachment.try_send(packet_id, payload),
        }
    }
}

struct ClientIpCarrierState {
    carrier: ClientIpCarrier,
    ready: bool,
}

enum ClientIpCarrierUpdate {
    Attached {
        key: ClientIpCarrierKey,
        carrier: ClientIpCarrier,
    },
    Retired {
        key: ClientIpCarrierKey,
    },
}

struct ClientIpCarrierSupervisors {
    tasks: Vec<tokio::task::JoinHandle<()>>,
    close_signals: HashMap<RelayPathKey, watch::Sender<Option<CloseReason>>>,
}

impl ClientIpCarrierSupervisors {
    fn signal_close(&self, path: RelayPathKey, reason: CloseReason) {
        if let Some(signal) = self.close_signals.get(&path) {
            signal.send_replace(Some(reason));
        }
    }
}

impl Drop for ClientIpCarrierSupervisors {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClientIpTunnelParameters {
    mtu: u16,
    addresses: Vec<IpAddr>,
}

impl ClientIpTunnelParameters {
    fn from_ready(mtu: u16, mut addresses: Vec<IpAddr>) -> Result<Self, RuntimeError> {
        if usize::from(mtu) < MIN_TUN_L3_MTU
            || (addresses.iter().any(IpAddr::is_ipv6) && mtu < 1280)
        {
            return Err(RuntimeError::Protocol(
                "server returned an invalid TUN-L3 MTU",
            ));
        }
        if addresses.is_empty()
            || addresses.iter().any(|address| match address {
                IpAddr::V4(address) => {
                    address.is_unspecified()
                        || address.is_multicast()
                        || *address == Ipv4Addr::BROADCAST
                }
                IpAddr::V6(address) => address.is_unspecified() || address.is_multicast(),
            })
            || addresses.iter().filter(|address| address.is_ipv4()).count() > 1
            || addresses.iter().filter(|address| address.is_ipv6()).count() > 1
        {
            return Err(RuntimeError::Protocol(
                "server returned an invalid TUN-L3 host allocation",
            ));
        }
        addresses.sort_unstable();
        let unique = addresses.iter().copied().collect::<HashSet<_>>();
        if unique.len() != addresses.len() {
            return Err(RuntimeError::Protocol(
                "server returned duplicate TUN-L3 addresses",
            ));
        }
        Ok(Self { mtu, addresses })
    }

    fn ipv4(&self) -> Option<Ipv4Addr> {
        self.addresses.iter().find_map(|address| match address {
            IpAddr::V4(address) => Some(*address),
            IpAddr::V6(_) => None,
        })
    }

    fn ipv6(&self) -> Option<Ipv6Addr> {
        self.addresses.iter().find_map(|address| match address {
            IpAddr::V4(_) => None,
            IpAddr::V6(address) => Some(*address),
        })
    }
}

struct ClientIpTunnelState {
    tunnel_id: IpTunnelId,
    parameters: Option<ClientIpTunnelParameters>,
    carriers: HashMap<ClientIpCarrierKey, ClientIpCarrierState>,
    deferred_ready: HashMap<ClientIpCarrierKey, ClientIpTunnelParameters>,
    flows: PacketFlowTable<ClientIpCarrierKey>,
    received_packet_ids: crate::runtime::recent_ids::RecentIdCache<IpPacketId>,
    next_packet_id: u64,
}

impl ClientIpTunnelState {
    fn new(tunnel_id: IpTunnelId, capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            tunnel_id,
            parameters: None,
            carriers: HashMap::new(),
            deferred_ready: HashMap::new(),
            flows: PacketFlowTable::new(capacity),
            received_packet_ids: crate::runtime::recent_ids::RecentIdCache::new(
                capacity.saturating_mul(2),
            ),
            next_packet_id: 1,
        }
    }

    fn set_packet_capacity(&mut self, capacity: usize) {
        let capacity = capacity.max(1);
        self.flows.set_capacity(capacity);
        self.received_packet_ids =
            crate::runtime::recent_ids::RecentIdCache::new(capacity.saturating_mul(2));
    }

    fn apply_update(&mut self, update: ClientIpCarrierUpdate) -> Result<(), RuntimeError> {
        match update {
            ClientIpCarrierUpdate::Attached { key, carrier } => {
                let ready = if let Some(parameters) = self.deferred_ready.remove(&key) {
                    self.accept_parameters(parameters)?;
                    true
                } else {
                    false
                };
                self.carriers
                    .insert(key, ClientIpCarrierState { carrier, ready });
            }
            ClientIpCarrierUpdate::Retired { key } => self.remove_carrier(key),
        }
        Ok(())
    }

    fn handle_ready(
        &mut self,
        key: ClientIpCarrierKey,
        parameters: ClientIpTunnelParameters,
    ) -> Result<(), RuntimeError> {
        self.accept_parameters(parameters.clone())?;
        if let Some(carrier) = self.carriers.get_mut(&key) {
            carrier.ready = true;
        } else {
            self.deferred_ready.insert(key, parameters);
        }
        Ok(())
    }

    fn accept_parameters(
        &mut self,
        parameters: ClientIpTunnelParameters,
    ) -> Result<(), RuntimeError> {
        if self
            .parameters
            .as_ref()
            .is_some_and(|current| current != &parameters)
        {
            return Err(RuntimeError::Protocol(
                "TUN-L3 carrier attachments returned different allocations",
            ));
        }
        self.parameters.get_or_insert(parameters);
        Ok(())
    }

    fn remove_carrier(&mut self, key: ClientIpCarrierKey) {
        self.carriers.remove(&key);
        self.deferred_ready.remove(&key);
        self.flows.remove_carrier(key);
    }

    fn has_ready_carrier(&self) -> bool {
        self.carriers.values().any(|carrier| carrier.ready)
    }

    fn receive_packet(
        &mut self,
        key: ClientIpCarrierKey,
        packet_id: IpPacketId,
        payload: Bytes,
    ) -> Option<Bytes> {
        let parameters = self.parameters.as_ref()?;
        if !self.carriers.get(&key).is_some_and(|carrier| carrier.ready)
            || payload.len() > usize::from(parameters.mtu)
            || parse_ip_packet(&payload).is_err()
            || self.received_packet_ids.contains(&packet_id)
        {
            return None;
        }
        self.received_packet_ids.insert(packet_id);
        Some(payload)
    }

    fn send_packet(
        &mut self,
        context: &ClientPathContext,
        payload: Bytes,
    ) -> Result<bool, RuntimeError> {
        let parameters = self.parameters.as_ref().ok_or(RuntimeError::Protocol(
            "TUN-L3 packet device started without allocation",
        ))?;
        if payload.len() > usize::from(parameters.mtu) {
            return Ok(false);
        }
        let metadata = match parse_ip_packet(&payload) {
            Ok(metadata) => metadata,
            Err(_) => return Ok(false),
        };
        let now = Instant::now();
        let carrier =
            self.current_or_select_carrier(context, &metadata.flow_key, payload.len(), now);
        let Some(carrier) = carrier else {
            return Ok(false);
        };
        let packet_id = IpPacketId(self.next_packet_id);
        self.next_packet_id = self.next_packet_id.wrapping_add(1).max(1);
        let result = self
            .carriers
            .get(&carrier)
            .ok_or(RuntimeError::ReliablePathRetired)?
            .carrier
            .try_send(packet_id, payload.clone());
        match result {
            Ok(()) => Ok(true),
            Err(RuntimeError::SenderServiceBlocked) => Ok(false),
            Err(RuntimeError::ReliablePathRetired)
            | Err(RuntimeError::ReliablePathSessionClosed) => {
                self.remove_carrier(carrier);
                let Some(replacement) =
                    self.select_carrier(context, &metadata.flow_key, payload.len(), now)
                else {
                    return Ok(false);
                };
                match self
                    .carriers
                    .get(&replacement)
                    .ok_or(RuntimeError::ReliablePathRetired)?
                    .carrier
                    .try_send(packet_id, payload)
                {
                    Ok(()) => Ok(true),
                    Err(RuntimeError::SenderServiceBlocked) => Ok(false),
                    Err(RuntimeError::ReliablePathRetired)
                    | Err(RuntimeError::ReliablePathSessionClosed) => {
                        self.remove_carrier(replacement);
                        Ok(false)
                    }
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }

    fn current_or_select_carrier(
        &mut self,
        context: &ClientPathContext,
        flow: &IpPacketFlowKey,
        packet_bytes: usize,
        now: Instant,
    ) -> Option<ClientIpCarrierKey> {
        let carriers = &self.carriers;
        if let Some(current) = self.flows.current(flow, now, |carrier| {
            carriers.get(&carrier).is_some_and(|carrier| carrier.ready)
        }) {
            return Some(current);
        }
        self.select_carrier(context, flow, packet_bytes, now)
    }

    fn select_carrier(
        &mut self,
        context: &ClientPathContext,
        flow: &IpPacketFlowKey,
        packet_bytes: usize,
        now: Instant,
    ) -> Option<ClientIpCarrierKey> {
        let inputs = self
            .carriers
            .iter()
            .filter(|(_, carrier)| carrier.ready)
            .map(|(key, _)| PacketPathSelectionInput {
                attachment: PacketPathAttachment {
                    key: key.path,
                    path_instance_id: key.path_instance_id,
                },
                active_flows: self.flows.active_load_for(*key, flow),
            })
            .collect::<Vec<_>>();
        let candidate = context
            .ordered_packet_path_candidates(&inputs, packet_bytes)
            .into_iter()
            .next()?;
        let carrier = ClientIpCarrierKey {
            path: candidate.attachment.key,
            path_instance_id: candidate.attachment.path_instance_id,
        };
        let flowlet_timeout = transport_pto_from_snapshot(Some(candidate.snapshot));
        self.flows.bind(flow.clone(), carrier, now, flowlet_timeout);
        Some(carrier)
    }
}

pub(in crate::runtime) async fn run_client_tun_l3(
    inbound: String,
    config: TunL3IngressConfig,
    context: ClientPathContext,
    packet_devices: Arc<dyn PacketDeviceProvider>,
    readiness: RequiredServiceReadiness,
) -> Result<(), RuntimeError> {
    let tunnel_id = IpTunnelId(crate::runtime::identity::random_u64()?);
    let carrier_count = context
        .tcp_sessions
        .len()
        .saturating_add(context.udp_sessions.len())
        .max(1);
    let packet_bytes = context.mux_limits.max_datagram_queue_bytes;
    let lifecycle_slots = carrier_count.saturating_mul(LIFECYCLE_RECORDS_PER_CARRIER);
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let event_sink = ClientIpTunnelSink::new(events_tx, packet_bytes, lifecycle_slots);
    let _registration = context.ip_tunnels.register(event_sink.clone())?;
    let supervisors = spawn_carrier_supervisors(&context, tunnel_id, event_sink);
    let mut state = ClientIpTunnelState::new(tunnel_id, 1);

    while state.parameters.is_none() || !state.has_ready_carrier() {
        let Some(input) = events_rx.recv().await else {
            return Err(RuntimeError::Protocol("TUN-L3 carrier event source closed"));
        };
        match input {
            ClientIpTunnelInput::Lifecycle { event, .. } => {
                let _ = handle_client_event(&mut state, &supervisors, event)?;
            }
            ClientIpTunnelInput::Packet { event, .. } => {
                let _ = handle_client_event(&mut state, &supervisors, event)?;
            }
            ClientIpTunnelInput::CarrierUpdate {
                update, processed, ..
            } => {
                state.apply_update(update)?;
                let _ = processed.send(());
            }
        }
    }

    let parameters = state
        .parameters
        .clone()
        .expect("TUN-L3 parameters established before packet device");
    state.set_packet_capacity(
        context
            .mux_limits
            .max_datagram_queue_bytes
            .checked_div(usize::from(parameters.mtu))
            .unwrap_or(0)
            .max(1),
    );
    let device = packet_devices
        .open(&PacketDeviceConfig {
            interface_name: config.interface_name.as_deref(),
            ipv4: parameters.ipv4(),
            ipv4_prefix: 32,
            ipv4_gateway: None,
            ipv6: parameters.ipv6(),
            ipv6_prefix: 128,
            mtu: parameters.mtu,
        })
        .map_err(RuntimeError::TunDevice)?;
    let interface = config
        .interface_name
        .as_deref()
        .unwrap_or("host-selected interface");
    let (device, mut managed) = device.into_parts();
    let framed = DeviceFramed::new(device, BytesCodec::new());
    let (mut tun_sink, mut tun_stream) = framed.split();
    let (tun_writes, mut pending_tun_writes) = mpsc::unbounded_channel::<BudgetedTunWrite>();
    let mut tun_writer = tokio::task::JoinSet::new();
    tun_writer.spawn(async move {
        while let Some(write) = pending_tun_writes.recv().await {
            tun_sink
                .send(BytesMut::from(write.payload.as_ref()))
                .await?;
        }
        Ok::<(), RuntimeError>(())
    });
    if let Some(managed) = managed.as_mut() {
        managed.signal_ready();
    }
    crate::observability::emit_lifecycle(
        crate::config::LogLevel::Info,
        "inbound",
        "ready",
        format_args!(
            "{inbound}: TUN-L3 packet ingress ready on {interface}; {} carrier attachment(s)",
            state
                .carriers
                .values()
                .filter(|carrier| carrier.ready)
                .count()
        ),
    );
    readiness.ready();

    loop {
        tokio::select! {
            biased;
            input = events_rx.recv() => {
                let Some(input) = input else {
                    return Err(RuntimeError::Protocol("TUN-L3 carrier event source closed"));
                };
                match input {
                    ClientIpTunnelInput::Lifecycle { event, .. } => {
                        let _ = handle_client_event(&mut state, &supervisors, event)?;
                    }
                    ClientIpTunnelInput::Packet { event, budget } => {
                        if let Some(payload) = handle_client_event(&mut state, &supervisors, event)? {
                            tun_writes
                                .send(BudgetedTunWrite {
                                    payload,
                                    _budget: budget,
                                })
                                .map_err(|_| RuntimeError::Protocol("TUN-L3 device packet sink closed"))?;
                        }
                    }
                    ClientIpTunnelInput::CarrierUpdate {
                        update, processed, ..
                    } => {
                        state.apply_update(update)?;
                        let _ = processed.send(());
                    }
                }
            }
            packet = tun_stream.next() => {
                let Some(packet) = packet else {
                    return Err(RuntimeError::Protocol("TUN-L3 device packet source closed"));
                };
                let packet = packet?;
                let _ = state.send_packet(&context, packet.freeze())?;
            }
            result = tun_writer.join_next() => {
                return match result {
                    Some(Ok(result)) => result,
                    Some(Err(error)) => Err(RuntimeError::TaskJoin(error)),
                    None => Err(RuntimeError::Protocol("TUN-L3 device writer stopped")),
                };
            }
        }
    }
}

fn spawn_carrier_supervisors(
    context: &ClientPathContext,
    tunnel_id: IpTunnelId,
    events: ClientIpTunnelSink,
) -> ClientIpCarrierSupervisors {
    let mut tasks = Vec::with_capacity(
        context
            .tcp_sessions
            .len()
            .saturating_add(context.udp_sessions.len()),
    );
    let mut close_signals = HashMap::with_capacity(tasks.capacity());
    for (index, session) in context.tcp_sessions.iter().cloned().enumerate() {
        let path = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index,
        };
        let (close, close_events) = watch::channel(None);
        close_signals.insert(path, close);
        let context = context.clone();
        let events = events.clone();
        tasks.push(tokio::spawn(run_tcp_carrier_supervisor(
            context,
            path,
            session,
            tunnel_id,
            events,
            close_events,
        )));
    }
    for (index, session) in context.udp_sessions.iter().cloned().enumerate() {
        let path = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index,
        };
        let (close, close_events) = watch::channel(None);
        close_signals.insert(path, close);
        let context = context.clone();
        let events = events.clone();
        tasks.push(tokio::spawn(run_udp_carrier_supervisor(
            context,
            path,
            session,
            tunnel_id,
            events,
            close_events,
        )));
    }
    ClientIpCarrierSupervisors {
        tasks,
        close_signals,
    }
}

async fn run_udp_carrier_supervisor(
    context: ClientPathContext,
    path: RelayPathKey,
    session: crate::runtime::path::quic::client::ClientUdpPathSessionHandle,
    tunnel_id: IpTunnelId,
    events: ClientIpTunnelSink,
    mut close_events: watch::Receiver<Option<CloseReason>>,
) {
    loop {
        let snapshot = context.reliable_path_snapshot(path);
        let open_deadline = tokio::time::Instant::now()
            + path_open_timeout(snapshot, context.reliable_path_rtt_is_observed(path));
        match session
            .open_ip_tunnel_attachment(tunnel_id, open_deadline)
            .await
        {
            Ok(ClientUdpIpTunnelOpenOutcome::Attached { attachment, start }) => {
                let key = ClientIpCarrierKey {
                    path,
                    path_instance_id: attachment.path_instance_id(),
                };
                let attachment = Arc::new(attachment);
                if events
                    .send_update(ClientIpCarrierUpdate::Attached {
                        key,
                        carrier: ClientIpCarrier::Quic(attachment.clone()),
                    })
                    .await
                    .is_err()
                {
                    return;
                }
                if start.send(()).is_err() {
                    let _ = events
                        .send_update(ClientIpCarrierUpdate::Retired { key })
                        .await;
                    return;
                }
                let close_reason = tokio::select! {
                    _ = attachment.wait_retired() => None,
                    changed = close_events.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        *close_events.borrow_and_update()
                    }
                };
                if events
                    .send_update(ClientIpCarrierUpdate::Retired { key })
                    .await
                    .is_err()
                {
                    return;
                }
                if close_reason.is_some_and(|reason| reason != CloseReason::Normal) {
                    session
                        .wait_for_connection_instance_change(key.path_instance_id)
                        .await;
                }
            }
            Ok(ClientUdpIpTunnelOpenOutcome::Rejected { path_instance_id }) => {
                crate::observability::process_event!(
                    Warn,
                    "tun_l3",
                    "attachment_rejected",
                    "TUN-L3 attachment on QUIC path {} was rejected by the peer",
                    client_tun_l3_path_name(&context, path),
                );
                session
                    .wait_for_connection_instance_change(path_instance_id)
                    .await;
            }
            Err(error) => {
                crate::observability::process_event!(
                    Warn,
                    "tun_l3",
                    "attachment_retry",
                    "TUN-L3 attachment on QUIC path {} failed: {error}; retrying",
                    client_tun_l3_path_name(&context, path),
                );
                let retry = transport_pto_from_snapshot(context.reliable_path_snapshot(path));
                tokio::select! {
                    _ = tokio::time::sleep(retry) => {}
                    changed = close_events.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
            }
        }
    }
}

async fn run_tcp_carrier_supervisor(
    context: ClientPathContext,
    path: RelayPathKey,
    session: crate::runtime::path::tcp::client::ClientTcpPathSessionHandle,
    tunnel_id: IpTunnelId,
    events: ClientIpTunnelSink,
    mut close_events: watch::Receiver<Option<CloseReason>>,
) {
    loop {
        let snapshot = context.reliable_path_snapshot(path);
        let open_deadline = tokio::time::Instant::now()
            + path_open_timeout(snapshot, context.reliable_path_rtt_is_observed(path));
        match session
            .prepare_ip_tunnel_attachment(tunnel_id, open_deadline)
            .await
        {
            Ok(attachment) => {
                let key = ClientIpCarrierKey {
                    path,
                    path_instance_id: attachment.path_instance_id(),
                };
                let attachment = Arc::new(attachment);
                if events
                    .send_update(ClientIpCarrierUpdate::Attached {
                        key,
                        carrier: ClientIpCarrier::Tcp(attachment.clone()),
                    })
                    .await
                    .is_err()
                {
                    return;
                }
                if let Err(error) = attachment.start(open_deadline).await {
                    if events
                        .send_update(ClientIpCarrierUpdate::Retired { key })
                        .await
                        .is_err()
                    {
                        return;
                    }
                    crate::observability::process_event!(
                        Warn,
                        "tun_l3",
                        "attachment_retry",
                        "TUN-L3 attachment on TCP path {} failed: {error}; retrying",
                        client_tun_l3_path_name(&context, path),
                    );
                    let retry = transport_pto_from_snapshot(context.reliable_path_snapshot(path));
                    tokio::select! {
                        _ = tokio::time::sleep(retry) => {}
                        changed = close_events.changed() => {
                            if changed.is_err() {
                                return;
                            }
                        }
                    }
                    continue;
                }
                let close_reason = tokio::select! {
                    _ = attachment.wait_retired() => None,
                    changed = close_events.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        *close_events.borrow_and_update()
                    }
                };
                if events
                    .send_update(ClientIpCarrierUpdate::Retired { key })
                    .await
                    .is_err()
                {
                    return;
                }
                if close_reason.is_some_and(|reason| reason != CloseReason::Normal) {
                    session
                        .wait_for_connection_instance_change(key.path_instance_id)
                        .await;
                }
            }
            Err(error) => {
                crate::observability::process_event!(
                    Warn,
                    "tun_l3",
                    "attachment_retry",
                    "TUN-L3 attachment on TCP path {} failed: {error}; retrying",
                    client_tun_l3_path_name(&context, path),
                );
                let retry = transport_pto_from_snapshot(context.reliable_path_snapshot(path));
                tokio::select! {
                    _ = tokio::time::sleep(retry) => {}
                    changed = close_events.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
            }
        }
    }
}

fn client_tun_l3_path_name(context: &ClientPathContext, path: RelayPathKey) -> &str {
    match path.underlay {
        UnderlayProtocol::Tcp => context.tcp_path_name(path.index),
        UnderlayProtocol::Udp => context.udp_path_names.get(path.index).map(String::as_str),
    }
    .unwrap_or("unknown")
}

fn handle_client_event(
    state: &mut ClientIpTunnelState,
    supervisors: &ClientIpCarrierSupervisors,
    event: ClientIpTunnelEvent,
) -> Result<Option<Bytes>, RuntimeError> {
    let key = ClientIpCarrierKey {
        path: event.path,
        path_instance_id: event.path_instance_id,
    };
    match event.frame {
        Frame::IpTunnelReady {
            tunnel_id,
            mtu,
            addresses,
        } if tunnel_id == state.tunnel_id => {
            state.handle_ready(key, ClientIpTunnelParameters::from_ready(mtu, addresses)?)?;
            Ok(None)
        }
        Frame::IpPacket {
            tunnel_id,
            packet_id,
            payload,
        } if tunnel_id == state.tunnel_id => Ok(state.receive_packet(key, packet_id, payload)),
        Frame::IpTunnelClose { tunnel_id, reason } if tunnel_id == state.tunnel_id => {
            state.remove_carrier(key);
            supervisors.signal_close(event.path, reason);
            Ok(None)
        }
        Frame::IpTunnelReady { .. } | Frame::IpPacket { .. } | Frame::IpTunnelClose { .. } => Err(
            RuntimeError::Protocol("TUN-L3 carrier returned a different tunnel identity"),
        ),
        _ => Err(RuntimeError::Protocol(
            "TUN-L3 event port received a non-packet frame",
        )),
    }
}

#[cfg(test)]
#[path = "tests_client.rs"]
mod tests;
