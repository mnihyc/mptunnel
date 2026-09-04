//! Client packet-device ownership and transport-neutral carrier attachment.
//!
//! This service owns complete inner IP packets only. It neither enters the
//! Product router nor installs host routes, DNS, firewall policy, or NAT.

use super::flow::PacketFlowTable;
use super::queue::{IpPacketQueueBudget, IpPacketQueuePermit};
use crate::ingress::TunL3IngressConfig;
use crate::model::carrier_rate_authority::CarrierRateAuthorityScope;
use crate::model::path::{CarrierPathInstanceId, RelayPathKey};
use crate::model::timing::{path_open_timeout, transport_pto_from_snapshot};
use crate::model::tun_l3::{IpPacketFlowKey, parse_ip_packet};
use crate::platform::{PacketDeviceConfig, PacketDeviceProvider};
use crate::protocol::{
    CloseReason, Frame, IpPacketId, IpTunnelId, PathMetricDirection, UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::model::PacketPathCandidate;
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
use tokio::sync::{OwnedSemaphorePermit, mpsc, oneshot, watch};
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
    packet_budget: IpPacketQueueBudget,
    lifecycle_slots: Arc<tokio::sync::Semaphore>,
}

enum ClientIpTunnelInput {
    Lifecycle {
        event: ClientIpTunnelEvent,
        _slot: OwnedSemaphorePermit,
    },
    Packet {
        event: ClientIpTunnelEvent,
        budget: IpPacketQueuePermit,
    },
    CarrierUpdate {
        update: ClientIpCarrierUpdate,
        processed: oneshot::Sender<()>,
        _slot: OwnedSemaphorePermit,
    },
}

struct BudgetedTunWrite {
    payload: Bytes,
    _budget: IpPacketQueuePermit,
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
        Self {
            events,
            packet_budget: IpPacketQueueBudget::new(packet_budget),
            lifecycle_slots: Arc::new(tokio::sync::Semaphore::new(
                lifecycle_slots.clamp(1, tokio::sync::Semaphore::MAX_PERMITS),
            )),
        }
    }

    fn route_event(&self, event: ClientIpTunnelEvent) -> Result<(), RuntimeError> {
        if let Frame::IpPacket { payload, .. } = &event.frame {
            let permit = self.packet_budget.try_reserve(payload.len())?;
            self.events
                .send(ClientIpTunnelInput::Packet {
                    event,
                    budget: permit,
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

#[derive(Clone)]
enum ClientIpCarrier {
    Tcp(Arc<ClientTcpIpTunnelAttachment>),
    Quic(Arc<ClientUdpIpTunnelAttachment>),
    #[cfg(test)]
    NativeTest {
        authority: Arc<crate::runtime::path::authority::NativeCarrierRateAuthorityHandle>,
        accepted: Arc<std::sync::atomic::AtomicUsize>,
    },
}

impl ClientIpCarrier {
    fn try_send(
        &self,
        packet_id: IpPacketId,
        payload: Bytes,
        budget: &IpPacketQueueBudget,
    ) -> Result<(), RuntimeError> {
        match self {
            Self::Tcp(attachment) => attachment.try_send(packet_id, payload),
            Self::Quic(attachment) => {
                let permit = budget.try_reserve(payload.len())?;
                attachment.try_send(packet_id, payload, permit)
            }
            #[cfg(test)]
            Self::NativeTest { accepted, .. } => {
                accepted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
        }
    }

    fn native_rate_authority(
        &self,
    ) -> Option<Arc<crate::runtime::path::authority::NativeCarrierRateAuthorityHandle>> {
        match self {
            Self::Tcp(_) => None,
            Self::Quic(attachment) => Some(attachment.native_rate_authority()),
            #[cfg(test)]
            Self::NativeTest { authority, .. } => Some(authority.clone()),
        }
    }
}

struct ClientIpCarrierState {
    carrier: ClientIpCarrier,
    ready: bool,
}

// The planned candidate is kept inline so packet-path selection remains a
// copy-only ownership decision with no allocation on the datagram hot path.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Copy)]
enum ClientIpCarrierSelection {
    Bound(ClientIpCarrierKey),
    Planned(PacketPathCandidate),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientIpPacketSendOutcome {
    Accepted,
    Blocked,
    Retired,
    StaleNativeDecision,
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
    carrier_packet_budget: IpPacketQueueBudget,
    next_packet_id: u64,
}

impl ClientIpTunnelState {
    fn new(tunnel_id: IpTunnelId, capacity: usize, packet_queue_bytes: usize) -> Self {
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
            carrier_packet_budget: IpPacketQueueBudget::new(packet_queue_bytes),
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
        let selection =
            self.current_or_select_carrier(context, &metadata.flow_key, payload.len(), now);
        let Some(selection) = selection else {
            return Ok(false);
        };
        let packet_id = IpPacketId(self.next_packet_id);
        self.next_packet_id = self.next_packet_id.wrapping_add(1).max(1);
        let first =
            self.try_send_selection(selection, &metadata.flow_key, packet_id, payload.clone())?;
        match first {
            ClientIpPacketSendOutcome::Accepted => return Ok(true),
            ClientIpPacketSendOutcome::Blocked => return Ok(false),
            ClientIpPacketSendOutcome::Retired => {
                self.remove_carrier(selection.carrier_key());
            }
            ClientIpPacketSendOutcome::StaleNativeDecision => {}
        }

        // Preserve the existing one replacement attempt. A stale Native
        // decision is not carrier failure: replan once without blacklisting.
        let Some(replacement) = self.select_carrier(context, &metadata.flow_key, payload.len())
        else {
            return Ok(false);
        };
        match self.try_send_planned(replacement, &metadata.flow_key, packet_id, payload)? {
            ClientIpPacketSendOutcome::Accepted => Ok(true),
            ClientIpPacketSendOutcome::Blocked | ClientIpPacketSendOutcome::StaleNativeDecision => {
                Ok(false)
            }
            ClientIpPacketSendOutcome::Retired => {
                self.remove_carrier(ClientIpCarrierKey::from(replacement.attachment));
                Ok(false)
            }
        }
    }

    fn try_send_selection(
        &mut self,
        selection: ClientIpCarrierSelection,
        flow: &IpPacketFlowKey,
        packet_id: IpPacketId,
        payload: Bytes,
    ) -> Result<ClientIpPacketSendOutcome, RuntimeError> {
        match selection {
            ClientIpCarrierSelection::Bound(carrier) => {
                let result = self
                    .carriers
                    .get(&carrier)
                    .ok_or(RuntimeError::ReliablePathRetired)?
                    .carrier
                    .try_send(packet_id, payload, &self.carrier_packet_budget);
                let outcome = classify_client_ip_packet_send(result)?;
                if outcome == ClientIpPacketSendOutcome::Accepted {
                    let _ = self
                        .flows
                        .commit_planned_current(flow, carrier, Instant::now());
                }
                Ok(outcome)
            }
            ClientIpCarrierSelection::Planned(candidate) => {
                self.try_send_planned(candidate, flow, packet_id, payload)
            }
        }
    }

    fn try_send_planned(
        &mut self,
        candidate: PacketPathCandidate,
        flow: &IpPacketFlowKey,
        packet_id: IpPacketId,
        payload: Bytes,
    ) -> Result<ClientIpPacketSendOutcome, RuntimeError> {
        let carrier_key = ClientIpCarrierKey::from(candidate.attachment);
        let carrier = self
            .carriers
            .get(&carrier_key)
            .filter(|carrier| carrier.ready)
            .ok_or(RuntimeError::ReliablePathRetired)?
            .carrier
            .clone();
        match candidate.attachment.key.underlay {
            UnderlayProtocol::Tcp => {
                if candidate.native_authority_stamp.is_some()
                    || carrier.native_rate_authority().is_some()
                {
                    return Ok(ClientIpPacketSendOutcome::StaleNativeDecision);
                }
                let result = carrier.try_send(packet_id, payload, &self.carrier_packet_budget);
                if result.is_ok() {
                    let flowlet_timeout = transport_pto_from_snapshot(Some(candidate.snapshot));
                    self.flows
                        .bind(flow.clone(), carrier_key, Instant::now(), flowlet_timeout);
                }
                classify_client_ip_packet_send(result)
            }
            UnderlayProtocol::Udp => {
                let Some(expected_stamp) = candidate.native_authority_stamp else {
                    return Ok(ClientIpPacketSendOutcome::StaleNativeDecision);
                };
                let Some(authority) = carrier.native_rate_authority() else {
                    return Ok(ClientIpPacketSendOutcome::StaleNativeDecision);
                };
                let committed = authority.commit_with_current_scheduling_shape(
                    expected_stamp,
                    |current_shape| {
                        let Some(flowlet_timeout) = client_native_packet_flowlet_timeout(
                            candidate,
                            expected_stamp,
                            current_shape,
                        ) else {
                            return Ok(ClientIpPacketSendOutcome::StaleNativeDecision);
                        };
                        let result =
                            carrier.try_send(packet_id, payload, &self.carrier_packet_budget);
                        if result.is_ok() {
                            self.flows.bind(
                                flow.clone(),
                                carrier_key,
                                Instant::now(),
                                flowlet_timeout,
                            );
                        }
                        classify_client_ip_packet_send(result)
                    },
                );
                match committed {
                    Ok(result) => result,
                    Err(_) => Ok(ClientIpPacketSendOutcome::StaleNativeDecision),
                }
            }
        }
    }

    fn current_or_select_carrier(
        &mut self,
        context: &ClientPathContext,
        flow: &IpPacketFlowKey,
        packet_bytes: usize,
        now: Instant,
    ) -> Option<ClientIpCarrierSelection> {
        let carriers = &self.carriers;
        if let Some(current) = self.flows.planned_current(flow, now, |carrier| {
            carriers.get(&carrier).is_some_and(|carrier| carrier.ready)
        }) {
            return Some(ClientIpCarrierSelection::Bound(current));
        }
        self.select_carrier(context, flow, packet_bytes)
            .map(ClientIpCarrierSelection::Planned)
    }

    fn select_carrier(
        &mut self,
        context: &ClientPathContext,
        flow: &IpPacketFlowKey,
        packet_bytes: usize,
    ) -> Option<PacketPathCandidate> {
        let inputs = self
            .carriers
            .iter()
            .filter(|(_, carrier)| carrier.ready)
            .filter_map(|(key, state)| {
                let native_scheduling_shape = match key.path.underlay {
                    UnderlayProtocol::Tcp => {
                        if state.carrier.native_rate_authority().is_some() {
                            return None;
                        }
                        None
                    }
                    UnderlayProtocol::Udp => {
                        let authority = state.carrier.native_rate_authority()?;
                        let scope = CarrierRateAuthorityScope::new(
                            key.path_instance_id,
                            PathMetricDirection::ClientToServer,
                        );
                        Some(authority.scheduling_shape_snapshot(scope).ok()?)
                    }
                };
                Some(PacketPathSelectionInput {
                    attachment: PacketPathAttachment {
                        key: key.path,
                        path_instance_id: key.path_instance_id,
                    },
                    active_flows: self.flows.active_load_for(*key, flow),
                    native_scheduling_shape,
                })
            })
            .collect::<Vec<_>>();
        context
            .ordered_packet_path_candidates(&inputs, packet_bytes)
            .into_iter()
            .next()
    }
}

fn client_native_packet_flowlet_timeout(
    candidate: PacketPathCandidate,
    expected_stamp: crate::model::carrier_rate_authority::CarrierRateAuthorityStamp,
    current_shape: crate::runtime::path::authority::NativeCarrierSchedulingShapeSnapshot,
) -> Option<std::time::Duration> {
    let scope = current_shape.stamp().scope();
    if current_shape.stamp() != expected_stamp
        || candidate.native_authority_stamp != Some(expected_stamp)
        || candidate.attachment.key.underlay != UnderlayProtocol::Udp
        || scope.carrier_instance_id() != candidate.attachment.path_instance_id
        || scope.direction() != PathMetricDirection::ClientToServer
    {
        return None;
    }
    let mut snapshot = candidate
        .snapshot
        .with_scheduling_service_rate(current_shape.service_rate());
    snapshot.srtt_ms = if current_shape.srtt().is_zero() {
        crate::runtime::path::model::default_path_srtt_ms()
    } else {
        current_shape.srtt().as_secs_f64() * 1_000.0
    };
    snapshot.jitter_ms = current_shape.rttvar().as_secs_f64() * 1_000.0;
    Some(transport_pto_from_snapshot(Some(snapshot)))
}

impl From<PacketPathAttachment> for ClientIpCarrierKey {
    fn from(attachment: PacketPathAttachment) -> Self {
        Self {
            path: attachment.key,
            path_instance_id: attachment.path_instance_id,
        }
    }
}

impl ClientIpCarrierSelection {
    fn carrier_key(self) -> ClientIpCarrierKey {
        match self {
            Self::Bound(carrier) => carrier,
            Self::Planned(candidate) => ClientIpCarrierKey::from(candidate.attachment),
        }
    }
}

fn classify_client_ip_packet_send(
    result: Result<(), RuntimeError>,
) -> Result<ClientIpPacketSendOutcome, RuntimeError> {
    match result {
        Ok(()) => Ok(ClientIpPacketSendOutcome::Accepted),
        Err(RuntimeError::SenderServiceBlocked) => Ok(ClientIpPacketSendOutcome::Blocked),
        Err(RuntimeError::ReliablePathRetired) | Err(RuntimeError::ReliablePathSessionClosed) => {
            Ok(ClientIpPacketSendOutcome::Retired)
        }
        Err(error) => Err(error),
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
    let mut state = ClientIpTunnelState::new(tunnel_id, 1, packet_bytes);
    context.ensure_session_active()?;
    let session_retirement = context.session_retirement().wait();
    tokio::pin!(session_retirement);

    while state.parameters.is_none() || !state.has_ready_carrier() {
        let input = tokio::select! {
            biased;
            reason = &mut session_retirement => {
                return Err(RuntimeError::RemoteClosed(reason));
            }
            input = events_rx.recv() => input,
        };
        let Some(input) = input else {
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
                context.commit_if_session_active(|| state.apply_update(update))??;
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
    context.commit_if_session_active(|| {
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
    })?;

    loop {
        tokio::select! {
            biased;
            reason = &mut session_retirement => {
                return Err(RuntimeError::RemoteClosed(reason));
            }
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
                        context.commit_if_session_active(|| state.apply_update(update))??;
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
                match context.commit_if_session_active(|| start.send(())) {
                    Ok(Ok(())) => {}
                    Ok(Err(())) => {
                        let _ = events
                            .send_update(ClientIpCarrierUpdate::Retired { key })
                            .await;
                        return;
                    }
                    Err(_) => return,
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
