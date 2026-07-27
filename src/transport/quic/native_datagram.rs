//! RFC 9297 HTTP Datagrams associated with an HTTP/3 request stream.
//!
//! Quinn owns unreliable delivery. This adapter adds only the Quarter Stream
//! ID and a bounded fragment envelope so an MPP datagram keeps one directional
//! `(flow_id, datagram_id)` identity when it exceeds the current path MTU.

use super::QuicCarrierError;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{DatagramFlowId, DatagramId, Frame};
use bytes::{Buf, Bytes};
use quinn::Connection;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

const NATIVE_DATAGRAM_VERSION: u8 = 1;
const NATIVE_FRAGMENT_HEADER_BYTES: usize = 1 + 8 + 8 + 4 + 2 + 2 + 4;
const MAX_NATIVE_FRAGMENTS: usize = 64;

#[derive(Debug)]
struct Route {
    generation: u64,
    tx: mpsc::Sender<BudgetedPacket>,
}

#[derive(Debug, Default)]
struct RoutingTable {
    active: HashMap<u64, Route>,
    pending: HashMap<u64, VecDeque<PendingPacket>>,
}

#[derive(Debug)]
struct PendingPacket {
    deadline: Instant,
    packet: BudgetedPacket,
}

#[derive(Debug)]
struct HubState {
    routing: Mutex<RoutingTable>,
    next_generation: AtomicU64,
    buffered_bytes: Arc<AtomicUsize>,
    max_buffered_bytes: usize,
    max_routes: usize,
    max_pending_packets_per_route: usize,
    active_reassemblies: AtomicUsize,
    max_active_reassemblies: usize,
    dropped_packets: AtomicU64,
}

#[derive(Debug, Clone)]
pub(super) struct NativeDatagramHub {
    connection: Connection,
    state: Arc<HubState>,
    route_queue: usize,
}

#[derive(Debug, Clone)]
pub(super) struct NativeDatagramSender {
    connection: Connection,
    request_stream_id: u64,
}

#[derive(Debug)]
pub(super) struct NativeDatagramReceiver {
    request_stream_id: u64,
    generation: u64,
    state: Arc<HubState>,
    rx: mpsc::Receiver<BudgetedPacket>,
    reassemblies: HashMap<(DatagramFlowId, DatagramId), Reassembly>,
}

#[derive(Debug)]
struct BudgetedPacket {
    bytes: Bytes,
    buffered_bytes: Arc<AtomicUsize>,
    received_at: Instant,
}

impl Drop for BudgetedPacket {
    fn drop(&mut self) {
        self.buffered_bytes
            .fetch_sub(self.bytes.len(), Ordering::AcqRel);
    }
}

#[derive(Debug)]
struct Reassembly {
    _permit: ReassemblyPermit,
    deadline: Instant,
    ttl_ms: u32,
    total_len: usize,
    received_len: usize,
    parts: Vec<Option<BudgetedPacket>>,
}

#[derive(Debug)]
struct Fragment {
    flow_id: DatagramFlowId,
    datagram_id: DatagramId,
    ttl_ms: u32,
    index: usize,
    count: usize,
    total_len: usize,
    packet: BudgetedPacket,
    payload_offset: usize,
    received_at: Instant,
}

#[derive(Debug)]
struct ReassemblyPermit {
    state: Arc<HubState>,
}

impl Drop for ReassemblyPermit {
    fn drop(&mut self) {
        self.state
            .active_reassemblies
            .fetch_sub(1, Ordering::AcqRel);
    }
}

impl NativeDatagramHub {
    pub(super) fn new(
        connection: Connection,
        max_buffered_bytes: usize,
        max_routes: usize,
        route_queue: usize,
    ) -> Self {
        let state = Arc::new(HubState {
            routing: Mutex::new(RoutingTable::default()),
            next_generation: AtomicU64::new(1),
            buffered_bytes: Arc::new(AtomicUsize::new(0)),
            max_buffered_bytes: max_buffered_bytes.max(1),
            max_routes: max_routes.max(1),
            max_pending_packets_per_route: route_queue.max(1),
            active_reassemblies: AtomicUsize::new(0),
            max_active_reassemblies: (max_buffered_bytes / 1_200).clamp(16, 256),
            dropped_packets: AtomicU64::new(0),
        });
        let hub = Self {
            connection: connection.clone(),
            state: state.clone(),
            route_queue: route_queue.max(1),
        };
        tokio::spawn(run_datagram_router(connection, state));
        hub
    }

    pub(super) fn sender(&self, request_stream_id: u64) -> NativeDatagramSender {
        NativeDatagramSender {
            connection: self.connection.clone(),
            request_stream_id,
        }
    }

    pub(super) fn register(
        &self,
        request_stream_id: u64,
    ) -> Result<NativeDatagramReceiver, QuicCarrierError> {
        let generation = self.state.next_generation.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(self.route_queue);
        let mut routing = self
            .state
            .routing
            .lock()
            .expect("native datagram route lock");
        let replacing_active = routing.active.contains_key(&request_stream_id);
        let pending = routing.pending.remove(&request_stream_id);
        if !replacing_active && !make_room_for_active_route(&self.state, &mut routing) {
            return Err(QuicCarrierError::NativeDatagramRoutesExhausted);
        }
        if let Some(pending) = pending {
            let now = Instant::now();
            for pending in pending {
                if pending.deadline <= now || tx.try_send(pending.packet).is_err() {
                    self.state.dropped_packets.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        // Publish the active route only after the pre-route queue is drained
        // while holding the same lock used by the router. A newly arriving
        // packet therefore cannot jump ahead of the retained first flight.
        routing
            .active
            .insert(request_stream_id, Route { generation, tx });
        drop(routing);
        Ok(NativeDatagramReceiver {
            request_stream_id,
            generation,
            state: self.state.clone(),
            rx,
            reassemblies: HashMap::new(),
        })
    }
}

fn make_room_for_active_route(state: &HubState, routing: &mut RoutingTable) -> bool {
    while routing.active.len().saturating_add(routing.pending.len()) >= state.max_routes {
        // A resolved H3 request is authoritative. Prefer it over an
        // unregistered Quarter Stream ID that may be attacker-selected, and
        // evict the pending route closest to expiry. Dropping its packets
        // releases the shared byte budget immediately.
        let Some(pending_id) = routing
            .pending
            .iter()
            .filter_map(|(request_stream_id, packets)| {
                packets
                    .front()
                    .map(|packet| (packet.deadline, *request_stream_id))
            })
            .min()
            .map(|(_, request_stream_id)| request_stream_id)
        else {
            return false;
        };
        let packets = routing
            .pending
            .remove(&pending_id)
            .expect("selected pending native route exists");
        state
            .dropped_packets
            .fetch_add(packets.len() as u64, Ordering::Relaxed);
    }
    true
}

impl NativeDatagramSender {
    pub(super) fn send_frame(
        &self,
        frame: &Frame,
        limits: CodecLimits,
    ) -> Result<(), QuicCarrierError> {
        let Frame::DatagramData {
            flow_id,
            datagram_id,
            ttl_ms,
            ..
        } = frame
        else {
            return Err(QuicCarrierError::InvalidNativeDatagram(
                "only DATAGRAM_DATA may use an HTTP Datagram",
            ));
        };
        if *ttl_ms == 0 {
            return Err(QuicCarrierError::InvalidNativeDatagram(
                "expired datagram cannot be transmitted",
            ));
        }
        let Frame::DatagramData { payload, .. } = frame else {
            unreachable!("DATAGRAM_DATA checked above");
        };
        if payload.len() > limits.max_payload_bytes {
            return Err(QuicCarrierError::NativeDatagramTooLarge);
        }
        let quarter_stream_id = self.request_stream_id.checked_div(4).ok_or(
            QuicCarrierError::InvalidNativeDatagram("invalid HTTP/3 request stream ID"),
        )?;
        if !self.request_stream_id.is_multiple_of(4) {
            return Err(QuicCarrierError::InvalidNativeDatagram(
                "HTTP Datagram must reference a client request stream",
            ));
        }
        let mut quarter_stream_id_bytes = Vec::with_capacity(8);
        encode_varint(quarter_stream_id, &mut quarter_stream_id_bytes)?;
        let maximum = self
            .connection
            .max_datagram_size()
            .ok_or(QuicCarrierError::NativeDatagramUnavailable)?;
        let fragment_payload_bytes = maximum
            .checked_sub(quarter_stream_id_bytes.len() + NATIVE_FRAGMENT_HEADER_BYTES)
            .filter(|value| *value > 0)
            .ok_or(QuicCarrierError::NativeDatagramTooLarge)?;
        let fragment_count = payload.len().max(1).div_ceil(fragment_payload_bytes);
        if fragment_count > MAX_NATIVE_FRAGMENTS {
            return Err(QuicCarrierError::NativeDatagramTooLarge);
        }
        let fragment_count_u16 =
            u16::try_from(fragment_count).map_err(|_| QuicCarrierError::NativeDatagramTooLarge)?;
        let total_len =
            u32::try_from(payload.len()).map_err(|_| QuicCarrierError::NativeDatagramTooLarge)?;

        for index in 0..fragment_count {
            let start = index.saturating_mul(fragment_payload_bytes);
            let end = start
                .saturating_add(fragment_payload_bytes)
                .min(payload.len());
            let mut packet = Vec::with_capacity(
                quarter_stream_id_bytes.len() + NATIVE_FRAGMENT_HEADER_BYTES + end - start,
            );
            packet.extend_from_slice(&quarter_stream_id_bytes);
            packet.push(NATIVE_DATAGRAM_VERSION);
            packet.extend_from_slice(&flow_id.0.to_be_bytes());
            packet.extend_from_slice(&datagram_id.0.to_be_bytes());
            packet.extend_from_slice(&ttl_ms.to_be_bytes());
            packet.extend_from_slice(&(index as u16).to_be_bytes());
            packet.extend_from_slice(&fragment_count_u16.to_be_bytes());
            packet.extend_from_slice(&total_len.to_be_bytes());
            packet.extend_from_slice(&payload[start..end]);
            self.connection
                .send_datagram(Bytes::from(packet))
                .map_err(QuicCarrierError::from)?;
        }
        Ok(())
    }
}

impl NativeDatagramReceiver {
    pub(super) async fn recv_frame(
        &mut self,
        limits: CodecLimits,
    ) -> Result<Frame, QuicCarrierError> {
        loop {
            self.expire_reassemblies();
            let packet = if let Some(deadline) = self.next_reassembly_expiry() {
                tokio::select! {
                    packet = self.rx.recv() => packet,
                    () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                        self.expire_reassemblies();
                        continue;
                    }
                }
            } else {
                self.rx.recv().await
            }
            .ok_or(QuicCarrierError::H3DriverClosed)?;
            match decode_fragment(packet, limits) {
                Ok(fragment) => {
                    if let Some(frame) = self.insert_fragment(fragment) {
                        return Ok(frame);
                    }
                }
                Err(()) => {
                    self.state.dropped_packets.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    fn insert_fragment(&mut self, fragment: Fragment) -> Option<Frame> {
        let now = Instant::now();
        let fragment_deadline =
            fragment.received_at + Duration::from_millis(u64::from(fragment.ttl_ms));
        if fragment_deadline <= now {
            self.state.dropped_packets.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let key = (fragment.flow_id, fragment.datagram_id);
        if !self.reassemblies.contains_key(&key) {
            let Some(permit) = try_acquire_reassembly(self.state.clone()) else {
                self.state.dropped_packets.fetch_add(1, Ordering::Relaxed);
                return None;
            };
            self.reassemblies.insert(
                key,
                Reassembly {
                    _permit: permit,
                    deadline: fragment_deadline,
                    ttl_ms: fragment.ttl_ms,
                    total_len: fragment.total_len,
                    received_len: 0,
                    parts: (0..fragment.count).map(|_| None).collect(),
                },
            );
        }
        let entry = self
            .reassemblies
            .get_mut(&key)
            .expect("native reassembly inserted");
        if entry.ttl_ms != fragment.ttl_ms
            || entry.total_len != fragment.total_len
            || entry.parts.len() != fragment.count
        {
            self.reassemblies.remove(&key);
            self.state.dropped_packets.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        entry.deadline = entry.deadline.min(fragment_deadline);
        let Some(slot) = entry.parts.get_mut(fragment.index) else {
            self.reassemblies.remove(&key);
            self.state.dropped_packets.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        if slot.is_none() {
            let payload_len = fragment
                .packet
                .bytes
                .len()
                .saturating_sub(fragment.payload_offset);
            entry.received_len = entry.received_len.saturating_add(payload_len);
            *slot = Some(fragment.packet);
        }
        if entry.received_len != entry.total_len || entry.parts.iter().any(Option::is_none) {
            return None;
        }
        let complete = self
            .reassemblies
            .remove(&key)
            .expect("completed native reassembly exists");
        let remaining_ttl_ms = complete
            .deadline
            .saturating_duration_since(Instant::now())
            .as_millis()
            .min(u128::from(u32::MAX)) as u32;
        if remaining_ttl_ms == 0 {
            return None;
        }
        let mut payload = Vec::with_capacity(complete.total_len);
        for part in complete.parts {
            let part = part.expect("native reassembly completeness checked");
            payload.extend_from_slice(&part.bytes[NATIVE_FRAGMENT_HEADER_BYTES..]);
        }
        if payload.len() != complete.total_len {
            self.state.dropped_packets.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        Some(Frame::DatagramData {
            flow_id: key.0,
            datagram_id: key.1,
            ttl_ms: remaining_ttl_ms,
            payload: Bytes::from(payload),
        })
    }

    fn expire_reassemblies(&mut self) {
        let now = Instant::now();
        self.reassemblies
            .retain(|_, reassembly| reassembly.deadline > now);
    }

    fn next_reassembly_expiry(&self) -> Option<Instant> {
        self.reassemblies
            .values()
            .map(|reassembly| reassembly.deadline)
            .min()
    }
}

impl Drop for NativeDatagramReceiver {
    fn drop(&mut self) {
        let mut routing = self
            .state
            .routing
            .lock()
            .expect("native datagram route lock");
        if routing
            .active
            .get(&self.request_stream_id)
            .is_some_and(|route| route.generation == self.generation)
        {
            routing.active.remove(&self.request_stream_id);
        }
    }
}

async fn run_datagram_router(connection: Connection, state: Arc<HubState>) {
    loop {
        let next_expiry = next_pending_route_expiry(&state);
        let packet = if let Some(deadline) = next_expiry {
            tokio::select! {
                packet = connection.read_datagram() => packet,
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                    expire_pending_routes(&state, Instant::now());
                    continue;
                }
            }
        } else {
            connection.read_datagram().await
        };
        let Ok(packet) = packet else {
            return;
        };
        route_datagram(&connection, &state, packet);
    }
}

fn route_datagram(connection: &Connection, state: &HubState, packet: Bytes) {
    let (request_stream_id, header_len) = match decode_quarter_stream_id(&packet) {
        Some(decoded) => decoded,
        None => {
            state.dropped_packets.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    let payload = packet.slice(header_len..);
    if !reserve_buffered_bytes(state, payload.len()) {
        state.dropped_packets.fetch_add(1, Ordering::Relaxed);
        return;
    }
    let now = Instant::now();
    let budgeted = BudgetedPacket {
        bytes: payload,
        buffered_bytes: state.buffered_bytes.clone(),
        received_at: now,
    };
    let mut routing = state.routing.lock().expect("native datagram route lock");
    if let Some(route) = routing.active.get(&request_stream_id) {
        if route.tx.try_send(budgeted).is_err() {
            state.dropped_packets.fetch_add(1, Ordering::Relaxed);
        }
        return;
    }

    // RFC 9297 permits a receiver to retain an HTTP Datagram for roughly one
    // RTT while the associated request stream is still being created. This is
    // the normal first-packet race between QUIC DATAGRAM and H3 HEADERS/DATA,
    // not a reliability mechanism: the wait adds no sender RTT and expiry
    // still drops the packet.
    if !routing.pending.contains_key(&request_stream_id)
        && routing.active.len().saturating_add(routing.pending.len()) >= state.max_routes
    {
        state.dropped_packets.fetch_add(1, Ordering::Relaxed);
        return;
    }
    let queue = routing.pending.entry(request_stream_id).or_default();
    if queue.len() >= state.max_pending_packets_per_route {
        state.dropped_packets.fetch_add(1, Ordering::Relaxed);
        return;
    }
    let route_wait = pending_route_wait(connection);
    let deadline = pending_packet_deadline(&budgeted.bytes, now, route_wait);
    queue.push_back(PendingPacket {
        deadline,
        packet: budgeted,
    });
}

fn pending_route_wait(connection: &Connection) -> Duration {
    connection
        .rtt()
        .saturating_mul(2)
        .clamp(Duration::from_millis(25), Duration::from_millis(250))
}

fn pending_packet_deadline(payload: &Bytes, now: Instant, route_wait: Duration) -> Instant {
    const TTL_OFFSET: usize = 1 + 8 + 8;
    let ttl = payload
        .get(TTL_OFFSET..TTL_OFFSET + 4)
        .and_then(|encoded| <[u8; 4]>::try_from(encoded).ok())
        .map(u32::from_be_bytes)
        .filter(|ttl_ms| *ttl_ms > 0)
        .map(|ttl_ms| Duration::from_millis(u64::from(ttl_ms)));
    now + ttl.map_or(route_wait, |ttl| ttl.min(route_wait))
}

fn next_pending_route_expiry(state: &HubState) -> Option<Instant> {
    let routing = state.routing.lock().expect("native datagram route lock");
    routing
        .pending
        .values()
        .filter_map(|packets| packets.front().map(|packet| packet.deadline))
        .min()
}

fn expire_pending_routes(state: &HubState, now: Instant) {
    let mut routing = state.routing.lock().expect("native datagram route lock");
    routing.pending.retain(|_, packets| {
        while packets
            .front()
            .is_some_and(|pending| pending.deadline <= now)
        {
            let _ = packets.pop_front();
            state.dropped_packets.fetch_add(1, Ordering::Relaxed);
        }
        !packets.is_empty()
    });
}

fn reserve_buffered_bytes(state: &HubState, bytes: usize) -> bool {
    state
        .buffered_bytes
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current
                .checked_add(bytes)
                .filter(|next| *next <= state.max_buffered_bytes)
        })
        .is_ok()
}

fn decode_fragment(packet: BudgetedPacket, limits: CodecLimits) -> Result<Fragment, ()> {
    let received_at = packet.received_at;
    let bytes = &packet.bytes;
    if bytes.len() < NATIVE_FRAGMENT_HEADER_BYTES {
        return Err(());
    }
    let mut cursor = bytes.clone();
    let version = cursor.get_u8();
    if version != NATIVE_DATAGRAM_VERSION {
        return Err(());
    }
    let flow_id = DatagramFlowId(cursor.get_u64());
    let datagram_id = DatagramId(cursor.get_u64());
    let ttl_ms = cursor.get_u32();
    let index = usize::from(cursor.get_u16());
    let count = usize::from(cursor.get_u16());
    let total_len = cursor.get_u32() as usize;
    if ttl_ms == 0
        || count == 0
        || count > MAX_NATIVE_FRAGMENTS
        || index >= count
        || total_len > limits.max_payload_bytes
        || (total_len > 0 && count > total_len)
        || (total_len == 0 && (count != 1 || index != 0))
        || cursor.remaining() > total_len
    {
        return Err(());
    }
    let payload_offset = bytes.len().saturating_sub(cursor.remaining());
    Ok(Fragment {
        flow_id,
        datagram_id,
        ttl_ms,
        index,
        count,
        total_len,
        packet,
        payload_offset,
        received_at,
    })
}

fn try_acquire_reassembly(state: Arc<HubState>) -> Option<ReassemblyPermit> {
    state
        .active_reassemblies
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current < state.max_active_reassemblies).then_some(current + 1)
        })
        .ok()?;
    Some(ReassemblyPermit { state })
}

fn decode_quarter_stream_id(packet: &Bytes) -> Option<(u64, usize)> {
    let first = *packet.first()?;
    let len = 1usize << (first >> 6);
    if packet.len() < len {
        return None;
    }
    let mut value = u64::from(first & 0x3f);
    for byte in &packet[1..len] {
        value = (value << 8) | u64::from(*byte);
    }
    value.checked_mul(4).map(|stream_id| (stream_id, len))
}

fn encode_varint(value: u64, output: &mut Vec<u8>) -> Result<(), QuicCarrierError> {
    let (len, marker) = match value {
        0..=63 => (1, 0b00),
        64..=16_383 => (2, 0b01),
        16_384..=1_073_741_823 => (4, 0b10),
        1_073_741_824..=4_611_686_018_427_387_903 => (8, 0b11),
        _ => return Err(QuicCarrierError::NativeDatagramTooLarge),
    };
    let encoded = value | ((marker as u64) << ((len * 8) - 2));
    output.extend_from_slice(&encoded.to_be_bytes()[8 - len..]);
    Ok(())
}

#[cfg(test)]
#[path = "native_datagram_test.rs"]
mod tests;
