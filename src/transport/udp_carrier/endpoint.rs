use super::crypto::{CarrierRole, PacketCipher};
use super::error::{UdpCarrierConnectionError, UdpCarrierFrameError, UdpCarrierTransportError};
use super::packet::{
    MAX_PROBED_DATAGRAM_BYTES, PacketAckRange, PacketHeader, PacketPayload,
    SAFE_TARGET_DATAGRAM_BYTES, decode_packet, encode_packet, max_frame_fragment_payload,
    max_frame_fragment_payload_for_datagram, peek_connection_id,
};
use super::stream::{RecvStream, SendStream, StreamCommand};
use crate::config::CipherSuite;
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use bytes::{Bytes, BytesMut};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, Notify, RwLock, mpsc};

const MAX_UDP_PACKET_BYTES: usize = 65_535;
const INITIAL_RTT: Duration = Duration::from_millis(100);
const MIN_RTO: Duration = Duration::from_millis(25);
const MAX_RTO: Duration = Duration::from_secs(1);
const RETRANSMIT_TICK_FRACTION: u32 = 4;
const ACK_FLUSH_PACKET_THRESHOLD: usize = 32;
const MAX_ACK_RANGES_PER_PACKET: usize = 64;
const STREAM_ID_CLIENT_FIRST: u64 = 1;
const STREAM_ID_SERVER_FIRST: u64 = 2;

#[derive(Debug)]
pub struct Endpoint {
    inner: Arc<EndpointInner>,
    incoming: Mutex<mpsc::Receiver<Connection>>,
}

#[derive(Debug)]
struct EndpointInner {
    socket: Arc<UdpSocket>,
    role: CarrierRole,
    secret: Arc<[u8]>,
    cipher_suite: CipherSuite,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    connections: StdMutex<HashMap<u64, Arc<ConnectionInner>>>,
    incoming: mpsc::Sender<Connection>,
}

#[derive(Clone, Debug)]
pub struct Connection {
    inner: Arc<ConnectionInner>,
}

#[derive(Debug)]
struct ConnectionInner {
    socket: Arc<UdpSocket>,
    role: CarrierRole,
    peer: RwLock<SocketAddr>,
    connection_id: u64,
    cipher: PacketCipher,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    send_packet_number: AtomicU64,
    next_stream_id: AtomicU64,
    commands: mpsc::Sender<StreamCommand>,
    streams: Mutex<HashMap<u64, StreamState>>,
    incoming_streams: mpsc::Sender<(SendStream, RecvStream)>,
    incoming_streams_rx: Mutex<mpsc::Receiver<(SendStream, RecvStream)>>,
    pending: Mutex<PacketWindow>,
    pending_bytes: AtomicUsize,
    send_notify: Notify,
    ack_state: Mutex<AckState>,
    controller: Mutex<UdpPathController>,
    closed: AtomicBool,
}

struct ConnectionParams<'a> {
    socket: Arc<UdpSocket>,
    role: CarrierRole,
    peer: SocketAddr,
    connection_id: u64,
    secret: &'a [u8],
    cipher_suite: CipherSuite,
    mux_limits: MuxLimits,
    codec_limits: CodecLimits,
}

#[derive(Debug)]
struct StreamState {
    frames: mpsc::Sender<Bytes>,
    assemblies: BTreeMap<u64, FrameAssembly>,
    completed: BTreeMap<u64, Bytes>,
    next_frame_id: u64,
}

#[derive(Debug)]
struct FrameAssembly {
    total_len: usize,
    received_bytes: usize,
    buffer: BytesMut,
    ranges: Vec<(usize, usize)>,
}

#[derive(Debug, Default)]
struct AckState {
    pending: Vec<u64>,
    largest_seen: u64,
    scheduled: bool,
}

#[derive(Debug, Clone)]
struct PendingPacket {
    packet: Bytes,
    sent_at: Instant,
    last_sent_at: Instant,
    deadline: Instant,
    generation: u32,
    retransmit_count: u32,
}

#[derive(Debug, Default)]
struct PacketWindow {
    base: u64,
    slots: VecDeque<Option<PendingPacket>>,
    deadlines: BinaryHeap<Reverse<PacketDeadline>>,
    bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct PacketDeadline {
    at: Instant,
    packet_number: u64,
    generation: u32,
}

#[derive(Debug, Clone, Copy)]
struct RttEstimator {
    srtt: Duration,
    rttvar: Duration,
}

impl Endpoint {
    pub async fn bind_server(
        addr: SocketAddr,
        secret: &[u8],
        cipher_suite: CipherSuite,
        mux_limits: MuxLimits,
        codec_limits: CodecLimits,
    ) -> Result<Self, UdpCarrierTransportError> {
        Self::bind(
            addr,
            CarrierRole::Server,
            secret,
            cipher_suite,
            mux_limits,
            codec_limits,
        )
        .await
    }

    pub async fn bind_client(
        addr: SocketAddr,
        secret: &[u8],
        cipher_suite: CipherSuite,
        mux_limits: MuxLimits,
        codec_limits: CodecLimits,
    ) -> Result<Self, UdpCarrierTransportError> {
        Self::bind(
            addr,
            CarrierRole::Client,
            secret,
            cipher_suite,
            mux_limits,
            codec_limits,
        )
        .await
    }

    async fn bind(
        addr: SocketAddr,
        role: CarrierRole,
        secret: &[u8],
        cipher_suite: CipherSuite,
        mux_limits: MuxLimits,
        codec_limits: CodecLimits,
    ) -> Result<Self, UdpCarrierTransportError> {
        if secret.is_empty() {
            return Err(UdpCarrierTransportError::EmptySecret);
        }
        let socket = Arc::new(UdpSocket::bind(addr).await?);
        let (incoming_tx, incoming_rx) = mpsc::channel(carrier_connection_queue(mux_limits));
        let inner = Arc::new(EndpointInner {
            socket,
            role,
            secret: Arc::from(secret),
            cipher_suite,
            codec_limits,
            mux_limits,
            connections: StdMutex::new(HashMap::new()),
            incoming: incoming_tx,
        });
        tokio::spawn(run_endpoint_reader(inner.clone()));
        Ok(Self {
            inner,
            incoming: Mutex::new(incoming_rx),
        })
    }

    pub async fn connect(
        &self,
        remote_addr: SocketAddr,
    ) -> Result<Connection, UdpCarrierTransportError> {
        let connection_id = random_connection_id()?;
        let inner = ConnectionInner::new(ConnectionParams {
            socket: self.inner.socket.clone(),
            role: self.inner.role,
            peer: remote_addr,
            connection_id,
            secret: &self.inner.secret,
            cipher_suite: self.inner.cipher_suite,
            mux_limits: self.inner.mux_limits,
            codec_limits: self.inner.codec_limits,
        })?;
        self.inner
            .connections
            .lock()
            .expect("UDP carrier connection map poisoned")
            .insert(connection_id, inner.clone());
        Ok(Connection { inner })
    }

    pub async fn accept(&self) -> Option<Connection> {
        self.incoming.lock().await.recv().await
    }

    pub fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.inner.socket.local_addr()
    }
}

impl Connection {
    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), UdpCarrierConnectionError> {
        if self.inner.closed.load(Ordering::Relaxed) {
            return Err(UdpCarrierConnectionError::Closed);
        }
        let stream_id = self.inner.next_stream_id.fetch_add(2, Ordering::Relaxed);
        let (frames_tx, frames_rx) =
            mpsc::channel(carrier_stream_frame_queue(self.inner.mux_limits));
        self.inner.streams.lock().await.insert(
            stream_id,
            StreamState {
                frames: frames_tx,
                assemblies: BTreeMap::new(),
                completed: BTreeMap::new(),
                next_frame_id: 0,
            },
        );
        Ok((
            SendStream::new(stream_id, self.inner.commands.clone()),
            RecvStream::new(frames_rx),
        ))
    }

    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream), UdpCarrierConnectionError> {
        self.inner
            .incoming_streams_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or(UdpCarrierConnectionError::Closed)
    }

    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Relaxed);
    }
}

impl ConnectionInner {
    fn new(params: ConnectionParams<'_>) -> Result<Arc<Self>, UdpCarrierTransportError> {
        let ConnectionParams {
            socket,
            role,
            peer,
            connection_id,
            secret,
            cipher_suite,
            mux_limits,
            codec_limits,
        } = params;
        let cipher = PacketCipher::new(secret, cipher_suite, connection_id)?;
        let (commands_tx, commands_rx) = mpsc::channel(carrier_command_queue(mux_limits));
        let (incoming_streams_tx, incoming_streams_rx) =
            mpsc::channel(carrier_stream_accept_queue(mux_limits));
        let next_stream_id = match role {
            CarrierRole::Client => STREAM_ID_CLIENT_FIRST,
            CarrierRole::Server => STREAM_ID_SERVER_FIRST,
        };
        let inner = Arc::new(Self {
            socket,
            role,
            peer: RwLock::new(peer),
            connection_id,
            cipher,
            codec_limits,
            mux_limits,
            send_packet_number: AtomicU64::new(1),
            next_stream_id: AtomicU64::new(next_stream_id),
            commands: commands_tx,
            streams: Mutex::new(HashMap::new()),
            incoming_streams: incoming_streams_tx,
            incoming_streams_rx: Mutex::new(incoming_streams_rx),
            pending: Mutex::new(PacketWindow::default()),
            pending_bytes: AtomicUsize::new(0),
            send_notify: Notify::new(),
            ack_state: Mutex::new(AckState::default()),
            controller: Mutex::new(UdpPathController::new(mux_limits)),
            closed: AtomicBool::new(false),
        });
        tokio::spawn(run_connection_commands(inner.clone(), commands_rx));
        tokio::spawn(run_connection_retransmit(inner.clone()));
        Ok(inner)
    }

    async fn send_payload(
        &self,
        payload: PacketPayload,
        reliable: bool,
    ) -> Result<(), UdpCarrierFrameError> {
        let packet_number = self.send_packet_number.fetch_add(1, Ordering::Relaxed);
        let header = PacketHeader {
            direction: self.role.send_direction(),
            connection_id: self.connection_id,
            packet_number,
        };
        let packet = Bytes::from(encode_packet(&self.cipher, header, &payload)?);
        if reliable {
            self.wait_for_send_capacity(packet.len()).await;
        }
        let peer = *self.peer.read().await;
        if reliable {
            let now = Instant::now();
            let deadline = {
                let mut controller = self.controller.lock().await;
                controller.on_packet_sent(packet.len(), now);
                now + controller.rto()
            };
            self.pending_bytes
                .fetch_add(packet.len(), Ordering::Relaxed);
            self.pending.lock().await.insert(
                packet_number,
                PendingPacket {
                    packet: packet.clone(),
                    sent_at: now,
                    last_sent_at: now,
                    deadline,
                    generation: 0,
                    retransmit_count: 0,
                },
            );
        }
        #[cfg(feature = "lab-diagnostics")]
        let started = Instant::now();
        self.socket.send_to(&packet, peer).await?;
        #[cfg(feature = "lab-diagnostics")]
        crate::lab_diagnostics::lab_perf_record(
            "transport.udp_carrier.write_socket_wait",
            started.elapsed(),
            packet.len(),
        );
        Ok(())
    }

    async fn wait_for_send_capacity(&self, packet_len: usize) {
        while !self.closed.load(Ordering::Relaxed) {
            let pending_bytes = self.pending_bytes.load(Ordering::Relaxed);
            let delay = {
                let mut controller = self.controller.lock().await;
                controller.send_delay(packet_len, pending_bytes, self.mux_limits, Instant::now())
            };
            let Some(delay) = delay else {
                return;
            };
            tokio::select! {
                _ = self.send_notify.notified() => {}
                _ = tokio::time::sleep(delay.max(Duration::from_millis(1))) => {}
            }
        }
    }

    async fn retransmit_delay(&self) -> Duration {
        self.controller.lock().await.rto()
    }

    async fn frame_fragment_payload_len(&self) -> usize {
        self.controller.lock().await.frame_fragment_payload_len()
    }

    async fn process_packet(
        self: &Arc<Self>,
        source: SocketAddr,
        header: PacketHeader,
        payload: PacketPayload,
    ) -> Result<(), UdpCarrierFrameError> {
        *self.peer.write().await = source;
        match payload {
            PacketPayload::Ack { ranges } => {
                self.apply_ack_ranges(&ranges).await;
            }
            PacketPayload::FrameFragment {
                ordered,
                ack_eliciting,
                stream_id,
                frame_id,
                offset,
                total_len,
                payload,
            } => {
                if ack_eliciting {
                    self.queue_ack(header.packet_number).await;
                }
                self.receive_fragment(stream_id, frame_id, offset, total_len, payload, ordered)
                    .await?;
            }
            PacketPayload::CloseStream { stream_id } => {
                self.queue_ack(header.packet_number).await;
                let _ = stream_id;
            }
        }
        Ok(())
    }

    async fn queue_ack(self: &Arc<Self>, packet_number: u64) {
        let should_flush = {
            let mut state = self.ack_state.lock().await;
            let gap_observed =
                state.largest_seen != 0 && packet_number > state.largest_seen.saturating_add(1);
            state.largest_seen = state.largest_seen.max(packet_number);
            state.pending.push(packet_number);
            if state.pending.len() >= ACK_FLUSH_PACKET_THRESHOLD || gap_observed {
                true
            } else if !state.scheduled {
                state.scheduled = true;
                let connection = self.clone();
                tokio::spawn(async move {
                    let delay = (connection.retransmit_delay().await / 8)
                        .max(Duration::from_millis(1))
                        .min(Duration::from_millis(5));
                    tokio::time::sleep(delay).await;
                    connection.flush_acks().await;
                });
                false
            } else {
                false
            }
        };
        if should_flush {
            self.flush_acks().await;
        }
    }

    async fn flush_acks(&self) {
        let ranges = {
            let mut state = self.ack_state.lock().await;
            if state.pending.is_empty() {
                state.scheduled = false;
                return;
            }
            state.scheduled = false;
            let ranges = packet_ack_ranges(&mut state.pending);
            state.pending.clear();
            ranges
        };
        if let Err(err) = self
            .send_payload(PacketPayload::Ack { ranges }, false)
            .await
        {
            eprintln!("warning: UDP carrier ACK send failed: {err}");
        }
    }

    async fn apply_ack_ranges(&self, ranges: &[PacketAckRange]) {
        let ack_frontier = ranges.iter().map(|range| range.end).max().unwrap_or(0);
        let now = Instant::now();
        let fast_retransmit_spacing = self.retransmit_delay().await / 2;
        let released: Vec<PendingPacket>;
        let fast_retransmit: Vec<Bytes>;
        {
            let mut pending = self.pending.lock().await;
            released = pending.remove_acked_ranges(ranges);
            fast_retransmit = pending.retransmit_before(
                ack_frontier,
                now,
                fast_retransmit_spacing,
                ACK_FLUSH_PACKET_THRESHOLD * 2,
            );
        }
        let mut released_bytes = 0usize;
        for packet in &released {
            self.pending_bytes
                .fetch_sub(packet.packet.len(), Ordering::Relaxed);
            released_bytes = released_bytes.saturating_add(packet.packet.len());
        }
        {
            let mut controller = self.controller.lock().await;
            controller.on_packets_acked(&released, now, self.mux_limits);
            if !fast_retransmit.is_empty() {
                let retransmit_bytes = fast_retransmit
                    .iter()
                    .fold(0usize, |sum, packet| sum.saturating_add(packet.len()));
                controller.on_loss(retransmit_bytes, self.mux_limits);
            }
        }
        if released_bytes > 0 {
            self.send_notify.notify_waiters();
        }
        if !fast_retransmit.is_empty() {
            let peer = *self.peer.read().await;
            for packet in fast_retransmit {
                if let Err(err) = self.socket.send_to(&packet, peer).await {
                    eprintln!("warning: UDP carrier fast retransmit failed: {err}");
                    break;
                }
            }
        }
    }

    async fn receive_fragment(
        self: &Arc<Self>,
        stream_id: u64,
        frame_id: u64,
        offset: u32,
        total_len: u32,
        payload: Bytes,
        ordered: bool,
    ) -> Result<(), UdpCarrierFrameError> {
        let total_len = usize::try_from(total_len)
            .map_err(|_| UdpCarrierFrameError::InvalidPacket("frame length overflow"))?;
        if total_len > self.codec_limits.max_frame_bytes {
            return Err(UdpCarrierFrameError::FrameTooLarge {
                actual: total_len,
                limit: self.codec_limits.max_frame_bytes,
            });
        }

        let mut incoming = None;
        let mut completed = None;
        let frames = {
            let mut streams = self.streams.lock().await;
            let state = match streams.get_mut(&stream_id) {
                Some(state) => state,
                None => {
                    let (frames_tx, frames_rx) =
                        mpsc::channel(carrier_stream_frame_queue(self.mux_limits));
                    streams.insert(
                        stream_id,
                        StreamState {
                            frames: frames_tx,
                            assemblies: BTreeMap::new(),
                            completed: BTreeMap::new(),
                            next_frame_id: 0,
                        },
                    );
                    incoming = Some((
                        SendStream::new(stream_id, self.commands.clone()),
                        RecvStream::new(frames_rx),
                    ));
                    streams
                        .get_mut(&stream_id)
                        .expect("newly inserted stream exists")
                }
            };
            let assembly = state
                .assemblies
                .entry(frame_id)
                .or_insert_with(|| FrameAssembly {
                    total_len,
                    received_bytes: 0,
                    buffer: BytesMut::zeroed(total_len),
                    ranges: Vec::new(),
                });
            if let Some(frame) = assembly.insert(offset, total_len, payload)? {
                state.assemblies.remove(&frame_id);
                let mut ready = Vec::from([frame]);
                if ordered {
                    let frame = ready.pop().expect("completed frame");
                    state.completed.insert(frame_id, frame);
                    while let Some(frame) = state.completed.remove(&state.next_frame_id) {
                        state.next_frame_id = state.next_frame_id.checked_add(1).ok_or(
                            UdpCarrierFrameError::InvalidPacket("receive frame id overflow"),
                        )?;
                        ready.push(frame);
                    }
                }
                if !ready.is_empty() {
                    completed = Some(ready);
                }
            }
            state.frames.clone()
        };

        if let Some(streams) = incoming {
            self.incoming_streams
                .send(streams)
                .await
                .map_err(|_| UdpCarrierFrameError::Closed)?;
        }
        if let Some(ready) = completed {
            for frame in ready {
                if frames.send(frame).await.is_err() {
                    self.streams.lock().await.remove(&stream_id);
                    break;
                }
            }
        }
        Ok(())
    }
}

impl FrameAssembly {
    fn insert(
        &mut self,
        offset: u32,
        total_len: usize,
        payload: Bytes,
    ) -> Result<Option<Bytes>, UdpCarrierFrameError> {
        if total_len != self.total_len {
            return Err(UdpCarrierFrameError::InvalidPacket(
                "fragment total length changed",
            ));
        }
        let offset_usize = usize::try_from(offset)
            .map_err(|_| UdpCarrierFrameError::InvalidPacket("fragment offset overflow"))?;
        let end =
            offset_usize
                .checked_add(payload.len())
                .ok_or(UdpCarrierFrameError::InvalidPacket(
                    "fragment range overflow",
                ))?;
        if end > self.total_len {
            return Err(UdpCarrierFrameError::InvalidPacket(
                "fragment exceeds frame length",
            ));
        }
        if self
            .ranges
            .iter()
            .any(|(start, existing_end)| offset_usize < *existing_end && end > *start)
        {
            return Ok(None);
        }
        self.received_bytes = self.received_bytes.saturating_add(payload.len());
        self.buffer[offset_usize..end].copy_from_slice(&payload);
        self.ranges.push((offset_usize, end));
        if self.received_bytes < self.total_len {
            return Ok(None);
        }
        self.ranges.sort_unstable_by_key(|(start, _)| *start);
        let mut cursor = 0usize;
        for (start, end) in &self.ranges {
            if *start != cursor {
                return Ok(None);
            }
            cursor = *end;
        }
        if cursor == self.total_len {
            Ok(Some(std::mem::take(&mut self.buffer).freeze()))
        } else {
            Ok(None)
        }
    }
}

impl RttEstimator {
    fn new() -> Self {
        Self {
            srtt: INITIAL_RTT,
            rttvar: INITIAL_RTT / 2,
        }
    }

    fn observe(&mut self, sample: Duration) {
        let srtt = duration_to_secs(self.srtt);
        let rttvar = duration_to_secs(self.rttvar);
        let sample = duration_to_secs(sample);
        let next_rttvar = 0.75 * rttvar + 0.25 * (srtt - sample).abs();
        let next_srtt = 0.875 * srtt + 0.125 * sample;
        self.srtt = secs_to_duration(next_srtt);
        self.rttvar = secs_to_duration(next_rttvar);
    }

    fn rto(self) -> Duration {
        secs_to_duration(duration_to_secs(self.srtt) + 4.0 * duration_to_secs(self.rttvar))
            .clamp(MIN_RTO, MAX_RTO)
    }
}

impl PacketWindow {
    fn insert(&mut self, packet_number: u64, packet: PendingPacket) {
        if self.slots.is_empty() {
            self.base = packet_number;
        }
        if packet_number < self.base {
            return;
        }
        let Ok(index) = usize::try_from(packet_number - self.base) else {
            return;
        };
        if index >= self.slots.len() {
            self.slots.resize_with(index + 1, || None);
        }
        if let Some(existing) = self.slots[index].replace(packet) {
            self.bytes = self.bytes.saturating_sub(existing.packet.len());
        }
        if let Some(packet) = self.slots[index].as_ref() {
            self.bytes = self.bytes.saturating_add(packet.packet.len());
            self.deadlines.push(Reverse(PacketDeadline {
                at: packet.deadline,
                packet_number,
                generation: packet.generation,
            }));
        }
    }

    fn remove_acked_ranges(&mut self, ranges: &[PacketAckRange]) -> Vec<PendingPacket> {
        let mut released = Vec::new();
        for range in ranges {
            if range.start >= range.end || self.slots.is_empty() {
                continue;
            }
            let window_end = self.base.saturating_add(self.slots.len() as u64);
            let start = range.start.max(self.base);
            let end = range.end.min(window_end);
            if start >= end {
                continue;
            }
            let start_index = usize::try_from(start - self.base).unwrap_or(usize::MAX);
            let end_index = usize::try_from(end - self.base).unwrap_or(usize::MAX);
            for index in start_index..end_index.min(self.slots.len()) {
                if let Some(packet) = self.slots[index].take() {
                    self.bytes = self.bytes.saturating_sub(packet.packet.len());
                    released.push(packet);
                }
            }
        }
        self.trim_front();
        released
    }

    fn retransmit_before(
        &mut self,
        ack_frontier: u64,
        now: Instant,
        min_spacing: Duration,
        limit: usize,
    ) -> Vec<Bytes> {
        if ack_frontier <= self.base || self.slots.is_empty() || limit == 0 {
            return Vec::new();
        }
        let end = ack_frontier.min(self.base.saturating_add(self.slots.len() as u64));
        let end_index = usize::try_from(end - self.base).unwrap_or(self.slots.len());
        let mut packets = Vec::new();
        for index in 0..end_index.min(self.slots.len()) {
            let packet_number = self.base.saturating_add(index as u64);
            let Some(packet) = self.slots[index].as_mut() else {
                continue;
            };
            if now.duration_since(packet.last_sent_at) < min_spacing {
                continue;
            }
            let (payload, deadline, generation) = {
                let payload = Self::mark_retransmit(packet, now, min_spacing);
                (payload, packet.deadline, packet.generation)
            };
            self.deadlines.push(Reverse(PacketDeadline {
                at: deadline,
                packet_number,
                generation,
            }));
            packets.push(payload);
            if packets.len() >= limit {
                break;
            }
        }
        packets
    }

    fn due_retransmits(&mut self, now: Instant, rto: Duration, limit: usize) -> Vec<Bytes> {
        let mut packets = Vec::new();
        while packets.len() < limit {
            let Some(Reverse(deadline)) = self.deadlines.peek().copied() else {
                break;
            };
            if deadline.at > now {
                break;
            }
            self.deadlines.pop();
            let Some(packet) = self.get_mut(deadline.packet_number) else {
                continue;
            };
            if packet.generation != deadline.generation || packet.deadline != deadline.at {
                continue;
            }
            let (payload, next_deadline, generation) = {
                let payload = Self::mark_retransmit(packet, now, rto);
                (payload, packet.deadline, packet.generation)
            };
            self.deadlines.push(Reverse(PacketDeadline {
                at: next_deadline,
                packet_number: deadline.packet_number,
                generation,
            }));
            packets.push(payload);
        }
        packets
    }

    fn get_mut(&mut self, packet_number: u64) -> Option<&mut PendingPacket> {
        if packet_number < self.base {
            return None;
        }
        let index = usize::try_from(packet_number - self.base).ok()?;
        self.slots.get_mut(index)?.as_mut()
    }

    fn mark_retransmit(packet: &mut PendingPacket, now: Instant, delay: Duration) -> Bytes {
        packet.last_sent_at = now;
        packet.retransmit_count = packet.retransmit_count.saturating_add(1);
        packet.generation = packet.generation.saturating_add(1);
        packet.deadline = now + delay.max(MIN_RTO);
        packet.packet.clone()
    }

    fn trim_front(&mut self) {
        while matches!(self.slots.front(), Some(None)) {
            self.slots.pop_front();
            self.base = self.base.saturating_add(1);
        }
        if self.slots.is_empty() {
            self.deadlines.clear();
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct UdpPathController {
    rtt: RttEstimator,
    min_rtt: Duration,
    delivery_rate_bps: f64,
    pacing_rate_bps: f64,
    inflight_hi: usize,
    bytes_in_flight: usize,
    last_ack_at: Option<Instant>,
    next_send_at: Instant,
    target_datagram_bytes: usize,
    pmtu_acked_bytes: usize,
    loss_events: u64,
}

impl UdpPathController {
    fn new(mux_limits: MuxLimits) -> Self {
        let fragment = max_frame_fragment_payload().max(1);
        let budget = carrier_pending_byte_budget(mux_limits);
        let startup_inflight = mux_limits
            .max_tcp_path_inflight_bytes
            .clamp(fragment * 64, budget.max(fragment * 64));
        let pacing_rate_bps = bytes_per_rtt_to_bps(startup_inflight, INITIAL_RTT);
        Self {
            rtt: RttEstimator::new(),
            min_rtt: INITIAL_RTT,
            delivery_rate_bps: pacing_rate_bps,
            pacing_rate_bps,
            inflight_hi: startup_inflight,
            bytes_in_flight: 0,
            last_ack_at: None,
            next_send_at: Instant::now(),
            target_datagram_bytes: SAFE_TARGET_DATAGRAM_BYTES,
            pmtu_acked_bytes: 0,
            loss_events: 0,
        }
    }

    fn send_delay(
        &mut self,
        packet_len: usize,
        pending_bytes: usize,
        mux_limits: MuxLimits,
        now: Instant,
    ) -> Option<Duration> {
        self.refresh_limits(mux_limits);
        if pending_bytes.saturating_add(packet_len) > carrier_pending_byte_budget(mux_limits) {
            return Some(self.rto() / RETRANSMIT_TICK_FRACTION);
        }
        if self.bytes_in_flight.saturating_add(packet_len) > self.inflight_hi {
            return Some(self.rtt.srtt / RETRANSMIT_TICK_FRACTION);
        }
        let granularity = pacing_granularity(self.rtt.srtt);
        if self.next_send_at <= now + granularity {
            return None;
        }
        Some(self.next_send_at.duration_since(now))
    }

    fn on_packet_sent(&mut self, packet_len: usize, now: Instant) {
        self.bytes_in_flight = self.bytes_in_flight.saturating_add(packet_len);
        let gap = secs_to_duration(packet_len as f64 * 8.0 / self.pacing_rate_bps.max(1.0));
        let base = self.next_send_at.max(now);
        self.next_send_at = base + gap;
    }

    fn on_packets_acked(
        &mut self,
        released: &[PendingPacket],
        now: Instant,
        mux_limits: MuxLimits,
    ) {
        if released.is_empty() {
            return;
        }
        let mut delivered = 0usize;
        for packet in released {
            delivered = delivered.saturating_add(packet.packet.len());
            self.bytes_in_flight = self.bytes_in_flight.saturating_sub(packet.packet.len());
            if packet.retransmit_count == 0 {
                let sample = now.duration_since(packet.sent_at);
                self.rtt.observe(sample);
                self.min_rtt = self.min_rtt.min(sample);
            }
        }
        if let Some(last_ack_at) = self.last_ack_at {
            let interval = now.duration_since(last_ack_at);
            if interval >= Duration::from_millis(1) {
                let sample_rate = delivered as f64 * 8.0 / duration_to_secs(interval);
                self.delivery_rate_bps = smooth_rate(self.delivery_rate_bps, sample_rate, 0.125);
            }
        }
        self.last_ack_at = Some(now);
        self.inflight_hi = self
            .inflight_hi
            .saturating_add(delivered)
            .min(carrier_pending_byte_budget(mux_limits));
        self.pmtu_acked_bytes = self.pmtu_acked_bytes.saturating_add(delivered);
        self.maybe_grow_datagram_size();
        self.refresh_limits(mux_limits);
        #[cfg(feature = "lab-diagnostics")]
        crate::lab_diagnostics::lab_diagnostic(
            "udp_controller_ack",
            format_args!(
                "delivered_bytes={} inflight_bytes={} inflight_hi={} target_datagram_bytes={} srtt_ms={:.3} min_rtt_ms={:.3} delivery_rate_mbps={:.3} pacing_rate_mbps={:.3}",
                delivered,
                self.bytes_in_flight,
                self.inflight_hi,
                self.target_datagram_bytes,
                self.rtt.srtt.as_secs_f64() * 1000.0,
                self.min_rtt.as_secs_f64() * 1000.0,
                self.delivery_rate_bps / 1_000_000.0,
                self.pacing_rate_bps / 1_000_000.0,
            ),
        );
    }

    fn on_loss(&mut self, lost_bytes: usize, mux_limits: MuxLimits) {
        if lost_bytes == 0 {
            return;
        }
        self.loss_events = self.loss_events.saturating_add(1);
        let fragment = max_frame_fragment_payload().max(1);
        let min_flight = fragment * 16;
        let floor = self
            .bytes_in_flight
            .saturating_add(fragment * 4)
            .max(min_flight);
        let reduced = self.inflight_hi.saturating_mul(85) / 100;
        self.inflight_hi = reduced
            .max(floor)
            .min(carrier_pending_byte_budget(mux_limits));
        self.delivery_rate_bps *= 0.95;
        self.target_datagram_bytes =
            SAFE_TARGET_DATAGRAM_BYTES.max(self.target_datagram_bytes.saturating_mul(9) / 10);
        self.pmtu_acked_bytes = 0;
        self.refresh_limits(mux_limits);
        #[cfg(feature = "lab-diagnostics")]
        crate::lab_diagnostics::lab_diagnostic(
            "udp_controller_loss",
            format_args!(
                "lost_bytes={} inflight_bytes={} inflight_hi={} target_datagram_bytes={} loss_events={} pacing_rate_mbps={:.3}",
                lost_bytes,
                self.bytes_in_flight,
                self.inflight_hi,
                self.target_datagram_bytes,
                self.loss_events,
                self.pacing_rate_bps / 1_000_000.0,
            ),
        );
    }

    fn rto(self) -> Duration {
        self.rtt.rto()
    }

    fn frame_fragment_payload_len(self) -> usize {
        max_frame_fragment_payload_for_datagram(self.target_datagram_bytes)
    }

    fn refresh_limits(&mut self, mux_limits: MuxLimits) {
        let fragment = max_frame_fragment_payload().max(1);
        let min_flight = fragment * 16;
        let budget = carrier_pending_byte_budget(mux_limits);
        self.inflight_hi = self.inflight_hi.clamp(min_flight, budget.max(min_flight));
        let bdp_rate = bytes_per_rtt_to_bps(self.inflight_hi, self.min_rtt.max(MIN_RTO));
        self.pacing_rate_bps = self.delivery_rate_bps.max(bdp_rate).max(1.0);
    }

    fn maybe_grow_datagram_size(&mut self) {
        if self.target_datagram_bytes >= MAX_PROBED_DATAGRAM_BYTES {
            return;
        }
        let probe_interval = self.target_datagram_bytes.saturating_mul(256);
        if self.pmtu_acked_bytes < probe_interval {
            return;
        }
        self.pmtu_acked_bytes = 0;
        let step = (self.target_datagram_bytes / 16).clamp(16, 64);
        self.target_datagram_bytes = self
            .target_datagram_bytes
            .saturating_add(step)
            .min(MAX_PROBED_DATAGRAM_BYTES);
    }
}

async fn run_endpoint_reader(endpoint: Arc<EndpointInner>) {
    let mut buffer = vec![0u8; MAX_UDP_PACKET_BYTES];
    loop {
        let (len, source) = match endpoint.socket.recv_from(&mut buffer).await {
            Ok(received) => received,
            Err(err) => {
                eprintln!("warning: UDP carrier socket receive failed: {err}");
                return;
            }
        };
        let packet = &buffer[..len];
        if let Err(err) = process_endpoint_packet(&endpoint, source, packet).await
            && !matches!(
                err,
                UdpCarrierFrameError::Crypto | UdpCarrierFrameError::InvalidPacket(_)
            )
        {
            eprintln!("warning: UDP carrier packet failed: {err}");
        }
    }
}

async fn process_endpoint_packet(
    endpoint: &Arc<EndpointInner>,
    source: SocketAddr,
    packet: &[u8],
) -> Result<(), UdpCarrierFrameError> {
    let connection_id =
        peek_connection_id(packet).ok_or(UdpCarrierFrameError::InvalidPacket("missing header"))?;
    let existing = endpoint
        .connections
        .lock()
        .expect("UDP carrier connection map poisoned")
        .get(&connection_id)
        .cloned();

    if let Some(connection) = existing {
        let (header, payload) =
            decode_packet(&connection.cipher, packet, connection.role.recv_direction())?;
        return connection.process_packet(source, header, payload).await;
    }

    if endpoint.role != CarrierRole::Server {
        return Err(UdpCarrierFrameError::InvalidPacket("unknown connection"));
    }

    let cipher = PacketCipher::new(&endpoint.secret, endpoint.cipher_suite, connection_id)
        .map_err(|_| UdpCarrierFrameError::Crypto)?;
    let (header, payload) = decode_packet(&cipher, packet, endpoint.role.recv_direction())?;
    let connection = ConnectionInner::new(ConnectionParams {
        socket: endpoint.socket.clone(),
        role: endpoint.role,
        peer: source,
        connection_id,
        secret: &endpoint.secret,
        cipher_suite: endpoint.cipher_suite,
        mux_limits: endpoint.mux_limits,
        codec_limits: endpoint.codec_limits,
    })
    .map_err(|err| match err {
        UdpCarrierTransportError::Io(err) => UdpCarrierFrameError::Io(err),
        UdpCarrierTransportError::EmptySecret | UdpCarrierTransportError::Random(_) => {
            UdpCarrierFrameError::Crypto
        }
    })?;
    endpoint
        .connections
        .lock()
        .expect("UDP carrier connection map poisoned")
        .insert(connection_id, connection.clone());
    let _ = endpoint
        .incoming
        .send(Connection {
            inner: connection.clone(),
        })
        .await;
    connection.process_packet(source, header, payload).await
}

async fn run_connection_commands(
    connection: Arc<ConnectionInner>,
    mut commands: mpsc::Receiver<StreamCommand>,
) {
    while let Some(command) = commands.recv().await {
        let result = match command {
            StreamCommand::SendFrame {
                ordered,
                reliable,
                stream_id,
                frame_id,
                encoded,
            } => {
                send_frame_fragments(&connection, stream_id, frame_id, encoded, ordered, reliable)
                    .await
            }
            StreamCommand::Finish => Ok(()),
        };
        if let Err(err) = result {
            eprintln!("warning: UDP carrier send failed: {err}");
        }
    }
}

async fn send_frame_fragments(
    connection: &ConnectionInner,
    stream_id: u64,
    frame_id: u64,
    encoded: Bytes,
    ordered: bool,
    reliable: bool,
) -> Result<(), UdpCarrierFrameError> {
    if encoded.len() > connection.codec_limits.max_frame_bytes {
        return Err(UdpCarrierFrameError::FrameTooLarge {
            actual: encoded.len(),
            limit: connection.codec_limits.max_frame_bytes,
        });
    }
    let total_len =
        u32::try_from(encoded.len()).map_err(|_| UdpCarrierFrameError::FrameTooLarge {
            actual: encoded.len(),
            limit: u32::MAX as usize,
        })?;
    let fragment_payload = connection.frame_fragment_payload_len().await.max(1);
    for index in 0..encoded.len().div_ceil(fragment_payload) {
        let start =
            index
                .checked_mul(fragment_payload)
                .ok_or(UdpCarrierFrameError::InvalidPacket(
                    "fragment offset overflow",
                ))?;
        let end = start.saturating_add(fragment_payload).min(encoded.len());
        let offset = u32::try_from(start)
            .map_err(|_| UdpCarrierFrameError::InvalidPacket("fragment offset overflow"))?;
        connection
            .send_payload(
                PacketPayload::FrameFragment {
                    ordered,
                    ack_eliciting: reliable,
                    stream_id,
                    frame_id,
                    offset,
                    total_len,
                    payload: encoded.slice(start..end),
                },
                reliable,
            )
            .await?;
    }
    Ok(())
}

async fn run_connection_retransmit(connection: Arc<ConnectionInner>) {
    loop {
        if connection.closed.load(Ordering::Relaxed)
            && connection.pending_bytes.load(Ordering::Relaxed) == 0
        {
            return;
        }
        let rto = connection.retransmit_delay().await;
        tokio::time::sleep((rto / RETRANSMIT_TICK_FRACTION).max(Duration::from_millis(1))).await;
        let now = Instant::now();
        let due = {
            let mut pending = connection.pending.lock().await;
            pending.due_retransmits(now, rto, ACK_FLUSH_PACKET_THRESHOLD * 4)
        };
        if due.is_empty() {
            continue;
        }
        {
            let retransmit_bytes = due
                .iter()
                .fold(0usize, |sum, packet| sum.saturating_add(packet.len()));
            connection
                .controller
                .lock()
                .await
                .on_loss(retransmit_bytes, connection.mux_limits);
        }
        let peer = *connection.peer.read().await;
        for packet in due {
            if let Err(err) = connection.socket.send_to(&packet, peer).await {
                eprintln!("warning: UDP carrier retransmit failed: {err}");
                break;
            }
        }
    }
}

fn carrier_connection_queue(mux_limits: MuxLimits) -> usize {
    mux_limits.max_streams.clamp(1, 1024)
}

fn carrier_stream_accept_queue(mux_limits: MuxLimits) -> usize {
    mux_limits.max_streams.clamp(16, 4096)
}

fn carrier_command_queue(mux_limits: MuxLimits) -> usize {
    let unit = max_frame_fragment_payload().max(1);
    mux_limits
        .max_stream_window_bytes
        .saturating_div(unit as u64)
        .clamp(64, 4096) as usize
}

fn carrier_stream_frame_queue(mux_limits: MuxLimits) -> usize {
    let unit = max_frame_fragment_payload().max(1);
    mux_limits
        .max_reorder_bytes
        .saturating_div(unit)
        .clamp(64, 4096)
}

fn carrier_pending_byte_budget(mux_limits: MuxLimits) -> usize {
    let window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX / 2);
    window
        .saturating_add(mux_limits.max_repair_bytes)
        .max(max_frame_fragment_payload() * 64)
}

fn packet_ack_ranges(packets: &mut Vec<u64>) -> Vec<PacketAckRange> {
    packets.sort_unstable();
    packets.dedup();
    let mut ranges = Vec::new();
    let mut current: Option<PacketAckRange> = None;
    for packet_number in packets.iter() {
        match current.as_mut() {
            Some(range) if range.end == *packet_number => {
                range.end = range.end.saturating_add(1);
            }
            Some(_) => {
                if let Some(range) = current.take() {
                    ranges.push(range);
                }
                current = Some(PacketAckRange {
                    start: *packet_number,
                    end: packet_number.saturating_add(1),
                });
            }
            None => {
                current = Some(PacketAckRange {
                    start: *packet_number,
                    end: packet_number.saturating_add(1),
                });
            }
        }
        if ranges.len() >= MAX_ACK_RANGES_PER_PACKET {
            break;
        }
    }
    if ranges.len() < MAX_ACK_RANGES_PER_PACKET
        && let Some(range) = current
    {
        ranges.push(range);
    }
    ranges
}

fn random_connection_id() -> Result<u64, UdpCarrierTransportError> {
    let mut bytes = [0u8; 8];
    getrandom::getrandom(&mut bytes)?;
    let mut id = u64::from_be_bytes(bytes);
    if id == 0 {
        id = 1;
    }
    Ok(id)
}

fn duration_to_secs(value: Duration) -> f64 {
    value.as_secs_f64().max(0.000_001)
}

fn secs_to_duration(value: f64) -> Duration {
    Duration::from_secs_f64(value.max(0.000_001))
}

fn bytes_per_rtt_to_bps(bytes: usize, rtt: Duration) -> f64 {
    bytes as f64 * 8.0 / duration_to_secs(rtt)
}

fn smooth_rate(previous: f64, sample: f64, alpha: f64) -> f64 {
    if !previous.is_finite() || previous <= 0.0 {
        return sample.max(1.0);
    }
    previous * (1.0 - alpha) + sample.max(1.0) * alpha
}

fn pacing_granularity(srtt: Duration) -> Duration {
    (srtt / 128)
        .max(Duration::from_micros(250))
        .min(Duration::from_millis(2))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_assembly_waits_for_all_fragments_and_reassembles_in_order() {
        let mut assembly = FrameAssembly {
            total_len: 11,
            received_bytes: 0,
            buffer: BytesMut::zeroed(11),
            ranges: Vec::new(),
        };
        assert!(
            assembly
                .insert(6, 11, Bytes::from_static(b"world"))
                .expect("second fragment")
                .is_none()
        );
        let completed = assembly
            .insert(0, 11, Bytes::from_static(b"hello "))
            .expect("first fragment")
            .expect("complete");
        assert_eq!(&completed[..], b"hello world");
    }

    #[test]
    fn rtt_estimator_is_adaptive_and_bounded() {
        let mut rtt = RttEstimator::new();
        let initial = rtt.rto();
        for _ in 0..8 {
            rtt.observe(Duration::from_millis(20));
        }
        assert!(rtt.rto() < initial);
        for _ in 0..10 {
            rtt.observe(Duration::from_secs(10));
        }
        assert_eq!(rtt.rto(), MAX_RTO);
    }
}
