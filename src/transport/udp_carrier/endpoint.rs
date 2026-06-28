use super::crypto::{CarrierRole, PacketCipher};
use super::error::{UdpCarrierConnectionError, UdpCarrierFrameError, UdpCarrierTransportError};
use super::packet::{
    PacketAckRange, PacketHeader, PacketPayload, decode_packet, encode_packet,
    max_frame_fragment_payload, peek_connection_id,
};
use super::stream::{RecvStream, SendStream, StreamCommand};
use crate::config::CipherSuite;
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, RwLock, mpsc};

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
    pending: Mutex<BTreeMap<u64, PendingPacket>>,
    pending_bytes: AtomicUsize,
    ack_state: Mutex<AckState>,
    rtt: Mutex<RttEstimator>,
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
    frames: mpsc::Sender<Vec<u8>>,
    assemblies: BTreeMap<u64, FrameAssembly>,
    completed: BTreeMap<u64, Vec<u8>>,
    next_frame_id: u64,
}

#[derive(Debug)]
struct FrameAssembly {
    total_len: usize,
    received_bytes: usize,
    fragments: BTreeMap<u32, Vec<u8>>,
}

#[derive(Debug, Default)]
struct AckState {
    pending: BTreeSet<u64>,
    scheduled: bool,
}

#[derive(Debug, Clone)]
struct PendingPacket {
    packet: Vec<u8>,
    sent_at: Instant,
    last_sent_at: Instant,
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
            pending: Mutex::new(BTreeMap::new()),
            pending_bytes: AtomicUsize::new(0),
            ack_state: Mutex::new(AckState::default()),
            rtt: Mutex::new(RttEstimator::new()),
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
        if reliable {
            self.wait_for_pending_capacity().await;
        }
        let packet_number = self.send_packet_number.fetch_add(1, Ordering::Relaxed);
        let header = PacketHeader {
            direction: self.role.send_direction(),
            connection_id: self.connection_id,
            packet_number,
        };
        let packet = encode_packet(&self.cipher, header, &payload)?;
        let peer = *self.peer.read().await;
        if reliable {
            let now = Instant::now();
            self.pending_bytes
                .fetch_add(packet.len(), Ordering::Relaxed);
            self.pending.lock().await.insert(
                packet_number,
                PendingPacket {
                    packet: packet.clone(),
                    sent_at: now,
                    last_sent_at: now,
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

    async fn wait_for_pending_capacity(&self) {
        while self.pending_bytes.load(Ordering::Relaxed)
            > carrier_pending_byte_budget(self.mux_limits)
            && !self.closed.load(Ordering::Relaxed)
        {
            let delay = self.retransmit_delay().await / RETRANSMIT_TICK_FRACTION;
            tokio::time::sleep(delay.max(Duration::from_millis(1))).await;
        }
    }

    async fn retransmit_delay(&self) -> Duration {
        self.rtt.lock().await.rto()
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
            state.pending.insert(packet_number);
            if state.pending.len() >= ACK_FLUSH_PACKET_THRESHOLD {
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
            let ranges = packet_ack_ranges(&state.pending);
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
        let mut released = Vec::new();
        let ack_frontier = ranges.iter().map(|range| range.end).max().unwrap_or(0);
        let now = Instant::now();
        let fast_retransmit_spacing = self.retransmit_delay().await / 2;
        let fast_retransmit: Vec<Vec<u8>>;
        {
            let mut pending = self.pending.lock().await;
            for range in ranges {
                let acked = pending
                    .range(range.start..range.end)
                    .map(|(packet_number, _)| *packet_number)
                    .collect::<Vec<_>>();
                for packet_number in acked {
                    if let Some(packet) = pending.remove(&packet_number) {
                        released.push(packet);
                    }
                }
            }
            fast_retransmit = pending
                .range_mut(..ack_frontier)
                .filter_map(|(_, packet)| {
                    if now.duration_since(packet.last_sent_at) >= fast_retransmit_spacing {
                        packet.last_sent_at = now;
                        Some(packet.packet.clone())
                    } else {
                        None
                    }
                })
                .take(ACK_FLUSH_PACKET_THRESHOLD * 2)
                .collect();
        }
        let mut rtt = self.rtt.lock().await;
        for packet in released {
            self.pending_bytes
                .fetch_sub(packet.packet.len(), Ordering::Relaxed);
            rtt.observe(packet.sent_at.elapsed());
        }
        drop(rtt);
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
        payload: Vec<u8>,
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
                    fragments: BTreeMap::new(),
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
        payload: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, UdpCarrierFrameError> {
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
        if self.fragments.contains_key(&offset) {
            return Ok(None);
        }
        self.received_bytes = self.received_bytes.saturating_add(payload.len());
        self.fragments.insert(offset, payload);
        if self.received_bytes < self.total_len {
            return Ok(None);
        }
        let mut out = Vec::with_capacity(self.total_len);
        for (fragment_offset, fragment) in &self.fragments {
            if out.len() != *fragment_offset as usize {
                return Ok(None);
            }
            out.extend_from_slice(fragment);
        }
        if out.len() == self.total_len {
            Ok(Some(out))
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
    encoded: Vec<u8>,
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
    let fragment_payload = max_frame_fragment_payload();
    for (index, chunk) in encoded.chunks(fragment_payload).enumerate() {
        let offset = index
            .checked_mul(fragment_payload)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or(UdpCarrierFrameError::InvalidPacket(
                "fragment offset overflow",
            ))?;
        connection
            .send_payload(
                PacketPayload::FrameFragment {
                    ordered,
                    ack_eliciting: reliable,
                    stream_id,
                    frame_id,
                    offset,
                    total_len,
                    payload: chunk.to_vec(),
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
            pending
                .values_mut()
                .filter_map(|packet| {
                    if now.duration_since(packet.last_sent_at) >= rto {
                        packet.last_sent_at = now;
                        Some(packet.packet.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };
        if due.is_empty() {
            continue;
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

fn packet_ack_ranges(packets: &BTreeSet<u64>) -> Vec<PacketAckRange> {
    let mut ranges = Vec::new();
    let mut current: Option<PacketAckRange> = None;
    for packet_number in packets {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_assembly_waits_for_all_fragments_and_reassembles_in_order() {
        let mut assembly = FrameAssembly {
            total_len: 11,
            received_bytes: 0,
            fragments: BTreeMap::new(),
        };
        assert!(
            assembly
                .insert(6, 11, b"world".to_vec())
                .expect("second fragment")
                .is_none()
        );
        let completed = assembly
            .insert(0, 11, b"hello ".to_vec())
            .expect("first fragment")
            .expect("complete");
        assert_eq!(completed, b"hello world");
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
