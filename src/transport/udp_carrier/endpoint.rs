use super::ack::{
    ACK_FLUSH_PACKET_THRESHOLD, ACK_IMMEDIATE_MIN_INTERVAL, AckState, packet_ack_ranges,
};
use super::assembly::{
    ClosedStreamCache, FrameAssembly, FrameKey, OrphanFragment, OrphanFragmentBuffer, StreamState,
    new_stream_pair,
};
use super::controller::{PTO_PROBE_PACKET_LIMIT, RETRANSMIT_TICK_FRACTION, UdpPathController};
use super::crypto::{CarrierRole, PacketCipher};
use super::error::{UdpCarrierConnectionError, UdpCarrierFrameError, UdpCarrierTransportError};
use super::packet::{
    PacketAckRange, PacketHeader, PacketPayload, decode_packet, encode_packet, encoded_packet_len,
    max_frame_fragment_payload, peek_connection_id,
};
use super::send::{GsoSendOutcome, send_udp_segments};
use super::stream::{RecvStream, SendStream, StreamCommand};
use super::window::{PacketSample, PacketWindow, PendingPacket};
use crate::config::CipherSuite;
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use bytes::Bytes;
use std::collections::{HashMap, VecDeque, hash_map::Entry};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, Notify, RwLock, mpsc};

#[cfg(test)]
use super::ack::MAX_ACK_RANGES_PER_PACKET;
#[cfg(test)]
use super::assembly::{CLOSED_STREAM_DEDUP_WINDOW, UNORDERED_DEDUP_WINDOW};
#[cfg(test)]
use super::controller::{
    INITIAL_RTT, MAX_RTO, PACKET_LOSS_THRESHOLD, RttEstimator, STARTUP_MAX_FLIGHT_PACKETS,
    STARTUP_MIN_FLIGHT_PACKETS, STARTUP_PACING_GAIN, bytes_per_rtt_to_bps,
    carrier_pending_byte_budget,
};
#[cfg(test)]
use super::window::AckedPacket;

const MAX_UDP_PACKET_BYTES: usize = 65_535;
const STREAM_ID_CLIENT_FIRST: u64 = 1;
const STREAM_ID_SERVER_FIRST: u64 = 2;
const GSO_UNKNOWN: u8 = 0;
const GSO_AVAILABLE: u8 = 1;
const GSO_UNAVAILABLE: u8 = 2;
const UDP_GSO_MAX_SEGMENTS: usize = 64;

#[derive(Debug)]
pub struct Endpoint {
    inner: Arc<EndpointInner>,
    incoming: Mutex<mpsc::Receiver<Connection>>,
}

#[derive(Debug, Clone, Copy)]
pub struct UdpCarrierPathMetrics {
    pub direction: u8,
    pub srtt: Duration,
    pub rttvar: Duration,
    pub min_rtt: Duration,
    pub min_rtt_observed: bool,
    pub delivery_rate_bps: f64,
    pub pacing_rate_bps: f64,
    pub inflight_hi: usize,
    pub bytes_in_flight: usize,
    pub pending_bytes: usize,
    pub target_datagram_bytes: usize,
    pub loss_events: u64,
    pub spurious_loss_events: u64,
    pub packet_loss_threshold: u64,
    pub pto_count: u32,
    pub app_limited: bool,
    pub delivery_sample_count: u64,
    pub last_delivery_sample_at: Option<Instant>,
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
    closed_streams: Mutex<ClosedStreamCache>,
    orphans: Mutex<OrphanFragmentBuffer>,
    incoming_streams: mpsc::Sender<(SendStream, RecvStream)>,
    incoming_streams_rx: Mutex<mpsc::Receiver<(SendStream, RecvStream)>>,
    pending: Mutex<PacketWindow>,
    pending_bytes: AtomicUsize,
    send_notify: Notify,
    ack_state: Mutex<AckState>,
    controller: Mutex<UdpPathController>,
    gso_state: AtomicU8,
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
        let (state, streams) = new_stream_pair(
            stream_id,
            self.inner.commands.clone(),
            carrier_stream_frame_queue(self.inner.mux_limits),
        );
        self.inner.streams.lock().await.insert(stream_id, state);
        Ok(streams)
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

    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Relaxed)
    }

    pub async fn tx_metrics(&self) -> UdpCarrierPathMetrics {
        let snapshot = self.inner.controller.lock().await.snapshot();
        UdpCarrierPathMetrics {
            direction: snapshot.direction,
            srtt: snapshot.srtt,
            rttvar: snapshot.rttvar,
            min_rtt: snapshot.min_rtt,
            min_rtt_observed: snapshot.min_rtt_observed,
            delivery_rate_bps: snapshot.delivery_rate_bps,
            pacing_rate_bps: snapshot.pacing_rate_bps,
            inflight_hi: snapshot.inflight_hi,
            bytes_in_flight: snapshot.bytes_in_flight,
            pending_bytes: self.inner.pending_bytes.load(Ordering::Relaxed),
            target_datagram_bytes: snapshot.target_datagram_bytes,
            loss_events: snapshot.loss_events,
            spurious_loss_events: snapshot.spurious_loss_events,
            packet_loss_threshold: snapshot.packet_loss_threshold,
            pto_count: snapshot.pto_count,
            app_limited: snapshot.app_limited,
            delivery_sample_count: snapshot.delivery_sample_count,
            last_delivery_sample_at: snapshot.last_delivery_sample_at,
        }
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
            closed_streams: Mutex::new(ClosedStreamCache::default()),
            orphans: Mutex::new(OrphanFragmentBuffer::default()),
            incoming_streams: incoming_streams_tx,
            incoming_streams_rx: Mutex::new(incoming_streams_rx),
            pending: Mutex::new(PacketWindow::default()),
            pending_bytes: AtomicUsize::new(0),
            send_notify: Notify::new(),
            ack_state: Mutex::new(AckState::default()),
            controller: Mutex::new(UdpPathController::new(mux_limits, role.send_direction())),
            gso_state: AtomicU8::new(GSO_UNKNOWN),
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
        self.send_payload_with_sample(payload, reliable, PacketSample::Control)
            .await
    }

    async fn send_payload_with_sample(
        &self,
        payload: PacketPayload,
        reliable: bool,
        sample: PacketSample,
    ) -> Result<(), UdpCarrierFrameError> {
        self.send_payload_with_options(payload, reliable, sample, false)
            .await
    }

    async fn send_recovery_payload_with_sample(
        &self,
        payload: PacketPayload,
        sample: PacketSample,
    ) -> Result<(), UdpCarrierFrameError> {
        self.send_payload_with_options(payload, true, sample, true)
            .await
    }

    async fn send_payload_with_options(
        &self,
        payload: PacketPayload,
        reliable: bool,
        sample: PacketSample,
        recovery_allowance: bool,
    ) -> Result<(), UdpCarrierFrameError> {
        let packet_number = self.send_packet_number.fetch_add(1, Ordering::Relaxed);
        let header = PacketHeader {
            direction: self.role.send_direction(),
            connection_id: self.connection_id,
            packet_number,
        };
        let packet = Bytes::from(encode_packet(&self.cipher, header, &payload)?);
        let pending_bytes_before = self.pending_bytes.load(Ordering::Relaxed);
        if reliable && !recovery_allowance {
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
                    payload,
                    encoded_len: packet.len(),
                    sample: sample.with_app_limited(pending_bytes_before == 0),
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

    async fn prepare_payload_for_segment(
        &self,
        payload: PacketPayload,
        sample: PacketSample,
    ) -> Result<Bytes, UdpCarrierFrameError> {
        let packet_number = self.send_packet_number.fetch_add(1, Ordering::Relaxed);
        let header = PacketHeader {
            direction: self.role.send_direction(),
            connection_id: self.connection_id,
            packet_number,
        };
        let packet = Bytes::from(encode_packet(&self.cipher, header, &payload)?);
        let pending_bytes_before = self.pending_bytes.load(Ordering::Relaxed);
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
                payload,
                encoded_len: packet.len(),
                sample: sample.with_app_limited(pending_bytes_before == 0),
                sent_at: now,
                last_sent_at: now,
                deadline,
                generation: 0,
                retransmit_count: 0,
            },
        );
        Ok(packet)
    }

    async fn send_capacity_delay(&self, packet_len: usize) -> Option<Duration> {
        let pending_bytes = self.pending_bytes.load(Ordering::Relaxed);
        let mut controller = self.controller.lock().await;
        controller.send_delay(packet_len, pending_bytes, self.mux_limits, Instant::now())
    }

    async fn packet_run_segment_budget(&self, packet_len: usize) -> usize {
        self.controller
            .lock()
            .await
            .packet_run_segment_budget(packet_len, UDP_GSO_MAX_SEGMENTS)
    }

    async fn write_prepared_packet(&self, packet: &Bytes) -> Result<(), UdpCarrierFrameError> {
        let peer = *self.peer.read().await;
        #[cfg(feature = "lab-diagnostics")]
        let started = Instant::now();
        self.socket.send_to(packet, peer).await?;
        #[cfg(feature = "lab-diagnostics")]
        crate::lab_diagnostics::lab_perf_record(
            "transport.udp_carrier.write_socket_wait",
            started.elapsed(),
            packet.len(),
        );
        Ok(())
    }

    async fn write_segment_run(
        &self,
        packets: &mut Vec<Bytes>,
    ) -> Result<(), UdpCarrierFrameError> {
        if packets.is_empty() {
            return Ok(());
        }
        if packets.len() < 2 || self.gso_state.load(Ordering::Relaxed) == GSO_UNAVAILABLE {
            for packet in packets.drain(..) {
                self.write_prepared_packet(&packet).await?;
            }
            return Ok(());
        }

        let segment_len = packets[0].len();
        if packets.iter().any(|packet| packet.len() != segment_len) {
            for packet in packets.drain(..) {
                self.write_prepared_packet(&packet).await?;
            }
            return Ok(());
        }

        let peer = *self.peer.read().await;
        #[cfg(feature = "lab-diagnostics")]
        let started = Instant::now();
        match send_udp_segments(&self.socket, peer, packets, segment_len).await? {
            GsoSendOutcome::Sent => {
                self.gso_state.store(GSO_AVAILABLE, Ordering::Relaxed);
                #[cfg(feature = "lab-diagnostics")]
                {
                    let bytes = packets
                        .iter()
                        .fold(0usize, |sum, packet| sum.saturating_add(packet.len()));
                    crate::lab_diagnostics::lab_perf_record(
                        "transport.udp_carrier.write_socket_wait",
                        started.elapsed(),
                        bytes,
                    );
                    crate::lab_diagnostics::lab_diagnostic(
                        "udp_carrier_gso_send",
                        format_args!(
                            "segments={} segment_len={} bytes={}",
                            packets.len(),
                            segment_len,
                            bytes
                        ),
                    );
                }
                packets.clear();
            }
            GsoSendOutcome::Unsupported => {
                self.gso_state.store(GSO_UNAVAILABLE, Ordering::Relaxed);
                for packet in packets.drain(..) {
                    self.write_prepared_packet(&packet).await?;
                }
            }
        }
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
            PacketPayload::Ack {
                largest_acked,
                ack_delay_us,
                ranges,
            } => {
                self.apply_ack_ranges(&ranges, largest_acked, ack_delay_us)
                    .await;
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
                self.receive_fragment(stream_id, frame_id, offset, total_len, payload, ordered)
                    .await?;
                if ack_eliciting {
                    self.queue_ack(header.packet_number).await;
                }
            }
            PacketPayload::CloseStream { stream_id } => {
                self.queue_ack(header.packet_number).await;
                self.close_remote_stream(stream_id).await;
            }
        }
        Ok(())
    }

    async fn close_remote_stream(&self, stream_id: u64) {
        self.closed_streams.lock().await.remember(stream_id);
        self.streams.lock().await.remove(&stream_id);
    }

    async fn queue_ack(self: &Arc<Self>, packet_number: u64) {
        let now = Instant::now();
        let default_ack_delay = (self.retransmit_delay().await / 8).min(Duration::from_millis(5));
        let mut schedule_ack_flush = None;
        let should_flush = {
            let mut state = self.ack_state.lock().await;
            let gap_observed =
                state.largest_seen != 0 && packet_number > state.largest_seen.saturating_add(1);
            let was_new = state.remember_received(packet_number, now);
            let filled_gap = was_new && packet_number < state.largest_seen;
            if packet_number >= state.largest_seen {
                state.largest_seen = packet_number;
            }
            if state
                .pending_largest_acked
                .is_none_or(|largest| packet_number >= largest)
            {
                state.pending_largest_acked = Some(packet_number);
                state.pending_largest_acked_at = Some(now);
            }
            state.pending.push(packet_number);
            if gap_observed || filled_gap {
                let earliest_flush = state
                    .last_flush_at
                    .map(|last| last + ACK_IMMEDIATE_MIN_INTERVAL)
                    .unwrap_or(now);
                if earliest_flush <= now {
                    Some(true)
                } else {
                    if !state.scheduled {
                        state.scheduled = true;
                        schedule_ack_flush = Some(earliest_flush.duration_since(now));
                    }
                    None
                }
            } else if state.pending.len() >= ACK_FLUSH_PACKET_THRESHOLD {
                Some(gap_observed)
            } else if !state.scheduled {
                state.scheduled = true;
                schedule_ack_flush = Some(default_ack_delay);
                None
            } else {
                None
            }
        };
        if let Some(delay) = schedule_ack_flush {
            if delay.is_zero() {
                self.flush_acks(false).await;
            } else {
                let connection = self.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    connection.flush_acks(false).await;
                });
            }
        }
        if let Some(force) = should_flush {
            self.flush_acks(force).await;
        }
    }

    async fn flush_acks(self: &Arc<Self>, _force: bool) {
        let ack = {
            let mut state = self.ack_state.lock().await;
            if state.pending.is_empty() {
                state.scheduled = false;
                return;
            }
            let now = Instant::now();
            state.scheduled = false;
            state.last_flush_at = Some(now);
            let mut ack_packets = state.ack_packets();
            let ranges = packet_ack_ranges(&mut ack_packets);
            state.pending.clear();
            let largest_acked = ranges
                .iter()
                .map(|range| range.end.saturating_sub(1))
                .max()
                .unwrap_or_else(|| state.pending_largest_acked.unwrap_or(state.largest_seen));
            let ack_delay_us = state
                .received_at(largest_acked)
                .or(state.pending_largest_acked_at)
                .map(|received_at| {
                    let micros = now.saturating_duration_since(received_at).as_micros();
                    u32::try_from(micros).unwrap_or(u32::MAX)
                })
                .unwrap_or(0);
            state.pending_largest_acked = None;
            state.pending_largest_acked_at = None;
            (largest_acked, ack_delay_us, ranges)
        };
        let (largest_acked, ack_delay_us, ranges) = ack;
        if let Err(err) = self
            .send_payload(
                PacketPayload::Ack {
                    largest_acked,
                    ack_delay_us,
                    ranges,
                },
                false,
            )
            .await
        {
            eprintln!("warning: UDP carrier ACK send failed: {err}");
        }
    }

    async fn apply_ack_ranges(
        &self,
        ranges: &[PacketAckRange],
        largest_acked: u64,
        ack_delay_us: u32,
    ) {
        let now = Instant::now();
        let fast_retransmit_spacing = self.retransmit_delay().await / 2;
        let acked = {
            let mut pending = self.pending.lock().await;
            pending.remove_acked_ranges(ranges)
        };
        let mut released_bytes = 0usize;
        for acked in &acked.released {
            self.pending_bytes
                .fetch_sub(acked.packet.encoded_len, Ordering::Relaxed);
            released_bytes = released_bytes.saturating_add(acked.packet.encoded_len);
        }
        let fast_retransmit = {
            let mut controller = self.controller.lock().await;
            let loss = controller.on_packets_acked(
                &acked.released,
                acked.spurious_losses,
                Duration::from_micros(ack_delay_us as u64),
                now,
                self.mux_limits,
            );
            let mut pending = self.pending.lock().await;
            pending.detect_losses(
                largest_acked,
                now,
                loss.packet_threshold,
                loss.time_threshold,
                fast_retransmit_spacing,
                ACK_FLUSH_PACKET_THRESHOLD * 2,
            )
        };
        let lost_bytes = fast_retransmit
            .iter()
            .fold(0usize, |sum, packet| sum.saturating_add(packet.encoded_len));
        if lost_bytes > 0 {
            self.pending_bytes.fetch_sub(lost_bytes, Ordering::Relaxed);
        }
        {
            let mut controller = self.controller.lock().await;
            if lost_bytes > 0 {
                controller.on_loss(lost_bytes, self.mux_limits);
            }
        }
        #[cfg(feature = "lab-diagnostics")]
        crate::lab_diagnostics::lab_diagnostic(
            "udp_carrier_ack_applied",
            format_args!(
                "connection_id={} largest_acked={} ack_delay_us={} ranges={} released_packets={} released_bytes={} lost_packets={} lost_bytes={} spurious_losses={} pending_bytes={}",
                self.connection_id,
                largest_acked,
                ack_delay_us,
                ranges.len(),
                acked.released.len(),
                released_bytes,
                fast_retransmit.len(),
                lost_bytes,
                acked.spurious_losses,
                self.pending_bytes.load(Ordering::Relaxed),
            ),
        );
        if released_bytes > 0 || lost_bytes > 0 {
            self.send_notify.notify_waiters();
        }
        if !fast_retransmit.is_empty() {
            for packet in fast_retransmit {
                if let Err(err) = self
                    .send_recovery_payload_with_sample(packet.payload, packet.sample)
                    .await
                {
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
        if self.closed_streams.lock().await.contains(stream_id) {
            return Ok(());
        }

        let incoming = {
            let mut streams = self.streams.lock().await;
            match streams.entry(stream_id) {
                Entry::Occupied(_) => None,
                Entry::Vacant(entry) => {
                    if ordered {
                        let (state, stream_pair) = new_stream_pair(
                            stream_id,
                            self.commands.clone(),
                            carrier_stream_frame_queue(self.mux_limits),
                        );
                        entry.insert(state);
                        Some(stream_pair)
                    } else {
                        drop(streams);
                        self.store_orphan_fragment(
                            stream_id,
                            OrphanFragment::new(frame_id, offset, total_len, payload),
                        )
                        .await;
                        return Ok(());
                    }
                }
            }
        };

        let created = incoming.is_some();
        if let Some(streams) = incoming {
            self.incoming_streams
                .send(streams)
                .await
                .map_err(|_| UdpCarrierFrameError::Closed)?;
        }
        self.receive_existing_fragment(stream_id, frame_id, offset, total_len, payload, ordered)
            .await?;

        if created {
            let orphans = self.drain_orphan_fragments(stream_id).await;
            for orphan in orphans {
                self.receive_existing_fragment(
                    stream_id,
                    orphan.frame_id,
                    orphan.offset,
                    orphan.total_len,
                    orphan.payload,
                    false,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn receive_existing_fragment(
        self: &Arc<Self>,
        stream_id: u64,
        frame_id: u64,
        offset: u32,
        total_len: usize,
        payload: Bytes,
        ordered: bool,
    ) -> Result<(), UdpCarrierFrameError> {
        let mut completed = None;
        let frames = {
            let mut streams = self.streams.lock().await;
            let Some(state) = streams.get_mut(&stream_id) else {
                return Ok(());
            };
            if state.should_ignore_fragment(ordered, frame_id) {
                return Ok(());
            }
            let key = FrameKey { ordered, frame_id };
            let assembly = state
                .assemblies
                .entry(key)
                .or_insert_with(|| FrameAssembly::new(total_len));
            if let Some(frame) = assembly.insert(offset, total_len, payload)? {
                state.assemblies.remove(&key);
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
                } else {
                    state.remember_unordered_delivery(frame_id);
                }
                if !ready.is_empty() {
                    completed = Some(ready);
                }
            }
            state.frames.clone()
        };

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

    async fn store_orphan_fragment(&self, stream_id: u64, fragment: OrphanFragment) {
        let ttl = self.orphan_fragment_ttl().await;
        let now = Instant::now();
        let stored = self.orphans.lock().await.store(
            stream_id,
            fragment,
            self.mux_limits.max_reorder_bytes,
            now,
            ttl,
        );
        #[cfg(feature = "lab-diagnostics")]
        if !stored {
            crate::lab_diagnostics::lab_diagnostic(
                "udp_carrier_orphan_drop",
                format_args!(
                    "connection_id={} stream_id={}",
                    self.connection_id, stream_id
                ),
            );
        }
        #[cfg(not(feature = "lab-diagnostics"))]
        let _ = stored;
    }

    async fn drain_orphan_fragments(&self, stream_id: u64) -> Vec<OrphanFragment> {
        let ttl = self.orphan_fragment_ttl().await;
        self.orphans
            .lock()
            .await
            .drain(stream_id, Instant::now(), ttl)
    }

    async fn orphan_fragment_ttl(&self) -> Duration {
        self.retransmit_delay().await * 4
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
    let mut backlog = VecDeque::new();
    loop {
        let Some(command) = next_stream_command(&mut commands, &mut backlog).await else {
            return;
        };
        let result = match command {
            StreamCommand::SendFrame {
                ordered,
                reliable,
                stream_id,
                frame_id,
                encoded,
                next_offset,
            } => {
                send_frame_fragments(
                    &connection,
                    stream_id,
                    frame_id,
                    encoded,
                    ordered,
                    reliable,
                    next_offset,
                )
                .await
            }
            StreamCommand::Finish { stream_id } => connection
                .send_payload(PacketPayload::CloseStream { stream_id }, true)
                .await
                .map(|_| None),
        };
        match result {
            Ok(Some(continuation)) => {
                enqueue_stream_continuation(&mut commands, &mut backlog, continuation);
            }
            Ok(None) => {}
            Err(err) => {
                eprintln!("warning: UDP carrier send failed: {err}");
            }
        }
    }
}

async fn next_stream_command(
    commands: &mut mpsc::Receiver<StreamCommand>,
    backlog: &mut VecDeque<StreamCommand>,
) -> Option<StreamCommand> {
    if let Some(command) = backlog.pop_front() {
        Some(command)
    } else {
        commands.recv().await
    }
}

fn enqueue_stream_continuation(
    commands: &mut mpsc::Receiver<StreamCommand>,
    backlog: &mut VecDeque<StreamCommand>,
    continuation: StreamCommand,
) {
    drain_available_stream_commands(commands, backlog);
    let stream_id = continuation.stream_id();
    let mut urgent_other_stream = VecDeque::new();
    let mut throughput_other_stream = VecDeque::new();
    let mut same_stream = VecDeque::new();
    while let Some(command) = backlog.pop_front() {
        if command.stream_id() == stream_id {
            same_stream.push_back(command);
        } else if stream_command_preempts_bulk(&command) {
            urgent_other_stream.push_back(command);
        } else {
            throughput_other_stream.push_back(command);
        }
    }
    let (urgent_head, urgent_tail) = split_bounded_urgent_preemption(urgent_other_stream);
    let mut reordered = VecDeque::with_capacity(
        urgent_head
            .len()
            .saturating_add(urgent_tail.len())
            .saturating_add(throughput_other_stream.len())
            .saturating_add(same_stream.len())
            .saturating_add(1),
    );
    reordered.extend(urgent_head);
    reordered.push_back(continuation);
    reordered.extend(urgent_tail);
    reordered.extend(throughput_other_stream);
    reordered.extend(same_stream);
    *backlog = reordered;
}

fn split_bounded_urgent_preemption(
    mut urgent: VecDeque<StreamCommand>,
) -> (VecDeque<StreamCommand>, VecDeque<StreamCommand>) {
    let budget = max_frame_fragment_payload().max(1);
    let mut used = 0usize;
    let mut head = VecDeque::new();
    let mut tail = VecDeque::new();
    while let Some(command) = urgent.pop_front() {
        let cost = stream_command_preemption_cost(&command);
        if head.is_empty() || used.saturating_add(cost) <= budget {
            used = used.saturating_add(cost);
            head.push_back(command);
        } else {
            tail.push_back(command);
        }
    }
    (head, tail)
}

fn stream_command_preempts_bulk(command: &StreamCommand) -> bool {
    match command {
        StreamCommand::Finish { .. } => true,
        StreamCommand::SendFrame {
            ordered,
            reliable,
            encoded,
            ..
        } => *ordered || !*reliable || encoded.len() <= max_frame_fragment_payload(),
    }
}

fn stream_command_preemption_cost(command: &StreamCommand) -> usize {
    match command {
        StreamCommand::Finish { .. } => 1,
        StreamCommand::SendFrame { encoded, .. } => encoded.len().max(1),
    }
}

fn drain_available_stream_commands(
    commands: &mut mpsc::Receiver<StreamCommand>,
    backlog: &mut VecDeque<StreamCommand>,
) {
    loop {
        match commands.try_recv() {
            Ok(command) => backlog.push_back(command),
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                return;
            }
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
    next_offset: usize,
) -> Result<Option<StreamCommand>, UdpCarrierFrameError> {
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
    let sample = packet_sample_for_encoded_frame(&encoded);
    let fragment_payload = connection.frame_fragment_payload_len().await.max(1);
    if next_offset > encoded.len() {
        return Err(UdpCarrierFrameError::InvalidPacket(
            "fragment continuation offset beyond frame",
        ));
    }
    if next_offset == encoded.len() {
        return Ok(None);
    }
    if reliable
        && !ordered
        && connection.gso_state.load(Ordering::Relaxed) != GSO_UNAVAILABLE
        && encoded.len().div_ceil(fragment_payload) > 1
    {
        return send_frame_fragments_with_gso(
            connection,
            GsoFragmentRequest {
                stream_id,
                frame_id,
                encoded,
                ordered,
                sample,
                fragment_payload,
                total_len,
                next_offset,
            },
        )
        .await;
    }
    let mut cursor = next_offset;
    let mut run_segments = 0usize;
    while cursor < encoded.len() {
        let start = cursor;
        let end = start.saturating_add(fragment_payload).min(encoded.len());
        let offset = u32::try_from(start)
            .map_err(|_| UdpCarrierFrameError::InvalidPacket("fragment offset overflow"))?;
        let payload = PacketPayload::FrameFragment {
            ordered,
            ack_eliciting: reliable,
            stream_id,
            frame_id,
            offset,
            total_len,
            payload: encoded.slice(start..end),
        };
        let packet_len = encoded_packet_len(&payload)?;
        connection
            .send_payload_with_sample(payload, reliable, sample)
            .await?;
        cursor = end;
        run_segments = run_segments.saturating_add(1);
        if reliable && !ordered && cursor < encoded.len() {
            let budget = connection.packet_run_segment_budget(packet_len).await;
            if run_segments >= budget {
                record_frame_preemption(stream_id, frame_id, cursor, budget, false);
                return Ok(Some(stream_frame_continuation(
                    ordered, reliable, stream_id, frame_id, encoded, cursor,
                )));
            }
        }
    }
    Ok(None)
}

struct GsoFragmentRequest {
    stream_id: u64,
    frame_id: u64,
    encoded: Bytes,
    ordered: bool,
    sample: PacketSample,
    fragment_payload: usize,
    total_len: u32,
    next_offset: usize,
}

async fn send_frame_fragments_with_gso(
    connection: &ConnectionInner,
    request: GsoFragmentRequest,
) -> Result<Option<StreamCommand>, UdpCarrierFrameError> {
    let GsoFragmentRequest {
        stream_id,
        frame_id,
        encoded,
        ordered,
        sample,
        fragment_payload,
        total_len,
        next_offset,
    } = request;
    let mut run = Vec::new();
    let mut run_segment_len = None;
    let mut cursor = next_offset;
    while cursor < encoded.len() {
        let start = cursor;
        let end = start.saturating_add(fragment_payload).min(encoded.len());
        let offset = u32::try_from(start)
            .map_err(|_| UdpCarrierFrameError::InvalidPacket("fragment offset overflow"))?;
        let payload = PacketPayload::FrameFragment {
            ordered,
            ack_eliciting: true,
            stream_id,
            frame_id,
            offset,
            total_len,
            payload: encoded.slice(start..end),
        };
        let packet_len = encoded_packet_len(&payload)?;
        if connection.send_capacity_delay(packet_len).await.is_some() {
            if !run.is_empty() {
                connection.write_segment_run(&mut run).await?;
                record_frame_preemption(stream_id, frame_id, start, 0, true);
                return Ok(Some(stream_frame_continuation(
                    ordered, true, stream_id, frame_id, encoded, start,
                )));
            }
            connection.wait_for_send_capacity(packet_len).await;
        }

        let segment_budget = connection.packet_run_segment_budget(packet_len).await;
        if !run.is_empty()
            && (run_segment_len != Some(packet_len)
                || run.len() >= segment_budget
                || run.len() >= UDP_GSO_MAX_SEGMENTS)
        {
            connection.write_segment_run(&mut run).await?;
            record_frame_preemption(stream_id, frame_id, start, segment_budget, true);
            return Ok(Some(stream_frame_continuation(
                ordered, true, stream_id, frame_id, encoded, start,
            )));
        }

        let packet = connection
            .prepare_payload_for_segment(payload, sample)
            .await?;
        if run_segment_len.is_none() {
            run_segment_len = Some(packet.len());
        }
        run.push(packet);
        cursor = end;
        if cursor < encoded.len() && run.len() >= segment_budget {
            connection.write_segment_run(&mut run).await?;
            record_frame_preemption(stream_id, frame_id, cursor, segment_budget, true);
            return Ok(Some(stream_frame_continuation(
                ordered, true, stream_id, frame_id, encoded, cursor,
            )));
        }
    }
    connection.write_segment_run(&mut run).await?;
    Ok(None)
}

fn stream_frame_continuation(
    ordered: bool,
    reliable: bool,
    stream_id: u64,
    frame_id: u64,
    encoded: Bytes,
    next_offset: usize,
) -> StreamCommand {
    StreamCommand::SendFrame {
        ordered,
        reliable,
        stream_id,
        frame_id,
        encoded,
        next_offset,
    }
}

fn record_frame_preemption(
    _stream_id: u64,
    _frame_id: u64,
    _next_offset: usize,
    _segment_budget: usize,
    _gso: bool,
) {
    #[cfg(feature = "lab-diagnostics")]
    crate::lab_diagnostics::lab_diagnostic(
        "udp_carrier_frame_preempt",
        format_args!(
            "stream_id={} frame_id={} next_offset={} segment_budget={} gso={}",
            _stream_id, _frame_id, _next_offset, _segment_budget, _gso
        ),
    );
}

fn packet_sample_for_encoded_frame(encoded: &Bytes) -> PacketSample {
    const PRODUCT_MAGIC: &[u8; 4] = b"MPTF";
    const PRODUCT_FRAME_KIND_STREAM_DATA: u8 = 8;
    if encoded.len() > 5
        && &encoded[..4] == PRODUCT_MAGIC
        && encoded[5] == PRODUCT_FRAME_KIND_STREAM_DATA
    {
        PacketSample::Data { app_limited: false }
    } else {
        PacketSample::Control
    }
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
            pending.due_retransmits(now, rto, PTO_PROBE_PACKET_LIMIT)
        };
        if due.is_empty() {
            continue;
        }
        let retransmit_bytes = due
            .iter()
            .fold(0usize, |sum, packet| sum.saturating_add(packet.encoded_len));
        connection
            .controller
            .lock()
            .await
            .on_probe_timeout(retransmit_bytes);
        #[cfg(feature = "lab-diagnostics")]
        crate::lab_diagnostics::lab_diagnostic(
            "udp_carrier_pto_probe",
            format_args!(
                "connection_id={} probe_packets={} probe_bytes={} pending_bytes={}",
                connection.connection_id,
                due.len(),
                retransmit_bytes,
                connection.pending_bytes.load(Ordering::Relaxed),
            ),
        );
        for packet in due {
            if let Err(err) = connection
                .send_recovery_payload_with_sample(packet.payload, packet.sample)
                .await
            {
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

fn random_connection_id() -> Result<u64, UdpCarrierTransportError> {
    let mut bytes = [0u8; 8];
    getrandom::getrandom(&mut bytes)?;
    let mut id = u64::from_be_bytes(bytes);
    if id == 0 {
        id = 1;
    }
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream_state_for_test() -> StreamState {
        let (frames, _rx) = mpsc::channel(1);
        StreamState::new(frames)
    }

    fn pending_packet_for_test(sample: PacketSample, now: Instant) -> PendingPacket {
        PendingPacket {
            payload: PacketPayload::FrameFragment {
                ordered: true,
                ack_eliciting: true,
                stream_id: 1,
                frame_id: 1,
                offset: 0,
                total_len: 1,
                payload: Bytes::from_static(b"x"),
            },
            encoded_len: max_frame_fragment_payload(),
            sample,
            sent_at: now - Duration::from_millis(500),
            last_sent_at: now - Duration::from_millis(500),
            deadline: now + Duration::from_millis(100),
            generation: 0,
            retransmit_count: 0,
        }
    }

    async fn connection_for_test(role: CarrierRole) -> Arc<ConnectionInner> {
        let socket = Arc::new(
            UdpSocket::bind("127.0.0.1:0")
                .await
                .expect("bind test socket"),
        );
        let peer = socket.local_addr().expect("local addr");
        ConnectionInner::new(ConnectionParams {
            socket,
            role,
            peer,
            connection_id: 77,
            secret: b"test secret with enough entropy",
            cipher_suite: CipherSuite::Aes256Gcm,
            mux_limits: MuxLimits::default(),
            codec_limits: CodecLimits::default(),
        })
        .expect("connection")
    }

    #[tokio::test]
    async fn unknown_reliable_unordered_fragment_is_carrier_acked_and_buffered() {
        let connection = connection_for_test(CarrierRole::Server).await;
        let peer = *connection.peer.read().await;
        connection
            .process_packet(
                peer,
                PacketHeader {
                    direction: CarrierRole::Server.recv_direction(),
                    connection_id: connection.connection_id,
                    packet_number: 10,
                },
                PacketPayload::FrameFragment {
                    ordered: false,
                    ack_eliciting: true,
                    stream_id: 99,
                    frame_id: 1,
                    offset: 0,
                    total_len: 4,
                    payload: Bytes::from_static(b"data"),
                },
            )
            .await
            .expect("process orphan");

        let ack_state = connection.ack_state.lock().await;
        assert!(ack_state.pending.contains(&10));
        drop(ack_state);
        assert_eq!(connection.orphans.lock().await.bytes(), 4);
    }

    #[tokio::test]
    async fn ordered_control_arrival_drains_orphan_fragments() {
        let connection = connection_for_test(CarrierRole::Server).await;
        let peer = *connection.peer.read().await;
        connection
            .process_packet(
                peer,
                PacketHeader {
                    direction: CarrierRole::Server.recv_direction(),
                    connection_id: connection.connection_id,
                    packet_number: 10,
                },
                PacketPayload::FrameFragment {
                    ordered: false,
                    ack_eliciting: true,
                    stream_id: 99,
                    frame_id: 1,
                    offset: 0,
                    total_len: 4,
                    payload: Bytes::from_static(b"data"),
                },
            )
            .await
            .expect("process orphan");
        connection
            .process_packet(
                peer,
                PacketHeader {
                    direction: CarrierRole::Server.recv_direction(),
                    connection_id: connection.connection_id,
                    packet_number: 11,
                },
                PacketPayload::FrameFragment {
                    ordered: true,
                    ack_eliciting: true,
                    stream_id: 99,
                    frame_id: 0,
                    offset: 0,
                    total_len: 4,
                    payload: Bytes::from_static(b"open"),
                },
            )
            .await
            .expect("process open");

        let (_send, mut recv) = Connection {
            inner: connection.clone(),
        }
        .accept_bi()
        .await
        .expect("accepted stream");
        assert_eq!(recv.frames.recv().await.as_deref(), Some(&b"open"[..]));
        assert_eq!(recv.frames.recv().await.as_deref(), Some(&b"data"[..]));
        assert_eq!(connection.orphans.lock().await.bytes(), 0);
    }

    #[test]
    fn frame_assembly_waits_for_all_fragments_and_reassembles_in_order() {
        let mut assembly = FrameAssembly::new(11);
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
    fn stream_state_drops_retransmitted_ordered_frames_before_frontier() {
        let mut state = stream_state_for_test();
        state.next_frame_id = 3;

        assert!(state.should_ignore_fragment(true, 0));
        assert!(state.should_ignore_fragment(true, 2));
        assert!(!state.should_ignore_fragment(true, 3));
    }

    #[test]
    fn stream_state_deduplicates_reliable_unordered_frames_with_bounded_memory() {
        let mut state = stream_state_for_test();
        state.remember_unordered_delivery(10);

        assert!(state.should_ignore_fragment(false, 10));
        assert!(!state.should_ignore_fragment(false, 11));

        for frame_id in 11..(UNORDERED_DEDUP_WINDOW as u64 + 12) {
            state.remember_unordered_delivery(frame_id);
        }

        assert!(!state.should_ignore_fragment(false, 10));
        assert!(state.should_ignore_fragment(false, UNORDERED_DEDUP_WINDOW as u64 + 11));
    }

    #[test]
    fn closed_stream_cache_suppresses_recent_late_fragments() {
        let mut cache = ClosedStreamCache::default();
        cache.remember(7);

        assert!(cache.contains(7));
        assert!(!cache.contains(8));

        for stream_id in 8..(CLOSED_STREAM_DEDUP_WINDOW as u64 + 9) {
            cache.remember(stream_id);
        }

        assert!(!cache.contains(7));
        assert!(cache.contains(CLOSED_STREAM_DEDUP_WINDOW as u64 + 8));
    }

    #[test]
    fn packet_run_budget_grows_from_controller_pacing_model() {
        let mut controller = UdpPathController::new(
            MuxLimits::default(),
            crate::transport::udp_carrier::crypto::DIR_CLIENT_TO_SERVER,
        );
        let packet_len = 1_400;
        let startup_budget = controller.packet_run_segment_budget(packet_len, UDP_GSO_MAX_SEGMENTS);

        controller.delivery_rate_bps *= 16.0;
        controller.pacing_rate_bps = controller.delivery_rate_bps;
        let faster_budget = controller.packet_run_segment_budget(packet_len, UDP_GSO_MAX_SEGMENTS);

        assert!(startup_budget >= 1);
        assert!(faster_budget > startup_budget);
        assert!(faster_budget <= UDP_GSO_MAX_SEGMENTS);
    }

    #[test]
    fn stream_continuation_prioritizes_small_work_without_stalling_current_frame() {
        let (tx, mut rx) = mpsc::channel(8);
        let single_packet_payload = max_frame_fragment_payload();
        tx.try_send(StreamCommand::SendFrame {
            ordered: false,
            reliable: true,
            stream_id: 2,
            frame_id: 0,
            encoded: Bytes::from(vec![0u8; single_packet_payload * 2]),
            next_offset: 0,
        })
        .expect("other bulk stream command");
        tx.try_send(StreamCommand::SendFrame {
            ordered: false,
            reliable: true,
            stream_id: 3,
            frame_id: 0,
            encoded: Bytes::from(vec![0u8; single_packet_payload]),
            next_offset: 0,
        })
        .expect("small stream command");
        tx.try_send(StreamCommand::SendFrame {
            ordered: false,
            reliable: true,
            stream_id: 4,
            frame_id: 0,
            encoded: Bytes::from(vec![0u8; single_packet_payload]),
            next_offset: 0,
        })
        .expect("second small stream command");
        tx.try_send(StreamCommand::Finish { stream_id: 1 })
            .expect("same stream finish");

        let mut backlog = VecDeque::new();
        enqueue_stream_continuation(
            &mut rx,
            &mut backlog,
            StreamCommand::SendFrame {
                ordered: false,
                reliable: true,
                stream_id: 1,
                frame_id: 9,
                encoded: Bytes::from_static(b"bulk"),
                next_offset: 1_400,
            },
        );

        assert_eq!(backlog.pop_front().expect("small stream").stream_id(), 3);
        match backlog.pop_front().expect("continuation") {
            StreamCommand::SendFrame {
                stream_id,
                frame_id,
                next_offset,
                ..
            } => {
                assert_eq!(stream_id, 1);
                assert_eq!(frame_id, 9);
                assert_eq!(next_offset, 1_400);
            }
            StreamCommand::Finish { .. } => panic!("same-stream finish overtook continuation"),
        }
        assert_eq!(
            backlog
                .pop_front()
                .expect("deferred small stream")
                .stream_id(),
            4
        );
        assert_eq!(
            backlog.pop_front().expect("other bulk stream").stream_id(),
            2
        );
        assert!(matches!(
            backlog.pop_front().expect("same stream finish"),
            StreamCommand::Finish { stream_id: 1 }
        ));
    }

    #[test]
    fn rtt_estimator_is_adaptive_and_bounded() {
        let mut rtt = RttEstimator::new();
        let initial = rtt.pto();
        for _ in 0..8 {
            rtt.observe(Duration::from_millis(20));
        }
        assert!(rtt.pto() < initial);
        for _ in 0..10 {
            rtt.observe(Duration::from_secs(10));
        }
        assert_eq!(rtt.pto(), MAX_RTO);
    }

    #[test]
    fn udp_controller_loss_backoff_preserves_delivery_rate_model() {
        let limits = MuxLimits::default();
        let mut tiny_loss = UdpPathController::new(
            limits,
            crate::transport::udp_carrier::crypto::DIR_CLIENT_TO_SERVER,
        );
        let mut large_loss = UdpPathController::new(
            limits,
            crate::transport::udp_carrier::crypto::DIR_CLIENT_TO_SERVER,
        );
        let initial_inflight = tiny_loss.inflight_hi;
        let initial_rate = tiny_loss.delivery_rate_bps;

        tiny_loss.on_loss(max_frame_fragment_payload(), limits);
        large_loss.on_loss(initial_inflight / 4, limits);

        assert!(tiny_loss.inflight_hi > initial_inflight * 99 / 100);
        assert_eq!(tiny_loss.delivery_rate_bps, initial_rate);
        assert!(large_loss.inflight_hi < tiny_loss.inflight_hi);
        assert_eq!(large_loss.delivery_rate_bps, initial_rate);
    }

    #[test]
    fn udp_controller_startup_flight_is_bounded_below_pending_budget() {
        let limits = MuxLimits::default();
        let controller = UdpPathController::new(
            limits,
            crate::transport::udp_carrier::crypto::DIR_CLIENT_TO_SERVER,
        );
        let fragment = max_frame_fragment_payload();
        let budget = carrier_pending_byte_budget(limits);

        assert!(controller.inflight_hi >= fragment * STARTUP_MIN_FLIGHT_PACKETS);
        assert!(controller.inflight_hi <= fragment * STARTUP_MAX_FLIGHT_PACKETS);
        assert!(controller.inflight_hi < budget);
        assert!(
            controller.pacing_rate_bps > bytes_per_rtt_to_bps(controller.inflight_hi, INITIAL_RTT)
        );
    }

    #[test]
    fn udp_controller_low_samples_do_not_reduce_delivery_rate_model() {
        let limits = MuxLimits::default();
        let mut controller = UdpPathController::new(
            limits,
            crate::transport::udp_carrier::crypto::DIR_CLIENT_TO_SERVER,
        );
        let now = Instant::now();
        let sample_interval = controller.delivery_rate_sample_interval();
        let initial_rate = controller.delivery_rate_bps;
        let mut app_limited_packet =
            pending_packet_for_test(PacketSample::Data { app_limited: true }, now);
        app_limited_packet.sent_at = now - sample_interval;
        let acked = AckedPacket {
            packet_number: 7,
            packet: app_limited_packet,
        };

        controller.on_packets_acked(&[acked], 0, Duration::ZERO, now, limits);

        assert_eq!(controller.delivery_rate_bps, initial_rate);
        assert!(controller.app_limited);

        let later = now + sample_interval.max(Duration::from_millis(100));
        let mut non_app_limited_packet =
            pending_packet_for_test(PacketSample::Data { app_limited: false }, later);
        non_app_limited_packet.encoded_len = 512 * 1024;
        non_app_limited_packet.sent_at = now;
        let non_app_limited = AckedPacket {
            packet_number: 8,
            packet: non_app_limited_packet,
        };

        controller.on_packets_acked(&[non_app_limited], 0, Duration::ZERO, later, limits);

        assert_eq!(controller.delivery_rate_bps, initial_rate);
        assert!(controller.delivery_rate_bps > 0.0);
        assert!(!controller.app_limited);
    }

    #[test]
    fn udp_controller_ack_compressed_bursts_do_not_create_instant_rate_spikes() {
        let limits = MuxLimits::default();
        let mut controller = UdpPathController::new(
            limits,
            crate::transport::udp_carrier::crypto::DIR_SERVER_TO_CLIENT,
        );
        let now = Instant::now();
        let initial_rate = controller.delivery_rate_bps;
        let packet_sent_at = now - Duration::from_millis(1);

        for packet_number in 1..=8 {
            let mut packet =
                pending_packet_for_test(PacketSample::Data { app_limited: false }, now);
            packet.sent_at = packet_sent_at;
            controller.on_packets_acked(
                &[AckedPacket {
                    packet_number,
                    packet,
                }],
                0,
                Duration::ZERO,
                now + Duration::from_millis(packet_number),
                limits,
            );
        }

        assert!(controller.delivery_rate_bps <= initial_rate);
        assert!(controller.rate_sample_delivered_bytes > 0);
    }

    #[test]
    fn udp_controller_delivery_rate_growth_is_startup_gain_limited() {
        let limits = MuxLimits::default();
        let mut controller = UdpPathController::new(
            limits,
            crate::transport::udp_carrier::crypto::DIR_SERVER_TO_CLIENT,
        );
        let now = Instant::now();
        let initial_rate = controller.delivery_rate_bps;
        let sample_interval = controller.delivery_rate_sample_interval();
        let mut first = pending_packet_for_test(PacketSample::Data { app_limited: false }, now);
        first.encoded_len = 8 * 1024 * 1024;
        first.sent_at = now - sample_interval;
        let mut second = pending_packet_for_test(PacketSample::Data { app_limited: false }, now);
        second.encoded_len = 8 * 1024 * 1024;
        second.sent_at = now;

        controller.on_packets_acked(
            &[
                AckedPacket {
                    packet_number: 1,
                    packet: first,
                },
                AckedPacket {
                    packet_number: 2,
                    packet: second,
                },
            ],
            0,
            Duration::ZERO,
            now,
            limits,
        );

        assert!(controller.delivery_rate_bps > initial_rate);
        assert!(controller.delivery_rate_bps <= initial_rate * STARTUP_PACING_GAIN);
    }

    #[test]
    fn packet_window_does_not_redeclare_confirmed_loss() {
        let now = Instant::now();
        let mut window = PacketWindow::default();

        for packet_number in 1..=5 {
            window.insert(
                packet_number,
                pending_packet_for_test(PacketSample::Data { app_limited: false }, now),
            );
        }

        let first = window.detect_losses(
            5,
            now,
            PACKET_LOSS_THRESHOLD,
            Duration::from_millis(1),
            Duration::ZERO,
            64,
        );

        assert!(!first.is_empty());

        let second = window.detect_losses(
            5,
            now + Duration::from_millis(10),
            PACKET_LOSS_THRESHOLD,
            Duration::from_millis(1),
            Duration::ZERO,
            64,
        );

        assert!(second.is_empty());
    }

    #[test]
    fn packet_window_records_spurious_ack_for_removed_lost_packet() {
        let now = Instant::now();
        let mut window = PacketWindow::default();
        window.insert(
            1,
            pending_packet_for_test(PacketSample::Data { app_limited: false }, now),
        );
        window.insert(
            4,
            pending_packet_for_test(PacketSample::Data { app_limited: false }, now),
        );

        let lost = window.detect_losses(
            4,
            now,
            PACKET_LOSS_THRESHOLD,
            Duration::from_millis(1),
            Duration::ZERO,
            64,
        );
        assert_eq!(lost.len(), 1);

        let acked = window.remove_acked_ranges(&[PacketAckRange { start: 1, end: 2 }]);
        assert!(acked.released.is_empty());
        assert_eq!(acked.spurious_losses, 1);
    }

    #[test]
    fn ack_ranges_prefer_newest_packets_when_bounded() {
        let mut packets: Vec<u64> = (1..=(MAX_ACK_RANGES_PER_PACKET as u64 + 8))
            .map(|packet_number| packet_number * 2)
            .collect();
        let ranges = packet_ack_ranges(&mut packets);
        let newest = (MAX_ACK_RANGES_PER_PACKET as u64 + 8) * 2;

        assert_eq!(ranges.len(), MAX_ACK_RANGES_PER_PACKET);
        assert_eq!(
            ranges.first().copied(),
            Some(PacketAckRange {
                start: newest,
                end: newest + 1,
            })
        );
        assert!(ranges.iter().any(|range| range.start == newest));
        assert!(!ranges.iter().any(|range| range.start == 2));
    }

    #[test]
    fn ack_state_repeats_recent_received_ranges_after_flush() {
        let now = Instant::now();
        let mut state = AckState::default();
        state.remember_received(10, now);
        state.pending.push(10);

        let mut first_packets = state.ack_packets();
        let first = packet_ack_ranges(&mut first_packets);
        state.pending.clear();

        state.remember_received(11, now + Duration::from_millis(1));
        state.pending.push(11);
        let mut second_packets = state.ack_packets();
        let second = packet_ack_ranges(&mut second_packets);

        assert_eq!(first, vec![PacketAckRange { start: 10, end: 11 }]);
        assert_eq!(second, vec![PacketAckRange { start: 10, end: 12 }]);
    }

    #[test]
    fn packet_window_pto_is_path_gated_not_per_packet_storm() {
        let now = Instant::now();
        let mut window = PacketWindow::default();

        for packet_number in 1..=4 {
            let mut packet =
                pending_packet_for_test(PacketSample::Data { app_limited: false }, now);
            packet.deadline = now - Duration::from_millis(1);
            window.insert(packet_number, packet);
        }

        let first = window.due_retransmits(now, Duration::from_millis(200), 2);
        assert_eq!(first.len(), 2);

        let gated = window.due_retransmits(
            now + Duration::from_millis(50),
            Duration::from_millis(200),
            2,
        );
        assert!(gated.is_empty());

        let next = window.due_retransmits(
            now + Duration::from_millis(250),
            Duration::from_millis(200),
            2,
        );
        assert_eq!(next.len(), 2);
    }

    #[test]
    fn udp_controller_pto_uses_bounded_exponential_backoff() {
        let limits = MuxLimits::default();
        let mut controller = UdpPathController::new(
            limits,
            crate::transport::udp_carrier::crypto::DIR_SERVER_TO_CLIENT,
        );
        let base = controller.rto();

        controller.on_probe_timeout(max_frame_fragment_payload());
        let once = controller.rto();
        controller.on_probe_timeout(max_frame_fragment_payload());
        let twice = controller.rto();

        assert!(once >= base.saturating_mul(2));
        assert!(twice >= base.saturating_mul(4));

        let mut acked =
            pending_packet_for_test(PacketSample::Data { app_limited: false }, Instant::now());
        acked.encoded_len = max_frame_fragment_payload();
        controller.on_packets_acked(
            &[AckedPacket {
                packet_number: 1,
                packet: acked,
            }],
            0,
            Duration::ZERO,
            Instant::now(),
            limits,
        );

        assert_eq!(controller.pto_count, 0);
    }
}
