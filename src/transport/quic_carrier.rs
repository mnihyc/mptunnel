#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_perf_record;
use crate::mux::MuxLimits;
use crate::protocol::codec::{
    CodecLimits, decode_frame_bytes, encode_frame_into, encoded_frame_capacity_hint,
};
use crate::protocol::{Frame, StreamFlags};
use bytes::BytesMut;
use quinn::{
    ClientConfig, ConnectionError, Endpoint as QuinnEndpoint, ServerConfig, TransportConfig, VarInt,
};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use sha2::{Digest, Sha256};
use std::any::Any;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// Thin QUIC carrier boundary. Quinn owns packet recovery and congestion state;
// this module adds mptunnel record framing and exports coherent native metrics.
// It deliberately does not decide which product flow or range uses this path.

const FRAME_LEN_BYTES: usize = 4;
const QUIC_RECV_CHUNK_BYTES: usize = 64 * 1024;
// Capacity trains are streamed as bounded records so one exploratory attempt
// cannot allocate its full session budget in a single Rust Vec.
const QUIC_CAPACITY_RECORD_PAYLOAD_BYTES: usize = 64 * 1024;
// Carrier recordization limit for length-prefixed STREAM_DATA frames written on
// an ordered QUIC stream. This must not be confused with the product sender
// quantum: product scheduling still emits the 64 KiB BBR service quantum, while
// this writer splits only the serialized records so a lost QUIC packet does not
// withhold an entire product quantum from the peer.
const QUIC_STREAM_RECORD_PAYLOAD_BYTES: usize = 10 * 1200;
const QUIC_CERT_DNS_NAME: &str = "mptunnel.invalid";
const ED25519_PKCS8_PREFIX: &[u8] = &[
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];
#[derive(Debug)]
pub struct Endpoint {
    endpoint: QuinnEndpoint,
}

#[derive(Debug, Clone)]
pub struct Connection {
    connection: quinn::Connection,
    write_backlog: Arc<AtomicU64>,
    delivery_evidence_written: Arc<AtomicU64>,
    telemetry: Arc<QuicCarrierTelemetry>,
}

#[derive(Debug, Clone, Copy)]
pub struct CapacityProbeSpec {
    pub token: u64,
    pub train_payload_bytes: u64,
    pub sample_floor_bytes: u64,
    pub warmup_carrier_bytes: u64,
    pub required_timed_carrier_bytes: u64,
    pub expires_at: Instant,
    pub proof_validity: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacityProbePhase {
    Writing,
    Measuring,
    ProvenDraining,
    Proven,
    Expired,
    Aborted,
}

#[derive(Debug, Clone, Copy)]
pub struct CapacityProbeMetrics {
    pub token: u64,
    pub train_payload_bytes: u64,
    pub sample_floor_bytes: u64,
    pub warmup_carrier_bytes: u64,
    pub required_timed_carrier_bytes: u64,
    pub expires_at: Instant,
    pub phase: CapacityProbePhase,
    pub started_clean: bool,
    pub write_committed: bool,
    pub written_payload_bytes: u64,
    pub written_data_frame_count: u64,
    pub total_acked_carrier_bytes: u64,
    pub total_ack_sample_count: u64,
    pub warmup_acked_carrier_bytes: u64,
    pub warmup_ack_sample_count: u64,
    pub measurement_acked_carrier_bytes: u64,
    pub measurement_ack_sample_count: u64,
    pub timed_measurement_acked_carrier_bytes: u64,
    pub timed_measurement_ack_sample_count: u64,
    pub app_limited_acked_carrier_bytes: u64,
    pub app_limited_ack_sample_count: u64,
    pub timed_measurement_ack_elapsed: Option<Duration>,
    pub native_proved_at: Option<Instant>,
    pub proved_at: Option<Instant>,
    pub proof_validity: Duration,
    pub receipt_received_payload_bytes: u64,
    pub receipt_elapsed: Option<Duration>,
    pub receipt_rtt: Option<Duration>,
    pub receipt_at: Option<Instant>,
    // These snapshots explain native cleanup ordering around the receipt. They
    // are diagnostic because ACK-only sends have no later ACK callback.
    pub last_authoritative_in_flight: Option<u64>,
    pub last_authoritative_in_flight_at: Option<Instant>,
    pub last_authoritative_sent_watermark: Option<u64>,
    pub receipt_frozen_sent_watermark: Option<u64>,
    pub current_sent_watermark: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct CongestionMetrics {
    pub congestion_window: u64,
    pub bytes_in_flight: Option<u64>,
    pub pending_bytes: u64,
    pub pacing_rate_bps: Option<u64>,
    pub loss_ppm: Option<u32>,
    pub ecn_ppm: Option<u32>,
    pub newly_acked_bytes: Option<u64>,
    pub non_app_limited_acked_bytes: Option<u64>,
    pub timed_non_app_limited_acked_bytes: Option<u64>,
    pub non_app_limited_ack_elapsed: Option<Duration>,
    pub delivery_evidence_written_bytes: u64,
    pub delivery_sample_count: u64,
    pub non_app_limited_delivery_sample_count: u64,
    pub timed_non_app_limited_delivery_sample_count: u64,
    pub app_limited: bool,
    pub capacity_probe: Option<CapacityProbeMetrics>,
}

#[derive(Debug)]
pub struct SendStream {
    stream: quinn::SendStream,
    connection: quinn::Connection,
    write_backlog: Arc<AtomicU64>,
    delivery_evidence_written: Arc<AtomicU64>,
    telemetry: Arc<QuicCarrierTelemetry>,
    encode_buffer: Vec<u8>,
}

// Quinn writes can consume a prefix before cancellation. Fail the whole path
// so record framing never resumes from an ambiguous carrier-stream offset.
struct QuicWriteTransaction {
    connection: quinn::Connection,
    write_backlog: Arc<AtomicU64>,
    packet_len: u64,
    fail_close: bool,
}

impl QuicWriteTransaction {
    fn new(connection: quinn::Connection, write_backlog: Arc<AtomicU64>, packet_len: u64) -> Self {
        Self {
            connection,
            write_backlog,
            packet_len,
            fail_close: true,
        }
    }

    fn commit(mut self) {
        self.fail_close = false;
    }
}

impl Drop for QuicWriteTransaction {
    fn drop(&mut self) {
        let _ = self
            .write_backlog
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(self.packet_len))
            });
        if self.fail_close {
            self.connection
                .close(VarInt::from_u32(1), b"cancelled or failed carrier write");
        }
    }
}

#[derive(Debug)]
pub struct RecvStream {
    stream: quinn::RecvStream,
    // QUIC RecvStream::read_exact is explicitly not cancellation-safe. The
    // runtime polls path reads inside tokio::select!, where a local read, ACK,
    // timer, or capacity notification may cancel the pending branch. Keep the
    // partially-read carrier bytes here and advance the underlying QUIC stream
    // only through cancel-safe read() calls. Otherwise a cancelled frame read
    // can silently drop the already-consumed prefix and desynchronize the
    // length-prefixed mptunnel frame stream, which shows up as random stalls,
    // repair storms, and bursty zero-throughput intervals on QUIC paths.
    read_buffer: BytesMut,
    read_scratch: Vec<u8>,
}

#[derive(Debug, Default)]
struct InstrumentedBbrConfig;

#[derive(Debug, Default)]
struct QuicCarrierTelemetry {
    bytes_in_flight: AtomicU64,
    bytes_in_flight_authoritative: AtomicBool,
    // Quinn invokes congestion callbacks through one mutable controller. The
    // sequence makes its cumulative ACK counters coherent for a concurrent
    // metrics reader without putting a lock on the packet ACK hot path.
    ack_snapshot_sequence: AtomicU64,
    newly_acked_bytes: AtomicU64,
    non_app_limited_acked_bytes: AtomicU64,
    timed_non_app_limited_acked_bytes: AtomicU64,
    non_app_limited_ack_elapsed_nanos: AtomicU64,
    delivery_sample_count: AtomicU64,
    non_app_limited_delivery_sample_count: AtomicU64,
    timed_non_app_limited_delivery_sample_count: AtomicU64,
    ack_snapshot_cursor: Mutex<QuicAckTelemetryTotals>,
    sent_bytes: AtomicU64,
    lost_bytes: AtomicU64,
    app_limited: AtomicBool,
    capacity_active_token: AtomicU64,
    ordinary_writer_count: AtomicU64,
    capacity_gate_notify: tokio::sync::Notify,
    capacity_fail_close_notify: tokio::sync::Notify,
    capacity_fail_close_requested: AtomicBool,
    capacity_probe: Mutex<Option<CapacityProbeEpoch>>,
    capacity_ack_quarantine_active: AtomicBool,
    capacity_ack_quarantine: Mutex<Option<CapacityAckQuarantine>>,
}

#[derive(Debug)]
struct CapacityProbeEpoch {
    metrics: CapacityProbeMetrics,
    started_at: Instant,
    write_started_at: Option<Instant>,
    receiver_confirmed: bool,
    batch_measurement_acked_carrier_bytes: u64,
    batch_measurement_ack_sample_count: u64,
    batch_earliest_sent: Option<Instant>,
    batch_latest_sent: Option<Instant>,
    last_ack: Option<Instant>,
    sent_high_watermark: Option<Instant>,
}

#[derive(Debug, Clone, Copy)]
struct CapacityAckQuarantine {
    token: u64,
    sent_at_or_after: Instant,
    sent_before: Instant,
}

struct OrdinaryWriteGuard {
    telemetry: Arc<QuicCarrierTelemetry>,
}

impl Drop for OrdinaryWriteGuard {
    fn drop(&mut self) {
        if self
            .telemetry
            .ordinary_writer_count
            .fetch_sub(1, Ordering::AcqRel)
            == 1
        {
            self.telemetry.capacity_gate_notify.notify_waiters();
        }
    }
}

struct CapacityGateReservation {
    telemetry: Arc<QuicCarrierTelemetry>,
    token: u64,
    keep_reserved: bool,
}

impl CapacityGateReservation {
    fn commit(mut self) {
        self.keep_reserved = true;
    }
}

impl Drop for CapacityGateReservation {
    fn drop(&mut self) {
        if !self.keep_reserved {
            self.telemetry.release_capacity_token(self.token);
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct QuicAckTelemetryTotals {
    acked_bytes: u64,
    non_app_limited_acked_bytes: u64,
    timed_non_app_limited_acked_bytes: u64,
    non_app_limited_ack_elapsed_nanos: u64,
    sample_count: u64,
    non_app_limited_sample_count: u64,
    timed_non_app_limited_sample_count: u64,
}

#[derive(Debug, Clone, Copy)]
struct QuicCarrierTelemetrySnapshot {
    bytes_in_flight: Option<u64>,
    newly_acked_bytes: Option<u64>,
    non_app_limited_acked_bytes: Option<u64>,
    timed_non_app_limited_acked_bytes: Option<u64>,
    non_app_limited_ack_elapsed: Option<Duration>,
    delivery_sample_count: u64,
    non_app_limited_delivery_sample_count: u64,
    timed_non_app_limited_delivery_sample_count: u64,
    loss_ppm: Option<u32>,
    app_limited: bool,
    capacity_probe: Option<CapacityProbeMetrics>,
}

struct InstrumentedController {
    inner: Box<dyn quinn::congestion::Controller>,
    telemetry: Arc<QuicCarrierTelemetry>,
    ack_batch_acked_bytes: u64,
    ack_batch_non_app_limited_acked_bytes: u64,
    ack_batch_sample_count: u64,
    ack_batch_non_app_limited_sample_count: u64,
    ack_batch_earliest_non_app_limited_sent: Option<Instant>,
    ack_batch_latest_non_app_limited_sent: Option<Instant>,
    last_non_app_limited_ack: Option<Instant>,
    non_app_limited_sent_high_watermark: Option<Instant>,
}

impl std::fmt::Debug for InstrumentedController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstrumentedController")
            .field("telemetry", &self.telemetry)
            .finish_non_exhaustive()
    }
}

impl QuicCarrierTelemetry {
    fn try_enter_ordinary_writer(self: &Arc<Self>) -> Option<OrdinaryWriteGuard> {
        if self.capacity_active_token.load(Ordering::Acquire) != 0 {
            return None;
        }
        self.ordinary_writer_count.fetch_add(1, Ordering::AcqRel);
        if self.capacity_active_token.load(Ordering::Acquire) == 0 {
            return Some(OrdinaryWriteGuard {
                telemetry: self.clone(),
            });
        }
        if self.ordinary_writer_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.capacity_gate_notify.notify_waiters();
        }
        None
    }

    async fn enter_ordinary_writer(self: &Arc<Self>) -> OrdinaryWriteGuard {
        loop {
            if let Some(guard) = self.try_enter_ordinary_writer() {
                return guard;
            }
            let notified = self.capacity_gate_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.capacity_active_token.load(Ordering::Acquire) != 0 {
                notified.await;
            }
        }
    }

    async fn wait_for_capacity_release(&self) {
        loop {
            let notified = self.capacity_gate_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.capacity_active_token.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    async fn reserve_capacity_token(
        self: &Arc<Self>,
        token: u64,
        expires_at: Instant,
    ) -> Result<CapacityGateReservation, QuicCarrierError> {
        if self.capacity_fail_close_requested.load(Ordering::Acquire) {
            return Err(QuicCarrierError::CapacityProbeExpired);
        }
        if self.capacity_ack_quarantine_blocks_new_probe(Instant::now()) {
            return Err(QuicCarrierError::CapacityProbeBusy);
        }
        if self
            .capacity_active_token
            .compare_exchange(0, token, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(QuicCarrierError::CapacityProbeBusy);
        }
        let reservation = CapacityGateReservation {
            telemetry: self.clone(),
            token,
            keep_reserved: false,
        };
        if self.capacity_fail_close_requested.load(Ordering::Acquire) {
            return Err(QuicCarrierError::CapacityProbeExpired);
        }
        loop {
            let notified = self.capacity_gate_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.ordinary_writer_count.load(Ordering::Acquire) == 0 {
                return Ok(reservation);
            }
            tokio::select! {
                _ = &mut notified => {}
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(expires_at)) => {
                    return Err(QuicCarrierError::CapacityProbeExpired);
                }
            }
        }
    }

    fn release_capacity_token(&self, token: u64) {
        if self
            .capacity_active_token
            .compare_exchange(token, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.capacity_gate_notify.notify_waiters();
        }
    }

    fn capacity_ack_quarantine_blocks_new_probe(&self, now: Instant) -> bool {
        if !self.capacity_ack_quarantine_active.load(Ordering::Acquire) {
            return false;
        }
        let mut current = self
            .capacity_ack_quarantine
            .lock()
            .expect("QUIC capacity ACK quarantine lock");
        if current
            .as_ref()
            .is_some_and(|quarantine| now < quarantine.sent_before)
        {
            return true;
        }
        current.take();
        self.capacity_ack_quarantine_active
            .store(false, Ordering::Release);
        false
    }

    fn install_capacity_ack_quarantine(
        &self,
        token: u64,
        sent_at_or_after: Instant,
        receipt_at: Instant,
        proof_validity: Duration,
    ) -> bool {
        let Some(sent_before) = receipt_at.checked_add(proof_validity) else {
            return false;
        };
        let mut current = self
            .capacity_ack_quarantine
            .lock()
            .expect("QUIC capacity ACK quarantine lock");
        if current.as_ref().is_some_and(|quarantine| {
            quarantine.token != token && receipt_at < quarantine.sent_before
        }) {
            return false;
        }
        *current = Some(CapacityAckQuarantine {
            token,
            sent_at_or_after,
            sent_before,
        });
        self.capacity_ack_quarantine_active
            .store(true, Ordering::Release);
        true
    }

    fn consume_capacity_ack_quarantine(&self, now: Instant, sent: Instant) -> bool {
        if !self.capacity_ack_quarantine_active.load(Ordering::Acquire) {
            return false;
        }
        let mut current = self
            .capacity_ack_quarantine
            .lock()
            .expect("QUIC capacity ACK quarantine lock");
        let Some(quarantine) = current.as_ref() else {
            self.capacity_ack_quarantine_active
                .store(false, Ordering::Release);
            return false;
        };
        if now >= quarantine.sent_before {
            current.take();
            self.capacity_ack_quarantine_active
                .store(false, Ordering::Release);
            return false;
        }
        sent >= quarantine.sent_at_or_after && sent < quarantine.sent_before
    }

    fn capacity_probe_metrics(spec: CapacityProbeSpec) -> CapacityProbeMetrics {
        CapacityProbeMetrics {
            token: spec.token,
            train_payload_bytes: spec.train_payload_bytes,
            sample_floor_bytes: spec.sample_floor_bytes,
            warmup_carrier_bytes: spec.warmup_carrier_bytes,
            required_timed_carrier_bytes: spec.required_timed_carrier_bytes,
            expires_at: spec.expires_at,
            proof_validity: spec.proof_validity,
            phase: CapacityProbePhase::Writing,
            // Quinn's congestion callback does not identify ACK-only sends and
            // write completion does not drain its cross-stream send queue.
            // Native telemetry therefore never claims a clean start.
            started_clean: false,
            write_committed: false,
            written_payload_bytes: 0,
            written_data_frame_count: 0,
            total_acked_carrier_bytes: 0,
            total_ack_sample_count: 0,
            warmup_acked_carrier_bytes: 0,
            warmup_ack_sample_count: 0,
            measurement_acked_carrier_bytes: 0,
            measurement_ack_sample_count: 0,
            timed_measurement_acked_carrier_bytes: 0,
            timed_measurement_ack_sample_count: 0,
            app_limited_acked_carrier_bytes: 0,
            app_limited_ack_sample_count: 0,
            timed_measurement_ack_elapsed: None,
            native_proved_at: None,
            proved_at: None,
            receipt_received_payload_bytes: 0,
            receipt_elapsed: None,
            receipt_rtt: None,
            receipt_at: None,
            last_authoritative_in_flight: None,
            last_authoritative_in_flight_at: None,
            last_authoritative_sent_watermark: None,
            receipt_frozen_sent_watermark: None,
            current_sent_watermark: 0,
        }
    }

    fn ack_totals(&self) -> QuicAckTelemetryTotals {
        loop {
            let before = self.ack_snapshot_sequence.load(Ordering::Acquire);
            if before & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            let totals = QuicAckTelemetryTotals {
                acked_bytes: self.newly_acked_bytes.load(Ordering::Relaxed),
                non_app_limited_acked_bytes: self
                    .non_app_limited_acked_bytes
                    .load(Ordering::Relaxed),
                timed_non_app_limited_acked_bytes: self
                    .timed_non_app_limited_acked_bytes
                    .load(Ordering::Relaxed),
                non_app_limited_ack_elapsed_nanos: self
                    .non_app_limited_ack_elapsed_nanos
                    .load(Ordering::Relaxed),
                sample_count: self.delivery_sample_count.load(Ordering::Relaxed),
                non_app_limited_sample_count: self
                    .non_app_limited_delivery_sample_count
                    .load(Ordering::Relaxed),
                timed_non_app_limited_sample_count: self
                    .timed_non_app_limited_delivery_sample_count
                    .load(Ordering::Relaxed),
            };
            fence(Ordering::Acquire);
            let after = self.ack_snapshot_sequence.load(Ordering::Relaxed);
            if before == after {
                return totals;
            }
        }
    }

    fn snapshot(&self) -> QuicCarrierTelemetrySnapshot {
        let mut cursor = self
            .ack_snapshot_cursor
            .lock()
            .expect("QUIC ACK telemetry snapshot lock");
        let totals = self.ack_totals();
        let newly_acked_bytes = totals.acked_bytes.wrapping_sub(cursor.acked_bytes);
        let non_app_limited_acked_bytes = totals
            .non_app_limited_acked_bytes
            .wrapping_sub(cursor.non_app_limited_acked_bytes);
        let timed_non_app_limited_acked_bytes = totals
            .timed_non_app_limited_acked_bytes
            .wrapping_sub(cursor.timed_non_app_limited_acked_bytes);
        let non_app_limited_ack_elapsed_nanos = totals
            .non_app_limited_ack_elapsed_nanos
            .wrapping_sub(cursor.non_app_limited_ack_elapsed_nanos);
        let delivery_sample_count = totals.sample_count.wrapping_sub(cursor.sample_count);
        let non_app_limited_delivery_sample_count = totals
            .non_app_limited_sample_count
            .wrapping_sub(cursor.non_app_limited_sample_count);
        let timed_non_app_limited_delivery_sample_count = totals
            .timed_non_app_limited_sample_count
            .wrapping_sub(cursor.timed_non_app_limited_sample_count);
        *cursor = totals;
        drop(cursor);
        let sent_bytes = self.sent_bytes.load(Ordering::Relaxed);
        let lost_bytes = self.lost_bytes.load(Ordering::Relaxed);
        let loss_ppm = (sent_bytes > 0).then(|| {
            let ratio = (lost_bytes as f64 / sent_bytes as f64).clamp(0.0, 1.0);
            (ratio * 1_000_000.0).round() as u32
        });
        let bytes_in_flight = self
            .bytes_in_flight_authoritative
            .load(Ordering::Acquire)
            .then(|| self.bytes_in_flight.load(Ordering::Relaxed))
            .filter(|_| {
                fence(Ordering::Acquire);
                self.bytes_in_flight_authoritative.load(Ordering::Relaxed)
            });
        QuicCarrierTelemetrySnapshot {
            bytes_in_flight,
            newly_acked_bytes: (newly_acked_bytes > 0).then_some(newly_acked_bytes),
            non_app_limited_acked_bytes: (non_app_limited_acked_bytes > 0)
                .then_some(non_app_limited_acked_bytes),
            timed_non_app_limited_acked_bytes: (timed_non_app_limited_acked_bytes > 0)
                .then_some(timed_non_app_limited_acked_bytes),
            non_app_limited_ack_elapsed: (non_app_limited_ack_elapsed_nanos > 0)
                .then(|| Duration::from_nanos(non_app_limited_ack_elapsed_nanos)),
            delivery_sample_count,
            non_app_limited_delivery_sample_count,
            timed_non_app_limited_delivery_sample_count,
            loss_ppm,
            app_limited: self.app_limited.load(Ordering::Relaxed),
            capacity_probe: self
                .capacity_probe
                .lock()
                .expect("QUIC capacity probe lock")
                .as_ref()
                .map(|probe| {
                    let mut metrics = probe.metrics;
                    metrics.current_sent_watermark = self.sent_bytes.load(Ordering::Acquire);
                    metrics
                }),
        }
    }

    fn install_capacity_probe(
        &self,
        spec: CapacityProbeSpec,
        write_backlog: u64,
    ) -> Result<(), QuicCarrierError> {
        if spec.token == 0
            || spec.train_payload_bytes == 0
            || spec.sample_floor_bytes == 0
            || spec.sample_floor_bytes > spec.train_payload_bytes
            || spec.required_timed_carrier_bytes == 0
            || spec.proof_validity.is_zero()
            || spec
                .warmup_carrier_bytes
                .saturating_add(spec.required_timed_carrier_bytes)
                > spec.train_payload_bytes
            || spec.expires_at <= Instant::now()
        {
            return Err(QuicCarrierError::InvalidCapacityProbe);
        }
        // `on_sent` includes ACK-only datagrams, which Quinn never reports to
        // `on_ack`; its additive BIF estimate can therefore contain phantom
        // bytes. The token receipt, not this provisional estimate, owns proof.
        if write_backlog != 0 {
            return Err(QuicCarrierError::CapacityProbeNotIdle);
        }
        let mut current = self
            .capacity_probe
            .lock()
            .expect("QUIC capacity probe lock");
        if self.capacity_active_token.load(Ordering::Acquire) != spec.token {
            return Err(QuicCarrierError::CapacityProbeBusy);
        }
        if current.as_ref().is_some_and(|probe| {
            !matches!(
                probe.metrics.phase,
                CapacityProbePhase::Proven
                    | CapacityProbePhase::Expired
                    | CapacityProbePhase::Aborted
            )
        }) {
            return Err(QuicCarrierError::CapacityProbeBusy);
        }
        *current = Some(CapacityProbeEpoch {
            metrics: Self::capacity_probe_metrics(spec),
            started_at: Instant::now(),
            write_started_at: None,
            receiver_confirmed: false,
            batch_measurement_acked_carrier_bytes: 0,
            batch_measurement_ack_sample_count: 0,
            batch_earliest_sent: None,
            batch_latest_sent: None,
            last_ack: None,
            sent_high_watermark: None,
        });
        Ok(())
    }

    fn mark_capacity_probe_write_started(&self, token: u64, now: Instant) -> bool {
        let mut current = self
            .capacity_probe
            .lock()
            .expect("QUIC capacity probe lock");
        let Some(probe) = current.as_mut().filter(|probe| {
            probe.metrics.token == token && probe.metrics.phase == CapacityProbePhase::Writing
        }) else {
            return false;
        };
        if now >= probe.metrics.expires_at {
            return false;
        }
        probe.write_started_at = Some(now);
        true
    }

    fn record_capacity_probe_data_written(&self, token: u64, payload_bytes: u64) -> bool {
        if payload_bytes == 0 {
            return false;
        }
        let mut current = self
            .capacity_probe
            .lock()
            .expect("QUIC capacity probe lock");
        let Some(probe) = current.as_mut().filter(|probe| {
            probe.metrics.token == token && probe.metrics.phase == CapacityProbePhase::Writing
        }) else {
            return false;
        };
        let Some(written_payload_bytes) = probe
            .metrics
            .written_payload_bytes
            .checked_add(payload_bytes)
            .filter(|written| *written <= probe.metrics.train_payload_bytes)
        else {
            return false;
        };
        probe.metrics.written_payload_bytes = written_payload_bytes;
        probe.metrics.written_data_frame_count =
            probe.metrics.written_data_frame_count.saturating_add(1);
        true
    }

    fn commit_capacity_probe_write(&self, token: u64, now: Instant) -> bool {
        let mut release_token = false;
        let mut current = self
            .capacity_probe
            .lock()
            .expect("QUIC capacity probe lock");
        let Some(probe) = current
            .as_mut()
            .filter(|probe| probe.metrics.token == token)
        else {
            return false;
        };
        if !matches!(probe.metrics.phase, CapacityProbePhase::Writing) {
            return false;
        }
        if now >= probe.metrics.expires_at {
            drop(current);
            let _ = self.finish_capacity_probe(token, CapacityProbePhase::Expired, now);
            return false;
        }
        if probe.metrics.written_payload_bytes != probe.metrics.train_payload_bytes
            || probe.metrics.written_data_frame_count == 0
        {
            return false;
        }
        probe.metrics.write_committed = true;
        probe.metrics.phase = if self.capacity_probe_can_finalize(probe) {
            release_token = true;
            CapacityProbePhase::Proven
        } else if probe.metrics.native_proved_at.is_some() {
            CapacityProbePhase::ProvenDraining
        } else {
            CapacityProbePhase::Measuring
        };
        drop(current);
        if release_token {
            self.release_capacity_token(token);
        }
        true
    }

    fn accumulate_capacity_ack(
        &self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
    ) -> bool {
        let token = self.capacity_active_token.load(Ordering::Acquire);
        if bytes == 0 {
            return false;
        }
        if token == 0 {
            return self.consume_capacity_ack_quarantine(now, sent);
        }
        let mut current = self
            .capacity_probe
            .lock()
            .expect("QUIC capacity probe lock");
        let Some(probe) = current.as_mut().filter(|probe| {
            probe.metrics.token == token
                && matches!(
                    probe.metrics.phase,
                    CapacityProbePhase::Writing
                        | CapacityProbePhase::Measuring
                        | CapacityProbePhase::ProvenDraining
                )
                && sent >= probe.started_at
        }) else {
            drop(current);
            return self.consume_capacity_ack_quarantine(now, sent);
        };

        let accepted_bytes = bytes.min(
            probe
                .metrics
                .train_payload_bytes
                .saturating_sub(probe.metrics.total_acked_carrier_bytes),
        );
        // Carrier headers and autonomous QUIC control can share probe packets.
        // Never let that overhead create more token credit than the declared
        // train, but continue routing excess ACKs away from product evidence.
        if accepted_bytes == 0 {
            return true;
        }
        probe.metrics.total_acked_carrier_bytes = probe
            .metrics
            .total_acked_carrier_bytes
            .saturating_add(accepted_bytes);
        probe.metrics.total_ack_sample_count =
            probe.metrics.total_ack_sample_count.saturating_add(1);
        if app_limited {
            probe.metrics.app_limited_acked_carrier_bytes = probe
                .metrics
                .app_limited_acked_carrier_bytes
                .saturating_add(accepted_bytes);
            probe.metrics.app_limited_ack_sample_count =
                probe.metrics.app_limited_ack_sample_count.saturating_add(1);
        }

        let warmup_remaining = probe
            .metrics
            .warmup_carrier_bytes
            .saturating_sub(probe.metrics.warmup_acked_carrier_bytes);
        let warmup_bytes = accepted_bytes.min(warmup_remaining);
        if warmup_bytes > 0 {
            probe.metrics.warmup_acked_carrier_bytes = probe
                .metrics
                .warmup_acked_carrier_bytes
                .saturating_add(warmup_bytes);
            probe.metrics.warmup_ack_sample_count =
                probe.metrics.warmup_ack_sample_count.saturating_add(1);
        }
        let measurement_bytes = accepted_bytes.saturating_sub(warmup_bytes);
        if measurement_bytes > 0 {
            probe.metrics.measurement_acked_carrier_bytes = probe
                .metrics
                .measurement_acked_carrier_bytes
                .saturating_add(measurement_bytes);
            probe.metrics.measurement_ack_sample_count =
                probe.metrics.measurement_ack_sample_count.saturating_add(1);
            probe.batch_measurement_acked_carrier_bytes = probe
                .batch_measurement_acked_carrier_bytes
                .saturating_add(measurement_bytes);
            probe.batch_measurement_ack_sample_count =
                probe.batch_measurement_ack_sample_count.saturating_add(1);
        }
        probe.batch_earliest_sent = Some(
            probe
                .batch_earliest_sent
                .map_or(sent, |earliest| earliest.min(sent)),
        );
        probe.batch_latest_sent = Some(
            probe
                .batch_latest_sent
                .map_or(sent, |latest| latest.max(sent)),
        );
        true
    }

    fn finish_capacity_ack_batch(&self, now: Instant, in_flight: u64) {
        let token = self.capacity_active_token.load(Ordering::Acquire);
        if token == 0 {
            return;
        }
        let mut release_token = false;
        let mut current = self
            .capacity_probe
            .lock()
            .expect("QUIC capacity probe lock");
        let Some(probe) = current
            .as_mut()
            .filter(|probe| probe.metrics.token == token)
        else {
            return;
        };
        if now >= probe.metrics.expires_at {
            drop(current);
            let _ = self.finish_capacity_probe(token, CapacityProbePhase::Expired, now);
            return;
        }
        probe.metrics.last_authoritative_in_flight = Some(in_flight);
        probe.metrics.last_authoritative_in_flight_at = Some(now);
        probe.metrics.last_authoritative_sent_watermark =
            Some(self.sent_bytes.load(Ordering::Acquire));
        let batch_sent_range = probe.batch_earliest_sent.zip(probe.batch_latest_sent);
        let elapsed = batch_sent_range.and_then(|(earliest_sent, latest_sent)| {
            let within_batch_send_elapsed = latest_sent.saturating_duration_since(earliest_sent);
            let elapsed = match (probe.last_ack, probe.sent_high_watermark) {
                (Some(previous_ack), Some(sent_high_watermark)) => now
                    .saturating_duration_since(previous_ack)
                    .max(latest_sent.saturating_duration_since(sent_high_watermark))
                    .max(within_batch_send_elapsed),
                _ => within_batch_send_elapsed,
            };
            (!elapsed.is_zero()).then_some(elapsed)
        });
        if probe.batch_measurement_acked_carrier_bytes > 0
            && let Some(elapsed) = elapsed
        {
            let timed_bytes = probe.batch_measurement_acked_carrier_bytes.min(
                probe
                    .metrics
                    .required_timed_carrier_bytes
                    .saturating_sub(probe.metrics.timed_measurement_acked_carrier_bytes),
            );
            probe.metrics.timed_measurement_acked_carrier_bytes = probe
                .metrics
                .timed_measurement_acked_carrier_bytes
                .saturating_add(timed_bytes);
            if timed_bytes > 0 {
                probe.metrics.timed_measurement_ack_sample_count = probe
                    .metrics
                    .timed_measurement_ack_sample_count
                    .saturating_add(probe.batch_measurement_ack_sample_count);
                probe.metrics.timed_measurement_ack_elapsed = Some(
                    probe
                        .metrics
                        .timed_measurement_ack_elapsed
                        .unwrap_or_default()
                        .saturating_add(elapsed),
                );
            }
        }
        probe.batch_measurement_acked_carrier_bytes = 0;
        probe.batch_measurement_ack_sample_count = 0;
        probe.batch_earliest_sent = None;
        probe.batch_latest_sent = None;
        if let Some((_, latest_sent)) = batch_sent_range {
            probe.last_ack = Some(now);
            probe.sent_high_watermark = Some(
                probe
                    .sent_high_watermark
                    .map_or(latest_sent, |high_watermark| {
                        high_watermark.max(latest_sent)
                    }),
            );
        }

        if probe.metrics.native_proved_at.is_none()
            && probe.metrics.timed_measurement_acked_carrier_bytes
                >= probe.metrics.required_timed_carrier_bytes
        {
            probe.metrics.native_proved_at = Some(now);
            if probe.metrics.write_committed {
                probe.metrics.phase = CapacityProbePhase::ProvenDraining;
            }
        }
        if self.capacity_probe_can_finalize(probe) {
            probe.metrics.phase = CapacityProbePhase::Proven;
            release_token = true;
        }
        drop(current);
        if release_token {
            self.release_capacity_token(token);
        }
    }

    fn capacity_probe_can_finalize(&self, probe: &CapacityProbeEpoch) -> bool {
        // Quinn delivers transmit callbacks before application receive events,
        // so the receipt-triggered ACK-only send can follow the last BIF zero
        // with no later ACK batch. Exact receipt owns completion; native flight
        // and send watermarks remain cleanup diagnostics only.
        probe.receiver_confirmed
            && probe.metrics.receipt_received_payload_bytes == probe.metrics.train_payload_bytes
            && probe.metrics.receipt_at.is_some()
            && probe.metrics.receipt_elapsed.is_some()
            && probe.metrics.write_committed
            && probe.metrics.written_payload_bytes >= probe.metrics.train_payload_bytes
    }

    fn confirm_capacity_probe_receipt(
        &self,
        token: u64,
        received_payload_bytes: u64,
        received_at: Instant,
        receipt_rtt: Duration,
    ) -> bool {
        let mut release_token = false;
        let mut current = self
            .capacity_probe
            .lock()
            .expect("QUIC capacity probe lock");
        let Some(probe) = current.as_mut().filter(|probe| {
            probe.metrics.token == token
                && matches!(
                    probe.metrics.phase,
                    CapacityProbePhase::Writing
                        | CapacityProbePhase::Measuring
                        | CapacityProbePhase::ProvenDraining
                )
                && received_payload_bytes == probe.metrics.train_payload_bytes
                && received_at < probe.metrics.expires_at
        }) else {
            return false;
        };
        let Some(write_started_at) = probe.write_started_at else {
            return false;
        };
        if received_at < write_started_at
            || received_at
                .checked_add(probe.metrics.proof_validity)
                .is_none()
        {
            return false;
        }
        if probe.receiver_confirmed {
            return true;
        }
        // Receipt owns completion and releases writers. This timestamp fence has
        // a separate job: suppress probe-era ACKs after that public epoch retires.
        if !self.install_capacity_ack_quarantine(
            token,
            probe.started_at,
            received_at,
            probe.metrics.proof_validity,
        ) {
            return false;
        }
        probe.receiver_confirmed = true;
        probe.metrics.receipt_received_payload_bytes = received_payload_bytes;
        probe.metrics.receipt_elapsed =
            Some(received_at.saturating_duration_since(write_started_at));
        probe.metrics.receipt_rtt = (!receipt_rtt.is_zero()).then_some(receipt_rtt);
        probe.metrics.receipt_at = Some(received_at);
        probe.metrics.proved_at = Some(received_at);
        probe.metrics.receipt_frozen_sent_watermark = Some(self.sent_bytes.load(Ordering::Acquire));
        if self.capacity_probe_can_finalize(probe) {
            probe.metrics.phase = CapacityProbePhase::Proven;
            release_token = true;
        }
        drop(current);
        if release_token {
            self.release_capacity_token(token);
        }
        true
    }

    fn finish_capacity_probe(&self, token: u64, phase: CapacityProbePhase, now: Instant) -> bool {
        let mut current = self
            .capacity_probe
            .lock()
            .expect("QUIC capacity probe lock");
        let Some(probe) = current
            .as_mut()
            .filter(|probe| probe.metrics.token == token)
        else {
            return false;
        };
        if matches!(
            probe.metrics.phase,
            CapacityProbePhase::Proven | CapacityProbePhase::Expired | CapacityProbePhase::Aborted
        ) {
            return false;
        }
        if phase == CapacityProbePhase::Expired && now < probe.metrics.expires_at {
            return false;
        }
        probe.metrics.phase = phase;
        let should_close = probe.write_started_at.is_some();
        drop(current);
        if should_close {
            self.capacity_fail_close_requested
                .store(true, Ordering::Release);
            self.capacity_fail_close_notify.notify_one();
        }
        self.release_capacity_token(token);
        should_close
    }

    fn expire_capacity_probe(&self, token: u64, now: Instant) -> bool {
        self.finish_capacity_probe(token, CapacityProbePhase::Expired, now)
    }

    fn abort_capacity_probe(&self, token: u64) -> bool {
        self.finish_capacity_probe(token, CapacityProbePhase::Aborted, Instant::now())
    }

    fn retire_capacity_probe(&self, token: u64) -> bool {
        let mut current = self
            .capacity_probe
            .lock()
            .expect("QUIC capacity probe lock");
        if current.as_ref().is_none_or(|probe| {
            probe.metrics.token != token
                || !matches!(
                    probe.metrics.phase,
                    CapacityProbePhase::Proven
                        | CapacityProbePhase::Expired
                        | CapacityProbePhase::Aborted
                )
        }) {
            return false;
        }
        current.take();
        true
    }

    fn add_sent(&self, bytes: u64) {
        // Quinn invokes on_sent for ACK-only datagrams too. Until on_end_acks
        // supplies its exact path flight, an additive estimate is not sound.
        // Invalidate that snapshot before the send watermark's release so a
        // reader cannot pair a new send with the preceding authoritative BIF.
        self.bytes_in_flight_authoritative
            .store(false, Ordering::Release);
        self.sent_bytes.fetch_add(bytes, Ordering::AcqRel);
    }

    fn publish_ack_batch(&self, totals: QuicAckTelemetryTotals, in_flight: u64, app_limited: bool) {
        if totals.sample_count > 0 {
            self.ack_snapshot_sequence.fetch_add(1, Ordering::AcqRel);
            self.newly_acked_bytes
                .fetch_add(totals.acked_bytes, Ordering::Relaxed);
            if totals.non_app_limited_sample_count > 0 {
                self.non_app_limited_acked_bytes
                    .fetch_add(totals.non_app_limited_acked_bytes, Ordering::Relaxed);
                self.non_app_limited_ack_elapsed_nanos
                    .fetch_add(totals.non_app_limited_ack_elapsed_nanos, Ordering::Relaxed);
                self.non_app_limited_delivery_sample_count
                    .fetch_add(totals.non_app_limited_sample_count, Ordering::Relaxed);
                if totals.timed_non_app_limited_sample_count > 0 {
                    self.timed_non_app_limited_acked_bytes
                        .fetch_add(totals.timed_non_app_limited_acked_bytes, Ordering::Relaxed);
                    self.timed_non_app_limited_delivery_sample_count
                        .fetch_add(totals.timed_non_app_limited_sample_count, Ordering::Relaxed);
                }
            }
            self.delivery_sample_count
                .fetch_add(totals.sample_count, Ordering::Relaxed);
            self.ack_snapshot_sequence.fetch_add(1, Ordering::Release);
        }
        self.bytes_in_flight.store(in_flight, Ordering::Relaxed);
        self.bytes_in_flight_authoritative
            .store(true, Ordering::Release);
        self.app_limited.store(app_limited, Ordering::Relaxed);
    }

    fn add_lost(&self, lost_bytes: u64) {
        if lost_bytes > 0 {
            self.lost_bytes.fetch_add(lost_bytes, Ordering::Relaxed);
            let _ = self.bytes_in_flight.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |current| Some(current.saturating_sub(lost_bytes)),
            );
        }
    }
}

impl InstrumentedController {
    fn new(
        inner: Box<dyn quinn::congestion::Controller>,
        telemetry: Arc<QuicCarrierTelemetry>,
    ) -> Self {
        Self {
            inner,
            telemetry,
            ack_batch_acked_bytes: 0,
            ack_batch_non_app_limited_acked_bytes: 0,
            ack_batch_sample_count: 0,
            ack_batch_non_app_limited_sample_count: 0,
            ack_batch_earliest_non_app_limited_sent: None,
            ack_batch_latest_non_app_limited_sent: None,
            last_non_app_limited_ack: None,
            non_app_limited_sent_high_watermark: None,
        }
    }

    fn accumulate_ack_telemetry(&mut self, sent: Instant, bytes: u64, app_limited: bool) {
        if bytes == 0 {
            return;
        }
        self.ack_batch_acked_bytes = self.ack_batch_acked_bytes.saturating_add(bytes);
        self.ack_batch_sample_count = self.ack_batch_sample_count.saturating_add(1);
        if app_limited {
            return;
        }
        self.ack_batch_non_app_limited_acked_bytes = self
            .ack_batch_non_app_limited_acked_bytes
            .saturating_add(bytes);
        self.ack_batch_non_app_limited_sample_count = self
            .ack_batch_non_app_limited_sample_count
            .saturating_add(1);
        self.ack_batch_earliest_non_app_limited_sent = Some(
            self.ack_batch_earliest_non_app_limited_sent
                .map_or(sent, |earliest| earliest.min(sent)),
        );
        self.ack_batch_latest_non_app_limited_sent = Some(
            self.ack_batch_latest_non_app_limited_sent
                .map_or(sent, |latest| latest.max(sent)),
        );
    }

    fn route_ack_telemetry(&mut self, now: Instant, sent: Instant, bytes: u64, app_limited: bool) {
        if !self
            .telemetry
            .accumulate_capacity_ack(now, sent, bytes, app_limited)
        {
            self.accumulate_ack_telemetry(sent, bytes, app_limited);
        }
    }

    fn finish_ack_telemetry(&mut self, now: Instant, in_flight: u64, app_limited: bool) {
        // Delivery rate uses the slower of the ACK and send clocks, expressed
        // as their maximum elapsed time. This excludes propagation RTT from a
        // new epoch while preventing compressed ACKs from inflating capacity.
        let batch_sent_range = self
            .ack_batch_earliest_non_app_limited_sent
            .zip(self.ack_batch_latest_non_app_limited_sent);
        let non_app_limited_ack_elapsed =
            batch_sent_range.and_then(|(earliest_sent, latest_sent)| {
                let within_batch_send_elapsed =
                    latest_sent.saturating_duration_since(earliest_sent);
                let elapsed = match (
                    self.last_non_app_limited_ack,
                    self.non_app_limited_sent_high_watermark,
                ) {
                    (Some(previous_ack), Some(sent_high_watermark)) => {
                        let ack_elapsed = now.saturating_duration_since(previous_ack);
                        let forward_send_elapsed =
                            latest_sent.saturating_duration_since(sent_high_watermark);
                        ack_elapsed
                            .max(forward_send_elapsed)
                            .max(within_batch_send_elapsed)
                    }
                    _ => within_batch_send_elapsed,
                };
                (!elapsed.is_zero()).then_some(elapsed)
            });
        let totals = QuicAckTelemetryTotals {
            acked_bytes: self.ack_batch_acked_bytes,
            non_app_limited_acked_bytes: self.ack_batch_non_app_limited_acked_bytes,
            timed_non_app_limited_acked_bytes: non_app_limited_ack_elapsed
                .map_or(0, |_| self.ack_batch_non_app_limited_acked_bytes),
            non_app_limited_ack_elapsed_nanos: non_app_limited_ack_elapsed
                .map_or(0, duration_as_u64_nanos),
            sample_count: self.ack_batch_sample_count,
            non_app_limited_sample_count: self.ack_batch_non_app_limited_sample_count,
            timed_non_app_limited_sample_count: non_app_limited_ack_elapsed
                .map_or(0, |_| self.ack_batch_non_app_limited_sample_count),
        };

        self.ack_batch_acked_bytes = 0;
        self.ack_batch_non_app_limited_acked_bytes = 0;
        self.ack_batch_sample_count = 0;
        self.ack_batch_non_app_limited_sample_count = 0;
        self.ack_batch_earliest_non_app_limited_sent = None;
        self.ack_batch_latest_non_app_limited_sent = None;

        if app_limited {
            self.last_non_app_limited_ack = None;
            self.non_app_limited_sent_high_watermark = None;
        } else if let Some((_, latest_sent)) = batch_sent_range {
            self.last_non_app_limited_ack = Some(now);
            self.non_app_limited_sent_high_watermark = Some(
                self.non_app_limited_sent_high_watermark
                    .map_or(latest_sent, |high_watermark| {
                        high_watermark.max(latest_sent)
                    }),
            );
        }
        self.telemetry
            .publish_ack_batch(totals, in_flight, app_limited);
    }
}

fn duration_as_u64_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

impl quinn::congestion::ControllerFactory for InstrumentedBbrConfig {
    fn build(
        self: Arc<Self>,
        now: Instant,
        current_mtu: u16,
    ) -> Box<dyn quinn::congestion::Controller> {
        // Use Quinn BBR for the QUIC carrier. mptunnel does not have an
        // operator-provided per-path bandwidth contract, so a fixed-rate
        // Brutal-style controller would either underfill unknown good paths or
        // overload weaker/shared paths. BBR's delivery-rate/RTT model is the
        // stable production default for feeding the product multipath scheduler;
        // QUIC still owns packet pacing, loss recovery, and bytes in flight.
        let inner = Arc::new(quinn::congestion::BbrConfig::default()).build(now, current_mtu);
        Box::new(InstrumentedController::new(
            inner,
            Arc::new(QuicCarrierTelemetry::default()),
        ))
    }
}

impl quinn::congestion::Controller for InstrumentedController {
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {
        self.telemetry.add_sent(bytes);
        self.inner.on_sent(now, bytes, last_packet_number);
    }

    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &quinn_proto::RttEstimator,
    ) {
        // A deliberate probe owns a connection-wide write epoch. Its ACKs
        // remain token evidence even when Quinn observes an empty app buffer,
        // and must never also become product delivery evidence.
        self.route_ack_telemetry(now, sent, bytes, app_limited);
        self.inner.on_ack(now, sent, bytes, app_limited, rtt);
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
        self.finish_ack_telemetry(now, in_flight, app_limited);
        self.telemetry.finish_capacity_ack_batch(now, in_flight);
        self.inner
            .on_end_acks(now, in_flight, app_limited, largest_packet_num_acked);
    }

    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        lost_bytes: u64,
    ) {
        self.telemetry.add_lost(lost_bytes);
        self.inner
            .on_congestion_event(now, sent, is_persistent_congestion, lost_bytes);
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.inner.on_mtu_update(new_mtu);
    }

    fn window(&self) -> u64 {
        self.inner.window()
    }

    fn metrics(&self) -> quinn::congestion::ControllerMetrics {
        self.inner.metrics()
    }

    fn clone_box(&self) -> Box<dyn quinn::congestion::Controller> {
        Box::new(Self {
            inner: self.inner.clone_box(),
            telemetry: self.telemetry.clone(),
            ack_batch_acked_bytes: self.ack_batch_acked_bytes,
            ack_batch_non_app_limited_acked_bytes: self.ack_batch_non_app_limited_acked_bytes,
            ack_batch_sample_count: self.ack_batch_sample_count,
            ack_batch_non_app_limited_sample_count: self.ack_batch_non_app_limited_sample_count,
            ack_batch_earliest_non_app_limited_sent: self.ack_batch_earliest_non_app_limited_sent,
            ack_batch_latest_non_app_limited_sent: self.ack_batch_latest_non_app_limited_sent,
            last_non_app_limited_ack: self.last_non_app_limited_ack,
            non_app_limited_sent_high_watermark: self.non_app_limited_sent_high_watermark,
        })
    }

    fn initial_window(&self) -> u64 {
        self.inner.initial_window()
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl Endpoint {
    pub async fn bind_server(
        addr: SocketAddr,
        secret: &[u8],
        mux_limits: MuxLimits,
    ) -> Result<Self, QuicCarrierError> {
        let endpoint = QuinnEndpoint::server(server_config(secret, mux_limits)?, addr)?;
        Ok(Self { endpoint })
    }

    pub async fn bind_client(
        addr: SocketAddr,
        secret: &[u8],
        mux_limits: MuxLimits,
    ) -> Result<Self, QuicCarrierError> {
        let mut endpoint = QuinnEndpoint::client(addr)?;
        endpoint.set_default_client_config(client_config(secret, mux_limits)?);
        Ok(Self { endpoint })
    }

    pub async fn connect(&self, remote: SocketAddr) -> Result<Connection, QuicCarrierError> {
        let connecting = self
            .endpoint
            .connect(remote, QUIC_CERT_DNS_NAME)
            .map_err(QuicCarrierError::Connect)?;
        Ok(Connection::from_quinn(connecting.await?))
    }

    pub async fn accept(&self) -> Option<Connection> {
        loop {
            let incoming = self.endpoint.accept().await?;
            match incoming.await {
                Ok(connection) => {
                    return Some(Connection::from_quinn(connection));
                }
                Err(err) => {
                    eprintln!("warning: QUIC carrier accept failed: {err}");
                    continue;
                }
            }
        }
    }

    pub fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.endpoint.local_addr()
    }
}

impl Connection {
    fn from_quinn(connection: quinn::Connection) -> Self {
        let telemetry = connection
            .congestion_state()
            .into_any()
            .downcast::<InstrumentedController>()
            .expect("QUIC carrier must use the instrumented congestion controller")
            .telemetry
            .clone();
        Self {
            connection,
            write_backlog: Arc::new(AtomicU64::new(0)),
            delivery_evidence_written: Arc::new(AtomicU64::new(0)),
            telemetry,
        }
    }

    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), QuicCarrierError> {
        let (send, recv) = self.connection.open_bi().await?;
        Ok((
            SendStream {
                stream: send,
                connection: self.connection.clone(),
                write_backlog: self.write_backlog.clone(),
                delivery_evidence_written: self.delivery_evidence_written.clone(),
                telemetry: self.telemetry.clone(),
                encode_buffer: Vec::new(),
            },
            RecvStream::new(recv),
        ))
    }

    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream), QuicCarrierError> {
        let (send, recv) = self.connection.accept_bi().await?;
        Ok((
            SendStream {
                stream: send,
                connection: self.connection.clone(),
                write_backlog: self.write_backlog.clone(),
                delivery_evidence_written: self.delivery_evidence_written.clone(),
                telemetry: self.telemetry.clone(),
                encode_buffer: Vec::new(),
            },
            RecvStream::new(recv),
        ))
    }

    pub fn close(&self) {
        self.connection.close(VarInt::from_u32(0), b"closed");
    }

    pub fn is_closed(&self) -> bool {
        self.connection.close_reason().is_some()
    }

    pub fn capacity_probe_active(&self) -> bool {
        self.telemetry.capacity_active_token.load(Ordering::Acquire) != 0
    }

    pub async fn wait_for_capacity_probe_release(&self) {
        self.telemetry.wait_for_capacity_release().await;
    }

    pub fn stats(&self) -> quinn::ConnectionStats {
        self.connection.stats()
    }

    pub fn congestion_metrics(&self) -> CongestionMetrics {
        let controller = self.connection.congestion_state();
        let metrics = controller.metrics();
        let current_telemetry = controller
            .into_any()
            .downcast::<InstrumentedController>()
            .expect("QUIC carrier must use the instrumented congestion controller")
            .telemetry
            .clone();
        if !Arc::ptr_eq(&current_telemetry, &self.telemetry) {
            // Quinn creates a fresh controller for a cross-address migration.
            // Existing streams still hold the old write gate, so fail this
            // carrier instead of publishing split ownership from two epochs.
            self.telemetry
                .capacity_fail_close_requested
                .store(true, Ordering::Release);
            self.connection.close(
                VarInt::from_u32(1),
                b"QUIC congestion controller ownership changed",
            );
        }
        let snapshot = current_telemetry.snapshot();
        CongestionMetrics {
            congestion_window: metrics.congestion_window,
            bytes_in_flight: snapshot.bytes_in_flight,
            pending_bytes: self.write_backlog.load(Ordering::Relaxed),
            pacing_rate_bps: metrics.pacing_rate,
            loss_ppm: snapshot.loss_ppm,
            ecn_ppm: None,
            newly_acked_bytes: snapshot.newly_acked_bytes,
            non_app_limited_acked_bytes: snapshot.non_app_limited_acked_bytes,
            timed_non_app_limited_acked_bytes: snapshot.timed_non_app_limited_acked_bytes,
            non_app_limited_ack_elapsed: snapshot.non_app_limited_ack_elapsed,
            delivery_evidence_written_bytes: self.delivery_evidence_written.load(Ordering::Relaxed),
            delivery_sample_count: snapshot.delivery_sample_count,
            non_app_limited_delivery_sample_count: snapshot.non_app_limited_delivery_sample_count,
            timed_non_app_limited_delivery_sample_count: snapshot
                .timed_non_app_limited_delivery_sample_count,
            app_limited: snapshot.app_limited,
            capacity_probe: snapshot.capacity_probe,
        }
    }

    pub fn cancel_capacity_probe(&self, token: u64) -> bool {
        let should_close = self.telemetry.abort_capacity_probe(token);
        if should_close {
            self.connection
                .close(VarInt::from_u32(1), b"cancelled capacity probe");
        }
        should_close
    }

    pub fn retire_capacity_probe(&self, token: u64) -> bool {
        self.telemetry.retire_capacity_probe(token)
    }

    pub fn confirm_capacity_probe_receipt(
        &self,
        token: u64,
        received_payload_bytes: u64,
        received_at: Instant,
    ) -> bool {
        let current_telemetry = self
            .connection
            .congestion_state()
            .into_any()
            .downcast::<InstrumentedController>()
            .expect("QUIC carrier must use the instrumented congestion controller")
            .telemetry
            .clone();
        if !Arc::ptr_eq(&current_telemetry, &self.telemetry) {
            self.connection.close(
                VarInt::from_u32(1),
                b"QUIC congestion controller ownership changed",
            );
            return false;
        }
        self.telemetry.confirm_capacity_probe_receipt(
            token,
            received_payload_bytes,
            received_at,
            self.connection.rtt(),
        )
    }
}

impl SendStream {
    pub fn cancel_capacity_probe(&self, token: u64) -> bool {
        let should_close = self.telemetry.abort_capacity_probe(token);
        if should_close {
            self.connection
                .close(VarInt::from_u32(1), b"cancelled capacity probe");
        }
        should_close
    }
}

pub async fn write_frame(
    send: &mut SendStream,
    frame: &Frame,
    limits: CodecLimits,
) -> Result<(), QuicCarrierError> {
    write_frames(send, std::slice::from_ref(frame), limits).await
}

pub async fn write_frames(
    send: &mut SendStream,
    frames: &[Frame],
    limits: CodecLimits,
) -> Result<(), QuicCarrierError> {
    if frames.is_empty() {
        return Ok(());
    }
    if frames.iter().any(|frame| {
        matches!(
            frame,
            Frame::PathCapacityData { .. }
                | Frame::PathCapacityFinish { .. }
                | Frame::PathCapacityReceipt { .. }
        )
    }) {
        return Err(QuicCarrierError::CapacityProbeRequiresDedicatedWrite);
    }
    let _ordinary_write = send.telemetry.enter_ordinary_writer().await;
    if send
        .telemetry
        .capacity_fail_close_requested
        .load(Ordering::Acquire)
    {
        send.connection
            .close(VarInt::from_u32(1), b"capacity probe failed closed");
        return Err(QuicCarrierError::CapacityProbeExpired);
    }
    #[cfg(feature = "lab-diagnostics")]
    let encode_started = std::time::Instant::now();
    let delivery_evidence_bytes = frames.iter().fold(0u64, |total, frame| {
        total.saturating_add(frame_delivery_evidence_bytes(frame) as u64)
    });
    let packet_len = {
        let packet = &mut send.encode_buffer;
        packet.clear();
        let capacity_hint = frames.iter().fold(0usize, |total, frame| {
            total.saturating_add(quic_encoded_frame_capacity_hint(frame))
        });
        packet.reserve(capacity_hint);
        for frame in frames {
            encode_quic_length_prefixed_frame(frame, limits, packet)?;
        }
        packet.len() as u64
    };
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.quic.encode_frames",
        encode_started.elapsed(),
        packet_len as usize,
    );
    #[cfg(feature = "lab-diagnostics")]
    let write_started = std::time::Instant::now();
    let transaction_connection = send.connection.clone();
    let transaction_backlog = send.write_backlog.clone();
    send.write_backlog.fetch_add(packet_len, Ordering::Relaxed);
    // Publish before the awaited write. Quinn can ACK earlier chunks while
    // write_all is flow-controlled; publishing afterward loses attribution for
    // those ACKs. A failed write closes the path, so stale evidence cannot be
    // reused by a live calibration target.
    if delivery_evidence_bytes > 0 {
        send.delivery_evidence_written
            .fetch_add(delivery_evidence_bytes, Ordering::Relaxed);
    }
    let write_transaction =
        QuicWriteTransaction::new(transaction_connection, transaction_backlog, packet_len);
    send.stream.write_all(&send.encode_buffer).await?;
    write_transaction.commit();
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.quic.write_frames_wait",
        write_started.elapsed(),
        packet_len as usize,
    );
    Ok(())
}

pub async fn write_capacity_probe(
    send: &mut SendStream,
    path_id: crate::protocol::PathId,
    spec: CapacityProbeSpec,
    max_payload_bytes: usize,
    limits: CodecLimits,
) -> Result<(), QuicCarrierError> {
    let current_telemetry = send
        .connection
        .congestion_state()
        .into_any()
        .downcast::<InstrumentedController>()
        .expect("QUIC carrier must use the instrumented congestion controller")
        .telemetry
        .clone();
    if !Arc::ptr_eq(&current_telemetry, &send.telemetry) {
        send.connection.close(
            VarInt::from_u32(1),
            b"QUIC congestion controller ownership changed",
        );
        return Err(QuicCarrierError::CapacityProbeExpired);
    }
    if send
        .telemetry
        .capacity_fail_close_requested
        .load(Ordering::Acquire)
    {
        send.connection
            .close(VarInt::from_u32(1), b"capacity probe failed closed");
        return Err(QuicCarrierError::CapacityProbeExpired);
    }
    if max_payload_bytes == 0
        || usize::try_from(spec.train_payload_bytes).is_err()
        || spec.token == 0
        || spec.train_payload_bytes == 0
    {
        return Err(QuicCarrierError::InvalidCapacityProbe);
    }
    let chunk_bytes = max_payload_bytes
        .min(limits.max_payload_bytes.max(1))
        .min(QUIC_CAPACITY_RECORD_PAYLOAD_BYTES);
    let train_payload_bytes = usize::try_from(spec.train_payload_bytes)
        .map_err(|_| QuicCarrierError::InvalidCapacityProbe)?;

    let reservation = send
        .telemetry
        .reserve_capacity_token(spec.token, spec.expires_at)
        .await?;
    send.telemetry
        .install_capacity_probe(spec, send.write_backlog.load(Ordering::Acquire))?;
    reservation.commit();
    let mut start_guard = CapacityProbeStartGuard {
        telemetry: send.telemetry.clone(),
        token: spec.token,
        keep_epoch: false,
    };
    let expiry_telemetry = send.telemetry.clone();
    let expiry_connection = send.connection.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(spec.expires_at)) => {
                if expiry_telemetry.expire_capacity_probe(spec.token, Instant::now()) {
                    expiry_connection.close(VarInt::from_u32(1), b"capacity probe expired");
                }
            }
            _ = expiry_telemetry.capacity_fail_close_notify.notified() => {
                expiry_connection.close(VarInt::from_u32(1), b"capacity probe failed closed");
            }
            _ = expiry_connection.closed() => {}
        }
    });

    let write_started_at = Instant::now();
    if !send
        .telemetry
        .mark_capacity_probe_write_started(spec.token, write_started_at)
    {
        return Err(QuicCarrierError::CapacityProbeExpired);
    }
    #[cfg(feature = "lab-diagnostics")]
    let write_started = Instant::now();
    #[cfg(feature = "lab-diagnostics")]
    let mut encode_elapsed = Duration::ZERO;
    #[cfg(feature = "lab-diagnostics")]
    let mut encoded_bytes = 0_u64;
    let zero_block = bytes::Bytes::from(vec![0_u8; chunk_bytes.min(train_payload_bytes)]);
    let mut remaining = train_payload_bytes;
    while remaining > 0 {
        let payload_bytes = remaining.min(zero_block.len());
        #[cfg(feature = "lab-diagnostics")]
        let record_encode_started = Instant::now();
        let record_bytes = encode_capacity_probe_record(
            send,
            &Frame::PathCapacityData {
                path_id,
                calibration_id: spec.token,
                payload: zero_block.slice(..payload_bytes),
            },
            limits,
        )?;
        #[cfg(feature = "lab-diagnostics")]
        {
            encode_elapsed = encode_elapsed.saturating_add(record_encode_started.elapsed());
        }
        #[cfg(feature = "lab-diagnostics")]
        {
            encoded_bytes = encoded_bytes.saturating_add(record_bytes);
        }
        write_capacity_probe_record(send, record_bytes).await?;
        if !send
            .telemetry
            .record_capacity_probe_data_written(spec.token, payload_bytes as u64)
        {
            return Err(QuicCarrierError::CapacityProbeExpired);
        }
        remaining -= payload_bytes;
    }
    #[cfg(feature = "lab-diagnostics")]
    let finish_encode_started = Instant::now();
    let finish_bytes = encode_capacity_probe_record(
        send,
        &Frame::PathCapacityFinish {
            path_id,
            calibration_id: spec.token,
            payload_bytes: spec.train_payload_bytes,
        },
        limits,
    )?;
    #[cfg(feature = "lab-diagnostics")]
    {
        encode_elapsed = encode_elapsed.saturating_add(finish_encode_started.elapsed());
    }
    #[cfg(feature = "lab-diagnostics")]
    {
        encoded_bytes = encoded_bytes.saturating_add(finish_bytes);
    }
    write_capacity_probe_record(send, finish_bytes).await?;
    if !send
        .telemetry
        .commit_capacity_probe_write(spec.token, Instant::now())
    {
        return Err(QuicCarrierError::CapacityProbeExpired);
    }
    start_guard.keep_epoch = true;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.quic.encode_capacity_probe",
        encode_elapsed,
        usize::try_from(encoded_bytes).unwrap_or(usize::MAX),
    );
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.quic.write_capacity_probe_wait",
        write_started.elapsed(),
        usize::try_from(encoded_bytes).unwrap_or(usize::MAX),
    );
    Ok(())
}

pub async fn write_capacity_receipt(
    send: &mut SendStream,
    path_id: crate::protocol::PathId,
    token: u64,
    received_payload_bytes: u64,
    limits: CodecLimits,
) -> Result<(), QuicCarrierError> {
    if token == 0 || received_payload_bytes == 0 {
        return Err(QuicCarrierError::InvalidCapacityProbe);
    }
    let _ordinary_write = send.telemetry.enter_ordinary_writer().await;
    if send
        .telemetry
        .capacity_fail_close_requested
        .load(Ordering::Acquire)
    {
        send.connection
            .close(VarInt::from_u32(1), b"capacity probe failed closed");
        return Err(QuicCarrierError::CapacityProbeExpired);
    }
    let record_bytes = encode_capacity_probe_record(
        send,
        &Frame::PathCapacityReceipt {
            path_id,
            calibration_id: token,
            received_payload_bytes,
        },
        limits,
    )?;
    write_capacity_probe_record(send, record_bytes).await
}

fn encode_capacity_probe_record(
    send: &mut SendStream,
    frame: &Frame,
    limits: CodecLimits,
) -> Result<u64, QuicCarrierError> {
    let packet = &mut send.encode_buffer;
    packet.clear();
    packet.reserve(quic_encoded_frame_capacity_hint(frame));
    encode_quic_length_prefixed_frame(frame, limits, packet)?;
    Ok(packet.len() as u64)
}

async fn write_capacity_probe_record(
    send: &mut SendStream,
    packet_len: u64,
) -> Result<(), QuicCarrierError> {
    let transaction_connection = send.connection.clone();
    let transaction_backlog = send.write_backlog.clone();
    send.write_backlog.fetch_add(packet_len, Ordering::Relaxed);
    let write_transaction =
        QuicWriteTransaction::new(transaction_connection, transaction_backlog, packet_len);
    send.stream.write_all(&send.encode_buffer).await?;
    write_transaction.commit();
    Ok(())
}

struct CapacityProbeStartGuard {
    telemetry: Arc<QuicCarrierTelemetry>,
    token: u64,
    keep_epoch: bool,
}

impl Drop for CapacityProbeStartGuard {
    fn drop(&mut self) {
        if !self.keep_epoch {
            self.telemetry.abort_capacity_probe(self.token);
        }
    }
}

fn frame_delivery_evidence_bytes(frame: &Frame) -> usize {
    match frame {
        Frame::StreamData { payload, .. } | Frame::DatagramData { payload, .. } => payload.len(),
        _ => 0,
    }
}

fn quic_encoded_frame_capacity_hint(frame: &Frame) -> usize {
    match frame {
        Frame::StreamData { payload, .. } if payload.len() > QUIC_STREAM_RECORD_PAYLOAD_BYTES => {
            let chunks = payload.len().div_ceil(QUIC_STREAM_RECORD_PAYLOAD_BYTES);
            encoded_frame_capacity_hint(frame)
                .saturating_add(chunks.saturating_mul(FRAME_LEN_BYTES + 32))
        }
        _ => FRAME_LEN_BYTES.saturating_add(encoded_frame_capacity_hint(frame)),
    }
}

fn encode_quic_length_prefixed_frame(
    frame: &Frame,
    limits: CodecLimits,
    packet: &mut Vec<u8>,
) -> Result<(), QuicCarrierError> {
    let Frame::StreamData {
        stream_id,
        offset,
        flags,
        payload,
    } = frame
    else {
        return encode_length_prefixed_frame(frame, limits, packet);
    };

    if payload.len() <= QUIC_STREAM_RECORD_PAYLOAD_BYTES {
        return encode_length_prefixed_frame(frame, limits, packet);
    }

    let mut cursor = 0usize;
    while cursor < payload.len() {
        let next = cursor
            .saturating_add(QUIC_STREAM_RECORD_PAYLOAD_BYTES)
            .min(payload.len());
        let split_flags = StreamFlags {
            fin: flags.fin && next == payload.len(),
            early_data: flags.early_data && cursor == 0,
        };
        let split = Frame::StreamData {
            stream_id: *stream_id,
            offset: offset.saturating_add(cursor as u64),
            flags: split_flags,
            payload: payload.slice(cursor..next),
        };
        encode_length_prefixed_frame(&split, limits, packet)?;
        cursor = next;
    }
    Ok(())
}

fn encode_length_prefixed_frame(
    frame: &Frame,
    limits: CodecLimits,
    packet: &mut Vec<u8>,
) -> Result<(), QuicCarrierError> {
    let len_offset = packet.len();
    packet.extend_from_slice(&[0u8; FRAME_LEN_BYTES]);
    let frame_start = packet.len();
    encode_frame_into(frame, limits, packet)?;
    let frame_len = packet.len().saturating_sub(frame_start);
    let frame_len = u32::try_from(frame_len).map_err(|_| QuicCarrierError::FrameTooLarge)?;
    packet[len_offset..len_offset + FRAME_LEN_BYTES].copy_from_slice(&frame_len.to_be_bytes());
    Ok(())
}

impl RecvStream {
    fn new(stream: quinn::RecvStream) -> Self {
        Self {
            stream,
            read_buffer: BytesMut::new(),
            read_scratch: Vec::new(),
        }
    }

    fn buffered_frame_len(&self, limits: CodecLimits) -> Result<Option<usize>, QuicCarrierError> {
        if self.read_buffer.len() < FRAME_LEN_BYTES {
            return Ok(None);
        }
        let len = u32::from_be_bytes([
            self.read_buffer[0],
            self.read_buffer[1],
            self.read_buffer[2],
            self.read_buffer[3],
        ]) as usize;
        if len > limits.max_frame_bytes {
            return Err(QuicCarrierError::FrameTooLarge);
        }
        Ok(Some(len))
    }

    fn pop_buffered_frame(
        &mut self,
        limits: CodecLimits,
    ) -> Result<Option<Frame>, QuicCarrierError> {
        let Some(len) = self.buffered_frame_len(limits)? else {
            return Ok(None);
        };
        let frame_end = FRAME_LEN_BYTES.saturating_add(len);
        if self.read_buffer.len() < frame_end {
            return Ok(None);
        }
        let _ = self.read_buffer.split_to(FRAME_LEN_BYTES);
        let encoded = self.read_buffer.split_to(len).freeze();
        Ok(Some(decode_frame_bytes(encoded, limits)?))
    }

    fn next_read_len(&self, limits: CodecLimits) -> Result<usize, QuicCarrierError> {
        let wanted = match self.buffered_frame_len(limits)? {
            Some(len) => FRAME_LEN_BYTES.saturating_add(len),
            None => FRAME_LEN_BYTES,
        };
        Ok(wanted
            .saturating_sub(self.read_buffer.len())
            .clamp(1, QUIC_RECV_CHUNK_BYTES))
    }
}

pub async fn read_frame(
    recv: &mut RecvStream,
    limits: CodecLimits,
) -> Result<Frame, QuicCarrierError> {
    loop {
        if let Some(frame) = recv.pop_buffered_frame(limits)? {
            return Ok(frame);
        }

        let read_len = recv.next_read_len(limits)?;
        recv.read_scratch.resize(read_len, 0);
        let read = recv
            .stream
            .read(&mut recv.read_scratch[..])
            .await
            .map_err(QuicCarrierError::Read)?
            .ok_or(QuicCarrierError::UnexpectedEnd)?;
        if read == 0 {
            return Err(QuicCarrierError::UnexpectedEnd);
        }
        recv.read_buffer
            .extend_from_slice(&recv.read_scratch[..read]);
    }
}

pub fn finish_stream(send: &mut SendStream) -> Result<(), QuicCarrierError> {
    // FIN is application output too. Refuse it while a capacity epoch owns the
    // connection rather than silently adding unclassified carrier bytes.
    let _ordinary_write = send
        .telemetry
        .try_enter_ordinary_writer()
        .ok_or(QuicCarrierError::CapacityProbeActive)?;
    if send
        .telemetry
        .capacity_fail_close_requested
        .load(Ordering::Acquire)
    {
        send.connection
            .close(VarInt::from_u32(1), b"capacity probe failed closed");
        return Err(QuicCarrierError::CapacityProbeExpired);
    }
    Ok(send.stream.finish()?)
}

pub fn max_stream_payload_bytes(limits: CodecLimits) -> usize {
    limits.max_payload_bytes.max(1)
}

fn server_config(secret: &[u8], mux_limits: MuxLimits) -> Result<ServerConfig, QuicCarrierError> {
    let (cert_der, key_der) = secret_bound_certificate(secret)?;
    let mut config = ServerConfig::with_single_cert(vec![cert_der], key_der.into())?;
    config.transport = Arc::new(quic_transport_config(mux_limits));
    Ok(config)
}

fn client_config(secret: &[u8], mux_limits: MuxLimits) -> Result<ClientConfig, QuicCarrierError> {
    let (cert_der, _) = secret_bound_certificate(secret)?;
    let mut config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(PinnedServerCertificate::new(cert_der))
        .with_no_client_auth();
    config.enable_sni = false;
    let mut config = ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(config)?,
    ));
    config.transport_config(Arc::new(quic_transport_config(mux_limits)));
    Ok(config)
}

fn quic_transport_config(mux_limits: MuxLimits) -> TransportConfig {
    let stream_receive_window = mux_limits.max_stream_window_bytes.max(1);
    let connection_receive_window = stream_receive_window
        .saturating_add(mux_limits.max_repair_bytes as u64)
        .saturating_add(mux_limits.max_reorder_bytes as u64)
        .saturating_add(mux_limits.max_datagram_queue_bytes as u64)
        .saturating_add(mux_limits.max_path_flight_bytes as u64);
    let send_window = (mux_limits.max_path_flight_bytes as u64)
        .max(mux_limits.max_reliable_relay_chunk_bytes as u64)
        .max(1);
    let concurrent_streams = (mux_limits.max_quic_concurrent_bidi_streams as u64)
        .max(1)
        .min(mux_limits.max_streams as u64);

    let mut transport = TransportConfig::default();
    transport
        .stream_receive_window(varint_saturating(stream_receive_window))
        .receive_window(varint_saturating(connection_receive_window))
        .send_window(send_window)
        .max_concurrent_bidi_streams(varint_saturating(concurrent_streams))
        .max_concurrent_uni_streams(0_u8.into())
        .datagram_receive_buffer_size(Some(mux_limits.max_datagram_queue_bytes))
        .datagram_send_buffer_size(mux_limits.max_datagram_queue_bytes)
        .congestion_controller_factory(Arc::new(InstrumentedBbrConfig));
    transport
}

fn varint_saturating(value: u64) -> VarInt {
    VarInt::from_u64(value.min(VarInt::MAX.into_inner()))
        .expect("bounded to QUIC variable integer range")
}

fn secret_bound_certificate(
    secret: &[u8],
) -> Result<(CertificateDer<'static>, PrivatePkcs8KeyDer<'static>), QuicCarrierError> {
    if secret.is_empty() {
        return Err(QuicCarrierError::EmptySecret);
    }
    let mut hasher = Sha256::new();
    hasher.update(b"mptunnel quic cert ed25519 seed v1");
    hasher.update(secret);
    let seed = hasher.finalize();
    let mut pkcs8 = Vec::with_capacity(ED25519_PKCS8_PREFIX.len() + seed.len());
    pkcs8.extend_from_slice(ED25519_PKCS8_PREFIX);
    pkcs8.extend_from_slice(&seed);

    let key_der = PrivatePkcs8KeyDer::from(pkcs8);
    let key_pair = rcgen::KeyPair::from_pkcs8_der_and_sign_algo(&key_der, &rcgen::PKCS_ED25519)?;
    let params = rcgen::CertificateParams::new(vec![QUIC_CERT_DNS_NAME.into()])?;
    let cert = params.self_signed(&key_pair)?;
    Ok((CertificateDer::from(cert), key_der))
}

#[derive(Debug)]
struct PinnedServerCertificate {
    expected_der: CertificateDer<'static>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl PinnedServerCertificate {
    fn new(expected_der: CertificateDer<'static>) -> Arc<Self> {
        Arc::new(Self {
            expected_der,
            provider: Arc::new(rustls::crypto::ring::default_provider()),
        })
    }
}

impl rustls::client::danger::ServerCertVerifier for PinnedServerCertificate {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if end_entity.as_ref() == self.expected_der.as_ref() {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(
                "QUIC server certificate does not match shared secret".into(),
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[derive(Debug)]
pub enum QuicCarrierError {
    Io(std::io::Error),
    Connect(quinn::ConnectError),
    Connection(ConnectionError),
    Write(quinn::WriteError),
    Read(quinn::ReadError),
    ReadExact(quinn::ReadExactError),
    UnexpectedEnd,
    ClosedStream(quinn::ClosedStream),
    FrameTooLarge,
    Codec(crate::protocol::codec::CodecError),
    Rustls(rustls::Error),
    QuinnCrypto(quinn::crypto::rustls::NoInitialCipherSuite),
    Rcgen(rcgen::Error),
    EmptySecret,
    InvalidCapacityProbe,
    CapacityProbeBusy,
    CapacityProbeNotIdle,
    CapacityProbeExpired,
    CapacityProbeRequiresDedicatedWrite,
    CapacityProbeActive,
}

impl fmt::Display for QuicCarrierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "QUIC carrier I/O failed: {err}"),
            Self::Connect(err) => write!(f, "QUIC carrier connect failed: {err}"),
            Self::Connection(err) => write!(f, "QUIC carrier connection failed: {err}"),
            Self::Write(err) => write!(f, "QUIC carrier write failed: {err}"),
            Self::Read(err) => write!(f, "QUIC carrier read failed: {err}"),
            Self::ReadExact(err) => write!(f, "QUIC carrier exact read failed: {err}"),
            Self::UnexpectedEnd => write!(f, "QUIC carrier stream ended mid-frame"),
            Self::ClosedStream(err) => write!(f, "QUIC carrier stream already closed: {err}"),
            Self::FrameTooLarge => write!(f, "QUIC carrier frame exceeds configured limits"),
            Self::Codec(err) => write!(f, "QUIC carrier frame codec failed: {err}"),
            Self::Rustls(err) => write!(f, "QUIC carrier TLS config failed: {err}"),
            Self::QuinnCrypto(err) => write!(f, "QUIC carrier crypto config failed: {err}"),
            Self::Rcgen(err) => write!(f, "QUIC carrier certificate generation failed: {err}"),
            Self::EmptySecret => write!(f, "QUIC carrier shared secret must not be empty"),
            Self::InvalidCapacityProbe => write!(f, "invalid QUIC capacity probe specification"),
            Self::CapacityProbeBusy => write!(f, "QUIC capacity probe result is still owned"),
            Self::CapacityProbeNotIdle => {
                write!(f, "QUIC capacity probe requires a quiescent carrier writer")
            }
            Self::CapacityProbeExpired => write!(f, "QUIC capacity probe expired"),
            Self::CapacityProbeRequiresDedicatedWrite => {
                write!(f, "QUIC capacity frames require the dedicated probe writer")
            }
            Self::CapacityProbeActive => write!(f, "QUIC capacity probe owns the write epoch"),
        }
    }
}

impl std::error::Error for QuicCarrierError {}

impl From<std::io::Error> for QuicCarrierError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<ConnectionError> for QuicCarrierError {
    fn from(value: ConnectionError) -> Self {
        Self::Connection(value)
    }
}

impl From<quinn::WriteError> for QuicCarrierError {
    fn from(value: quinn::WriteError) -> Self {
        Self::Write(value)
    }
}

impl From<quinn::ClosedStream> for QuicCarrierError {
    fn from(value: quinn::ClosedStream) -> Self {
        Self::ClosedStream(value)
    }
}

impl From<crate::protocol::codec::CodecError> for QuicCarrierError {
    fn from(value: crate::protocol::codec::CodecError) -> Self {
        Self::Codec(value)
    }
}

impl From<rustls::Error> for QuicCarrierError {
    fn from(value: rustls::Error) -> Self {
        Self::Rustls(value)
    }
}

impl From<quinn::crypto::rustls::NoInitialCipherSuite> for QuicCarrierError {
    fn from(value: quinn::crypto::rustls::NoInitialCipherSuite) -> Self {
        Self::QuinnCrypto(value)
    }
}

impl From<rcgen::Error> for QuicCarrierError {
    fn from(value: rcgen::Error) -> Self {
        Self::Rcgen(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{DatagramFlowId, DatagramId, PathId, StreamId};
    use bytes::Bytes;
    use tokio::time::timeout;

    #[test]
    fn quic_delivery_evidence_excludes_capacity_payloads() {
        let capacity = Frame::PathCapacityData {
            path_id: PathId(4),
            calibration_id: 17,
            payload: Bytes::from_static(b"capacity"),
        };
        let finish = Frame::PathCapacityFinish {
            path_id: PathId(4),
            calibration_id: 17,
            payload_bytes: 8,
        };
        let receipt = Frame::PathCapacityReceipt {
            path_id: PathId(4),
            calibration_id: 17,
            received_payload_bytes: 8,
        };
        let stream = Frame::StreamData {
            stream_id: StreamId(8),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"stream"),
        };
        let datagram = Frame::DatagramData {
            flow_id: DatagramFlowId(2),
            datagram_id: DatagramId(3),
            ttl_ms: 1_000,
            payload: Bytes::from_static(b"datagram"),
        };

        assert_eq!(frame_delivery_evidence_bytes(&capacity), 0);
        assert_eq!(frame_delivery_evidence_bytes(&finish), 0);
        assert_eq!(frame_delivery_evidence_bytes(&receipt), 0);
        assert_eq!(frame_delivery_evidence_bytes(&stream), 6);
        assert_eq!(frame_delivery_evidence_bytes(&datagram), 8);
        assert_eq!(frame_delivery_evidence_bytes(&Frame::Ping { nonce: 1 }), 0);
    }

    #[test]
    fn quic_ack_snapshot_keeps_non_app_limited_classification_coherent() {
        const BATCHES: u64 = 20_000;
        const ACK_BYTES: u64 = 1200;
        const NON_APP_ACKS_PER_BATCH: u64 = 2;
        const ACKS_PER_BATCH: u64 = 3;
        const ELAPSED_PER_BATCH: Duration = Duration::from_micros(7);
        let telemetry = Arc::new(QuicCarrierTelemetry::default());
        let writer = {
            let telemetry = telemetry.clone();
            std::thread::spawn(move || {
                for index in 0..BATCHES {
                    let non_app_limited = index % 2 == 0;
                    telemetry.publish_ack_batch(
                        QuicAckTelemetryTotals {
                            acked_bytes: ACKS_PER_BATCH * ACK_BYTES,
                            non_app_limited_acked_bytes: if non_app_limited {
                                NON_APP_ACKS_PER_BATCH * ACK_BYTES
                            } else {
                                0
                            },
                            timed_non_app_limited_acked_bytes: if non_app_limited {
                                NON_APP_ACKS_PER_BATCH * ACK_BYTES
                            } else {
                                0
                            },
                            non_app_limited_ack_elapsed_nanos: if non_app_limited {
                                duration_as_u64_nanos(ELAPSED_PER_BATCH)
                            } else {
                                0
                            },
                            sample_count: ACKS_PER_BATCH,
                            non_app_limited_sample_count: if non_app_limited {
                                NON_APP_ACKS_PER_BATCH
                            } else {
                                0
                            },
                            timed_non_app_limited_sample_count: if non_app_limited {
                                NON_APP_ACKS_PER_BATCH
                            } else {
                                0
                            },
                        },
                        0,
                        !non_app_limited,
                    );
                }
            })
        };

        let mut acked_bytes = 0_u64;
        let mut non_app_limited_bytes = 0_u64;
        let mut samples = 0_u64;
        let mut non_app_limited_samples = 0_u64;
        let mut non_app_limited_elapsed = Duration::ZERO;
        while !writer.is_finished() {
            let snapshot = telemetry.snapshot();
            assert_eq!(
                snapshot.newly_acked_bytes.unwrap_or(0),
                snapshot.delivery_sample_count * ACK_BYTES
            );
            assert_eq!(
                snapshot.non_app_limited_acked_bytes.unwrap_or(0),
                snapshot.non_app_limited_delivery_sample_count * ACK_BYTES
            );
            assert_eq!(
                snapshot.timed_non_app_limited_acked_bytes,
                snapshot.non_app_limited_acked_bytes
            );
            assert_eq!(
                snapshot.timed_non_app_limited_delivery_sample_count,
                snapshot.non_app_limited_delivery_sample_count
            );
            assert_eq!(
                snapshot.non_app_limited_ack_elapsed.unwrap_or_default(),
                ELAPSED_PER_BATCH
                    * (snapshot.non_app_limited_delivery_sample_count / NON_APP_ACKS_PER_BATCH)
                        as u32
            );
            acked_bytes = acked_bytes.saturating_add(snapshot.newly_acked_bytes.unwrap_or(0));
            non_app_limited_bytes = non_app_limited_bytes
                .saturating_add(snapshot.non_app_limited_acked_bytes.unwrap_or(0));
            samples = samples.saturating_add(snapshot.delivery_sample_count);
            non_app_limited_samples = non_app_limited_samples
                .saturating_add(snapshot.non_app_limited_delivery_sample_count);
            non_app_limited_elapsed += snapshot.non_app_limited_ack_elapsed.unwrap_or_default();
        }
        writer.join().expect("QUIC ACK telemetry writer");
        let final_snapshot = telemetry.snapshot();
        assert_eq!(
            final_snapshot.newly_acked_bytes.unwrap_or(0),
            final_snapshot.delivery_sample_count * ACK_BYTES
        );
        assert_eq!(
            final_snapshot.non_app_limited_acked_bytes.unwrap_or(0),
            final_snapshot.non_app_limited_delivery_sample_count * ACK_BYTES
        );
        assert_eq!(
            final_snapshot
                .non_app_limited_ack_elapsed
                .unwrap_or_default(),
            ELAPSED_PER_BATCH
                * (final_snapshot.non_app_limited_delivery_sample_count / NON_APP_ACKS_PER_BATCH)
                    as u32
        );
        acked_bytes = acked_bytes.saturating_add(final_snapshot.newly_acked_bytes.unwrap_or(0));
        non_app_limited_bytes = non_app_limited_bytes
            .saturating_add(final_snapshot.non_app_limited_acked_bytes.unwrap_or(0));
        samples = samples.saturating_add(final_snapshot.delivery_sample_count);
        non_app_limited_samples = non_app_limited_samples
            .saturating_add(final_snapshot.non_app_limited_delivery_sample_count);
        non_app_limited_elapsed += final_snapshot
            .non_app_limited_ack_elapsed
            .unwrap_or_default();

        assert_eq!(acked_bytes, BATCHES * ACKS_PER_BATCH * ACK_BYTES);
        assert_eq!(
            non_app_limited_bytes,
            BATCHES / 2 * NON_APP_ACKS_PER_BATCH * ACK_BYTES
        );
        assert_eq!(samples, BATCHES * ACKS_PER_BATCH);
        assert_eq!(
            non_app_limited_samples,
            BATCHES / 2 * NON_APP_ACKS_PER_BATCH
        );
        assert_eq!(
            non_app_limited_elapsed,
            ELAPSED_PER_BATCH * (BATCHES / 2) as u32
        );
    }

    fn test_instrumented_controller(
        base: Instant,
    ) -> (InstrumentedController, Arc<QuicCarrierTelemetry>) {
        let telemetry = Arc::new(QuicCarrierTelemetry::default());
        let inner = quinn::congestion::ControllerFactory::build(
            Arc::new(quinn::congestion::BbrConfig::default()),
            base,
            1200,
        );
        (
            InstrumentedController::new(inner, telemetry.clone()),
            telemetry,
        )
    }

    fn test_capacity_probe_spec(base: Instant) -> CapacityProbeSpec {
        CapacityProbeSpec {
            token: 91,
            train_payload_bytes: 300,
            sample_floor_bytes: 300,
            warmup_carrier_bytes: 100,
            required_timed_carrier_bytes: 200,
            expires_at: base + Duration::from_secs(5),
            proof_validity: Duration::from_secs(1),
        }
    }

    async fn install_test_capacity_probe(
        telemetry: &Arc<QuicCarrierTelemetry>,
        spec: CapacityProbeSpec,
    ) {
        let reservation = telemetry
            .reserve_capacity_token(spec.token, spec.expires_at)
            .await
            .expect("reserve capacity token");
        telemetry
            .install_capacity_probe(spec, 0)
            .expect("install capacity probe");
        reservation.commit();
        assert!(telemetry.mark_capacity_probe_write_started(spec.token, Instant::now()));
        assert!(
            telemetry.record_capacity_probe_data_written(spec.token, spec.train_payload_bytes,)
        );
        assert!(telemetry.commit_capacity_probe_write(spec.token, Instant::now()));
    }

    #[tokio::test]
    async fn quic_capacity_probe_accepts_app_limited_acks_without_product_evidence() {
        let base = Instant::now();
        let (mut controller, telemetry) = test_instrumented_controller(base);
        let spec = test_capacity_probe_spec(base);
        install_test_capacity_probe(&telemetry, spec).await;

        controller.route_ack_telemetry(
            base + Duration::from_millis(100),
            base + Duration::from_millis(1),
            100,
            true,
        );
        controller.finish_ack_telemetry(base + Duration::from_millis(100), 200, true);
        telemetry.finish_capacity_ack_batch(base + Duration::from_millis(100), 200);
        controller.route_ack_telemetry(
            base + Duration::from_millis(110),
            base + Duration::from_millis(11),
            100,
            true,
        );
        controller.route_ack_telemetry(
            base + Duration::from_millis(110),
            base + Duration::from_millis(21),
            100,
            true,
        );
        controller.finish_ack_telemetry(base + Duration::from_millis(110), 0, true);
        telemetry.finish_capacity_ack_batch(base + Duration::from_millis(110), 0);

        let provisional = telemetry.snapshot();
        assert_eq!(provisional.newly_acked_bytes, None);
        assert_eq!(provisional.non_app_limited_acked_bytes, None);
        let capacity = provisional.capacity_probe.expect("capacity snapshot");
        assert_eq!(capacity.phase, CapacityProbePhase::ProvenDraining);
        assert_eq!(capacity.total_acked_carrier_bytes, 300);
        assert_eq!(capacity.warmup_acked_carrier_bytes, 100);
        assert_eq!(capacity.measurement_acked_carrier_bytes, 200);
        assert_eq!(capacity.timed_measurement_acked_carrier_bytes, 200);
        assert_eq!(capacity.app_limited_acked_carrier_bytes, 300);
        assert_eq!(capacity.app_limited_ack_sample_count, 3);
        assert_eq!(
            capacity.timed_measurement_ack_elapsed,
            Some(Duration::from_millis(20))
        );
        assert!(capacity.native_proved_at.is_some());
        assert_eq!(capacity.proved_at, None);
        assert!(telemetry.confirm_capacity_probe_receipt(
            spec.token,
            spec.train_payload_bytes,
            base + Duration::from_millis(111),
            Duration::from_millis(10),
        ));
        let completed = telemetry
            .snapshot()
            .capacity_probe
            .expect("receipt-completed probe");
        assert_eq!(completed.phase, CapacityProbePhase::Proven);
        assert_eq!(completed.proved_at, completed.receipt_at);
        assert_eq!(
            completed.receipt_received_payload_bytes,
            spec.train_payload_bytes
        );
        assert!(completed.receipt_elapsed.is_some());
    }

    #[tokio::test]
    async fn quic_capacity_probe_zero_span_measurement_does_not_prove() {
        let base = Instant::now();
        let (mut controller, telemetry) = test_instrumented_controller(base);
        let spec = CapacityProbeSpec {
            train_payload_bytes: 200,
            sample_floor_bytes: 200,
            warmup_carrier_bytes: 0,
            required_timed_carrier_bytes: 100,
            ..test_capacity_probe_spec(base)
        };
        install_test_capacity_probe(&telemetry, spec).await;

        controller.route_ack_telemetry(
            base + Duration::from_millis(100),
            base + Duration::from_millis(1),
            100,
            false,
        );
        controller.finish_ack_telemetry(base + Duration::from_millis(100), 100, false);
        telemetry.finish_capacity_ack_batch(base + Duration::from_millis(100), 100);
        let untimed = telemetry.snapshot().capacity_probe.expect("untimed probe");
        assert_eq!(untimed.measurement_acked_carrier_bytes, 100);
        assert_eq!(untimed.timed_measurement_acked_carrier_bytes, 0);
        assert_eq!(untimed.phase, CapacityProbePhase::Measuring);

        controller.route_ack_telemetry(
            base + Duration::from_millis(110),
            base + Duration::from_millis(11),
            100,
            false,
        );
        controller.finish_ack_telemetry(base + Duration::from_millis(110), 0, false);
        telemetry.finish_capacity_ack_batch(base + Duration::from_millis(110), 0);
        let timed = telemetry.snapshot().capacity_probe.expect("timed probe");
        assert_eq!(timed.timed_measurement_acked_carrier_bytes, 100);
        assert_eq!(
            timed.timed_measurement_ack_elapsed,
            Some(Duration::from_millis(10))
        );
        assert_eq!(timed.phase, CapacityProbePhase::ProvenDraining);
        assert!(telemetry.confirm_capacity_probe_receipt(
            spec.token,
            spec.train_payload_bytes,
            base + Duration::from_millis(111),
            Duration::from_millis(10),
        ));
        assert_eq!(
            telemetry
                .snapshot()
                .capacity_probe
                .expect("receipt-completed probe")
                .phase,
            CapacityProbePhase::Proven
        );
    }

    #[tokio::test]
    async fn quic_capacity_probe_snapshot_is_cumulative_and_terminal_is_sticky() {
        let base = Instant::now();
        let (_controller, telemetry) = test_instrumented_controller(base);
        let spec = test_capacity_probe_spec(base);
        install_test_capacity_probe(&telemetry, spec).await;
        assert!(telemetry.accumulate_capacity_ack(
            base + Duration::from_millis(10),
            base + Duration::from_millis(1),
            75,
            true,
        ));
        telemetry.finish_capacity_ack_batch(base + Duration::from_millis(10), 225);

        let first = telemetry.snapshot().capacity_probe.expect("first snapshot");
        let second = telemetry
            .snapshot()
            .capacity_probe
            .expect("second snapshot");
        assert_eq!(first.total_acked_carrier_bytes, 75);
        assert_eq!(second.total_acked_carrier_bytes, 75);
        assert_eq!(second.app_limited_acked_carrier_bytes, 75);
        assert!(!telemetry.retire_capacity_probe(spec.token));
        assert!(telemetry.abort_capacity_probe(spec.token));
        assert_eq!(
            telemetry
                .snapshot()
                .capacity_probe
                .expect("aborted probe")
                .phase,
            CapacityProbePhase::Aborted
        );
        assert!(telemetry.retire_capacity_probe(spec.token));
        assert!(telemetry.snapshot().capacity_probe.is_none());
    }

    #[tokio::test]
    async fn quic_capacity_probe_replaces_only_a_terminal_prior_token() {
        let base = Instant::now();
        let (_controller, telemetry) = test_instrumented_controller(base);
        let first = test_capacity_probe_spec(base);
        let first_reservation = telemetry
            .reserve_capacity_token(first.token, first.expires_at)
            .await
            .expect("reserve first token");
        telemetry
            .install_capacity_probe(first, 0)
            .expect("install first probe");
        first_reservation.commit();
        assert!(!telemetry.abort_capacity_probe(first.token));

        let second = CapacityProbeSpec {
            token: first.token + 1,
            ..first
        };
        install_test_capacity_probe(&telemetry, second).await;
        let snapshot = telemetry
            .snapshot()
            .capacity_probe
            .expect("replacement probe");
        assert_eq!(snapshot.token, second.token);
        assert_eq!(snapshot.phase, CapacityProbePhase::Measuring);
        assert_eq!(snapshot.total_acked_carrier_bytes, 0);
        assert_eq!(snapshot.timed_measurement_acked_carrier_bytes, 0);
    }

    #[tokio::test]
    async fn quic_capacity_probe_rejects_mismatched_receipt_without_releasing_gate() {
        let base = Instant::now();
        let (_controller, telemetry) = test_instrumented_controller(base);
        let spec = test_capacity_probe_spec(base);
        install_test_capacity_probe(&telemetry, spec).await;
        telemetry.finish_capacity_ack_batch(base + Duration::from_millis(1), 0);

        assert!(!telemetry.confirm_capacity_probe_receipt(
            spec.token,
            spec.train_payload_bytes - 1,
            base + Duration::from_millis(2),
            Duration::from_millis(10),
        ));
        assert_eq!(
            telemetry.capacity_active_token.load(Ordering::Acquire),
            spec.token
        );
        let snapshot = telemetry.snapshot().capacity_probe.expect("active probe");
        assert_eq!(snapshot.receipt_received_payload_bytes, 0);
        assert_eq!(snapshot.receipt_elapsed, None);
    }

    #[tokio::test]
    async fn quic_capacity_probe_exact_receipt_releases_despite_native_flight_snapshot() {
        let base = Instant::now();
        let (mut controller, telemetry) = test_instrumented_controller(base);
        let spec = test_capacity_probe_spec(base);
        install_test_capacity_probe(&telemetry, spec).await;

        controller.route_ack_telemetry(
            base + Duration::from_millis(20),
            base + Duration::from_millis(1),
            100,
            true,
        );
        controller.route_ack_telemetry(
            base + Duration::from_millis(20),
            base + Duration::from_millis(11),
            200,
            true,
        );
        controller.finish_ack_telemetry(base + Duration::from_millis(20), 120, true);
        telemetry.finish_capacity_ack_batch(base + Duration::from_millis(20), 120);
        assert!(telemetry.confirm_capacity_probe_receipt(
            spec.token,
            spec.train_payload_bytes,
            base + Duration::from_millis(21),
            Duration::from_millis(10),
        ));
        assert_eq!(
            telemetry
                .snapshot()
                .capacity_probe
                .expect("completed probe")
                .phase,
            CapacityProbePhase::Proven
        );
        assert_eq!(telemetry.capacity_active_token.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn quic_capacity_probe_zero_then_ack_only_then_receipt_releases_gate() {
        let base = Instant::now();
        let telemetry = Arc::new(QuicCarrierTelemetry::default());
        let spec = test_capacity_probe_spec(base);
        install_test_capacity_probe(&telemetry, spec).await;

        telemetry.finish_capacity_ack_batch(base + Duration::from_millis(1), 0);
        telemetry.add_sent(1200);
        assert!(telemetry.confirm_capacity_probe_receipt(
            spec.token,
            spec.train_payload_bytes,
            base + Duration::from_millis(2),
            Duration::from_millis(10),
        ));
        assert_eq!(
            telemetry
                .snapshot()
                .capacity_probe
                .expect("receipt-completed probe")
                .phase,
            CapacityProbePhase::Proven,
            "exact receipt must not wait for an ACK-only send to receive an impossible ACK"
        );
        assert_eq!(telemetry.capacity_active_token.load(Ordering::Acquire), 0);
        let completed = telemetry
            .snapshot()
            .capacity_probe
            .expect("receipt-completed probe metrics");
        assert_eq!(completed.last_authoritative_in_flight, Some(0));
        assert_eq!(completed.last_authoritative_sent_watermark, Some(0));
        assert_eq!(completed.receipt_frozen_sent_watermark, Some(1200));
        assert_eq!(completed.current_sent_watermark, 1200);
    }

    #[tokio::test]
    async fn quic_capacity_probe_exact_receipt_releases_without_ack_batch() {
        let base = Instant::now();
        let telemetry = Arc::new(QuicCarrierTelemetry::default());
        let spec = test_capacity_probe_spec(base);
        install_test_capacity_probe(&telemetry, spec).await;

        telemetry.add_sent(1200);
        assert!(telemetry.confirm_capacity_probe_receipt(
            spec.token,
            spec.train_payload_bytes,
            base + Duration::from_millis(1),
            Duration::from_millis(10),
        ));
        let received = telemetry
            .snapshot()
            .capacity_probe
            .expect("receipt-confirmed probe");
        assert_eq!(received.receipt_frozen_sent_watermark, Some(1200));
        assert_eq!(received.current_sent_watermark, 1200);
        assert_eq!(received.phase, CapacityProbePhase::Proven);
        assert_eq!(received.last_authoritative_in_flight, None);
        assert_eq!(received.last_authoritative_sent_watermark, None);
        assert_eq!(telemetry.capacity_active_token.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn quic_capacity_receipt_releases_writers_but_quarantines_probe_era_acks() {
        let base = Instant::now();
        let (mut controller, telemetry) = test_instrumented_controller(base);
        let spec = test_capacity_probe_spec(base);
        install_test_capacity_probe(&telemetry, spec).await;
        let probe_started_at = telemetry
            .capacity_probe
            .lock()
            .expect("capacity probe lock")
            .as_ref()
            .expect("installed capacity probe")
            .started_at;
        let receipt_at = Instant::now();

        assert!(telemetry.confirm_capacity_probe_receipt(
            spec.token,
            spec.train_payload_bytes,
            receipt_at,
            Duration::from_millis(10),
        ));
        assert_eq!(telemetry.capacity_active_token.load(Ordering::Acquire), 0);
        assert!(telemetry.retire_capacity_probe(spec.token));
        assert!(telemetry.snapshot().capacity_probe.is_none());

        let second = CapacityProbeSpec {
            token: spec.token + 1,
            ..spec
        };
        assert!(matches!(
            telemetry
                .reserve_capacity_token(second.token, second.expires_at)
                .await,
            Err(QuicCarrierError::CapacityProbeBusy)
        ));

        // Product payload may be admitted as soon as receipt releases the writer
        // gate. A late ACK from the probe interval must not satisfy that evidence.
        controller.route_ack_telemetry(
            receipt_at + Duration::from_millis(1),
            probe_started_at,
            200,
            false,
        );
        controller.finish_ack_telemetry(receipt_at + Duration::from_millis(1), 0, false);
        let quarantined = telemetry.snapshot();
        assert_eq!(quarantined.newly_acked_bytes, None);
        assert_eq!(quarantined.non_app_limited_acked_bytes, None);

        let quarantine_end = receipt_at + spec.proof_validity;
        controller.route_ack_telemetry(
            quarantine_end + Duration::from_millis(1),
            quarantine_end,
            77,
            false,
        );
        controller.finish_ack_telemetry(quarantine_end + Duration::from_millis(1), 0, false);
        let later_product = telemetry.snapshot();
        assert_eq!(later_product.newly_acked_bytes, Some(77));
        assert_eq!(later_product.non_app_limited_acked_bytes, Some(77));

        let reservation = telemetry
            .reserve_capacity_token(second.token, second.expires_at)
            .await
            .expect("expired quarantine permits replacement probe");
        drop(reservation);
    }

    #[tokio::test]
    async fn quic_capacity_probe_ack_after_deadline_expires_instead_of_proving() {
        let base = Instant::now();
        let (mut controller, telemetry) = test_instrumented_controller(base);
        let spec = CapacityProbeSpec {
            expires_at: base + Duration::from_secs(1),
            ..test_capacity_probe_spec(base)
        };
        install_test_capacity_probe(&telemetry, spec).await;

        controller.route_ack_telemetry(
            base + Duration::from_millis(1_001),
            base + Duration::from_millis(1),
            100,
            true,
        );
        controller.route_ack_telemetry(
            base + Duration::from_millis(1_001),
            base + Duration::from_millis(2),
            200,
            true,
        );
        controller.finish_ack_telemetry(base + Duration::from_millis(1_001), 0, true);
        telemetry.finish_capacity_ack_batch(base + Duration::from_millis(1_001), 0);

        let expired = telemetry.snapshot().capacity_probe.expect("expired probe");
        assert_eq!(expired.phase, CapacityProbePhase::Expired);
        assert_eq!(expired.proved_at, None);
        assert_eq!(telemetry.capacity_active_token.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn quic_ack_only_flight_estimate_cannot_claim_or_block_clean_start() {
        let base = Instant::now();
        let telemetry = Arc::new(QuicCarrierTelemetry::default());
        telemetry.bytes_in_flight.store(37, Ordering::Release);
        assert_eq!(telemetry.snapshot().bytes_in_flight, None);
        let spec = test_capacity_probe_spec(base);
        let reservation = telemetry
            .reserve_capacity_token(spec.token, spec.expires_at)
            .await
            .expect("reserve capacity token");
        telemetry
            .install_capacity_probe(spec, 0)
            .expect("provisional probe ignores phantom native flight");
        reservation.commit();

        let probe = telemetry
            .snapshot()
            .capacity_probe
            .expect("installed probe");
        assert!(!probe.started_clean);
        assert_eq!(probe.phase, CapacityProbePhase::Writing);
        assert!(!telemetry.abort_capacity_probe(spec.token));
    }

    #[tokio::test]
    async fn quic_capacity_gate_waits_for_an_existing_ordinary_writer() {
        let base = Instant::now();
        let telemetry = Arc::new(QuicCarrierTelemetry::default());
        let ordinary = telemetry.enter_ordinary_writer().await;
        let waiter_telemetry = telemetry.clone();
        let waiter = tokio::spawn(async move {
            waiter_telemetry
                .reserve_capacity_token(41, base + Duration::from_secs(1))
                .await
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        drop(ordinary);
        let reservation = timeout(Duration::from_secs(1), waiter)
            .await
            .expect("capacity reservation timeout")
            .expect("capacity reservation task")
            .expect("capacity reservation");
        assert_eq!(telemetry.capacity_active_token.load(Ordering::Acquire), 41);
        drop(reservation);
        assert_eq!(telemetry.capacity_active_token.load(Ordering::Acquire), 0);
    }

    #[test]
    fn quic_first_ack_batch_excludes_path_rtt_and_app_limited_idle() {
        let base = Instant::now();
        let (mut controller, telemetry) = test_instrumented_controller(base);

        controller.accumulate_ack_telemetry(base, 200, false);
        controller.accumulate_ack_telemetry(base + Duration::from_millis(4), 100, false);
        controller.finish_ack_telemetry(base + Duration::from_millis(200), 900, false);
        let first = telemetry.snapshot();
        assert_eq!(first.non_app_limited_acked_bytes, Some(300));
        assert_eq!(first.non_app_limited_delivery_sample_count, 2);
        assert_eq!(
            first.non_app_limited_ack_elapsed,
            Some(Duration::from_millis(4)),
            "the first delivery interval must not include the 200 ms path RTT"
        );

        controller.accumulate_ack_telemetry(base + Duration::from_millis(205), 25, true);
        controller.finish_ack_telemetry(base + Duration::from_millis(210), 875, true);
        let idle = telemetry.snapshot();
        assert_eq!(idle.newly_acked_bytes, Some(25));
        assert_eq!(idle.non_app_limited_acked_bytes, None);
        assert_eq!(idle.non_app_limited_ack_elapsed, None);

        controller.accumulate_ack_telemetry(base + Duration::from_millis(300), 250, false);
        controller.accumulate_ack_telemetry(base + Duration::from_millis(306), 250, false);
        controller.finish_ack_telemetry(base + Duration::from_millis(600), 0, false);
        let after_idle = telemetry.snapshot();
        assert_eq!(after_idle.non_app_limited_acked_bytes, Some(500));
        assert_eq!(
            after_idle.non_app_limited_ack_elapsed,
            Some(Duration::from_millis(6)),
            "an app-limited end must reset both delivery clocks"
        );
        assert_eq!(after_idle.bytes_in_flight, Some(0));
    }

    #[test]
    fn quic_ack_send_clock_resists_ack_compression() {
        let base = Instant::now();
        let (mut controller, telemetry) = test_instrumented_controller(base);

        controller.accumulate_ack_telemetry(base, 100, false);
        controller.accumulate_ack_telemetry(base + Duration::from_millis(10), 100, false);
        controller.finish_ack_telemetry(base + Duration::from_millis(100), 0, false);
        assert_eq!(
            telemetry.snapshot().non_app_limited_ack_elapsed,
            Some(Duration::from_millis(10))
        );

        controller.accumulate_ack_telemetry(base + Duration::from_millis(15), 100, false);
        controller.accumulate_ack_telemetry(base + Duration::from_millis(30), 100, false);
        controller.finish_ack_telemetry(base + Duration::from_millis(101), 0, false);
        assert_eq!(
            telemetry.snapshot().non_app_limited_ack_elapsed,
            Some(Duration::from_millis(20)),
            "the send clock must win when ACK batches are only 1 ms apart"
        );
    }

    #[test]
    fn quic_zero_span_first_ack_batch_is_untimed_but_seeds_clocks() {
        let base = Instant::now();
        let (mut controller, telemetry) = test_instrumented_controller(base);

        controller.accumulate_ack_telemetry(base, 1200, false);
        controller.finish_ack_telemetry(base + Duration::from_millis(250), 0, false);
        let seed = telemetry.snapshot();
        assert_eq!(seed.non_app_limited_acked_bytes, Some(1200));
        assert_eq!(seed.non_app_limited_delivery_sample_count, 1);
        assert_eq!(seed.timed_non_app_limited_acked_bytes, None);
        assert_eq!(seed.timed_non_app_limited_delivery_sample_count, 0);
        assert_eq!(seed.non_app_limited_ack_elapsed, None);

        controller.accumulate_ack_telemetry(base + Duration::from_millis(10), 1200, false);
        controller.finish_ack_telemetry(base + Duration::from_millis(260), 0, false);
        assert_eq!(
            telemetry.snapshot().non_app_limited_ack_elapsed,
            Some(Duration::from_millis(10)),
            "an untimed first batch must still seed both clocks"
        );
    }

    #[test]
    fn quic_untimed_seed_cannot_join_timed_bytes_between_metric_polls() {
        let base = Instant::now();
        let (mut controller, telemetry) = test_instrumented_controller(base);

        controller.accumulate_ack_telemetry(base, 1200, false);
        controller.finish_ack_telemetry(base + Duration::from_millis(250), 0, false);
        controller.accumulate_ack_telemetry(base + Duration::from_millis(10), 1200, false);
        controller.finish_ack_telemetry(base + Duration::from_millis(260), 0, false);

        let combined = telemetry.snapshot();
        assert_eq!(combined.non_app_limited_acked_bytes, Some(2400));
        assert_eq!(combined.non_app_limited_delivery_sample_count, 2);
        assert_eq!(combined.timed_non_app_limited_acked_bytes, Some(1200));
        assert_eq!(combined.timed_non_app_limited_delivery_sample_count, 1);
        assert_eq!(
            combined.non_app_limited_ack_elapsed,
            Some(Duration::from_millis(10))
        );
    }

    #[test]
    fn quic_reordered_ack_batch_cannot_move_send_frontier_backward() {
        let base = Instant::now();
        let (mut controller, telemetry) = test_instrumented_controller(base);

        controller.accumulate_ack_telemetry(base, 100, false);
        controller.accumulate_ack_telemetry(base + Duration::from_millis(10), 100, false);
        controller.finish_ack_telemetry(base + Duration::from_millis(100), 0, false);
        assert_eq!(
            telemetry.snapshot().non_app_limited_ack_elapsed,
            Some(Duration::from_millis(10))
        );

        controller.accumulate_ack_telemetry(base + Duration::from_millis(8), 100, false);
        controller.accumulate_ack_telemetry(base + Duration::from_millis(2), 100, false);
        controller.finish_ack_telemetry(base + Duration::from_millis(101), 0, false);
        assert_eq!(
            telemetry.snapshot().non_app_limited_ack_elapsed,
            Some(Duration::from_millis(6)),
            "within-batch send spacing must guard a reordered ACK batch"
        );

        controller.accumulate_ack_telemetry(base + Duration::from_millis(12), 100, false);
        controller.finish_ack_telemetry(base + Duration::from_millis(102), 0, false);
        assert_eq!(
            telemetry.snapshot().non_app_limited_ack_elapsed,
            Some(Duration::from_millis(2)),
            "the send frontier must remain at 10 ms rather than regress to 8 ms"
        );
    }

    #[test]
    fn quic_writer_splits_large_stream_data_below_product_scheduler() {
        let limits = CodecLimits::default();
        let payload = Bytes::from(vec![7u8; QUIC_STREAM_RECORD_PAYLOAD_BYTES * 2 + 17]);
        let mut packet = Vec::new();
        encode_quic_length_prefixed_frame(
            &Frame::StreamData {
                stream_id: StreamId(9),
                offset: 123,
                flags: StreamFlags {
                    fin: true,
                    early_data: true,
                },
                payload,
            },
            limits,
            &mut packet,
        )
        .expect("encode split stream data");

        let mut cursor = 0usize;
        let mut decoded = Vec::new();
        while cursor < packet.len() {
            let len = u32::from_be_bytes([
                packet[cursor],
                packet[cursor + 1],
                packet[cursor + 2],
                packet[cursor + 3],
            ]) as usize;
            cursor += FRAME_LEN_BYTES;
            let frame = decode_frame_bytes(
                Bytes::copy_from_slice(&packet[cursor..cursor + len]),
                limits,
            )
            .expect("decode split carrier record");
            decoded.push(frame);
            cursor += len;
        }

        assert_eq!(decoded.len(), 3);
        let mut expected_offset = 123u64;
        for (index, frame) in decoded.iter().enumerate() {
            let Frame::StreamData {
                stream_id,
                offset,
                flags,
                payload,
            } = frame
            else {
                panic!("all split records must remain STREAM_DATA");
            };
            assert_eq!(*stream_id, StreamId(9));
            assert_eq!(*offset, expected_offset);
            expected_offset = expected_offset.saturating_add(payload.len() as u64);
            assert!(payload.len() <= QUIC_STREAM_RECORD_PAYLOAD_BYTES);
            assert_eq!(flags.early_data, index == 0);
            assert_eq!(flags.fin, index == 2);
        }
    }

    #[tokio::test]
    async fn quic_carrier_round_trips_product_frames() {
        let secret = b"0123456789abcdef0123456789abcdef";
        let limits = CodecLimits::default();
        let mux_limits = MuxLimits::default();
        let server = Endpoint::bind_server(
            "127.0.0.1:0".parse().expect("server addr"),
            secret,
            mux_limits,
        )
        .await
        .expect("server endpoint");
        let server_addr = server.local_addr().expect("server local addr");
        let (client_done_tx, client_done_rx) = tokio::sync::oneshot::channel();
        let server_task = tokio::spawn(async move {
            let connection = server.accept().await.expect("accepted connection");
            let (mut send, mut recv) = connection.accept_bi().await.expect("accepted stream");
            match read_frame(&mut recv, limits)
                .await
                .expect("server read ping")
            {
                Frame::Ping { nonce } => {
                    write_frame(&mut send, &Frame::Pong { nonce }, limits)
                        .await
                        .expect("server write pong");
                    finish_stream(&mut send).expect("server finish stream");
                }
                frame => panic!("unexpected frame: {frame:?}"),
            }
            let _ = timeout(Duration::from_secs(5), client_done_rx).await;
        });

        let client = Endpoint::bind_client(
            "127.0.0.1:0".parse().expect("client addr"),
            secret,
            mux_limits,
        )
        .await
        .expect("client endpoint");
        let connection = client.connect(server_addr).await.expect("client connect");
        let (mut send, mut recv) = connection.open_bi().await.expect("client stream");
        write_frame(&mut send, &Frame::Ping { nonce: 42 }, limits)
            .await
            .expect("client write ping");
        assert_eq!(connection.congestion_metrics().pending_bytes, 0);
        assert!(!connection.is_closed());
        finish_stream(&mut send).expect("client finish stream");
        let response = timeout(Duration::from_secs(5), read_frame(&mut recv, limits))
            .await
            .expect("response timeout")
            .expect("client read pong");
        assert_eq!(response, Frame::Pong { nonce: 42 });
        let _ = client_done_tx.send(());

        server_task.await.expect("server task");
    }

    #[tokio::test]
    async fn quic_capacity_probe_dedicated_writer_round_trips_declared_train() {
        let secret = b"0123456789abcdef0123456789abcdef";
        let limits = CodecLimits::default();
        let mux_limits = MuxLimits::default();
        let path_id = PathId(3);
        let token = 0xabc_u64;
        let train_payload_bytes = 96 * 1024_u64;
        let server = Endpoint::bind_server(
            "127.0.0.1:0".parse().expect("server addr"),
            secret,
            mux_limits,
        )
        .await
        .expect("server endpoint");
        let server_addr = server.local_addr().expect("server local addr");
        let (receipt_consumed_tx, receipt_consumed_rx) = tokio::sync::oneshot::channel();
        let server_task = tokio::spawn(async move {
            let connection = server.accept().await.expect("accepted connection");
            let (mut send, mut recv) = connection.accept_bi().await.expect("accepted stream");
            assert_eq!(
                read_frame(&mut recv, limits).await.expect("read opener"),
                Frame::Ping { nonce: 1 }
            );
            write_frame(&mut send, &Frame::Pong { nonce: 1 }, limits)
                .await
                .expect("write opener response");
            let mut received = 0_u64;
            while received < train_payload_bytes {
                let frame = read_frame(&mut recv, limits)
                    .await
                    .expect("read capacity frame");
                let Frame::PathCapacityData {
                    path_id: received_path_id,
                    calibration_id,
                    payload,
                } = frame
                else {
                    panic!("dedicated capacity writer emitted a product frame");
                };
                assert_eq!(received_path_id, path_id);
                assert_eq!(calibration_id, token);
                received = received.saturating_add(payload.len() as u64);
            }
            assert_eq!(received, train_payload_bytes);
            assert_eq!(
                read_frame(&mut recv, limits)
                    .await
                    .expect("read capacity finish"),
                Frame::PathCapacityFinish {
                    path_id,
                    calibration_id: token,
                    payload_bytes: train_payload_bytes,
                }
            );
            write_capacity_receipt(&mut send, path_id, token, received, limits)
                .await
                .expect("write capacity receipt");
            // `write_all` queues into Quinn; retain the connection until the
            // peer consumes the receipt so endpoint teardown cannot overtake it.
            let _ = timeout(Duration::from_secs(5), receipt_consumed_rx).await;
        });

        let client = Endpoint::bind_client(
            "127.0.0.1:0".parse().expect("client addr"),
            secret,
            mux_limits,
        )
        .await
        .expect("client endpoint");
        let connection = client.connect(server_addr).await.expect("client connect");
        let (mut send, mut recv) = connection.open_bi().await.expect("client stream");
        write_frame(&mut send, &Frame::Ping { nonce: 1 }, limits)
            .await
            .expect("write opener");
        assert_eq!(
            read_frame(&mut recv, limits)
                .await
                .expect("read opener response"),
            Frame::Pong { nonce: 1 }
        );
        // Quinn reports ACK-only datagrams through Controller::on_sent without
        // a matching on_ack, so native BIF is not an idle barrier. The exact
        // token receipt owns completion; this test exercises provisional I/O.
        write_capacity_probe(
            &mut send,
            path_id,
            CapacityProbeSpec {
                token,
                train_payload_bytes,
                sample_floor_bytes: 64 * 1024,
                warmup_carrier_bytes: 32 * 1024,
                required_timed_carrier_bytes: 32 * 1024,
                expires_at: Instant::now() + Duration::from_secs(5),
                proof_validity: Duration::from_secs(1),
            },
            16 * 1024,
            limits,
        )
        .await
        .expect("write dedicated capacity train");
        let metrics = connection.congestion_metrics();
        let probe = metrics.capacity_probe.expect("installed capacity epoch");
        assert_eq!(probe.token, token);
        assert_eq!(probe.written_payload_bytes, train_payload_bytes);
        assert_eq!(probe.written_data_frame_count, 6);
        assert!(probe.write_committed);
        assert_eq!(metrics.delivery_evidence_written_bytes, 0);
        assert!(send.encode_buffer.capacity() < train_payload_bytes as usize);

        let receipt = timeout(Duration::from_secs(5), read_frame(&mut recv, limits))
            .await
            .expect("capacity receipt timeout")
            .expect("read capacity receipt");
        assert_eq!(
            receipt,
            Frame::PathCapacityReceipt {
                path_id,
                calibration_id: token,
                received_payload_bytes: train_payload_bytes,
            }
        );
        assert!(connection.confirm_capacity_probe_receipt(
            token,
            train_payload_bytes,
            Instant::now(),
        ));
        timeout(Duration::from_secs(5), async {
            loop {
                if connection
                    .congestion_metrics()
                    .capacity_probe
                    .is_some_and(|probe| probe.phase == CapacityProbePhase::Proven)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("capacity receipt did not release carrier gate");
        let _ = receipt_consumed_tx.send(());

        timeout(Duration::from_secs(5), server_task)
            .await
            .expect("capacity receiver timeout")
            .expect("capacity receiver task");
    }

    #[tokio::test]
    async fn stopped_quic_write_fail_closes_and_releases_backlog() {
        let secret = b"0123456789abcdef0123456789abcdef";
        let limits = CodecLimits::default();
        let mux_limits = MuxLimits::default();
        let stop_code = VarInt::from_u32(37);
        let server = Endpoint::bind_server(
            "127.0.0.1:0".parse().expect("server addr"),
            secret,
            mux_limits,
        )
        .await
        .expect("server endpoint");
        let server_addr = server.local_addr().expect("server local addr");
        let (client_done_tx, client_done_rx) = tokio::sync::oneshot::channel();
        let server_task = tokio::spawn(async move {
            let connection = server.accept().await.expect("accepted connection");
            let (_send, mut recv) = connection.accept_bi().await.expect("accepted stream");
            assert_eq!(
                read_frame(&mut recv, limits).await.expect("read opener"),
                Frame::Ping { nonce: 1 }
            );
            recv.stream.stop(stop_code).expect("stop client writer");
            let _ = timeout(Duration::from_secs(5), client_done_rx).await;
        });

        let client = Endpoint::bind_client(
            "127.0.0.1:0".parse().expect("client addr"),
            secret,
            mux_limits,
        )
        .await
        .expect("client endpoint");
        let connection = client.connect(server_addr).await.expect("client connect");
        let (mut send, _recv) = connection.open_bi().await.expect("client stream");
        write_frame(&mut send, &Frame::Ping { nonce: 1 }, limits)
            .await
            .expect("open carrier stream");
        assert_eq!(connection.congestion_metrics().pending_bytes, 0);
        assert!(!connection.is_closed());

        assert_eq!(
            timeout(Duration::from_secs(5), send.stream.stopped())
                .await
                .expect("STOP_SENDING timeout")
                .expect("connection remains available"),
            Some(stop_code)
        );
        let payload = Bytes::from_static(b"monotonic delivery evidence");
        let payload_len = payload.len() as u64;
        let err = write_frame(
            &mut send,
            &Frame::StreamData {
                stream_id: StreamId(9),
                offset: 0,
                flags: StreamFlags::NONE,
                payload,
            },
            limits,
        )
        .await
        .expect_err("stopped stream write must fail");

        assert!(matches!(
            err,
            QuicCarrierError::Write(quinn::WriteError::Stopped(code)) if code == stop_code
        ));
        let metrics = connection.congestion_metrics();
        assert_eq!(metrics.pending_bytes, 0);
        assert_eq!(metrics.delivery_evidence_written_bytes, payload_len);
        assert!(connection.is_closed());

        let _ = client_done_tx.send(());
        server_task.await.expect("server task");
    }

    #[tokio::test]
    async fn cancelled_quic_write_fail_closes_and_releases_backlog() {
        let secret = b"0123456789abcdef0123456789abcdef";
        let limits = CodecLimits::default();
        let mux_limits = MuxLimits {
            max_stream_window_bytes: 4 * 1024,
            max_repair_bytes: 4 * 1024,
            max_reorder_bytes: 4 * 1024,
            max_datagram_queue_bytes: 4 * 1024,
            max_path_flight_bytes: 4 * 1024,
            max_reliable_relay_chunk_bytes: 4 * 1024,
            ..MuxLimits::default()
        };
        let server = Endpoint::bind_server(
            "127.0.0.1:0".parse().expect("server addr"),
            secret,
            mux_limits,
        )
        .await
        .expect("server endpoint");
        let server_addr = server.local_addr().expect("server local addr");
        let (server_ready_tx, server_ready_rx) = tokio::sync::oneshot::channel();
        let (client_done_tx, client_done_rx) = tokio::sync::oneshot::channel();
        let server_task = tokio::spawn(async move {
            let connection = server.accept().await.expect("accepted connection");
            let (_send, mut recv) = connection.accept_bi().await.expect("accepted stream");
            assert_eq!(
                read_frame(&mut recv, limits).await.expect("read opener"),
                Frame::Ping { nonce: 1 }
            );
            let _ = server_ready_tx.send(());
            let _recv = recv;
            let _ = timeout(Duration::from_secs(5), client_done_rx).await;
        });

        let client = Endpoint::bind_client(
            "127.0.0.1:0".parse().expect("client addr"),
            secret,
            mux_limits,
        )
        .await
        .expect("client endpoint");
        let connection = client.connect(server_addr).await.expect("client connect");
        let (mut send, _recv) = connection.open_bi().await.expect("client stream");
        write_frame(&mut send, &Frame::Ping { nonce: 1 }, limits)
            .await
            .expect("open carrier stream");
        timeout(Duration::from_secs(5), server_ready_rx)
            .await
            .expect("server ready timeout")
            .expect("server ready sender");

        let payload_len = 256 * 1024;
        let write_task = tokio::spawn(async move {
            write_frame(
                &mut send,
                &Frame::StreamData {
                    stream_id: StreamId(9),
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from(vec![0x5a; payload_len]),
                },
                limits,
            )
            .await
        });
        timeout(Duration::from_secs(5), async {
            loop {
                if connection.congestion_metrics().pending_bytes > 0 {
                    break;
                }
                assert!(!write_task.is_finished(), "constrained write must block");
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("write did not enter backlog");

        write_task.abort();
        assert!(
            write_task
                .await
                .expect_err("aborted writer must be cancelled")
                .is_cancelled()
        );
        let metrics = connection.congestion_metrics();
        assert_eq!(metrics.pending_bytes, 0);
        assert_eq!(metrics.delivery_evidence_written_bytes, payload_len as u64);
        assert!(connection.is_closed());

        let _ = client_done_tx.send(());
        server_task.await.expect("server task");
    }

    #[tokio::test]
    async fn quic_carrier_batches_multiple_product_frames_per_write() {
        let secret = b"0123456789abcdef0123456789abcdef";
        let limits = CodecLimits::default();
        let mux_limits = MuxLimits::default();
        let server = Endpoint::bind_server(
            "127.0.0.1:0".parse().expect("server addr"),
            secret,
            mux_limits,
        )
        .await
        .expect("server endpoint");
        let server_addr = server.local_addr().expect("server local addr");
        let server_task = tokio::spawn(async move {
            let connection = server.accept().await.expect("accepted connection");
            let (_send, mut recv) = connection.accept_bi().await.expect("accepted stream");
            assert_eq!(
                read_frame(&mut recv, limits).await.expect("read first"),
                Frame::Ping { nonce: 1 }
            );
            assert_eq!(
                read_frame(&mut recv, limits).await.expect("read second"),
                Frame::Pong { nonce: 2 }
            );
        });

        let client = Endpoint::bind_client(
            "127.0.0.1:0".parse().expect("client addr"),
            secret,
            mux_limits,
        )
        .await
        .expect("client endpoint");
        let connection = client.connect(server_addr).await.expect("client connect");
        let (mut send, _recv) = connection.open_bi().await.expect("client stream");
        write_frames(
            &mut send,
            &[Frame::Ping { nonce: 1 }, Frame::Pong { nonce: 2 }],
            limits,
        )
        .await
        .expect("client write batch");
        finish_stream(&mut send).expect("client finish stream");
        timeout(Duration::from_secs(5), server_task)
            .await
            .expect("server task timeout")
            .expect("server task");
    }

    #[tokio::test]
    async fn quic_carrier_rejects_wrong_shared_secret_before_product_frames() {
        let server_secret = b"0123456789abcdef0123456789abcdef";
        let wrong_client_secret = b"fedcba9876543210fedcba9876543210";
        let good_client_secret = server_secret;
        let mux_limits = MuxLimits::default();
        let server = Endpoint::bind_server(
            "127.0.0.1:0".parse().expect("server addr"),
            server_secret,
            mux_limits,
        )
        .await
        .expect("server endpoint");
        let server_addr = server.local_addr().expect("server local addr");
        let server_task = tokio::spawn(async move {
            timeout(Duration::from_secs(5), server.accept())
                .await
                .expect("server accept timeout")
                .expect("server should accept the later valid client");
        });

        let client = Endpoint::bind_client(
            "127.0.0.1:0".parse().expect("client addr"),
            wrong_client_secret,
            mux_limits,
        )
        .await
        .expect("client endpoint");
        let err = timeout(Duration::from_secs(5), client.connect(server_addr))
            .await
            .expect("connect timeout")
            .expect_err("wrong secret must fail QUIC authentication");
        match err {
            QuicCarrierError::Connection(_) => {}
            err => panic!("unexpected QUIC wrong-secret error: {err:?}"),
        }

        let good_client = Endpoint::bind_client(
            "127.0.0.1:0".parse().expect("good client addr"),
            good_client_secret,
            mux_limits,
        )
        .await
        .expect("good client endpoint");
        timeout(Duration::from_secs(5), good_client.connect(server_addr))
            .await
            .expect("good connect timeout")
            .expect("valid client should connect after failed handshake");

        server_task.await.expect("server task");
    }

    #[test]
    fn quic_transport_profile_follows_mux_resource_envelope() {
        let mux_limits = MuxLimits::default();
        let transport = quic_transport_config(mux_limits);
        let rendered = format!("{transport:?}");
        let stream_window = mux_limits.max_stream_window_bytes;
        let receive_window = stream_window
            + mux_limits.max_repair_bytes as u64
            + mux_limits.max_reorder_bytes as u64
            + mux_limits.max_datagram_queue_bytes as u64
            + mux_limits.max_path_flight_bytes as u64;
        let send_window = mux_limits.max_path_flight_bytes as u64;
        let bidi_streams = mux_limits.max_quic_concurrent_bidi_streams;
        assert!(rendered.contains(&format!("stream_receive_window: {stream_window}")));
        assert!(rendered.contains(&format!("receive_window: {receive_window}")));
        assert!(rendered.contains(&format!("send_window: {send_window}")));
        assert!(rendered.contains(&format!("max_concurrent_bidi_streams: {bidi_streams}")));
        assert!(rendered.contains("max_concurrent_uni_streams: 0"));
    }

    #[test]
    fn quic_stream_limit_is_independent_from_receive_window_ratio() {
        let mux_limits = MuxLimits {
            max_stream_window_bytes: 64 * 1024 * 1024,
            max_repair_bytes: 64 * 1024 * 1024,
            max_reorder_bytes: 64 * 1024 * 1024,
            max_datagram_queue_bytes: 16 * 1024 * 1024,
            max_path_flight_bytes: 64 * 1024 * 1024,
            max_streams: 65_536,
            max_quic_concurrent_bidi_streams: 4096,
            ..MuxLimits::default()
        };
        let transport = quic_transport_config(mux_limits);
        let rendered = format!("{transport:?}");

        assert!(rendered.contains("max_concurrent_bidi_streams: 4096"));
        assert!(!rendered.contains("max_concurrent_bidi_streams: 4,"));
    }
}
