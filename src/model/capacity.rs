//! Capacity evidence and carrier-shared timing primitives.
//!
//! Typed records and shared geometry belong to the model. Runtime services
//! gather and validate evidence, then apply decisions without owning its shape.

use crate::mux::MuxLimits;
use crate::protocol::UnderlayProtocol;
use crate::scheduler::{PathSnapshot, TrafficClass};
use std::time::{Duration, Instant};

pub(crate) const TRANSPORT_MSS_BYTES: usize = 1460;
// Conservative packet-size estimate for UDP scheduling math. Quinn owns the
// live QUIC PMTU and packetization; this is not measured path state.
pub(crate) const UDP_BASELINE_PACKET_PAYLOAD_BYTES: usize = 1200;
pub(crate) const MAX_PRODUCT_DATAGRAM_PAYLOAD_BYTES: usize = 65_000;
pub(crate) const RELIABLE_INITIAL_WINDOW_PACKETS: usize = 10;
pub(crate) const QUIC_INITIAL_WINDOW_PACKETS: usize = RELIABLE_INITIAL_WINDOW_PACKETS;
pub(crate) const PATH_OPEN_SCORE_BYTES: usize =
    RELIABLE_INITIAL_WINDOW_PACKETS * TRANSPORT_MSS_BYTES;

// MPP keeps a bounded service quantum separate from its data-level window.
// Native TCP/QUIC congestion controllers remain authoritative below both.
pub(crate) const RELIABLE_SERVICE_QUANTUM_INTERVAL: Duration = Duration::from_millis(1);
pub(crate) const MAX_RELIABLE_SERVICE_QUANTUM_BYTES: usize = 64 * 1024;
pub(crate) const MIN_RELIABLE_SERVICE_QUANTUM_PACKETS: usize = 2;
pub(crate) const MIN_RELIABLE_PIPE_PACKETS: usize = 4;
pub(crate) const RELIABLE_PIPE_WINDOW_BDPS: f64 = 2.0;

pub(crate) const TRANSPORT_TIMER_GRANULARITY: Duration = Duration::from_millis(1);
pub(crate) const QUIC_TIMER_GRANULARITY: Duration = TRANSPORT_TIMER_GRANULARITY;
// Product datagram feedback is carrier-neutral; these budgets must not change
// just because a QUIC protocol timer is retuned.
pub(crate) const DATAGRAM_FEEDBACK_DELAY_BUDGET: Duration = Duration::from_millis(25);
pub(crate) const DATAGRAM_RESPONSE_DEADLINE_MULTIPLIER: u32 = 3;
pub(crate) const RELIABLE_INITIAL_RTT: Duration = Duration::from_millis(333);
pub(crate) const QUIC_MAX_ACK_DELAY: Duration = Duration::from_millis(25);
pub(crate) const QUIC_PERSISTENT_CONGESTION_THRESHOLD: u32 = 3;
pub(crate) const MIN_RATE_SAMPLE_BYTES: u64 = PATH_OPEN_SCORE_BYTES as u64;
pub(crate) const RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES: u64 = 512 * 1024;
pub(crate) const RELIABLE_UDP_MIN_PRODUCT_WINDOW_BYTES: u64 = 512 * 1024;
pub(crate) const CAPACITY_TIMING_SLACK_BYTES: u64 = MAX_RELIABLE_SERVICE_QUANTUM_BYTES as u64;

/// Immutable evidence for one exact TCP capacity train.
///
/// The model owns this cross-layer measurement evidence; TCP runtime code owns
/// receipt interpretation and native telemetry validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpCapacityProofCandidate {
    pub(crate) token: u64,
    pub(crate) train_bytes: u64,
    pub(crate) received_bytes: u64,
    /// Payload represented by `proof_elapsed`; request TCP uses the full train.
    pub(crate) rate_sample_bytes: u64,
    pub(crate) proof_elapsed: Duration,
    pub(crate) receipt_rate_bps: u64,
    pub(crate) rate_bps: u64,
    pub(crate) accepted_at: Instant,
    pub(crate) expires_at: Instant,
}

/// Validates one exact TCP capacity receipt and its fixed evidence lifetime.
///
/// Both request runtime and response bindings consume this immutable rule;
/// reservation, socket telemetry, and proof publication remain runtime-owned.
pub(crate) fn valid_tcp_capacity_proof_candidate_at(
    proof: TcpCapacityProofCandidate,
    now: Instant,
) -> bool {
    proof.token > 0
        && proof.train_bytes >= PATH_OPEN_SCORE_BYTES as u64
        && proof.received_bytes == proof.train_bytes
        && proof.rate_sample_bytes >= PATH_OPEN_SCORE_BYTES as u64
        && proof.rate_sample_bytes <= proof.train_bytes
        && !proof.proof_elapsed.is_zero()
        && proof.receipt_rate_bps > 0
        && proof.rate_bps >= proof.receipt_rate_bps
        && proof.accepted_at < proof.expires_at
        && now < proof.expires_at
}

pub(crate) fn product_delivery_samples_override_startup_prior(delivery_samples: u32) -> bool {
    delivery_samples >= RELIABLE_INITIAL_WINDOW_PACKETS as u32
}

pub(crate) fn reliable_path_startup_sample_limit_bytes(mux_limits: MuxLimits) -> u64 {
    let configured_envelope = (mux_limits.max_path_flight_bytes as u64)
        .min(mux_limits.max_repair_bytes as u64)
        .min(mux_limits.max_reorder_bytes as u64)
        .min(mux_limits.max_stream_window_bytes)
        .max(1);
    RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES
        .saturating_div(2)
        .max(PATH_OPEN_SCORE_BYTES as u64)
        .min(configured_envelope)
}

/// Bounds unique DSN assignment to a path before Data ACK evidence.
///
/// One bounded startup flight is enough to establish path-local Data ACK
/// evidence. The current frontier owner avoids cross-path reorder penalties,
/// but still uses this floor inside its modeled carrier service window.
pub(crate) fn reliable_unproven_path_startup_flight_limit_bytes(mux_limits: MuxLimits) -> u64 {
    let configured_envelope = (mux_limits.max_path_flight_bytes as u64)
        .min(mux_limits.max_repair_bytes as u64)
        .min(mux_limits.max_reorder_bytes as u64)
        .min(mux_limits.max_stream_window_bytes)
        .max(1);
    RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES
        .max(PATH_OPEN_SCORE_BYTES as u64)
        .min(configured_envelope)
}

pub(crate) fn reliable_capacity_measurement_session_limit_bytes(mux_limits: MuxLimits) -> u64 {
    (mux_limits.max_path_flight_bytes as u64)
        .min(mux_limits.max_repair_bytes as u64)
        .min(mux_limits.max_reorder_bytes as u64)
        .min(mux_limits.max_stream_window_bytes)
        .max(1)
}

/// Maximum product payload represented by one reliable sender read buffer.
///
/// This is resource-envelope geometry, not relay task state. Carrier writers
/// may split it further according to their own framing and pacing limits.
pub(crate) fn reliable_relay_buffer_len(mux_limits: MuxLimits) -> usize {
    mux_limits
        .max_reliable_relay_chunk_bytes
        .min(mux_limits.max_payload_bytes)
        .min(mux_limits.max_path_flight_bytes)
        .max(1)
}

/// Initial product receive authority advertised independently of carrier cwnd.
///
/// TCP and QUIC congestion controllers bound carrier flight below this window.
/// Latency-sensitive QUIC keeps a smaller initial product-memory commitment.
pub(crate) fn reliable_stream_initial_advertised_window_bytes(
    underlay: UnderlayProtocol,
    lane: TrafficClass,
    mux_limits: MuxLimits,
) -> u64 {
    reliable_stream_advertised_window_from_underlay(underlay, lane, mux_limits)
}

pub(crate) fn reliable_stream_advertised_window_bytes(
    path: Option<PathSnapshot>,
    lane: TrafficClass,
    mux_limits: MuxLimits,
) -> u64 {
    let underlay = path
        .map(|snapshot| snapshot.underlay)
        .unwrap_or(UnderlayProtocol::Tcp);
    reliable_stream_advertised_window_from_underlay(underlay, lane, mux_limits)
}

fn reliable_stream_advertised_window_from_underlay(
    underlay: UnderlayProtocol,
    lane: TrafficClass,
    mux_limits: MuxLimits,
) -> u64 {
    let configured = mux_limits.max_stream_window_bytes.max(1);
    if underlay != UnderlayProtocol::Udp || lane.is_bulk() {
        return configured;
    }

    let relay_chunk = reliable_relay_buffer_len(mux_limits) as u64;
    let min_window = RELIABLE_UDP_MIN_PRODUCT_WINDOW_BYTES
        .max(relay_chunk.saturating_mul(4))
        .min(configured);
    RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES
        .max(min_window)
        .min(configured)
}

pub(crate) fn reliable_stream_max_data_update_bytes(
    advertised_window_bytes: u64,
    mux_limits: MuxLimits,
) -> u64 {
    let window_step = advertised_window_bytes.saturating_div(4).max(1);
    let payload_step = reliable_relay_buffer_len(mux_limits) as u64;
    window_step
        .max(payload_step)
        .min(advertised_window_bytes.max(1))
}

pub(crate) fn reliable_stream_ack_update_bytes(
    path: Option<PathSnapshot>,
    lane: TrafficClass,
    mux_limits: MuxLimits,
) -> u64 {
    if !lane.is_bulk() {
        return 1;
    }
    let advertised_window = reliable_stream_advertised_window_bytes(path, lane, mux_limits);
    let resource_ceiling = reliable_stream_max_data_update_bytes(advertised_window, mux_limits)
        .min(
            (mux_limits.max_repair_bytes as u64)
                .saturating_div(4)
                .max(1),
        )
        .max(PATH_OPEN_SCORE_BYTES as u64);
    let service_floor = MAX_RELIABLE_SERVICE_QUANTUM_BYTES
        .min(reliable_relay_buffer_len(mux_limits))
        .max(PATH_OPEN_SCORE_BYTES) as u64;
    let measured_step = path
        .map(|path| {
            (reliable_path_product_bdp_bytes(path) / 2.0)
                .ceil()
                .max(1.0) as u64
        })
        .unwrap_or(service_floor);
    measured_step
        .clamp(service_floor.min(resource_ceiling), resource_ceiling)
        .min(mux_limits.max_repair_bytes as u64)
        .max(PATH_OPEN_SCORE_BYTES as u64)
}

pub(crate) fn adaptive_reliable_relay_chunk_bytes(
    path: Option<PathSnapshot>,
    lane: TrafficClass,
    mux_limits: MuxLimits,
) -> usize {
    let cap = reliable_relay_scheduler_quantum_cap(path, lane, mux_limits);
    let floor = relay_lane_min_chunk_bytes(path, lane, mux_limits)
        .min(cap)
        .max(1);
    let Some(path) = path else {
        let startup = relay_lane_startup_chunk_bytes(lane, mux_limits);
        let startup = if lane.is_bulk() {
            startup.max(reliable_bulk_carrier_feed_quantum_bytes(mux_limits))
        } else {
            startup
        };
        return startup.min(cap).max(floor);
    };

    let quantum = reliable_service_quantum_bytes(path, mux_limits);
    let target = match lane {
        TrafficClass::Control | TrafficClass::RealtimeDatagram => {
            min_reliable_service_quantum_bytes(mux_limits).min(quantum)
        }
        TrafficClass::Latency => PATH_OPEN_SCORE_BYTES.min(quantum).max(floor),
        TrafficClass::Throughput | TrafficClass::Background => quantum,
    };
    let target = if lane.is_bulk() {
        // TCP and QUIC already own packet pacing and congestion below the
        // MPP sender. Feed a bulk carrier with a bounded service quantum so
        // application records do not keep an otherwise healthy path idle.
        target.max(reliable_bulk_carrier_feed_quantum_bytes(mux_limits))
    } else {
        target
    };
    target.clamp(floor, cap)
}

pub(crate) fn adaptive_reliable_relay_chunk_bytes_with_frame_limit(
    path: Option<PathSnapshot>,
    lane: TrafficClass,
    mux_limits: MuxLimits,
    max_frame_payload_bytes: usize,
) -> usize {
    adaptive_reliable_relay_chunk_bytes(path, lane, mux_limits)
        .min(max_frame_payload_bytes)
        .max(1)
}

pub(crate) fn adaptive_reliable_relay_inflight_bytes(
    path: Option<PathSnapshot>,
    lane: TrafficClass,
    mux_limits: MuxLimits,
) -> usize {
    let cap = mux_limits.max_path_flight_bytes.max(1);
    let floor = reliable_lane_min_inflight_bytes(lane, mux_limits)
        .min(cap)
        .max(1);
    let Some(path) = path else {
        return reliable_lane_startup_inflight_bytes(lane, mux_limits)
            .min(cap)
            .max(floor);
    };

    // This is a Data Sequence service window, not another congestion controller
    // or a reservation of native send credit. It bounds unique ordered work
    // accepted above the carrier; TCP/QUIC still enforce actual packet flight,
    // recovery, pacing, and writer backpressure. An app-limited product sample
    // must not clamp a larger native window that is available to drain work.
    let modeled =
        (data_level_service_window_bytes(path, lane, mux_limits).ceil() as usize).clamp(floor, cap);
    if lane.is_bulk()
        && path.underlay == UnderlayProtocol::Tcp
        && path.carrier_inflight_limit_bytes == 0
        && path.has_durable_product_progress
    {
        // Without native TCP send credit, a Data ACK rate is completion
        // evidence but not a congestion window. After one bounded product
        // proof, let the shared stream/reorder windows and socket backpressure
        // govern service instead of feeding an app-limited rate back into a
        // second per-path window.
        return cap;
    }
    if !lane.is_bulk() || path.carrier_inflight_limit_bytes == 0 {
        return modeled;
    }
    let carrier_limit = usize::try_from(path.carrier_inflight_limit_bytes).unwrap_or(usize::MAX);
    let native_feed =
        carrier_limit.saturating_add(reliable_bulk_carrier_feed_quantum_bytes(mux_limits));
    modeled.max(native_feed).min(cap)
}

pub(crate) fn reliable_relay_sender_dispatch_budget(
    mux_limits: MuxLimits,
    lane: TrafficClass,
    adaptive_chunk: usize,
    inflight_limit: usize,
    queue_limit: usize,
) -> (usize, usize) {
    let quantum = adaptive_chunk.max(1);
    if !lane.is_bulk() {
        return (quantum, 1);
    }

    // A sender service may drain one bounded feed window per pass. Carrier
    // queues remain writer pipes; they do not become a second scheduler.
    let service_window = reliable_relay_buffer_len(mux_limits)
        .min(queue_limit.max(1))
        .min(inflight_limit.max(1))
        .max(quantum);
    let items = service_window.div_ceil(quantum).max(1);
    let bytes = quantum
        .saturating_mul(items)
        .min(service_window)
        .max(quantum);
    (bytes, items)
}

pub(crate) fn adaptive_reliable_relay_reinjection_bytes(
    path: Option<PathSnapshot>,
    lane: TrafficClass,
    mux_limits: MuxLimits,
) -> usize {
    let reinjection_lane = match lane {
        TrafficClass::Throughput | TrafficClass::Background => TrafficClass::Latency,
        other => other,
    };
    adaptive_reliable_relay_chunk_bytes(path, reinjection_lane, mux_limits).max(1)
}

fn reliable_path_product_bdp_bytes(path: PathSnapshot) -> f64 {
    let rate_bps = path.delivery_rate_bps.max(
        path.product_progress_rate_bps
            .unwrap_or(path.delivery_rate_bps),
    );
    let rate_bps = rate_bps.max(1.0);
    (rate_bps / 8.0) * (path.srtt_ms.max(1.0) / 1000.0)
}

fn min_reliable_service_quantum_bytes(mux_limits: MuxLimits) -> usize {
    let cap = reliable_relay_buffer_len(mux_limits).max(1);
    (MIN_RELIABLE_SERVICE_QUANTUM_PACKETS * TRANSPORT_MSS_BYTES)
        .min(cap)
        .max(1)
}

pub(crate) fn reliable_bulk_carrier_feed_quantum_bytes(mux_limits: MuxLimits) -> usize {
    let cap = reliable_relay_buffer_len(mux_limits).max(1);
    MAX_RELIABLE_SERVICE_QUANTUM_BYTES
        .min(cap)
        .max(min_reliable_service_quantum_bytes(mux_limits))
}

fn min_reliable_pipe_bytes(mux_limits: MuxLimits) -> usize {
    let cap = mux_limits.max_path_flight_bytes.max(1);
    (MIN_RELIABLE_PIPE_PACKETS * TRANSPORT_MSS_BYTES)
        .min(cap)
        .max(1)
}

fn reliable_service_quantum_bytes(path: PathSnapshot, mux_limits: MuxLimits) -> usize {
    let cap = reliable_relay_buffer_len(mux_limits).max(1);
    let floor = min_reliable_service_quantum_bytes(mux_limits);
    let ceiling = MAX_RELIABLE_SERVICE_QUANTUM_BYTES.min(cap).max(floor);
    let rate_bps = path
        .pacing_rate_bps
        .max(path.delivery_rate_bps)
        .max(path.product_progress_rate_bps.unwrap_or(0.0))
        .max(1.0);
    let quantum =
        (rate_bps / 8.0 * RELIABLE_SERVICE_QUANTUM_INTERVAL.as_secs_f64()).ceil() as usize;
    quantum.clamp(floor, ceiling)
}

fn relay_lane_min_chunk_bytes(
    path: Option<PathSnapshot>,
    lane: TrafficClass,
    mux_limits: MuxLimits,
) -> usize {
    let cap = reliable_relay_buffer_len(mux_limits).max(1);
    let min_quantum = min_reliable_service_quantum_bytes(mux_limits);
    match lane {
        TrafficClass::Control | TrafficClass::RealtimeDatagram => min_quantum,
        TrafficClass::Latency => PATH_OPEN_SCORE_BYTES.min(cap).max(min_quantum),
        TrafficClass::Throughput | TrafficClass::Background if path.is_none() => {
            PATH_OPEN_SCORE_BYTES.min(cap).max(min_quantum)
        }
        TrafficClass::Throughput | TrafficClass::Background => min_quantum,
    }
}

pub(crate) fn relay_lane_startup_chunk_bytes(lane: TrafficClass, mux_limits: MuxLimits) -> usize {
    let cap = reliable_relay_scheduler_quantum_cap(None, lane, mux_limits);
    let floor = relay_lane_min_chunk_bytes(None, lane, mux_limits);
    let target = match lane {
        TrafficClass::Control | TrafficClass::RealtimeDatagram => {
            min_reliable_service_quantum_bytes(mux_limits)
        }
        TrafficClass::Latency => PATH_OPEN_SCORE_BYTES,
        TrafficClass::Throughput | TrafficClass::Background => {
            reliable_startup_send_quantum_bytes()
        }
    };
    target.clamp(floor.min(cap).max(1), cap)
}

pub(crate) fn reliable_relay_scheduler_quantum_cap(
    _path: Option<PathSnapshot>,
    _lane: TrafficClass,
    mux_limits: MuxLimits,
) -> usize {
    // Carrier pressure is enforced by the bounded command queue and native
    // transport credit. Feeding less because that same pressure is observed
    // here creates a second feedback loop and can keep a recovered path idle.
    reliable_relay_buffer_len(mux_limits).max(1)
}

fn reliable_lane_min_inflight_bytes(lane: TrafficClass, mux_limits: MuxLimits) -> usize {
    let cap = mux_limits.max_path_flight_bytes.max(1);
    let min_pipe = min_reliable_pipe_bytes(mux_limits);
    let initial_window = PATH_OPEN_SCORE_BYTES.min(cap).max(min_pipe);
    match lane {
        TrafficClass::Control | TrafficClass::RealtimeDatagram => min_pipe,
        TrafficClass::Latency => initial_window,
        TrafficClass::Throughput | TrafficClass::Background => {
            reliable_relay_buffer_len(mux_limits)
                .max(initial_window)
                .min(cap)
                .max(1)
        }
    }
}

fn reliable_lane_startup_inflight_bytes(lane: TrafficClass, mux_limits: MuxLimits) -> usize {
    let cap = mux_limits.max_path_flight_bytes.max(1);
    let floor = reliable_lane_min_inflight_bytes(lane, mux_limits);
    let target = data_level_service_window_bytes_for_model(
        reliable_startup_bdp_bytes(),
        reliable_startup_send_quantum_bytes() as f64,
        min_reliable_pipe_bytes(mux_limits) as f64,
        lane,
    );
    (target.ceil() as usize).clamp(floor.min(cap).max(1), cap)
}

pub(crate) fn data_level_service_window_bytes(
    path: PathSnapshot,
    lane: TrafficClass,
    mux_limits: MuxLimits,
) -> f64 {
    let bdp = reliable_path_product_bdp_bytes(path);
    let send_quantum = reliable_service_quantum_bytes(path, mux_limits) as f64;
    let min_pipe = min_reliable_pipe_bytes(mux_limits) as f64;
    data_level_service_window_bytes_for_model(bdp, send_quantum, min_pipe, lane)
}

fn data_level_service_window_bytes_for_model(
    bdp: f64,
    send_quantum: f64,
    min_pipe: f64,
    lane: TrafficClass,
) -> f64 {
    let data_level_window = (bdp * RELIABLE_PIPE_WINDOW_BDPS)
        .max(send_quantum)
        .max(min_pipe);
    match lane {
        TrafficClass::Control | TrafficClass::RealtimeDatagram | TrafficClass::Latency => {
            data_level_window.min(send_quantum.max(min_pipe))
        }
        TrafficClass::Throughput | TrafficClass::Background => data_level_window,
    }
}

fn reliable_startup_srtt_ms() -> f64 {
    RELIABLE_INITIAL_RTT.as_secs_f64() * 1000.0
}

fn reliable_startup_rate_bps() -> f64 {
    PATH_OPEN_SCORE_BYTES as f64 * 8.0 / RELIABLE_INITIAL_RTT.as_secs_f64()
}

fn reliable_startup_bdp_bytes() -> f64 {
    reliable_startup_rate_bps() / 8.0 * (reliable_startup_srtt_ms() / 1000.0)
}

fn reliable_startup_send_quantum_bytes() -> usize {
    reliable_service_quantum_bytes_for_rate(reliable_startup_rate_bps())
}

fn reliable_service_quantum_bytes_for_rate(rate_bps: f64) -> usize {
    let floor = MIN_RELIABLE_SERVICE_QUANTUM_PACKETS * TRANSPORT_MSS_BYTES;
    let quantum =
        (rate_bps.max(1.0) / 8.0 * RELIABLE_SERVICE_QUANTUM_INTERVAL.as_secs_f64()).ceil() as usize;
    quantum.clamp(floor, MAX_RELIABLE_SERVICE_QUANTUM_BYTES)
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PathRateSample {
    bytes: u64,
    elapsed: Duration,
}

impl PathRateSample {
    pub(crate) fn new(bytes: u64, elapsed: Duration) -> Option<Self> {
        if bytes < MIN_RATE_SAMPLE_BYTES {
            return None;
        }
        Some(Self { bytes, elapsed })
    }

    pub(crate) fn rate_bps(self) -> f64 {
        self.bytes as f64 * 8.0 / self.elapsed.max(TRANSPORT_TIMER_GRANULARITY).as_secs_f64()
    }

    pub(crate) fn bytes(self) -> u64 {
        self.bytes
    }

    #[cfg(any(test, feature = "lab-diagnostics"))]
    pub(crate) fn elapsed(self) -> Duration {
        self.elapsed
    }
}

#[cfg(test)]
#[path = "capacity_test.rs"]
mod tests;
