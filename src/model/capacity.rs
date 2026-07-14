//! Capacity evidence and carrier-shared timing primitives.
//!
//! Typed records and shared geometry belong to the model. Runtime services
//! gather and validate evidence, then apply decisions without owning its shape.

use crate::mux::MuxLimits;
use crate::protocol::UnderlayProtocol;
use crate::scheduler::{FlowLane, PathSnapshot};
use std::time::{Duration, Instant};

pub(crate) const TRANSPORT_MSS_BYTES: usize = 1460;
pub(crate) const UDP_DEFAULT_MTU_PAYLOAD_BYTES: usize = 1200;
pub(crate) const UDP_MIN_MTU_PAYLOAD_BYTES: usize = 512;
pub(crate) const UDP_MAX_MTU_PAYLOAD_BYTES: usize = 65_000;
pub(crate) const RELIABLE_INITIAL_WINDOW_PACKETS: usize = 10;
pub(crate) const QUIC_INITIAL_WINDOW_PACKETS: usize = RELIABLE_INITIAL_WINDOW_PACKETS;
pub(crate) const PATH_OPEN_SCORE_BYTES: usize =
    RELIABLE_INITIAL_WINDOW_PACKETS * TRANSPORT_MSS_BYTES;

// BBR separates pacing quantum from inflight volume. These protocol-shape
// values are shared model geometry, not path- or lab-specific tuning.
pub(crate) const BBR_SEND_QUANTUM_INTERVAL: Duration = Duration::from_millis(1);
pub(crate) const BBR_MAX_SEND_QUANTUM_BYTES: usize = 64 * 1024;
pub(crate) const BBR_MIN_SEND_QUANTUM_PACKETS: usize = 2;
pub(crate) const BBR_MIN_PIPE_CWND_PACKETS: usize = 4;
pub(crate) const BBR_DEFAULT_CWND_GAIN: f64 = 2.0;

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
pub(crate) const CAPACITY_TIMING_SLACK_BYTES: u64 = BBR_MAX_SEND_QUANTUM_BYTES as u64;

/// Immutable evidence for one exact QUIC capacity train.
///
/// The model owns this record's geometry; QUIC runtime code owns how evidence
/// is gathered and validated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QuicCapacityProofCandidate {
    pub(crate) token: u64,
    pub(crate) train_bytes: u64,
    pub(crate) sample_floor_bytes: u64,
    pub(crate) accounting_slack_bytes: u64,
    pub(crate) warmup_bytes: u64,
    pub(crate) required_proof_bytes: u64,
    pub(crate) written_bytes: u64,
    pub(crate) written_data_frame_count: u64,
    pub(crate) receipt_confirmed: bool,
    pub(crate) received_bytes: u64,
    pub(crate) proof_elapsed: Duration,
    pub(crate) rate_bps: u64,
    pub(crate) accepted_at: Instant,
    pub(crate) expires_at: Instant,
    pub(crate) proof_validity: Duration,
}

/// Validates carrier-proof safety without reimplementing sender train policy.
///
/// Warmup plus the strict proof window is the evidence minimum. A sender may
/// append bounded timing guard bytes; the reservation owner separately enforces
/// the session envelope before any carrier command is admitted.
pub(crate) fn valid_quic_capacity_proof_geometry(
    train_bytes: u64,
    sample_floor_bytes: u64,
    accounting_slack_bytes: u64,
    warmup_bytes: u64,
    required_proof_bytes: u64,
) -> bool {
    let expected_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor_bytes / 8);
    let expected_required = sample_floor_bytes.checked_sub(accounting_slack_bytes);
    let minimum_train = warmup_bytes
        .checked_add(required_proof_bytes)
        .map(|bytes| bytes.max(sample_floor_bytes));
    train_bytes > 0
        && sample_floor_bytes > 0
        && required_proof_bytes > 0
        && accounting_slack_bytes == expected_slack
        && expected_required == Some(required_proof_bytes)
        && minimum_train.is_some_and(|minimum| train_bytes >= minimum)
}

/// Converts an exact QUIC train receipt into its bounded integer rate.
///
/// The carrier timer floor prevents sub-granularity timestamps from creating
/// an unstable denominator; callers still own proof freshness and eligibility.
pub(crate) fn quic_capacity_receipt_rate_bps(
    train_bytes: u64,
    proof_elapsed: Duration,
) -> Option<u64> {
    if train_bytes == 0 || proof_elapsed.is_zero() {
        return None;
    }
    let rate = train_bytes as f64 * 8.0 / proof_elapsed.max(QUIC_TIMER_GRANULARITY).as_secs_f64();
    rate.is_finite()
        .then_some(rate.round().max(1.0).min(u64::MAX as f64) as u64)
}

/// Immutable evidence for one exact TCP capacity train.
///
/// The model owns this cross-layer handoff record; TCP runtime code owns
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

pub(crate) fn product_delivery_samples_override_startup_prior(delivery_samples: u32) -> bool {
    delivery_samples >= RELIABLE_INITIAL_WINDOW_PACKETS as u32
}

pub(crate) fn reliable_subflow_startup_sample_limit_bytes(mux_limits: MuxLimits) -> u64 {
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

pub(crate) fn reliable_capacity_calibration_session_limit_bytes(mux_limits: MuxLimits) -> u64 {
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
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> u64 {
    reliable_stream_advertised_window_from_underlay(None, underlay, lane, mux_limits)
}

pub(crate) fn reliable_stream_advertised_window_bytes(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> u64 {
    let underlay = path
        .map(|snapshot| snapshot.underlay)
        .unwrap_or(UnderlayProtocol::Tcp);
    reliable_stream_advertised_window_from_underlay(path, underlay, lane, mux_limits)
}

fn reliable_stream_advertised_window_from_underlay(
    _path: Option<PathSnapshot>,
    underlay: UnderlayProtocol,
    lane: FlowLane,
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
    lane: FlowLane,
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
    let service_floor = BBR_MAX_SEND_QUANTUM_BYTES
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
    lane: FlowLane,
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

    let quantum = bbr_send_quantum_bytes(path, mux_limits);
    let condition =
        reliable_path_quantum_condition_factor(path, reliable_path_product_bdp_bytes(path));
    let target = match lane {
        FlowLane::Control | FlowLane::RealtimeDatagram => {
            bbr_min_send_quantum_bytes(mux_limits).min(quantum)
        }
        FlowLane::Latency => PATH_OPEN_SCORE_BYTES.min(quantum).max(floor),
        FlowLane::Throughput | FlowLane::Background => {
            ((quantum as f64) * condition).ceil() as usize
        }
    };
    let target = if lane.is_bulk() {
        // TCP and QUIC already own packet pacing and congestion below the
        // product sender. Feed a bulk carrier with a bounded BBR quantum so
        // application records do not keep an otherwise healthy path idle.
        target.max(reliable_bulk_carrier_feed_quantum_bytes(mux_limits))
    } else {
        target
    };
    target.clamp(floor, cap)
}

pub(crate) fn adaptive_reliable_relay_chunk_bytes_with_frame_limit(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
    max_frame_payload_bytes: usize,
) -> usize {
    adaptive_reliable_relay_chunk_bytes(path, lane, mux_limits)
        .min(max_frame_payload_bytes)
        .max(1)
}

pub(crate) fn adaptive_reliable_relay_inflight_bytes(
    path: Option<PathSnapshot>,
    lane: FlowLane,
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

    let bdp_bytes = reliable_path_product_bdp_bytes(path);
    let target = bbr_inflight_target_bytes(path, lane, mux_limits)
        * reliable_path_stability_factor(path)
        * reliable_path_backlog_factor(path, bdp_bytes);
    (target.ceil() as usize).clamp(floor, cap)
}

pub(crate) fn reliable_relay_sender_dispatch_budget(
    mux_limits: MuxLimits,
    lane: FlowLane,
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

pub(crate) fn adaptive_reliable_relay_repair_bytes(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> usize {
    let repair_lane = match lane {
        FlowLane::Throughput | FlowLane::Background => FlowLane::Latency,
        other => other,
    };
    adaptive_reliable_relay_chunk_bytes(path, repair_lane, mux_limits).max(1)
}

fn reliable_path_product_bdp_bytes(path: PathSnapshot) -> f64 {
    let rate_bps = path.delivery_rate_bps.max(
        path.product_progress_rate_bps
            .unwrap_or(path.delivery_rate_bps),
    );
    let rate_bps = rate_bps.max(1.0);
    (rate_bps / 8.0) * (path.srtt_ms.max(1.0) / 1000.0)
}

fn bbr_min_send_quantum_bytes(mux_limits: MuxLimits) -> usize {
    let cap = reliable_relay_buffer_len(mux_limits).max(1);
    (BBR_MIN_SEND_QUANTUM_PACKETS * TRANSPORT_MSS_BYTES)
        .min(cap)
        .max(1)
}

pub(crate) fn reliable_bulk_carrier_feed_quantum_bytes(mux_limits: MuxLimits) -> usize {
    let cap = reliable_relay_buffer_len(mux_limits).max(1);
    BBR_MAX_SEND_QUANTUM_BYTES
        .min(cap)
        .max(bbr_min_send_quantum_bytes(mux_limits))
}

fn bbr_min_pipe_cwnd_bytes(mux_limits: MuxLimits) -> usize {
    let cap = mux_limits.max_path_flight_bytes.max(1);
    (BBR_MIN_PIPE_CWND_PACKETS * TRANSPORT_MSS_BYTES)
        .min(cap)
        .max(1)
}

fn bbr_send_quantum_bytes(path: PathSnapshot, mux_limits: MuxLimits) -> usize {
    let cap = reliable_relay_buffer_len(mux_limits).max(1);
    let floor = bbr_min_send_quantum_bytes(mux_limits);
    let ceiling = BBR_MAX_SEND_QUANTUM_BYTES.min(cap).max(floor);
    let rate_bps = path
        .pacing_rate_bps
        .max(path.delivery_rate_bps)
        .max(path.product_progress_rate_bps.unwrap_or(0.0))
        .max(1.0);
    let quantum = (rate_bps / 8.0 * BBR_SEND_QUANTUM_INTERVAL.as_secs_f64()).ceil() as usize;
    quantum.clamp(floor, ceiling)
}

fn relay_lane_min_chunk_bytes(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> usize {
    let cap = reliable_relay_buffer_len(mux_limits).max(1);
    let min_quantum = bbr_min_send_quantum_bytes(mux_limits);
    match lane {
        FlowLane::Control | FlowLane::RealtimeDatagram => min_quantum,
        FlowLane::Latency => PATH_OPEN_SCORE_BYTES.min(cap).max(min_quantum),
        FlowLane::Throughput | FlowLane::Background if path.is_none() => {
            PATH_OPEN_SCORE_BYTES.min(cap).max(min_quantum)
        }
        FlowLane::Throughput | FlowLane::Background => min_quantum,
    }
}

pub(crate) fn relay_lane_startup_chunk_bytes(lane: FlowLane, mux_limits: MuxLimits) -> usize {
    let cap = reliable_relay_scheduler_quantum_cap(None, lane, mux_limits);
    let floor = relay_lane_min_chunk_bytes(None, lane, mux_limits);
    let target = match lane {
        FlowLane::Control | FlowLane::RealtimeDatagram => bbr_min_send_quantum_bytes(mux_limits),
        FlowLane::Latency => PATH_OPEN_SCORE_BYTES,
        FlowLane::Throughput | FlowLane::Background => reliable_startup_send_quantum_bytes(),
    };
    target.clamp(floor.min(cap).max(1), cap)
}

pub(crate) fn reliable_relay_scheduler_quantum_cap(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> usize {
    let read_ceiling = reliable_relay_buffer_len(mux_limits).max(1);
    let Some(path) = path else {
        return read_ceiling;
    };
    if lane != FlowLane::Throughput {
        return read_ceiling;
    }
    let bdp = reliable_path_product_bdp_bytes(path);
    let condition_factor = reliable_path_quantum_condition_factor(path, bdp);
    (((read_ceiling as f64) * condition_factor)
        .ceil()
        .max(bbr_min_send_quantum_bytes(mux_limits) as f64) as usize)
        .min(read_ceiling)
        .max(1)
}

fn reliable_lane_min_inflight_bytes(lane: FlowLane, mux_limits: MuxLimits) -> usize {
    let cap = mux_limits.max_path_flight_bytes.max(1);
    let min_pipe = bbr_min_pipe_cwnd_bytes(mux_limits);
    let initial_window = PATH_OPEN_SCORE_BYTES.min(cap).max(min_pipe);
    match lane {
        FlowLane::Control | FlowLane::RealtimeDatagram => min_pipe,
        FlowLane::Latency => initial_window,
        FlowLane::Throughput | FlowLane::Background => reliable_relay_buffer_len(mux_limits)
            .max(initial_window)
            .min(cap)
            .max(1),
    }
}

fn reliable_lane_startup_inflight_bytes(lane: FlowLane, mux_limits: MuxLimits) -> usize {
    let cap = mux_limits.max_path_flight_bytes.max(1);
    let floor = reliable_lane_min_inflight_bytes(lane, mux_limits);
    let target = bbr_inflight_target_bytes_for_model(
        reliable_startup_bdp_bytes(),
        reliable_startup_send_quantum_bytes() as f64,
        bbr_min_pipe_cwnd_bytes(mux_limits) as f64,
        lane,
    );
    (target.ceil() as usize).clamp(floor.min(cap).max(1), cap)
}

pub(crate) fn bbr_inflight_target_bytes(
    path: PathSnapshot,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> f64 {
    let bdp = reliable_path_product_bdp_bytes(path);
    let send_quantum = bbr_send_quantum_bytes(path, mux_limits) as f64;
    let min_pipe = bbr_min_pipe_cwnd_bytes(mux_limits) as f64;
    bbr_inflight_target_bytes_for_model(bdp, send_quantum, min_pipe, lane)
}

fn bbr_inflight_target_bytes_for_model(
    bdp: f64,
    send_quantum: f64,
    min_pipe: f64,
    lane: FlowLane,
) -> f64 {
    let bbr_window = (bdp * BBR_DEFAULT_CWND_GAIN)
        .max(send_quantum)
        .max(min_pipe);
    match lane {
        FlowLane::Control | FlowLane::RealtimeDatagram | FlowLane::Latency => {
            bbr_window.min(send_quantum.max(min_pipe))
        }
        FlowLane::Throughput | FlowLane::Background => bbr_window,
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
    bbr_send_quantum_bytes_for_rate(reliable_startup_rate_bps())
}

fn reliable_path_stability_factor(path: PathSnapshot) -> f64 {
    let bdp_bytes = reliable_path_product_bdp_bytes(path);
    let min_pipe = (BBR_MIN_PIPE_CWND_PACKETS * TRANSPORT_MSS_BYTES) as f64;
    let floor = adaptive_transport_floor_factor(min_pipe, bdp_bytes);
    let loss_factor = (1.0 - path.loss_rate.clamp(0.0, 1.0)).max(floor);
    let srtt = path.srtt_ms.max(1.0);
    let jitter_factor = (srtt / (srtt + path.jitter_ms.max(0.0))).max(floor);
    loss_factor * jitter_factor
}

fn reliable_path_queue_factor(path: PathSnapshot, bdp_bytes: f64) -> f64 {
    let queued = path.queue_bytes.saturating_add(path.bytes_in_flight) as f64;
    let floor = adaptive_transport_floor_factor(
        (BBR_MIN_PIPE_CWND_PACKETS * TRANSPORT_MSS_BYTES) as f64,
        bdp_bytes,
    );
    (bdp_bytes / (bdp_bytes + queued.max(0.0))).max(floor)
}

fn reliable_path_backlog_factor(path: PathSnapshot, bdp_bytes: f64) -> f64 {
    let queued = path.queue_bytes as f64;
    let floor = adaptive_transport_floor_factor(
        bbr_send_quantum_bytes_for_rate(
            path.pacing_rate_bps
                .max(path.delivery_rate_bps)
                .max(path.product_progress_rate_bps.unwrap_or(0.0)),
        ) as f64,
        bdp_bytes,
    );
    (bdp_bytes / (bdp_bytes + queued.max(0.0))).max(floor)
}

fn reliable_path_quantum_condition_factor(path: PathSnapshot, bdp_bytes: f64) -> f64 {
    let stability = reliable_path_stability_factor(path);
    let queue = reliable_path_queue_factor(path, bdp_bytes);
    let floor = adaptive_transport_floor_factor(
        bbr_send_quantum_bytes_for_rate(
            path.pacing_rate_bps
                .max(path.delivery_rate_bps)
                .max(path.product_progress_rate_bps.unwrap_or(0.0)),
        ) as f64,
        bdp_bytes,
    );
    (stability * queue.sqrt()).max(floor).min(1.0)
}

fn bbr_send_quantum_bytes_for_rate(rate_bps: f64) -> usize {
    let floor = BBR_MIN_SEND_QUANTUM_PACKETS * TRANSPORT_MSS_BYTES;
    let quantum =
        (rate_bps.max(1.0) / 8.0 * BBR_SEND_QUANTUM_INTERVAL.as_secs_f64()).ceil() as usize;
    quantum.clamp(floor, BBR_MAX_SEND_QUANTUM_BYTES)
}

fn adaptive_transport_floor_factor(minimum_bytes: f64, bdp_bytes: f64) -> f64 {
    let denominator = bdp_bytes.max(minimum_bytes).max(1.0);
    (minimum_bytes.max(1.0) / denominator).min(1.0)
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

    pub(crate) fn elapsed(self) -> Duration {
        self.elapsed
    }
}

#[cfg(test)]
#[path = "capacity_test.rs"]
mod tests;
