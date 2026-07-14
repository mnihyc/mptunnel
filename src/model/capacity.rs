//! Capacity evidence and carrier-shared timing primitives.
//!
//! Typed records and shared geometry belong to the model. Runtime services
//! gather and validate evidence, then apply decisions without owning its shape.

use crate::mux::MuxLimits;
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
