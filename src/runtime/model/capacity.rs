//! Carrier-neutral capacity and timing primitives.
//!
//! These constants describe protocol/model geometry. Runtime services measure
//! paths and apply decisions; they do not own these values.

use std::time::Duration;

pub(in crate::runtime) const TRANSPORT_MSS_BYTES: usize = 1460;
pub(in crate::runtime) const RELIABLE_INITIAL_WINDOW_PACKETS: usize = 10;
pub(in crate::runtime) const QUIC_INITIAL_WINDOW_PACKETS: usize = RELIABLE_INITIAL_WINDOW_PACKETS;
pub(in crate::runtime) const PATH_OPEN_SCORE_BYTES: usize =
    RELIABLE_INITIAL_WINDOW_PACKETS * TRANSPORT_MSS_BYTES;

// BBR separates pacing quantum from inflight volume. These protocol-shape
// values are shared model geometry, not path- or lab-specific tuning.
pub(in crate::runtime) const BBR_SEND_QUANTUM_INTERVAL: Duration = Duration::from_millis(1);
pub(in crate::runtime) const BBR_MAX_SEND_QUANTUM_BYTES: usize = 64 * 1024;
pub(in crate::runtime) const BBR_MIN_SEND_QUANTUM_PACKETS: usize = 2;
pub(in crate::runtime) const BBR_MIN_PIPE_CWND_PACKETS: usize = 4;
pub(in crate::runtime) const BBR_DEFAULT_CWND_GAIN: f64 = 2.0;

pub(in crate::runtime) const TRANSPORT_TIMER_GRANULARITY: Duration = Duration::from_millis(1);
pub(in crate::runtime) const QUIC_TIMER_GRANULARITY: Duration = TRANSPORT_TIMER_GRANULARITY;
pub(in crate::runtime) const RELIABLE_INITIAL_RTT: Duration = Duration::from_millis(333);
pub(in crate::runtime) const QUIC_MAX_ACK_DELAY: Duration = Duration::from_millis(25);
pub(in crate::runtime) const QUIC_PERSISTENT_CONGESTION_THRESHOLD: u32 = 3;
pub(in crate::runtime) const MIN_RATE_SAMPLE_BYTES: u64 = PATH_OPEN_SCORE_BYTES as u64;

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct PathRateSample {
    bytes: u64,
    elapsed: Duration,
}

impl PathRateSample {
    pub(in crate::runtime) fn new(bytes: u64, elapsed: Duration) -> Option<Self> {
        if bytes < MIN_RATE_SAMPLE_BYTES {
            return None;
        }
        Some(Self { bytes, elapsed })
    }

    pub(in crate::runtime) fn rate_bps(self) -> f64 {
        self.bytes as f64 * 8.0 / self.elapsed.max(TRANSPORT_TIMER_GRANULARITY).as_secs_f64()
    }

    pub(in crate::runtime) fn bytes(self) -> u64 {
        self.bytes
    }

    pub(in crate::runtime) fn elapsed(self) -> Duration {
        self.elapsed
    }
}
