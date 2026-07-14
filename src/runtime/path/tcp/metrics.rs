//! Optional native TCP sender telemetry.
//!
//! Polling stays beside the socket lifecycle; receipt proof interpretation
//! lives in `capacity` so portable TCP does not depend on platform counters.

use crate::model::capacity::PATH_OPEN_SCORE_BYTES;
use crate::protocol::{PathId, PathMetricDirection, PathMetrics, UnderlayProtocol};
use crate::runtime::path::model::{metric_epoch_now, ratio_to_ppm};
use crate::transport::tcp_telemetry::{TcpTelemetrySnapshot, TcpTelemetrySocket};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

const TCP_METRIC_MIN_INTERVAL: Duration = Duration::from_millis(5);
const TCP_METRIC_MAX_INTERVAL: Duration = Duration::from_millis(250);

/// Exact same-socket sender queues used to establish a receipt-rate baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct TcpSenderQueueSnapshot {
    pub(in crate::runtime) unacked_packets: u32,
    pub(in crate::runtime) notsent_bytes: u32,
}

impl TcpSenderQueueSnapshot {
    /// The full typed receiver receipt, not a cumulative TCP ACK, owns rate.
    /// Older unacked control can only delay that receipt; unsent bytes must
    /// drain so the measured train begins at a writer boundary.
    pub(in crate::runtime) fn is_write_queue_drained(self) -> bool {
        self.notsent_bytes == 0
    }
}

impl From<TcpTelemetrySnapshot> for TcpSenderQueueSnapshot {
    fn from(snapshot: TcpTelemetrySnapshot) -> Self {
        Self {
            unacked_packets: snapshot.unacked_packets,
            notsent_bytes: snapshot.notsent_bytes,
        }
    }
}

/// Keeps polling in the carrier task so telemetry cannot outlive its socket or
/// exact registry registration.
#[derive(Debug)]
pub(in crate::runtime) struct TcpMetricPublisher {
    socket: TcpTelemetrySocket,
    tracker: Option<TcpSenderMetricTracker>,
    next_sample_at: Instant,
}

impl TcpMetricPublisher {
    pub(in crate::runtime) fn capture(socket: &TcpStream) -> Option<Self> {
        Some(Self {
            socket: TcpTelemetrySocket::capture(socket).ok()?,
            tracker: None,
            next_sample_at: Instant::now(),
        })
    }

    /// Starts the cumulative sender epoch after authenticated readiness bytes.
    pub(in crate::runtime) fn begin_epoch(&mut self) {
        self.tracker = self
            .socket
            .snapshot()
            .ok()
            .flatten()
            .map(TcpSenderMetricTracker::new);
        self.next_sample_at = Instant::now();
    }

    /// Queries exact sender queues without advancing periodic metric cadence.
    pub(in crate::runtime) fn sender_queue_snapshot(&self) -> Option<TcpSenderQueueSnapshot> {
        self.socket.snapshot().ok().flatten().map(Into::into)
    }

    pub(in crate::runtime) fn maybe_observe(
        &mut self,
        path_id: PathId,
        direction: PathMetricDirection,
        force: bool,
    ) -> Option<PathMetrics> {
        let now = Instant::now();
        if !force && now < self.next_sample_at {
            return None;
        }
        let current = self.socket.snapshot().ok().flatten()?;
        let tracker = self.tracker.as_ref()?;
        let metrics = tracker.observe(path_id, direction, current);
        let interval = (Duration::from_micros(u64::from(metrics.srtt_us.max(1))) / 2)
            .clamp(TCP_METRIC_MIN_INTERVAL, TCP_METRIC_MAX_INTERVAL);
        self.next_sample_at = now.checked_add(interval).unwrap_or(now);
        Some(metrics)
    }
}

/// Converts one exact Linux TCP sender into product-neutral carrier evidence.
/// The baseline begins after authentication so handshake bytes never graduate
/// an optional response path.
#[derive(Debug)]
pub(in crate::runtime) struct TcpSenderMetricTracker {
    baseline: TcpTelemetrySnapshot,
}

impl TcpSenderMetricTracker {
    pub(in crate::runtime) fn new(baseline: TcpTelemetrySnapshot) -> Self {
        Self { baseline }
    }

    pub(in crate::runtime) fn observe(
        &self,
        path_id: PathId,
        direction: PathMetricDirection,
        current: TcpTelemetrySnapshot,
    ) -> PathMetrics {
        let acknowledged_bytes = current
            .bytes_acked
            .saturating_sub(self.baseline.bytes_acked);
        let sent_segments = current
            .data_segments_out
            .saturating_sub(self.baseline.data_segments_out);
        let retransmits = current
            .retransmits
            .saturating_sub(self.baseline.retransmits);
        let mss = u64::from(current.snd_mss_bytes.max(1));
        let inflight_limit_bytes = u64::from(current.snd_cwnd_packets).saturating_mul(mss);
        let inflight_hi_bytes = if current.snd_ssthresh_packets == u32::MAX {
            inflight_limit_bytes
        } else {
            u64::from(current.snd_ssthresh_packets).saturating_mul(mss)
        };
        let delivery_rate_bps = bytes_per_second_to_bits(current.delivery_rate_bytes_per_second);
        let pacing_rate_bps = bytes_per_second_to_bits(current.pacing_rate_bytes_per_second)
            .filter(|_| current.pacing_rate_bytes_per_second != u64::MAX)
            .or(delivery_rate_bps)
            .unwrap_or(1);
        let delivery_rate_bps = delivery_rate_bps.unwrap_or(pacing_rate_bps).max(1);
        let loss_ppm = if sent_segments == 0 {
            0
        } else {
            ratio_to_ppm(f64::from(retransmits) / f64::from(sent_segments))
        };
        let sample_floor = inflight_limit_bytes.max(PATH_OPEN_SCORE_BYTES as u64);
        PathMetrics {
            path_id,
            underlay: UnderlayProtocol::Tcp,
            direction,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: current.min_rtt_us,
            srtt_us: current.srtt_us.max(1),
            rttvar_us: current.rttvar_us,
            jitter_us: current.rttvar_us,
            delivery_rate_bps,
            pacing_rate_bps,
            loss_ppm,
            ecn_ppm: 0,
            loss_observed: retransmits > 0,
            ecn_observed: false,
            bytes_in_flight: u64::from(current.unacked_packets).saturating_mul(mss),
            queue_bytes: u64::from(current.notsent_bytes),
            inflight_limit_bytes,
            inflight_hi_bytes,
            confidence_ppm: ratio_to_ppm(acknowledged_bytes as f64 / sample_floor.max(1) as f64),
            app_limited: current.app_limited,
            // TCP_INFO cannot distinguish authenticated liveness/control bytes
            // from product or a typed capacity epoch. Keep those counters out
            // of bulk authority; the receipt handler upgrades exact probe bytes.
            has_ack_derived_data_sample: false,
            data_sample_count: 0,
            data_sample_bytes: 0,
        }
    }
}

fn bytes_per_second_to_bits(rate: u64) -> Option<u64> {
    (rate > 0).then(|| rate.saturating_mul(8))
}

#[cfg(test)]
#[path = "metrics_test.rs"]
mod tests;
