use super::*;
#[cfg(target_os = "linux")]
use crate::transport::tcp_info::{TcpInfoSnapshot, TcpInfoSocket};
#[cfg(target_os = "linux")]
use std::os::fd::AsFd;

#[cfg(target_os = "linux")]
const TCP_METRIC_MIN_INTERVAL: Duration = Duration::from_millis(5);
#[cfg(target_os = "linux")]
const TCP_METRIC_MAX_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct TcpCapacityProofCandidate {
    pub(in crate::runtime) token: u64,
    pub(in crate::runtime) train_bytes: u64,
    pub(in crate::runtime) received_bytes: u64,
    /// Payload represented by `proof_elapsed`; request TCP uses the full train.
    pub(in crate::runtime) rate_sample_bytes: u64,
    pub(in crate::runtime) proof_elapsed: Duration,
    pub(in crate::runtime) receipt_rate_bps: u64,
    pub(in crate::runtime) rate_bps: u64,
    pub(in crate::runtime) accepted_at: Instant,
    pub(in crate::runtime) expires_at: Instant,
}

pub(in crate::runtime) fn valid_tcp_capacity_proof_candidate_at(
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

pub(in crate::runtime) fn tcp_capacity_receipt_rate_bps(
    sample_bytes: u64,
    elapsed: Duration,
) -> Option<u64> {
    if sample_bytes == 0 || elapsed.is_zero() {
        return None;
    }
    let rate = sample_bytes as f64 * 8.0 / elapsed.max(TRANSPORT_TIMER_GRANULARITY).as_secs_f64();
    rate.is_finite()
        .then_some(rate.round().clamp(1.0, u64::MAX as f64) as u64)
}

pub(in crate::runtime) fn tcp_capacity_proof_validity(metrics: PathMetrics) -> Duration {
    Duration::from_micros(u64::from(metrics.srtt_us.max(1)))
        .saturating_mul(4)
        .clamp(Duration::from_secs(1), Duration::from_secs(5))
}

pub(in crate::runtime) fn tcp_capacity_authoritative_rate_bps(
    receipt_rate_bps: u64,
    delivery_rate_bps: u64,
    _pacing_rate_bps: u64,
) -> u64 {
    // The typed ACK/receipt rate remains the floor. Native ACK delivery may lift
    // it by one BBR cwnd gain; pacing alone still proves no delivery.
    let receipt_uplift = (receipt_rate_bps as f64 * BBR_DEFAULT_CWND_GAIN)
        .ceil()
        .clamp(1.0, u64::MAX as f64) as u64;
    receipt_rate_bps
        .max(delivery_rate_bps.min(receipt_uplift))
        .max(1)
}

/// Exact same-socket sender queues used to establish a receipt-rate baseline.
#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TcpSenderQueueSnapshot {
    pub(super) unacked_packets: u32,
    pub(super) notsent_bytes: u32,
}

#[cfg(target_os = "linux")]
impl TcpSenderQueueSnapshot {
    /// The full typed receiver receipt, not a cumulative TCP ACK, owns rate.
    /// Older unacked control can only delay that receipt; unsent bytes must
    /// drain so the measured train begins at a writer boundary.
    pub(super) fn is_write_queue_drained(self) -> bool {
        self.notsent_bytes == 0
    }
}

#[cfg(target_os = "linux")]
impl From<TcpInfoSnapshot> for TcpSenderQueueSnapshot {
    fn from(snapshot: TcpInfoSnapshot) -> Self {
        Self {
            unacked_packets: snapshot.unacked_packets,
            notsent_bytes: snapshot.notsent_bytes,
        }
    }
}

/// Keeps polling in the carrier task so telemetry cannot outlive its socket or
/// exact registry registration.
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(super) struct TcpMetricPublisher {
    socket: TcpInfoSocket,
    tracker: Option<TcpSenderMetricTracker>,
    next_sample_at: Instant,
}

#[cfg(target_os = "linux")]
impl TcpMetricPublisher {
    pub(super) fn capture(socket: &impl AsFd) -> Option<Self> {
        Some(Self {
            socket: TcpInfoSocket::capture(socket).ok()?,
            tracker: None,
            next_sample_at: Instant::now(),
        })
    }

    /// Starts the cumulative sender epoch after authenticated readiness bytes.
    pub(super) fn begin_epoch(&mut self) {
        self.tracker = self
            .socket
            .snapshot()
            .ok()
            .flatten()
            .map(TcpSenderMetricTracker::new);
        self.next_sample_at = Instant::now();
    }

    /// Queries exact sender queues without advancing periodic metric cadence.
    pub(super) fn sender_queue_snapshot(&self) -> Option<TcpSenderQueueSnapshot> {
        self.socket.snapshot().ok().flatten().map(Into::into)
    }

    pub(super) fn maybe_observe(
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
#[cfg(target_os = "linux")]
pub(super) struct TcpSenderMetricTracker {
    baseline: TcpInfoSnapshot,
}

#[cfg(target_os = "linux")]
impl TcpSenderMetricTracker {
    pub(super) fn new(baseline: TcpInfoSnapshot) -> Self {
        Self { baseline }
    }

    pub(super) fn observe(
        &self,
        path_id: PathId,
        direction: PathMetricDirection,
        current: TcpInfoSnapshot,
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

#[cfg(target_os = "linux")]
fn bytes_per_second_to_bits(rate: u64) -> Option<u64> {
    (rate > 0).then(|| rate.saturating_mul(8))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    fn snapshot() -> TcpInfoSnapshot {
        TcpInfoSnapshot {
            app_limited: true,
            retransmits: 10,
            min_rtt_us: 18_000,
            srtt_us: 20_000,
            rttvar_us: 2_000,
            snd_mss_bytes: 1_460,
            unacked_packets: 2,
            snd_ssthresh_packets: u32::MAX,
            snd_cwnd_packets: 10,
            pacing_rate_bytes_per_second: 25_000_000,
            bytes_acked: 100,
            notsent_bytes: 4_096,
            data_segments_out: 20,
            delivery_rate_bytes_per_second: 12_500_000,
        }
    }

    #[cfg(target_os = "linux")]
    fn sender_queue_snapshot(unacked_packets: u32, notsent_bytes: u32) -> TcpSenderQueueSnapshot {
        TcpSenderQueueSnapshot {
            unacked_packets,
            notsent_bytes,
        }
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn tcp_receipt_baseline_waits_only_for_unsent_bytes() {
        assert!(sender_queue_snapshot(0, 0).is_write_queue_drained());
        assert!(
            sender_queue_snapshot(1, 0).is_write_queue_drained(),
            "prior unacked control can only lengthen a full receiver receipt"
        );
        assert!(!sender_queue_snapshot(0, 1).is_write_queue_drained());
        assert!(!sender_queue_snapshot(1, 1).is_write_queue_drained());
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn native_tcp_metrics_are_post_handshake_and_keep_kernel_units_explicit() {
        let baseline = snapshot();
        let tracker = TcpSenderMetricTracker::new(baseline);
        let current = TcpInfoSnapshot {
            app_limited: false,
            retransmits: 12,
            unacked_packets: 3,
            snd_cwnd_packets: 20,
            bytes_acked: baseline.bytes_acked + 300_000,
            notsent_bytes: 8_192,
            data_segments_out: baseline.data_segments_out + 200,
            ..baseline
        };
        let metrics = tracker.observe(PathId(3), PathMetricDirection::ServerToClient, current);

        assert_eq!(metrics.delivery_rate_bps, 100_000_000);
        assert_eq!(metrics.pacing_rate_bps, 200_000_000);
        assert_eq!(metrics.bytes_in_flight, 3 * 1_460);
        assert_eq!(metrics.inflight_limit_bytes, 20 * 1_460);
        assert_eq!(metrics.inflight_hi_bytes, metrics.inflight_limit_bytes);
        assert_eq!(metrics.queue_bytes, 8_192);
        assert_eq!(metrics.data_sample_count, 0);
        assert_eq!(metrics.data_sample_bytes, 0);
        assert!(!metrics.has_ack_derived_data_sample);
        assert!(metrics.loss_observed);
        assert!(!metrics.app_limited);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn control_only_snapshot_never_claims_ack_derived_data() {
        let baseline = snapshot();
        let metrics = TcpSenderMetricTracker::new(baseline).observe(
            PathId(1),
            PathMetricDirection::ServerToClient,
            baseline,
        );
        assert!(!metrics.has_ack_derived_data_sample);
        assert_eq!(metrics.data_sample_count, 0);
        assert_eq!(metrics.data_sample_bytes, 0);
        assert_eq!(metrics.confidence_ppm, 0);
    }

    #[test]
    fn tcp_capacity_proof_requires_exact_fresh_receipt() {
        let accepted_at = Instant::now();
        let proof = TcpCapacityProofCandidate {
            token: 7,
            train_bytes: 2 * 1024 * 1024,
            received_bytes: 2 * 1024 * 1024,
            rate_sample_bytes: 2 * 1024 * 1024,
            proof_elapsed: Duration::from_millis(400),
            receipt_rate_bps: 40_000_000,
            rate_bps: 80_000_000,
            accepted_at,
            expires_at: accepted_at + Duration::from_secs(1),
        };
        assert!(valid_tcp_capacity_proof_candidate_at(proof, accepted_at));
        assert!(!valid_tcp_capacity_proof_candidate_at(
            TcpCapacityProofCandidate {
                received_bytes: proof.received_bytes - 1,
                ..proof
            },
            accepted_at,
        ));
        assert!(!valid_tcp_capacity_proof_candidate_at(
            proof,
            proof.expires_at
        ));
    }

    #[test]
    fn tcp_capacity_authority_ignores_pacing_and_bounds_delivery_uplift() {
        assert_eq!(
            tcp_capacity_authoritative_rate_bps(10_000_000, 15_000_000, 1_000_000_000),
            15_000_000
        );
        assert_eq!(
            tcp_capacity_authoritative_rate_bps(10_000_000, 1_000_000_000, 2_000_000_000),
            20_000_000
        );
    }
}
