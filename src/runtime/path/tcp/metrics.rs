use super::*;
use crate::transport::tcp_telemetry::{TcpTelemetrySnapshot, TcpTelemetrySocket};
use tokio::net::TcpStream;

const TCP_METRIC_MIN_INTERVAL: Duration = Duration::from_millis(5);
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

pub(in crate::runtime) fn request_tcp_capacity_receipt_metrics(
    path_id: PathId,
    received_bytes: u64,
    receipt_rate_bps: u64,
    baseline: Option<PathMetrics>,
    native: Option<PathMetrics>,
) -> PathMetrics {
    // A cold request train may be below the real BDP. Its full receiver receipt
    // is the conservative rate seed; product ACKs replace it after handoff.
    tcp_capacity_receipt_metrics(
        path_id,
        PathMetricDirection::ClientToServer,
        received_bytes,
        receipt_rate_bps,
        baseline,
        native,
        false,
    )
}

pub(in crate::runtime) fn response_tcp_capacity_receipt_metrics(
    path_id: PathId,
    received_bytes: u64,
    receipt_rate_bps: u64,
    baseline: Option<PathMetrics>,
    native: Option<PathMetrics>,
) -> PathMetrics {
    // Response discovery may use bounded same-socket delivery uplift because
    // the server owns both the train and the native sender sample.
    tcp_capacity_receipt_metrics(
        path_id,
        PathMetricDirection::ServerToClient,
        received_bytes,
        receipt_rate_bps,
        baseline,
        native,
        true,
    )
}

fn tcp_capacity_receipt_metrics(
    path_id: PathId,
    direction: PathMetricDirection,
    received_bytes: u64,
    receipt_rate_bps: u64,
    baseline: Option<PathMetrics>,
    native: Option<PathMetrics>,
    native_delivery_may_uplift: bool,
) -> PathMetrics {
    let has_native_shape = native.is_some();
    let native_delivery_rate_bps = native.map_or(0, |metrics| metrics.delivery_rate_bps);
    let native_pacing_rate_bps = native.map_or(0, |metrics| metrics.pacing_rate_bps);
    let mut metrics = native
        .or(baseline)
        .unwrap_or_else(|| portable_tcp_receipt_metrics(path_id, direction));
    let rate_bps = if native_delivery_may_uplift {
        tcp_capacity_authoritative_rate_bps(
            receipt_rate_bps,
            native_delivery_rate_bps,
            native_pacing_rate_bps,
        )
    } else {
        receipt_rate_bps
    }
    .max(1);
    metrics.delivery_rate_bps = rate_bps;
    metrics.pacing_rate_bps = rate_bps;
    metrics.has_ack_derived_data_sample = true;
    metrics.data_sample_count = metrics.data_sample_count.max(1);
    metrics.data_sample_bytes = metrics.data_sample_bytes.max(received_bytes);
    metrics.confidence_ppm = 1_000_000;
    if !has_native_shape {
        // A configured startup prior is not durable delivery evidence. Keep
        // the path app-limited after the short typed receipt proof expires and
        // leave cwnd unknown so receipt-rate BDP, not an initial-window hint,
        // bounds portable high-bandwidth admission.
        metrics.app_limited = true;
        metrics.inflight_limit_bytes = 0;
        metrics.inflight_hi_bytes = 0;
    }
    metrics
}

fn portable_tcp_receipt_metrics(path_id: PathId, direction: PathMetricDirection) -> PathMetrics {
    // This is path shape, not rate evidence. The typed receipt installed by the
    // caller supplies rate while this conservative prior supplies RFC-like RTT
    // and initial-window geometry when the host has no native socket counters.
    let initial_rtt_us = u32::try_from(RELIABLE_INITIAL_RTT.as_micros()).unwrap_or(u32::MAX);
    PathMetrics {
        path_id,
        underlay: UnderlayProtocol::Tcp,
        direction,
        metric_epoch: metric_epoch_now(),
        metric_age_us: 0,
        min_rtt_us: initial_rtt_us,
        srtt_us: initial_rtt_us,
        rttvar_us: initial_rtt_us / 2,
        jitter_us: initial_rtt_us / 2,
        delivery_rate_bps: 1,
        pacing_rate_bps: 1,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: false,
        ecn_observed: false,
        bytes_in_flight: 0,
        queue_bytes: 0,
        inflight_limit_bytes: PATH_OPEN_SCORE_BYTES as u64,
        inflight_hi_bytes: PATH_OPEN_SCORE_BYTES as u64,
        confidence_ppm: 0,
        app_limited: true,
        has_ack_derived_data_sample: false,
        data_sample_count: 0,
        data_sample_bytes: 0,
    }
}

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
mod tests;
