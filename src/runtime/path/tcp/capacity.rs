//! TCP receipt proof interpretation.
//!
//! This owner converts typed receiver receipts and optional native snapshots
//! into capacity evidence. Socket capture and polling remain in `metrics`.

use super::metrics::TcpNativeObservation;
pub(in crate::runtime) use crate::model::capacity::TcpCapacityProofCandidate;
use crate::model::capacity::{
    BBR_DEFAULT_CWND_GAIN, PATH_OPEN_SCORE_BYTES, RELIABLE_INITIAL_RTT, TRANSPORT_TIMER_GRANULARITY,
};
use crate::protocol::{PathId, PathMetricDirection, PathMetrics, UnderlayProtocol};
use crate::runtime::path::model::metric_epoch_now;
use std::time::{Duration, Instant};

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
) -> u64 {
    // The typed receipt rate remains the floor. Native ACK delivery may lift
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
    native: Option<TcpNativeObservation>,
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
    native: Option<TcpNativeObservation>,
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
    native: Option<TcpNativeObservation>,
    native_delivery_may_uplift: bool,
) -> PathMetrics {
    let native_delivery_rate_bps = native.and_then(TcpNativeObservation::delivery_rate_bps);
    let mut metrics = baseline.unwrap_or_else(|| portable_tcp_receipt_metrics(path_id, direction));
    if let Some(native) = native {
        native.apply_transport_shape(&mut metrics);
        metrics.metric_epoch = metric_epoch_now();
        metrics.metric_age_us = 0;
    }
    let rate_bps = if native_delivery_may_uplift {
        tcp_capacity_authoritative_rate_bps(receipt_rate_bps, native_delivery_rate_bps.unwrap_or(0))
    } else {
        receipt_rate_bps
    }
    .max(1);
    metrics.path_id = path_id;
    metrics.underlay = UnderlayProtocol::Tcp;
    metrics.direction = direction;
    metrics.delivery_rate_bps = rate_bps;
    metrics.pacing_rate_bps = rate_bps;
    metrics.has_ack_derived_data_sample = true;
    metrics.data_sample_count = metrics.data_sample_count.max(1);
    metrics.data_sample_bytes = metrics.data_sample_bytes.max(received_bytes);
    metrics.confidence_ppm = 1_000_000;
    metrics.app_limited = native
        .and_then(TcpNativeObservation::app_limited)
        .unwrap_or(true);
    if !native.is_some_and(TcpNativeObservation::has_flight) {
        // A configured startup prior is not native congestion evidence. Keep
        // cwnd unknown so receipt-rate BDP, not an initial-window hint, bounds
        // portable high-bandwidth admission.
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

#[cfg(test)]
#[path = "capacity_test.rs"]
mod tests;
