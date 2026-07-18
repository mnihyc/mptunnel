//! Shared QUIC estimator test fixtures.

use crate::transport::quic as quic_transport;
use std::time::Duration;

pub(super) fn quic_congestion(
    congestion_window: u64,
    pacing_rate_bps: Option<u64>,
) -> quic_transport::CongestionMetrics {
    quic_transport::CongestionMetrics {
        congestion_window,
        bytes_in_flight: Some(0),
        pending_bytes: 0,
        pacing_rate_bps,
        loss_ppm: None,
        lost_bytes: 0,
        ecn_ppm: None,
        newly_acked_bytes: None,
        non_app_limited_acked_bytes: None,
        timed_non_app_limited_acked_bytes: None,
        non_app_limited_ack_elapsed: None,
        delivery_evidence_written_bytes: 0,
        delivery_sample_count: 0,
        non_app_limited_delivery_sample_count: 0,
        timed_non_app_limited_delivery_sample_count: 0,
        app_limited: true,
    }
}

pub(super) fn with_delivery_evidence_written(
    mut metrics: quic_transport::CongestionMetrics,
    bytes: u64,
) -> quic_transport::CongestionMetrics {
    metrics.delivery_evidence_written_bytes = bytes;
    metrics
}

pub(super) fn with_acked_bytes(
    metrics: quic_transport::CongestionMetrics,
    bytes: u64,
    sample_count: u64,
) -> quic_transport::CongestionMetrics {
    with_acked_bytes_elapsed(metrics, bytes, sample_count, Duration::from_millis(100))
}

pub(super) fn with_acked_bytes_elapsed(
    mut metrics: quic_transport::CongestionMetrics,
    bytes: u64,
    sample_count: u64,
    elapsed: Duration,
) -> quic_transport::CongestionMetrics {
    metrics.newly_acked_bytes = Some(bytes);
    metrics.non_app_limited_acked_bytes = Some(bytes);
    metrics.timed_non_app_limited_acked_bytes = (!elapsed.is_zero()).then_some(bytes);
    metrics.non_app_limited_ack_elapsed = (!elapsed.is_zero()).then_some(elapsed);
    metrics.delivery_sample_count = sample_count;
    metrics.non_app_limited_delivery_sample_count = sample_count;
    metrics.timed_non_app_limited_delivery_sample_count =
        if elapsed.is_zero() { 0 } else { sample_count };
    metrics.app_limited = false;
    metrics
}
