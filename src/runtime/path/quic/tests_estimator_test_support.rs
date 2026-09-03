//! Shared QUIC native-observer test fixtures.

use crate::transport::quic as quic_transport;
use std::time::Duration;

pub(super) fn quic_congestion(
    congestion_window: u64,
    pacing_rate_bps: Option<u64>,
) -> quic_transport::CongestionMetrics {
    quic_transport::CongestionMetrics {
        path_epoch: 1,
        delivery_clock_epoch: 1,
        congestion_window,
        bytes_in_flight: Some(0),
        pending_bytes: 0,
        bandwidth_estimate_bps: None,
        pacing_rate_bps,
        loss_ppm: None,
        lost_bytes: 0,
        ecn_ppm: None,
        newly_acked_bytes: None,
        non_app_limited_acked_bytes: None,
        timed_non_app_limited_acked_bytes: None,
        non_app_limited_ack_elapsed: None,
        delivery_sample_count: 0,
        non_app_limited_delivery_sample_count: 0,
        timed_non_app_limited_delivery_sample_count: 0,
        app_limited: true,
    }
}

pub(super) fn with_delivery_clock_epoch(
    mut metrics: quic_transport::CongestionMetrics,
    delivery_clock_epoch: u64,
) -> quic_transport::CongestionMetrics {
    metrics.delivery_clock_epoch = delivery_clock_epoch;
    metrics.timed_non_app_limited_acked_bytes = None;
    metrics.non_app_limited_ack_elapsed = None;
    metrics.timed_non_app_limited_delivery_sample_count = 0;
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
    if !elapsed.is_zero() {
        metrics.timed_non_app_limited_acked_bytes = Some(
            metrics
                .timed_non_app_limited_acked_bytes
                .unwrap_or(0)
                .saturating_add(bytes),
        );
        metrics.non_app_limited_ack_elapsed = Some(
            metrics
                .non_app_limited_ack_elapsed
                .unwrap_or_default()
                .saturating_add(elapsed),
        );
        metrics.timed_non_app_limited_delivery_sample_count = metrics
            .timed_non_app_limited_delivery_sample_count
            .saturating_add(sample_count);
    }
    metrics.delivery_sample_count = sample_count;
    metrics.non_app_limited_delivery_sample_count = sample_count;
    metrics.app_limited = false;
    metrics
}
