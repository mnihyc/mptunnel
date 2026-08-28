//! Shared QUIC estimator test fixtures.

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
        pacing_rate_bps,
        loss_ppm: None,
        lost_bytes: 0,
        ecn_ppm: None,
        newly_acked_bytes: None,
        non_app_limited_acked_bytes: None,
        timed_non_app_limited_acked_bytes: None,
        non_app_limited_ack_elapsed: None,
        timed_non_app_limited_delivery_evidence_acked_bytes: 0,
        timed_non_app_limited_delivery_evidence_sample_count: 0,
        timed_non_app_limited_delivery_evidence_elapsed: Duration::ZERO,
        delivery_evidence_written_bytes: 0,
        delivery_evidence_cancelled_bytes: 0,
        delivery_evidence_pending_ack_bytes: 0,
        delivery_evidence_newly_acked_bytes: None,
        delivery_sample_count: 0,
        non_app_limited_delivery_sample_count: 0,
        timed_non_app_limited_delivery_sample_count: 0,
        app_limited: true,
    }
}

pub(super) fn with_delivery_evidence_cancelled(
    mut metrics: quic_transport::CongestionMetrics,
    bytes: u64,
) -> quic_transport::CongestionMetrics {
    metrics.delivery_evidence_cancelled_bytes = bytes;
    metrics.delivery_evidence_pending_ack_bytes = metrics
        .delivery_evidence_written_bytes
        .saturating_sub(bytes)
        .saturating_sub(metrics.delivery_evidence_newly_acked_bytes.unwrap_or(0));
    metrics
}

pub(super) fn with_delivery_evidence_written(
    mut metrics: quic_transport::CongestionMetrics,
    bytes: u64,
) -> quic_transport::CongestionMetrics {
    let previously_acked_bytes = metrics
        .delivery_evidence_written_bytes
        .saturating_sub(metrics.delivery_evidence_cancelled_bytes)
        .saturating_sub(metrics.delivery_evidence_pending_ack_bytes);
    metrics.delivery_evidence_written_bytes = bytes;
    metrics.delivery_evidence_pending_ack_bytes = bytes
        .saturating_sub(metrics.delivery_evidence_cancelled_bytes)
        .saturating_sub(previously_acked_bytes);
    metrics.newly_acked_bytes = None;
    metrics.non_app_limited_acked_bytes = None;
    metrics.delivery_evidence_newly_acked_bytes = None;
    metrics.delivery_sample_count = 0;
    metrics.non_app_limited_delivery_sample_count = 0;
    metrics.app_limited = true;
    metrics
}

pub(super) fn with_delivery_clock_epoch(
    mut metrics: quic_transport::CongestionMetrics,
    delivery_clock_epoch: u64,
) -> quic_transport::CongestionMetrics {
    metrics.delivery_clock_epoch = delivery_clock_epoch;
    metrics.timed_non_app_limited_acked_bytes = None;
    metrics.non_app_limited_ack_elapsed = None;
    metrics.timed_non_app_limited_delivery_evidence_acked_bytes = 0;
    metrics.timed_non_app_limited_delivery_evidence_sample_count = 0;
    metrics.timed_non_app_limited_delivery_evidence_elapsed = Duration::ZERO;
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
    metrics.delivery_evidence_newly_acked_bytes = Some(
        bytes.min(
            metrics
                .delivery_evidence_written_bytes
                .saturating_sub(metrics.delivery_evidence_cancelled_bytes),
        ),
    );
    metrics.delivery_evidence_pending_ack_bytes = metrics
        .delivery_evidence_pending_ack_bytes
        .saturating_sub(metrics.delivery_evidence_newly_acked_bytes.unwrap_or(0));
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
        let product_acked_bytes = metrics.delivery_evidence_newly_acked_bytes.unwrap_or(0);
        if product_acked_bytes > 0 {
            metrics.timed_non_app_limited_delivery_evidence_acked_bytes = metrics
                .timed_non_app_limited_delivery_evidence_acked_bytes
                .saturating_add(product_acked_bytes);
            metrics.timed_non_app_limited_delivery_evidence_sample_count = metrics
                .timed_non_app_limited_delivery_evidence_sample_count
                .saturating_add(sample_count);
            metrics.timed_non_app_limited_delivery_evidence_elapsed = metrics
                .timed_non_app_limited_delivery_evidence_elapsed
                .saturating_add(elapsed);
        }
    }
    metrics.delivery_sample_count = sample_count;
    metrics.non_app_limited_delivery_sample_count = sample_count;
    if !elapsed.is_zero() {
        metrics.timed_non_app_limited_delivery_sample_count = metrics
            .timed_non_app_limited_delivery_sample_count
            .saturating_add(sample_count);
    }
    metrics.app_limited = false;
    metrics
}
