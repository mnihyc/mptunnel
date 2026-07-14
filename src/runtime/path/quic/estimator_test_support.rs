use super::*;

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
        measurement: None,
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

pub(super) fn capacity_probe_metrics(
    token: u64,
    now: Instant,
    warmup_bytes: u64,
    required_bytes: u64,
    timed_bytes: u64,
    timed_count: u64,
    timed_elapsed: Option<Duration>,
) -> quic_transport::MeasurementMetrics {
    let sample_floor_bytes = required_bytes.saturating_add(PATH_OPEN_SCORE_BYTES as u64);
    let train_payload_bytes = warmup_bytes
        .saturating_add(required_bytes)
        .max(sample_floor_bytes);
    let receipt_elapsed = Duration::from_millis(80);
    quic_transport::MeasurementMetrics {
        token,
        train_payload_bytes,
        sample_floor_bytes,
        warmup_carrier_bytes: warmup_bytes,
        required_timed_carrier_bytes: required_bytes,
        expires_at: now + Duration::from_secs(5),
        phase: quic_transport::MeasurementPhase::Complete,
        started_clean: false,
        write_committed: true,
        written_payload_bytes: train_payload_bytes,
        written_data_frame_count: train_payload_bytes.div_ceil(64 * 1024),
        total_acked_carrier_bytes: train_payload_bytes,
        total_ack_sample_count: timed_count.saturating_add(u64::from(warmup_bytes > 0)),
        warmup_acked_carrier_bytes: warmup_bytes,
        warmup_ack_sample_count: u64::from(warmup_bytes > 0),
        measurement_acked_carrier_bytes: train_payload_bytes.saturating_sub(warmup_bytes),
        measurement_ack_sample_count: timed_count,
        timed_measurement_acked_carrier_bytes: timed_bytes,
        timed_measurement_ack_sample_count: timed_count,
        app_limited_acked_carrier_bytes: timed_bytes,
        app_limited_ack_sample_count: timed_count,
        timed_measurement_ack_elapsed: timed_elapsed,
        native_threshold_at: timed_elapsed.map(|_| now),
        confirmed_at: Some(now),
        retention: Duration::from_secs(3),
        receipt_received_payload_bytes: train_payload_bytes,
        receipt_elapsed: Some(receipt_elapsed),
        receipt_rtt: Some(Duration::from_millis(20)),
        receipt_at: Some(now),
        last_authoritative_in_flight: Some(0),
        last_authoritative_in_flight_at: Some(now),
        last_authoritative_sent_watermark: Some(train_payload_bytes),
        receipt_frozen_sent_watermark: Some(train_payload_bytes),
        current_sent_watermark: train_payload_bytes,
    }
}

pub(super) fn with_capacity_probe(
    mut metrics: quic_transport::CongestionMetrics,
    probe: quic_transport::MeasurementMetrics,
) -> quic_transport::CongestionMetrics {
    metrics.measurement = Some(probe);
    metrics
}
