use super::estimator::UdpPathMetricTracker;
use super::io::UdpPathConnection;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{
    QUIC_INITIAL_WINDOW_PACKETS, QUIC_MAX_ACK_DELAY, QUIC_TIMER_GRANULARITY,
    QuicCapacityProofCandidate,
};
use crate::model::timing::{default_transport_pto, transport_pto_from_ms};
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::SessionId;
use crate::protocol::{PathId, PathMetricDirection, PathMetrics, UnderlayProtocol};
use crate::runtime::path::ServerCarrierPathRegistration;
use crate::runtime::path::model::{metric_epoch_now, ratio_to_ppm};
use crate::runtime::path::server_context::ServerPathContext;
use crate::transport::quic as quic_transport;
use std::time::{Duration, Instant};

#[cfg(feature = "lab-diagnostics")]
#[derive(Debug, Clone, Copy, Default)]
pub(in crate::runtime) struct QuicAckPollDiagnostics {
    pub(in crate::runtime) newly_acked_bytes: u64,
    pub(in crate::runtime) non_app_limited_acked_bytes: u64,
    pub(in crate::runtime) timed_non_app_limited_acked_bytes: u64,
    pub(in crate::runtime) ack_elapsed: Duration,
    pub(in crate::runtime) delivery_sample_count: u64,
    pub(in crate::runtime) non_app_limited_sample_count: u64,
    pub(in crate::runtime) timed_non_app_limited_sample_count: u64,
    pub(in crate::runtime) carrier_app_limited: bool,
    pub(in crate::runtime) delivery_evidence_written_delta: u64,
    pub(in crate::runtime) delivery_evidence_newly_acked_bytes: u64,
    pub(in crate::runtime) delivery_evidence_pending_ack_bytes: u64,
    pub(in crate::runtime) pending_sample_bytes: u64,
    pub(in crate::runtime) pending_sample_count: u64,
    pub(in crate::runtime) pending_sample_elapsed: Duration,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct UdpPathMetrics {
    pub(in crate::runtime) direction: u8,
    pub(in crate::runtime) srtt: Duration,
    pub(in crate::runtime) rttvar: Duration,
    pub(in crate::runtime) min_rtt: Duration,
    pub(in crate::runtime) min_rtt_observed: bool,
    pub(in crate::runtime) delivery_rate_bps: f64,
    pub(in crate::runtime) pacing_rate_bps: f64,
    pub(in crate::runtime) inflight_hi: usize,
    pub(in crate::runtime) bytes_in_flight: usize,
    pub(in crate::runtime) pending_bytes: usize,
    pub(in crate::runtime) loss_ppm: Option<u32>,
    pub(in crate::runtime) ecn_ppm: Option<u32>,
    pub(in crate::runtime) app_limited: bool,
    pub(in crate::runtime) ack_derived_data_seen: bool,
    pub(in crate::runtime) delivery_sample_count: u64,
    pub(in crate::runtime) delivery_sample_bytes: u64,
    pub(in crate::runtime) last_delivery_sample_at: Option<Instant>,
    #[cfg_attr(not(any(test, feature = "lab-diagnostics")), allow(dead_code))]
    pub(in crate::runtime) bulk_proof_expires_at: Option<Instant>,
    // The latest accepted strict sample is kept separate from cumulative model
    // state so diagnostics can audit its carrier-clock denominator directly.
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) latest_delivery_sample_bytes: u64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) latest_delivery_sample_count: u64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) latest_carrier_ack_elapsed: Option<Duration>,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) latest_rate_sample_elapsed: Option<Duration>,
    pub(in crate::runtime) capacity_proof_candidate: Option<QuicCapacityProofCandidate>,
    pub(in crate::runtime) capacity_probe: Option<quic_transport::MeasurementMetrics>,
    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) ack_poll: QuicAckPollDiagnostics,
}

pub(super) async fn run_server_quic_path_metrics(
    context: ServerPathContext,
    path_registration: ServerCarrierPathRegistration,
    connection: UdpPathConnection,
) {
    let path_id = path_registration.path_id();
    #[cfg(feature = "lab-diagnostics")]
    let path_instance_id = path_registration.path_instance_id();
    #[cfg(feature = "lab-diagnostics")]
    let session_id = path_registration.session_id();
    let mut tracker = UdpPathMetricTracker::default();
    #[cfg(feature = "lab-diagnostics")]
    let mut last_metrics_poll_at = None;
    loop {
        if connection.is_closed() {
            return;
        }
        let Some(mut metrics) = connection.tx_metrics(&mut tracker, 2).await else {
            tokio::time::sleep(default_transport_pto()).await;
            continue;
        };
        #[cfg(feature = "lab-diagnostics")]
        let metrics_poll_at = Instant::now();
        #[cfg(feature = "lab-diagnostics")]
        let poll_elapsed = last_metrics_poll_at
            .replace(metrics_poll_at)
            .map(|previous| metrics_poll_at.saturating_duration_since(previous))
            .unwrap_or_default();
        #[cfg(feature = "lab-diagnostics")]
        log_quic_ack_poll_diagnostics(
            session_id,
            path_id,
            path_instance_id.as_u64(),
            metrics,
            poll_elapsed,
        );

        let capacity_proof_accepted = metrics.capacity_proof_candidate.is_some_and(|candidate| {
                let proof_metrics = path_metrics_from_quic_capacity_proof(
                    path_id,
                    metrics,
                    candidate,
                );
                if context.reliable_streams.record_local_quic_capacity_proof(
                    &path_registration,
                    proof_metrics,
                    candidate,
                ) {
                    tracker.accept_capacity_proof(&mut metrics, candidate);
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "quic_capacity_proof",
                        format_args!(
                            "phase=accepted session_id={} path_id={} path_instance_id={} calibration_id={} train_bytes={} sample_floor_bytes={} warmup_bytes={} required_proof_bytes={} written_data_frame_count={} received_bytes={} proof_elapsed_us={} rate_bps={} proof_validity_ms={}",
                            session_id.0,
                            path_id.0,
                            path_instance_id.as_u64(),
                            candidate.token,
                            candidate.train_bytes,
                            candidate.sample_floor_bytes,
                            candidate.warmup_bytes,
                            candidate.required_proof_bytes,
                            candidate.written_data_frame_count,
                            candidate.received_bytes,
                            candidate.proof_elapsed.as_micros(),
                            candidate.rate_bps,
                            candidate.proof_validity.as_millis(),
                        ),
                    );
                    true
                } else {
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "quic_capacity_proof",
                        format_args!(
                            "phase=rejected session_id={} path_id={} path_instance_id={} calibration_id={} train_bytes={} sample_floor_bytes={} warmup_bytes={} required_proof_bytes={} written_data_frame_count={} received_bytes={} proof_elapsed_us={} rate_bps={}",
                            session_id.0,
                            path_id.0,
                            path_instance_id.as_u64(),
                            candidate.token,
                            candidate.train_bytes,
                            candidate.sample_floor_bytes,
                            candidate.warmup_bytes,
                            candidate.required_proof_bytes,
                            candidate.written_data_frame_count,
                            candidate.received_bytes,
                            candidate.proof_elapsed.as_micros(),
                            candidate.rate_bps,
                        ),
                    );
                    false
                }
            });
        if let Some(token) =
            tracker.terminal_capacity_probe_to_retire(metrics.capacity_probe, Instant::now())
        {
            let _retired = connection.retire_capacity_probe(token);
            tracker.retire_capacity_candidate(token);
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "quic_capacity_probe_retired",
                format_args!(
                    "session_id={} path_id={} path_instance_id={} calibration_id={} proof_accepted={} carrier_retired={}",
                    session_id.0,
                    path_id.0,
                    path_instance_id.as_u64(),
                    token,
                    capacity_proof_accepted,
                    _retired,
                ),
            );
        }
        if quic_path_metrics_should_publish_local_sender(metrics) {
            #[cfg(feature = "lab-diagnostics")]
            if let (Some(carrier_elapsed), Some(rate_elapsed)) = (
                metrics.latest_carrier_ack_elapsed,
                metrics.latest_rate_sample_elapsed,
            ) {
                let raw_rate_bps = (metrics.latest_delivery_sample_bytes as f64 * 8.0
                    / rate_elapsed.as_secs_f64())
                .round() as u64;
                lab_diagnostic(
                    "quic_carrier_rate_sample",
                    format_args!(
                        "session_id={} path_id={} path_instance_id={} direction={} rate_source=quic_send_ack_max sample_bytes={} sample_count={} carrier_elapsed_us={} sample_elapsed_us={} raw_rate_bps={} published_rate_bps={} poll_elapsed_us={} total_sample_count={} total_sample_bytes={} app_limited={}",
                        session_id.0,
                        path_id.0,
                        path_instance_id.as_u64(),
                        metrics.direction,
                        metrics.latest_delivery_sample_bytes,
                        metrics.latest_delivery_sample_count,
                        carrier_elapsed.as_micros(),
                        rate_elapsed.as_micros(),
                        raw_rate_bps,
                        metrics.delivery_rate_bps.round() as u64,
                        poll_elapsed.as_micros(),
                        metrics.delivery_sample_count,
                        metrics.delivery_sample_bytes,
                        metrics.app_limited,
                    ),
                );
            }
            if !capacity_proof_accepted {
                context.reliable_streams.record_local_path_metrics(
                    &path_registration,
                    path_metrics_from_quic_path(path_id, metrics),
                );
            }
        }
        tokio::time::sleep(quic_path_metrics_poll_interval(metrics)).await;
    }
}

fn quic_path_metrics_should_publish_local_sender(metrics: UdpPathMetrics) -> bool {
    metrics.delivery_sample_count > 0 || metrics.ack_derived_data_seen
}

#[cfg(feature = "lab-diagnostics")]
pub(super) fn log_quic_ack_poll_diagnostics(
    session_id: SessionId,
    path_id: PathId,
    path_instance_id: u64,
    metrics: UdpPathMetrics,
    poll_elapsed: Duration,
) {
    let ack = metrics.ack_poll;
    if ack.newly_acked_bytes > 0
        || ack.delivery_evidence_written_delta > 0
        || ack.pending_sample_bytes > 0
        || metrics.capacity_probe.is_some()
    {
        lab_diagnostic(
            "quic_carrier_ack_poll",
            format_args!(
                "session_id={} path_id={} path_instance_id={} direction={} poll_elapsed_us={} newly_acked_bytes={} non_app_limited_acked_bytes={} timed_non_app_limited_acked_bytes={} ack_elapsed_us={} sample_count={} non_app_limited_sample_count={} timed_non_app_limited_sample_count={} carrier_app_limited={} evidence_written_delta={} evidence_newly_acked_bytes={} evidence_pending_ack_bytes={} pending_sample_bytes={} pending_sample_count={} pending_sample_elapsed_us={} proof_expires_in_us={}",
                session_id.0,
                path_id.0,
                path_instance_id,
                metrics.direction,
                poll_elapsed.as_micros(),
                ack.newly_acked_bytes,
                ack.non_app_limited_acked_bytes,
                ack.timed_non_app_limited_acked_bytes,
                ack.ack_elapsed.as_micros(),
                ack.delivery_sample_count,
                ack.non_app_limited_sample_count,
                ack.timed_non_app_limited_sample_count,
                ack.carrier_app_limited,
                ack.delivery_evidence_written_delta,
                ack.delivery_evidence_newly_acked_bytes,
                ack.delivery_evidence_pending_ack_bytes,
                ack.pending_sample_bytes,
                ack.pending_sample_count,
                ack.pending_sample_elapsed.as_micros(),
                metrics
                    .bulk_proof_expires_at
                    .map(|expires_at| expires_at
                        .saturating_duration_since(Instant::now())
                        .as_micros())
                    .unwrap_or(0),
            ),
        );
    }
    if let Some(probe) = metrics.capacity_probe {
        let now = Instant::now();
        lab_diagnostic(
            "quic_capacity_ack_poll",
            format_args!(
                "session_id={} path_id={} path_instance_id={} direction={} calibration_id={} phase={:?} write_committed={} train_bytes={} written_bytes={} written_data_frame_count={} sample_floor_bytes={} warmup_bytes={} required_proof_bytes={} native_started_clean={} native_total_acked_bytes={} native_total_ack_count={} native_warmup_acked_bytes={} native_warmup_ack_count={} native_measurement_acked_bytes={} native_measurement_ack_count={} native_timed_measurement_acked_bytes={} native_timed_measurement_ack_count={} native_app_limited_acked_bytes={} native_app_limited_ack_count={} native_timed_elapsed_us={} native_proved_age_us={} receipt_received_bytes={} receipt_elapsed_us={} receipt_rtt_us={} receipt_age_us={} last_authoritative_bif_bytes={} last_authoritative_bif_age_us={} last_authoritative_sent_watermark={} receipt_frozen_sent_watermark={} current_sent_watermark={} proof_validity_ms={} proved_age_us={} attempt_remaining_us={} candidate_emitted={}",
                session_id.0,
                path_id.0,
                path_instance_id,
                metrics.direction,
                probe.token,
                probe.phase,
                probe.write_committed,
                probe.train_payload_bytes,
                probe.written_payload_bytes,
                probe.written_data_frame_count,
                probe.sample_floor_bytes,
                probe.warmup_carrier_bytes,
                probe.required_timed_carrier_bytes,
                probe.started_clean,
                probe.total_acked_carrier_bytes,
                probe.total_ack_sample_count,
                probe.warmup_acked_carrier_bytes,
                probe.warmup_ack_sample_count,
                probe.measurement_acked_carrier_bytes,
                probe.measurement_ack_sample_count,
                probe.timed_measurement_acked_carrier_bytes,
                probe.timed_measurement_ack_sample_count,
                probe.app_limited_acked_carrier_bytes,
                probe.app_limited_ack_sample_count,
                probe
                    .timed_measurement_ack_elapsed
                    .unwrap_or_default()
                    .as_micros(),
                probe
                    .native_threshold_at
                    .map(|confirmed_at| now.saturating_duration_since(confirmed_at).as_micros())
                    .unwrap_or(0),
                probe.receipt_received_payload_bytes,
                probe.receipt_elapsed.unwrap_or_default().as_micros(),
                probe.receipt_rtt.unwrap_or_default().as_micros(),
                probe
                    .receipt_at
                    .map(|receipt_at| now.saturating_duration_since(receipt_at).as_micros())
                    .unwrap_or(0),
                probe.last_authoritative_in_flight.unwrap_or(0),
                probe
                    .last_authoritative_in_flight_at
                    .map(|observed_at| now.saturating_duration_since(observed_at).as_micros())
                    .unwrap_or(0),
                probe.last_authoritative_sent_watermark.unwrap_or(0),
                probe.receipt_frozen_sent_watermark.unwrap_or(0),
                probe.current_sent_watermark,
                probe.retention.as_millis(),
                probe
                    .confirmed_at
                    .map(|confirmed_at| now.saturating_duration_since(confirmed_at).as_micros())
                    .unwrap_or(0),
                probe.expires_at.saturating_duration_since(now).as_micros(),
                metrics.capacity_proof_candidate.is_some(),
            ),
        );
    }
}

fn path_metrics_from_quic_path(path_id: PathId, metrics: UdpPathMetrics) -> PathMetrics {
    PathMetrics {
        path_id,
        underlay: UnderlayProtocol::Udp,
        direction: match metrics.direction {
            1 => PathMetricDirection::ClientToServer,
            2 => PathMetricDirection::ServerToClient,
            _ => PathMetricDirection::ServerToClient,
        },
        metric_epoch: metric_epoch_now(),
        metric_age_us: metrics
            .last_delivery_sample_at
            .map(|seen| {
                let micros = Instant::now().saturating_duration_since(seen).as_micros();
                u32::try_from(micros).unwrap_or(u32::MAX)
            })
            .unwrap_or(0),
        min_rtt_us: duration_to_micros_u32(metrics.min_rtt),
        srtt_us: duration_to_micros_u32(metrics.srtt),
        rttvar_us: duration_to_micros_u32(metrics.rttvar),
        jitter_us: duration_to_micros_u32(metrics.rttvar),
        delivery_rate_bps: metrics.delivery_rate_bps.max(1.0).round() as u64,
        pacing_rate_bps: metrics.pacing_rate_bps.max(1.0).round() as u64,
        loss_ppm: metrics.loss_ppm.unwrap_or(0),
        ecn_ppm: metrics.ecn_ppm.unwrap_or(0),
        loss_observed: metrics.loss_ppm.is_some(),
        ecn_observed: metrics.ecn_ppm.is_some(),
        bytes_in_flight: metrics.bytes_in_flight as u64,
        queue_bytes: metrics
            .pending_bytes
            .saturating_sub(metrics.bytes_in_flight) as u64,
        inflight_limit_bytes: metrics.inflight_hi as u64,
        inflight_hi_bytes: metrics.inflight_hi as u64,
        confidence_ppm: ratio_to_ppm(
            (metrics.delivery_sample_count as f64 / QUIC_INITIAL_WINDOW_PACKETS as f64)
                .clamp(0.0, 1.0),
        ),
        app_limited: metrics.app_limited,
        has_ack_derived_data_sample: metrics.ack_derived_data_seen,
        data_sample_count: u32::try_from(metrics.delivery_sample_count).unwrap_or(u32::MAX),
        data_sample_bytes: metrics.delivery_sample_bytes,
    }
}

fn path_metrics_from_quic_capacity_proof(
    path_id: PathId,
    metrics: UdpPathMetrics,
    _candidate: QuicCapacityProofCandidate,
) -> PathMetrics {
    path_metrics_from_quic_path(path_id, metrics)
}

fn duration_to_micros_u32(duration: Duration) -> u32 {
    u32::try_from(duration.as_micros()).unwrap_or(u32::MAX)
}

pub(super) fn quic_path_metrics_poll_interval(metrics: UdpPathMetrics) -> Duration {
    if metrics.capacity_probe.is_some_and(|probe| {
        matches!(
            probe.phase,
            quic_transport::MeasurementPhase::Writing
                | quic_transport::MeasurementPhase::Measuring
                | quic_transport::MeasurementPhase::AwaitingReceipt
                | quic_transport::MeasurementPhase::Complete
        )
    }) {
        // Receipt and retirement are short-lived control transitions. Poll at
        // quarter RTT, bounded by timer precision and QUIC's max ACK delay.
        return (metrics.srtt / 4).clamp(QUIC_TIMER_GRANULARITY, QUIC_MAX_ACK_DELAY);
    }
    if metrics.app_limited {
        transport_pto_from_ms(
            metrics.srtt.as_secs_f64() * 1000.0,
            metrics.rttvar.as_secs_f64() * 1000.0,
        )
    } else {
        (metrics.srtt / 2).max(QUIC_TIMER_GRANULARITY)
    }
}

#[cfg(test)]
#[path = "metrics_test.rs"]
mod tests;
