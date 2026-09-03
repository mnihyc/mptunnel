use super::estimator::UdpPathMetricTracker;
use super::io::UdpPathConnection;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{QUIC_INITIAL_WINDOW_PACKETS, QUIC_TIMER_GRANULARITY};
use crate::model::timing::transport_pto_from_ms;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::SessionId;
use crate::protocol::{
    PATH_METRICS_MAX_RATE_VALID_FOR_US, PathId, PathMetricDirection, PathMetrics, UnderlayProtocol,
};
use crate::runtime::path::model::{metric_epoch_now, ratio_to_ppm};
use crate::runtime::path::server_context::ServerPathContext;
use crate::runtime::path::{CarrierDeliveryRateSample, ServerCarrierPathRegistration};
use std::time::{Duration, Instant};

#[cfg(feature = "lab-diagnostics")]
#[derive(Debug, Clone, Copy, Default)]
pub(in crate::runtime) struct QuicAckPollDiagnostics {
    pub(in crate::runtime) newly_lost_bytes: u64,
    pub(in crate::runtime) newly_acked_bytes: u64,
    pub(in crate::runtime) non_app_limited_acked_bytes: u64,
    pub(in crate::runtime) timed_non_app_limited_acked_bytes: u64,
    pub(in crate::runtime) ack_elapsed: Duration,
    pub(in crate::runtime) delivery_sample_count: u64,
    pub(in crate::runtime) non_app_limited_sample_count: u64,
    pub(in crate::runtime) timed_non_app_limited_sample_count: u64,
    pub(in crate::runtime) carrier_app_limited: bool,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct UdpPathMetrics {
    pub(in crate::runtime) controller_path_epoch: u64,
    pub(in crate::runtime) direction: PathMetricDirection,
    pub(in crate::runtime) srtt: Duration,
    pub(in crate::runtime) rttvar: Duration,
    pub(in crate::runtime) rtt_observed: bool,
    pub(in crate::runtime) delivery_rate_bps: f64,
    pub(in crate::runtime) pacing_rate_bps: f64,
    pub(in crate::runtime) controller_bandwidth_bps: Option<u64>,
    pub(in crate::runtime) inflight_hi: usize,
    pub(in crate::runtime) bytes_in_flight: usize,
    pub(in crate::runtime) pending_bytes: usize,
    pub(in crate::runtime) loss_ppm: Option<u32>,
    pub(in crate::runtime) ecn_ppm: Option<u32>,
    /// Native QUIC congestion-controller app-limited state. Placement-proof
    /// freshness remains independent in `bulk_proof_expires_at`.
    pub(in crate::runtime) app_limited: bool,
    /// A full bounded window of connection-wide, non-app-limited native ACKs
    /// has qualified this carrier. This never identifies Product bytes.
    pub(in crate::runtime) ack_derived_data_seen: bool,
    pub(in crate::runtime) delivery_sample_count: u64,
    pub(in crate::runtime) delivery_sample_bytes: u64,
    pub(in crate::runtime) last_delivery_sample_at: Option<Instant>,
    #[cfg_attr(not(any(test, feature = "lab-diagnostics")), allow(dead_code))]
    pub(in crate::runtime) bulk_proof_expires_at: Option<Instant>,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) latest_delivery_sample_bytes: u64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) latest_delivery_sample_count: u64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) latest_carrier_ack_elapsed: Option<Duration>,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) latest_rate_sample_elapsed: Option<Duration>,
    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) ack_poll: QuicAckPollDiagnostics,
}

#[derive(Debug, Default)]
struct QuicControllerPublicationCursor {
    previous: Option<(u64, Option<u64>)>,
}

impl QuicControllerPublicationCursor {
    fn changed(&mut self, metrics: UdpPathMetrics) -> bool {
        let current = (
            metrics.controller_path_epoch,
            metrics.controller_bandwidth_bps.filter(|rate| *rate > 0),
        );
        self.previous.replace(current) != Some(current)
    }
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
    let Some(native_authority) = connection.native_rate_authority() else {
        connection.close();
        return;
    };
    let mut authority_changed = native_authority.accepted_change_cursor();
    let _ = *authority_changed.borrow_and_update();
    let mut controller_publication = QuicControllerPublicationCursor::default();
    let mut delivery_rate_sample = None;
    #[cfg(feature = "lab-diagnostics")]
    let mut last_metrics_poll_at = None;
    let write_activity = connection.write_activity_notify();
    loop {
        let activity_started = write_activity.notified();
        tokio::pin!(activity_started);
        activity_started.as_mut().enable();
        if connection.is_closed() {
            return;
        }
        let (metrics, native_shape) = match connection.tx_metrics(&mut tracker) {
            Ok(sample) => sample,
            Err(error) if error.is_retryable_publication() => {
                tokio::select! {
                    _ = authority_changed.changed() => {}
                    _ = tokio::time::sleep(QUIC_TIMER_GRANULARITY) => {}
                    _ = &mut activity_started => {}
                }
                continue;
            }
            Err(_) => {
                connection.close();
                return;
            }
        };
        let shape_publication = native_authority.commit_if_current(native_shape.stamp(), || {
            // Only the O(1) registry-slot replacement occurs under the native
            // fence. Fanout is stamped and revision-guarded below, so a large
            // stream population cannot stall Quinn activation/publication.
            context
                .reliable_streams
                .stage_native_scheduling_shape(&path_registration, native_shape)
        });
        match shape_publication {
            Ok(true) => context
                .reliable_streams
                .fanout_native_scheduling_shape(&path_registration, native_shape),
            Ok(false) => {}
            Err(error) if error.is_retryable_publication() => continue,
            Err(_) => {
                connection.close();
                return;
            }
        }
        let controller_state_changed = controller_publication.changed(metrics);
        let previous_delivery_rate_sample = delivery_rate_sample;
        delivery_rate_sample = retained_quic_delivery_rate_sample(delivery_rate_sample, metrics);
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

        if quic_path_metrics_should_publish_local_sender(metrics, controller_state_changed)
            || (previous_delivery_rate_sample.is_some() && delivery_rate_sample.is_none())
        {
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
                        "session_id={} path_id={} path_instance_id={} direction={:?} rate_source=native_ack_clock sample_bytes={} sample_count={} carrier_elapsed_us={} sample_elapsed_us={} raw_rate_bps={} published_rate_bps={} poll_elapsed_us={} total_sample_count={} total_sample_bytes={} app_limited={}",
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
            context
                .reliable_streams
                .record_local_path_metrics_with_native_epoch(
                    &path_registration,
                    path_metrics_from_quic_path(path_id, metrics, delivery_rate_sample),
                    false,
                    Some(metrics.controller_path_epoch),
                    delivery_rate_sample,
                );
        }
        tokio::select! {
            _ = tokio::time::sleep(quic_path_metrics_poll_interval(metrics)) => {}
            _ = &mut activity_started => {
                // A write transition starts an ACK-clock sample; sampling at
                // the write instant would only republish pre-delivery state.
                tokio::time::sleep(quic_path_metrics_ack_interval(metrics)).await;
            }
            _ = authority_changed.changed() => {}
        }
    }
}

fn retained_quic_delivery_rate_sample(
    previous: Option<CarrierDeliveryRateSample>,
    metrics: UdpPathMetrics,
) -> Option<CarrierDeliveryRateSample> {
    let Some(observed_at) = metrics.last_delivery_sample_at else {
        // Only a native path-evidence epoch reset removes this marker. That is
        // the causal boundary allowed to clear a retained diagnostic sample.
        return None;
    };
    let Some(expires_at) = metrics.bulk_proof_expires_at else {
        // Expiry revokes placement authority but keeps the immutable sample
        // for diagnostics until the native path epoch itself changes.
        return previous;
    };
    if previous.is_some_and(|sample| sample.observed_at == observed_at) {
        return previous;
    }
    Some(CarrierDeliveryRateSample {
        delivery_rate_bps: metrics.delivery_rate_bps.max(1.0).round() as u64,
        pacing_rate_bps: (metrics.pacing_rate_bps.is_finite() && metrics.pacing_rate_bps > 0.0)
            .then(|| metrics.pacing_rate_bps.round() as u64),
        sample_count: u32::try_from(metrics.delivery_sample_count).unwrap_or(u32::MAX),
        sample_bytes: metrics.delivery_sample_bytes,
        delivery_window_covered: true,
        observed_at,
        expires_at,
    })
}

fn quic_path_metrics_should_publish_local_sender(
    metrics: UdpPathMetrics,
    controller_state_changed: bool,
) -> bool {
    // A fresh path epoch must clear an older live controller value even before
    // Product delivery proof is reacquired. Stable idle controller state does
    // not needlessly walk every bound response-stream registry entry.
    controller_state_changed || metrics.delivery_sample_count > 0 || metrics.ack_derived_data_seen
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
    if ack.newly_lost_bytes > 0 || ack.newly_acked_bytes > 0 {
        lab_diagnostic(
            "quic_carrier_ack_poll",
            format_args!(
                "session_id={} path_id={} path_instance_id={} direction={:?} path_epoch={} poll_elapsed_us={} srtt_us={} rttvar_us={} congestion_window_bytes={} bytes_in_flight={} pending_bytes={} controller_bandwidth_bps={} pacing_rate_bps={} loss_ppm={} newly_lost_bytes={} newly_acked_bytes={} non_app_limited_acked_bytes={} timed_non_app_limited_acked_bytes={} ack_elapsed_us={} sample_count={} non_app_limited_sample_count={} timed_non_app_limited_sample_count={} carrier_app_limited={}",
                session_id.0,
                path_id.0,
                path_instance_id,
                metrics.direction,
                metrics.controller_path_epoch,
                poll_elapsed.as_micros(),
                metrics.srtt.as_micros(),
                metrics.rttvar.as_micros(),
                metrics.inflight_hi,
                metrics.bytes_in_flight,
                metrics.pending_bytes,
                metrics
                    .controller_bandwidth_bps
                    .map(|rate| rate.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                metrics.pacing_rate_bps.round() as u64,
                metrics.loss_ppm.unwrap_or(0),
                ack.newly_lost_bytes,
                ack.newly_acked_bytes,
                ack.non_app_limited_acked_bytes,
                ack.timed_non_app_limited_acked_bytes,
                ack.ack_elapsed.as_micros(),
                ack.delivery_sample_count,
                ack.non_app_limited_sample_count,
                ack.timed_non_app_limited_sample_count,
                ack.carrier_app_limited,
            ),
        );
    }
}

fn path_metrics_from_quic_path(
    path_id: PathId,
    metrics: UdpPathMetrics,
    delivery_rate_sample: Option<CarrierDeliveryRateSample>,
) -> PathMetrics {
    let now = Instant::now();
    // Current shape polls may update RTT, flight, and Quinn pacing, but only
    // the immutable full-window ACK epoch may carry native rate authority.
    let qualified_rate_epoch = delivery_rate_sample.filter(|sample| {
        sample.sample_count > 0 && sample.sample_bytes > 0 && sample.observed_at <= now
    });
    let rate_valid_for_us = qualified_rate_epoch
        .map(|sample| {
            u64::try_from(sample.expires_at.saturating_duration_since(now).as_micros())
                .unwrap_or(u64::MAX)
        })
        .unwrap_or(0)
        .min(PATH_METRICS_MAX_RATE_VALID_FOR_US);
    let pacing_rate_observed =
        qualified_rate_epoch.is_some_and(|sample| sample.pacing_rate_bps.is_some());
    let rate_observed = qualified_rate_epoch.is_some();
    let delivery_rate_bps = qualified_rate_epoch.map_or_else(
        || metrics.delivery_rate_bps.max(1.0).round() as u64,
        |sample| sample.delivery_rate_bps.max(1),
    );
    let pacing_rate_bps = if pacing_rate_observed {
        qualified_rate_epoch
            .and_then(|sample| sample.pacing_rate_bps)
            .unwrap_or(delivery_rate_bps)
    } else {
        delivery_rate_bps
    };
    PathMetrics {
        path_id,
        underlay: UnderlayProtocol::Udp,
        direction: metrics.direction,
        metric_epoch: metric_epoch_now(),
        metric_age_us: qualified_rate_epoch
            .map(|sample| {
                u32::try_from(
                    now.saturating_duration_since(sample.observed_at)
                        .as_micros(),
                )
                .unwrap_or(u32::MAX)
            })
            .unwrap_or(0),
        rate_valid_for_us,
        rate_observed,
        srtt_us: duration_to_micros_u32(metrics.srtt),
        rttvar_us: duration_to_micros_u32(metrics.rttvar),
        jitter_us: duration_to_micros_u32(metrics.rttvar),
        delivery_rate_bps,
        pacing_rate_bps,
        pacing_rate_observed,
        loss_ppm: metrics.loss_ppm.unwrap_or(0),
        ecn_ppm: metrics.ecn_ppm.unwrap_or(0),
        loss_observed: metrics.loss_ppm.is_some(),
        ecn_observed: metrics.ecn_ppm.is_some(),
        bytes_in_flight_observed: true,
        queue_observed: true,
        bytes_in_flight: metrics.bytes_in_flight as u64,
        queue_bytes: metrics
            .pending_bytes
            .saturating_sub(metrics.bytes_in_flight) as u64,
        inflight_limit_bytes: metrics.inflight_hi as u64,
        inflight_hi_bytes: metrics.inflight_hi as u64,
        confidence_ppm: ratio_to_ppm(
            (qualified_rate_epoch.map_or(0, |sample| sample.sample_count) as f64
                / QUIC_INITIAL_WINDOW_PACKETS as f64)
                .clamp(0.0, 1.0),
        ),
        // Application-limited is current Quinn controller state. The retained
        // delivery epoch below owns rate provenance only and cannot rewrite it.
        app_limited: metrics.app_limited,
        // This flag is generic native carrier evidence in PathMetrics; MPP
        // Product completion remains independently DataACK-derived.
        has_ack_derived_data_sample: rate_observed || metrics.ack_derived_data_seen,
        data_sample_count: qualified_rate_epoch.map_or(0, |sample| sample.sample_count),
        data_sample_bytes: qualified_rate_epoch.map_or(0, |sample| sample.sample_bytes),
    }
}

fn duration_to_micros_u32(duration: Duration) -> u32 {
    u32::try_from(duration.as_micros()).unwrap_or(u32::MAX)
}

pub(super) fn quic_path_metrics_poll_interval(metrics: UdpPathMetrics) -> Duration {
    let carrier_is_active = metrics.bytes_in_flight > 0 || metrics.pending_bytes > 0;
    if metrics.app_limited && !carrier_is_active {
        transport_pto_from_ms(
            metrics.srtt.as_secs_f64() * 1000.0,
            metrics.rttvar.as_secs_f64() * 1000.0,
        )
    } else {
        quic_path_metrics_ack_interval(metrics)
    }
}

pub(super) fn quic_path_metrics_ack_interval(metrics: UdpPathMetrics) -> Duration {
    (metrics.srtt / 2).max(QUIC_TIMER_GRANULARITY)
}

#[cfg(test)]
#[path = "tests_metrics.rs"]
mod tests;
