//! QUIC native congestion and ACK observer.
//!
//! Quinn owns packet delivery, congestion, and pacing. This module projects
//! connection-wide native delivery into carrier-capacity evidence; it never
//! interprets a native packet ACK as MPP Product completion. Exact Product
//! progress belongs to the MPP DataACK clock maintained by the reliable-stream
//! runtime.

use super::io::UdpPathConnection;
#[cfg(feature = "lab-diagnostics")]
use super::metrics::QuicAckPollDiagnostics;
use super::metrics::UdpPathMetrics;
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, QUIC_INITIAL_WINDOW_PACKETS, QUIC_TIMER_GRANULARITY,
    RELIABLE_PIPE_WINDOW_BDPS, RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES,
};
use crate::model::timing::quic_bulk_proof_freshness_horizon;
use crate::protocol::PathMetricDirection;
use crate::runtime::path::authority::{
    NativeCarrierRateAuthorityRuntimeError, NativeCarrierSchedulingShapeSnapshot,
};
use crate::runtime::path::model::default_path_rate_bps;
use crate::transport::quic as quic_transport;
use std::time::{Duration, Instant};

#[derive(Debug, Default)]
pub(super) struct UdpPathMetricTracker {
    quic: QuicPathMetricTracker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DeliveryClockEpochKey {
    path_epoch: u64,
    delivery_clock_epoch: u64,
}

#[derive(Debug, Default, Clone, Copy)]
struct DeliveryClockTotals {
    timed_acked_bytes: u64,
    timed_sample_count: u64,
    elapsed: Duration,
}

#[derive(Debug, Clone, Copy)]
struct DeliveryClockCursor {
    key: DeliveryClockEpochKey,
    totals: DeliveryClockTotals,
}

/// One connection-wide native delivery-clock epoch under construction.
///
/// These bytes are deliberately not attributed to an H3 request or Product
/// frame. They qualify only the shared QUIC carrier after enough native volume
/// covers a complete bounded congestion window.
#[derive(Debug, Clone, Copy)]
struct PendingNativeDeliverySampleEpoch {
    key: DeliveryClockEpochKey,
    sample_bytes: u64,
    sample_count: u64,
    sample_elapsed: Duration,
    publish_floor: u64,
    durable_floor: u64,
}

impl UdpPathMetricTracker {
    #[cfg(test)]
    pub(super) fn observe(
        &mut self,
        stats: quinn::ConnectionStats,
        congestion: quic_transport::CongestionMetrics,
        direction: PathMetricDirection,
    ) -> UdpPathMetrics {
        self.quic.observe(stats, congestion, direction)
    }
}

#[derive(Debug, Default)]
struct QuicPathMetricTracker {
    path_epoch: Option<u64>,
    delivery_rate_bps: Option<f64>,
    native_delivery_seen: bool,
    delivery_clock_cursor: Option<DeliveryClockCursor>,
    pending_native_sample: Option<PendingNativeDeliverySampleEpoch>,
    delivery_sample_count: u64,
    delivery_sample_bytes: u64,
    last_delivery_sample_at: Option<Instant>,
    bulk_proof_expires_at: Option<Instant>,
    #[cfg(feature = "lab-diagnostics")]
    last_lost_bytes: Option<u64>,
}

impl UdpPathConnection {
    pub(super) fn native_capacity_epoch(&self) -> u64 {
        self.connection.native_path_epoch()
    }

    pub(super) fn tx_metrics(
        &self,
        tracker: &mut UdpPathMetricTracker,
    ) -> Result<
        (UdpPathMetrics, NativeCarrierSchedulingShapeSnapshot),
        NativeCarrierRateAuthorityRuntimeError,
    > {
        let authority = self
            .native_rate_authority()
            .ok_or(NativeCarrierRateAuthorityRuntimeError::TransportSourceUnavailable)?;
        let scope = authority.stamp()?.scope();
        let shape = authority.refresh_scheduling_shape(scope)?;
        let congestion = self.connection.congestion_metrics();
        let metrics = tracker
            .quic
            .observe_native(shape, congestion, scope.direction());
        Ok((metrics, shape))
    }
}

impl QuicPathMetricTracker {
    fn enter_path_epoch(&mut self, congestion: quic_transport::CongestionMetrics) {
        match self.path_epoch {
            None => self.path_epoch = Some(congestion.path_epoch),
            Some(path_epoch) if path_epoch == congestion.path_epoch => {}
            Some(_) => {
                // A migrated/reset native path must earn its own rate and
                // bounded proof lifetime. No capacity scalar crosses epochs.
                *self = Self {
                    path_epoch: Some(congestion.path_epoch),
                    #[cfg(feature = "lab-diagnostics")]
                    last_lost_bytes: Some(congestion.lost_bytes),
                    ..Self::default()
                };
            }
        }
    }

    fn clear_pending_native_sample(&mut self) {
        self.pending_native_sample = None;
    }

    fn observe_delivery_clock(
        &mut self,
        congestion: quic_transport::CongestionMetrics,
    ) -> (DeliveryClockEpochKey, DeliveryClockTotals) {
        let key = DeliveryClockEpochKey {
            path_epoch: congestion.path_epoch,
            delivery_clock_epoch: congestion.delivery_clock_epoch,
        };
        let totals = DeliveryClockTotals {
            timed_acked_bytes: congestion.timed_non_app_limited_acked_bytes.unwrap_or(0),
            timed_sample_count: congestion.timed_non_app_limited_delivery_sample_count,
            elapsed: congestion.non_app_limited_ack_elapsed.unwrap_or_default(),
        };
        let fragment = match self.delivery_clock_cursor {
            None => totals,
            Some(previous) if previous.key != key => {
                self.clear_pending_native_sample();
                totals
            }
            Some(previous) => {
                let Some(timed_acked_bytes) = totals
                    .timed_acked_bytes
                    .checked_sub(previous.totals.timed_acked_bytes)
                else {
                    self.clear_pending_native_sample();
                    self.delivery_clock_cursor = Some(DeliveryClockCursor { key, totals });
                    return (key, DeliveryClockTotals::default());
                };
                let Some(timed_sample_count) = totals
                    .timed_sample_count
                    .checked_sub(previous.totals.timed_sample_count)
                else {
                    self.clear_pending_native_sample();
                    self.delivery_clock_cursor = Some(DeliveryClockCursor { key, totals });
                    return (key, DeliveryClockTotals::default());
                };
                let Some(elapsed) = totals.elapsed.checked_sub(previous.totals.elapsed) else {
                    self.clear_pending_native_sample();
                    self.delivery_clock_cursor = Some(DeliveryClockCursor { key, totals });
                    return (key, DeliveryClockTotals::default());
                };
                DeliveryClockTotals {
                    timed_acked_bytes,
                    timed_sample_count,
                    elapsed,
                }
            }
        };
        self.delivery_clock_cursor = Some(DeliveryClockCursor { key, totals });
        (key, fragment)
    }

    fn expire_stale_bulk_proof(&mut self, now: Instant) {
        if self
            .bulk_proof_expires_at
            .is_none_or(|expires_at| now < expires_at)
        {
            return;
        }
        // Keep the immutable diagnostic point, but revoke placement authority.
        // The next native clock epoch must earn fresh byte coverage.
        self.bulk_proof_expires_at = None;
        self.delivery_sample_count = 0;
        self.delivery_sample_bytes = 0;
    }

    fn observe(
        &mut self,
        stats: quinn::ConnectionStats,
        congestion: quic_transport::CongestionMetrics,
        direction: PathMetricDirection,
    ) -> UdpPathMetrics {
        self.observe_at(stats, congestion, direction, Instant::now())
    }

    fn observe_native(
        &mut self,
        shape: NativeCarrierSchedulingShapeSnapshot,
        congestion: quic_transport::CongestionMetrics,
        direction: PathMetricDirection,
    ) -> UdpPathMetrics {
        self.observe_shape_at(
            shape.srtt(),
            shape.rttvar(),
            shape.congestion_window(),
            shape.bytes_in_flight(),
            shape.current_mtu(),
            shape.finite_rate_bps(),
            shape.pacing_rate_bps(),
            shape.app_limited(),
            congestion,
            direction,
            Instant::now(),
        )
    }

    fn observe_at(
        &mut self,
        stats: quinn::ConnectionStats,
        congestion: quic_transport::CongestionMetrics,
        direction: PathMetricDirection,
        now: Instant,
    ) -> UdpPathMetrics {
        let rtt = stats.path.rtt.max(QUIC_TIMER_GRANULARITY);
        let congestion_window = congestion.congestion_window.max(stats.path.cwnd);
        let bytes_in_flight = congestion.bytes_in_flight.unwrap_or(0);
        let controller_rate = congestion.bandwidth_estimate_bps.unwrap_or_else(|| {
            let inflight = congestion_window.max(stats.path.current_mtu as u64);
            (inflight as f64 * 8.0 / rtt.as_secs_f64().max(QUIC_TIMER_GRANULARITY.as_secs_f64()))
                .ceil()
                .max(1.0) as u64
        });
        self.observe_shape_at(
            rtt,
            rtt / 4,
            congestion_window,
            bytes_in_flight,
            stats.path.current_mtu,
            Some(controller_rate),
            congestion.pacing_rate_bps,
            congestion.app_limited,
            congestion,
            direction,
            now,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn observe_shape_at(
        &mut self,
        raw_rtt: Duration,
        raw_rttvar: Duration,
        congestion_window: u64,
        bytes_in_flight: u64,
        current_mtu: u16,
        authority_rate_bps: Option<u64>,
        native_pacing_rate_bps: Option<u64>,
        native_app_limited: bool,
        congestion: quic_transport::CongestionMetrics,
        direction: PathMetricDirection,
        now: Instant,
    ) -> UdpPathMetrics {
        self.enter_path_epoch(congestion);
        #[cfg(feature = "lab-diagnostics")]
        let newly_lost_bytes = {
            let delta = self
                .last_lost_bytes
                .map_or(0, |previous| congestion.lost_bytes.saturating_sub(previous));
            self.last_lost_bytes = Some(congestion.lost_bytes);
            delta
        };

        let rtt = raw_rtt.max(QUIC_TIMER_GRANULARITY);
        let rttvar = raw_rttvar;
        let bulk_proof_freshness_horizon = quic_bulk_proof_freshness_horizon(rtt, rttvar);
        self.expire_stale_bulk_proof(now);
        let carrier_capacity_known = native_pacing_rate_bps.is_some() || congestion_window > 0;
        let inflight_hi = if carrier_capacity_known {
            congestion_window.max(u64::from(current_mtu)) as usize
        } else {
            0
        };
        let startup_rate = default_path_rate_bps();
        let raw_pacing_rate = native_pacing_rate_bps.map(|rate| rate.max(1) as f64);
        // Unlimited startup has no numeric authority value. Metrics may still
        // report a best-effort native pacing/window estimate, but that
        // diagnostic must not manufacture a scheduling-rate authority.
        let controller_rate = authority_rate_bps.map(|rate| rate as f64);
        let usable_pacing_rate = raw_pacing_rate.map(|rate| {
            if self.delivery_sample_count == 0 {
                rate.max(startup_rate)
            } else {
                rate
            }
        });
        let fallback_rate = controller_rate.or(usable_pacing_rate).unwrap_or_else(|| {
            if carrier_capacity_known {
                let cwnd_rate = inflight_hi as f64 * 8.0
                    / rtt.as_secs_f64().max(QUIC_TIMER_GRANULARITY.as_secs_f64());
                if self.delivery_sample_count == 0 {
                    cwnd_rate.max(startup_rate)
                } else {
                    cwnd_rate
                }
            } else {
                startup_rate
            }
        });
        let evidence_inflight_hi = if inflight_hi > 0 {
            inflight_hi as u64
        } else {
            (fallback_rate / 8.0 * rtt.as_secs_f64().max(QUIC_TIMER_GRANULARITY.as_secs_f64()))
                .ceil()
                .max(1.0) as u64
        };

        let (delivery_clock_key, native_clock_fragment) = self.observe_delivery_clock(congestion);
        let native_ack_elapsed =
            (!native_clock_fragment.elapsed.is_zero()).then_some(native_clock_fragment.elapsed);
        let timed_native_delivery =
            native_ack_elapsed.is_some() && native_clock_fragment.timed_acked_bytes > 0;
        // Connection-wide pending/flight includes H3 framing and carrier
        // control, which is correct for native queue telemetry and says
        // nothing about Product completion.
        let carrier_committed_bytes = congestion.pending_bytes.max(bytes_in_flight);

        let confidence_sample_floor = QUIC_INITIAL_WINDOW_PACKETS as u64;
        let capacity_sample_cap = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES.saturating_div(2);
        let preconfidence_publish_floor = evidence_inflight_hi
            .max(PATH_OPEN_SCORE_BYTES as u64)
            .min(capacity_sample_cap);
        let durable_native_window_floor = evidence_inflight_hi
            .max((PATH_OPEN_SCORE_BYTES as u64).saturating_mul(4))
            .min(capacity_sample_cap);
        let mut publishable_sample = None;
        if timed_native_delivery {
            let publish_floor = if self.delivery_sample_count == 0 {
                preconfidence_publish_floor
            } else {
                durable_native_window_floor
            };
            let pending =
                self.pending_native_sample
                    .get_or_insert(PendingNativeDeliverySampleEpoch {
                        key: delivery_clock_key,
                        sample_bytes: 0,
                        sample_count: 0,
                        sample_elapsed: Duration::ZERO,
                        publish_floor,
                        durable_floor: durable_native_window_floor,
                    });
            debug_assert_eq!(pending.key, delivery_clock_key);
            pending.sample_bytes = pending
                .sample_bytes
                .saturating_add(native_clock_fragment.timed_acked_bytes);
            pending.sample_count = pending.sample_count.saturating_add(
                native_clock_fragment
                    .timed_sample_count
                    .max(u64::from(native_clock_fragment.timed_acked_bytes > 0)),
            );
            pending.sample_elapsed = pending
                .sample_elapsed
                .saturating_add(native_clock_fragment.elapsed);
            if pending.sample_bytes >= pending.publish_floor {
                publishable_sample = self.pending_native_sample.take();
            }
        }

        let mut latest_delivery_sample_bytes = 0;
        let mut latest_delivery_sample_count = 0;
        let mut latest_rate_sample_elapsed = None;
        if let Some(publishable_sample) = publishable_sample {
            let sample_bytes = publishable_sample.sample_bytes;
            let sample_count = publishable_sample.sample_count;
            let sample_elapsed = publishable_sample
                .sample_elapsed
                .max(QUIC_TIMER_GRANULARITY);
            latest_delivery_sample_bytes = sample_bytes;
            latest_delivery_sample_count = sample_count;
            latest_rate_sample_elapsed = Some(sample_elapsed);
            let sample_rate = (sample_bytes as f64 * 8.0 / sample_elapsed.as_secs_f64()).max(1.0);
            let previous_sample_count = self.delivery_sample_count;
            let next_sample_bytes = self.delivery_sample_bytes.saturating_add(sample_bytes);
            let candidate_sample_count = self.delivery_sample_count.saturating_add(sample_count);
            let confidence_has_byte_volume =
                next_sample_bytes >= evidence_inflight_hi.max(PATH_OPEN_SCORE_BYTES as u64);
            let next_sample_count = if previous_sample_count < confidence_sample_floor
                && candidate_sample_count >= confidence_sample_floor
                && !confidence_has_byte_volume
            {
                confidence_sample_floor.saturating_sub(1)
            } else {
                candidate_sample_count
            };
            let establishes_measured_rate = previous_sample_count < confidence_sample_floor
                && next_sample_count >= confidence_sample_floor;
            self.delivery_sample_count = next_sample_count;
            self.delivery_sample_bytes = next_sample_bytes;

            // Only a complete bounded native window obtains placement
            // authority. Smaller ACK fragments remain telemetry and cannot
            // mint either native capacity or Product progress.
            if sample_bytes >= publishable_sample.durable_floor {
                self.native_delivery_seen = true;
                self.last_delivery_sample_at = Some(now);
                self.bulk_proof_expires_at = now.checked_add(bulk_proof_freshness_horizon);
                let estimated_rate = self.delivery_rate_bps.unwrap_or(fallback_rate).max(1.0);
                let current_rate = if previous_sample_count < confidence_sample_floor {
                    estimated_rate.max(fallback_rate)
                } else {
                    estimated_rate
                };
                let bounded_sample = sample_rate.min(current_rate * RELIABLE_PIPE_WINDOW_BDPS);
                self.delivery_rate_bps = Some(match self.delivery_rate_bps {
                    Some(_) | None if establishes_measured_rate => bounded_sample,
                    Some(previous) => previous.mul_add(0.25, bounded_sample * 0.75),
                    None => bounded_sample,
                });
            }
        }

        let estimated_rate = self.delivery_rate_bps.unwrap_or(fallback_rate).max(1.0);
        let delivery_rate_bps = if self.delivery_sample_count < confidence_sample_floor {
            estimated_rate.max(fallback_rate)
        } else {
            estimated_rate
        };
        let pacing_rate_bps = usable_pacing_rate
            .unwrap_or(delivery_rate_bps)
            .max(delivery_rate_bps);

        #[cfg(feature = "lab-diagnostics")]
        let newly_acked_bytes = congestion.newly_acked_bytes.unwrap_or(0);
        #[cfg(feature = "lab-diagnostics")]
        let non_app_limited_acked_bytes = congestion
            .non_app_limited_acked_bytes
            .unwrap_or(0)
            .min(newly_acked_bytes);

        UdpPathMetrics {
            controller_path_epoch: congestion.path_epoch,
            direction,
            srtt: rtt,
            rttvar,
            rtt_observed: !raw_rtt.is_zero(),
            // NativeMode has one scheduling rate: the central C0/Bop value.
            // An Unlimited startup has no numeric authority, so this legacy
            // metric reports only the best-effort transport estimate while
            // the typed scheduling projection remains nonnumeric.
            delivery_rate_bps: authority_rate_bps.map_or(delivery_rate_bps, |rate| rate as f64),
            pacing_rate_bps,
            controller_bandwidth_bps: authority_rate_bps,
            inflight_hi,
            bytes_in_flight: usize::try_from(bytes_in_flight).unwrap_or(usize::MAX),
            pending_bytes: usize::try_from(carrier_committed_bytes).unwrap_or(usize::MAX),
            loss_ppm: congestion.loss_ppm,
            ecn_ppm: congestion.ecn_ppm,
            app_limited: native_app_limited,
            // Historical field name; this is native carrier qualification,
            // never Product-byte attribution.
            ack_derived_data_seen: self.native_delivery_seen,
            delivery_sample_count: self.delivery_sample_count,
            delivery_sample_bytes: self.delivery_sample_bytes,
            last_delivery_sample_at: self.last_delivery_sample_at,
            bulk_proof_expires_at: self.bulk_proof_expires_at,
            latest_delivery_sample_bytes,
            latest_delivery_sample_count,
            latest_carrier_ack_elapsed: native_ack_elapsed,
            latest_rate_sample_elapsed,
            #[cfg(feature = "lab-diagnostics")]
            ack_poll: QuicAckPollDiagnostics {
                newly_lost_bytes,
                newly_acked_bytes,
                non_app_limited_acked_bytes,
                timed_non_app_limited_acked_bytes: native_clock_fragment.timed_acked_bytes,
                ack_elapsed: native_clock_fragment.elapsed,
                delivery_sample_count: congestion.delivery_sample_count,
                non_app_limited_sample_count: congestion.non_app_limited_delivery_sample_count,
                timed_non_app_limited_sample_count: native_clock_fragment.timed_sample_count,
                carrier_app_limited: native_app_limited,
            },
        }
    }
}

#[cfg(test)]
#[path = "tests_estimator_lifecycle.rs"]
mod lifecycle_tests;
#[cfg(test)]
#[path = "tests_estimator_rate.rs"]
mod rate_tests;
