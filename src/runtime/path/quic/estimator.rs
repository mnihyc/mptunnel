//! QUIC ACK and delivery-rate estimator state.
//!
//! This module turns native carrier snapshots into typed path evidence; it does
//! not publish health or choose paths.

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
    timed_delivery_evidence_acked_bytes: u64,
    timed_sample_count: u64,
    elapsed: Duration,
    timed_delivery_evidence_sample_count: u64,
    timed_delivery_evidence_elapsed: Duration,
}

#[derive(Debug, Clone, Copy)]
struct DeliveryClockCursor {
    key: DeliveryClockEpochKey,
    totals: DeliveryClockTotals,
}

#[derive(Debug, Clone, Copy)]
struct PendingDeliverySampleEpoch {
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
    last_delivery_evidence_written_bytes: u64,
    last_delivery_evidence_cancelled_bytes: u64,
    delivery_evidence_pending_ack_bytes: u64,
    delivery_rate_bps: Option<f64>,
    ack_derived_data_seen: bool,
    delivery_clock_cursor: Option<DeliveryClockCursor>,
    pending_delivery_sample: Option<PendingDeliverySampleEpoch>,
    delivery_sample_count: u64,
    delivery_sample_bytes: u64,
    last_delivery_sample_at: Option<Instant>,
    bulk_proof_expires_at: Option<Instant>,
    #[cfg(feature = "lab-diagnostics")]
    last_lost_bytes: Option<u64>,
}

impl UdpPathConnection {
    pub(super) fn tx_metrics(
        &self,
        tracker: &mut UdpPathMetricTracker,
        direction: PathMetricDirection,
    ) -> UdpPathMetrics {
        let stats = self.connection.stats();
        let congestion = self.connection.congestion_metrics();
        tracker.quic.observe(stats, congestion, direction)
    }
}

impl QuicPathMetricTracker {
    fn enter_path_epoch(&mut self, congestion: quic_transport::CongestionMetrics) {
        match self.path_epoch {
            None => {
                self.path_epoch = Some(congestion.path_epoch);
                self.last_delivery_evidence_written_bytes =
                    congestion.delivery_evidence_written_bytes;
                self.last_delivery_evidence_cancelled_bytes =
                    congestion.delivery_evidence_cancelled_bytes;
                self.delivery_evidence_pending_ack_bytes =
                    congestion.delivery_evidence_pending_ack_bytes;
            }
            Some(path_epoch) if path_epoch == congestion.path_epoch => {}
            Some(_) => {
                // A new network path has a fresh congestion model. Retaining
                // delivery rate, confidence, pending evidence, or placement
                // proof across that boundary would let the old path authorize
                // traffic on an unmeasured one.
                *self = Self {
                    path_epoch: Some(congestion.path_epoch),
                    last_delivery_evidence_written_bytes: congestion
                        .delivery_evidence_written_bytes,
                    last_delivery_evidence_cancelled_bytes: congestion
                        .delivery_evidence_cancelled_bytes,
                    delivery_evidence_pending_ack_bytes: congestion
                        .delivery_evidence_pending_ack_bytes,
                    #[cfg(feature = "lab-diagnostics")]
                    last_lost_bytes: Some(congestion.lost_bytes),
                    ..Self::default()
                };
            }
        }
    }

    fn clear_pending_delivery_sample(&mut self) {
        self.pending_delivery_sample = None;
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
            timed_delivery_evidence_acked_bytes: congestion
                .timed_non_app_limited_delivery_evidence_acked_bytes,
            timed_sample_count: congestion.timed_non_app_limited_delivery_sample_count,
            elapsed: congestion.non_app_limited_ack_elapsed.unwrap_or_default(),
            timed_delivery_evidence_sample_count: congestion
                .timed_non_app_limited_delivery_evidence_sample_count,
            timed_delivery_evidence_elapsed: congestion
                .timed_non_app_limited_delivery_evidence_elapsed,
        };
        let fragment = match self.delivery_clock_cursor {
            None => totals,
            Some(previous) if previous.key != key => {
                // Clock identity, rather than an idle/app-limited poll flag, is
                // the exact boundary that forbids joining two rate samples.
                self.clear_pending_delivery_sample();
                totals
            }
            Some(previous) => {
                let Some(timed_acked_bytes) = totals
                    .timed_acked_bytes
                    .checked_sub(previous.totals.timed_acked_bytes)
                else {
                    self.clear_pending_delivery_sample();
                    self.delivery_clock_cursor = Some(DeliveryClockCursor { key, totals });
                    return (key, DeliveryClockTotals::default());
                };
                let Some(timed_delivery_evidence_acked_bytes) = totals
                    .timed_delivery_evidence_acked_bytes
                    .checked_sub(previous.totals.timed_delivery_evidence_acked_bytes)
                else {
                    self.clear_pending_delivery_sample();
                    self.delivery_clock_cursor = Some(DeliveryClockCursor { key, totals });
                    return (key, DeliveryClockTotals::default());
                };
                let Some(timed_sample_count) = totals
                    .timed_sample_count
                    .checked_sub(previous.totals.timed_sample_count)
                else {
                    self.clear_pending_delivery_sample();
                    self.delivery_clock_cursor = Some(DeliveryClockCursor { key, totals });
                    return (key, DeliveryClockTotals::default());
                };
                let Some(elapsed) = totals.elapsed.checked_sub(previous.totals.elapsed) else {
                    self.clear_pending_delivery_sample();
                    self.delivery_clock_cursor = Some(DeliveryClockCursor { key, totals });
                    return (key, DeliveryClockTotals::default());
                };
                let Some(timed_delivery_evidence_sample_count) = totals
                    .timed_delivery_evidence_sample_count
                    .checked_sub(previous.totals.timed_delivery_evidence_sample_count)
                else {
                    self.clear_pending_delivery_sample();
                    self.delivery_clock_cursor = Some(DeliveryClockCursor { key, totals });
                    return (key, DeliveryClockTotals::default());
                };
                let Some(timed_delivery_evidence_elapsed) = totals
                    .timed_delivery_evidence_elapsed
                    .checked_sub(previous.totals.timed_delivery_evidence_elapsed)
                else {
                    self.clear_pending_delivery_sample();
                    self.delivery_clock_cursor = Some(DeliveryClockCursor { key, totals });
                    return (key, DeliveryClockTotals::default());
                };
                DeliveryClockTotals {
                    timed_acked_bytes,
                    timed_delivery_evidence_acked_bytes,
                    timed_sample_count,
                    elapsed,
                    timed_delivery_evidence_sample_count,
                    timed_delivery_evidence_elapsed,
                }
            }
        };
        self.delivery_clock_cursor = Some(DeliveryClockCursor { key, totals });
        (key, fragment)
    }

    fn expire_stale_bulk_proof(&mut self, now: Instant) {
        let proof_is_stale = self
            .bulk_proof_expires_at
            .is_some_and(|expires_at| now >= expires_at);
        if !proof_is_stale {
            return;
        }

        // The rate estimate remains diagnostic input to the next estimator
        // update, but committed confidence and byte coverage belong to this
        // frozen proof epoch. An in-progress sample is owned by the independent
        // delivery-clock epoch and must not acquire an implicit 3-PTO timeout.
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

    fn observe_at(
        &mut self,
        stats: quinn::ConnectionStats,
        congestion: quic_transport::CongestionMetrics,
        direction: PathMetricDirection,
        now: Instant,
    ) -> UdpPathMetrics {
        self.enter_path_epoch(congestion);
        let delivery_evidence_delta = congestion
            .delivery_evidence_written_bytes
            .saturating_sub(self.last_delivery_evidence_written_bytes);
        self.last_delivery_evidence_written_bytes = congestion.delivery_evidence_written_bytes;
        let delivery_evidence_cancelled_delta = congestion
            .delivery_evidence_cancelled_bytes
            .saturating_sub(self.last_delivery_evidence_cancelled_bytes);
        self.last_delivery_evidence_cancelled_bytes = congestion.delivery_evidence_cancelled_bytes;
        #[cfg(feature = "lab-diagnostics")]
        let newly_lost_bytes = {
            let delta = self
                .last_lost_bytes
                .map_or(0, |previous| congestion.lost_bytes.saturating_sub(previous));
            self.last_lost_bytes = Some(congestion.lost_bytes);
            delta
        };
        self.delivery_evidence_pending_ack_bytes = self
            .delivery_evidence_pending_ack_bytes
            .saturating_add(delivery_evidence_delta);

        let rtt = stats.path.rtt.max(QUIC_TIMER_GRANULARITY);
        let rttvar = rtt / 4;
        let bulk_proof_freshness_horizon = quic_bulk_proof_freshness_horizon(rtt, rttvar);
        self.expire_stale_bulk_proof(now);
        let congestion_window = congestion.congestion_window.max(stats.path.cwnd);
        let carrier_capacity_known = congestion.pacing_rate_bps.is_some() || congestion_window > 0;
        let bytes_in_flight = congestion.bytes_in_flight.unwrap_or(0);
        let inflight_hi = if carrier_capacity_known {
            congestion_window.max(stats.path.current_mtu as u64) as usize
        } else {
            0
        };
        let startup_rate = default_path_rate_bps();
        let raw_pacing_rate = congestion.pacing_rate_bps.map(|rate| rate.max(1) as f64);
        let usable_pacing_rate = raw_pacing_rate.map(|rate| {
            if self.delivery_sample_count == 0 {
                rate.max(startup_rate)
            } else {
                rate
            }
        });
        let fallback_rate = usable_pacing_rate.unwrap_or_else(|| {
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

        let newly_acked_bytes = congestion.newly_acked_bytes.unwrap_or(0);
        #[cfg(feature = "lab-diagnostics")]
        let non_app_limited_acked_bytes = congestion
            .non_app_limited_acked_bytes
            .unwrap_or(0)
            .min(newly_acked_bytes);
        let delivery_evidence_newly_acked_bytes = congestion
            .delivery_evidence_newly_acked_bytes
            .unwrap_or(0)
            .min(newly_acked_bytes);
        let (delivery_clock_key, delivery_clock_fragment) = self.observe_delivery_clock(congestion);
        let timed_non_app_limited_acked_bytes = delivery_clock_fragment.timed_acked_bytes;
        let timed_non_app_limited_delivery_evidence_bytes = delivery_clock_fragment
            .timed_delivery_evidence_acked_bytes
            .min(timed_non_app_limited_acked_bytes);
        #[cfg(feature = "lab-diagnostics")]
        let carrier_ack_elapsed =
            (!delivery_clock_fragment.elapsed.is_zero()).then_some(delivery_clock_fragment.elapsed);
        let delivery_evidence_ack_elapsed = (!delivery_clock_fragment
            .timed_delivery_evidence_elapsed
            .is_zero())
        .then_some(delivery_clock_fragment.timed_delivery_evidence_elapsed);
        let timed_non_app_limited_evidence = delivery_evidence_ack_elapsed.is_some()
            && timed_non_app_limited_delivery_evidence_bytes > 0;
        // A first/compressed zero-span ACK batch proves reachability but has no
        // carrier-clock denominator and therefore cannot enter the rate model.
        if delivery_evidence_newly_acked_bytes > 0 {
            self.ack_derived_data_seen = true;
        }
        self.delivery_evidence_pending_ack_bytes = self
            .delivery_evidence_pending_ack_bytes
            .saturating_sub(delivery_evidence_newly_acked_bytes)
            .saturating_sub(delivery_evidence_cancelled_delta);
        // Product evidence is separate from connection-wide pending/flight
        // counters, which still include framing and carrier control bytes.
        let carrier_committed_bytes = self
            .delivery_evidence_pending_ack_bytes
            .max(congestion.pending_bytes)
            .max(bytes_in_flight);

        let confidence_sample_floor = QUIC_INITIAL_WINDOW_PACKETS as u64;
        let capacity_sample_cap = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES.saturating_div(2);
        let preconfidence_publish_floor = evidence_inflight_hi
            .max(PATH_OPEN_SCORE_BYTES as u64)
            .min(capacity_sample_cap);
        let durable_bulk_sample_floor = evidence_inflight_hi
            .max((PATH_OPEN_SCORE_BYTES as u64).saturating_mul(4))
            .min(capacity_sample_cap);
        let mut publishable_sample = None;
        if timed_non_app_limited_evidence {
            let publish_floor = if self.delivery_sample_count == 0 {
                preconfidence_publish_floor
            } else {
                durable_bulk_sample_floor
            };
            let pending = self
                .pending_delivery_sample
                .get_or_insert(PendingDeliverySampleEpoch {
                    key: delivery_clock_key,
                    sample_bytes: 0,
                    sample_count: 0,
                    sample_elapsed: Duration::ZERO,
                    publish_floor,
                    durable_floor: durable_bulk_sample_floor,
                });
            debug_assert_eq!(pending.key, delivery_clock_key);
            pending.sample_bytes = pending
                .sample_bytes
                .saturating_add(timed_non_app_limited_delivery_evidence_bytes);
            pending.sample_count = pending.sample_count.saturating_add(
                delivery_clock_fragment
                    .timed_delivery_evidence_sample_count
                    .max(u64::from(timed_non_app_limited_delivery_evidence_bytes > 0)),
            );
            pending.sample_elapsed = pending
                .sample_elapsed
                .saturating_add(delivery_evidence_ack_elapsed.unwrap_or_default());
            if pending.sample_bytes >= pending.publish_floor {
                publishable_sample = self.pending_delivery_sample.take();
            }
        }

        let mut latest_delivery_sample_bytes = 0;
        let mut latest_delivery_sample_count = 0;
        let mut latest_carrier_ack_elapsed = None;
        let mut latest_rate_sample_elapsed = None;
        if let Some(publishable_sample) = publishable_sample {
            let publishable_sample_bytes = publishable_sample.sample_bytes;
            let publishable_sample_count = publishable_sample.sample_count;
            let mut publishable_sample_elapsed = publishable_sample.sample_elapsed;
            latest_delivery_sample_bytes = publishable_sample_bytes;
            latest_delivery_sample_count = publishable_sample_count;
            latest_carrier_ack_elapsed = Some(publishable_sample_elapsed);
            publishable_sample_elapsed = publishable_sample_elapsed.max(QUIC_TIMER_GRANULARITY);
            latest_rate_sample_elapsed = Some(publishable_sample_elapsed);
            let sample_rate = (publishable_sample_bytes as f64 * 8.0
                / publishable_sample_elapsed.as_secs_f64())
            .max(1.0);
            let delivery_evidence_floor = if self.delivery_sample_count == 0 {
                preconfidence_publish_floor
            } else {
                evidence_inflight_hi
            };
            let previous_sample_count = self.delivery_sample_count;
            let next_sample_bytes = self
                .delivery_sample_bytes
                .saturating_add(publishable_sample_bytes);
            // Carrier-timed fragments are aggregated into full transport
            // windows, so poll boundaries cannot create or refresh a proof.
            let refreshes_bulk_proof = publishable_sample_bytes >= publishable_sample.durable_floor;
            let estimated_rate = self.delivery_rate_bps.unwrap_or(fallback_rate).max(1.0);
            let current_rate = if self.delivery_sample_count < confidence_sample_floor {
                estimated_rate.max(fallback_rate)
            } else {
                estimated_rate
            };
            let bounded_sample = sample_rate.min(current_rate * RELIABLE_PIPE_WINDOW_BDPS);
            let candidate_sample_count = self
                .delivery_sample_count
                .saturating_add(publishable_sample_count);
            let confidence_has_byte_volume =
                next_sample_bytes >= delivery_evidence_floor.max(PATH_OPEN_SCORE_BYTES as u64);
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
            if refreshes_bulk_proof {
                self.last_delivery_sample_at = Some(now);
                self.bulk_proof_expires_at = now.checked_add(bulk_proof_freshness_horizon);
                self.delivery_rate_bps = Some(match self.delivery_rate_bps {
                    Some(_) | None if establishes_measured_rate => bounded_sample,
                    Some(previous) if bounded_sample > previous => {
                        previous.mul_add(0.25, bounded_sample * 0.75)
                    }
                    // A stale overestimate can misplace a whole response flow,
                    // so full lower windows get the same 75% new-sample weight.
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
        UdpPathMetrics {
            direction,
            srtt: rtt,
            rttvar,
            rtt_observed: stats.path.rtt > Duration::ZERO,
            delivery_rate_bps,
            pacing_rate_bps,
            inflight_hi,
            bytes_in_flight: usize::try_from(bytes_in_flight).unwrap_or(usize::MAX),
            pending_bytes: usize::try_from(carrier_committed_bytes).unwrap_or(usize::MAX),
            loss_ppm: congestion.loss_ppm,
            ecn_ppm: congestion.ecn_ppm,
            // Preserve Quinn's congestion-controller state. Bulk-rate proof
            // freshness is a separate timestamp and must not redefine the
            // standard app-limited signal used to qualify rate samples.
            app_limited: congestion.app_limited,
            ack_derived_data_seen: self.ack_derived_data_seen,
            delivery_sample_count: self.delivery_sample_count,
            delivery_sample_bytes: self.delivery_sample_bytes,
            last_delivery_sample_at: self.last_delivery_sample_at,
            bulk_proof_expires_at: self.bulk_proof_expires_at,
            latest_delivery_sample_bytes,
            latest_delivery_sample_count,
            latest_carrier_ack_elapsed,
            latest_rate_sample_elapsed,
            #[cfg(feature = "lab-diagnostics")]
            ack_poll: QuicAckPollDiagnostics {
                newly_lost_bytes,
                newly_acked_bytes,
                non_app_limited_acked_bytes,
                timed_non_app_limited_acked_bytes,
                ack_elapsed: carrier_ack_elapsed.unwrap_or_default(),
                delivery_sample_count: congestion.delivery_sample_count,
                non_app_limited_sample_count: congestion.non_app_limited_delivery_sample_count,
                timed_non_app_limited_sample_count: delivery_clock_fragment.timed_sample_count,
                carrier_app_limited: congestion.app_limited,
                delivery_evidence_written_delta: delivery_evidence_delta,
                delivery_evidence_newly_acked_bytes,
                delivery_evidence_pending_ack_bytes: self.delivery_evidence_pending_ack_bytes,
                pending_sample_bytes: self
                    .pending_delivery_sample
                    .map_or(0, |pending| pending.sample_bytes),
                pending_sample_count: self
                    .pending_delivery_sample
                    .map_or(0, |pending| pending.sample_count),
                pending_sample_elapsed: self
                    .pending_delivery_sample
                    .map_or(Duration::ZERO, |pending| pending.sample_elapsed),
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
