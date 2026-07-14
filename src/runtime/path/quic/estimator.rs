//! QUIC ACK and delivery-rate estimator state.
//!
//! This module turns native carrier snapshots into typed path evidence; it does
//! not publish health or choose paths.

use super::io::UdpPathConnection;
#[cfg(feature = "lab-diagnostics")]
use super::metrics::QuicAckPollDiagnostics;
use super::metrics::UdpPathMetrics;
use super::*;
use crate::model::capacity::QuicCapacityProofCandidate;
use crate::runtime::stream::response::quic_capacity_receipt_rate_bps;

#[derive(Debug, Default)]
pub(super) struct UdpPathMetricTracker {
    quic: QuicPathMetricTracker,
}

impl UdpPathMetricTracker {
    pub(super) fn accept_capacity_proof(
        &mut self,
        metrics: &mut UdpPathMetrics,
        candidate: QuicCapacityProofCandidate,
    ) {
        self.quic.accept_capacity_proof(metrics, candidate);
    }

    pub(super) fn retire_capacity_candidate(&mut self, token: u64) {
        self.quic.retire_capacity_candidate(token);
    }

    pub(super) fn terminal_capacity_probe_to_retire(
        &mut self,
        probe: Option<quic_transport::MeasurementMetrics>,
        now: Instant,
    ) -> Option<u64> {
        self.quic.terminal_capacity_probe_to_retire(probe, now)
    }

    #[cfg(test)]
    pub(super) fn observe(
        &mut self,
        stats: quinn::ConnectionStats,
        congestion: quic_transport::CongestionMetrics,
        direction: u8,
    ) -> UdpPathMetrics {
        self.quic.observe(stats, congestion, direction)
    }

    #[cfg(test)]
    pub(super) fn observe_at(
        &mut self,
        stats: quinn::ConnectionStats,
        congestion: quic_transport::CongestionMetrics,
        direction: u8,
        now: Instant,
    ) -> UdpPathMetrics {
        self.quic.observe_at(stats, congestion, direction, now)
    }
}

#[derive(Debug, Default)]
struct QuicPathMetricTracker {
    last_delivery_evidence_written_bytes: u64,
    delivery_evidence_pending_ack_bytes: u64,
    delivery_rate_bps: Option<f64>,
    ack_derived_data_seen: bool,
    pending_non_app_limited_sample_bytes: u64,
    pending_non_app_limited_sample_count: u64,
    pending_non_app_limited_sample_elapsed: Duration,
    delivery_sample_count: u64,
    delivery_sample_bytes: u64,
    last_delivery_sample_at: Option<Instant>,
    bulk_proof_expires_at: Option<Instant>,
    // Carrier snapshots are cumulative and sticky. Remember registry acceptance,
    // not observation, so a transient publication race may retry the same token.
    last_accepted_capacity_probe_token: Option<u64>,
    pending_capacity_proof_candidate: Option<QuicCapacityProofCandidate>,
    min_rtt: Option<Duration>,
}

impl UdpPathConnection {
    pub(super) async fn tx_metrics(
        &self,
        tracker: &mut UdpPathMetricTracker,
        direction: u8,
    ) -> Option<UdpPathMetrics> {
        let stats = self.connection.stats();
        let congestion = self.connection.congestion_metrics();
        Some(tracker.quic.observe(stats, congestion, direction))
    }
}

impl QuicPathMetricTracker {
    fn expire_stale_bulk_proof(&mut self, now: Instant) {
        let proof_is_stale = self
            .bulk_proof_expires_at
            .is_some_and(|expires_at| now >= expires_at);
        if !proof_is_stale {
            return;
        }

        // The deadline owns placement authority, not estimator history. Keep
        // the measured rate/sample state for scheduling; `app_limited` and age
        // prevent it from silently renewing the expired right.
        self.bulk_proof_expires_at = None;
    }

    fn capacity_proof_candidate(
        &mut self,
        probe: Option<quic_transport::MeasurementMetrics>,
        now: Instant,
    ) -> Option<QuicCapacityProofCandidate> {
        let probe = probe?;
        if self.last_accepted_capacity_probe_token == Some(probe.token) {
            return None;
        }
        // Receipt and a committed write atomically terminalize the carrier
        // epoch. Accepting the same fields in a nonterminal phase hides a broken
        // carrier transition rather than preserving useful proof.
        if probe.phase != quic_transport::MeasurementPhase::Complete {
            return None;
        }
        if let Some(candidate) = self
            .pending_capacity_proof_candidate
            .filter(|candidate| candidate.token == probe.token)
        {
            return (now < candidate.expires_at).then_some(candidate);
        }
        if !probe.write_committed
            || probe.train_payload_bytes == 0
            || probe.written_payload_bytes != probe.train_payload_bytes
            || probe.written_data_frame_count == 0
            || probe.required_timed_carrier_bytes == 0
            || probe.required_timed_carrier_bytes
                != probe.sample_floor_bytes.saturating_sub(
                    (PATH_OPEN_SCORE_BYTES as u64).min(probe.sample_floor_bytes / 8),
                )
            || probe.sample_floor_bytes > probe.train_payload_bytes
            || probe
                .warmup_carrier_bytes
                .saturating_add(probe.required_timed_carrier_bytes)
                > probe.train_payload_bytes
            || probe.retention.is_zero()
            || probe.receipt_received_payload_bytes != probe.train_payload_bytes
        {
            return None;
        }
        let receipt_at = probe.receipt_at?;
        if probe.confirmed_at != Some(receipt_at) || receipt_at >= probe.expires_at {
            return None;
        }
        let receipt_elapsed = probe.receipt_elapsed.filter(|elapsed| !elapsed.is_zero())?;
        // Receipt time owns both the service interval and proof lifetime.
        // Use its full cold-start interval: subtracting an RTT can create an
        // unstable near-zero denominator, while native timing keeps changing.
        let proof_elapsed = receipt_elapsed.max(QUIC_TIMER_GRANULARITY);
        let expires_at = receipt_at.checked_add(probe.retention)?;
        if now >= expires_at {
            return None;
        }
        let rate_bps = quic_capacity_receipt_rate_bps(probe.train_payload_bytes, proof_elapsed)?;
        let candidate = QuicCapacityProofCandidate {
            token: probe.token,
            train_bytes: probe.train_payload_bytes,
            sample_floor_bytes: probe.sample_floor_bytes,
            accounting_slack_bytes: probe
                .sample_floor_bytes
                .saturating_sub(probe.required_timed_carrier_bytes),
            warmup_bytes: probe.warmup_carrier_bytes,
            required_proof_bytes: probe.required_timed_carrier_bytes,
            written_bytes: probe.written_payload_bytes,
            written_data_frame_count: probe.written_data_frame_count,
            receipt_confirmed: true,
            received_bytes: probe.receipt_received_payload_bytes,
            proof_elapsed,
            rate_bps,
            accepted_at: receipt_at,
            expires_at,
            proof_validity: probe.retention,
        };
        // Freeze rate and freshness on first sight. Repeated registry attempts
        // reuse this exact candidate instead of extending a delayed proof.
        self.pending_capacity_proof_candidate = Some(candidate);
        (now < expires_at).then_some(candidate)
    }

    fn accept_capacity_proof(
        &mut self,
        _metrics: &mut UdpPathMetrics,
        candidate: QuicCapacityProofCandidate,
    ) {
        debug_assert_ne!(
            self.last_accepted_capacity_probe_token,
            Some(candidate.token)
        );
        self.last_accepted_capacity_probe_token = Some(candidate.token);
        self.pending_capacity_proof_candidate = None;
    }

    fn terminal_capacity_probe_to_retire(
        &self,
        probe: Option<quic_transport::MeasurementMetrics>,
        now: Instant,
    ) -> Option<u64> {
        let probe = probe?;
        match probe.phase {
            quic_transport::MeasurementPhase::Expired
            | quic_transport::MeasurementPhase::Aborted => Some(probe.token),
            quic_transport::MeasurementPhase::Complete => {
                if self.last_accepted_capacity_probe_token == Some(probe.token) {
                    return Some(probe.token);
                }
                match self.pending_capacity_proof_candidate {
                    Some(candidate) if candidate.token == probe.token => {
                        (now >= candidate.expires_at).then_some(probe.token)
                    }
                    // A terminal snapshot that cannot form a proof must not
                    // retain the exclusive carrier epoch indefinitely.
                    _ => Some(probe.token),
                }
            }
            quic_transport::MeasurementPhase::Writing
            | quic_transport::MeasurementPhase::Measuring
            | quic_transport::MeasurementPhase::AwaitingReceipt => None,
        }
    }

    fn retire_capacity_candidate(&mut self, token: u64) {
        if self
            .pending_capacity_proof_candidate
            .is_some_and(|candidate| candidate.token == token)
        {
            self.pending_capacity_proof_candidate = None;
        }
    }

    fn observe(
        &mut self,
        stats: quinn::ConnectionStats,
        congestion: quic_transport::CongestionMetrics,
        direction: u8,
    ) -> UdpPathMetrics {
        self.observe_at(stats, congestion, direction, Instant::now())
    }

    fn observe_at(
        &mut self,
        stats: quinn::ConnectionStats,
        congestion: quic_transport::CongestionMetrics,
        direction: u8,
        now: Instant,
    ) -> UdpPathMetrics {
        let delivery_evidence_delta = congestion
            .delivery_evidence_written_bytes
            .saturating_sub(self.last_delivery_evidence_written_bytes);
        self.last_delivery_evidence_written_bytes = congestion.delivery_evidence_written_bytes;
        self.delivery_evidence_pending_ack_bytes = self
            .delivery_evidence_pending_ack_bytes
            .saturating_add(delivery_evidence_delta);

        if stats.path.rtt > Duration::ZERO {
            self.min_rtt = Some(
                self.min_rtt
                    .map_or(stats.path.rtt, |previous| previous.min(stats.path.rtt)),
            );
        }
        let rtt = stats.path.rtt.max(QUIC_TIMER_GRANULARITY);
        let rttvar = rtt / 4;
        let min_rtt = self.min_rtt.unwrap_or(rtt);
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
        let startup_rate = default_path_rate_bps(UnderlayProtocol::Udp);
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
        let non_app_limited_acked_bytes = congestion
            .non_app_limited_acked_bytes
            .unwrap_or(0)
            .min(newly_acked_bytes);
        let timed_non_app_limited_acked_bytes = congestion
            .timed_non_app_limited_acked_bytes
            .unwrap_or(0)
            .min(non_app_limited_acked_bytes);
        let delivery_evidence_pending_before_ack = self.delivery_evidence_pending_ack_bytes;
        let delivery_evidence_newly_acked_bytes =
            newly_acked_bytes.min(delivery_evidence_pending_before_ack);
        let timed_non_app_limited_delivery_evidence_bytes =
            timed_non_app_limited_acked_bytes.min(delivery_evidence_newly_acked_bytes);
        let carrier_ack_elapsed = congestion
            .non_app_limited_ack_elapsed
            .filter(|elapsed| !elapsed.is_zero());
        let timed_non_app_limited_evidence =
            carrier_ack_elapsed.is_some() && timed_non_app_limited_delivery_evidence_bytes > 0;
        // A first/compressed zero-span ACK batch proves reachability but has no
        // carrier-clock denominator and therefore cannot enter the rate model.
        if delivery_evidence_newly_acked_bytes > 0 {
            self.ack_derived_data_seen = true;
            self.delivery_evidence_pending_ack_bytes = self
                .delivery_evidence_pending_ack_bytes
                .saturating_sub(delivery_evidence_newly_acked_bytes);
        }
        // Generic evidence counts product payload only. The connection-wide
        // pending/flight counters still include an exclusive capacity train so
        // scheduling cannot treat carrier debt as an empty path.
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
        let mut publishable_sample_bytes = if timed_non_app_limited_evidence {
            timed_non_app_limited_delivery_evidence_bytes
        } else {
            0
        };
        let mut publishable_sample_count = timed_non_app_limited_evidence
            .then_some(congestion.timed_non_app_limited_delivery_sample_count)
            .unwrap_or(0)
            .max(u64::from(publishable_sample_bytes > 0));
        let mut publishable_sample_elapsed = carrier_ack_elapsed.unwrap_or_default();
        if publishable_sample_bytes > 0 {
            self.pending_non_app_limited_sample_bytes = self
                .pending_non_app_limited_sample_bytes
                .saturating_add(publishable_sample_bytes);
            self.pending_non_app_limited_sample_count = self
                .pending_non_app_limited_sample_count
                .saturating_add(publishable_sample_count);
            self.pending_non_app_limited_sample_elapsed = self
                .pending_non_app_limited_sample_elapsed
                .saturating_add(publishable_sample_elapsed);
            let publish_floor = if self.delivery_sample_count == 0 {
                preconfidence_publish_floor
            } else {
                durable_bulk_sample_floor
            };
            if self.pending_non_app_limited_sample_bytes < publish_floor {
                publishable_sample_bytes = 0;
                if self.delivery_evidence_pending_ack_bytes == 0 {
                    if self.delivery_sample_count > 0 {
                        let next_sample_bytes = self
                            .delivery_sample_bytes
                            .saturating_add(self.pending_non_app_limited_sample_bytes);
                        let candidate_sample_count = self
                            .delivery_sample_count
                            .saturating_add(self.pending_non_app_limited_sample_count);
                        let confidence_has_byte_volume = next_sample_bytes
                            >= evidence_inflight_hi.max(PATH_OPEN_SCORE_BYTES as u64);
                        self.delivery_sample_count = if self.delivery_sample_count
                            < confidence_sample_floor
                            && candidate_sample_count >= confidence_sample_floor
                            && !confidence_has_byte_volume
                        {
                            confidence_sample_floor.saturating_sub(1)
                        } else {
                            candidate_sample_count
                        };
                        self.delivery_sample_bytes = next_sample_bytes;
                    }
                    self.pending_non_app_limited_sample_bytes = 0;
                    self.pending_non_app_limited_sample_count = 0;
                    self.pending_non_app_limited_sample_elapsed = Duration::ZERO;
                }
            } else {
                publishable_sample_bytes = self.pending_non_app_limited_sample_bytes;
                publishable_sample_count = self.pending_non_app_limited_sample_count;
                publishable_sample_elapsed = self.pending_non_app_limited_sample_elapsed;
                self.pending_non_app_limited_sample_bytes = 0;
                self.pending_non_app_limited_sample_count = 0;
                self.pending_non_app_limited_sample_elapsed = Duration::ZERO;
            }
        } else if publishable_sample_bytes == 0 && self.delivery_evidence_pending_ack_bytes == 0 {
            self.pending_non_app_limited_sample_bytes = 0;
            self.pending_non_app_limited_sample_count = 0;
            self.pending_non_app_limited_sample_elapsed = Duration::ZERO;
        }

        let mut latest_delivery_sample_bytes = 0;
        let mut latest_delivery_sample_count = 0;
        let mut latest_carrier_ack_elapsed = None;
        let mut latest_rate_sample_elapsed = None;
        if publishable_sample_bytes > 0 {
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
            let refreshes_bulk_proof = publishable_sample_bytes >= durable_bulk_sample_floor;
            let estimated_rate = self.delivery_rate_bps.unwrap_or(fallback_rate).max(1.0);
            let current_rate = if self.delivery_sample_count < confidence_sample_floor {
                estimated_rate.max(fallback_rate)
            } else {
                estimated_rate
            };
            let bounded_sample = sample_rate.min(current_rate * BBR_DEFAULT_CWND_GAIN);
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

        let bulk_proof_is_fresh = self
            .bulk_proof_expires_at
            .is_some_and(|expires_at| now < expires_at);
        // Bulk eligibility follows a recent full transport window; an idle
        // connection retains ACK reachability without retaining placement.
        let app_limited = !bulk_proof_is_fresh;

        let estimated_rate = self.delivery_rate_bps.unwrap_or(fallback_rate).max(1.0);
        let delivery_rate_bps = if self.delivery_sample_count < confidence_sample_floor {
            estimated_rate.max(fallback_rate)
        } else {
            estimated_rate
        };
        let pacing_rate_bps = usable_pacing_rate
            .unwrap_or(delivery_rate_bps)
            .max(delivery_rate_bps);
        let capacity_proof_candidate = self.capacity_proof_candidate(congestion.measurement, now);
        UdpPathMetrics {
            direction,
            srtt: rtt,
            rttvar,
            min_rtt,
            min_rtt_observed: stats.path.rtt > Duration::ZERO,
            delivery_rate_bps,
            pacing_rate_bps,
            inflight_hi,
            bytes_in_flight: usize::try_from(bytes_in_flight).unwrap_or(usize::MAX),
            pending_bytes: usize::try_from(carrier_committed_bytes).unwrap_or(usize::MAX),
            loss_ppm: congestion.loss_ppm,
            ecn_ppm: congestion.ecn_ppm,
            app_limited,
            ack_derived_data_seen: self.ack_derived_data_seen,
            delivery_sample_count: self.delivery_sample_count,
            delivery_sample_bytes: self.delivery_sample_bytes,
            last_delivery_sample_at: self.last_delivery_sample_at,
            bulk_proof_expires_at: self.bulk_proof_expires_at,
            latest_delivery_sample_bytes,
            latest_delivery_sample_count,
            latest_carrier_ack_elapsed,
            latest_rate_sample_elapsed,
            capacity_proof_candidate,
            capacity_probe: congestion.measurement,
            #[cfg(feature = "lab-diagnostics")]
            ack_poll: QuicAckPollDiagnostics {
                newly_acked_bytes,
                non_app_limited_acked_bytes,
                timed_non_app_limited_acked_bytes,
                ack_elapsed: carrier_ack_elapsed.unwrap_or_default(),
                delivery_sample_count: congestion.delivery_sample_count,
                non_app_limited_sample_count: congestion.non_app_limited_delivery_sample_count,
                timed_non_app_limited_sample_count: congestion
                    .timed_non_app_limited_delivery_sample_count,
                carrier_app_limited: congestion.app_limited,
                delivery_evidence_written_delta: delivery_evidence_delta,
                delivery_evidence_newly_acked_bytes,
                delivery_evidence_pending_ack_bytes: self.delivery_evidence_pending_ack_bytes,
                pending_sample_bytes: self.pending_non_app_limited_sample_bytes,
                pending_sample_count: self.pending_non_app_limited_sample_count,
                pending_sample_elapsed: self.pending_non_app_limited_sample_elapsed,
            },
        }
    }
}

#[cfg(test)]
#[path = "estimator_capacity_test.rs"]
mod capacity_tests;
#[cfg(test)]
#[path = "estimator_lifecycle_test.rs"]
mod lifecycle_tests;
#[cfg(test)]
#[path = "estimator_rate_test.rs"]
mod rate_tests;
