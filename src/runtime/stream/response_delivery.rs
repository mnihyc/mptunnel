//! Exact product-range flight and STREAM_ACK ordering ownership.
//! This layer linearizes product ACK release and flight identity; carrier ACK
//! and packet recovery remain below it, while sender ranking remains above it.

use super::ResponseStreamBinding;
use super::response_ack_clock::{ResponseAckClockRateEvidence, ResponseAckClockRateEvidenceUpdate};
use super::response_topology::{
    ResponseSenderPathTarget, ResponseStreamOutputs, TcpResponseCapacityPrior,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{
    PathRateSample, TRANSPORT_TIMER_GRANULARITY, product_delivery_samples_override_startup_prior,
};
use crate::model::path::CarrierPathKey;
use crate::model::work::CarrierWorkKind;
use crate::protocol::{Frame, OffsetRange, StreamOpenRole, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::ReliablePathCommandSender;
use crate::runtime::relay_striping::reliable_stream_frame_extent;
use crate::scheduler::{FlowLane, PathSnapshot};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// Product byte range currently assigned to a carrier path.
///
/// STREAM_ACK releases this ledger entry from product flight; carrier ACKs only
/// update carrier/path evidence and must not release product repair state.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct CarrierPathFlight {
    pub(super) key: CarrierPathKey,
    pub(super) output_incarnation: u64,
    pub(super) end: u64,
    pub(super) bytes: usize,
    pub(super) sent_at: Instant,
    pub(super) kind: CarrierWorkKind,
    pub(super) evidence_eligible: bool,
}

impl CarrierPathFlight {
    pub(in crate::runtime::stream) fn fixed_output(
        key: CarrierPathKey,
        end: u64,
        bytes: usize,
        sent_at: Instant,
        kind: CarrierWorkKind,
    ) -> Self {
        Self {
            key,
            output_incarnation: 0,
            end,
            bytes,
            sent_at,
            kind,
            evidence_eligible: true,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct CarrierPathReleasedFlight {
    pub(super) flight: CarrierPathFlight,
    pub(super) path_proving: bool,
}

impl CarrierPathReleasedFlight {
    pub(in crate::runtime::stream) fn fixed_output_sample(self) -> (usize, Instant, bool) {
        (self.flight.bytes, self.flight.sent_at, self.path_proving)
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct CarrierPathFlightDebt {
    pub(in crate::runtime) key: CarrierPathKey,
    pub(in crate::runtime) bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct CarrierPathAckedHole {
    pub(super) key: CarrierPathKey,
    pub(super) output_incarnation: u64,
    pub(super) end: u64,
    pub(super) bytes: u64,
    pub(super) kind: CarrierWorkKind,
    pub(super) path_proving: bool,
}

#[derive(Debug, Default)]
pub(in crate::runtime) struct ResponseAckOrderingState {
    pub(super) contiguous_frontier: u64,
    pub(super) acked_holes: BTreeMap<u64, Vec<CarrierPathAckedHole>>,
}

pub(in crate::runtime) struct ResponseAckOrderingUpdate {
    pub(super) changed: bool,
    pub(super) contiguous_frontier: u64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(super) acked_hole_bytes: u64,
    pub(super) newly_contiguous: Vec<CarrierPathAckedHole>,
}

impl ResponseAckOrderingState {
    pub(super) fn apply_normalized_ack(
        &mut self,
        ranges: &[OffsetRange],
        released: &[(u64, CarrierPathReleasedFlight)],
    ) -> ResponseAckOrderingUpdate {
        let previous_frontier = self.contiguous_frontier;
        let previous_hole_bytes = self.acked_hole_bytes();
        let mut newly_contiguous = Vec::new();

        for (offset, release) in released {
            let flight = release.flight;
            let hole = CarrierPathAckedHole {
                key: flight.key,
                output_incarnation: flight.output_incarnation,
                end: flight.end,
                bytes: flight.bytes as u64,
                kind: flight.kind,
                path_proving: release.path_proving,
            };
            if hole.end <= self.contiguous_frontier {
                newly_contiguous.push(hole);
            } else {
                self.acked_holes.entry(*offset).or_default().push(hole);
            }
        }

        self.advance_contiguous_frontier(ranges);
        let frontier = self.contiguous_frontier;
        self.acked_holes.retain(|_, holes| {
            holes.retain(|hole| {
                if hole.end <= frontier {
                    newly_contiguous.push(*hole);
                    false
                } else {
                    true
                }
            });
            !holes.is_empty()
        });
        let acked_hole_bytes = self.acked_hole_bytes();

        ResponseAckOrderingUpdate {
            changed: previous_frontier != self.contiguous_frontier
                || previous_hole_bytes != acked_hole_bytes
                || !newly_contiguous.is_empty(),
            contiguous_frontier: self.contiguous_frontier,
            acked_hole_bytes,
            newly_contiguous,
        }
    }

    fn advance_contiguous_frontier(&mut self, ranges: &[OffsetRange]) {
        loop {
            let mut next_frontier = self.contiguous_frontier;
            for range in ranges {
                if range.start > next_frontier {
                    break;
                }
                if range.end > next_frontier {
                    next_frontier = range.end;
                }
            }
            for (offset, holes) in self.acked_holes.range(..=next_frontier) {
                if *offset > next_frontier {
                    break;
                }
                for hole in holes {
                    if hole.end > next_frontier {
                        next_frontier = hole.end;
                    }
                }
            }
            if next_frontier == self.contiguous_frontier {
                break;
            }
            self.contiguous_frontier = next_frontier;
        }
    }

    pub(super) fn acked_hole_bytes(&self) -> u64 {
        self.acked_holes
            .values()
            .filter_map(|holes| response_latest_ordering_hole(holes))
            .map(|hole| hole.bytes)
            .sum()
    }
}

pub(in crate::runtime) fn response_latest_ordering_hole(
    holes: &[CarrierPathAckedHole],
) -> Option<&CarrierPathAckedHole> {
    holes
        .iter()
        .rev()
        .find(|hole| hole.kind.is_ordering_owner())
}

impl ResponseStreamBinding {
    pub(in crate::runtime) fn tail_repair_snapshot(
        &self,
        ack_frontier: u64,
        lane: FlowLane,
    ) -> Option<PathSnapshot> {
        let owner_key = self
            .blocking_owner_key_at_or_after(ack_frontier)
            .or_else(|| self.ordered_data_owner());
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        owner_key.and_then(|key| {
            outputs.snapshot_for_key(
                key,
                self.session_id,
                &self.lane_tracker,
                lane,
                self.mux_limits,
            )
        })
    }

    pub(in crate::runtime) fn tail_repair_owner_underlay(
        &self,
        ack_frontier: u64,
    ) -> Option<UnderlayProtocol> {
        self.blocking_owner_key_at_or_after(ack_frontier)
            .or_else(|| self.ordered_data_owner())
            .map(|key| key.underlay)
    }

    pub(in crate::runtime) fn has_multipath_repair_alternative(&self) -> bool {
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .len()
            > 1
    }

    pub(in crate::runtime) fn has_repair_output_for_frame(&self, frame: &Frame) -> bool {
        let avoid_keys = self.flight_keys_overlapping_frame(frame);
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        outputs
            .entries
            .iter()
            .any(|entry| !avoid_keys.contains(&entry.key))
    }

    pub(in crate::runtime) fn has_live_owner_tail_repair_output_for_frame(
        &self,
        frame: &Frame,
    ) -> bool {
        let owner_keys = self.owner_flight_keys_overlapping_frame(frame);
        if owner_keys.is_empty() {
            return false;
        }
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .any(|entry| !owner_keys.contains(&entry.key))
    }

    pub(in crate::runtime) fn has_recent_live_repair_flight_overlap(
        &self,
        frame: &Frame,
        retry_after: Duration,
    ) -> bool {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return false;
        };
        let now = Instant::now();
        let live_keys = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .map(|entry| entry.key)
            .collect::<Vec<_>>();
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        product_flights_have_recent_repair_overlap(&flights, start, end, now, retry_after, |key| {
            live_keys.contains(&key)
        })
    }

    pub(in crate::runtime) fn has_failed_owner_repair_output_for_frame(
        &self,
        frame: &Frame,
    ) -> bool {
        let avoid_keys = self.flight_keys_overlapping_frame(frame);
        if avoid_keys.is_empty() {
            return false;
        }
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let recorded_output_still_live = outputs
            .entries
            .iter()
            .any(|entry| avoid_keys.contains(&entry.key));
        !recorded_output_still_live
            && outputs
                .entries
                .iter()
                .any(|entry| !avoid_keys.contains(&entry.key))
    }

    pub(in crate::runtime) fn has_unknown_owner_repair_output_for_frame(
        &self,
        frame: &Frame,
    ) -> bool {
        if !self.flight_keys_overlapping_frame(frame).is_empty()
            || self.ordered_data_owner().is_some()
        {
            return false;
        }
        !self
            .outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .is_empty()
    }

    pub(in crate::runtime) fn release_normalized_acked_ranges(&self, ranges: &[OffsetRange]) {
        self.release_normalized_acked_ranges_at(ranges, Instant::now());
    }

    pub(super) fn release_normalized_acked_ranges_at(&self, ranges: &[OffsetRange], now: Instant) {
        if ranges.is_empty() {
            return;
        }
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let mut flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        let released = release_carrier_path_flight_ranges(&mut flights, ranges);
        if released.is_empty() {
            drop(flights);
            let ordering_update = self
                .ack_ordering
                .lock()
                .expect("server response ACK ordering lock")
                .apply_normalized_ack(ranges, &[]);
            if ordering_update.changed {
                // Publish the generation only after the coherent ordering view
                // exists. Duplicate ACKs need no fence or shared atomic write.
                self.response_model_generation
                    .fetch_add(1, Ordering::AcqRel);
            }
            drop(outputs);
            if ordering_update.changed {
                self.notify_update();
            }
            return;
        }
        let active_calibration_has_owner_flights = outputs
            .active_ack_clock_calibration
            .is_some_and(|(active_key, active_incarnation)| {
                flights.values().flatten().any(|flight| {
                    flight.key == active_key
                        && flight.output_incarnation == active_incarnation
                        && flight.kind.is_ordering_owner()
                })
            });
        drop(flights);

        let ordering_update = {
            let mut ordering = self
                .ack_ordering
                .lock()
                .expect("server response ACK ordering lock");
            ordering.apply_normalized_ack(ranges, &released)
        };
        #[cfg(feature = "lab-diagnostics")]
        if ordering_update.acked_hole_bytes > 0 {
            lab_diagnostic(
                "server_ack_ordering_state",
                format_args!(
                    "session_id={} contiguous_frontier={} acked_hole_bytes={} released_flights={}",
                    self.session_id.0,
                    ordering_update.contiguous_frontier,
                    ordering_update.acked_hole_bytes,
                    released.len(),
                ),
            );
        }

        let mut changed = false;
        let mut path_samples =
            HashMap::<(CarrierPathKey, u64), (u64, u64, Instant, Instant)>::new();
        for (_, release) in released {
            let flight = release.flight;
            let identity = (flight.key, flight.output_incarnation);
            let stage_authorized_at = outputs
                .ack_clock_calibrations
                .get(&identity)
                .map(|calibration| calibration.stage_authorized_at);
            if let Some(entry) = outputs.entries.iter_mut().find(|entry| {
                entry.key == flight.key && entry.incarnation == flight.output_incarnation
            }) {
                entry.bytes_in_flight = entry.bytes_in_flight.saturating_sub(flight.bytes as u64);
                if flight.kind.is_ordering_owner() {
                    entry.owner_data_in_flight_bytes = entry
                        .owner_data_in_flight_bytes
                        .saturating_sub(flight.bytes as u64);
                }
                if release.path_proving {
                    entry.owner_data_acked_bytes = entry
                        .owner_data_acked_bytes
                        .saturating_add(flight.bytes as u64);
                    let sample = path_samples.entry(identity).or_insert((
                        0_u64,
                        0_u64,
                        flight.sent_at,
                        flight.sent_at,
                    ));
                    sample.0 = sample.0.saturating_add(flight.bytes as u64);
                    if stage_authorized_at
                        .is_some_and(|authorized_at| flight.sent_at >= authorized_at)
                    {
                        sample.1 = sample.1.saturating_add(flight.bytes as u64);
                    }
                    sample.2 = sample.2.min(flight.sent_at);
                    sample.3 = sample.3.max(flight.sent_at);
                }
                changed = true;
            }
        }
        for ((key, output_incarnation), (bytes, fresh_bytes, first_sent_at, last_sent_at)) in
            path_samples
        {
            let identity = (key, output_incarnation);
            let ack_clock_update = if outputs.active_ack_clock_calibration == Some(identity) {
                outputs
                    .ack_clock_calibrations
                    .get_mut(&identity)
                    .filter(|calibration| {
                        calibration.spent_bytes > 0 && !calibration.proven && !calibration.retired
                    })
                    .map(|calibration| {
                        calibration
                            .rate_evidence
                            .get_or_insert_with(|| ResponseAckClockRateEvidence::new(first_sent_at))
                            .observe_with_fresh_bytes(
                                bytes,
                                fresh_bytes,
                                first_sent_at,
                                last_sent_at,
                                now,
                            )
                    })
            } else {
                None
            };
            let ack_clock_window = match ack_clock_update {
                Some(ResponseAckClockRateEvidenceUpdate::Proven {
                    sample,
                    bytes,
                    fresh_bytes,
                    first_window,
                    earliest_sent_at,
                    previous_window_acked_at,
                    latest_sent_at,
                }) => Some((
                    if first_window { None } else { sample },
                    bytes,
                    fresh_bytes,
                    first_window,
                    earliest_sent_at,
                    previous_window_acked_at,
                    latest_sent_at,
                )),
                _ => None,
            };
            let calibration_update = ack_clock_window.and_then(
                |(
                    strict_rate_sample,
                    window_bytes,
                    fresh_window_bytes,
                    first_window,
                    earliest_sent_at,
                    previous_ack_at,
                    latest_sent_at,
                )| {
                    outputs
                        .ack_clock_calibrations
                        .get_mut(&identity)
                        .map(|calibration| {
                            let sample_bps = strict_rate_sample
                                .map(PathRateSample::rate_bps)
                                .unwrap_or(0.0);
                            let sample_elapsed = strict_rate_sample
                                .map(PathRateSample::elapsed)
                                .unwrap_or(Duration::ZERO);
                            let previous_credit = calibration.credit_limit_bytes;
                            let stage_authorized_at = calibration.stage_authorized_at;
                            let stage_authorized_spent_bytes =
                                calibration.stage_authorized_spent_bytes;
                            let stage_credit_bytes = calibration.stage_credit_bytes();
                            let stage_window_eligible = earliest_sent_at >= stage_authorized_at;
                            let stage_rate_evidence_accepted = stage_window_eligible
                                && strict_rate_sample.is_some()
                                && fresh_window_bytes == window_bytes;
                            let stage_evidence_bytes = if stage_rate_evidence_accepted {
                                calibration
                                    .stage_rate_evidence_bytes
                                    .saturating_add(window_bytes)
                            } else {
                                calibration.stage_rate_evidence_bytes
                            };
                            let stage_evidence_elapsed = if stage_rate_evidence_accepted {
                                calibration
                                    .stage_rate_evidence_elapsed
                                    .saturating_add(sample_elapsed)
                            } else {
                                calibration.stage_rate_evidence_elapsed
                            };
                            let stage_rate_ineligible_bytes =
                                if fresh_window_bytes > 0 && !stage_rate_evidence_accepted {
                                    calibration
                                        .stage_rate_ineligible_bytes
                                        .saturating_add(fresh_window_bytes)
                                } else {
                                    calibration.stage_rate_ineligible_bytes
                                };
                            let stage_fully_spent =
                                calibration.spent_bytes >= calibration.credit_limit_bytes;
                            let stage_strict_capacity_bytes =
                                stage_credit_bytes.saturating_sub(stage_rate_ineligible_bytes);
                            let previous_stage_rate_sample_count =
                                calibration.stage_rate_sample_count();
                            let aggregate_rate_bps = (stage_fully_spent
                                && stage_strict_capacity_bytes
                                    >= calibration.stage_rate_coverage_floor_bytes
                                && stage_evidence_bytes
                                    >= calibration.stage_rate_coverage_floor_bytes)
                                .then(|| {
                                    stage_evidence_bytes as f64 * 8.0
                                        / stage_evidence_elapsed
                                            .max(TRANSPORT_TIMER_GRANULARITY)
                                            .as_secs_f64()
                                })
                                .unwrap_or(0.0);
                            let credit_grew = calibration.record_ack_clock_window(
                                strict_rate_sample,
                                window_bytes,
                                fresh_window_bytes,
                                earliest_sent_at,
                                now,
                            );
                            debug_assert_eq!(
                                credit_grew,
                                calibration.credit_limit_bytes > previous_credit
                            );
                            let stage_rate_sample_accepted = calibration.stage_rate_sample_count()
                                > previous_stage_rate_sample_count;
                            (
                                sample_bps,
                                *calibration,
                                credit_grew,
                                first_window,
                                strict_rate_sample.is_some(),
                                stage_window_eligible,
                                stage_rate_evidence_accepted,
                                stage_fully_spent,
                                stage_rate_sample_accepted,
                                window_bytes,
                                fresh_window_bytes,
                                sample_elapsed,
                                stage_evidence_bytes,
                                stage_evidence_elapsed,
                                stage_rate_ineligible_bytes,
                                calibration.stage_rate_coverage_floor_bytes,
                                stage_authorized_spent_bytes,
                                stage_credit_bytes,
                                stage_strict_capacity_bytes,
                                aggregate_rate_bps,
                                stage_authorized_at,
                                earliest_sent_at,
                                previous_ack_at,
                                latest_sent_at,
                            )
                        })
                },
            );
            let calibration_snapshot = outputs.ack_clock_calibrations.get(&identity).copied();
            let calibration_identity_active =
                outputs.active_ack_clock_calibration == Some(identity);
            if let Some(entry) = outputs
                .entries
                .iter_mut()
                .find(|entry| entry.key == key && entry.incarnation == output_incarnation)
            {
                let udp_assignment_sample = (entry.key.underlay == UnderlayProtocol::Udp)
                    .then(|| {
                        PathRateSample::new(bytes, now.saturating_duration_since(first_sent_at))
                    })
                    .flatten();
                // Flight timestamps mark scheduler assignment, not TCP kernel
                // dispatch. The first exact ACK establishes the clock; later
                // binding-local OwnerData bytes use continuous ACK wall time so
                // callback compression cannot discard the preceding silence.
                let (tcp_ack_clock_sample, tcp_ack_clock_window_complete) =
                    if entry.key.underlay == UnderlayProtocol::Tcp {
                        let evidence = entry.tcp_product_rate_evidence.get_or_insert_with(|| {
                            ResponseAckClockRateEvidence::new(first_sent_at)
                        });
                        let update = evidence.observe_with_fresh_bytes(
                            bytes,
                            bytes,
                            first_sent_at,
                            last_sent_at,
                            now,
                        );
                        (
                            evidence.goodput_sample(),
                            matches!(update, ResponseAckClockRateEvidenceUpdate::Proven { .. }),
                        )
                    } else {
                        (None, false)
                    };
                let carrier_app_limited = entry
                    .local_path_metrics
                    .is_some_and(|metrics| metrics.metrics.app_limited);
                let calibrated_rate_bps =
                    calibration_snapshot.and_then(|calibration| calibration.calibrated_rate_bps);
                let mut ordinary_tcp_rate_replaces_capacity_prior = false;
                if entry.key.underlay == UnderlayProtocol::Tcp
                    && !calibration_identity_active
                    && calibration_update.is_none()
                    && tcp_ack_clock_window_complete
                    && let Some(prior) = entry.tcp_capacity_prior.as_mut()
                {
                    prior.ordinary_windows = prior.ordinary_windows.saturating_add(1);
                    ordinary_tcp_rate_replaces_capacity_prior =
                        product_delivery_samples_override_startup_prior(prior.ordinary_windows)
                            && tcp_ack_clock_sample.is_some();
                }
                if ordinary_tcp_rate_replaces_capacity_prior {
                    entry.tcp_capacity_prior = None;
                }
                // A terminal calibration ACK remains exclusive, while either
                // capacity-prior source stays temporary until a fresh ordinary
                // exact-ACK epoch reaches the normal evidence threshold.
                let tcp_measurement_owns_rate = entry.key.underlay == UnderlayProtocol::Tcp
                    && (calibration_identity_active
                        || calibration_update.is_some()
                        || entry.tcp_capacity_prior.is_some()
                        || calibration_snapshot.is_some_and(|calibration| {
                            !calibration.proven && !calibration.retired
                        }));
                if !tcp_measurement_owns_rate {
                    match (
                        entry.key.underlay,
                        tcp_ack_clock_sample,
                        udp_assignment_sample,
                    ) {
                        (UnderlayProtocol::Tcp, Some(sample), _) => {
                            // The sample already smooths a bounded byte/time
                            // epoch. Averaging point rates would restore the ACK
                            // compression bias this ratio removes.
                            let rate_bps = sample.rate_bps();
                            entry.tcp_ack_clock_rate_bps = Some(rate_bps);
                            entry.product_progress_rate_bps = Some(rate_bps);
                            entry.delivery_rate_bps = Some(rate_bps);
                        }
                        (UnderlayProtocol::Udp, _, Some(sample)) => {
                            let sample_bps = sample.rate_bps();
                            entry.product_progress_rate_bps =
                                Some(match entry.product_progress_rate_bps {
                                    Some(previous) if carrier_app_limited => {
                                        previous.max(sample_bps)
                                    }
                                    Some(previous) => previous.mul_add(0.75, sample_bps * 0.25),
                                    None => sample_bps,
                                });
                        }
                        _ => {}
                    }
                }
                if entry.key.underlay == UnderlayProtocol::Tcp
                    && calibration_identity_active
                    && let Some(calibrated_rate_bps) = calibrated_rate_bps
                {
                    entry.product_progress_rate_bps = Some(calibrated_rate_bps);
                    entry.delivery_rate_bps = Some(calibrated_rate_bps);
                }
            }
            if let Some((
                sample_bps,
                calibration,
                credit_grew,
                first_window,
                strict_rate_window,
                stage_window_eligible,
                stage_rate_evidence_accepted,
                stage_fully_spent,
                stage_rate_sample_accepted,
                sample_bytes,
                fresh_sample_bytes,
                sample_elapsed,
                stage_evidence_bytes,
                stage_evidence_elapsed,
                stage_rate_ineligible_bytes,
                stage_rate_coverage_floor_bytes,
                stage_authorized_spent_bytes,
                stage_credit_bytes,
                stage_strict_capacity_bytes,
                aggregate_rate_bps,
                stage_authorized_at,
                earliest_sent_at,
                previous_ack_at,
                latest_sent_at,
            )) = calibration_update
            {
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = (
                    sample_bps,
                    calibration,
                    credit_grew,
                    first_window,
                    strict_rate_window,
                    stage_window_eligible,
                    stage_rate_evidence_accepted,
                    stage_fully_spent,
                    stage_rate_sample_accepted,
                    sample_bytes,
                    fresh_sample_bytes,
                    sample_elapsed,
                    stage_evidence_bytes,
                    stage_evidence_elapsed,
                    stage_rate_ineligible_bytes,
                    stage_rate_coverage_floor_bytes,
                    stage_authorized_spent_bytes,
                    stage_credit_bytes,
                    stage_strict_capacity_bytes,
                    aggregate_rate_bps,
                    stage_authorized_at,
                    earliest_sent_at,
                    previous_ack_at,
                    latest_sent_at,
                );
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "response_ack_clock_calibration",
                    format_args!(
                        "phase=ack_clock_window session_id={} binding_instance_id={} underlay={:?} path_id={} incarnation={} rate_bps={} sample_bytes={} fresh_sample_bytes={} sample_elapsed_us={} calibrated_rate_bps={} calibrated_rate_ready={} first_window={} strict_rate_window={} stage_window_eligible={} stage_rate_evidence_accepted={} stage_fully_spent={} stage_rate_sample_accepted={} stage_evidence_bytes={} stage_evidence_elapsed_us={} stage_rate_ineligible_bytes={} stage_rate_coverage_floor_bytes={} stage_authorized_spent_bytes={} stage_credit_bytes={} stage_strict_capacity_bytes={} aggregate_rate_bps={} spent_bytes={} credit_limit_bytes={} max_limit_bytes={} credit_grew={} proven={} stage_authorized_age_us={} earliest_sent_age_us={} previous_ack_age_us={} latest_sent_age_us={} stage_provenance_slack_us={} causal_slack_us={}",
                        self.session_id.0,
                        self.binding_instance_id,
                        key.underlay,
                        key.path_id.0,
                        output_incarnation,
                        sample_bps,
                        sample_bytes,
                        fresh_sample_bytes,
                        sample_elapsed.as_micros(),
                        calibration.calibrated_rate_bps.unwrap_or(0.0),
                        calibration.calibrated_rate_bps.is_some(),
                        first_window,
                        strict_rate_window,
                        stage_window_eligible,
                        stage_rate_evidence_accepted,
                        stage_fully_spent,
                        stage_rate_sample_accepted,
                        stage_evidence_bytes,
                        stage_evidence_elapsed.as_micros(),
                        stage_rate_ineligible_bytes,
                        stage_rate_coverage_floor_bytes,
                        stage_authorized_spent_bytes,
                        stage_credit_bytes,
                        stage_strict_capacity_bytes,
                        aggregate_rate_bps,
                        calibration.spent_bytes,
                        calibration.credit_limit_bytes,
                        calibration.max_limit_bytes,
                        credit_grew,
                        calibration.proven,
                        now.saturating_duration_since(stage_authorized_at)
                            .as_micros(),
                        now.saturating_duration_since(earliest_sent_at).as_micros(),
                        previous_ack_at.map_or(0, |acked_at| {
                            now.saturating_duration_since(acked_at).as_micros()
                        }),
                        now.saturating_duration_since(latest_sent_at).as_micros(),
                        earliest_sent_at
                            .saturating_duration_since(stage_authorized_at)
                            .as_micros(),
                        previous_ack_at.map_or(0, |acked_at| {
                            acked_at
                                .saturating_duration_since(latest_sent_at)
                                .as_micros()
                        }),
                    ),
                );
            }
        }
        if !active_calibration_has_owner_flights
            && let Some(identity) = outputs.active_ack_clock_calibration
        {
            let previous_credit = outputs
                .ack_clock_calibrations
                .get(&identity)
                .map_or(0, |calibration| calibration.credit_limit_bytes);
            let mut transition_snapshot = None;
            let (clear_active, terminal_reason) =
                match outputs.ack_clock_calibrations.get_mut(&identity) {
                    None => (true, "missing_state"),
                    Some(calibration) => {
                        if calibration.proven {
                            transition_snapshot = Some(*calibration);
                            (
                                true,
                                if calibration.calibrated_rate_bps.is_some() {
                                    "robust_rate"
                                } else {
                                    "hard_ceiling_no_rate"
                                },
                            )
                        } else if calibration.retired {
                            transition_snapshot = Some(*calibration);
                            (true, "retired_drain")
                        } else {
                            let previous_stage_rate_samples = calibration.stage_rate_sample_count();
                            if calibration.advance_drained_stage(now) {
                                let accepted_stage = calibration.stage_rate_sample_count()
                                    > previous_stage_rate_samples;
                                transition_snapshot = Some(*calibration);
                                (
                                    false,
                                    if accepted_stage {
                                        "drain_stage_advance"
                                    } else {
                                        "drain_reachability_topup"
                                    },
                                )
                            } else if calibration.proven {
                                transition_snapshot = Some(*calibration);
                                (
                                    true,
                                    if calibration.calibrated_rate_bps.is_some() {
                                        "robust_rate"
                                    } else {
                                        "hard_ceiling_no_rate"
                                    },
                                )
                            } else if calibration.spent_bytes >= calibration.max_limit_bytes {
                                transition_snapshot = Some(*calibration);
                                calibration.retire();
                                (true, "hard_ceiling_drain")
                            } else if calibration.spent_bytes >= calibration.credit_limit_bytes {
                                transition_snapshot = Some(*calibration);
                                calibration.retire();
                                (true, "under_covered_drain")
                            } else {
                                (false, "credit_remaining")
                            }
                        }
                    }
                };
            if clear_active || terminal_reason != "credit_remaining" {
                let terminal = transition_snapshot;
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "response_ack_clock_calibration",
                    format_args!(
                        "phase={} session_id={} binding_instance_id={} underlay={:?} path_id={} incarnation={} reason={} active_owner_flights=false calibrated_rate_ready={} calibrated_rate_bps={} spent_bytes={} previous_credit_limit_bytes={} credit_limit_bytes={} max_limit_bytes={} stage_authorized_spent_bytes={} stage_credit_bytes={} stage_strict_capacity_bytes={} stage_evidence_bytes={} stage_rate_ineligible_bytes={} proven={} retired={}",
                        if clear_active {
                            "terminal"
                        } else {
                            "drain_transition"
                        },
                        self.session_id.0,
                        self.binding_instance_id,
                        identity.0.underlay,
                        identity.0.path_id.0,
                        identity.1,
                        terminal_reason,
                        terminal.is_some_and(|state| state.calibrated_rate_bps.is_some()),
                        terminal
                            .and_then(|state| state.calibrated_rate_bps)
                            .unwrap_or(0.0),
                        terminal.map_or(0, |state| state.spent_bytes),
                        previous_credit,
                        terminal.map_or(0, |state| state.credit_limit_bytes),
                        terminal.map_or(0, |state| state.max_limit_bytes),
                        terminal.map_or(0, |state| state.stage_authorized_spent_bytes),
                        terminal.map_or(0, |state| state.stage_credit_bytes()),
                        terminal.map_or(0, |state| state.stage_strict_capacity_bytes()),
                        terminal.map_or(0, |state| state.stage_rate_evidence_bytes),
                        terminal.map_or(0, |state| state.stage_rate_ineligible_bytes),
                        terminal.is_some_and(|state| state.proven),
                        terminal.is_some_and(|state| state.retired),
                    ),
                );
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = (terminal_reason, terminal, previous_credit);
            };
            if clear_active {
                outputs.active_ack_clock_calibration = None;
                if let Some(entry) = outputs
                    .entries
                    .iter_mut()
                    .find(|entry| entry.key == identity.0 && entry.incarnation == identity.1)
                {
                    // Exclude bounded calibration traffic from the continuous
                    // ACK clock that will eventually replace its startup prior.
                    entry.tcp_product_rate_evidence = None;
                    entry.tcp_ack_clock_rate_bps = None;
                    entry.tcp_capacity_prior = transition_snapshot
                        .and_then(|state| state.calibrated_rate_bps)
                        .map(|rate_bps| TcpResponseCapacityPrior {
                            rate_bps,
                            ordinary_windows: 0,
                        });
                    if let Some(prior) = entry.tcp_capacity_prior {
                        entry.product_progress_rate_bps = Some(prior.rate_bps);
                        entry.delivery_rate_bps = Some(prior.rate_bps);
                    }
                }
            }
        }
        for hole in ordering_update.newly_contiguous {
            if !hole.path_proving {
                continue;
            }
            if let Some(entry) = outputs
                .entries
                .iter_mut()
                .find(|entry| entry.key == hole.key && entry.incarnation == hole.output_incarnation)
            {
                if hole.end <= ordering_update.contiguous_frontier {
                    entry.delivery_samples = entry.delivery_samples.saturating_add(1);
                    changed = true;
                }
            }
        }
        // A planner captures this before reading lower flights and path
        // snapshots. Publish it only after both ledgers describe the ACK.
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
        drop(outputs);
        if changed || ordering_update.changed {
            self.graduate_completed_response_startup_owner();
            // ACK progress updates path evidence and ordering, but Subflow
            // admission credit is epoch state. Recreate it only on a semantic
            // reset or admission-envelope change, not passive membership growth.
            self.notify_update();
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_owner_flight_for_target(
        &self,
        target: &ResponseSenderPathTarget,
        frame: &Frame,
    ) {
        self.record_product_flight(
            target.key,
            target.incarnation,
            target.attachment_role,
            &target.commands,
            frame,
            CarrierWorkKind::OwnerData,
        )
    }

    pub(super) fn record_validated_owner_flight_with_outputs(
        &self,
        outputs: &mut ResponseStreamOutputs,
        target_index: usize,
        frame: &Frame,
    ) {
        let Some((offset, end, bytes)) = reliable_stream_frame_extent(frame) else {
            return;
        };
        let (key, output_incarnation) = {
            let entry = outputs
                .entries
                .get_mut(target_index)
                .expect("validated response output index");
            debug_assert_ne!(entry.role, StreamOpenRole::Repair);
            entry.owner_data_in_flight_bytes = entry
                .owner_data_in_flight_bytes
                .saturating_add(bytes as u64);
            entry.bytes_in_flight = entry.bytes_in_flight.saturating_add(bytes as u64);
            (entry.key, entry.incarnation)
        };
        self.flights
            .lock()
            .expect("server reliable stream flight lock")
            .entry(offset)
            .or_default()
            .push(CarrierPathFlight {
                key,
                output_incarnation,
                end,
                bytes,
                sent_at: Instant::now(),
                kind: CarrierWorkKind::OwnerData,
                evidence_eligible: true,
            });
        // Keep path counters and the exact range ledger in one published model
        // generation so a concurrent calibration plan cannot mix the views.
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(in crate::runtime) fn try_enqueue_repair_frame_for_target(
        &self,
        target: &ResponseSenderPathTarget,
        frame: &Frame,
        lane: FlowLane,
    ) -> Result<(), RuntimeError> {
        if !self.response_stream_open.load(Ordering::Acquire) {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        if !self.response_stream_open.load(Ordering::Acquire) {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        let target_matches = outputs.entries.iter().any(|entry| {
            entry.key == target.key
                && entry.incarnation == target.incarnation
                && entry.commands.same_channel(&target.commands)
                && entry.role == target.attachment_role
        });
        if !target_matches {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        target
            .commands
            .try_enqueue_admitted_frame(frame.clone(), lane)?;
        self.record_product_flight_with_outputs(
            &mut outputs,
            target.key,
            target.incarnation,
            target.attachment_role,
            &target.commands,
            frame,
            CarrierWorkKind::RepairData,
        );
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_owner_flight(&self, key: CarrierPathKey, frame: &Frame) {
        let (incarnation, role, commands) = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| (entry.incarnation, entry.role, entry.commands.clone()))
            .expect("test owner output must be attached");
        self.record_product_flight(
            key,
            incarnation,
            role,
            &commands,
            frame,
            CarrierWorkKind::OwnerData,
        )
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_repair_flight(&self, key: CarrierPathKey, frame: &Frame) {
        let (incarnation, role, commands) = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| (entry.incarnation, entry.role, entry.commands.clone()))
            .expect("test repair output must be attached");
        self.record_product_flight(
            key,
            incarnation,
            role,
            &commands,
            frame,
            CarrierWorkKind::RepairData,
        )
    }

    #[cfg(test)]
    pub(in crate::runtime) fn age_repair_flights_for_test(&self, age: Duration) {
        let sent_at = Instant::now().checked_sub(age).unwrap_or_else(Instant::now);
        let mut flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        for path_flights in flights.values_mut() {
            for flight in path_flights {
                if flight.kind == CarrierWorkKind::RepairData {
                    flight.sent_at = sent_at;
                }
            }
        }
    }

    #[cfg(test)]
    fn record_product_flight(
        &self,
        key: CarrierPathKey,
        output_incarnation: u64,
        planned_role: StreamOpenRole,
        planned_commands: &ReliablePathCommandSender,
        frame: &Frame,
        kind: CarrierWorkKind,
    ) {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        self.record_product_flight_with_outputs(
            &mut outputs,
            key,
            output_incarnation,
            planned_role,
            planned_commands,
            frame,
            kind,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn record_product_flight_with_outputs(
        &self,
        outputs: &mut ResponseStreamOutputs,
        key: CarrierPathKey,
        output_incarnation: u64,
        planned_role: StreamOpenRole,
        planned_commands: &ReliablePathCommandSender,
        frame: &Frame,
        kind: CarrierWorkKind,
    ) {
        debug_assert!(kind.carries_product_offsets());
        let Some((offset, end, bytes)) = reliable_stream_frame_extent(frame) else {
            return;
        };
        let (recorded_incarnation, evidence_eligible) = if let Some(entry) = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == key && entry.commands.same_channel(planned_commands))
        {
            let incarnation_matches = entry.incarnation == output_incarnation;
            let role_matches = entry.role == planned_role;
            if kind.is_ordering_owner() {
                entry.owner_data_in_flight_bytes = entry
                    .owner_data_in_flight_bytes
                    .saturating_add(bytes as u64);
            }
            entry.bytes_in_flight = entry.bytes_in_flight.saturating_add(bytes as u64);
            (
                entry.incarnation,
                incarnation_matches && role_matches && planned_role != StreamOpenRole::Repair,
            )
        } else {
            (output_incarnation, false)
        };
        self.flights
            .lock()
            .expect("server reliable stream flight lock")
            .entry(offset)
            .or_default()
            .push(CarrierPathFlight {
                key,
                output_incarnation: recorded_incarnation,
                end,
                bytes,
                sent_at: Instant::now(),
                kind,
                evidence_eligible,
            });
        // The generation becomes visible only after the matching exact range.
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(super) fn invalidate_path_flight_evidence(
        &self,
        key: CarrierPathKey,
        output_incarnation: u64,
    ) {
        let mut flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        for path_flights in flights.values_mut() {
            for flight in path_flights.iter_mut().filter(|flight| {
                flight.key == key && flight.output_incarnation == output_incarnation
            }) {
                flight.evidence_eligible = false;
            }
        }
    }

    pub(super) fn rebind_path_flights_after_live_role_change(
        &self,
        key: CarrierPathKey,
        previous_incarnation: u64,
        current_incarnation: u64,
    ) {
        let mut flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        for path_flights in flights.values_mut() {
            for flight in path_flights.iter_mut().filter(|flight| {
                flight.key == key && flight.output_incarnation == previous_incarnation
            }) {
                flight.output_incarnation = current_incarnation;
                flight.evidence_eligible = false;
            }
        }
    }

    pub(in crate::runtime) fn lower_flights_before_frame(
        &self,
        frame: &Frame,
    ) -> Vec<CarrierPathFlightDebt> {
        let Some((offset, _, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        self.lower_flights_before_offset(offset)
    }

    pub(in crate::runtime) fn flight_keys_overlapping_frame(
        &self,
        frame: &Frame,
    ) -> Vec<CarrierPathKey> {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        let mut keys = Vec::new();
        for (_, path_flights) in flights.range(..end) {
            for flight in path_flights {
                if flight.end <= start || keys.contains(&flight.key) {
                    continue;
                }
                keys.push(flight.key);
            }
        }
        keys
    }

    pub(in crate::runtime) fn owner_flight_keys_overlapping_frame(
        &self,
        frame: &Frame,
    ) -> Vec<CarrierPathKey> {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        let mut keys = Vec::new();
        for (_, path_flights) in flights.range(..end) {
            for flight in path_flights {
                if flight.end <= start
                    || !flight.kind.is_ordering_owner()
                    || keys.contains(&flight.key)
                {
                    continue;
                }
                keys.push(flight.key);
            }
        }
        keys
    }

    pub(in crate::runtime) fn lower_flights_before_offset(
        &self,
        offset: u64,
    ) -> Vec<CarrierPathFlightDebt> {
        let mut debts = BTreeMap::<u64, CarrierPathFlightDebt>::new();
        {
            let ack_ordering = self
                .ack_ordering
                .lock()
                .expect("server response ACK ordering lock");
            for (hole_offset, holes) in ack_ordering.acked_holes.range(..offset) {
                if let Some(latest) = response_latest_ordering_hole(holes) {
                    debts.insert(
                        *hole_offset,
                        CarrierPathFlightDebt {
                            key: latest.key,
                            bytes: latest.bytes,
                        },
                    );
                }
            }
        }
        debts.into_values().collect()
    }

    fn blocking_owner_key_at_or_after(&self, offset: u64) -> Option<CarrierPathKey> {
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        for path_flights in flights.values() {
            for flight in path_flights {
                if flight.kind.is_ordering_owner() && flight.end > offset {
                    return Some(flight.key);
                }
            }
        }
        None
    }
}

pub(in crate::runtime::stream) fn release_carrier_path_flight_ranges(
    flights: &mut BTreeMap<u64, Vec<CarrierPathFlight>>,
    ranges: &[OffsetRange],
) -> Vec<(u64, CarrierPathReleasedFlight)> {
    if ranges.is_empty() || flights.is_empty() {
        return Vec::new();
    }

    let original_flights = std::mem::take(flights)
        .into_iter()
        .flat_map(|(start, path_flights)| {
            path_flights.into_iter().map(move |flight| (start, flight))
        })
        .collect::<Vec<_>>();
    let ambiguous_intervals = carrier_path_ambiguous_flight_intervals(&original_flights);
    let mut released = Vec::new();
    for (start, flight) in original_flights.iter().copied() {
        let split = split_carrier_flight_interval_by_ack(start, flight.end, ranges);
        for (acked_start, acked_end) in split.acked {
            let bytes = carrier_flight_interval_bytes(acked_start, acked_end);
            if bytes == 0 {
                continue;
            }
            released.push((
                acked_start,
                CarrierPathReleasedFlight {
                    flight: CarrierPathFlight {
                        end: acked_end,
                        bytes,
                        ..flight
                    },
                    path_proving: flight.evidence_eligible
                        && flight.kind.is_ordering_owner()
                        && !carrier_flight_intervals_overlap(
                            &ambiguous_intervals,
                            acked_start,
                            acked_end,
                        ),
                },
            ));
        }
        for (retained_start, retained_end) in split.retained {
            let bytes = carrier_flight_interval_bytes(retained_start, retained_end);
            if bytes == 0 {
                continue;
            }
            flights
                .entry(retained_start)
                .or_default()
                .push(CarrierPathFlight {
                    end: retained_end,
                    bytes,
                    ..flight
                });
        }
    }
    released
}

fn carrier_path_ambiguous_flight_intervals(
    flights: &[(u64, CarrierPathFlight)],
) -> Vec<(u64, u64)> {
    let mut events = BTreeMap::<u64, i64>::new();
    for (start, flight) in flights {
        *events.entry(*start).or_default() += 1;
        *events.entry(flight.end).or_default() -= 1;
    }
    let mut intervals = Vec::new();
    let mut active = 0_i64;
    let mut previous = None;
    for (position, delta) in events {
        if let Some(previous) = previous
            && previous < position
            && active > 1
        {
            intervals.push((previous, position));
        }
        active += delta;
        previous = Some(position);
    }
    intervals
}

fn carrier_flight_intervals_overlap(intervals: &[(u64, u64)], start: u64, end: u64) -> bool {
    let position = intervals.partition_point(|(_, interval_end)| *interval_end <= start);
    intervals
        .get(position)
        .is_some_and(|(interval_start, _)| *interval_start < end)
}

struct CarrierFlightIntervalSplit {
    acked: Vec<(u64, u64)>,
    retained: Vec<(u64, u64)>,
}

fn split_carrier_flight_interval_by_ack(
    start: u64,
    end: u64,
    ranges: &[OffsetRange],
) -> CarrierFlightIntervalSplit {
    let mut acked = Vec::new();
    let mut retained = Vec::new();
    let mut cursor = start;
    for range in ranges {
        if range.end <= cursor {
            continue;
        }
        if range.start >= end {
            break;
        }
        let ack_start = cursor.max(range.start);
        if cursor < ack_start {
            retained.push((cursor, ack_start));
        }
        let ack_end = end.min(range.end);
        if ack_start < ack_end {
            acked.push((ack_start, ack_end));
            cursor = ack_end;
        }
        if cursor >= end {
            break;
        }
    }
    if cursor < end {
        retained.push((cursor, end));
    }
    CarrierFlightIntervalSplit { acked, retained }
}

fn carrier_flight_interval_bytes(start: u64, end: u64) -> usize {
    usize::try_from(end.saturating_sub(start)).unwrap_or(usize::MAX)
}

pub(in crate::runtime::stream) fn product_flights_have_recent_repair_overlap(
    flights: &BTreeMap<u64, Vec<CarrierPathFlight>>,
    start: u64,
    end: u64,
    now: Instant,
    retry_after: Duration,
    mut live: impl FnMut(CarrierPathKey) -> bool,
) -> bool {
    if start >= end {
        return false;
    }
    for (&offset, path_flights) in flights.range(..end) {
        for flight in path_flights {
            if offset >= end || flight.end <= start {
                continue;
            }
            if flight.kind != CarrierWorkKind::RepairData || !live(flight.key) {
                continue;
            }
            if now.saturating_duration_since(flight.sent_at) < retry_after {
                return true;
            }
        }
    }
    false
}
