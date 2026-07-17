//! Exact product-range flight and STREAM_ACK ordering ownership.
//! This layer linearizes product ACK release and flight identity; carrier ACK
//! and packet recovery remain below it, while sender ranking remains above it.

use super::ResponseStreamBinding;
use super::ack_clock::apply_response_ack_clock_release_samples;
use super::attachment::{ResponseDispatchTarget, ResponseStreamOutputs};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::path::CarrierPathKey;
use crate::model::response::CarrierPathFlightDebt;
use crate::model::work::{
    CarrierWorkKind, ambiguous_flight_intervals, flight_interval_bytes, flight_intervals_overlap,
    split_flight_interval_by_ack,
};
use crate::protocol::frame::{
    normalize_offset_ranges, offset_ranges_not_covered, reliable_stream_frame_extent,
};
use crate::protocol::{Frame, OffsetRange, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::ReliablePathCommandSender;
use crate::scheduler::{PathSnapshot, TrafficClass};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// Product byte range currently assigned to a carrier path.
///
/// STREAM_ACK releases this ledger entry from product flight; carrier ACKs only
/// update carrier/path evidence and must not release product reinjection state.
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
    /// Hole volume is observation-only; ordering still computes it locally to
    /// detect semantic state changes in every build.
    #[cfg(feature = "lab-diagnostics")]
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
            #[cfg(feature = "lab-diagnostics")]
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
            .filter_map(|holes| response_latest_original_hole(holes))
            .map(|hole| hole.bytes)
            .sum()
    }
}

pub(in crate::runtime) fn response_latest_original_hole(
    holes: &[CarrierPathAckedHole],
) -> Option<&CarrierPathAckedHole> {
    holes
        .iter()
        .rev()
        .find(|hole| hole.kind.is_original_transmission())
}

impl ResponseStreamBinding {
    pub(in crate::runtime) fn tail_reinjection_snapshot(
        &self,
        ack_frontier: u64,
        lane: TrafficClass,
    ) -> Option<PathSnapshot> {
        let owner = self.blocking_original_path_at_or_after(ack_frontier);
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        owner.and_then(|(key, incarnation)| {
            outputs.snapshot_for_instance(key, incarnation, lane, self.mux_limits)
        })
    }

    pub(in crate::runtime) fn tail_reinjection_original_underlay(
        &self,
        ack_frontier: u64,
    ) -> Option<UnderlayProtocol> {
        self.blocking_original_path_at_or_after(ack_frontier)
            .map(|(key, _)| key.underlay)
    }

    pub(in crate::runtime) fn has_multipath_reinjection_alternative(&self) -> bool {
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .filter(|entry| !entry.commands.is_closed())
            .take(2)
            .count()
            > 1
    }

    pub(in crate::runtime) fn has_reinjection_path_for_frame(&self, frame: &Frame) -> bool {
        let avoid_outputs = self.flight_outputs_overlapping_frame(frame);
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        outputs
            .entries
            .iter()
            .any(|entry| !avoid_outputs.contains(&(entry.key, entry.incarnation)))
    }

    pub(in crate::runtime) fn has_tail_reinjection_output_for_frame(&self, frame: &Frame) -> bool {
        let owner_outputs = self.original_flight_outputs_overlapping_frame(frame);
        if owner_outputs.is_empty() {
            return false;
        }
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .any(|entry| !owner_outputs.contains(&(entry.key, entry.incarnation)))
    }

    pub(in crate::runtime) fn has_recent_reinjection_overlap(
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
        product_flights_have_recent_reinjection_overlap(
            &flights,
            start,
            end,
            now,
            retry_after,
            |key| live_keys.contains(&key),
        )
    }

    /// Returns every unacknowledged OriginalData range whose exact output no
    /// longer exists, excluding bytes already reinjected on a live output.
    /// Native recovery remains responsible for ranges whose output is live.
    pub(in crate::runtime) fn uncovered_failed_original_ranges(&self) -> Vec<OffsetRange> {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let live_outputs = outputs
            .entries
            .iter()
            .filter(|entry| !entry.commands.is_closed())
            .map(|entry| (entry.key, entry.incarnation))
            .collect::<Vec<_>>();
        if live_outputs.is_empty() {
            return Vec::new();
        }
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        let failed_original_ranges = normalize_offset_ranges(
            flights
                .iter()
                .filter_map(|(start, path_flights)| {
                    let original = path_flights
                        .iter()
                        .rev()
                        .find(|flight| flight.kind.is_original_transmission())?;
                    (!live_outputs.contains(&(original.key, original.output_incarnation)))
                        .then_some(OffsetRange {
                            start: *start,
                            end: original.end,
                        })
                })
                .collect(),
        );
        if failed_original_ranges.is_empty() {
            return Vec::new();
        }
        let live_reinjected_ranges = normalize_offset_ranges(
            flights
                .iter()
                .flat_map(|(start, path_flights)| {
                    path_flights.iter().filter_map(|flight| {
                        (flight.kind == CarrierWorkKind::ReinjectedData
                            && live_outputs.contains(&(flight.key, flight.output_incarnation)))
                        .then_some(OffsetRange {
                            start: *start,
                            end: flight.end,
                        })
                    })
                })
                .collect(),
        );
        offset_ranges_not_covered(&failed_original_ranges, &live_reinjected_ranges)
    }

    pub(in crate::runtime) fn has_untracked_data_reinjection_path_for_frame(
        &self,
        frame: &Frame,
    ) -> bool {
        if !self.flight_outputs_overlapping_frame(frame).is_empty() {
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
        self.release_normalized_acked_ranges_at(ranges, Instant::now())
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
            if let Some(entry) = outputs.entries.iter_mut().find(|entry| {
                entry.key == flight.key && entry.incarnation == flight.output_incarnation
            }) {
                entry.bytes_in_flight = entry.bytes_in_flight.saturating_sub(flight.bytes as u64);
                if flight.kind.is_original_transmission() {
                    entry.original_data_in_flight_bytes = entry
                        .original_data_in_flight_bytes
                        .saturating_sub(flight.bytes as u64);
                }
                if release.path_proving {
                    entry.original_data_acked_bytes = entry
                        .original_data_acked_bytes
                        .saturating_add(flight.bytes as u64);
                    let sample = path_samples.entry(identity).or_insert((
                        0_u64,
                        0_u64,
                        flight.sent_at,
                        flight.sent_at,
                    ));
                    sample.0 = sample.0.saturating_add(flight.bytes as u64);
                    sample.1 = sample.1.saturating_add(flight.bytes as u64);
                    sample.2 = sample.2.min(flight.sent_at);
                    sample.3 = sample.3.max(flight.sent_at);
                }
                changed = true;
            }
        }
        apply_response_ack_clock_release_samples(&mut outputs, path_samples, now);
        for hole in ordering_update.newly_contiguous {
            if !hole.path_proving {
                continue;
            }
            if let Some(entry) = outputs
                .entries
                .iter_mut()
                .find(|entry| entry.key == hole.key && entry.incarnation == hole.output_incarnation)
                && hole.end <= ordering_update.contiguous_frontier
            {
                entry.delivery_samples = entry.delivery_samples.saturating_add(1);
                changed = true;
            }
        }
        // Scheduling captures this before reading lower flights and path
        // snapshots. Publish it only after both ledgers describe the ACK.
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
        drop(outputs);
        if changed || ordering_update.changed {
            self.notify_update();
        }
    }

    pub(super) fn record_validated_original_flight_with_outputs(
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
            entry.original_data_in_flight_bytes = entry
                .original_data_in_flight_bytes
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
                kind: CarrierWorkKind::OriginalData,
                evidence_eligible: true,
            });
        // Keep path counters and the exact range ledger in one published model
        // generation so a concurrent measurement plan cannot mix the views.
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(in crate::runtime) fn try_enqueue_reinjected_frame_for_target(
        &self,
        target: &ResponseDispatchTarget,
        frame: &Frame,
        lane: TrafficClass,
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
        let Some(target_index) = outputs.entries.iter().position(|entry| {
            entry.key == target.key
                && entry.path_instance_id == target.path_instance_id
                && entry.incarnation == target.incarnation
        }) else {
            return Err(RuntimeError::SenderServiceBlocked);
        };
        let commands = outputs.entries[target_index].commands.clone();
        let command = commands.try_reserve_reinjection_frame(frame.clone(), lane)?;
        self.record_product_flight_with_outputs(
            &mut outputs,
            target.key,
            target.incarnation,
            &commands,
            frame,
            CarrierWorkKind::ReinjectedData,
        );
        command.commit();
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_original_flight(&self, key: CarrierPathKey, frame: &Frame) {
        let (incarnation, commands) = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| (entry.incarnation, entry.commands.clone()))
            .expect("test owner output must be attached");
        self.record_product_flight(
            key,
            incarnation,
            &commands,
            frame,
            CarrierWorkKind::OriginalData,
        )
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_reinjected_flight(&self, key: CarrierPathKey, frame: &Frame) {
        let (incarnation, commands) = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| (entry.incarnation, entry.commands.clone()))
            .expect("test reinjection output must be attached");
        self.record_product_flight(
            key,
            incarnation,
            &commands,
            frame,
            CarrierWorkKind::ReinjectedData,
        )
    }

    #[cfg(test)]
    pub(in crate::runtime) fn age_reinjected_flights_for_test(&self, age: Duration) {
        let sent_at = Instant::now().checked_sub(age).unwrap_or_else(Instant::now);
        let mut flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        for path_flights in flights.values_mut() {
            for flight in path_flights {
                if flight.kind == CarrierWorkKind::ReinjectedData {
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
            planned_commands,
            frame,
            kind,
        );
    }

    fn record_product_flight_with_outputs(
        &self,
        outputs: &mut ResponseStreamOutputs,
        key: CarrierPathKey,
        output_incarnation: u64,
        planned_commands: &ReliablePathCommandSender,
        frame: &Frame,
        kind: CarrierWorkKind,
    ) {
        let Some((offset, end, bytes)) = reliable_stream_frame_extent(frame) else {
            return;
        };
        let (recorded_incarnation, evidence_eligible) = if let Some(entry) = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == key && entry.commands.same_channel(planned_commands))
        {
            let incarnation_matches = entry.incarnation == output_incarnation;
            if kind.is_original_transmission() {
                entry.original_data_in_flight_bytes = entry
                    .original_data_in_flight_bytes
                    .saturating_add(bytes as u64);
            }
            entry.bytes_in_flight = entry.bytes_in_flight.saturating_add(bytes as u64);
            (entry.incarnation, incarnation_matches)
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

    pub(in crate::runtime) fn flight_outputs_overlapping_frame(
        &self,
        frame: &Frame,
    ) -> Vec<(CarrierPathKey, u64)> {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        let mut outputs = Vec::new();
        for (_, path_flights) in flights.range(..end) {
            for flight in path_flights {
                let output = (flight.key, flight.output_incarnation);
                if flight.end <= start || outputs.contains(&output) {
                    continue;
                }
                outputs.push(output);
            }
        }
        outputs
    }

    pub(in crate::runtime) fn original_flight_outputs_overlapping_frame(
        &self,
        frame: &Frame,
    ) -> Vec<(CarrierPathKey, u64)> {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        let mut outputs = Vec::new();
        for (_, path_flights) in flights.range(..end) {
            for flight in path_flights {
                let output = (flight.key, flight.output_incarnation);
                if flight.end <= start
                    || !flight.kind.is_original_transmission()
                    || outputs.contains(&output)
                {
                    continue;
                }
                outputs.push(output);
            }
        }
        outputs
    }

    pub(in crate::runtime) fn lower_flights_before_offset(
        &self,
        offset: u64,
    ) -> Vec<CarrierPathFlightDebt> {
        let mut debts = BTreeMap::<u64, CarrierPathFlightDebt>::new();
        {
            let flights = self
                .flights
                .lock()
                .expect("server reliable stream flight lock");
            for (flight_offset, path_flights) in flights.range(..offset) {
                if let Some(original) = path_flights
                    .iter()
                    .rev()
                    .find(|flight| flight.kind.is_original_transmission())
                {
                    debts.insert(
                        *flight_offset,
                        CarrierPathFlightDebt {
                            key: original.key,
                            output_incarnation: original.output_incarnation,
                            bytes: original.bytes as u64,
                        },
                    );
                }
            }
        }
        {
            let ack_ordering = self
                .ack_ordering
                .lock()
                .expect("server response ACK ordering lock");
            for (hole_offset, holes) in ack_ordering.acked_holes.range(..offset) {
                if let Some(latest) = response_latest_original_hole(holes) {
                    debts.insert(
                        *hole_offset,
                        CarrierPathFlightDebt {
                            key: latest.key,
                            output_incarnation: latest.output_incarnation,
                            bytes: latest.bytes,
                        },
                    );
                }
            }
        }
        debts.into_values().collect()
    }

    pub(in crate::runtime) fn blocking_original_path_at_or_after(
        &self,
        offset: u64,
    ) -> Option<(CarrierPathKey, u64)> {
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        for path_flights in flights.values() {
            for flight in path_flights {
                if flight.kind.is_original_transmission() && flight.end > offset {
                    return Some((flight.key, flight.output_incarnation));
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
    let ambiguous_intervals = ambiguous_flight_intervals(
        original_flights
            .iter()
            .map(|(start, flight)| (*start, flight.end)),
    );
    let mut released = Vec::new();
    for (start, flight) in original_flights.iter().copied() {
        let split = split_flight_interval_by_ack(start, flight.end, ranges);
        for (acked_start, acked_end) in split.acked {
            let bytes = flight_interval_bytes(acked_start, acked_end);
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
                        && flight.kind.is_original_transmission()
                        && !flight_intervals_overlap(&ambiguous_intervals, acked_start, acked_end),
                },
            ));
        }
        for (retained_start, retained_end) in split.retained {
            let bytes = flight_interval_bytes(retained_start, retained_end);
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

pub(in crate::runtime::stream) fn product_flights_have_recent_reinjection_overlap(
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
            if flight.kind != CarrierWorkKind::ReinjectedData || !live(flight.key) {
                continue;
            }
            if now.saturating_duration_since(flight.sent_at) < retry_after {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
#[path = "delivery_test.rs"]
mod tests;
