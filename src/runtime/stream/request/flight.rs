//! Exact request-data flight ownership.
//!
//! Original and reinjected ranges remain keyed by exact attachment instance so
//! Data ACK attribution cannot cross a reconnect boundary.

use crate::model::path::{RelayPathInstance, RelayPathKey};
use crate::model::work::{
    CarrierWorkKind, ambiguous_flight_intervals, flight_interval_bytes, flight_intervals_overlap,
    split_flight_interval_by_ack,
};
use crate::protocol::frame::{
    normalize_offset_ranges, offset_ranges_not_covered, reliable_stream_frame_extent,
};
use crate::protocol::{Frame, OffsetRange, UnderlayProtocol};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct RequestPathRelease {
    pub(in crate::runtime) instance: RelayPathInstance,
    pub(in crate::runtime) bytes: usize,
    pub(in crate::runtime) sent_at: Instant,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) elapsed: Duration,
    pub(in crate::runtime) path_proving: bool,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct RequestObservedPathRelease {
    pub(in crate::runtime) instance: RelayPathInstance,
    pub(in crate::runtime) range: OffsetRange,
    pub(in crate::runtime) kind: CarrierWorkKind,
    pub(in crate::runtime) unambiguous: bool,
}

#[derive(Debug, Default)]
pub(in crate::runtime) struct RequestFlightLedger {
    // OriginalData identifies the ordered path; ReinjectedData remains a duplicate.
    // Exact attachment instances fence ACK evidence across path replacement.
    flights: BTreeMap<u64, Vec<RequestFlight>>,
}

impl RequestFlightLedger {
    pub(in crate::runtime) fn record_original_frame_instance(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
    ) -> usize {
        self.record_product_frame(instance, frame, CarrierWorkKind::OriginalData)
    }

    pub(in crate::runtime) fn record_reinjection_frame_instance(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
    ) -> usize {
        self.record_product_frame(instance, frame, CarrierWorkKind::ReinjectedData)
    }

    fn record_product_frame(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
        kind: CarrierWorkKind,
    ) -> usize {
        let Some((offset, end, bytes)) = reliable_stream_frame_extent(frame) else {
            return 0;
        };
        self.flights.entry(offset).or_default().push(RequestFlight {
            instance,
            end,
            bytes,
            sent_at: Instant::now(),
            kind,
        });
        bytes
    }

    pub(in crate::runtime) fn release_normalized_acked_ranges(
        &mut self,
        ranges: &[OffsetRange],
    ) -> Vec<RequestPathRelease> {
        self.release_normalized_acked_ranges_inner::<false, _>(ranges, |_| {})
    }

    pub(in crate::runtime) fn release_normalized_acked_ranges_observed(
        &mut self,
        ranges: &[OffsetRange],
        observe: impl FnMut(RequestObservedPathRelease),
    ) -> Vec<RequestPathRelease> {
        self.release_normalized_acked_ranges_inner::<true, _>(ranges, observe)
    }

    fn release_normalized_acked_ranges_inner<
        const OBSERVE: bool,
        F: FnMut(RequestObservedPathRelease),
    >(
        &mut self,
        ranges: &[OffsetRange],
        mut observe: F,
    ) -> Vec<RequestPathRelease> {
        if ranges.is_empty() || self.flights.is_empty() {
            return Vec::new();
        }

        let original_flights = std::mem::take(&mut self.flights)
            .into_iter()
            .flat_map(|(start, flights)| flights.into_iter().map(move |flight| (start, flight)))
            .collect::<Vec<_>>();
        let ambiguous_intervals = ambiguous_flight_intervals(
            original_flights
                .iter()
                .map(|(start, flight)| (*start, flight.end)),
        );
        let now = Instant::now();
        let mut released = Vec::new();
        for (start, flight) in original_flights.iter().copied() {
            let split = split_flight_interval_by_ack(start, flight.end, ranges);
            for (acked_start, acked_end) in split.acked {
                let bytes = flight_interval_bytes(acked_start, acked_end);
                if bytes == 0 {
                    continue;
                }
                let unambiguous =
                    !flight_intervals_overlap(&ambiguous_intervals, acked_start, acked_end);
                if OBSERVE {
                    observe(RequestObservedPathRelease {
                        instance: flight.instance,
                        range: OffsetRange {
                            start: acked_start,
                            end: acked_end,
                        },
                        kind: flight.kind,
                        unambiguous,
                    });
                }
                released.push(RequestPathRelease {
                    instance: flight.instance,
                    bytes,
                    sent_at: flight.sent_at,
                    elapsed: now.saturating_duration_since(flight.sent_at),
                    path_proving: flight.kind.is_original_transmission() && unambiguous,
                });
            }
            for (retained_start, retained_end) in split.retained {
                let bytes = flight_interval_bytes(retained_start, retained_end);
                if bytes == 0 {
                    continue;
                }
                self.flights
                    .entry(retained_start)
                    .or_default()
                    .push(RequestFlight {
                        end: retained_end,
                        bytes,
                        ..flight
                    });
            }
        }
        released
    }

    pub(in crate::runtime) fn drain_all(&mut self) -> Vec<RequestPathRelease> {
        let mut released = Vec::new();
        for flights in std::mem::take(&mut self.flights).into_values() {
            for flight in flights {
                released.push(RequestPathRelease {
                    instance: flight.instance,
                    bytes: flight.bytes,
                    sent_at: flight.sent_at,
                    elapsed: Instant::now().saturating_duration_since(flight.sent_at),
                    path_proving: false,
                });
            }
        }
        released
    }

    pub(in crate::runtime) fn sent_instances_for_frame(
        &self,
        frame: &Frame,
    ) -> Vec<RelayPathInstance> {
        let Some((offset, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let mut instances = Vec::new();
        if let Some(flights) = self.flights.get(&offset) {
            for flight in flights {
                if flight.end >= end && !instances.contains(&flight.instance) {
                    instances.push(flight.instance);
                }
            }
        }
        instances
    }

    /// Exact unique Data Sequence bytes still owned by one attachment. This is
    /// the portable ordered-stream backlog when native per-socket state is not
    /// available; reinjection copies never contribute to it.
    pub(in crate::runtime) fn original_data_in_flight_bytes(
        &self,
        instance: RelayPathInstance,
    ) -> u64 {
        self.flights
            .values()
            .flat_map(|flights| flights.iter())
            .filter(|flight| flight.instance == instance && flight.kind.is_original_transmission())
            .fold(0_u64, |bytes, flight| {
                bytes.saturating_add(flight.bytes as u64)
            })
    }

    pub(in crate::runtime) fn has_recent_reinjection_on_instance(
        &self,
        frame: &Frame,
        instance: RelayPathInstance,
        retry_after: Duration,
    ) -> bool {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return false;
        };
        let now = Instant::now();
        self.flights.range(..end).any(|(offset, flights)| {
            *offset < end
                && flights.iter().any(|flight| {
                    flight.end > start
                        && flight.instance == instance
                        && flight.kind == CarrierWorkKind::ReinjectedData
                        && now.saturating_duration_since(flight.sent_at) < retry_after
                })
        })
    }

    #[cfg(test)]
    pub(in crate::runtime) fn age_reinjected_flights_for_test(&mut self, elapsed: Duration) {
        for flights in self.flights.values_mut() {
            for flight in flights
                .iter_mut()
                .filter(|flight| flight.kind == CarrierWorkKind::ReinjectedData)
            {
                flight.sent_at = flight
                    .sent_at
                    .checked_sub(elapsed)
                    .unwrap_or(flight.sent_at);
            }
        }
    }

    pub(in crate::runtime) fn has_missing_original_transmission_before_offset(
        &self,
        offset: u64,
        live_instances: &[RelayPathInstance],
    ) -> bool {
        self.flights.range(..offset).any(|(_, flights)| {
            flights.iter().any(|flight| {
                flight.kind.is_original_transmission() && !live_instances.contains(&flight.instance)
            })
        })
    }

    pub(in crate::runtime) fn original_transmission_keys_for_frame(
        &self,
        frame: &Frame,
        live_instances: &[RelayPathInstance],
    ) -> Vec<RelayPathKey> {
        let mut keys = Vec::new();
        for instance in self.original_transmission_instances_for_frame(frame, live_instances) {
            if !keys.contains(&instance.key) {
                keys.push(instance.key);
            }
        }
        keys
    }

    pub(in crate::runtime) fn original_transmission_instances_for_frame(
        &self,
        frame: &Frame,
        live_instances: &[RelayPathInstance],
    ) -> Vec<RelayPathInstance> {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let mut owners = Vec::new();
        for (offset, flights) in self.flights.range(..end) {
            if *offset > start {
                break;
            }
            for flight in flights {
                if flight.kind.is_original_transmission()
                    && flight.end >= end
                    && live_instances.contains(&flight.instance)
                    && !owners.contains(&flight.instance)
                {
                    owners.push(flight.instance);
                }
            }
        }
        owners
    }

    pub(in crate::runtime) fn original_transmission_underlay_for_frame(
        &self,
        frame: &Frame,
    ) -> Option<UnderlayProtocol> {
        let owner_keys = self.original_transmission_keys_for_frame_any_instance(frame);
        let underlay = owner_keys.first()?.underlay;
        owner_keys
            .iter()
            .all(|key| key.underlay == underlay)
            .then_some(underlay)
    }

    pub(in crate::runtime) fn original_transmission_keys_for_frame_any_instance(
        &self,
        frame: &Frame,
    ) -> Vec<RelayPathKey> {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let mut owner_keys = Vec::new();
        for (offset, flights) in self.flights.range(..end) {
            if *offset > start {
                break;
            }
            for flight in flights {
                if !flight.kind.is_original_transmission() || flight.end < end {
                    continue;
                }
                if !owner_keys.contains(&flight.instance.key) {
                    owner_keys.push(flight.instance.key);
                }
            }
        }
        owner_keys
    }

    #[cfg(test)]
    pub(in crate::runtime) fn unique_original_path_for_frame(
        &self,
        frame: &Frame,
    ) -> Option<RelayPathInstance> {
        let (start, end, _) = reliable_stream_frame_extent(frame)?;
        self.unique_original_path_for_range(OffsetRange { start, end })
    }

    /// Returns exact ownership and its latest send epoch in one ledger pass.
    /// The latest adjacent flight is conservative when an ACK gap spans writes.
    pub(in crate::runtime) fn unique_original_flight_for_frame(
        &self,
        frame: &Frame,
    ) -> Option<(RelayPathInstance, Instant)> {
        let (start, end, _) = reliable_stream_frame_extent(frame)?;
        self.unique_original_flight_for_range(OffsetRange { start, end })
    }

    #[cfg(test)]
    pub(in crate::runtime) fn unique_original_sent_at_for_frame(
        &self,
        frame: &Frame,
    ) -> Option<Instant> {
        self.unique_original_flight_for_frame(frame)
            .map(|(_, sent_at)| sent_at)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn unique_original_path_for_range(
        &self,
        range: OffsetRange,
    ) -> Option<RelayPathInstance> {
        self.unique_original_flight_for_range(range)
            .map(|(owner, _)| owner)
    }

    fn unique_original_flight_for_range(
        &self,
        range: OffsetRange,
    ) -> Option<(RelayPathInstance, Instant)> {
        if range.is_empty() {
            return None;
        }
        let mut owner = None;
        let mut latest_sent_at = None;
        let mut covered = Vec::new();
        for (start, flights) in self.flights.range(..range.end) {
            for flight in flights {
                if !flight.kind.is_original_transmission() || flight.end <= range.start {
                    continue;
                }
                if owner.is_some_and(|owner| owner != flight.instance) {
                    return None;
                }
                owner = Some(flight.instance);
                latest_sent_at = Some(
                    latest_sent_at
                        .map_or(flight.sent_at, |latest: Instant| latest.max(flight.sent_at)),
                );
                covered.push(OffsetRange {
                    start: (*start).max(range.start),
                    end: flight.end.min(range.end),
                });
            }
        }
        let covered = normalize_offset_ranges(covered);
        if covered.len() == 1 && covered[0] == range {
            owner.zip(latest_sent_at)
        } else {
            None
        }
    }

    pub(in crate::runtime) fn tail_reinjection_owner_keys(
        &self,
        frame: &Frame,
        live_instances: &[RelayPathInstance],
        first_reinjection_after: Duration,
        repeat_reinjection_after: Duration,
    ) -> Vec<RelayPathKey> {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let now = Instant::now();
        let expected_owner_keys = self.original_transmission_keys_for_frame(frame, live_instances);
        let mut owner_keys = Vec::new();
        for (offset, flights) in self.flights.range(..end) {
            if *offset > start {
                break;
            }
            for flight in flights {
                if !flight.kind.is_original_transmission()
                    || flight.end < end
                    || !live_instances.contains(&flight.instance)
                    || now.saturating_duration_since(flight.sent_at) < first_reinjection_after
                {
                    continue;
                }
                if expected_owner_keys.contains(&flight.instance.key)
                    && !owner_keys.contains(&flight.instance.key)
                {
                    owner_keys.push(flight.instance.key);
                }
            }
        }
        if owner_keys.is_empty() {
            return owner_keys;
        }
        let recent_distinct_reinjection = self.flights.range(..end).any(|(offset, flights)| {
            *offset < end
                && flights.iter().any(|flight| {
                    flight.end > start
                        && flight.kind == CarrierWorkKind::ReinjectedData
                        && live_instances.contains(&flight.instance)
                        && !owner_keys.contains(&flight.instance.key)
                        && now.saturating_duration_since(flight.sent_at) < repeat_reinjection_after
                })
        });
        if recent_distinct_reinjection {
            Vec::new()
        } else {
            owner_keys
        }
    }

    pub(in crate::runtime) fn latest_unacked_ranges_for_path_instance(
        &self,
        instance: RelayPathInstance,
    ) -> Vec<OffsetRange> {
        let ranges = self
            .flights
            .iter()
            .filter_map(|(offset, flights)| {
                latest_original_transmission(flights)
                    .filter(|owner| owner.instance == instance)
                    .map(|owner| OffsetRange {
                        start: *offset,
                        end: owner.end,
                    })
            })
            .collect::<Vec<_>>();
        normalize_offset_ranges(ranges)
    }

    pub(in crate::runtime) fn earliest_unacked_original_path(&self) -> Option<RelayPathInstance> {
        self.flights
            .values()
            .find_map(|flights| latest_original_transmission(flights).map(|flight| flight.instance))
    }

    /// Returns original data not already reinjected on a usable alternate
    /// path. Each carrier remains responsible for recovering its own flights.
    pub(in crate::runtime) fn uncovered_unacked_ranges_for_reinjection(
        &self,
        original_path: RelayPathInstance,
        usable_alternate_paths: &[RelayPathInstance],
    ) -> Vec<OffsetRange> {
        let original_ranges = self.latest_unacked_ranges_for_path_instance(original_path);
        if original_ranges.is_empty() {
            return Vec::new();
        }
        let reinjected_ranges = normalize_offset_ranges(
            self.flights
                .iter()
                .flat_map(|(start, flights)| {
                    flights.iter().filter_map(move |flight| {
                        (flight.kind == CarrierWorkKind::ReinjectedData
                            && flight.instance != original_path
                            && usable_alternate_paths.contains(&flight.instance))
                        .then_some(OffsetRange {
                            start: *start,
                            end: flight.end,
                        })
                    })
                })
                .collect(),
        );
        offset_ranges_not_covered(&original_ranges, &reinjected_ranges)
    }

    pub(in crate::runtime) fn original_transmission_instances(&self) -> Vec<RelayPathInstance> {
        let mut instances = Vec::new();
        for flights in self.flights.values() {
            for flight in flights {
                if flight.kind.is_original_transmission() && !instances.contains(&flight.instance) {
                    instances.push(flight.instance);
                }
            }
        }
        instances
    }

    pub(in crate::runtime) fn ordering_debt_bytes_before_offset(
        &self,
        key: RelayPathKey,
        offset: u64,
    ) -> u64 {
        self.flights
            .range(..offset)
            .filter_map(|(_, flights)| {
                let latest = latest_original_transmission(flights)?;
                (latest.instance.key != key).then_some(latest.bytes as u64)
            })
            .sum()
    }

    pub(in crate::runtime) fn original_transmission_bytes_before_offset(&self, offset: u64) -> u64 {
        self.flights
            .range(..offset)
            .filter_map(|(_, flights)| latest_original_transmission(flights))
            .map(|owner| owner.bytes as u64)
            .sum()
    }

    pub(in crate::runtime) fn has_original_transmission_flights_for_instance(
        &self,
        instance: RelayPathInstance,
    ) -> bool {
        self.flights.values().any(|flights| {
            flights
                .iter()
                .any(|flight| flight.instance == instance && flight.kind.is_original_transmission())
        })
    }

    pub(in crate::runtime) fn has_foreign_original_transmission_before_offset(
        &self,
        offset: u64,
        allowed: &[RelayPathKey],
    ) -> bool {
        self.flights.range(..offset).any(|(_, flights)| {
            flights.iter().any(|flight| {
                flight.kind.is_original_transmission() && !allowed.contains(&flight.instance.key)
            })
        })
    }

    pub(in crate::runtime) fn foreign_original_transmission_debt_before_offset(
        &self,
        offset: u64,
        allowed: &[RelayPathKey],
    ) -> (usize, u64) {
        self.flights
            .range(..offset)
            .filter_map(|(_, flights)| {
                flights
                    .iter()
                    .find(|flight| {
                        flight.kind.is_original_transmission()
                            && !allowed.contains(&flight.instance.key)
                    })
                    .map(|flight| flight.bytes as u64)
            })
            .fold((0usize, 0u64), |(ranges, bytes), flight_bytes| {
                (ranges.saturating_add(1), bytes.saturating_add(flight_bytes))
            })
    }

    pub(in crate::runtime) fn has_reinjection_flights_before_offset(&self, offset: u64) -> bool {
        self.flights.range(..offset).any(|(_, flights)| {
            flights
                .iter()
                .any(|flight| flight.kind == CarrierWorkKind::ReinjectedData)
        })
    }

    pub(in crate::runtime) fn oldest_lower_flight_owner_before_offset(
        &self,
        offset: u64,
    ) -> Option<RelayPathKey> {
        self.flights.range(..offset).find_map(|(_, flights)| {
            latest_original_transmission(flights).map(|flight| flight.instance.key)
        })
    }
}

fn latest_original_transmission(flights: &[RequestFlight]) -> Option<&RequestFlight> {
    flights
        .iter()
        .rev()
        .find(|flight| flight.kind.is_original_transmission())
}

#[derive(Debug, Clone, Copy)]
struct RequestFlight {
    instance: RelayPathInstance,
    end: u64,
    bytes: usize,
    sent_at: Instant,
    kind: CarrierWorkKind,
}

#[cfg(test)]
#[path = "flight_test.rs"]
mod tests;
