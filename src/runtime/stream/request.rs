//! Exact request-stream flight ownership.
//!
//! The stream owns product offsets and attachment-fenced flights so path
//! selection can observe ordering debt without owning or mutating it.

use crate::model::path::{RelayPathInstance, RelayPathKey};
use crate::model::work::CarrierWorkKind;
use crate::protocol::frame::{normalized_offset_ranges, reliable_stream_frame_extent};
use crate::protocol::{Frame, OffsetRange, UnderlayProtocol};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct RequestPathRelease {
    pub(in crate::runtime) key: RelayPathKey,
    pub(in crate::runtime) instance: RelayPathInstance,
    pub(in crate::runtime) bytes: usize,
    pub(in crate::runtime) sent_at: Instant,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) elapsed: Duration,
    pub(in crate::runtime) path_proving: bool,
}

#[derive(Debug, Default)]
pub(in crate::runtime) struct RequestFlightLedger {
    // OwnerData identifies the ordered path; RepairData remains a duplicate.
    // Exact attachment instances fence ACK evidence across path replacement.
    flights: BTreeMap<u64, Vec<RequestFlight>>,
}

impl RequestFlightLedger {
    #[cfg(test)]
    pub(in crate::runtime) fn record_owner_frame(
        &mut self,
        key: RelayPathKey,
        frame: &Frame,
    ) -> usize {
        self.record_owner_frame_instance(RelayPathInstance { key, id: 0 }, frame)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_repair_frame(
        &mut self,
        key: RelayPathKey,
        frame: &Frame,
    ) -> usize {
        self.record_repair_frame_instance(RelayPathInstance { key, id: 0 }, frame)
    }

    pub(in crate::runtime) fn record_owner_frame_instance(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
    ) -> usize {
        self.record_product_frame(instance, frame, CarrierWorkKind::OwnerData)
    }

    pub(in crate::runtime) fn record_repair_frame_instance(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
    ) -> usize {
        self.record_product_frame(instance, frame, CarrierWorkKind::RepairData)
    }

    fn record_product_frame(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
        kind: CarrierWorkKind,
    ) -> usize {
        debug_assert!(kind.carries_product_offsets());
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
        if ranges.is_empty() || self.flights.is_empty() {
            return Vec::new();
        }

        let original_flights = std::mem::take(&mut self.flights)
            .into_iter()
            .flat_map(|(start, flights)| flights.into_iter().map(move |flight| (start, flight)))
            .collect::<Vec<_>>();
        let ambiguous_intervals = request_ambiguous_flight_intervals(&original_flights);
        let now = Instant::now();
        let mut released = Vec::new();
        for (start, flight) in original_flights.iter().copied() {
            let split = split_flight_interval_by_ack(start, flight.end, ranges);
            for (acked_start, acked_end) in split.acked {
                let bytes = flight_interval_bytes(acked_start, acked_end);
                if bytes == 0 {
                    continue;
                }
                released.push(RequestPathRelease {
                    key: flight.instance.key,
                    instance: flight.instance,
                    bytes,
                    sent_at: flight.sent_at,
                    elapsed: now.saturating_duration_since(flight.sent_at),
                    path_proving: flight.kind.is_ordering_owner()
                        && !flight_intervals_overlap(&ambiguous_intervals, acked_start, acked_end),
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
                    key: flight.instance.key,
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

    #[cfg(test)]
    pub(in crate::runtime) fn age_product_flights_for_test(&mut self, age: Duration) {
        let sent_at = Instant::now().checked_sub(age).unwrap_or_else(Instant::now);
        for flights in self.flights.values_mut() {
            for flight in flights {
                flight.sent_at = sent_at;
            }
        }
    }

    pub(in crate::runtime) fn sent_keys_for_frame(&self, frame: &Frame) -> Vec<RelayPathKey> {
        let Some((offset, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let mut keys = Vec::new();
        if let Some(flights) = self.flights.get(&offset) {
            for flight in flights {
                if flight.end >= end && !keys.contains(&flight.instance.key) {
                    keys.push(flight.instance.key);
                }
            }
        }
        keys
    }

    pub(in crate::runtime) fn has_missing_ordering_owner_before_offset(
        &self,
        offset: u64,
        live_instances: &[RelayPathInstance],
    ) -> bool {
        self.flights.range(..offset).any(|(_, flights)| {
            flights.iter().any(|flight| {
                flight.kind.is_ordering_owner() && !live_instances.contains(&flight.instance)
            })
        })
    }

    pub(in crate::runtime) fn ordering_owner_keys_for_frame(
        &self,
        frame: &Frame,
        live_instances: &[RelayPathInstance],
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
                if flight.kind.is_ordering_owner()
                    && flight.end >= end
                    && live_instances.contains(&flight.instance)
                    && !owner_keys.contains(&flight.instance.key)
                {
                    owner_keys.push(flight.instance.key);
                }
            }
        }
        owner_keys
    }

    pub(in crate::runtime) fn ordering_owner_underlay_for_frame(
        &self,
        frame: &Frame,
    ) -> Option<UnderlayProtocol> {
        let owner_keys = self.ordering_owner_keys_for_frame_any_instance(frame);
        let underlay = owner_keys.first()?.underlay;
        owner_keys
            .iter()
            .all(|key| key.underlay == underlay)
            .then_some(underlay)
    }

    pub(in crate::runtime) fn ordering_owner_keys_for_frame_any_instance(
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
                if !flight.kind.is_ordering_owner() || flight.end < end {
                    continue;
                }
                if !owner_keys.contains(&flight.instance.key) {
                    owner_keys.push(flight.instance.key);
                }
            }
        }
        owner_keys
    }

    pub(in crate::runtime) fn live_owner_tail_repair_owner_keys(
        &self,
        frame: &Frame,
        live_instances: &[RelayPathInstance],
        first_repair_after: Duration,
        repeat_repair_after: Duration,
    ) -> Vec<RelayPathKey> {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let now = Instant::now();
        let expected_owner_keys = self.ordering_owner_keys_for_frame(frame, live_instances);
        let mut owner_keys = Vec::new();
        for (offset, flights) in self.flights.range(..end) {
            if *offset > start {
                break;
            }
            for flight in flights {
                if !flight.kind.is_ordering_owner()
                    || flight.end < end
                    || !live_instances.contains(&flight.instance)
                    || now.saturating_duration_since(flight.sent_at) < first_repair_after
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
        let recent_distinct_repair = self.flights.range(..end).any(|(offset, flights)| {
            *offset < end
                && flights.iter().any(|flight| {
                    flight.end > start
                        && flight.kind == CarrierWorkKind::RepairData
                        && live_instances.contains(&flight.instance)
                        && !owner_keys.contains(&flight.instance.key)
                        && now.saturating_duration_since(flight.sent_at) < repeat_repair_after
                })
        });
        if recent_distinct_repair {
            Vec::new()
        } else {
            owner_keys
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn latest_unacked_ranges_for_path(
        &self,
        key: RelayPathKey,
    ) -> Vec<OffsetRange> {
        let mut ranges = Vec::new();
        for (offset, flights) in &self.flights {
            let Some(latest) = latest_ordering_owner(flights) else {
                continue;
            };
            if latest.instance.key == key {
                ranges.push(OffsetRange {
                    start: *offset,
                    end: latest.end,
                });
            }
        }
        normalized_offset_ranges(&ranges)
    }

    pub(in crate::runtime) fn latest_unacked_ranges_for_path_instance(
        &self,
        instance: RelayPathInstance,
    ) -> Vec<OffsetRange> {
        let ranges = self
            .flights
            .iter()
            .filter_map(|(offset, flights)| {
                latest_ordering_owner(flights)
                    .filter(|owner| owner.instance == instance)
                    .map(|owner| OffsetRange {
                        start: *offset,
                        end: owner.end,
                    })
            })
            .collect::<Vec<_>>();
        normalized_offset_ranges(&ranges)
    }

    pub(in crate::runtime) fn ordering_owner_instances(&self) -> Vec<RelayPathInstance> {
        let mut instances = Vec::new();
        for flights in self.flights.values() {
            for flight in flights {
                if flight.kind.is_ordering_owner() && !instances.contains(&flight.instance) {
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
                let latest = latest_ordering_owner(flights)?;
                (latest.instance.key != key).then_some(latest.bytes as u64)
            })
            .sum()
    }

    pub(in crate::runtime) fn ordering_owner_bytes_before_offset(&self, offset: u64) -> u64 {
        self.flights
            .range(..offset)
            .filter_map(|(_, flights)| latest_ordering_owner(flights))
            .map(|owner| owner.bytes as u64)
            .sum()
    }

    pub(in crate::runtime) fn has_ordering_owner_flights_for_instance(
        &self,
        instance: RelayPathInstance,
    ) -> bool {
        self.flights.values().any(|flights| {
            flights
                .iter()
                .any(|flight| flight.instance == instance && flight.kind.is_ordering_owner())
        })
    }

    pub(in crate::runtime) fn has_foreign_ordering_owner_before_offset(
        &self,
        offset: u64,
        allowed: &[RelayPathKey],
    ) -> bool {
        self.flights.range(..offset).any(|(_, flights)| {
            flights.iter().any(|flight| {
                flight.kind.is_ordering_owner() && !allowed.contains(&flight.instance.key)
            })
        })
    }

    pub(in crate::runtime) fn foreign_ordering_owner_debt_before_offset(
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
                        flight.kind.is_ordering_owner() && !allowed.contains(&flight.instance.key)
                    })
                    .map(|flight| flight.bytes as u64)
            })
            .fold((0usize, 0u64), |(ranges, bytes), flight_bytes| {
                (ranges.saturating_add(1), bytes.saturating_add(flight_bytes))
            })
    }

    pub(in crate::runtime) fn has_repair_flights_before_offset(&self, offset: u64) -> bool {
        self.flights.range(..offset).any(|(_, flights)| {
            flights
                .iter()
                .any(|flight| flight.kind == CarrierWorkKind::RepairData)
        })
    }

    pub(in crate::runtime) fn oldest_lower_flight_owner_before_offset(
        &self,
        offset: u64,
    ) -> Option<RelayPathKey> {
        self.flights.range(..offset).find_map(|(_, flights)| {
            latest_ordering_owner(flights).map(|flight| flight.instance.key)
        })
    }
}

fn latest_ordering_owner(flights: &[RequestFlight]) -> Option<&RequestFlight> {
    flights
        .iter()
        .rev()
        .find(|flight| flight.kind.is_ordering_owner())
}

fn request_ambiguous_flight_intervals(flights: &[(u64, RequestFlight)]) -> Vec<(u64, u64)> {
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

fn flight_intervals_overlap(intervals: &[(u64, u64)], start: u64, end: u64) -> bool {
    let position = intervals.partition_point(|(_, interval_end)| *interval_end <= start);
    intervals
        .get(position)
        .is_some_and(|(interval_start, _)| *interval_start < end)
}

struct FlightIntervalSplit {
    acked: Vec<(u64, u64)>,
    retained: Vec<(u64, u64)>,
}

fn split_flight_interval_by_ack(
    start: u64,
    end: u64,
    ranges: &[OffsetRange],
) -> FlightIntervalSplit {
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
    FlightIntervalSplit { acked, retained }
}

fn flight_interval_bytes(start: u64, end: u64) -> usize {
    usize::try_from(end.saturating_sub(start)).unwrap_or(usize::MAX)
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
#[path = "request_test.rs"]
mod tests;
