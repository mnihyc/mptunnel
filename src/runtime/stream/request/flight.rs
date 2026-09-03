//! Exact request-data flight ownership.
//!
//! Original and reinjected ranges remain keyed by exact attachment instance so
//! Data ACK attribution cannot cross a reconnect boundary.

use super::state::RequestProductQualificationReceipt;
use crate::model::path::{RelayPathInstance, RelayPathKey};
use crate::model::work::{
    CarrierWorkKind, RangeRecoveryState, ReliableFlightSpan, ReliableLiveOwnerFrontier,
    ambiguous_flight_intervals, flight_interval_bytes, flight_intervals_overlap,
    reliable_live_owner_uniform_frontier, split_flight_interval_by_ack,
};
use crate::protocol::frame::{
    normalize_offset_ranges, offset_ranges_not_covered, reliable_stream_frame_extent,
};
use crate::protocol::{Frame, OffsetRange, UnderlayProtocol};
use smallvec::SmallVec;
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct RequestPathRelease {
    pub(in crate::runtime) instance: RelayPathInstance,
    pub(in crate::runtime) range: OffsetRange,
    pub(in crate::runtime) bytes: usize,
    pub(in crate::runtime) kind: CarrierWorkKind,
    pub(in crate::runtime) sent_at: Instant,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) elapsed: Duration,
    pub(in crate::runtime) path_proving: bool,
    /// Exact qualification authority clipped to this released interval.
    pub(in crate::runtime) qualification: Option<RequestProductQualificationReceipt>,
}

#[derive(Debug, Default)]
pub(in crate::runtime) struct RequestFlightLedger {
    // OriginalData identifies the ordered path; ReinjectedData remains a duplicate.
    // Exact attachment instances fence ACK evidence across path replacement.
    flights: BTreeMap<u64, Vec<RequestFlight>>,
    /// Exact un-DataACKed OriginalData bytes across this logical stream.
    ///
    /// The serialized request sender is the only mutation owner. Keeping this
    /// aggregate beside the range ledger makes the shared Product-window apply
    /// check O(1) instead of rescanning every outstanding range per quantum.
    original_data_in_flight_bytes: u64,
    /// Exact un-DataACKed OriginalData bytes by physical attachment instance.
    /// Reconnect successors must start at zero even when they reuse a path key.
    original_data_in_flight_bytes_by_instance: HashMap<RelayPathInstance, u64>,
}

impl RequestFlightLedger {
    /// Exact actor-attached live-owner/accepted-copy shape at the lowest
    /// recovery frontier.  Storage chunk boundaries do not divide the result.
    pub(in crate::runtime) fn live_owner_uniform_frontier(
        &self,
        range: OffsetRange,
        actor_attached_instances: &[RelayPathInstance],
    ) -> Option<ReliableLiveOwnerFrontier<RelayPathInstance>> {
        reliable_live_owner_uniform_frontier(
            range,
            self.flights
                .range(..range.end)
                .flat_map(|(start, flights)| {
                    flights.iter().filter_map(move |flight| {
                        (flight.end > range.start
                            && actor_attached_instances.contains(&flight.instance))
                        .then_some(ReliableFlightSpan {
                            range: OffsetRange {
                                start: *start,
                                end: flight.end,
                            },
                            identity: flight.instance,
                            kind: flight.kind,
                            sent_at: flight.sent_at,
                        })
                    })
                }),
        )
    }

    /// One retained OriginalData range for a non-owning requalification copy.
    ///
    /// The owner need not itself be Qualified: when every attachment is stale,
    /// an evidence-ineligible sole-survivor fallback is the only available
    /// payload source.  The copy never enters this ledger, so the existing
    /// OriginalData owner remains authoritative regardless of probe arrival or
    /// loss.
    pub(in crate::runtime) fn requalification_source_range(
        &self,
        byte_limit: usize,
    ) -> Option<OffsetRange> {
        if byte_limit == 0 {
            return None;
        }
        self.flights.iter().find_map(|(start, flights)| {
            let owner = latest_original_transmission(flights)?;
            let end = owner.end.min(start.saturating_add(byte_limit as u64));
            OffsetRange::new(*start, end)
        })
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_original_frame_instance(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
    ) -> usize {
        self.record_original_frame_instance_with_evidence(instance, frame, true, None)
    }

    pub(in crate::runtime) fn record_original_frame_instance_with_evidence(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
        evidence_eligible: bool,
        qualification: Option<RequestProductQualificationReceipt>,
    ) -> usize {
        self.record_product_frame(
            instance,
            frame,
            CarrierWorkKind::OriginalData,
            evidence_eligible,
            qualification,
            None,
        )
        .0
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_reinjection_frame_instance(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
    ) -> usize {
        self.record_reinjection_frame_instance_with_suppression_interval(
            instance,
            frame,
            Duration::from_secs(1),
        )
        .0
    }

    pub(in crate::runtime) fn record_reinjection_frame_instance_with_suppression_interval(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
        suppression_interval: Duration,
    ) -> (usize, Option<Instant>) {
        self.record_product_frame(
            instance,
            frame,
            CarrierWorkKind::ReinjectedData,
            false,
            None,
            Some(suppression_interval),
        )
    }

    fn record_product_frame(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
        kind: CarrierWorkKind,
        evidence_eligible: bool,
        qualification: Option<RequestProductQualificationReceipt>,
        reinjection_suppression_interval: Option<Duration>,
    ) -> (usize, Option<Instant>) {
        let Some((offset, end, bytes)) = reliable_stream_frame_extent(frame) else {
            return (0, None);
        };
        let sent_at = Instant::now();
        let reinjection_suppression_deadline = reinjection_suppression_interval
            .and_then(|interval| sent_at.checked_add(interval))
            .or(reinjection_suppression_interval.map(|_| sent_at));
        self.flights.entry(offset).or_default().push(RequestFlight {
            instance,
            end,
            bytes,
            sent_at,
            kind,
            evidence_eligible,
            qualification,
            reinjection_suppression_deadline,
        });
        if kind.is_original_transmission() {
            self.original_data_in_flight_bytes = self
                .original_data_in_flight_bytes
                .saturating_add(bytes as u64);
            let instance_bytes = self
                .original_data_in_flight_bytes_by_instance
                .entry(instance)
                .or_default();
            *instance_bytes = instance_bytes.saturating_add(bytes as u64);
        }
        (bytes, reinjection_suppression_deadline)
    }

    /// Revokes every assignment from the pre-stale Product authority epoch.
    /// The flights remain live for exact ACK release and recovery ownership.
    pub(in crate::runtime) fn invalidate_original_evidence(&mut self, instance: RelayPathInstance) {
        for flights in self.flights.values_mut() {
            for flight in flights.iter_mut().filter(|flight| {
                flight.instance == instance && flight.kind.is_original_transmission()
            }) {
                flight.evidence_eligible = false;
            }
        }
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
                if flight.kind.is_original_transmission() {
                    self.original_data_in_flight_bytes = self
                        .original_data_in_flight_bytes
                        .checked_sub(bytes as u64)
                        .expect("request shared OriginalData debt covers released flight");
                    let instance_bytes = self
                        .original_data_in_flight_bytes_by_instance
                        .get_mut(&flight.instance)
                        .expect("request exact-instance OriginalData debt covers released flight");
                    *instance_bytes = instance_bytes
                        .checked_sub(bytes as u64)
                        .expect("request exact-instance OriginalData debt covers released bytes");
                    let remove_instance = *instance_bytes == 0;
                    if remove_instance {
                        self.original_data_in_flight_bytes_by_instance
                            .remove(&flight.instance);
                    }
                }
                let path_proving = flight.evidence_eligible
                    && flight.kind.is_original_transmission()
                    && !flight_intervals_overlap(&ambiguous_intervals, acked_start, acked_end);
                released.push(RequestPathRelease {
                    instance: flight.instance,
                    range: OffsetRange {
                        start: acked_start,
                        end: acked_end,
                    },
                    bytes,
                    kind: flight.kind,
                    sent_at: flight.sent_at,
                    elapsed: now.saturating_duration_since(flight.sent_at),
                    path_proving,
                    qualification: flight.qualification.and_then(|qualification| {
                        qualification.intersect(OffsetRange {
                            start: acked_start,
                            end: acked_end,
                        })
                    }),
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
                        qualification: flight.qualification.and_then(|qualification| {
                            qualification.intersect(OffsetRange {
                                start: retained_start,
                                end: retained_end,
                            })
                        }),
                        ..flight
                    });
            }
        }
        released
    }

    pub(in crate::runtime) fn drain_all(&mut self) -> Vec<RequestPathRelease> {
        let mut released = Vec::new();
        self.original_data_in_flight_bytes = 0;
        self.original_data_in_flight_bytes_by_instance.clear();
        for (start, flights) in std::mem::take(&mut self.flights) {
            for flight in flights {
                released.push(RequestPathRelease {
                    instance: flight.instance,
                    range: OffsetRange {
                        start,
                        end: flight.end,
                    },
                    bytes: flight.bytes,
                    kind: flight.kind,
                    sent_at: flight.sent_at,
                    elapsed: Instant::now().saturating_duration_since(flight.sent_at),
                    path_proving: false,
                    qualification: flight.qualification.and_then(|qualification| {
                        qualification.intersect(OffsetRange {
                            start,
                            end: flight.end,
                        })
                    }),
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

    /// Exact OriginalData qualification authorities overlapping one accepted
    /// duplicate. The caller consumes them before publishing ReinjectedData,
    /// so a later ACK cannot attribute the duplicated bytes uniquely.
    pub(in crate::runtime) fn overlapping_original_qualification_receipts(
        &self,
        range: OffsetRange,
    ) -> Vec<RequestProductQualificationReceipt> {
        if range.is_empty() {
            return Vec::new();
        }
        self.flights
            .range(..range.end)
            .flat_map(|(start, flights)| {
                flights.iter().filter_map(move |flight| {
                    (flight.kind.is_original_transmission() && flight.end > range.start)
                        .then_some(flight.qualification)
                        .flatten()
                        .and_then(|qualification| {
                            qualification.intersect(OffsetRange {
                                start: (*start).max(range.start),
                                end: flight.end.min(range.end),
                            })
                        })
                })
            })
            .collect()
    }

    /// Exact unique Data Sequence bytes still owned by one attachment. This is
    /// the portable ordered-stream backlog when native per-socket state is not
    /// available; reinjection copies never contribute to it.
    pub(in crate::runtime) fn original_data_in_flight_bytes(
        &self,
        instance: RelayPathInstance,
    ) -> u64 {
        self.original_data_in_flight_bytes_by_instance
            .get(&instance)
            .copied()
            .unwrap_or(0)
    }

    /// Exact shared OriginalData debt for the logical stream.
    pub(in crate::runtime) fn total_original_data_in_flight_bytes(&self) -> u64 {
        self.original_data_in_flight_bytes
    }

    /// Every exact un-DataACKed ReinjectedData byte accepted on one attachment
    /// instance. A suppression deadline may authorize another exact target;
    /// it cannot renew the same reliable carrier's recovery authority.
    pub(in crate::runtime) fn reinjected_data_in_flight_bytes(
        &self,
        instance: RelayPathInstance,
    ) -> usize {
        self.flights
            .values()
            .flat_map(|flights| flights.iter())
            .filter(|flight| {
                flight.instance == instance && flight.kind == CarrierWorkKind::ReinjectedData
            })
            .fold(0usize, |bytes, flight| bytes.saturating_add(flight.bytes))
    }

    /// Earliest immutable accepted-copy expiry overlapping one exact range on
    /// any attachment still owned by this actor.
    pub(in crate::runtime) fn reinjection_suppression_deadline_for_frame(
        &self,
        frame: &Frame,
        actor_attached_paths: &[RelayPathInstance],
    ) -> Option<Instant> {
        let (start, end, _) = reliable_stream_frame_extent(frame)?;
        let now = Instant::now();
        self.flights
            .range(..end)
            .flat_map(|(offset, flights)| {
                flights
                    .iter()
                    .filter(move |flight| *offset < end && flight.end > start)
            })
            .filter(|flight| {
                flight.kind == CarrierWorkKind::ReinjectedData
                    && actor_attached_paths.contains(&flight.instance)
            })
            .filter_map(|flight| flight.reinjection_suppression_deadline)
            .filter(|deadline| *deadline > now)
            .min()
    }

    /// Earliest immutable expiry of an accepted ReinjectedData copy still
    /// owned by this actor's exact attachment set.
    pub(in crate::runtime) fn earliest_reinjection_suppression_deadline(
        &self,
        actor_attached_paths: &[RelayPathInstance],
    ) -> Option<Instant> {
        let now = Instant::now();
        self.flights
            .values()
            .flat_map(|flights| flights.iter())
            .filter(|flight| {
                flight.kind == CarrierWorkKind::ReinjectedData
                    && actor_attached_paths.contains(&flight.instance)
            })
            .filter_map(|flight| flight.reinjection_suppression_deadline)
            .filter(|deadline| *deadline > now)
            .min()
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
                flight.reinjection_suppression_deadline = flight
                    .reinjection_suppression_deadline
                    .and_then(|deadline| deadline.checked_sub(elapsed));
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
                        && flight
                            .reinjection_suppression_deadline
                            .is_some_and(|deadline| deadline > now)
                })
        });
        if recent_distinct_reinjection {
            Vec::new()
        } else {
            owner_keys
        }
    }

    #[cfg(test)]
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

    /// Exact current-epoch attachment owners with retained unacknowledged
    /// OriginalData below a complete authoritative Data ACK horizon. The
    /// caller filters current attachment liveness.
    pub(in crate::runtime) fn unacked_original_paths_before(
        &self,
        authoritative_horizon: u64,
    ) -> SmallVec<[RelayPathInstance; 4]> {
        let mut paths = SmallVec::new();
        for (_, flights) in self.flights.range(..authoritative_horizon) {
            let Some(original) = latest_original_transmission(flights) else {
                continue;
            };
            if original.evidence_eligible && !paths.contains(&original.instance) {
                paths.push(original.instance);
            }
        }
        paths
    }

    /// Observes one stale owner's due ranges and next exact-copy expiry in one
    /// ledger pass. Each carrier remains responsible for its own flights.
    pub(in crate::runtime) fn range_recovery_state(
        &self,
        original_path: RelayPathInstance,
        actor_attached_paths: &[RelayPathInstance],
    ) -> RangeRecoveryState {
        let now = Instant::now();
        let mut original_ranges = Vec::new();
        let mut current_reinjections = Vec::new();
        for (start, flights) in &self.flights {
            if let Some(owner) = latest_original_transmission(flights)
                && owner.instance == original_path
            {
                original_ranges.push(OffsetRange {
                    start: *start,
                    end: owner.end,
                });
            }
            for flight in flights {
                if flight.kind != CarrierWorkKind::ReinjectedData
                    || flight.instance == original_path
                    || !actor_attached_paths.contains(&flight.instance)
                {
                    continue;
                }
                let Some(deadline) = flight.reinjection_suppression_deadline else {
                    continue;
                };
                if deadline > now {
                    current_reinjections.push((
                        OffsetRange {
                            start: *start,
                            end: flight.end,
                        },
                        deadline,
                    ));
                }
            }
        }
        let original_ranges = normalize_offset_ranges(original_ranges);
        if original_ranges.is_empty() {
            return RangeRecoveryState::default();
        }

        let mut covered_ranges = Vec::new();
        let mut retry_deadline = None;
        let mut original_index = 0usize;
        for (range, deadline) in current_reinjections {
            while original_index < original_ranges.len()
                && original_ranges[original_index].end <= range.start
            {
                original_index += 1;
            }
            if original_ranges
                .get(original_index)
                .is_some_and(|original| original.start < range.end)
            {
                covered_ranges.push(range);
                retry_deadline =
                    Some(retry_deadline.map_or(deadline, |current: Instant| current.min(deadline)));
            }
        }
        RangeRecoveryState {
            uncovered_ranges: offset_ranges_not_covered(
                &original_ranges,
                &normalize_offset_ranges(covered_ranges),
            ),
            retry_deadline,
        }
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
        self.oldest_lower_flight_owner_instance_before_offset(offset)
            .map(|instance| instance.key)
    }

    /// Exact attachment owning the oldest OriginalData range below `offset`.
    /// A reconnect may reuse a configured key, so apply-time authority must not
    /// collapse this identity to `RelayPathKey`.
    pub(in crate::runtime) fn oldest_lower_flight_owner_instance_before_offset(
        &self,
        offset: u64,
    ) -> Option<RelayPathInstance> {
        self.flights.range(..offset).find_map(|(_, flights)| {
            latest_original_transmission(flights).map(|flight| flight.instance)
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
    evidence_eligible: bool,
    qualification: Option<RequestProductQualificationReceipt>,
    /// Frozen from the selected carrier's exact snapshot at command commit.
    reinjection_suppression_deadline: Option<Instant>,
}

#[cfg(test)]
#[path = "tests_flight.rs"]
mod tests;
