use super::ack_clock_policy::reliable_ack_clock_calibration_limit_bytes;
#[cfg(feature = "lab-diagnostics")]
use super::bulk_admission::bulk_completion_horizon_ms_with_ordering_debt;
use super::bulk_admission::{
    BulkAdmissionCheck, BulkAdmissionRole, bulk_additional_admission_role,
    bulk_candidate_admission_suppression_with_ordering_debt, bulk_service_horizon_payload_bytes,
};
use super::*;
use std::collections::BTreeMap;

// Client/request-side striping owns dispatch choices and its exact flight
// ledger. Carrier TCP/QUIC controllers remain responsible for wire emission.

#[derive(Debug, Clone, Copy)]
pub(super) struct RelayPathRelease {
    pub(super) key: RelayPathKey,
    pub(super) instance: RelayPathInstance,
    pub(super) bytes: usize,
    pub(super) sent_at: Instant,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(super) elapsed: Duration,
    pub(super) path_proving: bool,
}

#[derive(Debug, Default)]
pub(super) struct RelayPathFlightLedger {
    // Client/request-side product-flight ledger. It intentionally mirrors the
    // response binding's OwnerData/RepairData ACK rule. Logical keys drive
    // scheduling and repair; exact attachment instances fence ACK evidence.
    flights: BTreeMap<u64, Vec<RelayPathFlight>>,
}

impl RelayPathFlightLedger {
    #[cfg(test)]
    pub(super) fn record_owner_frame(&mut self, key: RelayPathKey, frame: &Frame) -> usize {
        self.record_owner_frame_instance(RelayPathInstance { key, id: 0 }, frame)
    }

    #[cfg(test)]
    pub(super) fn record_repair_frame(&mut self, key: RelayPathKey, frame: &Frame) -> usize {
        self.record_repair_frame_instance(RelayPathInstance { key, id: 0 }, frame)
    }

    pub(super) fn record_owner_frame_instance(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
    ) -> usize {
        self.record_product_frame(instance, frame, CarrierWorkKind::OwnerData)
    }

    pub(super) fn record_repair_frame_instance(
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
        self.flights
            .entry(offset)
            .or_default()
            .push(RelayPathFlight {
                instance,
                end,
                bytes,
                sent_at: Instant::now(),
                kind,
            });
        bytes
    }

    pub(super) fn release_normalized_acked_ranges(
        &mut self,
        ranges: &[OffsetRange],
    ) -> Vec<RelayPathRelease> {
        if ranges.is_empty() || self.flights.is_empty() {
            return Vec::new();
        }

        let original_flights = std::mem::take(&mut self.flights)
            .into_iter()
            .flat_map(|(start, flights)| flights.into_iter().map(move |flight| (start, flight)))
            .collect::<Vec<_>>();
        let ambiguous_intervals = relay_path_ambiguous_flight_intervals(&original_flights);
        let now = Instant::now();
        let mut released = Vec::new();
        for (start, flight) in original_flights.iter().copied() {
            let split = split_flight_interval_by_ack(start, flight.end, ranges);
            for (acked_start, acked_end) in split.acked {
                let bytes = flight_interval_bytes(acked_start, acked_end);
                if bytes == 0 {
                    continue;
                }
                released.push(RelayPathRelease {
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
                    .push(RelayPathFlight {
                        end: retained_end,
                        bytes,
                        ..flight
                    });
            }
        }
        released
    }

    pub(super) fn drain_all(&mut self) -> Vec<RelayPathRelease> {
        let mut released = Vec::new();
        for flights in std::mem::take(&mut self.flights).into_values() {
            for flight in flights {
                released.push(RelayPathRelease {
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
    pub(super) fn age_product_flights_for_test(&mut self, age: Duration) {
        let sent_at = Instant::now().checked_sub(age).unwrap_or_else(Instant::now);
        for flights in self.flights.values_mut() {
            for flight in flights {
                flight.sent_at = sent_at;
            }
        }
    }

    pub(super) fn sent_keys_for_frame(&self, frame: &Frame) -> Vec<RelayPathKey> {
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

    pub(super) fn has_missing_ordering_owner_before_offset(
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

    pub(super) fn ordering_owner_keys_for_frame(
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

    pub(super) fn ordering_owner_underlay_for_frame(
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

    pub(super) fn ordering_owner_keys_for_frame_any_instance(
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

    pub(super) fn live_owner_tail_repair_owner_keys(
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
    pub(super) fn latest_unacked_ranges_for_path(&self, key: RelayPathKey) -> Vec<OffsetRange> {
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

    pub(super) fn latest_unacked_ranges_for_path_instance(
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

    pub(super) fn ordering_owner_instances(&self) -> Vec<RelayPathInstance> {
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

    pub(super) fn ordering_debt_bytes_before_offset(&self, key: RelayPathKey, offset: u64) -> u64 {
        self.flights
            .range(..offset)
            .filter_map(|(_, flights)| {
                let latest = latest_ordering_owner(flights)?;
                (latest.instance.key != key).then_some(latest.bytes as u64)
            })
            .sum()
    }

    pub(super) fn has_ordering_owner_flights_for_instance(
        &self,
        instance: RelayPathInstance,
    ) -> bool {
        self.flights.values().any(|flights| {
            flights
                .iter()
                .any(|flight| flight.instance == instance && flight.kind.is_ordering_owner())
        })
    }

    pub(super) fn oldest_ordering_owner_age_before_offset(&self, offset: u64) -> Option<Duration> {
        let now = Instant::now();
        self.flights
            .range(..offset)
            .flat_map(|(_, flights)| flights.iter())
            .filter(|flight| flight.kind.is_ordering_owner())
            .map(|flight| now.saturating_duration_since(flight.sent_at))
            .max()
    }

    pub(super) fn has_foreign_ordering_owner_before_offset(
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

    pub(super) fn has_repair_flights_before_offset(&self, offset: u64) -> bool {
        self.flights.range(..offset).any(|(_, flights)| {
            flights
                .iter()
                .any(|flight| flight.kind == CarrierWorkKind::RepairData)
        })
    }

    pub(super) fn oldest_lower_flight_owner_before_offset(
        &self,
        offset: u64,
    ) -> Option<RelayPathKey> {
        self.flights.range(..offset).find_map(|(_, flights)| {
            latest_ordering_owner(flights).map(|flight| flight.instance.key)
        })
    }
}

fn latest_ordering_owner(flights: &[RelayPathFlight]) -> Option<&RelayPathFlight> {
    flights
        .iter()
        .rev()
        .find(|flight| flight.kind.is_ordering_owner())
}

fn relay_path_ambiguous_flight_intervals(flights: &[(u64, RelayPathFlight)]) -> Vec<(u64, u64)> {
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

pub(super) fn normalized_offset_ranges(ranges: &[OffsetRange]) -> Vec<OffsetRange> {
    let mut ranges = ranges.to_vec();
    ranges.sort_unstable_by_key(|range| (range.start, range.end));
    let mut merged: Vec<OffsetRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if range.start >= range.end {
            continue;
        }
        match merged.last_mut() {
            Some(previous) if previous.end >= range.start => {
                previous.end = previous.end.max(range.end);
            }
            _ => merged.push(range),
        }
    }
    merged
}

#[derive(Debug, Clone, Copy)]
struct RelayPathFlight {
    instance: RelayPathInstance,
    end: u64,
    bytes: usize,
    sent_at: Instant,
    kind: CarrierWorkKind,
}

pub(super) fn reliable_stream_frame_extent(frame: &Frame) -> Option<(u64, u64, usize)> {
    let Frame::StreamData {
        offset, payload, ..
    } = frame
    else {
        return None;
    };
    let bytes = payload.len();
    if bytes == 0 {
        return None;
    }
    let end = offset.saturating_add(bytes as u64);
    Some((*offset, end, bytes))
}

pub(super) fn reliable_stream_frame_payload_bytes(frame: &Frame) -> usize {
    reliable_stream_frame_extent(frame).map_or(1, |(_, _, bytes)| bytes)
}

pub(super) fn relay_frame_is_bulk_stream_data(frame: &Frame, lane: FlowLane) -> bool {
    lane.is_bulk() && matches!(frame, Frame::StreamData { .. })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BulkRelayPathChoice {
    Selected(usize),
    SelectedStartupSubflow {
        position: usize,
        service: RelayPathInstance,
        candidate: RelayPathInstance,
    },
    SelectedAckClockCalibration {
        position: usize,
        candidate: RelayPathInstance,
    },
    Blocked,
    NotApplicable,
}

#[derive(Clone, Copy)]
pub(super) struct RequestAckClockCalibration<'a> {
    pub(super) proven_subflows: &'a HashSet<RelayPathInstance>,
    pub(super) spent_bytes: &'a HashMap<RelayPathInstance, u64>,
}

pub(super) struct BulkRelayPathRequest<'a> {
    pub(super) stream_id: StreamId,
    pub(super) context: &'a ClientPathContext,
    pub(super) paths: &'a [ReliableRelayRemotePath],
    pub(super) lane: FlowLane,
    pub(super) frame: Option<&'a Frame>,
    pub(super) offset: u64,
    pub(super) payload_bytes: usize,
    pub(super) cursor: usize,
    pub(super) avoid_keys: &'a [RelayPathKey],
    pub(super) path_flights: Option<&'a RelayPathFlightLedger>,
    pub(super) ordered_data_owner: Option<RelayPathKey>,
    pub(super) subflow_set: Option<&'a FlowSubflowSet<RelayPathInstance>>,
    pub(super) proven_subflows: Option<&'a HashSet<RelayPathInstance>>,
    pub(super) graduated_subflows: Option<&'a HashSet<RelayPathInstance>>,
    pub(super) attempted_subflows: Option<&'a HashSet<RelayPathInstance>>,
    pub(super) ack_clock_calibration: Option<RequestAckClockCalibration<'a>>,
}

pub(super) struct BulkRelayFrameRequest<'a> {
    pub(super) stream_id: StreamId,
    pub(super) context: &'a ClientPathContext,
    pub(super) paths: &'a [ReliableRelayRemotePath],
    pub(super) lane: FlowLane,
    pub(super) frame: &'a Frame,
    pub(super) cursor: usize,
    pub(super) avoid_keys: &'a [RelayPathKey],
    pub(super) path_flights: Option<&'a RelayPathFlightLedger>,
    pub(super) ordered_data_owner: Option<RelayPathKey>,
    pub(super) subflow_set: Option<&'a FlowSubflowSet<RelayPathInstance>>,
    pub(super) proven_subflows: Option<&'a HashSet<RelayPathInstance>>,
    pub(super) graduated_subflows: Option<&'a HashSet<RelayPathInstance>>,
    pub(super) attempted_subflows: Option<&'a HashSet<RelayPathInstance>>,
    pub(super) ack_clock_calibration: Option<RequestAckClockCalibration<'a>>,
}

#[derive(Debug, Clone, Copy)]
struct RelayBulkLead {
    key: RelayPathKey,
    snapshot: PathSnapshot,
    eta_ms: f64,
}

fn relay_path_runtime_role(
    key: RelayPathKey,
    active_key: Option<RelayPathKey>,
    lower_flight_owner: Option<RelayPathKey>,
    has_bulk_model_evidence: bool,
) -> PathRuntimeRole {
    if Some(key) == active_key || Some(key) == lower_flight_owner {
        PathRuntimeRole::Service
    } else if has_bulk_model_evidence {
        PathRuntimeRole::Subflow
    } else {
        PathRuntimeRole::Standby
    }
}

fn request_startup_product_envelope_bytes(payload_bytes: usize, mux_limits: MuxLimits) -> u64 {
    (mux_limits.max_path_flight_bytes as u64)
        .min(mux_limits.max_repair_bytes as u64)
        .min(mux_limits.max_reorder_bytes as u64)
        .min(mux_limits.max_stream_window_bytes)
        .max(payload_bytes as u64)
}

#[allow(clippy::too_many_arguments)]
fn choose_request_startup_subflow(
    context: &ClientPathContext,
    paths: &[ReliableRelayRemotePath],
    lane: FlowLane,
    frame: Option<&Frame>,
    offset: u64,
    payload_bytes: usize,
    active_key: Option<RelayPathKey>,
    path_flights: Option<&RelayPathFlightLedger>,
    subflow_set: Option<&FlowSubflowSet<RelayPathInstance>>,
    proven_subflows: Option<&HashSet<RelayPathInstance>>,
    graduated_subflows: Option<&HashSet<RelayPathInstance>>,
    attempted_subflows: Option<&HashSet<RelayPathInstance>>,
) -> Option<BulkRelayPathChoice> {
    let service_key = active_key?;
    let service = paths
        .iter()
        .find(|path| path.key() == service_key && path.placement == RelayPathPlacement::Active)?;
    let service_instance = service.instance();
    if proven_subflows.is_some_and(|proven| !proven.contains(&service_instance)) {
        return None;
    }
    let flights = path_flights?;
    let mut allowed_lower_owners = vec![service_key];
    if let Some(startup_key) = subflow_set
        .and_then(FlowSubflowSet::startup_owner_key)
        .map(|instance| instance.key)
        && !allowed_lower_owners.contains(&startup_key)
    {
        allowed_lower_owners.push(startup_key);
    }
    if flights.has_foreign_ordering_owner_before_offset(offset, &allowed_lower_owners)
        || flights.has_repair_flights_before_offset(offset)
    {
        return None;
    }
    if !context.relay_path_has_bulk_model_evidence(service_key.underlay, service_key.index)
        || context.reliable_relay_has_latency_pressure()
    {
        return None;
    }
    let service_snapshot = context.reliable_path_snapshot(service_key)?;
    scheduler::score_path(
        service_snapshot,
        lane,
        payload_bytes,
        SchedulerPolicy::default(),
    )?;
    if service_snapshot.active_latency_sensitive_flows > 0
        || service_snapshot.session_active_latency_sensitive_flows > 0
    {
        return None;
    }
    let oldest_lower_age = flights.oldest_ordering_owner_age_before_offset(offset);
    if oldest_lower_age
        .is_some_and(|age| age >= reliable_relay_tail_repair_delay(Some(service_snapshot), lane))
    {
        return None;
    }

    let startup_credit = usize::try_from(reliable_subflow_startup_sample_limit_bytes(
        context.mux_limits,
    ))
    .unwrap_or(usize::MAX);
    let current_epoch = subflow_set.filter(|epoch| {
        epoch.matches_envelope(service_instance, startup_credit, 0, Duration::ZERO)
    });
    let mut epoch = current_epoch.cloned().unwrap_or_else(|| {
        FlowSubflowSet::new(0, service_instance, startup_credit, 0, Duration::ZERO)
    });
    let product_envelope =
        request_startup_product_envelope_bytes(payload_bytes, context.mux_limits);
    let mut candidates = paths
        .iter()
        .enumerate()
        .filter(|(_, path)| path.placement == RelayPathPlacement::Validation)
        .filter(|(_, path)| path.key().underlay == service_key.underlay)
        .filter(|(_, path)| {
            graduated_subflows.is_none_or(|graduated| !graduated.contains(&path.instance()))
        })
        .filter(|(_, path)| {
            subflow_set.and_then(FlowSubflowSet::startup_owner_key) == Some(path.instance())
                || attempted_subflows.is_none_or(|attempted| !attempted.contains(&path.instance()))
        })
        .filter(|(_, path)| {
            frame
                .map(|frame| path.stream.can_enqueue_frame_now(frame, lane))
                .unwrap_or(true)
        })
        .filter(|(_, path)| {
            path.path_proof_id.is_some_and(|proof_id| {
                context.relay_path_has_fresh_proof(
                    path.key().underlay,
                    path.key().index,
                    proof_id,
                    path.attached_at,
                )
            })
        })
        .filter_map(|(position, path)| {
            let snapshot = context.reliable_path_snapshot(path.key())?;
            if snapshot.active_latency_sensitive_flows > 0
                || snapshot.session_active_latency_sensitive_flows > 0
            {
                return None;
            }
            let ordering_debt = flights.ordering_debt_bytes_before_offset(path.key(), offset);
            let projected_product_debt = ordering_debt
                .saturating_add(snapshot.product_bytes_in_flight)
                .saturating_add(snapshot.product_queue_bytes)
                .saturating_add(payload_bytes as u64);
            if projected_product_debt > product_envelope {
                return None;
            }
            let score =
                scheduler::score_path(snapshot, lane, payload_bytes, SchedulerPolicy::default())?;
            Some((position, path, score.eta_ms))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        left.2
            .total_cmp(&right.2)
            .then_with(|| left.1.instance_id.cmp(&right.1.instance_id))
    });

    for (position, path, _) in candidates {
        let input = SubflowAdmissionInput {
            key: path.instance(),
            bulk_rate_proven: false,
            startup_owner_allowed: true,
            frontier_clear: true,
            completion_improves: false,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: payload_bytes,
            optional_overhead_bytes: 0,
        };
        if epoch.admit_subflow_owner(input).decision == PathAdmissionDecision::AdmitSubflow {
            return Some(BulkRelayPathChoice::SelectedStartupSubflow {
                position,
                service: service_instance,
                candidate: path.instance(),
            });
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
fn choose_request_ack_clock_calibration(
    context: &ClientPathContext,
    paths: &[ReliableRelayRemotePath],
    lane: FlowLane,
    frame: Option<&Frame>,
    offset: u64,
    payload_bytes: usize,
    cursor: usize,
    active_key: Option<RelayPathKey>,
    path_flights: Option<&RelayPathFlightLedger>,
    subflow_set: Option<&FlowSubflowSet<RelayPathInstance>>,
    proven_subflows: Option<&HashSet<RelayPathInstance>>,
    graduated_subflows: Option<&HashSet<RelayPathInstance>>,
    calibration: Option<RequestAckClockCalibration<'_>>,
) -> Option<BulkRelayPathChoice> {
    let service_key = active_key?;
    let service = paths
        .iter()
        .find(|path| path.key() == service_key && path.placement == RelayPathPlacement::Active)?;
    let service_instance = service.instance();
    let service_proven = proven_subflows.is_none_or(|proven| proven.contains(&service_instance));
    let startup_owner = subflow_set.and_then(FlowSubflowSet::startup_owner_key);
    let latency_pressure = context.reliable_relay_has_latency_pressure();
    let service_bulk_evidence =
        context.relay_path_has_bulk_model_evidence(service_key.underlay, service_key.index);
    if !service_proven || startup_owner.is_some() || latency_pressure || !service_bulk_evidence {
        #[cfg(feature = "lab-diagnostics")]
        if graduated_subflows.is_some_and(|graduated| !graduated.is_empty()) {
            static EARLY_TRACE_COUNT: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            let count = EARLY_TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if count < 16 || count % 1024 == 0 {
                lab_diagnostic(
                    "ack_clock_calibration",
                    format_args!(
                        "phase=early_gate service_underlay={:?} service_index={} service_instance={} service_proven={} startup_owner={} latency_pressure={} service_bulk_evidence={}",
                        service_key.underlay,
                        service_key.index,
                        service_instance.id,
                        service_proven,
                        startup_owner.is_some(),
                        latency_pressure,
                        service_bulk_evidence,
                    ),
                );
            }
        }
        return None;
    }
    let flights = path_flights?;
    if flights.has_repair_flights_before_offset(offset) {
        #[cfg(feature = "lab-diagnostics")]
        if graduated_subflows.is_some_and(|graduated| !graduated.is_empty()) {
            static REPAIR_TRACE_COUNT: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            let count = REPAIR_TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if count < 16 || count % 1024 == 0 {
                lab_diagnostic(
                    "ack_clock_calibration",
                    format_args!(
                        "phase=repair_gate service_underlay={:?} service_index={} service_instance={} offset={}",
                        service_key.underlay, service_key.index, service_instance.id, offset,
                    ),
                );
            }
        }
        return None;
    }
    let service_snapshot = context.reliable_path_snapshot(service_key)?;
    if flights
        .oldest_ordering_owner_age_before_offset(offset)
        .is_some_and(|age| age >= reliable_relay_tail_repair_delay(Some(service_snapshot), lane))
    {
        #[cfg(feature = "lab-diagnostics")]
        if graduated_subflows.is_some_and(|graduated| !graduated.is_empty()) {
            static TAIL_TRACE_COUNT: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            let count = TAIL_TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if count < 16 || count % 1024 == 0 {
                lab_diagnostic(
                    "ack_clock_calibration",
                    format_args!(
                        "phase=stale_tail_gate service_underlay={:?} service_index={} service_instance={} offset={}",
                        service_key.underlay, service_key.index, service_instance.id, offset,
                    ),
                );
            }
        }
        return None;
    }
    let graduated = graduated_subflows?;
    let calibration = calibration?;
    let limit = reliable_ack_clock_calibration_limit_bytes(context.mux_limits);
    let product_envelope =
        request_startup_product_envelope_bytes(payload_bytes, context.mux_limits);
    let mut allowed_owner_keys = vec![service_key];
    for path in paths.iter().filter(|path| {
        path.key().underlay == service_key.underlay && graduated.contains(&path.instance())
    }) {
        if !allowed_owner_keys.contains(&path.key()) {
            allowed_owner_keys.push(path.key());
        }
    }
    let pending_calibration = paths
        .iter()
        .filter(|path| path.placement == RelayPathPlacement::Validation)
        .filter(|path| path.key().underlay == service_key.underlay)
        .filter(|path| graduated.contains(&path.instance()))
        .filter(|path| !calibration.proven_subflows.contains(&path.instance()))
        .filter(|path| {
            calibration
                .spent_bytes
                .get(&path.instance())
                .copied()
                .unwrap_or(0)
                > 0
        })
        .filter(|path| {
            let spent = calibration
                .spent_bytes
                .get(&path.instance())
                .copied()
                .unwrap_or(0);
            spent.saturating_add(payload_bytes as u64) <= limit
                || flights.has_ordering_owner_flights_for_instance(path.instance())
        })
        .map(ReliableRelayRemotePath::instance)
        .min_by_key(|instance| instance.id);

    #[cfg(feature = "lab-diagnostics")]
    {
        static CANDIDATE_TRACE_COUNT: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let count = CANDIDATE_TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if count < 32 || count % 1024 == 0 {
            for path in paths.iter().filter(|path| {
                path.placement == RelayPathPlacement::Validation
                    && graduated.contains(&path.instance())
                    && !calibration.proven_subflows.contains(&path.instance())
            }) {
                let proof_fresh = path.path_proof_id.is_some_and(|proof_id| {
                    context.relay_path_has_fresh_proof(
                        path.key().underlay,
                        path.key().index,
                        proof_id,
                        path.attached_at,
                    )
                });
                let candidate_bulk_evidence = context
                    .relay_path_has_bulk_model_evidence(path.key().underlay, path.key().index);
                let snapshot = context.reliable_path_snapshot(path.key());
                let spent = calibration
                    .spent_bytes
                    .get(&path.instance())
                    .copied()
                    .unwrap_or(0);
                let foreign_owner =
                    flights.has_foreign_ordering_owner_before_offset(offset, &allowed_owner_keys);
                let ordering_debt = flights.ordering_debt_bytes_before_offset(path.key(), offset);
                lab_diagnostic(
                    "ack_clock_calibration",
                    format_args!(
                        "phase=candidate stream_id={} underlay={:?} path_index={} instance_id={} same_underlay={} proof_fresh={} bulk_evidence={} spent_bytes={} limit_bytes={} payload_bytes={} foreign_owner={} ordering_debt={} product_envelope={} can_enqueue={} product_inflight={} product_queue={} active_latency={} session_latency={}",
                        path.stream.stream_id.0,
                        path.key().underlay,
                        path.key().index,
                        path.instance_id,
                        path.key().underlay == service_key.underlay,
                        proof_fresh,
                        candidate_bulk_evidence,
                        spent,
                        limit,
                        payload_bytes,
                        foreign_owner,
                        ordering_debt,
                        product_envelope,
                        frame
                            .map(|frame| path.stream.can_enqueue_frame_now(frame, lane))
                            .unwrap_or(true),
                        snapshot.map_or(0, |snapshot| snapshot.product_bytes_in_flight),
                        snapshot.map_or(0, |snapshot| snapshot.product_queue_bytes),
                        snapshot.map_or(0, |snapshot| snapshot.active_latency_sensitive_flows),
                        snapshot.map_or(0, |snapshot| snapshot
                            .session_active_latency_sensitive_flows),
                    ),
                );
            }
        }
    }

    paths
        .iter()
        .enumerate()
        .filter(|(_, path)| path.placement == RelayPathPlacement::Validation)
        .filter(|(_, path)| path.key().underlay == service_key.underlay)
        .filter(|(_, path)| graduated.contains(&path.instance()))
        .filter(|(_, path)| !calibration.proven_subflows.contains(&path.instance()))
        .filter(|(_, path)| pending_calibration.is_none_or(|owner| owner == path.instance()))
        .filter(|(_, path)| {
            calibration
                .spent_bytes
                .get(&path.instance())
                .copied()
                .unwrap_or(0)
                .saturating_add(payload_bytes as u64)
                <= limit
        })
        .filter(|(_, path)| {
            frame
                .map(|frame| path.stream.can_enqueue_frame_now(frame, lane))
                .unwrap_or(true)
        })
        .filter(|(_, path)| {
            path.path_proof_id.is_some_and(|proof_id| {
                context.relay_path_has_fresh_proof(
                    path.key().underlay,
                    path.key().index,
                    proof_id,
                    path.attached_at,
                )
            })
        })
        .filter(|_| !flights.has_foreign_ordering_owner_before_offset(offset, &allowed_owner_keys))
        .filter_map(|(position, path)| {
            let snapshot = context.reliable_path_snapshot(path.key())?;
            if snapshot.active_latency_sensitive_flows > 0
                || snapshot.session_active_latency_sensitive_flows > 0
                || !context
                    .relay_path_has_bulk_model_evidence(path.key().underlay, path.key().index)
            {
                return None;
            }
            let ordering_debt = flights.ordering_debt_bytes_before_offset(path.key(), offset);
            let candidate_product_debt = snapshot
                .product_bytes_in_flight
                .saturating_add(snapshot.product_queue_bytes)
                .saturating_add(payload_bytes as u64);
            if candidate_product_debt > limit
                || ordering_debt.saturating_add(candidate_product_debt) > product_envelope
            {
                return None;
            }
            let score =
                scheduler::score_path(snapshot, lane, payload_bytes, SchedulerPolicy::default())?;
            let spent = calibration
                .spent_bytes
                .get(&path.instance())
                .copied()
                .unwrap_or(0);
            Some((
                position,
                path.instance(),
                spent > 0,
                path_cursor_distance(position, cursor, paths.len()),
                score.eta_ms,
            ))
        })
        .min_by(|left, right| {
            right
                .2
                .cmp(&left.2)
                .then_with(|| left.3.cmp(&right.3))
                .then_with(|| left.4.total_cmp(&right.4))
                .then_with(|| left.1.id.cmp(&right.1.id))
        })
        .map(
            |(position, candidate, _, _, _)| BulkRelayPathChoice::SelectedAckClockCalibration {
                position,
                candidate,
            },
        )
}

#[cfg(feature = "lab-diagnostics")]
#[derive(Debug, Clone, Copy)]
struct BulkRelayCandidateDiagnostics {
    stream_id: StreamId,
    lane: FlowLane,
    key: RelayPathKey,
    lead_key: Option<RelayPathKey>,
    role: Option<BulkAdmissionRole>,
    eta_ms: Option<f64>,
    best_eta_ms: Option<f64>,
    completion_horizon_ms: Option<f64>,
    stream_ordering_debt_bytes: u64,
    payload_bytes: usize,
    snapshot: Option<PathSnapshot>,
}

#[cfg(feature = "lab-diagnostics")]
impl BulkRelayCandidateDiagnostics {
    fn skipped(
        stream_id: StreamId,
        lane: FlowLane,
        key: RelayPathKey,
        lead_key: Option<RelayPathKey>,
        payload_bytes: usize,
    ) -> Self {
        Self {
            stream_id,
            lane,
            key,
            lead_key,
            role: None,
            eta_ms: None,
            best_eta_ms: None,
            completion_horizon_ms: None,
            stream_ordering_debt_bytes: 0,
            payload_bytes,
            snapshot: None,
        }
    }
}

#[cfg(feature = "lab-diagnostics")]
fn log_bulk_relay_candidate_decision(
    diagnostics: BulkRelayCandidateDiagnostics,
    selected: bool,
    reason: &'static str,
) {
    if !lab_diagnostic_event_enabled("scheduler_decision") {
        return;
    }
    let lead_underlay = diagnostics
        .lead_key
        .map(|key| format!("{:?}", key.underlay))
        .unwrap_or_else(|| "none".to_string());
    let lead_index = diagnostics
        .lead_key
        .map(|key| key.index.to_string())
        .unwrap_or_else(|| "none".to_string());
    let role = diagnostics
        .role
        .map(|role| format!("{role:?}"))
        .unwrap_or_else(|| "unknown".to_string());
    let eta_ms = diagnostics
        .eta_ms
        .map(|eta_ms| format!("{eta_ms:.3}"))
        .unwrap_or_else(|| "none".to_string());
    let best_eta_ms = diagnostics
        .best_eta_ms
        .map(|eta_ms| format!("{eta_ms:.3}"))
        .unwrap_or_else(|| "none".to_string());
    let completion_horizon_ms = diagnostics
        .completion_horizon_ms
        .map(|horizon| format!("{horizon:.3}"))
        .unwrap_or_else(|| "none".to_string());
    let (
        product_queue_debt,
        carrier_queue_debt,
        bytes_in_flight,
        inflight_limit,
        confidence,
        app_limited,
        delivery_rate_bps,
        pacing_rate_bps,
    ) = diagnostics
        .snapshot
        .map(|snapshot| {
            (
                snapshot.product_bytes_in_flight,
                snapshot.queue_bytes,
                snapshot.bytes_in_flight,
                snapshot.inflight_limit_bytes,
                snapshot.confidence,
                snapshot.app_limited,
                snapshot.delivery_rate_bps,
                snapshot.pacing_rate_bps,
            )
        })
        .unwrap_or((0, 0, 0, 0, 0.0, false, 0.0, 0.0));

    lab_diagnostic(
        "scheduler_decision",
        format_args!(
            "stream_id={} lane={:?} candidate_underlay={:?} candidate_index={} lead_underlay={} lead_index={} role={} selected={} reason={} eta_ms={} best_eta_ms={} completion_horizon_ms={} stream_ordering_debt_bytes={} payload_bytes={} product_queue_debt={} carrier_queue_debt={} bytes_in_flight={} inflight_limit={} confidence={:.3} app_limited={} delivery_rate_bps={:.0} pacing_rate_bps={:.0} delivery_sample_source=sender_model",
            diagnostics.stream_id.0,
            diagnostics.lane,
            diagnostics.key.underlay,
            diagnostics.key.index,
            lead_underlay,
            lead_index,
            role,
            selected,
            reason,
            eta_ms,
            best_eta_ms,
            completion_horizon_ms,
            diagnostics.stream_ordering_debt_bytes,
            diagnostics.payload_bytes,
            product_queue_debt,
            carrier_queue_debt,
            bytes_in_flight,
            inflight_limit,
            confidence,
            app_limited,
            delivery_rate_bps,
            pacing_rate_bps,
        ),
    );
}

pub(super) fn choose_bulk_relay_path_avoiding(
    request: BulkRelayFrameRequest<'_>,
) -> BulkRelayPathChoice {
    let BulkRelayFrameRequest {
        stream_id,
        context,
        paths,
        lane,
        frame,
        cursor,
        avoid_keys,
        path_flights,
        ordered_data_owner,
        subflow_set,
        proven_subflows,
        graduated_subflows,
        attempted_subflows,
        ack_clock_calibration,
    } = request;
    let Some((offset, _, payload_bytes)) = reliable_stream_frame_extent(frame) else {
        return BulkRelayPathChoice::NotApplicable;
    };
    choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
        stream_id,
        context,
        paths,
        lane,
        frame: Some(frame),
        offset,
        payload_bytes,
        cursor,
        avoid_keys,
        path_flights,
        ordered_data_owner,
        subflow_set,
        proven_subflows,
        graduated_subflows,
        attempted_subflows,
        ack_clock_calibration,
    })
}

pub(super) fn choose_bulk_relay_path_for_extent_avoiding(
    request: BulkRelayPathRequest<'_>,
) -> BulkRelayPathChoice {
    let BulkRelayPathRequest {
        stream_id,
        context,
        paths,
        lane,
        frame,
        offset,
        payload_bytes,
        cursor,
        avoid_keys,
        path_flights,
        ordered_data_owner,
        subflow_set,
        proven_subflows,
        graduated_subflows,
        attempted_subflows,
        ack_clock_calibration,
    } = request;
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = stream_id;
    if !lane.is_bulk() || payload_bytes == 0 {
        return BulkRelayPathChoice::NotApplicable;
    }
    let policy = SchedulerPolicy::default();
    // The request-side primary owner is the existing ordered-data owner when it
    // is still attached; otherwise it is the first active path opened for the
    // stream.  Newly attached active paths are candidate subflows, not automatic
    // service owners.  Using `paths.last()` here made an attached UDP survivor
    // become the service path before any bytes were sent, which blocked the
    // initial TCP owner in the lower-frontier regression test and can also
    // create real upload instability by letting attachment order override
    // product-byte ownership.
    let active_key = ordered_data_owner
        .filter(|owner| paths.iter().any(|path| path.key() == *owner))
        .or_else(|| {
            paths
                .iter()
                .find(|path| path.placement == RelayPathPlacement::Active)
                .map(|path| path.key())
        })
        .or_else(|| paths.last().map(|path| path.key()));
    let normal_bulk_send = avoid_keys.is_empty();
    if paths.len() <= 1 {
        if normal_bulk_send
            && let Some(flights) = path_flights
            && let Some(owner) = flights.oldest_lower_flight_owner_before_offset(offset)
            && paths.first().is_none_or(|path| path.key() != owner)
        {
            return BulkRelayPathChoice::Blocked;
        }
        return BulkRelayPathChoice::NotApplicable;
    }
    let mut admitted_bulk_keys = if normal_bulk_send {
        context.ordered_reliable_bulk_striping_path_keys(payload_bytes)
    } else {
        Vec::new()
    };
    if let Some(graduated) = graduated_subflows {
        admitted_bulk_keys.retain(|key| {
            paths.iter().any(|path| {
                path.key() == *key
                    && (path.placement == RelayPathPlacement::Active
                        || graduated.contains(&path.instance()))
            })
        });
    }
    let lower_flight_owner = if normal_bulk_send {
        path_flights.and_then(|flights| flights.oldest_lower_flight_owner_before_offset(offset))
    } else {
        None
    };
    let lower_owner_cross_path_debt = if normal_bulk_send {
        lower_flight_owner
            .and_then(|owner| {
                path_flights.map(|flights| flights.ordering_debt_bytes_before_offset(owner, offset))
            })
            .unwrap_or(0)
    } else {
        0
    };
    let restrict_to_admitted = normal_bulk_send
        && paths
            .iter()
            .any(|path| admitted_bulk_keys.contains(&path.key()));
    let lead = if normal_bulk_send {
        choose_admissible_relay_bulk_lead(RelayBulkLeadRequest {
            context,
            paths,
            lane,
            payload_bytes,
            frame,
            active_key,
            admitted_bulk_keys: &admitted_bulk_keys,
            restrict_to_admitted,
            lower_flight_owner,
            lower_owner_cross_path_debt,
            policy,
        })
    } else {
        None
    };
    if normal_bulk_send
        && ordered_data_owner.is_some()
        && let Some(choice) = choose_request_startup_subflow(
            context,
            paths,
            lane,
            frame,
            offset,
            payload_bytes,
            active_key,
            path_flights,
            subflow_set,
            proven_subflows,
            graduated_subflows,
            attempted_subflows,
        )
    {
        return choice;
    }
    if normal_bulk_send
        && ordered_data_owner.is_some()
        && let Some(choice) = choose_request_ack_clock_calibration(
            context,
            paths,
            lane,
            frame,
            offset,
            payload_bytes,
            cursor,
            active_key,
            path_flights,
            subflow_set,
            proven_subflows,
            graduated_subflows,
            ack_clock_calibration,
        )
    {
        return choice;
    }
    if normal_bulk_send && lead.is_none() {
        if lower_flight_owner.is_none()
            && let Some(active_key) = active_key
            && let Some((position, _)) = paths.iter().enumerate().find(|(_, path)| {
                path.key() == active_key
                    && path.placement != RelayPathPlacement::Repair
                    && frame
                        .map(|frame| path.stream.can_enqueue_frame_now(frame, lane))
                        .unwrap_or(true)
            })
        {
            // First owner bytes establish the lower-frontier owner.  The
            // no-fallback rule applies after that frontier exists; before then,
            // blocking the active primary just creates a sender-service stall.
            return BulkRelayPathChoice::Selected(position);
        }
        return BulkRelayPathChoice::Blocked;
    }
    let lead_key = lead.map(|lead| lead.key);
    let lead_baseline = lead.map(|lead| (lead.snapshot, lead.eta_ms));
    let mut best: Option<(usize, f64, usize, PathSnapshot)> = None;
    let mut old_lead_candidate: Option<(usize, f64, PathSnapshot)> = None;
    #[cfg(feature = "lab-diagnostics")]
    let mut best_diagnostics: Option<BulkRelayCandidateDiagnostics> = None;
    for (position, path) in paths.iter().enumerate() {
        let key = path.key();
        if normal_bulk_send
            && subflow_set.and_then(FlowSubflowSet::startup_owner_key) == Some(path.instance())
        {
            // A startup owner remains governed by its cumulative epoch until
            // all attributed ranges drain and the caller commits graduation.
            // Bulk-rate evidence from an early ACK must not bypass the startup
            // credit through the ordinary measured-path branch.
            continue;
        }
        if normal_bulk_send
            && path.placement == RelayPathPlacement::Validation
            && graduated_subflows.is_some_and(|graduated| !graduated.contains(&path.instance()))
        {
            continue;
        }
        if normal_bulk_send && path.placement == RelayPathPlacement::Repair {
            #[cfg(feature = "lab-diagnostics")]
            log_bulk_relay_candidate_decision(
                BulkRelayCandidateDiagnostics::skipped(
                    stream_id,
                    lane,
                    key,
                    lead_key,
                    payload_bytes,
                ),
                false,
                "repair_path_not_for_ordinary_bulk",
            );
            continue;
        }
        if normal_bulk_send
            && let Some(frame) = frame
            && !path.stream.can_enqueue_frame_now(frame, lane)
        {
            #[cfg(feature = "lab-diagnostics")]
            log_bulk_relay_candidate_decision(
                BulkRelayCandidateDiagnostics::skipped(
                    stream_id,
                    lane,
                    key,
                    lead_key,
                    payload_bytes,
                ),
                false,
                "carrier_credit",
            );
            continue;
        }
        if avoid_keys.contains(&path.key())
            && paths.iter().any(|path| !avoid_keys.contains(&path.key()))
        {
            #[cfg(feature = "lab-diagnostics")]
            log_bulk_relay_candidate_decision(
                BulkRelayCandidateDiagnostics::skipped(
                    stream_id,
                    lane,
                    key,
                    lead_key,
                    payload_bytes,
                ),
                false,
                "avoid_previous_path",
            );
            continue;
        }
        if normal_bulk_send {
            let owns_lower_frontier = lower_flight_owner == Some(key);
            if restrict_to_admitted {
                if !owns_lower_frontier && !admitted_bulk_keys.contains(&key) {
                    #[cfg(feature = "lab-diagnostics")]
                    log_bulk_relay_candidate_decision(
                        BulkRelayCandidateDiagnostics::skipped(
                            stream_id,
                            lane,
                            key,
                            lead_key,
                            payload_bytes,
                        ),
                        false,
                        "not_in_admitted_subflow_set",
                    );
                    continue;
                }
            } else if !owns_lower_frontier && Some(key) != active_key {
                #[cfg(feature = "lab-diagnostics")]
                log_bulk_relay_candidate_decision(
                    BulkRelayCandidateDiagnostics::skipped(
                        stream_id,
                        lane,
                        key,
                        lead_key,
                        payload_bytes,
                    ),
                    false,
                    "no_safe_subflow_set_non_active_path",
                );
                continue;
            }
            if let Some(active) = active_key {
                // The request/upload side follows the same production rule as
                // the response scheduler: same-stream reliable OwnerData stays
                // inside the active carrier family. Different-family paths may
                // still be used for probes or repair, but they must not create
                // new ordered-byte ownership that can stall behind an unrelated
                // TCP/QUIC recovery clock. A path that already owns the lower
                // frontier remains eligible only to drain existing debt.
                if key.underlay != active.underlay && !owns_lower_frontier {
                    #[cfg(feature = "lab-diagnostics")]
                    log_bulk_relay_candidate_decision(
                        BulkRelayCandidateDiagnostics::skipped(
                            stream_id,
                            lane,
                            key,
                            lead_key,
                            payload_bytes,
                        ),
                        false,
                        "cross_family_owner_disabled",
                    );
                    continue;
                }
            }
        }
        if normal_bulk_send
            && Some(key) != active_key
            && lower_flight_owner != Some(key)
            && !(lower_flight_owner.is_none()
                && restrict_to_admitted
                && admitted_bulk_keys.contains(&key))
            && !relay_path_runtime_role(
                key,
                active_key,
                lower_flight_owner,
                context.relay_path_has_bulk_model_evidence(key.underlay, key.index),
            )
            .may_own_unique_data()
        {
            #[cfg(feature = "lab-diagnostics")]
            log_bulk_relay_candidate_decision(
                BulkRelayCandidateDiagnostics::skipped(
                    stream_id,
                    lane,
                    key,
                    lead_key,
                    payload_bytes,
                ),
                false,
                "no_sender_evidence",
            );
            continue;
        }
        let Some(snapshot) = relay_path_snapshot_for_bulk_choice(context, key, active_key) else {
            #[cfg(feature = "lab-diagnostics")]
            log_bulk_relay_candidate_decision(
                BulkRelayCandidateDiagnostics::skipped(
                    stream_id,
                    lane,
                    key,
                    lead_key,
                    payload_bytes,
                ),
                false,
                "no_path_snapshot",
            );
            continue;
        };
        let scoring_payload_bytes = if lane.is_bulk() {
            bulk_service_horizon_payload_bytes(payload_bytes, context.mux_limits)
        } else {
            payload_bytes
        };
        let Some(score) = scheduler::score_path(snapshot, lane, scoring_payload_bytes, policy)
        else {
            #[cfg(feature = "lab-diagnostics")]
            log_bulk_relay_candidate_decision(
                BulkRelayCandidateDiagnostics {
                    stream_id,
                    lane,
                    key,
                    lead_key,
                    role: None,
                    eta_ms: None,
                    best_eta_ms: None,
                    completion_horizon_ms: None,
                    stream_ordering_debt_bytes: 0,
                    payload_bytes,
                    snapshot: Some(snapshot),
                },
                false,
                "no_path_score",
            );
            continue;
        };
        #[cfg(feature = "lab-diagnostics")]
        let mut candidate_diagnostics = None;
        if normal_bulk_send {
            let cross_path_ordering_debt = path_flights
                .map(|flights| flights.ordering_debt_bytes_before_offset(key, offset))
                .unwrap_or(0);
            let owns_lower_frontier = lower_flight_owner == Some(key);
            let role = if owns_lower_frontier
                || (Some(key) == lead_key && cross_path_ordering_debt == 0)
            {
                BulkAdmissionRole::ActiveDataPath
            } else if let Some(owner) = lower_flight_owner {
                bulk_additional_admission_role(owner.underlay, key.underlay)
            } else if let Some(lead_key) = lead_key {
                bulk_additional_admission_role(lead_key.underlay, key.underlay)
            } else {
                BulkAdmissionRole::ActiveDataPath
            };
            let admission_ordering_debt = cross_path_ordering_debt;
            let (best_snapshot, best_eta_ms) =
                if owns_lower_frontier && role == BulkAdmissionRole::ActiveDataPath {
                    (snapshot, score.eta_ms)
                } else {
                    lead_baseline.unwrap_or((snapshot, score.eta_ms))
                };
            #[cfg(feature = "lab-diagnostics")]
            {
                let completion_horizon_ms = bulk_completion_horizon_ms_with_ordering_debt(
                    best_snapshot,
                    best_eta_ms,
                    snapshot,
                    payload_bytes,
                    context.mux_limits,
                    admission_ordering_debt,
                );
                candidate_diagnostics = Some(BulkRelayCandidateDiagnostics {
                    stream_id,
                    lane,
                    key,
                    lead_key,
                    role: Some(role),
                    eta_ms: Some(score.eta_ms),
                    best_eta_ms: Some(best_eta_ms),
                    completion_horizon_ms: Some(completion_horizon_ms),
                    stream_ordering_debt_bytes: admission_ordering_debt,
                    payload_bytes,
                    snapshot: Some(snapshot),
                });
            }
            let ordering_suppression =
                bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                    best_snapshot,
                    best_eta_ms,
                    candidate_snapshot: snapshot,
                    candidate_eta_ms: score.eta_ms,
                    payload_bytes,
                    mux_limits: context.mux_limits,
                    role,
                    stream_ordering_debt_bytes: admission_ordering_debt,
                });
            if let Some(reason) = ordering_suppression {
                #[cfg(feature = "lab-diagnostics")]
                if let Some(diagnostics) = candidate_diagnostics {
                    log_bulk_relay_candidate_decision(diagnostics, false, reason);
                }
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = reason;
                continue;
            }
        }
        if normal_bulk_send && lower_flight_owner.is_none() && Some(key) == ordered_data_owner {
            old_lead_candidate = Some((position, score.eta_ms, snapshot));
        }
        let cursor_distance = path_cursor_distance(position, cursor, paths.len());
        match best {
            None => {
                best = Some((position, score.eta_ms, cursor_distance, snapshot));
                #[cfg(feature = "lab-diagnostics")]
                {
                    best_diagnostics =
                        candidate_diagnostics.or(Some(BulkRelayCandidateDiagnostics {
                            stream_id,
                            lane,
                            key,
                            lead_key,
                            role: None,
                            eta_ms: Some(score.eta_ms),
                            best_eta_ms: Some(score.eta_ms),
                            completion_horizon_ms: None,
                            stream_ordering_debt_bytes: 0,
                            payload_bytes,
                            snapshot: Some(snapshot),
                        }));
                }
            }
            Some((_, best_eta, best_distance, _)) => {
                if score.eta_ms < best_eta
                    || (score.eta_ms == best_eta && cursor_distance < best_distance)
                {
                    best = Some((position, score.eta_ms, cursor_distance, snapshot));
                    #[cfg(feature = "lab-diagnostics")]
                    {
                        best_diagnostics =
                            candidate_diagnostics.or(Some(BulkRelayCandidateDiagnostics {
                                stream_id,
                                lane,
                                key,
                                lead_key,
                                role: None,
                                eta_ms: Some(score.eta_ms),
                                best_eta_ms: Some(score.eta_ms),
                                completion_horizon_ms: None,
                                stream_ordering_debt_bytes: 0,
                                payload_bytes,
                                snapshot: Some(snapshot),
                            }));
                    }
                }
            }
        }
    }
    if let Some((best_position, best_eta_ms, _, best_snapshot)) = best {
        let position = old_lead_candidate
            .filter(|(_, old_eta_ms, old_snapshot)| {
                relay_path_within_adaptive_lead_hysteresis(
                    *old_eta_ms,
                    *old_snapshot,
                    best_eta_ms,
                    best_snapshot,
                    payload_bytes,
                )
            })
            .map(|(position, _, _)| position)
            .unwrap_or(best_position);
        #[cfg(feature = "lab-diagnostics")]
        if let Some(diagnostics) = best_diagnostics {
            log_bulk_relay_candidate_decision(diagnostics, true, "selected");
        }
        return BulkRelayPathChoice::Selected(position);
    }
    if !normal_bulk_send {
        return BulkRelayPathChoice::NotApplicable;
    }
    BulkRelayPathChoice::Blocked
}

fn relay_path_within_adaptive_lead_hysteresis(
    old_eta_ms: f64,
    old_snapshot: PathSnapshot,
    best_eta_ms: f64,
    best_snapshot: PathSnapshot,
    payload_bytes: usize,
) -> bool {
    path_within_adaptive_lead_hysteresis(
        old_eta_ms,
        old_snapshot,
        best_eta_ms,
        best_snapshot,
        payload_bytes,
    )
}

struct RelayBulkLeadRequest<'a> {
    context: &'a ClientPathContext,
    paths: &'a [ReliableRelayRemotePath],
    lane: FlowLane,
    payload_bytes: usize,
    frame: Option<&'a Frame>,
    active_key: Option<RelayPathKey>,
    admitted_bulk_keys: &'a [RelayPathKey],
    restrict_to_admitted: bool,
    lower_flight_owner: Option<RelayPathKey>,
    lower_owner_cross_path_debt: u64,
    policy: SchedulerPolicy,
}

fn choose_admissible_relay_bulk_lead(request: RelayBulkLeadRequest<'_>) -> Option<RelayBulkLead> {
    let RelayBulkLeadRequest {
        context,
        paths,
        lane,
        payload_bytes,
        frame,
        active_key,
        admitted_bulk_keys,
        restrict_to_admitted,
        lower_flight_owner,
        lower_owner_cross_path_debt,
        policy,
    } = request;
    paths
        .iter()
        .filter(|path| path.placement != RelayPathPlacement::Repair)
        .filter(|path| {
            frame
                .map(|frame| path.stream.can_enqueue_frame_now(frame, lane))
                .unwrap_or(true)
        })
        .filter(|path| {
            let key = path.key();
            if let Some(owner) = lower_flight_owner {
                return key == owner;
            }
            if restrict_to_admitted {
                admitted_bulk_keys.contains(&key)
            } else {
                Some(key) == active_key
            }
        })
        .filter(|path| {
            let key = path.key();
            Some(key) == active_key
                || lower_flight_owner == Some(key)
                || (lower_flight_owner.is_none()
                    && restrict_to_admitted
                    && admitted_bulk_keys.contains(&key))
                || context.relay_path_has_bulk_model_evidence(key.underlay, key.index)
        })
        .filter_map(|path| {
            let key = path.key();
            let (snapshot, eta_ms) = scored_relay_path_snapshot_for_bulk_choice(
                context,
                key,
                active_key,
                lane,
                payload_bytes,
                policy,
            )?;
            let stream_ordering_debt_bytes = if lower_flight_owner == Some(key) {
                lower_owner_cross_path_debt
            } else {
                0
            };
            let suppression =
                bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                    best_snapshot: snapshot,
                    best_eta_ms: eta_ms,
                    candidate_snapshot: snapshot,
                    candidate_eta_ms: eta_ms,
                    payload_bytes,
                    mux_limits: context.mux_limits,
                    role: BulkAdmissionRole::ActiveDataPath,
                    stream_ordering_debt_bytes,
                });
            suppression.is_none().then_some(RelayBulkLead {
                key,
                snapshot,
                eta_ms,
            })
        })
        .min_by(|left, right| {
            left.eta_ms
                .total_cmp(&right.eta_ms)
                .then_with(|| context.relay_path_key_order(left.key, right.key))
        })
}

fn scored_relay_path_snapshot_for_bulk_choice(
    context: &ClientPathContext,
    key: RelayPathKey,
    active_key: Option<RelayPathKey>,
    lane: FlowLane,
    payload_bytes: usize,
    policy: SchedulerPolicy,
) -> Option<(PathSnapshot, f64)> {
    let snapshot = relay_path_snapshot_for_bulk_choice(context, key, active_key)?;
    let scoring_payload_bytes = if lane.is_bulk() {
        bulk_service_horizon_payload_bytes(payload_bytes, context.mux_limits)
    } else {
        payload_bytes
    };
    let score = scheduler::score_path(snapshot, lane, scoring_payload_bytes, policy)?;
    Some((snapshot, score.eta_ms))
}

fn relay_path_snapshot_for_bulk_choice(
    context: &ClientPathContext,
    key: RelayPathKey,
    active_key: Option<RelayPathKey>,
) -> Option<PathSnapshot> {
    let mut snapshot = context.reliable_path_snapshot(key)?;
    if Some(key) != active_key {
        snapshot.active_flows = snapshot.active_flows.saturating_add(1);
    }
    Some(snapshot)
}

fn path_cursor_distance(position: usize, cursor: usize, len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    position.wrapping_add(len).wrapping_sub(cursor % len) % len
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SharedSecret;

    fn security() -> SecurityConfig {
        SecurityConfig::encrypted(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
        )
    }

    fn context(paths: &[&str]) -> ClientPathContext {
        ClientPathContext::new(
            paths
                .iter()
                .map(|path| path.parse::<PathSpec>().expect("path spec"))
                .collect(),
            security(),
            ResourceLimits::default(),
        )
        .expect("context")
    }

    fn data_frame(offset: u64, len: usize) -> Frame {
        Frame::StreamData {
            stream_id: StreamId(7),
            offset,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0x5a; len]),
        }
    }

    fn relay_path(
        underlay: UnderlayProtocol,
        index: usize,
        placement: RelayPathPlacement,
    ) -> ReliableRelayRemotePath {
        let (commands, _receivers) = reliable_path_command_channels(8);
        ReliableRelayRemotePath {
            path_index: index,
            instance_id: index as u64 + 1,
            placement,
            load_reserved: placement == RelayPathPlacement::Active,
            attached_at: Instant::now(),
            path_proof_id: (placement == RelayPathPlacement::Validation)
                .then_some(index as u64 + 1),
            path_proof_generation: 0,
            stream: ReliablePathStreamHandle {
                stream_id: StreamId(7),
                max_offset: u64::MAX,
                lane: FlowLane::Throughput,
                underlay,
                max_frame_payload_bytes: 64 * 1024,
                output: ReliablePathStreamOutput::fixed(
                    underlay,
                    PathId(index as u16),
                    commands,
                    MuxLimits::default(),
                ),
            },
        }
    }

    fn mark_bulk_service(context: &ClientPathContext, key: RelayPathKey) {
        context.mark_relay_path_rate_sample(
            key.underlay,
            key.index,
            PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(80))
                .expect("bulk service sample"),
        );
    }

    fn mark_path_proof(context: &ClientPathContext, key: RelayPathKey, elapsed: Duration) {
        context.mark_relay_path_proof_observation(
            key.underlay,
            key.index,
            PathProofObservation {
                proof_id: key.index as u64 + 1,
                bytes: PATH_OPEN_SCORE_BYTES as u64,
                elapsed,
                sent_at: Instant::now(),
            },
        );
    }

    #[test]
    fn request_startup_subflow_requires_proof_from_current_attachment() {
        let context = context(&[
            "tcp://127.0.0.1:10080?srtt-ms=20&rate-mbps=500",
            "tcp://127.0.0.1:10081?srtt-ms=10&rate-mbps=500",
        ]);
        let service_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let candidate_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        };
        mark_bulk_service(&context, service_key);
        mark_path_proof(&context, candidate_key, Duration::from_millis(10));
        let paths = vec![
            relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
            relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation),
        ];
        let ledger = RelayPathFlightLedger::default();

        assert!(
            choose_request_startup_subflow(
                &context,
                &paths,
                FlowLane::Throughput,
                None,
                0,
                64 * 1024,
                Some(service_key),
                Some(&ledger),
                None,
                None,
                None,
                None,
            )
            .is_none(),
            "a proof observed before this output attached must not authorize unique data"
        );

        context.mark_relay_path_proof_observation(
            candidate_key.underlay,
            candidate_key.index,
            PathProofObservation {
                proof_id: 999,
                bytes: PATH_OPEN_SCORE_BYTES as u64,
                elapsed: Duration::from_millis(8),
                sent_at: Instant::now(),
            },
        );
        assert!(
            choose_request_startup_subflow(
                &context,
                &paths,
                FlowLane::Throughput,
                None,
                0,
                64 * 1024,
                Some(service_key),
                Some(&ledger),
                None,
                None,
                None,
                None,
            )
            .is_none(),
            "another stream's newer proof ID must not authorize this attachment"
        );

        mark_path_proof(&context, candidate_key, Duration::from_millis(8));
        assert!(matches!(
            choose_request_startup_subflow(
                &context,
                &paths,
                FlowLane::Throughput,
                None,
                0,
                64 * 1024,
                Some(service_key),
                Some(&ledger),
                None,
                None,
                None,
                None,
            ),
            Some(BulkRelayPathChoice::SelectedStartupSubflow {
                position: 1,
                service,
                candidate,
            }) if service == paths[0].instance() && candidate == paths[1].instance()
        ));
        assert!(
            !context
                .relay_path_has_bulk_model_evidence(candidate_key.underlay, candidate_key.index,),
            "PATH_PROOF is current-instance reachability evidence, not capacity evidence"
        );
    }

    #[test]
    fn graduated_candidate_gets_bounded_ack_clock_calibration_before_eta_ranking() {
        let context = context(&[
            "tcp://127.0.0.1:10082?srtt-ms=20&rate-mbps=500",
            "tcp://127.0.0.1:10083?srtt-ms=10&rate-mbps=500",
        ]);
        let service_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let candidate_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        };
        let paths = vec![
            relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
            relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation),
        ];
        let service = paths[0].instance();
        let candidate = paths[1].instance();
        mark_bulk_service(&context, service_key);
        context.mark_relay_path_rate_sample(
            candidate_key.underlay,
            candidate_key.index,
            PathRateSample::new(256 * 1024, Duration::from_secs(1))
                .expect("low receipt-rate evidence"),
        );
        mark_path_proof(&context, candidate_key, Duration::from_millis(10));
        let proven = HashSet::from([service, candidate]);
        let graduated = HashSet::from([candidate]);
        let ack_clock_proven = HashSet::new();
        let mut spent = HashMap::new();
        let flights = RelayPathFlightLedger::default();
        let request = |spent: &HashMap<RelayPathInstance, u64>,
                       ack_clock_proven: &HashSet<RelayPathInstance>| {
            choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
                stream_id: StreamId(7),
                context: &context,
                paths: &paths,
                lane: FlowLane::Throughput,
                frame: None,
                offset: 0,
                payload_bytes: BBR_MAX_SEND_QUANTUM_BYTES,
                cursor: 1,
                avoid_keys: &[],
                path_flights: Some(&flights),
                ordered_data_owner: Some(service_key),
                subflow_set: None,
                proven_subflows: Some(&proven),
                graduated_subflows: Some(&graduated),
                attempted_subflows: Some(&graduated),
                ack_clock_calibration: Some(RequestAckClockCalibration {
                    proven_subflows: ack_clock_proven,
                    spent_bytes: spent,
                }),
            })
        };

        assert_eq!(
            request(&spent, &ack_clock_proven),
            BulkRelayPathChoice::SelectedAckClockCalibration {
                position: 1,
                candidate,
            },
            "the low receipt-rate candidate needs a bounded ACK-clock window before ordinary ETA can be trusted"
        );

        spent.insert(
            candidate,
            reliable_ack_clock_calibration_limit_bytes(context.mux_limits),
        );
        assert_eq!(
            request(&spent, &ack_clock_proven),
            BulkRelayPathChoice::Selected(0),
            "calibration credit is cumulative and does not refill"
        );
        let ack_clock_proven = HashSet::from([candidate]);
        assert_eq!(
            request(&HashMap::new(), &ack_clock_proven),
            BulkRelayPathChoice::Selected(0),
            "a usable ACK-clock sample permanently returns the instance to ordinary ETA ranking"
        );
    }

    #[test]
    fn exhausted_calibration_waits_for_flights_then_advances_to_next_candidate() {
        let context = context(&[
            "tcp://127.0.0.1:10084?srtt-ms=20&rate-mbps=500",
            "tcp://127.0.0.1:10085?srtt-ms=10&rate-mbps=500",
            "tcp://127.0.0.1:10086?srtt-ms=10&rate-mbps=500",
        ]);
        let paths = vec![
            relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
            relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation),
            relay_path(UnderlayProtocol::Tcp, 2, RelayPathPlacement::Validation),
        ];
        let service_key = paths[0].key();
        let first = paths[1].instance();
        let second = paths[2].instance();
        mark_bulk_service(&context, service_key);
        for path in paths.iter().skip(1) {
            context.mark_relay_path_rate_sample(
                path.key().underlay,
                path.key().index,
                PathRateSample::new(256 * 1024, Duration::from_secs(1))
                    .expect("receipt-rate evidence"),
            );
            mark_path_proof(&context, path.key(), Duration::from_millis(10));
        }
        let proven = HashSet::from([paths[0].instance(), first, second]);
        let graduated = HashSet::from([first, second]);
        let ack_clock_proven = HashSet::new();
        let spent = HashMap::from([(
            first,
            reliable_ack_clock_calibration_limit_bytes(context.mux_limits),
        )]);
        let choose = |flights: &RelayPathFlightLedger| {
            choose_request_ack_clock_calibration(
                &context,
                &paths,
                FlowLane::Throughput,
                None,
                BBR_MAX_SEND_QUANTUM_BYTES as u64,
                BBR_MAX_SEND_QUANTUM_BYTES,
                2,
                Some(service_key),
                Some(flights),
                None,
                Some(&proven),
                Some(&graduated),
                Some(RequestAckClockCalibration {
                    proven_subflows: &ack_clock_proven,
                    spent_bytes: &spent,
                }),
            )
        };

        let mut outstanding = RelayPathFlightLedger::default();
        outstanding.record_owner_frame_instance(first, &data_frame(0, BBR_MAX_SEND_QUANTUM_BYTES));
        assert_eq!(
            choose(&outstanding),
            None,
            "another candidate must wait while the exhausted exact calibration still owns bytes"
        );
        assert_eq!(
            choose(&RelayPathFlightLedger::default()),
            Some(BulkRelayPathChoice::SelectedAckClockCalibration {
                position: 2,
                candidate: second,
            }),
            "drained exhausted credit is a tombstone, not a permanent serialization lock"
        );
    }

    #[test]
    fn ineligible_spent_instance_does_not_block_live_validation_calibration() {
        let context = context(&[
            "tcp://127.0.0.1:10087?srtt-ms=20&rate-mbps=500",
            "tcp://127.0.0.1:10088?srtt-ms=10&rate-mbps=500",
            "tcp://127.0.0.1:10089?srtt-ms=10&rate-mbps=500",
        ]);
        let paths = vec![
            relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
            relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Repair),
            relay_path(UnderlayProtocol::Tcp, 2, RelayPathPlacement::Validation),
        ];
        let service_key = paths[0].key();
        let ineligible = paths[1].instance();
        let candidate = paths[2].instance();
        mark_bulk_service(&context, service_key);
        context.mark_relay_path_rate_sample(
            candidate.key.underlay,
            candidate.key.index,
            PathRateSample::new(256 * 1024, Duration::from_secs(1)).expect("receipt-rate evidence"),
        );
        mark_path_proof(&context, candidate.key, Duration::from_millis(10));
        let proven = HashSet::from([paths[0].instance(), ineligible, candidate]);
        let graduated = HashSet::from([ineligible, candidate]);
        let spent = HashMap::from([(ineligible, BBR_MAX_SEND_QUANTUM_BYTES as u64)]);

        assert_eq!(
            choose_request_ack_clock_calibration(
                &context,
                &paths,
                FlowLane::Throughput,
                None,
                0,
                BBR_MAX_SEND_QUANTUM_BYTES,
                2,
                Some(service_key),
                Some(&RelayPathFlightLedger::default()),
                None,
                Some(&proven),
                Some(&graduated),
                Some(RequestAckClockCalibration {
                    proven_subflows: &HashSet::new(),
                    spent_bytes: &spent,
                }),
            ),
            Some(BulkRelayPathChoice::SelectedAckClockCalibration {
                position: 2,
                candidate,
            }),
            "spent credit on a Repair placement must not serialize live Validation work"
        );
    }

    #[test]
    fn request_startup_subflow_rejects_cross_family_repair_and_latency_pressure() {
        let context = context(&[
            "tcp://127.0.0.1:10090?srtt-ms=20&rate-mbps=500",
            "udp://127.0.0.1:10091?srtt-ms=10&rate-mbps=500",
        ]);
        let service_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let candidate_key = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        };
        mark_bulk_service(&context, service_key);
        let cross_family = vec![
            relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
            relay_path(UnderlayProtocol::Udp, 0, RelayPathPlacement::Validation),
        ];
        mark_path_proof(&context, candidate_key, Duration::from_millis(8));
        let ledger = RelayPathFlightLedger::default();
        assert!(
            choose_request_startup_subflow(
                &context,
                &cross_family,
                FlowLane::Throughput,
                None,
                0,
                64 * 1024,
                Some(service_key),
                Some(&ledger),
                None,
                None,
                None,
                None,
            )
            .is_none(),
            "independent carrier recovery models cannot share startup credit"
        );

        let repair = vec![
            relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
            relay_path(UnderlayProtocol::Udp, 0, RelayPathPlacement::Repair),
        ];
        assert!(
            choose_request_startup_subflow(
                &context,
                &repair,
                FlowLane::Throughput,
                None,
                0,
                64 * 1024,
                Some(service_key),
                Some(&ledger),
                None,
                None,
                None,
                None,
            )
            .is_none(),
            "Repair placement is never a capacity-sampling owner"
        );

        context.reserve_tcp_path_load(0, FlowLane::Latency);
        let same_family = vec![
            relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
            relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Validation),
        ];
        assert!(
            choose_request_startup_subflow(
                &context,
                &same_family,
                FlowLane::Throughput,
                None,
                0,
                64 * 1024,
                Some(service_key),
                Some(&ledger),
                None,
                None,
                None,
                None,
            )
            .is_none(),
            "any reliable latency pressure suppresses optional startup sampling"
        );
    }

    #[test]
    fn request_startup_waits_for_service_anchor_and_authoritative_debt() {
        let context = context(&[
            "tcp://127.0.0.1:10092?srtt-ms=20&rate-mbps=500",
            "tcp://127.0.0.1:10093?srtt-ms=10&rate-mbps=500",
        ]);
        let service_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let candidate_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        };
        mark_bulk_service(&context, service_key);
        let paths = vec![
            relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
            relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation),
        ];
        mark_path_proof(&context, candidate_key, Duration::from_millis(8));
        let empty = RelayPathFlightLedger::default();
        let exact_state = HashSet::new();

        assert_eq!(
            choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
                stream_id: StreamId(7),
                context: &context,
                paths: &paths,
                lane: FlowLane::Throughput,
                frame: None,
                offset: 0,
                payload_bytes: 64 * 1024,
                cursor: 1,
                avoid_keys: &[],
                path_flights: Some(&empty),
                ordered_data_owner: None,
                subflow_set: None,
                proven_subflows: None,
                graduated_subflows: Some(&exact_state),
                attempted_subflows: Some(&exact_state),
                ack_clock_calibration: None,
            }),
            BulkRelayPathChoice::Selected(0),
            "offset zero must establish Service before any Validation path can own data"
        );

        let mut foreign = RelayPathFlightLedger::default();
        foreign.record_owner_frame(candidate_key, &data_frame(0, 64 * 1024));
        assert!(
            choose_request_startup_subflow(
                &context,
                &paths,
                FlowLane::Throughput,
                None,
                64 * 1024,
                64 * 1024,
                Some(service_key),
                Some(&foreign),
                None,
                None,
                Some(&exact_state),
                Some(&exact_state),
            )
            .is_none(),
            "a foreign lower OwnerData range is authoritative and cannot be crossed"
        );

        let mut repaired = RelayPathFlightLedger::default();
        repaired.record_owner_frame(service_key, &data_frame(0, 64 * 1024));
        repaired.record_repair_frame(candidate_key, &data_frame(0, 64 * 1024));
        assert!(
            choose_request_startup_subflow(
                &context,
                &paths,
                FlowLane::Throughput,
                None,
                64 * 1024,
                64 * 1024,
                Some(service_key),
                Some(&repaired),
                None,
                None,
                Some(&exact_state),
                Some(&exact_state),
            )
            .is_none(),
            "Repair ambiguity must drain before optional unique-data sampling"
        );
    }

    #[test]
    fn request_startup_owner_cannot_bypass_credit_before_flights_drain() {
        let context = context(&[
            "tcp://127.0.0.1:10100?srtt-ms=30&rate-mbps=500",
            "tcp://127.0.0.1:10101?srtt-ms=5&rate-mbps=500",
        ]);
        let service_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let candidate_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        };
        mark_bulk_service(&context, service_key);
        mark_bulk_service(&context, candidate_key);
        let paths = vec![
            relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
            relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation),
        ];
        mark_path_proof(&context, candidate_key, Duration::from_millis(5));
        let startup_credit = usize::try_from(reliable_subflow_startup_sample_limit_bytes(
            context.mux_limits,
        ))
        .expect("startup credit");
        let mut epoch =
            FlowSubflowSet::new(0, paths[0].instance(), startup_credit, 0, Duration::ZERO);
        assert_eq!(
            epoch
                .admit_subflow_owner(SubflowAdmissionInput {
                    key: paths[1].instance(),
                    bulk_rate_proven: false,
                    startup_owner_allowed: true,
                    frontier_clear: true,
                    completion_improves: false,
                    observed_goodput_non_degrading: true,
                    read_gap: Duration::ZERO,
                    owner_bytes: startup_credit,
                    optional_overhead_bytes: 0,
                })
                .decision,
            PathAdmissionDecision::AdmitSubflow
        );
        let mut ledger = RelayPathFlightLedger::default();
        ledger.record_owner_frame(candidate_key, &data_frame(0, 64 * 1024));
        let mut graduated = HashSet::new();
        let attempted = HashSet::from([paths[1].instance()]);

        let pre_graduation = choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
            stream_id: StreamId(7),
            context: &context,
            paths: &paths,
            lane: FlowLane::Throughput,
            frame: None,
            offset: 64 * 1024,
            payload_bytes: 64 * 1024,
            cursor: 1,
            avoid_keys: &[],
            path_flights: Some(&ledger),
            ordered_data_owner: Some(service_key),
            subflow_set: Some(&epoch),
            proven_subflows: None,
            graduated_subflows: Some(&graduated),
            attempted_subflows: Some(&attempted),
            ack_clock_calibration: None,
        });
        assert!(
            matches!(
                pre_graduation,
                BulkRelayPathChoice::Selected(0) | BulkRelayPathChoice::Blocked
            ),
            "early rate evidence must not let the startup owner escape its cumulative epoch while attributed ranges remain"
        );

        ledger.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: 64 * 1024,
        }]);
        assert!(epoch.graduate_startup_owner(paths[1].instance()));
        graduated.insert(paths[1].instance());
        assert_eq!(
            choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
                stream_id: StreamId(7),
                context: &context,
                paths: &paths,
                lane: FlowLane::Throughput,
                frame: None,
                offset: 64 * 1024,
                payload_bytes: 64 * 1024,
                cursor: 1,
                avoid_keys: &[],
                path_flights: Some(&ledger),
                ordered_data_owner: Some(service_key),
                subflow_set: Some(&epoch),
                proven_subflows: None,
                graduated_subflows: Some(&graduated),
                attempted_subflows: Some(&attempted),
                ack_clock_calibration: None,
            }),
            BulkRelayPathChoice::Selected(1),
            "drained and explicitly graduated evidence may enter ordinary measured admission"
        );
    }

    #[test]
    fn ordering_debt_counts_lower_bytes_owned_by_other_paths() {
        let path0 = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        };
        let path1 = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        };
        let path2 = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let mut ledger = RelayPathFlightLedger::default();
        ledger.record_owner_frame(path0, &data_frame(0, 4096));
        ledger.record_owner_frame(path1, &data_frame(4096, 4096));

        assert_eq!(ledger.ordering_debt_bytes_before_offset(path0, 8192), 4096);
        assert_eq!(ledger.ordering_debt_bytes_before_offset(path1, 8192), 4096);
        assert_eq!(ledger.ordering_debt_bytes_before_offset(path2, 8192), 8192);
        assert_eq!(
            ledger.oldest_lower_flight_owner_before_offset(8192),
            Some(path0)
        );
    }

    #[test]
    fn missing_later_owner_is_detected_even_when_oldest_owner_is_live() {
        let live_owner = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let missing_owner = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        };
        let mut ledger = RelayPathFlightLedger::default();
        ledger.record_owner_frame(live_owner, &data_frame(0, 4096));
        ledger.record_owner_frame(missing_owner, &data_frame(4096, 4096));
        let live_instance = RelayPathInstance {
            key: live_owner,
            id: 0,
        };
        let missing_instance = RelayPathInstance {
            key: missing_owner,
            id: 0,
        };

        assert!(ledger.has_missing_ordering_owner_before_offset(8192, &[live_instance]));
        assert!(
            !ledger.has_missing_ordering_owner_before_offset(
                8192,
                &[live_instance, missing_instance],
            )
        );
    }

    #[test]
    fn same_key_replacement_does_not_mask_stale_instance_owner_flight() {
        let key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let stale = RelayPathInstance { key, id: 7 };
        let replacement = RelayPathInstance { key, id: 8 };
        let frame = data_frame(0, 4096);
        let mut ledger = RelayPathFlightLedger::default();
        ledger.record_owner_frame_instance(stale, &frame);

        assert!(ledger.has_missing_ordering_owner_before_offset(4097, &[replacement]));
        assert!(
            ledger
                .ordering_owner_keys_for_frame(&frame, &[replacement])
                .is_empty()
        );
        assert_eq!(
            ledger.ordering_owner_underlay_for_frame(&frame),
            Some(UnderlayProtocol::Tcp),
            "repair policy must retain the stale OwnerData transport family after same-key replacement"
        );
        assert_eq!(
            ledger.latest_unacked_ranges_for_path_instance(stale),
            vec![OffsetRange {
                start: 0,
                end: 4096,
            }]
        );
        assert!(
            ledger
                .latest_unacked_ranges_for_path_instance(replacement)
                .is_empty()
        );
    }

    #[test]
    fn repair_copy_does_not_become_ordering_owner() {
        let owner = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        };
        let duplicate = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        };
        let frame = data_frame(0, 4096);
        let mut ledger = RelayPathFlightLedger::default();
        ledger.record_owner_frame(owner, &frame);
        ledger.record_repair_frame(duplicate, &frame);

        assert_eq!(
            ledger.oldest_lower_flight_owner_before_offset(4096),
            Some(owner)
        );
        assert_eq!(ledger.ordering_debt_bytes_before_offset(owner, 4096), 0);
        assert_eq!(
            ledger.ordering_debt_bytes_before_offset(duplicate, 4096),
            4096
        );

        let released = ledger.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: 4096,
        }]);
        assert_eq!(released.len(), 2);
        assert!(released.iter().any(|release| release.key == owner));
        assert!(released.iter().any(|release| release.key == duplicate));
        assert!(
            released.iter().all(|release| !release.path_proving),
            "ACK of duplicated request bytes releases inflight state but is not path-scoped proof"
        );
    }

    #[test]
    fn owner_only_ack_release_is_path_proving() {
        let owner = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let frame = data_frame(0, 4096);
        let mut ledger = RelayPathFlightLedger::default();
        ledger.record_owner_frame(owner, &frame);

        let released = ledger.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: 4096,
        }]);

        assert_eq!(released.len(), 1);
        assert_eq!(released[0].key, owner);
        assert!(
            released[0].path_proving,
            "a single outstanding owner copy is path-scoped STREAM_ACK evidence"
        );
    }

    #[test]
    fn partial_same_start_duplicate_ack_retains_owner_suffix() {
        let owner = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let repair = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        };
        let mut ledger = RelayPathFlightLedger::default();
        ledger.record_owner_frame(owner, &data_frame(0, 4096));
        ledger.record_repair_frame(repair, &data_frame(0, 1024));

        let prefix_releases = ledger.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: 1024,
        }]);
        assert_eq!(prefix_releases.len(), 2);
        assert!(prefix_releases.iter().all(|release| release.bytes == 1024));
        assert!(
            prefix_releases.iter().all(|release| !release.path_proving),
            "an ACK shared by OwnerData and RepairData cannot identify a delivery path"
        );
        assert_eq!(
            ledger.latest_unacked_ranges_for_path(owner),
            vec![OffsetRange {
                start: 1024,
                end: 4096,
            }],
            "releasing the shorter same-start RepairData copy must retain the OwnerData suffix"
        );
        assert!(ledger.latest_unacked_ranges_for_path(repair).is_empty());
        assert_eq!(
            ledger.ordering_owner_keys_for_frame(
                &data_frame(1024, 3072),
                &[
                    RelayPathInstance { key: owner, id: 0 },
                    RelayPathInstance { key: repair, id: 0 },
                ],
            ),
            vec![owner],
            "the trimmed suffix retains OwnerData identity without retaining the RepairData key"
        );

        let suffix_releases = ledger.release_normalized_acked_ranges(&[OffsetRange {
            start: 1024,
            end: 4096,
        }]);
        assert_eq!(suffix_releases.len(), 1);
        assert_eq!(suffix_releases[0].key, owner);
        assert_eq!(suffix_releases[0].bytes, 3072);
        assert!(
            suffix_releases[0].path_proving,
            "the retained owner-only suffix is unambiguous when it is acknowledged later"
        );
        assert!(ledger.latest_unacked_ranges_for_path(owner).is_empty());
    }

    #[test]
    fn bulk_ready_blocks_when_no_attached_path_can_advance_ordered_frontier() {
        let context = context(&[
            "tcp://127.0.0.1:10100?srtt-ms=50&rate-mbps=1",
            "udp://127.0.0.1:10101?srtt-ms=50&rate-mbps=1",
        ]);
        let paths = vec![
            relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Validation),
            relay_path(UnderlayProtocol::Udp, 0, RelayPathPlacement::Active),
        ];
        let missing_owner = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        };
        let mut ledger = RelayPathFlightLedger::default();
        ledger.record_owner_frame(missing_owner, &data_frame(0, 64 * 1024));

        assert_eq!(
            choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
                stream_id: StreamId(7),
                context: &context,
                paths: &paths,
                lane: FlowLane::Throughput,
                frame: None,
                offset: 64 * 1024,
                payload_bytes: 64 * 1024,
                cursor: 0,
                avoid_keys: &[],
                path_flights: Some(&ledger),
                ordered_data_owner: None,
                subflow_set: None,
                proven_subflows: None,
                graduated_subflows: None,
                attempted_subflows: None,
                ack_clock_calibration: None,
            }),
            BulkRelayPathChoice::Blocked
        );
    }

    #[test]
    fn relay_lower_frontier_owner_can_lead_from_validation_attachment() {
        let context = context(&[
            "tcp://127.0.0.1:10110?srtt-ms=50&rate-mbps=1",
            "udp://127.0.0.1:10111?srtt-ms=50&rate-mbps=1",
        ]);
        let lower_owner = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let paths = vec![
            relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Validation),
            relay_path(UnderlayProtocol::Udp, 0, RelayPathPlacement::Active),
        ];
        let mut ledger = RelayPathFlightLedger::default();
        ledger.record_owner_frame(lower_owner, &data_frame(0, 64 * 1024));

        assert_eq!(
            choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
                stream_id: StreamId(7),
                context: &context,
                paths: &paths,
                lane: FlowLane::Throughput,
                frame: None,
                offset: 64 * 1024,
                payload_bytes: 64 * 1024,
                cursor: 0,
                avoid_keys: &[],
                path_flights: Some(&ledger),
                ordered_data_owner: None,
                subflow_set: None,
                proven_subflows: None,
                graduated_subflows: None,
                attempted_subflows: None,
                ack_clock_calibration: None,
            }),
            BulkRelayPathChoice::Selected(0)
        );
    }

    #[test]
    fn relay_bulk_lead_must_be_admissible_not_lowest_raw_eta() {
        let context = context(&[
            "udp://127.0.0.1:10120?srtt-ms=20&rate-mbps=500",
            "udp://127.0.0.1:10121?srtt-ms=30&rate-mbps=500",
        ]);
        let saturated = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        };
        let admissible = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        };
        context.mark_relay_path_rate_sample(
            admissible.underlay,
            admissible.index,
            PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(80))
                .expect("sender evidence"),
        );
        context.record_relay_path_send(saturated.underlay, saturated.index, 128 * 1024 * 1024);
        let paths = vec![
            relay_path(UnderlayProtocol::Udp, 0, RelayPathPlacement::Active),
            relay_path(UnderlayProtocol::Udp, 1, RelayPathPlacement::Validation),
        ];

        let lead = choose_admissible_relay_bulk_lead(RelayBulkLeadRequest {
            context: &context,
            paths: &paths,
            lane: FlowLane::Throughput,
            payload_bytes: 64 * 1024,
            frame: None,
            active_key: Some(saturated),
            admitted_bulk_keys: &[saturated, admissible],
            restrict_to_admitted: true,
            lower_flight_owner: None,
            lower_owner_cross_path_debt: 0,
            policy: SchedulerPolicy::default(),
        })
        .expect("admissible path should become lead");

        assert_eq!(lead.key, admissible);
    }

    #[test]
    fn relay_lower_owner_uses_sliding_window_not_ordering_debt() {
        let context = context(&[
            "udp://127.0.0.1:10130?srtt-ms=20&rate-mbps=500",
            "udp://127.0.0.1:10131?srtt-ms=30&rate-mbps=500",
        ]);
        let owner = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        };
        let alternate = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        };
        context.mark_relay_path_rate_sample(
            owner.underlay,
            owner.index,
            PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(80))
                .expect("owner evidence"),
        );
        context.mark_relay_path_rate_sample(
            alternate.underlay,
            alternate.index,
            PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(80))
                .expect("sender evidence"),
        );
        context.record_relay_path_send(owner.underlay, owner.index, 1024 * 1024);
        let paths = vec![
            relay_path(UnderlayProtocol::Udp, 0, RelayPathPlacement::Active),
            relay_path(UnderlayProtocol::Udp, 1, RelayPathPlacement::Validation),
        ];

        let lead = choose_admissible_relay_bulk_lead(RelayBulkLeadRequest {
            context: &context,
            paths: &paths,
            lane: FlowLane::Throughput,
            payload_bytes: 64 * 1024,
            frame: None,
            active_key: Some(owner),
            admitted_bulk_keys: &[owner, alternate],
            restrict_to_admitted: true,
            lower_flight_owner: Some(owner),
            lower_owner_cross_path_debt: 1024 * 1024,
            policy: SchedulerPolicy::default(),
        })
        .expect("same-carrier lower flight is sliding-window flight");

        assert_eq!(lead.key, owner);
    }

    #[test]
    fn relay_ordinary_bulk_uses_lower_eta_when_frontier_is_clear() {
        let context = context(&[
            "udp://127.0.0.1:10140?srtt-ms=50&rate-mbps=500",
            "udp://127.0.0.1:10141?srtt-ms=5&rate-mbps=500",
        ]);
        let lead_key = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        };
        let paths = vec![
            relay_path(UnderlayProtocol::Udp, 0, RelayPathPlacement::Active),
            relay_path(UnderlayProtocol::Udp, 1, RelayPathPlacement::Validation),
        ];

        assert_eq!(
            choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
                stream_id: StreamId(7),
                context: &context,
                paths: &paths,
                lane: FlowLane::Throughput,
                frame: None,
                offset: 64 * 1024,
                payload_bytes: 64 * 1024,
                cursor: 1,
                avoid_keys: &[],
                path_flights: Some(&RelayPathFlightLedger::default()),
                ordered_data_owner: Some(lead_key),
                subflow_set: None,
                proven_subflows: None,
                graduated_subflows: None,
                attempted_subflows: None,
                ack_clock_calibration: None,
            }),
            BulkRelayPathChoice::Selected(1)
        );
    }

    #[test]
    fn relay_ordinary_bulk_keeps_lead_only_inside_measured_hysteresis() {
        let mut lead = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 6.0, 500_000_000.0);
        let mut alternate = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 5.0, 500_000_000.0);
        lead.jitter_ms = 2.0;
        alternate.jitter_ms = 1.0;

        assert!(relay_path_within_adaptive_lead_hysteresis(
            6.0,
            lead,
            5.0,
            alternate,
            64 * 1024
        ));

        lead.jitter_ms = 0.0;
        alternate.jitter_ms = 0.0;

        assert!(
            !relay_path_within_adaptive_lead_hysteresis(6.0, lead, 5.0, alternate, 64 * 1024),
            "old relay lead must not survive outside measured jitter/queue hysteresis"
        );
    }
}
