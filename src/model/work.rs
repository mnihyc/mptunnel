//! Carrier-neutral product work classifications.
//!
//! `TrafficClass` describes latency versus throughput demand. These types instead
//! describe what product work may do to ordered ownership and sender queues.

use crate::model::capacity::{
    adaptive_reliable_relay_inflight_bytes, adaptive_reliable_relay_reinjection_bytes,
    reliable_bulk_carrier_feed_quantum_bytes,
};
use crate::mux::MuxLimits;
use crate::protocol::OffsetRange;
use crate::scheduler::{PathSnapshot, TrafficClass};
use std::collections::BTreeMap;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CarrierWorkKind {
    OriginalData,
    ReinjectedData,
}

impl CarrierWorkKind {
    pub(crate) fn is_original_transmission(self) -> bool {
        matches!(self, Self::OriginalData)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReliableWorkClass {
    Control,
    Data,
    Reinjection,
}

/// One atomic observation of exact-range recovery state.
///
/// Recovery actors must consume the due ranges and the next expiry from the
/// same ledger scan. Splitting those observations can lose the wake when a
/// recovery copy expires between two scans.
#[derive(Debug, Default)]
pub(crate) struct RangeRecoveryState {
    pub(crate) uncovered_ranges: Vec<OffsetRange>,
    pub(crate) retry_deadline: Option<Instant>,
}

/// Caps one product reinjection event by current debt and configured resource
/// ceilings; carrier command admission remains the final emission authority.
pub(crate) fn reliable_critical_tail_reinjection_limit_bytes(
    event_reinjection_limit: usize,
    reinjection_debt_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    if reinjection_debt_bytes == 0 {
        return 0;
    }
    let resource_cap = mux_limits
        .max_repair_bytes
        .min(mux_limits.max_path_flight_bytes)
        .max(1);
    reinjection_debt_bytes
        .min(event_reinjection_limit.max(1))
        .min(resource_cap)
}

/// Sizes failover of original product data from the live target's measured
/// service opportunity without replacing native transport recovery.
pub(crate) fn reliable_failed_original_reinjection_limit_bytes(
    path: Option<PathSnapshot>,
    reinjection_debt_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    // Failed-output ranges have already lost their native recovery owner. Keep
    // one product work quantum available even when the replacement path already
    // owns a full modeled flight; its TCP/QUIC sender still gates actual emission.
    let event_limit =
        adaptive_reliable_relay_reinjection_bytes(path, TrafficClass::Throughput, mux_limits)
            .max(reliable_bulk_carrier_feed_quantum_bytes(mux_limits));
    let target_flight =
        adaptive_reliable_relay_inflight_bytes(path, TrafficClass::Throughput, mux_limits);
    let existing_target_debt = path.map_or(0, |snapshot| {
        snapshot
            .data_level_bytes_in_flight
            .max(snapshot.data_level_queue_bytes)
            .max(snapshot.queue_bytes)
            .min(usize::MAX as u64) as usize
    });
    let event_limit = event_limit.max(target_flight.saturating_sub(existing_target_debt));
    reliable_critical_tail_reinjection_limit_bytes(event_limit, reinjection_debt_bytes, mux_limits)
}

pub(crate) fn reliable_critical_tail_reinjection_is_over_budget(
    budget_remaining: usize,
    reinjection_limit: usize,
) -> bool {
    budget_remaining == 0 && reinjection_limit > 0
}

/// ACK release must use identical range math in both product directions so
/// request and response ledgers cannot disagree about path-proving bytes.
pub(crate) fn ambiguous_flight_intervals(
    flights: impl IntoIterator<Item = (u64, u64)>,
) -> Vec<(u64, u64)> {
    let mut events = BTreeMap::<u64, i64>::new();
    for (start, end) in flights {
        *events.entry(start).or_default() += 1;
        *events.entry(end).or_default() -= 1;
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

pub(crate) fn flight_intervals_overlap(intervals: &[(u64, u64)], start: u64, end: u64) -> bool {
    let position = intervals.partition_point(|(_, interval_end)| *interval_end <= start);
    intervals
        .get(position)
        .is_some_and(|(interval_start, _)| *interval_start < end)
}

pub(crate) struct FlightIntervalSplit {
    pub(crate) acked: Vec<(u64, u64)>,
    pub(crate) retained: Vec<(u64, u64)>,
}

pub(crate) fn split_flight_interval_by_ack(
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

pub(crate) fn flight_interval_bytes(start: u64, end: u64) -> usize {
    usize::try_from(end.saturating_sub(start)).unwrap_or(usize::MAX)
}
