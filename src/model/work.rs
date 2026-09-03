//! Carrier-neutral product work classifications.
//!
//! `TrafficClass` describes latency versus throughput demand. These types instead
//! describe what product work may do to ordered ownership and sender queues.

use crate::model::capacity::{
    adaptive_reliable_relay_reinjection_bytes, reliable_bulk_carrier_feed_quantum_bytes,
    reliable_product_recovery_window_bytes,
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

/// Committed work that consumes one selected target's Product recovery
/// authority.
///
/// `path.data_level_bytes_in_flight` contains exact OriginalData only.
/// `accepted_reinjection_bytes` contains every exact un-DataACKed repair copy
/// accepted by this target incarnation: a retry deadline or native backlog
/// release does not remove it. Queued repair contains target-bound work plus
/// current-stream target-unbound work, but never raw Data/control or repair
/// already bound to another exact target.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReliableReinjectionTargetWork {
    path: Option<PathSnapshot>,
    queued_reinjection_bytes: usize,
    accepted_reinjection_bytes: usize,
}

impl ReliableReinjectionTargetWork {
    pub(crate) fn new(
        path: Option<PathSnapshot>,
        queued_reinjection_bytes: usize,
        accepted_reinjection_bytes: usize,
    ) -> Self {
        Self {
            path,
            queued_reinjection_bytes,
            accepted_reinjection_bytes,
        }
    }
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

/// Sizes one Product reinjection service window from the selected target's
/// measured opportunity without replacing native transport recovery.
///
/// Exact carrier failure and a persistent authoritative MPP Data ACK gap have
/// different eligibility rules, but once either has selected a target they
/// share the same byte authority: unacknowledged ranges may fill only the
/// target's available Product service window. The target's TCP or QUIC sender
/// remains the final pacing, congestion, and enqueue authority.
pub(crate) fn reliable_reinjection_service_limit_bytes(
    target: ReliableReinjectionTargetWork,
    reinjection_debt_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    // `Some` identifies an exact selected target, so zero published Product
    // authority is a complete negative observation. A portable `None` target
    // may still use the bounded fallback below; an exact target must not turn
    // missing/expired P into a renewable emergency reserve.
    if target
        .path
        .is_some_and(|snapshot| snapshot.data_level_limit_bytes == 0)
    {
        return 0;
    }
    // Keep one Product work quantum available when ordinary target headroom is
    // full, but treat it as one outstanding reserve. Reevaluation cannot mint
    // another reserve while queued or accepted ReinjectedData still owns it.
    let emergency_reserve = adaptive_reliable_relay_reinjection_bytes(
        target.path,
        TrafficClass::Throughput,
        mux_limits,
    )
    .max(reliable_bulk_carrier_feed_quantum_bytes(mux_limits));
    let target_window =
        reliable_product_recovery_window_bytes(target.path, TrafficClass::Throughput, mux_limits);
    // Product recovery authority is exact to one stream direction and target
    // incarnation. Raw Product staging and sampled native queue/flight are
    // neither assigned to this target nor native admission authority. The
    // bounded writer reservation below this model owns native admission.
    let original_data = target.path.map_or(0, |snapshot| {
        usize::try_from(snapshot.data_level_bytes_in_flight).unwrap_or(usize::MAX)
    });
    let repair_cap = target_window
        .saturating_sub(original_data)
        .max(emergency_reserve);
    let outstanding_reinjection = target
        .accepted_reinjection_bytes
        .saturating_add(target.queued_reinjection_bytes);
    let service_limit = repair_cap.saturating_sub(outstanding_reinjection);
    if service_limit == 0 {
        return 0;
    }
    reliable_critical_tail_reinjection_limit_bytes(
        service_limit,
        reinjection_debt_bytes,
        mux_limits,
    )
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
