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

/// One exact actor-owned Product flight clipped by a recovery observation.
///
/// The identity is an attachment incarnation, never merely a configured path
/// key.  A reconnect therefore cannot inherit either ownership or duplicate
/// avoidance from the generation it replaced.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReliableFlightSpan<I> {
    pub(crate) range: OffsetRange,
    pub(crate) identity: I,
    pub(crate) kind: CarrierWorkKind,
    pub(crate) sent_at: Instant,
}

/// Maximal lowest prefix whose exact OriginalData owners and all accepted-copy
/// owners are identical at every byte.
#[derive(Debug, Clone)]
pub(crate) struct ReliableLiveOwnerFrontier<I> {
    pub(crate) range: OffsetRange,
    pub(crate) owners: Vec<I>,
    pub(crate) avoid: Vec<I>,
    /// Latest immutable OriginalData assignment for each exact owner across
    /// the whole uniform prefix.  Callers combine it with that owner's R and
    /// take the maximum deadline; a cache boundary never resets this clock.
    pub(crate) owner_assignments: Vec<(I, Instant)>,
}

fn identity_set_eq<I: Eq>(left: &[I], right: &[I]) -> bool {
    left.len() == right.len() && left.iter().all(|identity| right.contains(identity))
}

/// Sweeps every flight boundary from the exact lowest missing byte and stops
/// at the first ownership/avoidance-set change or coverage hole.
///
/// Flight storage and retransmission-cache chunking are deliberately absent
/// from this model.  Thin direction-specific wrappers supply only flights
/// still owned by their actor; the cache is verified independently before a
/// resulting prefix is scored or applied.
pub(crate) fn reliable_live_owner_uniform_frontier<I: Copy + Eq>(
    range: OffsetRange,
    spans: impl IntoIterator<Item = ReliableFlightSpan<I>>,
) -> Option<ReliableLiveOwnerFrontier<I>> {
    if range.is_empty() {
        return None;
    }
    let spans = spans
        .into_iter()
        .filter_map(|span| {
            let clipped = OffsetRange {
                start: span.range.start.max(range.start),
                end: span.range.end.min(range.end),
            };
            (!clipped.is_empty()).then_some(ReliableFlightSpan {
                range: clipped,
                ..span
            })
        })
        .collect::<Vec<_>>();
    if spans.is_empty() {
        return None;
    }

    let mut boundaries = Vec::with_capacity(spans.len().saturating_mul(2).saturating_add(2));
    boundaries.push(range.start);
    boundaries.push(range.end);
    for span in &spans {
        boundaries.push(span.range.start);
        boundaries.push(span.range.end);
    }
    boundaries.sort_unstable();
    boundaries.dedup();

    let mut frontier_owners = None::<Vec<I>>;
    let mut frontier_avoid = None::<Vec<I>>;
    let mut owner_assignments = Vec::<(I, Instant)>::new();
    let mut frontier_end = range.start;
    for boundary in boundaries.windows(2) {
        let start = boundary[0];
        let end = boundary[1];
        if start < range.start || start >= range.end || start != frontier_end || start >= end {
            continue;
        }
        let mut owners = Vec::<I>::new();
        let mut avoid = Vec::<I>::new();
        let mut segment_assignments = Vec::<(I, Instant)>::new();
        for span in spans
            .iter()
            .filter(|span| span.range.start <= start && span.range.end >= end)
        {
            if !avoid.contains(&span.identity) {
                avoid.push(span.identity);
            }
            if span.kind.is_original_transmission() {
                if !owners.contains(&span.identity) {
                    owners.push(span.identity);
                }
                if let Some((_, latest)) = segment_assignments
                    .iter_mut()
                    .find(|(identity, _)| *identity == span.identity)
                {
                    *latest = (*latest).max(span.sent_at);
                } else {
                    segment_assignments.push((span.identity, span.sent_at));
                }
            }
        }
        if owners.is_empty() || avoid.is_empty() {
            break;
        }
        if let (Some(expected_owners), Some(expected_avoid)) = (&frontier_owners, &frontier_avoid)
            && (!identity_set_eq(expected_owners, &owners)
                || !identity_set_eq(expected_avoid, &avoid))
        {
            break;
        }
        frontier_owners.get_or_insert_with(|| owners.clone());
        frontier_avoid.get_or_insert_with(|| avoid.clone());
        for (identity, sent_at) in segment_assignments {
            if let Some((_, latest)) = owner_assignments
                .iter_mut()
                .find(|(owner, _)| *owner == identity)
            {
                *latest = (*latest).max(sent_at);
            } else {
                owner_assignments.push((identity, sent_at));
            }
        }
        frontier_end = end;
    }

    (frontier_end > range.start).then(|| ReliableLiveOwnerFrontier {
        range: OffsetRange {
            start: range.start,
            end: frontier_end,
        },
        owners: frontier_owners.expect("non-empty uniform frontier has owners"),
        avoid: frontier_avoid.expect("non-empty uniform frontier has avoidance identities"),
        owner_assignments,
    })
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

/// Applies the selected target's repair and service limits without enlarging
/// the common frontier quantum that was used to rank that target.
pub(crate) fn reliable_live_frontier_reinjection_limit_bytes(
    target_reinjection_quantum: usize,
    selection_reinjection_quantum: usize,
    exact_frontier_extent_bytes: usize,
    reinjection_debt_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    if target_reinjection_quantum == 0
        || selection_reinjection_quantum == 0
        || exact_frontier_extent_bytes == 0
        || reinjection_debt_bytes == 0
        || mux_limits.max_repair_bytes == 0
        || mux_limits.max_path_flight_bytes == 0
    {
        return 0;
    }
    reliable_critical_tail_reinjection_limit_bytes(
        target_reinjection_quantum.min(selection_reinjection_quantum),
        reinjection_debt_bytes,
        mux_limits,
    )
    .min(exact_frontier_extent_bytes)
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

/// Combines one live owner's optional gap-service credit with the bounded
/// liveness floor that becomes available only after that owner's recovery
/// interval. `live_owner_attempt_ready` is durable per-direction epoch state,
/// not a fact reconstructed from queue or target state. The floor never
/// enlarges the selected target's Product capacity; it only prevents stronger
/// evidence of a blocking frontier from removing the one-quantum progress
/// authority already available to a silent live tail.
pub(crate) fn reliable_live_gap_reinjection_authority(
    target_service_limit: usize,
    optional_credit: usize,
    frontier_limit: usize,
    owner_recovery_ready: bool,
    live_owner_attempt_ready: bool,
) -> (usize, bool) {
    let critical_floor = if owner_recovery_ready && live_owner_attempt_ready {
        frontier_limit.min(target_service_limit)
    } else {
        0
    };
    let service_limit = target_service_limit.min(optional_credit.max(critical_floor));
    (service_limit, service_limit > optional_credit)
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

#[cfg(test)]
mod live_owner_reinjection_tests {
    use super::{
        CarrierWorkKind, ReliableFlightSpan, reliable_live_frontier_reinjection_limit_bytes,
        reliable_live_gap_reinjection_authority, reliable_live_owner_uniform_frontier,
    };
    use crate::mux::MuxLimits;
    use crate::protocol::OffsetRange;
    use std::time::{Duration, Instant};

    #[test]
    fn authority_uses_max_of_optional_credit_and_one_frontier_floor() {
        assert_eq!(
            reliable_live_gap_reinjection_authority(100, 0, 40, true, true),
            (40, true),
        );
        assert_eq!(
            reliable_live_gap_reinjection_authority(100, 20, 40, true, true),
            (40, true),
            "partial optional credit is replaced by, not added to, the floor",
        );
        assert_eq!(
            reliable_live_gap_reinjection_authority(100, 60, 40, true, true),
            (60, false),
        );
        assert_eq!(
            reliable_live_gap_reinjection_authority(80, 100, 40, true, true),
            (80, false),
        );
        assert_eq!(
            reliable_live_gap_reinjection_authority(100, 0, 40, false, true),
            (0, false),
            "optional exhaustion before the owner fallback has no bypass",
        );
        assert_eq!(
            reliable_live_gap_reinjection_authority(100, 100, 40, true, false),
            (100, false),
            "a consumed temporal opportunity blocks only the over-credit frontier floor",
        );
        assert_eq!(
            reliable_live_gap_reinjection_authority(0, 100, 40, true, true),
            (0, false),
            "the floor cannot bypass exact target service capacity",
        );
    }

    #[test]
    fn exact_frontier_smaller_than_credit_is_optional_and_can_fill_credit() {
        let frontier =
            reliable_live_frontier_reinjection_limit_bytes(100, 100, 20, 100, MuxLimits::default());
        assert_eq!(frontier, 20);
        assert_eq!(
            reliable_live_gap_reinjection_authority(100, 40, frontier, true, true),
            (40, false),
            "H < C < A must preserve optional credit beyond the exact frontier instead of misclassifying H as an over-budget attempt",
        );
    }

    #[test]
    fn target_apply_can_only_shrink_the_ranked_frontier() {
        let limits = MuxLimits::default();
        assert_eq!(
            reliable_live_frontier_reinjection_limit_bytes(80, 40, 100, 100, limits),
            40,
        );
        assert_eq!(
            reliable_live_frontier_reinjection_limit_bytes(20, 40, 100, 100, limits),
            20,
        );
    }

    #[test]
    fn live_frontier_zero_authority_fails_closed() {
        let limits = MuxLimits::default();
        for (target, selection, extent, debt) in [
            (0, 40, 100, 100),
            (40, 0, 100, 100),
            (40, 40, 0, 100),
            (40, 40, 100, 0),
        ] {
            assert_eq!(
                reliable_live_frontier_reinjection_limit_bytes(
                    target, selection, extent, debt, limits,
                ),
                0,
            );
        }
        assert_eq!(
            reliable_live_frontier_reinjection_limit_bytes(
                40,
                40,
                100,
                100,
                MuxLimits {
                    max_repair_bytes: 0,
                    ..limits
                },
            ),
            0,
        );
        assert_eq!(
            reliable_live_frontier_reinjection_limit_bytes(
                40,
                40,
                100,
                100,
                MuxLimits {
                    max_path_flight_bytes: 0,
                    ..limits
                },
            ),
            0,
        );
    }

    #[test]
    fn uniform_frontier_crosses_storage_boundaries_and_aggregates_assignment_time() {
        let now = Instant::now();
        let early = now - Duration::from_secs(2);
        let late = now - Duration::from_secs(1);
        let frontier = reliable_live_owner_uniform_frontier(
            OffsetRange {
                start: 0,
                end: 64 * 1024,
            },
            [
                ReliableFlightSpan {
                    range: OffsetRange {
                        start: 0,
                        end: 1024,
                    },
                    identity: 1_u8,
                    kind: CarrierWorkKind::OriginalData,
                    sent_at: early,
                },
                ReliableFlightSpan {
                    range: OffsetRange {
                        start: 1024,
                        end: 64 * 1024,
                    },
                    identity: 1_u8,
                    kind: CarrierWorkKind::OriginalData,
                    sent_at: late,
                },
                ReliableFlightSpan {
                    range: OffsetRange {
                        start: 0,
                        end: 64 * 1024,
                    },
                    identity: 2_u8,
                    kind: CarrierWorkKind::ReinjectedData,
                    sent_at: early,
                },
            ],
        )
        .expect("same per-byte O/A sets form one frontier");

        assert_eq!(frontier.range.end, 64 * 1024);
        assert_eq!(frontier.owners, vec![1]);
        assert_eq!(frontier.avoid.len(), 2);
        assert!(frontier.avoid.contains(&1));
        assert!(frontier.avoid.contains(&2));
        assert_eq!(frontier.owner_assignments, vec![(1, late)]);
    }

    #[test]
    fn uniform_frontier_stops_at_first_owner_or_avoidance_change() {
        let sent_at = Instant::now();
        let frontier = reliable_live_owner_uniform_frontier(
            OffsetRange {
                start: 0,
                end: 64 * 1024,
            },
            [
                ReliableFlightSpan {
                    range: OffsetRange {
                        start: 0,
                        end: 1024,
                    },
                    identity: 1_u8,
                    kind: CarrierWorkKind::OriginalData,
                    sent_at,
                },
                ReliableFlightSpan {
                    range: OffsetRange {
                        start: 1024,
                        end: 64 * 1024,
                    },
                    identity: 2_u8,
                    kind: CarrierWorkKind::OriginalData,
                    sent_at,
                },
            ],
        )
        .expect("lowest owned prefix");

        assert_eq!(
            frontier.range,
            OffsetRange {
                start: 0,
                end: 1024
            }
        );
        assert_eq!(frontier.owners, vec![1]);
        assert_eq!(frontier.avoid, vec![1]);
    }
}
