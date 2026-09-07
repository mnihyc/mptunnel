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
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
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

#[derive(Default)]
struct FrontierCoverage {
    originals: usize,
    copies: usize,
    owner_index: Option<usize>,
    initially_avoided: bool,
}

impl FrontierCoverage {
    fn membership_changes(&self) -> usize {
        usize::from((self.originals != 0) != self.owner_index.is_some())
            + usize::from((self.copies != 0) != self.initially_avoided)
    }
}

#[cfg(test)]
std::thread_local! {
    static FRONTIER_SPAN_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn observe_frontier_span_visits_for_test<T>(observe: impl FnOnce() -> T) -> (T, usize) {
    let before = FRONTIER_SPAN_VISITS.with(|visits| visits.get());
    let result = observe();
    let visits = FRONTIER_SPAN_VISITS.with(|visits| visits.get() - before);
    (result, visits)
}

/// Sweeps every flight boundary from the exact lowest missing byte and stops
/// at the first ownership/avoidance-set change or coverage hole.
///
/// Flight storage and retransmission-cache chunking are deliberately absent
/// from this model.  Thin direction-specific wrappers supply only flights
/// still owned by their actor; the cache is verified independently before a
/// resulting prefix is scored or applied. Sorted endpoints and incremental
/// membership counts avoid rescanning N spans at each of O(N) boundaries.
pub(crate) fn reliable_live_owner_uniform_frontier<I: Copy + Eq + Hash>(
    range: OffsetRange,
    spans: impl IntoIterator<Item = ReliableFlightSpan<I>>,
) -> Option<ReliableLiveOwnerFrontier<I>> {
    if range.is_empty() {
        return None;
    }
    let spans = spans
        .into_iter()
        .filter_map(|span| {
            #[cfg(test)]
            FRONTIER_SPAN_VISITS.with(|visits| visits.set(visits.get() + 1));
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

    // Preserve input order within each boundary, including first-segment
    // owner/avoid vector order. Hash-map iteration never determines output.
    let mut events = Vec::with_capacity(spans.len().saturating_mul(2));
    for (index, span) in spans.iter().enumerate() {
        #[cfg(test)]
        FRONTIER_SPAN_VISITS.with(|visits| visits.set(visits.get() + 1));
        events.push((span.range.start, index, true));
        events.push((span.range.end, index, false));
    }
    events.sort_unstable();
    if events[0].0 != range.start {
        return None;
    }

    let mut coverage = HashMap::<I, FrontierCoverage>::new();
    let mut owners = Vec::new();
    let mut avoid = Vec::new();
    let mut owner_assignments = Vec::<(I, Instant)>::new();
    let mut frontier_end = range.start;
    let mut membership_changes = 0;
    let mut cursor = 0;
    while cursor < events.len() && events[cursor].0 < range.end {
        let boundary = events[cursor].0;
        let first = boundary == range.start;
        let group_start = cursor;
        // Apply simultaneous ends/starts before testing membership. Adjacent
        // chunks of the same exact owner must not manufacture a coverage hole.
        while cursor < events.len() && events[cursor].0 == boundary {
            #[cfg(test)]
            FRONTIER_SPAN_VISITS.with(|visits| visits.set(visits.get() + 1));
            let (_, index, starts) = events[cursor];
            let span = &spans[index];
            let state = coverage.entry(span.identity).or_default();
            if !first {
                membership_changes -= state.membership_changes();
            }
            let original = span.kind.is_original_transmission();
            if starts {
                if first && state.copies == 0 {
                    state.initially_avoided = true;
                    avoid.push(span.identity);
                }
                if first && original && state.originals == 0 {
                    state.owner_index = Some(owners.len());
                    owners.push(span.identity);
                    owner_assignments.push((span.identity, span.sent_at));
                }
                state.copies += 1;
                state.originals += usize::from(original);
            } else {
                state.copies -= 1;
                state.originals -= usize::from(original);
            }
            if !first {
                membership_changes += state.membership_changes();
            }
            cursor += 1;
        }
        if owners.is_empty() || membership_changes != 0 {
            break;
        }
        // A start contributes its immutable assignment exactly once, and only
        // if its segment belongs to the accepted prefix. Ends cannot erase
        // assignment history; starts at a rejected boundary cannot extend it.
        for &(_, index, starts) in &events[group_start..cursor] {
            #[cfg(test)]
            FRONTIER_SPAN_VISITS.with(|visits| visits.set(visits.get() + 1));
            let span = &spans[index];
            if starts && span.kind.is_original_transmission() {
                let owner = coverage[&span.identity]
                    .owner_index
                    .expect("unchanged owner set retains initial owner index");
                owner_assignments[owner].1 = owner_assignments[owner].1.max(span.sent_at);
            }
        }
        frontier_end = events[cursor].0;
    }

    (frontier_end > range.start).then_some(ReliableLiveOwnerFrontier {
        range: OffsetRange {
            start: range.start,
            end: frontier_end,
        },
        owners,
        avoid,
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
/// This computes target capacity, not cause-specific publication authority.
/// Exact carrier failure may consume the bounded service window; a persistent
/// authoritative MPP Data ACK gap whose original owner remains live is capped
/// separately to the exact frontier quantum that selected the target. The
/// target's TCP or QUIC sender remains the final pacing, congestion, and
/// enqueue authority.
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

/// Preserves one live owner's ranked Product frontier through Apply.
///
/// The target limit already subtracts queued, accepted, and stable-slot repair
/// debt.  That larger service opportunity cannot widen the exact quantum whose
/// completion selected the target: a Data-ACK gap proves Product reordering or
/// loss, not failure of the live native-reliable owner.  A configured traffic-
/// accounting percentage is deliberately absent, and native admission remains
/// the final authority below this model.
pub(crate) fn reliable_live_gap_reinjection_authority(
    target_service_limit: usize,
    ranked_frontier_limit: usize,
    recovery_ready: bool,
) -> usize {
    if recovery_ready {
        target_service_limit.min(ranked_frontier_limit)
    } else {
        0
    }
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
    let mut intervals = Vec::<(u64, u64)>::new();
    let mut active = 0_i64;
    let mut previous = None;
    for (position, delta) in events {
        if let Some(previous) = previous
            && previous < position
            && active > 1
        {
            // Multiplicity can change without changing ambiguity. Retain its
            // union, not a boundary for every nested copy: those boundaries
            // would needlessly fragment each downstream release/hole record.
            if let Some((_, end)) = intervals.last_mut()
                && *end == previous
            {
                *end = position;
            } else {
                intervals.push((previous, position));
            }
        }
        active += delta;
        previous = Some(position);
    }
    intervals
}

/// Partition one released interval by the pre-release ambiguity index. The
/// returned flag describes only that atom: a copied prefix cannot erase proof
/// from the adjacent original-only suffix. No flight ownership is changed.
pub(crate) fn flight_evidence_segments(
    start: u64,
    end: u64,
    ambiguous: &[(u64, u64)],
) -> impl Iterator<Item = (u64, u64, bool)> + '_ {
    let mut cursor = start;
    let mut index = ambiguous.partition_point(|(_, interval_end)| *interval_end <= start);
    std::iter::from_fn(move || {
        if cursor >= end {
            return None;
        }
        while let Some(&(ambiguous_start, ambiguous_end)) = ambiguous.get(index) {
            if ambiguous_end <= cursor {
                index += 1;
                continue;
            }
            if ambiguous_start >= end {
                break;
            }
            let is_ambiguous = cursor >= ambiguous_start;
            let segment_end = if is_ambiguous {
                ambiguous_end.min(end)
            } else {
                ambiguous_start
            };
            let segment = (cursor, segment_end, is_ambiguous);
            cursor = segment_end;
            return Some(segment);
        }
        let segment = (cursor, end, false);
        cursor = end;
        Some(segment)
    })
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
mod flight_evidence_tests {
    use super::{ambiguous_flight_intervals, flight_evidence_segments};

    #[test]
    fn evidence_partition_matches_byte_ambiguity_and_half_open_bounds() {
        let nested = ambiguous_flight_intervals((1..=64).map(|end| (0, end)));
        assert_eq!(nested, vec![(0, 63)]);
        for end in 1..=64 {
            assert!(flight_evidence_segments(0, end, &nested).count() <= 2);
        }
        // All ambiguity patterns on eight bytes, including adjacent intervals;
        // all clipped/empty queries. Each atom must cover exactly its bytes.
        for mask in 0..256_u16 {
            let ambiguous = (0..8_u64)
                .filter(|byte| mask & (1 << byte) != 0)
                .map(|byte| (byte, byte + 1))
                .collect::<Vec<_>>();
            for start in 0..=8 {
                for end in start..=8 {
                    let mut cursor = start;
                    for (low, high, is_ambiguous) in
                        flight_evidence_segments(start, end, &ambiguous)
                    {
                        assert_eq!(low, cursor);
                        assert!(low < high && high <= end);
                        for byte in low..high {
                            assert_eq!(is_ambiguous, mask & (1 << byte) != 0);
                        }
                        cursor = high;
                    }
                    assert_eq!(cursor, end);
                }
            }
        }
    }
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
    fn uniform_frontier_span_visits_do_not_multiply_storage_boundaries() {
        const CHUNKS: usize = 2048;
        let sent_at = Instant::now();
        super::FRONTIER_SPAN_VISITS.with(|visits| visits.set(0));
        let frontier = reliable_live_owner_uniform_frontier(
            OffsetRange {
                start: 0,
                end: CHUNKS as u64,
            },
            (0..CHUNKS as u64).map(|start| ReliableFlightSpan {
                range: OffsetRange {
                    start,
                    end: start + 1,
                },
                identity: 1_u8,
                kind: CarrierWorkKind::OriginalData,
                sent_at,
            }),
        )
        .expect("one owner across every adjacent storage chunk");
        assert_eq!(frontier.range.end, CHUNKS as u64);
        let visits = super::FRONTIER_SPAN_VISITS.with(|visits| visits.get());
        println!("{CHUNKS} spans: {visits} span/event visits (sorting excluded)");
        assert!(
            visits <= 6 * CHUNKS,
            "{CHUNKS} spans required {visits} span/event visits; coverage must not be rescanned at every boundary"
        );
    }

    // Independent byte-cell oracle: no endpoint events or incremental counts.
    // Small integer coordinates make exact sets and accepted assignment maxima
    // directly enumerable, including invalid/empty and clipped input spans.
    fn frontier_by_byte(
        range: OffsetRange,
        spans: &[ReliableFlightSpan<u8>],
    ) -> Option<super::ReliableLiveOwnerFrontier<u8>> {
        let mut result = None::<super::ReliableLiveOwnerFrontier<u8>>;
        for byte in range.start..range.end {
            let mut owners = Vec::new();
            let mut avoid = Vec::new();
            for span in spans
                .iter()
                .filter(|s| s.range.start <= byte && byte < s.range.end)
            {
                if !avoid.contains(&span.identity) {
                    avoid.push(span.identity);
                }
                if span.kind.is_original_transmission() && !owners.contains(&span.identity) {
                    owners.push(span.identity);
                }
            }
            if owners.is_empty() {
                break;
            }
            if let Some(first) = &result
                && (owners.len() != first.owners.len()
                    || avoid.len() != first.avoid.len()
                    || owners.iter().any(|i| !first.owners.contains(i))
                    || avoid.iter().any(|i| !first.avoid.contains(i)))
            {
                break;
            }
            let frontier = result.get_or_insert_with(|| super::ReliableLiveOwnerFrontier {
                range: OffsetRange {
                    start: range.start,
                    end: byte,
                },
                owner_assignments: owners
                    .iter()
                    .map(|&identity| {
                        let latest = spans
                            .iter()
                            .filter(|s| {
                                s.identity == identity
                                    && s.kind.is_original_transmission()
                                    && s.range.start <= byte
                                    && byte < s.range.end
                            })
                            .map(|s| s.sent_at)
                            .max()
                            .unwrap();
                        (identity, latest)
                    })
                    .collect(),
                owners,
                avoid,
            });
            for (identity, latest) in &mut frontier.owner_assignments {
                for span in spans.iter().filter(|s| {
                    s.identity == *identity
                        && s.kind.is_original_transmission()
                        && s.range.start <= byte
                        && byte < s.range.end
                }) {
                    *latest = (*latest).max(span.sent_at);
                }
            }
            frontier.range.end = byte + 1;
        }
        result
    }

    #[test]
    fn uniform_frontier_matches_byte_oracle_with_overlap_clipping_and_input_order() {
        let now = Instant::now();
        let mut seed = 0x1a72_64ef_u64;
        let mut random = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for case in 0..4096 {
            let range = OffsetRange {
                start: random() % 8,
                end: 8 + random() % 17,
            };
            let mut spans = (0..12)
                .map(|_| ReliableFlightSpan {
                    range: OffsetRange {
                        start: random() % 25,
                        end: random() % 25,
                    },
                    identity: (random() % 4) as u8,
                    kind: if random() & 1 == 0 {
                        CarrierWorkKind::OriginalData
                    } else {
                        CarrierWorkKind::ReinjectedData
                    },
                    sent_at: now + Duration::from_millis(random() % 100),
                })
                .collect::<Vec<_>>();
            if case % 2 == 0 {
                // Ensure many nonempty prefixes, including future spans whose
                // earlier input position must not reorder the initial vectors.
                spans.push(ReliableFlightSpan {
                    range,
                    identity: 0,
                    kind: CarrierWorkKind::OriginalData,
                    sent_at: now,
                });
            }
            let expected = frontier_by_byte(range, &spans);
            let actual = reliable_live_owner_uniform_frontier(range, spans.iter().copied());
            let full_frontier_range = actual.as_ref().map(|frontier| frontier.range);
            let project = |f: super::ReliableLiveOwnerFrontier<u8>| {
                (f.range, f.owners, f.avoid, f.owner_assignments)
            };
            assert_eq!(
                actual.map(project),
                expected.map(project),
                "case {case}: {spans:?}"
            );
            let extent = range.end - range.start;
            for quantum in [0, 1, extent / 2, extent] {
                let query_end = range.start.saturating_add(quantum).min(range.end);
                let two_query = full_frontier_range.and_then(|full| {
                    let scoring_range = OffsetRange::new(range.start, full.end.min(query_end))?;
                    reliable_live_owner_uniform_frontier(scoring_range, spans.iter().copied())
                });
                let restricted = reliable_live_owner_uniform_frontier(
                    OffsetRange {
                        start: range.start,
                        end: query_end,
                    },
                    spans.iter().copied(),
                );
                assert_eq!(
                    restricted.map(project),
                    two_query.map(project),
                    "prefix restriction case {case}, quantum {quantum}: {spans:?}",
                );
            }
        }
    }

    #[test]
    fn live_gap_authority_preserves_the_ranked_frontier_when_due() {
        assert_eq!(reliable_live_gap_reinjection_authority(100, 40, true), 40);
        assert_eq!(
            reliable_live_gap_reinjection_authority(20, 40, true),
            20,
            "exact target headroom may shrink but never widen the ranked frontier",
        );
        assert_eq!(
            reliable_live_gap_reinjection_authority(100, 40, false),
            0,
            "a recovery cause that is not due remains a hard denial",
        );
        assert_eq!(
            reliable_live_gap_reinjection_authority(0, 40, true),
            0,
            "zero exact target service remains a hard structural denial",
        );
        assert_eq!(
            reliable_live_gap_reinjection_authority(100, 0, true),
            0,
            "zero ranked frontier cannot manufacture live-owner service",
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
