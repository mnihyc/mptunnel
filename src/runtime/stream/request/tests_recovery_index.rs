//! Exact lookup and work-bound tests through the production scored-query API.
use super::*;

fn duplicate_ledger(source: &RequestFlightLedger) -> RequestFlightLedger {
    RequestFlightLedger {
        flights: source.flights.clone(),
        geometry_revision: source.geometry_revision,
        original_data_in_flight_bytes: source.original_data_in_flight_bytes,
        original_data_in_flight_bytes_by_instance: source
            .original_data_in_flight_bytes_by_instance
            .clone(),
        reinjected_data_in_flight_bytes_by_instance: source
            .reinjected_data_in_flight_bytes_by_instance
            .clone(),
    }
}

#[test]
fn recovery_index_reads_live_timing_and_updates_exact_assignment_siblings() {
    let a = path(UnderlayProtocol::Tcp, 0, 6201);
    let b = path(UnderlayProtocol::Udp, 1, 6202);
    let copy = path(UnderlayProtocol::Tcp, 2, 6203);
    let mut fast = RequestFlightLedger::default();
    fast.record_original_frame_instance(a, &data_frame(0, 16));
    fast.record_original_frame_instance(b, &data_frame(16, 16));
    fast.record_reinjection_frame_instance(copy, &data_frame(2, 28));
    fast.release_normalized_acked_ranges(&[
        OffsetRange { start: 4, end: 7 },
        OffsetRange { start: 21, end: 24 },
    ]);
    let mut slow = duplicate_ledger(&fast);
    let view = fast.recovery_ownership_view(32);
    // A single immutable geometry view survives repeated timing tightening.
    // A sibling outside the exact queried range must still inherit the minima.
    for rtt in [300.0, 50.0, 500.0, 20.0] {
        for range in [
            OffsetRange { start: 0, end: 4 },
            OffsetRange { start: 7, end: 12 },
            OffsetRange { start: 12, end: 16 },
            OffsetRange { start: 16, end: 21 },
            OffsetRange { start: 24, end: 32 },
            OffsetRange { start: 3, end: 8 },
            OffsetRange { start: 12, end: 19 },
        ] {
            let expected = slow.observe_original_recovery_timing_for_range(range, |owner| {
                Some(recovery_timing_snapshot(owner, rtt))
            });
            let actual = fast.observe_original_recovery_timing_in_view(range, &view, |owner| {
                Some(recovery_timing_snapshot(owner, rtt))
            });
            assert_eq!(actual, expected, "range={range:?} rtt={rtt}");
            assert_eq!(
                format!("{:?}", fast.flights),
                format!("{:?}", slow.flights),
                "all live records including nonqueried siblings and copy deadlines"
            );
        }
    }
}

#[test]
fn recovery_index_frame_facts_keep_whole_frame_and_adjacent_coverage_distinct() {
    let a = path(UnderlayProtocol::Tcp, 0, 6301);
    let b = path(UnderlayProtocol::Udp, 1, 6302);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(a, &data_frame(0, 8));
    ledger.record_original_frame_instance(a, &data_frame(8, 8));
    ledger.record_original_frame_instance(b, &data_frame(4, 8));
    ledger.record_reinjection_frame_instance(b, &data_frame(0, 20));
    for phase in 0..2 {
        if phase == 1 {
            ledger.release_normalized_acked_ranges(&[OffsetRange { start: 5, end: 7 }]);
        }
        let view = ledger.recovery_ownership_view(24);
        for start in 0..24 {
            for end in start + 1..=24 {
                let frame = data_frame(start, (end - start) as usize);
                let expected = (
                    ledger.unique_original_flight_for_frame(&frame),
                    ledger.original_transmission_underlay_for_frame(&frame),
                );
                assert_eq!(
                    ledger.original_frame_facts_with_view(&frame, Some(&view)),
                    expected,
                    "phase={phase} [{start},{end})"
                );
                assert_eq!(
                    ledger.original_frame_facts_with_view(&frame, None),
                    expected
                );
            }
        }
    }
}

#[test]
fn recovery_index_stale_geometry_and_moved_ledger_fall_back_without_false_absence() {
    let owner = path(UnderlayProtocol::Tcp, 0, 6401);
    let other = path(UnderlayProtocol::Udp, 1, 6402);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(owner, &data_frame(0, 16));
    let view = ledger.recovery_ownership_view(16);
    ledger.record_reinjection_frame_instance(other, &data_frame(4, 10));
    for range in [
        OffsetRange { start: 3, end: 10 },
        OffsetRange { start: 6, end: 12 },
    ] {
        let a = ledger
            .live_owner_uniform_frontier(range, &[owner, other])
            .unwrap();
        let b = ledger
            .live_owner_uniform_frontier_in_view(range, &[owner, other], &view)
            .unwrap();
        assert_eq!(
            (a.range, a.owners, a.avoid, a.owner_assignments),
            (b.range, b.owners, b.avoid, b.owner_assignments)
        );
    }
    ledger.release_normalized_acked_ranges(&[OffsetRange { start: 6, end: 9 }]);
    assert!(
        ledger
            .live_owner_uniform_frontier_in_view(
                OffsetRange { start: 6, end: 9 },
                &[owner, other],
                &view
            )
            .is_none()
    );
    let moved = duplicate_ledger(&ledger);
    let frame = data_frame(9, 5);
    assert_eq!(
        moved.original_frame_facts_with_view(&frame, Some(&view)),
        moved.original_frame_facts_with_view(&frame, None)
    );
    ledger.drain_all();
    assert!(
        ledger
            .live_owner_uniform_frontier_in_view(
                OffsetRange { start: 0, end: 16 },
                &[owner, other],
                &view
            )
            .is_none()
    );
}

#[test]
fn recovery_index_preserves_degenerate_saturated_frame_facts() {
    let owner = path(UnderlayProtocol::Tcp, 0, 6501);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(owner, &data_frame(u64::MAX - 8, 8));
    let view = ledger.recovery_ownership_view(u64::MAX);
    // Runtime assignment rejects overflow, but a storage optimization need
    // not change this existing helper's saturated-input behavior either.
    for frame in [
        data_frame(u64::MAX, 1),
        data_frame(u64::MAX - 1, 1),
        data_frame(u64::MAX - 4, 8),
    ] {
        assert_eq!(
            ledger.original_frame_facts_with_view(&frame, Some(&view)),
            ledger.original_frame_facts_with_view(&frame, None)
        );
    }
}

#[test]
fn recovery_index_does_not_rescan_unrelated_prefix_per_boundary() {
    const N: usize = 2048;
    let owner = path(UnderlayProtocol::Udp, 0, 6001);
    let mut ledger = RequestFlightLedger::default();
    for i in 0..N {
        ledger.record_original_frame_instance(owner, &data_frame((i * 4) as u64, 4));
    }
    RequestFlightLedger::take_recovery_lookup_work_for_test();
    let view = ledger.recovery_ownership_view((N * 4) as u64);
    for i in 0..N {
        let range = OffsetRange {
            start: (i * 4) as u64,
            end: ((i + 1) * 4) as u64,
        };
        let actual = ledger
            .live_owner_uniform_frontier_in_view(range, &[owner], &view)
            .unwrap();
        assert_eq!(actual.range, range);
        assert_eq!(actual.owners, vec![owner]);
    }
    let visits = RequestFlightLedger::take_recovery_lookup_work_for_test();
    eprintln!("recovery_index_work buckets={N} queries={N} visits={visits}");
    // For this adjacent geometry: linear index construction, a depth-11 tree
    // lookup plus one exact bucket per query. 32*N leaves room for both branch
    // tests; it is a computational fixture bound, not a runtime service cap.
    assert!(
        visits <= 32 * N,
        "unrelated-prefix work: {visits} > {}",
        32 * N
    );
}

#[test]
fn recovery_index_preserves_exact_order_assignment_clocks_and_crossing_copies() {
    let a = path(UnderlayProtocol::Tcp, 0, 6101);
    let b = path(UnderlayProtocol::Udp, 1, 6102);
    let c = path(UnderlayProtocol::Tcp, 2, 6103);
    let replacement = path(UnderlayProtocol::Tcp, 0, 6104);
    let instances = [a, b, c, replacement];
    let mut ledger = RequestFlightLedger::default();
    // Copies may start much earlier than the queried bucket and cross holes.
    ledger.record_reinjection_frame_instance(c, &data_frame(0, 31));
    ledger.record_original_frame_instance(a, &data_frame(0, 8));
    ledger.record_original_frame_instance(a, &data_frame(8, 8));
    ledger.record_original_frame_instance(b, &data_frame(16, 8));
    ledger.record_original_frame_instance(replacement, &data_frame(26, 6));
    ledger.record_reinjection_frame_instance(a, &data_frame(17, 14));
    ledger.record_reinjection_frame_instance(b, &data_frame(6, 13));
    for phase in 0..3 {
        if phase == 1 {
            ledger.release_normalized_acked_ranges(&[
                OffsetRange { start: 3, end: 5 },
                OffsetRange { start: 20, end: 23 },
            ]);
        }
        if phase == 2 {
            ledger.record_reinjection_frame_instance(replacement, &data_frame(1, 29));
        }
        let view = ledger.recovery_ownership_view(32);
        for mask in 0..16usize {
            let live = instances
                .iter()
                .enumerate()
                .filter_map(|(i, p)| (mask & (1 << i) != 0).then_some(*p))
                .collect::<Vec<_>>();
            for start in 0..=32 {
                for end in start..=32 {
                    let range = OffsetRange { start, end };
                    let expected = ledger.live_owner_uniform_frontier(range, &live);
                    let actual = ledger.live_owner_uniform_frontier_in_view(range, &live, &view);
                    let exact =
                        |v: crate::model::work::ReliableLiveOwnerFrontier<RelayPathInstance>| {
                            (v.range, v.owners, v.avoid, v.owner_assignments)
                        };
                    assert_eq!(
                        actual.map(exact),
                        expected.map(exact),
                        "phase={phase} range={range:?} mask={mask}"
                    );
                }
            }
        }
    }
}
