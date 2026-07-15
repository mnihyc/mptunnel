use super::{
    RequestFlightLedger, RequestOutstandingWindow, RequestStartupState,
    request_tcp_product_limit_for_turnover,
};
use crate::model::capacity::{PATH_OPEN_SCORE_BYTES, reliable_relay_buffer_len};
use crate::model::multipath::FlowSubflowSet;
use crate::model::path::{RelayPathInstance, RelayPathKey};
use crate::model::request::evidence::{RequestOwnerAckProgress, RequestWindowGrowthEvidence};
use crate::mux::MuxLimits;
use crate::protocol::{Frame, OffsetRange, StreamFlags, StreamId, UnderlayProtocol};
use crate::scheduler::FlowLane;
use bytes::Bytes;
use smallvec::smallvec;
use std::time::{Duration, Instant};

fn data_frame(offset: u64, len: usize) -> Frame {
    Frame::StreamData {
        stream_id: StreamId(7),
        offset,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x5a; len]),
    }
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
    let mut ledger = RequestFlightLedger::default();
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
    let mut ledger = RequestFlightLedger::default();
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
        !ledger.has_missing_ordering_owner_before_offset(8192, &[live_instance, missing_instance],)
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
    let mut ledger = RequestFlightLedger::default();
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
    let mut ledger = RequestFlightLedger::default();
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
    let mut ledger = RequestFlightLedger::default();
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
    let mut ledger = RequestFlightLedger::default();
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

fn startup_instance(index: usize, id: u64) -> RelayPathInstance {
    RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index,
        },
        id,
    }
}

#[test]
fn planning_startup_admission_does_not_mutate_state() {
    let state = RequestStartupState::default();
    let service = startup_instance(0, 10);
    let candidate = startup_instance(1, 11);

    let admission = state
        .plan_admission(MuxLimits::default(), service, candidate, 4096)
        .expect("valid same-family startup plan");

    assert!(state.epoch.is_none());
    assert!(!state.attempted_subflows.contains(&candidate));
    drop(admission);
    assert!(state.epoch.is_none());
}

#[test]
fn committing_startup_admission_installs_epoch_and_attempt_atomically() {
    let mut state = RequestStartupState::default();
    let service = startup_instance(0, 20);
    let candidate = startup_instance(1, 21);
    let admission = state
        .plan_admission(MuxLimits::default(), service, candidate, 4096)
        .expect("valid same-family startup plan");

    state.commit_admission(admission);

    assert_eq!(
        state
            .epoch
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key),
        Some(candidate)
    );
    assert!(state.attempted_subflows.contains(&candidate));
}

fn request_test_path_instance(
    underlay: UnderlayProtocol,
    index: usize,
    id: u64,
) -> RelayPathInstance {
    RelayPathInstance {
        key: RelayPathKey { underlay, index },
        id,
    }
}

#[test]
fn request_window_epoch_ignores_active_churn_until_ordered_service_commit() {
    let mux_limits = MuxLimits::default();
    let now = Instant::now();
    let first_active = request_test_path_instance(UnderlayProtocol::Tcp, 0, 1);
    let later_active = request_test_path_instance(UnderlayProtocol::Tcp, 1, 2);
    let mut window = RequestOutstandingWindow::new_at(now);

    let provisional = window.resolved_service_instance(None, Some(first_active));
    assert_eq!(provisional, Some(first_active));
    assert_eq!(
        window.limit_bytes_at(
            provisional,
            FlowLane::Throughput,
            64 * 1024,
            mux_limits,
            now,
        ),
        4 * 1024 * 1024
    );
    assert_eq!(
        window.resolved_service_instance(None, Some(later_active)),
        None,
        "a later Active attachment is not a committed Service and cannot reopen the retained epoch"
    );
    assert_eq!(window.service_epoch_instance, Some(first_active));
    assert_eq!(
        window.resolved_service_instance(Some(first_active), Some(later_active)),
        Some(first_active),
        "committing the original owner preserves the same exact epoch"
    );
    assert_eq!(
        window.resolved_service_instance(Some(later_active), Some(later_active)),
        Some(later_active),
        "only an ordered Service commit authorizes an exact epoch handoff"
    );
    assert_eq!(
        window.limit_bytes_at(
            Some(later_active),
            FlowLane::Throughput,
            64 * 1024,
            mux_limits,
            now + Duration::from_millis(1),
        ),
        4 * 1024 * 1024
    );
    assert_eq!(window.service_epoch_instance, Some(later_active));
}

#[test]
fn tcp_request_outstanding_limit_preserves_classifier_reservoir_and_ack_growth() {
    let mux_limits = MuxLimits::default();
    let now = Instant::now();
    let mut window = RequestOutstandingWindow::new_at(now);
    let tcp = request_test_path_instance(UnderlayProtocol::Tcp, 0, 1);
    let startup = window.limit_bytes_at(
        Some(tcp),
        FlowLane::Latency,
        PATH_OPEN_SCORE_BYTES,
        mux_limits,
        now,
    );
    assert_eq!(startup, reliable_relay_buffer_len(mux_limits));

    let promoted_at = now + Duration::from_millis(1);
    assert_eq!(
        window.limit_bytes_at(
            Some(tcp),
            FlowLane::Throughput,
            64 * 1024,
            mux_limits,
            promoted_at,
        ),
        4 * 1024 * 1024
    );
    window.apply_growth_evidence(
        RequestWindowGrowthEvidence::AckClockTurnover {
            service: tcp,
            turnover_bytes: 2 * 1024 * 1024,
            observed_at: promoted_at,
        },
        FlowLane::Throughput,
        mux_limits,
    );
    assert_eq!(window.product_limit_bytes, 8 * 1024 * 1024);

    let expired_at = promoted_at + Duration::from_secs(1);
    window.growth_epoch_at = promoted_at;
    window.record_tcp_ack_clock_turnover(
        4 * 1024 * 1024,
        Some(tcp),
        FlowLane::Throughput,
        mux_limits,
    );
    assert_eq!(
        window.product_limit_bytes,
        16 * 1024 * 1024,
        "exact per-owner ACK-clock turnover must not depend on relay callback timing"
    );
    assert_eq!(
        window.limit_bytes_at(
            Some(tcp),
            FlowLane::Latency,
            PATH_OPEN_SCORE_BYTES,
            mux_limits,
            expired_at + Duration::from_millis(51),
        ),
        reliable_relay_buffer_len(mux_limits),
        "bulk-to-latency demotion must close previously grown source read-ahead"
    );
    assert_eq!(
        window.limit_bytes_at(
            Some(tcp),
            FlowLane::Throughput,
            64 * 1024,
            mux_limits,
            expired_at + Duration::from_millis(52),
        ),
        4 * 1024 * 1024,
        "promotion starts from the bounded Service reservoir, not the old bulk allowance"
    );
}

#[test]
fn tcp_request_outstanding_turnover_preserves_threshold_carryover() {
    let mux_limits = MuxLimits::default();
    let now = Instant::now();
    let tcp = request_test_path_instance(UnderlayProtocol::Tcp, 0, 1);
    let mut coalesced = RequestOutstandingWindow::new_at(now);
    assert_eq!(
        coalesced.limit_bytes_at(Some(tcp), FlowLane::Throughput, 64 * 1024, mux_limits, now,),
        4 * 1024 * 1024
    );

    coalesced.record_tcp_ack_clock_turnover(
        4 * 1024 * 1024,
        Some(tcp),
        FlowLane::Throughput,
        mux_limits,
    );
    assert_eq!(
        coalesced.product_limit_bytes,
        16 * 1024 * 1024,
        "one modeled aggregate must cross every satisfied doubling threshold"
    );

    let mut poor = RequestOutstandingWindow::new_at(now);
    poor.limit_bytes_at(Some(tcp), FlowLane::Throughput, 64 * 1024, mux_limits, now);
    poor.record_tcp_ack_clock_turnover(
        2 * 1024 * 1024 - 1,
        Some(tcp),
        FlowLane::Throughput,
        mux_limits,
    );
    assert_eq!(
        poor.product_limit_bytes,
        4 * 1024 * 1024,
        "a slow modeled pipe below half-window turnover stays at the memory floor"
    );
}

#[test]
fn tcp_request_turnover_quantization_has_exact_stage_boundaries() {
    let mib = 1024 * 1024;
    let current = 4 * mib;
    let floor = 2 * mib;
    let ceiling = 64 * mib;
    assert_eq!(
        request_tcp_product_limit_for_turnover(current, 2 * mib - 1, floor, ceiling,),
        4 * mib
    );
    assert_eq!(
        request_tcp_product_limit_for_turnover(current, 2 * mib, floor, ceiling,),
        8 * mib
    );
    assert_eq!(
        request_tcp_product_limit_for_turnover(current, 4 * mib, floor, ceiling,),
        16 * mib
    );
    assert_eq!(
        request_tcp_product_limit_for_turnover(current, 8 * mib, floor, ceiling,),
        32 * mib
    );
}

#[test]
fn udp_request_outstanding_limit_uses_service_reservoir_then_product_ack_growth() {
    let mux_limits = MuxLimits::default();
    let now = Instant::now();
    let mut window = RequestOutstandingWindow::new_at(now);
    let udp = request_test_path_instance(UnderlayProtocol::Udp, 0, 1);

    assert_eq!(
        window.limit_bytes_at(Some(udp), FlowLane::Throughput, 64 * 1024, mux_limits, now,),
        4 * 1024 * 1024,
        "QUIC carrier capacity must not bypass the staged product window"
    );
    window.apply_growth_evidence(
        RequestWindowGrowthEvidence::OwnerAckCredits {
            service: udp,
            credits: smallvec![RequestOwnerAckProgress {
                instance: udp,
                bytes: 2 * 1024 * 1024,
            }],
            growth_interval: Duration::from_secs(1),
            observed_at: now + Duration::from_millis(1),
        },
        FlowLane::Throughput,
        mux_limits,
    );
    assert_eq!(
        window.limit_bytes_at(
            Some(udp),
            FlowLane::Throughput,
            64 * 1024,
            mux_limits,
            now + Duration::from_millis(1),
        ),
        8 * 1024 * 1024,
        "durable UDP product ACKs should grow stream read-ahead without becoming carrier-capacity evidence"
    );
}

#[test]
fn request_outstanding_limit_resets_on_protocol_and_exact_instance_handoff() {
    let mux_limits = MuxLimits::default();
    let now = Instant::now();
    let mut window = RequestOutstandingWindow::new_at(now);
    let udp = request_test_path_instance(UnderlayProtocol::Udp, 0, 1);
    let replacement_udp = request_test_path_instance(UnderlayProtocol::Udp, 0, 2);
    let tcp = request_test_path_instance(UnderlayProtocol::Tcp, 0, 3);

    let mut recovering = RequestOutstandingWindow::new_at(now);
    let latency_limit = recovering.limit_bytes_at(
        Some(udp),
        FlowLane::Latency,
        PATH_OPEN_SCORE_BYTES,
        mux_limits,
        now,
    );
    let unavailable = recovering.resolved_service_instance(None, Some(replacement_udp));
    assert_eq!(unavailable, None);
    assert_eq!(
        recovering.limit_bytes_at(
            unavailable,
            FlowLane::Throughput,
            64 * 1024,
            mux_limits,
            now + Duration::from_millis(1),
        ),
        latency_limit,
        "lane promotion without an Active Service must retain the prior bound"
    );

    assert_eq!(
        window.limit_bytes_at(Some(udp), FlowLane::Throughput, 64 * 1024, mux_limits, now,),
        4 * 1024 * 1024
    );
    window.record_acked_at(
        2 * 1024 * 1024,
        udp,
        Some(udp),
        true,
        FlowLane::Throughput,
        Duration::from_secs(1),
        mux_limits,
        now + Duration::from_millis(1),
    );
    let unavailable = window.resolved_service_instance(None, Some(replacement_udp));
    assert_eq!(unavailable, None);
    assert_eq!(
        window.limit_bytes_at(
            unavailable,
            FlowLane::Throughput,
            64 * 1024,
            mux_limits,
            now + Duration::from_millis(2),
        ),
        8 * 1024 * 1024,
        "temporary loss of Active placement must retain the bounded product allowance"
    );
    assert_eq!(
        window.limit_bytes_at(
            unavailable,
            FlowLane::Latency,
            PATH_OPEN_SCORE_BYTES,
            mux_limits,
            now + Duration::from_micros(2500),
        ),
        reliable_relay_buffer_len(mux_limits),
        "demotion must shrink read-ahead even while Service is absent"
    );
    assert_eq!(
        window.limit_bytes_at(
            unavailable,
            FlowLane::Throughput,
            64 * 1024,
            mux_limits,
            now + Duration::from_micros(2750),
        ),
        reliable_relay_buffer_len(mux_limits),
        "promotion without Service must not restore the prior bulk allowance"
    );
    assert_eq!(
        window.limit_bytes_at(
            Some(replacement_udp),
            FlowLane::Throughput,
            64 * 1024,
            mux_limits,
            now + Duration::from_millis(3),
        ),
        4 * 1024 * 1024,
        "same-key replacement must not inherit an old UDP instance's ACK epoch"
    );
    window.record_acked_at(
        2 * 1024 * 1024,
        replacement_udp,
        Some(replacement_udp),
        true,
        FlowLane::Throughput,
        Duration::from_secs(1),
        mux_limits,
        now + Duration::from_millis(4),
    );
    assert_eq!(
        window.limit_bytes_at(
            Some(tcp),
            FlowLane::Throughput,
            64 * 1024,
            mux_limits,
            now + Duration::from_millis(5),
        ),
        4 * 1024 * 1024,
        "UDP-to-TCP handoff must begin a fresh product ACK epoch"
    );
    window.record_acked_at(
        2 * 1024 * 1024,
        tcp,
        Some(tcp),
        true,
        FlowLane::Throughput,
        Duration::from_secs(1),
        mux_limits,
        now + Duration::from_millis(6),
    );
    assert_eq!(
        window.limit_bytes_at(
            Some(udp),
            FlowLane::Throughput,
            64 * 1024,
            mux_limits,
            now + Duration::from_millis(7),
        ),
        4 * 1024 * 1024,
        "TCP-to-UDP handoff must not borrow the TCP product ACK epoch"
    );
}

#[test]
fn request_outstanding_growth_is_scoped_to_the_ordered_service_family() {
    let mux_limits = MuxLimits::default();
    let now = Instant::now();
    let mut window = RequestOutstandingWindow::new_at(now);
    let udp = request_test_path_instance(UnderlayProtocol::Udp, 0, 1);
    let tcp = request_test_path_instance(UnderlayProtocol::Tcp, 0, 2);
    let replacement_tcp = request_test_path_instance(UnderlayProtocol::Tcp, 0, 3);
    assert_eq!(
        window.limit_bytes_at(
            Some(udp),
            FlowLane::Latency,
            PATH_OPEN_SCORE_BYTES,
            mux_limits,
            now,
        ),
        reliable_relay_buffer_len(mux_limits)
    );
    assert_eq!(window.service_epoch_instance, Some(udp));

    assert_eq!(
        window.limit_bytes_at(
            Some(tcp),
            FlowLane::Throughput,
            64 * 1024,
            mux_limits,
            now + Duration::from_millis(1),
        ),
        4 * 1024 * 1024
    );
    window.record_acked_at(
        2 * 1024 * 1024,
        udp,
        Some(tcp),
        true,
        FlowLane::Throughput,
        Duration::from_secs(1),
        mux_limits,
        now + Duration::from_millis(2),
    );
    assert_eq!(
        window.product_limit_bytes,
        4 * 1024 * 1024,
        "UDP-owned progress must not expand the ordered TCP Service allowance"
    );
    window.record_acked_at(
        2 * 1024 * 1024,
        tcp,
        Some(tcp),
        true,
        FlowLane::Throughput,
        Duration::from_secs(1),
        mux_limits,
        now + Duration::from_millis(3),
    );
    assert_eq!(window.product_limit_bytes, 8 * 1024 * 1024);
    assert_eq!(
        window.limit_bytes_at(
            Some(replacement_tcp),
            FlowLane::Throughput,
            64 * 1024,
            mux_limits,
            now + Duration::from_millis(4),
        ),
        4 * 1024 * 1024,
        "a direct TCP carrier handoff must start a fresh path-local ACK clock"
    );
    assert_eq!(
        window.limit_bytes_at(
            None,
            FlowLane::Throughput,
            64 * 1024,
            mux_limits,
            now + Duration::from_millis(5),
        ),
        4 * 1024 * 1024,
        "losing the ordered TCP Service must not reopen reads during recovery"
    );
    assert_eq!(
        window.limit_bytes_at(
            Some(udp),
            FlowLane::Throughput,
            64 * 1024,
            mux_limits,
            now + Duration::from_millis(6),
        ),
        4 * 1024 * 1024,
        "a TCP-to-UDP handoff starts a fresh product window"
    );
    assert_eq!(
        window.limit_bytes_at(
            Some(tcp),
            FlowLane::Throughput,
            64 * 1024,
            mux_limits,
            now + Duration::from_millis(7),
        ),
        4 * 1024 * 1024,
        "a UDP-to-TCP handoff must start a fresh path-local ACK clock"
    );
}

#[test]
fn tcp_request_outstanding_limit_counts_live_subflow_owner_progress() {
    let mux_limits = MuxLimits::default();
    let now = Instant::now();
    let mut window = RequestOutstandingWindow::new_at(now);
    let service = request_test_path_instance(UnderlayProtocol::Tcp, 0, 1);
    let subflow = request_test_path_instance(UnderlayProtocol::Tcp, 1, 2);
    assert_eq!(
        window.limit_bytes_at(
            Some(service),
            FlowLane::Throughput,
            64 * 1024,
            mux_limits,
            now,
        ),
        4 * 1024 * 1024
    );

    window.record_acked_at(
        2 * 1024 * 1024,
        subflow,
        Some(service),
        false,
        FlowLane::Throughput,
        Duration::from_secs(1),
        mux_limits,
        now + Duration::from_millis(1),
    );
    assert_eq!(
        window.product_limit_bytes,
        4 * 1024 * 1024,
        "detached or stale exact instances must not grow the Service epoch"
    );

    window.record_acked_at(
        2 * 1024 * 1024,
        subflow,
        Some(service),
        true,
        FlowLane::Throughput,
        Duration::from_secs(1),
        mux_limits,
        now + Duration::from_millis(2),
    );
    assert_eq!(
        window.product_limit_bytes,
        8 * 1024 * 1024,
        "receiver-confirmed OwnerData on a live same-family subflow must grow stream read-ahead"
    );
}

#[test]
fn request_outstanding_limit_never_exceeds_product_resource_ceiling_for_either_underlay() {
    let mux_limits = MuxLimits {
        max_stream_window_bytes: 1024 * 1024,
        ..MuxLimits::default()
    };
    let now = Instant::now();

    for (ordinal, underlay) in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp]
        .into_iter()
        .enumerate()
    {
        let instance = request_test_path_instance(underlay, ordinal, ordinal as u64 + 1);
        let mut window = RequestOutstandingWindow::new_at(now);
        assert_eq!(
            window.limit_bytes_at(
                Some(instance),
                FlowLane::Throughput,
                64 * 1024,
                mux_limits,
                now,
            ),
            512 * 1024,
            "{underlay:?} should start below the product resource ceiling"
        );
        window.record_acked_at(
            256 * 1024,
            instance,
            Some(instance),
            true,
            FlowLane::Throughput,
            Duration::from_secs(1),
            mux_limits,
            now + Duration::from_millis(1),
        );
        assert_eq!(
            window.limit_bytes_at(
                Some(instance),
                FlowLane::Throughput,
                64 * 1024,
                mux_limits,
                now + Duration::from_millis(1),
            ),
            1024 * 1024
        );
        window.record_acked_at(
            1024 * 1024,
            instance,
            Some(instance),
            true,
            FlowLane::Throughput,
            Duration::from_secs(1),
            mux_limits,
            now + Duration::from_millis(2),
        );
        assert_eq!(
            window.limit_bytes_at(
                Some(instance),
                FlowLane::Throughput,
                64 * 1024,
                mux_limits,
                now + Duration::from_millis(2),
            ),
            1024 * 1024,
            "{underlay:?} product read-ahead must remain resource-bounded"
        );
    }
}
