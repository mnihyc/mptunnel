use super::*;
use crate::model::capacity::reliable_unproven_path_startup_flight_limit_bytes;
use crate::model::path::CarrierPathKey;
use crate::protocol::{Frame, PathUsage, StreamId, UnderlayProtocol};
use crate::runtime::sender::response::test_support::response_target;
use crate::scheduler::PathState;

fn select(
    targets: &[ResponseSenderPathTarget],
    lower_flights: &[CarrierPathFlightDebt],
    ordering_debt: usize,
) -> Option<CarrierPathKey> {
    select_response_data_path(
        targets,
        TrafficClass::Throughput,
        64 * 1024,
        MuxLimits::default(),
        lower_flights,
        ordering_debt,
    )
    .map(|target| target.observation.key)
}

#[test]
fn completion_time_selects_the_earliest_available_path() {
    let first = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    let later = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);

    assert_eq!(
        select(&[first, later.clone()], &[], 0),
        Some(later.observation.key)
    );
}

#[test]
fn contiguous_path_is_not_throttled_by_the_additional_path_startup_limit() {
    let mux_limits = MuxLimits::default();
    let startup_flight = reliable_unproven_path_startup_flight_limit_bytes(mux_limits);
    let mut target = response_target(
        0,
        UnderlayProtocol::Tcp,
        20.0,
        startup_flight,
        16 * 1024 * 1024,
        true,
    );
    target.observation.has_bulk_rate_evidence = true;
    target.observation.snapshot.has_durable_product_progress = false;

    let selected = select_response_data_path(
        &[target.clone()],
        TrafficClass::Throughput,
        64 * 1024,
        mux_limits,
        &[],
        startup_flight as usize,
    )
    .expect("the contiguous path remains governed by carrier credit");
    assert_eq!(selected.observation.key, target.observation.key);

    target.observation.snapshot.has_durable_product_progress = true;
    assert!(
        select_response_data_path(
            &[target],
            TrafficClass::Throughput,
            64 * 1024,
            mux_limits,
            &[],
            startup_flight as usize,
        )
        .is_some(),
        "durable progress does not change contiguous-path eligibility",
    );
}

#[test]
fn additional_unproven_path_owns_at_most_one_startup_flight() {
    let mux_limits = MuxLimits::default();
    let startup_flight = reliable_unproven_path_startup_flight_limit_bytes(mux_limits);
    let owner = response_target(
        0,
        UnderlayProtocol::Tcp,
        80.0,
        64 * 1024,
        16 * 1024 * 1024,
        true,
    );
    let mut candidate = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        startup_flight,
        16 * 1024 * 1024,
        false,
    );
    candidate.observation.snapshot.has_durable_product_progress = false;
    let lower = [CarrierPathFlightDebt {
        key: owner.observation.key,
        output_incarnation: owner.observation.incarnation,
        bytes: 64 * 1024,
    }];

    let selected = select_response_data_path(
        &[owner.clone(), candidate.clone()],
        TrafficClass::Throughput,
        64 * 1024,
        mux_limits,
        &lower,
        (64 * 1024) + startup_flight as usize,
    )
    .expect("the frontier owner remains eligible");
    assert_eq!(selected.observation.key, owner.observation.key);

    candidate.observation.snapshot.has_durable_product_progress = true;
    let selected = select_response_data_path(
        &[owner, candidate.clone()],
        TrafficClass::Throughput,
        64 * 1024,
        mux_limits,
        &lower,
        (64 * 1024) + startup_flight as usize,
    )
    .expect("durable Data ACK progress unlocks additional-path placement");
    assert_eq!(selected.observation.key, candidate.observation.key);
}

#[test]
fn queue_growth_moves_bulk_data_to_the_earlier_completion_path() {
    let mut left = response_target(0, UnderlayProtocol::Tcp, 10.0, 0, 16 * 1024 * 1024, true);
    let mut right = response_target(1, UnderlayProtocol::Tcp, 10.0, 0, 16 * 1024 * 1024, false);
    left.observation.snapshot.queue_bytes = 4 * 1024 * 1024;

    assert_eq!(
        select(&[left.clone(), right.clone()], &[], 0),
        Some(right.observation.key)
    );

    left.observation.snapshot.queue_bytes = 0;
    right.observation.snapshot.queue_bytes = 4 * 1024 * 1024;
    assert_eq!(
        select(&[left.clone(), right], &[], 0),
        Some(left.observation.key)
    );
}

#[test]
fn backup_path_is_used_only_without_a_schedulable_regular_path() {
    let mut regular = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let mut backup = response_target(1, UnderlayProtocol::Udp, 1.0, 0, 16 * 1024 * 1024, false);
    backup.observation.snapshot.policy.backup = true;

    assert_eq!(
        select(&[regular.clone(), backup.clone()], &[], 0),
        Some(regular.observation.key)
    );

    regular.observation.snapshot.state = PathState::Failed;
    assert_eq!(
        select(&[regular, backup.clone()], &[], 0),
        Some(backup.observation.key)
    );
}

#[test]
fn peer_backup_preference_is_directional_and_available_first() {
    let available = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let mut peer_backup =
        response_target(1, UnderlayProtocol::Udp, 1.0, 0, 16 * 1024 * 1024, false);
    peer_backup.observation.snapshot.peer_usage = Some(PathUsage::Backup);

    assert_eq!(
        select(&[available.clone(), peer_backup.clone()], &[], 0),
        Some(available.observation.key)
    );

    let mut failed = available;
    failed.observation.snapshot.state = PathState::Failed;
    assert_eq!(
        select(&[failed, peer_backup.clone()], &[], 0),
        Some(peer_backup.observation.key)
    );
}

#[test]
fn ack_gap_reinjection_requires_a_distinct_measured_output() {
    let available = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let mut backup = response_target(1, UnderlayProtocol::Udp, 1.0, 0, 16 * 1024 * 1024, false);
    backup.observation.snapshot.peer_usage = Some(PathUsage::Backup);
    let frame = Frame::StreamFin {
        stream_id: StreamId(9),
        final_offset: 1024,
    };

    let selected = select_response_frame_path(
        &[available.clone(), backup.clone()],
        TrafficClass::Throughput,
        &frame,
        CarrierEmitMode::StreamOrdered,
        &[available.observation.key],
        Some(RelaySendCause::AckGapReinjection),
    )
    .expect("distinct measured reinjection path");
    assert_eq!(
        selected.observation.key, backup.observation.key,
        "connection-level gap reinjection may use Backup when the Available path owns the missing range",
    );

    let selected = select_response_frame_path(
        &[available.clone(), backup.clone()],
        TrafficClass::Throughput,
        &frame,
        CarrierEmitMode::StreamOrdered,
        &[available.observation.key],
        Some(RelaySendCause::TailReinjection),
    )
    .expect("distinct tail reinjection path");
    assert_eq!(
        selected.observation.key, backup.observation.key,
        "strict tail reinjection may use Backup only because same-path duplication is forbidden",
    );
}

#[test]
fn persistent_ack_gap_reinjection_requires_measured_delivery_rate() {
    let mut unproven = response_target(0, UnderlayProtocol::Tcp, 1.0, 0, 16 * 1024 * 1024, true);
    unproven.observation.has_bulk_rate_evidence = false;
    let proven = response_target(1, UnderlayProtocol::Udp, 40.0, 0, 16 * 1024 * 1024, false);
    let frame = Frame::StreamFin {
        stream_id: StreamId(9),
        final_offset: 1024,
    };

    let selected = select_response_frame_path(
        &[unproven.clone(), proven.clone()],
        TrafficClass::Throughput,
        &frame,
        CarrierEmitMode::StreamOrdered,
        &[],
        Some(RelaySendCause::PersistentAckGapReinjection),
    )
    .expect("measured persistent reinjection target");
    assert_eq!(selected.observation.key, proven.observation.key);
    assert!(
        select_response_frame_path(
            &[unproven.clone()],
            TrafficClass::Throughput,
            &frame,
            CarrierEmitMode::StreamOrdered,
            &[],
            Some(RelaySendCause::PersistentAckGapReinjection),
        )
        .is_none(),
        "an unmeasured output cannot take ownership of a persistent Data ACK gap",
    );
    assert!(
        select_response_frame_path(
            &[unproven.clone()],
            TrafficClass::Throughput,
            &frame,
            CarrierEmitMode::StreamOrdered,
            &[],
            Some(RelaySendCause::AckGapReinjection),
        )
        .is_none(),
        "the bounded PTO-stage event also requires measured delivery evidence",
    );

    let mut alternate = response_target(2, UnderlayProtocol::Udp, 1.0, 0, 16 * 1024 * 1024, false);
    alternate.observation.has_bulk_rate_evidence = false;
    assert!(
        select_response_frame_path(
            &[proven.clone(), alternate],
            TrafficClass::Throughput,
            &frame,
            CarrierEmitMode::StreamOrdered,
            &[proven.observation.key],
            Some(RelaySendCause::PersistentAckGapReinjection),
        )
        .is_none(),
        "native recovery retains a live original output until a measured alternate exists",
    );
}

#[test]
fn path_failure_prefers_measured_survivor_but_preserves_liveness_fallback() {
    let mut unproven = response_target(0, UnderlayProtocol::Tcp, 1.0, 0, 16 * 1024 * 1024, true);
    unproven.observation.has_bulk_rate_evidence = false;
    let proven = response_target(1, UnderlayProtocol::Udp, 40.0, 0, 16 * 1024 * 1024, false);
    let frame = Frame::StreamFin {
        stream_id: StreamId(9),
        final_offset: 1024,
    };

    let selected = select_response_frame_path(
        &[unproven.clone(), proven.clone()],
        TrafficClass::Throughput,
        &frame,
        CarrierEmitMode::StreamOrdered,
        &[],
        Some(RelaySendCause::PathFailureReinjection),
    )
    .expect("measured failed-path survivor");
    assert_eq!(selected.observation.key, proven.observation.key);

    let selected = select_response_frame_path(
        &[unproven.clone()],
        TrafficClass::Throughput,
        &frame,
        CarrierEmitMode::StreamOrdered,
        &[],
        Some(RelaySendCause::PathFailureReinjection),
    )
    .expect("live unmeasured fallback");
    assert_eq!(selected.observation.key, unproven.observation.key);
}

#[test]
fn tcp_and_quic_paths_share_one_completion_time_decision() {
    let tcp = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    let quic = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

    assert_eq!(
        select(&[tcp, quic.clone()], &[], 0),
        Some(quic.observation.key)
    );
}

#[test]
fn cross_underlay_path_can_add_capacity_with_bounded_ordering_debt() {
    let tcp = response_target(
        0,
        UnderlayProtocol::Tcp,
        80.0,
        64 * 1024,
        16 * 1024 * 1024,
        true,
    );
    let quic = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let lower = [CarrierPathFlightDebt {
        key: tcp.observation.key,
        output_incarnation: tcp.observation.incarnation,
        bytes: 64 * 1024,
    }];

    let selected = select_response_data_path(
        &[tcp, quic.clone()],
        TrafficClass::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &lower,
        64 * 1024,
    )
    .expect("bounded cross-underlay work has completion gain");
    assert_eq!(selected.observation.key, quic.observation.key);
}

#[test]
fn replacement_path_does_not_inherit_old_incarnation_frontier_credit() {
    let replacement = response_target(0, UnderlayProtocol::Tcp, 100.0, 0, 64 * 1024 * 1024, true);
    let lower = [CarrierPathFlightDebt {
        key: replacement.observation.key,
        output_incarnation: replacement.observation.incarnation.saturating_add(1),
        bytes: 16 * 1024 * 1024,
    }];

    assert!(
        select_response_data_path(
            &[replacement],
            TrafficClass::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &lower,
            16 * 1024 * 1024,
        )
        .is_some(),
        "the replacement remains a measured failover candidate without becoming the old range owner",
    );
}

#[test]
fn shared_product_queue_does_not_become_path_assigned_work() {
    let mut left = response_target(0, UnderlayProtocol::Tcp, 10.0, 0, 16 * 1024 * 1024, true);
    let mut right = response_target(1, UnderlayProtocol::Tcp, 10.0, 0, 16 * 1024 * 1024, false);
    left.observation.snapshot.data_level_queue_bytes = 32 * 1024 * 1024;
    right.observation.snapshot.data_level_queue_bytes = 32 * 1024 * 1024;
    right.observation.snapshot.queue_bytes = 1024 * 1024;

    assert_eq!(
        select(&[left.clone(), right], &[], 0),
        Some(left.observation.key)
    );
}

#[test]
fn reorder_limit_allows_the_path_draining_the_older_range() {
    let mux_limits = MuxLimits {
        max_reorder_bytes: 128 * 1024,
        max_stream_window_bytes: 128 * 1024,
        ..MuxLimits::default()
    };
    let owner = response_target(
        0,
        UnderlayProtocol::Tcp,
        20.0,
        64 * 1024,
        16 * 1024 * 1024,
        true,
    );
    let alternate = response_target(1, UnderlayProtocol::Udp, 1.0, 0, 16 * 1024 * 1024, false);
    let lower = [CarrierPathFlightDebt {
        key: owner.observation.key,
        output_incarnation: owner.observation.incarnation,
        bytes: 64 * 1024,
    }];

    let selected = select_response_data_path(
        &[owner.clone(), alternate],
        TrafficClass::Throughput,
        64 * 1024,
        mux_limits,
        &lower,
        64 * 1024,
    )
    .expect("the path carrying the older range remains schedulable");
    assert_eq!(selected.observation.key, owner.observation.key);
}

#[test]
fn modeled_pipe_limit_stops_offsets_before_the_configured_reorder_ceiling() {
    let payload = 64 * 1024;
    let owner = response_target(
        0,
        UnderlayProtocol::Tcp,
        100.0,
        1024 * 1024,
        64 * 1024 * 1024,
        true,
    );
    let alternate = response_target(
        1,
        UnderlayProtocol::Tcp,
        100.0,
        16 * 1024 * 1024,
        64 * 1024 * 1024,
        false,
    );
    let lower = [
        CarrierPathFlightDebt {
            key: owner.observation.key,
            output_incarnation: owner.observation.incarnation,
            bytes: 1024 * 1024,
        },
        CarrierPathFlightDebt {
            key: alternate.observation.key,
            output_incarnation: alternate.observation.incarnation,
            bytes: 16 * 1024 * 1024,
        },
    ];

    assert!(
        select_response_data_path(
            &[owner, alternate],
            TrafficClass::Throughput,
            payload,
            MuxLimits::default(),
            &lower,
            17 * 1024 * 1024,
        )
        .is_none(),
        "the negotiated 64 MiB safety ceiling must not replace the measured BDP/in-flight boundary",
    );
}

#[test]
fn failed_paths_do_not_receive_new_data() {
    let mut tcp = response_target(0, UnderlayProtocol::Tcp, 1.0, 0, 16 * 1024 * 1024, true);
    let mut quic = response_target(1, UnderlayProtocol::Udp, 1.0, 0, 16 * 1024 * 1024, false);
    tcp.observation.snapshot.state = PathState::Failed;
    quic.observation.snapshot.state = PathState::Failed;

    assert_eq!(select(&[tcp, quic], &[], 0), None);
}
