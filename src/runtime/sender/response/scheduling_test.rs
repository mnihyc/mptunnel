use super::*;
use crate::model::capacity::reliable_unproven_path_startup_flight_limit_bytes;
use crate::model::path::CarrierPathKey;
use crate::protocol::{Frame, PathId, PathUsage, StreamId, UnderlayProtocol};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::sender::response::test_support::response_target;
use crate::scheduler::PathState;
use bytes::Bytes;

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

fn block_data_queue(target: &mut ResponseSenderPathTarget) {
    let (commands, _receivers) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id: StreamId(1),
                offset: 0,
                payload: Bytes::from_static(b"queued"),
            },
            TrafficClass::Throughput,
        )
        .expect("fill data queue");
    target.command_queue = commands.queue_snapshot();
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
fn sole_tcp_path_is_not_double_limited_by_product_flight() {
    let mux_limits = MuxLimits::default();
    let send_window = reliable_unproven_path_startup_flight_limit_bytes(mux_limits);
    let mut target = response_target(
        0,
        UnderlayProtocol::Tcp,
        20.0,
        send_window,
        send_window,
        true,
    );
    target.observation.has_bulk_rate_evidence = true;
    target.observation.snapshot.has_durable_product_progress = false;

    assert_eq!(
        select_response_data_path(
            &[target.clone()],
            TrafficClass::Throughput,
            64 * 1024,
            mux_limits,
            &[],
            send_window as usize,
        )
        .map(|selected| selected.observation.key),
        Some(target.observation.key),
        "one live carrier remains work-conserving under its native TCP controller"
    );
}

#[test]
fn sole_quic_path_is_not_double_limited_by_product_flight() {
    let mux_limits = MuxLimits::default();
    let startup_flight = reliable_unproven_path_startup_flight_limit_bytes(mux_limits);
    let mut target = response_target(
        0,
        UnderlayProtocol::Udp,
        20.0,
        startup_flight,
        16 * 1024 * 1024,
        true,
    );
    target.observation.has_bulk_rate_evidence = true;
    target.observation.snapshot.has_durable_product_progress = false;

    assert_eq!(
        select_response_data_path(
            &[target.clone()],
            TrafficClass::Throughput,
            64 * 1024,
            mux_limits,
            &[],
            startup_flight as usize,
        )
        .map(|selected| selected.observation.key),
        Some(target.observation.key),
        "one live carrier remains work-conserving under Quinn's native controller",
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

    candidate.observation.original_data_in_flight_bytes = 0;
    let selected = select_response_data_path(
        &[owner.clone(), candidate.clone()],
        TrafficClass::Throughput,
        64 * 1024,
        mux_limits,
        &lower,
        64 * 1024,
    )
    .expect("Data ACK release opens another bounded startup flight");
    assert_eq!(
        selected.observation.key, candidate.observation.key,
        "connection acknowledgement must not become a permanent per-path send quota",
    );

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
fn blocked_frontier_owner_allows_only_bounded_cold_path_startup() {
    let mut owner = response_target(
        0,
        UnderlayProtocol::Tcp,
        115.0,
        2 * 1024 * 1024,
        16 * 1024 * 1024,
        true,
    );
    block_data_queue(&mut owner);
    let mut cold = response_target(1, UnderlayProtocol::Tcp, 333.0, 0, 16 * 1024 * 1024, false);
    cold.observation.snapshot.delivery_rate_bps = 351_000.0;
    cold.observation.snapshot.pacing_rate_bps = 351_000.0;
    cold.observation.snapshot.confidence = 0.0;
    cold.observation.snapshot.app_limited = true;
    cold.observation.snapshot.has_durable_product_progress = false;
    cold.observation.has_bulk_rate_evidence = false;
    let lower = [CarrierPathFlightDebt {
        key: owner.observation.key,
        output_incarnation: owner.observation.incarnation,
        bytes: 2 * 1024 * 1024,
    }];

    assert_eq!(
        select(&[owner.clone(), cold.clone()], &lower, 2 * 1024 * 1024),
        Some(cold.observation.key),
        "an unmeasured path can acquire a bounded startup service flight",
    );
    let startup = reliable_unproven_path_startup_flight_limit_bytes(MuxLimits::default());
    cold.observation.original_data_in_flight_bytes = startup;
    cold.observation.snapshot.data_level_bytes_in_flight = startup;
    assert_eq!(
        select(&[owner, cold], &lower, 2 * 1024 * 1024),
        None,
        "cold-path exploration stops at its startup service limit",
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
fn ack_gap_reinjection_uses_distinct_repair_headroom_when_fresh_data_is_full() {
    let available = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let mut backup = response_target(1, UnderlayProtocol::Udp, 1.0, 0, 16 * 1024 * 1024, false);
    backup.observation.snapshot.peer_usage = Some(PathUsage::Backup);
    block_data_queue(&mut backup);
    let frame = Frame::StreamData {
        stream_id: StreamId(9),
        offset: 0,
        payload: Bytes::from_static(b"repair"),
    };

    let selected = select_response_frame_path(
        &[available.clone(), backup.clone()],
        TrafficClass::Throughput,
        &frame,
        CarrierEmitMode::StreamOrdered,
        &[(available.observation.key, available.observation.incarnation)],
        Some(RelaySendCause::AckGapReinjection),
    )
    .expect("distinct reinjection path with repair headroom");
    assert_eq!(
        selected.observation.key, backup.observation.key,
        "connection-level gap reinjection may use Backup when the Available path owns the missing range",
    );

    let selected = select_response_frame_path(
        &[available.clone(), backup.clone()],
        TrafficClass::Throughput,
        &frame,
        CarrierEmitMode::StreamOrdered,
        &[(available.observation.key, available.observation.incarnation)],
        Some(RelaySendCause::TailReinjection),
    )
    .expect("distinct tail reinjection path");
    assert_eq!(
        selected.observation.key, backup.observation.key,
        "strict tail reinjection may use Backup only because same-path duplication is forbidden",
    );
}

#[test]
fn persistent_ack_gap_model_requires_progress_but_bounded_repair_does_not() {
    let mut unproven = response_target(0, UnderlayProtocol::Tcp, 1.0, 0, 16 * 1024 * 1024, true);
    unproven.observation.has_bulk_rate_evidence = false;
    unproven.observation.snapshot.has_durable_product_progress = false;
    let proven = response_target(1, UnderlayProtocol::Udp, 40.0, 0, 16 * 1024 * 1024, false);
    let mut durable_only =
        response_target(2, UnderlayProtocol::Tcp, 20.0, 0, 16 * 1024 * 1024, false);
    durable_only.observation.has_bulk_rate_evidence = false;
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
        "an output without observed delivery progress cannot take ownership of a persistent Data ACK gap",
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
        .is_some(),
        "one bounded repair quantum needs a live carrier, not a mature rate sample",
    );
    let selected = select_response_frame_path(
        &[durable_only.clone()],
        TrafficClass::Throughput,
        &frame,
        CarrierEmitMode::StreamOrdered,
        &[],
        Some(RelaySendCause::PersistentAckGapReinjection),
    )
    .expect("Data ACK progress is sufficient reinjection eligibility");
    assert_eq!(selected.observation.key, durable_only.observation.key);
    assert!(
        select_response_frame_path(
            &[durable_only],
            TrafficClass::Throughput,
            &frame,
            CarrierEmitMode::StreamOrdered,
            &[],
            Some(RelaySendCause::AckGapReinjection),
        )
        .is_some(),
        "the bounded recovery event must not depend on ACK sample timing once product delivery is proven",
    );

    let mut alternate = response_target(2, UnderlayProtocol::Udp, 1.0, 0, 16 * 1024 * 1024, false);
    alternate.observation.has_bulk_rate_evidence = false;
    alternate.observation.snapshot.has_durable_product_progress = false;
    assert!(
        select_response_frame_path(
            &[proven.clone(), alternate],
            TrafficClass::Throughput,
            &frame,
            CarrierEmitMode::StreamOrdered,
            &[(proven.observation.key, proven.observation.incarnation)],
            Some(RelaySendCause::PersistentAckGapReinjection),
        )
        .is_none(),
        "native recovery retains a live original output until an alternate proves delivery progress",
    );
}

#[test]
fn ack_gap_reinjection_can_use_a_replacement_of_the_original_path_key() {
    let replacement = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
    let old_incarnation = replacement.observation.incarnation.wrapping_add(1);
    let frame = Frame::StreamFin {
        stream_id: StreamId(9),
        final_offset: 1024,
    };

    let selected = select_response_frame_path(
        std::slice::from_ref(&replacement),
        TrafficClass::Throughput,
        &frame,
        CarrierEmitMode::StreamOrdered,
        &[(replacement.observation.key, old_incarnation)],
        Some(RelaySendCause::AckGapReinjection),
    )
    .expect("a replacement carrier is not the original output instance");

    assert_eq!(
        (selected.observation.key, selected.observation.incarnation),
        (
            replacement.observation.key,
            replacement.observation.incarnation
        )
    );
}

#[test]
fn reinjection_uses_an_idle_tcp_carrier_instead_of_appending_to_native_backlog() {
    let original = response_target(0, UnderlayProtocol::Udp, 20.0, 0, 16 * 1024 * 1024, true);
    let mut busy_tcp = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    busy_tcp.observation.native_queue_bytes = 64 * 1024;
    busy_tcp.observation.snapshot.queue_bytes = busy_tcp.observation.native_queue_bytes;
    let idle_tcp = response_target(2, UnderlayProtocol::Tcp, 15.0, 0, 16 * 1024 * 1024, false);
    let frame = Frame::StreamFin {
        stream_id: StreamId(9),
        final_offset: 1024,
    };
    let avoid_original = [(original.observation.key, original.observation.incarnation)];

    let selected = select_response_frame_path(
        &[busy_tcp.clone(), idle_tcp.clone()],
        TrafficClass::Throughput,
        &frame,
        CarrierEmitMode::StreamOrdered,
        &avoid_original,
        Some(RelaySendCause::AckGapReinjection),
    )
    .expect("idle TCP repair carrier");

    assert_eq!(
        (selected.observation.key, selected.observation.incarnation),
        (idle_tcp.observation.key, idle_tcp.observation.incarnation)
    );
    assert!(
        select_response_frame_path(
            std::slice::from_ref(&busy_tcp),
            TrafficClass::Throughput,
            &frame,
            CarrierEmitMode::StreamOrdered,
            &avoid_original,
            Some(RelaySendCause::AckGapReinjection),
        )
        .is_none(),
        "repair appended to a busy TCP byte stream cannot overtake its backlog",
    );
}

#[test]
fn tcp_reinjection_waits_for_the_ordered_writer_to_handoff_its_batch() {
    let mut tcp = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    tcp.observation.writer_pending_bytes = 64 * 1024;
    let frame = Frame::StreamFin {
        stream_id: StreamId(9),
        final_offset: 1024,
    };

    assert!(
        select_response_frame_path(
            std::slice::from_ref(&tcp),
            TrafficClass::Throughput,
            &frame,
            CarrierEmitMode::StreamOrdered,
            &[],
            Some(RelaySendCause::AckGapReinjection),
        )
        .is_none(),
        "a priority command cannot overtake a frame held by the private ordered writer",
    );
    tcp.observation.writer_pending_bytes = 0;
    assert!(
        select_response_frame_path(
            &[tcp],
            TrafficClass::Throughput,
            &frame,
            CarrierEmitMode::StreamOrdered,
            &[],
            Some(RelaySendCause::AckGapReinjection),
        )
        .is_some(),
    );
}

#[test]
fn tcp_reinjection_waits_for_native_unacknowledged_flight_to_drain() {
    let original = response_target(0, UnderlayProtocol::Udp, 20.0, 0, 16 * 1024 * 1024, true);
    let busy_tcp = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        64 * 1024,
        16 * 1024 * 1024,
        false,
    );
    let frame = Frame::StreamFin {
        stream_id: StreamId(9),
        final_offset: 1024,
    };

    assert!(
        select_response_frame_path(
            std::slice::from_ref(&busy_tcp),
            TrafficClass::Throughput,
            &frame,
            CarrierEmitMode::StreamOrdered,
            &[(original.observation.key, original.observation.incarnation)],
            Some(RelaySendCause::AckGapReinjection),
        )
        .is_none(),
        "MPTCP-style data-level repair requires an idle native TCP queue",
    );
}

#[test]
fn tcp_reinjection_uses_product_flight_without_complete_native_drain_evidence() {
    let frame = Frame::StreamFin {
        stream_id: StreamId(9),
        final_offset: 1024,
    };

    for carrier_limit in [0, 16 * 1024 * 1024] {
        let mut tcp = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, carrier_limit, false);
        tcp.observation.native_drain_observed = false;
        tcp.observation.original_data_in_flight_bytes = 64 * 1024;
        tcp.observation.snapshot.data_level_bytes_in_flight = 64 * 1024;

        assert!(
            select_response_frame_path(
                std::slice::from_ref(&tcp),
                TrafficClass::Throughput,
                &frame,
                CarrierEmitMode::StreamOrdered,
                &[],
                Some(RelaySendCause::AckGapReinjection),
            )
            .is_none(),
            "missing native drain counters cannot be treated as measured idle while product flight remains",
        );
        tcp.observation.original_data_in_flight_bytes = 0;
        tcp.observation.snapshot.data_level_bytes_in_flight = 0;
        assert!(
            select_response_frame_path(
                &[tcp],
                TrafficClass::Throughput,
                &frame,
                CarrierEmitMode::StreamOrdered,
                &[],
                Some(RelaySendCause::AckGapReinjection),
            )
            .is_some(),
            "TCP repair becomes eligible after exact product flight drains",
        );
    }
}

#[test]
fn quic_reinjection_waits_for_same_stream_unique_flight_and_writer_backlog() {
    let mut quic = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    quic.observation.original_data_in_flight_bytes = 64 * 1024;
    quic.observation.snapshot.data_level_bytes_in_flight = 64 * 1024;
    let frame = Frame::StreamFin {
        stream_id: StreamId(9),
        final_offset: 1024,
    };
    let select = |target: &ResponseSenderPathTarget| {
        select_response_frame_path(
            std::slice::from_ref(target),
            TrafficClass::Throughput,
            &frame,
            CarrierEmitMode::StreamOrdered,
            &[],
            Some(RelaySendCause::AckGapReinjection),
        )
    };

    assert!(select(&quic).is_none());
    quic.observation.original_data_in_flight_bytes = 0;
    quic.observation.snapshot.data_level_bytes_in_flight = 0;
    quic.observation.native_queue_bytes = 1;
    assert!(select(&quic).is_none());
    quic.observation.native_queue_bytes = 0;
    assert!(
        select(&quic).is_some(),
        "QUIC packet flight on other streams is not a same-stream ordering backlog",
    );
}

#[test]
fn path_failure_prefers_measured_survivor_but_preserves_liveness_fallback() {
    let mut unproven = response_target(0, UnderlayProtocol::Tcp, 1.0, 0, 16 * 1024 * 1024, true);
    unproven.observation.has_bulk_rate_evidence = false;
    unproven.observation.snapshot.has_durable_product_progress = false;
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
fn replacement_path_gets_bounded_credit_without_inheriting_old_incarnation() {
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
        "a replacement remains a bounded failover candidate without inheriting the old range",
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
fn shared_reorder_envelope_allows_metric_selection_within_the_limit() {
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
    .expect("one more quantum fits the shared reorder envelope");
    assert_eq!(
        selected.observation.key,
        CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        },
        "path metrics, not the carrier family of the lower range, choose among candidates inside the shared envelope",
    );
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
