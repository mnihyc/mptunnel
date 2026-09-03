use super::*;
use crate::model::admission::ReliableDataAckFrontierState;
use crate::model::capacity::{
    reliable_bulk_carrier_feed_quantum_bytes, reliable_unproven_path_startup_flight_limit_bytes,
};
use crate::model::path::CarrierPathKey;
use crate::protocol::{Frame, PathId, PathUsage, StreamId, UnderlayProtocol};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::sender::response::test_support::response_target;
use crate::scheduler::{PathRateScope, PathState};
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
fn response_data_queue_readiness_follows_its_traffic_class() {
    let mut target = response_target(0, UnderlayProtocol::Tcp, 20.0, 0, 16 * 1024 * 1024, true);
    block_data_queue(&mut target);

    assert!(
        select_response_data_path(
            std::slice::from_ref(&target),
            TrafficClass::Throughput,
            4096,
            MuxLimits::default(),
            &[],
            0,
        )
        .is_none(),
        "a full bulk queue remains backpressured",
    );
    assert_eq!(
        select_response_data_path(
            std::slice::from_ref(&target),
            TrafficClass::Latency,
            4096,
            MuxLimits::default(),
            &[],
            0,
        )
        .map(|selected| selected.observation.key),
        Some(target.observation.key),
        "latency data may use its independent priority queue",
    );
}

#[test]
fn stale_response_output_is_excluded_until_it_is_the_only_live_output() {
    let mut stale = response_target(0, UnderlayProtocol::Tcp, 1.0, 0, 16 * 1024 * 1024, true);
    stale.observation.stale_for_original_data = true;
    let mut alternate = response_target(1, UnderlayProtocol::Udp, 40.0, 0, 16 * 1024 * 1024, false);

    assert_eq!(
        select(&[stale.clone(), alternate.clone()], &[], 0),
        Some(alternate.observation.key),
        "a live non-stale output owns all new OriginalData placement"
    );
    let mut draining = alternate.clone();
    draining.product_admission_active = false;
    assert_eq!(
        select(&[stale.clone(), draining], &[], 0),
        Some(stale.observation.key),
        "a Product-inactive drain cannot suppress the stale active fallback"
    );
    let mut probe_only = alternate.clone();
    probe_only.observation.snapshot.policy.probe_only = true;
    assert_eq!(
        select(&[stale.clone(), probe_only], &[], 0),
        Some(stale.observation.key),
        "an unschedulable non-stale output cannot suppress the stale active fallback"
    );
    block_data_queue(&mut alternate);
    assert_eq!(
        select(&[stale.clone(), alternate], &[], 0),
        None,
        "transient alternate backpressure does not reactivate stale placement"
    );
    assert_eq!(
        select(std::slice::from_ref(&stale), &[], 0),
        Some(stale.observation.key),
        "the still-live stale output is the sole-output liveness fallback"
    );

    let identity = crate::runtime::sender::ServerReinjectionOutputIdentity {
        key: stale.observation.key,
        incarnation: stale.observation.incarnation,
    };
    let frame = Frame::StreamData {
        stream_id: StreamId(1),
        offset: 0,
        payload: Bytes::from_static(b"repair"),
    };
    assert!(
        select_response_frame_path(
            std::slice::from_ref(&stale),
            TrafficClass::Throughput,
            &frame,
            CarrierEmitMode::Classified,
            &[(identity.key, identity.incarnation)],
            Some(RelaySendCause::StaleResponsePathReinjection(identity)),
        )
        .is_none(),
        "stale recovery never reinjects onto its stale owner"
    );
}

#[test]
fn latency_response_does_not_wait_for_a_bulk_service_window() {
    let mut target = response_target(0, UnderlayProtocol::Tcp, 20.0, 0, 1024 * 1024, true);
    target.observation.snapshot.active_flows = 2;
    target.observation.snapshot.active_latency_sensitive_flows = 1;
    target.observation.snapshot.queue_bytes = 64 * 1024;
    target.observation.snapshot.data_level_limit_bytes = 24 * 1024;

    assert_eq!(
        select_response_data_path(
            &[target.clone()],
            TrafficClass::Latency,
            64,
            MuxLimits::default(),
            &[],
            0,
        )
        .map(|selected| selected.observation.key),
        Some(target.observation.key),
        "waiting cannot reprioritize a latency frame ahead of an ordered carrier backlog",
    );
}

#[test]
fn latency_response_still_requires_exact_product_headroom() {
    let mut target = response_target(0, UnderlayProtocol::Tcp, 20.0, 0, 1024 * 1024, true);
    target.observation.snapshot.data_level_limit_bytes = 24 * 1024;
    target.observation.snapshot.data_level_bytes_in_flight = 24 * 1024;
    target.observation.original_data_in_flight_bytes = 24 * 1024;

    assert!(
        select_response_data_path(
            &[target],
            TrafficClass::Latency,
            64,
            MuxLimits::default(),
            &[],
            0,
        )
        .is_none(),
        "latency bypasses ECF and hysteresis, not exact Product O < P authority",
    );
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
fn authenticated_carrier_does_not_wait_for_redundant_stream_proof() {
    let validated = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    let mut unvalidated =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    unvalidated.observation.has_path_proof_evidence = false;

    assert_eq!(
        select(&[validated.clone(), unvalidated.clone()], &[], 0),
        Some(unvalidated.observation.key),
        "carrier establishment and authenticated path join already authorize placement",
    );

    unvalidated.observation.has_path_proof_evidence = true;
    assert_eq!(
        select(&[validated, unvalidated.clone()], &[], 0),
        Some(unvalidated.observation.key),
        "proof completion admits the faster path without a timing threshold",
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
fn sole_tcp_path_with_a_latency_flow_defers_native_arbitration_to_its_writer() {
    let mux_limits = MuxLimits::default();
    let product_flight = 2 * 1024 * 1024_u64;
    let mut target = response_target(
        0,
        UnderlayProtocol::Tcp,
        20.0,
        product_flight,
        16 * 1024 * 1024,
        true,
    );
    target.observation.snapshot.active_flows = 2;
    target.observation.snapshot.active_latency_sensitive_flows = 1;

    assert_eq!(
        select_response_data_path(
            &[target],
            TrafficClass::Throughput,
            64 * 1024,
            mux_limits,
            &[],
            product_flight as usize,
        )
        .map(|selected| selected.observation.key),
        Some(CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        }),
        "sampled flow mix ranks completion but cannot replace the exact bounded writer reservation as native admission authority",
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
fn sole_tcp_path_does_not_treat_sampled_native_shape_as_enqueue_credit() {
    let mux_limits = MuxLimits::default();
    let native_cwnd = 112 * 1024_u64;
    let product_flight = 10 * 1024 * 1024_u64;
    let mut target = response_target(
        0,
        UnderlayProtocol::Tcp,
        100.0,
        native_cwnd,
        native_cwnd,
        true,
    );
    target.observation.snapshot.has_durable_product_progress = false;
    target.observation.original_data_in_flight_bytes = product_flight;
    target.observation.snapshot.data_level_bytes_in_flight = product_flight;
    target.observation.snapshot.data_level_limit_bytes = product_flight + 64 * 1024;
    target.observation.snapshot.queue_bytes = product_flight - native_cwnd;

    assert_eq!(
        select_response_data_path(
            &[target.clone()],
            TrafficClass::Throughput,
            64 * 1024,
            mux_limits,
            &[],
            product_flight as usize,
        )
        .map(|selected| selected.observation.key),
        Some(target.observation.key),
        "exact queue readiness and the bounded writer head, not periodic cwnd/queue samples, own feed",
    );
}

#[test]
fn sole_tcp_path_without_native_telemetry_still_uses_the_bounded_writer_head() {
    let mux_limits = MuxLimits::default();
    let product_flight = 32 * 1024 * 1024_u64;
    let mut target = response_target(0, UnderlayProtocol::Tcp, 100.0, product_flight, 0, true);
    target.observation.snapshot.data_level_limit_bytes = product_flight + 64 * 1024;

    assert_eq!(
        select_response_data_path(
            &[target.clone()],
            TrafficClass::Throughput,
            64 * 1024,
            mux_limits,
            &[],
            product_flight as usize,
        )
        .map(|selected| selected.observation.key),
        Some(target.observation.key),
        "optional TCP telemetry cannot turn unknown native state into a closed Product gate",
    );
}

#[test]
fn sole_quic_path_defers_native_enqueue_authority_to_exact_writer_reservation() {
    let mux_limits = MuxLimits::default();
    let native_cwnd = 112 * 1024_u64;
    let product_flight = 10 * 1024 * 1024_u64;
    let mut target = response_target(
        0,
        UnderlayProtocol::Udp,
        100.0,
        native_cwnd,
        native_cwnd,
        true,
    );
    target.observation.snapshot.has_durable_product_progress = false;
    target.observation.original_data_in_flight_bytes = product_flight;
    target.observation.snapshot.data_level_bytes_in_flight = product_flight;
    target.observation.snapshot.queue_bytes = product_flight - native_cwnd;

    assert_eq!(
        select_response_data_path(
            &[target.clone()],
            TrafficClass::Throughput,
            64 * 1024,
            mux_limits,
            &[],
            product_flight as usize,
        )
        .map(|selected| selected.observation.key),
        Some(target.observation.key),
        "sampled native queue/cwnd are completion diagnostics; the exact bounded QUIC writer reservation owns native enqueue authority",
    );
}

#[test]
fn repair_debt_cannot_reopen_additional_path_original_credit() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = 64 * 1024;
    let mut owner = response_target(
        0,
        UnderlayProtocol::Tcp,
        100.0,
        payload_bytes as u64,
        4 * 1024 * 1024,
        true,
    );
    owner.observation.snapshot.data_level_limit_bytes = 4 * 1024 * 1024;
    let lower = [CarrierPathFlightDebt {
        key: owner.observation.key,
        output_incarnation: owner.observation.incarnation,
        bytes: payload_bytes as u64,
    }];

    let native_window = 1024 * 1024_u64;
    let repair_flight = 64 * 1024_u64;
    let forward_ceiling = native_window
        + u64::try_from(reliable_bulk_carrier_feed_quantum_bytes(mux_limits))
            .expect("test feed quantum fits u64");
    let mut alternate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, native_window, false);
    alternate.observation.snapshot.data_level_limit_bytes = forward_ceiling;
    alternate.observation.original_data_in_flight_bytes = 8 * 1024 * 1024;
    alternate.observation.snapshot.data_level_bytes_in_flight =
        alternate.observation.original_data_in_flight_bytes + repair_flight;

    assert_eq!(
        select_response_data_path(
            &[owner.clone(), alternate.clone()],
            TrafficClass::Throughput,
            payload_bytes,
            mux_limits,
            &lower,
            payload_bytes,
        )
        .map(|selected| selected.observation.key),
        Some(owner.observation.key),
        "retained OriginalData and repair debt cannot renew an additional path's forward credit",
    );

    alternate.observation.original_data_in_flight_bytes =
        forward_ceiling.saturating_sub(payload_bytes as u64);
    alternate.observation.snapshot.data_level_bytes_in_flight =
        alternate.observation.original_data_in_flight_bytes + repair_flight;
    assert_eq!(
        select_response_data_path(
            &[owner, alternate.clone()],
            TrafficClass::Throughput,
            payload_bytes,
            mux_limits,
            &lower,
            payload_bytes,
        )
        .map(|selected| selected.observation.key),
        Some(alternate.observation.key),
        "Data ACK progress below the live forward ceiling reopens the faster additional path",
    );
}

#[test]
fn sole_survivor_cannot_extend_a_nonlive_cross_path_frontier_without_bound() {
    let mux_limits = MuxLimits::default();
    let mut survivor = response_target(
        3,
        UnderlayProtocol::Udp,
        900.0,
        8 * 1024 * 1024,
        3 * 1024 * 1024,
        true,
    );
    survivor.observation.snapshot.data_level_limit_bytes = 3 * 1024 * 1024;
    survivor.observation.snapshot.data_level_bytes_in_flight = 8 * 1024 * 1024;
    let absent_owner = CarrierPathFlightDebt {
        key: crate::model::path::CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: crate::protocol::PathId(2),
        },
        output_incarnation: survivor.observation.incarnation.wrapping_add(1),
        bytes: 10 * 1024 * 1024,
    };

    assert!(
        select_response_data_path(
            &[survivor],
            TrafficClass::Throughput,
            64 * 1024,
            mux_limits,
            &[absent_owner],
            18 * 1024 * 1024,
        )
        .is_none(),
        "a sole survivor is still an additional output while another output owns the lower Data Sequence frontier",
    );
}

#[test]
fn clear_frontier_owner_remains_work_conserving_below_product_window() {
    let mux_limits = MuxLimits::default();
    let product_flight = 2 * 1024 * 1024_u64;
    let mut owner = response_target(
        0,
        UnderlayProtocol::Tcp,
        5.0,
        product_flight,
        512 * 1024,
        true,
    );
    owner.observation.snapshot.data_level_limit_bytes = product_flight + 64 * 1024;
    owner.observation.snapshot.bytes_in_flight = 0;
    let mut alternate = response_target(1, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, false);
    block_data_queue(&mut alternate);
    let lower = [CarrierPathFlightDebt {
        key: owner.observation.key,
        output_incarnation: owner.observation.incarnation,
        bytes: product_flight,
    }];

    assert_eq!(
        select_response_data_path(
            &[owner.clone(), alternate.clone()],
            TrafficClass::Throughput,
            64 * 1024,
            mux_limits,
            &lower,
            product_flight as usize,
        )
        .map(|selected| selected.observation.key),
        Some(owner.observation.key),
        "the exact frontier owner remains work-conserving while Product credit remains",
    );
}

#[test]
fn live_response_frontier_cannot_bypass_an_exhausted_product_window() {
    let mux_limits = MuxLimits::default();
    let product_flight = 48 * 1024 * 1024_u64;
    let mut owner = response_target(
        0,
        UnderlayProtocol::Tcp,
        5.0,
        product_flight,
        512 * 1024,
        true,
    );
    owner.observation.snapshot.data_level_limit_bytes = 1024 * 1024;
    owner.observation.snapshot.bytes_in_flight = 0;
    let mut alternate = response_target(1, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, false);
    block_data_queue(&mut alternate);
    let lower = [CarrierPathFlightDebt {
        key: owner.observation.key,
        output_incarnation: owner.observation.incarnation,
        bytes: product_flight,
    }];

    assert!(
        select_response_data_path(
            &[owner.clone(), alternate.clone()],
            TrafficClass::Throughput,
            64 * 1024,
            mux_limits,
            &lower,
            product_flight as usize,
        )
        .is_none(),
        "a live contiguous owner cannot renew Product debt merely because TCP has native credit",
    );
    assert!(
        select_response_data_path_at_frontier(
            &[owner, alternate],
            TrafficClass::Throughput,
            64 * 1024,
            mux_limits,
            &lower,
            product_flight as usize,
            ReliableDataAckFrontierState::AuthoritativeGap,
        )
        .is_none(),
        "an authoritative gap remains blocked by the same exhausted Product authority",
    );
}

#[test]
fn unproven_path_payload_is_not_hard_capped_by_sampled_native_window() {
    let mut target = response_target(0, UnderlayProtocol::Tcp, 800.0, 0, 16 * 1024, true);
    target.observation.has_bulk_rate_evidence = false;
    target.observation.snapshot.has_durable_product_progress = false;
    target.observation.snapshot.product_progress_rate_bps = None;

    let selected = select_response_data_path_with_payload(
        &[target],
        TrafficClass::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        0,
        ReliableDataAckFrontierState::Live,
    )
    .expect("unproven path with native credit");

    assert_eq!(
        selected.payload_bytes,
        64 * 1024,
        "sampled native flight is completion evidence, not user-space send credit",
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
fn recovered_quic_native_opportunity_receives_unique_product_before_product_rate_refresh() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = 64 * 1024;
    let lead_flight = 8 * 1024 * 1024_u64;
    let mut lead = response_target(
        0,
        UnderlayProtocol::Tcp,
        100.0,
        lead_flight,
        2 * 1024 * 1024,
        true,
    );
    lead.observation.snapshot.delivery_rate_bps = 16_000_000.0;
    lead.observation.snapshot.pacing_rate_bps = 16_000_000.0;
    lead.observation.snapshot.carrier_delivery_rate_bps = Some(16_000_000.0);
    lead.observation.snapshot.product_progress_rate_bps = Some(16_000_000.0);

    let mut recovered = response_target(1, UnderlayProtocol::Udp, 100.0, 0, 6_250_000, false);
    // This is the reported recovery state: native QUIC has already exposed a
    // high delivery/pacing opportunity, while the path-local Product rate has
    // expired because no fresh unique range has yet completed on this output.
    recovered.observation.snapshot.delivery_rate_bps = 392_000_000.0;
    recovered.observation.snapshot.pacing_rate_bps = 471_000_000.0;
    recovered.observation.snapshot.carrier_delivery_rate_bps = Some(392_000_000.0);
    recovered.observation.snapshot.product_progress_rate_bps = None;
    recovered.observation.snapshot.has_durable_product_progress = false;
    recovered.observation.has_bulk_rate_evidence = true;

    let lower = [CarrierPathFlightDebt {
        key: lead.observation.key,
        output_incarnation: lead.observation.incarnation,
        bytes: lead_flight,
    }];

    assert_eq!(
        select_response_data_path(
            &[lead.clone(), recovered.clone()],
            TrafficClass::Throughput,
            payload_bytes,
            mux_limits,
            &lower,
            lead_flight as usize,
        )
        .map(|selected| selected.observation.key),
        Some(recovered.observation.key),
        "fresh native completion evidence must admit real Product that refreshes the stale Product rate; stale Product evidence cannot create a circular no-work gate",
    );

    recovered.observation.snapshot.delivery_rate_bps = 351_000.0;
    recovered.observation.snapshot.pacing_rate_bps = 351_000.0;
    recovered.observation.snapshot.carrier_delivery_rate_bps = None;
    recovered.observation.has_bulk_rate_evidence = false;
    assert_eq!(
        select_response_data_path(
            &[lead, recovered.clone()],
            TrafficClass::Throughput,
            payload_bytes,
            mux_limits,
            &lower,
            lead_flight as usize,
        )
        .map(|selected| selected.observation.key),
        Some(recovered.observation.key),
        "even after the native rate sample expires, one bounded Product quantum completes before the lead's lower backlog and refreshes the recovered path without forced exploration",
    );
}

#[test]
fn mature_additional_tcp_output_keeps_fresh_product_completion_evidence() {
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
        20.0,
        startup_flight,
        16 * 1024 * 1024,
        false,
    );
    let native_rate_bps = 1_000_000.0;
    let product_rate_bps = 500_000_000.0;
    candidate.observation.snapshot.carrier_delivery_rate_bps = Some(native_rate_bps);
    candidate.observation.snapshot.delivery_rate_bps = product_rate_bps;
    candidate.observation.snapshot.product_progress_rate_bps = Some(product_rate_bps);
    candidate.observation.snapshot.has_durable_product_progress = true;
    candidate.observation.has_bulk_rate_evidence = true;
    let lower = [CarrierPathFlightDebt {
        key: owner.observation.key,
        output_incarnation: owner.observation.incarnation,
        bytes: 64 * 1024,
    }];
    let ordering_debt = (64 * 1024) + startup_flight as usize;

    let mut native_only = candidate.clone();
    native_only.observation.snapshot.delivery_rate_bps = native_rate_bps;
    native_only.observation.snapshot.product_progress_rate_bps = None;
    let native_selection = select_response_data_path(
        &[owner.clone(), native_only],
        TrafficClass::Throughput,
        64 * 1024,
        mux_limits,
        &lower,
        ordering_debt,
    )
    .expect("the lower-frontier owner remains schedulable");
    assert_eq!(
        native_selection.observation.key, owner.observation.key,
        "the candidate's qualified native TCP completion cannot beat the existing lower frontier",
    );

    let selected = select_response_data_path(
        &[owner.clone(), candidate.clone()],
        TrafficClass::Throughput,
        64 * 1024,
        mux_limits,
        &lower,
        ordering_debt,
    )
    .expect("fresh Product completion keeps the mature additional output schedulable");
    assert_eq!(
        selected.observation.key, candidate.observation.key,
        "REGRESSION: a transient Additional-output role discarded fresh exact-output Product completion evidence and stranded mature TCP acquisition",
    );
}

#[test]
fn product_only_completion_fallback_remains_eligible_on_an_additional_tcp_path() {
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
        20.0,
        startup_flight,
        16 * 1024 * 1024,
        false,
    );
    candidate.observation.snapshot.carrier_delivery_rate_bps = None;
    candidate.observation.snapshot.delivery_rate_bps = 500_000_000.0;
    candidate.observation.snapshot.rate_scope = PathRateScope::PerFlowGoodput;
    candidate.observation.snapshot.product_progress_rate_bps = Some(500_000_000.0);
    candidate.observation.snapshot.has_durable_product_progress = true;
    candidate.observation.has_bulk_rate_evidence = true;
    let lower = [CarrierPathFlightDebt {
        key: owner.observation.key,
        output_incarnation: owner.observation.incarnation,
        bytes: 64 * 1024,
    }];

    let selected = select_response_data_path(
        &[owner, candidate.clone()],
        TrafficClass::Throughput,
        64 * 1024,
        mux_limits,
        &lower,
        (64 * 1024) + startup_flight as usize,
    )
    .expect("the Product-only completion fallback remains schedulable");
    assert_eq!(
        selected.observation.key, candidate.observation.key,
        "a missing native observation must not disable the RFC Product fallback",
    );
}

#[test]
fn product_raised_tcp_floor_remains_effective_for_the_contiguous_frontier() {
    let mux_limits = MuxLimits::default();
    let startup_flight = reliable_unproven_path_startup_flight_limit_bytes(mux_limits);
    let mut owner = response_target(
        0,
        UnderlayProtocol::Tcp,
        20.0,
        64 * 1024,
        16 * 1024 * 1024,
        true,
    );
    owner.observation.snapshot.carrier_delivery_rate_bps = Some(1_000_000.0);
    owner.observation.snapshot.delivery_rate_bps = 500_000_000.0;
    owner.observation.snapshot.product_progress_rate_bps = Some(500_000_000.0);
    let mut candidate = response_target(
        1,
        UnderlayProtocol::Tcp,
        80.0,
        startup_flight,
        16 * 1024 * 1024,
        false,
    );
    candidate.observation.snapshot.carrier_delivery_rate_bps = Some(100_000_000.0);
    candidate.observation.snapshot.delivery_rate_bps = 100_000_000.0;
    candidate.observation.snapshot.product_progress_rate_bps = None;
    let lower = [CarrierPathFlightDebt {
        key: owner.observation.key,
        output_incarnation: owner.observation.incarnation,
        bytes: 64 * 1024,
    }];
    let ordering_debt = (64 * 1024) + startup_flight as usize;

    let mut native_only_owner = owner.clone();
    native_only_owner.observation.snapshot.delivery_rate_bps = 1_000_000.0;
    native_only_owner
        .observation
        .snapshot
        .product_progress_rate_bps = None;
    assert_eq!(
        select(
            &[native_only_owner, candidate.clone()],
            &lower,
            ordering_debt,
        ),
        Some(candidate.observation.key),
        "the faster candidate wins when the frontier has only its low native estimate",
    );
    assert_eq!(
        select(&[owner.clone(), candidate], &lower, ordering_debt),
        Some(owner.observation.key),
        "the current frontier retains its demonstrated Product completion floor",
    );
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
fn unqualified_fallback_rate_remains_completion_ranked_against_live_frontier() {
    let owner_flight = 16 * 1024 * 1024;
    let mut owner = response_target(
        0,
        UnderlayProtocol::Tcp,
        115.0,
        owner_flight,
        owner_flight,
        true,
    );
    owner.observation.snapshot.data_level_limit_bytes = owner_flight + 64 * 1024;

    let mut cold = response_target(1, UnderlayProtocol::Tcp, 333.0, 0, 16 * 1024 * 1024, false);
    cold.observation.snapshot.delivery_rate_bps = 351_000.0;
    cold.observation.snapshot.pacing_rate_bps = 351_000.0;
    cold.observation.snapshot.confidence = 1.0;
    cold.observation.snapshot.app_limited = false;
    cold.observation.snapshot.has_durable_product_progress = false;
    cold.observation.has_bulk_rate_evidence = false;

    let lower = [CarrierPathFlightDebt {
        key: owner.observation.key,
        output_incarnation: owner.observation.incarnation,
        bytes: owner_flight,
    }];

    assert_eq!(
        select(
            &[owner.clone(), cold.clone()],
            &lower,
            owner_flight as usize
        ),
        Some(owner.observation.key),
        "bounded acquisition is not priority over a lower-completion live frontier",
    );
}

#[test]
fn unmeasured_path_uses_completion_ranking_without_losing_bounded_liveness() {
    let mut measured = response_target(0, UnderlayProtocol::Tcp, 20.0, 0, 16 * 1024 * 1024, true);
    measured.observation.snapshot.product_progress_rate_bps = Some(500_000_000.0);
    let mut unmeasured =
        response_target(1, UnderlayProtocol::Tcp, 800.0, 0, 16 * 1024 * 1024, false);
    unmeasured.observation.has_bulk_rate_evidence = false;
    unmeasured.observation.snapshot.has_durable_product_progress = false;
    unmeasured.observation.snapshot.product_progress_rate_bps = None;
    unmeasured.observation.snapshot.delivery_rate_bps = 351_000.0;
    unmeasured.observation.snapshot.pacing_rate_bps = 351_000.0;
    let payload_bytes = 64 * 1024;

    assert_eq!(
        select(&[measured.clone(), unmeasured.clone()], &[], 0),
        Some(measured.observation.key),
        "lack of Product evidence does not override ordinary completion ranking"
    );

    let mut unavailable_measured = measured.clone();
    block_data_queue(&mut unavailable_measured);
    assert_eq!(
        select(&[unavailable_measured, unmeasured.clone()], &[], 0),
        Some(unmeasured.observation.key),
        "an unmeasured output remains a bounded liveness fallback when the qualified output cannot enqueue",
    );

    let mut faster_unmeasured = unmeasured.clone();
    faster_unmeasured.observation.snapshot.srtt_ms = 1.0;
    faster_unmeasured.observation.snapshot.delivery_rate_bps = 1_000_000_000.0;
    faster_unmeasured.observation.snapshot.pacing_rate_bps = 1_000_000_000.0;
    assert_eq!(
        select(&[measured.clone(), faster_unmeasured.clone()], &[], 0),
        Some(faster_unmeasured.observation.key),
        "an unmeasured output that genuinely has the lower modeled completion remains discoverable",
    );

    unmeasured.observation.original_data_in_flight_bytes = payload_bytes as u64;
    unmeasured.observation.snapshot.data_level_bytes_in_flight = payload_bytes as u64;
    assert_eq!(
        select(&[measured.clone(), unmeasured.clone()], &[], 0),
        Some(measured.observation.key),
        "the bounded startup cap does not grant another acquisition priority"
    );

    unmeasured.observation.original_data_in_flight_bytes = 0;
    unmeasured.observation.snapshot.data_level_bytes_in_flight = 0;
    unmeasured.observation.snapshot.product_progress_rate_bps = Some(351_000.0);
    assert_eq!(
        select(&[measured.clone(), unmeasured], &[], 0),
        Some(measured.observation.key),
        "after the first product sample, ordinary completion-time ranking resumes"
    );
}

#[test]
fn slow_unmeasured_acquisition_cannot_own_the_first_dsn_ahead_of_qualified_quic() {
    const OBJECT_BYTES: usize = 100_000;
    const FIRST_QUANTUM_BYTES: usize = 64 * 1024;

    let mut qualified_quic =
        response_target(0, UnderlayProtocol::Udp, 20.0, 0, 16 * 1024 * 1024, true);
    qualified_quic
        .observation
        .snapshot
        .product_progress_rate_bps = Some(500_000_000.0);

    // This is the exact post-idle/direction-switch state at issue: the TCP
    // attachment remains established but its directional Product evidence has
    // expired, so only the conservative startup prior remains.
    let mut unmeasured_tcp =
        response_target(1, UnderlayProtocol::Tcp, 800.0, 0, 16 * 1024 * 1024, false);
    unmeasured_tcp.observation.has_bulk_rate_evidence = false;
    unmeasured_tcp
        .observation
        .snapshot
        .has_durable_product_progress = false;
    unmeasured_tcp
        .observation
        .snapshot
        .product_progress_rate_bps = None;
    unmeasured_tcp.observation.snapshot.delivery_rate_bps = 351_000.0;
    unmeasured_tcp.observation.snapshot.pacing_rate_bps = 351_000.0;

    let selected = select_response_data_path(
        &[qualified_quic.clone(), unmeasured_tcp.clone()],
        TrafficClass::Throughput,
        FIRST_QUANTUM_BYTES,
        MuxLimits::default(),
        &[],
        0,
    )
    .expect("the qualified QUIC output is schedulable");

    // Committing this decision assigns offset zero to the selected exact
    // output. Any faster suffix then remains above this owner in Data-ACK order.
    let lower = [CarrierPathFlightDebt {
        key: selected.observation.key,
        output_incarnation: selected.observation.incarnation,
        bytes: FIRST_QUANTUM_BYTES as u64,
    }];
    assert_eq!(
        response_oldest_lower_flight_owner(&lower),
        Some((selected.observation.key, selected.observation.incarnation)),
    );

    let tcp_prefix_eta_ms = crate::scheduler::score_path(
        response_completion_snapshot(&unmeasured_tcp),
        TrafficClass::Throughput,
        FIRST_QUANTUM_BYTES,
    )
    .expect("unmeasured TCP startup prior is scoreable")
    .eta_ms;
    let qualified_only_object_eta_ms = crate::scheduler::score_path(
        response_completion_snapshot(&qualified_quic),
        TrafficClass::Throughput,
        OBJECT_BYTES,
    )
    .expect("qualified QUIC control is scoreable")
    .eta_ms;

    assert!(
        selected.observation.key != unmeasured_tcp.observation.key
            || tcp_prefix_eta_ms <= qualified_only_object_eta_ms,
        "unique offset-zero acquisition creates deterministic HOL: the unmeasured TCP prefix completes in {tcp_prefix_eta_ms:.3} ms, after the qualified-only QUIC control's whole 100 kB object in {qualified_only_object_eta_ms:.3} ms",
    );
    assert_eq!(
        selected.observation.key, qualified_quic.observation.key,
        "lack of directional evidence is an acquisition state, not authority to preempt a qualified lower-completion output",
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

    backup.observation.writer_pending_bytes = 1;
    let selected = select_response_frame_path(
        &[available.clone(), backup.clone()],
        TrafficClass::Throughput,
        &frame,
        CarrierEmitMode::StreamOrdered,
        &[(available.observation.key, available.observation.incarnation)],
        Some(RelaySendCause::AckGapReinjection),
    )
    .expect("sampled writer backlog does not revoke exact repair authority");
    assert_eq!(selected.observation.key, backup.observation.key);
    let selected = select_response_frame_path(
        &[available.clone(), backup.clone()],
        TrafficClass::Throughput,
        &frame,
        CarrierEmitMode::StreamOrdered,
        &[(available.observation.key, available.observation.incarnation)],
        Some(RelaySendCause::TailReinjection),
    )
    .expect("bounded tail recovery survives unrelated shared-carrier work");
    assert_eq!(selected.observation.key, backup.observation.key);
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
fn ordinary_reinjection_ranks_native_backlog_without_turning_it_into_authority() {
    let original = response_target(0, UnderlayProtocol::Udp, 20.0, 0, 16 * 1024 * 1024, true);
    let mut busy_tcp = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    busy_tcp.observation.snapshot.queue_bytes = 8 * 1024 * 1024;
    busy_tcp.observation.native_queue_bytes = 8 * 1024 * 1024;
    busy_tcp.observation.native_drain_observed = true;
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
        (idle_tcp.observation.key, idle_tcp.observation.incarnation),
        "large native backlog remains completion-time evidence",
    );

    let mut fast_busy_tcp = busy_tcp.clone();
    fast_busy_tcp.observation.snapshot.queue_bytes = 64 * 1024;
    fast_busy_tcp.observation.native_queue_bytes = 64 * 1024;
    let slow_idle_tcp =
        response_target(3, UnderlayProtocol::Tcp, 800.0, 0, 16 * 1024 * 1024, false);
    let selected = select_response_frame_path(
        &[fast_busy_tcp.clone(), slow_idle_tcp.clone()],
        TrafficClass::Throughput,
        &frame,
        CarrierEmitMode::StreamOrdered,
        &avoid_original,
        Some(RelaySendCause::AckGapReinjection),
    )
    .expect("drained TCP repair carrier");
    assert_eq!(
        selected.observation.key, fast_busy_tcp.observation.key,
        "the lower completion-time target wins without a sampled-idleness veto",
    );

    let selected = select_response_frame_path(
        std::slice::from_ref(&fast_busy_tcp),
        TrafficClass::Throughput,
        &frame,
        CarrierEmitMode::StreamOrdered,
        &avoid_original,
        Some(RelaySendCause::AckGapReinjection),
    )
    .expect("native backlog alone cannot veto the sole exact repair target");
    assert_eq!(selected.observation.key, fast_busy_tcp.observation.key);

    let selected = select_response_frame_path(
        std::slice::from_ref(&fast_busy_tcp),
        TrafficClass::Throughput,
        &frame,
        CarrierEmitMode::StreamOrdered,
        &avoid_original,
        Some(RelaySendCause::PersistentAckGapReinjection),
    )
    .expect("a measured earlier completion admits bounded busy-carrier repair");
    assert_eq!(selected.observation.key, fast_busy_tcp.observation.key);

    let bound = RelaySendCause::persistent_server_ack_gap_reinjection(
        crate::runtime::sender::ServerReinjectionOutputIdentity {
            key: fast_busy_tcp.observation.key,
            incarnation: fast_busy_tcp.observation.incarnation,
        },
        fast_busy_tcp.observation.snapshot,
    );
    let selected = select_response_frame_path(
        std::slice::from_ref(&fast_busy_tcp),
        TrafficClass::Throughput,
        &frame,
        CarrierEmitMode::StreamOrdered,
        &avoid_original,
        Some(bound),
    )
    .expect("the bound recovery target remains eligible until dispatch");
    assert_eq!(selected.observation.key, fast_busy_tcp.observation.key);
}

#[test]
fn ordinary_tcp_reinjection_does_not_require_sampled_native_sender_idleness() {
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

    let mut busy_tcp = busy_tcp;
    busy_tcp.observation.original_data_in_flight_bytes = 0;
    busy_tcp.observation.snapshot.data_level_bytes_in_flight = 0;
    busy_tcp.observation.native_drain_observed = true;
    busy_tcp.observation.native_queue_bytes = 1;
    busy_tcp.observation.writer_pending_bytes = 1;
    assert!(
        select_response_frame_path(
            &[busy_tcp],
            TrafficClass::Throughput,
            &frame,
            CarrierEmitMode::StreamOrdered,
            &[(original.observation.key, original.observation.incarnation)],
            Some(RelaySendCause::AckGapReinjection),
        )
        .is_some(),
        "sampled native flight, native queue, and writer backlog are ranking evidence, not repair authority",
    );
}

#[test]
fn portable_tcp_reinjection_waits_for_exact_product_flight() {
    let frame = Frame::StreamFin {
        stream_id: StreamId(9),
        final_offset: 1024,
    };

    for carrier_limit in [0, 16 * 1024 * 1024] {
        let mut tcp = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, carrier_limit, false);
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
            "without native drain evidence, exact product flight is the portable repair-readiness authority",
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
fn quic_reinjection_retains_product_authority_without_sampled_native_idleness_gate() {
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
    assert!(
        select(&quic).is_some(),
        "sampled native queue occupancy cannot veto exact repair authority",
    );
    quic.observation.native_queue_bytes = 0;
    quic.observation.writer_pending_bytes = 1;
    assert!(
        select(&quic).is_some(),
        "sampled writer backlog cannot veto exact repair authority",
    );
    quic.observation.writer_pending_bytes = 0;
    assert!(
        select(&quic).is_some(),
        "QUIC repair remains eligible after sampled writer backlog changes",
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

    let mut busy_unproven = unproven.clone();
    busy_unproven.observation.writer_pending_bytes = 1;
    busy_unproven.observation.native_queue_bytes = 1;
    let selected = select_response_frame_path(
        &[busy_unproven.clone()],
        TrafficClass::Throughput,
        &frame,
        CarrierEmitMode::StreamOrdered,
        &[],
        Some(RelaySendCause::PathFailureReinjection),
    )
    .expect("live unmeasured fallback");
    assert_eq!(selected.observation.key, busy_unproven.observation.key);
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
fn exact_lower_flight_owner_continues_within_measured_hysteresis() {
    let mut owner = response_target(
        0,
        UnderlayProtocol::Udp,
        10.0,
        64 * 1024,
        16 * 1024 * 1024,
        true,
    );
    owner.observation.snapshot.jitter_ms = 3.0;
    let mut challenger = response_target(1, UnderlayProtocol::Tcp, 9.0, 0, 16 * 1024 * 1024, false);
    challenger.observation.snapshot.jitter_ms = 3.0;
    let lower = [CarrierPathFlightDebt {
        key: owner.observation.key,
        output_incarnation: owner.observation.incarnation,
        bytes: 64 * 1024,
    }];

    let selected = select_response_data_path(
        &[owner.clone(), challenger],
        TrafficClass::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &lower,
        64 * 1024,
    )
    .expect("both qualified carriers remain schedulable");
    assert_eq!(
        selected.observation.key, owner.observation.key,
        "the exact lower-flight owner must not flap on a sub-jitter completion difference",
    );
}

#[test]
fn response_owner_hysteresis_does_not_activate_unused_quic_credit() {
    let mut owner = response_target(
        0,
        UnderlayProtocol::Tcp,
        100.0,
        64 * 1024,
        16 * 1024 * 1024,
        true,
    );
    owner.observation.snapshot.jitter_ms = 3.0;
    let mut underfed = response_target(1, UnderlayProtocol::Udp, 103.0, 0, 16 * 1024 * 1024, false);
    underfed.observation.snapshot.jitter_ms = 3.0;
    underfed.observation.snapshot.app_limited = true;
    let lower = [CarrierPathFlightDebt {
        key: owner.observation.key,
        output_incarnation: owner.observation.incarnation,
        bytes: 64 * 1024,
    }];

    let owner_eta = scheduler::score_path(
        owner.observation.snapshot,
        TrafficClass::Throughput,
        64 * 1024,
    )
    .expect("owner score")
    .eta_ms;
    let underfed_eta = scheduler::score_path(
        underfed.observation.snapshot,
        TrafficClass::Throughput,
        64 * 1024,
    )
    .expect("underfed score")
    .eta_ms;
    assert!(
        underfed_eta > owner_eta
            && underfed_eta <= owner_eta + owner.observation.snapshot.jitter_ms,
        "the fixture must keep acquisition inside measured completion uncertainty",
    );
    assert_eq!(
        select(&[owner.clone(), underfed.clone()], &lower, 64 * 1024),
        Some(owner.observation.key),
        "response placement must not activate a path merely because its QUIC controller is app-limited",
    );
    assert_eq!(
        select_response_data_path(
            &[owner.clone(), underfed.clone()],
            TrafficClass::Latency,
            64 * 1024,
            MuxLimits::default(),
            &lower,
            64 * 1024,
        )
        .map(|target| target.observation.key),
        Some(owner.observation.key),
        "QUIC-window acquisition must not change latency placement",
    );
}

#[test]
fn tcp_delivery_sample_app_limited_flag_does_not_preempt_response_owner() {
    let mut owner = response_target(
        0,
        UnderlayProtocol::Udp,
        100.0,
        64 * 1024,
        16 * 1024 * 1024,
        true,
    );
    owner.observation.snapshot.jitter_ms = 3.0;
    let mut sampled_tcp =
        response_target(1, UnderlayProtocol::Tcp, 103.0, 0, 16 * 1024 * 1024, false);
    sampled_tcp.observation.snapshot.jitter_ms = 3.0;
    sampled_tcp.observation.snapshot.app_limited = true;
    let lower = [CarrierPathFlightDebt {
        key: owner.observation.key,
        output_incarnation: owner.observation.incarnation,
        bytes: 64 * 1024,
    }];

    assert_eq!(
        select(&[owner.clone(), sampled_tcp], &lower, 64 * 1024),
        Some(owner.observation.key),
        "TCP_INFO delivery-sample classification cannot override lower-owner hysteresis",
    );
}

#[test]
fn materially_slower_underfed_native_credit_cannot_preempt_the_live_frontier() {
    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let owner_underlay = match underlay {
            UnderlayProtocol::Tcp => UnderlayProtocol::Udp,
            UnderlayProtocol::Udp => UnderlayProtocol::Tcp,
        };
        let mut owner = response_target(0, owner_underlay, 80.0, 64 * 1024, 16 * 1024 * 1024, true);
        owner.observation.snapshot.jitter_ms = 20.0;
        owner.observation.snapshot.delivery_rate_bps = 555_000_000.0;
        owner.observation.snapshot.pacing_rate_bps = 555_000_000.0;

        let mut underfed = response_target(1, underlay, 100.0, 0, 16 * 1024 * 1024, false);
        underfed.observation.snapshot.jitter_ms = 20.0;
        underfed.observation.snapshot.delivery_rate_bps = 351_000.0;
        underfed.observation.snapshot.pacing_rate_bps = 351_000.0;
        underfed.observation.snapshot.app_limited = true;
        underfed.observation.has_bulk_rate_evidence = false;
        let lower = [CarrierPathFlightDebt {
            key: owner.observation.key,
            output_incarnation: owner.observation.incarnation,
            bytes: 4 * 1024 * 1024,
        }];

        let owner_eta = scheduler::score_path(
            owner.observation.snapshot,
            TrafficClass::Throughput,
            64 * 1024,
        )
        .expect("owner score")
        .eta_ms;
        let underfed_eta = scheduler::score_path(
            underfed.observation.snapshot,
            TrafficClass::Throughput,
            64 * 1024,
        )
        .expect("underfed score")
        .eta_ms;
        assert!(
            underfed_eta > owner_eta + owner.observation.snapshot.jitter_ms,
            "the fixture must reproduce a materially later underfed carrier: owner={owner_eta:.3} ms underfed={underfed_eta:.3} ms",
        );
        assert_eq!(
            select(&[owner.clone(), underfed], &lower, 4 * 1024 * 1024),
            Some(owner.observation.key),
            "current native starvation is acquisition evidence, not authority to put a materially later {underlay:?} range below the live frontier",
        );
    }
}

#[test]
fn material_completion_gain_preempts_lower_flight_owner_hysteresis() {
    let mut owner = response_target(
        0,
        UnderlayProtocol::Udp,
        40.0,
        64 * 1024,
        16 * 1024 * 1024,
        true,
    );
    owner.observation.snapshot.jitter_ms = 3.0;
    let mut challenger = response_target(1, UnderlayProtocol::Tcp, 9.0, 0, 16 * 1024 * 1024, false);
    challenger.observation.snapshot.jitter_ms = 3.0;
    let lower = [CarrierPathFlightDebt {
        key: owner.observation.key,
        output_incarnation: owner.observation.incarnation,
        bytes: 64 * 1024,
    }];

    assert_eq!(
        select(&[owner, challenger.clone()], &lower, 64 * 1024),
        Some(challenger.observation.key),
        "measured hysteresis must not preserve a materially slower frontier owner",
    );
}

#[test]
fn queue_growth_beyond_one_quantum_preempts_lower_flight_owner_hysteresis() {
    let mut owner = response_target(
        0,
        UnderlayProtocol::Udp,
        10.0,
        64 * 1024,
        16 * 1024 * 1024,
        true,
    );
    owner.observation.snapshot.jitter_ms = 100.0;
    owner.observation.snapshot.queue_bytes = 2 * 64 * 1024;
    let mut challenger = response_target(1, UnderlayProtocol::Tcp, 9.0, 0, 16 * 1024 * 1024, false);
    challenger.observation.snapshot.jitter_ms = 100.0;
    let lower = [CarrierPathFlightDebt {
        key: owner.observation.key,
        output_incarnation: owner.observation.incarnation,
        bytes: 64 * 1024,
    }];

    assert_eq!(
        select(&[owner, challenger.clone()], &lower, 64 * 1024),
        Some(challenger.observation.key),
        "hysteresis must not hide more than one scheduling quantum of queue growth",
    );
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
