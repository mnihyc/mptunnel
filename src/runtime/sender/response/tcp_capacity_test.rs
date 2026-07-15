use super::super::admission::{
    response_target_assigned_product_bytes, response_target_unique_owner_admission,
};
use super::super::planner::select_response_sender_data_target_with_ordered_debt_and_epoch;
use super::*;
use crate::model::ack_clock::{
    reliable_ack_clock_calibration_ceiling_bytes, reliable_ack_clock_calibration_limit_bytes,
};
use crate::model::capacity::reliable_bulk_carrier_feed_quantum_bytes;
use crate::model::multipath::{PathAdmissionDecision, PathRuntimeRole};
use crate::model::response::ResponseBulkLead;
use crate::protocol::{Frame, StreamFlags, StreamId};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::sender::response::test_support::response_target;
use crate::scheduler::PathRateScope;
use bytes::Bytes;

#[test]
fn tcp_capacity_probe_does_not_wait_for_product_subflow_graduation() {
    let mux_limits = MuxLimits::default();
    let mut service = response_target(0, UnderlayProtocol::Tcp, 20.0, 0, 64 * 1024, true);
    let mut cold = response_target(1, UnderlayProtocol::Tcp, 80.0, 0, 64 * 1024, false);
    service.observation.has_bulk_rate_evidence = false;
    cold.observation.has_bulk_rate_evidence = false;
    let (cold_commands, _cold_receivers) = reliable_path_command_channels(4);
    cold.commands = cold_commands;

    assert!(
        select_response_tcp_capacity_probe_target(
            &[service.clone(), cold.clone()],
            FlowLane::Throughput,
            Some(service.observation.key),
            ResponseServiceFamilyLoads::default(),
            mux_limits,
        )
        .is_none()
    );

    service.observation.has_bulk_rate_evidence = true;
    let (selected, train_bytes) = select_response_tcp_capacity_probe_target(
        &[service.clone(), cold.clone()],
        FlowLane::Throughput,
        Some(service.observation.key),
        ResponseServiceFamilyLoads::default(),
        mux_limits,
    )
    .expect("proven Service opens offset-free discovery");
    assert_eq!(selected.observation.key, cold.observation.key);
    assert_eq!(train_bytes, 2 * 1024 * 1024);

    let udp = response_target(2, UnderlayProtocol::Udp, 10.0, 0, 64 * 1024, false);
    assert!(
        select_response_tcp_capacity_probe_target(
            &[service.clone(), cold, udp],
            FlowLane::Throughput,
            Some(service.observation.key),
            ResponseServiceFamilyLoads::new(2, 0),
            mux_limits,
        )
        .is_none(),
        "a measured cross-family handoff must outrank optional TCP discovery"
    );
}

#[test]
fn endpoint_only_response_calibration_uses_service_only_as_opportunity_prior() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 600.0, 0, 16 * 1024 * 1024, true);
    service.observation.snapshot.delivery_rate_bps = 128_000_000.0;
    service.observation.snapshot.pacing_rate_bps = 128_000_000.0;
    service.observation.snapshot.srtt_ms = 333.0;
    service.observation.snapshot.min_rtt_ms = 333.0;

    let mut candidate =
        response_target(1, UnderlayProtocol::Tcp, 570.0, 0, 16 * 1024 * 1024, false);
    candidate.observation.snapshot.delivery_rate_bps = 2_500_000.0;
    candidate.observation.snapshot.pacing_rate_bps = 2_500_000.0;
    candidate.observation.snapshot.srtt_ms = 722.0;
    candidate.observation.snapshot.min_rtt_ms = 722.0;
    candidate.observation.snapshot.app_limited = true;
    candidate.endpoint_only_service_prior_eligible = true;
    candidate.ack_clock_calibration_eligible = true;
    candidate.ack_clock_calibration_credit_limit_bytes = 452_124;
    candidate.ack_clock_calibration_max_limit_bytes = 64 * 1024 * 1024;

    let (effective, effective_eta_ms, borrowed) = response_tcp_calibration_opportunity_candidate(
        &service,
        &candidate,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );
    assert!(borrowed);
    assert_eq!(
        effective.delivery_rate_bps,
        service.observation.snapshot.delivery_rate_bps
    );
    assert_eq!(effective.rate_scope, PathRateScope::PathCapacity);
    assert!(effective_eta_ms < candidate.observation.eta_ms);
    assert_eq!(
        candidate.observation.snapshot.delivery_rate_bps,
        2_500_000.0
    );

    let all_targets = [service.clone(), candidate.clone()];
    let targets = all_targets.iter().collect::<Vec<_>>();
    let selected = select_response_ack_clock_calibration_target(
        &all_targets,
        &targets,
        FlowLane::Throughput,
        service.observation.key,
        0,
        payload_bytes,
        mux_limits,
        &[],
        None,
        true,
        &mut Vec::new(),
    )
    .expect("the endpoint-only candidate should receive bounded calibration work");
    assert_eq!(selected.target.observation.key, candidate.observation.key);
    assert_eq!(
        (
            selected.selection.limit_bytes,
            selected.selection.requires_active_response_start,
        ),
        (candidate.ack_clock_calibration_credit_limit_bytes, true),
    );

    let feed_reservoir = bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits);
    let mut calibrating = candidate.clone();
    calibrating.ack_clock_calibration_active = true;
    calibrating.ack_clock_calibration_spent_bytes =
        calibrating.ack_clock_calibration_credit_limit_bytes;
    let calibration_reservoir = feed_reservoir
        + usize::try_from(calibrating.ack_clock_calibration_credit_limit_bytes).unwrap();
    assert!(response_calibration_service_reservoir_has_credit(
        calibration_reservoir - payload_bytes,
        calibrating.ack_clock_calibration_credit_limit_bytes,
        payload_bytes,
        mux_limits,
    ));
    assert!(
        !response_calibration_service_reservoir_has_credit(
            calibration_reservoir,
            calibrating.ack_clock_calibration_credit_limit_bytes,
            payload_bytes,
            mux_limits,
        ),
        "Service must wait when calibration flight and its projected follow-up fill the reservoir"
    );

    candidate.endpoint_only_service_prior_eligible = false;
    let (configured, configured_eta_ms, configured_borrowed) =
        response_tcp_calibration_opportunity_candidate(
            &service,
            &candidate,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        );
    assert_eq!(
        (
            configured.delivery_rate_bps,
            configured_eta_ms,
            configured_borrowed,
        ),
        (
            candidate.observation.snapshot.delivery_rate_bps,
            candidate.observation.eta_ms,
            false,
        ),
    );
    let configured_targets = [service.clone(), candidate];
    let configured_refs = configured_targets.iter().collect::<Vec<_>>();
    assert!(
        select_response_ack_clock_calibration_target(
            &configured_targets,
            &configured_refs,
            FlowLane::Throughput,
            service.observation.key,
            0,
            payload_bytes,
            mux_limits,
            &[],
            None,
            true,
            &mut Vec::new(),
        )
        .is_none(),
        "configured candidate rejection must leave calibration ownership untouched"
    );
}

#[test]
fn fresh_tcp_calibration_is_dormant_without_active_response_demand() {
    let mut candidate =
        response_target(1, UnderlayProtocol::Tcp, 500.0, 0, 16 * 1024 * 1024, false);
    let (commands, _receivers) = reliable_path_command_channels(8);
    candidate.commands = commands;
    candidate.ack_clock_calibration_eligible = true;
    candidate.ack_clock_calibration_credit_limit_bytes = 256 * 1024;
    candidate.ack_clock_calibration_max_limit_bytes = 64 * 1024 * 1024;

    assert!(response_ack_clock_calibration_pending(&candidate, true));
    assert!(!response_ack_clock_calibration_pending(&candidate, false));
    assert!(response_ack_clock_calibration_blocks_generic_owner(
        &candidate
    ));
}

#[test]
fn tcp_ack_clock_calibration_rejects_seed_beyond_service_reservoir() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
    let mut candidate = response_target(
        1,
        UnderlayProtocol::Tcp,
        1_500.0,
        0,
        16 * 1024 * 1024,
        false,
    );
    candidate.observation.snapshot.delivery_rate_bps = 2_000_000.0;
    candidate.observation.snapshot.product_progress_rate_bps = Some(2_000_000.0);
    candidate.observation.snapshot.app_limited = true;
    candidate.ack_clock_calibration_eligible = true;
    let initial_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
    candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
    candidate.ack_clock_calibration_max_limit_bytes = initial_limit.saturating_mul(4);
    let candidates = [&service, &candidate];
    let lead = ResponseBulkLead {
        key: service.observation.key,
        snapshot: service.observation.snapshot,
        eta_ms: service.observation.eta_ms,
    };
    assert_eq!(
        response_target_unique_owner_admission(
            &candidate,
            &candidates,
            lead,
            None,
            0,
            payload_bytes,
            mux_limits,
        )
        .decision,
        PathAdmissionDecision::Standby,
        "the provisional first-RTT rate remains too slow for ordinary ECF admission"
    );

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), candidate.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        0,
        None,
    )
    .expect("Service remains available when exploration would create an ordering stall");
    assert_eq!(selected.target().observation.key, service.observation.key);
    assert!(selected.ack_clock_calibration_selection().is_none());
}

#[test]
fn tcp_ack_clock_calibration_explores_within_service_reservoir() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(
        0,
        UnderlayProtocol::Tcp,
        1_098.657,
        0,
        16 * 1024 * 1024,
        true,
    );
    service.observation.snapshot.delivery_rate_bps = 18_561_000.0;
    service.observation.snapshot.pacing_rate_bps = 18_561_000.0;
    service.observation.snapshot.srtt_ms = 333.0;
    service.observation.snapshot.min_rtt_ms = 333.0;

    let mut candidate = response_target(
        1,
        UnderlayProtocol::Tcp,
        1_406.704,
        0,
        16 * 1024 * 1024,
        false,
    );
    candidate.observation.snapshot.delivery_rate_bps = 1_007_000.0;
    candidate.observation.snapshot.pacing_rate_bps = 1_007_000.0;
    candidate.observation.snapshot.product_progress_rate_bps = Some(1_007_000.0);
    candidate.observation.snapshot.srtt_ms = 730.287;
    candidate.observation.snapshot.min_rtt_ms = 730.287;
    candidate.observation.snapshot.app_limited = true;
    candidate.ack_clock_calibration_eligible = true;
    let initial_limit = 183_802;
    candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
    candidate.ack_clock_calibration_max_limit_bytes = 64 * 1024 * 1024;
    let candidates = [&service, &candidate];
    let lead = ResponseBulkLead {
        key: service.observation.key,
        snapshot: service.observation.snapshot,
        eta_ms: service.observation.eta_ms,
    };
    assert_eq!(
        response_target_unique_owner_admission(
            &candidate,
            &candidates,
            lead,
            None,
            0,
            payload_bytes,
            mux_limits,
        )
        .decision,
        PathAdmissionDecision::Standby,
        "the provisional model still cannot claim ordinary ownership"
    );

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), candidate.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        0,
        None,
    )
    .expect("bounded exploration should fit behind the Service reservoir");
    assert_eq!(selected.target().observation.key, candidate.observation.key);
    assert_eq!(selected.admission().role, PathRuntimeRole::Subflow);
    assert!(selected.ack_clock_calibration_selection().is_some());

    candidate.ack_clock_calibration_active = true;
    candidate.ack_clock_calibration_spent_bytes = initial_limit;
    candidate.ack_clock_calibration_credit_limit_bytes = initial_limit.saturating_mul(2);
    let grown = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), candidate.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        0,
        None,
    )
    .expect("a causally authorized stage continues calibration");
    assert_eq!(grown.target().observation.key, candidate.observation.key);
    assert_eq!(
        grown
            .ack_clock_calibration_selection()
            .expect("staged calibration commit")
            .limit_bytes,
        initial_limit.saturating_mul(2)
    );

    candidate.ack_clock_calibration_spent_bytes = initial_limit.saturating_mul(2);
    let awaiting_evidence = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), candidate],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        0,
        None,
    )
    .expect("a stage awaiting new ACK evidence returns to Service");
    assert_eq!(
        awaiting_evidence.target().observation.key,
        service.observation.key
    );
    assert!(
        awaiting_evidence
            .ack_clock_calibration_selection()
            .is_none()
    );
}

#[test]
fn safe_tcp_calibration_waits_for_repair_carrier_headroom() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(0, UnderlayProtocol::Tcp, 5_000.0, 0, 16 * 1024 * 1024, true);
    let mut candidate =
        response_target(1, UnderlayProtocol::Tcp, 100.0, 0, 16 * 1024 * 1024, false);
    candidate.ack_clock_calibration_eligible = true;
    candidate.ack_clock_calibration_credit_limit_bytes = 256 * 1024;
    candidate.ack_clock_calibration_max_limit_bytes = 64 * 1024 * 1024;
    candidate.observation.snapshot.product_bytes_in_flight = 256 * 1024;
    candidate.observation.owner_data_in_flight_bytes = 0;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), candidate],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        0,
        None,
    )
    .expect("Service remains available while RepairData occupies candidate headroom");

    assert_eq!(selected.target().observation.key, service.observation.key);
    assert!(selected.ack_clock_calibration_selection().is_none());
}

#[test]
fn tcp_response_calibration_does_not_double_count_pending_owner_flight() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
    let mut candidate =
        response_target(1, UnderlayProtocol::Tcp, 500.0, 0, 16 * 1024 * 1024, false);
    let (commands, _receivers) = reliable_path_command_channels(8);
    candidate.commands = commands;
    let initial_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
    let committed = initial_limit - payload_bytes as u64;
    candidate.observation.snapshot.product_bytes_in_flight = committed;
    candidate.ack_clock_calibration_eligible = true;
    candidate.ack_clock_calibration_active = true;
    candidate.ack_clock_calibration_spent_bytes = committed;
    candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
    candidate.ack_clock_calibration_max_limit_bytes =
        reliable_ack_clock_calibration_ceiling_bytes(mux_limits);
    candidate
        .commands
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: StreamId(991),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![0x5a; committed as usize]),
            },
            FlowLane::Throughput,
        )
        .expect("mirror the product flight in the carrier queue");
    assert_eq!(candidate.commands.pending_bytes(), committed);
    assert_eq!(
        response_target_assigned_product_bytes(&candidate),
        committed
    );
    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), candidate.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        0,
        None,
    )
    .expect("overlapping flight and queue views count as one debt");

    assert_eq!(selected.target().observation.key, candidate.observation.key);
    assert_eq!(selected.admission().role, PathRuntimeRole::Subflow);
    assert_eq!(
        selected
            .ack_clock_calibration_selection()
            .expect("calibration commit")
            .limit_bytes,
        initial_limit
    );
}

#[test]
fn tcp_response_calibration_does_not_double_count_global_ordered_tail() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 64 * 1024 * 1024, true);
    let mut candidate =
        response_target(1, UnderlayProtocol::Tcp, 500.0, 0, 64 * 1024 * 1024, false);
    let ceiling = reliable_ack_clock_calibration_ceiling_bytes(mux_limits);
    let committed = ceiling - payload_bytes as u64;
    candidate.observation.snapshot.product_bytes_in_flight = committed;
    candidate.ack_clock_calibration_eligible = true;
    candidate.ack_clock_calibration_active = true;
    candidate.ack_clock_calibration_spent_bytes = committed;
    candidate.ack_clock_calibration_credit_limit_bytes = ceiling;
    candidate.ack_clock_calibration_max_limit_bytes = ceiling;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), candidate.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        committed as usize,
        None,
    )
    .expect("the global tail and candidate flight are the same product debt");

    assert_eq!(selected.target().observation.key, candidate.observation.key);
    assert_eq!(
        selected
            .ack_clock_calibration_selection()
            .expect("calibration commit")
            .limit_bytes,
        ceiling
    );
}

#[test]
fn blocked_active_ack_clock_candidate_does_not_select_another_calibration_owner() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
    let initial_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);

    let mut active_candidate =
        response_target(1, UnderlayProtocol::Tcp, 500.0, 0, 16 * 1024 * 1024, false);
    let (blocked_commands, _blocked_receivers) = reliable_path_command_channels(1);
    blocked_commands
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: StreamId(901),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"x"),
            },
            FlowLane::Throughput,
        )
        .expect("fill active calibration candidate queue");
    active_candidate.commands = blocked_commands;
    active_candidate.ack_clock_calibration_eligible = true;
    active_candidate.ack_clock_calibration_active = true;
    active_candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
    active_candidate.ack_clock_calibration_max_limit_bytes = initial_limit.saturating_mul(2);

    let mut other_candidate = response_target(
        2,
        UnderlayProtocol::Tcp,
        1_500.0,
        0,
        16 * 1024 * 1024,
        false,
    );
    other_candidate.observation.snapshot.delivery_rate_bps = 2_000_000.0;
    other_candidate
        .observation
        .snapshot
        .product_progress_rate_bps = Some(2_000_000.0);
    other_candidate.observation.snapshot.app_limited = true;
    other_candidate.ack_clock_calibration_eligible = true;
    other_candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
    other_candidate.ack_clock_calibration_max_limit_bytes = initial_limit.saturating_mul(2);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), active_candidate, other_candidate],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        0,
        None,
    )
    .expect("Service remains feedable while the active calibration path is blocked");
    assert_eq!(selected.target().observation.key, service.observation.key);
    assert!(selected.ack_clock_calibration_selection().is_none());
}

#[test]
fn exhausted_active_calibration_cannot_bypass_saturated_service_via_generic_subflow() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
    let (blocked_service_commands, _blocked_service_receivers) = reliable_path_command_channels(1);
    blocked_service_commands
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: StreamId(902),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"x"),
            },
            FlowLane::Throughput,
        )
        .expect("fill Service queue");
    service.commands = blocked_service_commands;

    let initial_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
    let mut candidate = response_target(1, UnderlayProtocol::Tcp, 1.0, 0, 16 * 1024 * 1024, false);
    candidate.ack_clock_calibration_eligible = true;
    candidate.ack_clock_calibration_active = true;
    candidate.ack_clock_calibration_spent_bytes = initial_limit;
    candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
    candidate.ack_clock_calibration_max_limit_bytes = initial_limit.saturating_mul(2);

    assert!(
        select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), candidate],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.observation.key),
            0,
            None,
        )
        .is_none(),
        "generic Subflow selection must not bypass staged credit while Service is blocked"
    );
}

#[test]
fn proven_active_calibration_cannot_reenter_generic_ownership_before_drain() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
    let (blocked_service_commands, _blocked_service_receivers) = reliable_path_command_channels(1);
    blocked_service_commands
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: StreamId(903),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"x"),
            },
            FlowLane::Throughput,
        )
        .expect("fill Service queue");
    service.commands = blocked_service_commands;

    let initial_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
    let mut candidate = response_target(1, UnderlayProtocol::Tcp, 1.0, 0, 16 * 1024 * 1024, false);
    candidate.ack_clock_calibration_eligible = true;
    candidate.ack_clock_calibration_active = true;
    candidate.ack_clock_calibration_proven = true;
    candidate.ack_clock_calibration_spent_bytes = initial_limit;
    candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
    candidate.ack_clock_calibration_max_limit_bytes = initial_limit;

    assert!(response_ack_clock_calibration_blocks_generic_owner(
        &candidate
    ));
    assert!(
        select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), candidate.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.observation.key),
            0,
            None,
        )
        .is_none(),
        "the exact active fence must drain before proven capacity becomes ordinary ownership"
    );

    candidate.ack_clock_calibration_active = false;
    assert!(!response_ack_clock_calibration_blocks_generic_owner(
        &candidate
    ));
}

#[test]
fn closed_active_calibration_drain_fence_blocks_next_startup_owner() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    service.commands = service_commands;

    let initial_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
    let mut draining = response_target(1, UnderlayProtocol::Tcp, 500.0, 0, 16 * 1024 * 1024, false);
    let (closed_commands, closed_receivers) = reliable_path_command_channels(8);
    drop(closed_receivers);
    draining.commands = closed_commands;
    draining.ack_clock_calibration_eligible = true;
    draining.ack_clock_calibration_active = true;
    draining.ack_clock_calibration_spent_bytes = initial_limit;
    draining.ack_clock_calibration_credit_limit_bytes = initial_limit;
    draining.ack_clock_calibration_max_limit_bytes = initial_limit.saturating_mul(2);
    assert!(draining.commands.is_closed());

    let mut next_startup =
        response_target(2, UnderlayProtocol::Tcp, 1.0, 0, 16 * 1024 * 1024, false);
    let (startup_commands, _startup_receivers) = reliable_path_command_channels(8);
    next_startup.commands = startup_commands;
    next_startup.observation.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), draining, next_startup],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        0,
        None,
    )
    .expect("Service remains available during exact-flight drain");
    assert_eq!(selected.target().observation.key, service.observation.key);
    assert!(selected.subflow_admission_selection().is_none());
}
