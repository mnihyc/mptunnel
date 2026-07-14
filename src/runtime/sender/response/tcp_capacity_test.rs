use super::*;
use crate::model::capacity::reliable_bulk_carrier_feed_quantum_bytes;
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::sender::response::test_support::response_target;
use crate::scheduler::PathRateScope;

#[test]
fn tcp_capacity_probe_does_not_wait_for_product_subflow_graduation() {
    let mux_limits = MuxLimits::default();
    let mut service = response_target(0, UnderlayProtocol::Tcp, 20.0, 0, 64 * 1024, true);
    let mut cold = response_target(1, UnderlayProtocol::Tcp, 80.0, 0, 64 * 1024, false);
    service.has_bulk_rate_evidence = false;
    cold.has_bulk_rate_evidence = false;
    let (cold_commands, _cold_receivers) = reliable_path_command_channels(4);
    cold.commands = cold_commands;

    assert!(
        select_response_tcp_capacity_probe_target(
            &[service.clone(), cold.clone()],
            FlowLane::Throughput,
            Some(service.key),
            ResponseServiceFamilyLoads::default(),
            mux_limits,
        )
        .is_none()
    );

    service.has_bulk_rate_evidence = true;
    let (selected, train_bytes) = select_response_tcp_capacity_probe_target(
        &[service.clone(), cold.clone()],
        FlowLane::Throughput,
        Some(service.key),
        ResponseServiceFamilyLoads::default(),
        mux_limits,
    )
    .expect("proven Service opens offset-free discovery");
    assert_eq!(selected.key, cold.key);
    assert_eq!(train_bytes, 2 * 1024 * 1024);

    let udp = response_target(2, UnderlayProtocol::Udp, 10.0, 0, 64 * 1024, false);
    assert!(
        select_response_tcp_capacity_probe_target(
            &[service.clone(), cold, udp],
            FlowLane::Throughput,
            Some(service.key),
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
    service.snapshot.delivery_rate_bps = 128_000_000.0;
    service.snapshot.pacing_rate_bps = 128_000_000.0;
    service.snapshot.srtt_ms = 333.0;
    service.snapshot.min_rtt_ms = 333.0;

    let mut candidate =
        response_target(1, UnderlayProtocol::Tcp, 570.0, 0, 16 * 1024 * 1024, false);
    candidate.snapshot.delivery_rate_bps = 2_500_000.0;
    candidate.snapshot.pacing_rate_bps = 2_500_000.0;
    candidate.snapshot.srtt_ms = 722.0;
    candidate.snapshot.min_rtt_ms = 722.0;
    candidate.snapshot.app_limited = true;
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
        service.snapshot.delivery_rate_bps
    );
    assert_eq!(effective.rate_scope, PathRateScope::PathCapacity);
    assert!(effective_eta_ms < candidate.eta_ms);
    assert_eq!(candidate.snapshot.delivery_rate_bps, 2_500_000.0);

    let all_targets = [service.clone(), candidate.clone()];
    let targets = all_targets.iter().collect::<Vec<_>>();
    let selected = select_response_ack_clock_calibration_target(
        &all_targets,
        &targets,
        FlowLane::Throughput,
        service.key,
        0,
        payload_bytes,
        mux_limits,
        &[],
        None,
        true,
        &mut Vec::new(),
    )
    .expect("the endpoint-only candidate should receive bounded calibration work");
    assert_eq!(selected.target.key, candidate.key);
    assert_eq!(
        (
            selected.commit.limit_bytes,
            selected.commit.requires_active_response_start,
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
            candidate.snapshot.delivery_rate_bps,
            candidate.eta_ms,
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
            service.key,
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
