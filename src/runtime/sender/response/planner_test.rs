use super::super::admission::{
    response_service_emission_credit_bytes, response_service_startup_emission_credit_bytes,
};
use super::*;
use crate::model::ack_clock::reliable_ack_clock_calibration_limit_bytes;
use crate::model::admission::{
    bulk_active_service_product_envelope_bytes, bulk_latency_pressure_service_feed_window_bytes,
    bulk_service_feed_reservoir_payload_bytes, bulk_service_horizon_payload_bytes,
    bulk_service_product_envelope_payload_bytes,
};
use crate::model::capacity::QuicCapacityProofCandidate;
use crate::model::response::{
    CarrierPathFlightDebt, ResponseOrderedTail, ResponseSameFamilyReservoir,
    ResponseServiceFamilyLoads, ResponseServiceHandoffMode,
};
use crate::protocol::frame::reliable_stream_frame_accounted_bytes;
use crate::runtime::sender::response::test_support::{
    observe_response_target_commands, response_target,
};
use crate::runtime::stream::response::{
    ResponseAckClockCalibrationRetirementRequest, ResponseDispatchTarget, ResponseSenderPathTarget,
    ResponseServiceHandoffDrainReservation, ResponseStreamAttachOutcome, ResponseStreamBinding,
    ServerPathLaneTracker, ServerPathMetricsSource, next_server_carrier_path_instance_id,
};
use crate::scheduler::{PathRateScope, PathSnapshot};

#[test]
fn repair_target_requires_active_or_bulk_rate_evidence() {
    let mut proof_only = response_target(1, UnderlayProtocol::Udp, 25.0, 0, 1_000_000, false);
    proof_only.observation.has_sender_evidence = true;
    proof_only.observation.has_bulk_rate_evidence = false;
    let mut unevidenced = response_target(2, UnderlayProtocol::Tcp, 5.0, 0, 1_000_000, false);
    unevidenced.observation.has_sender_evidence = false;
    unevidenced.observation.has_bulk_rate_evidence = false;

    assert!(
        choose_response_repair_target(
            &[proof_only, unevidenced],
            &[],
            RelaySendCause::AckGapRepair,
        )
        .is_none(),
        "RepairData is correctness traffic, not path discovery; unproven outputs must not receive repair merely because no proven target is available"
    );
}

#[test]
fn persistent_response_repair_stays_bound_to_modeled_output() {
    let modeled = response_target(1, UnderlayProtocol::Udp, 25.0, 0, 1_000_000, false);
    let alternate = response_target(2, UnderlayProtocol::Tcp, 5.0, 0, 1_000_000, false);
    let cause = RelaySendCause::persistent_server_ack_gap_repair(
        ServerRepairOutputIdentity {
            key: modeled.observation.key,
            incarnation: modeled.observation.incarnation,
        },
        modeled.observation.snapshot,
    );

    let selected = choose_response_repair_target(&[modeled.clone(), alternate.clone()], &[], cause)
        .expect("modeled output remains eligible");
    assert_eq!(selected.observation.key, modeled.observation.key);
    assert!(
        choose_response_repair_target(&[alternate], &[], cause).is_none(),
        "a queued BDP repair must pause instead of switching to a differently modeled output"
    );
    let mut replacement = modeled;
    replacement.observation.incarnation = replacement.observation.incarnation.saturating_add(1);
    assert!(
        choose_response_repair_target(&[replacement], &[], cause).is_none(),
        "a same-key replacement must not inherit a batch sized from the old output incarnation"
    );
}

#[test]
fn response_owner_data_waits_for_missing_lower_owner_debt() {
    let frame = Frame::StreamData {
        stream_id: StreamId(82),
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from_static(b"owner"),
    };
    let survivor = response_target(1, UnderlayProtocol::Udp, 10.0, 0, 1_000_000, false);
    let lower_flights = [
        CarrierPathFlightDebt {
            key: survivor.observation.key,
            bytes: 64,
        },
        CarrierPathFlightDebt {
            key: CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(9),
            },
            bytes: 64,
        },
    ];
    assert!(
        select_response_sender_data_target_with_ordered_debt_and_epoch(
            std::slice::from_ref(&survivor),
            FlowLane::Latency,
            reliable_stream_frame_accounted_bytes(&frame),
            MuxLimits::default(),
            &lower_flights,
            None,
            128,
            None,
        )
        .is_none(),
        "a sole survivor must not receive later OwnerData while a missing lower owner still has debt"
    );
    assert!(
        select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[survivor],
            FlowLane::Latency,
            reliable_stream_frame_accounted_bytes(&frame),
            MuxLimits::default(),
            &[],
            None,
            0,
            None,
        )
        .is_some()
    );
}

#[test]
fn repair_target_does_not_fallback_to_avoided_owner_path() {
    let owner = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 1_000_000, true);
    let mut proof_only = response_target(2, UnderlayProtocol::Udp, 25.0, 0, 1_000_000, false);
    proof_only.observation.has_sender_evidence = true;
    proof_only.observation.has_bulk_rate_evidence = false;

    assert!(
        choose_response_repair_target(
            &[owner.clone(), proof_only],
            &[owner.observation.key],
            RelaySendCause::AckGapRepair,
        )
        .is_none(),
        "RepairData must not retransmit an already-owned range on the same Service path when no distinct proven repair output exists"
    );
}

#[test]
fn path_failure_repair_may_retry_stale_copy_when_all_outputs_are_avoided() {
    let owner = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 1_000_000, true);
    let backup = response_target(2, UnderlayProtocol::Udp, 25.0, 0, 1_000_000, false);

    let selected = choose_response_repair_target(
        &[owner.clone(), backup.clone()],
        &[owner.observation.key, backup.observation.key],
        RelaySendCause::PathFailureRepair,
    )
    .expect("path-failure recovery may retry on a stale live output");

    assert_eq!(
        selected.observation.key, owner.observation.key,
        "PathFailureRepair should fall back by metrics when every live output already has a stale copy; this must not be available to ordinary AckGapRepair"
    );
    assert!(
        choose_response_repair_target(
            &[owner.clone(), backup.clone()],
            &[selected.observation.key],
            RelaySendCause::AckGapRepair,
        )
        .is_some(),
        "ordinary ACK-gap repair still uses a distinct available output when one exists"
    );
    assert!(
        choose_response_repair_target(
            &[owner.clone(), backup.clone()],
            &[owner.observation.key, backup.observation.key],
            RelaySendCause::AckGapRepair,
        )
        .is_none(),
        "ordinary ACK-gap repair must not retry an already-owned or already-repaired range when every output is avoided"
    );
}

#[test]
fn response_lead_must_be_admissible_not_lowest_raw_eta() {
    let mux_limits = MuxLimits::default();
    let mut saturated_low_eta =
        response_target(0, UnderlayProtocol::Udp, 1.0, 512 * 1024, 512 * 1024, true);
    saturated_low_eta
        .observation
        .snapshot
        .product_bytes_in_flight = mux_limits.max_path_flight_bytes as u64;
    let admissible_higher_eta =
        response_target(1, UnderlayProtocol::Udp, 2.0, 0, 512 * 1024, false);
    let selected = choose_response_sender_target(
        &[saturated_low_eta, admissible_higher_eta.clone()],
        FlowLane::Throughput,
        &Frame::StreamData {
            stream_id: StreamId(7),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0; 64 * 1024]),
        },
        CarrierEmitMode::Classified,
        mux_limits,
        &[],
        &[],
        None,
    )
    .expect("admissible higher ETA path should lead");

    assert_eq!(
        selected.observation.key,
        admissible_higher_eta.observation.key
    );
}

#[test]
fn response_stream_ordered_final_control_stays_on_active_lead() {
    let active_data_owner = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 512 * 1024, true);
    let validation_lower_eta = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 512 * 1024, false);

    let selected = choose_response_sender_target(
        &[active_data_owner.clone(), validation_lower_eta],
        FlowLane::Throughput,
        &Frame::StreamFin {
            stream_id: StreamId(7),
            final_offset: 2 * 1024 * 1024,
        },
        CarrierEmitMode::StreamOrdered,
        MuxLimits::default(),
        &[],
        &[],
        None,
    )
    .expect("stream-ordered final control should remain dispatchable");

    assert_eq!(
        selected.observation.key, active_data_owner.observation.key,
        "FIN/final-offset must not move to a validation path and overtake older data"
    );
}

#[test]
fn response_stream_ack_prefers_request_active_over_response_owner() {
    let mut request_active = response_target(1, UnderlayProtocol::Tcp, 50.0, 0, 512 * 1024, false);
    request_active.observation.is_request_active = true;
    let mut response_owner = response_target(0, UnderlayProtocol::Udp, 5.0, 0, 512 * 1024, true);
    response_owner.observation.is_request_active = false;
    let selected = choose_response_sender_target(
        &[response_owner, request_active.clone()],
        FlowLane::Control,
        &Frame::StreamAck {
            stream_id: StreamId(7),
            complete: true,
            ranges: vec![OffsetRange { start: 0, end: 64 }],
        },
        CarrierEmitMode::Classified,
        MuxLimits::default(),
        &[],
        &[],
        None,
    )
    .expect("request Active ACK carrier should remain dispatchable");

    assert_eq!(selected.observation.key, request_active.observation.key);
}

#[test]
fn response_stream_ordered_final_control_waits_for_backpressured_active_lead() {
    let (active_commands, _active_receivers) = reliable_path_command_channels(1);
    active_commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::Throughput,
        )
        .expect("fill active data queue");
    let mut active_data_owner =
        response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 512 * 1024, true);
    observe_response_target_commands(&mut active_data_owner, &active_commands);
    let validation_lower_eta = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 512 * 1024, false);

    let selected = choose_response_sender_target(
        &[active_data_owner, validation_lower_eta],
        FlowLane::Throughput,
        &Frame::StreamFin {
            stream_id: StreamId(7),
            final_offset: 2 * 1024 * 1024,
        },
        CarrierEmitMode::StreamOrdered,
        MuxLimits::default(),
        &[],
        &[],
        None,
    );

    assert!(
        selected.is_none(),
        "stream-ordered FIN must wait behind older active-owner data instead of escaping to validation output"
    );
}

#[test]
fn single_active_response_target_still_obeys_bulk_admission() {
    let mux_limits = MuxLimits::default();
    let mut saturated =
        response_target(0, UnderlayProtocol::Udp, 1.0, 512 * 1024, 512 * 1024, true);
    saturated.observation.snapshot.product_bytes_in_flight =
        mux_limits.max_path_flight_bytes as u64;
    let candidates = [&saturated];
    let outcome = response_target_unique_owner_admission_with_epoch(
        &saturated,
        &candidates,
        ResponseBulkLead {
            key: saturated.observation.key,
            snapshot: saturated.observation.snapshot,
            eta_ms: saturated.observation.eta_ms,
        },
        None,
        Some(saturated.observation.key),
        0,
        ResponseOrderedTail::new(Some(saturated.observation.key), 0)
            .for_candidate(saturated.observation.key),
        64 * 1024,
        mux_limits,
        None,
        true,
        false,
    );
    let (admission, _, _, model_suppression) = outcome.into_parts();
    assert_eq!(admission.decision, PathAdmissionDecision::Standby);
    assert_eq!(model_suppression, Some("inflight_limit"));

    let selected = choose_response_sender_data_target(
        &[saturated],
        FlowLane::Throughput,
        64 * 1024,
        mux_limits,
        &[],
        None,
    );

    assert!(
        selected.is_none(),
        "a temporarily single attached output must not bypass product/carrier flight admission"
    );
}

#[test]
fn response_data_admission_uses_writer_pending_bytes_not_only_slots() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 512 * 1024,
        max_repair_bytes: 512 * 1024,
        max_reorder_bytes: 512 * 1024,
        max_stream_window_bytes: 512 * 1024,
        ..MuxLimits::default()
    };
    let payload_bytes = 8 * 1024;
    let (commands, _receivers) = reliable_path_command_channels(2048);
    let mut snapshot = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 1.0, 8_000_000.0);
    snapshot.confidence = 1.0;
    let mut saturated = ResponseSenderPathTarget {
        #[cfg(feature = "lab-diagnostics")]
        session_id: SessionId(0),
        #[cfg(feature = "lab-diagnostics")]
        binding_instance_id: 0,
        observation: crate::model::response::ResponsePathObservation {
            key: CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(0),
            },
            path_instance_id: next_server_carrier_path_instance_id(),
            incarnation: 1,
            attachment_role: StreamOpenRole::Active,
            snapshot,
            owner_data_in_flight_bytes: 0,
            command_pending_bytes: 0,
            eta_ms: 1.0,
            is_service: true,
            is_request_active: true,
            has_sender_evidence: true,
            has_service_feed_evidence: true,
            has_bulk_rate_evidence: true,
        },
        command_queue: commands.queue_snapshot(),
        tcp_capacity_probe_attempted: commands.tcp_capacity_probe_attempted(),
        tcp_capacity_probe_active: commands.tcp_capacity_probe_active(),
        endpoint_only_service_prior_eligible: false,
        quic_capacity_proof: None,
        quic_capacity_calibration_attempts: 0,
        ack_clock_calibration_eligible: false,
        ack_clock_calibration_proven: false,
        ack_clock_calibration_spent_bytes: 0,
        ack_clock_calibration_credit_limit_bytes: 0,
        ack_clock_calibration_max_limit_bytes: 0,
        ack_clock_calibration_active: false,
    };
    let credit = response_target_emission_credit_bytes(
        &saturated,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );
    while commands.pending_bytes() + payload_bytes as u64 <= credit as u64 {
        commands
            .try_enqueue_admitted_frame(
                Frame::StreamData {
                    stream_id: StreamId(7),
                    offset: commands.pending_bytes(),
                    flags: StreamFlags::NONE,
                    payload: Bytes::from(vec![0; payload_bytes]),
                },
                FlowLane::Throughput,
            )
            .expect("prefill data pipe");
    }
    observe_response_target_commands(&mut saturated, &commands);

    let admissible = response_target(1, UnderlayProtocol::Udp, 2.0, 0, 512 * 1024, false);
    let selected = choose_response_sender_data_target(
        &[saturated.clone(), admissible.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        None,
    )
    .expect("higher-ETA target with writer credit should be selected");

    assert_eq!(selected.observation.key, admissible.observation.key);
    assert!(
        commands
            .pending_bytes()
            .saturating_add(payload_bytes as u64)
            > credit as u64,
        "test must fill the low-ETA writer pipe until the next data frame would exceed byte credit"
    );
}

#[test]
fn quic_proof_success_path_gets_bounded_bulk_only_startup_sampling() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut active = response_target(
        0,
        UnderlayProtocol::Udp,
        1.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    active.observation.snapshot.active_flows = 2;
    let mut proof_success = response_target(1, UnderlayProtocol::Udp, 500.0, 0, 0, false);
    proof_success.observation.snapshot.delivery_rate_bps =
        default_path_rate_bps(UnderlayProtocol::Udp);
    proof_success.observation.snapshot.pacing_rate_bps =
        proof_success.observation.snapshot.delivery_rate_bps;
    proof_success.observation.snapshot.app_limited = true;
    proof_success.observation.snapshot.confidence = 1.0;
    proof_success.observation.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[active.clone(), proof_success.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(active.observation.key),
        0,
        None,
    )
    .expect("QUIC Validation sampling should be dispatchable");

    assert_eq!(
        selected.target.observation.key,
        proof_success.observation.key
    );
    assert_eq!(selected.admission().role, PathRuntimeRole::Subflow);
    assert!(
        selected
            .subflow_admission_selection()
            .is_some_and(|commit| commit.input.startup_owner_allowed)
    );
}

#[test]
fn proof_path_owner_sampling_is_explicit_subflow_not_service_migration() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut active = response_target(
        0,
        UnderlayProtocol::Udp,
        1.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    active.observation.snapshot.active_flows = 2;
    let mut proof_success = response_target(1, UnderlayProtocol::Udp, 500.0, 0, 0, false);
    proof_success.observation.has_sender_evidence = true;
    proof_success.observation.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[active.clone(), proof_success],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(active.observation.key),
        0,
        None,
    )
    .expect("bounded startup sampling should be dispatchable");

    assert_ne!(selected.target.observation.key, active.observation.key);
    assert_eq!(selected.admission().role, PathRuntimeRole::Subflow);
    assert!(
        selected
            .subflow_admission_selection()
            .is_some_and(|commit| commit.input.startup_owner_allowed)
    );
}

#[test]
fn measured_udp_bulk_path_remains_overflow_behind_feedable_udp_service() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let active_udp = response_target(
        0,
        UnderlayProtocol::Udp,
        150.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    let measured_udp = response_target(
        1,
        UnderlayProtocol::Udp,
        10.0,
        0,
        4 * payload_bytes as u64,
        false,
    );

    let selected = choose_response_sender_data_target(
        &[active_udp.clone(), measured_udp],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        }),
    )
    .expect("the feedable UDP Service should remain eligible for ordinary bulk");

    assert_eq!(
        selected.observation.key, active_udp.observation.key,
        "a measured same-family Subflow is additive overflow and must not displace feedable Service"
    );
}

#[test]
fn measured_udp_bulk_path_does_not_steal_tcp_owner_under_lower_debt() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let active_tcp = response_target(
        0,
        UnderlayProtocol::Tcp,
        150.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    let measured_udp = response_target(
        1,
        UnderlayProtocol::Udp,
        10.0,
        0,
        4 * payload_bytes as u64,
        false,
    );

    let selected = choose_response_sender_data_target(
        &[active_tcp.clone(), measured_udp],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[CarrierPathFlightDebt {
            key: active_tcp.observation.key,
            bytes: payload_bytes as u64,
        }],
        Some(active_tcp.observation.key),
    )
    .expect("current TCP primary remains eligible while it owns unresolved lower bytes");

    assert_eq!(
        selected.observation.key, active_tcp.observation.key,
        "mixed TCP/QUIC paths may probe or repair, but must not steal same-stream OwnerData under lower-owner debt"
    );
}

#[test]
fn measured_udp_alternate_does_not_replace_active_service_at_clear_frontier() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut active_unproven_udp = response_target(
        0,
        UnderlayProtocol::Udp,
        5.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    active_unproven_udp.observation.has_bulk_rate_evidence = false;
    let measured_udp = response_target(
        1,
        UnderlayProtocol::Udp,
        10.0,
        0,
        4 * payload_bytes as u64,
        false,
    );

    let selected = choose_response_sender_data_target(
        &[active_unproven_udp, measured_udp.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        }),
    )
    .expect("bulk-rate-proven UDP owner should be eligible at a clear frontier");

    assert_eq!(
        selected.observation.key,
        CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        },
        "a measured alternate must not steal Service ownership merely by existing"
    );
}

#[test]
fn clear_frontier_without_live_service_elects_liveness_service_failover() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut restart = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    restart.observation.has_sender_evidence = false;
    restart.observation.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[restart.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        None,
        0,
        None,
    );

    let selected = selected.expect(
        "when the previous Service is gone and the ordered frontier is clear, the stream must elect a new Service failover path",
    );
    assert_eq!(
        selected.target.observation.key, restart.observation.key,
        "liveness from an attached output is enough for bounded Service failover only when no live Service owner remains"
    );
    assert_eq!(
        selected.admission().role,
        PathRuntimeRole::Service,
        "failover owner bytes are Service OwnerData, not optional Subflow exploration"
    );
}

#[test]
fn repair_attachment_cannot_suppress_liveness_service_failover() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut repair = response_target(
        0,
        UnderlayProtocol::Tcp,
        1.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    repair.observation.attachment_role = StreamOpenRole::Repair;
    let mut validation = response_target(
        1,
        UnderlayProtocol::Tcp,
        50.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    validation.observation.has_sender_evidence = false;
    validation.observation.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[repair, validation.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        None,
        0,
        None,
    )
    .expect("Repair output must not hide an eligible liveness Service survivor");

    assert_eq!(selected.target.observation.key, validation.observation.key);
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);
}

#[test]
fn unproven_liveness_service_failover_respects_startup_assigned_credit() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut failover = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    let startup_credit = response_service_startup_emission_credit_bytes(
        failover.observation.key.underlay,
        payload_bytes,
        mux_limits,
    );
    failover.observation.has_service_feed_evidence = false;
    failover.observation.has_bulk_rate_evidence = false;
    failover.observation.snapshot.product_bytes_in_flight =
        startup_credit.saturating_sub(payload_bytes) as u64;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[failover.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        None,
        0,
        None,
    )
    .expect("a prospective Service with startup credit remaining stays feedable");
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);

    failover.observation.snapshot.product_bytes_in_flight = startup_credit as u64;
    assert!(
        select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[failover],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            None,
            0,
            None,
        )
        .is_none(),
        "newly elected unproven Service must not exceed the cumulative startup horizon before becoming active"
    );
}

#[test]
fn prospective_service_uses_service_credit_instead_of_optional_pipe_credit() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let (commands, _receivers) = reliable_path_command_channels(128);
    let mut failover = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        payload_bytes as u64,
        false,
    );
    observe_response_target_commands(&mut failover, &commands);
    failover.observation.has_bulk_rate_evidence = false;
    failover.observation.snapshot.delivery_rate_bps = 1.0;
    failover.observation.snapshot.pacing_rate_bps = 1.0;
    let optional_credit = response_target_emission_credit_bytes(
        &failover,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );
    let service_credit =
        response_service_emission_credit_bytes(&failover, payload_bytes, mux_limits);
    assert!(
        optional_credit < service_credit,
        "fixture requires optional-path credit below prospective Service credit"
    );
    while commands
        .pending_bytes()
        .saturating_add(payload_bytes as u64)
        <= optional_credit as u64
    {
        commands
            .try_enqueue_admitted_frame(
                Frame::StreamData {
                    stream_id: StreamId(74),
                    offset: commands.pending_bytes(),
                    flags: StreamFlags::NONE,
                    payload: Bytes::from(vec![0; payload_bytes]),
                },
                FlowLane::Throughput,
            )
            .expect("prefill prospective Service without exhausting queue slots");
    }
    observe_response_target_commands(&mut failover, &commands);
    assert!(
        commands.can_enqueue_lane_now(FlowLane::Throughput),
        "fixture must retain a real writer queue slot"
    );
    assert!(
        !response_target_has_emission_credit(
            &failover,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        ),
        "fixture must exceed the optional-path pipe credit"
    );
    assert!(
        response_service_has_assigned_owner_credit(
            &failover,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        ),
        "the same assigned queue remains inside prospective Service credit"
    );

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[failover],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        None,
        0,
        None,
    )
    .expect("pre-role optional-path credit must not suppress Service failover");
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);
}

#[test]
fn mature_liveness_service_failover_uses_product_envelope() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut failover = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    let mature_credit =
        response_service_emission_credit_bytes(&failover, payload_bytes, mux_limits);
    let full_envelope = usize::try_from(bulk_active_service_product_envelope_bytes(
        failover.observation.snapshot,
        payload_bytes,
        mux_limits,
    ))
    .unwrap();
    assert!(
        mature_credit
            > response_service_startup_emission_credit_bytes(
                failover.observation.key.underlay,
                payload_bytes,
                mux_limits,
            ),
        "fixture requires a mature product envelope larger than startup credit"
    );
    assert_eq!(mature_credit, full_envelope);
    failover.observation.snapshot.product_bytes_in_flight =
        mature_credit.saturating_sub(payload_bytes) as u64;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[failover.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        None,
        0,
        None,
    )
    .expect("bulk-rate-proven prospective Service may use the product envelope");
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);

    failover.observation.snapshot.product_bytes_in_flight = mature_credit as u64;
    assert!(
        select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[failover],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            None,
            0,
            None,
        )
        .is_none(),
        "mature Service failover must stop at the product envelope"
    );
}

#[test]
fn mixed_family_clear_frontier_service_failover_is_metric_first() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut tcp = response_target(
        1,
        UnderlayProtocol::Tcp,
        50.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    tcp.observation.has_sender_evidence = true;
    tcp.observation.has_bulk_rate_evidence = false;
    let mut udp = response_target(
        0,
        UnderlayProtocol::Udp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    udp.observation.has_sender_evidence = true;
    udp.observation.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[tcp, udp.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        None,
        0,
        None,
    );

    let selected = selected
        .expect("Service failover must be carrier-neutral when no live ordered owner remains");
    assert_eq!(
        selected.target.observation.key, udp.observation.key,
        "clear-frontier Service failover is selected by path metrics, not by TCP/UDP family"
    );
    assert_eq!(
        selected.admission().role,
        PathRuntimeRole::Service,
        "the elected failover path becomes the new Service owner"
    );
}

#[test]
fn clear_frontier_stale_owner_without_lane_capacity_elects_liveness_service_failover() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut stale_owner =
        response_target(1, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let (owner_commands, _owner_rx) = reliable_path_command_channels(1);
    owner_commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id: StreamId(99),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::Throughput,
        )
        .expect("seed full owner data queue");
    observe_response_target_commands(&mut stale_owner, &owner_commands);
    let mut failover = response_target(
        0,
        UnderlayProtocol::Udp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    failover.observation.has_sender_evidence = true;
    failover.observation.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[stale_owner.clone(), failover.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(stale_owner.observation.key),
        0,
        None,
    );

    let selected = selected.expect(
        "when the ordered frontier is clear and the old Service cannot enqueue, a validated survivor must become Service failover",
    );
    assert_eq!(
        selected.target.observation.key, failover.observation.key,
        "clear-frontier failover is metric-first and must not be trapped by the stale owner's carrier family"
    );
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);
}

#[test]
fn liveness_service_failover_waits_behind_live_owner_tail_guard() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut failover = response_target(
        1,
        UnderlayProtocol::Udp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    failover.observation.has_sender_evidence = true;
    failover.observation.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[failover],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        None,
        payload_bytes,
        None,
    );

    assert!(
        selected.is_none(),
        "liveness Service failover can only own future bytes after the live lower owner frontier is clear"
    );
}

#[test]
fn repair_prefers_bulk_proven_path_over_proof_only_low_eta_path() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let original_owner = response_target(
        0,
        UnderlayProtocol::Tcp,
        20.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    let proven_alternate = response_target(
        1,
        UnderlayProtocol::Tcp,
        50.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    let mut proof_only_udp = response_target(
        2,
        UnderlayProtocol::Udp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    proof_only_udp.observation.has_bulk_rate_evidence = false;

    let selected = choose_response_sender_target(
        &[
            original_owner.clone(),
            proven_alternate.clone(),
            proof_only_udp,
        ],
        FlowLane::Latency,
        &Frame::StreamData {
            stream_id: StreamId(7),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0; payload_bytes]),
        },
        CarrierEmitMode::Classified,
        mux_limits,
        &[],
        &[original_owner.observation.key],
        Some(RelaySendCause::AckGapRepair),
    )
    .expect("repair should remain dispatchable on the proven alternate");

    assert_eq!(
        selected.observation.key, proven_alternate.observation.key,
        "repair must not treat proof-only validation as bulk-capable just because it has lower ETA"
    );
}

#[test]
fn repair_does_not_use_proof_only_path_when_no_proven_repair_path_exists() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let original_owner = response_target(
        0,
        UnderlayProtocol::Tcp,
        20.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    let mut proof_only_udp = response_target(
        1,
        UnderlayProtocol::Udp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    proof_only_udp.observation.has_bulk_rate_evidence = false;

    let selected = choose_response_sender_target(
        &[original_owner.clone(), proof_only_udp.clone()],
        FlowLane::Latency,
        &Frame::StreamData {
            stream_id: StreamId(7),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0; payload_bytes]),
        },
        CarrierEmitMode::Classified,
        mux_limits,
        &[],
        &[original_owner.observation.key],
        Some(RelaySendCause::AckGapRepair),
    );

    assert!(
        selected.is_none(),
        "RepairData must wait for an active or bulk-rate-proven alternate instead of turning proof-only validation into a repair path"
    );
}

#[test]
fn path_failure_repair_can_use_live_liveness_survivor_without_path_proving_it() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let original_owner = response_target(
        0,
        UnderlayProtocol::Tcp,
        20.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    let mut liveness_survivor = response_target(
        1,
        UnderlayProtocol::Udp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    liveness_survivor.observation.has_sender_evidence = true;
    liveness_survivor.observation.has_bulk_rate_evidence = false;

    let selected = choose_response_sender_target(
        &[original_owner.clone(), liveness_survivor.clone()],
        FlowLane::Latency,
        &Frame::StreamData {
            stream_id: StreamId(7),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0; payload_bytes]),
        },
        CarrierEmitMode::Classified,
        mux_limits,
        &[],
        &[original_owner.observation.key],
        Some(RelaySendCause::PathFailureRepair),
    )
    .expect("path-failure repair must be able to recover on a live non-owner output");

    assert_eq!(
        selected.observation.key, liveness_survivor.observation.key,
        "PathFailureRepair is bounded failover retransmission; it must not require bulk-rate proof because it never path-proves or changes Service ownership"
    );
}

#[test]
fn path_failure_repair_prefers_same_family_survivor_before_cross_family_low_eta() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let original_owner = response_target(
        0,
        UnderlayProtocol::Tcp,
        20.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    let mut same_family_survivor = response_target(
        1,
        UnderlayProtocol::Tcp,
        50.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    same_family_survivor.observation.has_sender_evidence = true;
    same_family_survivor.observation.has_bulk_rate_evidence = false;
    let mut cross_family_low_eta = response_target(
        2,
        UnderlayProtocol::Udp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    cross_family_low_eta.observation.has_sender_evidence = true;
    cross_family_low_eta.observation.has_bulk_rate_evidence = false;

    let selected = choose_response_sender_target(
        &[
            original_owner.clone(),
            same_family_survivor.clone(),
            cross_family_low_eta,
        ],
        FlowLane::Throughput,
        &Frame::StreamData {
            stream_id: StreamId(7),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0; payload_bytes]),
        },
        CarrierEmitMode::Classified,
        mux_limits,
        &[],
        &[original_owner.observation.key],
        Some(RelaySendCause::PathFailureRepair),
    )
    .expect("path-failure repair should remain dispatchable on a live survivor");

    assert_eq!(
        selected.observation.key, same_family_survivor.observation.key,
        "failed-owner RepairData should follow the same-family failover survivor before trying cross-family low-ETA repair"
    );
}

#[test]
fn path_failure_repair_bypasses_stale_owner_emission_credit_but_not_queue_capacity() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024,
        max_repair_bytes: 64 * 1024,
        max_reorder_bytes: 64 * 1024,
        max_stream_window_bytes: 64 * 1024,
        ..MuxLimits::default()
    };
    let payload_bytes = 8 * 1024;
    let (commands, _receivers) = reliable_path_command_channels(64);
    let mut survivor = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024, false);
    observe_response_target_commands(&mut survivor, &commands);
    survivor.observation.has_sender_evidence = true;
    survivor.observation.has_bulk_rate_evidence = false;

    let credit = response_target_emission_credit_bytes(
        &survivor,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );
    while commands
        .pending_bytes()
        .saturating_add(payload_bytes as u64)
        <= credit as u64
    {
        commands
            .try_enqueue_admitted_frame(
                Frame::StreamData {
                    stream_id: StreamId(72),
                    offset: commands.pending_bytes(),
                    flags: StreamFlags::NONE,
                    payload: Bytes::from(vec![0; payload_bytes]),
                },
                FlowLane::Throughput,
            )
            .expect("prefill survivor data queue without exhausting slots");
    }
    observe_response_target_commands(&mut survivor, &commands);

    let repair_frame = Frame::StreamData {
        stream_id: StreamId(72),
        offset: 1024,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![7_u8; payload_bytes]),
    };
    assert!(
        commands.can_enqueue_frame_now(&repair_frame, FlowLane::Throughput),
        "test setup must leave a real queue slot for failover RepairData"
    );
    assert!(
        !response_target_has_emission_credit(
            &survivor,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        ),
        "test setup must exceed ordinary owner emission credit"
    );

    let selected = choose_response_sender_target(
        &[survivor.clone()],
        FlowLane::Throughput,
        &repair_frame,
        CarrierEmitMode::Classified,
        mux_limits,
        &[],
        &[],
        Some(RelaySendCause::PathFailureRepair),
    )
    .expect("path-failure RepairData must be admitted while a live queue slot exists");

    assert_eq!(
        selected.observation.key, survivor.observation.key,
        "failed-owner repair is bounded correctness traffic and must not be blocked by stale owner emission credit"
    );
}

#[test]
fn ack_data_only_udp_path_cannot_own_unique_data_when_lower_owner_exists() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let active_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let mut active = response_target(
        0,
        UnderlayProtocol::Udp,
        50.0,
        payload_bytes as u64,
        4 * payload_bytes as u64,
        true,
    );
    active.observation.has_bulk_rate_evidence = false;
    let mut ack_data_only_path = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 0, false);
    ack_data_only_path.observation.has_bulk_rate_evidence = false;

    let selected = choose_response_sender_data_target(
        &[active.clone(), ack_data_only_path.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[CarrierPathFlightDebt {
            key: active_key,
            bytes: PATH_OPEN_SCORE_BYTES as u64,
        }],
        Some(active_key),
    )
    .expect("active owner should remain admissible while lower bytes are unresolved");

    assert_eq!(
        selected.observation.key, active.observation.key,
        "ACK-data-only QUIC paths must not own later ordered bytes while another path owns unresolved lower bytes"
    );
}

#[test]
fn ack_data_quic_path_does_not_preempt_service_owner_under_lower_debt() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let active_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let mut active = response_target(
        0,
        UnderlayProtocol::Udp,
        5.0,
        payload_bytes as u64,
        16 * payload_bytes as u64,
        true,
    );
    active.observation.has_bulk_rate_evidence = true;
    let mut ack_data_only_path = response_target(1, UnderlayProtocol::Udp, 500.0, 0, 0, false);
    ack_data_only_path.observation.has_bulk_rate_evidence = false;
    ack_data_only_path.observation.snapshot.delivery_rate_bps =
        default_path_rate_bps(UnderlayProtocol::Udp);
    ack_data_only_path.observation.snapshot.pacing_rate_bps =
        ack_data_only_path.observation.snapshot.delivery_rate_bps;
    ack_data_only_path.observation.snapshot.app_limited = true;

    let selected = choose_response_sender_data_target(
        &[active.clone(), ack_data_only_path.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[CarrierPathFlightDebt {
            key: active_key,
            bytes: PATH_OPEN_SCORE_BYTES as u64,
        }],
        Some(active_key),
    )
    .expect("active owner should remain selected while it owns the lower frontier");

    assert_eq!(
        selected.observation.key, active.observation.key,
        "ACK-data-only paths must not preempt the service owner while lower-owner debt exists"
    );
}

#[test]
fn quic_ack_data_seen_validation_path_bootstraps_as_bounded_subflow() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut active = response_target(
        0,
        UnderlayProtocol::Udp,
        50.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    active.observation.has_bulk_rate_evidence = true;
    active.observation.snapshot.active_flows = 2;
    let mut ack_data_only = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 0, false);
    ack_data_only.observation.has_bulk_rate_evidence = false;
    ack_data_only.observation.has_sender_evidence = true;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[active.clone(), ack_data_only.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        }),
        0,
        None,
    )
    .expect("bulk-rate-proven Service should remain dispatchable");

    assert_eq!(
        selected.target.observation.key, ack_data_only.observation.key,
        "sender-evidenced same-family Validation may consume bounded startup sampling credit"
    );
    assert_eq!(
        selected.admission().role,
        PathRuntimeRole::Subflow,
        "startup sampling must not migrate the Service owner"
    );
    assert!(
        selected
            .subflow_admission_selection()
            .is_some_and(|commit| commit.input.startup_owner_allowed)
    );
}

#[test]
fn measured_same_family_subflow_is_not_throttled_by_startup_credit() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let active_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let mut active = response_target(
        0,
        UnderlayProtocol::Udp,
        50.0,
        0,
        16 * payload_bytes as u64,
        true,
    );
    active.observation.has_bulk_rate_evidence = true;
    let service_envelope = bulk_active_service_product_envelope_bytes(
        active.observation.snapshot,
        payload_bytes,
        mux_limits,
    );
    active.observation.snapshot.product_bytes_in_flight = service_envelope;
    active.observation.snapshot.queue_bytes = payload_bytes as u64;
    let mut bulk_rate_subflow = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 0, false);
    bulk_rate_subflow.observation.has_sender_evidence = true;
    bulk_rate_subflow.observation.has_bulk_rate_evidence = true;

    let first = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[active.clone(), bulk_rate_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(active_key),
        0,
        None,
    )
    .expect("first measured Subflow frame should be admitted");
    let commit = first
        .subflow_admission_selection()
        .expect("measured Subflow admission should carry commit state");
    assert_eq!(first.admission().role, PathRuntimeRole::Subflow);
    assert_eq!(
        commit.startup_owner_credit_bytes,
        usize::try_from(reliable_subflow_startup_sample_limit_bytes(mux_limits)).unwrap(),
        "the Subflow ledger keeps one stable startup sampling envelope across all decisions"
    );

    let mut subflow_set = FlowSubflowSet::new(
        0,
        commit.service,
        commit.startup_owner_credit_bytes,
        commit.optional_overhead_budget_bytes,
        commit.max_read_gap_budget,
    );
    assert_eq!(
        subflow_set.admit_subflow_owner(commit.input).decision,
        PathAdmissionDecision::AdmitSubflow
    );

    let second = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[active.clone(), bulk_rate_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(active_key),
        0,
        Some(&subflow_set),
    )
    .expect("measured Subflow should remain eligible if per-decision no-worse gates pass");
    assert_eq!(
        second.target.observation.key,
        bulk_rate_subflow.observation.key
    );
    assert_eq!(second.admission().role, PathRuntimeRole::Subflow);
}

#[test]
fn sender_evidence_same_family_candidate_cannot_own_under_lower_owner_debt() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let active_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let active = response_target(
        active_key.path_id.0,
        active_key.underlay,
        100.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    let mut proof_only = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    proof_only.observation.has_bulk_rate_evidence = false;
    proof_only.observation.has_sender_evidence = true;

    let selected = choose_response_sender_data_target(
        &[active.clone(), proof_only.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[CarrierPathFlightDebt {
            key: active_key,
            bytes: payload_bytes as u64,
        }],
        Some(active_key),
    )
    .expect("service path should remain dispatchable");

    assert_eq!(
        selected.observation.key, active.observation.key,
        "same-family sender evidence is not enough to assign later unique bytes while the Service owns unresolved lower bytes"
    );
}

#[test]
fn bulk_rate_same_family_candidate_cannot_own_later_data_under_lower_owner_debt() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut owner = response_target(
        0,
        UnderlayProtocol::Udp,
        80.0,
        2 * 1024 * 1024,
        16 * 1024 * 1024,
        true,
    );
    owner.observation.snapshot.product_progress_rate_bps = Some(500_000_000.0);
    let alternate = response_target(1, UnderlayProtocol::Udp, 7.0, 0, 16 * 1024 * 1024, false);
    let lower_flights = vec![CarrierPathFlightDebt {
        key: owner.observation.key,
        bytes: 2 * 1024 * 1024,
    }];

    let selected = choose_response_sender_data_target(
        &[owner.clone(), alternate],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &lower_flights,
        Some(owner.observation.key),
    )
    .expect("lower owner should remain dispatchable");

    assert_eq!(
        selected.observation.key, owner.observation.key,
        "bulk-rate evidence proves the alternate path is eligible at a clear frontier, not that it may extend an existing ordered receive hole"
    );
}

#[test]
fn single_response_carrier_uses_sliding_window_not_multipath_ordering_debt() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let assigned_bytes = bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits)
        .saturating_sub(payload_bytes);
    let mut target = response_target(
        0,
        UnderlayProtocol::Tcp,
        5.0,
        assigned_bytes as u64,
        16 * 1024 * 1024,
        true,
    );
    target.observation.snapshot.product_progress_rate_bps = Some(10_000_000_000.0);
    let lower_flights = vec![CarrierPathFlightDebt {
        key: target.observation.key,
        bytes: assigned_bytes as u64,
    }];

    let selected = choose_response_sender_data_target(
        std::slice::from_ref(&target),
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &lower_flights,
        Some(target.observation.key),
    )
    .expect("single carrier lower flight is normal sliding-window debt");

    assert_eq!(selected.observation.key, target.observation.key);
}

#[test]
fn proven_udp_candidate_cannot_overtake_large_lower_owner() {
    let mut owner = response_target(
        0,
        UnderlayProtocol::Udp,
        80.0,
        2 * 1024 * 1024,
        16 * 1024 * 1024,
        true,
    );
    owner.observation.snapshot.product_progress_rate_bps = Some(500_000_000.0);
    let alternate = response_target(1, UnderlayProtocol::Udp, 7.0, 0, 16 * 1024 * 1024, false);
    let lower_flights = vec![CarrierPathFlightDebt {
        key: owner.observation.key,
        bytes: 2 * 1024 * 1024,
    }];

    let selected = choose_response_sender_data_target(
        &[owner.clone(), alternate.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &lower_flights,
        Some(owner.observation.key),
    )
    .expect("lower owner should remain eligible while it owns unresolved lower bytes");

    assert_eq!(selected.observation.key, owner.observation.key);
}

#[test]
fn proven_udp_candidate_waits_even_when_lower_owner_debt_is_within_reorder_budget() {
    let mut owner = response_target(
        0,
        UnderlayProtocol::Udp,
        80.0,
        2 * 1024 * 1024,
        16 * 1024 * 1024,
        true,
    );
    owner.observation.snapshot.product_progress_rate_bps = Some(500_000_000.0);
    let lower_eta_alternate =
        response_target(1, UnderlayProtocol::Udp, 7.0, 0, 16 * 1024 * 1024, false);
    let lower_flights = vec![CarrierPathFlightDebt {
        key: owner.observation.key,
        bytes: 64 * 1024,
    }];

    let selected = choose_response_sender_data_target(
        &[owner.clone(), lower_eta_alternate],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &lower_flights,
        Some(owner.observation.key),
    )
    .expect("lower owner should remain eligible while the frontier is not clear");

    assert_eq!(selected.observation.key.path_id, PathId(0));
}

#[test]
fn proof_only_udp_candidate_is_blocked_from_unique_data_with_lower_udp_owner() {
    let mut owner = response_target(
        0,
        UnderlayProtocol::Udp,
        80.0,
        2 * 1024 * 1024,
        16 * 1024 * 1024,
        true,
    );
    owner.observation.snapshot.product_progress_rate_bps = Some(500_000_000.0);
    let mut proof_only = response_target(1, UnderlayProtocol::Udp, 7.0, 0, 16 * 1024 * 1024, false);
    proof_only.observation.has_bulk_rate_evidence = false;
    let lower_flights = vec![CarrierPathFlightDebt {
        key: owner.observation.key,
        bytes: 64 * 1024,
    }];

    let selected = choose_response_sender_data_target(
        &[owner.clone(), proof_only],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &lower_flights,
        Some(owner.observation.key),
    )
    .expect("proof-only path should not own unique later bytes");

    assert_eq!(selected.observation.key, owner.observation.key);
}

#[test]
fn proof_only_tcp_candidate_does_not_displace_bulk_rate_proven_udp() {
    let bulk_proven_udp =
        response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let mut proof_only_tcp =
        response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    proof_only_tcp.observation.has_sender_evidence = true;
    proof_only_tcp.observation.has_bulk_rate_evidence = false;

    let selected = choose_response_sender_data_target(
        &[bulk_proven_udp.clone(), proof_only_tcp],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(bulk_proven_udp.observation.key),
    )
    .expect("bulk-rate-proven path should remain unique ordered owner");

    assert_eq!(selected.observation.key, bulk_proven_udp.observation.key);
}

#[test]
fn response_clear_frontier_keeps_feedable_service_ahead_of_lower_eta_subflow() {
    let lead = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let lower_eta_alternate =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

    let selected = choose_response_sender_data_target(
        &[lead.clone(), lower_eta_alternate.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(lead.observation.key),
    )
    .expect("feedable Service should remain selected");

    assert_eq!(selected.observation.key, lead.observation.key);
}

#[test]
fn feedable_service_precedes_lower_eta_same_family_subflow() {
    let service = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let lower_eta_subflow =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), lower_eta_subflow.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(service.observation.key),
        0,
        None,
    )
    .expect("feedable Service should remain selected ahead of admitted overflow");

    assert_eq!(selected.target.observation.key, service.observation.key);
    assert_eq!(
        selected.admission().role,
        PathRuntimeRole::Service,
        "a lower-ETA Subflow remains eligible overflow and does not displace feedable Service"
    );
}

#[test]
fn same_family_lower_frontier_owner_remains_subflow() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);

    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let service = response_target(1, underlay, 50.0, 0, 16 * 1024 * 1024, true);
        let lower_owner = response_target(0, underlay, 5.0, 0, 16 * 1024 * 1024, false);
        let lower_flights = [CarrierPathFlightDebt {
            key: lower_owner.observation.key,
            bytes: payload_bytes as u64,
        }];

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), lower_owner.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &lower_flights,
            Some(service.observation.key),
            payload_bytes.saturating_mul(2),
            None,
        )
        .expect("measured lower-frontier owner should remain dispatchable as a Subflow");

        assert_eq!(
            selected.target.observation.key, lower_owner.observation.key,
            "{underlay:?}"
        );
        assert_eq!(selected.admission().role, PathRuntimeRole::Subflow);
        assert_eq!(
            selected
                .subflow_admission_selection()
                .map(|commit| commit.service),
            Some(service.observation.key),
            "{underlay:?} lower-frontier continuation must retain the Service anchor"
        );
    }
}

#[test]
fn cross_family_lower_frontier_owner_remains_subflow() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(1, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let lower_owner = response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let lower_flights = [CarrierPathFlightDebt {
        key: lower_owner.observation.key,
        bytes: payload_bytes as u64,
    }];

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), lower_owner.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &lower_flights,
        Some(service.observation.key),
        payload_bytes.saturating_mul(2),
        None,
    )
    .expect("measured cross-family lower-frontier owner should remain dispatchable");

    assert_eq!(selected.target.observation.key, lower_owner.observation.key);
    assert_eq!(selected.admission().role, PathRuntimeRole::Subflow);
    assert_eq!(
        selected
            .subflow_admission_selection()
            .map(|commit| commit.service),
        Some(service.observation.key),
        "cross-family continuation must not commit an implicit Service migration"
    );
}

#[test]
fn authoritative_lower_frontier_suspends_unmeasured_startup_sampling() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);

    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let service = response_target(1, underlay, 50.0, 0, 16 * 1024 * 1024, true);
        let mut proof_only = response_target(0, underlay, 5.0, 0, 16 * 1024 * 1024, false);
        proof_only.observation.has_bulk_rate_evidence = false;
        let lower_flights = [CarrierPathFlightDebt {
            key: proof_only.observation.key,
            bytes: payload_bytes as u64,
        }];

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), proof_only],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &lower_flights,
            Some(service.observation.key),
            payload_bytes.saturating_mul(2),
            None,
        );

        assert!(
            selected.is_none(),
            "{underlay:?} sender evidence alone must not extend an ACK hole"
        );
    }
}

#[test]
fn slow_measured_lower_frontier_cannot_borrow_service_admission() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);

    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let service = response_target(1, underlay, 5.0, 0, 16 * 1024 * 1024, true);
        let mut slow_lower_owner = response_target(0, underlay, 500.0, 0, 16 * 1024 * 1024, false);
        slow_lower_owner.observation.snapshot.delivery_rate_bps = 20_000_000.0;
        slow_lower_owner.observation.snapshot.pacing_rate_bps = 20_000_000.0;
        slow_lower_owner
            .observation
            .snapshot
            .product_progress_rate_bps = Some(20_000_000.0);
        let lower_flights = [CarrierPathFlightDebt {
            key: slow_lower_owner.observation.key,
            bytes: payload_bytes as u64,
        }];

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), slow_lower_owner],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &lower_flights,
            Some(service.observation.key),
            payload_bytes.saturating_mul(2),
            None,
        );

        assert!(
            selected.is_none(),
            "{underlay:?} lower ownership is not permission to borrow Service admission"
        );
    }
}

#[test]
fn backpressured_service_remains_lower_frontier_completion_baseline() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(1, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let (service_commands, _service_receivers) = reliable_path_command_channels(1);
    service_commands
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: StreamId(901),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::Throughput,
        )
        .expect("test setup should fill the Service data queue");
    observe_response_target_commands(&mut service, &service_commands);
    let lower_owner = response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let lower_flights = [CarrierPathFlightDebt {
        key: lower_owner.observation.key,
        bytes: payload_bytes as u64,
    }];

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), lower_owner.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &lower_flights,
        Some(service.observation.key),
        payload_bytes.saturating_mul(2),
        None,
    )
    .expect("measured lower-frontier Subflow should be evaluated against queued Service");

    assert_eq!(selected.target.observation.key, lower_owner.observation.key);
    assert_eq!(selected.admission().role, PathRuntimeRole::Subflow);
    assert_eq!(
        selected
            .subflow_admission_selection()
            .map(|commit| commit.service),
        Some(service.observation.key)
    );
}

#[test]
fn detached_service_with_lower_frontier_waits_for_repair_or_ack_clear() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let lower_owner = response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let lower_flights = [CarrierPathFlightDebt {
        key: lower_owner.observation.key,
        bytes: payload_bytes as u64,
    }];

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        std::slice::from_ref(&lower_owner),
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &lower_flights,
        None,
        payload_bytes.saturating_mul(2),
        None,
    );

    assert!(
        selected.is_none(),
        "a lower-hole owner cannot infer Service authority after the anchor detaches"
    );
}

#[test]
fn clear_frontier_unavailable_ordered_owner_reanchors_service_to_bulk_proven_path() {
    let (service_commands, _service_receivers) = reliable_path_command_channels(1);
    let mut service_snapshot =
        PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 50.0, 500_000_000.0);
    service_snapshot.inflight_limit_bytes = 16 * 1024 * 1024;
    service_snapshot.confidence = 1.0;
    let mut service = ResponseSenderPathTarget {
        #[cfg(feature = "lab-diagnostics")]
        session_id: SessionId(0),
        #[cfg(feature = "lab-diagnostics")]
        binding_instance_id: 0,
        observation: crate::model::response::ResponsePathObservation {
            key: CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(1),
            },
            path_instance_id: next_server_carrier_path_instance_id(),
            incarnation: 1,
            attachment_role: StreamOpenRole::Active,
            snapshot: service_snapshot,
            owner_data_in_flight_bytes: 0,
            command_pending_bytes: 0,
            eta_ms: 50.0,
            is_service: true,
            is_request_active: true,
            has_sender_evidence: true,
            has_service_feed_evidence: true,
            has_bulk_rate_evidence: true,
        },
        command_queue: service_commands.queue_snapshot(),
        tcp_capacity_probe_attempted: service_commands.tcp_capacity_probe_attempted(),
        tcp_capacity_probe_active: service_commands.tcp_capacity_probe_active(),
        endpoint_only_service_prior_eligible: false,
        quic_capacity_proof: None,
        quic_capacity_calibration_attempts: 0,
        ack_clock_calibration_eligible: false,
        ack_clock_calibration_proven: false,
        ack_clock_calibration_spent_bytes: 0,
        ack_clock_calibration_credit_limit_bytes: 0,
        ack_clock_calibration_max_limit_bytes: 0,
        ack_clock_calibration_active: false,
    };
    service_commands
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: StreamId(900),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"x"),
            },
            FlowLane::Throughput,
        )
        .expect("test setup should fill the service data queue");
    observe_response_target_commands(&mut service, &service_commands);
    let lower_eta_subflow =
        response_target(2, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), lower_eta_subflow.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(service.observation.key),
        0,
        None,
    )
    .expect("bulk-rate-proven alternate should become Service when the prior clear-frontier owner is not dispatchable");

    assert_eq!(
        selected.target.observation.key,
        lower_eta_subflow.observation.key
    );
    assert_eq!(
        selected.admission().role,
        PathRuntimeRole::Service,
        "a clear-frontier owner hint is not a permanent Service anchor when that output cannot enqueue owner bytes"
    );
}

#[test]
fn lower_eta_same_family_subflow_does_not_borrow_active_service_envelope() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let mut saturated_subflow =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 512 * 1024, false);
    saturated_subflow
        .observation
        .snapshot
        .product_bytes_in_flight = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES;
    saturated_subflow.observation.snapshot.bytes_in_flight =
        RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), saturated_subflow],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        0,
        None,
    )
    .expect("Service should remain eligible when the lower-ETA Subflow is out of credit");

    assert_eq!(
        selected.target.observation.key, service.observation.key,
        "non-active Subflow admission must use additional-path gates instead of the active Service envelope"
    );
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);
}

#[test]
fn response_ordinary_bulk_keeps_lead_only_inside_measured_hysteresis() {
    let mut lead = response_target(0, UnderlayProtocol::Udp, 5.1, 0, 16 * 1024 * 1024, true);
    let mut lower_eta_alternate =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    lead.observation.snapshot.jitter_ms = 0.2;
    lower_eta_alternate.observation.snapshot.jitter_ms = 0.1;

    let selected = choose_response_sender_data_target(
        &[lead.clone(), lower_eta_alternate],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(lead.observation.key),
    )
    .expect("near-tie lead should remain selected inside observed jitter");

    assert_eq!(selected.observation.key, lead.observation.key);
}

#[test]
fn active_service_remains_admissible_lead_when_subflow_is_not_admissible() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 25.0, 0, 16 * 1024 * 1024, true);
    service.observation.has_bulk_rate_evidence = false;
    let mut subflow = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        mux_limits.max_path_flight_bytes as u64,
        16 * 1024 * 1024,
        false,
    );
    subflow.observation.has_bulk_rate_evidence = true;
    let candidates = [&service, &subflow];

    let lead = choose_response_admissible_lead(
        &candidates,
        Some(&service.observation),
        mux_limits,
        payload_bytes,
        &[],
        false,
    )
    .expect("active Service must remain a lead candidate when optional Subflow is blocked");

    assert_eq!(
        lead.key, service.observation.key,
        "optional bulk-rate evidence must not hide the current Service owner"
    );
}

#[test]
fn active_service_remains_lead_when_measured_subflow_has_lower_eta() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(0, UnderlayProtocol::Tcp, 25.0, 0, 16 * 1024 * 1024, true);
    let measured_subflow =
        response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    let candidates = [&service, &measured_subflow];

    let lead = choose_response_admissible_lead(
        &candidates,
        Some(&service.observation),
        mux_limits,
        payload_bytes,
        &[],
        false,
    )
    .expect("active Service should remain the lead anchor");

    assert_eq!(
        lead.key, service.observation.key,
        "a lower-ETA same-family Subflow must not redefine Service ownership"
    );
}

#[test]
fn feedable_service_owner_is_selected_before_lower_eta_same_family_subflow() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
    service.observation.snapshot.product_progress_rate_bps = Some(80_000_000.0);
    service.observation.has_sender_evidence = true;
    service.observation.has_bulk_rate_evidence = true;

    let mut measured_subflow =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    measured_subflow
        .observation
        .snapshot
        .product_progress_rate_bps = Some(120_000_000.0);
    measured_subflow.observation.snapshot.app_limited = false;
    measured_subflow.observation.has_sender_evidence = true;
    measured_subflow.observation.has_bulk_rate_evidence = true;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        0,
        None,
    )
    .expect("feedable Service owner should remain dispatchable");

    assert_eq!(
        selected.target.observation.key, service.observation.key,
        "same-family Subflow OwnerData is additive; it must not replace a feedable Service quantum just because its instantaneous ETA is lower"
    );
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);
}

#[test]
fn measured_tcp_subflow_uses_bounded_reservoir_beyond_service_horizon() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let mut service = response_target(
        0,
        UnderlayProtocol::Tcp,
        25.0,
        service_horizon.saturating_sub(payload_bytes) as u64,
        mux_limits.max_path_flight_bytes as u64,
        true,
    );
    service.observation.snapshot.product_progress_rate_bps = Some(80_000_000.0);
    let mut measured_subflow = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    measured_subflow
        .observation
        .snapshot
        .product_progress_rate_bps = Some(120_000_000.0);
    measured_subflow.observation.snapshot.srtt_ms = 80.0;
    measured_subflow.observation.snapshot.min_rtt_ms = 80.0;
    measured_subflow.observation.snapshot.delivery_rate_bps = 200_000_000.0;
    measured_subflow.observation.snapshot.pacing_rate_bps = 200_000_000.0;
    measured_subflow.observation.snapshot.app_limited = false;

    let below_horizon = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        service_horizon.saturating_sub(payload_bytes),
        None,
    )
    .expect("Service should fill its protected horizon first");
    assert_eq!(
        below_horizon.target.observation.key,
        service.observation.key
    );
    assert_eq!(below_horizon.admission().role, PathRuntimeRole::Service);

    service.observation.snapshot.product_bytes_in_flight = service_horizon as u64;
    service.observation.owner_data_in_flight_bytes = service_horizon as u64;
    let reservoir_subflow = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        service_horizon,
        None,
    )
    .expect("measured TCP Subflow should use the remaining source reservoir");
    assert_eq!(
        reservoir_subflow.target.observation.key,
        measured_subflow.observation.key
    );
    assert_eq!(reservoir_subflow.admission().role, PathRuntimeRole::Subflow);
    assert_eq!(
        reservoir_subflow
            .subflow_admission_selection()
            .map(|commit| commit.service),
        Some(service.observation.key),
        "overflow must remain bound to the exact current Service epoch"
    );

    let product_reservoir = bulk_service_product_envelope_payload_bytes(payload_bytes, mux_limits);
    service.observation.snapshot.product_bytes_in_flight = (product_reservoir / 2) as u64;
    service.observation.owner_data_in_flight_bytes = (product_reservoir / 2) as u64;
    let mut backlog_subflow = measured_subflow.clone();
    backlog_subflow.observation.eta_ms = 400.0;
    backlog_subflow.observation.snapshot.srtt_ms = 360.0;
    backlog_subflow.observation.snapshot.min_rtt_ms = 360.0;
    backlog_subflow.observation.snapshot.delivery_rate_bps = 200_000_000.0;
    backlog_subflow.observation.snapshot.pacing_rate_bps = 200_000_000.0;
    let backlog_selection = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), backlog_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        product_reservoir / 2,
        None,
    )
    .expect("Service remains feedable when cross-path prefix debt is capped");
    assert_eq!(
        backlog_selection.target.observation.key,
        service.observation.key
    );
    assert_eq!(backlog_selection.admission().role, PathRuntimeRole::Service);

    service.observation.snapshot.product_bytes_in_flight = product_reservoir as u64;
    service.observation.owner_data_in_flight_bytes = product_reservoir as u64;
    let exhausted_reservoir = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        product_reservoir,
        None,
    );
    assert!(
        exhausted_reservoir.is_none(),
        "the full product envelope blocks new ownership until ACK progress"
    );
}

#[test]
fn measured_quic_subflow_uses_bounded_reservoir_before_new_startup() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let mut service = response_target(
        0,
        UnderlayProtocol::Udp,
        25.0,
        service_horizon as u64,
        mux_limits.max_path_flight_bytes as u64,
        true,
    );
    service.observation.snapshot.product_progress_rate_bps = Some(80_000_000.0);
    service.observation.snapshot.active_flows = 1;
    service.observation.owner_data_in_flight_bytes = service_horizon as u64;
    let mut measured_subflow = response_target(
        1,
        UnderlayProtocol::Udp,
        5.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    measured_subflow
        .observation
        .snapshot
        .product_progress_rate_bps = Some(120_000_000.0);
    measured_subflow.observation.snapshot.srtt_ms = 80.0;
    measured_subflow.observation.snapshot.min_rtt_ms = 80.0;
    measured_subflow.observation.snapshot.delivery_rate_bps = 200_000_000.0;
    measured_subflow.observation.snapshot.pacing_rate_bps = 200_000_000.0;
    measured_subflow.observation.snapshot.app_limited = false;
    let mut unmeasured = response_target(
        2,
        UnderlayProtocol::Udp,
        1.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    unmeasured.observation.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow.clone(), unmeasured],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        service_horizon,
        None,
    )
    .expect("a measured QUIC Subflow should use the bounded same-family partition");

    assert_eq!(
        selected.target.observation.key,
        measured_subflow.observation.key
    );
    assert_eq!(selected.admission().role, PathRuntimeRole::Subflow);
    assert_eq!(
        selected
            .subflow_admission_selection()
            .map(|commit| commit.service),
        Some(service.observation.key),
        "measured QUIC overflow remains bound to the current Service"
    );

    let product_reservoir = bulk_service_product_envelope_payload_bytes(payload_bytes, mux_limits);
    let exhausted = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        product_reservoir,
        None,
    )
    .expect("Service remains the fallback at the ordering-reservoir boundary");
    assert_eq!(exhausted.target.observation.key, service.observation.key);
    assert_eq!(exhausted.admission().role, PathRuntimeRole::Service);
}

#[test]
fn measured_quic_subflow_does_not_cross_into_equal_path_load() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let mut service = response_target(
        0,
        UnderlayProtocol::Udp,
        25.0,
        service_horizon as u64,
        mux_limits.max_path_flight_bytes as u64,
        true,
    );
    service.observation.owner_data_in_flight_bytes = service_horizon as u64;
    service.observation.snapshot.active_flows = 1;
    let mut measured_subflow = response_target(
        1,
        UnderlayProtocol::Udp,
        5.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    measured_subflow.observation.snapshot.active_flows = 1;
    measured_subflow.observation.snapshot.app_limited = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        service_horizon,
        None,
    )
    .expect("the balanced QUIC Service should remain dispatchable");

    assert_eq!(selected.target.observation.key, service.observation.key);
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);
}

#[test]
fn tcp_reservoir_does_not_charge_service_horizon_to_low_bdp_subflow() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let mut service = response_target(
        0,
        UnderlayProtocol::Tcp,
        25.0,
        service_horizon as u64,
        mux_limits.max_path_flight_bytes as u64,
        true,
    );
    service.observation.snapshot.product_progress_rate_bps = Some(80_000_000.0);

    let mut low_bdp_subflow = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    low_bdp_subflow
        .observation
        .snapshot
        .product_progress_rate_bps = Some(54_016_000.0);
    low_bdp_subflow.observation.snapshot.delivery_rate_bps = 54_016_000.0;
    low_bdp_subflow.observation.snapshot.pacing_rate_bps = 54_016_000.0;
    low_bdp_subflow.observation.snapshot.srtt_ms = 137.968;
    low_bdp_subflow.observation.snapshot.min_rtt_ms = 137.968;
    low_bdp_subflow.observation.snapshot.app_limited = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), low_bdp_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        service_horizon,
        None,
    )
    .expect("Service or its measured TCP Subflow must remain feedable");

    assert_eq!(
        selected.target.observation.key, low_bdp_subflow.observation.key,
        "the connection-level Service horizon consumes global reservoir credit once; it is not candidate-local BDP flight"
    );
    assert_eq!(selected.admission().role, PathRuntimeRole::Subflow);
}

#[test]
fn tcp_reservoir_requires_unique_service_owner_horizon() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let mut service = response_target(
        0,
        UnderlayProtocol::Tcp,
        25.0,
        service_horizon as u64,
        mux_limits.max_path_flight_bytes as u64,
        true,
    );
    service.observation.owner_data_in_flight_bytes = payload_bytes as u64;
    service.observation.snapshot.queue_bytes = service_horizon as u64;
    service.observation.snapshot.product_progress_rate_bps = Some(80_000_000.0);

    let mut measured_subflow = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    measured_subflow
        .observation
        .snapshot
        .product_progress_rate_bps = Some(200_000_000.0);
    measured_subflow.observation.snapshot.delivery_rate_bps = 200_000_000.0;
    measured_subflow.observation.snapshot.pacing_rate_bps = 200_000_000.0;
    measured_subflow.observation.snapshot.app_limited = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        service_horizon,
        None,
    )
    .expect("Service remains the fallback until its unique quota is assigned");

    assert_eq!(selected.target.observation.key, service.observation.key);
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);
}

#[test]
fn tcp_reservoir_split_derives_reduced_resource_geometry() {
    let mut mux_limits = MuxLimits::default();
    let resource_limit = 4 * 1024 * 1024;
    mux_limits.max_path_flight_bytes = resource_limit;
    mux_limits.max_repair_bytes = resource_limit;
    mux_limits.max_reorder_bytes = resource_limit;
    mux_limits.max_stream_window_bytes = resource_limit as u64;
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let feed_reservoir = bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits);
    assert!(
        service_horizon < bulk_service_horizon_payload_bytes(payload_bytes, MuxLimits::default())
    );
    assert!(feed_reservoir <= resource_limit);

    let service = response_target(
        0,
        UnderlayProtocol::Tcp,
        25.0,
        service_horizon as u64,
        resource_limit as u64,
        true,
    );
    let mut measured_subflow = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        resource_limit as u64,
        false,
    );
    measured_subflow.observation.snapshot.srtt_ms = 80.0;
    measured_subflow.observation.snapshot.min_rtt_ms = 80.0;
    measured_subflow.observation.snapshot.delivery_rate_bps = 200_000_000.0;
    measured_subflow.observation.snapshot.pacing_rate_bps = 200_000_000.0;
    measured_subflow.observation.snapshot.app_limited = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        service_horizon,
        None,
    )
    .expect("reduced valid resources should retain the derived TCP split");
    assert_eq!(
        selected.target.observation.key,
        measured_subflow.observation.key
    );
    assert_eq!(selected.admission().role, PathRuntimeRole::Subflow);
}

#[test]
fn tcp_reservoir_split_yields_to_latency_and_calibration_fences() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let mut service = response_target(
        0,
        UnderlayProtocol::Tcp,
        25.0,
        service_horizon as u64,
        mux_limits.max_path_flight_bytes as u64,
        true,
    );
    let mut measured_subflow = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    measured_subflow.observation.snapshot.srtt_ms = 80.0;
    measured_subflow.observation.snapshot.min_rtt_ms = 80.0;
    measured_subflow.observation.snapshot.delivery_rate_bps = 200_000_000.0;
    measured_subflow.observation.snapshot.pacing_rate_bps = 200_000_000.0;
    measured_subflow.observation.snapshot.app_limited = false;

    service.observation.snapshot.active_latency_sensitive_flows = 1;
    let path_pressure = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        service_horizon,
        None,
    )
    .expect("Service stays live under path-local latency pressure");
    assert_eq!(
        path_pressure.target.observation.key,
        service.observation.key
    );

    service.observation.snapshot.active_latency_sensitive_flows = 0;
    service
        .observation
        .snapshot
        .session_active_latency_sensitive_flows = 1;
    let session_pressure = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        service_horizon,
        None,
    )
    .expect("Service stays live under session latency pressure");
    assert_eq!(
        session_pressure.target.observation.key,
        service.observation.key
    );

    service
        .observation
        .snapshot
        .session_active_latency_sensitive_flows = 0;
    measured_subflow.ack_clock_calibration_eligible = true;
    measured_subflow.ack_clock_calibration_active = true;
    measured_subflow.ack_clock_calibration_proven = true;
    measured_subflow.ack_clock_calibration_spent_bytes =
        reliable_ack_clock_calibration_limit_bytes(mux_limits);
    measured_subflow.ack_clock_calibration_credit_limit_bytes =
        measured_subflow.ack_clock_calibration_spent_bytes;
    measured_subflow.ack_clock_calibration_max_limit_bytes =
        measured_subflow.ack_clock_calibration_spent_bytes;
    let calibration_fence = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        service_horizon,
        None,
    )
    .expect("Service remains available while exact calibration flights drain");
    assert_eq!(
        calibration_fence.target.observation.key,
        service.observation.key
    );
    assert_eq!(calibration_fence.admission().role, PathRuntimeRole::Service);
}

#[test]
fn tcp_reservoir_waits_for_binding_calibration_tail() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let mut service = response_target(
        0,
        UnderlayProtocol::Tcp,
        25.0,
        service_horizon as u64,
        mux_limits.max_path_flight_bytes as u64,
        true,
    );
    service.observation.snapshot.product_progress_rate_bps = Some(80_000_000.0);

    let mut proven = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    proven.observation.snapshot.product_progress_rate_bps = Some(200_000_000.0);
    proven.observation.snapshot.delivery_rate_bps = 200_000_000.0;
    proven.observation.snapshot.pacing_rate_bps = 200_000_000.0;
    proven.observation.snapshot.app_limited = false;

    let mut calibrating = response_target(
        2,
        UnderlayProtocol::Tcp,
        10.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    let stage = reliable_ack_clock_calibration_limit_bytes(mux_limits);
    calibrating.ack_clock_calibration_eligible = true;
    calibrating.ack_clock_calibration_active = true;
    calibrating.ack_clock_calibration_spent_bytes = stage;
    calibrating.ack_clock_calibration_credit_limit_bytes = stage;
    calibrating.ack_clock_calibration_max_limit_bytes = 2 * stage;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), proven, calibrating],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        service_horizon,
        None,
    )
    .expect("Service remains available while calibration waits for ACK evidence");

    assert_eq!(selected.target.observation.key, service.observation.key);
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);
}

#[test]
fn udp_service_remains_first_after_its_service_horizon() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let service = response_target(
        0,
        UnderlayProtocol::Udp,
        25.0,
        service_horizon as u64,
        mux_limits.max_path_flight_bytes as u64,
        true,
    );
    let measured_subflow = response_target(
        1,
        UnderlayProtocol::Udp,
        5.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        service_horizon,
        None,
    )
    .expect("UDP Service remains the packet-controller owner policy");
    assert_eq!(selected.target.observation.key, service.observation.key);
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);
}

#[test]
fn unproven_service_bootstraps_before_app_limited_proven_subflow() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
    service.observation.snapshot.product_queue_bytes = (2 * payload_bytes) as u64;
    service.observation.snapshot.app_limited = true;
    service.observation.has_bulk_rate_evidence = false;

    let mut proven_subflow =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    proven_subflow.observation.snapshot.app_limited = true;
    proven_subflow.observation.has_bulk_rate_evidence = true;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), proven_subflow],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        0,
        None,
    )
    .expect("the unproven live Service remains feedable");

    assert_eq!(selected.target.observation.key, service.observation.key);
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);
}

#[test]
fn feedable_service_precedes_less_committed_app_limited_subflow() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
    service.observation.snapshot.product_queue_bytes = (2 * payload_bytes) as u64;

    let mut underloaded =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    underloaded.observation.snapshot.app_limited = true;
    underloaded.observation.has_bulk_rate_evidence = true;

    let mut overloaded = response_target(2, UnderlayProtocol::Udp, 1.0, 0, 16 * 1024 * 1024, false);
    overloaded.observation.snapshot.product_queue_bytes = (4 * payload_bytes) as u64;
    overloaded.observation.snapshot.app_limited = true;
    overloaded.observation.has_bulk_rate_evidence = true;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), underloaded, overloaded],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        }),
        0,
        None,
    )
    .expect("feedable Service remains selected despite more committed work");

    assert_eq!(selected.target.observation.key, service.observation.key);
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);
}

#[test]
fn measured_same_family_alternate_is_subflow_when_service_is_not_feedable() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 25.0, 0, 16 * 1024 * 1024, true);
    let service_envelope = bulk_active_service_product_envelope_bytes(
        service.observation.snapshot,
        payload_bytes,
        mux_limits,
    );
    service.observation.snapshot.product_bytes_in_flight = service_envelope;
    service.observation.snapshot.queue_bytes = payload_bytes as u64;
    let measured_subflow =
        response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        0,
        None,
    )
    .expect("measured same-family path should remain an admissible Subflow when Service is not feedable");

    assert_eq!(
        selected.target.observation.key,
        measured_subflow.observation.key
    );
    assert_eq!(
        selected.admission().role,
        PathRuntimeRole::Subflow,
        "additional same-family owner bytes must be labeled Subflow, not Service"
    );
    assert!(
        selected.subflow_admission_selection().is_some(),
        "Subflow OwnerData must be committed through the Subflow admission ledger"
    );
}

#[test]
fn saturated_service_may_admit_one_startup_same_underlay_subflow_owner() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(
        0,
        UnderlayProtocol::Tcp,
        25.0,
        mux_limits.max_path_flight_bytes as u64,
        16 * 1024 * 1024,
        true,
    );
    service.observation.snapshot.product_progress_rate_bps = Some(180_000_000.0);
    service.observation.has_sender_evidence = true;
    service.observation.has_bulk_rate_evidence = true;
    service.observation.snapshot.active_flows = 2;
    let mut startup_subflow =
        response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    startup_subflow.observation.has_sender_evidence = true;
    startup_subflow.observation.has_bulk_rate_evidence = false;
    startup_subflow.observation.snapshot.product_queue_bytes =
        mux_limits.max_path_flight_bytes as u64;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), startup_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        0,
        None,
    );

    let selected =
        selected.expect("startup same-underlay Subflow should receive one owner quantum");
    assert_eq!(
        selected.target.observation.key,
        startup_subflow.observation.key
    );
    assert_eq!(selected.admission().role, PathRuntimeRole::Subflow);
    assert!(
        selected
            .subflow_admission_selection()
            .is_some_and(|commit| commit.input.startup_owner_allowed),
        "sender evidence permits only explicit bounded startup Subflow admission"
    );
}

#[test]
fn bulk_only_live_tcp_service_tail_admits_bounded_same_underlay_startup_sampling() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 25.0, 0, 16 * 1024 * 1024, true);
    service.observation.snapshot.product_progress_rate_bps = Some(180_000_000.0);
    service.observation.has_sender_evidence = true;
    service.observation.has_bulk_rate_evidence = true;
    service.observation.snapshot.active_flows = 2;
    let mut startup_subflow =
        response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    startup_subflow.observation.has_sender_evidence = true;
    startup_subflow.observation.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), startup_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        payload_bytes,
        None,
    )
    .expect("bounded TCP startup sampling should remain dispatchable behind a live Service suffix");

    assert_eq!(
        selected.target.observation.key,
        startup_subflow.observation.key
    );
    assert_eq!(selected.admission().role, PathRuntimeRole::Subflow);
    assert!(
        selected
            .subflow_admission_selection()
            .is_some_and(|commit| commit.input.startup_owner_allowed),
        "TCP startup sampling must be explicit and ledger-bounded"
    );
}

#[test]
fn quic_service_uses_bounded_startup_when_no_measured_subflow_exists() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
    service.observation.snapshot.product_progress_rate_bps = Some(180_000_000.0);
    service.observation.has_sender_evidence = true;
    service.observation.has_bulk_rate_evidence = true;
    service.observation.snapshot.active_flows = 2;
    let mut startup_subflow =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    startup_subflow.observation.has_sender_evidence = true;
    startup_subflow.observation.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), startup_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        payload_bytes,
        None,
    )
    .expect("one unmeasured QUIC path should receive bounded startup work");

    assert_eq!(
        selected.target.observation.key,
        startup_subflow.observation.key
    );
    assert_eq!(selected.admission().role, PathRuntimeRole::Subflow);
    assert!(
        selected
            .subflow_admission_selection()
            .is_some_and(|commit| commit.input.startup_owner_allowed)
    );
}

#[test]
fn sole_quic_service_does_not_sample_an_equally_loaded_path() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
    service.observation.snapshot.product_progress_rate_bps = Some(180_000_000.0);
    service.observation.snapshot.active_flows = 1;
    let mut validation = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    validation.observation.has_bulk_rate_evidence = false;
    validation.observation.snapshot.active_flows = 1;

    let selected = select_response_sender_data_target_with_ordered_debt_inner(
        &[service.clone(), validation],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        payload_bytes,
        None,
        true,
    )
    .expect("the equally loaded Service should remain dispatchable");

    assert_eq!(selected.target.observation.key, service.observation.key);
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);
    assert!(selected.subflow_admission_selection().is_none());
}

#[test]
fn latency_pressure_keeps_unmeasured_validation_path_out_of_owner_sampling() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
    service.observation.snapshot.product_progress_rate_bps = Some(180_000_000.0);
    service
        .observation
        .snapshot
        .session_active_latency_sensitive_flows = 1;
    let mut validation = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    validation.observation.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), validation.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        payload_bytes,
        None,
    )
    .expect("the Service path should remain dispatchable under latency pressure");

    assert_eq!(selected.target.observation.key, service.observation.key);
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);
    assert!(selected.subflow_admission_selection().is_none());
}

#[test]
fn repair_attachment_never_receives_startup_owner_sampling() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 25.0, 0, 16 * 1024 * 1024, true);
    service.observation.snapshot.product_progress_rate_bps = Some(180_000_000.0);
    let mut repair = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    repair.observation.attachment_role = StreamOpenRole::Repair;
    repair.observation.has_bulk_rate_evidence = true;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), repair],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        payload_bytes,
        None,
    )
    .expect("the Service path should remain dispatchable with a proven Repair attachment");

    assert_eq!(selected.target.observation.key, service.observation.key);
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);
}

#[test]
fn exact_startup_owner_continues_lower_frontier_within_multi_flow_cap() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let startup_credit =
        usize::try_from(reliable_subflow_startup_sample_limit_bytes(mux_limits)).unwrap();
    assert_eq!(startup_credit % payload_bytes, 0);

    let mut service = response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
    service.observation.snapshot.active_flows = 2;
    service.observation.has_bulk_rate_evidence = true;
    let mut startup_owner =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    startup_owner.observation.has_bulk_rate_evidence = false;

    let first = select_response_sender_data_target_with_ordered_debt_inner(
        &[service.clone(), startup_owner.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        0,
        None,
        true,
    )
    .expect("the first bounded startup quantum should be admitted");
    let input = first
        .subflow_admission_selection()
        .expect("startup admission must carry the exact epoch commit")
        .input;
    let mut partial_epoch = FlowSubflowSet::new(
        0,
        service.observation.key,
        startup_credit,
        0,
        Duration::ZERO,
    );
    assert_eq!(
        partial_epoch.admit_subflow_owner(input).decision,
        PathAdmissionDecision::AdmitSubflow
    );
    startup_owner.observation.snapshot.product_bytes_in_flight = payload_bytes as u64;
    startup_owner.observation.owner_data_in_flight_bytes = payload_bytes as u64;
    let startup_lower_flight = [CarrierPathFlightDebt {
        key: startup_owner.observation.key,
        bytes: payload_bytes as u64,
    }];

    assert!(
        select_response_sender_data_target_with_ordered_debt_inner(
            &[service.clone(), startup_owner.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &startup_lower_flight,
            Some(service.observation.key),
            payload_bytes,
            Some(&partial_epoch),
            false,
        )
        .is_none(),
        "an exact startup owner cannot bypass a disabled startup policy"
    );

    let continued = select_response_sender_data_target_with_ordered_debt_inner(
        &[service.clone(), startup_owner.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &startup_lower_flight,
        Some(service.observation.key),
        payload_bytes,
        Some(&partial_epoch),
        true,
    )
    .expect("the exact startup owner should continue its own lower frontier");
    assert_eq!(
        continued.target.observation.key,
        startup_owner.observation.key
    );
    assert_eq!(continued.admission().role, PathRuntimeRole::Subflow);

    let mut other_unmeasured =
        response_target(2, UnderlayProtocol::Udp, 4.0, 0, 16 * 1024 * 1024, false);
    other_unmeasured.observation.has_bulk_rate_evidence = false;
    let other_lower_flight = [CarrierPathFlightDebt {
        key: other_unmeasured.observation.key,
        bytes: payload_bytes as u64,
    }];
    assert!(
        select_response_sender_data_target_with_ordered_debt_inner(
            &[service.clone(), startup_owner.clone(), other_unmeasured],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &other_lower_flight,
            Some(service.observation.key),
            payload_bytes,
            Some(&partial_epoch),
            true,
        )
        .is_none(),
        "a different unmeasured lower owner cannot borrow the startup epoch"
    );

    let mut exhausted_epoch = partial_epoch;
    for _ in 1..(startup_credit / payload_bytes) {
        assert_eq!(
            exhausted_epoch.admit_subflow_owner(input).decision,
            PathAdmissionDecision::AdmitSubflow
        );
    }
    startup_owner.observation.snapshot.product_bytes_in_flight = startup_credit as u64;
    startup_owner.observation.owner_data_in_flight_bytes = startup_credit as u64;
    assert!(
        select_response_sender_data_target_with_ordered_debt_inner(
            &[service.clone(), startup_owner.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &startup_lower_flight,
            Some(service.observation.key),
            startup_credit,
            Some(&exhausted_epoch),
            true,
        )
        .is_none(),
        "an exhausted unproven startup owner must wait for its lower ACK hole"
    );

    let after_ack = select_response_sender_data_target_with_ordered_debt_inner(
        &[service.clone(), startup_owner],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        startup_credit,
        Some(&exhausted_epoch),
        true,
    )
    .expect("Service should resume after the exhausted startup hole clears");
    assert_eq!(after_ack.target.observation.key, service.observation.key);
    assert_eq!(after_ack.admission().role, PathRuntimeRole::Service);
}

#[test]
fn active_response_flow_may_start_one_bounded_same_family_sample() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
    service.observation.snapshot.product_progress_rate_bps = Some(180_000_000.0);
    service.observation.snapshot.active_flows = 1;
    let service_key = service.observation.key;
    let mut validation = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    validation.observation.has_bulk_rate_evidence = false;

    let no_active_work = select_response_sender_data_target_with_ordered_debt_inner(
        &[service.clone(), validation.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service_key),
        0,
        None,
        false,
    )
    .expect("the Service remains dispatchable while discovery is dormant");
    assert_eq!(
        no_active_work.target.observation.key,
        service.observation.key
    );
    assert_eq!(no_active_work.admission().role, PathRuntimeRole::Service);
    assert!(no_active_work.subflow_admission_selection().is_none());

    let active_response = select_response_sender_data_target_with_ordered_debt_inner(
        &[service, validation.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service_key),
        0,
        None,
        true,
    )
    .expect("one active response may spend the bounded startup sample");
    assert_eq!(
        active_response.target.observation.key,
        validation.observation.key
    );
    assert!(
        active_response
            .subflow_admission_selection()
            .is_some_and(|commit| commit.input.startup_owner_allowed)
    );
}

#[test]
fn startup_sample_cap_returns_dispatch_to_service() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let startup_credit =
        usize::try_from(reliable_subflow_startup_sample_limit_bytes(mux_limits)).unwrap();
    assert_eq!(startup_credit % payload_bytes, 0);

    let mut service = response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
    service.observation.snapshot.product_progress_rate_bps = Some(180_000_000.0);
    service.observation.snapshot.active_flows = 2;
    let mut validation = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    validation.observation.has_bulk_rate_evidence = false;
    let candidates = [&service, &validation];
    let lead = ResponseBulkLead {
        key: service.observation.key,
        snapshot: service.observation.snapshot,
        eta_ms: service.observation.eta_ms,
    };
    let outcome = response_target_unique_owner_admission_with_epoch(
        &validation,
        &candidates,
        lead,
        None,
        Some(service.observation.key),
        0,
        ResponseOrderedTail::new(Some(service.observation.key), payload_bytes)
            .for_candidate(validation.observation.key),
        payload_bytes,
        mux_limits,
        None,
        true,
        false,
    );
    let (_, subflow_selection, _, _) = outcome.into_parts();
    let input = subflow_selection
        .expect("first sample quantum should be admitted")
        .input;
    let mut epoch = FlowSubflowSet::new(
        0,
        service.observation.key,
        startup_credit,
        0,
        Duration::ZERO,
    );
    for _ in 0..(startup_credit / payload_bytes) {
        assert_eq!(
            epoch.admit_subflow_owner(input).decision,
            PathAdmissionDecision::AdmitSubflow
        );
    }

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), validation],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        payload_bytes,
        Some(&epoch),
    )
    .expect("Service should resume once startup sampling credit is exhausted");

    assert_eq!(selected.target.observation.key, service.observation.key);
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);
    assert!(selected.subflow_admission_selection().is_none());
}

#[test]
fn feedable_service_precedes_measured_subflow_under_bounded_tail_debt() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    service.observation.has_sender_evidence = true;
    service.observation.has_bulk_rate_evidence = true;
    let mut measured_subflow =
        response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    measured_subflow.observation.has_sender_evidence = true;
    measured_subflow.observation.has_bulk_rate_evidence = true;
    measured_subflow.observation.snapshot.app_limited = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        payload_bytes.saturating_mul(2),
        None,
    )
    .expect("feedable Service should remain selected under bounded tail debt");

    assert_eq!(selected.target.observation.key, service.observation.key);
    assert_eq!(
        selected.admission().role,
        PathRuntimeRole::Service,
        "measured Subflow remains overflow while Service has capacity"
    );
}

#[test]
fn response_owner_tail_guard_keeps_service_owner_feedable_under_pressure() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let owner = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
    let alternate = response_target(1, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, false);
    let owner_key = owner.observation.key;
    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[owner, alternate],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(owner_key),
        owner_tail_guard_bytes,
        None,
    )
    .expect("live Service owner must remain feedable under contiguous owner-tail guard");

    assert_eq!(
        selected.target.observation.key, owner_key,
        "contiguous owner-tail guard blocks alternates but must not starve the current Service owner"
    );
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);
}

#[test]
fn response_owner_tail_guard_uses_measured_same_underlay_when_service_queue_is_full() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut owner = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let alternate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let (owner_commands, _owner_rx) = reliable_path_command_channels(1);
    owner_commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id: StreamId(99),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::Throughput,
        )
        .expect("seed full owner data queue");
    observe_response_target_commands(&mut owner, &owner_commands);

    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[owner.clone(), alternate.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(owner.observation.key),
        owner_tail_guard_bytes,
        None,
    );
    let selected =
        selected.expect("measured same-underlay Subflow should remain eligible under tail debt");
    assert_eq!(selected.target.observation.key, alternate.observation.key);
    assert_eq!(
        selected.admission().role,
        PathRuntimeRole::Subflow,
        "queue backpressure on Service does not promote a new Service; it admits a measured same-underlay Subflow"
    );
}

#[test]
fn ordered_owner_debt_admits_measured_same_underlay_subflow_when_service_is_backpressured() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let (service_commands, _service_rx) = reliable_path_command_channels(1);
    service_commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id: StreamId(199),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::Throughput,
        )
        .expect("seed full stale Service data queue");
    observe_response_target_commands(&mut service, &service_commands);
    let survivor = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

    let selected = select_response_sender_data_target_with_ordered_debt_inner(
        &[service.clone(), survivor.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        owner_tail_guard_bytes,
        None,
        true,
    );

    let selected =
        selected.expect("measured same-underlay Subflow should pass tail-debt admission");
    assert_eq!(selected.target.observation.key, survivor.observation.key);
    assert_eq!(
        selected.admission().role,
        PathRuntimeRole::Subflow,
        "queue backpressure on a live Service owner is not Service failure; measured same-underlay work remains Subflow OwnerData"
    );
}

#[test]
fn ordered_owner_debt_keeps_live_service_owner_when_it_has_capacity() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 333.0, 0, 16 * 1024 * 1024, true);
    service.observation.has_sender_evidence = true;
    service.observation.has_bulk_rate_evidence = true;
    service.observation.snapshot.product_progress_rate_bps = Some(1_121_000.0);
    let survivor = response_target(1, UnderlayProtocol::Tcp, 712.0, 0, 16 * 1024 * 1024, false);
    let owner_tail_guard_bytes = payload_bytes.saturating_mul(58);

    let selected = select_response_sender_data_target_with_ordered_debt_inner(
        &[service.clone(), survivor],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.observation.key),
        owner_tail_guard_bytes,
        None,
        true,
    )
    .expect("ordered-owner debt must not suppress a live Service owner with emission credit");

    assert_eq!(
        selected.target.observation.key, service.observation.key,
        "ordered-owner debt must not eject a live owner and create no_admissible_lead"
    );
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);
}

#[test]
fn unresolved_ordered_owner_debt_does_not_grant_owner_bytes_to_unmeasured_survivor() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut stale_service =
        response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let (service_commands, _service_rx) = reliable_path_command_channels(1);
    service_commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id: StreamId(200),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::Throughput,
        )
        .expect("seed full stale Service data queue");
    observe_response_target_commands(&mut stale_service, &service_commands);
    let mut proof_only = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    proof_only.observation.has_sender_evidence = true;
    proof_only.observation.has_bulk_rate_evidence = false;
    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

    let selected = select_response_sender_data_target_with_ordered_debt_inner(
        &[stale_service.clone(), proof_only],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(stale_service.observation.key),
        owner_tail_guard_bytes,
        None,
        true,
    );

    assert!(
        selected.is_none(),
        "ordered-owner debt is not a proof shortcut; an unmeasured survivor remains Probe/Standby until path-scoped bulk evidence exists"
    );
}

#[test]
fn unresolved_ordered_owner_debt_blocks_active_liveness_survivor() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let stale_owner = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(2),
    };
    let mut active_validation =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);
    active_validation.observation.has_sender_evidence = true;
    active_validation.observation.has_bulk_rate_evidence = false;
    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

    let selected = select_response_sender_data_target_with_ordered_debt_inner(
        &[active_validation],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(stale_owner),
        owner_tail_guard_bytes,
        None,
        true,
    );

    assert!(
        selected.is_none(),
        "unresolved prior Service bytes block active validation/liveness from becoming Service OwnerData"
    );
}

#[test]
fn clear_frontier_stale_owner_hint_does_not_block_liveness_service_failover() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut stale_owner =
        response_target(2, UnderlayProtocol::Tcp, 500.0, 0, 16 * 1024 * 1024, false);
    stale_owner.observation.has_sender_evidence = true;
    stale_owner.observation.has_bulk_rate_evidence = false;
    let mut survivor = response_target(3, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, false);
    survivor.observation.has_sender_evidence = true;
    survivor.observation.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[stale_owner.clone(), survivor.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(stale_owner.observation.key),
        0,
        None,
    )
    .expect("with no active Service and a clear frontier, sender-evidence survivors may elect exactly one liveness Service");

    assert_eq!(
        selected.target.observation.key, survivor.observation.key,
        "a stale ordered-owner hint without unresolved bytes must not pin Service ownership to a worse proof-only path"
    );
    assert_eq!(
        selected.admission().role,
        PathRuntimeRole::Service,
        "liveness failover elects one Service owner; it must not admit optional Subflow ownership"
    );
}

#[test]
fn clear_frontier_ownerless_stream_elects_measured_service() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let survivor = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

    let selected = select_response_sender_data_target_with_ordered_debt_inner(
        std::slice::from_ref(&survivor),
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        None,
        0,
        None,
        true,
    )
    .expect("frontier-clear ownerless stream may elect a measured survivor as Service");

    assert_eq!(selected.target.observation.key, survivor.observation.key);
    assert_eq!(
        selected.admission().role,
        PathRuntimeRole::Service,
        "ownerless failover elects a new Service, not an optional Subflow behind missing-owner debt"
    );
}

#[test]
fn response_owner_tail_guard_admits_measured_same_underlay_when_service_over_budget() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload_bytes = 64 * 1024usize;
    let mut owner = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let service_envelope = bulk_active_service_product_envelope_bytes(
        owner.observation.snapshot,
        payload_bytes,
        mux_limits,
    );
    owner.observation.snapshot.product_bytes_in_flight = service_envelope;
    owner.observation.snapshot.queue_bytes = payload_bytes as u64;
    let alternate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[owner.clone(), alternate],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(owner.observation.key),
        owner_tail_guard_bytes,
        None,
    )
    .expect("measured same-underlay Subflow should remain eligible under bounded tail debt");
    assert_eq!(
        selected.admission().role,
        PathRuntimeRole::Subflow,
        "owner-tail debt is accounted as ordering risk, not an absolute same-underlay Subflow ban"
    );
}

#[test]
fn response_owner_tail_guard_blocks_cross_underlay_when_owner_queue_is_full() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut owner = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let alternate = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    let (owner_commands, _owner_rx) = reliable_path_command_channels(1);
    owner_commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id: StreamId(99),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::Throughput,
        )
        .expect("seed full owner data queue");
    observe_response_target_commands(&mut owner, &owner_commands);

    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

    assert!(
        select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[owner.clone(), alternate],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(owner.observation.key),
            owner_tail_guard_bytes,
            None,
        )
        .is_none(),
        "owner-debt fallback must not migrate ordered bytes across TCP/QUIC families"
    );
}

#[test]
fn cross_underlay_alternate_waits_when_service_owner_is_backpressured() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let assigned_bytes = bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits)
        .saturating_sub(payload_bytes);
    let owner = response_target(
        1,
        UnderlayProtocol::Tcp,
        50.0,
        assigned_bytes as u64,
        0,
        true,
    );
    let alternate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);
    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[owner.clone(), alternate],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(owner.observation.key),
        owner_tail_guard_bytes,
        None,
    );

    let selected = selected.expect("feedable Service owner should remain selected under tail debt");
    assert_eq!(
        selected.target.observation.key, owner.observation.key,
        "a cross-underlay alternate must not own later bytes while the current Service owner has unresolved contiguous tail"
    );
}

#[test]
fn response_owner_tail_guard_blocks_proof_only_same_family_subflow() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut owner = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let mut alternate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    alternate.observation.has_sender_evidence = true;
    alternate.observation.has_bulk_rate_evidence = false;
    let (owner_commands, _owner_rx) = reliable_path_command_channels(1);
    owner_commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id: StreamId(99),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::Throughput,
        )
        .expect("seed full owner data queue");
    observe_response_target_commands(&mut owner, &owner_commands);

    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

    assert!(
        select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[owner.clone(), alternate],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(owner.observation.key),
            owner_tail_guard_bytes,
            None,
        )
        .is_none(),
        "proof-only paths must stay Probe/Standby while older owner debt is unresolved"
    );
}

#[test]
fn response_small_owner_debt_keeps_feedable_service_ahead_of_measured_subflow() {
    let owner = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let lower_eta_alternate =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[owner.clone(), lower_eta_alternate.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(owner.observation.key),
        64 * 1024,
        None,
    )
    .expect("feedable Service should pass bounded tail-debt admission");

    assert_eq!(
        selected.target.observation.key, owner.observation.key,
        "small Service-tail debt must not displace a feedable Service with optional same-underlay work"
    );
    assert_eq!(
        selected.admission().role,
        PathRuntimeRole::Service,
        "the lower-ETA same-underlay path remains Subflow overflow"
    );
}

#[test]
fn small_ordered_owner_debt_blocks_cross_underlay_service_migration() {
    let owner = response_target(1, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let active_cross_underlay =
        response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[owner.clone(), active_cross_underlay],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(owner.observation.key),
        64 * 1024,
        None,
    );

    assert!(
        selected.is_none()
            || selected
                .as_ref()
                .is_some_and(|selected| selected.target.observation.key == owner.observation.key),
        "any unresolved ordered-owner tail must block TCP/QUIC Service migration until the frontier clears or the candidate already owns the lower range"
    );
}

#[test]
fn ordered_owner_debt_blocks_fallback_service_when_owner_target_is_absent() {
    let missing_owner = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let active_cross_underlay =
        response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[active_cross_underlay],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(missing_owner),
        64 * 1024,
        None,
    );

    assert!(
        selected.is_none(),
        "an absent ordered owner with unresolved lower bytes must trigger repair/failover handling, not make another underlay the Service owner for later bytes"
    );
}

#[test]
fn missing_same_underlay_owner_debt_admits_measured_service_failover() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let missing_owner = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let measured_survivor =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        std::slice::from_ref(&measured_survivor),
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(missing_owner),
        payload_bytes.saturating_mul(2),
        None,
    )
    .expect("a bulk-rate-proven same-underlay survivor should elect Service failover when the previous Service output is gone and no lower-flight owner remains");

    assert_eq!(
        selected.target.observation.key,
        measured_survivor.observation.key
    );
    assert_eq!(
        selected.admission().role,
        PathRuntimeRole::Service,
        "same-underlay failover resumes Service OwnerData; it is not optional Subflow exploration and does not credit RepairData as proof"
    );
}

#[test]
fn missing_same_underlay_service_failover_respects_path_latency_window() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let missing_owner = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let mut measured_survivor = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    measured_survivor.observation.snapshot.delivery_rate_bps = 10_000_000_000.0;
    measured_survivor.observation.snapshot.pacing_rate_bps = 10_000_000_000.0;
    measured_survivor
        .observation
        .snapshot
        .active_latency_sensitive_flows = 1;
    let latency_credit = usize::try_from(bulk_latency_pressure_service_feed_window_bytes(
        payload_bytes,
        mux_limits,
    ))
    .unwrap();
    measured_survivor
        .observation
        .snapshot
        .product_bytes_in_flight = latency_credit.saturating_sub(payload_bytes) as u64;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        std::slice::from_ref(&measured_survivor),
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(missing_owner),
        payload_bytes.saturating_mul(2),
        None,
    )
    .expect("mature same-underlay Service failover may consume remaining latency-window credit");
    assert_eq!(selected.admission().role, PathRuntimeRole::Service);

    measured_survivor
        .observation
        .snapshot
        .product_bytes_in_flight = latency_credit as u64;
    assert!(
        select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[measured_survivor],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(missing_owner),
            payload_bytes.saturating_mul(2),
            None,
        )
        .is_none(),
        "runtime Service failover must stop at the same path-local latency window even when its bulk role is AdditionalSameUnderlay"
    );
}

#[test]
fn missing_same_underlay_owner_debt_admits_sender_evidence_service_failover() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let missing_owner = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let mut liveness_survivor =
        response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    liveness_survivor.observation.has_sender_evidence = true;
    liveness_survivor.observation.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        std::slice::from_ref(&liveness_survivor),
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(missing_owner),
        payload_bytes.saturating_mul(2),
        None,
    )
    .expect("a same-underlay sender-evidenced survivor should receive bounded Service failover when the previous Service output is gone and no lower-flight owner remains");

    assert_eq!(
        selected.target.observation.key,
        liveness_survivor.observation.key
    );
    assert_eq!(
        selected.admission().role,
        PathRuntimeRole::Service,
        "same-underlay failover is Service continuation, not Subflow aggregation"
    );
    assert!(
        selected.subflow_admission_selection().is_none(),
        "failover Service election must not spend Subflow owner credit"
    );
}

#[test]
fn ordered_owner_debt_without_owner_hint_blocks_active_fallback_service() {
    let active_cross_underlay =
        response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[active_cross_underlay],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        None,
        64 * 1024,
        None,
    );

    assert!(
        selected.is_none(),
        "ordered-owner debt without an owner hint must not fall back to the active path as Service"
    );
}

#[test]
fn proof_only_active_service_can_continue_under_its_own_tail_guard() {
    let mut active_fallback =
        response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);
    active_fallback.observation.has_sender_evidence = true;
    active_fallback.observation.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[active_fallback.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(active_fallback.observation.key),
        315_680,
        None,
    );

    let selected =
        selected.expect("the live active Service owner may continue under its own tail guard");
    assert_eq!(
        selected.target.observation.key,
        active_fallback.observation.key
    );
    assert_eq!(
        selected.admission().role,
        PathRuntimeRole::Service,
        "tail guard must not turn active Service OwnerData into Subflow exploration"
    );
}

#[test]
fn bulk_only_tcp_sender_evidence_admits_startup_subflow_not_service() {
    let mut owner = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    owner.observation.snapshot.active_flows = 2;
    let mut lower_eta_alternate =
        response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    lower_eta_alternate.observation.has_sender_evidence = true;
    lower_eta_alternate.observation.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[owner.clone(), lower_eta_alternate.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(owner.observation.key),
        0,
        None,
    )
    .expect("current Service owner should remain eligible");

    assert_eq!(
        selected.target.observation.key, lower_eta_alternate.observation.key,
        "sender evidence may start one bounded same-underlay Subflow sampling epoch"
    );
    assert_eq!(
        selected.admission().role,
        PathRuntimeRole::Subflow,
        "startup owner bytes are Subflow OwnerData and must not migrate Service ownership"
    );
    assert!(
        selected
            .subflow_admission_selection()
            .is_some_and(|commit| commit.input.startup_owner_allowed),
        "startup Subflow admission must be explicit and bounded"
    );
}

#[test]
fn cross_underlay_candidate_does_not_displace_owner_without_bulk_rate() {
    let owner = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    let mut candidate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    candidate.observation.has_bulk_rate_evidence = false;

    let selected = choose_response_sender_data_target(
        &[owner.clone(), candidate],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(owner.observation.key),
    )
    .expect(
        "current service owner should remain eligible while cross-family candidate is unproven",
    );

    assert_eq!(selected.observation.key, owner.observation.key);
}

#[test]
fn cross_underlay_bulk_rate_candidate_does_not_become_service_at_clear_frontier() {
    let owner = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    let candidate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

    let selected = choose_response_sender_data_target(
        &[owner.clone(), candidate.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(owner.observation.key),
    )
    .expect("current Service owner should remain eligible at a clear frontier");

    assert_eq!(
        selected.observation.key, owner.observation.key,
        "mixed-family Service migration must be explicit; lower-ETA cross-underlay candidates do not become Service through per-quantum selection"
    );
}

#[test]
fn cross_underlay_candidate_does_not_become_service_when_owner_hint_is_missing() {
    let owner = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    let candidate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

    let selected = choose_response_sender_data_target(
        &[owner.clone(), candidate.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        None,
    )
    .expect(
        "active Service output should anchor family ownership even if the owner hint was cleared",
    );

    assert_eq!(
        selected.observation.key, owner.observation.key,
        "a missing ordered-owner hint is not permission for implicit cross-family Service migration while an active Service output is live"
    );
}

#[test]
fn cross_underlay_bulk_rate_candidate_that_owns_lower_flight_remains_eligible() {
    let service = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    let candidate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let lower_flights = vec![CarrierPathFlightDebt {
        key: candidate.observation.key,
        bytes: 64 * 1024,
    }];

    let selected = choose_response_sender_data_target(
        &[service.clone(), candidate.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &lower_flights,
        Some(service.observation.key),
    )
    .expect("candidate owning the lower flight should remain eligible");

    assert_eq!(
        selected.observation.key, candidate.observation.key,
        "a bulk-rate-proven path that already owns the lower range must not be blocked by a stale cross-family frontier check"
    );
}

#[test]
fn active_cross_underlay_path_that_owns_lower_flight_remains_service_candidate() {
    let mut old_service =
        response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, false);
    old_service.observation.has_bulk_rate_evidence = true;
    let mut lower_active =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);
    lower_active.observation.has_sender_evidence = true;
    lower_active.observation.has_bulk_rate_evidence = false;
    let lower_flights = vec![CarrierPathFlightDebt {
        key: lower_active.observation.key,
        bytes: 64 * 1024,
    }];

    let selected = choose_response_sender_data_target(
        &[old_service.clone(), lower_active.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &lower_flights,
        Some(old_service.observation.key),
    )
    .expect("active lower-owner path must remain eligible to advance its own frontier");

    assert_eq!(
        selected.observation.key, lower_active.observation.key,
        "mixed-family health gates must not remove the active path that already owns unresolved lower bytes"
    );
}

#[test]
fn owner_tail_guard_keeps_cross_underlay_candidate_that_owns_lower_flight() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    let candidate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let lower_flights = vec![CarrierPathFlightDebt {
        key: candidate.observation.key,
        bytes: payload_bytes as u64,
    }];
    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), candidate.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &lower_flights,
        Some(service.observation.key),
        owner_tail_guard_bytes,
        None,
    )
    .expect("candidate owning the lower flight should survive tail-guard filtering");

    assert_eq!(
        selected.target.observation.key, candidate.observation.key,
        "tail guard must filter by candidate ordering safety, not by carrier family alone"
    );
}
