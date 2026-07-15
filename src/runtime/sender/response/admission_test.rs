use super::*;
use crate::model::admission::bulk_candidate_admission_suppression_with_ordering_debt;
use crate::model::multipath::PathAdmission;
use crate::runtime::sender::response::test_support::response_target;

#[test]
fn active_quic_response_owner_emission_credit_uses_product_envelope_not_carrier_cwnd() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut active = response_target(0, UnderlayProtocol::Udp, 5.0, 0, payload_bytes as u64, true);
    active.observation.snapshot.inflight_limit_bytes = payload_bytes as u64;

    let credit = response_target_emission_credit_bytes(
        &active,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(
        credit,
        bulk_active_service_product_envelope_bytes(payload_bytes, mux_limits) as usize,
        "active response owner must use the product envelope, not current carrier cwnd"
    );
    assert!(
        credit > payload_bytes,
        "the regression requires credit above one carrier quantum"
    );
}

#[test]
fn active_tcp_response_owner_without_bulk_evidence_uses_startup_credit_not_full_envelope() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut active = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    active.observation.has_sender_evidence = false;
    active.observation.has_service_feed_evidence = false;
    active.observation.has_bulk_rate_evidence = false;

    let credit = response_target_emission_credit_bytes(
        &active,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(
        credit,
        bulk_service_horizon_payload_bytes(payload_bytes, mux_limits),
        "unproven active Service startup must be bounded until path-scoped bulk-rate evidence exists"
    );
    assert!(
        credit >= payload_bytes,
        "startup Service credit must still admit at least one bulk quantum"
    );

    active.observation.snapshot.product_bytes_in_flight = credit as u64;
    assert!(
        !response_service_has_assigned_owner_credit(
            &active,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        ),
        "startup credit bounds cumulative assigned flight, not only the draining writer queue"
    );
}

#[test]
fn active_quic_response_owner_bootstraps_with_bounded_feed_reservoir() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut active = response_target(0, UnderlayProtocol::Udp, 360.0, 0, 16 * 1024 * 1024, true);
    active.observation.has_sender_evidence = true;
    active.observation.has_service_feed_evidence = false;
    active.observation.has_bulk_rate_evidence = false;

    let credit = response_target_emission_credit_bytes(
        &active,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(
        credit,
        bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits),
        "QUIC needs a durable ACK-derived sample before the full product envelope"
    );
    assert!(
        credit < bulk_active_service_product_envelope_bytes(payload_bytes, mux_limits,) as usize
    );

    active.observation.has_service_feed_evidence = true;
    let mature_feed_credit = response_target_emission_credit_bytes(
        &active,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );
    assert_eq!(
        mature_feed_credit,
        bulk_active_service_product_envelope_bytes(payload_bytes, mux_limits) as usize,
        "durable current-Service QUIC ACK progress unlocks the product envelope"
    );
    assert!(
        !active.observation.has_bulk_rate_evidence,
        "current-Service feed evidence must not grant optional Subflow or handoff authority"
    );
}

#[test]
fn response_quic_feed_credit_uses_live_carrier_debt_not_outdated_bdp() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = 64 * 1024usize;
    let mut loaded_quic = response_target(0, UnderlayProtocol::Udp, 250.0, 0, 64 * 1024, true);
    loaded_quic.observation.snapshot.delivery_rate_bps = 351_000.0;
    loaded_quic.observation.snapshot.pacing_rate_bps = 351_000.0;
    loaded_quic.observation.snapshot.product_progress_rate_bps = Some(10_000_000.0);
    loaded_quic.observation.snapshot.bytes_in_flight = 8 * 1024 * 1024;
    loaded_quic.observation.snapshot.queue_bytes = 1024 * 1024;

    let quic_credit = response_target_emission_credit_bytes(
        &loaded_quic,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );
    let outdated_bdp_credit = adaptive_reliable_relay_inflight_bytes(
        Some(loaded_quic.observation.snapshot),
        FlowLane::Throughput,
        mux_limits,
    );

    assert_eq!(
        quic_credit,
        bulk_active_service_product_envelope_bytes(payload_bytes, mux_limits,) as usize,
        "active QUIC Service feed credit must follow the product envelope, not live carrier debt"
    );
    assert!(
        quic_credit > outdated_bdp_credit,
        "app-limited BDP must not be the only active QUIC Service writer-feed ceiling"
    );

    let mut loaded_tcp = response_target(1, UnderlayProtocol::Tcp, 250.0, 0, 64 * 1024, true);
    loaded_tcp.observation.snapshot.delivery_rate_bps = 351_000.0;
    loaded_tcp.observation.snapshot.pacing_rate_bps = 351_000.0;
    loaded_tcp.observation.snapshot.bytes_in_flight = 8 * 1024 * 1024;
    loaded_tcp.observation.snapshot.queue_bytes = 1024 * 1024;
    loaded_tcp.observation.snapshot.product_progress_rate_bps = Some(351_000.0);
    let tcp_credit = response_target_emission_credit_bytes(
        &loaded_tcp,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(
        tcp_credit,
        bulk_active_service_product_envelope_bytes(payload_bytes, mux_limits,) as usize,
        "active TCP owners use the same carrier-neutral product envelope as active QUIC owners"
    );

    let mut subflow_quic = response_target(2, UnderlayProtocol::Udp, 250.0, 0, 64 * 1024, false);
    subflow_quic.observation.snapshot.delivery_rate_bps = 351_000.0;
    subflow_quic.observation.snapshot.pacing_rate_bps = 351_000.0;
    subflow_quic.observation.snapshot.bytes_in_flight = 8 * 1024 * 1024;
    subflow_quic.observation.snapshot.queue_bytes = 1024 * 1024;
    let subflow_credit = response_target_emission_credit_bytes(
        &subflow_quic,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );

    assert!(
        subflow_credit >= 8 * 1024 * 1024,
        "Subflow QUIC paths remain carrier-debt gated rather than borrowing the active owner envelope"
    );
}

#[test]
fn active_tcp_response_owner_uses_product_envelope() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut target = response_target(
        0,
        UnderlayProtocol::Tcp,
        50.0,
        0,
        payload_bytes as u64,
        true,
    );
    target.observation.snapshot.product_progress_rate_bps = Some(10_000_000_000.0);

    assert_eq!(
        response_target_emission_credit_bytes(
            &target,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits
        ),
        bulk_active_service_product_envelope_bytes(payload_bytes, mux_limits) as usize,
        "active TCP and QUIC owners should use the same product envelope; transport pacing belongs below the sender service"
    );
}

#[test]
fn proof_only_fallback_lead_cannot_become_response_service_owner() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut proof_only = response_target(
        1,
        UnderlayProtocol::Udp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    proof_only.observation.has_sender_evidence = true;
    proof_only.observation.has_bulk_rate_evidence = false;
    let lead = ResponseBulkLead {
        key: proof_only.observation.key,
        snapshot: proof_only.observation.snapshot,
        eta_ms: proof_only.observation.eta_ms,
    };

    let admission = response_target_unique_owner_admission(
        &proof_only,
        &[&proof_only],
        lead,
        None,
        0,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(
        admission,
        PathAdmission::ProbeOnly,
        "sender/proof evidence is not Service ownership; only an active anchor or bulk-rate-proven failover may own the Service role"
    );
    assert_eq!(admission, PathAdmission::ProbeOnly);
}

#[test]
fn proof_only_validation_candidate_gets_explicit_startup_admission() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut active = response_target(
        0,
        UnderlayProtocol::Udp,
        100.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    active.observation.snapshot.active_flows = 2;
    let mut proof_only = response_target(
        1,
        UnderlayProtocol::Udp,
        50.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    proof_only.observation.has_bulk_rate_evidence = false;
    proof_only.observation.has_sender_evidence = true;
    let candidates = vec![&active, &proof_only];
    let lead = ResponseBulkLead {
        key: active.observation.key,
        snapshot: active.observation.snapshot,
        eta_ms: active.observation.eta_ms,
    };

    let admission = response_target_unique_owner_admission(
        &proof_only,
        &candidates,
        lead,
        None,
        0,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(admission, PathAdmission::Subflow);
    assert_eq!(admission, PathAdmission::Subflow);
}

#[test]
fn frontier_clear_bulk_rate_candidate_is_subflow_not_service() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let active = response_target(0, UnderlayProtocol::Udp, 80.0, 0, 16 * 1024 * 1024, true);
    let alternate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let candidates = vec![&active, &alternate];
    let lead = ResponseBulkLead {
        key: alternate.observation.key,
        snapshot: alternate.observation.snapshot,
        eta_ms: alternate.observation.eta_ms,
    };

    let admission = response_target_unique_owner_admission(
        &alternate,
        &candidates,
        lead,
        None,
        0,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(admission, PathAdmission::Subflow);
    assert_eq!(admission, PathAdmission::Subflow);
}

#[test]
fn tcp_reservoir_subtracts_only_unique_owner_not_queue_or_repair() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let overflow = 1024 * 1024;
    let candidate_owner_bytes = 128 * 1024;
    let candidate_product_copies = 2 * 1024 * 1024;
    let service = response_target(
        0,
        UnderlayProtocol::Tcp,
        25.0,
        service_horizon as u64,
        mux_limits.max_path_flight_bytes as u64,
        true,
    );
    let mut candidate = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        candidate_product_copies,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    candidate.observation.owner_data_in_flight_bytes = candidate_owner_bytes;
    candidate.observation.snapshot.queue_bytes = (3 * 1024 * 1024) as u64;
    let tail = ResponseOrderedTail::new(Some(service.observation.key), service_horizon + overflow);
    let reservoir = ResponseSameFamilyReservoir::new(
        service.observation.key,
        tail,
        service_horizon as u64,
        service_horizon,
        bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits),
        payload_bytes,
    )
    .expect("global reservoir has credit");

    let debt = response_same_family_reservoir_candidate_debt(reservoir, &candidate);
    assert_eq!(
        debt.external_bytes(),
        (overflow - candidate_owner_bytes as usize) as u64
    );
    assert_eq!(
        debt.external_bytes() + candidate.observation.snapshot.product_bytes_in_flight,
        (overflow + candidate_product_copies as usize - candidate_owner_bytes as usize) as u64,
        "shared queue pressure and duplicate RepairData cannot erase unique tail exposure"
    );
}

#[test]
fn app_limited_bulk_proven_slow_subflow_still_requires_completion_gain() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);
    service.observation.snapshot.product_progress_rate_bps = Some(120_000_000.0);
    let mut slow_subflow =
        response_target(1, UnderlayProtocol::Udp, 500.0, 0, 16 * 1024 * 1024, false);
    slow_subflow.observation.snapshot.product_progress_rate_bps = Some(20_000_000.0);
    slow_subflow.observation.snapshot.app_limited = true;
    slow_subflow.observation.has_bulk_rate_evidence = true;
    let candidates = [&service, &slow_subflow];
    let lead = ResponseBulkLead {
        key: service.observation.key,
        snapshot: service.observation.snapshot,
        eta_ms: service.observation.eta_ms,
    };

    let admission = response_target_unique_owner_admission(
        &slow_subflow,
        &candidates,
        lead,
        None,
        0,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(admission, PathAdmission::Standby);
}

#[test]
fn tcp_response_startup_does_not_double_count_global_ordered_tail() {
    let mut mux_limits = MuxLimits::default();
    mux_limits.max_path_flight_bytes = 2 * 1024 * 1024;
    mux_limits.max_repair_bytes = 2 * 1024 * 1024;
    mux_limits.max_reorder_bytes = 2 * 1024 * 1024;
    mux_limits.max_stream_window_bytes = 2 * 1024 * 1024;
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 2 * 1024 * 1024, true);
    service.observation.snapshot.active_flows = 1;
    let mut candidate = response_target(1, UnderlayProtocol::Tcp, 500.0, 0, 2 * 1024 * 1024, false);
    let committed = 2 * 1024 * 1024 - payload_bytes as u64;
    candidate.observation.snapshot.product_bytes_in_flight = committed;
    candidate.observation.has_bulk_rate_evidence = false;

    assert!(response_target_is_startup_same_underlay_subflow_candidate(
        service.observation.key,
        &service,
        &candidate,
        committed,
        payload_bytes,
        mux_limits,
    ));
}

#[test]
fn app_limited_bulk_proven_fast_subflow_can_still_improve_completion() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    service.observation.snapshot.product_progress_rate_bps = Some(20_000_000.0);
    let mut fast_subflow =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    fast_subflow.observation.snapshot.product_progress_rate_bps = Some(120_000_000.0);
    fast_subflow.observation.snapshot.app_limited = true;
    fast_subflow.observation.has_bulk_rate_evidence = true;
    let candidates = [&service, &fast_subflow];
    let lead = ResponseBulkLead {
        key: service.observation.key,
        snapshot: service.observation.snapshot,
        eta_ms: service.observation.eta_ms,
    };

    let admission = response_target_unique_owner_admission(
        &fast_subflow,
        &candidates,
        lead,
        None,
        0,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(admission, PathAdmission::Subflow);
}

#[test]
fn active_attachment_without_bulk_evidence_remains_service_anchor_when_measured_subflow_exists() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut active_attachment =
        response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    active_attachment.observation.has_bulk_rate_evidence = false;
    let measured_lead = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    let candidates = vec![&active_attachment, &measured_lead];
    let lead = ResponseBulkLead {
        key: measured_lead.observation.key,
        snapshot: measured_lead.observation.snapshot,
        eta_ms: measured_lead.observation.eta_ms,
    };

    let admission = response_target_unique_owner_admission(
        &active_attachment,
        &candidates,
        lead,
        None,
        0,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(
        admission,
        PathAdmission::Service,
        "the active attachment remains the Service anchor; measured alternates are Subflows"
    );
}

#[test]
fn measured_subflow_requires_later_startup_candidate_to_beat_service_reservoir() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(
        0,
        UnderlayProtocol::Tcp,
        250.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        true,
    );
    let mut measured = response_target(
        1,
        UnderlayProtocol::Tcp,
        400.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    measured.observation.snapshot.app_limited = false;
    let mut cold = response_target(
        2,
        UnderlayProtocol::Tcp,
        10_000.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    cold.observation.has_bulk_rate_evidence = false;

    assert!(
        response_startup_sample_has_completion_opportunity(
            &[&service, &cold],
            &service,
            &cold,
            payload_bytes,
            mux_limits,
        ),
        "the first bounded candidate remains the non-circular discovery bootstrap"
    );
    assert!(
        !response_startup_sample_has_completion_opportunity(
            &[&service, &measured, &cold],
            &service,
            &cold,
            payload_bytes,
            mux_limits,
        ),
        "after useful capacity exists, a cold candidate cannot create a much slower ordered prefix"
    );
}

#[test]
fn response_fallback_preserves_lower_flight_completion_backlog() {
    let payload_bytes = 64 * 1024;
    let mux_limits = MuxLimits::default();
    let mut service = response_target(0, UnderlayProtocol::Tcp, 400.0, 0, 16 * 1024 * 1024, true);
    service.observation.snapshot.srtt_ms = 360.0;
    service.observation.snapshot.delivery_rate_bps = 400_000_000.0;
    service.observation.snapshot.pacing_rate_bps = 400_000_000.0;
    let mut candidate =
        response_target(1, UnderlayProtocol::Tcp, 410.0, 0, 16 * 1024 * 1024, false);
    candidate.observation.snapshot.srtt_ms = 360.0;
    candidate.observation.snapshot.delivery_rate_bps = 200_000_000.0;
    candidate.observation.snapshot.pacing_rate_bps = 200_000_000.0;
    candidate.observation.snapshot.app_limited = false;
    let lead = ResponseBulkLead {
        key: service.observation.key,
        snapshot: service.observation.snapshot,
        eta_ms: service.observation.eta_ms,
    };
    let lower_flight_bytes = 8 * 1024 * 1024;
    let check = BulkAdmissionCheck {
        best_snapshot: lead.snapshot,
        best_eta_ms: lead.eta_ms,
        candidate_snapshot: candidate.observation.snapshot,
        candidate_eta_ms: candidate.observation.eta_ms,
        payload_bytes,
        mux_limits,
        role: BulkAdmissionRole::AdditionalSameUnderlay,
        stream_ordering_debt_bytes: lower_flight_bytes,
    };

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(check),
        Some("same_underlay_no_completion_gain"),
        "request receive-hole policy must not infer Service backlog"
    );
    assert_eq!(
        response_fallback_bulk_model_suppression(
            &candidate,
            lead,
            lower_flight_bytes,
            payload_bytes,
            mux_limits,
            BulkAdmissionRole::AdditionalSameUnderlay,
        ),
        None,
        "response lower flight is real completion backlog and must retain the 8c response policy"
    );
}
