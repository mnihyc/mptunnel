use super::*;
use crate::protocol::{PathId, UnderlayProtocol};

fn mbps(value: f64) -> f64 {
    value * 1_000_000.0
}

fn candidate(index: usize, eta_ms: f64, srtt_ms: f64, rate_mbps: f64) -> BulkPathCandidate {
    BulkPathCandidate {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index,
        },
        eta_ms,
        has_liveness_evidence: true,
        has_path_proof_evidence: false,
        has_ack_data_evidence: true,
        has_bulk_rate_evidence: true,
        has_sender_delivery_evidence: true,
        snapshot: PathSnapshot::new(
            PathId(index as u16),
            UnderlayProtocol::Udp,
            srtt_ms,
            mbps(rate_mbps),
        ),
    }
}

#[test]
fn exploration_completion_separates_useful_uncertainty_from_ordering_stalls() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = 64 * 1024;
    let useful_service = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 333.0, mbps(18.561));
    let useful_candidate =
        PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 730.287, mbps(1.007));
    let useful = bulk_exploration_completion_projection(
        useful_service,
        1_098.657,
        useful_candidate,
        1_406.704,
        183_802,
        payload_bytes,
        mux_limits,
    );
    assert!(useful.completes_within_service_reservoir());

    let stalled_service = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 333.0, mbps(47.429));
    let stalled_candidate =
        PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 891.787, mbps(1.342));
    let stalled = bulk_exploration_completion_projection(
        stalled_service,
        575.508,
        stalled_candidate,
        836.595,
        299_176,
        payload_bytes,
        mux_limits,
    );
    assert!(!stalled.completes_within_service_reservoir());
}

#[test]
fn bulk_admission_allows_same_underlay_candidate_that_beats_lead_next_quantum() {
    let admitted = bulk_striping_admitted_subflows(
        vec![
            candidate(0, 1000.0, 250.0, 100.0),
            candidate(1, 1004.0, 260.0, 100.0),
        ],
        64 * 1024,
        MuxLimits::default(),
    );

    assert_eq!(admitted.len(), 2);
    assert_eq!(admitted[1].key.index, 1);
}

#[test]
fn same_underlay_candidate_with_sub_quantum_debt_still_gets_one_service_quantum() {
    let payload_bytes = 512 * 1024;
    let best = candidate(0, 1000.0, 80.0, 200.0);
    let mut alternate = candidate(1, 1001.0, 80.0, 200.0);
    alternate.snapshot.inflight_limit_bytes = payload_bytes as u64;
    alternate.snapshot.bytes_in_flight = 34;
    alternate.snapshot.product_bytes_in_flight = 34;

    let admitted =
        bulk_striping_admitted_subflows(vec![best, alternate], payload_bytes, MuxLimits::default());

    assert_eq!(
        admitted
            .iter()
            .map(|candidate| candidate.key.index)
            .collect::<Vec<_>>(),
        vec![0, 1],
        "same-underlay admission is quantum-granular: tiny existing product debt should not suppress one otherwise-admissible service quantum"
    );
}

#[test]
fn cross_underlay_bulk_admission_rejects_candidate_outside_completion_horizon() {
    let mut cross_underlay = candidate(1, 5000.0, 260.0, 250.0);
    cross_underlay.key.underlay = UnderlayProtocol::Tcp;
    cross_underlay.snapshot.underlay = UnderlayProtocol::Tcp;
    let admitted = bulk_striping_admitted_subflows(
        vec![candidate(0, 1000.0, 250.0, 300.0), cross_underlay],
        64 * 1024,
        MuxLimits::default(),
    );

    assert_eq!(admitted.len(), 1);
    assert_eq!(admitted[0].key.index, 0);
}

#[test]
fn active_tcp_product_inflight_limit_is_model_based() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 32 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 50.0, mbps(50.0));
    let limit = bulk_product_inflight_limit_bytes(
        candidate,
        64 * 1024,
        mux_limits,
        BulkAdmissionRole::ActiveDataPath,
    );

    assert!(limit < mux_limits.max_path_flight_bytes as u64);
    assert_eq!(limit, 625_000);
}

#[test]
fn active_tcp_with_same_path_latency_pressure_uses_preemptible_service_horizon() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 32 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 672.0, mbps(100.0));
    candidate.active_latency_sensitive_flows = 1;
    let payload = 64 * 1024;
    let limit = bulk_product_inflight_limit_bytes(
        candidate,
        payload,
        mux_limits,
        BulkAdmissionRole::ActiveDataPath,
    );

    assert_eq!(
        limit,
        bulk_service_horizon_payload_bytes(payload, mux_limits) as u64
    );
    assert!(limit < bulk_bbr_inflight_bytes(bulk_path_bdp_bytes(candidate)));
    assert!(limit >= payload as u64);
}

#[test]
fn active_tcp_with_session_only_latency_pressure_keeps_service_owner_reservoir() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 32 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 672.0, mbps(100.0));
    candidate.session_active_latency_sensitive_flows = 1;
    let payload = 64 * 1024;
    let limit = bulk_product_inflight_limit_bytes(
        candidate,
        payload,
        mux_limits,
        BulkAdmissionRole::ActiveDataPath,
    );

    assert!(
        limit > bulk_service_horizon_payload_bytes(payload, mux_limits) as u64,
        "latency work on other paths must not shrink this Service owner to the preemptible horizon"
    );
}

#[test]
fn bulk_service_horizon_is_geometric_mean_not_full_envelope() {
    let payload = 64 * 1024;
    let mux_limits = MuxLimits::default();

    assert_eq!(
        bulk_service_horizon_payload_bytes(payload, mux_limits),
        2 * 1024 * 1024,
        "the service horizon is a preemptible scoring window, not the full product envelope"
    );
}

#[test]
fn active_tcp_with_same_path_latency_pressure_rejects_hidden_command_backlog() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 672.0, mbps(100.0));
    active.queue_bytes = 8 * 1024 * 1024;
    active.product_bytes_in_flight = 8 * 1024 * 1024;
    active.product_progress_rate_bps = Some(mbps(1_000.0));
    active.active_latency_sensitive_flows = 1;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            active,
            100.0,
            64 * 1024,
            MuxLimits::default(),
            BulkAdmissionRole::ActiveDataPath,
        ),
        Some("inflight_limit")
    );
}

#[test]
fn active_service_unions_overlapping_product_flight_and_command_queue() {
    let payload = 64 * 1024;
    let envelope = 512 * 1024;
    let mux_limits = MuxLimits {
        max_path_flight_bytes: envelope,
        max_reorder_bytes: envelope,
        max_stream_window_bytes: envelope as u64,
        ..MuxLimits::default()
    };
    let best = candidate(0, 10.0, 20.0, 500.0);
    let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 20.0, mbps(500.0));
    active.product_bytes_in_flight = (envelope - payload) as u64;
    active.queue_bytes = (envelope - payload) as u64;

    assert_eq!(
        bulk_assigned_service_debt_bytes(active),
        (envelope - payload) as u64
    );
    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            active,
            10.0,
            payload,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "carrier-pending OwnerData is already represented in product flight"
    );

    active.product_bytes_in_flight = 0;
    active.queue_bytes = envelope as u64;
    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            active,
            10.0,
            payload,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
        ),
        Some("inflight_limit"),
        "queue pressure remains an authoritative fallback when product flight is absent"
    );
}

#[test]
fn active_service_same_path_backlog_is_not_reorder_debt() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload = 64 * 1024;
    let mut active = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 333.0, mbps(0.351));
    active.pacing_rate_bps = mbps(0.351);
    active.product_progress_rate_bps = Some(mbps(0.322));
    active.product_bytes_in_flight = 8_912_896;
    active.product_queue_bytes = 512 * 1024;
    active.queue_bytes = 2 * 1024 * 1024;
    active.session_active_latency_sensitive_flows = 1;
    active.confidence = 1.0;
    active.app_limited = true;

    assert!(
        bulk_candidate_within_reorder_budget(
            active,
            payload,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
            0,
        ),
        "same-Service queued bytes are feed/backpressure debt, not cross-path reorder debt"
    );
    assert_ne!(
        bulk_candidate_admission_suppression(
            active,
            117_551.370,
            active,
            117_551.370,
            payload,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
        ),
        Some("reorder_budget"),
        "the active Service may be allowed or backpressured, but same-path backlog must not be reported as cross-path reorder debt"
    );
}

#[test]
fn active_tcp_service_with_clear_frontier_uses_product_envelope_for_reorder_budget() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload = 64 * 1024;
    let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 333.0, mbps(0.351));
    active.pacing_rate_bps = mbps(0.351);
    active.product_progress_rate_bps = Some(mbps(0.351));
    active.queue_bytes = payload as u64;
    active.product_bytes_in_flight = 341_200;
    active.app_limited = true;

    assert_eq!(
        bulk_candidate_admission_suppression(
            active,
            50_018.062,
            active,
            50_018.062,
            payload,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "a clear-frontier Service owner must not lose lead eligibility because the reorder gate reuses a tiny app-limited BDP budget"
    );
}

#[test]
fn active_tcp_service_with_latency_pressure_caps_total_owner_credit() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload = 64 * 1024;
    let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 333.0, mbps(0.369));
    active.pacing_rate_bps = mbps(0.369);
    active.product_progress_rate_bps = Some(mbps(0.369));
    active.product_bytes_in_flight = 8 * 1024 * 1024;
    active.active_latency_sensitive_flows = 1;
    active.app_limited = true;

    assert_eq!(
        bulk_candidate_admission_suppression(
            active,
            50_286.929,
            active,
            50_286.929,
            payload,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
        ),
        Some("inflight_limit"),
        "latency-sensitive mixed work must cap total clear-frontier Service owner credit, not just queued backlog, or stale owner flight becomes read-gap and repair debt"
    );
}

#[test]
fn app_limited_service_progress_uses_product_envelope_for_clear_frontier_owner() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload = 64 * 1024;
    let mut active = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 333.0, mbps(0.351));
    active.pacing_rate_bps = mbps(0.351);
    active.product_progress_rate_bps = Some(mbps(0.288));
    active.product_bytes_in_flight = 3_670_016;
    active.product_queue_bytes = 512 * 1024;
    active.queue_bytes = 0;
    active.inflight_limit_bytes = 0;
    active.confidence = 1.0;
    active.app_limited = true;

    assert_eq!(
        bulk_candidate_admission_suppression(
            active,
            73_038.285,
            active,
            73_038.285,
            payload,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "app-limited product progress must not shrink a clear-frontier Service owner below the product envelope"
    );
}

#[test]
fn app_limited_udp_service_progress_does_not_shrink_below_startup_headroom() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload = 64 * 1024;
    let observed_rate_bps = mbps(57.0);
    let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 60.0, observed_rate_bps);
    active.pacing_rate_bps = mbps(1_500.0);
    active.delivery_rate_bps = observed_rate_bps;
    active.product_progress_rate_bps = Some(observed_rate_bps);
    active.app_limited = true;
    active.product_bytes_in_flight =
        bulk_bbr_inflight_bytes(bulk_rate_bdp_bytes(observed_rate_bps, active.srtt_ms));

    assert_eq!(
        bulk_candidate_admission_suppression(
            active,
            10.0,
            active,
            10.0,
            payload,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "an app-limited active Service path should not be shrunk below startup headroom; optional Subflows and ordering-debt paths own the receive-hole risk"
    );
}

#[test]
fn active_udp_service_under_latency_pressure_keeps_bounded_headroom_after_failover() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload = 64 * 1024;
    let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 47.643, mbps(71.713));
    active.pacing_rate_bps = mbps(73.287);
    active.product_bytes_in_flight = 2_049_152;
    active.product_queue_bytes = 458_752;
    active.queue_bytes = 14_600;
    active.inflight_limit_bytes = 30_570;
    active.active_latency_sensitive_flows = 1;
    active.confidence = 1.0;
    active.app_limited = true;

    assert_eq!(
        bulk_candidate_admission_suppression(
            active,
            398.634,
            active,
            398.634,
            payload,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "latency pressure must keep the Service preemptible, but not suppress a measured active UDP Service with only bounded post-failover feed debt"
    );
}

#[test]
fn tiny_app_limited_service_progress_uses_stable_service_horizon() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload = 64 * 1024;
    let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 333.0, mbps(0.351));
    active.pacing_rate_bps = mbps(0.351);
    active.delivery_rate_bps = mbps(0.351);
    active.product_progress_rate_bps = Some(mbps(0.351));
    active.product_bytes_in_flight = 1_048_576;
    active.product_queue_bytes = 0;
    active.queue_bytes = 0;
    active.app_limited = true;
    active.confidence = 0.1;

    assert_eq!(
        bulk_candidate_admission_suppression(
            active,
            72_439.0,
            active,
            72_439.0,
            payload,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "tiny app-limited progress is not bulk-rate proof, but the clear-frontier Service owner may still use the stable service horizon because it cannot create a cross-path stream hole"
    );
}

#[test]
fn pre_progress_service_startup_uses_stable_service_horizon() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload = 64 * 1024;
    let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 333.0, mbps(0.351));
    active.pacing_rate_bps = mbps(0.351);
    active.delivery_rate_bps = mbps(0.351);
    active.product_progress_rate_bps = None;
    active.product_bytes_in_flight = 1_048_576;
    active.app_limited = true;

    assert_eq!(
        bulk_candidate_admission_suppression(
            active,
            72_439.0,
            active,
            72_439.0,
            payload,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "before product progress exists, a clear-frontier Service owner should still get the stable service horizon; sender credit and stream flow control remain the safety gates"
    );
}

#[test]
fn sub_quantum_service_tail_uses_stable_service_horizon_under_latency_pressure() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload = 1_896;
    let mut active = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 333.0, mbps(0.351));
    active.pacing_rate_bps = mbps(0.351);
    active.delivery_rate_bps = mbps(0.351);
    active.product_progress_rate_bps = Some(mbps(0.351));
    active.product_bytes_in_flight = 773_728;
    active.product_queue_bytes = payload as u64;
    active.active_latency_sensitive_flows = 1;
    active.app_limited = true;
    active.confidence = 0.0;

    assert_eq!(
        bulk_candidate_admission_suppression(
            active,
            8_870.050,
            active,
            8_870.050,
            payload,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "a tiny tail payload must not shrink the active Service horizon below the normal feed quantum and deadlock stream completion under latency pressure"
    );
}

#[test]
fn active_udp_owner_with_clear_frontier_uses_product_envelope_not_carrier_cwnd() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 32 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 80.0, mbps(500.0));
    candidate.inflight_limit_bytes = 128 * 1024;
    candidate.product_bytes_in_flight = 96 * 1024;
    candidate.queue_bytes = 16 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            candidate,
            10.0,
            candidate,
            10.0,
            32 * 1024,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "active QUIC owner must not use current carrier cwnd as a product admission ceiling"
    );
}

#[test]
fn active_udp_clear_frontier_uses_product_service_envelope() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 60.0, mbps(200.0));
    candidate.inflight_limit_bytes = 512 * 1024;
    candidate.product_bytes_in_flight = 512 * 1024;
    candidate.product_progress_rate_bps = Some(mbps(200.0));
    candidate.queue_bytes = 0;

    assert_eq!(
        bulk_candidate_admission_suppression(
            candidate,
            10.0,
            candidate,
            10.0,
            64 * 1024,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "active service owner should be fed through the product service envelope when the ordered frontier is clear"
    );
}

#[test]
fn raw_sender_queue_does_not_consume_active_service_owner_envelope() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 512 * 1024,
        max_reorder_bytes: 512 * 1024,
        max_stream_window_bytes: 512 * 1024,
        ..MuxLimits::default()
    };
    let payload = 64 * 1024;
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 20.0, mbps(500.0));
    candidate.product_queue_bytes = 512 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            candidate,
            10.0,
            candidate,
            10.0,
            payload,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "raw staged bytes have no offset or path owner and must not block their own dispatch"
    );
}

#[test]
fn active_udp_with_product_progress_debt_uses_product_envelope() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 1000.0, mbps(500.0));
    candidate.pacing_rate_bps = mbps(2_000.0);
    candidate.product_progress_rate_bps = Some(mbps(10.0));
    candidate.product_bytes_in_flight = 8 * 1024 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            candidate,
            10.0,
            candidate,
            10.0,
            64 * 1024,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "active QUIC Service ownership is bounded by product resource envelopes; carrier pacing and optional Subflow admission own lower-layer limits"
    );
}

#[test]
fn active_udp_with_latency_pressure_caps_total_owner_credit() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 1000.0, mbps(500.0));
    candidate.pacing_rate_bps = mbps(2_000.0);
    candidate.product_progress_rate_bps = Some(mbps(10.0));
    candidate.product_bytes_in_flight = 8 * 1024 * 1024;
    candidate.active_latency_sensitive_flows = 1;

    assert_eq!(
        bulk_candidate_admission_suppression(
            candidate,
            10.0,
            candidate,
            10.0,
            64 * 1024,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
        ),
        Some("inflight_limit"),
        "QUIC Service ownership also becomes preemptible under realtime/mixed pressure; carrier cwnd is not the product ceiling, but already-owned product flight must still be bounded"
    );
}

#[test]
fn active_udp_without_product_progress_uses_product_envelope() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 80.0, mbps(500.0));
    candidate.pacing_rate_bps = mbps(2_000.0);
    candidate.product_bytes_in_flight = 8 * 1024 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            candidate,
            10.0,
            candidate,
            10.0,
            64 * 1024,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "active QUIC Service startup may use the product envelope because it is the current ordered owner, not an optional Subflow"
    );
}

#[test]
fn active_tcp_without_product_progress_uses_product_envelope() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 80.0, mbps(500.0));
    candidate.pacing_rate_bps = mbps(2_000.0);
    candidate.product_bytes_in_flight = 8 * 1024 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            candidate,
            10.0,
            candidate,
            10.0,
            64 * 1024,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "active TCP Service startup uses the same carrier-neutral product envelope as QUIC"
    );
}

#[test]
fn cross_underlay_product_inflight_limit_is_bdp_modeled() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 32 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 50.0, mbps(50.0));
    let limit = bulk_product_inflight_limit_bytes(
        candidate,
        64 * 1024,
        mux_limits,
        BulkAdmissionRole::AdditionalCrossUnderlay,
    );

    assert!(limit < mux_limits.max_path_flight_bytes as u64);
    assert_eq!(limit, 625_000);
}

#[test]
fn same_underlay_candidate_must_not_join_only_because_reorder_budget_can_absorb_gap() {
    let admitted = bulk_striping_admitted_subflows(
        vec![
            candidate(0, 2958.0, 80.0, 180.0),
            candidate(1, 3202.0, 180.0, 220.0),
        ],
        64 * 1024,
        MuxLimits::default(),
    );

    assert_eq!(admitted.len(), 1);
    assert_eq!(admitted[0].key.index, 0);
}

#[test]
fn bulk_admission_rejects_candidate_that_would_exceed_product_inflight_limit() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut saturated = candidate(1, 110.0, 50.0, 500.0);
    saturated.snapshot.inflight_limit_bytes = 64 * 1024;
    saturated.snapshot.bytes_in_flight = MuxLimits::default().max_path_flight_bytes as u64;

    let admitted =
        bulk_striping_admitted_subflows(vec![best, saturated], 16 * 1024, MuxLimits::default());

    assert_eq!(admitted.len(), 1);
    assert_eq!(admitted[0].key.index, 0);
}

#[test]
fn additional_path_reorder_budget_is_not_floored_to_product_inflight_limit() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut extra = candidate(1, 100.0, 50.0, 50.0);
    extra.snapshot.confidence = 1.0;
    extra.snapshot.inflight_limit_bytes = MuxLimits::default().max_path_flight_bytes as u64;
    extra.snapshot.bytes_in_flight = 1024 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            extra.snapshot,
            extra.eta_ms,
            64 * 1024,
            MuxLimits::default(),
            BulkAdmissionRole::AdditionalCrossUnderlay,
        ),
        Some("reorder_budget")
    );
}

#[test]
fn high_confidence_quic_window_needs_product_progress_before_expanding_product_authority() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = 64 * 1024;
    let mut proof_only = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 360.0, mbps(18.0));
    proof_only.pacing_rate_bps = mbps(500.0);

    let receipt_rate_inflight_target =
        bulk_same_underlay_product_authority_bytes(proof_only, payload_bytes, mux_limits);
    assert_eq!(receipt_rate_inflight_target, 1_620_000);

    proof_only.inflight_limit_bytes = 7 * 1024 * 1024;
    assert_eq!(
        bulk_same_underlay_product_authority_bytes(proof_only, payload_bytes, mux_limits),
        receipt_rate_inflight_target,
        "strict carrier pacing is not product authority with or without a native window"
    );
    assert!(receipt_rate_inflight_target < proof_only.inflight_limit_bytes);

    proof_only.confidence = 0.1;
    assert_eq!(
        bulk_same_underlay_product_authority_bytes(proof_only, payload_bytes, mux_limits),
        proof_only.inflight_limit_bytes,
        "the explicit startup epoch retains its native-window allowance"
    );

    proof_only.confidence = 1.0;
    proof_only.product_progress_rate_bps = Some(mbps(18.0));
    assert_eq!(
        bulk_same_underlay_product_authority_bytes(proof_only, payload_bytes, mux_limits),
        receipt_rate_inflight_target,
        "one exact product ACK is not yet durable window authority"
    );

    proof_only.has_durable_product_progress = true;
    proof_only.product_progress_rate_bps = None;
    assert_eq!(
        bulk_same_underlay_product_authority_bytes(proof_only, payload_bytes, mux_limits),
        proof_only.inflight_limit_bytes,
        "durable product bytes may couple the native window without a point rate"
    );
}

#[test]
fn quic_same_underlay_separates_candidate_credit_from_stream_reorder_envelope() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        max_stream_window_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload_bytes = 64 * 1024;
    let mut proof_only = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(18.3));
    proof_only.confidence = 0.2;
    proof_only.inflight_limit_bytes = 8_835_877;

    assert!(bulk_candidate_within_reorder_budget(
        proof_only,
        payload_bytes,
        mux_limits,
        BulkAdmissionRole::AdditionalSameUnderlay,
        16_580_396,
    ));

    proof_only.product_bytes_in_flight = proof_only.inflight_limit_bytes;
    assert!(
        !bulk_candidate_within_reorder_budget(
            proof_only,
            payload_bytes,
            mux_limits,
            BulkAdmissionRole::AdditionalSameUnderlay,
            0,
        ),
        "foreign debt no longer consumes local credit, but candidate-owned bytes still do"
    );

    proof_only.product_bytes_in_flight = 0;
    assert!(
        !bulk_candidate_within_reorder_budget(
            proof_only,
            payload_bytes,
            mux_limits,
            BulkAdmissionRole::AdditionalSameUnderlay,
            mux_limits.max_reorder_bytes as u64,
        ),
        "local credit does not permit the aggregate receive hole to exceed its stream envelope"
    );
}

#[test]
fn app_limited_tcp_active_path_uses_service_headroom_until_backpressured() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut active = candidate(0, 100.0, 50.0, 0.35);
    active.snapshot.underlay = UnderlayProtocol::Tcp;
    active.snapshot.confidence = 0.1;
    active.snapshot.app_limited = true;
    active.snapshot.bytes_in_flight = 8 * 1024 * 1024;
    active.snapshot.product_bytes_in_flight = 8 * 1024 * 1024;
    active.snapshot.product_progress_rate_bps = Some(mbps(1_000.0));

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            active.snapshot,
            active.eta_ms,
            64 * 1024,
            MuxLimits::default(),
            BulkAdmissionRole::ActiveDataPath,
        ),
        None
    );
}

#[test]
fn tcp_active_service_under_bulk_demand_is_not_starved_by_latency_pressure_flag() {
    let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 333.0, mbps(0.351));
    active.pacing_rate_bps = mbps(0.351);
    active.product_bytes_in_flight = 380_304;
    active.product_queue_bytes = 395_112;
    active.product_progress_rate_bps = Some(mbps(0.351));
    active.session_active_latency_sensitive_flows = 1;
    active.confidence = 0.1;
    active.app_limited = true;

    assert_eq!(
        bulk_candidate_admission_suppression(
            active,
            57_482.654,
            active,
            57_482.654,
            64 * 1024,
            MuxLimits::default(),
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "latency-first startup state must not shrink an active bulk Service owner to a tiny startup-rate BDP while the ordered frontier is clear"
    );
}

#[test]
fn tcp_active_service_partial_quantum_does_not_shrink_feed_window() {
    let payload = 45_536;
    let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 333.0, mbps(0.351));
    active.pacing_rate_bps = mbps(0.351);
    active.product_bytes_in_flight = 686_088;
    active.product_queue_bytes = payload as u64;
    active.product_progress_rate_bps = Some(mbps(0.351));
    active.confidence = 0.1;
    active.app_limited = true;

    assert_eq!(
        bulk_candidate_admission_suppression(
            active,
            41_548.284,
            active,
            41_548.284,
            payload,
            MuxLimits::default(),
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "a partial local read quantum must not reduce the active Service owner feed window below the stable service horizon"
    );
}

#[test]
fn tcp_active_service_startup_uses_stable_service_horizon_before_bulk_samples() {
    let payload = 64 * 1024;
    let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 333.0, mbps(0.351));
    active.pacing_rate_bps = mbps(0.351);
    active.product_bytes_in_flight = 906_488;
    active.product_queue_bytes = 393_216;
    active.product_progress_rate_bps = Some(mbps(0.351));
    active.confidence = 0.1;
    active.app_limited = true;

    assert_eq!(
        bulk_candidate_admission_suppression(
            active,
            57_439.409,
            active,
            57_439.409,
            payload,
            MuxLimits::default(),
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "a clear-frontier Service owner should ramp within the stable service horizon instead of being trapped below a tiny startup feedback window"
    );
}

#[test]
fn tcp_active_service_startup_allows_bbr_headroom_over_service_horizon() {
    let payload = 64 * 1024;
    let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 333.0, mbps(0.351));
    active.pacing_rate_bps = mbps(0.351);
    active.queue_bytes = 262_144;
    active.product_bytes_in_flight = 1_561_848;
    active.product_queue_bytes = 393_216;
    active.product_progress_rate_bps = Some(mbps(0.351));
    active.confidence = 0.1;
    active.app_limited = true;

    assert_eq!(
        bulk_candidate_admission_suppression(
            active,
            63_418.447,
            active,
            63_418.447,
            payload,
            MuxLimits::default(),
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "active Service feed may use BBR-style headroom above the preemptible service horizon so a healthy path can leave startup"
    );
}

#[test]
fn tcp_active_service_app_limited_progress_does_not_shrink_below_startup_headroom() {
    let payload = 64 * 1024;
    let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 333.0, mbps(9.522));
    active.pacing_rate_bps = mbps(9.522);
    active.delivery_rate_bps = mbps(9.522);
    active.product_progress_rate_bps = Some(mbps(9.522));
    active.product_bytes_in_flight = 655_360;
    active.product_queue_bytes = 327_680;
    active.confidence = 1.0;
    active.app_limited = true;

    assert_eq!(
        bulk_candidate_admission_suppression(
            active,
            2_203.842,
            active,
            2_203.842,
            payload,
            MuxLimits::default(),
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "app-limited progress on the Service owner is ACK-clock visibility, not a reason to shrink the Service feed below startup headroom"
    );
}

#[test]
fn tcp_active_service_clear_frontier_uses_product_envelope_not_modeled_bdp_cap() {
    let payload = 64 * 1024;
    let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 333.0, mbps(25.404));
    active.pacing_rate_bps = mbps(25.404);
    active.delivery_rate_bps = mbps(25.404);
    active.product_progress_rate_bps = Some(mbps(25.404));
    active.product_bytes_in_flight = 4_128_768;
    active.product_queue_bytes = 458_752;
    active.confidence = 1.0;
    active.app_limited = true;

    assert_eq!(
        bulk_candidate_admission_suppression(
            active,
            971.380,
            active,
            971.380,
            payload,
            MuxLimits::default(),
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "clear-frontier Service owner admission should be bounded by product resource envelopes, not a low modeled BDP cap created by prior starvation"
    );
}

#[test]
fn tcp_active_path_with_ordering_debt_obeys_model_based_product_flight_budget() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut active = candidate(0, 100.0, 50.0, 50.0);
    active.snapshot.underlay = UnderlayProtocol::Tcp;
    active.snapshot.confidence = 1.0;
    active.snapshot.inflight_limit_bytes = MuxLimits::default().max_path_flight_bytes as u64;
    active.snapshot.bytes_in_flight = 1024 * 1024;
    active.snapshot.product_bytes_in_flight = 1024 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: best.snapshot,
            best_eta_ms: best.eta_ms,
            candidate_snapshot: active.snapshot,
            candidate_eta_ms: active.eta_ms,
            payload_bytes: 64 * 1024,
            mux_limits: MuxLimits::default(),
            role: BulkAdmissionRole::ActiveDataPath,
            stream_ordering_debt_bytes: 1024 * 1024,
        }),
        Some("inflight_limit")
    );
}

#[test]
fn lagging_active_path_must_not_expand_cross_path_stream_hole() {
    let best = candidate(1, 100.0, 170.0, 180.0);
    let mut lagging_active = candidate(0, 1900.0, 1800.0, 1.0);
    lagging_active.snapshot.underlay = UnderlayProtocol::Tcp;
    lagging_active.snapshot.confidence = 1.0;
    lagging_active.snapshot.inflight_limit_bytes =
        MuxLimits::default().max_path_flight_bytes as u64;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: best.snapshot,
            best_eta_ms: best.eta_ms,
            candidate_snapshot: lagging_active.snapshot,
            candidate_eta_ms: lagging_active.eta_ms,
            payload_bytes: 16 * 1024,
            mux_limits: MuxLimits::default(),
            role: BulkAdmissionRole::ActiveDataPath,
            stream_ordering_debt_bytes: 15 * 1024 * 1024,
        },),
        Some("reorder_budget")
    );
}

#[test]
fn best_active_path_can_continue_across_small_existing_hole() {
    let active = candidate(0, 100.0, 170.0, 180.0);
    let payload = 16 * 1024;
    let stream_ordering_debt_bytes = 64 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: active.snapshot,
            best_eta_ms: active.eta_ms,
            candidate_snapshot: active.snapshot,
            candidate_eta_ms: active.eta_ms,
            payload_bytes: payload,
            mux_limits: MuxLimits::default(),
            role: BulkAdmissionRole::ActiveDataPath,
            stream_ordering_debt_bytes,
        },),
        None
    );
}

#[test]
fn active_lower_frontier_owner_uses_adaptive_reorder_budget_without_latency_pressure() {
    let active = candidate(0, 100.0, 170.0, 500.0);
    let payload = 64 * 1024;
    let mux_limits = MuxLimits::default();
    let service_horizon = bulk_service_horizon_payload_bytes(payload, mux_limits) as u64;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: active.snapshot,
            best_eta_ms: active.eta_ms,
            candidate_snapshot: active.snapshot,
            candidate_eta_ms: active.eta_ms,
            payload_bytes: payload,
            mux_limits,
            role: BulkAdmissionRole::ActiveDataPath,
            stream_ordering_debt_bytes: service_horizon.saturating_add(payload as u64),
        },),
        None,
        "bulk-only lower-frontier progress should use the adaptive BDP budget instead of stalling at the geometric Service horizon"
    );
}

#[test]
fn latency_pressured_lower_frontier_owner_keeps_preemptible_service_horizon() {
    let mut active = candidate(0, 100.0, 170.0, 500.0);
    active.snapshot.active_latency_sensitive_flows = 1;
    let payload = 64 * 1024;
    let mux_limits = MuxLimits::default();
    let service_horizon = bulk_service_horizon_payload_bytes(payload, mux_limits) as u64;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: active.snapshot,
            best_eta_ms: active.eta_ms,
            candidate_snapshot: active.snapshot,
            candidate_eta_ms: active.eta_ms,
            payload_bytes: payload,
            mux_limits,
            role: BulkAdmissionRole::ActiveDataPath,
            stream_ordering_debt_bytes: service_horizon.saturating_add(payload as u64),
        },),
        Some("reorder_budget"),
        "latency pressure must retain the bounded preemptible Service horizon"
    );
}

#[test]
fn lead_path_with_large_cross_path_hole_uses_reorder_budget() {
    let lead = candidate(0, 100.0, 170.0, 180.0);

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: lead.snapshot,
            best_eta_ms: lead.eta_ms,
            candidate_snapshot: lead.snapshot,
            candidate_eta_ms: lead.eta_ms,
            payload_bytes: 16 * 1024,
            mux_limits: MuxLimits::default(),
            role: BulkAdmissionRole::ActiveDataPath,
            stream_ordering_debt_bytes: 32 * 1024 * 1024,
        },),
        Some("reorder_budget")
    );
}

#[test]
fn product_inflight_limit_is_modeled_limit_capped_by_configured_ceiling() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut constrained = candidate(1, 100.1, 50.0, 10.0);
    constrained.snapshot.confidence = 1.0;
    constrained.snapshot.inflight_limit_bytes = 64 * 1024;
    constrained.snapshot.bytes_in_flight = 128 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            constrained.snapshot,
            constrained.eta_ms,
            16 * 1024,
            MuxLimits::default(),
            BulkAdmissionRole::AdditionalCrossUnderlay,
        ),
        Some("inflight_limit")
    );
}

#[test]
fn udp_multipath_active_path_ignores_carrier_flight_as_product_stop() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut active = candidate(0, 100.0, 50.0, 500.0);
    active.snapshot.confidence = 1.0;
    active.snapshot.inflight_limit_bytes = 512 * 1024;
    active.snapshot.bytes_in_flight = 512 * 1024;
    active.snapshot.product_bytes_in_flight = 0;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            active.snapshot,
            active.eta_ms,
            64 * 1024,
            MuxLimits::default(),
            BulkAdmissionRole::ActiveDataPath,
        ),
        None
    );
}

#[test]
fn udp_active_ordered_owner_uses_product_envelope_not_carrier_cwnd_gate() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut active = candidate(0, 100.0, 50.0, 500.0);
    active.snapshot.confidence = 1.0;
    active.snapshot.inflight_limit_bytes = 512 * 1024;
    active.snapshot.bytes_in_flight = 512 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            active.snapshot,
            active.eta_ms,
            64 * 1024,
            MuxLimits::default(),
            BulkAdmissionRole::ActiveDataPath,
        ),
        None,
        "QUIC carrier cwnd/in-flight is carrier-owned pacing evidence; the product active owner must be bounded by product flight and carrier writer backpressure, not a duplicate hard cwnd gate"
    );
}

#[test]
fn udp_single_carrier_lead_uses_product_budget_not_duplicate_carrier_cwnd() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut active = candidate(0, 100.0, 50.0, 500.0);
    active.snapshot.confidence = 1.0;
    active.snapshot.active_flows = 2;
    active.snapshot.active_latency_sensitive_flows = 1;
    active.snapshot.inflight_limit_bytes = 512 * 1024;
    active.snapshot.bytes_in_flight = 512 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            active.snapshot,
            active.eta_ms,
            64 * 1024,
            MuxLimits::default(),
            BulkAdmissionRole::ActiveSingleCarrier,
        ),
        None
    );
}

#[test]
fn udp_single_flow_lead_uses_product_gate_not_carrier_queue_gate() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut active = candidate(0, 100.0, 50.0, 500.0);
    active.snapshot.confidence = 1.0;
    active.snapshot.active_flows = 1;
    active.snapshot.active_latency_sensitive_flows = 0;
    active.snapshot.inflight_limit_bytes = 512 * 1024;
    active.snapshot.bytes_in_flight = 512 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            active.snapshot,
            active.eta_ms,
            64 * 1024,
            MuxLimits::default(),
            BulkAdmissionRole::ActiveSingleCarrier,
        ),
        None
    );
}

#[test]
fn udp_cross_underlay_extra_path_uses_carrier_queue_gate() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut extra = candidate(1, 100.0, 50.0, 500.0);
    extra.snapshot.confidence = 1.0;
    extra.snapshot.inflight_limit_bytes = 512 * 1024;
    extra.snapshot.bytes_in_flight = 512 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            extra.snapshot,
            extra.eta_ms,
            64 * 1024,
            MuxLimits::default(),
            BulkAdmissionRole::AdditionalCrossUnderlay,
        ),
        Some("inflight_limit")
    );
}

#[test]
fn udp_active_path_without_carrier_limit_uses_modeled_credit() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut startup = candidate(0, 100.1, 50.0, 10.0);
    startup.snapshot.confidence = 0.1;
    startup.snapshot.active_flows = 2;
    startup.snapshot.active_latency_sensitive_flows = 1;
    startup.snapshot.bytes_in_flight = 1024 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            startup.snapshot,
            startup.eta_ms,
            64 * 1024,
            MuxLimits::default(),
            BulkAdmissionRole::ActiveSingleCarrier,
        ),
        None
    );
    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            startup.snapshot,
            startup.eta_ms,
            64 * 1024,
            MuxLimits::default(),
            BulkAdmissionRole::AdditionalCrossUnderlay,
        ),
        Some("inflight_limit")
    );
}

#[test]
fn same_underlay_extra_path_uses_ack_clocked_budget_not_tiny_probe_budget() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut extra = candidate(1, 50.0, 50.0, 50.0);
    extra.snapshot.confidence = 0.1;
    extra.snapshot.bytes_in_flight = 512 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            extra.snapshot,
            extra.eta_ms,
            64 * 1024,
            MuxLimits::default(),
            BulkAdmissionRole::AdditionalSameUnderlay,
        ),
        None
    );
    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            extra.snapshot,
            extra.eta_ms,
            64 * 1024,
            MuxLimits::default(),
            BulkAdmissionRole::AdditionalCrossUnderlay,
        ),
        Some("reorder_budget")
    );
}

#[test]
fn tcp_subflow_uses_global_reorder_envelope_not_its_local_pipe_budget() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        max_stream_window_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let best = candidate(0, 700.0, 180.0, 400.0);
    let mut extra = candidate(1, 100.0, 180.0, 500.0);
    extra.key.underlay = UnderlayProtocol::Tcp;
    extra.snapshot.underlay = UnderlayProtocol::Tcp;
    extra.snapshot.confidence = 0.5;
    extra.snapshot.app_limited = true;
    let payload_bytes = 64 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: best.snapshot,
            best_eta_ms: best.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes,
            mux_limits,
            role: BulkAdmissionRole::AdditionalSameUnderlay,
            stream_ordering_debt_bytes: 40 * 1024 * 1024,
        }),
        None,
        "foreign lower-owner debt consumes the stream reorder resource, not this candidate's 2*BDP pipe allowance"
    );

    extra.snapshot.product_bytes_in_flight = 24 * 1024 * 1024;
    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: best.snapshot,
            best_eta_ms: best.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes,
            mux_limits,
            role: BulkAdmissionRole::AdditionalSameUnderlay,
            stream_ordering_debt_bytes: 0,
        }),
        Some("inflight_limit"),
        "the independent candidate-local BDP gate must still bound its own pipe"
    );

    extra.snapshot.product_bytes_in_flight = 1024 * 1024;
    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: best.snapshot,
            best_eta_ms: best.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes,
            mux_limits,
            role: BulkAdmissionRole::AdditionalSameUnderlay,
            stream_ordering_debt_bytes: 64 * 1024 * 1024,
        }),
        Some("reorder_budget"),
        "candidate flight plus foreign debt must still fit the aggregate stream envelope"
    );
}

#[test]
fn same_underlay_startup_probe_is_not_rejected_by_app_limited_completion_horizon() {
    let mut lead = candidate(0, 1_530_000.0, 50.0, 0.35);
    let mut extra = candidate(1, 3_060_000.0, 50.0, 0.35);
    lead.snapshot.confidence = 0.1;
    lead.snapshot.app_limited = true;
    extra.snapshot.confidence = 0.1;
    extra.snapshot.app_limited = true;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: lead.snapshot,
            best_eta_ms: lead.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes: 64 * 1024,
            mux_limits: MuxLimits::default(),
            role: BulkAdmissionRole::AdditionalSameUnderlay,
            stream_ordering_debt_bytes: 0,
        }),
        None
    );
}

#[test]
fn same_underlay_low_confidence_sender_samples_remain_startup_admissible() {
    let mut lead = candidate(0, 98_000.0, 80.0, 1.0);
    let mut extra = candidate(1, 1_500_000.0, 80.0, 1.0);
    lead.snapshot.confidence = 0.1;
    extra.snapshot.confidence = 0.1;
    extra.snapshot.inflight_limit_bytes = 256 * 1024;
    extra.snapshot.product_bytes_in_flight = 128 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: lead.snapshot,
            best_eta_ms: lead.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes: 64 * 1024,
            mux_limits: MuxLimits::default(),
            role: BulkAdmissionRole::AdditionalSameUnderlay,
            stream_ordering_debt_bytes: 0,
        }),
        None
    );
}

#[test]
fn same_underlay_clear_frontier_still_requires_completion_gain_after_bulk_proof() {
    let mut lead = candidate(0, 98_000.0, 80.0, 500.0);
    let mut extra = candidate(1, 1_500_000.0, 80.0, 500.0);
    lead.snapshot.confidence = 1.0;
    extra.snapshot.confidence = 1.0;
    extra.snapshot.inflight_limit_bytes = 16 * 1024 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: lead.snapshot,
            best_eta_ms: lead.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes: 64 * 1024,
            mux_limits: MuxLimits::default(),
            role: BulkAdmissionRole::AdditionalSameUnderlay,
            stream_ordering_debt_bytes: 0,
        }),
        Some("same_underlay_no_completion_gain")
    );
    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: lead.snapshot,
            best_eta_ms: lead.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes: 64 * 1024,
            mux_limits: MuxLimits::default(),
            role: BulkAdmissionRole::AdditionalSameUnderlay,
            stream_ordering_debt_bytes: 512 * 1024,
        }),
        Some("same_underlay_no_completion_gain")
    );
}

#[test]
fn request_ordering_debt_does_not_mint_service_completion_backlog() {
    let mut lead = candidate(0, 400.0, 360.0, 400.0);
    let mut extra = candidate(1, 410.0, 360.0, 200.0);
    lead.snapshot.confidence = 1.0;
    extra.snapshot.confidence = 1.0;
    extra.snapshot.app_limited = false;
    let check = BulkAdmissionCheck {
        best_snapshot: lead.snapshot,
        best_eta_ms: lead.eta_ms,
        candidate_snapshot: extra.snapshot,
        candidate_eta_ms: extra.eta_ms,
        payload_bytes: 64 * 1024,
        mux_limits: MuxLimits::default(),
        role: BulkAdmissionRole::AdditionalSameUnderlay,
        stream_ordering_debt_bytes: 8 * 1024 * 1024,
    };
    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(check),
        Some("same_underlay_no_completion_gain"),
        "a lower receive hole cannot extend Service's completion deadline and authorize more later-offset request work"
    );
}

#[test]
fn bulk_rate_proven_same_underlay_path_must_beat_lead_next_quantum() {
    let mut lead = candidate(0, 100_000_000.0, 50.0, 100.0);
    let mut extra = candidate(1, 500_000_000.0, 700.0, 500.0);
    lead.snapshot.confidence = 1.0;
    extra.snapshot.confidence = 1.0;
    extra.snapshot.app_limited = false;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: lead.snapshot,
            best_eta_ms: lead.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes: 64 * 1024,
            mux_limits: MuxLimits::default(),
            role: BulkAdmissionRole::AdditionalSameUnderlay,
            stream_ordering_debt_bytes: 0,
        }),
        Some("same_underlay_no_completion_gain")
    );
}

#[test]
fn bulk_backlog_admits_same_underlay_path_that_finishes_before_service_tail() {
    let mut lead = candidate(0, 400.0, 360.0, 400.0);
    let mut extra = candidate(1, 800.0, 360.0, 200.0);
    lead.snapshot.confidence = 1.0;
    extra.snapshot.confidence = 1.0;
    extra.snapshot.app_limited = false;

    assert_eq!(
        bulk_candidate_admission_suppression_with_completion_backlog(
            BulkAdmissionCheck {
                best_snapshot: lead.snapshot,
                best_eta_ms: lead.eta_ms,
                candidate_snapshot: extra.snapshot,
                candidate_eta_ms: extra.eta_ms,
                payload_bytes: 64 * 1024,
                mux_limits: MuxLimits::default(),
                role: BulkAdmissionRole::AdditionalSameUnderlay,
                stream_ordering_debt_bytes: 0,
            },
            32 * 1024 * 1024,
        ),
        None,
        "a proven path that completes before the lower Service backlog adds bulk capacity without extending the hole"
    );
}

#[test]
fn bulk_backlog_does_not_double_count_service_carrier_flight() {
    let mut lead = candidate(0, 735.0, 360.0, 400.0);
    let mut extra = candidate(1, 1_100.0, 360.0, 200.0);
    lead.snapshot.confidence = 1.0;
    lead.snapshot.bytes_in_flight = 16 * 1024 * 1024;
    extra.snapshot.confidence = 1.0;
    extra.snapshot.app_limited = false;

    assert_eq!(
        bulk_candidate_admission_suppression_with_completion_backlog(
            BulkAdmissionCheck {
                best_snapshot: lead.snapshot,
                best_eta_ms: lead.eta_ms,
                candidate_snapshot: extra.snapshot,
                candidate_eta_ms: extra.eta_ms,
                payload_bytes: 64 * 1024,
                mux_limits: MuxLimits::default(),
                role: BulkAdmissionRole::AdditionalSameUnderlay,
                stream_ordering_debt_bytes: 0,
            },
            32 * 1024 * 1024,
        ),
        Some("same_underlay_no_completion_gain"),
        "carrier flight already present in Service ETA cannot extend the completion deadline twice"
    );
}

#[test]
fn cross_underlay_path_can_join_only_when_it_beats_lead_next_quantum() {
    let best = candidate(0, 500.0, 50.0, 500.0);
    let mut extra = candidate(1, 504.0, 250.0, 500.0);
    extra.snapshot.confidence = 1.0;
    extra.snapshot.bytes_in_flight = 8 * 1024 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            extra.snapshot,
            extra.eta_ms,
            512 * 1024,
            MuxLimits::default(),
            BulkAdmissionRole::AdditionalCrossUnderlay,
        ),
        None
    );
}

#[test]
fn cross_underlay_path_is_rejected_when_it_cannot_beat_lead_next_quantum() {
    let best = candidate(0, 500.0, 50.0, 500.0);
    let mut extra = candidate(1, 620.0, 250.0, 500.0);
    extra.snapshot.confidence = 1.0;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            extra.snapshot,
            extra.eta_ms,
            512 * 1024,
            MuxLimits::default(),
            BulkAdmissionRole::AdditionalCrossUnderlay,
        ),
        Some("cross_underlay_no_completion_gain")
    );
}

#[test]
fn stream_ordering_debt_suppresses_cross_underlay_candidate() {
    let best = candidate(0, 80.0, 50.0, 500.0);
    let mut extra = candidate(1, 80.5, 50.0, 500.0);
    extra.snapshot.confidence = 1.0;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: best.snapshot,
            best_eta_ms: best.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes: 64 * 1024,
            mux_limits: MuxLimits::default(),
            role: BulkAdmissionRole::AdditionalCrossUnderlay,
            stream_ordering_debt_bytes: 4 * 1024 * 1024,
        },),
        Some("cross_underlay_ordering_debt")
    );
}

#[test]
fn active_path_with_ordering_debt_must_still_beat_lead_completion_horizon() {
    let best = candidate(0, 10.0, 10.0, 1000.0);
    let active_with_debt = candidate(1, 100.0, 10.0, 1000.0);
    let payload = 32 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: best.snapshot,
            best_eta_ms: best.eta_ms,
            candidate_snapshot: active_with_debt.snapshot,
            candidate_eta_ms: active_with_debt.eta_ms,
            payload_bytes: payload,
            mux_limits: MuxLimits::default(),
            role: BulkAdmissionRole::ActiveDataPath,
            stream_ordering_debt_bytes: 16 * 1024,
        }),
        Some("completion_horizon")
    );
}

#[test]
fn bulk_admission_rejects_saturated_best_candidate() {
    let mut saturated_best = candidate(0, 100.0, 50.0, 500.0);
    saturated_best.snapshot.inflight_limit_bytes = 64 * 1024;
    saturated_best.snapshot.product_bytes_in_flight =
        MuxLimits::default().max_path_flight_bytes as u64;
    let backup = candidate(1, 130.0, 50.0, 500.0);

    let admitted = bulk_striping_admitted_subflows(
        vec![saturated_best, backup],
        16 * 1024,
        MuxLimits::default(),
    );

    assert_eq!(admitted.len(), 1);
    assert_eq!(admitted[0].key.index, 1);
}

#[test]
fn low_confidence_cross_underlay_candidate_outside_eta_subflow_set_is_suppressed() {
    let mut best = candidate(0, 1000.0, 180.0, 500.0);
    best.snapshot.confidence = 1.0;
    let mut uncertain = candidate(1, 1350.0, 180.0, 500.0);
    uncertain.key.underlay = UnderlayProtocol::Tcp;
    uncertain.snapshot.underlay = UnderlayProtocol::Tcp;
    uncertain.snapshot.confidence = 0.1;

    let admitted =
        bulk_striping_admitted_subflows(vec![best, uncertain], 64 * 1024, MuxLimits::default());

    assert_eq!(admitted.len(), 1);
    assert_eq!(admitted[0].key.index, 0);
}
