use super::*;
use crate::protocol::{PathId, PathUsage, UnderlayProtocol};

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
fn bulk_striping_excludes_backup_while_available_candidate_is_schedulable() {
    let available = candidate(0, 100.0, 80.0, 100.0);
    let mut faster_backup = candidate(1, 10.0, 10.0, 1_000.0);
    faster_backup.snapshot.peer_usage = Some(PathUsage::Backup);

    let admitted = bulk_striping_admitted_candidates(
        [faster_backup, available],
        64 * 1024,
        MuxLimits::default(),
        |left, right| left.index.cmp(&right.index),
    );

    assert_eq!(
        admitted
            .iter()
            .map(|candidate| candidate.key.index)
            .collect::<Vec<_>>(),
        vec![0]
    );
}

#[test]
fn validated_quic_path_remains_available_before_rate_matures() {
    let proven = candidate(0, 100.0, 180.0, 200.0);
    let mut validated = candidate(1, 200.0, 180.0, 1.0);
    validated.has_path_proof_evidence = true;
    validated.has_ack_data_evidence = false;
    validated.has_bulk_rate_evidence = false;
    validated.has_sender_delivery_evidence = false;

    let admitted = bulk_striping_admitted_candidates(
        [proven, validated],
        64 * 1024,
        MuxLimits::default(),
        |left, right| left.index.cmp(&right.index),
    );

    assert_eq!(
        admitted
            .iter()
            .map(|candidate| candidate.key.index)
            .collect::<Vec<_>>(),
        vec![0, 1],
        "QUIC path validation permits ordinary startup data while native congestion control and the MPP reorder window bound flight"
    );
}

#[test]
fn validated_tcp_path_remains_available_before_rate_matures() {
    let mut proven = candidate(0, 100.0, 180.0, 200.0);
    proven.key.underlay = UnderlayProtocol::Tcp;
    proven.snapshot.underlay = UnderlayProtocol::Tcp;
    let mut validated = candidate(1, 200.0, 180.0, 1.0);
    validated.key.underlay = UnderlayProtocol::Tcp;
    validated.snapshot.underlay = UnderlayProtocol::Tcp;
    validated.has_path_proof_evidence = true;
    validated.has_ack_data_evidence = false;
    validated.has_bulk_rate_evidence = false;
    validated.has_sender_delivery_evidence = false;

    let admitted = bulk_striping_admitted_candidates(
        [proven, validated],
        64 * 1024,
        MuxLimits::default(),
        |left, right| left.index.cmp(&right.index),
    );

    assert_eq!(
        admitted
            .iter()
            .map(|candidate| candidate.key.index)
            .collect::<Vec<_>>(),
        vec![0, 1],
        "TCP path validation permits bounded product startup while native TCP owns congestion and recovery"
    );
}

#[test]
fn validated_quic_path_remains_available_beside_active_unmeasured_work() {
    let mut active = candidate(0, 100.0, 180.0, 1.0);
    active.key.underlay = UnderlayProtocol::Tcp;
    active.snapshot.underlay = UnderlayProtocol::Tcp;
    active.has_bulk_rate_evidence = false;
    active.has_sender_delivery_evidence = false;
    active.snapshot.active_flows = 1;

    let mut validated = candidate(1, 200.0, 180.0, 1.0);
    validated.has_path_proof_evidence = true;
    validated.has_ack_data_evidence = false;
    validated.has_bulk_rate_evidence = false;
    validated.has_sender_delivery_evidence = false;

    let admitted = bulk_striping_admitted_candidates(
        [active, validated],
        64 * 1024,
        MuxLimits::default(),
        |left, right| left.index.cmp(&right.index),
    );

    assert_eq!(
        admitted
            .iter()
            .map(|candidate| candidate.key)
            .collect::<Vec<_>>(),
        vec![active.key, validated.key],
    );
}

#[test]
fn bulk_admission_allows_same_underlay_candidate_that_beats_lead_next_quantum() {
    let admitted = bulk_striping_admitted_paths(
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
    alternate.snapshot.carrier_inflight_limit_bytes = payload_bytes as u64;
    alternate.snapshot.bytes_in_flight = 34;
    alternate.snapshot.data_level_bytes_in_flight = 34;

    let admitted =
        bulk_striping_admitted_paths(vec![best, alternate], payload_bytes, MuxLimits::default());

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
fn global_discovery_defers_ecf_for_every_underlay() {
    let mut best = candidate(0, 1000.0, 250.0, 300.0);
    best.snapshot.confidence = 1.0;
    let mut cross_underlay = candidate(1, 5000.0, 260.0, 250.0);
    cross_underlay.key.underlay = UnderlayProtocol::Tcp;
    cross_underlay.snapshot.underlay = UnderlayProtocol::Tcp;
    cross_underlay.snapshot.confidence = 1.0;
    let admitted =
        bulk_striping_admitted_paths(vec![best, cross_underlay], 64 * 1024, MuxLimits::default());

    assert_eq!(admitted.len(), 2);
    assert_eq!(
        bulk_candidate_admission_suppression_with_completion_backlog(
            BulkAdmissionCheck {
                best_snapshot: best.snapshot,
                best_eta_ms: best.eta_ms,
                candidate_snapshot: cross_underlay.snapshot,
                candidate_eta_ms: cross_underlay.eta_ms,
                payload_bytes: 64 * 1024,
                mux_limits: MuxLimits::default(),
                position: BulkCandidatePosition::AdditionalPath,
                stream_ordering_debt_bytes: 0,
            },
            0,
            true,
        ),
        Some("ecf_no_completion_gain"),
        "the directional sender applies ECF with its exact completion backlog",
    );
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
        BulkCandidatePosition::FirstPath,
        true,
    );

    assert!(limit < mux_limits.max_path_flight_bytes as u64);
    assert_eq!(limit, 625_000);
}

#[test]
fn explicit_data_level_service_window_preserves_bounded_carrier_feed_headroom() {
    let mut candidate = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 80.0, mbps(1.0));
    candidate.has_durable_product_progress = true;
    candidate.data_level_limit_bytes = 1_000_000;

    let limit = bulk_product_inflight_limit_bytes(
        candidate,
        64 * 1024,
        MuxLimits::default(),
        BulkCandidatePosition::AdditionalPath,
        true,
    );

    assert_eq!(
        limit, 1_000_000,
        "admission must honor the capacity model's already-bounded carrier feed window"
    );
}

#[test]
fn first_ranked_tcp_candidate_obeys_data_sequence_service_window() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        max_stream_window_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload = 64 * 1024;
    let mut active = candidate(0, 10.0, 50.0, 80.0);
    active.key.underlay = UnderlayProtocol::Tcp;
    active.snapshot.underlay = UnderlayProtocol::Tcp;
    active.snapshot.carrier_inflight_limit_bytes = 512 * 1024;
    active.snapshot.bytes_in_flight = 256 * 1024;
    active.snapshot.queue_bytes = 256 * 1024;
    let service_window = 1024 * 1024_u64;
    active.snapshot.has_durable_product_progress = true;
    active.snapshot.data_level_limit_bytes = service_window;
    active.snapshot.data_level_bytes_in_flight = service_window - payload as u64;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: active.snapshot,
            best_eta_ms: active.eta_ms,
            candidate_snapshot: active.snapshot,
            candidate_eta_ms: active.eta_ms,
            payload_bytes: payload,
            mux_limits,
            position: BulkCandidatePosition::FirstPath,
            stream_ordering_debt_bytes: 0,
        }),
        None,
        "the first-ranked TCP candidate remains schedulable within measured service credit"
    );

    active.snapshot.data_level_bytes_in_flight = service_window;
    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: active.snapshot,
            best_eta_ms: active.eta_ms,
            candidate_snapshot: active.snapshot,
            candidate_eta_ms: active.eta_ms,
            payload_bytes: payload,
            mux_limits,
            position: BulkCandidatePosition::FirstPath,
            stream_ordering_debt_bytes: 0,
        }),
        Some("inflight_limit"),
        "ranking cannot mint Data Sequence credit beyond the service window"
    );

    active.snapshot.data_level_bytes_in_flight = service_window;
    active.snapshot.data_level_limit_bytes = service_window * 2;
    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: active.snapshot,
            best_eta_ms: active.eta_ms,
            candidate_snapshot: active.snapshot,
            candidate_eta_ms: active.eta_ms,
            payload_bytes: payload,
            mux_limits,
            position: BulkCandidatePosition::FirstPath,
            stream_ordering_debt_bytes: 0,
        }),
        None,
        "measured Data ACK service growth must reopen placement without reclassifying the path"
    );
}

#[test]
fn first_ranked_quic_candidate_obeys_data_sequence_service_window() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        max_stream_window_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload = 64 * 1024;
    let mut active = candidate(0, 10.0, 50.0, 80.0);
    let service_window = 1024 * 1024_u64;
    active.snapshot.has_durable_product_progress = true;
    active.snapshot.data_level_limit_bytes = service_window;
    active.snapshot.data_level_bytes_in_flight = service_window - payload as u64;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: active.snapshot,
            best_eta_ms: active.eta_ms,
            candidate_snapshot: active.snapshot,
            candidate_eta_ms: active.eta_ms,
            payload_bytes: payload,
            mux_limits,
            position: BulkCandidatePosition::FirstPath,
            stream_ordering_debt_bytes: 0,
        }),
        None,
        "the first-ranked QUIC candidate may feed within its data-level service window"
    );

    active.snapshot.data_level_bytes_in_flight = service_window;
    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: active.snapshot,
            best_eta_ms: active.eta_ms,
            candidate_snapshot: active.snapshot,
            candidate_eta_ms: active.eta_ms,
            payload_bytes: payload,
            mux_limits,
            position: BulkCandidatePosition::FirstPath,
            stream_ordering_debt_bytes: 0,
        }),
        Some("inflight_limit"),
        "native QUIC backpressure and ranked Data Sequence admission remain separate resources"
    );

    active.snapshot.data_level_limit_bytes = service_window * 2;
    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: active.snapshot,
            best_eta_ms: active.eta_ms,
            candidate_snapshot: active.snapshot,
            candidate_eta_ms: active.eta_ms,
            payload_bytes: payload,
            mux_limits,
            position: BulkCandidatePosition::FirstPath,
            stream_ordering_debt_bytes: 0,
        }),
        None,
        "measured Data ACK service growth must reopen QUIC placement without fixed path classification"
    );
}

#[test]
fn contiguous_frontier_uses_native_send_authority_without_a_second_inflight_controller() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        max_stream_window_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload = 64 * 1024;
    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let mut active = candidate(0, 10.0, 50.0, 80.0);
        active.key.underlay = underlay;
        active.snapshot.underlay = underlay;
        active.snapshot.has_durable_product_progress = true;
        active.snapshot.carrier_inflight_limit_bytes = 512 * 1024;
        active.snapshot.data_level_limit_bytes = 1024 * 1024;
        active.snapshot.data_level_bytes_in_flight = 4 * 1024 * 1024;

        assert_eq!(
            bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                best_snapshot: active.snapshot,
                best_eta_ms: active.eta_ms,
                candidate_snapshot: active.snapshot,
                candidate_eta_ms: active.eta_ms,
                payload_bytes: payload,
                mux_limits,
                position: BulkCandidatePosition::ContiguousFrontier,
                stream_ordering_debt_bytes: 0,
            }),
            None,
            "the exact frontier owner is governed by shared credit and native backpressure"
        );

        active.snapshot.queue_bytes = 512 * 1024;
        active.snapshot.bytes_in_flight = 512 * 1024;
        assert_eq!(
            bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                best_snapshot: active.snapshot,
                best_eta_ms: active.eta_ms,
                candidate_snapshot: active.snapshot,
                candidate_eta_ms: active.eta_ms,
                payload_bytes: payload,
                mux_limits,
                position: BulkCandidatePosition::ContiguousFrontier,
                stream_ordering_debt_bytes: 0,
            }),
            Some("inflight_limit"),
            "native queue plus flight remains the exact carrier enqueue boundary"
        );
    }
}

#[test]
fn contiguous_frontier_uses_the_product_service_fallback_without_native_credit() {
    let mux_limits = MuxLimits::default();
    let payload = 64 * 1024;
    let mut active = candidate(0, 10.0, 50.0, 80.0);
    active.snapshot.has_durable_product_progress = true;
    active.snapshot.carrier_inflight_limit_bytes = 0;
    active.snapshot.data_level_limit_bytes = 8 * 1024 * 1024;
    active.snapshot.data_level_bytes_in_flight = 4 * 1024 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: active.snapshot,
            best_eta_ms: active.eta_ms,
            candidate_snapshot: active.snapshot,
            candidate_eta_ms: active.eta_ms,
            payload_bytes: payload,
            mux_limits,
            position: BulkCandidatePosition::ContiguousFrontier,
            stream_ordering_debt_bytes: 0,
        }),
        None,
        "the runtime-derived portable service limit remains authoritative"
    );

    active.snapshot.data_level_bytes_in_flight = mux_limits.max_path_flight_bytes as u64;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: active.snapshot,
            best_eta_ms: active.eta_ms,
            candidate_snapshot: active.snapshot,
            candidate_eta_ms: active.eta_ms,
            payload_bytes: payload,
            mux_limits,
            position: BulkCandidatePosition::ContiguousFrontier,
            stream_ordering_debt_bytes: 0,
        }),
        Some("inflight_limit"),
        "the transport-neutral Product window remains the bounded fallback"
    );
}

#[test]
fn contiguous_frontier_retains_shared_reorder_and_latency_pressure_bounds() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 1024 * 1024,
        max_stream_window_bytes: 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload = 64 * 1024;
    let mut active = candidate(0, 10.0, 50.0, 80.0);
    active.snapshot.has_durable_product_progress = true;
    active.snapshot.carrier_inflight_limit_bytes = 512 * 1024;
    active.snapshot.data_level_limit_bytes = 1024 * 1024;
    active.snapshot.data_level_bytes_in_flight = 1024 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: active.snapshot,
            best_eta_ms: active.eta_ms,
            candidate_snapshot: active.snapshot,
            candidate_eta_ms: active.eta_ms,
            payload_bytes: payload,
            mux_limits,
            position: BulkCandidatePosition::ContiguousFrontier,
            stream_ordering_debt_bytes: mux_limits.max_reorder_bytes as u64,
        }),
        Some("reorder_budget"),
        "frontier ownership cannot extend the shared receive-hole envelope"
    );

    active.snapshot.active_latency_sensitive_flows = 1;
    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: active.snapshot,
            best_eta_ms: active.eta_ms,
            candidate_snapshot: active.snapshot,
            candidate_eta_ms: active.eta_ms,
            payload_bytes: payload,
            mux_limits,
            position: BulkCandidatePosition::ContiguousFrontier,
            stream_ordering_debt_bytes: 0,
        }),
        Some("inflight_limit"),
        "latency pressure keeps the bounded Product horizon"
    );
}

#[test]
fn active_tcp_with_same_path_latency_pressure_uses_one_measured_bdp() {
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
        BulkCandidatePosition::FirstPath,
        true,
    );

    assert_eq!(
        limit,
        bulk_path_bdp_bytes(candidate),
        "qualified delivery and RTT evidence bound bulk handed to the ordered carrier"
    );
    assert!(limit < bulk_pipe_window_bytes(bulk_path_bdp_bytes(candidate)));
    assert!(limit >= payload as u64);
}

#[test]
fn same_path_latency_pressure_applies_to_an_additional_ordering_path() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 32 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 40.0, mbps(80.0));
    candidate.active_latency_sensitive_flows = 1;
    let payload = 64 * 1024;

    assert_eq!(
        bulk_product_inflight_limit_bytes(
            candidate,
            payload,
            mux_limits,
            BulkCandidatePosition::AdditionalPath,
            true,
        ),
        bulk_path_bdp_bytes(candidate),
        "ordering position must not disable load protection on the same ordered carrier"
    );
}

#[test]
fn latency_pressure_uses_carrier_capacity_instead_of_app_limited_product_rate() {
    let mux_limits = MuxLimits::default();
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 40.0, mbps(8.0));
    candidate.rate_scope = crate::scheduler::PathRateScope::PerFlowGoodput;
    candidate.carrier_delivery_rate_bps = Some(mbps(80.0));
    candidate.product_progress_rate_bps = Some(mbps(8.0));
    candidate.active_latency_sensitive_flows = 1;
    let payload = 64 * 1024;

    assert_eq!(
        bulk_product_inflight_limit_bytes(
            candidate,
            payload,
            mux_limits,
            BulkCandidatePosition::AdditionalPath,
            true,
        ),
        bulk_rate_bdp_bytes(mbps(80.0), 40.0),
        "the shared writer window must not feed its own app-limited product rate back into capacity"
    );
}

#[test]
fn latency_pressure_keeps_the_bounded_startup_window_until_rate_is_qualified() {
    let mux_limits = MuxLimits::default();
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 40.0, mbps(1.0));
    candidate.active_latency_sensitive_flows = 1;
    let payload = 64 * 1024;

    assert_eq!(
        bulk_product_inflight_limit_bytes(
            candidate,
            payload,
            mux_limits,
            BulkCandidatePosition::AdditionalPath,
            false,
        ),
        reliable_unproven_path_startup_flight_limit_bytes(mux_limits),
        "an immature point rate must not collapse the portable startup window"
    );
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
        BulkCandidatePosition::FirstPath,
        true,
    );

    assert!(
        limit > bulk_scheduling_horizon_bytes(payload, mux_limits) as u64,
        "latency work on other paths must not shrink this leading path owner to the preemptible horizon"
    );
}

#[test]
fn bulk_service_horizon_is_geometric_mean_not_full_envelope() {
    let payload = 64 * 1024;
    let mux_limits = MuxLimits::default();

    assert_eq!(
        bulk_scheduling_horizon_bytes(payload, mux_limits),
        2 * 1024 * 1024,
        "the service horizon is a preemptible scoring window, not the full product envelope"
    );
}

#[test]
fn active_tcp_with_same_path_latency_pressure_rejects_hidden_command_backlog() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 672.0, mbps(100.0));
    active.queue_bytes = 9 * 1024 * 1024;
    active.data_level_bytes_in_flight = 9 * 1024 * 1024;
    active.product_progress_rate_bps = Some(mbps(10.0));
    active.active_latency_sensitive_flows = 1;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            active,
            100.0,
            64 * 1024,
            MuxLimits::default(),
            BulkCandidatePosition::FirstPath,
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
    active.data_level_bytes_in_flight = (envelope - payload) as u64;
    active.queue_bytes = (envelope - payload) as u64;

    assert_eq!(
        bulk_assigned_product_debt_bytes(active),
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
            BulkCandidatePosition::FirstPath,
        ),
        None,
        "carrier-pending OriginalData is already represented in product flight"
    );

    active.data_level_bytes_in_flight = 0;
    active.queue_bytes = envelope as u64;
    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            active,
            10.0,
            payload,
            mux_limits,
            BulkCandidatePosition::FirstPath,
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
    active.data_level_bytes_in_flight = 8_912_896;
    active.data_level_queue_bytes = 512 * 1024;
    active.queue_bytes = 2 * 1024 * 1024;
    active.session_active_latency_sensitive_flows = 1;
    active.confidence = 1.0;
    active.app_limited = true;

    assert!(
        bulk_candidate_within_reorder_budget(
            active,
            payload,
            mux_limits,
            BulkCandidatePosition::FirstPath,
            0,
            true,
        ),
        "same-path queued bytes are feed/backpressure debt, not cross-path reorder debt"
    );
    assert_ne!(
        bulk_candidate_admission_suppression(
            active,
            117_551.370,
            active,
            117_551.370,
            payload,
            mux_limits,
            BulkCandidatePosition::FirstPath,
        ),
        Some("reorder_budget"),
        "the active leading path may be allowed or backpressured, but same-path backlog must not be reported as cross-path reorder debt"
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
    active.data_level_bytes_in_flight = 341_200;
    active.app_limited = true;

    let check = BulkAdmissionCheck {
        best_snapshot: active,
        best_eta_ms: 50_018.062,
        candidate_snapshot: active,
        candidate_eta_ms: 50_018.062,
        payload_bytes: payload,
        mux_limits,
        position: BulkCandidatePosition::FirstPath,
        stream_ordering_debt_bytes: 0,
    };

    assert_eq!(
        bulk_candidate_admission_suppression_with_completion_backlog(check, 0, false),
        None,
        "a clear-frontier owner without a window-qualified sample retains bounded acquisition credit"
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
    active.data_level_bytes_in_flight = 8 * 1024 * 1024;
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
            BulkCandidatePosition::FirstPath,
        ),
        Some("inflight_limit"),
        "latency-sensitive mixed work must cap total clear-frontier leading path owner credit, not just queued backlog, or stale owner flight becomes read-gap and reinjection debt"
    );
}

#[test]
fn active_udp_owner_with_clear_frontier_uses_product_envelope_not_carrier_cwnd() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 32 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 80.0, mbps(500.0));
    candidate.carrier_inflight_limit_bytes = 128 * 1024;
    candidate.data_level_bytes_in_flight = 96 * 1024;
    candidate.queue_bytes = 16 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            candidate,
            10.0,
            candidate,
            10.0,
            32 * 1024,
            mux_limits,
            BulkCandidatePosition::FirstPath,
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
    candidate.carrier_inflight_limit_bytes = 512 * 1024;
    candidate.data_level_bytes_in_flight = 512 * 1024;
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
            BulkCandidatePosition::FirstPath,
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
    candidate.data_level_queue_bytes = 512 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            candidate,
            10.0,
            candidate,
            10.0,
            payload,
            mux_limits,
            BulkCandidatePosition::FirstPath,
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
    candidate.data_level_bytes_in_flight = 8 * 1024 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            candidate,
            10.0,
            candidate,
            10.0,
            64 * 1024,
            mux_limits,
            BulkCandidatePosition::FirstPath,
        ),
        None,
        "active QUIC leading path ownership is bounded by product resource envelopes; carrier pacing and additional path admission own lower-layer limits"
    );
}

#[test]
fn active_udp_with_latency_pressure_caps_total_owner_credit() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 40.0, mbps(500.0));
    candidate.pacing_rate_bps = mbps(2_000.0);
    candidate.product_progress_rate_bps = Some(mbps(10.0));
    candidate.data_level_bytes_in_flight = 8 * 1024 * 1024;
    candidate.active_latency_sensitive_flows = 1;

    assert_eq!(
        bulk_candidate_admission_suppression(
            candidate,
            10.0,
            candidate,
            10.0,
            64 * 1024,
            mux_limits,
            BulkCandidatePosition::FirstPath,
        ),
        Some("inflight_limit"),
        "QUIC leading path ownership also becomes preemptible under realtime/mixed pressure; carrier cwnd is not the product ceiling, but already-owned product flight must still be bounded"
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
    candidate.data_level_bytes_in_flight = 8 * 1024 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            candidate,
            10.0,
            candidate,
            10.0,
            64 * 1024,
            mux_limits,
            BulkCandidatePosition::FirstPath,
        ),
        None,
        "active QUIC leading path startup may use the product envelope because it is the current ordered owner, not an optional Path"
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
    candidate.data_level_bytes_in_flight = 8 * 1024 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            candidate,
            10.0,
            candidate,
            10.0,
            64 * 1024,
            mux_limits,
            BulkCandidatePosition::FirstPath,
        ),
        None,
        "active TCP leading path startup uses the same carrier-neutral product envelope as QUIC"
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
        BulkCandidatePosition::AdditionalPath,
        true,
    );

    assert!(limit < mux_limits.max_path_flight_bytes as u64);
    assert_eq!(limit, 625_000);
}

#[test]
fn product_inflight_model_uses_delivery_not_transient_pacing_gain() {
    let mux_limits = MuxLimits::default();
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 360.0, mbps(20.0));
    candidate.pacing_rate_bps = mbps(700.0);
    candidate.has_durable_product_progress = true;

    assert_eq!(
        bulk_product_inflight_limit_bytes(
            candidate,
            64 * 1024,
            mux_limits,
            BulkCandidatePosition::AdditionalPath,
            true,
        ),
        1_800_000,
    );
}

#[test]
fn global_same_underlay_eligibility_defers_completion_to_the_stream_scheduler() {
    let mut best = candidate(0, 2958.0, 80.0, 180.0);
    let mut alternate = candidate(1, 3202.0, 180.0, 220.0);
    best.snapshot.confidence = 1.0;
    alternate.snapshot.confidence = 1.0;
    let admitted =
        bulk_striping_admitted_paths(vec![best, alternate], 64 * 1024, MuxLimits::default());

    assert_eq!(admitted.len(), 2);
    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: best.snapshot,
            best_eta_ms: best.eta_ms,
            candidate_snapshot: alternate.snapshot,
            candidate_eta_ms: alternate.eta_ms,
            payload_bytes: 64 * 1024,
            mux_limits: MuxLimits::default(),
            position: BulkCandidatePosition::AdditionalPath,
            stream_ordering_debt_bytes: 0,
        }),
        Some("ecf_no_completion_gain"),
        "the sender still applies ECF once it owns the exact stream backlog",
    );
}

#[test]
fn bulk_admission_rejects_candidate_that_would_exceed_product_inflight_limit() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut saturated = candidate(1, 110.0, 50.0, 500.0);
    saturated.snapshot.carrier_inflight_limit_bytes = 64 * 1024;
    saturated.snapshot.bytes_in_flight = MuxLimits::default().max_path_flight_bytes as u64;

    let admitted =
        bulk_striping_admitted_paths(vec![best, saturated], 16 * 1024, MuxLimits::default());

    assert_eq!(admitted.len(), 1);
    assert_eq!(admitted[0].key.index, 0);
}

#[test]
fn additional_path_uses_a_measured_reorder_allowance_inside_the_stream_envelope() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut extra = candidate(1, 100.0, 50.0, 50.0);
    extra.snapshot.confidence = 0.1;
    extra.snapshot.carrier_inflight_limit_bytes = MuxLimits::default().max_path_flight_bytes as u64;
    extra.snapshot.bytes_in_flight = 128 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            extra.snapshot,
            extra.eta_ms,
            64 * 1024,
            MuxLimits::default(),
            BulkCandidatePosition::AdditionalPath,
        ),
        None,
        "a bounded additional-path flight fits its measured reordering allowance",
    );
}

#[test]
fn same_underlay_data_ack_debt_does_not_reconsume_released_carrier_credit() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        max_stream_window_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload_bytes = 64 * 1024;
    let best = candidate(0, 100.0, 180.0, 500.0);
    let mut extra = candidate(1, 200.0, 180.0, 18.3);
    extra.snapshot.confidence = 0.2;
    extra.snapshot.app_limited = true;
    extra.snapshot.carrier_inflight_limit_bytes = 244_376;
    extra.snapshot.bytes_in_flight = 0;
    extra.snapshot.queue_bytes = 0;
    extra.snapshot.data_level_bytes_in_flight = 262_144;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: best.snapshot,
            best_eta_ms: best.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes,
            mux_limits,
            position: BulkCandidatePosition::AdditionalPath,
            stream_ordering_debt_bytes: 0,
        }),
        None,
        "Data-ACK-pending bytes must not consume a congestion window already released by native ACKs"
    );

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: best.snapshot,
            best_eta_ms: best.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes,
            mux_limits,
            position: BulkCandidatePosition::AdditionalPath,
            stream_ordering_debt_bytes: mux_limits.max_reorder_bytes as u64,
        }),
        Some("reorder_budget"),
        "released carrier credit cannot exceed the connection-wide receive-hole envelope"
    );
}

#[test]
fn cached_tcp_send_window_does_not_mint_data_sequence_credit() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut active = candidate(0, 100.0, 50.0, 0.35);
    active.snapshot.underlay = UnderlayProtocol::Tcp;
    active.snapshot.confidence = 0.1;
    active.snapshot.app_limited = true;
    active.snapshot.carrier_inflight_limit_bytes = 512 * 1024;
    active.snapshot.bytes_in_flight = 256 * 1024;
    active.snapshot.queue_bytes = 256 * 1024;
    active.snapshot.data_level_bytes_in_flight = 8 * 1024 * 1024;
    active.snapshot.product_progress_rate_bps = None;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            active.snapshot,
            active.eta_ms,
            64 * 1024,
            MuxLimits::default(),
            BulkCandidatePosition::FirstPath,
        ),
        Some("inflight_limit"),
        "cached native telemetry may rank a path but cannot authorize unique Data Sequence bytes"
    );
}

#[test]
fn tcp_active_service_under_bulk_demand_is_not_starved_by_latency_pressure_flag() {
    let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 333.0, mbps(0.351));
    active.pacing_rate_bps = mbps(0.351);
    active.data_level_bytes_in_flight = 380_304;
    active.data_level_queue_bytes = 395_112;
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
            BulkCandidatePosition::FirstPath,
        ),
        None,
        "latency-first startup state must not shrink an active bulk leading path owner to a tiny startup-rate BDP while the ordered frontier is clear"
    );
}

#[test]
fn tcp_active_service_app_limited_progress_does_not_shrink_below_startup_headroom() {
    let payload = 64 * 1024;
    let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 333.0, mbps(9.522));
    active.pacing_rate_bps = mbps(9.522);
    active.delivery_rate_bps = mbps(9.522);
    active.product_progress_rate_bps = Some(mbps(9.522));
    active.data_level_bytes_in_flight = 655_360;
    active.data_level_queue_bytes = 327_680;
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
            BulkCandidatePosition::FirstPath,
        ),
        None,
        "app-limited progress on the leading path owner is ACK-clock visibility, not a reason to shrink the leading path feed below startup headroom"
    );
}

#[test]
fn tcp_active_path_with_ordering_debt_obeys_model_based_product_flight_budget() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut active = candidate(0, 100.0, 50.0, 50.0);
    active.snapshot.underlay = UnderlayProtocol::Tcp;
    active.snapshot.confidence = 1.0;
    active.snapshot.carrier_inflight_limit_bytes =
        MuxLimits::default().max_path_flight_bytes as u64;
    active.snapshot.bytes_in_flight = 1024 * 1024;
    active.snapshot.data_level_bytes_in_flight = 1024 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: best.snapshot,
            best_eta_ms: best.eta_ms,
            candidate_snapshot: active.snapshot,
            candidate_eta_ms: active.eta_ms,
            payload_bytes: 64 * 1024,
            mux_limits: MuxLimits::default(),
            position: BulkCandidatePosition::FirstPath,
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
    lagging_active.snapshot.carrier_inflight_limit_bytes =
        MuxLimits::default().max_path_flight_bytes as u64;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: best.snapshot,
            best_eta_ms: best.eta_ms,
            candidate_snapshot: lagging_active.snapshot,
            candidate_eta_ms: lagging_active.eta_ms,
            payload_bytes: 16 * 1024,
            mux_limits: MuxLimits::default(),
            position: BulkCandidatePosition::FirstPath,
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
            position: BulkCandidatePosition::FirstPath,
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
    let service_horizon = bulk_scheduling_horizon_bytes(payload, mux_limits) as u64;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: active.snapshot,
            best_eta_ms: active.eta_ms,
            candidate_snapshot: active.snapshot,
            candidate_eta_ms: active.eta_ms,
            payload_bytes: payload,
            mux_limits,
            position: BulkCandidatePosition::FirstPath,
            stream_ordering_debt_bytes: service_horizon.saturating_add(payload as u64),
        },),
        None,
        "bulk-only lower-frontier progress should use the adaptive BDP budget instead of stalling at the geometric leading path horizon"
    );
}

#[test]
fn latency_pressured_lower_frontier_owner_keeps_preemptible_service_horizon() {
    let mut active = candidate(0, 100.0, 170.0, 500.0);
    active.snapshot.active_latency_sensitive_flows = 1;
    let payload = 64 * 1024;
    let mux_limits = MuxLimits::default();
    let service_horizon = bulk_scheduling_horizon_bytes(payload, mux_limits) as u64;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: active.snapshot,
            best_eta_ms: active.eta_ms,
            candidate_snapshot: active.snapshot,
            candidate_eta_ms: active.eta_ms,
            payload_bytes: payload,
            mux_limits,
            position: BulkCandidatePosition::FirstPath,
            stream_ordering_debt_bytes: service_horizon.saturating_add(payload as u64),
        },),
        Some("reorder_budget"),
        "latency pressure must retain the bounded preemptible leading path horizon"
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
            position: BulkCandidatePosition::FirstPath,
            stream_ordering_debt_bytes: 32 * 1024 * 1024,
        },),
        Some("reorder_budget")
    );
}

#[test]
fn unproven_product_inflight_limit_is_modeled_limit_capped_by_configured_ceiling() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut constrained = candidate(1, 100.1, 50.0, 10.0);
    constrained.snapshot.confidence = 1.0;
    constrained.snapshot.carrier_inflight_limit_bytes = 64 * 1024;
    constrained.snapshot.bytes_in_flight = 1024 * 1024;
    constrained.snapshot.data_level_bytes_in_flight = 1024 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            constrained.snapshot,
            constrained.eta_ms,
            16 * 1024,
            MuxLimits::default(),
            BulkCandidatePosition::AdditionalPath,
        ),
        Some("inflight_limit")
    );
}

#[test]
fn udp_multipath_active_path_ignores_carrier_flight_as_product_stop() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut active = candidate(0, 100.0, 50.0, 500.0);
    active.snapshot.confidence = 1.0;
    active.snapshot.carrier_inflight_limit_bytes = 512 * 1024;
    active.snapshot.bytes_in_flight = 512 * 1024;
    active.snapshot.data_level_bytes_in_flight = 0;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            active.snapshot,
            active.eta_ms,
            64 * 1024,
            MuxLimits::default(),
            BulkCandidatePosition::FirstPath,
        ),
        None
    );
}

#[test]
fn udp_active_ordered_owner_uses_product_envelope_not_carrier_cwnd_gate() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut active = candidate(0, 100.0, 50.0, 500.0);
    active.snapshot.confidence = 1.0;
    active.snapshot.carrier_inflight_limit_bytes = 512 * 1024;
    active.snapshot.bytes_in_flight = 512 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            active.snapshot,
            active.eta_ms,
            64 * 1024,
            MuxLimits::default(),
            BulkCandidatePosition::FirstPath,
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
    active.snapshot.carrier_inflight_limit_bytes = 512 * 1024;
    active.snapshot.bytes_in_flight = 512 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            active.snapshot,
            active.eta_ms,
            64 * 1024,
            MuxLimits::default(),
            BulkCandidatePosition::FirstPath,
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
    active.snapshot.carrier_inflight_limit_bytes = 512 * 1024;
    active.snapshot.bytes_in_flight = 512 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            active.snapshot,
            active.eta_ms,
            64 * 1024,
            MuxLimits::default(),
            BulkCandidatePosition::FirstPath,
        ),
        None
    );
}

#[test]
fn udp_cross_underlay_does_not_reuse_cached_native_credit_as_data_level_gate() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut extra = candidate(1, 100.0, 50.0, 500.0);
    extra.snapshot.confidence = 1.0;
    extra.snapshot.carrier_inflight_limit_bytes = 512 * 1024;
    extra.snapshot.bytes_in_flight = 512 * 1024;
    extra.snapshot.data_level_bytes_in_flight = 64 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            extra.snapshot,
            extra.eta_ms,
            64 * 1024,
            MuxLimits::default(),
            BulkCandidatePosition::AdditionalPath,
        ),
        None,
        "a cached congestion window is feed geometry, not MPP Data Sequence authority"
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
            BulkCandidatePosition::FirstPath,
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
            BulkCandidatePosition::AdditionalPath,
        ),
        Some("inflight_limit")
    );
}

#[test]
fn additional_path_reorder_allowance_does_not_depend_on_probe_confidence() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut extra = candidate(1, 50.0, 50.0, 50.0);
    extra.snapshot.confidence = 0.1;
    extra.snapshot.bytes_in_flight = 384 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            extra.snapshot,
            extra.eta_ms,
            64 * 1024,
            MuxLimits::default(),
            BulkCandidatePosition::AdditionalPath,
        ),
        None,
        "confidence affects completion evidence, not the measured reordering allowance",
    );
}

#[test]
fn durable_progress_without_qualified_rate_keeps_the_discovery_window() {
    let best = candidate(0, 700.0, 50.0, 80.0);
    let mut extra = candidate(1, 100.0, 180.0, 0.351);
    extra.snapshot.has_durable_product_progress = true;
    extra.snapshot.confidence = 1.0;
    extra.snapshot.app_limited = false;
    extra.snapshot.data_level_limit_bytes = 64 * 1024;
    extra.snapshot.data_level_bytes_in_flight = 448 * 1024;
    let check = BulkAdmissionCheck {
        best_snapshot: best.snapshot,
        best_eta_ms: best.eta_ms,
        candidate_snapshot: extra.snapshot,
        candidate_eta_ms: extra.eta_ms,
        payload_bytes: 64 * 1024,
        mux_limits: MuxLimits::default(),
        position: BulkCandidatePosition::AdditionalPath,
        stream_ordering_debt_bytes: 0,
    };

    assert_eq!(
        bulk_candidate_resource_suppression(check, false),
        None,
        "a positive Data ACK must not shrink the acquisition window before rate qualification"
    );
    assert_eq!(
        bulk_candidate_resource_suppression(check, true),
        Some("inflight_limit"),
        "qualified capacity evidence may replace the discovery floor with the measured service window"
    );
}

#[test]
fn unqualified_path_can_fill_native_window_without_completion_authority() {
    let lead = candidate(0, 100.0, 20.0, 80.0);
    let mut extra = candidate(1, 2_000.0, 180.0, 0.351);
    extra.snapshot.confidence = 1.0;
    extra.snapshot.app_limited = false;
    extra.snapshot.carrier_inflight_limit_bytes = 2 * 1024 * 1024;
    extra.snapshot.data_level_limit_bytes = 2 * 1024 * 1024;
    extra.snapshot.data_level_bytes_in_flight = 1024 * 1024;
    let mut check = BulkAdmissionCheck {
        best_snapshot: lead.snapshot,
        best_eta_ms: lead.eta_ms,
        candidate_snapshot: extra.snapshot,
        candidate_eta_ms: extra.eta_ms,
        payload_bytes: 64 * 1024,
        mux_limits: MuxLimits::default(),
        position: BulkCandidatePosition::AdditionalPath,
        stream_ordering_debt_bytes: 0,
    };

    assert_eq!(
        bulk_candidate_admission_suppression_with_completion_backlog(check, 0, false),
        None,
        "an unqualified path may use native congestion credit to finish measuring its pipe"
    );
    check.candidate_snapshot.data_level_bytes_in_flight = 2 * 1024 * 1024;
    assert_eq!(
        bulk_candidate_admission_suppression_with_completion_backlog(check, 0, false),
        Some("inflight_limit"),
        "native acquisition credit remains a bounded product flight window"
    );
}

#[test]
fn app_limited_rate_does_not_reclamp_the_product_service_window() {
    let lead = candidate(0, 2_000.0, 180.0, 200.0);
    let mut extra = candidate(1, 500.0, 400.0, 5.0);
    extra.snapshot.confidence = 1.0;
    extra.snapshot.app_limited = true;
    extra.snapshot.has_durable_product_progress = true;
    extra.snapshot.carrier_inflight_limit_bytes = 2 * 1024 * 1024;
    extra.snapshot.data_level_limit_bytes = 2 * 1024 * 1024;
    extra.snapshot.data_level_bytes_in_flight = 1024 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression_with_completion_backlog(
            BulkAdmissionCheck {
                best_snapshot: lead.snapshot,
                best_eta_ms: lead.eta_ms,
                candidate_snapshot: extra.snapshot,
                candidate_eta_ms: extra.eta_ms,
                payload_bytes: 64 * 1024,
                mux_limits: MuxLimits::default(),
                position: BulkCandidatePosition::AdditionalPath,
                stream_ordering_debt_bytes: 8 * 1024 * 1024,
            },
            16 * 1024 * 1024,
            true,
        ),
        None,
        "an app-limited delivery sample is not a second congestion window",
    );
}

#[test]
fn app_limited_native_path_retains_qualified_completion_authority() {
    let lead = candidate(0, 400.0, 40.0, 400.0);
    let mut extra = candidate(1, 800.0, 180.0, 20.0);
    extra.snapshot.confidence = 1.0;
    extra.snapshot.app_limited = true;
    extra.snapshot.carrier_inflight_limit_bytes = 2 * 1024 * 1024;
    let check = BulkAdmissionCheck {
        best_snapshot: lead.snapshot,
        best_eta_ms: lead.eta_ms,
        candidate_snapshot: extra.snapshot,
        candidate_eta_ms: extra.eta_ms,
        payload_bytes: 64 * 1024,
        mux_limits: MuxLimits::default(),
        position: BulkCandidatePosition::AdditionalPath,
        stream_ordering_debt_bytes: 0,
    };

    assert_eq!(
        bulk_candidate_admission_suppression_with_completion_backlog(check, 0, false),
        None,
        "an immature rate sample must leave bounded acquisition available"
    );
    assert_eq!(
        bulk_candidate_admission_suppression_with_completion_backlog(check, 0, true),
        Some("ecf_no_completion_gain"),
        "a later app-limited poll cannot revoke retained completion evidence"
    );

    let mut latency_check = check;
    latency_check.candidate_snapshot.active_flows = 2;
    latency_check
        .candidate_snapshot
        .active_latency_sensitive_flows = 1;
    assert_eq!(
        bulk_candidate_admission_suppression_with_completion_backlog(latency_check, 0, true),
        Some("ecf_no_completion_gain"),
        "an idle carrier sharing latency traffic retains the conservative ECF boundary"
    );
}

#[test]
fn unqualified_native_acquisition_cannot_reuse_a_larger_product_window() {
    let lead = candidate(0, 900.0, 20.0, 30.0);
    let mut extra = candidate(1, 2_300.0, 900.0, 70.0);
    extra.snapshot.confidence = 1.0;
    extra.snapshot.app_limited = true;
    extra.snapshot.has_durable_product_progress = true;
    extra.snapshot.carrier_inflight_limit_bytes = 4 * 1024 * 1024;
    extra.snapshot.data_level_limit_bytes = 48 * 1024 * 1024;
    extra.snapshot.data_level_bytes_in_flight = 3 * 1024 * 1024;
    let mut check = BulkAdmissionCheck {
        best_snapshot: lead.snapshot,
        best_eta_ms: lead.eta_ms,
        candidate_snapshot: extra.snapshot,
        candidate_eta_ms: extra.eta_ms,
        payload_bytes: 64 * 1024,
        mux_limits: MuxLimits::default(),
        position: BulkCandidatePosition::AdditionalPath,
        stream_ordering_debt_bytes: 15 * 1024 * 1024,
    };

    assert_eq!(
        bulk_candidate_admission_suppression_with_completion_backlog(check, 0, false),
        None,
        "an output without qualified completion evidence may fill current native credit"
    );
    assert_eq!(
        bulk_candidate_admission_suppression_with_completion_backlog(check, 0, true),
        Some("ecf_no_completion_gain"),
        "current app-limited state cannot revoke retained completion evidence"
    );
    check.candidate_snapshot.data_level_bytes_in_flight = 4 * 1024 * 1024;
    assert_eq!(
        bulk_candidate_admission_suppression_with_completion_backlog(check, 0, false),
        Some("inflight_limit"),
        "an older Product service window cannot enlarge native acquisition credit"
    );
}

#[test]
fn tcp_path_uses_product_service_and_connection_reorder_windows() {
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
    extra.snapshot.has_durable_product_progress = true;
    let payload_bytes = 64 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: best.snapshot,
            best_eta_ms: best.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes,
            mux_limits,
            position: BulkCandidatePosition::AdditionalPath,
            stream_ordering_debt_bytes: 40 * 1024 * 1024,
        }),
        None,
        "foreign lower-path debt must not consume this path's independent measured allowance"
    );

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: best.snapshot,
            best_eta_ms: best.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes,
            mux_limits,
            position: BulkCandidatePosition::AdditionalPath,
            stream_ordering_debt_bytes: 4 * 1024 * 1024,
        }),
        None,
        "an additional TCP path still receives enough measured reorder credit to aggregate"
    );

    extra.snapshot.data_level_bytes_in_flight = 24 * 1024 * 1024;
    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: best.snapshot,
            best_eta_ms: best.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes,
            mux_limits,
            position: BulkCandidatePosition::AdditionalPath,
            stream_ordering_debt_bytes: 0,
        }),
        Some("inflight_limit"),
        "the independent candidate-local BDP gate must still bound its own pipe"
    );

    extra.snapshot.data_level_limit_bytes = 64 * 1024 * 1024;
    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: best.snapshot,
            best_eta_ms: best.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes,
            mux_limits,
            position: BulkCandidatePosition::AdditionalPath,
            stream_ordering_debt_bytes: 0,
        }),
        None,
        "a portable TCP product window must not be collapsed to an app-limited rate BDP"
    );

    extra.snapshot.data_level_bytes_in_flight = 1024 * 1024;
    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: best.snapshot,
            best_eta_ms: best.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes,
            mux_limits,
            position: BulkCandidatePosition::AdditionalPath,
            stream_ordering_debt_bytes: 64 * 1024 * 1024,
        }),
        Some("reorder_budget"),
        "candidate flight plus foreign debt must still fit the aggregate stream envelope"
    );
}

#[test]
fn cold_tcp_same_underlay_candidate_uses_bounded_startup_flight_with_an_existing_hole() {
    let mut lead = candidate(0, 1_530_000.0, 50.0, 0.35);
    let mut extra = candidate(1, 3_060_000.0, 50.0, 0.35);
    lead.snapshot.underlay = UnderlayProtocol::Tcp;
    extra.snapshot.underlay = UnderlayProtocol::Tcp;
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
            position: BulkCandidatePosition::AdditionalPath,
            stream_ordering_debt_bytes: 512 * 1024,
        }),
        None,
        "an unmeasured candidate can acquire only its separately bounded startup service"
    );
}

#[test]
fn app_limited_tcp_path_does_not_expand_a_large_existing_hole() {
    let mut lead = candidate(0, 1_061.5, 180.0, 192.5);
    let mut extra = candidate(1, 1_094.8, 180.0, 23.0);
    lead.snapshot.underlay = UnderlayProtocol::Tcp;
    extra.snapshot.underlay = UnderlayProtocol::Tcp;
    extra.snapshot.confidence = 1.0;
    extra.snapshot.app_limited = true;
    extra.snapshot.has_durable_product_progress = true;
    extra.snapshot.data_level_limit_bytes = 3 * 1024 * 1024;
    extra.snapshot.data_level_bytes_in_flight = 2 * 1024 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: lead.snapshot,
            best_eta_ms: lead.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes: 64 * 1024,
            mux_limits: MuxLimits::default(),
            position: BulkCandidatePosition::AdditionalPath,
            stream_ordering_debt_bytes: 28 * 1024 * 1024,
        }),
        Some("ecf_no_completion_gain"),
        "completion ranking cannot authorize tens of megabytes of later data behind a lower-offset owner",
    );
}

#[test]
fn cold_quic_same_underlay_candidate_uses_bounded_startup_flight_with_an_existing_hole() {
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
            position: BulkCandidatePosition::AdditionalPath,
            stream_ordering_debt_bytes: 512 * 1024,
        }),
        None,
        "QUIC startup service remains bounded while Quinn owns native congestion"
    );
}

#[test]
fn additional_quic_path_cannot_exceed_measured_data_level_service_window() {
    let lead = candidate(0, 100.0, 360.0, 100.0);
    let mut extra = candidate(1, 110.0, 360.0, 20.0);
    extra.snapshot.pacing_rate_bps = mbps(700.0);
    extra.snapshot.app_limited = true;
    extra.snapshot.data_level_limit_bytes = 1_800_000;
    extra.snapshot.data_level_bytes_in_flight = 1_800_000;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: lead.snapshot,
            best_eta_ms: lead.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes: 64 * 1024,
            mux_limits: MuxLimits::default(),
            position: BulkCandidatePosition::AdditionalPath,
            stream_ordering_debt_bytes: 0,
        }),
        Some("inflight_limit"),
        "transient QUIC pacing gain must not mint unbounded Data Sequence ownership",
    );
}

#[test]
fn same_underlay_low_confidence_sender_samples_remain_startup_admissible() {
    let mut lead = candidate(0, 98_000.0, 80.0, 1.0);
    let mut extra = candidate(1, 1_500_000.0, 80.0, 1.0);
    lead.snapshot.confidence = 0.1;
    extra.snapshot.confidence = 0.1;
    extra.snapshot.carrier_inflight_limit_bytes = 256 * 1024;
    extra.snapshot.data_level_bytes_in_flight = 128 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: lead.snapshot,
            best_eta_ms: lead.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes: 64 * 1024,
            mux_limits: MuxLimits::default(),
            position: BulkCandidatePosition::AdditionalPath,
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
    extra.snapshot.carrier_inflight_limit_bytes = 16 * 1024 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: lead.snapshot,
            best_eta_ms: lead.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes: 64 * 1024,
            mux_limits: MuxLimits::default(),
            position: BulkCandidatePosition::AdditionalPath,
            stream_ordering_debt_bytes: 0,
        }),
        Some("ecf_no_completion_gain")
    );
    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: lead.snapshot,
            best_eta_ms: lead.eta_ms,
            candidate_snapshot: extra.snapshot,
            candidate_eta_ms: extra.eta_ms,
            payload_bytes: 64 * 1024,
            mux_limits: MuxLimits::default(),
            position: BulkCandidatePosition::AdditionalPath,
            stream_ordering_debt_bytes: 512 * 1024,
        }),
        Some("ecf_no_completion_gain")
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
        position: BulkCandidatePosition::AdditionalPath,
        stream_ordering_debt_bytes: 8 * 1024 * 1024,
    };
    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(check),
        Some("ecf_no_completion_gain"),
        "a lower receive hole cannot extend leading path's completion deadline and authorize more later-offset request work"
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
            position: BulkCandidatePosition::AdditionalPath,
            stream_ordering_debt_bytes: 0,
        }),
        Some("ecf_no_completion_gain")
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
                position: BulkCandidatePosition::AdditionalPath,
                stream_ordering_debt_bytes: 512 * 1024,
            },
            32 * 1024 * 1024,
            true,
        ),
        None,
        "a proven path that completes before the lower leading path backlog adds bulk capacity without extending the existing hole"
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
                position: BulkCandidatePosition::AdditionalPath,
                stream_ordering_debt_bytes: 0,
            },
            32 * 1024 * 1024,
            true,
        ),
        Some("ecf_no_completion_gain"),
        "carrier flight already present in leading path ETA cannot extend the completion deadline twice"
    );
}

#[test]
fn cross_underlay_path_can_join_only_when_it_beats_lead_next_quantum() {
    let best = candidate(0, 500.0, 50.0, 500.0);
    let mut extra = candidate(1, 504.0, 250.0, 500.0);
    extra.snapshot.confidence = 1.0;
    extra.snapshot.has_durable_product_progress = true;
    extra.snapshot.bytes_in_flight = 8 * 1024 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression(
            best.snapshot,
            best.eta_ms,
            extra.snapshot,
            extra.eta_ms,
            512 * 1024,
            MuxLimits::default(),
            BulkCandidatePosition::AdditionalPath,
        ),
        None
    );
}

#[test]
fn cross_underlay_path_is_rejected_when_it_cannot_beat_lead_next_quantum() {
    let mut best = candidate(0, 500.0, 50.0, 500.0);
    best.snapshot.confidence = 1.0;
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
            BulkCandidatePosition::AdditionalPath,
        ),
        Some("ecf_no_completion_gain")
    );
}

#[test]
fn bounded_stream_ordering_debt_uses_completion_and_reorder_limits() {
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
            position: BulkCandidatePosition::AdditionalPath,
            stream_ordering_debt_bytes: 4 * 1024 * 1024,
        },),
        None
    );
}

#[test]
fn active_path_with_ordering_debt_must_still_beat_lead_completion_horizon() {
    let mut best = candidate(0, 10.0, 10.0, 1000.0);
    let mut active_with_debt = candidate(1, 100.0, 10.0, 1000.0);
    best.snapshot.confidence = 1.0;
    active_with_debt.snapshot.confidence = 1.0;
    let payload = 32 * 1024;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: best.snapshot,
            best_eta_ms: best.eta_ms,
            candidate_snapshot: active_with_debt.snapshot,
            candidate_eta_ms: active_with_debt.eta_ms,
            payload_bytes: payload,
            mux_limits: MuxLimits::default(),
            position: BulkCandidatePosition::FirstPath,
            stream_ordering_debt_bytes: 16 * 1024,
        }),
        Some("completion_horizon")
    );
}

#[test]
fn bulk_admission_rejects_saturated_best_candidate() {
    let mut saturated_best = candidate(0, 100.0, 50.0, 500.0);
    saturated_best.snapshot.carrier_inflight_limit_bytes = 64 * 1024;
    saturated_best.snapshot.data_level_bytes_in_flight =
        MuxLimits::default().max_path_flight_bytes as u64;
    let backup = candidate(1, 130.0, 50.0, 500.0);

    let admitted = bulk_striping_admitted_paths(
        vec![saturated_best, backup],
        16 * 1024,
        MuxLimits::default(),
    );

    assert_eq!(admitted.len(), 1);
    assert_eq!(admitted[0].key.index, 1);
}

#[test]
fn low_confidence_additional_path_remains_a_bounded_startup_candidate() {
    let mut best = candidate(0, 1000.0, 180.0, 500.0);
    best.snapshot.confidence = 1.0;
    let mut uncertain = candidate(1, 1350.0, 180.0, 500.0);
    uncertain.key.underlay = UnderlayProtocol::Tcp;
    uncertain.snapshot.underlay = UnderlayProtocol::Tcp;
    uncertain.snapshot.confidence = 0.1;

    let admitted =
        bulk_striping_admitted_paths(vec![best, uncertain], 64 * 1024, MuxLimits::default());

    assert_eq!(admitted.len(), 2);
    assert_eq!(admitted[1].key.index, 1);
}
