use super::*;

fn bulk_evidence(qualified: bool) -> BulkAdmissionEvidence {
    BulkAdmissionEvidence {
        product_assignment_qualified: qualified,
        fresh_completion_rate: qualified,
    }
}
use crate::model::capacity::reliable_unproven_path_startup_flight_limit_bytes;
use crate::protocol::{PathId, PathUsage, UnderlayProtocol};

fn mbps(value: f64) -> f64 {
    value * 1_000_000.0
}

fn publish_configured_product(snapshot: &mut PathSnapshot, mux_limits: MuxLimits) {
    snapshot.data_level_limit_bytes =
        reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes;
}

fn candidate(index: usize, eta_ms: f64, srtt_ms: f64, rate_mbps: f64) -> BulkPathCandidate {
    let mut snapshot = PathSnapshot::new(
        PathId(index as u16),
        UnderlayProtocol::Udp,
        srtt_ms,
        mbps(rate_mbps),
    );
    snapshot.data_level_limit_bytes =
        reliable_bulk_product_windows(MuxLimits::default()).per_output_product_limit_bytes;
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
        snapshot,
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
fn global_discovery_keeps_second_same_underlay_structural_candidate() {
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
fn global_discovery_keeps_optional_measurement_completion_policy_separate() {
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
        bulk_measurement_start_suppression_with_completion_backlog(
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
            bulk_evidence(true),
        ),
        Some("ecf_no_completion_gain"),
        "the optional measurement owner may still decline an inferred-late annotation",
    );
}

#[test]
fn qualified_first_path_uses_published_product_p_not_native_bdp() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 32 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 50.0, mbps(50.0));
    candidate.carrier_delivery_rate_bps = Some(mbps(50.0));
    publish_configured_product(&mut candidate, mux_limits);
    let limit = bulk_product_inflight_limit_bytes(
        candidate,
        64 * 1024,
        mux_limits,
        BulkCandidatePosition::FirstPath,
        true,
    );

    assert_eq!(
        limit,
        reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes,
    );
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
fn sampled_native_shape_does_not_rewrite_product_credit_or_gate_assignment() {
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
        active.snapshot.carrier_inflight_limit_bytes = 64 * 1024;
        active.snapshot.queue_bytes = 8 * 1024 * 1024;
        active.snapshot.bytes_in_flight = 8 * 1024 * 1024;
        active.snapshot.data_level_limit_bytes = 64 * 1024 * 1024;
        active.snapshot.data_level_bytes_in_flight = 0;

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
            "sampled native queue and flight cannot install a second Product admission controller",
        );
    }
}

#[test]
fn tcp_frontier_exhausts_fixed_product_window_until_data_ack_reopens_it() {
    let mux_limits = MuxLimits::default();
    let payload = 64 * 1024;
    let mut active = candidate(0, 10.0, 50.0, 80.0);
    active.key.underlay = UnderlayProtocol::Tcp;
    active.snapshot.underlay = UnderlayProtocol::Tcp;
    active.snapshot.has_durable_product_progress = true;
    active.snapshot.carrier_inflight_limit_bytes = 0;
    active.snapshot.data_level_limit_bytes = payload as u64;
    active.snapshot.data_level_bytes_in_flight = payload as u64 - 1;

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
        "a hard Product envelope rejects a command quantum that would cross P"
    );

    active.snapshot.data_level_bytes_in_flight = payload as u64;
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
        "native writer progress cannot renew Product credit before MPP DataACK lowers exact debt",
    );

    active.snapshot.data_level_bytes_in_flight = 0;
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
        "an exact Product release that makes one complete command quantum available reopens the fixed total window",
    );
}

#[test]
fn quic_contiguous_frontier_uses_product_authority_and_writer_native_backpressure() {
    let mux_limits = MuxLimits::default();
    let payload = 64 * 1024;
    let native_window = 1024 * 1024_u64;
    let product_window = 2 * 1024 * 1024_u64;
    let mut active = candidate(0, 10.0, 100.0, 80.0);
    active.snapshot.carrier_inflight_limit_bytes = native_window;
    active.snapshot.data_level_limit_bytes = product_window;
    active.snapshot.data_level_bytes_in_flight = product_window - payload as u64;
    active.snapshot.bytes_in_flight = native_window;

    let suppression = |snapshot| {
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: snapshot,
            best_eta_ms: active.eta_ms,
            candidate_snapshot: snapshot,
            candidate_eta_ms: active.eta_ms,
            payload_bytes: payload,
            mux_limits,
            position: BulkCandidatePosition::ContiguousFrontier,
            stream_ordering_debt_bytes: 0,
        })
    };

    assert_eq!(suppression(active.snapshot), None);

    let mut product_full = active.snapshot;
    product_full.data_level_bytes_in_flight = product_window;
    assert_eq!(
        suppression(product_full),
        Some("inflight_limit"),
        "native ACK progress cannot recycle N into Product credit while unique debt equals P",
    );

    let mut native_full = active.snapshot;
    native_full.queue_bytes = payload as u64;
    assert_eq!(
        suppression(native_full),
        None,
        "sampled carrier queue and flight remain advisory after Product authority is established",
    );

    let mut reopened = product_full;
    reopened.data_level_bytes_in_flight = product_window - payload as u64;
    reopened.bytes_in_flight = native_window;
    assert_eq!(
        suppression(reopened),
        None,
        "DataACK release and native drain together restore contiguous work conservation",
    );
}

#[test]
fn additional_quic_path_keeps_hol_authority_beside_product_window() {
    let mux_limits = MuxLimits::default();
    let payload = 64 * 1024;
    let best = candidate(0, 500.0, 100.0, 20.0);
    let mut extra = candidate(1, 10.0, 100.0, 500.0);
    extra.snapshot.carrier_inflight_limit_bytes = 1024 * 1024;
    extra.snapshot.data_level_limit_bytes = 8 * 1024 * 1024;
    extra.snapshot.bytes_in_flight = 1024 * 1024 + payload as u64;
    let mut check = BulkAdmissionCheck {
        best_snapshot: best.snapshot,
        best_eta_ms: best.eta_ms,
        candidate_snapshot: extra.snapshot,
        candidate_eta_ms: extra.eta_ms,
        payload_bytes: payload,
        mux_limits,
        position: BulkCandidatePosition::AdditionalPath,
        stream_ordering_debt_bytes: 0,
    };

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(check),
        None,
        "sampled native flight cannot install a second admission gate beside Product and HOL authority",
    );

    check.candidate_snapshot.bytes_in_flight = 0;
    check.stream_ordering_debt_bytes = mux_limits.max_reorder_bytes as u64;
    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(check),
        Some("reorder_budget"),
        "Product headroom does not authorize extending the connection-wide ordered receive hole",
    );
}

#[test]
fn quic_frontier_without_native_telemetry_retains_the_product_fallback() {
    let mux_limits = MuxLimits::default();
    let payload = 64 * 1024;
    let mut active = candidate(0, 10.0, 50.0, 80.0);
    active.key.underlay = UnderlayProtocol::Udp;
    active.snapshot.underlay = UnderlayProtocol::Udp;
    active.snapshot.has_durable_product_progress = true;
    active.snapshot.carrier_inflight_limit_bytes = 0;
    active.snapshot.data_level_limit_bytes = 8 * 1024 * 1024;
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
        "QUIC retains the transport-neutral Product fallback until its bounded native writer head is established"
    );
}

#[test]
fn contiguous_frontier_retains_shared_reorder_bounds_without_latency_rewriting_product() {
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
    active.snapshot.data_level_bytes_in_flight = 512 * 1024;

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
        None,
        "with no cross-path ordering debt, latency demand cannot rewrite Product assignment authority"
    );
}

#[test]
fn same_path_latency_demand_keeps_configured_product_p() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 32 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 672.0, mbps(100.0));
    candidate.carrier_delivery_rate_bps = Some(mbps(100.0));
    candidate.active_latency_sensitive_flows = 1;
    publish_configured_product(&mut candidate, mux_limits);
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
        reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes,
        "qualified C/R remains carrier evidence and cannot become Product authority"
    );
}

#[test]
fn latency_demand_does_not_turn_sampled_rate_into_product_assignment_authority() {
    let mux_limits = MuxLimits::default();
    let payload = 64 * 1024;
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 100.0, mbps(4.0));
    candidate.carrier_delivery_rate_bps = Some(mbps(4.0));
    candidate.active_latency_sensitive_flows = 1;
    publish_configured_product(&mut candidate, mux_limits);

    let authority = bulk_original_data_assignment_authority(
        candidate,
        payload,
        mux_limits,
        BulkCandidatePosition::FirstPath,
        true,
    );

    assert_eq!(
        authority.assignment_limit_bytes, authority.product_limit_bytes,
        "traffic-class service ordering must protect latency; sampled carrier rate must not become a second Product congestion window",
    );
}

#[test]
fn qualified_additional_path_latency_demand_keeps_product_p() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 32 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 40.0, mbps(80.0));
    candidate.active_latency_sensitive_flows = 1;
    publish_configured_product(&mut candidate, mux_limits);
    let payload = 64 * 1024;

    assert_eq!(
        bulk_product_inflight_limit_bytes(
            candidate,
            payload,
            mux_limits,
            BulkCandidatePosition::AdditionalPath,
            true,
        ),
        reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes,
        "qualified additional paths use P; traffic-class arbitration protects latency below assignment"
    );
}

#[test]
fn latency_demand_cannot_select_either_carrier_or_product_sample_as_p() {
    let mux_limits = MuxLimits::default();
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 40.0, mbps(8.0));
    candidate.rate_scope = crate::scheduler::PathRateScope::PerFlowGoodput;
    candidate.carrier_delivery_rate_bps = Some(mbps(80.0));
    candidate.product_progress_rate_bps = Some(mbps(8.0));
    candidate.active_latency_sensitive_flows = 1;
    publish_configured_product(&mut candidate, mux_limits);
    let payload = 64 * 1024;

    assert_eq!(
        bulk_product_inflight_limit_bytes(
            candidate,
            payload,
            mux_limits,
            BulkCandidatePosition::AdditionalPath,
            true,
        ),
        reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes,
        "neither shared-carrier nor app-limited per-flow rate may define Product P"
    );
}

#[test]
fn unqualified_additional_path_keeps_bounded_exploration_until_rate_is_qualified() {
    let mux_limits = MuxLimits::default();
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 40.0, mbps(1.0));
    publish_configured_product(&mut candidate, mux_limits);
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
        "an immature point rate cannot expand or collapse portable exploration E"
    );
}

#[test]
fn session_latency_demand_does_not_shrink_product_reservoir() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 32 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 672.0, mbps(100.0));
    candidate.carrier_delivery_rate_bps = Some(mbps(100.0));
    candidate.session_active_latency_sensitive_flows = 1;
    publish_configured_product(&mut candidate, mux_limits);
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
fn same_path_latency_demand_leaves_native_command_backlog_to_writer_backpressure() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 672.0, mbps(100.0));
    publish_configured_product(&mut active, MuxLimits::default());
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
        None,
        "native command backlog is writer/backpressure state; only exact Product debt consumes P"
    );
}

#[test]
fn active_service_product_debt_is_not_replaced_by_shared_command_queue() {
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
    publish_configured_product(&mut active, mux_limits);
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
        None,
        "a carrier-wide TCP queue may contain other flows and cannot replace exact per-flow Product debt"
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
        bulk_candidate_within_stream_product_resource(active, payload, mux_limits, 0),
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
    publish_configured_product(&mut active, mux_limits);
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
        bulk_measurement_start_suppression_with_completion_backlog(check, 0, bulk_evidence(false),),
        None,
        "a clear-frontier owner without a window-qualified sample retains bounded acquisition credit"
    );
}

#[test]
fn active_tcp_latency_demand_does_not_cap_total_owner_credit_by_sampled_rate() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload = 64 * 1024;
    let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 333.0, mbps(0.369));
    publish_configured_product(&mut active, mux_limits);
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
        None,
        "traffic-class service and native writer arbitration protect latency without converting sampled rate into Product credit"
    );
}

#[test]
fn active_udp_owner_with_clear_frontier_uses_product_envelope_not_carrier_cwnd() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 32 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 80.0, mbps(500.0));
    publish_configured_product(&mut candidate, mux_limits);
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
    publish_configured_product(&mut candidate, mux_limits);
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
    publish_configured_product(&mut candidate, mux_limits);
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
    publish_configured_product(&mut candidate, mux_limits);
    candidate.pacing_rate_bps = mbps(2_000.0);
    candidate.product_progress_rate_bps = Some(mbps(10.0));
    candidate.has_durable_product_progress = true;
    candidate.data_level_bytes_in_flight = 1024 * 1024;

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
fn active_udp_latency_demand_does_not_cap_total_owner_credit_by_sampled_rate() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 40.0, mbps(500.0));
    publish_configured_product(&mut candidate, mux_limits);
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
        None,
        "QUIC writer pacing and traffic-class service protect latency; sampled rate cannot revoke Product P"
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
        Some("inflight_limit"),
        "an unqualified point rate cannot renew eight MiB of Product debt, even for the current ordered owner"
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
        Some("inflight_limit"),
        "an unqualified point rate cannot renew eight MiB of Product debt on TCP"
    );
}

#[test]
fn qualified_additional_path_uses_published_product_p_not_native_bdp() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 32 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 50.0, mbps(50.0));
    candidate.carrier_delivery_rate_bps = Some(mbps(50.0));
    publish_configured_product(&mut candidate, mux_limits);
    let limit = bulk_product_inflight_limit_bytes(
        candidate,
        64 * 1024,
        mux_limits,
        BulkCandidatePosition::AdditionalPath,
        true,
    );

    assert_eq!(
        limit,
        reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes,
    );
}

#[test]
fn qualified_product_p_is_independent_of_delivery_and_pacing_rates() {
    let mux_limits = MuxLimits::default();
    let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 360.0, mbps(20.0));
    candidate.pacing_rate_bps = mbps(700.0);
    candidate.carrier_delivery_rate_bps = Some(mbps(20.0));
    publish_configured_product(&mut candidate, mux_limits);

    assert_eq!(
        bulk_product_inflight_limit_bytes(
            candidate,
            64 * 1024,
            mux_limits,
            BulkCandidatePosition::AdditionalPath,
            true,
        ),
        reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes,
    );
}

#[test]
fn ordinary_same_underlay_admission_keeps_completion_in_ranking() {
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
        None,
        "completion inference may rank the candidate last but cannot revoke its Product resources",
    );
}

#[test]
fn bulk_admission_does_not_treat_native_packet_flight_as_product_debt() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut saturated = candidate(1, 110.0, 50.0, 500.0);
    saturated.snapshot.carrier_inflight_limit_bytes = 64 * 1024;
    saturated.snapshot.bytes_in_flight = MuxLimits::default().max_path_flight_bytes as u64;

    let admitted =
        bulk_striping_admitted_paths(vec![best, saturated], 16 * 1024, MuxLimits::default());

    assert_eq!(admitted.len(), 2);
    assert_eq!(admitted[0].key.index, 0);
    assert_eq!(admitted[1].key.index, 1);
}

#[test]
fn additional_path_uses_the_configured_stream_product_envelope() {
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
        "bounded additional-path Product debt fits configured W",
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
fn stale_tcp_send_window_cannot_mint_additional_path_exploration_credit() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut active = candidate(0, 100.0, 50.0, 0.35);
    active.snapshot.underlay = UnderlayProtocol::Tcp;
    active.snapshot.confidence = 0.1;
    active.snapshot.app_limited = true;
    // The freshness owner represents an expired native C as unavailable.
    active.snapshot.carrier_inflight_limit_bytes = 0;
    active.snapshot.bytes_in_flight = 256 * 1024;
    active.snapshot.queue_bytes = 256 * 1024;
    active.snapshot.data_level_bytes_in_flight = 8 * 1024 * 1024;
    active.snapshot.product_progress_rate_bps = None;

    assert_eq!(
        bulk_product_candidate_resource_suppression(
            BulkAdmissionCheck {
                best_snapshot: best.snapshot,
                best_eta_ms: best.eta_ms,
                candidate_snapshot: active.snapshot,
                candidate_eta_ms: active.eta_ms,
                payload_bytes: 64 * 1024,
                mux_limits: MuxLimits::default(),
                position: BulkCandidatePosition::AdditionalPath,
                stream_ordering_debt_bytes: 0,
            }
            .product_resource_check(false),
        ),
        Some("inflight_limit"),
        "an unqualified additional output cannot exceed its exact E authority"
    );
}

#[test]
fn exact_zero_product_authority_cannot_be_reconstructed_by_admission() {
    let mux_limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 100.0, mbps(500.0));
    path.carrier_inflight_limit_bytes = 6_250_000;
    path.carrier_delivery_rate_bps = Some(mbps(500.0));
    path.product_progress_rate_bps = Some(mbps(500.0));
    path.has_durable_product_progress = true;
    path.data_level_limit_bytes = 0;

    assert!(!bulk_contiguous_frontier_can_accept_enqueue(
        path,
        64 * 1024,
        mux_limits,
    ));
    assert_eq!(
        bulk_original_data_assignment_authority(
            path,
            64 * 1024,
            mux_limits,
            BulkCandidatePosition::AdditionalPath,
            true,
        ),
        BulkOriginalDataAssignmentAuthority {
            product_limit_bytes: 0,
            exploration_limit_bytes: 0,
            assignment_limit_bytes: 0,
            assignment_payload_bytes: 64 * 1024,
        },
    );
}

#[test]
fn bulk_assignment_uses_p_except_unqualified_additional_path_uses_e() {
    let mux_limits = MuxLimits::default();
    let product = reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes;
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 100.0, mbps(500.0));
    path.data_level_limit_bytes = product;
    path.carrier_inflight_limit_bytes = 2 * 1024 * 1024;
    path.confidence = 1.0;
    let exploration = reliable_bulk_unproven_exploration_limit_bytes(path, mux_limits);

    for position in [
        BulkCandidatePosition::FirstPath,
        BulkCandidatePosition::ContiguousFrontier,
    ] {
        let authority =
            bulk_original_data_assignment_authority(path, 64 * 1024, mux_limits, position, false);
        assert_eq!(authority.product_limit_bytes, product);
        assert_eq!(authority.exploration_limit_bytes, exploration);
        assert_eq!(authority.assignment_limit_bytes, product);
    }

    let unqualified = bulk_original_data_assignment_authority(
        path,
        64 * 1024,
        mux_limits,
        BulkCandidatePosition::AdditionalPath,
        false,
    );
    let qualified = bulk_original_data_assignment_authority(
        path,
        64 * 1024,
        mux_limits,
        BulkCandidatePosition::AdditionalPath,
        true,
    );
    assert_eq!(unqualified.assignment_limit_bytes, exploration);
    assert_eq!(qualified.assignment_limit_bytes, product);

    path.data_level_bytes_in_flight = exploration;
    assert!(
        !bulk_original_data_assignment_authority(
            path,
            64 * 1024,
            mux_limits,
            BulkCandidatePosition::AdditionalPath,
            false,
        )
        .has_headroom(path.data_level_bytes_in_flight)
    );
    assert!(
        bulk_original_data_assignment_authority(
            path,
            64 * 1024,
            mux_limits,
            BulkCandidatePosition::AdditionalPath,
            true,
        )
        .has_headroom(path.data_level_bytes_in_flight)
    );
    assert!(
        !bulk_original_data_assignment_authority(
            path,
            64 * 1024,
            mux_limits,
            BulkCandidatePosition::AdditionalPath,
            false,
        )
        .has_headroom(path.data_level_bytes_in_flight),
        "when fresh qualification expires, the same outstanding Product debt returns to E without mutating P",
    );

    let low_limits = MuxLimits {
        max_stream_window_bytes: 12 * 1024,
        max_repair_bytes: 10 * 1024,
        max_reorder_bytes: 8 * 1024,
        max_path_flight_bytes: 4 * 1024,
        ..MuxLimits::default()
    };
    path.data_level_limit_bytes = 64 * 1024;
    let low = bulk_original_data_assignment_authority(
        path,
        1024,
        low_limits,
        BulkCandidatePosition::AdditionalPath,
        false,
    );
    assert_eq!(low.product_limit_bytes, 4 * 1024);
    assert_eq!(low.exploration_limit_bytes, 4 * 1024);
    assert_eq!(low.assignment_limit_bytes, 4 * 1024);
}

#[test]
fn tcp_active_service_is_not_starved_by_session_latency_demand() {
    let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 333.0, mbps(0.351));
    publish_configured_product(&mut active, MuxLimits::default());
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
    publish_configured_product(&mut active, MuxLimits::default());
    active.pacing_rate_bps = mbps(9.522);
    active.delivery_rate_bps = mbps(9.522);
    active.product_progress_rate_bps = Some(mbps(9.522));
    active.has_durable_product_progress = true;
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
fn tcp_active_path_with_ordering_debt_uses_configured_product_resources() {
    let best = candidate(0, 100.0, 50.0, 500.0);
    let mut active = candidate(0, 100.0, 50.0, 50.0);
    active.snapshot.underlay = UnderlayProtocol::Tcp;
    active.snapshot.confidence = 1.0;
    active.snapshot.carrier_inflight_limit_bytes = 512 * 1024;
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
        None,
        "inferred BDP cannot shrink configured W/P while the exact debt fits"
    );
}

#[test]
fn lagging_active_path_remains_admissible_inside_configured_stream_window() {
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
        None,
        "a low inferred BDP changes rank, not configured stream authority"
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
fn active_lower_frontier_owner_uses_configured_stream_product_window() {
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
        "inferred BDP cannot shrink the configured stream Product window"
    );
}

#[test]
fn other_latency_flow_does_not_shrink_configured_lower_frontier_resources() {
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
        None,
        "cross-stream latency demand cannot rewrite configured Product resources"
    );
}

#[test]
fn lead_path_cannot_exceed_configured_stream_resource_window() {
    let lead = candidate(0, 100.0, 170.0, 180.0);
    let mux_limits = MuxLimits::default();
    let stream_limit = reliable_bulk_product_windows(mux_limits).stream_resource_limit_bytes;

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: lead.snapshot,
            best_eta_ms: lead.eta_ms,
            candidate_snapshot: lead.snapshot,
            candidate_eta_ms: lead.eta_ms,
            payload_bytes: 16 * 1024,
            mux_limits,
            position: BulkCandidatePosition::FirstPath,
            stream_ordering_debt_bytes: stream_limit,
        },),
        Some("reorder_budget"),
        "the complete next quantum must fit configured W"
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
        bulk_measurement_start_suppression_with_completion_backlog(
            BulkAdmissionCheck {
                best_snapshot: best.snapshot,
                best_eta_ms: best.eta_ms,
                candidate_snapshot: constrained.snapshot,
                candidate_eta_ms: constrained.eta_ms,
                payload_bytes: 16 * 1024,
                mux_limits: MuxLimits::default(),
                position: BulkCandidatePosition::AdditionalPath,
                stream_ordering_debt_bytes: 0,
            },
            0,
            bulk_evidence(false),
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
        None,
        "without a current native window, native packet flight cannot masquerade as Product debt"
    );
}

#[test]
fn additional_path_product_resources_do_not_depend_on_probe_confidence() {
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
        "confidence affects completion ranking, not configured Product resources",
    );
}

#[test]
fn exact_low_product_p_caps_unqualified_and_qualified_assignment() {
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
        bulk_product_candidate_resource_suppression(check.product_resource_check(false)),
        Some("inflight_limit"),
        "E cannot exceed the exact published Product P"
    );
    assert_eq!(
        bulk_product_candidate_resource_suppression(check.product_resource_check(true)),
        Some("inflight_limit"),
        "qualification cannot replace an exact low Product P"
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
        bulk_measurement_start_suppression_with_completion_backlog(check, 0, bulk_evidence(false),),
        None,
        "an unqualified path may use native congestion credit to finish measuring its pipe"
    );
    check.candidate_snapshot.data_level_bytes_in_flight = 2 * 1024 * 1024 + 64 * 1024;
    assert_eq!(
        bulk_measurement_start_suppression_with_completion_backlog(check, 0, bulk_evidence(false),),
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
        bulk_measurement_start_suppression_with_completion_backlog(
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
            bulk_evidence(true),
        ),
        None,
        "an app-limited delivery sample is not a second congestion window",
    );
}

#[test]
fn optional_measurement_uses_retained_completion_evidence_when_app_limited() {
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
        bulk_measurement_start_suppression_with_completion_backlog(check, 0, bulk_evidence(false),),
        None,
        "an immature rate sample must leave bounded acquisition available"
    );
    assert_eq!(
        bulk_measurement_start_suppression_with_completion_backlog(check, 0, bulk_evidence(true),),
        Some("ecf_no_completion_gain"),
        "retained completion evidence may decline a later inferred-late optional annotation"
    );

    let mut latency_check = check;
    latency_check.candidate_snapshot.active_flows = 2;
    latency_check
        .candidate_snapshot
        .active_latency_sensitive_flows = 1;
    assert_eq!(
        bulk_measurement_start_suppression_with_completion_backlog(
            latency_check,
            0,
            bulk_evidence(true),
        ),
        Some("ecf_no_completion_gain"),
        "latency demand does not erase the optional measurement completion boundary"
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
        bulk_measurement_start_suppression_with_completion_backlog(check, 0, bulk_evidence(false),),
        None,
        "an output without qualified completion evidence may fill current native credit"
    );
    assert_eq!(
        bulk_measurement_start_suppression_with_completion_backlog(check, 0, bulk_evidence(true),),
        Some("ecf_no_completion_gain"),
        "current app-limited state cannot revoke retained completion evidence"
    );
    check.candidate_snapshot.data_level_bytes_in_flight = 4 * 1024 * 1024 + 64 * 1024;
    assert_eq!(
        bulk_measurement_start_suppression_with_completion_backlog(check, 0, bulk_evidence(false),),
        Some("inflight_limit"),
        "an older Product service window cannot enlarge native acquisition credit"
    );
    check.candidate_snapshot.app_limited = false;
    assert_eq!(
        bulk_measurement_start_suppression_with_completion_backlog(check, 0, bulk_evidence(false),),
        Some("inflight_limit"),
        "current non-app-limited state is not rate qualification and cannot revive the old Product window",
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
        "candidate and foreign Product debt together remain inside configured W"
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
        "a smaller exact Product debt remains inside configured W"
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
        None,
        "qualified candidate-local Product debt remains inside the exact published P"
    );

    extra.snapshot.data_level_limit_bytes = 64 * 1024 * 1024;
    assert_eq!(
        bulk_product_candidate_resource_suppression(
            BulkAdmissionCheck {
                best_snapshot: best.snapshot,
                best_eta_ms: best.eta_ms,
                candidate_snapshot: extra.snapshot,
                candidate_eta_ms: extra.eta_ms,
                payload_bytes,
                mux_limits,
                position: BulkCandidatePosition::AdditionalPath,
                stream_ordering_debt_bytes: 0,
            }
            .product_resource_check(false),
        ),
        Some("inflight_limit"),
        "an unqualified additional TCP output cannot use P in place of E"
    );

    assert_eq!(
        bulk_product_candidate_resource_suppression(
            BulkAdmissionCheck {
                best_snapshot: best.snapshot,
                best_eta_ms: best.eta_ms,
                candidate_snapshot: extra.snapshot,
                candidate_eta_ms: extra.eta_ms,
                payload_bytes,
                mux_limits,
                position: BulkCandidatePosition::AdditionalPath,
                stream_ordering_debt_bytes: 0,
            }
            .product_resource_check(true),
        ),
        None,
        "exact Product qualification selects the published P independently of confidence"
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
fn app_limited_tcp_path_uses_configured_stream_authority_not_completion_eta() {
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
        None,
        "completion ranking cannot deny Product debt that remains inside configured W/P",
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
fn same_underlay_clear_frontier_keeps_completion_gain_advisory_after_bulk_proof() {
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
        None
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
        None
    );
}

#[test]
fn request_ordering_debt_does_not_turn_completion_prediction_into_admission() {
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
        None,
        "the configured Product envelope bounds the hole while completion remains advisory"
    );
}

#[test]
fn bulk_rate_proven_same_underlay_path_remains_structurally_admissible() {
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
        None
    );
}

#[test]
fn optional_measurement_admits_path_that_finishes_before_service_tail() {
    let mut lead = candidate(0, 400.0, 360.0, 400.0);
    let mut extra = candidate(1, 800.0, 360.0, 200.0);
    lead.snapshot.confidence = 1.0;
    extra.snapshot.confidence = 1.0;
    extra.snapshot.app_limited = false;

    assert_eq!(
        bulk_measurement_start_suppression_with_completion_backlog(
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
            bulk_evidence(true),
        ),
        None,
        "a proven path that completes before the lower leading path backlog adds bulk capacity without extending the existing hole"
    );
}

#[test]
fn optional_measurement_does_not_double_count_service_carrier_flight() {
    let mut lead = candidate(0, 735.0, 360.0, 400.0);
    let mut extra = candidate(1, 1_100.0, 360.0, 200.0);
    lead.snapshot.confidence = 1.0;
    lead.snapshot.bytes_in_flight = 16 * 1024 * 1024;
    extra.snapshot.confidence = 1.0;
    extra.snapshot.app_limited = false;

    assert_eq!(
        bulk_measurement_start_suppression_with_completion_backlog(
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
            bulk_evidence(true),
        ),
        Some("ecf_no_completion_gain"),
        "carrier flight already present in leading path ETA cannot extend the completion deadline twice"
    );
}

#[test]
fn cross_underlay_path_inside_product_resources_is_admissible_with_favorable_eta() {
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
fn cross_underlay_path_inside_product_resources_is_not_rejected_by_eta() {
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
        None
    );
}

#[test]
fn bounded_stream_ordering_debt_uses_configured_product_window() {
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
