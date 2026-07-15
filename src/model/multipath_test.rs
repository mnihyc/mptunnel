use super::*;
use crate::protocol::{PathId, UnderlayProtocol};

#[test]
fn only_owner_data_can_own_frontier_or_be_ack_evidence_candidate() {
    assert!(CarrierWorkKind::OwnerData.is_ordering_owner());

    for kind in [
        CarrierWorkKind::RepairData,
        CarrierWorkKind::Probe,
        CarrierWorkKind::Control,
    ] {
        assert!(
            !kind.is_ordering_owner(),
            "{kind:?} must not own stream ordering or be eligible for STREAM_ACK delivery evidence"
        );
    }
}

#[test]
fn extra_traffic_budget_is_hard_percent_plus_startup_floor() {
    let budget = ExtraTrafficBudget::new(
        1_000_000,
        49_999,
        1024,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );

    assert_eq!(budget.limit_bytes(), 51_024);
    assert!(budget.can_spend(1025));
    assert!(!budget.can_spend(1026));
    assert_eq!(budget.remaining_bytes(), 1025);
}

#[test]
fn extra_traffic_ledger_keeps_owner_and_optional_bytes_separate() {
    let mut ledger = ExtraTrafficLedger::default();
    ledger.record_owner_progress(1_000_000);
    ledger.record_optional(ExtraTrafficKind::Repair, 300);

    assert_eq!(ledger.owner_progress_bytes(), 1_000_000);
    assert_eq!(ledger.optional_spent_bytes(), 300);
    assert_eq!(
        ledger
            .budget(
                1024,
                MppPerformanceConfig {
                    extra_traffic_hint_percent: 5
                }
            )
            .limit_bytes(),
        51_024
    );
}

#[test]
fn sender_extra_budget_only_counts_extra_product_offset_work() {
    assert!(CarrierWorkKind::RepairData.counts_against_sender_extra_budget());
    for kind in [
        CarrierWorkKind::OwnerData,
        CarrierWorkKind::Probe,
        CarrierWorkKind::Control,
    ] {
        assert!(
            !kind.counts_against_sender_extra_budget(),
            "{kind:?} must not be charged to the sender-service extra product-offset budget"
        );
    }
}

fn key(path_id: u16) -> CarrierPathKey {
    CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(path_id),
    }
}

fn startup_input<K>(key: K, owner_bytes: usize) -> SubflowAdmissionInput<K> {
    SubflowAdmissionInput {
        key,
        bulk_rate_proven: false,
        startup_owner_allowed: true,
        frontier_clear: true,
        completion_improves: false,
        observed_goodput_non_degrading: true,
        read_gap: Duration::ZERO,
        owner_bytes,
        optional_overhead_bytes: 0,
    }
}

#[test]
fn subflow_set_admits_only_positive_bulk_rate_proven_subflow_owner() {
    let mut epoch =
        FlowSubflowSet::new(7, key(0), 256 * 1024, 64 * 1024, Duration::from_millis(200));

    let rejected = epoch.admit_subflow_owner(SubflowAdmissionInput {
        key: key(1),
        bulk_rate_proven: true,
        startup_owner_allowed: false,
        frontier_clear: true,
        completion_improves: false,
        observed_goodput_non_degrading: true,
        read_gap: Duration::from_millis(10),
        owner_bytes: 64 * 1024,
        optional_overhead_bytes: 0,
    });

    assert_eq!(rejected.decision, PathAdmissionDecision::ProbeOnly);
    assert!(epoch.members().is_empty());

    let admitted = epoch.admit_subflow_owner(SubflowAdmissionInput {
        key: key(1),
        bulk_rate_proven: true,
        startup_owner_allowed: false,
        frontier_clear: true,
        completion_improves: true,
        observed_goodput_non_degrading: true,
        read_gap: Duration::from_millis(10),
        owner_bytes: 64 * 1024,
        optional_overhead_bytes: 0,
    });

    assert_eq!(admitted.decision, PathAdmissionDecision::AdmitSubflow);
    assert_eq!(admitted.role, PathRuntimeRole::Subflow);
    assert_eq!(epoch.members().len(), 1);
    assert_eq!(epoch.members()[0].key, key(1));
    assert_eq!(epoch.members()[0].role, PathRuntimeRole::Subflow);
}

#[test]
fn subflow_set_rejects_unproven_owner_read_gap_or_overhead() {
    let mut epoch =
        FlowSubflowSet::new(8, key(0), 256 * 1024, 4 * 1024, Duration::from_millis(100));

    let unproven_owner = epoch.admit_subflow_owner(SubflowAdmissionInput {
        key: key(1),
        bulk_rate_proven: false,
        startup_owner_allowed: false,
        frontier_clear: true,
        completion_improves: true,
        observed_goodput_non_degrading: true,
        read_gap: Duration::from_millis(10),
        owner_bytes: 32 * 1024,
        optional_overhead_bytes: 0,
    });
    assert_eq!(
        unproven_owner.decision,
        PathAdmissionDecision::ProbeOnly,
        "liveness/proof-only paths may remain Probe candidates but cannot own product bytes"
    );

    let too_much_read_gap = epoch.admit_subflow_owner(SubflowAdmissionInput {
        key: key(1),
        bulk_rate_proven: true,
        startup_owner_allowed: false,
        frontier_clear: true,
        completion_improves: true,
        observed_goodput_non_degrading: true,
        read_gap: Duration::from_millis(101),
        owner_bytes: 32 * 1024,
        optional_overhead_bytes: 0,
    });
    assert_eq!(too_much_read_gap.decision, PathAdmissionDecision::Standby);

    let too_much_overhead = epoch.admit_subflow_owner(SubflowAdmissionInput {
        key: key(1),
        bulk_rate_proven: true,
        startup_owner_allowed: false,
        frontier_clear: true,
        completion_improves: true,
        observed_goodput_non_degrading: true,
        read_gap: Duration::from_millis(10),
        owner_bytes: 32 * 1024,
        optional_overhead_bytes: 8 * 1024,
    });
    assert_eq!(too_much_overhead.decision, PathAdmissionDecision::Standby);
    assert!(epoch.members().is_empty());
}

#[test]
fn subflow_set_rejects_sender_evidence_without_bulk_rate_proof() {
    let payload_bytes = 64 * 1024;
    let mut epoch =
        FlowSubflowSet::new(10, key(0), payload_bytes * 3, 0, Duration::from_millis(100));
    let input = SubflowAdmissionInput {
        key: key(1),
        bulk_rate_proven: false,
        startup_owner_allowed: false,
        frontier_clear: true,
        completion_improves: true,
        observed_goodput_non_degrading: true,
        read_gap: Duration::ZERO,
        owner_bytes: payload_bytes,
        optional_overhead_bytes: 0,
    };

    let rejected = epoch.admit_subflow_owner(input);
    assert_eq!(
        rejected.decision,
        PathAdmissionDecision::ProbeOnly,
        "sender/path-proof evidence is probe evidence, not enough to own ordered product bytes"
    );
    assert!(
        epoch.members().is_empty(),
        "unproven paths must not enter the owner Subflow set"
    );
}

#[test]
fn subflow_set_spends_stable_startup_epoch_across_payload_sizes() {
    let startup_credit_bytes = 160 * 1024;
    let mut epoch = FlowSubflowSet::new(
        11,
        key(0),
        startup_credit_bytes,
        0,
        Duration::from_millis(100),
    );
    let mut input = SubflowAdmissionInput {
        key: key(1),
        bulk_rate_proven: false,
        startup_owner_allowed: true,
        frontier_clear: true,
        completion_improves: false,
        observed_goodput_non_degrading: true,
        read_gap: Duration::ZERO,
        owner_bytes: 64 * 1024,
        optional_overhead_bytes: 0,
    };

    assert_eq!(
        epoch.admit_subflow_owner(input).decision,
        PathAdmissionDecision::AdmitSubflow
    );
    assert!(!epoch.startup_owner_sample_sealed(key(1)));

    input.owner_bytes = 32 * 1024;
    assert_eq!(
        epoch.admit_subflow_owner(input).decision,
        PathAdmissionDecision::AdmitSubflow,
        "changing frame size must not reset or close the startup epoch"
    );

    input.owner_bytes = 64 * 1024;
    assert_eq!(
        epoch.admit_subflow_owner(input).decision,
        PathAdmissionDecision::AdmitSubflow
    );
    assert_eq!(epoch.members().len(), 1);
    assert_eq!(
        epoch.members()[0].owner_sent_bytes,
        startup_credit_bytes as u64
    );
    assert!(epoch.startup_owner_sample_sealed(key(1)));

    input.owner_bytes = 1;
    assert_eq!(
        epoch.admit_subflow_owner(input).decision,
        PathAdmissionDecision::ProbeOnly,
        "startup OwnerData must stop at the stable cumulative epoch budget"
    );
    assert_eq!(
        epoch.members()[0].owner_sent_bytes,
        startup_credit_bytes as u64
    );
}

#[test]
fn subflow_set_seals_near_cap_sample_when_next_frame_cannot_fit() {
    let startup_credit_bytes = 160 * 1024;
    let payload_bytes = 64 * 1024;
    let mut epoch = FlowSubflowSet::new(
        14,
        key(0),
        startup_credit_bytes,
        0,
        Duration::from_millis(100),
    );
    let input = startup_input(key(1), payload_bytes);

    assert_eq!(
        epoch.admit_subflow_owner(input).decision,
        PathAdmissionDecision::AdmitSubflow
    );
    assert_eq!(
        epoch.admit_subflow_owner(input).decision,
        PathAdmissionDecision::AdmitSubflow
    );
    assert!(epoch.seal_startup_owner_if_next_frame_exceeds_credit(key(1), payload_bytes));
    assert_eq!(
        epoch.startup_owner_sealed_sample_bytes(key(1)),
        Some((2 * payload_bytes) as u64)
    );

    let smaller_later_frame = startup_input(key(1), 16 * 1024);
    assert_eq!(
        epoch.admit_subflow_owner(smaller_later_frame).decision,
        PathAdmissionDecision::ProbeOnly,
        "a sealed near-cap sample must not refill when a smaller frame arrives later"
    );
    assert_eq!(
        epoch.members()[0].owner_sent_bytes,
        (2 * payload_bytes) as u64
    );

    epoch.rollback_subflow_owner(input);
    assert_eq!(
        epoch.startup_owner_sealed_sample_bytes(key(1)),
        Some(payload_bytes as u64),
        "an explicit near-cap seal is irrevocable even if a stale rollback arrives"
    );
    assert_eq!(
        epoch.admit_subflow_owner(smaller_later_frame).decision,
        PathAdmissionDecision::ProbeOnly
    );
}

#[test]
fn subflow_set_keeps_one_unproven_candidate_exclusive_until_graduation() {
    let payload_bytes = 32 * 1024;
    let mut epoch =
        FlowSubflowSet::new(12, key(0), payload_bytes * 4, 0, Duration::from_millis(100));
    let startup = SubflowAdmissionInput {
        key: key(1),
        bulk_rate_proven: false,
        startup_owner_allowed: true,
        frontier_clear: true,
        completion_improves: false,
        observed_goodput_non_degrading: true,
        read_gap: Duration::ZERO,
        owner_bytes: payload_bytes,
        optional_overhead_bytes: 0,
    };

    assert_eq!(
        epoch.admit_subflow_owner(startup).decision,
        PathAdmissionDecision::AdmitSubflow
    );
    assert_eq!(epoch.startup_owner_key(), Some(startup.key));

    let competing_startup = SubflowAdmissionInput {
        key: key(2),
        ..startup
    };
    assert_eq!(
        epoch.admit_subflow_owner(competing_startup).decision,
        PathAdmissionDecision::ProbeOnly,
        "a second unproven candidate must not interleave ordered samples"
    );
    assert!(
        !epoch.graduate_startup_owner(competing_startup.key),
        "graduating a different key must not clear the current startup owner"
    );
    assert_eq!(epoch.startup_owner_key(), Some(startup.key));
    assert_eq!(
        epoch.admit_subflow_owner(startup).decision,
        PathAdmissionDecision::AdmitSubflow,
        "the selected startup candidate keeps its epoch across decisions"
    );

    let measured_but_slower = SubflowAdmissionInput {
        bulk_rate_proven: true,
        completion_improves: false,
        ..startup
    };
    assert_eq!(
        epoch.admit_subflow_owner(measured_but_slower).decision,
        PathAdmissionDecision::ProbeOnly,
        "bulk-rate proof does not bypass the measured completion guard"
    );

    assert_eq!(
        epoch.admit_subflow_owner(competing_startup).decision,
        PathAdmissionDecision::ProbeOnly,
        "bulk-rate observation alone must not implicitly replace the startup owner"
    );
    assert_eq!(epoch.members().len(), 1);
}

#[test]
fn explicit_startup_graduation_allows_a_different_generic_key() {
    let payload_bytes = 32 * 1024;
    let mut epoch = FlowSubflowSet::<u16>::new(13, 0, payload_bytes, 0, Duration::from_millis(100));
    let first = startup_input(1, payload_bytes);
    let second = startup_input(2, payload_bytes);

    assert_eq!(
        epoch.admit_subflow_owner(first).decision,
        PathAdmissionDecision::AdmitSubflow
    );
    assert!(epoch.graduate_startup_owner(first.key));
    assert_eq!(epoch.startup_owner_key(), None);
    assert_eq!(epoch.members().len(), 1);
    assert_eq!(epoch.members()[0].key, first.key);

    assert_eq!(
        epoch.admit_subflow_owner(second).decision,
        PathAdmissionDecision::AdmitSubflow,
        "explicit graduation must free the startup slot for a different unsampled key"
    );
    assert_eq!(epoch.startup_owner_key(), Some(second.key));
    assert_eq!(epoch.members().len(), 2);
    assert_eq!(epoch.members()[0].key, first.key);
}

#[test]
fn graduated_member_cannot_consume_fresh_startup_credit() {
    let payload_bytes = 32 * 1024;
    let mut epoch = FlowSubflowSet::<u16>::new(14, 0, payload_bytes, 0, Duration::from_millis(100));
    let sampled = startup_input(1, payload_bytes);

    assert_eq!(
        epoch.admit_subflow_owner(sampled).decision,
        PathAdmissionDecision::AdmitSubflow
    );
    assert!(epoch.graduate_startup_owner(sampled.key));
    assert_eq!(epoch.startup_owner_key(), None);

    assert_eq!(
        epoch.admit_subflow_owner(sampled).decision,
        PathAdmissionDecision::ProbeOnly,
        "retained sampled membership must prevent the same unproven key from receiving a fresh startup budget"
    );
    assert_eq!(epoch.startup_owner_key(), None);
    assert_eq!(epoch.members().len(), 1);
    assert_eq!(epoch.members()[0].owner_sent_bytes, payload_bytes as u64);
}

#[test]
fn subflow_set_rollback_restores_unemitted_startup_credit() {
    let payload_bytes = 64 * 1024;
    let mut epoch = FlowSubflowSet::new(13, key(0), payload_bytes, 0, Duration::from_millis(100));
    let input = SubflowAdmissionInput {
        key: key(1),
        bulk_rate_proven: false,
        startup_owner_allowed: true,
        frontier_clear: true,
        completion_improves: false,
        observed_goodput_non_degrading: true,
        read_gap: Duration::ZERO,
        owner_bytes: payload_bytes,
        optional_overhead_bytes: 0,
    };

    assert_eq!(
        epoch.admit_subflow_owner(input).decision,
        PathAdmissionDecision::AdmitSubflow
    );
    epoch.rollback_subflow_owner(input);
    assert_eq!(epoch.members()[0].owner_sent_bytes, 0);
    assert_eq!(
        epoch.admit_subflow_owner(input).decision,
        PathAdmissionDecision::AdmitSubflow,
        "a queue race that emits no bytes must not consume startup sampling credit"
    );
}

#[test]
fn subflow_set_keeps_service_as_owner_without_spending_subflow_credit() {
    let mut epoch =
        FlowSubflowSet::new(9, key(3), 256 * 1024, 16 * 1024, Duration::from_millis(100));

    let service = epoch.admit_subflow_owner(SubflowAdmissionInput {
        key: key(3),
        bulk_rate_proven: false,
        startup_owner_allowed: false,
        frontier_clear: false,
        completion_improves: false,
        observed_goodput_non_degrading: false,
        read_gap: Duration::from_secs(10),
        owner_bytes: 64 * 1024,
        optional_overhead_bytes: 16 * 1024,
    });

    assert_eq!(service.decision, PathAdmissionDecision::Service);
    assert_eq!(service.role, PathRuntimeRole::Service);
    assert_eq!(epoch.optional_overhead_spent_bytes(), 0);
    assert!(epoch.members().is_empty());
}

#[test]
fn same_family_candidate_remains_reliable_owner_eligible() {
    let owner = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };

    assert_eq!(
        cross_family_reliable_owner_health(Some(owner), true, candidate, true, false),
        CarrierFamilyHealth::Healthy
    );
    assert!(
        cross_family_reliable_owner_health(Some(owner), true, candidate, true, false)
            .reliable_owner_allowed()
    );
}

#[test]
fn cross_family_reliable_owner_requires_lower_frontier_continuation_and_bulk_rate() {
    let owner = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };

    assert!(
        !cross_family_reliable_owner_health(Some(owner), true, candidate, true, false)
            .reliable_owner_allowed()
    );
    assert!(
        cross_family_reliable_owner_health(Some(owner), true, candidate, true, true)
            .reliable_owner_allowed()
    );
    assert_eq!(
        cross_family_reliable_owner_health(Some(owner), true, candidate, false, true),
        CarrierFamilyHealth::ProbeOnly
    );
    assert_eq!(
        cross_family_reliable_owner_health(Some(owner), false, candidate, true, false),
        CarrierFamilyHealth::RepairOnly
    );
    assert_eq!(
        cross_family_reliable_owner_health(Some(owner), false, candidate, false, true),
        CarrierFamilyHealth::DisabledForReliableOwner
    );
    assert!(
        cross_family_reliable_owner_health(None, false, candidate, false, true)
            .reliable_owner_allowed()
    );
}
