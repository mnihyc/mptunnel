use super::*;
use crate::model::work::CarrierWorkKind;
use crate::protocol::{PathId, UnderlayProtocol};

#[test]
fn only_owner_data_can_own_frontier_or_be_ack_evidence_candidate() {
    assert!(CarrierWorkKind::OwnerData.is_ordering_owner());

    assert!(
        !CarrierWorkKind::RepairData.is_ordering_owner(),
        "repair copies must not own stream ordering or become ACK delivery evidence"
    );
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
    ledger.record_repair(300);

    assert_eq!(ledger.owner_progress_bytes(), 1_000_000);
    assert_eq!(ledger.repair_spent_bytes(), 300);
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
        owner_bytes,
    }
}

#[test]
fn subflow_set_admits_only_positive_bulk_rate_proven_subflow_owner() {
    let mut epoch = FlowSubflowSet::new(key(0), 256 * 1024);

    let rejected = epoch.admit_subflow_owner(SubflowAdmissionInput {
        key: key(1),
        bulk_rate_proven: true,
        startup_owner_allowed: false,
        frontier_clear: true,
        completion_improves: false,
        observed_goodput_non_degrading: true,
        owner_bytes: 64 * 1024,
    });

    assert_eq!(rejected, PathAdmission::ProbeOnly);
    assert!(epoch.admitted_keys().is_empty());

    let admitted = epoch.admit_subflow_owner(SubflowAdmissionInput {
        key: key(1),
        bulk_rate_proven: true,
        startup_owner_allowed: false,
        frontier_clear: true,
        completion_improves: true,
        observed_goodput_non_degrading: true,
        owner_bytes: 64 * 1024,
    });

    assert_eq!(admitted, PathAdmission::Subflow);
    assert_eq!(admitted, PathAdmission::Subflow);
    assert_eq!(epoch.admitted_keys(), &[key(1)]);
}

#[test]
fn subflow_set_rejects_sender_evidence_without_bulk_rate_proof() {
    let payload_bytes = 64 * 1024;
    let mut epoch = FlowSubflowSet::new(key(0), payload_bytes * 3);
    let input = SubflowAdmissionInput {
        key: key(1),
        bulk_rate_proven: false,
        startup_owner_allowed: false,
        frontier_clear: true,
        completion_improves: true,
        observed_goodput_non_degrading: true,
        owner_bytes: payload_bytes,
    };

    let rejected = epoch.admit_subflow_owner(input);
    assert_eq!(
        rejected,
        PathAdmission::ProbeOnly,
        "sender/path-proof evidence is probe evidence, not enough to own ordered product bytes"
    );
    assert!(
        epoch.admitted_keys().is_empty(),
        "unproven paths must not enter the owner Subflow set"
    );
}

#[test]
fn subflow_set_spends_stable_startup_epoch_across_payload_sizes() {
    let startup_credit_bytes = 160 * 1024;
    let mut epoch = FlowSubflowSet::new(key(0), startup_credit_bytes);
    let mut input = SubflowAdmissionInput {
        key: key(1),
        bulk_rate_proven: false,
        startup_owner_allowed: true,
        frontier_clear: true,
        completion_improves: false,
        observed_goodput_non_degrading: true,
        owner_bytes: 64 * 1024,
    };

    assert_eq!(epoch.admit_subflow_owner(input), PathAdmission::Subflow);
    assert!(!epoch.startup_owner_sample_sealed(key(1)));

    input.owner_bytes = 32 * 1024;
    assert_eq!(
        epoch.admit_subflow_owner(input),
        PathAdmission::Subflow,
        "changing frame size must not reset or close the startup epoch"
    );

    input.owner_bytes = 64 * 1024;
    assert_eq!(epoch.admit_subflow_owner(input), PathAdmission::Subflow);
    assert_eq!(
        epoch.startup_owner_sent_bytes(key(1)),
        Some(startup_credit_bytes as u64)
    );
    assert!(epoch.startup_owner_sample_sealed(key(1)));

    input.owner_bytes = 1;
    assert_eq!(
        epoch.admit_subflow_owner(input),
        PathAdmission::ProbeOnly,
        "startup OwnerData must stop at the stable cumulative epoch budget"
    );
    assert_eq!(
        epoch.startup_owner_sent_bytes(key(1)),
        Some(startup_credit_bytes as u64)
    );
}

#[test]
fn subflow_set_seals_near_cap_sample_when_next_frame_cannot_fit() {
    let startup_credit_bytes = 160 * 1024;
    let payload_bytes = 64 * 1024;
    let mut epoch = FlowSubflowSet::new(key(0), startup_credit_bytes);
    let input = startup_input(key(1), payload_bytes);

    assert_eq!(epoch.admit_subflow_owner(input), PathAdmission::Subflow);
    assert_eq!(epoch.admit_subflow_owner(input), PathAdmission::Subflow);
    assert!(epoch.seal_startup_owner_if_next_frame_exceeds_credit(key(1), payload_bytes));
    assert_eq!(
        epoch.startup_owner_sealed_sample_bytes(key(1)),
        Some((2 * payload_bytes) as u64)
    );

    let smaller_later_frame = startup_input(key(1), 16 * 1024);
    assert_eq!(
        epoch.admit_subflow_owner(smaller_later_frame),
        PathAdmission::ProbeOnly,
        "a sealed near-cap sample must not refill when a smaller frame arrives later"
    );
    assert_eq!(
        epoch.startup_owner_sent_bytes(key(1)),
        Some((2 * payload_bytes) as u64)
    );

    epoch.rollback_subflow_owner(input);
    assert_eq!(
        epoch.startup_owner_sealed_sample_bytes(key(1)),
        Some(payload_bytes as u64),
        "an explicit near-cap seal is irrevocable even if a stale rollback arrives"
    );
    assert_eq!(
        epoch.admit_subflow_owner(smaller_later_frame),
        PathAdmission::ProbeOnly
    );
}

#[test]
fn subflow_set_keeps_one_unproven_candidate_exclusive_until_graduation() {
    let payload_bytes = 32 * 1024;
    let mut epoch = FlowSubflowSet::new(key(0), payload_bytes * 4);
    let startup = SubflowAdmissionInput {
        key: key(1),
        bulk_rate_proven: false,
        startup_owner_allowed: true,
        frontier_clear: true,
        completion_improves: false,
        observed_goodput_non_degrading: true,
        owner_bytes: payload_bytes,
    };

    assert_eq!(epoch.admit_subflow_owner(startup), PathAdmission::Subflow);
    assert_eq!(epoch.startup_owner_key(), Some(startup.key));

    let competing_startup = SubflowAdmissionInput {
        key: key(2),
        ..startup
    };
    assert_eq!(
        epoch.admit_subflow_owner(competing_startup),
        PathAdmission::ProbeOnly,
        "a second unproven candidate must not interleave ordered samples"
    );
    assert!(
        !epoch.graduate_startup_owner(competing_startup.key),
        "graduating a different key must not clear the current startup owner"
    );
    assert_eq!(epoch.startup_owner_key(), Some(startup.key));
    assert_eq!(
        epoch.admit_subflow_owner(startup),
        PathAdmission::Subflow,
        "the selected startup candidate keeps its epoch across decisions"
    );

    let measured_but_slower = SubflowAdmissionInput {
        bulk_rate_proven: true,
        completion_improves: false,
        ..startup
    };
    assert_eq!(
        epoch.admit_subflow_owner(measured_but_slower),
        PathAdmission::ProbeOnly,
        "bulk-rate proof does not bypass the measured completion guard"
    );

    assert_eq!(
        epoch.admit_subflow_owner(competing_startup),
        PathAdmission::ProbeOnly,
        "bulk-rate observation alone must not implicitly replace the startup owner"
    );
    assert_eq!(epoch.admitted_keys(), &[startup.key]);
}

#[test]
fn explicit_startup_graduation_allows_a_different_generic_key() {
    let payload_bytes = 32 * 1024;
    let mut epoch = FlowSubflowSet::<u16>::new(0, payload_bytes);
    let first = startup_input(1, payload_bytes);
    let second = startup_input(2, payload_bytes);

    assert_eq!(epoch.admit_subflow_owner(first), PathAdmission::Subflow);
    assert!(epoch.graduate_startup_owner(first.key));
    assert_eq!(epoch.startup_owner_key(), None);
    assert_eq!(epoch.admitted_keys(), &[first.key]);

    assert_eq!(
        epoch.admit_subflow_owner(second),
        PathAdmission::Subflow,
        "explicit graduation must free the startup slot for a different unsampled key"
    );
    assert_eq!(epoch.startup_owner_key(), Some(second.key));
    assert_eq!(epoch.admitted_keys(), &[first.key, second.key]);
}

#[test]
fn graduated_member_cannot_consume_fresh_startup_credit() {
    let payload_bytes = 32 * 1024;
    let mut epoch = FlowSubflowSet::<u16>::new(0, payload_bytes);
    let sampled = startup_input(1, payload_bytes);

    assert_eq!(epoch.admit_subflow_owner(sampled), PathAdmission::Subflow);
    assert!(epoch.graduate_startup_owner(sampled.key));
    assert_eq!(epoch.startup_owner_key(), None);

    assert_eq!(
        epoch.admit_subflow_owner(sampled),
        PathAdmission::ProbeOnly,
        "retained sampled membership must prevent the same unproven key from receiving a fresh startup budget"
    );
    assert_eq!(epoch.startup_owner_key(), None);
    assert_eq!(epoch.admitted_keys(), &[sampled.key]);
}

#[test]
fn subflow_set_rollback_restores_unemitted_startup_credit() {
    let payload_bytes = 64 * 1024;
    let mut epoch = FlowSubflowSet::new(key(0), payload_bytes);
    let input = SubflowAdmissionInput {
        key: key(1),
        bulk_rate_proven: false,
        startup_owner_allowed: true,
        frontier_clear: true,
        completion_improves: false,
        observed_goodput_non_degrading: true,
        owner_bytes: payload_bytes,
    };

    assert_eq!(epoch.admit_subflow_owner(input), PathAdmission::Subflow);
    epoch.rollback_subflow_owner(input);
    assert_eq!(epoch.startup_owner_sent_bytes(input.key), Some(0));
    assert_eq!(
        epoch.admit_subflow_owner(input),
        PathAdmission::Subflow,
        "a queue race that emits no bytes must not consume startup sampling credit"
    );
}

#[test]
fn subflow_set_keeps_service_as_owner_without_spending_subflow_credit() {
    let mut epoch = FlowSubflowSet::new(key(3), 256 * 1024);

    let service = epoch.admit_subflow_owner(SubflowAdmissionInput {
        key: key(3),
        bulk_rate_proven: false,
        startup_owner_allowed: false,
        frontier_clear: false,
        completion_improves: false,
        observed_goodput_non_degrading: false,
        owner_bytes: 64 * 1024,
    });

    assert_eq!(service, PathAdmission::Service);
    assert_eq!(service, PathAdmission::Service);
    assert!(epoch.admitted_keys().is_empty());
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

    assert!(cross_family_reliable_owner_allowed(
        Some(owner),
        candidate,
        true,
        false
    ));
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

    assert!(!cross_family_reliable_owner_allowed(
        Some(owner),
        candidate,
        true,
        false
    ));
    assert!(cross_family_reliable_owner_allowed(
        Some(owner),
        candidate,
        true,
        true
    ));
    assert!(!cross_family_reliable_owner_allowed(
        Some(owner),
        candidate,
        false,
        true
    ));
    assert!(cross_family_reliable_owner_allowed(
        None, candidate, false, true
    ));
}
