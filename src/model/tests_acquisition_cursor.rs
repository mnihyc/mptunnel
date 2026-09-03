use super::*;
use crate::model::product_qualification::ProductQualificationLedger;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExactId {
    ordinal: u8,
    incarnation: u8,
}

fn id(ordinal: u8) -> ExactId {
    ExactId {
        ordinal,
        incarnation: 1,
    }
}

fn range(start: u64, end: u64) -> OffsetRange {
    OffsetRange::new(start, end).expect("nonempty test range")
}

fn qualification_identity(successor: bool) -> AcquisitionQualificationIdentity {
    let mut ledger = ProductQualificationLedger::default();
    if successor {
        ledger.revoke();
        assert!(
            ledger
                .reactivate_without_evidence()
                .expect("test epoch can reactivate")
        );
    }
    AcquisitionQualificationIdentity::capture(&ledger)
}

fn candidate(exact_id: ExactId, tier: AcquisitionTier) -> AcquisitionCandidate<ExactId> {
    AcquisitionCandidate {
        exact_id,
        qualification_identity: qualification_identity(false),
        tier,
        locally_eligible: true,
        stale: false,
        additional: true,
        qualified: false,
        generation_deficit_bytes: None,
        legal_whole_quantum: true,
    }
}

fn owner(exact_id: ExactId) -> AcquisitionCandidate<ExactId> {
    AcquisitionCandidate {
        additional: false,
        qualified: true,
        generation_deficit_bytes: Some(0),
        legal_whole_quantum: false,
        ..candidate(exact_id, AcquisitionTier::Regular)
    }
}

fn snapshot(
    ordinary_owner_established: bool,
    candidates: Vec<AcquisitionCandidate<ExactId>>,
) -> AcquisitionSnapshot<ExactId> {
    AcquisitionSnapshot {
        pending_range: range(100, 4196),
        quantum_bytes: 4096,
        ordinary_owner_established,
        candidates,
    }
}

fn begin(
    cursor: &mut DirectionLocalAcquisitionCursor<ExactId>,
    snapshot: &AcquisitionSnapshot<ExactId>,
) {
    assert_eq!(
        cursor.begin_dispatch(snapshot.clone()),
        AcquisitionDispatchStart::Started,
    );
}

#[test]
fn one_dispatch_forbids_double_advisory() {
    let observed = snapshot(
        true,
        vec![owner(id(0)), candidate(id(1), AcquisitionTier::Regular)],
    );
    let mut cursor = DirectionLocalAcquisitionCursor::default();
    assert!(cursor.can_begin_dispatch());
    begin(&mut cursor, &observed);
    let attempt = cursor.advisory_attempt().expect("first advisory token");

    assert!(!cursor.can_begin_dispatch());
    assert!(cursor.advisory_attempt().is_none());
    assert_eq!(
        cursor.begin_dispatch(observed.clone()),
        AcquisitionDispatchStart::PendingAttempt,
        "a new dispatch cannot silently replace an outstanding token",
    );
    assert!(cursor.attempt_is_current(&attempt, &observed));
}

#[test]
fn copied_or_replayed_attempt_is_consumed_only_once() {
    let observed = snapshot(
        true,
        vec![
            owner(id(0)),
            candidate(id(1), AcquisitionTier::Regular),
            candidate(id(2), AcquisitionTier::Regular),
        ],
    );
    let mut cursor = DirectionLocalAcquisitionCursor::default();
    begin(&mut cursor, &observed);
    let copied = cursor.advisory_attempt().expect("first token");
    assert_eq!(
        cursor.resolve_attempt(&copied, AcquisitionApplyOutcome::Failed, &observed),
        AcquisitionAttemptResolution::Skipped,
    );

    let newer = cursor.advisory_attempt().expect("sibling token");
    assert_eq!(newer.exact_id, id(2));
    assert_eq!(
        cursor.resolve_attempt(&copied, AcquisitionApplyOutcome::Committed, &observed),
        AcquisitionAttemptResolution::Invalidated,
        "a copied token cannot be consumed twice",
    );
    assert!(
        cursor.attempt_is_current(&newer, &observed),
        "a replay mismatch cannot consume the newer pending attempt",
    );
    assert_eq!(
        cursor.resolve_attempt(&newer, AcquisitionApplyOutcome::Committed, &observed),
        AcquisitionAttemptResolution::Committed,
    );
}

#[test]
fn structurally_equal_aba_dispatch_cannot_reuse_an_old_token() {
    let observed = snapshot(
        true,
        vec![owner(id(0)), candidate(id(1), AcquisitionTier::Regular)],
    );
    let changed = snapshot(
        true,
        vec![
            owner(id(0)),
            AcquisitionCandidate {
                qualification_identity: qualification_identity(true),
                ..candidate(id(1), AcquisitionTier::Regular)
            },
        ],
    );
    let mut cursor = DirectionLocalAcquisitionCursor::default();
    begin(&mut cursor, &observed);
    let old = cursor.advisory_attempt().expect("old token");
    assert_eq!(
        cursor.resolve_attempt(&old, AcquisitionApplyOutcome::Failed, &changed),
        AcquisitionAttemptResolution::Invalidated,
        "the always-present qualification epoch exposes lifecycle change",
    );

    begin(&mut cursor, &observed);
    let current = cursor.advisory_attempt().expect("new A-state token");
    assert_ne!(old.round_id, current.round_id);
    assert_ne!(old.attempt_id, current.attempt_id);
    assert_eq!(
        cursor.resolve_attempt(&old, AcquisitionApplyOutcome::Committed, &observed),
        AcquisitionAttemptResolution::Invalidated,
        "A-B-A structural equality cannot revive the prior round token",
    );
    assert!(cursor.attempt_is_current(&current, &observed));
}

#[test]
fn duplicate_exact_id_fails_dispatch_closed() {
    let duplicate = snapshot(
        true,
        vec![
            owner(id(0)),
            candidate(id(1), AcquisitionTier::Regular),
            candidate(id(1), AcquisitionTier::Backup),
        ],
    );
    let valid = snapshot(
        true,
        vec![owner(id(0)), candidate(id(1), AcquisitionTier::Regular)],
    );
    let mut cursor = DirectionLocalAcquisitionCursor::default();

    assert_eq!(
        cursor.begin_dispatch(duplicate),
        AcquisitionDispatchStart::InvalidSnapshot,
    );
    assert!(cursor.advisory_attempt().is_none());
    begin(&mut cursor, &valid);
    assert!(cursor.advisory_attempt().is_some());
}

#[test]
fn inconsistent_pending_range_and_n_fail_dispatch_closed() {
    let mut invalid = snapshot(
        true,
        vec![owner(id(0)), candidate(id(1), AcquisitionTier::Regular)],
    );
    invalid.quantum_bytes -= 1;
    assert_eq!(
        invalid.acquisition_readiness(),
        AcquisitionReadiness::InvalidSnapshot,
    );
    let mut cursor = DirectionLocalAcquisitionCursor::default();
    assert_eq!(
        cursor.begin_dispatch(invalid),
        AcquisitionDispatchStart::InvalidSnapshot,
    );
    assert!(cursor.advisory_attempt().is_none());
}

#[test]
fn same_snapshot_failure_advances_boundary_into_the_next_dispatch() {
    let observed = snapshot(
        true,
        vec![
            owner(id(0)),
            candidate(id(1), AcquisitionTier::Regular),
            candidate(id(2), AcquisitionTier::Regular),
        ],
    );
    let mut cursor = DirectionLocalAcquisitionCursor::default();
    begin(&mut cursor, &observed);
    let failed = cursor.advisory_attempt().expect("first candidate");
    assert_eq!(failed.exact_id, id(1));
    assert_eq!(
        cursor.resolve_attempt(&failed, AcquisitionApplyOutcome::Failed, &observed),
        AcquisitionAttemptResolution::Skipped,
    );

    begin(&mut cursor, &observed);
    assert_eq!(
        cursor
            .advisory_attempt()
            .expect("new scan starts at persistent successor")
            .exact_id,
        id(2),
    );
}

#[test]
fn committed_attempt_advances_boundary_into_the_next_dispatch() {
    let observed = snapshot(
        true,
        vec![
            owner(id(0)),
            candidate(id(1), AcquisitionTier::Regular),
            candidate(id(2), AcquisitionTier::Regular),
        ],
    );
    let mut cursor = DirectionLocalAcquisitionCursor::default();
    begin(&mut cursor, &observed);
    let committed = cursor.advisory_attempt().expect("first candidate");
    assert_eq!(committed.exact_id, id(1));
    assert_eq!(
        cursor.resolve_attempt(&committed, AcquisitionApplyOutcome::Committed, &observed,),
        AcquisitionAttemptResolution::Committed,
    );

    begin(&mut cursor, &observed);
    assert_eq!(
        cursor
            .advisory_attempt()
            .expect("new scan starts at committed successor")
            .exact_id,
        id(2),
    );
}

#[test]
fn scan_exhaustion_does_not_persist_across_dispatches() {
    let mut blocked = candidate(id(1), AcquisitionTier::Regular);
    blocked.legal_whole_quantum = false;
    let unavailable = snapshot(true, vec![owner(id(0)), blocked]);
    let available = snapshot(
        true,
        vec![owner(id(0)), candidate(id(1), AcquisitionTier::Regular)],
    );
    let mut cursor = DirectionLocalAcquisitionCursor::default();
    begin(&mut cursor, &unavailable);
    assert!(cursor.advisory_attempt().is_none());
    assert!(cursor.advisory_attempt().is_none());

    begin(&mut cursor, &available);
    assert_eq!(
        cursor
            .advisory_attempt()
            .expect("later service wake observes restored authority")
            .exact_id,
        id(1),
    );
}

#[test]
fn round_and_attempt_counter_exhaustion_never_wraps() {
    let observed = snapshot(
        true,
        vec![owner(id(0)), candidate(id(1), AcquisitionTier::Regular)],
    );

    let mut round_cursor = DirectionLocalAcquisitionCursor::default();
    round_cursor.next_round_id = u64::MAX;
    assert_eq!(
        round_cursor.begin_dispatch(observed.clone()),
        AcquisitionDispatchStart::CounterExhausted,
    );
    assert!(round_cursor.counter_exhausted());
    assert!(!round_cursor.can_begin_dispatch());
    assert_eq!(
        round_cursor.begin_dispatch(observed.clone()),
        AcquisitionDispatchStart::CounterExhausted,
    );

    let mut attempt_cursor = DirectionLocalAcquisitionCursor::default();
    begin(&mut attempt_cursor, &observed);
    attempt_cursor.next_attempt_id = u64::MAX;
    assert!(attempt_cursor.advisory_attempt().is_none());
    assert!(attempt_cursor.counter_exhausted());
    assert!(!attempt_cursor.can_begin_dispatch());
    assert_eq!(
        attempt_cursor.begin_dispatch(observed),
        AcquisitionDispatchStart::CounterExhausted,
    );
}

#[test]
fn ordinary_first_owner_precedes_acquisition() {
    let before_owner = snapshot(
        false,
        vec![owner(id(0)), candidate(id(1), AcquisitionTier::Regular)],
    );
    let after_owner = AcquisitionSnapshot {
        ordinary_owner_established: true,
        ..before_owner.clone()
    };
    let mut cursor = DirectionLocalAcquisitionCursor::default();
    begin(&mut cursor, &before_owner);
    assert!(cursor.advisory_attempt().is_none());

    begin(&mut cursor, &after_owner);
    assert_eq!(
        cursor
            .advisory_attempt()
            .expect("acquisition starts only after ordinary ownership")
            .exact_id,
        id(1),
    );
}

#[test]
fn regular_structural_membership_blocks_backup_but_not_regular_siblings() {
    let mut blocked_regular = candidate(id(1), AcquisitionTier::Regular);
    blocked_regular.legal_whole_quantum = false;
    let backup = candidate(id(3), AcquisitionTier::Backup);
    let observed = snapshot(
        true,
        vec![
            owner(id(0)),
            blocked_regular.clone(),
            candidate(id(2), AcquisitionTier::Regular),
            backup.clone(),
        ],
    );
    let mut cursor = DirectionLocalAcquisitionCursor::default();
    begin(&mut cursor, &observed);
    let attempt = cursor
        .advisory_attempt()
        .expect("a legal regular sibling remains");
    assert_eq!(attempt.exact_id, id(2));
    assert_eq!(attempt.tier, AcquisitionTier::Regular);

    blocked_regular.locally_eligible = false;
    let backup_only = snapshot(true, vec![blocked_regular, backup.clone()]);
    let mut backup_cursor = DirectionLocalAcquisitionCursor::default();
    begin(&mut backup_cursor, &backup_only);
    let backup_attempt = backup_cursor
        .advisory_attempt()
        .expect("Backup is selected only with no regular member");
    assert_eq!(backup_attempt.exact_id, backup.exact_id,);

    let regular_again = snapshot(
        true,
        vec![candidate(id(4), AcquisitionTier::Regular), backup.clone()],
    );
    assert_eq!(
        backup_cursor.resolve_attempt(
            &backup_attempt,
            AcquisitionApplyOutcome::Failed,
            &regular_again,
        ),
        AcquisitionAttemptResolution::Invalidated,
        "selected-tier change invalidates instead of retargeting a token",
    );
    begin(&mut backup_cursor, &regular_again);
    let regular = backup_cursor
        .advisory_attempt()
        .expect("regular tier becomes authoritative again");
    assert_eq!(regular.exact_id, id(4));
    assert_eq!(regular.tier, AcquisitionTier::Regular);
}

#[test]
fn readiness_distinguishes_blocked_demand_without_consuming_cursor_state() {
    let mut blocked_regular = candidate(id(1), AcquisitionTier::Regular);
    blocked_regular.legal_whole_quantum = false;
    let backup = candidate(id(2), AcquisitionTier::Backup);
    let established_owner = owner(id(0));
    let blocked = snapshot(
        true,
        vec![
            established_owner.clone(),
            blocked_regular.clone(),
            backup.clone(),
        ],
    );
    assert_eq!(
        blocked.acquisition_readiness(),
        AcquisitionReadiness::Blocked(AcquisitionTier::Regular),
        "an illegal Regular member retains demand without promoting Backup",
    );
    assert!(
        blocked.ordinary_target_preserves_acquisition(&established_owner.exact_id),
        "a blocked acquisition does not stall its established ordinary owner",
    );
    assert!(
        !blocked.ordinary_target_preserves_acquisition(&id(1)),
        "a same-tier unqualified additional output cannot bypass the cursor",
    );
    assert!(
        !blocked.ordinary_target_preserves_acquisition(&backup.exact_id),
        "ordinary fallback cannot smuggle an unqualified Backup into acquisition",
    );

    let ready = snapshot(
        true,
        vec![
            established_owner,
            blocked_regular,
            candidate(id(3), AcquisitionTier::Regular),
        ],
    );
    assert_eq!(
        ready.acquisition_readiness(),
        AcquisitionReadiness::Ready(AcquisitionTier::Regular),
    );
    assert_eq!(
        ready.acquisition_readiness(),
        AcquisitionReadiness::Ready(AcquisitionTier::Regular),
    );

    let mut cursor = DirectionLocalAcquisitionCursor::default();
    begin(&mut cursor, &ready);
    assert_eq!(
        cursor
            .advisory_attempt()
            .expect("preview did not consume the one-use cursor")
            .exact_id,
        id(3)
    );
}

#[test]
fn ordinary_fallback_remains_work_conserving_without_lower_tier_acquisition() {
    let qualified_regular = owner(id(1));
    let unqualified_backup = candidate(id(2), AcquisitionTier::Backup);
    let mut qualified_backup = candidate(id(3), AcquisitionTier::Backup);
    qualified_backup.qualified = true;
    qualified_backup.generation_deficit_bytes = Some(0);
    let observed = snapshot(
        true,
        vec![
            qualified_regular.clone(),
            unqualified_backup.clone(),
            qualified_backup.clone(),
        ],
    );

    assert_eq!(
        observed.acquisition_readiness(),
        AcquisitionReadiness::OrdinaryOnly,
        "nonselected Backup demand cannot become a Regular-tier cursor visit",
    );
    assert!(observed.ordinary_target_preserves_acquisition(&qualified_regular.exact_id));
    assert!(
        !observed.ordinary_target_preserves_acquisition(&unqualified_backup.exact_id),
        "a lower-tier ordinary choice may not start its acquisition generation",
    );
    assert!(
        observed.ordinary_target_preserves_acquisition(&qualified_backup.exact_id),
        "an already-qualified Backup remains a work-conserving ordinary fallback",
    );

    let before_owner = AcquisitionSnapshot {
        ordinary_owner_established: false,
        ..observed
    };
    assert_eq!(
        before_owner.acquisition_readiness(),
        AcquisitionReadiness::OrdinaryFirstOwner,
    );
    assert!(before_owner.ordinary_target_preserves_acquisition(&unqualified_backup.exact_id));

    let backup_only = snapshot(true, vec![unqualified_backup.clone()]);
    assert_eq!(
        backup_only.acquisition_readiness(),
        AcquisitionReadiness::Ready(AcquisitionTier::Backup),
    );
    assert!(
        !backup_only.ordinary_target_preserves_acquisition(&unqualified_backup.exact_id),
        "selected-tier acquisition still belongs to the cursor",
    );
}

#[test]
fn irrelevant_backup_churn_does_not_starve_a_regular_attempt() {
    let original = snapshot(
        true,
        vec![
            candidate(id(1), AcquisitionTier::Regular),
            candidate(id(2), AcquisitionTier::Backup),
        ],
    );
    let changed_backup = snapshot(
        true,
        vec![
            candidate(id(1), AcquisitionTier::Regular),
            AcquisitionCandidate {
                qualification_identity: qualification_identity(true),
                legal_whole_quantum: false,
                ..candidate(id(2), AcquisitionTier::Backup)
            },
        ],
    );
    let mut cursor = DirectionLocalAcquisitionCursor::default();
    begin(&mut cursor, &original);
    let regular = cursor.advisory_attempt().expect("regular attempt");

    assert!(cursor.attempt_is_current(&regular, &changed_backup));
    assert_eq!(
        cursor.resolve_attempt(
            &regular,
            AcquisitionApplyOutcome::Committed,
            &changed_backup,
        ),
        AcquisitionAttemptResolution::Committed,
        "a non-selected Backup cannot invalidate stable Regular authority",
    );
}

#[test]
fn failed_candidate_legality_churn_does_not_block_eligible_sibling() {
    let original = snapshot(
        true,
        vec![
            owner(id(0)),
            candidate(id(1), AcquisitionTier::Regular),
            candidate(id(2), AcquisitionTier::Regular),
        ],
    );
    let mut cursor = DirectionLocalAcquisitionCursor::default();
    begin(&mut cursor, &original);
    let failed = cursor.advisory_attempt().expect("first regular attempt");
    assert_eq!(failed.exact_id, id(1));
    assert_eq!(
        cursor.resolve_attempt(&failed, AcquisitionApplyOutcome::Failed, &original),
        AcquisitionAttemptResolution::Skipped,
    );

    let sibling = cursor.advisory_attempt().expect("eligible sibling attempt");
    assert_eq!(sibling.exact_id, id(2));
    let after_failure = snapshot(
        true,
        vec![
            owner(id(0)),
            AcquisitionCandidate {
                legal_whole_quantum: false,
                ..candidate(id(1), AcquisitionTier::Regular)
            },
            candidate(id(2), AcquisitionTier::Regular),
        ],
    );
    assert!(
        cursor.attempt_is_current(&sibling, &after_failure),
        "the failed sibling's writer-legality change cannot invalidate the exact eligible target",
    );
    assert_eq!(
        cursor.resolve_attempt(&sibling, AcquisitionApplyOutcome::Committed, &after_failure,),
        AcquisitionAttemptResolution::Committed,
    );
}

#[test]
fn attempt_certificate_is_target_local_but_tier_exact() {
    let target = candidate(id(1), AcquisitionTier::Regular);
    let original = snapshot(
        true,
        vec![
            owner(id(0)),
            target.clone(),
            candidate(id(2), AcquisitionTier::Regular),
            candidate(id(3), AcquisitionTier::Backup),
        ],
    );
    let mut cursor = DirectionLocalAcquisitionCursor::default();
    begin(&mut cursor, &original);
    let attempt = cursor.advisory_attempt().expect("exact target attempt");
    assert_eq!(attempt.exact_id, target.exact_id);

    for sibling_legal in [false, true] {
        for sibling_qualified in [false, true] {
            for sibling_member in [false, true] {
                for sibling_backup in [false, true] {
                    for reverse_order in [false, true] {
                        let mut sibling = candidate(
                            id(2),
                            if sibling_backup {
                                AcquisitionTier::Backup
                            } else {
                                AcquisitionTier::Regular
                            },
                        );
                        sibling.locally_eligible = sibling_member;
                        sibling.legal_whole_quantum = sibling_legal;
                        if sibling_qualified {
                            sibling.qualified = true;
                            sibling.generation_deficit_bytes = Some(0);
                        }
                        let mut candidates = vec![owner(id(0)), target.clone(), sibling];
                        if reverse_order {
                            candidates.swap(1, 2);
                        }
                        candidates.push(candidate(id(3), AcquisitionTier::Backup));
                        let current = snapshot(true, candidates);
                        assert!(
                            cursor.attempt_is_current(&attempt, &current),
                            "sibling-only churn cannot alter exact target authority while Regular remains selected",
                        );
                    }
                }
            }
        }
    }

    let mut changed_identity = target.clone();
    changed_identity.exact_id.incarnation += 1;
    let mut changed_qualification = target.clone();
    changed_qualification.qualification_identity = qualification_identity(true);
    let mut changed_role = target.clone();
    changed_role.tier = AcquisitionTier::Backup;
    let mut changed_deficit = target.clone();
    changed_deficit.generation_deficit_bytes = Some(1);
    let mut changed_legality = target.clone();
    changed_legality.legal_whole_quantum = false;
    for changed_target in [
        changed_identity,
        changed_qualification,
        changed_role,
        changed_deficit,
        changed_legality,
    ] {
        let current = snapshot(
            true,
            vec![
                owner(id(0)),
                changed_target,
                candidate(id(2), AcquisitionTier::Regular),
            ],
        );
        assert!(
            !cursor.attempt_is_current(&attempt, &current),
            "any exact-target authority change must invalidate the token",
        );
    }

    let backup_only = snapshot(true, vec![candidate(id(4), AcquisitionTier::Backup)]);
    let mut backup_cursor = DirectionLocalAcquisitionCursor::default();
    begin(&mut backup_cursor, &backup_only);
    let backup_attempt = backup_cursor
        .advisory_attempt()
        .expect("initial Backup attempt");
    let regular_appeared = snapshot(
        true,
        vec![
            candidate(id(5), AcquisitionTier::Regular),
            candidate(id(4), AcquisitionTier::Backup),
        ],
    );
    assert!(
        !backup_cursor.attempt_is_current(&backup_attempt, &regular_appeared),
        "a newly present Regular tier must invalidate an older Backup token",
    );
}

#[test]
fn exact_incarnation_replacement_invalidates_without_retargeting() {
    let old_id = id(1);
    let old_snapshot = snapshot(
        true,
        vec![owner(id(0)), candidate(old_id, AcquisitionTier::Regular)],
    );
    let replacement_id = ExactId {
        ordinal: 1,
        incarnation: 2,
    };
    let replacement_snapshot = snapshot(
        true,
        vec![
            owner(id(0)),
            candidate(replacement_id, AcquisitionTier::Regular),
        ],
    );
    let mut cursor = DirectionLocalAcquisitionCursor::default();
    begin(&mut cursor, &old_snapshot);
    let old = cursor.advisory_attempt().expect("old incarnation token");
    assert_eq!(
        cursor.resolve_attempt(&old, AcquisitionApplyOutcome::Failed, &replacement_snapshot,),
        AcquisitionAttemptResolution::Invalidated,
    );
    begin(&mut cursor, &replacement_snapshot);
    assert_eq!(
        cursor
            .advisory_attempt()
            .expect("replacement receives a fresh visit")
            .exact_id,
        replacement_id,
    );
}

#[test]
fn small_exhaustive_scan_never_repeats_and_respects_selected_tier() {
    for legal_mask in 0_u8..8 {
        for needed_mask in 0_u8..8 {
            for backup_mask in 0_u8..8 {
                let mut candidates = Vec::new();
                for offset in 0..3 {
                    let bit = 1 << offset;
                    let mut item = candidate(
                        id(offset + 1),
                        if backup_mask & bit == 0 {
                            AcquisitionTier::Regular
                        } else {
                            AcquisitionTier::Backup
                        },
                    );
                    item.legal_whole_quantum = legal_mask & bit != 0;
                    if needed_mask & bit == 0 {
                        item.qualified = true;
                        item.generation_deficit_bytes = Some(0);
                    }
                    candidates.push(item);
                }
                let observed = snapshot(true, candidates.clone());
                let selected_tier = select_tier(&candidates);
                let expected: Vec<_> = candidates
                    .iter()
                    .filter(|item| {
                        Some(item.tier) == selected_tier
                            && item.is_nonstale_tier_member(item.tier)
                            && item.needs_acquisition()
                            && item.legal_whole_quantum
                    })
                    .map(|item| item.exact_id)
                    .collect();
                let mut cursor = DirectionLocalAcquisitionCursor::default();
                begin(&mut cursor, &observed);
                let mut actual = Vec::new();
                while let Some(attempt) = cursor.advisory_attempt() {
                    assert!(!actual.contains(&attempt.exact_id));
                    actual.push(attempt.exact_id);
                    assert_eq!(
                        cursor.resolve_attempt(
                            &attempt,
                            AcquisitionApplyOutcome::Failed,
                            &observed,
                        ),
                        AcquisitionAttemptResolution::Skipped,
                    );
                }
                assert_eq!(actual, expected);
                assert!(actual.len() <= candidates.len());
            }
        }
    }
}
