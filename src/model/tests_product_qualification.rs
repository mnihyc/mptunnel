use super::*;

fn range(start: u64, end: u64) -> OffsetRange {
    OffsetRange::new(start, end).expect("non-empty test range")
}

fn tag(
    ledger: &mut ProductQualificationLedger,
    floor: u64,
    quantum: u64,
    range: OffsetRange,
) -> ProductQualificationReceipt {
    ledger
        .tag_admitted_original(floor, quantum, range)
        .expect("valid admission")
        .expect("positive deficit")
}

fn assert_invariant(ledger: &ProductQualificationLedger) {
    assert!(ledger.invariant().holds(), "{ledger:#?}");
}

#[test]
fn fresh_active_authority_can_freeze_parameters_and_admit() {
    let mut ledger = ProductQualificationLedger::default();
    assert_eq!(ledger.authority(), ProductQualificationAuthority::Active);
    assert_eq!(
        ledger.deficit_bytes(),
        None,
        "F is not frozen before admission"
    );
    let initial_epoch = ledger.epoch();
    let before = ledger.invariant();
    assert_eq!(
        ledger.tag_admitted_original(0, 4, range(0, 4)),
        Err(ProductQualificationAdmissionError::InvalidFloor)
    );
    assert_eq!(ledger.invariant(), before);

    let receipt = tag(&mut ledger, 8, 4, range(0, 4));
    assert_eq!(receipt.tagged_range(), range(0, 4));
    assert_eq!(receipt.epoch, initial_epoch);
    assert_eq!(ledger.deficit_bytes(), Some(4));
    assert_eq!(ledger.authority(), ProductQualificationAuthority::Active);
    assert_eq!(ledger.invariant().floor_bytes, Some(8));
    assert_eq!(ledger.invariant().max_quantum_bytes, Some(4));
    assert_invariant(&ledger);
}

#[test]
fn pre_generation_revoke_advances_epoch_and_only_exact_reactivation_restores_admission() {
    let mut ledger = ProductQualificationLedger::default();
    let initial_epoch = ledger.epoch();
    ledger.revoke();
    let revoked_epoch = ledger.epoch();
    assert_ne!(revoked_epoch, initial_epoch);
    assert_eq!(ledger.authority(), ProductQualificationAuthority::Revoked);
    assert_eq!(
        ledger.tag_admitted_original(4, 4, range(0, 4)),
        Err(ProductQualificationAdmissionError::AuthorityRevoked)
    );

    assert_eq!(ledger.reactivate_without_evidence(), Ok(true));
    assert_eq!(ledger.reactivate_without_evidence(), Ok(false));
    let receipt = tag(&mut ledger, 4, 4, range(0, 4));
    assert_eq!(receipt.epoch, revoked_epoch);
    assert_eq!(ledger.release_exact(receipt, range(0, 4)), 4);
    assert!(ledger.qualified());
    assert_invariant(&ledger);
}

#[test]
fn duplicate_revoke_does_not_advance_epoch_repeatedly() {
    let mut ledger = ProductQualificationLedger::default();
    ledger.revoke();
    let revoked_epoch = ledger.epoch();
    ledger.revoke();
    assert_eq!(ledger.epoch(), revoked_epoch);
    assert_invariant(&ledger);
}

#[test]
fn delayed_overlapping_old_receipt_cannot_qualify_successor_generation() {
    let mut ledger = ProductQualificationLedger::default();
    let old = tag(&mut ledger, 4, 4, range(100, 104));
    ledger.revoke();
    assert_eq!(ledger.reactivate_without_evidence(), Ok(true));
    let current = tag(&mut ledger, 4, 4, range(100, 104));

    assert_eq!(ledger.release_exact(old, range(100, 104)), 0);
    assert_eq!(ledger.invariant().verified_bytes, 0);
    assert_eq!(ledger.invariant().outstanding_tag_bytes, 4);
    assert_eq!(ledger.release_exact(current, range(100, 104)), 4);
    assert!(ledger.qualified());
    assert_invariant(&ledger);
}

#[test]
fn duplicate_and_split_receipts_are_byte_idempotent() {
    let mut ledger = ProductQualificationLedger::default();
    let receipt = tag(&mut ledger, 8, 8, range(10, 18));
    let left = receipt.intersect(range(10, 14)).expect("left split");
    let right = receipt.intersect(range(14, 18)).expect("right split");

    assert_eq!(ledger.release_exact(left, range(10, 14)), 4);
    assert_eq!(ledger.release_exact(left, range(10, 14)), 0);
    assert_eq!(ledger.release_ambiguous(right, range(14, 18)), 4);
    assert_eq!(ledger.release_exact(right, range(14, 18)), 0);
    assert_eq!(ledger.invariant().verified_bytes, 4);
    assert_eq!(ledger.deficit_bytes(), Some(4));
    assert_invariant(&ledger);
}

#[test]
fn admitted_quantum_larger_than_q_is_rejected_without_mutation() {
    let mut ledger = ProductQualificationLedger::default();
    let before = ledger.invariant();
    assert_eq!(
        ledger.tag_admitted_original(8, 4, range(0, 5)),
        Err(ProductQualificationAdmissionError::QuantumExceedsMaximum)
    );
    assert_eq!(ledger.invariant(), before);

    let receipt = tag(&mut ledger, 8, 4, range(10, 14));
    let active_before = ledger.invariant();
    assert_eq!(
        ledger.tag_admitted_original(8, 4, range(20, 25)),
        Err(ProductQualificationAdmissionError::QuantumExceedsMaximum)
    );
    assert_eq!(ledger.invariant(), active_before);
    assert_eq!(ledger.release_exact(receipt, range(10, 14)), 4);
    assert_invariant(&ledger);
}

#[test]
fn frozen_floor_or_quantum_mismatch_cannot_mutate_active_generation() {
    let mut ledger = ProductQualificationLedger::default();
    let receipt = tag(&mut ledger, 8, 4, range(0, 4));
    let before = ledger.invariant();

    for (floor, quantum) in [(9, 4), (8, 5)] {
        assert_eq!(
            ledger.tag_admitted_original(floor, quantum, range(20, 24)),
            Err(ProductQualificationAdmissionError::FrozenParametersMismatch)
        );
        assert_eq!(ledger.invariant(), before);
    }
    assert_eq!(ledger.release_exact(receipt, range(0, 4)), 4);
    assert_invariant(&ledger);
}

#[test]
fn revoke_clears_authority_and_reactivation_uses_a_fresh_epoch() {
    let mut ledger = ProductQualificationLedger::default();
    let old = tag(&mut ledger, 4, 4, range(0, 4));
    ledger.revoke();
    assert_eq!(ledger.authority(), ProductQualificationAuthority::Revoked);
    assert_eq!(ledger.deficit_bytes(), None);
    assert!(!ledger.qualified());
    assert_eq!(ledger.release_exact(old, range(0, 4)), 0);

    assert_eq!(
        ledger.tag_admitted_original(3, 2, range(100, 102)),
        Err(ProductQualificationAdmissionError::AuthorityRevoked)
    );
    assert_eq!(ledger.reactivate_without_evidence(), Ok(true));
    let current = tag(&mut ledger, 3, 2, range(100, 102));
    assert_ne!(current.epoch, old.epoch);
    assert_eq!(ledger.release_exact(current, range(100, 102)), 2);
    let final_receipt = tag(&mut ledger, 3, 2, range(200, 202));
    assert_eq!(final_receipt.tagged_range(), range(200, 201));
    assert_eq!(ledger.release_exact(final_receipt, range(200, 202)), 1);
    assert!(ledger.qualified());
    assert_invariant(&ledger);
}

#[test]
fn epoch_exhaustion_is_permanent_and_fails_closed() {
    let mut ledger = ProductQualificationLedger::active_at_epoch_for_test(u64::MAX);
    ledger.revoke();
    assert_eq!(ledger.authority(), ProductQualificationAuthority::Exhausted);
    assert_eq!(
        ledger.tag_admitted_original(4, 4, range(0, 4)),
        Err(ProductQualificationAdmissionError::EpochExhausted)
    );
    let exhausted = ledger.invariant();
    assert!(exhausted.holds());

    assert_eq!(
        ledger.tag_admitted_original(4, 4, range(0, 4)),
        Err(ProductQualificationAdmissionError::EpochExhausted)
    );
    assert_eq!(
        ledger.reactivate_without_evidence(),
        Err(ProductQualificationAdmissionError::EpochExhausted)
    );
    ledger.revoke();
    assert_eq!(ledger.invariant(), exhausted);
}

#[test]
fn final_atomic_quantum_has_strictly_less_than_q_untagged_surplus() {
    let mut ledger = ProductQualificationLedger::default();
    let first = tag(&mut ledger, 5, 4, range(0, 4));
    assert_eq!(ledger.release_exact(first, range(0, 4)), 4);

    let complete_admitted_quantum = range(100, 104);
    let final_receipt = tag(&mut ledger, 5, 4, complete_admitted_quantum);
    let untagged_surplus = complete_admitted_quantum.len() - final_receipt.tagged_range().len();
    assert_eq!(final_receipt.tagged_range(), range(100, 101));
    assert!(
        untagged_surplus < 4,
        "N - d must be strictly below frozen Q"
    );
    assert_eq!(
        ledger.release_exact(final_receipt, complete_admitted_quantum),
        1
    );
    assert!(ledger.qualified());
    assert_invariant(&ledger);
}

#[test]
fn normalized_t_satisfies_range_count_and_volume_bounds() {
    let mut ledger = ProductQualificationLedger::default();
    let a = tag(&mut ledger, 8, 2, range(0, 2));
    let b = tag(&mut ledger, 8, 2, range(4, 6));
    let c = tag(&mut ledger, 8, 2, range(2, 4));
    let invariant = ledger.invariant();
    assert_eq!(invariant.tagged_range_count, 1, "adjacent tags normalize");
    assert!(
        u64::try_from(invariant.tagged_range_count).unwrap() <= invariant.outstanding_tag_bytes
    );
    assert!(invariant.outstanding_tag_bytes <= invariant.floor_bytes.unwrap());

    assert_eq!(ledger.release_exact(a, range(0, 2)), 2);
    assert_eq!(ledger.release_ambiguous(b, range(4, 6)), 2);
    assert_eq!(ledger.release_exact(c, range(2, 4)), 2);
    assert_invariant(&ledger);
}

#[test]
fn release_is_clipped_inside_the_ledger_to_the_exact_event_range() {
    let mut ledger = ProductQualificationLedger::default();
    let receipt = tag(&mut ledger, 4, 4, range(0, 4));

    assert_eq!(ledger.release_exact(receipt, range(0, 1)), 1);
    assert_eq!(ledger.invariant().verified_bytes, 1);
    assert_eq!(ledger.invariant().outstanding_tag_bytes, 3);
    assert!(!ledger.qualified());
    assert_eq!(ledger.release_exact(receipt, range(1, 4)), 3);
    assert!(ledger.qualified());
    assert_invariant(&ledger);
}

#[test]
fn freshness_is_checked_even_when_the_generation_has_no_deficit() {
    let mut ledger = ProductQualificationLedger::default();
    let receipt = tag(&mut ledger, 4, 4, range(0, 4));
    let before = ledger.invariant();

    assert_eq!(
        ledger.tag_admitted_original(4, 4, range(0, 4)),
        Err(ProductQualificationAdmissionError::OverlapsOutstandingTag)
    );
    assert_eq!(ledger.invariant(), before);
    assert_eq!(ledger.release_exact(receipt, range(0, 4)), 4);
    assert_invariant(&ledger);
}

#[test]
fn exhaustive_small_transition_sequences_preserve_the_invariant() {
    const OP_COUNT: usize = 8;
    const STEPS: usize = 6;
    let sequence_count = OP_COUNT.pow(STEPS as u32);

    for encoded in 0..sequence_count {
        let mut ledger = ProductQualificationLedger::default();
        let mut receipts = [None; 2];
        let mut cursor = encoded;
        for _ in 0..STEPS {
            let op = cursor % OP_COUNT;
            cursor /= OP_COUNT;
            match op {
                0 => {
                    if let Ok(Some(receipt)) = ledger.tag_admitted_original(4, 2, range(0, 2)) {
                        receipts[0] = Some(receipt);
                    }
                }
                1 => {
                    if let Ok(Some(receipt)) = ledger.tag_admitted_original(4, 2, range(2, 4)) {
                        receipts[1] = Some(receipt);
                    }
                }
                2 => {
                    if let Some(receipt) = receipts[0] {
                        ledger.release_exact(receipt, range(0, 2));
                    }
                }
                3 => {
                    if let Some(receipt) = receipts[1] {
                        ledger.release_exact(receipt, range(2, 4));
                    }
                }
                4 => {
                    if let Some(receipt) =
                        receipts[0].and_then(|receipt| receipt.intersect(range(1, 2)))
                    {
                        ledger.release_ambiguous(receipt, range(1, 2));
                    }
                }
                5 => ledger.revoke(),
                6 => {
                    let _ = ledger.reactivate_without_evidence();
                }
                7 => {
                    let before = ledger.invariant();
                    let _ = ledger.tag_admitted_original(4, 1, range(8, 10));
                    assert_eq!(ledger.invariant(), before);
                }
                _ => unreachable!(),
            }
            assert_invariant(&ledger);
            let invariant = ledger.invariant();
            if ledger.qualified() {
                assert_eq!(invariant.verified_bytes, invariant.floor_bytes.unwrap());
                assert_eq!(invariant.outstanding_tag_bytes, 0);
            }
        }
    }
}
