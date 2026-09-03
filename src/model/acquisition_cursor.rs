//! Pure direction-local arbitration for bounded Product acquisition.
//!
//! This cursor owns no bytes, qualification evidence, rate, reservation, or
//! carrier authority. It owns one finite dispatch scan and a persistent exact
//! fairness boundary. Every attempt remains advisory: the serialized caller
//! must independently revalidate and reserve all byte and writer authority.

use crate::protocol::OffsetRange;

use super::product_qualification::{
    ProductQualificationAuthority, ProductQualificationEpoch, ProductQualificationLedger,
};

/// The non-stale directional usage tier selected before acquisition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AcquisitionTier {
    Regular,
    Backup,
}

/// Read-only classification of one exact Product quantum before arbitration.
///
/// `Blocked` is deliberately distinct from `OrdinaryOnly`: an unqualified
/// additional output still needs a selected-tier visit, but none can accept
/// the whole quantum now. The caller may remain work-conserving through
/// ordinary placement, provided
/// [`AcquisitionSnapshot::ordinary_target_preserves_acquisition`] rejects any
/// target that would silently become acquisition outside the cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AcquisitionReadiness {
    InvalidSnapshot,
    OrdinaryFirstOwner,
    OrdinaryOnly,
    Blocked(AcquisitionTier),
    Ready(AcquisitionTier),
}

/// One atomically observed qualification lifecycle identity.
///
/// The private fields prevent a caller from pairing an epoch from one ledger
/// state with an authority from another. Reactivation intentionally changes
/// this identity even though it retains the epoch advanced by revocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AcquisitionQualificationIdentity {
    epoch: ProductQualificationEpoch,
    authority: ProductQualificationAuthority,
}

impl AcquisitionQualificationIdentity {
    pub(crate) fn capture(ledger: &ProductQualificationLedger) -> Self {
        Self {
            epoch: ledger.epoch(),
            authority: ledger.authority(),
        }
    }

    fn is_active(self) -> bool {
        self.authority == ProductQualificationAuthority::Active
    }
}

/// One exact output in stable scheduling order.
///
/// `locally_eligible` is structural selected-tier membership. Transient
/// Product, credit, reorder, or writer blockage belongs only in
/// `legal_whole_quantum`, so a blocked regular member still prevents Backup
/// acquisition. `qualification_epoch` is deliberately non-optional: an active
/// attachment with no generation must still differ structurally from the same
/// attachment after a revoke/requalify lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcquisitionCandidate<Id> {
    pub(crate) exact_id: Id,
    pub(crate) qualification_identity: AcquisitionQualificationIdentity,
    pub(crate) tier: AcquisitionTier,
    pub(crate) locally_eligible: bool,
    pub(crate) stale: bool,
    pub(crate) additional: bool,
    pub(crate) qualified: bool,
    /// `None` means no generation exists. `Some(0)` means the current
    /// generation has no untagged deficit and awaits exact release.
    pub(crate) generation_deficit_bytes: Option<u64>,
    /// Whether the complete dispatch quantum `N` fits every observe-time
    /// authority on this exact output. Apply-time validation remains required.
    pub(crate) legal_whole_quantum: bool,
}

impl<Id> AcquisitionCandidate<Id> {
    fn is_nonstale_tier_member(&self, tier: AcquisitionTier) -> bool {
        self.tier == tier
            && self.qualification_identity.is_active()
            && self.locally_eligible
            && !self.stale
    }

    fn needs_acquisition(&self) -> bool {
        self.additional
            && !self.qualified
            && self
                .generation_deficit_bytes
                .is_none_or(|deficit_bytes| deficit_bytes > 0)
    }

    fn qualification_state_is_coherent(&self) -> bool {
        if !self.qualification_identity.is_active() || self.stale {
            !self.qualification_identity.is_active()
                && !self.qualified
                && self.generation_deficit_bytes.is_none()
                && !self.legal_whole_quantum
        } else if self.qualified {
            self.generation_deficit_bytes == Some(0)
        } else {
            true
        }
    }
}

/// One immutable semantic observation for one serialized service dispatch.
///
/// This value is owned rather than tied to a caller-managed epoch. Structural
/// matching covers the exact pending Product identity, ordinary ownership, the
/// selected tier, and the full authority of the attempted target. Sibling
/// churn is irrelevant while it cannot change the selected tier. `N` is
/// retained explicitly and must equal the nonempty range length;
/// inconsistency fails the dispatch closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcquisitionSnapshot<Id> {
    pub(crate) pending_range: OffsetRange,
    pub(crate) quantum_bytes: u64,
    pub(crate) ordinary_owner_established: bool,
    pub(crate) candidates: Vec<AcquisitionCandidate<Id>>,
}

impl<Id: Eq> AcquisitionSnapshot<Id> {
    fn is_well_formed(&self) -> bool {
        self.quantum_bytes > 0
            && self.pending_range.len() == self.quantum_bytes
            && self
                .candidates
                .iter()
                .all(AcquisitionCandidate::qualification_state_is_coherent)
            && self.candidates.iter().enumerate().all(|(index, left)| {
                self.candidates[index + 1..]
                    .iter()
                    .all(|right| left.exact_id != right.exact_id)
            })
    }

    /// Equality of the authority that can affect one exact selected-tier
    /// attempt.
    ///
    /// The finite scan owns its original sibling order and failure set. A
    /// sibling's transient legality, qualification, order, or membership
    /// cannot retarget this token and therefore must not defeat bounded
    /// same-dispatch progress. The current selected tier is still exact, so a
    /// newly present Regular member invalidates a Backup attempt. The attempted
    /// candidate itself is compared in full, including its lifecycle,
    /// qualification, deficit, and whole-`N` legality.
    fn matches_attempt_authority(
        &self,
        other: &Self,
        tier: AcquisitionTier,
        exact_id: &Id,
    ) -> bool {
        if !self.is_well_formed()
            || !other.is_well_formed()
            || self.pending_range != other.pending_range
            || self.quantum_bytes != other.quantum_bytes
            || self.ordinary_owner_established != other.ordinary_owner_established
            || select_tier(&self.candidates) != Some(tier)
            || select_tier(&other.candidates) != Some(tier)
        {
            return false;
        }

        let Some(original) = self
            .candidates
            .iter()
            .find(|candidate| &candidate.exact_id == exact_id)
        else {
            return false;
        };
        let Some(current) = other
            .candidates
            .iter()
            .find(|candidate| &candidate.exact_id == exact_id)
        else {
            return false;
        };

        original == current
            && original.is_nonstale_tier_member(tier)
            && original.needs_acquisition()
            && original.legal_whole_quantum
    }

    /// Classifies demand without cloning or advancing the one-use cursor.
    ///
    /// This is snapshot state, not cursor availability: integration must also
    /// reject a pending token or permanent cursor-counter exhaustion. A later
    /// attempt and serialized apply still own fairness and every real byte and
    /// writer revalidation.
    pub(crate) fn acquisition_readiness(&self) -> AcquisitionReadiness {
        if !self.is_well_formed() {
            return AcquisitionReadiness::InvalidSnapshot;
        }
        if !self.ordinary_owner_established {
            return AcquisitionReadiness::OrdinaryFirstOwner;
        }
        let Some(tier) = select_tier(&self.candidates) else {
            return AcquisitionReadiness::OrdinaryOnly;
        };
        let mut demand = false;
        let mut legal = false;
        for candidate in self
            .candidates
            .iter()
            .filter(|candidate| candidate.is_nonstale_tier_member(tier))
        {
            if candidate.needs_acquisition() {
                demand = true;
                legal |= candidate.legal_whole_quantum;
            }
        }
        match (demand, legal) {
            (false, _) => AcquisitionReadiness::OrdinaryOnly,
            (true, false) => AcquisitionReadiness::Blocked(tier),
            (true, true) => AcquisitionReadiness::Ready(tier),
        }
    }

    /// Whether ordinary placement on `exact_id` preserves cursor ownership.
    ///
    /// A blocked acquisition must not stall an independently usable qualified
    /// owner. Conversely, ordinary fallback must not let any unqualified
    /// additional output bypass the one-use cursor, even inside the selected
    /// tier or after an ordinary planner shrinks `N`. Once an ordinary owner
    /// exists, the fallback target is therefore legal only when this quantum
    /// cannot advance its qualification generation. The apply transaction must
    /// rebuild and recheck this predicate before publication.
    pub(crate) fn ordinary_target_preserves_acquisition(&self, exact_id: &Id) -> bool {
        if !self.is_well_formed() {
            return false;
        }
        let Some(target) = self
            .candidates
            .iter()
            .find(|candidate| &candidate.exact_id == exact_id)
        else {
            return false;
        };
        if !self.ordinary_owner_established {
            return true;
        }
        !target.needs_acquisition()
    }
}

/// Advisory output of the cursor. It grants no commit authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcquisitionAttempt<Id> {
    round_id: u64,
    attempt_id: u64,
    pub(crate) exact_id: Id,
    pub(crate) qualification_identity: AcquisitionQualificationIdentity,
    pub(crate) tier: AcquisitionTier,
    pub(crate) pending_range: OffsetRange,
    pub(crate) quantum_bytes: u64,
    successor: Id,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AcquisitionDispatchStart {
    Started,
    PendingAttempt,
    InvalidSnapshot,
    CounterExhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AcquisitionApplyOutcome {
    Committed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AcquisitionAttemptResolution {
    Committed,
    Skipped,
    Invalidated,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AcquisitionBoundary<Id> {
    tier: AcquisitionTier,
    exact_id: Id,
}

#[derive(Debug, Clone)]
struct AcquisitionDispatch<Id> {
    round_id: u64,
    snapshot: AcquisitionSnapshot<Id>,
    selected_tier: Option<AcquisitionTier>,
    failed_in_scan: Vec<Id>,
    exhausted: bool,
}

/// One replay-safe fairness cursor for a logical-stream sender direction.
///
/// Only `next_boundary` persists across dispatches. Failure and exhaustion are
/// scoped to `dispatch`, so a later service wake can observe newly available
/// authority. Checked IDs never wrap; exhausting either identity space closes
/// this cursor permanently rather than permitting ABA.
#[derive(Debug)]
pub(crate) struct DirectionLocalAcquisitionCursor<Id> {
    next_boundary: Option<AcquisitionBoundary<Id>>,
    next_round_id: u64,
    next_attempt_id: u64,
    dispatch: Option<AcquisitionDispatch<Id>>,
    pending: Option<AcquisitionAttempt<Id>>,
    counter_exhausted: bool,
}

impl<Id> Default for DirectionLocalAcquisitionCursor<Id> {
    fn default() -> Self {
        Self {
            next_boundary: None,
            next_round_id: 0,
            next_attempt_id: 0,
            dispatch: None,
            pending: None,
            counter_exhausted: false,
        }
    }
}

impl<Id: Clone + Eq> DirectionLocalAcquisitionCursor<Id> {
    /// Starts one finite scan from an owned structural observation.
    ///
    /// Starting another dispatch while an attempt is outstanding is rejected
    /// without consuming that attempt. Otherwise the previous scan, including
    /// its failure/exhaustion set, ends here. Duplicate exact identities and an
    /// inconsistent `(OffsetRange, N)` fail this dispatch closed.
    pub(crate) fn begin_dispatch(
        &mut self,
        snapshot: AcquisitionSnapshot<Id>,
    ) -> AcquisitionDispatchStart {
        if self.counter_exhausted {
            return AcquisitionDispatchStart::CounterExhausted;
        }
        if self.pending.is_some() {
            return AcquisitionDispatchStart::PendingAttempt;
        }

        self.dispatch = None;
        if !snapshot.is_well_formed() {
            return AcquisitionDispatchStart::InvalidSnapshot;
        }

        let Some(round_id) = self.allocate_round_id() else {
            self.fail_counter_closed();
            return AcquisitionDispatchStart::CounterExhausted;
        };
        let selected_tier = select_tier(&snapshot.candidates);
        self.normalize_boundary(&snapshot.candidates, selected_tier);
        self.dispatch = Some(AcquisitionDispatch {
            round_id,
            snapshot,
            selected_tier,
            failed_in_scan: Vec::new(),
            exhausted: false,
        });
        AcquisitionDispatchStart::Started
    }

    /// Returns at most one advisory attempt from the active dispatch.
    ///
    /// A second advisory is forbidden until the first token is resolved. A full
    /// scan is exhausted only for this dispatch; `begin_dispatch` always starts
    /// a new finite scan while the checked counters remain available.
    pub(crate) fn advisory_attempt(&mut self) -> Option<AcquisitionAttempt<Id>> {
        if self.counter_exhausted || self.pending.is_some() {
            return None;
        }

        let (round_id, selected_tier, candidate_index, successor_index) = {
            let dispatch = self.dispatch.as_mut()?;
            if !dispatch.snapshot.ordinary_owner_established || dispatch.exhausted {
                return None;
            }
            let Some(tier) = dispatch.selected_tier else {
                dispatch.exhausted = true;
                return None;
            };
            let members = selected_tier_member_indices(&dispatch.snapshot.candidates, tier);
            if members.is_empty() {
                dispatch.exhausted = true;
                return None;
            }
            let start = self
                .next_boundary
                .as_ref()
                .filter(|boundary| boundary.tier == tier)
                .and_then(|boundary| {
                    members.iter().position(|index| {
                        dispatch.snapshot.candidates[*index].exact_id == boundary.exact_id
                    })
                })
                .unwrap_or(0);

            let mut selected = None;
            for distance in 0..members.len() {
                let member_position = (start + distance) % members.len();
                let index = members[member_position];
                let candidate = &dispatch.snapshot.candidates[index];
                if dispatch
                    .failed_in_scan
                    .iter()
                    .any(|failed| *failed == candidate.exact_id)
                    || !candidate.needs_acquisition()
                    || !candidate.legal_whole_quantum
                {
                    continue;
                }
                selected = Some((index, members[(member_position + 1) % members.len()]));
                break;
            }
            let Some((candidate_index, successor_index)) = selected else {
                dispatch.exhausted = true;
                return None;
            };
            (dispatch.round_id, tier, candidate_index, successor_index)
        };

        let Some(attempt_id) = self.allocate_attempt_id() else {
            self.fail_counter_closed();
            return None;
        };
        let dispatch = self
            .dispatch
            .as_ref()
            .expect("counter allocation preserves the active dispatch");
        let candidate = &dispatch.snapshot.candidates[candidate_index];
        let successor = &dispatch.snapshot.candidates[successor_index];
        let attempt = AcquisitionAttempt {
            round_id,
            attempt_id,
            exact_id: candidate.exact_id.clone(),
            qualification_identity: candidate.qualification_identity,
            tier: selected_tier,
            pending_range: dispatch.snapshot.pending_range,
            quantum_bytes: dispatch.snapshot.quantum_bytes,
            successor: successor.exact_id.clone(),
        };
        self.pending = Some(attempt.clone());
        Some(attempt)
    }

    /// Whether this exact one-use token is pending under this exact snapshot.
    ///
    /// This remains advisory. It lets a serialized apply path reject a replay
    /// before reserving or mutating real byte authority; the later resolution
    /// still consumes the token on success, failure, or invalidation.
    pub(crate) fn attempt_is_current(
        &self,
        attempt: &AcquisitionAttempt<Id>,
        snapshot: &AcquisitionSnapshot<Id>,
    ) -> bool {
        !self.counter_exhausted
            && self.pending.as_ref() == Some(attempt)
            && self.dispatch.as_ref().is_some_and(|dispatch| {
                dispatch.round_id == attempt.round_id
                    && dispatch.snapshot.matches_attempt_authority(
                        snapshot,
                        attempt.tier,
                        &attempt.exact_id,
                    )
            })
    }

    /// Consumes one matching advisory token against the pre-mutation apply view.
    ///
    /// A mismatching or replayed token never consumes a newer pending attempt.
    /// A matching token is consumed exactly once. Structural change invalidates
    /// and ends its old dispatch. Commit and same-snapshot failure both advance
    /// the persistent exact boundary; only the failure remains in this scan so
    /// a sibling can be attempted before it repeats.
    pub(crate) fn resolve_attempt(
        &mut self,
        attempt: &AcquisitionAttempt<Id>,
        outcome: AcquisitionApplyOutcome,
        snapshot: &AcquisitionSnapshot<Id>,
    ) -> AcquisitionAttemptResolution {
        if self.pending.as_ref() != Some(attempt) {
            return AcquisitionAttemptResolution::Invalidated;
        }
        self.pending = None;

        let Some(dispatch) = self.dispatch.as_mut() else {
            return AcquisitionAttemptResolution::Invalidated;
        };
        if dispatch.round_id != attempt.round_id
            || !dispatch.snapshot.matches_attempt_authority(
                snapshot,
                attempt.tier,
                &attempt.exact_id,
            )
        {
            self.dispatch = None;
            return AcquisitionAttemptResolution::Invalidated;
        }

        let members = selected_tier_member_indices(&dispatch.snapshot.candidates, attempt.tier);
        let Some(member_position) = members
            .iter()
            .position(|index| dispatch.snapshot.candidates[*index].exact_id == attempt.exact_id)
        else {
            self.dispatch = None;
            return AcquisitionAttemptResolution::Invalidated;
        };
        let target = &dispatch.snapshot.candidates[members[member_position]];
        let successor =
            &dispatch.snapshot.candidates[members[(member_position + 1) % members.len()]];
        if dispatch.selected_tier != Some(attempt.tier)
            || target.qualification_identity != attempt.qualification_identity
            || successor.exact_id != attempt.successor
            || attempt.pending_range != dispatch.snapshot.pending_range
            || attempt.quantum_bytes != dispatch.snapshot.quantum_bytes
            || !target.needs_acquisition()
            || !target.legal_whole_quantum
        {
            self.dispatch = None;
            return AcquisitionAttemptResolution::Invalidated;
        }

        self.next_boundary = Some(AcquisitionBoundary {
            tier: attempt.tier,
            exact_id: attempt.successor.clone(),
        });
        match outcome {
            AcquisitionApplyOutcome::Committed => {
                self.dispatch = None;
                AcquisitionAttemptResolution::Committed
            }
            AcquisitionApplyOutcome::Failed => {
                dispatch.failed_in_scan.push(attempt.exact_id.clone());
                dispatch.exhausted = false;
                AcquisitionAttemptResolution::Skipped
            }
        }
    }

    pub(crate) fn counter_exhausted(&self) -> bool {
        self.counter_exhausted
    }

    /// Read-only service readiness for starting one new dispatch scan.
    pub(crate) fn can_begin_dispatch(&self) -> bool {
        !self.counter_exhausted && self.pending.is_none()
    }

    fn normalize_boundary(
        &mut self,
        candidates: &[AcquisitionCandidate<Id>],
        selected_tier: Option<AcquisitionTier>,
    ) {
        let Some(tier) = selected_tier else {
            self.next_boundary = None;
            return;
        };
        let boundary_is_current = self.next_boundary.as_ref().is_some_and(|boundary| {
            boundary.tier == tier
                && candidates.iter().any(|candidate| {
                    candidate.is_nonstale_tier_member(tier)
                        && candidate.exact_id == boundary.exact_id
                })
        });
        if !boundary_is_current {
            self.next_boundary = candidates
                .iter()
                .find(|candidate| candidate.is_nonstale_tier_member(tier))
                .map(|candidate| AcquisitionBoundary {
                    tier,
                    exact_id: candidate.exact_id.clone(),
                });
        }
    }

    fn allocate_round_id(&mut self) -> Option<u64> {
        let id = self.next_round_id;
        self.next_round_id = self.next_round_id.checked_add(1)?;
        Some(id)
    }

    fn allocate_attempt_id(&mut self) -> Option<u64> {
        let id = self.next_attempt_id;
        self.next_attempt_id = self.next_attempt_id.checked_add(1)?;
        Some(id)
    }

    fn fail_counter_closed(&mut self) {
        self.counter_exhausted = true;
        self.dispatch = None;
        self.pending = None;
    }
}

fn select_tier<Id>(candidates: &[AcquisitionCandidate<Id>]) -> Option<AcquisitionTier> {
    if candidates
        .iter()
        .any(|candidate| candidate.is_nonstale_tier_member(AcquisitionTier::Regular))
    {
        Some(AcquisitionTier::Regular)
    } else if candidates
        .iter()
        .any(|candidate| candidate.is_nonstale_tier_member(AcquisitionTier::Backup))
    {
        Some(AcquisitionTier::Backup)
    } else {
        None
    }
}

fn selected_tier_member_indices<Id>(
    candidates: &[AcquisitionCandidate<Id>],
    tier: AcquisitionTier,
) -> Vec<usize> {
    candidates
        .iter()
        .enumerate()
        .filter_map(|(index, candidate)| candidate.is_nonstale_tier_member(tier).then_some(index))
        .collect()
}

#[cfg(test)]
#[path = "tests_acquisition_cursor.rs"]
mod tests;
