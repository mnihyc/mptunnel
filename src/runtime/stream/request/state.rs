//! Request path evidence and serialized stream state.
//!
//! Exact attachment instances fence measurement state across reconnects. The
//! client relay serializes this aggregate, so it remains lock-free.

use super::flight::RequestFlightLedger;
use crate::model::path::RelayPathInstance;
#[cfg(test)]
use crate::model::product_qualification::ProductQualificationInvariant;
use crate::model::product_qualification::{
    ProductQualificationAdmissionError, ProductQualificationAuthority, ProductQualificationLedger,
    ProductQualificationReceipt,
};
use crate::model::requalification::StreamPathRequalification;
use crate::model::request_evidence::{RequestPathRateEvidence, RequestProductRateEpoch};
use crate::protocol::OffsetRange;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

/// Exact request attachment plus the only authority that may release one
/// qualification tag.
///
/// Keeping both fields private prevents callers from pairing a receipt minted
/// by one attachment with another attachment's ledger. Copies are safe because
/// the underlying normalized tag set makes release idempotent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct RequestProductQualificationReceipt {
    instance: RelayPathInstance,
    receipt: ProductQualificationReceipt,
}

impl RequestProductQualificationReceipt {
    fn new(instance: RelayPathInstance, receipt: ProductQualificationReceipt) -> Self {
        Self { instance, receipt }
    }

    pub(in crate::runtime) fn intersect(self, range: OffsetRange) -> Option<Self> {
        Some(Self {
            instance: self.instance,
            receipt: self.receipt.intersect(range)?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum RequestAckClockOperation {
    #[cfg(test)]
    Pending {
        reference: RelayPathInstance,
        candidate: RelayPathInstance,
    },
    Owner {
        candidate: RelayPathInstance,
        target_bytes: u64,
    },
}

impl RequestAckClockOperation {
    pub(in crate::runtime) fn candidate(self) -> RelayPathInstance {
        match self {
            #[cfg(test)]
            Self::Pending { candidate, .. } => candidate,
            Self::Owner { candidate, .. } => candidate,
        }
    }
}

/// Evidence owned by one exact request-path attachment.
///
/// Exact instances, rather than configured path indexes, fence evidence across
/// reconnects. Keeping one record per instance also makes partial cleanup an
/// explicit state transition instead of a collection of unrelated map edits.
#[derive(Debug, Default)]
pub(in crate::runtime) struct RequestPathState {
    rate_evidence: Option<RequestPathRateEvidence>,
    product_rate_epoch: Option<RequestProductRateEpoch>,
    product_qualification: ProductQualificationLedger,
    product_path_use_proven: bool,
    ack_clock_first_window: bool,
    ack_clock_proven: bool,
    ack_clock_measurement_bytes: Option<u64>,
    ack_clock_measurement_target: Option<u64>,
    tcp_capacity_proven: bool,
    capacity_admitted: bool,
}

impl RequestPathState {
    /// Starts the same bounded Product acquisition used by a fresh attachment.
    /// Historical flow capacity cannot authorize post-stale placement.
    pub(in crate::runtime) fn reset_for_requalification(&mut self) {
        // The qualification ledger is deliberately not replaced by Default:
        // revocation advances its checked epoch, so predecessor receipts can
        // never become current again after exact requalification.
        self.product_qualification.revoke();
        self.rate_evidence = None;
        self.product_rate_epoch = None;
        self.product_path_use_proven = false;
        self.ack_clock_first_window = false;
        self.ack_clock_proven = false;
        self.ack_clock_measurement_bytes = None;
        self.ack_clock_measurement_target = None;
        self.tcp_capacity_proven = false;
        self.capacity_admitted = false;
    }

    pub(in crate::runtime) fn rate_evidence_mut(
        &mut self,
        observed_at: Instant,
    ) -> &mut RequestPathRateEvidence {
        self.rate_evidence
            .get_or_insert_with(|| RequestPathRateEvidence::new(observed_at))
    }

    pub(in crate::runtime) fn product_rate_epoch(&self) -> Option<RequestProductRateEpoch> {
        self.product_rate_epoch
    }

    pub(in crate::runtime) fn set_product_rate_epoch(&mut self, epoch: RequestProductRateEpoch) {
        self.product_rate_epoch = Some(epoch);
    }

    pub(in crate::runtime) fn product_assignment_qualified(&self) -> bool {
        self.product_qualification.qualified()
    }

    #[cfg(test)]
    pub(in crate::runtime) fn product_qualification_deficit_bytes(&self) -> Option<u64> {
        self.product_qualification.deficit_bytes()
    }

    pub(in crate::runtime) fn product_qualification_authority(
        &self,
    ) -> ProductQualificationAuthority {
        self.product_qualification.authority()
    }

    pub(in crate::runtime) fn reactivate_product_qualification(
        &mut self,
    ) -> Result<bool, ProductQualificationAdmissionError> {
        self.product_qualification.reactivate_without_evidence()
    }

    #[cfg(test)]
    pub(in crate::runtime) fn product_qualification_invariant(
        &self,
    ) -> ProductQualificationInvariant {
        self.product_qualification.invariant()
    }

    pub(in crate::runtime) fn product_path_use_proven(&self) -> bool {
        self.product_path_use_proven
    }

    pub(in crate::runtime) fn mark_product_path_use_proven(&mut self) -> bool {
        !std::mem::replace(&mut self.product_path_use_proven, true)
    }

    pub(in crate::runtime) fn ack_clock_first_window(&self) -> bool {
        self.ack_clock_first_window
    }

    pub(in crate::runtime) fn mark_ack_clock_first_window(&mut self) -> bool {
        !std::mem::replace(&mut self.ack_clock_first_window, true)
    }

    pub(in crate::runtime) fn ack_clock_proven(&self) -> bool {
        self.ack_clock_proven
    }

    pub(in crate::runtime) fn mark_ack_clock_proven(&mut self) -> bool {
        !std::mem::replace(&mut self.ack_clock_proven, true)
    }

    pub(in crate::runtime) fn ack_clock_measurement_bytes(&self) -> Option<u64> {
        self.ack_clock_measurement_bytes
    }

    pub(in crate::runtime) fn set_ack_clock_measurement_bytes(&mut self, bytes: u64) {
        self.ack_clock_measurement_bytes = Some(bytes);
    }

    pub(in crate::runtime) fn ack_clock_measurement_target(&self) -> Option<u64> {
        self.ack_clock_measurement_target
    }

    pub(in crate::runtime) fn set_ack_clock_measurement_target(&mut self, bytes: u64) {
        self.ack_clock_measurement_target = Some(bytes);
    }

    pub(in crate::runtime) fn tcp_capacity_proven(&self) -> bool {
        self.tcp_capacity_proven
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_tcp_capacity_proven(&mut self) {
        self.tcp_capacity_proven = true;
    }

    pub(in crate::runtime) fn clear_tcp_capacity_proven(&mut self) {
        self.tcp_capacity_proven = false;
    }

    pub(in crate::runtime) fn capacity_admitted(&self) -> bool {
        self.capacity_admitted
    }

    pub(in crate::runtime) fn mark_capacity_admitted(&mut self) {
        self.capacity_admitted = true;
    }

    /// Whether this exact output owns evidence derived from Product delivery.
    ///
    /// Allocating a rate tracker or seeding its ACK boundary from an
    /// offset-free carrier receipt is bookkeeping, not Product provenance.
    #[cfg(test)]
    pub(in crate::runtime) fn has_product_evidence(&self) -> bool {
        self.rate_evidence
            .as_ref()
            .is_some_and(RequestPathRateEvidence::has_exact_path_provenance)
            || self.product_rate_epoch.is_some()
            || self.product_assignment_qualified()
            || self.product_path_use_proven
            || self.ack_clock_proven
    }

    /// Revoke TCP admission evidence while retaining a completed flow model.
    ///
    /// A flow model is receiver-proven product history; carrier proof expiry
    /// must not erase it. All incomplete measurement authority is discarded.
    pub(in crate::runtime) fn revoke_tcp_capacity(&mut self) {
        self.tcp_capacity_proven = false;
        self.capacity_admitted = false;
        self.ack_clock_first_window = false;
        self.ack_clock_proven = false;
        self.rate_evidence = None;
        self.ack_clock_measurement_bytes = None;
        self.ack_clock_measurement_target = None;
    }
}

/// Exact-instance path_state records for one request stream.
#[derive(Debug, Default)]
pub(in crate::runtime) struct RequestPathStates {
    entries: HashMap<RelayPathInstance, RequestPathState>,
}

impl RequestPathStates {
    pub(in crate::runtime) fn get(&self, instance: RelayPathInstance) -> Option<&RequestPathState> {
        self.entries.get(&instance)
    }

    pub(in crate::runtime) fn get_mut(
        &mut self,
        instance: RelayPathInstance,
    ) -> &mut RequestPathState {
        self.entries.entry(instance).or_default()
    }

    pub(in crate::runtime) fn get_existing_mut(
        &mut self,
        instance: RelayPathInstance,
    ) -> Option<&mut RequestPathState> {
        self.entries.get_mut(&instance)
    }

    pub(in crate::runtime) fn retain_live(&mut self, live: &HashSet<RelayPathInstance>) {
        self.entries.retain(|instance, state| {
            let retained = live.contains(instance);
            if !retained {
                state.product_qualification.revoke();
            }
            retained
        });
    }

    /// Atomically freezes the current attachment's bounds and tags the
    /// admitted OriginalData prefix. The returned compound receipt is the only
    /// later release authority.
    pub(in crate::runtime) fn tag_admitted_original(
        &mut self,
        instance: RelayPathInstance,
        floor_bytes: u64,
        max_quantum_bytes: u64,
        range: OffsetRange,
    ) -> Result<Option<RequestProductQualificationReceipt>, ProductQualificationAdmissionError>
    {
        self.get_mut(instance)
            .product_qualification
            .tag_admitted_original(floor_bytes, max_quantum_bytes, range)
            .map(|receipt| {
                receipt.map(|receipt| RequestProductQualificationReceipt::new(instance, receipt))
            })
    }

    pub(in crate::runtime) fn release_exact_product_qualification(
        &mut self,
        authority: RequestProductQualificationReceipt,
        event_range: OffsetRange,
    ) -> u64 {
        self.entries
            .get_mut(&authority.instance)
            .map_or(0, |state| {
                state
                    .product_qualification
                    .release_exact(authority.receipt, event_range)
            })
    }

    pub(in crate::runtime) fn release_ambiguous_product_qualification(
        &mut self,
        authority: RequestProductQualificationReceipt,
        event_range: OffsetRange,
    ) -> u64 {
        self.entries
            .get_mut(&authority.instance)
            .map_or(0, |state| {
                state
                    .product_qualification
                    .release_ambiguous(authority.receipt, event_range)
            })
    }

    pub(in crate::runtime) fn revoke_all_product_qualification(&mut self) {
        for state in self.entries.values_mut() {
            state.product_qualification.revoke();
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn qualify_product_assignment_for_test(
        &mut self,
        instance: RelayPathInstance,
    ) {
        let range = OffsetRange { start: 0, end: 1 };
        let receipt = self
            .tag_admitted_original(instance, 1, 1, range)
            .expect("valid one-byte qualification admission")
            .expect("one-byte qualification receipt");
        assert_eq!(self.release_exact_product_qualification(receipt, range), 1);
        assert!(
            self.get(instance)
                .is_some_and(RequestPathState::product_assignment_qualified)
        );
    }

    pub(in crate::runtime) fn iter(
        &self,
    ) -> impl Iterator<Item = (RelayPathInstance, &RequestPathState)> {
        self.entries
            .iter()
            .map(|(instance, state)| (*instance, state))
    }
}

/// Single-task request product state.
///
/// The client relay serializes this aggregate, so request offsets, evidence,
/// path evidence and reinjection history stay lock-free. Per-path evidence is
/// keyed once in `path_states`, preventing partial membership cleanup.
#[derive(Debug, Default)]
pub(in crate::runtime) struct RequestStreamState {
    pub(in crate::runtime) flights: RequestFlightLedger,
    pub(in crate::runtime) path_states: RequestPathStates,
    pub(in crate::runtime) ack_clock_operation: Option<RequestAckClockOperation>,
    pub(in crate::runtime) membership_generation: Option<u64>,
    /// Directional Product qualification for every exact attachment. Native
    /// path recovery remains independent of these stream-local transitions.
    pub(in crate::runtime) requalification: StreamPathRequalification<RelayPathInstance>,
}
