use super::CarrierPathKey;
use super::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CarrierWorkKind {
    OwnerData,
    RepairData,
    Probe,
    Control,
}

impl CarrierWorkKind {
    pub(super) fn is_ordering_owner(self) -> bool {
        matches!(self, Self::OwnerData)
    }

    pub(super) fn carries_product_offsets(self) -> bool {
        matches!(self, Self::OwnerData | Self::RepairData)
    }

    pub(super) fn counts_against_sender_extra_budget(self) -> bool {
        matches!(self, Self::RepairData)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PathRuntimeRole {
    Service,
    Trial,
    Cohort,
    Probe,
    RepairOnly,
    Standby,
    Failed,
}

impl PathRuntimeRole {
    pub(super) fn may_own_unique_data(self) -> bool {
        matches!(self, Self::Service | Self::Trial | Self::Cohort)
    }

    pub(super) fn may_probe(self) -> bool {
        !matches!(self, Self::Failed)
    }

    pub(super) fn may_repair(self) -> bool {
        matches!(
            self,
            Self::Service | Self::Cohort | Self::RepairOnly | Self::Trial
        )
    }

    pub(super) fn is_optional_owner(self) -> bool {
        matches!(self, Self::Trial | Self::Cohort)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CarrierFamilyHealth {
    Healthy,
    ProbeOnly,
    RepairOnly,
    DisabledForReliableOwner,
}

impl CarrierFamilyHealth {
    pub(super) fn reliable_owner_allowed(self) -> bool {
        matches!(self, Self::Healthy)
    }
}

pub(super) fn cross_family_reliable_owner_health(
    current_owner: Option<CarrierPathKey>,
    current_owner_bulk_rate_proven: bool,
    candidate: CarrierPathKey,
    candidate_bulk_rate_proven: bool,
) -> CarrierFamilyHealth {
    let Some(owner) = current_owner else {
        return CarrierFamilyHealth::Healthy;
    };
    if owner == candidate || owner.underlay == candidate.underlay {
        return CarrierFamilyHealth::Healthy;
    }
    if current_owner_bulk_rate_proven && candidate_bulk_rate_proven {
        CarrierFamilyHealth::Healthy
    } else if candidate_bulk_rate_proven {
        CarrierFamilyHealth::RepairOnly
    } else if current_owner_bulk_rate_proven {
        CarrierFamilyHealth::ProbeOnly
    } else {
        CarrierFamilyHealth::DisabledForReliableOwner
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ExtraTrafficBudget {
    owner_payload_bytes: u64,
    optional_spent_bytes: u64,
    startup_floor_bytes: u64,
    percent_budget: u16,
}

impl ExtraTrafficBudget {
    pub(super) fn new(
        owner_payload_bytes: u64,
        optional_spent_bytes: u64,
        startup_floor_bytes: usize,
        performance: MppPerformanceConfig,
    ) -> Self {
        Self {
            owner_payload_bytes,
            optional_spent_bytes,
            startup_floor_bytes: startup_floor_bytes as u64,
            percent_budget: performance.extra_traffic_hint_percent,
        }
    }

    pub(super) fn limit_bytes(self) -> u64 {
        self.startup_floor_bytes.saturating_add(
            self.owner_payload_bytes
                .saturating_mul(self.percent_budget as u64)
                / 100,
        )
    }

    pub(super) fn remaining_bytes(self) -> usize {
        self.limit_bytes()
            .saturating_sub(self.optional_spent_bytes)
            .min(usize::MAX as u64) as usize
    }

    pub(super) fn can_spend(self, bytes: usize) -> bool {
        self.optional_spent_bytes.saturating_add(bytes as u64) <= self.limit_bytes()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ExtraTrafficKind {
    DuplicateValidation,
    Repair,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ExtraTrafficLedger {
    owner_payload_bytes: u64,
    duplicate_validation_bytes: u64,
    repair_bytes: u64,
    trial_owner_bytes: u64,
}

impl ExtraTrafficLedger {
    pub(super) fn owner_payload_bytes(self) -> u64 {
        self.owner_payload_bytes
    }

    pub(super) fn optional_spent_bytes(self) -> u64 {
        self.duplicate_validation_bytes
            .saturating_add(self.repair_bytes)
    }

    pub(super) fn record_owner_payload(&mut self, bytes: usize) {
        self.owner_payload_bytes = self.owner_payload_bytes.saturating_add(bytes as u64);
    }

    pub(super) fn record_trial_owner(&mut self, bytes: usize) {
        self.trial_owner_bytes = self.trial_owner_bytes.saturating_add(bytes as u64);
    }

    pub(super) fn record_optional(&mut self, kind: ExtraTrafficKind, bytes: usize) {
        match kind {
            ExtraTrafficKind::DuplicateValidation => {
                self.duplicate_validation_bytes =
                    self.duplicate_validation_bytes.saturating_add(bytes as u64);
            }
            ExtraTrafficKind::Repair => {
                self.repair_bytes = self.repair_bytes.saturating_add(bytes as u64);
            }
        }
    }

    pub(super) fn budget(
        self,
        startup_floor_bytes: usize,
        performance: MppPerformanceConfig,
    ) -> ExtraTrafficBudget {
        ExtraTrafficBudget::new(
            self.owner_payload_bytes,
            self.optional_spent_bytes(),
            startup_floor_bytes,
            performance,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OptionalPathDecision {
    Service,
    AdmitOwner,
    ProbeOnly,
    Standby,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct OptionalPathAdmission {
    pub(super) role: PathRuntimeRole,
    pub(super) work: CarrierWorkKind,
    pub(super) decision: OptionalPathDecision,
}

impl OptionalPathAdmission {
    pub(super) fn service() -> Self {
        Self {
            role: PathRuntimeRole::Service,
            work: CarrierWorkKind::OwnerData,
            decision: OptionalPathDecision::Service,
        }
    }

    pub(super) fn optional_owner(role: PathRuntimeRole) -> Self {
        debug_assert!(role.is_optional_owner());
        Self {
            role,
            work: CarrierWorkKind::OwnerData,
            decision: OptionalPathDecision::AdmitOwner,
        }
    }

    pub(super) fn probe_only() -> Self {
        Self {
            role: PathRuntimeRole::Probe,
            work: CarrierWorkKind::Probe,
            decision: OptionalPathDecision::ProbeOnly,
        }
    }

    pub(super) fn standby() -> Self {
        Self {
            role: PathRuntimeRole::Standby,
            work: CarrierWorkKind::Control,
            decision: OptionalPathDecision::Standby,
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy)]
pub(super) struct CohortMember {
    pub(super) key: CarrierPathKey,
    pub(super) role: PathRuntimeRole,
    pub(super) owner_sent_bytes: u64,
    pub(super) optional_overhead_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct OptionalOwnerAdmissionInput {
    pub(super) key: CarrierPathKey,
    pub(super) bulk_rate_proven: bool,
    pub(super) frontier_clear: bool,
    pub(super) completion_improves: bool,
    pub(super) observed_goodput_non_degrading: bool,
    pub(super) read_gap: Duration,
    pub(super) owner_bytes: usize,
    pub(super) optional_overhead_bytes: usize,
}

#[derive(Debug, Clone)]
pub(super) struct FlowCohortEpoch {
    _generation: u64,
    service: CarrierPathKey,
    owner_credit_bytes: u64,
    optional_credit_bytes: u64,
    optional_owner_spent_bytes: u64,
    optional_overhead_budget_bytes: u64,
    optional_overhead_spent_bytes: u64,
    max_read_gap_budget: Duration,
    members: Vec<CohortMember>,
}

impl FlowCohortEpoch {
    pub(super) fn new(
        generation: u64,
        service: CarrierPathKey,
        owner_credit_bytes: usize,
        optional_credit_bytes: usize,
        optional_overhead_budget_bytes: usize,
        max_read_gap_budget: Duration,
    ) -> Self {
        Self {
            _generation: generation,
            service,
            owner_credit_bytes: owner_credit_bytes as u64,
            optional_credit_bytes: optional_credit_bytes as u64,
            optional_owner_spent_bytes: 0,
            optional_overhead_budget_bytes: optional_overhead_budget_bytes as u64,
            optional_overhead_spent_bytes: 0,
            max_read_gap_budget,
            members: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(super) fn members(&self) -> &[CohortMember] {
        &self.members
    }

    #[cfg(test)]
    pub(super) fn optional_owner_spent_bytes(&self) -> u64 {
        self.optional_owner_spent_bytes
    }

    #[cfg(test)]
    pub(super) fn optional_overhead_spent_bytes(&self) -> u64 {
        self.optional_overhead_spent_bytes
    }

    pub(super) fn matches_envelope(
        &self,
        service: CarrierPathKey,
        owner_credit_bytes: usize,
        optional_credit_bytes: usize,
        optional_overhead_budget_bytes: usize,
        max_read_gap_budget: Duration,
    ) -> bool {
        self.service == service
            && self.owner_credit_bytes == owner_credit_bytes as u64
            && self.optional_credit_bytes == optional_credit_bytes as u64
            && self.optional_overhead_budget_bytes == optional_overhead_budget_bytes as u64
            && self.max_read_gap_budget == max_read_gap_budget
    }

    pub(super) fn admit_optional_owner(
        &mut self,
        input: OptionalOwnerAdmissionInput,
    ) -> OptionalPathAdmission {
        if input.key == self.service {
            return OptionalPathAdmission::service();
        }
        if !self.optional_owner_allowed(input) {
            if self.optional_probe_allowed(input) {
                return OptionalPathAdmission::probe_only();
            }
            return OptionalPathAdmission::standby();
        }

        let role = if input.bulk_rate_proven {
            PathRuntimeRole::Cohort
        } else {
            PathRuntimeRole::Trial
        };

        self.optional_owner_spent_bytes = self
            .optional_owner_spent_bytes
            .saturating_add(input.owner_bytes as u64);
        self.optional_overhead_spent_bytes = self
            .optional_overhead_spent_bytes
            .saturating_add(input.optional_overhead_bytes as u64);
        if let Some(member) = self
            .members
            .iter_mut()
            .find(|member| member.key == input.key)
        {
            debug_assert!(member.role.is_optional_owner());
            member.owner_sent_bytes = member
                .owner_sent_bytes
                .saturating_add(input.owner_bytes as u64);
            member.optional_overhead_bytes = member
                .optional_overhead_bytes
                .saturating_add(input.optional_overhead_bytes as u64);
        } else {
            self.members.push(CohortMember {
                key: input.key,
                role,
                owner_sent_bytes: input.owner_bytes as u64,
                optional_overhead_bytes: input.optional_overhead_bytes as u64,
            });
        }

        OptionalPathAdmission::optional_owner(role)
    }

    fn optional_owner_allowed(&self, input: OptionalOwnerAdmissionInput) -> bool {
        input.frontier_clear
            && input.completion_improves
            && input.observed_goodput_non_degrading
            && input.read_gap <= self.max_read_gap_budget
            && self
                .optional_owner_spent_bytes
                .saturating_add(input.owner_bytes as u64)
                <= self.optional_credit_bytes
            && self
                .optional_overhead_spent_bytes
                .saturating_add(input.optional_overhead_bytes as u64)
                <= self.optional_overhead_budget_bytes
            && (input.owner_bytes as u64) <= self.owner_credit_bytes
    }

    fn optional_probe_allowed(&self, input: OptionalOwnerAdmissionInput) -> bool {
        input.frontier_clear
            && input.read_gap <= self.max_read_gap_budget
            && self
                .optional_overhead_spent_bytes
                .saturating_add(input.optional_overhead_bytes as u64)
                <= self.optional_overhead_budget_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        ledger.record_owner_payload(1_000_000);
        ledger.record_trial_owner(64 * 1024);
        ledger.record_optional(ExtraTrafficKind::DuplicateValidation, 200);
        ledger.record_optional(ExtraTrafficKind::Repair, 300);

        assert_eq!(ledger.owner_payload_bytes(), 1_000_000);
        assert_eq!(ledger.optional_spent_bytes(), 500);
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

    #[test]
    fn epoch_admits_only_positive_bulk_rate_proven_optional_owner() {
        let mut epoch = FlowCohortEpoch::new(
            7,
            key(0),
            256 * 1024,
            128 * 1024,
            64 * 1024,
            Duration::from_millis(200),
        );

        let rejected = epoch.admit_optional_owner(OptionalOwnerAdmissionInput {
            key: key(1),
            bulk_rate_proven: true,
            frontier_clear: true,
            completion_improves: false,
            observed_goodput_non_degrading: true,
            read_gap: Duration::from_millis(10),
            owner_bytes: 64 * 1024,
            optional_overhead_bytes: 0,
        });

        assert_eq!(rejected.decision, OptionalPathDecision::ProbeOnly);
        assert!(epoch.members().is_empty());

        let admitted = epoch.admit_optional_owner(OptionalOwnerAdmissionInput {
            key: key(1),
            bulk_rate_proven: true,
            frontier_clear: true,
            completion_improves: true,
            observed_goodput_non_degrading: true,
            read_gap: Duration::from_millis(10),
            owner_bytes: 64 * 1024,
            optional_overhead_bytes: 0,
        });

        assert_eq!(admitted.decision, OptionalPathDecision::AdmitOwner);
        assert_eq!(admitted.role, PathRuntimeRole::Cohort);
        assert_eq!(epoch.members().len(), 1);
        assert_eq!(epoch.members()[0].key, key(1));
        assert_eq!(epoch.members()[0].role, PathRuntimeRole::Cohort);
        assert_eq!(epoch.optional_owner_spent_bytes(), 64 * 1024);
    }

    #[test]
    fn epoch_rejects_optional_owner_when_budget_or_read_gap_is_exceeded() {
        let mut epoch = FlowCohortEpoch::new(
            8,
            key(0),
            256 * 1024,
            64 * 1024,
            4 * 1024,
            Duration::from_millis(100),
        );

        let too_much_owner_credit = epoch.admit_optional_owner(OptionalOwnerAdmissionInput {
            key: key(1),
            bulk_rate_proven: true,
            frontier_clear: true,
            completion_improves: true,
            observed_goodput_non_degrading: true,
            read_gap: Duration::from_millis(10),
            owner_bytes: 128 * 1024,
            optional_overhead_bytes: 0,
        });
        assert_eq!(
            too_much_owner_credit.decision,
            OptionalPathDecision::ProbeOnly
        );

        let too_much_read_gap = epoch.admit_optional_owner(OptionalOwnerAdmissionInput {
            key: key(1),
            bulk_rate_proven: true,
            frontier_clear: true,
            completion_improves: true,
            observed_goodput_non_degrading: true,
            read_gap: Duration::from_millis(101),
            owner_bytes: 32 * 1024,
            optional_overhead_bytes: 0,
        });
        assert_eq!(too_much_read_gap.decision, OptionalPathDecision::Standby);

        let too_much_overhead = epoch.admit_optional_owner(OptionalOwnerAdmissionInput {
            key: key(1),
            bulk_rate_proven: true,
            frontier_clear: true,
            completion_improves: true,
            observed_goodput_non_degrading: true,
            read_gap: Duration::from_millis(10),
            owner_bytes: 32 * 1024,
            optional_overhead_bytes: 8 * 1024,
        });
        assert_eq!(too_much_overhead.decision, OptionalPathDecision::Standby);
        assert!(epoch.members().is_empty());
    }

    #[test]
    fn epoch_keeps_service_as_owner_without_spending_optional_credit() {
        let mut epoch = FlowCohortEpoch::new(
            9,
            key(3),
            256 * 1024,
            64 * 1024,
            16 * 1024,
            Duration::from_millis(100),
        );

        let service = epoch.admit_optional_owner(OptionalOwnerAdmissionInput {
            key: key(3),
            bulk_rate_proven: false,
            frontier_clear: false,
            completion_improves: false,
            observed_goodput_non_degrading: false,
            read_gap: Duration::from_secs(10),
            owner_bytes: 64 * 1024,
            optional_overhead_bytes: 16 * 1024,
        });

        assert_eq!(service.decision, OptionalPathDecision::Service);
        assert_eq!(service.role, PathRuntimeRole::Service);
        assert_eq!(epoch.optional_owner_spent_bytes(), 0);
        assert_eq!(epoch.optional_overhead_spent_bytes(), 0);
        assert!(epoch.members().is_empty());
    }

    #[test]
    fn cross_family_reliable_owner_requires_both_families_healthy() {
        let owner = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };

        assert!(
            cross_family_reliable_owner_health(Some(owner), true, candidate, true)
                .reliable_owner_allowed()
        );
        assert_eq!(
            cross_family_reliable_owner_health(Some(owner), true, candidate, false),
            CarrierFamilyHealth::ProbeOnly
        );
        assert_eq!(
            cross_family_reliable_owner_health(Some(owner), false, candidate, true),
            CarrierFamilyHealth::RepairOnly
        );
        assert_eq!(
            cross_family_reliable_owner_health(Some(owner), false, candidate, false),
            CarrierFamilyHealth::DisabledForReliableOwner
        );
        assert!(
            cross_family_reliable_owner_health(None, false, candidate, false)
                .reliable_owner_allowed()
        );
    }
}
