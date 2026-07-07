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
    /// The current primary ordered-byte owner for this product stream.
    ///
    /// This is the mptunnel equivalent of the scheduler-selected primary path
    /// in an MPTCP/MPQUIC connection: it must remain fed while healthy and must
    /// not be displaced by validation/probe traffic.
    Service,
    /// A validated additional path that may carry unique ordered bytes.
    ///
    /// `Subflow` is intentionally the same term used by MPTCP. In mptunnel it
    /// means an additional same-family path admitted by the no-worse guard. It
    /// may spend bounded startup owner credit with sender evidence, then
    /// requires bulk-rate evidence for steady-state owner bytes.
    Subflow,
    /// Path-manager/control-plane probing only. A probe cannot own product
    /// offsets and cannot create product delivery proof.
    Probe,
    RepairOnly,
    Standby,
    /// Failed paths are filtered before ordinary sender targets are built.
    /// The role remains in the shared vocabulary for diagnostics and RFC
    /// alignment, but it is not an admissible data-plane outcome.
    #[allow(dead_code)]
    Failed,
}

impl PathRuntimeRole {
    pub(super) fn may_own_unique_data(self) -> bool {
        matches!(self, Self::Service | Self::Subflow)
    }

    pub(super) fn may_repair(self) -> bool {
        matches!(self, Self::Service | Self::Subflow | Self::RepairOnly)
    }

    pub(super) fn is_subflow_owner(self) -> bool {
        matches!(self, Self::Subflow)
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

    // Production v1 deliberately does not stripe one ordered product stream
    // across TCP and QUIC owner paths at the same time. MPTCP subflows share one
    // transport family and MPQUIC paths share one QUIC connection/path model; in
    // mptunnel a TCP carrier and a QUIC reliable-stream carrier have independent
    // recovery, pacing, flow-control, and ACK clocks. Treating both as
    // equal OwnerData subflows created cross-family holes, repair storms, and the
    // shaped reliable-mixed all-path collapse. Cross-family paths remain useful
    // as Probe/RepairOnly/Standby, but same-stream reliable OwnerData stays
    // within the current service family until an explicit future scheduler can
    // prove cross-family no-worse behavior.
    if candidate_bulk_rate_proven {
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
    Repair,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ExtraTrafficLedger {
    owner_payload_bytes: u64,
    repair_bytes: u64,
}

impl ExtraTrafficLedger {
    #[cfg(test)]
    pub(super) fn owner_payload_bytes(self) -> u64 {
        self.owner_payload_bytes
    }

    pub(super) fn optional_spent_bytes(self) -> u64 {
        self.repair_bytes
    }

    pub(super) fn record_owner_payload(&mut self, bytes: usize) {
        self.owner_payload_bytes = self.owner_payload_bytes.saturating_add(bytes as u64);
    }

    pub(super) fn record_optional(&mut self, kind: ExtraTrafficKind, bytes: usize) {
        match kind {
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
pub(super) enum PathAdmissionDecision {
    Service,
    AdmitSubflow,
    ProbeOnly,
    Standby,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PathAdmission {
    pub(super) role: PathRuntimeRole,
    pub(super) work: CarrierWorkKind,
    pub(super) decision: PathAdmissionDecision,
}

impl PathAdmission {
    pub(super) fn service() -> Self {
        Self {
            role: PathRuntimeRole::Service,
            work: CarrierWorkKind::OwnerData,
            decision: PathAdmissionDecision::Service,
        }
    }

    pub(super) fn subflow_owner(role: PathRuntimeRole) -> Self {
        debug_assert!(role.is_subflow_owner());
        Self {
            role,
            work: CarrierWorkKind::OwnerData,
            decision: PathAdmissionDecision::AdmitSubflow,
        }
    }

    pub(super) fn probe_only() -> Self {
        Self {
            role: PathRuntimeRole::Probe,
            work: CarrierWorkKind::Probe,
            decision: PathAdmissionDecision::ProbeOnly,
        }
    }

    pub(super) fn standby() -> Self {
        Self {
            role: PathRuntimeRole::Standby,
            work: CarrierWorkKind::Control,
            decision: PathAdmissionDecision::Standby,
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy)]
pub(super) struct SubflowMember {
    pub(super) key: CarrierPathKey,
    pub(super) role: PathRuntimeRole,
    pub(super) owner_sent_bytes: u64,
    pub(super) optional_overhead_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SubflowAdmissionInput {
    pub(super) key: CarrierPathKey,
    pub(super) sender_evidence: bool,
    pub(super) bulk_rate_proven: bool,
    pub(super) frontier_clear: bool,
    pub(super) completion_improves: bool,
    pub(super) observed_goodput_non_degrading: bool,
    pub(super) read_gap: Duration,
    pub(super) owner_bytes: usize,
    pub(super) optional_overhead_bytes: usize,
}

/// Per-flow admission memory for additional subflows.
///
/// This object deliberately does not own product offsets. The per-range flight
/// ledger remains the source of truth for ordering ownership. The set only
/// remembers whether measured subflows still fit the current no-worse
/// admission envelope. This mirrors the MPTCP distinction
/// between connection-level data ownership and per-subflow scheduling.
#[derive(Debug, Clone)]
pub(super) struct FlowSubflowSet {
    _generation: u64,
    service: CarrierPathKey,
    owner_credit_bytes: u64,
    optional_overhead_budget_bytes: u64,
    optional_overhead_spent_bytes: u64,
    max_read_gap_budget: Duration,
    members: Vec<SubflowMember>,
}

impl FlowSubflowSet {
    pub(super) fn new(
        generation: u64,
        service: CarrierPathKey,
        owner_credit_bytes: usize,
        optional_overhead_budget_bytes: usize,
        max_read_gap_budget: Duration,
    ) -> Self {
        Self {
            _generation: generation,
            service,
            owner_credit_bytes: owner_credit_bytes as u64,
            optional_overhead_budget_bytes: optional_overhead_budget_bytes as u64,
            optional_overhead_spent_bytes: 0,
            max_read_gap_budget,
            members: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(super) fn members(&self) -> &[SubflowMember] {
        &self.members
    }

    #[cfg(test)]
    pub(super) fn optional_overhead_spent_bytes(&self) -> u64 {
        self.optional_overhead_spent_bytes
    }

    pub(super) fn matches_envelope(
        &self,
        service: CarrierPathKey,
        owner_credit_bytes: usize,
        optional_overhead_budget_bytes: usize,
        max_read_gap_budget: Duration,
    ) -> bool {
        self.service == service
            && self.owner_credit_bytes == owner_credit_bytes as u64
            && self.optional_overhead_budget_bytes == optional_overhead_budget_bytes as u64
            && self.max_read_gap_budget == max_read_gap_budget
    }

    pub(super) fn admit_subflow_owner(&mut self, input: SubflowAdmissionInput) -> PathAdmission {
        if input.key == self.service {
            return PathAdmission::service();
        }
        if !self.subflow_owner_allowed(input) {
            if self.probe_allowed(input) {
                return PathAdmission::probe_only();
            }
            return PathAdmission::standby();
        }

        let role = PathRuntimeRole::Subflow;

        self.optional_overhead_spent_bytes = self
            .optional_overhead_spent_bytes
            .saturating_add(input.optional_overhead_bytes as u64);
        if let Some(member) = self
            .members
            .iter_mut()
            .find(|member| member.key == input.key)
        {
            debug_assert!(member.role.is_subflow_owner());
            member.owner_sent_bytes = member
                .owner_sent_bytes
                .saturating_add(input.owner_bytes as u64);
            member.optional_overhead_bytes = member
                .optional_overhead_bytes
                .saturating_add(input.optional_overhead_bytes as u64);
        } else {
            self.members.push(SubflowMember {
                key: input.key,
                role,
                owner_sent_bytes: input.owner_bytes as u64,
                optional_overhead_bytes: input.optional_overhead_bytes as u64,
            });
        }

        PathAdmission::subflow_owner(role)
    }

    fn subflow_owner_allowed(&self, input: SubflowAdmissionInput) -> bool {
        // Common gates are the invariant part of the no-worse rule: a
        // non-Service path may only receive unique bytes when the ordered
        // frontier is clear, modeled completion improves, observed goodput is
        // not degrading, read-gap pressure is within budget, and repair
        // overhead remains bounded.

        let common_ok = input.frontier_clear
            && input.completion_improves
            && input.observed_goodput_non_degrading
            && input.read_gap <= self.max_read_gap_budget
            && self
                .optional_overhead_spent_bytes
                .saturating_add(input.optional_overhead_bytes as u64)
                <= self.optional_overhead_budget_bytes
            && (input.owner_bytes as u64) <= self.owner_credit_bytes;
        if !common_ok {
            return false;
        }
        if input.bulk_rate_proven {
            return true;
        }

        if !input.sender_evidence || input.key.underlay != self.service.underlay {
            return false;
        }

        let already_sent = self
            .members
            .iter()
            .find(|member| member.key == input.key)
            .map_or(0, |member| member.owner_sent_bytes);
        already_sent.saturating_add(input.owner_bytes as u64) <= self.owner_credit_bytes
    }

    fn probe_allowed(&self, input: SubflowAdmissionInput) -> bool {
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
        ledger.record_optional(ExtraTrafficKind::Repair, 300);

        assert_eq!(ledger.owner_payload_bytes(), 1_000_000);
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

    #[test]
    fn subflow_set_admits_only_positive_bulk_rate_proven_subflow_owner() {
        let mut epoch =
            FlowSubflowSet::new(7, key(0), 256 * 1024, 64 * 1024, Duration::from_millis(200));

        let rejected = epoch.admit_subflow_owner(SubflowAdmissionInput {
            key: key(1),
            sender_evidence: true,
            bulk_rate_proven: true,
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
            sender_evidence: true,
            bulk_rate_proven: true,
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
            sender_evidence: false,
            bulk_rate_proven: false,
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
            sender_evidence: true,
            bulk_rate_proven: true,
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
            sender_evidence: true,
            bulk_rate_proven: true,
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
    fn subflow_set_allows_sender_evidence_startup_until_owner_credit_is_spent() {
        let payload_bytes = 64 * 1024;
        let mut epoch =
            FlowSubflowSet::new(10, key(0), payload_bytes * 3, 0, Duration::from_millis(100));
        let input = SubflowAdmissionInput {
            key: key(1),
            sender_evidence: true,
            bulk_rate_proven: false,
            frontier_clear: true,
            completion_improves: true,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: payload_bytes,
            optional_overhead_bytes: 0,
        };

        for _ in 0..3 {
            let admission = epoch.admit_subflow_owner(input);
            assert_eq!(admission.decision, PathAdmissionDecision::AdmitSubflow);
            assert_eq!(admission.role, PathRuntimeRole::Subflow);
        }

        let exhausted = epoch.admit_subflow_owner(input);
        assert_eq!(
            exhausted.decision,
            PathAdmissionDecision::ProbeOnly,
            "sender-evidence startup Subflow OwnerData is bounded by owner credit, not a new steady-state role"
        );
        assert_eq!(
            epoch.members()[0].owner_sent_bytes,
            (payload_bytes * 3) as u64
        );
    }

    #[test]
    fn subflow_set_keeps_service_as_owner_without_spending_subflow_credit() {
        let mut epoch =
            FlowSubflowSet::new(9, key(3), 256 * 1024, 16 * 1024, Duration::from_millis(100));

        let service = epoch.admit_subflow_owner(SubflowAdmissionInput {
            key: key(3),
            sender_evidence: false,
            bulk_rate_proven: false,
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
            cross_family_reliable_owner_health(Some(owner), true, candidate, true),
            CarrierFamilyHealth::Healthy
        );
        assert!(
            cross_family_reliable_owner_health(Some(owner), true, candidate, true)
                .reliable_owner_allowed()
        );
    }

    #[test]
    fn cross_family_reliable_owner_disabled_for_same_stream_owner_data() {
        let owner = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };

        assert!(
            !cross_family_reliable_owner_health(Some(owner), true, candidate, true)
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
