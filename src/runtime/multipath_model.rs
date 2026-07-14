use super::prelude::*;
use super::{CarrierPathKey, MIN_RATE_SAMPLE_BYTES};

// Shared product vocabulary for work, path roles, and cross-family health.
// Carrier implementations consume these decisions but do not define them.

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
    /// means an additional path admitted either by the no-worse guard after it
    /// has path-scoped bulk-rate evidence, or by one bounded startup sampling
    /// epoch. Liveness, proof, ACK-data visibility, and configured hints remain
    /// probe/ranking inputs; only explicit startup admission may temporarily
    /// grant an unproven path ordered-byte ownership.
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
    candidate_continues_lower_frontier: bool,
) -> CarrierFamilyHealth {
    let Some(owner) = current_owner else {
        return CarrierFamilyHealth::Healthy;
    };
    if owner == candidate || owner.underlay == candidate.underlay {
        return CarrierFamilyHealth::Healthy;
    }

    // MPTCP/MPQUIC schedule by path state inside one carrier-family recovery
    // model. mptunnel's mixed TCP+QUIC reliable streams have independent ACK
    // clocks, pacing, flow control, and loss recovery. A cross-family path
    // therefore cannot become an ordered-byte owner merely because it is
    // lower-ETA at a clear frontier; it is owner-eligible only when it is
    // continuing a lower-frontier range it already owns. Explicit Service
    // migration/failover is handled outside this health predicate.
    if candidate_continues_lower_frontier && candidate_bulk_rate_proven {
        return CarrierFamilyHealth::Healthy;
    }
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
    owner_progress_bytes: u64,
    optional_spent_bytes: u64,
    startup_floor_bytes: u64,
    percent_budget: u16,
}

impl ExtraTrafficBudget {
    pub(super) fn new(
        owner_progress_bytes: u64,
        optional_spent_bytes: u64,
        startup_floor_bytes: usize,
        performance: MppPerformanceConfig,
    ) -> Self {
        Self {
            owner_progress_bytes,
            optional_spent_bytes,
            startup_floor_bytes: startup_floor_bytes as u64,
            percent_budget: performance.extra_traffic_hint_percent,
        }
    }

    pub(super) fn limit_bytes(self) -> u64 {
        self.startup_floor_bytes.saturating_add(
            self.owner_progress_bytes
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
    owner_progress_bytes: u64,
    repair_bytes: u64,
}

impl ExtraTrafficLedger {
    #[cfg(test)]
    pub(super) fn owner_progress_bytes(self) -> u64 {
        self.owner_progress_bytes
    }

    pub(super) fn optional_spent_bytes(self) -> u64 {
        self.repair_bytes
    }

    pub(super) fn record_owner_progress(&mut self, bytes: usize) {
        self.owner_progress_bytes = self.owner_progress_bytes.saturating_add(bytes as u64);
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
            self.owner_progress_bytes,
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
pub(super) struct SubflowMember<K = CarrierPathKey> {
    pub(super) key: K,
    pub(super) role: PathRuntimeRole,
    pub(super) owner_sent_bytes: u64,
    pub(super) optional_overhead_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SubflowAdmissionInput<K = CarrierPathKey> {
    pub(super) key: K,
    pub(super) bulk_rate_proven: bool,
    pub(super) startup_owner_allowed: bool,
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
/// remembers measured membership and the one unproven candidate currently
/// consuming a bounded startup sampling budget. This mirrors the MPTCP
/// distinction between connection-level data ownership and per-subflow
/// scheduling.
#[derive(Debug, Clone)]
pub(super) struct FlowSubflowSet<K = CarrierPathKey> {
    _generation: u64,
    service: K,
    startup_owner_credit_bytes: u64,
    startup_owner: Option<StartupSubflowOwner<K>>,
    optional_overhead_budget_bytes: u64,
    optional_overhead_spent_bytes: u64,
    max_read_gap_budget: Duration,
    members: Vec<SubflowMember<K>>,
}

#[derive(Debug, Clone, Copy)]
struct StartupSubflowOwner<K = CarrierPathKey> {
    key: K,
    owner_sent_bytes: u64,
    sample_seal: Option<StartupSampleSeal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartupSampleSeal {
    CreditExhausted,
    NextFrameExceededCredit,
}

impl<K> FlowSubflowSet<K>
where
    K: Copy + Eq,
{
    pub(super) fn new(
        generation: u64,
        service: K,
        startup_owner_credit_bytes: usize,
        optional_overhead_budget_bytes: usize,
        max_read_gap_budget: Duration,
    ) -> Self {
        Self {
            _generation: generation,
            service,
            startup_owner_credit_bytes: startup_owner_credit_bytes as u64,
            startup_owner: None,
            optional_overhead_budget_bytes: optional_overhead_budget_bytes as u64,
            optional_overhead_spent_bytes: 0,
            max_read_gap_budget,
            members: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(super) fn members(&self) -> &[SubflowMember<K>] {
        &self.members
    }

    pub(super) fn has_members(&self) -> bool {
        !self.members.is_empty()
    }

    #[cfg(test)]
    pub(super) fn optional_overhead_spent_bytes(&self) -> u64 {
        self.optional_overhead_spent_bytes
    }

    pub(super) fn startup_owner_key(&self) -> Option<K> {
        self.startup_owner.map(|startup| startup.key)
    }

    pub(super) fn service_key(&self) -> K {
        self.service
    }

    pub(super) fn startup_owner_sealed_sample_bytes(&self, key: K) -> Option<u64> {
        self.startup_owner
            .filter(|startup| startup.key == key && startup.sample_seal.is_some())
            .map(|startup| startup.owner_sent_bytes)
    }

    pub(super) fn startup_owner_sample_sealed(&self, key: K) -> bool {
        self.startup_owner
            .filter(|startup| startup.key == key)
            .is_some_and(|startup| startup.sample_seal.is_some())
    }

    pub(super) fn seal_startup_owner_if_next_frame_exceeds_credit(
        &mut self,
        key: K,
        next_owner_bytes: usize,
    ) -> bool {
        let Some(startup) = self
            .startup_owner
            .as_mut()
            .filter(|startup| startup.key == key && startup.sample_seal.is_none())
        else {
            return false;
        };
        let remaining = self
            .startup_owner_credit_bytes
            .saturating_sub(startup.owner_sent_bytes);
        if startup.owner_sent_bytes < MIN_RATE_SAMPLE_BYTES || next_owner_bytes as u64 <= remaining
        {
            return false;
        }
        startup.sample_seal = Some(StartupSampleSeal::NextFrameExceededCredit);
        true
    }

    pub(super) fn graduate_startup_owner(&mut self, key: K) -> bool {
        if self.startup_owner_key() != Some(key) {
            return false;
        }
        self.startup_owner = None;
        true
    }

    pub(super) fn matches_envelope(
        &self,
        service: K,
        startup_owner_credit_bytes: usize,
        optional_overhead_budget_bytes: usize,
        max_read_gap_budget: Duration,
    ) -> bool {
        self.service == service
            && self.startup_owner_credit_bytes == startup_owner_credit_bytes as u64
            && self.optional_overhead_budget_bytes == optional_overhead_budget_bytes as u64
            && self.max_read_gap_budget == max_read_gap_budget
    }

    pub(super) fn admit_subflow_owner(&mut self, input: SubflowAdmissionInput<K>) -> PathAdmission {
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

        if !input.bulk_rate_proven {
            let startup = self.startup_owner.get_or_insert(StartupSubflowOwner {
                key: input.key,
                owner_sent_bytes: 0,
                sample_seal: None,
            });
            debug_assert!(startup.key == input.key);
            startup.owner_sent_bytes = startup
                .owner_sent_bytes
                .saturating_add(input.owner_bytes as u64);
            if startup.owner_sent_bytes >= self.startup_owner_credit_bytes {
                startup.sample_seal = Some(StartupSampleSeal::CreditExhausted);
            }
        }

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

    pub(super) fn rollback_subflow_owner(&mut self, input: SubflowAdmissionInput<K>) {
        if input.key == self.service {
            return;
        }
        if !input.bulk_rate_proven
            && let Some(startup) = self
                .startup_owner
                .as_mut()
                .filter(|startup| startup.key == input.key)
        {
            startup.owner_sent_bytes = startup
                .owner_sent_bytes
                .saturating_sub(input.owner_bytes as u64);
            if startup.sample_seal == Some(StartupSampleSeal::CreditExhausted)
                && startup.owner_sent_bytes < self.startup_owner_credit_bytes
            {
                startup.sample_seal = None;
            }
        }
        self.optional_overhead_spent_bytes = self
            .optional_overhead_spent_bytes
            .saturating_sub(input.optional_overhead_bytes as u64);
        if let Some(member) = self
            .members
            .iter_mut()
            .find(|member| member.key == input.key)
        {
            member.owner_sent_bytes = member
                .owner_sent_bytes
                .saturating_sub(input.owner_bytes as u64);
            member.optional_overhead_bytes = member
                .optional_overhead_bytes
                .saturating_sub(input.optional_overhead_bytes as u64);
        }
    }

    fn subflow_owner_allowed(&self, input: SubflowAdmissionInput<K>) -> bool {
        let bulk_rate_owner = input.bulk_rate_proven && input.completion_improves;
        let startup_owner = input.startup_owner_allowed
            && !input.bulk_rate_proven
            && self.startup_owner_key_available(input.key)
            && self.startup_owner_credit_available(input.owner_bytes);

        (bulk_rate_owner || startup_owner)
            && input.frontier_clear
            && input.observed_goodput_non_degrading
            && input.read_gap <= self.max_read_gap_budget
            && self
                .optional_overhead_spent_bytes
                .saturating_add(input.optional_overhead_bytes as u64)
                <= self.optional_overhead_budget_bytes
    }

    fn startup_owner_key_available(&self, key: K) -> bool {
        match self.startup_owner {
            Some(startup) => startup.key == key,
            None => !self.members.iter().any(|member| member.key == key),
        }
    }

    fn startup_owner_credit_available(&self, owner_bytes: usize) -> bool {
        self.startup_owner
            .is_none_or(|startup| startup.sample_seal.is_none())
            && self
                .startup_owner
                .map_or(0, |startup| startup.owner_sent_bytes)
                .saturating_add(owner_bytes as u64)
                <= self.startup_owner_credit_bytes
    }

    fn probe_allowed(&self, input: SubflowAdmissionInput<K>) -> bool {
        input.frontier_clear
            && input.read_gap <= self.max_read_gap_budget
            && self
                .optional_overhead_spent_bytes
                .saturating_add(input.optional_overhead_bytes as u64)
                <= self.optional_overhead_budget_bytes
    }
}

#[cfg(test)]
mod tests;
