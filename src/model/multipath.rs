//! Shared product vocabulary and per-flow subflow admission state.
//!
//! TCP and QUIC carriers provide evidence to this model. They consume its path
//! roles and work decisions, but neither carrier owns the product policy.

use crate::config::MppPerformanceConfig;
use crate::model::capacity::MIN_RATE_SAMPLE_BYTES;
use crate::model::path::CarrierPathKey;
use smallvec::SmallVec;

pub(crate) fn cross_family_reliable_owner_allowed(
    current_owner: Option<CarrierPathKey>,
    candidate: CarrierPathKey,
    candidate_bulk_rate_proven: bool,
    candidate_continues_lower_frontier: bool,
) -> bool {
    let Some(owner) = current_owner else {
        return true;
    };
    if owner == candidate || owner.underlay == candidate.underlay {
        return true;
    }

    // MPTCP/MPQUIC schedule by path state inside one carrier-family recovery
    // model. mptunnel's mixed TCP+QUIC reliable streams have independent ACK
    // clocks, pacing, flow control, and loss recovery. A cross-family path
    // therefore cannot become an ordered-byte owner merely because it is
    // lower-ETA at a clear frontier; it is owner-eligible only when it is
    // continuing a lower-frontier range it already owns. Explicit Service
    // migration/failover is handled outside this health predicate.
    if candidate_continues_lower_frontier && candidate_bulk_rate_proven {
        return true;
    }
    false
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ExtraTrafficBudget {
    owner_progress_bytes: u64,
    repair_spent_bytes: u64,
    startup_floor_bytes: u64,
    percent_budget: u16,
}

impl ExtraTrafficBudget {
    pub(crate) fn new(
        owner_progress_bytes: u64,
        repair_spent_bytes: u64,
        startup_floor_bytes: usize,
        performance: MppPerformanceConfig,
    ) -> Self {
        Self {
            owner_progress_bytes,
            repair_spent_bytes,
            startup_floor_bytes: startup_floor_bytes as u64,
            percent_budget: performance.extra_traffic_hint_percent,
        }
    }

    pub(crate) fn limit_bytes(self) -> u64 {
        self.startup_floor_bytes.saturating_add(
            self.owner_progress_bytes
                .saturating_mul(self.percent_budget as u64)
                / 100,
        )
    }

    pub(crate) fn remaining_bytes(self) -> usize {
        self.limit_bytes()
            .saturating_sub(self.repair_spent_bytes)
            .min(usize::MAX as u64) as usize
    }

    pub(crate) fn can_spend(self, bytes: usize) -> bool {
        self.repair_spent_bytes.saturating_add(bytes as u64) <= self.limit_bytes()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ExtraTrafficLedger {
    owner_progress_bytes: u64,
    repair_bytes: u64,
}

impl ExtraTrafficLedger {
    #[cfg(test)]
    pub(crate) fn owner_progress_bytes(self) -> u64 {
        self.owner_progress_bytes
    }

    pub(crate) fn repair_spent_bytes(self) -> u64 {
        self.repair_bytes
    }

    pub(crate) fn record_owner_progress(&mut self, bytes: usize) {
        self.owner_progress_bytes = self.owner_progress_bytes.saturating_add(bytes as u64);
    }

    pub(crate) fn record_repair(&mut self, bytes: usize) {
        self.repair_bytes = self.repair_bytes.saturating_add(bytes as u64);
    }

    pub(crate) fn budget(
        self,
        startup_floor_bytes: usize,
        performance: MppPerformanceConfig,
    ) -> ExtraTrafficBudget {
        ExtraTrafficBudget::new(
            self.owner_progress_bytes,
            self.repair_spent_bytes(),
            startup_floor_bytes,
            performance,
        )
    }
}

/// One path's authority for the next unique product range.
///
/// A single enum prevents role/work/decision combinations that the protocol
/// cannot execute. Repair selection is independent because it never grants
/// ownership of a new product offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathAdmission {
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
    ProbeOnly,
    Standby,
}

impl PathAdmission {
    pub(crate) fn owns_unique_data(self) -> bool {
        matches!(self, Self::Service | Self::Subflow)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SubflowAdmissionInput<K = CarrierPathKey> {
    pub(crate) key: K,
    pub(crate) bulk_rate_proven: bool,
    pub(crate) startup_owner_allowed: bool,
    pub(crate) frontier_clear: bool,
    pub(crate) completion_improves: bool,
    pub(crate) observed_goodput_non_degrading: bool,
    pub(crate) owner_bytes: usize,
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
pub(crate) struct FlowSubflowSet<K = CarrierPathKey> {
    service: K,
    startup_owner_credit_bytes: u64,
    startup_owner: Option<StartupSubflowOwner<K>>,
    // Keys remain after startup graduation so one path cannot receive a fresh
    // unproven startup allowance in the same flow epoch.
    admitted_keys: SmallVec<[K; 4]>,
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
    pub(crate) fn new(service: K, startup_owner_credit_bytes: usize) -> Self {
        Self {
            service,
            startup_owner_credit_bytes: startup_owner_credit_bytes as u64,
            startup_owner: None,
            admitted_keys: SmallVec::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn admitted_keys(&self) -> &[K] {
        &self.admitted_keys
    }

    pub(crate) fn has_admitted_paths(&self) -> bool {
        !self.admitted_keys.is_empty()
    }

    pub(crate) fn startup_owner_key(&self) -> Option<K> {
        self.startup_owner.map(|startup| startup.key)
    }

    pub(crate) fn service_key(&self) -> K {
        self.service
    }

    pub(crate) fn startup_owner_sealed_sample_bytes(&self, key: K) -> Option<u64> {
        self.startup_owner
            .filter(|startup| startup.key == key && startup.sample_seal.is_some())
            .map(|startup| startup.owner_sent_bytes)
    }

    pub(crate) fn startup_owner_sample_sealed(&self, key: K) -> bool {
        self.startup_owner
            .filter(|startup| startup.key == key)
            .is_some_and(|startup| startup.sample_seal.is_some())
    }

    #[cfg(test)]
    pub(crate) fn startup_owner_sent_bytes(&self, key: K) -> Option<u64> {
        self.startup_owner
            .filter(|startup| startup.key == key)
            .map(|startup| startup.owner_sent_bytes)
    }

    pub(crate) fn seal_startup_owner_if_next_frame_exceeds_credit(
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

    pub(crate) fn graduate_startup_owner(&mut self, key: K) -> bool {
        if self.startup_owner_key() != Some(key) {
            return false;
        }
        self.startup_owner = None;
        true
    }

    pub(crate) fn matches_envelope(&self, service: K, startup_owner_credit_bytes: usize) -> bool {
        self.service == service
            && self.startup_owner_credit_bytes == startup_owner_credit_bytes as u64
    }

    pub(crate) fn admit_subflow_owner(&mut self, input: SubflowAdmissionInput<K>) -> PathAdmission {
        if input.key == self.service {
            return PathAdmission::Service;
        }

        if !self.subflow_owner_allowed(input) {
            if self.probe_allowed(input) {
                return PathAdmission::ProbeOnly;
            }
            return PathAdmission::Standby;
        }

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

        if !self.admitted_keys.contains(&input.key) {
            self.admitted_keys.push(input.key);
        }

        PathAdmission::Subflow
    }

    pub(crate) fn rollback_subflow_owner(&mut self, input: SubflowAdmissionInput<K>) {
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
    }

    fn startup_owner_key_available(&self, key: K) -> bool {
        match self.startup_owner {
            Some(startup) => startup.key == key,
            None => !self.admitted_keys.contains(&key),
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
    }
}

#[cfg(test)]
#[path = "multipath_test.rs"]
mod tests;
