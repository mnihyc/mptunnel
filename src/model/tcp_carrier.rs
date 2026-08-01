//! Product-service validation for elastic TCP carriers.
//!
//! The directional sender owns the frozen comparison key and supplies exact
//! writer/Data-ACK boundaries. This module owns only the RFC-defined geometry,
//! bounded evidence, and exact rate ordering. It has no sockets, timers,
//! carrier actors, or platform policy.

use super::ack_clock::reliable_data_ack_rate_coverage_floor_bytes;
use super::capacity::{
    reliable_path_startup_sample_limit_bytes, reliable_product_measurement_session_envelope_bytes,
};
use crate::mux::MuxLimits;
use crate::protocol::{PathUsage, TcpCarrierValidationResult};
use std::cmp::Ordering;
use std::num::NonZeroU64;
use std::time::Duration;

/// Session-owned policy epochs that may invalidate one frozen comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpCarrierPolicyEpochs {
    pub(crate) ordinary_eligibility_generation: NonZeroU64,
    pub(crate) admission_policy_generation: NonZeroU64,
    pub(crate) resource_policy_generation: NonZeroU64,
}

/// Stable sender-owned generations surrounding one ordinary placement.
///
/// Queue occupancy and transport samples are deliberately absent. They are
/// mutable evidence, not authority to create another admission generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpCarrierStableGenerations {
    pub(crate) membership_generation: u64,
    pub(crate) ordinary_eligibility_generation: NonZeroU64,
    pub(crate) authority_class: PathUsage,
    pub(crate) admission_policy_generation: NonZeroU64,
    pub(crate) resource_policy_generation: NonZeroU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpCarrierValidationGeometry {
    startup_coverage_bytes: u64,
    rate_window_bytes: u64,
    cohort_coverage_bytes: u64,
    candidate_work_limit_bytes: u64,
}

impl TcpCarrierValidationGeometry {
    pub(crate) fn cohort_coverage_bytes(self) -> u64 {
        self.cohort_coverage_bytes
    }
}

/// Freezes one validation's byte geometry before candidate Product service.
///
/// Each input is one frozen ordinary carrier's established two-BDP Product
/// service pipe. The checked sum is bounded once by the existing session
/// measurement envelope. Cohorts contain whole established rate windows and
/// never exceed that envelope.
pub(crate) fn tcp_carrier_validation_geometry(
    ordinary_service_pipes: impl IntoIterator<Item = u64>,
    mux_limits: MuxLimits,
) -> Option<TcpCarrierValidationGeometry> {
    let startup_coverage_bytes = reliable_path_startup_sample_limit_bytes(mux_limits);
    let rate_window_bytes = reliable_data_ack_rate_coverage_floor_bytes(mux_limits);
    let validation_envelope = reliable_product_measurement_session_envelope_bytes(mux_limits);
    if startup_coverage_bytes == 0
        || rate_window_bytes == 0
        || validation_envelope < rate_window_bytes
    {
        return None;
    }

    let mut ordinary_count = 0_usize;
    let mut ordinary_sum = 0_u64;
    for pipe in ordinary_service_pipes {
        if pipe == 0 {
            return None;
        }
        ordinary_count = ordinary_count.checked_add(1)?;
        ordinary_sum = ordinary_sum.checked_add(pipe)?;
    }
    if ordinary_count == 0 {
        return None;
    }

    let ordinary_pipe_bytes = ordinary_sum.min(validation_envelope);
    let desired_coverage = ordinary_pipe_bytes.max(rate_window_bytes);
    let desired_windows = desired_coverage.div_ceil(rate_window_bytes);
    let cohort_coverage_bytes = desired_windows.checked_mul(rate_window_bytes)?;
    if cohort_coverage_bytes > validation_envelope {
        return None;
    }
    let candidate_work_limit_bytes = startup_coverage_bytes.checked_add(cohort_coverage_bytes)?;

    Some(TcpCarrierValidationGeometry {
        startup_coverage_bytes,
        rate_window_bytes,
        cohort_coverage_bytes,
        candidate_work_limit_bytes,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProductServiceRate {
    bytes: u64,
    elapsed: Duration,
}

impl ProductServiceRate {
    fn new(bytes: u64, writer_elapsed: Duration, ack_elapsed: Duration) -> Option<Self> {
        let elapsed = writer_elapsed.max(ack_elapsed);
        (bytes > 0 && !elapsed.is_zero()).then_some(Self { bytes, elapsed })
    }

    fn cmp_exact(self, other: Self) -> Ordering {
        compare_nonnegative_fractions(
            self.bytes as u128,
            self.elapsed.as_nanos(),
            other.bytes as u128,
            other.elapsed.as_nanos(),
        )
    }
}

/// Compares nonnegative fractions without overflowing a cross product.
fn compare_nonnegative_fractions(
    mut left_numerator: u128,
    mut left_denominator: u128,
    mut right_numerator: u128,
    mut right_denominator: u128,
) -> Ordering {
    debug_assert!(left_denominator > 0);
    debug_assert!(right_denominator > 0);
    let mut reversed = false;
    loop {
        let left_integer = left_numerator / left_denominator;
        let right_integer = right_numerator / right_denominator;
        if left_integer != right_integer {
            let order = left_integer.cmp(&right_integer);
            return if reversed { order.reverse() } else { order };
        }

        let left_remainder = left_numerator % left_denominator;
        let right_remainder = right_numerator % right_denominator;
        match (left_remainder == 0, right_remainder == 0) {
            (true, true) => return Ordering::Equal,
            (true, false) => {
                return if reversed {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
            }
            (false, true) => {
                return if reversed {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }
            (false, false) => {}
        }

        left_numerator = left_denominator;
        left_denominator = left_remainder;
        right_numerator = right_denominator;
        right_denominator = right_remainder;
        reversed = !reversed;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProductServiceCohort {
    candidate_bytes: u64,
    target_rate: ProductServiceRate,
    aggregate_rate: ProductServiceRate,
}

impl ProductServiceCohort {
    fn new(
        geometry: TcpCarrierValidationGeometry,
        target_bytes: u64,
        aggregate_bytes: u64,
        candidate_bytes: u64,
        writer_elapsed: Duration,
        ack_elapsed: Duration,
    ) -> Option<Self> {
        if target_bytes < geometry.cohort_coverage_bytes
            || aggregate_bytes < target_bytes
            || candidate_bytes > target_bytes
        {
            return None;
        }
        Some(Self {
            candidate_bytes,
            target_rate: ProductServiceRate::new(target_bytes, writer_elapsed, ack_elapsed)?,
            aggregate_rate: ProductServiceRate::new(aggregate_bytes, writer_elapsed, ack_elapsed)?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TcpCarrierValidationPhase {
    Reference,
    CandidateStartup,
    Assisted,
    Confirmation,
    Settled(TcpCarrierValidationResult),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TcpCarrierValidationUpdate {
    Pending,
    Advanced(TcpCarrierValidationPhase),
    Settled(TcpCarrierValidationResult),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct TcpCarrierCandidateWorkState {
    pub(crate) queued_bytes: u64,
    pub(crate) original_flight_bytes: u64,
    pub(crate) recovery_bytes: u64,
    pub(crate) reorder_debt_bytes: u64,
}

impl TcpCarrierCandidateWorkState {
    fn is_zero(self) -> bool {
        self.queued_bytes == 0
            && self.original_flight_bytes == 0
            && self.recovery_bytes == 0
            && self.reorder_debt_bytes == 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct CandidateAssignmentLedger {
    assigned_bytes: u64,
    resolved_bytes: u64,
    qualified_release_bytes: u64,
}

impl CandidateAssignmentLedger {
    fn unresolved_bytes(self) -> Option<u64> {
        self.assigned_bytes.checked_sub(self.resolved_bytes)
    }

    fn record_assignment(&mut self, bytes: u64, limit: u64) -> bool {
        if bytes == 0 {
            return false;
        }
        let Some(assigned) = self.assigned_bytes.checked_add(bytes) else {
            return false;
        };
        if assigned > limit {
            return false;
        }
        self.assigned_bytes = assigned;
        true
    }

    fn record_resolution(&mut self, resolved_bytes: u64, qualified_release_bytes: u64) -> bool {
        if resolved_bytes == 0 || qualified_release_bytes > resolved_bytes {
            return false;
        }
        let Some(resolved) = self.resolved_bytes.checked_add(resolved_bytes) else {
            return false;
        };
        let Some(qualified) = self
            .qualified_release_bytes
            .checked_add(qualified_release_bytes)
        else {
            return false;
        };
        if resolved > self.assigned_bytes || qualified > resolved {
            return false;
        }
        self.resolved_bytes = resolved;
        self.qualified_release_bytes = qualified;
        true
    }
}

/// Ordered sender-side evidence state for one exact directional validation.
///
/// Runtime owns identities, the frozen key, exact offset ranges, and absolute
/// deadlines. It calls `withdraw` when any of those authorities changes. This
/// state independently prevents over-assignment and phase contamination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpCarrierValidationState {
    geometry: TcpCarrierValidationGeometry,
    phase: TcpCarrierValidationPhase,
    reference: Option<ProductServiceCohort>,
    assisted: Option<ProductServiceCohort>,
    confirmation: Option<ProductServiceCohort>,
    startup: CandidateAssignmentLedger,
    assisted_work: CandidateAssignmentLedger,
}

impl TcpCarrierValidationState {
    pub(crate) fn new(geometry: TcpCarrierValidationGeometry) -> Self {
        Self {
            geometry,
            phase: TcpCarrierValidationPhase::Reference,
            reference: None,
            assisted: None,
            confirmation: None,
            startup: CandidateAssignmentLedger::default(),
            assisted_work: CandidateAssignmentLedger::default(),
        }
    }

    pub(crate) fn result(self) -> Option<TcpCarrierValidationResult> {
        match self.phase {
            TcpCarrierValidationPhase::Settled(result) => Some(result),
            _ => None,
        }
    }

    pub(crate) fn cohort_coverage_bytes(self) -> u64 {
        self.geometry.cohort_coverage_bytes()
    }

    pub(crate) fn candidate_assignment_credit_bytes(self) -> u64 {
        match self.phase {
            TcpCarrierValidationPhase::CandidateStartup => self
                .geometry
                .startup_coverage_bytes
                .saturating_sub(self.startup.assigned_bytes),
            TcpCarrierValidationPhase::Assisted if self.assisted.is_none() => self
                .geometry
                .cohort_coverage_bytes
                .saturating_sub(self.assisted_work.assigned_bytes),
            _ => 0,
        }
    }

    pub(crate) fn record_candidate_assignment(&mut self, bytes: u64) -> TcpCarrierValidationUpdate {
        let valid = match self.phase {
            TcpCarrierValidationPhase::CandidateStartup => self
                .startup
                .record_assignment(bytes, self.geometry.startup_coverage_bytes),
            TcpCarrierValidationPhase::Assisted if self.assisted.is_none() => {
                let within_phase = self
                    .assisted_work
                    .record_assignment(bytes, self.geometry.cohort_coverage_bytes);
                let within_total = self
                    .startup
                    .assigned_bytes
                    .checked_add(self.assisted_work.assigned_bytes)
                    .is_some_and(|total| total <= self.geometry.candidate_work_limit_bytes);
                within_phase && within_total
            }
            _ => false,
        };
        if valid {
            TcpCarrierValidationUpdate::Pending
        } else {
            self.settle_withdrawn()
        }
    }

    pub(crate) fn observe_candidate_resolution(
        &mut self,
        resolved_bytes: u64,
        qualified_release_bytes: u64,
    ) -> TcpCarrierValidationUpdate {
        let valid = match self.phase {
            TcpCarrierValidationPhase::CandidateStartup => self
                .startup
                .record_resolution(resolved_bytes, qualified_release_bytes),
            TcpCarrierValidationPhase::Assisted => self
                .assisted_work
                .record_resolution(resolved_bytes, qualified_release_bytes),
            _ => false,
        };
        if valid {
            TcpCarrierValidationUpdate::Pending
        } else {
            self.settle_withdrawn()
        }
    }

    /// Installs the one complete ACK-bounded cohort for the current phase.
    pub(crate) fn observe_cohort(
        &mut self,
        target_bytes: u64,
        aggregate_bytes: u64,
        candidate_bytes: u64,
        writer_elapsed: Duration,
        ack_elapsed: Duration,
    ) -> TcpCarrierValidationUpdate {
        let Some(cohort) = ProductServiceCohort::new(
            self.geometry,
            target_bytes,
            aggregate_bytes,
            candidate_bytes,
            writer_elapsed,
            ack_elapsed,
        ) else {
            return self.settle_withdrawn();
        };
        let installed = match self.phase {
            TcpCarrierValidationPhase::Reference
                if candidate_bytes == 0 && self.reference.is_none() =>
            {
                self.reference = Some(cohort);
                true
            }
            TcpCarrierValidationPhase::Assisted
                if self.assisted.is_none()
                    && self.assisted_work.qualified_release_bytes
                        >= self.geometry.rate_window_bytes
                    && candidate_bytes == self.assisted_work.qualified_release_bytes =>
            {
                self.assisted = Some(cohort);
                true
            }
            TcpCarrierValidationPhase::Confirmation
                if candidate_bytes == 0 && self.confirmation.is_none() =>
            {
                self.confirmation = Some(cohort);
                true
            }
            _ => false,
        };
        if installed {
            TcpCarrierValidationUpdate::Pending
        } else {
            self.settle_withdrawn()
        }
    }

    /// Commits a phase boundary after runtime proves exact candidate Product
    /// queue, flight, recovery, and reorder state are all empty.
    pub(crate) fn advance_at_causal_boundary(
        &mut self,
        candidate_work: TcpCarrierCandidateWorkState,
    ) -> TcpCarrierValidationUpdate {
        if !candidate_work.is_zero() {
            return self.settle_withdrawn();
        }
        let next = match self.phase {
            TcpCarrierValidationPhase::Reference if self.reference.is_some() => {
                Some(TcpCarrierValidationPhase::CandidateStartup)
            }
            TcpCarrierValidationPhase::CandidateStartup
                if self.startup.assigned_bytes == self.geometry.startup_coverage_bytes
                    && self.startup.resolved_bytes == self.startup.assigned_bytes
                    && self.startup.qualified_release_bytes == self.startup.assigned_bytes =>
            {
                Some(TcpCarrierValidationPhase::Assisted)
            }
            TcpCarrierValidationPhase::Assisted
                if self.assisted.is_some()
                    && self.assisted_work.unresolved_bytes() == Some(0)
                    && self.assisted.is_some_and(|cohort| {
                        cohort.candidate_bytes >= self.geometry.rate_window_bytes
                    }) =>
            {
                Some(TcpCarrierValidationPhase::Confirmation)
            }
            TcpCarrierValidationPhase::Confirmation if self.confirmation.is_some() => {
                let result = self.compare_complete_cohorts();
                self.phase = TcpCarrierValidationPhase::Settled(result);
                return TcpCarrierValidationUpdate::Settled(result);
            }
            TcpCarrierValidationPhase::Settled(result) => {
                return TcpCarrierValidationUpdate::Settled(result);
            }
            _ => None,
        };
        let Some(next) = next else {
            return self.settle_withdrawn();
        };
        self.phase = next;
        TcpCarrierValidationUpdate::Advanced(next)
    }

    pub(crate) fn withdraw(&mut self) -> TcpCarrierValidationUpdate {
        self.settle_withdrawn()
    }

    fn compare_complete_cohorts(self) -> TcpCarrierValidationResult {
        let reference = self.reference.expect("complete reference cohort");
        let assisted = self.assisted.expect("complete assisted cohort");
        let confirmation = self.confirmation.expect("complete confirmation cohort");
        if assisted.target_rate.cmp_exact(reference.target_rate) == Ordering::Greater
            && assisted.target_rate.cmp_exact(confirmation.target_rate) == Ordering::Greater
            && assisted.aggregate_rate.cmp_exact(reference.aggregate_rate) == Ordering::Greater
            && assisted
                .aggregate_rate
                .cmp_exact(confirmation.aggregate_rate)
                == Ordering::Greater
        {
            TcpCarrierValidationResult::Retain
        } else {
            TcpCarrierValidationResult::NoGain
        }
    }

    fn settle_withdrawn(&mut self) -> TcpCarrierValidationUpdate {
        if let TcpCarrierValidationPhase::Settled(result) = self.phase {
            return TcpCarrierValidationUpdate::Settled(result);
        }
        self.phase = TcpCarrierValidationPhase::Settled(TcpCarrierValidationResult::Withdrawn);
        TcpCarrierValidationUpdate::Settled(TcpCarrierValidationResult::Withdrawn)
    }
}

#[cfg(test)]
#[path = "tcp_carrier_test.rs"]
mod tests;
