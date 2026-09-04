//! Connection-level accounting shared by multipath scheduling directions.
//!
//! TCP and QUIC retain independent congestion control and recovery. This model
//! accounts for duplicate traffic relative to acknowledged progress of
//! original connection data. Product recovery authority is structural and is
//! not granted or denied by that accounting target.

use crate::performance::MppPerformanceConfig;
use std::time::{Duration, Instant};

/// Monotonic absolute owner fallback for one exact ranked live-owner prefix.
///
/// A metric refresh may reveal that the original assignment should mature
/// sooner, but it cannot restart that immutable assignment with a later
/// deadline. Changing the lowest frontier, exact owner, or latest
/// participating assignment begins a distinct transaction; target-driven
/// quantum changes alone do not.
#[derive(Debug)]
pub(crate) struct LiveOwnerFallbackEpoch<I> {
    frontier: Option<u64>,
    owners: Vec<I>,
    assignment_at: Option<Instant>,
    deadline: Option<Instant>,
}

impl<I> Default for LiveOwnerFallbackEpoch<I> {
    fn default() -> Self {
        Self {
            frontier: None,
            owners: Vec::new(),
            assignment_at: None,
            deadline: None,
        }
    }
}

impl<I: Copy + Eq> LiveOwnerFallbackEpoch<I> {
    pub(crate) fn observe(
        &mut self,
        range: crate::protocol::OffsetRange,
        owners: &[I],
        timing: crate::model::timing::ReliableDataAckGapTiming,
    ) -> Instant {
        let same_frontier = self.frontier == Some(range.start)
            && self.assignment_at == Some(timing.assignment_at)
            && self.owners.len() == owners.len()
            && self.owners.iter().all(|owner| owners.contains(owner));
        if !same_frontier {
            self.frontier = Some(range.start);
            self.owners.clear();
            self.owners.extend_from_slice(owners);
            self.assignment_at = Some(timing.assignment_at);
            self.deadline = Some(timing.fallback_at);
            return timing.fallback_at;
        }
        let deadline = self.deadline.map_or(timing.fallback_at, |current| {
            current.min(timing.fallback_at)
        });
        self.deadline = Some(deadline);
        deadline
    }

    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct OptionalReinjectionBudget {
    delivered_data_bytes: u64,
    reinjected_bytes: u64,
    startup_floor_bytes: u64,
    percent_budget: u16,
}

impl OptionalReinjectionBudget {
    pub(crate) fn new(
        delivered_data_bytes: u64,
        reinjected_bytes: u64,
        startup_floor_bytes: usize,
        performance: MppPerformanceConfig,
    ) -> Self {
        Self {
            delivered_data_bytes,
            reinjected_bytes,
            startup_floor_bytes: startup_floor_bytes as u64,
            percent_budget: performance.optional_reinjection_budget_percent,
        }
    }

    pub(crate) fn limit_bytes(self) -> u64 {
        self.startup_floor_bytes.saturating_add(
            self.delivered_data_bytes
                .saturating_mul(self.percent_budget as u64)
                / 100,
        )
    }

    pub(crate) fn remaining_bytes(self) -> usize {
        self.limit_bytes()
            .saturating_sub(self.reinjected_bytes)
            .min(usize::MAX as u64) as usize
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct OptionalReinjectionLedger {
    delivered_data_bytes: u64,
    reinjected_bytes: u64,
}

impl OptionalReinjectionLedger {
    #[cfg(test)]
    pub(crate) fn delivered_data_bytes(self) -> u64 {
        self.delivered_data_bytes
    }

    pub(crate) fn reinjected_bytes(self) -> u64 {
        self.reinjected_bytes
    }

    pub(crate) fn record_delivered_data(&mut self, bytes: usize) {
        self.delivered_data_bytes = self.delivered_data_bytes.saturating_add(bytes as u64);
    }

    pub(crate) fn record_reinjection(&mut self, bytes: usize) {
        self.reinjected_bytes = self.reinjected_bytes.saturating_add(bytes as u64);
    }

    pub(crate) fn budget(
        self,
        startup_floor_bytes: usize,
        performance: MppPerformanceConfig,
    ) -> OptionalReinjectionBudget {
        OptionalReinjectionBudget::new(
            self.delivered_data_bytes,
            self.reinjected_bytes(),
            startup_floor_bytes,
            performance,
        )
    }
}

/// One non-accumulating live-owner successor observation per recovery interval
/// in a sending direction.
///
/// The clock is deliberately independent of gap shape, queue residency, and
/// target identity.  A contiguous tail and an authoritative frontier gap are
/// two observations of the same stalled Product direction, so recording either
/// retains one immutable successor deadline rather than accumulating missed
/// intervals. The deadline remains observable to the existing tail wake; it is
/// not Product authority and does not gate a due recovery cause.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct LiveOwnerFrontierFloorEpoch {
    next_attempt_at: Option<Instant>,
    recovery_interval: Option<Duration>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct LiveOwnerRecoveryWake {
    pub(crate) due: bool,
    pub(crate) deadline: Option<Instant>,
}

/// Combines a retained recovery cause with the existing shared live-owner
/// successor observation.
///
/// Without a retained gap/tail cause there is no wake. The cause clock remains
/// actionable independently of the configured traffic-accounting target. The
/// later epoch is retained as a successor observation pending the separate
/// service-policy transaction. A past eligibility point is returned as durable
/// due state rather than as a past timer that can disappear or busy-spin.
pub(crate) fn live_owner_recovery_wake(
    cause_deadline: Option<Instant>,
    epoch_deadline: Option<Instant>,
    observed_at: Instant,
) -> LiveOwnerRecoveryWake {
    let Some(cause_deadline) = cause_deadline else {
        return LiveOwnerRecoveryWake {
            due: false,
            deadline: None,
        };
    };
    let successor_deadline = epoch_deadline.map_or(cause_deadline, |epoch_deadline| {
        cause_deadline.max(epoch_deadline)
    });
    live_owner_recovery_wake_from_branches(
        Some(cause_deadline),
        Some(successor_deadline),
        observed_at,
    )
}

/// Resolves the two-stage authoritative-gap clock without coupling the
/// retained cause to a configured traffic-accounting target.
pub(crate) fn live_owner_gap_recovery_wake(
    candidate_deadline: Option<Instant>,
    owner_fallback_at: Option<Instant>,
    epoch_deadline: Option<Instant>,
    observed_at: Instant,
) -> LiveOwnerRecoveryWake {
    let cause_deadline = candidate_deadline.or(owner_fallback_at);
    let successor_deadline = owner_fallback_at.map(|fallback_at| {
        epoch_deadline.map_or(fallback_at, |epoch_deadline| {
            fallback_at.max(epoch_deadline)
        })
    });
    live_owner_recovery_wake_from_branches(cause_deadline, successor_deadline, observed_at)
}

fn live_owner_recovery_wake_from_branches(
    cause_deadline: Option<Instant>,
    successor_deadline: Option<Instant>,
    observed_at: Instant,
) -> LiveOwnerRecoveryWake {
    let due = cause_deadline
        .into_iter()
        .chain(successor_deadline)
        .any(|deadline| deadline <= observed_at);
    let deadline = cause_deadline
        .into_iter()
        .chain(successor_deadline)
        .filter(|deadline| *deadline > observed_at)
        .min();
    LiveOwnerRecoveryWake { due, deadline }
}

/// Extends one accepted live-owner repair batch by the recovery interval of
/// another accepted frame without shortening the retained successor
/// observation.
pub(crate) fn include_live_owner_recovery_interval(
    current: Option<Duration>,
    accepted: Duration,
) -> Duration {
    current.map_or(accepted, |current| current.max(accepted))
}

impl LiveOwnerFrontierFloorEpoch {
    pub(crate) fn attempt_ready(&self, observed_at: Instant) -> bool {
        self.next_attempt_at
            .is_none_or(|deadline| observed_at >= deadline)
    }

    pub(crate) fn next_attempt_at(&self) -> Option<Instant> {
        self.next_attempt_at
    }

    pub(crate) fn record_accepted_attempt(
        &mut self,
        observed_at: Instant,
        recovery_interval: Duration,
    ) {
        // Further accepted recovery inside the current interval does not
        // postpone its retained successor observation. Once the immutable
        // deadline is due, the first newly accepted live-owner batch starts
        // exactly one successor interval; missed intervals never accumulate.
        if self.attempt_ready(observed_at) {
            self.recovery_interval = Some(recovery_interval);
            self.next_attempt_at = observed_at
                .checked_add(recovery_interval)
                .or(Some(observed_at));
        }
    }

    pub(crate) fn record_data_ack_progress(&mut self, observed_at: Instant) {
        let Some(recovery_interval) = self.recovery_interval else {
            // The first live-owner recovery still derives its cause deadline
            // from exact OriginalData age. The ACK that first exposes a gap
            // must not manufacture a successor observation before acceptance.
            return;
        };
        let progress_deadline = observed_at
            .checked_add(recovery_interval)
            .unwrap_or(observed_at);
        self.next_attempt_at = Some(
            self.next_attempt_at
                .map_or(progress_deadline, |current| current.max(progress_deadline)),
        );
    }

    #[cfg(test)]
    pub(crate) fn record_accepted_attempt_at_for_test(
        &mut self,
        observed_at: Instant,
        recovery_interval: Duration,
    ) {
        self.record_accepted_attempt(observed_at, recovery_interval);
    }

    #[cfg(test)]
    pub(crate) fn record_data_ack_progress_at_for_test(&mut self, observed_at: Instant) {
        self.record_data_ack_progress(observed_at);
    }
}

#[cfg(test)]
#[path = "tests_multipath.rs"]
mod tests;
