//! Endpoint-scoped TCP carrier service validation.
//!
//! The directional sender owns this state. Runtime adapters provide exact
//! carrier, demand, writer-commit, saturation, and Data ACK events; this module
//! owns only their bounded RFC-defined validation transaction.

use super::path::CarrierPathInstanceId;
use crate::protocol::{
    OffsetRange, PathMetricDirection, SessionId, StreamId, TcpCarrierAcceptedPath,
    TcpCarrierValidationResult,
};
use std::cmp::Ordering;
use std::time::{Duration, Instant};

/// Endpoint-local identity of one configured TCP carrier group.
///
/// This topology identity is controller authority only. It is never encoded
/// on the wire and cannot be reconstructed from carrier locators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpServiceCarrierGroupId(u64);

impl TcpServiceCarrierGroupId {
    pub(crate) fn from_raw(raw: u64) -> Self {
        Self(raw)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpServiceCarrierFence {
    pub(crate) accepted: TcpCarrierAcceptedPath,
    pub(crate) local_instance_id: CarrierPathInstanceId,
    /// Changes only when directional eligibility changes.
    pub(crate) eligibility_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpServiceStreamFence {
    pub(crate) stream_id: StreamId,
    /// Changes only with demand class, open state, or cohort membership.
    pub(crate) demand_generation: u64,
    /// Fences detach and reattachment of this directional stream.
    pub(crate) attachment_incarnation: u64,
    pub(crate) data_ack_horizon_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TcpServiceDemandFence {
    Local,
    PeerRequest {
        request_id: u64,
        anchor: TcpServiceCarrierFence,
    },
}

impl TcpServiceDemandFence {
    fn request_id(self) -> u64 {
        match self {
            Self::Local => 0,
            Self::PeerRequest { request_id, .. } => request_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TcpServiceValidationFence {
    /// Fences the configured endpoint range and its resource generation.
    pub(crate) range_generation: u64,
    pub(crate) demand: TcpServiceDemandFence,
    pub(crate) accepted: Vec<TcpServiceCarrierFence>,
    pub(crate) candidate: TcpServiceCarrierFence,
    pub(crate) streams: Vec<TcpServiceStreamFence>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TcpServiceSuppressionIdentity {
    accepted: Vec<TcpServiceSuppressionCarrierIdentity>,
    streams: Vec<TcpServiceSuppressionStreamIdentity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpServiceSuppressionCarrierIdentity {
    accepted: TcpCarrierAcceptedPath,
    local_instance_id: CarrierPathInstanceId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpServiceSuppressionStreamIdentity {
    stream_id: StreamId,
    demand_generation: u64,
    attachment_incarnation: u64,
}

impl TcpServiceValidationFence {
    fn suppression_identity(&self) -> TcpServiceSuppressionIdentity {
        TcpServiceSuppressionIdentity {
            accepted: self
                .accepted
                .iter()
                .map(|carrier| TcpServiceSuppressionCarrierIdentity {
                    accepted: carrier.accepted,
                    local_instance_id: carrier.local_instance_id,
                })
                .collect(),
            streams: self.cohort_identity(),
        }
    }

    fn cohort_identity(&self) -> Vec<TcpServiceSuppressionStreamIdentity> {
        self.streams
            .iter()
            .map(|stream| TcpServiceSuppressionStreamIdentity {
                stream_id: stream.stream_id,
                demand_generation: stream.demand_generation,
                attachment_incarnation: stream.attachment_incarnation,
            })
            .collect()
    }

    pub(crate) fn accepted_wire_instances(&self) -> Vec<TcpCarrierAcceptedPath> {
        self.accepted
            .iter()
            .map(|carrier| carrier.accepted)
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpServiceValidationLimits {
    pub(crate) max_paths: usize,
    pub(crate) max_streams: usize,
    pub(crate) max_ack_release_records: usize,
    /// Absolute bound including one indivisible ACK-event overshoot.
    pub(crate) max_window_bytes: u64,
    pub(crate) validation_horizon_bytes: u64,
    pub(crate) unproven_flight_bytes: u64,
    pub(crate) data_ack_sample_floor_bytes: u64,
}

/// Opaque identity installed only on the frozen directional writers of one
/// reserved validation lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpServiceWriterLifecycle {
    session_id: SessionId,
    lifecycle_id: u64,
    direction: PathMetricDirection,
}

impl TcpServiceWriterLifecycle {
    /// Runtime clocks serialize `at` before constructing the point. The model
    /// rejects points from every other lifecycle.
    pub(crate) fn point(self, at: Instant) -> TcpServiceWriterPoint {
        TcpServiceWriterPoint {
            lifecycle: self,
            at,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_runtime_test(
        session_id: SessionId,
        lifecycle_id: u64,
        direction: PathMetricDirection,
    ) -> Self {
        Self {
            session_id,
            lifecycle_id,
            direction,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpServiceWriterPoint {
    lifecycle: TcpServiceWriterLifecycle,
    at: Instant,
}

impl TcpServiceWriterPoint {
    pub(crate) fn lifecycle(self) -> TcpServiceWriterLifecycle {
        self.lifecycle
    }

    pub(crate) fn at(self) -> Instant {
        self.at
    }

    fn duration_since(self, earlier: Self) -> Option<Duration> {
        (self.lifecycle == earlier.lifecycle && self.at >= earlier.at)
            .then(|| self.at.duration_since(earlier.at))
    }

    fn strictly_after(self, earlier: Self) -> bool {
        self.lifecycle == earlier.lifecycle && self.at > earlier.at
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpServiceBoundary {
    pub(crate) ack_sequence: u64,
    pub(crate) acked_at: Instant,
    /// Captured after applying the ACK while the directional sender is
    /// serialized against later writer commits.
    pub(crate) writer: TcpServiceWriterPoint,
}

#[derive(Debug, Clone)]
pub(crate) struct TcpServiceValidationPlan {
    pub(crate) session_id: SessionId,
    pub(crate) trial_id: u64,
    pub(crate) direction: PathMetricDirection,
    pub(crate) carrier_group_id: TcpServiceCarrierGroupId,
    pub(crate) fence: TcpServiceValidationFence,
    pub(crate) limits: TcpServiceValidationLimits,
    pub(crate) registered_at: Instant,
    pub(crate) absolute_deadline: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TcpServiceReleaseKind {
    Original,
    Duplicate,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TcpServiceAckRelease {
    pub(crate) carrier: TcpServiceCarrierFence,
    pub(crate) range: OffsetRange,
    /// `None` means the original writer commit preceded installation of this
    /// validation lifecycle. It drains flight but cannot enter a window.
    pub(crate) committed_at: Option<TcpServiceWriterPoint>,
    pub(crate) kind: TcpServiceReleaseKind,
    /// False when another copy makes delivery provenance ambiguous.
    pub(crate) unambiguous: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct TcpServiceDataAckEvent {
    pub(crate) sequence: u64,
    pub(crate) stream: TcpServiceStreamFence,
    pub(crate) assigned_end: u64,
    pub(crate) acked_at: Instant,
    pub(crate) next_writer_boundary: TcpServiceWriterPoint,
    pub(crate) releases: Vec<TcpServiceAckRelease>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpServiceCandidatePlacement {
    pub(crate) stream: TcpServiceStreamFence,
    pub(crate) range: OffsetRange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpServiceCandidatePlacementPermit {
    reservation: TcpServiceValidationReservation,
    id: u64,
    placement: TcpServiceCandidatePlacement,
    reserved_at: Instant,
    phase: TcpServiceValidationPhase,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TcpServiceCandidateReservationUpdate {
    Granted(TcpServiceCandidatePlacementPermit),
    Unavailable,
    Settled,
}

#[derive(Debug, Clone)]
pub(crate) struct TcpServiceSaturationObservation {
    pub(crate) observed_at: TcpServiceWriterPoint,
    pub(crate) accepted_with_original_flight: Vec<TcpServiceCarrierFence>,
    pub(crate) streams_with_fresh_demand: Vec<TcpServiceStreamFence>,
    pub(crate) blocked_stream: TcpServiceStreamFence,
    pub(crate) blocked_range: OffsetRange,
}

#[derive(Debug, Clone, Copy)]
struct TcpServiceSaturationEvent {
    sequence: u64,
    observed_at: TcpServiceWriterPoint,
}

#[derive(Debug, Clone, Copy)]
struct TcpServicePendingCandidatePlacement {
    permit: TcpServiceCandidatePlacementPermit,
    bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct TcpServiceCommittedCandidatePlacement {
    placement: TcpServiceCandidatePlacement,
    committed_at: TcpServiceWriterPoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpServiceRate {
    pub(crate) bytes: u64,
    pub(crate) elapsed: Duration,
}

impl TcpServiceRate {
    pub(crate) fn cmp_exact(self, other: Self) -> Ordering {
        compare_nonnegative_fractions(
            self.bytes as u128,
            self.elapsed.as_nanos(),
            other.bytes as u128,
            other.elapsed.as_nanos(),
        )
    }
}

/// Compares two nonnegative fractions without cross-multiplication overflow.
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
pub(crate) struct TcpServiceReferenceRange {
    lowest: TcpServiceRate,
    highest: TcpServiceRate,
}

impl TcpServiceReferenceRange {
    fn new(lowest: TcpServiceRate, highest: TcpServiceRate) -> Option<Self> {
        (lowest.cmp_exact(highest) != Ordering::Greater).then_some(Self { lowest, highest })
    }

    fn strictly_disjoint(self, other: Self) -> bool {
        self.highest.cmp_exact(other.lowest) == Ordering::Less
            || self.lowest.cmp_exact(other.highest) == Ordering::Greater
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TcpServiceNoGainSuppression {
    identity: TcpServiceSuppressionIdentity,
    rejected_reference_range: TcpServiceReferenceRange,
}

impl TcpServiceNoGainSuppression {
    fn permits(
        &self,
        identity: &TcpServiceSuppressionIdentity,
        fresh_reference_range: TcpServiceReferenceRange,
    ) -> bool {
        &self.identity != identity
            || self
                .rejected_reference_range
                .strictly_disjoint(fresh_reference_range)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TcpServiceWithdrawalReason {
    Deadline,
    FenceChanged,
    DemandEnded,
    NoGainSuppressed,
    InvalidEvidence,
    ResourceLimit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TcpServiceValidationOutcome {
    pub(crate) session_id: SessionId,
    pub(crate) trial_id: u64,
    pub(crate) candidate: TcpServiceCarrierFence,
    pub(crate) direction: PathMetricDirection,
    pub(crate) result: TcpCarrierValidationResult,
    pub(crate) withdrawal_reason: Option<TcpServiceWithdrawalReason>,
    pub(crate) no_gain_suppression: Option<TcpServiceNoGainSuppression>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TcpServiceValidationPhase {
    PreReference { completed_windows: u8 },
    Readiness,
    Comparison { completed_windows: u8 },
    PostReference { completed_windows: u8 },
    Settled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TcpServiceValidationUpdate {
    Pending,
    PhaseChanged(TcpServiceValidationPhase),
    Settled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TcpServiceValidationError {
    ZeroIdentifier,
    DirectionRequestMismatch,
    InvalidDeadline,
    InvalidBoundary,
    InvalidLimits,
    TooManyPaths,
    TooManyStreams,
    NonCanonicalCarriers,
    NonCanonicalStreams,
    CandidateInAcceptedSet,
    InvalidCarrierFence,
    InvalidStreamFence,
    HorizonOverflow,
    ValidationInProgress,
    TrialNotIncreasing,
    CandidateHistoryLimit,
    CarrierGroupLimit,
    LifecycleOverflow,
    ValidationNotInstalling,
    FenceChanged,
    DeadlineExpired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpServiceValidationReservation {
    session_id: SessionId,
    lifecycle_id: u64,
    trial_id: u64,
    candidate: TcpServiceCarrierFence,
    direction: PathMetricDirection,
    carrier_group_id: TcpServiceCarrierGroupId,
}

impl TcpServiceValidationReservation {
    fn writer_lifecycle(self) -> TcpServiceWriterLifecycle {
        TcpServiceWriterLifecycle {
            session_id: self.session_id,
            lifecycle_id: self.lifecycle_id,
            direction: self.direction,
        }
    }
}

#[derive(Debug)]
pub(crate) struct TcpServiceValidationPreparation {
    plan: TcpServiceValidationPlan,
    reservation: TcpServiceValidationReservation,
    suppression_identity: TcpServiceSuppressionIdentity,
    cohort_identity: Vec<TcpServiceSuppressionStreamIdentity>,
    identity_changed: bool,
    prior_no_gain_suppression: Option<TcpServiceNoGainSuppression>,
}

impl TcpServiceValidationPreparation {
    pub(crate) fn writer_lifecycle(&self) -> TcpServiceWriterLifecycle {
        self.reservation.writer_lifecycle()
    }

    pub(crate) fn fence(&self) -> &TcpServiceValidationFence {
        &self.plan.fence
    }
}

#[derive(Debug)]
pub(crate) struct TcpServiceActivationFailure {
    pub(crate) preparation: TcpServiceValidationPreparation,
    pub(crate) error: TcpServiceValidationError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpServiceActiveValidation {
    Installing {
        reservation: TcpServiceValidationReservation,
        absolute_deadline: Instant,
    },
    Running {
        reservation: TcpServiceValidationReservation,
        absolute_deadline: Instant,
    },
    Expiring {
        reservation: TcpServiceValidationReservation,
        stage: TcpServiceExpiredStage,
    },
}

impl TcpServiceActiveValidation {
    fn reservation(self) -> TcpServiceValidationReservation {
        match self {
            Self::Installing { reservation, .. }
            | Self::Running { reservation, .. }
            | Self::Expiring { reservation, .. } => reservation,
        }
    }

    fn absolute_deadline(self) -> Option<Instant> {
        match self {
            Self::Installing {
                absolute_deadline, ..
            }
            | Self::Running {
                absolute_deadline, ..
            } => Some(absolute_deadline),
            Self::Expiring { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TcpServiceExpiredStage {
    Installing,
    Running,
}

/// Exact lifecycle whose absolute resource lifetime elapsed. Runtime removes
/// only this lifecycle's passive writer observers before acknowledging cleanup.
#[derive(Debug)]
pub(crate) struct TcpServiceExpiredLifecycle {
    reservation: TcpServiceValidationReservation,
    stage: TcpServiceExpiredStage,
}

impl TcpServiceExpiredLifecycle {
    pub(crate) fn writer_lifecycle(&self) -> TcpServiceWriterLifecycle {
        self.reservation.writer_lifecycle()
    }

    pub(crate) fn stage(&self) -> TcpServiceExpiredStage {
        self.stage
    }

    /// Exact withdrawal authority retained even if the running validation
    /// object was lost with a cancelled task. It carries no capacity verdict.
    pub(crate) fn withdrawal_outcome(&self) -> TcpServiceValidationOutcome {
        TcpServiceValidationOutcome {
            session_id: self.reservation.session_id,
            trial_id: self.reservation.trial_id,
            candidate: self.reservation.candidate,
            direction: self.reservation.direction,
            result: TcpCarrierValidationResult::Withdrawn,
            withdrawal_reason: Some(TcpServiceWithdrawalReason::Deadline),
            no_gain_suppression: None,
        }
    }
}

#[derive(Debug, Default)]
struct TcpServiceDirectionState {
    carrier_groups: Vec<TcpServiceCarrierGroupState>,
    saturation_cohort: Option<Vec<TcpServiceSuppressionStreamIdentity>>,
    saturation_high_watermarks: Vec<(TcpServiceSuppressionStreamIdentity, u64)>,
}

#[derive(Debug)]
struct TcpServiceCarrierGroupState {
    carrier_group_id: TcpServiceCarrierGroupId,
    current_suppression_identity: TcpServiceSuppressionIdentity,
    no_gain_suppression: Option<TcpServiceNoGainSuppression>,
}

#[derive(Debug)]
pub(crate) struct TcpServiceSessionController {
    session_id: SessionId,
    active: Option<TcpServiceActiveValidation>,
    candidate_trial_high_watermarks: Vec<(CarrierPathInstanceId, u64)>,
    /// Every configured group owns at least one carrier slot, so the existing
    /// carrier-inventory bound also bounds per-direction group state.
    max_carrier_instances: usize,
    next_lifecycle_id: u64,
    next_saturation_sequence: u64,
    client_to_server: TcpServiceDirectionState,
    server_to_client: TcpServiceDirectionState,
}

impl TcpServiceSessionController {
    pub(crate) fn new(
        session_id: SessionId,
        max_carrier_instances: usize,
    ) -> Result<Self, TcpServiceValidationError> {
        if max_carrier_instances == 0 {
            return Err(TcpServiceValidationError::InvalidLimits);
        }
        Ok(Self {
            session_id,
            active: None,
            candidate_trial_high_watermarks: Vec::new(),
            max_carrier_instances,
            next_lifecycle_id: 1,
            next_saturation_sequence: 1,
            client_to_server: TcpServiceDirectionState::default(),
            server_to_client: TcpServiceDirectionState::default(),
        })
    }

    pub(crate) fn reserve(
        &mut self,
        plan: TcpServiceValidationPlan,
    ) -> Result<TcpServiceValidationPreparation, TcpServiceValidationError> {
        if plan.session_id != self.session_id {
            return Err(TcpServiceValidationError::DirectionRequestMismatch);
        }
        if self.active.is_some() {
            return Err(TcpServiceValidationError::ValidationInProgress);
        }
        validate_plan(&plan)?;
        let candidate_instance = plan.fence.candidate.local_instance_id;
        let trial_index = self
            .candidate_trial_high_watermarks
            .iter()
            .position(|(candidate, _)| *candidate == candidate_instance);
        if trial_index
            .is_some_and(|index| plan.trial_id <= self.candidate_trial_high_watermarks[index].1)
        {
            return Err(TcpServiceValidationError::TrialNotIncreasing);
        }
        if trial_index.is_none()
            && self.candidate_trial_high_watermarks.len() >= self.max_carrier_instances
        {
            return Err(TcpServiceValidationError::CandidateHistoryLimit);
        }
        let Some(next_lifecycle_id) = self.next_lifecycle_id.checked_add(1) else {
            return Err(TcpServiceValidationError::LifecycleOverflow);
        };
        let reservation = TcpServiceValidationReservation {
            session_id: self.session_id,
            lifecycle_id: self.next_lifecycle_id,
            trial_id: plan.trial_id,
            candidate: plan.fence.candidate,
            direction: plan.direction,
            carrier_group_id: plan.carrier_group_id,
        };
        let suppression_identity = plan.fence.suppression_identity();
        let cohort_identity = plan.fence.cohort_identity();
        let direction = plan.direction;
        let state = self.direction_state(direction);
        let group_state = state
            .carrier_groups
            .iter()
            .find(|group| group.carrier_group_id == plan.carrier_group_id);
        if group_state.is_none() && state.carrier_groups.len() >= self.max_carrier_instances {
            return Err(TcpServiceValidationError::CarrierGroupLimit);
        }
        let identity_changed = group_state
            .is_none_or(|group| group.current_suppression_identity != suppression_identity);
        let prior_no_gain_suppression = match group_state {
            Some(group) if !identity_changed => group.no_gain_suppression.clone(),
            _ => None,
        };

        if let Some(index) = trial_index {
            self.candidate_trial_high_watermarks[index].1 = reservation.trial_id;
        } else {
            self.candidate_trial_high_watermarks
                .push((candidate_instance, reservation.trial_id));
        }
        self.next_lifecycle_id = next_lifecycle_id;
        self.active = Some(TcpServiceActiveValidation::Installing {
            reservation,
            absolute_deadline: plan.absolute_deadline,
        });
        Ok(TcpServiceValidationPreparation {
            plan,
            reservation,
            suppression_identity,
            cohort_identity,
            identity_changed,
            prior_no_gain_suppression,
        })
    }

    pub(crate) fn activate(
        &mut self,
        preparation: TcpServiceValidationPreparation,
        initial_boundary: TcpServiceBoundary,
        now: Instant,
        current_fence: &TcpServiceValidationFence,
    ) -> Result<TcpServiceSenderValidation, TcpServiceActivationFailure> {
        let reservation = preparation.reservation;
        let failure = if self.active
            != Some(TcpServiceActiveValidation::Installing {
                reservation,
                absolute_deadline: preparation.plan.absolute_deadline,
            }) {
            Some(TcpServiceValidationError::ValidationNotInstalling)
        } else if current_fence != &preparation.plan.fence {
            Some(TcpServiceValidationError::FenceChanged)
        } else if now >= preparation.plan.absolute_deadline {
            Some(TcpServiceValidationError::DeadlineExpired)
        } else if initial_boundary.ack_sequence == 0
            || initial_boundary.acked_at < preparation.plan.registered_at
            || initial_boundary.acked_at > now
            || initial_boundary.writer.lifecycle != preparation.writer_lifecycle()
            || initial_boundary.writer.at() < initial_boundary.acked_at
            || initial_boundary.writer.at() > now
            || initial_boundary.writer.at() >= preparation.plan.absolute_deadline
        {
            Some(TcpServiceValidationError::InvalidBoundary)
        } else {
            None
        };
        if let Some(error) = failure {
            return Err(TcpServiceActivationFailure { preparation, error });
        }
        let validation = match TcpServiceSenderValidation::new(
            preparation.plan.clone(),
            initial_boundary,
            preparation.prior_no_gain_suppression.clone(),
            reservation,
        ) {
            Ok(validation) => validation,
            Err(error) => {
                return Err(TcpServiceActivationFailure { preparation, error });
            }
        };

        let direction = preparation.plan.direction;
        let carrier_group_id = preparation.reservation.carrier_group_id;
        let max_carrier_instances = self.max_carrier_instances;
        let state = self.direction_state_mut(direction);
        if let Some(group) = state
            .carrier_groups
            .iter_mut()
            .find(|group| group.carrier_group_id == carrier_group_id)
        {
            if preparation.identity_changed {
                group.current_suppression_identity = preparation.suppression_identity;
                group.no_gain_suppression = None;
            }
        } else {
            debug_assert!(state.carrier_groups.len() < max_carrier_instances);
            state.carrier_groups.push(TcpServiceCarrierGroupState {
                carrier_group_id,
                current_suppression_identity: preparation.suppression_identity,
                no_gain_suppression: None,
            });
        }
        if state.saturation_cohort.as_ref() != Some(&preparation.cohort_identity) {
            state.saturation_cohort = Some(preparation.cohort_identity);
            state.saturation_high_watermarks.clear();
        }
        self.active = Some(TcpServiceActiveValidation::Running {
            reservation,
            absolute_deadline: preparation.plan.absolute_deadline,
        });
        Ok(validation)
    }

    pub(crate) fn cancel(&mut self, preparation: TcpServiceValidationPreparation) -> bool {
        if self.active
            != Some(TcpServiceActiveValidation::Installing {
                reservation: preparation.reservation,
                absolute_deadline: preparation.plan.absolute_deadline,
            })
        {
            return false;
        }
        self.active = None;
        true
    }

    pub(crate) fn finish(
        &mut self,
        validation: &mut TcpServiceSenderValidation,
        now: Instant,
        current_fence: &TcpServiceValidationFence,
    ) -> Option<TcpServiceValidationOutcome> {
        if validation.lifecycle_finished
            || self.active
                != Some(TcpServiceActiveValidation::Running {
                    reservation: validation.reservation,
                    absolute_deadline: validation.absolute_deadline,
                })
        {
            return None;
        }
        let provisional_result = validation.outcome.as_ref()?.result;
        if provisional_result != TcpCarrierValidationResult::Withdrawn {
            if current_fence != &validation.fence {
                validation.replace_provisional_withdrawal(TcpServiceWithdrawalReason::FenceChanged);
            } else if now >= validation.absolute_deadline {
                validation.replace_provisional_withdrawal(TcpServiceWithdrawalReason::Deadline);
            }
        }
        let outcome = validation
            .outcome
            .clone()
            .expect("settlement checked before finish");
        if outcome.session_id != self.session_id
            || outcome.direction != validation.reservation.direction
        {
            return None;
        }
        if let Some(suppression) = outcome.no_gain_suppression.as_ref() {
            let state = self.direction_state_mut(outcome.direction);
            let Some(group) = state
                .carrier_groups
                .iter_mut()
                .find(|group| group.carrier_group_id == validation.reservation.carrier_group_id)
            else {
                return None;
            };
            if group.current_suppression_identity != suppression.identity {
                return None;
            }
            group.no_gain_suppression = Some(suppression.clone());
        }
        validation.lifecycle_finished = true;
        self.active = None;
        Some(outcome)
    }

    pub(crate) fn observe_saturation(
        &mut self,
        validation: &mut TcpServiceSenderValidation,
        observation: TcpServiceSaturationObservation,
        now: Instant,
        current_fence: &TcpServiceValidationFence,
    ) -> TcpServiceValidationUpdate {
        if self.active
            != Some(TcpServiceActiveValidation::Running {
                reservation: validation.reservation,
                absolute_deadline: validation.absolute_deadline,
            })
            || validation.lifecycle_finished
        {
            return validation.withdraw(TcpServiceWithdrawalReason::InvalidEvidence);
        }
        if let Err(reason) = validation.preflight_saturation(&observation, now, current_fence) {
            return validation.withdraw(reason);
        }
        let stream_identity = TcpServiceSuppressionStreamIdentity {
            stream_id: observation.blocked_stream.stream_id,
            demand_generation: observation.blocked_stream.demand_generation,
            attachment_incarnation: observation.blocked_stream.attachment_incarnation,
        };
        if self
            .direction_state(validation.direction)
            .saturation_cohort
            .as_ref()
            != Some(&validation.fence.cohort_identity())
        {
            return validation.withdraw(TcpServiceWithdrawalReason::FenceChanged);
        }
        let Some(next_sequence) = self.next_saturation_sequence.checked_add(1) else {
            return validation.withdraw(TcpServiceWithdrawalReason::ResourceLimit);
        };
        let existing_index = self
            .direction_state(validation.direction)
            .saturation_high_watermarks
            .iter()
            .position(|(stream, _)| *stream == stream_identity);
        if existing_index.is_none()
            && self
                .direction_state(validation.direction)
                .saturation_high_watermarks
                .len()
                >= validation.fence.streams.len()
        {
            return validation.withdraw(TcpServiceWithdrawalReason::ResourceLimit);
        }
        if existing_index.is_some_and(|index| {
            observation.blocked_range.start
                < self
                    .direction_state(validation.direction)
                    .saturation_high_watermarks[index]
                    .1
        }) {
            return validation.withdraw(TcpServiceWithdrawalReason::InvalidEvidence);
        }

        let state = self.direction_state_mut(validation.direction);
        if let Some((_, high_watermark)) = state
            .saturation_high_watermarks
            .iter_mut()
            .find(|(stream, _)| *stream == stream_identity)
        {
            *high_watermark = observation.blocked_range.end;
        } else {
            state
                .saturation_high_watermarks
                .push((stream_identity, observation.blocked_range.end));
        }
        let event = TcpServiceSaturationEvent {
            sequence: self.next_saturation_sequence,
            observed_at: observation.observed_at,
        };
        self.next_saturation_sequence = next_sequence;
        validation.record_saturation_event(event)
    }

    /// Begins cleanup only after the active lifecycle's absolute deadline.
    /// The controller remains unavailable until the caller conditionally
    /// removes the returned lifecycle's writer observers and acknowledges it.
    pub(crate) fn begin_expiry(&mut self, now: Instant) -> Option<TcpServiceExpiredLifecycle> {
        let active = self.active?;
        if let TcpServiceActiveValidation::Expiring { reservation, stage } = active {
            return Some(TcpServiceExpiredLifecycle { reservation, stage });
        }
        if active
            .absolute_deadline()
            .is_none_or(|deadline| now < deadline)
        {
            return None;
        }
        let reservation = active.reservation();
        let stage = match active {
            TcpServiceActiveValidation::Installing { .. } => TcpServiceExpiredStage::Installing,
            TcpServiceActiveValidation::Running { .. } => TcpServiceExpiredStage::Running,
            TcpServiceActiveValidation::Expiring { .. } => unreachable!("returned above"),
        };
        self.active = Some(TcpServiceActiveValidation::Expiring { reservation, stage });
        Some(TcpServiceExpiredLifecycle { reservation, stage })
    }

    pub(crate) fn complete_expiry(&mut self, expired: TcpServiceExpiredLifecycle) -> bool {
        if self.active
            != Some(TcpServiceActiveValidation::Expiring {
                reservation: expired.reservation,
                stage: expired.stage,
            })
        {
            return false;
        }
        self.active = None;
        true
    }

    pub(crate) fn active_deadline(&self) -> Option<Instant> {
        self.active
            .and_then(TcpServiceActiveValidation::absolute_deadline)
    }

    pub(crate) fn retire_candidate(&mut self, candidate: TcpServiceCarrierFence) -> bool {
        if self.active.is_some_and(|active| {
            active.reservation().candidate.local_instance_id == candidate.local_instance_id
        }) {
            return false;
        }
        let Some(index) = self
            .candidate_trial_high_watermarks
            .iter()
            .position(|(instance, _)| *instance == candidate.local_instance_id)
        else {
            return false;
        };
        self.candidate_trial_high_watermarks.swap_remove(index);
        true
    }

    pub(crate) fn has_active_validation(&self) -> bool {
        self.active.is_some()
    }

    fn direction_state(&self, direction: PathMetricDirection) -> &TcpServiceDirectionState {
        match direction {
            PathMetricDirection::ClientToServer => &self.client_to_server,
            PathMetricDirection::ServerToClient => &self.server_to_client,
        }
    }

    fn direction_state_mut(
        &mut self,
        direction: PathMetricDirection,
    ) -> &mut TcpServiceDirectionState {
        match direction {
            PathMetricDirection::ClientToServer => &mut self.client_to_server,
            PathMetricDirection::ServerToClient => &mut self.server_to_client,
        }
    }
}

#[derive(Debug, Clone)]
struct TcpServiceWindow {
    boundary: TcpServiceBoundary,
    stream_bytes: Vec<u64>,
    total_bytes: u64,
    accepted_bytes: u64,
    candidate_bytes: u64,
    latest_commit_at: Option<TcpServiceWriterPoint>,
    last_evidence_ack: Option<TcpServiceBoundary>,
    saturation: Option<(u64, TcpServiceWriterPoint)>,
}

impl TcpServiceWindow {
    fn new(boundary: TcpServiceBoundary, stream_count: usize) -> Self {
        Self {
            boundary,
            stream_bytes: vec![0; stream_count],
            total_bytes: 0,
            accepted_bytes: 0,
            candidate_bytes: 0,
            latest_commit_at: None,
            last_evidence_ack: None,
            saturation: None,
        }
    }

    fn record_saturation(&mut self, event: &TcpServiceSaturationEvent) -> bool {
        if event.observed_at.strictly_after(self.boundary.writer) {
            self.saturation = Some((event.sequence, event.observed_at));
            return true;
        }
        false
    }

    fn add_ack_evidence(
        &mut self,
        stream_index: usize,
        accepted_bytes: u64,
        candidate_bytes: u64,
        latest_commit_at: Option<TcpServiceWriterPoint>,
        ack_boundary: TcpServiceBoundary,
        max_window_bytes: u64,
    ) -> Result<(), TcpServiceWithdrawalReason> {
        let event_bytes = accepted_bytes
            .checked_add(candidate_bytes)
            .ok_or(TcpServiceWithdrawalReason::ResourceLimit)?;
        if event_bytes == 0 {
            return Ok(());
        }
        let total_bytes = self
            .total_bytes
            .checked_add(event_bytes)
            .ok_or(TcpServiceWithdrawalReason::ResourceLimit)?;
        if total_bytes > max_window_bytes {
            return Err(TcpServiceWithdrawalReason::ResourceLimit);
        }
        let stream_bytes = self.stream_bytes[stream_index]
            .checked_add(event_bytes)
            .ok_or(TcpServiceWithdrawalReason::ResourceLimit)?;
        let accepted_total = self
            .accepted_bytes
            .checked_add(accepted_bytes)
            .ok_or(TcpServiceWithdrawalReason::ResourceLimit)?;
        let candidate_total = self
            .candidate_bytes
            .checked_add(candidate_bytes)
            .ok_or(TcpServiceWithdrawalReason::ResourceLimit)?;

        self.total_bytes = total_bytes;
        self.stream_bytes[stream_index] = stream_bytes;
        self.accepted_bytes = accepted_total;
        self.candidate_bytes = candidate_total;
        if let Some(committed_at) = latest_commit_at {
            self.latest_commit_at = Some(self.latest_commit_at.map_or(committed_at, |current| {
                if committed_at.strictly_after(current) {
                    committed_at
                } else {
                    current
                }
            }));
        }
        self.last_evidence_ack = Some(ack_boundary);
        Ok(())
    }

    fn complete(
        &self,
        streams: &[TcpServiceStreamFence],
        directional_stream_horizon: u64,
        matched_horizon: u64,
        validation_horizon: u64,
        phase: TcpServiceValidationPhase,
    ) -> Option<(TcpServiceRate, TcpServiceBoundary)> {
        let (_, saturation_at) = self.saturation?;
        if self.total_bytes < matched_horizon
            || self
                .stream_bytes
                .iter()
                .zip(streams)
                .any(|(covered, stream)| *covered < stream.data_ack_horizon_bytes)
        {
            return None;
        }
        match phase {
            TcpServiceValidationPhase::PreReference { .. }
            | TcpServiceValidationPhase::PostReference { .. } => {
                if self.candidate_bytes != 0 || self.accepted_bytes != self.total_bytes {
                    return None;
                }
            }
            TcpServiceValidationPhase::Comparison { .. } => {
                if self.candidate_bytes != validation_horizon
                    || self.accepted_bytes < directional_stream_horizon
                {
                    return None;
                }
            }
            _ => return None,
        }
        let ack_boundary = self.last_evidence_ack?;
        if !ack_boundary.writer.strictly_after(saturation_at) {
            return None;
        }
        self.latest_commit_at?;
        let ack_elapsed = ack_boundary
            .acked_at
            .saturating_duration_since(self.boundary.acked_at);
        let writer_elapsed = ack_boundary.writer.duration_since(self.boundary.writer)?;
        let elapsed = ack_elapsed.max(writer_elapsed);
        if elapsed.is_zero() {
            return None;
        }
        Some((
            TcpServiceRate {
                bytes: self.total_bytes,
                elapsed,
            },
            ack_boundary,
        ))
    }
}

#[derive(Debug)]
pub(crate) struct TcpServiceSenderValidation {
    reservation: TcpServiceValidationReservation,
    lifecycle_finished: bool,
    session_id: SessionId,
    trial_id: u64,
    request_id: u64,
    direction: PathMetricDirection,
    fence: TcpServiceValidationFence,
    limits: TcpServiceValidationLimits,
    absolute_deadline: Instant,
    phase: TcpServiceValidationPhase,
    boundary: TcpServiceBoundary,
    window: Option<TcpServiceWindow>,
    directional_stream_horizon: u64,
    matched_horizon: u64,
    candidate_outstanding_bytes: u64,
    candidate_reserved_bytes: u64,
    pending_candidate_placements: Vec<TcpServicePendingCandidatePlacement>,
    committed_candidate_placements: Vec<TcpServiceCommittedCandidatePlacement>,
    candidate_placement_history: Vec<TcpServiceCandidatePlacement>,
    next_candidate_permit_id: u64,
    candidate_phase_committed_bytes: u64,
    candidate_phase_qualified_acked_bytes: u64,
    candidate_total_committed_bytes: u64,
    last_ack_sequence: u64,
    last_ack_at: Instant,
    last_writer_boundary: TcpServiceWriterPoint,
    last_saturation_sequence: u64,
    pre_reference_rates: Vec<TcpServiceRate>,
    comparison_rates: Vec<TcpServiceRate>,
    post_reference_rates: Vec<TcpServiceRate>,
    prior_no_gain_suppression: Option<TcpServiceNoGainSuppression>,
    outcome: Option<TcpServiceValidationOutcome>,
}

impl TcpServiceSenderValidation {
    fn new(
        plan: TcpServiceValidationPlan,
        initial_boundary: TcpServiceBoundary,
        prior_no_gain_suppression: Option<TcpServiceNoGainSuppression>,
        reservation: TcpServiceValidationReservation,
    ) -> Result<Self, TcpServiceValidationError> {
        validate_plan(&plan)?;
        if reservation.session_id != plan.session_id
            || reservation.trial_id != plan.trial_id
            || reservation.candidate != plan.fence.candidate
            || reservation.direction != plan.direction
            || reservation.carrier_group_id != plan.carrier_group_id
            || initial_boundary.writer.lifecycle != reservation.writer_lifecycle()
        {
            return Err(TcpServiceValidationError::InvalidCarrierFence);
        }
        let directional_stream_horizon = plan
            .fence
            .streams
            .iter()
            .try_fold(0_u64, |total, stream| {
                total.checked_add(stream.data_ack_horizon_bytes)
            })
            .ok_or(TcpServiceValidationError::HorizonOverflow)?;
        let matched_horizon = directional_stream_horizon
            .checked_add(plan.limits.validation_horizon_bytes)
            .ok_or(TcpServiceValidationError::HorizonOverflow)?;
        let phase = TcpServiceValidationPhase::PreReference {
            completed_windows: 0,
        };
        Ok(Self {
            reservation,
            lifecycle_finished: false,
            session_id: plan.session_id,
            trial_id: plan.trial_id,
            request_id: plan.fence.demand.request_id(),
            direction: plan.direction,
            window: Some(TcpServiceWindow::new(
                initial_boundary,
                plan.fence.streams.len(),
            )),
            boundary: initial_boundary,
            last_ack_sequence: initial_boundary.ack_sequence,
            last_ack_at: initial_boundary.acked_at,
            last_writer_boundary: initial_boundary.writer,
            fence: plan.fence,
            limits: plan.limits,
            absolute_deadline: plan.absolute_deadline,
            phase,
            directional_stream_horizon,
            matched_horizon,
            candidate_outstanding_bytes: 0,
            candidate_reserved_bytes: 0,
            pending_candidate_placements: Vec::new(),
            committed_candidate_placements: Vec::new(),
            candidate_placement_history: Vec::new(),
            next_candidate_permit_id: 1,
            candidate_phase_committed_bytes: 0,
            candidate_phase_qualified_acked_bytes: 0,
            candidate_total_committed_bytes: 0,
            last_saturation_sequence: 0,
            pre_reference_rates: Vec::with_capacity(2),
            comparison_rates: Vec::with_capacity(2),
            post_reference_rates: Vec::with_capacity(2),
            prior_no_gain_suppression,
            outcome: None,
        })
    }

    pub(crate) fn phase(&self) -> TcpServiceValidationPhase {
        self.phase
    }

    pub(crate) fn boundary(&self) -> TcpServiceBoundary {
        self.boundary
    }

    fn outcome(&self) -> Option<&TcpServiceValidationOutcome> {
        self.outcome.as_ref()
    }

    pub(crate) fn request_id(&self) -> u64 {
        self.request_id
    }

    fn candidate_placement_credit_bytes(&self) -> u64 {
        if !matches!(
            self.phase,
            TcpServiceValidationPhase::Readiness | TcpServiceValidationPhase::Comparison { .. }
        ) {
            return 0;
        }
        self.limits
            .validation_horizon_bytes
            .saturating_sub(
                self.candidate_phase_committed_bytes
                    .saturating_add(self.candidate_reserved_bytes),
            )
            .min(
                self.limits.unproven_flight_bytes.saturating_sub(
                    self.candidate_outstanding_bytes
                        .saturating_add(self.candidate_reserved_bytes),
                ),
            )
    }

    pub(crate) fn poll(
        &mut self,
        now: Instant,
        current_fence: &TcpServiceValidationFence,
    ) -> TcpServiceValidationUpdate {
        let previous = self.phase;
        self.guard(now, current_fence);
        self.update_since(previous)
    }

    pub(crate) fn withdraw(
        &mut self,
        reason: TcpServiceWithdrawalReason,
    ) -> TcpServiceValidationUpdate {
        let previous = self.phase;
        self.settle_withdrawn(reason);
        self.update_since(previous)
    }

    fn preflight_saturation(
        &self,
        observation: &TcpServiceSaturationObservation,
        now: Instant,
        current_fence: &TcpServiceValidationFence,
    ) -> Result<(), TcpServiceWithdrawalReason> {
        if self.outcome.is_some() || self.lifecycle_finished {
            return Err(TcpServiceWithdrawalReason::InvalidEvidence);
        }
        if current_fence != &self.fence {
            return Err(TcpServiceWithdrawalReason::FenceChanged);
        }
        if now >= self.absolute_deadline {
            return Err(TcpServiceWithdrawalReason::Deadline);
        }
        let Some(window) = self.window.as_ref() else {
            return Err(TcpServiceWithdrawalReason::InvalidEvidence);
        };
        if observation.observed_at.at() > now
            || observation.accepted_with_original_flight != self.fence.accepted
            || observation.streams_with_fresh_demand != self.fence.streams
            || !self.fence.streams.contains(&observation.blocked_stream)
            || valid_range_bytes(observation.blocked_range).is_none()
            || window.saturation.is_some()
            || !observation
                .observed_at
                .strictly_after(window.boundary.writer)
        {
            return Err(TcpServiceWithdrawalReason::InvalidEvidence);
        }
        Ok(())
    }

    fn record_saturation_event(
        &mut self,
        event: TcpServiceSaturationEvent,
    ) -> TcpServiceValidationUpdate {
        let previous = self.phase;
        debug_assert!(event.sequence > self.last_saturation_sequence);
        self.last_saturation_sequence = event.sequence;
        let window = self
            .window
            .as_mut()
            .expect("saturation was preflighted against a measured window");
        assert!(
            window.record_saturation(&event),
            "preflighted saturation must follow both window boundaries"
        );
        self.advance_completed_window();
        self.update_since(previous)
    }

    /// Reserves all model resources before the caller reserves a carrier
    /// queue. Commit remains pre-publication and cannot discover an
    /// unreserved ledger or history slot.
    pub(crate) fn reserve_candidate_placement(
        &mut self,
        placement: TcpServiceCandidatePlacement,
        now: Instant,
        current_fence: &TcpServiceValidationFence,
    ) -> TcpServiceCandidateReservationUpdate {
        if !self.guard(now, current_fence) {
            return TcpServiceCandidateReservationUpdate::Settled;
        }
        let Some(bytes) = valid_range_bytes(placement.range) else {
            self.settle_withdrawn(TcpServiceWithdrawalReason::InvalidEvidence);
            return TcpServiceCandidateReservationUpdate::Settled;
        };
        if !self.fence.streams.contains(&placement.stream) {
            self.settle_withdrawn(TcpServiceWithdrawalReason::FenceChanged);
            return TcpServiceCandidateReservationUpdate::Settled;
        }
        if self.candidate_placement_history.iter().any(|committed| {
            committed.stream == placement.stream && ranges_overlap(committed.range, placement.range)
        }) {
            self.settle_withdrawn(TcpServiceWithdrawalReason::InvalidEvidence);
            return TcpServiceCandidateReservationUpdate::Settled;
        }
        let Some(max_history_records) = self.limits.max_ack_release_records.checked_mul(3) else {
            self.settle_withdrawn(TcpServiceWithdrawalReason::ResourceLimit);
            return TcpServiceCandidateReservationUpdate::Settled;
        };
        if self.candidate_placement_history.len() >= max_history_records {
            self.settle_withdrawn(TcpServiceWithdrawalReason::ResourceLimit);
            return TcpServiceCandidateReservationUpdate::Settled;
        }
        let Some(reserved_history_records) = self
            .candidate_placement_history
            .len()
            .checked_add(self.pending_candidate_placements.len())
        else {
            self.settle_withdrawn(TcpServiceWithdrawalReason::ResourceLimit);
            return TcpServiceCandidateReservationUpdate::Settled;
        };
        let Some(reserved_ledger_records) = self
            .committed_candidate_placements
            .len()
            .checked_add(self.pending_candidate_placements.len())
        else {
            self.settle_withdrawn(TcpServiceWithdrawalReason::ResourceLimit);
            return TcpServiceCandidateReservationUpdate::Settled;
        };
        if self
            .pending_candidate_placements
            .iter()
            .any(|pending| pending.permit.placement.stream == placement.stream)
            || bytes > self.candidate_placement_credit_bytes()
            || reserved_history_records >= max_history_records
            || reserved_ledger_records >= self.limits.max_ack_release_records
        {
            return TcpServiceCandidateReservationUpdate::Unavailable;
        }
        let Some(next_permit_id) = self.next_candidate_permit_id.checked_add(1) else {
            self.settle_withdrawn(TcpServiceWithdrawalReason::ResourceLimit);
            return TcpServiceCandidateReservationUpdate::Settled;
        };
        let Some(reserved) = self.candidate_reserved_bytes.checked_add(bytes) else {
            self.settle_withdrawn(TcpServiceWithdrawalReason::ResourceLimit);
            return TcpServiceCandidateReservationUpdate::Settled;
        };
        let permit = TcpServiceCandidatePlacementPermit {
            reservation: self.reservation,
            id: self.next_candidate_permit_id,
            placement,
            reserved_at: now,
            phase: self.phase,
        };
        self.next_candidate_permit_id = next_permit_id;
        self.candidate_reserved_bytes = reserved;
        self.pending_candidate_placements
            .push(TcpServicePendingCandidatePlacement { permit, bytes });
        TcpServiceCandidateReservationUpdate::Granted(permit)
    }

    pub(crate) fn commit_candidate_placement(
        &mut self,
        permit: TcpServiceCandidatePlacementPermit,
        committed_at: TcpServiceWriterPoint,
        now: Instant,
        current_fence: &TcpServiceValidationFence,
    ) -> TcpServiceValidationUpdate {
        let previous = self.phase;
        if !self.guard(now, current_fence) {
            return self.update_since(previous);
        }
        let Some(pending_index) = self
            .pending_candidate_placements
            .iter()
            .position(|pending| pending.permit == permit)
        else {
            self.settle_withdrawn(TcpServiceWithdrawalReason::InvalidEvidence);
            return self.update_since(previous);
        };
        let pending = self.pending_candidate_placements[pending_index];
        let Some(max_history_records) = self.limits.max_ack_release_records.checked_mul(3) else {
            self.settle_withdrawn(TcpServiceWithdrawalReason::ResourceLimit);
            return self.update_since(previous);
        };
        if self.candidate_placement_history.len() >= max_history_records {
            self.settle_withdrawn(TcpServiceWithdrawalReason::ResourceLimit);
            return self.update_since(previous);
        }
        if pending.permit.phase != self.phase
            || committed_at.at() < pending.permit.reserved_at
            || committed_at.at() > now
            || !committed_at.strictly_after(self.last_writer_boundary)
            || !committed_at.strictly_after(self.boundary.writer)
            || self.committed_candidate_placements.len() >= self.limits.max_ack_release_records
        {
            self.settle_withdrawn(TcpServiceWithdrawalReason::InvalidEvidence);
            return self.update_since(previous);
        }
        let bytes = pending.bytes;
        let Some(phase_committed) = self.candidate_phase_committed_bytes.checked_add(bytes) else {
            self.settle_withdrawn(TcpServiceWithdrawalReason::ResourceLimit);
            return self.update_since(previous);
        };
        let Some(outstanding) = self.candidate_outstanding_bytes.checked_add(bytes) else {
            self.settle_withdrawn(TcpServiceWithdrawalReason::ResourceLimit);
            return self.update_since(previous);
        };
        let Some(total_committed) = self.candidate_total_committed_bytes.checked_add(bytes) else {
            self.settle_withdrawn(TcpServiceWithdrawalReason::ResourceLimit);
            return self.update_since(previous);
        };
        let Some(total_limit) = self.limits.validation_horizon_bytes.checked_mul(3) else {
            self.settle_withdrawn(TcpServiceWithdrawalReason::ResourceLimit);
            return self.update_since(previous);
        };
        if phase_committed > self.limits.validation_horizon_bytes
            || outstanding > self.limits.unproven_flight_bytes
            || total_committed > total_limit
        {
            self.settle_withdrawn(TcpServiceWithdrawalReason::ResourceLimit);
            return self.update_since(previous);
        }
        self.pending_candidate_placements.swap_remove(pending_index);
        self.candidate_reserved_bytes = self
            .candidate_reserved_bytes
            .checked_sub(bytes)
            .expect("pending placement bytes are reserved");
        self.committed_candidate_placements
            .push(TcpServiceCommittedCandidatePlacement {
                placement: pending.permit.placement,
                committed_at,
            });
        self.candidate_placement_history
            .push(pending.permit.placement);
        self.candidate_phase_committed_bytes = phase_committed;
        self.candidate_outstanding_bytes = outstanding;
        self.candidate_total_committed_bytes = total_committed;
        self.update_since(previous)
    }

    pub(crate) fn cancel_candidate_placement(
        &mut self,
        permit: TcpServiceCandidatePlacementPermit,
    ) -> bool {
        let Some(index) = self
            .pending_candidate_placements
            .iter()
            .position(|pending| pending.permit == permit)
        else {
            return false;
        };
        let pending = self.pending_candidate_placements.swap_remove(index);
        self.candidate_reserved_bytes = self
            .candidate_reserved_bytes
            .checked_sub(pending.bytes)
            .expect("pending placement bytes are reserved");
        true
    }

    pub(crate) fn observe_data_ack(
        &mut self,
        event: TcpServiceDataAckEvent,
        now: Instant,
        current_fence: &TcpServiceValidationFence,
    ) -> TcpServiceValidationUpdate {
        let previous = self.phase;
        if !self.guard(now, current_fence) {
            return self.update_since(previous);
        }
        let Some(stream_index) = self
            .fence
            .streams
            .iter()
            .position(|stream| *stream == event.stream)
        else {
            self.settle_withdrawn(TcpServiceWithdrawalReason::FenceChanged);
            return self.update_since(previous);
        };
        if event.next_writer_boundary.at() > now
            || event.sequence <= self.last_ack_sequence
            || event.acked_at < self.last_ack_at
            || event.next_writer_boundary.at() < event.acked_at
            || !event
                .next_writer_boundary
                .strictly_after(self.last_writer_boundary)
            || event.releases.len() > self.limits.max_ack_release_records
        {
            self.settle_withdrawn(TcpServiceWithdrawalReason::InvalidEvidence);
            return self.update_since(previous);
        }
        let mut candidate_original_released = 0_u64;
        let mut accepted_evidence = 0_u64;
        let mut candidate_evidence = 0_u64;
        let mut latest_commit_at = None;
        let mut crosses_writer_boundary = false;
        let Some(candidate_ledger_limit) = self
            .limits
            .max_ack_release_records
            .checked_sub(self.pending_candidate_placements.len())
        else {
            self.settle_withdrawn(TcpServiceWithdrawalReason::ResourceLimit);
            return self.update_since(previous);
        };
        let mut committed_candidate_placements = self.committed_candidate_placements.clone();
        for release in &event.releases {
            let Some(bytes) = valid_range_bytes(release.range) else {
                self.settle_withdrawn(TcpServiceWithdrawalReason::InvalidEvidence);
                return self.update_since(previous);
            };
            if release.range.end > event.assigned_end
                || release
                    .committed_at
                    .is_some_and(|committed_at| committed_at.at() > event.acked_at)
            {
                self.settle_withdrawn(TcpServiceWithdrawalReason::InvalidEvidence);
                return self.update_since(previous);
            }
            let is_candidate = release.carrier == self.fence.candidate;
            let is_accepted = self.fence.accepted.contains(&release.carrier);
            if is_candidate {
                if release.kind != TcpServiceReleaseKind::Original || !release.unambiguous {
                    self.settle_withdrawn(TcpServiceWithdrawalReason::InvalidEvidence);
                    return self.update_since(previous);
                }
                let exact_released = match consume_candidate_release(
                    &mut committed_candidate_placements,
                    event.stream,
                    *release,
                    candidate_ledger_limit,
                ) {
                    Ok(bytes) => bytes,
                    Err(reason) => {
                        self.settle_withdrawn(reason);
                        return self.update_since(previous);
                    }
                };
                let Some(released) = candidate_original_released.checked_add(exact_released) else {
                    self.settle_withdrawn(TcpServiceWithdrawalReason::ResourceLimit);
                    return self.update_since(previous);
                };
                candidate_original_released = released;
            }
            if release.kind != TcpServiceReleaseKind::Original || !release.unambiguous {
                continue;
            }
            let Some(committed_at) = release
                .committed_at
                .filter(|committed_at| committed_at.strictly_after(self.boundary.writer))
            else {
                if is_candidate || is_accepted {
                    crosses_writer_boundary = true;
                }
                continue;
            };
            if is_candidate {
                let Some(total) = candidate_evidence.checked_add(bytes) else {
                    self.settle_withdrawn(TcpServiceWithdrawalReason::ResourceLimit);
                    return self.update_since(previous);
                };
                candidate_evidence = total;
            } else if is_accepted {
                let Some(total) = accepted_evidence.checked_add(bytes) else {
                    self.settle_withdrawn(TcpServiceWithdrawalReason::ResourceLimit);
                    return self.update_since(previous);
                };
                accepted_evidence = total;
            } else {
                continue;
            }
            latest_commit_at = Some(latest_commit_at.map_or(committed_at, |current| {
                if committed_at.strictly_after(current) {
                    committed_at
                } else {
                    current
                }
            }));
        }
        // A complete Data ACK transaction is one indivisible evidence event.
        // If its otherwise-qualified original releases cross the writer
        // boundary, none of that event can be truncated into this window.
        if crosses_writer_boundary {
            accepted_evidence = 0;
            candidate_evidence = 0;
            latest_commit_at = None;
        }

        if candidate_original_released != candidate_evidence
            || candidate_original_released > self.candidate_outstanding_bytes
            || (matches!(
                self.phase,
                TcpServiceValidationPhase::PreReference { .. }
                    | TcpServiceValidationPhase::PostReference { .. }
            ) && candidate_original_released != 0)
        {
            self.settle_withdrawn(TcpServiceWithdrawalReason::InvalidEvidence);
            return self.update_since(previous);
        }
        let Some(candidate_outstanding) = self
            .candidate_outstanding_bytes
            .checked_sub(candidate_original_released)
        else {
            self.settle_withdrawn(TcpServiceWithdrawalReason::InvalidEvidence);
            return self.update_since(previous);
        };
        let Some(candidate_qualified_acked) = self
            .candidate_phase_qualified_acked_bytes
            .checked_add(candidate_evidence)
        else {
            self.settle_withdrawn(TcpServiceWithdrawalReason::ResourceLimit);
            return self.update_since(previous);
        };
        if candidate_qualified_acked > self.candidate_phase_committed_bytes {
            self.settle_withdrawn(TcpServiceWithdrawalReason::InvalidEvidence);
            return self.update_since(previous);
        }

        let ack_boundary = TcpServiceBoundary {
            ack_sequence: event.sequence,
            acked_at: event.acked_at,
            writer: event.next_writer_boundary,
        };
        if let Some(window) = self.window.as_mut()
            && let Err(reason) = window.add_ack_evidence(
                stream_index,
                accepted_evidence,
                candidate_evidence,
                latest_commit_at,
                ack_boundary,
                self.limits.max_window_bytes,
            )
        {
            self.settle_withdrawn(reason);
            return self.update_since(previous);
        }

        self.last_ack_sequence = event.sequence;
        self.last_ack_at = event.acked_at;
        self.last_writer_boundary = event.next_writer_boundary;
        self.committed_candidate_placements = committed_candidate_placements;
        self.candidate_outstanding_bytes = candidate_outstanding;
        if matches!(
            self.phase,
            TcpServiceValidationPhase::Readiness | TcpServiceValidationPhase::Comparison { .. }
        ) {
            self.candidate_phase_qualified_acked_bytes = candidate_qualified_acked;
        } else if candidate_evidence != 0 {
            self.settle_withdrawn(TcpServiceWithdrawalReason::InvalidEvidence);
            return self.update_since(previous);
        }

        if self.phase == TcpServiceValidationPhase::Readiness
            && self.candidate_phase_committed_bytes == self.limits.validation_horizon_bytes
            && self.candidate_phase_qualified_acked_bytes == self.limits.validation_horizon_bytes
            && self.candidate_outstanding_bytes == 0
        {
            self.begin_comparison(ack_boundary);
        } else {
            self.advance_completed_window();
        }
        self.update_since(previous)
    }

    fn guard(&mut self, now: Instant, current_fence: &TcpServiceValidationFence) -> bool {
        if self.outcome.is_some() {
            return false;
        }
        if current_fence != &self.fence {
            self.settle_withdrawn(TcpServiceWithdrawalReason::FenceChanged);
            return false;
        }
        if now >= self.absolute_deadline {
            self.settle_withdrawn(TcpServiceWithdrawalReason::Deadline);
            return false;
        }
        true
    }

    fn advance_completed_window(&mut self) {
        let Some((rate, next_boundary)) = self.window.as_ref().and_then(|window| {
            window.complete(
                &self.fence.streams,
                self.directional_stream_horizon,
                self.matched_horizon,
                self.limits.validation_horizon_bytes,
                self.phase,
            )
        }) else {
            return;
        };
        match self.phase {
            TcpServiceValidationPhase::PreReference { .. } => {
                self.pre_reference_rates.push(rate);
                if self.pre_reference_rates.len() == 2 {
                    let fresh_reference_range =
                        reference_range(self.pre_reference_rates.iter().copied())
                            .expect("two completed pre-reference windows");
                    let identity = self.fence.suppression_identity();
                    if self
                        .prior_no_gain_suppression
                        .as_ref()
                        .is_some_and(|suppression| {
                            !suppression.permits(&identity, fresh_reference_range)
                        })
                    {
                        self.settle_withdrawn(TcpServiceWithdrawalReason::NoGainSuppressed);
                        return;
                    }
                    self.phase = TcpServiceValidationPhase::Readiness;
                    self.boundary = next_boundary;
                    self.window = None;
                    self.reset_candidate_phase();
                } else {
                    self.phase = TcpServiceValidationPhase::PreReference {
                        completed_windows: self.pre_reference_rates.len() as u8,
                    };
                    self.begin_window(next_boundary);
                }
            }
            TcpServiceValidationPhase::Comparison { .. } => {
                if self.candidate_phase_committed_bytes != self.limits.validation_horizon_bytes
                    || self.candidate_phase_qualified_acked_bytes
                        != self.limits.validation_horizon_bytes
                    || self.candidate_outstanding_bytes != 0
                {
                    return;
                }
                self.comparison_rates.push(rate);
                if self.comparison_rates.len() == 2 {
                    self.phase = TcpServiceValidationPhase::PostReference {
                        completed_windows: 0,
                    };
                    self.reset_candidate_phase();
                    self.begin_window(next_boundary);
                } else {
                    self.phase = TcpServiceValidationPhase::Comparison {
                        completed_windows: self.comparison_rates.len() as u8,
                    };
                    self.reset_candidate_phase();
                    self.begin_window(next_boundary);
                }
            }
            TcpServiceValidationPhase::PostReference { .. } => {
                self.post_reference_rates.push(rate);
                if self.post_reference_rates.len() == 2 {
                    self.settle_capacity_result();
                } else {
                    self.phase = TcpServiceValidationPhase::PostReference {
                        completed_windows: self.post_reference_rates.len() as u8,
                    };
                    self.begin_window(next_boundary);
                }
            }
            _ => {}
        }
    }

    fn begin_comparison(&mut self, boundary: TcpServiceBoundary) {
        self.phase = TcpServiceValidationPhase::Comparison {
            completed_windows: 0,
        };
        self.reset_candidate_phase();
        self.begin_window(boundary);
    }

    fn begin_window(&mut self, boundary: TcpServiceBoundary) {
        self.boundary = boundary;
        self.window = Some(TcpServiceWindow::new(boundary, self.fence.streams.len()));
    }

    fn reset_candidate_phase(&mut self) {
        self.candidate_phase_committed_bytes = 0;
        self.candidate_phase_qualified_acked_bytes = 0;
    }

    fn settle_capacity_result(&mut self) {
        let retain = self.comparison_rates.iter().all(|comparison| {
            self.pre_reference_rates
                .iter()
                .chain(&self.post_reference_rates)
                .all(|reference| comparison.cmp_exact(*reference) == Ordering::Greater)
        });
        let result = if retain {
            TcpCarrierValidationResult::Retain
        } else {
            TcpCarrierValidationResult::NoGain
        };
        let no_gain_suppression = (!retain).then(|| TcpServiceNoGainSuppression {
            identity: self.fence.suppression_identity(),
            rejected_reference_range: reference_range(
                self.pre_reference_rates
                    .iter()
                    .chain(&self.post_reference_rates)
                    .copied(),
            )
            .expect("four completed reference windows"),
        });
        self.release_candidate_work();
        self.outcome = Some(TcpServiceValidationOutcome {
            session_id: self.session_id,
            trial_id: self.trial_id,
            candidate: self.fence.candidate,
            direction: self.direction,
            result,
            withdrawal_reason: None,
            no_gain_suppression,
        });
    }

    fn settle_withdrawn(&mut self, reason: TcpServiceWithdrawalReason) {
        if self.outcome.is_some() {
            return;
        }
        self.release_candidate_work();
        self.outcome = Some(TcpServiceValidationOutcome {
            session_id: self.session_id,
            trial_id: self.trial_id,
            candidate: self.fence.candidate,
            direction: self.direction,
            result: TcpCarrierValidationResult::Withdrawn,
            withdrawal_reason: Some(reason),
            no_gain_suppression: None,
        });
    }

    fn replace_provisional_withdrawal(&mut self, reason: TcpServiceWithdrawalReason) {
        debug_assert!(!self.lifecycle_finished);
        self.release_candidate_work();
        self.outcome = Some(TcpServiceValidationOutcome {
            session_id: self.session_id,
            trial_id: self.trial_id,
            candidate: self.fence.candidate,
            direction: self.direction,
            result: TcpCarrierValidationResult::Withdrawn,
            withdrawal_reason: Some(reason),
            no_gain_suppression: None,
        });
    }

    fn release_candidate_work(&mut self) {
        self.phase = TcpServiceValidationPhase::Settled;
        self.window = None;
        self.pending_candidate_placements = Vec::new();
        self.committed_candidate_placements = Vec::new();
        self.candidate_placement_history = Vec::new();
        self.candidate_reserved_bytes = 0;
    }

    fn update_since(&self, previous: TcpServiceValidationPhase) -> TcpServiceValidationUpdate {
        if self.outcome.is_some() {
            TcpServiceValidationUpdate::Settled
        } else if self.phase != previous {
            TcpServiceValidationUpdate::PhaseChanged(self.phase)
        } else {
            TcpServiceValidationUpdate::Pending
        }
    }
}

fn consume_candidate_release(
    placements: &mut Vec<TcpServiceCommittedCandidatePlacement>,
    stream: TcpServiceStreamFence,
    release: TcpServiceAckRelease,
    max_records: usize,
) -> Result<u64, TcpServiceWithdrawalReason> {
    let Some(index) = placements.iter().position(|committed| {
        committed.placement.stream == stream
            && Some(committed.committed_at) == release.committed_at
            && committed.placement.range.start <= release.range.start
            && release.range.end <= committed.placement.range.end
    }) else {
        return Err(TcpServiceWithdrawalReason::InvalidEvidence);
    };
    let committed = placements[index];
    let retained_fragments = usize::from(committed.placement.range.start < release.range.start)
        + usize::from(release.range.end < committed.placement.range.end);
    if placements
        .len()
        .saturating_sub(1)
        .saturating_add(retained_fragments)
        > max_records
    {
        return Err(TcpServiceWithdrawalReason::ResourceLimit);
    }
    placements.swap_remove(index);
    if committed.placement.range.start < release.range.start {
        placements.push(TcpServiceCommittedCandidatePlacement {
            placement: TcpServiceCandidatePlacement {
                stream,
                range: OffsetRange {
                    start: committed.placement.range.start,
                    end: release.range.start,
                },
            },
            committed_at: committed.committed_at,
        });
    }
    if release.range.end < committed.placement.range.end {
        placements.push(TcpServiceCommittedCandidatePlacement {
            placement: TcpServiceCandidatePlacement {
                stream,
                range: OffsetRange {
                    start: release.range.end,
                    end: committed.placement.range.end,
                },
            },
            committed_at: committed.committed_at,
        });
    }
    valid_range_bytes(release.range).ok_or(TcpServiceWithdrawalReason::InvalidEvidence)
}

fn reference_range(
    rates: impl IntoIterator<Item = TcpServiceRate>,
) -> Option<TcpServiceReferenceRange> {
    let mut rates = rates.into_iter();
    let first = rates.next()?;
    let mut lowest = first;
    let mut highest = first;
    for rate in rates {
        if rate.cmp_exact(lowest) == Ordering::Less {
            lowest = rate;
        }
        if rate.cmp_exact(highest) == Ordering::Greater {
            highest = rate;
        }
    }
    TcpServiceReferenceRange::new(lowest, highest)
}

fn valid_range_bytes(range: OffsetRange) -> Option<u64> {
    range
        .end
        .checked_sub(range.start)
        .filter(|bytes| *bytes > 0)
}

fn ranges_overlap(left: OffsetRange, right: OffsetRange) -> bool {
    left.start < right.end && right.start < left.end
}

fn validate_plan(plan: &TcpServiceValidationPlan) -> Result<(), TcpServiceValidationError> {
    if plan.trial_id == 0 {
        return Err(TcpServiceValidationError::ZeroIdentifier);
    }
    match (plan.direction, plan.fence.demand) {
        (PathMetricDirection::ClientToServer, TcpServiceDemandFence::Local) => {}
        (
            PathMetricDirection::ServerToClient,
            TcpServiceDemandFence::PeerRequest { request_id, anchor },
        ) if request_id != 0 && plan.fence.accepted.contains(&anchor) => {}
        _ => return Err(TcpServiceValidationError::DirectionRequestMismatch),
    }
    if plan.absolute_deadline <= plan.registered_at {
        return Err(TcpServiceValidationError::InvalidDeadline);
    }
    validate_limits(plan.limits)?;
    validate_fence(&plan.fence, plan.limits)?;
    let stream_horizon = plan
        .fence
        .streams
        .iter()
        .try_fold(0_u64, |total, stream| {
            total.checked_add(stream.data_ack_horizon_bytes)
        })
        .ok_or(TcpServiceValidationError::HorizonOverflow)?;
    let matched_horizon = stream_horizon
        .checked_add(plan.limits.validation_horizon_bytes)
        .ok_or(TcpServiceValidationError::HorizonOverflow)?;
    if matched_horizon > plan.limits.max_window_bytes
        || plan
            .limits
            .validation_horizon_bytes
            .checked_mul(3)
            .is_none()
    {
        return Err(TcpServiceValidationError::HorizonOverflow);
    }
    Ok(())
}

fn validate_limits(limits: TcpServiceValidationLimits) -> Result<(), TcpServiceValidationError> {
    if limits.max_paths == 0
        || limits.max_streams == 0
        || limits.max_ack_release_records == 0
        || limits.max_window_bytes == 0
        || limits.validation_horizon_bytes == 0
        || limits.unproven_flight_bytes == 0
        || limits.data_ack_sample_floor_bytes == 0
        || limits.validation_horizon_bytes < limits.data_ack_sample_floor_bytes
        || limits.unproven_flight_bytes < limits.data_ack_sample_floor_bytes
        || limits.max_ack_release_records.checked_mul(3).is_none()
    {
        return Err(TcpServiceValidationError::InvalidLimits);
    }
    Ok(())
}

fn validate_fence(
    fence: &TcpServiceValidationFence,
    limits: TcpServiceValidationLimits,
) -> Result<(), TcpServiceValidationError> {
    if fence.range_generation == 0 {
        return Err(TcpServiceValidationError::InvalidCarrierFence);
    }
    if fence.accepted.is_empty() || fence.accepted.len().saturating_add(1) > limits.max_paths {
        return Err(TcpServiceValidationError::TooManyPaths);
    }
    if fence.streams.is_empty() || fence.streams.len() > limits.max_streams {
        return Err(TcpServiceValidationError::TooManyStreams);
    }
    if !valid_carrier_fence(fence.candidate)
        || fence
            .accepted
            .iter()
            .any(|carrier| !valid_carrier_fence(*carrier))
    {
        return Err(TcpServiceValidationError::InvalidCarrierFence);
    }
    if !fence
        .accepted
        .windows(2)
        .all(|pair| pair[0].accepted < pair[1].accepted)
    {
        return Err(TcpServiceValidationError::NonCanonicalCarriers);
    }
    for (index, carrier) in fence.accepted.iter().enumerate() {
        if fence.accepted[index + 1..].iter().any(|other| {
            carrier.accepted.path_id == other.accepted.path_id
                || carrier.local_instance_id == other.local_instance_id
        }) {
            return Err(TcpServiceValidationError::NonCanonicalCarriers);
        }
    }
    if fence.accepted.iter().any(|carrier| {
        carrier.accepted.path_id == fence.candidate.accepted.path_id
            || carrier.local_instance_id == fence.candidate.local_instance_id
    }) {
        return Err(TcpServiceValidationError::CandidateInAcceptedSet);
    }
    if !fence
        .streams
        .windows(2)
        .all(|pair| pair[0].stream_id < pair[1].stream_id)
    {
        return Err(TcpServiceValidationError::NonCanonicalStreams);
    }
    if fence.streams.iter().any(|stream| {
        stream.demand_generation == 0
            || stream.attachment_incarnation == 0
            || stream.data_ack_horizon_bytes == 0
    }) {
        return Err(TcpServiceValidationError::InvalidStreamFence);
    }
    Ok(())
}

fn valid_carrier_fence(carrier: TcpServiceCarrierFence) -> bool {
    carrier.local_instance_id.as_u64() != 0 && carrier.eligibility_generation != 0
}

#[cfg(test)]
#[path = "tcp_service_test.rs"]
mod tests;
