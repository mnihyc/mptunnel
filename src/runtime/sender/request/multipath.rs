//! Serialized request multipath lifecycle.
//!
//! This owner combines carrier-neutral product offsets and evidence with TCP's
//! fallback capacity controller. QUIC uses validated paths under native
//! congestion control and writer backpressure.

use super::super::queue::ReliableRelaySenderQueue;
use super::super::work::{ClientReinjectionOutputIdentity, RelaySendCause};
use super::scheduling::{
    BulkRelayFrameRequest, BulkRelayPathChoice, ObservedBulkPathCandidate,
    ObservedOrdinaryPathChoice, RequestRelayPathObservation, RequestRelaySchedulingObservation,
    RequestSchedulingState, choose_observed_ordinary_data_path,
    choose_ordinary_bulk_relay_path_avoiding, request_ack_clock_measurement_for_ordinary_target,
    request_original_data_authority_snapshot,
};
use super::tcp_capacity::{
    RequestTcpCapacityController, RequestTcpCapacityEvent, RequestTcpCapacityRetirement,
};
use super::{
    RequestCompletionTailTarget, RequestCompletionTailTargetObservation,
    RequestDataAckGapObservation,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::admission::{
    BulkCandidatePosition, BulkOriginalDataAssignmentAuthority, ReliableDataAckFrontierState,
    bulk_original_data_assignment_authority,
};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, PathRateSample, ReliableOriginalDataOutput,
    ReliableStreamSourceAdmission, product_delivery_samples_override_startup_prior,
    reliable_bulk_product_windows, reliable_path_startup_sample_limit_bytes,
    reliable_relay_buffer_len, reliable_stream_source_admission,
};
use crate::model::carrier_rate_authority::{CarrierRateAuthorityScope, CarrierRateAuthorityStamp};
use crate::model::path::{RelayPathInstance, RelayPathKey, RelayPathProofEpoch};
use crate::model::product_qualification::{
    ProductQualificationAdmissionError, ProductQualificationAuthority,
};
use crate::model::requalification::{StreamPathRequalification, StreamRequalificationProbe};
use crate::model::request_evidence::{
    RequestOwnerAckProgress, RequestPathRateEvidenceUpdate, RequestProductRateEpoch,
    request_path_rate_coverage_floor_bytes,
};
use crate::model::timing::{
    reliable_data_retransmission_interval, reliable_path_stale_interval,
    transport_rate_sample_freshness_horizon,
};
use crate::model::work::{
    RangeRecoveryState, ReliableLiveOwnerFrontier, ReliableReinjectionTargetWork,
    reliable_reinjection_service_limit_bytes,
};
use crate::mux::MuxLimits;
use crate::mux::stream::ReliableSendStream;
use crate::protocol::frame::{reliable_stream_frame_accounted_bytes, reliable_stream_frame_extent};
use crate::protocol::{Frame, OffsetRange, PathMetricDirection, StreamId, UnderlayProtocol};
use crate::runtime::path::authority::NativeCarrierSchedulingShapeSnapshot;
use crate::runtime::path::commands::ReliablePathCommandSender;
use crate::runtime::path::{ClientPathContext, ReliableRequestNativeShape};
use crate::runtime::stream::request::{
    RequestAckClockOperation, RequestFlightLedger, RequestPathRelease, RequestStreamState,
};
use crate::runtime::stream::{
    ReliablePathStreamOutput, ReliableRelayRemotePath, ReliableRelayRemoteSet,
    RequalificationAttempt, TargetCarrierCapacityWait,
};
use crate::scheduler::{self, PathSnapshot, TrafficClass, cyclic_cursor_distance};
use smallvec::SmallVec;
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

fn request_native_scheduling_shape(path: &ReliableRelayRemotePath) -> ReliableRequestNativeShape {
    if path.key().underlay != UnderlayProtocol::Udp {
        return ReliableRequestNativeShape::NotApplicable;
    }
    let ReliablePathStreamOutput::Fixed(output) = &path.stream.output else {
        return ReliableRequestNativeShape::Unavailable;
    };
    let scope =
        CarrierRateAuthorityScope::new(path.path_instance_id, PathMetricDirection::ClientToServer);
    let Some(authority) = output.commands().native_rate_authority() else {
        return ReliableRequestNativeShape::Unavailable;
    };
    authority
        .scheduling_shape_snapshot(scope)
        .map(ReliableRequestNativeShape::Current)
        .unwrap_or(ReliableRequestNativeShape::Unavailable)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn observe_request_relay_scheduling(
    context: &ClientPathContext,
    stream_id: StreamId,
    membership_generation: u64,
    remote_paths: &[ReliableRelayRemotePath],
    frame: Option<&Frame>,
    lane: TrafficClass,
    payload_bytes: usize,
    include_bulk_admission: bool,
    requalification: &StreamPathRequalification<RelayPathInstance>,
) -> RequestRelaySchedulingObservation {
    observe_request_relay_scheduling_with_native_override(
        context,
        stream_id,
        membership_generation,
        remote_paths,
        frame,
        lane,
        payload_bytes,
        include_bulk_admission,
        requalification,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn observe_request_relay_scheduling_with_native_override(
    context: &ClientPathContext,
    stream_id: StreamId,
    membership_generation: u64,
    remote_paths: &[ReliableRelayRemotePath],
    frame: Option<&Frame>,
    lane: TrafficClass,
    payload_bytes: usize,
    include_bulk_admission: bool,
    requalification: &StreamPathRequalification<RelayPathInstance>,
    native_override: Option<(RelayPathInstance, NativeCarrierSchedulingShapeSnapshot)>,
) -> RequestRelaySchedulingObservation {
    // Resolve attachment-owned Native shapes before entering the health-lock
    // observation. Native publication/apply uses Native -> health ordering;
    // leaving this iterator lazy would invert that order as health -> Native.
    let attached_paths = remote_paths
        .iter()
        .map(|path| {
            let instance = path.instance();
            let shape = match native_override {
                Some((target, shape)) if target == instance => {
                    ReliableRequestNativeShape::Current(shape)
                }
                // The override is evaluated while the target Native fence is
                // held. Do not acquire another carrier authority lock here;
                // other candidates remain advisory health observations.
                Some(_) => ReliableRequestNativeShape::NotApplicable,
                None => request_native_scheduling_shape(path),
            };
            (
                instance,
                path.path_proof_id.map(|proof_id| RelayPathProofEpoch {
                    proof_id,
                    proof_generation: path.path_proof_generation,
                    attached_at: path.attached_at,
                }),
                shape,
            )
        })
        .collect::<SmallVec<[_; 4]>>();
    let path_evidence = context.observe_reliable_request_paths(
        attached_paths,
        payload_bytes,
        include_bulk_admission,
    );
    let has_nonstale_product_output =
        remote_paths
            .iter()
            .zip(path_evidence.paths.iter())
            .any(|(path, evidence)| {
                path.stream.product_admission_active()
                    && !requalification.stale_for_original_data(path.instance())
                    && evidence.shared_snapshot.is_some_and(|snapshot| {
                        scheduler::score_path(snapshot, lane, payload_bytes).is_some()
                    })
            });
    let paths = remote_paths
        .iter()
        .zip(path_evidence.paths)
        .map(|(path, evidence)| {
            let instance = path.instance();
            debug_assert_eq!(instance, evidence.instance);
            let exact_instance_live = evidence.shared_snapshot.is_some();
            let original_data_eligible =
                !requalification.stale_for_original_data(instance) || !has_nonstale_product_output;
            RequestRelayPathObservation {
                instance,
                can_enqueue_frame: exact_instance_live
                    && original_data_eligible
                    && frame
                        .map(|frame| path.stream.can_enqueue_frame_now(frame, lane))
                        .unwrap_or(true),
                can_enqueue_stream_lane: exact_instance_live
                    && original_data_eligible
                    && frame
                        .map(|frame| path.stream.can_enqueue_frame_now(frame, path.stream.lane))
                        .unwrap_or(true),
                carrier_pending_bytes: path.stream.carrier_pending_bytes(),
                load_owned: path.has_load_reservation(),
                shared_snapshot: evidence.shared_snapshot,
                startup_snapshot: evidence.startup_snapshot,
                tcp: evidence.tcp,
                has_bulk_model_evidence: evidence.has_bulk_model_evidence,
                has_fresh_native_carrier_rate_evidence: evidence
                    .has_fresh_native_carrier_rate_evidence,
                fresh_proof: evidence.fresh_proof,
                native_authority_stamp: evidence.native_authority_stamp,
                native_authority_unavailable: evidence.native_authority_unavailable,
                config_ordinal: evidence.config_ordinal,
                member_ordinal: evidence.member_ordinal,
            }
        })
        .collect();
    RequestRelaySchedulingObservation {
        stream_id,
        membership_generation,
        mux_limits: context.mux_limits,
        paths,
        global_bulk_candidates: path_evidence
            .bulk_candidates
            .into_iter()
            .map(|candidate| ObservedBulkPathCandidate {
                candidate,
                config_ordinal: context.relay_path_config_ordinal(candidate.key),
                member_ordinal: context.relay_path_member_ordinal(candidate.key),
            })
            .collect(),
        latency_pressure: path_evidence.latency_pressure,
        observed_at: Instant::now(),
    }
}

fn relay_path_can_enqueue_frame_for_cause_now(
    path: &ReliableRelayRemotePath,
    frame: &Frame,
    cause: RelaySendCause,
) -> bool {
    if matches!(cause, RelaySendCause::StreamFin) {
        path.stream.output.can_enqueue_lane_now(path.stream.lane)
    } else if cause.is_reinjection() {
        path.stream.can_enqueue_reinjection_frame_now(frame)
    } else {
        path.stream.can_enqueue_frame_now(frame, path.stream.lane)
    }
}

fn observed_request_load_expectation(
    observation: &RequestRelaySchedulingObservation,
    instance: RelayPathInstance,
) -> Result<Option<(RelayPathKey, u32, u32)>, RequestMultipathPlanError> {
    let path = observation
        .path_by_instance(instance)
        .ok_or(RequestMultipathPlanError::ServiceBlocked)?;
    if path.load_owned {
        return Ok(None);
    }
    let snapshot = path
        .shared_snapshot
        .ok_or(RequestMultipathPlanError::ServiceBlocked)?;
    Ok(Some((
        instance.key,
        snapshot.active_flows,
        snapshot.active_latency_sensitive_flows,
    )))
}

/// Why one serialized request decision produced no carrier intent.
///
/// `OutputUnavailable` is a definitive Product/recovery result. In contrast,
/// `OrderedTerminalPending` means the exact attachment is still registered but
/// has frozen fresh command admission; its input forwarder remains the sole
/// authority for publishing the ordered terminal and triggering removal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RequestMultipathPlanError {
    ServiceBlocked,
    OrderedTerminalPending,
    OutputUnavailable,
}

fn blocked_attachment_set_error(remotes: &ReliableRelayRemoteSet) -> RequestMultipathPlanError {
    if remotes
        .paths
        .iter()
        .any(|path| path.stream.product_admission_active())
    {
        RequestMultipathPlanError::ServiceBlocked
    } else {
        debug_assert!(!remotes.paths.is_empty());
        RequestMultipathPlanError::OrderedTerminalPending
    }
}

fn require_active_exact_attachment(
    remotes: &ReliableRelayRemoteSet,
    required: RelayPathInstance,
) -> Result<(), RequestMultipathPlanError> {
    let Some(path) = remotes
        .paths
        .iter()
        .find(|path| path.instance() == required)
    else {
        return Err(RequestMultipathPlanError::OutputUnavailable);
    };
    if !path.stream.product_admission_active() {
        return Err(RequestMultipathPlanError::OrderedTerminalPending);
    }
    Ok(())
}

/// Health and directional usage determine the current RFC regular/backup set.
/// Active and Suspect deliberately share one schedulable class; Failed,
/// Draining, manual disable, missing readiness usage, and lane policy do not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestPathEligibility {
    Unavailable,
    Regular,
    Backup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RequestPathEligibilityExpectation {
    instance: RelayPathInstance,
    eligibility: RequestPathEligibility,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RequestOriginalDataApplyAuthority {
    position: BulkCandidatePosition,
    stream_outstanding_bytes: u64,
    stream_limit_bytes: u64,
    output_outstanding_bytes: u64,
    output: BulkOriginalDataAssignmentAuthority,
}

impl RequestOriginalDataApplyAuthority {
    pub(super) fn has_headroom(self) -> bool {
        self.stream_outstanding_bytes
            .checked_add(self.output.assignment_payload_bytes)
            .is_some_and(|committed| committed <= self.stream_limit_bytes)
            && self.output.has_headroom(self.output_outstanding_bytes)
    }
}

fn request_path_eligibility(
    snapshot: Option<PathSnapshot>,
    lane: TrafficClass,
) -> RequestPathEligibility {
    let Some(snapshot) = snapshot.filter(|snapshot| snapshot.peer_usage.is_some()) else {
        return RequestPathEligibility::Unavailable;
    };
    if !scheduler::path_is_schedulable(snapshot, lane) {
        return RequestPathEligibility::Unavailable;
    }
    if scheduler::path_is_backup(snapshot) {
        RequestPathEligibility::Backup
    } else {
        RequestPathEligibility::Regular
    }
}

fn request_path_eligibility_expectation(
    observation: &RequestRelaySchedulingObservation,
    lane: TrafficClass,
) -> SmallVec<[RequestPathEligibilityExpectation; 4]> {
    observation
        .paths
        .iter()
        .map(|path| RequestPathEligibilityExpectation {
            instance: path.instance,
            eligibility: request_path_eligibility(path.shared_snapshot, lane),
        })
        .collect()
}

/// Returns the exact current OriginalData tier. Queue availability is omitted:
/// apply calls this only after reserving the selected writer command.
fn current_request_original_data_tier(
    observation: &RequestRelaySchedulingObservation,
    remotes: &ReliableRelayRemoteSet,
    requalification: &StreamPathRequalification<RelayPathInstance>,
    lane: TrafficClass,
) -> SmallVec<[RelayPathInstance; 4]> {
    let candidates = observation
        .paths
        .iter()
        .filter_map(|path| {
            let remote = remotes
                .paths
                .iter()
                .find(|remote| remote.instance() == path.instance)?;
            if !remote.stream.product_admission_active() {
                return None;
            }
            let eligibility = request_path_eligibility(path.shared_snapshot, lane);
            (eligibility != RequestPathEligibility::Unavailable).then_some((
                path.instance,
                eligibility,
                requalification.stale_for_original_data(path.instance),
            ))
        })
        .collect::<SmallVec<[_; 4]>>();
    [
        (RequestPathEligibility::Regular, false),
        (RequestPathEligibility::Backup, false),
        (RequestPathEligibility::Regular, true),
        (RequestPathEligibility::Backup, true),
    ]
    .into_iter()
    .find_map(|(eligibility, stale)| {
        let tier = candidates
            .iter()
            .filter_map(|(instance, candidate_eligibility, candidate_stale)| {
                (*candidate_eligibility == eligibility && *candidate_stale == stale)
                    .then_some(*instance)
            })
            .collect::<SmallVec<[_; 4]>>();
        (!tier.is_empty()).then_some(tier)
    })
    .unwrap_or_default()
}

#[derive(Debug)]
pub(super) struct RequestMultipathPlan {
    target: RequestMultipathTarget,
    product_mutation: RequestProductSendMutation,
    /// Immutable Product envelope selected for this exact output incarnation.
    /// Apply rechecks the serialized exact ledger against it after command
    /// reservation, so every OriginalData planner shares one authority seam.
    product_limit_bytes: Option<u64>,
    request_load_expectation: Option<(RelayPathKey, u32, u32)>,
    request_proof_expectation: Option<RelayPathProofEpoch>,
    native_authority_stamp: Option<CarrierRateAuthorityStamp>,
    path_eligibility_expectation: SmallVec<[RequestPathEligibilityExpectation; 4]>,
}

/// Preparation may enqueue control evidence but never publishes unique data.
/// The resulting generation and payload classification bound one observation.
#[derive(Debug, Clone, Copy)]
struct PreparedRequestMultipathDecision {
    membership_generation: u64,
    unique_data_payload_bytes: Option<usize>,
}

/// Reconnects may reuse a logical path key, so apply is fenced by incarnation
/// identity and by the complete attachment topology observed during selection.
#[derive(Debug, Clone, Copy)]
struct RequestMultipathTarget {
    membership_generation: u64,
    instance: RelayPathInstance,
}

/// The one product-state mutation authorized after carrier enqueue succeeds.
#[derive(Debug)]
enum RequestProductSendMutation {
    None,
    Data,
    OriginalData {
        candidate: RelayPathInstance,
        target_bytes: u64,
        payload_bytes: u64,
        entry_offset: u64,
        foreign_optional_ranges: usize,
        foreign_optional_bytes: u64,
    },
}

impl RequestProductSendMutation {
    fn assigns_original_data(&self) -> bool {
        matches!(self, Self::Data | Self::OriginalData { .. })
    }
}

impl RequestMultipathPlan {
    fn new(target: RequestMultipathTarget, product_mutation: RequestProductSendMutation) -> Self {
        Self {
            target,
            product_mutation,
            product_limit_bytes: None,
            request_load_expectation: None,
            request_proof_expectation: None,
            native_authority_stamp: None,
            path_eligibility_expectation: SmallVec::new(),
        }
    }

    pub(super) fn target(&self) -> (u64, RelayPathInstance) {
        (self.target.membership_generation, self.target.instance)
    }

    pub(super) fn load_expectation(&self) -> Option<(RelayPathKey, u32, u32)> {
        self.request_load_expectation
    }

    pub(super) fn proof_expectation(&self) -> Option<RelayPathProofEpoch> {
        self.request_proof_expectation
    }

    /// Holds the exact Native activation, central generation, and coherent
    /// shape through the first irreversible Product/queue publication. A
    /// mismatch means the advisory plan is stale, not that the path failed.
    pub(super) fn commit_with_current_native_shape<R>(
        &self,
        commands: &ReliablePathCommandSender,
        commit: impl FnOnce(Option<NativeCarrierSchedulingShapeSnapshot>) -> R,
    ) -> Option<R> {
        match (
            commands.native_rate_authority(),
            self.native_authority_stamp,
        ) {
            (Some(authority), Some(stamp)) => authority
                .commit_with_current_scheduling_shape(stamp, |shape| commit(Some(shape)))
                .ok(),
            (None, None) if self.target.instance.key.underlay == UnderlayProtocol::Tcp => {
                Some(commit(None))
            }
            _ => None,
        }
    }

    pub(super) fn assigns_original_data(&self) -> bool {
        self.product_mutation.assigns_original_data()
    }

    /// Records one target-local apply failure for a finite same-quantum
    /// ordinary replan. This owns no scheduling order: the next attempt is
    /// selected again by ordinary ECF with this exact incarnation excluded.
    pub(super) fn reject_failed_bulk_original_target(
        &self,
        lane: TrafficClass,
        rejected: &mut SmallVec<[RelayPathInstance; 8]>,
    ) -> bool {
        if !lane.is_bulk()
            || !self.assigns_original_data()
            || rejected.contains(&self.target.instance)
        {
            return false;
        }
        rejected.push(self.target.instance);
        true
    }

    /// Resolves the exact planned output at apply. Bulk OriginalData refreshes
    /// the complete current tier after native reservation, so an unrelated
    /// attach/detach must not invalidate the still-current target instance.
    /// Other work retains the frozen membership transaction.
    pub(super) fn target_position_for_apply(
        &self,
        remotes: &ReliableRelayRemoteSet,
        lane: TrafficClass,
    ) -> Option<usize> {
        if lane.is_bulk() && self.assigns_original_data() {
            remotes
                .paths
                .iter()
                .position(|path| path.instance() == self.target.instance)
        } else {
            remotes.path_position_at_generation(
                self.target.membership_generation,
                self.target.instance,
            )
        }
    }

    fn with_eligibility_expectation(
        mut self,
        observation: &RequestRelaySchedulingObservation,
        lane: TrafficClass,
        request_state: Option<RequestSchedulingState<'_>>,
    ) -> Result<Self, RequestMultipathPlanError> {
        self.path_eligibility_expectation = request_path_eligibility_expectation(observation, lane);
        let target = self
            .path_eligibility_expectation
            .iter()
            .find(|expectation| expectation.instance == self.target.instance)
            .filter(|expectation| expectation.eligibility != RequestPathEligibility::Unavailable)
            .ok_or(RequestMultipathPlanError::ServiceBlocked)?;
        debug_assert_eq!(target.instance, self.target.instance);
        self.native_authority_stamp = observation
            .paths
            .iter()
            .find(|path| path.instance == self.target.instance)
            .and_then(|path| path.native_authority_stamp);
        if self.product_mutation.assigns_original_data() {
            let snapshot = request_original_data_authority_snapshot(
                observation,
                self.target.instance,
                None,
                lane,
                request_state,
                observation
                    .paths
                    .iter()
                    .find(|path| path.instance == self.target.instance)
                    .is_some_and(|path| path.load_owned),
            )
            .ok_or(RequestMultipathPlanError::ServiceBlocked)?;
            self.product_limit_bytes = Some(snapshot.data_level_limit_bytes);
        }
        Ok(self)
    }

    /// Revalidates exact unique Product debt after the native command lane has
    /// accepted a reservation and before Product ownership is mutated. This is
    /// deliberately independent of sampled native queue/flight: the command
    /// reservation and downstream writer own native admission.
    pub(super) fn retains_exact_product_headroom(&self, flights: &RequestFlightLedger) -> bool {
        self.product_limit_bytes.is_none_or(|limit| {
            limit > 0 && flights.original_data_in_flight_bytes(self.target.instance) < limit
        })
    }

    /// Revalidates the exact physical owners and the RFC regular/backup
    /// eligibility set immediately before carrier publication. Topology can
    /// stay unchanged across replacement, disable, drain, failure, or a peer
    /// usage update, so membership generation alone is not an apply fence.
    pub(super) fn target_retains_exact_eligibility(
        &self,
        context: &ClientPathContext,
        lane: TrafficClass,
    ) -> bool {
        let current = context.observe_reliable_request_paths(
            self.path_eligibility_expectation.iter().map(|expectation| {
                (
                    expectation.instance,
                    None,
                    ReliableRequestNativeShape::NotApplicable,
                )
            }),
            0,
            false,
        );
        let current_expectation = current
            .paths
            .iter()
            .map(|path| RequestPathEligibilityExpectation {
                instance: path.instance,
                eligibility: request_path_eligibility(path.shared_snapshot, lane),
            })
            .collect::<SmallVec<[_; 4]>>();
        current_expectation == self.path_eligibility_expectation
            && current_expectation.iter().any(|expectation| {
                expectation.instance == self.target.instance
                    && expectation.eligibility != RequestPathEligibility::Unavailable
            })
    }
}

/// Product state and TCP's portable fallback authority for one request.
#[derive(Debug)]
pub(super) struct RequestMultipathController {
    stream_id: StreamId,
    request: RequestStreamState,
    tcp_capacity: RequestTcpCapacityController,
    next_send_index: usize,
}

/// Both accounting views removed by one request Product ACK transaction.
pub(super) struct RequestProductAckRelease {
    pub(super) data_ack_progress_paths: SmallVec<[RelayPathInstance; 4]>,
    /// Exact attachments whose final unique OriginalData flight was released.
    /// Reinjection copies do not participate in active-demand ownership.
    pub(super) idle_original_data_instances: SmallVec<[RelayPathInstance; 4]>,
}

#[cfg(test)]
impl RequestProductAckRelease {
    fn is_empty(&self) -> bool {
        self.data_ack_progress_paths.is_empty()
    }

    fn as_slice(&self) -> &[RelayPathInstance] {
        self.data_ack_progress_paths.as_slice()
    }
}

impl RequestMultipathController {
    pub(super) fn new(stream_id: StreamId) -> Self {
        Self {
            stream_id,
            request: RequestStreamState::default(),
            tcp_capacity: RequestTcpCapacityController::default(),
            next_send_index: 0,
        }
    }

    pub(super) fn reliable_stream_source_admission(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> ReliableStreamSourceAdmission {
        let observation = observe_request_relay_scheduling(
            context,
            self.stream_id,
            remotes.membership_generation(),
            &remotes.paths,
            None,
            lane,
            payload_bytes,
            false,
            &self.request.requalification,
        );
        let request_state = RequestSchedulingState {
            operation: self.request.ack_clock_operation,
            path_states: &self.request.path_states,
            flights: Some(&self.request.flights),
        };
        reliable_stream_source_admission(
            remotes.paths.iter().filter_map(|remote| {
                if !remote.stream.product_admission_active() {
                    return None;
                }
                let instance = remote.instance();
                request_original_data_authority_snapshot(
                    &observation,
                    instance,
                    None,
                    lane,
                    Some(request_state),
                    remote.has_load_reservation(),
                )
                .map(|snapshot| ReliableOriginalDataOutput {
                    snapshot,
                    stale: self
                        .request
                        .requalification
                        .stale_for_original_data(instance),
                })
            }),
            lane,
            payload_bytes,
            context.mux_limits,
        )
    }

    pub(super) fn plan_retains_exact_product_headroom(&self, plan: &RequestMultipathPlan) -> bool {
        plan.retains_exact_product_headroom(&self.request.flights)
    }

    /// Recomputes bulk OriginalData authority from the exact current output set
    /// after the native command lane has accepted a reservation.
    ///
    /// The reservation is final native admission. This transaction then
    /// revalidates current structural eligibility and Product W/P/E without
    /// installing a second sampled queue/flight gate.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn bulk_original_data_apply_authority(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        plan: &RequestMultipathPlan,
        frame: &Frame,
        lane: TrafficClass,
        frontier_state: ReliableDataAckFrontierState,
        pending_load_claim: bool,
    ) -> Option<RequestOriginalDataApplyAuthority> {
        self.bulk_original_data_apply_authority_with_native_shape(
            context,
            remotes,
            plan,
            frame,
            lane,
            frontier_state,
            pending_load_claim,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn bulk_original_data_apply_authority_with_native_shape(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        plan: &RequestMultipathPlan,
        frame: &Frame,
        lane: TrafficClass,
        frontier_state: ReliableDataAckFrontierState,
        pending_load_claim: bool,
        native_shape: Option<NativeCarrierSchedulingShapeSnapshot>,
    ) -> Option<RequestOriginalDataApplyAuthority> {
        if !lane.is_bulk() || !plan.assigns_original_data() {
            return None;
        }
        let (entry_offset, _, payload_bytes) = reliable_stream_frame_extent(frame)?;
        let observation = observe_request_relay_scheduling_with_native_override(
            context,
            self.stream_id,
            remotes.membership_generation(),
            &remotes.paths,
            None,
            lane,
            payload_bytes,
            true,
            &self.request.requalification,
            native_shape.map(|shape| (plan.target.instance, shape)),
        );
        let eligible = current_request_original_data_tier(
            &observation,
            remotes,
            &self.request.requalification,
            lane,
        );
        if !eligible.contains(&plan.target.instance) {
            return None;
        }

        let stream_outstanding_bytes = self.request.flights.total_original_data_in_flight_bytes();
        let lower_owner = self
            .request
            .flights
            .oldest_lower_flight_owner_instance_before_offset(entry_offset);
        let position = if stream_outstanding_bytes == 0 {
            BulkCandidatePosition::FirstPath
        } else if lower_owner == Some(plan.target.instance)
            && frontier_state.owner_has_live_contiguous_frontier()
        {
            BulkCandidatePosition::ContiguousFrontier
        } else {
            BulkCandidatePosition::AdditionalPath
        };
        let target_owns_load = pending_load_claim
            || remotes
                .paths
                .iter()
                .find(|path| path.instance() == plan.target.instance)
                .is_some_and(ReliableRelayRemotePath::has_load_reservation);
        let request_state = RequestSchedulingState {
            operation: self.request.ack_clock_operation,
            path_states: &self.request.path_states,
            flights: Some(&self.request.flights),
        };
        let snapshot = request_original_data_authority_snapshot(
            &observation,
            plan.target.instance,
            lower_owner.map(|instance| instance.key),
            TrafficClass::Throughput,
            Some(request_state),
            target_owns_load,
        )?;
        let output = bulk_original_data_assignment_authority(
            snapshot,
            payload_bytes,
            context.mux_limits,
            position,
            request_state
                .product_assignment_qualified(plan.target.instance, observation.observed_at),
        );
        let windows = reliable_bulk_product_windows(context.mux_limits);
        Some(RequestOriginalDataApplyAuthority {
            position,
            stream_outstanding_bytes,
            stream_limit_bytes: windows.stream_resource_limit_bytes,
            output_outstanding_bytes: self
                .request
                .flights
                .original_data_in_flight_bytes(plan.target.instance),
            output,
        })
    }

    #[cfg(feature = "lab-diagnostics")]
    pub(super) fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub(super) fn data_ack_gap_reinjection_model(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        preview: &Frame,
        lane: TrafficClass,
    ) -> RequestDataAckGapObservation {
        self.data_ack_gap_reinjection_model_with_service(
            context,
            remotes,
            preview,
            lane,
            reliable_stream_frame_accounted_bytes(preview),
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn data_ack_gap_reinjection_service_model(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        preview: &Frame,
        lane: TrafficClass,
        sender_queue: &ReliableRelaySenderQueue,
        reinjection_debt_bytes: usize,
        mux_limits: MuxLimits,
    ) -> RequestDataAckGapObservation {
        self.data_ack_gap_reinjection_model_with_service(
            context,
            remotes,
            preview,
            lane,
            reliable_stream_frame_accounted_bytes(preview),
            Some((sender_queue, reinjection_debt_bytes, mux_limits)),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn data_ack_gap_reinjection_service_model_for_extent(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        preview: &Frame,
        lane: TrafficClass,
        sender_queue: &ReliableRelaySenderQueue,
        reinjection_debt_bytes: usize,
        mux_limits: MuxLimits,
        scoring_payload_bytes: usize,
    ) -> RequestDataAckGapObservation {
        self.data_ack_gap_reinjection_model_with_service(
            context,
            remotes,
            preview,
            lane,
            scoring_payload_bytes,
            Some((sender_queue, reinjection_debt_bytes, mux_limits)),
        )
    }

    fn data_ack_gap_reinjection_model_with_service(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        preview: &Frame,
        lane: TrafficClass,
        scoring_payload_bytes: usize,
        service: Option<(&ReliableRelaySenderQueue, usize, MuxLimits)>,
    ) -> RequestDataAckGapObservation {
        let original_flight = self
            .request
            .flights
            .unique_original_flight_for_frame(preview);
        let original_path = original_flight
            .map(|(instance, _)| instance)
            .filter(|instance| remotes.contains_path_instance(*instance));
        let original_underlay = self
            .request
            .flights
            .original_transmission_underlay_for_frame(preview);
        let recovery_observation = observe_request_relay_scheduling(
            context,
            self.stream_id,
            remotes.membership_generation(),
            &remotes.paths,
            None,
            TrafficClass::Throughput,
            PATH_OPEN_SCORE_BYTES,
            true,
            &self.request.requalification,
        );
        // A replacement carrier with the same numeric path key must not lend
        // its RTT or congestion evidence to an older attachment's flight. The
        // owner and alternate below are both projected from this one immutable
        // observation. The frontier is already part of the owner's exact
        // OriginalData debt, so only the alternate is charged the new copy.
        let original_path_timing = original_path.and_then(|instance| {
            self.request_reinjection_target_snapshot_from_observation(
                &recovery_observation,
                instance,
            )
        });
        let owner_completion = original_path_timing
            .and_then(|snapshot| scheduler::score_path(snapshot, lane, 0))
            .filter(|score| score.eta_ms.is_finite())
            .map(|score| Duration::from_secs_f64(score.eta_ms.max(0.0) / 1000.0));
        let live_instances = remotes.path_instances();
        let mut avoid_instances = self.request.flights.sent_instances_for_frame(preview);
        avoid_instances.retain(|instance| live_instances.contains(instance));
        let target_model_pending = remotes.paths.iter().any(|path| {
            let instance = path.instance();
            !avoid_instances.contains(&instance)
                && path.stream.product_admission_active()
                && !self
                    .request
                    .requalification
                    .stale_for_original_data(instance)
                && (!recovery_observation
                    .path_by_instance(instance)
                    .is_some_and(|observed| observed.has_bulk_model_evidence)
                    || self
                        .request_reinjection_target_snapshot_from_observation(
                            &recovery_observation,
                            instance,
                        )
                        .is_none())
        });
        let mut target_service_exhausted = false;
        let reinjection_path = loop {
            let position = match self.choose_lowest_eta_relay_path_for_extent(
                context,
                remotes,
                preview,
                lane,
                RelaySendCause::PersistentAckGapReinjection,
                &avoid_instances,
                scoring_payload_bytes,
                Some(&recovery_observation),
            ) {
                Ok(position) => position,
                Err(RequestMultipathPlanError::ServiceBlocked) => {
                    // Structural eligibility and measured Product evidence
                    // were present, but every exact alternate's native queue
                    // was full. Preserve that distinction so the actor waits
                    // on capacity rather than an unrelated model publication.
                    target_service_exhausted = true;
                    break None;
                }
                Err(_) => break None,
            };
            let path = &remotes.paths[position];
            let instance = path.instance();
            if !recovery_observation
                .path_by_instance(instance)
                .is_some_and(|observed| observed.has_bulk_model_evidence)
            {
                break None;
            }
            let Some(snapshot) = self.request_reinjection_target_snapshot_from_observation(
                &recovery_observation,
                instance,
            ) else {
                break None;
            };
            let accepted_reinjection_bytes = self
                .request
                .flights
                .reinjected_data_in_flight_bytes(instance);
            if let Some((sender_queue, debt, limits)) = service {
                let queued = sender_queue.request_target_queued_reinjection_bytes(instance, false);
                if reliable_reinjection_service_limit_bytes(
                    ReliableReinjectionTargetWork::new(
                        Some(snapshot),
                        queued,
                        accepted_reinjection_bytes,
                    ),
                    debt,
                    limits,
                ) == 0
                {
                    target_service_exhausted = true;
                    avoid_instances.push(instance);
                    continue;
                }
            }
            let Some(score) = scheduler::score_path(snapshot, lane, scoring_payload_bytes) else {
                break None;
            };
            if !score.eta_ms.is_finite() {
                break None;
            }
            break Some((
                ClientReinjectionOutputIdentity { instance },
                snapshot,
                Duration::from_secs_f64(score.eta_ms.max(0.0) / 1000.0),
                accepted_reinjection_bytes,
            ));
        };
        RequestDataAckGapObservation {
            has_live_original_path: original_path.is_some(),
            original_assignment_at: original_flight.map(|(_, sent_at)| sent_at),
            original_underlay,
            original_path_timing,
            reinjection_target: reinjection_path
                .map(|(identity, snapshot, _, _)| (identity, snapshot)),
            reinjection_target_flight_bytes: reinjection_path
                .map_or(0, |(_, _, _, accepted)| accepted),
            reinjection_completion: reinjection_path.map(|(_, _, completion, _)| completion),
            owner_completion,
            target_service_exhausted: target_service_exhausted && reinjection_path.is_none(),
            target_model_pending: target_model_pending && reinjection_path.is_none(),
            uniform_frontier_extent_bytes: 0,
            owner_recovery_timing: None,
        }
    }

    pub(super) fn reinjection_avoid_instances(
        &self,
        frame: &Frame,
        cause: RelaySendCause,
        remotes: &ReliableRelayRemoteSet,
    ) -> Vec<RelayPathInstance> {
        match cause {
            RelaySendCause::TailReinjection | RelaySendCause::CompletionTailReinjection(_) => {
                let owner_keys = self.request.flights.tail_reinjection_owner_keys(
                    frame,
                    &remotes.path_instances(),
                    Duration::ZERO,
                );
                let mut avoided = remotes
                    .path_instances()
                    .into_iter()
                    .filter(|instance| owner_keys.contains(&instance.key))
                    .collect::<Vec<_>>();
                for instance in self.request.flights.sent_instances_for_frame(frame) {
                    if !avoided.contains(&instance) {
                        avoided.push(instance);
                    }
                }
                avoided
            }
            cause if cause.is_ack_gap_reinjection() => {
                self.request.flights.sent_instances_for_frame(frame)
            }
            RelaySendCause::StalePathReinjection(_)
            | RelaySendCause::ClientStalePathReinjection { .. } => {
                self.request.flights.sent_instances_for_frame(frame)
            }
            cause if cause.is_reinjection() => self.request.flights.sent_instances_for_frame(frame),
            _ => Vec::new(),
        }
    }

    pub(super) fn owner_capable_instances(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        lane: TrafficClass,
    ) -> Vec<RelayPathInstance> {
        self.request_owner_capable_instances(context, remotes, lane)
    }

    pub(super) fn original_transmission_instances_for_frame(
        &self,
        frame: &Frame,
        live_instances: &[RelayPathInstance],
    ) -> Vec<RelayPathInstance> {
        self.request
            .flights
            .original_transmission_instances_for_frame(frame, live_instances)
    }

    pub(super) fn live_owner_uniform_frontier(
        &self,
        range: OffsetRange,
        live_instances: &[RelayPathInstance],
    ) -> Option<ReliableLiveOwnerFrontier<RelayPathInstance>> {
        self.request
            .flights
            .live_owner_uniform_frontier(range, live_instances)
    }

    pub(super) fn tail_reinjection_owner_keys(
        &self,
        frame: &Frame,
        live_instances: &[RelayPathInstance],
        first_reinjection_after: Duration,
    ) -> Vec<RelayPathKey> {
        self.request.flights.tail_reinjection_owner_keys(
            frame,
            live_instances,
            first_reinjection_after,
        )
    }

    /// Returns a distinct measured output only when the exact live original
    /// owner and the measured output both have current completion evidence,
    /// and the original owner no longer falls within the scheduler's adaptive
    /// lead hysteresis. This races a finite retained tail without withdrawing the
    /// live carrier or changing ordinary OriginalData placement.
    pub(super) fn tail_reinjection_earlier_completion_target(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        frame: &Frame,
        lane: TrafficClass,
    ) -> Option<ClientReinjectionOutputIdentity> {
        self.tail_reinjection_earlier_completion_target_from_model(
            context,
            remotes,
            frame,
            lane,
            reliable_stream_frame_accounted_bytes(frame),
            self.data_ack_gap_reinjection_model(context, remotes, frame, lane),
        )
        .map(|(identity, _)| identity)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn tail_reinjection_earlier_completion_service_target(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        frame: &Frame,
        lane: TrafficClass,
        sender_queue: &ReliableRelaySenderQueue,
        reinjection_debt_bytes: usize,
        mux_limits: MuxLimits,
    ) -> Option<RequestCompletionTailTarget> {
        self.tail_reinjection_earlier_completion_service_target_for_extent(
            context,
            remotes,
            frame,
            lane,
            sender_queue,
            reinjection_debt_bytes,
            mux_limits,
            reliable_stream_frame_accounted_bytes(frame),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn tail_reinjection_earlier_completion_service_target_for_extent(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        frame: &Frame,
        lane: TrafficClass,
        sender_queue: &ReliableRelaySenderQueue,
        reinjection_debt_bytes: usize,
        mux_limits: MuxLimits,
        scoring_payload_bytes: usize,
    ) -> Option<RequestCompletionTailTarget> {
        let model = self.data_ack_gap_reinjection_model_with_service(
            context,
            remotes,
            frame,
            lane,
            scoring_payload_bytes,
            Some((sender_queue, reinjection_debt_bytes, mux_limits)),
        );
        let (identity, snapshot) = self.tail_reinjection_earlier_completion_target_from_model(
            context,
            remotes,
            frame,
            lane,
            scoring_payload_bytes,
            model,
        )?;
        self.completion_tail_target_observation(
            identity,
            snapshot,
            model.reinjection_target_flight_bytes,
            sender_queue,
            reinjection_debt_bytes,
            mux_limits,
        )
        .target
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn tail_reinjection_fallback_service_target_observation_for_extent(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        frame: &Frame,
        lane: TrafficClass,
        sender_queue: &ReliableRelaySenderQueue,
        reinjection_debt_bytes: usize,
        mux_limits: MuxLimits,
        scoring_payload_bytes: usize,
    ) -> RequestCompletionTailTargetObservation {
        let model = self.data_ack_gap_reinjection_model_with_service(
            context,
            remotes,
            frame,
            lane,
            scoring_payload_bytes,
            Some((sender_queue, reinjection_debt_bytes, mux_limits)),
        );
        let target_service_exhausted = model.target_service_exhausted;
        let reinjection_target_flight_bytes = model.reinjection_target_flight_bytes;
        let Some((identity, snapshot)) = model.reinjection_target else {
            return RequestCompletionTailTargetObservation {
                target: None,
                target_service_exhausted,
                waiting_for_path_model_publication: model.target_model_pending,
            };
        };
        let mut observation = self.completion_tail_target_observation(
            identity,
            snapshot,
            reinjection_target_flight_bytes,
            sender_queue,
            reinjection_debt_bytes,
            mux_limits,
        );
        observation.target_service_exhausted |= target_service_exhausted;
        observation.waiting_for_path_model_publication |= model.target_model_pending;
        observation
    }

    fn completion_tail_target_observation(
        &self,
        identity: ClientReinjectionOutputIdentity,
        snapshot: PathSnapshot,
        reinjection_target_flight_bytes: usize,
        sender_queue: &ReliableRelaySenderQueue,
        reinjection_debt_bytes: usize,
        mux_limits: MuxLimits,
    ) -> RequestCompletionTailTargetObservation {
        let queued = sender_queue.request_target_queued_reinjection_bytes(identity.instance, false);
        let service_limit_bytes = reliable_reinjection_service_limit_bytes(
            ReliableReinjectionTargetWork::new(
                Some(snapshot),
                queued,
                reinjection_target_flight_bytes,
            ),
            reinjection_debt_bytes,
            mux_limits,
        );
        RequestCompletionTailTargetObservation {
            target: (service_limit_bytes > 0).then_some(RequestCompletionTailTarget {
                identity,
                snapshot,
                service_limit_bytes,
                recovery_interval: reliable_data_retransmission_interval(
                    Some(snapshot.underlay),
                    Some(snapshot),
                ),
            }),
            target_service_exhausted: service_limit_bytes == 0,
            waiting_for_path_model_publication: false,
        }
    }

    fn tail_reinjection_earlier_completion_target_from_model(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        frame: &Frame,
        lane: TrafficClass,
        scoring_payload_bytes: usize,
        model: RequestDataAckGapObservation,
    ) -> Option<(ClientReinjectionOutputIdentity, PathSnapshot)> {
        let (original_instance, _) = self
            .request
            .flights
            .unique_original_flight_for_frame(frame)
            .filter(|(instance, _)| remotes.contains_path_instance(*instance))?;
        if !context.relay_path_instance_has_bulk_model_evidence(original_instance) {
            return None;
        }
        let original = context.reliable_path_snapshot_for_instance(original_instance)?;
        let (target, alternate) = model.reinjection_target?;
        if !context.relay_path_instance_has_bulk_model_evidence(target.instance) {
            return None;
        }
        let payload_bytes = scoring_payload_bytes;
        let original_score = scheduler::score_path(original, lane, payload_bytes)?;
        let alternate_score = scheduler::score_path(alternate, lane, payload_bytes)?;
        if alternate_score.eta_ms >= original_score.eta_ms {
            return None;
        }
        (!scheduler::path_within_adaptive_lead_hysteresis(
            original_score.eta_ms,
            original,
            alternate_score.eta_ms,
            alternate,
            payload_bytes,
        ))
        .then_some((target, alternate))
    }

    #[cfg(test)]
    pub(super) fn latest_unacked_ranges_for_path_instance(
        &self,
        instance: RelayPathInstance,
    ) -> Vec<OffsetRange> {
        self.request
            .flights
            .latest_unacked_ranges_for_path_instance(instance)
    }

    pub(super) fn unacked_original_paths_before(
        &self,
        remotes: &ReliableRelayRemoteSet,
        authoritative_horizon: u64,
    ) -> smallvec::SmallVec<[RelayPathInstance; 4]> {
        self.request
            .flights
            .unacked_original_paths_before(authoritative_horizon)
            .into_iter()
            .filter(|instance| remotes.contains_path_instance(*instance))
            .filter(|instance| {
                !self
                    .request
                    .requalification
                    .stale_for_original_data(*instance)
            })
            .collect()
    }

    pub(super) fn path_recovery_state(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        original_path: RelayPathInstance,
        lane: TrafficClass,
    ) -> RangeRecoveryState {
        let usable_alternate_paths =
            self.usable_reinjection_paths(context, remotes, original_path, lane);
        if usable_alternate_paths.is_empty() {
            return RangeRecoveryState::default();
        }
        // Eligibility to accept a new Product copy and ownership of a copy
        // already accepted by an exact carrier are separate lifetimes. Drain,
        // lane, or policy changes fence future placement but do not erase the
        // accepted flight before exact detach/failure or Data ACK. Its frozen
        // deadline may make the range eligible on another exact target; it
        // never releases the target that already accepted the copy.
        let actor_attached_paths = remotes.path_instances();
        self.request
            .flights
            .range_recovery_state(original_path, &actor_attached_paths)
    }

    pub(super) fn earliest_reinjection_suppression_deadline(
        &self,
        remotes: &ReliableRelayRemoteSet,
    ) -> Option<Instant> {
        self.request
            .flights
            .earliest_reinjection_suppression_deadline(&remotes.path_instances())
    }

    pub(super) fn reinjection_suppression_deadline_for_frame(
        &self,
        frame: &Frame,
        remotes: &ReliableRelayRemoteSet,
    ) -> Option<Instant> {
        self.request
            .flights
            .reinjection_suppression_deadline_for_frame(frame, &remotes.path_instances())
    }

    fn usable_reinjection_paths(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        original_path: RelayPathInstance,
        lane: TrafficClass,
    ) -> Vec<RelayPathInstance> {
        remotes
            .paths
            .iter()
            .filter(|path| path.instance() != original_path)
            .filter(|path| self.path_is_payload_schedulable(context, path, lane))
            .map(|path| path.instance())
            .collect()
    }

    fn path_is_payload_schedulable(
        &self,
        context: &ClientPathContext,
        path: &ReliableRelayRemotePath,
        lane: TrafficClass,
    ) -> bool {
        path.stream.product_admission_active()
            && !self
                .request
                .requalification
                .stale_for_original_data(path.instance())
            && request_path_eligibility(
                context.reliable_path_snapshot_for_instance(path.instance()),
                lane,
            ) != RequestPathEligibility::Unavailable
    }

    pub(super) fn mark_path_stale(&mut self, instance: RelayPathInstance) -> bool {
        let changed = self
            .request
            .requalification
            .mark_stale(instance, Instant::now());
        if changed {
            self.request.flights.invalidate_original_evidence(instance);
            self.request
                .path_states
                .get_mut(instance)
                .reset_for_requalification();
            if self
                .request
                .ack_clock_operation
                .is_some_and(|operation| operation.candidate() == instance)
            {
                self.request.ack_clock_operation = None;
            }
        }
        changed
    }

    pub(super) fn path_is_stale(&self, instance: RelayPathInstance) -> bool {
        self.request
            .requalification
            .stale_for_original_data(instance)
    }

    pub(super) fn requalification_deadline(&self) -> Option<Instant> {
        self.request.requalification.next_deadline()
    }

    pub(super) fn try_enqueue_requalification_probe(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        lane: TrafficClass,
        byte_limit: usize,
    ) -> Result<RequalificationAttempt<RelayPathInstance>, crate::runtime::RuntimeError> {
        let now = Instant::now();
        // Attachment membership can change while no new Product placement is
        // planned. Reconcile here so a detached pending candidate cannot own
        // the only wake deadline and starve another live stale attachment.
        self.reconcile_request_attachment_membership(remotes);
        let candidates = self
            .request
            .requalification
            .eligible_probe_candidates_where(now, |instance| {
                remotes.paths.iter().any(|path| {
                    path.instance() == instance && path.stream.product_admission_active()
                })
            });
        let mut capacity_blocked = Vec::new();
        for candidate in candidates {
            let Some(source_range) = self
                .request
                .flights
                .requalification_source_range(byte_limit)
            else {
                continue;
            };
            let Some(Frame::StreamData {
                stream_id,
                offset,
                payload,
            }) = send_stream
                .retransmission_frames_for_ranges(&[source_range], byte_limit)
                .into_iter()
                .next()
            else {
                continue;
            };
            let Some(path) = remotes
                .paths
                .iter()
                .find(|path| path.instance() == candidate)
            else {
                continue;
            };
            let preview = Frame::StreamRequalifyData {
                stream_id,
                probe_id: 1,
                offset,
                payload: payload.clone(),
            };
            let Some(capacity_wait) =
                TargetCarrierCapacityWait::arm_all(candidate, path.stream.capacity_notifies())
            else {
                continue;
            };
            if !path.stream.can_enqueue_reinjection_frame_now(&preview) {
                capacity_blocked.push(capacity_wait);
                continue;
            }
            let retry_after = reliable_path_stale_interval(
                Some(candidate.key.underlay),
                context.reliable_path_snapshot_for_instance(candidate),
            );
            let Some(probe) = self.request.requalification.start_probe(
                candidate,
                offset,
                payload.len(),
                retry_after,
                now,
            ) else {
                continue;
            };
            let frame = Frame::StreamRequalifyData {
                stream_id,
                probe_id: probe.id,
                offset,
                payload,
            };
            match path.stream.try_enqueue_requalification_frame(frame, lane) {
                Ok(()) => {
                    return Ok(RequalificationAttempt::Published {
                        target: candidate,
                        payload_bytes: probe.payload_bytes as usize,
                    });
                }
                Err(crate::runtime::RuntimeError::SenderServiceBlocked) => {
                    capacity_blocked.push(capacity_wait);
                    self.request
                        .requalification
                        .cancel_unpublished_probe(candidate, probe, now);
                }
                Err(crate::runtime::RuntimeError::ReliablePathSessionClosed) => {
                    self.request
                        .requalification
                        .cancel_unpublished_probe(candidate, probe, now);
                }
                Err(error) => {
                    self.request
                        .requalification
                        .cancel_unpublished_probe(candidate, probe, now);
                    return Err(error);
                }
            }
        }
        if capacity_blocked.is_empty() {
            Ok(RequalificationAttempt::Idle)
        } else {
            Ok(RequalificationAttempt::CapacityBlocked {
                targets: capacity_blocked,
            })
        }
    }

    pub(super) fn acknowledge_requalification_probe(
        &mut self,
        _authenticated_return_instance: RelayPathInstance,
        probe: StreamRequalificationProbe,
    ) -> bool {
        let now = Instant::now();
        // The relay actor authenticates the carrying attachment. The exact
        // non-reused probe, not that return path, identifies the forward
        // attachment whose qualification transaction is pending.
        let Some(target) = self.request.requalification.pending_probe_candidate(probe) else {
            return false;
        };
        let authority_revoked = self.request.path_states.get(target).is_some_and(|state| {
            state.product_qualification_authority() == ProductQualificationAuthority::Revoked
        });
        if !authority_revoked {
            return false;
        }
        let acknowledged = self
            .request
            .requalification
            .acknowledge_probe(target, probe, now);
        if acknowledged {
            // Requalification restores admission under the epoch advanced by
            // the stale transition. It must not replace the ledger with a new
            // epoch-1 default or inherit predecessor evidence.
            let reactivated = self
                .request
                .path_states
                .get_mut(target)
                .reactivate_product_qualification();
            if !matches!(reactivated, Ok(true)) {
                // Serialized preflight makes this unreachable in a valid
                // execution. Restore a stale fail-closed state rather than
                // pairing Acquiring with inactive qualification authority.
                self.request
                    .path_states
                    .get_mut(target)
                    .reset_for_requalification();
                let _ = self.request.requalification.mark_stale(target, now);
                return false;
            }
        }
        acknowledged
    }

    pub(super) fn has_reinjection_path(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        candidate: RelayPathInstance,
        lane: TrafficClass,
    ) -> bool {
        remotes.paths.iter().any(|path| {
            path.instance() != candidate && self.path_is_payload_schedulable(context, path, lane)
        })
    }

    /// One exact request attachment's published Product recovery snapshot.
    ///
    /// Recovery consumes the same request-local, timestamp-coherent Product
    /// authority as OriginalData placement. The shared carrier snapshot does
    /// not publish that stream/direction/incarnation-specific `P` by itself.
    /// Carrier queue telemetry remains descriptive; the exact command
    /// reservation and transport writer own native admission.
    pub(super) fn request_reinjection_target_snapshot(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        path: &ReliableRelayRemotePath,
    ) -> Option<PathSnapshot> {
        let observation = observe_request_relay_scheduling(
            context,
            self.stream_id,
            remotes.membership_generation(),
            &remotes.paths,
            None,
            TrafficClass::Throughput,
            PATH_OPEN_SCORE_BYTES,
            false,
            &self.request.requalification,
        );
        self.request_reinjection_target_snapshot_from_observation(&observation, path.instance())
    }

    /// Rebuilds one QUIC reinjection target from the exact Native shape held
    /// by the caller's apply fence. The override is materialized before the
    /// health lock, preserving the runtime Native -> health lock order.
    pub(super) fn request_reinjection_target_snapshot_with_native_shape(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        instance: RelayPathInstance,
        shape: NativeCarrierSchedulingShapeSnapshot,
    ) -> Option<PathSnapshot> {
        if instance.key.underlay != UnderlayProtocol::Udp
            || shape.stamp().scope()
                != CarrierRateAuthorityScope::new(
                    instance.path_instance_id,
                    PathMetricDirection::ClientToServer,
                )
        {
            return None;
        }
        let observation = observe_request_relay_scheduling_with_native_override(
            context,
            self.stream_id,
            remotes.membership_generation(),
            &remotes.paths,
            None,
            TrafficClass::Throughput,
            PATH_OPEN_SCORE_BYTES,
            false,
            &self.request.requalification,
            Some((instance, shape)),
        );
        let mut snapshot =
            self.request_reinjection_target_snapshot_from_observation(&observation, instance)?;
        if !shape.srtt().is_zero() {
            snapshot.srtt_ms = shape.srtt().as_secs_f64() * 1_000.0;
            snapshot.jitter_ms = shape.rttvar().as_secs_f64() * 1_000.0;
        }
        snapshot.bytes_in_flight = shape.bytes_in_flight();
        snapshot.carrier_inflight_limit_bytes = shape
            .congestion_window()
            .max(u64::from(shape.current_mtu()));
        snapshot.app_limited = shape.app_limited();
        Some(snapshot)
    }

    fn request_reinjection_target_snapshot_from_observation(
        &self,
        observation: &RequestRelaySchedulingObservation,
        instance: RelayPathInstance,
    ) -> Option<PathSnapshot> {
        let path = observation.path_by_instance(instance)?;
        let request_state = RequestSchedulingState {
            operation: self.request.ack_clock_operation,
            path_states: &self.request.path_states,
            flights: Some(&self.request.flights),
        };
        let mut snapshot = request_original_data_authority_snapshot(
            observation,
            instance,
            None,
            TrafficClass::Throughput,
            Some(request_state),
            path.load_owned,
        )?;
        if snapshot.data_level_limit_bytes == 0 {
            return None;
        }
        if let Some(command_pending_bytes) = path.carrier_pending_bytes {
            snapshot.queue_bytes = snapshot.queue_bytes.max(command_pending_bytes);
        }
        snapshot.data_level_queue_bytes = 0;
        debug_assert_eq!(
            snapshot.data_level_bytes_in_flight,
            self.request.flights.original_data_in_flight_bytes(instance),
        );
        Some(snapshot)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn reinjection_path_snapshot(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        excluded: &[RelayPathInstance],
        sender_queue: &ReliableRelaySenderQueue,
        reinjection_debt_bytes: usize,
        mux_limits: MuxLimits,
    ) -> (Option<(RelayPathInstance, PathSnapshot, usize)>, bool) {
        let saw_exhausted_target = Cell::new(false);
        let choose = |allow_backup: bool| {
            remotes
                .paths
                .iter()
                .filter(|path| !excluded.contains(&path.instance()))
                .filter(|path| {
                    !self
                        .request
                        .requalification
                        .stale_for_original_data(path.instance())
                })
                .filter(|path| path.stream.product_admission_active())
                .filter_map(|path| {
                    self.request_reinjection_target_snapshot(context, remotes, path)
                        .map(|snapshot| (path.instance(), snapshot))
                })
                .filter(|(_, snapshot)| allow_backup || !scheduler::path_is_backup(*snapshot))
                .filter_map(|(instance, snapshot)| {
                    let score = scheduler::score_path(
                        snapshot,
                        TrafficClass::Latency,
                        PATH_OPEN_SCORE_BYTES,
                    )?;
                    let queued =
                        sender_queue.request_target_queued_reinjection_bytes(instance, false);
                    let service_limit = reliable_reinjection_service_limit_bytes(
                        ReliableReinjectionTargetWork::new(
                            Some(snapshot),
                            queued,
                            self.accepted_reinjected_data_bytes(instance),
                        ),
                        reinjection_debt_bytes,
                        mux_limits,
                    );
                    if service_limit == 0 {
                        saw_exhausted_target.set(true);
                        return None;
                    }
                    Some((score.eta_ms, instance, snapshot, service_limit))
                })
                .min_by(|left, right| left.0.total_cmp(&right.0))
                .map(|(_, instance, snapshot, service_limit)| (instance, snapshot, service_limit))
        };
        let target = choose(false).or_else(|| choose(true));
        (target, target.is_none() && saw_exhausted_target.get())
    }

    pub(super) fn accepted_reinjected_data_bytes(&self, instance: RelayPathInstance) -> usize {
        self.request
            .flights
            .reinjected_data_in_flight_bytes(instance)
    }

    pub(super) fn stale_original_paths(
        &self,
        remotes: &ReliableRelayRemoteSet,
    ) -> Vec<RelayPathInstance> {
        self.request
            .requalification
            .stale_candidates()
            .filter(|instance| remotes.contains_path_instance(*instance))
            .filter(|instance| {
                self.request
                    .flights
                    .has_original_transmission_flights_for_instance(*instance)
            })
            .collect()
    }

    pub(super) fn record_emitted_frame(
        &mut self,
        context: &ClientPathContext,
        instance: RelayPathInstance,
        frame: &Frame,
        cause: RelaySendCause,
        reinjection_target_snapshot: Option<PathSnapshot>,
    ) -> Result<(usize, Option<Instant>), ProductQualificationAdmissionError> {
        if cause.is_reinjection() {
            if let Some((start, end, _)) = reliable_stream_frame_extent(frame) {
                // Once a duplicate is accepted, overlapping OriginalData can
                // no longer prove which copy delivered it. Only receipts found
                // on the exact overlapping OriginalData flights may scrub M;
                // a bare DSN broadcast has no qualification authority.
                let duplicate_range = OffsetRange { start, end };
                let authorities = self
                    .request
                    .flights
                    .overlapping_original_qualification_receipts(duplicate_range);
                for authority in authorities {
                    self.request
                        .path_states
                        .release_ambiguous_product_qualification(authority, duplicate_range);
                }
            }
            let suppression_interval = reliable_data_retransmission_interval(
                Some(instance.key.underlay),
                Some(
                    reinjection_target_snapshot
                        .expect("accepted reinjection carries its exact target snapshot"),
                ),
            );
            Ok(self
                .request
                .flights
                .record_reinjection_frame_instance_with_suppression_interval(
                    instance,
                    frame,
                    suppression_interval,
                ))
        } else {
            let evidence_eligible = !self
                .request
                .requalification
                .stale_for_original_data(instance);
            let recorded = self.record_original_frame_with_qualification(
                instance,
                frame,
                evidence_eligible,
                context.mux_limits,
            )?;
            Ok((recorded, None))
        }
    }

    /// Starts and tags qualification metadata before installing exact Product
    /// ownership. The caller already owns the serialized stream transaction
    /// and publishes the reserved carrier command only after this returns.
    fn record_original_frame_with_qualification(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
        evidence_eligible: bool,
        mux_limits: MuxLimits,
    ) -> Result<usize, ProductQualificationAdmissionError> {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return Ok(0);
        };
        let max_quantum_bytes = u64::try_from(reliable_relay_buffer_len(mux_limits))
            .map_err(|_| ProductQualificationAdmissionError::InvalidMaxQuantum)?;
        let qualification = evidence_eligible
            .then(|| {
                self.request.path_states.tag_admitted_original(
                    instance,
                    reliable_path_startup_sample_limit_bytes(mux_limits),
                    max_quantum_bytes,
                    OffsetRange { start, end },
                )
            })
            .transpose()?
            .flatten();
        Ok(self
            .request
            .flights
            .record_original_frame_instance_with_evidence(
                instance,
                frame,
                evidence_eligible,
                qualification,
            ))
    }

    pub(super) fn normalize_cursor(&mut self, path_count: usize) {
        if path_count == 0 {
            self.next_send_index = 0;
        } else {
            self.next_send_index %= path_count;
        }
    }

    fn request_capacity_reference(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
    ) -> Option<(RelayPathInstance, RequestProductRateEpoch)> {
        let authority_at = Instant::now();
        remotes
            .paths
            .iter()
            .filter_map(|path| {
                let instance = path.instance();
                let snapshot = context.reliable_path_snapshot_for_instance(instance)?;
                if snapshot.state != scheduler::PathState::Active {
                    return None;
                }
                let model = self
                    .request
                    .path_states
                    .get(instance)?
                    .product_rate_epoch()
                    .filter(|model| {
                        model.fresh_rate_at(authority_at).is_some()
                            && product_delivery_samples_override_startup_prior(
                                model.delivery_samples,
                            )
                    })?;
                Some((instance, model))
            })
            .max_by(|left, right| left.1.rate_bps.total_cmp(&right.1.rate_bps))
    }

    fn record_request_per_flow_rate_sample(
        &mut self,
        instance: RelayPathInstance,
        sample: PathRateSample,
        replace: bool,
        observed_at: Instant,
        freshness_horizon: Duration,
    ) {
        let sample_bps = sample.rate_bps();
        let previous = self
            .request
            .path_states
            .get(instance)
            .and_then(|state| state.product_rate_epoch())
            .filter(|epoch| epoch.fresh_rate_at(observed_at).is_some());
        let (rate_bps, delivery_samples) = if replace || previous.is_none() {
            (sample_bps, 1)
        } else {
            (
                previous.map_or(sample_bps, |previous| {
                    previous.rate_bps.mul_add(0.75, sample_bps * 0.25)
                }),
                previous.map_or(1, |previous| previous.delivery_samples.saturating_add(1)),
            )
        };
        let Some(model) = RequestProductRateEpoch::new(
            rate_bps,
            delivery_samples,
            observed_at,
            freshness_horizon,
        ) else {
            return;
        };
        self.request
            .path_states
            .get_mut(instance)
            .set_product_rate_epoch(model);
    }

    fn request_product_rate_freshness_horizon(
        context: &ClientPathContext,
        instance: RelayPathInstance,
    ) -> Option<Duration> {
        let snapshot = context.reliable_path_snapshot_for_instance(instance)?;
        let srtt = Duration::from_secs_f64(snapshot.srtt_ms.max(1.0) / 1000.0);
        let rttvar = Duration::from_secs_f64(
            snapshot.jitter_ms.max(snapshot.srtt_ms / 8.0).max(0.001) / 1000.0,
        );
        Some(transport_rate_sample_freshness_horizon(srtt, rttvar))
    }

    fn prepare_relay_path_decision(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: &Frame,
        cause: RelaySendCause,
    ) -> Result<PreparedRequestMultipathDecision, RequestMultipathPlanError> {
        if remotes.paths.is_empty() {
            return Err(RequestMultipathPlanError::OutputUnavailable);
        }
        let membership_generation = remotes.membership_generation();
        let unique_data_payload_bytes = (!cause.is_reinjection())
            .then(|| reliable_stream_frame_extent(frame).map(|(_, _, bytes)| bytes))
            .flatten();
        if remotes
            .paths
            .last()
            .is_some_and(|path| path.stream.lane.is_bulk())
            && unique_data_payload_bytes.is_some()
        {
            remotes.retry_pending_path_proofs(context);
        }
        if !cause.is_reinjection()
            && let Some((offset, _, _)) = reliable_stream_frame_extent(frame)
            && self
                .request
                .flights
                .has_missing_original_transmission_before_offset(offset, &remotes.path_instances())
        {
            return Err(RequestMultipathPlanError::ServiceBlocked);
        }
        self.next_send_index %= remotes.paths.len();
        self.reconcile_request_path_state(context, remotes);
        Ok(PreparedRequestMultipathDecision {
            membership_generation,
            unique_data_payload_bytes,
        })
    }

    /// Keeps the ReceiptMode ACK-clock transaction subordinate to ordinary
    /// placement. The ordinary ECF decision owns the target; this function may
    /// only annotate that same target's useful Product commit with measurement
    /// authority.
    fn ordinary_request_product_mutation(
        &self,
        observation: &RequestRelaySchedulingObservation,
        lane: TrafficClass,
        frame: &Frame,
        payload_bytes: usize,
        target: RelayPathInstance,
    ) -> Result<(RequestProductSendMutation, Option<RelayPathProofEpoch>), RequestMultipathPlanError>
    {
        let entry_offset = reliable_stream_frame_extent(frame)
            .map(|(offset, _, _)| offset)
            .ok_or(RequestMultipathPlanError::ServiceBlocked)?;
        let reference_key = self
            .request
            .flights
            .oldest_lower_flight_owner_before_offset(entry_offset)
            .unwrap_or(target.key);
        let measurement = request_ack_clock_measurement_for_ordinary_target(
            observation,
            lane,
            entry_offset,
            payload_bytes,
            self.next_send_index,
            Some(reference_key),
            Some(&self.request.flights),
            Some(RequestSchedulingState {
                operation: self.request.ack_clock_operation,
                path_states: &self.request.path_states,
                flights: Some(&self.request.flights),
            }),
            target,
        )
        .and_then(|choice| match choice {
            BulkRelayPathChoice::SelectedAckClockMeasurement {
                candidate,
                target_bytes,
                proof,
            } if candidate == target => Some((target_bytes, proof)),
            _ => None,
        });
        let Some((target_bytes, proof)) = measurement else {
            return Ok((RequestProductSendMutation::Data, None));
        };
        let (foreign_optional_ranges, foreign_optional_bytes) = if !matches!(
            self.request.ack_clock_operation,
            Some(RequestAckClockOperation::Owner { .. })
        ) {
            self.request
                .flights
                .foreign_original_transmission_debt_before_offset(entry_offset, &[reference_key])
        } else {
            (0, 0)
        };
        Ok((
            RequestProductSendMutation::OriginalData {
                candidate: target,
                target_bytes,
                payload_bytes: u64::try_from(payload_bytes)
                    .map_err(|_| RequestMultipathPlanError::ServiceBlocked)?,
                entry_offset,
                foreign_optional_ranges,
                foreign_optional_bytes,
            },
            Some(proof),
        ))
    }

    #[cfg(test)]
    pub(super) fn plan_relay_path_send(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: &Frame,
        lane: TrafficClass,
        cause: RelaySendCause,
        avoid_instances: &[RelayPathInstance],
    ) -> Result<RequestMultipathPlan, RequestMultipathPlanError> {
        self.plan_relay_path_send_at_frontier(
            context,
            remotes,
            frame,
            lane,
            cause,
            avoid_instances,
            ReliableDataAckFrontierState::Live,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn plan_relay_path_send_at_frontier(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: &Frame,
        lane: TrafficClass,
        cause: RelaySendCause,
        avoid_instances: &[RelayPathInstance],
        frontier_state: ReliableDataAckFrontierState,
    ) -> Result<RequestMultipathPlan, RequestMultipathPlanError> {
        let prepared = self.prepare_relay_path_decision(context, remotes, frame, cause)?;
        if let Some(target) = cause.completion_tail_target() {
            require_active_exact_attachment(remotes, target.instance)?;
            // Decide ranked this exact target with the common frontier M.
            // Apply may have shrunk the queued frame to F, so repeating that
            // comparison here would answer a different question and could
            // discard the already bound winner.  The ordinary planning and
            // native reservation below still revalidate exact attachment,
            // owner avoidance, current Product headroom, and queue capacity.
        }
        let scoring_payload_bytes = reliable_stream_frame_accounted_bytes(frame);
        let observe_bulk_admission = prepared.unique_data_payload_bytes.is_some()
            && lane.is_bulk()
            && (remotes.paths.len() > 1
                || frontier_state == ReliableDataAckFrontierState::AuthoritativeGap);
        let relay_observation = observe_request_relay_scheduling(
            context,
            remotes.stream_id(),
            prepared.membership_generation,
            &remotes.paths,
            Some(frame),
            lane,
            scoring_payload_bytes,
            observe_bulk_admission,
            &self.request.requalification,
        );
        if let Some(payload_bytes) = prepared.unique_data_payload_bytes {
            self.reconcile_request_path_state(context, remotes);
            let request_state = RequestSchedulingState {
                operation: self.request.ack_clock_operation,
                path_states: &self.request.path_states,
                flights: Some(&self.request.flights),
            };
            let ordinary = choose_ordinary_bulk_relay_path_avoiding(BulkRelayFrameRequest {
                observation: &relay_observation,
                lane,
                frame,
                cursor: self.next_send_index,
                avoid_instances,
                path_flights: Some(&self.request.flights),
                request_state: Some(RequestSchedulingState {
                    operation: self.request.ack_clock_operation,
                    path_states: &self.request.path_states,
                    flights: Some(&self.request.flights),
                }),
                frontier_state,
            });
            let instance = match ordinary {
                BulkRelayPathChoice::Selected(instance) => instance,
                BulkRelayPathChoice::Blocked => {
                    return Err(blocked_attachment_set_error(remotes));
                }
                BulkRelayPathChoice::SelectedAckClockMeasurement { .. }
                | BulkRelayPathChoice::NotApplicable => {
                    match choose_observed_ordinary_data_path(
                        &relay_observation,
                        lane,
                        payload_bytes,
                        self.next_send_index,
                        avoid_instances,
                        Some(request_state),
                    ) {
                        ObservedOrdinaryPathChoice::Selected(instance) => instance,
                        ObservedOrdinaryPathChoice::Blocked => {
                            return Err(blocked_attachment_set_error(remotes));
                        }
                        ObservedOrdinaryPathChoice::NoLivePath => {
                            if relay_observation
                                .paths
                                .iter()
                                .any(|path| path.native_authority_unavailable)
                            {
                                return Err(RequestMultipathPlanError::ServiceBlocked);
                            }
                            return Err(RequestMultipathPlanError::OutputUnavailable);
                        }
                    }
                }
            };
            let (product_mutation, proof) = self.ordinary_request_product_mutation(
                &relay_observation,
                lane,
                frame,
                payload_bytes,
                instance,
            )?;
            let mut selection = RequestMultipathPlan::new(
                RequestMultipathTarget {
                    membership_generation: relay_observation.membership_generation,
                    instance,
                },
                product_mutation,
            );
            selection.request_load_expectation =
                observed_request_load_expectation(&relay_observation, instance)?;
            selection.request_proof_expectation = proof;
            return selection.with_eligibility_expectation(
                &relay_observation,
                lane,
                Some(request_state),
            );
        }
        let position = self.choose_lowest_eta_relay_path(
            context,
            remotes,
            frame,
            lane,
            cause,
            avoid_instances,
        )?;
        let product_mutation = if prepared.unique_data_payload_bytes.is_some() {
            RequestProductSendMutation::Data
        } else {
            RequestProductSendMutation::None
        };
        RequestMultipathPlan::new(
            RequestMultipathTarget {
                membership_generation: prepared.membership_generation,
                instance: remotes.paths[position].instance(),
            },
            product_mutation,
        )
        .with_eligibility_expectation(
            &relay_observation,
            lane,
            Some(RequestSchedulingState {
                operation: self.request.ack_clock_operation,
                path_states: &self.request.path_states,
                flights: Some(&self.request.flights),
            }),
        )
    }

    pub(super) fn commit_enqueued_request_product_send(
        &mut self,
        context: &ClientPathContext,
        frame: &Frame,
        plan: &RequestMultipathPlan,
        position: usize,
        path_count: usize,
    ) {
        let instance = plan.target.instance;
        let mutation = &plan.product_mutation;
        self.commit_request_ack_clock_measurement(mutation);
        if !matches!(frame, Frame::StreamData { .. }) {
            debug_assert!(matches!(mutation, RequestProductSendMutation::None));
            self.next_send_index = if path_count == 0 {
                0
            } else {
                (position + 1) % path_count
            };
            return;
        }
        let sent_bytes = reliable_stream_frame_accounted_bytes(frame);
        match mutation {
            RequestProductSendMutation::Data | RequestProductSendMutation::OriginalData { .. } => {
                context.record_relay_path_send(instance, sent_bytes);
            }
            // Reinjection repeats an existing Product offset. Its carrier enqueue
            // neither installs unique-data ownership nor reopens Product inflight
            // credit, so it cannot clear the completion-wait lifecycle.
            RequestProductSendMutation::None => {}
        }
        self.next_send_index = if path_count == 0 {
            0
        } else {
            (position + 1) % path_count
        };
    }

    fn commit_request_ack_clock_measurement(&mut self, commit: &RequestProductSendMutation) {
        match commit {
            RequestProductSendMutation::OriginalData {
                candidate,
                target_bytes,
                payload_bytes,
                entry_offset,
                foreign_optional_ranges,
                foreign_optional_bytes,
            } => {
                let (
                    candidate,
                    target_bytes,
                    payload_bytes,
                    entry_offset,
                    foreign_optional_ranges,
                    foreign_optional_bytes,
                ) = (
                    *candidate,
                    *target_bytes,
                    *payload_bytes,
                    *entry_offset,
                    *foreign_optional_ranges,
                    *foreign_optional_bytes,
                );
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = (
                    entry_offset,
                    foreign_optional_ranges,
                    foreign_optional_bytes,
                );
                if self.request.ack_clock_operation.is_some_and(|operation| {
                    matches!(
                        operation,
                        RequestAckClockOperation::Owner { candidate: owner, .. }
                            if owner != candidate
                    )
                }) {
                    debug_assert!(false, "measurement owner changed before enqueue commit");
                    return;
                }
                if let Some(RequestAckClockOperation::Pending {
                    reference,
                    candidate: pending,
                }) = self.request.ack_clock_operation
                {
                    debug_assert_eq!(pending, candidate);
                    debug_assert!(reference != candidate);
                }
                let beginning = !matches!(
                    self.request.ack_clock_operation,
                    Some(RequestAckClockOperation::Owner { .. })
                );
                let target_bytes = match self.request.ack_clock_operation {
                    Some(RequestAckClockOperation::Owner { target_bytes, .. }) => target_bytes,
                    _ => target_bytes,
                };
                let previous_bytes = if beginning {
                    0
                } else {
                    self.request
                        .path_states
                        .get(candidate)
                        .and_then(|state| state.ack_clock_measurement_bytes())
                        .unwrap_or(0)
                };
                let spent_bytes = previous_bytes.saturating_add(payload_bytes);
                self.request.ack_clock_operation = Some(RequestAckClockOperation::Owner {
                    candidate,
                    target_bytes,
                });
                let candidate_state = self.request.path_states.get_mut(candidate);
                candidate_state.set_ack_clock_measurement_target(target_bytes);
                candidate_state.set_ack_clock_measurement_bytes(spent_bytes);
                #[cfg(feature = "lab-diagnostics")]
                {
                    if beginning {
                        lab_diagnostic(
                            "ack_clock_measurement",
                            format_args!(
                                "phase=owner_started stream_id={} underlay={:?} path_index={} instance_id={} payload_bytes={} target_bytes={} entry_offset={} foreign_optional_ranges={} foreign_optional_bytes={}",
                                self.stream_id.0,
                                candidate.key.underlay,
                                candidate.key.index,
                                candidate.attachment_id,
                                payload_bytes,
                                target_bytes,
                                entry_offset,
                                foreign_optional_ranges,
                                foreign_optional_bytes,
                            ),
                        );
                    }
                    if previous_bytes < target_bytes && spent_bytes >= target_bytes {
                        lab_diagnostic(
                            "ack_clock_measurement",
                            format_args!(
                                "phase=target_spent stream_id={} underlay={:?} path_index={} instance_id={} spent_bytes={} target_bytes={}",
                                self.stream_id.0,
                                candidate.key.underlay,
                                candidate.key.index,
                                candidate.attachment_id,
                                spent_bytes,
                                target_bytes,
                            ),
                        );
                    }
                    static TRACE_COUNT: std::sync::atomic::AtomicU64 =
                        std::sync::atomic::AtomicU64::new(0);
                    let count = TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if count < 16 || count.is_multiple_of(256) {
                        lab_diagnostic(
                            "ack_clock_measurement",
                            format_args!(
                                "phase=selected stream_id={} underlay={:?} path_index={} instance_id={} payload_bytes={} spent_bytes={} target_bytes={}",
                                self.stream_id.0,
                                candidate.key.underlay,
                                candidate.key.index,
                                candidate.attachment_id,
                                payload_bytes,
                                spent_bytes,
                                target_bytes,
                            ),
                        );
                    }
                }
            }
            RequestProductSendMutation::None | RequestProductSendMutation::Data => {}
        }
    }

    fn request_ack_clock_measurement_target_is_sealed(&self, target: RelayPathInstance) -> bool {
        self.request
            .ack_clock_operation
            .filter(|operation| {
                matches!(
                    operation,
                    RequestAckClockOperation::Owner { candidate, .. } if *candidate == target
                )
            })
            .is_some_and(|operation| {
                let RequestAckClockOperation::Owner { target_bytes, .. } = operation else {
                    unreachable!("filtered ACK-clock owner operation")
                };
                self.request
                    .path_states
                    .get(target)
                    .and_then(|state| state.ack_clock_measurement_bytes())
                    .is_some_and(|spent| spent >= target_bytes)
            })
    }

    fn revoke_request_tcp_capacity_measurement(
        &mut self,
        target: RelayPathInstance,
        preserve_committed_product: bool,
    ) -> bool {
        if let Some(state) = self.request.path_states.get_existing_mut(target) {
            state.clear_tcp_capacity_proven();
        }
        self.tcp_capacity.remove(target);
        let product_transaction_preserved = preserve_committed_product
            && self.request_ack_clock_measurement_target_is_sealed(target);
        if product_transaction_preserved {
            // Carrier freshness admits a bounded product transaction but does
            // not own it. Once the fixed target is sealed, keep its exact ACK
            // evidence until product proof or a real path lifecycle change.
            return true;
        }
        if let Some(state) = self.request.path_states.get_existing_mut(target) {
            state.revoke_tcp_capacity();
        }
        if self
            .request
            .ack_clock_operation
            .is_some_and(|operation| operation.candidate() == target)
        {
            self.request.ack_clock_operation = None;
        }
        false
    }

    fn apply_request_tcp_capacity_event(&mut self, event: RequestTcpCapacityEvent) {
        match event {
            RequestTcpCapacityEvent::CarrierProofAccepted {
                target,
                token: _token,
                proof,
            } => {
                let target_state = self.request.path_states.get_mut(target);
                target_state.mark_tcp_capacity_proven();
                target_state.mark_capacity_admitted();
                target_state
                    .rate_evidence_mut(proof.accepted_at)
                    .seed_ack_boundary(proof.accepted_at);
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_tcp_capacity_measurement",
                    format_args!(
                        "phase=carrier_proven stream_id={} path_index={} instance_id={} measurement_id={} train_bytes={} rate_mbps={:.3} proof_ms={}",
                        self.stream_id.0,
                        target.key.index,
                        target.attachment_id,
                        _token,
                        proof.train_bytes,
                        proof.rate_bps as f64 / 1_000_000.0,
                        proof
                            .expires_at
                            .saturating_duration_since(proof.accepted_at)
                            .as_millis(),
                    ),
                );
            }
            RequestTcpCapacityEvent::ProductAdmissionCommitted {
                target,
                measurement,
            } => {
                let _token = measurement.token;
                if let Some(state) = self.request.path_states.get_existing_mut(target) {
                    state.clear_tcp_capacity_proven();
                }
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_tcp_capacity_measurement",
                    format_args!(
                        "phase=product_admission_committed stream_id={} path_index={} instance_id={} measurement_id={}",
                        self.stream_id.0, target.key.index, target.attachment_id, _token,
                    ),
                );
                drop(measurement);
            }
            RequestTcpCapacityEvent::CarrierAuthorityRetired {
                target,
                measurement,
                cause,
            } => {
                let _token = measurement.token;
                let natural_expiry = cause == RequestTcpCapacityRetirement::AuthorityExpired;
                let _product_transaction_preserved =
                    self.revoke_request_tcp_capacity_measurement(target, natural_expiry);
                #[cfg(feature = "lab-diagnostics")]
                match cause {
                    RequestTcpCapacityRetirement::AuthorityExpired => lab_diagnostic(
                        "request_tcp_capacity_measurement",
                        format_args!(
                            "phase=carrier_authority_expired stream_id={} path_index={} instance_id={} measurement_id={} product_transaction_preserved={}",
                            self.stream_id.0,
                            target.key.index,
                            target.attachment_id,
                            _token,
                            _product_transaction_preserved,
                        ),
                    ),
                    RequestTcpCapacityRetirement::AuthorityLost => lab_diagnostic(
                        "request_tcp_capacity_measurement",
                        format_args!(
                            "phase=revoked stream_id={} path_index={} instance_id={} measurement_id={} reason=carrier_authority_lost",
                            self.stream_id.0, target.key.index, target.attachment_id, _token,
                        ),
                    ),
                    RequestTcpCapacityRetirement::Detached
                    | RequestTcpCapacityRetirement::PublicationExpired => {}
                }
                // Product authority is retired before lease Drop can enter the
                // shared path-state lock.
                drop(measurement);
            }
        }
    }

    fn reconcile_request_attachment_membership(&mut self, remotes: &ReliableRelayRemoteSet) {
        let membership_generation = remotes.membership_generation();
        if self.request.membership_generation == Some(membership_generation) {
            return;
        }
        let live_instances = remotes.path_instances().into_iter().collect::<HashSet<_>>();
        for instance in self.request.flights.original_transmission_instances() {
            if !live_instances.contains(&instance) {
                // A detached predecessor retains Product debt for recovery,
                // but its later ACK cannot re-create exact-instance evidence.
                self.request.flights.invalidate_original_evidence(instance);
            }
        }
        self.request.path_states.retain_live(&live_instances);
        self.request
            .requalification
            .retain_live(|instance| live_instances.contains(&instance));
        self.request.membership_generation = Some(membership_generation);
    }

    fn reconcile_request_path_state(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
    ) {
        self.reconcile_request_attachment_membership(remotes);
        let now = Instant::now();
        let reconciliation = context.request_capacity_reconciliation_view(
            self.stream_id,
            self.tcp_capacity.proof_queries(),
            now,
        );
        let committed_product_admissions = self
            .tcp_capacity
            .measurements
            .keys()
            .copied()
            .filter(|target| {
                self.request.path_states.get(*target).is_some_and(|state| {
                    state.ack_clock_proven() && state.product_rate_epoch().is_some()
                })
            })
            .collect::<HashSet<_>>();
        let tcp_events =
            self.tcp_capacity
                .reconcile(&reconciliation, remotes, &committed_product_admissions);
        for event in tcp_events {
            self.apply_request_tcp_capacity_event(event);
        }
        if self.request.ack_clock_operation.is_some_and(|operation| {
            let RequestAckClockOperation::Pending {
                reference,
                candidate,
            } = operation
            else {
                return false;
            };
            self.request
                .path_states
                .get(candidate)
                .is_some_and(|state| state.ack_clock_proven())
                || !self
                    .request
                    .path_states
                    .get(candidate)
                    .is_some_and(|state| state.capacity_admitted())
                || !self
                    .request
                    .path_states
                    .get(candidate)
                    .is_some_and(|state| {
                        state.ack_clock_first_window() || state.tcp_capacity_proven()
                    })
                || !remotes.contains_path_instance(reference)
                || !remotes.paths.iter().any(|path| {
                    path.instance() == candidate && path.key().underlay == UnderlayProtocol::Tcp
                })
        }) {
            self.request.ack_clock_operation = None;
        }
        if let Some(RequestAckClockOperation::Owner {
            candidate,
            target_bytes: _,
        }) = self.request.ack_clock_operation
        {
            if self
                .request
                .path_states
                .get(candidate)
                .is_some_and(|state| state.ack_clock_proven())
            {
                self.request.ack_clock_operation = None;
            } else {
                let placement_valid = self
                    .request
                    .path_states
                    .get(candidate)
                    .is_some_and(|state| state.capacity_admitted())
                    && remotes.contains_path_instance(candidate);
                let transaction_authorized = self
                    .request_ack_clock_measurement_target_is_sealed(candidate)
                    || self
                        .request
                        .path_states
                        .get(candidate)
                        .is_some_and(|state| {
                            state.ack_clock_first_window() || state.tcp_capacity_proven()
                        });
                if !placement_valid || !transaction_authorized {
                    // A sealed AwaitingAck target remains exact-instance state.
                    // Real placement loss or a partial transaction without its
                    // entry proof performs the full abort cleanup.
                    self.revoke_request_tcp_capacity_measurement(candidate, false);
                }
            }
        }
    }

    /// Releases every exact flight copy and retains unique assignment drain.
    pub(super) fn apply_product_ack(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        ranges: &[OffsetRange],
        acked_at: Instant,
    ) -> RequestProductAckRelease {
        self.reconcile_request_attachment_membership(remotes);
        let released = self.request.flights.release_normalized_acked_ranges(ranges);
        let mut released_original_instances = SmallVec::<[RelayPathInstance; 4]>::new();
        for release in released
            .iter()
            .filter(|release| release.kind.is_original_transmission())
        {
            if !released_original_instances.contains(&release.instance) {
                released_original_instances.push(release.instance);
            }
        }
        let idle_original_data_instances = released_original_instances
            .into_iter()
            .filter(|instance| {
                !self
                    .request
                    .flights
                    .has_original_transmission_flights_for_instance(*instance)
            })
            .collect::<SmallVec<[_; 4]>>();
        let delivered_data = self.apply_released_data_at(context, released, acked_at);
        let data_ack_progress_paths = delivered_data
            .iter()
            .map(|progress| progress.instance)
            .collect::<SmallVec<[_; 4]>>();
        RequestProductAckRelease {
            data_ack_progress_paths,
            idle_original_data_instances,
        }
    }

    fn apply_released_data_at(
        &mut self,
        context: &ClientPathContext,
        released: Vec<RequestPathRelease>,
        acked_at: Instant,
    ) -> smallvec::SmallVec<[RequestOwnerAckProgress<RelayPathInstance>; 4]> {
        let mut ordinary_owner_samples =
            HashMap::<RelayPathInstance, (u64, Instant, Instant)>::new();
        let mut delivered_data =
            smallvec::SmallVec::<[RequestOwnerAckProgress<RelayPathInstance>; 4]>::new();
        for release in released {
            if release.kind.is_original_transmission() {
                context.release_relay_path_inflight(release.instance, release.bytes);
            }
            let product_evidence_eligible = release.path_proving
                && self
                    .request
                    .requalification
                    .observe_unique_original_progress(release.instance, release.sent_at);
            if release.kind.is_original_transmission() {
                if let Some(qualification) = release.qualification {
                    if product_evidence_eligible {
                        self.request
                            .path_states
                            .release_exact_product_qualification(qualification, release.range);
                    } else {
                        self.request
                            .path_states
                            .release_ambiguous_product_qualification(qualification, release.range);
                    }
                }
            }
            if product_evidence_eligible {
                // One exact unique Data ACK proves path use independently of
                // capped-volume assignment qualification and numeric timing.
                self.request
                    .path_states
                    .get_mut(release.instance)
                    .mark_product_path_use_proven();
            }
            if product_evidence_eligible {
                if let Some(progress) = delivered_data
                    .iter_mut()
                    .find(|progress| progress.instance == release.instance)
                {
                    progress.bytes = progress.bytes.saturating_add(release.bytes);
                } else {
                    delivered_data.push(RequestOwnerAckProgress {
                        instance: release.instance,
                        bytes: release.bytes,
                    });
                }
            }
            if product_evidence_eligible {
                let sample = ordinary_owner_samples.entry(release.instance).or_insert((
                    0,
                    release.sent_at,
                    release.sent_at,
                ));
                sample.0 = sample.0.saturating_add(release.bytes as u64);
                sample.1 = sample.1.min(release.sent_at);
                sample.2 = sample.2.max(release.sent_at);
            }
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_model",
                format_args!(
                    "stream_id={} path_underlay={:?} path_index={} path_instance={} released_bytes={} elapsed_ms={:.3} path_proving={} cause=stream_ack",
                    self.stream_id.0,
                    release.instance.key.underlay,
                    release.instance.key.index,
                    release.instance.attachment_id,
                    release.bytes,
                    release.elapsed.as_secs_f64() * 1000.0,
                    release.path_proving,
                ),
            );
        }
        for (instance, (bytes, first_sent_at, latest_sent_at)) in ordinary_owner_samples {
            if let Some(expired_epoch) = self
                .request
                .path_states
                .get(instance)
                .and_then(|state| state.product_rate_epoch())
                .filter(|epoch| epoch.fresh_rate_at(acked_at).is_none())
            {
                // Pending timing from the expired authority interval cannot be
                // blended into its successor. A wholly post-expiry sample may
                // qualify immediately; a straddling sample only seeds the next
                // causal ACK window.
                self.request
                    .path_states
                    .get_mut(instance)
                    .rate_evidence_mut(expired_epoch.expires_at)
                    .seed_successor_epoch_boundary(expired_epoch);
            }
            let coverage_floor_bytes = request_path_rate_coverage_floor_bytes(
                instance.key.underlay,
                self.request
                    .path_states
                    .get(instance)
                    .and_then(|state| state.ack_clock_measurement_target()),
                context.mux_limits,
            );
            let update = {
                let evidence = self
                    .request
                    .path_states
                    .get_mut(instance)
                    .rate_evidence_mut(first_sent_at);
                let update = evidence.observe(
                    bytes,
                    first_sent_at,
                    latest_sent_at,
                    acked_at,
                    coverage_floor_bytes,
                    true,
                );
                update
            };
            if let RequestPathRateEvidenceUpdate::Proven {
                sample,
                first_window,
            } = update
            {
                if instance.key.underlay == UnderlayProtocol::Tcp {
                    if first_window {
                        self.request
                            .path_states
                            .get_mut(instance)
                            .mark_ack_clock_first_window();
                    }
                    if let Some(sample) = sample {
                        let replace_startup_rate = self
                            .request
                            .path_states
                            .get_mut(instance)
                            .mark_ack_clock_proven();
                        self.request
                            .path_states
                            .get_mut(instance)
                            .mark_capacity_admitted();
                        if self
                            .request
                            .ack_clock_operation
                            .is_some_and(|operation| operation.candidate() == instance)
                        {
                            self.request.ack_clock_operation = None;
                        }
                        if let Some(freshness_horizon) =
                            Self::request_product_rate_freshness_horizon(context, instance)
                        {
                            self.record_request_per_flow_rate_sample(
                                instance,
                                sample,
                                replace_startup_rate,
                                acked_at,
                                freshness_horizon,
                            );
                        }
                        context.mark_relay_path_ack_clock_rate_sample(
                            instance,
                            sample,
                            replace_startup_rate,
                        );
                        #[cfg(feature = "lab-diagnostics")]
                        {
                            lab_diagnostic(
                                "ack_clock_measurement",
                                format_args!(
                                    "phase=ack_clock_sample stream_id={} underlay={:?} path_index={} instance_id={} evidence_bytes={} sample_elapsed_us={} replace_startup_rate={} rate_bps={}",
                                    self.stream_id.0,
                                    instance.key.underlay,
                                    instance.key.index,
                                    instance.attachment_id,
                                    sample.bytes(),
                                    sample.elapsed().as_micros(),
                                    replace_startup_rate,
                                    sample.rate_bps(),
                                ),
                            );
                        }
                    }
                } else if let Some(sample) = sample {
                    self.request
                        .path_states
                        .get_mut(instance)
                        .mark_capacity_admitted();
                    if let Some(freshness_horizon) =
                        Self::request_product_rate_freshness_horizon(context, instance)
                    {
                        self.record_request_per_flow_rate_sample(
                            instance,
                            sample,
                            false,
                            acked_at,
                            freshness_horizon,
                        );
                    }
                    context.mark_relay_path_rate_sample(instance, sample);
                }
            }
        }
        delivered_data
    }

    pub(super) fn discard_unusable_tail_reinjections(
        &self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        lane: TrafficClass,
    ) -> usize {
        let live_instances = self.request_owner_capable_instances(context, remotes, lane);
        let live_keys = live_instances
            .iter()
            .map(|instance| instance.key)
            .collect::<Vec<_>>();
        sender_queue.discard_unusable_tail_reinjections(|frame| {
            let owner_keys = self
                .request
                .flights
                .original_transmission_keys_for_frame(frame, &live_instances);
            !owner_keys.is_empty() && live_keys.iter().any(|key| !owner_keys.contains(key))
        })
    }

    pub(super) fn discard_stale_bound_reinjections(
        &self,
        sender_queue: &mut ReliableRelaySenderQueue,
        remotes: &ReliableRelayRemoteSet,
    ) -> usize {
        let live_instances = remotes.path_instances();
        sender_queue.discard_stale_bound_reinjections(|cause| {
            cause
                .persistent_client_target()
                .is_none_or(|target| live_instances.contains(&target))
                && cause.server_bound_target().is_none()
        })
    }

    pub(super) fn discard_unavailable_client_path_recovery_reinjections(
        &self,
        sender_queue: &mut ReliableRelaySenderQueue,
        remotes: &ReliableRelayRemoteSet,
    ) -> usize {
        let live_instances = remotes.path_instances();
        sender_queue.discard_unavailable_client_path_recovery_reinjections(|target| {
            live_instances.contains(&target)
        })
    }

    pub(super) fn discard_resolved_stale_path_reinjections(
        &self,
        sender_queue: &mut ReliableRelaySenderQueue,
        remotes: &ReliableRelayRemoteSet,
    ) -> usize {
        sender_queue.discard_resolved_stale_path_reinjections(|path| {
            self.path_is_stale(path) || !remotes.contains_path_instance(path)
        })
    }

    fn request_owner_capable_instances(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        lane: TrafficClass,
    ) -> Vec<RelayPathInstance> {
        remotes
            .paths
            .iter()
            .filter(|path| self.path_is_payload_schedulable(context, path, lane))
            .map(ReliableRelayRemotePath::instance)
            .collect()
    }

    pub(super) fn request_recovery_original_paths(
        &self,
        remotes: &ReliableRelayRemoteSet,
    ) -> Vec<RelayPathInstance> {
        let mut instances = self.stale_original_paths(remotes);
        for instance in self.request.flights.original_transmission_instances() {
            if !remotes.contains_path_instance(instance) && !instances.contains(&instance) {
                instances.push(instance);
            }
        }
        instances
    }

    pub(super) fn release_all(&mut self, context: &ClientPathContext) {
        // The terminal edge revokes authority before discarding its flights,
        // so every retained receipt is already inert while ownership drains.
        self.request.path_states.revoke_all_product_qualification();
        for release in self.request.flights.drain_all() {
            if release.kind.is_original_transmission() {
                context.release_relay_path_inflight(release.instance, release.bytes);
            }
        }
    }

    #[cfg(test)]
    pub(super) fn record_original_frame_for_test(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
    ) {
        let evidence_eligible = !self
            .request
            .requalification
            .stale_for_original_data(instance);
        let mux_limits = MuxLimits::default();
        let max_quantum = reliable_relay_buffer_len(mux_limits);
        if let Frame::StreamData {
            stream_id,
            offset,
            payload,
        } = frame
            && payload.len() > max_quantum
        {
            for start in (0..payload.len()).step_by(max_quantum) {
                let end = (start + max_quantum).min(payload.len());
                let quantum = Frame::StreamData {
                    stream_id: *stream_id,
                    offset: offset
                        .checked_add(u64::try_from(start).expect("test offset fits u64"))
                        .expect("test frame range is valid"),
                    payload: payload.slice(start..end),
                };
                self.record_original_frame_with_qualification(
                    instance,
                    &quantum,
                    evidence_eligible,
                    mux_limits,
                )
                .expect("test OriginalData quantum satisfies request qualification admission");
            }
            return;
        }
        self.record_original_frame_with_qualification(
            instance,
            frame,
            evidence_eligible,
            mux_limits,
        )
        .expect("test OriginalData satisfies request qualification admission");
    }

    #[cfg(test)]
    pub(super) fn record_reinjected_frame_for_test(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
    ) {
        if let Some((start, end, _)) = reliable_stream_frame_extent(frame) {
            let duplicate_range = OffsetRange { start, end };
            let authorities = self
                .request
                .flights
                .overlapping_original_qualification_receipts(duplicate_range);
            for authority in authorities {
                self.request
                    .path_states
                    .release_ambiguous_product_qualification(authority, duplicate_range);
            }
        }
        self.request
            .flights
            .record_reinjection_frame_instance(instance, frame);
    }

    fn choose_lowest_eta_relay_path(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        frame: &Frame,
        lane: TrafficClass,
        cause: RelaySendCause,
        avoid_instances: &[RelayPathInstance],
    ) -> Result<usize, RequestMultipathPlanError> {
        self.choose_lowest_eta_relay_path_for_extent(
            context,
            remotes,
            frame,
            lane,
            cause,
            avoid_instances,
            reliable_stream_frame_accounted_bytes(frame),
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn choose_lowest_eta_relay_path_for_extent(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        frame: &Frame,
        lane: TrafficClass,
        cause: RelaySendCause,
        avoid_instances: &[RelayPathInstance],
        payload_bytes: usize,
        recovery_observation: Option<&RequestRelaySchedulingObservation>,
    ) -> Result<usize, RequestMultipathPlanError> {
        let ack_gap_reinjection = cause.is_ack_gap_reinjection();
        let completion_tail_target = cause.completion_tail_target();
        let requires_measured_reinjection_target = cause.is_persistent_ack_gap_reinjection();
        let required_client_target = cause
            .client_target()
            .or_else(|| completion_tail_target.map(|target| target.instance));
        if let Some(required) = required_client_target {
            require_active_exact_attachment(remotes, required)?;
        }
        let invalid_persistent_target =
            matches!(cause, RelaySendCause::PersistentServerAckGapReinjection(_));
        let live_tail_recovery = matches!(
            cause,
            RelaySendCause::TailReinjection | RelaySendCause::CompletionTailReinjection(_)
        );
        let requires_distinct_output = live_tail_recovery || ack_gap_reinjection;
        let operation_path_in_scope = |path: &ReliableRelayRemotePath| {
            !self
                .request
                .requalification
                .stale_for_original_data(path.instance())
                && !invalid_persistent_target
                && required_client_target.is_none_or(|required| path.instance() == required)
                && (!requires_distinct_output || !avoid_instances.contains(&path.instance()))
        };
        let path_snapshot = |path: &ReliableRelayRemotePath| {
            if cause.is_reinjection() {
                recovery_observation.map_or_else(
                    || self.request_reinjection_target_snapshot(context, remotes, path),
                    |observation| {
                        self.request_reinjection_target_snapshot_from_observation(
                            observation,
                            path.instance(),
                        )
                    },
                )
            } else {
                context.reliable_path_snapshot_for_instance(path.instance())
            }
        };
        let ordinary_path_allowed = |path: &ReliableRelayRemotePath| {
            let has_measured_reinjection_evidence = || {
                recovery_observation.map_or_else(
                    || context.relay_path_instance_has_bulk_model_evidence(path.instance()),
                    |observation| {
                        observation
                            .path_by_instance(path.instance())
                            .is_some_and(|observed| observed.has_bulk_model_evidence)
                    },
                )
            };
            operation_path_in_scope(path)
                && path_snapshot(path).is_some()
                && (!requires_measured_reinjection_target || has_measured_reinjection_evidence())
        };
        // Native queue/flight remains completion-time evidence inside the path
        // score. It is not repair admission authority: the bounded reinjection
        // lane is checked here and reserved atomically before exact K is applied.
        let can_enqueue = |path: &ReliableRelayRemotePath| {
            relay_path_can_enqueue_frame_for_cause_now(path, frame, cause)
        };
        let choose = |allow_backup: bool, prefer_avoiding: bool| {
            remotes
                .paths
                .iter()
                .enumerate()
                .filter(|(_, path)| !prefer_avoiding || !avoid_instances.contains(&path.instance()))
                .filter(|(_, path)| ordinary_path_allowed(path))
                .filter(|(_, path)| can_enqueue(path))
                .filter_map(|(position, path)| {
                    let snapshot = path_snapshot(path)?;
                    if !allow_backup && scheduler::path_is_backup(snapshot) {
                        return None;
                    }
                    let score = scheduler::score_path(snapshot, lane, payload_bytes)?;
                    Some((
                        position,
                        score.eta_ms,
                        cyclic_cursor_distance(position, self.next_send_index, remotes.paths.len()),
                    ))
                })
                .min_by(|left, right| {
                    left.1
                        .total_cmp(&right.1)
                        .then_with(|| left.2.cmp(&right.2))
                })
                .map(|(position, _, _)| position)
        };
        let choose_ranked = || {
            if requires_distinct_output {
                choose(false, true).or_else(|| choose(true, true))
            } else {
                choose(false, true)
                    .or_else(|| choose(false, false))
                    .or_else(|| choose(true, true))
                    .or_else(|| choose(true, false))
            }
        };
        let selected = choose_ranked();
        if let Some(position) = selected {
            return Ok(position);
        }
        let choose_capacity = |allow_backup: bool, prefer_avoiding: bool| {
            remotes
                .paths
                .iter()
                .enumerate()
                .filter(|(_, path)| ordinary_path_allowed(path))
                .filter(|(_, path)| can_enqueue(path))
                .filter(|(_, path)| !prefer_avoiding || !avoid_instances.contains(&path.instance()))
                .filter(|(_, path)| {
                    allow_backup
                        || path_snapshot(path)
                            .is_none_or(|snapshot| !scheduler::path_is_backup(snapshot))
                })
                .map(|(position, _)| position)
                .next()
        };
        let choose_capacity_fallback = || {
            if requires_distinct_output {
                choose_capacity(false, true).or_else(|| choose_capacity(true, true))
            } else {
                choose_capacity(false, true)
                    .or_else(|| choose_capacity(false, false))
                    .or_else(|| choose_capacity(true, true))
                    .or_else(|| choose_capacity(true, false))
            }
        };
        let capacity_fallback = choose_capacity_fallback();
        if let Some(position) = capacity_fallback {
            return Ok(position);
        }
        let has_active_eligible_path = remotes
            .paths
            .iter()
            .any(|path| ordinary_path_allowed(path) && path.stream.product_admission_active());
        if has_active_eligible_path {
            return Err(RequestMultipathPlanError::ServiceBlocked);
        }
        if remotes
            .paths
            .iter()
            .any(|path| operation_path_in_scope(path) && !path.stream.product_admission_active())
        {
            Err(RequestMultipathPlanError::OrderedTerminalPending)
        } else {
            Err(RequestMultipathPlanError::OutputUnavailable)
        }
    }
}

#[cfg(test)]
#[path = "tests_multipath.rs"]
mod tests;
