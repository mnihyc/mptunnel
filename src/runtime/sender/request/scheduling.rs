//! Request-direction path scheduling.
//!
//! This module ranks immutable path and request evidence. The serialized
//! sender owns preparation and applies the returned exact-instance decision.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{lab_diagnostic, lab_diagnostic_event_enabled};
use crate::model::ack_clock::{
    reliable_ack_clock_measurement_ceiling_bytes,
    reliable_request_ack_clock_measurement_target_bytes,
};
#[cfg(feature = "lab-diagnostics")]
use crate::model::admission::bulk_completion_horizon_ms_with_ordering_debt;
use crate::model::admission::{
    BulkAdmissionCheck, BulkCandidatePosition, BulkPathCandidate,
    bulk_candidate_admission_suppression_with_completion_backlog,
    bulk_candidate_admission_suppression_with_ordering_debt, bulk_candidate_pipe_bytes,
    bulk_contiguous_frontier_can_accept_enqueue, bulk_reorder_window_bytes,
    bulk_scheduling_horizon_bytes, bulk_scheduling_window_bytes, bulk_striping_admitted_candidates,
};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, adaptive_reliable_relay_inflight_bytes, data_level_service_window_bytes,
    product_delivery_samples_override_startup_prior,
};
use crate::model::path::{RelayPathInstance, RelayPathKey, RelayPathProofEpoch};
#[cfg(feature = "lab-diagnostics")]
use crate::model::request_evidence::RequestPerFlowRateModel;
use crate::model::tcp_carrier::{TcpCarrierPolicyEpochs, TcpCarrierStableGenerations};
use crate::mux::MuxLimits;
use crate::protocol::frame::reliable_stream_frame_extent;
use crate::protocol::{Frame, PathUsage, StreamId, UnderlayProtocol};
use crate::runtime::path::ReliableRequestTcpPathEvidence;
use crate::runtime::sender::RelaySendCause;
use crate::runtime::stream::request::{
    RequestAckClockOperation, RequestFlightLedger, RequestPathState, RequestPathStates,
};
use crate::scheduler::{
    self, PathRateScope, PathSnapshot, TrafficClass, cyclic_cursor_distance,
    path_within_adaptive_lead_hysteresis,
};
use smallvec::SmallVec;
use std::cmp::Ordering;
use std::collections::HashSet;

/// Exact ordinary authority class that could not accept one fresh request
/// placement. This is an admission observation, not a scheduling instruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct RequestOrdinaryCarrierService {
    pub(in crate::runtime) instance: RelayPathInstance,
    pub(in crate::runtime) service_pipe_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) struct RequestOrdinarySaturationObservation {
    pub(in crate::runtime) stream_id: StreamId,
    pub(in crate::runtime) stable: TcpCarrierStableGenerations,
    pub(in crate::runtime) ordinary_services: SmallVec<[RequestOrdinaryCarrierService; 4]>,
}

pub(super) struct RequestOrdinarySaturationCheck<'a> {
    pub(super) observation: &'a RequestRelaySchedulingObservation,
    pub(super) lane: TrafficClass,
    pub(super) frame: &'a Frame,
    pub(super) cause: RelaySendCause,
    pub(super) avoid_instances: &'a [RelayPathInstance],
    pub(super) path_flights: &'a RequestFlightLedger,
    pub(super) stale_paths: &'a HashSet<RelayPathInstance>,
}

/// Recognizes the Core ordinary-saturation boundary from one fresh blocked
/// observation. Transient apply, proof, load, and carrier-reservation failures
/// never call this check and therefore remain ordinary sender backpressure.
pub(super) fn request_ordinary_saturation_observation(
    check: RequestOrdinarySaturationCheck<'_>,
) -> Option<RequestOrdinarySaturationObservation> {
    let RequestOrdinarySaturationCheck {
        observation,
        lane,
        frame,
        cause,
        avoid_instances,
        path_flights,
        stale_paths,
    } = check;
    let Frame::StreamData {
        stream_id, payload, ..
    } = frame
    else {
        return None;
    };
    if lane != TrafficClass::Throughput
        || cause != RelaySendCause::StreamData
        || *stream_id != observation.stream_id
        || payload.is_empty()
        || !avoid_instances.is_empty()
        || observation.latency_pressure
        || !path_flights.sent_instances_for_frame(frame).is_empty()
    {
        return None;
    }

    let payload_bytes = payload.len();
    let eligible = observation
        .paths
        .iter()
        .filter(|path| !stale_paths.contains(&path.instance))
        .filter(|path| {
            path.shared_snapshot.is_some_and(|snapshot| {
                scheduler::score_path(snapshot, lane, payload_bytes).is_some()
            })
        })
        .collect::<SmallVec<[&RequestRelayPathObservation; 4]>>();
    let authority_class = if eligible.iter().any(|path| {
        path.shared_snapshot
            .is_some_and(|snapshot| !scheduler::path_is_backup(snapshot))
    }) {
        PathUsage::Available
    } else if eligible
        .iter()
        .any(|path| path.shared_snapshot.is_some_and(scheduler::path_is_backup))
    {
        PathUsage::Backup
    } else {
        return None;
    };
    let ordinary_services = eligible
        .into_iter()
        .filter(|path| {
            path.shared_snapshot.is_some_and(|snapshot| {
                scheduler::path_is_backup(snapshot) == (authority_class == PathUsage::Backup)
            })
        })
        .filter_map(|path| {
            let service_pipe = data_level_service_window_bytes(
                path.shared_snapshot?,
                TrafficClass::Throughput,
                observation.mux_limits,
            )
            .ceil();
            (service_pipe.is_finite() && service_pipe > 0.0).then_some(
                RequestOrdinaryCarrierService {
                    instance: path.instance,
                    service_pipe_bytes: service_pipe as u64,
                },
            )
        })
        .collect::<SmallVec<[RequestOrdinaryCarrierService; 4]>>();
    if ordinary_services.is_empty()
        || ordinary_services.iter().any(|ordinary| {
            let path = observation
                .path_by_instance(ordinary.instance)
                .expect("ordinary saturation instance came from this observation");
            !path_flights.has_original_transmission_flights_for_instance(ordinary.instance)
                || (path.can_enqueue_stream_lane
                    && path.shared_snapshot.is_some_and(|snapshot| {
                        bulk_contiguous_frontier_can_accept_enqueue(
                            snapshot,
                            payload_bytes,
                            observation.mux_limits,
                        )
                    }))
        })
    {
        return None;
    }

    Some(RequestOrdinarySaturationObservation {
        stream_id: observation.stream_id,
        stable: observation.tcp_carrier_stable_generations(authority_class)?,
        ordinary_services,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BulkRelayPathChoice {
    Selected(RelayPathInstance),
    SelectedAckClockMeasurement {
        candidate: RelayPathInstance,
        target_bytes: u64,
        proof: RelayPathProofEpoch,
    },
    SelectedAckClockMeasurementFence {
        reference: RelayPathInstance,
        candidate: RelayPathInstance,
    },
    Blocked,
    NotApplicable,
}

/// Carrier-neutral outcome of ordinary request path ranking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ObservedOrdinaryPathChoice {
    Selected(RelayPathInstance),
    Blocked,
    NoLivePath,
}

/// Immutable carrier evidence for one request scheduling decision.
///
/// Queue credit and path health can change immediately after observation. The
/// sender therefore treats a decision as an intent and revalidates topology,
/// proof authority, load, and carrier enqueue while applying it.
#[derive(Debug, Clone, Copy)]
pub(super) struct RequestRelayPathObservation {
    pub(super) instance: RelayPathInstance,
    pub(super) can_enqueue_frame: bool,
    pub(super) can_enqueue_stream_lane: bool,
    pub(super) load_owned: bool,
    pub(super) shared_snapshot: Option<PathSnapshot>,
    pub(super) tcp: Option<ReliableRequestTcpPathEvidence>,
    pub(super) has_bulk_model_evidence: bool,
    pub(super) has_fresh_native_carrier_rate_evidence: bool,
    pub(super) fresh_proof: Option<RelayPathProofEpoch>,
    pub(super) config_ordinal: usize,
}

impl RequestRelayPathObservation {
    fn key(self) -> RelayPathKey {
        self.instance.key
    }

    fn instance(self) -> RelayPathInstance {
        self.instance
    }
}

/// Chooses ordinary data from one immutable observation when no multipath
/// admission transaction applies.
pub(super) fn choose_observed_ordinary_data_path(
    observation: &RequestRelaySchedulingObservation,
    lane: TrafficClass,
    payload_bytes: usize,
    cursor: usize,
    avoid_instances: &[RelayPathInstance],
) -> ObservedOrdinaryPathChoice {
    let allowed = |path: &&RequestRelayPathObservation| path.can_enqueue_stream_lane;
    let choose = |allow_backup: bool, prefer_avoiding: bool| {
        observation
            .paths
            .iter()
            .enumerate()
            .filter(|(_, path)| !prefer_avoiding || !avoid_instances.contains(&path.instance))
            .filter(|(_, path)| allowed(path))
            .filter_map(|(position, path)| {
                let snapshot = path.shared_snapshot?;
                if !allow_backup && scheduler::path_is_backup(snapshot) {
                    return None;
                }
                let score = scheduler::score_path(snapshot, lane, payload_bytes)?;
                Some((
                    path.instance,
                    score.eta_ms,
                    cyclic_cursor_distance(position, cursor, observation.paths.len()),
                ))
            })
            .min_by(|left, right| {
                left.1
                    .total_cmp(&right.1)
                    .then_with(|| left.2.cmp(&right.2))
            })
            .map(|(instance, _, _)| instance)
    };
    if let Some(instance) = choose(false, true)
        .or_else(|| choose(false, false))
        .or_else(|| choose(true, true))
        .or_else(|| choose(true, false))
    {
        return ObservedOrdinaryPathChoice::Selected(instance);
    }
    let capacity_fallback = |allow_backup: bool, prefer_avoiding: bool| {
        observation
            .paths
            .iter()
            .filter(&allowed)
            .filter(|path| !prefer_avoiding || !avoid_instances.contains(&path.instance))
            .find(|path| {
                allow_backup
                    || path
                        .shared_snapshot
                        .is_none_or(|snapshot| !scheduler::path_is_backup(snapshot))
            })
    };
    let capacity_fallback = capacity_fallback(false, true)
        .or_else(|| capacity_fallback(false, false))
        .or_else(|| capacity_fallback(true, true))
        .or_else(|| capacity_fallback(true, false));
    if let Some(path) = capacity_fallback {
        return ObservedOrdinaryPathChoice::Selected(path.instance);
    }
    if !observation.paths.is_empty() {
        ObservedOrdinaryPathChoice::Blocked
    } else {
        ObservedOrdinaryPathChoice::NoLivePath
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ObservedBulkPathCandidate {
    pub(super) candidate: BulkPathCandidate,
    /// Preserves configured tie order for candidates not attached to this flow.
    pub(super) config_ordinal: usize,
}

/// Complete path-owner observation consumed by the read-only request policy.
///
/// Membership generation travels with the evidence so the apply phase cannot
/// accidentally fence a decision against a different attachment topology.
#[derive(Debug)]
pub(super) struct RequestRelaySchedulingObservation {
    pub(super) stream_id: StreamId,
    pub(super) membership_generation: u64,
    pub(super) mux_limits: MuxLimits,
    pub(super) paths: SmallVec<[RequestRelayPathObservation; 4]>,
    pub(super) global_bulk_candidates: SmallVec<[ObservedBulkPathCandidate; 4]>,
    pub(super) latency_pressure: bool,
    pub(super) tcp_carrier_policy_epochs: Option<TcpCarrierPolicyEpochs>,
}

impl RequestRelaySchedulingObservation {
    pub(super) fn tcp_carrier_stable_generations(
        &self,
        authority_class: PathUsage,
    ) -> Option<TcpCarrierStableGenerations> {
        let policy = self.tcp_carrier_policy_epochs?;
        Some(TcpCarrierStableGenerations {
            membership_generation: self.membership_generation,
            ordinary_eligibility_generation: policy.ordinary_eligibility_generation,
            authority_class,
            admission_policy_generation: policy.admission_policy_generation,
            resource_policy_generation: policy.resource_policy_generation,
        })
    }

    pub(super) fn tcp_carrier_stable_generations_for_instance(
        &self,
        instance: RelayPathInstance,
    ) -> Option<TcpCarrierStableGenerations> {
        let snapshot = self.path_by_instance(instance)?.shared_snapshot?;
        let authority_class = if scheduler::path_is_backup(snapshot) {
            PathUsage::Backup
        } else {
            PathUsage::Available
        };
        self.tcp_carrier_stable_generations(authority_class)
    }

    pub(super) fn path_by_instance(
        &self,
        instance: RelayPathInstance,
    ) -> Option<&RequestRelayPathObservation> {
        self.paths.iter().find(|path| path.instance == instance)
    }

    fn path_by_key(&self, key: RelayPathKey) -> Option<&RequestRelayPathObservation> {
        self.paths.iter().find(|path| path.key() == key)
    }

    fn compare_path_keys(&self, left: RelayPathKey, right: RelayPathKey) -> std::cmp::Ordering {
        let ordinal = |key| {
            self.global_bulk_candidates
                .iter()
                .find(|observed| observed.candidate.key == key)
                .map(|observed| observed.config_ordinal)
                .or_else(|| self.path_by_key(key).map(|path| path.config_ordinal))
                .unwrap_or(usize::MAX)
        };
        ordinal(left)
            .cmp(&ordinal(right))
            .then_with(|| left.index.cmp(&right.index))
            .then_with(|| left.underlay.cmp(&right.underlay))
    }
}

/// Immutable request evidence consumed by path selection.
///
/// Scheduling may rank this state but cannot publish or revoke evidence; the
/// serialized request sender remains the only mutation authority.
#[derive(Clone, Copy)]
pub(super) struct RequestSchedulingState<'a> {
    pub(super) operation: Option<RequestAckClockOperation>,
    pub(super) path_states: &'a RequestPathStates,
}

impl<'a> RequestSchedulingState<'a> {
    fn transaction_candidate(self) -> Option<RelayPathInstance> {
        self.operation.map(RequestAckClockOperation::candidate)
    }

    fn path_state(self, instance: RelayPathInstance) -> Option<&'a RequestPathState> {
        self.path_states.get(instance)
    }

    #[cfg(feature = "lab-diagnostics")]
    fn capacity_admitted_count(self) -> usize {
        self.path_states
            .iter()
            .filter(|(_, state)| state.capacity_admitted())
            .count()
    }
}

fn observed_request_ack_clock_measurement_transaction(
    paths: &[RequestRelayPathObservation],
    reference_key: RelayPathKey,
    request_state: Option<RequestSchedulingState<'_>>,
) -> Option<RelayPathInstance> {
    let request_state = request_state?;
    let reference_path = paths.iter().find(|path| path.key() == reference_key)?;
    let (candidate, owner_target_spent) = match request_state.operation? {
        RequestAckClockOperation::Owner {
            candidate,
            target_bytes,
        } => (
            candidate,
            request_state
                .path_state(candidate)
                .and_then(RequestPathState::ack_clock_measurement_bytes)
                .is_some_and(|spent| spent >= target_bytes),
        ),
        RequestAckClockOperation::Pending {
            reference: operation_reference,
            candidate,
        } => (
            (operation_reference == reference_path.instance()).then_some(candidate)?,
            false,
        ),
    };
    let candidate_path = paths.iter().find(|path| {
        path.instance() == candidate && path.key().underlay == UnderlayProtocol::Tcp
    })?;
    // Fresh carrier evidence authorizes entry and target emission. Once the
    // fixed target is fully committed, only exact product ACK or a real path
    // lifecycle change may retire its AwaitingAck transaction.
    let candidate_state = request_state.path_state(candidate)?;
    // An offset-free TCP receipt supplies only the causal boundary for one
    // bounded product stage. It is never equivalent to product ACK proof.
    let authorized = owner_target_spent
        || candidate_state.ack_clock_first_window()
        || candidate_state.tcp_capacity_proven();
    (candidate_state.capacity_admitted() && !candidate_state.ack_clock_proven() && authorized)
        .then_some(candidate_path.instance())
}

pub(super) struct BulkRelayPathRequest<'a> {
    pub(super) observation: &'a RequestRelaySchedulingObservation,
    pub(super) lane: TrafficClass,
    pub(super) offset: u64,
    pub(super) payload_bytes: usize,
    pub(super) cursor: usize,
    pub(super) avoid_instances: &'a [RelayPathInstance],
    pub(super) path_flights: Option<&'a RequestFlightLedger>,
    pub(super) request_state: Option<RequestSchedulingState<'a>>,
}

pub(super) struct BulkRelayFrameRequest<'a> {
    pub(super) observation: &'a RequestRelaySchedulingObservation,
    pub(super) lane: TrafficClass,
    pub(super) frame: &'a Frame,
    pub(super) cursor: usize,
    pub(super) avoid_instances: &'a [RelayPathInstance],
    pub(super) path_flights: Option<&'a RequestFlightLedger>,
    pub(super) request_state: Option<RequestSchedulingState<'a>>,
}

#[derive(Debug, Clone, Copy)]
struct RelayBulkLead {
    key: RelayPathKey,
    snapshot: PathSnapshot,
    eta_ms: f64,
}

fn globally_admitted_bulk_keys(
    observation: &RequestRelaySchedulingObservation,
    payload_bytes: usize,
) -> SmallVec<[RelayPathKey; 4]> {
    bulk_striping_admitted_candidates(
        observation
            .global_bulk_candidates
            .iter()
            .map(|observed| observed.candidate),
        payload_bytes,
        observation.mux_limits,
        |left, right| observation.compare_path_keys(left, right),
    )
    .into_iter()
    .map(|candidate| candidate.key)
    .collect()
}

fn request_path_has_exact_flow_local_delivery_evidence(
    path: &RequestRelayPathObservation,
    request_state: Option<RequestSchedulingState<'_>>,
) -> bool {
    let instance = path.instance();
    let Some(state) = request_state.and_then(|request| request.path_state(instance)) else {
        return false;
    };
    if !state.capacity_admitted() {
        return false;
    }

    match instance.key.underlay {
        UnderlayProtocol::Tcp => state.per_flow_rate().is_some() && state.ack_clock_proven(),
        UnderlayProtocol::Udp => state.product_delivery_proven(),
    }
}

fn request_path_is_validated(path: &RequestRelayPathObservation) -> bool {
    path.fresh_proof.is_some()
}

fn request_quic_native_window_utilization(snapshot: PathSnapshot) -> Option<(u64, u64)> {
    if snapshot.underlay != UnderlayProtocol::Udp
        || !snapshot.app_limited
        || snapshot.carrier_inflight_limit_bytes == 0
    {
        return None;
    }
    let carrier_work = snapshot
        .queue_bytes
        .saturating_add(snapshot.bytes_in_flight);
    let product_work = snapshot
        .data_level_queue_bytes
        .saturating_add(snapshot.data_level_bytes_in_flight);
    let committed = carrier_work.max(product_work);
    (committed < snapshot.carrier_inflight_limit_bytes)
        .then_some((committed, snapshot.carrier_inflight_limit_bytes))
}

fn compare_request_quic_native_window_utilization(
    left: PathSnapshot,
    right: PathSnapshot,
) -> Ordering {
    match (
        request_quic_native_window_utilization(left),
        request_quic_native_window_utilization(right),
    ) {
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (Some((left_committed, left_limit)), Some((right_committed, right_limit))) => {
            (u128::from(left_committed) * u128::from(right_limit))
                .cmp(&(u128::from(right_committed) * u128::from(left_limit)))
        }
        (None, None) => Ordering::Equal,
    }
}

fn request_startup_product_envelope_bytes(payload_bytes: usize, mux_limits: MuxLimits) -> u64 {
    (mux_limits.max_path_flight_bytes as u64)
        .min(mux_limits.max_repair_bytes as u64)
        .min(mux_limits.max_reorder_bytes as u64)
        .min(mux_limits.max_stream_window_bytes)
        .max(payload_bytes as u64)
}

#[cfg(feature = "lab-diagnostics")]
fn request_scoring_class(lane: TrafficClass) -> &'static str {
    if !lane.is_bulk() {
        "preemptible_quantum"
    } else {
        "bulk_horizon"
    }
}

fn request_ack_clock_measurement_reference_reservoir_has_credit(
    flights: &RequestFlightLedger,
    offset: u64,
    candidate: RelayPathInstance,
    request_state: RequestSchedulingState<'_>,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> bool {
    let measurement_prefix = match request_state.operation {
        Some(RequestAckClockOperation::Owner {
            candidate: owner,
            target_bytes,
        }) if owner == candidate => target_bytes,
        _ => reliable_request_ack_clock_measurement_target_bytes(mux_limits),
    };
    let product_envelope = bulk_reorder_window_bytes(payload_bytes, mux_limits);
    let reservoir = bulk_scheduling_window_bytes(payload_bytes, mux_limits)
        .saturating_add(usize::try_from(measurement_prefix).unwrap_or(usize::MAX))
        .min(product_envelope);
    let data_ack_outstanding =
        usize::try_from(flights.original_transmission_bytes_before_offset(offset))
            .unwrap_or(usize::MAX);
    data_ack_outstanding.saturating_add(payload_bytes) <= reservoir
}

#[allow(clippy::too_many_arguments)]
fn choose_request_ack_clock_measurement_with_rates(
    observation: &RequestRelaySchedulingObservation,
    lane: TrafficClass,
    offset: u64,
    payload_bytes: usize,
    cursor: usize,
    reference_key: Option<RelayPathKey>,
    path_flights: Option<&RequestFlightLedger>,
    request_state: Option<RequestSchedulingState<'_>>,
) -> Option<BulkRelayPathChoice> {
    let paths = observation.paths.as_slice();
    #[cfg(feature = "lab-diagnostics")]
    let stream_id = observation.stream_id;
    let reference_key = reference_key?;
    // Product ACK-clock measurement is the TCP target's fallback when native
    // carrier telemetry is unavailable. Its reference may use either underlay.
    let reference_path = paths.iter().find(|path| path.key() == reference_key)?;
    let reference_instance = reference_path.instance();
    let reference_proven = request_state.is_none_or(|request| {
        request
            .path_state(reference_instance)
            .is_some_and(RequestPathState::product_delivery_proven)
    });
    let latency_pressure = observation.latency_pressure;
    let reference_bulk_evidence = reference_path.has_bulk_model_evidence;
    if !reference_proven || latency_pressure || !reference_bulk_evidence {
        #[cfg(feature = "lab-diagnostics")]
        if request_state.is_some_and(|request| request.capacity_admitted_count() > 0) {
            static EARLY_TRACE_COUNT: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            let count = EARLY_TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if count < 16 || count.is_multiple_of(1024) {
                lab_diagnostic(
                    "ack_clock_measurement",
                    format_args!(
                        "phase=early_gate reference_underlay={:?} reference_index={} reference_instance={} reference_proven={} latency_pressure={} reference_bulk_evidence={}",
                        reference_key.underlay,
                        reference_key.index,
                        reference_instance.attachment_id,
                        reference_proven,
                        latency_pressure,
                        reference_bulk_evidence,
                    ),
                );
            }
        }
        return None;
    }
    let flights = path_flights?;
    if flights.has_reinjection_flights_before_offset(offset) {
        #[cfg(feature = "lab-diagnostics")]
        if request_state.is_some_and(|request| request.capacity_admitted_count() > 0) {
            static REPAIR_TRACE_COUNT: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            let count = REPAIR_TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if count < 16 || count.is_multiple_of(1024) {
                lab_diagnostic(
                    "ack_clock_measurement",
                    format_args!(
                        "phase=reinjection_gate reference_underlay={:?} reference_index={} reference_instance={} offset={}",
                        reference_key.underlay,
                        reference_key.index,
                        reference_instance.attachment_id,
                        offset,
                    ),
                );
            }
        }
        return None;
    }
    // Flight age starts at logical carrier enqueue, so a healthy deep TCP
    // pipeline can exceed one PTO. Exact reinjection, foreign-owner, and bounded
    // ordering-debt checks below own the product transition instead.
    let request_state = request_state?;
    let default_target =
        reliable_request_ack_clock_measurement_target_bytes(observation.mux_limits);
    let target = |instance: RelayPathInstance| match request_state.operation {
        Some(RequestAckClockOperation::Owner {
            candidate,
            target_bytes,
        }) if candidate == instance => target_bytes,
        _ => default_target,
    };
    let spent = |instance: RelayPathInstance| {
        matches!(
            request_state.operation,
            Some(RequestAckClockOperation::Owner { candidate, .. }) if candidate == instance
        )
        .then(|| {
            request_state
                .path_state(instance)
                .and_then(RequestPathState::ack_clock_measurement_bytes)
        })
        .flatten()
        .unwrap_or(0)
    };
    let hard_ceiling = reliable_ack_clock_measurement_ceiling_bytes(observation.mux_limits);
    let product_envelope =
        request_startup_product_envelope_bytes(payload_bytes, observation.mux_limits);
    let mut allowed_owner_keys = vec![reference_key];
    for path in paths.iter().filter(|path| {
        request_state
            .path_state(path.instance())
            .is_some_and(RequestPathState::capacity_admitted)
    }) {
        if !allowed_owner_keys.contains(&path.key()) {
            allowed_owner_keys.push(path.key());
        }
    }
    #[cfg(feature = "lab-diagnostics")]
    {
        static CANDIDATE_TRACE_COUNT: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let count = CANDIDATE_TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if count < 32 || count.is_multiple_of(1024) {
            for path in paths.iter().filter(|path| {
                path.key().underlay == UnderlayProtocol::Tcp
                    && path.key() != reference_key
                    && request_state
                        .path_state(path.instance())
                        .is_some_and(RequestPathState::capacity_admitted)
                    && !request_state
                        .path_state(path.instance())
                        .is_some_and(RequestPathState::ack_clock_proven)
            }) {
                let proof_fresh = path.fresh_proof.is_some();
                let candidate_bulk_evidence = path.has_bulk_model_evidence;
                let candidate_native_carrier_rate_evidence =
                    path.has_fresh_native_carrier_rate_evidence;
                let snapshot = relay_path_snapshot_for_bulk_choice(
                    observation,
                    path.instance(),
                    Some(reference_key),
                    Some(request_state),
                    path.load_owned,
                );
                let spent = spent(path.instance());
                let foreign_owner = flights
                    .has_foreign_original_transmission_before_offset(offset, &allowed_owner_keys);
                let ordering_debt = flights.ordering_debt_bytes_before_offset(path.key(), offset);
                let candidate_product_debt = snapshot.map_or(u64::MAX, |snapshot| {
                    snapshot
                        .data_level_bytes_in_flight
                        .saturating_add(snapshot.data_level_queue_bytes)
                        .saturating_add(payload_bytes as u64)
                });
                lab_diagnostic(
                    "ack_clock_measurement",
                    format_args!(
                        "phase=candidate stream_id={} underlay={:?} path_index={} instance_id={} same_underlay={} proof_fresh={} bulk_evidence={} native_carrier_rate_evidence={} receipt_boundary={} owner_match={} spent_bytes={} limit_bytes={} payload_bytes={} fits_target={} foreign_owner={} ordering_debt={} candidate_product_debt={} product_envelope={} within_envelope={} can_enqueue={} scoreable={} product_inflight={} product_queue={} active_latency={} session_latency={}",
                        stream_id.0,
                        path.key().underlay,
                        path.key().index,
                        path.instance.attachment_id,
                        path.key().underlay == reference_key.underlay,
                        proof_fresh,
                        candidate_bulk_evidence,
                        candidate_native_carrier_rate_evidence,
                        request_state
                            .path_state(path.instance())
                            .is_some_and(RequestPathState::ack_clock_first_window),
                        request_state
                            .transaction_candidate()
                            .is_none_or(|candidate| candidate == path.instance()),
                        spent,
                        target(path.instance()),
                        payload_bytes,
                        spent < target(path.instance())
                            && spent.saturating_add(payload_bytes as u64) <= hard_ceiling,
                        foreign_owner,
                        ordering_debt,
                        candidate_product_debt,
                        product_envelope,
                        candidate_product_debt <= hard_ceiling
                            && ordering_debt.saturating_add(candidate_product_debt)
                                <= product_envelope,
                        path.can_enqueue_frame,
                        snapshot.is_some_and(|snapshot| {
                            scheduler::score_path(snapshot, lane, payload_bytes).is_some()
                        }),
                        snapshot.map_or(0, |snapshot| snapshot.data_level_bytes_in_flight),
                        snapshot.map_or(0, |snapshot| snapshot.data_level_queue_bytes),
                        snapshot.map_or(0, |snapshot| snapshot.active_latency_sensitive_flows),
                        snapshot.map_or(0, |snapshot| snapshot
                            .session_active_latency_sensitive_flows),
                    ),
                );
            }
        }
    }

    paths
        .iter()
        .enumerate()
        .filter(|(_, path)| path.key().underlay == UnderlayProtocol::Tcp)
        .filter(|(_, path)| path.key() != reference_key)
        .filter(|(_, path)| {
            request_state
                .path_state(path.instance())
                .is_some_and(RequestPathState::capacity_admitted)
        })
        .filter(|(_, path)| {
            !request_state
                .path_state(path.instance())
                .is_some_and(RequestPathState::ack_clock_proven)
        })
        // TCP_INFO delivery-rate and congestion-window evidence already owns
        // subflow capacity. MPP Data ACK calibration remains only the fallback
        // when the native TCP measurement is unavailable.
        .filter(|(_, path)| {
            !path.has_fresh_native_carrier_rate_evidence
                || matches!(
                    request_state.operation,
                    Some(RequestAckClockOperation::Owner { candidate, .. })
                        if candidate == path.instance()
                )
        })
        .filter(|(_, path)| {
            request_state
                .path_state(path.instance())
                .is_some_and(|state| state.ack_clock_first_window() || state.tcp_capacity_proven())
        })
        .filter(|(_, path)| {
            request_state
                .transaction_candidate()
                .is_none_or(|candidate| candidate == path.instance())
        })
        .filter(|(_, path)| {
            spent(path.instance()) < target(path.instance())
                && spent(path.instance()).saturating_add(payload_bytes as u64) <= hard_ceiling
        })
        .filter(|(_, path)| path.can_enqueue_frame)
        .filter(|_| {
            !flights.has_foreign_original_transmission_before_offset(offset, &allowed_owner_keys)
        })
        .filter_map(|(position, path)| {
            let proof = path.fresh_proof?;
            let snapshot = relay_path_snapshot_for_bulk_choice(
                observation,
                path.instance(),
                Some(reference_key),
                Some(request_state),
                path.load_owned,
            )?;
            if snapshot.active_latency_sensitive_flows > 0
                || snapshot.session_active_latency_sensitive_flows > 0
                || !path.has_bulk_model_evidence
            {
                return None;
            }
            let ordering_debt = flights.ordering_debt_bytes_before_offset(path.key(), offset);
            let candidate_product_debt = snapshot
                .data_level_bytes_in_flight
                .saturating_add(snapshot.data_level_queue_bytes)
                .saturating_add(payload_bytes as u64);
            if candidate_product_debt > hard_ceiling
                || ordering_debt.saturating_add(candidate_product_debt) > product_envelope
            {
                return None;
            }
            let score = scheduler::score_path(snapshot, lane, payload_bytes)?;
            let spent = spent(path.instance());
            Some((
                position,
                path.instance(),
                target(path.instance()),
                proof,
                spent > 0,
                cyclic_cursor_distance(position, cursor, paths.len()),
                score.eta_ms,
                scheduler::path_is_backup(snapshot),
            ))
        })
        .min_by(|left, right| {
            left.7.cmp(&right.7).then_with(|| {
                right
                    .4
                    .cmp(&left.4)
                    .then_with(|| left.5.cmp(&right.5))
                    .then_with(|| left.6.total_cmp(&right.6))
                    .then_with(|| left.1.attachment_id.cmp(&right.1.attachment_id))
            })
        })
        .map(|(_, candidate, target_bytes, proof, _, _, _, _)| {
            BulkRelayPathChoice::SelectedAckClockMeasurement {
                candidate,
                target_bytes,
                proof,
            }
        })
}

#[cfg(feature = "lab-diagnostics")]
#[derive(Debug, Clone, Copy)]
struct BulkRelayCandidateDiagnostics {
    stream_id: StreamId,
    lane: TrafficClass,
    key: RelayPathKey,
    lead_key: Option<RelayPathKey>,
    position: Option<BulkCandidatePosition>,
    eta_ms: Option<f64>,
    best_eta_ms: Option<f64>,
    completion_horizon_ms: Option<f64>,
    stream_ordering_debt_bytes: u64,
    payload_bytes: usize,
    scoring_payload_bytes: Option<usize>,
    scoring_class: Option<&'static str>,
    snapshot: Option<PathSnapshot>,
}

#[cfg(feature = "lab-diagnostics")]
impl BulkRelayCandidateDiagnostics {
    fn skipped(
        stream_id: StreamId,
        lane: TrafficClass,
        key: RelayPathKey,
        lead_key: Option<RelayPathKey>,
        payload_bytes: usize,
    ) -> Self {
        Self {
            stream_id,
            lane,
            key,
            lead_key,
            position: None,
            eta_ms: None,
            best_eta_ms: None,
            completion_horizon_ms: None,
            stream_ordering_debt_bytes: 0,
            payload_bytes,
            scoring_payload_bytes: None,
            scoring_class: None,
            snapshot: None,
        }
    }
}

#[cfg(feature = "lab-diagnostics")]
struct RequestFlowLocalBulkCandidate {
    eta_ms: f64,
    cursor_distance: usize,
    snapshot: PathSnapshot,
    diagnostics: BulkRelayCandidateDiagnostics,
    instance: RelayPathInstance,
    initial_gate: &'static str,
    local_model: Option<RequestPerFlowRateModel>,
}

#[cfg(feature = "lab-diagnostics")]
fn log_bulk_relay_candidate_decision(
    diagnostics: BulkRelayCandidateDiagnostics,
    selected: bool,
    reason: &'static str,
) {
    if !lab_diagnostic_event_enabled("scheduler_decision") {
        return;
    }
    static LEAD_TRACE_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    static ADDITIONAL_TRACE_COUNT: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);
    static SKIPPED_TRACE_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let additional = diagnostics
        .lead_key
        .is_some_and(|lead| lead != diagnostics.key);
    let count = if !selected {
        SKIPPED_TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    } else if additional {
        ADDITIONAL_TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    } else {
        LEAD_TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    };
    let sample_period = if !selected || additional { 128 } else { 1024 };
    if count >= 512 && !count.is_multiple_of(sample_period) {
        return;
    }
    let lead_underlay = diagnostics
        .lead_key
        .map(|key| format!("{:?}", key.underlay))
        .unwrap_or_else(|| "none".to_string());
    let lead_index = diagnostics
        .lead_key
        .map(|key| key.index.to_string())
        .unwrap_or_else(|| "none".to_string());
    let position = diagnostics
        .position
        .map(|position| format!("{position:?}"))
        .unwrap_or_else(|| "unknown".to_string());
    let eta_ms = diagnostics
        .eta_ms
        .map(|eta_ms| format!("{eta_ms:.3}"))
        .unwrap_or_else(|| "none".to_string());
    let best_eta_ms = diagnostics
        .best_eta_ms
        .map(|eta_ms| format!("{eta_ms:.3}"))
        .unwrap_or_else(|| "none".to_string());
    let completion_horizon_ms = diagnostics
        .completion_horizon_ms
        .map(|horizon| format!("{horizon:.3}"))
        .unwrap_or_else(|| "none".to_string());
    let scoring_payload_bytes = diagnostics
        .scoring_payload_bytes
        .map(|bytes| bytes.to_string())
        .unwrap_or_else(|| "none".to_string());
    let scoring_class = diagnostics.scoring_class.unwrap_or("unknown");
    let (
        product_queue_debt,
        carrier_queue_debt,
        bytes_in_flight,
        inflight_limit,
        product_inflight_limit,
        confidence,
        app_limited,
        delivery_rate_bps,
        pacing_rate_bps,
    ) = diagnostics
        .snapshot
        .map(|snapshot| {
            (
                snapshot.data_level_bytes_in_flight,
                snapshot.queue_bytes,
                snapshot.bytes_in_flight,
                snapshot.carrier_inflight_limit_bytes,
                snapshot.data_level_limit_bytes,
                snapshot.confidence,
                snapshot.app_limited,
                snapshot.delivery_rate_bps,
                snapshot.pacing_rate_bps,
            )
        })
        .unwrap_or((0, 0, 0, 0, 0, 0.0, false, 0.0, 0.0));

    lab_diagnostic(
        "scheduler_decision",
        format_args!(
            "stream_id={} lane={:?} candidate_underlay={:?} candidate_index={} lead_underlay={} lead_index={} position={} selected={} reason={} eta_ms={} best_eta_ms={} completion_horizon_ms={} stream_ordering_debt_bytes={} payload_bytes={} scoring_payload_bytes={} scoring_class={} product_queue_debt={} carrier_queue_debt={} bytes_in_flight={} inflight_limit={} product_inflight_limit={} confidence={:.3} app_limited={} delivery_rate_bps={:.0} pacing_rate_bps={:.0} delivery_sample_source=sender_model",
            diagnostics.stream_id.0,
            diagnostics.lane,
            diagnostics.key.underlay,
            diagnostics.key.index,
            lead_underlay,
            lead_index,
            position,
            selected,
            reason,
            eta_ms,
            best_eta_ms,
            completion_horizon_ms,
            diagnostics.stream_ordering_debt_bytes,
            diagnostics.payload_bytes,
            scoring_payload_bytes,
            scoring_class,
            product_queue_debt,
            carrier_queue_debt,
            bytes_in_flight,
            inflight_limit,
            product_inflight_limit,
            confidence,
            app_limited,
            delivery_rate_bps,
            pacing_rate_bps,
        ),
    );
}

#[cfg(feature = "lab-diagnostics")]
fn log_request_flow_local_admission_shadow(
    diagnostics: BulkRelayCandidateDiagnostics,
    instance: RelayPathInstance,
    initial_gate: &'static str,
    outcome: &'static str,
    global_admitted_keys: &[RelayPathKey],
    retained_admitted_keys: &[RelayPathKey],
    local_model: Option<RequestPerFlowRateModel>,
) {
    if !lab_diagnostic_event_enabled("request_flow_local_admission_shadow") {
        return;
    }
    static TRACE_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let count = TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if count >= 512 && !count.is_multiple_of(256) {
        return;
    }
    let (local_rate_bps, local_delivery_samples) = local_model
        .map(|model| (model.rate_bps, model.delivery_samples))
        .unwrap_or((0.0, 0));
    let global_key_present = global_admitted_keys.contains(&diagnostics.key);
    let retained_key_present = retained_admitted_keys.contains(&diagnostics.key);
    let lead_underlay = diagnostics
        .lead_key
        .map(|key| format!("{:?}", key.underlay))
        .unwrap_or_else(|| "none".to_string());
    let lead_index = diagnostics
        .lead_key
        .map(|key| key.index.to_string())
        .unwrap_or_else(|| "none".to_string());
    let position = diagnostics
        .position
        .map(|position| format!("{position:?}"))
        .unwrap_or_else(|| "unknown".to_string());
    let confidence = diagnostics
        .snapshot
        .map_or(0.0, |snapshot| snapshot.confidence);
    let app_limited = diagnostics
        .snapshot
        .is_some_and(|snapshot| snapshot.app_limited);
    let rate_scope = diagnostics
        .snapshot
        .map(|snapshot| format!("{:?}", snapshot.rate_scope))
        .unwrap_or_else(|| "none".to_string());
    let (
        eta_ms,
        best_eta_ms,
        completion_horizon_ms,
        product_queue_debt,
        carrier_queue_debt,
        bytes_in_flight,
        inflight_limit,
        product_inflight_limit,
        delivery_rate_bps,
        pacing_rate_bps,
    ) = diagnostics
        .snapshot
        .map(|snapshot| {
            (
                diagnostics.eta_ms.unwrap_or(0.0),
                diagnostics.best_eta_ms.unwrap_or(0.0),
                diagnostics.completion_horizon_ms.unwrap_or(0.0),
                snapshot.data_level_bytes_in_flight,
                snapshot.queue_bytes,
                snapshot.bytes_in_flight,
                snapshot.carrier_inflight_limit_bytes,
                snapshot.data_level_limit_bytes,
                snapshot.delivery_rate_bps,
                snapshot.pacing_rate_bps,
            )
        })
        .unwrap_or((0.0, 0.0, 0.0, 0, 0, 0, 0, 0, 0.0, 0.0));
    lab_diagnostic(
        "request_flow_local_admission_shadow",
        format_args!(
            "ordinal={} stream_id={} candidate_underlay={:?} candidate_index={} instance_id={} initial_gate={} outcome={} global_key_present={} retained_key_present={} capacity_admitted=true ack_clock_proven=true local_model_present={} global_admitted_keys={:?} retained_admitted_keys={:?} lead_underlay={} lead_index={} position={} local_rate_bps={:.0} local_delivery_samples={} eta_ms={:.3} best_eta_ms={:.3} completion_horizon_ms={:.3} stream_ordering_debt_bytes={} product_queue_debt={} carrier_queue_debt={} bytes_in_flight={} inflight_limit={} product_inflight_limit={} confidence={:.3} app_limited={} rate_scope={} delivery_rate_bps={:.0} pacing_rate_bps={:.0}",
            count + 1,
            diagnostics.stream_id.0,
            diagnostics.key.underlay,
            diagnostics.key.index,
            instance.attachment_id,
            initial_gate,
            outcome,
            global_key_present,
            retained_key_present,
            local_model.is_some(),
            global_admitted_keys,
            retained_admitted_keys,
            lead_underlay,
            lead_index,
            position,
            local_rate_bps,
            local_delivery_samples,
            eta_ms,
            best_eta_ms,
            completion_horizon_ms,
            diagnostics.stream_ordering_debt_bytes,
            product_queue_debt,
            carrier_queue_debt,
            bytes_in_flight,
            inflight_limit,
            product_inflight_limit,
            confidence,
            app_limited,
            rate_scope,
            delivery_rate_bps,
            pacing_rate_bps,
        ),
    );
}

pub(super) fn choose_bulk_relay_path_avoiding(
    request: BulkRelayFrameRequest<'_>,
) -> BulkRelayPathChoice {
    let BulkRelayFrameRequest {
        observation,
        lane,
        frame,
        cursor,
        avoid_instances,
        path_flights,
        request_state,
    } = request;
    let Some((offset, _, payload_bytes)) = reliable_stream_frame_extent(frame) else {
        return BulkRelayPathChoice::NotApplicable;
    };
    choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
        observation,
        lane,
        offset,
        payload_bytes,
        cursor,
        avoid_instances,
        path_flights,
        request_state,
    })
}

pub(super) fn choose_bulk_relay_path_for_extent_avoiding(
    request: BulkRelayPathRequest<'_>,
) -> BulkRelayPathChoice {
    let BulkRelayPathRequest {
        observation,
        lane,
        offset,
        payload_bytes,
        cursor,
        avoid_instances,
        path_flights,
        request_state,
    } = request;
    let stream_id = observation.stream_id;
    let paths = observation.paths.as_slice();
    let mux_limits = observation.mux_limits;
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = stream_id;
    if !lane.is_bulk() || payload_bytes == 0 {
        return BulkRelayPathChoice::NotApplicable;
    }
    let normal_bulk_send = avoid_instances.is_empty();
    if paths.len() <= 1 {
        if normal_bulk_send
            && let Some(flights) = path_flights
            && let Some(owner) = flights.oldest_lower_flight_owner_before_offset(offset)
            && paths.first().is_none_or(|path| path.key() != owner)
        {
            return BulkRelayPathChoice::Blocked;
        }
        return BulkRelayPathChoice::NotApplicable;
    }
    #[cfg(feature = "lab-diagnostics")]
    if normal_bulk_send {
        static PRECHECK_TRACE_COUNT: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let count = PRECHECK_TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if count < 16 || count.is_multiple_of(4096) {
            lab_diagnostic(
                "request_startup_selection",
                format_args!(
                    "phase=bulk_precheck stream_id={} paths={}",
                    stream_id.0,
                    paths.len(),
                ),
            );
        }
    }
    let mut admitted_bulk_keys = if normal_bulk_send {
        globally_admitted_bulk_keys(observation, payload_bytes)
    } else {
        SmallVec::new()
    };
    #[cfg(feature = "lab-diagnostics")]
    let global_admitted_bulk_keys = admitted_bulk_keys.clone();
    if let Some(request_state) = request_state {
        admitted_bulk_keys.retain(|key| {
            paths.iter().any(|path| {
                let state = request_state.path_state(path.instance());
                path.key() == *key
                    && ((state.is_some_and(RequestPathState::capacity_admitted)
                        && (key.underlay != UnderlayProtocol::Tcp
                            || state.is_some_and(RequestPathState::ack_clock_proven)))
                        || path.has_bulk_model_evidence
                        || request_path_is_validated(path))
            })
        });
    }
    if normal_bulk_send {
        // A completed MPP Data ACK sample proves this flow used the exact path.
        // Native TCP/QUIC control still supplies transport rate and send credit.
        for path in paths
            .iter()
            .filter(|path| request_path_has_exact_flow_local_delivery_evidence(path, request_state))
        {
            let key = path.key();
            if !admitted_bulk_keys.contains(&key) {
                admitted_bulk_keys.push(key);
            }
        }
    }
    let lower_flight_owner = if normal_bulk_send {
        path_flights.and_then(|flights| flights.oldest_lower_flight_owner_before_offset(offset))
    } else {
        None
    };
    // The exact connection-level flight ledger supplies the only ordering
    // reference. Attachment position and insertion order carry no scheduling weight.
    let reference_key =
        lower_flight_owner.filter(|owner| paths.iter().any(|path| path.key() == *owner));
    if normal_bulk_send && reference_key.is_none() {
        return match choose_observed_ordinary_data_path(
            observation,
            lane,
            payload_bytes,
            cursor,
            avoid_instances,
        ) {
            ObservedOrdinaryPathChoice::Selected(instance) => {
                BulkRelayPathChoice::Selected(instance)
            }
            ObservedOrdinaryPathChoice::Blocked => BulkRelayPathChoice::Blocked,
            ObservedOrdinaryPathChoice::NoLivePath => BulkRelayPathChoice::NotApplicable,
        };
    }
    let lower_owner_cross_path_debt = if normal_bulk_send {
        lower_flight_owner
            .and_then(|owner| {
                path_flights.map(|flights| flights.ordering_debt_bytes_before_offset(owner, offset))
            })
            .unwrap_or(0)
    } else {
        0
    };
    let restrict_to_admitted = normal_bulk_send
        && paths
            .iter()
            .any(|path| admitted_bulk_keys.contains(&path.key()));
    let lead = if normal_bulk_send {
        choose_admissible_relay_bulk_lead(RelayBulkLeadRequest {
            observation,
            lane,
            payload_bytes,
            reference_key,
            admitted_bulk_keys: &admitted_bulk_keys,
            restrict_to_admitted,
            lower_flight_owner,
            lower_owner_cross_path_debt,
            request_state,
        })
    } else {
        None
    };
    let measurement_transaction_candidate = normal_bulk_send
        .then(|| {
            reference_key.and_then(|reference_key| {
                observed_request_ack_clock_measurement_transaction(
                    paths,
                    reference_key,
                    request_state,
                )
            })
        })
        .flatten();
    let mut measurement_reference_fence = None;
    if let Some(candidate) = measurement_transaction_candidate {
        let reference_key =
            reference_key.expect("a live measurement transaction has an attached reference path");
        let mut allowed_lower_owners = vec![reference_key];
        if request_state.is_some_and(|request| {
            matches!(
                request.operation,
                Some(RequestAckClockOperation::Owner { .. })
            )
        }) && !allowed_lower_owners.contains(&candidate.key)
        {
            allowed_lower_owners.push(candidate.key);
        }
        let foreign_optional_owner = path_flights.is_some_and(|flights| {
            flights.has_foreign_original_transmission_before_offset(offset, &allowed_lower_owners)
        });
        if !foreign_optional_owner
            && let Some(choice) = choose_request_ack_clock_measurement_with_rates(
                observation,
                lane,
                offset,
                payload_bytes,
                cursor,
                Some(reference_key),
                path_flights,
                request_state,
            )
        {
            return choice;
        }
        // A begun TCP measurement transaction preempts ordinary scheduling.
        // Its reference path still passes completion and reorder gates below.
        measurement_reference_fence = Some(candidate);
    }
    if measurement_transaction_candidate.is_none()
        && normal_bulk_send
        && reference_key.is_some()
        && let Some(choice) = choose_request_ack_clock_measurement_with_rates(
            observation,
            lane,
            offset,
            payload_bytes,
            cursor,
            reference_key,
            path_flights,
            request_state,
        )
    {
        if let BulkRelayPathChoice::SelectedAckClockMeasurement { candidate, .. } = choice
            && let Some(reference_key) = reference_key
            && path_flights.is_some_and(|flights| {
                flights.has_foreign_original_transmission_before_offset(offset, &[reference_key])
            })
        {
            // The candidate passed every existing entry gate; defer only its
            // ownership commit until prior optional work leaves the frontier.
            measurement_reference_fence = Some(candidate);
        } else {
            return choice;
        }
    }
    if normal_bulk_send && lead.is_none() {
        if measurement_reference_fence.is_some() {
            return BulkRelayPathChoice::Blocked;
        }
        if lower_flight_owner.is_none()
            && let Some(reference_key) = reference_key
            && let Some((position, _)) = paths
                .iter()
                .enumerate()
                .find(|(_, path)| path.key() == reference_key && path.can_enqueue_frame)
        {
            // First owner bytes establish the lower-frontier path. Before that
            // frontier exists, ordinary metric ranking remains authoritative.
            return BulkRelayPathChoice::Selected(paths[position].instance());
        }
        return BulkRelayPathChoice::Blocked;
    }
    let lead_key = lead.map(|lead| lead.key);
    let lead_baseline = lead.map(|lead| (lead.snapshot, lead.eta_ms));
    let completion_backlog_bytes = path_flights
        .map(|flights| flights.original_transmission_bytes_before_offset(offset))
        .unwrap_or(0);
    let mut best: Option<(usize, f64, usize, PathSnapshot)> = None;
    let mut old_lead_candidate: Option<(usize, f64, PathSnapshot)> = None;
    #[cfg(feature = "lab-diagnostics")]
    let mut best_diagnostics: Option<BulkRelayCandidateDiagnostics> = None;
    #[cfg(feature = "lab-diagnostics")]
    let mut best_flow_local_shadow: Option<RequestFlowLocalBulkCandidate> = None;
    for (position, path) in paths.iter().enumerate() {
        let key = path.key();
        if measurement_reference_fence.is_some() && Some(key) != reference_key {
            continue;
        }
        #[cfg(feature = "lab-diagnostics")]
        let mut flow_local_shadow_gate = None;
        #[cfg(feature = "lab-diagnostics")]
        let exact_flow_local_candidate =
            request_path_has_exact_flow_local_delivery_evidence(path, request_state);
        if normal_bulk_send && Some(key) != lower_flight_owner {
            let validated_path = request_path_is_validated(path);
            let is_uncapacity_admitted = request_state.is_some_and(|request| {
                !request
                    .path_state(path.instance())
                    .is_some_and(RequestPathState::capacity_admitted)
            });
            let lacks_tcp_capacity_proof = key.underlay == UnderlayProtocol::Tcp
                && request_state.is_some_and(|request| {
                    !request
                        .path_state(path.instance())
                        .is_some_and(RequestPathState::ack_clock_proven)
                });
            if (is_uncapacity_admitted || lacks_tcp_capacity_proof)
                && !path.has_bulk_model_evidence
                && !validated_path
            {
                #[cfg(feature = "lab-diagnostics")]
                log_bulk_relay_candidate_decision(
                    BulkRelayCandidateDiagnostics::skipped(
                        stream_id,
                        lane,
                        key,
                        lead_key,
                        payload_bytes,
                    ),
                    false,
                    if is_uncapacity_admitted {
                        "capacity_unproven"
                    } else {
                        "tcp_ack_clock_unproven"
                    },
                );
                continue;
            }
        }
        if normal_bulk_send && !path.can_enqueue_frame {
            #[cfg(feature = "lab-diagnostics")]
            log_bulk_relay_candidate_decision(
                BulkRelayCandidateDiagnostics::skipped(
                    stream_id,
                    lane,
                    key,
                    lead_key,
                    payload_bytes,
                ),
                false,
                "carrier_credit",
            );
            continue;
        }
        if avoid_instances.contains(&path.instance())
            && paths
                .iter()
                .any(|path| !avoid_instances.contains(&path.instance()))
        {
            #[cfg(feature = "lab-diagnostics")]
            log_bulk_relay_candidate_decision(
                BulkRelayCandidateDiagnostics::skipped(
                    stream_id,
                    lane,
                    key,
                    lead_key,
                    payload_bytes,
                ),
                false,
                "avoid_previous_path",
            );
            continue;
        }
        if normal_bulk_send {
            let owns_lower_frontier = lower_flight_owner == Some(key);
            if restrict_to_admitted {
                if !owns_lower_frontier && !admitted_bulk_keys.contains(&key) {
                    #[cfg(feature = "lab-diagnostics")]
                    log_bulk_relay_candidate_decision(
                        BulkRelayCandidateDiagnostics::skipped(
                            stream_id,
                            lane,
                            key,
                            lead_key,
                            payload_bytes,
                        ),
                        false,
                        "not_globally_admitted",
                    );
                    #[cfg(feature = "lab-diagnostics")]
                    {
                        if exact_flow_local_candidate {
                            flow_local_shadow_gate = Some("not_globally_admitted");
                        } else {
                            continue;
                        }
                    }
                    #[cfg(not(feature = "lab-diagnostics"))]
                    continue;
                }
            } else if !owns_lower_frontier && Some(key) != reference_key {
                #[cfg(feature = "lab-diagnostics")]
                log_bulk_relay_candidate_decision(
                    BulkRelayCandidateDiagnostics::skipped(
                        stream_id,
                        lane,
                        key,
                        lead_key,
                        payload_bytes,
                    ),
                    false,
                    "unproven_additional_path",
                );
                #[cfg(feature = "lab-diagnostics")]
                {
                    if exact_flow_local_candidate {
                        flow_local_shadow_gate = Some("unproven_additional_path");
                    } else {
                        continue;
                    }
                }
                #[cfg(not(feature = "lab-diagnostics"))]
                continue;
            }
        }
        if normal_bulk_send
            && Some(key) != reference_key
            && lower_flight_owner != Some(key)
            && !(lower_flight_owner.is_none()
                && restrict_to_admitted
                && admitted_bulk_keys.contains(&key))
            && !(path.has_bulk_model_evidence
                || request_path_has_exact_flow_local_delivery_evidence(path, request_state)
                || request_path_is_validated(path))
        {
            #[cfg(feature = "lab-diagnostics")]
            if flow_local_shadow_gate.is_none() {
                log_bulk_relay_candidate_decision(
                    BulkRelayCandidateDiagnostics::skipped(
                        stream_id,
                        lane,
                        key,
                        lead_key,
                        payload_bytes,
                    ),
                    false,
                    "no_sender_evidence",
                );
            }
            #[cfg(feature = "lab-diagnostics")]
            if flow_local_shadow_gate.is_none() && exact_flow_local_candidate {
                flow_local_shadow_gate = Some("no_sender_evidence");
            } else if flow_local_shadow_gate.is_none() {
                continue;
            }
            #[cfg(not(feature = "lab-diagnostics"))]
            continue;
        }
        let Some(snapshot) = relay_path_snapshot_for_bulk_choice(
            observation,
            path.instance(),
            reference_key,
            request_state,
            path.load_owned,
        ) else {
            #[cfg(feature = "lab-diagnostics")]
            if flow_local_shadow_gate.is_none() {
                log_bulk_relay_candidate_decision(
                    BulkRelayCandidateDiagnostics::skipped(
                        stream_id,
                        lane,
                        key,
                        lead_key,
                        payload_bytes,
                    ),
                    false,
                    "no_path_snapshot",
                );
            }
            #[cfg(feature = "lab-diagnostics")]
            if let Some(initial_gate) = flow_local_shadow_gate {
                log_request_flow_local_admission_shadow(
                    BulkRelayCandidateDiagnostics::skipped(
                        stream_id,
                        lane,
                        key,
                        lead_key,
                        payload_bytes,
                    ),
                    path.instance(),
                    initial_gate,
                    "no_path_snapshot",
                    &global_admitted_bulk_keys,
                    &admitted_bulk_keys,
                    request_state
                        .and_then(|request| request.path_state(path.instance()))
                        .and_then(RequestPathState::per_flow_rate),
                );
            }
            continue;
        };
        let scoring_payload_bytes = if lane.is_bulk() {
            bulk_scheduling_horizon_bytes(payload_bytes, mux_limits)
        } else {
            payload_bytes
        };
        let Some(score) = scheduler::score_path(snapshot, lane, scoring_payload_bytes) else {
            #[cfg(feature = "lab-diagnostics")]
            if flow_local_shadow_gate.is_none() {
                log_bulk_relay_candidate_decision(
                    BulkRelayCandidateDiagnostics {
                        stream_id,
                        lane,
                        key,
                        lead_key,
                        position: None,
                        eta_ms: None,
                        best_eta_ms: None,
                        completion_horizon_ms: None,
                        stream_ordering_debt_bytes: 0,
                        payload_bytes,
                        scoring_payload_bytes: Some(scoring_payload_bytes),
                        scoring_class: Some(request_scoring_class(lane)),
                        snapshot: Some(snapshot),
                    },
                    false,
                    "no_path_score",
                );
            }
            #[cfg(feature = "lab-diagnostics")]
            if let Some(initial_gate) = flow_local_shadow_gate {
                log_request_flow_local_admission_shadow(
                    BulkRelayCandidateDiagnostics {
                        stream_id,
                        lane,
                        key,
                        lead_key,
                        position: None,
                        eta_ms: None,
                        best_eta_ms: None,
                        completion_horizon_ms: None,
                        stream_ordering_debt_bytes: 0,
                        payload_bytes,
                        scoring_payload_bytes: Some(scoring_payload_bytes),
                        scoring_class: Some(request_scoring_class(lane)),
                        snapshot: Some(snapshot),
                    },
                    path.instance(),
                    initial_gate,
                    "no_path_score",
                    &global_admitted_bulk_keys,
                    &admitted_bulk_keys,
                    request_state
                        .and_then(|request| request.path_state(path.instance()))
                        .and_then(RequestPathState::per_flow_rate),
                );
            }
            continue;
        };
        #[cfg(feature = "lab-diagnostics")]
        let mut candidate_diagnostics = None;
        if normal_bulk_send {
            let cross_path_ordering_debt = path_flights
                .map(|flights| flights.ordering_debt_bytes_before_offset(key, offset))
                .unwrap_or(0);
            let owns_lower_frontier = lower_flight_owner == Some(key);
            let position = if owns_lower_frontier {
                BulkCandidatePosition::ContiguousFrontier
            } else if Some(key) == lead_key && cross_path_ordering_debt == 0 {
                BulkCandidatePosition::FirstPath
            } else if lower_flight_owner.is_some() || lead_key.is_some() {
                BulkCandidatePosition::AdditionalPath
            } else {
                BulkCandidatePosition::FirstPath
            };
            let admission_ordering_debt = cross_path_ordering_debt;
            let (best_snapshot, best_eta_ms) =
                if position == BulkCandidatePosition::ContiguousFrontier {
                    (snapshot, score.eta_ms)
                } else {
                    lead_baseline.unwrap_or((snapshot, score.eta_ms))
                };
            #[cfg(feature = "lab-diagnostics")]
            {
                let completion_horizon_ms = bulk_completion_horizon_ms_with_ordering_debt(
                    best_snapshot,
                    best_eta_ms,
                    snapshot,
                    payload_bytes,
                    mux_limits,
                    admission_ordering_debt,
                );
                candidate_diagnostics = Some(BulkRelayCandidateDiagnostics {
                    stream_id,
                    lane,
                    key,
                    lead_key,
                    position: Some(position),
                    eta_ms: Some(score.eta_ms),
                    best_eta_ms: Some(best_eta_ms),
                    completion_horizon_ms: Some(completion_horizon_ms),
                    stream_ordering_debt_bytes: admission_ordering_debt,
                    payload_bytes,
                    scoring_payload_bytes: Some(scoring_payload_bytes),
                    scoring_class: Some(request_scoring_class(lane)),
                    snapshot: Some(snapshot),
                });
            }
            let ordering_suppression = bulk_candidate_admission_suppression_with_completion_backlog(
                BulkAdmissionCheck {
                    best_snapshot,
                    best_eta_ms,
                    candidate_snapshot: snapshot,
                    candidate_eta_ms: score.eta_ms,
                    payload_bytes,
                    mux_limits,
                    position,
                    stream_ordering_debt_bytes: admission_ordering_debt,
                },
                completion_backlog_bytes,
                path.has_bulk_model_evidence,
            );
            let measurement_reference_reservoir =
                measurement_reference_fence.is_some_and(|candidate| {
                    Some(key) == reference_key
                        && request_state.is_some_and(|request_state| {
                            path_flights.is_some_and(|flights| {
                                request_ack_clock_measurement_reference_reservoir_has_credit(
                                    flights,
                                    offset,
                                    candidate,
                                    request_state,
                                    payload_bytes,
                                    mux_limits,
                                )
                            })
                        })
                });
            if let Some(reason) = ordering_suppression
                && !(measurement_reference_reservoir
                    && matches!(reason, "ecf_no_completion_gain" | "completion_horizon"))
            {
                #[cfg(feature = "lab-diagnostics")]
                if let Some(diagnostics) = candidate_diagnostics {
                    if flow_local_shadow_gate.is_none() {
                        log_bulk_relay_candidate_decision(diagnostics, false, reason);
                    }
                    if let Some(initial_gate) = flow_local_shadow_gate {
                        log_request_flow_local_admission_shadow(
                            diagnostics,
                            path.instance(),
                            initial_gate,
                            reason,
                            &global_admitted_bulk_keys,
                            &admitted_bulk_keys,
                            request_state
                                .and_then(|request| request.path_state(path.instance()))
                                .and_then(RequestPathState::per_flow_rate),
                        );
                    }
                }
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = reason;
                continue;
            }
        }
        #[cfg(feature = "lab-diagnostics")]
        if flow_local_shadow_gate.is_none()
            && let Some(diagnostics) = candidate_diagnostics
        {
            log_bulk_relay_candidate_decision(diagnostics, false, "admissible");
        }
        #[cfg(feature = "lab-diagnostics")]
        if let Some(initial_gate) = flow_local_shadow_gate {
            let cursor_distance = cyclic_cursor_distance(position, cursor, paths.len());
            let diagnostics = candidate_diagnostics.unwrap_or(BulkRelayCandidateDiagnostics {
                stream_id,
                lane,
                key,
                lead_key,
                position: None,
                eta_ms: Some(score.eta_ms),
                best_eta_ms: Some(score.eta_ms),
                completion_horizon_ms: None,
                stream_ordering_debt_bytes: 0,
                payload_bytes,
                scoring_payload_bytes: Some(scoring_payload_bytes),
                scoring_class: Some(request_scoring_class(lane)),
                snapshot: Some(snapshot),
            });
            log_request_flow_local_admission_shadow(
                diagnostics,
                path.instance(),
                initial_gate,
                "admissible",
                &global_admitted_bulk_keys,
                &admitted_bulk_keys,
                request_state
                    .and_then(|request| request.path_state(path.instance()))
                    .and_then(RequestPathState::per_flow_rate),
            );
            let candidate = RequestFlowLocalBulkCandidate {
                eta_ms: score.eta_ms,
                cursor_distance,
                snapshot,
                diagnostics,
                instance: path.instance(),
                initial_gate,
                local_model: request_state
                    .and_then(|request| request.path_state(path.instance()))
                    .and_then(RequestPathState::per_flow_rate),
            };
            let replaces_shadow = best_flow_local_shadow.as_ref().is_none_or(|best| {
                score.eta_ms < best.eta_ms
                    || (score.eta_ms == best.eta_ms && cursor_distance < best.cursor_distance)
            });
            if replaces_shadow {
                best_flow_local_shadow = Some(candidate);
            }
            continue;
        }
        if normal_bulk_send && Some(key) == reference_key {
            old_lead_candidate = Some((position, score.eta_ms, snapshot));
        }
        let cursor_distance = cyclic_cursor_distance(position, cursor, paths.len());
        match best {
            None => {
                best = Some((position, score.eta_ms, cursor_distance, snapshot));
                #[cfg(feature = "lab-diagnostics")]
                {
                    best_diagnostics =
                        candidate_diagnostics.or(Some(BulkRelayCandidateDiagnostics {
                            stream_id,
                            lane,
                            key,
                            lead_key,
                            position: None,
                            eta_ms: Some(score.eta_ms),
                            best_eta_ms: Some(score.eta_ms),
                            completion_horizon_ms: None,
                            stream_ordering_debt_bytes: 0,
                            payload_bytes,
                            scoring_payload_bytes: Some(scoring_payload_bytes),
                            scoring_class: Some(request_scoring_class(lane)),
                            snapshot: Some(snapshot),
                        }));
                }
            }
            Some((_, best_eta, best_distance, best_snapshot)) => {
                let native_window_order =
                    compare_request_quic_native_window_utilization(snapshot, best_snapshot);
                if native_window_order == Ordering::Less
                    || (native_window_order == Ordering::Equal
                        && (score.eta_ms < best_eta
                            || (score.eta_ms == best_eta && cursor_distance < best_distance)))
                {
                    best = Some((position, score.eta_ms, cursor_distance, snapshot));
                    #[cfg(feature = "lab-diagnostics")]
                    {
                        best_diagnostics =
                            candidate_diagnostics.or(Some(BulkRelayCandidateDiagnostics {
                                stream_id,
                                lane,
                                key,
                                lead_key,
                                position: None,
                                eta_ms: Some(score.eta_ms),
                                best_eta_ms: Some(score.eta_ms),
                                completion_horizon_ms: None,
                                stream_ordering_debt_bytes: 0,
                                payload_bytes,
                                scoring_payload_bytes: Some(scoring_payload_bytes),
                                scoring_class: Some(request_scoring_class(lane)),
                                snapshot: Some(snapshot),
                            }));
                    }
                }
            }
        }
    }
    #[cfg(feature = "lab-diagnostics")]
    if let Some(shadow) = best_flow_local_shadow {
        let shadow_is_best = best
            .as_ref()
            .is_none_or(|(_, best_eta_ms, best_distance, _)| {
                shadow.eta_ms < *best_eta_ms
                    || (shadow.eta_ms == *best_eta_ms && shadow.cursor_distance < *best_distance)
            });
        let owner_hysteresis_keeps_lead = shadow_is_best
            && old_lead_candidate
                .as_ref()
                .is_some_and(|(_, old_eta_ms, old_snapshot)| {
                    relay_path_within_adaptive_lead_hysteresis(
                        *old_eta_ms,
                        *old_snapshot,
                        shadow.eta_ms,
                        shadow.snapshot,
                        payload_bytes,
                    )
                });
        let outcome = if !shadow_is_best {
            "admitted_not_best"
        } else if owner_hysteresis_keeps_lead {
            "admitted_owner_hysteresis"
        } else {
            "would_select"
        };
        log_request_flow_local_admission_shadow(
            shadow.diagnostics,
            shadow.instance,
            shadow.initial_gate,
            outcome,
            &global_admitted_bulk_keys,
            &admitted_bulk_keys,
            shadow.local_model,
        );
    }
    if let Some((best_position, best_eta_ms, _, best_snapshot)) = best {
        let position = old_lead_candidate
            .filter(|(_, old_eta_ms, old_snapshot)| {
                compare_request_quic_native_window_utilization(best_snapshot, *old_snapshot)
                    != Ordering::Less
                    && relay_path_within_adaptive_lead_hysteresis(
                        *old_eta_ms,
                        *old_snapshot,
                        best_eta_ms,
                        best_snapshot,
                        payload_bytes,
                    )
            })
            .map(|(position, _, _)| position)
            .unwrap_or(best_position);
        #[cfg(feature = "lab-diagnostics")]
        if let Some(diagnostics) = best_diagnostics {
            log_bulk_relay_candidate_decision(diagnostics, true, "selected");
        }
        if let Some(candidate) = measurement_reference_fence {
            debug_assert_eq!(Some(paths[position].key()), reference_key);
            return BulkRelayPathChoice::SelectedAckClockMeasurementFence {
                reference: paths[position].instance(),
                candidate,
            };
        }
        return BulkRelayPathChoice::Selected(paths[position].instance());
    }
    if !normal_bulk_send {
        return BulkRelayPathChoice::NotApplicable;
    }
    BulkRelayPathChoice::Blocked
}

fn relay_path_within_adaptive_lead_hysteresis(
    old_eta_ms: f64,
    old_snapshot: PathSnapshot,
    best_eta_ms: f64,
    best_snapshot: PathSnapshot,
    payload_bytes: usize,
) -> bool {
    path_within_adaptive_lead_hysteresis(
        old_eta_ms,
        old_snapshot,
        best_eta_ms,
        best_snapshot,
        payload_bytes,
    )
}

struct RelayBulkLeadRequest<'a> {
    observation: &'a RequestRelaySchedulingObservation,
    lane: TrafficClass,
    payload_bytes: usize,
    reference_key: Option<RelayPathKey>,
    admitted_bulk_keys: &'a [RelayPathKey],
    restrict_to_admitted: bool,
    lower_flight_owner: Option<RelayPathKey>,
    lower_owner_cross_path_debt: u64,
    request_state: Option<RequestSchedulingState<'a>>,
}

fn choose_admissible_relay_bulk_lead(request: RelayBulkLeadRequest<'_>) -> Option<RelayBulkLead> {
    let RelayBulkLeadRequest {
        observation,
        lane,
        payload_bytes,
        reference_key,
        admitted_bulk_keys,
        restrict_to_admitted,
        lower_flight_owner,
        lower_owner_cross_path_debt,
        request_state,
    } = request;
    let paths = observation.paths.as_slice();
    paths
        .iter()
        .filter(|path| {
            // A congestion-window-blocked owner still defines the ordered
            // completion baseline. Send eligibility is checked separately in
            // the candidate loop so another path can make forward progress.
            lower_flight_owner == Some(path.key()) || path.can_enqueue_frame
        })
        .filter(|path| {
            let key = path.key();
            if let Some(owner) = lower_flight_owner {
                return key == owner;
            }
            if restrict_to_admitted {
                admitted_bulk_keys.contains(&key)
            } else {
                Some(key) == reference_key
            }
        })
        .filter(|path| {
            let key = path.key();
            Some(key) == reference_key
                || lower_flight_owner == Some(key)
                || (lower_flight_owner.is_none()
                    && restrict_to_admitted
                    && admitted_bulk_keys.contains(&key))
                || path.has_bulk_model_evidence
        })
        .filter_map(|path| {
            let key = path.key();
            let (snapshot, eta_ms) = scored_relay_path_snapshot_for_bulk_choice(
                observation,
                path.instance(),
                reference_key,
                lane,
                payload_bytes,
                request_state,
                path.load_owned,
            )?;
            let stream_ordering_debt_bytes = if lower_flight_owner == Some(key) {
                lower_owner_cross_path_debt
            } else {
                0
            };
            let suppression =
                bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                    best_snapshot: snapshot,
                    best_eta_ms: eta_ms,
                    candidate_snapshot: snapshot,
                    candidate_eta_ms: eta_ms,
                    payload_bytes,
                    mux_limits: observation.mux_limits,
                    position: BulkCandidatePosition::FirstPath,
                    stream_ordering_debt_bytes,
                });
            // The oldest DSN owner remains the completion baseline while its
            // path drains. Its own admission gate decides whether it may send
            // again in the candidate loop; a full owner must not hide service
            // available on another path.
            (lower_flight_owner == Some(key) || suppression.is_none()).then_some(RelayBulkLead {
                key,
                snapshot,
                eta_ms,
            })
        })
        .min_by(|left, right| {
            scheduler::path_is_backup(left.snapshot)
                .cmp(&scheduler::path_is_backup(right.snapshot))
                .then_with(|| left.eta_ms.total_cmp(&right.eta_ms))
                .then_with(|| observation.compare_path_keys(left.key, right.key))
        })
}

fn scored_relay_path_snapshot_for_bulk_choice(
    observation: &RequestRelaySchedulingObservation,
    instance: RelayPathInstance,
    reference_key: Option<RelayPathKey>,
    lane: TrafficClass,
    payload_bytes: usize,
    request_state: Option<RequestSchedulingState<'_>>,
    current_flow_owns_path: bool,
) -> Option<(PathSnapshot, f64)> {
    let snapshot = relay_path_snapshot_for_bulk_choice(
        observation,
        instance,
        reference_key,
        request_state,
        current_flow_owns_path,
    )?;
    let scoring_payload_bytes = if lane.is_bulk() {
        bulk_scheduling_horizon_bytes(payload_bytes, observation.mux_limits)
    } else {
        payload_bytes
    };
    let score = scheduler::score_path(snapshot, lane, scoring_payload_bytes)?;
    Some((snapshot, score.eta_ms))
}

fn relay_path_snapshot_for_bulk_choice(
    observation: &RequestRelaySchedulingObservation,
    instance: RelayPathInstance,
    reference_key: Option<RelayPathKey>,
    request_state: Option<RequestSchedulingState<'_>>,
    current_flow_owns_path: bool,
) -> Option<PathSnapshot> {
    let path = observation
        .paths
        .iter()
        .find(|path| path.instance == instance)?;
    let mut snapshot = path.shared_snapshot?;
    if instance.key.underlay == UnderlayProtocol::Tcp {
        let tcp = path.tcp?;
        let startup = tcp.startup_snapshot;
        let durable_shared_capacity_rate_bps = (path.has_bulk_model_evidence
            && snapshot.rate_scope == PathRateScope::PathCapacity)
            .then_some(snapshot.delivery_rate_bps);
        let local_model = request_state
            .and_then(|request| request.path_state(instance))
            .and_then(RequestPathState::per_flow_rate);
        if local_model.is_none() && snapshot.rate_scope == PathRateScope::PerFlowGoodput {
            // Shared TCP product samples belong to whichever logical flow
            // produced them. Until this flow has exact local evidence, fall
            // back to the carrier-capacity prior so active-flow sharing remains
            // visible instead of borrowing another flow's undivided goodput.
            snapshot.delivery_rate_bps = startup.delivery_rate_bps;
            snapshot.pacing_rate_bps = startup.pacing_rate_bps;
            snapshot.rate_scope = PathRateScope::PathCapacity;
            snapshot.product_progress_rate_bps = None;
            snapshot.has_durable_product_progress = false;
            snapshot.carrier_inflight_limit_bytes = startup.carrier_inflight_limit_bytes;
            snapshot.confidence = startup.confidence;
        }
        if let Some(model) = local_model {
            // A product ACK clock measures this logical flow's delivered share.
            // Keep it local and do not combine it with a carrier-capacity pacing
            // estimate or divide it by the shared active-flow count again.
            let mature = product_delivery_samples_override_startup_prior(model.delivery_samples);
            // This preserves the existing rate-prior gate exactly. Other path
            // metadata is intentionally not collapsed into this observation.
            let rate_hint_unknown = tcp.rate_hint_unknown;
            let reference_exploration_rate_bps = (Some(instance.key) != reference_key
                && rate_hint_unknown)
                .then(|| {
                    reference_key.and_then(|active| {
                        request_state
                            .and_then(|request| {
                                request.path_states.iter().find_map(|(instance, state)| {
                                    (instance.key == active)
                                        .then(|| state.per_flow_rate())
                                        .flatten()
                                        .map(|model| model.rate_bps)
                                })
                            })
                            .or_else(|| {
                                // Before this flow has a continuous delivery model,
                                // the exact reference-path model is still valid
                                // provisional scheduling credit. It never becomes
                                // candidate proof and is used only for endpoint-only
                                // candidates.
                                observation
                                    .path_by_key(active)
                                    .and_then(|path| path.shared_snapshot)
                                    .map(|snapshot| snapshot.delivery_rate_bps)
                            })
                    })
                })
                .flatten()
                .unwrap_or(0.0);
            // A durable same-carrier capacity observation has stronger
            // provenance than an immature per-flow Data ACK estimate.
            let provisional_rate_bps = startup
                .delivery_rate_bps
                .max(durable_shared_capacity_rate_bps.unwrap_or(0.0))
                .max(reference_exploration_rate_bps);
            let retain_capacity_prior = !mature && provisional_rate_bps > model.rate_bps;
            snapshot.delivery_rate_bps = if retain_capacity_prior {
                provisional_rate_bps
            } else {
                model.rate_bps
            }
            .max(1.0);
            snapshot.pacing_rate_bps = snapshot.delivery_rate_bps;
            snapshot.rate_scope = if retain_capacity_prior {
                PathRateScope::PathCapacity
            } else {
                PathRateScope::PerFlowGoodput
            };
            if Some(instance.key) != reference_key && retain_capacity_prior {
                // Endpoint-only paths have no configured capacity hint. Once
                // exact ownership is proven, borrow only the current reference
                // rate as bounded exploration credit so the candidate TCP can
                // leave slow start. The candidate's own tenth exact sample
                // replaces this prior; kernel cwnd and the product envelope
                // remain hard limits throughout.
                let provisional_pipe = bulk_candidate_pipe_bytes(snapshot).min(
                    reliable_ack_clock_measurement_ceiling_bytes(observation.mux_limits),
                );
                snapshot.data_level_limit_bytes = snapshot
                    .data_level_limit_bytes
                    .max(provisional_pipe)
                    .max(PATH_OPEN_SCORE_BYTES as u64);
            } else if Some(instance.key) != reference_key && mature {
                // A configured capacity prior may keep an underfed candidate in the
                // ranking set. Only a mature continuous per-flow ACK model may
                // shrink its initial pipe; one bounded proof sample is expected
                // to be app-limited while the TCP carrier is still ramping.
                let mut observed = snapshot;
                observed.delivery_rate_bps = model.rate_bps.max(1.0);
                observed.pacing_rate_bps = observed.delivery_rate_bps;
                observed.rate_scope = PathRateScope::PerFlowGoodput;
                let observed_pipe_bytes = data_level_service_window_bytes(
                    observed,
                    TrafficClass::Throughput,
                    observation.mux_limits,
                )
                .ceil()
                .max(PATH_OPEN_SCORE_BYTES as f64) as u64;
                snapshot.data_level_limit_bytes = if snapshot.data_level_limit_bytes > 0 {
                    snapshot.data_level_limit_bytes.min(observed_pipe_bytes)
                } else {
                    observed_pipe_bytes
                }
                .max(PATH_OPEN_SCORE_BYTES as u64);
            }
        }
    }
    if Some(instance.key) != reference_key && !current_flow_owns_path {
        snapshot.active_flows = snapshot.active_flows.saturating_add(1);
    }
    // Native TCP/QUIC credit supplies carrier feed geometry in both
    // directions. The shared Data Sequence and reorder windows remain the
    // connection-level admission authority above this path-local window.
    snapshot.data_level_limit_bytes = u64::try_from(adaptive_reliable_relay_inflight_bytes(
        Some(snapshot),
        TrafficClass::Throughput,
        observation.mux_limits,
    ))
    .unwrap_or(u64::MAX);
    Some(snapshot)
}

#[cfg(test)]
#[path = "tests_scheduling.rs"]
mod tests;
