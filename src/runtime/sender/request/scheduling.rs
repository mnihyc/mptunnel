//! Request-direction path scheduling.
//!
//! This module ranks immutable path and request evidence. The serialized
//! sender owns preparation and applies the returned exact-instance decision.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{lab_diagnostic, lab_diagnostic_event_enabled};
use crate::model::ack_clock::{
    reliable_ack_clock_calibration_ceiling_bytes,
    reliable_request_ack_clock_calibration_target_bytes,
};
#[cfg(feature = "lab-diagnostics")]
use crate::model::admission::bulk_completion_horizon_ms_with_ordering_debt;
use crate::model::admission::{
    BulkAdmissionCheck, BulkAdmissionRole, BulkPathCandidate, bulk_additional_admission_role,
    bulk_candidate_admission_suppression_with_ordering_debt, bulk_candidate_pipe_bytes,
    bulk_service_feed_reservoir_payload_bytes, bulk_service_horizon_payload_bytes,
    bulk_service_product_envelope_payload_bytes, bulk_striping_admitted_candidates,
};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, bbr_inflight_target_bytes,
    product_delivery_samples_override_startup_prior, reliable_subflow_startup_sample_limit_bytes,
};
use crate::model::multipath::{
    FlowSubflowSet, PathAdmissionDecision, PathRuntimeRole, SubflowAdmissionInput,
};
use crate::model::path::{
    RelayPathInstance, RelayPathKey, RelayPathPlacement, RelayPathProofEpoch,
};
#[cfg(feature = "lab-diagnostics")]
use crate::model::request::evidence::RequestPerFlowRateModel;
use crate::mux::MuxLimits;
use crate::protocol::frame::reliable_stream_frame_extent;
use crate::protocol::{Frame, StreamId, UnderlayProtocol};
use crate::runtime::stream::request::{
    RequestAckClockOperation, RequestFlightLedger, RequestSubflowState, RequestSubflows,
};
use crate::scheduler::{
    self, FlowLane, PathRateScope, PathSnapshot, SchedulerPolicy, cyclic_cursor_distance,
    path_within_adaptive_lead_hysteresis,
};
use smallvec::SmallVec;
use std::collections::HashSet;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BulkRelayPathChoice {
    Selected(RelayPathInstance),
    SelectedStartupSubflow {
        service: RelayPathInstance,
        candidate: RelayPathInstance,
        proof: RelayPathProofEpoch,
    },
    SelectedAckClockCalibration {
        candidate: RelayPathInstance,
        target_bytes: u64,
        proof: RelayPathProofEpoch,
    },
    SelectedAckClockCalibrationFence {
        service: RelayPathInstance,
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
    pub(super) placement: RelayPathPlacement,
    pub(super) can_enqueue_frame: bool,
    pub(super) can_enqueue_stream_lane: bool,
    pub(super) load_owned: bool,
    pub(super) shared_snapshot: Option<PathSnapshot>,
    pub(super) tcp: Option<RequestRelayTcpPathObservation>,
    pub(super) has_bulk_model_evidence: bool,
    pub(super) fresh_proof: Option<RelayPathProofEpoch>,
    pub(super) config_ordinal: usize,
}

/// TCP-only scheduling priors; QUIC paths cannot construct this extension.
#[derive(Debug, Clone, Copy)]
pub(super) struct RequestRelayTcpPathObservation {
    pub(super) startup_snapshot: PathSnapshot,
    pub(super) rate_hint_unknown: bool,
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
    lane: FlowLane,
    payload_bytes: usize,
    cursor: usize,
    avoid_keys: &[RelayPathKey],
) -> ObservedOrdinaryPathChoice {
    let has_active_path = observation
        .paths
        .iter()
        .any(|path| path.placement == RelayPathPlacement::Active);
    let allowed = |path: &&RequestRelayPathObservation| {
        (!has_active_path || path.placement == RelayPathPlacement::Active)
            && path.can_enqueue_stream_lane
    };
    let choose = |prefer_avoiding: bool| {
        observation
            .paths
            .iter()
            .enumerate()
            .filter(|(_, path)| !prefer_avoiding || !avoid_keys.contains(&path.instance.key))
            .filter(|(_, path)| allowed(path))
            .filter_map(|(position, path)| {
                let snapshot = path.shared_snapshot?;
                let score = scheduler::score_path(
                    snapshot,
                    lane,
                    payload_bytes,
                    SchedulerPolicy::default(),
                )?;
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
    if let Some(instance) = choose(true).or_else(|| choose(false)) {
        return ObservedOrdinaryPathChoice::Selected(instance);
    }
    let capacity_fallback = observation
        .paths
        .iter()
        .filter(&allowed)
        .find(|path| !avoid_keys.contains(&path.instance.key))
        .or_else(|| observation.paths.iter().find(allowed));
    if let Some(path) = capacity_fallback {
        return ObservedOrdinaryPathChoice::Selected(path.instance);
    }
    if observation
        .paths
        .iter()
        .any(|path| !has_active_path || path.placement == RelayPathPlacement::Active)
    {
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
    pub(super) active_tcp_service_bulk_flows: u32,
    pub(super) latency_pressure: bool,
}

impl RequestRelaySchedulingObservation {
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
    pub(super) subflows: &'a RequestSubflows,
}

impl<'a> RequestSchedulingState<'a> {
    fn transaction_candidate(self) -> Option<RelayPathInstance> {
        self.operation.map(RequestAckClockOperation::candidate)
    }

    fn subflow(self, instance: RelayPathInstance) -> Option<&'a RequestSubflowState> {
        self.subflows.get(instance)
    }

    #[cfg(feature = "lab-diagnostics")]
    fn graduated_count(self) -> usize {
        self.subflows
            .iter()
            .filter(|(_, state)| state.graduated())
            .count()
    }
}

fn observed_request_ack_clock_calibration_transaction(
    paths: &[RequestRelayPathObservation],
    service_key: RelayPathKey,
    request_state: Option<RequestSchedulingState<'_>>,
) -> Option<RelayPathInstance> {
    let request_state = request_state?;
    let service = paths
        .iter()
        .find(|path| path.key() == service_key && path.placement == RelayPathPlacement::Active)?;
    let (candidate, owner_target_spent) = match request_state.operation? {
        RequestAckClockOperation::Owner {
            candidate,
            target_bytes,
        } => (
            candidate,
            request_state
                .subflow(candidate)
                .and_then(RequestSubflowState::ack_clock_calibration_bytes)
                .is_some_and(|spent| spent >= target_bytes),
        ),
        RequestAckClockOperation::Pending {
            service: owner,
            candidate,
        } => ((owner == service.instance()).then_some(candidate)?, false),
    };
    let candidate_path = paths.iter().find(|path| {
        path.instance() == candidate
            && path.placement == RelayPathPlacement::Validation
            && path.key().underlay == UnderlayProtocol::Tcp
            && path.key().underlay == service_key.underlay
    })?;
    // Fresh carrier evidence authorizes entry and target emission. Once the
    // fixed target is fully committed, only exact product ACK or a real path
    // lifecycle change may retire its AwaitingAck transaction.
    let candidate_state = request_state.subflow(candidate)?;
    // An offset-free TCP receipt supplies only the causal boundary for one
    // bounded product stage. It is never equivalent to product ACK proof.
    let authorized = owner_target_spent
        || candidate_state.ack_clock_first_window()
        || candidate_state.tcp_capacity_proven();
    (candidate_state.graduated() && !candidate_state.ack_clock_proven() && authorized)
        .then_some(candidate_path.instance())
}

pub(super) struct BulkRelayPathRequest<'a> {
    pub(super) observation: &'a RequestRelaySchedulingObservation,
    pub(super) lane: FlowLane,
    pub(super) offset: u64,
    pub(super) payload_bytes: usize,
    pub(super) cursor: usize,
    pub(super) avoid_keys: &'a [RelayPathKey],
    pub(super) path_flights: Option<&'a RequestFlightLedger>,
    pub(super) ordered_data_owner: Option<RelayPathKey>,
    pub(super) subflow_set: Option<&'a FlowSubflowSet<RelayPathInstance>>,
    pub(super) request_state: Option<RequestSchedulingState<'a>>,
    pub(super) attempted_subflows: Option<&'a HashSet<RelayPathInstance>>,
}

pub(super) struct BulkRelayFrameRequest<'a> {
    pub(super) observation: &'a RequestRelaySchedulingObservation,
    pub(super) lane: FlowLane,
    pub(super) frame: &'a Frame,
    pub(super) cursor: usize,
    pub(super) avoid_keys: &'a [RelayPathKey],
    pub(super) path_flights: Option<&'a RequestFlightLedger>,
    pub(super) ordered_data_owner: Option<RelayPathKey>,
    pub(super) subflow_set: Option<&'a FlowSubflowSet<RelayPathInstance>>,
    pub(super) request_state: Option<RequestSchedulingState<'a>>,
    pub(super) attempted_subflows: Option<&'a HashSet<RelayPathInstance>>,
}

#[derive(Debug, Clone, Copy)]
struct RelayBulkLead {
    key: RelayPathKey,
    snapshot: PathSnapshot,
    eta_ms: f64,
}

fn relay_path_runtime_role(
    key: RelayPathKey,
    active_key: Option<RelayPathKey>,
    lower_flight_owner: Option<RelayPathKey>,
    has_bulk_model_evidence: bool,
) -> PathRuntimeRole {
    if Some(key) == active_key || Some(key) == lower_flight_owner {
        PathRuntimeRole::Service
    } else if has_bulk_model_evidence {
        PathRuntimeRole::Subflow
    } else {
        PathRuntimeRole::Standby
    }
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

fn request_path_has_exact_flow_local_bulk_model(
    path: &RequestRelayPathObservation,
    request_state: Option<RequestSchedulingState<'_>>,
) -> bool {
    let instance = path.instance();
    let state = request_state.and_then(|request| request.subflow(instance));
    path.placement == RelayPathPlacement::Validation
        && instance.key.underlay == UnderlayProtocol::Tcp
        && state.is_some_and(RequestSubflowState::graduated)
        && state.is_some_and(RequestSubflowState::ack_clock_proven)
        && state.and_then(RequestSubflowState::per_flow_rate).is_some()
}

fn request_startup_product_envelope_bytes(payload_bytes: usize, mux_limits: MuxLimits) -> u64 {
    (mux_limits.max_path_flight_bytes as u64)
        .min(mux_limits.max_repair_bytes as u64)
        .min(mux_limits.max_reorder_bytes as u64)
        .min(mux_limits.max_stream_window_bytes)
        .max(payload_bytes as u64)
}

#[cfg(feature = "lab-diagnostics")]
fn request_scoring_class(lane: FlowLane) -> &'static str {
    if !lane.is_bulk() {
        "preemptible_quantum"
    } else {
        "bulk_horizon"
    }
}

fn request_ack_clock_calibration_service_reservoir_has_credit(
    flights: &RequestFlightLedger,
    offset: u64,
    candidate: RelayPathInstance,
    request_state: RequestSchedulingState<'_>,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> bool {
    let calibration_prefix = match request_state.operation {
        Some(RequestAckClockOperation::Owner {
            candidate: owner,
            target_bytes,
        }) if owner == candidate => target_bytes,
        _ => reliable_request_ack_clock_calibration_target_bytes(mux_limits),
    };
    let product_envelope = bulk_service_product_envelope_payload_bytes(payload_bytes, mux_limits);
    let reservoir = bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits)
        .saturating_add(usize::try_from(calibration_prefix).unwrap_or(usize::MAX))
        .min(product_envelope);
    let ordered_owner_debt =
        usize::try_from(flights.ordering_owner_bytes_before_offset(offset)).unwrap_or(usize::MAX);
    ordered_owner_debt.saturating_add(payload_bytes) <= reservoir
}

#[allow(clippy::too_many_arguments)]
fn choose_request_startup_subflow_with_rates(
    observation: &RequestRelaySchedulingObservation,
    lane: FlowLane,
    offset: u64,
    payload_bytes: usize,
    active_key: Option<RelayPathKey>,
    path_flights: Option<&RequestFlightLedger>,
    subflow_set: Option<&FlowSubflowSet<RelayPathInstance>>,
    request_state: Option<RequestSchedulingState<'_>>,
    attempted_subflows: Option<&HashSet<RelayPathInstance>>,
) -> Option<BulkRelayPathChoice> {
    let paths = observation.paths.as_slice();
    #[cfg(feature = "lab-diagnostics")]
    let stream_id = observation.stream_id;
    let service_key = active_key?;
    // TCP can turn bounded OwnerData into strict ACK-clock capacity evidence.
    // A finite QUIC burst stays app-limited and cannot prove native capacity;
    // using ordered product bytes for that purpose only creates HOL debt.
    if service_key.underlay != UnderlayProtocol::Tcp {
        return None;
    }
    let startup_owner = subflow_set.and_then(FlowSubflowSet::startup_owner_key);
    if startup_owner.is_none() && observation.active_tcp_service_bulk_flows < 2 {
        #[cfg(feature = "lab-diagnostics")]
        {
            static TRACE_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let count = TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if count < 16 || count % 4096 == 0 {
                lab_diagnostic(
                    "request_startup_selection",
                    format_args!(
                        "phase=contention_gate stream_id={} active_tcp_service_flows={}",
                        stream_id.0, observation.active_tcp_service_bulk_flows,
                    ),
                );
            }
        }
        // One logical upload does not provide contention to amortize ordered
        // TCP startup bytes. An exact existing owner still finishes after 2->1.
        return None;
    }
    let service = paths
        .iter()
        .find(|path| path.key() == service_key && path.placement == RelayPathPlacement::Active)?;
    let service_instance = service.instance();
    if request_state.is_some_and(|request| {
        !request
            .subflow(service_instance)
            .is_some_and(RequestSubflowState::rate_proven)
    }) {
        #[cfg(feature = "lab-diagnostics")]
        {
            static TRACE_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let count = TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if count < 16 || count % 4096 == 0 {
                lab_diagnostic(
                    "request_startup_selection",
                    format_args!(
                        "phase=service_unproven stream_id={} path_index={} instance_id={}",
                        stream_id.0, service.instance.key.index, service.instance.id,
                    ),
                );
            }
        }
        return None;
    }
    let flights = path_flights?;
    let mut allowed_lower_owners = vec![service_key];
    if let Some(startup_key) = startup_owner.map(|instance| instance.key)
        && !allowed_lower_owners.contains(&startup_key)
    {
        allowed_lower_owners.push(startup_key);
    }
    if flights.has_foreign_ordering_owner_before_offset(offset, &allowed_lower_owners)
        || flights.has_repair_flights_before_offset(offset)
    {
        return None;
    }
    if !service.has_bulk_model_evidence || observation.latency_pressure {
        #[cfg(feature = "lab-diagnostics")]
        {
            static TRACE_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let count = TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if count < 16 || count % 4096 == 0 {
                lab_diagnostic(
                    "request_startup_selection",
                    format_args!(
                        "phase=service_gate stream_id={} bulk_evidence={} latency_pressure={}",
                        stream_id.0, service.has_bulk_model_evidence, observation.latency_pressure,
                    ),
                );
            }
        }
        return None;
    }
    let service_snapshot = relay_path_snapshot_for_bulk_choice(
        observation,
        service_instance,
        Some(service_key),
        request_state,
        service.load_owned,
    )?;
    scheduler::score_path(
        service_snapshot,
        lane,
        payload_bytes,
        SchedulerPolicy::default(),
    )?;
    if service_snapshot.active_latency_sensitive_flows > 0
        || service_snapshot.session_active_latency_sensitive_flows > 0
    {
        return None;
    }
    // Product-flight age starts at carrier enqueue and is not stale-tail
    // authority: a healthy high-BDP TCP writer can retain work past one PTO.
    // The caller fences missing instances, foreign/active-repair flights are
    // rejected above, and the product envelope bounds the live-Service suffix.

    let startup_credit = usize::try_from(reliable_subflow_startup_sample_limit_bytes(
        observation.mux_limits,
    ))
    .unwrap_or(usize::MAX);
    let current_epoch = subflow_set.filter(|epoch| {
        epoch.matches_envelope(service_instance, startup_credit, 0, Duration::ZERO)
    });
    let mut epoch = current_epoch.cloned().unwrap_or_else(|| {
        FlowSubflowSet::new(0, service_instance, startup_credit, 0, Duration::ZERO)
    });
    let product_envelope =
        request_startup_product_envelope_bytes(payload_bytes, observation.mux_limits);
    let mut candidates = paths
        .iter()
        .enumerate()
        .filter(|(_, path)| path.placement == RelayPathPlacement::Validation)
        .filter(|(_, path)| path.key().underlay == service_key.underlay)
        .filter(|(_, path)| {
            request_state.is_none_or(|request| {
                !request
                    .subflow(path.instance())
                    .is_some_and(RequestSubflowState::graduated)
            })
        })
        .filter(|(_, path)| {
            subflow_set.and_then(FlowSubflowSet::startup_owner_key) == Some(path.instance())
                || attempted_subflows.is_none_or(|attempted| !attempted.contains(&path.instance()))
        })
        .filter(|(_, path)| path.can_enqueue_frame)
        .filter_map(|(position, path)| {
            let proof = path.fresh_proof?;
            let snapshot = relay_path_snapshot_for_bulk_choice(
                observation,
                path.instance(),
                Some(service_key),
                request_state,
                path.load_owned,
            )?;
            if startup_owner != Some(path.instance())
                && !path.load_owned
                && snapshot.active_flows > 1
            {
                #[cfg(feature = "lab-diagnostics")]
                {
                    static TRACE_COUNT: std::sync::atomic::AtomicU64 =
                        std::sync::atomic::AtomicU64::new(0);
                    let count = TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if count < 16 || count % 4096 == 0 {
                        lab_diagnostic(
                            "request_startup_selection",
                            format_args!(
                                "phase=occupied stream_id={} path_index={} instance_id={} active_flows={}",
                                stream_id.0,
                                path.instance.key.index,
                                path.instance.id,
                                snapshot.active_flows,
                            ),
                        );
                    }
                }
                // Sharing an unproven Validation carrier couples two logical
                // flows behind one evidence-owner hole. Keep the contender on
                // Service until an idle candidate attaches; exact begun owners
                // still continue through the branch above.
                return None;
            }
            if snapshot.active_latency_sensitive_flows > 0
                || snapshot.session_active_latency_sensitive_flows > 0
            {
                return None;
            }
            let ordering_debt = flights.ordering_debt_bytes_before_offset(path.key(), offset);
            let projected_product_debt = ordering_debt
                .saturating_add(snapshot.product_bytes_in_flight)
                .saturating_add(snapshot.product_queue_bytes)
                .saturating_add(payload_bytes as u64);
            if projected_product_debt > product_envelope {
                return None;
            }
            let score =
                scheduler::score_path(snapshot, lane, payload_bytes, SchedulerPolicy::default())?;
            Some((position, path, proof, score.eta_ms, snapshot))
        })
        .collect::<Vec<_>>();
    #[cfg(feature = "lab-diagnostics")]
    if candidates.is_empty() {
        static TRACE_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let count = TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if count < 16 || count % 4096 == 0 {
            lab_diagnostic(
                "request_startup_selection",
                format_args!(
                    "phase=no_candidate stream_id={} validation_paths={} attempted={} graduated={}",
                    stream_id.0,
                    paths
                        .iter()
                        .filter(|path| path.placement == RelayPathPlacement::Validation)
                        .count(),
                    attempted_subflows.map_or(0, HashSet::len),
                    request_state.map_or(0, RequestSchedulingState::graduated_count),
                ),
            );
        }
    }
    candidates.sort_by(|left, right| {
        left.3
            .total_cmp(&right.3)
            .then_with(|| left.1.instance.id.cmp(&right.1.instance.id))
    });

    for (_, path, proof, _, _snapshot) in candidates {
        let input = SubflowAdmissionInput {
            key: path.instance(),
            bulk_rate_proven: false,
            startup_owner_allowed: true,
            frontier_clear: true,
            completion_improves: false,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: payload_bytes,
            optional_overhead_bytes: 0,
        };
        if epoch.admit_subflow_owner(input).decision == PathAdmissionDecision::AdmitSubflow {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "request_startup_selection",
                format_args!(
                    "phase=selected stream_id={} path_index={} instance_id={} active_flows={}",
                    stream_id.0, path.instance.key.index, path.instance.id, _snapshot.active_flows,
                ),
            );
            return Some(BulkRelayPathChoice::SelectedStartupSubflow {
                service: service_instance,
                candidate: path.instance(),
                proof,
            });
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
fn choose_request_ack_clock_calibration_with_rates(
    observation: &RequestRelaySchedulingObservation,
    lane: FlowLane,
    offset: u64,
    payload_bytes: usize,
    cursor: usize,
    active_key: Option<RelayPathKey>,
    path_flights: Option<&RequestFlightLedger>,
    subflow_set: Option<&FlowSubflowSet<RelayPathInstance>>,
    request_state: Option<RequestSchedulingState<'_>>,
) -> Option<BulkRelayPathChoice> {
    let paths = observation.paths.as_slice();
    #[cfg(feature = "lab-diagnostics")]
    let stream_id = observation.stream_id;
    let service_key = active_key?;
    // Product ACK-clock calibration is the TCP fallback for unavailable native
    // carrier telemetry. QUIC capacity remains owned by its packet ACK model.
    if service_key.underlay != UnderlayProtocol::Tcp {
        return None;
    }
    let service = paths
        .iter()
        .find(|path| path.key() == service_key && path.placement == RelayPathPlacement::Active)?;
    let service_instance = service.instance();
    let service_proven = request_state.is_none_or(|request| {
        request
            .subflow(service_instance)
            .is_some_and(RequestSubflowState::rate_proven)
    });
    let startup_owner = subflow_set.and_then(FlowSubflowSet::startup_owner_key);
    let latency_pressure = observation.latency_pressure;
    let service_bulk_evidence = service.has_bulk_model_evidence;
    if !service_proven || startup_owner.is_some() || latency_pressure || !service_bulk_evidence {
        #[cfg(feature = "lab-diagnostics")]
        if request_state.is_some_and(|request| request.graduated_count() > 0) {
            static EARLY_TRACE_COUNT: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            let count = EARLY_TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if count < 16 || count % 1024 == 0 {
                lab_diagnostic(
                    "ack_clock_calibration",
                    format_args!(
                        "phase=early_gate service_underlay={:?} service_index={} service_instance={} service_proven={} startup_owner={} latency_pressure={} service_bulk_evidence={}",
                        service_key.underlay,
                        service_key.index,
                        service_instance.id,
                        service_proven,
                        startup_owner.is_some(),
                        latency_pressure,
                        service_bulk_evidence,
                    ),
                );
            }
        }
        return None;
    }
    let flights = path_flights?;
    if flights.has_repair_flights_before_offset(offset) {
        #[cfg(feature = "lab-diagnostics")]
        if request_state.is_some_and(|request| request.graduated_count() > 0) {
            static REPAIR_TRACE_COUNT: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            let count = REPAIR_TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if count < 16 || count % 1024 == 0 {
                lab_diagnostic(
                    "ack_clock_calibration",
                    format_args!(
                        "phase=repair_gate service_underlay={:?} service_index={} service_instance={} offset={}",
                        service_key.underlay, service_key.index, service_instance.id, offset,
                    ),
                );
            }
        }
        return None;
    }
    // Flight age starts at logical carrier enqueue, so a healthy deep TCP
    // pipeline can exceed one PTO. Exact repair, foreign-owner, and bounded
    // ordering-debt checks below own the product transition instead.
    let request_state = request_state?;
    let default_target =
        reliable_request_ack_clock_calibration_target_bytes(observation.mux_limits);
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
                .subflow(instance)
                .and_then(RequestSubflowState::ack_clock_calibration_bytes)
        })
        .flatten()
        .unwrap_or(0)
    };
    let hard_ceiling = reliable_ack_clock_calibration_ceiling_bytes(observation.mux_limits);
    let product_envelope =
        request_startup_product_envelope_bytes(payload_bytes, observation.mux_limits);
    let mut allowed_owner_keys = vec![service_key];
    for path in paths.iter().filter(|path| {
        path.key().underlay == service_key.underlay
            && request_state
                .subflow(path.instance())
                .is_some_and(RequestSubflowState::graduated)
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
        if count < 32 || count % 1024 == 0 {
            for path in paths.iter().filter(|path| {
                path.placement == RelayPathPlacement::Validation
                    && request_state
                        .subflow(path.instance())
                        .is_some_and(RequestSubflowState::graduated)
                    && !request_state
                        .subflow(path.instance())
                        .is_some_and(RequestSubflowState::ack_clock_proven)
            }) {
                let proof_fresh = path.fresh_proof.is_some();
                let candidate_bulk_evidence = path.has_bulk_model_evidence;
                let snapshot = relay_path_snapshot_for_bulk_choice(
                    observation,
                    path.instance(),
                    Some(service_key),
                    Some(request_state),
                    path.load_owned,
                );
                let spent = spent(path.instance());
                let foreign_owner =
                    flights.has_foreign_ordering_owner_before_offset(offset, &allowed_owner_keys);
                let ordering_debt = flights.ordering_debt_bytes_before_offset(path.key(), offset);
                let candidate_product_debt = snapshot.map_or(u64::MAX, |snapshot| {
                    snapshot
                        .product_bytes_in_flight
                        .saturating_add(snapshot.product_queue_bytes)
                        .saturating_add(payload_bytes as u64)
                });
                lab_diagnostic(
                    "ack_clock_calibration",
                    format_args!(
                        "phase=candidate stream_id={} underlay={:?} path_index={} instance_id={} same_underlay={} proof_fresh={} bulk_evidence={} receipt_boundary={} owner_match={} active_tcp_service_flows={} spent_bytes={} limit_bytes={} payload_bytes={} fits_target={} foreign_owner={} ordering_debt={} candidate_product_debt={} product_envelope={} within_envelope={} can_enqueue={} scoreable={} product_inflight={} product_queue={} active_latency={} session_latency={}",
                        stream_id.0,
                        path.key().underlay,
                        path.key().index,
                        path.instance.id,
                        path.key().underlay == service_key.underlay,
                        proof_fresh,
                        candidate_bulk_evidence,
                        request_state
                            .subflow(path.instance())
                            .is_some_and(RequestSubflowState::ack_clock_first_window),
                        request_state
                            .transaction_candidate()
                            .is_none_or(|candidate| candidate == path.instance()),
                        observation.active_tcp_service_bulk_flows,
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
                        snapshot.is_some_and(|snapshot| scheduler::score_path(
                            snapshot,
                            lane,
                            payload_bytes,
                            SchedulerPolicy::default(),
                        )
                        .is_some()),
                        snapshot.map_or(0, |snapshot| snapshot.product_bytes_in_flight),
                        snapshot.map_or(0, |snapshot| snapshot.product_queue_bytes),
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
        .filter(|(_, path)| path.placement == RelayPathPlacement::Validation)
        .filter(|(_, path)| path.key().underlay == service_key.underlay)
        .filter(|(_, path)| {
            request_state
                .subflow(path.instance())
                .is_some_and(RequestSubflowState::graduated)
        })
        .filter(|(_, path)| {
            !request_state
                .subflow(path.instance())
                .is_some_and(RequestSubflowState::ack_clock_proven)
        })
        .filter(|(_, path)| {
            request_state
                .subflow(path.instance())
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
        .filter(|_| !flights.has_foreign_ordering_owner_before_offset(offset, &allowed_owner_keys))
        .filter_map(|(position, path)| {
            let proof = path.fresh_proof?;
            let snapshot = relay_path_snapshot_for_bulk_choice(
                observation,
                path.instance(),
                Some(service_key),
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
                .product_bytes_in_flight
                .saturating_add(snapshot.product_queue_bytes)
                .saturating_add(payload_bytes as u64);
            if candidate_product_debt > hard_ceiling
                || ordering_debt.saturating_add(candidate_product_debt) > product_envelope
            {
                return None;
            }
            let score =
                scheduler::score_path(snapshot, lane, payload_bytes, SchedulerPolicy::default())?;
            let spent = spent(path.instance());
            if request_state.transaction_candidate().is_none() {
                let typed_zero_spend = spent == 0
                    && request_state
                        .subflow(path.instance())
                        .is_some_and(RequestSubflowState::tcp_capacity_proven);
                if observation.active_tcp_service_bulk_flows < 2 && !typed_zero_spend {
                    return None;
                }
                // The fresh typed receipt proves delivery and instance ownership,
                // not steady capacity. Its zero-spend epoch is the bounded product
                // measurement; ordinary admission resumes only after product ACKs.
            }
            Some((
                position,
                path.instance(),
                target(path.instance()),
                proof,
                spent > 0,
                cyclic_cursor_distance(position, cursor, paths.len()),
                score.eta_ms,
            ))
        })
        .min_by(|left, right| {
            right
                .4
                .cmp(&left.4)
                .then_with(|| left.5.cmp(&right.5))
                .then_with(|| left.6.total_cmp(&right.6))
                .then_with(|| left.1.id.cmp(&right.1.id))
        })
        .map(|(_, candidate, target_bytes, proof, _, _, _)| {
            BulkRelayPathChoice::SelectedAckClockCalibration {
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
    lane: FlowLane,
    key: RelayPathKey,
    lead_key: Option<RelayPathKey>,
    role: Option<BulkAdmissionRole>,
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
        lane: FlowLane,
        key: RelayPathKey,
        lead_key: Option<RelayPathKey>,
        payload_bytes: usize,
    ) -> Self {
        Self {
            stream_id,
            lane,
            key,
            lead_key,
            role: None,
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
fn log_bulk_relay_candidate_decision(
    diagnostics: BulkRelayCandidateDiagnostics,
    selected: bool,
    reason: &'static str,
) {
    if !lab_diagnostic_event_enabled("scheduler_decision") {
        return;
    }
    static TRACE_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let count = TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if count >= 512 && count % 1024 != 0 {
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
    let role = diagnostics
        .role
        .map(|role| format!("{role:?}"))
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
        confidence,
        app_limited,
        delivery_rate_bps,
        pacing_rate_bps,
    ) = diagnostics
        .snapshot
        .map(|snapshot| {
            (
                snapshot.product_bytes_in_flight,
                snapshot.queue_bytes,
                snapshot.bytes_in_flight,
                snapshot.inflight_limit_bytes,
                snapshot.confidence,
                snapshot.app_limited,
                snapshot.delivery_rate_bps,
                snapshot.pacing_rate_bps,
            )
        })
        .unwrap_or((0, 0, 0, 0, 0.0, false, 0.0, 0.0));

    lab_diagnostic(
        "scheduler_decision",
        format_args!(
            "stream_id={} lane={:?} candidate_underlay={:?} candidate_index={} lead_underlay={} lead_index={} role={} selected={} reason={} eta_ms={} best_eta_ms={} completion_horizon_ms={} stream_ordering_debt_bytes={} payload_bytes={} scoring_payload_bytes={} scoring_class={} product_queue_debt={} carrier_queue_debt={} bytes_in_flight={} inflight_limit={} confidence={:.3} app_limited={} delivery_rate_bps={:.0} pacing_rate_bps={:.0} delivery_sample_source=sender_model",
            diagnostics.stream_id.0,
            diagnostics.lane,
            diagnostics.key.underlay,
            diagnostics.key.index,
            lead_underlay,
            lead_index,
            role,
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
    if count >= 512 && count % 256 != 0 {
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
    let role = diagnostics
        .role
        .map(|role| format!("{role:?}"))
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
        delivery_rate_bps,
        pacing_rate_bps,
    ) = diagnostics
        .snapshot
        .map(|snapshot| {
            (
                diagnostics.eta_ms.unwrap_or(0.0),
                diagnostics.best_eta_ms.unwrap_or(0.0),
                diagnostics.completion_horizon_ms.unwrap_or(0.0),
                snapshot.product_bytes_in_flight,
                snapshot.queue_bytes,
                snapshot.bytes_in_flight,
                snapshot.inflight_limit_bytes,
                snapshot.delivery_rate_bps,
                snapshot.pacing_rate_bps,
            )
        })
        .unwrap_or((0.0, 0.0, 0.0, 0, 0, 0, 0, 0.0, 0.0));
    lab_diagnostic(
        "request_flow_local_admission_shadow",
        format_args!(
            "ordinal={} stream_id={} candidate_underlay={:?} candidate_index={} instance_id={} initial_gate={} outcome={} global_key_present={} retained_key_present={} graduated=true ack_clock_proven=true local_model_present={} global_admitted_keys={:?} retained_admitted_keys={:?} lead_underlay={} lead_index={} role={} local_rate_bps={:.0} local_delivery_samples={} eta_ms={:.3} best_eta_ms={:.3} completion_horizon_ms={:.3} stream_ordering_debt_bytes={} product_queue_debt={} carrier_queue_debt={} bytes_in_flight={} inflight_limit={} confidence={:.3} app_limited={} rate_scope={} delivery_rate_bps={:.0} pacing_rate_bps={:.0}",
            count + 1,
            diagnostics.stream_id.0,
            diagnostics.key.underlay,
            diagnostics.key.index,
            instance.id,
            initial_gate,
            outcome,
            global_key_present,
            retained_key_present,
            local_model.is_some(),
            global_admitted_keys,
            retained_admitted_keys,
            lead_underlay,
            lead_index,
            role,
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
        avoid_keys,
        path_flights,
        ordered_data_owner,
        subflow_set,
        request_state,
        attempted_subflows,
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
        avoid_keys,
        path_flights,
        ordered_data_owner,
        subflow_set,
        request_state,
        attempted_subflows,
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
        avoid_keys,
        path_flights,
        ordered_data_owner,
        subflow_set,
        request_state,
        attempted_subflows,
    } = request;
    let stream_id = observation.stream_id;
    let paths = observation.paths.as_slice();
    let mux_limits = observation.mux_limits;
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = stream_id;
    if !lane.is_bulk() || payload_bytes == 0 {
        return BulkRelayPathChoice::NotApplicable;
    }
    let policy = SchedulerPolicy::default();
    // The request-side primary owner is the existing ordered-data owner when it
    // is still attached; otherwise it is the first active path opened for the
    // stream.  Newly attached active paths are candidate subflows, not automatic
    // service owners.  Using `paths.last()` here made an attached UDP survivor
    // become the service path before any bytes were sent, which blocked the
    // initial TCP owner in the lower-frontier regression test and can also
    // create real upload instability by letting attachment order override
    // product-byte ownership.
    let active_key = ordered_data_owner
        .filter(|owner| paths.iter().any(|path| path.key() == *owner))
        .or_else(|| {
            paths
                .iter()
                .find(|path| path.placement == RelayPathPlacement::Active)
                .map(|path| path.key())
        })
        .or_else(|| paths.last().map(|path| path.key()));
    let normal_bulk_send = avoid_keys.is_empty();
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
        if count < 16 || count % 4096 == 0 {
            lab_diagnostic(
                "request_startup_selection",
                format_args!(
                    "phase=bulk_precheck stream_id={} paths={} ordered_owner={} active_tcp_service_flows={}",
                    stream_id.0,
                    paths.len(),
                    ordered_data_owner.is_some(),
                    observation.active_tcp_service_bulk_flows,
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
                let state = request_state.subflow(path.instance());
                path.key() == *key
                    && (path.placement == RelayPathPlacement::Active
                        || (state.is_some_and(RequestSubflowState::graduated)
                            && (key.underlay != UnderlayProtocol::Tcp
                                || state.is_some_and(RequestSubflowState::ack_clock_proven))))
            })
        });
    }
    if normal_bulk_send {
        // Session-wide path health ranks shared carriers, but it cannot revoke
        // capability already proven by this exact TCP flow. Keep ownership
        // local while all ordinary completion and ordering gates remain below.
        for path in paths
            .iter()
            .filter(|path| request_path_has_exact_flow_local_bulk_model(path, request_state))
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
            active_key,
            admitted_bulk_keys: &admitted_bulk_keys,
            restrict_to_admitted,
            lower_flight_owner,
            lower_owner_cross_path_debt,
            policy,
            request_state,
        })
    } else {
        None
    };
    let calibration_transaction_candidate = normal_bulk_send
        .then(|| {
            active_key.and_then(|service_key| {
                observed_request_ack_clock_calibration_transaction(
                    paths,
                    service_key,
                    request_state,
                )
            })
        })
        .flatten();
    let mut calibration_service_fence = None;
    if let Some(candidate) = calibration_transaction_candidate {
        let service_key = active_key.expect("a live calibration transaction has a Service");
        let mut allowed_lower_owners = vec![service_key];
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
            flights.has_foreign_ordering_owner_before_offset(offset, &allowed_lower_owners)
        });
        if !foreign_optional_owner
            && let Some(choice) = choose_request_ack_clock_calibration_with_rates(
                observation,
                lane,
                offset,
                payload_bytes,
                cursor,
                active_key,
                path_flights,
                subflow_set,
                request_state,
            )
        {
            return choice;
        }
        // A begun transaction preempts startup and generic Subflow ownership.
        // Service still passes the ordinary completion and reorder gates below.
        calibration_service_fence = Some(candidate);
    }
    if calibration_transaction_candidate.is_none()
        && normal_bulk_send
        && ordered_data_owner.is_some()
        && let Some(choice) = choose_request_startup_subflow_with_rates(
            observation,
            lane,
            offset,
            payload_bytes,
            active_key,
            path_flights,
            subflow_set,
            request_state,
            attempted_subflows,
        )
    {
        return choice;
    }
    if calibration_transaction_candidate.is_none()
        && normal_bulk_send
        && ordered_data_owner.is_some()
        && let Some(choice) = choose_request_ack_clock_calibration_with_rates(
            observation,
            lane,
            offset,
            payload_bytes,
            cursor,
            active_key,
            path_flights,
            subflow_set,
            request_state,
        )
    {
        if let BulkRelayPathChoice::SelectedAckClockCalibration { candidate, .. } = choice
            && let Some(service_key) = active_key
            && path_flights.is_some_and(|flights| {
                flights.has_foreign_ordering_owner_before_offset(offset, &[service_key])
            })
        {
            // The candidate passed every existing entry gate; defer only its
            // ownership commit until prior optional work leaves the frontier.
            calibration_service_fence = Some(candidate);
        } else {
            return choice;
        }
    }
    if normal_bulk_send && lead.is_none() {
        if calibration_service_fence.is_some() {
            return BulkRelayPathChoice::Blocked;
        }
        if lower_flight_owner.is_none()
            && let Some(active_key) = active_key
            && let Some((position, _)) = paths.iter().enumerate().find(|(_, path)| {
                path.key() == active_key
                    && path.placement != RelayPathPlacement::Repair
                    && path.can_enqueue_frame
            })
        {
            // First owner bytes establish the lower-frontier owner.  The
            // no-fallback rule applies after that frontier exists; before then,
            // blocking the active primary just creates a sender-service stall.
            return BulkRelayPathChoice::Selected(paths[position].instance());
        }
        return BulkRelayPathChoice::Blocked;
    }
    let lead_key = lead.map(|lead| lead.key);
    let lead_baseline = lead.map(|lead| (lead.snapshot, lead.eta_ms));
    let mut best: Option<(usize, f64, usize, PathSnapshot)> = None;
    let mut old_lead_candidate: Option<(usize, f64, PathSnapshot)> = None;
    #[cfg(feature = "lab-diagnostics")]
    let mut best_diagnostics: Option<BulkRelayCandidateDiagnostics> = None;
    #[cfg(feature = "lab-diagnostics")]
    let mut best_flow_local_shadow: Option<(
        usize,
        f64,
        usize,
        PathSnapshot,
        BulkRelayCandidateDiagnostics,
        RelayPathInstance,
        &'static str,
        Option<RequestPerFlowRateModel>,
    )> = None;
    for (position, path) in paths.iter().enumerate() {
        let key = path.key();
        if calibration_service_fence.is_some() && Some(key) != active_key {
            continue;
        }
        #[cfg(feature = "lab-diagnostics")]
        let mut flow_local_shadow_gate = None;
        #[cfg(feature = "lab-diagnostics")]
        let exact_flow_local_candidate =
            request_path_has_exact_flow_local_bulk_model(path, request_state);
        if normal_bulk_send
            && subflow_set.and_then(FlowSubflowSet::startup_owner_key) == Some(path.instance())
        {
            // A startup owner remains governed by its cumulative epoch until
            // all attributed ranges drain and the caller commits graduation.
            // Bulk-rate evidence from an early ACK must not bypass the startup
            // credit through the ordinary measured-path branch.
            continue;
        }
        if normal_bulk_send && path.placement == RelayPathPlacement::Validation {
            let is_ungraduated = request_state.is_some_and(|request| {
                !request
                    .subflow(path.instance())
                    .is_some_and(RequestSubflowState::graduated)
            });
            let lacks_tcp_capacity_proof = key.underlay == UnderlayProtocol::Tcp
                && request_state.is_some_and(|request| {
                    !request
                        .subflow(path.instance())
                        .is_some_and(RequestSubflowState::ack_clock_proven)
                });
            if is_ungraduated || lacks_tcp_capacity_proof {
                continue;
            }
        }
        if normal_bulk_send && path.placement == RelayPathPlacement::Repair {
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
                "repair_path_not_for_ordinary_bulk",
            );
            continue;
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
        if avoid_keys.contains(&path.key())
            && paths.iter().any(|path| !avoid_keys.contains(&path.key()))
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
                        "not_in_admitted_subflow_set",
                    );
                    #[cfg(feature = "lab-diagnostics")]
                    {
                        if exact_flow_local_candidate {
                            flow_local_shadow_gate = Some("not_in_admitted_subflow_set");
                        } else {
                            continue;
                        }
                    }
                    #[cfg(not(feature = "lab-diagnostics"))]
                    continue;
                }
            } else if !owns_lower_frontier && Some(key) != active_key {
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
                    "no_safe_subflow_set_non_active_path",
                );
                #[cfg(feature = "lab-diagnostics")]
                {
                    if exact_flow_local_candidate {
                        flow_local_shadow_gate = Some("no_safe_subflow_set_non_active_path");
                    } else {
                        continue;
                    }
                }
                #[cfg(not(feature = "lab-diagnostics"))]
                continue;
            }
            if let Some(active) = active_key {
                // The request/upload side follows the same production rule as
                // the response scheduler: same-stream reliable OwnerData stays
                // inside the active carrier family. Different-family paths may
                // still be used for probes or repair, but they must not create
                // new ordered-byte ownership that can stall behind an unrelated
                // TCP/QUIC recovery clock. A path that already owns the lower
                // frontier remains eligible only to drain existing debt.
                if key.underlay != active.underlay && !owns_lower_frontier {
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
                            "cross_family_owner_disabled",
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
                            "cross_family_owner_disabled",
                            &global_admitted_bulk_keys,
                            &admitted_bulk_keys,
                            request_state
                                .and_then(|request| request.subflow(path.instance()))
                                .and_then(RequestSubflowState::per_flow_rate),
                        );
                    }
                    continue;
                }
            }
        }
        if normal_bulk_send
            && Some(key) != active_key
            && lower_flight_owner != Some(key)
            && !(lower_flight_owner.is_none()
                && restrict_to_admitted
                && admitted_bulk_keys.contains(&key))
            && !relay_path_runtime_role(
                key,
                active_key,
                lower_flight_owner,
                path.has_bulk_model_evidence
                    || request_path_has_exact_flow_local_bulk_model(path, request_state),
            )
            .may_own_unique_data()
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
            active_key,
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
                        .and_then(|request| request.subflow(path.instance()))
                        .and_then(RequestSubflowState::per_flow_rate),
                );
            }
            continue;
        };
        let scoring_payload_bytes = if lane.is_bulk() {
            bulk_service_horizon_payload_bytes(payload_bytes, mux_limits)
        } else {
            payload_bytes
        };
        let Some(score) = scheduler::score_path(snapshot, lane, scoring_payload_bytes, policy)
        else {
            #[cfg(feature = "lab-diagnostics")]
            if flow_local_shadow_gate.is_none() {
                log_bulk_relay_candidate_decision(
                    BulkRelayCandidateDiagnostics {
                        stream_id,
                        lane,
                        key,
                        lead_key,
                        role: None,
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
                        role: None,
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
                        .and_then(|request| request.subflow(path.instance()))
                        .and_then(RequestSubflowState::per_flow_rate),
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
            let role = if owns_lower_frontier
                || (Some(key) == lead_key && cross_path_ordering_debt == 0)
            {
                BulkAdmissionRole::ActiveDataPath
            } else if let Some(owner) = lower_flight_owner {
                bulk_additional_admission_role(owner.underlay, key.underlay)
            } else if let Some(lead_key) = lead_key {
                bulk_additional_admission_role(lead_key.underlay, key.underlay)
            } else {
                BulkAdmissionRole::ActiveDataPath
            };
            let admission_ordering_debt = cross_path_ordering_debt;
            let (best_snapshot, best_eta_ms) =
                if owns_lower_frontier && role == BulkAdmissionRole::ActiveDataPath {
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
                    role: Some(role),
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
            let ordering_suppression =
                bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                    best_snapshot,
                    best_eta_ms,
                    candidate_snapshot: snapshot,
                    candidate_eta_ms: score.eta_ms,
                    payload_bytes,
                    mux_limits,
                    role,
                    stream_ordering_debt_bytes: admission_ordering_debt,
                });
            let calibration_service_reservoir =
                calibration_service_fence.is_some_and(|candidate| {
                    Some(key) == active_key
                        && request_state.is_some_and(|request_state| {
                            path_flights.is_some_and(|flights| {
                                request_ack_clock_calibration_service_reservoir_has_credit(
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
                && !(calibration_service_reservoir
                    && matches!(
                        reason,
                        "same_underlay_no_completion_gain" | "completion_horizon"
                    ))
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
                                .and_then(|request| request.subflow(path.instance()))
                                .and_then(RequestSubflowState::per_flow_rate),
                        );
                    }
                }
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = reason;
                continue;
            }
        }
        #[cfg(feature = "lab-diagnostics")]
        if let Some(initial_gate) = flow_local_shadow_gate {
            let cursor_distance = cyclic_cursor_distance(position, cursor, paths.len());
            let diagnostics = candidate_diagnostics.unwrap_or(BulkRelayCandidateDiagnostics {
                stream_id,
                lane,
                key,
                lead_key,
                role: None,
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
                    .and_then(|request| request.subflow(path.instance()))
                    .and_then(RequestSubflowState::per_flow_rate),
            );
            let candidate = (
                position,
                score.eta_ms,
                cursor_distance,
                snapshot,
                diagnostics,
                path.instance(),
                initial_gate,
                request_state
                    .and_then(|request| request.subflow(path.instance()))
                    .and_then(RequestSubflowState::per_flow_rate),
            );
            let replaces_shadow = best_flow_local_shadow.as_ref().is_none_or(
                |(_, best_eta, best_distance, _, _, _, _, _)| {
                    score.eta_ms < *best_eta
                        || (score.eta_ms == *best_eta && cursor_distance < *best_distance)
                },
            );
            if replaces_shadow {
                best_flow_local_shadow = Some(candidate);
            }
            continue;
        }
        if normal_bulk_send && lower_flight_owner.is_none() && Some(key) == ordered_data_owner {
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
                            role: None,
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
            Some((_, best_eta, best_distance, _)) => {
                if score.eta_ms < best_eta
                    || (score.eta_ms == best_eta && cursor_distance < best_distance)
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
                                role: None,
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
    if let Some((
        _,
        shadow_eta_ms,
        shadow_distance,
        shadow_snapshot,
        diagnostics,
        instance,
        initial_gate,
        local_model,
    )) = best_flow_local_shadow
    {
        let shadow_is_best = best
            .as_ref()
            .is_none_or(|(_, best_eta_ms, best_distance, _)| {
                shadow_eta_ms < *best_eta_ms
                    || (shadow_eta_ms == *best_eta_ms && shadow_distance < *best_distance)
            });
        let owner_hysteresis_keeps_lead = shadow_is_best
            && old_lead_candidate
                .as_ref()
                .is_some_and(|(_, old_eta_ms, old_snapshot)| {
                    relay_path_within_adaptive_lead_hysteresis(
                        *old_eta_ms,
                        *old_snapshot,
                        shadow_eta_ms,
                        shadow_snapshot,
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
            diagnostics,
            instance,
            initial_gate,
            outcome,
            &global_admitted_bulk_keys,
            &admitted_bulk_keys,
            local_model,
        );
    }
    if let Some((best_position, best_eta_ms, _, best_snapshot)) = best {
        let position = old_lead_candidate
            .filter(|(_, old_eta_ms, old_snapshot)| {
                relay_path_within_adaptive_lead_hysteresis(
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
        if let Some(candidate) = calibration_service_fence {
            debug_assert_eq!(Some(paths[position].key()), active_key);
            return BulkRelayPathChoice::SelectedAckClockCalibrationFence {
                service: paths[position].instance(),
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
    lane: FlowLane,
    payload_bytes: usize,
    active_key: Option<RelayPathKey>,
    admitted_bulk_keys: &'a [RelayPathKey],
    restrict_to_admitted: bool,
    lower_flight_owner: Option<RelayPathKey>,
    lower_owner_cross_path_debt: u64,
    policy: SchedulerPolicy,
    request_state: Option<RequestSchedulingState<'a>>,
}

fn choose_admissible_relay_bulk_lead(request: RelayBulkLeadRequest<'_>) -> Option<RelayBulkLead> {
    let RelayBulkLeadRequest {
        observation,
        lane,
        payload_bytes,
        active_key,
        admitted_bulk_keys,
        restrict_to_admitted,
        lower_flight_owner,
        lower_owner_cross_path_debt,
        policy,
        request_state,
    } = request;
    let paths = observation.paths.as_slice();
    paths
        .iter()
        .filter(|path| path.placement != RelayPathPlacement::Repair)
        .filter(|path| path.can_enqueue_frame)
        .filter(|path| {
            let key = path.key();
            if let Some(owner) = lower_flight_owner {
                return key == owner;
            }
            if restrict_to_admitted {
                admitted_bulk_keys.contains(&key)
            } else {
                Some(key) == active_key
            }
        })
        .filter(|path| {
            let key = path.key();
            Some(key) == active_key
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
                active_key,
                lane,
                payload_bytes,
                policy,
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
                    role: BulkAdmissionRole::ActiveDataPath,
                    stream_ordering_debt_bytes,
                });
            suppression.is_none().then_some(RelayBulkLead {
                key,
                snapshot,
                eta_ms,
            })
        })
        .min_by(|left, right| {
            left.eta_ms
                .total_cmp(&right.eta_ms)
                .then_with(|| observation.compare_path_keys(left.key, right.key))
        })
}

fn scored_relay_path_snapshot_for_bulk_choice(
    observation: &RequestRelaySchedulingObservation,
    instance: RelayPathInstance,
    active_key: Option<RelayPathKey>,
    lane: FlowLane,
    payload_bytes: usize,
    policy: SchedulerPolicy,
    request_state: Option<RequestSchedulingState<'_>>,
    current_flow_owns_path: bool,
) -> Option<(PathSnapshot, f64)> {
    let snapshot = relay_path_snapshot_for_bulk_choice(
        observation,
        instance,
        active_key,
        request_state,
        current_flow_owns_path,
    )?;
    let scoring_payload_bytes = if lane.is_bulk() {
        bulk_service_horizon_payload_bytes(payload_bytes, observation.mux_limits)
    } else {
        payload_bytes
    };
    let score = scheduler::score_path(snapshot, lane, scoring_payload_bytes, policy)?;
    Some((snapshot, score.eta_ms))
}

fn relay_path_snapshot_for_bulk_choice(
    observation: &RequestRelaySchedulingObservation,
    instance: RelayPathInstance,
    active_key: Option<RelayPathKey>,
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
        let local_model = request_state
            .and_then(|request| request.subflow(instance))
            .and_then(RequestSubflowState::per_flow_rate);
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
            snapshot.inflight_limit_bytes = startup.inflight_limit_bytes;
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
            let service_exploration_rate_bps = (Some(instance.key) != active_key
                && rate_hint_unknown)
                .then(|| {
                    active_key.and_then(|active| {
                        request_state
                            .and_then(|request| {
                                request.subflows.iter().find_map(|(instance, state)| {
                                    (instance.key == active)
                                        .then(|| state.per_flow_rate())
                                        .flatten()
                                        .map(|model| model.rate_bps)
                                })
                            })
                            .or_else(|| {
                                // Before this flow has a continuous Service sample,
                                // the exact active Service path model is still valid
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
            let provisional_rate_bps = startup.delivery_rate_bps.max(service_exploration_rate_bps);
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
            if Some(instance.key) != active_key && retain_capacity_prior {
                // Endpoint-only paths have no configured capacity hint. Once
                // exact ownership is proven, borrow only the current Service
                // rate as bounded exploration credit so the candidate TCP can
                // leave slow start. The candidate's own tenth exact sample
                // replaces this prior; kernel cwnd and the product envelope
                // remain hard limits throughout.
                let provisional_pipe = bulk_candidate_pipe_bytes(snapshot).min(
                    reliable_ack_clock_calibration_ceiling_bytes(observation.mux_limits),
                );
                snapshot.inflight_limit_bytes = snapshot
                    .inflight_limit_bytes
                    .max(provisional_pipe)
                    .max(PATH_OPEN_SCORE_BYTES as u64);
            } else if Some(instance.key) != active_key && mature {
                // A configured capacity prior may keep an underfed candidate in the
                // ranking set. Only a mature continuous per-flow ACK model may
                // shrink its initial pipe; one bounded proof sample is expected
                // to be app-limited while the TCP carrier is still ramping.
                let mut observed = snapshot;
                observed.delivery_rate_bps = model.rate_bps.max(1.0);
                observed.pacing_rate_bps = observed.delivery_rate_bps;
                observed.rate_scope = PathRateScope::PerFlowGoodput;
                let observed_pipe_bytes = bbr_inflight_target_bytes(
                    observed,
                    FlowLane::Throughput,
                    observation.mux_limits,
                )
                .ceil()
                .max(PATH_OPEN_SCORE_BYTES as f64) as u64;
                snapshot.inflight_limit_bytes = snapshot
                    .inflight_limit_bytes
                    .min(observed_pipe_bytes)
                    .max(PATH_OPEN_SCORE_BYTES as u64);
            }
        }
    }
    if Some(instance.key) != active_key && !current_flow_owns_path {
        snapshot.active_flows = snapshot.active_flows.saturating_add(1);
    }
    Some(snapshot)
}

#[cfg(test)]
#[path = "scheduling_test.rs"]
mod tests;
