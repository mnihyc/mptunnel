//! Serialized request multipath lifecycle.
//!
//! This owner combines carrier-neutral product offsets and evidence with two
//! concrete capacity controllers. It accepts immutable observations, advances
//! one typed post-enqueue mutation, and never merges TCP and QUIC proof state.

use super::super::queue::ReliableRelaySenderQueue;
use super::super::work::{ClientRepairOutputIdentity, RelaySendCause};
use super::quic_capacity::{RequestQuicCapacityController, RequestQuicCapacityEvent};
use super::scheduling::{
    BulkRelayFrameRequest, BulkRelayPathChoice, ObservedBulkPathCandidate,
    ObservedOrdinaryPathChoice, RequestRelayPathObservation, RequestRelaySchedulingObservation,
    RequestRelayTcpPathObservation, RequestSchedulingState, choose_bulk_relay_path_avoiding,
    choose_observed_ordinary_data_path,
};
use super::tcp_capacity::{
    RequestTcpCapacityController, RequestTcpCapacityEvent, RequestTcpCapacityRetirement,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::PathRateSample;
use crate::model::multipath::FlowSubflowSet;
use crate::model::path::{
    RelayPathInstance, RelayPathKey, RelayPathPlacement, RelayPathProofEpoch,
};
use crate::model::request::evidence::{
    RequestOwnerAckProgress, RequestPathRateEvidenceUpdate, RequestPerFlowRateModel,
    RequestTcpAckTurnoverModel, RequestWindowGrowthEvidence,
    request_path_rate_coverage_floor_bytes, request_tcp_candidate_turnover_authorized,
};
use crate::model::timing::transport_pto_from_snapshot;
use crate::protocol::frame::{reliable_stream_frame_accounted_bytes, reliable_stream_frame_extent};
use crate::protocol::{Frame, OffsetRange, StreamId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ClientPathContext;
use crate::runtime::relay::remote::{ReliableRelayRemotePath, ReliableRelayRemoteSet};
use crate::runtime::stream::request::{
    RequestAckClockOperation, RequestStartupAdmission, RequestStreamState,
};
use crate::scheduler::{self, FlowLane, PathSnapshot, SchedulerPolicy, cyclic_cursor_distance};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

fn observe_request_relay_scheduling(
    context: &ClientPathContext,
    stream_id: StreamId,
    membership_generation: u64,
    remote_paths: &[ReliableRelayRemotePath],
    frame: Option<&Frame>,
    lane: FlowLane,
    payload_bytes: usize,
    include_bulk_admission: bool,
) -> RequestRelaySchedulingObservation {
    let path_evidence = context.observe_reliable_request_paths(
        remote_paths.iter().map(|path| {
            (
                path.key(),
                path.path_proof_id.map(|proof_id| RelayPathProofEpoch {
                    proof_id,
                    proof_generation: path.path_proof_generation,
                    attached_at: path.attached_at,
                }),
            )
        }),
        payload_bytes,
        include_bulk_admission,
    );
    let paths = remote_paths
        .iter()
        .zip(path_evidence.paths)
        .map(|(path, evidence)| {
            let instance = path.instance();
            debug_assert_eq!(instance.key, evidence.key);
            RequestRelayPathObservation {
                instance,
                placement: path.placement,
                can_enqueue_frame: frame
                    .map(|frame| path.stream.can_enqueue_frame_now(frame, lane))
                    .unwrap_or(true),
                can_enqueue_stream_lane: frame
                    .map(|frame| path.stream.can_enqueue_frame_now(frame, path.stream.lane))
                    .unwrap_or(true),
                load_owned: path.has_load_reservation(),
                shared_snapshot: evidence.shared_snapshot,
                tcp: evidence.tcp.map(|tcp| RequestRelayTcpPathObservation {
                    startup_snapshot: tcp.startup_snapshot,
                    rate_hint_unknown: tcp.rate_hint_unknown,
                }),
                has_bulk_model_evidence: evidence.has_bulk_model_evidence,
                fresh_proof: evidence.fresh_proof,
                config_ordinal: evidence.config_ordinal,
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
            })
            .collect(),
        active_tcp_service_bulk_flows: path_evidence.active_tcp_service_bulk_flows,
        latency_pressure: path_evidence.latency_pressure,
    }
}

fn choose_active_recv_progress_path_position(
    remotes: &ReliableRelayRemoteSet,
    frame: &Frame,
    cause: RelaySendCause,
) -> Option<usize> {
    remotes
        .paths
        .iter()
        .enumerate()
        .rev()
        .find(|(_, path)| {
            path.placement == RelayPathPlacement::Active
                && relay_path_can_enqueue_frame_for_cause_now(path, frame, cause)
        })
        .map(|(position, _)| position)
}

fn choose_repair_recv_progress_path_position(
    remotes: &ReliableRelayRemoteSet,
    frame: &Frame,
    cause: RelaySendCause,
) -> Option<usize> {
    remotes
        .paths
        .iter()
        .enumerate()
        .rev()
        .find(|(_, path)| {
            path.placement == RelayPathPlacement::Repair
                && relay_path_can_enqueue_frame_for_cause_now(path, frame, cause)
        })
        .map(|(position, _)| position)
}

fn relay_path_can_enqueue_frame_for_cause_now(
    path: &ReliableRelayRemotePath,
    frame: &Frame,
    cause: RelaySendCause,
) -> bool {
    if matches!(cause, RelaySendCause::StreamFin) {
        path.stream.output.can_enqueue_lane_now(path.stream.lane)
    } else {
        path.stream.can_enqueue_frame_now(frame, path.stream.lane)
    }
}

fn observed_request_load_expectation(
    observation: &RequestRelaySchedulingObservation,
    instance: RelayPathInstance,
) -> Result<Option<(RelayPathKey, u32, u32)>, RuntimeError> {
    let path = observation
        .path_by_instance(instance)
        .ok_or(RuntimeError::SenderServiceBlocked)?;
    if path.load_owned {
        return Ok(None);
    }
    let snapshot = path
        .shared_snapshot
        .ok_or(RuntimeError::SenderServiceBlocked)?;
    Ok(Some((
        instance.key,
        snapshot.active_flows,
        snapshot.active_latency_sensitive_flows,
    )))
}

#[derive(Debug)]
pub(super) struct RequestMultipathPlan {
    target: RequestMultipathTarget,
    product_mutation: RequestProductSendMutation,
    request_load_expectation: Option<(RelayPathKey, u32, u32)>,
    request_proof_expectation: Option<RelayPathProofEpoch>,
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
    InstallService,
    PreserveSubflow,
    CommitStartup(RequestStartupAdmission),
    ServiceFence {
        service: RelayPathInstance,
        candidate: RelayPathInstance,
        entry_offset: u64,
        foreign_optional_ranges: usize,
        foreign_optional_bytes: u64,
    },
    OwnerData {
        candidate: RelayPathInstance,
        target_bytes: u64,
        payload_bytes: u64,
        entry_offset: u64,
        foreign_optional_ranges: usize,
        foreign_optional_bytes: u64,
    },
}

impl RequestMultipathPlan {
    fn new(target: RequestMultipathTarget, product_mutation: RequestProductSendMutation) -> Self {
        Self {
            target,
            product_mutation,
            request_load_expectation: None,
            request_proof_expectation: None,
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
}

/// Product state and the two independent carrier authorities for one request.
#[derive(Debug)]
pub(super) struct RequestMultipathController {
    stream_id: StreamId,
    request: RequestStreamState,
    tcp_capacity: RequestTcpCapacityController,
    quic_capacity: RequestQuicCapacityController,
    next_send_index: usize,
}

impl RequestMultipathController {
    pub(super) fn new(stream_id: StreamId) -> Self {
        Self {
            stream_id,
            request: RequestStreamState::default(),
            tcp_capacity: RequestTcpCapacityController::default(),
            quic_capacity: RequestQuicCapacityController::default(),
            next_send_index: 0,
        }
    }

    #[cfg(feature = "lab-diagnostics")]
    pub(super) fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub(super) fn ack_gap_repair_path_model(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        preview: &Frame,
        lane: FlowLane,
    ) -> (
        Option<UnderlayProtocol>,
        Option<PathSnapshot>,
        Option<(ClientRepairOutputIdentity, PathSnapshot)>,
    ) {
        let owner_underlay = self
            .request
            .flights
            .ordering_owner_underlay_for_frame(preview);
        let owner_timing_path = self
            .request
            .flights
            .ordering_owner_keys_for_frame_any_instance(preview)
            .into_iter()
            .filter_map(|key| context.reliable_path_snapshot(key))
            .max_by(|left, right| {
                transport_pto_from_snapshot(Some(*left))
                    .cmp(&transport_pto_from_snapshot(Some(*right)))
            });
        let avoid_keys = self.request.flights.sent_keys_for_frame(preview);
        let repair_path = self
            .choose_lowest_eta_relay_path(
                context,
                remotes,
                preview,
                lane,
                RelaySendCause::PersistentAckGapRepair,
                &avoid_keys,
                false,
            )
            .ok()
            .and_then(|position| {
                let key = remotes.paths[position].key();
                let instance = remotes.paths[position].instance();
                context
                    .relay_path_has_bulk_model_evidence(key.underlay, key.index)
                    .then(|| {
                        context
                            .reliable_path_snapshot(key)
                            .map(|snapshot| (ClientRepairOutputIdentity { instance }, snapshot))
                    })
                    .flatten()
            });
        (owner_underlay, owner_timing_path, repair_path)
    }

    pub(super) fn repair_avoid_keys(
        &self,
        frame: &Frame,
        cause: RelaySendCause,
        remotes: &ReliableRelayRemoteSet,
    ) -> Vec<RelayPathKey> {
        match cause {
            RelaySendCause::LiveOwnerTailRepair => {
                self.request.flights.live_owner_tail_repair_owner_keys(
                    frame,
                    &remotes.path_instances(),
                    Duration::ZERO,
                    Duration::ZERO,
                )
            }
            cause if cause.is_repair() => self.request.flights.sent_keys_for_frame(frame),
            _ => Vec::new(),
        }
    }

    pub(super) fn owner_capable_instances(
        &self,
        remotes: &ReliableRelayRemoteSet,
    ) -> Vec<RelayPathInstance> {
        self.request_owner_capable_instances(remotes)
    }

    pub(super) fn ordering_owner_keys_for_frame(
        &self,
        frame: &Frame,
        live_instances: &[RelayPathInstance],
    ) -> Vec<RelayPathKey> {
        self.request
            .flights
            .ordering_owner_keys_for_frame(frame, live_instances)
    }

    pub(super) fn live_owner_tail_repair_owner_keys(
        &self,
        frame: &Frame,
        live_instances: &[RelayPathInstance],
        first_repair_after: Duration,
        repeat_repair_after: Duration,
    ) -> Vec<RelayPathKey> {
        self.request.flights.live_owner_tail_repair_owner_keys(
            frame,
            live_instances,
            first_repair_after,
            repeat_repair_after,
        )
    }

    pub(super) fn latest_unacked_ranges_for_path_instance(
        &self,
        instance: RelayPathInstance,
    ) -> Vec<OffsetRange> {
        self.request
            .flights
            .latest_unacked_ranges_for_path_instance(instance)
    }

    #[cfg(test)]
    pub(super) fn failed_path_gap_parts(
        &self,
        key: RelayPathKey,
    ) -> (Vec<RelayPathInstance>, Vec<OffsetRange>) {
        let instances = self
            .request
            .flights
            .ordering_owner_instances()
            .into_iter()
            .filter(|instance| instance.key == key)
            .collect();
        let ranges = self.request.flights.latest_unacked_ranges_for_path(key);
        (instances, ranges)
    }

    pub(super) fn record_missing_owner_repair_attempts(
        &mut self,
        instances: &[RelayPathInstance],
        attempted_at: Instant,
    ) {
        for instance in instances {
            self.request
                .missing_owner_repair_attempts
                .insert(*instance, attempted_at);
        }
    }

    pub(super) fn record_emitted_frame(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
        cause: RelaySendCause,
    ) -> usize {
        if cause.is_repair() {
            self.request
                .flights
                .record_repair_frame_instance(instance, frame)
        } else {
            self.request
                .flights
                .record_owner_frame_instance(instance, frame)
        }
    }

    pub(super) fn record_emit_failure(&mut self, instance: RelayPathInstance) {
        if self.request.ordered_service == Some(instance) {
            self.request.ordered_service = None;
            self.reset_request_subflow_epoch();
        } else if self
            .request
            .startup
            .epoch
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key)
            == Some(instance)
        {
            self.request.startup.epoch = None;
            self.request.startup.acked_bytes.remove(&instance);
            self.request.startup.first_sent_at.remove(&instance);
            self.request.startup.rate_evidence.remove(&instance);
            self.request.startup.receipt_proofs.remove(&instance);
            if let Some(state) = self.request.subflows.get_existing_mut(instance) {
                state.clear_graduated();
            }
        }
    }

    pub(super) fn normalize_cursor(&mut self, path_count: usize) {
        if path_count == 0 {
            self.next_send_index = 0;
        } else {
            self.next_send_index %= path_count;
        }
    }

    fn try_start_request_tcp_capacity_calibration(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        lane: FlowLane,
    ) {
        self.tcp_capacity
            .try_start(self.stream_id, &self.request, context, remotes, lane);
    }

    fn try_start_request_quic_capacity_calibration(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        lane: FlowLane,
    ) {
        self.quic_capacity
            .try_start(self.stream_id, &self.request, context, remotes, lane);
    }

    fn record_request_per_flow_rate_sample(
        &mut self,
        instance: RelayPathInstance,
        sample: PathRateSample,
        replace: bool,
    ) {
        if instance.key.underlay != UnderlayProtocol::Tcp {
            return;
        }
        let sample_bps = sample.rate_bps();
        let previous = self
            .request
            .subflows
            .get(instance)
            .and_then(|state| state.per_flow_rate());
        let model = if replace {
            RequestPerFlowRateModel {
                rate_bps: sample_bps,
                delivery_samples: 1,
            }
        } else {
            RequestPerFlowRateModel {
                rate_bps: previous.map_or(sample_bps, |previous| {
                    previous.rate_bps.mul_add(0.75, sample_bps * 0.25)
                }),
                delivery_samples: previous
                    .map_or(1, |previous| previous.delivery_samples.saturating_add(1)),
            }
        };
        self.request
            .subflows
            .get_mut(instance)
            .set_per_flow_rate(model);
    }

    fn record_request_tcp_ack_turnover_sample(
        &mut self,
        context: &ClientPathContext,
        instance: RelayPathInstance,
        sample: PathRateSample,
        sampled_at: Instant,
        candidate_sample: bool,
    ) {
        if instance.key.underlay != UnderlayProtocol::Tcp
            || (self.request.ordered_service != Some(instance) && !candidate_sample)
        {
            return;
        }
        let Some(snapshot) = context.reliable_path_snapshot(instance.key) else {
            return;
        };
        let pto = transport_pto_from_snapshot(Some(snapshot));
        let previous = self
            .request
            .subflows
            .get(instance)
            .and_then(|state| state.tcp_ack_turnover());
        if let Some(model) = RequestTcpAckTurnoverModel::observe(previous, sample, pto, sampled_at)
        {
            self.request
                .subflows
                .get_mut(instance)
                .set_tcp_ack_turnover(model);
        }
    }

    fn prepare_relay_path_decision(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: &Frame,
        lane: FlowLane,
        cause: RelaySendCause,
    ) -> Result<PreparedRequestMultipathDecision, RuntimeError> {
        if remotes.paths.is_empty() {
            return Err(RuntimeError::ReliablePathSessionClosed);
        }
        let membership_generation = remotes.membership_generation();
        let unique_data_payload_bytes = (matches!(frame, Frame::StreamData { .. })
            && !cause.is_repair())
        .then(|| reliable_stream_frame_accounted_bytes(frame));
        if remotes
            .paths
            .last()
            .is_some_and(|path| path.stream.lane.is_bulk())
            && unique_data_payload_bytes.is_some()
        {
            remotes.retry_pending_path_proofs(context);
        }
        self.retry_request_startup_receipt_proof(context, remotes);
        if !cause.is_repair()
            && let Some((offset, _, _)) = reliable_stream_frame_extent(frame)
            && self
                .request
                .flights
                .has_missing_ordering_owner_before_offset(offset, &remotes.path_instances())
        {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        self.next_send_index %= remotes.paths.len();
        if self
            .request
            .ordered_service
            .is_some_and(|owner| !remotes.contains_path_instance(owner))
        {
            self.request.ordered_service = None;
            self.reset_request_subflow_epoch();
        }
        self.reconcile_request_subflow_set(context, remotes);
        if let Some(payload_bytes) = unique_data_payload_bytes {
            self.try_start_request_tcp_capacity_calibration(context, remotes, lane);
            self.try_start_request_quic_capacity_calibration(context, remotes, lane);
            let sealed_owner = self.request.startup.epoch.as_mut().and_then(|epoch| {
                let owner = epoch.startup_owner_key()?;
                epoch
                    .seal_startup_owner_if_next_frame_exceeds_credit(owner, payload_bytes)
                    .then_some(owner)
            });
            if let Some(owner) = sealed_owner {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_startup_receipt",
                    format_args!(
                        "phase=sealed stream_id={} underlay={:?} path_index={} instance_id={} next_payload_bytes={}",
                        self.stream_id.0,
                        owner.key.underlay,
                        owner.key.index,
                        owner.id,
                        payload_bytes,
                    ),
                );
                self.try_enqueue_request_startup_receipt_proof(context, remotes, owner);
            }
        }
        Ok(PreparedRequestMultipathDecision {
            membership_generation,
            unique_data_payload_bytes,
        })
    }

    pub(super) fn plan_relay_path_send(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: &Frame,
        lane: FlowLane,
        cause: RelaySendCause,
        avoid_keys: &[RelayPathKey],
    ) -> Result<RequestMultipathPlan, RuntimeError> {
        let prepared = self.prepare_relay_path_decision(context, remotes, frame, lane, cause)?;
        if let Some(payload_bytes) = prepared.unique_data_payload_bytes {
            let observe_bulk_admission =
                lane.is_bulk() && remotes.paths.len() > 1 && avoid_keys.is_empty();
            let relay_observation = observe_request_relay_scheduling(
                context,
                remotes.stream_id(),
                prepared.membership_generation,
                &remotes.paths,
                Some(frame),
                lane,
                payload_bytes,
                observe_bulk_admission,
            );
            match choose_bulk_relay_path_avoiding(BulkRelayFrameRequest {
                observation: &relay_observation,
                lane,
                frame,
                cursor: self.next_send_index,
                avoid_keys,
                path_flights: Some(&self.request.flights),
                ordered_data_owner: self.request.ordered_service_key(),
                subflow_set: self.request.startup.epoch.as_ref(),
                request_state: Some(RequestSchedulingState {
                    operation: self.request.ack_clock_operation,
                    subflows: &self.request.subflows,
                }),
                attempted_subflows: Some(&self.request.startup.attempted_subflows),
            }) {
                BulkRelayPathChoice::Selected(instance) => {
                    let key = instance.key;
                    let product_mutation = if self
                        .request
                        .ordered_service_key()
                        .is_none_or(|owner| owner == key)
                    {
                        RequestProductSendMutation::InstallService
                    } else {
                        RequestProductSendMutation::PreserveSubflow
                    };
                    let mut selection = RequestMultipathPlan::new(
                        RequestMultipathTarget {
                            membership_generation: relay_observation.membership_generation,
                            instance,
                        },
                        product_mutation,
                    );
                    selection.request_load_expectation =
                        observed_request_load_expectation(&relay_observation, instance)?;
                    return Ok(selection);
                }
                BulkRelayPathChoice::SelectedStartupSubflow {
                    service,
                    candidate,
                    proof,
                } => {
                    let payload_bytes = reliable_stream_frame_accounted_bytes(frame);
                    let admission = self
                        .request
                        .startup
                        .plan_admission(context.mux_limits, service, candidate, payload_bytes)
                        .ok_or(RuntimeError::SenderServiceBlocked)?;
                    return Ok(RequestMultipathPlan {
                        target: RequestMultipathTarget {
                            membership_generation: relay_observation.membership_generation,
                            instance: candidate,
                        },
                        product_mutation: RequestProductSendMutation::CommitStartup(admission),
                        request_load_expectation: observed_request_load_expectation(
                            &relay_observation,
                            candidate,
                        )?,
                        request_proof_expectation: Some(proof),
                    });
                }
                BulkRelayPathChoice::SelectedAckClockCalibration {
                    candidate,
                    target_bytes,
                    proof,
                } => {
                    let payload_bytes = reliable_stream_frame_accounted_bytes(frame) as u64;
                    let entry_offset = reliable_stream_frame_extent(frame)
                        .map(|(offset, _, _)| offset)
                        .unwrap_or(0);
                    let service_key = self
                        .request
                        .ordered_service
                        .map(|service| service.key)
                        .unwrap_or(candidate.key);
                    let (foreign_optional_ranges, foreign_optional_bytes) = if !matches!(
                        self.request.ack_clock_operation,
                        Some(RequestAckClockOperation::Owner { .. })
                    ) {
                        self.request
                            .flights
                            .foreign_ordering_owner_debt_before_offset(entry_offset, &[service_key])
                    } else {
                        (0, 0)
                    };
                    return Ok(RequestMultipathPlan {
                        target: RequestMultipathTarget {
                            membership_generation: relay_observation.membership_generation,
                            instance: candidate,
                        },
                        product_mutation: RequestProductSendMutation::OwnerData {
                            candidate,
                            target_bytes,
                            payload_bytes,
                            entry_offset,
                            foreign_optional_ranges,
                            foreign_optional_bytes,
                        },
                        request_load_expectation: observed_request_load_expectation(
                            &relay_observation,
                            candidate,
                        )?,
                        request_proof_expectation: Some(proof),
                    });
                }
                BulkRelayPathChoice::SelectedAckClockCalibrationFence { service, candidate } => {
                    debug_assert_eq!(Some(service), self.request.ordered_service);
                    let entry_offset = reliable_stream_frame_extent(frame)
                        .map(|(offset, _, _)| offset)
                        .unwrap_or(0);
                    let (foreign_optional_ranges, foreign_optional_bytes) = if self
                        .request
                        .ack_clock_operation
                        .is_none()
                    {
                        self.request
                            .flights
                            .foreign_ordering_owner_debt_before_offset(entry_offset, &[service.key])
                    } else {
                        (0, 0)
                    };
                    return Ok(RequestMultipathPlan {
                        target: RequestMultipathTarget {
                            membership_generation: relay_observation.membership_generation,
                            instance: service,
                        },
                        product_mutation: RequestProductSendMutation::ServiceFence {
                            service,
                            candidate,
                            entry_offset,
                            foreign_optional_ranges,
                            foreign_optional_bytes,
                        },
                        request_load_expectation: observed_request_load_expectation(
                            &relay_observation,
                            service,
                        )?,
                        request_proof_expectation: None,
                    });
                }
                BulkRelayPathChoice::Blocked => return Err(RuntimeError::SenderServiceBlocked),
                BulkRelayPathChoice::NotApplicable => {
                    let instance = match choose_observed_ordinary_data_path(
                        &relay_observation,
                        lane,
                        payload_bytes,
                        self.next_send_index,
                        avoid_keys,
                    ) {
                        ObservedOrdinaryPathChoice::Selected(instance) => instance,
                        ObservedOrdinaryPathChoice::Blocked => {
                            return Err(RuntimeError::SenderServiceBlocked);
                        }
                        ObservedOrdinaryPathChoice::NoLivePath => {
                            return Err(RuntimeError::ReliablePathSessionClosed);
                        }
                    };
                    let mut selection = RequestMultipathPlan::new(
                        RequestMultipathTarget {
                            membership_generation: relay_observation.membership_generation,
                            instance,
                        },
                        RequestProductSendMutation::InstallService,
                    );
                    selection.request_load_expectation =
                        observed_request_load_expectation(&relay_observation, instance)?;
                    return Ok(selection);
                }
            }
        }
        let position = self.choose_lowest_eta_relay_path(
            context,
            remotes,
            frame,
            lane,
            cause,
            avoid_keys,
            prepared.unique_data_payload_bytes.is_some(),
        )?;
        let product_mutation = if prepared.unique_data_payload_bytes.is_some() {
            RequestProductSendMutation::InstallService
        } else {
            RequestProductSendMutation::None
        };
        Ok(RequestMultipathPlan::new(
            RequestMultipathTarget {
                membership_generation: prepared.membership_generation,
                instance: remotes.paths[position].instance(),
            },
            product_mutation,
        ))
    }

    fn reset_request_subflow_epoch(&mut self) {
        self.request.reset_subflow_epoch();
    }

    pub(super) fn commit_enqueued_request_product_send(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        frame: &Frame,
        plan: RequestMultipathPlan,
        position: usize,
        path_count: usize,
    ) {
        let instance = plan.target.instance;
        let mutation = plan.product_mutation;
        self.commit_request_ack_clock_calibration(&mutation);
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
            RequestProductSendMutation::InstallService => {
                context.record_relay_path_send(
                    instance.key.underlay,
                    instance.key.index,
                    sent_bytes,
                );
                self.request.ordered_service = Some(instance);
            }
            RequestProductSendMutation::CommitStartup(admission) => {
                self.request.startup.commit_admission(admission);
                context.record_relay_path_send(
                    instance.key.underlay,
                    instance.key.index,
                    sent_bytes,
                );
                if self
                    .request
                    .startup
                    .epoch
                    .as_ref()
                    .and_then(FlowSubflowSet::startup_owner_key)
                    == Some(instance)
                {
                    self.request
                        .startup
                        .first_sent_at
                        .entry(instance)
                        .or_insert_with(Instant::now);
                    self.try_enqueue_request_startup_receipt_proof(context, remotes, instance);
                }
            }
            RequestProductSendMutation::PreserveSubflow
            | RequestProductSendMutation::ServiceFence { .. }
            | RequestProductSendMutation::OwnerData { .. } => {
                context.record_relay_path_send(
                    instance.key.underlay,
                    instance.key.index,
                    sent_bytes,
                );
            }
            RequestProductSendMutation::None => {
                debug_assert!(false, "STREAM_DATA selection requires a product mutation");
            }
        }
        self.next_send_index = if path_count == 0 {
            0
        } else {
            (position + 1) % path_count
        };
    }

    fn commit_request_ack_clock_calibration(&mut self, commit: &RequestProductSendMutation) {
        match commit {
            RequestProductSendMutation::ServiceFence {
                service,
                candidate,
                entry_offset,
                foreign_optional_ranges,
                foreign_optional_bytes,
            } => {
                let (
                    service,
                    candidate,
                    entry_offset,
                    foreign_optional_ranges,
                    foreign_optional_bytes,
                ) = (
                    *service,
                    *candidate,
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
                if let Some(RequestAckClockOperation::Owner {
                    candidate: owner, ..
                }) = self.request.ack_clock_operation
                {
                    debug_assert_eq!(owner, candidate);
                    return;
                }
                debug_assert_eq!(self.request.ordered_service, Some(service));
                let pending = RequestAckClockOperation::Pending { service, candidate };
                if self.request.ack_clock_operation == Some(pending) {
                    return;
                }
                self.request.ack_clock_operation = Some(pending);
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "ack_clock_calibration",
                    format_args!(
                        "phase=pending_started stream_id={} service_underlay={:?} service_index={} service_instance={} candidate_index={} candidate_instance={} entry_offset={} foreign_optional_ranges={} foreign_optional_bytes={}",
                        self.stream_id.0,
                        service.key.underlay,
                        service.key.index,
                        service.id,
                        candidate.key.index,
                        candidate.id,
                        entry_offset,
                        foreign_optional_ranges,
                        foreign_optional_bytes,
                    ),
                );
            }
            RequestProductSendMutation::OwnerData {
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
                    debug_assert!(false, "calibration owner changed before enqueue commit");
                    return;
                }
                if let Some(RequestAckClockOperation::Pending {
                    service,
                    candidate: pending,
                }) = self.request.ack_clock_operation
                {
                    debug_assert_eq!(pending, candidate);
                    debug_assert_eq!(Some(service), self.request.ordered_service);
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
                        .subflows
                        .get(candidate)
                        .and_then(|state| state.ack_clock_calibration_bytes())
                        .unwrap_or(0)
                };
                let spent_bytes = previous_bytes.saturating_add(payload_bytes);
                self.request.ack_clock_operation = Some(RequestAckClockOperation::Owner {
                    candidate,
                    target_bytes,
                });
                let candidate_state = self.request.subflows.get_mut(candidate);
                candidate_state.set_ack_clock_calibration_target(target_bytes);
                candidate_state.set_ack_clock_calibration_bytes(spent_bytes);
                #[cfg(feature = "lab-diagnostics")]
                {
                    if beginning {
                        lab_diagnostic(
                            "ack_clock_calibration",
                            format_args!(
                                "phase=owner_started stream_id={} underlay={:?} path_index={} instance_id={} payload_bytes={} target_bytes={} entry_offset={} foreign_optional_ranges={} foreign_optional_bytes={}",
                                self.stream_id.0,
                                candidate.key.underlay,
                                candidate.key.index,
                                candidate.id,
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
                            "ack_clock_calibration",
                            format_args!(
                                "phase=target_spent stream_id={} underlay={:?} path_index={} instance_id={} spent_bytes={} target_bytes={}",
                                self.stream_id.0,
                                candidate.key.underlay,
                                candidate.key.index,
                                candidate.id,
                                spent_bytes,
                                target_bytes,
                            ),
                        );
                    }
                    static TRACE_COUNT: std::sync::atomic::AtomicU64 =
                        std::sync::atomic::AtomicU64::new(0);
                    let count = TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if count < 16 || count % 256 == 0 {
                        lab_diagnostic(
                            "ack_clock_calibration",
                            format_args!(
                                "phase=selected stream_id={} underlay={:?} path_index={} instance_id={} payload_bytes={} spent_bytes={} target_bytes={}",
                                self.stream_id.0,
                                candidate.key.underlay,
                                candidate.key.index,
                                candidate.id,
                                payload_bytes,
                                spent_bytes,
                                target_bytes,
                            ),
                        );
                    }
                }
            }
            RequestProductSendMutation::None
            | RequestProductSendMutation::InstallService
            | RequestProductSendMutation::PreserveSubflow
            | RequestProductSendMutation::CommitStartup(_) => {}
        }
    }

    fn retry_request_startup_receipt_proof(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
    ) {
        let Some(owner) = self
            .request
            .startup
            .epoch
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key)
        else {
            return;
        };
        self.try_enqueue_request_startup_receipt_proof(context, remotes, owner);
    }

    fn try_enqueue_request_startup_receipt_proof(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        owner: RelayPathInstance,
    ) {
        if owner.key.underlay != UnderlayProtocol::Tcp {
            return;
        }
        let proof_generation = context
            .relay_path_proof_generation(owner.key.underlay, owner.key.index)
            .unwrap_or(0);
        if self
            .request
            .startup
            .receipt_proofs
            .get(&owner)
            .is_some_and(|(_, generation)| *generation == proof_generation)
            || !self
                .request
                .startup
                .epoch
                .as_ref()
                .is_some_and(|epoch| epoch.startup_owner_sample_sealed(owner))
        {
            return;
        }
        let Some(path) = remotes.paths.iter().find(|path| {
            path.instance() == owner && path.placement == RelayPathPlacement::Validation
        }) else {
            return;
        };
        match path
            .stream
            .enqueue_stream_ordered_path_proof(path.stream.lane)
        {
            Ok(Some(proof_id)) => {
                self.request
                    .startup
                    .receipt_proofs
                    .insert(owner, (proof_id, proof_generation));
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_startup_receipt",
                    format_args!(
                        "phase=queued stream_id={} underlay={:?} path_index={} instance_id={} proof_id={} proof_generation={}",
                        self.stream_id.0,
                        owner.key.underlay,
                        owner.key.index,
                        owner.id,
                        proof_id,
                        proof_generation,
                    ),
                );
            }
            Ok(None) => {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_startup_receipt",
                    format_args!(
                        "phase=unsupported stream_id={} underlay={:?} path_index={} instance_id={} proof_generation={}",
                        self.stream_id.0,
                        owner.key.underlay,
                        owner.key.index,
                        owner.id,
                        proof_generation,
                    ),
                );
            }
            Err(err) => {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_startup_receipt",
                    format_args!(
                        "phase=queue_failed stream_id={} underlay={:?} path_index={} instance_id={} proof_generation={} error={}",
                        self.stream_id.0,
                        owner.key.underlay,
                        owner.key.index,
                        owner.id,
                        proof_generation,
                        err,
                    ),
                );
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = err;
            }
        }
    }

    fn request_ack_clock_calibration_target_is_sealed(&self, target: RelayPathInstance) -> bool {
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
                    .subflows
                    .get(target)
                    .and_then(|state| state.ack_clock_calibration_bytes())
                    .is_some_and(|spent| spent >= target_bytes)
            })
    }

    fn revoke_request_tcp_capacity_calibration(
        &mut self,
        target: RelayPathInstance,
        preserve_committed_product: bool,
    ) -> bool {
        if let Some(state) = self.request.subflows.get_existing_mut(target) {
            state.clear_tcp_capacity_proven();
        }
        self.tcp_capacity.remove(target);
        let product_transaction_preserved = preserve_committed_product
            && self.request_ack_clock_calibration_target_is_sealed(target);
        if product_transaction_preserved {
            // Carrier freshness admits a bounded product transaction but does
            // not own it. Once the fixed target is sealed, keep its exact ACK
            // evidence until product proof or a real path lifecycle change.
            return true;
        }
        if let Some(state) = self.request.subflows.get_existing_mut(target) {
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
                let target_state = self.request.subflows.get_mut(target);
                target_state.mark_tcp_capacity_proven();
                target_state.mark_graduated();
                target_state
                    .rate_evidence_mut(proof.accepted_at)
                    .seed_ack_boundary(proof.accepted_at);
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_tcp_capacity_calibration",
                    format_args!(
                        "phase=carrier_proven stream_id={} path_index={} instance_id={} calibration_id={} train_bytes={} rate_mbps={:.3} proof_ms={}",
                        self.stream_id.0,
                        target.key.index,
                        target.id,
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
            RequestTcpCapacityEvent::ProductHandoffComplete {
                target,
                calibration,
            } => {
                let _token = calibration.token;
                if let Some(state) = self.request.subflows.get_existing_mut(target) {
                    state.clear_tcp_capacity_proven();
                }
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_tcp_capacity_calibration",
                    format_args!(
                        "phase=handoff_complete stream_id={} path_index={} instance_id={} calibration_id={}",
                        self.stream_id.0, target.key.index, target.id, _token,
                    ),
                );
                drop(calibration);
            }
            RequestTcpCapacityEvent::CarrierAuthorityRetired {
                target,
                calibration,
                cause,
            } => {
                let _token = calibration.token;
                let natural_expiry = cause == RequestTcpCapacityRetirement::AuthorityExpired;
                let _product_transaction_preserved =
                    self.revoke_request_tcp_capacity_calibration(target, natural_expiry);
                #[cfg(feature = "lab-diagnostics")]
                match cause {
                    RequestTcpCapacityRetirement::AuthorityExpired => lab_diagnostic(
                        "request_tcp_capacity_calibration",
                        format_args!(
                            "phase=carrier_authority_expired stream_id={} path_index={} instance_id={} calibration_id={} product_transaction_preserved={}",
                            self.stream_id.0,
                            target.key.index,
                            target.id,
                            _token,
                            _product_transaction_preserved,
                        ),
                    ),
                    RequestTcpCapacityRetirement::AuthorityLost => lab_diagnostic(
                        "request_tcp_capacity_calibration",
                        format_args!(
                            "phase=revoked stream_id={} path_index={} instance_id={} calibration_id={} reason=carrier_authority_lost",
                            self.stream_id.0, target.key.index, target.id, _token,
                        ),
                    ),
                    RequestTcpCapacityRetirement::Detached
                    | RequestTcpCapacityRetirement::PublicationExpired => {}
                }
                // Product authority is retired before lease Drop can enter the
                // shared path-state lock.
                drop(calibration);
            }
        }
    }

    fn apply_request_quic_capacity_event(&mut self, event: RequestQuicCapacityEvent) {
        let (phase, target, _token) = match event {
            RequestQuicCapacityEvent::CarrierProofAccepted { target, token } => {
                self.request.subflows.get_mut(target).mark_graduated();
                ("graduated", target, token)
            }
            RequestQuicCapacityEvent::ProductHandoffComplete {
                target,
                calibration,
            } => {
                let token = calibration.token;
                drop(calibration);
                ("handoff_complete", target, token)
            }
            RequestQuicCapacityEvent::ProductHandoffExpired {
                target,
                calibration,
            } => {
                // Probe credit is transactional and cannot survive a failed
                // handoff into product-owned evidence.
                if let Some(state) = self.request.subflows.get_existing_mut(target) {
                    state.clear_graduated();
                }
                let token = calibration.token;
                // Product authority is gone before lease Drop can enter the
                // shared path-state lock.
                drop(calibration);
                ("handoff_expired", target, token)
            }
        };
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "request_quic_capacity_calibration",
            format_args!(
                "phase={} stream_id={} path_index={} instance_id={} calibration_id={}",
                phase, self.stream_id.0, target.key.index, target.id, _token,
            ),
        );
        #[cfg(not(feature = "lab-diagnostics"))]
        let _ = (phase, target, _token);
    }

    fn reconcile_request_subflow_set(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
    ) {
        let membership_generation = remotes.membership_generation();
        if self.request.membership_generation != Some(membership_generation) {
            let live_instances = remotes.path_instances().into_iter().collect::<HashSet<_>>();
            self.request.startup.retain_live(&live_instances);
            self.request.subflows.retain_live(&live_instances);
            self.request.membership_generation = Some(membership_generation);
        }
        let now = Instant::now();
        let quic_query = self.quic_capacity.reconciliation_query();
        let reconciliation = context.request_capacity_reconciliation_view(
            self.stream_id,
            self.tcp_capacity.proof_queries(),
            quic_query,
            now,
        );
        let completed_product_handoffs = self
            .tcp_capacity
            .calibrations
            .keys()
            .copied()
            .filter(|target| {
                self.request.subflows.get(*target).is_some_and(|state| {
                    state.ack_clock_proven() && state.per_flow_rate().is_some()
                })
            })
            .collect::<HashSet<_>>();
        let tcp_events =
            self.tcp_capacity
                .reconcile(&reconciliation, remotes, &completed_product_handoffs);
        for event in tcp_events {
            self.apply_request_tcp_capacity_event(event);
        }
        for event in self
            .quic_capacity
            .reconcile(context, &reconciliation, remotes)
        {
            self.apply_request_quic_capacity_event(event);
        }
        if self.request.ack_clock_operation.is_some_and(|operation| {
            let RequestAckClockOperation::Pending { service, candidate } = operation else {
                return false;
            };
            self.request.ordered_service != Some(service)
                || service.key.underlay != UnderlayProtocol::Tcp
                || self
                    .request
                    .subflows
                    .get(candidate)
                    .is_some_and(|state| state.ack_clock_proven())
                || !self
                    .request
                    .subflows
                    .get(candidate)
                    .is_some_and(|state| state.graduated())
                || !self.request.subflows.get(candidate).is_some_and(|state| {
                    state.ack_clock_first_window() || state.tcp_capacity_proven()
                })
                || !remotes.paths.iter().any(|path| {
                    path.instance() == service && path.placement == RelayPathPlacement::Active
                })
                || !remotes.paths.iter().any(|path| {
                    path.instance() == candidate
                        && path.placement == RelayPathPlacement::Validation
                        && path.key().underlay == UnderlayProtocol::Tcp
                        && path.key().underlay == service.key.underlay
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
                .subflows
                .get(candidate)
                .is_some_and(|state| state.ack_clock_proven())
            {
                self.request.ack_clock_operation = None;
            } else {
                let placement_valid = self
                    .request
                    .subflows
                    .get(candidate)
                    .is_some_and(|state| state.graduated())
                    && remotes.paths.iter().any(|path| {
                        path.instance() == candidate
                            && path.placement == RelayPathPlacement::Validation
                    });
                let transaction_authorized = self
                    .request_ack_clock_calibration_target_is_sealed(candidate)
                    || self.request.subflows.get(candidate).is_some_and(|state| {
                        state.ack_clock_first_window() || state.tcp_capacity_proven()
                    });
                if !placement_valid || !transaction_authorized {
                    // A sealed AwaitingAck target remains exact-instance state.
                    // Real placement loss or a partial transaction without its
                    // entry proof performs the full abort cleanup.
                    self.revoke_request_tcp_capacity_calibration(candidate, false);
                }
            }
        }
        for instance in self.quic_capacity.native_evidence_targets(
            context,
            self.request.ordered_service,
            remotes,
            now,
        ) {
            self.request.subflows.get_mut(instance).mark_graduated();
        }
        let service = self.request.ordered_service.filter(|owner| {
            remotes.paths.iter().any(|path| {
                path.instance() == *owner && path.placement == RelayPathPlacement::Active
            })
        });
        if self
            .request
            .startup
            .epoch
            .as_ref()
            .is_some_and(|epoch| service.is_none_or(|service| epoch.service_key() != service))
        {
            self.reset_request_subflow_epoch();
            return;
        }
        let Some(owner) = self
            .request
            .startup
            .epoch
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key)
        else {
            return;
        };
        if owner.key.underlay != UnderlayProtocol::Tcp {
            self.reset_request_subflow_epoch();
            return;
        }
        if !remotes.paths.iter().any(|path| {
            path.instance() == owner && path.placement == RelayPathPlacement::Validation
        }) {
            self.request.startup.epoch = None;
            self.request.startup.acked_bytes.remove(&owner);
            self.request.startup.first_sent_at.remove(&owner);
            self.request.startup.rate_evidence.remove(&owner);
            self.request.startup.receipt_proofs.remove(&owner);
            return;
        }
        let required_evidence_bytes = self
            .request
            .startup
            .epoch
            .as_ref()
            .and_then(|epoch| epoch.startup_owner_sealed_sample_bytes(owner))
            .unwrap_or(u64::MAX);
        let receipt_acked_at = self
            .request
            .startup
            .receipt_proofs
            .get(&owner)
            .copied()
            .and_then(|(proof_id, generation)| {
                (context.relay_path_proof_generation(owner.key.underlay, owner.key.index)
                    == Some(generation))
                .then_some(proof_id)
            })
            .and_then(|proof_id| {
                remotes
                    .paths
                    .iter()
                    .find(|path| path.instance() == owner)
                    .and_then(|path| {
                        context.relay_path_fresh_proof_acked_as_of(
                            owner.key.underlay,
                            owner.key.index,
                            proof_id,
                            path.attached_at,
                            now,
                        )
                    })
            });
        if let Some(receipt_acked_at) = receipt_acked_at
            && !self.request.startup.rate_evidence.contains(&owner)
            && let Some(first_sent_at) = self.request.startup.first_sent_at.get(&owner).copied()
            && let Some(sample) = PathRateSample::new(
                required_evidence_bytes,
                receipt_acked_at.saturating_duration_since(first_sent_at),
            )
        {
            self.request.startup.rate_evidence.insert(owner);
            self.request.subflows.get_mut(owner).mark_rate_proven();
            self.record_request_per_flow_rate_sample(owner, sample, false);
            context.mark_relay_path_rate_sample(owner.key.underlay, owner.key.index, sample);
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "request_startup_receipt",
                format_args!(
                    "phase=rate_sample stream_id={} underlay={:?} path_index={} instance_id={} evidence_bytes={} elapsed_us={} rate_bps={}",
                    self.stream_id.0,
                    owner.key.underlay,
                    owner.key.index,
                    owner.id,
                    required_evidence_bytes,
                    receipt_acked_at
                        .saturating_duration_since(first_sent_at)
                        .as_micros(),
                    sample.rate_bps(),
                ),
            );
        }
        if let Some(receipt_acked_at) = receipt_acked_at
            && self.request.startup.rate_evidence.contains(&owner)
            && self
                .request
                .subflows
                .get_mut(owner)
                .mark_ack_clock_first_window()
        {
            // The ordered receipt follows the sealed startup sample on this
            // exact TCP attachment. Once product flight also drains below, it
            // is the causal boundary for the first calibration window.
            self.request
                .subflows
                .get_mut(owner)
                .rate_evidence_mut(receipt_acked_at)
                .seed_ack_boundary(receipt_acked_at);
        }
        if self.request.startup.rate_evidence.contains(&owner)
            && !self
                .request
                .flights
                .has_ordering_owner_flights_for_instance(owner)
            && let Some(epoch) = self.request.startup.epoch.as_mut()
        {
            let graduated = epoch.graduate_startup_owner(owner);
            debug_assert!(graduated);
            self.request.subflows.get_mut(owner).mark_graduated();
            self.request.startup.receipt_proofs.remove(&owner);
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "request_startup_receipt",
                format_args!(
                    "phase=graduated stream_id={} underlay={:?} path_index={} instance_id={}",
                    self.stream_id.0, owner.key.underlay, owner.key.index, owner.id,
                ),
            );
        }
    }

    #[cfg(test)]
    pub(super) fn release_normalized_acked_ranges(
        &mut self,
        context: &ClientPathContext,
        ranges: &[OffsetRange],
    ) {
        let _ = self.release_normalized_acked_ranges_with_owner_progress(context, ranges);
    }

    /// Releases every exact flight copy and derives one neutral window effect.
    pub(super) fn apply_product_ack(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        ranges: &[OffsetRange],
        acked_at: Instant,
    ) -> RequestWindowGrowthEvidence<RelayPathInstance> {
        let mut owner_progress =
            self.release_normalized_acked_ranges_with_owner_progress_at(context, ranges, acked_at);
        self.request
            .ordered_service
            .filter(|service| remotes.contains_path_instance(*service))
            .map_or(RequestWindowGrowthEvidence::None, |service| {
                match service.key.underlay {
                    UnderlayProtocol::Tcp => {
                        let owner_capable = owner_progress.iter().any(|progress| {
                            self.request_owner_ack_can_grow_window(
                                remotes,
                                Some(service),
                                progress.instance,
                            )
                        });
                        if !owner_capable {
                            RequestWindowGrowthEvidence::None
                        } else {
                            RequestWindowGrowthEvidence::AckClockTurnover {
                                service,
                                turnover_bytes: self.request_tcp_owner_ack_turnover_bytes(
                                    remotes,
                                    Some(service),
                                    acked_at,
                                ),
                                observed_at: acked_at,
                            }
                        }
                    }
                    UnderlayProtocol::Udp => {
                        owner_progress.retain(|progress| {
                            self.request_owner_ack_can_grow_window(
                                remotes,
                                Some(service),
                                progress.instance,
                            )
                        });
                        if owner_progress.is_empty() {
                            RequestWindowGrowthEvidence::None
                        } else {
                            let snapshot = context.reliable_path_snapshot(service.key);
                            RequestWindowGrowthEvidence::OwnerAckCredits {
                                service,
                                credits: owner_progress,
                                growth_interval: transport_pto_from_snapshot(snapshot),
                                observed_at: acked_at,
                            }
                        }
                    }
                }
            })
    }

    #[cfg(test)]
    fn release_normalized_acked_ranges_with_owner_progress(
        &mut self,
        context: &ClientPathContext,
        ranges: &[OffsetRange],
    ) -> smallvec::SmallVec<[RequestOwnerAckProgress<RelayPathInstance>; 4]> {
        self.release_normalized_acked_ranges_with_owner_progress_at(context, ranges, Instant::now())
    }

    fn release_normalized_acked_ranges_with_owner_progress_at(
        &mut self,
        context: &ClientPathContext,
        ranges: &[OffsetRange],
        acked_at: Instant,
    ) -> smallvec::SmallVec<[RequestOwnerAckProgress<RelayPathInstance>; 4]> {
        let startup_owner = self
            .request
            .startup
            .epoch
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key);
        let startup_required_bytes = self
            .request
            .startup
            .epoch
            .as_ref()
            .and_then(|epoch| {
                startup_owner.and_then(|owner| epoch.startup_owner_sealed_sample_bytes(owner))
            })
            .unwrap_or(u64::MAX);
        let mut ordinary_owner_samples =
            HashMap::<RelayPathInstance, (u64, Instant, Instant)>::new();
        let mut owner_progress =
            smallvec::SmallVec::<[RequestOwnerAckProgress<RelayPathInstance>; 4]>::new();
        for release in self.request.flights.release_normalized_acked_ranges(ranges) {
            self.request
                .missing_owner_repair_attempts
                .remove(&release.instance);
            context.release_relay_path_inflight(
                release.key.underlay,
                release.key.index,
                release.bytes,
            );
            if release.path_proving {
                context.record_relay_path_product_ack(
                    self.stream_id,
                    release.instance,
                    release.bytes,
                    release.sent_at,
                    acked_at,
                );
                if let Some(progress) = owner_progress
                    .iter_mut()
                    .find(|progress| progress.instance == release.instance)
                {
                    progress.bytes = progress.bytes.saturating_add(release.bytes);
                } else {
                    owner_progress.push(RequestOwnerAckProgress {
                        instance: release.instance,
                        bytes: release.bytes,
                    });
                }
            }
            if release.path_proving
                && release.key.underlay == UnderlayProtocol::Tcp
                && startup_owner == Some(release.instance)
            {
                let first_sent_at = self
                    .request
                    .startup
                    .first_sent_at
                    .entry(release.instance)
                    .or_insert(release.sent_at);
                *first_sent_at = (*first_sent_at).min(release.sent_at);
                let acked_bytes = self
                    .request
                    .startup
                    .acked_bytes
                    .entry(release.instance)
                    .or_default();
                *acked_bytes = acked_bytes.saturating_add(release.bytes as u64);
            }
            if release.path_proving && startup_owner != Some(release.instance) {
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
                    release.key.underlay,
                    release.key.index,
                    release.instance.id,
                    release.bytes,
                    release.elapsed.as_secs_f64() * 1000.0,
                    release.path_proving,
                ),
            );
        }
        if let Some(owner) = startup_owner
            && owner.key.underlay == UnderlayProtocol::Tcp
            && let (Some(acked_bytes), Some(first_sent_at)) = (
                self.request.startup.acked_bytes.get(&owner).copied(),
                self.request.startup.first_sent_at.get(&owner).copied(),
            )
            && acked_bytes >= startup_required_bytes
            && let Some(sample) = PathRateSample::new(
                acked_bytes,
                acked_at.saturating_duration_since(first_sent_at),
            )
            && self.request.startup.rate_evidence.insert(owner)
        {
            self.request.subflows.get_mut(owner).mark_rate_proven();
            self.record_request_per_flow_rate_sample(owner, sample, false);
            context.mark_relay_path_rate_sample(owner.key.underlay, owner.key.index, sample);
            if self
                .request
                .subflows
                .get_mut(owner)
                .mark_ack_clock_first_window()
            {
                // The exact product ACK that completes the sealed TCP startup
                // owner window is also a causal boundary: every calibration
                // byte selected after this point is post-boundary by
                // construction. The explicit path receipt remains an
                // equivalent boundary when it arrives first.
                self.request
                    .subflows
                    .get_mut(owner)
                    .rate_evidence_mut(acked_at)
                    .seed_ack_boundary(acked_at);
            }
        }
        for (instance, (bytes, first_sent_at, latest_sent_at)) in ordinary_owner_samples {
            // TCP lacks carrier-native delivery telemetry, so its product ACK
            // fallback needs a representative window. QUIC keeps its existing
            // small product-provenance threshold; carrier ACKs own its rate.
            let is_ordered_service = self.request.ordered_service == Some(instance);
            let coverage_floor_bytes = request_path_rate_coverage_floor_bytes(
                instance.key.underlay,
                is_ordered_service,
                self.request
                    .subflows
                    .get(instance)
                    .and_then(|state| state.ack_clock_calibration_target()),
                context.mux_limits,
            );
            let (update, has_exact_path_provenance, exact_attributed_bytes) = {
                let evidence = self
                    .request
                    .subflows
                    .get_mut(instance)
                    .rate_evidence_mut(first_sent_at);
                let update = evidence.observe(
                    bytes,
                    first_sent_at,
                    latest_sent_at,
                    acked_at,
                    coverage_floor_bytes,
                    !is_ordered_service,
                );
                (
                    update,
                    evidence.has_exact_path_provenance(),
                    evidence.exact_attributed_bytes(),
                )
            };
            if has_exact_path_provenance {
                // Exact ownership is enough to establish that this flow used
                // the path. It is not enough to publish a rate sample.
                self.request.subflows.get_mut(instance).mark_rate_proven();
            }
            if let RequestPathRateEvidenceUpdate::Proven {
                sample,
                first_window,
            } = update
            {
                if instance.key.underlay == UnderlayProtocol::Tcp
                    && is_ordered_service
                    && let Some(sample) = sample
                {
                    if !first_window {
                        self.record_request_tcp_ack_turnover_sample(
                            context, instance, sample, acked_at, false,
                        );
                    }
                    context.mark_relay_path_rate_sample(
                        instance.key.underlay,
                        instance.key.index,
                        sample,
                    );
                    if !first_window {
                        self.record_request_per_flow_rate_sample(instance, sample, false);
                    }
                } else if instance.key.underlay == UnderlayProtocol::Tcp && first_window {
                    self.request
                        .subflows
                        .get_mut(instance)
                        .mark_ack_clock_first_window();
                } else if instance.key.underlay == UnderlayProtocol::Tcp
                    && let Some(sample) = sample
                {
                    let replace_startup_rate = self
                        .request
                        .subflows
                        .get_mut(instance)
                        .mark_ack_clock_proven();
                    let turnover_authorized = !replace_startup_rate
                        && self
                            .request
                            .subflows
                            .get(instance)
                            .and_then(|state| state.ack_clock_calibration_target())
                            .is_some_and(|target_bytes| {
                                request_tcp_candidate_turnover_authorized(
                                    exact_attributed_bytes,
                                    target_bytes,
                                    coverage_floor_bytes,
                                )
                            });
                    if turnover_authorized {
                        self.request
                            .subflows
                            .get_mut(instance)
                            .mark_window_turnover_proven();
                    }
                    if self
                        .request
                        .ack_clock_operation
                        .is_some_and(|operation| operation.candidate() == instance)
                    {
                        self.request.ack_clock_operation = None;
                    }
                    self.record_request_per_flow_rate_sample(
                        instance,
                        sample,
                        replace_startup_rate,
                    );
                    self.record_request_tcp_ack_turnover_sample(
                        context, instance, sample, acked_at, true,
                    );
                    context.mark_relay_path_ack_clock_rate_sample(
                        instance.key.underlay,
                        instance.key.index,
                        sample,
                        replace_startup_rate,
                    );
                    #[cfg(feature = "lab-diagnostics")]
                    {
                        lab_diagnostic(
                            "ack_clock_calibration",
                            format_args!(
                                "phase=ack_clock_sample stream_id={} underlay={:?} path_index={} instance_id={} evidence_bytes={} sample_elapsed_us={} replace_startup_rate={} rate_bps={}",
                                self.stream_id.0,
                                instance.key.underlay,
                                instance.key.index,
                                instance.id,
                                sample.bytes(),
                                sample.elapsed().as_micros(),
                                replace_startup_rate,
                                sample.rate_bps(),
                            ),
                        );
                    }
                }
            }
        }
        owner_progress
    }

    pub(super) fn discard_unusable_live_owner_tail_repairs(
        &self,
        sender_queue: &mut ReliableRelaySenderQueue,
        remotes: &ReliableRelayRemoteSet,
    ) -> usize {
        let live_instances = self.request_owner_capable_instances(remotes);
        let live_keys = live_instances
            .iter()
            .map(|instance| instance.key)
            .collect::<Vec<_>>();
        sender_queue.discard_unusable_live_owner_tail_repairs(|frame| {
            let owner_keys = self
                .request
                .flights
                .ordering_owner_keys_for_frame(frame, &live_instances);
            !owner_keys.is_empty() && live_keys.iter().any(|key| !owner_keys.contains(key))
        })
    }

    pub(super) fn discard_stale_persistent_ack_gap_repairs(
        &self,
        sender_queue: &mut ReliableRelaySenderQueue,
        remotes: &ReliableRelayRemoteSet,
    ) -> usize {
        let live_instances = remotes.path_instances();
        sender_queue.discard_stale_persistent_ack_gap_repairs(|cause| {
            cause
                .persistent_client_target()
                .is_none_or(|target| live_instances.contains(&target))
                && cause.persistent_server_target().is_none()
        })
    }

    fn request_owner_capable_instances(
        &self,
        remotes: &ReliableRelayRemoteSet,
    ) -> Vec<RelayPathInstance> {
        let startup_owner = self
            .request
            .startup
            .epoch
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key);
        remotes
            .paths
            .iter()
            .filter(|path| {
                path.placement != RelayPathPlacement::Validation
                    || startup_owner == Some(path.instance())
                    || self
                        .request
                        .subflows
                        .get(path.instance())
                        .is_some_and(|state| state.graduated())
                    || self
                        .request
                        .flights
                        .has_ordering_owner_flights_for_instance(path.instance())
            })
            .map(ReliableRelayRemotePath::instance)
            .collect()
    }

    pub(super) fn request_ordered_service_instance(&self) -> Option<RelayPathInstance> {
        self.request.ordered_service
    }

    fn request_owner_ack_can_grow_window(
        &self,
        remotes: &ReliableRelayRemoteSet,
        service_instance: Option<RelayPathInstance>,
        instance: RelayPathInstance,
    ) -> bool {
        let Some(service) = service_instance else {
            return false;
        };
        if self.request.ordered_service != Some(service)
            || !remotes.contains_path_instance(service)
            || service.key.underlay != instance.key.underlay
        {
            return false;
        }
        remotes.paths.iter().any(|path| {
            path.instance() == instance
                && (instance == service
                    || (self
                        .request
                        .subflows
                        .get(instance)
                        .is_some_and(|state| state.graduated())
                        && (instance.key.underlay == UnderlayProtocol::Udp
                            || self
                                .request
                                .subflows
                                .get(instance)
                                .is_some_and(|state| state.ack_clock_proven()))))
        })
    }

    fn request_tcp_owner_ack_turnover_bytes(
        &self,
        remotes: &ReliableRelayRemoteSet,
        service_instance: Option<RelayPathInstance>,
        now: Instant,
    ) -> usize {
        let Some(service) = service_instance.filter(|service| {
            service.key.underlay == UnderlayProtocol::Tcp
                && self.request.ordered_service == Some(*service)
                && remotes.contains_path_instance(*service)
        }) else {
            return 0;
        };
        remotes
            .paths
            .iter()
            .filter_map(|path| {
                let instance = path.instance();
                if !self.request_owner_ack_can_grow_window(remotes, Some(service), instance) {
                    return None;
                }
                if instance != service
                    && !self
                        .request
                        .subflows
                        .get(instance)
                        .is_some_and(|state| state.window_turnover_proven())
                {
                    return None;
                }
                self.request
                    .subflows
                    .get(instance)
                    .and_then(|state| state.tcp_ack_turnover())
                    .filter(|model| model.is_fresh_at(now))
                    .map(|model| model.turnover_bytes)
            })
            .sum::<f64>()
            .ceil() as usize
    }

    pub(super) fn unreported_missing_owner_instances(
        &mut self,
        remotes: &ReliableRelayRemoteSet,
        retry_after: Duration,
    ) -> Vec<RelayPathInstance> {
        let owner_instances = self.request.flights.ordering_owner_instances();
        self.request
            .missing_owner_repair_attempts
            .retain(|instance, _| {
                owner_instances.contains(instance) && !remotes.contains_path_instance(*instance)
            });
        let now = Instant::now();
        owner_instances
            .into_iter()
            .filter(|instance| {
                !remotes.contains_path_instance(*instance)
                    && self
                        .request
                        .missing_owner_repair_attempts
                        .get(instance)
                        .is_none_or(|attempt| {
                            now.saturating_duration_since(*attempt) >= retry_after
                        })
            })
            .collect()
    }

    #[cfg(test)]
    pub(super) fn unreported_missing_owner_keys(
        &mut self,
        remotes: &ReliableRelayRemoteSet,
        retry_after: Duration,
    ) -> Vec<RelayPathKey> {
        let mut keys = Vec::new();
        for instance in self.unreported_missing_owner_instances(remotes, retry_after) {
            if !keys.contains(&instance.key) {
                keys.push(instance.key);
            }
        }
        keys
    }

    pub(super) fn release_all(&mut self, context: &ClientPathContext) {
        for release in self.request.flights.drain_all() {
            context.release_relay_path_inflight(
                release.key.underlay,
                release.key.index,
                release.bytes,
            );
        }
    }

    #[cfg(test)]
    pub(super) fn age_product_flights_for_test(&mut self, age: Duration) {
        self.request.flights.age_product_flights_for_test(age);
    }

    #[cfg(test)]
    pub(super) fn record_owner_frame_for_test(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
    ) {
        self.request
            .flights
            .record_owner_frame_instance(instance, frame);
        self.request.ordered_service = Some(instance);
    }

    #[cfg(test)]
    pub(super) fn ordered_data_owner_for_test(&self) -> Option<RelayPathKey> {
        self.request.ordered_service_key()
    }

    fn choose_lowest_eta_relay_path(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        frame: &Frame,
        lane: FlowLane,
        cause: RelaySendCause,
        avoid_keys: &[RelayPathKey],
        ordinary_stream_data: bool,
    ) -> Result<usize, RuntimeError> {
        let persistent_ack_gap_repair = cause.is_persistent_ack_gap_repair();
        let required_persistent_target = cause.persistent_client_target();
        let invalid_persistent_target =
            matches!(cause, RelaySendCause::PersistentServerAckGapRepair(_));
        let requires_distinct_output =
            cause == RelaySendCause::LiveOwnerTailRepair || persistent_ack_gap_repair;
        let payload_bytes = reliable_stream_frame_accounted_bytes(frame);
        if cause == RelaySendCause::RecvProgressRecovery
            && let Some(position) = choose_repair_recv_progress_path_position(remotes, frame, cause)
        {
            return Ok(position);
        }
        if cause.is_recv_progress() {
            if let Some(position) = choose_active_recv_progress_path_position(remotes, frame, cause)
            {
                return Ok(position);
            }
        }
        let has_active_path = remotes
            .paths
            .iter()
            .any(|path| path.placement == RelayPathPlacement::Active);
        let ordinary_path_allowed = |path: &ReliableRelayRemotePath| {
            (!ordinary_stream_data
                || !has_active_path
                || path.placement == RelayPathPlacement::Active)
                && (cause != RelaySendCause::RecvProgressRecovery
                    || path.placement != RelayPathPlacement::Validation)
                && (cause != RelaySendCause::LiveOwnerTailRepair
                    || path.placement != RelayPathPlacement::Validation)
                && !invalid_persistent_target
                && required_persistent_target.is_none_or(|required| path.instance() == required)
                && (!persistent_ack_gap_repair
                    || context
                        .relay_path_has_bulk_model_evidence(path.key().underlay, path.key().index))
        };
        let can_enqueue = |path: &ReliableRelayRemotePath| {
            relay_path_can_enqueue_frame_for_cause_now(path, frame, cause)
        };
        let choose = |prefer_avoiding: bool| {
            remotes
                .paths
                .iter()
                .enumerate()
                .filter(|(_, path)| !prefer_avoiding || !avoid_keys.contains(&path.key()))
                .filter(|(_, path)| ordinary_path_allowed(path))
                .filter(|(_, path)| can_enqueue(path))
                .filter_map(|(position, path)| {
                    let key = path.key();
                    let snapshot = context.reliable_path_snapshot(key)?;
                    let score = scheduler::score_path(
                        snapshot,
                        lane,
                        payload_bytes,
                        SchedulerPolicy::default(),
                    )?;
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
        let selected = if requires_distinct_output {
            choose(true)
        } else {
            choose(true).or_else(|| choose(false))
        };
        if let Some(position) = selected {
            return Ok(position);
        }
        let distinct_capacity_fallback = remotes
            .paths
            .iter()
            .enumerate()
            .filter(|(_, path)| ordinary_path_allowed(path))
            .filter(|(_, path)| can_enqueue(path))
            .map(|(position, _)| position)
            .find(|position| !avoid_keys.contains(&remotes.paths[*position].key()));
        let capacity_fallback = if requires_distinct_output {
            distinct_capacity_fallback
        } else {
            distinct_capacity_fallback.or_else(|| {
                remotes
                    .paths
                    .iter()
                    .enumerate()
                    .filter(|(_, path)| ordinary_path_allowed(path))
                    .filter(|(_, path)| can_enqueue(path))
                    .map(|(position, _)| position)
                    .next()
            })
        };
        if let Some(position) = capacity_fallback {
            return Ok(position);
        }
        let has_eligible_path = remotes.paths.iter().any(ordinary_path_allowed);
        let has_distinct_eligible_path = remotes
            .paths
            .iter()
            .any(|path| ordinary_path_allowed(path) && !avoid_keys.contains(&path.key()));
        if (requires_distinct_output && has_distinct_eligible_path)
            || (!requires_distinct_output && has_eligible_path)
        {
            Err(RuntimeError::SenderServiceBlocked)
        } else {
            Err(RuntimeError::ReliablePathSessionClosed)
        }
    }
}

#[cfg(test)]
#[path = "multipath_test.rs"]
mod tests;
