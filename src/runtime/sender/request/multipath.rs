//! Serialized request multipath lifecycle.
//!
//! This owner combines carrier-neutral product offsets and evidence with TCP's
//! fallback capacity controller. QUIC uses validated paths under native
//! congestion control and writer backpressure.

use super::super::queue::ReliableRelaySenderQueue;
use super::super::work::{ClientReinjectionOutputIdentity, RelaySendCause};
use super::RequestDataAckGapObservation;
use super::scheduling::{
    BulkRelayFrameRequest, BulkRelayPathChoice, ObservedBulkPathCandidate,
    ObservedOrdinaryPathChoice, RequestRelayPathObservation, RequestRelaySchedulingObservation,
    RequestSchedulingState, choose_bulk_relay_path_avoiding, choose_observed_ordinary_data_path,
};
use super::tcp_capacity::{
    RequestTcpCapacityController, RequestTcpCapacityEvent, RequestTcpCapacityRetirement,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, PathRateSample, product_delivery_samples_override_startup_prior,
};
use crate::model::path::{RelayPathInstance, RelayPathKey, RelayPathProofEpoch};
use crate::model::request_evidence::{
    RequestOwnerAckProgress, RequestPathRateEvidenceUpdate, RequestPerFlowRateModel,
    request_path_rate_coverage_floor_bytes,
};
use crate::model::timing::reliable_relay_tail_reinjection_delay;
use crate::protocol::frame::{reliable_stream_frame_accounted_bytes, reliable_stream_frame_extent};
use crate::protocol::{Frame, OffsetRange, StreamId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ClientPathContext;
use crate::runtime::stream::request::{RequestAckClockOperation, RequestStreamState};
use crate::runtime::stream::{ReliableRelayRemotePath, ReliableRelayRemoteSet};
use crate::scheduler::{self, PathSnapshot, TrafficClass, cyclic_cursor_distance};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

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
    stale_paths: &HashSet<RelayPathInstance>,
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
                can_enqueue_frame: !stale_paths.contains(&instance)
                    && frame
                        .map(|frame| path.stream.can_enqueue_frame_now(frame, lane))
                        .unwrap_or(true),
                can_enqueue_stream_lane: !stale_paths.contains(&instance)
                    && frame
                        .map(|frame| path.stream.can_enqueue_frame_now(frame, path.stream.lane))
                        .unwrap_or(true),
                load_owned: path.has_load_reservation(),
                shared_snapshot: evidence.shared_snapshot,
                tcp: evidence.tcp,
                has_bulk_model_evidence: evidence.has_bulk_model_evidence,
                has_fresh_native_carrier_rate_evidence: evidence
                    .has_fresh_native_carrier_rate_evidence,
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
        latency_pressure: path_evidence.latency_pressure,
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
    Data,
    MeasurementFence {
        reference: RelayPathInstance,
        candidate: RelayPathInstance,
        entry_offset: u64,
        foreign_optional_ranges: usize,
        foreign_optional_bytes: u64,
    },
    OriginalData {
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

/// Product state and TCP's portable fallback authority for one request.
#[derive(Debug)]
pub(super) struct RequestMultipathController {
    stream_id: StreamId,
    request: RequestStreamState,
    tcp_capacity: RequestTcpCapacityController,
    next_send_index: usize,
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
        // A replacement carrier with the same numeric path key must not lend
        // its RTT or congestion evidence to an older attachment's flight.
        let original_path_timing =
            original_path.and_then(|instance| context.reliable_path_snapshot(instance.key));
        let live_instances = remotes.path_instances();
        let avoid_instances = self
            .request
            .flights
            .original_transmission_instances_for_frame(preview, &live_instances);
        let reinjection_path = self
            .choose_lowest_eta_relay_path(
                context,
                remotes,
                preview,
                lane,
                RelaySendCause::PersistentAckGapReinjection,
                &avoid_instances,
            )
            .ok()
            .and_then(|position| {
                let key = remotes.paths[position].key();
                let instance = remotes.paths[position].instance();
                if !context.relay_path_has_bulk_model_evidence(key.underlay, key.index) {
                    return None;
                }
                let snapshot = context.reliable_path_snapshot(key)?;
                let score = scheduler::score_path(
                    snapshot,
                    lane,
                    reliable_stream_frame_accounted_bytes(preview),
                )?;
                score.eta_ms.is_finite().then(|| {
                    (
                        ClientReinjectionOutputIdentity { instance },
                        snapshot,
                        Duration::from_secs_f64(score.eta_ms.max(0.0) / 1000.0),
                    )
                })
            });
        RequestDataAckGapObservation {
            has_live_original_path: original_path.is_some(),
            original_underlay,
            original_path_timing,
            reinjection_target: reinjection_path
                .map(|(identity, snapshot, _)| (identity, snapshot)),
            reinjection_completion: reinjection_path.map(|(_, _, completion)| completion),
        }
    }

    pub(super) fn reinjection_avoid_instances(
        &self,
        frame: &Frame,
        cause: RelaySendCause,
        remotes: &ReliableRelayRemoteSet,
    ) -> Vec<RelayPathInstance> {
        match cause {
            RelaySendCause::TailReinjection => {
                let owner_keys = self.request.flights.tail_reinjection_owner_keys(
                    frame,
                    &remotes.path_instances(),
                    Duration::ZERO,
                    Duration::ZERO,
                );
                remotes
                    .path_instances()
                    .into_iter()
                    .filter(|instance| owner_keys.contains(&instance.key))
                    .collect()
            }
            cause if cause.is_ack_gap_reinjection() => self
                .request
                .flights
                .original_transmission_instances_for_frame(frame, &remotes.path_instances()),
            cause if cause.is_reinjection() => self.request.flights.sent_instances_for_frame(frame),
            _ => Vec::new(),
        }
    }

    pub(super) fn owner_capable_instances(
        &self,
        remotes: &ReliableRelayRemoteSet,
    ) -> Vec<RelayPathInstance> {
        self.request_owner_capable_instances(remotes)
    }

    pub(super) fn original_transmission_keys_for_frame(
        &self,
        frame: &Frame,
        live_instances: &[RelayPathInstance],
    ) -> Vec<RelayPathKey> {
        self.request
            .flights
            .original_transmission_keys_for_frame(frame, live_instances)
    }

    pub(super) fn tail_reinjection_owner_keys(
        &self,
        frame: &Frame,
        live_instances: &[RelayPathInstance],
        first_reinjection_after: Duration,
        repeat_reinjection_after: Duration,
    ) -> Vec<RelayPathKey> {
        self.request.flights.tail_reinjection_owner_keys(
            frame,
            live_instances,
            first_reinjection_after,
            repeat_reinjection_after,
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

    pub(super) fn earliest_unacked_original_path(
        &self,
        remotes: &ReliableRelayRemoteSet,
    ) -> Option<RelayPathInstance> {
        self.request
            .flights
            .earliest_unacked_original_path()
            .filter(|instance| remotes.contains_path_instance(*instance))
    }

    pub(super) fn uncovered_unacked_ranges_for_reinjection(
        &self,
        remotes: &ReliableRelayRemoteSet,
        original_path: RelayPathInstance,
    ) -> Vec<OffsetRange> {
        let usable_alternate_paths = remotes
            .paths
            .iter()
            .filter(|path| path.instance() != original_path)
            .filter(|path| !self.request.stale_paths.contains(&path.instance()))
            .filter(|path| !path.stream.output_is_terminally_closed())
            .map(|path| path.instance())
            .collect::<Vec<_>>();
        self.request
            .flights
            .uncovered_unacked_ranges_for_reinjection(original_path, &usable_alternate_paths)
    }

    pub(super) fn record_reinjection_attempts(
        &mut self,
        instances: &[RelayPathInstance],
        attempted_at: Instant,
    ) {
        for instance in instances {
            self.request
                .reinjection_attempts
                .insert(*instance, attempted_at);
        }
    }

    pub(super) fn mark_path_stale(&mut self, instance: RelayPathInstance) -> bool {
        self.request.stale_paths.insert(instance)
    }

    pub(super) fn path_is_stale(&self, instance: RelayPathInstance) -> bool {
        self.request.stale_paths.contains(&instance)
    }

    pub(super) fn has_reinjection_path(
        &self,
        remotes: &ReliableRelayRemoteSet,
        candidate: RelayPathInstance,
    ) -> bool {
        remotes.paths.iter().any(|path| {
            path.instance() != candidate
                && !self.request.stale_paths.contains(&path.instance())
                && !path.stream.output_is_terminally_closed()
        })
    }

    pub(super) fn reinjection_path_snapshot(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        excluded: &[RelayPathInstance],
    ) -> Option<PathSnapshot> {
        let choose = |allow_backup: bool| {
            remotes
                .paths
                .iter()
                .filter(|path| !excluded.contains(&path.instance()))
                .filter(|path| !self.request.stale_paths.contains(&path.instance()))
                .filter(|path| !path.stream.output_is_terminally_closed())
                .filter_map(|path| context.reliable_path_snapshot(path.key()))
                .filter(|snapshot| allow_backup || !scheduler::path_is_backup(*snapshot))
                .filter_map(|snapshot| {
                    scheduler::score_path(snapshot, TrafficClass::Latency, PATH_OPEN_SCORE_BYTES)
                        .map(|score| (score.eta_ms, snapshot))
                })
                .min_by(|left, right| left.0.total_cmp(&right.0))
                .map(|(_, snapshot)| snapshot)
        };
        choose(false).or_else(|| choose(true))
    }

    pub(super) fn stale_path_reinjection_due(
        &self,
        instance: RelayPathInstance,
        retry_after: Duration,
    ) -> bool {
        self.request.stale_paths.contains(&instance)
            && self
                .request
                .reinjection_attempts
                .get(&instance)
                .is_none_or(|attempted_at| attempted_at.elapsed() >= retry_after)
    }

    pub(super) fn stale_paths_requiring_reinjection(
        &self,
        remotes: &ReliableRelayRemoteSet,
        retry_after: Duration,
    ) -> Vec<RelayPathInstance> {
        self.request
            .stale_paths
            .iter()
            .copied()
            .filter(|instance| remotes.contains_path_instance(*instance))
            .filter(|instance| {
                self.request
                    .flights
                    .has_original_transmission_flights_for_instance(*instance)
            })
            .filter(|instance| self.stale_path_reinjection_due(*instance, retry_after))
            .filter(|instance| {
                !self
                    .uncovered_unacked_ranges_for_reinjection(remotes, *instance)
                    .is_empty()
            })
            .collect()
    }

    pub(super) fn record_emitted_frame(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
        cause: RelaySendCause,
    ) -> usize {
        if cause.is_reinjection() {
            self.request
                .flights
                .record_reinjection_frame_instance(instance, frame)
        } else {
            self.request
                .flights
                .record_original_frame_instance(instance, frame)
        }
    }

    pub(super) fn normalize_cursor(&mut self, path_count: usize) {
        if path_count == 0 {
            self.next_send_index = 0;
        } else {
            self.next_send_index %= path_count;
        }
    }

    fn try_start_request_tcp_capacity_measurement(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        lane: TrafficClass,
    ) {
        let reference = self.request_capacity_reference(context, remotes);
        self.tcp_capacity.try_start(
            self.stream_id,
            &self.request,
            context,
            remotes,
            lane,
            reference,
        );
    }

    fn request_capacity_reference(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
    ) -> Option<(RelayPathInstance, RequestPerFlowRateModel)> {
        remotes
            .paths
            .iter()
            .filter_map(|path| {
                let instance = path.instance();
                let snapshot = context.reliable_path_snapshot(instance.key)?;
                if snapshot.state != scheduler::PathState::Active {
                    return None;
                }
                let model = self
                    .request
                    .path_states
                    .get(instance)?
                    .per_flow_rate()
                    .filter(|model| {
                        product_delivery_samples_override_startup_prior(model.delivery_samples)
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
    ) {
        let sample_bps = sample.rate_bps();
        let previous = self
            .request
            .path_states
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
            .path_states
            .get_mut(instance)
            .set_per_flow_rate(model);
    }

    fn prepare_relay_path_decision(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: &Frame,
        lane: TrafficClass,
        cause: RelaySendCause,
    ) -> Result<PreparedRequestMultipathDecision, RuntimeError> {
        if remotes.paths.is_empty() {
            return Err(RuntimeError::ReliablePathSessionClosed);
        }
        let membership_generation = remotes.membership_generation();
        let unique_data_payload_bytes = (matches!(frame, Frame::StreamData { .. })
            && !cause.is_reinjection())
        .then(|| reliable_stream_frame_accounted_bytes(frame));
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
            return Err(RuntimeError::SenderServiceBlocked);
        }
        self.next_send_index %= remotes.paths.len();
        self.reconcile_request_path_state(context, remotes);
        if unique_data_payload_bytes.is_some() {
            self.try_start_request_tcp_capacity_measurement(context, remotes, lane);
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
        lane: TrafficClass,
        cause: RelaySendCause,
        avoid_instances: &[RelayPathInstance],
    ) -> Result<RequestMultipathPlan, RuntimeError> {
        let prepared = self.prepare_relay_path_decision(context, remotes, frame, lane, cause)?;
        if let Some(payload_bytes) = prepared.unique_data_payload_bytes {
            let observe_bulk_admission =
                lane.is_bulk() && remotes.paths.len() > 1 && avoid_instances.is_empty();
            let relay_observation = observe_request_relay_scheduling(
                context,
                remotes.stream_id(),
                prepared.membership_generation,
                &remotes.paths,
                Some(frame),
                lane,
                payload_bytes,
                observe_bulk_admission,
                &self.request.stale_paths,
            );
            self.reconcile_request_path_state(context, remotes);
            match choose_bulk_relay_path_avoiding(BulkRelayFrameRequest {
                observation: &relay_observation,
                lane,
                frame,
                cursor: self.next_send_index,
                avoid_instances,
                path_flights: Some(&self.request.flights),
                request_state: Some(RequestSchedulingState {
                    operation: self.request.ack_clock_operation,
                    path_states: &self.request.path_states,
                }),
            }) {
                BulkRelayPathChoice::Selected(instance) => {
                    let mut selection = RequestMultipathPlan::new(
                        RequestMultipathTarget {
                            membership_generation: relay_observation.membership_generation,
                            instance,
                        },
                        RequestProductSendMutation::Data,
                    );
                    selection.request_load_expectation =
                        observed_request_load_expectation(&relay_observation, instance)?;
                    return Ok(selection);
                }
                BulkRelayPathChoice::SelectedAckClockMeasurement {
                    candidate,
                    target_bytes,
                    proof,
                } => {
                    let payload_bytes = reliable_stream_frame_accounted_bytes(frame) as u64;
                    let entry_offset = reliable_stream_frame_extent(frame)
                        .map(|(offset, _, _)| offset)
                        .unwrap_or(0);
                    let reference_key = self
                        .request
                        .flights
                        .oldest_lower_flight_owner_before_offset(entry_offset)
                        .unwrap_or(candidate.key);
                    let (foreign_optional_ranges, foreign_optional_bytes) = if !matches!(
                        self.request.ack_clock_operation,
                        Some(RequestAckClockOperation::Owner { .. })
                    ) {
                        self.request
                            .flights
                            .foreign_original_transmission_debt_before_offset(
                                entry_offset,
                                &[reference_key],
                            )
                    } else {
                        (0, 0)
                    };
                    return Ok(RequestMultipathPlan {
                        target: RequestMultipathTarget {
                            membership_generation: relay_observation.membership_generation,
                            instance: candidate,
                        },
                        product_mutation: RequestProductSendMutation::OriginalData {
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
                BulkRelayPathChoice::SelectedAckClockMeasurementFence {
                    reference,
                    candidate,
                } => {
                    let entry_offset = reliable_stream_frame_extent(frame)
                        .map(|(offset, _, _)| offset)
                        .unwrap_or(0);
                    let (foreign_optional_ranges, foreign_optional_bytes) =
                        if self.request.ack_clock_operation.is_none() {
                            self.request
                                .flights
                                .foreign_original_transmission_debt_before_offset(
                                    entry_offset,
                                    &[reference.key],
                                )
                        } else {
                            (0, 0)
                        };
                    return Ok(RequestMultipathPlan {
                        target: RequestMultipathTarget {
                            membership_generation: relay_observation.membership_generation,
                            instance: reference,
                        },
                        product_mutation: RequestProductSendMutation::MeasurementFence {
                            reference,
                            candidate,
                            entry_offset,
                            foreign_optional_ranges,
                            foreign_optional_bytes,
                        },
                        request_load_expectation: observed_request_load_expectation(
                            &relay_observation,
                            reference,
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
                        avoid_instances,
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
                        RequestProductSendMutation::Data,
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
            avoid_instances,
        )?;
        let product_mutation = if prepared.unique_data_payload_bytes.is_some() {
            RequestProductSendMutation::Data
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

    pub(super) fn commit_enqueued_request_product_send(
        &mut self,
        context: &ClientPathContext,
        frame: &Frame,
        plan: RequestMultipathPlan,
        position: usize,
        path_count: usize,
    ) {
        let instance = plan.target.instance;
        let mutation = plan.product_mutation;
        self.commit_request_ack_clock_measurement(&mutation);
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
            RequestProductSendMutation::Data
            | RequestProductSendMutation::MeasurementFence { .. }
            | RequestProductSendMutation::OriginalData { .. } => {
                context.record_relay_path_send(instance, sent_bytes);
            }
            // Reinjection data repeats an existing product offset, so enqueue must
            // not install or advance unique-data ownership.
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
            RequestProductSendMutation::MeasurementFence {
                reference,
                candidate,
                entry_offset,
                foreign_optional_ranges,
                foreign_optional_bytes,
            } => {
                let (
                    reference,
                    candidate,
                    entry_offset,
                    foreign_optional_ranges,
                    foreign_optional_bytes,
                ) = (
                    *reference,
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
                let pending = RequestAckClockOperation::Pending {
                    reference,
                    candidate,
                };
                if self.request.ack_clock_operation == Some(pending) {
                    return;
                }
                self.request.ack_clock_operation = Some(pending);
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "ack_clock_measurement",
                    format_args!(
                        "phase=pending_started stream_id={} service_underlay={:?} service_index={} service_instance={} candidate_index={} candidate_instance={} entry_offset={} foreign_optional_ranges={} foreign_optional_bytes={}",
                        self.stream_id.0,
                        reference.key.underlay,
                        reference.key.index,
                        reference.attachment_id,
                        candidate.key.index,
                        candidate.attachment_id,
                        entry_offset,
                        foreign_optional_ranges,
                        foreign_optional_bytes,
                    ),
                );
            }
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

    fn reconcile_request_path_state(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
    ) {
        let membership_generation = remotes.membership_generation();
        if self.request.membership_generation != Some(membership_generation) {
            let live_instances = remotes.path_instances().into_iter().collect::<HashSet<_>>();
            self.request.path_states.retain_live(&live_instances);
            self.request
                .stale_paths
                .retain(|instance| live_instances.contains(instance));
            self.request.membership_generation = Some(membership_generation);
        }
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
                    state.ack_clock_proven() && state.per_flow_rate().is_some()
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

    /// Releases every exact flight copy and returns paths with exact progress.
    pub(super) fn apply_product_ack(
        &mut self,
        context: &ClientPathContext,
        _remotes: &ReliableRelayRemoteSet,
        ranges: &[OffsetRange],
        acked_at: Instant,
    ) -> smallvec::SmallVec<[RelayPathInstance; 4]> {
        let delivered_data =
            self.release_normalized_acked_ranges_with_delivered_data_at(context, ranges, acked_at);
        let data_ack_progress_paths = delivered_data
            .iter()
            .map(|progress| progress.instance)
            .collect::<smallvec::SmallVec<[_; 4]>>();
        for instance in &data_ack_progress_paths {
            self.request.stale_paths.remove(instance);
        }
        data_ack_progress_paths
    }

    fn release_normalized_acked_ranges_with_delivered_data_at(
        &mut self,
        context: &ClientPathContext,
        ranges: &[OffsetRange],
        acked_at: Instant,
    ) -> smallvec::SmallVec<[RequestOwnerAckProgress<RelayPathInstance>; 4]> {
        let mut ordinary_owner_samples =
            HashMap::<RelayPathInstance, (u64, Instant, Instant)>::new();
        let mut delivered_data =
            smallvec::SmallVec::<[RequestOwnerAckProgress<RelayPathInstance>; 4]>::new();
        for release in self.request.flights.release_normalized_acked_ranges(ranges) {
            self.request.reinjection_attempts.remove(&release.instance);
            context.release_relay_path_inflight(release.instance, release.bytes);
            if release.path_proving {
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
            if release.path_proving {
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
            let coverage_floor_bytes = request_path_rate_coverage_floor_bytes(
                instance.key.underlay,
                self.request
                    .path_states
                    .get(instance)
                    .and_then(|state| state.ack_clock_measurement_target()),
                context.mux_limits,
            );
            let (update, has_exact_path_provenance) = {
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
                (update, evidence.has_exact_path_provenance())
            };
            if has_exact_path_provenance {
                // Exact ownership is enough to establish that this flow used
                // the path. It is not enough to publish a rate sample.
                self.request
                    .path_states
                    .get_mut(instance)
                    .mark_product_delivery_proven();
            }
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
                        self.record_request_per_flow_rate_sample(
                            instance,
                            sample,
                            replace_startup_rate,
                        );
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
                    self.record_request_per_flow_rate_sample(instance, sample, false);
                    context.mark_relay_path_rate_sample(instance, sample);
                }
            }
        }
        delivered_data
    }

    pub(super) fn discard_unusable_tail_reinjections(
        &self,
        sender_queue: &mut ReliableRelaySenderQueue,
        remotes: &ReliableRelayRemoteSet,
    ) -> usize {
        let live_instances = self.request_owner_capable_instances(remotes);
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

    pub(super) fn discard_stale_persistent_ack_gap_reinjections(
        &self,
        sender_queue: &mut ReliableRelaySenderQueue,
        remotes: &ReliableRelayRemoteSet,
    ) -> usize {
        let live_instances = remotes.path_instances();
        sender_queue.discard_stale_persistent_ack_gap_reinjections(|cause| {
            cause
                .persistent_client_target()
                .is_none_or(|target| live_instances.contains(&target))
                && cause.persistent_server_target().is_none()
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
        remotes: &ReliableRelayRemoteSet,
    ) -> Vec<RelayPathInstance> {
        remotes
            .paths
            .iter()
            .map(ReliableRelayRemotePath::instance)
            .filter(|instance| !self.request.stale_paths.contains(instance))
            .collect()
    }

    pub(super) fn unreported_missing_owner_instances(
        &mut self,
        remotes: &ReliableRelayRemoteSet,
        retry_after: Duration,
    ) -> Vec<RelayPathInstance> {
        let owner_instances = self.request.flights.original_transmission_instances();
        self.request.reinjection_attempts.retain(|instance, _| {
            owner_instances.contains(instance)
                && (!remotes.contains_path_instance(*instance)
                    || self.request.stale_paths.contains(instance))
        });
        let now = Instant::now();
        owner_instances
            .into_iter()
            .filter(|instance| {
                !remotes.contains_path_instance(*instance)
                    && self
                        .request
                        .reinjection_attempts
                        .get(instance)
                        .is_none_or(|attempt| {
                            now.saturating_duration_since(*attempt) >= retry_after
                        })
            })
            .collect()
    }

    pub(super) fn release_all(&mut self, context: &ClientPathContext) {
        for release in self.request.flights.drain_all() {
            context.release_relay_path_inflight(release.instance, release.bytes);
        }
    }

    #[cfg(test)]
    pub(super) fn record_original_frame_for_test(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
    ) {
        self.request
            .flights
            .record_original_frame_instance(instance, frame);
    }

    /// Periodic Data-ACK repair is useful only when the chosen ordered carrier
    /// can get ahead of the blocked copy. TCP uses native socket state when it
    /// exists; QUIC and portable TCP use exact same-stream product ownership.
    pub(super) fn ordered_reinjection_carrier_ready(
        &self,
        context: &ClientPathContext,
        path: &ReliableRelayRemotePath,
        frame: &Frame,
        cause: RelaySendCause,
    ) -> bool {
        if !cause.is_reinjection() {
            return true;
        }
        if path.stream.ordered_writer_pending_bytes() != Some(0) {
            return false;
        }
        if (cause.is_ack_gap_reinjection() || cause == RelaySendCause::TailReinjection)
            && self.request.flights.has_recent_reinjection_on_instance(
                frame,
                path.instance(),
                reliable_relay_tail_reinjection_delay(context.reliable_path_snapshot(path.key())),
            )
        {
            return false;
        }

        let snapshot = context.reliable_path_snapshot(path.key());
        if path.key().underlay == UnderlayProtocol::Tcp
            && context.tcp_native_drain_observed(path.key().index)
            && let Some(snapshot) = snapshot
        {
            return snapshot.bytes_in_flight == 0 && snapshot.queue_bytes == 0;
        }
        self.request
            .flights
            .original_data_in_flight_bytes(path.instance())
            == 0
    }

    fn choose_lowest_eta_relay_path(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        frame: &Frame,
        lane: TrafficClass,
        cause: RelaySendCause,
        avoid_instances: &[RelayPathInstance],
    ) -> Result<usize, RuntimeError> {
        let ack_gap_reinjection = cause.is_ack_gap_reinjection();
        let requires_measured_reinjection_target = cause.is_persistent_ack_gap_reinjection();
        let required_persistent_target = cause.persistent_client_target();
        let invalid_persistent_target =
            matches!(cause, RelaySendCause::PersistentServerAckGapReinjection(_));
        let failure_recovery = matches!(
            cause,
            RelaySendCause::PathFailureReinjection | RelaySendCause::StalePathReinjection(_)
        );
        let requires_distinct_output =
            cause == RelaySendCause::TailReinjection || ack_gap_reinjection;
        let payload_bytes = reliable_stream_frame_accounted_bytes(frame);
        let ordinary_path_allowed = |path: &ReliableRelayRemotePath| {
            !self.request.stale_paths.contains(&path.instance())
                && !invalid_persistent_target
                && required_persistent_target.is_none_or(|required| path.instance() == required)
                && (!requires_measured_reinjection_target
                    || context
                        .relay_path_has_bulk_model_evidence(path.key().underlay, path.key().index))
        };
        let busy_reinjection_allowed = failure_recovery || requires_measured_reinjection_target;
        let carrier_reinjection_ready = |path: &ReliableRelayRemotePath, require_idle: bool| {
            (!require_idle && busy_reinjection_allowed)
                || self.ordered_reinjection_carrier_ready(context, path, frame, cause)
        };
        let can_enqueue = |path: &ReliableRelayRemotePath| {
            relay_path_can_enqueue_frame_for_cause_now(path, frame, cause)
        };
        let choose = |allow_backup: bool, prefer_avoiding: bool, require_idle: bool| {
            remotes
                .paths
                .iter()
                .enumerate()
                .filter(|(_, path)| !prefer_avoiding || !avoid_instances.contains(&path.instance()))
                .filter(|(_, path)| ordinary_path_allowed(path))
                .filter(|(_, path)| carrier_reinjection_ready(path, require_idle))
                .filter(|(_, path)| can_enqueue(path))
                .filter_map(|(position, path)| {
                    let key = path.key();
                    let snapshot = context.reliable_path_snapshot(key)?;
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
        let choose_ranked = |require_idle: bool| {
            if requires_distinct_output {
                choose(false, true, require_idle).or_else(|| choose(true, true, require_idle))
            } else {
                choose(false, true, require_idle)
                    .or_else(|| choose(false, false, require_idle))
                    .or_else(|| choose(true, true, require_idle))
                    .or_else(|| choose(true, false, require_idle))
            }
        };
        let selected = choose_ranked(!busy_reinjection_allowed);
        if let Some(position) = selected {
            return Ok(position);
        }
        let choose_capacity = |allow_backup: bool, prefer_avoiding: bool, require_idle: bool| {
            remotes
                .paths
                .iter()
                .enumerate()
                .filter(|(_, path)| ordinary_path_allowed(path))
                .filter(|(_, path)| carrier_reinjection_ready(path, require_idle))
                .filter(|(_, path)| can_enqueue(path))
                .filter(|(_, path)| !prefer_avoiding || !avoid_instances.contains(&path.instance()))
                .filter(|(_, path)| {
                    allow_backup
                        || context
                            .reliable_path_snapshot(path.key())
                            .is_none_or(|snapshot| !scheduler::path_is_backup(snapshot))
                })
                .map(|(position, _)| position)
                .next()
        };
        let choose_capacity_fallback = |require_idle: bool| {
            if requires_distinct_output {
                choose_capacity(false, true, require_idle)
                    .or_else(|| choose_capacity(true, true, require_idle))
            } else {
                choose_capacity(false, true, require_idle)
                    .or_else(|| choose_capacity(false, false, require_idle))
                    .or_else(|| choose_capacity(true, true, require_idle))
                    .or_else(|| choose_capacity(true, false, require_idle))
            }
        };
        let capacity_fallback = choose_capacity_fallback(!busy_reinjection_allowed);
        if let Some(position) = capacity_fallback {
            return Ok(position);
        }
        let has_eligible_path = remotes.paths.iter().any(ordinary_path_allowed);
        let has_distinct_eligible_path = remotes
            .paths
            .iter()
            .any(|path| ordinary_path_allowed(path) && !avoid_instances.contains(&path.instance()));
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
