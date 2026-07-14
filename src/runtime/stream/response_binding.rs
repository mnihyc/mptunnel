#[path = "response_ack_clock.rs"]
mod response_ack_clock;
#[path = "response_admission.rs"]
mod response_admission;
#[path = "response_delivery.rs"]
mod response_delivery;
#[path = "response_diagnostics.rs"]
mod response_diagnostics;
#[path = "response_evidence.rs"]
mod response_evidence;
#[path = "response_handoff.rs"]
mod response_handoff;
#[path = "response_quic_capacity.rs"]
mod response_quic_capacity;
#[path = "response_session.rs"]
pub(super) mod response_session;
#[path = "response_snapshot.rs"]
mod response_snapshot;
#[path = "response_topology.rs"]
mod response_topology;

pub(in crate::runtime) use crate::runtime::path::quic::metrics::QuicCapacityProofCandidate;
#[cfg(test)]
pub(in crate::runtime) use response_ack_clock::{
    ResponseAckClockCalibrationState, ResponseAckClockRateEvidence,
    ResponseAckClockRateEvidenceUpdate,
};
use response_admission::ResponseSubflowSetState;
pub(in crate::runtime) use response_admission::{
    ResponseSubflowAdmissionRequest, server_output_accepts_service_capacity_prior,
    server_output_has_bulk_rate_evidence_with_limits, server_output_has_sender_evidence,
    server_output_has_service_feed_evidence_with_limits,
};
#[cfg(test)]
pub(in crate::runtime) use response_admission::{
    ResponseSubflowAdmissionReservation, server_output_has_bulk_rate_evidence,
    server_output_has_durable_product_progress,
};
#[cfg(test)]
pub(in crate::runtime) use response_delivery::{
    CarrierPathAckedHole, CarrierPathReleasedFlight, ResponseAckOrderingUpdate,
    response_latest_ordering_hole,
};
pub(in crate::runtime) use response_delivery::{
    CarrierPathFlight, CarrierPathFlightDebt, ResponseAckOrderingState,
};
pub(super) use response_delivery::{
    product_flights_have_recent_repair_overlap, release_carrier_path_flight_ranges,
};
pub(in crate::runtime) use response_diagnostics::record_server_sender_decision;
pub(in crate::runtime) use response_evidence::{ServerPathMetricsEntry, ServerPathMetricsSource};
pub(in crate::runtime) use response_handoff::{
    ResponseServiceHandoffDrainRequest, ResponseServiceHandoffRequest,
};
pub(in crate::runtime) use response_quic_capacity::ResponseQuicCapacityCalibrationRequest;
#[cfg(any(test, feature = "lab-diagnostics"))]
pub(in crate::runtime) use response_session::well_formed_quic_capacity_proof_candidate;
pub(in crate::runtime) use response_session::{
    ResponseServiceFamilyLoads, ResponseServiceHandoffDrainReservation,
    ResponseSessionSchedulingSnapshot, ServerPathLaneTracker, ServerRealtimeFlowRegistration,
    TcpCapacityProbeSessionLease, quic_capacity_proof_pin_matches_marker,
    quic_capacity_receipt_rate_bps, valid_quic_capacity_proof_candidate_at,
};
pub(in crate::runtime) use response_snapshot::server_bulk_output_eta_ms;
#[cfg(test)]
pub(in crate::runtime) use response_topology::TcpResponseCapacityPrior;
pub(in crate::runtime) use response_topology::{
    ResponseDispatchTarget, ResponseSenderPathTarget, ResponseStreamAttachOutcome,
    ResponseStreamOutputEntry, ResponseStreamOutputs, ServerCarrierPathInstanceId,
    next_server_carrier_path_instance_id,
};
use response_topology::{
    response_live_ordered_data_owner, response_owner_underlay_seen_bit,
    response_stream_role_reserves_flow_load,
};

use self::response_evidence::server_output_quic_capacity_proof_marker;
#[cfg(test)]
use self::response_session::ServerQuicCapacityCalibrationPhase;
use self::response_session::ServerResponseFlowRegistration;
#[cfg(test)]
use self::response_snapshot::server_bulk_output_snapshot;
use self::response_snapshot::server_bulk_output_snapshot_with_scheduling;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{lab_diagnostic, lab_diagnostic_event_enabled};
use crate::model::ack_clock::reliable_ack_clock_calibration_ceiling_bytes;
#[cfg(test)]
use crate::model::ack_clock::reliable_ack_clock_calibration_limit_bytes;
#[cfg(feature = "lab-diagnostics")]
use crate::model::admission::{
    bulk_active_service_product_envelope_bytes, bulk_latency_pressure_service_feed_window_bytes,
    bulk_service_feed_reservoir_payload_bytes, bulk_service_horizon_payload_bytes,
};
#[cfg(test)]
use crate::model::capacity::PathRateSample;
#[cfg(any(test, feature = "lab-diagnostics"))]
use crate::model::capacity::reliable_subflow_startup_sample_limit_bytes;
use crate::model::multipath::PathAdmissionDecision;
#[cfg(test)]
use crate::model::multipath::{FlowSubflowSet, SubflowAdmissionInput};
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
#[cfg(test)]
use crate::protocol::OffsetRange;
use crate::protocol::{
    Frame, PathId, PathMetrics, SessionId, StreamId, StreamOpenRole, UnderlayProtocol,
};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::{ReliablePathCommand, ReliablePathCommandSender};
use crate::runtime::path::tcp::capacity::TcpCapacityProofCandidate;
#[cfg(feature = "lab-diagnostics")]
use crate::runtime::relay::io::reliable_bulk_carrier_feed_quantum_bytes;
use crate::runtime::relay_striping::reliable_stream_frame_extent;
use crate::scheduler::{FlowLane, PathSnapshot};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
#[cfg(feature = "lab-diagnostics")]
use std::time::Duration;
use std::time::Instant;
use tokio::sync::{Notify, watch};

// Reliable-path bindings own attachment instances, exact range flights,
// evidence, and atomic commit. Sender services rank immutable snapshots.

pub(in crate::runtime) const MAX_RESPONSE_QUIC_CAPACITY_CALIBRATION_ATTEMPTS_PER_PATH: u8 = 2;
// One sustained response stream is real demand. Same-family discovery stays
// single-owner and bounded; requiring a second stream prevents large one-flow
// transfers from ever measuring spare paths.
pub(in crate::runtime) const MIN_ACTIVE_RESPONSE_FLOWS_FOR_SAME_FAMILY_DISCOVERY: u32 = 1;
static NEXT_RESPONSE_STREAM_BINDING_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);
// Ownership boundary:
// This module owns carrier-neutral reliable stream bindings on the response
// side. It tracks which carrier path carried each product byte range, records
// ordering debt and stream-ACK release. It must not choose among joined carrier
// paths for response frames; dispatch belongs to the sender service. It must
// not implement TCP framing, QUIC packet recovery, or target socket I/O; those
// belong to carrier and outbound modules.

/// Server-side response owner for one product reliable stream.
///
/// This binding owns the stream's attached carrier outputs, product byte flight
/// ledger, stream-ACK ordering state, lane tracking, and path-metric hints used
/// for response scheduling. It does not own the target socket and does not own
/// TCP/QUIC packet recovery.

#[derive(Debug, Clone, Copy)]
/// Optimistic calibration reservation. Generations fence product/path model
/// changes; pending values fence the exact queue-pressure projection.
pub(in crate::runtime) struct ResponseAckClockCalibrationRequest {
    pub(in crate::runtime) expected_planner_generation: u64,
    pub(in crate::runtime) expected_lane_generation: u64,
    pub(in crate::runtime) expected_model_generation: u64,
    pub(in crate::runtime) service: CarrierPathKey,
    pub(in crate::runtime) service_incarnation: u64,
    pub(in crate::runtime) service_pending_bytes: u64,
    pub(in crate::runtime) target_pending_bytes: u64,
    pub(in crate::runtime) limit_bytes: u64,
    /// Fresh work requires active response demand; exact begun work may finish.
    pub(in crate::runtime) requires_active_response_start: bool,
}

#[derive(Debug, Clone, Copy)]
/// Zero-spend retirement uses the same coherent planner/model snapshot as Admit.
pub(in crate::runtime) struct ResponseAckClockCalibrationRetirementRequest {
    pub(in crate::runtime) expected_planner_generation: u64,
    pub(in crate::runtime) expected_lane_generation: u64,
    pub(in crate::runtime) expected_model_generation: u64,
    pub(in crate::runtime) service: CarrierPathKey,
    pub(in crate::runtime) service_incarnation: u64,
    pub(in crate::runtime) service_pending_bytes: u64,
    pub(in crate::runtime) target: CarrierPathKey,
    pub(in crate::runtime) target_incarnation: u64,
    pub(in crate::runtime) target_pending_bytes: u64,
    pub(in crate::runtime) limit_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ResponseSourceServiceSnapshot {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime) key: CarrierPathKey,
    pub(in crate::runtime) active_latency_sensitive_flows: u32,
    pub(in crate::runtime) has_service_feed_evidence: bool,
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime) has_bulk_rate_evidence: bool,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ResponseRelayReadSnapshot {
    pub(in crate::runtime) send_path: Option<PathSnapshot>,
    pub(in crate::runtime) source_service: Option<ResponseSourceServiceSnapshot>,
    pub(in crate::runtime) independent_source_staging: bool,
}

#[cfg(feature = "lab-diagnostics")]
#[derive(Clone, Copy)]
struct ResponseServiceHandoffDiagnosticState {
    model_generation: u64,
    evaluation_signature: u64,
    capacity_marker_signature: u64,
    emitted_at: Instant,
}

#[cfg(feature = "lab-diagnostics")]
#[derive(Clone, Copy, PartialEq, Eq)]
struct ResponseServiceFeedDiagnosticState {
    path_instance_id: ServerCarrierPathInstanceId,
    attachment_role: StreamOpenRole,
    is_active: bool,
    has_bulk_rate_evidence: bool,
    has_service_feed_evidence: bool,
    owner_progress_bucket: u8,
    product_rate_available: bool,
    carrier_sample_bucket: u8,
    carrier_sample_available: bool,
    carrier_app_limited: bool,
    latency_pressure: bool,
    source_limit_bytes: u64,
    emission_limit_bytes: u64,
}

pub(in crate::runtime) struct ResponseStreamBinding {
    session_id: SessionId,
    binding_instance_id: u64,
    lane: Mutex<FlowLane>,
    mux_limits: MuxLimits,
    lane_tracker: Arc<ServerPathLaneTracker>,
    response_flow_registration: ServerResponseFlowRegistration,
    next_output_incarnation: AtomicU64,
    // Publishes coherent path evidence, exact flights, ACK ordering, and queue
    // inputs so calibration cannot commit a mixture of old and new views.
    response_model_generation: AtomicU64,
    owner_underlay_history: AtomicU8,
    // Close publishes before carrier commands so no later scheduler commit can
    // resurrect response Service ownership after stream retirement begins.
    response_stream_open: AtomicBool,
    // A successful carrier-family Service handoff is sticky. Reopening this
    // decision would turn whole-flow placement into per-epoch path hopping.
    response_service_handoff_open: AtomicBool,
    // A failed bounded drain is not retried for this flow; repeated pauses
    // would convert an optional placement optimization into periodic stalls.
    response_service_handoff_drain_attempted: AtomicBool,
    // Lab evaluation is transition/interval scoped so a hot sender loop does
    // not turn one failed placement gate into one event per product frame.
    #[cfg(feature = "lab-diagnostics")]
    response_service_handoff_diagnostic: Mutex<Option<ResponseServiceHandoffDiagnosticState>>,
    #[cfg(feature = "lab-diagnostics")]
    response_service_feed_diagnostic:
        Mutex<HashMap<(CarrierPathKey, u64), ResponseServiceFeedDiagnosticState>>,
    outputs: Mutex<ResponseStreamOutputs>,
    request_active_owner: Mutex<Option<CarrierPathKey>>,
    // Historical name: this is the persistent response Service anchor, not
    // exclusive ownership of every range. `flights` owns exact byte identity.
    ordered_data_owner: Mutex<Option<CarrierPathKey>>,
    flights: Mutex<BTreeMap<u64, Vec<CarrierPathFlight>>>,
    ack_ordering: Mutex<ResponseAckOrderingState>,
    subflow_set: Mutex<ResponseSubflowSetState>,
    version: watch::Sender<u64>,
}

impl Drop for ResponseStreamBinding {
    fn drop(&mut self) {
        self.response_flow_registration.set_active(false);
        self.lane_tracker
            .clear_response_service_handoff_drain_for_binding(
                self.session_id,
                self.binding_instance_id,
            );
        let lane = *self
            .lane
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let outputs = self
            .outputs
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for entry in outputs.entries.drain(..) {
            if response_stream_role_reserves_flow_load(entry.role) {
                self.lane_tracker.detach(self.session_id, entry.key, lane);
            }
            self.lane_tracker.clear_quic_capacity_calibration(
                self.session_id,
                self.binding_instance_id,
                entry.key,
                entry.path_instance_id,
            );
        }
    }
}

impl ResponseStreamBinding {
    #[cfg(test)]
    pub(in crate::runtime) fn new(
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: FlowLane,
    ) -> Arc<Self> {
        Self::new_with_limits(
            session_id,
            underlay,
            path_id,
            commands,
            lane,
            MuxLimits::default(),
        )
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime) fn new_with_limits(
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: FlowLane,
        mux_limits: MuxLimits,
    ) -> Arc<Self> {
        Self::new_with_limits_and_tracker(
            session_id,
            underlay,
            path_id,
            commands,
            lane,
            mux_limits,
            Arc::new(ServerPathLaneTracker::default()),
        )
    }

    pub(in crate::runtime) fn new_with_limits_and_tracker(
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: FlowLane,
        mux_limits: MuxLimits,
        lane_tracker: Arc<ServerPathLaneTracker>,
    ) -> Arc<Self> {
        Self::new_with_limits_tracker_and_path_instance(
            session_id,
            underlay,
            path_id,
            commands,
            lane,
            mux_limits,
            lane_tracker,
            next_server_carrier_path_instance_id(),
        )
    }

    pub(super) fn new_with_limits_tracker_and_path_instance(
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: FlowLane,
        mux_limits: MuxLimits,
        lane_tracker: Arc<ServerPathLaneTracker>,
        path_instance_id: ServerCarrierPathInstanceId,
    ) -> Arc<Self> {
        let (version, _) = watch::channel(0);
        let key = CarrierPathKey { underlay, path_id };
        let response_flow_registration =
            ServerResponseFlowRegistration::new(lane_tracker.clone(), session_id, key, lane);
        lane_tracker.attach(session_id, key, lane);
        response_flow_registration.set_active(true);
        Arc::new(Self {
            session_id,
            binding_instance_id: NEXT_RESPONSE_STREAM_BINDING_INSTANCE_ID
                .fetch_add(1, Ordering::AcqRel),
            lane: Mutex::new(lane),
            mux_limits,
            lane_tracker,
            response_flow_registration,
            next_output_incarnation: AtomicU64::new(2),
            response_model_generation: AtomicU64::new(0),
            owner_underlay_history: AtomicU8::new(response_owner_underlay_seen_bit(underlay)),
            response_stream_open: AtomicBool::new(true),
            response_service_handoff_open: AtomicBool::new(true),
            response_service_handoff_drain_attempted: AtomicBool::new(false),
            #[cfg(feature = "lab-diagnostics")]
            response_service_handoff_diagnostic: Mutex::new(None),
            #[cfg(feature = "lab-diagnostics")]
            response_service_feed_diagnostic: Mutex::new(HashMap::new()),
            outputs: Mutex::new(ResponseStreamOutputs {
                entries: vec![ResponseStreamOutputEntry {
                    key,
                    path_instance_id,
                    incarnation: 1,
                    commands,
                    role: StreamOpenRole::Active,
                    owner_data_in_flight_bytes: 0,
                    bytes_in_flight: 0,
                    product_queue_bytes: 0,
                    product_progress_rate_bps: None,
                    delivery_rate_bps: None,
                    tcp_ack_clock_rate_bps: None,
                    tcp_product_rate_evidence: None,
                    tcp_capacity_prior: None,
                    srtt_ms: None,
                    delivery_samples: 0,
                    owner_data_acked_bytes: 0,
                    local_path_metrics: None,
                    peer_path_metrics: None,
                }],
                ack_clock_calibrations: HashMap::new(),
                active_ack_clock_calibration: None,
            }),
            request_active_owner: Mutex::new(Some(key)),
            ordered_data_owner: Mutex::new(Some(key)),
            flights: Mutex::new(BTreeMap::new()),
            ack_ordering: Mutex::new(ResponseAckOrderingState::default()),
            subflow_set: Mutex::new(ResponseSubflowSetState::default()),
            version,
        })
    }

    pub(in crate::runtime) fn subscribe_updates(&self) -> watch::Receiver<u64> {
        self.version.subscribe()
    }

    pub(in crate::runtime) fn send_path_snapshot(
        &self,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Option<PathSnapshot> {
        self.relay_read_snapshot(lane, payload_bytes).send_path
    }

    pub(in crate::runtime) fn relay_read_snapshot(
        &self,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> ResponseRelayReadSnapshot {
        let may_have_mixed_owner_underlays = self.may_have_mixed_owner_underlays();
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let stored_service_key = *self
            .ordered_data_owner
            .lock()
            .expect("server reliable stream ordered data owner lock");
        outputs.relay_read_snapshot(
            stored_service_key,
            may_have_mixed_owner_underlays,
            self.session_id,
            &self.lane_tracker,
            lane,
            payload_bytes,
            self.mux_limits,
        )
    }

    pub(super) fn set_sender_queue_bytes(&self, bytes: usize) {
        let bytes = bytes as u64;
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let mut changed = false;
        for entry in &mut outputs.entries {
            if entry.product_queue_bytes != bytes {
                entry.product_queue_bytes = bytes;
                changed = true;
            }
        }
        if changed {
            self.response_model_generation
                .fetch_add(1, Ordering::AcqRel);
        }
        drop(outputs);
        if changed {
            self.notify_update();
        }
    }

    pub(super) fn capacity_notifies(&self) -> Vec<Arc<Notify>> {
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .map(|entry| entry.commands.capacity_notify())
            .collect()
    }

    pub(in crate::runtime) fn response_model_generation(&self) -> u64 {
        self.response_model_generation.load(Ordering::Acquire)
    }

    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) fn should_emit_response_service_handoff_diagnostic(
        &self,
        model_generation: u64,
        evaluation_signature: u64,
        capacity_marker_signature: u64,
        now: Instant,
    ) -> bool {
        const REFRESH_INTERVAL: Duration = Duration::from_secs(1);

        let mut previous = self
            .response_service_handoff_diagnostic
            .lock()
            .expect("response Service handoff diagnostic lock");
        let should_emit = previous.is_none_or(|previous| {
            previous.evaluation_signature != evaluation_signature
                || previous.capacity_marker_signature != capacity_marker_signature
                || (previous.model_generation != model_generation
                    && now.saturating_duration_since(previous.emitted_at) >= REFRESH_INTERVAL)
        });
        if should_emit {
            *previous = Some(ResponseServiceHandoffDiagnosticState {
                model_generation,
                evaluation_signature,
                capacity_marker_signature,
                emitted_at: now,
            });
        }
        should_emit
    }

    #[cfg(test)]
    pub(in crate::runtime) fn lane_generation(&self) -> u64 {
        self.lane_tracker.generation(self.session_id)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn lane_generation_and_active_response_flows(&self) -> (u64, u32) {
        self.lane_tracker
            .generation_and_active_response_flows(self.session_id)
    }

    pub(in crate::runtime) fn response_scheduling_snapshot(
        &self,
    ) -> ResponseSessionSchedulingSnapshot {
        self.lane_tracker
            .response_scheduling_snapshot(self.session_id)
    }

    pub(in crate::runtime) fn try_reserve_tcp_capacity_probe(
        &self,
        expected_generation: u64,
    ) -> Option<TcpCapacityProbeSessionLease> {
        self.lane_tracker
            .try_reserve_tcp_capacity_probe(self.session_id, expected_generation)
    }

    pub(in crate::runtime) fn try_retire_tcp_ack_clock_calibration(
        &self,
        request: ResponseAckClockCalibrationRetirementRequest,
    ) -> bool {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let mut retired = None;
        let applied = self
            .lane_tracker
            .with_matching_generation_and_min_active_response_flows(
                self.session_id,
                request.expected_lane_generation,
                MIN_ACTIVE_RESPONSE_FLOWS_FOR_SAME_FAMILY_DISCOVERY,
                || {
                    if self.response_model_generation.load(Ordering::Acquire)
                        != request.expected_model_generation
                    {
                        return false;
                    }
                    let mut subflow_state = self
                        .subflow_set
                        .lock()
                        .expect("server reliable stream subflow set lock");
                    if subflow_state.planner_generation != request.expected_planner_generation
                        || subflow_state.set.as_ref().is_none_or(|epoch| {
                            epoch.service_key() != request.service
                                || epoch.startup_owner_key().is_some()
                        })
                    {
                        return false;
                    }
                    let service_is_exact_and_proven = outputs.entries.iter().any(|entry| {
                        entry.key == request.service
                            && entry.incarnation == request.service_incarnation
                            && entry.role != StreamOpenRole::Repair
                            && !entry.commands.is_closed()
                            && entry.commands.pending_bytes() == request.service_pending_bytes
                            && entry.key.underlay == UnderlayProtocol::Tcp
                            && server_output_has_bulk_rate_evidence_with_limits(
                                entry,
                                self.mux_limits,
                            )
                    });
                    let target_is_exact_and_drained = outputs.entries.iter().any(|entry| {
                        entry.key == request.target
                            && entry.incarnation == request.target_incarnation
                            && entry.role == StreamOpenRole::Validation
                            && !entry.commands.is_closed()
                            && entry.commands.pending_bytes() == request.target_pending_bytes
                            && entry.key.underlay == UnderlayProtocol::Tcp
                            && entry.key.underlay == request.service.underlay
                            // RepairData may remain as carrier pressure, but it
                            // cannot preserve a unique OwnerData policy fence.
                            && entry.owner_data_in_flight_bytes == 0
                    });
                    let identity = (request.target, request.target_incarnation);
                    if !service_is_exact_and_proven
                        || !target_is_exact_and_drained
                        || outputs.active_ack_clock_calibration.is_some()
                    {
                        return false;
                    }
                    let flights = self
                        .flights
                        .lock()
                        .expect("server reliable stream flight lock");
                    let has_exact_owner_flight = flights.values().flatten().any(|flight| {
                        flight.key == request.target
                            && flight.output_incarnation == request.target_incarnation
                            && flight.kind.is_ordering_owner()
                    });
                    if has_exact_owner_flight {
                        return false;
                    }
                    drop(flights);

                    let Some(calibration) = outputs.ack_clock_calibrations.get_mut(&identity)
                    else {
                        return false;
                    };
                    if calibration.proven
                        || calibration.retired
                        || calibration.spent_bytes != 0
                        || calibration.credit_limit_bytes != request.limit_bytes
                        || calibration.credit_limit_bytes > calibration.max_limit_bytes
                    {
                        return false;
                    }
                    calibration.retire();
                    retired = Some(*calibration);
                    subflow_state.planner_generation =
                        subflow_state.planner_generation.wrapping_add(1);
                    true
                },
            )
            .unwrap_or(false);
        drop(outputs);
        if !applied {
            return false;
        }
        #[cfg(feature = "lab-diagnostics")]
        if let Some(calibration) = retired {
            lab_diagnostic(
                "response_ack_clock_calibration",
                format_args!(
                    "phase=terminal session_id={} binding_instance_id={} underlay={:?} path_id={} incarnation={} reason=completion_horizon active_owner_flights=false calibrated_rate_ready=false calibrated_rate_bps=0 spent_bytes={} previous_credit_limit_bytes={} credit_limit_bytes={} max_limit_bytes={} stage_authorized_spent_bytes={} stage_credit_bytes={} stage_strict_capacity_bytes={} stage_evidence_bytes={} stage_rate_ineligible_bytes={} proven={} retired={}",
                    self.session_id.0,
                    self.binding_instance_id,
                    request.target.underlay,
                    request.target.path_id.0,
                    request.target_incarnation,
                    calibration.spent_bytes,
                    request.limit_bytes,
                    calibration.credit_limit_bytes,
                    calibration.max_limit_bytes,
                    calibration.stage_authorized_spent_bytes,
                    calibration.stage_credit_bytes(),
                    calibration.stage_strict_capacity_bytes(),
                    calibration.stage_rate_evidence_bytes,
                    calibration.stage_rate_ineligible_bytes,
                    calibration.proven,
                    calibration.retired,
                ),
            );
        }
        #[cfg(not(feature = "lab-diagnostics"))]
        let _ = retired;
        self.notify_update();
        true
    }

    #[cfg(test)]
    pub(in crate::runtime) fn try_enqueue_owner_frame_for_target(
        &self,
        target: &ResponseSenderPathTarget,
        frame: &Frame,
        lane: FlowLane,
        subflow_request: Option<ResponseSubflowAdmissionRequest>,
        calibration_request: Option<ResponseAckClockCalibrationRequest>,
    ) -> Result<Option<u64>, RuntimeError> {
        self.try_enqueue_owner_frame_for_dispatch_target(
            &target.into(),
            frame,
            lane,
            subflow_request,
            calibration_request,
        )
    }

    pub(in crate::runtime) fn try_enqueue_owner_frame_for_dispatch_target(
        &self,
        target: &ResponseDispatchTarget,
        frame: &Frame,
        lane: FlowLane,
        subflow_request: Option<ResponseSubflowAdmissionRequest>,
        calibration_request: Option<ResponseAckClockCalibrationRequest>,
    ) -> Result<Option<u64>, RuntimeError> {
        self.try_enqueue_owner_frame_for_target_inner(
            target,
            frame,
            lane,
            subflow_request,
            calibration_request,
            || {},
        )
    }

    fn try_enqueue_owner_frame_for_target_inner(
        &self,
        target: &ResponseDispatchTarget,
        frame: &Frame,
        lane: FlowLane,
        subflow_request: Option<ResponseSubflowAdmissionRequest>,
        calibration_request: Option<ResponseAckClockCalibrationRequest>,
        after_subflow_reservation: impl FnOnce(),
    ) -> Result<Option<u64>, RuntimeError> {
        if !self.response_stream_open.load(Ordering::Acquire) {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        if !self.response_stream_open.load(Ordering::Acquire) {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        let target_matches = |entry: &ResponseStreamOutputEntry| {
            entry.key == target.key
                && entry.incarnation == target.incarnation
                && entry.commands.same_channel(&target.commands)
                && entry.role == target.attachment_role
                && entry.role != StreamOpenRole::Repair
        };
        let target_index = outputs
            .entries
            .last()
            .filter(|entry| target_matches(entry))
            .map(|_| outputs.entries.len() - 1)
            .or_else(|| outputs.entries.iter().position(target_matches));
        let Some(target_index) = target_index else {
            return Err(RuntimeError::SenderServiceBlocked);
        };
        if subflow_request.is_some() && calibration_request.is_some() {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        if let Some(request) = calibration_request {
            let Some((_, _, payload_bytes)) = reliable_stream_frame_extent(frame) else {
                return Err(RuntimeError::SenderServiceBlocked);
            };
            let calibration_ceiling = reliable_ack_clock_calibration_ceiling_bytes(self.mux_limits);
            let calibration_limit = request.limit_bytes.min(calibration_ceiling);
            return self
                .lane_tracker
                .with_matching_generation_and_min_active_response_flows(
                    self.session_id,
                    request.expected_lane_generation,
                    if request.requires_active_response_start {
                        MIN_ACTIVE_RESPONSE_FLOWS_FOR_SAME_FAMILY_DISCOVERY
                    } else {
                        0
                    },
                    || {
                    {
                        if self.response_model_generation.load(Ordering::Acquire)
                            != request.expected_model_generation
                        {
                            return Err(RuntimeError::SenderServiceBlocked);
                        }
                        let state = self
                            .subflow_set
                            .lock()
                            .expect("server reliable stream subflow set lock");
                        if state.planner_generation != request.expected_planner_generation
                            || state.set.as_ref().is_none_or(|epoch| {
                                epoch.service_key() != request.service
                                    || epoch.startup_owner_key().is_some()
                            })
                        {
                            return Err(RuntimeError::SenderServiceBlocked);
                        }
                    }
                    let service_is_exact_and_proven = outputs.entries.iter().any(|entry| {
                        entry.key == request.service
                            && entry.incarnation == request.service_incarnation
                            && entry.role != StreamOpenRole::Repair
                            && !entry.commands.is_closed()
                            && entry.commands.pending_bytes() == request.service_pending_bytes
                            && entry.key.underlay == UnderlayProtocol::Tcp
                            && server_output_has_bulk_rate_evidence_with_limits(
                                entry,
                                self.mux_limits,
                            )
                    });
                    let target_entry = &outputs.entries[target_index];
                    let identity = (target_entry.key, target_entry.incarnation);
                    let target_is_tcp_validation = target_entry.role == StreamOpenRole::Validation
                        && target_entry.key.underlay == UnderlayProtocol::Tcp
                        && target_entry.key.underlay == request.service.underlay
                        && !target_entry.commands.is_closed()
                        && target_entry.commands.pending_bytes() == request.target_pending_bytes;
                    // The product-flight ledger already includes frames that
                    // remain pending in the carrier command pipe.
                    let target_has_calibration_headroom = target_entry
                        .bytes_in_flight
                        .max(target_entry.commands.pending_bytes())
                        .saturating_add(payload_bytes as u64)
                        <= calibration_limit;
                    let active_matches = outputs
                        .active_ack_clock_calibration
                        .is_none_or(|active| active == identity);
                    let calibration_is_available = outputs
                        .ack_clock_calibrations
                        .get(&identity)
                        .is_some_and(|calibration| {
                            !calibration.proven
                                && request.limit_bytes == calibration.credit_limit_bytes
                                && calibration.credit_limit_bytes <= calibration.max_limit_bytes
                                && calibration.max_limit_bytes <= calibration_ceiling
                                && calibration
                                    .spent_bytes
                                    .saturating_add(payload_bytes as u64)
                                    <= calibration_limit
                        });
                    if !service_is_exact_and_proven
                        || !target_is_tcp_validation
                        || !target_has_calibration_headroom
                        || !active_matches
                        || !calibration_is_available
                    {
                        return Err(RuntimeError::SenderServiceBlocked);
                    }

                    let previous_active = outputs.active_ack_clock_calibration;
                    let previous_calibration = *outputs
                        .ack_clock_calibrations
                        .get(&identity)
                        .expect("validated response calibration identity");
                    let reserved_calibration = {
                        let calibration = outputs
                        .ack_clock_calibrations
                        .get_mut(&identity)
                        .expect("validated response calibration identity");
                        calibration.spent_bytes = calibration
                            .spent_bytes
                            .saturating_add(payload_bytes as u64);
                        *calibration
                    };
                    #[cfg(not(feature = "lab-diagnostics"))]
                    let _ = reserved_calibration;
                    outputs.active_ack_clock_calibration = Some(identity);
                    if let Err(err) = target
                        .commands
                        .try_enqueue_stream_ordered_frame(frame.clone(), lane)
                    {
                        *outputs
                            .ack_clock_calibrations
                            .get_mut(&identity)
                            .expect("reserved response calibration identity") =
                            previous_calibration;
                        outputs.active_ack_clock_calibration = previous_active;
                        return Err(err);
                    }
                    self.record_validated_owner_flight_with_outputs(
                        &mut outputs,
                        target_index,
                        frame,
                    );
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "response_ack_clock_calibration",
                        format_args!(
                            "phase=selected session_id={} binding_instance_id={} underlay={:?} path_id={} incarnation={} payload_bytes={} spent_bytes={} credit_limit_bytes={} max_limit_bytes={} proven={}",
                            self.session_id.0,
                            self.binding_instance_id,
                            identity.0.underlay,
                            identity.0.path_id.0,
                            identity.1,
                            payload_bytes,
                            reserved_calibration.spent_bytes,
                            reserved_calibration.credit_limit_bytes,
                            reserved_calibration.max_limit_bytes,
                            reserved_calibration.proven,
                        ),
                    );
                    Ok(None)
                    },
                )
                .unwrap_or(Err(RuntimeError::SenderServiceBlocked));
        }
        if let Some(request) = subflow_request {
            return self
                .lane_tracker
                .with_matching_generation(self.session_id, request.expected_lane_generation, || {
                    let reservation = self.reserve_subflow_owner_admission_for_request(request);
                    if reservation.admission.decision != PathAdmissionDecision::AdmitSubflow {
                        return Err(RuntimeError::SenderServiceBlocked);
                    }
                    after_subflow_reservation();
                    if let Err(err) = target
                        .commands
                        .try_enqueue_stream_ordered_frame(frame.clone(), lane)
                    {
                        if let Some(epoch_generation) = reservation.epoch_generation {
                            self.rollback_subflow_owner_admission_for_epoch(
                                epoch_generation,
                                request.input,
                            );
                        }
                        return Err(err);
                    }
                    self.record_validated_owner_flight_with_outputs(
                        &mut outputs,
                        target_index,
                        frame,
                    );
                    Ok(reservation.epoch_generation)
                })
                .unwrap_or(Err(RuntimeError::SenderServiceBlocked));
        }
        target
            .commands
            .try_enqueue_stream_ordered_frame(frame.clone(), lane)?;
        self.record_validated_owner_flight_with_outputs(&mut outputs, target_index, frame);
        Ok(None)
    }

    #[cfg(feature = "lab-diagnostics")]
    fn lab_response_service_feed_state(
        &self,
        entry: &ResponseStreamOutputEntry,
        snapshot: PathSnapshot,
        lane: FlowLane,
        is_active: bool,
        has_bulk_rate_evidence: bool,
        has_service_feed_evidence: bool,
        command_pending_bytes: u64,
    ) {
        if !lab_diagnostic_event_enabled("response_service_feed_state") {
            return;
        }

        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(self.mux_limits);
        let startup_floor = reliable_subflow_startup_sample_limit_bytes(self.mux_limits);
        let service_floor =
            bulk_service_horizon_payload_bytes(payload_bytes, self.mux_limits) as u64;
        let progress_bucket = |bytes: u64| {
            if bytes == 0 {
                0
            } else if bytes < startup_floor {
                1
            } else if bytes < service_floor {
                2
            } else {
                3
            }
        };
        let local_metrics = entry
            .local_path_metrics
            .as_ref()
            .filter(|metrics| metrics.source == ServerPathMetricsSource::LocalSender)
            .map(|metrics| metrics.metrics);
        let carrier_sample_bytes = local_metrics.map_or(0, |metrics| metrics.data_sample_bytes);
        let carrier_sample_count = local_metrics.map_or(0, |metrics| metrics.data_sample_count);
        let carrier_sample_available =
            local_metrics.is_some_and(|metrics| metrics.has_ack_derived_data_sample);
        let carrier_app_limited = local_metrics.is_none_or(|metrics| metrics.app_limited);
        let latency_pressure = snapshot.active_latency_sensitive_flows > 0
            || snapshot.session_active_latency_sensitive_flows > 0;
        let source_limit_bytes = if latency_pressure {
            service_floor
        } else if has_service_feed_evidence {
            bulk_active_service_product_envelope_bytes(snapshot, payload_bytes, self.mux_limits)
        } else {
            bulk_service_feed_reservoir_payload_bytes(payload_bytes, self.mux_limits) as u64
        };
        let emission_limit_bytes = if !has_service_feed_evidence {
            if entry.key.underlay == UnderlayProtocol::Udp {
                bulk_service_feed_reservoir_payload_bytes(payload_bytes, self.mux_limits) as u64
            } else {
                service_floor
            }
        } else if latency_pressure {
            bulk_latency_pressure_service_feed_window_bytes(payload_bytes, self.mux_limits)
        } else {
            bulk_active_service_product_envelope_bytes(snapshot, payload_bytes, self.mux_limits)
        };
        let state = ResponseServiceFeedDiagnosticState {
            path_instance_id: entry.path_instance_id,
            attachment_role: entry.role,
            is_active,
            has_bulk_rate_evidence,
            has_service_feed_evidence,
            owner_progress_bucket: progress_bucket(entry.owner_data_acked_bytes),
            product_rate_available: entry.product_progress_rate_bps.is_some(),
            carrier_sample_bucket: progress_bucket(carrier_sample_bytes),
            carrier_sample_available,
            carrier_app_limited,
            latency_pressure,
            source_limit_bytes,
            emission_limit_bytes,
        };
        let identity = (entry.key, entry.incarnation);
        let mut previous = self
            .response_service_feed_diagnostic
            .lock()
            .expect("response Service-feed diagnostic lock");
        if previous.get(&identity) == Some(&state) {
            return;
        }
        previous.insert(identity, state);
        drop(previous);

        lab_diagnostic(
            "response_service_feed_state",
            format_args!(
                "session_id={} binding_instance_id={} path_underlay={:?} path_id={} path_instance_id={} incarnation={} attachment_role={:?} lane={:?} is_active={} latency_pressure={} owner_data_acked_bytes={} owner_progress_bucket={} product_progress_rate_mbps={:.3} product_rate_available={} carrier_sample_bytes={} carrier_sample_count={} carrier_sample_available={} carrier_app_limited={} startup_floor_bytes={} service_floor_bytes={} bulk_rate_evidence={} service_feed_evidence={} source_limit_bytes={} emission_limit_bytes={} command_pending_bytes={} owner_data_inflight_bytes={} product_inflight_bytes={} product_queue_bytes={} path_queue_bytes={}",
                self.session_id.0,
                self.binding_instance_id,
                entry.key.underlay,
                entry.key.path_id.0,
                entry.path_instance_id.as_u64(),
                entry.incarnation,
                entry.role,
                lane,
                is_active,
                latency_pressure,
                entry.owner_data_acked_bytes,
                state.owner_progress_bucket,
                entry.product_progress_rate_bps.unwrap_or(0.0) / 1_000_000.0,
                state.product_rate_available,
                carrier_sample_bytes,
                carrier_sample_count,
                carrier_sample_available,
                carrier_app_limited,
                startup_floor,
                service_floor,
                has_bulk_rate_evidence,
                has_service_feed_evidence,
                source_limit_bytes,
                emission_limit_bytes,
                command_pending_bytes,
                entry.owner_data_in_flight_bytes,
                snapshot.product_bytes_in_flight,
                snapshot.product_queue_bytes,
                snapshot.queue_bytes,
            ),
        );
    }

    pub(in crate::runtime) fn sender_path_targets(
        &self,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Vec<ResponseSenderPathTarget> {
        let stored_active_key = self.ordered_data_owner();
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let request_active_key = *self
            .request_active_owner
            .lock()
            .expect("server reliable stream request active owner lock");
        let active_key = response_live_ordered_data_owner(stored_active_key, &outputs.entries);
        let now = Instant::now();
        let response_scheduling = self.lane_tracker.response_path_scheduling_snapshots(
            self.session_id,
            outputs
                .entries
                .iter()
                .map(|entry| (entry.key, entry.path_instance_id)),
        );
        outputs
            .entries
            .iter()
            .zip(response_scheduling)
            .map(|(entry, response_scheduling)| {
                let command_pending_bytes = entry.commands.pending_bytes();
                let calibration_identity = (entry.key, entry.incarnation);
                let calibration = outputs
                    .ack_clock_calibrations
                    .get(&calibration_identity)
                    .copied();
                let response_snapshot = server_bulk_output_snapshot_with_scheduling(
                    entry,
                    lane,
                    self.mux_limits,
                    now,
                    command_pending_bytes,
                    response_scheduling,
                );
                let snapshot = response_snapshot.path;
                let is_active = Some(entry.key) == active_key;
                let has_bulk_rate_evidence =
                    server_output_has_bulk_rate_evidence_with_limits(entry, self.mux_limits);
                let has_service_feed_evidence = has_bulk_rate_evidence
                    || (is_active
                        && server_output_has_service_feed_evidence_with_limits(
                            entry,
                            self.mux_limits,
                        ));
                let endpoint_only_service_prior_eligible =
                    server_output_accepts_service_capacity_prior(entry);
                #[cfg(feature = "lab-diagnostics")]
                self.lab_response_service_feed_state(
                    entry,
                    snapshot,
                    lane,
                    is_active,
                    has_bulk_rate_evidence,
                    has_service_feed_evidence,
                    command_pending_bytes,
                );
                ResponseSenderPathTarget {
                    #[cfg(feature = "lab-diagnostics")]
                    session_id: self.session_id,
                    #[cfg(feature = "lab-diagnostics")]
                    binding_instance_id: self.binding_instance_id,
                    key: entry.key,
                    path_instance_id: entry.path_instance_id,
                    incarnation: entry.incarnation,
                    commands: entry.commands.clone(),
                    attachment_role: entry.role,
                    snapshot,
                    owner_data_in_flight_bytes: entry.owner_data_in_flight_bytes,
                    command_pending_bytes,
                    eta_ms: server_bulk_output_eta_ms(
                        entry.key,
                        snapshot,
                        active_key,
                        lane,
                        payload_bytes,
                        self.mux_limits,
                    ),
                    is_active,
                    is_request_active: Some(entry.key) == request_active_key,
                    has_sender_evidence: server_output_has_sender_evidence(entry),
                    has_service_feed_evidence,
                    has_bulk_rate_evidence,
                    endpoint_only_service_prior_eligible,
                    quic_capacity_proof: server_output_quic_capacity_proof_marker(entry),
                    quic_capacity_calibration_attempts: response_snapshot
                        .quic_capacity_calibration_attempts,
                    ack_clock_calibration_eligible: calibration.is_some(),
                    ack_clock_calibration_proven: calibration
                        .is_some_and(|calibration| calibration.proven),
                    ack_clock_calibration_spent_bytes: calibration
                        .map_or(0, |calibration| calibration.spent_bytes),
                    ack_clock_calibration_credit_limit_bytes: calibration
                        .map_or(0, |calibration| calibration.credit_limit_bytes),
                    ack_clock_calibration_max_limit_bytes: calibration
                        .map_or(0, |calibration| calibration.max_limit_bytes),
                    ack_clock_calibration_active: outputs.active_ack_clock_calibration
                        == Some(calibration_identity),
                }
            })
            .collect()
    }

    pub(in crate::runtime) fn mux_limits(&self) -> MuxLimits {
        self.mux_limits
    }

    pub(in crate::runtime) fn active_tcp_ack_clock_calibration_remaining_bytes(
        &self,
    ) -> Option<usize> {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let identity = outputs.active_ack_clock_calibration?;
        if identity.0.underlay != UnderlayProtocol::Tcp {
            return None;
        }
        let calibration = outputs.ack_clock_calibrations.get(&identity)?;
        if calibration.proven || calibration.retired {
            return None;
        }
        let remaining = calibration
            .credit_limit_bytes
            .saturating_sub(calibration.spent_bytes);
        (remaining > 0).then(|| usize::try_from(remaining).unwrap_or(usize::MAX))
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_output_bulk_proven_for_test(&self, key: CarrierPathKey) {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let entry = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == key)
            .expect("test bulk-proven output");
        entry.product_progress_rate_bps = Some(100_000_000.0);
        entry.delivery_rate_bps = Some(100_000_000.0);
        entry.delivery_samples = 1;
        entry.owner_data_acked_bytes = reliable_subflow_startup_sample_limit_bytes(self.mux_limits);
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn set_output_product_model_for_test(
        &self,
        key: CarrierPathKey,
        rate_bps: f64,
        srtt_ms: f64,
    ) {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let entry = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == key)
            .expect("test modeled output");
        entry.product_progress_rate_bps = Some(rate_bps.max(1.0));
        entry.delivery_rate_bps = Some(rate_bps.max(1.0));
        entry.srtt_ms = Some(srtt_ms.max(1.0));
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn install_tcp_ack_clock_calibration_for_test(
        &self,
        key: CarrierPathKey,
        spent_bytes: u64,
        credit_limit_bytes: u64,
        max_limit_bytes: u64,
        active: bool,
    ) {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == key)
            .expect("test calibration output");
        assert_eq!(entry.key.underlay, UnderlayProtocol::Tcp);
        let identity = (entry.key, entry.incarnation);
        let mut calibration =
            ResponseAckClockCalibrationState::new(credit_limit_bytes, max_limit_bytes);
        calibration.spent_bytes = spent_bytes;
        outputs.ack_clock_calibrations.insert(identity, calibration);
        if active {
            outputs.active_ack_clock_calibration = Some(identity);
        } else if outputs.active_ack_clock_calibration == Some(identity) {
            outputs.active_ack_clock_calibration = None;
        }
    }

    pub(in crate::runtime) fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub(in crate::runtime) async fn close_stream(&self, stream_id: StreamId) {
        let outputs = {
            let outputs = self
                .outputs
                .lock()
                .expect("server reliable stream binding lock");
            self.response_stream_open.store(false, Ordering::Release);
            outputs.entries.clone()
        };
        self.response_flow_registration.set_active(false);
        self.lane_tracker
            .clear_quic_capacity_calibration_for_binding(self.session_id, self.binding_instance_id);
        self.lane_tracker
            .clear_response_service_handoff_drain_for_binding(
                self.session_id,
                self.binding_instance_id,
            );
        for entry in outputs {
            let _ = entry
                .commands
                .send_control(ReliablePathCommand::CloseStream(stream_id))
                .await;
        }
        let mut lead = self
            .ordered_data_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *lead = None;
        self.response_flow_registration.set_service(None);
    }

    pub(in crate::runtime) async fn close_stream_ordered(
        &self,
        stream_id: StreamId,
        lane: FlowLane,
    ) {
        let outputs = {
            let outputs = self
                .outputs
                .lock()
                .expect("server reliable stream binding lock");
            self.response_stream_open.store(false, Ordering::Release);
            outputs.entries.clone()
        };
        self.response_flow_registration.set_active(false);
        self.lane_tracker
            .clear_quic_capacity_calibration_for_binding(self.session_id, self.binding_instance_id);
        self.lane_tracker
            .clear_response_service_handoff_drain_for_binding(
                self.session_id,
                self.binding_instance_id,
            );
        for entry in outputs {
            let _ = entry
                .commands
                .send_stream_ordered_close(stream_id, lane)
                .await;
        }
        let mut lead = self
            .ordered_data_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *lead = None;
        self.response_flow_registration.set_service(None);
    }

    fn notify_update(&self) {
        let current = *self.version.borrow();
        let _ = self.version.send(current.wrapping_add(1));
    }

    pub(in crate::runtime) fn update_path_metrics_for_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
        metrics: PathMetrics,
        source: ServerPathMetricsSource,
    ) {
        self.update_path_metrics_matching(key, Some(path_instance_id), metrics, source);
    }

    pub(in crate::runtime) fn install_quic_capacity_proof_for_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
        metrics: PathMetrics,
        candidate: QuicCapacityProofCandidate,
    ) -> bool {
        self.install_path_metrics_entry_matching(
            key,
            Some(path_instance_id),
            ServerPathMetricsEntry {
                metrics,
                source: ServerPathMetricsSource::LocalSender,
                recorded_at: Instant::now(),
                capacity_proof: Some(candidate),
                tcp_capacity_proof: None,
            },
            false,
        )
        .0
    }

    pub(in crate::runtime) fn install_tcp_capacity_proof_for_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
        metrics: PathMetrics,
        candidate: TcpCapacityProofCandidate,
    ) -> bool {
        self.install_path_metrics_entry_matching(
            key,
            Some(path_instance_id),
            ServerPathMetricsEntry {
                metrics,
                source: ServerPathMetricsSource::LocalSender,
                recorded_at: Instant::now(),
                capacity_proof: None,
                tcp_capacity_proof: Some(candidate),
            },
            false,
        )
        .0
    }

    pub(in crate::runtime) fn install_stored_path_metrics_for_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
        path_metrics: ServerPathMetricsEntry,
    ) {
        self.install_path_metrics_entry_matching(key, Some(path_instance_id), path_metrics, true);
    }

    pub(in crate::runtime) fn notify_installed_path_metrics(&self) {
        self.graduate_completed_response_startup_owner();
        self.notify_update();
    }

    #[cfg(test)]
    pub(in crate::runtime) fn update_path_metrics(
        &self,
        key: CarrierPathKey,
        metrics: PathMetrics,
        source: ServerPathMetricsSource,
    ) {
        self.update_path_metrics_matching(key, None, metrics, source);
    }

    fn update_path_metrics_matching(
        &self,
        key: CarrierPathKey,
        path_instance_id: Option<ServerCarrierPathInstanceId>,
        metrics: PathMetrics,
        source: ServerPathMetricsSource,
    ) {
        let (_, changed) = self.install_path_metrics_entry_matching(
            key,
            path_instance_id,
            ServerPathMetricsEntry {
                metrics,
                source,
                recorded_at: Instant::now(),
                capacity_proof: None,
                tcp_capacity_proof: None,
            },
            true,
        );
        if changed {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "server_response_path_metrics_attached",
                format_args!(
                    "session_id={} underlay={:?} path_id={} source={:?} direction={:?} rate_mbps={:.3} pacing_mbps={:.3} srtt_ms={:.3} confidence_ppm={} app_limited={} ack_sample={} sample_count={} sample_bytes={}",
                    self.session_id.0,
                    key.underlay,
                    key.path_id.0,
                    source,
                    metrics.direction,
                    metrics.delivery_rate_bps as f64 / 1_000_000.0,
                    metrics.pacing_rate_bps as f64 / 1_000_000.0,
                    metrics.srtt_us as f64 / 1000.0,
                    metrics.confidence_ppm,
                    metrics.app_limited,
                    metrics.has_ack_derived_data_sample,
                    metrics.data_sample_count,
                    metrics.data_sample_bytes,
                ),
            );
        }
    }

    fn install_path_metrics_entry_matching(
        &self,
        key: CarrierPathKey,
        path_instance_id: Option<ServerCarrierPathInstanceId>,
        mut path_metrics: ServerPathMetricsEntry,
        notify: bool,
    ) -> (bool, bool) {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let now = Instant::now();
        let source = path_metrics.source;
        let metrics = path_metrics.metrics;
        let explicit_quic_capacity_proof = path_metrics.capacity_proof.is_some();
        let explicit_tcp_capacity_proof = path_metrics.tcp_capacity_proof.is_some();
        let mut matched = false;
        let mut changed = false;
        for entry in &mut outputs.entries {
            if entry.key == key
                && path_instance_id.is_none_or(|instance| entry.path_instance_id == instance)
            {
                matched = true;
                let current = match source {
                    ServerPathMetricsSource::LocalSender => &mut entry.local_path_metrics,
                    ServerPathMetricsSource::PeerHint => &mut entry.peer_path_metrics,
                };
                if !explicit_quic_capacity_proof {
                    path_metrics.capacity_proof = current
                        .and_then(|previous| previous.capacity_proof)
                        .filter(|proof| proof.expires_at > now);
                }
                if !explicit_tcp_capacity_proof {
                    path_metrics.tcp_capacity_proof = current
                        .and_then(|previous| previous.tcp_capacity_proof)
                        .filter(|proof| proof.expires_at > now);
                }
                let scheduling_changed = current.is_none_or(|previous| {
                    previous.source != source
                        || !server_path_metrics_scheduling_equivalent(previous.metrics, metrics)
                        || previous.capacity_proof != path_metrics.capacity_proof
                        || previous.tcp_capacity_proof != path_metrics.tcp_capacity_proof
                });
                *current = Some(path_metrics);
                changed |= scheduling_changed;
            }
        }
        if changed {
            self.response_model_generation
                .fetch_add(1, Ordering::AcqRel);
        }
        drop(outputs);
        if changed && notify {
            self.graduate_completed_response_startup_owner();
            self.notify_update();
        }
        (matched, changed)
    }
}

fn server_path_metrics_scheduling_equivalent(
    mut left: PathMetrics,
    mut right: PathMetrics,
) -> bool {
    // Epoch and age refresh evidence lifetime but do not change a ranking or
    // admission input. Suppressing that no-op update avoids waking every bound
    // response stream on each idle QUIC metrics poll.
    left.metric_epoch = 0;
    left.metric_age_us = 0;
    right.metric_epoch = 0;
    right.metric_age_us = 0;
    left == right
}

#[cfg(test)]
#[path = "response_binding_test.rs"]
mod tests;
