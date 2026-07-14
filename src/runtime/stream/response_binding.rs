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
#[path = "response_transaction.rs"]
mod response_transaction;

pub(in crate::runtime) use crate::runtime::path::quic::metrics::QuicCapacityProofCandidate;
#[cfg(test)]
pub(in crate::runtime) use response_ack_clock::{
    ResponseAckClockCalibrationState, ResponseAckClockRateEvidence,
    ResponseAckClockRateEvidenceUpdate,
};
pub(in crate::runtime) use response_admission::ResponseSubflowAdmissionRequest;
use response_admission::ResponseSubflowSetState;
#[cfg(test)]
pub(in crate::runtime) use response_admission::{
    ResponseSubflowAdmissionReservation, server_output_accepts_service_capacity_prior,
    server_output_has_bulk_rate_evidence, server_output_has_bulk_rate_evidence_with_limits,
    server_output_has_durable_product_progress, server_output_has_sender_evidence,
    server_output_has_service_feed_evidence_with_limits,
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
#[cfg(test)]
pub(in crate::runtime) use response_session::ResponseSessionSchedulingSnapshot;
#[cfg(any(test, feature = "lab-diagnostics"))]
pub(in crate::runtime) use response_session::well_formed_quic_capacity_proof_candidate;
pub(in crate::runtime) use response_session::{
    ResponseServiceFamilyLoads, ResponseServiceHandoffDrainReservation, ServerPathLaneTracker,
    ServerRealtimeFlowRegistration, TcpCapacityProbeSessionLease,
    quic_capacity_proof_pin_matches_marker, quic_capacity_receipt_rate_bps,
    valid_quic_capacity_proof_candidate_at,
};
pub(in crate::runtime) use response_snapshot::server_bulk_output_eta_ms;
#[cfg(test)]
pub(in crate::runtime) use response_snapshot::{
    ResponseRelayReadSnapshot, ResponseSourceServiceSnapshot,
};
#[cfg(test)]
pub(in crate::runtime) use response_topology::TcpResponseCapacityPrior;
pub(in crate::runtime) use response_topology::{
    ResponseDispatchTarget, ResponseSenderPathTarget, ResponseStreamAttachOutcome,
    ResponseStreamOutputEntry, ResponseStreamOutputs, ServerCarrierPathInstanceId,
    next_server_carrier_path_instance_id,
};
use response_topology::{
    response_owner_underlay_seen_bit, response_stream_role_reserves_flow_load,
};
pub(in crate::runtime) use response_transaction::{
    ResponseAckClockCalibrationRequest, ResponseAckClockCalibrationRetirementRequest,
};

#[cfg(test)]
use self::response_evidence::server_output_quic_capacity_proof_marker;
#[cfg(test)]
use self::response_session::ServerQuicCapacityCalibrationPhase;
use self::response_session::ServerResponseFlowRegistration;
#[cfg(test)]
use self::response_snapshot::server_bulk_output_snapshot;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{lab_diagnostic, lab_diagnostic_event_enabled};
#[cfg(test)]
use crate::model::ack_clock::{
    reliable_ack_clock_calibration_ceiling_bytes, reliable_ack_clock_calibration_limit_bytes,
};
#[cfg(feature = "lab-diagnostics")]
use crate::model::admission::{
    bulk_active_service_product_envelope_bytes, bulk_latency_pressure_service_feed_window_bytes,
    bulk_service_feed_reservoir_payload_bytes, bulk_service_horizon_payload_bytes,
};
#[cfg(test)]
use crate::model::capacity::PathRateSample;
#[cfg(any(test, feature = "lab-diagnostics"))]
use crate::model::capacity::reliable_subflow_startup_sample_limit_bytes;
#[cfg(test)]
use crate::model::multipath::{FlowSubflowSet, PathAdmissionDecision, SubflowAdmissionInput};
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
#[cfg(test)]
use crate::protocol::Frame;
#[cfg(test)]
use crate::protocol::OffsetRange;
#[cfg(test)]
use crate::protocol::PathMetrics;
use crate::protocol::{PathId, SessionId, StreamId, StreamOpenRole, UnderlayProtocol};
#[cfg(test)]
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::{ReliablePathCommand, ReliablePathCommandSender};
#[cfg(feature = "lab-diagnostics")]
use crate::runtime::relay::io::reliable_bulk_carrier_feed_quantum_bytes;
use crate::scheduler::FlowLane;
#[cfg(any(test, feature = "lab-diagnostics"))]
use crate::scheduler::PathSnapshot;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
#[cfg(feature = "lab-diagnostics")]
use std::time::Duration;
#[cfg(any(test, feature = "lab-diagnostics"))]
use std::time::Instant;
use tokio::sync::watch;

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

    pub(in crate::runtime) fn try_reserve_tcp_capacity_probe(
        &self,
        expected_generation: u64,
    ) -> Option<TcpCapacityProbeSessionLease> {
        self.lane_tracker
            .try_reserve_tcp_capacity_probe(self.session_id, expected_generation)
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
}

#[cfg(test)]
#[path = "response_binding_test.rs"]
mod tests;
