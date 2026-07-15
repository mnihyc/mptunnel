//! Server response-stream ownership and its narrow runtime contract.
//!
//! The binding schema stays here so child transition owners share private
//! invariants without widening its locks or state fields.

mod ack_clock;
mod attachment;
mod delivery;
mod diagnostics;
mod evidence;
mod handoff;
mod owner_commit;
mod quic_admission;
mod quic_capacity;
mod session;
mod session_load;
mod snapshot;
mod subflow;

pub(in crate::runtime) use attachment::{
    ResponseDispatchTarget, ResponseSenderPathTarget, ResponseStreamAttachOutcome,
    next_server_carrier_path_instance_id,
};
use attachment::{
    ResponseStreamOutputEntry, ResponseStreamOutputs, response_owner_underlay_seen_bit,
    response_stream_role_reserves_flow_load,
};
use delivery::ResponseAckOrderingState;
pub(super) use delivery::{
    CarrierPathFlight, product_flights_have_recent_repair_overlap,
    release_carrier_path_flight_ranges,
};
pub(in crate::runtime) use diagnostics::record_server_sender_decision;
#[cfg(feature = "lab-diagnostics")]
use diagnostics::{ResponseServiceFeedDiagnosticState, ResponseServiceHandoffDiagnosticState};
pub(in crate::runtime) use evidence::{ServerPathMetricsEntry, ServerPathMetricsSource};
pub(in crate::runtime) use owner_commit::{
    ResponseAckClockCalibrationRequest, ResponseAckClockCalibrationRetirementRequest,
    ResponseOwnerEnqueueAdmission,
};
pub(in crate::runtime) use quic_admission::ResponseQuicCapacityCalibrationRequest;
#[cfg(feature = "lab-diagnostics")]
pub(in crate::runtime) use quic_capacity::well_formed_quic_capacity_proof_candidate;
pub(in crate::runtime) use quic_capacity::{
    quic_capacity_proof_pin_matches_marker, valid_quic_capacity_proof_candidate_at,
};
pub(in crate::runtime) use session::{
    ResponseServiceHandoffDrainReservation, ServerPathLaneTracker, TcpCapacityProbeSessionLease,
};
pub(in crate::runtime) use session_load::ServerRealtimeFlowRegistration;
pub(in crate::runtime) use snapshot::server_bulk_output_eta_ms;
pub(in crate::runtime) use subflow::ResponseSubflowAdmissionRequest;
use subflow::ResponseSubflowSetState;

use self::session_load::ServerResponseFlowRegistration;
use crate::model::capacity::QuicCapacityProofCandidate;
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey};
use crate::model::response::ResponseServiceHandoffMode;
use crate::mux::MuxLimits;
use crate::protocol::{PathId, ResetReason, SessionId, StreamId, StreamOpenRole, UnderlayProtocol};
use crate::runtime::path::commands::{ReliablePathCommand, ReliablePathCommandSender};
use crate::scheduler::FlowLane;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;

// Reliable-path bindings own attachment instances, exact range flights,
// evidence, and atomic commit. Sender services rank immutable snapshots.

pub(in crate::runtime) const MAX_RESPONSE_QUIC_CAPACITY_CALIBRATION_ATTEMPTS_PER_PATH: u8 = 2;
// One sustained response stream is real demand. Same-family discovery stays
// single-owner and bounded; requiring a second stream prevents large one-flow
// transfers from ever measuring spare paths.
pub(in crate::runtime) const MIN_ACTIVE_RESPONSE_FLOWS_FOR_SAME_FAMILY_DISCOVERY: u32 = 1;
static NEXT_RESPONSE_STREAM_BINDING_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy)]
/// Bounded pause of fresh OwnerData assignment for one response binding while
/// already-owned ranges reach the STREAM_ACK frontier. Offset-free source
/// staging remains sender-service state, outside this transaction.
pub(in crate::runtime) struct ResponseServiceHandoffDrainRequest {
    pub(in crate::runtime) expected_planner_generation: u64,
    pub(in crate::runtime) expected_lane_generation: u64,
    pub(in crate::runtime) expected_model_generation: u64,
    pub(in crate::runtime) service: CarrierPathKey,
    pub(in crate::runtime) service_path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime) service_incarnation: u64,
    pub(in crate::runtime) target: CarrierPathKey,
    pub(in crate::runtime) target_path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime) target_incarnation: u64,
    pub(in crate::runtime) mode: ResponseServiceHandoffMode,
    pub(in crate::runtime) capacity_proof: Option<QuicCapacityProofCandidate>,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) outstanding_owner_bytes: u64,
    pub(in crate::runtime) lease: Duration,
}

#[derive(Debug, Clone, Copy)]
/// Exact clear-frontier whole-flow handoff. It changes persistent response
/// Service ownership; it never authorizes adjacent cross-family Subflow bytes.
pub(in crate::runtime) struct ResponseServiceHandoffRequest {
    pub(in crate::runtime) expected_planner_generation: u64,
    pub(in crate::runtime) expected_lane_generation: u64,
    pub(in crate::runtime) expected_model_generation: u64,
    pub(in crate::runtime) handoff_frontier: u64,
    pub(in crate::runtime) service: CarrierPathKey,
    pub(in crate::runtime) service_path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime) service_incarnation: u64,
    pub(in crate::runtime) target: CarrierPathKey,
    pub(in crate::runtime) target_path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime) target_incarnation: u64,
    pub(in crate::runtime) mode: ResponseServiceHandoffMode,
    /// Shared queue pressure may fall after ranking, but it may not grow beyond
    /// the byte-credit envelope that admitted this frame.
    pub(in crate::runtime) target_command_pending_limit_bytes: u64,
    pub(in crate::runtime) capacity_proof: Option<QuicCapacityProofCandidate>,
}

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

impl ResponseStreamBinding {
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
}

// Drop has exclusive aggregate access; lane and output cleanup needs no locks.
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

    pub(in crate::runtime::stream) fn new_with_limits_tracker_and_path_instance(
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: FlowLane,
        mux_limits: MuxLimits,
        lane_tracker: Arc<ServerPathLaneTracker>,
        path_instance_id: CarrierPathInstanceId,
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

    fn begin_close(&self) -> Vec<ResponseStreamOutputEntry> {
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
        outputs
    }

    fn finish_close(&self) {
        let mut lead = self
            .ordered_data_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *lead = None;
        self.response_flow_registration.set_service(None);
    }

    pub(in crate::runtime) async fn close_stream(&self, stream_id: StreamId) {
        let outputs = self.begin_close();
        for entry in outputs {
            let _ = entry
                .commands
                .send_control(ReliablePathCommand::CloseStream(stream_id))
                .await;
        }
        self.finish_close();
    }

    pub(in crate::runtime) async fn close_stream_ordered(
        &self,
        stream_id: StreamId,
        lane: FlowLane,
    ) {
        let outputs = self.begin_close();
        for entry in outputs {
            let _ = entry
                .commands
                .send_stream_ordered_close(stream_id, lane)
                .await;
        }
        self.finish_close();
    }

    /// Publishes terminal refusal and snapshots every affected output in one
    /// transaction, so an attachment cannot miss both the reset and closure.
    pub(in crate::runtime) async fn reset_and_close_stream_ordered(
        &self,
        stream_id: StreamId,
        reason: ResetReason,
        lane: FlowLane,
    ) {
        let outputs = self.begin_close();
        futures::future::join_all(outputs.into_iter().map(|entry| async move {
            let _ = entry
                .commands
                .send_stream_ordered_reset_and_close(stream_id, reason, lane)
                .await;
        }))
        .await;
        self.finish_close();
    }

    pub(super) fn notify_update(&self) {
        let current = *self.version.borrow();
        let _ = self.version.send(current.wrapping_add(1));
    }
}

#[cfg(test)]
#[path = "response_test.rs"]
mod tests;

#[cfg(test)]
mod test_support;

// TCP response capacity policy currently crosses admission, ACK application,
// and snapshot projection; keep its integration tests explicit until one owner exists.
#[cfg(test)]
mod tcp_capacity_policy_test;
