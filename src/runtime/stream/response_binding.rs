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
#[path = "response_lifecycle.rs"]
mod response_lifecycle;
#[path = "response_load.rs"]
mod response_load;
#[path = "response_quic_capacity.rs"]
mod response_quic_capacity;
#[path = "response_session.rs"]
pub(super) mod response_session;
#[path = "response_snapshot.rs"]
mod response_snapshot;
#[path = "response_tcp_capacity.rs"]
mod response_tcp_capacity;
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
#[cfg(feature = "lab-diagnostics")]
use response_diagnostics::{
    ResponseServiceFeedDiagnosticState, ResponseServiceHandoffDiagnosticState,
};
pub(in crate::runtime) use response_evidence::{ServerPathMetricsEntry, ServerPathMetricsSource};
pub(in crate::runtime) use response_handoff::{
    ResponseServiceHandoffDrainRequest, ResponseServiceHandoffRequest,
};
pub(in crate::runtime) use response_load::ServerRealtimeFlowRegistration;
pub(in crate::runtime) use response_quic_capacity::ResponseQuicCapacityCalibrationRequest;
#[cfg(test)]
pub(in crate::runtime) use response_session::ResponseSessionSchedulingSnapshot;
#[cfg(any(test, feature = "lab-diagnostics"))]
pub(in crate::runtime) use response_session::well_formed_quic_capacity_proof_candidate;
pub(in crate::runtime) use response_session::{
    ResponseServiceFamilyLoads, ResponseServiceHandoffDrainReservation, ServerPathLaneTracker,
    quic_capacity_proof_pin_matches_marker, quic_capacity_receipt_rate_bps,
    valid_quic_capacity_proof_candidate_at,
};
pub(in crate::runtime) use response_snapshot::server_bulk_output_eta_ms;
#[cfg(test)]
pub(in crate::runtime) use response_snapshot::{
    ResponseRelayReadSnapshot, ResponseSourceServiceSnapshot,
};
pub(in crate::runtime) use response_tcp_capacity::TcpCapacityProbeSessionLease;
pub(in crate::runtime) use response_topology::{
    ResponseDispatchTarget, ResponseSenderPathTarget, ResponseStreamAttachOutcome,
    ResponseStreamOutputs, ServerCarrierPathInstanceId, next_server_carrier_path_instance_id,
};
#[cfg(test)]
pub(in crate::runtime) use response_topology::{
    ResponseStreamOutputEntry, TcpResponseCapacityPrior,
};
pub(in crate::runtime) use response_transaction::{
    ResponseAckClockCalibrationRequest, ResponseAckClockCalibrationRetirementRequest,
};

#[cfg(test)]
use self::response_evidence::server_output_quic_capacity_proof_marker;
use self::response_load::ServerResponseFlowRegistration;
#[cfg(test)]
use self::response_session::ServerQuicCapacityCalibrationPhase;
#[cfg(test)]
use self::response_snapshot::server_bulk_output_snapshot;
#[cfg(test)]
use crate::model::ack_clock::{
    reliable_ack_clock_calibration_ceiling_bytes, reliable_ack_clock_calibration_limit_bytes,
};
#[cfg(test)]
use crate::model::capacity::PathRateSample;
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
use crate::protocol::SessionId;
#[cfg(test)]
use crate::protocol::{PathId, StreamId, StreamOpenRole, UnderlayProtocol};
#[cfg(test)]
use crate::runtime::RuntimeError;
#[cfg(test)]
use crate::runtime::path::commands::ReliablePathCommand;
use crate::scheduler::FlowLane;
#[cfg(test)]
use crate::scheduler::PathSnapshot;
use std::collections::BTreeMap;
#[cfg(feature = "lab-diagnostics")]
use std::collections::HashMap;
#[cfg(test)]
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64};
use std::sync::{Arc, Mutex};
#[cfg(test)]
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

#[cfg(test)]
#[path = "response_binding_test.rs"]
mod tests;
