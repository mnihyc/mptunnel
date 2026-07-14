//! Server response-stream ownership and its narrow runtime contract.
//!
//! The binding schema stays here so child transition owners share private
//! invariants without widening its locks or state fields.

mod ack_clock;
mod admission;
mod delivery;
mod diagnostics;
mod evidence;
mod handoff;
mod handoff_commit;
mod lifecycle;
mod load;
mod placement;
mod quic_capacity;
mod quic_probe;
mod session;
mod snapshot;
mod tcp_capacity;
mod topology;
mod transaction;

pub(in crate::runtime) use admission::ResponseSubflowAdmissionRequest;
use admission::ResponseSubflowSetState;
pub(in crate::runtime) use delivery::CarrierPathFlightDebt;
use delivery::ResponseAckOrderingState;
pub(super) use delivery::{
    CarrierPathFlight, product_flights_have_recent_repair_overlap,
    release_carrier_path_flight_ranges,
};
pub(in crate::runtime) use diagnostics::record_server_sender_decision;
#[cfg(feature = "lab-diagnostics")]
use diagnostics::{ResponseServiceFeedDiagnosticState, ResponseServiceHandoffDiagnosticState};
pub(in crate::runtime) use evidence::{ServerPathMetricsEntry, ServerPathMetricsSource};
pub(in crate::runtime) use handoff::{
    ResponseServiceHandoffDrainRequest, ResponseServiceHandoffDrainReservation,
};
pub(in crate::runtime) use handoff_commit::ResponseServiceHandoffRequest;
pub(in crate::runtime) use load::ServerRealtimeFlowRegistration;
pub(in crate::runtime) use placement::{
    ResponseServiceHandoffMode, response_rate_fair_share_bps, response_service_handoff_mode,
};
#[cfg(any(test, feature = "lab-diagnostics"))]
pub(in crate::runtime) use quic_capacity::well_formed_quic_capacity_proof_candidate;
pub(in crate::runtime) use quic_capacity::{
    quic_capacity_proof_pin_matches_marker, quic_capacity_receipt_rate_bps,
    valid_quic_capacity_proof_candidate_at,
};
pub(in crate::runtime) use quic_probe::ResponseQuicCapacityCalibrationRequest;
pub(in crate::runtime) use session::{ResponseServiceFamilyLoads, ServerPathLaneTracker};
pub(in crate::runtime) use snapshot::server_bulk_output_eta_ms;
pub(in crate::runtime) use tcp_capacity::TcpCapacityProbeSessionLease;
use topology::ResponseStreamOutputs;
pub(in crate::runtime) use topology::{
    ResponseDispatchTarget, ResponseSenderPathTarget, ResponseStreamAttachOutcome,
    ServerCarrierPathInstanceId, next_server_carrier_path_instance_id,
};
pub(in crate::runtime) use transaction::{
    ResponseAckClockCalibrationRequest, ResponseAckClockCalibrationRetirementRequest,
};

use self::load::ServerResponseFlowRegistration;
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::SessionId;
use crate::scheduler::FlowLane;
use std::collections::BTreeMap;
#[cfg(feature = "lab-diagnostics")]
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64};
use std::sync::{Arc, Mutex};
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
mod test_support;

// TCP response capacity policy currently crosses admission, ACK application,
// and snapshot projection; keep its integration tests explicit until one owner exists.
#[cfg(test)]
mod tcp_capacity_policy_test;
