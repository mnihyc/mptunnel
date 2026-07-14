//! Reliable product-stream ownership.
//!
//! Stream bindings own product offsets, exact carrier flights, attachment
//! generations, and atomic commit. Sender modules rank snapshots and submit
//! intents; carrier paths never own product byte ranges.

mod demand;
mod handle;
mod registry;
mod response_binding;
mod response_placement;
mod server;

pub(in crate::runtime) use demand::{
    flow_lane_from_stream_demand_hint, stream_demand_hint_for_lane,
};
pub(in crate::runtime) use handle::{
    FixedReliablePathOutput, ReliablePathStream, ReliablePathStreamHandle,
    ReliablePathStreamOutput, reliable_work_lane_to_carrier_lane,
    wait_for_carrier_capacity_notifies,
};
pub(in crate::runtime) use registry::{
    ServerCarrierPathRegistration, ServerReliablePathAttachment,
    ServerReliableRegistryManagementSnapshot, ServerReliableStreamOpen,
    ServerReliableStreamOpenRequest, ServerReliableStreamRegistry,
};
#[cfg(any(test, feature = "lab-diagnostics"))]
pub(in crate::runtime) use response_binding::well_formed_quic_capacity_proof_candidate;
#[cfg(test)]
pub(in crate::runtime) use response_binding::{
    CarrierPathAckedHole, CarrierPathFlight, CarrierPathReleasedFlight, QuicCapacityProofCandidate,
    ResponseAckClockCalibrationState, ResponseAckClockRateEvidence,
    ResponseAckClockRateEvidenceUpdate, ResponseAckOrderingState, ResponseAckOrderingUpdate,
    ResponseRelayReadSnapshot, ResponseSessionSchedulingSnapshot, ResponseSourceServiceSnapshot,
    ResponseStreamAttachOutcome, ResponseStreamOutputEntry, ResponseStreamOutputs,
    ResponseSubflowAdmissionReservation, ServerPathLaneTracker, ServerPathMetricsEntry,
    ServerPathMetricsSource, TcpResponseCapacityPrior, next_server_carrier_path_instance_id,
    response_latest_ordering_hole, server_output_accepts_service_capacity_prior,
    server_output_has_bulk_rate_evidence, server_output_has_bulk_rate_evidence_with_limits,
    server_output_has_durable_product_progress, server_output_has_sender_evidence,
    server_output_has_service_feed_evidence_with_limits,
};
pub(in crate::runtime) use response_binding::{
    CarrierPathFlightDebt, MAX_RESPONSE_QUIC_CAPACITY_CALIBRATION_ATTEMPTS_PER_PATH,
    MIN_ACTIVE_RESPONSE_FLOWS_FOR_SAME_FAMILY_DISCOVERY, ResponseAckClockCalibrationRequest,
    ResponseAckClockCalibrationRetirementRequest, ResponseDispatchTarget,
    ResponseQuicCapacityCalibrationRequest, ResponseSenderPathTarget, ResponseServiceFamilyLoads,
    ResponseServiceHandoffDrainRequest, ResponseServiceHandoffDrainReservation,
    ResponseServiceHandoffRequest, ResponseStreamBinding, ResponseSubflowAdmissionRequest,
    ServerCarrierPathInstanceId, ServerRealtimeFlowRegistration, TcpCapacityProbeSessionLease,
    quic_capacity_proof_pin_matches_marker, quic_capacity_receipt_rate_bps,
    record_server_sender_decision, server_bulk_output_eta_ms,
    valid_quic_capacity_proof_candidate_at,
};
pub(in crate::runtime) use response_placement::{
    ResponseRateScope, ResponseServiceHandoffMode, response_rate_fair_share_bps,
    response_service_handoff_mode,
};
pub(in crate::runtime) use server::{ServerStreamContext, run_server_reliable_stream};
