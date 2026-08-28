//! Carrier-path runtime ownership.
//!
//! This layer owns path commands, observations, proof state, and carrier
//! lifecycle. Product offset ownership remains in `stream`; policy remains in
//! `model` and `sender`.

#[cfg(test)]
use super::*;

pub(in crate::runtime) mod authentication;
mod carrier_inventory;
mod client_session;
pub(super) mod commands;
mod health;
pub(super) mod model;
mod ports;
pub(super) mod proof;
mod queue;
pub(in crate::runtime) mod quic;
mod selection;
mod server_context;
mod set;
mod state;
pub(in crate::runtime) mod tcp;

pub(in crate::runtime) use carrier_inventory::{
    AuthenticatedCarrierAvailability, AuthenticatedCarrierInventory,
    AuthenticatedCarrierRegistration,
};
pub(in crate::runtime) use commands::{CapacityProbeCommandTicket, RequestTcpCapacityProbeRequest};
pub(in crate::runtime) use health::{
    ClientPathHealth, ClientPathHealthRecord, ClientPathRateDiagnostics,
    RequestCapacityReconciliationView,
};
pub(in crate::runtime) use model::{
    PacketPathAttachment, PacketPathSelectionInput, PathDeliveryStats, UdpPathCandidate,
};
pub(in crate::runtime) use ports::{
    AcceptedServerDatagramFlow, CarrierDeliveryRateSample, OpenedReliableCarrierStream,
    ServerCarrierPathIdentity, ServerCarrierPathRegistration, ServerCarrierPathRetirement,
    ServerCarrierPathStatusSnapshot, ServerCarrierPeer, ServerDatagramOpenError,
    ServerDatagramOpenFailure, ServerDatagramOpenRequest, ServerDatagramPort,
    ServerDatagramPortBackend, ServerDatagramRequest, ServerDatagramSendOutcome,
    ServerDatagramTombstone, ServerDatagramTombstoneCache, ServerDatagramWorkerMessage,
    ServerLocalPathProperties, ServerMppIngress, ServerMppIngressObserver, ServerNewStreamPolicy,
    ServerPathValidation, ServerRealtimeFlowLease, ServerSessionManagementSnapshot,
    ServerSessionRetirement, ServerStreamFrameRoute, ServerStreamManagementSnapshot,
    ServerStreamOpenOutcome, ServerStreamOpenRequest, ServerStreamPathAttachment, ServerStreamPort,
    ServerStreamPortBackend, ServerTargetAdmission, fence_server_carrier_readiness,
};
#[cfg(test)]
pub(super) use proof::*;
pub(in crate::runtime) use selection::ReliableRequestTcpPathEvidence;
pub(in crate::runtime) use server_context::{
    CredentialRetirementControl, ServerLocalPath, ServerPathContext,
};
pub(in crate::runtime) use set::{ClientPathContext, ClientPathRuntimeOptions};
pub(in crate::runtime) use state::{
    ClientPathState, ClientSessionProductFlowLease, RelayPathLoadLease,
    RequestCapacityProbeCampaignBudget,
};
pub(in crate::runtime) use tcp::capacity::{
    RequestTcpCapacityProbeLease, RequestTcpCapacityProofQuery,
};
