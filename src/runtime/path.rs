//! Carrier-path runtime ownership.
//!
//! This layer owns path commands, observations, proof state, and carrier
//! lifecycle. Product offset ownership remains in `stream`; policy remains in
//! `model` and `sender`.

#[cfg(test)]
use super::*;

pub(in crate::runtime) mod authentication;
pub(in crate::runtime) mod authority;
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
#[cfg(test)]
pub(in crate::runtime) use commands::{CapacityProbeCommandTicket, RequestTcpCapacityProbeRequest};
#[cfg(test)]
pub(in crate::runtime) use health::RequestCapacityReconciliationView;
pub(in crate::runtime) use health::{
    ClientPathHealth, ClientPathHealthRecord, ClientPathRateDiagnostics,
};
pub(in crate::runtime) use model::{
    PacketPathAttachment, PacketPathSelectionInput, PathDeliveryStats, UdpPathCandidate,
};
pub(in crate::runtime) use ports::{
    AcceptedServerDatagramFlow, CarrierDeliveryRateSample, CarrierNativeWindowSample,
    OpenedReliableCarrierStream, ServerCarrierPathApplyAuthority, ServerCarrierPathIdentity,
    ServerCarrierPathRegistration, ServerCarrierPathRetirement, ServerCarrierPathStateHandle,
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
pub(in crate::runtime) use selection::{
    ReliableRequestNativeShape, ReliableRequestTcpPathEvidence,
};
pub(in crate::runtime) use server_context::{
    CredentialRetirementControl, ServerLocalPath, ServerPathContext,
};
pub(in crate::runtime) use set::{ClientPathContext, ClientPathRuntimeOptions};
#[cfg(test)]
pub(in crate::runtime) use state::RequestCapacityProbeCampaignBudget;
pub(in crate::runtime) use state::{
    ClientPathState, ClientSessionProductFlowLease, RelayPathLoadLease,
};
#[cfg(test)]
pub(in crate::runtime) use tcp::capacity::{
    RequestTcpCapacityProbeLease, RequestTcpCapacityProofQuery,
};
