//! Carrier-path runtime ownership.
//!
//! This layer owns path commands, observations, proof state, and carrier
//! lifecycle. Product offset ownership remains in `stream`; policy remains in
//! `model` and `sender`.

#[cfg(test)]
use super::*;

pub(in crate::runtime) mod authentication;
mod carrier_inventory;
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
    ClientPathHealth, ClientPathHealthRecord, RequestCapacityReconciliationView,
};
pub(in crate::runtime) use model::{PathDeliveryStats, UdpPathCandidate};
pub(in crate::runtime) use ports::{
    AcceptedServerDatagramFlow, CarrierDeliveryRateSample, OpenedReliableCarrierStream,
    ServerCarrierPathIdentity, ServerCarrierPathRegistration, ServerCarrierPathRetirement,
    ServerCarrierPathStatusSnapshot, ServerDatagramOpenError, ServerDatagramOpenRequest,
    ServerDatagramPort, ServerDatagramPortBackend, ServerDatagramRequest,
    ServerDatagramSendOutcome, ServerDatagramWorkerMessage, ServerLocalPathProperties,
    ServerNewStreamPolicy, ServerPathValidation, ServerRealtimeFlowLease,
    ServerSessionManagementSnapshot, ServerStreamFrameRoute, ServerStreamManagementSnapshot,
    ServerStreamOpenOutcome, ServerStreamOpenRequest, ServerStreamPathAttachment, ServerStreamPort,
    ServerStreamPortBackend,
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
