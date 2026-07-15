//! Carrier-path runtime ownership.
//!
//! This layer owns path commands, observations, proof state, and carrier
//! lifecycle. Product offset ownership remains in `stream`; policy remains in
//! `model` and `sender`.

#[cfg(test)]
use super::*;

pub(in crate::runtime) mod authentication;
pub(super) mod commands;
pub(super) mod model;
mod ports;
pub(super) mod proof;
pub(in crate::runtime) mod quic;
mod selection;
mod server_context;
mod set;
mod state;
pub(in crate::runtime) mod tcp;

pub(in crate::runtime) use commands::{
    CapacityProbeCommandTicket, QuicCapacityProbeCommand, QuicCapacityProbeOwner,
    RequestTcpCapacityProbeRequest,
};
pub(in crate::runtime) use model::PathDeliveryStats;
pub(in crate::runtime) use ports::{
    CarrierCommandLease, OpenedReliableCarrierStream, UdpStreamOpenOptions,
};
#[cfg(test)]
pub(super) use proof::*;
pub(in crate::runtime) use quic::{
    RequestQuicCapacityProbeLease, RequestQuicCapacityProductHandoffState,
    RequestQuicCapacityReconciliationQuery,
};
pub(in crate::runtime) use server_context::ServerPathContext;
pub(in crate::runtime) use set::ClientPathContext;
pub(in crate::runtime) use state::{
    ClientPathHealth, ClientPathHealthRecord, ClientPathState, RelayPathLoadLease,
    ReliableTcpRequestBulkFlowRegistration, RequestCapacityProbeCampaignBudget,
    RequestCapacityReconciliationView,
};
pub(in crate::runtime) use tcp::capacity::{
    RequestTcpCapacityProbeLease, RequestTcpCapacityProofQuery,
};
