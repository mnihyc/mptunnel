//! Product sender ownership.
//!
//! Request and response senders share bounded work queues and carrier-neutral
//! model vocabulary. Each direction owns its own state machine and dispatch
//! transaction; neither TCP nor QUIC owns product offsets.

mod queue;
mod request;
mod response;
mod work;

#[cfg(not(test))]
pub(in crate::runtime) use queue::{
    ReliableRelayQueuedWork, ReliableRelayQueuedWorkKind, ReliableRelaySenderQueue,
    reliable_relay_can_read_product_source, reliable_relay_sender_queue_limit,
    reliable_relay_sender_queue_read_budget,
};
#[cfg(not(test))]
pub(in crate::runtime) use request::{
    ClientQueuedDispatch, RelayRecvProgressSend, RequestOrdinarySaturationObservation,
    RequestProductAckOriginalResolution, RequestProductAckReceipt, RequestProductAckReceiptSink,
    RequestProductAckReceiptTarget, RequestSenderService,
};
#[cfg(not(test))]
pub(in crate::runtime) use response::{
    ResponseOrdinarySaturationObservation, ServerCarrierReadiness, ServerQueuedDispatch,
    ServerResponseSenderService,
};
#[cfg(not(test))]
pub(in crate::runtime) use work::{
    CarrierEmitMode, ProductWorkloadIdentity, RelaySendCause, ServerReinjectionOutputIdentity,
    sender_extra_traffic_startup_floor_bytes, sender_reinjection_minimum_useful_attempt_bytes,
};

#[cfg(test)]
pub(super) use queue::*;
#[cfg(test)]
pub(super) use request::*;
#[cfg(test)]
pub(super) use response::*;
#[cfg(test)]
pub(super) use work::*;
