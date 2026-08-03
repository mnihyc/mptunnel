//! Versioned MPP wire vocabulary and bounded codecs.
//!
//! Only peer-owned facts belong here; ingress, outbound, and configured path
//! policy stay with their local endpoint owners.

pub mod auth;
pub mod codec;
pub(crate) mod frame;
pub(crate) mod path_capacity;
mod types;

pub use types::{
    AuthNonce, AuthTag, CloseReason, DatagramFlowId, DatagramId, Frame, OffsetRange, PathId,
    PathMetricDirection, PathMetrics, PathUsage, PeerPathState, PeerPathStatus, PeerStatusCode,
    ResetReason, SessionId, StreamDemandHint, StreamId, TargetAddr, UnderlayProtocol,
};
