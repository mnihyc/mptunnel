//! Pure product-layer models.
//!
//! Modules here transform typed snapshots into decisions. They do not own
//! sockets, tasks, channels, timers, or platform APIs; runtime services gather
//! inputs and apply their outputs.

pub(in crate::runtime) mod ack_clock;
pub(in crate::runtime) mod admission;
pub(in crate::runtime) mod capacity;
pub(in crate::runtime) mod multipath;
mod response_ownership;
pub(in crate::runtime) mod work;

pub(super) use response_ownership::{
    ResponseCandidateTailDebt, ResponseOrderedTail, ResponseSameFamilyReservoir,
};
