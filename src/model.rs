//! Pure product-layer models.
//!
//! Modules here transform typed snapshots into decisions. They do not own
//! sockets, tasks, channels, timers, or platform APIs; runtime services gather
//! inputs and apply their outputs.

pub(crate) mod ack_clock;
pub(crate) mod admission;
pub(crate) mod capacity;
pub(crate) mod multipath;
pub(crate) mod path;
pub(crate) mod request;
pub(crate) mod response;
pub(crate) mod timing;
pub(crate) mod work;
