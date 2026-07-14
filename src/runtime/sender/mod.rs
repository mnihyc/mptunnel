//! Product sender ownership.
//!
//! Request and response senders share bounded work queues and carrier-neutral
//! model vocabulary. Each direction owns its own state machine and dispatch
//! transaction; neither TCP nor QUIC owns product offsets.

use super::*;

mod dispatch;
mod queue;
mod request;
mod response;
mod work;

pub(super) use queue::*;
pub(super) use request::*;
pub(super) use response::*;
pub(super) use work::*;
