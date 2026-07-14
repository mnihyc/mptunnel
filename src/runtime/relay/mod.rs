//! Product relay orchestration.
//!
//! Relay modules bridge local product I/O and sender/stream owners. They may
//! coordinate tasks, but carrier policy and exact-flight state live elsewhere.

use super::*;

pub(super) mod control;
mod diagnostics;
pub(super) mod flow;
pub(super) mod io;
pub(super) mod open;

pub(super) use control::*;
pub(super) use diagnostics::*;
pub(super) use flow::*;
pub(super) use io::*;
pub(super) use open::*;
