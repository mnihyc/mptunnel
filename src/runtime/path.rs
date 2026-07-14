//! Carrier-path runtime ownership.
//!
//! This layer owns path commands, observations, proof state, and carrier
//! lifecycle. Product offset ownership remains in `stream`; policy remains in
//! `model` and `sender`.

use super::*;

pub(in crate::runtime) mod authentication;
pub(super) mod commands;
pub(super) mod model;
pub(super) mod proof;
pub(in crate::runtime) mod quic;
mod selection;
mod server_context;
mod set;
mod state;
pub(in crate::runtime) mod tcp;

pub(super) use commands::*;
pub(super) use model::*;
pub(super) use proof::*;
pub(in crate::runtime) use server_context::*;
pub(in crate::runtime) use set::*;
pub(in crate::runtime) use state::*;
