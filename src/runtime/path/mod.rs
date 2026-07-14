//! Carrier-path runtime ownership.
//!
//! This layer owns path commands, observations, proof state, and carrier
//! lifecycle. Product offset ownership remains in `stream`; policy remains in
//! `model` and `sender`.

use super::*;

pub(super) mod commands;
pub(super) mod common;
pub(super) mod model;
pub(super) mod proof;
pub(in crate::runtime) mod tcp;
pub(in crate::runtime) mod udp;

pub(super) use commands::*;
pub(super) use common::*;
pub(super) use model::*;
pub(super) use proof::*;
