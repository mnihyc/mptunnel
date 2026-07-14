//! Reliable product-stream ownership.
//!
//! Stream bindings own product offsets, exact carrier flights, attachment
//! generations, and atomic commit. Sender modules rank snapshots and submit
//! intents; carrier paths never own product byte ranges.

use super::*;

pub(in crate::runtime) mod binding;
mod demand;
mod registry;
mod response_placement;
mod server;

pub(in crate::runtime) use binding::*;
pub(in crate::runtime) use demand::{
    flow_lane_from_stream_demand_hint, stream_demand_hint_for_lane,
};
pub(in crate::runtime) use registry::*;
pub(in crate::runtime) use response_placement::*;
pub(in crate::runtime) use server::{ServerStreamContext, run_server_reliable_stream};
