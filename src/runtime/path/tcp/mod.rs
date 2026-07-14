//! TCP carrier runtime.
//!
//! Client and server loops own TCP framing and lifecycle independently. Native
//! telemetry is optional transport evidence, not a prerequisite for TCP path
//! policy or receiver receipts.

use super::*;

pub(in crate::runtime) mod capacity;
pub(in crate::runtime) mod client;
mod client_capacity;
pub(in crate::runtime) mod client_connection;
mod client_receive;
mod client_session;
mod client_state;
mod client_stream;
mod client_writer;
pub(in crate::runtime) mod io;
pub(in crate::runtime) mod metrics;
pub(in crate::runtime) mod server;

use capacity::*;
use metrics::*;

#[cfg(test)]
pub(in crate::runtime) use client_session::connect_client_tcp_path_for_test;
