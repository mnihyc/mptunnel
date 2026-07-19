//! TCP carrier runtime.
//!
//! Client and server loops own TCP framing and lifecycle independently. Native
//! telemetry is optional transport evidence, not a prerequisite for TCP path
//! policy or receiver receipts.

pub(in crate::runtime) mod capacity;
pub(in crate::runtime) mod client;
mod client_capacity;
pub(in crate::runtime) mod client_connection;
mod client_datagram;
mod client_receive;
mod client_session;
mod client_state;
mod client_stream;
mod client_writer;
pub(in crate::runtime) mod io;
pub(in crate::runtime) mod metrics;
pub(in crate::runtime) mod server;
mod server_datagram;
mod server_evidence;
mod server_session;
mod server_stream;
mod server_writer;
