//! QUIC carrier runtime over UDP paths.
//!
//! QUIC packet-ACK evidence stays native to this carrier. Sender policy only
//! consumes typed path evidence and never treats it as TCP socket telemetry.

pub(in crate::runtime) mod client;
mod client_stream;
mod client_writer;
pub(in crate::runtime) mod datagram;
mod estimator;
pub(in crate::runtime) mod io;
pub(in crate::runtime) mod ip_tunnel;
pub(in crate::runtime) mod metrics;
pub(in crate::runtime) mod server;
mod server_stream;
mod server_writer;

#[cfg(test)]
#[path = "quic/tests_estimator_test_support.rs"]
mod estimator_test_support;
