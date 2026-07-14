//! QUIC carrier runtime over UDP paths.
//!
//! QUIC packet-ACK evidence stays native to this carrier. Sender policy only
//! consumes typed path evidence and never treats it as TCP socket telemetry.

use super::*;

pub(in crate::runtime) mod carrier;
pub(in crate::runtime) mod metrics;

use metrics::*;
