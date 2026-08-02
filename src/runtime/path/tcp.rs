//! TCP carrier runtime.
//!
//! Client and server loops own TCP framing and lifecycle independently. Native
//! telemetry is optional transport evidence, not a prerequisite for TCP path
//! policy or receiver receipts.

pub(in crate::runtime) mod admission;
pub(in crate::runtime) mod capacity;
pub(in crate::runtime) mod client;
pub(in crate::runtime) mod group;
pub(in crate::runtime) mod io;
pub(in crate::runtime) mod metrics;
pub(in crate::runtime) mod retained;
pub(in crate::runtime) mod server;
pub(in crate::runtime) mod service;
