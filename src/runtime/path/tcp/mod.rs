//! TCP carrier runtime.
//!
//! Client and server loops own TCP framing and lifecycle independently. Native
//! telemetry is optional transport evidence, not a prerequisite for TCP path
//! policy or receiver receipts.

use super::*;

pub(in crate::runtime) mod capacity;
pub(in crate::runtime) mod client;
pub(in crate::runtime) mod metrics;
pub(in crate::runtime) mod server;

use capacity::*;
use metrics::*;
