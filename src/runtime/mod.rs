mod bulk_admission;
mod core;
mod datagram;
mod error;
mod ingress_runtime;
mod management;
mod multipath_model;
mod path_commands;
mod path_common;
mod path_model;
mod path_proof;
mod prelude;
mod relay_control;
mod relay_flow;
mod relay_io;
mod relay_open;
mod relay_striping;
mod reliable_path;
mod sender_service;
mod server_runtime;
mod server_tcp;
mod tcp_path;
mod tun_l4;
mod udp_metrics;
mod udp_path;

pub(super) const RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES: u64 = 512 * 1024;
pub(super) const RELIABLE_UDP_MIN_PRODUCT_WINDOW_BYTES: u64 = 512 * 1024;
pub(super) const RELIABLE_UDP_BULK_BDP_GAIN: f64 = 4.0;

pub use core::run;
pub use datagram::client_udp_datagram_round_trip;
pub use error::RuntimeError;

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::*;
use core::*;
use datagram::*;
use ingress_runtime::*;
use management::*;
use multipath_model::*;
use path_commands::*;
use path_common::*;
use path_model::*;
use path_proof::*;
use prelude::*;
use relay_control::*;
use relay_flow::*;
use relay_io::*;
use relay_open::*;
use relay_striping::*;
use reliable_path::*;
use sender_service::*;
#[cfg(test)]
use server_runtime::run_server;
use server_tcp::*;
use tcp_path::*;
use tun_l4::*;
use udp_metrics::*;
use udp_path::*;

#[cfg(test)]
mod tests;
