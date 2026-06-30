mod bulk_admission;
mod core;
mod datagram;
mod error;
mod ingress_runtime;
mod management;
mod path_common;
mod prelude;
mod relay_control;
mod relay_flow;
mod relay_io;
mod relay_open;
mod relay_striping;
mod sender_service;
mod server_runtime;
mod server_tcp;
mod tcp_path;
mod tcp_path_commands;
mod tun_l4;
mod udp_path;

pub use core::run;
pub use datagram::client_udp_datagram_round_trip;
pub use error::RuntimeError;

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::*;
use core::*;
use datagram::*;
use ingress_runtime::*;
use management::*;
use path_common::*;
use prelude::*;
use relay_control::*;
use relay_flow::*;
use relay_io::*;
use relay_open::*;
use relay_striping::*;
use sender_service::*;
#[cfg(test)]
use server_runtime::run_server;
use server_tcp::*;
use tcp_path::*;
use tcp_path_commands::*;
use tun_l4::*;
use udp_path::*;

#[cfg(test)]
mod tests;
