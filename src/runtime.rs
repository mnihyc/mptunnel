//! Runtime composition for product flows and concrete carrier actors.
//!
//! This layer owns tasks, queues, channels, and mutation; pure evidence and
//! admission formulas remain in `model` and `scheduler`.

mod config_control;
mod datagram;
mod error;
mod gateway;
mod identity;
mod ingress_runtime;
mod management;
mod node;
mod outbound_registry;
mod path;
mod peer_status;
mod product_policy;
mod readiness;
mod recent_ids;
mod relay;
mod sender;
mod stream;
mod telemetry;
mod tun_l4;

pub(crate) use config_control::RuntimeConfigControl;
pub use datagram::{client_udp_datagram_round_trip, client_udp_datagram_round_trip_with_provider};
pub use error::RuntimeError;
pub(crate) use node::{
    RuntimeGenerationOutcome, run_with_all_host_providers_and_config_control,
    run_with_all_host_providers_and_generation_control, run_with_config_control,
    run_with_generation_control,
};
pub use node::{
    run, run_with_all_host_providers, run_with_host_providers, run_with_packet_device_provider,
    run_with_vpn_host_providers,
};
pub(crate) use readiness::RuntimeGenerationControl;
#[cfg(test)]
pub(crate) use readiness::RuntimeGenerationStopReason;

// Runtime integration suites intentionally share the composition namespace.
// Production modules never inherit these test-only conveniences.
#[cfg(test)]
use crate::config::{ClientSecurityConfig, ManagementConfig, ServerSecurityConfig};
#[cfg(test)]
use crate::ingress::http_connect::{self, HttpStatus};
#[cfg(test)]
use crate::ingress::socks5::{self, Socks5Reply};
#[cfg(test)]
use crate::ingress::tun::TunL4Config;
#[cfg(test)]
use crate::mux::MuxLimits;
#[cfg(test)]
use crate::mux::stream::{ReliableRecvStream, ReliableSendStream};
#[cfg(test)]
use crate::performance::MppPerformanceConfig;
#[cfg(test)]
use crate::protocol::codec::CodecLimits;
#[cfg(test)]
use crate::protocol::{
    DatagramFlowId, Frame, OffsetRange, PathId, PathMetricDirection, PathMetrics, SessionId,
    StreamId, TargetAddr, UnderlayProtocol,
};
#[cfg(test)]
use crate::scheduler::{PathSnapshot, PathState as SchedulerPathState, TrafficClass};
#[cfg(test)]
use crate::transport::encrypted::EncryptedFramedStream;
#[cfg(test)]
use crate::transport::tcp::{self, TcpConnectOptions};
#[cfg(test)]
use crate::transport::{PathSpec, RateHint};
#[cfg(test)]
use bytes::Bytes;
#[cfg(test)]
use std::collections::HashSet;
#[cfg(test)]
use std::hash::Hash;
#[cfg(test)]
use std::net::SocketAddr;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::time::{Duration, Instant};
#[cfg(test)]
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
#[cfg(test)]
use tokio::net::{TcpListener, TcpStream, UdpSocket};
#[cfg(test)]
use tokio::sync::{mpsc, oneshot};

#[cfg(test)]
use crate::model::capacity::*;
#[cfg(test)]
use crate::model::path::*;
#[cfg(test)]
use crate::model::timing::*;
#[cfg(test)]
use crate::model::work::*;
#[cfg(test)]
use crate::protocol::DatagramId;
#[cfg(test)]
use datagram::*;
#[cfg(test)]
use identity::*;
#[cfg(test)]
use ingress_runtime::*;
#[cfg(test)]
use node::probe_paths as probe_client_paths;
#[cfg(test)]
use node::server::run as run_server;
#[cfg(test)]
use path::tcp::client::*;
#[cfg(test)]
use path::tcp::client_connection::*;
#[cfg(test)]
use path::tcp::server::*;
#[cfg(test)]
use path::{ClientPathContext, ClientPathHealthRecord, PathDeliveryStats, commands::*, model::*};
#[cfg(test)]
use relay::*;
#[cfg(test)]
use sender::*;
#[cfg(test)]
use stream::*;
#[cfg(test)]
use tun_l4::*;

#[cfg(test)]
#[path = "runtime_test.rs"]
mod tests;
