mod datagram;
mod error;
mod identity;
mod ingress_runtime;
mod management;
mod node;
mod packet_device;
mod path;
mod recent_ids;
mod relay;
mod relay_striping;
mod sender;
mod stream;
mod tun_l4;

pub use datagram::{client_udp_datagram_round_trip, client_udp_datagram_round_trip_with_provider};
pub use error::RuntimeError;
pub use node::{run, run_with_host_providers, run_with_packet_device_provider};
pub use packet_device::{PacketDevice, PacketDeviceProvider, SystemPacketDeviceProvider};

// Runtime integration suites intentionally share the composition namespace.
// Production modules never inherit these test-only conveniences.
#[cfg(test)]
use crate::config::{
    ManagementConfig, MppPerformanceConfig, RouteTarget, RouteTargetKind, SecurityConfig,
};
#[cfg(test)]
use crate::ingress::http_connect::{self, HttpConnectError, HttpStatus};
#[cfg(test)]
use crate::ingress::socks5::{self, Socks5Error, Socks5Reply};
#[cfg(test)]
use crate::ingress::tun::TunL4Config;
#[cfg(test)]
use crate::ingress::{IngressConfig, ProxyAuthConfig};
#[cfg(test)]
use crate::mux::MuxLimits;
#[cfg(test)]
use crate::mux::stream::{ReliableRecvStream, ReliableSendStream};
#[cfg(test)]
use crate::outbound::{self, TargetProtocol};
#[cfg(test)]
use crate::protocol::auth::{PathJoinAuthCheck, SessionAuthCheck, SessionAuthenticator};
#[cfg(test)]
use crate::protocol::codec::CodecLimits;
#[cfg(test)]
use crate::protocol::{
    CloseReason, DatagramFlowId, Frame, IngressKind, OffsetRange, PathCapabilities, PathId,
    PathMetricDirection, PathMetrics, RateHint, ResetReason, SessionId, StreamFlags, StreamId,
    StreamOpenRole, TargetAddr, UnderlayProtocol,
};
#[cfg(test)]
use crate::scheduler::{
    self, FlowDemand, FlowLane, PathRateScope, PathSnapshot, PathState as SchedulerPathState,
    SchedulerPolicy,
};
#[cfg(test)]
use crate::transport::PathSpec;
#[cfg(test)]
use crate::transport::encrypted::{EncryptedFramedStream, EncryptedFramedTransportError, PeerRole};
#[cfg(test)]
use crate::transport::quic as quic_transport;
#[cfg(test)]
use crate::transport::tcp::{self, TcpConnectOptions};
#[cfg(test)]
use bytes::{Bytes, BytesMut};
#[cfg(test)]
use futures::{SinkExt, StreamExt};
#[cfg(test)]
use netstack_smoltcp::{StackBuilder, TcpListener as TunTcpListener, UdpSocket as TunUdpSocket};
#[cfg(test)]
use std::collections::{HashMap, HashSet, VecDeque};
#[cfg(test)]
use std::hash::Hash;
#[cfg(test)]
use std::net::SocketAddr;
#[cfg(test)]
use std::sync::{Arc, Mutex};
#[cfg(test)]
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
#[cfg(test)]
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
#[cfg(test)]
use tokio::net::{TcpListener, TcpStream, UdpSocket};
#[cfg(test)]
use tokio::sync::{Notify, mpsc, oneshot};
#[cfg(test)]
use tun_rs::async_framed::{BytesCodec, DeviceFramed};

#[cfg(all(test, feature = "lab-diagnostics"))]
use crate::lab_diagnostics::*;
#[cfg(test)]
use crate::model::capacity::*;
#[cfg(test)]
use crate::model::multipath::*;
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
use path::quic::{client::*, io::*};
#[cfg(test)]
use path::tcp::client::*;
#[cfg(test)]
use path::tcp::client_connection::*;
#[cfg(test)]
use path::tcp::server::*;
#[cfg(test)]
use path::{
    ClientPathContext, ClientPathHealthRecord, PathDeliveryStats, RelayPathLoadLease,
    ReliableTcpRequestBulkFlowRegistration, ServerPathContext, commands::*, model::*,
};
#[cfg(test)]
use path::{
    ClientPathHealth, ClientPathState, RequestCapacityProbeCampaignBudget,
    RequestQuicCapacityProbeLease, RequestQuicCapacityProductHandoffState,
    RequestTcpCapacityProbeLease, UdpDatagramPathObservation,
};
#[cfg(test)]
use relay::*;
#[cfg(test)]
use relay_striping::*;
#[cfg(test)]
use sender::*;
#[cfg(test)]
use stream::*;
#[cfg(test)]
use tun_l4::*;

#[cfg(test)]
#[path = "runtime_test.rs"]
mod tests;
