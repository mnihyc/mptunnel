//! Canonical product-wire values.
//!
//! These types describe interoperable facts, not runtime handles, transport
//! congestion state, or endpoint-local configuration.

use bytes::Bytes;
use std::net::{IpAddr, SocketAddr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SessionId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PathId(pub u16);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StreamId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatagramFlowId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatagramId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IpTunnelId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IpPacketId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AuthNonce(pub [u8; 16]);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AuthTag(pub [u8; 32]);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum UnderlayProtocol {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathMetricDirection {
    ClientToServer,
    ServerToClient,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetAddr {
    Domain { host: String, port: u16 },
    Ip(SocketAddr),
}

impl TargetAddr {
    pub fn port(&self) -> u16 {
        match self {
            Self::Domain { port, .. } => *port,
            Self::Ip(addr) => addr.port(),
        }
    }

    pub fn authority(&self) -> String {
        match self {
            Self::Domain { host, port } => format!("{host}:{port}"),
            Self::Ip(addr) => addr.to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamDemandHint {
    Latency,
    Throughput,
    Realtime,
}

impl StreamDemandHint {
    pub const fn latency() -> Self {
        Self::Latency
    }

    pub const fn throughput() -> Self {
        Self::Throughput
    }

    pub const fn realtime() -> Self {
        Self::Realtime
    }
}

/// Receiver-to-sender scheduling preference for one path direction.
///
/// This follows the regular/backup semantics established by MPTCP MP_PRIO and
/// multipath QUIC PATH_STATUS. Path health and attachment proof are local
/// lifecycle concerns and are deliberately not encoded as usage values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathUsage {
    Available,
    Backup,
}

/// Largest remaining delivery-rate authority representable by protocol v8.
///
/// This is the three-PTO horizon at the maximum wire RTT and RTT variance:
/// `(u32::MAX + 4 * u32::MAX + 25_000us) * 3`. A receiver rejects larger
/// values so an untrusted peer cannot manufacture unbounded rate authority.
pub const PATH_METRICS_MAX_RATE_VALID_FOR_US: u64 = 64_424_584_425;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PathMetrics {
    pub path_id: PathId,
    pub underlay: UnderlayProtocol,
    pub direction: PathMetricDirection,
    pub metric_epoch: u64,
    pub metric_age_us: u32,
    /// Non-increasing remaining authority budget for the advertised delivery
    /// epoch. Forwarders subtract local residence; zero means the rate is
    /// diagnostic or a startup prior only.
    pub rate_valid_for_us: u64,
    /// Whether `delivery_rate_bps` belongs to a measured rate epoch.
    /// This remains true after expiry so diagnostics can distinguish stale
    /// measured evidence from an unmeasured startup prior.
    pub rate_observed: bool,
    pub srtt_us: u32,
    pub rttvar_us: u32,
    pub jitter_us: u32,
    pub delivery_rate_bps: u64,
    pub pacing_rate_bps: u64,
    /// Whether pacing belongs to the same qualified native delivery epoch.
    /// This is provenance, not freshness: expired samples retain it for stale
    /// diagnostics, while authority also requires a nonzero remaining budget.
    pub pacing_rate_observed: bool,
    pub loss_ppm: u32,
    pub ecn_ppm: u32,
    pub loss_observed: bool,
    pub ecn_observed: bool,
    /// Whether `bytes_in_flight` is an actual carrier observation.
    /// Numeric zero remains a valid observation when this is true.
    pub bytes_in_flight_observed: bool,
    /// Whether `queue_bytes` is an actual carrier observation.
    /// Numeric zero remains a valid observation when this is true.
    pub queue_observed: bool,
    pub bytes_in_flight: u64,
    pub queue_bytes: u64,
    pub inflight_limit_bytes: u64,
    pub inflight_hi_bytes: u64,
    pub confidence_ppm: u32,
    pub app_limited: bool,
    pub has_ack_derived_data_sample: bool,
    pub data_sample_count: u32,
    pub data_sample_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerStatusCode {
    Ok,
    Disabled,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerPathState {
    Active,
    Suspect,
    Draining,
    Failed,
}

/// One peer-observed path snapshot returned only on an explicit diagnostic request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerPathStatus {
    pub state: PeerPathState,
    pub usage: PathUsage,
    pub metrics: PathMetrics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetRange {
    pub start: u64,
    pub end: u64,
}

impl OffsetRange {
    pub fn new(start: u64, end: u64) -> Option<Self> {
        (start < end).then_some(Self { start, end })
    }

    pub fn len(self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    pub fn is_empty(self) -> bool {
        self.start >= self.end
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    SessionHello {
        session_id: SessionId,
    },
    SessionAuth {
        session_id: SessionId,
        credential_id: String,
        nonce: AuthNonce,
        issued_at_unix_secs: u64,
        auth_tag: AuthTag,
    },
    SessionReady,
    SessionClose {
        reason: CloseReason,
    },
    PathJoin {
        session_id: SessionId,
        credential_id: String,
        path_id: PathId,
        underlay: UnderlayProtocol,
        nonce: AuthNonce,
        issued_at_unix_secs: u64,
        auth_tag: AuthTag,
    },
    /// Advertises how the receiver wants the peer to use this path for data it
    /// sends. The monotonically increasing sequence makes reordering explicit.
    PathStatus {
        path_id: PathId,
        sequence: u64,
        usage: PathUsage,
    },
    PathDrain {
        path_id: PathId,
    },
    PathClose {
        path_id: PathId,
        reason: CloseReason,
    },
    PathProofData {
        path_id: PathId,
        proof_id: u64,
        payload: Bytes,
    },
    PathProofAck {
        path_id: PathId,
        proof_id: u64,
        payload_bytes: u32,
    },
    // Capacity measurement is carrier traffic with an exact ordered receipt;
    // it never consumes or acknowledges product stream offsets.
    PathCapacityData {
        path_id: PathId,
        measurement_id: u64,
        payload: Bytes,
    },
    PathCapacityFinish {
        path_id: PathId,
        measurement_id: u64,
        payload_bytes: u64,
    },
    PathCapacityReceipt {
        path_id: PathId,
        measurement_id: u64,
        received_payload_bytes: u64,
    },
    OpenStream {
        stream_id: StreamId,
        target: TargetAddr,
        demand: StreamDemandHint,
    },
    StreamData {
        stream_id: StreamId,
        offset: u64,
        payload: Bytes,
    },
    StreamAck {
        stream_id: StreamId,
        complete: bool,
        ranges: Vec<OffsetRange>,
    },
    StreamMaxData {
        stream_id: StreamId,
        max_offset: u64,
    },
    StreamFin {
        stream_id: StreamId,
        final_offset: u64,
    },
    StreamDetach {
        stream_id: StreamId,
    },
    StreamReset {
        stream_id: StreamId,
        reason: ResetReason,
    },
    OpenDatagramFlow {
        flow_id: DatagramFlowId,
        target: TargetAddr,
    },
    DatagramData {
        flow_id: DatagramFlowId,
        datagram_id: DatagramId,
        ttl_ms: u32,
        payload: Bytes,
    },
    DatagramClose {
        flow_id: DatagramFlowId,
    },
    DatagramFeedback {
        flow_id: DatagramFlowId,
        received: Vec<OffsetRange>,
    },
    PathMetrics {
        metrics: PathMetrics,
    },
    PeerStatusRequest {
        request_id: u64,
    },
    PeerStatusResponse {
        request_id: u64,
        code: PeerStatusCode,
        paths: Vec<PeerPathStatus>,
    },
    /// Opens one authenticated layer-3 packet service. The same logical
    /// tunnel identity may attach to multiple carrier paths.
    OpenIpTunnel {
        tunnel_id: IpTunnelId,
    },
    /// Confirms the statically configured peer allocation. Addresses are host
    /// identities; neither endpoint infers or installs routes from this list.
    IpTunnelReady {
        tunnel_id: IpTunnelId,
        mtu: u16,
        addresses: Vec<IpAddr>,
    },
    /// One complete IPv4 or IPv6 packet. MPP does not acknowledge or
    /// retransmit this frame; reliability, if any, belongs to the carrier.
    IpPacket {
        tunnel_id: IpTunnelId,
        packet_id: IpPacketId,
        payload: Bytes,
    },
    IpTunnelClose {
        tunnel_id: IpTunnelId,
        reason: CloseReason,
    },
    Ping {
        nonce: u64,
    },
    Pong {
        nonce: u64,
    },
}

impl Frame {
    pub fn is_path_capacity(&self) -> bool {
        matches!(
            self,
            Self::PathCapacityData { .. }
                | Self::PathCapacityFinish { .. }
                | Self::PathCapacityReceipt { .. }
        )
    }

    pub fn delivery_evidence_bytes(&self) -> u64 {
        match self {
            Self::StreamData { payload, .. } | Self::DatagramData { payload, .. } => {
                payload.len() as u64
            }
            _ => 0,
        }
    }

    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::SessionHello { .. } => "SESSION_HELLO",
            Self::SessionAuth { .. } => "SESSION_AUTH",
            Self::SessionReady => "SESSION_READY",
            Self::SessionClose { .. } => "SESSION_CLOSE",
            Self::PathJoin { .. } => "PATH_JOIN",
            Self::PathStatus { .. } => "PATH_STATUS",
            Self::PathDrain { .. } => "PATH_DRAIN",
            Self::PathClose { .. } => "PATH_CLOSE",
            Self::PathProofData { .. } => "PATH_PROOF_DATA",
            Self::PathProofAck { .. } => "PATH_PROOF_ACK",
            Self::PathCapacityData { .. } => "PATH_CAPACITY_DATA",
            Self::PathCapacityFinish { .. } => "PATH_CAPACITY_FINISH",
            Self::PathCapacityReceipt { .. } => "PATH_CAPACITY_RECEIPT",
            Self::OpenStream { .. } => "OPEN_STREAM",
            Self::StreamData { .. } => "STREAM_DATA",
            Self::StreamAck { .. } => "STREAM_ACK",
            Self::StreamMaxData { .. } => "STREAM_MAX_DATA",
            Self::StreamFin { .. } => "STREAM_FIN",
            Self::StreamDetach { .. } => "STREAM_DETACH",
            Self::StreamReset { .. } => "STREAM_RESET",
            Self::OpenDatagramFlow { .. } => "OPEN_DGRAM_FLOW",
            Self::DatagramData { .. } => "DGRAM_DATA",
            Self::DatagramClose { .. } => "DGRAM_CLOSE",
            Self::DatagramFeedback { .. } => "DGRAM_FEEDBACK",
            Self::PathMetrics { .. } => "PATH_METRICS",
            Self::PeerStatusRequest { .. } => "PEER_STATUS_REQUEST",
            Self::PeerStatusResponse { .. } => "PEER_STATUS_RESPONSE",
            Self::OpenIpTunnel { .. } => "OPEN_IP_TUNNEL",
            Self::IpTunnelReady { .. } => "IP_TUNNEL_READY",
            Self::IpPacket { .. } => "IP_PACKET",
            Self::IpTunnelClose { .. } => "IP_TUNNEL_CLOSE",
            Self::Ping { .. } => "PING",
            Self::Pong { .. } => "PONG",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    Normal,
    ProtocolError,
    AuthenticationFailed,
    PolicyRejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetReason {
    Refused,
    TimedOut,
    RemoteClosed,
    PolicyRejected,
}

#[cfg(test)]
#[path = "tests_types.rs"]
mod tests;
