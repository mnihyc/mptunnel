//! Canonical product-wire values.
//!
//! These types describe interoperable facts, not runtime handles, transport
//! congestion state, or endpoint-local configuration.

use bytes::Bytes;
use std::net::SocketAddr;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

/// Control-plane attachment role; product Service/Subflow ownership is chosen
/// independently for each direction after the attachment is live.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamOpenRole {
    Active,
    Repair,
    Validation,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathStatus {
    Active,
    Suspect,
    Draining,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PathMetrics {
    pub path_id: PathId,
    pub underlay: UnderlayProtocol,
    pub direction: PathMetricDirection,
    pub metric_epoch: u64,
    pub metric_age_us: u32,
    pub srtt_us: u32,
    pub rttvar_us: u32,
    pub jitter_us: u32,
    pub delivery_rate_bps: u64,
    pub pacing_rate_bps: u64,
    pub loss_ppm: u32,
    pub ecn_ppm: u32,
    pub loss_observed: bool,
    pub ecn_observed: bool,
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
        path_id: PathId,
        underlay: UnderlayProtocol,
        nonce: AuthNonce,
        issued_at_unix_secs: u64,
        auth_tag: AuthTag,
    },
    PathJoinOk {
        path_id: PathId,
        nonce: AuthNonce,
        auth_tag: AuthTag,
    },
    PathStatus {
        path_id: PathId,
        status: PathStatus,
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
    // Capacity calibration is carrier traffic with an exact ordered receipt;
    // it never consumes or acknowledges product stream offsets.
    PathCapacityData {
        path_id: PathId,
        calibration_id: u64,
        payload: Bytes,
    },
    PathCapacityFinish {
        path_id: PathId,
        calibration_id: u64,
        payload_bytes: u64,
    },
    PathCapacityReceipt {
        path_id: PathId,
        calibration_id: u64,
        received_payload_bytes: u64,
    },
    OpenStream {
        stream_id: StreamId,
        target: TargetAddr,
        demand: StreamDemandHint,
        role: StreamOpenRole,
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
    Ping {
        nonce: u64,
    },
    Pong {
        nonce: u64,
    },
}

/// Separates ordinary product/control traffic from an exclusive measurement.
///
/// The protocol model owns this classification because only it knows a frame's
/// semantic role. Carriers use it to enforce writer ownership without learning
/// which measurement protocol produced the frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrameWriteClass {
    Ordinary { delivery_evidence_bytes: u64 },
    MeasurementData { payload_bytes: u64 },
    MeasurementFinish,
    MeasurementControl,
}

impl Frame {
    pub(crate) fn write_class(&self) -> FrameWriteClass {
        match self {
            Self::PathCapacityData { payload, .. } => FrameWriteClass::MeasurementData {
                payload_bytes: payload.len() as u64,
            },
            Self::PathCapacityFinish { .. } => FrameWriteClass::MeasurementFinish,
            Self::PathCapacityReceipt { .. } => FrameWriteClass::MeasurementControl,
            Self::StreamData { payload, .. } | Self::DatagramData { payload, .. } => {
                FrameWriteClass::Ordinary {
                    delivery_evidence_bytes: payload.len() as u64,
                }
            }
            _ => FrameWriteClass::Ordinary {
                delivery_evidence_bytes: 0,
            },
        }
    }

    pub(crate) fn kind_name(&self) -> &'static str {
        match self {
            Self::SessionHello { .. } => "SESSION_HELLO",
            Self::SessionAuth { .. } => "SESSION_AUTH",
            Self::SessionReady => "SESSION_READY",
            Self::SessionClose { .. } => "SESSION_CLOSE",
            Self::PathJoin { .. } => "PATH_JOIN",
            Self::PathJoinOk { .. } => "PATH_JOIN_OK",
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
#[path = "types_test.rs"]
mod tests;
