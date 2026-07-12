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
pub struct PacketNumber(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AuthNonce(pub [u8; 16]);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AuthTag(pub [u8; 32]);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnderlayProtocol {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathMetricDirection {
    ClientToServer,
    ServerToClient,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressKind {
    Socks5,
    HttpConnect,
    TunTcp,
    TunUdp,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundPolicy {
    Direct,
    BindSourceIp(IpAddr),
    Socks5 { proxy: SocketAddr },
    HttpConnect { proxy: SocketAddr },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamFlags {
    pub fin: bool,
    pub early_data: bool,
}

impl StreamFlags {
    pub const NONE: Self = Self {
        fin: false,
        early_data: false,
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamDemandHint {
    pub observed_bytes: u64,
    pub repair_bytes: u64,
    pub latency_weight_ppm: u32,
    pub throughput_weight_ppm: u32,
    pub realtime_weight_ppm: u32,
}

impl StreamDemandHint {
    pub const PPM_MAX: u32 = 1_000_000;

    pub const fn latency() -> Self {
        Self {
            observed_bytes: 0,
            repair_bytes: 0,
            latency_weight_ppm: Self::PPM_MAX,
            throughput_weight_ppm: 0,
            realtime_weight_ppm: 0,
        }
    }

    pub const fn throughput() -> Self {
        Self {
            observed_bytes: 0,
            repair_bytes: 0,
            latency_weight_ppm: 0,
            throughput_weight_ppm: Self::PPM_MAX,
            realtime_weight_ppm: 0,
        }
    }

    pub const fn realtime() -> Self {
        Self {
            observed_bytes: 0,
            repair_bytes: 0,
            latency_weight_ppm: 0,
            throughput_weight_ppm: 0,
            realtime_weight_ppm: Self::PPM_MAX,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PathCapabilities {
    pub backup: bool,
    pub expensive: bool,
    pub low_latency: bool,
    pub bulk_allowed: bool,
    pub probe_only: bool,
    pub no_udp: bool,
}

impl Default for PathCapabilities {
    fn default() -> Self {
        Self {
            backup: false,
            expensive: false,
            low_latency: false,
            bulk_allowed: true,
            probe_only: false,
            no_udp: false,
        }
    }
}

impl PathCapabilities {
    const BACKUP: u16 = 0x0001;
    const EXPENSIVE: u16 = 0x0002;
    const LOW_LATENCY: u16 = 0x0004;
    const BULK_ALLOWED: u16 = 0x0008;
    const PROBE_ONLY: u16 = 0x0010;
    const NO_UDP: u16 = 0x0020;
    const KNOWN_MASK: u16 = Self::BACKUP
        | Self::EXPENSIVE
        | Self::LOW_LATENCY
        | Self::BULK_ALLOWED
        | Self::PROBE_ONLY
        | Self::NO_UDP;

    pub fn to_bits(self) -> u16 {
        let mut bits = 0u16;
        if self.backup {
            bits |= Self::BACKUP;
        }
        if self.expensive {
            bits |= Self::EXPENSIVE;
        }
        if self.low_latency {
            bits |= Self::LOW_LATENCY;
        }
        if self.bulk_allowed {
            bits |= Self::BULK_ALLOWED;
        }
        if self.probe_only {
            bits |= Self::PROBE_ONLY;
        }
        if self.no_udp {
            bits |= Self::NO_UDP;
        }
        bits
    }

    pub fn from_bits(bits: u16) -> Option<Self> {
        if bits & !Self::KNOWN_MASK != 0 {
            return None;
        }
        Some(Self {
            backup: bits & Self::BACKUP != 0,
            expensive: bits & Self::EXPENSIVE != 0,
            low_latency: bits & Self::LOW_LATENCY != 0,
            bulk_allowed: bits & Self::BULK_ALLOWED != 0,
            probe_only: bits & Self::PROBE_ONLY != 0,
            no_udp: bits & Self::NO_UDP != 0,
        })
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
    pub min_rtt_us: u32,
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
pub enum RateHint {
    Unknown,
    Unlimited,
    BitsPerSecond(u64),
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
        capabilities: PathCapabilities,
        auth_tag: AuthTag,
    },
    PathJoinOk {
        path_id: PathId,
        nonce: AuthNonce,
        auth_tag: AuthTag,
    },
    PathChallenge {
        path_id: PathId,
        nonce: u64,
    },
    PathResponse {
        path_id: PathId,
        nonce: u64,
    },
    PathStatus {
        path_id: PathId,
        status: PathStatus,
        capabilities: PathCapabilities,
    },
    PathDrain {
        path_id: PathId,
    },
    PathClose {
        path_id: PathId,
        reason: CloseReason,
    },
    PathMtuProbe {
        path_id: PathId,
        probe_id: u64,
        payload: Bytes,
    },
    PathMtuAck {
        path_id: PathId,
        probe_id: u64,
        payload_bytes: u32,
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
        ingress: IngressKind,
        outbound: OutboundPolicy,
        demand: StreamDemandHint,
        role: StreamOpenRole,
    },
    StreamData {
        stream_id: StreamId,
        offset: u64,
        flags: StreamFlags,
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
        ingress: IngressKind,
        outbound: OutboundPolicy,
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
    RxRateHint {
        path_id: PathId,
        hint: RateHint,
    },
    MaxConnectionData {
        max_bytes: u64,
    },
    Ping {
        nonce: u64,
    },
    Pong {
        nonce: u64,
    },
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
mod tests {
    use super::*;

    #[test]
    fn offset_ranges_must_be_non_empty() {
        assert_eq!(OffsetRange::new(10, 10), None);
        assert_eq!(OffsetRange::new(11, 10), None);
        assert_eq!(OffsetRange::new(10, 12).map(OffsetRange::len), Some(2));
        assert!(OffsetRange { start: 10, end: 10 }.is_empty());
    }
}
