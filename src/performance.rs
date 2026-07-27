use crate::protocol::codec::CodecLimits;
use std::time::Duration;

pub const DEFAULT_STREAM_WINDOW_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_REPAIR_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_REORDER_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_DATAGRAM_QUEUE_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_PATH_FLIGHT_BYTES: usize = DEFAULT_REPAIR_BYTES;
pub const DEFAULT_MAX_RELIABLE_RELAY_CHUNK_BYTES: usize = 512 * 1024;
/// Per-stream sparse-node ceiling.
///
/// The 64 MiB byte envelopes hold 128 normal 512 KiB relay chunks. 65,536
/// nodes still permits a pathological 1 KiB average chunk (512x the normal
/// node count) while preventing one-byte fragmentation from consuming
/// unbounded allocator/B-tree metadata.
pub const DEFAULT_MAX_REINJECTION_CACHE_CHUNKS: usize = 65_536;
pub const DEFAULT_MAX_REORDER_BUFFER_CHUNKS: usize = 65_536;
pub const DEFAULT_MAX_RETAINED_RECEIVE_RANGES: usize = 65_536;
pub const DEFAULT_MAX_STREAMS: usize = 65_536;
pub const DEFAULT_MAX_QUIC_CONCURRENT_BIDI_STREAMS: usize = DEFAULT_MAX_STREAMS;
pub const DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL_MS: u64 = 10_000;
pub const DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL: Duration =
    Duration::from_millis(DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL_MS);
pub const DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT: Duration =
    Duration::from_millis(DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT_MS);
pub const DEFAULT_QUIC_PATH_KEEP_ALIVE_INTERVAL_MS: u64 = 10_000;
pub const DEFAULT_QUIC_PATH_IDLE_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_QUIC_PATH_KEEP_ALIVE_INTERVAL: Duration =
    Duration::from_millis(DEFAULT_QUIC_PATH_KEEP_ALIVE_INTERVAL_MS);
pub const DEFAULT_QUIC_PATH_IDLE_TIMEOUT: Duration =
    Duration::from_millis(DEFAULT_QUIC_PATH_IDLE_TIMEOUT_MS);
pub const DEFAULT_EXTRA_TRAFFIC_HINT_PERCENT: u16 = 5;

// QUIC variable integers are limited to 62 bits. Keeping the wire limit here
// avoids coupling carrier-neutral resource policy to one QUIC implementation.
const MAX_QUIC_VARINT: u128 = (1_u128 << 62) - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MppPerformanceConfig {
    /// Operator hint for adaptive duplicate/probe/reinjection overhead, in percent.
    ///
    /// 5 means the sender may spend roughly 5% extra transport traffic when
    /// runtime evidence shows that duplicate, reinjection, or probe work can reduce
    /// stalls. The sender enforces this as a hard optional-work budget plus a
    /// small startup floor; it is not a product-data throttle. 100 permits full
    /// duplication in pathological cases, and values above 100 bias toward
    /// redundant reinjection under severe instability.
    pub extra_traffic_hint_percent: u16,
}

impl Default for MppPerformanceConfig {
    fn default() -> Self {
        Self {
            extra_traffic_hint_percent: DEFAULT_EXTRA_TRAFFIC_HINT_PERCENT,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceLimits {
    pub max_frame_bytes: usize,
    pub max_payload_bytes: usize,
    pub max_ack_ranges: usize,
    pub max_paths: usize,
    pub max_streams: usize,
    pub max_quic_concurrent_bidi_streams: usize,
    pub max_stream_window_bytes: u64,
    pub max_repair_bytes: usize,
    pub max_reorder_bytes: usize,
    pub max_reinjection_cache_chunks: usize,
    pub max_reorder_buffer_chunks: usize,
    pub max_retained_receive_ranges: usize,
    pub max_datagram_queue_bytes: usize,
    pub max_path_flight_bytes: usize,
    pub max_reliable_relay_chunk_bytes: usize,
    pub tcp_path_heartbeat_interval: Duration,
    pub tcp_path_heartbeat_timeout: Duration,
    pub quic_path_keep_alive_interval: Duration,
    pub quic_path_idle_timeout: Duration,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1_048_576,
            max_payload_bytes: 1_048_512,
            max_ack_ranges: 256,
            max_paths: 64,
            max_streams: DEFAULT_MAX_STREAMS,
            max_quic_concurrent_bidi_streams: DEFAULT_MAX_QUIC_CONCURRENT_BIDI_STREAMS,
            max_stream_window_bytes: DEFAULT_STREAM_WINDOW_BYTES,
            max_repair_bytes: DEFAULT_REPAIR_BYTES,
            max_reorder_bytes: DEFAULT_REORDER_BYTES,
            max_reinjection_cache_chunks: DEFAULT_MAX_REINJECTION_CACHE_CHUNKS,
            max_reorder_buffer_chunks: DEFAULT_MAX_REORDER_BUFFER_CHUNKS,
            max_retained_receive_ranges: DEFAULT_MAX_RETAINED_RECEIVE_RANGES,
            max_datagram_queue_bytes: DEFAULT_DATAGRAM_QUEUE_BYTES,
            max_path_flight_bytes: DEFAULT_PATH_FLIGHT_BYTES,
            max_reliable_relay_chunk_bytes: DEFAULT_MAX_RELIABLE_RELAY_CHUNK_BYTES,
            tcp_path_heartbeat_interval: DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
            tcp_path_heartbeat_timeout: DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
            quic_path_keep_alive_interval: DEFAULT_QUIC_PATH_KEEP_ALIVE_INTERVAL,
            quic_path_idle_timeout: DEFAULT_QUIC_PATH_IDLE_TIMEOUT,
        }
    }
}

impl ResourceLimits {
    pub fn validate(self) -> Result<(), ResourceLimitError> {
        if self.max_frame_bytes < 64 {
            return Err(ResourceLimitError::FrameLimitTooSmall);
        }
        if self.max_payload_bytes > self.max_frame_bytes.saturating_sub(16) {
            return Err(ResourceLimitError::PayloadLimitExceedsFrameLimit);
        }
        if self.max_ack_ranges == 0 {
            return Err(ResourceLimitError::AckRangeLimitZero);
        }
        if self.max_paths == 0 {
            return Err(ResourceLimitError::PathLimitZero);
        }
        if self.max_paths > u16::MAX as usize {
            return Err(ResourceLimitError::PathLimitTooLarge);
        }
        if self.max_streams == 0 {
            return Err(ResourceLimitError::StreamLimitZero);
        }
        if self.max_quic_concurrent_bidi_streams == 0 {
            return Err(ResourceLimitError::QuicBidiStreamLimitZero);
        }
        if self.max_stream_window_bytes == 0 {
            return Err(ResourceLimitError::StreamWindowLimitZero);
        }
        if self.max_repair_bytes < self.max_payload_bytes {
            return Err(ResourceLimitError::ReinjectionLimitTooSmall);
        }
        if self.max_reorder_bytes < self.max_payload_bytes {
            return Err(ResourceLimitError::ReorderLimitTooSmall);
        }
        if self.max_reinjection_cache_chunks == 0 {
            return Err(ResourceLimitError::ReinjectionCacheChunkLimitZero);
        }
        if self.max_reorder_buffer_chunks == 0 {
            return Err(ResourceLimitError::ReorderBufferChunkLimitZero);
        }
        if self.max_retained_receive_ranges == 0 {
            return Err(ResourceLimitError::RetainedReceiveRangeLimitZero);
        }
        if self.max_datagram_queue_bytes < self.max_payload_bytes {
            return Err(ResourceLimitError::DatagramQueueLimitTooSmall);
        }
        if self.max_reliable_relay_chunk_bytes == 0 {
            return Err(ResourceLimitError::MaxReliableRelayChunkBytesZero);
        }
        if self.max_reliable_relay_chunk_bytes > self.max_payload_bytes {
            return Err(ResourceLimitError::MaxReliableRelayChunkExceedsPayloadLimit);
        }
        if self.max_path_flight_bytes < self.max_reliable_relay_chunk_bytes {
            return Err(ResourceLimitError::PathFlightLimitTooSmall);
        }
        if self.max_path_flight_bytes > self.max_repair_bytes {
            return Err(ResourceLimitError::PathFlightLimitExceedsReinjectionLimit);
        }
        if self.tcp_path_heartbeat_interval.is_zero() {
            return Err(ResourceLimitError::TcpPathHeartbeatIntervalZero);
        }
        if self.tcp_path_heartbeat_timeout.is_zero() {
            return Err(ResourceLimitError::TcpPathHeartbeatTimeoutZero);
        }
        if self.tcp_path_heartbeat_timeout < self.tcp_path_heartbeat_interval {
            return Err(ResourceLimitError::TcpPathHeartbeatTimeoutTooSmall);
        }
        if self.quic_path_keep_alive_interval.is_zero() {
            return Err(ResourceLimitError::QuicPathKeepAliveIntervalZero);
        }
        if self.quic_path_idle_timeout.is_zero() {
            return Err(ResourceLimitError::QuicPathIdleTimeoutZero);
        }
        if self.quic_path_idle_timeout <= self.quic_path_keep_alive_interval {
            return Err(ResourceLimitError::QuicPathIdleTimeoutTooSmall);
        }
        if self.quic_path_idle_timeout.as_millis() > MAX_QUIC_VARINT {
            return Err(ResourceLimitError::QuicPathIdleTimeoutTooLarge);
        }
        Ok(())
    }
}

impl From<ResourceLimits> for CodecLimits {
    fn from(value: ResourceLimits) -> Self {
        Self {
            max_frame_bytes: value.max_frame_bytes,
            max_payload_bytes: value.max_payload_bytes,
            max_ack_ranges: value.max_ack_ranges,
            max_host_bytes: 255,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceLimitError {
    FrameLimitTooSmall,
    PayloadLimitExceedsFrameLimit,
    AckRangeLimitZero,
    PathLimitZero,
    PathLimitTooLarge,
    StreamLimitZero,
    QuicBidiStreamLimitZero,
    StreamWindowLimitZero,
    ReinjectionLimitTooSmall,
    ReorderLimitTooSmall,
    ReinjectionCacheChunkLimitZero,
    ReorderBufferChunkLimitZero,
    RetainedReceiveRangeLimitZero,
    DatagramQueueLimitTooSmall,
    MaxReliableRelayChunkBytesZero,
    MaxReliableRelayChunkExceedsPayloadLimit,
    PathFlightLimitTooSmall,
    PathFlightLimitExceedsReinjectionLimit,
    TcpPathHeartbeatIntervalZero,
    TcpPathHeartbeatTimeoutZero,
    TcpPathHeartbeatTimeoutTooSmall,
    QuicPathKeepAliveIntervalZero,
    QuicPathIdleTimeoutZero,
    QuicPathIdleTimeoutTooSmall,
    QuicPathIdleTimeoutTooLarge,
}

impl std::fmt::Display for ResourceLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FrameLimitTooSmall => write!(f, "max frame bytes must be at least 64"),
            Self::PayloadLimitExceedsFrameLimit => {
                write!(f, "max payload bytes must fit inside max frame bytes")
            }
            Self::AckRangeLimitZero => write!(f, "max ack ranges must be greater than zero"),
            Self::PathLimitZero => write!(f, "max paths must be greater than zero"),
            Self::PathLimitTooLarge => write!(f, "max paths must fit in protocol path IDs"),
            Self::StreamLimitZero => write!(f, "max streams must be greater than zero"),
            Self::QuicBidiStreamLimitZero => write!(
                f,
                "max QUIC concurrent bidirectional streams must be greater than zero"
            ),
            Self::StreamWindowLimitZero => {
                write!(f, "max stream window bytes must be greater than zero")
            }
            Self::ReinjectionLimitTooSmall => write!(
                f,
                "max reinjection bytes must be at least max payload bytes"
            ),
            Self::ReorderLimitTooSmall => {
                write!(f, "max reorder bytes must be at least max payload bytes")
            }
            Self::ReinjectionCacheChunkLimitZero => {
                write!(f, "max reinjection cache chunks must be greater than zero")
            }
            Self::ReorderBufferChunkLimitZero => {
                write!(f, "max reorder buffer chunks must be greater than zero")
            }
            Self::RetainedReceiveRangeLimitZero => {
                write!(f, "max retained receive ranges must be greater than zero")
            }
            Self::DatagramQueueLimitTooSmall => write!(
                f,
                "max datagram queue bytes must be at least max payload bytes"
            ),
            Self::MaxReliableRelayChunkBytesZero => {
                write!(
                    f,
                    "max reliable relay chunk bytes must be greater than zero"
                )
            }
            Self::MaxReliableRelayChunkExceedsPayloadLimit => write!(
                f,
                "max reliable relay chunk bytes must be no greater than max payload bytes"
            ),
            Self::PathFlightLimitTooSmall => {
                write!(f, "max path flight bytes must be at least one relay chunk")
            }
            Self::PathFlightLimitExceedsReinjectionLimit => write!(
                f,
                "max path flight bytes must be no greater than max reinjection bytes"
            ),
            Self::TcpPathHeartbeatIntervalZero => {
                write!(f, "TCP path heartbeat interval must be greater than zero")
            }
            Self::TcpPathHeartbeatTimeoutZero => {
                write!(f, "TCP path heartbeat timeout must be greater than zero")
            }
            Self::TcpPathHeartbeatTimeoutTooSmall => write!(
                f,
                "TCP path heartbeat timeout must be at least the heartbeat interval"
            ),
            Self::QuicPathKeepAliveIntervalZero => {
                write!(f, "QUIC path keep-alive interval must be greater than zero")
            }
            Self::QuicPathIdleTimeoutZero => {
                write!(f, "QUIC path idle timeout must be greater than zero")
            }
            Self::QuicPathIdleTimeoutTooSmall => write!(
                f,
                "QUIC path idle timeout must exceed its keep-alive interval"
            ),
            Self::QuicPathIdleTimeoutTooLarge => {
                write!(f, "QUIC path idle timeout exceeds the protocol timer range")
            }
        }
    }
}

impl std::error::Error for ResourceLimitError {}

#[cfg(test)]
#[path = "performance_test.rs"]
mod tests;
