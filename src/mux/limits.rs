use crate::performance::{
    DEFAULT_DATAGRAM_QUEUE_BYTES, DEFAULT_MAX_QUIC_CONCURRENT_BIDI_STREAMS,
    DEFAULT_MAX_REINJECTION_CACHE_CHUNKS, DEFAULT_MAX_RELIABLE_RELAY_CHUNK_BYTES,
    DEFAULT_MAX_REORDER_BUFFER_CHUNKS, DEFAULT_MAX_RETAINED_RECEIVE_RANGES,
    DEFAULT_PATH_FLIGHT_BYTES, DEFAULT_QUIC_PATH_IDLE_TIMEOUT,
    DEFAULT_QUIC_PATH_KEEP_ALIVE_INTERVAL, DEFAULT_REORDER_BYTES, DEFAULT_REPAIR_BYTES,
    DEFAULT_STREAM_WINDOW_BYTES, DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
    DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT, ResourceLimits,
};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MuxLimits {
    pub max_payload_bytes: usize,
    pub max_ack_ranges: usize,
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

impl Default for MuxLimits {
    fn default() -> Self {
        Self {
            max_payload_bytes: 1_048_512,
            max_ack_ranges: 256,
            max_streams: 65_536,
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

impl From<ResourceLimits> for MuxLimits {
    fn from(value: ResourceLimits) -> Self {
        Self {
            max_payload_bytes: value.max_payload_bytes,
            max_ack_ranges: value.max_ack_ranges,
            max_streams: value.max_streams,
            max_quic_concurrent_bidi_streams: value.max_quic_concurrent_bidi_streams,
            max_stream_window_bytes: value.max_stream_window_bytes,
            max_repair_bytes: value.max_repair_bytes,
            max_reorder_bytes: value.max_reorder_bytes,
            max_reinjection_cache_chunks: value.max_reinjection_cache_chunks,
            max_reorder_buffer_chunks: value.max_reorder_buffer_chunks,
            max_retained_receive_ranges: value.max_retained_receive_ranges,
            max_datagram_queue_bytes: value.max_datagram_queue_bytes,
            max_path_flight_bytes: value.max_path_flight_bytes,
            max_reliable_relay_chunk_bytes: value.max_reliable_relay_chunk_bytes,
            tcp_path_heartbeat_interval: value.tcp_path_heartbeat_interval,
            tcp_path_heartbeat_timeout: value.tcp_path_heartbeat_timeout,
            quic_path_keep_alive_interval: value.quic_path_keep_alive_interval,
            quic_path_idle_timeout: value.quic_path_idle_timeout,
        }
    }
}
