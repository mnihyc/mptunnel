pub mod datagram;
pub mod stream;

use crate::config::ResourceLimits;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MuxLimits {
    pub max_payload_bytes: usize,
    pub max_ack_ranges: usize,
    pub max_stream_window_bytes: u64,
    pub max_repair_bytes: usize,
    pub max_reorder_bytes: usize,
    pub max_datagram_queue_bytes: usize,
    pub max_tcp_path_inflight_bytes: usize,
    pub tcp_path_heartbeat_interval: Duration,
    pub tcp_path_heartbeat_timeout: Duration,
}

impl Default for MuxLimits {
    fn default() -> Self {
        Self {
            max_payload_bytes: 1_048_512,
            max_ack_ranges: 256,
            max_stream_window_bytes: 16 * 1024 * 1024,
            max_repair_bytes: 16 * 1024 * 1024,
            max_reorder_bytes: 16 * 1024 * 1024,
            max_datagram_queue_bytes: 4 * 1024 * 1024,
            max_tcp_path_inflight_bytes: crate::config::DEFAULT_TCP_PATH_INFLIGHT_BYTES,
            tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
            tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
        }
    }
}

impl From<ResourceLimits> for MuxLimits {
    fn from(value: ResourceLimits) -> Self {
        Self {
            max_payload_bytes: value.max_payload_bytes,
            max_ack_ranges: value.max_ack_ranges,
            max_stream_window_bytes: value.max_stream_window_bytes,
            max_repair_bytes: value.max_repair_bytes,
            max_reorder_bytes: value.max_reorder_bytes,
            max_datagram_queue_bytes: value.max_datagram_queue_bytes,
            max_tcp_path_inflight_bytes: value.max_tcp_path_inflight_bytes,
            tcp_path_heartbeat_interval: value.tcp_path_heartbeat_interval,
            tcp_path_heartbeat_timeout: value.tcp_path_heartbeat_timeout,
        }
    }
}
