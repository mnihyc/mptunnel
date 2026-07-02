use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) struct UdpPathMetrics {
    pub(super) direction: u8,
    pub(super) srtt: Duration,
    pub(super) rttvar: Duration,
    pub(super) min_rtt: Duration,
    pub(super) min_rtt_observed: bool,
    pub(super) delivery_rate_bps: f64,
    pub(super) pacing_rate_bps: f64,
    pub(super) inflight_hi: usize,
    pub(super) bytes_in_flight: usize,
    pub(super) pending_bytes: usize,
    pub(super) app_limited: bool,
    pub(super) delivery_sample_count: u64,
    pub(super) last_delivery_sample_at: Option<Instant>,
}
