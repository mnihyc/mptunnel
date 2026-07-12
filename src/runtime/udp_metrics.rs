use super::*;

#[cfg(feature = "lab-diagnostics")]
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct QuicAckPollDiagnostics {
    pub(super) newly_acked_bytes: u64,
    pub(super) non_app_limited_acked_bytes: u64,
    pub(super) timed_non_app_limited_acked_bytes: u64,
    pub(super) ack_elapsed: Duration,
    pub(super) delivery_sample_count: u64,
    pub(super) non_app_limited_sample_count: u64,
    pub(super) timed_non_app_limited_sample_count: u64,
    pub(super) carrier_app_limited: bool,
    pub(super) delivery_evidence_written_delta: u64,
    pub(super) delivery_evidence_newly_acked_bytes: u64,
    pub(super) delivery_evidence_pending_ack_bytes: u64,
    pub(super) pending_sample_bytes: u64,
    pub(super) pending_sample_count: u64,
    pub(super) pending_sample_elapsed: Duration,
}

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
    pub(super) loss_ppm: Option<u32>,
    pub(super) ecn_ppm: Option<u32>,
    pub(super) app_limited: bool,
    pub(super) ack_derived_data_seen: bool,
    pub(super) delivery_sample_count: u64,
    pub(super) delivery_sample_bytes: u64,
    pub(super) last_delivery_sample_at: Option<Instant>,
    #[cfg_attr(not(any(test, feature = "lab-diagnostics")), allow(dead_code))]
    pub(super) bulk_proof_expires_at: Option<Instant>,
    // The latest accepted strict sample is kept separate from cumulative model
    // state so diagnostics can audit its carrier-clock denominator directly.
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(super) latest_delivery_sample_bytes: u64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(super) latest_delivery_sample_count: u64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(super) latest_carrier_ack_elapsed: Option<Duration>,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(super) latest_rate_sample_elapsed: Option<Duration>,
    pub(super) capacity_proof_candidate: Option<QuicCapacityProofCandidate>,
    pub(super) capacity_probe: Option<quic_carrier::CapacityProbeMetrics>,
    #[cfg(feature = "lab-diagnostics")]
    pub(super) ack_poll: QuicAckPollDiagnostics,
}
