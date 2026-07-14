use super::*;

#[cfg(feature = "lab-diagnostics")]
#[derive(Debug, Clone, Copy, Default)]
pub(in crate::runtime) struct QuicAckPollDiagnostics {
    pub(in crate::runtime) newly_acked_bytes: u64,
    pub(in crate::runtime) non_app_limited_acked_bytes: u64,
    pub(in crate::runtime) timed_non_app_limited_acked_bytes: u64,
    pub(in crate::runtime) ack_elapsed: Duration,
    pub(in crate::runtime) delivery_sample_count: u64,
    pub(in crate::runtime) non_app_limited_sample_count: u64,
    pub(in crate::runtime) timed_non_app_limited_sample_count: u64,
    pub(in crate::runtime) carrier_app_limited: bool,
    pub(in crate::runtime) delivery_evidence_written_delta: u64,
    pub(in crate::runtime) delivery_evidence_newly_acked_bytes: u64,
    pub(in crate::runtime) delivery_evidence_pending_ack_bytes: u64,
    pub(in crate::runtime) pending_sample_bytes: u64,
    pub(in crate::runtime) pending_sample_count: u64,
    pub(in crate::runtime) pending_sample_elapsed: Duration,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct UdpPathMetrics {
    pub(in crate::runtime) direction: u8,
    pub(in crate::runtime) srtt: Duration,
    pub(in crate::runtime) rttvar: Duration,
    pub(in crate::runtime) min_rtt: Duration,
    pub(in crate::runtime) min_rtt_observed: bool,
    pub(in crate::runtime) delivery_rate_bps: f64,
    pub(in crate::runtime) pacing_rate_bps: f64,
    pub(in crate::runtime) inflight_hi: usize,
    pub(in crate::runtime) bytes_in_flight: usize,
    pub(in crate::runtime) pending_bytes: usize,
    pub(in crate::runtime) loss_ppm: Option<u32>,
    pub(in crate::runtime) ecn_ppm: Option<u32>,
    pub(in crate::runtime) app_limited: bool,
    pub(in crate::runtime) ack_derived_data_seen: bool,
    pub(in crate::runtime) delivery_sample_count: u64,
    pub(in crate::runtime) delivery_sample_bytes: u64,
    pub(in crate::runtime) last_delivery_sample_at: Option<Instant>,
    #[cfg_attr(not(any(test, feature = "lab-diagnostics")), allow(dead_code))]
    pub(in crate::runtime) bulk_proof_expires_at: Option<Instant>,
    // The latest accepted strict sample is kept separate from cumulative model
    // state so diagnostics can audit its carrier-clock denominator directly.
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) latest_delivery_sample_bytes: u64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) latest_delivery_sample_count: u64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) latest_carrier_ack_elapsed: Option<Duration>,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) latest_rate_sample_elapsed: Option<Duration>,
    pub(in crate::runtime) capacity_proof_candidate: Option<QuicCapacityProofCandidate>,
    pub(in crate::runtime) capacity_probe: Option<quic_carrier::CapacityProbeMetrics>,
    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) ack_poll: QuicAckPollDiagnostics,
}
