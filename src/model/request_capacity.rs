//! Request-side TCP capacity transaction geometry.
//!
//! QUIC uses native congestion control and bounded ordinary product samples;
//! only TCP needs an explicit receiver-confirmed capacity transaction.

use super::capacity::{
    CAPACITY_TIMING_SLACK_BYTES, PATH_OPEN_SCORE_BYTES, RELIABLE_PIPE_WINDOW_BDPS,
    product_delivery_samples_override_startup_prior,
    reliable_capacity_measurement_session_limit_bytes, reliable_path_startup_sample_limit_bytes,
};
use super::request_evidence::RequestPerFlowRateModel;
use super::timing::transport_pto_from_snapshot;
use crate::mux::MuxLimits;
use crate::protocol::UnderlayProtocol;
use crate::scheduler::PathSnapshot;
use std::time::Duration;

#[cfg(test)]
#[path = "request_capacity_test.rs"]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RequestTcpCapacityMeasurementGeometry {
    pub(crate) train_bytes: u64,
    pub(crate) sample_floor_bytes: u64,
    pub(crate) accounting_slack_bytes: u64,
    pub(crate) timing_slack_bytes: u64,
    pub(crate) warmup_carrier_bytes: u64,
    pub(crate) required_timed_carrier_bytes: u64,
    pub(crate) reference_rate_bps: u64,
    pub(crate) candidate_carrier_flight_bytes: u64,
}

pub(crate) fn request_capacity_stable_candidate_share_bytes(
    mux_limits: MuxLimits,
    eligible_candidates: usize,
) -> u64 {
    // Divide once from configured policy eligibility. Attempt order and unused
    // earlier shares must not make a later path's measurement more expensive.
    let divisor = u64::try_from(eligible_candidates.max(1)).unwrap_or(u64::MAX);
    reliable_capacity_measurement_session_limit_bytes(mux_limits) / divisor
}

pub(crate) fn request_tcp_capacity_measurement_geometry(
    candidate: PathSnapshot,
    reference_model: RequestPerFlowRateModel,
    mux_limits: MuxLimits,
    train_envelope_bytes: u64,
) -> Option<RequestTcpCapacityMeasurementGeometry> {
    // TCP seeds from a full receiver-confirmed train and never truncates warmup.
    if candidate.underlay != UnderlayProtocol::Tcp
        || !product_delivery_samples_override_startup_prior(reference_model.delivery_samples)
        || !reference_model.rate_bps.is_finite()
        || reference_model.rate_bps <= 0.0
    {
        return None;
    }
    let sample_floor_bytes = reliable_path_startup_sample_limit_bytes(mux_limits);
    let accounting_slack_bytes = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor_bytes / 8);
    let required_timed_carrier_bytes = sample_floor_bytes
        .saturating_sub(accounting_slack_bytes)
        .max(1);
    let timing_slack_bytes = CAPACITY_TIMING_SLACK_BYTES;
    let candidate_carrier_flight_bytes = candidate.bytes_in_flight;
    let competing_rate_bdp =
        (reference_model.rate_bps / 8.0 * candidate.srtt_ms.max(1.0) / 1_000.0).ceil() as u64;
    let competing_rate_pipe =
        ((competing_rate_bdp as f64) * RELIABLE_PIPE_WINDOW_BDPS).ceil() as u64;
    // A larger configured/startup cwnd is not native evidence. The exact flight
    // and the reference-rate pipe are the only warmup authorities available here.
    let warmup_carrier_bytes = candidate_carrier_flight_bytes
        .max(competing_rate_pipe)
        .max(PATH_OPEN_SCORE_BYTES as u64);
    let train_bytes = warmup_carrier_bytes
        .checked_add(timing_slack_bytes)?
        .checked_add(required_timed_carrier_bytes)?;
    let train_envelope_bytes = train_envelope_bytes.min(
        reliable_capacity_measurement_session_limit_bytes(mux_limits),
    );
    if train_bytes > train_envelope_bytes {
        return None;
    }
    Some(RequestTcpCapacityMeasurementGeometry {
        train_bytes,
        sample_floor_bytes,
        accounting_slack_bytes,
        timing_slack_bytes,
        warmup_carrier_bytes,
        required_timed_carrier_bytes,
        reference_rate_bps: reference_model.rate_bps.ceil() as u64,
        candidate_carrier_flight_bytes,
    })
}

pub(crate) fn request_tcp_capacity_candidate_can_start_receipt(candidate: PathSnapshot) -> bool {
    // Product and unsent queue debt cannot enter a capacity epoch. Stale
    // control flight may remain locally unacknowledged: TCP ordering plus the
    // full typed receipt makes that delay conservative rather than ambiguous.
    candidate.queue_bytes == 0
        && candidate.data_level_bytes_in_flight == 0
        && candidate.data_level_queue_bytes == 0
        && candidate.active_latency_sensitive_flows == 0
        && candidate.session_active_latency_sensitive_flows == 0
}

pub(crate) fn request_capacity_slow_start_rounds(train_bytes: u64) -> u32 {
    let mut rounds = 1_u32;
    let mut window_bytes = PATH_OPEN_SCORE_BYTES as u64;
    let mut cumulative_bytes = window_bytes;
    while cumulative_bytes < train_bytes {
        window_bytes = window_bytes.saturating_mul(2);
        cumulative_bytes = cumulative_bytes.saturating_add(window_bytes);
        rounds = rounds.saturating_add(1);
        if cumulative_bytes == u64::MAX {
            break;
        }
    }
    rounds
}

pub(crate) fn request_tcp_capacity_measurement_lease(
    candidate: PathSnapshot,
    train_bytes: u64,
    reference_rate_bps: u64,
) -> Duration {
    let pto = transport_pto_from_snapshot(Some(candidate));
    // Ordinary loss can delay any cold congestion-growth round. Budget each
    // modeled round with the candidate PTO instead of assuming lossless
    // SRTT-paced doubling; this remains a deadline, so success finishes early.
    let growth = pto.saturating_mul(request_capacity_slow_start_rounds(train_bytes));
    let reference_transfer =
        Duration::from_secs_f64(train_bytes as f64 * 8.0 / reference_rate_bps.max(1) as f64);
    // One PTO lets prior unsent control drain; the trailing PTO covers the
    // final typed receipt and ordinary recovery without a fixed margin.
    pto.saturating_add(growth.max(reference_transfer))
        .saturating_add(pto)
        .max(Duration::from_secs(1))
}
