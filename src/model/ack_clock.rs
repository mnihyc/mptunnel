//! Direction-neutral TCP Data ACK sampling limits.
//!
//! A bounded connection-data sample can establish causal delivery evidence
//! when native TCP telemetry is unavailable. It does not replace TCP congestion
//! control, recovery, or pacing. Request and response owners use the same byte
//! geometry even though their runtime evidence stores are independent. QUIC
//! packet ACKs remain carrier-owned.

use super::admission::bulk_scheduling_horizon_bytes;
use crate::model::capacity::{MAX_RELIABLE_SERVICE_QUANTUM_BYTES, PATH_OPEN_SCORE_BYTES};
use crate::mux::MuxLimits;

pub(crate) fn reliable_ack_clock_measurement_limit_bytes(mux_limits: MuxLimits) -> u64 {
    let resource_ceiling = reliable_ack_clock_measurement_ceiling_bytes(mux_limits);
    if resource_ceiling == 0 {
        return 0;
    }
    let scheduling_horizon =
        bulk_scheduling_horizon_bytes(MAX_RELIABLE_SERVICE_QUANTUM_BYTES, mux_limits) as u64;
    scheduling_horizon.min(resource_ceiling)
}

pub(crate) fn reliable_data_ack_rate_coverage_floor_bytes(mux_limits: MuxLimits) -> u64 {
    let product_limit = reliable_ack_clock_measurement_limit_bytes(mux_limits);
    if product_limit == 0 {
        return 0;
    }
    product_limit
        .div_ceil(2)
        .max(PATH_OPEN_SCORE_BYTES as u64)
        .min(product_limit)
}

pub(crate) fn reliable_request_ack_clock_measurement_target_bytes(mux_limits: MuxLimits) -> u64 {
    let base = reliable_ack_clock_measurement_limit_bytes(mux_limits);
    let ceiling = reliable_ack_clock_measurement_ceiling_bytes(mux_limits);
    if base == 0 {
        return 0;
    }
    // This bounded epoch proves causal request ownership; it is not a second
    // congestion controller and must not serialize an entire high-BDP pipe.
    // Continuous exact samples mature after admission. Reserve one maximum
    // frame so non-divisible configured geometry can cross the target.
    let max_payload = MAX_RELIABLE_SERVICE_QUANTUM_BYTES
        .min(mux_limits.max_reliable_relay_chunk_bytes)
        .min(mux_limits.max_payload_bytes)
        .min(mux_limits.max_path_flight_bytes)
        .max(1) as u64;
    if max_payload > ceiling {
        return 0;
    }
    let reachable_target = ceiling.saturating_sub(max_payload);
    if reachable_target == 0 {
        base.min(ceiling)
    } else {
        base.min(reachable_target)
    }
}

pub(crate) fn reliable_ack_clock_measurement_ceiling_bytes(mux_limits: MuxLimits) -> u64 {
    let resource_ceiling = (mux_limits.max_path_flight_bytes as u64)
        .min(mux_limits.max_repair_bytes as u64)
        .min(mux_limits.max_reorder_bytes as u64)
        .min(mux_limits.max_stream_window_bytes);
    if resource_ceiling < PATH_OPEN_SCORE_BYTES as u64 {
        0
    } else {
        resource_ceiling
    }
}

#[cfg(test)]
#[path = "ack_clock_test.rs"]
mod tests;
