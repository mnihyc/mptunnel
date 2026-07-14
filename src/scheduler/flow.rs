use crate::protocol::StreamDemandHint;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FlowLane {
    Control,
    Latency,
    Throughput,
    RealtimeDatagram,
    Background,
}

impl FlowLane {
    /// Latency-sensitive work keeps priority queueing and avoids bulk-path
    /// sharing penalties across every carrier implementation.
    pub(crate) const fn is_latency_sensitive(self) -> bool {
        matches!(self, Self::Control | Self::Latency | Self::RealtimeDatagram)
    }

    /// Bulk lanes may trade latency for sustained carrier feeding; the
    /// transport-specific schedulers consume this product classification.
    pub(crate) const fn is_bulk(self) -> bool {
        matches!(self, Self::Throughput | Self::Background)
    }
}

/// Converts local scheduling intent into the transport-neutral wire weights.
pub(crate) fn stream_demand_hint_for_lane(lane: FlowLane) -> StreamDemandHint {
    match lane {
        FlowLane::Control | FlowLane::Latency => StreamDemandHint::latency(),
        FlowLane::Throughput | FlowLane::Background => StreamDemandHint::throughput(),
        FlowLane::RealtimeDatagram => StreamDemandHint::realtime(),
    }
}

/// Recovers the strongest local lane from peer-advertised wire weights.
pub(crate) fn flow_lane_from_stream_demand_hint(demand: StreamDemandHint) -> FlowLane {
    let latency = demand.latency_weight_ppm;
    let throughput = demand.throughput_weight_ppm;
    let realtime = demand.realtime_weight_ppm;
    if realtime > 0 && realtime >= latency && realtime >= throughput {
        FlowLane::RealtimeDatagram
    } else if throughput > 0 && throughput >= latency {
        FlowLane::Throughput
    } else {
        FlowLane::Latency
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowDemand {
    pub lane: FlowLane,
    pub observed_bytes: u64,
    pub repair_bytes: u64,
    pub latency_weight_ppm: u32,
    pub throughput_weight_ppm: u32,
    pub realtime_weight_ppm: u32,
}

impl FlowDemand {
    pub const PPM_MAX: u32 = 1_000_000;

    pub fn control() -> Self {
        Self {
            lane: FlowLane::Control,
            observed_bytes: 0,
            repair_bytes: 0,
            latency_weight_ppm: Self::PPM_MAX,
            throughput_weight_ppm: 0,
            realtime_weight_ppm: 0,
        }
    }

    pub fn realtime_datagram() -> Self {
        Self {
            lane: FlowLane::RealtimeDatagram,
            observed_bytes: 0,
            repair_bytes: 0,
            latency_weight_ppm: Self::PPM_MAX,
            throughput_weight_ppm: 0,
            realtime_weight_ppm: Self::PPM_MAX,
        }
    }

    pub fn reliable_stream(
        observed_bytes: u64,
        repair_bytes: u64,
        throughput_threshold_bytes: u64,
    ) -> Self {
        let threshold = throughput_threshold_bytes.max(1);
        let throughput_weight_ppm = observed_bytes
            .saturating_mul(u64::from(Self::PPM_MAX))
            .checked_div(threshold)
            .unwrap_or(u64::from(Self::PPM_MAX))
            .min(u64::from(Self::PPM_MAX)) as u32;
        let latency_weight_ppm = Self::PPM_MAX.saturating_sub(throughput_weight_ppm);
        let lane = if throughput_weight_ppm > latency_weight_ppm {
            FlowLane::Throughput
        } else {
            FlowLane::Latency
        };
        Self {
            lane,
            observed_bytes,
            repair_bytes,
            latency_weight_ppm,
            throughput_weight_ppm,
            realtime_weight_ppm: 0,
        }
    }
}

/// Stable round-robin tie break without indexing when the candidate set is empty.
pub(crate) fn cyclic_cursor_distance(position: usize, cursor: usize, len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    position.wrapping_add(len).wrapping_sub(cursor % len) % len
}

#[cfg(test)]
#[path = "flow_test.rs"]
mod tests;
