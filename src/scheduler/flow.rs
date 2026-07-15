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

/// Converts local scheduling intent into the transport-neutral wire class.
pub(crate) fn stream_demand_hint_for_lane(lane: FlowLane) -> StreamDemandHint {
    match lane {
        FlowLane::Control | FlowLane::Latency => StreamDemandHint::latency(),
        FlowLane::Throughput | FlowLane::Background => StreamDemandHint::throughput(),
        FlowLane::RealtimeDatagram => StreamDemandHint::realtime(),
    }
}

/// Recovers the local lane from the peer-advertised wire class.
pub(crate) fn flow_lane_from_stream_demand_hint(demand: StreamDemandHint) -> FlowLane {
    match demand {
        StreamDemandHint::Latency => FlowLane::Latency,
        StreamDemandHint::Throughput => FlowLane::Throughput,
        StreamDemandHint::Realtime => FlowLane::RealtimeDatagram,
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
