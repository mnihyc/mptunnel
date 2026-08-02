use crate::protocol::StreamDemandHint;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TrafficClass {
    Control,
    Latency,
    Throughput,
    RealtimeDatagram,
    Background,
}

impl TrafficClass {
    /// Latency-sensitive work keeps priority queueing and avoids bulk-path
    /// sharing penalties across every carrier implementation.
    pub(crate) const fn is_latency_sensitive(self) -> bool {
        matches!(self, Self::Control | Self::Latency | Self::RealtimeDatagram)
    }

    /// Bulk traffic classes may trade latency for sustained carrier feeding; the
    /// transport-specific schedulers consume this product classification.
    pub(crate) const fn is_bulk(self) -> bool {
        matches!(self, Self::Throughput | Self::Background)
    }
}

/// Converts local scheduling intent into the transport-neutral wire class.
pub(crate) fn stream_demand_hint_for_traffic_class(
    traffic_class: TrafficClass,
) -> StreamDemandHint {
    match traffic_class {
        TrafficClass::Control | TrafficClass::Latency => StreamDemandHint::latency(),
        TrafficClass::Throughput | TrafficClass::Background => StreamDemandHint::throughput(),
        TrafficClass::RealtimeDatagram => StreamDemandHint::realtime(),
    }
}

/// Recovers the local traffic class from the peer-advertised wire demand.
pub(crate) fn traffic_class_from_stream_demand_hint(demand: StreamDemandHint) -> TrafficClass {
    match demand {
        StreamDemandHint::Latency => TrafficClass::Latency,
        StreamDemandHint::Throughput => TrafficClass::Throughput,
        StreamDemandHint::Realtime => TrafficClass::RealtimeDatagram,
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
#[path = "tests_traffic.rs"]
mod tests;
