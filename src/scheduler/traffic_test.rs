use super::{
    TrafficClass, cyclic_cursor_distance, stream_demand_hint_for_traffic_class,
    traffic_class_from_stream_demand_hint,
};
use crate::protocol::StreamDemandHint;

#[test]
fn cyclic_cursor_distance_handles_empty_wrap_and_order() {
    assert_eq!(cyclic_cursor_distance(usize::MAX, usize::MAX, 0), 0);
    assert_eq!(cyclic_cursor_distance(2, 2, 4), 0);
    assert_eq!(cyclic_cursor_distance(3, 2, 4), 1);
    assert_eq!(cyclic_cursor_distance(0, 2, 4), 2);
    assert_eq!(cyclic_cursor_distance(1, 2, 4), 3);
    assert_eq!(cyclic_cursor_distance(1, 6, 4), 3);
}

#[test]
fn stream_open_demand_hint_preserves_aggressive_bulk_intent() {
    let throughput = stream_demand_hint_for_traffic_class(TrafficClass::Throughput);
    assert_eq!(
        traffic_class_from_stream_demand_hint(throughput),
        TrafficClass::Throughput
    );

    let latency = stream_demand_hint_for_traffic_class(TrafficClass::Latency);
    assert_eq!(
        traffic_class_from_stream_demand_hint(latency),
        TrafficClass::Latency
    );

    assert_eq!(
        traffic_class_from_stream_demand_hint(StreamDemandHint::Realtime),
        TrafficClass::RealtimeDatagram
    );
}
