use super::{
    FlowLane, cyclic_cursor_distance, flow_lane_from_stream_demand_hint,
    stream_demand_hint_for_lane,
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
    let throughput = stream_demand_hint_for_lane(FlowLane::Throughput);
    assert_eq!(
        flow_lane_from_stream_demand_hint(throughput),
        FlowLane::Throughput
    );

    let tie_break_to_throughput = StreamDemandHint {
        latency_weight_ppm: 500_000,
        throughput_weight_ppm: 500_000,
        ..StreamDemandHint::latency()
    };
    assert_eq!(
        flow_lane_from_stream_demand_hint(tie_break_to_throughput),
        FlowLane::Throughput
    );

    let latency = stream_demand_hint_for_lane(FlowLane::Latency);
    assert_eq!(
        flow_lane_from_stream_demand_hint(latency),
        FlowLane::Latency
    );
}
