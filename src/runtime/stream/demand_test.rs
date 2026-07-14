use super::*;

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
