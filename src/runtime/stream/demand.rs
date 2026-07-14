//! Translation between scheduler lanes and stream-open demand hints.
//!
//! The scheduler owns local lane names while the protocol owns portable demand
//! weights; this boundary keeps either representation from becoming the other.

use crate::protocol::StreamDemandHint;
use crate::scheduler::FlowLane;

pub(in crate::runtime) fn stream_demand_hint_for_lane(lane: FlowLane) -> StreamDemandHint {
    match lane {
        FlowLane::Control | FlowLane::Latency => StreamDemandHint::latency(),
        FlowLane::Throughput | FlowLane::Background => StreamDemandHint::throughput(),
        FlowLane::RealtimeDatagram => StreamDemandHint::realtime(),
    }
}

pub(in crate::runtime) fn flow_lane_from_stream_demand_hint(demand: StreamDemandHint) -> FlowLane {
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

#[cfg(test)]
#[path = "demand_test.rs"]
mod tests;
