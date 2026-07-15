use super::*;
use crate::protocol::UnderlayProtocol;

fn mbps(value: f64) -> f64 {
    value * 1_000_000.0
}

#[test]
fn heterogeneous_scheduler_prioritizes_control_over_bulk_queue() {
    let paths = [
        PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(100.0)),
        PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 40.0, mbps(100.0)),
    ];
    let mut scheduler = HeterogeneousScheduler::default();
    scheduler.enqueue(EnqueueRequest {
        flow_id: FlowId(1),
        lane: FlowLane::Throughput,
        payload_bytes: 128 * 1024,
        remaining_flow_bytes: 8 * 1024 * 1024,
        duplicate_eligible: false,
    });
    scheduler.enqueue(EnqueueRequest {
        flow_id: FlowId(2),
        lane: FlowLane::Control,
        payload_bytes: 512,
        remaining_flow_bytes: 512,
        duplicate_eligible: true,
    });

    let decision = scheduler.schedule_next(&paths).expect("decision");

    assert_eq!(decision.flow_id, FlowId(2));
}

#[test]
fn heterogeneous_scheduler_round_robins_bulk_flows_with_deficit() {
    let paths = [PathSnapshot::new(
        PathId(0),
        UnderlayProtocol::Udp,
        20.0,
        mbps(100.0),
    )];
    let mut scheduler = HeterogeneousScheduler::default();
    for flow_id in [FlowId(1), FlowId(2)] {
        scheduler.enqueue(EnqueueRequest {
            flow_id,
            lane: FlowLane::Throughput,
            payload_bytes: 128 * 1024,
            remaining_flow_bytes: 4 * 1024 * 1024,
            duplicate_eligible: false,
        });
    }

    let first = scheduler.schedule_next(&paths).expect("first");
    let second = scheduler.schedule_next(&paths).expect("second");

    assert_ne!(first.flow_id, second.flow_id);
}

#[test]
fn heterogeneous_scheduler_schedules_oversized_logical_chunk_with_actual_queue_charge() {
    let paths = [PathSnapshot::new(
        PathId(0),
        UnderlayProtocol::Udp,
        20.0,
        mbps(100.0),
    )];
    let mut scheduler = HeterogeneousScheduler::default();
    scheduler.enqueue(EnqueueRequest {
        flow_id: FlowId(3),
        lane: FlowLane::Throughput,
        payload_bytes: 16 * 1024 * 1024,
        remaining_flow_bytes: 16 * 1024 * 1024,
        duplicate_eligible: false,
    });

    let decision = scheduler.schedule_next(&paths).expect("decision");

    assert_eq!(decision.flow_id, FlowId(3));
    assert_eq!(scheduler.queued_path_bytes(PathId(0)), 16 * 1024 * 1024);
}

#[test]
fn heterogeneous_scheduler_switches_bulk_tail_to_latency_sensitive_mode() {
    let low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(50.0));
    let high_bandwidth = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(300.0));
    let mut scheduler = HeterogeneousScheduler::default();
    scheduler.enqueue(EnqueueRequest {
        flow_id: FlowId(7),
        lane: FlowLane::Throughput,
        payload_bytes: 128 * 1024,
        remaining_flow_bytes: 128 * 1024,
        duplicate_eligible: false,
    });

    let decision = scheduler
        .schedule_next(&[low_latency, high_bandwidth])
        .expect("decision");

    assert_eq!(decision.mode, SchedulingMode::TailAvoidance);
    assert_eq!(decision.scheduled_lane, FlowLane::Latency);
    assert_eq!(decision.path_id, PathId(0));
}

#[test]
fn heterogeneous_scheduler_duplicates_small_control_packets_when_cheap() {
    let first = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(100.0));
    let second = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 24.0, mbps(100.0));
    let mut scheduler = HeterogeneousScheduler::default();
    scheduler.enqueue(EnqueueRequest {
        flow_id: FlowId(9),
        lane: FlowLane::Control,
        payload_bytes: 512,
        remaining_flow_bytes: 512,
        duplicate_eligible: true,
    });

    let decision = scheduler.schedule_next(&[first, second]).expect("decision");

    assert_eq!(decision.path_id, PathId(0));
    assert_eq!(decision.duplicate_path_id, Some(PathId(1)));
    assert_eq!(scheduler.queued_path_bytes(PathId(0)), 512);
    assert_eq!(scheduler.queued_path_bytes(PathId(1)), 512);
}

#[test]
fn heterogeneous_scheduler_penalizes_suspected_shared_bottleneck() {
    let preferred = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 10.0, mbps(100.0));
    let mut busy_peer = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 12.0, mbps(100.0));
    busy_peer.queue_bytes = 1024 * 1024;
    let independent = PathSnapshot::new(PathId(2), UnderlayProtocol::Udp, 24.0, mbps(100.0));
    let mut scheduler = HeterogeneousScheduler::default();
    scheduler.enqueue(EnqueueRequest {
        flow_id: FlowId(10),
        lane: FlowLane::Latency,
        payload_bytes: 1024,
        remaining_flow_bytes: 1024,
        duplicate_eligible: false,
    });

    let decision = scheduler
        .schedule_next(&[preferred, busy_peer, independent])
        .expect("decision");

    assert_eq!(decision.path_id, PathId(2));
}
