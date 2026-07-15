use super::*;
use crate::protocol::UnderlayProtocol;
use crate::scheduler::PathSnapshot;

fn mbps(value: f64) -> f64 {
    value * 1_000_000.0
}

fn assert_between(value: f64, min: f64, max: f64) {
    assert!(
        value >= min && value <= max,
        "{value} is outside [{min}, {max}]"
    );
}

#[test]
fn simulator_keeps_interactive_traffic_off_bulk_queue() {
    let low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(30.0));
    let high_bandwidth = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(300.0));
    let mut simulator = Simulator::new(vec![
        VirtualPath::new(low_latency),
        VirtualPath::new(high_bandwidth),
    ]);

    let bulk = simulator
        .route(FlowLane::Throughput, 16 * 1024 * 1024)
        .expect("bulk route");
    let interactive = simulator
        .route(FlowLane::Latency, 1024)
        .expect("interactive route");

    assert_eq!(bulk.path_id, PathId(1));
    assert_eq!(interactive.path_id, PathId(0));
}

#[test]
fn simulator_failure_injection_removes_dead_path() {
    let fast = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(100.0));
    let slow = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 120.0, mbps(100.0));
    let mut simulator = Simulator::new(vec![
        VirtualPath::new(fast).with_failure_at(100.0),
        VirtualPath::new(slow),
    ]);

    simulator.advance_to(100.0);
    let send = simulator
        .route(FlowLane::Latency, 512)
        .expect("survivor route");

    assert_eq!(send.path_id, PathId(1));
    assert_eq!(simulator.paths()[0].snapshot.state, PathState::Failed);
}

#[test]
fn simulator_bulk_transfer_tracks_aggregation_efficiency() {
    let low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(30.0));
    let high_bandwidth = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(300.0));
    let mut simulator = Simulator::new(vec![
        VirtualPath::new(low_latency),
        VirtualPath::new(high_bandwidth),
    ]);

    let transfer = simulator
        .schedule_transfer(FlowLane::Throughput, 64 * 1024 * 1024, 1024 * 1024)
        .expect("bulk transfer");
    let path_bytes = transfer.path_bytes();

    assert!(path_bytes.get(&PathId(0)).copied().unwrap_or_default() > 0);
    assert!(path_bytes.get(&PathId(1)).copied().unwrap_or_default() > 0);
    assert_between(transfer.aggregation_efficiency(mbps(1000.0)), 0.70, 1.05);
}

#[test]
fn simulator_reinjects_failed_chunks_and_reports_failover_gap() {
    let fast = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(100.0));
    let slow = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 80.0, mbps(100.0));
    let mut simulator = Simulator::new(vec![
        VirtualPath::new(fast).with_failure_at(40.0),
        VirtualPath::new(slow),
    ]);

    let transfer = simulator
        .schedule_transfer_with_repair(FlowLane::Throughput, 4 * 1024 * 1024, 256 * 1024, 10.0)
        .expect("repaired transfer");
    let gap = transfer.failover_gap_ms.expect("failover gap");

    assert!(transfer.repaired_chunks > 0);
    assert_between(gap, 0.0, 500.0);
    assert!(
        transfer
            .attempts
            .iter()
            .any(|attempt| attempt.repair_attempt > 0 && attempt.path_id == PathId(1))
    );
}

#[test]
fn simulator_measures_interactive_p95_under_bulk_load() {
    let mut low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(30.0));
    low_latency.policy.bulk_allowed = false;
    let high_bandwidth = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(300.0));
    let mut simulator = Simulator::new(vec![
        VirtualPath::new(low_latency),
        VirtualPath::new(high_bandwidth),
    ]);

    simulator
        .schedule_transfer(FlowLane::Throughput, 64 * 1024 * 1024, 1024 * 1024)
        .expect("bulk transfer");
    let burst = simulator
        .route_interactive_burst(1024, 20, 5.0)
        .expect("interactive burst");
    let p95 = burst.p95_latency_ms().expect("p95 latency");

    assert_between(p95, 0.0, 40.0);
    assert!(burst.sends.iter().all(|send| send.path_id == PathId(0)));
}

#[test]
fn simulator_reports_bulk_tail_penalty_for_heterogeneous_paths() {
    let low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(80.0));
    let high_bandwidth = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(300.0));
    let mut simulator = Simulator::new(vec![
        VirtualPath::new(low_latency),
        VirtualPath::new(high_bandwidth),
    ]);

    let transfer = simulator
        .schedule_transfer(FlowLane::Throughput, 32 * 1024 * 1024, 512 * 1024)
        .expect("bulk transfer");

    assert_between(transfer.bulk_tail_penalty_ms(), 0.0, 250.0);
}

#[test]
fn simulator_duplicates_small_control_packets_when_cheap() {
    let first = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(100.0));
    let second = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 24.0, mbps(100.0));
    let mut simulator = Simulator::new(vec![VirtualPath::new(first), VirtualPath::new(second)]);

    let send = simulator
        .route(FlowLane::Control, 512)
        .expect("control route");

    assert_eq!(send.path_id, PathId(0));
    assert_eq!(send.duplicate_path_id, Some(PathId(1)));
}

#[test]
fn simulator_routes_bulk_tail_in_tail_avoidance_mode() {
    let low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(50.0));
    let high_bandwidth = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(300.0));
    let mut simulator = Simulator::new(vec![
        VirtualPath::new(low_latency),
        VirtualPath::new(high_bandwidth),
    ]);

    let send = simulator
        .route_flow(
            FlowId(77),
            FlowLane::Throughput,
            128 * 1024,
            128 * 1024,
            false,
        )
        .expect("tail route");

    assert_eq!(send.mode, SchedulingMode::TailAvoidance);
    assert_eq!(send.scheduled_lane, FlowLane::Latency);
    assert_eq!(send.path_id, PathId(0));
}

#[test]
fn simulator_avoids_suspected_shared_bottleneck_path() {
    let preferred = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 10.0, mbps(100.0));
    let mut busy_peer = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 12.0, mbps(100.0));
    busy_peer.queue_bytes = 1024 * 1024;
    let independent = PathSnapshot::new(PathId(2), UnderlayProtocol::Udp, 24.0, mbps(100.0));
    let mut simulator = Simulator::new(vec![
        VirtualPath::new(preferred),
        VirtualPath::new(busy_peer),
        VirtualPath::new(independent),
    ]);

    let send = simulator
        .route(FlowLane::Latency, 1024)
        .expect("interactive route");

    assert_eq!(send.path_id, PathId(2));
}
