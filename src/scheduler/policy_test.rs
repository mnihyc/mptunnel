use super::*;

fn mbps(value: f64) -> f64 {
    value * 1_000_000.0
}

#[test]
fn heterogeneous_links_send_interactive_to_low_latency_path() {
    let low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(30.0));
    let high_bandwidth = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(300.0));
    let mut unstable = PathSnapshot::new(PathId(2), UnderlayProtocol::Udp, 80.0, mbps(100.0));
    unstable.loss_rate = 0.08;
    unstable.jitter_ms = 30.0;

    let choice = choose_path(
        &[low_latency, high_bandwidth, unstable],
        FlowLane::Latency,
        2 * 1024,
    );

    assert_eq!(choice.map(|score| score.path_id), Some(PathId(0)));
}

#[test]
fn latency_scoring_uses_metrics_not_tcp_udp_family_penalty() {
    let lower_latency_tcp = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 10.0, mbps(100.0));
    let higher_latency_udp = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 18.0, mbps(100.0));

    let choice = choose_path(
        &[lower_latency_tcp, higher_latency_udp],
        FlowLane::RealtimeDatagram,
        512,
    );

    assert_eq!(
        choice.map(|score| score.path_id),
        Some(PathId(0)),
        "realtime/latency carrier choice must follow link metrics, not a hardcoded TCP penalty"
    );
}

#[test]
fn heterogeneous_links_send_large_bulk_to_high_bandwidth_path() {
    let low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(30.0));
    let high_bandwidth = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(300.0));
    let mut unstable = PathSnapshot::new(PathId(2), UnderlayProtocol::Udp, 80.0, mbps(100.0));
    unstable.loss_rate = 0.08;
    unstable.jitter_ms = 30.0;

    let choice = choose_path(
        &[low_latency, high_bandwidth, unstable],
        FlowLane::Throughput,
        4 * 1024 * 1024,
    );

    assert_eq!(choice.map(|score| score.path_id), Some(PathId(1)));
}

#[test]
fn throughput_scoring_accounts_for_active_bulk_flow_sharing() {
    let mut busy_low_latency =
        PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 20.0, mbps(200.0));
    busy_low_latency.active_flows = 3;
    let independent = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 80.0, mbps(200.0));

    let choice = choose_path(
        &[busy_low_latency, independent],
        FlowLane::Throughput,
        4 * 1024 * 1024,
    );

    assert_eq!(choice.map(|score| score.path_id), Some(PathId(1)));
}

#[test]
fn throughput_scoring_does_not_divide_per_flow_goodput_again() {
    let mut measured = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 20.0, mbps(200.0));
    measured.active_flows = 3;
    measured.rate_scope = PathRateScope::PerFlowGoodput;
    measured.pacing_rate_bps = mbps(600.0);

    assert_eq!(
        effective_path_rate_bps(measured, FlowLane::Throughput),
        mbps(200.0)
    );
}

#[test]
fn failed_and_draining_paths_are_not_schedulable() {
    let mut failed = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 1.0, mbps(1000.0));
    failed.state = PathState::Failed;
    let mut draining = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 1.0, mbps(1000.0));
    draining.state = PathState::Draining;
    let active = PathSnapshot::new(PathId(2), UnderlayProtocol::Udp, 50.0, mbps(10.0));

    let choice = choose_path(&[failed, draining, active], FlowLane::Latency, 512);

    assert_eq!(choice.map(|score| score.path_id), Some(PathId(2)));
}

#[test]
fn latency_sensitive_streams_validate_suspect_low_latency_path() {
    let mut low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 20.0, mbps(100.0));
    low_latency.state = PathState::Suspect;
    let active_high_latency =
        PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 180.0, mbps(100.0));

    let interactive = choose_path(&[low_latency, active_high_latency], FlowLane::Latency, 512);
    let bulk = choose_path(
        &[low_latency, active_high_latency],
        FlowLane::Throughput,
        4 * 1024 * 1024,
    );

    assert_eq!(interactive.map(|score| score.path_id), Some(PathId(0)));
    assert_eq!(bulk.map(|score| score.path_id), Some(PathId(1)));
}
