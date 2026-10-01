use super::*;
use crate::protocol::{PathId, UnderlayProtocol};

fn mbps(value: f64) -> f64 {
    value * 1_000_000.0
}

#[test]
fn heterogeneous_links_send_interactive_to_low_latency_path() {
    let low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(30.0));
    let high_bandwidth = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(300.0));
    let mut unstable = PathSnapshot::new(PathId(2), UnderlayProtocol::Udp, 80.0, mbps(100.0));
    unstable.loss_rate = 0.08;
    unstable.set_timing(crate::model::timing::PathTiming::startup(
        unstable.peer_timing().srtt_ms(),
        30.0,
    ));

    let choice = choose_path(
        &[low_latency, high_bandwidth, unstable],
        TrafficClass::Latency,
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
        TrafficClass::RealtimeDatagram,
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
    unstable.set_timing(crate::model::timing::PathTiming::startup(
        unstable.peer_timing().srtt_ms(),
        30.0,
    ));

    let choice = choose_path(
        &[low_latency, high_bandwidth, unstable],
        TrafficClass::Throughput,
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
        TrafficClass::Throughput,
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
        policy::effective_path_rate_bps(measured.completion(), TrafficClass::Throughput),
        mbps(200.0)
    );
}

#[test]
fn completion_scoring_does_not_treat_raw_pacing_as_delivered_capacity() {
    let mut measured = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 100.0, mbps(50.0));
    measured.rate_scope = PathRateScope::PathCapacity;
    measured.pacing_rate_bps = mbps(1_000.0);

    assert_eq!(
        policy::effective_path_rate_bps(measured.completion(), TrafficClass::Throughput),
        mbps(50.0)
    );
}

#[test]
fn completion_scoring_counts_queues_but_not_data_ack_ownership() {
    let idle = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 20.0, mbps(100.0));
    let mut data_ack_owned = idle;
    data_ack_owned.data_level_bytes_in_flight = 8 * 1024 * 1024;

    let idle_score = score_path(idle, TrafficClass::RealtimeDatagram, 512).expect("idle score");
    let owned_score = score_path(data_ack_owned, TrafficClass::RealtimeDatagram, 512)
        .expect("Data ACK ownership score");
    assert_eq!(owned_score.eta_ms, idle_score.eta_ms);

    let mut queued = idle;
    queued.data_level_queue_bytes = 256 * 1024;
    queued.queue_bytes = 512 * 1024;
    queued.bytes_in_flight = 768 * 1024;
    let queued_score =
        score_path(queued, TrafficClass::RealtimeDatagram, 512).expect("queued score");
    assert!(queued_score.eta_ms > idle_score.eta_ms);
}

#[test]
fn ordered_bulk_completion_includes_the_data_ack_frontier() {
    let idle = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 20.0, mbps(100.0));
    let mut outstanding = idle;
    outstanding.data_level_bytes_in_flight = 8 * 1024 * 1024;

    let idle_score = score_path(idle, TrafficClass::Throughput, 64 * 1024).expect("idle score");
    let outstanding_score =
        score_path(outstanding, TrafficClass::Throughput, 64 * 1024).expect("outstanding score");

    assert!(outstanding_score.eta_ms > idle_score.eta_ms);
}

#[test]
fn realtime_selection_is_not_diverted_by_data_ack_ownership() {
    let mut low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 40.0, mbps(100.0));
    low_latency.data_level_bytes_in_flight = 8 * 1024 * 1024;
    let high_latency = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 320.0, mbps(100.0));

    let choice = choose_path(
        &[low_latency, high_latency],
        TrafficClass::RealtimeDatagram,
        1_200,
    );

    assert_eq!(choice.map(|score| score.path_id), Some(PathId(0)));
}

#[test]
fn failed_and_draining_paths_are_not_schedulable() {
    let mut failed = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 1.0, mbps(1000.0));
    failed.state = PathState::Failed;
    let mut draining = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 1.0, mbps(1000.0));
    draining.state = PathState::Draining;
    let active = PathSnapshot::new(PathId(2), UnderlayProtocol::Udp, 50.0, mbps(10.0));

    let choice = choose_path(&[failed, draining, active], TrafficClass::Latency, 512);

    assert_eq!(choice.map(|score| score.path_id), Some(PathId(2)));
}

#[test]
fn latency_sensitive_streams_validate_suspect_low_latency_path() {
    let mut low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 20.0, mbps(100.0));
    low_latency.state = PathState::Suspect;
    let active_high_latency =
        PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 180.0, mbps(100.0));

    let interactive = choose_path(
        &[low_latency, active_high_latency],
        TrafficClass::Latency,
        512,
    );
    let bulk = choose_path(
        &[low_latency, active_high_latency],
        TrafficClass::Throughput,
        4 * 1024 * 1024,
    );

    assert_eq!(interactive.map(|score| score.path_id), Some(PathId(0)));
    assert_eq!(bulk.map(|score| score.path_id), Some(PathId(1)));
}

#[test]
fn material_completion_advantage_uses_eta_beyond_timing_uncertainty() {
    let blocked = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 100.0, mbps(100.0));
    let mut available = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 50.0, mbps(100.0));
    available.queue_bytes = 1024 * 1024;

    assert!(
        !path_has_material_completion_advantage(100.0, blocked, 50.0, available),
        "a clearly earlier available path remains work-conserving even with a longer queue",
    );
    assert!(
        path_has_material_completion_advantage(50.0, blocked, 100.0, available),
        "ETA already accounts for queue work, so the material lead needs no second queue test",
    );

    let mut queued_blocked = blocked;
    queued_blocked.queue_bytes = 2 * 1024 * 1024;
    assert!(
        path_has_material_completion_advantage(
            666.0,
            queued_blocked,
            1_155.0,
            PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 50.0, mbps(100.0)),
        ),
        "reducing the available raw queue cannot reverse a still-material ETA disadvantage",
    );
}

#[test]
fn split_tcp_completion_uses_peer_timing_without_rewriting_transport_shape() {
    let mut slow = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 0.1, mbps(100.0));
    let mut fast = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 20.0, mbps(100.0));
    slow.set_timing(crate::model::timing::PathTiming::tcp(
        slow.transport_timing(),
        crate::model::timing::PeerTiming::new(160.0, 0.0),
    ));
    fast.set_timing(crate::model::timing::PathTiming::tcp(
        fast.transport_timing(),
        crate::model::timing::PeerTiming::new(20.0, 0.0),
    ));
    let native_timing = slow.transport_timing();
    for lane in [
        TrafficClass::Control,
        TrafficClass::Latency,
        TrafficClass::Throughput,
        TrafficClass::RealtimeDatagram,
    ] {
        assert_eq!(
            choose_path(&[slow, fast], lane, 64 * 1024).unwrap().path_id,
            fast.id
        );
    }
    assert_eq!(slow.transport_timing().srtt_ms(), 0.1);
    assert_eq!(slow.transport_timing(), native_timing);
    assert!(!path_has_material_completion_advantage(79.0, slow, 80.0, {
        fast.set_timing(crate::model::timing::PathTiming::tcp(
            fast.transport_timing(),
            crate::model::timing::PeerTiming::new(20.0, 2.0),
        ));
        fast
    }));
}
