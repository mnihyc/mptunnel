use super::*;

fn mbps(value: f64) -> f64 {
    value * 1_000_000.0
}

#[test]
fn heterogeneous_links_send_interactive_to_low_latency_path() {
    let mut low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(30.0));
    low_latency.flags.low_latency = true;
    let high_bandwidth = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(300.0));
    let mut unstable = PathSnapshot::new(PathId(2), UnderlayProtocol::Udp, 80.0, mbps(100.0));
    unstable.loss_rate = 0.08;
    unstable.jitter_ms = 30.0;

    let choice = choose_path(
        &[low_latency, high_bandwidth, unstable],
        FlowLane::Latency,
        2 * 1024,
        SchedulerPolicy::default(),
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
        SchedulerPolicy::default(),
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
        SchedulerPolicy::default(),
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
        SchedulerPolicy::default(),
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

    let choice = choose_path(
        &[failed, draining, active],
        FlowLane::Latency,
        512,
        SchedulerPolicy::default(),
    );

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
        FlowLane::Latency,
        512,
        SchedulerPolicy::default(),
    );
    let bulk = choose_path(
        &[low_latency, active_high_latency],
        FlowLane::Throughput,
        4 * 1024 * 1024,
        SchedulerPolicy::default(),
    );

    assert_eq!(interactive.map(|score| score.path_id), Some(PathId(0)));
    assert_eq!(bulk.map(|score| score.path_id), Some(PathId(1)));
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

    let decision = scheduler
        .schedule_next(&paths, SchedulerPolicy::default())
        .expect("decision");

    assert_eq!(decision.lane, FlowLane::Control);
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

    let first = scheduler
        .schedule_next(&paths, SchedulerPolicy::default())
        .expect("first");
    let second = scheduler
        .schedule_next(&paths, SchedulerPolicy::default())
        .expect("second");

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

    let decision = scheduler
        .schedule_next(&paths, SchedulerPolicy::default())
        .expect("decision");

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
        .schedule_next(&[low_latency, high_bandwidth], SchedulerPolicy::default())
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

    let decision = scheduler
        .schedule_next(&[first, second], SchedulerPolicy::default())
        .expect("decision");

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
        .schedule_next(
            &[preferred, busy_peer, independent],
            SchedulerPolicy::default(),
        )
        .expect("decision");

    assert_eq!(decision.path_id, PathId(2));
}
