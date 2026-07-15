use super::*;

#[test]
fn flow_demand_rebalances_repeatedly_during_sustained_bulk() {
    let mut tracker = ReliableRelayFlowDemandTracker::new();
    let limits = MuxLimits::default();
    let first = tracker.refresh(
        ReliableRelayFlowSignals::new(reliable_relay_buffer_len(limits) as u64 * 4, 0, 0),
        None,
        limits,
    );

    assert!(first.promoted_to_throughput);
    assert!(tracker.should_rebalance(first));
    tracker.mark_rebalance_attempted();

    let immediate = tracker.refresh(
        ReliableRelayFlowSignals::new(reliable_relay_buffer_len(limits) as u64 * 5, 0, 0),
        None,
        limits,
    );
    assert!(!immediate.promoted_to_throughput);
    assert!(!tracker.should_rebalance(immediate));

    tracker.next_rebalance_at = Instant::now() - Duration::from_millis(1);
    let recurring = tracker.refresh(
        ReliableRelayFlowSignals::new(reliable_relay_buffer_len(limits) as u64 * 6, 0, 0),
        None,
        limits,
    );
    assert!(!recurring.promoted_to_throughput);
    assert!(tracker.should_rebalance(recurring));
}

#[test]
fn rate_evidence_does_not_promote_before_service_quantum_floor() {
    let mut tracker = ReliableRelayFlowDemandTracker::new();
    let limits = MuxLimits::default();
    let floor = reliable_flow_rate_bulk_evidence_bytes(None, limits, u64::MAX);
    let below_floor = floor.saturating_sub(1).max(1);

    let decision = tracker.refresh(
        ReliableRelayFlowSignals::new(below_floor, 0, 0),
        None,
        limits,
    );

    assert_eq!(decision.lane, FlowLane::Latency);
    assert!(!decision.promoted_to_throughput);
    assert!(
        decision.prevalidate_bulk,
        "amortized bulk validation should be allowed before full throughput promotion"
    );
}

#[test]
fn initial_window_response_stays_latency_and_does_not_prevalidate_bulk() {
    let mut tracker = ReliableRelayFlowDemandTracker::new();
    let limits = MuxLimits::default();
    let threshold = reliable_flow_rate_bulk_evidence_bytes(None, limits, u64::MAX);
    assert!(threshold > PATH_OPEN_SCORE_BYTES as u64);

    let decision = tracker.refresh(
        ReliableRelayFlowSignals::new(PATH_OPEN_SCORE_BYTES as u64, 0, 0),
        None,
        limits,
    );

    assert_eq!(decision.lane, FlowLane::Latency);
    assert!(!decision.promoted_to_throughput);
    assert!(
        !decision.prevalidate_bulk,
        "one initial window is ordinary short-flow traffic, not enough to spawn per-stream bulk validation"
    );
}

#[test]
fn bulk_prevalidation_uses_amortized_floor_before_throughput_promotion() {
    let mut tracker = ReliableRelayFlowDemandTracker::new();
    let limits = MuxLimits::default();
    let full_floor = reliable_flow_rate_bulk_evidence_bytes(None, limits, u64::MAX);
    let prevalidation_floor = reliable_relay_bulk_prevalidation_threshold_bytes(None, limits);

    assert!(prevalidation_floor > PATH_OPEN_SCORE_BYTES as u64);
    assert!(
        prevalidation_floor < full_floor,
        "prevalidation should start after several startup windows, not wait for the full throughput promotion floor"
    );

    let decision = tracker.refresh(
        ReliableRelayFlowSignals::new(prevalidation_floor, 0, 0),
        None,
        limits,
    );

    assert_eq!(decision.lane, FlowLane::Latency);
    assert!(!decision.promoted_to_throughput);
    assert!(decision.prevalidate_bulk);
}

#[test]
fn rate_evidence_promotes_after_service_quantum_floor() {
    let mut tracker = ReliableRelayFlowDemandTracker::new();
    let limits = MuxLimits::default();
    let floor = reliable_flow_rate_bulk_evidence_bytes(None, limits, u64::MAX);

    let decision = tracker.refresh(ReliableRelayFlowSignals::new(floor, 0, 0), None, limits);

    assert_eq!(decision.lane, FlowLane::Throughput);
    assert!(decision.promoted_to_throughput);
    assert!(tracker.should_rebalance(decision));
}

#[test]
fn latency_startup_owner_credit_stops_after_bulk_evidence_floor() {
    let limits = MuxLimits::default();
    let floor = reliable_flow_rate_bulk_evidence_bytes(None, limits, u64::MAX);

    assert_eq!(
        reliable_latency_startup_owner_credit_remaining_bytes(
            FlowLane::Latency,
            floor.saturating_sub(1),
            0,
            limits,
        ),
        1
    );
    assert_eq!(
        reliable_latency_startup_owner_credit_remaining_bytes(FlowLane::Latency, floor, 0, limits,),
        0
    );
    assert_eq!(
        reliable_latency_startup_owner_credit_remaining_bytes(
            FlowLane::Latency,
            floor / 2,
            usize::try_from(floor - floor / 2).unwrap(),
            limits,
        ),
        0
    );
    assert_eq!(
        reliable_latency_startup_owner_credit_remaining_bytes(
            FlowLane::Throughput,
            floor,
            usize::MAX,
            limits,
        ),
        usize::MAX
    );
}
