use super::*;

#[test]
fn peer_bulk_hint_seeds_but_does_not_pin_reliable_flow_demand() {
    let limits = MuxLimits::default();
    let mut tracker = ReliableRelayFlowDemandTracker::with_initial_lane(TrafficClass::Throughput);

    let initial = tracker.refresh(ReliableRelayFlowSignals::new(0, 0), None, limits);
    assert_eq!(initial.lane, TrafficClass::Throughput);

    tracker.last_progress_at = Instant::now() - reliable_flow_interactive_idle_gap(None);
    let idle = tracker.refresh(ReliableRelayFlowSignals::new(0, 0), None, limits);
    assert_eq!(
        idle.lane,
        TrafficClass::Latency,
        "a peer's open-time hint is startup evidence, not a permanent link or stream tag"
    );
}

#[test]
fn historical_bulk_volume_does_not_override_live_idleness() {
    let limits = MuxLimits::default();
    let bulk_bytes = reliable_flow_bulk_threshold_bytes(None, limits).max(
        reliable_flow_rate_bulk_evidence_bytes(None, limits, u64::MAX),
    );
    let mut tracker = ReliableRelayFlowDemandTracker::new();

    let active = tracker.refresh(ReliableRelayFlowSignals::new(bulk_bytes, 0), None, limits);
    assert_eq!(active.lane, TrafficClass::Throughput);

    tracker.last_progress_at = Instant::now() - reliable_flow_interactive_idle_gap(None);
    let idle = tracker.refresh(ReliableRelayFlowSignals::new(bulk_bytes, 0), None, limits);
    assert_eq!(
        idle.lane,
        TrafficClass::Latency,
        "cumulative bulk evidence must not pin an inactive flow to throughput"
    );

    let resumed = tracker.refresh(
        ReliableRelayFlowSignals::new(bulk_bytes.saturating_add(1), 0),
        None,
        limits,
    );
    assert_eq!(
        resumed.lane,
        TrafficClass::Latency,
        "one later byte is fresh interactive demand, not proof that the old bulk epoch resumed"
    );

    let reproven = tracker.refresh(
        ReliableRelayFlowSignals::new(bulk_bytes.saturating_mul(2), 0),
        None,
        limits,
    );
    assert_eq!(reproven.lane, TrafficClass::Throughput);
}

#[test]
fn pending_product_work_preserves_but_cannot_alone_create_bulk_demand() {
    let limits = MuxLimits::default();
    let bulk_bytes = reliable_flow_bulk_threshold_bytes(None, limits).max(
        reliable_flow_rate_bulk_evidence_bytes(None, limits, u64::MAX),
    );
    let mut bulk = ReliableRelayFlowDemandTracker::new();
    assert_eq!(
        bulk.refresh(ReliableRelayFlowSignals::new(bulk_bytes, 0), None, limits,)
            .lane,
        TrafficClass::Throughput
    );

    bulk.last_progress_at = Instant::now() - reliable_flow_interactive_idle_gap(None);
    let backpressured = bulk.refresh(
        ReliableRelayFlowSignals::new(bulk_bytes, 0)
            .with_product_work(0, reliable_relay_buffer_len(limits)),
        None,
        limits,
    );
    assert_eq!(
        backpressured.lane,
        TrafficClass::Throughput,
        "queued or unacknowledged product bytes are active work, not application idleness"
    );

    let drained = bulk.refresh(ReliableRelayFlowSignals::new(bulk_bytes, 0), None, limits);
    assert_eq!(drained.lane, TrafficClass::Latency);

    let mut latency = ReliableRelayFlowDemandTracker::new();
    latency.last_progress_at = Instant::now() - reliable_flow_interactive_idle_gap(None);
    let recovery_only = latency.refresh(
        ReliableRelayFlowSignals::new(0, 0).with_product_work(0, reliable_relay_buffer_len(limits)),
        None,
        limits,
    );
    assert_eq!(
        recovery_only.lane,
        TrafficClass::Latency,
        "pending recovery may preserve established demand but must not manufacture bulk intent"
    );
}

#[test]
fn direction_switch_contributes_fresh_flow_evidence() {
    let limits = MuxLimits::default();
    let floor = reliable_flow_rate_bulk_evidence_bytes(None, limits, u64::MAX);
    let first_direction = floor / 2;
    let second_direction = floor.saturating_sub(first_direction);
    let mut tracker = ReliableRelayFlowDemandTracker::new();

    let first = tracker.refresh(
        ReliableRelayFlowSignals::new(first_direction, 0),
        None,
        limits,
    );
    assert_eq!(first.lane, TrafficClass::Latency);
    let switched = tracker.refresh(
        ReliableRelayFlowSignals::new(first_direction, second_direction),
        None,
        limits,
    );
    assert_eq!(switched.lane, TrafficClass::Throughput);
}

#[test]
fn flow_demand_rebalances_repeatedly_during_sustained_bulk() {
    let mut tracker = ReliableRelayFlowDemandTracker::new();
    let limits = MuxLimits::default();
    let first = tracker.refresh(
        ReliableRelayFlowSignals::new(reliable_relay_buffer_len(limits) as u64 * 4, 0),
        None,
        limits,
    );

    assert!(first.promoted_to_throughput);
    assert!(tracker.should_rebalance(first));
    tracker.mark_rebalance_attempted();

    let immediate = tracker.refresh(
        ReliableRelayFlowSignals::new(reliable_relay_buffer_len(limits) as u64 * 5, 0),
        None,
        limits,
    );
    assert!(!immediate.promoted_to_throughput);
    assert!(!tracker.should_rebalance(immediate));

    tracker.next_rebalance_at = Instant::now() - Duration::from_millis(1);
    let recurring = tracker.refresh(
        ReliableRelayFlowSignals::new(reliable_relay_buffer_len(limits) as u64 * 6, 0),
        None,
        limits,
    );
    assert!(!recurring.promoted_to_throughput);
    assert!(tracker.should_rebalance(recurring));
}

#[test]
fn rate_evidence_does_not_classify_bulk_before_sustained_demand_floor() {
    let mut tracker = ReliableRelayFlowDemandTracker::new();
    let limits = MuxLimits::default();
    let floor = reliable_flow_rate_bulk_evidence_bytes(None, limits, u64::MAX);
    let below_floor = floor.saturating_sub(1).max(1);

    let decision = tracker.refresh(ReliableRelayFlowSignals::new(below_floor, 0), None, limits);

    assert_eq!(decision.lane, TrafficClass::Latency);
    assert!(!decision.promoted_to_throughput);
    assert!(
        decision.preopen_additional_paths,
        "additional-path preparation may start before full throughput classification"
    );
}

#[test]
fn initial_window_response_stays_latency_and_does_not_preopen_additional_paths() {
    let mut tracker = ReliableRelayFlowDemandTracker::new();
    let limits = MuxLimits::default();
    let threshold = reliable_flow_rate_bulk_evidence_bytes(None, limits, u64::MAX);
    assert!(threshold > PATH_OPEN_SCORE_BYTES as u64);

    let decision = tracker.refresh(
        ReliableRelayFlowSignals::new(PATH_OPEN_SCORE_BYTES as u64, 0),
        None,
        limits,
    );

    assert_eq!(decision.lane, TrafficClass::Latency);
    assert!(!decision.promoted_to_throughput);
    assert!(
        !decision.preopen_additional_paths,
        "one initial window is ordinary short-flow traffic, not enough to open an additional bulk path"
    );

    let mut queued = ReliableRelayFlowDemandTracker::new();
    let queued_decision = queued.refresh(
        ReliableRelayFlowSignals::new(PATH_OPEN_SCORE_BYTES as u64, 0)
            .with_product_work(reliable_relay_buffer_len(limits), 0),
        None,
        limits,
    );
    assert_eq!(
        queued_decision.lane,
        TrafficClass::Latency,
        "one queued initial window remains ordinary short-flow traffic"
    );
}

#[test]
fn additional_path_preparation_uses_an_amortized_bulk_floor() {
    let mut tracker = ReliableRelayFlowDemandTracker::new();
    let limits = MuxLimits::default();
    let full_floor = reliable_flow_rate_bulk_evidence_bytes(None, limits, u64::MAX);
    let path_open_floor = reliable_relay_bulk_path_open_threshold_bytes(None, limits);

    assert!(path_open_floor > PATH_OPEN_SCORE_BYTES as u64);
    assert!(
        path_open_floor < full_floor,
        "additional-path preparation starts after several startup windows without waiting for full bulk classification"
    );

    let decision = tracker.refresh(
        ReliableRelayFlowSignals::new(path_open_floor, 0),
        None,
        limits,
    );

    assert_eq!(decision.lane, TrafficClass::Latency);
    assert!(!decision.promoted_to_throughput);
    assert!(decision.preopen_additional_paths);

    let mut outstanding = ReliableRelayFlowDemandTracker::new();
    let outstanding_decision = outstanding.refresh(
        ReliableRelayFlowSignals::new(path_open_floor, 0)
            .with_product_work(0, reliable_relay_buffer_len(limits)),
        None,
        limits,
    );
    assert_eq!(
        outstanding_decision.lane,
        TrafficClass::Latency,
        "Data-ACK-outstanding flight cannot promote throughput"
    );

    let mut residual = ReliableRelayFlowDemandTracker::new();
    let residual_decision = residual.refresh(
        ReliableRelayFlowSignals::new(path_open_floor, 0).with_product_work(1, 0),
        None,
        limits,
    );
    assert_eq!(
        residual_decision.lane,
        TrafficClass::Latency,
        "one residual queued byte is not buffered bulk demand"
    );

    let queued_window = PATH_OPEN_SCORE_BYTES.min(reliable_relay_buffer_len(limits));
    let mut below_window = ReliableRelayFlowDemandTracker::new();
    let below_window_decision = below_window.refresh(
        ReliableRelayFlowSignals::new(path_open_floor, 0)
            .with_product_work(queued_window.saturating_sub(1), 0),
        None,
        limits,
    );
    assert_eq!(below_window_decision.lane, TrafficClass::Latency);

    for mut queued in [
        ReliableRelayFlowDemandTracker::new(),
        ReliableRelayFlowDemandTracker::new(),
    ] {
        let queued_decision = queued.refresh(
            ReliableRelayFlowSignals::new(path_open_floor, 0).with_product_work(queued_window, 0),
            None,
            limits,
        );
        assert_eq!(queued_decision.lane, TrafficClass::Throughput);
        assert!(queued_decision.promoted_to_throughput);
    }
}

#[test]
fn sustained_rate_evidence_classifies_the_flow_as_bulk() {
    let mut tracker = ReliableRelayFlowDemandTracker::new();
    let limits = MuxLimits::default();
    let floor = reliable_flow_rate_bulk_evidence_bytes(None, limits, u64::MAX);

    let decision = tracker.refresh(ReliableRelayFlowSignals::new(floor, 0), None, limits);

    assert_eq!(decision.lane, TrafficClass::Throughput);
    assert!(decision.promoted_to_throughput);
    assert!(tracker.should_rebalance(decision));
}

#[test]
fn byte_threshold_classifies_bulk_even_when_average_rate_is_low() {
    let mut tracker = ReliableRelayFlowDemandTracker::new();
    let limits = MuxLimits::default();
    let threshold = reliable_flow_bulk_threshold_bytes(None, limits);
    tracker.epoch_started_at = Some(Instant::now() - Duration::from_secs(60));

    let decision = tracker.refresh(ReliableRelayFlowSignals::new(threshold, 0), None, limits);

    assert_eq!(decision.lane, TrafficClass::Throughput);
    assert!(decision.promoted_to_throughput);
}

#[test]
fn latency_startup_credit_follows_the_directional_demand_episode() {
    let limits = MuxLimits::default();
    let path = PathSnapshot::new(
        crate::protocol::PathId(0),
        crate::protocol::UnderlayProtocol::Tcp,
        120.0,
        300_000_000.0,
    );
    let threshold = reliable_flow_bulk_threshold_bytes(Some(path), limits);
    let rate_floor = reliable_flow_rate_bulk_evidence_bytes(Some(path), limits, threshold);
    assert!(
        threshold > rate_floor,
        "test path must have a BDP above one relay quantum"
    );

    let mut tracker = ReliableRelayFlowDemandTracker::new();
    let historical_offset = threshold;
    let initial_bulk = tracker.refresh(
        ReliableRelayFlowSignals::new(historical_offset, 0),
        Some(path),
        limits,
    );
    assert_eq!(initial_bulk.lane, TrafficClass::Throughput);
    assert_eq!(
        tracker.latency_startup_credit_remaining_bytes(
            TrafficClass::Throughput,
            Some(path),
            limits,
        ),
        usize::MAX
    );

    tracker.last_progress_at = Instant::now() - reliable_flow_interactive_idle_gap(Some(path));
    let idle = tracker.refresh(
        ReliableRelayFlowSignals::new(historical_offset, 0),
        Some(path),
        limits,
    );
    assert_eq!(idle.lane, TrafficClass::Latency);
    assert_eq!(
        tracker.latency_startup_credit_remaining_bytes(TrafficClass::Latency, Some(path), limits,),
        usize::try_from(threshold).unwrap(),
        "lifetime stream volume must not consume a fresh demand episode's bounded credit"
    );

    // Make byte volume, rather than the independent rate signal, own the
    // remainder of this assertion.
    tracker.epoch_started_at = Some(Instant::now() - Duration::from_secs(60));
    let almost_bulk_offset = historical_offset.saturating_add(threshold.saturating_sub(1));
    let almost_bulk = tracker.refresh(
        ReliableRelayFlowSignals::new(almost_bulk_offset, 0),
        Some(path),
        limits,
    );
    assert_eq!(almost_bulk.lane, TrafficClass::Latency);

    assert_eq!(
        tracker.latency_startup_credit_remaining_bytes(TrafficClass::Latency, Some(path), limits,),
        1
    );

    let reproven = tracker.refresh(
        ReliableRelayFlowSignals::new(almost_bulk_offset.saturating_add(1), 0),
        Some(path),
        limits,
    );
    assert_eq!(reproven.lane, TrafficClass::Throughput);
    assert_eq!(
        tracker.latency_startup_credit_remaining_bytes(
            TrafficClass::Throughput,
            Some(path),
            limits,
        ),
        usize::MAX
    );
}
