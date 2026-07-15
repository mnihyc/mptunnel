use super::*;
use crate::config::SharedSecret;

fn security() -> SecurityConfig {
    SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    )
}

fn context(paths: &[&str]) -> ClientPathContext {
    ClientPathContext::new(
        paths
            .iter()
            .map(|path| path.parse::<PathSpec>().expect("path spec"))
            .collect(),
        security(),
        ResourceLimits::default(),
    )
    .expect("context")
}

fn register_active_tcp_request_bulk_flows(
    context: &ClientPathContext,
    count: usize,
) -> Vec<ReliableTcpRequestBulkFlowRegistration> {
    (0..count)
        .map(|_| {
            let registration = context.reliable_tcp_request_bulk_flow_registration();
            registration.update(true, Some(UnderlayProtocol::Tcp));
            registration
        })
        .collect()
}

fn data_frame(offset: u64, len: usize) -> Frame {
    Frame::StreamData {
        stream_id: StreamId(7),
        offset,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x5a; len]),
    }
}

fn relay_path(
    underlay: UnderlayProtocol,
    index: usize,
    placement: RelayPathPlacement,
) -> ReliableRelayRemotePath {
    let (commands, _receivers) = reliable_path_command_channels(8);
    ReliableRelayRemotePath {
        path_index: index,
        instance_id: index as u64 + 1,
        placement,
        load_reserved: placement == RelayPathPlacement::Active,
        load_lease: None,
        attached_at: Instant::now(),
        path_proof_id: (placement == RelayPathPlacement::Validation).then_some(index as u64 + 1),
        path_proof_generation: 0,
        stream: ReliablePathStreamHandle {
            stream_id: StreamId(7),
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay,
            max_frame_payload_bytes: 64 * 1024,
            output: ReliablePathStreamOutput::fixed(
                underlay,
                PathId(index as u16),
                commands,
                MuxLimits::default(),
            ),
        },
    }
}

fn mark_bulk_service(context: &ClientPathContext, key: RelayPathKey) {
    context.mark_relay_path_rate_sample(
        key.underlay,
        key.index,
        PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(80))
            .expect("bulk service sample"),
    );
}

fn mark_path_proof(context: &ClientPathContext, key: RelayPathKey, elapsed: Duration) {
    context.mark_relay_path_proof_observation(
        key.underlay,
        key.index,
        PathProofObservation {
            proof_id: key.index as u64 + 1,
            bytes: PATH_OPEN_SCORE_BYTES as u64,
            elapsed,
            sent_at: Instant::now(),
        },
    );
}

#[derive(Default)]
struct RequestSchedulingFixture {
    operation: Option<RequestAckClockOperation>,
    subflows: RequestSubflows,
}

impl RequestSchedulingFixture {
    fn with_operation(mut self, operation: RequestAckClockOperation) -> Self {
        self.operation = Some(operation);
        self
    }

    fn with_rate_proven(mut self, instances: impl IntoIterator<Item = RelayPathInstance>) -> Self {
        for instance in instances {
            self.subflows.get_mut(instance).mark_rate_proven();
        }
        self
    }

    fn with_graduated(mut self, instances: impl IntoIterator<Item = RelayPathInstance>) -> Self {
        for instance in instances {
            self.subflows.get_mut(instance).mark_graduated();
        }
        self
    }

    fn with_ack_clock_first_window(
        mut self,
        instances: impl IntoIterator<Item = RelayPathInstance>,
    ) -> Self {
        for instance in instances {
            self.subflows
                .get_mut(instance)
                .mark_ack_clock_first_window();
        }
        self
    }

    fn with_ack_clock_proven(
        mut self,
        instances: impl IntoIterator<Item = RelayPathInstance>,
    ) -> Self {
        for instance in instances {
            self.subflows.get_mut(instance).mark_ack_clock_proven();
        }
        self
    }

    fn with_tcp_capacity_proven(
        mut self,
        instances: impl IntoIterator<Item = RelayPathInstance>,
    ) -> Self {
        for instance in instances {
            self.subflows.get_mut(instance).mark_tcp_capacity_proven();
        }
        self
    }

    fn with_calibration_spend(
        mut self,
        spent: impl IntoIterator<Item = (RelayPathInstance, u64)>,
    ) -> Self {
        for (instance, bytes) in spent {
            self.subflows
                .get_mut(instance)
                .set_ack_clock_calibration_bytes(bytes);
        }
        self
    }

    fn with_per_flow_rates(
        mut self,
        rates: impl IntoIterator<Item = (RelayPathInstance, RequestPerFlowRateModel)>,
    ) -> Self {
        for (instance, model) in rates {
            self.subflows.get_mut(instance).set_per_flow_rate(model);
        }
        self
    }

    fn state(&self) -> RequestSchedulingState<'_> {
        RequestSchedulingState {
            operation: self.operation,
            subflows: &self.subflows,
        }
    }
}

#[test]
fn request_bulk_flow_registration_counts_only_tcp_service_flows_once() {
    let context = context(&["tcp://127.0.0.1:10079", "udp://127.0.0.1:10080"]);
    assert_eq!(context.active_tcp_service_request_bulk_flows(), 0);

    let first = context.reliable_tcp_request_bulk_flow_registration();
    first.update(true, Some(UnderlayProtocol::Udp));
    assert_eq!(context.active_tcp_service_request_bulk_flows(), 0);
    first.update(true, Some(UnderlayProtocol::Tcp));
    first.update(true, Some(UnderlayProtocol::Tcp));
    assert_eq!(context.active_tcp_service_request_bulk_flows(), 1);

    {
        let second = context.reliable_tcp_request_bulk_flow_registration();
        second.update(true, Some(UnderlayProtocol::Udp));
        assert_eq!(context.active_tcp_service_request_bulk_flows(), 1);
        second.update(true, Some(UnderlayProtocol::Tcp));
        assert_eq!(context.active_tcp_service_request_bulk_flows(), 2);
        second.update(true, Some(UnderlayProtocol::Udp));
        assert_eq!(context.active_tcp_service_request_bulk_flows(), 1);
    }
    assert_eq!(context.active_tcp_service_request_bulk_flows(), 1);

    let shared = first.clone();
    drop(first);
    assert_eq!(
        context.active_tcp_service_request_bulk_flows(),
        1,
        "dropping one publisher must not retire shared flow state"
    );
    drop(shared);
    assert_eq!(context.active_tcp_service_request_bulk_flows(), 0);
}

#[test]
fn request_scheduler_keeps_tcp_ack_rates_flow_local() {
    let context = context(&["tcp://127.0.0.1:10078?rate-mbps=500"]);
    let path = relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active);
    let instance = path.instance();
    mark_bulk_service(&context, instance.key);
    let slow_flow = RequestSchedulingFixture::default().with_per_flow_rates([(
        instance,
        RequestPerFlowRateModel {
            rate_bps: 80_000_000.0,
            delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
        },
    )]);
    let fast_flow = RequestSchedulingFixture::default().with_per_flow_rates([(
        instance,
        RequestPerFlowRateModel {
            rate_bps: 320_000_000.0,
            delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
        },
    )]);

    let slow = relay_path_snapshot_for_bulk_choice(
        &context,
        instance,
        Some(instance.key),
        Some(slow_flow.state()),
        true,
    )
    .expect("slow flow snapshot");
    let fast = relay_path_snapshot_for_bulk_choice(
        &context,
        instance,
        Some(instance.key),
        Some(fast_flow.state()),
        true,
    )
    .expect("fast flow snapshot");

    assert_eq!(slow.delivery_rate_bps, 80_000_000.0);
    assert_eq!(fast.delivery_rate_bps, 320_000_000.0);
    assert_eq!(slow.pacing_rate_bps, slow.delivery_rate_bps);
    assert_eq!(fast.pacing_rate_bps, fast.delivery_rate_bps);
    assert_eq!(slow.rate_scope, PathRateScope::PerFlowGoodput);
    assert_eq!(fast.rate_scope, PathRateScope::PerFlowGoodput);

    let provisional_flow = RequestSchedulingFixture::default().with_per_flow_rates([(
        instance,
        RequestPerFlowRateModel {
            rate_bps: 24_000_000.0,
            delivery_samples: 1,
        },
    )]);
    let provisional = relay_path_snapshot_for_bulk_choice(
        &context,
        instance,
        Some(instance.key),
        Some(provisional_flow.state()),
        true,
    )
    .expect("provisional flow snapshot");
    assert_eq!(provisional.delivery_rate_bps, 500_000_000.0);
    assert_eq!(provisional.rate_scope, PathRateScope::PathCapacity);

    context.mark_relay_path_ack_clock_rate_sample(
        instance.key.underlay,
        instance.key.index,
        PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(32))
            .expect("other-flow ACK sample"),
        true,
    );
    let unowned =
        relay_path_snapshot_for_bulk_choice(&context, instance, Some(instance.key), None, true)
            .expect("flow without local rate snapshot");
    assert_eq!(unowned.delivery_rate_bps, 500_000_000.0);
    assert_eq!(
        unowned.rate_scope,
        PathRateScope::PathCapacity,
        "another flow's TCP goodput must not become this flow's undivided path rate"
    );
}

#[test]
fn provisional_tcp_prior_yields_to_mature_exact_subflow_pipe_credit() {
    let context = context(&[
        "tcp://127.0.0.1:10078?srtt-ms=180&rate-mbps=500",
        "tcp://127.0.0.1:10079?srtt-ms=180&rate-mbps=500",
    ]);
    let service = relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active);
    let candidate = relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation);
    let candidate_instance = candidate.instance();
    let provisional_flow = RequestSchedulingFixture::default().with_per_flow_rates([(
        candidate_instance,
        RequestPerFlowRateModel {
            rate_bps: 24_000_000.0,
            delivery_samples: 1,
        },
    )]);

    let subflow = relay_path_snapshot_for_bulk_choice(
        &context,
        candidate_instance,
        Some(service.key()),
        Some(provisional_flow.state()),
        false,
    )
    .expect("provisional candidate snapshot");

    assert_eq!(subflow.delivery_rate_bps, 500_000_000.0);
    assert_eq!(subflow.rate_scope, PathRateScope::PathCapacity);
    assert_eq!(
        subflow.inflight_limit_bytes, 22_500_000,
        "one app-limited proof sample must not prevent the TCP carrier from ramping"
    );
    let mature_flow = RequestSchedulingFixture::default().with_per_flow_rates([(
        candidate_instance,
        RequestPerFlowRateModel {
            rate_bps: 24_000_000.0,
            delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
        },
    )]);
    let mature = relay_path_snapshot_for_bulk_choice(
        &context,
        candidate_instance,
        Some(service.key()),
        Some(mature_flow.state()),
        true,
    )
    .expect("mature candidate snapshot");
    assert_eq!(mature.delivery_rate_bps, 24_000_000.0);
    assert_eq!(mature.rate_scope, PathRateScope::PerFlowGoodput);
    assert_eq!(
        mature.inflight_limit_bytes, 1_080_000,
        "continuous exact ACK evidence replaces both the prior rate and pipe"
    );

    context.reserve_tcp_path_load(candidate_instance.key.index, FlowLane::Throughput);
    let owned = relay_path_snapshot_for_bulk_choice(
        &context,
        candidate_instance,
        Some(service.key()),
        Some(provisional_flow.state()),
        true,
    )
    .expect("owned candidate snapshot");
    let prospective = relay_path_snapshot_for_bulk_choice(
        &context,
        candidate_instance,
        Some(service.key()),
        Some(provisional_flow.state()),
        false,
    )
    .expect("prospective candidate snapshot");
    assert_eq!(owned.active_flows, 1);
    assert_eq!(prospective.active_flows, 2);
    context.release_tcp_path_load(candidate_instance.key.index, FlowLane::Throughput);
}

#[test]
fn endpoint_only_candidate_borrows_service_rate_only_until_its_model_matures() {
    let context = context(&["tcp://127.0.0.1:10078", "tcp://127.0.0.1:10079"]);
    let service = relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active);
    let candidate = relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation);
    let provisional = RequestSchedulingFixture::default().with_per_flow_rates([
        (
            service.instance(),
            RequestPerFlowRateModel {
                rate_bps: 400_000_000.0,
                delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
            },
        ),
        (
            candidate.instance(),
            RequestPerFlowRateModel {
                rate_bps: 10_000_000.0,
                delivery_samples: 1,
            },
        ),
    ]);
    let exploring = relay_path_snapshot_for_bulk_choice(
        &context,
        candidate.instance(),
        Some(service.key()),
        Some(provisional.state()),
        true,
    )
    .expect("endpoint-only exploration snapshot");
    assert_eq!(exploring.delivery_rate_bps, 400_000_000.0);
    assert!(exploring.inflight_limit_bytes > 1_000_000);

    let mature = RequestSchedulingFixture::default().with_per_flow_rates([
        (
            service.instance(),
            RequestPerFlowRateModel {
                rate_bps: 400_000_000.0,
                delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
            },
        ),
        (
            candidate.instance(),
            RequestPerFlowRateModel {
                rate_bps: 10_000_000.0,
                delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
            },
        ),
    ]);
    let corrected = relay_path_snapshot_for_bulk_choice(
        &context,
        candidate.instance(),
        Some(service.key()),
        Some(mature.state()),
        true,
    )
    .expect("mature endpoint-only snapshot");
    assert_eq!(corrected.delivery_rate_bps, 10_000_000.0);
    assert!(corrected.inflight_limit_bytes <= 1_000_000);
}

#[test]
fn configured_slow_candidate_does_not_borrow_faster_service_rate() {
    let context = context(&[
        "tcp://127.0.0.1:10078?srtt-ms=180&rate-mbps=500",
        "tcp://127.0.0.1:10079?srtt-ms=180&rate-mbps=100",
    ]);
    let service = relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active);
    let candidate = relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation);
    let rates = RequestSchedulingFixture::default().with_per_flow_rates([
        (
            service.instance(),
            RequestPerFlowRateModel {
                rate_bps: 500_000_000.0,
                delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
            },
        ),
        (
            candidate.instance(),
            RequestPerFlowRateModel {
                rate_bps: 10_000_000.0,
                delivery_samples: 1,
            },
        ),
    ]);
    let snapshot = relay_path_snapshot_for_bulk_choice(
        &context,
        candidate.instance(),
        Some(service.key()),
        Some(rates.state()),
        true,
    )
    .expect("configured heterogeneous candidate snapshot");
    assert_eq!(snapshot.delivery_rate_bps, 100_000_000.0);
    assert_eq!(snapshot.inflight_limit_bytes, 4_500_000);
}

#[test]
fn request_startup_needs_contention_but_existing_owner_drains_after_two_to_one() {
    let context = context(&[
        "tcp://127.0.0.1:10080?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10081?srtt-ms=10&rate-mbps=500",
    ]);
    let paths = vec![
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation),
    ];
    let service = paths[0].instance();
    let candidate = paths[1].instance();
    mark_bulk_service(&context, service.key);
    mark_path_proof(&context, candidate.key, Duration::from_millis(8));
    let flights = RequestFlightLedger::default();
    let choose = |epoch: Option<&FlowSubflowSet<RelayPathInstance>>| {
        choose_request_startup_subflow(
            &context,
            &paths,
            FlowLane::Throughput,
            None,
            0,
            64 * 1024,
            Some(service.key),
            Some(&flights),
            epoch,
            None,
            None,
        )
    };

    let first = context.reliable_tcp_request_bulk_flow_registration();
    first.update(true, Some(UnderlayProtocol::Tcp));
    assert!(choose(None).is_none(), "one upload must stay on Service");

    let second = context.reliable_tcp_request_bulk_flow_registration();
    second.update(true, Some(UnderlayProtocol::Udp));
    assert!(
        choose(None).is_none(),
        "a QUIC-Service upload must not unlock TCP startup"
    );
    second.update(true, Some(UnderlayProtocol::Tcp));
    assert!(matches!(
        choose(None),
        Some(BulkRelayPathChoice::SelectedStartupSubflow { .. })
    ));

    let startup_credit = usize::try_from(reliable_subflow_startup_sample_limit_bytes(
        context.mux_limits,
    ))
    .expect("startup credit");
    let mut epoch = FlowSubflowSet::new(0, service, startup_credit, 0, Duration::ZERO);
    assert_eq!(
        epoch
            .admit_subflow_owner(SubflowAdmissionInput {
                key: candidate,
                bulk_rate_proven: false,
                startup_owner_allowed: true,
                frontier_clear: true,
                completion_improves: false,
                observed_goodput_non_degrading: true,
                read_gap: Duration::ZERO,
                owner_bytes: 64 * 1024,
                optional_overhead_bytes: 0,
            })
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
    second.update(true, Some(UnderlayProtocol::Udp));
    assert_eq!(context.active_tcp_service_request_bulk_flows(), 1);
    assert!(matches!(
        choose(Some(&epoch)),
        Some(BulkRelayPathChoice::SelectedStartupSubflow {
            candidate: selected,
            ..
        }) if selected == candidate
    ));
}

#[test]
fn request_startup_prefers_idle_candidate_owned_by_no_other_flow() {
    let context = context(&[
        "tcp://127.0.0.1:10082?srtt-ms=180&rate-mbps=500",
        "tcp://127.0.0.1:10083?srtt-ms=180&rate-mbps=500",
        "tcp://127.0.0.1:10084?srtt-ms=180&rate-mbps=500",
    ]);
    let _bulk_flows = register_active_tcp_request_bulk_flows(&context, 2);
    let paths = vec![
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation),
        relay_path(UnderlayProtocol::Tcp, 2, RelayPathPlacement::Validation),
    ];
    let service = paths[0].instance();
    let occupied = paths[1].instance();
    let idle = paths[2].instance();
    mark_bulk_service(&context, service.key);
    mark_path_proof(&context, occupied.key, Duration::from_millis(180));
    mark_path_proof(&context, idle.key, Duration::from_millis(180));
    context.reserve_tcp_path_load(occupied.key.index, FlowLane::Throughput);
    let scheduling = RequestSchedulingFixture::default().with_rate_proven([service]);

    assert!(matches!(
        choose_request_startup_subflow(
            &context,
            &paths,
            FlowLane::Throughput,
            None,
            0,
            64 * 1024,
            Some(service.key),
            Some(&RequestFlightLedger::default()),
            None,
            Some(scheduling.state()),
            None,
        ),
        Some(BulkRelayPathChoice::SelectedStartupSubflow {
            candidate,
            ..
        }) if candidate == idle
    ));

    context.reserve_tcp_path_load(idle.key.index, FlowLane::Throughput);
    assert!(
        choose_request_startup_subflow(
            &context,
            &paths,
            FlowLane::Throughput,
            None,
            0,
            64 * 1024,
            Some(service.key),
            Some(&RequestFlightLedger::default()),
            None,
            Some(scheduling.state()),
            None,
        )
        .is_none(),
        "fresh startup must stay on Service when every Validation candidate already carries another flow"
    );
}

#[test]
fn request_startup_subflow_requires_proof_from_current_attachment() {
    let context = context(&[
        "tcp://127.0.0.1:10080?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10081?srtt-ms=10&rate-mbps=500",
    ]);
    let _bulk_flows = register_active_tcp_request_bulk_flows(&context, 2);
    let service_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let candidate_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    mark_bulk_service(&context, service_key);
    mark_path_proof(&context, candidate_key, Duration::from_millis(10));
    let paths = vec![
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation),
    ];
    let ledger = RequestFlightLedger::default();

    assert!(
        choose_request_startup_subflow(
            &context,
            &paths,
            FlowLane::Throughput,
            None,
            0,
            64 * 1024,
            Some(service_key),
            Some(&ledger),
            None,
            None,
            None,
        )
        .is_none(),
        "a proof observed before this output attached must not authorize unique data"
    );

    context.mark_relay_path_proof_observation(
        candidate_key.underlay,
        candidate_key.index,
        PathProofObservation {
            proof_id: 999,
            bytes: PATH_OPEN_SCORE_BYTES as u64,
            elapsed: Duration::from_millis(8),
            sent_at: Instant::now(),
        },
    );
    assert!(
        choose_request_startup_subflow(
            &context,
            &paths,
            FlowLane::Throughput,
            None,
            0,
            64 * 1024,
            Some(service_key),
            Some(&ledger),
            None,
            None,
            None,
        )
        .is_none(),
        "another stream's newer proof ID must not authorize this attachment"
    );

    mark_path_proof(&context, candidate_key, Duration::from_millis(8));
    assert!(matches!(
        choose_request_startup_subflow(
            &context,
            &paths,
            FlowLane::Throughput,
            None,
            0,
            64 * 1024,
            Some(service_key),
            Some(&ledger),
            None,
            None,
            None,
        ),
        Some(BulkRelayPathChoice::SelectedStartupSubflow {
            service,
            candidate,
            ..
        }) if service == paths[0].instance() && candidate == paths[1].instance()
    ));
    assert!(
        !context.relay_path_has_bulk_model_evidence(candidate_key.underlay, candidate_key.index,),
        "PATH_PROOF is current-instance reachability evidence, not capacity evidence"
    );
}

#[test]
fn request_calibration_ignores_path_wide_completion_without_directional_provenance() {
    let context = context(&[
        "tcp://127.0.0.1:10082?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10083?srtt-ms=10&rate-mbps=500",
    ]);
    let _bulk_flows = register_active_tcp_request_bulk_flows(&context, 2);
    let service_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let candidate_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    let paths = vec![
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation),
    ];
    let service = paths[0].instance();
    let candidate = paths[1].instance();
    mark_bulk_service(&context, service_key);
    context.mark_relay_path_rate_sample(
        candidate_key.underlay,
        candidate_key.index,
        PathRateSample::new(256 * 1024, Duration::from_secs(1)).expect("low receipt-rate evidence"),
    );
    mark_path_proof(&context, candidate_key, Duration::from_millis(10));
    let attempted = HashSet::from([candidate]);
    let calibration_target =
        reliable_request_ack_clock_calibration_target_bytes(context.mux_limits);
    let flights = RequestFlightLedger::default();
    let request = |spent: Option<u64>, ack_clock_proven: bool| {
        let scheduling = RequestSchedulingFixture::default()
            .with_rate_proven([service, candidate])
            .with_graduated([candidate])
            .with_ack_clock_first_window([candidate])
            .with_ack_clock_proven(ack_clock_proven.then_some(candidate))
            .with_calibration_spend(spent.map(|bytes| (candidate, bytes)))
            .with_operation(RequestAckClockOperation::Owner {
                candidate,
                target_bytes: calibration_target,
            });
        choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
            stream_id: StreamId(7),
            context: &context,
            paths: &paths,
            lane: FlowLane::Throughput,
            frame: None,
            offset: 0,
            payload_bytes: BBR_MAX_SEND_QUANTUM_BYTES,
            cursor: 1,
            avoid_keys: &[],
            path_flights: Some(&flights),
            ordered_data_owner: Some(service_key),
            subflow_set: None,
            request_state: Some(scheduling.state()),
            attempted_subflows: Some(&attempted),
        })
    };

    assert!(
        matches!(
            request(None, false),
            BulkRelayPathChoice::SelectedAckClockCalibration {
                candidate: selected,
                ..
            } if selected == candidate
        ),
        "an underfed startup rate cannot veto the calibration needed to replace it"
    );

    for _ in 0..8 {
        context.mark_relay_path_rate_sample(
            candidate_key.underlay,
            candidate_key.index,
            PathRateSample::new(256 * 1024, Duration::from_secs(1)).expect("mature slow evidence"),
        );
    }
    context.record_relay_path_send(
        candidate_key.underlay,
        candidate_key.index,
        BBR_MAX_SEND_QUANTUM_BYTES,
    );
    assert!(
        matches!(
            request(None, false),
            BulkRelayPathChoice::SelectedAckClockCalibration {
                candidate: selected,
                ..
            } if selected == candidate
        ),
        "path-wide maturity cannot veto request-direction calibration without provenance"
    );

    context.mark_relay_path_ack_clock_rate_sample(
        candidate_key.underlay,
        candidate_key.index,
        PathRateSample::new(256 * 1024, Duration::from_millis(4)).expect("fast ACK-clock evidence"),
        true,
    );
    assert!(
        matches!(
            request(None, false),
            BulkRelayPathChoice::SelectedAckClockCalibration {
                candidate: selected,
                ..
            } if selected == candidate
        ),
        "a mature fast candidate fits the bounded ACK-clock calibration window"
    );

    assert!(
        !matches!(
            request(Some(calibration_target), false),
            BulkRelayPathChoice::SelectedAckClockCalibration { .. }
        ),
        "calibration credit is cumulative and does not refill"
    );
    assert!(
        !matches!(
            request(None, true),
            BulkRelayPathChoice::SelectedAckClockCalibration { .. }
        ),
        "a usable ACK-clock sample permanently returns the instance to ordinary ETA ranking"
    );
}

#[test]
fn request_calibration_needs_contention_but_spent_owner_drains_after_two_to_one() {
    let context = context(&[
        "tcp://127.0.0.1:10084?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10085?srtt-ms=10&rate-mbps=1",
    ]);
    let paths = vec![
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation),
    ];
    let service = paths[0].instance();
    let candidate = paths[1].instance();
    mark_bulk_service(&context, service.key);
    context.mark_relay_path_rate_sample(
        candidate.key.underlay,
        candidate.key.index,
        PathRateSample::new(256 * 1024, Duration::from_secs(1)).expect("cold carrier receipt"),
    );
    mark_path_proof(&context, candidate.key, Duration::from_millis(8));
    let flights = RequestFlightLedger::default();
    let calibration_target =
        reliable_request_ack_clock_calibration_target_bytes(context.mux_limits);
    let service_snapshot = relay_path_snapshot_for_bulk_choice(
        &context,
        service,
        Some(service.key),
        None,
        paths[0].has_load_reservation(),
    )
    .expect("service snapshot");
    let candidate_snapshot = relay_path_snapshot_for_bulk_choice(
        &context,
        candidate,
        Some(service.key),
        None,
        paths[1].has_load_reservation(),
    )
    .expect("candidate snapshot");
    let service_eta = scheduler::score_path(
        service_snapshot,
        FlowLane::Throughput,
        BBR_MAX_SEND_QUANTUM_BYTES,
        SchedulerPolicy::default(),
    )
    .expect("service score")
    .eta_ms;
    let candidate_eta = scheduler::score_path(
        candidate_snapshot,
        FlowLane::Throughput,
        BBR_MAX_SEND_QUANTUM_BYTES,
        SchedulerPolicy::default(),
    )
    .expect("candidate score")
    .eta_ms;
    let cold_projection = reliable_tcp_ack_clock_calibration_opportunity(
        service_snapshot,
        service_eta,
        candidate_snapshot,
        candidate_eta,
        calibration_target,
        BBR_MAX_SEND_QUANTUM_BYTES,
        context.mux_limits,
    );
    assert!(
        !cold_projection.is_admitted(),
        "the fixture must be completion-noncompetitive before typed pre-measurement entry: service={service_snapshot:?} candidate={candidate_snapshot:?} projection={cold_projection:?}"
    );
    let choose = |spent: Option<u64>, tcp_capacity_proven: bool| {
        let scheduling = RequestSchedulingFixture::default()
            .with_rate_proven([service, candidate])
            .with_graduated([candidate])
            .with_ack_clock_first_window([candidate])
            .with_tcp_capacity_proven(tcp_capacity_proven.then_some(candidate))
            .with_calibration_spend(spent.map(|bytes| (candidate, bytes)));
        let scheduling = if spent.is_some() {
            scheduling.with_operation(RequestAckClockOperation::Owner {
                candidate,
                target_bytes: calibration_target,
            })
        } else {
            scheduling
        };
        choose_request_ack_clock_calibration(
            &context,
            &paths,
            FlowLane::Throughput,
            None,
            0,
            BBR_MAX_SEND_QUANTUM_BYTES,
            1,
            Some(service.key),
            Some(&flights),
            None,
            Some(scheduling.state()),
        )
    };

    let only_flow = context.reliable_tcp_request_bulk_flow_registration();
    only_flow.update(true, Some(UnderlayProtocol::Tcp));
    assert!(
        choose(None, false).is_none(),
        "one upload must not begin ordered calibration"
    );
    assert!(
        matches!(
            choose(None, true),
            Some(BulkRelayPathChoice::SelectedAckClockCalibration {
                candidate: selected,
                ..
            }) if selected == candidate
        ),
        "an exact typed receipt must authorize only its bounded measurement epoch"
    );
    let second_flow = context.reliable_tcp_request_bulk_flow_registration();
    second_flow.update(true, Some(UnderlayProtocol::Udp));
    assert!(
        choose(None, false).is_none(),
        "a QUIC-Service upload must not unlock TCP calibration"
    );
    second_flow.update(true, Some(UnderlayProtocol::Tcp));
    assert!(matches!(
        choose(None, false),
        Some(BulkRelayPathChoice::SelectedAckClockCalibration { .. })
    ));
    second_flow.update(true, Some(UnderlayProtocol::Udp));
    assert!(
        matches!(
            choose(Some(BBR_MAX_SEND_QUANTUM_BYTES as u64), false),
            Some(BulkRelayPathChoice::SelectedAckClockCalibration {
                candidate: selected,
                ..
            }) if selected == candidate
        ),
        "an exact spent owner must finish its bounded epoch after 2->1"
    );
}

#[test]
fn parallel_tcp_carrier_proofs_keep_one_product_calibration_owner() {
    let context = context(&[
        "tcp://127.0.0.1:10086?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10087?srtt-ms=10&rate-mbps=500",
        "tcp://127.0.0.1:10088?srtt-ms=12&rate-mbps=500",
    ]);
    let paths = vec![
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation),
        relay_path(UnderlayProtocol::Tcp, 2, RelayPathPlacement::Validation),
    ];
    let service = paths[0].instance();
    let first = paths[1].instance();
    let second = paths[2].instance();
    mark_bulk_service(&context, service.key);
    for candidate in [first, second] {
        context.mark_relay_path_rate_sample(
            candidate.key.underlay,
            candidate.key.index,
            PathRateSample::new(256 * 1024, Duration::from_millis(4))
                .expect("carrier-rate evidence"),
        );
        mark_path_proof(&context, candidate.key, Duration::from_millis(8));
    }
    let flow = context.reliable_tcp_request_bulk_flow_registration();
    flow.update(true, Some(UnderlayProtocol::Tcp));
    let flights = RequestFlightLedger::default();
    let target = reliable_request_ack_clock_calibration_target_bytes(context.mux_limits);
    let choose = |operation: Option<RequestAckClockOperation>,
                  ack_proven: Option<RelayPathInstance>| {
        let scheduling = RequestSchedulingFixture::default()
            .with_rate_proven([service, first, second])
            .with_graduated([first, second])
            .with_tcp_capacity_proven([first, second])
            .with_ack_clock_proven(ack_proven);
        let scheduling = match operation {
            Some(operation) => scheduling.with_operation(operation),
            None => scheduling,
        };
        choose_request_ack_clock_calibration(
            &context,
            &paths,
            FlowLane::Throughput,
            None,
            0,
            BBR_MAX_SEND_QUANTUM_BYTES,
            1,
            Some(service.key),
            Some(&flights),
            None,
            Some(scheduling.state()),
        )
    };

    let selected = match choose(None, None) {
        Some(BulkRelayPathChoice::SelectedAckClockCalibration { candidate, .. }) => candidate,
        _ => panic!("one carrier-proven candidate must acquire product ownership"),
    };
    let other = if selected == first { second } else { first };
    assert!(matches!(
        choose(
            Some(RequestAckClockOperation::Owner {
                candidate: selected,
                target_bytes: target,
            }),
            None,
        ),
        Some(BulkRelayPathChoice::SelectedAckClockCalibration { candidate, .. })
            if candidate == selected
    ));
    assert!(matches!(
        choose(None, Some(selected)),
        Some(BulkRelayPathChoice::SelectedAckClockCalibration { candidate, .. })
            if candidate == other
    ));
}

#[test]
fn request_calibration_transaction_drains_prior_optional_owner_before_exact_entry() {
    let context = context(&[
        "tcp://127.0.0.1:10090?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10091?srtt-ms=10&rate-mbps=500",
        "tcp://127.0.0.1:10092?srtt-ms=12&rate-mbps=500",
        "tcp://127.0.0.1:10093?srtt-ms=8&rate-mbps=500",
    ]);
    let _bulk_flows = register_active_tcp_request_bulk_flows(&context, 2);
    let paths = vec![
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation),
        relay_path(UnderlayProtocol::Tcp, 2, RelayPathPlacement::Validation),
        relay_path(UnderlayProtocol::Tcp, 3, RelayPathPlacement::Validation),
    ];
    let service = paths[0].instance();
    let previous = paths[1].instance();
    let candidate = paths[2].instance();
    let ungraduated = paths[3].instance();
    mark_bulk_service(&context, service.key);
    for path in paths.iter().skip(1) {
        context.mark_relay_path_rate_sample(
            path.key().underlay,
            path.key().index,
            PathRateSample::new(256 * 1024, Duration::from_millis(8))
                .expect("carrier-rate evidence"),
        );
        mark_path_proof(&context, path.key(), Duration::from_millis(8));
    }
    context.mark_relay_path_ack_clock_rate_sample(
        previous.key.underlay,
        previous.key.index,
        PathRateSample::new(2 * 1024 * 1024, Duration::from_millis(32))
            .expect("prior exact product rate"),
        true,
    );
    let attempted = HashSet::from([previous, candidate]);
    let target = reliable_request_ack_clock_calibration_target_bytes(context.mux_limits);
    let payload_bytes = BBR_MAX_SEND_QUANTUM_BYTES;
    let lower_frame = data_frame(0, payload_bytes);
    let mut prior_debt = RequestFlightLedger::default();
    prior_debt.record_owner_frame_instance(previous, &lower_frame);
    let choose = |flights: &RequestFlightLedger,
                  offset: u64,
                  operation: Option<RequestAckClockOperation>,
                  spent: Option<u64>| {
        let scheduling = RequestSchedulingFixture::default()
            .with_rate_proven([service, previous, candidate])
            .with_graduated([previous, candidate])
            .with_ack_clock_proven([previous])
            .with_tcp_capacity_proven([candidate])
            .with_calibration_spend(spent.map(|bytes| (candidate, bytes)));
        let scheduling = match operation {
            Some(operation) => scheduling.with_operation(operation),
            None => scheduling,
        };
        choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
            stream_id: StreamId(7),
            context: &context,
            paths: &paths,
            lane: FlowLane::Throughput,
            frame: None,
            offset,
            payload_bytes,
            cursor: 2,
            avoid_keys: &[],
            path_flights: Some(flights),
            ordered_data_owner: Some(service.key),
            subflow_set: None,
            request_state: Some(scheduling.state()),
            attempted_subflows: Some(&attempted),
        })
    };

    assert_eq!(
        choose(&prior_debt, payload_bytes as u64, None, None,),
        BulkRelayPathChoice::SelectedAckClockCalibrationFence { service, candidate },
        "typed entry must drain a proven prior optional owner on Service"
    );
    let pending = Some(RequestAckClockOperation::Pending { service, candidate });
    assert_eq!(
        choose(&prior_debt, payload_bytes as u64, pending, None,),
        BulkRelayPathChoice::SelectedAckClockCalibrationFence { service, candidate },
        "pending identity must suppress both prior-path refill and a new startup owner"
    );
    assert_ne!(candidate, ungraduated);

    let drained = RequestFlightLedger::default();
    assert!(matches!(
        choose(&drained, payload_bytes as u64, pending, None),
        BulkRelayPathChoice::SelectedAckClockCalibration {
            candidate: selected,
            ..
        } if selected == candidate
    ));

    let owner = Some(RequestAckClockOperation::Owner {
        candidate,
        target_bytes: target,
    });
    let live_scheduling = RequestSchedulingFixture::default()
        .with_rate_proven([service, previous, candidate])
        .with_graduated([previous, candidate])
        .with_ack_clock_proven([previous])
        .with_calibration_spend([(candidate, target)])
        .with_operation(owner.expect("calibration owner"));
    assert_eq!(
        live_request_ack_clock_calibration_transaction(
            &paths,
            service.key,
            Some(live_scheduling.state()),
        ),
        Some(candidate),
        "a sealed target must outlive its ephemeral carrier-entry proof"
    );
    let mut own_debt = RequestFlightLedger::default();
    own_debt.record_owner_frame_instance(candidate, &lower_frame);
    assert!(matches!(
        choose(
            &own_debt,
            payload_bytes as u64,
            owner,
            Some(payload_bytes as u64),
        ),
        BulkRelayPathChoice::SelectedAckClockCalibration {
            candidate: selected,
            ..
        } if selected == candidate
    ));

    assert_eq!(
        choose(&drained, payload_bytes as u64, owner, Some(target)),
        BulkRelayPathChoice::SelectedAckClockCalibrationFence { service, candidate },
        "target exhaustion must not release generic optional ownership before exact proof"
    );

    let product_envelope =
        bulk_service_product_envelope_payload_bytes(payload_bytes, context.mux_limits);
    let mut saturated = RequestFlightLedger::default();
    saturated.record_owner_frame_instance(previous, &data_frame(0, product_envelope));
    assert_eq!(
        choose(&saturated, product_envelope as u64, pending, None,),
        BulkRelayPathChoice::Blocked,
        "the Service fence must not bypass its bounded product reservoir"
    );
}

#[test]
fn request_tcp_carrier_calibration_uses_exact_debt_not_logical_flight_age() {
    let context = context(&[
        "tcp://127.0.0.1:10084?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10085?srtt-ms=10&rate-mbps=500",
    ]);
    let paths = vec![
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation),
    ];
    let service = paths[0].instance();
    let candidate = paths[1].instance();
    mark_bulk_service(&context, service.key);
    context.mark_relay_path_rate_sample(
        candidate.key.underlay,
        candidate.key.index,
        PathRateSample::new(256 * 1024, Duration::from_millis(4)).expect("carrier-rate evidence"),
    );
    mark_path_proof(&context, candidate.key, Duration::from_millis(8));
    let flow = context.reliable_tcp_request_bulk_flow_registration();
    flow.update(true, Some(UnderlayProtocol::Tcp));
    let scheduling = RequestSchedulingFixture::default()
        .with_rate_proven([service, candidate])
        .with_graduated([candidate])
        .with_tcp_capacity_proven([candidate]);
    let mut flights = RequestFlightLedger::default();
    flights.record_owner_frame_instance(service, &data_frame(0, 64 * 1024));
    flights.age_product_flights_for_test(Duration::from_secs(10));
    let choose = |flights: &RequestFlightLedger| {
        choose_request_ack_clock_calibration(
            &context,
            &paths,
            FlowLane::Throughput,
            None,
            64 * 1024,
            64 * 1024,
            1,
            Some(service.key),
            Some(flights),
            None,
            Some(scheduling.state()),
        )
    };

    assert!(matches!(
        choose(&flights),
        Some(BulkRelayPathChoice::SelectedAckClockCalibration {
            candidate: selected,
            ..
        }) if selected == candidate
    ));

    flights.record_repair_frame_instance(candidate, &data_frame(0, 64 * 1024));
    assert!(
        choose(&flights).is_none(),
        "an exact repair flight still fences new optional product data"
    );
}

#[test]
fn exhausted_calibration_waits_for_exact_owner_proof_before_next_candidate() {
    let context = context(&[
        "tcp://127.0.0.1:10084?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10085?srtt-ms=10&rate-mbps=500",
        "tcp://127.0.0.1:10086?srtt-ms=10&rate-mbps=500",
    ]);
    let _bulk_flows = register_active_tcp_request_bulk_flows(&context, 2);
    let paths = vec![
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation),
        relay_path(UnderlayProtocol::Tcp, 2, RelayPathPlacement::Validation),
    ];
    let service_key = paths[0].key();
    let first = paths[1].instance();
    let second = paths[2].instance();
    mark_bulk_service(&context, service_key);
    for path in paths.iter().skip(1) {
        context.mark_relay_path_rate_sample(
            path.key().underlay,
            path.key().index,
            PathRateSample::new(256 * 1024, Duration::from_millis(4))
                .expect("attractive receipt-rate evidence"),
        );
        mark_path_proof(&context, path.key(), Duration::from_millis(10));
    }
    let attempted = HashSet::from([first, second]);
    let calibration_target =
        reliable_request_ack_clock_calibration_target_bytes(context.mux_limits);
    let scheduling = RequestSchedulingFixture::default()
        .with_rate_proven([paths[0].instance(), first, second])
        .with_graduated([first, second])
        .with_ack_clock_first_window([first, second])
        .with_calibration_spend([(first, calibration_target)])
        .with_operation(RequestAckClockOperation::Owner {
            candidate: first,
            target_bytes: calibration_target,
        });
    let choose = |flights: &RequestFlightLedger| {
        choose_request_ack_clock_calibration(
            &context,
            &paths,
            FlowLane::Throughput,
            None,
            BBR_MAX_SEND_QUANTUM_BYTES as u64,
            BBR_MAX_SEND_QUANTUM_BYTES,
            2,
            Some(service_key),
            Some(flights),
            None,
            Some(scheduling.state()),
        )
    };

    let mut outstanding = RequestFlightLedger::default();
    outstanding.record_owner_frame_instance(first, &data_frame(0, BBR_MAX_SEND_QUANTUM_BYTES));
    assert_eq!(
        choose(&outstanding),
        None,
        "another candidate must wait while the exhausted exact calibration still owns bytes"
    );
    assert_eq!(
        choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
            stream_id: StreamId(7),
            context: &context,
            paths: &paths,
            lane: FlowLane::Throughput,
            frame: None,
            offset: BBR_MAX_SEND_QUANTUM_BYTES as u64,
            payload_bytes: BBR_MAX_SEND_QUANTUM_BYTES,
            cursor: 1,
            avoid_keys: &[],
            path_flights: Some(&outstanding),
            ordered_data_owner: Some(service_key),
            subflow_set: None,
            request_state: Some(scheduling.state()),
            attempted_subflows: Some(&attempted),
        }),
        BulkRelayPathChoice::SelectedAckClockCalibrationFence {
            service: paths[0].instance(),
            candidate: first,
        },
        "an exhausted unproven owner must drain through Service before any ordinary owner bypasses it"
    );
    assert_eq!(
        choose(&RequestFlightLedger::default()),
        None,
        "an exact exhausted owner remains authoritative until its ACK evidence proves or its lifecycle ends"
    );
}

#[test]
fn ineligible_spent_instance_does_not_block_live_validation_calibration() {
    let context = context(&[
        "tcp://127.0.0.1:10087?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10088?srtt-ms=10&rate-mbps=500",
        "tcp://127.0.0.1:10089?srtt-ms=10&rate-mbps=500",
    ]);
    let _bulk_flows = register_active_tcp_request_bulk_flows(&context, 2);
    let paths = vec![
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Repair),
        relay_path(UnderlayProtocol::Tcp, 2, RelayPathPlacement::Validation),
    ];
    let service_key = paths[0].key();
    let ineligible = paths[1].instance();
    let candidate = paths[2].instance();
    mark_bulk_service(&context, service_key);
    context.mark_relay_path_rate_sample(
        candidate.key.underlay,
        candidate.key.index,
        PathRateSample::new(256 * 1024, Duration::from_secs(1)).expect("receipt-rate evidence"),
    );
    mark_path_proof(&context, candidate.key, Duration::from_millis(10));
    let scheduling = RequestSchedulingFixture::default()
        .with_rate_proven([paths[0].instance(), ineligible, candidate])
        .with_graduated([ineligible, candidate])
        .with_ack_clock_first_window([candidate])
        .with_calibration_spend([(ineligible, BBR_MAX_SEND_QUANTUM_BYTES as u64)]);

    assert!(
        matches!(
            choose_request_ack_clock_calibration(
                &context,
                &paths,
                FlowLane::Throughput,
                None,
                0,
                BBR_MAX_SEND_QUANTUM_BYTES,
                2,
                Some(service_key),
                Some(&RequestFlightLedger::default()),
                None,
                Some(scheduling.state()),
            ),
            Some(BulkRelayPathChoice::SelectedAckClockCalibration {
                candidate: selected,
                ..
            }) if selected == candidate
        ),
        "spent credit on a Repair placement must not serialize live Validation work"
    );
}

#[test]
fn request_startup_subflow_rejects_cross_family_repair_and_latency_pressure() {
    let context = context(&[
        "tcp://127.0.0.1:10090?srtt-ms=20&rate-mbps=500",
        "udp://127.0.0.1:10091?srtt-ms=10&rate-mbps=500",
    ]);
    let _bulk_flows = register_active_tcp_request_bulk_flows(&context, 2);
    let service_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let candidate_key = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    mark_bulk_service(&context, service_key);
    let cross_family = vec![
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Udp, 0, RelayPathPlacement::Validation),
    ];
    mark_path_proof(&context, candidate_key, Duration::from_millis(8));
    let ledger = RequestFlightLedger::default();
    assert!(
        choose_request_startup_subflow(
            &context,
            &cross_family,
            FlowLane::Throughput,
            None,
            0,
            64 * 1024,
            Some(service_key),
            Some(&ledger),
            None,
            None,
            None,
        )
        .is_none(),
        "independent carrier recovery models cannot share startup credit"
    );

    let repair = vec![
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Udp, 0, RelayPathPlacement::Repair),
    ];
    assert!(
        choose_request_startup_subflow(
            &context,
            &repair,
            FlowLane::Throughput,
            None,
            0,
            64 * 1024,
            Some(service_key),
            Some(&ledger),
            None,
            None,
            None,
        )
        .is_none(),
        "Repair placement is never a capacity-sampling owner"
    );

    context.reserve_tcp_path_load(0, FlowLane::Latency);
    let same_family = vec![
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Validation),
    ];
    assert!(
        choose_request_startup_subflow(
            &context,
            &same_family,
            FlowLane::Throughput,
            None,
            0,
            64 * 1024,
            Some(service_key),
            Some(&ledger),
            None,
            None,
            None,
        )
        .is_none(),
        "any reliable latency pressure suppresses optional startup sampling"
    );
}

#[test]
fn request_startup_waits_for_service_anchor_and_authoritative_debt() {
    let context = context(&[
        "tcp://127.0.0.1:10092?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10093?srtt-ms=10&rate-mbps=500",
    ]);
    let _bulk_flows = register_active_tcp_request_bulk_flows(&context, 2);
    let service_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let candidate_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    mark_bulk_service(&context, service_key);
    let paths = vec![
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation),
    ];
    mark_path_proof(&context, candidate_key, Duration::from_millis(8));
    let empty = RequestFlightLedger::default();
    let exact_state = HashSet::new();
    let scheduling = RequestSchedulingFixture::default().with_rate_proven([paths[0].instance()]);

    assert_eq!(
        choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
            stream_id: StreamId(7),
            context: &context,
            paths: &paths,
            lane: FlowLane::Throughput,
            frame: None,
            offset: 0,
            payload_bytes: 64 * 1024,
            cursor: 1,
            avoid_keys: &[],
            path_flights: Some(&empty),
            ordered_data_owner: None,
            subflow_set: None,
            request_state: Some(scheduling.state()),
            attempted_subflows: Some(&exact_state),
        }),
        BulkRelayPathChoice::Selected(paths[0].instance()),
        "offset zero must establish Service before any Validation path can own data"
    );

    let mut foreign = RequestFlightLedger::default();
    foreign.record_owner_frame(candidate_key, &data_frame(0, 64 * 1024));
    assert!(
        choose_request_startup_subflow(
            &context,
            &paths,
            FlowLane::Throughput,
            None,
            64 * 1024,
            64 * 1024,
            Some(service_key),
            Some(&foreign),
            None,
            Some(scheduling.state()),
            Some(&exact_state),
        )
        .is_none(),
        "a foreign lower OwnerData range is authoritative and cannot be crossed"
    );

    let mut repaired = RequestFlightLedger::default();
    repaired.record_owner_frame(service_key, &data_frame(0, 64 * 1024));
    repaired.record_repair_frame(candidate_key, &data_frame(0, 64 * 1024));
    assert!(
        choose_request_startup_subflow(
            &context,
            &paths,
            FlowLane::Throughput,
            None,
            64 * 1024,
            64 * 1024,
            Some(service_key),
            Some(&repaired),
            None,
            Some(scheduling.state()),
            Some(&exact_state),
        )
        .is_none(),
        "Repair ambiguity must drain before optional unique-data sampling"
    );
}

#[test]
fn request_startup_allows_aged_ordinary_service_flight_within_envelope() {
    let mut context = context(&[
        "tcp://127.0.0.1:10094?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10095?srtt-ms=10&rate-mbps=500",
    ]);
    context.mux_limits.max_stream_window_bytes = 128 * 1024;
    context.mux_limits.max_repair_bytes = 128 * 1024;
    context.mux_limits.max_reorder_bytes = 128 * 1024;
    context.mux_limits.max_path_flight_bytes = 128 * 1024;
    let _bulk_flows = register_active_tcp_request_bulk_flows(&context, 2);
    let service_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let candidate_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    mark_bulk_service(&context, service_key);
    let paths = vec![
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation),
    ];
    mark_path_proof(&context, candidate_key, Duration::from_millis(8));
    let mut ledger = RequestFlightLedger::default();
    ledger.record_owner_frame(service_key, &data_frame(0, 64 * 1024));
    assert!(
        choose_request_startup_subflow(
            &context,
            &paths,
            FlowLane::Throughput,
            None,
            64 * 1024,
            64 * 1024,
            Some(service_key),
            Some(&ledger),
            None,
            None,
            None,
        )
        .is_some(),
        "the healthy Service and fresh Validation proof satisfy every startup gate"
    );
    ledger.age_product_flights_for_test(Duration::from_secs(10));

    assert!(
        matches!(
            choose_request_startup_subflow(
                &context,
                &paths,
                FlowLane::Throughput,
                None,
                64 * 1024,
                64 * 1024,
                Some(service_key),
                Some(&ledger),
                None,
                None,
                None,
            ),
            Some(BulkRelayPathChoice::SelectedStartupSubflow {
                service,
                candidate,
                ..
            }) if service == paths[0].instance() && candidate == paths[1].instance()
        ),
        "an old ordinary Service owner flight is not foreign debt or repair ambiguity; bounded startup remains inside the product envelope"
    );

    ledger.record_owner_frame(service_key, &data_frame(64 * 1024, 64 * 1024));
    assert!(
        choose_request_startup_subflow(
            &context,
            &paths,
            FlowLane::Throughput,
            None,
            128 * 1024,
            64 * 1024,
            Some(service_key),
            Some(&ledger),
            None,
            None,
            None,
        )
        .is_none(),
        "ordinary Service suffix plus candidate quantum must remain inside the product envelope"
    );
}

#[test]
fn request_startup_does_not_use_ordered_product_bytes_to_probe_quic_capacity() {
    let context = context(&[
        "udp://127.0.0.1:10096?srtt-ms=20&rate-mbps=500",
        "udp://127.0.0.1:10097?srtt-ms=10&rate-mbps=500",
    ]);
    let service_key = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let candidate_key = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 1,
    };
    mark_bulk_service(&context, service_key);
    let paths = vec![
        relay_path(UnderlayProtocol::Udp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Udp, 1, RelayPathPlacement::Validation),
    ];
    mark_path_proof(&context, candidate_key, Duration::from_millis(8));

    assert!(
        choose_request_startup_subflow(
            &context,
            &paths,
            FlowLane::Throughput,
            None,
            64 * 1024,
            64 * 1024,
            Some(service_key),
            Some(&RequestFlightLedger::default()),
            None,
            None,
            None,
        )
        .is_none(),
        "QUIC Validation needs native non-app-limited carrier proof; product startup sampling is TCP-only"
    );
}

#[test]
fn request_startup_owner_needs_ack_clock_proof_after_flights_drain() {
    let context = context(&[
        "tcp://127.0.0.1:10100?srtt-ms=30&rate-mbps=500",
        "tcp://127.0.0.1:10101?srtt-ms=5&rate-mbps=500",
    ]);
    let service_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let candidate_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    mark_bulk_service(&context, service_key);
    mark_bulk_service(&context, candidate_key);
    let paths = vec![
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation),
    ];
    mark_path_proof(&context, candidate_key, Duration::from_millis(5));
    let startup_credit = usize::try_from(reliable_subflow_startup_sample_limit_bytes(
        context.mux_limits,
    ))
    .expect("startup credit");
    let mut epoch = FlowSubflowSet::new(0, paths[0].instance(), startup_credit, 0, Duration::ZERO);
    assert_eq!(
        epoch
            .admit_subflow_owner(SubflowAdmissionInput {
                key: paths[1].instance(),
                bulk_rate_proven: false,
                startup_owner_allowed: true,
                frontier_clear: true,
                completion_improves: false,
                observed_goodput_non_degrading: true,
                read_gap: Duration::ZERO,
                owner_bytes: startup_credit,
                optional_overhead_bytes: 0,
            })
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
    let mut ledger = RequestFlightLedger::default();
    ledger.record_owner_frame(candidate_key, &data_frame(0, 64 * 1024));
    let attempted = HashSet::from([paths[1].instance()]);
    let pre_graduation_state =
        RequestSchedulingFixture::default().with_rate_proven([paths[0].instance()]);

    let pre_graduation = choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
        stream_id: StreamId(7),
        context: &context,
        paths: &paths,
        lane: FlowLane::Throughput,
        frame: None,
        offset: 64 * 1024,
        payload_bytes: 64 * 1024,
        cursor: 1,
        avoid_keys: &[],
        path_flights: Some(&ledger),
        ordered_data_owner: Some(service_key),
        subflow_set: Some(&epoch),
        request_state: Some(pre_graduation_state.state()),
        attempted_subflows: Some(&attempted),
    });
    assert!(
        pre_graduation == BulkRelayPathChoice::Selected(paths[0].instance())
            || pre_graduation == BulkRelayPathChoice::Blocked,
        "early rate evidence must not let the startup owner escape its cumulative epoch while attributed ranges remain"
    );

    ledger.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: 64 * 1024,
    }]);
    assert!(epoch.graduate_startup_owner(paths[1].instance()));
    let graduated_state = RequestSchedulingFixture::default()
        .with_rate_proven([paths[0].instance()])
        .with_graduated([paths[1].instance()]);
    assert_eq!(
        choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
            stream_id: StreamId(7),
            context: &context,
            paths: &paths,
            lane: FlowLane::Throughput,
            frame: None,
            offset: 64 * 1024,
            payload_bytes: 64 * 1024,
            cursor: 1,
            avoid_keys: &[],
            path_flights: Some(&ledger),
            ordered_data_owner: Some(service_key),
            subflow_set: Some(&epoch),
            request_state: Some(graduated_state.state()),
            attempted_subflows: Some(&attempted),
        }),
        BulkRelayPathChoice::Selected(paths[0].instance()),
        "TCP receipt graduation alone is not attributable capacity proof"
    );

    let proven_state = RequestSchedulingFixture::default()
        .with_rate_proven([paths[0].instance()])
        .with_graduated([paths[1].instance()])
        .with_ack_clock_proven([paths[1].instance()]);
    assert_eq!(
        choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
            stream_id: StreamId(7),
            context: &context,
            paths: &paths,
            lane: FlowLane::Throughput,
            frame: None,
            offset: 64 * 1024,
            payload_bytes: 64 * 1024,
            cursor: 1,
            avoid_keys: &[],
            path_flights: Some(&ledger),
            ordered_data_owner: Some(service_key),
            subflow_set: Some(&epoch),
            request_state: Some(proven_state.state()),
            attempted_subflows: Some(&attempted),
        }),
        BulkRelayPathChoice::Selected(paths[1].instance()),
        "drained startup plus attributable ACK-clock proof may enter ordinary admission"
    );
}

#[test]
fn exact_flow_local_model_can_own_without_session_global_membership() {
    let context = context(&[
        "tcp://127.0.0.1:10102?srtt-ms=180&rate-mbps=400",
        "tcp://127.0.0.1:10103?srtt-ms=5&rate-mbps=500",
    ]);
    let paths = vec![
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation),
    ];
    let service = paths[0].instance();
    let candidate = paths[1].instance();
    mark_bulk_service(&context, service.key);
    let service_rate = RequestPerFlowRateModel {
        rate_bps: 400_000_000.0,
        delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
    };
    let candidate_rate = RequestPerFlowRateModel {
        rate_bps: 500_000_000.0,
        delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
    };
    let scheduling = RequestSchedulingFixture::default()
        .with_rate_proven([service])
        .with_graduated([candidate])
        .with_ack_clock_proven([candidate])
        .with_per_flow_rates([(service, service_rate), (candidate, candidate_rate)]);
    let missing_graduation = RequestSchedulingFixture::default()
        .with_ack_clock_proven([candidate])
        .with_per_flow_rates([(candidate, candidate_rate)]);
    let missing_ack_proof = RequestSchedulingFixture::default()
        .with_graduated([candidate])
        .with_per_flow_rates([(candidate, candidate_rate)]);

    let globally_admitted = context.ordered_reliable_bulk_striping_path_keys(64 * 1024);
    assert!(globally_admitted.contains(&service.key));
    assert!(!globally_admitted.contains(&candidate.key));
    assert!(request_path_has_exact_flow_local_bulk_model(
        &paths[1],
        Some(scheduling.state()),
    ));
    assert!(!request_path_has_exact_flow_local_bulk_model(
        &paths[1],
        Some(missing_graduation.state()),
    ));
    assert!(!request_path_has_exact_flow_local_bulk_model(
        &paths[1],
        Some(missing_ack_proof.state()),
    ));
    let udp_candidate = relay_path(UnderlayProtocol::Udp, 1, RelayPathPlacement::Validation);
    let udp_state = RequestSchedulingFixture::default()
        .with_graduated([udp_candidate.instance()])
        .with_ack_clock_proven([udp_candidate.instance()])
        .with_per_flow_rates([(
            udp_candidate.instance(),
            RequestPerFlowRateModel {
                rate_bps: 500_000_000.0,
                delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
            },
        )]);
    assert!(!request_path_has_exact_flow_local_bulk_model(
        &udp_candidate,
        Some(udp_state.state()),
    ));

    let service_snapshot = relay_path_snapshot_for_bulk_choice(
        &context,
        service,
        Some(service.key),
        Some(scheduling.state()),
        true,
    )
    .expect("service snapshot");
    let candidate_snapshot = relay_path_snapshot_for_bulk_choice(
        &context,
        candidate,
        Some(service.key),
        Some(scheduling.state()),
        false,
    )
    .expect("candidate snapshot");
    let policy = SchedulerPolicy::default();
    let scoring_payload_bytes = bulk_service_horizon_payload_bytes(64 * 1024, context.mux_limits);
    let service_eta = scheduler::score_path(
        service_snapshot,
        FlowLane::Throughput,
        scoring_payload_bytes,
        policy,
    )
    .expect("service score")
    .eta_ms;
    let candidate_eta = scheduler::score_path(
        candidate_snapshot,
        FlowLane::Throughput,
        scoring_payload_bytes,
        policy,
    )
    .expect("candidate score")
    .eta_ms;
    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: service_snapshot,
            best_eta_ms: service_eta,
            candidate_snapshot,
            candidate_eta_ms: candidate_eta,
            payload_bytes: 64 * 1024,
            mux_limits: context.mux_limits,
            role: bulk_additional_admission_role(service.key.underlay, candidate.key.underlay,),
            stream_ordering_debt_bytes: 0,
        }),
        None,
        "the counterfactual must reach downstream admission before the lab can test the ownership mismatch"
    );

    assert_eq!(
        choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
            stream_id: StreamId(7),
            context: &context,
            paths: &paths,
            lane: FlowLane::Throughput,
            frame: None,
            offset: 64 * 1024,
            payload_bytes: 64 * 1024,
            cursor: 1,
            avoid_keys: &[],
            path_flights: Some(&RequestFlightLedger::default()),
            ordered_data_owner: Some(service.key),
            subflow_set: None,
            request_state: Some(scheduling.state()),
            attempted_subflows: Some(&HashSet::from([candidate])),
        }),
        BulkRelayPathChoice::Selected(paths[1].instance()),
        "exact per-flow TCP proof must not be revoked by another flow's session-global model"
    );
}

#[test]
fn bulk_ready_blocks_when_no_attached_path_can_advance_ordered_frontier() {
    let context = context(&[
        "tcp://127.0.0.1:10100?srtt-ms=50&rate-mbps=1",
        "udp://127.0.0.1:10101?srtt-ms=50&rate-mbps=1",
    ]);
    let paths = vec![
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Validation),
        relay_path(UnderlayProtocol::Udp, 0, RelayPathPlacement::Active),
    ];
    let missing_owner = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    let mut ledger = RequestFlightLedger::default();
    ledger.record_owner_frame(missing_owner, &data_frame(0, 64 * 1024));

    assert_eq!(
        choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
            stream_id: StreamId(7),
            context: &context,
            paths: &paths,
            lane: FlowLane::Throughput,
            frame: None,
            offset: 64 * 1024,
            payload_bytes: 64 * 1024,
            cursor: 0,
            avoid_keys: &[],
            path_flights: Some(&ledger),
            ordered_data_owner: None,
            subflow_set: None,
            request_state: None,
            attempted_subflows: None,
        }),
        BulkRelayPathChoice::Blocked
    );
}

#[test]
fn relay_lower_frontier_owner_can_lead_from_validation_attachment() {
    let context = context(&[
        "tcp://127.0.0.1:10110?srtt-ms=50&rate-mbps=1",
        "udp://127.0.0.1:10111?srtt-ms=50&rate-mbps=1",
    ]);
    let lower_owner = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let paths = vec![
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Validation),
        relay_path(UnderlayProtocol::Udp, 0, RelayPathPlacement::Active),
    ];
    let mut ledger = RequestFlightLedger::default();
    ledger.record_owner_frame(lower_owner, &data_frame(0, 64 * 1024));

    assert_eq!(
        choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
            stream_id: StreamId(7),
            context: &context,
            paths: &paths,
            lane: FlowLane::Throughput,
            frame: None,
            offset: 64 * 1024,
            payload_bytes: 64 * 1024,
            cursor: 0,
            avoid_keys: &[],
            path_flights: Some(&ledger),
            ordered_data_owner: None,
            subflow_set: None,
            request_state: None,
            attempted_subflows: None,
        }),
        BulkRelayPathChoice::Selected(paths[0].instance())
    );
}

#[test]
fn relay_bulk_lead_must_be_admissible_not_lowest_raw_eta() {
    let context = context(&[
        "udp://127.0.0.1:10120?srtt-ms=20&rate-mbps=500",
        "udp://127.0.0.1:10121?srtt-ms=30&rate-mbps=500",
    ]);
    let saturated = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let admissible = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 1,
    };
    context.mark_relay_path_rate_sample(
        admissible.underlay,
        admissible.index,
        PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(80)).expect("sender evidence"),
    );
    context.record_relay_path_send(saturated.underlay, saturated.index, 128 * 1024 * 1024);
    let paths = vec![
        relay_path(UnderlayProtocol::Udp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Udp, 1, RelayPathPlacement::Validation),
    ];

    let lead = choose_admissible_relay_bulk_lead(RelayBulkLeadRequest {
        context: &context,
        paths: &paths,
        lane: FlowLane::Throughput,
        payload_bytes: 64 * 1024,
        frame: None,
        active_key: Some(saturated),
        admitted_bulk_keys: &[saturated, admissible],
        restrict_to_admitted: true,
        lower_flight_owner: None,
        lower_owner_cross_path_debt: 0,
        policy: SchedulerPolicy::default(),
        request_state: None,
    })
    .expect("admissible path should become lead");

    assert_eq!(lead.key, admissible);
}

#[test]
fn relay_lower_owner_uses_sliding_window_not_ordering_debt() {
    let context = context(&[
        "udp://127.0.0.1:10130?srtt-ms=20&rate-mbps=500",
        "udp://127.0.0.1:10131?srtt-ms=30&rate-mbps=500",
    ]);
    let owner = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let alternate = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 1,
    };
    context.mark_relay_path_rate_sample(
        owner.underlay,
        owner.index,
        PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(80)).expect("owner evidence"),
    );
    context.mark_relay_path_rate_sample(
        alternate.underlay,
        alternate.index,
        PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(80)).expect("sender evidence"),
    );
    context.record_relay_path_send(owner.underlay, owner.index, 1024 * 1024);
    let paths = vec![
        relay_path(UnderlayProtocol::Udp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Udp, 1, RelayPathPlacement::Validation),
    ];

    let lead = choose_admissible_relay_bulk_lead(RelayBulkLeadRequest {
        context: &context,
        paths: &paths,
        lane: FlowLane::Throughput,
        payload_bytes: 64 * 1024,
        frame: None,
        active_key: Some(owner),
        admitted_bulk_keys: &[owner, alternate],
        restrict_to_admitted: true,
        lower_flight_owner: Some(owner),
        lower_owner_cross_path_debt: 1024 * 1024,
        policy: SchedulerPolicy::default(),
        request_state: None,
    })
    .expect("same-carrier lower flight is sliding-window flight");

    assert_eq!(lead.key, owner);
}

#[test]
fn ack_clock_proven_tcp_subflow_can_join_across_service_bdp_debt() {
    let context = context(&[
        "tcp://127.0.0.1:10135?srtt-ms=180&rate-mbps=500",
        "tcp://127.0.0.1:10136?srtt-ms=180&rate-mbps=500",
    ]);
    let paths = vec![
        relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Tcp, 1, RelayPathPlacement::Validation),
    ];
    let service = paths[0].instance();
    let candidate = paths[1].instance();
    mark_bulk_service(&context, service.key);
    mark_bulk_service(&context, candidate.key);

    let mut flights = RequestFlightLedger::default();
    let payload_bytes = 64 * 1024;
    let service_debt_bytes = 40 * 1024 * 1024;
    for offset in (0..service_debt_bytes).step_by(payload_bytes) {
        flights.record_owner_frame_instance(service, &data_frame(offset as u64, payload_bytes));
    }

    let scheduling = RequestSchedulingFixture::default()
        .with_rate_proven([service, candidate])
        .with_graduated([candidate])
        .with_ack_clock_proven([candidate])
        .with_calibration_spend([(candidate, 2 * 1024 * 1024)])
        .with_per_flow_rates([
            (
                service,
                RequestPerFlowRateModel {
                    rate_bps: 400_000_000.0,
                    delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
                },
            ),
            (
                candidate,
                RequestPerFlowRateModel {
                    rate_bps: 50_000_000.0,
                    delivery_samples: 1,
                },
            ),
        ]);

    assert_eq!(
        choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
            stream_id: StreamId(7),
            context: &context,
            paths: &paths,
            lane: FlowLane::Throughput,
            frame: None,
            offset: service_debt_bytes as u64,
            payload_bytes,
            cursor: 1,
            avoid_keys: &[],
            path_flights: Some(&flights),
            ordered_data_owner: Some(service.key),
            subflow_set: None,
            request_state: Some(scheduling.state()),
            attempted_subflows: None,
        }),
        BulkRelayPathChoice::Selected(paths[1].instance()),
        "a proven empty candidate must not inherit the Service prefix as its local pipe saturation"
    );
}

#[test]
fn relay_ordinary_bulk_uses_lower_eta_when_frontier_is_clear() {
    let context = context(&[
        "udp://127.0.0.1:10140?srtt-ms=50&rate-mbps=500",
        "udp://127.0.0.1:10141?srtt-ms=5&rate-mbps=500",
    ]);
    let lead_key = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let paths = vec![
        relay_path(UnderlayProtocol::Udp, 0, RelayPathPlacement::Active),
        relay_path(UnderlayProtocol::Udp, 1, RelayPathPlacement::Validation),
    ];

    assert_eq!(
        choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
            stream_id: StreamId(7),
            context: &context,
            paths: &paths,
            lane: FlowLane::Throughput,
            frame: None,
            offset: 64 * 1024,
            payload_bytes: 64 * 1024,
            cursor: 1,
            avoid_keys: &[],
            path_flights: Some(&RequestFlightLedger::default()),
            ordered_data_owner: Some(lead_key),
            subflow_set: None,
            request_state: None,
            attempted_subflows: None,
        }),
        BulkRelayPathChoice::Selected(paths[1].instance())
    );
}

#[test]
fn relay_ordinary_bulk_keeps_lead_only_inside_measured_hysteresis() {
    let mut lead = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 6.0, 500_000_000.0);
    let mut alternate = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 5.0, 500_000_000.0);
    lead.jitter_ms = 2.0;
    alternate.jitter_ms = 1.0;

    assert!(relay_path_within_adaptive_lead_hysteresis(
        6.0,
        lead,
        5.0,
        alternate,
        64 * 1024
    ));

    lead.jitter_ms = 0.0;
    alternate.jitter_ms = 0.0;

    assert!(
        !relay_path_within_adaptive_lead_hysteresis(6.0, lead, 5.0, alternate, 64 * 1024),
        "old relay lead must not survive outside measured jitter/queue hysteresis"
    );
}
