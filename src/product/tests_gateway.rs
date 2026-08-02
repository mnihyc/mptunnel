use super::*;

fn outbound(name: &str) -> OutboundId {
    OutboundId::parse(name).expect("outbound ID")
}

fn member(name: &str, weight: u32) -> GatewayMemberSpec {
    GatewayMemberSpec::new(outbound(name), weight, NetworkSet::TCP_UDP)
}

fn balancer(strategy: GatewayStrategy) -> GatewayBalancer {
    GatewayBalancer::compile(
        7,
        GatewayBalancerSpec::new(
            strategy,
            vec![member("first", 1), member("second", 1), member("third", 1)],
        ),
    )
    .expect("balancer")
}

fn zero_entropy() -> impl GatewayEntropy {
    || 0
}

fn select_name(
    balancer: &mut GatewayBalancer,
    now: u64,
    entropy: &mut impl GatewayEntropy,
) -> String {
    balancer
        .select(
            GatewayInstant::from_millis(now),
            Network::Tcp,
            None,
            &[],
            entropy,
        )
        .expect("selection")
        .member()
        .to_string()
}

#[test]
fn ordered_failover_excludes_unhealthy_draining_and_disabled_members() {
    let mut balancer = balancer(GatewayStrategy::OrderedFailover);
    let first = balancer.handle_for(&outbound("first")).expect("first");
    let second = balancer.handle_for(&outbound("second")).expect("second");
    let mut entropy = zero_entropy();

    assert_eq!(select_name(&mut balancer, 0, &mut entropy), "first");
    for now in 1..=3 {
        balancer
            .observe_passive(
                first,
                GatewayInstant::from_millis(now),
                GatewayOutcome::Failure,
            )
            .expect("failure");
    }
    assert_eq!(select_name(&mut balancer, 4, &mut entropy), "second");
    balancer
        .set_member_mode(second, GatewayMemberMode::Draining)
        .expect("drain");
    assert_eq!(select_name(&mut balancer, 5, &mut entropy), "third");
    balancer
        .set_member_mode(second, GatewayMemberMode::Disabled)
        .expect("disable");
    assert_eq!(select_name(&mut balancer, 6, &mut entropy), "third");
}

#[test]
fn round_robin_rotates_only_across_eligible_members() {
    let mut balancer = balancer(GatewayStrategy::RoundRobin);
    let second = balancer.handle_for(&outbound("second")).expect("second");
    balancer
        .set_member_mode(second, GatewayMemberMode::Draining)
        .expect("drain");
    let mut entropy = zero_entropy();
    let selected = (0..6)
        .map(|now| select_name(&mut balancer, now, &mut entropy))
        .collect::<Vec<_>>();
    assert_eq!(
        selected,
        ["first", "third", "first", "third", "first", "third",]
    );
}

#[test]
fn selection_filters_capabilities_before_health_and_stickiness() {
    let mut spec = GatewayBalancerSpec::new(
        GatewayStrategy::RoundRobin,
        vec![
            GatewayMemberSpec::new(outbound("tcp-only"), 1, NetworkSet::TCP),
            GatewayMemberSpec::new(outbound("udp-only"), 1, NetworkSet::UDP),
        ],
    );
    spec.stickiness = Some(GatewayStickinessPolicy {
        ttl: Duration::from_millis(100),
        capacity: 4,
    });
    let mut balancer = GatewayBalancer::compile(1, spec).expect("balancer");
    let target = ProtocolTarget::parse_authority("same.example:443").expect("target");
    let mut entropy = zero_entropy();

    let tcp = balancer
        .select(
            GatewayInstant::ZERO,
            Network::Tcp,
            Some(&target),
            &[],
            &mut entropy,
        )
        .expect("TCP selection");
    assert_eq!(tcp.member().as_str(), "tcp-only");

    let udp = balancer
        .select(
            GatewayInstant::ZERO,
            Network::Udp,
            Some(&target),
            &[],
            &mut entropy,
        )
        .expect("UDP selection");
    assert_eq!(udp.member().as_str(), "udp-only");

    let tcp_sticky = balancer
        .select(
            GatewayInstant::from_millis(1),
            Network::Tcp,
            Some(&target),
            &[],
            &mut entropy,
        )
        .expect("TCP sticky selection");
    assert_eq!(tcp_sticky.member().as_str(), "tcp-only");
    assert_eq!(
        tcp_sticky.reason(),
        GatewaySelectionReason::DestinationSticky
    );
}

#[test]
fn per_flow_exclusions_bound_failover_without_mutating_member_health() {
    let mut balancer = balancer(GatewayStrategy::OrderedFailover);
    let mut entropy = zero_entropy();
    let first = balancer
        .select(GatewayInstant::ZERO, Network::Tcp, None, &[], &mut entropy)
        .expect("first");
    let first_handle = first.handle();
    assert_eq!(first.member().as_str(), "first");

    let second = balancer
        .select(
            GatewayInstant::ZERO,
            Network::Tcp,
            None,
            &[first_handle],
            &mut entropy,
        )
        .expect("second");
    assert_eq!(second.member().as_str(), "second");
    assert_eq!(
        balancer
            .member_status(first_handle, GatewayInstant::ZERO)
            .expect("first status")
            .health,
        GatewayHealthStatus::Healthy
    );
}

#[test]
fn no_compatible_member_is_distinct_from_no_enabled_member() {
    let mut balancer = GatewayBalancer::compile(
        1,
        GatewayBalancerSpec::new(
            GatewayStrategy::OrderedFailover,
            vec![GatewayMemberSpec::new(
                outbound("tcp-only"),
                1,
                NetworkSet::TCP,
            )],
        ),
    )
    .expect("balancer");
    assert!(matches!(
        balancer.select(
            GatewayInstant::ZERO,
            Network::Udp,
            None,
            &[],
            &mut zero_entropy()
        ),
        Err(GatewaySelectionError::NoCompatibleMembers(Network::Udp))
    ));
}

#[test]
fn weighted_random_maps_deterministic_entropy_to_exact_weight_buckets() {
    let mut balancer = GatewayBalancer::compile(
        1,
        GatewayBalancerSpec::new(
            GatewayStrategy::WeightedRandom,
            vec![member("one", 1), member("three", 3), member("four", 4)],
        ),
    )
    .expect("balancer");
    let draws = [
        0,
        u64::MAX / 8,
        u64::MAX / 8 + 1,
        u64::MAX / 2 + 1,
        u64::MAX,
    ];
    let mut index = 0;
    let mut entropy = || {
        let draw = draws[index];
        index += 1;
        draw
    };
    let selected = (0..draws.len())
        .map(|now| select_name(&mut balancer, now as u64, &mut entropy))
        .collect::<Vec<_>>();
    assert_eq!(selected, ["one", "one", "three", "four", "four",]);
}

#[test]
fn least_latency_uses_product_observations_and_stable_ties() {
    let mut spec = GatewayBalancerSpec::new(
        GatewayStrategy::LeastLatency,
        vec![member("first", 1), member("second", 1), member("third", 1)],
    );
    spec.freshness_ttl = Duration::from_millis(10);
    let mut balancer = GatewayBalancer::compile(7, spec).expect("balancer");
    let first = balancer.handle_for(&outbound("first")).expect("first");
    let second = balancer.handle_for(&outbound("second")).expect("second");
    let third = balancer.handle_for(&outbound("third")).expect("third");
    for (handle, latency) in [(first, 30), (second, 10), (third, 10)] {
        balancer
            .observe_passive(
                handle,
                GatewayInstant::ZERO,
                GatewayOutcome::Success {
                    latency: Some(Duration::from_millis(latency)),
                },
            )
            .expect("observation");
    }
    let mut entropy = zero_entropy();
    assert_eq!(select_name(&mut balancer, 1, &mut entropy), "second");

    balancer
        .observe_outcome(
            second,
            GatewayInstant::from_millis(20),
            GatewayObservationSource::PassiveFlow,
            GatewayOutcome::Success { latency: None },
            None,
        )
        .expect("flow completion without a latency sample");
    assert_eq!(
        select_name(&mut balancer, 21, &mut entropy),
        "first",
        "a non-latency outcome must not refresh stale latency evidence"
    );
    let second_status = balancer
        .member_status(second, GatewayInstant::from_millis(21))
        .expect("status");
    assert_eq!(
        second_status.last_latency_observation,
        Some(GatewayInstant::ZERO)
    );
    assert_eq!(
        second_status.last_latency_observation_source,
        Some(GatewayObservationSource::PassiveOpen)
    );
}

#[test]
fn least_load_normalizes_by_member_weight() {
    let mut balancer = GatewayBalancer::compile(
        1,
        GatewayBalancerSpec::new(
            GatewayStrategy::LeastLoad,
            vec![member("small", 1), member("large", 4), member("idle", 1)],
        ),
    )
    .expect("balancer");
    for (name, active) in [("small", 2), ("large", 4), ("idle", 3)] {
        let handle = balancer.handle_for(&outbound(name)).expect("handle");
        balancer
            .set_load(
                handle,
                GatewayLoad {
                    active_flows: active,
                    pending_flows: 0,
                },
            )
            .expect("load");
    }
    let mut entropy = zero_entropy();
    assert_eq!(select_name(&mut balancer, 0, &mut entropy), "large");
}

#[test]
fn stickiness_is_per_destination_ttl_bounded_and_health_aware() {
    let mut spec = GatewayBalancerSpec::new(
        GatewayStrategy::RoundRobin,
        vec![member("first", 1), member("second", 1)],
    );
    spec.health.failure_threshold = 1;
    spec.stickiness = Some(GatewayStickinessPolicy {
        ttl: Duration::from_millis(100),
        capacity: 1,
    });
    let mut balancer = GatewayBalancer::compile(1, spec).expect("balancer");
    let a = ProtocolTarget::parse_authority("a.example:443").expect("target");
    let b = ProtocolTarget::parse_authority("b.example:443").expect("target");
    let mut entropy = zero_entropy();

    let first = balancer
        .select(
            GatewayInstant::ZERO,
            Network::Tcp,
            Some(&a),
            &[],
            &mut entropy,
        )
        .expect("selection")
        .handle();
    let sticky = balancer
        .select(
            GatewayInstant::from_millis(50),
            Network::Tcp,
            Some(&a),
            &[],
            &mut entropy,
        )
        .expect("sticky");
    assert_eq!(sticky.handle(), first);
    assert_eq!(sticky.reason(), GatewaySelectionReason::DestinationSticky);

    let replacement = balancer
        .select(
            GatewayInstant::from_millis(51),
            Network::Tcp,
            Some(&b),
            &[],
            &mut entropy,
        )
        .expect("replacement")
        .handle();
    assert_ne!(replacement, first);
    let after_eviction = balancer
        .select(
            GatewayInstant::from_millis(52),
            Network::Tcp,
            Some(&a),
            &[],
            &mut entropy,
        )
        .expect("after eviction");
    assert_eq!(after_eviction.handle(), first);
    assert_ne!(
        after_eviction.reason(),
        GatewaySelectionReason::DestinationSticky
    );

    balancer
        .observe_passive(
            first,
            GatewayInstant::from_millis(53),
            GatewayOutcome::Failure,
        )
        .expect("failure");
    assert_ne!(
        balancer
            .select(
                GatewayInstant::from_millis(54),
                Network::Tcp,
                Some(&a),
                &[],
                &mut entropy,
            )
            .expect("health failover")
            .handle(),
        first
    );

    let expired = balancer
        .select(
            GatewayInstant::from_millis(200),
            Network::Tcp,
            Some(&b),
            &[],
            &mut entropy,
        )
        .expect("expired");
    assert_ne!(expired.reason(), GatewaySelectionReason::DestinationSticky);
}

#[test]
fn failure_backoff_caps_and_recovery_requires_hysteresis_successes() {
    let mut spec =
        GatewayBalancerSpec::new(GatewayStrategy::OrderedFailover, vec![member("only", 1)]);
    spec.health = GatewayHealthPolicy {
        failure_threshold: 2,
        recovery_threshold: 2,
        initial_backoff: Duration::from_millis(10),
        maximum_backoff: Duration::from_millis(40),
    };
    let mut balancer = GatewayBalancer::compile(1, spec).expect("balancer");
    let handle = balancer.handle_for(&outbound("only")).expect("handle");

    for now in [0, 1] {
        balancer
            .observe_passive(
                handle,
                GatewayInstant::from_millis(now),
                GatewayOutcome::Failure,
            )
            .expect("failure");
    }
    assert_eq!(
        balancer
            .member_status(handle, GatewayInstant::from_millis(1))
            .expect("status")
            .health,
        GatewayHealthStatus::BackingOff {
            until: GatewayInstant::from_millis(11)
        }
    );
    for now in [11, 31, 71] {
        assert!(
            balancer
                .claim_recovery_probe(handle, GatewayInstant::from_millis(now))
                .unwrap()
        );
        balancer
            .observe_passive(
                handle,
                GatewayInstant::from_millis(now),
                GatewayOutcome::Failure,
            )
            .expect("failure");
    }
    assert_eq!(
        balancer
            .member_status(handle, GatewayInstant::from_millis(71))
            .expect("status")
            .health,
        GatewayHealthStatus::BackingOff {
            until: GatewayInstant::from_millis(111)
        }
    );

    assert!(
        balancer
            .claim_recovery_probe(handle, GatewayInstant::from_millis(111))
            .unwrap()
    );
    balancer
        .observe_passive(
            handle,
            GatewayInstant::from_millis(111),
            GatewayOutcome::Success { latency: None },
        )
        .expect("first recovery success");
    assert_eq!(
        balancer
            .member_status(handle, GatewayInstant::from_millis(111))
            .expect("status")
            .health,
        GatewayHealthStatus::RecoveryProbeEligible
    );
    assert!(
        balancer
            .claim_recovery_probe(handle, GatewayInstant::from_millis(111))
            .unwrap()
    );
    balancer
        .observe_passive(
            handle,
            GatewayInstant::from_millis(111),
            GatewayOutcome::Success { latency: None },
        )
        .expect("second recovery success");
    assert_eq!(
        balancer
            .member_status(handle, GatewayInstant::from_millis(111))
            .expect("status")
            .health,
        GatewayHealthStatus::Healthy
    );
}

#[test]
fn all_failed_fallback_is_deterministic_and_honors_probe_backoff() {
    let mut spec = GatewayBalancerSpec::new(
        GatewayStrategy::WeightedRandom,
        vec![member("first", 1), member("second", 100)],
    );
    spec.health.failure_threshold = 1;
    spec.health.initial_backoff = Duration::from_millis(10);
    spec.health.maximum_backoff = Duration::from_millis(10);
    let mut balancer = GatewayBalancer::compile(1, spec).expect("balancer");
    for name in ["first", "second"] {
        let handle = balancer.handle_for(&outbound(name)).expect("handle");
        balancer
            .observe_passive(handle, GatewayInstant::ZERO, GatewayOutcome::Failure)
            .expect("failure");
    }
    let mut entropy = || u64::MAX;
    let deferred = balancer
        .select(
            GatewayInstant::from_millis(5),
            Network::Tcp,
            None,
            &[],
            &mut entropy,
        )
        .expect("fallback");
    assert_eq!(deferred.member().to_string(), "first");
    assert_eq!(
        deferred.reason(),
        GatewaySelectionReason::AllUnhealthyDeferred {
            until: GatewayInstant::from_millis(10)
        }
    );
    assert!(!deferred.may_attempt());

    let probe = balancer
        .select(
            GatewayInstant::from_millis(10),
            Network::Tcp,
            None,
            &[],
            &mut entropy,
        )
        .expect("probe");
    assert_eq!(probe.member().to_string(), "first");
    assert_eq!(
        probe.reason(),
        GatewaySelectionReason::AllUnhealthyRecoveryProbe
    );
    assert!(probe.may_attempt());

    let other = balancer
        .select(
            GatewayInstant::from_millis(10),
            Network::Tcp,
            None,
            &[],
            &mut entropy,
        )
        .expect("other recovery probe");
    assert_eq!(other.member().to_string(), "second");
    assert_eq!(
        other.reason(),
        GatewaySelectionReason::AllUnhealthyRecoveryProbe
    );
}

#[test]
fn no_enabled_member_does_not_fall_back_to_draining_or_disabled() {
    let mut balancer = balancer(GatewayStrategy::OrderedFailover);
    for (index, name) in ["first", "second", "third"].into_iter().enumerate() {
        let handle = balancer.handle_for(&outbound(name)).expect("handle");
        let mode = if index == 0 {
            GatewayMemberMode::Draining
        } else {
            GatewayMemberMode::Disabled
        };
        balancer.set_member_mode(handle, mode).expect("mode");
    }
    assert!(matches!(
        balancer.select(
            GatewayInstant::ZERO,
            Network::Tcp,
            None,
            &[],
            &mut zero_entropy()
        ),
        Err(GatewaySelectionError::NoEnabledMembers(Network::Tcp))
    ));
}

#[test]
fn compile_rejects_unbounded_or_ambiguous_state() {
    let duplicate = GatewayBalancerSpec::new(
        GatewayStrategy::RoundRobin,
        vec![member("same", 1), member("same", 2)],
    );
    assert!(matches!(
        GatewayBalancer::compile(1, duplicate),
        Err(GatewayCompileError::DuplicateMember(_))
    ));

    let too_many = GatewayBalancerSpec::new(
        GatewayStrategy::RoundRobin,
        (0..=MAX_GATEWAY_MEMBERS)
            .map(|index| member(&format!("member-{index}"), 1))
            .collect(),
    );
    assert!(matches!(
        GatewayBalancer::compile(1, too_many),
        Err(GatewayCompileError::TooManyMembers { .. })
    ));

    let no_capability = GatewayBalancerSpec::new(
        GatewayStrategy::RoundRobin,
        vec![GatewayMemberSpec::new(
            outbound("none"),
            1,
            NetworkSet::NONE,
        )],
    );
    assert!(matches!(
        GatewayBalancer::compile(1, no_capability),
        Err(GatewayCompileError::MissingNetworkCapability(_))
    ));
}

#[test]
fn stale_time_cannot_reopen_expired_backoff_or_stickiness() {
    let mut spec = GatewayBalancerSpec::new(
        GatewayStrategy::RoundRobin,
        vec![member("first", 1), member("second", 1)],
    );
    spec.stickiness = Some(GatewayStickinessPolicy {
        ttl: Duration::from_millis(10),
        capacity: 2,
    });
    let mut balancer = GatewayBalancer::compile(1, spec).expect("balancer");
    let target = ProtocolTarget::parse_authority("time.example:443").expect("target");
    let mut entropy = zero_entropy();
    balancer
        .select(
            GatewayInstant::from_millis(100),
            Network::Tcp,
            Some(&target),
            &[],
            &mut entropy,
        )
        .expect("initial");
    let stale = balancer
        .select(
            GatewayInstant::from_millis(1),
            Network::Tcp,
            Some(&target),
            &[],
            &mut entropy,
        )
        .expect("stale timestamp");
    assert_eq!(stale.reason(), GatewaySelectionReason::DestinationSticky);
    let expired = balancer
        .select(
            GatewayInstant::from_millis(110),
            Network::Tcp,
            Some(&target),
            &[],
            &mut entropy,
        )
        .expect("expired");
    assert_ne!(expired.reason(), GatewaySelectionReason::DestinationSticky);
}

#[test]
fn manual_and_random_are_explicit_distinct_strategies() {
    let mut manual_spec = GatewayBalancerSpec::new(
        GatewayStrategy::Manual,
        vec![member("first", 1), member("second", 1)],
    );
    manual_spec.manual_member = Some(outbound("second"));
    let mut manual = GatewayBalancer::compile(1, manual_spec).expect("manual balancer");
    let mut entropy = zero_entropy();
    let selected = manual
        .select(GatewayInstant::ZERO, Network::Tcp, None, &[], &mut entropy)
        .expect("manual selection");
    assert_eq!(selected.member().as_str(), "second");
    assert_eq!(selected.reason(), GatewaySelectionReason::Manual);
    assert_eq!(
        manual.set_manual_override(None),
        Err(GatewayStateError::ManualStrategyRequiresOverride)
    );
    let first = manual.handle_for(&outbound("first")).expect("first");
    manual
        .set_member_mode(first, GatewayMemberMode::Disabled)
        .expect("disable");
    assert_eq!(
        manual.set_manual_override(Some(first)),
        Err(GatewayStateError::ManualOverrideMemberNotEnabled)
    );

    let mut random = balancer(GatewayStrategy::Random);
    let draws = [0, u64::MAX];
    let mut position = 0;
    let mut entropy = || {
        let draw = draws[position];
        position += 1;
        draw
    };
    assert_eq!(select_name(&mut random, 0, &mut entropy), "first");
    assert_eq!(select_name(&mut random, 1, &mut entropy), "third");
}

#[test]
fn manual_member_recovers_after_cooldown_without_active_probes() {
    let mut spec = GatewayBalancerSpec::new(GatewayStrategy::Manual, vec![member("only", 1)]);
    spec.manual_member = Some(outbound("only"));
    spec.health = GatewayHealthPolicy {
        failure_threshold: 1,
        recovery_threshold: 1,
        initial_backoff: Duration::from_millis(10),
        maximum_backoff: Duration::from_millis(10),
    };
    let mut balancer = GatewayBalancer::compile(1, spec).expect("manual balancer");
    let mut entropy = zero_entropy();
    let first = balancer
        .select(GatewayInstant::ZERO, Network::Tcp, None, &[], &mut entropy)
        .expect("initial manual selection");
    let handle = first.handle();
    balancer
        .observe_passive(handle, GatewayInstant::ZERO, GatewayOutcome::Failure)
        .expect("eject manual member");

    let deferred = balancer
        .select(
            GatewayInstant::from_millis(9),
            Network::Tcp,
            None,
            &[],
            &mut entropy,
        )
        .expect("deterministic deferred recovery plan");
    assert!(matches!(
        deferred.reason(),
        GatewaySelectionReason::AllUnhealthyDeferred { .. }
    ));
    assert!(!deferred.may_attempt());

    let recovery = balancer
        .select(
            GatewayInstant::from_millis(10),
            Network::Tcp,
            None,
            &[],
            &mut entropy,
        )
        .expect("single recovery attempt");
    assert_eq!(
        recovery.reason(),
        GatewaySelectionReason::AllUnhealthyRecoveryProbe
    );
    assert!(recovery.may_attempt());
    balancer
        .observe_passive(
            handle,
            GatewayInstant::from_millis(10),
            GatewayOutcome::Success { latency: None },
        )
        .expect("recover manual member");

    assert_eq!(
        balancer
            .select(
                GatewayInstant::from_millis(11),
                Network::Tcp,
                None,
                &[],
                &mut entropy,
            )
            .expect("ordinary manual selection after recovery")
            .reason(),
        GatewaySelectionReason::Manual
    );
}

#[test]
fn principal_stickiness_is_independent_of_destination() {
    let mut spec = GatewayBalancerSpec::new(
        GatewayStrategy::RoundRobin,
        vec![member("first", 1), member("second", 1)],
    );
    spec.stickiness = Some(GatewayStickinessPolicy {
        ttl: Duration::from_secs(1),
        capacity: 4,
    });
    spec.stickiness_key = GatewayStickinessKey::Principal;
    let mut balancer = GatewayBalancer::compile(1, spec).expect("balancer");
    let alice = PrincipalId::parse("alice").expect("principal");
    let bob = PrincipalId::parse("bob").expect("principal");
    let first_target = ProtocolTarget::parse_authority("first.example:443").expect("target");
    let second_target = ProtocolTarget::parse_authority("second.example:443").expect("target");
    let mut entropy = zero_entropy();

    let first = balancer
        .select_with_principal(
            GatewayInstant::ZERO,
            Network::Tcp,
            Some(&first_target),
            Some(&alice),
            &[],
            &mut entropy,
        )
        .expect("initial selection")
        .handle();
    let sticky = balancer
        .select_with_principal(
            GatewayInstant::from_millis(1),
            Network::Tcp,
            Some(&second_target),
            Some(&alice),
            &[],
            &mut entropy,
        )
        .expect("principal sticky selection");
    assert_eq!(sticky.handle(), first);
    assert_eq!(sticky.reason(), GatewaySelectionReason::PrincipalSticky);
    let other = balancer
        .select_with_principal(
            GatewayInstant::from_millis(2),
            Network::Tcp,
            Some(&first_target),
            Some(&bob),
            &[],
            &mut entropy,
        )
        .expect("other principal");
    assert_ne!(other.handle(), first);
}

#[test]
fn active_probe_feedback_tracks_freshness_errors_and_circuit_counters() {
    let mut spec =
        GatewayBalancerSpec::new(GatewayStrategy::OrderedFailover, vec![member("only", 1)]);
    spec.health.failure_threshold = 1;
    spec.health.recovery_threshold = 1;
    spec.health.initial_backoff = Duration::from_millis(10);
    spec.health.maximum_backoff = Duration::from_millis(10);
    spec.freshness_ttl = Duration::from_millis(20);
    spec.probe = Some(GatewayProbePolicy {
        target: ProtocolTarget::parse_authority("192.0.2.1:443").expect("probe target"),
        interval: Duration::from_millis(100),
        timeout: Duration::from_millis(20),
    });
    let mut balancer = GatewayBalancer::compile(1, spec).expect("balancer");
    let handle = balancer.handle_for(&outbound("only")).expect("member");

    assert!(
        balancer
            .claim_active_probe(handle, GatewayInstant::ZERO)
            .expect("claim")
    );
    balancer
        .observe_outcome(
            handle,
            GatewayInstant::ZERO,
            GatewayObservationSource::ActiveProbe,
            GatewayOutcome::Failure,
            Some("probe timeout".to_string()),
        )
        .expect("failure");
    let failed = balancer
        .member_status(handle, GatewayInstant::ZERO)
        .expect("status");
    assert_eq!(failed.last_error, Some("probe timeout"));
    assert_eq!(failed.counters.probes, 1);
    assert_eq!(failed.counters.probe_failures, 1);
    assert_eq!(failed.counters.ejections, 1);
    assert!(matches!(
        failed.freshness,
        GatewayFreshnessStatus::Fresh { .. }
    ));

    assert!(
        balancer
            .claim_active_probe(handle, GatewayInstant::from_millis(10))
            .expect("recovery claim")
    );
    balancer
        .observe_outcome(
            handle,
            GatewayInstant::from_millis(10),
            GatewayObservationSource::ActiveProbe,
            GatewayOutcome::Success {
                latency: Some(Duration::from_millis(5)),
            },
            None,
        )
        .expect("recovery");
    let recovered = balancer
        .member_status(handle, GatewayInstant::from_millis(31))
        .expect("status");
    assert_eq!(recovered.health, GatewayHealthStatus::Healthy);
    assert_eq!(recovered.counters.recoveries, 1);
    assert!(matches!(
        recovered.freshness,
        GatewayFreshnessStatus::Stale { .. }
    ));
}
