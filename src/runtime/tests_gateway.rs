use super::*;
use crate::product::{
    GatewayBalancerSpec, GatewayHealthPolicy, GatewayMemberSpec, GatewayStrategy, NetworkSet,
    OutboundId,
};

fn config() -> GatewayBalancerConfig {
    let outbound = OutboundId::parse("edge-a").expect("outbound ID");
    let mut spec = GatewayBalancerSpec::new(
        GatewayStrategy::OrderedFailover,
        vec![GatewayMemberSpec::new(outbound, 1, NetworkSet::TCP_UDP)],
    );
    spec.health = GatewayHealthPolicy {
        failure_threshold: 1,
        recovery_threshold: 1,
        initial_backoff: Duration::from_millis(2),
        maximum_backoff: Duration::from_millis(2),
    };
    GatewayBalancerConfig {
        id: crate::product::BalancerId::parse("gateway").expect("balancer ID"),
        generation: 7,
        spec,
    }
}

fn destination() -> ProtocolTarget {
    ProtocolTarget::from_host_port("example.com", 443).expect("destination")
}

#[test]
fn pending_drop_only_balances_load_and_does_not_invent_failure() {
    let runtime = ClientGatewayRuntime::compile(&config()).expect("runtime");
    drop(
        runtime
            .select(Network::Tcp, &destination(), &[])
            .expect("first selection"),
    );
    drop(
        runtime
            .select(Network::Tcp, &destination(), &[])
            .expect("pending drop leaves member healthy"),
    );
}

#[test]
fn failed_open_ejects_member_then_recovery_probe_restores_it() {
    let runtime = ClientGatewayRuntime::compile(&config()).expect("runtime");
    let mut failed = runtime
        .select(Network::Tcp, &destination(), &[])
        .expect("initial selection");
    failed
        .lease
        .failed("injected open failure")
        .expect("failure feedback");
    assert!(matches!(
        runtime.select(Network::Tcp, &destination(), &[]),
        Err(RuntimeError::GatewayUnavailable(_))
    ));

    std::thread::sleep(Duration::from_millis(4));
    let mut recovery = runtime
        .select(Network::Tcp, &destination(), &[])
        .expect("recovery probe selection");
    recovery.lease.opened().expect("recovery success");
    drop(recovery);

    drop(
        runtime
            .select(Network::Tcp, &destination(), &[])
            .expect("member recovered for ordinary selection"),
    );
}

#[test]
fn abandoned_recovery_probe_releases_probe_ownership_without_failure() {
    let runtime = ClientGatewayRuntime::compile(&config()).expect("runtime");
    let mut failed = runtime
        .select(Network::Tcp, &destination(), &[])
        .expect("initial selection");
    failed
        .lease
        .failed("injected open failure")
        .expect("failure feedback");
    std::thread::sleep(Duration::from_millis(4));

    drop(
        runtime
            .select(Network::Tcp, &destination(), &[])
            .expect("first recovery probe"),
    );
    drop(
        runtime
            .select(Network::Tcp, &destination(), &[])
            .expect("abandoned recovery probe can be reclaimed"),
    );
}

#[test]
fn webhook_connections_account_load_without_passive_health_feedback() {
    let runtime = ClientGatewayRuntime::compile(&config()).expect("runtime");
    let mut failed = runtime
        .select_for_webhook(Network::Tcp, &destination(), &[])
        .expect("internal selection");
    assert_eq!(runtime.snapshot().unwrap().members[0].load.pending_flows, 1);
    failed
        .lease
        .failed("callback transport failed")
        .expect("release pending");
    let member = runtime.snapshot().unwrap().members.remove(0);
    assert_eq!(member.health, GatewayHealthStatus::Healthy);
    assert_eq!(member.load.total(), 0);
    assert_eq!(member.consecutive_failures, 0);
    assert_eq!(member.counters.open_failures, 0);
    assert_eq!(member.last_observation, None);

    let mut delivered = runtime
        .select_for_webhook(Network::Tcp, &destination(), &[])
        .expect("next internal selection");
    delivered.lease.opened().expect("opened");
    assert_eq!(runtime.snapshot().unwrap().members[0].load.active_flows, 1);
    delivered
        .lease
        .completed(Some("HTTP callback failed".into()))
        .expect("completed");
    let member = runtime.snapshot().unwrap().members.remove(0);
    assert_eq!(member.health, GatewayHealthStatus::Healthy);
    assert_eq!(member.load.total(), 0);
    assert_eq!(member.counters.flow_failures, 0);
    assert_eq!(member.last_observation, None);
}

#[test]
fn gateway_webhooks_emit_committed_changes_and_one_probe_completion() {
    use crate::runtime::webhook::EventPublisher;
    use crate::webhook::EventKind;
    let runtime = ClientGatewayRuntime::compile(&config()).unwrap();
    let (publisher, capture) = EventPublisher::test_capture(
        &[
            EventKind::BalancerMemberChanged,
            EventKind::BalancerProbeCompleted,
        ],
        32,
    );
    runtime.attach_webhook_publisher(publisher);
    let member = OutboundId::parse("edge-a").unwrap();
    runtime
        .set_member_mode(&member, GatewayMemberMode::Draining)
        .unwrap();
    runtime
        .set_member_mode(&member, GatewayMemberMode::Draining)
        .unwrap();
    runtime
        .set_member_mode(&member, GatewayMemberMode::Enabled)
        .unwrap();
    assert_eq!(capture.snapshot().len(), 2, "same policy is silent");
    {
        let _cancelled = runtime.begin_active_probe(&member).unwrap().unwrap();
        assert!(runtime.begin_active_probe(&member).unwrap().is_none());
    }
    let mut probe = runtime.begin_active_probe(&member).unwrap().unwrap();
    probe.succeeded().unwrap();
    probe.succeeded().unwrap();
    drop(probe);
    let mut probe = runtime.begin_active_probe(&member).unwrap().unwrap();
    probe.failed("test failure").unwrap();
    drop(probe);
    std::thread::sleep(Duration::from_millis(4));
    let mut repeated = runtime.begin_active_probe(&member).unwrap().unwrap();
    repeated.failed("still unavailable").unwrap();
    drop(repeated);
    let events = capture.snapshot();
    let probes = events
        .iter()
        .filter(|event| event["event"]["type"] == "balancer.probe_completed")
        .map(|event| event["probe"]["outcome"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(probes, ["cancelled", "success", "failure", "failure"]);
    assert_eq!(
        events
            .iter()
            .filter(|event| event["change"]["to"] == "unhealthy")
            .count(),
        1,
        "repeated failed probes are not repeated health transitions"
    );
    assert!(events.windows(2).all(|pair| pair[0]["subject_sequence"].as_u64() < pair[1]["subject_sequence"].as_u64()));
    assert_eq!(capture.dropped(), 0);
}
