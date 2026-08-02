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
