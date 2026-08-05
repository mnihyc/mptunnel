use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use ipnet::IpNet;
use mptunnel::product::{
    AclEffect, AclRuleSpec, CompiledRouteTable, DestinationAcl, DomainName, EgressAction,
    FlowContext, GatewayBalancer, GatewayBalancerSpec, GatewayInstant, GatewayMemberSpec,
    GatewayStickinessKey, GatewayStickinessPolicy, GatewayStrategy, InboundId, InitialDemand,
    Network, NetworkSet, OutboundId, PortRange, PrincipalId, ProtocolTarget,
    RULE_SET_SIGNATURE_CONTEXT, RouteAction, RouteInput, RouteMatchSpec, RouteRuleSpec, RuleId,
    RuleSetPublisher, RuleSetPublisherCatalog, RuleSetPublisherId, SourceEndpoint, VerifiedRuleSet,
};
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::net::IpAddr;
use std::sync::Arc;

struct CountingAllocator;

struct AllocationState {
    track: Cell<bool>,
    allocations: Cell<usize>,
}

thread_local! {
    static ALLOCATION_STATE: AllocationState = const {
        AllocationState {
            track: Cell::new(false),
            allocations: Cell::new(0),
        }
    };
}

fn record_allocation() {
    let _ = ALLOCATION_STATE.try_with(|state| {
        if state.track.get() {
            state.allocations.set(state.allocations.get() + 1);
        }
    });
}

fn begin_measurement() {
    ALLOCATION_STATE.with(|state| {
        state.allocations.set(0);
        state.track.set(true);
    });
}

fn finish_measurement() -> usize {
    ALLOCATION_STATE.with(|state| {
        state.track.set(false);
        state.allocations.get()
    })
}

// SAFETY: Every operation is delegated unchanged to the process System
// allocator. Thread-local state counts only allocations made by the measured
// synchronous hot path, excluding unrelated parallel test-harness work.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: The caller supplies the GlobalAlloc contract and the same
        // layout is delegated to System.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The pointer and layout came from the delegated System
        // allocator operation above.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: The caller supplies the GlobalAlloc contract and the same
        // layout is delegated to System.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation();
        // SAFETY: The caller supplies the GlobalAlloc contract and all
        // arguments are delegated unchanged to System.
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn route_id(value: &str) -> RuleId {
    RuleId::parse(value).expect("rule ID")
}

fn signed_rule_set() -> Arc<VerifiedRuleSet> {
    let payload = serde_json::to_vec(&json!({
        "schema": 1,
        "id": "allocation-gate",
        "revision": 1,
        "expires_at_unix_secs": null,
        "domain_exact": ["api.service.example"],
        "domain_suffix": [],
        "destination_cidrs": ["203.0.113.0/24"]
    }))
    .expect("rule-set payload");
    let checksum: [u8; 32] = Sha256::digest(&payload).into();
    let key = Ed25519KeyPair::from_seed_unchecked(&[19_u8; 32]).expect("test signing key");
    let mut signed = RULE_SET_SIGNATURE_CONTEXT.to_vec();
    signed.extend_from_slice(&checksum);
    let signature = key.sign(&signed);
    let envelope = serde_json::to_vec(&json!({
        "schema": 1,
        "publisher": "test",
        "checksum_sha256": checksum
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        "payload_base64": BASE64.encode(payload),
        "signature_base64": BASE64.encode(signature.as_ref())
    }))
    .expect("rule-set envelope");
    let public_key: [u8; 32] = key
        .public_key()
        .as_ref()
        .try_into()
        .expect("Ed25519 public key");
    let catalog = RuleSetPublisherCatalog::compile(vec![RuleSetPublisher::new(
        RuleSetPublisherId::parse("test").expect("publisher ID"),
        public_key,
    )])
    .expect("publisher catalog");
    Arc::new(VerifiedRuleSet::verify_json(&envelope, &catalog, 0).expect("verified rule set"))
}

fn matcher(rule_set: Arc<VerifiedRuleSet>) -> RouteMatchSpec {
    RouteMatchSpec {
        domain_exact: vec![DomainName::parse("not-this.example").expect("domain")],
        domain_suffix: vec![DomainName::parse("other.example").expect("domain")],
        domain_keyword: vec!["nomatch".to_owned()],
        domain_regex: vec![r"^not-this-either\.example$".to_owned()],
        domain_rule_sets: vec![rule_set.clone()],
        destination_cidrs: vec!["192.0.2.0/24".parse::<IpNet>().expect("CIDR")],
        destination_rule_sets: vec![rule_set],
        source_cidrs: vec!["198.51.100.0/24".parse::<IpNet>().expect("CIDR")],
        destination_ports: vec![PortRange::single(443)],
        source_ports: vec![PortRange::new(40_000, 50_000).expect("range")],
        networks: vec![Network::Tcp],
        inbounds: vec![InboundId::parse("socks").expect("inbound")],
        principals: vec![PrincipalId::parse("alice").expect("principal")],
        ..RouteMatchSpec::default()
    }
}

#[test]
fn warmed_route_and_acl_classification_allocate_nothing() {
    let rule_set = signed_rule_set();
    let table = CompiledRouteTable::compile(
        1,
        vec![
            RouteRuleSpec::new(
                route_id("specific"),
                matcher(rule_set.clone()),
                RouteAction::new(
                    EgressAction::Outbound(OutboundId::parse("edge").expect("outbound")),
                    None,
                    InitialDemand::Automatic,
                ),
            ),
            RouteRuleSpec::new(
                route_id("default"),
                RouteMatchSpec::default(),
                RouteAction::direct(InitialDemand::Automatic),
            ),
        ],
    )
    .expect("route table");
    let acl = DestinationAcl::compile(
        1,
        vec![AclRuleSpec::new(
            route_id("public-service"),
            matcher(rule_set),
            AclEffect::Allow,
        )],
    )
    .expect("ACL");
    let flow = FlowContext::new(
        Network::Tcp,
        ProtocolTarget::from_host_port("api.service.example", 443).expect("target"),
        SourceEndpoint::new("198.51.100.5".parse::<IpAddr>().expect("source"), 45_000),
        PrincipalId::parse("alice").expect("principal"),
        InboundId::parse("socks").expect("inbound"),
    );
    let input =
        RouteInput::post_resolution(&flow, "203.0.113.7".parse().expect("destination address"));
    let mut balancer = GatewayBalancer::compile(
        1,
        GatewayBalancerSpec::new(
            GatewayStrategy::RoundRobin,
            vec![
                GatewayMemberSpec::new(
                    OutboundId::parse("edge-a").expect("outbound"),
                    1,
                    NetworkSet::TCP_UDP,
                ),
                GatewayMemberSpec::new(
                    OutboundId::parse("edge-b").expect("outbound"),
                    1,
                    NetworkSet::TCP_UDP,
                ),
            ],
        ),
    )
    .expect("gateway balancer");
    let mut entropy = || 0;

    // Warm lazy regex automata before measuring.
    black_box(table.classify(input));
    black_box(acl.evaluate(input));
    black_box(
        balancer
            .select(GatewayInstant::ZERO, Network::Tcp, None, &[], &mut entropy)
            .expect("gateway selection"),
    );

    begin_measurement();
    for _ in 0..10_000 {
        black_box(table.classify(input));
        black_box(acl.evaluate(input));
        black_box(
            balancer
                .select(GatewayInstant::ZERO, Network::Tcp, None, &[], &mut entropy)
                .expect("gateway selection"),
        );
    }

    assert_eq!(finish_measurement(), 0);
}

#[test]
fn warmed_destination_and_principal_stickiness_hits_allocate_nothing() {
    let target = ProtocolTarget::from_host_port("daily.example", 443).expect("target");
    let principal = PrincipalId::parse("daily-user").expect("principal");
    for key in [
        GatewayStickinessKey::Destination,
        GatewayStickinessKey::Principal,
    ] {
        let mut spec = GatewayBalancerSpec::new(
            GatewayStrategy::RoundRobin,
            vec![
                GatewayMemberSpec::new(
                    OutboundId::parse("edge-a").expect("outbound"),
                    1,
                    NetworkSet::TCP_UDP,
                ),
                GatewayMemberSpec::new(
                    OutboundId::parse("edge-b").expect("outbound"),
                    1,
                    NetworkSet::TCP_UDP,
                ),
            ],
        );
        spec.stickiness = Some(GatewayStickinessPolicy {
            ttl: std::time::Duration::from_secs(60),
            capacity: 128,
        });
        spec.stickiness_key = key;
        let mut balancer = GatewayBalancer::compile(1, spec).expect("gateway balancer");
        let mut entropy = || 0;

        black_box(
            balancer
                .select_with_principal(
                    GatewayInstant::ZERO,
                    Network::Tcp,
                    Some(&target),
                    Some(&principal),
                    &[],
                    &mut entropy,
                )
                .expect("warm sticky entry"),
        );
        begin_measurement();
        for now in 1..=10_000 {
            black_box(
                balancer
                    .select_with_principal(
                        GatewayInstant::from_millis(now),
                        Network::Tcp,
                        Some(&target),
                        Some(&principal),
                        &[],
                        &mut entropy,
                    )
                    .expect("sticky hit"),
            );
        }
        assert_eq!(
            finish_measurement(),
            0,
            "{key:?} sticky hits must remain allocation-free"
        );
    }
}
