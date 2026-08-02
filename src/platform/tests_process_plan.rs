use super::*;
use crate::platform::{DnsCaptureConfig, ManagedVpnConfig};

fn route(family: AddressFamily, index: u32, gateway: Option<&str>) -> ProcessNativeRoute {
    ProcessNativeRoute::new(
        family,
        index,
        gateway.map(|value| value.parse().expect("gateway")),
        10,
    )
    .expect("native route")
}

fn dual_environment() -> ProcessVpnEnvironment {
    ProcessVpnEnvironment::new(
        [
            route(AddressFamily::Ipv4, 7, Some("192.0.2.1")),
            route(AddressFamily::Ipv6, 8, Some("2001:db8::1")),
        ],
        vec![
            ProcessNativeNetwork::new(
                "192.0.2.0/24".parse().expect("network"),
                route(AddressFamily::Ipv4, 7, None),
                true,
            )
            .expect("local"),
            ProcessNativeNetwork::new(
                "2001:db8::/64".parse().expect("network"),
                route(AddressFamily::Ipv6, 8, None),
                true,
            )
            .expect("local"),
        ],
    )
    .expect("environment")
}

fn dual_full() -> ManagedVpnConfig {
    ManagedVpnConfig::new(
        vec![
            "10.88.0.1/24".parse().expect("IPv4"),
            "fd88::1/64".parse().expect("IPv6"),
        ],
        1400,
        RouteMode::Full,
    )
    .expect("config")
}

#[test]
fn full_plan_prepares_bypasses_and_publishes_capture_then_dns() {
    let config = dual_full()
        .with_dns(
            DnsCaptureConfig::new(vec![
                "10.88.0.53".parse().expect("IPv4 DNS"),
                "fd88::53".parse().expect("IPv6 DNS"),
            ])
            .expect("DNS"),
        )
        .expect("config DNS");
    let plan = ProcessVpnPlan::build(
        &config,
        &dual_environment(),
        42,
        [
            "198.51.100.10".parse().expect("carrier"),
            "2001:db8:1::10".parse().expect("carrier"),
        ],
        ["9.9.9.9".parse().expect("bootstrap")],
    )
    .expect("plan");

    assert_eq!(plan.prepare_operations().len(), 3);
    assert!(
        plan.prepare_operations()
            .iter()
            .all(|operation| matches!(operation, ProcessHostOperation::AddBypassRoute { .. }))
    );
    assert!(matches!(
        plan.publish_operations().last(),
        Some(ProcessHostOperation::ConfigureDns { .. })
    ));
    let captures = plan
        .publish_operations()
        .iter()
        .filter(|operation| matches!(operation, ProcessHostOperation::AddCaptureRoute { .. }))
        .count();
    assert_eq!(captures, 2, "DNS host routes collapse into full defaults");
}

#[test]
fn split_plan_merges_bypass_reasons_and_keeps_local_routes_native() {
    let config = ManagedVpnConfig::new(
        vec!["10.88.0.1/24".parse().expect("address")],
        1400,
        RouteMode::Split(vec!["203.0.113.0/24".parse().expect("include")]),
    )
    .expect("config")
    .with_excludes(vec!["192.0.2.0/24".parse().expect("exclude")])
    .expect("exclude")
    .with_local_lan(true);
    let plan = ProcessVpnPlan::build(
        &config,
        &dual_environment(),
        42,
        ["192.0.2.7".parse().expect("carrier")],
        [],
    )
    .expect("plan");

    let local = plan
        .prepare_operations()
        .iter()
        .find_map(|operation| match operation {
            ProcessHostOperation::AddBypassRoute {
                destination,
                reasons,
                ..
            } if *destination == "192.0.2.0/24".parse::<IpNet>().expect("network") => {
                Some(*reasons)
            }
            _ => None,
        })
        .expect("local bypass");
    assert!(local.contains(BypassReason::ExplicitExclude));
    assert!(local.contains(BypassReason::LocalLan));
}

#[test]
fn more_specific_native_route_wins_for_endpoint_bypass() {
    let environment = ProcessVpnEnvironment::new(
        [route(AddressFamily::Ipv4, 7, Some("192.0.2.1"))],
        vec![
            ProcessNativeNetwork::new(
                "198.51.100.0/24".parse().expect("network"),
                route(AddressFamily::Ipv4, 9, Some("203.0.113.1")),
                false,
            )
            .expect("network"),
        ],
    )
    .expect("environment");
    let config = ManagedVpnConfig::new(
        vec!["10.88.0.1/24".parse().expect("address")],
        1400,
        RouteMode::Full,
    )
    .expect("config");
    let plan = ProcessVpnPlan::build(
        &config,
        &environment,
        42,
        ["198.51.100.7".parse().expect("carrier")],
        [],
    )
    .expect("plan");

    assert!(matches!(
        &plan.prepare_operations()[0],
        ProcessHostOperation::AddBypassRoute { native, .. }
            if native.interface_index().get() == 9
    ));
}

#[test]
fn planner_rejects_native_loop_back_into_tunnel() {
    let environment =
        ProcessVpnEnvironment::new([route(AddressFamily::Ipv4, 42, Some("192.0.2.1"))], vec![])
            .expect("environment");
    let config = ManagedVpnConfig::new(
        vec!["10.88.0.1/24".parse().expect("address")],
        1400,
        RouteMode::Full,
    )
    .expect("config");
    assert!(matches!(
        ProcessVpnPlan::build(
            &config,
            &environment,
            42,
            ["198.51.100.7".parse().expect("carrier")],
            [],
        ),
        Err(ProcessVpnPlanError::NativeRouteUsesTunnel { .. })
    ));
}

#[test]
fn planner_rejects_bypassed_dns() {
    let config = dual_full()
        .with_dns(DnsCaptureConfig::new(vec!["9.9.9.9".parse().expect("DNS")]).expect("DNS"))
        .expect("config DNS");
    assert!(matches!(
        ProcessVpnPlan::build(
            &config,
            &dual_environment(),
            42,
            [],
            ["9.9.9.9".parse().expect("bootstrap")],
        ),
        Err(ProcessVpnPlanError::DnsServerBypassed(_))
    ));
}

#[test]
fn environment_rejects_equal_prefix_ecmp_ambiguity() {
    let first = ProcessNativeNetwork::new(
        "198.51.100.0/24".parse().expect("network"),
        route(AddressFamily::Ipv4, 7, Some("192.0.2.1")),
        false,
    )
    .expect("route");
    let second = ProcessNativeNetwork::new(
        "198.51.100.0/24".parse().expect("network"),
        route(AddressFamily::Ipv4, 8, Some("203.0.113.1")),
        false,
    )
    .expect("route");
    assert!(matches!(
        ProcessVpnEnvironment::new([], vec![first, second]),
        Err(ProcessNativeRouteError::ConflictingNativeNetwork(_))
    ));
}

#[test]
fn plan_is_deterministic_under_input_permutation_and_duplicates() {
    let config = dual_full();
    let left = ProcessVpnPlan::build(
        &config,
        &dual_environment(),
        42,
        [
            "198.51.100.2".parse().expect("endpoint"),
            "198.51.100.1".parse().expect("endpoint"),
            "198.51.100.1".parse().expect("endpoint"),
        ],
        [],
    )
    .expect("left");
    let right = ProcessVpnPlan::build(
        &config,
        &dual_environment(),
        42,
        [
            "198.51.100.1".parse().expect("endpoint"),
            "198.51.100.2".parse().expect("endpoint"),
        ],
        [],
    )
    .expect("right");
    assert_eq!(left, right);
}
