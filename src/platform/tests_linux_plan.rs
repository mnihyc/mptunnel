use super::*;
use crate::platform::config::DnsCaptureConfig;

fn interface(name: &str) -> LinuxInterfaceName {
    LinuxInterfaceName::parse(name).expect("interface")
}

fn route(family: AddressFamily, interface_name: &str, gateway: &str) -> LinuxNativeRoute {
    LinuxNativeRoute::new(
        family,
        interface(interface_name),
        Some(gateway.parse().expect("gateway")),
        None,
        100,
    )
    .expect("native route")
}

fn dual_environment() -> LinuxVpnEnvironment {
    LinuxVpnEnvironment::new(
        vec![
            route(AddressFamily::Ipv4, "eth0", "192.0.2.1"),
            route(AddressFamily::Ipv6, "eth0", "2001:db8::1"),
        ],
        vec![
            LinuxNativeNetwork::new(
                "192.168.0.0/16".parse().expect("LAN"),
                LinuxNativeRoute::new(
                    AddressFamily::Ipv4,
                    interface("eth0"),
                    None,
                    Some("192.168.1.2".parse().expect("source")),
                    50,
                )
                .expect("LAN route"),
            )
            .expect("LAN"),
            LinuxNativeNetwork::new(
                "fd12:3456::/48".parse().expect("LAN"),
                LinuxNativeRoute::new(
                    AddressFamily::Ipv6,
                    interface("eth0"),
                    None,
                    Some("fd12:3456::2".parse().expect("source")),
                    50,
                )
                .expect("LAN route"),
            )
            .expect("LAN"),
        ],
    )
    .expect("environment")
}

fn dual_full_config() -> LinuxVpnConfig {
    LinuxVpnConfig::new(
        interface("mptun0"),
        vec![
            "10.88.0.1/24".parse().expect("address"),
            "fd00:88::1/64".parse().expect("address"),
        ],
        1500,
        RouteMode::Full,
    )
    .expect("config")
}

#[test]
fn full_dual_stack_plan_has_exact_safe_activation_order() {
    let config = dual_full_config()
        .with_excludes(vec!["198.51.100.0/24".parse().expect("exclude")])
        .expect("exclude")
        .with_local_lan(true)
        .with_dns(
            DnsCaptureConfig::new(vec![
                "1.1.1.1".parse().expect("DNS"),
                "2606:4700:4700::1111".parse().expect("DNS"),
            ])
            .expect("DNS"),
        )
        .expect("DNS");
    let plan = LinuxVpnPlan::build(
        &config,
        &dual_environment(),
        [
            "203.0.113.9".parse().expect("carrier"),
            "2001:db8:ffff::9".parse().expect("carrier"),
        ],
        ["9.9.9.9".parse().expect("bootstrap")],
    )
    .expect("plan");

    let prepare = plan.prepare_operations();
    let publish = plan.publish_operations();
    assert!(matches!(
        prepare[0],
        LinuxHostOperation::CheckResolvedSupport
    ));
    assert!(matches!(prepare[1], LinuxHostOperation::CreateTun { .. }));
    assert!(matches!(prepare[2], LinuxHostOperation::AddAddress { .. }));
    assert!(matches!(prepare[3], LinuxHostOperation::AddAddress { .. }));
    assert!(matches!(prepare[4], LinuxHostOperation::SetLinkUp { .. }));

    let last_bypass = prepare
        .iter()
        .rposition(|operation| matches!(operation, LinuxHostOperation::AddBypassRoute { .. }))
        .expect("bypass");
    let first_capture = prepare
        .iter()
        .position(|operation| matches!(operation, LinuxHostOperation::AddCaptureRoute(_)))
        .expect("capture");
    assert!(last_bypass < first_capture);
    assert!(prepare.iter().all(|operation| !matches!(
        operation,
        LinuxHostOperation::ActivateNativeEgressRule { .. }
            | LinuxHostOperation::ActivateCaptureRule { .. }
            | LinuxHostOperation::ConfigureDns { .. }
    )));
    assert!(publish.iter().take(2).all(|operation| matches!(
        operation,
        LinuxHostOperation::ActivateNativeEgressRule {
            mark: crate::platform::config::DEFAULT_LINUX_SOCKET_MARK,
            priority: crate::platform::config::DEFAULT_LINUX_NATIVE_RULE_PRIORITY,
            ..
        }
    )));
    assert!(publish.iter().skip(2).take(2).all(|operation| matches!(
        operation,
        LinuxHostOperation::ActivateCaptureRule {
            priority: crate::platform::config::DEFAULT_LINUX_CAPTURE_RULE_PRIORITY,
            ..
        }
    )));
    assert!(matches!(
        publish.last(),
        Some(LinuxHostOperation::ConfigureDns {
            route_all: true,
            ..
        })
    ));

    let bypass_reasons = prepare
        .iter()
        .filter_map(|operation| match operation {
            LinuxHostOperation::AddBypassRoute { reasons, .. } => Some(*reasons),
            _ => None,
        })
        .collect::<Vec<_>>();
    let ranks = bypass_reasons
        .iter()
        .map(|reasons| reasons.order())
        .collect::<Vec<_>>();
    assert!(ranks.windows(2).all(|pair| pair[0] <= pair[1]));
    for reason in [
        BypassReason::CarrierEndpoint,
        BypassReason::BootstrapDns,
        BypassReason::ExplicitExclude,
        BypassReason::LocalLan,
    ] {
        assert!(
            bypass_reasons
                .iter()
                .any(|reasons| reasons.contains(reason))
        );
    }
}

#[test]
fn custom_linux_mark_policy_is_ordered_before_capture_without_removing_bypasses() {
    let mark = crate::platform::config::LinuxSocketMark::new(0x1234).expect("mark");
    let policy = crate::platform::config::LinuxPolicyConfig::new(40_000, 7_000, 8_000, mark)
        .expect("policy");
    let config = dual_full_config().with_linux_policy(policy);
    let plan = LinuxVpnPlan::build(
        &config,
        &dual_environment(),
        ["203.0.113.9".parse().expect("carrier")],
        ["9.9.9.9".parse().expect("bootstrap")],
    )
    .expect("plan");

    assert!(matches!(
        plan.publish_operations(),
        [
            LinuxHostOperation::ActivateNativeEgressRule {
                family: AddressFamily::Ipv4,
                mark: actual_v4,
                priority: 7_000,
            },
            LinuxHostOperation::ActivateNativeEgressRule {
                family: AddressFamily::Ipv6,
                mark: actual_v6,
                priority: 7_000,
            },
            LinuxHostOperation::ActivateCaptureRule {
                family: AddressFamily::Ipv4,
                table: 40_000,
                priority: 8_000,
            },
            LinuxHostOperation::ActivateCaptureRule {
                family: AddressFamily::Ipv6,
                table: 40_000,
                priority: 8_000,
            },
        ] if *actual_v4 == mark && *actual_v6 == mark
    ));
    let bypass_reasons = plan
        .prepare_operations()
        .iter()
        .filter_map(|operation| match operation {
            LinuxHostOperation::AddBypassRoute { reasons, .. } => Some(*reasons),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        bypass_reasons
            .iter()
            .any(|reasons| reasons.contains(BypassReason::CarrierEndpoint))
    );
    assert!(
        bypass_reasons
            .iter()
            .any(|reasons| reasons.contains(BypassReason::BootstrapDns))
    );
}

#[test]
fn explicit_exclude_wins_over_more_specific_split_include() {
    let config = LinuxVpnConfig::new(
        interface("mptun0"),
        vec!["10.88.0.1/24".parse().expect("address")],
        1500,
        RouteMode::Split(vec![
            "10.10.0.0/16".parse().expect("include"),
            "172.16.0.0/12".parse().expect("include"),
        ]),
    )
    .expect("config")
    .with_excludes(vec!["10.0.0.0/8".parse().expect("exclude")])
    .expect("exclude");
    let plan = LinuxVpnPlan::build(
        &config,
        &dual_environment(),
        ["203.0.113.9".parse().expect("carrier")],
        [],
    )
    .expect("plan");
    let captures = plan
        .prepare_operations()
        .iter()
        .filter_map(|operation| match operation {
            LinuxHostOperation::AddCaptureRoute(route) => Some(route.destination),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        captures,
        vec!["172.16.0.0/12".parse().expect("remaining capture")]
    );
}

#[test]
fn duplicate_bypass_addresses_merge_reasons_without_reordering() {
    let config = dual_full_config();
    let address: IpAddr = "203.0.113.9".parse().expect("address");
    let plan = LinuxVpnPlan::build(&config, &dual_environment(), [address, address], [address])
        .expect("plan");
    let matching = plan
        .prepare_operations()
        .iter()
        .filter_map(|operation| match operation {
            LinuxHostOperation::AddBypassRoute {
                destination,
                reasons,
                ..
            } if destination.contains(&address) => Some(*reasons),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(matching.len(), 1);
    assert!(matching[0].contains(BypassReason::CarrierEndpoint));
    assert!(matching[0].contains(BypassReason::BootstrapDns));
}

#[test]
fn dns_gets_a_capture_route_in_split_mode_and_cannot_be_bypassed() {
    let config = LinuxVpnConfig::new(
        interface("mptun0"),
        vec!["10.88.0.1/24".parse().expect("address")],
        1500,
        RouteMode::Split(vec!["10.0.0.0/8".parse().expect("include")]),
    )
    .expect("config")
    .with_dns(DnsCaptureConfig::new(vec!["1.1.1.1".parse().expect("DNS")]).expect("DNS"))
    .expect("DNS");
    let plan = LinuxVpnPlan::build(
        &config,
        &dual_environment(),
        ["203.0.113.9".parse().expect("carrier")],
        [],
    )
    .expect("plan");
    assert!(plan.prepare_operations().iter().any(|operation| matches!(
        operation,
        LinuxHostOperation::AddCaptureRoute(LinuxCaptureRoute { destination, .. })
            if *destination == "1.1.1.1/32".parse::<IpNet>().expect("DNS route")
    )));

    let conflict = LinuxVpnPlan::build(
        &config,
        &dual_environment(),
        ["203.0.113.9".parse().expect("carrier")],
        ["1.1.1.1".parse().expect("bootstrap")],
    );
    assert_eq!(
        conflict,
        Err(LinuxVpnPlanError::DnsServerBypassed(
            "1.1.1.1".parse().expect("DNS")
        ))
    );
}

#[test]
fn plan_is_deterministic_across_input_order_and_duplicates() {
    let config = dual_full_config().with_local_lan(true);
    let environment = dual_environment();
    let first = LinuxVpnPlan::build(
        &config,
        &environment,
        [
            "203.0.113.10".parse().expect("carrier"),
            "203.0.113.9".parse().expect("carrier"),
        ],
        [
            "9.9.9.9".parse().expect("bootstrap"),
            "8.8.8.8".parse().expect("bootstrap"),
        ],
    )
    .expect("first plan");
    let second = LinuxVpnPlan::build(
        &config,
        &environment,
        [
            "203.0.113.9".parse().expect("carrier"),
            "203.0.113.10".parse().expect("carrier"),
            "203.0.113.9".parse().expect("carrier"),
        ],
        [
            "8.8.8.8".parse().expect("bootstrap"),
            "9.9.9.9".parse().expect("bootstrap"),
        ],
    )
    .expect("second plan");
    assert_eq!(first, second);
}

#[test]
fn direct_only_full_vpn_uses_native_mark_without_static_bypasses() {
    let config = dual_full_config();
    let plan =
        LinuxVpnPlan::build(&config, &dual_environment(), [], []).expect("direct-only full VPN");
    assert!(
        !plan
            .prepare_operations()
            .iter()
            .any(|operation| matches!(operation, LinuxHostOperation::AddBypassRoute { .. }))
    );
    for family in [AddressFamily::Ipv4, AddressFamily::Ipv6] {
        let native = plan
            .publish_operations()
            .iter()
            .position(|operation| {
                matches!(
                    operation,
                    LinuxHostOperation::ActivateNativeEgressRule {
                        family: candidate,
                        ..
                    } if *candidate == family
                )
            })
            .expect("native-main mark rule");
        let capture = plan
            .publish_operations()
            .iter()
            .position(|operation| {
                matches!(
                    operation,
                    LinuxHostOperation::ActivateCaptureRule {
                        family: candidate,
                        ..
                    } if *candidate == family
                )
            })
            .expect("capture rule");
        assert!(native < capture);
    }
}

#[test]
fn planner_requires_native_routes_for_known_endpoints() {
    let config = dual_full_config();
    let v4_only_environment = LinuxVpnEnvironment::new(
        vec![route(AddressFamily::Ipv4, "eth0", "192.0.2.1")],
        vec![],
    )
    .expect("environment");
    assert!(matches!(
        LinuxVpnPlan::build(
            &config,
            &v4_only_environment,
            ["2001:db8:ffff::9".parse().expect("carrier")],
            [],
        ),
        Err(LinuxVpnPlanError::MissingNativeRoute { .. })
    ));
}

#[test]
fn environment_rejects_conflicting_or_ambiguous_native_state() {
    assert_eq!(
        LinuxVpnEnvironment::new(
            vec![
                route(AddressFamily::Ipv4, "eth0", "192.0.2.1"),
                route(AddressFamily::Ipv4, "eth1", "198.51.100.1"),
            ],
            vec![],
        ),
        Err(LinuxNativeRouteError::DuplicateDefaultRoute)
    );

    let first = LinuxNativeNetwork::new(
        "192.168.0.0/16".parse().expect("LAN"),
        LinuxNativeRoute::new(AddressFamily::Ipv4, interface("eth0"), None, None, 10)
            .expect("route"),
    )
    .expect("network");
    let second = LinuxNativeNetwork::new(
        "192.168.0.0/16".parse().expect("LAN"),
        LinuxNativeRoute::new(AddressFamily::Ipv4, interface("eth1"), None, None, 10)
            .expect("route"),
    )
    .expect("network");
    assert_eq!(
        LinuxVpnEnvironment::new(vec![], vec![first, second]),
        Err(LinuxNativeRouteError::ConflictingLocalNetwork(
            "192.168.0.0/16".parse().expect("LAN")
        ))
    );
}

#[test]
fn every_bypass_precedes_every_capture_for_large_plans() {
    let config = dual_full_config()
        .with_excludes(
            (0..64)
                .map(|index| format!("100.{index}.0.0/16").parse().expect("exclude"))
                .collect(),
        )
        .expect("excludes");
    let carriers = (1..=64)
        .map(|last| format!("203.0.113.{last}").parse().expect("carrier"))
        .collect::<Vec<_>>();
    let plan = LinuxVpnPlan::build(&config, &dual_environment(), carriers, []).expect("large plan");
    assert!(bypasses_precede_capture(plan.prepare_operations()));
}
