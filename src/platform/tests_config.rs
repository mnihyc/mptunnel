use super::*;

fn interface() -> LinuxInterfaceName {
    LinuxInterfaceName::parse("mptun0").expect("interface")
}

#[test]
fn interface_name_is_linux_strict() {
    assert!(LinuxInterfaceName::parse("mptun0").is_ok());
    assert!(LinuxInterfaceName::parse("mptun.1-a").is_ok());
    assert_eq!(
        LinuxInterfaceName::parse(""),
        Err(LinuxInterfaceNameError::Empty)
    );
    assert_eq!(
        LinuxInterfaceName::parse("-mptun"),
        Err(LinuxInterfaceNameError::InvalidFirstCharacter)
    );
    assert_eq!(
        LinuxInterfaceName::parse("mptun/0"),
        Err(LinuxInterfaceNameError::InvalidCharacter)
    );
    assert!(matches!(
        LinuxInterfaceName::parse("interface-name-too-long"),
        Err(LinuxInterfaceNameError::TooLong { .. })
    ));
}

#[test]
fn config_rejects_ambiguous_or_unsupported_families() {
    assert_eq!(
        LinuxVpnConfig::new(interface(), Vec::new(), 1500, RouteMode::Full),
        Err(ManagedVpnConfigError::AddressRequired)
    );
    assert_eq!(
        LinuxVpnConfig::new(
            interface(),
            vec![
                "10.88.0.1/24".parse().expect("address"),
                "10.89.0.1/24".parse().expect("address"),
            ],
            1500,
            RouteMode::Full,
        ),
        Err(ManagedVpnConfigError::DuplicateAddressFamily)
    );
    assert!(matches!(
        LinuxVpnConfig::new(
            interface(),
            vec!["10.88.0.1/24".parse().expect("address")],
            1500,
            RouteMode::Split(vec!["2001:db8::/32".parse().expect("include")]),
        ),
        Err(ManagedVpnConfigError::UnsupportedRouteFamily(_))
    ));
}

#[test]
fn ipv6_requires_protocol_minimum_mtu() {
    assert_eq!(
        LinuxVpnConfig::new(
            interface(),
            vec!["fd00:88::1/64".parse().expect("address")],
            1279,
            RouteMode::Full,
        ),
        Err(ManagedVpnConfigError::Ipv6MtuTooSmall(1279))
    );
}

#[test]
fn routes_and_dns_are_canonical_and_conflicts_are_rejected() {
    let config = LinuxVpnConfig::new(
        interface(),
        vec!["10.88.0.1/24".parse().expect("address")],
        1500,
        RouteMode::Split(vec![
            "10.1.2.3/8".parse().expect("include"),
            "10.0.0.0/8".parse().expect("include"),
        ]),
    )
    .expect("config")
    .with_excludes(vec![
        "192.168.1.10/24".parse().expect("exclude"),
        "192.168.1.0/24".parse().expect("exclude"),
    ])
    .expect("excludes");

    assert_eq!(
        config.route_mode(),
        &RouteMode::Split(vec!["10.0.0.0/8".parse().expect("canonical")])
    );
    assert_eq!(
        config.excludes(),
        &["192.168.1.0/24".parse().expect("canonical")]
    );

    let dns = DnsCaptureConfig::new(vec![
        "1.1.1.1".parse().expect("DNS"),
        "1.1.1.1".parse().expect("DNS"),
    ])
    .expect("DNS config");
    let excluded_dns = config.with_excludes(vec!["1.1.1.0/24".parse().expect("exclude")]);
    assert!(excluded_dns.expect("exclude config").with_dns(dns).is_err());
}

#[test]
fn common_config_is_independent_from_linux_identity_and_policy() {
    let managed = ManagedVpnConfig::new(
        vec!["10.88.0.1/24".parse().expect("address")],
        1500,
        RouteMode::Full,
    )
    .expect("managed config")
    .with_local_lan(true);
    let linux = LinuxVpnConfig::from_managed(interface(), managed.clone());

    assert_eq!(linux.managed(), &managed);
    assert_eq!(linux.interface().as_str(), "mptun0");
    assert_eq!(linux.linux_policy(), LinuxPolicyConfig::default());
    assert!(managed.local_lan());
}

#[test]
fn linux_policy_rejects_kernel_reserved_values() {
    assert_eq!(LinuxSocketMark::new(0), Err(LinuxSocketMarkError::Zero));
    for table in [0, 253, 254, 255] {
        assert_eq!(
            LinuxPolicyConfig::new(
                table,
                DEFAULT_LINUX_NATIVE_RULE_PRIORITY,
                DEFAULT_LINUX_CAPTURE_RULE_PRIORITY,
                DEFAULT_LINUX_SOCKET_MARK,
            ),
            Err(LinuxPolicyConfigError::ReservedRouteTable(table))
        );
    }
    for priority in [0, 32_766, u32::MAX] {
        assert_eq!(
            LinuxPolicyConfig::new(
                DEFAULT_LINUX_ROUTE_TABLE,
                priority,
                DEFAULT_LINUX_CAPTURE_RULE_PRIORITY,
                DEFAULT_LINUX_SOCKET_MARK,
            ),
            Err(LinuxPolicyConfigError::InvalidNativeRulePriority(priority))
        );
        assert_eq!(
            LinuxPolicyConfig::new(
                DEFAULT_LINUX_ROUTE_TABLE,
                DEFAULT_LINUX_NATIVE_RULE_PRIORITY,
                priority,
                DEFAULT_LINUX_SOCKET_MARK,
            ),
            Err(LinuxPolicyConfigError::InvalidCaptureRulePriority(priority))
        );
    }
    for (native, capture) in [(10_000, 10_000), (10_001, 10_000)] {
        assert_eq!(
            LinuxPolicyConfig::new(
                DEFAULT_LINUX_ROUTE_TABLE,
                native,
                capture,
                DEFAULT_LINUX_SOCKET_MARK,
            ),
            Err(LinuxPolicyConfigError::NativeRuleMustPrecedeCapture { native, capture })
        );
    }
}

#[test]
fn vpn_config_exposes_one_validated_linux_policy() {
    let mark = LinuxSocketMark::new(0x1234).expect("mark");
    let policy = LinuxPolicyConfig::new(40_000, 7_000, 8_000, mark).expect("policy");
    let config = LinuxVpnConfig::new(
        interface(),
        vec!["10.88.0.1/24".parse().expect("address")],
        1500,
        RouteMode::Full,
    )
    .expect("config")
    .with_linux_policy(policy);
    assert_eq!(config.linux_policy(), policy);
    assert_eq!(config.linux_policy().socket_mark().get(), 0x1234);
}
