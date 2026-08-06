use super::*;
use crate::product::{CredentialCatalog, CredentialId, CredentialRecord, SharedSecret};

fn authority(principals: &[&str]) -> CredentialAuthority {
    let records = principals.iter().enumerate().map(|(index, principal)| {
        CredentialRecord::new(
            CredentialId::parse(&format!("credential-{index}")).expect("credential ID"),
            PrincipalId::parse(principal).expect("principal ID"),
            SharedSecret::new(vec![index as u8 + 1; 32]).expect("secret"),
            None,
            false,
            0,
        )
        .expect("credential")
    });
    let catalog = CredentialCatalog::compile(records).expect("catalog");
    let ids = (0..principals.len())
        .map(|index| CredentialId::parse(&format!("credential-{index}")).expect("credential ID"))
        .collect::<Vec<_>>();
    catalog.authority(&ids).expect("authority")
}

fn spec() -> TunL3ServerSpec {
    TunL3ServerSpec {
        interface_name: Some("mptun-server".to_string()),
        ipv4_pool: Some("10.88.0.0/24".parse().expect("IPv4 pool")),
        ipv4: Some("10.88.0.1".parse().expect("server IPv4")),
        ipv6_pool: Some("fd88::/64".parse().expect("IPv6 pool")),
        ipv6: Some("fd88::1".parse().expect("server IPv6")),
        mtu: 1500,
        allocations: vec![TunL3AllocationSpec {
            principal_id: PrincipalId::parse("phone").expect("principal"),
            ipv4: Some("10.88.0.2".parse().expect("peer IPv4")),
            ipv6: Some("fd88::2".parse().expect("peer IPv6")),
            allowed_ips: vec!["192.168.50.0/24".parse().expect("allowed prefix")],
        }],
    }
}

#[test]
fn static_pool_plan_resolves_exact_and_routed_ownership() {
    let plan = TunL3AddressPlan::compile(spec(), &authority(&["phone"])).expect("plan");
    let phone = PrincipalId::parse("phone").expect("principal");
    assert_eq!(
        plan.owner("10.88.0.2".parse().expect("address")),
        Some(&phone)
    );
    assert_eq!(
        plan.owner("fd88::2".parse().expect("address")),
        Some(&phone)
    );
    assert_eq!(
        plan.owner("192.168.50.99".parse().expect("address")),
        Some(&phone)
    );
    assert_eq!(plan.owner("10.88.0.3".parse().expect("address")), None);
}

#[test]
fn one_principal_may_own_nested_site_prefixes() {
    let mut address_spec = spec();
    address_spec.allocations[0].allowed_ips = vec![
        "192.168.0.0/16".parse().expect("site prefix"),
        "192.168.50.0/24".parse().expect("nested site prefix"),
    ];
    let plan = TunL3AddressPlan::compile(address_spec, &authority(&["phone"]))
        .expect("same-principal ownership is unambiguous");
    let phone = PrincipalId::parse("phone").expect("principal");
    assert_eq!(
        plan.owner("192.168.50.99".parse().expect("address")),
        Some(&phone)
    );

    let mut conflict = spec();
    conflict.allocations.push(TunL3AllocationSpec {
        principal_id: PrincipalId::parse("tablet").expect("principal"),
        ipv4: Some("10.88.0.3".parse().expect("peer IPv4")),
        ipv6: None,
        allowed_ips: vec!["192.168.50.0/25".parse().expect("conflicting prefix")],
    });
    assert!(matches!(
        TunL3AddressPlan::compile(conflict, &authority(&["phone", "tablet"])),
        Err(TunL3PlanError::OverlappingOwnership { .. })
    ));
}

#[test]
fn plan_rejects_unknown_principal_and_address_collisions() {
    let mut unknown = spec();
    unknown.allocations[0].principal_id = PrincipalId::parse("unknown").expect("principal");
    assert!(matches!(
        TunL3AddressPlan::compile(unknown, &authority(&["phone"])),
        Err(TunL3PlanError::UnknownPrincipal(_))
    ));

    let mut collision = spec();
    collision.allocations.push(TunL3AllocationSpec {
        principal_id: PrincipalId::parse("tablet").expect("principal"),
        ipv4: Some("10.88.0.2".parse().expect("peer IPv4")),
        ipv6: None,
        allowed_ips: Vec::new(),
    });
    assert!(matches!(
        TunL3AddressPlan::compile(collision, &authority(&["phone", "tablet"])),
        Err(TunL3PlanError::AddressOwnedTwice { .. })
    ));
}

#[test]
fn plan_rejects_out_of_pool_and_server_addresses() {
    let mut outside = spec();
    outside.allocations[0].ipv4 = Some("10.89.0.2".parse().expect("peer IPv4"));
    assert!(matches!(
        TunL3AddressPlan::compile(outside, &authority(&["phone"])),
        Err(TunL3PlanError::AllocationOutsidePool { .. })
    ));

    let mut server = spec();
    server.allocations[0].ipv4 = Some("10.88.0.1".parse().expect("server IPv4"));
    assert!(matches!(
        TunL3AddressPlan::compile(server, &authority(&["phone"])),
        Err(TunL3PlanError::AllocationUsesServerAddress { .. })
    ));
}

#[test]
fn plan_rejects_non_host_addresses_but_accepts_point_to_point_ipv4() {
    for address in ["10.88.0.0", "10.88.0.255"] {
        let mut invalid = spec();
        invalid.allocations[0].ipv4 = Some(address.parse().expect("invalid peer IPv4"));
        assert!(matches!(
            TunL3AddressPlan::compile(invalid, &authority(&["phone"])),
            Err(TunL3PlanError::UnusableHostAddress { .. })
        ));
    }

    let mut multicast = spec();
    multicast.ipv4_pool = Some("224.0.0.0/24".parse().expect("multicast pool"));
    multicast.ipv4 = Some("224.0.0.1".parse().expect("server IPv4"));
    multicast.allocations[0].ipv4 = Some("224.0.0.2".parse().expect("peer IPv4"));
    assert!(matches!(
        TunL3AddressPlan::compile(multicast, &authority(&["phone"])),
        Err(TunL3PlanError::UnusableHostAddress { .. })
    ));

    let mut unspecified_v6 = spec();
    unspecified_v6.ipv6_pool = Some("::/0".parse().expect("IPv6 pool"));
    unspecified_v6.ipv6 = Some("2001:db8::1".parse().expect("server IPv6"));
    unspecified_v6.allocations[0].ipv6 = Some("::".parse().expect("peer IPv6"));
    assert!(matches!(
        TunL3AddressPlan::compile(unspecified_v6, &authority(&["phone"])),
        Err(TunL3PlanError::UnusableHostAddress { .. })
    ));

    let mut point_to_point = spec();
    point_to_point.ipv4_pool = Some("10.88.0.0/31".parse().expect("IPv4 pool"));
    point_to_point.ipv4 = Some("10.88.0.0".parse().expect("server IPv4"));
    point_to_point.allocations[0].ipv4 = Some("10.88.0.1".parse().expect("peer IPv4"));
    TunL3AddressPlan::compile(point_to_point, &authority(&["phone"]))
        .expect("RFC 3021 point-to-point addresses are both usable");
}

#[test]
fn plan_enforces_ip_mtu_and_protects_server_ownership() {
    let mut ipv4_small = spec();
    ipv4_small.ipv6_pool = None;
    ipv4_small.ipv6 = None;
    ipv4_small.allocations[0].ipv6 = None;
    ipv4_small.mtu = 575;
    assert!(matches!(
        TunL3AddressPlan::compile(ipv4_small, &authority(&["phone"])),
        Err(TunL3PlanError::MtuTooSmall { .. })
    ));

    let mut ipv6_small = spec();
    ipv6_small.mtu = 1279;
    assert!(matches!(
        TunL3AddressPlan::compile(ipv6_small, &authority(&["phone"])),
        Err(TunL3PlanError::Ipv6MtuTooSmall { .. })
    ));

    let mut server_prefix = spec();
    server_prefix.allocations[0].allowed_ips =
        vec!["10.88.0.0/24".parse().expect("server-containing prefix")];
    assert!(matches!(
        TunL3AddressPlan::compile(server_prefix, &authority(&["phone"])),
        Err(TunL3PlanError::OwnershipContainsServerAddress { .. })
    ));
}
