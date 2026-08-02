use super::*;
use std::net::{Ipv4Addr, Ipv6Addr};

#[test]
fn domain_is_idna_lowercase_and_root_normalized() {
    let domain = DomainName::parse("BÜCHER.Example.").expect("valid IDN");
    assert_eq!(domain.as_str(), "xn--bcher-kva.example");
}

#[test]
fn domain_rejects_injection_and_ambiguous_numeric_text() {
    for input in [
        "",
        ".",
        "example..com",
        "example.com..",
        "example.com\r\nhost: attacker",
        "user@example.com",
        "example.com:443",
        "127.0.0.1",
        "127.1",
        "0177.0.0.1",
        "0x7f000001",
        "0x7f.0.0.1",
        "[::1]",
        " example.com",
    ] {
        assert!(DomainName::parse(input).is_err(), "{input:?}");
    }
}

#[test]
fn domain_enforces_dns_length_and_label_rules() {
    let label = "a".repeat(64);
    assert_eq!(
        DomainName::parse(&format!("{label}.example")),
        Err(FlowError::InvalidDomain)
    );
    let too_long = format!(
        "{}.{}.{}.{}",
        "a".repeat(63),
        "b".repeat(63),
        "c".repeat(63),
        "d".repeat(62)
    );
    assert!(DomainName::parse(&too_long).is_err());
}

#[test]
fn authority_parses_domain_ipv4_and_bracketed_ipv6() {
    let domain = ProtocolTarget::parse_authority("EXAMPLE.com:443").expect("domain");
    assert_eq!(domain.authority(), "example.com:443");
    let ipv4 = ProtocolTarget::parse_authority("192.0.2.1:53").expect("IPv4");
    assert_eq!(ipv4.ip(), Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1))));
    let ipv6 = ProtocolTarget::parse_authority("[2001:db8::1]:8443").expect("IPv6");
    assert_eq!(
        ipv6.ip(),
        Some(IpAddr::V6("2001:db8::1".parse().expect("address")))
    );
    assert_eq!(ipv6.authority(), "[2001:db8::1]:8443");
}

#[test]
fn authority_rejects_ambiguous_or_injectable_forms() {
    for input in [
        "example.com",
        "https://example.com:443",
        "user@example.com:443",
        "example.com:0",
        "example.com:65536",
        "example.com:+80",
        "::1:443",
        "[::1]443",
        "[::1%25eth0]:443",
        "example.com:443/path",
        "example.com:443\r\nx: y",
    ] {
        assert!(ProtocolTarget::parse_authority(input).is_err(), "{input:?}");
    }
}

#[test]
fn mapped_ipv6_is_canonicalized_to_ipv4() {
    let mapped = Ipv6Addr::from_str("::ffff:192.0.2.42").expect("mapped");
    let target = ProtocolTarget::from_ip(IpAddr::V6(mapped), 443).expect("target");
    assert_eq!(target.ip(), Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 42))));
}

#[test]
fn compatible_ipv6_cannot_hide_an_ipv4_destination() {
    let compatible = Ipv6Addr::from_str("::127.0.0.1").expect("compatible");
    let target = ProtocolTarget::from_ip(IpAddr::V6(compatible), 443).expect("target");
    assert_eq!(target.ip(), Some(IpAddr::V4(Ipv4Addr::LOCALHOST)));

    let loopback =
        ProtocolTarget::from_ip(IpAddr::V6(Ipv6Addr::LOCALHOST), 443).expect("IPv6 loopback");
    assert_eq!(loopback.ip(), Some(IpAddr::V6(Ipv6Addr::LOCALHOST)));
}

#[test]
fn policy_ids_are_normalized_and_bounded() {
    assert_eq!(
        PrincipalId::parse("Alice-Mobile")
            .expect("principal")
            .as_str(),
        "alice-mobile"
    );
    for input in ["", ".bad", "bad name", "bad/name", "bad\r\nname"] {
        assert!(PrincipalId::parse(input).is_err(), "{input:?}");
    }
}
