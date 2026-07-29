use super::*;

#[test]
fn path_specs_parse_tcp_and_udp() {
    let tcp =
        "tcp://example.com:443?source-ip=192.0.2.10&srtt-ms=20&rate-mbps=30&bulk-allowed=false"
            .parse::<PathSpec>()
            .expect("tcp");
    let udp = "udp://[2001:db8::1]:8443?jitter-ms=5&rate-bps=100000000&datagram-payload-limit=1400"
        .parse::<PathSpec>()
        .expect("udp");

    assert_eq!(tcp.underlay, UnderlayProtocol::Tcp);
    assert_eq!(tcp.endpoint.host, "example.com");
    assert_eq!(
        tcp.endpoint.ports(),
        CarrierPortSet::single(443).expect("port")
    );
    assert_eq!(
        tcp.binding.source_ip,
        Some("192.0.2.10".parse().expect("source IP"))
    );
    assert_eq!(tcp.metadata.initial_srtt_ms, Some(20));
    assert_eq!(
        tcp.metadata.initial_rate,
        RateHint::BitsPerSecond(30_000_000)
    );
    assert!(!tcp.metadata.policy.bulk_allowed);
    assert_eq!(udp.underlay, UnderlayProtocol::Udp);
    assert_eq!(udp.endpoint.host, "2001:db8::1");
    assert_eq!(
        udp.endpoint.ports(),
        CarrierPortSet::single(8443).expect("port")
    );
    assert_eq!(udp.binding, PathBinding::default());
    assert_eq!(udp.metadata.initial_jitter_ms, Some(5));
    assert_eq!(
        udp.metadata.initial_rate,
        RateHint::BitsPerSecond(100_000_000)
    );
    assert_eq!(udp.metadata.max_datagram_payload_bytes, Some(1400));

    let ranged_tcp = "tcp://example.com:111-222"
        .parse::<PathSpec>()
        .expect("ranged TCP carrier");
    let ranged_udp = "udp://[2001:db8::2]:5000-5010"
        .parse::<PathSpec>()
        .expect("ranged UDP carrier");
    assert_eq!(ranged_tcp.endpoint.ports().first(), 111);
    assert_eq!(ranged_tcp.endpoint.ports().last(), 222);
    assert!(
        ranged_tcp
            .endpoint
            .ports()
            .contains(ranged_tcp.endpoint.ports().select().expect("OS entropy"))
    );
    assert_eq!(
        CarrierPortSet::single(443)
            .expect("fixed port")
            .select()
            .expect("fixed selection"),
        443
    );
    assert_eq!(ranged_tcp.endpoint.authority(), "example.com:111-222");
    assert_eq!(ranged_udp.endpoint.ports().first(), 5000);
    assert_eq!(ranged_udp.endpoint.ports().last(), 5010);
    assert_eq!(ranged_udp.endpoint.authority(), "[2001:db8::2]:5000-5010");
}

#[test]
fn path_specs_reject_ambiguous_values() {
    assert!("example.com:443".parse::<PathSpec>().is_err());
    assert!("tcp://example.com".parse::<PathSpec>().is_err());
    assert!("udp://example.com:0".parse::<PathSpec>().is_err());
    for invalid in [
        "tcp://example.com:0-10",
        "tcp://example.com:222-111",
        "tcp://example.com:443-443",
        "tcp://example.com:443-",
        "tcp://example.com:-444",
        "tcp://example.com:443-444-445",
        "udp://2001:db8::1:443-444",
    ] {
        assert!(
            invalid.parse::<PathSpec>().is_err(),
            "ambiguous carrier endpoint must be rejected: {invalid}"
        );
    }
    assert!("example.com:443-444".parse::<Endpoint>().is_err());
    assert!("tcp://example.com:443?".parse::<PathSpec>().is_err());
    assert!(
        "tcp://example.com:443?unknown=true"
            .parse::<PathSpec>()
            .is_err()
    );
    assert!(
        "tcp://example.com:443?source-ip=not-an-ip"
            .parse::<PathSpec>()
            .is_err()
    );
    assert!(
        "tcp://example.com:443?source-ip=192.0.2.1&source-ip=192.0.2.2"
            .parse::<PathSpec>()
            .is_err()
    );
    assert!(
        "udp://example.com:443?no-udp=true"
            .parse::<PathSpec>()
            .is_err()
    );
    assert!(
        "udp://example.com:443?datagram-payload-limit=100"
            .parse::<PathSpec>()
            .is_err()
    );
    for legacy in [
        "rtt-ms=20",
        "mtu=1400",
        "mtu-bytes=1400",
        "payload-mtu=1400",
        "bulk=true",
        "no-bulk=true",
        "backup=1",
        "backup=yes",
        "backup=on",
    ] {
        assert!(
            format!("tcp://example.com:443?{legacy}")
                .parse::<PathSpec>()
                .is_err(),
            "legacy carrier option must be rejected: {legacy}"
        );
    }
    assert!(
        "tcp://example.com:443?unsupported=true"
            .parse::<PathSpec>()
            .is_err()
    );
    assert!(
        "udp://example.com:443?unsupported=true"
            .parse::<PathSpec>()
            .is_err()
    );
    assert!(
        "udp://example.com:443?profile=experimental"
            .parse::<PathSpec>()
            .is_err()
    );
}
