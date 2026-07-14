use super::*;

#[test]
fn path_specs_parse_tcp_and_udp() {
    let tcp = "tcp://example.com:443?source-ip=192.0.2.10&srtt-ms=20&rate-mbps=30&low-latency=true"
        .parse::<PathSpec>()
        .expect("tcp");
    let udp = "udp://[2001:db8::1]:8443?jitter-ms=5&rate-bps=100000000&mtu=1400"
        .parse::<PathSpec>()
        .expect("udp");

    assert_eq!(tcp.underlay, UnderlayProtocol::Tcp);
    assert_eq!(tcp.endpoint.host, "example.com");
    assert_eq!(tcp.endpoint.port, 443);
    assert_eq!(
        tcp.binding.source_ip,
        Some("192.0.2.10".parse().expect("source IP"))
    );
    assert_eq!(tcp.metadata.initial_srtt_ms, Some(20));
    assert_eq!(
        tcp.metadata.initial_rate,
        RateHint::BitsPerSecond(30_000_000)
    );
    assert!(tcp.metadata.capabilities.low_latency);
    assert_eq!(udp.underlay, UnderlayProtocol::Udp);
    assert_eq!(udp.endpoint.host, "2001:db8::1");
    assert_eq!(udp.endpoint.port, 8443);
    assert_eq!(udp.binding, PathBinding::default());
    assert_eq!(udp.metadata.initial_jitter_ms, Some(5));
    assert_eq!(
        udp.metadata.initial_rate,
        RateHint::BitsPerSecond(100_000_000)
    );
    assert_eq!(udp.metadata.initial_mtu_payload_bytes, Some(1400));
}

#[test]
fn path_specs_reject_ambiguous_values() {
    assert!("example.com:443".parse::<PathSpec>().is_err());
    assert!("tcp://example.com".parse::<PathSpec>().is_err());
    assert!("udp://example.com:0".parse::<PathSpec>().is_err());
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
    assert!("udp://example.com:443?mtu=100".parse::<PathSpec>().is_err());
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
