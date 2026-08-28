use super::*;

#[test]
fn canonical_path_specs_parse_tcp_and_quic() {
    let tcp = concat!(
        "tcp://example.com:5000-5010?",
        "source-address=192.0.2.10&",
        "initial-srtt-s=0.02&",
        "initial-rttvar-s=0.005&",
        "initial-rate-mbps=30&",
        "max-tcp-carriers=5&",
        "port-rotation-interval-s=45&",
        "backup=true&",
        "expensive=false&",
        "allow-bulk=false&",
        "control-only=true&",
        "allow-datagrams=false"
    )
    .parse::<PathSpec>()
    .expect("canonical TCP path");

    assert_eq!(tcp.underlay, UnderlayProtocol::Tcp);
    assert_eq!(tcp.endpoint.host, "example.com");
    assert_eq!(tcp.endpoint.ports().first(), 5000);
    assert_eq!(tcp.endpoint.ports().last(), 5010);
    assert_eq!(
        tcp.binding.source_ip,
        Some("192.0.2.10".parse().expect("source address"))
    );
    assert_eq!(tcp.metadata.initial_srtt_ms, Some(20));
    assert_eq!(tcp.metadata.initial_jitter_ms, Some(5));
    assert_eq!(
        tcp.metadata.initial_rate,
        RateHint::BitsPerSecond(30_000_000)
    );
    assert_eq!(
        tcp.tcp_carrier_range(),
        Some(TcpCarrierRange::new(5).expect("carrier limit"))
    );
    assert_eq!(
        tcp.port_hop_interval(),
        Some(std::time::Duration::from_millis(45_000))
    );
    assert!(tcp.metadata.policy.backup);
    assert!(!tcp.metadata.policy.expensive);
    assert!(!tcp.metadata.policy.bulk_allowed);
    assert!(tcp.metadata.policy.probe_only);
    assert!(tcp.metadata.policy.no_udp);

    let quic = concat!(
        "quic://[2001:db8::1]:8443?",
        "initial-rttvar-s=0&",
        "initial-rate-bps=100000000&",
        "loss-compensation-percent=5.1250&",
        "max-datagram-payload-bytes=1400"
    )
    .parse::<PathSpec>()
    .expect("canonical QUIC path");
    assert_eq!(quic.underlay, UnderlayProtocol::Udp);
    assert_eq!(quic.endpoint.host, "2001:db8::1");
    assert_eq!(quic.endpoint.ports().first(), 8443);
    assert_eq!(quic.metadata.initial_jitter_ms, Some(0));
    assert_eq!(
        quic.metadata.initial_rate,
        RateHint::BitsPerSecond(100_000_000)
    );
    assert_eq!(quic.metadata.max_datagram_payload_bytes, Some(1400));
    assert_eq!(
        quic.metadata
            .loss_compensation
            .expect("configured loss compensation")
            .ppm(),
        51_250
    );
}

#[test]
fn carrier_endpoints_parse_canonical_fixed_and_ranged_ports() {
    let ranged_tcp = "tcp://example.com:111-222"
        .parse::<PathSpec>()
        .expect("ranged TCP carrier");
    let ranged_quic = "quic://[2001:db8::2]:5000-5010"
        .parse::<PathSpec>()
        .expect("ranged QUIC carrier");

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
    for current in [111, 166, 222] {
        let selected = ranged_tcp
            .endpoint
            .ports()
            .select_other(current)
            .expect("OS entropy");
        assert!(ranged_tcp.endpoint.ports().contains(selected));
        assert_ne!(selected, current);
    }
    assert_eq!(ranged_tcp.endpoint.authority(), "example.com:111-222");
    assert_eq!(ranged_quic.endpoint.authority(), "[2001:db8::2]:5000-5010");
    assert_eq!(
        ranged_quic.port_hop_interval(),
        Some(std::time::Duration::from_millis(
            DEFAULT_CARRIER_PORT_HOP_INTERVAL_MS.into()
        ))
    );
    assert_eq!(
        ranged_tcp.tcp_carrier_range(),
        Some(TcpCarrierRange::new(DEFAULT_TCP_CARRIER_MAX).expect("default limit"))
    );
}

#[test]
fn canonical_initial_rate_forms_are_exact_and_mutually_exclusive() {
    for (query, expected) in [
        ("initial-rate-bps=7", RateHint::BitsPerSecond(7)),
        ("initial-rate-kbps=7", RateHint::BitsPerSecond(7_000)),
        ("initial-rate-mbps=7", RateHint::BitsPerSecond(7_000_000)),
        ("initial-rate=unknown", RateHint::Unknown),
        ("initial-rate=unlimited", RateHint::Unlimited),
    ] {
        let path = format!("tcp://example.com:443?{query}")
            .parse::<PathSpec>()
            .expect("canonical rate form");
        assert_eq!(path.metadata.initial_rate, expected, "{query}");
    }

    for invalid in [
        "initial-rate-bps=0",
        "initial-rate-kbps=0",
        "initial-rate-mbps=0",
        "initial-rate=7",
        "initial-rate-mbps=18446744073710",
        "initial-rate-bps=1&initial-rate=unlimited",
        "initial-rate-kbps=1&initial-rate-mbps=1",
    ] {
        assert!(
            format!("tcp://example.com:443?{invalid}")
                .parse::<PathSpec>()
                .is_err(),
            "invalid rate must be rejected: {invalid}"
        );
    }
}

#[test]
fn loss_compensation_percent_parses_exactly_to_ppm() {
    for (value, expected_ppm) in [
        ("0", 0),
        ("0.0", 0),
        ("0.0000", 0),
        ("0.0001", 1),
        ("1", 10_000),
        ("1.2", 12_000),
        ("1.23", 12_300),
        ("1.234", 12_340),
        ("1.2345", 12_345),
        ("5.1200", 51_200),
        ("99.9999", 999_999),
    ] {
        let path = format!("quic://example.com:443?loss-compensation-percent={value}")
            .parse::<PathSpec>()
            .expect("exact loss-compensation percentage");
        assert_eq!(
            path.metadata
                .loss_compensation
                .expect("configured loss estimate")
                .ppm(),
            expected_ppm,
            "{value}"
        );
    }
    assert_eq!(PathMetadata::default().loss_compensation, None);
}

#[test]
fn loss_compensation_percent_rejects_non_decimal_or_out_of_range_values() {
    const KEY: &str = "loss-compensation-percent";
    for value in [
        "", "0.00010", "100", "100.0000", "-1", "+1", ".1", "1.", "1.2.3", "1e0", "NaN", "inf",
    ] {
        assert_eq!(
            format!("quic://example.com:443?{KEY}={value}")
                .parse::<PathSpec>()
                .expect_err("invalid loss compensation must be rejected"),
            PathSpecParseError::InvalidQueryParamValue(KEY.to_string(), value.to_string()),
            "{value}"
        );
    }
    assert_eq!(
        format!("quic://example.com:443?{KEY}")
            .parse::<PathSpec>()
            .expect_err("missing loss value must be rejected"),
        PathSpecParseError::MissingQueryParamValue(KEY.to_string())
    );
}

#[test]
fn loss_compensation_percent_is_quic_only_and_not_an_initial_rate_alias() {
    assert_eq!(
        "tcp://example.com:443?loss-compensation-percent=5"
            .parse::<PathSpec>()
            .expect_err("TCP must reject the QUIC loss input"),
        PathSpecParseError::LossCompensationRequiresQuicPath
    );

    let path = concat!(
        "quic://example.com:443?",
        "initial-rate-mbps=25&loss-compensation-percent=5"
    )
    .parse::<PathSpec>()
    .expect("independent QUIC startup inputs");
    assert_eq!(
        path.metadata.initial_rate,
        RateHint::BitsPerSecond(25_000_000)
    );
    assert_eq!(
        path.metadata
            .loss_compensation
            .expect("explicit loss estimate")
            .ppm(),
        50_000
    );
}

#[test]
fn path_specs_reject_noncanonical_schemes_hosts_and_ports() {
    for invalid in [
        "example.com:443",
        "udp://example.com:443",
        "UDP://example.com:443",
        "tcp://example.com",
        "quic://example.com:0",
        "tcp://example.com:0443",
        "tcp://example.com:+443",
        "tcp://example.com:0-10",
        "tcp://example.com:222-111",
        "tcp://example.com:443-443",
        "tcp://example.com:443-",
        "tcp://example.com:-444",
        "tcp://example.com:443-0444",
        "tcp://example.com:443-444-445",
        "quic://2001:db8::1:443-444",
        "quic://[example.com]:443",
        "quic://[2001:db8::1:443",
        "quic://2001:db8::1]:443",
        " tcp://example.com:443",
        "tcp://example.com:443 ",
    ] {
        assert!(
            invalid.parse::<PathSpec>().is_err(),
            "noncanonical carrier endpoint must be rejected: {invalid}"
        );
    }
    assert!("example.com:443-444".parse::<Endpoint>().is_err());
}

#[test]
fn path_options_reject_missing_invalid_duplicate_and_inapplicable_values() {
    for invalid in [
        "tcp://example.com:443?",
        "tcp://example.com:443?&backup=true",
        "tcp://example.com:443?unknown=true",
        "tcp://example.com:443?source-address=not-an-ip",
        "tcp://example.com:443?initial-srtt-s=0",
        "tcp://example.com:443?initial-srtt-s=0.0005",
        "tcp://example.com:443?max-tcp-carriers=0",
        "tcp://example.com:443?max-tcp-carriers=65536",
        "tcp://example.com:443?max-tcp-carriers=1-3",
        "tcp://example.com:443?max-datagram-payload-bytes=1400",
        "quic://example.com:443?max-tcp-carriers=3",
        "quic://example.com:443?allow-datagrams=true",
        "quic://example.com:443?allow-datagrams=false",
        "quic://example.com:443?max-datagram-payload-bytes=511",
        "quic://example.com:443?max-datagram-payload-bytes=65001",
        "tcp://example.com:443?loss-compensation-percent=5",
        "tcp://example.com:443?port-rotation-interval-s=300",
        "quic://example.com:443?port-rotation-interval-s=300",
        "quic://example.com:443-444?port-rotation-interval-s=4.999",
    ] {
        assert!(
            invalid.parse::<PathSpec>().is_err(),
            "invalid or ineffective option must be rejected: {invalid}"
        );
    }

    for boolean in [
        "backup",
        "expensive",
        "allow-bulk",
        "control-only",
        "allow-datagrams",
    ] {
        assert!(
            format!("tcp://example.com:443?{boolean}")
                .parse::<PathSpec>()
                .is_err(),
            "Boolean values must be explicit: {boolean}"
        );
        assert!(
            format!("tcp://example.com:443?{boolean}=yes")
                .parse::<PathSpec>()
                .is_err(),
            "Boolean values must be true or false: {boolean}"
        );
        assert!(
            format!("tcp://example.com:443?{boolean}=true&{boolean}=false")
                .parse::<PathSpec>()
                .is_err(),
            "duplicate Boolean option must be rejected: {boolean}"
        );
    }

    for duplicate in [
        "source-address=192.0.2.1&source-address=192.0.2.2",
        "initial-srtt-s=0.001&initial-srtt-s=0.002",
        "initial-rttvar-s=0.001&initial-rttvar-s=0.002",
        "max-datagram-payload-bytes=1200&max-datagram-payload-bytes=1300",
        "loss-compensation-percent=1&loss-compensation-percent=2",
        "max-tcp-carriers=1&max-tcp-carriers=2",
        "port-rotation-interval-s=5&port-rotation-interval-s=6",
    ] {
        let scheme = if duplicate.starts_with("max-datagram")
            || duplicate.starts_with("loss-compensation")
        {
            "quic"
        } else {
            "tcp"
        };
        let endpoint = if duplicate.starts_with("port-rotation") {
            "example.com:443-444"
        } else {
            "example.com:443"
        };
        assert!(
            format!("{scheme}://{endpoint}?{duplicate}")
                .parse::<PathSpec>()
                .is_err(),
            "duplicate option must be rejected: {duplicate}"
        );
    }
}

#[test]
fn legacy_path_grammar_is_rejected() {
    for legacy in [
        "source-ip=192.0.2.1",
        "initial-srtt-ms=20",
        "initial-rttvar-ms=5",
        "port-rotation-interval-ms=5000",
        "srtt-ms=20",
        "jitter-ms=5",
        "rate-bps=1000",
        "rate-kbps=1000",
        "rate-mbps=100",
        "rate=unknown",
        "datagram-payload-limit=1400",
        "tcp-carriers=1-3",
        "port-hop-interval-ms=5000",
        "bulk-allowed=false",
        "probe-only=true",
        "no-udp=true",
        "rtt-ms=20",
        "mtu=1400",
        "mtu-bytes=1400",
        "payload-mtu=1400",
        "bulk=true",
        "no-bulk=true",
    ] {
        assert!(
            format!("tcp://example.com:443?{legacy}")
                .parse::<PathSpec>()
                .is_err(),
            "legacy carrier option must be rejected: {legacy}"
        );
    }
}
