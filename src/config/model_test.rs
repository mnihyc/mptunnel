use super::*;

#[test]
fn extra_traffic_hint_default_is_five_percent() {
    assert_eq!(
        MppPerformanceConfig::default().extra_traffic_hint_percent,
        5
    );
}

#[test]
fn udp_paths_reject_unknown_query_parameters() {
    let default_path = "udp://127.0.0.1:443"
        .parse::<PathSpec>()
        .expect("default udp path parses");

    assert_eq!(
        default_path.underlay,
        crate::protocol::UnderlayProtocol::Udp
    );
    assert!(
        "udp://127.0.0.1:443?unsupported=true"
            .parse::<PathSpec>()
            .is_err()
    );
    assert!(
        "udp://127.0.0.1:443?profile=experimental"
            .parse::<PathSpec>()
            .is_err()
    );
}

#[test]
fn server_paths_reject_client_source_binding() {
    let security = SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("test shared secret"),
    );
    let server = ServerConfig {
        tag: None,
        route_target: None,
        bind_paths: vec![
            "tcp://127.0.0.1:443?source-ip=127.0.0.1"
                .parse()
                .expect("server path"),
        ],
        security,
        outbound: OutboundConfig::Direct,
        outbound_dns: DnsConfig::default(),
        outbound_connect_timeout: DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        performance: MppPerformanceConfig::default(),
    };

    assert_eq!(
        validate_server_config(&server, ResourceLimits::default()),
        Err(ConfigError::ServerPathSourceBinding)
    );
}
