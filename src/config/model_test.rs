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

#[test]
fn management_dashboard_requires_an_http_listener() {
    let config = ManagementConfig {
        dashboard: true,
        ..ManagementConfig::default()
    };

    assert_eq!(
        config.validate(),
        Err(ConfigError::ManagementDashboardWithoutListener)
    );
}

#[test]
fn loopback_management_listener_requires_a_token() {
    let config = ManagementConfig {
        listen: vec!["127.0.0.1:7600".parse().expect("listen")],
        ..ManagementConfig::default()
    };

    assert_eq!(
        config.validate(),
        Err(ConfigError::ManagementListenerRequiresToken)
    );
}

#[test]
fn management_listener_rejects_an_empty_token() {
    let config = ManagementConfig {
        listen: vec!["127.0.0.1:7600".parse().expect("listen")],
        token: Some(String::new()),
        ..ManagementConfig::default()
    };

    assert_eq!(config.validate(), Err(ConfigError::ManagementTokenEmpty));
}

#[test]
fn management_listener_rejects_weak_or_header_unsafe_tokens() {
    for token in ["short", "sixteen bytes bad ", "sixteen\nbytesbad"] {
        let config = ManagementConfig {
            listen: vec!["127.0.0.1:9090".parse().expect("address")],
            token: Some(token.to_string()),
            ..ManagementConfig::default()
        };
        assert_eq!(config.validate(), Err(ConfigError::ManagementTokenInvalid));
    }
}

#[test]
fn non_loopback_management_listener_is_rejected_even_with_a_token() {
    let config = ManagementConfig {
        listen: vec!["0.0.0.0:7600".parse().expect("listen")],
        token: Some("operator-token-123".to_string()),
        ..ManagementConfig::default()
    };

    assert_eq!(
        config.validate(),
        Err(ConfigError::ManagementListenerMustBeLoopback)
    );
}

#[test]
fn loopback_management_listener_with_a_token_is_valid() {
    let config = ManagementConfig {
        listen: vec!["[::1]:7600".parse().expect("listen")],
        token: Some("operator-token-123".to_string()),
        ..ManagementConfig::default()
    };

    assert_eq!(config.validate(), Ok(()));
}

#[test]
fn peer_diagnostics_does_not_require_a_local_http_listener() {
    let config = ManagementConfig {
        allow_peer_diagnostics: true,
        ..ManagementConfig::default()
    };

    assert_eq!(config.validate(), Ok(()));
    assert!(!config.http_enabled());
    assert!(config.peer_diagnostics_enabled());
}
