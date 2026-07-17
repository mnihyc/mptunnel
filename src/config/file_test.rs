use super::*;
use crate::config::CommandConfig;

fn ingress_configs(ingresses: &[LocalIngressConfig]) -> Vec<IngressConfig> {
    ingresses
        .iter()
        .map(|ingress| ingress.config.clone())
        .collect()
}

#[test]
fn resource_file_config_derives_path_flight_from_reinjection_envelope() {
    let limits = ResourceFileConfig {
        max_repair_bytes: Some(128 * 1024 * 1024),
        ..ResourceFileConfig::default()
    }
    .into_limits();

    assert_eq!(limits.max_repair_bytes, 128 * 1024 * 1024);
    assert_eq!(limits.max_path_flight_bytes, limits.max_repair_bytes);
}

#[test]
fn resource_file_config_derives_payload_and_chunk_from_frame_envelope() {
    let limits = ResourceFileConfig {
        max_frame_bytes: Some(4096),
        ..ResourceFileConfig::default()
    }
    .into_limits();

    assert_eq!(limits.max_payload_bytes, 4080);
    assert_eq!(limits.max_reliable_relay_chunk_bytes, 4080);
}

#[test]
fn toml_separates_logical_session_retention_from_carrier_liveness() {
    let config = load_config_toml_str(
        r#"
[session]
retention_timeout_ms = 45000

[resources]
tcp_path_heartbeat_interval_ms = 2000
tcp_path_heartbeat_timeout_ms = 7000
quic_path_keep_alive_interval_ms = 3000
quic_path_idle_timeout_ms = 12000

[[inbounds]]
protocol = "socks5"

[[outbounds]]
protocol = "mpp"
endpoints = ["tcp://127.0.0.1:443", "udp://127.0.0.1:443"]

[outbounds.security]
secret = "0123456789abcdef0123456789abcdef"
"#,
    )
    .expect("config");

    assert_eq!(config.session.retention_timeout, Duration::from_secs(45));
    assert_eq!(
        config.resources.tcp_path_heartbeat_timeout,
        Duration::from_secs(7)
    );
    assert_eq!(
        config.resources.quic_path_keep_alive_interval,
        Duration::from_secs(3)
    );
    assert_eq!(
        config.resources.quic_path_idle_timeout,
        Duration::from_secs(12)
    );
}

#[test]
fn repository_root_config_is_one_valid_client_profile_after_secret_replacement() {
    let contents =
        include_str!("../../config.toml").replace("REPLACE_ME", "0123456789abcdef0123456789abcdef");
    let config = load_config_toml_str(&contents).expect("root client config");

    assert_eq!(config.session, SessionConfig::default());
    assert_eq!(config.resources, ResourceLimits::default());
    let CommandConfig::Node(node) = config.command else {
        panic!("root config must build a node graph");
    };
    assert_eq!(node.clients.len(), 1);
    assert!(node.servers.is_empty());
    assert_eq!(node.clients[0].ingresses.len(), 2);
    assert_eq!(node.clients[0].paths.len(), 2);
}

#[test]
fn node_config_toml_uses_inbound_to_mpp_outbound_defaults_and_management() {
    let config = load_config_toml_str(
        r#"
[management]
listen = ["127.0.0.1:7600"]
token = "operator-token-123"
dashboard = true
allow_peer_diagnostics = true

[[inbounds]]
protocol = "socks5"

[[outbounds]]
tag = "mpp-main"
protocol = "mpp"
endpoints = ["tcp://127.0.0.1:443", "udp://127.0.0.1:443"]

[outbounds.performance]
extra_traffic_hint_percent = 25

[outbounds.security]
secret = "0123456789abcdef0123456789abcdef"
"#,
    )
    .expect("config");

    assert_eq!(config.management.listen.len(), 1);
    assert!(config.management.dashboard);
    assert!(config.management.allow_peer_diagnostics);
    match config.command {
        CommandConfig::Node(node) => {
            assert!(node.servers.is_empty());
            assert_eq!(node.clients.len(), 1);
            let client = &node.clients[0];
            assert_eq!(
                client
                    .route_target
                    .as_ref()
                    .map(|target| (&target.kind, target.tag.as_str())),
                Some((&RouteTargetKind::Outbound, "mpp-main"))
            );
            assert_eq!(client.paths.len(), 2);
            assert_eq!(client.performance.extra_traffic_hint_percent, 25);
            assert_eq!(client.ingresses[0].tag, None);
            assert_eq!(
                ingress_configs(&client.ingresses),
                vec![IngressConfig::Socks5 {
                    listen: vec!["127.0.0.1:1080".parse().expect("listen")],
                    proxy_auth: ProxyAuthConfig::disabled(),
                }]
            );
        }
        CommandConfig::Client(_) | CommandConfig::Server(_) => panic!("expected node"),
    }
}

#[test]
fn node_config_toml_preserves_local_inbound_tags() {
    let config = load_config_toml_str(
        r#"
[[inbounds]]
tag = "local-socks"
protocol = "socks5"
listen = ["127.0.0.1:1080"]
outbound = "mpp-main"

[[inbounds]]
tag = "local-http"
protocol = "http"
listen = ["127.0.0.1:8080"]
outbound = "mpp-main"

[[outbounds]]
tag = "mpp-main"
protocol = "mpp"
endpoints = ["tcp://127.0.0.1:443"]

[outbounds.security]
secret = "0123456789abcdef0123456789abcdef"
"#,
    )
    .expect("config");

    let CommandConfig::Node(node) = config.command else {
        panic!("expected node");
    };
    assert_eq!(node.clients.len(), 1);
    let ingresses = &node.clients[0].ingresses;
    assert_eq!(ingresses.len(), 2);
    assert_eq!(ingresses[0].tag.as_deref(), Some("local-socks"));
    assert_eq!(ingresses[1].tag.as_deref(), Some("local-http"));
    assert!(matches!(&ingresses[0].config, IngressConfig::Socks5 { .. }));
    assert!(matches!(
        &ingresses[1].config,
        IngressConfig::HttpConnect { .. }
    ));
}

#[test]
fn inbound_tags_must_be_unique() {
    let err = load_config_toml_str(
        r#"
[[inbounds]]
tag = "duplicate"
protocol = "socks5"

[[inbounds]]
tag = "duplicate"
protocol = "http"

[[outbounds]]
tag = "mpp-main"
protocol = "mpp"
endpoints = ["tcp://127.0.0.1:443"]

[outbounds.security]
secret = "0123456789abcdef0123456789abcdef"
"#,
    )
    .expect_err("duplicate inbound tag should fail");

    assert!(matches!(
        err,
        ConfigFileError::DuplicateInboundTag(tag) if tag == "duplicate"
    ));
}

#[test]
fn node_config_toml_covers_forwarding_chaining_and_outbound_dns() {
    let config = load_config_toml_str(
            r#"
[[inbounds]]
tag = "local-http"
protocol = "http"
listen = ["127.0.0.1:8081"]
outbound = "mpp-main"

[[inbounds]]
tag = "edge-mpp"
protocol = "mpp"
endpoints = ["tcp://0.0.0.0:8443", "udp://0.0.0.0:8443"]
outbound = "proxy-egress"

[inbounds.security]
secret = "0123456789abcdef0123456789abcdef"

[inbounds.performance]
extra_traffic_hint_percent = 200

[[outbounds]]
tag = "mpp-main"
protocol = "mpp"
endpoints = ["tcp://127.0.0.1:443"]

[outbounds.security]
secret = "fedcba9876543210fedcba9876543210"

[[outbounds]]
tag = "proxy-egress"
protocol = "http-connect-udp"
proxy = "127.0.0.1:8080"
dns = { resolvers = ["1.1.1.1:53", "[2606:4700:4700::1111]:53"], strategy = "ipv4-and-ipv6", timeout_ms = 1500 }
"#,
        )
        .expect("config");

    match config.command {
        CommandConfig::Node(node) => {
            assert_eq!(node.clients.len(), 1);
            assert_eq!(node.servers.len(), 1);
            let server = &node.servers[0];
            assert_eq!(server.tag.as_deref(), Some("edge-mpp"));
            assert_eq!(server.performance.extra_traffic_hint_percent, 200);
            assert_eq!(
                server
                    .route_target
                    .as_ref()
                    .map(|target| (&target.kind, target.tag.as_str())),
                Some((&RouteTargetKind::Outbound, "proxy-egress"))
            );
            assert_eq!(server.bind_paths.len(), 2);
            assert_eq!(server.outbound_dns.resolvers.len(), 2);
            assert_eq!(server.outbound_dns.strategy, DnsIpStrategy::Ipv4AndIpv6);
            assert_eq!(server.outbound_dns.timeout, Duration::from_millis(1500));
            assert!(matches!(
                server.outbound,
                OutboundConfig::HttpConnectUdp { .. }
            ));
        }
        CommandConfig::Client(_) | CommandConfig::Server(_) => panic!("expected node"),
    }
}

#[test]
fn routing_can_build_combined_mpp_and_sequence_egress() {
    let config = load_config_toml_str(
        r#"
[[inbounds]]
tag = "local-socks"
protocol = "socks5"
balancer = "combined-edge"

[[inbounds]]
tag = "edge"
protocol = "mpp"
endpoints = ["tcp://0.0.0.0:8443"]
balancer = "egress-sequence"

[inbounds.security]
secret = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

[[outbounds]]
tag = "mpp-a"
protocol = "mpp"
endpoints = ["tcp://127.0.0.1:443"]

[outbounds.security]
secret = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

[[outbounds]]
tag = "mpp-b"
protocol = "mpp"
endpoints = ["udp://127.0.0.1:443"]

[outbounds.security]
secret = "cccccccccccccccccccccccccccccccc"

[[outbounds]]
tag = "direct-a"
protocol = "direct"

[[outbounds]]
tag = "direct-b"
protocol = "direct"
bind_ip = "127.0.0.1"

[routing]

[[routing.balancers]]
tag = "combined-edge"
strategy = "combined-mpp"
outbounds = ["mpp-a", "mpp-b"]

[[routing.balancers]]
tag = "egress-sequence"
strategy = "sequence"
outbounds = ["direct-a", "direct-b"]
"#,
    )
    .expect("config");

    let CommandConfig::Node(node) = config.command else {
        panic!("expected node");
    };
    assert_eq!(node.clients.len(), 1);
    assert_eq!(
        node.clients[0]
            .route_target
            .as_ref()
            .map(|target| (&target.kind, target.tag.as_str())),
        Some((&RouteTargetKind::Balancer, "combined-edge"))
    );
    assert_eq!(node.clients[0].paths.len(), 2);
    assert_ne!(
        node.clients[0].paths[0].security.secret.as_bytes(),
        node.clients[0].paths[1].security.secret.as_bytes()
    );
    assert_eq!(node.servers.len(), 1);
    assert_eq!(
        node.servers[0]
            .route_target
            .as_ref()
            .map(|target| (&target.kind, target.tag.as_str())),
        Some((&RouteTargetKind::Balancer, "egress-sequence"))
    );
    assert!(matches!(
        node.servers[0].outbound,
        OutboundConfig::Sequence { .. }
    ));
}

#[test]
fn inbound_outbound_field_cannot_reference_balancer() {
    let err = load_config_toml_str(
        r#"
[[inbounds]]
protocol = "socks5"
outbound = "combined-edge"

[[outbounds]]
tag = "mpp-a"
protocol = "mpp"
endpoints = ["tcp://127.0.0.1:443"]

[outbounds.security]
secret = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

[[outbounds]]
tag = "mpp-b"
protocol = "mpp"
endpoints = ["udp://127.0.0.1:443"]

[outbounds.security]
secret = "cccccccccccccccccccccccccccccccc"

[routing]

[[routing.balancers]]
tag = "combined-edge"
strategy = "combined-mpp"
outbounds = ["mpp-a", "mpp-b"]
"#,
    )
    .expect_err("balancer should require balancer field");

    assert!(matches!(
        err,
        ConfigFileError::OutboundFieldReferencesBalancer(tag) if tag == "combined-edge"
    ));
}

#[test]
fn routing_members_must_be_outbounds_not_balancers() {
    let err = load_config_toml_str(
        r#"
[[inbounds]]
protocol = "mpp"
endpoints = ["tcp://0.0.0.0:8443"]
balancer = "outer"

[inbounds.security]
secret = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

[[outbounds]]
tag = "direct-a"
protocol = "direct"

[[outbounds]]
tag = "direct-b"
protocol = "direct"

[routing]

[[routing.balancers]]
tag = "inner"
strategy = "sequence"
outbounds = ["direct-a"]

[[routing.balancers]]
tag = "outer"
strategy = "sequence"
outbounds = ["inner", "direct-b"]
"#,
    )
    .expect_err("nested routing balancer members are invalid");

    assert!(matches!(
        err,
        ConfigFileError::RoutingBalancerMemberWrongProtocol {
            balancer,
            member,
            expected: "egress",
        } if balancer == "outer" && member == "inner"
    ));
}
