use super::*;
use crate::config::{CipherSuite, CommandConfig};
use clap::Parser;

fn ingress_configs(ingresses: &[LocalIngressConfig]) -> Vec<IngressConfig> {
    ingresses
        .iter()
        .map(|ingress| ingress.config.clone())
        .collect()
}

#[test]
fn client_cli_builds_default_socks_config() {
    let cli = Cli::try_parse_from([
        "mptunnel",
        "--check-config",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "client",
        "--path",
        "tcp://127.0.0.1:443",
        "--path",
        "udp://127.0.0.1:443",
    ])
    .expect("parse cli");
    let config = cli.into_config().expect("config");

    assert_eq!(config.security.cipher, CipherSuite::Aes256Gcm);
    assert!(config.check_config);
    assert_eq!(config.service, ServiceConfig::default());
    assert_eq!(config.resources, ResourceLimits::default());
    match config.command {
        CommandConfig::Client(client) => {
            assert_eq!(client.paths.len(), 2);
            assert_eq!(
                ingress_configs(&client.ingresses),
                vec![IngressConfig::Socks5 {
                    listen: vec!["127.0.0.1:1080".parse().expect("listen")],
                    proxy_auth: ProxyAuthConfig::disabled(),
                }]
            );
            assert_eq!(
                client.path_probe_interval,
                crate::config::DEFAULT_PATH_PROBE_INTERVAL
            );
            assert_eq!(
                client.path_probe_timeout,
                crate::config::DEFAULT_PATH_PROBE_TIMEOUT
            );
        }
        CommandConfig::Server(_) | CommandConfig::Node(_) => panic!("expected client config"),
    }
}

#[test]
fn client_cli_enables_dashboard_and_peer_diagnostics_independently() {
    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "--management-listen",
        "127.0.0.1:7600",
        "--management-token",
        "operator-token-123",
        "--management-dashboard",
        "--management-allow-peer-diagnostics",
        "client",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse CLI");
    let config = cli.into_config().expect("config");

    assert_eq!(
        config.management.listen,
        vec!["127.0.0.1:7600".parse().expect("listen")]
    );
    assert!(config.management.dashboard);
    assert!(config.management.allow_peer_diagnostics);
}

#[test]
fn client_proxy_cli_accepts_multiple_dual_stack_listen_addresses() {
    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "client",
        "--http-listen",
        "127.0.0.1:8080",
        "--http-listen",
        "[::1]:8080",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");
    let config = cli.into_config().expect("config");

    let CommandConfig::Client(client) = config.command else {
        panic!("expected client config");
    };
    assert_eq!(
        ingress_configs(&client.ingresses),
        vec![IngressConfig::HttpConnect {
            listen: vec![
                "127.0.0.1:8080".parse().expect("ipv4 listen"),
                "[::1]:8080".parse().expect("ipv6 listen"),
            ],
            proxy_auth: ProxyAuthConfig::disabled(),
        }]
    );
}

#[test]
fn client_cli_accepts_multiple_proxy_ingresses() {
    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "client",
        "--socks5-listen",
        "127.0.0.1:1080",
        "--http-listen",
        "127.0.0.1:8080",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");
    let config = cli.into_config().expect("config");

    let CommandConfig::Client(client) = config.command else {
        panic!("expected client config");
    };
    assert_eq!(
        ingress_configs(&client.ingresses),
        vec![
            IngressConfig::Socks5 {
                listen: vec!["127.0.0.1:1080".parse().expect("socks listen")],
                proxy_auth: ProxyAuthConfig::disabled(),
            },
            IngressConfig::HttpConnect {
                listen: vec!["127.0.0.1:8080".parse().expect("http listen")],
                proxy_auth: ProxyAuthConfig::disabled(),
            },
        ]
    );
}

#[test]
fn client_cli_parses_optional_proxy_auth() {
    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "client",
        "--proxy-username",
        "operator",
        "--proxy-password",
        "secret",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");
    let config = cli.into_config().expect("config");

    let CommandConfig::Client(client) = config.command else {
        panic!("expected client config");
    };
    let [
        LocalIngressConfig {
            config: IngressConfig::Socks5 { proxy_auth, .. },
            ..
        },
    ] = client.ingresses.as_slice()
    else {
        panic!("expected default SOCKS5 ingress");
    };
    assert!(proxy_auth.is_required());
    assert!(proxy_auth.verify("operator", "secret"));
    assert!(!proxy_auth.verify("operator", "wrong"));

    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "client",
        "--proxy-username",
        "operator",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");
    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::ProxyPasswordRequired)
    ));
}

#[test]
fn client_cli_treats_listen_as_socks5_shorthand_with_http_ingress() {
    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "client",
        "--listen",
        "127.0.0.1:1080",
        "--http-listen",
        "127.0.0.1:8080",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");
    let config = cli.into_config().expect("config");

    let CommandConfig::Client(client) = config.command else {
        panic!("expected client config");
    };
    assert_eq!(
        ingress_configs(&client.ingresses),
        vec![
            IngressConfig::Socks5 {
                listen: vec!["127.0.0.1:1080".parse().expect("socks listen")],
                proxy_auth: ProxyAuthConfig::disabled(),
            },
            IngressConfig::HttpConnect {
                listen: vec!["127.0.0.1:8080".parse().expect("http listen")],
                proxy_auth: ProxyAuthConfig::disabled(),
            },
        ]
    );
}

#[test]
fn cipher_cli_can_select_chacha20_poly1305() {
    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "--cipher",
        "chacha20-poly1305",
        "client",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");
    let config = cli.into_config().expect("config");

    assert_eq!(config.security.cipher, CipherSuite::Chacha20Poly1305);
}

#[test]
fn service_supervisor_cli_is_parsed_and_validated() {
    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "--service-mode",
        "--supervise",
        "--restart-backoff-ms",
        "500",
        "--restart-max-backoff-ms",
        "5000",
        "--max-restarts",
        "3",
        "client",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");
    let config = cli.into_config().expect("config");

    assert!(config.service.service_mode);
    assert!(config.service.supervise);
    assert_eq!(config.service.restart_backoff, Duration::from_millis(500));
    assert_eq!(
        config.service.restart_max_backoff,
        Duration::from_millis(5000)
    );
    assert_eq!(config.service.max_restarts, Some(3));

    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "--restart-backoff-ms",
        "5000",
        "--restart-max-backoff-ms",
        "500",
        "client",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");
    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::Config(
            crate::config::ConfigError::RestartMaxBackoffTooSmall
        ))
    ));
}

#[test]
fn platform_command_does_not_require_runtime_secret() {
    let cli = Cli::try_parse_from(["mptunnel", "platform"]).expect("parse cli");
    assert!(matches!(cli.command, Command::Platform(_)));
}

#[test]
fn secret_is_required() {
    let cli = Cli::try_parse_from(["mptunnel", "client", "--path", "tcp://127.0.0.1:443"])
        .expect("parse cli");

    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::Security(
            crate::config::SecurityPolicyError::MissingSecret
        ))
    ));
}

#[test]
fn server_outbound_requires_matching_parameters() {
    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "server",
        "--bind-path",
        "tcp://0.0.0.0:443",
        "--outbound",
        "socks5",
    ])
    .expect("parse cli");

    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::MissingUpstreamSocks5)
    ));

    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "server",
        "--bind-path",
        "udp://0.0.0.0:443",
        "--outbound",
        "http-connect-udp",
    ])
    .expect("parse cli");

    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::MissingUpstreamHttp)
    ));
}

#[test]
fn server_http_connect_udp_outbound_is_parsed() {
    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "server",
        "--bind-path",
        "udp://0.0.0.0:443",
        "--outbound",
        "http-connect-udp",
        "--upstream-http",
        "127.0.0.1:8080",
    ])
    .expect("parse cli");
    let config = cli.into_config().expect("config");

    let CommandConfig::Server(server) = config.command else {
        panic!("expected server config");
    };
    assert_eq!(
        server.outbound,
        OutboundConfig::HttpConnectUdp {
            proxy: "127.0.0.1:8080".parse().expect("proxy")
        }
    );
}

#[test]
fn server_outbound_dns_is_parsed() {
    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "server",
        "--bind-path",
        "tcp://0.0.0.0:443",
        "--outbound-dns-resolver",
        "1.1.1.1:53",
        "--outbound-dns-resolver",
        "[2606:4700:4700::1111]:53",
        "--outbound-dns-strategy",
        "ipv6-then-ipv4",
        "--outbound-dns-timeout-ms",
        "1500",
    ])
    .expect("parse cli");
    let config = cli.into_config().expect("config");

    let CommandConfig::Server(server) = config.command else {
        panic!("expected server config");
    };
    assert_eq!(
        server.outbound_dns.resolvers,
        vec![
            "1.1.1.1:53".parse().expect("resolver"),
            "[2606:4700:4700::1111]:53".parse().expect("resolver"),
        ]
    );
    assert_eq!(server.outbound_dns.strategy, DnsIpStrategy::Ipv6ThenIpv4);
    assert_eq!(server.outbound_dns.timeout, Duration::from_millis(1500));

    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "server",
        "--bind-path",
        "tcp://0.0.0.0:443",
        "--outbound-dns-resolver",
        "1.1.1.1:0",
    ])
    .expect("parse cli");
    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::Config(
            crate::config::ConfigError::OutboundDnsResolverPortZero
        ))
    ));
}

#[test]
fn tun_l4_cli_parses_dual_stack_and_dns() {
    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "client",
        "--tun-name",
        "mptun0",
        "--tun-ipv6",
        "fd00::1",
        "--tun-dns-resolver",
        "1.1.1.1:53",
        "--tun-dns-resolver",
        "[2606:4700:4700::1111]:53",
        "--path",
        "tcp://127.0.0.1:443",
        "--path",
        "udp://127.0.0.1:443",
    ])
    .expect("parse cli");
    let config = cli.into_config().expect("config");

    let CommandConfig::Client(client) = config.command else {
        panic!("expected client config");
    };
    let [
        LocalIngressConfig {
            config: IngressConfig::TunL4(tun),
            ..
        },
    ] = client.ingresses.as_slice()
    else {
        panic!("expected TUN L4 ingress");
    };
    assert_eq!(tun.name.as_deref(), Some("mptun0"));
    assert_eq!(tun.ipv4, Some(crate::ingress::tun::DEFAULT_TUN_IPV4));
    assert_eq!(tun.ipv6, Some("fd00::1".parse().expect("ipv6")));
    assert_eq!(
        tun.dns_resolvers,
        vec![
            "1.1.1.1:53".parse().expect("resolver"),
            "[2606:4700:4700::1111]:53".parse().expect("resolver"),
        ]
    );
    assert!(tun.enable_icmp);
    assert!(
        client
            .paths
            .iter()
            .any(|path| { path.spec.underlay == crate::protocol::UnderlayProtocol::Tcp })
    );
    assert!(
        client
            .paths
            .iter()
            .any(|path| { path.spec.underlay == crate::protocol::UnderlayProtocol::Udp })
    );
}

#[test]
fn tun_l4_cli_supports_ipv6_only() {
    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "client",
        "--tun",
        "--tun-disable-ipv4",
        "--tun-ipv6",
        "fd00::1",
        "--path",
        "tcp://127.0.0.1:443",
        "--path",
        "udp://127.0.0.1:443",
    ])
    .expect("parse cli");
    let config = cli.into_config().expect("config");

    let CommandConfig::Client(client) = config.command else {
        panic!("expected client config");
    };
    let [
        LocalIngressConfig {
            config: IngressConfig::TunL4(tun),
            ..
        },
    ] = client.ingresses.as_slice()
    else {
        panic!("expected TUN L4 ingress");
    };
    assert_eq!(tun.ipv4, None);
    assert_eq!(tun.ipv6, Some("fd00::1".parse().expect("ipv6")));
}

#[test]
fn tun_l4_validation_accepts_single_underlay_and_rejects_ipv4_flags() {
    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "client",
        "--tun",
        "--tun-disable-ipv4",
        "--tun-ipv4",
        "10.88.0.2",
        "--tun-ipv6",
        "fd00::1",
        "--path",
        "tcp://127.0.0.1:443",
        "--path",
        "udp://127.0.0.1:443",
    ])
    .expect("parse cli");
    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::TunIpv4DisabledWithIpv4Options)
    ));

    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "client",
        "--tun",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");
    cli.into_config().expect("TCP-only TUN config");

    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "client",
        "--tun",
        "--path",
        "udp://127.0.0.1:443",
    ])
    .expect("parse cli");
    cli.into_config().expect("UDP-only TUN config");

    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "client",
        "--tun",
        "--tun-dns-resolver",
        "1.1.1.1:0",
        "--path",
        "tcp://127.0.0.1:443",
        "--path",
        "udp://127.0.0.1:443",
    ])
    .expect("parse cli");
    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::Config(
            crate::config::ConfigError::TunDnsResolverPortZero
        ))
    ));
}

#[test]
fn resource_limits_are_validated() {
    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "--max-frame-bytes",
        "128",
        "--max-payload-bytes",
        "128",
        "client",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");

    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::Config(
            crate::config::ConfigError::PayloadLimitExceedsFrameLimit
        ))
    ));
}

#[test]
fn mux_memory_limits_are_validated() {
    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "--max-payload-bytes",
        "1024",
        "--max-repair-bytes",
        "512",
        "client",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");

    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::Config(
            crate::config::ConfigError::ReinjectionLimitTooSmall
        ))
    ));

    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "--max-payload-bytes",
        "1024",
        "--max-repair-bytes",
        "4096",
        "--max-reliable-relay-chunk-bytes",
        "1024",
        "--max-path-flight-bytes",
        "512",
        "client",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");

    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::Config(
            crate::config::ConfigError::PathFlightLimitTooSmall
        ))
    ));

    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "--max-payload-bytes",
        "1024",
        "--max-repair-bytes",
        "4096",
        "--max-reliable-relay-chunk-bytes",
        "1024",
        "--max-path-flight-bytes",
        "8192",
        "client",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");

    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::Config(
            crate::config::ConfigError::PathFlightLimitExceedsReinjectionLimit
        ))
    ));

    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "--max-reliable-relay-chunk-bytes",
        "0",
        "client",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");

    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::Config(
            crate::config::ConfigError::MaxReliableRelayChunkBytesZero
        ))
    ));

    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "--max-payload-bytes",
        "1024",
        "--max-reliable-relay-chunk-bytes",
        "2048",
        "client",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");

    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::Config(
            crate::config::ConfigError::MaxReliableRelayChunkExceedsPayloadLimit
        ))
    ));
}

#[test]
fn tcp_path_heartbeat_timing_is_validated() {
    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "--tcp-path-heartbeat-interval-ms",
        "0",
        "client",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");

    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::Config(
            crate::config::ConfigError::TcpPathHeartbeatIntervalZero
        ))
    ));

    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "--tcp-path-heartbeat-timeout-ms",
        "0",
        "client",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");

    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::Config(
            crate::config::ConfigError::TcpPathHeartbeatTimeoutZero
        ))
    ));

    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "--tcp-path-heartbeat-interval-ms",
        "2000",
        "--tcp-path-heartbeat-timeout-ms",
        "1000",
        "client",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");

    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::Config(
            crate::config::ConfigError::TcpPathHeartbeatTimeoutTooSmall
        ))
    ));
}

#[test]
fn path_probe_timing_is_validated() {
    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "client",
        "--path-probe-interval-ms",
        "0",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");

    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::Config(
            crate::config::ConfigError::PathProbeIntervalZero
        ))
    ));

    let cli = Cli::try_parse_from([
        "mptunnel",
        "--secret",
        "0123456789abcdef0123456789abcdef",
        "client",
        "--path-probe-timeout-ms",
        "0",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");

    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::Config(
            crate::config::ConfigError::PathProbeTimeoutZero
        ))
    ));
}
