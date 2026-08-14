use super::*;
use crate::config::CommandConfig;
use clap::Parser;
use std::ffi::OsString;

struct TestTlsMaterial {
    certificate: std::path::PathBuf,
    private_key: std::path::PathBuf,
    credential: std::path::PathBuf,
    transport_secret: std::path::PathBuf,
}

fn test_tls_material() -> &'static TestTlsMaterial {
    static MATERIAL: std::sync::OnceLock<TestTlsMaterial> = std::sync::OnceLock::new();
    MATERIAL.get_or_init(|| {
        let directory =
            std::env::temp_dir().join(format!("mptunnel-cli-test-{}", std::process::id()));
        std::fs::create_dir_all(&directory).expect("create CLI-test TLS directory");
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["mptunnel.test".to_string()])
                .expect("generate CLI-test TLS identity");
        let certificate = directory.join("certificate.pem");
        let private_key = directory.join("private-key.pem");
        let credential = directory.join("credential.key");
        let transport_secret = directory.join("transport-secret.key");
        std::fs::write(&certificate, cert.pem()).expect("write CLI-test certificate");
        std::fs::write(&private_key, signing_key.serialize_pem())
            .expect("write CLI-test private key");
        std::fs::write(&credential, b"0123456789abcdef0123456789abcdef")
            .expect("write CLI-test credential");
        std::fs::write(&transport_secret, b"transport-secret-32-bytes-value!")
            .expect("write CLI-test transport secret");
        TestTlsMaterial {
            certificate,
            private_key,
            credential,
            transport_secret,
        }
    })
}

fn parse_cli<I, T>(args: I) -> Result<Cli, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString>,
{
    let mut args = args.into_iter().map(Into::into).collect::<Vec<OsString>>();
    let client = args.iter().any(|arg| arg == "client");
    let server = args.iter().any(|arg| arg == "server");
    let has_credential = args
        .iter()
        .any(|arg| arg.to_string_lossy().starts_with("--credential-secret-"));
    if (client || server) && !has_credential {
        let material = test_tls_material();
        args.extend([
            OsString::from("--credential-secret-file"),
            material.credential.as_os_str().to_owned(),
        ]);
    }
    if client {
        let material = test_tls_material();
        args.extend([
            OsString::from("--tls-server-name"),
            OsString::from("mptunnel.test"),
            OsString::from("--tls-pinned-certificate"),
            material.certificate.as_os_str().to_owned(),
        ]);
    } else if server {
        let material = test_tls_material();
        args.extend([
            OsString::from("--tls-certificate-chain"),
            material.certificate.as_os_str().to_owned(),
            OsString::from("--tls-private-key"),
            material.private_key.as_os_str().to_owned(),
        ]);
    }
    Cli::try_parse_from(args)
}

fn ingress_configs(ingresses: &[LocalIngressConfig]) -> Vec<IngressConfig> {
    ingresses
        .iter()
        .map(|ingress| ingress.config.clone())
        .collect()
}

fn command_node(command: CommandConfig) -> NodeConfig {
    let CommandConfig::Node(node) = command;
    node
}

fn only_mpp_outbound(node: &NodeConfig) -> &MppOutboundConfig {
    let [OutboundLeafConfig::Mpp { config, .. }] = node.outbounds.as_slice() else {
        panic!("expected one MPP outbound");
    };
    config
}

fn only_local_outbound(node: &NodeConfig) -> &OutboundConfig {
    let [OutboundLeafConfig::Local { config, .. }] = node.outbounds.as_slice() else {
        panic!("expected one local outbound");
    };
    config
}

#[test]
fn mpp_cli_defaults_identity_and_keeps_transport_secret_distinct() {
    let material = test_tls_material();
    let legacy = client_tls_from_cli(None, Some(material.certificate.clone()), None)
        .expect("default MPP TLS identity");
    let explicit = client_tls_from_cli(
        Some(DEFAULT_MPP_TLS_SERVER_NAME.to_string()),
        Some(material.certificate.clone()),
        None,
    )
    .expect("explicit default MPP TLS identity");
    assert_eq!(legacy, explicit);

    let protected = client_tls_from_cli(
        None,
        Some(material.certificate.clone()),
        Some(material.transport_secret.clone()),
    )
    .expect("client shared transport secret");
    assert_ne!(legacy, protected);
    assert!(!format!("{protected:?}").contains("transport-secret-32-bytes-value"));

    server_tls_from_cli(
        Some(material.certificate.clone()),
        Some(material.private_key.clone()),
        Some(material.transport_secret.clone()),
        Duration::from_secs(DEFAULT_AUTH_FRESHNESS_WINDOW_SECONDS),
        crate::config::DEFAULT_MAX_PENDING_AUTHENTICATIONS,
    )
    .expect("server shared transport secret");
}

#[test]
fn client_cli_builds_default_socks_config() {
    let cli = parse_cli([
        "mptunnel",
        "--check-config",
        "client",
        "--path",
        "tcp://127.0.0.1:443-445",
        "--path",
        "quic://127.0.0.1:443",
    ])
    .expect("parse cli");
    let config = cli.into_config().expect("config");

    assert!(config.check_config);
    assert_eq!(config.service, ServiceConfig::default());
    assert_eq!(config.session, SessionConfig::default());
    assert_eq!(config.resources, ResourceLimits::default());
    let node = command_node(config.command);
    assert_eq!(node.forwarding_mode, crate::config::ForwardingMode::L4);
    let client = only_mpp_outbound(&node);
    assert_eq!(client.paths.len(), 2);
    assert_eq!(client.paths[0].spec.endpoint.ports().first(), 443);
    assert_eq!(client.paths[0].spec.endpoint.ports().last(), 445);
    assert_eq!(node.local_ingresses[0].name, "socks5");
    assert_eq!(
        ingress_configs(&node.local_ingresses),
        vec![IngressConfig::Socks5 {
            listen: vec!["127.0.0.1:1080".parse().expect("listen")],
            proxy_auth: ProxyAuthConfig::disabled(),
            admission: crate::ingress::LocalIngressAdmissionConfig::default(),
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

#[test]
fn client_cli_rejects_unsupported_log_levels() {
    for level in ["warning", "debug", "trace"] {
        let error = parse_cli([
            "mptunnel",
            "--log-level",
            level,
            "client",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect_err("unsupported typed log level must fail CLI parsing");
        assert_eq!(error.kind(), clap::error::ErrorKind::InvalidValue);
    }
}

#[test]
fn simple_cli_maps_the_complete_logging_surface() {
    let config = parse_cli([
        "mptunnel",
        "--log-level",
        "info",
        "--log-format",
        "json",
        "--log-file",
        "mptunnel.jsonl",
        "--log-no-console",
        "--log-flow-events",
        "client",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse logging CLI")
    .into_config()
    .expect("compile logging CLI");
    assert_eq!(config.logging.level, crate::config::LogLevel::Info);
    assert_eq!(config.logging.format, crate::config::LogFormat::Json);
    assert_eq!(
        config.logging.file,
        Some(std::path::PathBuf::from("mptunnel.jsonl"))
    );
    assert!(!config.logging.console);
    assert!(config.logging.flow_events);
}

#[test]
fn client_cli_exposes_sparse_node_limits() {
    let cli = parse_cli([
        "mptunnel",
        "--max-reinjection-cache-chunks",
        "101",
        "--max-reorder-buffer-chunks",
        "102",
        "--max-retained-receive-ranges",
        "103",
        "client",
        "--path",
        "quic://127.0.0.1:443",
    ])
    .expect("parse CLI sparse-node limits");
    let config = cli.into_config().expect("valid sparse-node limits");

    assert_eq!(config.resources.max_reinjection_cache_chunks, 101);
    assert_eq!(config.resources.max_reorder_buffer_chunks, 102);
    assert_eq!(config.resources.max_retained_receive_ranges, 103);
}

#[test]
fn client_cli_enables_dashboard_and_peer_diagnostics_independently() {
    let management_token = test_tls_material().credential.as_os_str().to_owned();
    let cli = parse_cli([
        OsString::from("mptunnel"),
        OsString::from("--management-listen"),
        OsString::from("127.0.0.1:7600"),
        OsString::from("--management-token-file"),
        management_token,
        OsString::from("--management-dashboard"),
        OsString::from("--management-allow-peer-diagnostics"),
        OsString::from("client"),
        OsString::from("--path"),
        OsString::from("tcp://127.0.0.1:443"),
    ])
    .expect("parse CLI");
    let config = cli.into_config().expect("config");

    assert_eq!(
        config.management.listen,
        vec!["127.0.0.1:7600".parse().expect("listen")]
    );
    assert_eq!(
        config.management.token.as_deref(),
        Some("0123456789abcdef0123456789abcdef")
    );
    assert!(config.management.dashboard);
    assert!(config.management.allow_peer_diagnostics);
}

#[test]
fn client_proxy_cli_accepts_multiple_dual_stack_listen_addresses() {
    let cli = parse_cli([
        "mptunnel",
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

    let node = command_node(config.command);
    assert_eq!(
        ingress_configs(&node.local_ingresses),
        vec![IngressConfig::HttpConnect {
            listen: vec![
                "127.0.0.1:8080".parse().expect("ipv4 listen"),
                "[::1]:8080".parse().expect("ipv6 listen"),
            ],
            proxy_auth: ProxyAuthConfig::disabled(),
            admission: crate::ingress::LocalIngressAdmissionConfig::default(),
        }]
    );
}

#[test]
fn client_cli_accepts_multiple_proxy_ingresses() {
    let cli = parse_cli([
        "mptunnel",
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

    let node = command_node(config.command);
    assert_eq!(
        ingress_configs(&node.local_ingresses),
        vec![
            IngressConfig::Socks5 {
                listen: vec!["127.0.0.1:1080".parse().expect("socks listen")],
                proxy_auth: ProxyAuthConfig::disabled(),
                admission: crate::ingress::LocalIngressAdmissionConfig::default(),
            },
            IngressConfig::HttpConnect {
                listen: vec!["127.0.0.1:8080".parse().expect("http listen")],
                proxy_auth: ProxyAuthConfig::disabled(),
                admission: crate::ingress::LocalIngressAdmissionConfig::default(),
            },
        ]
    );
}

#[test]
fn client_cli_builds_simple_bounded_tcp_and_udp_port_forwards() {
    let cli = parse_cli([
        "mptunnel",
        "client",
        "--tcp-forward-listen",
        "127.0.0.1:8443",
        "--tcp-forward-target",
        "SERVICE.Example.:443",
        "--tcp-forward-max-connections",
        "24",
        "--udp-forward-listen",
        "127.0.0.1:5353",
        "--udp-forward-target",
        "[2001:db8::53]:53",
        "--udp-forward-max-associations",
        "12",
        "--udp-forward-idle-timeout-ms",
        "5000",
        "--udp-forward-datagram-ttl-ms",
        "1500",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse CLI");
    let config = cli.into_config().expect("config");
    let node = command_node(config.command);
    let [tcp, udp] = node.local_ingresses.as_slice() else {
        panic!("fixed forwards must suppress the implicit SOCKS5 listener");
    };

    let IngressConfig::TcpForward(tcp) = &tcp.config else {
        panic!("expected TCP forward");
    };
    assert_eq!(tcp.listen(), &["127.0.0.1:8443".parse().expect("listen")]);
    assert_eq!(tcp.target().to_string(), "service.example:443");
    assert_eq!(tcp.max_connections(), 24);

    let IngressConfig::UdpForward(udp) = &udp.config else {
        panic!("expected UDP forward");
    };
    assert_eq!(udp.listen(), &["127.0.0.1:5353".parse().expect("listen")]);
    assert_eq!(udp.target().to_string(), "[2001:db8::53]:53");
    assert_eq!(udp.max_associations(), 12);
    assert_eq!(udp.idle_timeout(), Duration::from_secs(5));
    assert_eq!(udp.datagram_ttl_ms(), 1500);

    let incomplete = parse_cli([
        "mptunnel",
        "client",
        "--tcp-forward-max-connections",
        "24",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse incomplete CLI")
    .into_config()
    .expect_err("forward controls without listen/target must fail");
    assert!(matches!(incomplete, CliConfigError::PortForward(_)));
}

#[test]
fn client_cli_parses_optional_proxy_auth() {
    let proxy_password = test_tls_material().credential.as_os_str().to_owned();
    let cli = parse_cli([
        OsString::from("mptunnel"),
        OsString::from("client"),
        OsString::from("--proxy-username"),
        OsString::from("operator"),
        OsString::from("--proxy-password-file"),
        proxy_password,
        OsString::from("--path"),
        OsString::from("tcp://127.0.0.1:443"),
    ])
    .expect("parse cli");
    let config = cli.into_config().expect("config");

    let node = command_node(config.command);
    let [
        LocalIngressConfig {
            config: IngressConfig::Socks5 { proxy_auth, .. },
            ..
        },
    ] = node.local_ingresses.as_slice()
    else {
        panic!("expected default SOCKS5 ingress");
    };
    assert!(proxy_auth.is_required());
    assert_eq!(
        proxy_auth
            .authenticate("operator", "0123456789abcdef0123456789abcdef")
            .expect("authenticated")
            .as_str(),
        "operator"
    );
    assert!(proxy_auth.authenticate("operator", "wrong").is_none());

    let cli = parse_cli([
        "mptunnel",
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
    let cli = parse_cli([
        "mptunnel",
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

    let node = command_node(config.command);
    assert_eq!(
        ingress_configs(&node.local_ingresses),
        vec![
            IngressConfig::Socks5 {
                listen: vec!["127.0.0.1:1080".parse().expect("socks listen")],
                proxy_auth: ProxyAuthConfig::disabled(),
                admission: crate::ingress::LocalIngressAdmissionConfig::default(),
            },
            IngressConfig::HttpConnect {
                listen: vec!["127.0.0.1:8080".parse().expect("http listen")],
                proxy_auth: ProxyAuthConfig::disabled(),
                admission: crate::ingress::LocalIngressAdmissionConfig::default(),
            },
        ]
    );
}

#[test]
fn removed_record_cipher_cli_is_rejected() {
    let error = parse_cli([
        "mptunnel",
        "--cipher",
        "chacha20-poly1305",
        "client",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect_err("legacy record cipher flag must not survive the TLS cut");
    assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
}

#[test]
fn service_supervisor_cli_is_parsed_and_validated() {
    let cli = parse_cli([
        "mptunnel",
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

    let cli = parse_cli([
        "mptunnel",
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
    let cli = parse_cli(["mptunnel", "platform"]).expect("parse cli");
    assert!(matches!(cli.command, Command::Platform(_)));
}

#[test]
fn operational_commands_are_typed_and_do_not_require_runtime_credentials() {
    let route = Cli::try_parse_from([
        "mptunnel",
        "--config",
        "edge.toml",
        "route",
        "explain",
        "--target",
        "API.Example.:443",
        "--network",
        "tcp",
        "--source",
        "198.51.100.8:41000",
        "--principal-id",
        "alice",
        "--inbound",
        "local-socks",
        "--resolved-ip",
        "192.0.2.4",
    ])
    .expect("parse route explain");
    let Command::Route(route) = route.command else {
        panic!("expected route explain");
    };
    let RouteCommand::Explain(route) = route.command;
    assert_eq!(route.target.authority(), "api.example:443");
    assert_eq!(route.network, RouteNetworkArg::Tcp);
    assert_eq!(route.resolved_ip, Some("192.0.2.4".parse().expect("IP")));

    for (flag, value) in [
        ("--interface", "eth0"),
        ("--process-name", "browser"),
        ("--process-path", "/apps/browser"),
        ("--process-package", "com.example.browser"),
        ("--tls-server-name", "api.example"),
        ("--http-host", "api.example"),
        ("--quic-server-name", "api.example"),
    ] {
        let error = Cli::try_parse_from([
            "mptunnel",
            "route",
            "explain",
            "--target",
            "api.example:443",
            "--network",
            "tcp",
            "--source",
            "198.51.100.8:41000",
            "--inbound",
            "local-socks",
            flag,
            value,
        ])
        .expect_err("unsupported route-explain metadata flag must be rejected");
        assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    let status = Cli::try_parse_from([
        "mptunnel",
        "--management-token-env",
        "MPTUNNEL_TEST_TOKEN",
        "status",
        "--address",
        "127.0.0.1:7600",
    ])
    .expect("parse status");
    assert!(matches!(
        status.command,
        Command::Status(ManagementClientArgs {
            address: Some(address)
        }) if address == "127.0.0.1:7600".parse().expect("address")
    ));

    let dns = Cli::try_parse_from([
        "mptunnel",
        "--management-token-file",
        "management-token.key",
        "dns",
        "--address",
        "[::1]:7600",
        "query",
        "WWW.Example.",
        "--type",
        "AAAA",
    ])
    .expect("parse DNS query");
    assert!(matches!(
        dns.command,
        Command::Dns(DnsArgs {
            address: Some(address),
            command: DnsCommand::Query(DnsQueryArgs { record_type, .. }),
        }) if address == "[::1]:7600".parse().expect("address") && record_type == "AAAA"
    ));

    assert!(
        Cli::try_parse_from([
            "mptunnel",
            "route",
            "explain",
            "--network",
            "tcp",
            "--source",
            "127.0.0.1:1",
            "--principal-id",
            "alice",
            "--inbound",
            "local-socks",
        ])
        .is_err()
    );
}

#[test]
fn management_token_value_has_no_raw_cli_argument() {
    assert!(
        Cli::try_parse_from([
            "mptunnel",
            "--management-token",
            "0123456789abcdef",
            "status",
        ])
        .is_err()
    );
}

#[test]
fn secret_is_required() {
    let cli = Cli::try_parse_from(["mptunnel", "client", "--path", "quic://127.0.0.1:443"])
        .expect("parse cli");

    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::Security(
            crate::config::SecurityPolicyError::MissingSecret
        ))
    ));
}

#[test]
fn raw_secret_argument_is_not_part_of_the_clean_cli() {
    assert!(
        Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef0123456789abcdef",
            "client",
            "--path",
            "quic://127.0.0.1:443",
        ])
        .is_err()
    );
}

#[test]
fn server_outbound_requires_matching_parameters() {
    let cli = parse_cli([
        "mptunnel",
        "server",
        "--bind-path",
        "tcp://0.0.0.0:443",
        "--outbound-protocol",
        "socks5",
    ])
    .expect("parse cli");

    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::MissingUpstreamSocks5)
    ));

    let cli = parse_cli([
        "mptunnel",
        "server",
        "--bind-path",
        "quic://0.0.0.0:443",
        "--outbound-protocol",
        "http-connect",
    ])
    .expect("parse cli");

    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::MissingUpstreamHttp)
    ));

    let cli = parse_cli(["mptunnel", "server", "--bind-path", "tcp://0.0.0.0:443-445"])
        .expect("parse ranged carrier endpoint");
    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::Config(
            crate::config::ConfigError::ServerPathPortRange
        ))
    ));
}

#[test]
fn server_https_connect_outbound_and_proxy_auth_are_parsed() {
    let proxy_password = test_tls_material().credential.as_os_str().to_owned();
    let cli = parse_cli([
        OsString::from("mptunnel"),
        OsString::from("server"),
        OsString::from("--bind-path"),
        OsString::from("quic://0.0.0.0:443"),
        OsString::from("--outbound-protocol"),
        OsString::from("https-connect"),
        OsString::from("--upstream-http"),
        OsString::from("127.0.0.1:8443"),
        OsString::from("--upstream-http-tls-server-name"),
        OsString::from("proxy.example"),
        OsString::from("--upstream-proxy-username"),
        OsString::from("alice"),
        OsString::from("--upstream-proxy-password-file"),
        proxy_password,
    ])
    .expect("parse cli");
    let config = cli.into_config().expect("config");

    let node = command_node(config.command);
    let OutboundConfig::HttpsConnect(proxy) = only_local_outbound(&node) else {
        panic!("expected HTTPS CONNECT");
    };
    assert_eq!(proxy.tls_server_name(), "proxy.example");
    assert_eq!(
        proxy.proxy().credentials().expect("credentials").username(),
        "alice"
    );
    assert!(!format!("{proxy:?}").contains("0123456789abcdef"));
}

#[test]
fn server_upstream_proxy_auth_requires_both_fields() {
    let cli = parse_cli([
        "mptunnel",
        "server",
        "--bind-path",
        "quic://0.0.0.0:443",
        "--outbound-protocol",
        "socks5",
        "--upstream-socks5",
        "127.0.0.1:1080",
        "--upstream-proxy-username",
        "alice",
    ])
    .expect("parse cli");

    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::UpstreamProxyPasswordRequired)
    ));
}

#[test]
fn server_outbound_dns_is_parsed() {
    let cli = parse_cli([
        "mptunnel",
        "server",
        "--bind-path",
        "tcp://0.0.0.0:443",
        "--outbound-dns-server",
        "1.1.1.1:53",
        "--outbound-dns-server",
        "[2606:4700:4700::1111]:53",
        "--outbound-dns-protocol",
        "udp-tcp",
        "--outbound-dns-family",
        "ipv6-then-ipv4",
        "--outbound-dns-timeout-ms",
        "1500",
    ])
    .expect("parse cli");
    let config = cli.into_config().expect("config");

    let node = command_node(config.command);
    assert_eq!(node.forwarding_mode, crate::config::ForwardingMode::L4);
    let dns = node.dns_policy.compile().expect("DNS policy");
    assert_eq!(
        dns.upstreams()
            .filter_map(|upstream| upstream.endpoint().bootstrap())
            .collect::<Vec<_>>(),
        vec![
            "1.1.1.1:53".parse().expect("resolver"),
            "[2606:4700:4700::1111]:53".parse().expect("resolver"),
        ]
    );
    let default = crate::product::DnsPlanId::parse("default").expect("plan");
    let plan = dns.plan(&default).expect("default DNS plan");
    assert_eq!(plan.ip_strategy(), DnsIpStrategy::Ipv6ThenIpv4);
    assert_eq!(plan.limits().lookup_timeout, Duration::from_millis(1500));
    assert_eq!(node.servers[0].dns_plan.as_ref(), Some(&default));

    let cli = parse_cli([
        "mptunnel",
        "server",
        "--bind-path",
        "tcp://0.0.0.0:443",
        "--outbound-dns-server",
        "1.1.1.1:0",
        "--outbound-dns-protocol",
        "udp-tcp",
    ])
    .expect("parse cli");
    assert!(matches!(cli.into_config(), Err(CliConfigError::Dns(_))));
}

#[test]
fn removed_dns_cli_names_are_not_accepted_as_aliases() {
    for arguments in [
        vec![
            "mptunnel",
            "server",
            "--bind-path",
            "tcp://0.0.0.0:443",
            "--outbound-dns-resolver",
            "1.1.1.1:53",
        ],
        vec![
            "mptunnel",
            "client",
            "--path",
            "tcp://127.0.0.1:443",
            "--tun-dns-resolver",
            "1.1.1.1:53",
        ],
        vec![
            "mptunnel",
            "client",
            "--path",
            "tcp://127.0.0.1:443",
            "--tun-dns-dot-bootstrap",
            "1.1.1.1:853",
        ],
    ] {
        assert!(parse_cli(arguments).is_err());
    }
}

#[test]
fn tun_l4_cli_parses_dual_stack_and_dns() {
    let cli = parse_cli([
        "mptunnel",
        "client",
        "--tun-interface-name",
        "mptun0",
        "--tun-ipv6",
        "fd00::1",
        "--tun-dns-redirect",
        "1.1.1.1:53",
        "--tun-dns-redirect",
        "[2606:4700:4700::1111]:53",
        "--path",
        "tcp://127.0.0.1:443",
        "--path",
        "quic://127.0.0.1:443",
    ])
    .expect("parse cli");
    let config = cli.into_config().expect("config");

    let node = command_node(config.command);
    let client = only_mpp_outbound(&node);
    let [
        LocalIngressConfig {
            config: IngressConfig::TunL4(tun),
            ..
        },
    ] = node.local_ingresses.as_slice()
    else {
        panic!("expected TUN L4 ingress");
    };
    assert_eq!(tun.interface_name.as_deref(), Some("mptun0"));
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
    assert!(matches!(
        tun.host,
        crate::ingress::tun::TunHostConfig::External
    ));
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
    let cli = parse_cli([
        "mptunnel",
        "client",
        "--tun",
        "--tun-disable-ipv4",
        "--tun-ipv6",
        "fd00::1",
        "--path",
        "tcp://127.0.0.1:443",
        "--path",
        "quic://127.0.0.1:443",
    ])
    .expect("parse cli");
    let config = cli.into_config().expect("config");

    let node = command_node(config.command);
    let [
        LocalIngressConfig {
            config: IngressConfig::TunL4(tun),
            ..
        },
    ] = node.local_ingresses.as_slice()
    else {
        panic!("expected TUN L4 ingress");
    };
    assert_eq!(tun.ipv4, None);
    assert_eq!(tun.ipv6, Some("fd00::1".parse().expect("ipv6")));
}

#[test]
fn tun_l4_validation_accepts_single_underlay_and_rejects_ipv4_flags() {
    let cli = parse_cli([
        "mptunnel",
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
        "quic://127.0.0.1:443",
    ])
    .expect("parse cli");
    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::TunIpv4DisabledWithIpv4Options)
    ));

    let cli = parse_cli([
        "mptunnel",
        "client",
        "--tun",
        "--path",
        "tcp://127.0.0.1:443",
    ])
    .expect("parse cli");
    cli.into_config().expect("TCP-only TUN config");

    let cli = parse_cli([
        "mptunnel",
        "client",
        "--tun",
        "--path",
        "quic://127.0.0.1:443",
    ])
    .expect("parse cli");
    cli.into_config().expect("UDP-only TUN config");

    let cli = parse_cli([
        "mptunnel",
        "client",
        "--tun",
        "--tun-dns-redirect",
        "1.1.1.1:0",
        "--path",
        "tcp://127.0.0.1:443",
        "--path",
        "quic://127.0.0.1:443",
    ])
    .expect("parse cli");
    assert!(matches!(cli.into_config(), Err(CliConfigError::Dns(_))));
}

#[test]
fn managed_full_vpn_cli_builds_explicit_host_policy() {
    let cli = parse_cli([
        "mptunnel",
        "client",
        "--tun-vpn-mode",
        "full",
        "--tun-interface-name",
        "daily0",
        "--tun-exclude-cidr",
        "192.168.4.7/16",
        "--tun-local-lan",
        "--tun-dns-listener",
        "10.88.0.53",
        "--tun-dns-dot-address",
        "1.1.1.1:853",
        "--tun-dns-dot-tls-name",
        "cloudflare-dns.com",
        "--path",
        "quic://127.0.0.1:443",
    ])
    .expect("parse CLI");
    let config = cli.into_config().expect("managed full config");
    let node = command_node(config.command);
    let dns = node.dns_policy.compile().expect("managed DNS policy");
    assert!(dns.is_encrypted_only());
    assert!(!dns.uses_system_resolution());
    assert_eq!(
        dns.bootstrap_endpoints().collect::<Vec<_>>(),
        vec!["1.1.1.1:853".parse().expect("DoT bootstrap")]
    );
    let [
        LocalIngressConfig {
            config: IngressConfig::TunL4(tun),
            ..
        },
    ] = node.local_ingresses.as_slice()
    else {
        panic!("expected managed TUN");
    };
    assert!(
        tun.managed_vpn()
            .expect("portable managed policy")
            .platform
            .linux
            .is_none(),
        "generic CLI must not synthesize Linux-only tuning"
    );
    let platform = tun
        .compile_managed_vpn()
        .expect("compile")
        .expect("managed");

    assert_eq!(tun.interface_name.as_deref(), Some("daily0"));
    assert_eq!(platform.route_mode(), &crate::platform::RouteMode::Full);
    assert_eq!(
        platform.excludes(),
        &["192.168.0.0/16".parse().expect("exclude")]
    );
    assert!(platform.local_lan());
    assert_eq!(
        tun.managed_dns_capture_servers(),
        &["10.88.0.53".parse::<std::net::IpAddr>().expect("DNS")]
    );
}

#[test]
fn managed_split_vpn_cli_builds_bounded_include_policy() {
    let cli = parse_cli([
        "mptunnel",
        "client",
        "--tun-vpn-mode",
        "split",
        "--tun-include-cidr",
        "10.1.2.3/8",
        "--tun-exclude-cidr",
        "10.20.30.40/16",
        "--tun-dns-dot-address",
        "9.9.9.9:853",
        "--tun-dns-dot-tls-name",
        "dns.quad9.net",
        "--path",
        "quic://127.0.0.1:443",
    ])
    .expect("parse CLI");
    let config = cli.into_config().expect("managed split config");
    let node = command_node(config.command);
    let [
        LocalIngressConfig {
            config: IngressConfig::TunL4(tun),
            ..
        },
    ] = node.local_ingresses.as_slice()
    else {
        panic!("expected managed TUN");
    };
    let platform = tun
        .compile_managed_vpn()
        .expect("compile")
        .expect("managed");

    assert_eq!(
        platform.route_mode(),
        &crate::platform::RouteMode::Split(vec!["10.0.0.0/8".parse().expect("include")])
    );
    assert_eq!(
        platform.excludes(),
        &["10.20.0.0/16".parse().expect("exclude")]
    );
    assert!(platform.dns().is_none());
}

#[test]
fn managed_vpn_cli_requires_explicit_mode_and_valid_mode_fields() {
    assert!(
        parse_cli([
            "mptunnel",
            "client",
            "--tun-local-lan",
            "--path",
            "quic://127.0.0.1:443",
        ])
        .is_err(),
        "managed-only flags must not silently change an external TUN"
    );

    let full_with_include = parse_cli([
        "mptunnel",
        "client",
        "--tun-vpn-mode",
        "full",
        "--tun-include-cidr",
        "10.0.0.0/8",
        "--tun-dns-listener",
        "10.88.0.53",
        "--tun-dns-dot-address",
        "1.1.1.1:853",
        "--tun-dns-dot-tls-name",
        "cloudflare-dns.com",
        "--path",
        "quic://127.0.0.1:443",
    ])
    .expect("parse CLI");
    assert!(matches!(
        full_with_include.into_config(),
        Err(CliConfigError::ManagedVpn(message))
            if message.contains("cannot be combined with --tun-include-cidr")
    ));
}

#[test]
fn managed_full_vpn_cli_requires_capture_and_rejects_external_dns() {
    let without_capture = parse_cli([
        "mptunnel",
        "client",
        "--tun-vpn-mode",
        "full",
        "--tun-dns-dot-address",
        "1.1.1.1:853",
        "--tun-dns-dot-tls-name",
        "cloudflare-dns.com",
        "--path",
        "quic://127.0.0.1:443",
    ])
    .expect("parse CLI");
    assert!(matches!(
        without_capture.into_config(),
        Err(CliConfigError::Config(
            crate::config::ConfigError::ManagedVpn(message)
        )) if message.contains("requires at least one DNS listener")
    ));

    let managed_without_dot = parse_cli([
        "mptunnel",
        "client",
        "--tun-vpn-mode",
        "full",
        "--tun-dns-listener",
        "10.88.0.53",
        "--path",
        "quic://127.0.0.1:443",
    ])
    .expect("parse CLI");
    assert!(matches!(
        managed_without_dot.into_config(),
        Err(CliConfigError::ManagedDnsDotRequired)
    ));

    let plaintext_dns = parse_cli([
        "mptunnel",
        "client",
        "--tun-vpn-mode",
        "full",
        "--tun-dns-listener",
        "10.88.0.53",
        "--tun-dns-redirect",
        "1.1.1.1:53",
        "--path",
        "quic://127.0.0.1:443",
    ])
    .expect("parse CLI");
    assert!(matches!(
        plaintext_dns.into_config(),
        Err(CliConfigError::ManagedVpn(message))
            if message.contains("external/manual only")
    ));
}

#[test]
fn managed_vpn_cli_requires_a_complete_dot_identity() {
    for incomplete in [
        vec!["--tun-dns-dot-address", "1.1.1.1:853"],
        vec!["--tun-dns-dot-tls-name", "cloudflare-dns.com"],
    ] {
        let mut args = vec![
            "mptunnel",
            "client",
            "--tun-vpn-mode",
            "full",
            "--tun-dns-listener",
            "10.88.0.53",
            "--path",
            "quic://127.0.0.1:443",
        ];
        args.extend(incomplete);
        assert!(
            parse_cli(args).is_err(),
            "DoT bootstrap and identity must be paired"
        );
    }

    let invalid_name = parse_cli([
        "mptunnel",
        "client",
        "--tun-vpn-mode",
        "full",
        "--tun-dns-listener",
        "10.88.0.53",
        "--tun-dns-dot-address",
        "1.1.1.1:853",
        "--tun-dns-dot-tls-name",
        "not a dns name",
        "--path",
        "quic://127.0.0.1:443",
    ])
    .expect("parse CLI");
    assert!(matches!(
        invalid_name.into_config(),
        Err(CliConfigError::Dns(_))
    ));

    let zero_port = parse_cli([
        "mptunnel",
        "client",
        "--tun-vpn-mode",
        "full",
        "--tun-dns-listener",
        "10.88.0.53",
        "--tun-dns-dot-address",
        "1.1.1.1:0",
        "--tun-dns-dot-tls-name",
        "cloudflare-dns.com",
        "--path",
        "quic://127.0.0.1:443",
    ])
    .expect("parse CLI");
    assert!(matches!(
        zero_port.into_config(),
        Err(CliConfigError::Dns(_))
    ));
}

#[test]
fn resource_limits_are_validated() {
    let cli = parse_cli([
        "mptunnel",
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
    let cli = parse_cli([
        "mptunnel",
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

    let cli = parse_cli([
        "mptunnel",
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

    let cli = parse_cli([
        "mptunnel",
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

    let cli = parse_cli([
        "mptunnel",
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

    let cli = parse_cli([
        "mptunnel",
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
    let cli = parse_cli([
        "mptunnel",
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

    let cli = parse_cli([
        "mptunnel",
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

    let cli = parse_cli([
        "mptunnel",
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
fn logical_session_retention_and_quic_liveness_are_independently_configurable() {
    let cli = parse_cli([
        "mptunnel",
        "--session-retention-timeout-ms",
        "45000",
        "--quic-path-keep-alive-interval-ms",
        "3000",
        "--quic-path-idle-timeout-ms",
        "12000",
        "client",
        "--path",
        "quic://127.0.0.1:443",
    ])
    .expect("parse cli");
    let config = cli.into_config().expect("config");

    assert_eq!(config.session.retention_timeout, Duration::from_secs(45));
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
fn logical_session_and_quic_liveness_timing_is_validated() {
    for (args, expected) in [
        (
            vec!["--session-retention-timeout-ms", "0"],
            crate::config::ConfigError::SessionRetentionTimeoutZero,
        ),
        (
            vec!["--quic-path-keep-alive-interval-ms", "0"],
            crate::config::ConfigError::QuicPathKeepAliveIntervalZero,
        ),
        (
            vec!["--quic-path-idle-timeout-ms", "0"],
            crate::config::ConfigError::QuicPathIdleTimeoutZero,
        ),
    ] {
        let mut command = vec!["mptunnel"];
        command.extend(args);
        command.extend(["client", "--path", "quic://127.0.0.1:443"]);
        let cli = parse_cli(command).expect("parse cli");
        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Config(actual)) if actual == expected
        ));
    }

    let cli = parse_cli([
        "mptunnel",
        "--quic-path-keep-alive-interval-ms",
        "10000",
        "--quic-path-idle-timeout-ms",
        "10000",
        "client",
        "--path",
        "quic://127.0.0.1:443",
    ])
    .expect("parse cli");
    assert!(matches!(
        cli.into_config(),
        Err(CliConfigError::Config(
            crate::config::ConfigError::QuicPathIdleTimeoutTooSmall
        ))
    ));
}

#[test]
fn path_probe_timing_is_validated() {
    let cli = parse_cli([
        "mptunnel",
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

    let cli = parse_cli([
        "mptunnel",
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
