use super::*;
use crate::config::CommandConfig;
use crate::product::{
    DomainName, EgressAction, FlowContext, InboundId, InitialDemand, Network, PrincipalId,
    ProtocolTarget, RouteInput, RouteMismatch, SourceEndpoint,
};

const TEST_CERTIFICATE_FILE: &str = "mptunnel-test-certificate.pem";
const TEST_PRIVATE_KEY_FILE: &str = "mptunnel-test-private-key.pem";
const TEST_CREDENTIAL_FILE: &str = "mptunnel-test-credential.key";
const TEST_CREDENTIAL_A_FILE: &str = "mptunnel-test-credential-a.key";
const TEST_CREDENTIAL_B_FILE: &str = "mptunnel-test-credential-b.key";
const TEST_CREDENTIAL_C_FILE: &str = "mptunnel-test-credential-c.key";
const TEST_CREDENTIAL_FED_FILE: &str = "mptunnel-test-credential-fed.key";
const TEST_REFERENCE_CREDENTIAL_FILE: &str = "mpp-credential.key";
const TEST_MANAGEMENT_TOKEN_FILE: &str = "management-token.key";
const TEST_PROXY_PASSWORD_FILE: &str = "proxy-password.key";
const TEST_TRANSPORT_SECRET_FILE: &str = "mptunnel-transport-secret.key";
const TEST_SHORT_TRANSPORT_SECRET_FILE: &str = "mptunnel-short-transport-secret.key";
const TEST_CREDENTIAL_CATALOG: &str = r#"
[[credentials]]
credential_id = "test-default"
principal_id = "test-peer"
secret = { from = "file", path = "mptunnel-test-credential.key" }

[[credentials]]
credential_id = "test-a"
principal_id = "test-peer"
secret = { from = "file", path = "mptunnel-test-credential-a.key" }

[[credentials]]
credential_id = "test-b"
principal_id = "test-peer"
secret = { from = "file", path = "mptunnel-test-credential-b.key" }

[[credentials]]
credential_id = "test-c"
principal_id = "test-peer"
secret = { from = "file", path = "mptunnel-test-credential-c.key" }

[[credentials]]
credential_id = "test-fed"
principal_id = "test-peer"
secret = { from = "file", path = "mptunnel-test-credential-fed.key" }
"#;

struct TestTlsDirectory {
    path: std::path::PathBuf,
}

impl TestTlsDirectory {
    fn new() -> Self {
        static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sequence = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mptunnel-config-file-test-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir(&path).expect("create config-test TLS directory");
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["mptunnel.test".to_string()])
                .expect("generate config-test TLS identity");
        std::fs::write(path.join(TEST_CERTIFICATE_FILE), cert.pem())
            .expect("write config-test certificate");
        std::fs::write(
            path.join(TEST_PRIVATE_KEY_FILE),
            signing_key.serialize_pem(),
        )
        .expect("write config-test private key");
        for (file, secret) in [
            (
                TEST_CREDENTIAL_FILE,
                b"0123456789abcdef0123456789abcdef".as_slice(),
            ),
            (TEST_CREDENTIAL_A_FILE, b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            (TEST_CREDENTIAL_B_FILE, b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            (TEST_CREDENTIAL_C_FILE, b"cccccccccccccccccccccccccccccccc"),
            (
                TEST_CREDENTIAL_FED_FILE,
                b"fedcba9876543210fedcba9876543210",
            ),
            (
                TEST_REFERENCE_CREDENTIAL_FILE,
                b"0123456789abcdef0123456789abcdef",
            ),
            (TEST_MANAGEMENT_TOKEN_FILE, b"operator-token-123".as_slice()),
            (TEST_PROXY_PASSWORD_FILE, b"proxy-password".as_slice()),
            (
                TEST_TRANSPORT_SECRET_FILE,
                b"transport-secret-32-bytes-value!",
            ),
            (TEST_SHORT_TRANSPORT_SECRET_FILE, b"too-short"),
        ] {
            std::fs::write(path.join(file), secret).expect("write config-test credential");
        }
        Self { path }
    }
}

impl Drop for TestTlsDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn load_config_toml_str(contents: &str) -> Result<AppConfig, ConfigFileError> {
    let directory = TestTlsDirectory::new();
    let document = if contents.contains("[[credentials]]") {
        contents.to_string()
    } else {
        format!("{TEST_CREDENTIAL_CATALOG}\n{contents}")
    };
    super::load_config_toml_str_at(&document, &directory.path)
}

#[test]
fn logging_schema_is_typed_strict_and_config_relative() {
    for (level, expected) in [
        ("off", LogLevel::Off),
        ("error", LogLevel::Error),
        ("warn", LogLevel::Warn),
        ("info", LogLevel::Info),
    ] {
        let document = format!(
            "[logging]\nlevel = {level:?}\n{TEST_CREDENTIAL_CATALOG}\n{}",
            managed_tun_document("")
        );
        assert_eq!(
            load_config_toml_str(&document)
                .expect("supported log level")
                .logging
                .level,
            expected
        );
    }

    for level in ["warning", "INFO", "debug", "trace", "verbose", ""] {
        let document = format!(
            "[logging]\nlevel = {level:?}\n{TEST_CREDENTIAL_CATALOG}\n{}",
            managed_tun_document("")
        );
        assert!(matches!(
            load_config_toml_str(&document),
            Err(ConfigFileError::Toml(_))
        ));
    }

    for (format, expected) in [("text", LogFormat::Text), ("json", LogFormat::Json)] {
        let document = format!(
            "[logging]\nformat = {format:?}\n{TEST_CREDENTIAL_CATALOG}\n{}",
            managed_tun_document("")
        );
        assert_eq!(
            load_config_toml_str(&document)
                .expect("supported log format")
                .logging
                .format,
            expected
        );
    }

    let directory = TestTlsDirectory::new();
    let document = format!(
        "[logging]\nformat = \"json\"\nconsole = false\nfile = \"logs/mptunnel.jsonl\"\nflow_events = true\n{TEST_CREDENTIAL_CATALOG}\n{}",
        managed_tun_document("")
    );
    let config = super::load_config_toml_str_at(&document, &directory.path)
        .expect("config-relative file logger");
    assert_eq!(
        config.logging.file,
        Some(directory.path.join("logs/mptunnel.jsonl"))
    );
    assert!(config.logging.flow_events);

    for logging in [
        "log_level = \"info\"",
        "[logging]\nconsole = false",
        "[logging]\nfile = \"\"",
        "[logging]\nlevel = \"warn\"\nflow_events = true",
        "[logging]\nunknown = true",
    ] {
        let document = format!(
            "{logging}\n{TEST_CREDENTIAL_CATALOG}\n{}",
            managed_tun_document("")
        );
        assert!(
            load_config_toml_str(&document).is_err(),
            "invalid logging document was accepted: {logging}"
        );
    }
}

#[test]
fn toml_diagnostics_discard_document_values_at_the_parse_boundary() {
    for (canary, document) in [
        (
            "inline-toml-canary",
            "[management]\ntoken = \"inline-toml-canary\"",
        ),
        (
            "multiline-toml-canary",
            "[management]\ntoken = \"\"\"multiline-toml-canary\"\"\"",
        ),
        (
            "ordinary-field-canary",
            "[resources]\nmax_streams = \"ordinary-field-canary\"",
        ),
    ] {
        let error = super::load_config_toml_str_at(document, Path::new("."))
            .expect_err("invalid TOML field type");
        let rendered = error.to_string();
        let debug = format!("{error:?}");
        assert!(rendered.contains("configuration document is invalid"));
        assert!(rendered.contains("line") && rendered.contains("column"));
        assert!(!rendered.contains(canary), "display leaked {canary}");
        assert!(!debug.contains(canary), "debug leaked {canary}");
        assert!(std::error::Error::source(&error).is_none());
    }
}

fn ingress_configs(ingresses: &[LocalIngressConfig]) -> Vec<IngressConfig> {
    ingresses
        .iter()
        .map(|ingress| ingress.config.clone())
        .collect()
}

fn mpp_outbounds(node: &NodeConfig) -> Vec<&MppOutboundConfig> {
    node.outbounds
        .iter()
        .filter_map(|outbound| match outbound {
            OutboundLeafConfig::Mpp { config, .. } => Some(config.as_ref()),
            OutboundLeafConfig::Local { .. } => None,
        })
        .collect()
}

#[test]
fn forwarding_mode_is_strict_global_and_defaults_to_l4() {
    let omitted = load_config_toml_str(&managed_tun_document("")).expect("omitted forwarding mode");
    let CommandConfig::Node(omitted) = omitted.command;
    assert_eq!(omitted.forwarding_mode, ForwardingMode::L4);

    let explicit = format!(
        "forwarding_mode = \"l4\"\n{TEST_CREDENTIAL_CATALOG}\n{}",
        managed_tun_document("")
    );
    let explicit = load_config_toml_str(&explicit).expect("explicit L4 forwarding mode");
    let CommandConfig::Node(explicit) = explicit.command;
    assert_eq!(explicit.forwarding_mode, ForwardingMode::L4);

    for unsupported in ["L4", "layer-4", "auto", ""] {
        let document = format!(
            "forwarding_mode = {unsupported:?}\n{TEST_CREDENTIAL_CATALOG}\n{}",
            managed_tun_document("")
        );
        assert!(matches!(
            load_config_toml_str(&document),
            Err(ConfigFileError::Toml(_))
        ));
    }
}

fn mpp_outbound_security_document(extra_security: &str) -> String {
    format!(
        r#"
[[inbounds]]
name = "local-socks"
protocol = "socks5"

[[outbounds]]
name = "edge"
protocol = "mpp"
paths = [{{ name = "path-1", endpoint = "udp://127.0.0.1:443" }}]

[outbounds.security]
credential_id = "test-default"
tls_pinned_certificate_file = "{TEST_CERTIFICATE_FILE}"
{extra_security}

[routing]
[[routing.rules]]
name = "default"
action = "outbound"
outbound = "edge"
"#,
    )
}

fn mpp_inbound_security_document(extra_security: &str) -> String {
    format!(
        r#"
[[inbounds]]
name = "edge"
protocol = "mpp"
paths = [{{ name = "path-1", endpoint = "tcp://127.0.0.1:443" }}]
outbound = "direct"

[inbounds.security]
credential_ids = ["test-default"]
tls_certificate_chain_file = "{TEST_CERTIFICATE_FILE}"
tls_private_key_file = "{TEST_PRIVATE_KEY_FILE}"
{extra_security}

[[outbounds]]
name = "direct"
protocol = "direct"
"#,
    )
}

#[test]
fn shared_transport_secret_is_optional_distinct_and_strict() {
    let default_name =
        load_config_toml_str(&mpp_outbound_security_document("")).expect("default MPP TLS name");
    let explicit_name = load_config_toml_str(&mpp_outbound_security_document(
        r#"tls_server_name = "mptunnel.example""#,
    ))
    .expect("explicit default MPP TLS name");
    let CommandConfig::Node(default_name) = default_name.command;
    let CommandConfig::Node(explicit_name) = explicit_name.command;
    assert_eq!(
        mpp_outbounds(&default_name)[0].paths[0]
            .tls
            .quic_server_name_text()
            .as_deref(),
        Some(DEFAULT_MPP_TLS_SERVER_NAME)
    );
    assert_eq!(
        mpp_outbounds(&explicit_name)[0].paths[0]
            .tls
            .quic_server_name_text(),
        mpp_outbounds(&default_name)[0].paths[0]
            .tls
            .quic_server_name_text(),
        "omitted MPP TLS name must compile to the documented default"
    );

    let protected = load_config_toml_str(&mpp_outbound_security_document(&format!(
        "transport_secret_file = {TEST_TRANSPORT_SECRET_FILE:?}"
    )))
    .expect("client shared transport secret");
    let CommandConfig::Node(protected) = protected.command;
    assert!(
        !mpp_outbounds(&default_name)[0].paths[0]
            .tls
            .shared_transport_secret_configured()
    );
    assert!(
        mpp_outbounds(&protected)[0].paths[0]
            .tls
            .shared_transport_secret_configured()
    );
    assert!(
        !format!("{:?}", mpp_outbounds(&protected)[0].paths[0].tls)
            .contains("transport-secret-32-bytes-value")
    );

    let server = load_config_toml_str(&mpp_inbound_security_document(&format!(
        "transport_secret_file = {TEST_TRANSPORT_SECRET_FILE:?}"
    )))
    .expect("server shared transport secret");
    let CommandConfig::Node(server) = server.command;
    assert_eq!(server.servers.len(), 1);
    assert!(server.servers[0].tls.shared_transport_secret_configured());

    for file in [
        TEST_SHORT_TRANSPORT_SECRET_FILE,
        "missing-transport-secret.key",
    ] {
        let error = load_config_toml_str(&mpp_outbound_security_document(&format!(
            "transport_secret_file = {file:?}"
        )))
        .expect_err("invalid shared transport secret");
        assert!(error.to_string().contains("shared transport secret"));
    }

    for removed in ["tcp_server_public_key_file", "tcp_server_private_key_file"] {
        assert!(matches!(
            load_config_toml_str(&mpp_outbound_security_document(&format!(
                "{removed} = \"obsolete.key\""
            ))),
            Err(ConfigFileError::Toml(_))
        ));
    }
}

fn local_outbound<'a>(node: &'a NodeConfig, id: &str) -> &'a OutboundConfig {
    node.outbounds
        .iter()
        .find_map(|outbound| match outbound {
            OutboundLeafConfig::Local {
                id: outbound_id,
                config,
                ..
            } if outbound_id.as_str() == id => Some(config),
            OutboundLeafConfig::Mpp { .. } | OutboundLeafConfig::Local { .. } => None,
        })
        .expect("local outbound")
}

fn managed_tun_document(host: &str) -> String {
    format!(
        r#"
[[inbounds]]
name = "local-tun"
protocol = "tun"
interface_name = "daily0"
ipv4 = "10.88.0.1"
ipv4_prefix = 24
ipv6 = "fd00:88::1"
ipv6_prefix = 64

{host}

[[outbounds]]
name = "edge"
protocol = "mpp"
paths = [{{ name = "path-1", endpoint = "udp://127.0.0.1:443" }}]

[outbounds.security]
credential_id = "test-default"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]

[[routing.rules]]
name = "default"
action = "outbound"
outbound = "edge"
"#
    )
}

fn managed_fake_dns_document(host: &str) -> String {
    format!(
        r#"
[dns]
default_dns_plan = "secure"

[dns.fake_dns]
ipv4_pool = "198.18.0.0/16"
max_entries = 4096
answer_ttl_ms = 30000
recovery_ttl_ms = 120000

[[dns.upstreams]]
name = "dot"
transport = "tls"
bootstrap = "1.1.1.1:853"
server_name = "cloudflare-dns.com"

[[dns.plans]]
name = "secure"
upstreams = ["dot"]
security = "require-encrypted"

{}
"#,
        managed_tun_document(host)
    )
}

fn only_tun(node: &NodeConfig) -> &crate::ingress::tun::TunL4Config {
    let [
        LocalIngressConfig {
            config: IngressConfig::TunL4(tun),
            ..
        },
    ] = node.local_ingresses.as_slice()
    else {
        panic!("expected one TUN ingress");
    };
    tun
}

#[test]
fn toml_tun_host_defaults_to_external_manual_ownership() {
    let config = load_config_toml_str(&managed_tun_document("")).expect("external TUN config");
    let CommandConfig::Node(node) = config.command;
    let tun = only_tun(&node);

    assert!(matches!(
        tun.host,
        crate::ingress::tun::TunHostConfig::External
    ));
    assert_eq!(tun.compile_managed_vpn().expect("compile"), None);
}

#[test]
fn toml_managed_full_vpn_nests_optional_linux_policy_fields() {
    let config = load_config_toml_str(&managed_tun_document(
        r#"
[inbounds.host]
mode = "managed"
route_mode = "full"
exclude_cidrs = ["192.168.0.9/16"]
local_lan = true
dns_capture_servers = ["10.88.0.53", "fd00:88::53"]

[inbounds.host.linux]
route_table = 51821
native_rule_priority = 9997
capture_rule_priority = 9998
socket_mark = 1297106006
"#,
    ))
    .expect("managed full VPN config");
    let CommandConfig::Node(node) = config.command;
    let tun = only_tun(&node);
    let linux = tun
        .managed_vpn()
        .expect("managed policy")
        .platform
        .linux
        .expect("nested Linux tuning");
    assert_eq!(linux.route_table(), 51_821);
    assert_eq!(linux.native_rule_priority(), 9_997);
    assert_eq!(linux.capture_rule_priority(), 9_998);
    assert_eq!(linux.socket_mark().get(), 1_297_106_006);

    let portable = tun
        .compile_managed_vpn()
        .expect("compile")
        .expect("managed");

    assert_eq!(tun.interface_name.as_deref(), Some("daily0"));
    assert_eq!(portable.route_mode(), &crate::platform::RouteMode::Full);
    assert_eq!(
        portable.excludes(),
        &["192.168.0.0/16".parse().expect("canonical exclude")]
    );
    assert!(portable.local_lan());
    assert_eq!(
        portable.dns().expect("DNS capture").servers(),
        &[
            "10.88.0.53".parse::<std::net::IpAddr>().expect("IPv4 DNS"),
            "fd00:88::53".parse::<std::net::IpAddr>().expect("IPv6 DNS"),
        ]
    );
}

#[test]
fn toml_managed_split_vpn_requires_and_canonicalizes_includes() {
    let config = load_config_toml_str(&managed_tun_document(
        r#"
[inbounds.host]
mode = "managed"
route_mode = "split"
include_cidrs = ["10.1.2.3/8", "2001:db8:1::1/32"]
exclude_cidrs = ["10.9.8.7/16"]
"#,
    ))
    .expect("managed split VPN config");
    let CommandConfig::Node(node) = config.command;
    let tun = only_tun(&node);
    assert!(
        tun.managed_vpn()
            .expect("managed policy")
            .platform
            .linux
            .is_none(),
        "portable policy should not synthesize a Linux override"
    );
    let platform = tun
        .compile_managed_vpn()
        .expect("compile")
        .expect("managed");

    assert_eq!(
        platform.route_mode(),
        &crate::platform::RouteMode::Split(vec![
            "10.0.0.0/8".parse().expect("IPv4 include"),
            "2001:db8::/32".parse().expect("IPv6 include"),
        ])
    );
    assert_eq!(
        platform.excludes(),
        &["10.9.0.0/16".parse().expect("exclude")]
    );
    assert!(platform.dns().is_none());
}

#[test]
fn managed_vpn_must_capture_and_must_not_exclude_fake_dns_pools() {
    let valid = load_config_toml_str(&managed_fake_dns_document(
        r#"
[inbounds.host]
mode = "managed"
route_mode = "split"
include_cidrs = ["198.18.0.0/16"]
"#,
    ));
    assert!(
        valid.is_ok(),
        "captured FakeDNS pool should compile: {valid:?}"
    );

    let missing = load_config_toml_str(&managed_fake_dns_document(
        r#"
[inbounds.host]
mode = "managed"
route_mode = "split"
include_cidrs = ["10.0.0.0/8"]
"#,
    ))
    .expect_err("uncaptured FakeDNS pool must fail closed");
    assert!(matches!(
        missing,
        ConfigFileError::Config(ConfigError::DnsPolicy(message))
            if message.contains("does not capture FakeDNS pool")
    ));

    let excluded = load_config_toml_str(&managed_fake_dns_document(
        r#"
[inbounds.host]
mode = "managed"
route_mode = "split"
include_cidrs = ["198.18.0.0/16"]
exclude_cidrs = ["198.18.0.0/24"]
"#,
    ))
    .expect_err("excluded FakeDNS pool must fail closed");
    assert!(matches!(
        excluded,
        ConfigFileError::Config(ConfigError::DnsPolicy(message))
            if message.contains("overlaps a managed VPN exclude")
    ));
}

#[test]
fn toml_managed_vpn_rejects_unknown_fields_and_invalid_policy() {
    let unknown = toml::from_str::<TunHostFileConfig>(
        r#"
mode = "managed"
route_mode = "full"
dns_capture_servers = ["10.88.0.53"]
auto_route = true
"#,
    );
    assert!(unknown.is_err(), "unknown host field must be rejected");

    let zero_mark = toml::from_str::<TunHostFileConfig>(
        r#"
mode = "managed"
route_mode = "full"
dns_capture_servers = ["10.88.0.53"]

[linux]
socket_mark = 0
"#,
    )
    .expect("file shape");
    assert!(matches!(
        zero_mark.into_config(),
        Err(ConfigFileError::ManagedVpnValue(message)) if message.contains("nonzero")
    ));
}

#[test]
fn toml_managed_vpn_rejects_legacy_mode_and_flat_linux_tuning() {
    for unsupported_mode in ["managed-linux", "android"] {
        let document = format!(
            r#"
mode = "{unsupported_mode}"
route_mode = "full"
dns_capture_servers = ["10.88.0.53"]
"#
        );
        assert!(
            toml::from_str::<TunHostFileConfig>(&document).is_err(),
            "{unsupported_mode} must not alias process-managed host ownership"
        );
    }

    let flat_linux = toml::from_str::<TunHostFileConfig>(
        r#"
mode = "managed"
route_mode = "full"
dns_capture_servers = ["10.88.0.53"]
route_table = 51821
"#,
    );
    assert!(
        flat_linux.is_err(),
        "Linux tuning must remain nested below the portable policy"
    );
}

#[test]
fn toml_managed_full_vpn_rejects_split_only_includes() {
    let host = toml::from_str::<TunHostFileConfig>(
        r#"
mode = "managed"
route_mode = "full"
include_cidrs = ["10.0.0.0/8"]
dns_capture_servers = ["10.88.0.53"]
"#,
    )
    .expect("file shape");

    assert!(matches!(
        host.into_config(),
        Err(ConfigFileError::ManagedVpnValue(message))
            if message.contains("full VPN cannot set include_cidrs")
    ));
}

#[test]
fn dns_file_config_exposes_tagged_transport_bounds_and_ttl_caps() {
    let file: DnsFileConfig = toml::from_str(
        r#"
generation = 7
default_dns_plan = "default"

[[upstreams]]
name = "v4"
transport = "udp-tcp"
bootstrap = "1.1.1.1:53"

[[upstreams]]
name = "v6"
transport = "udp-tcp"
bootstrap = "[2606:4700:4700::1111]:53"

[[plans]]
name = "default"
upstreams = ["v4", "v6"]
ip_strategy = "ipv6-and-ipv4"
upstream_strategy = "race"
fallback_delay_ms = 50
expected_cidrs = ["192.0.2.0/24", "2001:db8::/32"]
lookup_timeout_ms = 1500
cache_capacity = 2048
max_inflight = 32
positive_ttl_cap_ms = 120000
negative_ttl_cap_ms = 15000
stale_if_error_ms = 45000
prefetch_max_ms = 12000

[[hosts]]
domain = "router.home.arpa"
addresses = ["192.0.2.1", "2001:db8::1"]
"#,
    )
    .expect("DNS file config");
    let outbounds = ParsedOutbounds {
        leaves: HashMap::new(),
        balancers: HashMap::new(),
        order: Vec::new(),
        balancer_order: Vec::new(),
    };
    let config = file.into_config(&outbounds).expect("DNS config");
    let compiled = config.compile().expect("compiled DNS");
    let default = crate::product::DnsPlanId::parse("default").expect("plan ID");
    let plan = compiled.plan(&default).expect("default plan");

    assert_eq!(config.generation, 7);
    assert_eq!(compiled.bootstrap_endpoints().count(), 2);
    assert_eq!(plan.ip_strategy(), DnsIpStrategy::Ipv6AndIpv4);
    assert_eq!(
        plan.upstream_strategy(),
        DnsUpstreamStrategy::Race {
            fallback_delay: Duration::from_millis(50)
        }
    );
    assert_eq!(
        plan.expected_cidrs(),
        &[
            "192.0.2.0/24".parse().expect("IPv4 CIDR"),
            "2001:db8::/32".parse().expect("IPv6 CIDR")
        ]
    );
    assert_eq!(plan.limits().lookup_timeout, Duration::from_millis(1_500));
    assert_eq!(plan.limits().cache_capacity, 2_048);
    assert_eq!(plan.limits().max_inflight, 32);
    assert_eq!(
        plan.limits().positive_ttl_cap,
        Duration::from_millis(120_000)
    );
    assert_eq!(
        plan.limits().negative_ttl_cap,
        Duration::from_millis(15_000)
    );
    assert_eq!(plan.limits().stale_if_error, Duration::from_millis(45_000));
    assert_eq!(plan.limits().prefetch_max, Duration::from_millis(12_000));
    assert_eq!(
        compiled
            .host(&DomainName::parse("router.home.arpa").expect("host"))
            .expect("compiled host")
            .as_ref(),
        &[
            "192.0.2.1".parse::<IpAddr>().expect("IPv4"),
            "2001:db8::1".parse::<IpAddr>().expect("IPv6")
        ]
    );
}

#[test]
fn dns_file_config_strictly_compiles_doq_and_fake_dns() {
    let file: DnsFileConfig = toml::from_str(
        r#"
default_dns_plan = "secure"

[fake_dns]
ipv4_pool = "198.18.0.0/16"
ipv6_pool = "fd00:4d50::/112"
max_entries = 4096
answer_ttl_ms = 30000
recovery_ttl_ms = 120000

[[upstreams]]
name = "doq"
transport = "quic"
bootstrap = "1.1.1.1:853"
server_name = "cloudflare-dns.com"

[[plans]]
name = "secure"
upstreams = ["doq"]
security = "require-encrypted"
"#,
    )
    .expect("strict DoQ/FakeDNS file");
    let config = file
        .into_config(&ParsedOutbounds {
            leaves: HashMap::new(),
            balancers: HashMap::new(),
            order: Vec::new(),
            balancer_order: Vec::new(),
        })
        .expect("DoQ/FakeDNS config");
    let compiled = config.compile().expect("compiled DoQ/FakeDNS");
    let doq = compiled
        .upstream(&crate::product::DnsUpstreamId::parse("doq").expect("upstream"))
        .expect("DoQ upstream");
    assert_eq!(
        doq.endpoint().transport(),
        crate::product::DnsTransport::Quic
    );
    let fake = compiled.fake_dns().expect("FakeDNS policy");
    assert_eq!(fake.max_entries, 4096);
    assert_eq!(fake.answer_ttl, Duration::from_secs(30));
    assert_eq!(fake.recovery_ttl, Duration::from_secs(120));

    let invalid_pool = toml::from_str::<DnsFileConfig>(
        r#"
default_dns_plan = "default"
[fake_dns]
ipv4_pool = "203.0.113.0/24"
max_entries = 32
answer_ttl_ms = 30000
recovery_ttl_ms = 120000
[[upstreams]]
name = "system"
transport = "system"
[[plans]]
name = "default"
upstreams = ["system"]
"#,
    )
    .expect("file shape")
    .into_config(&ParsedOutbounds {
        leaves: HashMap::new(),
        balancers: HashMap::new(),
        order: Vec::new(),
        balancer_order: Vec::new(),
    })
    .expect_err("public FakeDNS pool must fail closed");
    assert!(matches!(
        invalid_pool,
        ConfigFileError::DnsPolicy(message)
            if message.contains("198.18.0.0/15")
    ));

    let invalid_doq_path = toml::from_str::<DnsFileConfig>(
        r#"
default_dns_plan = "default"
[[upstreams]]
name = "doq"
transport = "quic"
bootstrap = "1.1.1.1:853"
server_name = "cloudflare-dns.com"
path = "/dns-query"
[[plans]]
name = "default"
upstreams = ["doq"]
"#,
    )
    .expect("file shape")
    .into_config(&ParsedOutbounds {
        leaves: HashMap::new(),
        balancers: HashMap::new(),
        order: Vec::new(),
        balancer_order: Vec::new(),
    })
    .expect_err("DoQ must reject HTTP path");
    assert!(matches!(
        invalid_doq_path,
        ConfigFileError::DnsValue(message) if message.contains("does not accept an HTTP path")
    ));
}

#[test]
fn routed_dot_accepts_only_a_literal_proxy_control_endpoint() {
    let literal = load_config_toml_str(
        r#"
[dns]
default_dns_plan = "secure"

[[dns.upstreams]]
name = "dot"
transport = "tls"
bootstrap = "1.1.1.1:853"
server_name = "cloudflare-dns.com"
outbound = "proxy"

[[dns.plans]]
name = "secure"
upstreams = ["dot"]
security = "require-encrypted"

[[inbounds]]
name = "local"
protocol = "socks5"

[[outbounds]]
name = "proxy"
protocol = "socks5"
endpoint = "127.0.0.1:1080"

[routing]
[[routing.rules]]
name = "default"
action = "outbound"
outbound = "proxy"
"#,
    );
    assert!(
        literal.is_ok(),
        "literal proxy should be DNS-independent: {literal:?}"
    );

    let named = load_config_toml_str(
        r#"
[dns]
default_dns_plan = "secure"

[[dns.upstreams]]
name = "dot"
transport = "tls"
bootstrap = "1.1.1.1:853"
server_name = "cloudflare-dns.com"
outbound = "proxy"

[[dns.plans]]
name = "secure"
upstreams = ["dot"]
security = "require-encrypted"

[[inbounds]]
name = "local"
protocol = "socks5"

[[outbounds]]
name = "proxy"
protocol = "socks5"
endpoint = "proxy.example:1080"

[routing]
[[routing.rules]]
name = "default"
action = "outbound"
outbound = "proxy"
"#,
    )
    .expect_err("a proxy hostname would recursively require DNS");
    assert!(matches!(
        named,
        ConfigFileError::DnsPolicy(message) if message.contains("DNS-dependent outbound proxy")
    ));
}

#[test]
fn routed_dns_capabilities_do_not_overstate_udp_support() {
    let error = load_config_toml_str(
        r#"
[dns]
default_dns_plan = "plain"

[[dns.upstreams]]
name = "udp"
transport = "udp"
bootstrap = "1.1.1.1:53"
outbound = "proxy"

[[dns.plans]]
name = "plain"
upstreams = ["udp"]

[[inbounds]]
name = "local"
protocol = "socks5"

[[outbounds]]
name = "proxy"
protocol = "socks5"
endpoint = "127.0.0.1:1080"

[routing]
[[routing.rules]]
name = "default"
action = "outbound"
outbound = "proxy"
"#,
    )
    .expect_err("UDP DNS has no routed datagram connector yet");
    assert!(matches!(
        error,
        ConfigFileError::DnsPolicy(message)
            if message.to_ascii_lowercase().contains("requires udp") && message.contains("proxy")
    ));
}

#[test]
fn routed_doh_accepts_a_literal_mpp_carrier_inventory() {
    let config = load_config_toml_str(
        r#"
[dns]
default_dns_plan = "secure"

[[dns.upstreams]]
name = "doh"
transport = "https"
bootstrap = "1.1.1.1:443"
server_name = "cloudflare-dns.com"
path = "/dns-query"
outbound = "edge"

[[dns.plans]]
name = "secure"
upstreams = ["doh"]
security = "require-encrypted"

[[inbounds]]
name = "local"
protocol = "socks5"

[[outbounds]]
name = "edge"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "udp://127.0.0.1:7443" }]

[outbounds.security]
credential_id = "test-default"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]
[[routing.rules]]
name = "default"
action = "outbound"
outbound = "edge"
"#,
    );
    assert!(
        config.is_ok(),
        "literal MPP carriers should be DNS-independent: {config:?}"
    );
}

#[test]
fn proxy_auth_file_debug_redacts_password() {
    let auth = OutboundProxyAuthFileConfig {
        username: Some("alice".to_string()),
        password: Some(SecretMaterialReference::File {
            path: "do-not-print-secret.key".into(),
        }),
    };

    let debug = format!("{auth:?}");
    assert!(debug.contains("alice"));
    assert!(!debug.contains("proxy-password"));
}

#[test]
fn named_local_users_resolve_secret_references_and_map_to_product_principals() {
    let config = load_config_toml_str(
        r#"
[[local_users]]
name = "phone-login"
principal_id = "family"
username = "mobile-user"
password = { from = "file", path = "proxy-password.key" }

[[inbounds]]
name = "local"
protocol = "socks5"
local_users = ["phone-login"]

[inbounds.admission]
max_connections = 40
max_connections_per_source = 20
max_connections_per_principal = 10
handshake_timeout_ms = 5000

[[outbounds]]
name = "edge"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "udp://127.0.0.1:7443" }]

[outbounds.security]
credential_id = "test-default"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]
[[routing.rules]]
name = "default"
action = "outbound"
outbound = "edge"
"#,
    )
    .expect("named local user config");

    let CommandConfig::Node(node) = config.command;
    let IngressConfig::Socks5 {
        proxy_auth,
        admission,
        ..
    } = &node.local_ingresses[0].config
    else {
        panic!("expected SOCKS5 inbound");
    };
    assert_eq!(proxy_auth.user_count(), 1);
    assert_eq!(
        proxy_auth
            .authenticate("mobile-user", "proxy-password")
            .expect("authenticated principal")
            .as_str(),
        "family"
    );
    assert!(proxy_auth.authenticate("mobile-user", "wrong").is_none());
    assert_eq!(admission.max_connections(), 40);
    assert_eq!(admission.max_connections_per_source(), 20);
    assert_eq!(admission.max_connections_per_principal(), 10);
    assert_eq!(admission.handshake_timeout(), Duration::from_secs(5));
    assert!(!format!("{proxy_auth:?}").contains("mobile-user"));
    assert!(!format!("{proxy_auth:?}").contains("proxy-password"));
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
fn resource_file_config_exposes_sparse_node_limits() {
    let limits = ResourceFileConfig {
        max_reinjection_cache_chunks: Some(101),
        max_reorder_buffer_chunks: Some(102),
        max_retained_receive_ranges: Some(103),
        ..ResourceFileConfig::default()
    }
    .into_limits();

    assert_eq!(limits.max_reinjection_cache_chunks, 101);
    assert_eq!(limits.max_reorder_buffer_chunks, 102);
    assert_eq!(limits.max_retained_receive_ranges, 103);
}

#[test]
fn tun_l3_toml_compiles_client_binding_and_server_address_plan() {
    let document = format!(
        "forwarding_mode = \"l3\"\n{TEST_CREDENTIAL_CATALOG}\n{}",
        r#"
[[inbounds]]
name = "packet-client"
protocol = "tun-l3"
outbound = "edge"
interface_name = "mptun-client"

[[inbounds]]
name = "packet-server"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "tcp://127.0.0.1:7443" }]
outbound = "direct"

[inbounds.security]
credential_ids = ["test-default"]
tls_certificate_chain_file = "mptunnel-test-certificate.pem"
tls_private_key_file = "mptunnel-test-private-key.pem"

[inbounds.tun_l3]
interface_name = "mptun-server"
ipv4_pool = "10.88.0.0/24"
ipv4 = "10.88.0.1"
mtu = 1400

[[inbounds.tun_l3.allocations]]
principal_id = "test-peer"
ipv4 = "10.88.0.2"
allowed_ips = ["192.168.50.0/24"]

[[outbounds]]
name = "edge"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "tcp://127.0.0.1:443" }]

[outbounds.security]
credential_id = "test-default"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[[outbounds]]
name = "direct"
protocol = "direct"
"#
    );
    let config = load_config_toml_str(&document).expect("TUN-L3 client and server configuration");

    let CommandConfig::Node(node) = config.command;
    assert_eq!(node.forwarding_mode, ForwardingMode::L3);
    let [client] = node.tun_l3_ingresses.as_slice() else {
        panic!("expected one TUN-L3 client ingress");
    };
    assert_eq!(client.name, "packet-client");
    assert_eq!(client.config.outbound.as_str(), "edge");
    assert_eq!(
        client.config.interface_name.as_deref(),
        Some("mptun-client")
    );

    let [server] = node.servers.as_slice() else {
        panic!("expected one MPP server inbound");
    };
    let plan = server.tun_l3.as_ref().expect("server TUN-L3 plan");
    assert_eq!(plan.interface_name(), Some("mptun-server"));
    assert_eq!(
        plan.ipv4_pool(),
        Some("10.88.0.0/24".parse().expect("pool"))
    );
    assert_eq!(plan.ipv4(), Some("10.88.0.1".parse().expect("address")));
    assert_eq!(plan.mtu(), 1400);
    let principal = PrincipalId::parse("test-peer").expect("principal");
    let allocation = plan.peer(&principal).expect("peer allocation");
    assert_eq!(
        allocation.ipv4(),
        Some("10.88.0.2".parse().expect("address"))
    );
    assert_eq!(
        allocation.allowed_ips(),
        &["192.168.50.0/24".parse().expect("allowed prefix")]
    );
}

#[test]
fn l4_forwarding_mode_rejects_client_and_server_tun_l3() {
    let managed_host = r#"
[inbounds.host]
mode = "managed"
route_mode = "full"
dns_capture_servers = ["10.88.0.53"]
"#;
    let client = format!(
        "{}\n{}",
        managed_tun_document(managed_host),
        r#"
[[inbounds]]
name = "packet-client"
protocol = "tun-l3"
outbound = "edge"
"#
    );
    assert!(matches!(
        load_config_toml_str(&client),
        Err(ConfigFileError::Config(ConfigError::L4ContainsTunL3))
    ));

    let server = format!(
        "{}\n{}",
        managed_tun_document(managed_host),
        r#"
[[inbounds]]
name = "packet-server"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "tcp://127.0.0.1:7443" }]
outbound = "direct"

[inbounds.security]
credential_ids = ["test-default"]
tls_certificate_chain_file = "mptunnel-test-certificate.pem"
tls_private_key_file = "mptunnel-test-private-key.pem"

[inbounds.tun_l3]
ipv4_pool = "10.89.0.0/24"
ipv4 = "10.89.0.1"

[[inbounds.tun_l3.allocations]]
principal_id = "test-peer"
ipv4 = "10.89.0.2"

[[outbounds]]
name = "direct"
protocol = "direct"
"#
    );
    assert!(matches!(
        load_config_toml_str(&server),
        Err(ConfigFileError::Config(ConfigError::L4ContainsTunL3))
    ));
}

#[test]
fn l3_forwarding_mode_rejects_l4_services() {
    let local = format!(
        "forwarding_mode = \"l3\"\n{TEST_CREDENTIAL_CATALOG}\n{}",
        managed_tun_document("")
    );
    assert!(matches!(
        load_config_toml_str(&local),
        Err(ConfigFileError::Config(ConfigError::L3ContainsL4Inbound(name)))
            if name == "local-tun"
    ));

    let server = format!(
        "forwarding_mode = \"l3\"\n{TEST_CREDENTIAL_CATALOG}\n{}",
        mpp_inbound_security_document("")
    );
    assert!(matches!(
        load_config_toml_str(&server),
        Err(ConfigFileError::Config(ConfigError::L3ServerMissingTunL3(name)))
            if name == "edge"
    ));
}

#[test]
fn tun_l3_rejects_conflicting_explicit_packet_device_names() {
    let document = format!(
        "forwarding_mode = \"l3\"\n{TEST_CREDENTIAL_CATALOG}\n{}",
        r#"
[[inbounds]]
name = "packet-client"
protocol = "tun-l3"
outbound = "edge"
interface_name = "mptun0"

[[inbounds]]
name = "packet-server"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "tcp://127.0.0.1:7443" }]
outbound = "direct"

[inbounds.security]
credential_ids = ["test-default"]
tls_certificate_chain_file = "mptunnel-test-certificate.pem"
tls_private_key_file = "mptunnel-test-private-key.pem"

[inbounds.tun_l3]
interface_name = "mptun0"
ipv4_pool = "10.88.0.0/24"
ipv4 = "10.88.0.1"

[[inbounds.tun_l3.allocations]]
principal_id = "test-peer"
ipv4 = "10.88.0.2"

[[outbounds]]
name = "edge"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "tcp://127.0.0.1:443" }]

[outbounds.security]
credential_id = "test-default"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[[outbounds]]
name = "direct"
protocol = "direct"
"#
    );

    assert!(matches!(
        load_config_toml_str(&document),
        Err(ConfigFileError::Config(
            ConfigError::DuplicatePacketDeviceName(name)
        )) if name == "mptun0"
    ));
}

#[test]
fn toml_separates_logical_session_retention_from_carrier_liveness() {
    let config = load_config_toml_str(
        r#"
[session]
retention_timeout_ms = 45000

[resources]
max_reinjection_cache_chunks = 101
max_reorder_buffer_chunks = 102
max_retained_receive_ranges = 103
tcp_path_heartbeat_interval_ms = 2000
tcp_path_heartbeat_timeout_ms = 7000
quic_path_keep_alive_interval_ms = 3000
quic_path_idle_timeout_ms = 12000

[[inbounds]]
name = "local-socks"
protocol = "socks5"

[[outbounds]]
name = "mpp"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "tcp://127.0.0.1:443" }, { name = "path-2", endpoint = "udp://127.0.0.1:443" }]

[outbounds.security]
credential_id = "test-default"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]

[[routing.rules]]
name = "default"
action = "outbound"
outbound = "mpp"
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
    assert_eq!(config.resources.max_reinjection_cache_chunks, 101);
    assert_eq!(config.resources.max_reorder_buffer_chunks, 102);
    assert_eq!(config.resources.max_retained_receive_ranges, 103);
}

#[test]
fn shipped_configuration_documents_match_the_runtime_schema() {
    let load = |contents: &str| {
        let contents = contents
            .replace("REPLACE_ME", "0123456789abcdef0123456789abcdef")
            .replace("server.example.com", "mptunnel.test")
            .replace("REPLACE_WITH_SERVER_CERT.pem", TEST_CERTIFICATE_FILE)
            .replace("server-cert.pem", TEST_CERTIFICATE_FILE)
            .replace("server-key.pem", TEST_PRIVATE_KEY_FILE)
            .replace("mpp-transport.key", TEST_TRANSPORT_SECRET_FILE);
        load_config_toml_str(&contents).expect("shipped configuration")
    };

    let reference = load(include_str!("../../examples/config.reference.toml"));
    assert_eq!(reference.session, SessionConfig::default());
    assert_eq!(reference.resources, ResourceLimits::default());
    let CommandConfig::Node(reference) = reference.command;
    assert_eq!(reference.forwarding_mode, ForwardingMode::L4);
    assert!(reference.servers.is_empty());
    assert_eq!(reference.local_ingresses.len(), 2);
    assert!(reference.tun_l3_ingresses.is_empty());
    assert_eq!(mpp_outbounds(&reference)[0].paths.len(), 2);
    assert!(
        mpp_outbounds(&reference)[0].paths[0]
            .tls
            .shared_transport_secret_configured()
    );

    let client = load(include_str!("../../examples/client.toml"));
    let CommandConfig::Node(client) = client.command;
    assert_eq!(client.forwarding_mode, ForwardingMode::L4);
    assert!(client.servers.is_empty());
    assert_eq!(client.local_ingresses.len(), 1);
    assert!(client.tun_l3_ingresses.is_empty());
    assert_eq!(mpp_outbounds(&client)[0].paths.len(), 2);
    assert!(
        mpp_outbounds(&client)[0].paths[0]
            .tls
            .shared_transport_secret_configured()
    );

    let server = load(include_str!("../../examples/server.toml"));
    let CommandConfig::Node(server) = server.command;
    assert_eq!(server.forwarding_mode, ForwardingMode::L4);
    assert!(server.local_ingresses.is_empty());
    assert!(server.tun_l3_ingresses.is_empty());
    assert_eq!(server.servers.len(), 1);
    assert!(server.servers[0].tun_l3.is_none());
    assert_eq!(server.servers[0].paths.len(), 2);
    assert!(server.servers[0].tls.shared_transport_secret_configured());
    assert!(mpp_outbounds(&server).is_empty());
}

#[test]
fn node_config_toml_uses_inbound_to_mpp_outbound_defaults_and_management() {
    let config = load_config_toml_str(
        r#"
[management]
listen = ["127.0.0.1:7600"]
token = { from = "file", path = "management-token.key" }
dashboard = true
allow_peer_diagnostics = true

[[inbounds]]
name = "local-socks"
protocol = "socks5"

[[outbounds]]
name = "mpp-main"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "tcp://127.0.0.1:443" }, { name = "path-2", endpoint = "udp://127.0.0.1:8443-8450" }]

[outbounds.performance]
extra_traffic_hint_percent = 25

[outbounds.security]
credential_id = "test-default"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]

[[routing.rules]]
name = "default"
action = "outbound"
outbound = "mpp-main"
"#,
    )
    .expect("config");

    assert_eq!(config.management.listen.len(), 1);
    assert_eq!(
        config.management.token.as_deref(),
        Some("operator-token-123")
    );
    assert!(config.management.dashboard);
    assert!(config.management.allow_peer_diagnostics);
    let CommandConfig::Node(node) = config.command;
    assert!(node.servers.is_empty());
    let clients = mpp_outbounds(&node);
    assert_eq!(clients.len(), 1);
    assert_eq!(clients[0].paths.len(), 2);
    assert_eq!(clients[0].paths[1].spec.endpoint.ports().first(), 8443);
    assert_eq!(clients[0].paths[1].spec.endpoint.ports().last(), 8450);
    assert_eq!(clients[0].performance.extra_traffic_hint_percent, 25);
    assert_eq!(node.local_ingresses[0].name, "local-socks");
    assert_eq!(
        ingress_configs(&node.local_ingresses),
        vec![IngressConfig::Socks5 {
            listen: vec!["127.0.0.1:1080".parse().expect("listen")],
            proxy_auth: ProxyAuthConfig::disabled(),
            admission: LocalIngressAdmissionConfig::default(),
        }]
    );
}

#[test]
fn mixed_inbound_uses_one_listener_and_shared_proxy_policy() {
    let config = load_config_toml_str(
        r#"
[[local_users]]
name = "phone-login"
principal_id = "family"
username = "mobile-user"
password = { from = "file", path = "proxy-password.key" }

[[inbounds]]
name = "local-mixed"
protocol = "mixed"
local_users = ["phone-login"]

[inbounds.admission]
max_connections = 40
max_connections_per_source = 20
max_connections_per_principal = 10
handshake_timeout_ms = 5000

[[outbounds]]
name = "direct"
protocol = "direct"

[routing]
[[routing.rules]]
name = "default"
action = "outbound"
outbound = "direct"
"#,
    )
    .expect("mixed inbound config");

    let CommandConfig::Node(node) = config.command;
    assert_eq!(node.local_ingresses[0].name, "local-mixed");
    let IngressConfig::Mixed {
        listen,
        proxy_auth,
        admission,
    } = &node.local_ingresses[0].config
    else {
        panic!("expected mixed inbound");
    };
    assert_eq!(
        listen,
        &["127.0.0.1:1080".parse().expect("default mixed listen")]
    );
    assert_eq!(
        proxy_auth
            .authenticate("mobile-user", "proxy-password")
            .expect("shared proxy authentication")
            .as_str(),
        "family"
    );
    assert_eq!(admission.max_connections(), 40);
    assert_eq!(admission.max_connections_per_source(), 20);
    assert_eq!(admission.max_connections_per_principal(), 10);
    assert_eq!(admission.handshake_timeout(), Duration::from_secs(5));
}

#[test]
fn node_config_toml_preserves_local_inbound_names() {
    let config = load_config_toml_str(
        r#"
[[inbounds]]
name = "local-socks"
protocol = "socks5"
listen = ["127.0.0.1:1080"]

[[inbounds]]
name = "local-http"
protocol = "http-connect"
listen = ["127.0.0.1:8080"]

[[outbounds]]
name = "mpp-main"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "tcp://127.0.0.1:443" }]

[outbounds.security]
credential_id = "test-default"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]

[[routing.rules]]
name = "default"
action = "outbound"
outbound = "mpp-main"
"#,
    )
    .expect("config");

    let CommandConfig::Node(node) = config.command;
    assert_eq!(mpp_outbounds(&node).len(), 1);
    let ingresses = &node.local_ingresses;
    assert_eq!(ingresses.len(), 2);
    assert_eq!(ingresses[0].name, "local-socks");
    assert_eq!(ingresses[1].name, "local-http");
    assert!(matches!(&ingresses[0].config, IngressConfig::Socks5 { .. }));
    assert!(matches!(
        &ingresses[1].config,
        IngressConfig::HttpConnect { .. }
    ));
}

#[test]
fn node_config_toml_builds_strict_fixed_target_tcp_and_udp_inbounds() {
    let config = load_config_toml_str(
        r#"
[[inbounds]]
name = "local-tcp-forward"
protocol = "tcp-forward"
listen = ["127.0.0.1:8443", "[::1]:8443"]
target = "BÜCHER.Example.:443"
max_connections = 32

[[inbounds]]
name = "local-udp-forward"
protocol = "udp-forward"
listen = ["127.0.0.1:5353"]
target = "[2001:db8::53]:53"
max_associations = 16
idle_timeout_ms = 5000
datagram_ttl_ms = 1500

[[outbounds]]
name = "mpp-main"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "tcp://127.0.0.1:443" }]

[outbounds.security]
credential_id = "test-default"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]

[[routing.rules]]
name = "default"
action = "outbound"
outbound = "mpp-main"
"#,
    )
    .expect("fixed-target inbound config");

    let CommandConfig::Node(node) = config.command;
    let [tcp, udp] = node.local_ingresses.as_slice() else {
        panic!("expected TCP and UDP fixed-target inbounds");
    };
    assert_eq!(tcp.name, "local-tcp-forward");
    let IngressConfig::TcpForward(tcp) = &tcp.config else {
        panic!("expected TCP fixed-target inbound");
    };
    assert_eq!(tcp.listen().len(), 2);
    assert_eq!(tcp.target().to_string(), "xn--bcher-kva.example:443");
    assert_eq!(tcp.max_connections(), 32);

    assert_eq!(udp.name, "local-udp-forward");
    let IngressConfig::UdpForward(udp) = &udp.config else {
        panic!("expected UDP fixed-target inbound");
    };
    assert_eq!(
        udp.listen(),
        &["127.0.0.1:5353".parse().expect("UDP listen")]
    );
    assert_eq!(udp.target().to_string(), "[2001:db8::53]:53");
    assert_eq!(udp.max_associations(), 16);
    assert_eq!(udp.idle_timeout(), Duration::from_secs(5));
    assert_eq!(udp.datagram_ttl_ms(), 1500);
}

#[test]
fn inbound_names_must_be_unique() {
    let err = load_config_toml_str(
        r#"
[[inbounds]]
name = "duplicate"
protocol = "socks5"

[[inbounds]]
name = "duplicate"
protocol = "http-connect"

[[outbounds]]
name = "mpp-main"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "tcp://127.0.0.1:443" }]

[outbounds.security]
credential_id = "test-default"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"
"#,
    )
    .expect_err("duplicate inbound name should fail");

    assert!(matches!(
        err,
        ConfigFileError::DuplicateInboundName(name) if name == "duplicate"
    ));
}

#[test]
fn node_config_toml_covers_forwarding_chaining_and_outbound_dns() {
    let config = load_config_toml_str(
        r#"
[dns]
generation = 3
default_dns_plan = "egress"

[[dns.upstreams]]
name = "v4"
transport = "udp-tcp"
bootstrap = "1.1.1.1:53"

[[dns.upstreams]]
name = "v6"
transport = "udp-tcp"
bootstrap = "[2606:4700:4700::1111]:53"

[[dns.plans]]
name = "egress"
upstreams = ["v4", "v6"]
ip_strategy = "ipv4-and-ipv6"
lookup_timeout_ms = 1500
cache_capacity = 2048
max_inflight = 32
positive_ttl_cap_ms = 120000
negative_ttl_cap_ms = 15000

[[inbounds]]
name = "local-http"
protocol = "http-connect"
listen = ["127.0.0.1:8081"]

[[inbounds]]
name = "edge-mpp"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "tcp://0.0.0.0:8443" }, { name = "path-2", endpoint = "udp://0.0.0.0:8443" }]
outbound = "proxy-egress"
dns_plan = "egress"

[inbounds.security]
credential_ids = ["test-default"]
tls_certificate_chain_file = "mptunnel-test-certificate.pem"
tls_private_key_file = "mptunnel-test-private-key.pem"

[inbounds.performance]
extra_traffic_hint_percent = 200

[[outbounds]]
name = "mpp-main"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "tcp://127.0.0.1:443" }]

[outbounds.security]
credential_id = "test-fed"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[[outbounds]]
name = "proxy-egress"
protocol = "socks5"
endpoint = "127.0.0.1:8080"

[routing]

[[routing.rules]]
name = "default"
action = "outbound"
outbound = "mpp-main"
"#,
    )
    .expect("config");

    let CommandConfig::Node(node) = config.command;
    assert_eq!(mpp_outbounds(&node).len(), 1);
    assert_eq!(node.servers.len(), 1);
    let server = &node.servers[0];
    assert_eq!(server.name, "edge-mpp");
    assert_eq!(server.performance.extra_traffic_hint_percent, 200);
    assert!(matches!(
        &server.egress,
        EgressRef::Outbound(outbound) if outbound.as_str() == "proxy-egress"
    ));
    assert_eq!(server.paths.len(), 2);
    assert_eq!(
        node.dns_policy
            .spec
            .outbound_capabilities
            .iter()
            .map(|capability| capability.outbound.as_str())
            .collect::<Vec<_>>(),
        ["mpp-main", "proxy-egress"],
        "compiled config identity must not depend on map iteration order"
    );
    assert_eq!(
        server
            .dns_plan
            .as_ref()
            .map(crate::product::DnsPlanId::as_str),
        Some("egress")
    );
    let dns = node.dns_policy.compile().expect("DNS policy");
    let plan = dns
        .plan(server.dns_plan.as_ref().expect("DNS plan"))
        .expect("plan");
    assert_eq!(plan.ip_strategy(), DnsIpStrategy::Ipv4AndIpv6);
    assert_eq!(plan.limits().lookup_timeout, Duration::from_millis(1500));
    assert_eq!(plan.limits().cache_capacity, 2048);
    assert_eq!(plan.limits().max_inflight, 32);
    assert_eq!(
        plan.limits().positive_ttl_cap,
        Duration::from_millis(120_000)
    );
    assert_eq!(
        plan.limits().negative_ttl_cap,
        Duration::from_millis(15_000)
    );
    assert!(matches!(
        local_outbound(&node, "proxy-egress"),
        OutboundConfig::Socks5(_)
    ));
}

#[test]
fn https_connect_outbound_parses_tls_identity_roots_and_redacted_auth() {
    let config = load_config_toml_str(
        r#"
[[inbounds]]
name = "local-http"
protocol = "http-connect"

[[outbounds]]
name = "secure-proxy"
protocol = "https-connect"
endpoint = "127.0.0.1:4443"
tls_server_name = "mptunnel.test"
tls_ca_certificate_file = "mptunnel-test-certificate.pem"

[outbounds.auth]
username = "alice"
password = { from = "file", path = "proxy-password.key" }

[routing]

[[routing.rules]]
name = "default"
action = "outbound"
outbound = "secure-proxy"
"#,
    )
    .expect("config");

    let CommandConfig::Node(node) = config.command;
    let OutboundConfig::HttpsConnect(proxy) = local_outbound(&node, "secure-proxy") else {
        panic!("expected HTTPS CONNECT outbound");
    };
    assert_eq!(proxy.tls_server_name(), "mptunnel.test");
    assert_eq!(
        proxy.proxy().credentials().expect("credentials").username(),
        "alice"
    );
    assert!(!format!("{proxy:?}").contains("proxy-password"));
}

#[test]
fn outbound_proxy_auth_requires_a_complete_credential_pair() {
    let err = load_config_toml_str(
        r#"
[[inbounds]]
name = "edge"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "tcp://127.0.0.1:8443" }]
outbound = "proxy"

[inbounds.security]
credential_ids = ["test-default"]
tls_certificate_chain_file = "mptunnel-test-certificate.pem"
tls_private_key_file = "mptunnel-test-private-key.pem"

[[outbounds]]
name = "proxy"
protocol = "socks5"
endpoint = "127.0.0.1:1080"

[outbounds.auth]
username = "alice"
"#,
    )
    .expect_err("incomplete outbound credentials");

    assert!(matches!(err, ConfigFileError::ProxyPasswordRequired));
    assert!(!err.to_string().contains("alice"));
}

#[test]
fn routing_builds_independent_mpp_gateway_leaves_and_leaf_egress() {
    let config = load_config_toml_str(
        r#"
[[inbounds]]
name = "local-socks"
protocol = "socks5"

[[inbounds]]
name = "edge"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "tcp://0.0.0.0:8443" }]
outbound = "direct-a"

[inbounds.security]
credential_ids = ["test-a"]
tls_certificate_chain_file = "mptunnel-test-certificate.pem"
tls_private_key_file = "mptunnel-test-private-key.pem"

[[outbounds]]
name = "mpp-a"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "tcp://127.0.0.1:443" }]

[outbounds.security]
credential_id = "test-b"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[[outbounds]]
name = "mpp-b"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "udp://127.0.0.1:443" }]

[outbounds.security]
credential_id = "test-c"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[[outbounds]]
name = "direct-a"
protocol = "direct"

[routing]

[[routing.balancers]]
name = "edge-gateway"
strategy = "round-robin"
members = [{ outbound = "mpp-a" }, { outbound = "mpp-b" }]

[[routing.rules]]
name = "default"
action = "balancer"
balancer = "edge-gateway"
"#,
    )
    .expect("config");

    let CommandConfig::Node(node) = config.command;
    let clients = mpp_outbounds(&node);
    assert_eq!(clients.len(), 2);
    assert!(
        clients.iter().all(|client| client.paths.len() == 1),
        "a gateway must never concatenate independent MPP path groups"
    );
    assert_ne!(
        clients[0].paths[0].security.credential.secret().as_bytes(),
        clients[1].paths[0].security.credential.secret().as_bytes()
    );
    assert_eq!(node.gateway_balancers.len(), 1);
    assert_eq!(node.gateway_balancers[0].id.as_str(), "edge-gateway");
    assert_eq!(node.gateway_balancers[0].spec.members.len(), 2);
    assert_eq!(node.servers.len(), 1);
    assert!(matches!(
        &node.servers[0].egress,
        EgressRef::Outbound(outbound) if outbound.as_str() == "direct-a"
    ));
    assert!(matches!(
        local_outbound(&node, "direct-a"),
        OutboundConfig::Direct
    ));
}

#[test]
fn routing_gateway_schema_compiles_manual_random_affinity_probe_and_member_modes() {
    let config = load_config_toml_str(
        r#"
[[inbounds]]
name = "local-socks"
protocol = "socks5"

[[outbounds]]
name = "edge-a"
protocol = "direct"

[[outbounds]]
name = "edge-b"
protocol = "direct"

[routing]
generation = 9

[[routing.balancers]]
name = "manual-edge"
strategy = "manual"
manual_outbound = "edge-a"
freshness_ttl_ms = 30000
members = [
  { outbound = "edge-a", weight = 3 },
  { outbound = "edge-b", mode = "draining" },
]
stickiness = { key = "principal", ttl_ms = 60000, capacity = 1024 }
probe = { target = "192.0.2.1:443", interval_ms = 10000, timeout_ms = 2000 }

[[routing.balancers]]
name = "random-edge"
strategy = "random"
members = [
  { outbound = "edge-a" },
  { outbound = "edge-b" },
]

[[routing.rules]]
name = "default"
action = "balancer"
balancer = "manual-edge"
"#,
    )
    .expect("mature gateway config");

    let CommandConfig::Node(node) = config.command;
    assert_eq!(node.gateway_balancers.len(), 2);
    let manual = &node.gateway_balancers[0].spec;
    assert_eq!(manual.strategy, GatewayStrategy::Manual);
    assert_eq!(
        manual.manual_member.as_ref().map(OutboundId::as_str),
        Some("edge-a")
    );
    assert_eq!(manual.stickiness_key, GatewayStickinessKey::Principal);
    assert_eq!(manual.members[0].weight, 3);
    assert_eq!(manual.members[1].mode, GatewayMemberMode::Draining);
    assert_eq!(
        manual.probe.as_ref().map(|probe| probe.target.authority()),
        Some("192.0.2.1:443".to_string())
    );
    assert_eq!(
        node.gateway_balancers[1].spec.strategy,
        GatewayStrategy::Random
    );
}

#[test]
fn legacy_combined_mpp_strategy_is_rejected() {
    let err = load_config_toml_str(
        r#"
[[inbounds]]
name = "local-socks"
protocol = "socks5"

[[outbounds]]
name = "mpp-a"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "tcp://127.0.0.1:443" }]

[outbounds.security]
credential_id = "test-b"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[[outbounds]]
name = "mpp-b"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "udp://127.0.0.1:443" }]

[outbounds.security]
credential_id = "test-c"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]

[[routing.balancers]]
name = "combined-edge"
strategy = "combined-mpp"
members = [{ outbound = "mpp-a" }, { outbound = "mpp-b" }]
"#,
    )
    .expect_err("legacy combined MPP must not deserialize");

    assert!(matches!(err, ConfigFileError::Toml(_)));
}

#[test]
fn legacy_http_protocol_alias_is_rejected() {
    let err = load_config_toml_str(
        r#"
[[inbounds]]
name = "local-http"
protocol = "http"
"#,
    )
    .expect_err("legacy HTTP alias must not deserialize");

    assert!(matches!(err, ConfigFileError::Toml(_)));
}

#[test]
fn legacy_sequence_balancer_schema_is_rejected() {
    let err = load_config_toml_str(
        r#"
[[inbounds]]
name = "edge"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "tcp://0.0.0.0:8443" }]
balancer = "outer"

[inbounds.security]
credential_ids = ["test-a"]
tls_certificate_chain_file = "mptunnel-test-certificate.pem"
tls_private_key_file = "mptunnel-test-private-key.pem"

[[outbounds]]
name = "direct-a"
protocol = "direct"

[[outbounds]]
name = "direct-b"
protocol = "direct"

[routing]

[[routing.balancers]]
name = "inner"
strategy = "sequence"
members = [{ outbound = "direct-a" }]

[[routing.balancers]]
name = "outer"
strategy = "sequence"
members = [{ outbound = "inner" }, { outbound = "direct-b" }]
"#,
    )
    .expect_err("legacy sequence balancers are invalid");

    assert!(matches!(err, ConfigFileError::Toml(_)));
}

#[test]
fn routing_rules_compile_every_match_category_and_select_named_mpp_targets() {
    let config = load_config_toml_str(
        r#"
[[inbounds]]
name = "local-socks"
protocol = "socks5"

[[inbounds]]
name = "local-http"
protocol = "http-connect"

[[outbounds]]
name = "edge-a"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "udp://127.0.0.1:7443" }]
[outbounds.security]
credential_id = "test-a"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[[outbounds]]
name = "edge-b"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "udp://127.0.0.1:8443" }]
[outbounds.security]
credential_id = "test-b"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]
generation = 42

[[routing.balancers]]
name = "all-edges"
strategy = "round-robin"
members = [{ outbound = "edge-a" }, { outbound = "edge-b" }]

[[routing.rules]]
name = "interactive-api"
domain_exact = ["api.example.com"]
domain_suffix = ["example.com"]
domain_keyword = ["api"]
domain_regex = ["^api\\.example\\.com$"]
destination_cidrs = []
source_cidrs = ["198.51.100.0/24"]
destination_ports = [443, "8443-8444"]
source_ports = ["40000-50000"]
networks = ["tcp"]
inbounds = ["local-socks"]
principal_ids = ["alice"]
stages = ["pre-resolution"]
action = "outbound"
outbound = "edge-a"
initial_demand = "throughput"
explanation = "API traffic uses edge A"

[[routing.rules]]
name = "default"
action = "balancer"
balancer = "all-edges"
initial_demand = "automatic"
"#,
    )
    .expect("compiled routing config");

    let CommandConfig::Node(node) = config.command;
    let policy = node
        .product_policy
        .expect("local Product policy")
        .compile()
        .expect("compiled generation");
    assert_eq!(policy.generation(), 42);
    let flow = FlowContext::new(
        Network::Tcp,
        ProtocolTarget::from_host_port("API.EXAMPLE.COM", 443).expect("target"),
        SourceEndpoint::new("198.51.100.7".parse().expect("source"), 45_000),
        PrincipalId::parse("alice").expect("principal"),
        InboundId::parse("local-socks").expect("inbound"),
    );
    let decision = policy.routes().classify(RouteInput::pre_resolution(&flow));
    assert_eq!(decision.rule_id().as_str(), "interactive-api");
    assert_eq!(
        decision.action().initial_demand(),
        InitialDemand::Throughput
    );
    assert!(matches!(
        decision.action().egress(),
        EgressAction::Outbound(outbound) if outbound.as_str() == "edge-a"
    ));

    let default_flow = FlowContext::new(
        Network::Tcp,
        ProtocolTarget::from_host_port("other.test", 80).expect("target"),
        SourceEndpoint::new("203.0.113.7".parse().expect("source"), 39_000),
        PrincipalId::parse("anonymous").expect("principal"),
        InboundId::parse("local-http").expect("inbound"),
    );
    let decision = policy
        .routes()
        .classify(RouteInput::pre_resolution(&default_flow));
    assert!(matches!(
        decision.action().egress(),
        EgressAction::Balancer(balancer) if balancer.as_str() == "all-edges"
    ));
}

#[test]
fn signed_rule_sets_are_pinned_compiled_and_visible_in_route_explain() {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use serde_json::json;
    use sha2::{Digest, Sha256};

    let directory = TestTlsDirectory::new();
    let payload = serde_json::to_vec(&json!({
        "schema": 1,
        "id": "geo-public",
        "revision": 42,
        "expires_at_unix_secs": null,
        "domain_exact": ["API.Example"],
        "domain_suffix": ["media.example"],
        "destination_cidrs": ["203.0.113.99/24", "2001:db8::/32"]
    }))
    .expect("rule-set payload");
    let checksum: [u8; 32] = Sha256::digest(&payload).into();
    let key = Ed25519KeyPair::from_seed_unchecked(&[11_u8; 32]).expect("test signing key");
    let mut signed_message = crate::product::RULE_SET_SIGNATURE_CONTEXT.to_vec();
    signed_message.extend_from_slice(&checksum);
    let signature = key.sign(&signed_message);
    let checksum_hex = checksum
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let envelope = serde_json::to_vec(&json!({
        "schema": 1,
        "publisher": "official",
        "checksum_sha256": checksum_hex,
        "payload_base64": BASE64.encode(payload),
        "signature_base64": BASE64.encode(signature.as_ref()),
    }))
    .expect("rule-set envelope");
    std::fs::write(directory.path.join("geo-public.ruleset.json"), envelope)
        .expect("write signed rule set");
    let public_key = BASE64.encode(key.public_key().as_ref());

    let config_document = |minimum_revision: u64| {
        format!(
            r#"
{TEST_CREDENTIAL_CATALOG}

[[inbounds]]
name = "local-socks"
protocol = "socks5"

[[outbounds]]
name = "edge"
protocol = "mpp"
paths = [{{ name = "path-1", endpoint = "udp://127.0.0.1:7443" }}]
[outbounds.security]
credential_id = "test-a"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]

[[routing.rule_set_publishers]]
publisher_id = "official"
ed25519_public_key_base64 = "{public_key}"

[[routing.rule_sets]]
rule_set_id = "geo-public"
publisher_id = "official"
minimum_revision = {minimum_revision}
file = "geo-public.ruleset.json"

[[routing.rules]]
name = "signed-domain"
domain_rule_set_ids = ["geo-public"]
action = "outbound"
outbound = "edge"

[[routing.rules]]
name = "signed-network"
destination_rule_set_ids = ["geo-public"]
stages = ["post-resolution"]
action = "reject"

[[routing.rules]]
name = "default"
action = "outbound"
outbound = "edge"
"#
        )
    };
    let config = super::load_config_toml_str_at(&config_document(42), &directory.path)
        .expect("verified signed routing config");
    let CommandConfig::Node(node) = config.command;
    let policy = node
        .product_policy
        .expect("Product policy")
        .compile()
        .expect("compiled Product generation");

    let domain_flow = FlowContext::new(
        Network::Tcp,
        ProtocolTarget::from_host_port("cdn.media.example", 443).expect("domain target"),
        SourceEndpoint::new("198.51.100.7".parse().expect("source"), 45_000),
        PrincipalId::parse("anonymous").expect("principal"),
        InboundId::parse("local-socks").expect("inbound"),
    );
    assert_eq!(
        policy
            .routes()
            .classify(RouteInput::pre_resolution(&domain_flow))
            .rule_id()
            .as_str(),
        "signed-domain"
    );

    let ip_flow = FlowContext::new(
        Network::Tcp,
        ProtocolTarget::from_host_port("other.example", 443).expect("domain target"),
        SourceEndpoint::new("198.51.100.7".parse().expect("source"), 45_000),
        PrincipalId::parse("anonymous").expect("principal"),
        InboundId::parse("local-socks").expect("inbound"),
    );
    assert_eq!(
        policy
            .routes()
            .classify(RouteInput::post_resolution(
                &ip_flow,
                "203.0.113.7".parse().expect("resolved address"),
            ))
            .rule_id()
            .as_str(),
        "signed-network"
    );
    let explanation = policy
        .routes()
        .explain(RouteInput::pre_resolution(&ip_flow));
    let signed_domain = explanation
        .rules()
        .iter()
        .find(|trace| trace.rule_id().as_str() == "signed-domain")
        .expect("signed-domain trace");
    assert_eq!(
        signed_domain.first_mismatch(),
        Some(RouteMismatch::DomainRuleSet)
    );
    let referenced = signed_domain.domain_rule_sets();
    assert_eq!(referenced.len(), 1);
    assert_eq!(referenced[0].id().as_str(), "geo-public");
    assert_eq!(referenced[0].publisher().as_str(), "official");
    assert_eq!(referenced[0].revision(), 42);

    let rollback = super::load_config_toml_str_at(&config_document(43), &directory.path)
        .expect_err("minimum revision rejects rollback");
    assert!(
        rollback
            .to_string()
            .contains("revision 42 is below configured minimum 43")
    );
}

#[test]
fn routing_rules_require_a_final_default_and_existing_typed_target() {
    let missing_default = load_config_toml_str(
        r#"
[[inbounds]]
name = "local-socks"
protocol = "socks5"

[[outbounds]]
name = "edge"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "udp://127.0.0.1:7443" }]
[outbounds.security]
credential_id = "test-a"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]
[[routing.rules]]
name = "only-specific"
domain_suffix = ["example.com"]
action = "outbound"
outbound = "edge"
"#,
    )
    .expect_err("final default is mandatory");
    assert!(matches!(
        missing_default,
        ConfigFileError::RoutingPolicy(message) if message.contains("final catch-all")
    ));

    let missing_target = load_config_toml_str(
        r#"
[[inbounds]]
name = "local-socks"
protocol = "socks5"

[[outbounds]]
name = "edge"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "udp://127.0.0.1:7443" }]
[outbounds.security]
credential_id = "test-a"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]
[[routing.rules]]
name = "default"
action = "outbound"
outbound = "missing"
"#,
    )
    .expect_err("referenced outbound must exist");
    assert!(matches!(
        missing_target,
        ConfigFileError::RoutingRuleMissingOutbound { outbound, .. } if outbound == "missing"
    ));
}

#[test]
fn routing_schema_rejects_unsupported_runtime_metadata_selectors() {
    for field in [
        "interfaces",
        "process_names",
        "process_paths",
        "process_packages",
        "tls_server_names",
        "http_hosts",
        "quic_server_names",
    ] {
        let document = format!(
            r#"
[[inbounds]]
name = "local-socks"
protocol = "socks5"

[[outbounds]]
name = "direct"
protocol = "direct"

[routing]
[[routing.rules]]
name = "default"
{field} = []
action = "outbound"
outbound = "direct"
"#
        );
        let error = load_config_toml_str(&document)
            .expect_err("unsupported route selector must be rejected by strict TOML parsing");
        let rendered = error.to_string();
        assert!(
            rendered.contains("unknown field") && rendered.contains(field),
            "unexpected error for {field}: {rendered}"
        );
    }
}

#[test]
fn routing_terminal_actions_are_strict_targetless_and_compile() {
    let config = load_config_toml_str(
        r#"
[[inbounds]]
name = "local-socks"
protocol = "socks5"

[[outbounds]]
name = "edge"
protocol = "mpp"
paths = [{ name = "path-1", endpoint = "udp://127.0.0.1:7443" }]
[outbounds.security]
credential_id = "test-a"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]
[[routing.rules]]
name = "default-deny"
action = "reject"
initial_demand = "automatic"
"#,
    )
    .expect("reject policy");
    let CommandConfig::Node(node) = config.command;
    let policy = node
        .product_policy
        .expect("policy")
        .compile()
        .expect("policy");
    let flow = FlowContext::new(
        Network::Tcp,
        ProtocolTarget::from_host_port("example.com", 443).expect("target"),
        SourceEndpoint::new("198.51.100.1".parse().expect("source"), 41_000),
        PrincipalId::parse("anonymous").expect("principal"),
        InboundId::parse("local-socks").expect("inbound"),
    );
    let decision = policy.routes().classify(RouteInput::pre_resolution(&flow));
    assert_eq!(decision.action().egress(), &EgressAction::Reject);
    assert_eq!(decision.action().initial_demand(), InitialDemand::Automatic);

    let unsupported_reset = load_config_toml_str(
        r#"
[[inbounds]]
name = "local-socks"
protocol = "socks5"

[routing]
[[routing.rules]]
name = "default-deny"
action = "reset"
"#,
    )
    .expect_err("generic reset action must not be accepted");
    let diagnostic = unsupported_reset.to_string();
    assert!(
        diagnostic.contains("configuration document is invalid")
            && diagnostic.contains("line")
            && diagnostic.contains("column"),
        "unexpected reset-action error: {unsupported_reset}"
    );
    assert!(!diagnostic.contains("reset"));
}

#[test]
fn product_admission_is_strict_finite_and_independent_from_core_resources() {
    let document = |admission: &str| {
        format!(
            r#"
{admission}

[[inbounds]]
name = "local-socks"
protocol = "socks5"

[routing]
[[routing.rules]]
name = "default-deny"
action = "reject"
"#
        )
    };
    let config = load_config_toml_str(&document(
        r#"
[admission]
max_live_flows = 2000
max_concurrent_work = 300
max_live_flows_per_principal = 800
max_live_flows_per_outbound = 1200
max_connects_per_outbound = 200
max_live_flows_per_target = 120
max_connects_per_target = 24
max_dns_work = 80
"#,
    ))
    .expect("explicit Product admission");
    assert_eq!(
        config.admission,
        ProductAdmissionConfig {
            max_live_flows: 2_000,
            max_concurrent_work: 300,
            max_live_flows_per_principal: 800,
            max_live_flows_per_outbound: 1_200,
            max_connects_per_outbound: 200,
            max_live_flows_per_target: 120,
            max_connects_per_target: 24,
            max_dns_work: 80,
        }
    );
    assert_eq!(
        config.resources,
        ResourceLimits::default(),
        "Product admission does not rewrite Core resource limits"
    );

    for invalid in [
        "[admission]\nmax_live_flows = 0",
        "[admission]\nmax_live_flows = 4\nmax_live_flows_per_principal = 5",
        "[admission]\nmax_concurrent_work = 4\nmax_dns_work = 5",
        "[admission]\nmax_live_flows_per_target = 2\nmax_connects_per_target = 3",
    ] {
        assert!(
            matches!(
                load_config_toml_str(&document(invalid)),
                Err(ConfigFileError::Config(ConfigError::ProductAdmission(_)))
            ),
            "invalid Product admission document unexpectedly passed: {invalid}"
        );
    }

    let unknown = document("[admission]\nunbounded_flows = true");
    assert!(matches!(
        load_config_toml_str(&unknown),
        Err(ConfigFileError::Toml(_))
    ));
}
