use super::*;
use crate::config::CommandConfig;
use crate::product::{
    DomainName, EgressAction, FlowContext, InboundId, Network, PrincipalId, ProtocolTarget,
    RouteInput, RouteMismatch, SourceEndpoint, TrafficIntent,
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
const TEST_CREDENTIAL_CATALOG: &str = r#"
[[credentials]]
id = "test-default"
principal = "test-peer"
secret = { from = "file", path = "mptunnel-test-credential.key" }

[[credentials]]
id = "test-a"
principal = "test-peer"
secret = { from = "file", path = "mptunnel-test-credential-a.key" }

[[credentials]]
id = "test-b"
principal = "test-peer"
secret = { from = "file", path = "mptunnel-test-credential-b.key" }

[[credentials]]
id = "test-c"
principal = "test-peer"
secret = { from = "file", path = "mptunnel-test-credential-c.key" }

[[credentials]]
id = "test-fed"
principal = "test-peer"
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
fn log_level_vocabulary_is_exact_and_case_sensitive() {
    for level in ["off", "error", "warn", "info", "debug", "trace"] {
        let document = format!(
            "log_level = {level:?}\n{TEST_CREDENTIAL_CATALOG}\n{}",
            managed_tun_document("")
        );
        assert_eq!(
            load_config_toml_str(&document)
                .expect("supported log level")
                .log_level,
            level
        );
    }

    for level in ["warning", "INFO", "verbose", ""] {
        let document = format!(
            "log_level = {level:?}\n{TEST_CREDENTIAL_CATALOG}\n{}",
            managed_tun_document("")
        );
        assert!(matches!(
            load_config_toml_str(&document),
            Err(ConfigFileError::Config(ConfigError::InvalidLogLevel(actual)))
                if actual == level
        ));
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
tag = "local-tun"
protocol = "tun"
name = "daily0"
ipv4 = "10.88.0.1"
ipv4_prefix = 24
ipv6 = "fd00:88::1"
ipv6_prefix = 64

{host}

[[outbounds]]
tag = "edge"
protocol = "mpp"
endpoints = ["udp://127.0.0.1:443"]

[outbounds.security]
credential = "test-default"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]

[[routing.rules]]
id = "default"
action = "outbound"
target = "edge"
"#
    )
}

fn managed_fake_dns_document(host: &str) -> String {
    format!(
        r#"
[dns]
default_plan = "secure"
system_fallback = false

[dns.fake_dns]
ipv4_pool = "198.18.0.0/16"
max_entries = 4096
answer_ttl_ms = 30000
recovery_ttl_ms = 120000

[[dns.upstreams]]
id = "dot"
transport = "tls"
bootstrap = "1.1.1.1:853"
server_name = "cloudflare-dns.com"

[[dns.plans]]
id = "secure"
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

    assert_eq!(tun.name.as_deref(), Some("daily0"));
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
default_plan = "default"

[[upstreams]]
id = "v4"
transport = "udp-tcp"
bootstrap = "1.1.1.1:53"

[[upstreams]]
id = "v6"
transport = "udp-tcp"
bootstrap = "[2606:4700:4700::1111]:53"

[[plans]]
id = "default"
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
name = "router.home.arpa"
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
default_plan = "secure"
system_fallback = false

[fake_dns]
ipv4_pool = "198.18.0.0/16"
ipv6_pool = "fd00:4d50::/112"
max_entries = 4096
answer_ttl_ms = 30000
recovery_ttl_ms = 120000

[[upstreams]]
id = "doq"
transport = "quic"
bootstrap = "1.1.1.1:853"
server_name = "cloudflare-dns.com"

[[plans]]
id = "secure"
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
default_plan = "default"
[fake_dns]
ipv4_pool = "203.0.113.0/24"
max_entries = 32
answer_ttl_ms = 30000
recovery_ttl_ms = 120000
[[upstreams]]
id = "system"
transport = "system"
[[plans]]
id = "default"
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
default_plan = "default"
[[upstreams]]
id = "doq"
transport = "quic"
bootstrap = "1.1.1.1:853"
server_name = "cloudflare-dns.com"
path = "/dns-query"
[[plans]]
id = "default"
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

    let fallback = toml::from_str::<DnsFileConfig>(
        r#"
default_plan = "default"
system_fallback = true
[[upstreams]]
id = "system"
transport = "system"
[[plans]]
id = "default"
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
    .expect_err("implicit system fallback is forbidden");
    assert!(matches!(
        fallback,
        ConfigFileError::DnsValue(message) if message.contains("system_fallback must be false")
    ));
}

#[test]
fn routed_dot_accepts_only_a_literal_proxy_control_endpoint() {
    let literal = load_config_toml_str(
        r#"
[dns]
default_plan = "secure"

[[dns.upstreams]]
id = "dot"
transport = "tls"
bootstrap = "1.1.1.1:853"
server_name = "cloudflare-dns.com"
egress_outbound = "proxy"

[[dns.plans]]
id = "secure"
upstreams = ["dot"]
security = "require-encrypted"

[[inbounds]]
tag = "local"
protocol = "socks5"

[[outbounds]]
tag = "proxy"
protocol = "socks5"
proxy = "127.0.0.1:1080"

[routing]
[[routing.rules]]
id = "default"
action = "outbound"
target = "proxy"
"#,
    );
    assert!(
        literal.is_ok(),
        "literal proxy should be DNS-independent: {literal:?}"
    );

    let named = load_config_toml_str(
        r#"
[dns]
default_plan = "secure"

[[dns.upstreams]]
id = "dot"
transport = "tls"
bootstrap = "1.1.1.1:853"
server_name = "cloudflare-dns.com"
egress_outbound = "proxy"

[[dns.plans]]
id = "secure"
upstreams = ["dot"]
security = "require-encrypted"

[[inbounds]]
tag = "local"
protocol = "socks5"

[[outbounds]]
tag = "proxy"
protocol = "socks5"
proxy = "proxy.example:1080"

[routing]
[[routing.rules]]
id = "default"
action = "outbound"
target = "proxy"
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
default_plan = "plain"

[[dns.upstreams]]
id = "udp"
transport = "udp"
bootstrap = "1.1.1.1:53"
egress_outbound = "proxy"

[[dns.plans]]
id = "plain"
upstreams = ["udp"]

[[inbounds]]
tag = "local"
protocol = "socks5"

[[outbounds]]
tag = "proxy"
protocol = "socks5"
proxy = "127.0.0.1:1080"

[routing]
[[routing.rules]]
id = "default"
action = "outbound"
target = "proxy"
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
default_plan = "secure"

[[dns.upstreams]]
id = "doh"
transport = "https"
bootstrap = "1.1.1.1:443"
server_name = "cloudflare-dns.com"
path = "/dns-query"
egress_outbound = "edge"

[[dns.plans]]
id = "secure"
upstreams = ["doh"]
security = "require-encrypted"

[[inbounds]]
tag = "local"
protocol = "socks5"

[[outbounds]]
tag = "edge"
protocol = "mpp"
endpoints = ["udp://127.0.0.1:7443"]

[outbounds.security]
credential = "test-default"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]
[[routing.rules]]
id = "default"
action = "outbound"
target = "edge"
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
id = "phone-login"
principal = "family"
username = "mobile-user"
password = { from = "file", path = "proxy-password.key" }

[[inbounds]]
tag = "local"
protocol = "socks5"
users = ["phone-login"]

[inbounds.admission]
max_connections = 40
max_connections_per_source = 20
max_connections_per_principal = 10
handshake_timeout_ms = 5000

[[outbounds]]
tag = "edge"
protocol = "mpp"
endpoints = ["udp://127.0.0.1:7443"]

[outbounds.security]
credential = "test-default"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]
[[routing.rules]]
id = "default"
action = "outbound"
target = "edge"
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
tag = "local-socks"
protocol = "socks5"

[[outbounds]]
protocol = "mpp"
endpoints = ["tcp://127.0.0.1:443", "udp://127.0.0.1:443"]

[outbounds.security]
credential = "test-default"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]

[[routing.rules]]
id = "default"
action = "outbound"
target = "mpp"
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
fn repository_root_config_is_one_valid_client_profile_after_secret_replacement() {
    let contents = include_str!("../../examples/config.reference.toml")
        .replace("REPLACE_ME", "0123456789abcdef0123456789abcdef")
        .replace("server.example.com", "mptunnel.test")
        .replace(
            "REPLACE_WITH_SERVER_CERT.pem",
            "mptunnel-test-certificate.pem",
        );
    let config = load_config_toml_str(&contents).expect("root client config");

    assert_eq!(config.session, SessionConfig::default());
    assert_eq!(config.resources, ResourceLimits::default());
    let CommandConfig::Node(node) = config.command;
    let clients = mpp_outbounds(&node);
    assert_eq!(clients.len(), 1);
    assert!(node.servers.is_empty());
    assert_eq!(node.local_ingresses.len(), 2);
    assert_eq!(clients[0].paths.len(), 2);
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
tag = "local-socks"
protocol = "socks5"

[[outbounds]]
tag = "mpp-main"
protocol = "mpp"
endpoints = ["tcp://127.0.0.1:443", "udp://127.0.0.1:443"]

[outbounds.performance]
extra_traffic_hint_percent = 25

[outbounds.security]
credential = "test-default"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]

[[routing.rules]]
id = "default"
action = "outbound"
target = "mpp-main"
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
    assert_eq!(clients[0].performance.extra_traffic_hint_percent, 25);
    assert_eq!(node.local_ingresses[0].tag.as_deref(), Some("local-socks"));
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
fn node_config_toml_preserves_local_inbound_tags() {
    let config = load_config_toml_str(
        r#"
[[inbounds]]
tag = "local-socks"
protocol = "socks5"
listen = ["127.0.0.1:1080"]

[[inbounds]]
tag = "local-http"
protocol = "http-connect"
listen = ["127.0.0.1:8080"]

[[outbounds]]
tag = "mpp-main"
protocol = "mpp"
endpoints = ["tcp://127.0.0.1:443"]

[outbounds.security]
credential = "test-default"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]

[[routing.rules]]
id = "default"
action = "outbound"
target = "mpp-main"
"#,
    )
    .expect("config");

    let CommandConfig::Node(node) = config.command;
    assert_eq!(mpp_outbounds(&node).len(), 1);
    let ingresses = &node.local_ingresses;
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
fn node_config_toml_builds_strict_fixed_target_tcp_and_udp_inbounds() {
    let config = load_config_toml_str(
        r#"
[[inbounds]]
tag = "local-tcp-forward"
protocol = "tcp-forward"
listen = ["127.0.0.1:8443", "[::1]:8443"]
target = "BÜCHER.Example.:443"
max_connections = 32

[[inbounds]]
tag = "local-udp-forward"
protocol = "udp-forward"
listen = ["127.0.0.1:5353"]
target = "[2001:db8::53]:53"
max_associations = 16
idle_timeout_ms = 5000
datagram_ttl_ms = 1500

[[outbounds]]
tag = "mpp-main"
protocol = "mpp"
endpoints = ["tcp://127.0.0.1:443"]

[outbounds.security]
credential = "test-default"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]

[[routing.rules]]
id = "default"
action = "outbound"
target = "mpp-main"
"#,
    )
    .expect("fixed-target inbound config");

    let CommandConfig::Node(node) = config.command;
    let [tcp, udp] = node.local_ingresses.as_slice() else {
        panic!("expected TCP and UDP fixed-target inbounds");
    };
    assert_eq!(tcp.tag.as_deref(), Some("local-tcp-forward"));
    let IngressConfig::TcpForward(tcp) = &tcp.config else {
        panic!("expected TCP fixed-target inbound");
    };
    assert_eq!(tcp.listen().len(), 2);
    assert_eq!(tcp.target().to_string(), "xn--bcher-kva.example:443");
    assert_eq!(tcp.max_connections(), 32);

    assert_eq!(udp.tag.as_deref(), Some("local-udp-forward"));
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
fn inbound_tags_must_be_unique() {
    let err = load_config_toml_str(
        r#"
[[inbounds]]
tag = "duplicate"
protocol = "socks5"

[[inbounds]]
tag = "duplicate"
protocol = "http-connect"

[[outbounds]]
tag = "mpp-main"
protocol = "mpp"
endpoints = ["tcp://127.0.0.1:443"]

[outbounds.security]
credential = "test-default"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"
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
[dns]
generation = 3
default_plan = "egress"

[[dns.upstreams]]
id = "v4"
transport = "udp-tcp"
bootstrap = "1.1.1.1:53"

[[dns.upstreams]]
id = "v6"
transport = "udp-tcp"
bootstrap = "[2606:4700:4700::1111]:53"

[[dns.plans]]
id = "egress"
upstreams = ["v4", "v6"]
ip_strategy = "ipv4-and-ipv6"
lookup_timeout_ms = 1500
cache_capacity = 2048
max_inflight = 32
positive_ttl_cap_ms = 120000
negative_ttl_cap_ms = 15000

[[inbounds]]
tag = "local-http"
protocol = "http-connect"
listen = ["127.0.0.1:8081"]

[[inbounds]]
tag = "edge-mpp"
protocol = "mpp"
endpoints = ["tcp://0.0.0.0:8443", "udp://0.0.0.0:8443"]
outbound = "proxy-egress"
dns_plan = "egress"

[inbounds.security]
credentials = ["test-default"]
tls_certificate_chain_file = "mptunnel-test-certificate.pem"
tls_private_key_file = "mptunnel-test-private-key.pem"

[inbounds.performance]
extra_traffic_hint_percent = 200

[[outbounds]]
tag = "mpp-main"
protocol = "mpp"
endpoints = ["tcp://127.0.0.1:443"]

[outbounds.security]
credential = "test-fed"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[[outbounds]]
tag = "proxy-egress"
protocol = "socks5"
proxy = "127.0.0.1:8080"

[routing]

[[routing.rules]]
id = "default"
action = "outbound"
target = "mpp-main"
"#,
    )
    .expect("config");

    let CommandConfig::Node(node) = config.command;
    assert_eq!(mpp_outbounds(&node).len(), 1);
    assert_eq!(node.servers.len(), 1);
    let server = &node.servers[0];
    assert_eq!(server.tag.as_deref(), Some("edge-mpp"));
    assert_eq!(server.performance.extra_traffic_hint_percent, 200);
    assert_eq!(server.route_target.kind, RouteTargetKind::Outbound);
    assert_eq!(server.route_target.tag, "proxy-egress");
    assert_eq!(server.bind_paths.len(), 2);
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
tag = "local-http"
protocol = "http-connect"

[[outbounds]]
tag = "secure-proxy"
protocol = "https-connect"
proxy = "127.0.0.1:4443"
tls_server_name = "mptunnel.test"
tls_ca_certificate_file = "mptunnel-test-certificate.pem"

[outbounds.auth]
username = "alice"
password = { from = "file", path = "proxy-password.key" }

[routing]

[[routing.rules]]
id = "default"
action = "outbound"
target = "secure-proxy"
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
protocol = "mpp"
endpoints = ["tcp://127.0.0.1:8443"]
outbound = "proxy"

[inbounds.security]
credentials = ["test-default"]
tls_certificate_chain_file = "mptunnel-test-certificate.pem"
tls_private_key_file = "mptunnel-test-private-key.pem"

[[outbounds]]
tag = "proxy"
protocol = "socks5"
proxy = "127.0.0.1:1080"

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
tag = "local-socks"
protocol = "socks5"

[[inbounds]]
tag = "edge"
protocol = "mpp"
endpoints = ["tcp://0.0.0.0:8443"]
outbound = "direct-a"

[inbounds.security]
credentials = ["test-a"]
tls_certificate_chain_file = "mptunnel-test-certificate.pem"
tls_private_key_file = "mptunnel-test-private-key.pem"

[[outbounds]]
tag = "mpp-a"
protocol = "mpp"
endpoints = ["tcp://127.0.0.1:443"]

[outbounds.security]
credential = "test-b"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[[outbounds]]
tag = "mpp-b"
protocol = "mpp"
endpoints = ["udp://127.0.0.1:443"]

[outbounds.security]
credential = "test-c"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[[outbounds]]
tag = "direct-a"
protocol = "direct"

[routing]

[[routing.balancers]]
tag = "edge-gateway"
strategy = "round-robin"
members = [{ outbound = "mpp-a" }, { outbound = "mpp-b" }]

[[routing.rules]]
id = "default"
action = "balancer"
target = "edge-gateway"
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
    assert_eq!(node.servers[0].route_target.kind, RouteTargetKind::Outbound);
    assert_eq!(node.servers[0].route_target.tag, "direct-a");
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
tag = "local-socks"
protocol = "socks5"

[[outbounds]]
tag = "edge-a"
protocol = "direct"

[[outbounds]]
tag = "edge-b"
protocol = "direct"

[routing]
generation = 9

[[routing.balancers]]
tag = "manual-edge"
strategy = "manual"
manual_member = "edge-a"
freshness_ttl_ms = 30000
members = [
  { outbound = "edge-a", weight = 3 },
  { outbound = "edge-b", mode = "draining" },
]
stickiness = { key = "principal", ttl_ms = 60000, capacity = 1024 }
probe = { target = "192.0.2.1:443", interval_ms = 10000, timeout_ms = 2000 }

[[routing.balancers]]
tag = "random-edge"
strategy = "random"
members = [
  { outbound = "edge-a" },
  { outbound = "edge-b" },
]

[[routing.rules]]
id = "default"
action = "balancer"
target = "manual-edge"
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
tag = "local-socks"
protocol = "socks5"

[[outbounds]]
tag = "mpp-a"
protocol = "mpp"
endpoints = ["tcp://127.0.0.1:443"]

[outbounds.security]
credential = "test-b"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[[outbounds]]
tag = "mpp-b"
protocol = "mpp"
endpoints = ["udp://127.0.0.1:443"]

[outbounds.security]
credential = "test-c"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]

[[routing.balancers]]
tag = "combined-edge"
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
tag = "local-http"
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
protocol = "mpp"
endpoints = ["tcp://0.0.0.0:8443"]
balancer = "outer"

[inbounds.security]
credentials = ["test-a"]
tls_certificate_chain_file = "mptunnel-test-certificate.pem"
tls_private_key_file = "mptunnel-test-private-key.pem"

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
members = [{ outbound = "direct-a" }]

[[routing.balancers]]
tag = "outer"
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
tag = "local-socks"
protocol = "socks5"

[[inbounds]]
tag = "local-http"
protocol = "http-connect"

[[outbounds]]
tag = "edge-a"
protocol = "mpp"
endpoints = ["udp://127.0.0.1:7443"]
[outbounds.security]
credential = "test-a"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[[outbounds]]
tag = "edge-b"
protocol = "mpp"
endpoints = ["udp://127.0.0.1:8443"]
[outbounds.security]
credential = "test-b"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]
generation = 42

[[routing.balancers]]
tag = "all-edges"
strategy = "round-robin"
members = [{ outbound = "edge-a" }, { outbound = "edge-b" }]

[[routing.rules]]
id = "interactive-api"
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
principals = ["alice"]
stages = ["pre-resolution"]
action = "outbound"
target = "edge-a"
traffic_intent = "throughput"
explanation = "API traffic uses edge A"

[[routing.rules]]
id = "default"
action = "balancer"
target = "all-edges"
traffic_intent = "interactive"
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
        decision.action().traffic_intent(),
        TrafficIntent::Throughput
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
tag = "local-socks"
protocol = "socks5"

[[outbounds]]
tag = "edge"
protocol = "mpp"
endpoints = ["udp://127.0.0.1:7443"]
[outbounds.security]
credential = "test-a"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]

[[routing.rule_set_publishers]]
id = "official"
ed25519_public_key_base64 = "{public_key}"

[[routing.rule_sets]]
id = "geo-public"
publisher = "official"
minimum_revision = {minimum_revision}
file = "geo-public.ruleset.json"

[[routing.rules]]
id = "signed-domain"
domain_rule_sets = ["geo-public"]
action = "outbound"
target = "edge"

[[routing.rules]]
id = "signed-network"
destination_rule_sets = ["geo-public"]
stages = ["post-resolution"]
action = "reject"

[[routing.rules]]
id = "default"
action = "outbound"
target = "edge"
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
tag = "local-socks"
protocol = "socks5"

[[outbounds]]
tag = "edge"
protocol = "mpp"
endpoints = ["udp://127.0.0.1:7443"]
[outbounds.security]
credential = "test-a"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]
[[routing.rules]]
id = "only-specific"
domain_suffix = ["example.com"]
action = "outbound"
target = "edge"
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
tag = "local-socks"
protocol = "socks5"

[[outbounds]]
tag = "edge"
protocol = "mpp"
endpoints = ["udp://127.0.0.1:7443"]
[outbounds.security]
credential = "test-a"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]
[[routing.rules]]
id = "default"
action = "outbound"
target = "missing"
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
tag = "local-socks"
protocol = "socks5"

[[outbounds]]
tag = "direct"
protocol = "direct"

[routing]
[[routing.rules]]
id = "default"
{field} = []
action = "outbound"
target = "direct"
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
tag = "local-socks"
protocol = "socks5"

[[outbounds]]
tag = "edge"
protocol = "mpp"
endpoints = ["udp://127.0.0.1:7443"]
[outbounds.security]
credential = "test-a"
tls_server_name = "mptunnel.test"
tls_pinned_certificate_file = "mptunnel-test-certificate.pem"

[routing]
[[routing.rules]]
id = "default-deny"
action = "reject"
traffic_intent = "background"
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
    assert_eq!(
        decision.action().traffic_intent(),
        TrafficIntent::Background
    );

    let unsupported_reset = load_config_toml_str(
        r#"
[[inbounds]]
tag = "local-socks"
protocol = "socks5"

[routing]
[[routing.rules]]
id = "default-deny"
action = "reset"
"#,
    )
    .expect_err("generic reset action must not be accepted");
    assert!(
        unsupported_reset
            .to_string()
            .contains("unknown variant `reset`"),
        "unexpected reset-action error: {unsupported_reset}"
    );
}

#[test]
fn product_admission_is_strict_finite_and_independent_from_core_resources() {
    let document = |admission: &str| {
        format!(
            r#"
{admission}

[[inbounds]]
tag = "local-socks"
protocol = "socks5"

[routing]
[[routing.rules]]
id = "default-deny"
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
