use mptunnel::transport::CARRIER_PATH_QUERY_KEYS;

const MATERIAL_SOURCE_KINDS: [&str; 5] = ["file", "env", "hex", "base64", "raw"];
const DNS_PROTOCOLS: [&str; 7] = ["system", "udp", "tcp", "udp-tcp", "dot", "doh", "doq"];

const REFERENCE: &str = include_str!("../examples/config.reference.toml");

#[test]
fn reference_documents_every_material_source_and_consumer() {
    for source in MATERIAL_SOURCE_KINDS {
        assert!(
            REFERENCE.contains(&format!("from = \"{source}\"")),
            "configuration reference omits material source {source}"
        );
    }
    assert!(REFERENCE.contains("secret = { value = \"exact UTF-8 bytes\" }"));

    for consumer in [
        "credentials.secret",
        "local_users.password",
        "outbounds.auth.password",
        "management.token",
        "transport_secret",
        "ed25519_public_key",
        "tls_pinned_certificate",
        "tls_certificate_chain",
        "tls_private_key",
        "tls_ca_certificate",
    ] {
        assert!(
            REFERENCE.contains(consumer),
            "configuration reference omits material consumer {consumer}"
        );
    }
}

#[test]
fn reference_documents_complete_dns_and_carrier_vocabularies() {
    for protocol in DNS_PROTOCOLS {
        assert!(
            REFERENCE.contains(&format!("protocol = \"{protocol}\"")),
            "configuration reference omits DNS protocol {protocol}"
        );
    }
    for &key in CARRIER_PATH_QUERY_KEYS {
        assert!(
            REFERENCE.contains(key),
            "configuration reference omits carrier query key {key}"
        );
    }
    assert!(REFERENCE.contains("[[dns.servers]]"));
    assert!(REFERENCE.contains("[[dns.policies]]"));
    assert!(REFERENCE.contains("[[dns.records]]"));
    assert!(REFERENCE.contains("[dns.override]"));
    assert!(REFERENCE.contains("query = { timeout_ms"));
    assert!(REFERENCE.contains("cache = { entries"));
}

#[test]
fn current_documents_do_not_publish_superseded_configuration_names() {
    let documents = [
        include_str!("../README.md"),
        include_str!("../RFC.md"),
        include_str!("../SECURITY.md"),
        include_str!("../docs/ARCHITECTURE.md"),
        include_str!("../docs/OPERATIONS.md"),
        include_str!("../docs/PERFORMANCE.md"),
        include_str!("../docs/PLATFORM.md"),
        REFERENCE,
        include_str!("../examples/client.toml"),
        include_str!("../examples/server.toml"),
        include_str!("../packaging/README.md"),
    ];
    for stale in [
        "api/v2",
        "default_dns_plan",
        "dns_plan",
        "dns.upstreams",
        "dns.plans",
        "dns.hosts",
        "dns.fake_dns",
        "udp://",
        "?source-ip=",
        "&source-ip=",
        "?srtt-ms=",
        "&srtt-ms=",
        "?jitter-ms=",
        "&jitter-ms=",
        "port-hop-interval-ms",
        "bulk-allowed",
        "probe-only",
        "no-udp",
    ] {
        assert!(
            documents.iter().all(|document| !document.contains(stale)),
            "published document contains superseded spelling {stale}"
        );
    }
}

#[test]
fn release_package_links_but_does_not_bundle_the_exhaustive_reference() {
    let package_readme = include_str!("../packaging/README.md");
    let release_contract = include_str!("../packaging/tools/release_contract.py");
    assert!(package_readme.contains("../examples/config.reference.toml"));
    assert!(!release_contract.contains("config.reference.toml"));
}
