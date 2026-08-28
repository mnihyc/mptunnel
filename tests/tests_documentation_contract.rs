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
    assert!(REFERENCE.contains("password = { value = \"exact UTF-8 plaintext\" }"));

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
    assert!(REFERENCE.contains("[[dns.override_records]]"));
    assert!(REFERENCE.contains("[[dns.synthetic_capture]]"));
    assert!(REFERENCE.contains("override_records ="));
    assert!(REFERENCE.contains("synthetic_capture ="));
    assert!(REFERENCE.contains("query = { timeout_s"));
    assert!(REFERENCE.contains("cache = { entries"));
}

#[test]
fn reference_documents_current_product_duration_names() {
    for key in [
        "restart_backoff_s",
        "restart_max_backoff_s",
        "retention_timeout_s",
        "idle_timeout_s",
        "tcp_path_heartbeat_interval_s",
        "tcp_path_heartbeat_timeout_s",
        "quic_path_keep_alive_interval_s",
        "quic_path_idle_timeout_s",
        "expires_at_unix_s",
        "revocation_grace_s",
        "auth_freshness_window_s",
        "authentication_timeout_s",
        "datagram_ttl_s",
        "dns_ttl_s",
        "handshake_timeout_s",
        "path_probe_interval_s",
        "path_probe_timeout_s",
        "connect_timeout_s",
        "freshness_ttl_s",
        "initial_backoff_s",
        "maximum_backoff_s",
        "ttl_s",
        "interval_s",
        "timeout_s",
        "answer_ttl_s",
        "recovery_ttl_s",
        "fallback_s",
        "positive_ttl_s",
        "negative_ttl_s",
        "stale_s",
        "prefetch_s",
    ] {
        assert!(
            REFERENCE.contains(&format!("{key} =")),
            "configuration reference omits Product duration key {key}"
        );
    }
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
        "initial-srtt-ms",
        "initial-rttvar-ms",
        "port-hop-interval-ms",
        "port-rotation-interval-ms",
        "auth_freshness_window_seconds",
        "revocation_grace_seconds",
        "answer_ttl_seconds",
        "recovery_ttl_seconds",
        "restart_backoff_ms",
        "restart_max_backoff_ms",
        "retention_timeout_ms",
        "idle_timeout_ms",
        "datagram_ttl_ms",
        "tcp_path_heartbeat_interval_ms",
        "tcp_path_heartbeat_timeout_ms",
        "quic_path_keep_alive_interval_ms",
        "quic_path_idle_timeout_ms",
        "authentication_timeout_ms",
        "handshake_timeout_ms",
        "dns_ttl_ms",
        "path_probe_interval_ms",
        "path_probe_timeout_ms",
        "connect_timeout_ms",
        "freshness_ttl_ms",
        "initial_backoff_ms",
        "maximum_backoff_ms",
        "fallback_ms",
        "query.timeout_ms",
        "positive_ttl_ms",
        "negative_ttl_ms",
        "stale_ms",
        "prefetch_ms",
        "session-retention-timeout-ms",
        "udp-forward-idle-timeout-ms",
        "udp-forward-datagram-ttl-ms",
        "outbound-dns-timeout-ms",
        "bulk-allowed",
        "probe-only",
        "no-udp",
    ] {
        assert!(
            documents.iter().all(|document| !document.contains(stale)),
            "published document contains superseded spelling {stale}"
        );
    }

    let configuration_examples = [
        REFERENCE,
        include_str!("../examples/client.toml"),
        include_str!("../examples/server.toml"),
    ];
    for stale in [
        "[[dns.records]]",
        "[dns.override]",
        "forwarding_mode",
        "generation =",
        "[routing.destination_acl]",
        "[inbounds.destination_acl]",
        "effect =",
        "action = \"outbound\"",
        "action = \"balancer\"",
        "action = \"reject\"",
        "action = \"drop\"",
    ] {
        assert!(
            configuration_examples
                .iter()
                .all(|document| !document.contains(stale)),
            "published configuration contains superseded spelling {stale}"
        );
    }
}

#[test]
fn release_package_links_and_bundles_the_exhaustive_reference() {
    let package_readme = include_str!("../packaging/README.md");
    let release_contract = include_str!("../packaging/tools/release_contract.py");
    assert!(package_readme.contains("examples/config.reference.toml"));
    assert!(release_contract.contains("examples/config.reference.toml"));
}
