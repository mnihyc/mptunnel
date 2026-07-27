use super::*;
use crate::dns::{DnsGeneration, DnsRuntimeError};
use crate::ingress::socks5 as ingress_socks5;
use crate::outbound::ServerDestinationPolicy;
use crate::product::{
    AclEffect, AclRuleSpec, CompiledDnsPolicy, DestinationAcl, DnsPlanId, DnsPlanSpec,
    DnsPolicySpec, DnsUpstreamEndpoint, DnsUpstreamId, DnsUpstreamSpec, DomainName, Network,
    PortRange, RouteMatchSpec, RuleId,
};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};

struct TestHttpsIdentity {
    certificate: rustls::pki_types::CertificateDer<'static>,
    private_key_der: Vec<u8>,
}

fn test_https_identity() -> &'static TestHttpsIdentity {
    static IDENTITY: std::sync::OnceLock<TestHttpsIdentity> = std::sync::OnceLock::new();
    IDENTITY.get_or_init(|| {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["mptunnel.test".to_string()])
                .expect("generate HTTPS proxy test identity");
        TestHttpsIdentity {
            certificate: rustls::pki_types::CertificateDer::from(cert),
            private_key_der: signing_key.serialize_der(),
        }
    })
}

fn test_https_server_config() -> rustls::ServerConfig {
    let identity = test_https_identity();
    let private_key =
        rustls::pki_types::PrivatePkcs8KeyDer::from(identity.private_key_der.clone()).into();
    let mut config = rustls::ServerConfig::builder_with_protocol_versions(&[
        &rustls::version::TLS13,
        &rustls::version::TLS12,
    ])
    .with_no_client_auth()
    .with_single_cert(vec![identity.certificate.clone()], private_key)
    .expect("TLS server identity");
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    config
}

fn test_https_proxy_config(
    endpoint: Endpoint,
    server_name: &str,
    credentials: Option<ProxyCredentials>,
) -> HttpsProxyConfig {
    let roots = vec![test_https_identity().certificate.clone()];
    HttpsProxyConfig::new(
        ProxyConfig::new(endpoint, credentials),
        Some(server_name.to_string()),
        roots,
    )
    .expect("HTTPS proxy config")
}

fn test_dns_runtime() -> DnsGeneration {
    let mut answers = HashMap::new();
    answers.insert(
        "example.com".to_string(),
        vec![IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10))],
    );
    DnsGeneration::from_test_answers(answers)
}

fn test_resolved_addr(port: u16) -> SocketAddr {
    SocketAddr::from(([192, 0, 2, 10], port))
}

fn static_dns_runtime(
    answers: impl IntoIterator<Item = (&'static str, Vec<IpAddr>)>,
) -> DnsGeneration {
    DnsGeneration::from_test_answers(
        answers
            .into_iter()
            .map(|(domain, addresses)| (domain.to_string(), addresses))
            .collect(),
    )
}

fn blackhole_dns_runtime(upstream: SocketAddr, lookup_timeout: Duration) -> DnsGeneration {
    let upstream_id = DnsUpstreamId::parse("blackhole").expect("upstream ID");
    let plan_id = DnsPlanId::parse("default").expect("plan ID");
    let mut plan = DnsPlanSpec::new(plan_id.clone(), vec![upstream_id.clone()]);
    plan.limits.lookup_timeout = lookup_timeout;
    let policy = CompiledDnsPolicy::compile(
        1,
        DnsPolicySpec {
            upstreams: vec![DnsUpstreamSpec::direct(
                upstream_id,
                DnsUpstreamEndpoint::Udp {
                    bootstrap: upstream,
                },
            )],
            outbound_capabilities: Vec::new(),
            plans: vec![plan],
            rules: Vec::new(),
            hosts: Vec::new(),
            fake_dns: None,
            default_plan: plan_id,
        },
    )
    .expect("blackhole DNS policy");
    DnsGeneration::compile(Arc::new(policy)).expect("blackhole DNS generation")
}

fn safe_destination_policy() -> impl DestinationAuthorizer {
    ServerDestinationPolicy::new(DestinationAcl::safe_default(1)).test_principal_policy()
}

fn scoped_loopback_policy(domain: &str, port: u16) -> impl DestinationAuthorizer {
    let matcher = RouteMatchSpec {
        domain_exact: vec![DomainName::parse(domain).expect("test ACL domain")],
        destination_cidrs: vec!["127.0.0.0/8".parse().expect("test ACL CIDR")],
        destination_ports: vec![PortRange::single(port)],
        networks: vec![Network::Tcp, Network::Udp],
        ..RouteMatchSpec::default()
    };
    ServerDestinationPolicy::new(
        DestinationAcl::compile(
            9,
            vec![AclRuleSpec::new(
                RuleId::parse("allow-scoped-loopback").expect("test ACL rule ID"),
                matcher,
                AclEffect::AllowRestricted,
            )],
        )
        .expect("test destination ACL"),
    )
    .test_principal_policy()
}

async fn connect_tcp(
    config: &OutboundConfig,
    dns: &DnsGeneration,
    target: &TargetAddr,
    timeout: Duration,
) -> Result<OutboundTcpStream, OutboundConnectError> {
    super::connect_tcp(
        config,
        dns,
        None,
        &ServerDestinationPolicy::allow_restricted_for_test().test_principal_policy(),
        target,
        timeout,
    )
    .await
}

async fn connect_udp(
    config: &OutboundConfig,
    dns: &DnsGeneration,
    target: &TargetAddr,
    timeout: Duration,
) -> Result<OutboundUdpSocket, OutboundConnectError> {
    super::connect_udp(
        config,
        dns,
        None,
        &ServerDestinationPolicy::allow_restricted_for_test().test_principal_policy(),
        target,
        timeout,
    )
    .await
}

#[test]
fn outbound_support_matrix_matches_protocol_semantics() {
    assert!(
        OutboundConfig::Direct
            .ensure_supports(TargetProtocol::Tcp)
            .is_ok()
    );
    assert!(
        OutboundConfig::Direct
            .ensure_supports(TargetProtocol::Udp)
            .is_ok()
    );
    let socks5 = OutboundConfig::Socks5(ProxyConfig::new(
        "127.0.0.1:1080".parse().expect("proxy"),
        None,
    ));
    assert!(socks5.ensure_supports(TargetProtocol::Tcp).is_ok());
    assert!(socks5.ensure_supports(TargetProtocol::Udp).is_ok());
    let http_connect = OutboundConfig::HttpConnect(ProxyConfig::new(
        "127.0.0.1:8080".parse().expect("proxy"),
        None,
    ));
    assert!(http_connect.ensure_supports(TargetProtocol::Tcp).is_ok());
    assert_eq!(
        http_connect.ensure_supports(TargetProtocol::Udp),
        Err(OutboundError::UdpNotSupported)
    );
}

#[tokio::test]
async fn direct_tcp_outbound_connects_to_target() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.expect("read");
        assert_eq!(&buf, b"ping");
        stream.write_all(b"pong").await.expect("write");
    });

    let config = OutboundConfig::Direct;
    let dns = test_dns_runtime();
    let mut stream = connect_tcp(&config, &dns, &TargetAddr::Ip(addr), Duration::from_secs(1))
        .await
        .expect("connect");
    stream.write_all(b"ping").await.expect("write");
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).await.expect("read");

    assert_eq!(&buf, b"pong");
    server.await.expect("server");
}

#[tokio::test]
async fn direct_udp_outbound_connects_to_target() {
    let target = UdpSocket::bind("127.0.0.1:0").await.expect("target");
    let target_addr = target.local_addr().expect("target addr");
    let server = tokio::spawn(async move {
        let mut buf = [0u8; 16];
        let (len, peer) = target.recv_from(&mut buf).await.expect("recv");
        assert_eq!(&buf[..len], b"ping");
        target.send_to(b"pong", peer).await.expect("send");
    });

    let config = OutboundConfig::Direct;
    let dns = test_dns_runtime();
    let mut socket = connect_udp(
        &config,
        &dns,
        &TargetAddr::Ip(target_addr),
        Duration::from_secs(1),
    )
    .await
    .expect("connect");
    socket.send(b"ping").await.expect("send");
    let mut buf = [0u8; 16];
    let len = socket.recv(&mut buf).await.expect("recv");

    assert_eq!(&buf[..len], b"pong");
    server.await.expect("server");
}

#[tokio::test]
async fn proxy_host_resolution_obeys_explicit_dns_without_system_leakage() {
    let blackhole = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("blackhole resolver");
    let proxy_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("proxy listener");
    let config = OutboundConfig::Socks5(ProxyConfig::new(
        format!(
            "example.com:{}",
            proxy_listener.local_addr().expect("proxy address").port()
        )
        .parse()
        .expect("proxy endpoint"),
        None,
    ));
    let dns = blackhole_dns_runtime(
        blackhole.local_addr().expect("resolver address"),
        Duration::from_millis(40),
    );

    assert!(matches!(
        connect_tcp(
            &config,
            &dns,
            &TargetAddr::Ip("192.0.2.1:443".parse().expect("target")),
            Duration::from_secs(1),
        )
        .await,
        Err(OutboundConnectError::Dns(DnsRuntimeError::Timeout { domain, .. }))
            if domain.as_str() == "example.com"
    ));
}

#[tokio::test]
async fn proxy_transaction_deadline_includes_proxy_dns_resolution() {
    let blackhole = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("blackhole resolver");
    let config = OutboundConfig::Socks5(ProxyConfig::new(
        "example.com:1080".parse().expect("proxy endpoint"),
        None,
    ));
    let dns = blackhole_dns_runtime(
        blackhole.local_addr().expect("resolver address"),
        Duration::from_secs(2),
    );

    let error = connect_tcp(
        &config,
        &dns,
        &TargetAddr::Ip("192.0.2.1:443".parse().expect("target")),
        Duration::from_millis(30),
    )
    .await
    .expect_err("proxy DNS must share outbound deadline");

    assert!(
        matches!(error, OutboundConnectError::ProxyTimeout),
        "unexpected error: {error:?}"
    );
}

#[tokio::test]
async fn socks5_udp_outbound_builds_udp_association() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let proxy: Endpoint = listener
        .local_addr()
        .expect("addr")
        .to_string()
        .parse()
        .expect("proxy");
    let target = TargetAddr::Domain {
        host: "example.com".to_string(),
        port: 53,
    };
    let expected_target = TargetAddr::Ip(test_resolved_addr(53));
    let credentials = ProxyCredentials::new("alice".to_string(), "udp-password".to_string())
        .expect("credentials");
    let expected_auth = socks5::username_password_request(&credentials);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut greeting = [0u8; 3];
        stream.read_exact(&mut greeting).await.expect("greeting");
        assert_eq!(greeting, socks5::username_password_greeting());
        stream.write_all(&[0x05, 0x02]).await.expect("method");
        let mut auth = vec![0u8; expected_auth.len()];
        stream.read_exact(&mut auth).await.expect("auth");
        assert_eq!(auth, expected_auth);
        stream.write_all(&[0x01, 0x00]).await.expect("auth reply");

        let mut request = [0u8; 10];
        stream.read_exact(&mut request).await.expect("request");
        assert_eq!(
            request.as_slice(),
            socks5::udp_associate_request("0.0.0.0:0".parse().expect("addr"))
                .expect("expected request")
        );

        let relay = UdpSocket::bind("127.0.0.1:0").await.expect("relay bind");
        let relay_addr = relay.local_addr().expect("relay addr");
        stream
            .write_all(&ingress_socks5::connect_reply(
                ingress_socks5::Socks5Reply::Succeeded,
                relay_addr,
            ))
            .await
            .expect("reply");

        let mut packet = [0u8; 512];
        let (len, peer) = relay.recv_from(&mut packet).await.expect("relay recv");
        let (datagram, consumed) =
            ingress_socks5::parse_udp_datagram(&packet[..len]).expect("udp packet");
        assert_eq!(consumed, len);
        assert_eq!(datagram.target, expected_target);
        assert_eq!(&datagram.payload[..], b"ping");

        let response_target = TargetAddr::Ip(test_resolved_addr(53));
        let response =
            ingress_socks5::udp_datagram(&response_target, b"pong").expect("response packet");
        relay.send_to(&response, peer).await.expect("relay send");
    });

    let config = OutboundConfig::Socks5(ProxyConfig::new(proxy, Some(credentials)));
    let dns = test_dns_runtime();
    let mut socket = connect_udp(&config, &dns, &target, Duration::from_secs(1))
        .await
        .expect("connect");
    socket.send(b"ping").await.expect("send");
    let mut buf = [0u8; 16];
    let len = socket.recv(&mut buf).await.expect("recv");

    assert_eq!(&buf[..len], b"pong");
    server.await.expect("server");
}

#[tokio::test]
async fn socks5_tcp_outbound_builds_connect_tunnel() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let proxy: Endpoint = listener
        .local_addr()
        .expect("addr")
        .to_string()
        .parse()
        .expect("proxy");
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut greeting = [0u8; 3];
        stream.read_exact(&mut greeting).await.expect("greeting");
        assert_eq!(greeting, socks5::no_auth_greeting());
        stream.write_all(&[0x05, 0x00]).await.expect("method");

        let expected = socks5::connect_request(&TargetAddr::Ip(test_resolved_addr(443)))
            .expect("expected request");
        let mut request = vec![0u8; expected.len()];
        stream.read_exact(&mut request).await.expect("request");
        assert_eq!(request, expected);
        stream
            .write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0])
            .await
            .expect("reply");

        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.expect("payload read");
        assert_eq!(&buf, b"ping");
        stream.write_all(b"pong").await.expect("payload write");
    });

    let config = OutboundConfig::Socks5(ProxyConfig::new(proxy, None));
    let dns = test_dns_runtime();
    let mut stream = connect_tcp(
        &config,
        &dns,
        &TargetAddr::Domain {
            host: "example.com".to_string(),
            port: 443,
        },
        Duration::from_secs(1),
    )
    .await
    .expect("connect");
    stream.write_all(b"ping").await.expect("payload write");
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).await.expect("payload read");

    assert_eq!(&buf, b"pong");
    server.await.expect("server");
}

#[tokio::test]
async fn http_connect_tcp_outbound_builds_connect_tunnel() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let proxy: Endpoint = listener
        .local_addr()
        .expect("addr")
        .to_string()
        .parse()
        .expect("proxy");
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut request = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            stream.read_exact(&mut byte).await.expect("request byte");
            request.push(byte[0]);
            if request.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        assert_eq!(
            request,
            http_connect::connect_request(&TargetAddr::Ip(test_resolved_addr(443)), None, None,)
                .expect("expected request")
        );
        stream
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .await
            .expect("reply");

        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.expect("payload read");
        assert_eq!(&buf, b"ping");
        stream.write_all(b"pong").await.expect("payload write");
    });

    let config = OutboundConfig::HttpConnect(ProxyConfig::new(proxy, None));
    let dns = test_dns_runtime();
    let mut stream = connect_tcp(
        &config,
        &dns,
        &TargetAddr::Domain {
            host: "example.com".to_string(),
            port: 443,
        },
        Duration::from_secs(1),
    )
    .await
    .expect("connect");
    stream.write_all(b"ping").await.expect("payload write");
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).await.expect("payload read");

    assert_eq!(&buf, b"pong");
    server.await.expect("server");
}

#[tokio::test]
async fn https_connect_authenticates_proxy_name_and_builds_basic_tunnel() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let proxy: Endpoint = listener
        .local_addr()
        .expect("addr")
        .to_string()
        .parse()
        .expect("proxy");
    let credentials = ProxyCredentials::new("alice".to_string(), "https-password".to_string())
        .expect("credentials");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut stream = tokio_rustls::TlsAcceptor::from(Arc::new(test_https_server_config()))
            .accept(stream)
            .await
            .expect("TLS accept");
        let mut request = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            stream.read_exact(&mut byte).await.expect("request byte");
            request.push(byte[0]);
            assert!(request.len() <= 16 * 1024);
            if request.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let request = String::from_utf8(request).expect("request text");
        assert!(request.starts_with("CONNECT 192.0.2.10:443 HTTP/1.1\r\n"));
        assert!(request.contains("Proxy-Authorization: Basic YWxpY2U6aHR0cHMtcGFzc3dvcmQ=\r\n"));
        stream
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .await
            .expect("reply");
        let mut payload = [0u8; 4];
        stream.read_exact(&mut payload).await.expect("payload");
        assert_eq!(&payload, b"ping");
        stream.write_all(b"pong").await.expect("response");
    });

    let config = OutboundConfig::HttpsConnect(Box::new(test_https_proxy_config(
        proxy,
        "mptunnel.test",
        Some(credentials),
    )));
    let dns = test_dns_runtime();
    let mut stream = connect_tcp(
        &config,
        &dns,
        &TargetAddr::Domain {
            host: "example.com".to_string(),
            port: 443,
        },
        Duration::from_secs(2),
    )
    .await
    .expect("HTTPS CONNECT");
    stream.write_all(b"ping").await.expect("payload write");
    let mut payload = [0u8; 4];
    stream.read_exact(&mut payload).await.expect("payload read");

    assert_eq!(&payload, b"pong");
    server.await.expect("server");
}

#[tokio::test]
async fn https_connect_rejects_certificate_for_wrong_proxy_name() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let proxy: Endpoint = listener
        .local_addr()
        .expect("addr")
        .to_string()
        .parse()
        .expect("proxy");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let _ = tokio_rustls::TlsAcceptor::from(Arc::new(test_https_server_config()))
            .accept(stream)
            .await;
    });
    let config = OutboundConfig::HttpsConnect(Box::new(test_https_proxy_config(
        proxy,
        "wrong-name.test",
        None,
    )));
    let dns = test_dns_runtime();

    let error = connect_tcp(
        &config,
        &dns,
        &TargetAddr::Domain {
            host: "example.com".to_string(),
            port: 443,
        },
        Duration::from_secs(2),
    )
    .await
    .expect_err("wrong proxy identity must fail");

    assert!(matches!(error, OutboundConnectError::Io(_)));
    assert!(!error.to_string().contains("https-password"));
    server.await.expect("server");
}

#[tokio::test]
async fn proxy_handshake_timeout_bounds_silent_http_proxy() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let proxy: Endpoint = listener
        .local_addr()
        .expect("addr")
        .to_string()
        .parse()
        .expect("proxy");
    let server = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.expect("accept");
        std::future::pending::<()>().await;
    });
    let config = OutboundConfig::HttpConnect(ProxyConfig::new(proxy, None));
    let dns = test_dns_runtime();

    let error = connect_tcp(
        &config,
        &dns,
        &TargetAddr::Domain {
            host: "example.com".to_string(),
            port: 443,
        },
        Duration::from_millis(40),
    )
    .await
    .expect_err("silent proxy must time out");

    assert!(matches!(error, OutboundConnectError::ProxyTimeout));
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn http_proxy_response_headers_are_strictly_bounded() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let proxy: Endpoint = listener
        .local_addr()
        .expect("addr")
        .to_string()
        .parse()
        .expect("proxy");
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut request = Vec::new();
        let mut byte = [0u8; 1];
        while !request.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).await.expect("request");
            request.push(byte[0]);
        }
        stream
            .write_all(&vec![b'a'; MAX_HTTP_CONNECT_RESPONSE_BYTES])
            .await
            .expect("oversized response");
    });
    let config = OutboundConfig::HttpConnect(ProxyConfig::new(proxy, None));
    let dns = test_dns_runtime();

    let error = connect_tcp(
        &config,
        &dns,
        &TargetAddr::Domain {
            host: "example.com".to_string(),
            port: 443,
        },
        Duration::from_secs(2),
    )
    .await
    .expect_err("oversized proxy headers");

    assert!(matches!(error, OutboundConnectError::InvalidProxyResponse));
    server.await.expect("server");
}

#[tokio::test]
async fn safe_default_blocks_literal_pivot_before_tcp_or_udp_proxy_connector() {
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.expect("proxy bind");
    let proxy = ProxyConfig::new(
        proxy_listener
            .local_addr()
            .expect("proxy address")
            .to_string()
            .parse()
            .expect("proxy endpoint"),
        None,
    );
    let config = OutboundConfig::Socks5(proxy);
    let dns = static_dns_runtime([]);
    let policy = safe_destination_policy();
    let target = TargetAddr::Ip("127.0.0.1:443".parse().expect("target"));

    for result in [
        super::connect_tcp(
            &config,
            &dns,
            None,
            &policy,
            &target,
            Duration::from_secs(1),
        )
        .await
        .map(|_| ()),
        super::connect_udp(
            &config,
            &dns,
            None,
            &policy,
            &target,
            Duration::from_secs(1),
        )
        .await
        .map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(OutboundConnectError::DestinationAuthorization(
                DestinationAuthorizationError::Acl(_)
            ))
        ));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(30), proxy_listener.accept())
            .await
            .is_err(),
        "a denied target must not invoke even the configured proxy connector"
    );
}

#[tokio::test]
async fn mixed_public_and_loopback_dns_answer_fails_closed_before_any_dial() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("target bind");
    let port = listener.local_addr().expect("target address").port();
    let dns = static_dns_runtime([(
        "mixed.test",
        vec![
            "93.184.216.34".parse().expect("public answer"),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
        ],
    )]);
    let target = TargetAddr::Domain {
        host: "mixed.test".to_string(),
        port,
    };
    let error = super::connect_tcp(
        &OutboundConfig::Direct,
        &dns,
        None,
        &safe_destination_policy(),
        &target,
        Duration::from_secs(1),
    )
    .await
    .expect_err("one denied DNS answer must deny the complete answer set");

    assert!(matches!(
        error,
        OutboundConnectError::DestinationAuthorization(DestinationAuthorizationError::Acl(_))
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err(),
        "no authorized or denied DNS answer may be dialed after mixed-answer denial"
    );
}

#[tokio::test]
async fn scoped_lan_override_authorizes_matching_domain_port_for_tcp_and_udp() {
    let tcp_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("TCP target bind");
    let target_addr = tcp_listener.local_addr().expect("TCP target address");
    let udp_target = UdpSocket::bind(target_addr).await.expect("UDP target bind");
    let port = target_addr.port();
    let dns = static_dns_runtime([("lan.test", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])]);
    let policy = scoped_loopback_policy("lan.test", port);
    let target = TargetAddr::Domain {
        host: "lan.test".to_string(),
        port,
    };
    let tcp_server = tokio::spawn(async move {
        let (mut stream, _) = tcp_listener.accept().await.expect("TCP accept");
        let mut request = [0u8; 4];
        stream.read_exact(&mut request).await.expect("TCP read");
        assert_eq!(&request, b"ping");
        stream.write_all(b"pong").await.expect("TCP write");
    });
    let udp_server = tokio::spawn(async move {
        let mut request = [0u8; 4];
        let (len, peer) = udp_target
            .recv_from(&mut request)
            .await
            .expect("UDP receive");
        assert_eq!(&request[..len], b"ping");
        udp_target.send_to(b"pong", peer).await.expect("UDP reply");
    });

    let mut tcp = super::connect_tcp(
        &OutboundConfig::Direct,
        &dns,
        None,
        &policy,
        &target,
        Duration::from_secs(1),
    )
    .await
    .expect("scoped TCP authorization");
    tcp.write_all(b"ping").await.expect("TCP request");
    let mut response = [0u8; 4];
    tcp.read_exact(&mut response).await.expect("TCP response");
    assert_eq!(&response, b"pong");

    let mut udp = super::connect_udp(
        &OutboundConfig::Direct,
        &dns,
        None,
        &policy,
        &target,
        Duration::from_secs(1),
    )
    .await
    .expect("scoped UDP authorization");
    udp.send(b"ping").await.expect("UDP request");
    let len = udp.recv(&mut response).await.expect("UDP response");
    assert_eq!(&response[..len], b"pong");

    tcp_server.await.expect("TCP server");
    udp_server.await.expect("UDP server");
}
