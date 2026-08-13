use super::*;

fn proxy_auth() -> ProxyAuthConfig {
    let principal = PrincipalId::parse("daily-user").expect("principal");
    let user = LocalProxyUser::new(
        "operator".to_string(),
        principal,
        "operator".to_string(),
        "secret".to_string(),
    )
    .expect("local proxy user");
    ProxyAuthConfig::required([user]).expect("proxy auth")
}

#[test]
fn proxy_credentials_debug_redacts_password() {
    let auth = proxy_auth();

    let rendered = format!("{auth:?}");

    assert!(rendered.contains("user_count: 1"));
    assert!(!rendered.contains("operator"));
    assert!(!rendered.contains("secret"));
}

#[test]
fn proxy_auth_verifies_basic_header() {
    let auth = proxy_auth();
    let header = format!(
        "Basic {}",
        BASE64_STANDARD.encode("operator:secret".as_bytes())
    );

    assert_eq!(
        auth.authenticate_basic_header(Some(&header))
            .expect("authenticated")
            .as_str(),
        "daily-user"
    );
    assert!(auth.authenticate_basic_header(Some("Basic bad")).is_none());
    assert!(auth.authenticate_basic_header(None).is_none());
}

#[test]
fn port_forward_target_is_strict_and_canonical() {
    let domain = PortForwardTarget::parse("BÜCHER.Example.:443").expect("canonical target");
    assert_eq!(
        domain.as_target(),
        &TargetAddr::Domain {
            host: "xn--bcher-kva.example".to_string(),
            port: 443,
        }
    );
    assert_eq!(
        PortForwardTarget::parse("[2001:0db8::1]:53")
            .expect("IPv6 target")
            .as_target(),
        &TargetAddr::Ip("[2001:db8::1]:53".parse().expect("socket address"))
    );

    for invalid in [
        "example.com",
        "example.com:0",
        " user@example.com:443",
        "https://example.com:443",
        "127.0.0.1:0",
        "2001:db8::1:443",
    ] {
        assert!(
            PortForwardTarget::parse(invalid).is_err(),
            "{invalid:?} must be rejected"
        );
    }
}

#[test]
fn port_forward_configs_reject_unbounded_or_ambiguous_runtime_values() {
    let target = PortForwardTarget::parse("127.0.0.1:53").expect("target");
    assert_eq!(
        TcpForwardConfig::with_defaults(Vec::new(), target.clone()),
        Err(PortForwardConfigError::NoListeners)
    );
    assert!(matches!(
        TcpForwardConfig::with_defaults(
            vec!["127.0.0.1:0".parse().expect("listener")],
            target.clone()
        ),
        Err(PortForwardConfigError::ZeroListenPort(_))
    ));
    let duplicate = "127.0.0.1:5300".parse().expect("listener");
    assert_eq!(
        TcpForwardConfig::with_defaults(vec![duplicate, duplicate], target.clone()),
        Err(PortForwardConfigError::DuplicateListener(duplicate))
    );
    assert_eq!(
        TcpForwardConfig::new(vec![duplicate], target.clone(), 0),
        Err(PortForwardConfigError::ZeroMaxConnections)
    );
    assert_eq!(
        TcpForwardConfig::new(
            vec![duplicate],
            target.clone(),
            MAX_TCP_FORWARD_CONNECTIONS + 1,
        ),
        Err(PortForwardConfigError::TooManyTcpConnections)
    );
    let mixed = MixedForwardConfig::with_defaults(
        vec!["127.0.0.1:5400".parse().expect("mixed listener")],
        target.clone(),
    )
    .expect("mixed forward defaults");
    assert_eq!(mixed.target(), &target);
    assert_eq!(mixed.max_connections(), DEFAULT_TCP_FORWARD_MAX_CONNECTIONS);
    assert_eq!(
        mixed.max_associations(),
        DEFAULT_UDP_FORWARD_MAX_ASSOCIATIONS
    );
    assert_eq!(
        UdpForwardConfig::new(
            vec!["127.0.0.1:5300".parse().expect("listener")],
            target.clone(),
            0,
            Duration::from_secs(60),
            Duration::from_secs(30),
        ),
        Err(PortForwardConfigError::ZeroMaxAssociations)
    );
    assert_eq!(
        UdpForwardConfig::new(
            vec!["127.0.0.1:5300".parse().expect("listener")],
            target.clone(),
            MAX_UDP_FORWARD_ASSOCIATIONS + 1,
            Duration::from_secs(60),
            Duration::from_secs(30),
        ),
        Err(PortForwardConfigError::TooManyUdpAssociations)
    );
    assert_eq!(
        UdpForwardConfig::new(
            vec!["127.0.0.1:5300".parse().expect("listener")],
            target.clone(),
            1,
            Duration::ZERO,
            Duration::from_secs(30),
        ),
        Err(PortForwardConfigError::InvalidIdleTimeout)
    );
    assert_eq!(
        UdpForwardConfig::new(
            vec!["127.0.0.1:5300".parse().expect("listener")],
            target.clone(),
            1,
            Duration::from_secs(60),
            Duration::from_nanos(1),
        ),
        Err(PortForwardConfigError::InvalidDatagramTtl)
    );
    assert_eq!(
        UdpForwardConfig::new(
            vec!["127.0.0.1:5300".parse().expect("listener")],
            target,
            1,
            Duration::from_millis(u64::from(u32::MAX) + 1),
            Duration::from_secs(30),
        ),
        Err(PortForwardConfigError::InvalidIdleTimeout)
    );
}
