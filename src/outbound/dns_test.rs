
use super::*;

#[test]
fn custom_resolver_preserves_dual_stack_socket_addrs_and_ports() {
    let resolver = name_server_config("[2001:db8::53]:5353".parse().expect("resolver"));

    assert_eq!(resolver.ip, "2001:db8::53".parse::<IpAddr>().expect("ip"));
    assert_eq!(resolver.connections.len(), 2);
    assert!(resolver.connections.iter().all(|conn| conn.port == 5353));
}

#[tokio::test]
async fn literal_ips_resolve_without_dns_queries() {
    let config = DnsConfig {
        resolvers: vec!["127.0.0.53:5353".parse().expect("resolver")],
        ..DnsConfig::default()
    };

    assert_eq!(
        resolve_socket_addrs("2001:db8::1", 443, &config)
            .await
            .expect("literal"),
        vec!["[2001:db8::1]:443".parse::<SocketAddr>().expect("addr")]
    );
}
