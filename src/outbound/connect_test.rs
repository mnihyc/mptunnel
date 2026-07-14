
use super::*;
use crate::ingress::socks5 as ingress_socks5;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};

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
    assert!(
        OutboundConfig::HttpConnect {
            proxy: "127.0.0.1:8080".parse().expect("proxy")
        }
        .ensure_supports(TargetProtocol::Tcp)
        .is_ok()
    );
    assert_eq!(
        OutboundConfig::HttpConnect {
            proxy: "127.0.0.1:8080".parse().expect("proxy")
        }
        .ensure_supports(TargetProtocol::Udp),
        Err(OutboundError::UdpNotSupported)
    );
    assert!(
        OutboundConfig::HttpConnectUdp {
            proxy: "127.0.0.1:8080".parse().expect("proxy")
        }
        .ensure_supports(TargetProtocol::Tcp)
        .is_ok()
    );
    assert!(
        OutboundConfig::HttpConnectUdp {
            proxy: "127.0.0.1:8080".parse().expect("proxy")
        }
        .ensure_supports(TargetProtocol::Udp)
        .is_ok()
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

    let mut stream = connect_tcp(
        &OutboundConfig::Direct,
        &DnsConfig::default(),
        &TargetAddr::Ip(addr),
        Duration::from_secs(1),
    )
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

    let mut socket = connect_udp(
        &OutboundConfig::Direct,
        &DnsConfig::default(),
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
async fn sequence_outbound_tries_members_until_connect_succeeds() {
    let proxy = TcpListener::bind("127.0.0.1:0").await.expect("proxy bind");
    let proxy_addr = proxy.local_addr().expect("proxy addr");
    let proxy = tokio::spawn(async move {
        let (mut stream, _) = proxy.accept().await.expect("proxy accept");
        let mut request = [0u8; 8];
        let _ = stream.read(&mut request).await.expect("proxy read");
    });

    let target = TcpListener::bind("127.0.0.1:0").await.expect("target bind");
    let target_addr = target.local_addr().expect("target addr");
    let target_server = tokio::spawn(async move {
        let (mut stream, _) = target.accept().await.expect("target accept");
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.expect("target read");
        assert_eq!(&buf, b"ping");
        stream.write_all(b"pong").await.expect("target write");
    });

    let config = OutboundConfig::Sequence {
        members: vec![
            OutboundRouteMember {
                config: Box::new(OutboundConfig::HttpConnect {
                    proxy: proxy_addr.to_string().parse().expect("proxy"),
                }),
                dns: DnsConfig::default(),
                connect_timeout: Duration::from_secs(1),
            },
            OutboundRouteMember {
                config: Box::new(OutboundConfig::Direct),
                dns: DnsConfig::default(),
                connect_timeout: Duration::from_secs(1),
            },
        ],
    };

    let mut stream = connect_tcp(
        &config,
        &DnsConfig::default(),
        &TargetAddr::Ip(target_addr),
        Duration::from_secs(1),
    )
    .await
    .expect("connect");
    stream.write_all(b"ping").await.expect("write");
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).await.expect("read");

    assert_eq!(&buf, b"pong");
    proxy.await.expect("proxy");
    target_server.await.expect("target server");
}

#[tokio::test]
async fn random_outbound_falls_back_across_members() {
    let target = TcpListener::bind("127.0.0.1:0").await.expect("target bind");
    let target_addr = target.local_addr().expect("target addr");
    let target_server = tokio::spawn(async move {
        let (mut stream, _) = target.accept().await.expect("target accept");
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.expect("target read");
        assert_eq!(&buf, b"ping");
        stream.write_all(b"pong").await.expect("target write");
    });

    let proxy = TcpListener::bind("127.0.0.1:0").await.expect("proxy bind");
    let proxy_addr = proxy.local_addr().expect("proxy addr");
    let proxy = tokio::spawn(async move {
        let (mut stream, _) = proxy.accept().await.expect("proxy accept");
        let mut request = [0u8; 8];
        let _ = stream.read(&mut request).await.expect("proxy read");
    });

    let config = OutboundConfig::Random {
        members: vec![
            OutboundRouteMember {
                config: Box::new(OutboundConfig::HttpConnect {
                    proxy: proxy_addr.to_string().parse().expect("proxy"),
                }),
                dns: DnsConfig::default(),
                connect_timeout: Duration::from_secs(1),
            },
            OutboundRouteMember {
                config: Box::new(OutboundConfig::Direct),
                dns: DnsConfig::default(),
                connect_timeout: Duration::from_secs(1),
            },
        ],
    };

    let mut stream = connect_tcp(
        &config,
        &DnsConfig::default(),
        &TargetAddr::Ip(target_addr),
        Duration::from_secs(1),
    )
    .await
    .expect("connect");
    stream.write_all(b"ping").await.expect("write");
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).await.expect("read");

    assert_eq!(&buf, b"pong");
    target_server.await.expect("target server");
    if !proxy.is_finished() {
        proxy.abort();
    }
    let _ = proxy.await;
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
    let expected_target = target.clone();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut greeting = [0u8; 3];
        stream.read_exact(&mut greeting).await.expect("greeting");
        assert_eq!(greeting, socks5::no_auth_greeting());
        stream.write_all(&[0x05, 0x00]).await.expect("method");

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

        let response_target = TargetAddr::Ip("127.0.0.1:53".parse().expect("response target"));
        let response =
            ingress_socks5::udp_datagram(&response_target, b"pong").expect("response packet");
        relay.send_to(&response, peer).await.expect("relay send");
    });

    let mut socket = connect_udp(
        &OutboundConfig::Socks5 { proxy },
        &DnsConfig::default(),
        &target,
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

        let mut request = vec![0u8; 5 + 11 + 2];
        stream.read_exact(&mut request).await.expect("request");
        assert_eq!(
            request,
            socks5::connect_request(&TargetAddr::Domain {
                host: "example.com".to_string(),
                port: 443,
            })
            .expect("expected request")
        );
        stream
            .write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0])
            .await
            .expect("reply");

        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.expect("payload read");
        assert_eq!(&buf, b"ping");
        stream.write_all(b"pong").await.expect("payload write");
    });

    let mut stream = connect_tcp(
        &OutboundConfig::Socks5 { proxy },
        &DnsConfig::default(),
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
            http_connect::connect_request(
                &TargetAddr::Domain {
                    host: "example.com".to_string(),
                    port: 443,
                },
                None,
            )
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

    let mut stream = connect_tcp(
        &OutboundConfig::HttpConnect { proxy },
        &DnsConfig::default(),
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
async fn http_connect_udp_outbound_builds_capsule_tunnel() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let proxy: Endpoint = listener
        .local_addr()
        .expect("addr")
        .to_string()
        .parse()
        .expect("proxy");
    let target = TargetAddr::Domain {
        host: "example.com".to_string(),
        port: 443,
    };
    let expected_request = http_connect::connect_udp_request(&proxy, &target).expect("request");
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
        assert_eq!(request, expected_request);
        stream
                .write_all(
                    b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: connect-udp\r\nCapsule-Protocol: ?1\r\n\r\n",
                )
                .await
                .expect("reply");

        let payload = http_connect::read_datagram_capsule(&mut stream)
            .await
            .expect("capsule");
        assert_eq!(&payload, b"ping");
        let response = http_connect::datagram_capsule(b"pong").expect("response");
        stream.write_all(&response).await.expect("response write");
    });

    let mut socket = connect_udp(
        &OutboundConfig::HttpConnectUdp { proxy },
        &DnsConfig::default(),
        &target,
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
