use super::*;

#[tokio::test]
async fn udp_path_connects_to_bound_socket_and_datagrams_work() {
    let bind_path = reserve_udp_path().await;
    let socket = bind_socket(&bind_path).await.expect("bind socket");
    let local_addr = socket.local_addr().expect("local addr");
    let client_path = format!("quic://{local_addr}")
        .parse::<PathSpec>()
        .expect("client path");

    let server = tokio::spawn(async move {
        let mut buf = [0u8; 64];
        let (len, peer) = socket.recv_from(&mut buf).await.expect("recv");
        assert_eq!(&buf[..len], b"ping");
        socket.send_to(b"pong", peer).await.expect("send");
    });

    let socket = connect_path(
        &client_path,
        UdpConnectOptions {
            timeout: Duration::from_secs(1),
            ..UdpConnectOptions::default()
        },
    )
    .await
    .expect("connect");
    socket.send(b"ping").await.expect("send");
    let mut buf = [0u8; 64];
    let len = timeout(Duration::from_secs(1), socket.recv(&mut buf))
        .await
        .expect("recv timeout")
        .expect("recv");
    assert_eq!(&buf[..len], b"pong");

    server.await.expect("join");
}

#[tokio::test]
async fn udp_path_rejects_tcp_underlay() {
    let path = "tcp://127.0.0.1:1234".parse::<PathSpec>().expect("path");
    let err = connect_path(&path, UdpConnectOptions::default())
        .await
        .expect_err("wrong underlay");

    assert!(matches!(
        err,
        UdpTransportError::WrongUnderlay(UnderlayProtocol::Tcp)
    ));
}

async fn reserve_udp_path() -> PathSpec {
    let probe = UdpSocket::bind("127.0.0.1:0").await.expect("reserve port");
    let port = probe.local_addr().expect("reserved addr").port();
    drop(probe);
    format!("quic://127.0.0.1:{port}")
        .parse()
        .expect("bind path")
}
