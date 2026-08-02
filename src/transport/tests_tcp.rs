use super::*;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{Frame, SessionId};
use crate::transport::framed::FramedStream;
use std::time::Duration;

#[tokio::test]
async fn tcp_path_connects_to_bound_listener_and_frames_work() {
    let bind_path = reserve_tcp_path().await;
    let listener = bind_listener(&bind_path).await.expect("bind listener");
    let local_addr = listener.local_addr().expect("local addr");
    let client_path = format!("tcp://{local_addr}")
        .parse::<PathSpec>()
        .expect("client path");

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut framed = FramedStream::new(stream, CodecLimits::default());
        framed.read_frame().await.expect("read")
    });

    let stream = connect_path(
        &client_path,
        TcpConnectOptions {
            timeout: Duration::from_secs(1),
            ..TcpConnectOptions::default()
        },
    )
    .await
    .expect("connect");
    let mut framed = FramedStream::new(stream, CodecLimits::default());
    let frame = Frame::SessionHello {
        session_id: SessionId(7),
    };
    framed.write_frame(&frame).await.expect("write");
    framed.flush().await.expect("flush");

    assert_eq!(server.await.expect("join"), frame);
}

#[tokio::test]
async fn tcp_path_rejects_udp_underlay() {
    let path = "udp://127.0.0.1:1234".parse::<PathSpec>().expect("path");
    let err = connect_path(&path, TcpConnectOptions::default())
        .await
        .expect_err("wrong underlay");

    assert!(matches!(
        err,
        TcpTransportError::WrongUnderlay(UnderlayProtocol::Udp)
    ));
}

#[tokio::test]
async fn tcp_address_race_does_not_let_one_family_consume_the_deadline() {
    let v6 = SocketAddr::from(([0x2001, 0xdb8, 0, 0, 0, 0, 0, 1], 443));
    let v4 = SocketAddr::from(([192, 0, 2, 1], 443));
    let deadline = Instant::now() + Duration::from_millis(120);

    let selected = tokio::time::timeout(
        Duration::from_secs(1),
        race_tcp_address_attempts(vec![v6, v4], deadline, |addr, _| async move {
            if addr.is_ipv6() {
                std::future::pending::<Result<SocketAddr, TcpTransportError>>().await
            } else {
                Ok(addr)
            }
        }),
    )
    .await
    .expect("family stagger must not hang")
    .expect("alternate family succeeds");

    assert_eq!(selected, v4);
}

async fn reserve_tcp_path() -> PathSpec {
    let probe = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve port");
    let port = probe.local_addr().expect("reserved addr").port();
    drop(probe);
    format!("tcp://127.0.0.1:{port}")
        .parse()
        .expect("bind path")
}
