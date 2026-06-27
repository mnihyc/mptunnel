use super::*;

#[test]
fn server_path_context_rejects_replayed_path_join_nonce() {
    let context = server_context(OutboundConfig::Direct);
    let session_id = SessionId(42);
    let path_id = PathId(3);
    let nonce = AuthNonce([9; 16]);

    assert!(context.accept_path_join_nonce(session_id, path_id, UnderlayProtocol::Tcp, nonce));
    assert!(!context.accept_path_join_nonce(session_id, path_id, UnderlayProtocol::Tcp, nonce));
    assert!(context.accept_path_join_nonce(session_id, path_id, UnderlayProtocol::Udp, nonce));
    assert!(context.accept_path_join_nonce(session_id, PathId(4), UnderlayProtocol::Tcp, nonce));
    assert!(context.accept_path_join_nonce(
        session_id,
        path_id,
        UnderlayProtocol::Tcp,
        AuthNonce([10; 16])
    ));
}

#[tokio::test]
async fn server_udp_path_rejects_replayed_path_join_for_fresh_session() {
    let socket = Arc::new(
        UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("server udp bind"),
    );
    let first_peer = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("first peer udp bind");
    let second_peer = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("second peer udp bind");
    let context = server_context(OutboundConfig::Direct);
    let path = "udp://127.0.0.1:7443".parse::<PathSpec>().expect("path");
    let session_id = SessionId(177);
    let path_id = PathId(7);
    let (_, _, join) = authenticated_path_join_frames_for_session(
        &security(),
        &path,
        path_id,
        UnderlayProtocol::Udp,
        session_id,
    )
    .expect("auth frames");

    let mut first = ServerUdpPathSession::new(
        socket.clone(),
        first_peer.local_addr().expect("first peer addr"),
        context.clone(),
    )
    .expect("first session");
    first.handle_frame(join.clone()).await.expect("first join");
    assert!(matches!(first.state, ServerUdpPathState::Established));

    let mut second = ServerUdpPathSession::new(
        socket,
        second_peer.local_addr().expect("second peer addr"),
        context,
    )
    .expect("second session");
    assert!(matches!(
        second.handle_frame(join).await,
        Err(RuntimeError::Protocol("unexpected UDP datagram path frame"))
    ));
    assert!(matches!(
        second.state,
        ServerUdpPathState::AwaitSessionHello
    ));
    assert_eq!(second.session_id, None);
}
