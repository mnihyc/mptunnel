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
