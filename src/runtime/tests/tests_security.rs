use super::*;
use crate::product::CredentialId;
use crate::protocol::AuthNonce;

#[test]
fn server_path_context_rejects_replayed_path_join_nonce() {
    let runtime = server_runtime(OutboundConfig::Direct);
    let context = &runtime.paths;
    let session_id = SessionId(42);
    let path_id = PathId(3);
    let nonce = AuthNonce([9; 16]);
    let issued_at = 1_735_689_600;
    let credential_id = CredentialId::parse("test-credential").expect("static test credential ID");

    assert!(context.accept_path_join_nonce(
        session_id,
        credential_id.clone(),
        path_id,
        UnderlayProtocol::Tcp,
        nonce,
        issued_at,
        issued_at,
    ));
    assert!(!context.accept_path_join_nonce(
        session_id,
        credential_id.clone(),
        path_id,
        UnderlayProtocol::Tcp,
        nonce,
        issued_at,
        issued_at,
    ));
    assert!(context.accept_path_join_nonce(
        session_id,
        credential_id.clone(),
        path_id,
        UnderlayProtocol::Udp,
        nonce,
        issued_at,
        issued_at,
    ));
    assert!(context.accept_path_join_nonce(
        session_id,
        credential_id.clone(),
        PathId(4),
        UnderlayProtocol::Tcp,
        nonce,
        issued_at,
        issued_at,
    ));
    assert!(context.accept_path_join_nonce(
        session_id,
        credential_id.clone(),
        path_id,
        UnderlayProtocol::Tcp,
        AuthNonce([10; 16]),
        issued_at,
        issued_at,
    ));
    let freshness_window = context.security.auth_freshness_window.as_secs();
    assert!(!context.accept_path_join_nonce(
        session_id,
        credential_id.clone(),
        path_id,
        UnderlayProtocol::Tcp,
        AuthNonce([11; 16]),
        issued_at,
        issued_at + freshness_window + 1,
    ));
    assert!(!context.accept_path_join_nonce(
        session_id,
        credential_id,
        path_id,
        UnderlayProtocol::Tcp,
        AuthNonce([12; 16]),
        issued_at + freshness_window + 1,
        issued_at,
    ));
}
