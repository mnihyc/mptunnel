use super::*;

fn authenticator() -> SessionAuthenticator {
    SessionAuthenticator::new(b"test exporter secret").expect("authenticator")
}

#[test]
fn empty_secret_is_rejected() {
    assert_eq!(
        SessionAuthenticator::new([]).expect_err("empty"),
        AuthError::EmptySecret
    );
}

#[test]
fn session_auth_tag_verifies_and_detects_tampering() {
    let auth = authenticator();
    let nonce = AuthNonce([1; 16]);
    let issued_at = 1_735_689_600;
    let tag = auth.session_auth_tag(SessionId(9), nonce, issued_at);

    assert!(auth.verify_session_auth(SessionAuthCheck {
        session_id: SessionId(9),
        nonce,
        issued_at_unix_secs: issued_at,
        tag,
        now_unix_secs: issued_at,
        freshness_window_secs: 300,
    }));
    assert!(!auth.verify_session_auth(SessionAuthCheck {
        session_id: SessionId(10),
        nonce,
        issued_at_unix_secs: issued_at,
        tag,
        now_unix_secs: issued_at,
        freshness_window_secs: 300,
    }));
    assert!(!auth.verify_session_auth(SessionAuthCheck {
        session_id: SessionId(9),
        nonce: AuthNonce([2; 16]),
        issued_at_unix_secs: issued_at,
        tag,
        now_unix_secs: issued_at,
        freshness_window_secs: 300,
    }));
    assert!(!auth.verify_session_auth(SessionAuthCheck {
        session_id: SessionId(9),
        nonce,
        issued_at_unix_secs: issued_at,
        tag,
        now_unix_secs: issued_at + 301,
        freshness_window_secs: 300,
    }));
}

#[test]
fn path_join_tag_covers_underlay_and_capabilities() {
    let auth = authenticator();
    let nonce = AuthNonce([3; 16]);
    let issued_at = 1_735_689_600;
    let caps = PathCapabilities {
        low_latency: true,
        bulk_allowed: true,
        ..PathCapabilities::default()
    };
    let tag = auth.path_join_tag(
        SessionId(9),
        PathId(2),
        UnderlayProtocol::Udp,
        nonce,
        issued_at,
        caps,
    );

    assert!(auth.verify_path_join(PathJoinAuthCheck {
        session_id: SessionId(9),
        path_id: PathId(2),
        underlay: UnderlayProtocol::Udp,
        nonce,
        issued_at_unix_secs: issued_at,
        capabilities: caps,
        tag,
        now_unix_secs: issued_at,
        freshness_window_secs: 300,
    }));
    assert!(!auth.verify_path_join(PathJoinAuthCheck {
        session_id: SessionId(9),
        path_id: PathId(2),
        underlay: UnderlayProtocol::Tcp,
        nonce,
        issued_at_unix_secs: issued_at,
        capabilities: caps,
        tag,
        now_unix_secs: issued_at,
        freshness_window_secs: 300,
    }));
    assert!(!auth.verify_path_join(PathJoinAuthCheck {
        session_id: SessionId(9),
        path_id: PathId(2),
        underlay: UnderlayProtocol::Udp,
        nonce,
        issued_at_unix_secs: issued_at,
        capabilities: PathCapabilities {
            expensive: true,
            ..caps
        },
        tag,
        now_unix_secs: issued_at,
        freshness_window_secs: 300,
    }));
    assert!(!auth.verify_path_join(PathJoinAuthCheck {
        session_id: SessionId(9),
        path_id: PathId(2),
        underlay: UnderlayProtocol::Udp,
        nonce,
        issued_at_unix_secs: issued_at,
        capabilities: caps,
        tag,
        now_unix_secs: issued_at + 301,
        freshness_window_secs: 300,
    }));
}
