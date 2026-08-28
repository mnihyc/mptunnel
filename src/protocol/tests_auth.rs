//! Authentication contract tests for the current clean-break wire version.

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
    assert!(!format!("{:?}", authenticator()).contains("test exporter secret"));
}

#[test]
fn session_auth_tag_verifies_and_detects_tampering() {
    let auth = authenticator();
    let credential_id = "home-client";
    let nonce = AuthNonce([1; 16]);
    let issued_at = 1_735_689_600;
    let tag = auth.session_auth_tag(SessionId(9), credential_id, nonce, issued_at);

    assert!(auth.verify_session_auth(SessionAuthCheck {
        session_id: SessionId(9),
        credential_id,
        nonce,
        issued_at_unix_secs: issued_at,
        tag,
        now_unix_secs: issued_at,
        freshness_window_secs: 300,
    }));
    assert!(!auth.verify_session_auth(SessionAuthCheck {
        session_id: SessionId(10),
        credential_id,
        nonce,
        issued_at_unix_secs: issued_at,
        tag,
        now_unix_secs: issued_at,
        freshness_window_secs: 300,
    }));
    assert!(!auth.verify_session_auth(SessionAuthCheck {
        credential_id: "other",
        ..SessionAuthCheck {
            session_id: SessionId(9),
            credential_id,
            nonce,
            issued_at_unix_secs: issued_at,
            tag,
            now_unix_secs: issued_at,
            freshness_window_secs: 300,
        }
    }));
    assert!(!auth.verify_session_auth(SessionAuthCheck {
        session_id: SessionId(9),
        credential_id,
        nonce: AuthNonce([2; 16]),
        issued_at_unix_secs: issued_at,
        tag,
        now_unix_secs: issued_at,
        freshness_window_secs: 300,
    }));
    assert!(!auth.verify_session_auth(SessionAuthCheck {
        session_id: SessionId(9),
        credential_id,
        nonce,
        issued_at_unix_secs: issued_at,
        tag,
        now_unix_secs: issued_at + 301,
        freshness_window_secs: 300,
    }));
}

#[test]
fn path_join_tag_covers_identity_underlay_nonce_and_freshness() {
    let auth = authenticator();
    let credential_id = "home-client";
    let nonce = AuthNonce([3; 16]);
    let issued_at = 1_735_689_600;
    let tag = auth.path_join_tag(
        SessionId(9),
        credential_id,
        PathId(2),
        UnderlayProtocol::Udp,
        nonce,
        issued_at,
    );
    let check = PathJoinAuthCheck {
        session_id: SessionId(9),
        credential_id,
        path_id: PathId(2),
        underlay: UnderlayProtocol::Udp,
        nonce,
        issued_at_unix_secs: issued_at,
        tag,
        now_unix_secs: issued_at,
        freshness_window_secs: 300,
    };

    assert!(auth.verify_path_join(check));
    assert!(!auth.verify_path_join(PathJoinAuthCheck {
        credential_id: "other",
        ..check
    }));
    assert!(!auth.verify_path_join(PathJoinAuthCheck {
        path_id: PathId(3),
        ..check
    }));
    assert!(!auth.verify_path_join(PathJoinAuthCheck {
        underlay: UnderlayProtocol::Tcp,
        ..check
    }));
    assert!(!auth.verify_path_join(PathJoinAuthCheck {
        nonce: AuthNonce([4; 16]),
        ..check
    }));
    let mut tampered_tag = tag.0;
    tampered_tag[0] ^= 1;
    assert!(!auth.verify_path_join(PathJoinAuthCheck {
        tag: AuthTag(tampered_tag),
        ..check
    }));
    assert!(!auth.verify_path_join(PathJoinAuthCheck {
        now_unix_secs: issued_at + 301,
        ..check
    }));
}

#[test]
fn auth_contexts_and_tcp_prelude_have_stable_tags() {
    let auth = authenticator();
    let issued_at = 1_735_689_600;

    assert_eq!(
        auth.session_auth_tag(SessionId(9), "home-client", AuthNonce([1; 16]), issued_at,),
        AuthTag([
            0x98, 0x1b, 0xee, 0x52, 0x19, 0x9c, 0x0f, 0x1b, 0x04, 0x2b, 0x82, 0x7d, 0x6d, 0xf0,
            0x4d, 0xa1, 0x81, 0x80, 0xec, 0x86, 0x51, 0x8a, 0x4f, 0x01, 0x93, 0x76, 0x14, 0x10,
            0x78, 0x8b, 0x43, 0x8c,
        ])
    );
    assert_eq!(
        auth.path_join_tag(
            SessionId(9),
            "home-client",
            PathId(2),
            UnderlayProtocol::Udp,
            AuthNonce([3; 16]),
            issued_at,
        ),
        AuthTag([
            0x21, 0x65, 0xa2, 0xa0, 0x98, 0x8a, 0x47, 0x0e, 0xe1, 0x4e, 0xe8, 0x76, 0x1b, 0x52,
            0x47, 0x16, 0xf8, 0x7a, 0x67, 0x2a, 0x36, 0x80, 0x69, 0xf5, 0x5e, 0x78, 0x21, 0x49,
            0x92, 0x38, 0x32, 0x53,
        ])
    );
    assert_eq!(
        auth.tcp_session_auth_tag(
            SessionId(9),
            "home-client",
            AuthNonce([4; 16]),
            issued_at,
            &[5; 32],
        ),
        AuthTag([
            0x52, 0xfd, 0x3b, 0x02, 0xfc, 0x8b, 0xef, 0xd6, 0x48, 0x87, 0x6e, 0x20, 0x92, 0x28,
            0x35, 0xf7, 0x1b, 0x4d, 0xd3, 0x31, 0x93, 0x0d, 0x12, 0xb7, 0x84, 0x55, 0xd0, 0x6c,
            0x80, 0x70, 0xd9, 0x6e,
        ])
    );
}
