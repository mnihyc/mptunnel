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
            0x40, 0xe3, 0xbb, 0x20, 0x31, 0x10, 0x35, 0x01, 0xfe, 0xd1, 0xc0, 0x4a, 0x08, 0x56,
            0xfe, 0x92, 0x6e, 0x47, 0xc3, 0x49, 0xd5, 0x85, 0xb9, 0x09, 0xe0, 0x8c, 0xf4, 0x96,
            0x38, 0xe5, 0xb4, 0x9a,
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
            0x37, 0x37, 0xe9, 0x76, 0xc3, 0xb5, 0xc2, 0x7d, 0x87, 0xce, 0x8c, 0xd5, 0x19, 0xc5,
            0xfb, 0xdb, 0x01, 0x1d, 0x1d, 0x63, 0xb1, 0xfc, 0x20, 0xf4, 0x45, 0x05, 0x6c, 0xd1,
            0xb5, 0xbc, 0x99, 0x3e,
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
