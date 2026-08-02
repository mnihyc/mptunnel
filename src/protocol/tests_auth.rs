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
    let credential_id = "home-2026";
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
fn path_join_tag_covers_identity_underlay_purpose_nonce_and_freshness() {
    let auth = authenticator();
    let credential_id = "home-2026";
    let nonce = AuthNonce([3; 16]);
    let issued_at = 1_735_689_600;
    let tag = auth.path_join_tag(
        SessionId(9),
        credential_id,
        PathId(2),
        UnderlayProtocol::Udp,
        PathPurpose::Ordinary,
        nonce,
        issued_at,
    );
    let check = PathJoinAuthCheck {
        session_id: SessionId(9),
        credential_id,
        path_id: PathId(2),
        underlay: UnderlayProtocol::Udp,
        purpose: PathPurpose::Ordinary,
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
        purpose: PathPurpose::Validation,
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
fn v5_auth_contexts_and_v1_tcp_prelude_have_stable_tags() {
    let auth = authenticator();
    let issued_at = 1_735_689_600;

    assert_eq!(
        auth.session_auth_tag(SessionId(9), "home-2026", AuthNonce([1; 16]), issued_at,),
        AuthTag([
            0xba, 0x1c, 0xf2, 0xd0, 0x98, 0x73, 0xce, 0x94, 0xab, 0x69, 0xa6, 0x14, 0x04, 0x7e,
            0x7d, 0x8c, 0xcb, 0xc1, 0x6a, 0xab, 0x54, 0x95, 0x1a, 0xee, 0x3d, 0x1c, 0xf3, 0xd5,
            0xf5, 0x98, 0x43, 0x43,
        ])
    );
    assert_eq!(
        auth.path_join_tag(
            SessionId(9),
            "home-2026",
            PathId(2),
            UnderlayProtocol::Udp,
            PathPurpose::Ordinary,
            AuthNonce([3; 16]),
            issued_at,
        ),
        AuthTag([
            0xaa, 0xdf, 0x16, 0xef, 0x20, 0xb3, 0xbe, 0xed, 0xf5, 0x23, 0x44, 0xb4, 0x14, 0x32,
            0x5f, 0x56, 0x57, 0xc0, 0x79, 0x60, 0xb2, 0xfe, 0xae, 0xd2, 0x98, 0xb2, 0x28, 0xe5,
            0x47, 0xa4, 0xe6, 0xcf,
        ])
    );
    assert_eq!(
        auth.tcp_session_auth_tag(
            SessionId(9),
            "home-2026",
            AuthNonce([4; 16]),
            issued_at,
            &[5; 32],
        ),
        AuthTag([
            0x4b, 0x38, 0xc5, 0x9c, 0x31, 0xd4, 0x1d, 0xe8, 0x6b, 0x6e, 0x2c, 0x44, 0xb7, 0x0e,
            0x94, 0x0a, 0x2c, 0x30, 0x9e, 0xa7, 0x17, 0xd7, 0xea, 0xc6, 0x99, 0x21, 0xf9, 0xee,
            0xbc, 0xbc, 0x03, 0x81,
        ])
    );
}
