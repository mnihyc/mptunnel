use super::{AuthNonce, AuthTag, PathCapabilities, PathId, SessionId, UnderlayProtocol};
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

const SESSION_AUTH_CONTEXT: &[u8] = b"mptunnel session auth v1";
const PATH_JOIN_CONTEXT: &[u8] = b"mptunnel path join v1";
const KEY_UPDATE_CONTEXT: &[u8] = b"mptunnel key update v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionAuthCheck {
    pub session_id: SessionId,
    pub nonce: AuthNonce,
    pub issued_at_unix_secs: u64,
    pub tag: AuthTag,
    pub now_unix_secs: u64,
    pub freshness_window_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PathJoinAuthCheck {
    pub session_id: SessionId,
    pub path_id: PathId,
    pub underlay: UnderlayProtocol,
    pub nonce: AuthNonce,
    pub issued_at_unix_secs: u64,
    pub capabilities: PathCapabilities,
    pub tag: AuthTag,
    pub now_unix_secs: u64,
    pub freshness_window_secs: u64,
}

#[derive(Debug, Clone)]
pub struct SessionAuthenticator {
    secret: Vec<u8>,
}

impl SessionAuthenticator {
    pub fn new(exporter_secret: impl AsRef<[u8]>) -> Result<Self, AuthError> {
        let secret = exporter_secret.as_ref();
        if secret.is_empty() {
            return Err(AuthError::EmptySecret);
        }
        Ok(Self {
            secret: secret.to_vec(),
        })
    }

    pub fn session_auth_tag(
        &self,
        session_id: SessionId,
        nonce: AuthNonce,
        issued_at_unix_secs: u64,
    ) -> AuthTag {
        let mut mac = self.mac();
        mac.update(SESSION_AUTH_CONTEXT);
        update_session_id(&mut mac, session_id);
        update_nonce(&mut mac, nonce);
        update_issued_at(&mut mac, issued_at_unix_secs);
        finalize_tag(mac)
    }

    pub fn verify_session_auth(&self, check: SessionAuthCheck) -> bool {
        if !issued_at_is_fresh(
            check.issued_at_unix_secs,
            check.now_unix_secs,
            check.freshness_window_secs,
        ) {
            return false;
        }
        let mut mac = self.mac();
        mac.update(SESSION_AUTH_CONTEXT);
        update_session_id(&mut mac, check.session_id);
        update_nonce(&mut mac, check.nonce);
        update_issued_at(&mut mac, check.issued_at_unix_secs);
        verify_tag(mac, check.tag)
    }

    pub fn path_join_tag(
        &self,
        session_id: SessionId,
        path_id: PathId,
        underlay: UnderlayProtocol,
        nonce: AuthNonce,
        issued_at_unix_secs: u64,
        capabilities: PathCapabilities,
    ) -> AuthTag {
        let mut mac = self.mac();
        mac.update(PATH_JOIN_CONTEXT);
        update_session_id(&mut mac, session_id);
        update_path_id(&mut mac, path_id);
        update_underlay(&mut mac, underlay);
        update_nonce(&mut mac, nonce);
        update_issued_at(&mut mac, issued_at_unix_secs);
        update_capabilities(&mut mac, capabilities);
        finalize_tag(mac)
    }

    pub fn verify_path_join(&self, check: PathJoinAuthCheck) -> bool {
        if !issued_at_is_fresh(
            check.issued_at_unix_secs,
            check.now_unix_secs,
            check.freshness_window_secs,
        ) {
            return false;
        }
        let mut mac = self.mac();
        mac.update(PATH_JOIN_CONTEXT);
        update_session_id(&mut mac, check.session_id);
        update_path_id(&mut mac, check.path_id);
        update_underlay(&mut mac, check.underlay);
        update_nonce(&mut mac, check.nonce);
        update_issued_at(&mut mac, check.issued_at_unix_secs);
        update_capabilities(&mut mac, check.capabilities);
        verify_tag(mac, check.tag)
    }

    pub fn key_update_tag(&self, key_phase: u64, nonce: AuthNonce) -> AuthTag {
        let mut mac = self.mac();
        mac.update(KEY_UPDATE_CONTEXT);
        mac.update(&key_phase.to_be_bytes());
        update_nonce(&mut mac, nonce);
        finalize_tag(mac)
    }

    pub fn verify_key_update(&self, key_phase: u64, nonce: AuthNonce, tag: AuthTag) -> bool {
        let mut mac = self.mac();
        mac.update(KEY_UPDATE_CONTEXT);
        mac.update(&key_phase.to_be_bytes());
        update_nonce(&mut mac, nonce);
        verify_tag(mac, tag)
    }

    fn mac(&self) -> HmacSha256 {
        HmacSha256::new_from_slice(&self.secret).expect("HMAC accepts keys of any length")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthError {
    EmptySecret,
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptySecret => write!(f, "authenticator secret must not be empty"),
        }
    }
}

impl std::error::Error for AuthError {}

fn update_session_id(mac: &mut HmacSha256, session_id: SessionId) {
    mac.update(&session_id.0.to_be_bytes());
}

fn update_path_id(mac: &mut HmacSha256, path_id: PathId) {
    mac.update(&path_id.0.to_be_bytes());
}

fn update_underlay(mac: &mut HmacSha256, underlay: UnderlayProtocol) {
    let value = match underlay {
        UnderlayProtocol::Tcp => 1u8,
        UnderlayProtocol::Udp => 2u8,
    };
    mac.update(&[value]);
}

fn update_nonce(mac: &mut HmacSha256, nonce: AuthNonce) {
    mac.update(&nonce.0);
}

fn update_issued_at(mac: &mut HmacSha256, issued_at_unix_secs: u64) {
    mac.update(&issued_at_unix_secs.to_be_bytes());
}

fn update_capabilities(mac: &mut HmacSha256, capabilities: PathCapabilities) {
    mac.update(&capabilities.to_bits().to_be_bytes());
}

fn issued_at_is_fresh(
    issued_at_unix_secs: u64,
    now_unix_secs: u64,
    freshness_window_secs: u64,
) -> bool {
    if freshness_window_secs == 0 {
        return false;
    }
    let skew = issued_at_unix_secs.abs_diff(now_unix_secs);
    skew <= freshness_window_secs
}

fn finalize_tag(mac: HmacSha256) -> AuthTag {
    AuthTag(mac.finalize().into_bytes().into())
}

fn verify_tag(mac: HmacSha256, tag: AuthTag) -> bool {
    mac.verify_slice(&tag.0).is_ok()
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn key_update_tags_are_domain_separated_from_session_auth() {
        let auth = authenticator();
        let nonce = AuthNonce([4; 16]);
        let session_tag = auth.session_auth_tag(SessionId(7), nonce, 1_735_689_600);
        let key_tag = auth.key_update_tag(7, nonce);

        assert_ne!(session_tag, key_tag);
        assert!(auth.verify_key_update(7, nonce, key_tag));
        assert!(!auth.verify_key_update(8, nonce, key_tag));
    }
}
