use super::{AuthNonce, AuthTag, PathId, SessionId, UnderlayProtocol};
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

const SESSION_AUTH_CONTEXT: &[u8] = b"mptunnel session auth v1";
const PATH_JOIN_CONTEXT: &[u8] = b"mptunnel path join v1";

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
    ) -> AuthTag {
        let mut mac = self.mac();
        mac.update(PATH_JOIN_CONTEXT);
        update_session_id(&mut mac, session_id);
        update_path_id(&mut mac, path_id);
        update_underlay(&mut mac, underlay);
        update_nonce(&mut mac, nonce);
        update_issued_at(&mut mac, issued_at_unix_secs);
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
        verify_tag(mac, check.tag)
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
#[path = "auth_test.rs"]
mod tests;
