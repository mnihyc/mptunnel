//! Carrier-neutral path authentication protocol transitions.
//!
//! TCP and QUIC own framing and transport lifecycle. This owner constructs the
//! shared client flight and makes the server's HELLO -> AUTH -> JOIN order
//! structural without owning sockets, carrier acknowledgements, or replay state.

use crate::config::{ClientSecurityConfig, ServerSecurityConfig};
use crate::product::{
    CredentialAdmissionError, CredentialCandidate, CredentialId, PrincipalPermit,
};
use crate::protocol::auth::{
    PathJoinAuthCheck, SessionAuthCheck, SessionAuthenticator, TcpSessionAuthCheck,
};
use crate::protocol::{AuthNonce, AuthTag, Frame, PathId, SessionId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::identity::{current_unix_secs, random_nonce};
use crate::transport::quic::{QuicCandidateSelector, QuicCandidateVerifier};
use std::sync::{Arc, RwLock};

const DUMMY_AUTH_SECRET: [u8; 32] = [0xA5; 32];

/// Bounded Product callback invoked only during carrier authentication.
///
/// The handshake owner verifies protocol MACs. Product resolves status and
/// issues immutable principal authorization without entering the data path.
pub(in crate::runtime) trait CredentialAdmissionPort:
    Send + Sync + std::fmt::Debug
{
    fn candidate(
        &self,
        credential_id: &CredentialId,
        now_unix_secs: u64,
    ) -> Result<CredentialCandidate, CredentialAdmissionError>;

    fn admit(
        &self,
        candidate: CredentialCandidate,
        session_id: SessionId,
        now_unix_secs: u64,
    ) -> Result<PrincipalPermit, CredentialAdmissionError>;
}

#[derive(Debug)]
pub(in crate::runtime) struct ProductCredentialAdmission {
    state: RwLock<ProductCredentialState>,
}

#[derive(Debug)]
struct ProductCredentialState {
    authority: crate::product::CredentialAuthority,
    quic_candidates: Vec<QuicCredentialCandidate>,
}

#[derive(Debug)]
struct QuicCredentialCandidate {
    selector: QuicCandidateSelector,
    expires_at_unix_secs: Option<u64>,
    revoked: bool,
}

impl ProductCredentialState {
    fn new(authority: crate::product::CredentialAuthority) -> Self {
        let quic_candidates = authority
            .credentials()
            .into_iter()
            .map(|record| QuicCredentialCandidate {
                selector: QuicCandidateSelector::derive(
                    record.id().as_str(),
                    record.secret().as_bytes(),
                ),
                expires_at_unix_secs: record.expires_at_unix_secs(),
                revoked: record.revoked(),
            })
            .collect();
        Self {
            authority,
            quic_candidates,
        }
    }
}

impl ProductCredentialAdmission {
    pub(in crate::runtime) fn from_security(security: &ServerSecurityConfig) -> Arc<Self> {
        Arc::new(Self {
            state: RwLock::new(ProductCredentialState::new(
                security.credential_authority.clone(),
            )),
        })
    }

    pub(in crate::runtime) fn authority(&self) -> crate::product::CredentialAuthority {
        self.state
            .read()
            .expect("credential authority read lock")
            .authority
            .clone()
    }

    /// Atomically publishes a prevalidated credential authority. Handshakes
    /// that fetched a candidate before publication are revalidated in
    /// [`Self::admit`], so no stale candidate can cross the publication point.
    pub(in crate::runtime) fn replace_authority(
        &self,
        authority: crate::product::CredentialAuthority,
    ) {
        *self.state.write().expect("credential authority write lock") =
            ProductCredentialState::new(authority);
    }
}

impl CredentialAdmissionPort for ProductCredentialAdmission {
    fn candidate(
        &self,
        credential_id: &CredentialId,
        now_unix_secs: u64,
    ) -> Result<CredentialCandidate, CredentialAdmissionError> {
        self.state
            .read()
            .expect("credential authority read lock")
            .authority
            .candidate(credential_id, now_unix_secs)
    }

    fn admit(
        &self,
        candidate: CredentialCandidate,
        _session_id: SessionId,
        now_unix_secs: u64,
    ) -> Result<PrincipalPermit, CredentialAdmissionError> {
        // Revalidate against the authority current at admission. This closes
        // the lookup/HMAC/publication race without a data-path policy call.
        self.state
            .read()
            .expect("credential authority read lock")
            .authority
            .candidate(candidate.id(), now_unix_secs)
            .map(|current| current.into_permit(now_unix_secs))
    }
}

impl QuicCandidateVerifier for ProductCredentialAdmission {
    fn accepts(&self, selector: &QuicCandidateSelector) -> bool {
        let Ok(now_unix_secs) = current_unix_secs() else {
            return false;
        };
        let state = self.state.read().expect("credential authority read lock");
        let mut accepted = false;
        for candidate in &state.quic_candidates {
            let active = !candidate.revoked
                && candidate
                    .expires_at_unix_secs
                    .is_none_or(|expires_at| now_unix_secs < expires_at);
            accepted |= active & candidate.selector.matches(selector);
        }
        accepted
    }
}

/// The authenticated three-frame flight that starts one client carrier path.
pub(in crate::runtime) struct ClientPathAuthenticationFrames {
    pub(in crate::runtime) session_hello: Frame,
    pub(in crate::runtime) session_auth: Frame,
    pub(in crate::runtime) path_join: Frame,
}

impl ClientPathAuthenticationFrames {
    /// Persistent TCP and QUIC carriers authenticate under their shared MPP
    /// session while retaining distinct path identities.
    pub(in crate::runtime) fn for_session(
        security: &ClientSecurityConfig,
        path_id: PathId,
        underlay: UnderlayProtocol,
        session_id: SessionId,
    ) -> Result<Self, RuntimeError> {
        let credential_id = security.credential.id().as_str();
        let authenticator = SessionAuthenticator::new(security.credential.secret().as_bytes())?;
        let issued_at_unix_secs = current_unix_secs()?;
        let session_nonce = random_nonce()?;
        let session_tag = authenticator.session_auth_tag(
            session_id,
            credential_id,
            session_nonce,
            issued_at_unix_secs,
        );
        let path_nonce = random_nonce()?;
        let path_tag = authenticator.path_join_tag(
            session_id,
            credential_id,
            path_id,
            underlay,
            path_nonce,
            issued_at_unix_secs,
        );
        Ok(Self {
            session_hello: Frame::SessionHello { session_id },
            session_auth: Frame::SessionAuth {
                session_id,
                credential_id: credential_id.to_string(),
                nonce: session_nonce,
                issued_at_unix_secs,
                auth_tag: session_tag,
            },
            path_join: Frame::PathJoin {
                session_id,
                credential_id: credential_id.to_string(),
                path_id,
                underlay,
                nonce: path_nonce,
                issued_at_unix_secs,
                auth_tag: path_tag,
            },
        })
    }

    pub(in crate::runtime) fn into_array(self) -> [Frame; 3] {
        [self.session_hello, self.session_auth, self.path_join]
    }
}

/// Pending server authentication after a syntactically valid SESSION_HELLO.
pub(in crate::runtime) struct ServerPathAuthentication {
    session_id: SessionId,
    credential_admission: Arc<dyn CredentialAdmissionPort>,
    freshness_window_secs: u64,
}

impl ServerPathAuthentication {
    /// Authenticates the fixed TCP prelude after the TLS 1.3 handshake.
    ///
    /// Invalid IDs, unavailable credentials, stale timestamps, wrong exporters,
    /// and wrong tags all take the same rejection path. A dummy HMAC keeps
    /// unknown credentials from becoming a cheap credential-existence oracle.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::runtime) fn authenticate_tcp_session(
        security: &ServerSecurityConfig,
        credential_admission: Arc<dyn CredentialAdmissionPort>,
        session_id: SessionId,
        credential_id: &str,
        nonce: AuthNonce,
        issued_at_unix_secs: u64,
        auth_tag: AuthTag,
        tls_exporter: &[u8; 32],
    ) -> Result<Option<AuthenticatedServerPathSession>, RuntimeError> {
        let now_unix_secs = current_unix_secs()?;
        let parsed_id = CredentialId::parse(credential_id).ok();
        let candidate = parsed_id
            .as_ref()
            .and_then(|id| credential_admission.candidate(id, now_unix_secs).ok());
        let authenticator = SessionAuthenticator::new(
            candidate
                .as_ref()
                .map(|value| value.secret().as_bytes())
                .unwrap_or(&DUMMY_AUTH_SECRET),
        )?;
        let verified = authenticator.verify_tcp_session_auth(TcpSessionAuthCheck {
            session_id,
            credential_id,
            nonce,
            issued_at_unix_secs,
            tls_exporter,
            tag: auth_tag,
            now_unix_secs,
            freshness_window_secs: security.auth_freshness_window.as_secs(),
        });
        let (Some(credential_id), Some(candidate)) = (parsed_id, candidate) else {
            return Ok(None);
        };
        if !verified {
            return Ok(None);
        }
        let Ok(principal_permit) = credential_admission.admit(candidate, session_id, now_unix_secs)
        else {
            return Ok(None);
        };
        Ok(Some(AuthenticatedServerPathSession {
            session_id,
            credential_id,
            authenticator,
            freshness_window_secs: security.auth_freshness_window.as_secs(),
            principal_permit,
        }))
    }

    /// Returns `None` for a wrong first frame so each carrier can preserve its
    /// own protocol error text while sharing the authentication state machine.
    pub(in crate::runtime) fn from_session_hello(
        security: &ServerSecurityConfig,
        credential_admission: Arc<dyn CredentialAdmissionPort>,
        frame: Frame,
    ) -> Result<Option<Self>, RuntimeError> {
        let Frame::SessionHello { session_id } = frame else {
            return Ok(None);
        };
        Ok(Some(Self {
            session_id,
            credential_admission,
            freshness_window_secs: security.auth_freshness_window.as_secs(),
        }))
    }

    /// Consuming the pending state prevents PATH_JOIN verification before the
    /// enclosing session authentication has succeeded.
    pub(in crate::runtime) fn authenticate_session(
        self,
        frame: Frame,
    ) -> Result<Option<AuthenticatedServerPathSession>, RuntimeError> {
        let now_unix_secs = current_unix_secs()?;
        self.authenticate_session_at(frame, now_unix_secs)
    }

    fn authenticate_session_at(
        self,
        frame: Frame,
        now_unix_secs: u64,
    ) -> Result<Option<AuthenticatedServerPathSession>, RuntimeError> {
        let Frame::SessionAuth {
            session_id,
            credential_id,
            nonce,
            issued_at_unix_secs,
            auth_tag,
        } = frame
        else {
            return Ok(None);
        };
        if session_id != self.session_id {
            return Ok(None);
        }
        let parsed_id = CredentialId::parse(&credential_id).ok();
        let candidate = parsed_id
            .as_ref()
            .and_then(|id| self.credential_admission.candidate(id, now_unix_secs).ok());
        let authenticator = SessionAuthenticator::new(
            candidate
                .as_ref()
                .map(|value| value.secret().as_bytes())
                .unwrap_or(&DUMMY_AUTH_SECRET),
        )?;
        let verified = authenticator.verify_session_auth(SessionAuthCheck {
            session_id,
            credential_id: &credential_id,
            nonce,
            issued_at_unix_secs,
            tag: auth_tag,
            now_unix_secs,
            freshness_window_secs: self.freshness_window_secs,
        });
        let (Some(credential_id), Some(candidate)) = (parsed_id, candidate) else {
            return Ok(None);
        };
        if !verified {
            return Ok(None);
        }
        let Ok(principal_permit) =
            self.credential_admission
                .admit(candidate, session_id, now_unix_secs)
        else {
            return Ok(None);
        };
        Ok(Some(AuthenticatedServerPathSession {
            session_id,
            credential_id,
            authenticator,
            freshness_window_secs: self.freshness_window_secs,
            principal_permit,
        }))
    }
}

/// An authenticated session that may verify exactly one carrier PATH_JOIN.
pub(in crate::runtime) struct AuthenticatedServerPathSession {
    session_id: SessionId,
    credential_id: CredentialId,
    authenticator: SessionAuthenticator,
    freshness_window_secs: u64,
    principal_permit: PrincipalPermit,
}

impl AuthenticatedServerPathSession {
    pub(in crate::runtime) fn authenticate_path_join(
        self,
        expected_underlay: UnderlayProtocol,
        frame: Frame,
    ) -> Result<Option<AuthenticatedPathJoin>, RuntimeError> {
        let now_unix_secs = current_unix_secs()?;
        Ok(self.authenticate_path_join_at(expected_underlay, frame, now_unix_secs))
    }

    fn authenticate_path_join_at(
        self,
        expected_underlay: UnderlayProtocol,
        frame: Frame,
        now_unix_secs: u64,
    ) -> Option<AuthenticatedPathJoin> {
        let Frame::PathJoin {
            session_id,
            credential_id,
            path_id,
            underlay,
            nonce,
            issued_at_unix_secs,
            auth_tag,
        } = frame
        else {
            return None;
        };
        if session_id != self.session_id
            || credential_id != self.credential_id.as_str()
            || underlay != expected_underlay
            || !self.authenticator.verify_path_join(PathJoinAuthCheck {
                session_id,
                credential_id: self.credential_id.as_str(),
                path_id,
                underlay,
                nonce,
                issued_at_unix_secs,
                tag: auth_tag,
                now_unix_secs,
                freshness_window_secs: self.freshness_window_secs,
            })
        {
            return None;
        }
        Some(AuthenticatedPathJoin {
            session_id,
            credential_id: self.credential_id,
            path_id,
            nonce,
            issued_at_unix_secs,
            verified_at_unix_secs: now_unix_secs,
            principal_permit: self.principal_permit,
        })
    }
}

/// Verified carrier identity; replay admission remains a server-context action.
pub(in crate::runtime) struct AuthenticatedPathJoin {
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) credential_id: CredentialId,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) nonce: AuthNonce,
    pub(in crate::runtime) issued_at_unix_secs: u64,
    pub(in crate::runtime) verified_at_unix_secs: u64,
    pub(in crate::runtime) principal_permit: PrincipalPermit,
}

#[cfg(test)]
#[path = "tests_authentication.rs"]
mod tests;
