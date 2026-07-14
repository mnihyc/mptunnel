//! Carrier-neutral path authentication protocol transitions.
//!
//! TCP and QUIC own framing and transport lifecycle. This owner constructs the
//! shared client flight and makes the server's HELLO -> AUTH -> JOIN order
//! structural without owning sockets, carrier acknowledgements, or replay state.

use crate::config::SecurityConfig;
use crate::protocol::auth::{PathJoinAuthCheck, SessionAuthCheck, SessionAuthenticator};
use crate::protocol::{AuthNonce, Frame, PathCapabilities, PathId, SessionId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::identity::{current_unix_secs, random_nonce, random_session_id};
use crate::transport::PathSpec;

/// The authenticated three-frame flight that starts one client carrier path.
pub(in crate::runtime) struct ClientPathAuthenticationFrames {
    pub(in crate::runtime) session_hello: Frame,
    pub(in crate::runtime) session_auth: Frame,
    pub(in crate::runtime) path_join: Frame,
}

impl ClientPathAuthenticationFrames {
    /// Transient probes use an isolated session identity rather than borrowing
    /// delivery evidence from a long-lived multipath session.
    pub(in crate::runtime) fn for_new_session(
        security: &SecurityConfig,
        path: &PathSpec,
        path_id: PathId,
        underlay: UnderlayProtocol,
    ) -> Result<Self, RuntimeError> {
        Self::for_session(security, path, path_id, underlay, random_session_id()?)
    }

    /// Persistent TCP and QUIC carriers authenticate under their shared MPP
    /// session while retaining distinct path identities and capabilities.
    pub(in crate::runtime) fn for_session(
        security: &SecurityConfig,
        path: &PathSpec,
        path_id: PathId,
        underlay: UnderlayProtocol,
        session_id: SessionId,
    ) -> Result<Self, RuntimeError> {
        let authenticator = SessionAuthenticator::new(security.secret.as_bytes())?;
        let issued_at_unix_secs = current_unix_secs()?;
        let session_nonce = random_nonce()?;
        let session_tag =
            authenticator.session_auth_tag(session_id, session_nonce, issued_at_unix_secs);
        let path_nonce = random_nonce()?;
        let capabilities = path.metadata.capabilities;
        let path_tag = authenticator.path_join_tag(
            session_id,
            path_id,
            underlay,
            path_nonce,
            issued_at_unix_secs,
            capabilities,
        );
        Ok(Self {
            session_hello: Frame::SessionHello { session_id },
            session_auth: Frame::SessionAuth {
                session_id,
                nonce: session_nonce,
                issued_at_unix_secs,
                auth_tag: session_tag,
            },
            path_join: Frame::PathJoin {
                session_id,
                path_id,
                underlay,
                nonce: path_nonce,
                issued_at_unix_secs,
                capabilities,
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
    authenticator: SessionAuthenticator,
    now_unix_secs: u64,
    freshness_window_secs: u64,
}

impl ServerPathAuthentication {
    /// Returns `None` for a wrong first frame so each carrier can preserve its
    /// own protocol error text while sharing the authentication state machine.
    pub(in crate::runtime) fn from_session_hello(
        security: &SecurityConfig,
        frame: Frame,
    ) -> Result<Option<Self>, RuntimeError> {
        let Frame::SessionHello { session_id } = frame else {
            return Ok(None);
        };
        Ok(Some(Self {
            session_id,
            authenticator: SessionAuthenticator::new(security.secret.as_bytes())?,
            now_unix_secs: current_unix_secs()?,
            freshness_window_secs: security.auth_freshness_window.as_secs(),
        }))
    }

    /// Consuming the pending state prevents PATH_JOIN verification before the
    /// enclosing session authentication has succeeded.
    pub(in crate::runtime) fn authenticate_session(
        self,
        frame: Frame,
    ) -> Option<AuthenticatedServerPathSession> {
        let Frame::SessionAuth {
            session_id,
            nonce,
            issued_at_unix_secs,
            auth_tag,
        } = frame
        else {
            return None;
        };
        if session_id != self.session_id
            || !self.authenticator.verify_session_auth(SessionAuthCheck {
                session_id,
                nonce,
                issued_at_unix_secs,
                tag: auth_tag,
                now_unix_secs: self.now_unix_secs,
                freshness_window_secs: self.freshness_window_secs,
            })
        {
            return None;
        }
        Some(AuthenticatedServerPathSession {
            session_id,
            authenticator: self.authenticator,
            now_unix_secs: self.now_unix_secs,
            freshness_window_secs: self.freshness_window_secs,
        })
    }
}

/// An authenticated session that may verify exactly one carrier PATH_JOIN.
pub(in crate::runtime) struct AuthenticatedServerPathSession {
    session_id: SessionId,
    authenticator: SessionAuthenticator,
    now_unix_secs: u64,
    freshness_window_secs: u64,
}

impl AuthenticatedServerPathSession {
    pub(in crate::runtime) fn authenticate_path_join(
        self,
        expected_underlay: UnderlayProtocol,
        frame: Frame,
    ) -> Option<AuthenticatedPathJoin> {
        let Frame::PathJoin {
            session_id,
            path_id,
            underlay,
            nonce,
            issued_at_unix_secs,
            capabilities,
            auth_tag,
        } = frame
        else {
            return None;
        };
        if session_id != self.session_id
            || underlay != expected_underlay
            || !self.authenticator.verify_path_join(PathJoinAuthCheck {
                session_id,
                path_id,
                underlay,
                nonce,
                issued_at_unix_secs,
                capabilities,
                tag: auth_tag,
                now_unix_secs: self.now_unix_secs,
                freshness_window_secs: self.freshness_window_secs,
            })
        {
            return None;
        }
        Some(AuthenticatedPathJoin {
            session_id,
            path_id,
            nonce,
            capabilities,
        })
    }
}

/// Verified carrier identity; replay admission remains a server-context action.
pub(in crate::runtime) struct AuthenticatedPathJoin {
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) nonce: AuthNonce,
    pub(in crate::runtime) capabilities: PathCapabilities,
}
