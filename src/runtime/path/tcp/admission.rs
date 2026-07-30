//! Fixed TCP carrier admission inside an authenticated TLS 1.3 connection.
//!
//! This module is Product admission glue, not a record layer. The prelude is
//! sent exactly once before ordinary MPP frames and adds no steady-state
//! framing, encryption, padding, or scheduling work.

use crate::config::{ClientSecurityConfig, ServerSecurityConfig};
use crate::protocol::auth::SessionAuthenticator;
use crate::protocol::{
    AuthNonce, AuthTag, Frame, PathId, PathPurpose, SessionId, UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::identity::{current_unix_secs, random_nonce, random_session_id};
use crate::runtime::path::authentication::{
    AuthenticatedServerPathSession, CredentialAdmissionPort, ServerPathAuthentication,
};
use crate::transport::encrypted::TCP_ADMISSION_PRELUDE_LEN;
use std::sync::Arc;

const CLIENT_ROLE: u8 = 1;
const CLIENT_TO_SERVER: u8 = 1;
const MAX_CREDENTIAL_ID_BYTES: usize = 64;

const ROLE_OFFSET: usize = 0;
const DIRECTION_OFFSET: usize = 1;
const CREDENTIAL_LENGTH_OFFSET: usize = 2;
const CREDENTIAL_OFFSET: usize = 3;
const SESSION_ID_OFFSET: usize = CREDENTIAL_OFFSET + MAX_CREDENTIAL_ID_BYTES;
const NONCE_OFFSET: usize = SESSION_ID_OFFSET + 8;
const ISSUED_AT_OFFSET: usize = NONCE_OFFSET + 16;
const TAG_OFFSET: usize = ISSUED_AT_OFFSET + 8;

const _: () = assert!(TAG_OFFSET + 32 == TCP_ADMISSION_PRELUDE_LEN);

pub(in crate::runtime) struct ClientTcpPathAuthentication {
    prelude: [u8; TCP_ADMISSION_PRELUDE_LEN],
    path_join: Frame,
}

impl ClientTcpPathAuthentication {
    pub(in crate::runtime) fn for_new_session(
        security: &ClientSecurityConfig,
        path_id: PathId,
        tls_exporter: &[u8; 32],
    ) -> Result<Self, RuntimeError> {
        Self::for_session(security, path_id, random_session_id()?, tls_exporter)
    }

    pub(in crate::runtime) fn for_session(
        security: &ClientSecurityConfig,
        path_id: PathId,
        session_id: SessionId,
        tls_exporter: &[u8; 32],
    ) -> Result<Self, RuntimeError> {
        let credential_id = security.credential.id().as_str();
        if credential_id.is_empty() || credential_id.len() > MAX_CREDENTIAL_ID_BYTES {
            return Err(RuntimeError::Protocol(
                "TCP admission credential ID is outside the wire limit",
            ));
        }
        let issued_at_unix_secs = current_unix_secs()?;
        let session_nonce = random_nonce()?;
        let path_nonce = random_nonce()?;
        let authenticator = SessionAuthenticator::new(security.credential.secret().as_bytes())?;
        let session_tag = authenticator.tcp_session_auth_tag(
            session_id,
            credential_id,
            session_nonce,
            issued_at_unix_secs,
            tls_exporter,
        );
        let path_tag = authenticator.path_join_tag(
            session_id,
            credential_id,
            path_id,
            UnderlayProtocol::Tcp,
            PathPurpose::Ordinary,
            path_nonce,
            issued_at_unix_secs,
        );
        Ok(Self {
            prelude: encode_prelude(
                session_id,
                credential_id,
                session_nonce,
                issued_at_unix_secs,
                session_tag,
            ),
            path_join: Frame::PathJoin {
                session_id,
                credential_id: credential_id.to_string(),
                path_id,
                underlay: UnderlayProtocol::Tcp,
                purpose: PathPurpose::Ordinary,
                nonce: path_nonce,
                issued_at_unix_secs,
                auth_tag: path_tag,
            },
        })
    }

    pub(in crate::runtime) fn into_parts(self) -> ([u8; TCP_ADMISSION_PRELUDE_LEN], Frame) {
        (self.prelude, self.path_join)
    }
}

pub(in crate::runtime) fn authenticate_prelude(
    security: &ServerSecurityConfig,
    credential_admission: Arc<dyn CredentialAdmissionPort>,
    encoded: &[u8; TCP_ADMISSION_PRELUDE_LEN],
    tls_exporter: &[u8; 32],
) -> Result<Option<AuthenticatedServerPathSession>, RuntimeError> {
    let Some(decoded) = decode_prelude(encoded) else {
        return Ok(None);
    };
    ServerPathAuthentication::authenticate_tcp_session(
        security,
        credential_admission,
        decoded.session_id,
        decoded.credential_id,
        decoded.nonce,
        decoded.issued_at_unix_secs,
        decoded.auth_tag,
        tls_exporter,
    )
}

fn encode_prelude(
    session_id: SessionId,
    credential_id: &str,
    nonce: AuthNonce,
    issued_at_unix_secs: u64,
    auth_tag: AuthTag,
) -> [u8; TCP_ADMISSION_PRELUDE_LEN] {
    debug_assert!(!credential_id.is_empty() && credential_id.len() <= MAX_CREDENTIAL_ID_BYTES);
    let mut encoded = [0u8; TCP_ADMISSION_PRELUDE_LEN];
    encoded[ROLE_OFFSET] = CLIENT_ROLE;
    encoded[DIRECTION_OFFSET] = CLIENT_TO_SERVER;
    encoded[CREDENTIAL_LENGTH_OFFSET] =
        u8::try_from(credential_id.len()).expect("credential length already bounded");
    encoded[CREDENTIAL_OFFSET..CREDENTIAL_OFFSET + credential_id.len()]
        .copy_from_slice(credential_id.as_bytes());
    encoded[SESSION_ID_OFFSET..NONCE_OFFSET].copy_from_slice(&session_id.0.to_be_bytes());
    encoded[NONCE_OFFSET..ISSUED_AT_OFFSET].copy_from_slice(&nonce.0);
    encoded[ISSUED_AT_OFFSET..TAG_OFFSET].copy_from_slice(&issued_at_unix_secs.to_be_bytes());
    encoded[TAG_OFFSET..].copy_from_slice(&auth_tag.0);
    encoded
}

struct DecodedPrelude<'a> {
    session_id: SessionId,
    credential_id: &'a str,
    nonce: AuthNonce,
    issued_at_unix_secs: u64,
    auth_tag: AuthTag,
}

fn decode_prelude(encoded: &[u8; TCP_ADMISSION_PRELUDE_LEN]) -> Option<DecodedPrelude<'_>> {
    if encoded[ROLE_OFFSET] != CLIENT_ROLE || encoded[DIRECTION_OFFSET] != CLIENT_TO_SERVER {
        return None;
    }
    let credential_length = usize::from(encoded[CREDENTIAL_LENGTH_OFFSET]);
    if credential_length == 0 || credential_length > MAX_CREDENTIAL_ID_BYTES {
        return None;
    }
    if encoded[CREDENTIAL_OFFSET + credential_length..SESSION_ID_OFFSET]
        .iter()
        .any(|byte| *byte != 0)
    {
        return None;
    }
    let credential_id =
        std::str::from_utf8(&encoded[CREDENTIAL_OFFSET..CREDENTIAL_OFFSET + credential_length])
            .ok()?;
    let session_id = SessionId(u64::from_be_bytes(
        encoded[SESSION_ID_OFFSET..NONCE_OFFSET]
            .try_into()
            .expect("fixed session ID slice"),
    ));
    let nonce = AuthNonce(
        encoded[NONCE_OFFSET..ISSUED_AT_OFFSET]
            .try_into()
            .expect("fixed nonce slice"),
    );
    let issued_at_unix_secs = u64::from_be_bytes(
        encoded[ISSUED_AT_OFFSET..TAG_OFFSET]
            .try_into()
            .expect("fixed issue-time slice"),
    );
    let auth_tag = AuthTag(
        encoded[TAG_OFFSET..]
            .try_into()
            .expect("fixed authentication-tag slice"),
    );
    Some(DecodedPrelude {
        session_id,
        credential_id,
        nonce,
        issued_at_unix_secs,
        auth_tag,
    })
}

#[cfg(test)]
#[path = "admission_test.rs"]
mod tests;
