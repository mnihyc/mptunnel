//! Runtime-generated protocol identities and wall-clock authentication time.
//!
//! Protocol code validates these values; this module owns only access to host
//! entropy and the system clock.

use crate::protocol::{AuthNonce, SessionId};
use crate::runtime::error::RuntimeError;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn current_unix_secs() -> Result<u64, RuntimeError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RuntimeError::Protocol("system clock is before UNIX epoch"))?
        .as_secs())
}

pub(super) fn random_session_id() -> Result<SessionId, RuntimeError> {
    Ok(SessionId(random_u64()?))
}

pub(super) fn random_u64() -> Result<u64, RuntimeError> {
    let mut bytes = [0u8; 8];
    getrandom::fill(&mut bytes).map_err(RuntimeError::Random)?;
    Ok(u64::from_be_bytes(bytes))
}

pub(super) fn random_nonce() -> Result<AuthNonce, RuntimeError> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(RuntimeError::Random)?;
    Ok(AuthNonce(bytes))
}
