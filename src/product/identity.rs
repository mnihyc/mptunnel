//! Product-owned MPP credentials, principals, and immutable admission permits.
//!
//! Credential lookup happens only while a carrier authenticates. The data path
//! receives an immutable [`PrincipalPermit`] and never calls this catalog per
//! frame or per byte.

use super::{CredentialId, PrincipalId};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

pub const MAX_CREDENTIALS: usize = 64;
pub const MAX_CREDENTIAL_EXPIRY_UNIX_SECS: u64 = 253_402_300_799;

#[derive(Clone, PartialEq, Eq)]
pub struct SharedSecret(Arc<[u8; Self::DERIVED_BYTES]>);

impl SharedSecret {
    pub const MIN_BYTES: usize = 32;
    pub const DERIVED_BYTES: usize = 32;

    pub fn new(value: impl Into<Vec<u8>>) -> Result<Self, SecurityPolicyError> {
        let value = value.into();
        if let Some(uuid_bytes) = parse_uuid_secret(&value) {
            return Ok(Self(Arc::new(derive_secret_material(b"uuid", &uuid_bytes))));
        }
        if value.len() < Self::MIN_BYTES {
            return Err(SecurityPolicyError::SecretTooShort {
                actual: value.len(),
                minimum: Self::MIN_BYTES,
            });
        }
        Ok(Self(Arc::new(derive_secret_material(b"raw", &value))))
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }
}

fn parse_uuid_secret(value: &[u8]) -> Option<[u8; 16]> {
    let text = std::str::from_utf8(value).ok()?.trim();
    let uuid = Uuid::parse_str(text).ok()?;
    Some(*uuid.as_bytes())
}

fn derive_secret_material(kind: &[u8], value: &[u8]) -> [u8; SharedSecret::DERIVED_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"mptunnel shared secret master v1");
    hasher.update(kind);
    hasher.update(value);
    hasher.finalize().into()
}

impl std::fmt::Debug for SharedSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SharedSecret(<redacted>)")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityPolicyError {
    MissingSecret,
    SecretTooShort { actual: usize, minimum: usize },
}

impl std::fmt::Display for SecurityPolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingSecret => write!(f, "credential secret reference is required"),
            Self::SecretTooShort { actual, minimum } => {
                write!(
                    f,
                    "credential secret is {actual} bytes, minimum is {minimum} bytes or a UUID"
                )
            }
        }
    }
}

impl std::error::Error for SecurityPolicyError {}

/// One named credential in the process-wide immutable Product catalog.
#[derive(Clone, PartialEq, Eq)]
pub struct CredentialRecord {
    id: CredentialId,
    principal: PrincipalId,
    secret: SharedSecret,
    expires_at_unix_secs: Option<u64>,
    revoked: bool,
    revocation_grace_secs: u64,
}

impl CredentialRecord {
    pub fn new(
        id: CredentialId,
        principal: PrincipalId,
        secret: SharedSecret,
        expires_at_unix_secs: Option<u64>,
        revoked: bool,
        revocation_grace_secs: u64,
    ) -> Result<Self, CredentialCatalogError> {
        if expires_at_unix_secs
            .is_some_and(|value| value == 0 || value > MAX_CREDENTIAL_EXPIRY_UNIX_SECS)
        {
            return Err(CredentialCatalogError::InvalidExpiration(id));
        }
        Ok(Self {
            id,
            principal,
            secret,
            expires_at_unix_secs,
            revoked,
            revocation_grace_secs,
        })
    }

    pub const fn id(&self) -> &CredentialId {
        &self.id
    }

    pub const fn principal(&self) -> &PrincipalId {
        &self.principal
    }

    pub const fn secret(&self) -> &SharedSecret {
        &self.secret
    }

    pub const fn expires_at_unix_secs(&self) -> Option<u64> {
        self.expires_at_unix_secs
    }

    pub const fn revoked(&self) -> bool {
        self.revoked
    }

    pub const fn revocation_grace_secs(&self) -> u64 {
        self.revocation_grace_secs
    }
}

impl std::fmt::Debug for CredentialRecord {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CredentialRecord")
            .field("id", &self.id)
            .field("principal", &self.principal)
            .field("secret", &"<redacted>")
            .field("expires_at_unix_secs", &self.expires_at_unix_secs)
            .field("revoked", &self.revoked)
            .field("revocation_grace_secs", &self.revocation_grace_secs)
            .finish()
    }
}

/// Global immutable credential catalog. MPP inbounds and outbounds reference it
/// by ID instead of embedding key material.
#[derive(Clone, PartialEq, Eq)]
pub struct CredentialCatalog {
    records: Arc<HashMap<CredentialId, Arc<CredentialRecord>>>,
}

impl CredentialCatalog {
    pub fn compile(
        records: impl IntoIterator<Item = CredentialRecord>,
    ) -> Result<Self, CredentialCatalogError> {
        let mut compiled = HashMap::new();
        for record in records {
            if compiled.len() >= MAX_CREDENTIALS {
                return Err(CredentialCatalogError::TooManyCredentials {
                    limit: MAX_CREDENTIALS,
                });
            }
            let id = record.id.clone();
            if compiled.insert(id.clone(), Arc::new(record)).is_some() {
                return Err(CredentialCatalogError::DuplicateCredential(id));
            }
        }
        Ok(Self {
            records: Arc::new(compiled),
        })
    }

    pub fn credential(
        &self,
        id: &CredentialId,
    ) -> Result<Arc<CredentialRecord>, CredentialCatalogError> {
        self.records
            .get(id)
            .cloned()
            .ok_or_else(|| CredentialCatalogError::MissingCredential(id.clone()))
    }

    pub fn authority(
        &self,
        ids: &[CredentialId],
    ) -> Result<CredentialAuthority, CredentialCatalogError> {
        if ids.is_empty() {
            return Err(CredentialCatalogError::EmptyAuthority);
        }
        let mut selected = HashMap::with_capacity(ids.len());
        let mut unique = HashSet::with_capacity(ids.len());
        for id in ids {
            if !unique.insert(id.clone()) {
                return Err(CredentialCatalogError::DuplicateAuthorityCredential(
                    id.clone(),
                ));
            }
            selected.insert(id.clone(), self.credential(id)?);
        }
        Ok(CredentialAuthority {
            records: Arc::new(selected),
        })
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

impl std::fmt::Debug for CredentialCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CredentialCatalog")
            .field("credential_count", &self.records.len())
            .finish()
    }
}

/// Immutable credential subset accepted by one MPP inbound.
#[derive(Clone, PartialEq, Eq)]
pub struct CredentialAuthority {
    records: Arc<HashMap<CredentialId, Arc<CredentialRecord>>>,
}

impl CredentialAuthority {
    /// O(1) lookup and status validation performed before the HMAC check.
    ///
    /// Callers must not expose the exact error to an unauthenticated peer.
    pub fn candidate(
        &self,
        id: &CredentialId,
        now_unix_secs: u64,
    ) -> Result<CredentialCandidate, CredentialAdmissionError> {
        let record = self
            .records
            .get(id)
            .cloned()
            .ok_or(CredentialAdmissionError::UnknownCredential)?;
        if record.revoked {
            return Err(CredentialAdmissionError::Revoked);
        }
        if record
            .expires_at_unix_secs
            .is_some_and(|expires_at| now_unix_secs >= expires_at)
        {
            return Err(CredentialAdmissionError::Expired);
        }
        Ok(CredentialCandidate { record })
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub(crate) fn credential(&self, id: &CredentialId) -> Option<Arc<CredentialRecord>> {
        self.records.get(id).cloned()
    }

    pub(crate) fn credentials(&self) -> Vec<Arc<CredentialRecord>> {
        self.records.values().cloned().collect()
    }
}

impl std::fmt::Debug for CredentialAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CredentialAuthority")
            .field("credential_count", &self.records.len())
            .finish()
    }
}

/// Secret-bearing lookup result retained only through the authentication
/// flight. Debug output deliberately excludes the credential key.
#[derive(Clone)]
pub struct CredentialCandidate {
    record: Arc<CredentialRecord>,
}

impl CredentialCandidate {
    pub fn id(&self) -> &CredentialId {
        self.record.id()
    }

    pub fn principal(&self) -> &PrincipalId {
        self.record.principal()
    }

    pub fn secret(&self) -> &SharedSecret {
        self.record.secret()
    }

    /// Issues immutable authorization only after the handshake owner verifies
    /// the HMAC. Calling this method does not perform another policy lookup.
    pub fn into_permit(self, admitted_at_unix_secs: u64) -> PrincipalPermit {
        PrincipalPermit {
            credential_id: self.record.id.clone(),
            principal: self.record.principal.clone(),
            admitted_at_unix_secs,
            expires_at_unix_secs: self.record.expires_at_unix_secs,
            revocation_grace_secs: self.record.revocation_grace_secs,
        }
    }
}

impl std::fmt::Debug for CredentialCandidate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CredentialCandidate")
            .field("id", self.id())
            .field("principal", self.principal())
            .finish_non_exhaustive()
    }
}

/// Authorization attached to an authenticated session/path. Its identity and
/// expiry are immutable; runtime actors may schedule one expiry timer from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrincipalPermit {
    credential_id: CredentialId,
    principal: PrincipalId,
    admitted_at_unix_secs: u64,
    expires_at_unix_secs: Option<u64>,
    revocation_grace_secs: u64,
}

impl PrincipalPermit {
    pub const fn credential_id(&self) -> &CredentialId {
        &self.credential_id
    }

    pub const fn principal(&self) -> &PrincipalId {
        &self.principal
    }

    pub const fn admitted_at_unix_secs(&self) -> u64 {
        self.admitted_at_unix_secs
    }

    pub const fn expires_at_unix_secs(&self) -> Option<u64> {
        self.expires_at_unix_secs
    }

    pub const fn revocation_grace_secs(&self) -> u64 {
        self.revocation_grace_secs
    }

    pub fn forced_close_at_unix_secs(&self) -> Option<u64> {
        self.expires_at_unix_secs
            .map(|expires_at| expires_at.saturating_add(self.revocation_grace_secs))
    }

    pub fn same_principal(&self, other: &Self) -> bool {
        self.principal == other.principal
    }

    #[cfg(test)]
    pub(crate) fn for_test(principal: &str) -> Self {
        Self {
            credential_id: CredentialId::parse("test-credential")
                .expect("static test credential ID"),
            principal: PrincipalId::parse(principal).expect("static test principal"),
            admitted_at_unix_secs: 1,
            expires_at_unix_secs: None,
            revocation_grace_secs: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialCatalogError {
    TooManyCredentials { limit: usize },
    DuplicateCredential(CredentialId),
    MissingCredential(CredentialId),
    DuplicateAuthorityCredential(CredentialId),
    InvalidExpiration(CredentialId),
    EmptyAuthority,
}

impl std::fmt::Display for CredentialCatalogError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooManyCredentials { limit } => {
                write!(formatter, "credential catalog exceeds limit {limit}")
            }
            Self::DuplicateCredential(id) => write!(formatter, "duplicate credential {id}"),
            Self::MissingCredential(id) => write!(formatter, "missing credential {id}"),
            Self::DuplicateAuthorityCredential(id) => {
                write!(formatter, "credential {id} is referenced more than once")
            }
            Self::InvalidExpiration(id) => {
                write!(
                    formatter,
                    "credential {id} expiration must be a Unix timestamp from 1 through {MAX_CREDENTIAL_EXPIRY_UNIX_SECS}"
                )
            }
            Self::EmptyAuthority => formatter.write_str("MPP inbound requires credentials"),
        }
    }
}

impl std::error::Error for CredentialCatalogError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialAdmissionError {
    UnknownCredential,
    Revoked,
    Expired,
    Overloaded,
}

impl std::fmt::Display for CredentialAdmissionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownCredential => formatter.write_str("unknown credential"),
            Self::Revoked => formatter.write_str("credential is revoked"),
            Self::Expired => formatter.write_str("credential is expired"),
            Self::Overloaded => formatter.write_str("credential admission is overloaded"),
        }
    }
}

impl std::error::Error for CredentialAdmissionError {}

#[cfg(test)]
#[path = "identity_test.rs"]
mod tests;
