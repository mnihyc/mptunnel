use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum CipherSuite {
    #[default]
    Aes256Gcm,
    Chacha20Poly1305,
}

impl CipherSuite {
    pub fn key_context(self) -> &'static [u8] {
        match self {
            Self::Aes256Gcm => b"aes-256-gcm",
            Self::Chacha20Poly1305 => b"chacha20-poly1305",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SharedSecret(Vec<u8>);

impl SharedSecret {
    pub const MIN_BYTES: usize = 32;
    pub const DERIVED_BYTES: usize = 32;

    pub fn new(value: impl Into<Vec<u8>>) -> Result<Self, SecurityPolicyError> {
        let value = value.into();
        if let Some(uuid_bytes) = parse_uuid_secret(&value) {
            return Ok(Self(derive_secret_material(b"uuid", &uuid_bytes)));
        }
        if value.len() < Self::MIN_BYTES {
            return Err(SecurityPolicyError::SecretTooShort {
                actual: value.len(),
                minimum: Self::MIN_BYTES,
            });
        }
        Ok(Self(derive_secret_material(b"raw", &value)))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

fn parse_uuid_secret(value: &[u8]) -> Option<[u8; 16]> {
    let text = std::str::from_utf8(value).ok()?.trim();
    let uuid = Uuid::parse_str(text).ok()?;
    Some(*uuid.as_bytes())
}

fn derive_secret_material(kind: &[u8], value: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(b"mptunnel shared secret master v1");
    hasher.update(kind);
    hasher.update(value);
    hasher.finalize().to_vec()
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
            Self::MissingSecret => write!(f, "shared secret is required"),
            Self::SecretTooShort { actual, minimum } => {
                write!(
                    f,
                    "shared secret is {actual} bytes, minimum is {minimum} bytes or a UUID"
                )
            }
        }
    }
}

impl std::error::Error for SecurityPolicyError {}

#[cfg(test)]
#[path = "security_test.rs"]
mod tests;
