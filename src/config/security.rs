use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionMode {
    #[default]
    Required,
    AllowPlaintextLab,
}

impl EncryptionMode {
    pub fn permits_plaintext(self) -> bool {
        matches!(self, Self::AllowPlaintextLab)
    }

    pub fn plaintext_warning(self) -> Option<&'static str> {
        self.permits_plaintext().then_some(
            "plaintext transport is enabled for lab use; internal traffic is not confidential",
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportSecurity {
    Encrypted,
    Plaintext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportIntegrity {
    Authenticated,
}

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

pub fn validate_transport_security(
    mode: EncryptionMode,
    security: TransportSecurity,
    integrity: TransportIntegrity,
    secret: &SharedSecret,
) -> Result<(), SecurityPolicyError> {
    if !matches!(integrity, TransportIntegrity::Authenticated) {
        return Err(SecurityPolicyError::IntegrityRequired);
    }
    if secret.as_bytes().len() < SharedSecret::DERIVED_BYTES {
        return Err(SecurityPolicyError::SecretTooShort {
            actual: secret.as_bytes().len(),
            minimum: SharedSecret::DERIVED_BYTES,
        });
    }
    match (mode, security) {
        (_, TransportSecurity::Encrypted) => Ok(()),
        (EncryptionMode::AllowPlaintextLab, TransportSecurity::Plaintext) => Ok(()),
        (EncryptionMode::Required, TransportSecurity::Plaintext) => {
            Err(SecurityPolicyError::PlaintextRejected)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityPolicyError {
    PlaintextRejected,
    IntegrityRequired,
    MissingSecret,
    SecretTooShort { actual: usize, minimum: usize },
}

impl std::fmt::Display for SecurityPolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PlaintextRejected => {
                write!(f, "plaintext transport requires explicit insecure lab mode")
            }
            Self::IntegrityRequired => write!(f, "session/path integrity is required"),
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
mod tests {
    use super::*;

    #[test]
    fn encryption_is_required_by_default() {
        let mode = EncryptionMode::default();
        let secret =
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret");

        assert_eq!(
            validate_transport_security(
                mode,
                TransportSecurity::Plaintext,
                TransportIntegrity::Authenticated,
                &secret
            ),
            Err(SecurityPolicyError::PlaintextRejected)
        );
        assert!(
            validate_transport_security(
                mode,
                TransportSecurity::Encrypted,
                TransportIntegrity::Authenticated,
                &secret
            )
            .is_ok()
        );
    }

    #[test]
    fn plaintext_requires_explicit_lab_mode_warning_and_authenticated_integrity() {
        let mode = EncryptionMode::AllowPlaintextLab;
        let secret =
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret");

        assert!(
            validate_transport_security(
                mode,
                TransportSecurity::Plaintext,
                TransportIntegrity::Authenticated,
                &secret
            )
            .is_ok()
        );
        assert!(mode.plaintext_warning().is_some());
    }

    #[test]
    fn shared_secret_is_redacted_and_minimum_sized() {
        assert_eq!(
            SharedSecret::new(b"short".to_vec()),
            Err(SecurityPolicyError::SecretTooShort {
                actual: 5,
                minimum: SharedSecret::MIN_BYTES
            })
        );

        let secret =
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret");
        assert_eq!(format!("{secret:?}"), "SharedSecret(<redacted>)");
        assert_eq!(secret.as_bytes().len(), SharedSecret::DERIVED_BYTES);
    }

    #[test]
    fn shared_secret_accepts_uuid_and_derives_master_material() {
        let uuid_secret = SharedSecret::new(b"6ba7b810-9dad-11d1-80b4-00c04fd430c8".to_vec())
            .expect("uuid secret");
        let raw_secret =
            SharedSecret::new(b"raw-secret-material-with-at-least-32-bytes".to_vec()).expect("raw");

        assert_eq!(uuid_secret.as_bytes().len(), SharedSecret::DERIVED_BYTES);
        assert_eq!(raw_secret.as_bytes().len(), SharedSecret::DERIVED_BYTES);
        assert_ne!(uuid_secret.as_bytes(), raw_secret.as_bytes());
    }

    #[test]
    fn aes_256_gcm_is_default_cipher_suite() {
        assert_eq!(CipherSuite::default(), CipherSuite::Aes256Gcm);
        assert_ne!(
            CipherSuite::Aes256Gcm.key_context(),
            CipherSuite::Chacha20Poly1305.key_context()
        );
    }
}
