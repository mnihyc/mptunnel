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

#[derive(Clone, PartialEq, Eq)]
pub struct SharedSecret(Vec<u8>);

impl SharedSecret {
    pub const MIN_BYTES: usize = 16;

    pub fn new(value: impl Into<Vec<u8>>) -> Result<Self, SecurityPolicyError> {
        let value = value.into();
        if value.len() < Self::MIN_BYTES {
            return Err(SecurityPolicyError::SecretTooShort {
                actual: value.len(),
                minimum: Self::MIN_BYTES,
            });
        }
        Ok(Self(value))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
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
    if secret.as_bytes().len() < SharedSecret::MIN_BYTES {
        return Err(SecurityPolicyError::SecretTooShort {
            actual: secret.as_bytes().len(),
            minimum: SharedSecret::MIN_BYTES,
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
                write!(f, "shared secret is {actual} bytes, minimum is {minimum}")
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
        let secret = SharedSecret::new(b"0123456789abcdef".to_vec()).expect("secret");

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
        let secret = SharedSecret::new(b"0123456789abcdef".to_vec()).expect("secret");

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

        let secret = SharedSecret::new(b"0123456789abcdef".to_vec()).expect("secret");
        assert_eq!(format!("{secret:?}"), "SharedSecret(<redacted>)");
    }
}
