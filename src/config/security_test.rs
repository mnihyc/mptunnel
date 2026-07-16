use super::*;

#[test]
fn shared_secret_is_redacted_and_minimum_sized() {
    assert_eq!(
        SharedSecret::new(b"short".to_vec()),
        Err(SecurityPolicyError::SecretTooShort {
            actual: 5,
            minimum: SharedSecret::MIN_BYTES
        })
    );

    let secret = SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret");
    assert_eq!(format!("{secret:?}"), "SharedSecret(<redacted>)");
    assert_eq!(secret.as_bytes().len(), SharedSecret::DERIVED_BYTES);
}

#[test]
fn shared_secret_accepts_uuid_and_derives_master_material() {
    let uuid_secret =
        SharedSecret::new(b"6ba7b810-9dad-11d1-80b4-00c04fd430c8".to_vec()).expect("uuid secret");
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
