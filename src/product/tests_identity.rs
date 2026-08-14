use super::*;

fn credential(
    id: &str,
    principal: &str,
    secret_byte: u8,
    expires_at_unix_secs: Option<u64>,
    revoked: bool,
    revocation_grace_secs: u64,
) -> CredentialRecord {
    CredentialRecord::new(
        CredentialId::parse(id).expect("credential ID"),
        PrincipalId::parse(principal).expect("principal ID"),
        SharedSecret::new(vec![secret_byte; SharedSecret::MIN_BYTES]).expect("credential secret"),
        expires_at_unix_secs,
        revoked,
        revocation_grace_secs,
    )
    .expect("credential")
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

    let secret = SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret");
    assert_eq!(format!("{secret:?}"), "SharedSecret(<redacted>)");
    assert_eq!(secret.as_bytes().len(), SharedSecret::DERIVED_BYTES);
}

#[test]
fn shared_secret_accepts_uuid_and_derives_master_material() {
    let uuid = b"6ba7b810-9dad-11d1-80b4-00c04fd430c8";
    let uuid_secret = SharedSecret::new(uuid.to_vec()).expect("uuid secret");
    let uuid_with_newline =
        SharedSecret::new([uuid.as_slice(), b"\n"].concat()).expect("raw UUID bytes");
    let raw_secret =
        SharedSecret::new(b"raw-secret-material-with-at-least-32-bytes".to_vec()).expect("raw");

    assert_eq!(uuid_secret.as_bytes().len(), SharedSecret::DERIVED_BYTES);
    assert_eq!(raw_secret.as_bytes().len(), SharedSecret::DERIVED_BYTES);
    assert_ne!(uuid_secret.as_bytes(), raw_secret.as_bytes());
    assert_ne!(uuid_secret.as_bytes(), uuid_with_newline.as_bytes());
}

#[test]
fn rotation_overlap_preserves_principal_while_revocation_is_credential_scoped() {
    let old_id = CredentialId::parse("home-primary").expect("credential ID");
    let next_id = CredentialId::parse("home-next").expect("credential ID");
    let catalog = CredentialCatalog::compile([
        credential("home-primary", "home", 1, None, true, 30),
        credential("home-next", "home", 2, None, false, 30),
    ])
    .expect("credential catalog");
    let authority = catalog
        .authority(&[old_id.clone(), next_id.clone()])
        .expect("credential authority");

    assert!(matches!(
        authority.candidate(&old_id, 100),
        Err(CredentialAdmissionError::Revoked)
    ));
    let permit = authority
        .candidate(&next_id, 100)
        .expect("overlapping replacement credential")
        .into_permit(100);
    assert_eq!(permit.credential_id(), &next_id);
    assert_eq!(permit.principal().as_str(), "home");
    assert_eq!(permit.forced_close_at_unix_secs(), None);
}

#[test]
fn expiration_boundary_and_retirement_grace_are_explicit() {
    let id = CredentialId::parse("short-lived").expect("credential ID");
    let catalog = CredentialCatalog::compile([credential(
        "short-lived",
        "traveler",
        3,
        Some(1_000),
        false,
        15,
    )])
    .expect("credential catalog");
    let authority = catalog
        .authority(std::slice::from_ref(&id))
        .expect("authority");

    let permit = authority
        .candidate(&id, 999)
        .expect("credential is valid before its boundary")
        .into_permit(999);
    assert_eq!(permit.expires_at_unix_secs(), Some(1_000));
    assert_eq!(permit.forced_close_at_unix_secs(), Some(1_015));
    assert!(matches!(
        authority.candidate(&id, 1_000),
        Err(CredentialAdmissionError::Expired)
    ));
}

#[test]
fn catalog_rejects_ambiguous_or_impossible_identity_state_and_redacts_keys() {
    let record = credential("device-a", "home", 4, None, false, 0);
    let debug = format!("{record:?}");
    assert!(debug.contains("device-a"));
    assert!(!debug.contains(&"04".repeat(SharedSecret::MIN_BYTES)));

    assert!(matches!(
        CredentialCatalog::compile([record.clone(), record]),
        Err(CredentialCatalogError::DuplicateCredential(_))
    ));
    assert!(matches!(
        CredentialRecord::new(
            CredentialId::parse("invalid-expiry").expect("credential ID"),
            PrincipalId::parse("home").expect("principal ID"),
            SharedSecret::new(vec![5; SharedSecret::MIN_BYTES]).expect("secret"),
            Some(MAX_CREDENTIAL_EXPIRY_UNIX_SECS + 1),
            false,
            0,
        ),
        Err(CredentialCatalogError::InvalidExpiration(_))
    ));
}
