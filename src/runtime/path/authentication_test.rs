use super::*;
use crate::config::{ClientSecurityConfig, ServerSecurityConfig, SharedSecret};
use crate::product::{CredentialAuthority, CredentialCatalog, CredentialRecord, PrincipalId};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

const SESSION_ID: SessionId = SessionId(41);
const PATH_ID: PathId = PathId(7);

fn security(freshness_window_secs: u64) -> (ClientSecurityConfig, ServerSecurityConfig) {
    let secret =
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("test secret");
    (
        ClientSecurityConfig::for_test(secret.clone())
            .with_auth_freshness_window(Duration::from_secs(freshness_window_secs)),
        ServerSecurityConfig::for_test(secret)
            .with_auth_freshness_window(Duration::from_secs(freshness_window_secs)),
    )
}

fn pending(security: &ServerSecurityConfig) -> ServerPathAuthentication {
    ServerPathAuthentication::from_session_hello(
        security,
        ProductCredentialAdmission::from_security(security),
        Frame::SessionHello {
            session_id: SESSION_ID,
        },
    )
    .expect("authentication state")
    .expect("SESSION_HELLO")
}

fn session_auth_frame(security: &ClientSecurityConfig, issued_at_unix_secs: u64) -> Frame {
    let nonce = AuthNonce([3; 16]);
    let credential_id = security.credential.id().as_str();
    let authenticator =
        SessionAuthenticator::new(security.credential.secret().as_bytes()).expect("authenticator");
    Frame::SessionAuth {
        session_id: SESSION_ID,
        credential_id: credential_id.to_string(),
        nonce,
        issued_at_unix_secs,
        auth_tag: authenticator.session_auth_tag(
            SESSION_ID,
            credential_id,
            nonce,
            issued_at_unix_secs,
        ),
    }
}

fn authenticated_session(
    client: &ClientSecurityConfig,
    server: &ServerSecurityConfig,
    issued_at_unix_secs: u64,
    now_unix_secs: u64,
) -> Option<AuthenticatedServerPathSession> {
    pending(server)
        .authenticate_session_at(
            session_auth_frame(client, issued_at_unix_secs),
            now_unix_secs,
        )
        .expect("credential admission")
}

fn path_join_frame(security: &ClientSecurityConfig, issued_at_unix_secs: u64) -> Frame {
    path_join_frame_with_purpose(security, issued_at_unix_secs, PathPurpose::Ordinary)
}

fn path_join_frame_with_purpose(
    security: &ClientSecurityConfig,
    issued_at_unix_secs: u64,
    purpose: PathPurpose,
) -> Frame {
    let nonce = AuthNonce([5; 16]);
    let credential_id = security.credential.id().as_str();
    let authenticator =
        SessionAuthenticator::new(security.credential.secret().as_bytes()).expect("authenticator");
    Frame::PathJoin {
        session_id: SESSION_ID,
        credential_id: credential_id.to_string(),
        path_id: PATH_ID,
        underlay: UnderlayProtocol::Tcp,
        purpose,
        nonce,
        issued_at_unix_secs,
        auth_tag: authenticator.path_join_tag(
            SESSION_ID,
            credential_id,
            PATH_ID,
            UnderlayProtocol::Tcp,
            purpose,
            nonce,
            issued_at_unix_secs,
        ),
    }
}

#[derive(Debug)]
struct RecordingAdmission {
    authority: CredentialAuthority,
    candidate_calls: AtomicUsize,
    admit_calls: AtomicUsize,
}

impl CredentialAdmissionPort for RecordingAdmission {
    fn candidate(
        &self,
        credential_id: &CredentialId,
        now_unix_secs: u64,
    ) -> Result<CredentialCandidate, CredentialAdmissionError> {
        self.candidate_calls.fetch_add(1, Ordering::Relaxed);
        self.authority.candidate(credential_id, now_unix_secs)
    }

    fn admit(
        &self,
        candidate: CredentialCandidate,
        _session_id: SessionId,
        now_unix_secs: u64,
    ) -> Result<PrincipalPermit, CredentialAdmissionError> {
        self.admit_calls.fetch_add(1, Ordering::Relaxed);
        Ok(candidate.into_permit(now_unix_secs))
    }
}

#[test]
fn product_admission_is_handshake_only_and_rejections_never_issue_a_permit() {
    let id = CredentialId::parse("recorded-client").expect("credential ID");
    let secret = SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret");
    let record = CredentialRecord::new(
        id.clone(),
        PrincipalId::parse("recorded-peer").expect("principal ID"),
        secret,
        None,
        false,
        0,
    )
    .expect("credential");
    let catalog = CredentialCatalog::compile([record]).expect("catalog");
    let client = ClientSecurityConfig::new(catalog.credential(&id).expect("client credential"));
    let authority = catalog
        .authority(std::slice::from_ref(&id))
        .expect("authority");
    let server = ServerSecurityConfig::new(authority.clone());
    let admission = Arc::new(RecordingAdmission {
        authority,
        candidate_calls: AtomicUsize::new(0),
        admit_calls: AtomicUsize::new(0),
    });

    let mut wrong_mac = session_auth_frame(&client, 100);
    let Frame::SessionAuth { auth_tag, .. } = &mut wrong_mac else {
        unreachable!("test helper returns SESSION_AUTH");
    };
    auth_tag.0[0] ^= 1;
    let rejected = ServerPathAuthentication::from_session_hello(
        &server,
        admission.clone(),
        Frame::SessionHello {
            session_id: SESSION_ID,
        },
    )
    .expect("authentication state")
    .expect("SESSION_HELLO")
    .authenticate_session_at(wrong_mac, 100)
    .expect("credential lookup");
    assert!(rejected.is_none());
    assert_eq!(admission.candidate_calls.load(Ordering::Relaxed), 1);
    assert_eq!(admission.admit_calls.load(Ordering::Relaxed), 0);

    let mut unknown = session_auth_frame(&client, 100);
    let Frame::SessionAuth { credential_id, .. } = &mut unknown else {
        unreachable!("test helper returns SESSION_AUTH");
    };
    *credential_id = "unknown-client".to_string();
    let rejected = ServerPathAuthentication::from_session_hello(
        &server,
        admission.clone(),
        Frame::SessionHello {
            session_id: SESSION_ID,
        },
    )
    .expect("authentication state")
    .expect("SESSION_HELLO")
    .authenticate_session_at(unknown, 100)
    .expect("unknown credential follows the uniform rejection path");
    assert!(rejected.is_none());
    assert_eq!(admission.candidate_calls.load(Ordering::Relaxed), 2);
    assert_eq!(admission.admit_calls.load(Ordering::Relaxed), 0);

    let authenticated = ServerPathAuthentication::from_session_hello(
        &server,
        admission.clone(),
        Frame::SessionHello {
            session_id: SESSION_ID,
        },
    )
    .expect("authentication state")
    .expect("SESSION_HELLO")
    .authenticate_session_at(session_auth_frame(&client, 100), 100)
    .expect("credential admission")
    .expect("valid SESSION_AUTH");
    assert_eq!(admission.candidate_calls.load(Ordering::Relaxed), 3);
    assert_eq!(admission.admit_calls.load(Ordering::Relaxed), 1);

    assert!(
        authenticated
            .authenticate_path_join_at(UnderlayProtocol::Tcp, path_join_frame(&client, 100), 100,)
            .is_some()
    );
    assert_eq!(admission.candidate_calls.load(Ordering::Relaxed), 3);
    assert_eq!(admission.admit_calls.load(Ordering::Relaxed), 1);
}

#[test]
fn authority_publication_closes_the_candidate_to_admit_race() {
    let retired_id = CredentialId::parse("retired").expect("credential ID");
    let replacement_id = CredentialId::parse("replacement").expect("credential ID");
    let old_catalog = CredentialCatalog::compile([CredentialRecord::new(
        retired_id.clone(),
        PrincipalId::parse("home").expect("principal ID"),
        SharedSecret::new(vec![7; SharedSecret::MIN_BYTES]).expect("secret"),
        None,
        false,
        0,
    )
    .expect("credential")])
    .expect("catalog");
    let old_authority = old_catalog
        .authority(std::slice::from_ref(&retired_id))
        .expect("old authority");
    let server = ServerSecurityConfig::new(old_authority);
    let admission = ProductCredentialAdmission::from_security(&server);
    let stale_candidate = admission
        .candidate(&retired_id, 100)
        .expect("candidate before publication");

    let next_catalog = CredentialCatalog::compile([CredentialRecord::new(
        replacement_id.clone(),
        PrincipalId::parse("home").expect("principal ID"),
        SharedSecret::new(vec![8; SharedSecret::MIN_BYTES]).expect("secret"),
        None,
        false,
        0,
    )
    .expect("credential")])
    .expect("catalog");
    admission.replace_authority(
        next_catalog
            .authority(std::slice::from_ref(&replacement_id))
            .expect("replacement authority"),
    );

    assert!(matches!(
        admission.admit(stale_candidate, SESSION_ID, 100),
        Err(CredentialAdmissionError::UnknownCredential)
    ));
}

#[test]
fn quic_candidate_verifier_tracks_only_the_current_active_authority() {
    let active_id = CredentialId::parse("active").expect("active credential ID");
    let revoked_id = CredentialId::parse("revoked").expect("revoked credential ID");
    let active_secret = SharedSecret::new(vec![3; SharedSecret::MIN_BYTES]).expect("active secret");
    let revoked_secret =
        SharedSecret::new(vec![4; SharedSecret::MIN_BYTES]).expect("revoked secret");
    let active_selector =
        QuicCandidateSelector::derive(active_id.as_str(), active_secret.as_bytes());
    let revoked_selector =
        QuicCandidateSelector::derive(revoked_id.as_str(), revoked_secret.as_bytes());
    let catalog = CredentialCatalog::compile([
        CredentialRecord::new(
            active_id.clone(),
            PrincipalId::parse("home").expect("principal ID"),
            active_secret,
            None,
            false,
            0,
        )
        .expect("active credential"),
        CredentialRecord::new(
            revoked_id.clone(),
            PrincipalId::parse("home").expect("principal ID"),
            revoked_secret,
            None,
            true,
            0,
        )
        .expect("revoked credential"),
    ])
    .expect("catalog");
    let server = ServerSecurityConfig::new(
        catalog
            .authority(&[active_id.clone(), revoked_id])
            .expect("initial authority"),
    );
    let admission = ProductCredentialAdmission::from_security(&server);

    assert!(admission.accepts(&active_selector));
    assert!(!admission.accepts(&revoked_selector));

    let replacement_id = CredentialId::parse("replacement").expect("replacement credential ID");
    let replacement_secret =
        SharedSecret::new(vec![5; SharedSecret::MIN_BYTES]).expect("replacement secret");
    let replacement_selector =
        QuicCandidateSelector::derive(replacement_id.as_str(), replacement_secret.as_bytes());
    let replacement_catalog = CredentialCatalog::compile([CredentialRecord::new(
        replacement_id.clone(),
        PrincipalId::parse("home").expect("principal ID"),
        replacement_secret,
        None,
        false,
        0,
    )
    .expect("replacement credential")])
    .expect("replacement catalog");
    admission.replace_authority(
        replacement_catalog
            .authority(std::slice::from_ref(&replacement_id))
            .expect("replacement authority"),
    );

    assert!(!admission.accepts(&active_selector));
    assert!(admission.accepts(&replacement_selector));
}

#[test]
fn auth_and_join_decisions_do_not_reuse_an_earlier_clock_sample() {
    let (client, server) = security(10);
    let authenticated =
        authenticated_session(&client, &server, 100, 100).expect("fresh SESSION_AUTH");

    assert!(
        authenticated
            .authenticate_path_join_at(UnderlayProtocol::Tcp, path_join_frame(&client, 100), 111,)
            .is_none(),
        "a PATH_JOIN delayed beyond freshness must not inherit SESSION_AUTH time"
    );
}

#[test]
fn session_auth_rejects_stale_and_too_far_future_timestamps() {
    let (client, server) = security(10);

    assert!(authenticated_session(&client, &server, 100, 111).is_none());
    assert!(authenticated_session(&client, &server, 100, 89).is_none());
    assert!(authenticated_session(&client, &server, 100, 110).is_some());
    assert!(authenticated_session(&client, &server, 100, 90).is_some());
}

#[test]
fn authenticated_path_join_preserves_its_signed_issue_time_and_purpose() {
    let (client, server) = security(10);
    let authenticated =
        authenticated_session(&client, &server, 100, 105).expect("fresh SESSION_AUTH");

    let joined = authenticated
        .authenticate_path_join_at(
            UnderlayProtocol::Tcp,
            path_join_frame_with_purpose(&client, 104, PathPurpose::Validation),
            106,
        )
        .expect("fresh PATH_JOIN");

    assert_eq!(joined.session_id, SESSION_ID);
    assert_eq!(joined.path_id, PATH_ID);
    assert_eq!(joined.purpose, PathPurpose::Validation);
    assert_eq!(joined.issued_at_unix_secs, 104);
    assert_eq!(joined.verified_at_unix_secs, 106);
}

#[test]
fn path_join_rejects_a_purpose_changed_after_authentication() {
    let (client, server) = security(10);
    let authenticated =
        authenticated_session(&client, &server, 100, 100).expect("fresh SESSION_AUTH");
    let mut path_join = path_join_frame(&client, 100);
    let Frame::PathJoin { purpose, .. } = &mut path_join else {
        unreachable!("helper returns PATH_JOIN");
    };
    *purpose = PathPurpose::Validation;

    assert!(
        authenticated
            .authenticate_path_join_at(UnderlayProtocol::Tcp, path_join, 100)
            .is_none()
    );
}
