use super::*;
use crate::config::{ClientSecurityConfig, ServerSecurityConfig, SharedSecret};
use crate::runtime::path::authentication::ProductCredentialAdmission;

fn security() -> (ClientSecurityConfig, ServerSecurityConfig) {
    let secret =
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("test secret");
    (
        ClientSecurityConfig::for_test(secret.clone()),
        ServerSecurityConfig::for_test(secret),
    )
}

#[test]
fn fixed_tcp_prelude_authenticates_session_and_then_distinct_path_join() {
    let (client, server) = security();
    let exporter = [7; 32];
    let session_id = SessionId(41);
    let path_id = PathId(9);
    let (prelude, path_join) =
        ClientTcpPathAuthentication::for_session(&client, path_id, session_id, &exporter)
            .expect("client TCP admission")
            .into_parts();

    assert_eq!(prelude.len(), TCP_ADMISSION_PRELUDE_LEN);
    assert_eq!(prelude[ROLE_OFFSET], CLIENT_ROLE);
    assert_eq!(prelude[DIRECTION_OFFSET], CLIENT_TO_SERVER);
    let credential_length = usize::from(prelude[CREDENTIAL_LENGTH_OFFSET]);
    assert_eq!(
        &prelude[CREDENTIAL_OFFSET..CREDENTIAL_OFFSET + credential_length],
        client.credential.id().as_str().as_bytes()
    );
    assert!(
        prelude[CREDENTIAL_OFFSET + credential_length..SESSION_ID_OFFSET]
            .iter()
            .all(|byte| *byte == 0),
        "fixed credential field uses canonical zero padding"
    );

    let authenticated = authenticate_prelude(
        &server,
        ProductCredentialAdmission::from_security(&server),
        &prelude,
        &exporter,
    )
    .expect("server admission")
    .expect("valid TLS-bound prelude");
    let joined = authenticated
        .authenticate_path_join(UnderlayProtocol::Tcp, path_join)
        .expect("path authentication")
        .expect("separately authenticated PATH_JOIN");
    assert_eq!(joined.session_id, session_id);
    assert_eq!(joined.path_id, path_id);
    assert_eq!(joined.purpose, PathPurpose::Ordinary);
}

#[test]
fn tcp_prelude_rejects_cross_connection_replay_and_malformed_fixed_fields() {
    let (client, server) = security();
    let exporter = [11; 32];
    let (prelude, _) =
        ClientTcpPathAuthentication::for_session(&client, PathId(2), SessionId(8), &exporter)
            .expect("client TCP admission")
            .into_parts();

    assert!(
        authenticate_prelude(
            &server,
            ProductCredentialAdmission::from_security(&server),
            &prelude,
            &[12; 32],
        )
        .expect("cross-connection decision")
        .is_none(),
        "a valid credential prelude cannot be replayed under another TLS exporter"
    );

    for offset in [ROLE_OFFSET, DIRECTION_OFFSET, TAG_OFFSET] {
        let mut malformed = prelude;
        malformed[offset] ^= 0x40;
        assert!(
            authenticate_prelude(
                &server,
                ProductCredentialAdmission::from_security(&server),
                &malformed,
                &exporter,
            )
            .expect("malformed decision")
            .is_none()
        );
    }

    let mut noncanonical_padding = prelude;
    let credential_length = usize::from(noncanonical_padding[CREDENTIAL_LENGTH_OFFSET]);
    noncanonical_padding[CREDENTIAL_OFFSET + credential_length] = 1;
    assert!(
        authenticate_prelude(
            &server,
            ProductCredentialAdmission::from_security(&server),
            &noncanonical_padding,
            &exporter,
        )
        .expect("padding decision")
        .is_none()
    );
}
