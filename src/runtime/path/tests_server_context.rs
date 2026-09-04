use super::*;
use crate::mux::MuxLimits;
use crate::product::{CredentialCatalog, CredentialRecord, PrincipalId, SharedSecret};
use crate::protocol::Frame;
use crate::runtime::path::proof::PathProofTracker;
use bytes::Bytes;

fn credential(
    id: &str,
    principal: &str,
    secret_byte: u8,
    revoked: bool,
    grace_secs: u64,
) -> CredentialRecord {
    CredentialRecord::new(
        CredentialId::parse(id).expect("credential ID"),
        PrincipalId::parse(principal).expect("principal ID"),
        SharedSecret::new(vec![secret_byte; SharedSecret::MIN_BYTES]).expect("secret"),
        None,
        revoked,
        grace_secs,
    )
    .expect("credential")
}

fn authority(records: Vec<CredentialRecord>, ids: &[&str]) -> CredentialAuthority {
    let catalog = CredentialCatalog::compile(records).expect("credential catalog");
    let ids = ids
        .iter()
        .map(|id| CredentialId::parse(id).expect("credential ID"))
        .collect::<Vec<_>>();
    catalog.authority(&ids).expect("credential authority")
}

#[test]
fn local_listener_configuration_is_independent_of_peer_path_id() {
    let spec = "quic://127.0.0.1:12900?initial-srtt-s=0.073&initial-rate-mbps=420&backup=true&allow-bulk=false"
        .parse::<PathSpec>()
        .expect("server UDP path");
    let local = ServerLocalPath::new(7, spec);
    let metrics = local.startup_metrics(PathId(0));

    assert_eq!(local.config_ordinal(), 7);
    assert_eq!(local.underlay(), UnderlayProtocol::Udp);
    assert_eq!(local.advertised_usage(), crate::protocol::PathUsage::Backup);
    assert!(local.policy().backup);
    assert!(!local.policy().bulk_allowed);
    assert_eq!(metrics.path_id, PathId(0));
    assert_eq!(metrics.underlay, UnderlayProtocol::Udp);
    assert_eq!(metrics.srtt_us, 73_000);
    assert_eq!(
        local.startup_rate_prior(),
        RateHint::BitsPerSecond(420_000_000)
    );
    assert_eq!(
        metrics.delivery_rate_bps,
        crate::model::service_rate::portable_startup_rate()
            .expect("portable PATH_METRICS placeholder")
            .get(),
        "endpoint-local configured rate remains local policy and is not serialized as peer-observed PATH_METRICS"
    );
    assert!(!metrics.rate_observed);
}

#[tokio::test]
async fn live_revocation_retires_only_the_matching_credential() {
    let current = authority(
        vec![
            credential("home-old", "home", 1, false, 0),
            credential("home-next", "home", 2, false, 0),
        ],
        &["home-old", "home-next"],
    );
    let next = authority(
        vec![
            credential("home-old", "home", 1, true, 0),
            credential("home-next", "home", 2, false, 0),
        ],
        &["home-old", "home-next"],
    );
    let old_id = CredentialId::parse("home-old").expect("credential ID");
    let next_id = CredentialId::parse("home-next").expect("credential ID");
    let old_permit = current
        .candidate(&old_id, 99)
        .expect("old credential")
        .into_permit(99);
    let next_permit = current
        .candidate(&next_id, 99)
        .expect("replacement credential")
        .into_permit(99);
    let retirement = CredentialRetirementControl::new();
    let deadlines = credential_authority_retirements(&current, &next, &retirement, 100)
        .expect("valid live credential publication");
    assert_eq!(deadlines, vec![(old_id, 100)]);
    retirement.publish(deadlines);

    tokio::time::timeout(Duration::from_millis(50), retirement.wait_for(old_permit))
        .await
        .expect("revoked credential retires promptly");
    assert!(
        tokio::time::timeout(Duration::from_millis(20), retirement.wait_for(next_permit))
            .await
            .is_err(),
        "overlapping replacement credential must remain active"
    );
}

#[test]
fn live_publication_rejects_in_place_identity_or_key_mutation() {
    let current = authority(
        vec![credential("device-a", "home", 1, false, 0)],
        &["device-a"],
    );
    let changed_key = authority(
        vec![credential("device-a", "home", 2, false, 0)],
        &["device-a"],
    );
    let changed_principal = authority(
        vec![credential("device-a", "guest", 1, false, 0)],
        &["device-a"],
    );
    let retirement = CredentialRetirementControl::new();

    assert_eq!(
        credential_authority_retirements(&current, &changed_key, &retirement, 100),
        Err("credential principal or secret changes require a new credential ID")
    );
    assert_eq!(
        credential_authority_retirements(&current, &changed_principal, &retirement, 100),
        Err("credential principal or secret changes require a new credential ID")
    );
}

#[test]
fn unanswered_path_proof_correlations_follow_the_ack_control_envelope() {
    let limits = MuxLimits {
        max_ack_ranges: 2,
        ..MuxLimits::default()
    };
    let mut proofs = PathProofTracker::from_limits(limits);
    for proof_id in 1..=3 {
        proofs.record_sent_frame(&Frame::PathProofData {
            path_id: PathId(0),
            proof_id,
            payload: Bytes::from_static(b"proof"),
        });
    }

    assert_eq!(proofs.pending_len(), 2);
    assert!(
        proofs.acknowledge(PathId(0), 1, 5).is_none(),
        "the oldest unanswered proof must be evicted at the control-state ceiling"
    );
    assert!(
        proofs.acknowledge(PathId(0), 3, 5).is_some(),
        "a current proof remains usable after bounded eviction"
    );
}
