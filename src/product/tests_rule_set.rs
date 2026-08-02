use super::*;
use base64::engine::general_purpose::STANDARD as BASE64;
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::json;
use std::net::{IpAddr, Ipv4Addr};

const TEST_NOW: u64 = 1_800_000_000;

fn signed_artifact(payload: serde_json::Value) -> (Vec<u8>, RuleSetPublisherCatalog) {
    let payload = serde_json::to_vec(&payload).expect("payload JSON");
    let checksum: [u8; 32] = Sha256::digest(&payload).into();
    let seed = [7_u8; 32];
    let key = Ed25519KeyPair::from_seed_unchecked(&seed).expect("test key");
    let mut message = [0_u8; RULE_SET_SIGNATURE_CONTEXT.len() + 32];
    message[..RULE_SET_SIGNATURE_CONTEXT.len()].copy_from_slice(RULE_SET_SIGNATURE_CONTEXT);
    message[RULE_SET_SIGNATURE_CONTEXT.len()..].copy_from_slice(&checksum);
    let signature = key.sign(&message);
    let checksum_hex = checksum
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let envelope = json!({
        "schema": 1,
        "publisher": "official",
        "checksum_sha256": checksum_hex,
        "payload_base64": BASE64.encode(payload),
        "signature_base64": BASE64.encode(signature.as_ref()),
    });
    let public_key: [u8; 32] = key
        .public_key()
        .as_ref()
        .try_into()
        .expect("Ed25519 public key");
    let catalog = RuleSetPublisherCatalog::compile(vec![RuleSetPublisher::new(
        RuleSetPublisherId::parse("official").expect("publisher"),
        public_key,
    )])
    .expect("catalog");
    (
        serde_json::to_vec(&envelope).expect("envelope JSON"),
        catalog,
    )
}

fn payload() -> serde_json::Value {
    json!({
        "schema": 1,
        "id": "geo-public",
        "revision": 42,
        "expires_at_unix_secs": TEST_NOW + 3600,
        "domain_exact": ["API.Example"],
        "domain_suffix": ["media.example"],
        "destination_cidrs": ["203.0.113.99/24", "2001:db8::/32"]
    })
}

#[test]
fn verifies_checksum_signature_expiry_and_typed_entries() {
    let (artifact, catalog) = signed_artifact(payload());
    let verified = VerifiedRuleSet::verify_json(&artifact, &catalog, TEST_NOW).expect("verified");
    assert_eq!(verified.id().as_str(), "geo-public");
    assert_eq!(verified.publisher().as_str(), "official");
    assert_eq!(verified.revision(), 42);
    assert!(verified.matches_domain(&DomainName::parse("api.example").expect("domain")));
    assert!(verified.matches_domain(&DomainName::parse("cdn.media.example").expect("domain")));
    assert!(verified.matches_destination_ip("203.0.113.7".parse().expect("address")));
    assert!(verified.matches_destination_ip("2001:db8::7".parse().expect("address")));
    assert_eq!(
        verified.destination_cidrs()[0],
        "203.0.113.0/24".parse::<IpNet>().expect("CIDR")
    );
}

#[test]
fn tamper_unknown_fields_and_expiry_fail_closed() {
    let (artifact, catalog) = signed_artifact(payload());
    let mut envelope: serde_json::Value = serde_json::from_slice(&artifact).expect("envelope");
    envelope["payload_base64"] = json!(BASE64.encode(b"{}"));
    let tampered = serde_json::to_vec(&envelope).expect("tampered");
    assert_eq!(
        VerifiedRuleSet::verify_json(&tampered, &catalog, TEST_NOW),
        Err(RuleSetError::ChecksumMismatch)
    );

    let mut unknown = payload();
    unknown["unexpected"] = json!(true);
    let (unknown, catalog) = signed_artifact(unknown);
    assert_eq!(
        VerifiedRuleSet::verify_json(&unknown, &catalog, TEST_NOW),
        Err(RuleSetError::InvalidPayload)
    );

    let (expired, catalog) = signed_artifact(json!({
        "schema": 1,
        "id": "expired",
        "revision": 1,
        "expires_at_unix_secs": TEST_NOW,
        "domain_exact": [],
        "domain_suffix": [],
        "destination_cidrs": []
    }));
    assert_eq!(
        VerifiedRuleSet::verify_json(&expired, &catalog, TEST_NOW),
        Err(RuleSetError::Expired)
    );
}

#[test]
fn registry_rejects_duplicate_ids() {
    let (artifact, catalog) = signed_artifact(payload());
    let verified = VerifiedRuleSet::verify_json(&artifact, &catalog, TEST_NOW).expect("verified");
    assert!(matches!(
        CompiledRuleSetRegistry::compile(vec![verified.clone(), verified]),
        Err(RuleSetError::DuplicateRuleSet(id)) if id.as_str() == "geo-public"
    ));
}

#[test]
fn domain_only_destination_set_cannot_create_address_routing_demand() {
    use crate::product::{
        CompiledRouteTable, EgressAction, FlowContext, InboundId, Network, OutboundId, PrincipalId,
        ProtocolTarget, RouteAction, RouteMatchSpec, RouteRuleSpec, RuleId, SourceEndpoint,
        TrafficIntent,
    };

    let (artifact, catalog) = signed_artifact(json!({
        "schema": 1,
        "id": "domains-only",
        "revision": 1,
        "expires_at_unix_secs": TEST_NOW + 3600,
        "domain_exact": ["unrelated.example"],
        "domain_suffix": [],
        "destination_cidrs": []
    }));
    let domain_only = Arc::new(
        VerifiedRuleSet::verify_json(&artifact, &catalog, TEST_NOW).expect("verified set"),
    );
    let table = CompiledRouteTable::compile(
        7,
        vec![
            RouteRuleSpec::new(
                RuleId::parse("impossible-address-set").expect("rule"),
                RouteMatchSpec {
                    destination_rule_sets: vec![domain_only],
                    ..RouteMatchSpec::default()
                },
                RouteAction::new(EgressAction::Reject, None, TrafficIntent::Interactive),
            ),
            RouteRuleSpec::new(
                RuleId::parse("stable-domain").expect("rule"),
                RouteMatchSpec {
                    domain_exact: vec![DomainName::parse("service.example").expect("domain")],
                    ..RouteMatchSpec::default()
                },
                RouteAction::new(
                    EgressAction::Outbound(OutboundId::parse("proxy").expect("outbound")),
                    None,
                    TrafficIntent::Interactive,
                ),
            ),
            RouteRuleSpec::new(
                RuleId::parse("default").expect("rule"),
                RouteMatchSpec::default(),
                RouteAction::new(EgressAction::Reject, None, TrafficIntent::Interactive),
            ),
        ],
    )
    .expect("route table");
    let flow = FlowContext::new(
        Network::Tcp,
        ProtocolTarget::from_host_port("service.example", 443).expect("target"),
        SourceEndpoint::new(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 4)), 40_000),
        PrincipalId::parse("anonymous").expect("principal"),
        InboundId::parse("local-socks").expect("inbound"),
    );

    let (decision, requires_address_evidence) = table.classify_pre_resolution(&flow);
    assert_eq!(decision.rule_id().as_str(), "stable-domain");
    assert!(!requires_address_evidence);
}
