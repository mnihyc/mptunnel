use crate::product::flow::{DomainName, FlowError, canonical_policy_id};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use ipnet::IpNet;
use ring::signature;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;
use std::net::IpAddr;
use std::sync::Arc;

pub const RULE_SET_SCHEMA_VERSION: u16 = 1;
pub const RULE_SET_SIGNATURE_CONTEXT: &[u8] = b"mptunnel signed route rule set v1\0";
pub const MAX_RULE_SET_ENVELOPE_BYTES: usize = 12 * 1024 * 1024;
pub const MAX_RULE_SET_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_RULE_SET_PUBLISHERS: usize = 64;
pub const MAX_RULE_SETS: usize = 256;
pub const MAX_ENTRIES_PER_RULE_SET: usize = 1_000_000;
pub const MAX_ENTRIES_ACROSS_RULE_SET_REGISTRY: usize = 4_000_000;

macro_rules! rule_set_id {
    ($name:ident) => {
        #[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            pub fn parse(input: &str) -> Result<Self, FlowError> {
                canonical_policy_id(input).map(Self)
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.0)
                    .finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

rule_set_id!(RuleSetId);
rule_set_id!(RuleSetPublisherId);

#[derive(Clone, PartialEq, Eq)]
pub struct RuleSetPublisher {
    id: RuleSetPublisherId,
    ed25519_public_key: [u8; 32],
}

impl RuleSetPublisher {
    pub const fn new(id: RuleSetPublisherId, ed25519_public_key: [u8; 32]) -> Self {
        Self {
            id,
            ed25519_public_key,
        }
    }

    pub const fn id(&self) -> &RuleSetPublisherId {
        &self.id
    }
}

impl fmt::Debug for RuleSetPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuleSetPublisher")
            .field("id", &self.id)
            .field("ed25519_public_key", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Default)]
pub struct RuleSetPublisherCatalog {
    publishers: BTreeMap<RuleSetPublisherId, [u8; 32]>,
}

impl RuleSetPublisherCatalog {
    pub fn compile(publishers: Vec<RuleSetPublisher>) -> Result<Self, RuleSetError> {
        if publishers.len() > MAX_RULE_SET_PUBLISHERS {
            return Err(RuleSetError::TooManyPublishers {
                count: publishers.len(),
                maximum: MAX_RULE_SET_PUBLISHERS,
            });
        }
        let mut compiled = BTreeMap::new();
        for publisher in publishers {
            if compiled
                .insert(publisher.id.clone(), publisher.ed25519_public_key)
                .is_some()
            {
                return Err(RuleSetError::DuplicatePublisher(publisher.id));
            }
        }
        Ok(Self {
            publishers: compiled,
        })
    }

    fn public_key(&self, id: &RuleSetPublisherId) -> Option<&[u8; 32]> {
        self.publishers.get(id)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct VerifiedRuleSet {
    id: RuleSetId,
    publisher: RuleSetPublisherId,
    revision: u64,
    expires_at_unix_secs: Option<u64>,
    checksum_sha256: [u8; 32],
    domain_exact: Vec<DomainName>,
    domain_suffix: Vec<DomainName>,
    destination_cidrs: Vec<IpNet>,
    ipv4_prefixes: u64,
    ipv6_prefixes: [u64; 3],
}

impl fmt::Debug for VerifiedRuleSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedRuleSet")
            .field("id", &self.id)
            .field("publisher", &self.publisher)
            .field("revision", &self.revision)
            .field("expires_at_unix_secs", &self.expires_at_unix_secs)
            .field("domain_exact_entries", &self.domain_exact.len())
            .field("domain_suffix_entries", &self.domain_suffix.len())
            .field("destination_cidr_entries", &self.destination_cidrs.len())
            .finish_non_exhaustive()
    }
}

impl VerifiedRuleSet {
    /// Verify the fixed envelope, payload checksum, publisher signature,
    /// expiry, strict payload schema, and all entry bounds before exposing
    /// policy data. The Ed25519 signature covers
    /// `RULE_SET_SIGNATURE_CONTEXT || sha256(payload_bytes)`.
    pub fn verify_json(
        envelope_bytes: &[u8],
        publishers: &RuleSetPublisherCatalog,
        now_unix_secs: u64,
    ) -> Result<Self, RuleSetError> {
        if envelope_bytes.len() > MAX_RULE_SET_ENVELOPE_BYTES {
            return Err(RuleSetError::EnvelopeTooLarge {
                bytes: envelope_bytes.len(),
                maximum: MAX_RULE_SET_ENVELOPE_BYTES,
            });
        }
        let envelope: RuleSetEnvelope =
            serde_json::from_slice(envelope_bytes).map_err(|_| RuleSetError::InvalidEnvelope)?;
        if envelope.schema != RULE_SET_SCHEMA_VERSION {
            return Err(RuleSetError::UnsupportedEnvelopeSchema(envelope.schema));
        }
        let publisher = RuleSetPublisherId::parse(&envelope.publisher)
            .map_err(|_| RuleSetError::InvalidPublisherId)?;
        let public_key = publishers
            .public_key(&publisher)
            .ok_or_else(|| RuleSetError::UnknownPublisher(publisher.clone()))?;
        if envelope.payload_base64.len() > MAX_RULE_SET_ENVELOPE_BYTES {
            return Err(RuleSetError::PayloadTooLarge {
                bytes: envelope.payload_base64.len(),
                maximum: MAX_RULE_SET_PAYLOAD_BYTES,
            });
        }
        let payload = BASE64
            .decode(envelope.payload_base64.as_bytes())
            .map_err(|_| RuleSetError::InvalidPayloadEncoding)?;
        if payload.len() > MAX_RULE_SET_PAYLOAD_BYTES {
            return Err(RuleSetError::PayloadTooLarge {
                bytes: payload.len(),
                maximum: MAX_RULE_SET_PAYLOAD_BYTES,
            });
        }

        let expected_checksum =
            decode_lower_hex_32(&envelope.checksum_sha256).ok_or(RuleSetError::InvalidChecksum)?;
        let actual_checksum: [u8; 32] = Sha256::digest(&payload).into();
        if actual_checksum != expected_checksum {
            return Err(RuleSetError::ChecksumMismatch);
        }
        let signature_bytes = BASE64
            .decode(envelope.signature_base64.as_bytes())
            .map_err(|_| RuleSetError::InvalidSignatureEncoding)?;
        if signature_bytes.len() != 64 {
            return Err(RuleSetError::InvalidSignatureEncoding);
        }
        let mut signed_message = [0_u8; RULE_SET_SIGNATURE_CONTEXT.len() + 32];
        signed_message[..RULE_SET_SIGNATURE_CONTEXT.len()]
            .copy_from_slice(RULE_SET_SIGNATURE_CONTEXT);
        signed_message[RULE_SET_SIGNATURE_CONTEXT.len()..].copy_from_slice(&actual_checksum);
        signature::UnparsedPublicKey::new(&signature::ED25519, public_key)
            .verify(&signed_message, &signature_bytes)
            .map_err(|_| RuleSetError::InvalidSignature)?;

        let payload: RuleSetPayload =
            serde_json::from_slice(&payload).map_err(|_| RuleSetError::InvalidPayload)?;
        if payload.schema != RULE_SET_SCHEMA_VERSION {
            return Err(RuleSetError::UnsupportedPayloadSchema(payload.schema));
        }
        if payload.revision == 0 {
            return Err(RuleSetError::InvalidRevision);
        }
        if payload
            .expires_at_unix_secs
            .is_some_and(|expiry| expiry <= now_unix_secs)
        {
            return Err(RuleSetError::Expired);
        }
        let id = RuleSetId::parse(&payload.id).map_err(|_| RuleSetError::InvalidRuleSetId)?;
        let entry_count = payload
            .domain_exact
            .len()
            .saturating_add(payload.domain_suffix.len())
            .saturating_add(payload.destination_cidrs.len());
        if entry_count > MAX_ENTRIES_PER_RULE_SET {
            return Err(RuleSetError::TooManyEntries {
                count: entry_count,
                maximum: MAX_ENTRIES_PER_RULE_SET,
            });
        }

        let domain_exact = parse_domains(payload.domain_exact)?;
        let domain_suffix = parse_domains(payload.domain_suffix)?;
        let destination_cidrs = parse_cidrs(payload.destination_cidrs)?;
        let (ipv4_prefixes, ipv6_prefixes) = prefix_index(&destination_cidrs);
        Ok(Self {
            id,
            publisher,
            revision: payload.revision,
            expires_at_unix_secs: payload.expires_at_unix_secs,
            checksum_sha256: actual_checksum,
            domain_exact,
            domain_suffix,
            destination_cidrs,
            ipv4_prefixes,
            ipv6_prefixes,
        })
    }

    pub const fn id(&self) -> &RuleSetId {
        &self.id
    }

    pub const fn publisher(&self) -> &RuleSetPublisherId {
        &self.publisher
    }

    pub const fn revision(&self) -> u64 {
        self.revision
    }

    pub const fn expires_at_unix_secs(&self) -> Option<u64> {
        self.expires_at_unix_secs
    }

    pub const fn checksum_sha256(&self) -> &[u8; 32] {
        &self.checksum_sha256
    }

    pub fn domain_exact(&self) -> &[DomainName] {
        &self.domain_exact
    }

    pub fn domain_suffix(&self) -> &[DomainName] {
        &self.domain_suffix
    }

    pub fn destination_cidrs(&self) -> &[IpNet] {
        &self.destination_cidrs
    }

    pub fn matches_domain(&self, domain: &DomainName) -> bool {
        if self.domain_exact.binary_search(domain).is_ok() {
            return true;
        }
        let mut suffix = domain.as_str();
        loop {
            if self
                .domain_suffix
                .binary_search_by(|candidate| candidate.as_str().cmp(suffix))
                .is_ok()
            {
                return true;
            }
            let Some((_, remaining)) = suffix.split_once('.') else {
                return false;
            };
            suffix = remaining;
        }
    }

    pub fn matches_destination_ip(&self, address: IpAddr) -> bool {
        match address {
            IpAddr::V4(_) => (0_u8..=32).rev().any(|prefix| {
                self.ipv4_prefixes & (1_u64 << u32::from(prefix)) != 0
                    && self.contains_network(address, prefix)
            }),
            IpAddr::V6(_) => (0_u8..=128).rev().any(|prefix| {
                let index = usize::from(prefix);
                self.ipv6_prefixes[index / 64] & (1_u64 << (index % 64)) != 0
                    && self.contains_network(address, prefix)
            }),
        }
    }

    fn contains_network(&self, address: IpAddr, prefix: u8) -> bool {
        IpNet::new(address, prefix)
            .ok()
            .map(|network| network.trunc())
            .is_some_and(|network| self.destination_cidrs.binary_search(&network).is_ok())
    }

    pub fn entry_count(&self) -> usize {
        self.domain_exact
            .len()
            .saturating_add(self.domain_suffix.len())
            .saturating_add(self.destination_cidrs.len())
    }
}

#[derive(Debug, Clone, Default)]
pub struct CompiledRuleSetRegistry {
    sets: BTreeMap<RuleSetId, Arc<VerifiedRuleSet>>,
    total_entries: usize,
}

impl CompiledRuleSetRegistry {
    pub fn compile(sets: Vec<VerifiedRuleSet>) -> Result<Self, RuleSetError> {
        if sets.len() > MAX_RULE_SETS {
            return Err(RuleSetError::TooManyRuleSets {
                count: sets.len(),
                maximum: MAX_RULE_SETS,
            });
        }
        let mut compiled = BTreeMap::new();
        let mut total_entries = 0_usize;
        for set in sets {
            total_entries = total_entries.saturating_add(set.entry_count());
            if total_entries > MAX_ENTRIES_ACROSS_RULE_SET_REGISTRY {
                return Err(RuleSetError::RegistryTooLarge {
                    count: total_entries,
                    maximum: MAX_ENTRIES_ACROSS_RULE_SET_REGISTRY,
                });
            }
            let id = set.id.clone();
            if compiled.insert(id.clone(), Arc::new(set)).is_some() {
                return Err(RuleSetError::DuplicateRuleSet(id));
            }
        }
        Ok(Self {
            sets: compiled,
            total_entries,
        })
    }

    pub fn resolve(&self, id: &RuleSetId) -> Option<Arc<VerifiedRuleSet>> {
        self.sets.get(id).cloned()
    }

    pub fn sets(&self) -> impl ExactSizeIterator<Item = &VerifiedRuleSet> {
        self.sets.values().map(Arc::as_ref)
    }

    pub fn total_entries(&self) -> usize {
        self.total_entries
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleSetEnvelope {
    schema: u16,
    publisher: String,
    checksum_sha256: String,
    payload_base64: String,
    signature_base64: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleSetPayload {
    schema: u16,
    id: String,
    revision: u64,
    expires_at_unix_secs: Option<u64>,
    #[serde(default)]
    domain_exact: Vec<String>,
    #[serde(default)]
    domain_suffix: Vec<String>,
    #[serde(default)]
    destination_cidrs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleSetError {
    EnvelopeTooLarge { bytes: usize, maximum: usize },
    PayloadTooLarge { bytes: usize, maximum: usize },
    TooManyPublishers { count: usize, maximum: usize },
    TooManyRuleSets { count: usize, maximum: usize },
    TooManyEntries { count: usize, maximum: usize },
    RegistryTooLarge { count: usize, maximum: usize },
    DuplicatePublisher(RuleSetPublisherId),
    DuplicateRuleSet(RuleSetId),
    InvalidEnvelope,
    UnsupportedEnvelopeSchema(u16),
    InvalidPublisherId,
    UnknownPublisher(RuleSetPublisherId),
    InvalidPayloadEncoding,
    InvalidChecksum,
    ChecksumMismatch,
    InvalidSignatureEncoding,
    InvalidSignature,
    InvalidPayload,
    UnsupportedPayloadSchema(u16),
    InvalidRuleSetId,
    InvalidRevision,
    Expired,
    InvalidDomain,
    InvalidCidr,
}

impl fmt::Display for RuleSetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EnvelopeTooLarge { bytes, maximum } => {
                write!(
                    formatter,
                    "rule-set envelope is {bytes} bytes; maximum is {maximum}"
                )
            }
            Self::PayloadTooLarge { bytes, maximum } => {
                write!(
                    formatter,
                    "rule-set payload is {bytes} bytes; maximum is {maximum}"
                )
            }
            Self::TooManyPublishers { count, maximum } => {
                write!(
                    formatter,
                    "rule-set catalog has {count} publishers; maximum is {maximum}"
                )
            }
            Self::TooManyRuleSets { count, maximum } => {
                write!(
                    formatter,
                    "registry has {count} rule sets; maximum is {maximum}"
                )
            }
            Self::TooManyEntries { count, maximum } => {
                write!(
                    formatter,
                    "rule set has {count} entries; maximum is {maximum}"
                )
            }
            Self::RegistryTooLarge { count, maximum } => {
                write!(
                    formatter,
                    "rule-set registry has {count} entries; maximum is {maximum}"
                )
            }
            Self::DuplicatePublisher(id) => write!(formatter, "duplicate rule-set publisher {id}"),
            Self::DuplicateRuleSet(id) => write!(formatter, "duplicate rule set {id}"),
            Self::InvalidEnvelope => formatter.write_str("rule-set envelope is invalid"),
            Self::UnsupportedEnvelopeSchema(schema) => {
                write!(formatter, "unsupported rule-set envelope schema {schema}")
            }
            Self::InvalidPublisherId => formatter.write_str("rule-set publisher ID is invalid"),
            Self::UnknownPublisher(id) => write!(formatter, "unknown rule-set publisher {id}"),
            Self::InvalidPayloadEncoding => {
                formatter.write_str("rule-set payload base64 is invalid")
            }
            Self::InvalidChecksum => formatter.write_str("rule-set checksum is invalid"),
            Self::ChecksumMismatch => {
                formatter.write_str("rule-set payload checksum does not match")
            }
            Self::InvalidSignatureEncoding => {
                formatter.write_str("rule-set signature encoding is invalid")
            }
            Self::InvalidSignature => formatter.write_str("rule-set signature is invalid"),
            Self::InvalidPayload => formatter.write_str("rule-set payload is invalid"),
            Self::UnsupportedPayloadSchema(schema) => {
                write!(formatter, "unsupported rule-set payload schema {schema}")
            }
            Self::InvalidRuleSetId => formatter.write_str("rule-set ID is invalid"),
            Self::InvalidRevision => formatter.write_str("rule-set revision must be non-zero"),
            Self::Expired => formatter.write_str("rule set is expired"),
            Self::InvalidDomain => formatter.write_str("rule set contains an invalid domain"),
            Self::InvalidCidr => formatter.write_str("rule set contains an invalid IP network"),
        }
    }
}

impl std::error::Error for RuleSetError {}

fn parse_domains(values: Vec<String>) -> Result<Vec<DomainName>, RuleSetError> {
    let mut parsed = Vec::with_capacity(values.len());
    for value in values {
        let domain = DomainName::parse(&value).map_err(|_| RuleSetError::InvalidDomain)?;
        parsed.push(domain);
    }
    parsed.sort_unstable();
    parsed.dedup();
    Ok(parsed)
}

fn parse_cidrs(values: Vec<String>) -> Result<Vec<IpNet>, RuleSetError> {
    let mut parsed = Vec::with_capacity(values.len());
    for value in values {
        let network = value
            .parse::<IpNet>()
            .map_err(|_| RuleSetError::InvalidCidr)?
            .trunc();
        parsed.push(network);
    }
    parsed.sort_unstable();
    parsed.dedup();
    Ok(parsed)
}

fn prefix_index(networks: &[IpNet]) -> (u64, [u64; 3]) {
    let mut ipv4 = 0_u64;
    let mut ipv6 = [0_u64; 3];
    for network in networks {
        let prefix = usize::from(network.prefix_len());
        match network {
            IpNet::V4(_) => ipv4 |= 1_u64 << prefix,
            IpNet::V6(_) => ipv6[prefix / 64] |= 1_u64 << (prefix % 64),
        }
    }
    (ipv4, ipv6)
}

fn decode_lower_hex_32(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Some(decoded)
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use serde_json::json;

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
        let verified =
            VerifiedRuleSet::verify_json(&artifact, &catalog, TEST_NOW).expect("verified");
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
        let verified =
            VerifiedRuleSet::verify_json(&artifact, &catalog, TEST_NOW).expect("verified");
        assert!(matches!(
            CompiledRuleSetRegistry::compile(vec![verified.clone(), verified]),
            Err(RuleSetError::DuplicateRuleSet(id)) if id.as_str() == "geo-public"
        ));
    }
}
