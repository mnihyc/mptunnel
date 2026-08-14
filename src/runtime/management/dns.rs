//! Authenticated DNS runtime operations and JSON projection.
//!
//! HTTP parsing and route dispatch remain in `http`; this module owns DNS
//! request validation, generation-local actions, and detached response values.

use super::ManagementTarget;
use super::http::ManagementHttpError;
use crate::dns::{
    DnsPlanRuntimeSnapshot, DnsRuntimeError, DnsUpstreamDescriptor, DnsUpstreamRuntimeSnapshot,
};
use crate::product::{DnsEgressSpec, DnsPlanId, DnsTransport, DnsUpstreamStrategy, DomainName};
use hickory_proto::op::ResponseCode;
use hickory_proto::rr::{Record, RecordType};
use serde::Deserialize;
use serde_json::{Value, json};
use std::str::FromStr;

const DNS_STATUS_SCHEMA: &str = "mptunnel.dns.status.v3";
const DNS_EXPLAIN_SCHEMA: &str = "mptunnel.dns.explain.v3";
const DNS_QUERY_SCHEMA: &str = "mptunnel.dns.query.v3";
const DNS_FLUSH_SCHEMA: &str = "mptunnel.dns.flush.v3";

impl ManagementTarget {
    pub(super) fn dns_status_json(&self) -> Result<Value, ManagementHttpError> {
        let dns = self.dns.as_ref().ok_or_else(dns_runtime_unavailable)?;
        let snapshot = dns.runtime_snapshot();
        Ok(json!({
            "schema": DNS_STATUS_SCHEMA,
            "generation": snapshot.generation,
            "records": snapshot.host_overrides,
            "override": snapshot.fake_dns.map(|fake| json!({
                "ipv4_pool": fake.ipv4_pool.map(|pool| pool.to_string()),
                "ipv6_pool": fake.ipv6_pool.map(|pool| pool.to_string()),
                "max_entries": fake.max_entries,
                "owned_entries": fake.owned_entries,
                "active_entries": fake.active_entries,
                "answers": fake.answers.to_string(),
                "recoveries": fake.recoveries.to_string(),
                "expired_recoveries": fake.expired_recoveries.to_string(),
                "unknown_recoveries": fake.unknown_recoveries.to_string(),
                "capacity_failures": fake.capacity_failures.to_string(),
            })),
            "policies": snapshot.plans.into_iter().map(policy_status_json).collect::<Vec<_>>(),
            "operations": {
                "explain": "GET /api/v3/dns/explain?domain=<domain>",
                "query": "POST /api/v3/dns/query",
                "flush": "POST /api/v3/dns/cache/flush"
            }
        }))
    }

    pub(super) fn dns_explain_json(&self, domain: &str) -> Result<Value, ManagementHttpError> {
        let dns = self.dns.as_ref().ok_or_else(dns_runtime_unavailable)?;
        let domain = parse_domain(domain)?;
        let explanation = dns.explain(&domain);
        Ok(json!({
            "schema": DNS_EXPLAIN_SCHEMA,
            "generation": explanation.generation,
            "domain": explanation.domain.as_str(),
            "policy": explanation.plan.as_str(),
            "rule": explanation.rule.as_ref().map(|rule| rule.as_str()),
            "match": format!("{:?}", explanation.match_kind).to_ascii_lowercase(),
            "matched_domain": explanation.matched_domain.as_ref().map(DomainName::as_str),
            "explanation": explanation.explanation.as_deref(),
            "record_addresses": explanation.host_addresses.as_deref().map(|addresses| {
                addresses.iter().map(ToString::to_string).collect::<Vec<_>>()
            }),
            "override": explanation.fake_dns.map(|fake| json!({
                "ipv4_pool": fake.ipv4_pool.map(|pool| pool.to_string()),
                "ipv6_pool": fake.ipv6_pool.map(|pool| pool.to_string()),
                "answer_ttl_ms": fake.answer_ttl.as_millis().min(u64::MAX as u128) as u64,
                "recovery_ttl_ms": fake.recovery_ttl.as_millis().min(u64::MAX as u128) as u64,
                "capture_only": true,
            })),
            "strategy": server_strategy_name(explanation.upstream_strategy),
            "fallback_ms": server_fallback_ms(explanation.upstream_strategy),
            "answer_cidrs": explanation.expected_cidrs.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "servers": explanation.upstreams.iter().map(server_descriptor_json).collect::<Vec<_>>(),
        }))
    }

    pub(super) async fn dns_query_json(&self, body: &[u8]) -> Result<Value, ManagementHttpError> {
        let dns = self.dns.as_ref().ok_or_else(dns_runtime_unavailable)?;
        let request = serde_json::from_slice::<DnsQueryRequest>(body).map_err(|_| {
            ManagementHttpError::new(400, "Bad Request", "invalid DNS query JSON body")
        })?;
        let domain = parse_domain(&request.domain)?;
        let record_type =
            RecordType::from_str(&request.record_type.to_ascii_uppercase()).map_err(|_| {
                ManagementHttpError::new(400, "Bad Request", "unsupported DNS record type")
            })?;
        if matches!(record_type, RecordType::AXFR | RecordType::IXFR) {
            return Err(ManagementHttpError::new(
                400,
                "Bad Request",
                "DNS transfer record types are not supported",
            ));
        }
        let resolution = dns
            .query_record(&domain, record_type)
            .await
            .map_err(map_dns_runtime_error)?;
        let message = resolution.message();
        let response_code = message.metadata.response_code;
        Ok(json!({
            "schema": DNS_QUERY_SCHEMA,
            "generation": resolution.metadata().generation(),
            "domain": domain.as_str(),
            "type": record_type.to_string(),
            "policy": resolution.metadata().plan().as_str(),
            "rule": resolution.metadata().rule().map(|rule| rule.as_str()),
            "match": format!("{:?}", resolution.metadata().match_kind()).to_ascii_lowercase(),
            "stale": resolution.is_stale(),
            "rcode": u16::from(response_code),
            "rcode_name": response_code_name(response_code),
            "authoritative": message.metadata.authoritative,
            "authenticated_data": message.metadata.authentic_data,
            "answers": records_json(&message.answers),
            "authorities": records_json(&message.authorities),
            "additionals": records_json(&message.additionals),
        }))
    }

    pub(super) fn dns_flush_json(&self, body: &[u8]) -> Result<Value, ManagementHttpError> {
        let dns = self.dns.as_ref().ok_or_else(dns_runtime_unavailable)?;
        let request = serde_json::from_slice::<DnsFlushRequest>(body).map_err(|_| {
            ManagementHttpError::new(400, "Bad Request", "invalid DNS cache-flush JSON body")
        })?;
        let policy = request
            .policy
            .as_deref()
            .map(DnsPlanId::parse)
            .transpose()
            .map_err(|_| ManagementHttpError::new(400, "Bad Request", "invalid DNS policy name"))?;
        let flushed = dns
            .flush_cache(policy.as_ref())
            .map_err(map_dns_runtime_error)?;
        Ok(json!({
            "schema": DNS_FLUSH_SCHEMA,
            "generation": flushed.generation,
            "flushed_policies": flushed.plans,
            "removed_entries": flushed.removed_entries,
        }))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DnsQueryRequest {
    domain: String,
    #[serde(rename = "type")]
    record_type: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct DnsFlushRequest {
    #[serde(default)]
    policy: Option<String>,
}

fn parse_domain(value: &str) -> Result<DomainName, ManagementHttpError> {
    DomainName::parse(value)
        .map_err(|error| ManagementHttpError::new(400, "Bad Request", error.to_string()))
}

fn policy_status_json(plan: DnsPlanRuntimeSnapshot) -> Value {
    json!({
        "name": plan.plan.as_str(),
        "strategy": server_strategy_name(plan.upstream_strategy),
        "fallback_ms": server_fallback_ms(plan.upstream_strategy),
        "answer_cidrs": plan.expected_cidrs.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "cache": {
            "entries": plan.cache_entries,
            "fresh": plan.fresh_cache_entries,
            "stale": plan.stale_cache_entries,
            "hits": plan.fresh_cache_hits.to_string(),
            "misses": plan.cache_misses.to_string(),
            "evictions": plan.cache_evictions.to_string(),
            "flushes": plan.cache_flushes.to_string(),
        },
        "in_flight": plan.in_flight,
        "queries": plan.queries.to_string(),
        "coalesced_queries": plan.coalesced_queries.to_string(),
        "refreshes_started": plan.refreshes_started.to_string(),
        "stale_answers": plan.stale_answers.to_string(),
        "record_answers": plan.host_answers.to_string(),
        "servers": plan.upstreams.into_iter().map(server_status_json).collect::<Vec<_>>(),
    })
}

fn server_status_json(upstream: DnsUpstreamRuntimeSnapshot) -> Value {
    let average_latency_micros =
        (upstream.successes > 0).then(|| upstream.total_latency_micros / upstream.successes);
    json!({
        "name": upstream.upstream.as_str(),
        "protocol": dns_protocol_name(upstream.transport),
        "address": upstream.bootstrap.map(|address| address.to_string()),
        "outbound": outbound_name(&upstream.egress),
        "attempts": upstream.attempts.to_string(),
        "successes": upstream.successes.to_string(),
        "negative_answers": upstream.negative_answers.to_string(),
        "failures": upstream.failures.to_string(),
        "timeouts": upstream.timeouts.to_string(),
        "rejected_answers": upstream.rejected_answers.to_string(),
        "canceled_attempts": upstream.canceled_attempts.to_string(),
        "total_latency_micros": upstream.total_latency_micros.to_string(),
        "average_success_latency_micros": average_latency_micros.map(|value| value.to_string()),
    })
}

fn server_descriptor_json(upstream: &DnsUpstreamDescriptor) -> Value {
    json!({
        "name": upstream.upstream.as_str(),
        "protocol": dns_protocol_name(upstream.transport),
        "address": upstream.bootstrap.map(|address| address.to_string()),
        "outbound": outbound_name(&upstream.egress),
    })
}

const fn server_strategy_name(strategy: DnsUpstreamStrategy) -> &'static str {
    match strategy {
        DnsUpstreamStrategy::Ordered => "ordered",
        DnsUpstreamStrategy::Race { .. } => "race",
    }
}

fn server_fallback_ms(strategy: DnsUpstreamStrategy) -> Option<u64> {
    match strategy {
        DnsUpstreamStrategy::Ordered => None,
        DnsUpstreamStrategy::Race { fallback_delay } => {
            Some(fallback_delay.as_millis().min(u64::MAX as u128) as u64)
        }
    }
}

const fn dns_protocol_name(protocol: DnsTransport) -> &'static str {
    match protocol {
        DnsTransport::System => "system",
        DnsTransport::Udp => "udp",
        DnsTransport::Tcp => "tcp",
        DnsTransport::UdpTcp => "udp-tcp",
        DnsTransport::Tls => "dot",
        DnsTransport::Https => "doh",
        DnsTransport::Quic => "doq",
    }
}

fn outbound_name(egress: &DnsEgressSpec) -> Option<&str> {
    match egress {
        DnsEgressSpec::Direct => None,
        DnsEgressSpec::Outbound(outbound) => Some(outbound.as_str()),
    }
}

fn records_json(records: &[Record]) -> Vec<Value> {
    records
        .iter()
        .map(|record| {
            json!({
                "owner_name": record.name.to_utf8(),
                "type": record.record_type().to_string(),
                "class": record.dns_class.to_string(),
                "ttl": record.ttl,
                "data": record.data.to_string(),
            })
        })
        .collect()
}

fn response_code_name(code: ResponseCode) -> &'static str {
    match code {
        ResponseCode::NoError => "NOERROR",
        ResponseCode::FormErr => "FORMERR",
        ResponseCode::ServFail => "SERVFAIL",
        ResponseCode::NXDomain => "NXDOMAIN",
        ResponseCode::NotImp => "NOTIMP",
        ResponseCode::Refused => "REFUSED",
        ResponseCode::YXDomain => "YXDOMAIN",
        ResponseCode::YXRRSet => "YXRRSET",
        ResponseCode::NXRRSet => "NXRRSET",
        ResponseCode::NotAuth => "NOTAUTH",
        ResponseCode::NotZone => "NOTZONE",
        ResponseCode::BADVERS => "BADVERS",
        ResponseCode::BADSIG => "BADSIG",
        ResponseCode::BADKEY => "BADKEY",
        ResponseCode::BADTIME => "BADTIME",
        ResponseCode::BADMODE => "BADMODE",
        ResponseCode::BADNAME => "BADNAME",
        ResponseCode::BADALG => "BADALG",
        ResponseCode::BADTRUNC => "BADTRUNC",
        ResponseCode::BADCOOKIE => "BADCOOKIE",
        ResponseCode::Unknown(_) => "UNKNOWN",
    }
}

fn dns_runtime_unavailable() -> ManagementHttpError {
    ManagementHttpError::new(
        409,
        "Conflict",
        "this runtime generation has no configured DNS service",
    )
}

fn map_dns_runtime_error(error: DnsRuntimeError) -> ManagementHttpError {
    match error {
        DnsRuntimeError::InvalidDomain { .. } | DnsRuntimeError::InvalidPort => {
            ManagementHttpError::new(400, "Bad Request", error.to_string())
        }
        DnsRuntimeError::UnknownPlan(_) => {
            ManagementHttpError::new(404, "Not Found", error.to_string())
        }
        DnsRuntimeError::AtCapacity { .. } | DnsRuntimeError::ProductAtCapacity { .. } => {
            ManagementHttpError::new(429, "Too Many Requests", error.to_string())
        }
        DnsRuntimeError::Timeout { .. } => {
            ManagementHttpError::new(504, "Gateway Timeout", error.to_string())
        }
        DnsRuntimeError::NoRecords { .. } => {
            ManagementHttpError::new(404, "Not Found", error.to_string())
        }
        DnsRuntimeError::PolicyInvariant(_) => {
            ManagementHttpError::new(500, "Internal Server Error", error.to_string())
        }
        DnsRuntimeError::MissingEgressConnector { .. }
        | DnsRuntimeError::RecursiveEgressConnector { .. }
        | DnsRuntimeError::PrepublicationDnsRequiresDirect { .. }
        | DnsRuntimeError::PrepublicationSystemDns { .. }
        | DnsRuntimeError::UnsupportedEgressTransport { .. }
        | DnsRuntimeError::Build { .. }
        | DnsRuntimeError::TooManyAnswers { .. }
        | DnsRuntimeError::AllUpstreamsFailed { .. } => {
            ManagementHttpError::new(502, "Bad Gateway", error.to_string())
        }
    }
}
