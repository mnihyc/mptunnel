use crate::product::flow::{
    FlowContext, FlowError, InboundId, Network, PrincipalId, canonical_policy_id,
};
use crate::product::rule_set::VerifiedRuleSet;
use idna::domain_to_ascii_strict;
use ipnet::IpNet;
use regex::{Regex, RegexBuilder};
use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;

const MAX_RULES: usize = 8_192;
const MAX_VALUES_PER_FIELD: usize = 1_024;
const MAX_REGEX_PER_RULE: usize = 32;
const MAX_REGEX_BYTES: usize = 1_024;
const MAX_EXPLANATION_BYTES: usize = 256;
const REGEX_SIZE_LIMIT: usize = 1 << 20;
const REGEX_DFA_SIZE_LIMIT: usize = 1 << 20;

macro_rules! route_id {
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

        impl FromStr for $name {
            type Err = FlowError;

            fn from_str(input: &str) -> Result<Self, Self::Err> {
                Self::parse(input)
            }
        }
    };
}

route_id!(RuleId);
route_id!(OutboundId);
route_id!(BalancerId);
route_id!(DnsPlanId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RouteStage {
    PreResolution,
    PostResolution,
}

#[derive(Debug, Clone, Copy)]
pub struct RouteInput<'a> {
    flow: &'a FlowContext,
    stage: RouteStage,
    resolved_ip: Option<IpAddr>,
}

impl<'a> RouteInput<'a> {
    pub const fn pre_resolution(flow: &'a FlowContext) -> Self {
        Self {
            flow,
            stage: RouteStage::PreResolution,
            resolved_ip: None,
        }
    }

    pub const fn post_resolution(flow: &'a FlowContext, resolved_ip: IpAddr) -> Self {
        Self {
            flow,
            stage: RouteStage::PostResolution,
            resolved_ip: Some(match flow.target().ip() {
                Some(literal) => literal,
                None => canonical_ip(resolved_ip),
            }),
        }
    }

    pub const fn flow(self) -> &'a FlowContext {
        self.flow
    }

    pub const fn stage(self) -> RouteStage {
        self.stage
    }

    pub const fn resolved_ip(self) -> Option<IpAddr> {
        self.resolved_ip
    }

    pub(crate) const fn destination_ip(self) -> Option<IpAddr> {
        match self.flow.target().ip() {
            Some(literal) => Some(literal),
            None => match (self.stage, self.resolved_ip) {
                (RouteStage::PostResolution, Some(address)) => Some(address),
                _ => None,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PortRange {
    start: u16,
    end: u16,
}

impl PortRange {
    pub fn new(start: u16, end: u16) -> Result<Self, RouteCompileError> {
        if start > end {
            return Err(RouteCompileError::InvalidPortRange { start, end });
        }
        Ok(Self { start, end })
    }

    pub const fn single(port: u16) -> Self {
        Self {
            start: port,
            end: port,
        }
    }

    pub const fn start(self) -> u16 {
        self.start
    }

    pub const fn end(self) -> u16 {
        self.end
    }

    pub const fn contains(self, port: u16) -> bool {
        self.start <= port && port <= self.end
    }
}

/// Match categories are ANDed; values inside one category are ORed. Empty
/// categories are wildcards.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RouteMatchSpec {
    pub domain_exact: Vec<crate::product::DomainName>,
    pub domain_suffix: Vec<crate::product::DomainName>,
    pub domain_keyword: Vec<String>,
    pub domain_regex: Vec<String>,
    /// Signed, verified sets ORed with the explicit domain matchers above.
    pub domain_rule_sets: Vec<Arc<VerifiedRuleSet>>,
    pub destination_cidrs: Vec<IpNet>,
    /// Signed, verified sets ORed with explicit destination CIDRs.
    pub destination_rule_sets: Vec<Arc<VerifiedRuleSet>>,
    pub source_cidrs: Vec<IpNet>,
    pub destination_ports: Vec<PortRange>,
    pub source_ports: Vec<PortRange>,
    pub networks: Vec<Network>,
    pub inbounds: Vec<InboundId>,
    pub principals: Vec<PrincipalId>,
    pub stages: Vec<RouteStage>,
}

impl RouteMatchSpec {
    pub fn is_catch_all(&self) -> bool {
        self.domain_exact.is_empty()
            && self.domain_suffix.is_empty()
            && self.domain_keyword.is_empty()
            && self.domain_regex.is_empty()
            && self.domain_rule_sets.is_empty()
            && self.destination_cidrs.is_empty()
            && self.destination_rule_sets.is_empty()
            && self.source_cidrs.is_empty()
            && self.destination_ports.is_empty()
            && self.source_ports.is_empty()
            && self.networks.is_empty()
            && self.inbounds.is_empty()
            && self.principals.is_empty()
            && self.stages.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TrafficIntent {
    Interactive,
    Throughput,
    Realtime,
    Background,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EgressAction {
    Direct,
    Reject,
    Drop,
    Outbound(OutboundId),
    Balancer(BalancerId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteAction {
    egress: EgressAction,
    dns_plan: Option<DnsPlanId>,
    traffic_intent: TrafficIntent,
}

impl RouteAction {
    pub const fn new(
        egress: EgressAction,
        dns_plan: Option<DnsPlanId>,
        traffic_intent: TrafficIntent,
    ) -> Self {
        Self {
            egress,
            dns_plan,
            traffic_intent,
        }
    }

    pub const fn direct(traffic_intent: TrafficIntent) -> Self {
        Self::new(EgressAction::Direct, None, traffic_intent)
    }

    pub const fn egress(&self) -> &EgressAction {
        &self.egress
    }

    pub const fn dns_plan(&self) -> Option<&DnsPlanId> {
        self.dns_plan.as_ref()
    }

    pub const fn traffic_intent(&self) -> TrafficIntent {
        self.traffic_intent
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteRuleSpec {
    /// The operator-facing `[[routing.rules]].name`, compiled to a typed
    /// diagnostic identity. Rule selection still follows declared order.
    pub id: RuleId,
    pub matcher: RouteMatchSpec,
    pub action: RouteAction,
    /// Optional bounded display text compiled once with the rule.
    pub explanation: Option<String>,
}

impl RouteRuleSpec {
    pub fn new(id: RuleId, matcher: RouteMatchSpec, action: RouteAction) -> Self {
        Self {
            id,
            matcher,
            action,
            explanation: None,
        }
    }
}

#[derive(Debug)]
struct CompiledRouteRule {
    id: RuleId,
    matcher: CompiledMatcher,
    action: RouteAction,
    explanation: String,
}

/// One immutable routing generation. Compilation either returns the complete
/// table or an error; no partially compiled state is exposed.
#[derive(Debug)]
pub struct CompiledRouteTable {
    generation: u64,
    rules: Vec<CompiledRouteRule>,
}

impl CompiledRouteTable {
    pub fn compile(generation: u64, rules: Vec<RouteRuleSpec>) -> Result<Self, RouteCompileError> {
        if rules.is_empty() {
            return Err(RouteCompileError::MissingDefaultRule);
        }
        if rules.len() > MAX_RULES {
            return Err(RouteCompileError::TooManyRules {
                count: rules.len(),
                maximum: MAX_RULES,
            });
        }

        let mut ids = HashSet::with_capacity(rules.len());
        let mut compiled = Vec::with_capacity(rules.len());
        let final_index = rules.len() - 1;
        for (index, rule) in rules.into_iter().enumerate() {
            if !ids.insert(rule.id.clone()) {
                return Err(RouteCompileError::DuplicateRuleId(rule.id));
            }
            if rule.action.dns_plan().is_some()
                && !rule.matcher.stages.is_empty()
                && rule
                    .matcher
                    .stages
                    .iter()
                    .all(|stage| *stage == RouteStage::PostResolution)
            {
                return Err(RouteCompileError::PostResolutionDnsPlan(rule.id));
            }
            if rule.matcher.is_catch_all() && index != final_index {
                return Err(RouteCompileError::ShadowingDefaultRule(rule.id));
            }
            if index == final_index && !rule.matcher.is_catch_all() {
                return Err(RouteCompileError::MissingDefaultRule);
            }
            let explanation = compile_explanation(rule.explanation, &rule.id)?;
            let matcher = CompiledMatcher::compile(rule.matcher, &rule.id)?;
            compiled.push(CompiledRouteRule {
                id: rule.id,
                matcher,
                action: rule.action,
                explanation,
            });
        }
        Ok(Self {
            generation,
            rules: compiled,
        })
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub fn classify<'table>(&'table self, input: RouteInput<'_>) -> RouteDecision<'table> {
        // A default rule is a compile-time invariant.
        let rule = self
            .rules
            .iter()
            .find(|rule| rule.matcher.matches(input))
            .expect("compiled route table has a final catch-all rule");
        RouteDecision {
            generation: self.generation,
            rule_id: &rule.id,
            action: &rule.action,
            explanation: &rule.explanation,
        }
    }

    /// Classify a pre-resolution flow and determine, in the same ordered rule
    /// pass, whether any earlier rule can require IP routing evidence or the
    /// selected rule cannot remain first after resolution.
    pub fn classify_pre_resolution<'table>(
        &'table self,
        flow: &FlowContext,
    ) -> (RouteDecision<'table>, bool) {
        if let Some(address) = flow.target().ip() {
            let pre = self.classify(RouteInput::pre_resolution(flow));
            let post = self.classify(RouteInput::post_resolution(flow, address));
            let requires_post_resolution = pre.rule_id() != post.rule_id();
            return (pre, requires_post_resolution);
        }

        let input = RouteInput::pre_resolution(flow);
        let mut earlier_post_candidate = false;
        for rule in &self.rules {
            if rule.matcher.matches(input) {
                let requires_post_resolution =
                    earlier_post_candidate || !rule.matcher.could_match_post_resolution(flow);
                return (
                    RouteDecision {
                        generation: self.generation,
                        rule_id: &rule.id,
                        action: &rule.action,
                        explanation: &rule.explanation,
                    },
                    requires_post_resolution,
                );
            }
            earlier_post_candidate |= rule.matcher.could_match_post_resolution(flow);
        }
        unreachable!("compiled route table has a final catch-all rule")
    }

    /// Return whether first-match routing for this flow can change once a
    /// domain has address evidence.
    ///
    /// Rules are inspected only up to the rule selected before resolution.
    /// An earlier rule whose non-address fields can match after resolution may
    /// become the first match for at least one answer, while a pre-resolution
    /// rule that cannot remain eligible after resolution must also advance to
    /// the post-resolution table. Rules after a stable selected rule cannot
    /// affect the result and therefore do not trigger DNS.
    pub fn requires_post_resolution(&self, flow: &FlowContext) -> bool {
        if let Some(address) = flow.target().ip() {
            let pre_input = RouteInput::pre_resolution(flow);
            let selected = self
                .rules
                .iter()
                .position(|rule| rule.matcher.matches(pre_input))
                .expect("compiled route table has a final catch-all rule");
            let post_input = RouteInput::post_resolution(flow, address);
            let post_selected = self
                .rules
                .iter()
                .position(|rule| rule.matcher.matches(post_input))
                .expect("compiled route table has a final catch-all rule");
            return post_selected != selected;
        }
        self.classify_pre_resolution(flow).1
    }

    /// Produce a bounded control-plane trace without changing classification
    /// semantics. Normal forwarding calls `classify` and allocates nothing;
    /// explicit route-explain/dry-run operations may allocate this trace.
    pub fn explain<'table>(&'table self, input: RouteInput<'_>) -> RouteExplanation<'table> {
        let mut selected = None;
        let mut rules = Vec::with_capacity(self.rules.len());
        for rule in &self.rules {
            let first_mismatch = rule.matcher.first_mismatch(input);
            let is_selected = selected.is_none() && first_mismatch.is_none();
            if is_selected {
                selected = Some(rule);
            }
            rules.push(RouteRuleTrace {
                rule_id: &rule.id,
                selected: is_selected,
                first_mismatch,
                domain_rule_sets: &rule.matcher.domain_rule_sets,
                destination_rule_sets: &rule.matcher.destination_rule_sets,
            });
        }
        let selected = selected.expect("compiled route table has a final matching catch-all rule");
        RouteExplanation {
            generation: self.generation,
            selected: RouteDecision {
                generation: self.generation,
                rule_id: &selected.id,
                action: &selected.action,
                explanation: &selected.explanation,
            },
            rules,
        }
    }

    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RouteDecision<'a> {
    generation: u64,
    rule_id: &'a RuleId,
    action: &'a RouteAction,
    explanation: &'a str,
}

impl<'a> RouteDecision<'a> {
    pub const fn generation(self) -> u64 {
        self.generation
    }

    pub const fn rule_id(self) -> &'a RuleId {
        self.rule_id
    }

    pub const fn action(self) -> &'a RouteAction {
        self.action
    }

    pub const fn explanation(self) -> &'a str {
        self.explanation
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteMismatch {
    Domain,
    DomainRuleSet,
    DestinationIp,
    DestinationRuleSet,
    SourceIp,
    DestinationPort,
    SourcePort,
    Network,
    Inbound,
    Principal,
    Stage,
}

impl fmt::Display for RouteMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Domain => "domain",
            Self::DomainRuleSet => "domain rule set",
            Self::DestinationIp => "destination IP",
            Self::DestinationRuleSet => "destination IP rule set",
            Self::SourceIp => "source IP",
            Self::DestinationPort => "destination port",
            Self::SourcePort => "source port",
            Self::Network => "network",
            Self::Inbound => "inbound",
            Self::Principal => "principal",
            Self::Stage => "route stage",
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RouteRuleTrace<'a> {
    rule_id: &'a RuleId,
    selected: bool,
    first_mismatch: Option<RouteMismatch>,
    domain_rule_sets: &'a [Arc<VerifiedRuleSet>],
    destination_rule_sets: &'a [Arc<VerifiedRuleSet>],
}

impl<'a> RouteRuleTrace<'a> {
    pub const fn rule_id(self) -> &'a RuleId {
        self.rule_id
    }

    pub const fn selected(self) -> bool {
        self.selected
    }

    pub const fn first_mismatch(self) -> Option<RouteMismatch> {
        self.first_mismatch
    }

    /// Verified signed sets consulted by the rule's domain category. Control
    /// surfaces can display each set's ID, publisher, revision, and checksum
    /// without adding work to normal classification.
    pub const fn domain_rule_sets(self) -> &'a [Arc<VerifiedRuleSet>] {
        self.domain_rule_sets
    }

    /// Verified signed sets consulted by the rule's destination-IP category.
    pub const fn destination_rule_sets(self) -> &'a [Arc<VerifiedRuleSet>] {
        self.destination_rule_sets
    }
}

#[derive(Debug)]
pub struct RouteExplanation<'a> {
    generation: u64,
    selected: RouteDecision<'a>,
    rules: Vec<RouteRuleTrace<'a>>,
}

impl<'a> RouteExplanation<'a> {
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn selected(&self) -> RouteDecision<'a> {
        self.selected
    }

    pub fn rules(&self) -> &[RouteRuleTrace<'a>] {
        &self.rules
    }
}

#[derive(Debug)]
pub(crate) struct CompiledMatcher {
    domain_exact: Vec<crate::product::DomainName>,
    domain_suffix: Vec<crate::product::DomainName>,
    domain_keyword: Vec<String>,
    domain_regex: Vec<Regex>,
    domain_rule_sets: Vec<Arc<VerifiedRuleSet>>,
    destination_cidrs: Vec<IpNet>,
    destination_rule_sets: Vec<Arc<VerifiedRuleSet>>,
    source_cidrs: Vec<IpNet>,
    destination_ports: Vec<PortRange>,
    source_ports: Vec<PortRange>,
    networks: Vec<Network>,
    inbounds: Vec<InboundId>,
    principals: Vec<PrincipalId>,
    stages: Vec<RouteStage>,
}

impl CompiledMatcher {
    pub(crate) fn compile(
        spec: RouteMatchSpec,
        rule_id: &RuleId,
    ) -> Result<Self, RouteCompileError> {
        for (field, count) in [
            ("domain_exact", spec.domain_exact.len()),
            ("domain_suffix", spec.domain_suffix.len()),
            ("domain_keyword", spec.domain_keyword.len()),
            ("domain_rule_sets", spec.domain_rule_sets.len()),
            ("destination_cidrs", spec.destination_cidrs.len()),
            ("destination_rule_sets", spec.destination_rule_sets.len()),
            ("source_cidrs", spec.source_cidrs.len()),
            ("destination_ports", spec.destination_ports.len()),
            ("source_ports", spec.source_ports.len()),
            ("networks", spec.networks.len()),
            ("inbounds", spec.inbounds.len()),
            ("principals", spec.principals.len()),
            ("stages", spec.stages.len()),
        ] {
            if count > MAX_VALUES_PER_FIELD {
                return Err(RouteCompileError::TooManyMatcherValues {
                    rule_id: rule_id.clone(),
                    field,
                    count,
                    maximum: MAX_VALUES_PER_FIELD,
                });
            }
        }
        if spec.domain_regex.len() > MAX_REGEX_PER_RULE {
            return Err(RouteCompileError::TooManyMatcherValues {
                rule_id: rule_id.clone(),
                field: "domain_regex",
                count: spec.domain_regex.len(),
                maximum: MAX_REGEX_PER_RULE,
            });
        }

        let mut keywords = Vec::with_capacity(spec.domain_keyword.len());
        for keyword in spec.domain_keyword {
            keywords.push(compile_domain_keyword(&keyword, rule_id)?);
        }
        let mut regexes = Vec::with_capacity(spec.domain_regex.len());
        for pattern in spec.domain_regex {
            if pattern.is_empty()
                || pattern.len() > MAX_REGEX_BYTES
                || pattern.chars().any(char::is_control)
            {
                return Err(RouteCompileError::InvalidDomainRegex {
                    rule_id: rule_id.clone(),
                    pattern,
                });
            }
            let regex = RegexBuilder::new(&pattern)
                .case_insensitive(true)
                .unicode(false)
                .size_limit(REGEX_SIZE_LIMIT)
                .dfa_size_limit(REGEX_DFA_SIZE_LIMIT)
                .build()
                .map_err(|_| RouteCompileError::InvalidDomainRegex {
                    rule_id: rule_id.clone(),
                    pattern: pattern.clone(),
                })?;
            regexes.push(regex);
        }

        Ok(Self {
            domain_exact: deduplicate(spec.domain_exact),
            domain_suffix: deduplicate(spec.domain_suffix),
            domain_keyword: deduplicate(keywords),
            domain_regex: regexes,
            domain_rule_sets: deduplicate_rule_sets(
                spec.domain_rule_sets,
                rule_id,
                "domain_rule_sets",
            )?,
            destination_cidrs: deduplicate(spec.destination_cidrs),
            destination_rule_sets: deduplicate_rule_sets(
                spec.destination_rule_sets,
                rule_id,
                "destination_rule_sets",
            )?,
            source_cidrs: deduplicate(spec.source_cidrs),
            destination_ports: deduplicate(spec.destination_ports),
            source_ports: deduplicate(spec.source_ports),
            networks: deduplicate(spec.networks),
            inbounds: deduplicate(spec.inbounds),
            principals: deduplicate(spec.principals),
            stages: deduplicate(spec.stages),
        })
    }

    pub(crate) fn matches(&self, input: RouteInput<'_>) -> bool {
        self.first_mismatch(input).is_none()
    }

    fn first_mismatch(&self, input: RouteInput<'_>) -> Option<RouteMismatch> {
        if !self.matches_domain(input) {
            return Some(
                if self.domain_exact.is_empty()
                    && self.domain_suffix.is_empty()
                    && self.domain_keyword.is_empty()
                    && self.domain_regex.is_empty()
                    && !self.domain_rule_sets.is_empty()
                {
                    RouteMismatch::DomainRuleSet
                } else {
                    RouteMismatch::Domain
                },
            );
        }
        if !self.matches_destination(input) {
            return Some(if self.destination_cidrs.is_empty() {
                RouteMismatch::DestinationRuleSet
            } else {
                RouteMismatch::DestinationIp
            });
        }
        self.first_mismatch_after_destination(input)
    }

    /// A non-empty address matcher may match at least one possible answer.
    /// Referenced sets without address entries cannot, so they must not turn
    /// an otherwise stable domain decision into a DNS dependency.
    pub(crate) fn could_match_post_resolution(&self, flow: &FlowContext) -> bool {
        let input = RouteInput {
            flow,
            stage: RouteStage::PostResolution,
            resolved_ip: None,
        };
        self.destination_can_match_post_resolution()
            && self.matches_domain(input)
            && self.first_mismatch_after_destination(input).is_none()
    }

    fn destination_can_match_post_resolution(&self) -> bool {
        !self.destination_cidrs.is_empty()
            || self.destination_rule_sets.is_empty()
            || self
                .destination_rule_sets
                .iter()
                .any(|rule_set| !rule_set.destination_cidrs().is_empty())
    }

    fn first_mismatch_after_destination(&self, input: RouteInput<'_>) -> Option<RouteMismatch> {
        if !matches_value(
            &self.source_cidrs,
            Some(input.flow().source().address()),
            |net, ip| net.contains(&ip),
        ) {
            return Some(RouteMismatch::SourceIp);
        }
        if !matches_value(
            &self.destination_ports,
            Some(input.flow().target().port().get()),
            |range, port| range.contains(port),
        ) {
            return Some(RouteMismatch::DestinationPort);
        }
        if !matches_value(
            &self.source_ports,
            Some(input.flow().source().port()),
            |range, port| range.contains(port),
        ) {
            return Some(RouteMismatch::SourcePort);
        }
        if !matches_eq(&self.networks, Some(input.flow().network())) {
            return Some(RouteMismatch::Network);
        }
        if !matches_ref(&self.inbounds, Some(input.flow().inbound())) {
            return Some(RouteMismatch::Inbound);
        }
        if !matches_ref(&self.principals, Some(input.flow().principal())) {
            return Some(RouteMismatch::Principal);
        }
        if !matches_eq(&self.stages, Some(input.stage())) {
            return Some(RouteMismatch::Stage);
        }
        None
    }

    fn matches_domain(&self, input: RouteInput<'_>) -> bool {
        let constrained = !self.domain_exact.is_empty()
            || !self.domain_suffix.is_empty()
            || !self.domain_keyword.is_empty()
            || !self.domain_regex.is_empty()
            || !self.domain_rule_sets.is_empty();
        if !constrained {
            return true;
        }
        let Some(domain) = input.flow().target().domain() else {
            return false;
        };
        let value = domain.as_str();
        self.domain_exact
            .iter()
            .any(|candidate| candidate == domain)
            || self
                .domain_suffix
                .iter()
                .any(|suffix| domain_has_suffix(value, suffix.as_str()))
            || self
                .domain_keyword
                .iter()
                .any(|keyword| value.contains(keyword))
            || self.domain_regex.iter().any(|regex| regex.is_match(value))
            || self
                .domain_rule_sets
                .iter()
                .any(|rule_set| rule_set.matches_domain(domain))
    }

    fn matches_destination(&self, input: RouteInput<'_>) -> bool {
        if self.destination_cidrs.is_empty() && self.destination_rule_sets.is_empty() {
            return true;
        }
        input.destination_ip().is_some_and(|address| {
            self.destination_cidrs
                .iter()
                .any(|network| network.contains(&address))
                || self
                    .destination_rule_sets
                    .iter()
                    .any(|rule_set| rule_set.matches_destination_ip(address))
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteCompileError {
    MissingDefaultRule,
    ShadowingDefaultRule(RuleId),
    DuplicateRuleId(RuleId),
    TooManyRules {
        count: usize,
        maximum: usize,
    },
    TooManyMatcherValues {
        rule_id: RuleId,
        field: &'static str,
        count: usize,
        maximum: usize,
    },
    InvalidPortRange {
        start: u16,
        end: u16,
    },
    InvalidDomainKeyword {
        rule_id: RuleId,
        keyword: String,
    },
    InvalidDomainRegex {
        rule_id: RuleId,
        pattern: String,
    },
    DuplicateRuleSetReference {
        rule_id: RuleId,
        field: &'static str,
        rule_set: crate::product::RuleSetId,
    },
    PostResolutionDnsPlan(RuleId),
    InvalidExplanation(RuleId),
}

impl fmt::Display for RouteCompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingDefaultRule => {
                formatter.write_str("route table requires a final catch-all rule")
            }
            Self::ShadowingDefaultRule(id) => {
                write!(formatter, "catch-all route rule {id} must be last")
            }
            Self::DuplicateRuleId(id) => write!(formatter, "duplicate route rule ID {id}"),
            Self::TooManyRules { count, maximum } => {
                write!(
                    formatter,
                    "route table has {count} rules; maximum is {maximum}"
                )
            }
            Self::TooManyMatcherValues {
                rule_id,
                field,
                count,
                maximum,
            } => write!(
                formatter,
                "route rule {rule_id} field {field} has {count} values; maximum is {maximum}"
            ),
            Self::InvalidPortRange { start, end } => {
                write!(formatter, "invalid port range {start}-{end}")
            }
            Self::InvalidDomainKeyword { rule_id, .. } => {
                write!(
                    formatter,
                    "route rule {rule_id} has an invalid domain keyword"
                )
            }
            Self::InvalidDomainRegex { rule_id, .. } => {
                write!(
                    formatter,
                    "route rule {rule_id} has an invalid bounded domain regex"
                )
            }
            Self::DuplicateRuleSetReference {
                rule_id,
                field,
                rule_set,
            } => write!(
                formatter,
                "route rule {rule_id} field {field} references rule set {rule_set} more than once"
            ),
            Self::PostResolutionDnsPlan(rule_id) => write!(
                formatter,
                "post-resolution-only route rule {rule_id} cannot select a DNS plan"
            ),
            Self::InvalidExplanation(rule_id) => {
                write!(
                    formatter,
                    "route rule {rule_id} has unsafe explanation text"
                )
            }
        }
    }
}

impl Error for RouteCompileError {}

fn compile_domain_keyword(keyword: &str, rule_id: &RuleId) -> Result<String, RouteCompileError> {
    if keyword.is_empty()
        || keyword.len() > 253
        || keyword.chars().any(char::is_control)
        || keyword.contains(['/', '\\', '?', '#', '@', ':', '[', ']'])
    {
        return Err(RouteCompileError::InvalidDomainKeyword {
            rule_id: rule_id.clone(),
            keyword: keyword.to_owned(),
        });
    }
    let canonical = domain_to_ascii_strict(keyword)
        .map_err(|_| RouteCompileError::InvalidDomainKeyword {
            rule_id: rule_id.clone(),
            keyword: keyword.to_owned(),
        })?
        .to_ascii_lowercase();
    if canonical.is_empty() {
        return Err(RouteCompileError::InvalidDomainKeyword {
            rule_id: rule_id.clone(),
            keyword: keyword.to_owned(),
        });
    }
    Ok(canonical)
}

fn compile_explanation(
    explanation: Option<String>,
    rule_id: &RuleId,
) -> Result<String, RouteCompileError> {
    match explanation {
        Some(explanation)
            if !explanation.is_empty()
                && explanation.len() <= MAX_EXPLANATION_BYTES
                && !explanation.chars().any(char::is_control) =>
        {
            Ok(explanation)
        }
        Some(_) => Err(RouteCompileError::InvalidExplanation(rule_id.clone())),
        None => Ok(format!("matched route rule '{}'", rule_id.as_str())),
    }
}

fn domain_has_suffix(domain: &str, suffix: &str) -> bool {
    domain == suffix
        || domain
            .strip_suffix(suffix)
            .is_some_and(|prefix| prefix.ends_with('.'))
}

fn matches_value<T, V>(
    candidates: &[T],
    value: Option<V>,
    predicate: impl Fn(&T, V) -> bool,
) -> bool
where
    V: Copy,
{
    candidates.is_empty()
        || value.is_some_and(|value| {
            candidates
                .iter()
                .any(|candidate| predicate(candidate, value))
        })
}

fn matches_eq<T: PartialEq>(candidates: &[T], value: Option<T>) -> bool {
    candidates.is_empty() || value.is_some_and(|value| candidates.contains(&value))
}

fn matches_ref<T: PartialEq>(candidates: &[T], value: Option<&T>) -> bool {
    candidates.is_empty()
        || value.is_some_and(|value| candidates.iter().any(|candidate| candidate == value))
}

fn deduplicate<T>(values: Vec<T>) -> Vec<T>
where
    T: Eq + std::hash::Hash + Clone,
{
    let mut seen = HashSet::with_capacity(values.len());
    values
        .into_iter()
        .filter(|value| seen.insert(value.clone()))
        .collect()
}

fn deduplicate_rule_sets(
    values: Vec<Arc<VerifiedRuleSet>>,
    rule_id: &RuleId,
    field: &'static str,
) -> Result<Vec<Arc<VerifiedRuleSet>>, RouteCompileError> {
    let mut seen = HashSet::with_capacity(values.len());
    let mut deduplicated = Vec::with_capacity(values.len());
    for value in values {
        if !seen.insert(value.id().clone()) {
            return Err(RouteCompileError::DuplicateRuleSetReference {
                rule_id: rule_id.clone(),
                field,
                rule_set: value.id().clone(),
            });
        }
        deduplicated.push(value);
    }
    Ok(deduplicated)
}

const fn canonical_ip(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V6(address) if address.is_unspecified() || address.is_loopback() => {
            IpAddr::V6(address)
        }
        IpAddr::V6(address) => match address.to_ipv4() {
            Some(address) => IpAddr::V4(address),
            None => IpAddr::V6(address),
        },
        IpAddr::V4(address) => IpAddr::V4(address),
    }
}

#[cfg(test)]
#[path = "tests_routing.rs"]
mod tests;
