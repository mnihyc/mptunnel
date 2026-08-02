use crate::product::{Network, NetworkSet, OutboundId, PrincipalId, ProtocolTarget};
use hashbrown::{Equivalent, HashMap};
use std::collections::hash_map::RandomState;
use std::error::Error;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::time::Duration;

pub const MAX_GATEWAY_MEMBERS: usize = 256;
pub const MAX_GATEWAY_STICKY_DESTINATIONS: usize = 65_536;

const MAX_HEALTH_THRESHOLD: u32 = 1_024;
const MAX_POLICY_DURATION: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const DEFAULT_GATEWAY_FRESHNESS: Duration = Duration::from_secs(30);
const LATENCY_EWMA_OLD_SAMPLES: u128 = 7;
const LATENCY_EWMA_TOTAL_SAMPLES: u128 = 8;

/// A monotonic millisecond timestamp relative to the owning runtime
/// generation. Wall-clock time must never be supplied here.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GatewayInstant(u64);

impl GatewayInstant {
    pub const ZERO: Self = Self(0);

    pub const fn from_millis(milliseconds: u64) -> Self {
        Self(milliseconds)
    }

    pub const fn as_millis(self) -> u64 {
        self.0
    }

    fn add(self, duration: Duration) -> Self {
        let milliseconds = duration.as_millis().min(u128::from(u64::MAX)) as u64;
        Self(self.0.saturating_add(milliseconds))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayStrategy {
    Manual,
    OrderedFailover,
    RoundRobin,
    Random,
    WeightedRandom,
    LeastLatency,
    LeastLoad,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayMemberSpec {
    /// The referenced `[[outbounds]].name`, represented internally as a typed
    /// ID so it cannot be confused with a balancer name or wire identity.
    pub id: OutboundId,
    /// Relative capacity for weighted-random and least-load selection.
    pub weight: u32,
    /// Target networks this leaf can open. This is immutable for one
    /// configuration generation and is checked before health/stickiness.
    pub networks: NetworkSet,
    pub mode: GatewayMemberMode,
}

impl GatewayMemberSpec {
    pub const fn new(id: OutboundId, weight: u32, networks: NetworkSet) -> Self {
        Self {
            id,
            weight,
            networks,
            mode: GatewayMemberMode::Enabled,
        }
    }

    pub const fn with_mode(mut self, mode: GatewayMemberMode) -> Self {
        self.mode = mode;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GatewayHealthPolicy {
    pub failure_threshold: u32,
    pub recovery_threshold: u32,
    pub initial_backoff: Duration,
    pub maximum_backoff: Duration,
}

impl Default for GatewayHealthPolicy {
    fn default() -> Self {
        Self {
            failure_threshold: 3,
            recovery_threshold: 2,
            initial_backoff: Duration::from_secs(1),
            maximum_backoff: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GatewayStickinessPolicy {
    pub ttl: Duration,
    pub capacity: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayStickinessKey {
    Destination,
    Principal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayProbePolicy {
    /// Literal TCP endpoint opened through each member. Product probes never
    /// consult routing or Core carrier metrics.
    pub target: ProtocolTarget,
    pub interval: Duration,
    pub timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayBalancerSpec {
    pub strategy: GatewayStrategy,
    pub members: Vec<GatewayMemberSpec>,
    pub health: GatewayHealthPolicy,
    pub stickiness: Option<GatewayStickinessPolicy>,
    pub stickiness_key: GatewayStickinessKey,
    /// Initial explicit pin. `Manual` requires one; automatic strategies may
    /// also start pinned and later return to automatic selection through an
    /// explicit control action.
    pub manual_member: Option<OutboundId>,
    pub probe: Option<GatewayProbePolicy>,
    /// Age after which an observation remains visible but no longer ranks as
    /// fresh latency evidence.
    pub freshness_ttl: Duration,
}

impl GatewayBalancerSpec {
    pub fn new(strategy: GatewayStrategy, members: Vec<GatewayMemberSpec>) -> Self {
        Self {
            strategy,
            members,
            health: GatewayHealthPolicy::default(),
            stickiness: None,
            stickiness_key: GatewayStickinessKey::Destination,
            manual_member: None,
            probe: None,
            freshness_ttl: DEFAULT_GATEWAY_FRESHNESS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GatewayCompileError {
    MissingMembers,
    TooManyMembers { count: usize, maximum: usize },
    DuplicateMember(OutboundId),
    ZeroWeight(OutboundId),
    MissingNetworkCapability(OutboundId),
    InvalidFailureThreshold(u32),
    InvalidRecoveryThreshold(u32),
    InvalidBackoff,
    InvalidStickinessTtl,
    InvalidStickinessCapacity { capacity: usize, maximum: usize },
    MissingManualMember,
    UnknownManualMember(OutboundId),
    ManualMemberNotEnabled(OutboundId),
    InvalidFreshnessTtl,
    InvalidProbeInterval,
    InvalidProbeTimeout,
    ProbeTargetMustBeLiteralIp,
    ProbeMemberDoesNotSupportTcp(OutboundId),
}

impl fmt::Display for GatewayCompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingMembers => formatter.write_str("balancer requires members"),
            Self::TooManyMembers { count, maximum } => {
                write!(
                    formatter,
                    "balancer has {count} members; maximum is {maximum}"
                )
            }
            Self::DuplicateMember(id) => write!(formatter, "duplicate balancer member {id}"),
            Self::ZeroWeight(id) => write!(formatter, "balancer member {id} has zero weight"),
            Self::MissingNetworkCapability(id) => {
                write!(formatter, "balancer member {id} supports no target network")
            }
            Self::InvalidFailureThreshold(value) => write!(
                formatter,
                "balancer failure threshold {value} is outside 1..={MAX_HEALTH_THRESHOLD}"
            ),
            Self::InvalidRecoveryThreshold(value) => write!(
                formatter,
                "balancer recovery threshold {value} is outside 1..={MAX_HEALTH_THRESHOLD}"
            ),
            Self::InvalidBackoff => formatter.write_str(
                "balancer backoff must be millisecond-granular, non-zero, bounded, and ordered",
            ),
            Self::InvalidStickinessTtl => formatter.write_str(
                "balancer stickiness TTL must be millisecond-granular, non-zero, and bounded",
            ),
            Self::InvalidStickinessCapacity { capacity, maximum } => write!(
                formatter,
                "balancer stickiness capacity {capacity} is outside 1..={maximum}"
            ),
            Self::MissingManualMember => {
                formatter.write_str("manual balancer strategy requires manual_outbound")
            }
            Self::UnknownManualMember(id) => {
                write!(formatter, "balancer manual outbound {id} is not a configured member")
            }
            Self::ManualMemberNotEnabled(id) => {
                write!(formatter, "balancer manual outbound {id} is not enabled")
            }
            Self::InvalidFreshnessTtl => formatter.write_str(
                "balancer freshness TTL must be millisecond-granular, non-zero, and bounded",
            ),
            Self::InvalidProbeInterval => formatter.write_str(
                "balancer probe interval must be millisecond-granular, non-zero, and bounded",
            ),
            Self::InvalidProbeTimeout => formatter.write_str(
                "balancer probe timeout must be millisecond-granular, non-zero, bounded, and no longer than its interval",
            ),
            Self::ProbeTargetMustBeLiteralIp => {
                formatter.write_str("balancer probe target must be a literal IP authority")
            }
            Self::ProbeMemberDoesNotSupportTcp(id) => {
                write!(formatter, "balancer probe member {id} does not support TCP")
            }
        }
    }
}

impl Error for GatewayCompileError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GatewayMemberHandle {
    generation: u64,
    slot: u16,
}

impl GatewayMemberHandle {
    pub const fn generation(self) -> u64 {
        self.generation
    }

    pub const fn slot(self) -> u16 {
        self.slot
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayMemberMode {
    Enabled,
    Draining,
    Disabled,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GatewayLoad {
    pub active_flows: u32,
    pub pending_flows: u32,
}

impl GatewayLoad {
    pub const fn total(self) -> u64 {
        self.active_flows as u64 + self.pending_flows as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayOutcome {
    Success { latency: Option<Duration> },
    Failure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayObservationSource {
    ActiveProbe,
    PassiveOpen,
    PassiveFlow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayHealthStatus {
    Healthy,
    BackingOff { until: GatewayInstant },
    RecoveryProbeEligible,
    RecoveryProbeInFlight,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayFreshnessStatus {
    NeverObserved,
    Fresh {
        observed_at: GatewayInstant,
    },
    Stale {
        observed_at: GatewayInstant,
        stale_since: GatewayInstant,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewaySelectionReason {
    Manual,
    OrderedFailover,
    RoundRobin,
    Random,
    WeightedRandom,
    LeastLatency,
    LeastLoad,
    DestinationSticky,
    PrincipalSticky,
    AllUnhealthyRecoveryProbe,
    AllUnhealthyDeferred { until: GatewayInstant },
    AllUnhealthyRecoveryInFlight,
}

#[derive(Debug, Clone, Copy)]
pub struct GatewaySelection<'a> {
    handle: GatewayMemberHandle,
    member: &'a OutboundId,
    reason: GatewaySelectionReason,
}

impl<'a> GatewaySelection<'a> {
    pub const fn handle(self) -> GatewayMemberHandle {
        self.handle
    }

    pub const fn member(self) -> &'a OutboundId {
        self.member
    }

    pub const fn reason(self) -> GatewaySelectionReason {
        self.reason
    }

    /// False means the selected fallback is only a deterministic retry plan;
    /// the caller must not connect until a later selection grants the probe.
    pub const fn may_attempt(self) -> bool {
        !matches!(
            self.reason,
            GatewaySelectionReason::AllUnhealthyDeferred { .. }
                | GatewaySelectionReason::AllUnhealthyRecoveryInFlight
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GatewayMemberStatus<'a> {
    pub handle: GatewayMemberHandle,
    pub member: &'a OutboundId,
    pub networks: NetworkSet,
    pub mode: GatewayMemberMode,
    pub health: GatewayHealthStatus,
    pub freshness: GatewayFreshnessStatus,
    pub probe_in_flight: bool,
    pub consecutive_failures: u32,
    pub recovery_successes: u32,
    pub latency_ewma: Option<Duration>,
    pub last_latency_observation: Option<GatewayInstant>,
    pub last_latency_observation_source: Option<GatewayObservationSource>,
    pub load: GatewayLoad,
    pub last_observation: Option<GatewayInstant>,
    pub last_observation_source: Option<GatewayObservationSource>,
    pub last_error: Option<&'a str>,
    pub last_error_at: Option<GatewayInstant>,
    pub last_selection_reason: Option<GatewaySelectionReason>,
    pub last_selected_at: Option<GatewayInstant>,
    pub counters: GatewayMemberCounters,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GatewayMemberCounters {
    pub selections: u64,
    pub open_attempts: u64,
    pub open_successes: u64,
    pub open_failures: u64,
    pub flow_successes: u64,
    pub flow_failures: u64,
    pub probes: u64,
    pub probe_successes: u64,
    pub probe_failures: u64,
    pub ejections: u64,
    pub recoveries: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayStateError {
    ForeignHandle,
    ManualStrategyRequiresOverride,
    ManualOverrideMemberNotEnabled,
}

impl fmt::Display for GatewayStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ForeignHandle => "balancer member handle belongs to another generation",
            Self::ManualStrategyRequiresOverride => {
                "manual balancer strategy cannot clear its outbound override"
            }
            Self::ManualOverrideMemberNotEnabled => "balancer manual outbound must be enabled",
        })
    }
}

impl Error for GatewayStateError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewaySelectionError {
    TooManyExclusions { count: usize, maximum: usize },
    ForeignExclusionHandle,
    NoCompatibleMembers(Network),
    NoEnabledMembers(Network),
    ManualMemberUnavailable(Network),
}

impl fmt::Display for GatewaySelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyExclusions { count, maximum } => write!(
                formatter,
                "balancer selection has {count} exclusions; maximum is {maximum}"
            ),
            Self::ForeignExclusionHandle => {
                formatter.write_str("balancer exclusion handle belongs to another generation")
            }
            Self::NoCompatibleMembers(network) => {
                write!(formatter, "balancer has no {network}-capable members")
            }
            Self::NoEnabledMembers(network) => write!(
                formatter,
                "balancer has no enabled {network}-capable members for a new flow"
            ),
            Self::ManualMemberUnavailable(network) => write!(
                formatter,
                "balancer manual outbound is not healthy, enabled, and {network}-capable"
            ),
        }
    }
}

impl Error for GatewaySelectionError {}

/// Runtime-provided entropy. Tests can inject an exact deterministic sequence;
/// production can adapt its process RNG without coupling this crate to it.
pub trait GatewayEntropy {
    fn next_u64(&mut self) -> u64;
}

impl<F> GatewayEntropy for F
where
    F: FnMut() -> u64,
{
    fn next_u64(&mut self) -> u64 {
        self()
    }
}

#[derive(Debug)]
struct GatewayMemberState {
    id: OutboundId,
    weight: u32,
    networks: NetworkSet,
    mode: GatewayMemberMode,
    healthy: bool,
    consecutive_failures: u32,
    recovery_successes: u32,
    backoff_until: GatewayInstant,
    recovery_probe_in_flight: bool,
    active_probe_in_flight: bool,
    latency_ewma_micros: Option<u64>,
    last_latency_observation: Option<GatewayInstant>,
    load: GatewayLoad,
    last_observation: Option<GatewayInstant>,
    observability: Box<GatewayMemberObservability>,
}

#[derive(Debug, Default)]
struct GatewayMemberObservability {
    last_observation_source: Option<GatewayObservationSource>,
    last_latency_observation_source: Option<GatewayObservationSource>,
    last_error: Option<String>,
    last_error_at: Option<GatewayInstant>,
    last_selection_reason: Option<GatewaySelectionReason>,
    last_selected_at: Option<GatewayInstant>,
    counters: GatewayMemberCounters,
}

impl GatewayMemberState {
    fn new(spec: GatewayMemberSpec) -> Self {
        Self {
            id: spec.id,
            weight: spec.weight,
            networks: spec.networks,
            mode: spec.mode,
            healthy: true,
            consecutive_failures: 0,
            recovery_successes: 0,
            backoff_until: GatewayInstant::ZERO,
            recovery_probe_in_flight: false,
            active_probe_in_flight: false,
            latency_ewma_micros: None,
            last_latency_observation: None,
            load: GatewayLoad::default(),
            last_observation: None,
            observability: Box::default(),
        }
    }

    fn supports(&self, network: Network) -> bool {
        self.networks.contains(network)
    }

    fn eligible_for_new_flow(&self, network: Network, excluded: bool) -> bool {
        self.mode == GatewayMemberMode::Enabled
            && self.healthy
            && self.supports(network)
            && !excluded
    }
}

#[derive(Debug, Clone, Copy)]
struct StickyEntry {
    member_slot: u16,
    expires_at: GatewayInstant,
    last_used_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StickyKey {
    Destination {
        network: Network,
        destination: ProtocolTarget,
    },
    Principal {
        network: Network,
        principal: PrincipalId,
    },
}

#[derive(Clone, Copy)]
enum StickyKeyRef<'a> {
    Destination {
        network: Network,
        destination: &'a ProtocolTarget,
    },
    Principal {
        network: Network,
        principal: &'a PrincipalId,
    },
}

impl Hash for StickyKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Destination {
                network,
                destination,
            } => hash_sticky_destination(*network, destination, state),
            Self::Principal { network, principal } => {
                hash_sticky_principal(*network, principal, state)
            }
        }
    }
}

impl Hash for StickyKeyRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Destination {
                network,
                destination,
            } => hash_sticky_destination(*network, destination, state),
            Self::Principal { network, principal } => {
                hash_sticky_principal(*network, principal, state)
            }
        }
    }
}

impl Equivalent<StickyKey> for StickyKeyRef<'_> {
    fn equivalent(&self, key: &StickyKey) -> bool {
        match (self, key) {
            (
                Self::Destination {
                    network: left_network,
                    destination: left_destination,
                },
                StickyKey::Destination {
                    network: right_network,
                    destination: right_destination,
                },
            ) => left_network == right_network && *left_destination == right_destination,
            (
                Self::Principal {
                    network: left_network,
                    principal: left_principal,
                },
                StickyKey::Principal {
                    network: right_network,
                    principal: right_principal,
                },
            ) => left_network == right_network && *left_principal == right_principal,
            _ => false,
        }
    }
}

fn hash_sticky_destination<H: Hasher>(
    network: Network,
    destination: &ProtocolTarget,
    state: &mut H,
) {
    0_u8.hash(state);
    network.hash(state);
    destination.hash(state);
}

fn hash_sticky_principal<H: Hasher>(network: Network, principal: &PrincipalId, state: &mut H) {
    1_u8.hash(state);
    network.hash(state);
    principal.hash(state);
}

/// Mutable Product state for selecting an independent egress for a new flow.
///
/// The returned handle is a flow binding, not a migration instruction.
/// Established reliable flows remain on their selected server even if a later
/// call selects another member.
#[derive(Debug)]
pub struct GatewayBalancer {
    generation: u64,
    strategy: GatewayStrategy,
    members: Vec<GatewayMemberState>,
    health: GatewayHealthPolicy,
    stickiness: Option<GatewayStickinessPolicy>,
    stickiness_key: GatewayStickinessKey,
    manual_override: Option<u16>,
    probe: Option<GatewayProbePolicy>,
    freshness_ttl: Duration,
    sticky: HashMap<StickyKey, StickyEntry, RandomState>,
    sticky_sequence: u64,
    round_robin_cursor: usize,
    observed_now: GatewayInstant,
}

impl GatewayBalancer {
    pub fn compile(
        generation: u64,
        spec: GatewayBalancerSpec,
    ) -> Result<Self, GatewayCompileError> {
        validate_spec(&spec)?;
        let sticky_capacity = spec.stickiness.map_or(0, |stickiness| stickiness.capacity);
        let manual_override = spec.manual_member.as_ref().map(|manual| {
            spec.members
                .iter()
                .position(|member| &member.id == manual)
                .expect("validated manual balancer member exists") as u16
        });
        Ok(Self {
            generation,
            strategy: spec.strategy,
            members: spec
                .members
                .into_iter()
                .map(GatewayMemberState::new)
                .collect(),
            health: spec.health,
            stickiness: spec.stickiness,
            stickiness_key: spec.stickiness_key,
            manual_override,
            probe: spec.probe,
            freshness_ttl: spec.freshness_ttl,
            sticky: HashMap::with_capacity_and_hasher(sticky_capacity, RandomState::new()),
            sticky_sequence: 0,
            round_robin_cursor: 0,
            observed_now: GatewayInstant::ZERO,
        })
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    pub const fn strategy(&self) -> GatewayStrategy {
        self.strategy
    }

    pub const fn probe_policy(&self) -> Option<&GatewayProbePolicy> {
        self.probe.as_ref()
    }

    pub fn manual_override(&self) -> Option<GatewayMemberHandle> {
        self.manual_override
            .map(|slot| self.handle(usize::from(slot)))
    }

    pub fn handle_for(&self, id: &OutboundId) -> Option<GatewayMemberHandle> {
        self.members
            .iter()
            .position(|member| &member.id == id)
            .map(|slot| self.handle(slot))
    }

    pub fn member_id(&self, handle: GatewayMemberHandle) -> Result<&OutboundId, GatewayStateError> {
        Ok(&self.member(handle)?.id)
    }

    pub fn set_member_mode(
        &mut self,
        handle: GatewayMemberHandle,
        mode: GatewayMemberMode,
    ) -> Result<(), GatewayStateError> {
        let member = self.member_mut(handle)?;
        member.mode = mode;
        if mode != GatewayMemberMode::Enabled {
            member.recovery_probe_in_flight = false;
            member.active_probe_in_flight = false;
        }
        Ok(())
    }

    pub fn set_manual_override(
        &mut self,
        handle: Option<GatewayMemberHandle>,
    ) -> Result<(), GatewayStateError> {
        let slot = match handle {
            Some(handle) => {
                if self.member(handle)?.mode != GatewayMemberMode::Enabled {
                    return Err(GatewayStateError::ManualOverrideMemberNotEnabled);
                }
                Some(handle.slot)
            }
            None if self.strategy == GatewayStrategy::Manual => {
                return Err(GatewayStateError::ManualStrategyRequiresOverride);
            }
            None => None,
        };
        self.manual_override = slot;
        Ok(())
    }

    pub fn set_load(
        &mut self,
        handle: GatewayMemberHandle,
        load: GatewayLoad,
    ) -> Result<(), GatewayStateError> {
        self.member_mut(handle)?.load = load;
        Ok(())
    }

    pub fn observe_passive(
        &mut self,
        handle: GatewayMemberHandle,
        now: GatewayInstant,
        outcome: GatewayOutcome,
    ) -> Result<(), GatewayStateError> {
        self.observe_outcome(
            handle,
            now,
            GatewayObservationSource::PassiveOpen,
            outcome,
            None,
        )
    }

    pub fn observe_outcome(
        &mut self,
        handle: GatewayMemberHandle,
        now: GatewayInstant,
        source: GatewayObservationSource,
        outcome: GatewayOutcome,
        error: Option<String>,
    ) -> Result<(), GatewayStateError> {
        let now = self.advance_time(now);
        self.observe(handle, now, source, outcome, error)
    }

    pub fn record_open_attempt(
        &mut self,
        handle: GatewayMemberHandle,
    ) -> Result<(), GatewayStateError> {
        let member = self.member_mut(handle)?;
        member.observability.counters.open_attempts = member
            .observability
            .counters
            .open_attempts
            .saturating_add(1);
        Ok(())
    }

    pub fn claim_recovery_probe(
        &mut self,
        handle: GatewayMemberHandle,
        now: GatewayInstant,
    ) -> Result<bool, GatewayStateError> {
        let now = self.advance_time(now);
        let member = self.member_mut(handle)?;
        if member.mode != GatewayMemberMode::Enabled
            || member.healthy
            || member.recovery_probe_in_flight
            || now < member.backoff_until
        {
            return Ok(false);
        }
        member.recovery_probe_in_flight = true;
        Ok(true)
    }

    pub fn claim_active_probe(
        &mut self,
        handle: GatewayMemberHandle,
        now: GatewayInstant,
    ) -> Result<bool, GatewayStateError> {
        let now = self.advance_time(now);
        let member = self.member_mut(handle)?;
        if member.mode != GatewayMemberMode::Enabled
            || member.active_probe_in_flight
            || (!member.healthy && (member.recovery_probe_in_flight || now < member.backoff_until))
        {
            return Ok(false);
        }
        member.active_probe_in_flight = true;
        if !member.healthy {
            member.recovery_probe_in_flight = true;
        }
        member.observability.counters.probes =
            member.observability.counters.probes.saturating_add(1);
        Ok(true)
    }

    pub fn cancel_active_probe(
        &mut self,
        handle: GatewayMemberHandle,
    ) -> Result<(), GatewayStateError> {
        let member = self.member_mut(handle)?;
        member.active_probe_in_flight = false;
        member.recovery_probe_in_flight = false;
        Ok(())
    }

    pub fn cancel_recovery_probe(
        &mut self,
        handle: GatewayMemberHandle,
    ) -> Result<(), GatewayStateError> {
        self.member_mut(handle)?.recovery_probe_in_flight = false;
        Ok(())
    }

    pub fn member_status(
        &self,
        handle: GatewayMemberHandle,
        now: GatewayInstant,
    ) -> Result<GatewayMemberStatus<'_>, GatewayStateError> {
        let now = now.max(self.observed_now);
        let member = self.member(handle)?;
        let health = health_status(member, now);
        Ok(GatewayMemberStatus {
            handle,
            member: &member.id,
            networks: member.networks,
            mode: member.mode,
            health,
            freshness: freshness_status(member, now, self.freshness_ttl),
            probe_in_flight: member.active_probe_in_flight,
            consecutive_failures: member.consecutive_failures,
            recovery_successes: member.recovery_successes,
            latency_ewma: member.latency_ewma_micros.map(Duration::from_micros),
            last_latency_observation: member.last_latency_observation,
            last_latency_observation_source: member.observability.last_latency_observation_source,
            load: member.load,
            last_observation: member.last_observation,
            last_observation_source: member.observability.last_observation_source,
            last_error: member.observability.last_error.as_deref(),
            last_error_at: member.observability.last_error_at,
            last_selection_reason: member.observability.last_selection_reason,
            last_selected_at: member.observability.last_selected_at,
            counters: member.observability.counters,
        })
    }

    pub fn select<'a>(
        &'a mut self,
        now: GatewayInstant,
        network: Network,
        destination: Option<&ProtocolTarget>,
        excluded_handles: &[GatewayMemberHandle],
        entropy: &mut impl GatewayEntropy,
    ) -> Result<GatewaySelection<'a>, GatewaySelectionError> {
        self.select_with_principal(now, network, destination, None, excluded_handles, entropy)
    }

    pub fn select_with_principal<'a>(
        &'a mut self,
        now: GatewayInstant,
        network: Network,
        destination: Option<&ProtocolTarget>,
        principal: Option<&PrincipalId>,
        excluded_handles: &[GatewayMemberHandle],
        entropy: &mut impl GatewayEntropy,
    ) -> Result<GatewaySelection<'a>, GatewaySelectionError> {
        let excluded = self.exclusion_mask(excluded_handles)?;
        if !self.members.iter().any(|member| member.supports(network)) {
            return Err(GatewaySelectionError::NoCompatibleMembers(network));
        }
        let now = self.advance_time(now);
        if let Some(slot) = self.manual_override.map(usize::from) {
            let member = &mut self.members[slot];
            if member.mode != GatewayMemberMode::Enabled
                || !member.supports(network)
                || excluded[slot]
            {
                return Err(GatewaySelectionError::ManualMemberUnavailable(network));
            }
            let reason = if member.healthy {
                GatewaySelectionReason::Manual
            } else if member.recovery_probe_in_flight {
                GatewaySelectionReason::AllUnhealthyRecoveryInFlight
            } else if now < member.backoff_until {
                GatewaySelectionReason::AllUnhealthyDeferred {
                    until: member.backoff_until,
                }
            } else {
                member.recovery_probe_in_flight = true;
                GatewaySelectionReason::AllUnhealthyRecoveryProbe
            };
            self.record_selection(slot, now, reason);
            return Ok(self.selection(slot, reason));
        }
        if let Some((slot, reason)) =
            self.sticky_member(network, destination, principal, now, &excluded)
        {
            self.record_selection(slot, now, reason);
            return Ok(self.selection(slot, reason));
        }

        let (slot, reason) = if self
            .members
            .iter()
            .enumerate()
            .any(|(slot, member)| member.eligible_for_new_flow(network, excluded[slot]))
        {
            self.select_healthy(network, &excluded, entropy)
        } else {
            self.select_all_unhealthy_fallback(now, network, &excluded)?
        };

        if reason_allows_stickiness(reason) {
            self.remember_sticky(network, destination, principal, slot, now);
        }
        self.record_selection(slot, now, reason);
        Ok(self.selection(slot, reason))
    }

    fn select_healthy(
        &mut self,
        network: Network,
        excluded: &[bool; MAX_GATEWAY_MEMBERS],
        entropy: &mut impl GatewayEntropy,
    ) -> (usize, GatewaySelectionReason) {
        match self.strategy {
            GatewayStrategy::Manual => {
                unreachable!("validated manual strategy always has an override")
            }
            GatewayStrategy::OrderedFailover => (
                self.first_healthy(network, excluded)
                    .expect("healthy compatible member exists"),
                GatewaySelectionReason::OrderedFailover,
            ),
            GatewayStrategy::RoundRobin => {
                let slot = (0..self.members.len())
                    .map(|offset| (self.round_robin_cursor + offset) % self.members.len())
                    .find(|slot| {
                        self.members[*slot].eligible_for_new_flow(network, excluded[*slot])
                    })
                    .expect("healthy compatible member exists");
                self.round_robin_cursor = (slot + 1) % self.members.len();
                (slot, GatewaySelectionReason::RoundRobin)
            }
            GatewayStrategy::Random => {
                let eligible = self
                    .members
                    .iter()
                    .enumerate()
                    .filter(|(slot, member)| member.eligible_for_new_flow(network, excluded[*slot]))
                    .count();
                let target = ((u128::from(entropy.next_u64()) * eligible as u128) >> 64) as usize;
                let slot = self
                    .members
                    .iter()
                    .enumerate()
                    .filter(|(slot, member)| member.eligible_for_new_flow(network, excluded[*slot]))
                    .nth(target)
                    .map(|(slot, _)| slot)
                    .expect("healthy compatible member exists");
                (slot, GatewaySelectionReason::Random)
            }
            GatewayStrategy::WeightedRandom => {
                let total_weight = self
                    .members
                    .iter()
                    .enumerate()
                    .filter(|(slot, member)| member.eligible_for_new_flow(network, excluded[*slot]))
                    .map(|(_, member)| u64::from(member.weight))
                    .sum::<u64>();
                let target =
                    ((u128::from(entropy.next_u64()) * u128::from(total_weight)) >> 64) as u64;
                let mut cumulative = 0_u64;
                let slot = self
                    .members
                    .iter()
                    .enumerate()
                    .filter(|(slot, member)| member.eligible_for_new_flow(network, excluded[*slot]))
                    .find_map(|(slot, member)| {
                        cumulative = cumulative.saturating_add(u64::from(member.weight));
                        (target < cumulative).then_some(slot)
                    })
                    .expect("positive healthy compatible weight exists");
                (slot, GatewaySelectionReason::WeightedRandom)
            }
            GatewayStrategy::LeastLatency => {
                let slot = self
                    .members
                    .iter()
                    .enumerate()
                    .filter(|(slot, member)| member.eligible_for_new_flow(network, excluded[*slot]))
                    .min_by_key(|(slot, member)| {
                        (
                            fresh_latency_micros(member, self.observed_now, self.freshness_ttl)
                                .unwrap_or(u64::MAX),
                            *slot,
                        )
                    })
                    .map(|(slot, _)| slot)
                    .expect("healthy compatible member exists");
                (slot, GatewaySelectionReason::LeastLatency)
            }
            GatewayStrategy::LeastLoad => {
                let slot = self
                    .members
                    .iter()
                    .enumerate()
                    .filter(|(slot, member)| member.eligible_for_new_flow(network, excluded[*slot]))
                    .reduce(|best, candidate| {
                        if normalized_load_is_less(candidate, best) {
                            candidate
                        } else {
                            best
                        }
                    })
                    .map(|(slot, _)| slot)
                    .expect("healthy compatible member exists");
                (slot, GatewaySelectionReason::LeastLoad)
            }
        }
    }

    fn select_all_unhealthy_fallback(
        &mut self,
        now: GatewayInstant,
        network: Network,
        excluded: &[bool; MAX_GATEWAY_MEMBERS],
    ) -> Result<(usize, GatewaySelectionReason), GatewaySelectionError> {
        let slot = self
            .members
            .iter()
            .enumerate()
            .filter(|(slot, member)| {
                member.mode == GatewayMemberMode::Enabled
                    && member.supports(network)
                    && !excluded[*slot]
            })
            .min_by_key(|(slot, member)| fallback_rank(member, now, *slot))
            .map(|(slot, _)| slot)
            .ok_or(GatewaySelectionError::NoEnabledMembers(network))?;
        let member = &mut self.members[slot];
        let reason = if member.recovery_probe_in_flight {
            GatewaySelectionReason::AllUnhealthyRecoveryInFlight
        } else if now < member.backoff_until {
            GatewaySelectionReason::AllUnhealthyDeferred {
                until: member.backoff_until,
            }
        } else {
            member.recovery_probe_in_flight = true;
            GatewaySelectionReason::AllUnhealthyRecoveryProbe
        };
        Ok((slot, reason))
    }

    fn observe(
        &mut self,
        handle: GatewayMemberHandle,
        now: GatewayInstant,
        source: GatewayObservationSource,
        outcome: GatewayOutcome,
        error: Option<String>,
    ) -> Result<(), GatewayStateError> {
        let health = self.health;
        let member = self.member_mut(handle)?;
        member.recovery_probe_in_flight = false;
        if source == GatewayObservationSource::ActiveProbe {
            member.active_probe_in_flight = false;
        }
        member.last_observation = Some(now);
        member.observability.last_observation_source = Some(source);
        match (source, outcome) {
            (GatewayObservationSource::ActiveProbe, GatewayOutcome::Success { .. }) => {
                member.observability.counters.probe_successes = member
                    .observability
                    .counters
                    .probe_successes
                    .saturating_add(1);
            }
            (GatewayObservationSource::ActiveProbe, GatewayOutcome::Failure) => {
                member.observability.counters.probe_failures = member
                    .observability
                    .counters
                    .probe_failures
                    .saturating_add(1);
            }
            (GatewayObservationSource::PassiveOpen, GatewayOutcome::Success { .. }) => {
                member.observability.counters.open_successes = member
                    .observability
                    .counters
                    .open_successes
                    .saturating_add(1);
            }
            (GatewayObservationSource::PassiveOpen, GatewayOutcome::Failure) => {
                member.observability.counters.open_failures = member
                    .observability
                    .counters
                    .open_failures
                    .saturating_add(1);
            }
            (GatewayObservationSource::PassiveFlow, GatewayOutcome::Success { .. }) => {
                member.observability.counters.flow_successes = member
                    .observability
                    .counters
                    .flow_successes
                    .saturating_add(1);
            }
            (GatewayObservationSource::PassiveFlow, GatewayOutcome::Failure) => {
                member.observability.counters.flow_failures = member
                    .observability
                    .counters
                    .flow_failures
                    .saturating_add(1);
            }
        }
        match outcome {
            GatewayOutcome::Success { latency } => {
                if let Some(latency) = latency {
                    update_latency(member, latency);
                    member.last_latency_observation = Some(now);
                    member.observability.last_latency_observation_source = Some(source);
                }
                if member.healthy {
                    member.consecutive_failures = 0;
                    member.recovery_successes = 0;
                    member.backoff_until = now;
                } else {
                    member.recovery_successes = member.recovery_successes.saturating_add(1);
                    if member.recovery_successes >= health.recovery_threshold {
                        member.healthy = true;
                        member.consecutive_failures = 0;
                        member.recovery_successes = 0;
                        member.observability.counters.recoveries =
                            member.observability.counters.recoveries.saturating_add(1);
                    }
                    member.backoff_until = now;
                }
            }
            GatewayOutcome::Failure => {
                if let Some(error) = error {
                    member.observability.last_error = Some(error);
                    member.observability.last_error_at = Some(now);
                }
                member.recovery_successes = 0;
                member.consecutive_failures = member.consecutive_failures.saturating_add(1);
                if member.consecutive_failures >= health.failure_threshold {
                    if member.healthy {
                        member.observability.counters.ejections =
                            member.observability.counters.ejections.saturating_add(1);
                    }
                    member.healthy = false;
                    let exponent = member
                        .consecutive_failures
                        .saturating_sub(health.failure_threshold)
                        .min(63);
                    let multiplier = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
                    let initial_millis = health.initial_backoff.as_millis();
                    let maximum_millis = health.maximum_backoff.as_millis();
                    let backoff_millis = initial_millis
                        .saturating_mul(u128::from(multiplier))
                        .min(maximum_millis)
                        .min(u128::from(u64::MAX)) as u64;
                    member.backoff_until = now.add(Duration::from_millis(backoff_millis));
                }
            }
        }
        Ok(())
    }

    fn sticky_member(
        &mut self,
        network: Network,
        destination: Option<&ProtocolTarget>,
        principal: Option<&PrincipalId>,
        now: GatewayInstant,
        excluded: &[bool; MAX_GATEWAY_MEMBERS],
    ) -> Option<(usize, GatewaySelectionReason)> {
        self.stickiness?;
        let (key, reason) = self.sticky_key_ref(network, destination, principal)?;
        let entry = self.sticky.get(&key).copied()?;
        let slot = usize::from(entry.member_slot);
        if now >= entry.expires_at
            || !self
                .members
                .get(slot)
                .is_some_and(|member| member.eligible_for_new_flow(network, excluded[slot]))
        {
            self.sticky.remove(&key);
            return None;
        }
        let sequence = self.next_sticky_sequence();
        if let Some(entry) = self.sticky.get_mut(&key) {
            entry.last_used_sequence = sequence;
        }
        Some((slot, reason))
    }

    fn remember_sticky(
        &mut self,
        network: Network,
        destination: Option<&ProtocolTarget>,
        principal: Option<&PrincipalId>,
        member_slot: usize,
        now: GatewayInstant,
    ) {
        let Some(policy) = self.stickiness else {
            return;
        };
        let Some(key) = self.sticky_key_owned(network, destination, principal) else {
            return;
        };
        if !self.sticky.contains_key(&key) && self.sticky.len() >= policy.capacity {
            let oldest = self
                .sticky
                .iter()
                .min_by_key(|(_, entry)| entry.last_used_sequence)
                .map(|(target, _)| target.clone());
            if let Some(oldest) = oldest {
                self.sticky.remove(&oldest);
            }
        }
        let sequence = self.next_sticky_sequence();
        self.sticky.insert(
            key,
            StickyEntry {
                member_slot: member_slot as u16,
                expires_at: now.add(policy.ttl),
                last_used_sequence: sequence,
            },
        );
    }

    fn sticky_key_ref<'a>(
        &self,
        network: Network,
        destination: Option<&'a ProtocolTarget>,
        principal: Option<&'a PrincipalId>,
    ) -> Option<(StickyKeyRef<'a>, GatewaySelectionReason)> {
        match self.stickiness_key {
            GatewayStickinessKey::Destination => Some((
                StickyKeyRef::Destination {
                    network,
                    destination: destination?,
                },
                GatewaySelectionReason::DestinationSticky,
            )),
            GatewayStickinessKey::Principal => Some((
                StickyKeyRef::Principal {
                    network,
                    principal: principal?,
                },
                GatewaySelectionReason::PrincipalSticky,
            )),
        }
    }

    fn sticky_key_owned(
        &self,
        network: Network,
        destination: Option<&ProtocolTarget>,
        principal: Option<&PrincipalId>,
    ) -> Option<StickyKey> {
        match self.stickiness_key {
            GatewayStickinessKey::Destination => Some(StickyKey::Destination {
                network,
                destination: destination?.clone(),
            }),
            GatewayStickinessKey::Principal => Some(StickyKey::Principal {
                network,
                principal: principal?.clone(),
            }),
        }
    }

    fn next_sticky_sequence(&mut self) -> u64 {
        self.sticky_sequence = self.sticky_sequence.saturating_add(1);
        self.sticky_sequence
    }

    fn first_healthy(
        &self,
        network: Network,
        excluded: &[bool; MAX_GATEWAY_MEMBERS],
    ) -> Option<usize> {
        self.members
            .iter()
            .enumerate()
            .find(|(slot, member)| member.eligible_for_new_flow(network, excluded[*slot]))
            .map(|(slot, _)| slot)
    }

    fn selection(&self, slot: usize, reason: GatewaySelectionReason) -> GatewaySelection<'_> {
        GatewaySelection {
            handle: self.handle(slot),
            member: &self.members[slot].id,
            reason,
        }
    }

    fn record_selection(
        &mut self,
        slot: usize,
        now: GatewayInstant,
        reason: GatewaySelectionReason,
    ) {
        let member = &mut self.members[slot];
        member.observability.last_selection_reason = Some(reason);
        member.observability.last_selected_at = Some(now);
        member.observability.counters.selections =
            member.observability.counters.selections.saturating_add(1);
    }

    fn handle(&self, slot: usize) -> GatewayMemberHandle {
        GatewayMemberHandle {
            generation: self.generation,
            slot: slot as u16,
        }
    }

    fn exclusion_mask(
        &self,
        excluded_handles: &[GatewayMemberHandle],
    ) -> Result<[bool; MAX_GATEWAY_MEMBERS], GatewaySelectionError> {
        if excluded_handles.len() > self.members.len() {
            return Err(GatewaySelectionError::TooManyExclusions {
                count: excluded_handles.len(),
                maximum: self.members.len(),
            });
        }
        let mut excluded = [false; MAX_GATEWAY_MEMBERS];
        for handle in excluded_handles {
            if handle.generation != self.generation
                || usize::from(handle.slot) >= self.members.len()
            {
                return Err(GatewaySelectionError::ForeignExclusionHandle);
            }
            excluded[usize::from(handle.slot)] = true;
        }
        Ok(excluded)
    }

    fn member(
        &self,
        handle: GatewayMemberHandle,
    ) -> Result<&GatewayMemberState, GatewayStateError> {
        if handle.generation != self.generation {
            return Err(GatewayStateError::ForeignHandle);
        }
        self.members
            .get(usize::from(handle.slot))
            .ok_or(GatewayStateError::ForeignHandle)
    }

    fn member_mut(
        &mut self,
        handle: GatewayMemberHandle,
    ) -> Result<&mut GatewayMemberState, GatewayStateError> {
        if handle.generation != self.generation {
            return Err(GatewayStateError::ForeignHandle);
        }
        self.members
            .get_mut(usize::from(handle.slot))
            .ok_or(GatewayStateError::ForeignHandle)
    }

    fn advance_time(&mut self, now: GatewayInstant) -> GatewayInstant {
        self.observed_now = self.observed_now.max(now);
        self.observed_now
    }
}

fn validate_spec(spec: &GatewayBalancerSpec) -> Result<(), GatewayCompileError> {
    if spec.members.is_empty() {
        return Err(GatewayCompileError::MissingMembers);
    }
    if spec.members.len() > MAX_GATEWAY_MEMBERS {
        return Err(GatewayCompileError::TooManyMembers {
            count: spec.members.len(),
            maximum: MAX_GATEWAY_MEMBERS,
        });
    }
    for (index, member) in spec.members.iter().enumerate() {
        if member.weight == 0 {
            return Err(GatewayCompileError::ZeroWeight(member.id.clone()));
        }
        if member.networks.is_empty() {
            return Err(GatewayCompileError::MissingNetworkCapability(
                member.id.clone(),
            ));
        }
        if spec.members[..index]
            .iter()
            .any(|previous| previous.id == member.id)
        {
            return Err(GatewayCompileError::DuplicateMember(member.id.clone()));
        }
    }
    if !(1..=MAX_HEALTH_THRESHOLD).contains(&spec.health.failure_threshold) {
        return Err(GatewayCompileError::InvalidFailureThreshold(
            spec.health.failure_threshold,
        ));
    }
    if !(1..=MAX_HEALTH_THRESHOLD).contains(&spec.health.recovery_threshold) {
        return Err(GatewayCompileError::InvalidRecoveryThreshold(
            spec.health.recovery_threshold,
        ));
    }
    if !valid_duration(spec.health.initial_backoff)
        || !valid_duration(spec.health.maximum_backoff)
        || spec.health.maximum_backoff < spec.health.initial_backoff
    {
        return Err(GatewayCompileError::InvalidBackoff);
    }
    if let Some(stickiness) = spec.stickiness {
        if !valid_duration(stickiness.ttl) {
            return Err(GatewayCompileError::InvalidStickinessTtl);
        }
        if !(1..=MAX_GATEWAY_STICKY_DESTINATIONS).contains(&stickiness.capacity) {
            return Err(GatewayCompileError::InvalidStickinessCapacity {
                capacity: stickiness.capacity,
                maximum: MAX_GATEWAY_STICKY_DESTINATIONS,
            });
        }
    }
    if !valid_duration(spec.freshness_ttl) {
        return Err(GatewayCompileError::InvalidFreshnessTtl);
    }
    if spec.strategy == GatewayStrategy::Manual && spec.manual_member.is_none() {
        return Err(GatewayCompileError::MissingManualMember);
    }
    if let Some(manual) = spec.manual_member.as_ref()
        && !spec.members.iter().any(|member| &member.id == manual)
    {
        return Err(GatewayCompileError::UnknownManualMember(manual.clone()));
    }
    if let Some(manual) = spec.manual_member.as_ref()
        && spec
            .members
            .iter()
            .find(|member| &member.id == manual)
            .is_some_and(|member| member.mode != GatewayMemberMode::Enabled)
    {
        return Err(GatewayCompileError::ManualMemberNotEnabled(manual.clone()));
    }
    if let Some(probe) = spec.probe.as_ref() {
        if !valid_duration(probe.interval) {
            return Err(GatewayCompileError::InvalidProbeInterval);
        }
        if !valid_duration(probe.timeout) || probe.timeout > probe.interval {
            return Err(GatewayCompileError::InvalidProbeTimeout);
        }
        if probe.target.ip().is_none() {
            return Err(GatewayCompileError::ProbeTargetMustBeLiteralIp);
        }
        if let Some(member) = spec
            .members
            .iter()
            .find(|member| !member.networks.contains(Network::Tcp))
        {
            return Err(GatewayCompileError::ProbeMemberDoesNotSupportTcp(
                member.id.clone(),
            ));
        }
    }
    Ok(())
}

fn valid_duration(duration: Duration) -> bool {
    !duration.is_zero()
        && duration <= MAX_POLICY_DURATION
        && duration.subsec_nanos().is_multiple_of(1_000_000)
}

fn update_latency(member: &mut GatewayMemberState, latency: Duration) {
    let sample = latency.as_micros().min(u128::from(u64::MAX));
    member.latency_ewma_micros = Some(match member.latency_ewma_micros {
        Some(previous) => ((u128::from(previous) * LATENCY_EWMA_OLD_SAMPLES + sample)
            / LATENCY_EWMA_TOTAL_SAMPLES)
            .min(u128::from(u64::MAX)) as u64,
        None => sample as u64,
    });
}

fn health_status(member: &GatewayMemberState, now: GatewayInstant) -> GatewayHealthStatus {
    if member.healthy {
        GatewayHealthStatus::Healthy
    } else if member.recovery_probe_in_flight {
        GatewayHealthStatus::RecoveryProbeInFlight
    } else if now < member.backoff_until {
        GatewayHealthStatus::BackingOff {
            until: member.backoff_until,
        }
    } else {
        GatewayHealthStatus::RecoveryProbeEligible
    }
}

fn freshness_status(
    member: &GatewayMemberState,
    now: GatewayInstant,
    freshness_ttl: Duration,
) -> GatewayFreshnessStatus {
    let Some(observed_at) = member.last_observation else {
        return GatewayFreshnessStatus::NeverObserved;
    };
    let stale_since = observed_at.add(freshness_ttl);
    if now < stale_since {
        GatewayFreshnessStatus::Fresh { observed_at }
    } else {
        GatewayFreshnessStatus::Stale {
            observed_at,
            stale_since,
        }
    }
}

fn fresh_latency_micros(
    member: &GatewayMemberState,
    now: GatewayInstant,
    freshness_ttl: Duration,
) -> Option<u64> {
    let observed_at = member.last_latency_observation?;
    (now < observed_at.add(freshness_ttl))
        .then_some(member.latency_ewma_micros)
        .flatten()
}

fn normalized_load_is_less(
    candidate: (usize, &GatewayMemberState),
    best: (usize, &GatewayMemberState),
) -> bool {
    let candidate_scaled = u128::from(candidate.1.load.total()) * u128::from(best.1.weight);
    let best_scaled = u128::from(best.1.load.total()) * u128::from(candidate.1.weight);
    candidate_scaled < best_scaled || (candidate_scaled == best_scaled && candidate.0 < best.0)
}

fn fallback_rank(
    member: &GatewayMemberState,
    now: GatewayInstant,
    slot: usize,
) -> (u8, GatewayInstant, u32, usize) {
    let class = if member.recovery_probe_in_flight {
        2
    } else if now >= member.backoff_until {
        0
    } else {
        1
    };
    (
        class,
        member.backoff_until,
        member.consecutive_failures,
        slot,
    )
}

fn reason_allows_stickiness(reason: GatewaySelectionReason) -> bool {
    !matches!(
        reason,
        GatewaySelectionReason::AllUnhealthyDeferred { .. }
            | GatewaySelectionReason::AllUnhealthyRecoveryInFlight
    )
}

#[cfg(test)]
#[path = "tests_gateway.rs"]
mod tests;
