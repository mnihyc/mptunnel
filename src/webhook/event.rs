use std::str::FromStr;

/// The stable, bounded event vocabulary exposed by the webhook interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EventKind {
    PathStateChanged,
    PathPolicyChanged,
    PathProbeCompleted,
    PathInterval,
    CarrierStateChanged,
    CarrierPolicyChanged,
    CarrierAddressChanged,
    SessionStateChanged,
    SessionPeerAddressesChanged,
    NodeStateChanged,
    BalancerMemberChanged,
    BalancerProbeCompleted,
}

impl EventKind {
    pub const ALL: [Self; 12] = [
        Self::PathStateChanged,
        Self::PathPolicyChanged,
        Self::PathProbeCompleted,
        Self::PathInterval,
        Self::CarrierStateChanged,
        Self::CarrierPolicyChanged,
        Self::CarrierAddressChanged,
        Self::SessionStateChanged,
        Self::SessionPeerAddressesChanged,
        Self::NodeStateChanged,
        Self::BalancerMemberChanged,
        Self::BalancerProbeCompleted,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PathStateChanged => "path.state_changed",
            Self::PathPolicyChanged => "path.policy_changed",
            Self::PathProbeCompleted => "path.probe_completed",
            Self::PathInterval => "path.interval",
            Self::CarrierStateChanged => "carrier.state_changed",
            Self::CarrierPolicyChanged => "carrier.policy_changed",
            Self::CarrierAddressChanged => "carrier.address_changed",
            Self::SessionStateChanged => "session.state_changed",
            Self::SessionPeerAddressesChanged => "session.peer_addresses_changed",
            Self::NodeStateChanged => "node.state_changed",
            Self::BalancerMemberChanged => "balancer.member_changed",
            Self::BalancerProbeCompleted => "balancer.probe_completed",
        }
    }
}

impl std::fmt::Display for EventKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for EventKind {
    type Err = EventKindParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.as_str() == value)
            .ok_or_else(|| EventKindParseError(value.to_owned()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventKindParseError(String);

impl std::fmt::Display for EventKindParseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "unknown webhook event type {:?}", self.0)
    }
}

impl std::error::Error for EventKindParseError {}
