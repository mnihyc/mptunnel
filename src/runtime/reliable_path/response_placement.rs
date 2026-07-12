use super::response_session::ResponseServiceFamilyLoads;
use crate::protocol::UnderlayProtocol;
use crate::scheduler::{PathRateScope, PathSnapshot};

// Carrier-neutral placement policy consumes normalized evidence. TCP and QUIC
// produce different rate scopes; neither transport owns whole-flow decisions.

pub(in crate::runtime) type ResponseRateScope = PathRateScope;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ResponseServiceHandoffMode {
    Diversification,
    PerformanceOverride,
}

pub(in crate::runtime) fn response_rate_fair_share_bps(
    snapshot: PathSnapshot,
    scope: ResponseRateScope,
    adds_flow: bool,
) -> f64 {
    let bulk_flows = snapshot
        .active_flows
        .saturating_sub(snapshot.active_latency_sensitive_flows)
        .saturating_add(u32::from(adds_flow))
        .max(1);
    match scope {
        ResponseRateScope::PerFlowGoodput => snapshot.delivery_rate_bps.max(1.0),
        ResponseRateScope::PathCapacity => {
            snapshot.delivery_rate_bps.max(1.0) / f64::from(bulk_flows)
        }
    }
}

pub(in crate::runtime) fn response_service_handoff_mode(
    service_underlay: UnderlayProtocol,
    service_share_bps: f64,
    target_underlay: UnderlayProtocol,
    target_share_bps: f64,
    family_loads: ResponseServiceFamilyLoads,
) -> Option<ResponseServiceHandoffMode> {
    let diversifies = family_loads.for_underlay(service_underlay)
        >= family_loads.for_underlay(target_underlay).saturating_add(2)
        && target_share_bps >= service_share_bps;
    if diversifies {
        return Some(ResponseServiceHandoffMode::Diversification);
    }
    // A 2x gain survives one additional equal-share flow without erasing the
    // improvement, so family balance becomes a preference rather than a veto.
    (target_share_bps >= service_share_bps * 2.0)
        .then_some(ResponseServiceHandoffMode::PerformanceOverride)
}
