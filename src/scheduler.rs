//! Carrier-neutral path scoring over immutable snapshots.
//!
//! Deployed queue ownership stays in runtime senders. Deterministic virtual
//! queues and simulation-only policies stay in `simulator`.

mod policy;
mod snapshot;
mod traffic;

pub(crate) use policy::QUIC_INITIAL_WINDOW_PACKETS;
pub use policy::{PathRateScope, PathScore, PathState};
pub use snapshot::PathSnapshot;
pub use traffic::TrafficClass;
pub(crate) use traffic::{
    cyclic_cursor_distance, stream_demand_hint_for_traffic_class,
    traffic_class_from_stream_demand_hint,
};

// Projection is an API boundary. Completion policy cannot inspect the native
// RTT, native rate diagnostics or admission credit of the source snapshot.
#[inline]
pub fn score_path(
    path: PathSnapshot,
    lane: TrafficClass,
    payload_bytes: usize,
) -> Option<PathScore> {
    policy::score_path(path.completion(), lane, payload_bytes)
}

pub fn choose_path(
    paths: &[PathSnapshot],
    lane: TrafficClass,
    payload_bytes: usize,
) -> Option<PathScore> {
    let choose = |allow_backup: bool| {
        paths
            .iter()
            .filter(|path| allow_backup || !path_is_backup(**path))
            .filter_map(|path| score_path(*path, lane, payload_bytes))
            .min_by(|left, right| left.eta_ms.total_cmp(&right.eta_ms))
    };
    choose(false).or_else(|| choose(true))
}

#[inline]
pub(crate) fn path_is_backup(path: PathSnapshot) -> bool {
    policy::path_is_backup(path.completion())
}

#[inline]
pub(crate) fn path_is_schedulable(path: PathSnapshot, lane: TrafficClass) -> bool {
    policy::path_is_schedulable(path.completion(), lane)
}

#[inline]
pub(crate) fn path_within_adaptive_lead_hysteresis(
    old_eta_ms: f64,
    old: PathSnapshot,
    best_eta_ms: f64,
    best: PathSnapshot,
    payload_bytes: usize,
) -> bool {
    policy::path_within_adaptive_lead_hysteresis(
        old_eta_ms,
        old.completion(),
        best_eta_ms,
        best.completion(),
        payload_bytes,
    )
}

#[inline]
pub(crate) fn path_has_material_completion_advantage(
    candidate_eta_ms: f64,
    candidate: PathSnapshot,
    available_eta_ms: f64,
    available: PathSnapshot,
) -> bool {
    policy::path_has_material_completion_advantage(
        candidate_eta_ms,
        candidate.completion(),
        available_eta_ms,
        available.completion(),
    )
}

#[inline]
pub(crate) fn path_pto_ms(path: PathSnapshot) -> f64 {
    policy::path_pto_ms(path.completion())
}

#[inline]
pub(crate) fn payload_tx_ms(path: PathSnapshot, payload_bytes: usize) -> f64 {
    policy::payload_tx_ms(path.completion(), payload_bytes)
}

#[inline]
pub(crate) fn path_bdp_bytes(path: PathSnapshot) -> usize {
    ((policy::effective_path_rate_bps(path.completion(), TrafficClass::Throughput) / 8.0)
        * (path.peer_timing().srtt_ms().max(1.0) / 1000.0))
        .ceil()
        .max(1.0) as usize
}

#[cfg(test)]
#[path = "scheduler/tests_policy.rs"]
mod tests_policy;
