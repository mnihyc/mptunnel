//! Carrier-neutral path scoring over immutable snapshots.
//!
//! Deployed queue ownership stays in runtime senders. Deterministic virtual
//! queues and simulation-only policies stay in `simulator`.

mod policy;
mod traffic;

pub use policy::{PathRateScope, PathScore, PathSnapshot, PathState, choose_path, score_path};
pub(crate) use policy::{
    QUIC_INITIAL_WINDOW_PACKETS, path_bdp_bytes, path_is_backup, path_is_schedulable, path_pto_ms,
    path_within_adaptive_lead_hysteresis, payload_tx_ms,
};
pub use traffic::TrafficClass;
pub(crate) use traffic::{
    cyclic_cursor_distance, stream_demand_hint_for_traffic_class,
    traffic_class_from_stream_demand_hint,
};
