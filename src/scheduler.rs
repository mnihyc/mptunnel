//! Carrier-neutral path scoring over immutable snapshots.
//!
//! Deployed queue ownership stays in runtime senders. Deterministic virtual
//! queues and experimental policies stay in `simulator`.

mod flow;
mod policy;

pub use flow::FlowLane;
pub(crate) use flow::{
    cyclic_cursor_distance, flow_lane_from_stream_demand_hint, stream_demand_hint_for_lane,
};
pub use policy::{PathRateScope, PathScore, PathSnapshot, PathState, choose_path, score_path};
pub(crate) use policy::{
    QUIC_INITIAL_WINDOW_PACKETS, path_bdp_bytes, path_is_schedulable, path_pto_ms,
    path_within_adaptive_lead_hysteresis, payload_tx_ms,
};
