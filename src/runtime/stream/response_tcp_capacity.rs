//! Session-wide TCP carrier-discovery reservation and lease release.
//!
//! TCP discovery only serializes a carrier probe. QUIC capacity proof owns its
//! separate transport evidence, attempt budget, and proof lifecycle.

use super::response_session::ServerPathLaneTracker;
use crate::protocol::SessionId;
use std::sync::Arc;

/// Holds the one session-wide TCP carrier-discovery slot until the typed
/// command is dropped after receipt, failure, or cancellation.
#[derive(Debug)]
pub(in crate::runtime) struct TcpCapacityProbeSessionLease {
    tracker: Arc<ServerPathLaneTracker>,
    session_id: SessionId,
}

impl Drop for TcpCapacityProbeSessionLease {
    fn drop(&mut self) {
        let mut state = self
            .tracker
            .state
            .lock()
            .expect("server path lane tracker lock");
        if state
            .tcp_capacity_probe_reservations
            .remove(&self.session_id)
        {
            state.bump_generation(self.session_id);
            state.maybe_reclaim_session(self.session_id);
        }
    }
}

impl ServerPathLaneTracker {
    pub(in crate::runtime) fn try_reserve_tcp_capacity_probe(
        self: &Arc<Self>,
        session_id: SessionId,
        expected_generation: u64,
    ) -> Option<TcpCapacityProbeSessionLease> {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let generation = state
            .session_generations
            .get(&session_id)
            .copied()
            .unwrap_or(0);
        if generation != expected_generation
            || state.tcp_capacity_probe_reservations.contains(&session_id)
            || state.quic_capacity_calibration_reserved(session_id)
            || state.response_service_handoff_drain_reserved(session_id)
        {
            return None;
        }
        state.tcp_capacity_probe_reservations.insert(session_id);
        state.bump_generation(session_id);
        Some(TcpCapacityProbeSessionLease {
            tracker: Arc::clone(self),
            session_id,
        })
    }
}
