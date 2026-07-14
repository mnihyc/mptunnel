//! Session-wide TCP carrier-discovery reservation and lease release.
//!
//! TCP discovery only serializes a carrier probe. QUIC capacity proof owns its
//! separate transport evidence, attempt budget, and proof lifecycle.

use super::session::ServerPathLaneTracker;
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
        let released = state.session_mut(self.session_id).is_some_and(|session| {
            if !session.clear_tcp_capacity_probe() {
                return false;
            }
            session.bump_generation();
            true
        });
        if released {
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
        let generation = state.generation(session_id);
        if generation != expected_generation {
            return None;
        }
        let session = state.session_mut_or_default(session_id);
        if !session.reserve_tcp_capacity_probe() {
            return None;
        }
        session.bump_generation();
        Some(TcpCapacityProbeSessionLease {
            tracker: Arc::clone(self),
            session_id,
        })
    }
}
