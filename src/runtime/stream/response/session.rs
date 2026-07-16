//! Server session lifetime accounting.
//!
//! A session remains registered while any carrier path, response stream, or
//! realtime flow refers to it. Scheduling, path metrics, and probe state have
//! separate owners and must not be stored here.

use crate::protocol::SessionId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
pub(in crate::runtime) struct ServerSessionTracker {
    references: Mutex<HashMap<SessionId, u32>>,
}

impl ServerSessionTracker {
    pub(in crate::runtime::stream) fn attach_session(&self, session_id: SessionId) {
        let mut references = self.references.lock().expect("server session tracker lock");
        let count = references.entry(session_id).or_default();
        *count = count
            .checked_add(1)
            .expect("server session reference count overflow");
    }

    pub(in crate::runtime::stream) fn detach_session(&self, session_id: SessionId) {
        let mut references = self.references.lock().expect("server session tracker lock");
        let count = references
            .get_mut(&session_id)
            .expect("detached unregistered server session");
        *count -= 1;
        if *count == 0 {
            references.remove(&session_id);
        }
    }

    pub(in crate::runtime::stream) fn management_snapshot(&self) -> Vec<(SessionId, u32)> {
        let mut sessions = self
            .references
            .lock()
            .expect("server session tracker lock")
            .iter()
            .map(|(session_id, references)| (*session_id, *references))
            .collect::<Vec<_>>();
        sessions.sort_unstable_by_key(|(session_id, _)| *session_id);
        sessions
    }

    #[cfg(test)]
    pub(super) fn reference_count(&self, session_id: SessionId) -> u32 {
        self.references
            .lock()
            .expect("server session tracker lock")
            .get(&session_id)
            .copied()
            .unwrap_or(0)
    }
}

/// Owns one server-session reference for a response stream, carrier, or flow.
pub(in crate::runtime) struct ServerSessionRegistration {
    tracker: Arc<ServerSessionTracker>,
    session_id: SessionId,
}

impl ServerSessionRegistration {
    pub(in crate::runtime::stream) fn new(
        tracker: Arc<ServerSessionTracker>,
        session_id: SessionId,
    ) -> Self {
        tracker.attach_session(session_id);
        Self {
            tracker,
            session_id,
        }
    }
}

impl Drop for ServerSessionRegistration {
    fn drop(&mut self) {
        self.tracker.detach_session(self.session_id);
    }
}

#[cfg(test)]
#[path = "session_test.rs"]
mod tests;
