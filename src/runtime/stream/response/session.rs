//! Server session lifetime accounting.
//!
//! A session remains registered while any carrier path, response stream, or
//! realtime flow refers to it. Scheduling, path metrics, and probe state have
//! separate owners and must not be stored here.

use super::super::send_buffer::SessionSendBuffer;
use crate::mux::MuxLimits;
use crate::product::PrincipalPermit;
use crate::protocol::SessionId;
use crate::runtime::RuntimeError;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub(in crate::runtime) struct ServerSessionTracker {
    send_buffer_limit_bytes: usize,
    max_sessions: usize,
    sessions: Mutex<HashMap<SessionId, ServerSessionEntry>>,
}

#[derive(Debug)]
struct ServerSessionEntry {
    references: u32,
    send_buffer: SessionSendBuffer,
    principal_permit: PrincipalPermit,
}

impl Default for ServerSessionTracker {
    fn default() -> Self {
        let limits = MuxLimits::default();
        Self::from_limits(limits, limits.max_streams)
    }
}

impl ServerSessionTracker {
    pub(in crate::runtime::stream) fn from_limits(limits: MuxLimits, max_sessions: usize) -> Self {
        Self {
            send_buffer_limit_bytes: SessionSendBuffer::from_limits(limits).limit_bytes(),
            max_sessions,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    pub(in crate::runtime::stream) fn attach_authenticated_session(
        &self,
        session_id: SessionId,
        principal_permit: &PrincipalPermit,
    ) -> Result<SessionSendBuffer, RuntimeError> {
        let mut sessions = self.sessions.lock().expect("server session tracker lock");
        if !sessions.contains_key(&session_id) && sessions.len() >= self.max_sessions {
            return Err(RuntimeError::Protocol(
                "server authenticated session limit reached",
            ));
        }
        let entry = sessions
            .entry(session_id)
            .or_insert_with(|| ServerSessionEntry {
                references: 0,
                send_buffer: SessionSendBuffer::new(self.send_buffer_limit_bytes),
                principal_permit: principal_permit.clone(),
            });
        if !entry.principal_permit.same_principal(principal_permit) {
            return Err(RuntimeError::AuthenticationRejected(
                "session principal changed across carrier paths",
            ));
        }
        entry.references = entry
            .references
            .checked_add(1)
            .expect("server session reference count overflow");
        Ok(entry.send_buffer.clone())
    }

    pub(in crate::runtime::stream) fn attach_session(
        &self,
        session_id: SessionId,
    ) -> SessionSendBuffer {
        let mut sessions = self.sessions.lock().expect("server session tracker lock");
        let entry = sessions
            .get_mut(&session_id)
            .expect("product flow attached before authenticated carrier");
        entry.references = entry
            .references
            .checked_add(1)
            .expect("server session reference count overflow");
        entry.send_buffer.clone()
    }

    pub(in crate::runtime::stream) fn detach_session(&self, session_id: SessionId) {
        let mut sessions = self.sessions.lock().expect("server session tracker lock");
        let entry = sessions
            .get_mut(&session_id)
            .expect("detached unregistered server session");
        entry.references -= 1;
        if entry.references == 0 {
            sessions.remove(&session_id);
        }
    }

    pub(in crate::runtime::stream) fn management_snapshot(&self) -> Vec<(SessionId, u32)> {
        let mut sessions = self
            .sessions
            .lock()
            .expect("server session tracker lock")
            .iter()
            .map(|(session_id, entry)| (*session_id, entry.references))
            .collect::<Vec<_>>();
        sessions.sort_unstable_by_key(|(session_id, _)| *session_id);
        sessions
    }

    #[cfg(test)]
    pub(super) fn reference_count(&self, session_id: SessionId) -> u32 {
        self.sessions
            .lock()
            .expect("server session tracker lock")
            .get(&session_id)
            .map(|entry| entry.references)
            .unwrap_or(0)
    }
}

/// Owns one server-session reference for a response stream, carrier, or flow.
pub(in crate::runtime) struct ServerSessionRegistration {
    tracker: Arc<ServerSessionTracker>,
    session_id: SessionId,
    send_buffer: SessionSendBuffer,
}

impl ServerSessionRegistration {
    pub(in crate::runtime::stream) fn new(
        tracker: Arc<ServerSessionTracker>,
        session_id: SessionId,
    ) -> Self {
        let send_buffer = tracker.attach_session(session_id);
        Self {
            tracker,
            session_id,
            send_buffer,
        }
    }

    pub(in crate::runtime::stream) fn send_buffer(&self) -> SessionSendBuffer {
        self.send_buffer.clone()
    }
}

impl Drop for ServerSessionRegistration {
    fn drop(&mut self) {
        self.tracker.detach_session(self.session_id);
    }
}

#[cfg(test)]
#[path = "tests_session.rs"]
mod tests;
