//! Server session lifetime accounting.
//!
//! A session remains registered while any carrier path, response stream, or
//! realtime flow refers to it. Scheduling, path metrics, and probe state have
//! separate owners and must not be stored here.

use super::super::send_buffer::SessionSendBuffer;
use crate::mux::MuxLimits;
use crate::product::PrincipalPermit;
use crate::protocol::{CloseReason, SessionId};
use crate::runtime::RuntimeError;
use crate::runtime::path::ServerSessionRetirement;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::watch;

#[derive(Debug)]
pub(in crate::runtime) struct ServerSessionTracker {
    send_buffer_limit_bytes: usize,
    max_sessions: usize,
    session_retention_timeout: Duration,
    sessions: Mutex<HashMap<SessionId, ServerSessionEntry>>,
}

#[derive(Debug)]
struct ServerSessionEntry {
    references: u32,
    send_buffer: SessionSendBuffer,
    principal_permit: PrincipalPermit,
    retirement: watch::Sender<Option<CloseReason>>,
    retired_until: Option<Instant>,
}

impl Default for ServerSessionTracker {
    fn default() -> Self {
        let limits = MuxLimits::default();
        Self::from_limits(limits, limits.max_streams)
    }
}

impl ServerSessionTracker {
    pub(in crate::runtime::stream) fn from_limits(limits: MuxLimits, max_sessions: usize) -> Self {
        Self::from_limits_and_retention(
            limits,
            max_sessions,
            crate::config::DEFAULT_SESSION_RETENTION_TIMEOUT,
        )
    }

    pub(in crate::runtime::stream) fn from_limits_and_retention(
        limits: MuxLimits,
        max_sessions: usize,
        session_retention_timeout: Duration,
    ) -> Self {
        Self {
            send_buffer_limit_bytes: SessionSendBuffer::from_limits(limits).limit_bytes(),
            max_sessions,
            session_retention_timeout,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    fn prune_expired_tombstones(
        sessions: &mut HashMap<SessionId, ServerSessionEntry>,
        now: Instant,
    ) {
        sessions.retain(|_, entry| {
            entry.references != 0
                || entry
                    .retired_until
                    .is_some_and(|retired_until| retired_until > now)
        });
    }

    pub(in crate::runtime::stream) fn attach_authenticated_session(
        &self,
        session_id: SessionId,
        principal_permit: &PrincipalPermit,
    ) -> Result<(SessionSendBuffer, ServerSessionRetirement), RuntimeError> {
        let mut sessions = self.sessions.lock().expect("server session tracker lock");
        Self::prune_expired_tombstones(&mut sessions, Instant::now());
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
                retirement: watch::channel(None).0,
                retired_until: None,
            });
        if !entry.principal_permit.same_principal(principal_permit) {
            return Err(RuntimeError::AuthenticationRejected(
                "session principal changed across carrier paths",
            ));
        }
        if let Some(reason) = *entry.retirement.borrow() {
            return Err(RuntimeError::RemoteClosed(reason));
        }
        entry.references = entry
            .references
            .checked_add(1)
            .expect("server session reference count overflow");
        Ok((
            entry.send_buffer.clone(),
            ServerSessionRetirement::pending(entry.retirement.subscribe()),
        ))
    }

    pub(in crate::runtime::stream) fn attach_session(
        &self,
        session_id: SessionId,
    ) -> Result<(SessionSendBuffer, ServerSessionRetirement), RuntimeError> {
        let mut sessions = self.sessions.lock().expect("server session tracker lock");
        let entry = sessions
            .get_mut(&session_id)
            .expect("product flow attached before authenticated carrier");
        if let Some(reason) = *entry.retirement.borrow() {
            return Err(RuntimeError::RemoteClosed(reason));
        }
        entry.references = entry
            .references
            .checked_add(1)
            .expect("server session reference count overflow");
        Ok((
            entry.send_buffer.clone(),
            ServerSessionRetirement::pending(entry.retirement.subscribe()),
        ))
    }

    pub(in crate::runtime::stream) fn session_retirement(
        &self,
        session_id: SessionId,
    ) -> Result<ServerSessionRetirement, RuntimeError> {
        let sessions = self.sessions.lock().expect("server session tracker lock");
        let entry = sessions
            .get(&session_id)
            .ok_or(RuntimeError::ReliablePathSessionClosed)?;
        Ok(ServerSessionRetirement::pending(
            entry.retirement.subscribe(),
        ))
    }

    fn commit_if_active<T>(
        &self,
        session_id: SessionId,
        commit: impl FnOnce() -> T,
    ) -> Result<T, RuntimeError> {
        let sessions = self.sessions.lock().expect("server session tracker lock");
        let entry = sessions
            .get(&session_id)
            .ok_or(RuntimeError::ReliablePathSessionClosed)?;
        if let Some(reason) = *entry.retirement.borrow() {
            return Err(RuntimeError::RemoteClosed(reason));
        }
        // Terminal publication uses this same lock. The bounded synchronous
        // commit therefore either precedes publication and is visible to the
        // following owner sweep, or observes the sticky terminal reason.
        Ok(commit())
    }

    pub(in crate::runtime::stream) fn retire_session(
        &self,
        session_id: SessionId,
        reason: CloseReason,
    ) -> bool {
        let now = Instant::now();
        let mut sessions = self.sessions.lock().expect("server session tracker lock");
        Self::prune_expired_tombstones(&mut sessions, now);
        let Some(entry) = sessions.get_mut(&session_id) else {
            return false;
        };
        if entry.retirement.borrow().is_some() {
            return false;
        }
        entry.retired_until = Some(now + self.session_retention_timeout);
        entry.retirement.send_replace(Some(reason));
        true
    }

    pub(in crate::runtime::stream) fn detach_session(&self, session_id: SessionId) {
        let mut sessions = self.sessions.lock().expect("server session tracker lock");
        let entry = sessions
            .get_mut(&session_id)
            .expect("detached unregistered server session");
        entry.references -= 1;
        if entry.references == 0 && entry.retired_until.is_none() {
            sessions.remove(&session_id);
        }
    }

    pub(in crate::runtime::stream) fn management_snapshot(&self) -> Vec<(SessionId, u32)> {
        let mut sessions = self
            .sessions
            .lock()
            .expect("server session tracker lock")
            .iter()
            .filter(|(_, entry)| entry.references != 0)
            .map(|(session_id, entry)| (*session_id, entry.references))
            .collect::<Vec<_>>();
        sessions.sort_unstable_by_key(|(session_id, _)| *session_id);
        sessions
    }

    #[cfg(test)]
    pub(in crate::runtime::stream) fn reference_count(&self, session_id: SessionId) -> u32 {
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
    retirement: ServerSessionRetirement,
}

impl ServerSessionRegistration {
    pub(in crate::runtime::stream) fn try_new(
        tracker: Arc<ServerSessionTracker>,
        session_id: SessionId,
    ) -> Result<Self, RuntimeError> {
        let (send_buffer, retirement) = tracker.attach_session(session_id)?;
        Ok(Self {
            tracker,
            session_id,
            send_buffer,
            retirement,
        })
    }

    #[cfg(test)]
    pub(super) fn new(tracker: Arc<ServerSessionTracker>, session_id: SessionId) -> Self {
        Self::try_new(tracker, session_id).expect("register active test session owner")
    }

    pub(in crate::runtime::stream) fn send_buffer(&self) -> SessionSendBuffer {
        self.send_buffer.clone()
    }

    pub(in crate::runtime::stream) fn retirement(&self) -> ServerSessionRetirement {
        self.retirement.clone()
    }

    pub(in crate::runtime::stream) fn commit_if_active<T>(
        &self,
        commit: impl FnOnce() -> T,
    ) -> Result<T, RuntimeError> {
        self.tracker.commit_if_active(self.session_id, commit)
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
