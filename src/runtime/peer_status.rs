//! Bounded correlation for manually requested authenticated-peer diagnostics.
//!
//! Carrier actors retain writer ownership. This broker only selects a live
//! carrier, correlates one request per session, and caches the latest result.

use crate::protocol::codec::{CodecLimits, peer_status_response_path_limit};
use crate::protocol::{Frame, PeerPathStatus, PeerStatusCode, SessionId};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{mpsc, oneshot};

const PEER_STATUS_COMMAND_CAPACITY: usize = 1;
const PEER_STATUS_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const PEER_STATUS_INCOMING_MIN_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) struct PeerStatusResult {
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) request_id: u64,
    pub(in crate::runtime) code: PeerStatusCode,
    pub(in crate::runtime) paths: Vec<PeerPathStatus>,
    pub(in crate::runtime) received_at: SystemTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum PeerStatusRequestError {
    SessionUnavailable,
    RequestInProgress,
    NoAvailableCarrier,
    TimedOut,
}

impl std::fmt::Display for PeerStatusRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::SessionUnavailable => "peer session is unavailable",
            Self::RequestInProgress => "peer status request is already in progress",
            Self::NoAvailableCarrier => "peer session has no available control carrier",
            Self::TimedOut => "peer status request timed out",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for PeerStatusRequestError {}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct PeerStatusBroker {
    inner: Arc<PeerStatusBrokerInner>,
}

#[derive(Debug)]
struct PeerStatusBrokerInner {
    allow_incoming: bool,
    request_timeout: Duration,
    state: Mutex<PeerStatusBrokerState>,
}

#[derive(Debug, Default)]
struct PeerStatusBrokerState {
    next_registration_id: u64,
    next_request_id: u64,
    sessions: HashMap<SessionId, PeerStatusSession>,
}

#[derive(Debug, Default)]
struct PeerStatusSession {
    carriers: BTreeMap<u64, mpsc::Sender<u64>>,
    last_selected_registration: Option<u64>,
    last_incoming_response_at: Option<Instant>,
    pending: Option<PendingPeerStatusRequest>,
    latest: Option<PeerStatusResult>,
}

#[derive(Debug)]
struct PendingPeerStatusRequest {
    request_id: u64,
    registration_id: u64,
    response: oneshot::Sender<PeerStatusResult>,
}

pub(in crate::runtime) struct PeerStatusCarrier {
    broker: PeerStatusBroker,
    session_id: SessionId,
    registration_id: u64,
    requests: mpsc::Receiver<u64>,
}

#[derive(Clone)]
pub(in crate::runtime) struct PeerStatusSnapshotSource {
    // `None` means the full exact session-owned set cannot be represented at
    // this instant. The wire boundary converts it to `UNAVAILABLE`.
    snapshot: Arc<dyn Fn() -> Option<Vec<PeerPathStatus>> + Send + Sync>,
}

impl std::fmt::Debug for PeerStatusSnapshotSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PeerStatusSnapshotSource")
            .finish_non_exhaustive()
    }
}

impl PeerStatusSnapshotSource {
    pub(in crate::runtime) fn new(
        snapshot: impl Fn() -> Option<Vec<PeerPathStatus>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            snapshot: Arc::new(snapshot),
        }
    }

    pub(in crate::runtime) fn snapshot(&self) -> Option<Vec<PeerPathStatus>> {
        (self.snapshot)()
    }
}

impl std::fmt::Debug for PeerStatusCarrier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PeerStatusCarrier")
            .field("session_id", &self.session_id)
            .field("registration_id", &self.registration_id)
            .finish_non_exhaustive()
    }
}

impl PeerStatusBroker {
    pub(in crate::runtime) fn new(allow_incoming: bool) -> Self {
        Self::with_timeout(allow_incoming, PEER_STATUS_REQUEST_TIMEOUT)
    }

    pub(in crate::runtime) fn allows_incoming(&self) -> bool {
        self.inner.allow_incoming
    }

    fn with_timeout(allow_incoming: bool, request_timeout: Duration) -> Self {
        Self {
            inner: Arc::new(PeerStatusBrokerInner {
                allow_incoming,
                request_timeout,
                state: Mutex::new(PeerStatusBrokerState {
                    next_registration_id: 1,
                    next_request_id: 1,
                    sessions: HashMap::new(),
                }),
            }),
        }
    }

    pub(in crate::runtime) fn register(&self, session_id: SessionId) -> PeerStatusCarrier {
        let (requests, requests_rx) = mpsc::channel(PEER_STATUS_COMMAND_CAPACITY);
        let registration_id = {
            let mut state = self.inner.state.lock().expect("peer status broker lock");
            let registration_id = next_nonzero(&mut state.next_registration_id);
            state
                .sessions
                .entry(session_id)
                .or_default()
                .carriers
                .insert(registration_id, requests);
            registration_id
        };
        PeerStatusCarrier {
            broker: self.clone(),
            session_id,
            registration_id,
            requests: requests_rx,
        }
    }

    pub(in crate::runtime) fn session_ids(&self) -> Vec<SessionId> {
        let state = self.inner.state.lock().expect("peer status broker lock");
        let mut sessions = state.sessions.keys().copied().collect::<Vec<_>>();
        sessions.sort_unstable();
        sessions
    }

    pub(in crate::runtime) fn carrier_count(&self, session_id: SessionId) -> usize {
        self.inner
            .state
            .lock()
            .expect("peer status broker lock")
            .sessions
            .get(&session_id)
            .map_or(0, |session| session.carriers.len())
    }

    pub(in crate::runtime) fn latest(&self, session_id: SessionId) -> Option<PeerStatusResult> {
        self.inner
            .state
            .lock()
            .expect("peer status broker lock")
            .sessions
            .get(&session_id)
            .and_then(|session| session.latest.clone())
    }

    pub(in crate::runtime) async fn request(
        &self,
        session_id: SessionId,
    ) -> Result<PeerStatusResult, PeerStatusRequestError> {
        let (response_tx, response_rx) = oneshot::channel();
        let request_id = {
            let mut state = self.inner.state.lock().expect("peer status broker lock");
            let request_id = next_nonzero(&mut state.next_request_id);
            let session = state
                .sessions
                .get_mut(&session_id)
                .ok_or(PeerStatusRequestError::SessionUnavailable)?;
            if session.pending.is_some() {
                return Err(PeerStatusRequestError::RequestInProgress);
            }
            let Some(registration_id) = try_send_request(session, request_id) else {
                return Err(PeerStatusRequestError::NoAvailableCarrier);
            };
            session.pending = Some(PendingPeerStatusRequest {
                request_id,
                registration_id,
                response: response_tx,
            });
            request_id
        };
        let _guard = PendingRequestGuard {
            broker: Arc::downgrade(&self.inner),
            session_id,
            request_id,
        };
        match tokio::time::timeout(self.inner.request_timeout, response_rx).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err(PeerStatusRequestError::SessionUnavailable),
            Err(_) => Err(PeerStatusRequestError::TimedOut),
        }
    }

    fn receive_response(
        &self,
        session_id: SessionId,
        request_id: u64,
        code: PeerStatusCode,
        mut paths: Vec<PeerPathStatus>,
    ) -> bool {
        if code != PeerStatusCode::Ok {
            paths.clear();
        }
        let mut state = self.inner.state.lock().expect("peer status broker lock");
        let Some(session) = state.sessions.get_mut(&session_id) else {
            return false;
        };
        let Some(pending) = session
            .pending
            .take_if(|pending| pending.request_id == request_id)
        else {
            return false;
        };
        let result = PeerStatusResult {
            session_id,
            request_id,
            code,
            paths,
            received_at: SystemTime::now(),
        };
        session.latest = Some(result.clone());
        let _ = pending.response.send(result);
        true
    }

    fn accept_incoming_request(&self, session_id: SessionId) -> bool {
        let now = Instant::now();
        let mut state = self.inner.state.lock().expect("peer status broker lock");
        let Some(session) = state.sessions.get_mut(&session_id) else {
            return false;
        };
        if session.last_incoming_response_at.is_some_and(|previous| {
            now.saturating_duration_since(previous) < PEER_STATUS_INCOMING_MIN_INTERVAL
        }) {
            return false;
        }
        session.last_incoming_response_at = Some(now);
        true
    }

    fn unregister(&self, session_id: SessionId, registration_id: u64) {
        let mut state = self.inner.state.lock().expect("peer status broker lock");
        let remove_session = if let Some(session) = state.sessions.get_mut(&session_id) {
            session.carriers.remove(&registration_id);
            if session
                .pending
                .as_ref()
                .is_some_and(|pending| pending.registration_id == registration_id)
            {
                session.pending = None;
            }
            session.carriers.is_empty()
        } else {
            false
        };
        if remove_session {
            state.sessions.remove(&session_id);
        }
    }
}

impl PeerStatusCarrier {
    pub(in crate::runtime) async fn recv_request(&mut self) -> Option<u64> {
        self.requests.recv().await
    }

    pub(in crate::runtime) fn response_frame(
        &self,
        request_id: u64,
        codec_limits: CodecLimits,
        paths: impl FnOnce() -> Option<Vec<PeerPathStatus>>,
    ) -> Frame {
        if !self.broker.inner.allow_incoming {
            return Frame::PeerStatusResponse {
                request_id,
                code: PeerStatusCode::Disabled,
                paths: Vec::new(),
            };
        }
        if !self.broker.accept_incoming_request(self.session_id) {
            return Frame::PeerStatusResponse {
                request_id,
                code: PeerStatusCode::Unavailable,
                paths: Vec::new(),
            };
        }
        let Some(paths) = paths() else {
            return Frame::PeerStatusResponse {
                request_id,
                code: PeerStatusCode::Unavailable,
                paths: Vec::new(),
            };
        };
        if paths.len() > peer_status_response_path_limit(codec_limits) {
            return Frame::PeerStatusResponse {
                request_id,
                code: PeerStatusCode::Unavailable,
                paths: Vec::new(),
            };
        }
        Frame::PeerStatusResponse {
            request_id,
            code: PeerStatusCode::Ok,
            paths,
        }
    }

    pub(in crate::runtime) fn receive_response(
        &self,
        request_id: u64,
        code: PeerStatusCode,
        paths: Vec<PeerPathStatus>,
    ) -> bool {
        self.broker
            .receive_response(self.session_id, request_id, code, paths)
    }
}

impl Drop for PeerStatusCarrier {
    fn drop(&mut self) {
        self.broker
            .unregister(self.session_id, self.registration_id);
    }
}

struct PendingRequestGuard {
    broker: Weak<PeerStatusBrokerInner>,
    session_id: SessionId,
    request_id: u64,
}

impl Drop for PendingRequestGuard {
    fn drop(&mut self) {
        let Some(broker) = self.broker.upgrade() else {
            return;
        };
        let mut state = broker.state.lock().expect("peer status broker lock");
        if let Some(session) = state.sessions.get_mut(&self.session_id)
            && session
                .pending
                .as_ref()
                .is_some_and(|pending| pending.request_id == self.request_id)
        {
            session.pending = None;
        }
    }
}

fn try_send_request(session: &mut PeerStatusSession, request_id: u64) -> Option<u64> {
    let registration_ids = session.carriers.keys().copied().collect::<Vec<_>>();
    let start = session.last_selected_registration.map_or(0, |last| {
        registration_ids.partition_point(|registration_id| *registration_id <= last)
    });
    for registration_id in registration_ids[start..]
        .iter()
        .chain(registration_ids[..start].iter())
        .copied()
    {
        let Some(carrier) = session.carriers.get(&registration_id) else {
            continue;
        };
        if carrier.try_send(request_id).is_ok() {
            session.last_selected_registration = Some(registration_id);
            return Some(registration_id);
        }
    }
    None
}

fn next_nonzero(next: &mut u64) -> u64 {
    let value = (*next).max(1);
    *next = value.wrapping_add(1).max(1);
    value
}

#[cfg(test)]
#[path = "tests_peer_status.rs"]
mod tests;
