//! Bounded correlation for manually requested authenticated-peer diagnostics.
//!
//! Carrier actors retain writer ownership. This broker only selects a live
//! carrier, correlates one request per session, and caches the latest result.

use crate::protocol::codec::{CodecLimits, peer_status_response_path_limit};
use crate::protocol::{Frame, PathId, PeerPathStatus, PeerStatusCode, SessionId, UnderlayProtocol};
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
    /// Endpoint-local identities captured with this exact response. Keeping
    /// them on the result prevents later authenticated PathId reuse from
    /// relabeling cached diagnostics.
    pub(in crate::runtime) local_path_indices: BTreeMap<(UnderlayProtocol, PathId), usize>,
    pub(in crate::runtime) received_at: SystemTime,
}

impl PeerStatusResult {
    #[cfg(test)]
    pub(in crate::runtime) fn local_path_index(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
    ) -> Option<usize> {
        self.local_path_indices.get(&(underlay, path_id)).copied()
    }
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
    allow_all_incoming: bool,
    has_scoped_incoming: bool,
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
    allow_incoming: bool,
    carriers: BTreeMap<u64, mpsc::Sender<u64>>,
    // Endpoint-local correlation only. The peer returns opaque wire PathIds;
    // management may map them back to the local configured path that admitted
    // the authenticated carrier without putting names or endpoints on wire.
    local_path_assignments: BTreeMap<(UnderlayProtocol, PathId), LocalPathAssignment>,
    preferred_registration: Option<u64>,
    last_attempted_registration: Option<u64>,
    last_incoming_response_at: Option<Instant>,
    pending: Option<PendingPeerStatusRequest>,
    latest: Option<PeerStatusResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LocalPathAssignment {
    local_path_index: usize,
    registration_id: u64,
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
    local_path_identity: Option<(UnderlayProtocol, PathId)>,
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
    pub(in crate::runtime) fn new(allow_all_incoming: bool) -> Self {
        Self::with_policy_and_timeout(allow_all_incoming, false, PEER_STATUS_REQUEST_TIMEOUT)
    }

    /// Construct a broker whose endpoint may authorize only selected live
    /// sessions. `allow_all_incoming` is the process-wide override;
    /// `has_scoped_incoming` records whether the endpoint has any configured
    /// principal authorization for management capability reporting.
    pub(in crate::runtime) fn with_scoped_incoming(
        allow_all_incoming: bool,
        has_scoped_incoming: bool,
    ) -> Self {
        Self::with_policy_and_timeout(
            allow_all_incoming,
            has_scoped_incoming,
            PEER_STATUS_REQUEST_TIMEOUT,
        )
    }

    pub(in crate::runtime) fn allows_incoming(&self) -> bool {
        self.inner.allow_all_incoming || self.inner.has_scoped_incoming
    }

    #[cfg(test)]
    fn with_timeout(allow_all_incoming: bool, request_timeout: Duration) -> Self {
        Self::with_policy_and_timeout(allow_all_incoming, false, request_timeout)
    }

    fn with_policy_and_timeout(
        allow_all_incoming: bool,
        has_scoped_incoming: bool,
        request_timeout: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(PeerStatusBrokerInner {
                allow_all_incoming,
                has_scoped_incoming,
                request_timeout,
                state: Mutex::new(PeerStatusBrokerState {
                    next_registration_id: 1,
                    next_request_id: 1,
                    sessions: HashMap::new(),
                }),
            }),
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn register(&self, session_id: SessionId) -> PeerStatusCarrier {
        self.register_with_incoming(session_id, false)
    }

    pub(in crate::runtime) fn register_path(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        local_path_index: usize,
    ) -> PeerStatusCarrier {
        self.register_path_with_incoming(session_id, underlay, path_id, local_path_index, false)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn register_with_incoming(
        &self,
        session_id: SessionId,
        allow_incoming: bool,
    ) -> PeerStatusCarrier {
        self.register_inner(session_id, allow_incoming, None)
    }

    pub(in crate::runtime) fn register_path_with_incoming(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        local_path_index: usize,
        allow_incoming: bool,
    ) -> PeerStatusCarrier {
        self.register_inner(
            session_id,
            allow_incoming,
            Some(((underlay, path_id), local_path_index)),
        )
    }

    fn register_inner(
        &self,
        session_id: SessionId,
        allow_incoming: bool,
        local_path: Option<((UnderlayProtocol, PathId), usize)>,
    ) -> PeerStatusCarrier {
        let (requests, requests_rx) = mpsc::channel(PEER_STATUS_COMMAND_CAPACITY);
        let local_path_identity = local_path.map(|(identity, _)| identity);
        let registration_id = {
            let mut state = self.inner.state.lock().expect("peer status broker lock");
            let registration_id = next_nonzero(&mut state.next_registration_id);
            let session = state.sessions.entry(session_id).or_default();
            session.allow_incoming |= allow_incoming;
            if let Some((identity, local_path_index)) = local_path {
                // Only successful carrier authentication installs or replaces
                // a live assignment. Completed response objects snapshot the
                // then-current value, so this reuse cannot relabel cached data.
                session.local_path_assignments.insert(
                    identity,
                    LocalPathAssignment {
                        local_path_index,
                        registration_id,
                    },
                );
            }
            session.carriers.insert(registration_id, requests);
            registration_id
        };
        PeerStatusCarrier {
            broker: self.clone(),
            session_id,
            registration_id,
            local_path_identity,
            requests: requests_rx,
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn local_path_index(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
    ) -> Option<usize> {
        self.inner
            .state
            .lock()
            .expect("peer status broker lock")
            .sessions
            .get(&session_id)
            .and_then(|session| session.local_path_assignments.get(&(underlay, path_id)))
            .map(|assignment| assignment.local_path_index)
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
        let mut guard = PendingRequestGuard {
            broker: Arc::downgrade(&self.inner),
            session_id,
            request_id,
            timed_out: false,
        };
        match tokio::time::timeout(self.inner.request_timeout, response_rx).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err(PeerStatusRequestError::SessionUnavailable),
            Err(_) => {
                guard.timed_out = true;
                Err(PeerStatusRequestError::TimedOut)
            }
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
        session.preferred_registration = Some(pending.registration_id);
        let local_path_indices = paths
            .iter()
            .filter_map(|path| {
                let identity = (path.metrics.underlay, path.metrics.path_id);
                session
                    .local_path_assignments
                    .get(&identity)
                    .map(|assignment| (identity, assignment.local_path_index))
            })
            .collect();
        let result = PeerStatusResult {
            session_id,
            request_id,
            code,
            paths,
            local_path_indices,
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
        if !self.inner.allow_all_incoming && !session.allow_incoming {
            return false;
        }
        if session.last_incoming_response_at.is_some_and(|previous| {
            now.saturating_duration_since(previous) < PEER_STATUS_INCOMING_MIN_INTERVAL
        }) {
            return false;
        }
        session.last_incoming_response_at = Some(now);
        true
    }

    fn incoming_enabled(&self, session_id: SessionId) -> bool {
        self.inner.allow_all_incoming
            || self
                .inner
                .state
                .lock()
                .expect("peer status broker lock")
                .sessions
                .get(&session_id)
                .is_some_and(|session| session.allow_incoming)
    }

    fn unregister(
        &self,
        session_id: SessionId,
        registration_id: u64,
        local_path_identity: Option<(UnderlayProtocol, PathId)>,
    ) {
        let mut state = self.inner.state.lock().expect("peer status broker lock");
        let remove_session = if let Some(session) = state.sessions.get_mut(&session_id) {
            session.carriers.remove(&registration_id);
            if let Some(identity) = local_path_identity
                && session
                    .local_path_assignments
                    .get(&identity)
                    .is_some_and(|assignment| assignment.registration_id == registration_id)
            {
                session.local_path_assignments.remove(&identity);
            }
            if session.preferred_registration == Some(registration_id) {
                session.preferred_registration = None;
            }
            if session.last_attempted_registration == Some(registration_id) {
                session.last_attempted_registration = None;
            }
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
        if !self.broker.incoming_enabled(self.session_id) {
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
        self.broker.unregister(
            self.session_id,
            self.registration_id,
            self.local_path_identity,
        );
    }
}

struct PendingRequestGuard {
    broker: Weak<PeerStatusBrokerInner>,
    session_id: SessionId,
    request_id: u64,
    timed_out: bool,
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
            if self.timed_out
                && session.preferred_registration
                    == session
                        .pending
                        .as_ref()
                        .map(|pending| pending.registration_id)
            {
                session.preferred_registration = None;
            }
            session.pending = None;
        }
    }
}

fn try_send_request(session: &mut PeerStatusSession, request_id: u64) -> Option<u64> {
    let registration_ids = session.carriers.keys().copied().collect::<Vec<_>>();
    if let Some(preferred) = session.preferred_registration
        && session
            .carriers
            .get(&preferred)
            .is_some_and(|carrier| carrier.try_send(request_id).is_ok())
    {
        session.last_attempted_registration = Some(preferred);
        return Some(preferred);
    }
    let start = session.last_attempted_registration.map_or(0, |last| {
        registration_ids.partition_point(|registration_id| *registration_id <= last)
    });
    for registration_id in registration_ids[start..]
        .iter()
        .chain(registration_ids[..start].iter())
        .copied()
    {
        if session.preferred_registration == Some(registration_id) {
            continue;
        }
        let Some(carrier) = session.carriers.get(&registration_id) else {
            continue;
        };
        if carrier.try_send(request_id).is_ok() {
            session.last_attempted_registration = Some(registration_id);
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
