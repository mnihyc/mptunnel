//! Request-stream carrier attachment ownership.
//!
//! A logical request stream owns its carrier membership, attachment generation,
//! scheduler leases, and frame fan-in. Relay code may open or select carriers,
//! but a pending carrier becomes durable only when this owner commits it.

use super::super::feedback::{
    StreamFeedbackPublication, StreamFeedbackPublicationCursor, StreamFeedbackService,
    StreamFeedbackState,
};
use crate::model::capacity::reliable_relay_buffer_len;
#[cfg(test)]
use crate::model::path::next_carrier_path_instance_id;
use crate::model::path::{CarrierPathInstanceId, RelayPathInstance, RelayPathKey};
use crate::mux::MuxLimits;
use crate::protocol::{
    Frame, PathUsage, ResetReason, StreamAttachmentPhase, StreamId, StreamReturnPlan,
    UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCarrierTerminalCause, ReliablePathCarrierTerminalSignal,
};
use crate::runtime::path::{ClientPathContext, RelayPathLoadLease};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamHandle};
use crate::scheduler::{PathSnapshot, TrafficClass, path_is_backup, score_path};
#[cfg(test)]
use std::collections::HashMap;
use std::future::Future;
#[cfg(test)]
use std::sync::OnceLock;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{Notify, mpsc};

#[cfg(test)]
type ClientRelayAttachmentCommitRegistry =
    HashMap<(CarrierPathInstanceId, StreamId), (u64, mpsc::UnboundedSender<RelayPathInstance>)>;

#[cfg(test)]
static CLIENT_RELAY_ATTACHMENT_COMMITS: OnceLock<Mutex<ClientRelayAttachmentCommitRegistry>> =
    OnceLock::new();

#[cfg(test)]
static NEXT_CLIENT_RELAY_ATTACHMENT_COMMIT_ID: AtomicU64 = AtomicU64::new(1);

/// One immutable return-topology candidate published by the requester.
///
/// Ordinals belong to this frozen vector rather than a configured path slot:
/// a replacement reusing `RelayPathKey` is ordinary later topology and cannot
/// inherit startup enrollment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ReliableRelayReturnCandidate {
    pub(in crate::runtime) key: RelayPathKey,
    /// Exact owner frozen at publication. `None` is an eligible configured
    /// slot that may bind only the first instance accepted in this one round.
    pub(in crate::runtime) path_instance_id: Option<CarrierPathInstanceId>,
    pub(in crate::runtime) ordinal: u8,
}

/// Client-owned immutable identity of one response startup transaction.
#[derive(Debug)]
pub(in crate::runtime) struct ReliableRelayReturnPlan {
    trigger_bytes: u64,
    candidate_tier: PathUsage,
    candidates: Vec<ReliableRelayReturnCandidate>,
}

impl ReliableRelayReturnPlan {
    pub(in crate::runtime) fn new(
        trigger_bytes: u64,
        candidate_tier: PathUsage,
        slots: Vec<(RelayPathKey, Option<CarrierPathInstanceId>)>,
    ) -> Result<Self, RuntimeError> {
        if slots.is_empty() {
            return Err(RuntimeError::Protocol(
                "return startup plan must contain an opening candidate",
            ));
        }
        if slots.len() > usize::from(u8::MAX) {
            return Err(RuntimeError::Protocol(
                "return startup candidate total exceeds wire bound",
            ));
        }
        let mut candidates = Vec::with_capacity(slots.len());
        for (ordinal, (key, path_instance_id)) in slots.into_iter().enumerate() {
            if candidates
                .iter()
                .any(|candidate: &ReliableRelayReturnCandidate| {
                    candidate.key == key
                        || (path_instance_id.is_some()
                            && candidate.path_instance_id == path_instance_id)
                })
            {
                return Err(RuntimeError::Protocol(
                    "return startup plan contains a duplicate exact carrier",
                ));
            }
            candidates.push(ReliableRelayReturnCandidate {
                key,
                path_instance_id,
                ordinal: u8::try_from(ordinal)
                    .map_err(|_| RuntimeError::Protocol("return startup ordinal overflow"))?,
            });
        }
        if candidates.len() == 1 && trigger_bytes != 0 {
            return Err(RuntimeError::Protocol(
                "singleton return startup plan must be ready immediately",
            ));
        }
        if candidates.len() > 1 && trigger_bytes == 0 {
            return Err(RuntimeError::Protocol(
                "lazy return startup plan requires a positive trigger",
            ));
        }
        Ok(Self {
            trigger_bytes,
            candidate_tier,
            candidates,
        })
    }

    pub(in crate::runtime) fn trigger_bytes(&self) -> u64 {
        self.trigger_bytes
    }

    #[cfg(test)]
    pub(in crate::runtime) fn candidate_tier(&self) -> PathUsage {
        self.candidate_tier
    }

    pub(in crate::runtime) fn candidates(&self) -> &[ReliableRelayReturnCandidate] {
        &self.candidates
    }

    pub(in crate::runtime) fn candidate(
        &self,
        ordinal: u8,
    ) -> Option<ReliableRelayReturnCandidate> {
        self.candidates.get(usize::from(ordinal)).copied()
    }

    pub(in crate::runtime) fn candidate_for_key(
        &self,
        key: RelayPathKey,
    ) -> Option<ReliableRelayReturnCandidate> {
        self.candidates
            .iter()
            .find(|candidate| candidate.key == key)
            .copied()
    }

    pub(in crate::runtime) fn wire(
        &self,
        phase: StreamAttachmentPhase,
        candidate_ordinal: u8,
    ) -> StreamReturnPlan {
        StreamReturnPlan {
            trigger_bytes: self.trigger_bytes,
            candidate_total: self.candidates.len() as u8,
            candidate_tier: self.candidate_tier,
            phase,
            candidate_ordinal,
        }
    }
}

/// Initial-open settlement carried into the serialized relay owner.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct ReliableRelayOpenedStartup {
    pub(in crate::runtime) plan: Arc<ReliableRelayReturnPlan>,
    pub(in crate::runtime) opening_ordinal: u8,
    pub(in crate::runtime) failed_ordinals: Vec<u8>,
}

/// Test-only observation of durable client attachment-set commits for one
/// logical stream on one exact physical carrier lifetime.
#[cfg(test)]
pub(in crate::runtime) struct ClientRelayAttachmentCommitHandle {
    key: (CarrierPathInstanceId, StreamId),
    id: u64,
    commits: mpsc::UnboundedReceiver<RelayPathInstance>,
}

#[cfg(test)]
impl ClientRelayAttachmentCommitHandle {
    pub(in crate::runtime) async fn wait_committed(&mut self) -> RelayPathInstance {
        self.commits
            .recv()
            .await
            .expect("client relay attachment commit observer")
    }
}

#[cfg(test)]
impl Drop for ClientRelayAttachmentCommitHandle {
    fn drop(&mut self) {
        let mut observers = CLIENT_RELAY_ATTACHMENT_COMMITS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .expect("client relay attachment commit registry lock");
        if observers
            .get(&self.key)
            .is_some_and(|(registered_id, _)| *registered_id == self.id)
        {
            observers.remove(&self.key);
        }
    }
}

#[cfg(test)]
pub(in crate::runtime) fn arm_client_relay_attachment_commits_for_test(
    path_instance_id: CarrierPathInstanceId,
    stream_id: StreamId,
) -> ClientRelayAttachmentCommitHandle {
    let key = (path_instance_id, stream_id);
    let id = NEXT_CLIENT_RELAY_ATTACHMENT_COMMIT_ID.fetch_add(1, Ordering::Relaxed);
    let (commits_tx, commits_rx) = mpsc::unbounded_channel();
    let replaced = CLIENT_RELAY_ATTACHMENT_COMMITS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("client relay attachment commit registry lock")
        .insert(key, (id, commits_tx));
    assert!(
        replaced.is_none(),
        "a client relay attachment commit observer is already armed for this carrier and stream"
    );
    ClientRelayAttachmentCommitHandle {
        key,
        id,
        commits: commits_rx,
    }
}

#[cfg(test)]
fn record_client_relay_attachment_commit_for_test(
    instance: RelayPathInstance,
    stream_id: StreamId,
) {
    if let Some((_, commits)) = CLIENT_RELAY_ATTACHMENT_COMMITS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("client relay attachment commit registry lock")
        .get(&(instance.path_instance_id, stream_id))
    {
        let _ = commits.send(instance);
    }
}

/// Open carrier awaiting attachment-set commit.
///
/// Keeping stream cleanup and scheduler load in one value makes cancellation,
/// duplicate rejection, and attach-control failure the same rollback path.
pub(in crate::runtime) struct OpenedRemoteStream {
    terminal: Option<crate::runtime::path::PendingStreamTerminal>,
    terminal_owner: Option<crate::runtime::path::ClientStreamTerminalOwner>,
    stream: Option<ReliablePathStream>,
    path_index: usize,
    path_instance_id: CarrierPathInstanceId,
    advertised_recv_max_offset: u64,
    load_lease: Option<RelayPathLoadLease>,
    startup: Option<ReliableRelayOpenedStartup>,
}

impl OpenedRemoteStream {
    pub(in crate::runtime) fn from_opened_carrier(
        mut carrier: crate::runtime::path::OpenedReliableCarrierStream,
        path_index: usize,
        advertised_recv_max_offset: u64,
    ) -> Self {
        let path_instance_id = carrier.path_instance_id;
        let retirement = carrier.retirement.take();
        let terminal = carrier.terminal.take();
        let terminal_owner = carrier.terminal_owner.take();
        let opened = Self {
            terminal,
            terminal_owner,
            stream: Some(ReliablePathStream::from_opened_carrier(carrier)),
            path_index,
            path_instance_id,
            advertised_recv_max_offset,
            load_lease: None,
            startup: None,
        };
        if let Some(retirement) = retirement {
            // The existing pending-wrapper Drop now owns exactly this cleanup.
            retirement.disarm();
        }
        opened
    }

    /// A concrete carrier open starts without scheduler ownership; its caller
    /// adds the reservation that represents the product attachment demand.
    #[cfg(test)]
    pub(in crate::runtime) fn pending(stream: ReliablePathStream, path_index: usize) -> Self {
        Self {
            terminal: None,
            terminal_owner: None,
            stream: Some(stream),
            path_index,
            path_instance_id: next_carrier_path_instance_id(),
            advertised_recv_max_offset: 0,
            load_lease: None,
            startup: None,
        }
    }

    pub(in crate::runtime) fn terminal_scope(&self) -> Option<crate::runtime::path::ClientStreamTerminalScope> {
        self.terminal.as_ref().and_then(|terminal| terminal.scope())
    }

    pub(in crate::runtime) fn terminal_error(&self) -> Option<RuntimeError> {
        self.terminal_scope().and_then(|scope| scope.reset_error())
    }

    pub(in crate::runtime) fn with_terminal_owner(mut self, owner: Option<crate::runtime::path::ClientStreamTerminalOwner>) -> Self {
        debug_assert!(self.terminal_owner.is_none());
        self.terminal_owner = owner;
        self
    }

    pub(in crate::runtime) fn take_terminal_owner(&mut self) -> Option<crate::runtime::path::ClientStreamTerminalOwner> {
        self.terminal_owner.take()
    }

    pub(in crate::runtime) fn stream(&self) -> &ReliablePathStream {
        self.stream.as_ref().expect("pending remote stream")
    }

    pub(in crate::runtime) fn stream_mut(&mut self) -> &mut ReliablePathStream {
        self.stream.as_mut().expect("pending remote stream")
    }

    pub(in crate::runtime) fn path_index(&self) -> usize {
        self.path_index
    }

    pub(in crate::runtime) fn path_instance_id(&self) -> CarrierPathInstanceId {
        self.path_instance_id
    }

    pub(in crate::runtime) fn with_load_lease(mut self, lease: RelayPathLoadLease) -> Self {
        debug_assert!(self.load_lease.is_none());
        debug_assert_eq!(
            lease.key(),
            RelayPathKey {
                underlay: self.stream().underlay,
                index: self.path_index,
            }
        );
        self.load_lease = Some(lease);
        self
    }

    pub(in crate::runtime) fn with_startup(
        mut self,
        plan: Arc<ReliableRelayReturnPlan>,
        opening_ordinal: u8,
        failed_ordinals: Vec<u8>,
    ) -> Self {
        debug_assert!(self.startup.is_none());
        let candidate = plan
            .candidate(opening_ordinal)
            .expect("opening ordinal belongs to frozen plan");
        debug_assert_eq!(
            candidate.key,
            RelayPathKey {
                underlay: self.stream().underlay,
                index: self.path_index,
            }
        );
        debug_assert!(
            candidate
                .path_instance_id
                .is_none_or(|frozen| frozen == self.path_instance_id)
        );
        self.startup = Some(ReliableRelayOpenedStartup {
            plan,
            opening_ordinal,
            failed_ordinals,
        });
        self
    }

    pub(in crate::runtime) fn startup(&self) -> Option<&ReliableRelayOpenedStartup> {
        self.startup.as_ref()
    }

    pub(in crate::runtime) fn into_attachment_parts(
        mut self,
    ) -> (
        ReliablePathStream,
        usize,
        CarrierPathInstanceId,
        u64,
        Option<RelayPathLoadLease>,
    ) {
        let stream = self.stream.take().expect("pending remote stream");
        let load_lease = self.load_lease.take();
        (
            stream,
            self.path_index,
            self.path_instance_id,
            self.advertised_recv_max_offset,
            load_lease,
        )
    }

    /// A stream that never commits to a remote set must release both the peer
    /// binding and the local carrier actor entry.
    pub(in crate::runtime) async fn close(mut self) {
        drop(self.load_lease.take());
        if let Some(stream) = self.stream.as_ref() {
            stream.send_detach().await;
            stream.close().await;
        }
        drop(self.stream.take());
    }

    /// Transfers an uncommitted accepted stream to the carrier-owned
    /// retirement lane. The lane preserves detach-before-close ordering
    /// without waiting for bounded Product command capacity.
    pub(in crate::runtime) fn retire_uncommitted(mut self) {
        drop(self.load_lease.take());
        let Some(stream) = self.stream.take() else {
            return;
        };
        let _ = stream.retire_uncommitted();
    }
}

impl Drop for OpenedRemoteStream {
    fn drop(&mut self) {
        // Scheduler ownership must disappear before carrier cleanup can block.
        drop(self.load_lease.take());
        let Some(stream) = self.stream.take() else {
            return;
        };
        let _ = stream.retire_uncommitted();
    }
}

pub(in crate::runtime) struct ReliableRelayRemotePath {
    pub(in crate::runtime) path_index: usize,
    pub(in crate::runtime) path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime) attachment_id: u64,
    // Declaration order also depublishes load before an abrupt stream drop.
    pub(in crate::runtime) load_lease: Option<RelayPathLoadLease>,
    pub(in crate::runtime) attached_at: Instant,
    pub(in crate::runtime) path_proof_id: Option<u64>,
    pub(in crate::runtime) path_proof_generation: u64,
    /// Greatest shared receive grant accepted by this attachment's queue.
    pub(in crate::runtime) published_max_data_offset: u64,
    /// Publication fence for the logical receiver's retained cumulative ACK.
    stream_ack_publication: StreamFeedbackPublicationCursor,
    /// One actor-accepted incoming proof and its bounded, exact-output reply.
    feedback_receipt: ClientFeedbackReceipt,
    /// Immutable FINAL set accepted by this attachment's control queue in the
    /// current membership wave. A later attachment starts unpublished.
    published_return_plan_final: Option<Vec<u8>>,
    input_forwarder: ReliableRelayInputForwarder,
    pub(in crate::runtime) stream: ReliablePathStreamHandle,
}

#[derive(Debug, Default)]
struct ClientFeedbackReceipt {
    latest: Option<(u64, u64)>,
    pending: bool,
}

impl ClientFeedbackReceipt {
    fn observe(&mut self, token: u64, max_offset: u64) -> Result<(), RuntimeError> {
        if let Some((latest, required)) = self.latest {
            if token < latest {
                return Ok(());
            }
            if token == latest {
                return if max_offset == required {
                    Ok(())
                } else {
                    Err(RuntimeError::Protocol(
                        "feedback probe token changed its credit fence",
                    ))
                };
            }
        }
        self.latest = Some((token, max_offset));
        self.pending = true;
        Ok(())
    }

    fn ready(&self, applied_max_offset: u64) -> Option<u64> {
        self.latest.and_then(|(token, required)| {
            (self.pending && applied_max_offset >= required).then_some(token)
        })
    }

    fn admitted(&mut self, token: u64) {
        if self.latest.is_some_and(|(latest, _)| latest == token) {
            self.pending = false;
        }
    }
}

impl ReliableRelayRemotePath {
    pub(in crate::runtime) fn key(&self) -> RelayPathKey {
        RelayPathKey {
            underlay: self.stream.underlay,
            index: self.path_index,
        }
    }

    pub(in crate::runtime) fn instance(&self) -> RelayPathInstance {
        RelayPathInstance {
            key: self.key(),
            path_instance_id: self.path_instance_id,
            attachment_id: self.attachment_id,
        }
    }

    pub(in crate::runtime) fn has_load_reservation(&self) -> bool {
        self.load_lease.is_some()
    }

    /// Carrier shutdown may block, so retire scheduler-visible load first.
    fn depublish_load(&mut self) {
        drop(self.load_lease.take());
    }

    fn stop_input_forwarder(&self) {
        self.input_forwarder.abort();
    }
}

pub(in crate::runtime) struct ReliableRelayRemoteFrame {
    pub(in crate::runtime) instance: RelayPathInstance,
    pub(in crate::runtime) frame: Result<Frame, RuntimeError>,
}

struct ReliableRelayInputForwarder(tokio::task::JoinHandle<()>);

impl ReliableRelayInputForwarder {
    fn abort(&self) {
        self.0.abort();
    }
}

impl Drop for ReliableRelayInputForwarder {
    fn drop(&mut self) {
        self.abort();
    }
}

#[derive(Default)]
struct ReliableRelayCreditState {
    greatest: Option<(RelayPathInstance, u64)>,
    pending: bool,
    sealed: bool,
}

/// Received credit belongs to this logical input lifetime, not its carrier
/// queues. This retains evidence only; the relay's mux still applies the grant.
struct ReliableRelayCreditIngress {
    stream_id: StreamId,
    state: Mutex<ReliableRelayCreditState>,
    changed: Notify,
}

impl ReliableRelayCreditIngress {
    fn new(stream_id: StreamId) -> Self {
        Self {
            stream_id,
            state: Mutex::new(ReliableRelayCreditState::default()),
            changed: Notify::new(),
        }
    }

    fn publish(&self, instance: RelayPathInstance, max_offset: u64) {
        let wake = {
            let mut state = self.state.lock().expect("request credit ingress lock");
            if state.sealed
                || state
                    .greatest
                    .is_some_and(|(_, greatest)| max_offset <= greatest)
            {
                return;
            }
            // A strict advance keeps its actual source; equal siblings cannot
            // replace that source or create another pending service event.
            state.greatest = Some((instance, max_offset));
            !std::mem::replace(&mut state.pending, true)
        };
        if wake {
            self.changed.notify_one();
        }
    }

    fn take_pending(&self) -> Option<ReliableRelayRemoteFrame> {
        let mut state = self.state.lock().expect("request credit ingress lock");
        if !std::mem::take(&mut state.pending) {
            return None;
        }
        let (instance, max_offset) = state.greatest.expect("pending credit has a value");
        Some(ReliableRelayRemoteFrame {
            instance,
            frame: Ok(Frame::StreamMaxData {
                stream_id: self.stream_id,
                max_offset,
            }),
        })
    }

    fn has_pending(&self) -> bool {
        self.state
            .lock()
            .expect("request credit ingress lock")
            .pending
    }

    fn seal(&self) {
        self.state
            .lock()
            .expect("request credit ingress lock")
            .sealed = true;
    }

    fn close(&self) {
        {
            let mut state = self.state.lock().expect("request credit ingress lock");
            state.sealed = true;
            state.pending = false;
            state.greatest = None;
        }
        self.changed.notify_waiters();
    }
}

async fn forward_reliable_relay_attachment_frame(
    frames_tx: &mpsc::Sender<ReliableRelayRemoteFrame>,
    credit: &ReliableRelayCreditIngress,
    instance: RelayPathInstance,
    frame: Result<Frame, RuntimeError>,
    product_terminal_received: &mut bool,
) -> bool {
    *product_terminal_received |= matches!(
        &frame,
        Ok(Frame::StreamFin { .. } | Frame::StreamReset { .. })
    );
    if let Ok(Frame::StreamMaxData {
        stream_id,
        max_offset,
    }) = &frame
        && *stream_id == credit.stream_id
    {
        // Absorbing state must not keep a retired recipient's forwarder alive.
        let forwarded = if frames_tx.is_closed() {
            false
        } else {
            credit.publish(instance, *max_offset);
            !frames_tx.is_closed()
        };
        #[cfg(test)]
        tests::observe_attachment_input_processed(instance);
        return forwarded;
    }
    if matches!(&frame, Ok(Frame::StreamReset { stream_id, .. }) if *stream_id == credit.stream_id)
    {
        // Preserve prior pending credit, but no later attachment can revive
        // credit after this logical RESET. FIN and carrier errors do not seal.
        credit.seal();
    }
    let carrier_terminal = frame.is_err();
    let forwarded = frames_tx
        .send(ReliableRelayRemoteFrame { instance, frame })
        .await
        .is_ok();
    #[cfg(test)]
    tests::observe_attachment_input_processed(instance);
    forwarded && !carrier_terminal
}

async fn drain_reliable_relay_attachment_after_terminal(
    instance: RelayPathInstance,
    frames: &mut mpsc::Receiver<Result<Frame, RuntimeError>>,
    frames_tx: &mpsc::Sender<ReliableRelayRemoteFrame>,
    credit: &ReliableRelayCreditIngress,
    cause: ReliablePathCarrierTerminalCause,
    mut product_terminal_received: bool,
) {
    // Closing the receiver rejects sends that did not cross admission before
    // terminal. Tokio still delivers buffered messages and outstanding permits,
    // so reaching `None` is the exact accepted-input drain boundary.
    frames.close();
    while let Some(frame) = frames.recv().await {
        if !forward_reliable_relay_attachment_frame(
            frames_tx,
            credit,
            instance,
            frame,
            &mut product_terminal_received,
        )
        .await
        {
            return;
        }
    }
    // A product FIN suppresses only an unclassified input-channel closure.
    // Exact carrier terminal authority remains observable because the other
    // product direction may still need recovery and final feedback.
    let _ = frames_tx
        .send(ReliableRelayRemoteFrame {
            instance,
            frame: Err(cause.into_error()),
        })
        .await;
}

async fn forward_reliable_relay_attachment_frames(
    instance: RelayPathInstance,
    mut frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
    frames_tx: mpsc::Sender<ReliableRelayRemoteFrame>,
    credit: Arc<ReliableRelayCreditIngress>,
    terminal: ReliablePathCarrierTerminalSignal,
) {
    let mut product_terminal_received = false;
    loop {
        // The sticky check prevents a continuously ready producer from
        // starving terminal observation. A frame selected concurrently with
        // terminal had already crossed input admission and remains ordered.
        if let Some(cause) = terminal.cause() {
            drain_reliable_relay_attachment_after_terminal(
                instance,
                &mut frames,
                &frames_tx,
                &credit,
                cause,
                product_terminal_received,
            )
            .await;
            return;
        }
        tokio::select! {
            biased;
            frame = frames.recv() => {
                let Some(frame) = frame else {
                    if product_terminal_received {
                        // A product terminal explains input closure but not a
                        // later output-owner failure. Remain attachment-local
                        // until exact membership removal aborts this watcher.
                        tokio::select! {
                            cause = terminal.wait() => {
                                let _ = frames_tx
                                    .send(ReliableRelayRemoteFrame {
                                        instance,
                                        frame: Err(cause.into_error()),
                                    })
                                    .await;
                            }
                            _ = frames_tx.closed() => {}
                        }
                    } else {
                        let cause = terminal
                            .cause()
                            .unwrap_or(ReliablePathCarrierTerminalCause::Failed);
                        let _ = frames_tx
                            .send(ReliableRelayRemoteFrame {
                                instance,
                                frame: Err(cause.into_error()),
                            })
                            .await;
                    }
                    return;
                };
                if !forward_reliable_relay_attachment_frame(
                    &frames_tx,
                    &credit,
                    instance,
                    frame,
                    &mut product_terminal_received,
                )
                .await
                {
                    return;
                }
            }
            cause = terminal.wait() => {
                drain_reliable_relay_attachment_after_terminal(
                    instance,
                    &mut frames,
                    &frames_tx,
                    &credit,
                    cause,
                    product_terminal_received,
                )
                .await;
                return;
            }
        }
    }
}

/// Reports whether attachment-set ownership committed; a rejected pending open
/// rolls back its carrier and scheduler lease when the value is dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ReliableRelayAttachOutcome {
    Attached,
    RejectedDuplicate,
}

/// Actor-owned credit state and ordered event FIFO, independent from synchronous
/// attachment metadata. Exact path removal does not discard admitted input.
pub(in crate::runtime) struct ReliableRelayRemoteInput {
    frames_rx: mpsc::Receiver<ReliableRelayRemoteFrame>,
    credit: Arc<ReliableRelayCreditIngress>,
    /// A dequeued FIFO item retained while current credit is returned first.
    pending_frame: Option<ReliableRelayRemoteFrame>,
    prefer_credit: bool,
}

impl ReliableRelayRemoteInput {
    /// Return actual received MAX evidence before an ordered proof barrier.
    /// The probe's required offset is never receive-credit authority.
    pub(in crate::runtime) fn take_pending_credit(&mut self) -> Option<ReliableRelayRemoteFrame> {
        let credit = self.credit.take_pending();
        if credit.is_some() {
            self.prefer_credit = false;
        }
        credit
    }

    pub(in crate::runtime) async fn recv_frame(
        &mut self,
    ) -> Result<ReliableRelayRemoteFrame, RuntimeError> {
        // RelayServiceTurn owns cooperation for the whole Product turn,
        // including ready credit, dispatch and source reads. Do not add a
        // second input-only charging boundary inside that arbitration.
        let credit = self.credit.clone();
        loop {
            let changed = credit.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(frame) = self.try_recv_frame() {
                return Ok(frame);
            }
            tokio::select! {
                () = &mut changed => {}
                frame = self.frames_rx.recv() => {
                    match frame {
                        // Store ownership before another await, including
                        // cancellation of this receive by service arbitration.
                        Some(frame) => self.pending_frame = Some(frame),
                        None => return self.try_recv_frame()
                            .ok_or(RuntimeError::ReliablePathSessionClosed),
                    }
                }
            }
        }
    }

    /// Returns the visible FIFO backlog plus one current credit-state item.
    ///
    /// Ready-only receive batching snapshots this value before trying frames so
    /// producers cannot extend one actor turn indefinitely.
    pub(in crate::runtime) fn ready_frame_count(&self) -> usize {
        self.frames_rx
            .len()
            .saturating_add(usize::from(self.pending_frame.is_some()))
            .saturating_add(usize::from(self.credit.has_pending()))
    }

    /// Takes ready credit or a FIFO item without waiting. When both stay ready,
    /// alternate their service; a sealed RESET has at most one preceding grant.
    pub(in crate::runtime) fn try_recv_frame(&mut self) -> Option<ReliableRelayRemoteFrame> {
        if self.pending_frame.is_none() {
            self.pending_frame = self.frames_rx.try_recv().ok();
        }
        let before_reset = self.pending_frame.as_ref().is_some_and(|item| {
            matches!(&item.frame, Ok(Frame::StreamReset { stream_id, .. }) if *stream_id == self.credit.stream_id)
        });
        if (self.prefer_credit || before_reset || self.pending_frame.is_none())
            && let Some(credit) = self.credit.take_pending()
        {
            self.prefer_credit = false;
            return Some(credit);
        }
        let frame = self.pending_frame.take()?;
        self.prefer_credit = true;
        Some(frame)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn has_buffered_frame(&self) -> bool {
        self.ready_frame_count() > 0
    }
}

impl Drop for ReliableRelayRemoteInput {
    fn drop(&mut self) {
        self.credit.close();
        self.frames_rx.close();
    }
}

pub(in crate::runtime) struct ReliableRelayRemoteSet {
    _terminal_owner: Option<crate::runtime::path::ClientStreamTerminalOwner>,
    stream_id: StreamId,
    pub(in crate::runtime) paths: Vec<ReliableRelayRemotePath>,
    frames_tx: mpsc::Sender<ReliableRelayRemoteFrame>,
    credit: Arc<ReliableRelayCreditIngress>,
    /// Next exact attachment incarnation, or permanent exhaustion after MAX.
    next_instance_id: Option<u64>,
    membership_generation: u64,
    desired_feedback: StreamFeedbackState,
    #[cfg(feature = "lab-diagnostics")]
    feedback_diagnostic_scope: Option<(crate::protocol::SessionId, StreamId)>,
    /// Applied only by the logical send owner, never by a probe or decoder.
    applied_peer_max_offset: u64,
    /// Immutable startup receipt retained until response bytes above `h` or a
    /// response terminal proves the peer no longer needs a retry.
    desired_return_plan_final: Option<Vec<u8>>,
    /// One exact response-direction probe receipt retained independently from
    /// Product processing while every authenticated return attachment's
    /// control queue is full. The instance is only the preferred return path;
    /// the exact frame tuple identifies the peer's forward target. A newer
    /// probe supersedes an unqueued expired receipt.
    pending_requalification_ack: Option<(RelayPathInstance, Frame)>,
    /// Greatest accepted response-direction probe receipt. Retaining its exact
    /// tuple after queue publication makes authenticated duplicate and older
    /// probe replays bounded without suppressing a newer probe after expiry.
    latest_requalification_ack: Option<Frame>,
}

impl ReliableRelayRemoteSet {
    pub(in crate::runtime) fn publish_requalification_ack(
        &mut self,
        instance: RelayPathInstance,
        frame: Frame,
    ) -> Result<bool, RuntimeError> {
        let Frame::StreamRequalifyAck {
            probe_id: incoming_probe_id,
            ..
        } = &frame
        else {
            return Err(RuntimeError::Protocol(
                "pending requalification ACK must be STREAM_REQUALIFY_ACK",
            ));
        };
        if let Some(latest_frame) = &self.latest_requalification_ack {
            let Frame::StreamRequalifyAck {
                probe_id: latest_probe_id,
                ..
            } = latest_frame
            else {
                unreachable!("latest requalification ACK frame kind")
            };
            if incoming_probe_id < latest_probe_id {
                // Probe IDs are monotonic in one response direction. A delayed
                // replay cannot displace newer exact liveness work;
                // opportunistically retry that retained work instead.
                return self.retry_pending_requalification_ack();
            }
            if incoming_probe_id == latest_probe_id {
                if *latest_frame != frame {
                    return Err(RuntimeError::Protocol(
                        "requalification probe ID reused with a different exact tuple",
                    ));
                }
                // An equal replay retries only a still-retained zero-publication
                // receipt. Once one bounded fanout pass commits, it is a no-op.
                return self.retry_pending_requalification_ack();
            }
        }
        self.latest_requalification_ack = Some(frame.clone());
        self.pending_requalification_ack = Some((instance, frame));
        self.retry_pending_requalification_ack()
    }

    pub(in crate::runtime) fn retry_pending_requalification_ack(
        &mut self,
    ) -> Result<bool, RuntimeError> {
        let Some((preferred_instance, frame)) = self.pending_requalification_ack.clone() else {
            return Ok(false);
        };
        let preferred = self
            .paths
            .iter()
            .position(|path| path.instance() == preferred_instance);
        let candidates = preferred
            .into_iter()
            .chain((0..self.paths.len()).filter(move |candidate| Some(*candidate) != preferred));
        let mut published = false;
        let mut first_error = None;
        for candidate in candidates {
            match self.paths[candidate]
                .stream
                .try_enqueue_request_control_frame(frame.clone())
            {
                Ok(()) => published = true,
                Err(RuntimeError::SenderServiceBlocked)
                | Err(RuntimeError::ReliablePathSessionClosed) => {}
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(_) => {}
            }
        }
        if published {
            // Queue admission is not evidence that the selected carrier will
            // receive native service. Publish one identical, idempotent copy
            // on every attachment that admits it in this bounded pass, then
            // retire the retained receipt after the first committed pass.
            self.pending_requalification_ack = None;
            return Ok(true);
        }
        if let Some(error) = first_error {
            self.pending_requalification_ack = None;
            return Err(error);
        }
        Ok(false)
    }

    pub(in crate::runtime) fn has_pending_requalification_ack(&self) -> bool {
        self.pending_requalification_ack.is_some()
    }

    pub(in crate::runtime) fn pending_requalification_ack_capacity_notifies(
        &self,
    ) -> Vec<std::sync::Arc<tokio::sync::Notify>> {
        if self.pending_requalification_ack.is_none() {
            return Vec::new();
        }
        self.paths
            .iter()
            .filter_map(|path| path.stream.request_control_capacity_notify())
            .collect()
    }

    #[cfg(test)]
    pub(in crate::runtime) fn new(
        opened: OpenedRemoteStream,
        frame_queue: usize,
    ) -> (Self, ReliableRelayRemoteInput) {
        Self::try_new(opened, frame_queue).expect("test initial attachment commit")
    }

    pub(in crate::runtime) fn try_new(
        mut opened: OpenedRemoteStream,
        frame_queue: usize,
    ) -> Result<(Self, ReliableRelayRemoteInput), RuntimeError> {
        let stream_id = opened.stream().stream_id;
        let (frames_tx, frames_rx) = mpsc::channel(frame_queue);
        let credit = Arc::new(ReliableRelayCreditIngress::new(stream_id));
        let mut set = Self {
            _terminal_owner: opened.take_terminal_owner(),
            stream_id,
            paths: Vec::new(),
            frames_tx,
            credit: credit.clone(),
            next_instance_id: Some(0),
            membership_generation: 0,
            desired_feedback: StreamFeedbackState::default(),
            #[cfg(feature = "lab-diagnostics")]
            feedback_diagnostic_scope: None,
            applied_peer_max_offset: 0,
            desired_return_plan_final: None,
            pending_requalification_ack: None,
            latest_requalification_ack: None,
        };
        let outcome = set.attach_opened(opened)?;
        debug_assert_eq!(outcome, ReliableRelayAttachOutcome::Attached);
        Ok((
            set,
            ReliableRelayRemoteInput {
                frames_rx,
                credit,
                pending_frame: None,
                prefer_credit: true,
            },
        ))
    }

    pub(in crate::runtime) fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub(in crate::runtime) fn membership_generation(&self) -> u64 {
        self.membership_generation
    }

    fn allocate_attachment_incarnation(&mut self) -> Result<u64, RuntimeError> {
        let attachment_id = self
            .next_instance_id
            .ok_or(RuntimeError::ExactIdentityExhausted)?;
        self.next_instance_id = attachment_id.checked_add(1);
        Ok(attachment_id)
    }

    /// A selection is valid only for the exact attachment topology it observed.
    pub(in crate::runtime) fn path_position_at_generation(
        &self,
        generation: u64,
        instance: RelayPathInstance,
    ) -> Option<usize> {
        if self.membership_generation != generation {
            return None;
        }
        self.paths
            .iter()
            .position(|path| path.instance() == instance)
    }

    /// Returns the currently available attachment with the lowest scheduler
    /// completion estimate. Attachment order is never path-quality evidence.
    pub(in crate::runtime) fn lowest_eta_path_snapshot(
        &self,
        context: &ClientPathContext,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> Option<PathSnapshot> {
        let choose = |allow_backup: bool| {
            self.paths
                .iter()
                .filter_map(|path| context.reliable_path_snapshot_for_instance(path.instance()))
                .filter(|snapshot| allow_backup || !path_is_backup(*snapshot))
                .filter_map(|snapshot| {
                    score_path(snapshot, lane, payload_bytes).map(|score| (score.eta_ms, snapshot))
                })
                .min_by(|left, right| left.0.total_cmp(&right.0))
                .map(|(_, snapshot)| snapshot)
        };
        choose(false).or_else(|| choose(true))
    }

    pub(in crate::runtime) fn preferred_path_key(
        &self,
        context: &ClientPathContext,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> Option<RelayPathKey> {
        let choose = |allow_backup: bool| {
            self.paths
                .iter()
                .filter_map(|path| {
                    let snapshot = context.reliable_path_snapshot_for_instance(path.instance())?;
                    if !allow_backup && path_is_backup(snapshot) {
                        return None;
                    }
                    let score = score_path(snapshot, lane, payload_bytes)?;
                    Some((path.key(), score.eta_ms))
                })
                .min_by(|left, right| left.1.total_cmp(&right.1))
                .map(|(key, _)| key)
        };
        choose(false).or_else(|| choose(true))
    }

    pub(in crate::runtime) fn preferred_path_underlay(
        &self,
        context: &ClientPathContext,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> Option<UnderlayProtocol> {
        self.preferred_path_key(context, lane, payload_bytes)
            .map(|key| key.underlay)
    }

    pub(in crate::runtime) fn contains_path_key(&self, key: RelayPathKey) -> bool {
        self.paths.iter().any(|path| path.key() == key)
    }

    pub(in crate::runtime) fn contains_path_instance(&self, instance: RelayPathInstance) -> bool {
        self.paths.iter().any(|path| path.instance() == instance)
    }

    pub(in crate::runtime) fn path_instance_for_key(
        &self,
        key: RelayPathKey,
    ) -> Option<RelayPathInstance> {
        self.paths
            .iter()
            .find_map(|path| (path.key() == key).then(|| path.instance()))
    }

    pub(in crate::runtime) fn path_keys(&self) -> Vec<RelayPathKey> {
        self.paths
            .iter()
            .map(ReliableRelayRemotePath::key)
            .collect()
    }

    pub(in crate::runtime) fn path_instances(&self) -> Vec<RelayPathInstance> {
        self.paths
            .iter()
            .map(ReliableRelayRemotePath::instance)
            .collect()
    }

    pub(in crate::runtime) fn accepted_path_count(&self) -> usize {
        self.paths.len()
    }

    pub(in crate::runtime) fn has_receive_feedback_output(&self) -> bool {
        self.paths
            .iter()
            .any(|path| !path.stream.request_control_frame_admission_is_closed())
    }

    pub(in crate::runtime) fn publish_return_plan_final(
        &mut self,
        retained_ordinals: &[u8],
    ) -> Result<bool, RuntimeError> {
        if retained_ordinals.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(RuntimeError::Protocol(
                "return plan FINAL ordinals must be strictly increasing",
            ));
        }
        if let Some(current) = &self.desired_return_plan_final {
            if current != retained_ordinals {
                return Err(RuntimeError::Protocol(
                    "return plan FINAL changed after publication",
                ));
            }
        } else {
            self.desired_return_plan_final = Some(retained_ordinals.to_vec());
        }
        Ok(self.retry_pending_return_plan_final())
    }

    pub(in crate::runtime) fn retry_pending_return_plan_final(&mut self) -> bool {
        let Some(retained_ordinals) = self.desired_return_plan_final.clone() else {
            return false;
        };
        let mut published = false;
        for path in &mut self.paths {
            if path.stream.request_control_frame_admission_is_closed()
                || path.published_return_plan_final.as_deref() == Some(retained_ordinals.as_slice())
            {
                continue;
            }
            if path
                .stream
                .try_enqueue_request_control_frame(Frame::StreamReturnPlanFinal {
                    stream_id: self.stream_id,
                    retained_ordinals: retained_ordinals.clone(),
                })
                .is_ok()
            {
                path.published_return_plan_final = Some(retained_ordinals.clone());
                published = true;
            }
        }
        published
    }

    pub(in crate::runtime) fn has_pending_return_plan_final_publication(&self) -> bool {
        let Some(retained_ordinals) = &self.desired_return_plan_final else {
            return false;
        };
        self.paths.iter().any(|path| {
            !path.stream.request_control_frame_admission_is_closed()
                && path.published_return_plan_final.as_ref() != Some(retained_ordinals)
        })
    }

    pub(in crate::runtime) fn pending_return_plan_final_capacity_notifies(
        &self,
    ) -> Vec<std::sync::Arc<tokio::sync::Notify>> {
        let Some(retained_ordinals) = &self.desired_return_plan_final else {
            return Vec::new();
        };
        self.paths
            .iter()
            .filter(|path| {
                path.published_return_plan_final.as_ref() != Some(retained_ordinals)
                    && !path.stream.request_control_frame_admission_is_closed()
            })
            .filter_map(|path| path.stream.request_control_capacity_notify())
            .collect()
    }

    pub(in crate::runtime) fn clear_return_plan_final(&mut self) {
        self.desired_return_plan_final = None;
        for path in &mut self.paths {
            path.published_return_plan_final = None;
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn publish_max_data(
        &mut self,
        max_offset: u64,
    ) -> StreamFeedbackPublication {
        self.publish_max_data_inner(None, max_offset)
    }

    pub(in crate::runtime) fn publish_max_data_with_context(
        &mut self,
        context: &ClientPathContext,
        max_offset: u64,
    ) -> StreamFeedbackPublication {
        self.publish_max_data_inner(Some(context), max_offset)
    }

    fn publish_max_data_inner(
        &mut self,
        context: Option<&ClientPathContext>,
        max_offset: u64,
    ) -> StreamFeedbackPublication {
        self.desired_feedback.max_data_offset =
            self.desired_feedback.max_data_offset.max(max_offset);
        #[cfg(feature = "lab-diagnostics")]
        if let Some(context) = context {
            self.feedback_diagnostic_scope = Some((context.session_id, self.stream_id));
        }
        #[cfg(not(feature = "lab-diagnostics"))]
        let _ = context;
        self.service_pending_feedback(None)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn retry_pending_max_data(&mut self) -> StreamFeedbackPublication {
        self.service_pending_feedback(None)
    }

    pub(in crate::runtime) fn has_pending_max_data_publication(&self) -> bool {
        self.paths.iter().any(|path| {
            !path.stream.request_control_frame_admission_is_closed()
                && path.published_max_data_offset < self.desired_feedback.max_data_offset
        })
    }

    /// Retains the latest cumulative receive evidence and offers it to every
    /// exact live attachment. A started immutable tail finishes before a newer
    /// generation's catch-up; MAX shares each output's successful service turns.
    #[cfg(test)]
    pub(in crate::runtime) fn publish_stream_ack(
        &mut self,
        generation: u64,
        update_frames: Vec<Frame>,
        cumulative_frames: Vec<Frame>,
    ) -> StreamFeedbackPublication {
        self.publish_stream_ack_inner(None, generation, update_frames, cumulative_frames)
    }

    pub(in crate::runtime) fn publish_stream_ack_with_context(
        &mut self,
        context: &ClientPathContext,
        generation: u64,
        update_frames: Vec<Frame>,
        cumulative_frames: Vec<Frame>,
    ) -> StreamFeedbackPublication {
        self.publish_stream_ack_inner(Some(context), generation, update_frames, cumulative_frames)
    }

    fn publish_stream_ack_inner(
        &mut self,
        context: Option<&ClientPathContext>,
        generation: u64,
        update_frames: Vec<Frame>,
        cumulative_frames: Vec<Frame>,
    ) -> StreamFeedbackPublication {
        debug_assert!(generation != 0);
        debug_assert!(!update_frames.is_empty());
        debug_assert!(!cumulative_frames.is_empty());
        debug_assert!(
            update_frames
                .iter()
                .chain(&cumulative_frames)
                .all(|frame| matches!(frame, Frame::StreamAck { stream_id, .. } if *stream_id == self.stream_id))
        );
        self.desired_feedback.ack_generation = generation;
        self.desired_feedback.cumulative_ack_frames = cumulative_frames;
        #[cfg(feature = "lab-diagnostics")]
        if let Some(context) = context {
            self.feedback_diagnostic_scope = Some((context.session_id, self.stream_id));
        }
        #[cfg(not(feature = "lab-diagnostics"))]
        let _ = context;
        self.service_pending_feedback(Some(&update_frames))
    }

    pub(in crate::runtime) fn stream_ack_generation(&self) -> u64 {
        self.desired_feedback.ack_generation
    }

    pub(in crate::runtime) fn feedback_max_data_offset(&self) -> u64 {
        self.desired_feedback.max_data_offset
    }

    #[cfg(test)]
    pub(in crate::runtime) fn retry_pending_stream_ack(&mut self) -> StreamFeedbackPublication {
        self.service_pending_feedback(None)
    }

    pub(in crate::runtime) fn retry_pending_feedback_with_context(
        &mut self,
        context: &ClientPathContext,
    ) -> StreamFeedbackPublication {
        #[cfg(feature = "lab-diagnostics")]
        {
            self.feedback_diagnostic_scope = Some((context.session_id, self.stream_id));
        }
        #[cfg(not(feature = "lab-diagnostics"))]
        let _ = context;
        self.service_pending_feedback(None)
    }

    pub(in crate::runtime) fn observe_applied_peer_max_offset(&mut self, offset: u64) {
        self.applied_peer_max_offset = self.applied_peer_max_offset.max(offset);
    }

    /// Called only after the logical owner has applied preceding ACK frames.
    pub(in crate::runtime) fn receive_feedback_probe(
        &mut self,
        instance: RelayPathInstance,
        token: u64,
        required_max_offset: u64,
    ) -> Result<(), RuntimeError> {
        let Some(path) = self
            .paths
            .iter_mut()
            .find(|path| path.instance() == instance)
        else {
            return Ok(());
        };
        if path.stream.request_control_frame_admission_is_closed() {
            return Ok(());
        }
        #[cfg(feature = "lab-diagnostics")]
        let previous = path.feedback_receipt.latest;
        let result = path.feedback_receipt.observe(token, required_max_offset);
        #[cfg(feature = "lab-diagnostics")]
        if result.is_ok() && path.feedback_receipt.latest != previous {
            super::super::feedback::lab_feedback_return(
                self.feedback_diagnostic_scope,
                "reply_bound",
                format_args!(
                    "output={:?} token={} required_max_offset={} applied_peer_max_offset={}",
                    instance, token, required_max_offset, self.applied_peer_max_offset,
                ),
            );
        }
        result
    }

    fn path_has_pending_feedback(&self, path: &ReliableRelayRemotePath) -> bool {
        !path.stream.request_control_frame_admission_is_closed()
            && ((path
                .stream_ack_publication
                .is_pending(self.desired_feedback.ack_generation)
                || path.published_max_data_offset < self.desired_feedback.max_data_offset)
                || path
                    .feedback_receipt
                    .ready(self.applied_peer_max_offset)
                    .is_some())
    }

    fn service_pending_feedback(
        &mut self,
        update_frames: Option<&[Frame]>,
    ) -> StreamFeedbackPublication {
        let stream_id = self.stream_id;
        let state = &self.desired_feedback;
        let mut publication = StreamFeedbackPublication {
            ack_generation: state.ack_generation,
            ..StreamFeedbackPublication::default()
        };
        for path in &mut self.paths {
            if path.stream.request_control_frame_admission_is_closed() {
                // Channel closure and terminal carrier lifecycle are sticky
                // for this exact fixed output. Drop only its new tail storage;
                // ordered attachment removal and shared desired truth remain.
                path.stream_ack_publication = StreamFeedbackPublicationCursor::default();
                path.feedback_receipt = ClientFeedbackReceipt::default();
                continue;
            }
            #[cfg(feature = "lab-diagnostics")]
            let instance = path.instance();
            let attachment = path.stream_ack_publication.service(
                state,
                update_frames,
                stream_id,
                &mut path.published_max_data_offset,
                StreamFeedbackService {
                    receipt: path.feedback_receipt.ready(self.applied_peer_max_offset),
                },
                |frame| path.stream.try_enqueue_request_control_frame(frame).is_ok(),
            );
            if let Some(token) = attachment.receipt_admitted {
                #[cfg(feature = "lab-diagnostics")]
                super::super::feedback::lab_feedback_return(
                    self.feedback_diagnostic_scope,
                    "reply_admitted",
                    format_args!(
                        "output={:?} token={} required_max_offset={:?} applied_peer_max_offset={}",
                        instance,
                        token,
                        path.feedback_receipt.latest.map(|(_, required)| required),
                        self.applied_peer_max_offset,
                    ),
                );
                path.feedback_receipt.admitted(token);
            }
            publication.merge(attachment.feedback);
        }
        publication.ack.pending = self.has_pending_stream_ack_publication();
        publication.max_data.pending = self.has_pending_max_data_publication();
        publication
    }

    pub(in crate::runtime) fn has_pending_stream_ack_publication(&self) -> bool {
        let generation = self.desired_feedback.ack_generation;
        generation != 0
            && self.paths.iter().any(|path| {
                !path.stream.request_control_frame_admission_is_closed()
                    && path.stream_ack_publication.is_pending(generation)
            })
    }

    pub(in crate::runtime) fn has_pending_feedback_publication(&self) -> bool {
        self.paths
            .iter()
            .any(|path| self.path_has_pending_feedback(path))
    }

    pub(in crate::runtime) fn pending_feedback_capacity_notifies(
        &self,
    ) -> Vec<std::sync::Arc<tokio::sync::Notify>> {
        self.paths
            .iter()
            .filter(|path| self.path_has_pending_feedback(path))
            .filter_map(|path| path.stream.request_control_capacity_notify())
            .collect()
    }

    pub(in crate::runtime) fn set_lane(&mut self, lane: TrafficClass) {
        for path in &mut self.paths {
            path.stream.lane = lane;
            if let Some(lease) = &mut path.load_lease {
                // The lease owns both the lane label and the exact-incarnation
                // counter mutation, so a retired predecessor cannot reclassify
                // successor demand.
                lease.set_recorded_lane(lane);
            }
        }
    }

    pub(in crate::runtime) fn retry_pending_path_proofs(&mut self, context: &ClientPathContext) {
        for path in &mut self.paths {
            let generation = context
                .relay_path_proof_generation(path.key().underlay, path.key().index)
                .unwrap_or(path.path_proof_generation);
            if path.path_proof_id.is_some() && path.path_proof_generation == generation {
                continue;
            }
            if let Ok(Some(proof_id)) = path.stream.enqueue_path_proof() {
                path.path_proof_id = Some(proof_id);
                path.path_proof_generation = generation;
            }
        }
    }

    pub(in crate::runtime) fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    pub(in crate::runtime) fn max_offset(&self) -> u64 {
        self.paths
            .iter()
            .map(|path| path.stream.max_offset)
            .max()
            .unwrap_or(0)
    }

    pub(in crate::runtime) fn max_frame_payload_bytes(&self, mux_limits: MuxLimits) -> usize {
        self.paths
            .iter()
            .map(|path| path.stream.max_frame_payload_bytes)
            .min()
            .unwrap_or_else(|| reliable_relay_buffer_len(mux_limits))
            .max(1)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn attach(
        &mut self,
        opened: OpenedRemoteStream,
    ) -> ReliableRelayAttachOutcome {
        self.attach_opened(opened)
            .expect("test request attachment identity space")
    }

    #[cfg(test)]
    pub(in crate::runtime) fn attach_candidate(
        &mut self,
        opened: OpenedRemoteStream,
    ) -> ReliableRelayAttachOutcome {
        self.attach_opened(opened)
            .expect("test request attachment identity space")
    }

    pub(in crate::runtime) fn try_attach_candidate(
        &mut self,
        opened: OpenedRemoteStream,
    ) -> Result<ReliableRelayAttachOutcome, RuntimeError> {
        self.attach_opened(opened)
    }

    fn attach_opened(
        &mut self,
        mut opened: OpenedRemoteStream,
    ) -> Result<ReliableRelayAttachOutcome, RuntimeError> {
        if let Some(error) = opened.terminal_error() {
            return Err(error);
        }
        // A direct low-level opener may own its own temporary scope. Keep it
        // alive through commit, then close it outside the publication lock.
        let _opening_owner = opened.take_terminal_owner();
        let terminal = opened.terminal.clone();
        let path_index = opened.path_index();
        let underlay = opened.stream().underlay;
        let key = RelayPathKey {
            underlay,
            index: path_index,
        };
        if self.contains_path_key(key) {
            return Ok(ReliableRelayAttachOutcome::RejectedDuplicate);
        }
        let attachment_id = self.allocate_attachment_incarnation()?;
        match terminal {
            Some(terminal) => terminal.commit(|| self.commit_opened(opened, attachment_id)),
            None => Ok(self.commit_opened(opened, attachment_id)),
        }
    }

    fn commit_opened(
        &mut self,
        opened: OpenedRemoteStream,
        attachment_id: u64,
    ) -> ReliableRelayAttachOutcome {
        let path_index = opened.path_index();
        let underlay = opened.stream().underlay;
        let key = RelayPathKey {
            underlay,
            index: path_index,
        };
        debug_assert!(!self.contains_path_key(key));
        let path_instance_id = opened.path_instance_id;
        let instance = RelayPathInstance {
            key,
            path_instance_id,
            attachment_id,
        };
        let (stream, path_index, path_instance_id, advertised_recv_max_offset, mut load_lease) =
            opened.into_attachment_parts();
        self.desired_feedback.max_data_offset = self
            .desired_feedback
            .max_data_offset
            .max(advertised_recv_max_offset);
        // Path opening reserves prospective load across asynchronous I/O. Once
        // attachment commits, membership is not active demand until this exact
        // stream assigns OriginalData to the path.
        drop(load_lease.take());
        let (stream, frames, terminal) = stream.into_handle_and_frames();
        let frames_tx = self.frames_tx.clone();
        let credit = self.credit.clone();
        let input_forwarder = ReliableRelayInputForwarder(tokio::spawn(
            forward_reliable_relay_attachment_frames(instance, frames, frames_tx, credit, terminal),
        ));
        let mut path = ReliableRelayRemotePath {
            path_index,
            path_instance_id,
            attachment_id,
            load_lease,
            attached_at: Instant::now(),
            path_proof_id: None,
            path_proof_generation: 0,
            published_max_data_offset: advertised_recv_max_offset,
            stream_ack_publication: StreamFeedbackPublicationCursor::default(),
            feedback_receipt: ClientFeedbackReceipt::default(),
            published_return_plan_final: None,
            input_forwarder,
            stream,
        };
        if let Ok(Some(proof_id)) = path.stream.enqueue_path_proof() {
            path.path_proof_id = Some(proof_id);
        }
        self.paths.push(path);
        self.membership_generation = self.membership_generation.wrapping_add(1);
        #[cfg(test)]
        record_client_relay_attachment_commit_for_test(instance, self.stream_id);
        ReliableRelayAttachOutcome::Attached
    }

    /// Withdraws membership now and returns owned carrier teardown work.
    /// Construct only for the selected cleanup action, not a discarded select
    /// alternative: withdrawal is independent of polling the returned future.
    pub(in crate::runtime) fn close_all(&mut self) -> impl Future<Output = ()> + use<> {
        let paths = self.take_paths_for_close();
        async move {
            for path in paths {
                path.stream.send_detach().await;
                path.stream.close().await;
            }
        }
    }

    /// Product endpoint failure is terminal across every attachment. A reset
    /// prevents retention and reinjection while carrier-only failures continue
    /// to use detach and preserve the logical stream for path recovery.
    /// Membership withdrawal is synchronous, as for `close_all`.
    pub(in crate::runtime) fn reset_all(
        &mut self,
        reason: ResetReason,
    ) -> impl Future<Output = ()> + use<> {
        let paths = self.take_paths_for_close();
        async move {
            futures::future::join_all(paths.into_iter().map(|path| async move {
                path.stream.reset_and_close(reason).await;
            }))
            .await;
        }
    }

    /// Removes every path from Product scheduling and synchronously transfers
    /// its reset to the carrier-owned retirement lane. Carrier publication can
    /// remain pending without retaining this Product lifetime.
    pub(in crate::runtime) fn retire_all_with_reset(&mut self, reason: ResetReason) {
        for path in self.take_paths_for_close() {
            path.stream.retire_with_reset(reason);
        }
    }

    /// Successful retirement follows ordered FIN work on every carrier.
    /// Membership withdrawal is synchronous, as for `close_all`.
    pub(in crate::runtime) fn close_all_ordered(&mut self) -> impl Future<Output = ()> + use<> {
        let paths = self.take_paths_for_close();
        async move {
            for path in paths {
                path.stream.detach_and_close_ordered().await;
            }
        }
    }

    fn take_paths_for_close(&mut self) -> Vec<ReliableRelayRemotePath> {
        if !self.paths.is_empty() {
            self.membership_generation = self.membership_generation.wrapping_add(1);
        }
        let mut paths = std::mem::take(&mut self.paths);
        // The set stops owning every path as one atomic scheduling event even
        // when the first carrier queue makes detach asynchronous.
        for path in &mut paths {
            path.stop_input_forwarder();
            path.depublish_load();
            path.stream_ack_publication = StreamFeedbackPublicationCursor::default();
            path.feedback_receipt = ClientFeedbackReceipt::default();
        }
        paths
    }

    pub(in crate::runtime) fn fail_path_instance(
        &mut self,
        context: &ClientPathContext,
        instance: RelayPathInstance,
    ) -> bool {
        let Some(mut path) = self.remove_path_instance(instance) else {
            return false;
        };
        context.mark_relay_path_data_plane_failure(instance);
        path.depublish_load();
        let _ = path.stream.retire_attachment();
        true
    }

    pub(in crate::runtime) fn retire_path_instance(&mut self, instance: RelayPathInstance) -> bool {
        let Some(mut path) = self.remove_path_instance(instance) else {
            return false;
        };
        path.depublish_load();
        let _ = path.stream.retire_attachment();
        true
    }

    pub(in crate::runtime) fn remove_path_instance(
        &mut self,
        instance: RelayPathInstance,
    ) -> Option<ReliableRelayRemotePath> {
        let position = self
            .paths
            .iter()
            .position(|path| path.instance() == instance)?;
        self.remove_path_at(position)
    }

    pub(in crate::runtime) fn remove_path_at(
        &mut self,
        position: usize,
    ) -> Option<ReliableRelayRemotePath> {
        let mut path = self.paths.remove(position);
        path.stop_input_forwarder();
        // Native teardown may remain asynchronous after membership withdrawal.
        // Its no-longer-serviceable feedback tail does not follow that lifetime.
        path.stream_ack_publication = StreamFeedbackPublicationCursor::default();
        path.feedback_receipt = ClientFeedbackReceipt::default();
        self.membership_generation = self.membership_generation.wrapping_add(1);
        Some(path)
    }

    /// Releases active OriginalData demand only for the exact live attachment.
    /// ACKs from a removed incarnation cannot depublish its successor.
    pub(in crate::runtime) fn depublish_path_instance_load(
        &mut self,
        instance: RelayPathInstance,
    ) -> bool {
        let Some(path) = self
            .paths
            .iter_mut()
            .find(|path| path.instance() == instance)
        else {
            return false;
        };
        let owned = path.has_load_reservation();
        path.depublish_load();
        owned
    }

    /// Transfers a pre-enqueue claim after the synchronous queue commit.
    /// Generation and instance were resolved with no intervening await.
    #[cfg(test)]
    pub(in crate::runtime) fn commit_path_instance_load_claim(
        &mut self,
        instance: RelayPathInstance,
        lease: RelayPathLoadLease,
    ) {
        let path = self
            .paths
            .iter_mut()
            .find(|path| path.instance() == instance)
            .expect("generation-fenced selected path must remain attached");
        assert!(
            !path.has_load_reservation(),
            "conditionally claimed path load must remain unowned before transfer"
        );
        path.load_lease = Some(lease);
    }
}

#[cfg(test)]
#[path = "tests_attachment.rs"]
mod tests;
