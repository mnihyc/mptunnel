//! Product-stream lifecycle on one reliable TCP connection.
//!
//! Open ownership, cancellation, pending deadlines, frame routing, and failure
//! fan-out stay together so a product stream has one lifecycle authority.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{lab_diagnostic, lab_perf_record};
use crate::model::capacity::reliable_relay_buffer_len;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::reliable_path_frame_pacing_bytes;
use crate::protocol::{
    Frame, StreamDemandHint, StreamId, StreamReturnPlan, TargetAddr, UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ClientTcpOpenAttemptId, ClientTcpOpenResponse, ClientTcpOpenedStream, ReliablePathCommand,
    ReliablePathCommandSender,
};
use crate::runtime::path::ports::OpenedReliableCarrierStream;
use crate::runtime::path::tcp::client::state::{
    ClientTcpPathConnection, ClientTcpPathSessionRuntime,
};
use crate::runtime::recent_ids::RecentIdCache;
use crate::scheduler::TrafficClass;
use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

pub(in crate::runtime::path::tcp) struct ClientTcpOpenCancellation {
    commands: ReliablePathCommandSender,
    stream_id: StreamId,
    attempt_id: ClientTcpOpenAttemptId,
    armed: bool,
}

static NEXT_CLIENT_TCP_OPEN_ATTEMPT_ID: AtomicU64 = AtomicU64::new(1);

pub(in crate::runtime::path::tcp) fn next_client_tcp_open_attempt_id() -> ClientTcpOpenAttemptId {
    ClientTcpOpenAttemptId(NEXT_CLIENT_TCP_OPEN_ATTEMPT_ID.fetch_add(1, Ordering::Relaxed))
}

impl ClientTcpOpenCancellation {
    pub(in crate::runtime::path::tcp) fn new(
        commands: ReliablePathCommandSender,
        stream_id: StreamId,
        attempt_id: ClientTcpOpenAttemptId,
    ) -> Self {
        Self {
            commands,
            stream_id,
            attempt_id,
            armed: true,
        }
    }

    pub(in crate::runtime::path::tcp) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ClientTcpOpenCancellation {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let commands = self.commands.clone();
        let stream_id = self.stream_id;
        let attempt_id = self.attempt_id;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = commands
                    .send_control(ReliablePathCommand::CancelTcpOpen {
                        stream_id,
                        attempt_id,
                    })
                    .await;
            });
        }
    }
}

pub(in crate::runtime::path::tcp) struct ClientTcpPathStreamState {
    pub(in crate::runtime::path::tcp) terminal: Option<crate::runtime::path::PendingStreamTerminal>,
    pub(in crate::runtime::path::tcp) open_attempt_id: ClientTcpOpenAttemptId,
    pub(in crate::runtime::path::tcp) frames: mpsc::Sender<Result<Frame, RuntimeError>>,
}

pub(in crate::runtime::path::tcp) struct ClientTcpPendingOpen {
    initial: Option<crate::runtime::path::InitialOpenBackend>,
    response: oneshot::Sender<ClientTcpOpenResponse>,
    frames: Option<mpsc::Receiver<Result<Frame, RuntimeError>>>,
    session_commands: ReliablePathCommandSender,
    lane: TrafficClass,
    open_deadline: tokio::time::Instant,
}

/// One owner for established stream routing and the disjoint pending-open
/// lifecycle. Pending values live only in `pending`; established stream states
/// therefore do not pay for a pending-response slot after MAX. Both deadline
/// and expiry scans visit pending records only, while their deadlines remain
/// live reads from the current initial-open arbitration state.
pub(super) struct ClientTcpPathStreams {
    streams: HashMap<StreamId, ClientTcpPathStreamState>,
    pending: BTreeMap<StreamId, ClientTcpPendingOpen>,
}

impl ClientTcpPathStreams {
    pub(super) fn new() -> Self {
        Self {
            streams: HashMap::new(),
            pending: BTreeMap::new(),
        }
    }

    #[cfg(test)]
    pub(super) fn insert_open(&mut self, stream_id: StreamId, state: ClientTcpPathStreamState) {
        assert!(
            !self.streams.contains_key(&stream_id) && !self.pending.contains_key(&stream_id),
            "TCP stream ID already has an actor-owned state"
        );
        self.streams.insert(stream_id, state);
    }

    pub(super) fn insert_pending(
        &mut self,
        stream_id: StreamId,
        state: ClientTcpPathStreamState,
        pending: ClientTcpPendingOpen,
    ) {
        assert!(
            !self.streams.contains_key(&stream_id) && !self.pending.contains_key(&stream_id),
            "TCP stream ID already has an actor-owned state"
        );
        self.streams.insert(stream_id, state);
        self.pending.insert(stream_id, pending);
    }

    pub(super) fn get_mut(
        &mut self,
        stream_id: &StreamId,
    ) -> Option<&mut ClientTcpPathStreamState> {
        self.streams.get_mut(stream_id)
    }

    pub(super) fn is_pending(&self, stream_id: &StreamId) -> bool {
        self.pending.contains_key(stream_id)
    }

    fn pending_open(&self, stream_id: &StreamId) -> Option<&ClientTcpPendingOpen> {
        self.pending.get(stream_id)
    }

    fn take_pending_open(&mut self, stream_id: &StreamId) -> Option<ClientTcpPendingOpen> {
        self.pending.remove(stream_id)
    }

    pub(super) fn remove(&mut self, stream_id: &StreamId) -> Option<ClientTcpPathStreamState> {
        self.remove_with_pending(stream_id)
            .map(|(state, _pending)| state)
    }

    fn remove_with_pending(
        &mut self,
        stream_id: &StreamId,
    ) -> Option<(ClientTcpPathStreamState, Option<ClientTcpPendingOpen>)> {
        let state = self.streams.remove(stream_id)?;
        let pending = self.pending.remove(stream_id);
        Some((state, pending))
    }

    fn drain_with_pending(
        &mut self,
    ) -> impl Iterator<
        Item = (
            StreamId,
            ClientTcpPathStreamState,
            Option<ClientTcpPendingOpen>,
        ),
    > + '_ {
        let mut pending = std::mem::take(&mut self.pending);
        self.streams
            .drain()
            .map(move |(stream_id, state)| (stream_id, state, pending.remove(&stream_id)))
    }
}

impl Deref for ClientTcpPathStreams {
    type Target = HashMap<StreamId, ClientTcpPathStreamState>;

    fn deref(&self) -> &Self::Target {
        &self.streams
    }
}

pub(in crate::runtime::path::tcp) struct ClientTcpOpenStreamRequest {
    pub(in crate::runtime::path::tcp) terminal: Option<crate::runtime::path::PendingStreamTerminal>,
    pub(in crate::runtime::path::tcp) initial: Option<crate::runtime::path::InitialOpenBackend>,
    pub(in crate::runtime::path::tcp) stream_id: StreamId,
    pub(in crate::runtime::path::tcp) attempt_id: ClientTcpOpenAttemptId,
    pub(in crate::runtime::path::tcp) target: TargetAddr,
    pub(in crate::runtime::path::tcp) lane: TrafficClass,
    pub(in crate::runtime::path::tcp) initial_demand: StreamDemandHint,
    pub(in crate::runtime::path::tcp) return_plan: StreamReturnPlan,
    pub(in crate::runtime::path::tcp) advertised_recv_max_offset: u64,
    pub(in crate::runtime::path::tcp) open_deadline: tokio::time::Instant,
    pub(in crate::runtime::path::tcp) session_commands: ReliablePathCommandSender,
    pub(in crate::runtime::path::tcp) response: oneshot::Sender<ClientTcpOpenResponse>,
}

pub(in crate::runtime::path::tcp) fn next_client_tcp_pending_open_deadline(
    streams: &ClientTcpPathStreams,
) -> Option<tokio::time::Instant> {
    let now = tokio::time::Instant::now();
    streams
        .pending
        .values()
        .map(|pending| {
            if pending.response.is_closed() {
                now
            } else {
                pending
                    .initial
                    .as_ref()
                    .map_or(pending.open_deadline, |initial| {
                        initial.deadline().unwrap_or(now)
                    })
            }
        })
        .min()
}

fn expired_client_tcp_pending_open_ids(streams: &ClientTcpPathStreams) -> Option<Vec<StreamId>> {
    let now = tokio::time::Instant::now();
    let expired = streams
        .pending
        .iter()
        .filter_map(|(stream_id, pending)| {
            let deadline = pending
                .initial
                .as_ref()
                .map_or(pending.open_deadline, |initial| {
                    initial.deadline().unwrap_or(now)
                });
            (pending.response.is_closed() || deadline <= now).then_some(*stream_id)
        })
        .collect::<Vec<_>>();
    (!expired.is_empty()).then_some(expired)
}

pub(in crate::runtime::path::tcp) async fn expire_client_tcp_pending_opens(
    connection: &mut ClientTcpPathConnection,
    streams: &mut ClientTcpPathStreams,
    closed_streams: &mut RecentIdCache<StreamId>,
) -> Result<(), RuntimeError> {
    let Some(expired) = expired_client_tcp_pending_open_ids(streams) else {
        return Ok(());
    };

    let mut detached = false;
    for stream_id in expired {
        if let Some((_state, Some(pending))) = streams.remove_with_pending(&stream_id) {
            let _ = pending
                .response
                .send(ClientTcpOpenResponse::FailedAfterOpen(
                    RuntimeError::PathOpenTimedOut,
                ));
        }
        closed_streams.insert(stream_id);
        let detach = Frame::StreamDetach { stream_id };
        connection.carrier.writer.write_frame(&detach).await?;
        connection.path_proofs.record_sent_frame(&detach);
        detached = true;
    }
    if detached {
        connection.carrier.writer.flush().await?;
    }
    Ok(())
}

/// A completed submission already has this exact actor-owned pending entry.
/// Deadline/arbitration rejection retires only that entry, not a healthy carrier.
async fn retire_client_tcp_pending_open(
    connection: &mut ClientTcpPathConnection,
    streams: &mut ClientTcpPathStreams,
    closed_streams: &mut RecentIdCache<StreamId>,
    stream_id: StreamId,
    error: RuntimeError,
) -> Result<(), RuntimeError> {
    if !streams.is_pending(&stream_id) {
        return Ok(());
    }
    let (_state, pending) = streams
        .remove_with_pending(&stream_id)
        .expect("pending TCP open has a stream state");
    let pending = pending.expect("checked pending TCP response");
    let _ = pending
        .response
        .send(ClientTcpOpenResponse::FailedAfterOpen(error));
    closed_streams.insert(stream_id);
    let detach = Frame::StreamDetach { stream_id };
    connection.carrier.writer.write_frame(&detach).await?;
    connection.path_proofs.record_sent_frame(&detach);
    connection.carrier.writer.flush().await?;
    Ok(())
}

pub(in crate::runtime::path::tcp) async fn open_client_tcp_stream_on_connection(
    connection: &mut ClientTcpPathConnection,
    open: ClientTcpOpenStreamRequest,
    streams: &mut ClientTcpPathStreams,
    closed_streams: &mut RecentIdCache<StreamId>,
    stream_frame_queue: usize,
) -> Result<(), RuntimeError> {
    let ClientTcpOpenStreamRequest {
        stream_id,
        terminal,
        initial,
        attempt_id,
        target,
        lane,
        initial_demand,
        return_plan,
        advertised_recv_max_offset,
        open_deadline,
        session_commands,
        response,
    } = open;
    if response.is_closed() || open_deadline <= tokio::time::Instant::now() {
        let _ = response.send(ClientTcpOpenResponse::RejectedWithoutOpen(
            RuntimeError::PathOpenTimedOut,
        ));
        return Ok(());
    }
    if streams.contains_key(&stream_id) {
        let _ = response.send(ClientTcpOpenResponse::RejectedWithoutOpen(
            RuntimeError::SenderServiceBlocked,
        ));
        return Ok(());
    }
    if let Some(initial) = &initial
        && let Err(error) = initial.bind(connection.path_instance_id)
    {
        let _ = response.send(ClientTcpOpenResponse::RejectedWithoutOpen(error));
        return Ok(());
    }
    let (frames_tx, frames_rx) = mpsc::channel(stream_frame_queue);
    streams.insert_pending(
        stream_id,
        ClientTcpPathStreamState {
            terminal,
            open_attempt_id: attempt_id,
            frames: frames_tx,
        },
        ClientTcpPendingOpen {
            initial: initial.clone(),
            response,
            frames: Some(frames_rx),
            session_commands,
            lane,
            open_deadline,
        },
    );
    let send_open = async {
        connection
            .carrier
            .writer
            .write_frame(&Frame::PathMetrics {
                metrics: connection.startup_metrics,
            })
            .await?;
        connection
            .carrier
            .writer
            .write_frame(&Frame::OpenStream {
                stream_id,
                target,
                demand: initial_demand,
                return_plan,
            })
            .await?;
        // Initial opens publish the logical receive owner's starting credit.
        // Attachments pass zero so accepting another carrier cannot widen the
        // one shared receive window.
        connection
            .carrier
            .writer
            .write_frame(&Frame::StreamMaxData {
                stream_id,
                max_offset: advertised_recv_max_offset,
            })
            .await?;
        connection.carrier.writer.flush().await
    };
    tokio::time::timeout_at(open_deadline, send_open)
        .await
        .map_err(|_| RuntimeError::PathOpenTimedOut)??;
    if let Some(initial) = initial
        && let Err(error) = initial.submitted()
    {
        retire_client_tcp_pending_open(connection, streams, closed_streams, stream_id, error)
            .await?;
        return Ok(());
    }
    Ok(())
}

pub(in crate::runtime::path::tcp) fn remove_matching_client_tcp_open(
    streams: &mut ClientTcpPathStreams,
    stream_id: StreamId,
    attempt_id: ClientTcpOpenAttemptId,
) -> Option<ClientTcpPathStreamState> {
    streams
        .get(&stream_id)
        .is_some_and(|state| state.open_attempt_id == attempt_id)
        .then(|| streams.remove(&stream_id))
        .flatten()
}

async fn route_client_tcp_stream_frame(
    streams: &mut ClientTcpPathStreams,
    closed_streams: &mut RecentIdCache<StreamId>,
    stream_id: StreamId,
    frame: Frame,
) -> Result<(), RuntimeError> {
    let Some(state) = streams.get_mut(&stream_id) else {
        #[cfg(feature = "lab-diagnostics")]
        let was_recently_closed = closed_streams.contains(&stream_id);
        closed_streams.insert(stream_id);
        #[cfg(feature = "lab-diagnostics")]
        if !was_recently_closed {
            lab_diagnostic(
                "client_tcp_unknown_stream_frame_drop",
                format_args!("stream_id={} frame_kind={}", stream_id.0, frame.kind_name(),),
            );
        }
        return Ok(());
    };
    #[cfg(feature = "lab-diagnostics")]
    let bytes = reliable_path_frame_pacing_bytes(&frame);
    #[cfg(feature = "lab-diagnostics")]
    let started = Instant::now();
    if state.frames.send(Ok(frame)).await.is_err() {
        streams.remove(&stream_id);
        closed_streams.insert(stream_id);
    }
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record("runtime.tcp_stream.route_frame", started.elapsed(), bytes);
    Ok(())
}

pub(in crate::runtime::path::tcp) fn client_tcp_inbound_frame_retires_attachment(
    frame: &Frame,
) -> bool {
    // FIN declares the final Data Sequence offset; repair below that offset may
    // still arrive on this carrier until the relay explicitly closes it.
    matches!(frame, Frame::StreamReset { .. })
}

async fn route_client_tcp_lifecycle_frame(
    streams: &mut ClientTcpPathStreams,
    closed_streams: &mut RecentIdCache<StreamId>,
    stream_id: StreamId,
    frame: Frame,
) -> Result<(), RuntimeError> {
    let retires_attachment = client_tcp_inbound_frame_retires_attachment(&frame);
    let result = route_client_tcp_stream_frame(streams, closed_streams, stream_id, frame).await;
    if result.is_ok() && retires_attachment {
        streams.remove(&stream_id);
        closed_streams.insert(stream_id);
    }
    result
}

async fn handle_client_tcp_stream_detach(
    streams: &mut ClientTcpPathStreams,
    closed_streams: &mut RecentIdCache<StreamId>,
    stream_id: StreamId,
) {
    let Some((state, pending)) = streams.remove_with_pending(&stream_id) else {
        closed_streams.insert(stream_id);
        return;
    };
    closed_streams.insert(stream_id);
    if let Some(pending) = pending {
        let _ = pending
            .response
            .send(ClientTcpOpenResponse::FailedAfterOpen(
                RuntimeError::ReliablePathAttachmentRefused,
            ));
    } else {
        // Preserve ordering behind any already-routed frames and explicitly
        // report retirement of this accepted attachment.
        let _ = state
            .frames
            .send(Err(RuntimeError::ReliablePathRetired))
            .await;
    }
}

pub(in crate::runtime::path::tcp) fn fail_client_tcp_streams(
    streams: &mut ClientTcpPathStreams,
    reason: &RuntimeError,
) {
    for (_, state, pending) in streams.drain_with_pending() {
        if let Some(pending) = pending {
            let _ = pending
                .response
                .send(ClientTcpOpenResponse::FailedAfterOpen(
                    tcp_path_stream_error(reason),
                ));
        } else {
            let _ = state.frames.try_send(Err(tcp_path_stream_error(reason)));
        }
    }
}

fn tcp_path_stream_error(reason: &RuntimeError) -> RuntimeError {
    match reason {
        RuntimeError::PathHeartbeatTimeout => RuntimeError::PathHeartbeatTimeout,
        RuntimeError::PathOpenTimedOut => RuntimeError::PathOpenTimedOut,
        RuntimeError::ReliablePathSessionClosed => RuntimeError::ReliablePathSessionClosed,
        RuntimeError::ReliablePathRetired => RuntimeError::ReliablePathRetired,
        RuntimeError::RemoteReset(reason) => RuntimeError::RemoteReset(*reason),
        RuntimeError::RemotePathClosed(reason) => RuntimeError::RemotePathClosed(*reason),
        RuntimeError::RemoteClosed(reason) => RuntimeError::RemoteClosed(*reason),
        RuntimeError::Protocol(message) => RuntimeError::Protocol(message),
        _ => RuntimeError::ReliablePathSessionClosed,
    }
}

pub(in crate::runtime::path::tcp) async fn handle_client_tcp_stream_frame(
    frame: Frame,
    connection: &mut ClientTcpPathConnection,
    streams: &mut ClientTcpPathStreams,
    closed_streams: &mut RecentIdCache<StreamId>,
    runtime: &ClientTcpPathSessionRuntime,
) -> Result<(), RuntimeError> {
    match frame {
        Frame::StreamMaxData {
            stream_id,
            max_offset,
        } => {
            let admitted_deadline = streams
                .pending_open(&stream_id)
                .and_then(|pending| pending.initial.as_ref())
                .map(|initial| initial.admission())
                .transpose();
            let admitted_deadline = match admitted_deadline {
                Ok(deadline) => deadline,
                Err(error) => {
                    retire_client_tcp_pending_open(
                        connection,
                        streams,
                        closed_streams,
                        stream_id,
                        error,
                    )
                    .await?;
                    return Ok(());
                }
            };
            if streams.is_pending(&stream_id) {
                let mut pending = streams
                    .take_pending_open(&stream_id)
                    .expect("checked pending TCP response");
                let open_deadline = admitted_deadline.unwrap_or(pending.open_deadline);
                let frames = pending
                    .frames
                    .take()
                    .ok_or(RuntimeError::Protocol("missing TCP stream frame receiver"))?;
                let evidence = runtime.attachment_evidence(connection);
                let native_window_observed_at = Instant::now();
                let native_window = crate::runtime::path::CarrierNativeWindowSample::new(
                    evidence.metrics.inflight_limit_bytes,
                    native_window_observed_at,
                    crate::model::timing::transport_rate_sample_freshness_horizon(
                        Duration::from_micros(u64::from(evidence.metrics.srtt_us.max(1))),
                        Duration::from_micros(u64::from(evidence.metrics.rttvar_us)),
                    ),
                );
                let state = streams
                    .get_mut(&stream_id)
                    .expect("pending TCP response has a stream state");
                let carrier = OpenedReliableCarrierStream {
                    retirement: None,
                    terminal: state.terminal.clone(),
                    terminal_owner: None,
                    stream_id,
                    path_instance_id: connection.path_instance_id,
                    max_offset,
                    lane: pending.lane,
                    underlay: UnderlayProtocol::Tcp,
                    max_frame_payload_bytes: reliable_relay_buffer_len(runtime.mux_limits),
                    portable_startup: connection.startup_snapshot,
                    startup: evidence.snapshot,
                    startup_native_window: native_window,
                    startup_metrics: Some(evidence.metrics),
                    commands: pending.session_commands,
                    mux_limits: runtime.mux_limits,
                    frames,
                };
                if pending
                    .response
                    .send(ClientTcpOpenResponse::Opened(ClientTcpOpenedStream {
                        carrier,
                        open_deadline,
                        path_metrics: evidence.metrics,
                    }))
                    .is_err()
                {
                    streams.remove(&stream_id);
                    closed_streams.insert(stream_id);
                    let detach = Frame::StreamDetach { stream_id };
                    connection.carrier.writer.write_frame(&detach).await?;
                    connection.path_proofs.record_sent_frame(&detach);
                    connection.carrier.writer.flush().await?;
                }
                return Ok(());
            }
            route_client_tcp_stream_frame(
                streams,
                closed_streams,
                stream_id,
                Frame::StreamMaxData {
                    stream_id,
                    max_offset,
                },
            )
            .await
        }
        Frame::StreamReset { stream_id, reason } => {
            if let Some(terminal) = streams
                .get(&stream_id)
                .and_then(|state| state.terminal.as_ref())
            {
                terminal.publish_reset(stream_id, reason);
            }
            if streams.is_pending(&stream_id)
                && let Some((_state, Some(pending))) = streams.remove_with_pending(&stream_id)
            {
                closed_streams.insert(stream_id);
                let _ = pending
                    .response
                    .send(ClientTcpOpenResponse::FailedAfterOpen(
                        RuntimeError::RemoteReset(reason),
                    ));
                return Ok(());
            }
            route_client_tcp_lifecycle_frame(
                streams,
                closed_streams,
                stream_id,
                Frame::StreamReset { stream_id, reason },
            )
            .await
        }
        Frame::StreamData {
            stream_id,
            offset,
            payload,
        } => {
            route_client_tcp_stream_frame(
                streams,
                closed_streams,
                stream_id,
                Frame::StreamData {
                    stream_id,
                    offset,
                    payload,
                },
            )
            .await
        }
        Frame::StreamAck {
            stream_id,
            scope_start,
            ranges,
        } => {
            route_client_tcp_stream_frame(
                streams,
                closed_streams,
                stream_id,
                Frame::StreamAck {
                    stream_id,
                    scope_start,
                    ranges,
                },
            )
            .await
        }
        frame @ (Frame::StreamFeedbackProbe { stream_id, .. }
        | Frame::StreamFeedbackReceipt { stream_id, .. }) => {
            route_client_tcp_stream_frame(streams, closed_streams, stream_id, frame).await
        }
        Frame::StreamRequalifyData {
            stream_id,
            probe_id,
            offset,
            payload,
        } => {
            route_client_tcp_stream_frame(
                streams,
                closed_streams,
                stream_id,
                Frame::StreamRequalifyData {
                    stream_id,
                    probe_id,
                    offset,
                    payload,
                },
            )
            .await
        }
        Frame::StreamRequalifyAck {
            stream_id,
            probe_id,
            offset,
            payload_bytes,
        } => {
            route_client_tcp_stream_frame(
                streams,
                closed_streams,
                stream_id,
                Frame::StreamRequalifyAck {
                    stream_id,
                    probe_id,
                    offset,
                    payload_bytes,
                },
            )
            .await
        }
        Frame::StreamFin {
            stream_id,
            final_offset,
        } => {
            route_client_tcp_lifecycle_frame(
                streams,
                closed_streams,
                stream_id,
                Frame::StreamFin {
                    stream_id,
                    final_offset,
                },
            )
            .await
        }
        Frame::StreamDetach { stream_id } => {
            handle_client_tcp_stream_detach(streams, closed_streams, stream_id).await;
            Ok(())
        }
        _ => Err(RuntimeError::Protocol(
            "unexpected TCP product stream frame",
        )),
    }
}

#[cfg(test)]
#[path = "tests_stream.rs"]
mod tests;
