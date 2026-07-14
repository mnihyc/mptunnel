use super::*;
use crate::protocol::path_capacity::CapacityReceiveTracker;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

// Ownership boundary:
// This module owns TCP underlay carrier sessions only: encrypted framed TCP
// connection setup, command queues, heartbeat/liveness, frame reads, and writer
// shutdown. Product reliable stream identity, response path-flight ownership,
// and cross-carrier scheduling live in `reliable_path`.

pub(super) struct ClientTcpPathSessionHandle {
    runtime: ClientTcpPathSessionRuntime,
    commands: Arc<Mutex<Option<ClientTcpPathSessionSlot>>>,
    latency_commands: Arc<Mutex<Option<ClientTcpPathSessionSlot>>>,
}

#[derive(Clone)]
struct ClientTcpPathSessionSlot {
    commands: ReliablePathCommandSender,
    carrier_generation: Arc<AtomicU64>,
}

struct ClientTcpCarrierGeneration {
    published: Arc<AtomicU64>,
    current: u64,
}

pub(super) struct ClientTcpOpenCancellation {
    commands: ReliablePathCommandSender,
    stream_id: StreamId,
    attempt_id: ClientTcpOpenAttemptId,
    armed: bool,
}

static NEXT_CLIENT_TCP_OPEN_ATTEMPT_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_CLIENT_TCP_CARRIER_GENERATION: AtomicU64 = AtomicU64::new(1);

impl ClientTcpCarrierGeneration {
    fn new(published: Arc<AtomicU64>) -> Self {
        published.store(0, Ordering::Release);
        Self {
            published,
            current: 0,
        }
    }

    fn establish(&mut self) {
        let mut generation = NEXT_CLIENT_TCP_CARRIER_GENERATION.fetch_add(1, Ordering::Relaxed);
        if generation == 0 {
            generation = NEXT_CLIENT_TCP_CARRIER_GENERATION.fetch_add(1, Ordering::Relaxed);
        }
        self.current = generation;
        self.published.store(generation, Ordering::Release);
    }

    fn clear(&mut self) {
        self.published.store(0, Ordering::Release);
        self.current = 0;
    }
}

impl Drop for ClientTcpCarrierGeneration {
    fn drop(&mut self) {
        self.clear();
    }
}

impl ClientTcpOpenCancellation {
    pub(super) fn new(
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

    fn disarm(&mut self) {
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

impl std::fmt::Debug for ClientTcpPathSessionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientTcpPathSessionHandle")
            .finish_non_exhaustive()
    }
}

impl Clone for ClientTcpPathSessionHandle {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            commands: self.commands.clone(),
            latency_commands: self.latency_commands.clone(),
        }
    }
}

impl ClientTcpPathSessionHandle {
    pub(super) fn new(runtime: ClientTcpPathSessionRuntime) -> Self {
        Self {
            runtime,
            commands: Arc::new(Mutex::new(None)),
            latency_commands: Arc::new(Mutex::new(None)),
        }
    }

    #[cfg(test)]
    pub(super) fn session_id(&self) -> SessionId {
        self.runtime.session_id
    }

    #[cfg(test)]
    pub(super) fn carrier_generation(&self, lane: FlowLane) -> u64 {
        let lane = if tcp_path_lane_uses_latency_session(lane) {
            &self.latency_commands
        } else {
            &self.commands
        };
        lane.lock()
            .expect("TCP path session lock")
            .as_ref()
            .map_or(0, |session| {
                session.carrier_generation.load(Ordering::Acquire)
            })
    }

    #[cfg(test)]
    pub(super) async fn open_stream(
        &self,
        stream_id: StreamId,
        target: TargetAddr,
        ingress: IngressKind,
        lane: FlowLane,
        role: StreamOpenRole,
        open_deadline: tokio::time::Instant,
    ) -> Result<ReliablePathStream, RuntimeError> {
        self.open_stream_with_deadlines(
            stream_id,
            target,
            ingress,
            lane,
            role,
            ClientTcpOpenDeadlines::fixed(open_deadline),
        )
        .await
        .map(|opened| opened.stream)
    }

    pub(super) async fn open_stream_with_deadlines(
        &self,
        stream_id: StreamId,
        target: TargetAddr,
        ingress: IngressKind,
        lane: FlowLane,
        role: StreamOpenRole,
        open_deadlines: ClientTcpOpenDeadlines,
    ) -> Result<ClientTcpOpenedStream, RuntimeError> {
        let session = self.ensure_session_slot(lane);
        let commands = session.commands.clone();
        let observed_carrier_generation = session.carrier_generation.load(Ordering::Acquire);
        let (response_tx, response_rx) = oneshot::channel();
        let attempt_id =
            ClientTcpOpenAttemptId(NEXT_CLIENT_TCP_OPEN_ATTEMPT_ID.fetch_add(1, Ordering::Relaxed));
        let wait_for_deadline = || async {
            let first_deadline = if observed_carrier_generation == 0 {
                open_deadlines.setup
            } else {
                open_deadlines.live
            };
            tokio::time::sleep_until(first_deadline).await;
            if observed_carrier_generation != 0
                && session.carrier_generation.load(Ordering::Acquire) != observed_carrier_generation
            {
                tokio::time::sleep_until(open_deadlines.setup).await;
            }
        };
        tokio::select! {
            biased;
            result = commands.send_control(ReliablePathCommand::OpenStream {
                stream_id,
                attempt_id,
                observed_carrier_generation,
                target,
                ingress,
                lane,
                role,
                open_deadlines,
                session_commands: commands.clone(),
                response: response_tx,
            }) => result.map_err(|_| RuntimeError::ReliablePathSessionClosed)?,
            _ = wait_for_deadline() => return Err(RuntimeError::PathOpenTimedOut),
        }
        // Tokio mpsc send is cancellation-safe. Arm cleanup only after the
        // actor owns this exact generation, never for an unqueued attempt.
        let mut cancellation =
            ClientTcpOpenCancellation::new(commands.clone(), stream_id, attempt_id);
        let response = tokio::select! {
            biased;
            response = response_rx => response.map_err(|_| RuntimeError::ReliablePathSessionClosed)?,
            _ = wait_for_deadline() => return Err(RuntimeError::PathOpenTimedOut),
        };
        match response {
            ClientTcpOpenResponse::Opened(opened) => {
                cancellation.disarm();
                Ok(opened)
            }
            ClientTcpOpenResponse::RejectedWithoutOpen(err) => {
                cancellation.disarm();
                Err(err)
            }
            ClientTcpOpenResponse::FailedAfterOpen(err) => Err(err),
        }
    }

    #[cfg(test)]
    pub(super) fn ensure_session(&self, lane: FlowLane) -> ReliablePathCommandSender {
        self.ensure_session_slot(lane).commands
    }

    fn ensure_session_slot(&self, lane: FlowLane) -> ClientTcpPathSessionSlot {
        let lane = if tcp_path_lane_uses_latency_session(lane) {
            &self.latency_commands
        } else {
            &self.commands
        };
        let mut current = lane.lock().expect("TCP path session lock");
        if let Some(session) = current.as_ref()
            && !session.commands.is_closed()
        {
            return session.clone();
        }

        let (commands, receivers) = reliable_path_command_channels(self.runtime.command_queue);
        let carrier_generation = Arc::new(AtomicU64::new(0));
        tokio::spawn(run_client_tcp_path_session(
            self.runtime.clone(),
            receivers,
            carrier_generation.clone(),
        ));
        let session = ClientTcpPathSessionSlot {
            commands,
            carrier_generation,
        };
        *current = Some(session.clone());
        session
    }
}

pub(super) fn tcp_path_lane_uses_latency_session(lane: FlowLane) -> bool {
    matches!(
        lane,
        FlowLane::Control | FlowLane::Latency | FlowLane::RealtimeDatagram
    )
}

pub(super) struct ClientTcpPathConnection {
    pub(super) startup_snapshot: PathSnapshot,
    pub(super) startup_metrics: PathMetrics,
    pub(super) writer: EncryptedTcpWriter,
    pub(super) frames: mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>>,
    pub(super) heartbeat_interval: Duration,
    pub(super) next_heartbeat_at: tokio::time::Instant,
    pub(super) pending_heartbeat: Option<(u64, tokio::time::Instant)>,
    pub(super) path_proofs: PathProofTracker,
    #[cfg(target_os = "linux")]
    pub(super) tcp_metrics: Option<TcpMetricPublisher>,
    pub(super) request_tcp_capacity_probe: Option<PendingClientTcpCapacityProbe>,
    discarded_request_tcp_capacity_receipt: Option<DiscardedClientTcpCapacityReceipt>,
    pub(super) capacity_receive: CapacityReceiveTracker,
}

pub(super) struct PendingClientTcpCapacityProbe {
    probe: TcpCapacityProbeCommand,
    measurement: ClientTcpCapacityProbeMeasurement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ClientTcpCapacityProbeMeasurement {
    proof_started_at: Instant,
    train_wire_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientTcpCapacityProbeWriteOutcome {
    NoWire,
    Measured(ClientTcpCapacityProbeMeasurement),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DiscardedClientTcpCapacityReceipt {
    calibration_id: u64,
    train_payload_bytes: u64,
}

impl DiscardedClientTcpCapacityReceipt {
    fn from_probe(probe: &TcpCapacityProbeCommand) -> Self {
        Self {
            calibration_id: probe.calibration_id,
            train_payload_bytes: probe.train_payload_bytes,
        }
    }

    fn matches(self, calibration_id: u64, received_payload_bytes: u64) -> bool {
        self.calibration_id == calibration_id && self.train_payload_bytes == received_payload_bytes
    }
}

pub(super) type EncryptedTcpReader = EncryptedFramedReader<tokio::io::ReadHalf<TcpStream>>;
pub(super) type EncryptedTcpWriter = EncryptedFramedWriter<tokio::io::WriteHalf<TcpStream>>;

pub(super) struct ClientTcpPathStreamState {
    pub(super) open_attempt_id: ClientTcpOpenAttemptId,
    pub(super) frames: mpsc::Sender<Result<Frame, RuntimeError>>,
    pub(super) pending_open: Option<ClientTcpPendingOpen>,
    pub(super) local_close_pending: bool,
}

pub(super) struct ClientTcpPendingOpen {
    response: oneshot::Sender<ClientTcpOpenResponse>,
    frames: Option<mpsc::Receiver<Result<Frame, RuntimeError>>>,
    session_commands: ReliablePathCommandSender,
    lane: FlowLane,
    open_deadline: tokio::time::Instant,
}

#[derive(Clone)]
pub(super) struct ClientTcpPathSessionRuntime {
    pub(super) path: PathSpec,
    pub(super) path_index: usize,
    pub(super) session_id: SessionId,
    pub(super) security: SecurityConfig,
    pub(super) codec_limits: CodecLimits,
    pub(super) mux_limits: MuxLimits,
    pub(super) command_queue: usize,
    pub(super) stream_frame_queue: usize,
    pub(super) closed_stream_cache_capacity: usize,
    pub(super) health: Arc<Mutex<ClientPathHealth>>,
}

struct ClientTcpPathSessionState {
    connection: Option<ClientTcpPathConnection>,
    streams: HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: RecentIdCache<StreamId>,
}

struct ClientTcpOpenStreamRequest {
    stream_id: StreamId,
    attempt_id: ClientTcpOpenAttemptId,
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
    role: StreamOpenRole,
    open_deadline: tokio::time::Instant,
    session_commands: ReliablePathCommandSender,
    response: oneshot::Sender<ClientTcpOpenResponse>,
}

async fn run_client_tcp_path_session(
    runtime: ClientTcpPathSessionRuntime,
    mut commands: ReliablePathCommandReceivers,
    carrier_generation: Arc<AtomicU64>,
) {
    let mut carrier_generation = ClientTcpCarrierGeneration::new(carrier_generation);
    let mut state = ClientTcpPathSessionState {
        connection: None,
        streams: HashMap::new(),
        closed_streams: RecentIdCache::new(runtime.closed_stream_cache_capacity),
    };
    let mut pending_frames = Vec::<Frame>::new();

    loop {
        if state.connection.is_none() {
            match recv_reliable_path_command(&mut commands).await {
                Some(command) => {
                    let pending_bytes = reliable_path_command_pending_bytes(&command);
                    handle_disconnected_client_tcp_command(command, &runtime, &mut state).await;
                    if state.connection.is_some() {
                        carrier_generation.establish();
                    }
                    commands.release_pending_command_bytes(pending_bytes);
                }
                None => return,
            }
            continue;
        }

        let heartbeat_at = {
            let connection_ref = state
                .connection
                .as_ref()
                .expect("checked connected TCP path session");
            connection_ref
                .pending_heartbeat
                .as_ref()
                .map(|(_, deadline)| *deadline)
                .unwrap_or(connection_ref.next_heartbeat_at)
        };
        let heartbeat_timer = tokio::time::sleep_until(heartbeat_at);
        tokio::pin!(heartbeat_timer);
        let pending_open_deadline = next_client_tcp_pending_open_deadline(&state.streams);
        let pending_open_timer =
            tokio::time::sleep_until(pending_open_deadline.unwrap_or(heartbeat_at));
        tokio::pin!(pending_open_timer);

        let receivers_open = !reliable_path_receivers_closed(&commands);
        if !receivers_open {
            if let Some(connection_ref) = state.connection.as_mut() {
                let _ = close_client_tcp_path(
                    connection_ref,
                    PathId(runtime.path_index as u16),
                    !state.streams.is_empty(),
                )
                .await;
            }
            return;
        }
        let request_probe_deadline = state
            .connection
            .as_ref()
            .and_then(|connection| connection.request_tcp_capacity_probe.as_ref())
            .map(|pending| tokio::time::Instant::from_std(pending.probe.expires_at));
        let request_probe_timer =
            tokio::time::sleep_until(request_probe_deadline.unwrap_or(heartbeat_at));
        tokio::pin!(request_probe_timer);
        let request_probe_lease = state
            .connection
            .as_ref()
            .and_then(|connection| connection.request_tcp_capacity_probe.as_ref())
            .and_then(|pending| pending.probe.request_lease())
            .cloned();
        let request_probe_cancelled = async move {
            if let Some(lease) = request_probe_lease {
                lease.cancelled().await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::pin!(request_probe_cancelled);
        let request_probe_pending = request_probe_deadline.is_some();
        let command_may_recv = receivers_open && !request_probe_pending;

        let mut drop_connection = false;
        tokio::select! {
            biased;
            _ = &mut request_probe_cancelled, if request_probe_pending => {
                discard_pending_client_tcp_capacity_receipt(
                    state.connection.as_mut().expect("checked connected TCP path session"),
                );
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_tcp_capacity_probe",
                    format_args!("phase=discarded reason=cancelled_after_finish path_index={}", runtime.path_index),
                );
            }
            _ = &mut request_probe_timer, if request_probe_pending => {
                discard_pending_client_tcp_capacity_receipt(
                    state.connection.as_mut().expect("checked connected TCP path session"),
                );
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_tcp_capacity_probe",
                    format_args!("phase=discarded reason=receipt_timeout_after_finish path_index={}", runtime.path_index),
                );
            }
            _ = &mut pending_open_timer, if pending_open_deadline.is_some() => {
                if let Err(err) = expire_client_tcp_pending_opens(
                    state.connection.as_mut().expect("checked connected TCP path session"),
                    &mut state.streams,
                    &mut state.closed_streams,
                    runtime.mux_limits,
                ).await {
                    carrier_generation.clear();
                    fail_client_tcp_streams(&mut state.streams, &err);
                    eprintln!("warning: TCP pending stream cleanup failed: {err}");
                    drop_connection = true;
                }
            }
            frame = state.connection.as_mut().expect("checked connected TCP path session").frames.recv() => {
                match frame {
                    Some(Ok(frame)) => {
                        if let Err(err) = handle_client_tcp_path_frame(
                            frame,
                            state.connection.as_mut().expect("checked connected TCP path session"),
                            &mut state.streams,
                            &mut state.closed_streams,
                            &runtime,
                        )
                        .await
                        {
                            carrier_generation.clear();
                            fail_client_tcp_streams(&mut state.streams, &err);
                            eprintln!("warning: TCP path session frame handling failed: {err}");
                            drop_connection = true;
                        } else if command_may_recv
                            && let Some(command) = try_recv_reliable_path_command(&mut commands)
                        {
                            let result = handle_connected_client_tcp_command_run(
                                command,
                                &mut commands,
                                state
                                    .connection
                                    .as_mut()
                                    .expect("checked connected TCP path session"),
                                &mut state.streams,
                                &mut state.closed_streams,
                                &runtime,
                                carrier_generation.current,
                                runtime.stream_frame_queue,
                                runtime.mux_limits,
                                &mut pending_frames,
                            )
                            .await;
                            if let Err(err) = result {
                                carrier_generation.clear();
                                fail_client_tcp_streams(&mut state.streams, &err);
                                eprintln!("warning: TCP path session command failed: {err}");
                                drop_connection = true;
                            }
                        }
                    }
                    Some(Err(err)) => {
                        let err = RuntimeError::Encrypted(err);
                        carrier_generation.clear();
                        fail_client_tcp_streams(&mut state.streams, &err);
                        eprintln!("warning: TCP path session read failed: {err}");
                        drop_connection = true;
                    }
                    None => {
                        let err = RuntimeError::ReliablePathSessionClosed;
                        carrier_generation.clear();
                        fail_client_tcp_streams(&mut state.streams, &err);
                        drop_connection = true;
                    }
                }
            }
            command = recv_reliable_path_command(&mut commands), if command_may_recv => {
                match command {
                    Some(command) => {
                        let result = handle_connected_client_tcp_command_run(
                            command,
                            &mut commands,
                            state.connection.as_mut().expect("checked connected TCP path session"),
                            &mut state.streams,
                            &mut state.closed_streams,
                            &runtime,
                            carrier_generation.current,
                            runtime.stream_frame_queue,
                            runtime.mux_limits,
                            &mut pending_frames,
                        )
                        .await;
                        if let Err(err) = result {
                            carrier_generation.clear();
                            fail_client_tcp_streams(&mut state.streams, &err);
                            eprintln!("warning: TCP path session command failed: {err}");
                            drop_connection = true;
                        }
                    }
                    None => {
                        if reliable_path_receivers_closed(&commands) {
                            if let Some(connection_ref) = state.connection.as_mut() {
                                let _ = close_client_tcp_path(
                                    connection_ref,
                                    PathId(runtime.path_index as u16),
                                    !state.streams.is_empty(),
                                )
                                .await;
                            }
                            return;
                        }
                    }
                }
            }
            _ = &mut heartbeat_timer, if !request_probe_pending => {
                if let Err(err) = tick_client_tcp_path_heartbeat(
                    state.connection.as_mut().expect("checked connected TCP path session"),
                    runtime.mux_limits,
                    !state.streams.is_empty(),
                )
                .await
                {
                    carrier_generation.clear();
                    fail_client_tcp_streams(&mut state.streams, &err);
                    eprintln!("warning: TCP path heartbeat failed: {err}");
                    drop_connection = true;
                }
            }
        }

        if drop_connection {
            carrier_generation.clear();
            state.connection = None;
        }
    }
}

fn discard_pending_client_tcp_capacity_receipt(connection: &mut ClientTcpPathConnection) {
    let Some(pending) = connection.request_tcp_capacity_probe.take() else {
        return;
    };
    connection.discarded_request_tcp_capacity_receipt = Some(
        DiscardedClientTcpCapacityReceipt::from_probe(&pending.probe),
    );
}

fn next_client_tcp_pending_open_deadline(
    streams: &HashMap<StreamId, ClientTcpPathStreamState>,
) -> Option<tokio::time::Instant> {
    let now = tokio::time::Instant::now();
    streams
        .values()
        .filter_map(|state| state.pending_open.as_ref())
        .map(|pending| {
            if pending.response.is_closed() {
                now
            } else {
                pending.open_deadline
            }
        })
        .min()
}

async fn expire_client_tcp_pending_opens(
    connection: &mut ClientTcpPathConnection,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    mux_limits: MuxLimits,
) -> Result<(), RuntimeError> {
    let now = tokio::time::Instant::now();
    let expired = streams
        .iter()
        .filter_map(|(stream_id, state)| {
            state.pending_open.as_ref().and_then(|pending| {
                (pending.response.is_closed() || pending.open_deadline <= now).then_some(*stream_id)
            })
        })
        .collect::<Vec<_>>();
    if expired.is_empty() {
        return Ok(());
    }

    let mut detached = false;
    for stream_id in expired {
        if let Some(mut state) = streams.remove(&stream_id)
            && let Some(pending) = state.pending_open.take()
        {
            let _ = pending
                .response
                .send(ClientTcpOpenResponse::FailedAfterOpen(
                    RuntimeError::PathOpenTimedOut,
                ));
        }
        closed_streams.insert(stream_id);
        let detach = Frame::StreamDetach { stream_id };
        connection.writer.write_frame(&detach).await?;
        connection.path_proofs.record_sent_frame(&detach);
        detached = true;
    }
    if detached {
        connection.writer.flush().await?;
        record_client_tcp_path_outbound_activity(connection, mux_limits);
    }
    Ok(())
}

async fn handle_connected_client_tcp_command_run(
    first_command: ReliablePathCommand,
    commands: &mut ReliablePathCommandReceivers,
    connection: &mut ClientTcpPathConnection,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    runtime: &ClientTcpPathSessionRuntime,
    carrier_generation: u64,
    stream_frame_queue: usize,
    mux_limits: MuxLimits,
    pending_frames: &mut Vec<Frame>,
) -> Result<(), RuntimeError> {
    #[cfg(feature = "lab-diagnostics")]
    let drain_started = Instant::now();
    let byte_budget = reliable_path_command_writer_run_budget_bytes(mux_limits);
    let item_budget = reliable_path_command_writer_run_budget_items(mux_limits);
    let mut next_command = Some(first_command);
    pending_frames.clear();
    let mut sent_bytes = 0usize;
    let mut sent_items = 0usize;
    let mut wrote_frame = false;

    loop {
        let Some(command) = next_command
            .take()
            .or_else(|| try_recv_reliable_path_command(commands))
        else {
            if try_coalesce_reliable_path_writer_run(
                commands,
                &mut next_command,
                sent_items,
                sent_bytes,
                byte_budget,
                item_budget,
            )
            .await
            {
                continue;
            }
            break;
        };
        let pending_bytes = reliable_path_command_pending_bytes(&command);
        let writer_run_bytes = reliable_path_command_writer_run_bytes(&command);
        match command {
            ReliablePathCommand::SendFrame(frame)
                if reliable_path_frame_requires_capacity_command(&frame) =>
            {
                commands.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "client TCP path received an untyped capacity frame",
                ));
            }
            ReliablePathCommand::SendFrame(frame) => {
                let is_stream_detach = matches!(&frame, Frame::StreamDetach { .. });
                #[cfg(feature = "lab-diagnostics")]
                if let Frame::StreamAck {
                    stream_id,
                    complete,
                    ranges,
                } = &frame
                {
                    lab_diagnostic(
                        "client_tcp_stream_ack_dequeue",
                        format_args!(
                            "stream_id={} path_index={} complete={} ranges={} frontier={} largest_end={} pending_bytes_after={}",
                            stream_id.0,
                            runtime.path_index,
                            complete,
                            ranges.len(),
                            stream_ack_contiguous_frontier(*complete, ranges),
                            ranges.last().map_or(0, |range| range.end),
                            commands.pending_bytes(),
                        ),
                    );
                }
                pending_frames.push(frame);
                commands.release_pending_command_bytes(pending_bytes);
                wrote_frame = true;
                sent_bytes = sent_bytes.saturating_add(writer_run_bytes);
                sent_items = sent_items.saturating_add(1);
                if is_stream_detach || sent_bytes >= byte_budget || sent_items >= item_budget {
                    break;
                }
                continue;
            }
            ReliablePathCommand::SendQuicCapacityProbe(probe) => {
                probe.ticket.cancel();
                commands.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "client TCP path received QUIC capacity command",
                ));
            }
            ReliablePathCommand::SendTcpCapacityProbe(probe) => {
                let TcpCapacityProbeOwner::Request {
                    stream_id,
                    path_instance,
                } = probe.owner
                else {
                    commands.release_pending_command_bytes(pending_bytes);
                    return Err(RuntimeError::Protocol(
                        "client TCP path received response capacity command",
                    ));
                };
                let request_current = probe
                    .request_lease()
                    .is_some_and(RequestTcpCapacityProbeLease::is_current);
                let stream_is_attached = streams
                    .get(&stream_id)
                    .is_some_and(|state| state.pending_open.is_none());
                if !request_current || !stream_is_attached {
                    // A planner may revoke a queued probe after the stream or
                    // proof epoch changes, or the product stream may detach
                    // before dequeue. With no carrier bytes, both are normal
                    // canceled transactions rather than shared-path failures.
                    if let Some(lease) = probe.request_lease() {
                        lease.refund_if_unwritten();
                    }
                    commands.release_pending_command_bytes(pending_bytes);
                    return Ok(());
                }
                if probe.path_id != PathId(runtime.path_index as u16)
                    || path_instance.key.underlay != UnderlayProtocol::Tcp
                    || path_instance.key.index != runtime.path_index
                    || probe.train_payload_bytes < probe.sample_floor_bytes
                    || probe.train_payload_bytes
                        > reliable_quic_capacity_calibration_session_limit_bytes(mux_limits)
                    || !probe.valid_request_tcp_train()
                    || connection.request_tcp_capacity_probe.is_some()
                {
                    commands.release_pending_command_bytes(pending_bytes);
                    return Err(RuntimeError::Protocol(
                        "request TCP capacity command does not match its writer",
                    ));
                }
                if let Err(error) = flush_client_tcp_frame_batch(
                    connection,
                    pending_frames,
                    streams,
                    closed_streams,
                    runtime,
                )
                .await
                {
                    commands.release_pending_command_bytes(pending_bytes);
                    return Err(error);
                }
                if Instant::now() >= probe.expires_at {
                    if let Some(lease) = probe.request_lease() {
                        lease.refund_if_unwritten();
                    }
                    commands.release_pending_command_bytes(pending_bytes);
                    return Ok(());
                }
                let measurement_result = client_write_tcp_capacity_probe_interlocked(
                    connection,
                    &probe,
                    mux_limits.max_payload_bytes,
                    streams,
                    closed_streams,
                    mux_limits,
                )
                .await;
                commands.release_pending_command_bytes(pending_bytes);
                let (write_outcome, deferred_frames) = measurement_result?;
                record_client_tcp_path_outbound_activity(connection, mux_limits);
                match write_outcome {
                    ClientTcpCapacityProbeWriteOutcome::NoWire => {
                        if let Some(lease) = probe.request_lease() {
                            lease.refund_if_unwritten();
                        }
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "request_tcp_capacity_probe",
                            format_args!(
                                "phase=discarded reason=no_wire stream_id={} path_index={} instance_id={} calibration_id={}",
                                stream_id.0,
                                runtime.path_index,
                                path_instance.id,
                                probe.calibration_id,
                            ),
                        );
                    }
                    ClientTcpCapacityProbeWriteOutcome::Measured(measurement) => {
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "request_tcp_capacity_probe",
                            format_args!(
                                "phase=sent stream_id={} path_index={} instance_id={} calibration_id={} train_bytes={} train_wire_bytes={} sample_floor_bytes={} warmup_bytes={} timing_slack_bytes={} required_timed_bytes={}",
                                stream_id.0,
                                runtime.path_index,
                                path_instance.id,
                                probe.calibration_id,
                                probe.train_payload_bytes,
                                measurement.train_wire_bytes,
                                probe.sample_floor_bytes,
                                probe.warmup_carrier_bytes,
                                probe.timing_slack_bytes,
                                probe.required_timed_carrier_bytes,
                            ),
                        );
                        connection.request_tcp_capacity_probe =
                            Some(PendingClientTcpCapacityProbe { probe, measurement });
                    }
                }
                for frame in deferred_frames {
                    handle_client_tcp_path_frame(
                        frame,
                        connection,
                        streams,
                        closed_streams,
                        runtime,
                    )
                    .await?;
                }
                return Ok(());
            }
            command => {
                flush_client_tcp_frame_batch(
                    connection,
                    pending_frames,
                    streams,
                    closed_streams,
                    runtime,
                )
                .await?;
                handle_connected_client_tcp_command(
                    command,
                    connection,
                    streams,
                    closed_streams,
                    carrier_generation,
                    stream_frame_queue,
                    mux_limits,
                    false,
                )
                .await?;
                commands.release_pending_command_bytes(pending_bytes);
            }
        }
        sent_items = sent_items.saturating_add(1);
        if sent_bytes >= byte_budget || sent_items >= item_budget {
            break;
        }
    }

    flush_client_tcp_frame_batch(connection, pending_frames, streams, closed_streams, runtime)
        .await?;

    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "path_writer_drain",
        format_args!(
            "role=client underlay=Tcp sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
            sent_items,
            sent_bytes,
            byte_budget,
            item_budget,
            commands.pending_bytes(),
            drain_started.elapsed().as_micros(),
            sent_bytes >= byte_budget,
            sent_items >= item_budget,
        ),
    );
    if wrote_frame {
        connection.writer.flush().await?;
    }
    Ok(())
}

async fn flush_client_tcp_frame_batch(
    connection: &mut ClientTcpPathConnection,
    frames: &mut Vec<Frame>,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    runtime: &ClientTcpPathSessionRuntime,
) -> Result<(), RuntimeError> {
    if frames.is_empty() {
        return Ok(());
    }
    let mut deferred_frame = None;
    let mut routed_frames = 0usize;
    {
        let write = connection.writer.write_frames(frames);
        tokio::pin!(write);
        loop {
            tokio::select! {
                biased;
                result = &mut write => {
                    result?;
                    break;
                }
                incoming = connection.frames.recv(), if deferred_frame.is_none() => {
                    match incoming {
                        Some(Ok(frame)) => {
                            match try_route_client_tcp_stream_frame_during_write(
                                frame,
                                streams,
                                closed_streams,
                            ) {
                                ClientTcpWriteFrameRoute::Routed => {
                                    routed_frames = routed_frames.saturating_add(1);
                                }
                                ClientTcpWriteFrameRoute::Barrier(frame) => {
                                    deferred_frame = Some(frame);
                                }
                            }
                        }
                        Some(Err(err)) => return Err(RuntimeError::Encrypted(err)),
                        None => return Err(RuntimeError::ReliablePathSessionClosed),
                    }
                }
            }
        }
    }
    #[cfg(feature = "lab-diagnostics")]
    for frame in frames.iter() {
        if let Frame::StreamAck {
            stream_id,
            complete,
            ranges,
        } = frame
        {
            lab_diagnostic(
                "client_tcp_stream_ack_write_complete",
                format_args!(
                    "stream_id={} path_index={} complete={} ranges={} frontier={} largest_end={}",
                    stream_id.0,
                    runtime.path_index,
                    complete,
                    ranges.len(),
                    stream_ack_contiguous_frontier(*complete, ranges),
                    ranges.last().map_or(0, |range| range.end),
                ),
            );
        }
    }
    for frame in frames.iter() {
        connection.path_proofs.record_sent_frame(frame);
    }
    frames.clear();
    record_client_tcp_path_outbound_activity(connection, runtime.mux_limits);
    #[cfg(feature = "lab-diagnostics")]
    if routed_frames > 0 || deferred_frame.is_some() {
        lab_diagnostic(
            "client_tcp_write_feedback_interlock",
            format_args!(
                "path_index={} routed_frames={} deferred_frames={}",
                runtime.path_index,
                routed_frames,
                usize::from(deferred_frame.is_some()),
            ),
        );
    }
    if let Some(frame) = deferred_frame {
        handle_client_tcp_path_frame(frame, connection, streams, closed_streams, runtime).await?;
    }
    Ok(())
}

async fn client_write_tcp_capacity_probe_interlocked(
    connection: &mut ClientTcpPathConnection,
    probe: &TcpCapacityProbeCommand,
    max_payload_bytes: usize,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    mux_limits: MuxLimits,
) -> Result<(ClientTcpCapacityProbeWriteOutcome, Vec<Frame>), RuntimeError> {
    let Some(metrics) = connection.tcp_metrics.as_ref() else {
        return Ok((ClientTcpCapacityProbeWriteOutcome::NoWire, Vec::new()));
    };
    let write =
        client_write_tcp_capacity_probe(&mut connection.writer, metrics, probe, max_payload_bytes);
    tokio::pin!(write);
    let deferred_limit = reliable_path_writer_frame_queue(mux_limits).max(1);
    let mut deferred_frames = Vec::new();
    let mut defer_all = false;
    let mut routed_frames = 0usize;
    let mut deferred_error = None;
    let mut reader_open = true;
    let measurement = loop {
        tokio::select! {
            biased;
            result = &mut write => {
                break result;
            }
            incoming = connection.frames.recv(), if reader_open => {
                let frame = match incoming {
                    Some(Ok(frame)) => frame,
                    Some(Err(error)) => {
                        deferred_error.get_or_insert(RuntimeError::Encrypted(error));
                        reader_open = false;
                        continue;
                    }
                    None => {
                        deferred_error.get_or_insert(RuntimeError::ReliablePathSessionClosed);
                        reader_open = false;
                        continue;
                    }
                };
                if deferred_error.is_some() {
                    continue;
                }
                if !defer_all {
                    match try_route_client_tcp_stream_frame_during_write(
                        frame,
                        streams,
                        closed_streams,
                    ) {
                        ClientTcpWriteFrameRoute::Routed => {
                            routed_frames = routed_frames.saturating_add(1);
                            continue;
                        }
                        ClientTcpWriteFrameRoute::Barrier(frame) => {
                            defer_all = true;
                            deferred_frames.push(frame);
                        }
                    }
                } else if deferred_frames.len() < deferred_limit {
                    deferred_frames.push(frame);
                } else {
                    // Continue draining so the peer can read the in-progress
                    // probe, then fail the carrier and let reliable repair own
                    // any product frames that could not remain ordered here.
                    deferred_error.get_or_insert(RuntimeError::Protocol(
                        "request TCP capacity feedback interlock overflowed",
                    ));
                    deferred_frames.clear();
                }
            }
        }
    }?;
    if let Some(error) = deferred_error {
        return Err(error);
    }
    #[cfg(feature = "lab-diagnostics")]
    if routed_frames > 0 || !deferred_frames.is_empty() {
        lab_diagnostic(
            "request_tcp_capacity_feedback_interlock",
            format_args!(
                "routed_frames={} deferred_frames={}",
                routed_frames,
                deferred_frames.len(),
            ),
        );
    }
    Ok((measurement, deferred_frames))
}

async fn client_write_tcp_capacity_probe(
    writer: &mut EncryptedTcpWriter,
    metrics: &TcpMetricPublisher,
    probe: &TcpCapacityProbeCommand,
    max_payload_bytes: usize,
) -> Result<ClientTcpCapacityProbeWriteOutcome, RuntimeError> {
    writer.flush().await?;
    let Some(baseline_expires_at) = probe.baseline_expires_at else {
        return Ok(ClientTcpCapacityProbeWriteOutcome::NoWire);
    };
    #[cfg(feature = "lab-diagnostics")]
    let baseline_wait_started_at = Instant::now();
    let (proof_started_at, _baseline) =
        match wait_for_client_tcp_write_queue_drain(metrics, baseline_expires_at).await {
            Ok(baseline) => baseline,
            Err(_) => return Ok(ClientTcpCapacityProbeWriteOutcome::NoWire),
        };
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "request_tcp_capacity_probe",
        format_args!(
            "phase=write_queue_drained path_id={} calibration_id={} wait_ms={} unacked_packets={} notsent_bytes={}",
            probe.path_id.0,
            probe.calibration_id,
            baseline_wait_started_at.elapsed().as_millis(),
            _baseline.unacked_packets,
            _baseline.notsent_bytes,
        ),
    );
    let Some(write_expires_at) = probe.write_expires_at else {
        return Ok(ClientTcpCapacityProbeWriteOutcome::NoWire);
    };
    if Instant::now() >= write_expires_at {
        return Ok(ClientTcpCapacityProbeWriteOutcome::NoWire);
    }
    let writer_wire_baseline = writer.wire_bytes_written();
    // A short cumulative-ACK tail is not delivery-rate authority: delayed or
    // compressed ACKs can make a healthy 100-500 Mbps path look multi-gigabit.
    // The full typed train and its receiver receipt form the conservative seed;
    // same-socket TCP_INFO stays diagnostic until product ACKs replace the seed.
    let write_result =
        tokio::time::timeout_at(tokio::time::Instant::from_std(probe.expires_at), async {
            client_write_tcp_capacity_payload(
                writer,
                probe,
                probe.train_payload_bytes,
                max_payload_bytes,
            )
            .await?;
            let train_wire_bytes = writer
                .wire_bytes_written()
                .checked_sub(writer_wire_baseline)
                .filter(|bytes| *bytes > 0)
                .ok_or(RuntimeError::Protocol(
                    "request TCP capacity wire counter moved backwards",
                ))?;
            writer
                .write_frame(&Frame::PathCapacityFinish {
                    path_id: probe.path_id,
                    calibration_id: probe.calibration_id,
                    payload_bytes: probe.train_payload_bytes,
                })
                .await?;
            writer.flush().await?;
            Ok::<u64, RuntimeError>(train_wire_bytes)
        })
        .await;
    match write_result {
        Ok(Ok(train_wire_bytes)) => Ok(ClientTcpCapacityProbeWriteOutcome::Measured(
            ClientTcpCapacityProbeMeasurement {
                proof_started_at,
                train_wire_bytes,
            },
        )),
        Ok(Err(error)) => {
            if writer.wire_bytes_written() == writer_wire_baseline {
                Ok(ClientTcpCapacityProbeWriteOutcome::NoWire)
            } else {
                Err(error)
            }
        }
        Err(_) => {
            if writer.wire_bytes_written() == writer_wire_baseline {
                Ok(ClientTcpCapacityProbeWriteOutcome::NoWire)
            } else {
                Err(RuntimeError::Protocol(
                    "request TCP capacity probe timed out after a partial train",
                ))
            }
        }
    }
}

async fn client_write_tcp_capacity_payload(
    writer: &mut EncryptedTcpWriter,
    probe: &TcpCapacityProbeCommand,
    payload_bytes: u64,
    max_payload_bytes: usize,
) -> Result<(), RuntimeError> {
    let frame_payload_bytes = max_payload_bytes.max(1) as u64;
    let mut remaining = payload_bytes;
    while remaining > 0 {
        // Dequeue-time validation owns the transaction once the first record
        // can hit the wire. Later logical cancellation suppresses publication,
        // but interrupting a multi-record encrypted epoch would kill the path.
        let payload_bytes = remaining.min(frame_payload_bytes) as usize;
        writer
            .write_frame(&Frame::PathCapacityData {
                path_id: probe.path_id,
                calibration_id: probe.calibration_id,
                payload: Bytes::from(vec![0u8; payload_bytes]),
            })
            .await?;
        remaining = remaining.saturating_sub(payload_bytes as u64);
    }
    Ok(())
}

async fn wait_for_client_tcp_write_queue_drain(
    metrics: &TcpMetricPublisher,
    expires_at: Instant,
) -> Result<(Instant, TcpSenderQueueSnapshot), RuntimeError> {
    loop {
        // Start before getsockopt so receipt timing cannot omit syscall time.
        let observed_at = Instant::now();
        let snapshot = metrics
            .sender_queue_snapshot()
            .ok_or(RuntimeError::Protocol(
                "request TCP capacity ACK snapshot is unavailable",
            ))?;
        if snapshot.is_write_queue_drained() {
            return Ok((observed_at, snapshot));
        }
        let now = Instant::now();
        if now >= expires_at {
            return Err(RuntimeError::Protocol(
                "request TCP capacity sender write queue did not drain",
            ));
        }
        tokio::time::sleep(
            TRANSPORT_TIMER_GRANULARITY.min(expires_at.saturating_duration_since(now)),
        )
        .await;
    }
}

pub(super) enum ClientTcpWriteFrameRoute {
    Routed,
    Barrier(Frame),
}

pub(super) fn try_route_client_tcp_stream_frame_during_write(
    frame: Frame,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
) -> ClientTcpWriteFrameRoute {
    let stream_id = match &frame {
        Frame::StreamMaxData { stream_id, .. }
        | Frame::StreamReset { stream_id, .. }
        | Frame::StreamData { stream_id, .. }
        | Frame::StreamAck { stream_id, .. }
        | Frame::StreamFin { stream_id, .. } => *stream_id,
        Frame::StreamDetach { stream_id } => {
            if streams
                .get(stream_id)
                .is_some_and(|state| state.pending_open.is_some())
            {
                return ClientTcpWriteFrameRoute::Barrier(Frame::StreamDetach {
                    stream_id: *stream_id,
                });
            }
            streams.remove(stream_id);
            closed_streams.insert(*stream_id);
            return ClientTcpWriteFrameRoute::Routed;
        }
        _ => return ClientTcpWriteFrameRoute::Barrier(frame),
    };
    if streams
        .get(&stream_id)
        .is_some_and(|state| state.pending_open.is_some())
    {
        return ClientTcpWriteFrameRoute::Barrier(frame);
    }
    let closes_stream = matches!(&frame, Frame::StreamReset { .. } | Frame::StreamFin { .. });
    let Some(state) = streams.get_mut(&stream_id) else {
        closed_streams.insert(stream_id);
        return ClientTcpWriteFrameRoute::Routed;
    };
    let send_result = state.frames.try_send(Ok(frame));
    match send_result {
        Ok(()) => {
            if closes_stream {
                streams.remove(&stream_id);
                closed_streams.insert(stream_id);
            }
            ClientTcpWriteFrameRoute::Routed
        }
        Err(mpsc::error::TrySendError::Full(Ok(frame))) => ClientTcpWriteFrameRoute::Barrier(frame),
        Err(mpsc::error::TrySendError::Closed(_)) => {
            streams.remove(&stream_id);
            closed_streams.insert(stream_id);
            ClientTcpWriteFrameRoute::Routed
        }
        Err(mpsc::error::TrySendError::Full(Err(_))) => {
            unreachable!("client TCP interlock only routes successful frames")
        }
    }
}

async fn handle_disconnected_client_tcp_command(
    command: ReliablePathCommand,
    runtime: &ClientTcpPathSessionRuntime,
    state: &mut ClientTcpPathSessionState,
) {
    match command {
        ReliablePathCommand::OpenStream {
            stream_id,
            attempt_id,
            observed_carrier_generation: _,
            target,
            ingress,
            lane,
            role,
            open_deadlines,
            session_commands,
            mut response,
        } => {
            let open_deadline = open_deadlines.setup;
            if response.is_closed() || open_deadline <= tokio::time::Instant::now() {
                let _ = response.send(ClientTcpOpenResponse::RejectedWithoutOpen(
                    RuntimeError::PathOpenTimedOut,
                ));
                return;
            }
            let connect = connect_client_tcp_path(
                &runtime.path,
                runtime.path_index,
                runtime.session_id,
                &runtime.security,
                runtime.codec_limits,
                runtime.mux_limits,
                open_deadline,
            );
            tokio::pin!(connect);
            let connect_result = tokio::select! {
                biased;
                _ = response.closed() => return,
                result = &mut connect => result,
            };
            match connect_result {
                Ok(mut connected) => {
                    if response.is_closed() || open_deadline <= tokio::time::Instant::now() {
                        let _ = response.send(ClientTcpOpenResponse::RejectedWithoutOpen(
                            RuntimeError::PathOpenTimedOut,
                        ));
                        return;
                    }
                    let open = ClientTcpOpenStreamRequest {
                        stream_id,
                        attempt_id,
                        target,
                        ingress,
                        lane,
                        role,
                        open_deadline,
                        session_commands,
                        response,
                    };
                    let result = open_client_tcp_stream_on_connection(
                        &mut connected,
                        open,
                        &mut state.streams,
                        runtime.stream_frame_queue,
                    )
                    .await;
                    if result.is_ok() {
                        state.connection = Some(connected);
                    } else if let Err(err) = result {
                        eprintln!(
                            "warning: reliable stream open on new path session failed: {err}"
                        );
                        fail_client_tcp_streams(&mut state.streams, &err);
                    }
                }
                Err(err) => {
                    let _ = response.send(ClientTcpOpenResponse::RejectedWithoutOpen(err));
                }
            }
        }
        ReliablePathCommand::SendQuicCapacityProbe(probe) => {
            probe.ticket.cancel();
        }
        ReliablePathCommand::SendTcpCapacityProbe(_) => {}
        ReliablePathCommand::CancelTcpOpen { .. }
        | ReliablePathCommand::SendFrame(_)
        | ReliablePathCommand::CloseStream(_) => {}
    }
}

async fn handle_connected_client_tcp_command(
    command: ReliablePathCommand,
    connection: &mut ClientTcpPathConnection,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    carrier_generation: u64,
    stream_frame_queue: usize,
    mux_limits: MuxLimits,
    flush_after_frame: bool,
) -> Result<(), RuntimeError> {
    match command {
        ReliablePathCommand::OpenStream {
            stream_id,
            attempt_id,
            observed_carrier_generation,
            target,
            ingress,
            lane,
            role,
            open_deadlines,
            session_commands,
            response,
        } => {
            let open_deadline = open_deadlines
                .for_carrier_generation(observed_carrier_generation, carrier_generation);
            let open = ClientTcpOpenStreamRequest {
                stream_id,
                attempt_id,
                target,
                ingress,
                lane,
                role,
                open_deadline,
                session_commands,
                response,
            };
            open_client_tcp_stream_on_connection(connection, open, streams, stream_frame_queue)
                .await?;
            record_client_tcp_path_outbound_activity(connection, mux_limits);
            Ok(())
        }
        ReliablePathCommand::CancelTcpOpen {
            stream_id,
            attempt_id,
        } => {
            if remove_matching_client_tcp_open(streams, stream_id, attempt_id).is_none() {
                return Ok(());
            }
            closed_streams.insert(stream_id);
            let detach = Frame::StreamDetach { stream_id };
            connection.writer.write_frame(&detach).await?;
            connection.path_proofs.record_sent_frame(&detach);
            connection.writer.flush().await?;
            record_client_tcp_path_outbound_activity(connection, mux_limits);
            Ok(())
        }
        ReliablePathCommand::SendFrame(frame)
            if reliable_path_frame_requires_capacity_command(&frame) =>
        {
            Err(RuntimeError::Protocol(
                "client TCP path received an untyped capacity frame",
            ))
        }
        ReliablePathCommand::SendFrame(frame) => {
            connection.writer.write_frame(&frame).await?;
            connection.path_proofs.record_sent_frame(&frame);
            if flush_after_frame {
                connection.writer.flush().await?;
            }
            record_client_tcp_path_outbound_activity(connection, mux_limits);
            Ok(())
        }
        ReliablePathCommand::SendQuicCapacityProbe(probe) => {
            probe.ticket.cancel();
            Err(RuntimeError::Protocol(
                "client TCP path received QUIC capacity command",
            ))
        }
        ReliablePathCommand::SendTcpCapacityProbe(_) => Err(RuntimeError::Protocol(
            "client TCP path received server capacity command",
        )),
        ReliablePathCommand::CloseStream(stream_id) => {
            streams.remove(&stream_id);
            closed_streams.insert(stream_id);
            Ok(())
        }
    }
}

pub(super) async fn connect_client_tcp_path(
    path: &PathSpec,
    path_index: usize,
    session_id: SessionId,
    security: &SecurityConfig,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    open_deadline: tokio::time::Instant,
) -> Result<ClientTcpPathConnection, RuntimeError> {
    let connect = async {
        let connect_timeout = open_deadline.saturating_duration_since(tokio::time::Instant::now());
        let tcp_stream = tcp::connect_path(
            path,
            TcpConnectOptions {
                timeout: connect_timeout,
                ..TcpConnectOptions::default()
            },
        )
        .await?;
        #[cfg(target_os = "linux")]
        let mut tcp_metrics = TcpMetricPublisher::capture(&tcp_stream);
        let mut framed = EncryptedFramedStream::with_cipher_suite(
            tcp_stream,
            security.secret.as_bytes(),
            PeerRole::Client,
            codec_limits,
            security.cipher,
        )?;
        let path_id = PathId(path_index as u16);
        let (session_hello, session_auth, path_join) = authenticated_path_join_frames_for_session(
            security,
            path,
            path_id,
            UnderlayProtocol::Tcp,
            session_id,
        )?;

        framed
            .write_frames(&[session_hello, session_auth, path_join])
            .await?;
        framed.flush().await?;

        let mut session_ready = false;
        let mut path_active = false;
        while !session_ready || !path_active {
            match framed.read_frame().await? {
                Frame::SessionReady => session_ready = true,
                Frame::PathStatus {
                    status: crate::protocol::PathStatus::Active,
                    ..
                } => path_active = true,
                Frame::PathStatus { .. } => {
                    return Err(RuntimeError::Protocol(
                        "TCP path session did not become active",
                    ));
                }
                Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
                _ => {
                    return Err(RuntimeError::Protocol(
                        "unexpected TCP path handshake frame",
                    ));
                }
            }
        }

        #[cfg(target_os = "linux")]
        if let Some(metrics) = tcp_metrics.as_mut() {
            metrics.begin_epoch();
        }

        let (reader, writer) = framed.split()?;
        let now = tokio::time::Instant::now();
        let startup_snapshot = path_startup_snapshot(path, path_index);
        let startup_metrics =
            path_startup_metrics(path, path_index, PathMetricDirection::ClientToServer);
        Ok(ClientTcpPathConnection {
            startup_snapshot,
            startup_metrics,
            writer,
            frames: spawn_encrypted_tcp_reader(
                reader,
                reliable_path_writer_frame_queue(mux_limits),
            ),
            heartbeat_interval: mux_limits.tcp_path_heartbeat_interval,
            next_heartbeat_at: now + mux_limits.tcp_path_heartbeat_interval,
            pending_heartbeat: None,
            path_proofs: PathProofTracker::default(),
            #[cfg(target_os = "linux")]
            tcp_metrics,
            request_tcp_capacity_probe: None,
            discarded_request_tcp_capacity_receipt: None,
            capacity_receive: CapacityReceiveTracker::new(
                reliable_quic_capacity_calibration_session_limit_bytes(mux_limits),
            ),
        })
    };
    tokio::time::timeout_at(open_deadline, connect)
        .await
        .map_err(|_| RuntimeError::PathOpenTimedOut)?
}

async fn open_client_tcp_stream_on_connection(
    connection: &mut ClientTcpPathConnection,
    open: ClientTcpOpenStreamRequest,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    stream_frame_queue: usize,
) -> Result<(), RuntimeError> {
    let ClientTcpOpenStreamRequest {
        stream_id,
        attempt_id,
        target,
        ingress,
        lane,
        role,
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
    let (frames_tx, frames_rx) = mpsc::channel(stream_frame_queue);
    streams.insert(
        stream_id,
        ClientTcpPathStreamState {
            open_attempt_id: attempt_id,
            frames: frames_tx,
            pending_open: Some(ClientTcpPendingOpen {
                response,
                frames: Some(frames_rx),
                session_commands,
                lane,
                open_deadline,
            }),
            local_close_pending: false,
        },
    );
    let send_open = async {
        connection
            .writer
            .write_frame(&Frame::PathMetrics {
                metrics: connection.startup_metrics,
            })
            .await?;
        connection
            .writer
            .write_frame(&Frame::OpenStream {
                stream_id,
                target,
                ingress,
                outbound: OutboundPolicy::Direct,
                demand: stream_demand_hint_for_lane(lane),
                role,
            })
            .await?;
        connection.writer.flush().await
    };
    tokio::time::timeout_at(open_deadline, send_open)
        .await
        .map_err(|_| RuntimeError::PathOpenTimedOut)??;
    connection.next_heartbeat_at = tokio::time::Instant::now() + connection.heartbeat_interval;
    Ok(())
}

pub(super) fn remove_matching_client_tcp_open(
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    stream_id: StreamId,
    attempt_id: ClientTcpOpenAttemptId,
) -> Option<ClientTcpPathStreamState> {
    streams
        .get(&stream_id)
        .is_some_and(|state| state.open_attempt_id == attempt_id)
        .then(|| streams.remove(&stream_id))
        .flatten()
}

async fn handle_client_tcp_path_frame(
    frame: Frame,
    connection: &mut ClientTcpPathConnection,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    runtime: &ClientTcpPathSessionRuntime,
) -> Result<(), RuntimeError> {
    refresh_client_tcp_path_liveness(connection, runtime.mux_limits);
    expire_client_tcp_pending_opens(connection, streams, closed_streams, runtime.mux_limits)
        .await?;
    let path_id = PathId(runtime.path_index as u16);
    match frame {
        Frame::StreamMaxData {
            stream_id,
            max_offset,
        } => {
            if let Some(state) = streams.get_mut(&stream_id)
                && state.pending_open.is_some()
            {
                if state.local_close_pending {
                    return Ok(());
                }
                if let Some(mut pending) = state.pending_open.take() {
                    let open_deadline = pending.open_deadline;
                    let frames = pending
                        .frames
                        .take()
                        .ok_or(RuntimeError::Protocol("missing TCP stream frame receiver"))?;
                    let stream = ReliablePathStream {
                        stream_id,
                        max_offset,
                        lane: pending.lane,
                        underlay: UnderlayProtocol::Tcp,
                        max_frame_payload_bytes: reliable_relay_buffer_len(runtime.mux_limits),
                        output: ReliablePathStreamOutput::fixed_with_snapshot(
                            connection.startup_snapshot,
                            pending.session_commands,
                            runtime.mux_limits,
                        ),
                        frames,
                    };
                    if pending
                        .response
                        .send(ClientTcpOpenResponse::Opened(ClientTcpOpenedStream {
                            stream,
                            open_deadline,
                        }))
                        .is_err()
                    {
                        streams.remove(&stream_id);
                        closed_streams.insert(stream_id);
                        let detach = Frame::StreamDetach { stream_id };
                        connection.writer.write_frame(&detach).await?;
                        connection.path_proofs.record_sent_frame(&detach);
                        connection.writer.flush().await?;
                        record_client_tcp_path_outbound_activity(connection, runtime.mux_limits);
                    }
                    return Ok(());
                }
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
            if streams
                .get(&stream_id)
                .is_some_and(|state| state.pending_open.is_some())
                && let Some(mut state) = streams.remove(&stream_id)
                && let Some(pending) = state.pending_open.take()
            {
                closed_streams.insert(stream_id);
                let _ = pending
                    .response
                    .send(ClientTcpOpenResponse::FailedAfterOpen(
                        RuntimeError::RemoteReset(reason),
                    ));
                return Ok(());
            }
            let result = route_client_tcp_stream_frame(
                streams,
                closed_streams,
                stream_id,
                Frame::StreamReset { stream_id, reason },
            )
            .await;
            if result.is_ok() {
                streams.remove(&stream_id);
                closed_streams.insert(stream_id);
            }
            result
        }
        Frame::StreamData {
            stream_id,
            offset,
            flags,
            payload,
        } => {
            route_client_tcp_stream_frame(
                streams,
                closed_streams,
                stream_id,
                Frame::StreamData {
                    stream_id,
                    offset,
                    flags,
                    payload,
                },
            )
            .await
        }
        Frame::StreamAck {
            stream_id,
            complete,
            ranges,
        } => {
            route_client_tcp_stream_frame(
                streams,
                closed_streams,
                stream_id,
                Frame::StreamAck {
                    stream_id,
                    complete,
                    ranges,
                },
            )
            .await
        }
        Frame::StreamFin {
            stream_id,
            final_offset,
        } => {
            let result = route_client_tcp_stream_frame(
                streams,
                closed_streams,
                stream_id,
                Frame::StreamFin {
                    stream_id,
                    final_offset,
                },
            )
            .await;
            if result.is_ok() {
                streams.remove(&stream_id);
                closed_streams.insert(stream_id);
            }
            result
        }
        Frame::StreamDetach { stream_id } => {
            streams.remove(&stream_id);
            closed_streams.insert(stream_id);
            Ok(())
        }
        Frame::Ping { nonce } => {
            connection
                .writer
                .write_frame(&Frame::Pong { nonce })
                .await?;
            connection.writer.flush().await?;
            Ok(())
        }
        Frame::PathProofData {
            path_id: proof_path_id,
            proof_id,
            payload,
        } if proof_path_id == path_id => {
            connection
                .writer
                .write_frame(&path_proof_ack_frame(path_id, proof_id, payload.len()))
                .await?;
            connection.writer.flush().await?;
            Ok(())
        }
        Frame::PathProofAck {
            path_id: proof_path_id,
            proof_id,
            payload_bytes,
        } if proof_path_id == path_id => {
            if let Some(observation) =
                connection
                    .path_proofs
                    .acknowledge(path_id, proof_id, payload_bytes)
            {
                let transport_state = connection.tcp_metrics.as_mut().and_then(|publisher| {
                    publisher.maybe_observe(path_id, PathMetricDirection::ClientToServer, true)
                });
                if let Some(record) = runtime
                    .health
                    .lock()
                    .expect("client path health lock")
                    .tcp
                    .get_mut(runtime.path_index)
                {
                    record.mark_path_proof_success(observation);
                    if let Some(metrics) = transport_state {
                        record.mark_tcp_transport_state(metrics);
                    }
                }
            }
            Ok(())
        }
        Frame::PathCapacityData {
            path_id: capacity_path_id,
            calibration_id,
            payload,
        } if capacity_path_id == path_id => connection
            .capacity_receive
            .record_data(calibration_id, payload.len())
            .map_err(Into::into),
        Frame::PathCapacityFinish {
            path_id: capacity_path_id,
            calibration_id,
            payload_bytes,
        } if capacity_path_id == path_id => {
            let received_payload_bytes = connection
                .capacity_receive
                .finish(calibration_id, payload_bytes)?;
            connection
                .writer
                .write_frame(&Frame::PathCapacityReceipt {
                    path_id,
                    calibration_id,
                    received_payload_bytes,
                })
                .await?;
            connection.writer.flush().await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "tcp_capacity_receipt",
                format_args!(
                    "role=client phase=sent path_id={} calibration_id={} received_payload_bytes={}",
                    path_id.0, calibration_id, received_payload_bytes,
                ),
            );
            Ok(())
        }
        Frame::PathCapacityReceipt {
            path_id: receipt_path_id,
            calibration_id,
            received_payload_bytes,
        } if receipt_path_id == path_id => {
            let Some(pending) = connection.request_tcp_capacity_probe.take() else {
                if connection
                    .discarded_request_tcp_capacity_receipt
                    .is_some_and(|discarded| {
                        discarded.matches(calibration_id, received_payload_bytes)
                    })
                {
                    connection.discarded_request_tcp_capacity_receipt = None;
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "request_tcp_capacity_probe",
                        format_args!(
                            "phase=discarded reason=matched_late_receipt path_index={} calibration_id={} train_bytes={}",
                            runtime.path_index, calibration_id, received_payload_bytes,
                        ),
                    );
                    return Ok(());
                }
                return Err(RuntimeError::Protocol(
                    "request TCP capacity receipt has no active epoch",
                ));
            };
            let TcpCapacityProbeOwner::Request {
                stream_id,
                path_instance,
            } = pending.probe.owner
            else {
                return Err(RuntimeError::Protocol(
                    "client TCP capacity receipt has response ownership",
                ));
            };
            if pending.probe.calibration_id != calibration_id
                || pending.probe.train_payload_bytes != received_payload_bytes
                || path_instance.key.underlay != UnderlayProtocol::Tcp
                || path_instance.key.index != runtime.path_index
            {
                return Err(RuntimeError::Protocol(
                    "request TCP capacity receipt does not match active epoch",
                ));
            }
            if Instant::now() >= pending.probe.expires_at
                || !pending
                    .probe
                    .request_lease()
                    .is_some_and(RequestTcpCapacityProbeLease::is_current)
            {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_tcp_capacity_probe",
                    format_args!(
                        "phase=discarded reason=stale_matching_receipt path_index={} calibration_id={}",
                        runtime.path_index, calibration_id,
                    ),
                );
                return Ok(());
            }
            #[cfg(target_os = "linux")]
            {
                let elapsed = pending.measurement.proof_started_at.elapsed();
                let Some(receipt_rate_bps) =
                    tcp_capacity_receipt_rate_bps(received_payload_bytes, elapsed)
                else {
                    return Ok(());
                };
                let Some(mut metrics) = connection.tcp_metrics.as_mut().and_then(|publisher| {
                    publisher.maybe_observe(path_id, PathMetricDirection::ClientToServer, true)
                }) else {
                    return Ok(());
                };
                #[cfg(feature = "lab-diagnostics")]
                let kernel_delivery_rate_bps = metrics.delivery_rate_bps;
                #[cfg(feature = "lab-diagnostics")]
                let kernel_pacing_rate_bps = metrics.pacing_rate_bps;
                // Cold request trains can be smaller than the real path BDP, so
                // native delivery remains diagnostic here. The full typed receipt
                // is the seed; product ACK evidence replaces it after handoff.
                let rate_bps = receipt_rate_bps;
                metrics.delivery_rate_bps = rate_bps;
                metrics.pacing_rate_bps = rate_bps;
                metrics.has_ack_derived_data_sample = true;
                metrics.data_sample_count = metrics.data_sample_count.max(1);
                metrics.data_sample_bytes = metrics.data_sample_bytes.max(received_payload_bytes);
                metrics.confidence_ppm = 1_000_000;
                let accepted_at = Instant::now();
                let validity = tcp_capacity_proof_validity(metrics);
                let candidate = TcpCapacityProofCandidate {
                    token: calibration_id,
                    train_bytes: pending.probe.train_payload_bytes,
                    received_bytes: received_payload_bytes,
                    rate_sample_bytes: received_payload_bytes,
                    proof_elapsed: elapsed,
                    receipt_rate_bps,
                    rate_bps,
                    accepted_at,
                    expires_at: accepted_at.checked_add(validity).unwrap_or(accepted_at),
                };
                let accepted = runtime
                    .health
                    .lock()
                    .expect("client path health lock")
                    .tcp
                    .get_mut(runtime.path_index)
                    .is_some_and(|record| {
                        record.accept_request_tcp_capacity_proof(
                            stream_id,
                            path_instance,
                            candidate,
                            metrics,
                            accepted_at,
                        )
                    });
                if !accepted {
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "request_tcp_capacity_probe",
                        format_args!(
                            "phase=discarded reason=proof_publication_rejected path_index={} calibration_id={}",
                            runtime.path_index, calibration_id,
                        ),
                    );
                    return Ok(());
                }
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_tcp_capacity_probe",
                    format_args!(
                        "phase=confirmed stream_id={} path_index={} instance_id={} calibration_id={} train_bytes={} train_wire_bytes={} receipt_elapsed_ms={} receipt_rate_mbps={:.3} published_rate_mbps={:.3} kernel_delivery_rate_mbps={:.3} kernel_pacing_rate_mbps={:.3} srtt_ms={:.3}",
                        stream_id.0,
                        runtime.path_index,
                        path_instance.id,
                        calibration_id,
                        received_payload_bytes,
                        pending.measurement.train_wire_bytes,
                        elapsed.as_millis(),
                        receipt_rate_bps as f64 / 1_000_000.0,
                        rate_bps as f64 / 1_000_000.0,
                        kernel_delivery_rate_bps as f64 / 1_000_000.0,
                        kernel_pacing_rate_bps as f64 / 1_000_000.0,
                        metrics.srtt_us as f64 / 1_000.0,
                    ),
                );
            }
            #[cfg(not(target_os = "linux"))]
            return Ok(());
            Ok(())
        }
        Frame::Pong { nonce } => {
            let Some((pending_nonce, _)) = connection.pending_heartbeat.as_ref() else {
                return Err(RuntimeError::Protocol(
                    "unexpected TCP path heartbeat response",
                ));
            };
            if *pending_nonce != nonce {
                return Err(RuntimeError::Protocol(
                    "unexpected TCP path heartbeat response",
                ));
            }
            connection.pending_heartbeat = None;
            connection.next_heartbeat_at =
                tokio::time::Instant::now() + connection.heartbeat_interval;
            Ok(())
        }
        Frame::PathStatus {
            status: crate::protocol::PathStatus::Draining | crate::protocol::PathStatus::Failed,
            ..
        } => Err(RuntimeError::ReliablePathSessionClosed),
        Frame::PathStatus { .. } => Ok(()),
        Frame::SessionClose { reason } => Err(RuntimeError::RemoteClosed(reason)),
        Frame::PathDrain { .. } | Frame::PathClose { .. } => {
            Err(RuntimeError::ReliablePathSessionClosed)
        }
        _ => Err(RuntimeError::Protocol("unexpected TCP path session frame")),
    }
}

pub(super) fn refresh_client_tcp_path_liveness(
    connection: &mut ClientTcpPathConnection,
    mux_limits: MuxLimits,
) {
    refresh_client_tcp_path_liveness_state(
        &mut connection.next_heartbeat_at,
        connection.heartbeat_interval,
        &mut connection.pending_heartbeat,
        mux_limits.tcp_path_heartbeat_timeout,
    );
}

fn record_client_tcp_path_outbound_activity(
    connection: &mut ClientTcpPathConnection,
    mux_limits: MuxLimits,
) {
    refresh_client_tcp_path_liveness(connection, mux_limits);
}

pub(super) fn refresh_client_tcp_path_liveness_state(
    next_heartbeat_at: &mut tokio::time::Instant,
    heartbeat_interval: Duration,
    pending_heartbeat: &mut Option<(u64, tokio::time::Instant)>,
    heartbeat_timeout: Duration,
) {
    let now = tokio::time::Instant::now();
    *next_heartbeat_at = now + heartbeat_interval;
    if let Some((_, deadline)) = pending_heartbeat.as_mut() {
        *deadline = now + heartbeat_timeout;
    }
}

pub(super) async fn route_client_tcp_stream_frame(
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
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
                format_args!(
                    "stream_id={} frame_kind={}",
                    stream_id.0,
                    frame_kind_name(&frame),
                ),
            );
        }
        return Ok(());
    };
    #[cfg(feature = "lab-diagnostics")]
    let bytes = frame_pacing_bytes(&frame);
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

pub(super) async fn tick_client_tcp_path_heartbeat(
    connection: &mut ClientTcpPathConnection,
    mux_limits: MuxLimits,
    has_active_streams: bool,
) -> Result<(), RuntimeError> {
    let now = tokio::time::Instant::now();
    if let Some((_, deadline)) = connection.pending_heartbeat.as_ref()
        && now >= *deadline
    {
        if has_active_streams {
            connection.pending_heartbeat = None;
            connection.next_heartbeat_at = now + connection.heartbeat_interval;
            return Ok(());
        }
        return Err(RuntimeError::PathHeartbeatTimeout);
    }
    if connection.pending_heartbeat.is_none() && now >= connection.next_heartbeat_at {
        let nonce = random_u64()?;
        connection
            .writer
            .write_frame(&Frame::Ping { nonce })
            .await?;
        connection.writer.flush().await?;
        connection.pending_heartbeat = Some((nonce, now + mux_limits.tcp_path_heartbeat_timeout));
    }
    Ok(())
}

pub(super) async fn close_client_tcp_path(
    connection: &mut ClientTcpPathConnection,
    path_id: PathId,
    drain: bool,
) -> Result<(), RuntimeError> {
    if drain {
        connection
            .writer
            .write_frame(&Frame::PathDrain { path_id })
            .await?;
    }
    connection
        .writer
        .write_frame(&Frame::PathClose {
            path_id,
            reason: CloseReason::Normal,
        })
        .await?;
    connection
        .writer
        .write_frame(&Frame::SessionClose {
            reason: CloseReason::Normal,
        })
        .await?;
    connection.writer.flush().await?;
    Ok(())
}

fn fail_client_tcp_streams(
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    reason: &RuntimeError,
) {
    for (_, mut state) in streams.drain() {
        if let Some(pending) = state.pending_open.take() {
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
        RuntimeError::RemoteReset(reason) => RuntimeError::RemoteReset(*reason),
        RuntimeError::RemoteClosed(reason) => RuntimeError::RemoteClosed(*reason),
        RuntimeError::Protocol(message) => RuntimeError::Protocol(message),
        _ => RuntimeError::ReliablePathSessionClosed,
    }
}

pub(super) fn spawn_encrypted_tcp_reader(
    mut reader: EncryptedTcpReader,
    queue_size: usize,
) -> mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>> {
    let (frames_tx, frames_rx) = mpsc::channel(queue_size);
    tokio::spawn(async move {
        loop {
            let frame = reader.read_frame().await;
            let done = frame.is_err();
            #[cfg(feature = "lab-diagnostics")]
            let bytes = frame.as_ref().ok().map(frame_pacing_bytes).unwrap_or(0);
            #[cfg(feature = "lab-diagnostics")]
            let started = Instant::now();
            let send_result = frames_tx.send(frame).await;
            #[cfg(feature = "lab-diagnostics")]
            lab_perf_record("runtime.tcp_reader.queue_send", started.elapsed(), bytes);
            if send_result.is_err() || done {
                break;
            }
        }
    });
    frames_rx
}

pub(super) fn tcp_session_command_queue(resources: ResourceLimits) -> usize {
    reliable_path_command_queue(resources.into())
}

#[cfg(test)]
mod tests;
