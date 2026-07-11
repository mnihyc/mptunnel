use super::*;
use std::collections::HashMap;

// Ownership boundary:
// This module owns TCP underlay carrier sessions only: encrypted framed TCP
// connection setup, command queues, heartbeat/liveness, frame reads, and writer
// shutdown. Product reliable stream identity, response path-flight ownership,
// and cross-carrier scheduling live in `reliable_path`.

pub(super) struct ClientTcpPathSessionHandle {
    runtime: ClientTcpPathSessionRuntime,
    commands: Arc<Mutex<Option<ReliablePathCommandSender>>>,
    latency_commands: Arc<Mutex<Option<ReliablePathCommandSender>>>,
}

pub(super) struct ClientTcpOpenCancellation {
    commands: ReliablePathCommandSender,
    stream_id: StreamId,
    armed: bool,
}

impl ClientTcpOpenCancellation {
    pub(super) fn new(commands: ReliablePathCommandSender, stream_id: StreamId) -> Self {
        Self {
            commands,
            stream_id,
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
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = commands
                    .send_control(ReliablePathCommand::SendFrame(Frame::StreamDetach {
                        stream_id,
                    }))
                    .await;
                let _ = commands
                    .send_control(ReliablePathCommand::CloseStream(stream_id))
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

    pub(super) async fn open_stream(
        &self,
        stream_id: StreamId,
        target: TargetAddr,
        ingress: IngressKind,
        lane: FlowLane,
        role: StreamOpenRole,
        open_deadline: tokio::time::Instant,
    ) -> Result<ReliablePathStream, RuntimeError> {
        let commands = self.ensure_session(lane);
        let (response_tx, response_rx) = oneshot::channel();
        let mut cancellation = ClientTcpOpenCancellation::new(commands.clone(), stream_id);
        tokio::time::timeout_at(
            open_deadline,
            commands.send_control(ReliablePathCommand::OpenStream {
                stream_id,
                target,
                ingress,
                lane,
                role,
                open_deadline,
                session_commands: commands.clone(),
                response: response_tx,
            }),
        )
        .await
        .map_err(|_| RuntimeError::PathOpenTimedOut)?
        .map_err(|_| RuntimeError::ReliablePathSessionClosed)?;
        let response = tokio::time::timeout_at(open_deadline, response_rx)
            .await
            .map_err(|_| RuntimeError::PathOpenTimedOut)?
            .map_err(|_| RuntimeError::ReliablePathSessionClosed)?;
        match response {
            Ok(stream) => {
                cancellation.disarm();
                Ok(stream)
            }
            Err(err) => Err(err),
        }
    }

    pub(super) fn ensure_session(&self, lane: FlowLane) -> ReliablePathCommandSender {
        if tcp_path_lane_uses_dedicated_session(lane) && !self.runtime.reuse_latency_session {
            let (commands, receivers) = reliable_path_command_channels(self.runtime.command_queue);
            tokio::spawn(run_client_tcp_path_session(self.runtime.clone(), receivers));
            return commands;
        }

        let lane = if tcp_path_lane_uses_dedicated_session(lane) {
            &self.latency_commands
        } else {
            &self.commands
        };
        let mut current = lane.lock().expect("TCP path session lock");
        if let Some(commands) = current.as_ref()
            && !commands.is_closed()
        {
            return commands.clone();
        }

        let (commands, receivers) = reliable_path_command_channels(self.runtime.command_queue);
        tokio::spawn(run_client_tcp_path_session(self.runtime.clone(), receivers));
        *current = Some(commands.clone());
        commands
    }
}

pub(super) fn tcp_path_lane_uses_dedicated_session(lane: FlowLane) -> bool {
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
}

pub(super) type EncryptedTcpReader = EncryptedFramedReader<tokio::io::ReadHalf<TcpStream>>;
pub(super) type EncryptedTcpWriter = EncryptedFramedWriter<tokio::io::WriteHalf<TcpStream>>;

pub(super) struct ClientTcpPathStreamState {
    pub(super) frames: mpsc::Sender<Result<Frame, RuntimeError>>,
    pub(super) pending_open: Option<ClientTcpPendingOpen>,
    pub(super) local_close_pending: bool,
}

pub(super) struct ClientTcpPendingOpen {
    response: oneshot::Sender<Result<ReliablePathStream, RuntimeError>>,
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
    pub(super) reuse_latency_session: bool,
    pub(super) health: Arc<Mutex<ClientPathHealth>>,
}

struct ClientTcpPathSessionState {
    connection: Option<ClientTcpPathConnection>,
    streams: HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: RecentIdCache<StreamId>,
}

struct ClientTcpOpenStreamRequest {
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
    role: StreamOpenRole,
    open_deadline: tokio::time::Instant,
    session_commands: ReliablePathCommandSender,
    response: oneshot::Sender<Result<ReliablePathStream, RuntimeError>>,
}

async fn run_client_tcp_path_session(
    runtime: ClientTcpPathSessionRuntime,
    mut commands: ReliablePathCommandReceivers,
) {
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

        let command_may_recv = !reliable_path_receivers_closed(&commands);
        if !command_may_recv {
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

        let mut drop_connection = false;
        tokio::select! {
            biased;
            _ = &mut pending_open_timer, if pending_open_deadline.is_some() => {
                expire_client_tcp_pending_opens(
                    &mut state.streams,
                    &mut state.closed_streams,
                );
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
                                runtime.stream_frame_queue,
                                runtime.mux_limits,
                                &mut pending_frames,
                            )
                            .await;
                            if let Err(err) = result {
                                fail_client_tcp_streams(&mut state.streams, &err);
                                eprintln!("warning: TCP path session command failed: {err}");
                                drop_connection = true;
                            }
                        }
                    }
                    Some(Err(err)) => {
                        let err = RuntimeError::Encrypted(err);
                        fail_client_tcp_streams(&mut state.streams, &err);
                        eprintln!("warning: TCP path session read failed: {err}");
                        drop_connection = true;
                    }
                    None => {
                        let err = RuntimeError::ReliablePathSessionClosed;
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
                            runtime.stream_frame_queue,
                            runtime.mux_limits,
                            &mut pending_frames,
                        )
                        .await;
                        if let Err(err) = result {
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
            _ = &mut heartbeat_timer => {
                if let Err(err) = tick_client_tcp_path_heartbeat(
                    state.connection.as_mut().expect("checked connected TCP path session"),
                    runtime.mux_limits,
                    !state.streams.is_empty(),
                )
                .await
                {
                    fail_client_tcp_streams(&mut state.streams, &err);
                    eprintln!("warning: TCP path heartbeat failed: {err}");
                    drop_connection = true;
                }
            }
        }

        if drop_connection {
            state.connection = None;
        }
    }
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

fn expire_client_tcp_pending_opens(
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
) {
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
        return;
    }

    for stream_id in expired {
        if let Some(mut state) = streams.remove(&stream_id)
            && let Some(pending) = state.pending_open.take()
        {
            let _ = pending.response.send(Err(RuntimeError::PathOpenTimedOut));
        }
        closed_streams.insert(stream_id);
    }
}

async fn handle_connected_client_tcp_command_run(
    first_command: ReliablePathCommand,
    commands: &mut ReliablePathCommandReceivers,
    connection: &mut ClientTcpPathConnection,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    runtime: &ClientTcpPathSessionRuntime,
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
            ReliablePathCommand::SendFrame(frame) => {
                let is_stream_detach = matches!(&frame, Frame::StreamDetach { .. });
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
            target,
            ingress,
            lane,
            role,
            open_deadline,
            session_commands,
            mut response,
        } => {
            if response.is_closed() || open_deadline <= tokio::time::Instant::now() {
                let _ = response.send(Err(RuntimeError::PathOpenTimedOut));
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
                        let _ = response.send(Err(RuntimeError::PathOpenTimedOut));
                        return;
                    }
                    let open = ClientTcpOpenStreamRequest {
                        stream_id,
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
                    let _ = response.send(Err(err));
                }
            }
        }
        ReliablePathCommand::SendFrame(_) | ReliablePathCommand::CloseStream(_) => {}
    }
}

async fn handle_connected_client_tcp_command(
    command: ReliablePathCommand,
    connection: &mut ClientTcpPathConnection,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    stream_frame_queue: usize,
    mux_limits: MuxLimits,
    flush_after_frame: bool,
) -> Result<(), RuntimeError> {
    match command {
        ReliablePathCommand::OpenStream {
            stream_id,
            target,
            ingress,
            lane,
            role,
            open_deadline,
            session_commands,
            response,
        } => {
            let open = ClientTcpOpenStreamRequest {
                stream_id,
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
        ReliablePathCommand::SendFrame(frame) => {
            connection.writer.write_frame(&frame).await?;
            connection.path_proofs.record_sent_frame(&frame);
            if flush_after_frame {
                connection.writer.flush().await?;
            }
            record_client_tcp_path_outbound_activity(connection, mux_limits);
            Ok(())
        }
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
        target,
        ingress,
        lane,
        role,
        open_deadline,
        session_commands,
        response,
    } = open;
    if response.is_closed() || open_deadline <= tokio::time::Instant::now() {
        let _ = response.send(Err(RuntimeError::PathOpenTimedOut));
        return Ok(());
    }
    let (frames_tx, frames_rx) = mpsc::channel(stream_frame_queue);
    streams.insert(
        stream_id,
        ClientTcpPathStreamState {
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

async fn handle_client_tcp_path_frame(
    frame: Frame,
    connection: &mut ClientTcpPathConnection,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    runtime: &ClientTcpPathSessionRuntime,
) -> Result<(), RuntimeError> {
    refresh_client_tcp_path_liveness(connection, runtime.mux_limits);
    expire_client_tcp_pending_opens(streams, closed_streams);
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
                    if pending.response.send(Ok(stream)).is_err() {
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
                    .send(Err(RuntimeError::RemoteReset(reason)));
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
                && let Some(record) = runtime
                    .health
                    .lock()
                    .expect("client path health lock")
                    .tcp
                    .get_mut(runtime.path_index)
            {
                record.mark_path_proof_success(observation.elapsed);
            }
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
            let _ = pending.response.send(Err(tcp_path_stream_error(reason)));
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
mod tests {
    use super::*;

    #[test]
    fn control_and_ack_frames_never_use_throughput_lane() {
        let priority_frames = [
            (
                Frame::StreamAck {
                    stream_id: StreamId(1),
                    complete: false,
                    ranges: vec![],
                },
                FlowLane::Control,
            ),
            (
                Frame::StreamMaxData {
                    stream_id: StreamId(1),
                    max_offset: 1024,
                },
                FlowLane::Control,
            ),
            (
                Frame::StreamFin {
                    stream_id: StreamId(1),
                    final_offset: 64,
                },
                FlowLane::Control,
            ),
            (
                Frame::StreamReset {
                    stream_id: StreamId(1),
                    reason: ResetReason::RemoteClosed,
                },
                FlowLane::Control,
            ),
            (
                Frame::StreamDetach {
                    stream_id: StreamId(1),
                },
                FlowLane::Control,
            ),
            (
                Frame::DatagramFeedback {
                    flow_id: DatagramFlowId(1),
                    received: vec![],
                },
                FlowLane::RealtimeDatagram,
            ),
            (
                Frame::DatagramClose {
                    flow_id: DatagramFlowId(1),
                },
                FlowLane::Control,
            ),
        ];

        for (frame, expected_lane) in priority_frames {
            let effective_lane = reliable_path_effective_frame_lane(&frame, FlowLane::Throughput);
            assert_eq!(effective_lane, expected_lane);
            assert!(reliable_path_frame_uses_priority_queue(effective_lane));
        }
    }
}
