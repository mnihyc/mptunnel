use super::*;
use std::collections::HashMap;

// Ownership boundary:
// This module owns TCP underlay carrier sessions only: encrypted framed TCP
// connection setup, command queues, heartbeat/liveness, frame reads, and writer
// shutdown. Product reliable stream identity, response path-flight ownership,
// and cross-carrier scheduling live in `reliable_path`.

pub(super) struct ClientTcpPathSessionHandle {
    runtime: ClientTcpPathSessionRuntime,
    commands: Arc<Mutex<Option<TcpPathSessionCommandSender>>>,
    latency_commands: Arc<Mutex<Option<TcpPathSessionCommandSender>>>,
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
    ) -> Result<ReliablePathStream, RuntimeError> {
        let commands = self.ensure_session(lane);
        let (response_tx, response_rx) = oneshot::channel();
        commands
            .send_control(TcpPathSessionCommand::OpenStream {
                stream_id,
                target,
                ingress,
                lane,
                role,
                session_commands: commands.clone(),
                response: response_tx,
            })
            .await
            .map_err(|_| RuntimeError::TcpPathSessionClosed)?;
        response_rx
            .await
            .map_err(|_| RuntimeError::TcpPathSessionClosed)?
    }

    pub(super) async fn cancel_stream_open(&self, lane: FlowLane, stream_id: StreamId) {
        let commands =
            if tcp_path_lane_uses_dedicated_session(lane) && !self.runtime.reuse_latency_session {
                None
            } else if tcp_path_lane_uses_dedicated_session(lane) {
                self.latency_commands
                    .lock()
                    .expect("TCP path session lock")
                    .clone()
            } else {
                self.commands.lock().expect("TCP path session lock").clone()
            };
        if let Some(commands) = commands
            && !commands.is_closed()
        {
            let _ = commands
                .send_control(TcpPathSessionCommand::CloseStream(stream_id))
                .await;
        }
    }

    pub(super) fn ensure_session(&self, lane: FlowLane) -> TcpPathSessionCommandSender {
        if tcp_path_lane_uses_dedicated_session(lane) && !self.runtime.reuse_latency_session {
            let (commands, receivers) =
                tcp_path_session_command_channels(self.runtime.command_queue);
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

        let (commands, receivers) = tcp_path_session_command_channels(self.runtime.command_queue);
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
    pub(super) writer: EncryptedTcpWriter,
    pub(super) frames: mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>>,
    pub(super) heartbeat_interval: Duration,
    pub(super) next_heartbeat_at: tokio::time::Instant,
    pub(super) pending_heartbeat: Option<(u64, tokio::time::Instant)>,
}

pub(super) type EncryptedTcpReader = EncryptedFramedReader<tokio::io::ReadHalf<TcpStream>>;
pub(super) type EncryptedTcpWriter = EncryptedFramedWriter<tokio::io::WriteHalf<TcpStream>>;

pub(super) struct ClientTcpPathStreamState {
    pub(super) frames: mpsc::Sender<Result<Frame, RuntimeError>>,
    pub(super) pending_open: Option<ClientTcpPendingOpen>,
}

pub(super) struct ClientTcpPendingOpen {
    response: oneshot::Sender<Result<ReliablePathStream, RuntimeError>>,
    frames: Option<mpsc::Receiver<Result<Frame, RuntimeError>>>,
    session_commands: TcpPathSessionCommandSender,
    lane: FlowLane,
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
    session_commands: TcpPathSessionCommandSender,
    response: oneshot::Sender<Result<ReliablePathStream, RuntimeError>>,
}

async fn run_client_tcp_path_session(
    runtime: ClientTcpPathSessionRuntime,
    mut commands: TcpPathSessionCommandReceivers,
) {
    let mut state = ClientTcpPathSessionState {
        connection: None,
        streams: HashMap::new(),
        closed_streams: RecentIdCache::new(runtime.closed_stream_cache_capacity),
    };

    loop {
        if state.connection.is_none() {
            match recv_tcp_path_command(&mut commands).await {
                Some(command) => {
                    let pending_bytes = tcp_path_command_pending_bytes(&command);
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

        let command_may_recv = !tcp_path_receivers_closed(&commands);
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
            command = recv_tcp_path_command(&mut commands), if command_may_recv => {
                match command {
                    Some(command) => {
                        let pending_bytes = tcp_path_command_pending_bytes(&command);
                        let result = handle_connected_client_tcp_command(
                            command,
                            state.connection.as_mut().expect("checked connected TCP path session"),
                            &mut state.streams,
                            &mut state.closed_streams,
                            runtime.stream_frame_queue,
                            runtime.mux_limits,
                        )
                        .await;
                        commands.release_pending_command_bytes(pending_bytes);
                        if let Err(err) = result {
                            fail_client_tcp_streams(&mut state.streams, &err);
                            eprintln!("warning: TCP path session command failed: {err}");
                            drop_connection = true;
                        }
                    }
                    None => {
                        if tcp_path_receivers_closed(&commands) {
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
            frame = state.connection.as_mut().expect("checked connected TCP path session").frames.recv() => {
                match frame {
                    Some(Ok(frame)) => {
                        if let Err(err) = handle_client_tcp_path_frame(
                            frame,
                            state.connection.as_mut().expect("checked connected TCP path session"),
                            &mut state.streams,
                            &mut state.closed_streams,
                            runtime.mux_limits,
                        )
                        .await
                        {
                            fail_client_tcp_streams(&mut state.streams, &err);
                            eprintln!("warning: TCP path session frame handling failed: {err}");
                            drop_connection = true;
                        }
                    }
                    Some(Err(err)) => {
                        let err = RuntimeError::Encrypted(err);
                        fail_client_tcp_streams(&mut state.streams, &err);
                        eprintln!("warning: TCP path session read failed: {err}");
                        drop_connection = true;
                    }
                    None => {
                        let err = RuntimeError::TcpPathSessionClosed;
                        fail_client_tcp_streams(&mut state.streams, &err);
                        drop_connection = true;
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

async fn handle_disconnected_client_tcp_command(
    command: TcpPathSessionCommand,
    runtime: &ClientTcpPathSessionRuntime,
    state: &mut ClientTcpPathSessionState,
) {
    match command {
        TcpPathSessionCommand::OpenStream {
            stream_id,
            target,
            ingress,
            lane,
            role,
            session_commands,
            response,
        } => match connect_client_tcp_path(
            &runtime.path,
            runtime.path_index,
            runtime.session_id,
            &runtime.security,
            runtime.codec_limits,
            runtime.mux_limits,
        )
        .await
        {
            Ok(mut connected) => {
                let open = ClientTcpOpenStreamRequest {
                    stream_id,
                    target,
                    ingress,
                    lane,
                    role,
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
                    eprintln!("warning: reliable stream open on new path session failed: {err}");
                    fail_client_tcp_streams(&mut state.streams, &err);
                }
            }
            Err(err) => {
                let _ = response.send(Err(err));
            }
        },
        TcpPathSessionCommand::SendFrame(_) | TcpPathSessionCommand::CloseStream(_) => {}
    }
}

async fn handle_connected_client_tcp_command(
    command: TcpPathSessionCommand,
    connection: &mut ClientTcpPathConnection,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    stream_frame_queue: usize,
    mux_limits: MuxLimits,
) -> Result<(), RuntimeError> {
    match command {
        TcpPathSessionCommand::OpenStream {
            stream_id,
            target,
            ingress,
            lane,
            role,
            session_commands,
            response,
        } => {
            let open = ClientTcpOpenStreamRequest {
                stream_id,
                target,
                ingress,
                lane,
                role,
                session_commands,
                response,
            };
            open_client_tcp_stream_on_connection(connection, open, streams, stream_frame_queue)
                .await?;
            record_client_tcp_path_outbound_activity(connection, mux_limits);
            Ok(())
        }
        TcpPathSessionCommand::SendFrame(frame) => {
            connection.writer.write_frame(&frame).await?;
            connection.writer.flush().await?;
            record_client_tcp_path_outbound_activity(connection, mux_limits);
            Ok(())
        }
        TcpPathSessionCommand::CloseStream(stream_id) => {
            if streams.remove(&stream_id).is_some() {
                closed_streams.insert(stream_id);
            }
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
) -> Result<ClientTcpPathConnection, RuntimeError> {
    let tcp_stream = tcp::connect_path(path, TcpConnectOptions::default()).await?;
    let mut framed = EncryptedFramedStream::with_cipher_suite(
        tcp_stream,
        security.secret.as_bytes(),
        PeerRole::Client,
        codec_limits,
        security.cipher,
    );
    let path_id = PathId(path_index as u16);
    let (session_hello, session_auth, path_join) = authenticated_path_join_frames_for_session(
        security,
        path,
        path_id,
        UnderlayProtocol::Tcp,
        session_id,
    )?;

    framed.write_frame(&session_hello).await?;
    framed.write_frame(&session_auth).await?;
    framed.write_frame(&path_join).await?;
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

    let (reader, writer) = framed.split();
    let now = tokio::time::Instant::now();
    Ok(ClientTcpPathConnection {
        writer,
        frames: spawn_encrypted_tcp_reader(reader, tcp_path_session_frame_queue(mux_limits)),
        heartbeat_interval: mux_limits.tcp_path_heartbeat_interval,
        next_heartbeat_at: now + mux_limits.tcp_path_heartbeat_interval,
        pending_heartbeat: None,
    })
}

async fn open_client_tcp_stream_on_connection(
    connection: &mut ClientTcpPathConnection,
    open: ClientTcpOpenStreamRequest,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    stream_frame_queue: usize,
) -> Result<(), RuntimeError> {
    let stream_id = open.stream_id;
    let (frames_tx, frames_rx) = mpsc::channel(stream_frame_queue);
    streams.insert(
        stream_id,
        ClientTcpPathStreamState {
            frames: frames_tx,
            pending_open: Some(ClientTcpPendingOpen {
                response: open.response,
                frames: Some(frames_rx),
                session_commands: open.session_commands,
                lane: open.lane,
            }),
        },
    );
    connection
        .writer
        .write_frame(&Frame::OpenStream {
            stream_id,
            target: open.target,
            ingress: open.ingress,
            outbound: OutboundPolicy::Direct,
            demand: stream_demand_hint_for_lane(open.lane),
            role: open.role,
        })
        .await?;
    connection.writer.flush().await?;
    connection.next_heartbeat_at = tokio::time::Instant::now() + connection.heartbeat_interval;
    Ok(())
}

async fn handle_client_tcp_path_frame(
    frame: Frame,
    connection: &mut ClientTcpPathConnection,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    mux_limits: MuxLimits,
) -> Result<(), RuntimeError> {
    refresh_client_tcp_path_liveness(connection, mux_limits);
    match frame {
        Frame::StreamMaxData {
            stream_id,
            max_offset,
        } => {
            if let Some(state) = streams.get_mut(&stream_id)
                && let Some(mut pending) = state.pending_open.take()
            {
                let frames = pending
                    .frames
                    .take()
                    .ok_or(RuntimeError::Protocol("missing TCP stream frame receiver"))?;
                let stream = ReliablePathStream {
                    stream_id,
                    max_offset,
                    lane: pending.lane,
                    underlay: UnderlayProtocol::Tcp,
                    max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
                    output: ReliablePathStreamOutput::Fixed(pending.session_commands),
                    frames,
                };
                let _ = pending.response.send(Ok(stream));
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
            if let Some(mut state) = streams.remove(&stream_id)
                && let Some(pending) = state.pending_open.take()
            {
                closed_streams.insert(stream_id);
                let _ = pending
                    .response
                    .send(Err(RuntimeError::RemoteReset(reason)));
                return Ok(());
            }
            route_client_tcp_stream_frame(
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
            route_client_tcp_stream_frame(
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
        Frame::Ping { nonce } => {
            connection
                .writer
                .write_frame(&Frame::Pong { nonce })
                .await?;
            connection.writer.flush().await?;
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
        } => Err(RuntimeError::TcpPathSessionClosed),
        Frame::PathStatus { .. } => Ok(()),
        Frame::SessionClose { reason } => Err(RuntimeError::RemoteClosed(reason)),
        Frame::PathDrain { .. } | Frame::PathClose { .. } => {
            Err(RuntimeError::TcpPathSessionClosed)
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
        if closed_streams.contains(&stream_id) {
            return Ok(());
        }
        return Err(RuntimeError::Protocol("frame for unknown TCP stream"));
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
        RuntimeError::TcpPathSessionClosed => RuntimeError::TcpPathSessionClosed,
        RuntimeError::RemoteReset(reason) => RuntimeError::RemoteReset(*reason),
        RuntimeError::RemoteClosed(reason) => RuntimeError::RemoteClosed(*reason),
        RuntimeError::Protocol(message) => RuntimeError::Protocol(message),
        _ => RuntimeError::TcpPathSessionClosed,
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
    tcp_path_command_queue(resources.into())
}

pub(super) fn tcp_path_command_queue(mux_limits: MuxLimits) -> usize {
    let frame_payload = mux_limits
        .max_reliable_relay_chunk_bytes
        .min(mux_limits.max_payload_bytes)
        .max(1);
    tcp_path_command_queue_for_payload(mux_limits, frame_payload)
}

pub(super) fn tcp_path_command_queue_for_payload(
    mux_limits: MuxLimits,
    frame_payload_bytes: usize,
) -> usize {
    let frame_payload = frame_payload_bytes.min(mux_limits.max_payload_bytes).max(1);
    let inflight_frames = mux_limits
        .max_tcp_path_inflight_bytes
        .saturating_add(frame_payload - 1)
        / frame_payload;
    inflight_frames.saturating_add(4).clamp(
        4,
        tcp_path_session_frame_queue_for_payload(mux_limits, frame_payload).max(4),
    )
}

pub(super) fn tcp_path_session_frame_queue(mux_limits: MuxLimits) -> usize {
    let frame_payload = mux_limits
        .max_reliable_relay_chunk_bytes
        .min(mux_limits.max_payload_bytes)
        .max(1);
    tcp_path_session_frame_queue_for_payload(mux_limits, frame_payload)
}

pub(super) fn tcp_path_session_frame_queue_for_payload(
    mux_limits: MuxLimits,
    frame_payload_bytes: usize,
) -> usize {
    reliable_stream_frame_queue_for_payload(mux_limits, frame_payload_bytes)
        .saturating_mul(4)
        .clamp(16, 4096)
}

pub(super) fn reliable_stream_frame_queue(mux_limits: MuxLimits) -> usize {
    let frame_payload = mux_limits
        .max_reliable_relay_chunk_bytes
        .min(mux_limits.max_payload_bytes)
        .max(1);
    reliable_stream_frame_queue_for_payload(mux_limits, frame_payload)
}

pub(super) fn reliable_stream_frame_queue_for_payload(
    mux_limits: MuxLimits,
    frame_payload_bytes: usize,
) -> usize {
    let frame_payload = frame_payload_bytes.min(mux_limits.max_payload_bytes).max(1);
    (mux_limits.max_reorder_bytes / frame_payload)
        .saturating_add(4)
        .clamp(4, 1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_stream_fin_is_control_not_current_data_path() {
        let frame = Frame::StreamFin {
            stream_id: StreamId(1),
            final_offset: 64,
        };

        assert!(!server_frame_prefers_current_data_path(
            &frame,
            FlowLane::Throughput
        ));
    }

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
            let effective_lane = tcp_path_effective_frame_lane(&frame, FlowLane::Throughput);
            assert_eq!(effective_lane, expected_lane);
            assert!(tcp_path_frame_uses_priority_queue(effective_lane));
            if !matches!(frame, Frame::StreamFin { .. }) {
                assert!(!reliable_relay_frame_prefers_current_data_path(
                    &frame,
                    FlowLane::Throughput
                ));
            }
        }
    }
}
