use super::*;

pub(super) struct TcpPathStream {
    pub(super) stream_id: StreamId,
    pub(super) max_offset: u64,
    pub(super) lane: FlowLane,
    pub(super) underlay: UnderlayProtocol,
    pub(super) max_frame_payload_bytes: usize,
    pub(super) output: TcpPathStreamOutput,
    pub(super) frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
}

impl TcpPathStream {
    pub(super) fn into_handle_and_frames(
        self,
    ) -> (
        TcpPathStreamHandle,
        mpsc::Receiver<Result<Frame, RuntimeError>>,
    ) {
        (
            TcpPathStreamHandle {
                stream_id: self.stream_id,
                max_offset: self.max_offset,
                lane: self.lane,
                underlay: self.underlay,
                max_frame_payload_bytes: self.max_frame_payload_bytes,
                output: self.output,
            },
            self.frames,
        )
    }

    pub(super) async fn send_frame(&self, frame: Frame) -> Result<(), RuntimeError> {
        self.output
            .send_frame(self.stream_id, self.lane, frame)
            .await
    }

    pub(super) async fn recv_frame(&mut self) -> Result<Frame, RuntimeError> {
        match self.frames.recv().await {
            Some(Ok(frame)) => Ok(frame),
            Some(Err(err)) => Err(err),
            None => Err(RuntimeError::TcpPathSessionClosed),
        }
    }

    pub(super) fn current_lane(&self) -> FlowLane {
        self.output.current_lane(self.lane)
    }

    pub(super) fn set_lane(&mut self, lane: FlowLane) {
        self.lane = lane;
        self.output.set_lane(lane);
    }

    pub(super) async fn close(&self) {
        self.output.close_stream(self.stream_id).await;
    }
}

pub(super) struct TcpPathStreamHandle {
    pub(super) stream_id: StreamId,
    pub(super) max_offset: u64,
    pub(super) lane: FlowLane,
    pub(super) underlay: UnderlayProtocol,
    pub(super) max_frame_payload_bytes: usize,
    pub(super) output: TcpPathStreamOutput,
}

impl TcpPathStreamHandle {
    pub(super) async fn send_frame(&self, frame: Frame) -> Result<(), RuntimeError> {
        self.output
            .send_frame(self.stream_id, self.lane, frame)
            .await
    }

    pub(super) async fn close(&self) {
        self.output.close_stream(self.stream_id).await;
    }
}

#[derive(Clone)]
pub(super) enum TcpPathStreamOutput {
    Fixed(TcpPathSessionCommandSender),
    Switchable(Arc<ServerTcpStreamBinding>),
}

impl TcpPathStreamOutput {
    pub(super) async fn send_frame(
        &self,
        stream_id: StreamId,
        lane: FlowLane,
        frame: Frame,
    ) -> Result<(), RuntimeError> {
        match self {
            Self::Fixed(commands) => commands.send_frame(frame, lane).await,
            Self::Switchable(binding) => binding.send_frame(stream_id, lane, frame).await,
        }
    }

    pub(super) async fn close_stream(&self, stream_id: StreamId) {
        match self {
            Self::Fixed(commands) => {
                let _ = commands
                    .send_frame(Frame::StreamDetach { stream_id }, FlowLane::Control)
                    .await;
                let _ = commands
                    .send_control(TcpPathSessionCommand::CloseStream(stream_id))
                    .await;
            }
            Self::Switchable(binding) => binding.close_stream(stream_id).await,
        }
    }

    pub(super) fn current_lane(&self, fallback: FlowLane) -> FlowLane {
        match self {
            Self::Fixed(_) => fallback,
            Self::Switchable(binding) => binding.lane(),
        }
    }

    pub(super) fn set_lane(&self, lane: FlowLane) {
        if let Self::Switchable(binding) = self {
            binding.set_lane(lane);
        }
    }
}

pub(super) struct ServerTcpStreamBinding {
    lane: Mutex<FlowLane>,
    outputs: Mutex<ServerTcpStreamOutputs>,
    version: watch::Sender<u64>,
}

impl ServerTcpStreamBinding {
    pub(super) fn new(
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: TcpPathSessionCommandSender,
        lane: FlowLane,
    ) -> Arc<Self> {
        let (version, _) = watch::channel(0);
        Arc::new(Self {
            lane: Mutex::new(lane),
            outputs: Mutex::new(ServerTcpStreamOutputs {
                next_index: 0,
                entries: vec![ServerTcpStreamOutputEntry {
                    key: ServerTcpPathKey { underlay, path_id },
                    commands,
                }],
            }),
            version,
        })
    }

    pub(super) fn attach(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: TcpPathSessionCommandSender,
        lane: FlowLane,
        role: StreamOpenRole,
    ) {
        *self.lane.lock().expect("server TCP stream lane lock") = lane;
        let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
        let key = ServerTcpPathKey { underlay, path_id };
        let mut was_active = false;
        let entry =
            if let Some(position) = outputs.entries.iter().position(|entry| entry.key == key) {
                was_active = position + 1 == outputs.entries.len();
                let mut entry = outputs.entries.remove(position);
                entry.commands = commands;
                entry
            } else {
                ServerTcpStreamOutputEntry { key, commands }
            };
        let promote_or_keep_active_slot = server_stream_open_role_promotes_data_path(role, lane)
            || was_active
            || outputs.entries.is_empty();
        if promote_or_keep_active_slot {
            outputs.entries.push(entry);
        } else {
            let insert_at = outputs.entries.len().saturating_sub(1);
            outputs.entries.insert(insert_at, entry);
        }
        outputs.next_index %= outputs.entries.len().max(1);
        drop(outputs);
        self.notify_update();
    }

    pub(super) fn lane(&self) -> FlowLane {
        *self.lane.lock().expect("server TCP stream lane lock")
    }

    pub(super) fn set_lane(&self, lane: FlowLane) {
        *self.lane.lock().expect("server TCP stream lane lock") = lane;
        self.notify_update();
    }

    fn detach(&self, key: ServerTcpPathKey, commands: &TcpPathSessionCommandSender) {
        let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
        let before = outputs.entries.len();
        outputs
            .entries
            .retain(|entry| entry.key != key || !entry.commands.same_channel(commands));
        if outputs.entries.len() != before {
            outputs.next_index %= outputs.entries.len().max(1);
            drop(outputs);
            self.notify_update();
        }
    }

    fn next_commands(&self) -> Option<(ServerTcpPathKey, TcpPathSessionCommandSender)> {
        let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
        if outputs.entries.is_empty() {
            return None;
        }
        outputs.next_index %= outputs.entries.len();
        let entry = outputs.entries[outputs.next_index].clone();
        outputs.next_index = (outputs.next_index + 1) % outputs.entries.len();
        Some((entry.key, entry.commands))
    }

    fn data_commands(&self) -> Option<(ServerTcpPathKey, TcpPathSessionCommandSender)> {
        self.outputs
            .lock()
            .expect("server TCP stream binding lock")
            .entries
            .last()
            .cloned()
            .map(|entry| (entry.key, entry.commands))
    }

    pub(super) async fn send_frame(
        &self,
        _stream_id: StreamId,
        _lane: FlowLane,
        frame: Frame,
    ) -> Result<(), RuntimeError> {
        let mut updates = self.version.subscribe();
        loop {
            let selected = if server_frame_prefers_current_data_path(&frame) {
                self.data_commands()
            } else {
                self.next_commands()
            };
            if let Some((key, commands)) = selected {
                let lane = self.lane();
                tokio::select! {
                    result = commands.send_frame(frame.clone(), lane) => {
                        match result {
                            Ok(()) => return Ok(()),
                            Err(_) => self.detach(key, &commands),
                        }
                    }
                    changed = updates.changed() => {
                        changed.map_err(|_| RuntimeError::TcpPathSessionClosed)?;
                    }
                }
            } else {
                updates
                    .changed()
                    .await
                    .map_err(|_| RuntimeError::TcpPathSessionClosed)?;
            }
        }
    }

    pub(super) async fn close_stream(&self, stream_id: StreamId) {
        let outputs = self
            .outputs
            .lock()
            .expect("server TCP stream binding lock")
            .entries
            .clone();
        for entry in outputs {
            let _ = entry
                .commands
                .send_control(TcpPathSessionCommand::CloseStream(stream_id))
                .await;
        }
    }

    fn notify_update(&self) {
        let current = *self.version.borrow();
        let _ = self.version.send(current.wrapping_add(1));
    }
}

fn server_frame_prefers_current_data_path(frame: &Frame) -> bool {
    matches!(frame, Frame::StreamData { .. } | Frame::StreamFin { .. })
}

fn server_stream_open_role_promotes_data_path(role: StreamOpenRole, lane: FlowLane) -> bool {
    role == StreamOpenRole::Active
        || !matches!(lane, FlowLane::Control | FlowLane::RealtimeDatagram)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ServerTcpPathKey {
    underlay: UnderlayProtocol,
    path_id: PathId,
}

#[derive(Clone)]
struct ServerTcpStreamOutputEntry {
    key: ServerTcpPathKey,
    commands: TcpPathSessionCommandSender,
}

struct ServerTcpStreamOutputs {
    entries: Vec<ServerTcpStreamOutputEntry>,
    next_index: usize,
}

pub(super) struct ServerTcpStreamRegistry {
    streams: Mutex<HashMap<(SessionId, StreamId), ServerTcpStreamEntry>>,
    closed_streams: Mutex<RecentIdCache<(SessionId, StreamId)>>,
}

impl std::fmt::Debug for ServerTcpStreamRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerTcpStreamRegistry")
            .finish_non_exhaustive()
    }
}

struct ServerTcpStreamEntry {
    target: TargetAddr,
    lane: FlowLane,
    frames: mpsc::Sender<Result<Frame, RuntimeError>>,
    binding: Arc<ServerTcpStreamBinding>,
}

pub(super) struct ServerTcpPathAttachment {
    pub(super) path_id: PathId,
    pub(super) underlay: UnderlayProtocol,
    pub(super) commands: TcpPathSessionCommandSender,
    pub(super) max_frame_payload_bytes: usize,
    pub(super) role: StreamOpenRole,
}

pub(super) struct ServerTcpStreamOpenRequest<'a> {
    pub(super) session_id: SessionId,
    pub(super) stream_id: StreamId,
    pub(super) target: &'a TargetAddr,
    pub(super) lane: FlowLane,
    pub(super) attachment: ServerTcpPathAttachment,
}

pub(super) enum ServerTcpStreamOpen {
    New(TcpPathStream),
    Existing,
}

impl ServerTcpStreamRegistry {
    pub(super) fn new(max_streams: usize) -> Self {
        Self {
            streams: Mutex::new(HashMap::new()),
            closed_streams: Mutex::new(RecentIdCache::new(tcp_closed_stream_cache_capacity(
                max_streams,
            ))),
        }
    }

    pub(super) fn open_or_attach(
        &self,
        request: ServerTcpStreamOpenRequest<'_>,
        mux_limits: MuxLimits,
        max_streams: usize,
    ) -> Result<ServerTcpStreamOpen, RuntimeError> {
        let ServerTcpStreamOpenRequest {
            session_id,
            stream_id,
            target,
            lane,
            attachment,
        } = request;
        let max_frame_payload_bytes = attachment.max_frame_payload_bytes;
        let underlay = attachment.underlay;
        let mut streams = self
            .streams
            .lock()
            .expect("server TCP stream registry lock");
        if let Some(entry) = streams.get_mut(&(session_id, stream_id)) {
            if entry.target != *target {
                return Err(RuntimeError::Protocol(
                    "TCP stream migration target does not match original stream",
                ));
            }
            entry.lane = lane;
            entry.binding.attach(
                underlay,
                attachment.path_id,
                attachment.commands,
                lane,
                attachment.role,
            );
            return Ok(ServerTcpStreamOpen::Existing);
        }

        if streams.len() >= max_streams {
            return Err(RuntimeError::Protocol("server TCP stream limit reached"));
        }

        let (frames_tx, frames_rx) = mpsc::channel(tcp_stream_frame_queue(mux_limits));
        let binding =
            ServerTcpStreamBinding::new(underlay, attachment.path_id, attachment.commands, lane);
        streams.insert(
            (session_id, stream_id),
            ServerTcpStreamEntry {
                target: target.clone(),
                lane,
                frames: frames_tx,
                binding: binding.clone(),
            },
        );
        Ok(ServerTcpStreamOpen::New(TcpPathStream {
            stream_id,
            max_offset: mux_limits.max_stream_window_bytes,
            lane,
            underlay,
            max_frame_payload_bytes,
            output: TcpPathStreamOutput::Switchable(binding),
            frames: frames_rx,
        }))
    }

    pub(super) fn detach_path(
        &self,
        session_id: SessionId,
        stream_id: StreamId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: &TcpPathSessionCommandSender,
    ) {
        if let Some(binding) = self
            .streams
            .lock()
            .expect("server TCP stream registry lock")
            .get(&(session_id, stream_id))
            .map(|entry| entry.binding.clone())
        {
            binding.detach(ServerTcpPathKey { underlay, path_id }, commands);
        }
    }

    pub(super) async fn route_frame(
        &self,
        session_id: SessionId,
        stream_id: StreamId,
        frame: Frame,
    ) -> Result<(), RuntimeError> {
        #[cfg(feature = "lab-diagnostics")]
        let bytes = frame_pacing_bytes(&frame);
        let stream = {
            let streams = self
                .streams
                .lock()
                .expect("server TCP stream registry lock");
            streams
                .get(&(session_id, stream_id))
                .map(|entry| entry.frames.clone())
        };
        let Some(stream) = stream else {
            let closed_key = (session_id, stream_id);
            if self
                .closed_streams
                .lock()
                .expect("server TCP stream closed cache lock")
                .contains(&closed_key)
            {
                return Ok(());
            }
            return Err(RuntimeError::Protocol(
                "frame for unknown server TCP stream",
            ));
        };
        #[cfg(feature = "lab-diagnostics")]
        let started = Instant::now();
        let result = stream
            .send(Ok(frame))
            .await
            .map_err(|_| RuntimeError::TcpPathSessionClosed);
        #[cfg(feature = "lab-diagnostics")]
        lab_perf_record(
            "runtime.server_stream.route_frame",
            started.elapsed(),
            bytes,
        );
        result
    }

    pub(super) fn close(&self, session_id: SessionId, stream_id: StreamId) {
        let removed = self
            .streams
            .lock()
            .expect("server TCP stream registry lock")
            .remove(&(session_id, stream_id))
            .is_some();
        if removed {
            self.closed_streams
                .lock()
                .expect("server TCP stream closed cache lock")
                .insert((session_id, stream_id));
        }
    }
}

impl Default for ServerTcpStreamRegistry {
    fn default() -> Self {
        Self::new(ResourceLimits::default().max_streams)
    }
}

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

    pub(super) async fn open_stream(
        &self,
        stream_id: StreamId,
        target: TargetAddr,
        ingress: IngressKind,
        lane: FlowLane,
        role: StreamOpenRole,
    ) -> Result<TcpPathStream, RuntimeError> {
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

#[derive(Clone)]
pub(super) struct TcpPathSessionCommandSender {
    control: mpsc::Sender<TcpPathSessionCommand>,
    priority: mpsc::Sender<TcpPathSessionCommand>,
    data: mpsc::Sender<TcpPathSessionCommand>,
}

pub(super) struct TcpPathSessionCommandReceivers {
    control: mpsc::Receiver<TcpPathSessionCommand>,
    priority: mpsc::Receiver<TcpPathSessionCommand>,
    data: mpsc::Receiver<TcpPathSessionCommand>,
}

impl TcpPathSessionCommandSender {
    pub(super) async fn send_control(
        &self,
        command: TcpPathSessionCommand,
    ) -> Result<(), mpsc::error::SendError<TcpPathSessionCommand>> {
        #[cfg(feature = "lab-diagnostics")]
        let started = Instant::now();
        let result = self.control.send(command).await;
        #[cfg(feature = "lab-diagnostics")]
        lab_perf_record("runtime.path_queue.control_send", started.elapsed(), 0);
        result
    }

    pub(super) async fn send_frame(
        &self,
        frame: Frame,
        lane: FlowLane,
    ) -> Result<(), RuntimeError> {
        #[cfg(feature = "lab-diagnostics")]
        let bytes = frame_pacing_bytes(&frame);
        let queue = if tcp_path_frame_uses_priority_queue(lane) {
            &self.priority
        } else {
            &self.data
        };
        #[cfg(feature = "lab-diagnostics")]
        let started = Instant::now();
        let result = queue
            .send(TcpPathSessionCommand::SendFrame(frame))
            .await
            .map_err(|_| RuntimeError::TcpPathSessionClosed);
        #[cfg(feature = "lab-diagnostics")]
        lab_perf_record(
            if tcp_path_frame_uses_priority_queue(lane) {
                "runtime.path_queue.priority_send"
            } else {
                "runtime.path_queue.data_send"
            },
            started.elapsed(),
            bytes,
        );
        result
    }

    pub(super) fn is_closed(&self) -> bool {
        self.control.is_closed() && self.priority.is_closed() && self.data.is_closed()
    }

    pub(super) fn same_channel(&self, other: &Self) -> bool {
        self.control.same_channel(&other.control)
            && self.priority.same_channel(&other.priority)
            && self.data.same_channel(&other.data)
    }
}

pub(super) fn tcp_path_frame_uses_priority_queue(lane: FlowLane) -> bool {
    matches!(
        lane,
        FlowLane::Control | FlowLane::Latency | FlowLane::RealtimeDatagram
    )
}

pub(super) fn tcp_path_session_command_channels(
    queue: usize,
) -> (TcpPathSessionCommandSender, TcpPathSessionCommandReceivers) {
    let queue = queue.max(1);
    let (control_tx, control_rx) = mpsc::channel(queue);
    let (priority_tx, priority_rx) = mpsc::channel(queue);
    let (data_tx, data_rx) = mpsc::channel(queue);
    (
        TcpPathSessionCommandSender {
            control: control_tx,
            priority: priority_tx,
            data: data_tx,
        },
        TcpPathSessionCommandReceivers {
            control: control_rx,
            priority: priority_rx,
            data: data_rx,
        },
    )
}

fn tcp_receiver_may_recv<T>(receiver: &mpsc::Receiver<T>) -> bool {
    !receiver.is_closed() || !receiver.is_empty()
}

pub(super) fn tcp_path_receivers_closed(receivers: &TcpPathSessionCommandReceivers) -> bool {
    !tcp_receiver_may_recv(&receivers.control)
        && !tcp_receiver_may_recv(&receivers.priority)
        && !tcp_receiver_may_recv(&receivers.data)
}

pub(super) async fn recv_tcp_path_command(
    receivers: &mut TcpPathSessionCommandReceivers,
) -> Option<TcpPathSessionCommand> {
    let control_may_recv = tcp_receiver_may_recv(&receivers.control);
    let priority_may_recv = tcp_receiver_may_recv(&receivers.priority);
    let data_may_recv = tcp_receiver_may_recv(&receivers.data);
    match (control_may_recv, priority_may_recv, data_may_recv) {
        (true, true, true) => {
            tokio::select! {
                biased;
                command = receivers.control.recv() => command,
                command = receivers.priority.recv() => command,
                command = receivers.data.recv() => command,
            }
        }
        (true, true, false) => {
            tokio::select! {
                biased;
                command = receivers.control.recv() => command,
                command = receivers.priority.recv() => command,
            }
        }
        (true, false, true) => {
            tokio::select! {
                biased;
                command = receivers.control.recv() => command,
                command = receivers.data.recv() => command,
            }
        }
        (false, true, true) => {
            tokio::select! {
                biased;
                command = receivers.priority.recv() => command,
                command = receivers.data.recv() => command,
            }
        }
        (true, false, false) => receivers.control.recv().await,
        (false, true, false) => receivers.priority.recv().await,
        (false, false, true) => receivers.data.recv().await,
        (false, false, false) => None,
    }
}

pub(super) enum TcpPathSessionCommand {
    OpenStream {
        stream_id: StreamId,
        target: TargetAddr,
        ingress: IngressKind,
        lane: FlowLane,
        role: StreamOpenRole,
        session_commands: TcpPathSessionCommandSender,
        response: oneshot::Sender<Result<TcpPathStream, RuntimeError>>,
    },
    SendFrame(Frame),
    CloseStream(StreamId),
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
    response: oneshot::Sender<Result<TcpPathStream, RuntimeError>>,
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
    response: oneshot::Sender<Result<TcpPathStream, RuntimeError>>,
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
                    handle_disconnected_client_tcp_command(command, &runtime, &mut state).await;
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
                        if let Err(err) = handle_connected_client_tcp_command(
                            command,
                            state.connection.as_mut().expect("checked connected TCP path session"),
                            &mut state.streams,
                            &mut state.closed_streams,
                            runtime.stream_frame_queue,
                            runtime.mux_limits,
                        )
                        .await
                        {
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
                    eprintln!("warning: TCP stream open on new path session failed: {err}");
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
                let stream = TcpPathStream {
                    stream_id,
                    max_offset,
                    lane: pending.lane,
                    underlay: UnderlayProtocol::Tcp,
                    max_frame_payload_bytes: tcp_relay_buffer_len(mux_limits),
                    output: TcpPathStreamOutput::Fixed(pending.session_commands),
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
        Frame::StreamAck { stream_id, ranges } => {
            route_client_tcp_stream_frame(
                streams,
                closed_streams,
                stream_id,
                Frame::StreamAck { stream_id, ranges },
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
        .max_tcp_relay_chunk_bytes
        .min(mux_limits.max_payload_bytes)
        .max(1);
    let inflight_frames = mux_limits
        .max_tcp_path_inflight_bytes
        .saturating_add(frame_payload - 1)
        / frame_payload;
    inflight_frames
        .saturating_add(4)
        .clamp(4, tcp_path_session_frame_queue(mux_limits).max(4))
}

pub(super) fn tcp_path_session_frame_queue(mux_limits: MuxLimits) -> usize {
    tcp_stream_frame_queue(mux_limits)
        .saturating_mul(4)
        .clamp(16, 4096)
}

pub(super) fn tcp_stream_frame_queue(mux_limits: MuxLimits) -> usize {
    let frame_payload = mux_limits
        .max_tcp_relay_chunk_bytes
        .min(mux_limits.max_payload_bytes)
        .max(1);
    (mux_limits.max_reorder_bytes / frame_payload)
        .saturating_add(4)
        .clamp(4, 1024)
}
