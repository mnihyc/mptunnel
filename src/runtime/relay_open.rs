use super::*;

pub(super) struct OpenedRemoteStream {
    pub(super) stream: TcpPathStream,
    pub(super) path_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct RelayPathKey {
    pub(super) underlay: UnderlayProtocol,
    pub(super) index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct RelayPathInstance {
    pub(super) key: RelayPathKey,
    pub(super) id: u64,
}

pub(super) struct TcpRelayRemotePath {
    pub(super) path_index: usize,
    pub(super) instance_id: u64,
    pub(super) stream: TcpPathStreamHandle,
}

impl TcpRelayRemotePath {
    pub(super) fn key(&self) -> RelayPathKey {
        RelayPathKey {
            underlay: self.stream.underlay,
            index: self.path_index,
        }
    }

    pub(super) fn instance(&self) -> RelayPathInstance {
        RelayPathInstance {
            key: self.key(),
            id: self.instance_id,
        }
    }
}

pub(super) struct TcpRelayRemoteFrame {
    pub(super) instance: RelayPathInstance,
    pub(super) frame: Result<Frame, RuntimeError>,
}

pub(super) struct TcpRelayRemoteSet {
    stream_id: StreamId,
    pub(super) paths: Vec<TcpRelayRemotePath>,
    frames_tx: mpsc::Sender<TcpRelayRemoteFrame>,
    frames_rx: mpsc::Receiver<TcpRelayRemoteFrame>,
    next_send_index: usize,
    next_instance_id: u64,
}

impl TcpRelayRemoteSet {
    pub(super) fn new(opened: OpenedRemoteStream, frame_queue: usize) -> Self {
        let stream_id = opened.stream.stream_id;
        let (frames_tx, frames_rx) = mpsc::channel(frame_queue);
        let mut set = Self {
            stream_id,
            paths: Vec::new(),
            frames_tx,
            frames_rx,
            next_send_index: 0,
            next_instance_id: 0,
        };
        set.attach(opened);
        set
    }

    pub(super) fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub(super) fn primary_path_key(&self) -> Option<RelayPathKey> {
        self.paths.first().map(|path| path.key())
    }

    pub(super) fn active_path_instance(&self) -> Option<RelayPathInstance> {
        self.paths.last().map(TcpRelayRemotePath::instance)
    }

    pub(super) fn active_path_key(&self) -> Option<RelayPathKey> {
        self.active_path_instance().map(|instance| instance.key)
    }

    pub(super) fn active_path_index_for(&self, underlay: UnderlayProtocol) -> Option<usize> {
        self.paths
            .iter()
            .rev()
            .find(|path| path.stream.underlay == underlay)
            .map(|path| path.path_index)
    }

    pub(super) fn active_carrier_underlay(&self) -> Option<UnderlayProtocol> {
        self.paths.last().map(|path| path.stream.underlay)
    }

    pub(super) fn contains_path_key(&self, key: RelayPathKey) -> bool {
        self.paths.iter().any(|path| path.key() == key)
    }

    pub(super) fn path_keys(&self) -> Vec<RelayPathKey> {
        self.paths.iter().map(TcpRelayRemotePath::key).collect()
    }

    pub(super) fn set_lane(&mut self, lane: FlowLane) {
        for path in &mut self.paths {
            path.stream.lane = lane;
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    pub(super) fn max_offset(&self) -> u64 {
        self.paths
            .iter()
            .map(|path| path.stream.max_offset)
            .max()
            .unwrap_or(0)
    }

    pub(super) fn max_frame_payload_bytes(&self, mux_limits: MuxLimits) -> usize {
        self.paths
            .iter()
            .map(|path| path.stream.max_frame_payload_bytes)
            .min()
            .unwrap_or_else(|| tcp_relay_buffer_len(mux_limits))
            .max(1)
    }

    pub(super) fn fin_requires_repair_drain(&self) -> bool {
        self.paths
            .iter()
            .any(|path| path.stream.underlay == UnderlayProtocol::Udp)
    }

    pub(super) fn attach(&mut self, opened: OpenedRemoteStream) {
        self.attach_with_placement(opened, RelayPathPlacement::Active);
    }

    pub(super) fn attach_for_repair(&mut self, opened: OpenedRemoteStream) {
        self.attach_with_placement(opened, RelayPathPlacement::PreserveActive);
    }

    fn attach_with_placement(&mut self, opened: OpenedRemoteStream, placement: RelayPathPlacement) {
        let path_index = opened.path_index;
        let underlay = opened.stream.underlay;
        let key = RelayPathKey {
            underlay,
            index: path_index,
        };
        if self.contains_path_key(key) {
            return;
        }
        let instance_id = self.next_instance_id;
        self.next_instance_id = self.next_instance_id.wrapping_add(1);
        let instance = RelayPathInstance {
            key,
            id: instance_id,
        };
        let (stream, mut frames) = opened.stream.into_handle_and_frames();
        let frames_tx = self.frames_tx.clone();
        tokio::spawn(async move {
            while let Some(frame) = frames.recv().await {
                let done = frame.is_err();
                if frames_tx
                    .send(TcpRelayRemoteFrame { instance, frame })
                    .await
                    .is_err()
                    || done
                {
                    return;
                }
            }
            let _ = frames_tx
                .send(TcpRelayRemoteFrame {
                    instance,
                    frame: Err(RuntimeError::TcpPathSessionClosed),
                })
                .await;
        });
        let path = TcpRelayRemotePath {
            path_index,
            instance_id,
            stream,
        };
        let insert_at = match placement {
            RelayPathPlacement::Active => self.paths.len(),
            RelayPathPlacement::PreserveActive if self.paths.is_empty() => self.paths.len(),
            RelayPathPlacement::PreserveActive => self.paths.len() - 1,
        };
        if insert_at < self.paths.len() && self.next_send_index >= insert_at {
            self.next_send_index = self.next_send_index.saturating_add(1);
        }
        self.paths.insert(insert_at, path);
        if !self.paths.is_empty() {
            self.next_send_index %= self.paths.len();
        }
    }

    pub(super) async fn recv_frame(&mut self) -> Result<TcpRelayRemoteFrame, RuntimeError> {
        self.frames_rx
            .recv()
            .await
            .ok_or(RuntimeError::TcpPathSessionClosed)
    }

    pub(super) async fn send_frame(
        &mut self,
        context: &ClientPathContext,
        frame: Frame,
    ) -> Result<RelayPathKey, RuntimeError> {
        self.send_frame_with_avoid(context, frame, &[]).await
    }

    pub(super) async fn send_repair_frame(
        &mut self,
        context: &ClientPathContext,
        frame: Frame,
        avoid_keys: &[RelayPathKey],
    ) -> Result<RelayPathKey, RuntimeError> {
        self.send_frame_with_avoid(context, frame, avoid_keys).await
    }

    async fn send_frame_with_avoid(
        &mut self,
        context: &ClientPathContext,
        frame: Frame,
        avoid_keys: &[RelayPathKey],
    ) -> Result<RelayPathKey, RuntimeError> {
        let mut last_error = None;
        let stream_lane = self
            .paths
            .last()
            .map(|path| path.stream.lane)
            .unwrap_or(FlowLane::Latency);
        let prefer_current_data_path =
            tcp_relay_frame_prefers_current_data_path(&frame, stream_lane);
        while !self.paths.is_empty() {
            if let Some(position) = choose_bulk_relay_path_avoiding(
                context,
                &self.paths,
                stream_lane,
                &frame,
                self.next_send_index,
                avoid_keys,
            ) {
                self.next_send_index = position;
            } else if prefer_current_data_path
                || self.paths.last().is_some_and(|path| {
                    tcp_path_frame_uses_priority_queue(tcp_path_effective_frame_lane(
                        &frame,
                        path.stream.lane,
                    ))
                })
            {
                self.next_send_index = self.paths.len() - 1;
            }
            self.next_send_index %= self.paths.len();
            if avoid_keys.contains(&self.paths[self.next_send_index].key())
                && let Some(position) = self
                    .paths
                    .iter()
                    .position(|path| !avoid_keys.contains(&path.key()))
            {
                self.next_send_index = position;
            }
            let instance = self.paths[self.next_send_index].instance();
            match self.paths[self.next_send_index]
                .stream
                .send_frame(frame.clone())
                .await
            {
                Ok(()) => {
                    let sent_bytes = reliable_stream_frame_payload_bytes(&frame);
                    if relay_frame_is_bulk_stream_data(&frame, stream_lane) {
                        context.record_relay_path_send(
                            instance.key.underlay,
                            instance.key.index,
                            sent_bytes,
                        );
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "scheduler_decision",
                            format_args!(
                                "stream_id={} lane={:?} path_underlay={:?} path_index={} payload_bytes={} reason=bulk_stripe",
                                self.stream_id.0,
                                stream_lane,
                                instance.key.underlay,
                                instance.key.index,
                                sent_bytes,
                            ),
                        );
                    }
                    if !prefer_current_data_path
                        && !tcp_path_frame_uses_priority_queue(tcp_path_effective_frame_lane(
                            &frame,
                            self.paths[self.next_send_index].stream.lane,
                        ))
                    {
                        self.next_send_index = (self.next_send_index + 1) % self.paths.len();
                    }
                    return Ok(instance.key);
                }
                Err(err) => {
                    last_error = Some(err);
                    self.fail_path_instance(context, instance).await;
                }
            }
        }
        Err(last_error.unwrap_or(RuntimeError::TcpPathSessionClosed))
    }

    pub(super) async fn reannounce_active_path(
        &mut self,
        context: &ClientPathContext,
        spec: &TcpRelayOpenSpec,
        lane: FlowLane,
    ) -> Result<(), RuntimeError> {
        let Some(position) = self.paths.len().checked_sub(1) else {
            return Err(RuntimeError::TcpPathSessionClosed);
        };
        let instance = self.paths[position].instance();
        let output = self.paths[position].stream.output.clone();
        self.paths[position].stream.lane = lane;
        let frame = Frame::OpenStream {
            stream_id: self.stream_id,
            target: spec.target.clone(),
            ingress: spec.ingress,
            outbound: OutboundPolicy::Direct,
            role: StreamOpenRole::Active,
        };
        match output
            .send_frame(self.stream_id, FlowLane::Control, frame)
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => {
                self.fail_path_instance(context, instance).await;
                Err(err)
            }
        }
    }

    pub(super) async fn close_all(&mut self) {
        let paths = std::mem::take(&mut self.paths);
        for path in paths {
            path.stream.close().await;
        }
        self.next_send_index = 0;
    }

    pub(super) async fn fail_path_instance(
        &mut self,
        context: &ClientPathContext,
        instance: RelayPathInstance,
    ) -> bool {
        let Some(path) = self.remove_path_instance(instance) else {
            return false;
        };
        context.mark_relay_path_failure(path.stream.underlay, path.path_index);
        context.release_relay_path_load(path.stream.underlay, path.path_index, path.stream.lane);
        path.stream.close().await;
        true
    }

    pub(super) async fn fail_path_key(
        &mut self,
        context: &ClientPathContext,
        key: RelayPathKey,
    ) -> bool {
        let Some(path) = self.remove_path_key(key) else {
            return false;
        };
        context.mark_relay_path_failure(path.stream.underlay, path.path_index);
        context.release_relay_path_load(path.stream.underlay, path.path_index, path.stream.lane);
        path.stream.close().await;
        true
    }

    pub(super) fn remove_path_instance(
        &mut self,
        instance: RelayPathInstance,
    ) -> Option<TcpRelayRemotePath> {
        let position = self
            .paths
            .iter()
            .position(|path| path.instance() == instance)?;
        self.remove_path_at(position)
    }

    pub(super) fn remove_path_key(&mut self, key: RelayPathKey) -> Option<TcpRelayRemotePath> {
        let position = self.paths.iter().position(|path| path.key() == key)?;
        self.remove_path_at(position)
    }

    pub(super) fn remove_path_at(&mut self, position: usize) -> Option<TcpRelayRemotePath> {
        let path = self.paths.remove(position);
        if self.paths.is_empty() {
            self.next_send_index = 0;
        } else {
            self.next_send_index %= self.paths.len();
        }
        Some(path)
    }

    pub(super) fn promote_path_instance_to_active(&mut self, instance: RelayPathInstance) -> bool {
        let Some(position) = self
            .paths
            .iter()
            .position(|path| path.instance() == instance)
        else {
            return false;
        };
        if position + 1 == self.paths.len() {
            return false;
        }
        let path = self.paths.remove(position);
        self.paths.push(path);
        self.next_send_index = 0;
        true
    }

    pub(super) async fn replay_repair_cache_to_instance(
        &mut self,
        instance: RelayPathInstance,
        send_stream: &ReliableSendStream,
        resend_fin: bool,
        byte_limit: usize,
    ) -> Result<bool, RuntimeError> {
        let Some(position) = self
            .paths
            .iter()
            .position(|path| path.instance() == instance)
        else {
            return Ok(false);
        };
        for frame in send_stream.retransmission_frames_limited(byte_limit) {
            self.paths[position].stream.send_frame(frame).await?;
        }
        if resend_fin {
            self.paths[position]
                .stream
                .send_frame(Frame::StreamFin {
                    stream_id: self.stream_id,
                    final_offset: send_stream.next_offset(),
                })
                .await?;
        }
        Ok(true)
    }
}

#[derive(Debug, Clone, Copy)]
enum RelayPathPlacement {
    Active,
    PreserveActive,
}

#[derive(Clone)]
pub(super) struct TcpRelayOpenSpec {
    pub(super) target: TargetAddr,
    pub(super) ingress: IngressKind,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum TcpRelayAttachMode {
    Any,
    BulkStriping,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct UdpStreamOpenOptions {
    pub(super) wait_for_accept: bool,
    pub(super) role: StreamOpenRole,
}

impl UdpStreamOpenOptions {
    pub(super) const ACTIVE_WAIT: Self = Self {
        wait_for_accept: true,
        role: StreamOpenRole::Active,
    };
}

pub(super) async fn open_remote_stream(
    context: &ClientPathContext,
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
) -> Result<OpenedRemoteStream, RuntimeError> {
    let stream_id = context.allocate_tcp_stream_id()?;
    open_remote_stream_with_id(context, stream_id, target, ingress, lane).await
}

pub(super) async fn open_remote_stream_with_id(
    context: &ClientPathContext,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
) -> Result<OpenedRemoteStream, RuntimeError> {
    if context.tcp_paths.is_empty() {
        return open_remote_stream_with_id_over_udp(context, stream_id, target, ingress, lane)
            .await;
    }
    let mut attempted = Vec::new();
    let mut last_retryable_error = None;
    while attempted.len() < context.tcp_paths.len() {
        let Some(path_index) =
            context.reserve_tcp_stream_path(lane, PATH_OPEN_SCORE_BYTES, &attempted)
        else {
            break;
        };
        attempted.push(path_index);
        match open_remote_stream_on_reserved_path(
            context,
            stream_id,
            target.clone(),
            ingress,
            lane,
            path_index,
            StreamOpenRole::Active,
        )
        .await
        {
            Ok(opened) => return Ok(opened),
            Err(err) if stream_open_error_is_path_retryable(&err) => {
                context.release_tcp_path_load(path_index, lane);
                context.mark_tcp_path_failure(path_index);
                last_retryable_error = Some(err);
            }
            Err(err) => {
                context.release_tcp_path_load(path_index, lane);
                return Err(err);
            }
        }
    }
    Err(last_retryable_error.unwrap_or(RuntimeError::NoSchedulableTcpPath))
}

pub(super) async fn open_remote_stream_on_path(
    context: &ClientPathContext,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
    path_index: usize,
    role: StreamOpenRole,
) -> Result<OpenedRemoteStream, RuntimeError> {
    context.reserve_tcp_path_load(path_index, lane);
    match open_remote_stream_on_reserved_path(
        context, stream_id, target, ingress, lane, path_index, role,
    )
    .await
    {
        Ok(opened) => Ok(opened),
        Err(err) => {
            context.release_tcp_path_load(path_index, lane);
            Err(err)
        }
    }
}

pub(super) async fn open_remote_stream_on_reserved_path(
    context: &ClientPathContext,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
    path_index: usize,
    role: StreamOpenRole,
) -> Result<OpenedRemoteStream, RuntimeError> {
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "reliable_stream_open_attempt",
        format_args!(
            "stream_id={} underlay=tcp path_index={} lane={:?} role={:?} wait_for_accept=true tcp_paths={} udp_paths={}",
            stream_id.0,
            path_index,
            lane,
            role,
            context.tcp_paths.len(),
            context.udp_paths.len(),
        ),
    );
    let started_at = Instant::now();
    let stream = context
        .tcp_sessions
        .get(path_index)
        .ok_or(RuntimeError::NoSchedulableTcpPath)?
        .open_stream(stream_id, target, ingress, lane, role)
        .await?;
    let elapsed = started_at.elapsed();
    context.mark_tcp_path_reserved_open_success(path_index, elapsed);
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "reliable_stream_open_success",
        format_args!(
            "stream_id={} underlay=tcp path_index={} lane={:?} role={:?} elapsed_ms={:.3}",
            stream_id.0,
            path_index,
            lane,
            role,
            elapsed.as_secs_f64() * 1000.0,
        ),
    );
    Ok(OpenedRemoteStream { stream, path_index })
}

pub(super) async fn open_remote_stream_with_id_over_udp(
    context: &ClientPathContext,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
) -> Result<OpenedRemoteStream, RuntimeError> {
    if context.udp_paths.is_empty() {
        return Err(RuntimeError::NoTcpPath);
    }
    let mut attempted = Vec::new();
    let mut last_retryable_error = None;
    while attempted.len() < context.udp_paths.len() {
        let Some(path_index) =
            context.reserve_udp_stream_path(lane, PATH_OPEN_SCORE_BYTES, &attempted)
        else {
            break;
        };
        attempted.push(path_index);
        match open_remote_stream_on_reserved_udp_path(
            context,
            stream_id,
            target.clone(),
            ingress,
            lane,
            path_index,
            UdpStreamOpenOptions::ACTIVE_WAIT,
        )
        .await
        {
            Ok(opened) => return Ok(opened),
            Err(err) if udp_stream_open_error_is_path_retryable(&err) => {
                context.release_udp_stream_path_load(path_index, lane);
                context.mark_udp_path_failure(path_index);
                last_retryable_error = Some(err);
            }
            Err(err) => {
                context.release_udp_stream_path_load(path_index, lane);
                return Err(err);
            }
        }
    }
    Err(last_retryable_error.unwrap_or(RuntimeError::NoSchedulableUdpPath))
}

pub(super) async fn open_remote_stream_on_udp_path(
    context: &ClientPathContext,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
    path_index: usize,
    options: UdpStreamOpenOptions,
) -> Result<OpenedRemoteStream, RuntimeError> {
    if context.udp_paths.get(path_index).is_none() {
        return Err(RuntimeError::NoSchedulableUdpPath);
    }
    context.reserve_udp_stream_path_load(path_index, lane);
    match open_remote_stream_on_reserved_udp_path(
        context, stream_id, target, ingress, lane, path_index, options,
    )
    .await
    {
        Ok(opened) => Ok(opened),
        Err(err) => {
            context.release_udp_stream_path_load(path_index, lane);
            Err(err)
        }
    }
}

pub(super) async fn open_remote_stream_on_reserved_udp_path(
    context: &ClientPathContext,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
    path_index: usize,
    options: UdpStreamOpenOptions,
) -> Result<OpenedRemoteStream, RuntimeError> {
    let UdpStreamOpenOptions {
        wait_for_accept,
        role,
    } = options;
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "reliable_stream_open_attempt",
        format_args!(
            "stream_id={} underlay=udp path_index={} lane={:?} role={:?} wait_for_accept={} tcp_paths={} udp_paths={}",
            stream_id.0,
            path_index,
            lane,
            role,
            wait_for_accept,
            context.tcp_paths.len(),
            context.udp_paths.len(),
        ),
    );
    let started_at = Instant::now();
    let _udp_open_waits_for_accept = wait_for_accept;
    let stream = context
        .udp_sessions
        .get(path_index)
        .ok_or(RuntimeError::NoSchedulableUdpPath)?
        .open_stream(stream_id, target, ingress, lane, role)
        .await?;
    let elapsed = started_at.elapsed();
    context.mark_udp_stream_reserved_open_success(path_index, elapsed);
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "reliable_stream_open_success",
        format_args!(
            "stream_id={} underlay=udp path_index={} lane={:?} role={:?} wait_for_accept={} elapsed_ms={:.3}",
            stream_id.0,
            path_index,
            lane,
            role,
            wait_for_accept,
            elapsed.as_secs_f64() * 1000.0,
        ),
    );
    Ok(OpenedRemoteStream { stream, path_index })
}

pub(super) fn authenticated_path_join_frames(
    security: &SecurityConfig,
    path: &PathSpec,
    path_id: PathId,
    underlay: UnderlayProtocol,
) -> Result<(Frame, Frame, Frame), RuntimeError> {
    let session_id = random_session_id()?;
    authenticated_path_join_frames_for_session(security, path, path_id, underlay, session_id)
}

pub(super) fn authenticated_path_join_frames_for_session(
    security: &SecurityConfig,
    path: &PathSpec,
    path_id: PathId,
    underlay: UnderlayProtocol,
    session_id: SessionId,
) -> Result<(Frame, Frame, Frame), RuntimeError> {
    let authenticator = SessionAuthenticator::new(security.secret.as_bytes())?;
    let issued_at_unix_secs = current_unix_secs()?;
    let session_nonce = random_nonce()?;
    let session_tag =
        authenticator.session_auth_tag(session_id, session_nonce, issued_at_unix_secs);
    let path_nonce = random_nonce()?;
    let capabilities = path.metadata.capabilities;
    let path_tag = authenticator.path_join_tag(
        session_id,
        path_id,
        underlay,
        path_nonce,
        issued_at_unix_secs,
        capabilities,
    );
    Ok((
        Frame::SessionHello { session_id },
        Frame::SessionAuth {
            session_id,
            nonce: session_nonce,
            issued_at_unix_secs,
            auth_tag: session_tag,
        },
        Frame::PathJoin {
            session_id,
            path_id,
            underlay,
            nonce: path_nonce,
            issued_at_unix_secs,
            capabilities,
            auth_tag: path_tag,
        },
    ))
}

pub(super) fn stream_open_error_is_path_retryable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Tcp(_)
            | RuntimeError::Encrypted(_)
            | RuntimeError::Auth(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::TcpPathSessionClosed
            | RuntimeError::PathHeartbeatTimeout
            | RuntimeError::Protocol(_)
    )
}

pub(super) fn udp_stream_open_error_is_path_retryable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Udp(_)
            | RuntimeError::UdpCarrierTransport(_)
            | RuntimeError::UdpCarrierFrame(_)
            | RuntimeError::UdpCarrierConnection(_)
            | RuntimeError::Auth(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
    )
}

pub(super) fn relay_error_is_tcp_path_failure<T>(result: &Result<T, RuntimeError>) -> bool {
    matches!(
        result,
        Err(RuntimeError::PathHeartbeatTimeout)
            | Err(RuntimeError::TcpPathSessionClosed)
            | Err(RuntimeError::Tcp(_))
            | Err(RuntimeError::Encrypted(_))
            | Err(RuntimeError::RemoteClosed(_))
            | Err(RuntimeError::Protocol(_))
    )
}
