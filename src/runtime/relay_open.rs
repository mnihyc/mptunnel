use super::*;

pub(super) struct OpenedRemoteStream {
    pub(super) stream: ReliablePathStream,
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

pub(super) struct ReliableRelayRemotePath {
    pub(super) path_index: usize,
    pub(super) instance_id: u64,
    pub(super) placement: RelayPathPlacement,
    pub(super) stream: ReliablePathStreamHandle,
}

impl ReliableRelayRemotePath {
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

pub(super) struct ReliableRelayRemoteFrame {
    pub(super) instance: RelayPathInstance,
    pub(super) frame: Result<Frame, RuntimeError>,
}

pub(super) struct ReliableRelayRemoteSet {
    stream_id: StreamId,
    pub(super) paths: Vec<ReliableRelayRemotePath>,
    frames_tx: mpsc::Sender<ReliableRelayRemoteFrame>,
    frames_rx: mpsc::Receiver<ReliableRelayRemoteFrame>,
    next_instance_id: u64,
}

impl ReliableRelayRemoteSet {
    pub(super) fn new(opened: OpenedRemoteStream, frame_queue: usize) -> Self {
        let stream_id = opened.stream.stream_id;
        let (frames_tx, frames_rx) = mpsc::channel(frame_queue);
        let mut set = Self {
            stream_id,
            paths: Vec::new(),
            frames_tx,
            frames_rx,
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
        self.active_path_position()
            .and_then(|position| self.paths.get(position))
            .map(ReliableRelayRemotePath::instance)
    }

    pub(super) fn active_path_key(&self) -> Option<RelayPathKey> {
        self.active_path_instance().map(|instance| instance.key)
    }

    pub(super) fn active_path_index_for(&self, underlay: UnderlayProtocol) -> Option<usize> {
        self.paths
            .iter()
            .rev()
            .find(|path| {
                path.stream.underlay == underlay && path.placement == RelayPathPlacement::Active
            })
            .or_else(|| {
                self.paths
                    .iter()
                    .rev()
                    .find(|path| path.stream.underlay == underlay)
            })
            .map(|path| path.path_index)
    }

    pub(super) fn active_carrier_underlay(&self) -> Option<UnderlayProtocol> {
        self.active_path_position()
            .and_then(|position| self.paths.get(position))
            .map(|path| path.stream.underlay)
    }

    pub(super) fn contains_path_key(&self, key: RelayPathKey) -> bool {
        self.paths.iter().any(|path| path.key() == key)
    }

    pub(super) fn path_keys(&self) -> Vec<RelayPathKey> {
        self.paths
            .iter()
            .map(ReliableRelayRemotePath::key)
            .collect()
    }

    #[cfg(test)]
    pub(super) fn path_instance_for_key(&self, key: RelayPathKey) -> Option<RelayPathInstance> {
        self.paths
            .iter()
            .find(|path| path.key() == key)
            .map(ReliableRelayRemotePath::instance)
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
            .unwrap_or_else(|| reliable_relay_buffer_len(mux_limits))
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
        self.attach_with_placement(opened, RelayPathPlacement::Repair);
    }

    pub(super) fn attach_for_validation(&mut self, opened: OpenedRemoteStream) {
        self.attach_with_placement(opened, RelayPathPlacement::Validation);
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
                    .send(ReliableRelayRemoteFrame { instance, frame })
                    .await
                    .is_err()
                    || done
                {
                    return;
                }
            }
            let _ = frames_tx
                .send(ReliableRelayRemoteFrame {
                    instance,
                    frame: Err(RuntimeError::TcpPathSessionClosed),
                })
                .await;
        });
        let path = ReliableRelayRemotePath {
            path_index,
            instance_id,
            placement,
            stream,
        };
        let insert_at = match placement {
            RelayPathPlacement::Active => self.paths.len(),
            RelayPathPlacement::Repair | RelayPathPlacement::Validation
                if self.paths.is_empty() =>
            {
                self.paths.len()
            }
            RelayPathPlacement::Repair | RelayPathPlacement::Validation => self.paths.len() - 1,
        };
        self.paths.insert(insert_at, path);
    }

    pub(super) async fn recv_frame(&mut self) -> Result<ReliableRelayRemoteFrame, RuntimeError> {
        self.frames_rx
            .recv()
            .await
            .ok_or(RuntimeError::TcpPathSessionClosed)
    }

    pub(super) fn has_buffered_frame(&self) -> bool {
        !self.frames_rx.is_empty()
    }

    pub(super) fn can_enqueue_work_lane_now(
        &self,
        work_lane: ReliableRelayQueuedWorkLane,
        relay_lane: FlowLane,
    ) -> bool {
        self.paths.len() == 1
            && self
                .paths
                .first()
                .is_some_and(|path| path.stream.can_enqueue_work_lane_now(work_lane, relay_lane))
    }

    pub(super) async fn close_all(&mut self) {
        let paths = std::mem::take(&mut self.paths);
        for path in paths {
            path.stream.send_detach().await;
            path.stream.close().await;
        }
    }

    pub(super) async fn fail_path_instance(
        &mut self,
        context: &ClientPathContext,
        instance: RelayPathInstance,
    ) -> bool {
        let Some(path) = self.remove_path_instance(instance) else {
            return false;
        };
        context.mark_relay_path_data_plane_failure(path.stream.underlay, path.path_index);
        context.release_relay_path_load(path.stream.underlay, path.path_index, path.stream.lane);
        path.stream.send_detach().await;
        path.stream.close().await;
        true
    }

    pub(super) fn remove_path_instance(
        &mut self,
        instance: RelayPathInstance,
    ) -> Option<ReliableRelayRemotePath> {
        let position = self
            .paths
            .iter()
            .position(|path| path.instance() == instance)?;
        self.remove_path_at(position)
    }

    pub(super) fn remove_path_at(&mut self, position: usize) -> Option<ReliableRelayRemotePath> {
        let path = self.paths.remove(position);
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
        let mut path = self.paths.remove(position);
        path.placement = RelayPathPlacement::Active;
        self.paths.push(path);
        true
    }

    fn active_path_position(&self) -> Option<usize> {
        self.paths
            .iter()
            .rposition(|path| path.placement == RelayPathPlacement::Active)
            .or_else(|| self.paths.len().checked_sub(1))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RelayPathPlacement {
    Active,
    Repair,
    Validation,
}

#[derive(Clone)]
pub(super) struct ReliableRelayOpenSpec {
    pub(super) target: TargetAddr,
    pub(super) ingress: IngressKind,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum ReliableRelayAttachMode {
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
    let mut attempted = Vec::new();
    let mut last_retryable_error = None;
    let candidate_count = context
        .tcp_paths
        .len()
        .saturating_add(context.udp_paths.len());
    while attempted.len() < candidate_count {
        let Some(key) =
            context.reserve_reliable_stream_path(lane, PATH_OPEN_SCORE_BYTES, &attempted)
        else {
            break;
        };
        attempted.push(key);
        let open_result = match key.underlay {
            UnderlayProtocol::Tcp => {
                open_remote_stream_on_reserved_path(
                    context,
                    stream_id,
                    target.clone(),
                    ingress,
                    lane,
                    key.index,
                    StreamOpenRole::Active,
                )
                .await
            }
            UnderlayProtocol::Udp => {
                open_remote_stream_on_reserved_udp_path(
                    context,
                    stream_id,
                    target.clone(),
                    ingress,
                    lane,
                    key.index,
                    UdpStreamOpenOptions::ACTIVE_WAIT,
                )
                .await
            }
        };
        match open_result {
            Ok(opened) => return Ok(opened),
            Err(err) if relay_path_open_error_is_retryable(key.underlay, &err) => {
                context.release_relay_path_load(key.underlay, key.index, lane);
                context.mark_relay_path_failure(key.underlay, key.index);
                last_retryable_error = Some(err);
            }
            Err(err) => {
                context.release_relay_path_load(key.underlay, key.index, lane);
                return Err(err);
            }
        }
    }
    Err(last_retryable_error.unwrap_or_else(|| no_schedulable_reliable_path_error(context)))
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
    let open_timeout = reliable_relay_attach_open_timeout(
        context,
        RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: path_index,
        },
        lane,
    );
    let open_result = tokio::time::timeout(
        open_timeout,
        open_remote_stream_on_reserved_path(
            context, stream_id, target, ingress, lane, path_index, role,
        ),
    )
    .await;
    match open_result {
        Ok(Ok(opened)) => Ok(opened),
        Ok(Err(err)) => {
            context.release_tcp_path_load(path_index, lane);
            Err(err)
        }
        Err(_) => {
            context.release_tcp_path_load(path_index, lane);
            context.mark_tcp_path_data_plane_failure(path_index);
            if let Some(session) = context.tcp_sessions.get(path_index) {
                session.cancel_stream_open(lane, stream_id).await;
            }
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "reliable_stream_open_timeout",
                format_args!(
                    "stream_id={} underlay=tcp path_index={} lane={:?} role={:?} timeout_ms={}",
                    stream_id.0,
                    path_index,
                    lane,
                    role,
                    open_timeout.as_millis(),
                ),
            );
            Err(RuntimeError::PathOpenTimedOut)
        }
    }
}

pub(super) fn reliable_relay_attach_open_timeout(
    context: &ClientPathContext,
    key: RelayPathKey,
    lane: FlowLane,
) -> Duration {
    reliable_relay_stall_timeout(relay_path_snapshot(context, key), lane)
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
    send_open_path_metrics(context, &stream, UnderlayProtocol::Tcp, path_index).await?;
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
    send_open_path_metrics(context, &stream, UnderlayProtocol::Udp, path_index).await?;
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

async fn send_open_path_metrics(
    context: &ClientPathContext,
    stream: &ReliablePathStream,
    underlay: UnderlayProtocol,
    path_index: usize,
) -> Result<(), RuntimeError> {
    let Some(metrics) = context.relay_path_metrics(underlay, path_index) else {
        return Ok(());
    };
    send_sender_service_control_frame(stream, Frame::PathMetrics { metrics })
        .await
        .map(|_| ())
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
            | RuntimeError::PathOpenTimedOut
            | RuntimeError::Protocol(_)
    )
}

pub(super) fn udp_stream_open_error_is_path_retryable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Udp(_)
            | RuntimeError::QuicCarrier(_)
            | RuntimeError::Auth(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
    )
}

pub(super) fn relay_error_is_tcp_path_failure<T>(result: &Result<T, RuntimeError>) -> bool {
    matches!(
        result,
        Err(RuntimeError::PathHeartbeatTimeout)
            | Err(RuntimeError::PathOpenTimedOut)
            | Err(RuntimeError::TcpPathSessionClosed)
            | Err(RuntimeError::Tcp(_))
            | Err(RuntimeError::Encrypted(_))
            | Err(RuntimeError::RemoteClosed(_))
            | Err(RuntimeError::Protocol(_))
    )
}
