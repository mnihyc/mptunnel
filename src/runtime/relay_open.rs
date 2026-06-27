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

    pub(super) fn set_class(&mut self, class: TrafficClass) {
        for path in &mut self.paths {
            path.stream.class = class;
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
        let mut last_error = None;
        let prefer_current_data_path = tcp_relay_frame_prefers_current_data_path(&frame);
        while !self.paths.is_empty() {
            if prefer_current_data_path
                || self
                    .paths
                    .last()
                    .is_some_and(|path| tcp_path_frame_uses_priority_queue(path.stream.class))
            {
                self.next_send_index = self.paths.len() - 1;
            }
            self.next_send_index %= self.paths.len();
            let instance = self.paths[self.next_send_index].instance();
            match self.paths[self.next_send_index]
                .stream
                .send_frame(frame.clone())
                .await
            {
                Ok(()) => {
                    if !prefer_current_data_path
                        && !tcp_path_frame_uses_priority_queue(
                            self.paths[self.next_send_index].stream.class,
                        )
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
        class: TrafficClass,
    ) -> Result<(), RuntimeError> {
        let Some(position) = self.paths.len().checked_sub(1) else {
            return Err(RuntimeError::TcpPathSessionClosed);
        };
        let instance = self.paths[position].instance();
        let output = self.paths[position].stream.output.clone();
        self.paths[position].stream.class = class;
        let frame = Frame::OpenStream {
            stream_id: self.stream_id,
            target: spec.target.clone(),
            ingress: spec.ingress,
            outbound: OutboundPolicy::Direct,
            class,
            role: StreamOpenRole::Active,
        };
        match output
            .send_frame(self.stream_id, TrafficClass::Control, frame)
            .await
        {
            Ok(()) => Ok(()),
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
        context.release_relay_path_load(path.stream.underlay, path.path_index, path.stream.class);
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
        context.release_relay_path_load(path.stream.underlay, path.path_index, path.stream.class);
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
    ) -> Result<bool, RuntimeError> {
        let Some(position) = self
            .paths
            .iter()
            .position(|path| path.instance() == instance)
        else {
            return Ok(false);
        };
        for frame in send_stream.retransmission_frames() {
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
    AutoBulkDiscovery,
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
    class: TrafficClass,
) -> Result<OpenedRemoteStream, RuntimeError> {
    let stream_id = context.allocate_tcp_stream_id()?;
    open_remote_stream_with_id(context, stream_id, target, ingress, class).await
}

pub(super) async fn open_remote_stream_with_id(
    context: &ClientPathContext,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    class: TrafficClass,
) -> Result<OpenedRemoteStream, RuntimeError> {
    if context.tcp_paths.is_empty() {
        return open_remote_stream_with_id_over_udp(context, stream_id, target, ingress, class)
            .await;
    }
    let mut attempted = Vec::new();
    let mut last_retryable_error = None;
    while attempted.len() < context.tcp_paths.len() {
        let Some(path_index) =
            context.reserve_tcp_stream_path(class, PATH_OPEN_SCORE_BYTES, &attempted)
        else {
            break;
        };
        attempted.push(path_index);
        match open_remote_stream_on_reserved_path(
            context,
            stream_id,
            target.clone(),
            ingress,
            class,
            path_index,
            StreamOpenRole::Active,
        )
        .await
        {
            Ok(opened) => return Ok(opened),
            Err(err) if stream_open_error_is_path_retryable(&err) => {
                context.release_tcp_path_load(path_index, class);
                context.mark_tcp_path_failure(path_index);
                last_retryable_error = Some(err);
            }
            Err(err) => {
                context.release_tcp_path_load(path_index, class);
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
    class: TrafficClass,
    path_index: usize,
    role: StreamOpenRole,
) -> Result<OpenedRemoteStream, RuntimeError> {
    context.reserve_tcp_path_load(path_index, class);
    match open_remote_stream_on_reserved_path(
        context, stream_id, target, ingress, class, path_index, role,
    )
    .await
    {
        Ok(opened) => Ok(opened),
        Err(err) => {
            context.release_tcp_path_load(path_index, class);
            Err(err)
        }
    }
}

pub(super) async fn open_remote_stream_on_reserved_path(
    context: &ClientPathContext,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    class: TrafficClass,
    path_index: usize,
    role: StreamOpenRole,
) -> Result<OpenedRemoteStream, RuntimeError> {
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "reliable_stream_open_attempt",
        format_args!(
            "stream_id={} underlay=tcp path_index={} class={:?} role={:?} wait_for_accept=true tcp_paths={} udp_paths={}",
            stream_id.0,
            path_index,
            class,
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
        .open_stream(stream_id, target, ingress, class, role)
        .await?;
    let elapsed = started_at.elapsed();
    context.mark_tcp_path_reserved_open_success(path_index, elapsed);
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "reliable_stream_open_success",
        format_args!(
            "stream_id={} underlay=tcp path_index={} class={:?} role={:?} elapsed_ms={:.3}",
            stream_id.0,
            path_index,
            class,
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
    class: TrafficClass,
) -> Result<OpenedRemoteStream, RuntimeError> {
    if context.udp_paths.is_empty() {
        return Err(RuntimeError::NoTcpPath);
    }
    let mut attempted = Vec::new();
    let mut last_retryable_error = None;
    while attempted.len() < context.udp_paths.len() {
        let Some(path_index) =
            context.reserve_udp_stream_path(class, PATH_OPEN_SCORE_BYTES, &attempted)
        else {
            break;
        };
        attempted.push(path_index);
        match open_remote_stream_on_reserved_udp_path(
            context,
            stream_id,
            target.clone(),
            ingress,
            class,
            path_index,
            UdpStreamOpenOptions::ACTIVE_WAIT,
        )
        .await
        {
            Ok(opened) => return Ok(opened),
            Err(err) if udp_stream_open_error_is_path_retryable(&err) => {
                context.release_udp_stream_path_load(path_index, class);
                context.mark_udp_path_failure(path_index);
                last_retryable_error = Some(err);
            }
            Err(err) => {
                context.release_udp_stream_path_load(path_index, class);
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
    class: TrafficClass,
    path_index: usize,
    options: UdpStreamOpenOptions,
) -> Result<OpenedRemoteStream, RuntimeError> {
    if context.udp_paths.get(path_index).is_none() {
        return Err(RuntimeError::NoSchedulableUdpPath);
    }
    context.reserve_udp_stream_path_load(path_index, class);
    match open_remote_stream_on_reserved_udp_path(
        context, stream_id, target, ingress, class, path_index, options,
    )
    .await
    {
        Ok(opened) => Ok(opened),
        Err(err) => {
            context.release_udp_stream_path_load(path_index, class);
            Err(err)
        }
    }
}

pub(super) async fn open_remote_stream_on_reserved_udp_path(
    context: &ClientPathContext,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    class: TrafficClass,
    path_index: usize,
    options: UdpStreamOpenOptions,
) -> Result<OpenedRemoteStream, RuntimeError> {
    let path = context
        .udp_paths
        .get(path_index)
        .ok_or(RuntimeError::NoSchedulableUdpPath)?;
    let UdpStreamOpenOptions {
        wait_for_accept,
        role,
    } = options;
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "reliable_stream_open_attempt",
        format_args!(
            "stream_id={} underlay=udp path_index={} class={:?} role={:?} wait_for_accept={} tcp_paths={} udp_paths={}",
            stream_id.0,
            path_index,
            class,
            role,
            wait_for_accept,
            context.tcp_paths.len(),
            context.udp_paths.len(),
        ),
    );
    let started_at = Instant::now();
    let socket = udp::connect_path(
        path,
        crate::transport::udp::UdpConnectOptions {
            timeout: UDP_PATH_HANDSHAKE_TIMEOUT,
            ..crate::transport::udp::UdpConnectOptions::default()
        },
    )
    .await?;
    let mut encrypted = EncryptedUdpSocket::new_with_cipher_suite(
        socket,
        context.security.secret.as_bytes(),
        PeerRole::Client,
        context.codec_limits,
        context.security.cipher,
    );
    let path_id = PathId(path_index as u16);
    let session_id = random_session_id()?;
    let handshake_frames = {
        let (session_hello, session_auth, path_join) = authenticated_path_join_frames_for_session(
            &context.security,
            path,
            path_id,
            UnderlayProtocol::Udp,
            session_id,
        )?;
        [session_hello, session_auth, path_join]
    };

    for frame in &handshake_frames {
        encrypted.send_frame(frame).await?;
    }

    let mut buffer = vec![0u8; encrypted.max_datagram_bytes()?];
    let control_retry_interval = udp_stream_control_retry_interval(context, path_index);
    let handshake_started_at = Instant::now();
    let mut session_ready = false;
    let mut path_active = false;
    while !session_ready || !path_active {
        let elapsed = handshake_started_at.elapsed();
        if elapsed >= UDP_PATH_HANDSHAKE_TIMEOUT {
            return Err(RuntimeError::Protocol(
                "UDP stream path handshake timed out",
            ));
        }
        let remaining = UDP_PATH_HANDSHAKE_TIMEOUT.saturating_sub(elapsed);
        match tokio::time::timeout(
            control_retry_interval.min(remaining),
            encrypted.recv_frame(&mut buffer),
        )
        .await
        {
            Err(_) => {
                for frame in &handshake_frames {
                    encrypted.send_frame(frame).await?;
                }
                continue;
            }
            Ok(Err(err)) if encrypted_udp_error_is_ignorable(&err) => continue,
            Ok(Err(err)) => return Err(RuntimeError::EncryptedUdp(err)),
            Ok(Ok(Frame::SessionReady)) => session_ready = true,
            Ok(Ok(Frame::PathStatus {
                status: crate::protocol::PathStatus::Active,
                ..
            })) => path_active = true,
            Ok(Ok(Frame::PathStatus { .. })) => {
                return Err(RuntimeError::Protocol(
                    "UDP stream path did not become active",
                ));
            }
            Ok(Ok(Frame::SessionClose { reason })) => {
                return Err(RuntimeError::RemoteClosed(reason));
            }
            Ok(Ok(_)) => {
                return Err(RuntimeError::Protocol(
                    "unexpected UDP stream handshake frame",
                ));
            }
        }
    }

    let open_frame = Frame::OpenStream {
        stream_id,
        target,
        ingress,
        outbound: OutboundPolicy::Direct,
        class,
        role,
    };
    encrypted.send_frame(&open_frame).await?;

    let open_started_at = Instant::now();
    let open_retry_interval = control_retry_interval;
    let mut pending_open_retry = None;
    let max_offset = if wait_for_accept {
        loop {
            let elapsed = open_started_at.elapsed();
            if elapsed >= UDP_PATH_HANDSHAKE_TIMEOUT {
                return Err(RuntimeError::Protocol("UDP stream open timed out"));
            }
            let remaining = UDP_PATH_HANDSHAKE_TIMEOUT.saturating_sub(elapsed);
            match tokio::time::timeout(
                open_retry_interval.min(remaining),
                encrypted.recv_frame(&mut buffer),
            )
            .await
            {
                Err(_) => {
                    encrypted.send_frame(&open_frame).await?;
                    continue;
                }
                Ok(Err(err)) if encrypted_udp_error_is_ignorable(&err) => continue,
                Ok(Err(err)) => return Err(RuntimeError::EncryptedUdp(err)),
                Ok(Ok(frame)) => match frame {
                    Frame::StreamMaxData {
                        stream_id: max_stream_id,
                        max_offset,
                    } if max_stream_id == stream_id => break max_offset,
                    Frame::StreamReset {
                        stream_id: reset_stream_id,
                        reason,
                    } if reset_stream_id == stream_id => {
                        return Err(RuntimeError::RemoteReset(reason));
                    }
                    Frame::SessionClose { reason } => {
                        return Err(RuntimeError::RemoteClosed(reason));
                    }
                    Frame::SessionReady => {}
                    Frame::PathStatus { .. } => {}
                    _ => return Err(RuntimeError::Protocol("unexpected UDP stream open frame")),
                },
            }
        }
    } else {
        pending_open_retry = Some((open_frame.clone(), open_retry_interval));
        context.mux_limits.max_stream_window_bytes
    };

    let (commands, receivers) =
        tcp_path_session_command_channels(udp_stream_path_command_queue(context.mux_limits));
    let (frames_tx, frames_rx) = mpsc::channel(tcp_stream_frame_queue(context.mux_limits));
    tokio::spawn(run_client_udp_stream_path_session(
        encrypted,
        buffer,
        stream_id,
        path_id,
        receivers,
        frames_tx,
        pending_open_retry,
    ));
    let elapsed = started_at.elapsed();
    context.mark_udp_stream_reserved_open_success(path_index, elapsed);
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "reliable_stream_open_success",
        format_args!(
            "stream_id={} underlay=udp path_index={} class={:?} role={:?} wait_for_accept={} elapsed_ms={:.3}",
            stream_id.0,
            path_index,
            class,
            role,
            wait_for_accept,
            elapsed.as_secs_f64() * 1000.0,
        ),
    );
    Ok(OpenedRemoteStream {
        stream: TcpPathStream {
            stream_id,
            max_offset,
            class,
            underlay: UnderlayProtocol::Udp,
            max_frame_payload_bytes: udp_stream_frame_payload_bytes(context.mux_limits),
            output: TcpPathStreamOutput::Fixed(commands),
            frames: frames_rx,
        },
        path_index,
    })
}

pub(super) async fn run_client_udp_stream_path_session(
    mut encrypted: EncryptedUdpSocket,
    mut buffer: Vec<u8>,
    stream_id: StreamId,
    _path_id: PathId,
    mut commands: TcpPathSessionCommandReceivers,
    frames: mpsc::Sender<Result<Frame, RuntimeError>>,
    pending_open_retry: Option<(Frame, Duration)>,
) {
    let mut pending_open_retry = pending_open_retry
        .map(|(frame, interval)| (frame, interval, tokio::time::Instant::now() + interval));
    loop {
        let command_may_recv = !tcp_path_receivers_closed(&commands);
        if !command_may_recv {
            let _ = encrypted
                .send_frame(&Frame::SessionClose {
                    reason: CloseReason::Normal,
                })
                .await;
            return;
        }
        tokio::select! {
            biased;
            _ = async {
                if let Some((_, _, deadline)) = &pending_open_retry {
                    tokio::time::sleep_until(*deadline).await;
                }
            }, if pending_open_retry.is_some() => {
                if let Some((frame, interval, deadline)) = &mut pending_open_retry
                    && tokio::time::Instant::now() >= *deadline
                {
                    if let Err(err) = encrypted.send_frame(frame).await {
                        let _ = frames.send(Err(RuntimeError::EncryptedUdp(err))).await;
                        return;
                    }
                    *deadline = tokio::time::Instant::now() + *interval;
                }
            }
            frame = encrypted.recv_frame(&mut buffer) => {
                match frame {
                    Ok(Frame::Ping { nonce }) => {
                        if let Err(err) = encrypted.send_frame(&Frame::Pong { nonce }).await {
                            let _ = frames.send(Err(RuntimeError::EncryptedUdp(err))).await;
                            return;
                        }
                    }
                    Ok(Frame::SessionReady) => {}
                    Ok(frame @ (Frame::StreamData { stream_id: received_stream_id, .. }
                        | Frame::StreamAck { stream_id: received_stream_id, .. }
                        | Frame::StreamMaxData { stream_id: received_stream_id, .. }
                        | Frame::StreamFin { stream_id: received_stream_id, .. }
                        | Frame::StreamReset { stream_id: received_stream_id, .. }))
                        if received_stream_id == stream_id =>
                    {
                        pending_open_retry = None;
                        if frames.send(Ok(frame)).await.is_err() {
                            return;
                        }
                    }
                    Ok(frame @ Frame::PathStatus { .. }) => {
                        if frames.send(Ok(frame)).await.is_err() {
                            return;
                        }
                    }
                    Ok(Frame::SessionClose { reason }) => {
                        let _ = frames.send(Err(RuntimeError::RemoteClosed(reason))).await;
                        return;
                    }
                    Ok(_) => {
                        let _ = frames
                            .send(Err(RuntimeError::Protocol(
                                "unexpected UDP reliable stream frame",
                            )))
                            .await;
                        return;
                    }
                    Err(err) if encrypted_udp_error_is_ignorable(&err) => {}
                    Err(err) => {
                        let _ = frames.send(Err(RuntimeError::EncryptedUdp(err))).await;
                        return;
                    }
                }
            }
            command = recv_tcp_path_command(&mut commands), if command_may_recv => {
                match command {
                    Some(TcpPathSessionCommand::SendFrame(frame)) => {
                        if let Err(err) = encrypted.send_frame(&frame).await {
                            let _ = frames.send(Err(RuntimeError::EncryptedUdp(err))).await;
                            return;
                        }
                    }
                    Some(TcpPathSessionCommand::CloseStream(close_stream_id)) => {
                        if close_stream_id == stream_id {
                            let _ = encrypted
                                .send_frame(&Frame::SessionClose {
                                    reason: CloseReason::Normal,
                                })
                                .await;
                            return;
                        }
                    }
                    Some(TcpPathSessionCommand::OpenStream { .. }) => {
                        let _ = frames
                            .send(Err(RuntimeError::Protocol(
                                "client UDP stream path received open command",
                            )))
                            .await;
                        return;
                    }
                    None => {}
                }
            }
        }
    }
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
            | RuntimeError::EncryptedUdp(_)
            | RuntimeError::Auth(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
    )
}

pub(super) fn udp_stream_control_retry_interval(
    context: &ClientPathContext,
    path_index: usize,
) -> Duration {
    let max_retry = UDP_PATH_HANDSHAKE_TIMEOUT.mul_f64(0.5);
    let Some(snapshot) = context.udp_path_snapshot(path_index) else {
        return UDP_MIN_PATH_SUPPRESSION.min(max_retry);
    };
    let modeled_ms = snapshot.srtt_ms.max(1.0) * 2.0 + snapshot.jitter_ms.max(0.0) * 4.0 + 10.0;
    Duration::from_secs_f64(modeled_ms / 1000.0)
        .max(UDP_MIN_RESPONSE_TIMEOUT)
        .min(max_retry)
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
