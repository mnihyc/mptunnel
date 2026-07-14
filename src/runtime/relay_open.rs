use super::*;

pub(super) struct OpenedRemoteStream {
    pub(super) stream: ReliablePathStream,
    pub(super) path_index: usize,
}

impl OpenedRemoteStream {
    /// An accepted stream that is not committed to a remote set must release
    /// both the peer binding and the local carrier actor entry.
    pub(super) async fn close(self) {
        self.stream.send_detach().await;
        self.stream.close().await;
    }
}

pub(super) struct AcceptedRemoteStreamGuard {
    stream: Option<ReliablePathStream>,
}

impl AcceptedRemoteStreamGuard {
    pub(super) fn new(stream: ReliablePathStream) -> Self {
        Self {
            stream: Some(stream),
        }
    }

    pub(super) fn stream(&self) -> &ReliablePathStream {
        self.stream.as_ref().expect("accepted stream guard")
    }

    pub(super) fn commit(mut self) -> ReliablePathStream {
        self.stream.take().expect("accepted stream guard")
    }
}

impl Drop for AcceptedRemoteStreamGuard {
    fn drop(&mut self) {
        let Some(stream) = self.stream.take() else {
            return;
        };
        let stream_id = stream.stream_id;
        let output = stream.output.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                output.send_stream_detach(stream_id).await;
                output.close_stream(stream_id).await;
            });
        }
    }
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
    pub(super) load_reserved: bool,
    pub(super) load_lease: Option<RelayPathLoadLease>,
    pub(super) attached_at: Instant,
    pub(super) path_proof_id: Option<u64>,
    pub(super) path_proof_generation: u64,
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

    pub(super) fn has_load_reservation(&self) -> bool {
        self.load_reserved || self.load_lease.is_some()
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
    membership_generation: u64,
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
            membership_generation: 0,
        };
        set.attach(opened);
        set
    }

    pub(super) fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub(super) fn membership_generation(&self) -> u64 {
        self.membership_generation
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
            .map(|path| path.path_index)
    }

    pub(super) fn active_path_underlay(&self) -> Option<UnderlayProtocol> {
        self.active_path_position()
            .and_then(|position| self.paths.get(position))
            .map(|path| path.stream.underlay)
    }

    pub(super) fn contains_path_key(&self, key: RelayPathKey) -> bool {
        self.paths.iter().any(|path| path.key() == key)
    }

    pub(super) fn contains_path_instance(&self, instance: RelayPathInstance) -> bool {
        self.paths.iter().any(|path| path.instance() == instance)
    }

    pub(super) fn path_keys(&self) -> Vec<RelayPathKey> {
        self.paths
            .iter()
            .map(ReliableRelayRemotePath::key)
            .collect()
    }

    pub(super) fn path_instances(&self) -> Vec<RelayPathInstance> {
        self.paths
            .iter()
            .map(ReliableRelayRemotePath::instance)
            .collect()
    }

    pub(super) fn load_reserved_path_keys(&self) -> Vec<RelayPathKey> {
        self.paths
            .iter()
            .filter(|path| path.has_load_reservation())
            .map(ReliableRelayRemotePath::key)
            .collect()
    }

    pub(super) fn repair_path_instance_for_service_recovery(&self) -> Option<RelayPathInstance> {
        self.paths
            .iter()
            .rev()
            .find(|path| path.placement == RelayPathPlacement::Repair)
            .map(ReliableRelayRemotePath::instance)
    }

    pub(super) fn accepted_product_path_count(&self) -> usize {
        // Active and Repair opens enter this set only after peer acceptance.
        // Validation remains excluded from this attachment-role count even
        // when a stream separately graduates it from exact capacity evidence.
        self.paths
            .iter()
            .filter(|path| path.placement != RelayPathPlacement::Validation)
            .count()
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
            if let Some(lease) = &mut path.load_lease {
                // Relay control has already moved the shared load counters.
                lease.set_recorded_lane(lane);
            }
        }
    }

    pub(super) fn retry_pending_path_proofs(&mut self, context: &ClientPathContext) {
        for path in &mut self.paths {
            if path.placement != RelayPathPlacement::Validation {
                continue;
            }
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
        let OpenedRemoteStream { stream, path_index } = opened;
        let accepted = AcceptedRemoteStreamGuard::new(stream);
        let underlay = accepted.stream().underlay;
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
        let (stream, mut frames) = accepted.commit().into_handle_and_frames();
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
                    frame: Err(RuntimeError::ReliablePathSessionClosed),
                })
                .await;
        });
        let mut path = ReliableRelayRemotePath {
            path_index,
            instance_id,
            placement,
            load_reserved: placement == RelayPathPlacement::Active,
            load_lease: None,
            attached_at: Instant::now(),
            path_proof_id: None,
            path_proof_generation: 0,
            stream,
        };
        if placement == RelayPathPlacement::Validation {
            if let Ok(Some(proof_id)) = path.stream.enqueue_path_proof() {
                path.path_proof_id = Some(proof_id);
            }
        }
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
        self.membership_generation = self.membership_generation.wrapping_add(1);
    }

    pub(super) async fn recv_frame(&mut self) -> Result<ReliableRelayRemoteFrame, RuntimeError> {
        self.frames_rx
            .recv()
            .await
            .ok_or(RuntimeError::ReliablePathSessionClosed)
    }

    pub(super) fn has_buffered_frame(&self) -> bool {
        !self.frames_rx.is_empty()
    }

    pub(super) fn can_enqueue_work_lane_now(
        &self,
        work_lane: ReliableRelayQueuedWorkLane,
        relay_lane: FlowLane,
    ) -> bool {
        self.paths.iter().any(|path| {
            relay_path_placement_may_wake_work_lane(path.placement, work_lane)
                && path.stream.can_enqueue_work_lane_now(work_lane, relay_lane)
        })
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
        let Some(mut path) = self.remove_path_instance(instance) else {
            return false;
        };
        context.mark_relay_path_data_plane_failure(path.stream.underlay, path.path_index);
        if path.load_reserved {
            context.release_relay_path_load(
                path.stream.underlay,
                path.path_index,
                path.stream.lane,
            );
        }
        // The path is no longer schedulable. Release its exact optional-flow
        // lease before detach can wait behind a saturated carrier queue.
        drop(path.load_lease.take());
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
        self.membership_generation = self.membership_generation.wrapping_add(1);
        Some(path)
    }

    pub(super) fn activate_path_instance_after_service_open(
        &mut self,
        instance: RelayPathInstance,
    ) -> bool {
        let Some(position) = self
            .paths
            .iter()
            .position(|path| path.instance() == instance)
        else {
            return false;
        };
        if position + 1 == self.paths.len()
            && self.paths[position].placement == RelayPathPlacement::Active
        {
            return false;
        }
        if position + 1 == self.paths.len() {
            self.paths[position].placement = RelayPathPlacement::Active;
            return true;
        }
        let mut path = self.paths.remove(position);
        path.placement = RelayPathPlacement::Active;
        self.paths.push(path);
        true
    }

    pub(super) fn reserve_path_instance_load_if_needed(
        &mut self,
        context: &ClientPathContext,
        instance: RelayPathInstance,
        lane: FlowLane,
    ) -> bool {
        let Some(path) = self
            .paths
            .iter_mut()
            .find(|path| path.instance() == instance)
        else {
            return false;
        };
        if path.has_load_reservation() {
            return false;
        }
        match path.stream.underlay {
            UnderlayProtocol::Tcp => context.reserve_tcp_path_load(path.path_index, lane),
            UnderlayProtocol::Udp => context.reserve_udp_stream_path_load(path.path_index, lane),
        }
        path.load_reserved = true;
        true
    }

    pub(super) fn commit_path_instance_load_claim(
        &mut self,
        instance: RelayPathInstance,
        lease: RelayPathLoadLease,
    ) -> Result<(), RelayPathLoadLease> {
        let Some(path) = self
            .paths
            .iter_mut()
            .find(|path| path.instance() == instance)
        else {
            return Err(lease);
        };
        if path.has_load_reservation() {
            return Err(lease);
        }
        path.load_lease = Some(lease);
        Ok(())
    }

    fn active_path_position(&self) -> Option<usize> {
        self.paths
            .iter()
            .rposition(|path| path.placement == RelayPathPlacement::Active)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RelayPathPlacement {
    Active,
    Repair,
    Validation,
}

fn relay_path_placement_may_wake_work_lane(
    placement: RelayPathPlacement,
    work_lane: ReliableRelayQueuedWorkLane,
) -> bool {
    match work_lane {
        ReliableRelayQueuedWorkLane::Data => placement != RelayPathPlacement::Repair,
        ReliableRelayQueuedWorkLane::Control | ReliableRelayQueuedWorkLane::Repair => true,
    }
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
    RecoveryRepair,
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

pub(super) fn udp_relay_attachment_open_options(role: StreamOpenRole) -> UdpStreamOpenOptions {
    UdpStreamOpenOptions {
        wait_for_accept: role != StreamOpenRole::Validation,
        role,
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ReliableInitialOpenAttempt {
    pub(super) key: RelayPathKey,
    pub(super) stream_id: StreamId,
}

pub(super) fn reserve_reliable_initial_open_attempt(
    context: &ClientPathContext,
    lane: FlowLane,
    payload_bytes: usize,
    attempted: &mut Vec<RelayPathKey>,
) -> Result<Option<ReliableInitialOpenAttempt>, RuntimeError> {
    let candidate_count = context
        .tcp_paths
        .len()
        .saturating_add(context.udp_paths.len());
    if attempted.len() >= candidate_count {
        return Ok(None);
    }
    let Some(key) = context.reserve_reliable_stream_path(lane, payload_bytes, attempted) else {
        return Ok(None);
    };
    match context.allocate_reliable_stream_id() {
        Ok(stream_id) => {
            attempted.push(key);
            Ok(Some(ReliableInitialOpenAttempt { key, stream_id }))
        }
        Err(err) => {
            context.release_relay_path_load(key.underlay, key.index, lane);
            Err(err)
        }
    }
}

pub(super) fn mark_reliable_initial_open_retryable_failure(
    context: &ClientPathContext,
    key: RelayPathKey,
    lane: FlowLane,
) {
    context.release_relay_path_load(key.underlay, key.index, lane);
    context.mark_relay_path_data_plane_failure(key.underlay, key.index);
}

async fn open_reliable_initial_active_attempt(
    context: &ClientPathContext,
    attempt: ReliableInitialOpenAttempt,
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
    has_unattempted_alternative: bool,
) -> Result<OpenedRemoteStream, RuntimeError> {
    let ReliableInitialOpenAttempt { key, stream_id } = attempt;
    match key.underlay {
        UnderlayProtocol::Tcp => {
            let open_timeout = reliable_initial_active_open_timeout(
                context,
                key,
                lane,
                has_unattempted_alternative,
            );
            let open_deadlines =
                ClientTcpOpenDeadlines::fixed(tokio::time::Instant::now() + open_timeout);
            match open_remote_stream_on_reserved_path(
                context,
                stream_id,
                target,
                ingress,
                lane,
                key.index,
                StreamOpenRole::Active,
                open_deadlines,
            )
            .await
            {
                Ok(opened) => Ok(opened),
                Err(RuntimeError::PathOpenTimedOut) => {
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "reliable_stream_open_timeout",
                        format_args!(
                            "stream_id={} underlay=tcp path_index={} lane={:?} role={:?} timeout_ms={}",
                            stream_id.0,
                            key.index,
                            lane,
                            StreamOpenRole::Active,
                            open_timeout.as_millis(),
                        ),
                    );
                    Err(RuntimeError::PathOpenTimedOut)
                }
                Err(err) => Err(err),
            }
        }
        UnderlayProtocol::Udp => {
            let open_timeout = reliable_initial_active_open_timeout(
                context,
                key,
                lane,
                has_unattempted_alternative,
            );
            match tokio::time::timeout(
                open_timeout,
                open_remote_stream_on_reserved_udp_path(
                    context,
                    stream_id,
                    target,
                    ingress,
                    lane,
                    key.index,
                    UdpStreamOpenOptions::ACTIVE_WAIT,
                ),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => {
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "reliable_stream_open_timeout",
                        format_args!(
                            "stream_id={} underlay=udp path_index={} lane={:?} role={:?} timeout_ms={}",
                            stream_id.0,
                            key.index,
                            lane,
                            StreamOpenRole::Active,
                            open_timeout.as_millis(),
                        ),
                    );
                    Err(RuntimeError::PathOpenTimedOut)
                }
            }
        }
    }
}

pub(super) async fn open_remote_stream(
    context: &ClientPathContext,
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
) -> Result<OpenedRemoteStream, RuntimeError> {
    let mut attempted = Vec::new();
    let mut last_retryable_error = None;
    while let Some(attempt) =
        reserve_reliable_initial_open_attempt(context, lane, PATH_OPEN_SCORE_BYTES, &mut attempted)?
    {
        let key = attempt.key;
        let has_unattempted_alternative = context
            .ordered_reliable_path_keys(lane, PATH_OPEN_SCORE_BYTES)
            .into_iter()
            .any(|candidate| !attempted.contains(&candidate));
        match open_reliable_initial_active_attempt(
            context,
            attempt,
            target.clone(),
            ingress,
            lane,
            has_unattempted_alternative,
        )
        .await
        {
            Ok(opened) => return Ok(opened),
            Err(err) if relay_path_open_error_is_retryable(key.underlay, &err) => {
                mark_reliable_initial_open_retryable_failure(context, key, lane);
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
    let open_timeouts = reliable_relay_attach_open_timeouts(
        context,
        RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: path_index,
        },
        lane,
    );
    let open_deadlines = ClientTcpOpenDeadlines::from_timeouts(
        tokio::time::Instant::now(),
        open_timeouts.live,
        open_timeouts.setup,
    );
    let open_result = open_remote_stream_on_reserved_path(
        context,
        stream_id,
        target,
        ingress,
        lane,
        path_index,
        role,
        open_deadlines,
    )
    .await;
    match open_result {
        Ok(opened) => Ok(opened),
        Err(err) if !matches!(err, RuntimeError::PathOpenTimedOut) => {
            context.release_tcp_path_load(path_index, lane);
            Err(err)
        }
        Err(RuntimeError::PathOpenTimedOut) => {
            context.release_tcp_path_load(path_index, lane);
            context.mark_tcp_path_data_plane_failure(path_index);
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "reliable_stream_open_timeout",
                format_args!(
                    "stream_id={} underlay=tcp path_index={} lane={:?} role={:?} live_timeout_ms={} setup_timeout_ms={}",
                    stream_id.0,
                    path_index,
                    lane,
                    role,
                    open_timeouts.live.as_millis(),
                    open_timeouts.setup.as_millis(),
                ),
            );
            Err(RuntimeError::PathOpenTimedOut)
        }
        Err(err) => {
            context.release_tcp_path_load(path_index, lane);
            Err(err)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ReliableRelayAttachOpenTimeouts {
    pub(super) live: Duration,
    pub(super) setup: Duration,
}

pub(super) fn reliable_relay_attach_open_timeouts(
    context: &ClientPathContext,
    key: RelayPathKey,
    lane: FlowLane,
) -> ReliableRelayAttachOpenTimeouts {
    let snapshot = context.reliable_path_snapshot(key);
    let live = reliable_relay_stall_timeout(snapshot, lane);
    let setup = match key.underlay {
        UnderlayProtocol::Tcp => {
            // A cold lane-class actor owns carrier dial, authenticated path
            // join, and product open. A live association owns only the last.
            path_open_pto(snapshot, false)
                .saturating_mul(active_path_open_serialized_exchanges(snapshot))
        }
        UnderlayProtocol::Udp => live,
    };
    ReliableRelayAttachOpenTimeouts {
        live,
        setup: setup.max(live),
    }
}

pub(super) fn reliable_initial_active_open_timeout(
    context: &ClientPathContext,
    key: RelayPathKey,
    lane: FlowLane,
    has_unattempted_alternative: bool,
) -> Duration {
    let _ = lane;
    let snapshot = context.reliable_path_snapshot(key);
    let rtt_is_observed =
        key.underlay == UnderlayProtocol::Udp && context.reliable_path_rtt_is_observed(key);
    if has_unattempted_alternative {
        path_open_pto(snapshot, rtt_is_observed)
            .saturating_mul(active_path_open_serialized_exchanges(snapshot))
    } else {
        active_path_open_timeout(snapshot, rtt_is_observed)
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
    open_deadlines: ClientTcpOpenDeadlines,
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
    let opened = context
        .tcp_sessions
        .get(path_index)
        .ok_or(RuntimeError::NoSchedulableTcpPath)?
        .open_stream_with_deadlines(stream_id, target, ingress, lane, role, open_deadlines)
        .await?;
    let accepted = AcceptedRemoteStreamGuard::new(opened.stream);
    tokio::time::timeout_at(
        opened.open_deadline,
        send_open_path_metrics(
            context,
            accepted.stream(),
            UnderlayProtocol::Tcp,
            path_index,
        ),
    )
    .await
    .map_err(|_| RuntimeError::PathOpenTimedOut)??;
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
    Ok(OpenedRemoteStream {
        stream: accepted.commit(),
        path_index,
    })
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
    let open_timeout = reliable_relay_attach_open_timeouts(
        context,
        RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: path_index,
        },
        lane,
    )
    .setup;
    match relay_path_open_with_timeout(
        open_timeout,
        open_remote_stream_on_reserved_udp_path(
            context, stream_id, target, ingress, lane, path_index, options,
        ),
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

pub(super) async fn relay_path_open_with_timeout<T, F>(
    open_timeout: Duration,
    open: F,
) -> Result<T, RuntimeError>
where
    F: std::future::Future<Output = Result<T, RuntimeError>>,
{
    match tokio::time::timeout(open_timeout, open).await {
        Ok(result) => result,
        Err(_) => Err(RuntimeError::PathOpenTimedOut),
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
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = (wait_for_accept, role);
    let started_at = Instant::now();
    let stream = context
        .udp_sessions
        .get(path_index)
        .ok_or(RuntimeError::NoSchedulableUdpPath)?
        .open_stream(stream_id, target, ingress, lane, options)
        .await?;
    let accepted = AcceptedRemoteStreamGuard::new(stream);
    let elapsed = started_at.elapsed();
    context.mark_udp_stream_reserved_open_success(path_index, elapsed, wait_for_accept);
    send_open_path_metrics(
        context,
        accepted.stream(),
        UnderlayProtocol::Udp,
        path_index,
    )
    .await?;
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
    Ok(OpenedRemoteStream {
        stream: accepted.commit(),
        path_index,
    })
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
            | RuntimeError::ReliablePathSessionClosed
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
            | RuntimeError::ReliablePathSessionClosed
            | RuntimeError::PathHeartbeatTimeout
            | RuntimeError::PathOpenTimedOut
            | RuntimeError::Protocol(_)
    )
}

pub(super) fn relay_error_is_tcp_path_failure<T>(result: &Result<T, RuntimeError>) -> bool {
    matches!(
        result,
        Err(RuntimeError::PathHeartbeatTimeout)
            | Err(RuntimeError::PathOpenTimedOut)
            | Err(RuntimeError::ReliablePathSessionClosed)
            | Err(RuntimeError::Tcp(_))
            | Err(RuntimeError::Encrypted(_))
            | Err(RuntimeError::RemoteClosed(_))
            | Err(RuntimeError::Protocol(_))
    )
}

#[cfg(test)]
mod tests;
