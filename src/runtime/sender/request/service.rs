use super::*;
#[cfg(test)]
use crate::model::ack_clock::{
    reliable_ack_clock_calibration_ceiling_bytes, reliable_ack_clock_calibration_limit_bytes,
    reliable_request_ack_clock_calibration_target_bytes,
};
#[cfg(feature = "lab-diagnostics")]
use crate::model::request::capacity::request_quic_capacity_slow_start_rounds;
use crate::model::request::capacity::{
    request_capacity_stable_candidate_share_bytes, request_quic_capacity_calibration_geometry,
    request_quic_capacity_calibration_lease, request_tcp_capacity_calibration_geometry,
    request_tcp_capacity_calibration_lease, request_tcp_capacity_candidate_can_start_receipt,
};
use crate::model::request::evidence::{
    RequestOwnerAckProgress, RequestPathRateEvidence, RequestPathRateEvidenceUpdate,
    RequestPerFlowRateModel, RequestTcpAckTurnoverModel, request_path_rate_coverage_floor_bytes,
    request_tcp_candidate_turnover_authorized,
};
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::{reliable_path_frame_pacing_bytes, stream_ack_contiguous_frontier};
use crate::protocol::frame::{reliable_stream_frame_accounted_bytes, reliable_stream_frame_extent};

// Ownership boundary:
// Sender services own product work before it reaches carrier command queues.
// Client relay sending and server response dispatch both use this module for
// queueing, path ranking, reservation intents, and diagnostics. Reliable-path
// bindings own exact range flight and atomic commit; final TCP/UDP emission
// still happens through carrier command senders.

// Local diagnostic naming helper. The response `admission` owner has a private helper
// with the same purpose, but sender is a sibling module and must not
// depend on that module-private symbol when `lab-diagnostics` is enabled.
#[cfg(feature = "lab-diagnostics")]
fn sender_service_frame_kind(frame: &Frame) -> &'static str {
    match frame {
        Frame::StreamData { .. } => "stream_data",
        Frame::StreamAck { .. } => "stream_ack",
        Frame::StreamMaxData { .. } => "stream_max_data",
        Frame::StreamFin { .. } => "stream_fin",
        Frame::StreamReset { .. } => "stream_reset",
        Frame::StreamDetach { .. } => "stream_detach",
        Frame::DatagramData { .. } => "datagram_data",
        Frame::DatagramFeedback { .. } => "datagram_feedback",
        Frame::DatagramClose { .. } => "datagram_close",
        _ => "control",
    }
}

#[derive(Debug)]
struct RelayPathSendSelection {
    position: usize,
    data_role: Option<PathRuntimeRole>,
    request_startup_commit: Option<RequestStartupAdmission>,
    request_calibration_commit: Option<RequestAckClockCalibrationCommit>,
    request_load_expectation: Option<(RelayPathKey, u32, u32)>,
}

#[derive(Debug, Clone, Copy)]
enum RequestAckClockCalibrationCommit {
    ServiceFence {
        service: RelayPathInstance,
        candidate: RelayPathInstance,
        entry_offset: u64,
        foreign_optional_ranges: usize,
        foreign_optional_bytes: u64,
    },
    OwnerData {
        candidate: RelayPathInstance,
        target_bytes: u64,
        payload_bytes: u64,
        entry_offset: u64,
        foreign_optional_ranges: usize,
        foreign_optional_bytes: u64,
    },
}

#[derive(Debug)]
struct RequestQuicCapacityCalibration {
    target: RelayPathInstance,
    token: u64,
    publication_expires_at: Instant,
    graduated: bool,
    ticket: QuicCapacityProbeCommandTicket,
    _lease: RequestQuicCapacityProbeLease,
}

#[derive(Debug)]
struct RequestTcpCapacityCalibration {
    target: RelayPathInstance,
    token: u64,
    publication_expires_at: Instant,
    proof_expires_at: Option<Instant>,
    graduated: bool,
    lease: RequestTcpCapacityProbeLease,
}

fn request_tcp_carrier_authority_expired_naturally(
    published: bool,
    proof_expires_at: Option<Instant>,
    now: Instant,
) -> bool {
    published && proof_expires_at.is_some_and(|expires_at| now >= expires_at)
}

impl Drop for RequestTcpCapacityCalibration {
    fn drop(&mut self) {
        self.lease.cancel();
    }
}

impl Drop for RequestQuicCapacityCalibration {
    fn drop(&mut self) {
        self.ticket.cancel();
    }
}

impl RelayPathSendSelection {
    fn new(position: usize, data_role: Option<PathRuntimeRole>) -> Self {
        Self {
            position,
            data_role,
            request_startup_commit: None,
            request_calibration_commit: None,
            request_load_expectation: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) enum ClientQueuedDispatch {
    Data { payload_bytes: usize },
    Repair { payload_bytes: usize },
    RepairDeferred,
    PersistentRepairCancelled,
}

#[derive(Debug)]
pub(in crate::runtime) struct RelaySenderService {
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    stream_id: StreamId,
    flights: RelayPathFlightLedger,
    ordered_data_owner: Option<RelayPathKey>,
    ordered_data_owner_instance: Option<RelayPathInstance>,
    request_startup: RequestStartupState,
    request_rate_evidence: HashMap<RelayPathInstance, RequestPathRateEvidence>,
    request_per_flow_rate_bps: HashMap<RelayPathInstance, RequestPerFlowRateModel>,
    request_tcp_ack_turnover: HashMap<RelayPathInstance, RequestTcpAckTurnoverModel>,
    request_rate_proven_subflows: HashSet<RelayPathInstance>,
    request_ack_clock_first_window_subflows: HashSet<RelayPathInstance>,
    request_ack_clock_proven_subflows: HashSet<RelayPathInstance>,
    request_window_turnover_proven_subflows: HashSet<RelayPathInstance>,
    request_ack_clock_calibration_bytes: HashMap<RelayPathInstance, u64>,
    request_ack_clock_calibration_targets: HashMap<RelayPathInstance, u64>,
    request_ack_clock_calibration_owner: Option<RequestAckClockCalibrationOwner>,
    request_ack_clock_calibration_pending: Option<RequestAckClockCalibrationPending>,
    request_tcp_capacity_calibrations: HashMap<RelayPathInstance, RequestTcpCapacityCalibration>,
    request_tcp_capacity_attempted_paths: HashSet<usize>,
    request_tcp_capacity_proven_subflows: HashSet<RelayPathInstance>,
    request_tcp_capacity_campaign: Arc<RequestCapacityProbeCampaignBudget>,
    #[cfg(feature = "lab-diagnostics")]
    request_tcp_capacity_last_gate: Option<(Option<RelayPathInstance>, &'static str)>,
    request_quic_capacity_calibration: Option<RequestQuicCapacityCalibration>,
    request_quic_capacity_attempted_paths: HashSet<usize>,
    request_quic_capacity_campaign: Arc<RequestCapacityProbeCampaignBudget>,
    request_graduated_subflows: HashSet<RelayPathInstance>,
    request_membership_generation: Option<u64>,
    request_bulk_flow_registration: Option<ReliableTcpRequestBulkFlowRegistration>,
    missing_owner_repair_attempts: HashMap<RelayPathInstance, Instant>,
    next_send_index: usize,
    performance: MppPerformanceConfig,
    extra_traffic: ExtraTrafficLedger,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct RelayRecvProgressSend {
    path: Option<PathSnapshot>,
    lane: FlowLane,
    force_max_data: bool,
    recover_stalled_service: bool,
}

impl RelayRecvProgressSend {
    pub(in crate::runtime) fn new(
        path: Option<PathSnapshot>,
        lane: FlowLane,
        force_max_data: bool,
    ) -> Self {
        Self {
            path,
            lane,
            force_max_data,
            recover_stalled_service: false,
        }
    }

    pub(in crate::runtime) fn recover_stalled_service(mut self) -> Self {
        self.recover_stalled_service = true;
        self
    }
}

impl RelaySenderService {
    pub(in crate::runtime) fn new(stream_id: StreamId) -> Self {
        Self::new_with_performance(stream_id, MppPerformanceConfig::default())
    }

    pub(in crate::runtime) fn new_with_performance(
        stream_id: StreamId,
        performance: MppPerformanceConfig,
    ) -> Self {
        Self {
            stream_id,
            flights: RelayPathFlightLedger::default(),
            ordered_data_owner: None,
            ordered_data_owner_instance: None,
            request_startup: RequestStartupState::default(),
            request_rate_evidence: HashMap::new(),
            request_per_flow_rate_bps: HashMap::new(),
            request_tcp_ack_turnover: HashMap::new(),
            request_rate_proven_subflows: HashSet::new(),
            request_ack_clock_first_window_subflows: HashSet::new(),
            request_ack_clock_proven_subflows: HashSet::new(),
            request_window_turnover_proven_subflows: HashSet::new(),
            request_ack_clock_calibration_bytes: HashMap::new(),
            request_ack_clock_calibration_targets: HashMap::new(),
            request_ack_clock_calibration_owner: None,
            request_ack_clock_calibration_pending: None,
            request_tcp_capacity_calibrations: HashMap::new(),
            request_tcp_capacity_attempted_paths: HashSet::new(),
            request_tcp_capacity_proven_subflows: HashSet::new(),
            request_tcp_capacity_campaign: Arc::new(RequestCapacityProbeCampaignBudget::default()),
            #[cfg(feature = "lab-diagnostics")]
            request_tcp_capacity_last_gate: None,
            request_quic_capacity_calibration: None,
            request_quic_capacity_attempted_paths: HashSet::new(),
            request_quic_capacity_campaign: Arc::new(RequestCapacityProbeCampaignBudget::default()),
            request_graduated_subflows: HashSet::new(),
            request_membership_generation: None,
            request_bulk_flow_registration: None,
            missing_owner_repair_attempts: HashMap::new(),
            next_send_index: 0,
            performance,
            extra_traffic: ExtraTrafficLedger::default(),
        }
    }

    pub(in crate::runtime) fn bind_request_bulk_flow_registration(
        &mut self,
        registration: ReliableTcpRequestBulkFlowRegistration,
    ) {
        self.request_bulk_flow_registration = Some(registration);
    }

    pub(in crate::runtime) async fn fail_client_path_instance(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        instance: RelayPathInstance,
    ) -> bool {
        // Removal cleanup can await a full carrier queue. Only losing an
        // active Service invalidates logical contention; an optional
        // Validation failure must not hide the still-live Service meanwhile.
        let removes_active_service = remotes.paths.iter().any(|path| {
            path.instance() == instance && path.placement == RelayPathPlacement::Active
        });
        if removes_active_service && let Some(registration) = &self.request_bulk_flow_registration {
            registration.update(false, None);
        }
        remotes.fail_path_instance(context, instance).await
    }

    fn extra_traffic_budget_remaining(&self, mux_limits: MuxLimits) -> usize {
        self.extra_traffic
            .budget(
                sender_extra_traffic_startup_floor_bytes(mux_limits),
                self.performance,
            )
            .remaining_bytes()
    }

    pub(in crate::runtime) fn repair_extra_event_budget_remaining(
        &self,
        mux_limits: MuxLimits,
    ) -> usize {
        let remaining = self.extra_traffic_budget_remaining(mux_limits);
        if remaining < sender_repair_minimum_useful_attempt_bytes(mux_limits) {
            0
        } else {
            remaining
        }
    }

    pub(in crate::runtime) fn enqueue_repair_frame_with_priority(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        frame: Frame,
        cause: RelaySendCause,
        mux_limits: MuxLimits,
        critical_priority: bool,
    ) -> bool {
        debug_assert!(cause.is_repair());
        let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
        let budget = self.extra_traffic.budget(
            sender_extra_traffic_startup_floor_bytes(mux_limits),
            self.performance,
        );
        if !budget.can_spend(payload_bytes) {
            return false;
        }
        self.extra_traffic
            .record_optional(ExtraTrafficKind::Repair, payload_bytes);
        if critical_priority {
            sender_queue.push_critical_repair_with_cause(frame, cause);
        } else {
            sender_queue.push_repair_with_cause(frame, cause);
        }
        true
    }

    pub(in crate::runtime) fn enqueue_critical_repair_frame(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        frame: Frame,
        cause: RelaySendCause,
    ) {
        debug_assert!(cause.is_repair());
        let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
        self.extra_traffic
            .record_optional(ExtraTrafficKind::Repair, payload_bytes);
        sender_queue.push_critical_repair_with_cause(frame, cause);
    }

    pub(in crate::runtime) fn enqueue_critical_tail_repair_frame(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        frame: Frame,
    ) -> bool {
        if sender_queue.has_queued_repair_overlap(&frame) {
            return false;
        }
        self.enqueue_critical_repair_frame(sender_queue, frame, RelaySendCause::PathFailureRepair);
        true
    }

    #[cfg(test)]
    fn record_owner_progress_for_test(&mut self, bytes: usize) {
        self.record_owner_progress(bytes);
    }

    pub(in crate::runtime) fn record_owner_progress(&mut self, bytes: usize) {
        self.extra_traffic.record_owner_progress(bytes);
    }

    fn record_request_per_flow_rate_sample(
        &mut self,
        instance: RelayPathInstance,
        sample: PathRateSample,
        replace: bool,
    ) {
        if instance.key.underlay != UnderlayProtocol::Tcp {
            return;
        }
        let sample_bps = sample.rate_bps();
        let previous = self.request_per_flow_rate_bps.get(&instance).copied();
        let model = if replace {
            RequestPerFlowRateModel {
                rate_bps: sample_bps,
                delivery_samples: 1,
            }
        } else {
            RequestPerFlowRateModel {
                rate_bps: previous.map_or(sample_bps, |previous| {
                    previous.rate_bps.mul_add(0.75, sample_bps * 0.25)
                }),
                delivery_samples: previous
                    .map_or(1, |previous| previous.delivery_samples.saturating_add(1)),
            }
        };
        self.request_per_flow_rate_bps.insert(instance, model);
    }

    fn record_request_tcp_ack_turnover_sample(
        &mut self,
        context: &ClientPathContext,
        instance: RelayPathInstance,
        sample: PathRateSample,
        sampled_at: Instant,
        candidate_sample: bool,
    ) {
        if instance.key.underlay != UnderlayProtocol::Tcp
            || (self.ordered_data_owner_instance != Some(instance) && !candidate_sample)
        {
            return;
        }
        let Some(snapshot) = context.reliable_path_snapshot(instance.key) else {
            return;
        };
        let pto = transport_pto_from_snapshot(Some(snapshot));
        let previous = self.request_tcp_ack_turnover.get(&instance).copied();
        if let Some(model) = RequestTcpAckTurnoverModel::observe(previous, sample, pto, sampled_at)
        {
            self.request_tcp_ack_turnover.insert(instance, model);
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) async fn send_stream_data(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        self.send_frame(context, remotes, frame, RelaySendCause::StreamData, None)
            .await
    }

    async fn send_stream_data_for_request_lane(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        request_lane: FlowLane,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        self.send_frame(
            context,
            remotes,
            frame,
            RelaySendCause::StreamData,
            Some(request_lane),
        )
        .await
    }

    pub(in crate::runtime) async fn send_control_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        debug_assert!(!cause.is_repair());
        self.send_frame(context, remotes, frame, cause, None).await
    }

    pub(in crate::runtime) async fn send_repair_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        debug_assert!(cause.is_repair());
        self.send_frame(context, remotes, frame, cause, None).await
    }

    pub(in crate::runtime) fn ack_gap_repair_path_model(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        normalized_ranges: &[OffsetRange],
        preview_limit: usize,
        lane: FlowLane,
    ) -> (
        Option<UnderlayProtocol>,
        Option<PathSnapshot>,
        Option<(ClientRepairOutputIdentity, PathSnapshot)>,
    ) {
        let Some(preview) = send_stream
            .retransmission_frames_for_normalized_ack_gaps(normalized_ranges, preview_limit.max(1))
            .into_iter()
            .next()
        else {
            return (None, None, None);
        };
        let owner_underlay = self.flights.ordering_owner_underlay_for_frame(&preview);
        let owner_timing_path = self
            .flights
            .ordering_owner_keys_for_frame_any_instance(&preview)
            .into_iter()
            .filter_map(|key| context.reliable_path_snapshot(key))
            .max_by(|left, right| {
                transport_pto_from_snapshot(Some(*left))
                    .cmp(&transport_pto_from_snapshot(Some(*right)))
            });
        let avoid_keys = self.flights.sent_keys_for_frame(&preview);
        let repair_path = self
            .choose_lowest_eta_relay_path(
                context,
                remotes,
                &preview,
                lane,
                RelaySendCause::PersistentAckGapRepair,
                &avoid_keys,
                false,
            )
            .ok()
            .and_then(|position| {
                let key = remotes.paths[position].key();
                let instance = remotes.paths[position].instance();
                context
                    .relay_path_has_bulk_model_evidence(key.underlay, key.index)
                    .then(|| {
                        context
                            .reliable_path_snapshot(key)
                            .map(|snapshot| (ClientRepairOutputIdentity { instance }, snapshot))
                    })
                    .flatten()
            });
        (owner_underlay, owner_timing_path, repair_path)
    }

    pub(in crate::runtime) async fn dispatch_client_queued_work(
        &mut self,
        context: &ClientPathContext,
        spec: &ReliableRelayOpenSpec,
        relay_lane: FlowLane,
        request_lane: FlowLane,
        remotes: &mut ReliableRelayRemoteSet,
        send_stream: &mut ReliableSendStream,
        sender_queue: &mut ReliableRelaySenderQueue,
        local_open: bool,
        inflight_path_claims: &HashSet<RelayPathKey>,
        data_quantum_bytes: usize,
    ) -> Result<ClientQueuedDispatch, RuntimeError> {
        let queued_kind = sender_queue
            .front()
            .map(|(_, queued)| queued.kind.clone())
            .expect("queued_send_ready requires queued data");
        match queued_kind {
            ReliableRelayQueuedWorkKind::Control(_) => {
                Err(RuntimeError::Protocol("client sender queue control item"))
            }
            ReliableRelayQueuedWorkKind::Data(payload) => {
                self.dispatch_client_data_work(
                    context,
                    spec,
                    relay_lane,
                    request_lane,
                    remotes,
                    send_stream,
                    sender_queue,
                    local_open,
                    inflight_path_claims,
                    payload,
                    data_quantum_bytes,
                )
                .await
            }
            ReliableRelayQueuedWorkKind::Repair { frame, cause } => {
                self.dispatch_client_repair_work(
                    context,
                    spec,
                    relay_lane,
                    remotes,
                    send_stream,
                    sender_queue,
                    local_open,
                    inflight_path_claims,
                    frame,
                    cause,
                )
                .await
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn dispatch_client_data_work(
        &mut self,
        context: &ClientPathContext,
        spec: &ReliableRelayOpenSpec,
        relay_lane: FlowLane,
        request_lane: FlowLane,
        remotes: &mut ReliableRelayRemoteSet,
        send_stream: &mut ReliableSendStream,
        sender_queue: &mut ReliableRelaySenderQueue,
        local_open: bool,
        inflight_path_claims: &HashSet<RelayPathKey>,
        payload: Bytes,
        data_quantum_bytes: usize,
    ) -> Result<ClientQueuedDispatch, RuntimeError> {
        let dispatch_payload_bytes = data_quantum_bytes.min(payload.len()).max(1);
        let dispatch_payload = payload.slice(..dispatch_payload_bytes);
        let frame = send_stream
            .send_data(dispatch_payload, StreamFlags::NONE)
            .map_err(RuntimeError::Stream)?;
        let retry_frame = frame.clone();
        // Queue priority stays duplex-aware, but request exploration must not
        // borrow bulk classification from reverse-direction response bytes.
        match self
            .send_stream_data_for_request_lane(context, remotes, frame.clone(), request_lane)
            .await
        {
            Ok(outcome) => {
                let committed = sender_queue
                    .commit_front_data_prefix(dispatch_payload_bytes)
                    .expect("sent queued data must still be at queue front");
                let _ = outcome;
                Ok(ClientQueuedDispatch::Data {
                    payload_bytes: committed.payload_bytes,
                })
            }
            Err(RuntimeError::SenderServiceBlocked) => {
                let _ = send_stream.rollback_committed_data(&frame);
                Err(RuntimeError::SenderServiceBlocked)
            }
            Err(err) if reliable_relay_error_is_migratable(&err) => {
                let _ = send_stream.rollback_committed_data(&frame);
                match attach_reliable_relay_paths(
                    context,
                    spec,
                    relay_lane,
                    remotes,
                    send_stream,
                    !local_open,
                    ReliableRelayAttachMode::Any,
                    inflight_path_claims,
                )
                .await
                {
                    Ok(attached) if attached > 0 => {
                        if let Err(err) = send_stream.commit_prepared_data(&frame) {
                            return Err(RuntimeError::Stream(err));
                        }
                        match self
                            .send_stream_data_for_request_lane(
                                context,
                                remotes,
                                retry_frame,
                                request_lane,
                            )
                            .await
                        {
                            Ok(outcome) => {
                                let committed = sender_queue
                                    .commit_front_data_prefix(dispatch_payload_bytes)
                                    .expect("sent queued data must still be at queue front");
                                let _ = outcome;
                                Ok(ClientQueuedDispatch::Data {
                                    payload_bytes: committed.payload_bytes,
                                })
                            }
                            Err(RuntimeError::SenderServiceBlocked) => {
                                let _ = send_stream.rollback_committed_data(&frame);
                                Err(RuntimeError::SenderServiceBlocked)
                            }
                            Err(err) if reliable_relay_error_is_migratable(&err) => {
                                let _ = send_stream.rollback_committed_data(&frame);
                                Err(err)
                            }
                            Err(err) => {
                                let _ = send_stream.rollback_committed_data(&frame);
                                Err(err)
                            }
                        }
                    }
                    Ok(_) => Err(err),
                    Err(err) => Err(err),
                }
            }
            Err(err) => {
                let _ = send_stream.rollback_committed_data(&frame);
                Err(err)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn dispatch_client_repair_work(
        &mut self,
        context: &ClientPathContext,
        spec: &ReliableRelayOpenSpec,
        relay_lane: FlowLane,
        remotes: &mut ReliableRelayRemoteSet,
        send_stream: &mut ReliableSendStream,
        sender_queue: &mut ReliableRelaySenderQueue,
        local_open: bool,
        inflight_path_claims: &HashSet<RelayPathKey>,
        frame: Frame,
        cause: RelaySendCause,
    ) -> Result<ClientQueuedDispatch, RuntimeError> {
        let retry_frame = frame.clone();
        match self.send_repair_frame(context, remotes, frame, cause).await {
            Ok(outcome) => {
                let (_, committed) = sender_queue
                    .commit_front()
                    .expect("sent queued repair must still be at queue front");
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "repair",
                    format_args!(
                        "stream_id={} path_underlay={:?} path_index={} cause={} queued_dispatch=true payload_bytes={}",
                        self.stream_id.0,
                        outcome.path_key.underlay,
                        outcome.path_key.index,
                        cause.as_str(),
                        committed.payload_bytes,
                    ),
                );
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = outcome;
                Ok(ClientQueuedDispatch::Repair {
                    payload_bytes: committed.payload_bytes,
                })
            }
            Err(RuntimeError::SenderServiceBlocked) => Err(RuntimeError::SenderServiceBlocked),
            Err(err)
                if matches!(cause, RelaySendCause::PersistentClientAckGapRepair(_))
                    && reliable_relay_error_is_migratable(&err) =>
            {
                let discarded = sender_queue.discard_persistent_ack_gap_repair_batch(cause);
                debug_assert!(discarded > 0);
                Ok(ClientQueuedDispatch::PersistentRepairCancelled)
            }
            Err(err)
                if cause == RelaySendCause::LiveOwnerTailRepair
                    && reliable_relay_error_is_migratable(&err) =>
            {
                let (_, _) = sender_queue
                    .commit_front()
                    .expect("deferred live-tail repair must still be at queue front");
                Ok(ClientQueuedDispatch::RepairDeferred)
            }
            Err(err) if reliable_relay_error_is_migratable(&err) => {
                match attach_reliable_relay_paths(
                    context,
                    spec,
                    relay_lane,
                    remotes,
                    send_stream,
                    !local_open,
                    ReliableRelayAttachMode::Any,
                    inflight_path_claims,
                )
                .await
                {
                    Ok(attached) if attached > 0 => {
                        match self
                            .send_repair_frame(context, remotes, retry_frame, cause)
                            .await
                        {
                            Ok(outcome) => {
                                let (_, committed) = sender_queue
                                    .commit_front()
                                    .expect("sent queued repair must still be at queue front");
                                #[cfg(feature = "lab-diagnostics")]
                                lab_diagnostic(
                                    "repair",
                                    format_args!(
                                        "stream_id={} path_underlay={:?} path_index={} cause={} queued_dispatch=true after_attach=true payload_bytes={}",
                                        self.stream_id.0,
                                        outcome.path_key.underlay,
                                        outcome.path_key.index,
                                        cause.as_str(),
                                        committed.payload_bytes,
                                    ),
                                );
                                #[cfg(not(feature = "lab-diagnostics"))]
                                let _ = outcome;
                                Ok(ClientQueuedDispatch::Repair {
                                    payload_bytes: committed.payload_bytes,
                                })
                            }
                            Err(RuntimeError::SenderServiceBlocked) => {
                                Err(RuntimeError::SenderServiceBlocked)
                            }
                            Err(err) => Err(err),
                        }
                    }
                    Ok(_) => Err(err),
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(err),
        }
    }

    async fn send_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
        request_lane: Option<FlowLane>,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        let sent_frame = frame.clone();
        let avoid_keys = match cause {
            RelaySendCause::LiveOwnerTailRepair => self.flights.live_owner_tail_repair_owner_keys(
                &sent_frame,
                &remotes.path_instances(),
                Duration::ZERO,
                Duration::ZERO,
            ),
            cause if cause.is_repair() => self.flights.sent_keys_for_frame(&sent_frame),
            _ => Vec::new(),
        };
        let instance = self
            .emit_relay_frame(context, remotes, frame, cause, &avoid_keys, request_lane)
            .await?;
        let path_key = instance.key;
        let payload_bytes = if cause.is_repair() {
            self.flights
                .record_repair_frame_instance(instance, &sent_frame)
        } else {
            self.flights
                .record_owner_frame_instance(instance, &sent_frame)
        };
        self.record_decision(path_key, payload_bytes, &sent_frame, cause);
        Ok(RelaySendOutcome { path_key })
    }

    async fn emit_relay_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
        avoid_keys: &[RelayPathKey],
        request_lane: Option<FlowLane>,
    ) -> Result<RelayPathInstance, RuntimeError> {
        let mut last_error = None;
        while !remotes.paths.is_empty() {
            let stream_lane = remotes
                .paths
                .last()
                .map(|path| path.stream.lane)
                .unwrap_or(FlowLane::Latency);
            if stream_lane.is_bulk()
                && matches!(frame, Frame::StreamData { .. })
                && !cause.is_repair()
            {
                remotes.retry_pending_path_proofs(context);
            }
            let selection_lane = request_lane.unwrap_or(stream_lane);
            let selection = match self.choose_relay_path_position(
                context,
                remotes,
                &frame,
                selection_lane,
                cause,
                avoid_keys,
            ) {
                Ok(selection) => selection,
                Err(RuntimeError::SenderServiceBlocked) => {
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                Err(err) => return Err(last_error.unwrap_or(err)),
            };
            let RelayPathSendSelection {
                position,
                data_role,
                request_startup_commit,
                request_calibration_commit,
                request_load_expectation,
            } = selection;
            let instance = remotes.paths[position].instance();
            let (lane, emit_mode) = if matches!(cause, RelaySendCause::StreamFin) {
                (
                    remotes.paths[position].stream.lane,
                    CarrierEmitMode::StreamOrdered,
                )
            } else {
                (
                    reliable_path_effective_frame_lane(&frame, remotes.paths[position].stream.lane),
                    CarrierEmitMode::Classified,
                )
            };
            let request_load_claim =
                if let Some((key, active, latency_sensitive)) = request_load_expectation {
                    let Some(claim) = context.try_reserve_relay_path_load_if_unchanged(
                        key,
                        selection_lane,
                        active,
                        latency_sensitive,
                    ) else {
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "request_startup_selection",
                            format_args!(
                                "phase=claim_stale stream_id={} path_index={} instance_id={}",
                                self.stream_id.0, instance.key.index, instance.id,
                            ),
                        );
                        return Err(RuntimeError::SenderServiceBlocked);
                    };
                    Some(claim)
                } else {
                    None
                };
            match emit_relay_path_frame_with_mode(
                &remotes.paths[position].stream,
                frame.clone(),
                lane,
                emit_mode,
            ) {
                Ok(()) => {
                    if let Some(admission) = request_startup_commit {
                        self.request_startup.commit_admission(admission);
                    }
                    self.commit_request_ack_clock_calibration(request_calibration_commit);
                    let claimed_load = request_load_claim.is_some();
                    if let Some(claim) = request_load_claim {
                        // The exact path owns the lease after carrier enqueue;
                        // path removal or relay cancellation releases it.
                        let _ = remotes.commit_path_instance_load_claim(instance, claim);
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "request_startup_selection",
                            format_args!(
                                "phase=claim_committed stream_id={} path_index={} instance_id={}",
                                self.stream_id.0, instance.key.index, instance.id,
                            ),
                        );
                    }
                    if matches!(frame, Frame::StreamData { .. }) {
                        let sent_bytes = reliable_stream_frame_accounted_bytes(&frame);
                        if data_role.is_some() && !claimed_load {
                            // Validation attachment is not demand. Its first
                            // unique OwnerData commits this flow's carrier load
                            // so concurrent flows divide capacity and explore a
                            // different idle Subflow when one exists.
                            remotes.reserve_path_instance_load_if_needed(
                                context,
                                instance,
                                selection_lane,
                            );
                        }
                        context.record_relay_path_send(
                            instance.key.underlay,
                            instance.key.index,
                            sent_bytes,
                        );
                        if data_role == Some(PathRuntimeRole::Service) {
                            self.ordered_data_owner = Some(instance.key);
                            self.ordered_data_owner_instance = Some(instance);
                        } else if data_role == Some(PathRuntimeRole::Subflow)
                            && self
                                .request_startup
                                .epoch
                                .as_ref()
                                .and_then(FlowSubflowSet::startup_owner_key)
                                == Some(instance)
                        {
                            self.request_startup
                                .first_sent_at
                                .entry(instance)
                                .or_insert_with(Instant::now);
                            self.try_enqueue_request_startup_receipt_proof(
                                context, remotes, instance,
                            );
                        }
                    }
                    self.next_send_index = if remotes.paths.is_empty() {
                        0
                    } else {
                        (position + 1) % remotes.paths.len()
                    };
                    return Ok(instance);
                }
                Err(RuntimeError::SenderServiceBlocked) => {
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                Err(err) => {
                    last_error = Some(err);
                    if self.ordered_data_owner_instance == Some(instance) {
                        self.ordered_data_owner = None;
                        self.ordered_data_owner_instance = None;
                        self.reset_request_subflow_epoch();
                    } else if self
                        .request_startup
                        .epoch
                        .as_ref()
                        .and_then(FlowSubflowSet::startup_owner_key)
                        == Some(instance)
                    {
                        self.request_startup.epoch = None;
                        self.request_startup.acked_bytes.remove(&instance);
                        self.request_startup.first_sent_at.remove(&instance);
                        self.request_startup.rate_evidence.remove(&instance);
                        self.request_startup.receipt_proofs.remove(&instance);
                        self.request_graduated_subflows.remove(&instance);
                    }
                    self.fail_client_path_instance(context, remotes, instance)
                        .await;
                    if !remotes.paths.is_empty() {
                        self.next_send_index %= remotes.paths.len();
                    } else {
                        self.next_send_index = 0;
                    }
                }
            }
        }
        Err(last_error.unwrap_or(RuntimeError::ReliablePathSessionClosed))
    }

    fn choose_relay_path_position(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        frame: &Frame,
        lane: FlowLane,
        cause: RelaySendCause,
        avoid_keys: &[RelayPathKey],
    ) -> Result<RelayPathSendSelection, RuntimeError> {
        if remotes.paths.is_empty() {
            return Err(RuntimeError::ReliablePathSessionClosed);
        }
        self.retry_request_startup_receipt_proof(context, remotes);
        if !cause.is_repair()
            && let Some((offset, _, _)) = reliable_stream_frame_extent(frame)
            && self
                .flights
                .has_missing_ordering_owner_before_offset(offset, &remotes.path_instances())
        {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        self.next_send_index %= remotes.paths.len();
        if self.ordered_data_owner_instance.is_none()
            && let Some(owner) = self.ordered_data_owner
        {
            self.ordered_data_owner_instance = remotes
                .paths
                .iter()
                .find(|path| path.key() == owner)
                .map(ReliableRelayRemotePath::instance);
        }
        if self
            .ordered_data_owner_instance
            .is_some_and(|owner| !remotes.contains_path_instance(owner))
        {
            self.ordered_data_owner = None;
            self.ordered_data_owner_instance = None;
            self.reset_request_subflow_epoch();
        }
        self.reconcile_request_subflow_set(context, remotes);
        if matches!(frame, Frame::StreamData { .. }) && !cause.is_repair() {
            self.try_start_request_tcp_capacity_calibration(context, remotes, lane);
            self.try_start_request_quic_capacity_calibration(context, remotes, lane);
            let payload_bytes = reliable_stream_frame_accounted_bytes(frame);
            let sealed_owner = self.request_startup.epoch.as_mut().and_then(|epoch| {
                let owner = epoch.startup_owner_key()?;
                epoch
                    .seal_startup_owner_if_next_frame_exceeds_credit(owner, payload_bytes)
                    .then_some(owner)
            });
            if let Some(owner) = sealed_owner {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_startup_receipt",
                    format_args!(
                        "phase=sealed stream_id={} underlay={:?} path_index={} instance_id={} next_payload_bytes={}",
                        self.stream_id.0,
                        owner.key.underlay,
                        owner.key.index,
                        owner.id,
                        payload_bytes,
                    ),
                );
                self.try_enqueue_request_startup_receipt_proof(context, remotes, owner);
            }
            match choose_bulk_relay_path_avoiding(BulkRelayFrameRequest {
                stream_id: remotes.stream_id(),
                context,
                paths: &remotes.paths,
                lane,
                frame,
                cursor: self.next_send_index,
                avoid_keys,
                path_flights: Some(&self.flights),
                ordered_data_owner: self.ordered_data_owner,
                subflow_set: self.request_startup.epoch.as_ref(),
                proven_subflows: Some(&self.request_rate_proven_subflows),
                graduated_subflows: Some(&self.request_graduated_subflows),
                attempted_subflows: Some(&self.request_startup.attempted_subflows),
                ack_clock_calibration: Some(RequestAckClockCalibration {
                    owner: self.request_ack_clock_calibration_owner,
                    pending: self.request_ack_clock_calibration_pending,
                    proven_subflows: &self.request_ack_clock_proven_subflows,
                    first_window_acked_subflows: &self.request_ack_clock_first_window_subflows,
                    spent_bytes: &self.request_ack_clock_calibration_bytes,
                    tcp_carrier_proven_candidates: Some(&self.request_tcp_capacity_proven_subflows),
                }),
                request_per_flow_rate_bps: Some(&self.request_per_flow_rate_bps),
            }) {
                BulkRelayPathChoice::Selected(position) => {
                    let key = remotes.paths[position].key();
                    let role = if self.ordered_data_owner.is_none_or(|owner| owner == key) {
                        PathRuntimeRole::Service
                    } else {
                        PathRuntimeRole::Subflow
                    };
                    return Ok(RelayPathSendSelection::new(position, Some(role)));
                }
                BulkRelayPathChoice::SelectedStartupSubflow {
                    position,
                    service,
                    candidate,
                    load_expectation,
                } => {
                    let payload_bytes = reliable_stream_frame_accounted_bytes(frame);
                    let admission = self
                        .request_startup
                        .plan_admission(context.mux_limits, service, candidate, payload_bytes)
                        .ok_or(RuntimeError::SenderServiceBlocked)?;
                    return Ok(RelayPathSendSelection {
                        position,
                        data_role: Some(PathRuntimeRole::Subflow),
                        request_startup_commit: Some(admission),
                        request_calibration_commit: None,
                        request_load_expectation: load_expectation
                            .map(|(active, latency)| (candidate.key, active, latency)),
                    });
                }
                BulkRelayPathChoice::SelectedAckClockCalibration {
                    position,
                    candidate,
                    target_bytes,
                } => {
                    let payload_bytes = reliable_stream_frame_accounted_bytes(frame) as u64;
                    let entry_offset = reliable_stream_frame_extent(frame)
                        .map(|(offset, _, _)| offset)
                        .unwrap_or(0);
                    let service_key = self
                        .ordered_data_owner_instance
                        .map(|service| service.key)
                        .unwrap_or(candidate.key);
                    let (foreign_optional_ranges, foreign_optional_bytes) = if self
                        .request_ack_clock_calibration_owner
                        .is_none()
                    {
                        self.flights
                            .foreign_ordering_owner_debt_before_offset(entry_offset, &[service_key])
                    } else {
                        (0, 0)
                    };
                    return Ok(RelayPathSendSelection {
                        position,
                        data_role: Some(PathRuntimeRole::Subflow),
                        request_startup_commit: None,
                        request_calibration_commit: Some(
                            RequestAckClockCalibrationCommit::OwnerData {
                                candidate,
                                target_bytes,
                                payload_bytes,
                                entry_offset,
                                foreign_optional_ranges,
                                foreign_optional_bytes,
                            },
                        ),
                        request_load_expectation: None,
                    });
                }
                BulkRelayPathChoice::SelectedAckClockCalibrationFence {
                    position,
                    candidate,
                } => {
                    let service = remotes.paths[position].instance();
                    debug_assert_eq!(Some(service), self.ordered_data_owner_instance);
                    let entry_offset = reliable_stream_frame_extent(frame)
                        .map(|(offset, _, _)| offset)
                        .unwrap_or(0);
                    let (foreign_optional_ranges, foreign_optional_bytes) = if self
                        .request_ack_clock_calibration_owner
                        .is_none()
                        && self.request_ack_clock_calibration_pending.is_none()
                    {
                        self.flights
                            .foreign_ordering_owner_debt_before_offset(entry_offset, &[service.key])
                    } else {
                        (0, 0)
                    };
                    return Ok(RelayPathSendSelection {
                        position,
                        data_role: Some(PathRuntimeRole::Service),
                        request_startup_commit: None,
                        request_calibration_commit: Some(
                            RequestAckClockCalibrationCommit::ServiceFence {
                                service,
                                candidate,
                                entry_offset,
                                foreign_optional_ranges,
                                foreign_optional_bytes,
                            },
                        ),
                        request_load_expectation: None,
                    });
                }
                BulkRelayPathChoice::Blocked => return Err(RuntimeError::SenderServiceBlocked),
                BulkRelayPathChoice::NotApplicable => {}
            }
        }
        let position = self.choose_lowest_eta_relay_path(
            context,
            remotes,
            frame,
            lane,
            cause,
            avoid_keys,
            matches!(frame, Frame::StreamData { .. }) && !cause.is_repair(),
        )?;
        let data_role = if matches!(frame, Frame::StreamData { .. }) && !cause.is_repair() {
            Some(PathRuntimeRole::Service)
        } else {
            None
        };
        Ok(RelayPathSendSelection::new(position, data_role))
    }

    fn reset_request_subflow_epoch(&mut self) {
        self.request_startup.reset_epoch();
        // Pending entry owns no candidate bytes and is valid only for the exact
        // Service epoch that drained its lower optional frontier.
        self.request_ack_clock_calibration_pending = None;
    }

    fn commit_request_ack_clock_calibration(
        &mut self,
        commit: Option<RequestAckClockCalibrationCommit>,
    ) {
        let Some(commit) = commit else {
            return;
        };
        match commit {
            RequestAckClockCalibrationCommit::ServiceFence {
                service,
                candidate,
                entry_offset,
                foreign_optional_ranges,
                foreign_optional_bytes,
            } => {
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = (
                    entry_offset,
                    foreign_optional_ranges,
                    foreign_optional_bytes,
                );
                if let Some(owner) = self.request_ack_clock_calibration_owner {
                    debug_assert_eq!(owner.candidate, candidate);
                    return;
                }
                debug_assert_eq!(self.ordered_data_owner_instance, Some(service));
                let pending = RequestAckClockCalibrationPending { service, candidate };
                if self.request_ack_clock_calibration_pending == Some(pending) {
                    return;
                }
                self.request_ack_clock_calibration_pending = Some(pending);
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "ack_clock_calibration",
                    format_args!(
                        "phase=pending_started stream_id={} service_underlay={:?} service_index={} service_instance={} candidate_index={} candidate_instance={} entry_offset={} foreign_optional_ranges={} foreign_optional_bytes={}",
                        self.stream_id.0,
                        service.key.underlay,
                        service.key.index,
                        service.id,
                        candidate.key.index,
                        candidate.id,
                        entry_offset,
                        foreign_optional_ranges,
                        foreign_optional_bytes,
                    ),
                );
            }
            RequestAckClockCalibrationCommit::OwnerData {
                candidate,
                target_bytes,
                payload_bytes,
                entry_offset,
                foreign_optional_ranges,
                foreign_optional_bytes,
            } => {
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = (
                    entry_offset,
                    foreign_optional_ranges,
                    foreign_optional_bytes,
                );
                if self
                    .request_ack_clock_calibration_owner
                    .is_some_and(|owner| owner.candidate != candidate)
                {
                    debug_assert!(false, "calibration owner changed before enqueue commit");
                    return;
                }
                if let Some(pending) = self.request_ack_clock_calibration_pending {
                    debug_assert_eq!(pending.candidate, candidate);
                    debug_assert_eq!(Some(pending.service), self.ordered_data_owner_instance);
                }
                let beginning = self.request_ack_clock_calibration_owner.is_none();
                let target_bytes = self
                    .request_ack_clock_calibration_owner
                    .map_or(target_bytes, |owner| owner.target_bytes);
                let previous_bytes = if beginning {
                    0
                } else {
                    self.request_ack_clock_calibration_bytes
                        .get(&candidate)
                        .copied()
                        .unwrap_or(0)
                };
                let spent_bytes = previous_bytes.saturating_add(payload_bytes);
                self.request_ack_clock_calibration_pending = None;
                self.request_ack_clock_calibration_owner = Some(RequestAckClockCalibrationOwner {
                    candidate,
                    target_bytes,
                });
                self.request_ack_clock_calibration_targets
                    .insert(candidate, target_bytes);
                self.request_ack_clock_calibration_bytes
                    .insert(candidate, spent_bytes);
                #[cfg(feature = "lab-diagnostics")]
                {
                    if beginning {
                        lab_diagnostic(
                            "ack_clock_calibration",
                            format_args!(
                                "phase=owner_started stream_id={} underlay={:?} path_index={} instance_id={} payload_bytes={} target_bytes={} entry_offset={} foreign_optional_ranges={} foreign_optional_bytes={}",
                                self.stream_id.0,
                                candidate.key.underlay,
                                candidate.key.index,
                                candidate.id,
                                payload_bytes,
                                target_bytes,
                                entry_offset,
                                foreign_optional_ranges,
                                foreign_optional_bytes,
                            ),
                        );
                    }
                    if previous_bytes < target_bytes && spent_bytes >= target_bytes {
                        lab_diagnostic(
                            "ack_clock_calibration",
                            format_args!(
                                "phase=target_spent stream_id={} underlay={:?} path_index={} instance_id={} spent_bytes={} target_bytes={}",
                                self.stream_id.0,
                                candidate.key.underlay,
                                candidate.key.index,
                                candidate.id,
                                spent_bytes,
                                target_bytes,
                            ),
                        );
                    }
                    static TRACE_COUNT: std::sync::atomic::AtomicU64 =
                        std::sync::atomic::AtomicU64::new(0);
                    let count = TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if count < 16 || count % 256 == 0 {
                        lab_diagnostic(
                            "ack_clock_calibration",
                            format_args!(
                                "phase=selected stream_id={} underlay={:?} path_index={} instance_id={} payload_bytes={} spent_bytes={} target_bytes={}",
                                self.stream_id.0,
                                candidate.key.underlay,
                                candidate.key.index,
                                candidate.id,
                                payload_bytes,
                                spent_bytes,
                                target_bytes,
                            ),
                        );
                    }
                }
            }
        }
    }

    fn retry_request_startup_receipt_proof(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
    ) {
        let Some(owner) = self
            .request_startup
            .epoch
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key)
        else {
            return;
        };
        self.try_enqueue_request_startup_receipt_proof(context, remotes, owner);
    }

    fn try_enqueue_request_startup_receipt_proof(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        owner: RelayPathInstance,
    ) {
        if owner.key.underlay != UnderlayProtocol::Tcp {
            return;
        }
        let proof_generation = context
            .relay_path_proof_generation(owner.key.underlay, owner.key.index)
            .unwrap_or(0);
        if self
            .request_startup
            .receipt_proofs
            .get(&owner)
            .is_some_and(|(_, generation)| *generation == proof_generation)
            || !self
                .request_startup
                .epoch
                .as_ref()
                .is_some_and(|epoch| epoch.startup_owner_sample_sealed(owner))
        {
            return;
        }
        let Some(path) = remotes.paths.iter().find(|path| {
            path.instance() == owner && path.placement == RelayPathPlacement::Validation
        }) else {
            return;
        };
        match path
            .stream
            .enqueue_stream_ordered_path_proof(path.stream.lane)
        {
            Ok(Some(proof_id)) => {
                self.request_startup
                    .receipt_proofs
                    .insert(owner, (proof_id, proof_generation));
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_startup_receipt",
                    format_args!(
                        "phase=queued stream_id={} underlay={:?} path_index={} instance_id={} proof_id={} proof_generation={}",
                        self.stream_id.0,
                        owner.key.underlay,
                        owner.key.index,
                        owner.id,
                        proof_id,
                        proof_generation,
                    ),
                );
            }
            Ok(None) => {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_startup_receipt",
                    format_args!(
                        "phase=unsupported stream_id={} underlay={:?} path_index={} instance_id={} proof_generation={}",
                        self.stream_id.0,
                        owner.key.underlay,
                        owner.key.index,
                        owner.id,
                        proof_generation,
                    ),
                );
            }
            Err(err) => {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_startup_receipt",
                    format_args!(
                        "phase=queue_failed stream_id={} underlay={:?} path_index={} instance_id={} proof_generation={} error={}",
                        self.stream_id.0,
                        owner.key.underlay,
                        owner.key.index,
                        owner.id,
                        proof_generation,
                        err,
                    ),
                );
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = err;
            }
        }
    }

    fn request_ack_clock_calibration_target_is_sealed(&self, target: RelayPathInstance) -> bool {
        self.request_ack_clock_calibration_owner
            .filter(|owner| owner.candidate == target)
            .is_some_and(|owner| {
                self.request_ack_clock_calibration_bytes
                    .get(&target)
                    .is_some_and(|spent| *spent >= owner.target_bytes)
            })
    }

    fn revoke_request_tcp_capacity_calibration(
        &mut self,
        target: RelayPathInstance,
        preserve_committed_product: bool,
    ) -> bool {
        self.request_tcp_capacity_proven_subflows.remove(&target);
        self.request_tcp_capacity_calibrations.remove(&target);
        let product_transaction_preserved = preserve_committed_product
            && self.request_ack_clock_calibration_target_is_sealed(target);
        if product_transaction_preserved {
            // Carrier freshness admits a bounded product transaction but does
            // not own it. Once the fixed target is sealed, keep its exact ACK
            // evidence until product proof or a real path lifecycle change.
            return true;
        }
        self.request_graduated_subflows.remove(&target);
        self.request_rate_proven_subflows.remove(&target);
        self.request_ack_clock_first_window_subflows.remove(&target);
        self.request_ack_clock_proven_subflows.remove(&target);
        self.request_window_turnover_proven_subflows.remove(&target);
        self.request_rate_evidence.remove(&target);
        self.request_tcp_ack_turnover.remove(&target);
        self.request_ack_clock_calibration_bytes.remove(&target);
        self.request_ack_clock_calibration_targets.remove(&target);
        if self
            .request_ack_clock_calibration_owner
            .is_some_and(|owner| owner.candidate == target)
        {
            self.request_ack_clock_calibration_owner = None;
        }
        if self
            .request_ack_clock_calibration_pending
            .is_some_and(|pending| pending.candidate == target)
        {
            self.request_ack_clock_calibration_pending = None;
        }
        false
    }

    fn reconcile_request_subflow_set(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
    ) {
        let membership_generation = remotes.membership_generation();
        if self.request_membership_generation != Some(membership_generation) {
            let live_instances = remotes.path_instances().into_iter().collect::<HashSet<_>>();
            self.request_startup.retain_live(&live_instances);
            self.request_graduated_subflows
                .retain(|instance| live_instances.contains(instance));
            self.request_rate_proven_subflows
                .retain(|instance| live_instances.contains(instance));
            self.request_ack_clock_first_window_subflows
                .retain(|instance| live_instances.contains(instance));
            self.request_ack_clock_proven_subflows
                .retain(|instance| live_instances.contains(instance));
            self.request_window_turnover_proven_subflows
                .retain(|instance| live_instances.contains(instance));
            self.request_tcp_capacity_proven_subflows
                .retain(|instance| live_instances.contains(instance));
            self.request_ack_clock_calibration_bytes
                .retain(|instance, _| live_instances.contains(instance));
            self.request_ack_clock_calibration_targets
                .retain(|instance, _| live_instances.contains(instance));
            self.request_rate_evidence
                .retain(|instance, _| live_instances.contains(instance));
            self.request_per_flow_rate_bps
                .retain(|instance, _| live_instances.contains(instance));
            self.request_tcp_ack_turnover
                .retain(|instance, _| live_instances.contains(instance));
            self.request_membership_generation = Some(membership_generation);
        }
        let now = Instant::now();
        let detached_tcp_capacity_targets = self
            .request_tcp_capacity_calibrations
            .keys()
            .copied()
            .filter(|target| !remotes.contains_path_instance(*target))
            .collect::<Vec<_>>();
        for target in detached_tcp_capacity_targets {
            self.revoke_request_tcp_capacity_calibration(target, false);
        }
        let tcp_capacities = self
            .request_tcp_capacity_calibrations
            .values()
            .map(|calibration| {
                let proof = context.request_tcp_capacity_probe_proof(
                    self.stream_id,
                    calibration.target.key.index,
                    calibration.target,
                    calibration.token,
                );
                (
                    calibration.target,
                    calibration.token,
                    calibration.graduated,
                    calibration.publication_expires_at,
                    calibration.proof_expires_at,
                    calibration.lease.is_current(),
                    calibration.lease.is_published(),
                    proof,
                )
            })
            .collect::<Vec<_>>();
        for (
            target,
            _token,
            graduated,
            publication_expires_at,
            proof_expires_at,
            current,
            published,
            proof,
        ) in tcp_capacities
        {
            if self.request_ack_clock_proven_subflows.contains(&target)
                && self.request_per_flow_rate_bps.contains_key(&target)
            {
                self.request_tcp_capacity_proven_subflows.remove(&target);
                self.request_tcp_capacity_calibrations.remove(&target);
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_tcp_capacity_calibration",
                    format_args!(
                        "phase=handoff_complete stream_id={} path_index={} instance_id={} calibration_id={}",
                        self.stream_id.0, target.key.index, target.id, _token,
                    ),
                );
                continue;
            }
            if !graduated {
                if let Some(proof) = proof {
                    if let Some(calibration) =
                        self.request_tcp_capacity_calibrations.get_mut(&target)
                    {
                        calibration.graduated = true;
                        calibration.proof_expires_at = Some(proof.expires_at);
                    }
                    self.request_tcp_capacity_proven_subflows.insert(target);
                    self.request_graduated_subflows.insert(target);
                    self.request_rate_evidence
                        .entry(target)
                        .or_insert_with(|| RequestPathRateEvidence::new(proof.accepted_at))
                        .seed_ack_boundary(proof.accepted_at);
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "request_tcp_capacity_calibration",
                        format_args!(
                            "phase=carrier_proven stream_id={} path_index={} instance_id={} calibration_id={} train_bytes={} rate_mbps={:.3} proof_ms={}",
                            self.stream_id.0,
                            target.key.index,
                            target.id,
                            _token,
                            proof.train_bytes,
                            proof.rate_bps as f64 / 1_000_000.0,
                            proof
                                .expires_at
                                .saturating_duration_since(proof.accepted_at)
                                .as_millis(),
                        ),
                    );
                } else if now >= publication_expires_at || !current {
                    self.revoke_request_tcp_capacity_calibration(target, false);
                }
            } else if request_tcp_carrier_authority_expired_naturally(
                published,
                proof_expires_at,
                now,
            ) {
                let _product_transaction_preserved =
                    self.revoke_request_tcp_capacity_calibration(target, true);
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_tcp_capacity_calibration",
                    format_args!(
                        "phase=carrier_authority_expired stream_id={} path_index={} instance_id={} calibration_id={} product_transaction_preserved={}",
                        self.stream_id.0,
                        target.key.index,
                        target.id,
                        _token,
                        _product_transaction_preserved,
                    ),
                );
            } else if proof.is_none() || !published {
                self.revoke_request_tcp_capacity_calibration(target, false);
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_tcp_capacity_calibration",
                    format_args!(
                        "phase=revoked stream_id={} path_index={} instance_id={} calibration_id={} reason=carrier_authority_lost",
                        self.stream_id.0, target.key.index, target.id, _token,
                    ),
                );
            }
        }
        if self
            .request_quic_capacity_calibration
            .as_ref()
            .is_some_and(|calibration| !remotes.contains_path_instance(calibration.target))
        {
            self.request_quic_capacity_calibration = None;
        }
        // A proof accepted at the train deadline remains authoritative for its
        // own proof lifetime, even if sender reconciliation runs just after it.
        let graduated = self
            .request_quic_capacity_calibration
            .as_ref()
            .filter(|calibration| !calibration.graduated)
            .filter(|calibration| {
                context.request_quic_capacity_probe_proven(
                    calibration.target.key.index,
                    calibration.token,
                )
            })
            .map(|calibration| (calibration.target, calibration.token));
        if let Some((_target, _token)) = graduated {
            if let Some(calibration) = self.request_quic_capacity_calibration.as_mut() {
                calibration.graduated = true;
            }
            self.request_graduated_subflows.insert(_target);
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "request_quic_capacity_calibration",
                format_args!(
                    "phase=graduated stream_id={} path_index={} instance_id={} calibration_id={}",
                    self.stream_id.0, _target.key.index, _target.id, _token,
                ),
            );
        }
        if self
            .request_quic_capacity_calibration
            .as_ref()
            .is_some_and(|calibration| {
                !calibration.graduated && now >= calibration.publication_expires_at
            })
        {
            self.request_quic_capacity_calibration = None;
        }
        let handoff = self
            .request_quic_capacity_calibration
            .as_ref()
            .filter(|calibration| calibration.graduated)
            .map(|calibration| {
                (
                    calibration.target,
                    calibration.token,
                    context.request_quic_capacity_product_handoff_state(
                        calibration.target.key.index,
                        calibration.token,
                    ),
                )
            });
        if let Some((_target, _token, state)) = handoff
            && state != RequestQuicCapacityProductHandoffState::Pending
        {
            if state == RequestQuicCapacityProductHandoffState::Absent {
                // Ephemeral proof credit is transactional: an incomplete or
                // failed handoff cannot leave the Validation path owner-capable.
                self.request_graduated_subflows.remove(&_target);
            }
            self.request_quic_capacity_calibration = None;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "request_quic_capacity_calibration",
                format_args!(
                    "phase={} stream_id={} path_index={} instance_id={} calibration_id={}",
                    match state {
                        RequestQuicCapacityProductHandoffState::Complete => "handoff_complete",
                        RequestQuicCapacityProductHandoffState::Absent => "handoff_expired",
                        RequestQuicCapacityProductHandoffState::Pending => unreachable!(),
                    },
                    self.stream_id.0,
                    _target.key.index,
                    _target.id,
                    _token,
                ),
            );
        }
        if self
            .request_ack_clock_calibration_pending
            .is_some_and(|pending| {
                self.ordered_data_owner_instance != Some(pending.service)
                    || pending.service.key.underlay != UnderlayProtocol::Tcp
                    || self
                        .request_ack_clock_proven_subflows
                        .contains(&pending.candidate)
                    || !self.request_graduated_subflows.contains(&pending.candidate)
                    || !(self
                        .request_ack_clock_first_window_subflows
                        .contains(&pending.candidate)
                        || self
                            .request_tcp_capacity_proven_subflows
                            .contains(&pending.candidate))
                    || !remotes.paths.iter().any(|path| {
                        path.instance() == pending.service
                            && path.placement == RelayPathPlacement::Active
                    })
                    || !remotes.paths.iter().any(|path| {
                        path.instance() == pending.candidate
                            && path.placement == RelayPathPlacement::Validation
                            && path.key().underlay == UnderlayProtocol::Tcp
                            && path.key().underlay == pending.service.key.underlay
                    })
            })
        {
            self.request_ack_clock_calibration_pending = None;
        }
        if let Some(owner) = self.request_ack_clock_calibration_owner {
            if self
                .request_ack_clock_proven_subflows
                .contains(&owner.candidate)
            {
                self.request_ack_clock_calibration_owner = None;
            } else {
                let placement_valid = self.request_graduated_subflows.contains(&owner.candidate)
                    && remotes.paths.iter().any(|path| {
                        path.instance() == owner.candidate
                            && path.placement == RelayPathPlacement::Validation
                    });
                let transaction_authorized = self
                    .request_ack_clock_calibration_target_is_sealed(owner.candidate)
                    || self
                        .request_ack_clock_first_window_subflows
                        .contains(&owner.candidate)
                    || self
                        .request_tcp_capacity_proven_subflows
                        .contains(&owner.candidate);
                if !placement_valid || !transaction_authorized {
                    // A sealed AwaitingAck target remains exact-instance state.
                    // Real placement loss or a partial transaction without its
                    // entry proof performs the full abort cleanup.
                    self.revoke_request_tcp_capacity_calibration(owner.candidate, false);
                }
            }
        }
        if self.ordered_data_owner_instance.is_some_and(|service| {
            service.key.underlay == UnderlayProtocol::Udp && remotes.contains_path_instance(service)
        }) {
            for path in &remotes.paths {
                let instance = path.instance();
                let native_evidence = path.placement == RelayPathPlacement::Validation
                    && instance.key.underlay == UnderlayProtocol::Udp
                    && path.path_proof_id.is_some_and(|proof_id| {
                        context.relay_path_has_fresh_proof(
                            instance.key.underlay,
                            instance.key.index,
                            proof_id,
                            path.attached_at,
                        )
                    })
                    && context.relay_path_has_native_bulk_model_evidence_since(
                        instance.key.underlay,
                        instance.key.index,
                        path.attached_at,
                    );
                if native_evidence {
                    // QUIC graduates directly from its fresh, non-app-limited
                    // packet-ACK model. No ordered product sample or receipt
                    // marker participates in this carrier decision.
                    self.request_graduated_subflows.insert(instance);
                }
            }
        }
        let service = self.ordered_data_owner_instance.filter(|owner| {
            remotes.paths.iter().any(|path| {
                path.instance() == *owner && path.placement == RelayPathPlacement::Active
            })
        });
        if self
            .request_startup
            .epoch
            .as_ref()
            .is_some_and(|epoch| service.is_none_or(|service| epoch.service_key() != service))
        {
            self.reset_request_subflow_epoch();
            return;
        }
        let Some(owner) = self
            .request_startup
            .epoch
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key)
        else {
            return;
        };
        if owner.key.underlay != UnderlayProtocol::Tcp {
            self.reset_request_subflow_epoch();
            return;
        }
        if !remotes.paths.iter().any(|path| {
            path.instance() == owner && path.placement == RelayPathPlacement::Validation
        }) {
            self.request_startup.epoch = None;
            self.request_startup.acked_bytes.remove(&owner);
            self.request_startup.first_sent_at.remove(&owner);
            self.request_startup.rate_evidence.remove(&owner);
            self.request_startup.receipt_proofs.remove(&owner);
            return;
        }
        let required_evidence_bytes = self
            .request_startup
            .epoch
            .as_ref()
            .and_then(|epoch| epoch.startup_owner_sealed_sample_bytes(owner))
            .unwrap_or(u64::MAX);
        let receipt_acked_at = self
            .request_startup
            .receipt_proofs
            .get(&owner)
            .copied()
            .and_then(|(proof_id, generation)| {
                (context.relay_path_proof_generation(owner.key.underlay, owner.key.index)
                    == Some(generation))
                .then_some(proof_id)
            })
            .and_then(|proof_id| {
                remotes
                    .paths
                    .iter()
                    .find(|path| path.instance() == owner)
                    .and_then(|path| {
                        context.relay_path_fresh_proof_acked_at(
                            owner.key.underlay,
                            owner.key.index,
                            proof_id,
                            path.attached_at,
                        )
                    })
            });
        if let Some(receipt_acked_at) = receipt_acked_at
            && !self.request_startup.rate_evidence.contains(&owner)
            && let Some(first_sent_at) = self.request_startup.first_sent_at.get(&owner).copied()
            && let Some(sample) = PathRateSample::new(
                required_evidence_bytes,
                receipt_acked_at.saturating_duration_since(first_sent_at),
            )
        {
            self.request_startup.rate_evidence.insert(owner);
            self.request_rate_proven_subflows.insert(owner);
            self.record_request_per_flow_rate_sample(owner, sample, false);
            context.mark_relay_path_rate_sample(owner.key.underlay, owner.key.index, sample);
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "request_startup_receipt",
                format_args!(
                    "phase=rate_sample stream_id={} underlay={:?} path_index={} instance_id={} evidence_bytes={} elapsed_us={} rate_bps={}",
                    self.stream_id.0,
                    owner.key.underlay,
                    owner.key.index,
                    owner.id,
                    required_evidence_bytes,
                    receipt_acked_at
                        .saturating_duration_since(first_sent_at)
                        .as_micros(),
                    sample.rate_bps(),
                ),
            );
        }
        if let Some(receipt_acked_at) = receipt_acked_at
            && self.request_startup.rate_evidence.contains(&owner)
            && self.request_ack_clock_first_window_subflows.insert(owner)
        {
            // The ordered receipt follows the sealed startup sample on this
            // exact TCP attachment. Once product flight also drains below, it
            // is the causal boundary for the first calibration window.
            self.request_rate_evidence
                .entry(owner)
                .or_insert_with(|| RequestPathRateEvidence::new(receipt_acked_at))
                .seed_ack_boundary(receipt_acked_at);
        }
        if self.request_startup.rate_evidence.contains(&owner)
            && !self.flights.has_ordering_owner_flights_for_instance(owner)
            && let Some(epoch) = self.request_startup.epoch.as_mut()
        {
            let graduated = epoch.graduate_startup_owner(owner);
            debug_assert!(graduated);
            self.request_graduated_subflows.insert(owner);
            self.request_startup.receipt_proofs.remove(&owner);
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "request_startup_receipt",
                format_args!(
                    "phase=graduated stream_id={} underlay={:?} path_index={} instance_id={}",
                    self.stream_id.0, owner.key.underlay, owner.key.index, owner.id,
                ),
            );
        }
    }

    #[cfg(feature = "lab-diagnostics")]
    fn diagnose_request_tcp_capacity_gate(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        lane: FlowLane,
    ) {
        let active_tcp_service_flows = context.active_tcp_service_request_bulk_flows();
        let latency_pressure = context.reliable_relay_has_latency_pressure();
        let service_path = self.ordered_data_owner_instance.and_then(|service| {
            (service.key.underlay == UnderlayProtocol::Tcp)
                .then(|| {
                    remotes.paths.iter().find(|path| {
                        path.instance() == service && path.placement == RelayPathPlacement::Active
                    })
                })
                .flatten()
        });
        let service_instance = service_path.map(ReliableRelayRemotePath::instance);
        let service_snapshot =
            service_instance.and_then(|instance| context.reliable_path_snapshot(instance.key));
        let service_bulk_evidence = service_instance.is_some_and(|instance| {
            context.relay_path_has_bulk_model_evidence(instance.key.underlay, instance.key.index)
        });
        let service_model = service_instance
            .and_then(|instance| self.request_per_flow_rate_bps.get(&instance).copied());
        let candidate_path = remotes
            .paths
            .iter()
            .filter(|path| {
                path.placement == RelayPathPlacement::Validation
                    && path.key().underlay == UnderlayProtocol::Tcp
                    && context.relay_path_allows_automatic_bulk_use(path.key())
                    && !self
                        .request_tcp_capacity_attempted_paths
                        .contains(&path.key().index)
            })
            .min_by_key(|path| context.relay_path_config_ordinal(path.key()));
        let candidate_instance = candidate_path.map(ReliableRelayRemotePath::instance);
        let candidate_snapshot =
            candidate_instance.and_then(|instance| context.reliable_path_snapshot(instance.key));
        let eligible_candidates = context.automatic_bulk_path_count(
            UnderlayProtocol::Tcp,
            service_instance.map(|instance| instance.key.index),
        );
        let proposed_candidate_share =
            request_capacity_stable_candidate_share_bytes(context.mux_limits, eligible_candidates);
        let stable_candidate_share =
            context.request_tcp_capacity_probe_candidate_share_bytes(proposed_candidate_share);
        let campaign_remaining_bytes = self
            .request_tcp_capacity_campaign
            .remaining_bytes(stable_candidate_share);
        let session_remaining_bytes = context.request_tcp_capacity_probe_remaining_bytes();
        let train_envelope_bytes = candidate_instance.map_or(0, |instance| {
            session_remaining_bytes.min(campaign_remaining_bytes).min(
                context.request_tcp_capacity_probe_path_remaining_bytes(
                    instance.key.index,
                    stable_candidate_share,
                ),
            )
        });
        let geometry = candidate_snapshot
            .zip(service_model)
            .and_then(|(candidate, service)| {
                request_tcp_capacity_calibration_geometry(
                    candidate,
                    service,
                    context.mux_limits,
                    train_envelope_bytes,
                )
            });
        let candidate_bulk_evidence = candidate_instance.is_some_and(|instance| {
            context.relay_path_has_bulk_model_evidence(instance.key.underlay, instance.key.index)
        });
        let path_proof_fresh = candidate_path.is_some_and(|path| {
            let instance = path.instance();
            path.path_proof_id.is_some_and(|proof_id| {
                context.relay_path_has_fresh_proof(
                    instance.key.underlay,
                    instance.key.index,
                    proof_id,
                    path.attached_at,
                )
            })
        });
        let can_enqueue = candidate_path.is_some_and(|path| {
            path.stream
                .can_enqueue_work_lane_now(ReliableWorkClass::Data, lane)
        });
        let gate = if !lane.is_bulk() {
            "non_bulk_lane"
        } else if active_tcp_service_flows != 1 {
            "tcp_service_flow_count"
        } else if latency_pressure {
            "session_latency_pressure"
        } else if service_path.is_none() {
            "active_tcp_service_missing"
        } else if service_snapshot.is_none() {
            "service_snapshot_missing"
        } else if !service_bulk_evidence {
            "service_bulk_evidence_missing"
        } else if service_snapshot.is_some_and(|snapshot| {
            snapshot.active_latency_sensitive_flows > 0
                || snapshot.session_active_latency_sensitive_flows > 0
        }) {
            "service_latency_pressure"
        } else if service_model.is_none() {
            "service_flow_model_missing"
        } else if service_model.is_some_and(|model| {
            !product_delivery_samples_override_startup_prior(model.delivery_samples)
        }) {
            "service_flow_model_immature"
        } else if candidate_path.is_none() {
            "validation_tcp_missing"
        } else if candidate_instance
            .is_some_and(|instance| self.request_graduated_subflows.contains(&instance))
        {
            "candidate_already_graduated"
        } else if candidate_instance.is_some_and(|instance| {
            self.request_rate_evidence.contains_key(&instance)
                || self.request_per_flow_rate_bps.contains_key(&instance)
                || self.request_rate_proven_subflows.contains(&instance)
                || self.request_ack_clock_proven_subflows.contains(&instance)
        }) {
            "candidate_product_evidence_present"
        } else if candidate_path.is_some_and(|path| path.path_proof_id.is_none()) {
            "candidate_path_proof_missing"
        } else if !path_proof_fresh {
            "candidate_path_proof_stale"
        } else if candidate_bulk_evidence {
            "candidate_bulk_evidence_present"
        } else if candidate_snapshot.is_none() {
            "candidate_snapshot_missing"
        } else if geometry.is_none() {
            "train_geometry_unavailable"
        } else if candidate_snapshot.is_some_and(|snapshot| snapshot.queue_bytes > 0) {
            "candidate_carrier_queue"
        } else if candidate_snapshot.is_some_and(|snapshot| snapshot.product_bytes_in_flight > 0) {
            "candidate_product_inflight"
        } else if candidate_snapshot.is_some_and(|snapshot| snapshot.product_queue_bytes > 0) {
            "candidate_product_queue"
        } else if candidate_snapshot.is_some_and(|snapshot| {
            snapshot.active_latency_sensitive_flows > 0
                || snapshot.session_active_latency_sensitive_flows > 0
        }) {
            "candidate_latency_pressure"
        } else if !can_enqueue {
            "candidate_queue_credit_missing"
        } else {
            "eligible"
        };
        let signature = (candidate_instance, gate);
        if self.request_tcp_capacity_last_gate == Some(signature) {
            return;
        }
        self.request_tcp_capacity_last_gate = Some(signature);
        lab_diagnostic(
            "request_tcp_capacity_gate",
            format_args!(
                "stream_id={} first_failed_gate={} lane={:?} active_tcp_service_flows={} latency_pressure={} service_path_index={} service_bulk_evidence={} service_carrier_bif={} service_product_bif={} service_rate_mbps={:.3} service_delivery_samples={} candidate_path_index={} candidate_proof_id={} candidate_proof_fresh={} candidate_bulk_evidence={} candidate_carrier_bif={} candidate_queue_bytes={} candidate_product_bif={} candidate_product_queue_bytes={} can_enqueue={} train_bytes={} stable_candidate_share_bytes={} campaign_remaining_bytes={} train_envelope_bytes={} session_remaining_bytes={}",
                self.stream_id.0,
                gate,
                lane,
                active_tcp_service_flows,
                latency_pressure,
                service_instance.map_or(-1, |instance| instance.key.index as i64),
                service_bulk_evidence,
                service_snapshot.map_or(0, |snapshot| snapshot.bytes_in_flight),
                service_snapshot.map_or(0, |snapshot| snapshot.product_bytes_in_flight),
                service_model.map_or(0.0, |model| model.rate_bps / 1_000_000.0),
                service_model.map_or(0, |model| model.delivery_samples),
                candidate_instance.map_or(-1, |instance| instance.key.index as i64),
                candidate_path
                    .and_then(|path| path.path_proof_id)
                    .unwrap_or(0),
                path_proof_fresh,
                candidate_bulk_evidence,
                candidate_snapshot.map_or(0, |snapshot| snapshot.bytes_in_flight),
                candidate_snapshot.map_or(0, |snapshot| snapshot.queue_bytes),
                candidate_snapshot.map_or(0, |snapshot| snapshot.product_bytes_in_flight),
                candidate_snapshot.map_or(0, |snapshot| snapshot.product_queue_bytes),
                can_enqueue,
                geometry.map_or(0, |geometry| geometry.train_bytes),
                stable_candidate_share,
                campaign_remaining_bytes,
                train_envelope_bytes,
                context.request_tcp_capacity_probe_remaining_bytes(),
            ),
        );
    }

    fn try_start_request_tcp_capacity_calibration(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        lane: FlowLane,
    ) {
        #[cfg(feature = "lab-diagnostics")]
        self.diagnose_request_tcp_capacity_gate(context, remotes, lane);
        if !lane.is_bulk()
            || context.active_tcp_service_request_bulk_flows() != 1
            || context.reliable_relay_has_latency_pressure()
        {
            return;
        }
        let Some(service_path) = self.ordered_data_owner_instance.and_then(|service| {
            (service.key.underlay == UnderlayProtocol::Tcp).then(|| {
                remotes.paths.iter().find(|path| {
                    path.instance() == service && path.placement == RelayPathPlacement::Active
                })
            })?
        }) else {
            return;
        };
        let service = service_path.instance();
        let Some(service_snapshot) = context.reliable_path_snapshot(service.key) else {
            return;
        };
        if !context.relay_path_has_bulk_model_evidence(service.key.underlay, service.key.index)
            || service_snapshot.active_latency_sensitive_flows > 0
            || service_snapshot.session_active_latency_sensitive_flows > 0
        {
            return;
        }
        let Some(service_model) = self
            .request_per_flow_rate_bps
            .get(&service)
            .copied()
            .filter(|model| {
                product_delivery_samples_override_startup_prior(model.delivery_samples)
            })
        else {
            return;
        };
        // The Service model prices only train geometry. The candidate's full
        // receiver-confirmed train remains the sole cold startup-rate authority.
        // Freeze one fair share per configured eligible candidate. A late
        // path must not inherit the unused campaign budget of earlier paths.
        let eligible_candidates =
            context.automatic_bulk_path_count(UnderlayProtocol::Tcp, Some(service.key.index));
        let proposed_candidate_share =
            request_capacity_stable_candidate_share_bytes(context.mux_limits, eligible_candidates);
        let stable_candidate_share =
            context.request_tcp_capacity_probe_candidate_share_bytes(proposed_candidate_share);
        let session_remaining_bytes = context.request_tcp_capacity_probe_remaining_bytes();
        let mut candidates = remotes
            .paths
            .iter()
            .filter(|path| {
                let instance = path.instance();
                let snapshot = context.reliable_path_snapshot(instance.key);
                path.placement == RelayPathPlacement::Validation
                    && instance.key.underlay == UnderlayProtocol::Tcp
                    && context.relay_path_allows_automatic_bulk_use(instance.key)
                    && !self
                        .request_tcp_capacity_attempted_paths
                        .contains(&instance.key.index)
                    && !self.request_graduated_subflows.contains(&instance)
                    && !self.request_rate_evidence.contains_key(&instance)
                    && !self.request_per_flow_rate_bps.contains_key(&instance)
                    && !self.request_rate_proven_subflows.contains(&instance)
                    && !self.request_ack_clock_proven_subflows.contains(&instance)
                    && path.path_proof_id.is_some_and(|proof_id| {
                        context.relay_path_has_fresh_proof(
                            instance.key.underlay,
                            instance.key.index,
                            proof_id,
                            path.attached_at,
                        )
                    })
                    && !context.relay_path_has_bulk_model_evidence(
                        instance.key.underlay,
                        instance.key.index,
                    )
                    && snapshot.is_some_and(request_tcp_capacity_candidate_can_start_receipt)
                    && path
                        .stream
                        .can_enqueue_work_lane_now(ReliableWorkClass::Data, lane)
            })
            .filter_map(|path| {
                let candidate_snapshot = context.reliable_path_snapshot(path.key())?;
                let campaign_remaining_bytes = self
                    .request_tcp_capacity_campaign
                    .remaining_bytes(stable_candidate_share);
                let train_envelope_bytes = session_remaining_bytes
                    .min(campaign_remaining_bytes)
                    .min(context.request_tcp_capacity_probe_path_remaining_bytes(
                        path.key().index,
                        stable_candidate_share,
                    ));
                let geometry = request_tcp_capacity_calibration_geometry(
                    candidate_snapshot,
                    service_model,
                    context.mux_limits,
                    train_envelope_bytes,
                )?;
                Some((path, candidate_snapshot, geometry, train_envelope_bytes))
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(path, _, _, _)| context.relay_path_config_ordinal(path.key()));
        if candidates.is_empty() {
            return;
        }
        static NEXT_REQUEST_TCP_CAPACITY_ID: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(1);
        for (path, candidate_snapshot, geometry, _train_envelope_bytes) in candidates {
            let instance = path.instance();
            let train_payload_bytes = geometry.train_bytes;
            let token =
                NEXT_REQUEST_TCP_CAPACITY_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let ticket = QuicCapacityProbeCommandTicket::new();
            let now = Instant::now();
            let baseline_budget = transport_pto_from_snapshot(Some(candidate_snapshot));
            let lease_duration = request_tcp_capacity_calibration_lease(
                candidate_snapshot,
                train_payload_bytes,
                geometry.service_rate_bps,
            );
            let Some(baseline_expires_at) = now.checked_add(baseline_budget) else {
                continue;
            };
            let Some(expires_at) = now.checked_add(lease_duration) else {
                continue;
            };
            let Some(write_expires_at) = expires_at.checked_sub(baseline_budget) else {
                continue;
            };
            let Some(lease) = context.try_reserve_request_tcp_capacity_probe(
                self.stream_id,
                instance.key.index,
                instance,
                token,
                train_payload_bytes,
                stable_candidate_share,
                self.request_tcp_capacity_campaign.clone(),
                geometry.required_timed_carrier_bytes,
                path.attached_at,
                expires_at,
                ticket,
            ) else {
                continue;
            };
            let request = RequestTcpCapacityProbeRequest {
                stream_id: self.stream_id,
                path_instance: instance,
                path_id: PathId(instance.key.index as u16),
                calibration_id: token,
                train_payload_bytes,
                sample_floor_bytes: geometry.sample_floor_bytes,
                warmup_carrier_bytes: geometry.warmup_carrier_bytes,
                timing_slack_bytes: geometry.timing_slack_bytes,
                required_timed_carrier_bytes: geometry.required_timed_carrier_bytes,
                baseline_expires_at,
                write_expires_at,
                expires_at,
            };
            if path
                .stream
                .try_enqueue_request_tcp_capacity_probe(request, lease.clone())
                .is_err()
            {
                continue;
            }
            self.request_tcp_capacity_attempted_paths
                .insert(instance.key.index);
            if !lease.commit() {
                // The exact carrier dequeued and rejected this one-shot attempt
                // before planner commit without putting any train byte on wire.
                continue;
            }
            let previous = self.request_tcp_capacity_calibrations.insert(
                instance,
                RequestTcpCapacityCalibration {
                    target: instance,
                    token,
                    publication_expires_at: expires_at,
                    proof_expires_at: None,
                    graduated: false,
                    lease,
                },
            );
            debug_assert!(previous.is_none());
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "request_tcp_capacity_calibration",
                format_args!(
                    "phase=started stream_id={} path_index={} instance_id={} calibration_id={} train_bytes={} train_envelope_bytes={} sample_floor_bytes={} accounting_slack_bytes={} timing_slack_bytes={} warmup_bytes={} required_timed_bytes={} candidate_carrier_flight_bytes={} candidate_srtt_ms={:.3} candidate_jitter_ms={:.3} service_rate_mbps={:.3} service_delivery_samples={} baseline_budget_ms={} write_deadline_after_ms={} final_budget_ms={} lease_ms={}",
                    self.stream_id.0,
                    instance.key.index,
                    instance.id,
                    token,
                    train_payload_bytes,
                    _train_envelope_bytes,
                    geometry.sample_floor_bytes,
                    geometry.accounting_slack_bytes,
                    geometry.timing_slack_bytes,
                    geometry.warmup_carrier_bytes,
                    geometry.required_timed_carrier_bytes,
                    geometry.candidate_carrier_flight_bytes,
                    candidate_snapshot.srtt_ms,
                    candidate_snapshot.jitter_ms,
                    geometry.service_rate_bps as f64 / 1_000_000.0,
                    service_model.delivery_samples,
                    baseline_budget.as_millis(),
                    lease_duration.saturating_sub(baseline_budget).as_millis(),
                    baseline_budget.as_millis(),
                    lease_duration.as_millis(),
                ),
            );
        }
    }

    fn try_start_request_quic_capacity_calibration(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        lane: FlowLane,
    ) {
        if !lane.is_bulk() || self.request_quic_capacity_calibration.is_some() {
            return;
        }
        let has_unattempted_udp_candidate = remotes.paths.iter().any(|path| {
            let instance = path.instance();
            path.placement == RelayPathPlacement::Validation
                && instance.key.underlay == UnderlayProtocol::Udp
                && context.relay_path_allows_automatic_bulk_use(instance.key)
                && !self
                    .request_quic_capacity_attempted_paths
                    .contains(&instance.key.index)
                && !self.request_graduated_subflows.contains(&instance)
        });
        if !has_unattempted_udp_candidate || context.reliable_relay_has_latency_pressure() {
            // Bulk sends call this repeatedly. Topology can reject the common
            // single-path/completed case without touching session health.
            return;
        }
        let Some(service_path) = self.ordered_data_owner_instance.and_then(|service| {
            (service.key.underlay == UnderlayProtocol::Udp).then(|| {
                remotes.paths.iter().find(|path| {
                    path.instance() == service && path.placement == RelayPathPlacement::Active
                })
            })?
        }) else {
            return;
        };
        let service = service_path.instance();
        let Some(service_snapshot) = context.reliable_path_snapshot(service.key) else {
            return;
        };
        if service_snapshot.active_latency_sensitive_flows > 0
            || service_snapshot.session_active_latency_sensitive_flows > 0
        {
            return;
        }
        if service_snapshot.product_bytes_in_flight
            < reliable_subflow_startup_sample_limit_bytes(context.mux_limits)
        {
            return;
        }
        // QUIC keeps its native packet-ACK proof transaction, but shares TCP's
        // topology-stable budget policy. Attempt order cannot enlarge a train.
        let eligible_candidates =
            context.automatic_bulk_path_count(UnderlayProtocol::Udp, Some(service.key.index));
        let proposed_candidate_share =
            request_capacity_stable_candidate_share_bytes(context.mux_limits, eligible_candidates);
        let stable_candidate_share =
            context.request_quic_capacity_probe_candidate_share_bytes(proposed_candidate_share);
        let session_remaining_bytes = context.request_quic_capacity_probe_remaining_bytes();
        let Some((path, snapshot, geometry, _train_envelope_bytes)) = remotes
            .paths
            .iter()
            .filter(|path| {
                let instance = path.instance();
                let snapshot = context.reliable_path_snapshot(instance.key);
                path.placement == RelayPathPlacement::Validation
                    && instance.key.underlay == UnderlayProtocol::Udp
                    && context.relay_path_allows_automatic_bulk_use(instance.key)
                    && !self
                        .request_quic_capacity_attempted_paths
                        .contains(&instance.key.index)
                    && !self.request_graduated_subflows.contains(&instance)
                    && path.path_proof_id.is_some_and(|proof_id| {
                        context.relay_path_has_fresh_proof(
                            instance.key.underlay,
                            instance.key.index,
                            proof_id,
                            path.attached_at,
                        )
                    })
                    && !context.relay_path_has_native_bulk_model_evidence_since(
                        instance.key.underlay,
                        instance.key.index,
                        path.attached_at,
                    )
                    && snapshot.is_some_and(|snapshot| {
                        snapshot.bytes_in_flight == 0
                            && snapshot.queue_bytes == 0
                            && snapshot.product_bytes_in_flight == 0
                            && snapshot.product_queue_bytes == 0
                    })
                    && path
                        .stream
                        .can_enqueue_work_lane_now(ReliableWorkClass::Data, lane)
            })
            .filter_map(|path| {
                let snapshot = context.reliable_path_snapshot(path.key())?;
                let campaign_remaining_bytes = self
                    .request_quic_capacity_campaign
                    .remaining_bytes(stable_candidate_share);
                let train_envelope_bytes = session_remaining_bytes
                    .min(campaign_remaining_bytes)
                    .min(context.request_quic_capacity_probe_path_remaining_bytes(
                        path.key().index,
                        stable_candidate_share,
                    ));
                let geometry = request_quic_capacity_calibration_geometry(
                    snapshot,
                    service_snapshot.delivery_rate_bps,
                    context.mux_limits,
                    train_envelope_bytes,
                )?;
                Some((path, snapshot, geometry, train_envelope_bytes))
            })
            .min_by_key(|(path, _, _, _)| context.relay_path_config_ordinal(path.key()))
        else {
            return;
        };
        let train_payload_bytes = geometry.train_bytes;
        static NEXT_REQUEST_QUIC_CAPACITY_ID: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(1);
        let token =
            NEXT_REQUEST_QUIC_CAPACITY_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let ticket = QuicCapacityProbeCommandTicket::new();
        let lease_duration = request_quic_capacity_calibration_lease(snapshot, train_payload_bytes);
        let Some(expires_at) = Instant::now().checked_add(lease_duration) else {
            return;
        };
        let proof_validity = quic_bulk_proof_freshness_horizon(
            Duration::from_secs_f64(snapshot.srtt_ms.max(1.0) / 1_000.0),
            Duration::from_secs_f64(snapshot.jitter_ms.max(1.0) / 1_000.0),
        );
        let Some(publication_expires_at) = expires_at.checked_add(proof_validity) else {
            return;
        };
        let instance = path.instance();
        let Some(mut lease) = context.try_reserve_request_quic_capacity_probe(
            self.stream_id,
            instance.key.index,
            instance,
            token,
            train_payload_bytes,
            stable_candidate_share,
            self.request_quic_capacity_campaign.clone(),
            path.attached_at,
            expires_at,
            proof_validity,
            ticket.clone(),
        ) else {
            return;
        };
        let probe = QuicCapacityProbeCommand {
            owner: QuicCapacityProbeOwner::Request {
                stream_id: self.stream_id,
                path_instance: instance,
            },
            path_id: PathId(instance.key.index as u16),
            calibration_id: token,
            train_payload_bytes,
            sample_floor_bytes: geometry.sample_floor_bytes,
            warmup_carrier_bytes: geometry.warmup_carrier_bytes,
            required_timed_carrier_bytes: geometry.required_timed_carrier_bytes,
            proof_validity,
            expires_at,
            ticket: ticket.clone(),
            cancel_on_drop: true,
        };
        if path
            .stream
            .try_enqueue_request_quic_capacity_probe(probe)
            .is_err()
        {
            return;
        }
        lease.commit();
        self.request_quic_capacity_attempted_paths
            .insert(instance.key.index);
        self.request_quic_capacity_calibration = Some(RequestQuicCapacityCalibration {
            target: instance,
            token,
            publication_expires_at,
            graduated: false,
            ticket,
            _lease: lease,
        });
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "request_quic_capacity_calibration",
            format_args!(
                "phase=started stream_id={} path_index={} instance_id={} calibration_id={} train_bytes={} stable_candidate_share_bytes={} train_envelope_bytes={} sample_floor_bytes={} accounting_slack_bytes={} timing_slack_bytes={} desired_warmup_bytes={} warmup_bytes={} required_proof_bytes={} candidate_carrier_flight_bytes={} service_rate_bps={} service_rate_scope={:?} slow_start_rounds={} lease_ms={}",
                self.stream_id.0,
                instance.key.index,
                instance.id,
                token,
                train_payload_bytes,
                stable_candidate_share,
                _train_envelope_bytes,
                geometry.sample_floor_bytes,
                geometry.accounting_slack_bytes,
                geometry.timing_slack_bytes,
                geometry.desired_warmup_carrier_bytes,
                geometry.warmup_carrier_bytes,
                geometry.required_timed_carrier_bytes,
                geometry.candidate_carrier_flight_bytes,
                geometry.service_rate_bps,
                service_snapshot.rate_scope,
                request_quic_capacity_slow_start_rounds(train_payload_bytes),
                lease_duration.as_millis(),
            ),
        );
    }

    fn choose_lowest_eta_relay_path(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        frame: &Frame,
        lane: FlowLane,
        cause: RelaySendCause,
        avoid_keys: &[RelayPathKey],
        ordinary_stream_data: bool,
    ) -> Result<usize, RuntimeError> {
        let persistent_ack_gap_repair = cause.is_persistent_ack_gap_repair();
        let required_persistent_target = cause.persistent_client_target();
        let invalid_persistent_target =
            matches!(cause, RelaySendCause::PersistentServerAckGapRepair(_));
        let requires_distinct_output =
            cause == RelaySendCause::LiveOwnerTailRepair || persistent_ack_gap_repair;
        let payload_bytes = reliable_stream_frame_accounted_bytes(frame);
        if cause == RelaySendCause::RecvProgressRecovery
            && let Some(position) = choose_repair_recv_progress_path_position(remotes, frame, cause)
        {
            return Ok(position);
        }
        if cause.is_recv_progress() {
            if let Some(position) = choose_active_recv_progress_path_position(remotes, frame, cause)
            {
                return Ok(position);
            }
        }
        let has_active_path = remotes
            .paths
            .iter()
            .any(|path| path.placement == RelayPathPlacement::Active);
        let ordinary_path_allowed = |path: &ReliableRelayRemotePath| {
            (!ordinary_stream_data
                || !has_active_path
                || path.placement == RelayPathPlacement::Active)
                && (cause != RelaySendCause::RecvProgressRecovery
                    || path.placement != RelayPathPlacement::Validation)
                && (cause != RelaySendCause::LiveOwnerTailRepair
                    || path.placement != RelayPathPlacement::Validation)
                && !invalid_persistent_target
                && required_persistent_target.is_none_or(|required| path.instance() == required)
                && (!persistent_ack_gap_repair
                    || context
                        .relay_path_has_bulk_model_evidence(path.key().underlay, path.key().index))
        };
        let can_enqueue = |path: &ReliableRelayRemotePath| {
            relay_path_can_enqueue_frame_for_cause_now(path, frame, cause)
        };
        let choose = |prefer_avoiding: bool| {
            remotes
                .paths
                .iter()
                .enumerate()
                .filter(|(_, path)| !prefer_avoiding || !avoid_keys.contains(&path.key()))
                .filter(|(_, path)| ordinary_path_allowed(path))
                .filter(|(_, path)| can_enqueue(path))
                .filter_map(|(position, path)| {
                    let key = path.key();
                    let snapshot = context.reliable_path_snapshot(key)?;
                    let score = scheduler::score_path(
                        snapshot,
                        lane,
                        payload_bytes,
                        SchedulerPolicy::default(),
                    )?;
                    Some((
                        position,
                        score.eta_ms,
                        relay_cursor_distance(position, self.next_send_index, remotes.paths.len()),
                    ))
                })
                .min_by(|left, right| {
                    left.1
                        .total_cmp(&right.1)
                        .then_with(|| left.2.cmp(&right.2))
                })
                .map(|(position, _, _)| position)
        };
        let selected = if requires_distinct_output {
            choose(true)
        } else {
            choose(true).or_else(|| choose(false))
        };
        if let Some(position) = selected {
            return Ok(position);
        }
        let distinct_capacity_fallback = remotes
            .paths
            .iter()
            .enumerate()
            .filter(|(_, path)| ordinary_path_allowed(path))
            .filter(|(_, path)| can_enqueue(path))
            .map(|(position, _)| position)
            .find(|position| !avoid_keys.contains(&remotes.paths[*position].key()));
        let capacity_fallback = if requires_distinct_output {
            distinct_capacity_fallback
        } else {
            distinct_capacity_fallback.or_else(|| {
                remotes
                    .paths
                    .iter()
                    .enumerate()
                    .filter(|(_, path)| ordinary_path_allowed(path))
                    .filter(|(_, path)| can_enqueue(path))
                    .map(|(position, _)| position)
                    .next()
            })
        };
        if let Some(position) = capacity_fallback {
            return Ok(position);
        }
        let has_eligible_path = remotes.paths.iter().any(ordinary_path_allowed);
        let has_distinct_eligible_path = remotes
            .paths
            .iter()
            .any(|path| ordinary_path_allowed(path) && !avoid_keys.contains(&path.key()));
        if (requires_distinct_output && has_distinct_eligible_path)
            || (!requires_distinct_output && has_eligible_path)
        {
            Err(RuntimeError::SenderServiceBlocked)
        } else {
            Err(RuntimeError::ReliablePathSessionClosed)
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn release_normalized_acked_ranges(
        &mut self,
        context: &ClientPathContext,
        ranges: &[OffsetRange],
    ) {
        let _ = self.release_normalized_acked_ranges_with_owner_progress(context, ranges);
    }

    pub(in crate::runtime) fn release_normalized_acked_ranges_with_owner_progress(
        &mut self,
        context: &ClientPathContext,
        ranges: &[OffsetRange],
    ) -> smallvec::SmallVec<[RequestOwnerAckProgress<RelayPathInstance>; 4]> {
        let startup_owner = self
            .request_startup
            .epoch
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key);
        let startup_required_bytes = self
            .request_startup
            .epoch
            .as_ref()
            .and_then(|epoch| {
                startup_owner.and_then(|owner| epoch.startup_owner_sealed_sample_bytes(owner))
            })
            .unwrap_or(u64::MAX);
        let acked_at = Instant::now();
        let mut ordinary_owner_samples =
            HashMap::<RelayPathInstance, (u64, Instant, Instant)>::new();
        let mut owner_progress =
            smallvec::SmallVec::<[RequestOwnerAckProgress<RelayPathInstance>; 4]>::new();
        for release in self.flights.release_normalized_acked_ranges(ranges) {
            self.missing_owner_repair_attempts.remove(&release.instance);
            context.release_relay_path_inflight(
                release.key.underlay,
                release.key.index,
                release.bytes,
            );
            if release.path_proving {
                context.record_relay_path_product_ack(
                    self.stream_id,
                    release.instance,
                    release.bytes,
                    release.sent_at,
                    acked_at,
                );
                if let Some(progress) = owner_progress
                    .iter_mut()
                    .find(|progress| progress.instance == release.instance)
                {
                    progress.bytes = progress.bytes.saturating_add(release.bytes);
                } else {
                    owner_progress.push(RequestOwnerAckProgress {
                        instance: release.instance,
                        bytes: release.bytes,
                    });
                }
            }
            if release.path_proving
                && release.key.underlay == UnderlayProtocol::Tcp
                && startup_owner == Some(release.instance)
            {
                let first_sent_at = self
                    .request_startup
                    .first_sent_at
                    .entry(release.instance)
                    .or_insert(release.sent_at);
                *first_sent_at = (*first_sent_at).min(release.sent_at);
                let acked_bytes = self
                    .request_startup
                    .acked_bytes
                    .entry(release.instance)
                    .or_default();
                *acked_bytes = acked_bytes.saturating_add(release.bytes as u64);
            }
            if release.path_proving && startup_owner != Some(release.instance) {
                let sample = ordinary_owner_samples.entry(release.instance).or_insert((
                    0,
                    release.sent_at,
                    release.sent_at,
                ));
                sample.0 = sample.0.saturating_add(release.bytes as u64);
                sample.1 = sample.1.min(release.sent_at);
                sample.2 = sample.2.max(release.sent_at);
            }
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_model",
                format_args!(
                    "stream_id={} path_underlay={:?} path_index={} path_instance={} released_bytes={} elapsed_ms={:.3} path_proving={} cause=stream_ack",
                    self.stream_id.0,
                    release.key.underlay,
                    release.key.index,
                    release.instance.id,
                    release.bytes,
                    release.elapsed.as_secs_f64() * 1000.0,
                    release.path_proving,
                ),
            );
        }
        if let Some(owner) = startup_owner
            && owner.key.underlay == UnderlayProtocol::Tcp
            && let (Some(acked_bytes), Some(first_sent_at)) = (
                self.request_startup.acked_bytes.get(&owner).copied(),
                self.request_startup.first_sent_at.get(&owner).copied(),
            )
            && acked_bytes >= startup_required_bytes
            && let Some(sample) = PathRateSample::new(
                acked_bytes,
                acked_at.saturating_duration_since(first_sent_at),
            )
            && self.request_startup.rate_evidence.insert(owner)
        {
            self.request_rate_proven_subflows.insert(owner);
            self.record_request_per_flow_rate_sample(owner, sample, false);
            context.mark_relay_path_rate_sample(owner.key.underlay, owner.key.index, sample);
            if self.request_ack_clock_first_window_subflows.insert(owner) {
                // The exact product ACK that completes the sealed TCP startup
                // owner window is also a causal boundary: every calibration
                // byte selected after this point is post-boundary by
                // construction. The explicit path receipt remains an
                // equivalent boundary when it arrives first.
                self.request_rate_evidence
                    .entry(owner)
                    .or_insert_with(|| RequestPathRateEvidence::new(acked_at))
                    .seed_ack_boundary(acked_at);
            }
        }
        for (instance, (bytes, first_sent_at, latest_sent_at)) in ordinary_owner_samples {
            // TCP lacks carrier-native delivery telemetry, so its product ACK
            // fallback needs a representative window. QUIC keeps its existing
            // small product-provenance threshold; carrier ACKs own its rate.
            let is_ordered_service = self.ordered_data_owner_instance == Some(instance);
            let coverage_floor_bytes = request_path_rate_coverage_floor_bytes(
                instance.key.underlay,
                is_ordered_service,
                self.request_ack_clock_calibration_targets
                    .get(&instance)
                    .copied(),
                context.mux_limits,
            );
            let (update, has_exact_path_provenance, exact_attributed_bytes) = {
                let evidence = self
                    .request_rate_evidence
                    .entry(instance)
                    .or_insert_with(|| RequestPathRateEvidence::new(first_sent_at));
                let update = evidence.observe(
                    bytes,
                    first_sent_at,
                    latest_sent_at,
                    acked_at,
                    coverage_floor_bytes,
                    !is_ordered_service,
                );
                (
                    update,
                    evidence.has_exact_path_provenance(),
                    evidence.exact_attributed_bytes(),
                )
            };
            if has_exact_path_provenance {
                // Exact ownership is enough to establish that this flow used
                // the path. It is not enough to publish a rate sample.
                self.request_rate_proven_subflows.insert(instance);
            }
            if let RequestPathRateEvidenceUpdate::Proven {
                sample,
                first_window,
            } = update
            {
                if instance.key.underlay == UnderlayProtocol::Tcp
                    && is_ordered_service
                    && let Some(sample) = sample
                {
                    if !first_window {
                        self.record_request_tcp_ack_turnover_sample(
                            context, instance, sample, acked_at, false,
                        );
                    }
                    context.mark_relay_path_rate_sample(
                        instance.key.underlay,
                        instance.key.index,
                        sample,
                    );
                    if !first_window {
                        self.record_request_per_flow_rate_sample(instance, sample, false);
                    }
                } else if instance.key.underlay == UnderlayProtocol::Tcp && first_window {
                    self.request_ack_clock_first_window_subflows
                        .insert(instance);
                } else if instance.key.underlay == UnderlayProtocol::Tcp
                    && let Some(sample) = sample
                {
                    let replace_startup_rate =
                        self.request_ack_clock_proven_subflows.insert(instance);
                    let turnover_authorized = !replace_startup_rate
                        && self
                            .request_ack_clock_calibration_targets
                            .get(&instance)
                            .copied()
                            .is_some_and(|target_bytes| {
                                request_tcp_candidate_turnover_authorized(
                                    exact_attributed_bytes,
                                    target_bytes,
                                    coverage_floor_bytes,
                                )
                            });
                    if turnover_authorized {
                        self.request_window_turnover_proven_subflows
                            .insert(instance);
                    }
                    if self
                        .request_ack_clock_calibration_owner
                        .is_some_and(|owner| owner.candidate == instance)
                    {
                        self.request_ack_clock_calibration_owner = None;
                    }
                    if self
                        .request_ack_clock_calibration_pending
                        .is_some_and(|pending| pending.candidate == instance)
                    {
                        self.request_ack_clock_calibration_pending = None;
                    }
                    self.record_request_per_flow_rate_sample(
                        instance,
                        sample,
                        replace_startup_rate,
                    );
                    self.record_request_tcp_ack_turnover_sample(
                        context, instance, sample, acked_at, true,
                    );
                    context.mark_relay_path_ack_clock_rate_sample(
                        instance.key.underlay,
                        instance.key.index,
                        sample,
                        replace_startup_rate,
                    );
                    #[cfg(feature = "lab-diagnostics")]
                    {
                        lab_diagnostic(
                            "ack_clock_calibration",
                            format_args!(
                                "phase=ack_clock_sample stream_id={} underlay={:?} path_index={} instance_id={} evidence_bytes={} sample_elapsed_us={} replace_startup_rate={} rate_bps={}",
                                self.stream_id.0,
                                instance.key.underlay,
                                instance.key.index,
                                instance.id,
                                sample.bytes(),
                                sample.elapsed().as_micros(),
                                replace_startup_rate,
                                sample.rate_bps(),
                            ),
                        );
                    }
                }
            }
        }
        owner_progress
    }

    pub(in crate::runtime) fn discard_unusable_live_owner_tail_repairs(
        &self,
        sender_queue: &mut ReliableRelaySenderQueue,
        remotes: &ReliableRelayRemoteSet,
    ) -> usize {
        let live_instances = self.request_owner_capable_instances(remotes);
        let live_keys = live_instances
            .iter()
            .map(|instance| instance.key)
            .collect::<Vec<_>>();
        sender_queue.discard_unusable_live_owner_tail_repairs(|frame| {
            let owner_keys = self
                .flights
                .ordering_owner_keys_for_frame(frame, &live_instances);
            !owner_keys.is_empty() && live_keys.iter().any(|key| !owner_keys.contains(key))
        })
    }

    pub(in crate::runtime) fn discard_stale_persistent_ack_gap_repairs(
        &self,
        sender_queue: &mut ReliableRelaySenderQueue,
        remotes: &ReliableRelayRemoteSet,
    ) -> usize {
        let live_instances = remotes.path_instances();
        sender_queue.discard_stale_persistent_ack_gap_repairs(|cause| {
            cause
                .persistent_client_target()
                .is_none_or(|target| live_instances.contains(&target))
                && cause.persistent_server_target().is_none()
        })
    }

    fn request_owner_capable_instances(
        &self,
        remotes: &ReliableRelayRemoteSet,
    ) -> Vec<RelayPathInstance> {
        let startup_owner = self
            .request_startup
            .epoch
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key);
        remotes
            .paths
            .iter()
            .filter(|path| {
                path.placement != RelayPathPlacement::Validation
                    || startup_owner == Some(path.instance())
                    || self.request_graduated_subflows.contains(&path.instance())
                    || self
                        .flights
                        .has_ordering_owner_flights_for_instance(path.instance())
            })
            .map(ReliableRelayRemotePath::instance)
            .collect()
    }

    pub(in crate::runtime) fn request_ordered_service_instance(&self) -> Option<RelayPathInstance> {
        self.ordered_data_owner_instance
    }

    pub(in crate::runtime) fn request_owner_ack_can_grow_window(
        &self,
        remotes: &ReliableRelayRemoteSet,
        service_instance: Option<RelayPathInstance>,
        instance: RelayPathInstance,
    ) -> bool {
        let Some(service) = service_instance else {
            return false;
        };
        if self.ordered_data_owner_instance != Some(service)
            || !remotes.contains_path_instance(service)
            || service.key.underlay != instance.key.underlay
        {
            return false;
        }
        remotes.paths.iter().any(|path| {
            path.instance() == instance
                && (instance == service
                    || (self.request_graduated_subflows.contains(&instance)
                        && (instance.key.underlay == UnderlayProtocol::Udp
                            || self.request_ack_clock_proven_subflows.contains(&instance))))
        })
    }

    pub(in crate::runtime) fn request_tcp_owner_ack_turnover_bytes(
        &self,
        remotes: &ReliableRelayRemoteSet,
        service_instance: Option<RelayPathInstance>,
        now: Instant,
    ) -> usize {
        let Some(service) = service_instance.filter(|service| {
            service.key.underlay == UnderlayProtocol::Tcp
                && self.ordered_data_owner_instance == Some(*service)
                && remotes.contains_path_instance(*service)
        }) else {
            return 0;
        };
        remotes
            .paths
            .iter()
            .filter_map(|path| {
                let instance = path.instance();
                if !self.request_owner_ack_can_grow_window(remotes, Some(service), instance) {
                    return None;
                }
                if instance != service
                    && !self
                        .request_window_turnover_proven_subflows
                        .contains(&instance)
                {
                    return None;
                }
                self.request_tcp_ack_turnover
                    .get(&instance)
                    .copied()
                    .filter(|model| model.is_fresh_at(now))
                    .map(|model| model.turnover_bytes)
            })
            .sum::<f64>()
            .ceil() as usize
    }

    pub(in crate::runtime) fn unreported_missing_owner_instances(
        &mut self,
        remotes: &ReliableRelayRemoteSet,
        retry_after: Duration,
    ) -> Vec<RelayPathInstance> {
        let owner_instances = self.flights.ordering_owner_instances();
        self.missing_owner_repair_attempts.retain(|instance, _| {
            owner_instances.contains(instance) && !remotes.contains_path_instance(*instance)
        });
        let now = Instant::now();
        owner_instances
            .into_iter()
            .filter(|instance| {
                !remotes.contains_path_instance(*instance)
                    && self
                        .missing_owner_repair_attempts
                        .get(instance)
                        .is_none_or(|attempt| {
                            now.saturating_duration_since(*attempt) >= retry_after
                        })
            })
            .collect()
    }

    #[cfg(test)]
    pub(in crate::runtime) fn unreported_missing_owner_keys(
        &mut self,
        remotes: &ReliableRelayRemoteSet,
        retry_after: Duration,
    ) -> Vec<RelayPathKey> {
        let mut keys = Vec::new();
        for instance in self.unreported_missing_owner_instances(remotes, retry_after) {
            if !keys.contains(&instance.key) {
                keys.push(instance.key);
            }
        }
        keys
    }

    pub(in crate::runtime) fn release_all(&mut self, context: &ClientPathContext) {
        for release in self.flights.drain_all() {
            context.release_relay_path_inflight(
                release.key.underlay,
                release.key.index,
                release.bytes,
            );
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn age_product_flights_for_test(&mut self, age: Duration) {
        self.flights.age_product_flights_for_test(age);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_owner_frame_for_test(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
    ) {
        self.flights.record_owner_frame_instance(instance, frame);
        self.ordered_data_owner = Some(instance.key);
        self.ordered_data_owner_instance = Some(instance);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn ordered_data_owner_for_test(&self) -> Option<RelayPathKey> {
        self.ordered_data_owner
    }

    pub(in crate::runtime) async fn reannounce_active_path(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        spec: &ReliableRelayOpenSpec,
        lane: FlowLane,
    ) -> Result<(), RuntimeError> {
        let Some(position) = remotes
            .paths
            .iter()
            .rposition(|path| path.placement == RelayPathPlacement::Active)
        else {
            return Err(RuntimeError::ReliablePathSessionClosed);
        };
        let instance = remotes.paths[position].instance();
        remotes.paths[position].stream.lane = lane;
        let frame = Frame::OpenStream {
            stream_id: remotes.stream_id(),
            target: spec.target.clone(),
            ingress: spec.ingress,
            outbound: OutboundPolicy::Direct,
            demand: stream_demand_hint_for_lane(lane),
            role: StreamOpenRole::Active,
        };
        match emit_relay_path_frame(&remotes.paths[position].stream, frame, FlowLane::Control) {
            Ok(()) => Ok(()),
            Err(err) => {
                self.fail_client_path_instance(context, remotes, instance)
                    .await;
                Err(err)
            }
        }
    }

    pub(in crate::runtime) async fn reannounce_path_instance_as_active(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        instance: RelayPathInstance,
        spec: &ReliableRelayOpenSpec,
        lane: FlowLane,
    ) -> Result<bool, RuntimeError> {
        let Some(position) = remotes
            .paths
            .iter()
            .position(|path| path.instance() == instance)
        else {
            return Ok(false);
        };
        if remotes.paths[position].placement == RelayPathPlacement::Active
            && position + 1 == remotes.paths.len()
        {
            return Ok(false);
        }
        let frame = Frame::OpenStream {
            stream_id: remotes.stream_id(),
            target: spec.target.clone(),
            ingress: spec.ingress,
            outbound: OutboundPolicy::Direct,
            demand: stream_demand_hint_for_lane(lane),
            role: StreamOpenRole::Active,
        };
        let emit_result = {
            let path = &mut remotes.paths[position];
            path.stream.lane = lane;
            emit_relay_path_frame(&path.stream, frame, FlowLane::Control)
        };
        match emit_result {
            Ok(()) => {
                let activated = remotes.activate_path_instance_after_service_open(instance);
                if activated {
                    remotes.reserve_path_instance_load_if_needed(context, instance, lane);
                }
                Ok(activated)
            }
            Err(err) => {
                self.fail_client_path_instance(context, remotes, instance)
                    .await;
                Err(err)
            }
        }
    }

    pub(in crate::runtime) async fn send_attach_control_to_instance(
        &mut self,
        remotes: &mut ReliableRelayRemoteSet,
        instance: RelayPathInstance,
        send_stream: &ReliableSendStream,
        resend_fin: bool,
    ) -> Result<bool, RuntimeError> {
        let Some(position) = remotes
            .paths
            .iter()
            .position(|path| path.instance() == instance)
        else {
            return Ok(false);
        };
        if !resend_fin {
            return Ok(false);
        }
        emit_relay_path_frame_with_mode(
            &remotes.paths[position].stream,
            Frame::StreamFin {
                stream_id: remotes.stream_id(),
                final_offset: send_stream.next_offset(),
            },
            remotes.paths[position].stream.lane,
            CarrierEmitMode::StreamOrdered,
        )?;
        Ok(true)
    }

    pub(in crate::runtime) async fn send_recv_progress(
        &mut self,
        remotes: &mut ReliableRelayRemoteSet,
        context: &ClientPathContext,
        recv_stream: &ReliableRecvStream,
        progress: &mut ReliableRecvProgress,
        request: RelayRecvProgressSend,
    ) -> Result<bool, RuntimeError> {
        let mut sent_any = false;
        let cause = if request.recover_stalled_service {
            RelaySendCause::RecvProgressRecovery
        } else {
            RelaySendCause::RecvProgress
        };
        let ack_progress_before = progress.clone();
        if progress.should_send_ack(
            recv_stream,
            request.path,
            request.lane,
            context.mux_limits,
            request.force_max_data,
        ) {
            #[cfg(feature = "lab-diagnostics")]
            let ack_started = Instant::now();
            let ack_frames = recv_stream.ack_frames();
            #[cfg(feature = "lab-diagnostics")]
            lab_perf_record("mux.ack_frames", ack_started.elapsed(), ack_frames.len());
            for ack_frame in ack_frames {
                #[cfg(feature = "lab-diagnostics")]
                let (ack_complete, ack_ranges, ack_frontier, ack_largest_end) = match &ack_frame {
                    Frame::StreamAck {
                        complete, ranges, ..
                    } => (
                        *complete,
                        ranges.len(),
                        stream_ack_contiguous_frontier(ranges),
                        ranges.last().map_or(0, |range| range.end),
                    ),
                    _ => unreachable!("ack_frames only returns STREAM_ACK"),
                };
                match self
                    .send_control_frame(context, remotes, ack_frame, cause)
                    .await
                {
                    Ok(outcome) => {
                        sent_any = true;
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "recv_progress_ack_emit",
                            format_args!(
                                "stream_id={} complete={} ranges={} frontier={} largest_end={} recv_next_offset={} recv_reorder_bytes={} cause={} path_underlay={:?} path_index={}",
                                self.stream_id.0,
                                ack_complete,
                                ack_ranges,
                                ack_frontier,
                                ack_largest_end,
                                recv_stream.next_offset(),
                                recv_stream.reorder_bytes(),
                                cause.as_str(),
                                outcome.path_key.underlay,
                                outcome.path_key.index,
                            ),
                        );
                        #[cfg(not(feature = "lab-diagnostics"))]
                        let _ = outcome;
                    }
                    Err(RuntimeError::SenderServiceBlocked) => {
                        // Partial incomplete ACK chunks are safe to repeat. Put
                        // the ACK progress cursor back so the omitted chunks are
                        // sent on the next capacity notification instead of
                        // being inferred as sender-side loss.
                        *progress = ack_progress_before;
                        return Ok(sent_any);
                    }
                    Err(err) => return Err(err),
                }
            }
        }
        let max_data_progress_before = progress.clone();
        if progress.should_send_max_data(
            recv_stream,
            request.path,
            request.lane,
            context.mux_limits,
            request.force_max_data,
        ) {
            let advertised_window = reliable_stream_advertised_window_bytes(
                request.path,
                request.lane,
                context.mux_limits,
            );
            match self
                .send_control_frame(
                    context,
                    remotes,
                    recv_stream.max_data_frame_with_window(advertised_window),
                    cause,
                )
                .await
            {
                Ok(_) => {
                    sent_any = true;
                }
                Err(RuntimeError::SenderServiceBlocked) => {
                    *progress = max_data_progress_before;
                    return Ok(sent_any);
                }
                Err(err) => return Err(err),
            }
        }
        Ok(sent_any)
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::runtime) fn enqueue_live_owner_tail_repair(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        last_send_ack_ranges: &[OffsetRange],
        last_send_ack_complete: bool,
        last_send_ack_frontier: u64,
        lane: FlowLane,
    ) -> bool {
        if !last_send_ack_complete
            || last_send_ack_frontier == 0
            || last_send_ack_frontier >= send_stream.next_offset()
            || send_stream.repair_bytes() == 0
            || !matches!(
                last_send_ack_ranges,
                [range] if range.start == 0 && range.end == last_send_ack_frontier
            )
        {
            return false;
        }
        let live_instances = self.request_owner_capable_instances(remotes);
        let live_keys = live_instances
            .iter()
            .map(|instance| instance.key)
            .collect::<Vec<_>>();
        if live_keys.len() <= 1 {
            return false;
        }
        let repair_limit = reliable_critical_tail_repair_limit_bytes(
            live_keys
                .iter()
                .map(|key| {
                    adaptive_reliable_relay_repair_bytes(
                        context.reliable_path_snapshot(*key),
                        lane,
                        context.mux_limits,
                    )
                })
                .max()
                .unwrap_or(0),
            send_stream.repair_bytes(),
            context.mux_limits,
        );
        if repair_limit == 0 {
            return false;
        }
        let repair_frames = send_stream.retransmission_frames_for_ranges(
            &[OffsetRange {
                start: last_send_ack_frontier,
                end: send_stream.next_offset(),
            }],
            repair_limit,
        );
        let mut queued = false;
        for frame in repair_frames {
            let expected_owner_keys = self
                .flights
                .ordering_owner_keys_for_frame(&frame, &live_instances);
            if expected_owner_keys.is_empty()
                || !live_keys
                    .iter()
                    .any(|key| !expected_owner_keys.contains(key))
            {
                break;
            }
            let first_repair_after = expected_owner_keys
                .iter()
                .map(|key| {
                    reliable_relay_tail_repair_delay(context.reliable_path_snapshot(*key), lane)
                })
                .max()
                .unwrap_or_default();
            let repeat_repair_after =
                first_repair_after.saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD);
            let owner_keys = self.flights.live_owner_tail_repair_owner_keys(
                &frame,
                &live_instances,
                first_repair_after,
                repeat_repair_after,
            );
            if owner_keys.len() != expected_owner_keys.len() {
                break;
            }
            if sender_queue.has_queued_repair_overlap(&frame) {
                continue;
            }
            let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
            self.enqueue_critical_repair_frame(
                sender_queue,
                frame,
                RelaySendCause::LiveOwnerTailRepair,
            );
            queued = true;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "repair",
                format_args!(
                    "stream_id={} owner_underlay={:?} owner_index={} cause=live_owner_tail queued=true payload_bytes={}",
                    self.stream_id.0, owner_keys[0].underlay, owner_keys[0].index, payload_bytes,
                ),
            );
            #[cfg(not(feature = "lab-diagnostics"))]
            let _ = payload_bytes;
        }
        queued
    }

    pub(in crate::runtime) fn enqueue_failed_path_instance_gap_repairs(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        failed_instance: RelayPathInstance,
        lane: FlowLane,
    ) -> bool {
        let ranges = self
            .flights
            .latest_unacked_ranges_for_path_instance(failed_instance);
        self.enqueue_failed_path_gap_repairs_for_ranges(
            sender_queue,
            context,
            remotes,
            send_stream,
            failed_instance.key,
            &[failed_instance],
            ranges,
            lane,
        )
    }

    #[cfg(test)]
    pub(in crate::runtime) fn enqueue_failed_path_gap_repairs(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        failed_key: RelayPathKey,
        lane: FlowLane,
    ) -> bool {
        let failed_instances = self
            .flights
            .ordering_owner_instances()
            .into_iter()
            .filter(|instance| instance.key == failed_key)
            .collect::<Vec<_>>();
        let ranges = self.flights.latest_unacked_ranges_for_path(failed_key);
        self.enqueue_failed_path_gap_repairs_for_ranges(
            sender_queue,
            context,
            remotes,
            send_stream,
            failed_key,
            &failed_instances,
            ranges,
            lane,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))]
    fn enqueue_failed_path_gap_repairs_for_ranges(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        failed_key: RelayPathKey,
        failed_instances: &[RelayPathInstance],
        ranges: Vec<OffsetRange>,
        lane: FlowLane,
    ) -> bool {
        if ranges.is_empty() {
            return false;
        }
        let repair_path = remotes
            .primary_path_key()
            .and_then(|key| context.reliable_path_snapshot(key));
        let repair_limit = reliable_critical_tail_repair_limit_bytes(
            adaptive_reliable_relay_repair_bytes(repair_path, lane, context.mux_limits),
            send_stream.repair_bytes(),
            context.mux_limits,
        );
        let repair_frames = send_stream.retransmission_frames_for_ranges(&ranges, repair_limit);
        if repair_frames.is_empty() {
            return false;
        }
        let mut queued = false;
        for frame in repair_frames {
            let queued_frame = if sender_queue.has_queued_repair_overlap(&frame) {
                false
            } else {
                self.enqueue_critical_repair_frame(
                    sender_queue,
                    frame,
                    RelaySendCause::PathFailureRepair,
                );
                true
            };
            queued |= queued_frame;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "repair",
                format_args!(
                    "stream_id={} failed_underlay={:?} failed_index={} cause=path_failure queued={}",
                    self.stream_id.0, failed_key.underlay, failed_key.index, queued_frame,
                ),
            );
        }
        if queued {
            let now = Instant::now();
            for instance in failed_instances {
                self.missing_owner_repair_attempts.insert(*instance, now);
            }
        }
        queued
    }

    fn record_decision(
        &self,
        path_key: RelayPathKey,
        payload_bytes: usize,
        frame: &Frame,
        cause: RelaySendCause,
    ) {
        #[cfg(feature = "lab-diagnostics")]
        {
            let (frame_offset, frame_end_offset) = match frame {
                Frame::StreamData {
                    offset, payload, ..
                } => (*offset, offset.saturating_add(payload.len() as u64)),
                _ => (0, 0),
            };
            lab_sender_service_decision(
                "client",
                None,
                self.stream_id.0,
                "primary",
                sender_service_frame_kind(frame),
                payload_bytes,
                None,
                format_args!(
                    "cause={} path_underlay={:?} path_index={} pacing_bytes={} repair={} frame_offset={} frame_end_offset={}",
                    cause.as_str(),
                    path_key.underlay,
                    path_key.index,
                    reliable_path_frame_pacing_bytes(frame),
                    cause.is_repair(),
                    frame_offset,
                    frame_end_offset,
                ),
            );
        }
        #[cfg(not(feature = "lab-diagnostics"))]
        {
            let _ = (path_key, payload_bytes, frame, cause);
        }
    }
}

fn choose_active_recv_progress_path_position(
    remotes: &ReliableRelayRemoteSet,
    frame: &Frame,
    cause: RelaySendCause,
) -> Option<usize> {
    remotes
        .paths
        .iter()
        .enumerate()
        .rev()
        .find(|(_, path)| {
            path.placement == RelayPathPlacement::Active
                && relay_path_can_enqueue_frame_for_cause_now(path, frame, cause)
        })
        .map(|(position, _)| position)
}

fn choose_repair_recv_progress_path_position(
    remotes: &ReliableRelayRemoteSet,
    frame: &Frame,
    cause: RelaySendCause,
) -> Option<usize> {
    remotes
        .paths
        .iter()
        .enumerate()
        .rev()
        .find(|(_, path)| {
            path.placement == RelayPathPlacement::Repair
                && relay_path_can_enqueue_frame_for_cause_now(path, frame, cause)
        })
        .map(|(position, _)| position)
}

fn relay_path_can_enqueue_frame_for_cause_now(
    path: &ReliableRelayRemotePath,
    frame: &Frame,
    cause: RelaySendCause,
) -> bool {
    if matches!(cause, RelaySendCause::StreamFin) {
        path.stream.output.can_enqueue_lane_now(path.stream.lane)
    } else {
        path.stream.can_enqueue_frame_now(frame, path.stream.lane)
    }
}

#[cfg(test)]
#[path = "service_test.rs"]
mod tests;
