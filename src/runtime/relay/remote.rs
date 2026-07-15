//! Client relay attachment-set ownership.
//!
//! This module owns attachment incarnation identity, role and candidate policy,
//! in-flight claim exclusion, membership commit/rollback, scheduler load,
//! frame fan-in, and teardown. Concrete TCP/QUIC opens stay in `open`.

use super::io::reliable_relay_error_is_migratable;
use super::open::{
    OpenedRemoteStream, ReliableRelayOpenSpec, no_schedulable_reliable_path_error,
    open_remote_stream_for_relay_path, relay_path_open_error_is_retryable,
    stream_open_error_is_path_retryable, udp_stream_open_error_is_path_retryable,
};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, adaptive_reliable_relay_chunk_bytes, relay_lane_startup_chunk_bytes,
    reliable_relay_buffer_len,
};
use crate::model::path::{RelayPathInstance, RelayPathKey, RelayPathPlacement};
use crate::model::work::ReliableWorkClass;
use crate::mux::MuxLimits;
use crate::mux::stream::ReliableSendStream;
use crate::protocol::{Frame, StreamId, StreamOpenRole, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::{ClientPathContext, RelayPathLoadLease};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamHandle};
use crate::scheduler::FlowLane;
use std::collections::HashSet;
use std::time::Instant;
use tokio::sync::mpsc;

pub(in crate::runtime) struct ReliableRelayRemotePath {
    pub(in crate::runtime) path_index: usize,
    pub(in crate::runtime) instance_id: u64,
    pub(in crate::runtime) placement: RelayPathPlacement,
    // Declaration order also depublishes load before an abrupt stream drop.
    pub(in crate::runtime) load_lease: Option<RelayPathLoadLease>,
    pub(in crate::runtime) attached_at: Instant,
    pub(in crate::runtime) path_proof_id: Option<u64>,
    pub(in crate::runtime) path_proof_generation: u64,
    pub(in crate::runtime) stream: ReliablePathStreamHandle,
}

impl ReliableRelayRemotePath {
    pub(in crate::runtime) fn key(&self) -> RelayPathKey {
        RelayPathKey {
            underlay: self.stream.underlay,
            index: self.path_index,
        }
    }

    pub(in crate::runtime) fn instance(&self) -> RelayPathInstance {
        RelayPathInstance {
            key: self.key(),
            id: self.instance_id,
        }
    }

    pub(in crate::runtime) fn has_load_reservation(&self) -> bool {
        self.load_lease.is_some()
    }

    /// Carrier shutdown may block, so retire scheduler-visible load first.
    fn depublish_load(&mut self) {
        drop(self.load_lease.take());
    }
}

pub(in crate::runtime) struct ReliableRelayRemoteFrame {
    pub(in crate::runtime) instance: RelayPathInstance,
    pub(in crate::runtime) frame: Result<Frame, RuntimeError>,
}

/// Reports whether attachment-set ownership committed; a rejected pending open
/// rolls back its carrier and scheduler lease when the value is dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ReliableRelayAttachOutcome {
    Attached,
    RejectedDuplicate,
}

pub(in crate::runtime) struct ReliableRelayRemoteSet {
    stream_id: StreamId,
    pub(in crate::runtime) paths: Vec<ReliableRelayRemotePath>,
    frames_tx: mpsc::Sender<ReliableRelayRemoteFrame>,
    frames_rx: mpsc::Receiver<ReliableRelayRemoteFrame>,
    next_instance_id: u64,
    membership_generation: u64,
}

impl ReliableRelayRemoteSet {
    pub(in crate::runtime) fn new(opened: OpenedRemoteStream, frame_queue: usize) -> Self {
        let stream_id = opened.stream().stream_id;
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

    pub(in crate::runtime) fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub(in crate::runtime) fn membership_generation(&self) -> u64 {
        self.membership_generation
    }

    /// A selection is valid only for the exact attachment topology it observed.
    pub(in crate::runtime) fn path_position_at_generation(
        &self,
        generation: u64,
        instance: RelayPathInstance,
    ) -> Option<usize> {
        if self.membership_generation != generation {
            return None;
        }
        self.paths
            .iter()
            .position(|path| path.instance() == instance)
    }

    pub(in crate::runtime) fn primary_path_key(&self) -> Option<RelayPathKey> {
        self.paths.first().map(|path| path.key())
    }

    pub(in crate::runtime) fn active_path_instance(&self) -> Option<RelayPathInstance> {
        self.active_path_position()
            .and_then(|position| self.paths.get(position))
            .map(ReliableRelayRemotePath::instance)
    }

    pub(in crate::runtime) fn active_path_key(&self) -> Option<RelayPathKey> {
        self.active_path_instance().map(|instance| instance.key)
    }

    pub(in crate::runtime) fn active_path_index_for(
        &self,
        underlay: UnderlayProtocol,
    ) -> Option<usize> {
        self.paths
            .iter()
            .rev()
            .find(|path| {
                path.stream.underlay == underlay && path.placement == RelayPathPlacement::Active
            })
            .map(|path| path.path_index)
    }

    pub(in crate::runtime) fn active_path_underlay(&self) -> Option<UnderlayProtocol> {
        self.active_path_position()
            .and_then(|position| self.paths.get(position))
            .map(|path| path.stream.underlay)
    }

    pub(in crate::runtime) fn contains_path_key(&self, key: RelayPathKey) -> bool {
        self.paths.iter().any(|path| path.key() == key)
    }

    pub(in crate::runtime) fn contains_path_instance(&self, instance: RelayPathInstance) -> bool {
        self.paths.iter().any(|path| path.instance() == instance)
    }

    pub(in crate::runtime) fn path_keys(&self) -> Vec<RelayPathKey> {
        self.paths
            .iter()
            .map(ReliableRelayRemotePath::key)
            .collect()
    }

    pub(in crate::runtime) fn path_instances(&self) -> Vec<RelayPathInstance> {
        self.paths
            .iter()
            .map(ReliableRelayRemotePath::instance)
            .collect()
    }

    pub(in crate::runtime) fn load_owned_path_keys(&self) -> Vec<RelayPathKey> {
        self.paths
            .iter()
            .filter(|path| path.has_load_reservation())
            .map(ReliableRelayRemotePath::key)
            .collect()
    }

    pub(in crate::runtime) fn repair_path_instance_for_service_recovery(
        &self,
    ) -> Option<RelayPathInstance> {
        self.paths
            .iter()
            .rev()
            .find(|path| path.placement == RelayPathPlacement::Repair)
            .map(ReliableRelayRemotePath::instance)
    }

    pub(in crate::runtime) fn accepted_product_path_count(&self) -> usize {
        // Active and Repair opens enter this set only after peer acceptance.
        // Validation remains excluded from this attachment-role count even
        // when a stream separately graduates it from exact capacity evidence.
        self.paths
            .iter()
            .filter(|path| path.placement != RelayPathPlacement::Validation)
            .count()
    }

    #[cfg(test)]
    pub(in crate::runtime) fn path_instance_for_key(
        &self,
        key: RelayPathKey,
    ) -> Option<RelayPathInstance> {
        self.paths
            .iter()
            .find(|path| path.key() == key)
            .map(ReliableRelayRemotePath::instance)
    }

    pub(in crate::runtime) fn set_lane(&mut self, lane: FlowLane) {
        for path in &mut self.paths {
            path.stream.lane = lane;
            if let Some(lease) = &mut path.load_lease {
                // Relay control has already moved the shared load counters.
                lease.set_recorded_lane(lane);
            }
        }
    }

    pub(in crate::runtime) fn retry_pending_path_proofs(&mut self, context: &ClientPathContext) {
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

    pub(in crate::runtime) fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    pub(in crate::runtime) fn max_offset(&self) -> u64 {
        self.paths
            .iter()
            .map(|path| path.stream.max_offset)
            .max()
            .unwrap_or(0)
    }

    pub(in crate::runtime) fn max_frame_payload_bytes(&self, mux_limits: MuxLimits) -> usize {
        self.paths
            .iter()
            .map(|path| path.stream.max_frame_payload_bytes)
            .min()
            .unwrap_or_else(|| reliable_relay_buffer_len(mux_limits))
            .max(1)
    }

    pub(in crate::runtime) fn attach(
        &mut self,
        opened: OpenedRemoteStream,
    ) -> ReliableRelayAttachOutcome {
        self.attach_with_placement(opened, RelayPathPlacement::Active)
    }

    pub(in crate::runtime) fn attach_for_repair(
        &mut self,
        opened: OpenedRemoteStream,
    ) -> ReliableRelayAttachOutcome {
        self.attach_with_placement(opened, RelayPathPlacement::Repair)
    }

    pub(in crate::runtime) fn attach_for_validation(
        &mut self,
        opened: OpenedRemoteStream,
    ) -> ReliableRelayAttachOutcome {
        self.attach_with_placement(opened, RelayPathPlacement::Validation)
    }

    fn attach_with_placement(
        &mut self,
        opened: OpenedRemoteStream,
        placement: RelayPathPlacement,
    ) -> ReliableRelayAttachOutcome {
        let path_index = opened.path_index();
        let underlay = opened.stream().underlay;
        let key = RelayPathKey {
            underlay,
            index: path_index,
        };
        if self.contains_path_key(key) {
            return ReliableRelayAttachOutcome::RejectedDuplicate;
        }
        let instance_id = self.next_instance_id;
        self.next_instance_id = self.next_instance_id.wrapping_add(1);
        let instance = RelayPathInstance {
            key,
            id: instance_id,
        };
        let (stream, path_index, mut load_lease) = opened.into_attachment_parts();
        if placement != RelayPathPlacement::Active {
            // Candidate ranking reserves during a synchronous attach open, but
            // passive membership is not product demand.
            drop(load_lease.take());
        }
        let (stream, mut frames) = stream.into_handle_and_frames();
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
            load_lease,
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
        ReliableRelayAttachOutcome::Attached
    }

    pub(in crate::runtime) async fn recv_frame(
        &mut self,
    ) -> Result<ReliableRelayRemoteFrame, RuntimeError> {
        self.frames_rx
            .recv()
            .await
            .ok_or(RuntimeError::ReliablePathSessionClosed)
    }

    pub(in crate::runtime) fn has_buffered_frame(&self) -> bool {
        !self.frames_rx.is_empty()
    }

    pub(in crate::runtime) fn can_enqueue_work_lane_now(
        &self,
        work_lane: ReliableWorkClass,
        relay_lane: FlowLane,
    ) -> bool {
        self.paths.iter().any(|path| {
            relay_path_placement_may_wake_work_lane(path.placement, work_lane)
                && path.stream.can_enqueue_work_lane_now(work_lane, relay_lane)
        })
    }

    pub(in crate::runtime) async fn close_all(&mut self) {
        if !self.paths.is_empty() {
            self.membership_generation = self.membership_generation.wrapping_add(1);
        }
        let mut paths = std::mem::take(&mut self.paths);
        // The set stops owning every path as one atomic scheduling event even
        // when the first carrier queue makes detach asynchronous.
        for path in &mut paths {
            path.depublish_load();
        }
        for path in paths {
            path.stream.send_detach().await;
            path.stream.close().await;
        }
    }

    pub(in crate::runtime) async fn fail_path_instance(
        &mut self,
        context: &ClientPathContext,
        instance: RelayPathInstance,
    ) -> bool {
        let Some(mut path) = self.remove_path_instance(instance) else {
            return false;
        };
        context.mark_relay_path_data_plane_failure(path.stream.underlay, path.path_index);
        path.depublish_load();
        path.stream.send_detach().await;
        path.stream.close().await;
        true
    }

    pub(in crate::runtime) fn remove_path_instance(
        &mut self,
        instance: RelayPathInstance,
    ) -> Option<ReliableRelayRemotePath> {
        let position = self
            .paths
            .iter()
            .position(|path| path.instance() == instance)?;
        self.remove_path_at(position)
    }

    pub(in crate::runtime) fn remove_path_at(
        &mut self,
        position: usize,
    ) -> Option<ReliableRelayRemotePath> {
        let path = self.paths.remove(position);
        self.membership_generation = self.membership_generation.wrapping_add(1);
        Some(path)
    }

    pub(in crate::runtime) fn activate_path_instance_after_service_open(
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
            self.membership_generation = self.membership_generation.wrapping_add(1);
            return true;
        }
        let mut path = self.paths.remove(position);
        path.placement = RelayPathPlacement::Active;
        self.paths.push(path);
        self.membership_generation = self.membership_generation.wrapping_add(1);
        true
    }

    pub(in crate::runtime) fn reserve_path_instance_load_if_needed(
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
        let Some(lease) = context.reserve_relay_path_load(path.key(), lane) else {
            return false;
        };
        path.load_lease = Some(lease);
        true
    }

    /// Transfers a pre-enqueue claim after the synchronous queue commit.
    /// Generation and instance were resolved with no intervening await.
    pub(in crate::runtime) fn commit_path_instance_load_claim(
        &mut self,
        instance: RelayPathInstance,
        lease: RelayPathLoadLease,
    ) {
        let path = self
            .paths
            .iter_mut()
            .find(|path| path.instance() == instance)
            .expect("generation-fenced selected path must remain attached");
        assert!(
            !path.has_load_reservation(),
            "conditionally claimed path load must remain unowned before transfer"
        );
        path.load_lease = Some(lease);
    }

    fn active_path_position(&self) -> Option<usize> {
        self.paths
            .iter()
            .rposition(|path| path.placement == RelayPathPlacement::Active)
    }
}

fn relay_path_placement_may_wake_work_lane(
    placement: RelayPathPlacement,
    work_lane: ReliableWorkClass,
) -> bool {
    match work_lane {
        ReliableWorkClass::Data => placement != RelayPathPlacement::Repair,
        ReliableWorkClass::Control | ReliableWorkClass::Repair => true,
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) enum ReliableRelayAttachMode {
    Any,
    BulkStriping,
    RecoveryRepair,
}

fn send_request_attach_control_frames(
    path_stream: &ReliablePathStream,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
) -> Result<(), RuntimeError> {
    if resend_fin {
        path_stream.try_enqueue_request_control_frame(Frame::StreamFin {
            stream_id: path_stream.stream_id,
            final_offset: send_stream.next_offset(),
        })?;
    }
    Ok(())
}

struct RelayPathAttachRequest<'a> {
    spec: &'a ReliableRelayOpenSpec,
    lane: FlowLane,
    send_stream: &'a ReliableSendStream,
    resend_fin: bool,
    candidates: Vec<RelayPathKey>,
    role: StreamOpenRole,
    send_attach_control: bool,
}

struct RelayPathAttachResult {
    attached: usize,
    key: Option<RelayPathKey>,
}

async fn attach_relay_path_candidates(
    context: &ClientPathContext,
    remotes: &mut ReliableRelayRemoteSet,
    request: RelayPathAttachRequest<'_>,
) -> Result<RelayPathAttachResult, RuntimeError> {
    let stream_id = remotes.stream_id();
    let mut last_retryable_error = None;
    let candidates = request.candidates;

    for key in candidates {
        if remotes.contains_path_key(key) {
            continue;
        }
        match open_remote_stream_for_relay_path(
            context,
            stream_id,
            request.spec.target.clone(),
            request.spec.ingress,
            request.lane,
            key,
            request.role,
        )
        .await
        {
            Ok(opened) => {
                let attach_control_result = if request.send_attach_control {
                    send_request_attach_control_frames(
                        opened.stream(),
                        request.send_stream,
                        request.resend_fin,
                    )
                } else {
                    Ok(())
                };
                match attach_control_result {
                    Ok(()) => {
                        let attach_outcome = match request.role {
                            StreamOpenRole::Active => remotes.attach(opened),
                            StreamOpenRole::Repair => remotes.attach_for_repair(opened),
                            StreamOpenRole::Validation => remotes.attach_for_validation(opened),
                        };
                        match attach_outcome {
                            ReliableRelayAttachOutcome::Attached => {
                                return Ok(RelayPathAttachResult {
                                    attached: 1,
                                    key: Some(key),
                                });
                            }
                            ReliableRelayAttachOutcome::RejectedDuplicate => {
                                continue;
                            }
                        }
                    }
                    Err(err) if reliable_relay_error_is_migratable(&err) => {
                        context.mark_relay_path_failure(key.underlay, key.index);
                        last_retryable_error = Some(err);
                    }
                    Err(err) => return Err(err),
                }
            }
            Err(err) if relay_path_open_error_is_retryable(key.underlay, &err) => {
                context.mark_relay_path_failure(key.underlay, key.index);
                last_retryable_error = Some(err);
            }
            Err(err) => return Err(err),
        }
    }
    if remotes.is_empty() {
        Err(last_retryable_error.unwrap_or_else(|| no_schedulable_reliable_path_error(context)))
    } else {
        Ok(RelayPathAttachResult {
            attached: 0,
            key: None,
        })
    }
}

pub(in crate::runtime) async fn attach_reliable_relay_paths(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    lane: FlowLane,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: ReliableRelayAttachMode,
    inflight_path_claims: &HashSet<RelayPathKey>,
) -> Result<usize, RuntimeError> {
    let mut recovery_excluded_paths = HashSet::<RelayPathKey>::new();
    attach_reliable_relay_paths_with_claims_and_recovery_exclusions(
        context,
        spec,
        lane,
        remotes,
        send_stream,
        resend_fin,
        mode,
        &mut recovery_excluded_paths,
        inflight_path_claims,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn attach_reliable_relay_paths_with_claims_and_recovery_exclusions(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    lane: FlowLane,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: ReliableRelayAttachMode,
    recovery_excluded_paths: &mut HashSet<RelayPathKey>,
    inflight_path_claims: &HashSet<RelayPathKey>,
) -> Result<usize, RuntimeError> {
    let payload_bytes = match mode {
        ReliableRelayAttachMode::Any | ReliableRelayAttachMode::RecoveryRepair => {
            reliable_relay_attach_payload_bytes(send_stream, lane, context.mux_limits)
        }
        ReliableRelayAttachMode::BulkStriping => {
            reliable_relay_bulk_striping_payload_bytes(send_stream, context.mux_limits)
        }
    };
    if matches!(mode, ReliableRelayAttachMode::BulkStriping) {
        let result = attach_relay_path_candidates(
            context,
            remotes,
            RelayPathAttachRequest {
                spec,
                lane,
                send_stream,
                resend_fin,
                candidates: reliable_relay_exclude_inflight_open_claims(
                    context.ordered_reliable_bulk_striping_path_keys(payload_bytes),
                    &inflight_path_claims,
                ),
                role: StreamOpenRole::Validation,
                send_attach_control: false,
            },
        )
        .await;
        match result {
            Ok(result) if result.attached > 0 || !remotes.is_empty() => {
                return Ok(result.attached);
            }
            Ok(_) => {}
            Err(err)
                if remotes.is_empty()
                    && (stream_open_error_is_path_retryable(&err)
                        || udp_stream_open_error_is_path_retryable(&err)) => {}
            Err(err) => return Err(err),
        }
    }
    let role = reliable_relay_attach_role(lane, send_stream, resend_fin, mode);
    if role == StreamOpenRole::Repair {
        let result = attach_relay_path_candidates(
            context,
            remotes,
            RelayPathAttachRequest {
                spec,
                lane,
                send_stream,
                resend_fin,
                candidates: reliable_relay_exclude_inflight_open_claims(
                    reliable_relay_recovery_attach_candidates(
                        reliable_relay_repair_path_candidates(
                            context,
                            remotes,
                            lane,
                            payload_bytes,
                        ),
                        recovery_excluded_paths,
                        remotes.is_empty(),
                    ),
                    &inflight_path_claims,
                ),
                role,
                send_attach_control: true,
            },
        )
        .await?;
        if result.attached > 0
            && let Some(key) = result.key
        {
            recovery_excluded_paths.insert(key);
        }
        return Ok(result.attached);
    }
    let result = attach_relay_path_candidates(
        context,
        remotes,
        RelayPathAttachRequest {
            spec,
            lane,
            send_stream,
            resend_fin,
            candidates: reliable_relay_exclude_inflight_open_claims(
                reliable_relay_recovery_attach_candidates(
                    reliable_relay_active_path_candidates(context, remotes, lane, payload_bytes),
                    recovery_excluded_paths,
                    remotes.is_empty(),
                ),
                &inflight_path_claims,
            ),
            role,
            send_attach_control: true,
        },
    )
    .await?;
    Ok(result.attached)
}

pub(in crate::runtime) fn reliable_relay_attach_role(
    lane: FlowLane,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: ReliableRelayAttachMode,
) -> StreamOpenRole {
    match mode {
        ReliableRelayAttachMode::BulkStriping => StreamOpenRole::Validation,
        ReliableRelayAttachMode::RecoveryRepair => StreamOpenRole::Repair,
        ReliableRelayAttachMode::Any
            if reliable_relay_should_race_repair(lane, send_stream, resend_fin, mode) =>
        {
            StreamOpenRole::Repair
        }
        ReliableRelayAttachMode::Any => StreamOpenRole::Active,
    }
}

pub(in crate::runtime) fn reliable_relay_active_path_candidates(
    context: &ClientPathContext,
    remotes: &ReliableRelayRemoteSet,
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<RelayPathKey> {
    context
        .ordered_reliable_path_keys(lane, payload_bytes)
        .into_iter()
        .filter(|key| !remotes.contains_path_key(*key))
        .collect()
}

fn reliable_relay_recovery_attach_candidates(
    candidates: Vec<RelayPathKey>,
    recovery_excluded_paths: &HashSet<RelayPathKey>,
    allow_excluded_last_resort: bool,
) -> Vec<RelayPathKey> {
    if recovery_excluded_paths.is_empty() {
        return candidates;
    }
    let filtered = candidates
        .iter()
        .copied()
        .filter(|key| !recovery_excluded_paths.contains(key))
        .collect::<Vec<_>>();
    if filtered.is_empty() && allow_excluded_last_resort {
        candidates
    } else {
        filtered
    }
}

fn reliable_relay_exclude_inflight_open_claims(
    candidates: Vec<RelayPathKey>,
    inflight_path_claims: &HashSet<RelayPathKey>,
) -> Vec<RelayPathKey> {
    candidates
        .into_iter()
        .filter(|candidate| !inflight_path_claims.contains(candidate))
        .collect()
}

pub(in crate::runtime) fn reliable_relay_repair_path_candidates(
    context: &ClientPathContext,
    remotes: &ReliableRelayRemoteSet,
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<RelayPathKey> {
    context
        .ordered_reliable_repair_path_keys(
            remotes.active_path_index_for(UnderlayProtocol::Tcp),
            remotes.active_path_index_for(UnderlayProtocol::Udp),
            lane,
            payload_bytes,
        )
        .into_iter()
        .filter(|key| !remotes.contains_path_key(*key))
        .collect()
}

pub(in crate::runtime) fn reliable_relay_should_race_repair(
    lane: FlowLane,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: ReliableRelayAttachMode,
) -> bool {
    matches!(mode, ReliableRelayAttachMode::Any)
        && !resend_fin
        && (send_stream.repair_bytes() > 0
            || (lane.is_latency_sensitive() && send_stream.repair_bytes() <= PATH_OPEN_SCORE_BYTES))
}

pub(in crate::runtime) fn reliable_relay_attach_payload_bytes(
    send_stream: &ReliableSendStream,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> usize {
    let floor = if lane.is_latency_sensitive() {
        PATH_OPEN_SCORE_BYTES
    } else {
        reliable_relay_buffer_len(mux_limits)
    };
    let repair_bytes = send_stream.repair_bytes().max(floor);
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    repair_bytes.min(stream_window)
}

pub(in crate::runtime) fn reliable_relay_bulk_striping_payload_bytes(
    send_stream: &ReliableSendStream,
    mux_limits: MuxLimits,
) -> usize {
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    let decision_quantum =
        adaptive_reliable_relay_chunk_bytes(None, FlowLane::Throughput, mux_limits)
            .min(reliable_relay_buffer_len(mux_limits))
            .min(stream_window)
            .max(PATH_OPEN_SCORE_BYTES);
    let repair_bytes = send_stream.repair_bytes();
    if repair_bytes == 0 {
        return decision_quantum;
    }
    repair_bytes
        .min(decision_quantum)
        .min(stream_window)
        .max(PATH_OPEN_SCORE_BYTES)
}

pub(in crate::runtime) fn reliable_relay_bulk_validation_payload_bytes(
    send_stream: &ReliableSendStream,
    mux_limits: MuxLimits,
) -> usize {
    let proof_ceiling = relay_lane_startup_chunk_bytes(FlowLane::Latency, mux_limits);
    let proof_payload = reliable_relay_bulk_striping_payload_bytes(send_stream, mux_limits)
        .min(proof_ceiling)
        .max(PATH_OPEN_SCORE_BYTES);
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    proof_payload.min(stream_window).max(PATH_OPEN_SCORE_BYTES)
}

#[cfg(test)]
#[path = "remote_test.rs"]
mod tests;
