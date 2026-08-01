//! Request-stream carrier attachment ownership.
//!
//! A logical request stream owns its carrier membership, attachment generation,
//! scheduler leases, and frame fan-in. Relay code may open or select carriers,
//! but a pending carrier becomes durable only when this owner commits it.

use super::super::feedback::{
    StreamAckPublication, StreamAckPublicationCursor, StreamMaxDataPublication,
};
use crate::model::capacity::reliable_relay_buffer_len;
#[cfg(test)]
use crate::model::path::next_carrier_path_instance_id;
use crate::model::path::{CarrierPathInstanceId, RelayPathInstance, RelayPathKey};
use crate::mux::MuxLimits;
use crate::protocol::{Frame, ResetReason, StreamId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{ReliablePathCommand, ReliablePathCommandSender};
use crate::runtime::path::{ClientPathContext, RelayPathLoadLease};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamHandle};
use crate::scheduler::{PathSnapshot, TrafficClass, path_is_backup, score_path};
use std::time::Instant;
use tokio::sync::mpsc;

/// Open carrier awaiting attachment-set commit.
///
/// Keeping stream cleanup and scheduler load in one value makes cancellation,
/// duplicate rejection, and attach-control failure the same rollback path.
pub(in crate::runtime) struct OpenedRemoteStream {
    stream: Option<ReliablePathStream>,
    path_index: usize,
    path_instance_id: CarrierPathInstanceId,
    advertised_recv_max_offset: u64,
    load_lease: Option<RelayPathLoadLease>,
}

impl OpenedRemoteStream {
    pub(in crate::runtime) fn from_opened_carrier(
        carrier: crate::runtime::path::OpenedReliableCarrierStream,
        path_index: usize,
        advertised_recv_max_offset: u64,
    ) -> Self {
        let path_instance_id = carrier.path_instance_id;
        Self {
            stream: Some(ReliablePathStream::from_opened_carrier(carrier)),
            path_index,
            path_instance_id,
            advertised_recv_max_offset,
            load_lease: None,
        }
    }

    /// A concrete carrier open starts without scheduler ownership; its caller
    /// adds the reservation that represents the product attachment demand.
    #[cfg(test)]
    pub(in crate::runtime) fn pending(stream: ReliablePathStream, path_index: usize) -> Self {
        Self {
            stream: Some(stream),
            path_index,
            path_instance_id: next_carrier_path_instance_id(),
            advertised_recv_max_offset: 0,
            load_lease: None,
        }
    }

    pub(in crate::runtime) fn stream(&self) -> &ReliablePathStream {
        self.stream.as_ref().expect("pending remote stream")
    }

    pub(in crate::runtime) fn path_index(&self) -> usize {
        self.path_index
    }

    pub(in crate::runtime) fn with_load_lease(mut self, lease: RelayPathLoadLease) -> Self {
        debug_assert!(self.load_lease.is_none());
        debug_assert_eq!(
            lease.key(),
            RelayPathKey {
                underlay: self.stream().underlay,
                index: self.path_index,
            }
        );
        self.load_lease = Some(lease);
        self
    }

    pub(in crate::runtime) fn into_attachment_parts(
        mut self,
    ) -> (
        ReliablePathStream,
        usize,
        CarrierPathInstanceId,
        u64,
        Option<RelayPathLoadLease>,
    ) {
        let stream = self.stream.take().expect("pending remote stream");
        let load_lease = self.load_lease.take();
        (
            stream,
            self.path_index,
            self.path_instance_id,
            self.advertised_recv_max_offset,
            load_lease,
        )
    }

    /// A stream that never commits to a remote set must release both the peer
    /// binding and the local carrier actor entry.
    pub(in crate::runtime) async fn close(mut self) {
        drop(self.load_lease.take());
        if let Some(stream) = self.stream.as_ref() {
            stream.send_detach().await;
            stream.close().await;
        }
        drop(self.stream.take());
    }
}

impl Drop for OpenedRemoteStream {
    fn drop(&mut self) {
        // Scheduler ownership must disappear before carrier cleanup can block.
        drop(self.load_lease.take());
        let Some(stream) = self.stream.take() else {
            return;
        };
        let _ = stream.retire_uncommitted();
    }
}

pub(in crate::runtime) struct ReliableRelayRemotePath {
    pub(in crate::runtime) path_index: usize,
    pub(in crate::runtime) path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime) attachment_id: u64,
    // Declaration order also depublishes load before an abrupt stream drop.
    pub(in crate::runtime) load_lease: Option<RelayPathLoadLease>,
    pub(in crate::runtime) attached_at: Instant,
    pub(in crate::runtime) path_proof_id: Option<u64>,
    pub(in crate::runtime) path_proof_generation: u64,
    /// Greatest shared receive grant accepted by this attachment's queue.
    pub(in crate::runtime) published_max_data_offset: u64,
    /// Publication fence for the logical receiver's retained cumulative ACK.
    stream_ack_publication: StreamAckPublicationCursor,
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
            path_instance_id: self.path_instance_id,
            attachment_id: self.attachment_id,
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

/// One retained receive-only carrier attachment. Its separate collection is
/// the authority fence: it can return receive progress for this stream, but it
/// is never visible to request scheduling, load accounting, or OPEN_STREAM.
struct ReliableRelayReceiveOnlyPath {
    instance: RelayPathInstance,
    commands: ReliablePathCommandSender,
    published_max_data_offset: u64,
    stream_ack_publication: StreamAckPublicationCursor,
}

/// Reports whether attachment-set ownership committed; a rejected pending open
/// rolls back its carrier and scheduler lease when the value is dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ReliableRelayAttachOutcome {
    Attached,
    RejectedDuplicate,
}

/// One unpublished attachment incarnation reserved for an exact validation
/// candidate. The value is deliberately non-clone: ordinary membership may
/// consume the reservation once, after its target stream and physical carrier
/// identity have been bound.
#[derive(Debug)]
pub(in crate::runtime) struct ReliableRelayAttachmentReservation {
    stream_id: StreamId,
    attachment_id: u64,
    instance: Option<RelayPathInstance>,
}

impl ReliableRelayAttachmentReservation {
    pub(in crate::runtime) fn attachment_id(&self) -> u64 {
        self.attachment_id
    }

    pub(in crate::runtime) fn bind_exact(
        mut self,
        stream_id: StreamId,
        instance: RelayPathInstance,
    ) -> Result<Self, RuntimeError> {
        if self.instance.is_some()
            || stream_id != self.stream_id
            || instance.attachment_id != self.attachment_id
        {
            return Err(RuntimeError::Protocol(
                "reserved relay attachment identity does not match validation candidate",
            ));
        }
        self.instance = Some(instance);
        Ok(self)
    }
}

pub(in crate::runtime) struct ReliableRelayRemoteSet {
    stream_id: StreamId,
    pub(in crate::runtime) paths: Vec<ReliableRelayRemotePath>,
    /// One exact validation-only request attachment. It owns its bounded
    /// OriginalData flight without entering ordinary scheduling membership.
    validation_attachment: Option<RelayPathInstance>,
    receive_only_paths: Vec<ReliableRelayReceiveOnlyPath>,
    frames_tx: mpsc::Sender<ReliableRelayRemoteFrame>,
    frames_rx: mpsc::Receiver<ReliableRelayRemoteFrame>,
    next_instance_id: u64,
    membership_generation: u64,
    desired_max_data_offset: u64,
    desired_stream_ack_generation: u64,
    desired_stream_ack_frames: Vec<Frame>,
}

impl ReliableRelayRemoteSet {
    pub(in crate::runtime) fn new(opened: OpenedRemoteStream, frame_queue: usize) -> Self {
        let stream_id = opened.stream().stream_id;
        let (frames_tx, frames_rx) = mpsc::channel(frame_queue);
        let mut set = Self {
            stream_id,
            paths: Vec::new(),
            validation_attachment: None,
            receive_only_paths: Vec::new(),
            frames_tx,
            frames_rx,
            next_instance_id: 0,
            membership_generation: 0,
            desired_max_data_offset: 0,
            desired_stream_ack_generation: 0,
            desired_stream_ack_frames: Vec::new(),
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

    /// Reserves one target-stream attachment incarnation without publishing
    /// membership. Failed candidates consume their incarnation; it is never
    /// reused for another physical carrier.
    pub(in crate::runtime) fn reserve_attachment_incarnation(
        &mut self,
    ) -> ReliableRelayAttachmentReservation {
        let attachment_id = self.allocate_attachment_incarnation();
        ReliableRelayAttachmentReservation {
            stream_id: self.stream_id,
            attachment_id,
            instance: None,
        }
    }

    /// Binds one validation-only attachment as a live request-flight owner.
    ///
    /// The attachment remains absent from `paths`, so ordinary selection,
    /// flow load, and membership generations do not change. Its exact
    /// identity is nevertheless live for ordered-flight and recovery
    /// ownership until settlement or acknowledged promotion.
    pub(in crate::runtime) fn bind_validation_attachment(
        &mut self,
        reservation: ReliableRelayAttachmentReservation,
        instance: RelayPathInstance,
    ) -> Result<ReliableRelayAttachmentReservation, RuntimeError> {
        let reservation = reservation.bind_exact(self.stream_id, instance)?;
        if instance.key.underlay != UnderlayProtocol::Tcp
            || self.validation_attachment.is_some()
            || self
                .paths
                .iter()
                .any(|path| path.key() == instance.key || path.instance() == instance)
        {
            return Err(RuntimeError::ReliablePathRetired);
        }
        self.validation_attachment = Some(instance);
        Ok(reservation)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime) fn validation_attachment_is_current(
        &self,
        instance: RelayPathInstance,
    ) -> bool {
        self.validation_attachment == Some(instance)
    }

    /// Removes the exact validation-only attachment from live flight
    /// ownership without changing ordinary membership.
    pub(in crate::runtime) fn settle_validation_attachment(
        &mut self,
        instance: RelayPathInstance,
    ) -> bool {
        if self.validation_attachment != Some(instance) {
            return false;
        }
        self.validation_attachment = None;
        true
    }

    fn allocate_attachment_incarnation(&mut self) -> u64 {
        let attachment_id = self.next_instance_id;
        self.next_instance_id = self.next_instance_id.wrapping_add(1);
        attachment_id
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

    /// Returns the currently available attachment with the lowest scheduler
    /// completion estimate. Attachment order is never path-quality evidence.
    pub(in crate::runtime) fn lowest_eta_path_snapshot(
        &self,
        context: &ClientPathContext,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> Option<PathSnapshot> {
        let choose = |allow_backup: bool| {
            self.paths
                .iter()
                .filter_map(|path| context.reliable_path_snapshot(path.key()))
                .filter(|snapshot| allow_backup || !path_is_backup(*snapshot))
                .filter_map(|snapshot| {
                    score_path(snapshot, lane, payload_bytes).map(|score| (score.eta_ms, snapshot))
                })
                .min_by(|left, right| left.0.total_cmp(&right.0))
                .map(|(_, snapshot)| snapshot)
        };
        choose(false).or_else(|| choose(true))
    }

    pub(in crate::runtime) fn preferred_path_key(
        &self,
        context: &ClientPathContext,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> Option<RelayPathKey> {
        let choose = |allow_backup: bool| {
            self.paths
                .iter()
                .filter_map(|path| {
                    let snapshot = context.reliable_path_snapshot(path.key())?;
                    if !allow_backup && path_is_backup(snapshot) {
                        return None;
                    }
                    let score = score_path(snapshot, lane, payload_bytes)?;
                    Some((path.key(), score.eta_ms))
                })
                .min_by(|left, right| left.1.total_cmp(&right.1))
                .map(|(key, _)| key)
        };
        choose(false).or_else(|| choose(true))
    }

    pub(in crate::runtime) fn preferred_path_underlay(
        &self,
        context: &ClientPathContext,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> Option<UnderlayProtocol> {
        self.preferred_path_key(context, lane, payload_bytes)
            .map(|key| key.underlay)
    }

    pub(in crate::runtime) fn contains_path_key(&self, key: RelayPathKey) -> bool {
        self.paths.iter().any(|path| path.key() == key)
    }

    pub(in crate::runtime) fn contains_path_instance(&self, instance: RelayPathInstance) -> bool {
        self.paths.iter().any(|path| path.instance() == instance)
    }

    /// Exact live owners of request OriginalData. Validation ownership is
    /// included here but remains absent from ordinary `path_instances()`.
    pub(in crate::runtime) fn flight_owner_instances(&self) -> Vec<RelayPathInstance> {
        self.paths
            .iter()
            .map(ReliableRelayRemotePath::instance)
            .chain(self.validation_attachment)
            .collect()
    }

    pub(in crate::runtime) fn contains_flight_owner_instance(
        &self,
        instance: RelayPathInstance,
    ) -> bool {
        self.contains_path_instance(instance) || self.validation_attachment == Some(instance)
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

    pub(in crate::runtime) fn accepted_path_count(&self) -> usize {
        self.paths.len()
    }

    /// Adopts one already-retained S2C attachment without granting request
    /// output authority. All inputs share the existing bounded relay fan-in.
    pub(in crate::runtime) fn attach_receive_only(
        &mut self,
        instance: RelayPathInstance,
        commands: ReliablePathCommandSender,
        mut frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
    ) -> bool {
        if self
            .receive_only_paths
            .iter()
            .any(|path| path.instance == instance)
        {
            return false;
        }
        let frames_tx = self.frames_tx.clone();
        tokio::spawn(async move {
            let mut product_terminal_received = false;
            while let Some(frame) = frames.recv().await {
                product_terminal_received |= matches!(
                    &frame,
                    Ok(Frame::StreamFin { .. } | Frame::StreamReset { .. })
                );
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
            if !product_terminal_received {
                let _ = frames_tx
                    .send(ReliableRelayRemoteFrame {
                        instance,
                        frame: Err(RuntimeError::ReliablePathSessionClosed),
                    })
                    .await;
            }
        });
        self.receive_only_paths.push(ReliableRelayReceiveOnlyPath {
            instance,
            commands,
            published_max_data_offset: 0,
            stream_ack_publication: StreamAckPublicationCursor::default(),
        });
        true
    }

    pub(in crate::runtime) fn has_receive_feedback_output(&self) -> bool {
        self.paths
            .iter()
            .any(|path| !path.stream.request_control_frame_queue_is_closed())
            || self
                .receive_only_paths
                .iter()
                .any(|path| !path.commands.control_frame_queue_is_closed())
    }

    pub(in crate::runtime) fn is_receive_only_instance(&self, instance: RelayPathInstance) -> bool {
        self.receive_only_paths
            .iter()
            .any(|path| path.instance == instance)
    }

    pub(in crate::runtime) fn remove_receive_only_instance(
        &mut self,
        instance: RelayPathInstance,
    ) -> bool {
        let Some(position) = self
            .receive_only_paths
            .iter()
            .position(|path| path.instance == instance)
        else {
            return false;
        };
        self.receive_only_paths.remove(position);
        true
    }

    pub(in crate::runtime) fn remove_closed_receive_only_paths(&mut self) {
        self.receive_only_paths
            .retain(|path| !path.commands.control_frame_queue_is_closed());
    }

    /// Advances retained shared receive credit and publishes it independently
    /// on every currently live attachment.
    pub(in crate::runtime) fn publish_max_data(
        &mut self,
        max_offset: u64,
    ) -> StreamMaxDataPublication {
        self.desired_max_data_offset = self.desired_max_data_offset.max(max_offset);
        self.retry_pending_max_data()
    }

    pub(in crate::runtime) fn retry_pending_max_data(&mut self) -> StreamMaxDataPublication {
        let desired = self.desired_max_data_offset;
        let mut publication = StreamMaxDataPublication::default();
        for path in &mut self.paths {
            if path.stream.request_control_frame_queue_is_closed()
                || path.published_max_data_offset >= desired
            {
                continue;
            }
            if path
                .stream
                .try_enqueue_request_control_frame(Frame::StreamMaxData {
                    stream_id: self.stream_id,
                    max_offset: desired,
                })
                .is_ok()
            {
                path.published_max_data_offset = desired;
                publication.published_offset = Some(desired);
            }
        }
        for path in &mut self.receive_only_paths {
            if path.commands.control_frame_queue_is_closed()
                || path.published_max_data_offset >= desired
            {
                continue;
            }
            if path
                .commands
                .try_enqueue_admitted_frame(
                    Frame::StreamMaxData {
                        stream_id: self.stream_id,
                        max_offset: desired,
                    },
                    TrafficClass::Control,
                )
                .is_ok()
            {
                path.published_max_data_offset = desired;
                publication.published_offset = Some(desired);
            }
        }
        publication.pending = self.has_pending_max_data_publication();
        publication
    }

    pub(in crate::runtime) fn has_pending_max_data_publication(&self) -> bool {
        self.paths.iter().any(|path| {
            !path.stream.request_control_frame_queue_is_closed()
                && path.published_max_data_offset < self.desired_max_data_offset
        }) || self.receive_only_paths.iter().any(|path| {
            !path.commands.control_frame_queue_is_closed()
                && path.published_max_data_offset < self.desired_max_data_offset
        })
    }

    pub(in crate::runtime) fn pending_max_data_capacity_notifies(
        &self,
    ) -> Vec<std::sync::Arc<tokio::sync::Notify>> {
        self.paths
            .iter()
            .filter(|path| {
                path.published_max_data_offset < self.desired_max_data_offset
                    && !path.stream.request_control_frame_queue_is_closed()
            })
            .filter_map(|path| path.stream.request_control_capacity_notify())
            .chain(
                self.receive_only_paths
                    .iter()
                    .filter(|path| {
                        path.published_max_data_offset < self.desired_max_data_offset
                            && !path.commands.control_frame_queue_is_closed()
                    })
                    .map(|path| path.commands.capacity_notify()),
            )
            .collect()
    }

    /// Retains the latest cumulative receive evidence and offers it to every
    /// exact live attachment. A newer generation subsumes an unqueued tail
    /// from an older cumulative snapshot.
    pub(in crate::runtime) fn publish_stream_ack(
        &mut self,
        generation: u64,
        cumulative_frames: Vec<Frame>,
    ) -> StreamAckPublication {
        debug_assert!(generation != 0);
        debug_assert!(!cumulative_frames.is_empty());
        debug_assert!(
            cumulative_frames
                .iter()
                .all(|frame| matches!(frame, Frame::StreamAck { .. }))
        );
        self.desired_stream_ack_generation = generation;
        self.desired_stream_ack_frames = cumulative_frames;
        self.retry_pending_stream_ack()
    }

    pub(in crate::runtime) fn stream_ack_generation(&self) -> u64 {
        self.desired_stream_ack_generation
    }

    pub(in crate::runtime) fn retry_pending_stream_ack(&mut self) -> StreamAckPublication {
        let generation = self.desired_stream_ack_generation;
        if generation == 0 || self.desired_stream_ack_frames.is_empty() {
            return StreamAckPublication::default();
        }

        let stream_id = self.stream_id;
        let frames = &self.desired_stream_ack_frames;
        let mut publication = StreamAckPublication::default();
        for path in &mut self.paths {
            if path.stream.request_control_frame_queue_is_closed() {
                continue;
            }
            let attachment = path.stream_ack_publication.retry_cumulative(
                generation,
                frames,
                |frame| {
                    debug_assert!(
                        matches!(&frame, Frame::StreamAck { stream_id: id, .. } if *id == stream_id)
                    );
                    path.stream.try_enqueue_request_control_frame(frame).is_ok()
                },
            );
            publication.accepted |= attachment.accepted;
            publication.published |= attachment.published;
        }
        for path in &mut self.receive_only_paths {
            if path.commands.control_frame_queue_is_closed() {
                continue;
            }
            let commands = &path.commands;
            let attachment = path.stream_ack_publication.retry_cumulative(
                generation,
                frames,
                |frame| {
                    debug_assert!(
                        matches!(&frame, Frame::StreamAck { stream_id: id, .. } if *id == stream_id)
                    );
                    commands
                        .try_enqueue_admitted_frame(frame, TrafficClass::Control)
                        .is_ok()
                },
            );
            publication.accepted |= attachment.accepted;
            publication.published |= attachment.published;
        }
        publication.pending = self.has_pending_stream_ack_publication();
        publication
    }

    pub(in crate::runtime) fn has_pending_stream_ack_publication(&self) -> bool {
        let generation = self.desired_stream_ack_generation;
        generation != 0
            && (self.paths.iter().any(|path| {
                !path.stream.request_control_frame_queue_is_closed()
                    && path.stream_ack_publication.is_pending(generation)
            }) || self.receive_only_paths.iter().any(|path| {
                !path.commands.control_frame_queue_is_closed()
                    && path.stream_ack_publication.is_pending(generation)
            }))
    }

    pub(in crate::runtime) fn pending_stream_ack_capacity_notifies(
        &self,
    ) -> Vec<std::sync::Arc<tokio::sync::Notify>> {
        let generation = self.desired_stream_ack_generation;
        self.paths
            .iter()
            .filter(|path| {
                generation != 0
                    && path.stream_ack_publication.is_pending(generation)
                    && !path.stream.request_control_frame_queue_is_closed()
            })
            .filter_map(|path| path.stream.request_control_capacity_notify())
            .chain(
                self.receive_only_paths
                    .iter()
                    .filter(|path| {
                        generation != 0
                            && path.stream_ack_publication.is_pending(generation)
                            && !path.commands.control_frame_queue_is_closed()
                    })
                    .map(|path| path.commands.capacity_notify()),
            )
            .collect()
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

    pub(in crate::runtime) fn set_lane(&mut self, lane: TrafficClass) {
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
        self.attach_opened(opened, true)
    }

    pub(in crate::runtime) fn attach_candidate(
        &mut self,
        opened: OpenedRemoteStream,
    ) -> ReliableRelayAttachOutcome {
        self.attach_opened(opened, false)
    }

    /// Publishes an already accepted validation attachment under the exact
    /// incarnation reserved before candidate I/O. This consumes no new
    /// attachment ID and performs no carrier or `OPEN_STREAM` operation.
    pub(in crate::runtime) fn adopt_reserved_attachment(
        &mut self,
        opened: OpenedRemoteStream,
        reservation: ReliableRelayAttachmentReservation,
    ) -> Result<ReliableRelayAttachOutcome, RuntimeError> {
        let expected = reservation.instance.ok_or(RuntimeError::Protocol(
            "reserved relay attachment is not bound to a carrier instance",
        ))?;
        let key = RelayPathKey {
            underlay: opened.stream().underlay,
            index: opened.path_index(),
        };
        if reservation.stream_id != self.stream_id
            || opened.stream().stream_id != self.stream_id
            || reservation.attachment_id != expected.attachment_id
            || key != expected.key
            || opened.path_instance_id != expected.path_instance_id
            || self.validation_attachment != Some(expected)
        {
            return Err(RuntimeError::Protocol(
                "opened relay attachment does not match its exact reservation",
            ));
        }
        if self.contains_path_key(key) {
            return Ok(ReliableRelayAttachOutcome::RejectedDuplicate);
        }
        self.validation_attachment = None;
        Ok(self.commit_opened(opened, true, reservation.attachment_id))
    }

    fn attach_opened(
        &mut self,
        opened: OpenedRemoteStream,
        retain_open_load: bool,
    ) -> ReliableRelayAttachOutcome {
        let path_index = opened.path_index();
        let underlay = opened.stream().underlay;
        let key = RelayPathKey {
            underlay,
            index: path_index,
        };
        if self.contains_path_key(key)
            || self
                .validation_attachment
                .is_some_and(|validation| validation.key == key)
        {
            return ReliableRelayAttachOutcome::RejectedDuplicate;
        }
        let attachment_id = self.allocate_attachment_incarnation();
        self.commit_opened(opened, retain_open_load, attachment_id)
    }

    fn commit_opened(
        &mut self,
        opened: OpenedRemoteStream,
        retain_open_load: bool,
        attachment_id: u64,
    ) -> ReliableRelayAttachOutcome {
        let path_index = opened.path_index();
        let underlay = opened.stream().underlay;
        let key = RelayPathKey {
            underlay,
            index: path_index,
        };
        debug_assert!(!self.contains_path_key(key));
        let path_instance_id = opened.path_instance_id;
        let instance = RelayPathInstance {
            key,
            path_instance_id,
            attachment_id,
        };
        let (stream, path_index, path_instance_id, advertised_recv_max_offset, mut load_lease) =
            opened.into_attachment_parts();
        self.desired_max_data_offset = self.desired_max_data_offset.max(advertised_recv_max_offset);
        if !retain_open_load {
            // Candidate ranking reserves during an open transaction. Attached
            // membership is not load until this stream assigns product data.
            drop(load_lease.take());
        }
        let (stream, mut frames) = stream.into_handle_and_frames();
        let frames_tx = self.frames_tx.clone();
        tokio::spawn(async move {
            let mut product_terminal_received = false;
            while let Some(frame) = frames.recv().await {
                product_terminal_received |= matches!(
                    &frame,
                    Ok(Frame::StreamFin { .. } | Frame::StreamReset { .. })
                );
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
            if product_terminal_received {
                return;
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
            path_instance_id,
            attachment_id,
            load_lease,
            attached_at: Instant::now(),
            path_proof_id: None,
            path_proof_generation: 0,
            published_max_data_offset: advertised_recv_max_offset,
            stream_ack_publication: StreamAckPublicationCursor::default(),
            stream,
        };
        if let Ok(Some(proof_id)) = path.stream.enqueue_path_proof() {
            path.path_proof_id = Some(proof_id);
        }
        self.paths.push(path);
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

    /// Returns the relay-input backlog visible at this instant.
    ///
    /// Ready-only receive batching snapshots this value before trying frames so
    /// producers cannot extend one actor turn indefinitely.
    pub(in crate::runtime) fn ready_frame_count(&self) -> usize {
        self.frames_rx.len()
    }

    /// Takes one already-queued frame without waiting.
    pub(in crate::runtime) fn try_recv_frame(&mut self) -> Option<ReliableRelayRemoteFrame> {
        self.frames_rx.try_recv().ok()
    }

    pub(in crate::runtime) fn has_buffered_frame(&self) -> bool {
        !self.frames_rx.is_empty()
    }

    pub(in crate::runtime) async fn close_all(&mut self) {
        let receive_only_paths = self.take_receive_only_paths();
        for path in self.take_paths_for_close() {
            path.stream.send_detach().await;
            path.stream.close().await;
        }
        for path in receive_only_paths {
            let _ = path
                .commands
                .send_control(ReliablePathCommand::SendFrame(Frame::StreamDetach {
                    stream_id: self.stream_id,
                }))
                .await;
            let _ = path
                .commands
                .send_control(ReliablePathCommand::CloseStream(self.stream_id))
                .await;
        }
    }

    /// Product endpoint failure is terminal across every attachment. A reset
    /// prevents retention and reinjection while carrier-only failures continue
    /// to use detach and preserve the logical stream for path recovery.
    pub(in crate::runtime) async fn reset_all(&mut self, reason: ResetReason) {
        let receive_only_paths = self.take_receive_only_paths();
        let paths = self.take_paths_for_close();
        futures::future::join_all(paths.into_iter().map(|path| async move {
            path.stream.reset_and_close(reason).await;
        }))
        .await;
        for path in receive_only_paths {
            let _ = path
                .commands
                .send_control(ReliablePathCommand::ResetAndCloseStream {
                    stream_id: self.stream_id,
                    reason,
                })
                .await;
        }
    }

    /// Successful retirement follows ordered FIN work on every carrier.
    pub(in crate::runtime) async fn close_all_ordered(&mut self) {
        let receive_only_paths = self.take_receive_only_paths();
        for path in self.take_paths_for_close() {
            path.stream.detach_and_close_ordered().await;
        }
        for path in receive_only_paths {
            let _ = path
                .commands
                .send_stream_ordered_frame(
                    Frame::StreamDetach {
                        stream_id: self.stream_id,
                    },
                    TrafficClass::Throughput,
                )
                .await;
            let _ = path
                .commands
                .send_stream_ordered_close(self.stream_id, TrafficClass::Throughput)
                .await;
        }
    }

    fn take_paths_for_close(&mut self) -> Vec<ReliableRelayRemotePath> {
        if !self.paths.is_empty() {
            self.membership_generation = self.membership_generation.wrapping_add(1);
        }
        let mut paths = std::mem::take(&mut self.paths);
        // The set stops owning every path as one atomic scheduling event even
        // when the first carrier queue makes detach asynchronous.
        for path in &mut paths {
            path.depublish_load();
        }
        paths
    }

    fn take_receive_only_paths(&mut self) -> Vec<ReliableRelayReceiveOnlyPath> {
        std::mem::take(&mut self.receive_only_paths)
    }

    pub(in crate::runtime) async fn fail_path_instance(
        &mut self,
        context: &ClientPathContext,
        instance: RelayPathInstance,
    ) -> bool {
        let Some(mut path) = self.remove_path_instance(instance) else {
            return false;
        };
        context.mark_relay_path_data_plane_failure(instance);
        path.depublish_load();
        path.stream.send_detach().await;
        path.stream.close().await;
        true
    }

    pub(in crate::runtime) async fn retire_path_instance(
        &mut self,
        instance: RelayPathInstance,
    ) -> bool {
        let Some(mut path) = self.remove_path_instance(instance) else {
            return false;
        };
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

    /// Transfers a pre-enqueue claim after the synchronous queue commit.
    /// Generation and instance were resolved with no intervening await.
    #[cfg(test)]
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
}

#[cfg(test)]
#[path = "attachment_test.rs"]
mod tests;
