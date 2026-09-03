//! Response path attachment and identity transactions.
//! It linearizes membership changes; ranking and carrier recovery stay outside it.

use super::ResponseStreamBinding;
use super::ack_clock::ResponseAckClockRateEvidence;
use super::evidence::{ServerPathMetricsEntry, install_path_metrics_entry};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::carrier_rate_authority::CarrierRateAuthorityStamp;
#[cfg(test)]
use crate::model::path::next_carrier_path_instance_id;
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey, PathPolicy};
use crate::model::product_qualification::ProductQualificationLedger;
use crate::model::requalification::StreamPathQualification;
use crate::model::response::ResponsePathObservation;
#[cfg(test)]
use crate::protocol::PathId;
use crate::protocol::{Frame, PathUsage, StreamId, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::authority::NativeCarrierSchedulingShapeSnapshot;
use crate::runtime::path::commands::{
    ReliablePathCommandQueueSnapshot, ReliablePathCommandSender, ReliablePathLoadRegistration,
};
use crate::runtime::path::proof::PathProofObservation;
use crate::runtime::stream::feedback::{
    StreamAckPublication, StreamAckPublicationCursor, StreamMaxDataPublication,
};
use crate::scheduler::{PathSnapshot, TrafficClass};
use crate::transport::RateHint;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

#[cfg(test)]
pub(in crate::runtime) fn next_server_carrier_path_instance_id() -> CarrierPathInstanceId {
    next_carrier_path_instance_id()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ResponseStreamAttachOutcome {
    Attached,
    ReplacedClosedOutput,
    RejectedDuplicateLiveOutput,
    RejectedClosedStream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ResponsePathDetachOutcome {
    Begun(u64),
    Pending(u64),
}

/// Path evidence installed in the same transaction that publishes an output.
#[derive(Debug, Clone, Copy, Default)]
pub(in crate::runtime) struct ResponseOutputAttachmentState {
    pub(in crate::runtime::stream) metrics: Option<ServerPathMetricsEntry>,
    pub(in crate::runtime::stream) native_scheduling_shape:
        Option<NativeCarrierSchedulingShapeSnapshot>,
    pub(in crate::runtime::stream) peer_usage: Option<(u64, PathUsage)>,
    pub(in crate::runtime::stream) path_proof: Option<PathProofObservation>,
}

/// Complete output membership transaction for one carrier instance.
pub(in crate::runtime) struct ResponseOutputAttachment {
    pub(in crate::runtime::stream) key: CarrierPathKey,
    pub(in crate::runtime::stream) path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime::stream) local_policy: PathPolicy,
    /// Endpoint-local configuration bound to this exact carrier incarnation.
    /// Mutable evidence refreshes cannot rewrite it.
    pub(in crate::runtime::stream) startup_rate_prior: RateHint,
    pub(in crate::runtime::stream) commands: ReliablePathCommandSender,
    pub(in crate::runtime::stream) state: ResponseOutputAttachmentState,
}

/// Per-output Product goodput proven by one exact Data-ACK epoch.
///
/// The deadline is frozen when the ACK is observed. Retaining an expired
/// value is diagnostic only; every authority consumer must ask this epoch for
/// its value at the same scheduling instant.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct ResponseProductRateEpoch {
    pub(super) rate_bps: f64,
    pub(super) sample_count: u32,
    pub(super) sample_bytes: u64,
    pub(super) observed_at: Instant,
    pub(super) expires_at: Instant,
}

impl ResponseProductRateEpoch {
    pub(super) fn new(
        rate_bps: f64,
        sample_count: u32,
        sample_bytes: u64,
        observed_at: Instant,
        freshness_horizon: Duration,
    ) -> Option<Self> {
        (rate_bps.is_finite() && rate_bps > 0.0)
            .then(|| observed_at.checked_add(freshness_horizon))
            .flatten()
            .map(|expires_at| Self {
                rate_bps,
                sample_count,
                sample_bytes,
                observed_at,
                expires_at,
            })
    }

    pub(super) fn fresh_rate_at(self, now: Instant) -> Option<f64> {
        (self.observed_at <= now && now < self.expires_at).then_some(self.rate_bps)
    }
}

fn apply_attachment_state(
    entry: &mut ResponseStreamOutputEntry,
    state: ResponseOutputAttachmentState,
) -> bool {
    let mut changed = state
        .metrics
        .is_some_and(|metrics| install_path_metrics_entry(entry, metrics));
    if let Some(shape) = state.native_scheduling_shape
        && entry.native_scheduling_shape != Some(shape)
    {
        entry.native_scheduling_shape = Some(shape);
        changed = true;
    }
    if let Some((sequence, usage)) = state.peer_usage
        && entry
            .peer_usage_sequence
            .is_none_or(|current| sequence > current)
    {
        let usage_changed = (entry.local_policy.backup
            || entry.peer_usage == Some(PathUsage::Backup))
            != (entry.local_policy.backup || usage == PathUsage::Backup);
        changed |= usage_changed || entry.peer_usage_sequence != Some(sequence);
        entry.peer_usage_sequence = Some(sequence);
        entry.peer_usage = Some(usage);
    }
    if entry.path_proof.is_none()
        && let Some(observation) = state.path_proof
    {
        entry.path_proof = Some(observation);
        changed = true;
    }
    changed
}

/// One carrier output attached to a response stream.
///
/// It owns carrier command access and sender-evidence fields for this stream on
/// this path. Product reinjection and ordering identity stay in `ResponseStreamBinding`.
pub(in crate::runtime) struct ResponseStreamOutputEntry {
    pub(super) key: CarrierPathKey,
    pub(super) path_instance_id: CarrierPathInstanceId,
    pub(super) local_policy: PathPolicy,
    pub(super) startup_rate_prior: RateHint,
    pub(super) incarnation: u64,
    pub(super) commands: ReliablePathCommandSender,
    /// Publishes demand to the exact ordered writer only while this output owns
    /// unacknowledged unique OriginalData. Its lane survives inactive periods.
    pub(super) load_registration: ReliablePathLoadRegistration,
    /// Unacknowledged unique OriginalData assigned to this response output.
    /// Reinjection copies remain in `bytes_in_flight` but never enter this counter.
    pub(super) original_data_in_flight_bytes: u64,
    /// Data-ACK recovery may withdraw an output from new OriginalData placement
    /// without closing it or interfering with native carrier recovery.
    pub(super) qualification: StreamPathQualification,
    /// Durable exact-volume Product qualification for this attachment
    /// incarnation. Numeric rate evidence has an independent lifetime.
    pub(super) product_qualification: ProductQualificationLedger,
    pub(super) bytes_in_flight: u64,
    /// Exact OriginalData ACK goodput. It is per-flow evidence, not carrier
    /// capacity, and its immutable deadline owns all placement authority.
    pub(super) product_rate_epoch: Option<ResponseProductRateEpoch>,
    /// Per-output ACK clock; product ordering timestamps can be advanced when a
    /// different path closes a hole and therefore cannot own this boundary.
    pub(super) tcp_product_rate_evidence: Option<ResponseAckClockRateEvidence>,
    pub(super) srtt_ms: Option<f64>,
    pub(super) delivery_samples: u32,
    /// Cumulative uniquely owned product bytes ACKed on this output.
    ///
    /// The flight ledger increments this only for unambiguous `OriginalData`;
    /// duplicated `ReinjectedData` never contributes.
    pub(super) original_data_acked_bytes: u64,
    /// Greatest shared receive grant accepted by this attachment's queue.
    pub(super) published_max_data_offset: u64,
    /// Latest cumulative Data ACK generation accepted by this attachment.
    pub(super) ack_publication: StreamAckPublicationCursor,
    pub(super) local_path_metrics: Option<ServerPathMetricsEntry>,
    pub(super) peer_path_metrics: Option<ServerPathMetricsEntry>,
    /// Complete endpoint-local NativeMode decision/shape for this exact QUIC
    /// output. It is separate from peer/ACK/Product evidence.
    pub(super) native_scheduling_shape: Option<NativeCarrierSchedulingShapeSnapshot>,
    /// Latest directional preference advertised by the peer for this exact
    /// carrier instance. It is independent of local path health.
    pub(super) peer_usage: Option<PathUsage>,
    pub(super) peer_usage_sequence: Option<u64>,
    /// Monotonic response-direction proof for this physical carrier instance.
    pub(super) path_proof: Option<PathProofObservation>,
}

pub(in crate::runtime) struct ResponseStreamOutputs {
    /// Next exact output incarnation, or permanent exhaustion after MAX.
    pub(super) next_output_incarnation: Option<u64>,
    /// Outputs withdrawn from scheduling but still owned by the stream actor.
    /// Their flights remain live until the ordered detach event is applied.
    pub(super) detaching: Vec<ResponseStreamOutputEntry>,
    pub(super) entries: Vec<ResponseStreamOutputEntry>,
    /// Unique OriginalData retained by this response stream awaiting MPP
    /// DataACK. This survives output detach and excludes repair copies and
    /// native carrier work.
    pub(super) original_data_in_flight_bytes: u64,
    /// Offset-free sender-service staging belongs to the response stream, not
    /// to any carrier output.
    pub(super) data_level_queue_bytes: u64,
    /// Retained idempotent receive grant shared by every attachment.
    pub(super) desired_max_data_offset: u64,
    pub(super) next_requalification_probe_id: Option<u64>,
    /// Fair round-robin start for the next stale requalification attempt.
    /// Output order remains stable for every other scheduling decision.
    pub(super) next_requalification_candidate_index: usize,
}

fn publish_pending_max_data(
    outputs: &mut ResponseStreamOutputs,
    stream_id: StreamId,
) -> StreamMaxDataPublication {
    let desired = outputs.desired_max_data_offset;
    let mut publication = StreamMaxDataPublication::default();
    for entry in &mut outputs.entries {
        if entry.commands.control_frame_admission_is_closed()
            || entry.published_max_data_offset >= desired
        {
            continue;
        }
        if entry
            .commands
            .try_enqueue_admitted_frame(
                Frame::StreamMaxData {
                    stream_id,
                    max_offset: desired,
                },
                TrafficClass::Control,
            )
            .is_ok()
        {
            entry.published_max_data_offset = desired;
            publication.published_offset = Some(desired);
        }
    }
    publication.pending = outputs.entries.iter().any(|entry| {
        !entry.commands.control_frame_admission_is_closed()
            && entry.published_max_data_offset < desired
    });
    publication
}

fn publish_ack_update(
    outputs: &mut ResponseStreamOutputs,
    generation: u64,
    update_frames: &[Frame],
    cumulative_frames: &[Frame],
) -> StreamAckPublication {
    let mut publication = StreamAckPublication::default();
    for entry in &mut outputs.entries {
        if entry.commands.control_frame_admission_is_closed() {
            continue;
        }
        let commands = &entry.commands;
        let attachment = entry.ack_publication.publish_update(
            generation,
            update_frames,
            cumulative_frames,
            |frame| {
                commands
                    .try_enqueue_admitted_frame(frame, TrafficClass::Control)
                    .is_ok()
            },
        );
        publication.accepted |= attachment.accepted;
        publication.published |= attachment.published;
    }
    publication.published = outputs.entries.iter().any(|entry| {
        !entry.commands.control_frame_admission_is_closed()
            && !entry.ack_publication.is_pending(generation)
    });
    publication.pending = outputs.entries.iter().any(|entry| {
        !entry.commands.control_frame_admission_is_closed()
            && entry.ack_publication.is_pending(generation)
    });
    publication
}

fn retry_pending_ack(
    outputs: &mut ResponseStreamOutputs,
    generation: u64,
    cumulative_frames: &[Frame],
) -> StreamAckPublication {
    let mut publication = StreamAckPublication::default();
    for entry in &mut outputs.entries {
        if entry.commands.control_frame_admission_is_closed()
            || !entry.ack_publication.is_pending(generation)
        {
            continue;
        }
        let commands = &entry.commands;
        let attachment =
            entry
                .ack_publication
                .retry_cumulative(generation, cumulative_frames, |frame| {
                    commands
                        .try_enqueue_admitted_frame(frame, TrafficClass::Control)
                        .is_ok()
                });
        publication.accepted |= attachment.accepted;
        publication.published |= attachment.published;
    }
    publication.published = outputs.entries.iter().any(|entry| {
        !entry.commands.control_frame_admission_is_closed()
            && !entry.ack_publication.is_pending(generation)
    });
    publication.pending = outputs.entries.iter().any(|entry| {
        !entry.commands.control_frame_admission_is_closed()
            && entry.ack_publication.is_pending(generation)
    });
    publication
}

impl ResponseStreamBinding {
    pub(in crate::runtime) fn has_live_output(&self) -> bool {
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .any(|entry| !entry.commands.is_closed())
    }

    pub(in crate::runtime::stream) fn has_product_output(&self) -> bool {
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .any(|entry| entry.commands.product_admission_active())
    }

    fn allocate_output_incarnation(
        outputs: &mut ResponseStreamOutputs,
    ) -> Result<u64, RuntimeError> {
        let incarnation = outputs
            .next_output_incarnation
            .ok_or(RuntimeError::ExactIdentityExhausted)?;
        outputs.next_output_incarnation = incarnation.checked_add(1);
        Ok(incarnation)
    }

    pub(in crate::runtime) fn output_membership_generation(&self) -> u64 {
        self.output_membership_generation.load(Ordering::Acquire)
    }

    /// Publishes one newly observed Data ACK generation on every live output.
    /// An output exactly caught up to the preceding generation may receive the
    /// smaller update; a new or blocked output receives cumulative state.
    pub(in crate::runtime) fn publish_ack(
        &self,
        generation: u64,
        update_frames: &[Frame],
        cumulative_frames: &[Frame],
    ) -> StreamAckPublication {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        publish_ack_update(&mut outputs, generation, update_frames, cumulative_frames)
    }

    pub(in crate::runtime) fn retry_pending_ack(
        &self,
        generation: u64,
        cumulative_frames: &[Frame],
    ) -> StreamAckPublication {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        retry_pending_ack(&mut outputs, generation, cumulative_frames)
    }

    pub(in crate::runtime) fn pending_ack_capacity_notifies(
        &self,
        generation: u64,
    ) -> Vec<std::sync::Arc<tokio::sync::Notify>> {
        if generation == 0 {
            return Vec::new();
        }
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        outputs
            .entries
            .iter()
            .filter(|entry| {
                !entry.commands.control_frame_admission_is_closed()
                    && entry.ack_publication.is_pending(generation)
            })
            .map(|entry| entry.commands.capacity_notify())
            .collect()
    }

    /// Advances retained shared receive credit and publishes it independently
    /// on every currently live attachment.
    pub(in crate::runtime) fn publish_max_data(
        &self,
        stream_id: StreamId,
        max_offset: u64,
    ) -> StreamMaxDataPublication {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        outputs.desired_max_data_offset = outputs.desired_max_data_offset.max(max_offset);
        publish_pending_max_data(&mut outputs, stream_id)
    }

    pub(in crate::runtime) fn retry_pending_max_data(
        &self,
        stream_id: StreamId,
    ) -> StreamMaxDataPublication {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        publish_pending_max_data(&mut outputs, stream_id)
    }

    pub(in crate::runtime) fn has_pending_max_data_publication(&self) -> bool {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        outputs.entries.iter().any(|entry| {
            !entry.commands.control_frame_admission_is_closed()
                && entry.published_max_data_offset < outputs.desired_max_data_offset
        })
    }

    pub(in crate::runtime) fn pending_max_data_capacity_notifies(
        &self,
    ) -> Vec<std::sync::Arc<tokio::sync::Notify>> {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        outputs
            .entries
            .iter()
            .filter(|entry| {
                !entry.commands.control_frame_admission_is_closed()
                    && entry.published_max_data_offset < outputs.desired_max_data_offset
            })
            .map(|entry| entry.commands.capacity_notify())
            .collect()
    }

    #[cfg(test)]
    pub(in crate::runtime) fn attach(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: TrafficClass,
    ) -> ResponseStreamAttachOutcome {
        // Tests use this argument to establish explicit local sender state.
        // Production attachment admission never accepts a peer-provided lane.
        self.set_lane(lane);
        let key = CarrierPathKey { underlay, path_id };
        let path_instance_id = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .find(|entry| entry.key == key && entry.commands.same_channel(&commands))
            .map_or_else(next_server_carrier_path_instance_id, |entry| {
                entry.path_instance_id
            });
        self.attach_output(ResponseOutputAttachment {
            key,
            path_instance_id,
            local_policy: PathPolicy::default(),
            startup_rate_prior: RateHint::Unknown,
            commands,
            state: ResponseOutputAttachmentState::default(),
        })
    }

    #[cfg(test)]
    pub(in crate::runtime::stream) fn attach_output(
        &self,
        attachment: ResponseOutputAttachment,
    ) -> ResponseStreamAttachOutcome {
        self.try_attach_output(attachment)
            .expect("test response attachment identity space")
    }

    pub(in crate::runtime::stream) fn try_attach_output(
        &self,
        attachment: ResponseOutputAttachment,
    ) -> Result<ResponseStreamAttachOutcome, RuntimeError> {
        self.try_attach_output_with_return_plan(attachment, None)
    }

    pub(super) fn try_attach_output_with_return_plan(
        &self,
        attachment: ResponseOutputAttachment,
        return_plan: Option<crate::protocol::StreamReturnPlan>,
    ) -> Result<ResponseStreamAttachOutcome, RuntimeError> {
        let ResponseOutputAttachment {
            key,
            path_instance_id,
            local_policy,
            startup_rate_prior,
            commands,
            state: attachment_state,
        } = attachment;
        #[cfg(feature = "lab-diagnostics")]
        let CarrierPathKey { underlay, path_id } = key;
        // Enrollment/finalization and output publication share this lock order:
        // startup -> lane -> outputs. A sender can therefore observe neither a
        // published-but-unbound STARTUP output nor a binding without its exact
        // live output.
        let mut startup = return_plan.map(|_| {
            self.response_startup
                .lock()
                .expect("server response startup lock")
        });
        // A new carrier inherits the sender-local live response lane. The
        // peer's immutable OPEN_STREAM hint cannot mutate this direction.
        let current_lane = self.lane.lock().expect("server reliable stream lane lock");
        let lane = *current_lane;
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        // Close snapshots outputs under this lock after publishing the closed
        // flag. An attach either enters that snapshot or observes closure.
        if !self.response_stream_open.load(Ordering::Acquire) {
            return Ok(ResponseStreamAttachOutcome::RejectedClosedStream);
        }
        if outputs
            .detaching
            .iter()
            .any(|entry| entry.key == key && entry.path_instance_id == path_instance_id)
        {
            // The ordered detach still owns this exact carrier incarnation.
            // Refusing reattachment until the actor consumes that boundary
            // prevents authenticated reconnect churn from accumulating
            // detaching entries and one waiter task per retry.
            return Ok(ResponseStreamAttachOutcome::RejectedDuplicateLiveOutput);
        }
        let mut replaced_closed = false;
        let mut replaced_incarnation = None;
        let existing_position = outputs.entries.iter().position(|entry| entry.key == key);
        if let Some(position) = existing_position {
            let entry = &mut outputs.entries[position];
            if !entry.commands.is_closed() {
                let same_channel = entry.commands.same_channel(&commands);
                #[cfg(feature = "lab-diagnostics")]
                let attach_result = match same_channel {
                    true => "same_channel",
                    false => "duplicate_live",
                };
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "server_stream_output_attach",
                    format_args!(
                        "session_id={} path_underlay={:?} path_id={} lane={:?} result={} same_channel={}",
                        self.session_id.0, underlay, path_id.0, lane, attach_result, same_channel,
                    ),
                );
                if same_channel {
                    let exact = ResponseAcquisitionOutputId::from(&*entry);
                    let startup_commit = match (&startup, return_plan) {
                        (Some(startup), Some(plan)) => {
                            Some(startup.prepare_attachment(plan, exact)?)
                        }
                        _ => None,
                    };
                    if let (Some(startup), Some(commit)) = (&mut startup, startup_commit) {
                        startup.commit_attachment(commit);
                    }
                    let evidence_changed = apply_attachment_state(entry, attachment_state);
                    if evidence_changed {
                        self.response_model_generation
                            .fetch_add(1, Ordering::AcqRel);
                    }
                    drop(outputs);
                    drop(current_lane);
                    if evidence_changed {
                        self.notify_update();
                    }
                    return Ok(ResponseStreamAttachOutcome::Attached);
                }
                return Ok(ResponseStreamAttachOutcome::RejectedDuplicateLiveOutput);
            }
        }
        // Validate enrollment against the exact prospective incarnation before
        // mutating either output membership or the non-reusable incarnation
        // counter. The state lock makes the returned commit infallible.
        let prospective_incarnation = outputs
            .next_output_incarnation
            .ok_or(RuntimeError::ExactIdentityExhausted)?;
        let prospective_output = ResponseAcquisitionOutputId {
            key,
            path_instance_id,
            incarnation: prospective_incarnation,
        };
        let startup_commit = match (&startup, return_plan) {
            (Some(startup), Some(plan)) => {
                Some(startup.prepare_attachment(plan, prospective_output)?)
            }
            _ => None,
        };
        // Exact identity allocation is the final fallible step and precedes
        // removal or mutation of a closed predecessor.
        let allocated_incarnation = Self::allocate_output_incarnation(&mut outputs)?;
        debug_assert_eq!(allocated_incarnation, prospective_incarnation);
        let mut entry = if let Some(position) = existing_position {
            let mut entry = outputs.entries.remove(position);
            if entry.commands.is_closed() {
                replaced_incarnation = Some(entry.incarnation);
                entry.path_instance_id = path_instance_id;
                entry.local_policy = local_policy;
                entry.startup_rate_prior = startup_rate_prior;
                entry.incarnation = allocated_incarnation;
                entry.load_registration.deactivate();
                entry.commands = commands;
                entry.load_registration = entry.commands.register_inactive_flow(lane);
                entry.original_data_in_flight_bytes = 0;
                entry.qualification = StreamPathQualification::Qualified;
                // Incarnation is part of the exact owner identity. A closed
                // replacement therefore starts a new ledger rather than
                // reusing or reactivating predecessor authority.
                entry.product_qualification = ProductQualificationLedger::default();
                entry.bytes_in_flight = 0;
                entry.product_rate_epoch = None;
                entry.tcp_product_rate_evidence = None;
                entry.srtt_ms = None;
                entry.delivery_samples = 0;
                entry.original_data_acked_bytes = 0;
                entry.published_max_data_offset = 0;
                entry.ack_publication = StreamAckPublicationCursor::default();
                entry.local_path_metrics = None;
                entry.peer_path_metrics = None;
                entry.native_scheduling_shape = None;
                entry.peer_usage = None;
                entry.peer_usage_sequence = None;
                entry.path_proof = None;
                replaced_closed = true;
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "server_stream_output_attach",
                    format_args!(
                        "session_id={} path_underlay={:?} path_id={} lane={:?} result=replace_closed",
                        self.session_id.0, underlay, path_id.0, lane,
                    ),
                );
            } else {
                #[cfg(feature = "lab-diagnostics")]
                {
                    let same_channel = entry.commands.same_channel(&commands);
                    lab_diagnostic(
                        "server_stream_output_attach",
                        format_args!(
                            "session_id={} path_underlay={:?} path_id={} lane={:?} result=duplicate_live same_channel={}",
                            self.session_id.0, underlay, path_id.0, lane, same_channel,
                        ),
                    );
                }
            }
            entry
        } else {
            let load_registration = commands.register_inactive_flow(lane);
            ResponseStreamOutputEntry {
                key,
                path_instance_id,
                local_policy,
                startup_rate_prior,
                incarnation: allocated_incarnation,
                commands,
                load_registration,
                original_data_in_flight_bytes: 0,
                qualification: StreamPathQualification::Qualified,
                product_qualification: ProductQualificationLedger::default(),
                bytes_in_flight: 0,
                product_rate_epoch: None,
                tcp_product_rate_evidence: None,
                srtt_ms: None,
                delivery_samples: 0,
                original_data_acked_bytes: 0,
                published_max_data_offset: 0,
                ack_publication: StreamAckPublicationCursor::default(),
                local_path_metrics: None,
                peer_path_metrics: None,
                native_scheduling_shape: None,
                peer_usage: None,
                peer_usage_sequence: None,
                path_proof: None,
            }
        };
        let _ = apply_attachment_state(&mut entry, attachment_state);
        outputs.entries.push(entry);
        if let (Some(startup), Some(commit)) = (&mut startup, startup_commit) {
            startup.commit_attachment(commit);
        }
        for output in outputs.entries.iter().chain(&outputs.detaching) {
            output.load_registration.set_lane(lane);
        }
        if let Some(incarnation) = replaced_incarnation {
            self.invalidate_path_flight_evidence(key, incarnation);
        }
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
        self.output_membership_generation
            .fetch_add(1, Ordering::AcqRel);
        drop(outputs);
        drop(current_lane);
        self.notify_update();
        if replaced_closed {
            Ok(ResponseStreamAttachOutcome::ReplacedClosedOutput)
        } else {
            Ok(ResponseStreamAttachOutcome::Attached)
        }
    }

    pub(in crate::runtime) fn lane(&self) -> TrafficClass {
        *self.lane.lock().expect("server reliable stream lane lock")
    }

    pub(in crate::runtime) fn set_lane(&self, lane: TrafficClass) {
        let mut current_lane = self.lane.lock().expect("server reliable stream lane lock");
        let changed = *current_lane != lane;
        if changed {
            *current_lane = lane;
            let outputs = self
                .outputs
                .lock()
                .expect("server reliable stream binding lock");
            for entry in outputs.entries.iter().chain(&outputs.detaching) {
                entry.load_registration.set_lane(lane);
            }
            self.response_model_generation
                .fetch_add(1, Ordering::AcqRel);
        }
        drop(current_lane);
        if changed {
            self.notify_update();
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn detach(
        &self,
        key: CarrierPathKey,
        commands: &ReliablePathCommandSender,
    ) {
        self.detach_matching_output(key, |entry| entry.commands.same_channel(commands));
    }

    /// Withdraws a carrier from placement without yet declaring its flights
    /// failed. The stream actor completes the detach after earlier input.
    pub(in crate::runtime) fn begin_path_detach(
        &self,
        key: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
    ) -> Option<ResponsePathDetachOutcome> {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        if let Some(entry) = outputs
            .detaching
            .iter()
            .find(|entry| entry.key == key && entry.path_instance_id == path_instance_id)
        {
            return Some(ResponsePathDetachOutcome::Pending(entry.incarnation));
        }
        let position = outputs
            .entries
            .iter()
            .position(|entry| entry.key == key && entry.path_instance_id == path_instance_id)?;
        let entry = outputs.entries.remove(position);
        entry.load_registration.deactivate();
        let mut entry = entry;
        entry.product_qualification.revoke();
        let output_incarnation = entry.incarnation;
        outputs.detaching.push(entry);
        self.clear_request_feedback_ingress_if(key, path_instance_id);
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
        self.output_membership_generation
            .fetch_add(1, Ordering::AcqRel);
        drop(outputs);
        self.notify_update();
        Some(ResponsePathDetachOutcome::Begun(output_incarnation))
    }

    pub(in crate::runtime) fn complete_path_detach(
        &self,
        key: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
        output_incarnation: u64,
    ) {
        self.detach_matching_output(key, |entry| {
            entry.path_instance_id == path_instance_id && entry.incarnation == output_incarnation
        });
    }

    #[cfg(test)]
    pub(in crate::runtime) fn detach_path_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
    ) {
        self.detach_matching_output(key, |entry| entry.path_instance_id == path_instance_id);
    }

    fn detach_matching_output(
        &self,
        key: CarrierPathKey,
        matches: impl Fn(&ResponseStreamOutputEntry) -> bool,
    ) {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let live_outputs_before = outputs.entries.len();
        for entry in &mut outputs.entries {
            if entry.key == key && matches(entry) {
                entry.product_qualification.revoke();
            }
        }
        for entry in &mut outputs.detaching {
            if entry.key == key && matches(entry) {
                entry.product_qualification.revoke();
            }
        }
        let mut removed = Vec::new();
        let mut retain = |entry: &ResponseStreamOutputEntry| {
            let remove = entry.key == key && matches(entry);
            if remove {
                entry.load_registration.deactivate();
                removed.push((entry.incarnation, entry.path_instance_id));
            }
            !remove
        };
        outputs.entries.retain(&mut retain);
        let live_membership_changed = outputs.entries.len() != live_outputs_before;
        outputs.detaching.retain(&mut retain);
        if !removed.is_empty() {
            for (incarnation, _) in &removed {
                self.invalidate_path_flight_evidence(key, *incarnation);
            }
            for (_, path_instance_id) in &removed {
                self.clear_request_feedback_ingress_if(key, *path_instance_id);
            }
            self.response_model_generation
                .fetch_add(1, Ordering::AcqRel);
            if live_membership_changed {
                self.output_membership_generation
                    .fetch_add(1, Ordering::AcqRel);
            }
            drop(outputs);
            self.notify_update();
        }
    }

    pub(in crate::runtime) fn request_feedback_underlay(&self) -> Option<UnderlayProtocol> {
        self.request_feedback_ingress
            .lock()
            .expect("server reliable stream request feedback ingress lock")
            .map(|ingress| ingress.key.underlay)
    }

    pub(in crate::runtime) fn request_feedback_path_snapshot(
        &self,
        lane: TrafficClass,
    ) -> Option<PathSnapshot> {
        // The exact ingress incarnation is checked with the output snapshot so
        // a reconnect cannot inherit return-path affinity from an old carrier.
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let ingress = *self
            .request_feedback_ingress
            .lock()
            .expect("server reliable stream request feedback ingress lock");
        ingress.and_then(|ingress| {
            outputs
                .entries
                .iter()
                .find(|entry| {
                    entry.key == ingress.key
                        && entry.path_instance_id == ingress.path_instance_id
                        && !entry.commands.is_closed()
                })
                .map(|entry| {
                    super::snapshot::server_bulk_output_snapshot(
                        entry,
                        outputs.data_level_queue_bytes,
                        lane,
                        self.mux_limits,
                    )
                })
        })
    }

    pub(in crate::runtime) fn record_request_feedback_ingress(
        &self,
        key: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
    ) -> bool {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        if !outputs
            .entries
            .iter()
            .any(|entry| entry.key == key && entry.path_instance_id == path_instance_id)
        {
            return false;
        }
        let observed = super::RequestFeedbackIngress {
            key,
            path_instance_id,
        };
        let mut ingress = self
            .request_feedback_ingress
            .lock()
            .expect("server reliable stream request feedback ingress lock");
        if *ingress == Some(observed) {
            return true;
        }
        *ingress = Some(observed);
        drop(ingress);
        drop(outputs);
        self.notify_update();
        true
    }

    pub(in crate::runtime::stream) fn update_peer_path_usage_for_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
        sequence: u64,
        usage: PathUsage,
    ) -> bool {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        if let Some(entry) = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == key && entry.path_instance_id == path_instance_id)
        {
            if entry
                .peer_usage_sequence
                .is_some_and(|current| sequence <= current)
            {
                return false;
            }
            entry.peer_usage_sequence = Some(sequence);
            entry.peer_usage = Some(usage);
        } else {
            return false;
        }
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
        drop(outputs);
        self.notify_update();
        true
    }

    #[cfg(test)]
    pub(in crate::runtime) fn update_peer_path_usage_for_test(
        &self,
        key: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
        sequence: u64,
        usage: PathUsage,
    ) -> bool {
        self.update_peer_path_usage_for_instance(key, path_instance_id, sequence, usage)
    }

    pub(in crate::runtime::stream) fn mark_path_proof_success_for_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
        observation: PathProofObservation,
    ) -> bool {
        let changed = {
            let mut outputs = self
                .outputs
                .lock()
                .expect("server reliable stream binding lock");
            let Some(entry) = outputs
                .entries
                .iter_mut()
                .find(|entry| entry.key == key && entry.path_instance_id == path_instance_id)
            else {
                return false;
            };
            if entry.path_proof.is_some() {
                false
            } else {
                entry.path_proof = Some(observation);
                self.response_model_generation
                    .fetch_add(1, Ordering::AcqRel);
                true
            }
        };
        if changed {
            self.notify_update();
        }
        changed
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_output_path_proven_for_test(&self, key: CarrierPathKey) {
        let path_instance_id = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| entry.path_instance_id)
            .expect("test output path");
        assert!(self.mark_path_proof_success_for_instance(
            key,
            path_instance_id,
            PathProofObservation {
                proof_id: 1,
                elapsed: std::time::Duration::from_micros(1),
                sent_at: std::time::Instant::now(),
            },
        ));
    }

    pub(in crate::runtime) fn has_output_incarnation(
        &self,
        key: CarrierPathKey,
        incarnation: u64,
    ) -> bool {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        outputs
            .entries
            .iter()
            .chain(outputs.detaching.iter())
            .any(|entry| entry.key == key && entry.incarnation == incarnation)
    }

    pub(in crate::runtime) fn try_enqueue_classified_frame_for_target(
        &self,
        target: &ResponseDispatchTarget,
        frame: Frame,
        lane: TrafficClass,
    ) -> Result<(), RuntimeError> {
        self.try_enqueue_frame_for_target(target, frame, lane, false)
    }

    pub(in crate::runtime) fn try_enqueue_stream_ordered_frame_for_target(
        &self,
        target: &ResponseDispatchTarget,
        frame: Frame,
        lane: TrafficClass,
    ) -> Result<(), RuntimeError> {
        self.try_enqueue_frame_for_target(target, frame, lane, true)
    }

    fn try_enqueue_frame_for_target(
        &self,
        target: &ResponseDispatchTarget,
        frame: Frame,
        lane: TrafficClass,
        stream_ordered: bool,
    ) -> Result<(), RuntimeError> {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let Some(entry) = outputs.entries.iter().find(|entry| {
            entry.key == target.key
                && entry.path_instance_id == target.path_instance_id
                && entry.incarnation == target.incarnation
                && !entry.commands.is_closed()
        }) else {
            return Err(RuntimeError::SenderServiceBlocked);
        };
        if stream_ordered {
            entry.commands.try_enqueue_stream_ordered_frame(frame, lane)
        } else {
            entry.commands.try_enqueue_admitted_frame(frame, lane)
        }
    }

    pub(super) fn clear_request_feedback_ingress_if(
        &self,
        key: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
    ) {
        let mut ingress = self
            .request_feedback_ingress
            .lock()
            .expect("server reliable stream request feedback ingress lock");
        if ingress.is_some_and(|ingress| {
            ingress.key == key && ingress.path_instance_id == path_instance_id
        }) {
            *ingress = None;
        }
    }
}

/// Handle-free response candidate captured by one observe pass.
#[derive(Clone)]
pub(in crate::runtime) struct ResponseSenderPathTarget {
    pub(in crate::runtime) observation: ResponsePathObservation,
    pub(in crate::runtime) native_authority_stamp: Option<CarrierRateAuthorityStamp>,
    pub(in crate::runtime) product_admission_active: bool,
    pub(in crate::runtime) command_queue: ReliablePathCommandQueueSnapshot,
}

impl AsRef<ResponsePathObservation> for ResponseSenderPathTarget {
    fn as_ref(&self) -> &ResponsePathObservation {
        &self.observation
    }
}

impl ResponseSenderPathTarget {
    pub(in crate::runtime) fn can_enqueue_frame(
        &self,
        frame: &crate::protocol::Frame,
        lane: TrafficClass,
    ) -> bool {
        self.command_queue.can_enqueue_frame(frame, lane)
    }

    pub(in crate::runtime) fn can_enqueue_stream_ordered_frame(&self) -> bool {
        self.command_queue.can_enqueue_stream_ordered_frame()
    }

    pub(in crate::runtime) fn can_enqueue_stream_data(&self, lane: TrafficClass) -> bool {
        self.command_queue.can_enqueue_lane(lane)
    }

    pub(in crate::runtime) fn can_enqueue_reinjection_frame(
        &self,
        frame: &crate::protocol::Frame,
    ) -> bool {
        self.command_queue.can_enqueue_reinjection_frame(frame)
    }
}

/// ID-only apply target retained after ranking. The binding resolves the exact
/// live command port under its output lock before committing any mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ResponseDispatchTarget {
    pub(in crate::runtime) key: CarrierPathKey,
    pub(in crate::runtime) path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime) incarnation: u64,
    pub(in crate::runtime) has_bulk_rate_evidence: bool,
    pub(in crate::runtime) native_authority_stamp: Option<CarrierRateAuthorityStamp>,
}

/// Exact response-output identity used only by direction-local acquisition.
///
/// Logical path keys can be reused after a carrier reconnect, and attachment
/// incarnations can be replaced on one carrier. All three components are
/// therefore part of cursor authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ResponseAcquisitionOutputId {
    pub(in crate::runtime) key: CarrierPathKey,
    pub(in crate::runtime) path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime) incarnation: u64,
}

impl From<&ResponseStreamOutputEntry> for ResponseAcquisitionOutputId {
    fn from(entry: &ResponseStreamOutputEntry) -> Self {
        Self {
            key: entry.key,
            path_instance_id: entry.path_instance_id,
            incarnation: entry.incarnation,
        }
    }
}

impl From<&ResponseSenderPathTarget> for ResponseAcquisitionOutputId {
    fn from(target: &ResponseSenderPathTarget) -> Self {
        Self {
            key: target.observation.key,
            path_instance_id: target.observation.path_instance_id,
            incarnation: target.observation.incarnation,
        }
    }
}

impl From<ResponseSenderPathTarget> for ResponseDispatchTarget {
    fn from(target: ResponseSenderPathTarget) -> Self {
        Self {
            key: target.observation.key,
            path_instance_id: target.observation.path_instance_id,
            incarnation: target.observation.incarnation,
            has_bulk_rate_evidence: target.observation.has_bulk_rate_evidence,
            native_authority_stamp: target.native_authority_stamp,
        }
    }
}

impl From<&ResponseSenderPathTarget> for ResponseDispatchTarget {
    fn from(target: &ResponseSenderPathTarget) -> Self {
        Self {
            key: target.observation.key,
            path_instance_id: target.observation.path_instance_id,
            incarnation: target.observation.incarnation,
            has_bulk_rate_evidence: target.observation.has_bulk_rate_evidence,
            native_authority_stamp: target.native_authority_stamp,
        }
    }
}

#[cfg(test)]
#[path = "tests_attachment.rs"]
mod tests;
