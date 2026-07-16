//! Response path attachment and identity transactions.
//! It linearizes membership changes; ranking and carrier recovery stay outside it.

use super::ResponseStreamBinding;
use super::ack_clock::ResponseAckClockRateEvidence;
use super::evidence::ServerPathMetricsEntry;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
#[cfg(test)]
use crate::model::path::next_carrier_path_instance_id;
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey, PathPolicy};
use crate::model::response::ResponsePathObservation;
use crate::protocol::{Frame, PathId, PathUsage, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::{ReliablePathCommandQueueSnapshot, ReliablePathCommandSender};
use crate::runtime::path::proof::enqueue_path_proof_frame;
use crate::scheduler::{PathSnapshot, TrafficClass};
use std::sync::atomic::Ordering;

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

/// One carrier output attached to a response stream.
///
/// It owns carrier command access and sender-evidence fields for this stream on
/// this path. Product reinjection and ordering identity stay in `ResponseStreamBinding`.
#[cfg_attr(test, derive(Clone))]
pub(in crate::runtime) struct ResponseStreamOutputEntry {
    pub(super) key: CarrierPathKey,
    pub(super) path_instance_id: CarrierPathInstanceId,
    pub(super) local_policy: PathPolicy,
    pub(super) incarnation: u64,
    pub(super) commands: ReliablePathCommandSender,
    /// Unacknowledged unique OriginalData assigned to this response output.
    /// Reinjection copies remain in `bytes_in_flight` but never enter this counter.
    pub(super) original_data_in_flight_bytes: u64,
    pub(super) bytes_in_flight: u64,
    pub(super) product_progress_rate_bps: Option<f64>,
    pub(super) delivery_rate_bps: Option<f64>,
    /// TCP per-flow goodput from exact OriginalData ACKs. It is not carrier
    /// capacity; assignment-time evidence never publishes a rate or RTT.
    pub(super) tcp_ack_clock_rate_bps: Option<f64>,
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
    pub(super) local_path_metrics: Option<ServerPathMetricsEntry>,
    pub(super) peer_path_metrics: Option<ServerPathMetricsEntry>,
    /// Latest directional preference advertised by the peer for this exact
    /// carrier instance. It is independent of local path health.
    pub(super) peer_usage: Option<PathUsage>,
    pub(super) peer_usage_sequence: Option<u64>,
}

pub(in crate::runtime) struct ResponseStreamOutputs {
    pub(super) entries: Vec<ResponseStreamOutputEntry>,
    /// Offset-free sender-service staging belongs to the response stream, not
    /// to any carrier output.
    pub(super) data_level_queue_bytes: u64,
}

impl ResponseStreamBinding {
    fn allocate_output_incarnation(&self) -> u64 {
        self.next_output_incarnation.fetch_add(1, Ordering::AcqRel)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn attach(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: TrafficClass,
    ) -> ResponseStreamAttachOutcome {
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
        self.attach_with_path_instance(
            underlay,
            path_id,
            path_instance_id,
            PathPolicy::default(),
            commands,
            lane,
        )
    }

    pub(in crate::runtime::stream) fn attach_with_path_instance(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
        path_instance_id: CarrierPathInstanceId,
        local_policy: PathPolicy,
        commands: ReliablePathCommandSender,
        lane: TrafficClass,
    ) -> ResponseStreamAttachOutcome {
        let mut current_lane = self.lane.lock().expect("server reliable stream lane lock");
        let proof_commands = commands.clone();
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        // Close snapshots outputs under this lock after publishing the closed
        // flag. An attach either enters that snapshot or observes closure.
        if !self.response_stream_open.load(Ordering::Acquire) {
            return ResponseStreamAttachOutcome::RejectedClosedStream;
        }
        let key = CarrierPathKey { underlay, path_id };
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
                    let lane_changed = *current_lane != lane;
                    *current_lane = lane;
                    if lane_changed {
                        self.response_model_generation
                            .fetch_add(1, Ordering::AcqRel);
                    }
                    drop(outputs);
                    drop(current_lane);
                    if lane_changed {
                        self.notify_update();
                    }
                    return ResponseStreamAttachOutcome::Attached;
                }
                return ResponseStreamAttachOutcome::RejectedDuplicateLiveOutput;
            }
        }
        let entry = if let Some(position) = existing_position {
            let mut entry = outputs.entries.remove(position);
            if entry.commands.is_closed() {
                replaced_incarnation = Some(entry.incarnation);
                entry.path_instance_id = path_instance_id;
                entry.local_policy = local_policy;
                entry.incarnation = self.allocate_output_incarnation();
                entry.commands = commands;
                entry.original_data_in_flight_bytes = 0;
                entry.bytes_in_flight = 0;
                entry.product_progress_rate_bps = None;
                entry.delivery_rate_bps = None;
                entry.tcp_ack_clock_rate_bps = None;
                entry.tcp_product_rate_evidence = None;
                entry.srtt_ms = None;
                entry.delivery_samples = 0;
                entry.original_data_acked_bytes = 0;
                entry.local_path_metrics = None;
                entry.peer_path_metrics = None;
                entry.peer_usage = None;
                entry.peer_usage_sequence = None;
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
            ResponseStreamOutputEntry {
                key,
                path_instance_id,
                local_policy,
                incarnation: self.allocate_output_incarnation(),
                commands,
                original_data_in_flight_bytes: 0,
                bytes_in_flight: 0,
                product_progress_rate_bps: None,
                delivery_rate_bps: None,
                tcp_ack_clock_rate_bps: None,
                tcp_product_rate_evidence: None,
                srtt_ms: None,
                delivery_samples: 0,
                original_data_acked_bytes: 0,
                local_path_metrics: None,
                peer_path_metrics: None,
                peer_usage: None,
                peer_usage_sequence: None,
            }
        };
        outputs.entries.push(entry);
        if let Some(incarnation) = replaced_incarnation {
            self.invalidate_path_flight_evidence(key, incarnation);
        }
        *current_lane = lane;
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
        drop(outputs);
        drop(current_lane);
        // Path proof is independent of why the product stream attached.
        let _ = enqueue_path_proof_frame(&proof_commands, path_id, self.mux_limits);
        self.notify_update();
        if replaced_closed {
            ResponseStreamAttachOutcome::ReplacedClosedOutput
        } else {
            ResponseStreamAttachOutcome::Attached
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
            self.response_model_generation
                .fetch_add(1, Ordering::AcqRel);
        }
        drop(current_lane);
        if changed {
            self.notify_update();
        }
    }

    pub(in crate::runtime) fn detach(
        &self,
        key: CarrierPathKey,
        commands: &ReliablePathCommandSender,
    ) {
        self.detach_matching_output(key, |entry| entry.commands.same_channel(commands));
    }

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
        let removed = outputs
            .entries
            .iter()
            .find(|entry| entry.key == key && matches(entry))
            .map(|entry| (entry.incarnation, entry.path_instance_id));
        outputs
            .entries
            .retain(|entry| entry.key != key || !matches(entry));
        if let Some((incarnation, path_instance_id)) = removed {
            self.invalidate_path_flight_evidence(key, incarnation);
            self.clear_request_feedback_ingress_if(key, path_instance_id);
            self.response_model_generation
                .fetch_add(1, Ordering::AcqRel);
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
        let Some(entry) = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == key && entry.path_instance_id == path_instance_id)
        else {
            return false;
        };
        if entry
            .peer_usage_sequence
            .is_some_and(|current| sequence <= current)
        {
            return false;
        }
        entry.peer_usage_sequence = Some(sequence);
        entry.peer_usage = Some(usage);
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
        drop(outputs);
        self.notify_update();
        true
    }

    pub(in crate::runtime) fn has_output_incarnation(
        &self,
        key: CarrierPathKey,
        incarnation: u64,
    ) -> bool {
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
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

    fn clear_request_feedback_ingress_if(
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
}

/// ID-only apply target retained after ranking. The binding resolves the exact
/// live command port under its output lock before committing any mutation.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ResponseDispatchTarget {
    pub(in crate::runtime) key: CarrierPathKey,
    pub(in crate::runtime) path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime) incarnation: u64,
    pub(in crate::runtime) has_bulk_rate_evidence: bool,
}

impl From<ResponseSenderPathTarget> for ResponseDispatchTarget {
    fn from(target: ResponseSenderPathTarget) -> Self {
        Self {
            key: target.observation.key,
            path_instance_id: target.observation.path_instance_id,
            incarnation: target.observation.incarnation,
            has_bulk_rate_evidence: target.observation.has_bulk_rate_evidence,
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
        }
    }
}

#[cfg(test)]
#[path = "attachment_test.rs"]
mod tests;
