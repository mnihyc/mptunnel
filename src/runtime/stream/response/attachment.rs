//! Response attachment, role, lane, and persistent Service ownership.
//! It linearizes output-set changes; ranking and carrier recovery stay outside it.

use super::ResponseStreamBinding;
use super::ack_clock::{ResponseAckClockCalibrationState, ResponseAckClockRateEvidence};
use super::evidence::ServerPathMetricsEntry;
use super::session::TcpCapacityProbeSessionLease;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::QuicCapacityProofCandidate;
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey, next_carrier_path_instance_id};
use crate::model::response::ResponsePathObservation;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::SessionId;
use crate::protocol::{Frame, PathId, StreamOpenRole, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::CarrierCommandLease;
use crate::runtime::path::commands::{
    ReliablePathCommandQueueSnapshot, ReliablePathCommandSender, TcpCapacityProbeRequest,
};
use crate::runtime::path::proof::enqueue_path_proof_frame;
use crate::scheduler::{FlowLane, PathSnapshot};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
const RESPONSE_OWNER_TCP_SEEN: u8 = 1 << 0;
const RESPONSE_OWNER_UDP_SEEN: u8 = 1 << 1;
const RESPONSE_OWNER_MIXED_SEEN: u8 = RESPONSE_OWNER_TCP_SEEN | RESPONSE_OWNER_UDP_SEEN;

pub(super) fn response_owner_underlay_seen_bit(underlay: UnderlayProtocol) -> u8 {
    match underlay {
        UnderlayProtocol::Tcp => RESPONSE_OWNER_TCP_SEEN,
        UnderlayProtocol::Udp => RESPONSE_OWNER_UDP_SEEN,
    }
}

pub(in crate::runtime) fn next_server_carrier_path_instance_id() -> CarrierPathInstanceId {
    next_carrier_path_instance_id()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ResponseStreamAttachOutcome {
    Attached,
    RoleChanged,
    ReplacedClosedOutput,
    RejectedDuplicateLiveOutput,
    RejectedClosedStream,
}

/// One carrier output attached to a response stream.
///
/// It owns carrier command access and sender-evidence fields for this stream on
/// this path. Product repair and ordering identity stay in `ResponseStreamBinding`.
#[derive(Clone)]
pub(in crate::runtime) struct ResponseStreamOutputEntry {
    pub(super) key: CarrierPathKey,
    pub(super) path_instance_id: CarrierPathInstanceId,
    pub(super) incarnation: u64,
    pub(super) commands: ReliablePathCommandSender,
    pub(super) role: StreamOpenRole,
    /// Unacknowledged unique OwnerData assigned to this response output.
    /// Repair copies remain in `bytes_in_flight` but never enter this counter.
    pub(super) owner_data_in_flight_bytes: u64,
    pub(super) bytes_in_flight: u64,
    pub(super) product_queue_bytes: u64,
    pub(super) product_progress_rate_bps: Option<f64>,
    pub(super) delivery_rate_bps: Option<f64>,
    /// TCP per-flow goodput from exact OwnerData ACKs. It is not carrier
    /// capacity; assignment-time evidence never publishes a rate or RTT.
    pub(super) tcp_ack_clock_rate_bps: Option<f64>,
    /// Per-output ACK clock; product ordering timestamps can be advanced when a
    /// different path closes a hole and therefore cannot own this boundary.
    pub(super) tcp_product_rate_evidence: Option<ResponseAckClockRateEvidence>,
    /// Temporary carrier-capacity estimate. It may come from a bounded Service
    /// opportunity or exclusive calibration; ordinary exact-ACK evidence must
    /// mature in a separate epoch before replacing it.
    pub(super) tcp_capacity_prior: Option<TcpResponseCapacityPrior>,
    pub(super) srtt_ms: Option<f64>,
    pub(super) delivery_samples: u32,
    /// Cumulative uniquely owned product bytes ACKed on this output.
    ///
    /// The flight ledger increments this only for unambiguous `OwnerData`;
    /// duplicated `RepairData` never contributes.
    pub(super) owner_data_acked_bytes: u64,
    pub(super) local_path_metrics: Option<ServerPathMetricsEntry>,
    pub(super) peer_path_metrics: Option<ServerPathMetricsEntry>,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct TcpResponseCapacityPrior {
    pub(super) rate_bps: f64,
    pub(super) ordinary_windows: u32,
}

pub(in crate::runtime) struct ResponseStreamOutputs {
    pub(super) entries: Vec<ResponseStreamOutputEntry>,
    pub(super) ack_clock_calibrations:
        HashMap<(CarrierPathKey, u64), ResponseAckClockCalibrationState>,
    pub(super) active_ack_clock_calibration: Option<(CarrierPathKey, u64)>,
}

impl ResponseStreamBinding {
    fn allocate_output_incarnation(&self) -> u64 {
        self.next_output_incarnation.fetch_add(1, Ordering::AcqRel)
    }

    fn response_flow_is_active(outputs: &ResponseStreamOutputs) -> bool {
        outputs
            .entries
            .iter()
            .any(|entry| response_stream_role_reserves_flow_load(entry.role))
    }

    fn sync_response_flow_activity(&self, outputs: &ResponseStreamOutputs) {
        // Deactivation calls this before path-load removal; activation calls it
        // after path-load registration so every visible generation is conservative.
        self.response_flow_registration
            .set_active(Self::response_flow_is_active(outputs));
    }

    #[cfg(test)]
    pub(in crate::runtime) fn attach(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: FlowLane,
        role: StreamOpenRole,
        max_frame_payload_bytes: usize,
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
            commands,
            lane,
            role,
            max_frame_payload_bytes,
        )
    }

    pub(in crate::runtime::stream) fn attach_with_path_instance(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
        path_instance_id: CarrierPathInstanceId,
        commands: ReliablePathCommandSender,
        lane: FlowLane,
        role: StreamOpenRole,
        _max_frame_payload_bytes: usize,
    ) -> ResponseStreamAttachOutcome {
        let mut current_lane = self.lane.lock().expect("server reliable stream lane lock");
        let previous_lane = *current_lane;
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
        let response_flow_was_active = Self::response_flow_is_active(&outputs);
        let key = CarrierPathKey { underlay, path_id };
        let mut was_active = false;
        let mut previous_load_registered = false;
        let mut replaced_closed = false;
        let mut replaced_incarnation = None;
        let mut replaced_path_instance_id = None;
        let existing_position = outputs.entries.iter().position(|entry| entry.key == key);
        if let Some(position) = existing_position {
            let entry = &mut outputs.entries[position];
            if !entry.commands.is_closed() {
                let same_channel = entry.commands.same_channel(&commands);
                #[cfg(feature = "lab-diagnostics")]
                let attach_result = match same_channel {
                    true => "same_channel_role_update",
                    false => "duplicate_live",
                };
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "server_stream_output_attach",
                    format_args!(
                        "session_id={} path_underlay={:?} path_id={} role={:?} lane={:?} result={} same_channel={}",
                        self.session_id.0,
                        underlay,
                        path_id.0,
                        role,
                        lane,
                        attach_result,
                        same_channel,
                    ),
                );
                if same_channel {
                    let previous_role = entry.role;
                    let previous_load_registered =
                        response_stream_role_reserves_flow_load(previous_role);
                    entry.role = response_stream_live_role_update(entry.role, role);
                    let role_changed = entry.role != previous_role;
                    let response_state_invalidated =
                        response_stream_role_change_invalidates_response_state(
                            previous_role,
                            entry.role,
                        );
                    let response_state_incarnations = response_state_invalidated.then(|| {
                        let previous = entry.incarnation;
                        let current = self.allocate_output_incarnation();
                        (previous, current)
                    });
                    if response_state_invalidated {
                        entry.incarnation = response_state_incarnations
                            .expect("response eligibility change allocates an incarnation")
                            .1;
                        entry.product_progress_rate_bps = None;
                        entry.delivery_rate_bps = None;
                        entry.tcp_ack_clock_rate_bps = None;
                        entry.tcp_product_rate_evidence = None;
                        entry.tcp_capacity_prior = None;
                        entry.srtt_ms = None;
                        entry.delivery_samples = 0;
                        entry.owner_data_acked_bytes = 0;
                        entry.local_path_metrics = None;
                        entry.peer_path_metrics = None;
                    }
                    let updated_role = entry.role;
                    let updated_load_registered =
                        response_stream_role_reserves_flow_load(updated_role);
                    let lane_registered_keys = outputs
                        .entries
                        .iter()
                        .filter(|entry| {
                            response_stream_role_reserves_flow_load(entry.role)
                                && (entry.key != key || previous_load_registered)
                        })
                        .map(|entry| entry.key)
                        .collect::<Vec<_>>();
                    let response_flow_is_active = Self::response_flow_is_active(&outputs);
                    if response_flow_was_active && !response_flow_is_active {
                        self.sync_response_flow_activity(&outputs);
                    }
                    if let Some((previous_incarnation, current_incarnation)) =
                        response_state_incarnations
                    {
                        outputs
                            .ack_clock_calibrations
                            .remove(&(key, previous_incarnation));
                        if outputs.active_ack_clock_calibration == Some((key, previous_incarnation))
                        {
                            outputs.active_ack_clock_calibration = None;
                        }
                        self.rebind_path_flights_after_live_role_change(
                            key,
                            previous_incarnation,
                            current_incarnation,
                        );
                    }
                    if role_changed && updated_role != StreamOpenRole::Active {
                        self.clear_ordered_data_owner_if(key);
                    }
                    if previous_load_registered && !updated_load_registered {
                        self.lane_tracker
                            .detach(self.session_id, key, previous_lane);
                    }
                    *current_lane = lane;
                    if previous_lane != lane {
                        self.lane_tracker.change_lanes(
                            self.session_id,
                            &lane_registered_keys,
                            previous_lane,
                            lane,
                        );
                    }
                    if !previous_load_registered && updated_load_registered {
                        self.lane_tracker.attach(self.session_id, key, lane);
                    }
                    if !response_flow_was_active && response_flow_is_active {
                        self.sync_response_flow_activity(&outputs);
                    }
                    if response_state_invalidated {
                        // Crossing Repair changes response ownership
                        // eligibility, so publish the role and reset Subflow
                        // identities at one outputs-lock linearization point.
                        self.reset_subflow_set_with_outputs(&mut outputs);
                    }
                    if updated_role != StreamOpenRole::Repair {
                        self.owner_underlay_history
                            .fetch_or(response_owner_underlay_seen_bit(underlay), Ordering::AcqRel);
                    }
                    if role == StreamOpenRole::Active {
                        self.set_request_active_owner(key);
                    }
                    drop(outputs);
                    drop(current_lane);
                    self.notify_update();
                    return if role_changed {
                        ResponseStreamAttachOutcome::RoleChanged
                    } else {
                        ResponseStreamAttachOutcome::Attached
                    };
                }
                return ResponseStreamAttachOutcome::RejectedDuplicateLiveOutput;
            }
        }
        let entry = if let Some(position) = existing_position {
            was_active = position + 1 == outputs.entries.len();
            let mut entry = outputs.entries.remove(position);
            if entry.commands.is_closed() {
                previous_load_registered = response_stream_role_reserves_flow_load(entry.role);
                replaced_incarnation = Some(entry.incarnation);
                replaced_path_instance_id = Some(entry.path_instance_id);
                entry.path_instance_id = path_instance_id;
                entry.incarnation = self.allocate_output_incarnation();
                entry.commands = commands;
                entry.role = role;
                entry.owner_data_in_flight_bytes = 0;
                entry.bytes_in_flight = 0;
                entry.product_queue_bytes = 0;
                entry.product_progress_rate_bps = None;
                entry.delivery_rate_bps = None;
                entry.tcp_ack_clock_rate_bps = None;
                entry.tcp_product_rate_evidence = None;
                entry.tcp_capacity_prior = None;
                entry.srtt_ms = None;
                entry.delivery_samples = 0;
                entry.owner_data_acked_bytes = 0;
                entry.local_path_metrics = None;
                entry.peer_path_metrics = None;
                replaced_closed = true;
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "server_stream_output_attach",
                    format_args!(
                        "session_id={} path_underlay={:?} path_id={} role={:?} lane={:?} result=replace_closed",
                        self.session_id.0, underlay, path_id.0, role, lane,
                    ),
                );
            } else {
                #[cfg(feature = "lab-diagnostics")]
                {
                    let same_channel = entry.commands.same_channel(&commands);
                    lab_diagnostic(
                        "server_stream_output_attach",
                        format_args!(
                            "session_id={} path_underlay={:?} path_id={} role={:?} lane={:?} result=duplicate_live same_channel={}",
                            self.session_id.0, underlay, path_id.0, role, lane, same_channel,
                        ),
                    );
                }
            }
            entry
        } else {
            ResponseStreamOutputEntry {
                key,
                path_instance_id,
                incarnation: self.allocate_output_incarnation(),
                commands,
                role,
                owner_data_in_flight_bytes: 0,
                bytes_in_flight: 0,
                product_queue_bytes: 0,
                product_progress_rate_bps: None,
                delivery_rate_bps: None,
                tcp_ack_clock_rate_bps: None,
                tcp_product_rate_evidence: None,
                tcp_capacity_prior: None,
                srtt_ms: None,
                delivery_samples: 0,
                owner_data_acked_bytes: 0,
                local_path_metrics: None,
                peer_path_metrics: None,
            }
        };
        let promote_or_keep_active_slot = was_active || outputs.entries.is_empty();
        if promote_or_keep_active_slot {
            outputs.entries.push(entry);
        } else {
            let insert_at = outputs.entries.len().saturating_sub(1);
            outputs.entries.insert(insert_at, entry);
        }
        let updated_load_registered = response_stream_role_reserves_flow_load(role);
        let lane_registered_keys = outputs
            .entries
            .iter()
            .filter(|entry| {
                response_stream_role_reserves_flow_load(entry.role)
                    && (entry.key != key || previous_load_registered)
            })
            .map(|entry| entry.key)
            .collect::<Vec<_>>();
        let response_flow_is_active = Self::response_flow_is_active(&outputs);
        if response_flow_was_active && !response_flow_is_active {
            self.sync_response_flow_activity(&outputs);
        }
        if let Some(incarnation) = replaced_incarnation {
            outputs.ack_clock_calibrations.remove(&(key, incarnation));
            if outputs.active_ack_clock_calibration == Some((key, incarnation)) {
                outputs.active_ack_clock_calibration = None;
            }
            self.invalidate_path_flight_evidence(key, incarnation);
        }
        if let Some(replaced_path_instance_id) = replaced_path_instance_id {
            // Output replacement retires this binding's queue, not the shared
            // carrier instance. Only registry path-registration drop may reset
            // an exact carrier's bounded attempt count.
            self.lane_tracker.clear_quic_capacity_calibration(
                self.session_id,
                self.binding_instance_id,
                key,
                replaced_path_instance_id,
            );
            self.lane_tracker
                .clear_response_service_handoff_drain_for_path(
                    self.session_id,
                    self.binding_instance_id,
                    key,
                    replaced_path_instance_id,
                );
        }
        if replaced_closed && role != StreamOpenRole::Active {
            self.clear_ordered_data_owner_if(key);
        }
        if previous_load_registered && !updated_load_registered {
            self.lane_tracker
                .detach(self.session_id, key, previous_lane);
        }
        *current_lane = lane;
        if previous_lane != lane {
            self.lane_tracker.change_lanes(
                self.session_id,
                &lane_registered_keys,
                previous_lane,
                lane,
            );
        }
        if !previous_load_registered && updated_load_registered {
            self.lane_tracker.attach(self.session_id, key, lane);
        }
        if !response_flow_was_active && response_flow_is_active {
            self.sync_response_flow_activity(&outputs);
        }
        // A planner may snapshot the old generation before blocking on outputs,
        // but it cannot observe new membership before this invalidation completes.
        // Passive growth does not recreate cumulative startup sampling credit.
        if replaced_closed || role == StreamOpenRole::Active {
            self.reset_subflow_set_with_outputs(&mut outputs);
        } else {
            self.invalidate_subflow_plan();
        }
        if role != StreamOpenRole::Repair {
            self.owner_underlay_history
                .fetch_or(response_owner_underlay_seen_bit(underlay), Ordering::AcqRel);
        }
        if replaced_closed && role != StreamOpenRole::Active {
            self.clear_request_active_owner_if(key);
        } else if role == StreamOpenRole::Active {
            self.set_request_active_owner(key);
        }
        drop(outputs);
        drop(current_lane);
        if role == StreamOpenRole::Validation {
            let _ = enqueue_path_proof_frame(&proof_commands, path_id, self.mux_limits);
        }
        self.notify_update();
        if replaced_closed {
            ResponseStreamAttachOutcome::ReplacedClosedOutput
        } else {
            ResponseStreamAttachOutcome::Attached
        }
    }

    pub(in crate::runtime) fn lane(&self) -> FlowLane {
        *self.lane.lock().expect("server reliable stream lane lock")
    }

    pub(in crate::runtime) fn set_lane(&self, lane: FlowLane) {
        let mut current_lane = self.lane.lock().expect("server reliable stream lane lock");
        let previous_lane = *current_lane;
        if previous_lane != lane {
            let outputs = self
                .outputs
                .lock()
                .expect("server reliable stream binding lock");
            let attached_keys = outputs
                .entries
                .iter()
                .filter(|entry| response_stream_role_reserves_flow_load(entry.role))
                .map(|entry| entry.key)
                .collect::<Vec<_>>();
            *current_lane = lane;
            self.lane_tracker
                .change_lanes(self.session_id, &attached_keys, previous_lane, lane);
            self.response_flow_registration
                .change_lane_if_present(previous_lane, lane);
            drop(outputs);
        }
        drop(current_lane);
        self.notify_update();
    }

    #[cfg(test)]
    pub(in crate::runtime) fn has_live_mixed_owner_underlays(&self) -> bool {
        if !self.may_have_mixed_owner_underlays() {
            return false;
        }
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        response_outputs_have_live_mixed_owner_underlays(&outputs.entries)
    }

    pub(in crate::runtime) fn may_have_mixed_owner_underlays(&self) -> bool {
        self.owner_underlay_history.load(Ordering::Acquire) & RESPONSE_OWNER_MIXED_SEEN
            == RESPONSE_OWNER_MIXED_SEEN
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
        let current_lane = self.lane.lock().expect("server reliable stream lane lock");
        let lane = *current_lane;
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let response_flow_was_active = Self::response_flow_is_active(&outputs);
        let removed = outputs
            .entries
            .iter()
            .find(|entry| entry.key == key && matches(entry))
            .map(|entry| {
                (
                    entry.incarnation,
                    entry.path_instance_id,
                    response_stream_role_reserves_flow_load(entry.role),
                )
            });
        outputs
            .entries
            .retain(|entry| entry.key != key || !matches(entry));
        if let Some((incarnation, path_instance_id, load_registered)) = removed {
            if response_flow_was_active && !Self::response_flow_is_active(&outputs) {
                self.sync_response_flow_activity(&outputs);
            }
            self.invalidate_path_flight_evidence(key, incarnation);
            outputs.ack_clock_calibrations.remove(&(key, incarnation));
            if outputs.active_ack_clock_calibration == Some((key, incarnation)) {
                outputs.active_ack_clock_calibration = None;
            }
            if load_registered {
                self.lane_tracker.detach(self.session_id, key, lane);
            }
            self.lane_tracker.clear_quic_capacity_calibration(
                self.session_id,
                self.binding_instance_id,
                key,
                path_instance_id,
            );
            self.lane_tracker
                .clear_response_service_handoff_drain_for_path(
                    self.session_id,
                    self.binding_instance_id,
                    key,
                    path_instance_id,
                );
            self.repair_ordered_data_owner_after_output_change(&outputs.entries);
            self.reset_subflow_set_with_outputs(&mut outputs);
            self.clear_request_active_owner_if(key);
            drop(outputs);
            drop(current_lane);
            self.notify_update();
        }
    }

    pub(in crate::runtime) fn ordered_data_owner(&self) -> Option<CarrierPathKey> {
        *self
            .ordered_data_owner
            .lock()
            .expect("server reliable stream ordered data owner lock")
    }

    #[cfg(test)]
    pub(in crate::runtime) fn request_active_owner(&self) -> Option<CarrierPathKey> {
        *self
            .request_active_owner
            .lock()
            .expect("server reliable stream request active owner lock")
    }

    pub(in crate::runtime) fn request_active_underlay(&self) -> Option<UnderlayProtocol> {
        self.request_active_owner
            .lock()
            .expect("server reliable stream request active owner lock")
            .map(|key| key.underlay)
    }

    pub(in crate::runtime) fn request_active_path_snapshot(
        &self,
        lane: FlowLane,
    ) -> Option<PathSnapshot> {
        // Attach and detach take these locks in this order before changing the
        // request-side Active identity. Keep the identity and its metrics in a
        // single coherent snapshot without reversing that order.
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let active_key = *self
            .request_active_owner
            .lock()
            .expect("server reliable stream request active owner lock");
        active_key.and_then(|key| {
            outputs.snapshot_for_key(
                key,
                self.session_id,
                &self.lane_tracker,
                lane,
                self.mux_limits,
            )
        })
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

    pub(in crate::runtime) fn try_enqueue_tcp_capacity_probe_for_target(
        &self,
        target: &ResponseDispatchTarget,
        expected_pending_bytes: u64,
        request: TcpCapacityProbeRequest,
        session_lease: TcpCapacityProbeSessionLease,
    ) -> Result<u64, RuntimeError> {
        if !self.response_stream_open.load(Ordering::Acquire)
            || request.path_id != target.key.path_id
            || request.path_instance_id != target.path_instance_id
        {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let Some(entry) = outputs.entries.iter().find(|entry| {
            entry.key == target.key
                && entry.path_instance_id == target.path_instance_id
                && entry.incarnation == target.incarnation
                && entry.role == target.attachment_role
                && entry.role == StreamOpenRole::Validation
                && entry.key.underlay == UnderlayProtocol::Tcp
                && !entry.commands.is_closed()
                && entry.commands.pending_bytes() == expected_pending_bytes
                && !entry.commands.tcp_capacity_probe_attempted()
                && !entry.commands.tcp_capacity_probe_active()
        }) else {
            return Err(RuntimeError::SenderServiceBlocked);
        };
        entry
            .commands
            .try_enqueue_tcp_capacity_probe(request, CarrierCommandLease::hold(session_lease))
    }

    pub(in crate::runtime) fn try_enqueue_classified_frame_for_target(
        &self,
        target: &ResponseDispatchTarget,
        frame: Frame,
        lane: FlowLane,
    ) -> Result<(), RuntimeError> {
        self.try_enqueue_frame_for_target(target, frame, lane, false)
    }

    pub(in crate::runtime) fn try_enqueue_stream_ordered_frame_for_target(
        &self,
        target: &ResponseDispatchTarget,
        frame: Frame,
        lane: FlowLane,
    ) -> Result<(), RuntimeError> {
        self.try_enqueue_frame_for_target(target, frame, lane, true)
    }

    fn try_enqueue_frame_for_target(
        &self,
        target: &ResponseDispatchTarget,
        frame: Frame,
        lane: FlowLane,
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
                && entry.role == target.attachment_role
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

    fn set_request_active_owner(&self, key: CarrierPathKey) {
        *self
            .request_active_owner
            .lock()
            .expect("server reliable stream request active owner lock") = Some(key);
    }

    fn clear_request_active_owner_if(&self, key: CarrierPathKey) {
        let mut active = self
            .request_active_owner
            .lock()
            .expect("server reliable stream request active owner lock");
        if *active == Some(key) {
            *active = None;
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn set_ordered_data_owner(&self, key: CarrierPathKey) {
        let lane = self.lane();
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let mut lead = self
            .ordered_data_owner
            .lock()
            .expect("server reliable stream ordered data owner lock");
        if *lead != Some(key) {
            *lead = Some(key);
            self.reset_subflow_set_with_outputs(&mut outputs);
            self.response_flow_registration
                .set_service(Some((key, lane)));
            drop(lead);
            drop(outputs);
            self.notify_update();
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn commit_ordered_data_owner_for_target(
        &self,
        target: &ResponseSenderPathTarget,
    ) -> bool {
        self.commit_ordered_data_owner_for_dispatch_target(&target.into())
    }

    #[cfg(test)]
    pub(in crate::runtime) fn commit_ordered_data_owner_for_dispatch_target(
        &self,
        target: &ResponseDispatchTarget,
    ) -> bool {
        let lane = self.lane();
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let target_is_live = outputs.entries.iter().any(|entry| {
            entry.key == target.key
                && entry.incarnation == target.incarnation
                && entry.commands.same_channel(&target.commands)
                && !entry.commands.is_closed()
        });
        if !target_is_live {
            return false;
        }
        let mut lead = self
            .ordered_data_owner
            .lock()
            .expect("server reliable stream ordered data owner lock");
        if !self.response_stream_open.load(Ordering::Acquire) {
            return false;
        }
        let changed = *lead != Some(target.key);
        if changed {
            *lead = Some(target.key);
            outputs
                .ack_clock_calibrations
                .remove(&(target.key, target.incarnation));
            if outputs.active_ack_clock_calibration == Some((target.key, target.incarnation)) {
                outputs.active_ack_clock_calibration = None;
            }
            self.reset_subflow_set_with_outputs(&mut outputs);
            self.response_flow_registration
                .set_service(Some((target.key, lane)));
        }
        drop(lead);
        drop(outputs);
        if changed {
            self.notify_update();
        }
        true
    }

    fn clear_ordered_data_owner_if(&self, key: CarrierPathKey) {
        let mut lead = self
            .ordered_data_owner
            .lock()
            .expect("server reliable stream ordered data owner lock");
        let changed = *lead == Some(key);
        if changed {
            *lead = None;
            self.response_flow_registration.set_service(None);
        }
        drop(lead);
    }

    fn repair_ordered_data_owner_after_output_change(
        &self,
        live_entries: &[ResponseStreamOutputEntry],
    ) {
        let mut lead = self
            .ordered_data_owner
            .lock()
            .expect("server reliable stream ordered data owner lock");
        let live_lead = lead.is_some_and(|key| live_entries.iter().any(|entry| entry.key == key));
        let cleared = !live_lead && lead.is_some();
        if !live_lead {
            *lead = None;
        }
        if cleared {
            self.response_flow_registration.set_service(None);
        }
        drop(lead);
    }
}

/// Handle-free response candidate captured by one observe pass.
#[derive(Clone)]
pub(in crate::runtime) struct ResponseSenderPathTarget {
    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) session_id: SessionId,
    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) binding_instance_id: u64,
    pub(in crate::runtime) observation: ResponsePathObservation,
    pub(in crate::runtime) command_queue: ReliablePathCommandQueueSnapshot,
    pub(in crate::runtime) tcp_capacity_probe_attempted: bool,
    pub(in crate::runtime) tcp_capacity_probe_active: bool,
    /// Endpoint-only configuration plus an immature candidate ACK model may
    /// use Service only as a bounded calibration-opportunity prior.
    pub(in crate::runtime) endpoint_only_service_prior_eligible: bool,
    /// Raw receipt marker; handoff may pin it without renewing global freshness.
    pub(in crate::runtime) quic_capacity_proof: Option<QuicCapacityProofCandidate>,
    pub(in crate::runtime) quic_capacity_calibration_attempts: u8,
    pub(in crate::runtime) ack_clock_calibration_eligible: bool,
    pub(in crate::runtime) ack_clock_calibration_proven: bool,
    pub(in crate::runtime) ack_clock_calibration_spent_bytes: u64,
    pub(in crate::runtime) ack_clock_calibration_credit_limit_bytes: u64,
    pub(in crate::runtime) ack_clock_calibration_max_limit_bytes: u64,
    pub(in crate::runtime) ack_clock_calibration_active: bool,
}

impl AsRef<ResponsePathObservation> for ResponseSenderPathTarget {
    fn as_ref(&self) -> &ResponsePathObservation {
        &self.observation
    }
}

impl ResponseSenderPathTarget {
    pub(in crate::runtime) fn can_enqueue_lane(&self, lane: FlowLane) -> bool {
        self.command_queue.can_enqueue_lane(lane)
    }

    pub(in crate::runtime) fn can_enqueue_frame(
        &self,
        frame: &crate::protocol::Frame,
        lane: FlowLane,
    ) -> bool {
        self.command_queue.can_enqueue_frame(frame, lane)
    }

    pub(in crate::runtime) fn can_enqueue_stream_ordered_frame(&self) -> bool {
        self.command_queue.can_enqueue_stream_ordered_frame()
    }

    pub(in crate::runtime) fn data_queue_open(&self) -> bool {
        self.command_queue.data_open()
    }
}

/// ID-only apply target retained after ranking. The binding resolves the exact
/// live command port under its output lock before committing any mutation.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ResponseDispatchTarget {
    pub(in crate::runtime) key: CarrierPathKey,
    pub(in crate::runtime) path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime) incarnation: u64,
    pub(in crate::runtime) attachment_role: StreamOpenRole,
    pub(in crate::runtime) has_bulk_rate_evidence: bool,
}

impl From<ResponseSenderPathTarget> for ResponseDispatchTarget {
    fn from(target: ResponseSenderPathTarget) -> Self {
        Self {
            key: target.observation.key,
            path_instance_id: target.observation.path_instance_id,
            incarnation: target.observation.incarnation,
            attachment_role: target.observation.attachment_role,
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
            attachment_role: target.observation.attachment_role,
            has_bulk_rate_evidence: target.observation.has_bulk_rate_evidence,
        }
    }
}

fn response_stream_live_role_update(
    current: StreamOpenRole,
    requested: StreamOpenRole,
) -> StreamOpenRole {
    match (current, requested) {
        (StreamOpenRole::Active, _) => StreamOpenRole::Active,
        (_, StreamOpenRole::Active) => StreamOpenRole::Active,
        (StreamOpenRole::Repair, _) | (_, StreamOpenRole::Repair) => StreamOpenRole::Repair,
        _ => current,
    }
}

fn response_stream_role_change_invalidates_response_state(
    previous: StreamOpenRole,
    current: StreamOpenRole,
) -> bool {
    previous != current
        && ((previous == StreamOpenRole::Repair) != (current == StreamOpenRole::Repair))
}

pub(super) fn response_stream_role_reserves_flow_load(role: StreamOpenRole) -> bool {
    role == StreamOpenRole::Active
}

pub(super) fn response_live_ordered_data_owner(
    stored: Option<CarrierPathKey>,
    entries: &[ResponseStreamOutputEntry],
) -> Option<CarrierPathKey> {
    stored.filter(|key| entries.iter().any(|entry| entry.key == *key))
}

pub(super) fn response_outputs_have_live_mixed_owner_underlays(
    entries: &[ResponseStreamOutputEntry],
) -> bool {
    let mut first_underlay = None;
    for entry in entries
        .iter()
        .filter(|entry| entry.role != StreamOpenRole::Repair && !entry.commands.is_closed())
    {
        match first_underlay {
            Some(underlay) if underlay != entry.key.underlay => return true,
            Some(_) => {}
            None => first_underlay = Some(entry.key.underlay),
        }
    }
    false
}

#[cfg(test)]
#[path = "attachment_test.rs"]
mod tests;
