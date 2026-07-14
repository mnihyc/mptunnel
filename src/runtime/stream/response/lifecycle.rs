//! Construction, publication, and retirement of a response binding.
//! Close snapshots outputs under lock, then releases it before carrier awaits.

use super::admission::ResponseSubflowSetState;
use super::delivery::ResponseAckOrderingState;
use super::load::ServerResponseFlowRegistration;
use super::session::ServerPathLaneTracker;
use super::topology::{
    ResponseStreamOutputEntry, ResponseStreamOutputs, ServerCarrierPathInstanceId,
    next_server_carrier_path_instance_id, response_owner_underlay_seen_bit,
    response_stream_role_reserves_flow_load,
};
use super::{NEXT_RESPONSE_STREAM_BINDING_INSTANCE_ID, ResponseStreamBinding};
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::{PathId, SessionId, StreamId, StreamOpenRole, UnderlayProtocol};
use crate::runtime::path::commands::{ReliablePathCommand, ReliablePathCommandSender};
use crate::scheduler::FlowLane;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

// Drop has exclusive aggregate access; lane and output cleanup needs no locks.
impl Drop for ResponseStreamBinding {
    fn drop(&mut self) {
        self.response_flow_registration.set_active(false);
        self.lane_tracker
            .clear_response_service_handoff_drain_for_binding(
                self.session_id,
                self.binding_instance_id,
            );
        let lane = *self
            .lane
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let outputs = self
            .outputs
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for entry in outputs.entries.drain(..) {
            if response_stream_role_reserves_flow_load(entry.role) {
                self.lane_tracker.detach(self.session_id, entry.key, lane);
            }
            self.lane_tracker.clear_quic_capacity_calibration(
                self.session_id,
                self.binding_instance_id,
                entry.key,
                entry.path_instance_id,
            );
        }
    }
}

impl ResponseStreamBinding {
    #[cfg(test)]
    pub(in crate::runtime) fn new(
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: FlowLane,
    ) -> Arc<Self> {
        Self::new_with_limits(
            session_id,
            underlay,
            path_id,
            commands,
            lane,
            MuxLimits::default(),
        )
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime) fn new_with_limits(
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: FlowLane,
        mux_limits: MuxLimits,
    ) -> Arc<Self> {
        Self::new_with_limits_and_tracker(
            session_id,
            underlay,
            path_id,
            commands,
            lane,
            mux_limits,
            Arc::new(ServerPathLaneTracker::default()),
        )
    }

    pub(in crate::runtime) fn new_with_limits_and_tracker(
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: FlowLane,
        mux_limits: MuxLimits,
        lane_tracker: Arc<ServerPathLaneTracker>,
    ) -> Arc<Self> {
        Self::new_with_limits_tracker_and_path_instance(
            session_id,
            underlay,
            path_id,
            commands,
            lane,
            mux_limits,
            lane_tracker,
            next_server_carrier_path_instance_id(),
        )
    }

    pub(in crate::runtime::stream) fn new_with_limits_tracker_and_path_instance(
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: FlowLane,
        mux_limits: MuxLimits,
        lane_tracker: Arc<ServerPathLaneTracker>,
        path_instance_id: ServerCarrierPathInstanceId,
    ) -> Arc<Self> {
        let (version, _) = watch::channel(0);
        let key = CarrierPathKey { underlay, path_id };
        let response_flow_registration =
            ServerResponseFlowRegistration::new(lane_tracker.clone(), session_id, key, lane);
        lane_tracker.attach(session_id, key, lane);
        response_flow_registration.set_active(true);
        Arc::new(Self {
            session_id,
            binding_instance_id: NEXT_RESPONSE_STREAM_BINDING_INSTANCE_ID
                .fetch_add(1, Ordering::AcqRel),
            lane: Mutex::new(lane),
            mux_limits,
            lane_tracker,
            response_flow_registration,
            next_output_incarnation: AtomicU64::new(2),
            response_model_generation: AtomicU64::new(0),
            owner_underlay_history: AtomicU8::new(response_owner_underlay_seen_bit(underlay)),
            response_stream_open: AtomicBool::new(true),
            response_service_handoff_open: AtomicBool::new(true),
            response_service_handoff_drain_attempted: AtomicBool::new(false),
            #[cfg(feature = "lab-diagnostics")]
            response_service_handoff_diagnostic: Mutex::new(None),
            #[cfg(feature = "lab-diagnostics")]
            response_service_feed_diagnostic: Mutex::new(HashMap::new()),
            outputs: Mutex::new(ResponseStreamOutputs {
                entries: vec![ResponseStreamOutputEntry {
                    key,
                    path_instance_id,
                    incarnation: 1,
                    commands,
                    role: StreamOpenRole::Active,
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
                }],
                ack_clock_calibrations: HashMap::new(),
                active_ack_clock_calibration: None,
            }),
            request_active_owner: Mutex::new(Some(key)),
            ordered_data_owner: Mutex::new(Some(key)),
            flights: Mutex::new(BTreeMap::new()),
            ack_ordering: Mutex::new(ResponseAckOrderingState::default()),
            subflow_set: Mutex::new(ResponseSubflowSetState::default()),
            version,
        })
    }

    pub(in crate::runtime) fn subscribe_updates(&self) -> watch::Receiver<u64> {
        self.version.subscribe()
    }

    pub(in crate::runtime) async fn close_stream(&self, stream_id: StreamId) {
        let outputs = {
            let outputs = self
                .outputs
                .lock()
                .expect("server reliable stream binding lock");
            self.response_stream_open.store(false, Ordering::Release);
            outputs.entries.clone()
        };
        self.response_flow_registration.set_active(false);
        self.lane_tracker
            .clear_quic_capacity_calibration_for_binding(self.session_id, self.binding_instance_id);
        self.lane_tracker
            .clear_response_service_handoff_drain_for_binding(
                self.session_id,
                self.binding_instance_id,
            );
        for entry in outputs {
            let _ = entry
                .commands
                .send_control(ReliablePathCommand::CloseStream(stream_id))
                .await;
        }
        let mut lead = self
            .ordered_data_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *lead = None;
        self.response_flow_registration.set_service(None);
    }

    pub(in crate::runtime) async fn close_stream_ordered(
        &self,
        stream_id: StreamId,
        lane: FlowLane,
    ) {
        let outputs = {
            let outputs = self
                .outputs
                .lock()
                .expect("server reliable stream binding lock");
            self.response_stream_open.store(false, Ordering::Release);
            outputs.entries.clone()
        };
        self.response_flow_registration.set_active(false);
        self.lane_tracker
            .clear_quic_capacity_calibration_for_binding(self.session_id, self.binding_instance_id);
        self.lane_tracker
            .clear_response_service_handoff_drain_for_binding(
                self.session_id,
                self.binding_instance_id,
            );
        for entry in outputs {
            let _ = entry
                .commands
                .send_stream_ordered_close(stream_id, lane)
                .await;
        }
        let mut lead = self
            .ordered_data_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *lead = None;
        self.response_flow_registration.set_service(None);
    }

    pub(super) fn notify_update(&self) {
        let current = *self.version.borrow();
        let _ = self.version.send(current.wrapping_add(1));
    }
}

#[cfg(test)]
#[path = "lifecycle_test.rs"]
mod tests;
