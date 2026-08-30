//! Server response-stream ownership and its narrow runtime contract.
//!
//! The binding schema stays here so child transition owners share private
//! invariants without widening its locks or state fields.

mod ack_clock;
mod attachment;
mod data_commit;
mod delivery;
mod diagnostics;
mod evidence;
mod requalification;
mod session;
mod snapshot;

use crate::model::path::{CarrierPathInstanceId, CarrierPathKey, PathPolicy};
use crate::model::requalification::StreamPathQualification;
use crate::mux::MuxLimits;
use crate::protocol::{PathId, ResetReason, SessionId, StreamId, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::{ReliablePathCommand, ReliablePathCommandSender};
use crate::scheduler::TrafficClass;
#[cfg(test)]
pub(in crate::runtime) use attachment::next_server_carrier_path_instance_id;
pub(in crate::runtime) use attachment::{
    ResponseDispatchTarget, ResponseOutputAttachment, ResponseOutputAttachmentState,
    ResponsePathDetachOutcome, ResponseSenderPathTarget, ResponseStreamAttachOutcome,
};
use attachment::{ResponseStreamOutputEntry, ResponseStreamOutputs};
use delivery::ResponseAckOrderingState;
pub(super) use delivery::{
    CarrierPathFlight, product_flights_have_recent_reinjection_overlap,
    release_carrier_path_flight_ranges,
};
pub(in crate::runtime) use delivery::{ResponseDataAckRecoveryCandidate, ResponseDataAckRelease};
pub(in crate::runtime) use diagnostics::record_server_sender_decision;
pub(in crate::runtime) use evidence::{ServerPathMetricsEntry, ServerPathMetricsSource};
pub(in crate::runtime) use session::{ServerSessionRegistration, ServerSessionTracker};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

// Reliable-path bindings own attachment instances, exact range flights,
// evidence, and atomic commit. Sender services rank immutable snapshots.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RequestFeedbackIngress {
    key: CarrierPathKey,
    path_instance_id: CarrierPathInstanceId,
}

// Ownership boundary:
// This module owns carrier-neutral reliable stream bindings on the response
// side. It tracks which carrier path carried each product byte range, records
// ordering debt and stream-ACK release. It must not choose among joined carrier
// paths for response frames; dispatch belongs to the sender service. It must
// not implement TCP framing, QUIC packet recovery, or target socket I/O; those
// belong to carrier and outbound modules.

/// Server-side response owner for one product reliable stream.
///
/// This binding owns the stream's attached carrier outputs, product byte flight
/// ledger, stream-ACK ordering state, lane tracking, and path-metric hints used
/// for response scheduling. It does not own the target socket and does not own
/// TCP/QUIC packet recovery.
pub(in crate::runtime) struct ResponseStreamBinding {
    session_id: SessionId,
    lane: Mutex<TrafficClass>,
    mux_limits: MuxLimits,
    session_registration: ServerSessionRegistration,
    next_output_incarnation: AtomicU64,
    /// Changes only when the set of live response attachments changes.
    output_membership_generation: AtomicU64,
    // Publishes coherent path evidence, exact flights, ACK ordering, and queues.
    response_model_generation: AtomicU64,
    // Close publishes before carrier commands so no later scheduler commit can
    // assign new connection data after stream retirement begins.
    response_stream_open: AtomicBool,
    outputs: Mutex<ResponseStreamOutputs>,
    // Return-path affinity is observed ingress, not request or response
    // ownership. Exact carrier identity prevents reuse across reconnects.
    request_feedback_ingress: Mutex<Option<RequestFeedbackIngress>>,
    flights: Mutex<BTreeMap<u64, Vec<CarrierPathFlight>>>,
    ack_ordering: Mutex<ResponseAckOrderingState>,
    version: watch::Sender<u64>,
}

impl ResponseStreamBinding {
    #[cfg(test)]
    pub(in crate::runtime) fn new(
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: TrafficClass,
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

    #[cfg(test)]
    pub(in crate::runtime) fn new_with_limits(
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: TrafficClass,
        mux_limits: MuxLimits,
    ) -> Arc<Self> {
        Self::new_with_limits_and_tracker(
            session_id,
            underlay,
            path_id,
            commands,
            lane,
            mux_limits,
            Arc::new(ServerSessionTracker::default()),
        )
    }

    #[cfg(test)]
    pub(in crate::runtime) fn new_with_limits_and_tracker(
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: TrafficClass,
        mux_limits: MuxLimits,
        session_tracker: Arc<ServerSessionTracker>,
    ) -> Arc<Self> {
        // This test-only constructor stands in for a live authenticated
        // carrier. Exercise the same session/principal invariant as production
        // and release the temporary carrier reference after the product-flow
        // registration has taken ownership.
        session_tracker
            .attach_authenticated_session(
                session_id,
                &crate::product::PrincipalPermit::for_test("test-peer"),
            )
            .expect("register test authenticated carrier");
        let binding = Self::new_with_limits_tracker_and_path_instance(
            session_id,
            underlay,
            path_id,
            commands,
            lane,
            mux_limits,
            session_tracker.clone(),
            next_server_carrier_path_instance_id(),
            PathPolicy::default(),
        )
        .expect("register active test response owner");
        session_tracker.detach_session(session_id);
        binding
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::runtime::stream) fn new_with_limits_tracker_and_path_instance(
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: TrafficClass,
        mux_limits: MuxLimits,
        session_tracker: Arc<ServerSessionTracker>,
        path_instance_id: CarrierPathInstanceId,
        local_policy: PathPolicy,
    ) -> Result<Arc<Self>, RuntimeError> {
        let (version, _) = watch::channel(0);
        let key = CarrierPathKey { underlay, path_id };
        let session_registration = ServerSessionRegistration::try_new(session_tracker, session_id)?;
        let load_registration = commands.register_flow(lane);
        Ok(Arc::new(Self {
            session_id,
            lane: Mutex::new(lane),
            mux_limits,
            session_registration,
            next_output_incarnation: AtomicU64::new(2),
            output_membership_generation: AtomicU64::new(1),
            response_model_generation: AtomicU64::new(0),
            response_stream_open: AtomicBool::new(true),
            outputs: Mutex::new(ResponseStreamOutputs {
                detaching: Vec::new(),
                entries: vec![ResponseStreamOutputEntry {
                    key,
                    path_instance_id,
                    local_policy,
                    incarnation: 1,
                    commands,
                    load_registration,
                    original_data_in_flight_bytes: 0,
                    qualification: StreamPathQualification::Qualified,
                    bytes_in_flight: 0,
                    product_rate_epoch: None,
                    tcp_product_rate_evidence: None,
                    srtt_ms: None,
                    delivery_samples: 0,
                    original_data_acked_bytes: 0,
                    // Carrier admission starts at zero. Target establishment
                    // later publishes the retained logical receive grant.
                    published_max_data_offset: 0,
                    ack_publication: Default::default(),
                    local_path_metrics: None,
                    peer_path_metrics: None,
                    peer_usage: None,
                    peer_usage_sequence: None,
                    path_proof: None,
                }],
                data_level_queue_bytes: 0,
                desired_max_data_offset: 0,
                next_requalification_probe_id: Some(1),
                next_requalification_candidate_index: 0,
            }),
            request_feedback_ingress: Mutex::new(Some(RequestFeedbackIngress {
                key,
                path_instance_id,
            })),
            flights: Mutex::new(BTreeMap::new()),
            ack_ordering: Mutex::new(ResponseAckOrderingState::default()),
            version,
        }))
    }

    pub(in crate::runtime::stream) fn session_send_buffer(&self) -> super::SessionSendBuffer {
        self.session_registration.send_buffer()
    }

    pub(in crate::runtime::stream) fn attach_output_if_session_active(
        &self,
        attachment: ResponseOutputAttachment,
    ) -> Result<ResponseStreamAttachOutcome, RuntimeError> {
        self.session_registration
            .commit_if_active(|| self.attach_output(attachment))
    }

    pub(in crate::runtime) fn subscribe_updates(&self) -> watch::Receiver<u64> {
        self.version.subscribe()
    }

    fn begin_close(&self) -> Vec<ReliablePathCommandSender> {
        {
            let outputs = self
                .outputs
                .lock()
                .expect("server reliable stream binding lock");
            self.response_stream_open.store(false, Ordering::Release);
            for entry in outputs.entries.iter().chain(&outputs.detaching) {
                entry.load_registration.deactivate();
            }
            outputs
                .entries
                .iter()
                .map(|entry| entry.commands.clone())
                .collect::<Vec<_>>()
        }
    }

    /// Commits the one cancellation-safe terminal handoff. Unlike the
    /// awaitable ordinary close path, this transition cannot be interrupted
    /// between closing attachment admission and transferring every reset to
    /// the carrier retirement lanes.
    fn begin_terminal_handoff(&self) -> Vec<ReliablePathCommandSender> {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        if !self.response_stream_open.swap(false, Ordering::AcqRel) {
            return Vec::new();
        }
        for entry in outputs.entries.iter().chain(&outputs.detaching) {
            entry.load_registration.deactivate();
        }
        outputs
            .entries
            .iter()
            .map(|entry| entry.commands.clone())
            .collect()
    }

    pub(in crate::runtime) async fn close_stream(&self, stream_id: StreamId) {
        let outputs = self.begin_close();
        for commands in outputs {
            let _ = commands
                .send_control(ReliablePathCommand::CloseStream(stream_id))
                .await;
        }
    }

    pub(in crate::runtime) async fn reset_and_close_stream(
        &self,
        stream_id: StreamId,
        reason: ResetReason,
    ) {
        let outputs = self.begin_close();
        futures::future::join_all(outputs.into_iter().map(|commands| async move {
            let _ = commands
                .send_control(ReliablePathCommand::ResetAndCloseStream { stream_id, reason })
                .await;
        }))
        .await;
    }

    /// Closes Product attachment admission and synchronously transfers one
    /// timeout reset per exact live output to its carrier-owned retirement
    /// lane. Repeated retirement is an idempotent no-op.
    pub(in crate::runtime) fn retire_stream_with_reset(
        &self,
        stream_id: StreamId,
        reason: ResetReason,
    ) {
        for commands in self.begin_terminal_handoff() {
            let _ = commands.reset_accepted_stream(stream_id, reason);
        }
    }

    pub(in crate::runtime) async fn close_stream_ordered(
        &self,
        stream_id: StreamId,
        lane: TrafficClass,
    ) {
        let outputs = self.begin_close();
        for commands in outputs {
            let _ = commands.send_stream_ordered_close(stream_id, lane).await;
        }
    }

    /// Publishes terminal refusal and snapshots every affected output in one
    /// transaction, so an attachment cannot miss both the reset and closure.
    pub(in crate::runtime) async fn reset_and_close_stream_ordered(
        &self,
        stream_id: StreamId,
        reason: ResetReason,
        lane: TrafficClass,
    ) {
        let outputs = self.begin_close();
        futures::future::join_all(outputs.into_iter().map(|commands| async move {
            let _ = commands
                .send_stream_ordered_reset_and_close(stream_id, reason, lane)
                .await;
        }))
        .await;
    }

    pub(super) fn notify_update(&self) {
        let current = *self.version.borrow();
        let _ = self.version.send(current.wrapping_add(1));
    }
}

#[cfg(test)]
#[path = "tests_response.rs"]
mod tests;

#[cfg(test)]
#[path = "response/tests_test_support.rs"]
mod test_support;
