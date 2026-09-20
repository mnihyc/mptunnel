//! Client relay carrier acquisition and attachment policy.
//!
//! This module ranks and opens concrete carrier candidates. Request-stream
//! membership, generations, scheduler leases, and frame fan-in stay in
//! `stream::request::attachment`.

use super::client::ClientRelayPathOpenSuppressions;
use super::lifecycle::ClientReliableReturnPlan;
use super::open::{
    ReliableRelayOpenSpec, no_schedulable_reliable_path_error, open_remote_stream_for_relay_path,
    relay_path_open_error_is_retryable,
};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, adaptive_reliable_relay_chunk_bytes, relay_lane_startup_chunk_bytes,
    reliable_relay_buffer_len,
};
use crate::model::path::{CarrierPathInstanceId, RelayPathKey};
use crate::mux::MuxLimits;
use crate::mux::stream::ReliableSendStream;
use crate::protocol::{Frame, StreamId, UnderlayProtocol};
use crate::runtime::error::{RuntimeError, reliable_path_error_is_migratable};
use crate::runtime::path::ClientPathContext;
use crate::runtime::stream::{
    OpenedRemoteStream, ReliablePathStream, ReliableRelayAttachOutcome, ReliableRelayRemoteSet,
};
use crate::scheduler::TrafficClass;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) enum ReliableRelayAttachMode {
    Any,
    BulkStriping,
    Recovery,
}

/// Separates client-owned topology ranking from the sender-local request lane
/// installed on a newly opened output attachment.
#[derive(Debug, Clone, Copy)]
pub(super) struct ReliableRelayPathLanes {
    pub(super) selection: TrafficClass,
    pub(super) output: TrafficClass,
}

impl ReliableRelayPathLanes {
    pub(super) const fn new(selection: TrafficClass, output: TrafficClass) -> Self {
        Self { selection, output }
    }

    #[cfg(test)]
    pub(super) const fn same(lane: TrafficClass) -> Self {
        Self::new(lane, lane)
    }
}

/// Owned send-state inputs for an attachment open. Capture these before the
/// network await so opening a carrier does not retain a send-state borrow.
/// The caller retains the existing responsibility for FIN eligibility.
#[derive(Debug, Clone, Copy)]
pub(super) struct ReliableRelayAttachInput {
    mode: ReliableRelayAttachMode,
    payload_bytes: usize,
    prefer_reinjection_alternative: bool,
    final_offset: Option<u64>,
}

impl ReliableRelayAttachInput {
    pub(super) fn capture(
        send_stream: &ReliableSendStream,
        selection_lane: TrafficClass,
        mux_limits: MuxLimits,
        resend_fin: bool,
        mode: ReliableRelayAttachMode,
    ) -> Self {
        let payload_bytes = match mode {
            ReliableRelayAttachMode::Any | ReliableRelayAttachMode::Recovery => {
                reliable_relay_attach_payload_bytes(send_stream, selection_lane, mux_limits)
            }
            ReliableRelayAttachMode::BulkStriping => {
                reliable_relay_bulk_striping_payload_bytes(send_stream, mux_limits)
            }
        };
        let prefer_reinjection_alternative = matches!(mode, ReliableRelayAttachMode::Recovery)
            || reliable_relay_should_open_reinjection_alternative(
                selection_lane,
                send_stream,
                resend_fin,
                mode,
            );
        Self {
            mode,
            payload_bytes,
            prefer_reinjection_alternative,
            final_offset: resend_fin.then(|| send_stream.next_offset()),
        }
    }
}

fn send_request_attach_control_frames(
    path_stream: &ReliablePathStream,
    final_offset: Option<u64>,
) -> Result<(), RuntimeError> {
    if let Some(final_offset) = final_offset {
        path_stream.try_enqueue_request_control_frame(Frame::StreamFin {
            stream_id: path_stream.stream_id,
            final_offset,
        })?;
    }
    Ok(())
}

/// Actor-owned ordered retry state. Only an individual owned attempt crosses
/// native opening I/O; attachment membership is borrowed at begin/finish only.
pub(super) struct ReliableRelayAttachPlan {
    spec: ReliableRelayOpenSpec,
    stream_id: StreamId,
    output_lane: TrafficClass,
    final_offset: Option<u64>,
    candidates: std::vec::IntoIter<RelayPathKey>,
    last_retryable_error: Option<RuntimeError>,
}

pub(super) struct ReliableRelayAttachAttempt {
    spec: ReliableRelayOpenSpec,
    stream_id: StreamId,
    output_lane: TrafficClass,
    key: RelayPathKey,
    startup_ordinal: Option<u8>,
    startup_expected_instance: Option<CarrierPathInstanceId>,
}

pub(super) struct ReliableRelayAttachCompletion {
    key: RelayPathKey,
    startup_ordinal: Option<u8>,
    startup_expected_instance: Option<CarrierPathInstanceId>,
    result: Result<OpenedRemoteStream, RuntimeError>,
}

impl ReliableRelayAttachAttempt {
    pub(super) async fn open(self, context: &ClientPathContext) -> ReliableRelayAttachCompletion {
        let result = open_remote_stream_for_relay_path(
            context,
            self.stream_id,
            &self.spec,
            self.output_lane,
            self.key,
        )
        .await;
        ReliableRelayAttachCompletion {
            key: self.key,
            startup_ordinal: self.startup_ordinal,
            startup_expected_instance: self.startup_expected_instance,
            result,
        }
    }
}

impl ReliableRelayAttachPlan {
    pub(super) fn next_attempt(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        startup: &mut ClientReliableReturnPlan,
    ) -> Result<Option<ReliableRelayAttachAttempt>, RuntimeError> {
        for key in self.candidates.by_ref() {
            if remotes.contains_path_key(key) {
                continue;
            }
            let current_instance = context
                .health()
                .lock()
                .expect("client path health lock")
                .path_record(key)
                .and_then(|record| record.path_instance_id());
            let startup_ordinal = startup.begin_candidate_for_open(key, current_instance);
            let startup_expected_instance =
                startup_ordinal.and_then(|ordinal| startup.bound_instance(ordinal));
            let spec = startup_ordinal.map_or_else(
                || self.spec.for_ordinary_attachment(),
                |ordinal| self.spec.for_startup_ordinal(ordinal),
            );
            return Ok(Some(ReliableRelayAttachAttempt {
                spec,
                stream_id: self.stream_id,
                output_lane: self.output_lane,
                key,
                startup_ordinal,
                startup_expected_instance,
            }));
        }
        if remotes.is_empty() {
            Err(self
                .last_retryable_error
                .take()
                .unwrap_or_else(|| no_schedulable_reliable_path_error(context)))
        } else {
            Ok(None)
        }
    }

    pub(super) fn finish_attempt(
        &mut self,
        remotes: &mut ReliableRelayRemoteSet,
        startup: &mut ClientReliableReturnPlan,
        completion: ReliableRelayAttachCompletion,
    ) -> Result<bool, RuntimeError> {
        let ReliableRelayAttachCompletion {
            key,
            startup_ordinal,
            startup_expected_instance,
            result,
        } = completion;
        match result {
            Ok(opened) => {
                if let Some(error) = opened.terminal_error() {
                    return Err(error);
                }
                if startup_expected_instance
                    .is_some_and(|expected| opened.path_instance_id() != expected)
                {
                    opened.retire_uncommitted();
                    if let Some(ordinal) = startup_ordinal {
                        startup.settle_failed(ordinal)?;
                    }
                    return Ok(false);
                }
                let attach_control_result =
                    send_request_attach_control_frames(opened.stream(), self.final_offset);
                match attach_control_result {
                    Ok(()) => {
                        let attach_outcome = remotes.try_attach_candidate(opened)?;
                        match attach_outcome {
                            ReliableRelayAttachOutcome::Attached => {
                                if let Some(ordinal) = startup_ordinal {
                                    let instance = remotes.path_instance_for_key(key).ok_or(
                                        RuntimeError::Protocol(
                                            "attached startup carrier is absent from membership",
                                        ),
                                    )?;
                                    startup.settle_accepted(ordinal, instance)?;
                                }
                                return Ok(true);
                            }
                            ReliableRelayAttachOutcome::RejectedDuplicate => {
                                if let Some(ordinal) = startup_ordinal {
                                    startup.settle_failed(ordinal)?;
                                }
                                return Ok(false);
                            }
                        }
                    }
                    Err(err) if reliable_path_error_is_migratable(&err) => {
                        if let Some(ordinal) = startup_ordinal {
                            startup.settle_failed(ordinal)?;
                        }
                        self.last_retryable_error = Some(err);
                    }
                    Err(err) => {
                        if let Some(ordinal) = startup_ordinal {
                            startup.settle_failed(ordinal)?;
                        }
                        return Err(err);
                    }
                }
            }
            Err(err @ RuntimeError::ReliablePathAttachmentRefused) => {
                if let Some(ordinal) = startup_ordinal {
                    startup.settle_failed(ordinal)?;
                }
                // The server refused this attachment, not the carrier. Keep
                // global path health intact and consider the next candidate.
                self.last_retryable_error = Some(err);
            }
            Err(err) if relay_path_open_error_is_retryable(key.underlay, &err) => {
                if let Some(ordinal) = startup_ordinal {
                    startup.settle_failed(ordinal)?;
                }
                self.last_retryable_error = Some(err);
            }
            Err(err) => {
                if let Some(ordinal) = startup_ordinal {
                    startup.settle_failed(ordinal)?;
                }
                return Err(err);
            }
        }
        Ok(false)
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn begin_reliable_relay_attach_with_claims_and_suppressions(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    lanes: ReliableRelayPathLanes,
    remotes: &ReliableRelayRemoteSet,
    input: ReliableRelayAttachInput,
    path_open_suppressions: &ClientRelayPathOpenSuppressions,
    inflight_path_claims: &HashSet<RelayPathKey>,
) -> ReliableRelayAttachPlan {
    let payload_bytes = input.payload_bytes;
    // The previous bulk branch always returned: exhaustion is an error when
    // membership is empty and otherwise succeeds with zero attachments.
    let candidates = if matches!(input.mode, ReliableRelayAttachMode::BulkStriping) {
        context.ordered_reliable_bulk_striping_path_keys(payload_bytes)
    } else if input.prefer_reinjection_alternative {
        reliable_relay_reinjection_path_candidates(context, remotes, lanes.selection, payload_bytes)
    } else {
        reliable_relay_additional_path_candidates(context, remotes, lanes.selection, payload_bytes)
    };
    let candidates = reliable_relay_exclude_inflight_open_claims(
        reliable_relay_path_open_candidates_after_suppression(
            context,
            candidates,
            path_open_suppressions,
        ),
        inflight_path_claims,
    );
    ReliableRelayAttachPlan {
        spec: spec.clone(),
        stream_id: remotes.stream_id(),
        output_lane: lanes.output,
        final_offset: input.final_offset,
        candidates: candidates.into_iter(),
        last_retryable_error: None,
    }
}

pub(in crate::runtime) fn reliable_relay_additional_path_candidates(
    context: &ClientPathContext,
    remotes: &ReliableRelayRemoteSet,
    lane: TrafficClass,
    payload_bytes: usize,
) -> Vec<RelayPathKey> {
    context
        .ordered_reliable_path_keys(lane, payload_bytes)
        .into_iter()
        .filter(|key| !remotes.contains_path_key(*key))
        .collect()
}

pub(super) fn reliable_relay_path_open_candidates_after_suppression(
    context: &ClientPathContext,
    candidates: Vec<RelayPathKey>,
    path_open_suppressions: &ClientRelayPathOpenSuppressions,
) -> Vec<RelayPathKey> {
    let now = tokio::time::Instant::now();
    candidates
        .iter()
        .copied()
        .filter(|key| !path_open_suppressions.blocks(context, *key, now))
        .collect()
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

pub(in crate::runtime) fn reliable_relay_reinjection_path_candidates(
    context: &ClientPathContext,
    remotes: &ReliableRelayRemoteSet,
    lane: TrafficClass,
    payload_bytes: usize,
) -> Vec<RelayPathKey> {
    let preferred = remotes.preferred_path_key(context, lane, payload_bytes);
    context
        .ordered_reliable_reinjection_path_keys(
            preferred
                .filter(|key| key.underlay == UnderlayProtocol::Tcp)
                .map(|key| key.index),
            preferred
                .filter(|key| key.underlay == UnderlayProtocol::Udp)
                .map(|key| key.index),
            lane,
            payload_bytes,
        )
        .into_iter()
        .filter(|key| !remotes.contains_path_key(*key))
        .collect()
}

pub(in crate::runtime) fn reliable_relay_should_open_reinjection_alternative(
    lane: TrafficClass,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: ReliableRelayAttachMode,
) -> bool {
    matches!(mode, ReliableRelayAttachMode::Any)
        && !resend_fin
        && (send_stream.reinjection_bytes() > 0
            || (lane.is_latency_sensitive()
                && send_stream.reinjection_bytes() <= PATH_OPEN_SCORE_BYTES))
}

pub(in crate::runtime) fn reliable_relay_attach_payload_bytes(
    send_stream: &ReliableSendStream,
    lane: TrafficClass,
    mux_limits: MuxLimits,
) -> usize {
    let floor = if lane.is_latency_sensitive() {
        PATH_OPEN_SCORE_BYTES
    } else {
        reliable_relay_buffer_len(mux_limits)
    };
    let reinjection_bytes = send_stream.reinjection_bytes().max(floor);
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    reinjection_bytes.min(stream_window)
}

pub(in crate::runtime) fn reliable_relay_bulk_striping_payload_bytes(
    send_stream: &ReliableSendStream,
    mux_limits: MuxLimits,
) -> usize {
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    let decision_quantum =
        adaptive_reliable_relay_chunk_bytes(None, TrafficClass::Throughput, mux_limits)
            .min(reliable_relay_buffer_len(mux_limits))
            .min(stream_window)
            .max(PATH_OPEN_SCORE_BYTES);
    let reinjection_bytes = send_stream.reinjection_bytes();
    if reinjection_bytes == 0 {
        return decision_quantum;
    }
    reinjection_bytes
        .min(decision_quantum)
        .min(stream_window)
        .max(PATH_OPEN_SCORE_BYTES)
}

pub(in crate::runtime) fn reliable_relay_additional_path_open_payload_bytes(
    send_stream: &ReliableSendStream,
    mux_limits: MuxLimits,
) -> usize {
    let proof_ceiling = relay_lane_startup_chunk_bytes(TrafficClass::Latency, mux_limits);
    let proof_payload = reliable_relay_bulk_striping_payload_bytes(send_stream, mux_limits)
        .min(proof_ceiling)
        .max(PATH_OPEN_SCORE_BYTES);
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    proof_payload.min(stream_window).max(PATH_OPEN_SCORE_BYTES)
}

#[cfg(test)]
#[path = "tests_remote.rs"]
mod tests;
