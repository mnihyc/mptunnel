//! Client relay carrier acquisition and attachment policy.
//!
//! This module ranks and opens concrete carrier candidates. Request-stream
//! membership, generations, scheduler leases, and frame fan-in stay in
//! `stream::request::attachment`.

use super::open::{
    ReliableRelayOpenSpec, no_schedulable_reliable_path_error, open_remote_stream_for_relay_path,
    relay_path_open_error_is_retryable,
};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, adaptive_reliable_relay_chunk_bytes, relay_lane_startup_chunk_bytes,
    reliable_relay_buffer_len,
};
use crate::model::path::RelayPathKey;
use crate::mux::MuxLimits;
use crate::mux::stream::ReliableSendStream;
use crate::protocol::{Frame, UnderlayProtocol};
use crate::runtime::error::{RuntimeError, reliable_path_error_is_migratable};
use crate::runtime::path::ClientPathContext;
use crate::runtime::stream::{
    ReliablePathStream, ReliableRelayAttachOutcome, ReliableRelayRemoteSet,
};
use crate::scheduler::TrafficClass;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) enum ReliableRelayAttachMode {
    Any,
    BulkStriping,
    Recovery,
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
    lane: TrafficClass,
    send_stream: &'a ReliableSendStream,
    resend_fin: bool,
    candidates: Vec<RelayPathKey>,
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
            request.lane,
            key,
        )
        .await
        {
            Ok(opened) => {
                let attach_control_result = send_request_attach_control_frames(
                    opened.stream(),
                    request.send_stream,
                    request.resend_fin,
                );
                match attach_control_result {
                    Ok(()) => {
                        let attach_outcome = remotes.attach_candidate(opened);
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
                    Err(err) if reliable_path_error_is_migratable(&err) => {
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

#[allow(clippy::too_many_arguments)]
pub(in crate::runtime) async fn attach_reliable_relay_paths(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    lane: TrafficClass,
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
    lane: TrafficClass,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: ReliableRelayAttachMode,
    recovery_excluded_paths: &mut HashSet<RelayPathKey>,
    inflight_path_claims: &HashSet<RelayPathKey>,
) -> Result<usize, RuntimeError> {
    let payload_bytes = match mode {
        ReliableRelayAttachMode::Any | ReliableRelayAttachMode::Recovery => {
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
                    inflight_path_claims,
                ),
            },
        )
        .await;
        match result {
            Ok(result) if result.attached > 0 || !remotes.is_empty() => {
                return Ok(result.attached);
            }
            Ok(_) => {}
            Err(err) => return Err(err),
        }
    }
    let prefer_reinjection_alternative = matches!(mode, ReliableRelayAttachMode::Recovery)
        || reliable_relay_should_open_reinjection_alternative(lane, send_stream, resend_fin, mode);
    if prefer_reinjection_alternative {
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
                        reliable_relay_reinjection_path_candidates(
                            context,
                            remotes,
                            lane,
                            payload_bytes,
                        ),
                        recovery_excluded_paths,
                        remotes.is_empty(),
                    ),
                    inflight_path_claims,
                ),
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
                    reliable_relay_additional_path_candidates(
                        context,
                        remotes,
                        lane,
                        payload_bytes,
                    ),
                    recovery_excluded_paths,
                    remotes.is_empty(),
                ),
                inflight_path_claims,
            ),
        },
    )
    .await?;
    Ok(result.attached)
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
#[path = "remote_test.rs"]
mod tests;
