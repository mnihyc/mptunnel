//! Reliable client carrier opening.
//!
//! This module owns path reservation, concrete TCP/QUIC open transactions,
//! deadlines, peer acceptance, and retry classification. Successful opens
//! cross into attachment-set ownership through `OpenedRemoteStream`.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, RELIABLE_STREAM_ATTACHMENT_ACCEPT_MAX_OFFSET,
    reliable_stream_initial_advertised_window_bytes,
};
use crate::model::path::RelayPathKey;
use crate::model::timing::{
    path_open_pto, path_open_serialized_exchanges, path_open_timeout, transport_pto_from_snapshot,
};
use crate::protocol::{
    Frame, PathMetrics, StreamDemandHint, StreamId, TargetAddr, UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::ClientTcpOpenDeadlines;
use crate::runtime::path::quic::client::{ClientUdpErrorDisposition, client_udp_error_disposition};
use crate::runtime::path::{ClientPathContext, RelayPathLoadLease};
use crate::runtime::stream::{OpenedRemoteStream, ReliablePathStream};
use crate::scheduler::{TrafficClass, stream_demand_hint_for_traffic_class};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub(in crate::runtime) struct ReliableRelayOpenSpec {
    pub(in crate::runtime) target: TargetAddr,
    pub(in crate::runtime) initial_demand: StreamDemandHint,
}

impl ReliableRelayOpenSpec {
    pub(in crate::runtime) fn new(target: TargetAddr, initial_lane: TrafficClass) -> Self {
        Self {
            target,
            initial_demand: stream_demand_hint_for_traffic_class(initial_lane),
        }
    }
}

/// A selected initial carrier whose scheduler load remains owned across I/O.
pub(in crate::runtime) struct ReliableInitialOpenAttempt {
    pub(in crate::runtime) key: RelayPathKey,
    pub(in crate::runtime) stream_id: StreamId,
    load_lease: RelayPathLoadLease,
}

pub(in crate::runtime) fn reserve_reliable_initial_open_attempt(
    context: &ClientPathContext,
    lane: TrafficClass,
    payload_bytes: usize,
    attempted: &mut Vec<RelayPathKey>,
) -> Result<Option<ReliableInitialOpenAttempt>, RuntimeError> {
    #[cfg(test)]
    context.record_reliable_selection_pass_for_test();
    let candidate_count = context
        .tcp_paths
        .len()
        .saturating_add(context.udp_paths.len());
    if attempted.len() >= candidate_count {
        return Ok(None);
    }
    let Some(load_lease) = context.reserve_reliable_stream_path(lane, payload_bytes, attempted)
    else {
        return Ok(None);
    };
    let key = load_lease.key();
    match context.allocate_reliable_stream_id() {
        Ok(stream_id) => {
            attempted.push(key);
            Ok(Some(ReliableInitialOpenAttempt {
                key,
                stream_id,
                load_lease,
            }))
        }
        Err(err) => Err(err),
    }
}

async fn open_reliable_initial_attempt(
    context: &ClientPathContext,
    attempt: ReliableInitialOpenAttempt,
    target: TargetAddr,
    lane: TrafficClass,
    has_unattempted_alternative: bool,
) -> Result<OpenedRemoteStream, RuntimeError> {
    let spec = ReliableRelayOpenSpec::new(target, lane);
    let ReliableInitialOpenAttempt {
        key,
        stream_id,
        load_lease,
    } = attempt;
    match key.underlay {
        UnderlayProtocol::Tcp => {
            let open_timeout =
                reliable_initial_open_timeout(context, key, has_unattempted_alternative);
            let open_deadlines =
                ClientTcpOpenDeadlines::fixed(tokio::time::Instant::now() + open_timeout);
            match open_remote_stream_on_preselected_tcp_path(
                context,
                stream_id,
                &spec,
                lane,
                key.index,
                open_deadlines,
                reliable_stream_initial_advertised_window_bytes(
                    key.underlay,
                    lane,
                    context.mux_limits,
                ),
            )
            .await
            {
                Ok(opened) => Ok(opened.with_load_lease(load_lease)),
                Err(RuntimeError::PathOpenTimedOut) => {
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "reliable_stream_open_timeout",
                        format_args!(
                            "stream_id={} underlay=tcp path_index={} lane={:?} timeout_ms={}",
                            stream_id.0,
                            key.index,
                            lane,
                            open_timeout.as_millis(),
                        ),
                    );
                    Err(RuntimeError::PathOpenTimedOut)
                }
                Err(err) => Err(err),
            }
        }
        UnderlayProtocol::Udp => {
            let open_timeout =
                reliable_initial_open_timeout(context, key, has_unattempted_alternative);
            let open_deadline = tokio::time::Instant::now() + open_timeout;
            match relay_path_open_with_deadline(
                open_deadline,
                open_remote_stream_on_preselected_udp_path(
                    context,
                    stream_id,
                    &spec,
                    lane,
                    key.index,
                    open_deadline,
                    reliable_stream_initial_advertised_window_bytes(
                        key.underlay,
                        lane,
                        context.mux_limits,
                    ),
                ),
            )
            .await
            {
                Ok(opened) => Ok(opened.with_load_lease(load_lease)),
                Err(RuntimeError::PathOpenTimedOut) => {
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "reliable_stream_open_timeout",
                        format_args!(
                            "stream_id={} underlay=udp path_index={} lane={:?} timeout_ms={}",
                            stream_id.0,
                            key.index,
                            lane,
                            open_timeout.as_millis(),
                        ),
                    );
                    Err(RuntimeError::PathOpenTimedOut)
                }
                Err(err) => Err(err),
            }
        }
    }
}

pub(in crate::runtime) async fn open_remote_stream(
    context: &ClientPathContext,
    target: TargetAddr,
    lane: TrafficClass,
) -> Result<OpenedRemoteStream, RuntimeError> {
    context
        .complete_session_operation(open_remote_stream_active(context, target, lane))
        .await
}

async fn open_remote_stream_active(
    context: &ClientPathContext,
    target: TargetAddr,
    lane: TrafficClass,
) -> Result<OpenedRemoteStream, RuntimeError> {
    let mut attempted = Vec::new();
    let mut last_retryable_error = None;
    while let Some(attempt) =
        reserve_reliable_initial_open_attempt(context, lane, PATH_OPEN_SCORE_BYTES, &mut attempted)?
    {
        let key = attempt.key;
        let has_unattempted_alternative = context
            .ordered_reliable_path_keys(lane, PATH_OPEN_SCORE_BYTES)
            .into_iter()
            .any(|candidate| !attempted.contains(&candidate));
        match open_reliable_initial_attempt(
            context,
            attempt,
            target.clone(),
            lane,
            has_unattempted_alternative,
        )
        .await
        {
            Ok(opened) => return Ok(opened),
            Err(err @ RuntimeError::ReliablePathAttachmentRefused) => {
                // Refusal is scoped to this stream attachment. Try another
                // candidate without withdrawing the healthy carrier.
                last_retryable_error = Some(err);
            }
            Err(err) if relay_path_open_error_is_retryable(key.underlay, &err) => {
                last_retryable_error = Some(err);
            }
            Err(err) => return Err(err),
        }
    }
    Err(last_retryable_error.unwrap_or_else(|| no_schedulable_reliable_path_error(context)))
}

pub(in crate::runtime) async fn open_remote_stream_on_path(
    context: &ClientPathContext,
    stream_id: StreamId,
    spec: &ReliableRelayOpenSpec,
    lane: TrafficClass,
    path_index: usize,
) -> Result<OpenedRemoteStream, RuntimeError> {
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: path_index,
    };
    let load_lease = context
        .reserve_relay_path_load(key, lane)
        .ok_or(RuntimeError::NoSchedulableTcpPath)?;
    let open_timeouts = reliable_relay_attach_open_timeouts(context, key);
    let open_deadlines = ClientTcpOpenDeadlines::from_timeouts(
        tokio::time::Instant::now(),
        open_timeouts.live,
        open_timeouts.setup,
    );
    let open_result = open_remote_stream_on_preselected_tcp_path(
        context,
        stream_id,
        spec,
        lane,
        path_index,
        open_deadlines,
        RELIABLE_STREAM_ATTACHMENT_ACCEPT_MAX_OFFSET,
    )
    .await;
    match open_result {
        Ok(opened) => Ok(opened.with_load_lease(load_lease)),
        Err(err) if !matches!(err, RuntimeError::PathOpenTimedOut) => Err(err),
        Err(RuntimeError::PathOpenTimedOut) => {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "reliable_stream_open_timeout",
                format_args!(
                    "stream_id={} underlay=tcp path_index={} lane={:?} live_timeout_ms={} setup_timeout_ms={}",
                    stream_id.0,
                    path_index,
                    lane,
                    open_timeouts.live.as_millis(),
                    open_timeouts.setup.as_millis(),
                ),
            );
            Err(RuntimeError::PathOpenTimedOut)
        }
        Err(err) => Err(err),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ReliableRelayAttachOpenTimeouts {
    pub(in crate::runtime) live: Duration,
    pub(in crate::runtime) setup: Duration,
}

pub(in crate::runtime) fn reliable_relay_attach_open_timeouts(
    context: &ClientPathContext,
    key: RelayPathKey,
) -> ReliableRelayAttachOpenTimeouts {
    let snapshot = context.reliable_path_snapshot(key);
    let live = transport_pto_from_snapshot(snapshot);
    let setup = match key.underlay {
        UnderlayProtocol::Tcp => {
            // A cold lane-class actor owns carrier dial, authenticated path
            // join, and product open. A live association owns only the last.
            path_open_pto(snapshot, false).saturating_mul(path_open_serialized_exchanges(snapshot))
        }
        UnderlayProtocol::Udp => {
            // A cold QUIC attachment owns transport establishment, path
            // authentication, and product stream acceptance. Native RTT
            // evidence prices those serialized exchanges without relying on
            // periodic probes to keep a product connection prewarmed.
            path_open_pto(snapshot, context.reliable_path_rtt_is_observed(key))
                .saturating_mul(path_open_serialized_exchanges(snapshot))
        }
    };
    ReliableRelayAttachOpenTimeouts {
        live,
        setup: setup.max(live),
    }
}

pub(in crate::runtime) fn reliable_initial_open_timeout(
    context: &ClientPathContext,
    key: RelayPathKey,
    has_unattempted_alternative: bool,
) -> Duration {
    let snapshot = context.reliable_path_snapshot(key);
    let rtt_is_observed =
        key.underlay == UnderlayProtocol::Udp && context.reliable_path_rtt_is_observed(key);
    if has_unattempted_alternative {
        path_open_pto(snapshot, rtt_is_observed)
            .saturating_mul(path_open_serialized_exchanges(snapshot))
    } else {
        path_open_timeout(snapshot, rtt_is_observed)
    }
}

pub(in crate::runtime) async fn open_remote_stream_on_preselected_tcp_path(
    context: &ClientPathContext,
    stream_id: StreamId,
    spec: &ReliableRelayOpenSpec,
    lane: TrafficClass,
    path_index: usize,
    open_deadlines: ClientTcpOpenDeadlines,
    advertised_recv_max_offset: u64,
) -> Result<OpenedRemoteStream, RuntimeError> {
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "reliable_stream_open_attempt",
        format_args!(
            "stream_id={} underlay=tcp path_index={} lane={:?} wait_for_accept=true tcp_paths={} udp_paths={}",
            stream_id.0,
            path_index,
            lane,
            context.tcp_paths.len(),
            context.udp_paths.len(),
        ),
    );
    let started_at = Instant::now();
    let opened = context
        .tcp_sessions
        .get(path_index)
        .ok_or(RuntimeError::NoSchedulableTcpPath)?
        .open_stream_with_deadlines(
            stream_id,
            spec.target.clone(),
            lane,
            spec.initial_demand,
            open_deadlines,
            advertised_recv_max_offset,
        )
        .await?;
    let path_instance_id = opened.carrier.path_instance_id;
    let path_metrics = opened.path_metrics;
    let open_deadline = opened.open_deadline;
    let pending = OpenedRemoteStream::from_opened_carrier(
        opened.carrier,
        path_index,
        advertised_recv_max_offset,
    );
    #[cfg(test)]
    context
        .pause_reliable_tcp_settlement_for_test(path_index)
        .await;
    tokio::time::timeout_at(
        open_deadline,
        send_open_path_metrics(pending.stream(), Some(path_metrics)),
    )
    .await
    .map_err(|_| RuntimeError::PathOpenTimedOut)??;
    let elapsed = started_at.elapsed();
    context.mark_tcp_path_reserved_open_success_for_instance(path_index, path_instance_id, elapsed);
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "reliable_stream_open_success",
        format_args!(
            "stream_id={} underlay=tcp path_index={} lane={:?} elapsed_ms={:.3}",
            stream_id.0,
            path_index,
            lane,
            elapsed.as_secs_f64() * 1000.0,
        ),
    );
    Ok(pending)
}

pub(in crate::runtime) async fn open_remote_stream_on_udp_path(
    context: &ClientPathContext,
    stream_id: StreamId,
    spec: &ReliableRelayOpenSpec,
    lane: TrafficClass,
    path_index: usize,
) -> Result<OpenedRemoteStream, RuntimeError> {
    if context.udp_paths.get(path_index).is_none() {
        return Err(RuntimeError::NoSchedulableUdpPath);
    }
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: path_index,
    };
    let load_lease = context
        .reserve_relay_path_load(key, lane)
        .ok_or(RuntimeError::NoSchedulableUdpPath)?;
    let open_timeout = reliable_relay_attach_open_timeouts(context, key).setup;
    let open_deadline = tokio::time::Instant::now() + open_timeout;
    match relay_path_open_with_deadline(
        open_deadline,
        open_remote_stream_on_preselected_udp_path(
            context,
            stream_id,
            spec,
            lane,
            path_index,
            open_deadline,
            RELIABLE_STREAM_ATTACHMENT_ACCEPT_MAX_OFFSET,
        ),
    )
    .await
    {
        Ok(opened) => Ok(opened.with_load_lease(load_lease)),
        Err(err) => Err(err),
    }
}

/// Selects the concrete carrier open transaction after attachment policy has
/// chosen an exact path. Retry and membership decisions remain with the caller.
pub(in crate::runtime) async fn open_remote_stream_for_relay_path(
    context: &ClientPathContext,
    stream_id: StreamId,
    spec: &ReliableRelayOpenSpec,
    lane: TrafficClass,
    key: RelayPathKey,
) -> Result<OpenedRemoteStream, RuntimeError> {
    match key.underlay {
        UnderlayProtocol::Tcp => {
            open_remote_stream_on_path(context, stream_id, spec, lane, key.index).await
        }
        UnderlayProtocol::Udp => {
            open_remote_stream_on_udp_path(context, stream_id, spec, lane, key.index).await
        }
    }
}

pub(in crate::runtime) async fn relay_path_open_with_deadline<T, F>(
    open_deadline: tokio::time::Instant,
    open: F,
) -> Result<T, RuntimeError>
where
    F: std::future::Future<Output = Result<T, RuntimeError>>,
{
    match tokio::time::timeout_at(open_deadline, open).await {
        Ok(result) => result,
        Err(_) => Err(RuntimeError::PathOpenTimedOut),
    }
}

pub(in crate::runtime) async fn open_remote_stream_on_preselected_udp_path(
    context: &ClientPathContext,
    stream_id: StreamId,
    spec: &ReliableRelayOpenSpec,
    lane: TrafficClass,
    path_index: usize,
    open_deadline: tokio::time::Instant,
    advertised_recv_max_offset: u64,
) -> Result<OpenedRemoteStream, RuntimeError> {
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "reliable_stream_open_attempt",
        format_args!(
            "stream_id={} underlay=udp path_index={} lane={:?} wait_for_accept=true tcp_paths={} udp_paths={}",
            stream_id.0,
            path_index,
            lane,
            context.tcp_paths.len(),
            context.udp_paths.len(),
        ),
    );
    let started_at = Instant::now();
    let carrier = context
        .udp_sessions
        .get(path_index)
        .ok_or(RuntimeError::NoSchedulableUdpPath)?
        .open_stream(
            stream_id,
            spec.target.clone(),
            lane,
            spec.initial_demand,
            open_deadline,
            advertised_recv_max_offset,
        )
        .await?;
    // The handle has already atomically committed this exact carrier owner.
    let path_instance_id = carrier.path_instance_id;
    let pending =
        OpenedRemoteStream::from_opened_carrier(carrier, path_index, advertised_recv_max_offset);
    let elapsed = started_at.elapsed();
    let _ = context.mark_udp_stream_reserved_open_success_for_instance(
        path_index,
        path_instance_id,
        elapsed,
        true,
    );
    send_open_path_metrics(
        pending.stream(),
        context.relay_path_metrics(UnderlayProtocol::Udp, path_index),
    )
    .await?;
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "reliable_stream_open_success",
        format_args!(
            "stream_id={} underlay=udp path_index={} lane={:?} wait_for_accept=true elapsed_ms={:.3}",
            stream_id.0,
            path_index,
            lane,
            elapsed.as_secs_f64() * 1000.0,
        ),
    );
    Ok(pending)
}

async fn send_open_path_metrics(
    stream: &ReliablePathStream,
    metrics: Option<PathMetrics>,
) -> Result<(), RuntimeError> {
    let Some(metrics) = metrics else {
        return Ok(());
    };
    stream.try_enqueue_request_control_frame(Frame::PathMetrics { metrics })
}

pub(in crate::runtime) fn stream_open_error_is_path_retryable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Tcp(_)
            | RuntimeError::Encrypted(_)
            | RuntimeError::Auth(_)
            | RuntimeError::RemotePathClosed(_)
            | RuntimeError::ReliablePathSessionClosed
            | RuntimeError::PathHeartbeatTimeout
            | RuntimeError::PathOpenTimedOut
            | RuntimeError::Protocol(_)
    )
}

pub(in crate::runtime) fn udp_stream_open_error_is_path_retryable(err: &RuntimeError) -> bool {
    if client_udp_error_disposition(err) == ClientUdpErrorDisposition::Session {
        return false;
    }
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Udp(_)
            | RuntimeError::QuicCarrier(_)
            | RuntimeError::Auth(_)
            | RuntimeError::RemotePathClosed(_)
            | RuntimeError::ReliablePathSessionClosed
            | RuntimeError::ReliablePathRetired
            | RuntimeError::PathHeartbeatTimeout
            | RuntimeError::PathOpenTimedOut
            | RuntimeError::Protocol(_)
    )
}

/// Classifies retryability beside the concrete TCP and QUIC open contracts.
pub(in crate::runtime) fn relay_path_open_error_is_retryable(
    underlay: UnderlayProtocol,
    err: &RuntimeError,
) -> bool {
    match underlay {
        UnderlayProtocol::Tcp => stream_open_error_is_path_retryable(err),
        UnderlayProtocol::Udp => udp_stream_open_error_is_path_retryable(err),
    }
}

pub(in crate::runtime) fn no_schedulable_reliable_path_error(
    context: &ClientPathContext,
) -> RuntimeError {
    if !context.tcp_paths.is_empty() && !context.udp_paths.is_empty() {
        RuntimeError::NoSchedulableReliablePath
    } else if !context.tcp_paths.is_empty() {
        RuntimeError::NoSchedulableTcpPath
    } else {
        RuntimeError::NoSchedulableUdpPath
    }
}

#[cfg(test)]
#[path = "tests_open.rs"]
mod tests;
