//! Reliable client carrier opening.
//!
//! This module owns path reservation, concrete TCP/QUIC open transactions,
//! deadlines, peer acceptance, and retry classification. Successful opens
//! cross into attachment-set ownership through `OpenedRemoteStream`.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::PATH_OPEN_SCORE_BYTES;
use crate::model::path::RelayPathKey;
use crate::model::timing::{
    active_path_open_serialized_exchanges, active_path_open_timeout, path_open_pto,
    transport_pto_from_snapshot,
};
use crate::protocol::{Frame, IngressKind, StreamId, StreamOpenRole, TargetAddr, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::ClientTcpOpenDeadlines;
use crate::runtime::path::{ClientPathContext, RelayPathLoadLease, UdpStreamOpenOptions};
use crate::runtime::stream::ReliablePathStream;
use crate::scheduler::FlowLane;
use std::time::{Duration, Instant};

/// Open carrier awaiting attachment-set commit.
///
/// Keeping stream cleanup and scheduler load in one value makes cancellation,
/// duplicate rejection, and attach-control failure the same rollback path.
pub(in crate::runtime) struct OpenedRemoteStream {
    stream: Option<ReliablePathStream>,
    path_index: usize,
    load_lease: Option<RelayPathLoadLease>,
}

impl OpenedRemoteStream {
    /// Low-level validation opens have no demand lease; higher open owners add
    /// one before any await when the attempt represents product demand.
    pub(in crate::runtime) fn pending(stream: ReliablePathStream, path_index: usize) -> Self {
        Self {
            stream: Some(stream),
            path_index,
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
    ) -> (ReliablePathStream, usize, Option<RelayPathLoadLease>) {
        let stream = self.stream.take().expect("pending remote stream");
        let load_lease = self.load_lease.take();
        (stream, self.path_index, load_lease)
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

#[derive(Clone)]
pub(in crate::runtime) struct ReliableRelayOpenSpec {
    pub(in crate::runtime) target: TargetAddr,
    pub(in crate::runtime) ingress: IngressKind,
}

pub(in crate::runtime) fn udp_relay_attachment_open_options(
    role: StreamOpenRole,
) -> UdpStreamOpenOptions {
    UdpStreamOpenOptions {
        wait_for_accept: role != StreamOpenRole::Validation,
        role,
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
    lane: FlowLane,
    payload_bytes: usize,
    attempted: &mut Vec<RelayPathKey>,
) -> Result<Option<ReliableInitialOpenAttempt>, RuntimeError> {
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

pub(in crate::runtime) fn mark_reliable_initial_open_retryable_failure(
    context: &ClientPathContext,
    key: RelayPathKey,
) {
    context.mark_relay_path_data_plane_failure(key.underlay, key.index);
}

async fn open_reliable_initial_active_attempt(
    context: &ClientPathContext,
    attempt: ReliableInitialOpenAttempt,
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
    has_unattempted_alternative: bool,
) -> Result<OpenedRemoteStream, RuntimeError> {
    let ReliableInitialOpenAttempt {
        key,
        stream_id,
        load_lease,
    } = attempt;
    match key.underlay {
        UnderlayProtocol::Tcp => {
            let open_timeout =
                reliable_initial_active_open_timeout(context, key, has_unattempted_alternative);
            let open_deadlines =
                ClientTcpOpenDeadlines::fixed(tokio::time::Instant::now() + open_timeout);
            match open_remote_stream_on_preselected_tcp_path(
                context,
                stream_id,
                target,
                ingress,
                lane,
                key.index,
                StreamOpenRole::Active,
                open_deadlines,
            )
            .await
            {
                Ok(opened) => Ok(opened.with_load_lease(load_lease)),
                Err(RuntimeError::PathOpenTimedOut) => {
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "reliable_stream_open_timeout",
                        format_args!(
                            "stream_id={} underlay=tcp path_index={} lane={:?} role={:?} timeout_ms={}",
                            stream_id.0,
                            key.index,
                            lane,
                            StreamOpenRole::Active,
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
                reliable_initial_active_open_timeout(context, key, has_unattempted_alternative);
            let open_deadline = tokio::time::Instant::now() + open_timeout;
            match relay_path_open_with_deadline(
                open_deadline,
                open_remote_stream_on_preselected_udp_path(
                    context,
                    stream_id,
                    target,
                    ingress,
                    lane,
                    key.index,
                    UdpStreamOpenOptions::ACTIVE_WAIT,
                    open_deadline,
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
                            "stream_id={} underlay=udp path_index={} lane={:?} role={:?} timeout_ms={}",
                            stream_id.0,
                            key.index,
                            lane,
                            StreamOpenRole::Active,
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
    ingress: IngressKind,
    lane: FlowLane,
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
        match open_reliable_initial_active_attempt(
            context,
            attempt,
            target.clone(),
            ingress,
            lane,
            has_unattempted_alternative,
        )
        .await
        {
            Ok(opened) => return Ok(opened),
            Err(err) if relay_path_open_error_is_retryable(key.underlay, &err) => {
                mark_reliable_initial_open_retryable_failure(context, key);
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
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
    path_index: usize,
    role: StreamOpenRole,
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
        target,
        ingress,
        lane,
        path_index,
        role,
        open_deadlines,
    )
    .await;
    match open_result {
        Ok(opened) => Ok(opened.with_load_lease(load_lease)),
        Err(err) if !matches!(err, RuntimeError::PathOpenTimedOut) => Err(err),
        Err(RuntimeError::PathOpenTimedOut) => {
            context.mark_tcp_path_data_plane_failure(path_index);
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "reliable_stream_open_timeout",
                format_args!(
                    "stream_id={} underlay=tcp path_index={} lane={:?} role={:?} live_timeout_ms={} setup_timeout_ms={}",
                    stream_id.0,
                    path_index,
                    lane,
                    role,
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
            path_open_pto(snapshot, false)
                .saturating_mul(active_path_open_serialized_exchanges(snapshot))
        }
        UnderlayProtocol::Udp => live,
    };
    ReliableRelayAttachOpenTimeouts {
        live,
        setup: setup.max(live),
    }
}

pub(in crate::runtime) fn reliable_initial_active_open_timeout(
    context: &ClientPathContext,
    key: RelayPathKey,
    has_unattempted_alternative: bool,
) -> Duration {
    let snapshot = context.reliable_path_snapshot(key);
    let rtt_is_observed =
        key.underlay == UnderlayProtocol::Udp && context.reliable_path_rtt_is_observed(key);
    if has_unattempted_alternative {
        path_open_pto(snapshot, rtt_is_observed)
            .saturating_mul(active_path_open_serialized_exchanges(snapshot))
    } else {
        active_path_open_timeout(snapshot, rtt_is_observed)
    }
}

pub(in crate::runtime) async fn open_remote_stream_on_preselected_tcp_path(
    context: &ClientPathContext,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
    path_index: usize,
    role: StreamOpenRole,
    open_deadlines: ClientTcpOpenDeadlines,
) -> Result<OpenedRemoteStream, RuntimeError> {
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "reliable_stream_open_attempt",
        format_args!(
            "stream_id={} underlay=tcp path_index={} lane={:?} role={:?} wait_for_accept=true tcp_paths={} udp_paths={}",
            stream_id.0,
            path_index,
            lane,
            role,
            context.tcp_paths.len(),
            context.udp_paths.len(),
        ),
    );
    let started_at = Instant::now();
    let opened = context
        .tcp_sessions
        .get(path_index)
        .ok_or(RuntimeError::NoSchedulableTcpPath)?
        .open_stream_with_deadlines(stream_id, target, ingress, lane, role, open_deadlines)
        .await?;
    let pending = OpenedRemoteStream::pending(
        ReliablePathStream::from_opened_carrier(opened.carrier),
        path_index,
    );
    tokio::time::timeout_at(
        opened.open_deadline,
        send_open_path_metrics(context, pending.stream(), UnderlayProtocol::Tcp, path_index),
    )
    .await
    .map_err(|_| RuntimeError::PathOpenTimedOut)??;
    let elapsed = started_at.elapsed();
    context.mark_tcp_path_reserved_open_success(path_index, elapsed);
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "reliable_stream_open_success",
        format_args!(
            "stream_id={} underlay=tcp path_index={} lane={:?} role={:?} elapsed_ms={:.3}",
            stream_id.0,
            path_index,
            lane,
            role,
            elapsed.as_secs_f64() * 1000.0,
        ),
    );
    Ok(pending)
}

pub(in crate::runtime) async fn open_remote_stream_on_udp_path(
    context: &ClientPathContext,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
    path_index: usize,
    options: UdpStreamOpenOptions,
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
            target,
            ingress,
            lane,
            path_index,
            options,
            open_deadline,
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
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
    key: RelayPathKey,
    role: StreamOpenRole,
) -> Result<OpenedRemoteStream, RuntimeError> {
    match key.underlay {
        UnderlayProtocol::Tcp => {
            open_remote_stream_on_path(context, stream_id, target, ingress, lane, key.index, role)
                .await
        }
        UnderlayProtocol::Udp => {
            open_remote_stream_on_udp_path(
                context,
                stream_id,
                target,
                ingress,
                lane,
                key.index,
                udp_relay_attachment_open_options(role),
            )
            .await
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
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
    path_index: usize,
    options: UdpStreamOpenOptions,
    open_deadline: tokio::time::Instant,
) -> Result<OpenedRemoteStream, RuntimeError> {
    let UdpStreamOpenOptions {
        wait_for_accept,
        role,
    } = options;
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "reliable_stream_open_attempt",
        format_args!(
            "stream_id={} underlay=udp path_index={} lane={:?} role={:?} wait_for_accept={} tcp_paths={} udp_paths={}",
            stream_id.0,
            path_index,
            lane,
            role,
            wait_for_accept,
            context.tcp_paths.len(),
            context.udp_paths.len(),
        ),
    );
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = (wait_for_accept, role);
    let started_at = Instant::now();
    let carrier = context
        .udp_sessions
        .get(path_index)
        .ok_or(RuntimeError::NoSchedulableUdpPath)?
        .open_stream(stream_id, target, ingress, lane, options, open_deadline)
        .await?;
    let pending =
        OpenedRemoteStream::pending(ReliablePathStream::from_opened_carrier(carrier), path_index);
    let elapsed = started_at.elapsed();
    context.mark_udp_stream_reserved_open_success(path_index, elapsed, wait_for_accept);
    send_open_path_metrics(context, pending.stream(), UnderlayProtocol::Udp, path_index).await?;
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "reliable_stream_open_success",
        format_args!(
            "stream_id={} underlay=udp path_index={} lane={:?} role={:?} wait_for_accept={} elapsed_ms={:.3}",
            stream_id.0,
            path_index,
            lane,
            role,
            wait_for_accept,
            elapsed.as_secs_f64() * 1000.0,
        ),
    );
    Ok(pending)
}

async fn send_open_path_metrics(
    context: &ClientPathContext,
    stream: &ReliablePathStream,
    underlay: UnderlayProtocol,
    path_index: usize,
) -> Result<(), RuntimeError> {
    let Some(metrics) = context.relay_path_metrics(underlay, path_index) else {
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
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::ReliablePathSessionClosed
            | RuntimeError::PathHeartbeatTimeout
            | RuntimeError::PathOpenTimedOut
            | RuntimeError::Protocol(_)
    )
}

pub(in crate::runtime) fn udp_stream_open_error_is_path_retryable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Udp(_)
            | RuntimeError::QuicCarrier(_)
            | RuntimeError::Auth(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::ReliablePathSessionClosed
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

pub(in crate::runtime) fn relay_error_is_tcp_path_failure<T>(
    result: &Result<T, RuntimeError>,
) -> bool {
    matches!(
        result,
        Err(RuntimeError::PathHeartbeatTimeout)
            | Err(RuntimeError::PathOpenTimedOut)
            | Err(RuntimeError::ReliablePathSessionClosed)
            | Err(RuntimeError::Tcp(_))
            | Err(RuntimeError::Encrypted(_))
            | Err(RuntimeError::RemoteClosed(_))
            | Err(RuntimeError::Protocol(_))
    )
}

#[cfg(test)]
#[path = "open_test.rs"]
mod tests;
