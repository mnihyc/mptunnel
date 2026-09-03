//! Server reliable-stream lifecycle over a QUIC carrier path.

use super::io::{
    UdpPathRecvStream, UdpPathSendStream, spawn_quic_path_reader, udp_path_command_queue,
    udp_path_finish_stream, udp_path_max_stream_payload_bytes, udp_path_write_frame,
    udp_reliable_stream_frame_queue,
};
use super::server_writer::{
    drain_one_server_udp_command_while_input_deferred, drain_server_udp_reliable_commands,
};
use crate::model::capacity::RELIABLE_STREAM_ATTACHMENT_ACCEPT_MAX_OFFSET;
use crate::protocol::{
    Frame, PathId, SessionId, StreamDemandHint, StreamId, StreamReturnPlan, TargetAddr,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommandReceivers, ReliablePathCommandSender, recv_reliable_path_command,
    reliable_path_command_channels, reliable_path_receivers_closed, try_recv_reliable_path_command,
    try_recv_reliable_path_priority_command,
};
use crate::runtime::path::proof::{PathProofTracker, path_proof_ack_frame};
use crate::runtime::path::server_context::ServerPathContext;
use crate::runtime::path::{
    ServerCarrierPathRegistration, ServerStreamOpenOutcome, ServerStreamOpenRequest,
    ServerStreamPathAttachment, ServerStreamPort,
};

#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::{Mutex, OnceLock};
#[cfg(test)]
use tokio::sync::oneshot;

#[cfg(test)]
struct ServerUdpStreamAbortActor {
    attached: oneshot::Sender<()>,
    abort: oneshot::Receiver<()>,
    released: oneshot::Sender<()>,
}

#[cfg(test)]
type ServerUdpStreamAbortRegistry =
    HashMap<(SessionId, StreamId), (u64, ServerUdpStreamAbortActor)>;

#[cfg(test)]
static SERVER_UDP_STREAM_ABORTS: OnceLock<Mutex<ServerUdpStreamAbortRegistry>> = OnceLock::new();

#[cfg(test)]
static NEXT_SERVER_UDP_STREAM_ABORT_ID: AtomicU64 = AtomicU64::new(1);

/// Test-only observation and one-shot abort for one accepted H3 request stream.
///
/// The key is the logical stream, while the effect is deliberately scoped to
/// the currently accepted native request stream. It does not close or replace
/// the shared QUIC connection.
#[cfg(test)]
pub(in crate::runtime) struct ServerUdpStreamAbortHandle {
    key: (SessionId, StreamId),
    id: u64,
    attached: oneshot::Receiver<()>,
    abort: Option<oneshot::Sender<()>>,
    released: oneshot::Receiver<()>,
}

#[cfg(test)]
impl ServerUdpStreamAbortHandle {
    pub(in crate::runtime) async fn wait_attached(&mut self) -> bool {
        (&mut self.attached).await.is_ok()
    }

    pub(in crate::runtime) fn abort(&mut self) {
        if let Some(abort) = self.abort.take() {
            let _ = abort.send(());
        }
    }

    pub(in crate::runtime) async fn wait_released(&mut self) -> bool {
        (&mut self.released).await.is_ok()
    }
}

#[cfg(test)]
impl Drop for ServerUdpStreamAbortHandle {
    fn drop(&mut self) {
        let mut armed = SERVER_UDP_STREAM_ABORTS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .expect("server UDP stream abort registry lock");
        if armed
            .get(&self.key)
            .is_some_and(|(registered_id, _)| *registered_id == self.id)
        {
            armed.remove(&self.key);
        }
    }
}

#[cfg(test)]
pub(in crate::runtime) fn arm_server_udp_stream_abort_for_test(
    session_id: SessionId,
    stream_id: StreamId,
) -> ServerUdpStreamAbortHandle {
    let key = (session_id, stream_id);
    let id = NEXT_SERVER_UDP_STREAM_ABORT_ID.fetch_add(1, Ordering::Relaxed);
    let (attached_tx, attached_rx) = oneshot::channel();
    let (abort_tx, abort_rx) = oneshot::channel();
    let (released_tx, released_rx) = oneshot::channel();
    let replaced = SERVER_UDP_STREAM_ABORTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("server UDP stream abort registry lock")
        .insert(
            key,
            (
                id,
                ServerUdpStreamAbortActor {
                    attached: attached_tx,
                    abort: abort_rx,
                    released: released_tx,
                },
            ),
        );
    assert!(
        replaced.is_none(),
        "a server UDP request-stream abort is already armed for this logical stream"
    );
    ServerUdpStreamAbortHandle {
        key,
        id,
        attached: attached_rx,
        abort: Some(abort_tx),
        released: released_rx,
    }
}

#[cfg(test)]
fn take_server_udp_stream_abort_for_test(
    session_id: SessionId,
    stream_id: StreamId,
) -> Option<ServerUdpStreamAbortActor> {
    SERVER_UDP_STREAM_ABORTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("server UDP stream abort registry lock")
        .remove(&(session_id, stream_id))
        .map(|(_, actor)| actor)
}

pub(super) struct ServerUdpReliableStreamContext {
    pub(super) session_id: SessionId,
    pub(super) path_id: PathId,
    pub(super) path_registration: ServerCarrierPathRegistration,
    pub(super) stream_id: StreamId,
    pub(super) target: TargetAddr,
    pub(super) initial_demand: StreamDemandHint,
    pub(super) return_plan: StreamReturnPlan,
    pub(super) native_rate_authority:
        Option<std::sync::Arc<crate::runtime::path::authority::NativeCarrierRateAuthorityHandle>>,
}

struct ServerUdpReliableOutputDetachGuard {
    streams: ServerStreamPort,
    path_registration: ServerCarrierPathRegistration,
    stream_id: StreamId,
}

impl Drop for ServerUdpReliableOutputDetachGuard {
    fn drop(&mut self) {
        let _ = self
            .streams
            .detach_path(&self.path_registration, self.stream_id);
    }
}

async fn write_udp_stream_accept(
    send: &mut UdpPathSendStream,
    context: &ServerPathContext,
    path_registration: &ServerCarrierPathRegistration,
    stream_id: StreamId,
    path_proofs: &mut PathProofTracker,
) -> Result<(), RuntimeError> {
    udp_path_write_frame(
        send,
        &Frame::StreamMaxData {
            stream_id,
            // The logical receive owner already published this direction's
            // shared credit. Attachment acceptance must not widen it.
            max_offset: RELIABLE_STREAM_ATTACHMENT_ACCEPT_MAX_OFFSET,
        },
        context.codec_limits,
    )
    .await?;

    let Some(challenge) = path_registration.path_validation_challenge(context.mux_limits) else {
        return Ok(());
    };
    udp_path_write_frame(send, &challenge, context.codec_limits).await?;
    path_proofs.record_sent_frame(&challenge);
    Ok(())
}

pub(super) async fn handle_server_udp_reliable_stream(
    mut send: UdpPathSendStream,
    recv: UdpPathRecvStream,
    context: ServerPathContext,
    stream_context: ServerUdpReliableStreamContext,
) -> Result<(), RuntimeError> {
    let ServerUdpReliableStreamContext {
        session_id,
        path_id,
        path_registration,
        stream_id,
        target,
        initial_demand,
        return_plan,
        native_rate_authority,
    } = stream_context;
    let duplicate_open_target = target.clone();
    let (commands_tx, commands_rx) = reliable_path_command_channels(udp_path_command_queue(
        context.mux_limits,
        context.codec_limits,
    ));
    let commands_tx = match native_rate_authority {
        Some(authority) => commands_tx.with_native_rate_authority(authority),
        None => commands_tx,
    };
    let mut path_proofs = PathProofTracker::from_limits(context.mux_limits);
    let accept_existing = match context
        .reliable_streams
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: target.clone(),
            initial_demand,
            return_plan,
            attachment: ServerStreamPathAttachment {
                path_registration: path_registration.clone(),
                commands: commands_tx.clone(),
                max_frame_payload_bytes: udp_path_max_stream_payload_bytes(
                    context.codec_limits,
                    context.mux_limits,
                ),
            },
            mux_limits: context.mux_limits,
        })
        .await?
    {
        ServerStreamOpenOutcome::New(response_lane) => {
            send.set_traffic_class(response_lane)?;
            false
        }
        ServerStreamOpenOutcome::Existing(response_lane) => {
            send.set_traffic_class(response_lane)?;
            true
        }
        ServerStreamOpenOutcome::DuplicateLiveIgnored => {
            udp_path_write_frame(
                &mut send,
                &Frame::StreamDetach { stream_id },
                context.codec_limits,
            )
            .await?;
            let _ = udp_path_finish_stream(&mut send).await;
            return Ok(());
        }
        ServerStreamOpenOutcome::Rejected => {
            udp_path_write_frame(
                &mut send,
                &Frame::StreamDetach { stream_id },
                context.codec_limits,
            )
            .await?;
            let _ = udp_path_finish_stream(&mut send).await;
            return Ok(());
        }
        // Deliberately write no MPP response for a policy drop. Returning
        // retires only this native QUIC request stream, not the connection or
        // any sibling logical flow.
        ServerStreamOpenOutcome::Dropped => {
            let _ = send.cancel_pending_response();
            return Ok(());
        }
    };
    // Arm cleanup only after this native stream owns the attachment. A refused
    // duplicate has no attachment to detach and must leave the existing owner
    // untouched.
    let _output_detach_guard = ServerUdpReliableOutputDetachGuard {
        streams: context.reliable_streams.clone(),
        path_registration: path_registration.clone(),
        stream_id,
    };
    if accept_existing {
        write_udp_stream_accept(
            &mut send,
            &context,
            &path_registration,
            stream_id,
            &mut path_proofs,
        )
        .await?;
    }
    #[cfg(test)]
    if let Some(abort) = take_server_udp_stream_abort_for_test(session_id, stream_id) {
        let _ = abort.attached.send(());
        if abort.abort.await.is_ok() {
            // Drop only this native H3 request-stream attachment. The detach
            // guard removes its logical-path lease; the shared QUIC connection
            // and every sibling request stream remain alive.
            drop(_output_detach_guard);
            let _ = abort.released.send(());
            return Ok(());
        }
    }
    run_server_udp_reliable_stream_loop(
        send,
        recv,
        ServerUdpReliableStreamLoop {
            context,
            session_id,
            path_id,
            path_registration,
            stream_id,
            target: duplicate_open_target,
            commands_tx,
            commands_rx,
            path_proofs,
        },
    )
    .await
}

struct ServerUdpReliableStreamLoop {
    context: ServerPathContext,
    session_id: SessionId,
    path_id: PathId,
    path_registration: ServerCarrierPathRegistration,
    stream_id: StreamId,
    target: TargetAddr,
    commands_tx: ReliablePathCommandSender,
    commands_rx: ReliablePathCommandReceivers,
    path_proofs: PathProofTracker,
}

async fn run_server_udp_reliable_stream_loop(
    mut send: UdpPathSendStream,
    recv: UdpPathRecvStream,
    stream_context: ServerUdpReliableStreamLoop,
) -> Result<(), RuntimeError> {
    let ServerUdpReliableStreamLoop {
        context,
        session_id,
        path_id,
        path_registration,
        stream_id,
        target,
        commands_tx,
        mut commands_rx,
        mut path_proofs,
    } = stream_context;
    let carrier_frame_queue =
        udp_reliable_stream_frame_queue(context.codec_limits, context.mux_limits);
    let mut carrier_frames =
        spawn_quic_path_reader(recv, context.codec_limits, carrier_frame_queue);
    let mut pending_frames = Vec::<Frame>::new();
    let mut deferred_input = None;
    let mut terminal_drain_deadline = None;
    loop {
        // Finishing the server send half must not STOP the client's final
        // feedback. Drain the independent receive half to explicit detach.
        if let Some(deadline) = terminal_drain_deadline {
            let input = tokio::time::timeout_at(deadline, async {
                match deferred_input.take() {
                    Some(input) => Some(input),
                    None => carrier_frames.recv().await,
                }
            })
            .await;
            match input {
                Err(_) | Ok(None) => return Ok(()),
                Ok(Some(Ok(Frame::StreamDetach {
                    stream_id: detach_stream_id,
                }))) if detach_stream_id == stream_id => return Ok(()),
                Ok(Some(Ok(Frame::StreamDetach { .. }))) => {
                    return Err(RuntimeError::Protocol(
                        "QUIC UDP terminal drain stream mismatch",
                    ));
                }
                Ok(Some(Ok(
                    frame @ (Frame::StreamData {
                        stream_id: received_stream_id,
                        ..
                    }
                    | Frame::StreamAck {
                        stream_id: received_stream_id,
                        ..
                    }
                    | Frame::StreamReturnPlanFinal {
                        stream_id: received_stream_id,
                        ..
                    }
                    | Frame::StreamMaxData {
                        stream_id: received_stream_id,
                        ..
                    }
                    | Frame::StreamFin {
                        stream_id: received_stream_id,
                        ..
                    }
                    | Frame::StreamReset {
                        stream_id: received_stream_id,
                        ..
                    }),
                ))) if received_stream_id == stream_id => {
                    context
                        .reliable_streams
                        .route_frame(&path_registration, stream_id, frame)
                        .await?;
                }
                Ok(Some(Ok(
                    Frame::StreamRequalifyData {
                        stream_id: received_stream_id,
                        ..
                    }
                    | Frame::StreamRequalifyAck {
                        stream_id: received_stream_id,
                        ..
                    },
                ))) if received_stream_id == stream_id => {
                    // The exact response half is already finished and
                    // detached. A delayed receipt has no current authority,
                    // and a new probe cannot be acknowledged on this exact
                    // attachment; tolerate either until peer detach.
                }
                Ok(Some(Ok(
                    Frame::StreamData { .. }
                    | Frame::StreamAck { .. }
                    | Frame::StreamReturnPlanFinal { .. }
                    | Frame::StreamRequalifyData { .. }
                    | Frame::StreamRequalifyAck { .. }
                    | Frame::StreamMaxData { .. }
                    | Frame::StreamFin { .. }
                    | Frame::StreamReset { .. },
                ))) => {
                    return Err(RuntimeError::Protocol(
                        "QUIC UDP terminal drain stream mismatch",
                    ));
                }
                Ok(Some(Ok(Frame::PathMetrics { metrics }))) if metrics.path_id == path_id => {
                    context
                        .reliable_streams
                        .record_peer_path_metrics(&path_registration, metrics);
                }
                Ok(Some(Ok(Frame::PathStatus {
                    path_id: status_path_id,
                    sequence,
                    usage,
                }))) if status_path_id == path_id => {
                    context.reliable_streams.record_peer_path_usage(
                        &path_registration,
                        sequence,
                        usage,
                    );
                }
                Ok(Some(Ok(Frame::PathProofAck {
                    path_id: proof_path_id,
                    proof_id,
                    payload_bytes,
                }))) if proof_path_id == path_id => {
                    if let Some(observation) =
                        path_proofs.acknowledge(path_id, proof_id, payload_bytes)
                    {
                        // QUIC owns congestion and capacity evidence; this frame
                        // validates only the response direction of the carrier.
                        context
                            .reliable_streams
                            .record_path_proof_success(&path_registration, observation);
                    }
                }
                // These requests can already be deferred behind the terminal
                // write. The send half is finished, so no reply is possible.
                Ok(Some(Ok(Frame::Ping { .. }))) => {}
                Ok(Some(Ok(Frame::PathProofData {
                    path_id: proof_path_id,
                    ..
                }))) if proof_path_id == path_id => {}
                Ok(Some(Ok(Frame::SessionClose { reason }))) => {
                    context.retire_session(session_id, reason);
                    return Err(RuntimeError::RemoteClosed(reason));
                }
                Ok(Some(Ok(
                    Frame::PathCapacityData { .. }
                    | Frame::PathCapacityFinish { .. }
                    | Frame::PathCapacityReceipt { .. },
                ))) => {
                    return Err(RuntimeError::Protocol(
                        "PATH_CAPACITY frames are not valid on QUIC carriers",
                    ));
                }
                Ok(Some(Ok(_))) => {
                    return Err(RuntimeError::Protocol(
                        "unexpected server QUIC UDP terminal drain frame",
                    ));
                }
                Ok(Some(Err(err))) if super::io::udp_path_input_finished(&err) => return Ok(()),
                Ok(Some(Err(RuntimeError::ReliablePathSessionClosed))) => return Ok(()),
                Ok(Some(Err(err))) => return Err(err),
            }
        }
        let command_may_recv = !reliable_path_receivers_closed(&commands_rx);
        // A deferred exact requalification probe can itself be waiting for a
        // priority-queue slot for its ACK.  Drain that queue before retrying
        // the input; otherwise the stream loop would repeatedly retry the
        // same frame while being the only task able to release its slot.
        if let Some(command) = try_recv_reliable_path_priority_command(&mut commands_rx) {
            let result = if deferred_input.is_some() {
                drain_one_server_udp_command_while_input_deferred(
                    command,
                    &mut commands_rx,
                    &mut send,
                    &context,
                    stream_id,
                    &path_registration,
                    &mut path_proofs,
                )
                .await
            } else {
                drain_server_udp_reliable_commands(
                    command,
                    &mut commands_rx,
                    &mut send,
                    &context,
                    stream_id,
                    path_id,
                    &path_registration,
                    &mut pending_frames,
                    &mut path_proofs,
                    &mut carrier_frames,
                    &mut deferred_input,
                )
                .await
            };
            if result? {
                terminal_drain_deadline =
                    Some(tokio::time::Instant::now() + context.mux_limits.quic_path_idle_timeout);
            }
            continue;
        }
        tokio::select! {
            biased;
            frame = async {
                match deferred_input.take() {
                    Some(input) => Some(input),
                    None => carrier_frames.recv().await,
                }
            } => {
                match frame {
                    Some(Ok(frame @ Frame::StreamRequalifyData {
                        stream_id: received_stream_id,
                        ..
                    })) if received_stream_id == stream_id => {
                        match context.reliable_streams.try_route_frame(
                            &path_registration,
                            stream_id,
                            frame,
                        )? {
                            crate::runtime::path::ServerStreamFrameRoute::Routed => {}
                            crate::runtime::path::ServerStreamFrameRoute::Backpressured(frame) => {
                                deferred_input = Some(Ok(frame));
                            }
                        }
                    }
                    Some(Ok(frame @ (Frame::StreamData { stream_id: received_stream_id, .. }
                        | Frame::StreamAck { stream_id: received_stream_id, .. }
                        | Frame::StreamReturnPlanFinal { stream_id: received_stream_id, .. }
                        | Frame::StreamRequalifyAck { stream_id: received_stream_id, .. }
                        | Frame::StreamMaxData { stream_id: received_stream_id, .. }
                        | Frame::StreamFin { stream_id: received_stream_id, .. }
                        | Frame::StreamReset { stream_id: received_stream_id, .. })))
                        if received_stream_id == stream_id =>
                    {
                        context
                            .reliable_streams
                            .route_frame(&path_registration, stream_id, frame)
                            .await?;
                    }
                    Some(Ok(Frame::StreamDetach { stream_id: detach_stream_id }))
                        if detach_stream_id == stream_id =>
                    {
                        context
                            .reliable_streams
                            .detach_path(&path_registration, stream_id)
                            ?;
                        let _ = udp_path_finish_stream(&mut send).await;
                        return Ok(());
                    }
                    Some(Ok(Frame::PathMetrics { metrics })) if metrics.path_id == path_id => {
                        context.reliable_streams.record_peer_path_metrics(
                            &path_registration,
                            metrics,
                        );
                    }
                    Some(Ok(Frame::OpenStream {
                        stream_id: open_stream_id,
                        target: open_target,
                        demand: open_demand,
                        return_plan: open_return_plan,
                    })) if open_stream_id == stream_id && open_target == target =>
                    {
                        match context
                            .reliable_streams
                            .attach_existing(ServerStreamOpenRequest {
                                session_id,
                                stream_id,
                                target: target.clone(),
                                initial_demand: open_demand,
                                return_plan: open_return_plan,
                                attachment: ServerStreamPathAttachment {
                                    path_registration: path_registration.clone(),
                                    commands: commands_tx.clone(),
                                    max_frame_payload_bytes: udp_path_max_stream_payload_bytes(
                                        context.codec_limits,
                                        context.mux_limits,
                                    ),
                                },
                                mux_limits: context.mux_limits,
                            })
                            .await?
                        {
                            ServerStreamOpenOutcome::Existing(response_lane) => {
                                send.set_traffic_class(response_lane)?;
                                write_udp_stream_accept(
                                    &mut send,
                                    &context,
                                    &path_registration,
                                    stream_id,
                                    &mut path_proofs,
                                )
                                .await?;
                            }
                            ServerStreamOpenOutcome::New(_) => {
                                return Err(RuntimeError::Protocol(
                                    "QUIC UDP path reannouncement opened duplicate stream",
                                ));
                            }
                            ServerStreamOpenOutcome::DuplicateLiveIgnored
                            | ServerStreamOpenOutcome::Rejected => {
                                udp_path_write_frame(
                                    &mut send,
                                    &Frame::StreamDetach { stream_id },
                                    context.codec_limits,
                                )
                                .await?;
                                let _ = udp_path_finish_stream(&mut send).await;
                                return Ok(());
                            }
                            ServerStreamOpenOutcome::Dropped => {
                                let _ = send.cancel_pending_response();
                                return Ok(());
                            }
                        }
                        continue;
                    }
                    Some(Ok(Frame::PathStatus {
                        path_id: status_path_id,
                        sequence,
                        usage,
                    })) if status_path_id == path_id => {
                        context.reliable_streams.record_peer_path_usage(
                            &path_registration,
                            sequence,
                            usage,
                        );
                    }
                    Some(Ok(Frame::PathStatus { .. })) => {
                        return Err(RuntimeError::Protocol(
                            "QUIC path usage advertisement path mismatch",
                        ));
                    }
                    Some(Ok(Frame::Ping { nonce })) => {
                        udp_path_write_frame(&mut send, &Frame::Pong { nonce }, context.codec_limits).await?;
                    }
                    Some(Ok(Frame::PathProofData {
                        path_id: proof_path_id,
                        proof_id,
                        payload,
                    })) if proof_path_id == path_id => {
                        udp_path_write_frame(
                            &mut send,
                            &path_proof_ack_frame(path_id, proof_id, payload.len()),
                            context.codec_limits,
                        )
                        .await?;
                    }
                    Some(Ok(Frame::PathProofAck {
                        path_id: proof_path_id,
                        proof_id,
                        payload_bytes,
                    })) if proof_path_id == path_id => {
                        if let Some(observation) =
                            path_proofs.acknowledge(path_id, proof_id, payload_bytes)
                        {
                            // QUIC owns congestion and capacity evidence; this
                            // frame validates only the response direction.
                            context
                                .reliable_streams
                                .record_path_proof_success(&path_registration, observation);
                        }
                    }
                    Some(Ok(Frame::PathCapacityData { .. }
                        | Frame::PathCapacityFinish { .. }
                        | Frame::PathCapacityReceipt { .. })) => {
                        return Err(RuntimeError::Protocol(
                            "PATH_CAPACITY frames are not valid on QUIC carriers",
                        ));
                    }
                    Some(Ok(Frame::SessionClose { reason })) => {
                        context.retire_session(session_id, reason);
                        return Err(RuntimeError::RemoteClosed(reason));
                    }
                    Some(Ok(frame)) => {
                        crate::observability::process_event!(
                            Warn,
                            "quic",
                            "unexpected_reliable_frame",
                            "unexpected server QUIC reliable carrier frame: stream_id={} frame_kind={}",
                            stream_id.0,
                            frame.kind_name(),
                        );
                        return Err(RuntimeError::Protocol("unexpected server QUIC UDP path reliable stream frame"));
                    }
                    Some(Err(err)) if super::io::udp_path_input_finished(&err) => {
                        context
                            .reliable_streams
                            .detach_path(&path_registration, stream_id)
                            ?;
                        return Ok(());
                    }
                    Some(Err(RuntimeError::ReliablePathSessionClosed)) | None => {
                        context
                            .reliable_streams
                            .detach_path(&path_registration, stream_id)
                            ?;
                        return Ok(());
                    }
                    Some(Err(err)) => return Err(err),
                }
                if let Some(command) = try_recv_reliable_path_command(&mut commands_rx)
                {
                    let result = drain_server_udp_reliable_commands(
                        command,
                        &mut commands_rx,
                        &mut send,
                        &context,
                        stream_id,
                        path_id,
                        &path_registration,
                        &mut pending_frames,
                        &mut path_proofs,
                        &mut carrier_frames,
                        &mut deferred_input,
                    )
                    .await?;
                    if result {
                        terminal_drain_deadline = Some(
                            tokio::time::Instant::now()
                                + context.mux_limits.quic_path_idle_timeout,
                        );
                    }
                }
            }
            command = recv_reliable_path_command(&mut commands_rx), if command_may_recv => {
                if let Some(command) = command {
                    let result = drain_server_udp_reliable_commands(
                        command,
                        &mut commands_rx,
                        &mut send,
                        &context,
                        stream_id,
                        path_id,
                        &path_registration,
                        &mut pending_frames,
                        &mut path_proofs,
                        &mut carrier_frames,
                        &mut deferred_input,
                    ).await;
                    if result? {
                        terminal_drain_deadline = Some(
                            tokio::time::Instant::now()
                                + context.mux_limits.quic_path_idle_timeout,
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "tests_server_stream.rs"]
mod tests;
