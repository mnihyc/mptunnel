//! Server reliable-stream lifecycle over a QUIC carrier path.

use super::io::{
    UdpPathRecvStream, UdpPathSendStream, spawn_quic_path_reader, udp_path_command_queue,
    udp_path_finish_stream, udp_path_max_stream_payload_bytes, udp_path_write_frame,
    udp_reliable_stream_frame_queue,
};
use super::server_writer::drain_server_udp_reliable_commands;
use crate::model::capacity::reliable_stream_initial_advertised_window_bytes;
use crate::protocol::{
    Frame, PathId, PathMetricDirection, ResetReason, SessionId, StreamId, TargetAddr,
    UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommandReceivers, ReliablePathCommandSender, recv_reliable_path_command,
    reliable_path_command_channels, reliable_path_receivers_closed, try_recv_reliable_path_command,
    try_recv_reliable_path_priority_command,
};
use crate::runtime::path::proof::{PathProofTracker, path_proof_ack_frame, path_proof_metrics};
use crate::runtime::path::server_context::ServerPathContext;
use crate::runtime::path::{
    ServerCarrierPathRegistration, ServerStreamOpenOutcome, ServerStreamOpenRequest,
    ServerStreamPathAttachment, ServerStreamPort,
};
use crate::scheduler::{TrafficClass, traffic_class_from_stream_demand_hint};

pub(super) struct ServerUdpReliableStreamContext {
    pub(super) session_id: SessionId,
    pub(super) path_id: PathId,
    pub(super) path_registration: ServerCarrierPathRegistration,
    pub(super) stream_id: StreamId,
    pub(super) target: TargetAddr,
    pub(super) lane: TrafficClass,
}

struct ServerUdpReliableOutputDetachGuard {
    streams: ServerStreamPort,
    session_id: SessionId,
    stream_id: StreamId,
    path_id: PathId,
    commands: ReliablePathCommandSender,
}

impl Drop for ServerUdpReliableOutputDetachGuard {
    fn drop(&mut self) {
        self.streams.detach_path(
            self.session_id,
            self.stream_id,
            UnderlayProtocol::Udp,
            self.path_id,
            &self.commands,
        );
    }
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
        lane,
    } = stream_context;
    context.reliable_streams.validate_target(&target)?;
    let duplicate_open_target = target.clone();
    let (commands_tx, commands_rx) = reliable_path_command_channels(udp_path_command_queue(
        context.mux_limits,
        context.codec_limits,
    ));
    let _output_detach_guard = ServerUdpReliableOutputDetachGuard {
        streams: context.reliable_streams.clone(),
        session_id,
        stream_id,
        path_id,
        commands: commands_tx.clone(),
    };
    match context
        .reliable_streams
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: target.clone(),
            lane,
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
        ServerStreamOpenOutcome::New => {}
        ServerStreamOpenOutcome::Existing => {
            udp_path_write_frame(
                &mut send,
                &Frame::StreamMaxData {
                    stream_id,
                    max_offset: reliable_stream_initial_advertised_window_bytes(
                        UnderlayProtocol::Udp,
                        lane,
                        context.mux_limits,
                    ),
                },
                context.codec_limits,
            )
            .await?;
        }
        ServerStreamOpenOutcome::DuplicateLiveIgnored => {
            udp_path_write_frame(
                &mut send,
                &Frame::StreamReset {
                    stream_id,
                    reason: ResetReason::Refused,
                },
                context.codec_limits,
            )
            .await?;
            let _ = udp_path_finish_stream(&mut send);
            return Ok(());
        }
        ServerStreamOpenOutcome::Rejected => {
            udp_path_write_frame(
                &mut send,
                &Frame::StreamReset {
                    stream_id,
                    reason: ResetReason::Refused,
                },
                context.codec_limits,
            )
            .await?;
            let _ = udp_path_finish_stream(&mut send);
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
    } = stream_context;
    let carrier_frame_queue =
        udp_reliable_stream_frame_queue(context.codec_limits, context.mux_limits);
    let mut carrier_frames =
        spawn_quic_path_reader(recv, context.codec_limits, carrier_frame_queue);
    let mut pending_frames = Vec::<Frame>::new();
    let mut path_proofs = PathProofTracker::default();
    let mut deferred_input = None;
    loop {
        let command_may_recv = !reliable_path_receivers_closed(&commands_rx);
        if deferred_input.is_none()
            && let Some(command) = try_recv_reliable_path_priority_command(&mut commands_rx)
        {
            let result = drain_server_udp_reliable_commands(
                command,
                &mut commands_rx,
                &mut send,
                &context,
                session_id,
                stream_id,
                path_id,
                &path_registration,
                &commands_tx,
                &mut pending_frames,
                &mut path_proofs,
                &mut carrier_frames,
                &mut deferred_input,
            )
            .await;
            if result? {
                return Ok(());
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
                    Some(Ok(frame @ (Frame::StreamData { stream_id: received_stream_id, .. }
                        | Frame::StreamAck { stream_id: received_stream_id, .. }
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
                        context.reliable_streams.detach_path(
                            session_id,
                            stream_id,
                            UnderlayProtocol::Udp,
                            path_id,
                            &commands_tx,
                        );
                        let _ = udp_path_finish_stream(&mut send);
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
                        ..
                    })) if open_stream_id == stream_id && open_target == target =>
                    {
                        let updated_lane = traffic_class_from_stream_demand_hint(open_demand);
                        match context
                            .reliable_streams
                            .attach_existing(ServerStreamOpenRequest {
                                session_id,
                                stream_id,
                                target: target.clone(),
                                lane: updated_lane,
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
                            ServerStreamOpenOutcome::Existing => {
                                udp_path_write_frame(
                                    &mut send,
                                    &Frame::StreamMaxData {
                                        stream_id,
                                        max_offset: reliable_stream_initial_advertised_window_bytes(
                                            UnderlayProtocol::Udp,
                                            updated_lane,
                                            context.mux_limits,
                                        ),
                                    },
                                    context.codec_limits,
                                )
                                .await?;
                            }
                            ServerStreamOpenOutcome::New => {
                                return Err(RuntimeError::Protocol(
                                    "QUIC UDP path reannouncement opened duplicate stream",
                                ));
                            }
                            ServerStreamOpenOutcome::DuplicateLiveIgnored => {
                                udp_path_write_frame(
                                    &mut send,
                                    &Frame::StreamReset {
                                        stream_id,
                                        reason: ResetReason::Refused,
                                    },
                                    context.codec_limits,
                                )
                                .await?;
                                let _ = udp_path_finish_stream(&mut send);
                                return Ok(());
                            }
                            ServerStreamOpenOutcome::Rejected => {
                                udp_path_write_frame(
                                    &mut send,
                                    &Frame::StreamReset {
                                        stream_id,
                                        reason: ResetReason::Refused,
                                    },
                                    context.codec_limits,
                                )
                                .await?;
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
                            && let Some(metrics) = path_proof_metrics(
                                path_id,
                                UnderlayProtocol::Udp,
                                PathMetricDirection::ServerToClient,
                                observation,
                            )
                        {
                            context.reliable_streams.record_local_path_metrics(
                                &path_registration,
                                metrics,
                                false,
                            );
                        }
                    }
                    Some(Ok(Frame::PathCapacityData { .. }
                        | Frame::PathCapacityFinish { .. }
                        | Frame::PathCapacityReceipt { .. })) => {
                        return Err(RuntimeError::Protocol(
                            "PATH_CAPACITY frames are not valid on QUIC carriers",
                        ));
                    }
                    Some(Ok(Frame::SessionClose { reason })) => return Err(RuntimeError::RemoteClosed(reason)),
                    Some(Ok(frame)) => {
                        eprintln!(
                            "warning: unexpected server QUIC reliable carrier frame: stream_id={} frame_kind={}",
                            stream_id.0,
                            frame.kind_name(),
                        );
                        return Err(RuntimeError::Protocol("unexpected server QUIC UDP path reliable stream frame"));
                    }
                    Some(Err(RuntimeError::ReliablePathSessionClosed)) | None => {
                        context.reliable_streams.detach_path(
                            session_id,
                            stream_id,
                            UnderlayProtocol::Udp,
                            path_id,
                            &commands_tx,
                        );
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
                        session_id,
                        stream_id,
                        path_id,
                        &path_registration,
                        &commands_tx,
                        &mut pending_frames,
                        &mut path_proofs,
                        &mut carrier_frames,
                        &mut deferred_input,
                    )
                    .await?;
                    if result {
                        return Ok(());
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
                        session_id,
                        stream_id,
                        path_id,
                        &path_registration,
                        &commands_tx,
                        &mut pending_frames,
                        &mut path_proofs,
                        &mut carrier_frames,
                        &mut deferred_input,
                    ).await;
                    if result? {
                        return Ok(());
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "server_stream_test.rs"]
mod tests;
