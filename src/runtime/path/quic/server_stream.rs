//! Server reliable-stream lifecycle over a QUIC carrier path.

use super::capacity::{confirm_server_quic_capacity_receipt, udp_path_write_capacity_receipt};
use super::io::{
    UdpPathRecvStream, UdpPathSendStream, spawn_quic_path_reader, udp_path_command_queue,
    udp_path_finish_stream, udp_path_max_stream_payload_bytes, udp_path_write_frame,
    udp_reliable_stream_frame_queue,
};
use super::server_writer::drain_server_udp_reliable_commands;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{
    reliable_capacity_calibration_session_limit_bytes,
    reliable_stream_initial_advertised_window_bytes,
};
use crate::outbound::{self, TargetProtocol};
use crate::protocol::path_capacity::CapacityReceiveTracker;
use crate::protocol::{
    Frame, PathCapabilities, PathId, PathMetricDirection, ResetReason, SessionId, StreamId,
    StreamOpenRole, TargetAddr, UnderlayProtocol,
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
use crate::scheduler::{FlowLane, flow_lane_from_stream_demand_hint};

pub(super) struct ServerUdpReliableStreamContext {
    pub(super) session_id: SessionId,
    pub(super) path_id: PathId,
    pub(super) path_registration: ServerCarrierPathRegistration,
    pub(super) capabilities: PathCapabilities,
    pub(super) stream_id: StreamId,
    pub(super) target: TargetAddr,
    pub(super) lane: FlowLane,
    pub(super) role: StreamOpenRole,
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
        capabilities,
        stream_id,
        target,
        lane,
        role,
    } = stream_context;
    outbound::validate_target(&target)?;
    context.outbound.ensure_supports(TargetProtocol::Tcp)?;
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
                role,
                initial_metrics: context.local_path_startup_metrics(UnderlayProtocol::Udp, path_id),
            },
            mux_limits: context.mux_limits,
        })
        .await?
    {
        ServerStreamOpenOutcome::New => {}
        ServerStreamOpenOutcome::Existing => {
            context
                .reliable_streams
                .route_frame(
                    session_id,
                    stream_id,
                    Frame::PathStatus {
                        path_id,
                        status: crate::protocol::PathStatus::Active,
                        capabilities,
                    },
                )
                .await?;
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
            capabilities,
            stream_id,
            target: duplicate_open_target,
            lane,
            role,
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
    capabilities: PathCapabilities,
    stream_id: StreamId,
    target: TargetAddr,
    lane: FlowLane,
    role: StreamOpenRole,
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
        capabilities,
        stream_id,
        target,
        lane,
        role: _role,
        commands_tx,
        mut commands_rx,
    } = stream_context;
    let carrier_frame_queue =
        udp_reliable_stream_frame_queue(context.codec_limits, context.mux_limits);
    let mut carrier_frames =
        spawn_quic_path_reader(recv, context.codec_limits, carrier_frame_queue);
    let mut deferred_capacity_frames = std::collections::VecDeque::<Frame>::new();
    let mut pending_frames = Vec::<Frame>::new();
    let mut path_proofs = PathProofTracker::default();
    let mut capacity_receive = CapacityReceiveTracker::new(
        reliable_capacity_calibration_session_limit_bytes(context.mux_limits),
    );

    loop {
        // Receipt confirmation releases the connection-wide writer gate. This
        // task owns that receipt, so awaiting any ordinary write here would
        // self-deadlock until the probe fail-closed the whole QUIC connection.
        if send.connection.capacity_probe_active() {
            let release_connection = send.connection.clone();
            tokio::select! {
                biased;
                frame = carrier_frames.recv() => {
                    match frame {
                        Some(Ok(Frame::PathCapacityReceipt {
                            path_id: receipt_path_id,
                            calibration_id,
                            received_payload_bytes,
                        })) => {
                            confirm_server_quic_capacity_receipt(
                                &send,
                                session_id,
                                path_id,
                                path_registration.path_instance_id(),
                                stream_id,
                                receipt_path_id,
                                calibration_id,
                                received_payload_bytes,
                            )?;
                        }
                        Some(Ok(Frame::PathCapacityData {
                            path_id: capacity_path_id,
                            calibration_id,
                            payload,
                        })) => {
                            if capacity_path_id != path_id
                                || capacity_receive
                                    .record_data(calibration_id, payload.len())
                                    .is_err()
                            {
                                return Err(RuntimeError::Protocol(
                                    "invalid simultaneous server QUIC capacity data",
                                ));
                            }
                        }
                        Some(Ok(Frame::PathCapacityFinish {
                            path_id: capacity_path_id,
                            calibration_id,
                            payload_bytes,
                        })) => {
                            if capacity_path_id != path_id {
                                return Err(RuntimeError::Protocol(
                                    "simultaneous server QUIC capacity finish path mismatch",
                                ));
                            }
                            let received_payload_bytes =
                                capacity_receive.finish(calibration_id, payload_bytes)?;
                            udp_path_write_capacity_receipt(
                                &mut send,
                                path_id,
                                calibration_id,
                                received_payload_bytes,
                                context.codec_limits,
                            ).await?;
                        }
                        Some(Ok(frame)) => {
                            if deferred_capacity_frames.len() >= carrier_frame_queue {
                                return Err(RuntimeError::Protocol(
                                    "QUIC capacity receipt defer queue exceeded",
                                ));
                            }
                            deferred_capacity_frames.push_back(frame);
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
                }
                _ = release_connection.wait_for_capacity_probe_release() => {}
            }
            continue;
        }

        let replaying_capacity_frame = !deferred_capacity_frames.is_empty();
        let command_may_recv =
            !replaying_capacity_frame && !reliable_path_receivers_closed(&commands_rx);
        if !replaying_capacity_frame
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
                path_registration.path_instance_id(),
                &commands_tx,
                &mut pending_frames,
                &mut path_proofs,
            )
            .await;
            if result? {
                return Ok(());
            }
            continue;
        }
        let replay_frame = deferred_capacity_frames.pop_front();
        tokio::select! {
            biased;
            frame = async {
                match replay_frame {
                    Some(frame) => Some(Ok::<Frame, RuntimeError>(frame)),
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
                        context.reliable_streams.route_frame(session_id, stream_id, frame).await?;
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
                        role: open_role,
                        ..
                    })) if open_stream_id == stream_id && open_target == target =>
                    {
                        let updated_lane = flow_lane_from_stream_demand_hint(open_demand);
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
                                    role: open_role,
                                    initial_metrics: context
                                        .local_path_startup_metrics(UnderlayProtocol::Udp, path_id),
                                },
                                mux_limits: context.mux_limits,
                            })
                            .await?
                        {
                            ServerStreamOpenOutcome::Existing => {
                                context
                                    .reliable_streams
                                    .route_frame(
                                        session_id,
                                        stream_id,
                                        Frame::PathStatus {
                                            path_id,
                                            status: crate::protocol::PathStatus::Active,
                                            capabilities,
                                        },
                                    )
                                    .await?;
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
                            ServerStreamOpenOutcome::New => {
                                return Err(RuntimeError::Protocol(
                                    "QUIC UDP path reannouncement opened duplicate stream",
                                ));
                            }
                            ServerStreamOpenOutcome::DuplicateLiveIgnored => {
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
                            );
                        }
                    }
                    Some(Ok(Frame::PathCapacityReceipt {
                        path_id: receipt_path_id,
                        calibration_id,
                        received_payload_bytes,
                    })) => {
                        confirm_server_quic_capacity_receipt(
                            &send,
                            session_id,
                            path_id,
                            path_registration.path_instance_id(),
                            stream_id,
                            receipt_path_id,
                            calibration_id,
                            received_payload_bytes,
                        )?;
                    }
                    Some(Ok(Frame::PathCapacityData {
                        path_id: capacity_path_id,
                        calibration_id,
                        payload,
                    })) => {
                        if capacity_path_id != path_id
                            || capacity_receive
                                .record_data(calibration_id, payload.len())
                                .is_err()
                        {
                            return Err(RuntimeError::Protocol(
                                "invalid server QUIC request capacity data epoch",
                            ));
                        }
                    }
                    Some(Ok(Frame::PathCapacityFinish {
                        path_id: capacity_path_id,
                        calibration_id,
                        payload_bytes,
                    })) => {
                        if capacity_path_id != path_id {
                            return Err(RuntimeError::Protocol(
                                "server QUIC request capacity finish path mismatch",
                            ));
                        }
                        let received_payload_bytes =
                            capacity_receive.finish(calibration_id, payload_bytes)?;
                        udp_path_write_capacity_receipt(
                            &mut send,
                            path_id,
                            calibration_id,
                            received_payload_bytes,
                            context.codec_limits,
                        ).await?;
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "request_quic_capacity_receipt",
                            format_args!(
                                "phase=sent session_id={} path_id={} path_instance_id={} stream_id={} calibration_id={} received_payload_bytes={}",
                                session_id.0,
                                path_id.0,
                                path_registration.path_instance_id().as_u64(),
                                stream_id.0,
                                calibration_id,
                                received_payload_bytes,
                            ),
                        );
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
                if !send.connection.capacity_probe_active()
                    && let Some(command) = try_recv_reliable_path_command(&mut commands_rx)
                {
                    let result = drain_server_udp_reliable_commands(
                        command,
                        &mut commands_rx,
                        &mut send,
                        &context,
                        session_id,
                        stream_id,
                        path_id,
                        path_registration.path_instance_id(),
                        &commands_tx,
                        &mut pending_frames,
                        &mut path_proofs,
                    )
                    .await?;
                    if result {
                        return Ok(());
                    }
                }
            }
            command = recv_reliable_path_command(&mut commands_rx), if command_may_recv => {
                match command {
                    Some(command) => {
                        let result = drain_server_udp_reliable_commands(
                            command,
                            &mut commands_rx,
                            &mut send,
                            &context,
                            session_id,
                            stream_id,
                            path_id,
                            path_registration.path_instance_id(),
                            &commands_tx,
                            &mut pending_frames,
                            &mut path_proofs,
                        ).await;
                        if result? {
                            return Ok(());
                        }
                    }
                    None => {}
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "server_stream_test.rs"]
mod tests;
