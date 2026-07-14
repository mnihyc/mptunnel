//! Server QUIC reliable-stream command writer.

use super::capacity::{
    quic_capacity_command_drop_reason, quic_capacity_start_rejection_reason,
    udp_path_write_capacity_probe,
};
use super::io::*;
use super::*;

pub(super) async fn drain_server_udp_reliable_commands(
    first_command: ReliablePathCommand,
    commands: &mut ReliablePathCommandReceivers,
    send: &mut UdpPathSendStream,
    context: &ServerPathContext,
    session_id: SessionId,
    stream_id: StreamId,
    path_id: PathId,
    path_instance_id: ServerCarrierPathInstanceId,
    commands_tx: &ReliablePathCommandSender,
    pending_frames: &mut Vec<Frame>,
    path_proofs: &mut PathProofTracker,
) -> Result<bool, RuntimeError> {
    #[cfg(feature = "lab-diagnostics")]
    let drain_started = Instant::now();
    let byte_budget = reliable_path_command_writer_run_budget_bytes(context.mux_limits);
    let item_budget = reliable_path_command_writer_run_budget_items(context.mux_limits);
    let mut next_command = Some(first_command);
    pending_frames.clear();
    let mut sent_bytes = 0usize;
    let mut sent_items = 0usize;

    loop {
        let Some(command) = next_command
            .take()
            .or_else(|| try_recv_reliable_path_command(commands))
        else {
            if try_coalesce_reliable_path_writer_run(
                commands,
                &mut next_command,
                sent_items,
                sent_bytes,
                byte_budget,
                item_budget,
            )
            .await
            {
                continue;
            }
            flush_udp_frame_batch_with_path_proofs(
                send,
                pending_frames,
                context.codec_limits,
                path_proofs,
            )
            .await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=server underlay=Udp path_id={} stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    path_id.0,
                    stream_id.0,
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    false,
                    false,
                ),
            );
            return Ok(false);
        };
        let pending_bytes = reliable_path_command_pending_bytes(&command);
        let writer_run_bytes = reliable_path_command_writer_run_bytes(&command);
        let should_close = match command {
            ReliablePathCommand::SendFrame(frame)
                if reliable_path_frame_requires_capacity_command(&frame) =>
            {
                commands.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "server QUIC path received an untyped capacity frame",
                ));
            }
            ReliablePathCommand::SendFrame(frame) => {
                pending_frames.push(frame);
                commands.release_pending_command_bytes(pending_bytes);
                sent_bytes = sent_bytes.saturating_add(writer_run_bytes);
                sent_items = sent_items.saturating_add(1);
                if sent_bytes >= byte_budget || sent_items >= item_budget {
                    flush_udp_frame_batch_with_path_proofs(
                        send,
                        pending_frames,
                        context.codec_limits,
                        path_proofs,
                    )
                    .await?;
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "path_writer_drain",
                        format_args!(
                            "role=server underlay=Udp path_id={} stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                            path_id.0,
                            stream_id.0,
                            sent_items,
                            sent_bytes,
                            byte_budget,
                            item_budget,
                            commands.pending_bytes(),
                            drain_started.elapsed().as_micros(),
                            true,
                            sent_items >= item_budget,
                        ),
                    );
                    return Ok(false);
                }
                continue;
            }
            ReliablePathCommand::SendQuicCapacityProbe(mut probe) => {
                let QuicCapacityProbeOwner::Response {
                    binding_instance_id: _binding_instance_id,
                    path_instance_id: owner_path_instance_id,
                } = probe.owner
                else {
                    probe.ticket.cancel();
                    commands.release_pending_command_bytes(pending_bytes);
                    return Err(RuntimeError::Protocol(
                        "server QUIC path received client request capacity command",
                    ));
                };
                if probe.path_id != path_id || owner_path_instance_id != path_instance_id {
                    probe.ticket.cancel();
                    commands.release_pending_command_bytes(pending_bytes);
                    return Err(RuntimeError::Protocol(
                        "server QUIC capacity command path does not match writer",
                    ));
                }
                flush_udp_frame_batch_with_path_proofs(
                    send,
                    pending_frames,
                    context.codec_limits,
                    path_proofs,
                )
                .await?;
                if let Some(_reason) = quic_capacity_command_drop_reason(&probe, Instant::now()) {
                    probe.ticket.cancel();
                    commands.release_pending_command_bytes(pending_bytes);
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "response_quic_capacity_calibration",
                        format_args!(
                            "phase=command_dropped reason={} session_id={} binding_instance_id={} underlay=Udp path_id={} path_instance_id={} calibration_id={} train_bytes={}",
                            _reason,
                            session_id.0,
                            _binding_instance_id,
                            path_id.0,
                            owner_path_instance_id.as_u64(),
                            probe.calibration_id,
                            probe.train_payload_bytes,
                        ),
                    );
                    return Ok(false);
                }
                let result = {
                    let write = udp_path_write_capacity_probe(
                        send,
                        &probe,
                        context.codec_limits,
                        context.mux_limits,
                    );
                    tokio::pin!(write);
                    tokio::select! {
                        biased;
                        _ = probe.ticket.cancelled() => None,
                        result = &mut write => Some(result),
                    }
                };
                commands.release_pending_command_bytes(pending_bytes);
                let Some(result) = result else {
                    let _ = send.connection.cancel_capacity_probe(probe.calibration_id);
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "response_quic_capacity_calibration",
                        format_args!(
                            "phase=command_cancelled reason=ownership_invalidated_during_write session_id={} binding_instance_id={} underlay=Udp path_id={} path_instance_id={} calibration_id={} train_bytes={}",
                            session_id.0,
                            _binding_instance_id,
                            path_id.0,
                            owner_path_instance_id.as_u64(),
                            probe.calibration_id,
                            probe.train_payload_bytes,
                        ),
                    );
                    return Ok(false);
                };
                if let Err(err) = result {
                    if let Some(_reason) = quic_capacity_start_rejection_reason(&err) {
                        probe.ticket.cancel();
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "response_quic_capacity_calibration",
                            format_args!(
                                "phase=command_rejected reason={} session_id={} binding_instance_id={} underlay=Udp path_id={} path_instance_id={} calibration_id={} train_bytes={}",
                                _reason,
                                session_id.0,
                                _binding_instance_id,
                                path_id.0,
                                owner_path_instance_id.as_u64(),
                                probe.calibration_id,
                                probe.train_payload_bytes,
                            ),
                        );
                        return Ok(false);
                    }
                    return Err(err);
                }
                // The carrier epoch now owns cancellation. Before this point,
                // dropping a dequeued command must invalidate its session lease.
                probe.disarm_drop_cancellation();
                let cancellation_connection = send.connection.clone();
                let cancellation_ticket = probe.ticket.clone();
                let cancellation_token = probe.calibration_id;
                tokio::spawn(async move {
                    if cancellation_ticket.resolved().await
                        == QuicCapacityProbeCommandResolution::Cancelled
                    {
                        let _ = cancellation_connection.cancel_capacity_probe(cancellation_token);
                    }
                });
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "path_writer_drain",
                    format_args!(
                        "role=server underlay=Udp path_id={} stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={} capacity_probe=true calibration_id={}",
                        path_id.0,
                        stream_id.0,
                        sent_items.saturating_add(1),
                        sent_bytes.saturating_add(writer_run_bytes),
                        byte_budget,
                        item_budget,
                        commands.pending_bytes(),
                        drain_started.elapsed().as_micros(),
                        true,
                        false,
                        probe.calibration_id,
                    ),
                );
                // End the run at the epoch boundary. A later dequeue may block
                // on the carrier gate, but cannot enter this write transaction.
                return Ok(false);
            }
            ReliablePathCommand::SendTcpCapacityProbe(_) => {
                commands.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "server QUIC path received TCP capacity command",
                ));
            }
            ReliablePathCommand::CloseStream(close_stream_id) => {
                flush_udp_frame_batch_with_path_proofs(
                    send,
                    pending_frames,
                    context.codec_limits,
                    path_proofs,
                )
                .await?;
                if close_stream_id == stream_id {
                    context.reliable_streams.detach_path(
                        session_id,
                        stream_id,
                        UnderlayProtocol::Udp,
                        path_id,
                        commands_tx,
                    );
                    let _ = udp_path_finish_stream(send);
                    true
                } else {
                    false
                }
            }
            ReliablePathCommand::OpenStream { .. } => {
                return Err(RuntimeError::Protocol(
                    "server QUIC UDP path stream received client open command",
                ));
            }
            ReliablePathCommand::CancelTcpOpen { .. } => {
                return Err(RuntimeError::Protocol(
                    "server QUIC UDP path stream received TCP open cancellation",
                ));
            }
        };
        commands.release_pending_command_bytes(pending_bytes);
        if should_close {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=server underlay=Udp path_id={} stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    path_id.0,
                    stream_id.0,
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    false,
                    false,
                ),
            );
            return Ok(true);
        }
        sent_items = sent_items.saturating_add(1);
        if sent_bytes >= byte_budget || sent_items >= item_budget {
            flush_udp_frame_batch_with_path_proofs(
                send,
                pending_frames,
                context.codec_limits,
                path_proofs,
            )
            .await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=server underlay=Udp path_id={} stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    path_id.0,
                    stream_id.0,
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    true,
                    sent_items >= item_budget,
                ),
            );
            return Ok(false);
        }
    }
}
