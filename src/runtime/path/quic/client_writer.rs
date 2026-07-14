//! Client QUIC reliable-stream command writer.

use super::capacity::{
    quic_capacity_command_drop_reason, quic_capacity_start_rejection_reason,
    udp_path_write_capacity_probe,
};
use super::io::*;
use super::*;

pub(super) async fn drain_client_udp_stream_commands(
    first_command: ReliablePathCommand,
    commands: &mut ReliablePathCommandReceivers,
    send: &mut UdpPathSendStream,
    stream_id: StreamId,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    pending_frames: &mut Vec<Frame>,
    path_proofs: &mut PathProofTracker,
) -> Result<bool, RuntimeError> {
    #[cfg(feature = "lab-diagnostics")]
    let drain_started = Instant::now();
    let byte_budget = reliable_path_command_writer_run_budget_bytes(mux_limits);
    let item_budget = reliable_path_command_writer_run_budget_items(mux_limits);
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
            flush_udp_frame_batch_with_path_proofs(send, pending_frames, codec_limits, path_proofs)
                .await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=client underlay=Udp stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
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
                    "client QUIC path received an untyped capacity frame",
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
                        codec_limits,
                        path_proofs,
                    )
                    .await?;
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "path_writer_drain",
                        format_args!(
                            "role=client underlay=Udp stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
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
                let QuicCapacityProbeOwner::Request {
                    stream_id: owner_stream_id,
                    path_instance,
                } = probe.owner
                else {
                    probe.ticket.cancel();
                    commands.release_pending_command_bytes(pending_bytes);
                    return Err(RuntimeError::Protocol(
                        "client QUIC path received server response capacity command",
                    ));
                };
                if owner_stream_id != stream_id
                    || path_instance.key.underlay != UnderlayProtocol::Udp
                    || path_instance.key.index != usize::from(probe.path_id.0)
                {
                    probe.ticket.cancel();
                    commands.release_pending_command_bytes(pending_bytes);
                    return Err(RuntimeError::Protocol(
                        "client QUIC capacity command owner does not match writer",
                    ));
                }
                flush_udp_frame_batch_with_path_proofs(
                    send,
                    pending_frames,
                    codec_limits,
                    path_proofs,
                )
                .await?;
                if quic_capacity_command_drop_reason(&probe, Instant::now()).is_some() {
                    probe.ticket.cancel();
                    commands.release_pending_command_bytes(pending_bytes);
                    return Ok(false);
                }
                let result = {
                    let write =
                        udp_path_write_capacity_probe(send, &probe, codec_limits, mux_limits);
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
                    return Ok(false);
                };
                if let Err(err) = result {
                    if quic_capacity_start_rejection_reason(&err).is_some() {
                        probe.ticket.cancel();
                        return Ok(false);
                    }
                    return Err(err);
                }
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
                    "request_quic_capacity_calibration",
                    format_args!(
                        "phase=written stream_id={} path_index={} instance_id={} calibration_id={} train_bytes={}",
                        stream_id.0,
                        path_instance.key.index,
                        path_instance.id,
                        probe.calibration_id,
                        probe.train_payload_bytes,
                    ),
                );
                return Ok(false);
            }
            ReliablePathCommand::SendTcpCapacityProbe(_) => {
                commands.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "client QUIC path received TCP capacity command",
                ));
            }
            ReliablePathCommand::CloseStream(close_stream_id) => {
                flush_udp_frame_batch_with_path_proofs(
                    send,
                    pending_frames,
                    codec_limits,
                    path_proofs,
                )
                .await?;
                if close_stream_id == stream_id {
                    let _ = udp_path_finish_stream(send);
                    true
                } else {
                    false
                }
            }
            ReliablePathCommand::OpenStream { .. } => {
                return Err(RuntimeError::Protocol(
                    "client QUIC UDP path stream received open command",
                ));
            }
            ReliablePathCommand::CancelTcpOpen { .. } => {
                return Err(RuntimeError::Protocol(
                    "client QUIC UDP path stream received TCP open cancellation",
                ));
            }
        };
        commands.release_pending_command_bytes(pending_bytes);
        if should_close {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=client underlay=Udp stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
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
            flush_udp_frame_batch_with_path_proofs(send, pending_frames, codec_limits, path_proofs)
                .await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=client underlay=Udp stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
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
