//! Server QUIC reliable-stream command writer.

use super::io::{
    UdpPathSendStream, flush_udp_frame_batch_with_path_proofs, udp_path_finish_stream,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::protocol::{Frame, PathId, SessionId, StreamId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, ReliablePathCommandSender,
    reliable_path_command_pending_bytes, reliable_path_command_writer_run_budget_bytes,
    reliable_path_command_writer_run_budget_items, reliable_path_command_writer_run_bytes,
    reliable_path_frame_requires_capacity_command, try_coalesce_reliable_path_writer_run,
    try_recv_reliable_path_command,
};
use crate::runtime::path::proof::PathProofTracker;
use crate::runtime::path::server_context::ServerPathContext;
#[cfg(feature = "lab-diagnostics")]
use std::time::Instant;

#[allow(clippy::too_many_arguments)]
pub(super) async fn drain_server_udp_reliable_commands(
    first_command: ReliablePathCommand,
    commands: &mut ReliablePathCommandReceivers,
    send: &mut UdpPathSendStream,
    context: &ServerPathContext,
    session_id: SessionId,
    stream_id: StreamId,
    path_id: PathId,
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
            ReliablePathCommand::SendQuicCapacityProbe(probe) => {
                probe.ticket.cancel();
                commands.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "server QUIC path received request capacity command",
                ));
            }
            ReliablePathCommand::SendTcpCapacityProbe(_) => {
                commands.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "server QUIC path received TCP capacity command",
                ));
            }
            ReliablePathCommand::ResetAndCloseStream {
                stream_id: reset_stream_id,
                reason,
            } => {
                if reset_stream_id != stream_id {
                    commands.release_pending_command_bytes(pending_bytes);
                    return Err(RuntimeError::Protocol(
                        "server QUIC terminal command stream does not match writer",
                    ));
                }
                // This QUIC writer is stream-local; flush the reset before
                // detaching and finishing its carrier stream.
                pending_frames.push(Frame::StreamReset {
                    stream_id: reset_stream_id,
                    reason,
                });
                sent_bytes = sent_bytes.saturating_add(writer_run_bytes);
                sent_items = sent_items.saturating_add(1);
                flush_udp_frame_batch_with_path_proofs(
                    send,
                    pending_frames,
                    context.codec_limits,
                    path_proofs,
                )
                .await?;
                context.reliable_streams.detach_path(
                    session_id,
                    stream_id,
                    UnderlayProtocol::Udp,
                    path_id,
                    commands_tx,
                );
                let _ = udp_path_finish_stream(send);
                true
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
