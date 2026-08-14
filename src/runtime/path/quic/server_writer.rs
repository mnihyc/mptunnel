//! Server QUIC reliable-stream command writer.

use super::io::{
    UdpPathSendStream, flush_udp_frame_batch_with_path_proofs_interlocked, udp_path_finish_stream,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::protocol::{Frame, PathId, StreamId};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, reliable_path_command_pending_bytes,
    reliable_path_command_writer_run_budget_bytes, reliable_path_command_writer_run_budget_items,
    reliable_path_command_writer_run_bytes, reliable_path_frame_requires_capacity_command,
    try_coalesce_reliable_path_writer_run, try_recv_reliable_path_command,
};
use crate::runtime::path::proof::PathProofTracker;
use crate::runtime::path::server_context::ServerPathContext;
use crate::runtime::path::{ServerCarrierPathRegistration, ServerStreamFrameRoute};
#[cfg(feature = "lab-diagnostics")]
use std::time::Instant;
use tokio::sync::mpsc;

#[allow(clippy::too_many_arguments)]
pub(super) async fn drain_server_udp_reliable_commands(
    first_command: ReliablePathCommand,
    commands: &mut ReliablePathCommandReceivers,
    send: &mut UdpPathSendStream,
    context: &ServerPathContext,
    stream_id: StreamId,
    path_id: PathId,
    path_registration: &ServerCarrierPathRegistration,
    pending_frames: &mut Vec<Frame>,
    path_proofs: &mut PathProofTracker,
    carrier_frames: &mut mpsc::Receiver<Result<Frame, RuntimeError>>,
    deferred_input: &mut Option<Result<Frame, RuntimeError>>,
) -> Result<bool, RuntimeError> {
    debug_assert!(deferred_input.is_none());
    #[cfg(feature = "lab-diagnostics")]
    let drain_started = Instant::now();
    let byte_budget = reliable_path_command_writer_run_budget_bytes(context.mux_limits);
    let item_budget = reliable_path_command_writer_run_budget_items(context.mux_limits);
    let mut next_command = Some(first_command);
    pending_frames.clear();
    let mut sent_bytes = 0usize;
    let mut sent_items = 0usize;
    let mut pending_frame_command_bytes = 0usize;

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
            flush_server_udp_frame_batch(
                send,
                pending_frames,
                context.codec_limits,
                path_proofs,
                commands,
                &mut pending_frame_command_bytes,
                path_id,
                stream_id,
                context,
                path_registration,
                carrier_frames,
                deferred_input,
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
        let mut pending_released_by_batch = false;
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
                pending_frame_command_bytes =
                    pending_frame_command_bytes.saturating_add(pending_bytes);
                sent_bytes = sent_bytes.saturating_add(writer_run_bytes);
                sent_items = sent_items.saturating_add(1);
                if sent_bytes >= byte_budget || sent_items >= item_budget {
                    flush_server_udp_frame_batch(
                        send,
                        pending_frames,
                        context.codec_limits,
                        path_proofs,
                        commands,
                        &mut pending_frame_command_bytes,
                        path_id,
                        stream_id,
                        context,
                        path_registration,
                        carrier_frames,
                        deferred_input,
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
                pending_frame_command_bytes =
                    pending_frame_command_bytes.saturating_add(pending_bytes);
                flush_server_udp_frame_batch(
                    send,
                    pending_frames,
                    context.codec_limits,
                    path_proofs,
                    commands,
                    &mut pending_frame_command_bytes,
                    path_id,
                    stream_id,
                    context,
                    path_registration,
                    carrier_frames,
                    deferred_input,
                )
                .await?;
                pending_released_by_batch = true;
                context
                    .reliable_streams
                    .detach_path(path_registration, stream_id)?;
                let _ = udp_path_finish_stream(send).await;
                true
            }
            ReliablePathCommand::CloseStream(close_stream_id) => {
                flush_server_udp_frame_batch(
                    send,
                    pending_frames,
                    context.codec_limits,
                    path_proofs,
                    commands,
                    &mut pending_frame_command_bytes,
                    path_id,
                    stream_id,
                    context,
                    path_registration,
                    carrier_frames,
                    deferred_input,
                )
                .await?;
                if close_stream_id == stream_id {
                    context
                        .reliable_streams
                        .detach_path(path_registration, stream_id)?;
                    // An unordered close before any response is the wire
                    // representation used by a post-resolution policy drop.
                    // Cancel that request without manufacturing HTTP 200;
                    // an already-started response still finishes normally.
                    if !send.cancel_pending_response() {
                        let _ = udp_path_finish_stream(send).await;
                    }
                    true
                } else {
                    false
                }
            }
            ReliablePathCommand::PrepareConnection { .. }
            | ReliablePathCommand::OpenStream { .. }
            | ReliablePathCommand::OpenDatagramAttachment { .. }
            | ReliablePathCommand::OpenDatagramFlow { .. }
            | ReliablePathCommand::SendDatagramFrame { .. }
            | ReliablePathCommand::CloseDatagramAttachment { .. } => {
                return Err(RuntimeError::Protocol(
                    "server QUIC UDP path stream received client TCP session command",
                ));
            }
            ReliablePathCommand::CancelTcpOpen { .. } => {
                return Err(RuntimeError::Protocol(
                    "server QUIC UDP path stream received TCP open cancellation",
                ));
            }
        };
        if !pending_released_by_batch {
            commands.release_pending_command_bytes(pending_bytes);
        }
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
        if deferred_input.is_some() {
            return Ok(false);
        }
        sent_items = sent_items.saturating_add(1);
        if sent_bytes >= byte_budget || sent_items >= item_budget {
            flush_server_udp_frame_batch(
                send,
                pending_frames,
                context.codec_limits,
                path_proofs,
                commands,
                &mut pending_frame_command_bytes,
                path_id,
                stream_id,
                context,
                path_registration,
                carrier_frames,
                deferred_input,
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

// The borrowed writer, queues, and accounting owners remain explicit across the await.
#[allow(clippy::too_many_arguments)]
async fn flush_server_udp_frame_batch(
    send: &mut UdpPathSendStream,
    pending_frames: &mut Vec<Frame>,
    codec_limits: crate::protocol::codec::CodecLimits,
    path_proofs: &mut PathProofTracker,
    commands: &ReliablePathCommandReceivers,
    pending_frame_command_bytes: &mut usize,
    _path_id: PathId,
    stream_id: StreamId,
    context: &ServerPathContext,
    path_registration: &ServerCarrierPathRegistration,
    carrier_frames: &mut mpsc::Receiver<Result<Frame, RuntimeError>>,
    deferred_input: &mut Option<Result<Frame, RuntimeError>>,
) -> Result<(), RuntimeError> {
    let result = flush_udp_frame_batch_with_path_proofs_interlocked(
        send,
        pending_frames,
        codec_limits,
        path_proofs,
        carrier_frames,
        deferred_input,
        |frame| {
            try_route_server_udp_stream_frame_during_write(
                frame,
                stream_id,
                context,
                path_registration,
            )
        },
    )
    .await;
    commands.release_pending_command_bytes(std::mem::take(pending_frame_command_bytes));
    let _routed_frames = result?;
    #[cfg(feature = "lab-diagnostics")]
    if _routed_frames > 0 || deferred_input.is_some() {
        lab_diagnostic(
            "server_quic_write_feedback_interlock",
            format_args!(
                "path_id={} stream_id={} routed_frames={} deferred_frames={}",
                _path_id.0,
                stream_id.0,
                _routed_frames,
                usize::from(deferred_input.is_some()),
            ),
        );
    }
    Ok(())
}

fn try_route_server_udp_stream_frame_during_write(
    frame: Frame,
    stream_id: StreamId,
    context: &ServerPathContext,
    path_registration: &ServerCarrierPathRegistration,
) -> Result<Option<Frame>, RuntimeError> {
    let received_stream_id = match &frame {
        Frame::StreamData { stream_id, .. }
        | Frame::StreamAck { stream_id, .. }
        | Frame::StreamMaxData { stream_id, .. }
        | Frame::StreamFin { stream_id, .. }
        | Frame::StreamReset { stream_id, .. } => *stream_id,
        _ => return Ok(Some(frame)),
    };
    if received_stream_id != stream_id {
        return Ok(Some(frame));
    }
    match context
        .reliable_streams
        .try_route_frame(path_registration, stream_id, frame)?
    {
        ServerStreamFrameRoute::Routed => Ok(None),
        ServerStreamFrameRoute::Backpressured(frame) => Ok(Some(frame)),
    }
}
