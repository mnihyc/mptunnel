//! Client QUIC reliable-stream command writer.

use super::io::{
    UdpPathSendStream, flush_udp_frame_batch_with_path_proofs,
    flush_udp_frame_batch_with_path_proofs_interlocked, udp_path_finish_stream,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{Frame, StreamId};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, reliable_path_command_pending_bytes,
    reliable_path_command_writer_run_budget_bytes, reliable_path_command_writer_run_budget_items,
    reliable_path_command_writer_run_bytes, reliable_path_frame_requires_capacity_command,
    try_coalesce_reliable_path_writer_run, try_recv_reliable_path_command,
};
use crate::runtime::path::proof::PathProofTracker;
#[cfg(feature = "lab-diagnostics")]
use std::time::Instant;
use tokio::sync::mpsc;

#[allow(clippy::too_many_arguments)]
pub(super) async fn drain_client_udp_stream_commands(
    first_command: ReliablePathCommand,
    commands: &mut ReliablePathCommandReceivers,
    send: &mut UdpPathSendStream,
    stream_id: StreamId,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    pending_frames: &mut Vec<Frame>,
    path_proofs: &mut PathProofTracker,
    carrier_frames: &mut mpsc::Receiver<Result<Frame, RuntimeError>>,
    stream_frames: &mpsc::Sender<Result<Frame, RuntimeError>>,
    deferred_input: &mut Option<Result<Frame, RuntimeError>>,
    carrier_input_open: bool,
) -> Result<bool, RuntimeError> {
    debug_assert!(deferred_input.is_none());
    #[cfg(feature = "lab-diagnostics")]
    let drain_started = Instant::now();
    let byte_budget = reliable_path_command_writer_run_budget_bytes(mux_limits);
    let item_budget = reliable_path_command_writer_run_budget_items(mux_limits);
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
            flush_client_udp_frame_batch(
                send,
                pending_frames,
                codec_limits,
                path_proofs,
                commands,
                &mut pending_frame_command_bytes,
                stream_id,
                carrier_frames,
                stream_frames,
                deferred_input,
                carrier_input_open,
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
                pending_frame_command_bytes =
                    pending_frame_command_bytes.saturating_add(pending_bytes);
                sent_bytes = sent_bytes.saturating_add(writer_run_bytes);
                sent_items = sent_items.saturating_add(1);
                if sent_bytes >= byte_budget || sent_items >= item_budget {
                    flush_client_udp_frame_batch(
                        send,
                        pending_frames,
                        codec_limits,
                        path_proofs,
                        commands,
                        &mut pending_frame_command_bytes,
                        stream_id,
                        carrier_frames,
                        stream_frames,
                        deferred_input,
                        carrier_input_open,
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
            ReliablePathCommand::SendTcpCapacityProbe(_) => {
                commands.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "client QUIC path received TCP capacity command",
                ));
            }
            ReliablePathCommand::ResetAndCloseStream {
                stream_id: reset_stream_id,
                reason,
            } => {
                if reset_stream_id != stream_id {
                    commands.release_pending_command_bytes(pending_bytes);
                    return Err(RuntimeError::Protocol(
                        "client QUIC terminal command stream does not match writer",
                    ));
                }
                pending_frames.push(Frame::StreamReset {
                    stream_id: reset_stream_id,
                    reason,
                });
                flush_client_udp_frame_batch(
                    send,
                    pending_frames,
                    codec_limits,
                    path_proofs,
                    commands,
                    &mut pending_frame_command_bytes,
                    stream_id,
                    carrier_frames,
                    stream_frames,
                    deferred_input,
                    carrier_input_open,
                )
                .await?;
                let _ = udp_path_finish_stream(send);
                true
            }
            ReliablePathCommand::CloseStream(close_stream_id) => {
                flush_client_udp_frame_batch(
                    send,
                    pending_frames,
                    codec_limits,
                    path_proofs,
                    commands,
                    &mut pending_frame_command_bytes,
                    stream_id,
                    carrier_frames,
                    stream_frames,
                    deferred_input,
                    carrier_input_open,
                )
                .await?;
                if close_stream_id == stream_id {
                    let _ = udp_path_finish_stream(send);
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
                    "client QUIC UDP path stream received TCP session command",
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
        if deferred_input.is_some() {
            return Ok(false);
        }
        sent_items = sent_items.saturating_add(1);
        if sent_bytes >= byte_budget || sent_items >= item_budget {
            flush_client_udp_frame_batch(
                send,
                pending_frames,
                codec_limits,
                path_proofs,
                commands,
                &mut pending_frame_command_bytes,
                stream_id,
                carrier_frames,
                stream_frames,
                deferred_input,
                carrier_input_open,
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
    }
}

// The borrowed writer, queues, and accounting owners remain explicit across the await.
#[allow(clippy::too_many_arguments)]
async fn flush_client_udp_frame_batch(
    send: &mut UdpPathSendStream,
    pending_frames: &mut Vec<Frame>,
    codec_limits: CodecLimits,
    path_proofs: &mut PathProofTracker,
    commands: &ReliablePathCommandReceivers,
    pending_frame_command_bytes: &mut usize,
    stream_id: StreamId,
    carrier_frames: &mut mpsc::Receiver<Result<Frame, RuntimeError>>,
    stream_frames: &mpsc::Sender<Result<Frame, RuntimeError>>,
    deferred_input: &mut Option<Result<Frame, RuntimeError>>,
    carrier_input_open: bool,
) -> Result<(), RuntimeError> {
    let result = if carrier_input_open {
        flush_udp_frame_batch_with_path_proofs_interlocked(
            send,
            pending_frames,
            codec_limits,
            path_proofs,
            carrier_frames,
            deferred_input,
            |frame| try_route_client_udp_stream_frame_during_write(frame, stream_id, stream_frames),
        )
        .await
    } else {
        flush_udp_frame_batch_with_path_proofs(send, pending_frames, codec_limits, path_proofs)
            .await
            .map(|()| 0)
    };
    commands.release_pending_command_bytes(std::mem::take(pending_frame_command_bytes));
    let _routed_frames = result?;
    #[cfg(feature = "lab-diagnostics")]
    if _routed_frames > 0 || deferred_input.is_some() {
        lab_diagnostic(
            "client_quic_write_feedback_interlock",
            format_args!(
                "stream_id={} routed_frames={} deferred_frames={}",
                stream_id.0,
                _routed_frames,
                usize::from(deferred_input.is_some()),
            ),
        );
    }
    Ok(())
}

fn try_route_client_udp_stream_frame_during_write(
    frame: Frame,
    stream_id: StreamId,
    stream_frames: &mpsc::Sender<Result<Frame, RuntimeError>>,
) -> Result<Option<Frame>, RuntimeError> {
    let received_stream_id = match &frame {
        Frame::StreamData { stream_id, .. }
        | Frame::StreamAck { stream_id, .. }
        | Frame::StreamMaxData { stream_id, .. } => *stream_id,
        // Terminal frames change receive-half ownership. Defer them to the
        // outer stream loop so clean QUIC EOF cannot overtake that transition.
        Frame::StreamFin {
            stream_id: terminal_stream_id,
            ..
        }
        | Frame::StreamReset {
            stream_id: terminal_stream_id,
            ..
        } if *terminal_stream_id == stream_id => {
            return Ok(Some(frame));
        }
        Frame::StreamFin { stream_id, .. } | Frame::StreamReset { stream_id, .. } => *stream_id,
        _ => return Ok(Some(frame)),
    };
    if received_stream_id != stream_id {
        return Ok(Some(frame));
    }
    match stream_frames.try_send(Ok(frame)) {
        Ok(()) => Ok(None),
        Err(mpsc::error::TrySendError::Full(Ok(frame))) => Ok(Some(frame)),
        Err(mpsc::error::TrySendError::Closed(_)) => Err(RuntimeError::ReliablePathSessionClosed),
        Err(mpsc::error::TrySendError::Full(Err(_))) => {
            unreachable!("client QUIC interlock only routes successful frames")
        }
    }
}

#[cfg(test)]
#[path = "client_writer_test.rs"]
mod tests;
