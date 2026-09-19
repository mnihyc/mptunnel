//! Client QUIC reliable-stream command writer.

use super::io::{
    UdpPathRecvStream, UdpPathSendStream, flush_udp_frame_batch_with_path_proofs,
    flush_udp_frame_batch_with_path_proofs_interlocked, udp_path_finish_stream,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::path::CarrierPathInstanceId;
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{Frame, StreamId};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, recv_reliable_path_command,
    reliable_path_command_pending_bytes, reliable_path_command_writer_run_budget_bytes,
    reliable_path_command_writer_run_budget_items, reliable_path_command_writer_run_bytes,
    reliable_path_frame_requires_capacity_command, try_coalesce_reliable_path_writer_run,
    try_recv_reliable_path_command,
};
use crate::runtime::path::input::{CarrierInputRoute, PendingMailboxFrame};
use crate::runtime::path::proof::PathProofTracker;
use crate::runtime::sender::PreparedOriginalClaim;
#[cfg(feature = "lab-diagnostics")]
use std::time::Instant;
use tokio::sync::mpsc;

/// Drains the existing retirement transaction for a completely submitted OPEN.
/// No native input is parsed after the pending logical owner has withdrawn.
#[allow(clippy::too_many_arguments)]
pub(super) async fn retire_submitted_client_udp_stream(
    (mut send, _recv): (UdpPathSendStream, UdpPathRecvStream),
    stream_id: StreamId,
    path_instance_id: CarrierPathInstanceId,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    mut commands: ReliablePathCommandReceivers,
    frames: mpsc::Sender<Result<Frame, RuntimeError>>,
    #[cfg(test)] events: Option<super::client::ClientUdpPendingOpenEvents>,
) -> Result<(), RuntimeError> {
    commands.bind_native_commitment(send.bind_native_commitment()?)?;
    // This closed, empty receiver satisfies the ordinary writer interface.
    // carrier_input_open=false prevents every input poll; no reader is spawned.
    let (unused, mut carrier_frames) = mpsc::channel(1);
    drop(unused);
    let mut pending_frames = Vec::new();
    let mut path_proofs = PathProofTracker::from_limits(mux_limits);
    let mut deferred_input = None;
    while let Some(command) = recv_reliable_path_command(&mut commands).await {
        let drain = drain_client_udp_stream_commands(
            command,
            &mut commands,
            &mut send,
            stream_id,
            path_instance_id,
            codec_limits,
            mux_limits,
            &mut pending_frames,
            &mut path_proofs,
            &mut carrier_frames,
            &frames,
            &mut deferred_input,
            false,
        );
        #[cfg(test)]
        let closed = {
            use std::future::Future;
            let mut drain = std::pin::pin!(drain);
            let mut pending_reported = false;
            std::future::poll_fn(|cx| {
                let result = drain.as_mut().poll(cx);
                if result.is_pending() && !pending_reported {
                    pending_reported = true;
                    if let Some(events) = &events {
                        let _ = events.send((
                            stream_id,
                            super::client::ClientUdpPendingOpenEvent::RetirementPending,
                        ));
                    }
                }
                result
            })
            .await?
        };
        #[cfg(not(test))]
        let closed = drain.await?;
        if closed {
            return Ok(());
        }
    }
    Err(RuntimeError::ReliablePathSessionClosed)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn drain_client_udp_stream_commands(
    first_command: ReliablePathCommand,
    commands: &mut ReliablePathCommandReceivers,
    send: &mut UdpPathSendStream,
    stream_id: StreamId,
    path_instance_id: CarrierPathInstanceId,
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
    let mut has_prepared_claim = false;

    loop {
        let Some(command) = next_command
            .take()
            .or_else(|| try_recv_reliable_path_command(commands))
        else {
            if !has_prepared_claim
                && try_coalesce_reliable_path_writer_run(
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
        if !matches!(&command, ReliablePathCommand::PreparedOriginal(_)) {
            commands.withdraw_writer_ready();
        }
        let should_close = match command {
            ReliablePathCommand::PreparedOriginal(work) => {
                if work.request_instance().is_none()
                    || work.stream_id() != stream_id
                    || work.path_instance_id() != path_instance_id
                {
                    // An obsolete weak subscription owns neither payload nor
                    // carrier failure authority.
                    false
                } else if let Some(ready) = commands.writer_ready_boundary(path_instance_id) {
                    match work.try_claim(ready) {
                        PreparedOriginalClaim::Claimed(frame) => {
                            let bytes = commands.register_claimed_writer_frame(&frame);
                            let encoded_bytes =
                                crate::protocol::codec::encoded_frame_capacity_hint(&frame).max(1);
                            pending_frame_command_bytes = pending_frame_command_bytes
                                .checked_add(bytes)
                                .ok_or(RuntimeError::Protocol(
                                    "client QUIC writer transaction byte overflow",
                                ))?;
                            pending_frames.push(frame);
                            has_prepared_claim = true;
                            sent_bytes = sent_bytes.saturating_add(encoded_bytes);
                            // Feed the same imminent bounded batch without a
                            // source-actor reply. No coalescing await is allowed
                            // after the first actual claim and before flush.
                            work.requeue();
                        }
                        PreparedOriginalClaim::RecoveryQueued => {
                            work.requeue();
                            // A repair publication is one finite acquisition
                            // turn. Preserve every earlier owned Original and
                            // deferred input before returning to arbitration.
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
                            return Ok(false);
                        }
                        PreparedOriginalClaim::Busy(wait) => {
                            commands.defer_prepared_work(work, wait);
                        }
                        PreparedOriginalClaim::CarrierFailed(error) => return Err(error),
                        PreparedOriginalClaim::Blocked(wait) => {
                            commands.defer_prepared_work(work, wait);
                        }
                        PreparedOriginalClaim::Empty => {}
                    }
                    false
                } else {
                    false
                }
            }
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
            #[cfg(test)]
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
                let _ = udp_path_finish_stream(send).await;
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
                    let _ = udp_path_finish_stream(send).await;
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
    commands: &mut ReliablePathCommandReceivers,
    pending_frame_command_bytes: &mut usize,
    stream_id: StreamId,
    carrier_frames: &mut mpsc::Receiver<Result<Frame, RuntimeError>>,
    stream_frames: &mpsc::Sender<Result<Frame, RuntimeError>>,
    deferred_input: &mut Option<Result<Frame, RuntimeError>>,
    carrier_input_open: bool,
) -> Result<(), RuntimeError> {
    if !pending_frames.is_empty() {
        // A refused metadata claim may leave the idle epoch current. The
        // already-claimed batch must withdraw it before entering native I/O.
        commands.withdraw_writer_ready();
    }
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
) -> Result<CarrierInputRoute, RuntimeError> {
    let received_stream_id = match &frame {
        Frame::StreamData { stream_id, .. }
        | Frame::StreamAck { stream_id, .. }
        | Frame::StreamFeedbackProbe { stream_id, .. }
        | Frame::StreamFeedbackReceipt { stream_id, .. }
        | Frame::StreamRequalifyData { stream_id, .. }
        | Frame::StreamRequalifyAck { stream_id, .. }
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
            return Ok(CarrierInputRoute::Barrier(frame));
        }
        Frame::StreamFin { stream_id, .. } | Frame::StreamReset { stream_id, .. } => *stream_id,
        _ => return Ok(CarrierInputRoute::Barrier(frame)),
    };
    if received_stream_id != stream_id {
        return Ok(CarrierInputRoute::Barrier(frame));
    }
    match stream_frames.try_send(Ok(frame)) {
        Ok(()) => Ok(CarrierInputRoute::Routed),
        Err(mpsc::error::TrySendError::Full(Ok(frame))) => Ok(CarrierInputRoute::Mailbox(
            PendingMailboxFrame::new(frame, stream_frames.clone(), Ok),
        )),
        // The Product recipient can retire before this native write and its
        // ordered terminal commands. Preserve their independent ownership.
        Err(mpsc::error::TrySendError::Closed(_)) => Ok(CarrierInputRoute::Routed),
        Err(mpsc::error::TrySendError::Full(Err(_))) => {
            unreachable!("client QUIC interlock only routes successful frames")
        }
    }
}

#[cfg(test)]
#[path = "tests_client_writer.rs"]
mod tests;
