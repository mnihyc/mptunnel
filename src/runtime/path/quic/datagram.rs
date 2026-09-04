//! Server application-datagram stream lifecycle over QUIC.

use super::io::{
    UdpPathRecvStream, UdpPathSendStream, flush_udp_frame_batch, spawn_quic_path_reader,
    udp_path_command_queue, udp_path_finish_stream, udp_path_retain_datagram_denial,
    udp_path_write_datagram_refusal, udp_path_write_frame,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::product::PrincipalPermit;
use crate::protocol::frame::datagram_feedback_range;
use crate::protocol::{DatagramFlowId, Frame, SessionId, TargetAddr};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, ReliablePathCommandSender,
    recv_reliable_path_command, reliable_path_command_channels,
    reliable_path_command_pending_bytes, reliable_path_command_writer_run_budget_bytes,
    reliable_path_command_writer_run_budget_items, reliable_path_command_writer_run_bytes,
    reliable_path_frame_requires_capacity_command, reliable_path_receivers_closed,
    try_coalesce_reliable_path_writer_run, try_recv_reliable_path_command,
};
use crate::runtime::path::server_context::ServerPathContext;
use crate::runtime::path::{
    AcceptedServerDatagramFlow, ServerDatagramOpenFailure, ServerDatagramOpenRequest,
    ServerDatagramRequest, ServerDatagramSendOutcome, ServerDatagramTombstone,
    ServerDatagramTombstoneCache, ServerMppIngress, ServerMppIngressObserver,
};
#[cfg(feature = "lab-diagnostics")]
use std::time::Instant;

pub(super) struct ServerUdpDatagramStreamContext {
    pub(super) session_id: SessionId,
    pub(super) principal_permit: PrincipalPermit,
    pub(super) ingress: ServerMppIngressObserver,
    pub(super) flow_id: DatagramFlowId,
    pub(super) target: TargetAddr,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ServerUdpDatagramOpenOutcome {
    Opened,
    Rejected,
    Dropped,
}

pub(super) async fn handle_server_udp_datagram_stream(
    send: UdpPathSendStream,
    recv: UdpPathRecvStream,
    context: ServerPathContext,
    stream_context: ServerUdpDatagramStreamContext,
) -> Result<(), RuntimeError> {
    let (commands_tx, mut commands_rx) = reliable_path_command_channels(udp_path_command_queue(
        context.mux_limits,
        context.codec_limits,
    ));
    let mut send = send;
    let mut carrier_frames = spawn_quic_path_reader(
        recv,
        context.codec_limits,
        udp_path_command_queue(context.mux_limits, context.codec_limits),
    );
    let mut flows = Vec::<AcceptedServerDatagramFlow>::new();
    let mut tombstones = ServerDatagramTombstoneCache::new(context.max_udp_flows_per_session);
    let mut pending_frames = Vec::<Frame>::new();
    let mut saw_silent_drop = matches!(
        open_server_udp_datagram_flow(
            &context,
            &commands_tx,
            &mut send,
            &mut flows,
            &mut tombstones,
            stream_context.session_id,
            stream_context.principal_permit.clone(),
            stream_context.ingress.snapshot(),
            stream_context.flow_id,
            stream_context.target,
        )
        .await?,
        ServerUdpDatagramOpenOutcome::Dropped
    );
    loop {
        let command_may_recv = !reliable_path_receivers_closed(&commands_rx);
        tokio::select! {
            biased;
            frame = carrier_frames.recv() => {
                match frame {
                    Some(Ok(Frame::OpenDatagramFlow { flow_id, target, .. })) => {
                        let outcome = open_server_udp_datagram_flow(
                            &context,
                            &commands_tx,
                            &mut send,
                            &mut flows,
                            &mut tombstones,
                            stream_context.session_id,
                            stream_context.principal_permit.clone(),
                            stream_context.ingress.snapshot(),
                            flow_id,
                            target,
                        ).await?;
                        saw_silent_drop |= matches!(outcome, ServerUdpDatagramOpenOutcome::Dropped);
                    }
                    Some(Ok(Frame::DatagramData { flow_id, datagram_id, ttl_ms, payload })) => {
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "server_udp_datagram_request_received",
                            format_args!(
                                "session_id={} flow_id={} datagram_id={} payload_bytes={} ttl_ms={}",
                                stream_context.session_id.0,
                                flow_id.0,
                                datagram_id.0,
                                payload.len(),
                                ttl_ms,
                            ),
                        );
                        if let Some(flow) = flows.iter().find(|flow| flow.flow_id() == flow_id) {
                            if ttl_ms == 0 {
                                return Err(RuntimeError::Protocol("expired QUIC UDP path datagram received"));
                            }
                            match flow
                                .send(ServerDatagramRequest { datagram_id, ttl_ms, payload })
                                .await?
                            {
                                ServerDatagramSendOutcome::Accepted => {
                                    let received = datagram_feedback_range(datagram_id)
                                        .ok_or(RuntimeError::Protocol("datagram feedback range overflow"))?;
                                    udp_path_write_frame(
                                        &mut send,
                                        &Frame::DatagramFeedback {
                                            flow_id,
                                            received: vec![received],
                                        },
                                        context.codec_limits,
                                    ).await?;
                                    #[cfg(feature = "lab-diagnostics")]
                                    lab_diagnostic(
                                        "server_udp_datagram_feedback_written",
                                        format_args!(
                                            "session_id={} flow_id={} datagram_id={}",
                                            stream_context.session_id.0,
                                            flow_id.0,
                                            datagram_id.0,
                                        ),
                                    );
                                }
                                ServerDatagramSendOutcome::Full => {
                                    crate::observability::process_event!(
                                        Warn,
                                        "quic_datagram",
                                        "worker_queue_full",
                                        "QUIC UDP path datagram worker queue full; dropping request"
                                    );
                                }
                                ServerDatagramSendOutcome::Closed => {
                                    flows.retain(|flow| flow.flow_id() != flow_id);
                                    udp_path_write_frame(&mut send, &Frame::DatagramClose { flow_id }, context.codec_limits).await?;
                                }
                            }
                        }
                    }
                    Some(Ok(Frame::DatagramFeedback { flow_id, received })) => {
                        if let Some(flow) = flows.iter().find(|flow| flow.flow_id() == flow_id) {
                            flow.acknowledge_response(received);
                        }
                    }
                    Some(Ok(Frame::DatagramClose { flow_id })) => {
                        flows.retain(|flow| flow.flow_id() != flow_id);
                        tombstones.remove(flow_id);
                        if flows.is_empty() && tombstones.is_empty() {
                            if saw_silent_drop && send.cancel_pending_response() {
                                return Ok(());
                            }
                            let _ = udp_path_finish_stream(&mut send).await;
                            return Ok(());
                        }
                    }
                    Some(Ok(Frame::Ping { nonce })) => {
                        udp_path_write_frame(&mut send, &Frame::Pong { nonce }, context.codec_limits).await?;
                    }
                    Some(Ok(Frame::SessionClose { reason })) => {
                        context.retire_session(stream_context.session_id, reason);
                        return Err(RuntimeError::RemoteClosed(reason));
                    }
                    Some(Ok(_)) => return Err(RuntimeError::Protocol("unexpected server QUIC UDP path datagram stream frame")),
                    Some(Err(err)) if super::io::udp_path_input_finished(&err) => {
                        if saw_silent_drop {
                            let _ = send.cancel_pending_response();
                        }
                        return Ok(());
                    }
                    Some(Err(RuntimeError::ReliablePathSessionClosed)) | None => {
                        if saw_silent_drop {
                            let _ = send.cancel_pending_response();
                        }
                        return Ok(());
                    }
                    Some(Err(err)) => return Err(err),
                }
                if let Some(command) = try_recv_reliable_path_command(&mut commands_rx) {
                    let result = drain_server_udp_datagram_commands(
                        command,
                        &mut commands_rx,
                        &mut send,
                        &context,
                        &mut flows,
                        &mut pending_frames,
                    )
                    .await?;
                    if result {
                        if saw_silent_drop {
                            let _ = send.cancel_pending_response();
                        }
                        return Ok(());
                    }
                }
            }
            command = recv_reliable_path_command(&mut commands_rx), if command_may_recv => {
                if let Some(command) = command {
                    let result = drain_server_udp_datagram_commands(
                        command,
                        &mut commands_rx,
                        &mut send,
                        &context,
                        &mut flows,
                        &mut pending_frames,
                    ).await;
                    if result? {
                        if saw_silent_drop {
                            let _ = send.cancel_pending_response();
                        }
                        return Ok(());
                    }
                }
            }
        }
    }
}

async fn drain_server_udp_datagram_commands(
    first_command: ReliablePathCommand,
    commands: &mut ReliablePathCommandReceivers,
    send: &mut UdpPathSendStream,
    context: &ServerPathContext,
    flows: &mut Vec<AcceptedServerDatagramFlow>,
    pending_frames: &mut Vec<Frame>,
) -> Result<bool, RuntimeError> {
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
            flush_server_udp_datagram_frame_batch(
                send,
                pending_frames,
                context.codec_limits,
                commands,
                &mut pending_frame_command_bytes,
            )
            .await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=server underlay=Udp datagram=true sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
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
                    "server QUIC datagram writer received capacity data",
                ));
            }
            ReliablePathCommand::SendFrame(frame) => {
                if let Frame::DatagramClose { flow_id } = frame {
                    flows.retain(|flow| flow.flow_id() != flow_id);
                }
                pending_frames.push(frame);
                pending_frame_command_bytes =
                    pending_frame_command_bytes.saturating_add(pending_bytes);
                sent_bytes = sent_bytes.saturating_add(writer_run_bytes);
                sent_items = sent_items.saturating_add(1);
                if sent_bytes >= byte_budget || sent_items >= item_budget {
                    flush_server_udp_datagram_frame_batch(
                        send,
                        pending_frames,
                        context.codec_limits,
                        commands,
                        &mut pending_frame_command_bytes,
                    )
                    .await?;
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "path_writer_drain",
                        format_args!(
                            "role=server underlay=Udp datagram=true sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
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
                    "server QUIC datagram writer received TCP capacity command",
                ));
            }
            ReliablePathCommand::ResetAndCloseStream { .. } => {
                commands.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "server QUIC datagram writer received reliable terminal command",
                ));
            }
            ReliablePathCommand::CloseStream(_) => {
                flush_server_udp_datagram_frame_batch(
                    send,
                    pending_frames,
                    context.codec_limits,
                    commands,
                    &mut pending_frame_command_bytes,
                )
                .await?;
                let _ = udp_path_finish_stream(send).await;
                true
            }
            ReliablePathCommand::PrepareConnection { .. }
            | ReliablePathCommand::OpenStream { .. }
            | ReliablePathCommand::OpenDatagramAttachment { .. }
            | ReliablePathCommand::OpenDatagramFlow { .. }
            | ReliablePathCommand::SendDatagramFrame { .. }
            | ReliablePathCommand::CloseDatagramAttachment { .. } => {
                return Err(RuntimeError::Protocol(
                    "server QUIC UDP datagram stream received client TCP session command",
                ));
            }
            ReliablePathCommand::CancelTcpOpen { .. } => {
                return Err(RuntimeError::Protocol(
                    "server QUIC UDP datagram stream received TCP open cancellation",
                ));
            }
        };
        commands.release_pending_command_bytes(pending_bytes);
        if should_close {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=server underlay=Udp datagram=true sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
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
            flush_server_udp_datagram_frame_batch(
                send,
                pending_frames,
                context.codec_limits,
                commands,
                &mut pending_frame_command_bytes,
            )
            .await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=server underlay=Udp datagram=true sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
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

async fn flush_server_udp_datagram_frame_batch(
    send: &mut UdpPathSendStream,
    pending_frames: &mut Vec<Frame>,
    codec_limits: crate::protocol::codec::CodecLimits,
    commands: &ReliablePathCommandReceivers,
    pending_frame_command_bytes: &mut usize,
) -> Result<(), RuntimeError> {
    let result = flush_udp_frame_batch(send, pending_frames, codec_limits).await;
    commands.release_pending_command_bytes(std::mem::take(pending_frame_command_bytes));
    result
}

#[allow(
    clippy::too_many_arguments,
    reason = "the session actor passes borrowed queue state and authenticated flow identity without allocation"
)]
async fn open_server_udp_datagram_flow(
    context: &ServerPathContext,
    commands_tx: &ReliablePathCommandSender,
    send: &mut UdpPathSendStream,
    flows: &mut Vec<AcceptedServerDatagramFlow>,
    tombstones: &mut ServerDatagramTombstoneCache,
    session_id: SessionId,
    principal_permit: PrincipalPermit,
    ingress: ServerMppIngress,
    flow_id: DatagramFlowId,
    target: TargetAddr,
) -> Result<ServerUdpDatagramOpenOutcome, RuntimeError> {
    if flows.iter().any(|flow| flow.flow_id() == flow_id) {
        return Err(RuntimeError::Protocol(
            "duplicate QUIC UDP path datagram flow",
        ));
    }
    if let Some(tombstone) = tombstones.get(flow_id) {
        return write_server_udp_datagram_tombstone(send, context, flow_id, tombstone, None).await;
    }
    if flows.len() >= context.max_udp_flows_per_session {
        let tombstone = ServerDatagramTombstone::CapacityReject;
        let evicted = tombstones.insert_with_eviction(flow_id, tombstone);
        return write_server_udp_datagram_tombstone(send, context, flow_id, tombstone, evicted)
            .await;
    }
    let datagrams = context.datagrams.as_ref().ok_or(RuntimeError::Protocol(
        "L4 datagram service is unavailable for this MPP inbound",
    ))?;
    let flow = match datagrams
        .open(ServerDatagramOpenRequest {
            session_id,
            principal_permit,
            flow_id,
            target,
            commands: commands_tx.clone(),
            ingress,
        })
        .await
    {
        Ok(flow) => flow,
        Err(failure) => match failure.into_failure() {
            ServerDatagramOpenFailure::Capacity => {
                let tombstone = ServerDatagramTombstone::CapacityReject;
                let evicted = tombstones.insert_with_eviction(flow_id, tombstone);
                return write_server_udp_datagram_tombstone(
                    send, context, flow_id, tombstone, evicted,
                )
                .await;
            }
            ServerDatagramOpenFailure::Runtime(RuntimeError::RouteRejected) => {
                let tombstone = ServerDatagramTombstone::Reject;
                let evicted = tombstones.insert_with_eviction(flow_id, tombstone);
                return write_server_udp_datagram_tombstone(
                    send, context, flow_id, tombstone, evicted,
                )
                .await;
            }
            // QUIC can multiplex several Product datagram flows on this
            // request stream. A policy drop emits nothing and leaves every
            // accepted sibling intact.
            ServerDatagramOpenFailure::Runtime(RuntimeError::RouteDropped) => {
                let tombstone = ServerDatagramTombstone::Drop;
                let evicted = tombstones.insert_with_eviction(flow_id, tombstone);
                return write_server_udp_datagram_tombstone(
                    send, context, flow_id, tombstone, evicted,
                )
                .await;
            }
            ServerDatagramOpenFailure::Runtime(error) => return Err(error),
        },
    };
    flows.push(flow);
    Ok(ServerUdpDatagramOpenOutcome::Opened)
}

async fn write_server_udp_datagram_tombstone(
    send: &mut UdpPathSendStream,
    context: &ServerPathContext,
    flow_id: DatagramFlowId,
    tombstone: ServerDatagramTombstone,
    evicted: Option<DatagramFlowId>,
) -> Result<ServerUdpDatagramOpenOutcome, RuntimeError> {
    match tombstone {
        ServerDatagramTombstone::Reject | ServerDatagramTombstone::CapacityReject => {
            udp_path_write_datagram_refusal(
                send,
                flow_id,
                evicted,
                context.max_udp_flows_per_session,
                context.codec_limits,
            )
            .await?;
            Ok(ServerUdpDatagramOpenOutcome::Rejected)
        }
        ServerDatagramTombstone::Drop => {
            udp_path_retain_datagram_denial(
                send,
                flow_id,
                evicted,
                context.max_udp_flows_per_session,
            )?;
            Ok(ServerUdpDatagramOpenOutcome::Dropped)
        }
    }
}
