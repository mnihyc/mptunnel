//! Serialized writes and bounded command draining for a reliable TCP carrier.
//!
//! Only this owner batches commands or interlocks reads with a pending write,
//! preserving frame order without allowing feedback backpressure to deadlock.

use super::client_capacity::{ClientTcpCapacityProbeWriteOutcome, client_write_tcp_capacity_probe};
use super::client_datagram::ClientTcpDatagramState;
use super::client_receive::handle_client_tcp_path_frame;
use super::client_state::{ClientTcpPathConnection, ClientTcpPathSessionRuntime};
use super::client_stream::{
    ClientTcpOpenStreamRequest, ClientTcpPathStreamState,
    client_tcp_inbound_frame_retires_attachment, open_client_tcp_stream_on_connection,
    remove_matching_client_tcp_open,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::reliable_capacity_measurement_session_limit_bytes;
use crate::mux::MuxLimits;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::stream_ack_contiguous_frontier;
use crate::protocol::{Frame, PathId, StreamId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, TcpCapacityProbeCommand,
    reliable_path_command_pending_bytes, reliable_path_command_writer_run_budget_bytes,
    reliable_path_command_writer_run_budget_items, reliable_path_command_writer_run_bytes,
    reliable_path_frame_requires_capacity_command, reliable_path_writer_frame_queue,
    try_coalesce_reliable_path_writer_run, try_recv_reliable_path_command,
};
use crate::runtime::recent_ids::RecentIdCache;
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::mpsc;

#[derive(Clone, Copy)]
struct ClientTcpCommandOptions {
    carrier_generation: u64,
    stream_frame_queue: usize,
    flush_after_frame: bool,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_connected_client_tcp_command_run(
    first_command: ReliablePathCommand,
    commands: &mut ReliablePathCommandReceivers,
    connection: &mut ClientTcpPathConnection,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    datagrams: &mut ClientTcpDatagramState,
    runtime: &ClientTcpPathSessionRuntime,
    carrier_generation: u64,
    stream_frame_queue: usize,
    mux_limits: MuxLimits,
    pending_frames: &mut Vec<Frame>,
) -> Result<(), RuntimeError> {
    #[cfg(feature = "lab-diagnostics")]
    let drain_started = Instant::now();
    let byte_budget = reliable_path_command_writer_run_budget_bytes(mux_limits);
    let item_budget = reliable_path_command_writer_run_budget_items(mux_limits);
    let mut next_command = Some(first_command);
    pending_frames.clear();
    let mut sent_bytes = 0usize;
    let mut sent_items = 0usize;
    let mut wrote_frame = false;
    let mut terminal_stream_id = None;

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
            break;
        };
        let pending_bytes = reliable_path_command_pending_bytes(&command);
        let writer_run_bytes = reliable_path_command_writer_run_bytes(&command);
        match command {
            ReliablePathCommand::SendFrame(frame)
                if reliable_path_frame_requires_capacity_command(&frame) =>
            {
                commands.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "client TCP path received an untyped capacity frame",
                ));
            }
            ReliablePathCommand::SendFrame(frame) => {
                let is_stream_detach = matches!(&frame, Frame::StreamDetach { .. });
                #[cfg(feature = "lab-diagnostics")]
                if let Frame::StreamAck {
                    stream_id,
                    complete,
                    ranges,
                } = &frame
                {
                    lab_diagnostic(
                        "client_tcp_stream_ack_dequeue",
                        format_args!(
                            "stream_id={} path_index={} complete={} ranges={} frontier={} largest_end={} pending_bytes_after={}",
                            stream_id.0,
                            runtime.path_index,
                            complete,
                            ranges.len(),
                            stream_ack_contiguous_frontier(ranges),
                            ranges.last().map_or(0, |range| range.end),
                            commands.pending_bytes(),
                        ),
                    );
                }
                pending_frames.push(frame);
                commands.release_pending_command_bytes(pending_bytes);
                wrote_frame = true;
                sent_bytes = sent_bytes.saturating_add(writer_run_bytes);
                sent_items = sent_items.saturating_add(1);
                if is_stream_detach || sent_bytes >= byte_budget || sent_items >= item_budget {
                    break;
                }
                continue;
            }
            ReliablePathCommand::SendTcpCapacityProbe(probe) => {
                let stream_id = probe.stream_id;
                let path_instance = probe.path_instance;
                let request_current = probe.request_lease().is_current();
                let stream_is_attached = streams
                    .get(&stream_id)
                    .is_some_and(|state| state.pending_open.is_none());
                if !request_current || !stream_is_attached {
                    // A planner may revoke a queued probe after the stream or
                    // proof epoch changes, or the product stream may detach
                    // before dequeue. With no carrier bytes, both are normal
                    // canceled transactions rather than shared-path failures.
                    probe.request_lease().refund_if_unwritten();
                    commands.release_pending_command_bytes(pending_bytes);
                    return Ok(());
                }
                if probe.path_id != PathId(runtime.path_index as u16)
                    || path_instance.key.underlay != UnderlayProtocol::Tcp
                    || path_instance.key.index != runtime.path_index
                    || probe.train_payload_bytes < probe.sample_floor_bytes
                    || probe.train_payload_bytes
                        > reliable_capacity_measurement_session_limit_bytes(mux_limits)
                    || !probe.valid_request_tcp_train()
                    || connection.capacity.has_pending_request()
                {
                    commands.release_pending_command_bytes(pending_bytes);
                    return Err(RuntimeError::Protocol(
                        "request TCP capacity command does not match its writer",
                    ));
                }
                if let Err(error) = flush_client_tcp_frame_batch(
                    connection,
                    pending_frames,
                    streams,
                    closed_streams,
                    datagrams,
                    runtime,
                )
                .await
                {
                    commands.release_pending_command_bytes(pending_bytes);
                    return Err(error);
                }
                if Instant::now() >= probe.expires_at {
                    probe.request_lease().refund_if_unwritten();
                    commands.release_pending_command_bytes(pending_bytes);
                    return Ok(());
                }
                let measurement_result = client_write_tcp_capacity_probe_interlocked(
                    connection,
                    &probe,
                    mux_limits.max_payload_bytes,
                    streams,
                    closed_streams,
                    datagrams,
                    mux_limits,
                )
                .await;
                commands.release_pending_command_bytes(pending_bytes);
                let (write_outcome, deferred_frames) = measurement_result?;
                connection.record_outbound_activity();
                match write_outcome {
                    ClientTcpCapacityProbeWriteOutcome::NoWire => {
                        probe.request_lease().refund_if_unwritten();
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "request_tcp_capacity_probe",
                            format_args!(
                                "phase=discarded reason=no_wire stream_id={} path_index={} instance_id={} measurement_id={}",
                                stream_id.0,
                                runtime.path_index,
                                path_instance.attachment_id,
                                probe.measurement_id,
                            ),
                        );
                    }
                    ClientTcpCapacityProbeWriteOutcome::Measured(measurement) => {
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "request_tcp_capacity_probe",
                            format_args!(
                                "phase=sent stream_id={} path_index={} instance_id={} measurement_id={} train_bytes={} train_wire_bytes={} sample_floor_bytes={} warmup_bytes={} timing_slack_bytes={} required_timed_bytes={}",
                                stream_id.0,
                                runtime.path_index,
                                path_instance.attachment_id,
                                probe.measurement_id,
                                probe.train_payload_bytes,
                                measurement.train_wire_bytes,
                                probe.sample_floor_bytes,
                                probe.warmup_carrier_bytes,
                                probe.timing_slack_bytes,
                                probe.required_timed_carrier_bytes,
                            ),
                        );
                        connection.capacity.publish_request(probe, measurement);
                    }
                }
                for frame in deferred_frames {
                    handle_client_tcp_path_frame(
                        frame,
                        connection,
                        streams,
                        closed_streams,
                        datagrams,
                        runtime,
                    )
                    .await?;
                }
                return Ok(());
            }
            ReliablePathCommand::ResetAndCloseStream { stream_id, reason } => {
                pending_frames.push(Frame::StreamReset { stream_id, reason });
                commands.release_pending_command_bytes(pending_bytes);
                wrote_frame = true;
                #[cfg(feature = "lab-diagnostics")]
                {
                    sent_bytes = sent_bytes.saturating_add(writer_run_bytes);
                    sent_items = sent_items.saturating_add(1);
                }
                terminal_stream_id = Some(stream_id);
                break;
            }
            command => {
                flush_client_tcp_frame_batch(
                    connection,
                    pending_frames,
                    streams,
                    closed_streams,
                    datagrams,
                    runtime,
                )
                .await?;
                handle_connected_client_tcp_command(
                    command,
                    connection,
                    streams,
                    closed_streams,
                    datagrams,
                    ClientTcpCommandOptions {
                        carrier_generation,
                        stream_frame_queue,
                        flush_after_frame: false,
                    },
                )
                .await?;
                commands.release_pending_command_bytes(pending_bytes);
            }
        }
        sent_items = sent_items.saturating_add(1);
        if sent_bytes >= byte_budget || sent_items >= item_budget {
            break;
        }
    }

    flush_client_tcp_frame_batch(
        connection,
        pending_frames,
        streams,
        closed_streams,
        datagrams,
        runtime,
    )
    .await?;
    if let Some(stream_id) = terminal_stream_id {
        // TCP sessions are shared: terminal product state retires only this
        // stream after its reset is committed to the carrier writer.
        streams.remove(&stream_id);
        closed_streams.insert(stream_id);
    }

    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "path_writer_drain",
        format_args!(
            "role=client underlay=Tcp sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
            sent_items,
            sent_bytes,
            byte_budget,
            item_budget,
            commands.pending_bytes(),
            drain_started.elapsed().as_micros(),
            sent_bytes >= byte_budget,
            sent_items >= item_budget,
        ),
    );
    if wrote_frame {
        connection.carrier.writer.flush().await?;
    }
    runtime.observe_sender_transport_state(connection, false);
    Ok(())
}

async fn flush_client_tcp_frame_batch(
    connection: &mut ClientTcpPathConnection,
    frames: &mut Vec<Frame>,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    datagrams: &mut ClientTcpDatagramState,
    runtime: &ClientTcpPathSessionRuntime,
) -> Result<(), RuntimeError> {
    if frames.is_empty() {
        return Ok(());
    }
    let mut deferred_frame = None;
    let mut routed_frames = 0usize;
    {
        let write = connection.carrier.writer.write_frames(frames);
        tokio::pin!(write);
        loop {
            tokio::select! {
                biased;
                result = &mut write => {
                    result?;
                    break;
                }
                incoming = connection.carrier.frames.recv(), if deferred_frame.is_none() => {
                    match incoming {
                        Some(Ok(frame)) => {
                            match try_route_client_tcp_frame_during_write(
                                frame,
                                streams,
                                closed_streams,
                                datagrams,
                            )? {
                                ClientTcpWriteFrameRoute::Routed => {
                                    routed_frames = routed_frames.saturating_add(1);
                                }
                                ClientTcpWriteFrameRoute::Barrier(frame) => {
                                    deferred_frame = Some(frame);
                                }
                            }
                        }
                        Some(Err(err)) => return Err(RuntimeError::Encrypted(err)),
                        None => return Err(RuntimeError::ReliablePathSessionClosed),
                    }
                }
            }
        }
    }
    #[cfg(feature = "lab-diagnostics")]
    for frame in frames.iter() {
        if let Frame::StreamAck {
            stream_id,
            complete,
            ranges,
        } = frame
        {
            lab_diagnostic(
                "client_tcp_stream_ack_write_complete",
                format_args!(
                    "stream_id={} path_index={} complete={} ranges={} frontier={} largest_end={}",
                    stream_id.0,
                    runtime.path_index,
                    complete,
                    ranges.len(),
                    stream_ack_contiguous_frontier(ranges),
                    ranges.last().map_or(0, |range| range.end),
                ),
            );
        }
    }
    for frame in frames.iter() {
        connection.path_proofs.record_sent_frame(frame);
    }
    frames.clear();
    connection.record_outbound_activity();
    #[cfg(feature = "lab-diagnostics")]
    if routed_frames > 0 || deferred_frame.is_some() {
        lab_diagnostic(
            "client_tcp_write_feedback_interlock",
            format_args!(
                "path_index={} routed_frames={} deferred_frames={}",
                runtime.path_index,
                routed_frames,
                usize::from(deferred_frame.is_some()),
            ),
        );
    }
    if let Some(frame) = deferred_frame {
        handle_client_tcp_path_frame(
            frame,
            connection,
            streams,
            closed_streams,
            datagrams,
            runtime,
        )
        .await?;
    }
    Ok(())
}

async fn client_write_tcp_capacity_probe_interlocked(
    connection: &mut ClientTcpPathConnection,
    probe: &TcpCapacityProbeCommand,
    max_payload_bytes: usize,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    datagrams: &mut ClientTcpDatagramState,
    mux_limits: MuxLimits,
) -> Result<(ClientTcpCapacityProbeWriteOutcome, Vec<Frame>), RuntimeError> {
    let write = client_write_tcp_capacity_probe(
        &mut connection.carrier.writer,
        connection.carrier.tcp_metrics.as_ref(),
        probe,
        max_payload_bytes,
    );
    tokio::pin!(write);
    let deferred_limit = reliable_path_writer_frame_queue(mux_limits).max(1);
    let mut deferred_frames = Vec::new();
    let mut defer_all = false;
    let mut routed_frames = 0usize;
    let mut deferred_error = None;
    let mut reader_open = true;
    let measurement = loop {
        tokio::select! {
            biased;
            result = &mut write => {
                break result;
            }
            incoming = connection.carrier.frames.recv(), if reader_open => {
                let frame = match incoming {
                    Some(Ok(frame)) => frame,
                    Some(Err(error)) => {
                        deferred_error.get_or_insert(RuntimeError::Encrypted(error));
                        reader_open = false;
                        continue;
                    }
                    None => {
                        deferred_error.get_or_insert(RuntimeError::ReliablePathSessionClosed);
                        reader_open = false;
                        continue;
                    }
                };
                if deferred_error.is_some() {
                    continue;
                }
                if !defer_all {
                    match try_route_client_tcp_frame_during_write(
                        frame,
                        streams,
                        closed_streams,
                        datagrams,
                    )? {
                        ClientTcpWriteFrameRoute::Routed => {
                            routed_frames = routed_frames.saturating_add(1);
                            continue;
                        }
                        ClientTcpWriteFrameRoute::Barrier(frame) => {
                            defer_all = true;
                            deferred_frames.push(frame);
                        }
                    }
                } else if deferred_frames.len() < deferred_limit {
                    deferred_frames.push(frame);
                } else {
                    // Continue draining so the peer can read the in-progress
                    // probe, then fail the carrier and let reliable reinjection own
                    // any product frames that could not remain ordered here.
                    deferred_error.get_or_insert(RuntimeError::Protocol(
                        "request TCP capacity feedback interlock overflowed",
                    ));
                    deferred_frames.clear();
                }
            }
        }
    }?;
    if let Some(error) = deferred_error {
        return Err(error);
    }
    #[cfg(feature = "lab-diagnostics")]
    if routed_frames > 0 || !deferred_frames.is_empty() {
        lab_diagnostic(
            "request_tcp_capacity_feedback_interlock",
            format_args!(
                "routed_frames={} deferred_frames={}",
                routed_frames,
                deferred_frames.len(),
            ),
        );
    }
    Ok((measurement, deferred_frames))
}

enum ClientTcpWriteFrameRoute {
    Routed,
    Barrier(Frame),
}

fn try_route_client_tcp_frame_during_write(
    frame: Frame,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    datagrams: &mut ClientTcpDatagramState,
) -> Result<ClientTcpWriteFrameRoute, RuntimeError> {
    if matches!(
        &frame,
        Frame::DatagramData { .. } | Frame::DatagramFeedback { .. } | Frame::DatagramClose { .. }
    ) {
        datagrams.route_inbound(frame)?;
        return Ok(ClientTcpWriteFrameRoute::Routed);
    }
    let stream_id = match &frame {
        Frame::StreamMaxData { stream_id, .. }
        | Frame::StreamReset { stream_id, .. }
        | Frame::StreamData { stream_id, .. }
        | Frame::StreamAck { stream_id, .. }
        | Frame::StreamFin { stream_id, .. } => *stream_id,
        Frame::StreamDetach { stream_id } => {
            if streams
                .get(stream_id)
                .is_some_and(|state| state.pending_open.is_some())
            {
                return Ok(ClientTcpWriteFrameRoute::Barrier(Frame::StreamDetach {
                    stream_id: *stream_id,
                }));
            }
            streams.remove(stream_id);
            closed_streams.insert(*stream_id);
            return Ok(ClientTcpWriteFrameRoute::Routed);
        }
        _ => return Ok(ClientTcpWriteFrameRoute::Barrier(frame)),
    };
    if streams
        .get(&stream_id)
        .is_some_and(|state| state.pending_open.is_some())
    {
        return Ok(ClientTcpWriteFrameRoute::Barrier(frame));
    }
    let retires_attachment = client_tcp_inbound_frame_retires_attachment(&frame);
    let Some(state) = streams.get_mut(&stream_id) else {
        closed_streams.insert(stream_id);
        return Ok(ClientTcpWriteFrameRoute::Routed);
    };
    let send_result = state.frames.try_send(Ok(frame));
    match send_result {
        Ok(()) => {
            if retires_attachment {
                streams.remove(&stream_id);
                closed_streams.insert(stream_id);
            }
            Ok(ClientTcpWriteFrameRoute::Routed)
        }
        Err(mpsc::error::TrySendError::Full(Ok(frame))) => {
            Ok(ClientTcpWriteFrameRoute::Barrier(frame))
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            streams.remove(&stream_id);
            closed_streams.insert(stream_id);
            Ok(ClientTcpWriteFrameRoute::Routed)
        }
        Err(mpsc::error::TrySendError::Full(Err(_))) => {
            unreachable!("client TCP interlock only routes successful frames")
        }
    }
}

async fn handle_connected_client_tcp_command(
    command: ReliablePathCommand,
    connection: &mut ClientTcpPathConnection,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    datagrams: &mut ClientTcpDatagramState,
    options: ClientTcpCommandOptions,
) -> Result<(), RuntimeError> {
    let ClientTcpCommandOptions {
        carrier_generation,
        stream_frame_queue,
        flush_after_frame,
    } = options;
    match command {
        ReliablePathCommand::PrepareConnection { response, .. } => {
            let _ = response.send(Ok(None));
            Ok(())
        }
        ReliablePathCommand::OpenStream {
            stream_id,
            attempt_id,
            observed_carrier_generation,
            target,
            lane,
            open_deadlines,
            session_commands,
            response,
        } => {
            let open_deadline = open_deadlines
                .for_carrier_generation(observed_carrier_generation, carrier_generation);
            let open = ClientTcpOpenStreamRequest {
                stream_id,
                attempt_id,
                target,
                lane,
                open_deadline,
                session_commands,
                response,
            };
            open_client_tcp_stream_on_connection(connection, open, streams, stream_frame_queue)
                .await?;
            connection.record_outbound_activity();
            Ok(())
        }
        ReliablePathCommand::CancelTcpOpen {
            stream_id,
            attempt_id,
        } => {
            if remove_matching_client_tcp_open(streams, stream_id, attempt_id).is_none() {
                return Ok(());
            }
            closed_streams.insert(stream_id);
            let detach = Frame::StreamDetach { stream_id };
            connection.carrier.writer.write_frame(&detach).await?;
            connection.path_proofs.record_sent_frame(&detach);
            connection.carrier.writer.flush().await?;
            connection.record_outbound_activity();
            Ok(())
        }
        ReliablePathCommand::OpenDatagramAttachment {
            attachment_id,
            frames,
            failure,
            open_deadline,
            response,
        } => {
            if response.is_closed() || open_deadline <= tokio::time::Instant::now() {
                let _ = response.send(Err(RuntimeError::PathOpenTimedOut));
                return Ok(());
            }
            if let Err(err) = datagrams.attach(attachment_id, frames, failure) {
                let _ = response.send(Err(err));
                return Ok(());
            }
            if response.send(Ok(connection.path_instance_id)).is_err() {
                datagrams.remove_attachment(attachment_id);
            }
            Ok(())
        }
        ReliablePathCommand::OpenDatagramFlow {
            attachment_id,
            flow_id,
            target,
            open_deadline,
            response,
        } => {
            if response.is_closed() || open_deadline <= tokio::time::Instant::now() {
                let _ = response.send(Err(RuntimeError::PathOpenTimedOut));
                return Ok(());
            }
            let should_write = match datagrams.validate_open_flow(attachment_id, flow_id, &target) {
                Ok(should_write) => should_write,
                Err(err) => {
                    let _ = response.send(Err(err));
                    return Ok(());
                }
            };
            if should_write {
                let frame = Frame::OpenDatagramFlow {
                    flow_id,
                    target: target.clone(),
                };
                connection.carrier.writer.write_frame(&frame).await?;
                connection.carrier.writer.flush().await?;
                connection.path_proofs.record_sent_frame(&frame);
                connection.record_outbound_activity();
                datagrams.commit_open_flow(attachment_id, flow_id, target);
            }
            let _ = response.send(Ok(()));
            Ok(())
        }
        ReliablePathCommand::SendDatagramFrame {
            attachment_id,
            frame,
            write_deadline,
            expires_at,
            response,
        } => {
            if response.is_closed() || write_deadline <= tokio::time::Instant::now() {
                let _ = response.send(Err(RuntimeError::PathOpenTimedOut));
                return Ok(());
            }
            if let Err(err) = datagrams.validate_outbound(attachment_id, &frame) {
                let _ = response.send(Err(err));
                return Ok(());
            }
            let frame = match refresh_client_tcp_datagram_ttl(frame, expires_at) {
                Ok(frame) => frame,
                Err(err) => {
                    let _ = response.send(Err(err));
                    return Ok(());
                }
            };
            connection.carrier.writer.write_frame(&frame).await?;
            connection.carrier.writer.flush().await?;
            connection.path_proofs.record_sent_frame(&frame);
            connection.record_outbound_activity();
            let _ = response.send(Ok(()));
            Ok(())
        }
        ReliablePathCommand::CloseDatagramAttachment {
            attachment_id,
            response,
        } => {
            let flow_ids = datagrams.attachment_flow_ids(attachment_id);
            let wrote_close = !flow_ids.is_empty();
            for flow_id in flow_ids {
                let frame = Frame::DatagramClose { flow_id };
                connection.carrier.writer.write_frame(&frame).await?;
                connection.path_proofs.record_sent_frame(&frame);
            }
            if wrote_close {
                connection.carrier.writer.flush().await?;
                connection.record_outbound_activity();
            }
            datagrams.remove_attachment(attachment_id);
            if let Some(response) = response {
                let _ = response.send(Ok(()));
            }
            Ok(())
        }
        ReliablePathCommand::SendFrame(frame)
            if reliable_path_frame_requires_capacity_command(&frame) =>
        {
            Err(RuntimeError::Protocol(
                "client TCP path received an untyped capacity frame",
            ))
        }
        ReliablePathCommand::SendFrame(frame) => {
            connection.carrier.writer.write_frame(&frame).await?;
            connection.path_proofs.record_sent_frame(&frame);
            if flush_after_frame {
                connection.carrier.writer.flush().await?;
            }
            connection.record_outbound_activity();
            Ok(())
        }
        ReliablePathCommand::SendTcpCapacityProbe(_) => Err(RuntimeError::Protocol(
            "client TCP path received server capacity command",
        )),
        ReliablePathCommand::ResetAndCloseStream { stream_id, reason } => {
            let reset = Frame::StreamReset { stream_id, reason };
            connection.carrier.writer.write_frame(&reset).await?;
            connection.path_proofs.record_sent_frame(&reset);
            connection.carrier.writer.flush().await?;
            connection.record_outbound_activity();
            streams.remove(&stream_id);
            closed_streams.insert(stream_id);
            Ok(())
        }
        ReliablePathCommand::CloseStream(stream_id) => {
            streams.remove(&stream_id);
            closed_streams.insert(stream_id);
            Ok(())
        }
    }
}

fn refresh_client_tcp_datagram_ttl(
    frame: Frame,
    expires_at: Option<tokio::time::Instant>,
) -> Result<Frame, RuntimeError> {
    let (flow_id, datagram_id, payload) = match frame {
        Frame::DatagramData {
            flow_id,
            datagram_id,
            ttl_ms: _,
            payload,
        } => (flow_id, datagram_id, payload),
        frame => {
            if expires_at.is_some() {
                return Err(RuntimeError::Protocol(
                    "TCP datagram feedback carried a product deadline",
                ));
            }
            return Ok(frame);
        }
    };
    let expires_at = expires_at.ok_or(RuntimeError::Protocol(
        "TCP datagram data omitted its product deadline",
    ))?;
    let remaining = expires_at.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return Err(RuntimeError::PathOpenTimedOut);
    }
    let ttl_ms = remaining.as_millis().max(1).min(u128::from(u32::MAX)) as u32;
    Ok(Frame::DatagramData {
        flow_id,
        datagram_id,
        ttl_ms,
        payload,
    })
}

#[cfg(test)]
#[path = "client_writer_test.rs"]
mod tests;
