//! Server application-datagram stream lifecycle over QUIC.

use super::io::*;
use super::*;

pub(super) struct ServerUdpDatagramStreamContext {
    pub(super) session_id: SessionId,
    pub(super) flow_id: DatagramFlowId,
    pub(super) target: TargetAddr,
    pub(super) lane: FlowLane,
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
    let mut flows = Vec::<ServerDatagramFlow>::new();
    let mut pending_frames = Vec::<Frame>::new();
    open_server_udp_datagram_flow(
        &context,
        &commands_tx,
        &mut send,
        &mut flows,
        stream_context.session_id,
        stream_context.flow_id,
        stream_context.target,
        stream_context.lane,
    )
    .await?;
    loop {
        let command_may_recv = !reliable_path_receivers_closed(&commands_rx);
        tokio::select! {
            biased;
            frame = carrier_frames.recv() => {
                match frame {
                    Some(Ok(Frame::OpenDatagramFlow { flow_id, target, .. })) => {
                        open_server_udp_datagram_flow(
                            &context,
                            &commands_tx,
                            &mut send,
                            &mut flows,
                            stream_context.session_id,
                            flow_id,
                            target,
                            FlowLane::RealtimeDatagram,
                        ).await?;
                    }
                    Some(Ok(Frame::DatagramData { flow_id, datagram_id, ttl_ms, payload })) => {
                        if ttl_ms == 0 {
                            return Err(RuntimeError::Protocol("expired QUIC UDP path datagram received"));
                        }
                        let flow_index = flows
                            .iter()
                            .position(|flow| flow.flow_id == flow_id)
                            .ok_or(RuntimeError::Protocol("unknown QUIC UDP path datagram flow"))?;
                        let requests = flows
                            .get(flow_index)
                            .ok_or(RuntimeError::Protocol("unknown QUIC UDP path datagram flow"))?
                            .requests
                            .clone();
                        match requests.try_send(ServerDatagramRequest { datagram_id, ttl_ms, payload }) {
                            Ok(()) => {
                                udp_path_write_frame(
                                    &mut send,
                                    &Frame::DatagramFeedback {
                                        flow_id,
                                        received: vec![datagram_ack_range(datagram_id)?],
                                    },
                                    context.codec_limits,
                                ).await?;
                            }
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                eprintln!("warning: QUIC UDP path datagram worker queue full; dropping request");
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                flows.retain(|flow| flow.flow_id != flow_id);
                                udp_path_write_frame(&mut send, &Frame::DatagramClose { flow_id }, context.codec_limits).await?;
                            }
                        }
                    }
                    Some(Ok(Frame::DatagramFeedback { .. })) => {}
                    Some(Ok(Frame::DatagramClose { flow_id })) => {
                        flows.retain(|flow| flow.flow_id != flow_id);
                        if flows.is_empty() {
                            let _ = udp_path_finish_stream(&mut send);
                            return Ok(());
                        }
                    }
                    Some(Ok(Frame::Ping { nonce })) => {
                        udp_path_write_frame(&mut send, &Frame::Pong { nonce }, context.codec_limits).await?;
                    }
                    Some(Ok(Frame::SessionClose { reason })) => return Err(RuntimeError::RemoteClosed(reason)),
                    Some(Ok(_)) => return Err(RuntimeError::Protocol("unexpected server QUIC UDP path datagram stream frame")),
                    Some(Err(RuntimeError::ReliablePathSessionClosed)) | None => return Ok(()),
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
                        return Ok(());
                    }
                }
            }
            command = recv_reliable_path_command(&mut commands_rx), if command_may_recv => {
                match command {
                    Some(command) => {
                        let result = drain_server_udp_datagram_commands(
                            command,
                            &mut commands_rx,
                            &mut send,
                            &context,
                            &mut flows,
                            &mut pending_frames,
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

async fn drain_server_udp_datagram_commands(
    first_command: ReliablePathCommand,
    commands: &mut ReliablePathCommandReceivers,
    send: &mut UdpPathSendStream,
    context: &ServerPathContext,
    flows: &mut Vec<ServerDatagramFlow>,
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
            flush_udp_frame_batch(send, pending_frames, context.codec_limits).await?;
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
                    flows.retain(|flow| flow.flow_id != flow_id);
                }
                pending_frames.push(frame);
                commands.release_pending_command_bytes(pending_bytes);
                sent_bytes = sent_bytes.saturating_add(writer_run_bytes);
                sent_items = sent_items.saturating_add(1);
                if sent_bytes >= byte_budget || sent_items >= item_budget {
                    flush_udp_frame_batch(send, pending_frames, context.codec_limits).await?;
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
            ReliablePathCommand::SendQuicCapacityProbe(probe) => {
                probe.ticket.cancel();
                commands.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "server QUIC datagram writer received reliable capacity command",
                ));
            }
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
                flush_udp_frame_batch(send, pending_frames, context.codec_limits).await?;
                let _ = udp_path_finish_stream(send);
                true
            }
            ReliablePathCommand::OpenStream { .. } => {
                return Err(RuntimeError::Protocol(
                    "server QUIC UDP path datagram stream received open command",
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
            flush_udp_frame_batch(send, pending_frames, context.codec_limits).await?;
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

async fn open_server_udp_datagram_flow(
    context: &ServerPathContext,
    commands_tx: &ReliablePathCommandSender,
    send: &mut UdpPathSendStream,
    flows: &mut Vec<ServerDatagramFlow>,
    session_id: SessionId,
    flow_id: DatagramFlowId,
    target: TargetAddr,
    _lane: FlowLane,
) -> Result<(), RuntimeError> {
    if flows.iter().any(|flow| flow.flow_id == flow_id) {
        return Err(RuntimeError::Protocol(
            "duplicate QUIC UDP path datagram flow",
        ));
    }
    if flows.len() >= context.max_udp_flows_per_session {
        udp_path_write_frame(
            send,
            &Frame::DatagramClose { flow_id },
            context.codec_limits,
        )
        .await?;
        return Ok(());
    }
    outbound::validate_target(&target)?;
    context.outbound.ensure_supports(TargetProtocol::Udp)?;
    let realtime_registration = context.reliable_streams.register_realtime_flow(session_id);
    let outbound_socket = match outbound::connect_udp(
        &context.outbound,
        &context.outbound_dns,
        &target,
        context.outbound_connect_timeout,
    )
    .await
    {
        Ok(socket) => socket,
        Err(err) => {
            udp_path_write_frame(
                send,
                &Frame::DatagramClose { flow_id },
                context.codec_limits,
            )
            .await?;
            return Err(RuntimeError::OutboundConnect(err));
        }
    };
    let requests = spawn_server_datagram_flow_worker(
        flow_id,
        outbound_socket,
        commands_tx.clone(),
        context.mux_limits,
    );
    flows.push(ServerDatagramFlow {
        flow_id,
        requests,
        _realtime_registration: realtime_registration,
    });
    Ok(())
}
