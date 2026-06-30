use super::*;

pub(super) async fn handle_server_path(
    stream: TcpStream,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    let mut framed = EncryptedFramedStream::with_cipher_suite(
        stream,
        context.security.secret.as_bytes(),
        PeerRole::Server,
        context.codec_limits,
        context.security.cipher,
    );
    let session_id = match framed.read_frame().await? {
        Frame::SessionHello { session_id } => session_id,
        _ => return Err(RuntimeError::Protocol("expected SESSION_HELLO")),
    };
    let authenticator = SessionAuthenticator::new(context.security.secret.as_bytes())?;
    let now_unix_secs = current_unix_secs()?;
    let auth_freshness_window_secs = context.security.auth_freshness_window.as_secs();
    match framed.read_frame().await? {
        Frame::SessionAuth {
            session_id: auth_session_id,
            nonce,
            issued_at_unix_secs,
            auth_tag,
        } if auth_session_id == session_id
            && authenticator.verify_session_auth(SessionAuthCheck {
                session_id,
                nonce,
                issued_at_unix_secs,
                tag: auth_tag,
                now_unix_secs,
                freshness_window_secs: auth_freshness_window_secs,
            }) => {}
        _ => return Err(RuntimeError::Protocol("invalid SESSION_AUTH")),
    }
    let (path_id, path_capabilities) = match framed.read_frame().await? {
        Frame::PathJoin {
            session_id: join_session_id,
            path_id,
            underlay,
            nonce,
            issued_at_unix_secs,
            capabilities,
            auth_tag,
        } if join_session_id == session_id
            && underlay == UnderlayProtocol::Tcp
            && authenticator.verify_path_join(PathJoinAuthCheck {
                session_id,
                path_id,
                underlay,
                nonce,
                issued_at_unix_secs,
                capabilities,
                tag: auth_tag,
                now_unix_secs,
                freshness_window_secs: auth_freshness_window_secs,
            })
            && context.accept_path_join_nonce(session_id, path_id, underlay, nonce) =>
        {
            (path_id, capabilities)
        }
        _ => return Err(RuntimeError::Protocol("invalid PATH_JOIN")),
    };
    framed.write_frame(&Frame::SessionReady).await?;
    framed
        .write_frame(&Frame::PathStatus {
            path_id,
            status: crate::protocol::PathStatus::Active,
            capabilities: path_capabilities,
        })
        .await?;
    if let Err(err) = framed.flush().await {
        if encrypted_framed_peer_closed(&err) {
            return Ok(());
        }
        return Err(RuntimeError::Encrypted(err));
    }

    let (reader, mut writer) = framed.split();
    let mut path_frames =
        spawn_encrypted_tcp_reader(reader, tcp_path_session_frame_queue(context.mux_limits));
    let (commands_tx, mut commands_rx) =
        tcp_path_session_command_channels(tcp_server_session_command_queue(&context));
    let mut attached_streams = HashSet::new();
    let mut datagram_flows = Vec::<ServerUdpDatagramFlow>::new();
    let mut draining = false;

    loop {
        let Some(event) = recv_server_tcp_path_event(&mut path_frames, &mut commands_rx).await?
        else {
            return Ok(());
        };
        match event {
            ServerTcpPathEvent::Command(command) => {
                if let TcpPathSessionCommand::SendFrame(Frame::DatagramClose { flow_id }) = &command
                {
                    datagram_flows.retain(|flow| flow.flow_id != *flow_id);
                }
                if !handle_server_tcp_path_command(
                    command,
                    &mut writer,
                    &context,
                    &mut attached_streams,
                    ServerTcpPathCommandContext {
                        session_id,
                        path_id,
                        commands_tx: &commands_tx,
                        draining,
                        active_datagram_flows: datagram_flows.len(),
                    },
                )
                .await?
                {
                    return Ok(());
                }
            }
            ServerTcpPathEvent::Frame(frame) => match frame {
                Frame::OpenStream {
                    stream_id,
                    target,
                    demand,
                    role,
                    ..
                } if !draining => {
                    outbound::validate_target(&target)?;
                    context.outbound.ensure_supports(TargetProtocol::Tcp)?;
                    let lane = flow_lane_from_stream_demand_hint(demand);
                    match context.tcp_streams.open_or_attach(
                        ServerTcpStreamOpenRequest {
                            session_id,
                            stream_id,
                            target: &target,
                            lane,
                            attachment: ServerTcpPathAttachment {
                                path_id,
                                underlay: UnderlayProtocol::Tcp,
                                commands: commands_tx.clone(),
                                max_frame_payload_bytes: tcp_relay_buffer_len(context.mux_limits),
                                role,
                            },
                        },
                        context.mux_limits,
                        context.max_tcp_streams,
                    )? {
                        ServerTcpStreamOpen::New(stream) => {
                            attached_streams.insert(stream_id);
                            let stream_context = context.clone();
                            tokio::spawn(async move {
                                if let Err(err) = run_server_tcp_stream(
                                    stream_context,
                                    session_id,
                                    stream,
                                    target,
                                )
                                .await
                                {
                                    eprintln!("warning: server TCP stream failed: {err}");
                                }
                            });
                        }
                        ServerTcpStreamOpen::Existing => {
                            attached_streams.insert(stream_id);
                            context
                                .tcp_streams
                                .route_frame(
                                    session_id,
                                    stream_id,
                                    Frame::PathStatus {
                                        path_id,
                                        status: crate::protocol::PathStatus::Active,
                                        capabilities: path_capabilities,
                                    },
                                )
                                .await?;
                            if !server_write_tcp_path_frame(
                                &mut writer,
                                &Frame::StreamMaxData {
                                    stream_id,
                                    max_offset: context.mux_limits.max_stream_window_bytes,
                                },
                            )
                            .await?
                            {
                                return Ok(());
                            }
                        }
                    }
                }
                Frame::OpenStream { stream_id, .. } => {
                    if !server_write_tcp_path_frame(
                        &mut writer,
                        &Frame::StreamReset {
                            stream_id,
                            reason: ResetReason::Refused,
                        },
                    )
                    .await?
                    {
                        return Ok(());
                    }
                }
                Frame::OpenDatagramFlow {
                    flow_id, target, ..
                } if !draining => {
                    if datagram_flows.iter().any(|flow| flow.flow_id == flow_id) {
                        return Err(RuntimeError::Protocol("duplicate TCP datagram flow"));
                    }
                    if datagram_flows.len() >= context.max_udp_flows_per_session {
                        if !server_write_tcp_path_frame(
                            &mut writer,
                            &Frame::DatagramClose { flow_id },
                        )
                        .await?
                        {
                            return Ok(());
                        }
                        continue;
                    }
                    outbound::validate_target(&target)?;
                    context.outbound.ensure_supports(TargetProtocol::Udp)?;
                    let outbound_socket = match outbound::connect_udp(
                        &context.outbound,
                        &context.outbound_dns,
                        &target,
                        Duration::from_secs(10),
                    )
                    .await
                    {
                        Ok(socket) => socket,
                        Err(err) => {
                            if !server_write_tcp_path_frame(
                                &mut writer,
                                &Frame::DatagramClose { flow_id },
                            )
                            .await?
                            {
                                return Ok(());
                            }
                            return Err(RuntimeError::OutboundConnect(err));
                        }
                    };
                    let requests = spawn_server_udp_datagram_flow_worker(
                        flow_id,
                        outbound_socket,
                        commands_tx.clone(),
                        context.mux_limits,
                    );
                    datagram_flows.push(ServerUdpDatagramFlow { flow_id, requests });
                }
                Frame::OpenDatagramFlow { flow_id, .. } => {
                    if !server_write_tcp_path_frame(&mut writer, &Frame::DatagramClose { flow_id })
                        .await?
                    {
                        return Ok(());
                    }
                }
                Frame::DatagramData {
                    flow_id,
                    datagram_id,
                    ttl_ms,
                    payload,
                } => {
                    if ttl_ms == 0 {
                        return Err(RuntimeError::Protocol("expired TCP datagram received"));
                    }
                    let flow_index = datagram_flows
                        .iter()
                        .position(|flow| flow.flow_id == flow_id)
                        .ok_or(RuntimeError::Protocol("unknown TCP datagram flow"))?;
                    let requests = datagram_flows
                        .get(flow_index)
                        .ok_or(RuntimeError::Protocol("unknown TCP datagram flow"))?
                        .requests
                        .clone();
                    match requests.try_send(ServerUdpDatagramRequest {
                        datagram_id,
                        ttl_ms,
                        payload,
                    }) {
                        Ok(()) => {
                            if !server_write_tcp_path_frame(
                                &mut writer,
                                &Frame::DatagramFeedback {
                                    flow_id,
                                    received: vec![datagram_ack_range(datagram_id)?],
                                },
                            )
                            .await?
                            {
                                return Ok(());
                            }
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            eprintln!("warning: TCP datagram worker queue full; dropping request");
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            datagram_flows.retain(|flow| flow.flow_id != flow_id);
                            if !server_write_tcp_path_frame(
                                &mut writer,
                                &Frame::DatagramClose { flow_id },
                            )
                            .await?
                            {
                                return Ok(());
                            }
                        }
                    }
                }
                Frame::DatagramFeedback { .. } => {}
                Frame::DatagramClose { flow_id } => {
                    datagram_flows.retain(|flow| flow.flow_id != flow_id);
                    if draining && attached_streams.is_empty() && datagram_flows.is_empty() {
                        if !server_write_tcp_path_frame(
                            &mut writer,
                            &Frame::PathClose {
                                path_id,
                                reason: CloseReason::Normal,
                            },
                        )
                        .await?
                        {
                            return Ok(());
                        }
                        return Ok(());
                    }
                }
                Frame::StreamData {
                    stream_id,
                    offset,
                    flags,
                    payload,
                } => {
                    context
                        .tcp_streams
                        .route_frame(
                            session_id,
                            stream_id,
                            Frame::StreamData {
                                stream_id,
                                offset,
                                flags,
                                payload,
                            },
                        )
                        .await?;
                }
                Frame::StreamAck {
                    stream_id,
                    complete,
                    ranges,
                } => {
                    context
                        .tcp_streams
                        .route_frame(
                            session_id,
                            stream_id,
                            Frame::StreamAck {
                                stream_id,
                                complete,
                                ranges,
                            },
                        )
                        .await?;
                }
                Frame::StreamMaxData {
                    stream_id,
                    max_offset,
                } => {
                    context
                        .tcp_streams
                        .route_frame(
                            session_id,
                            stream_id,
                            Frame::StreamMaxData {
                                stream_id,
                                max_offset,
                            },
                        )
                        .await?;
                }
                Frame::StreamFin {
                    stream_id,
                    final_offset,
                } => {
                    context
                        .tcp_streams
                        .route_frame(
                            session_id,
                            stream_id,
                            Frame::StreamFin {
                                stream_id,
                                final_offset,
                            },
                        )
                        .await?;
                }
                Frame::StreamDetach { stream_id } => {
                    attached_streams.remove(&stream_id);
                    context.tcp_streams.detach_path(
                        session_id,
                        stream_id,
                        UnderlayProtocol::Tcp,
                        path_id,
                        &commands_tx,
                    );
                    if draining && attached_streams.is_empty() && datagram_flows.is_empty() {
                        if !server_write_tcp_path_frame(
                            &mut writer,
                            &Frame::PathClose {
                                path_id,
                                reason: CloseReason::Normal,
                            },
                        )
                        .await?
                        {
                            return Ok(());
                        }
                        return Ok(());
                    }
                }
                Frame::StreamReset { stream_id, reason } => {
                    context
                        .tcp_streams
                        .route_frame(
                            session_id,
                            stream_id,
                            Frame::StreamReset { stream_id, reason },
                        )
                        .await?;
                }
                Frame::Ping { nonce } => {
                    if !server_write_tcp_path_frame(&mut writer, &Frame::Pong { nonce }).await? {
                        return Ok(());
                    }
                }
                Frame::PathMetrics { metrics } if metrics.path_id == path_id => {
                    context.tcp_streams.record_path_metrics(
                        session_id,
                        UnderlayProtocol::Tcp,
                        path_id,
                        metrics,
                    );
                }
                Frame::PathDrain {
                    path_id: drain_path_id,
                } if drain_path_id == path_id => {
                    draining = true;
                    if !server_write_tcp_path_frame(
                        &mut writer,
                        &Frame::PathStatus {
                            path_id,
                            status: crate::protocol::PathStatus::Draining,
                            capabilities: path_capabilities,
                        },
                    )
                    .await?
                    {
                        return Ok(());
                    }
                    if attached_streams.is_empty() && datagram_flows.is_empty() {
                        return Ok(());
                    }
                }
                Frame::PathClose {
                    path_id: close_path_id,
                    ..
                } if close_path_id == path_id => return Ok(()),
                Frame::SessionClose { .. } => return Ok(()),
                _ => return Err(RuntimeError::Protocol("unexpected TCP path session frame")),
            },
        }
    }
}

pub(super) struct ServerTcpPathCommandContext<'a> {
    session_id: SessionId,
    path_id: PathId,
    commands_tx: &'a TcpPathSessionCommandSender,
    draining: bool,
    active_datagram_flows: usize,
}

pub(super) async fn handle_server_tcp_path_command(
    command: TcpPathSessionCommand,
    writer: &mut EncryptedTcpWriter,
    context: &ServerPathContext,
    attached_streams: &mut HashSet<StreamId>,
    command_context: ServerTcpPathCommandContext<'_>,
) -> Result<bool, RuntimeError> {
    match command {
        TcpPathSessionCommand::SendFrame(frame) => {
            server_write_tcp_path_frame(writer, &frame).await
        }
        TcpPathSessionCommand::CloseStream(stream_id) => {
            attached_streams.remove(&stream_id);
            context.tcp_streams.detach_path(
                command_context.session_id,
                stream_id,
                UnderlayProtocol::Tcp,
                command_context.path_id,
                command_context.commands_tx,
            );
            if command_context.draining
                && attached_streams.is_empty()
                && command_context.active_datagram_flows == 0
            {
                let _ = server_write_tcp_path_frame(
                    writer,
                    &Frame::PathClose {
                        path_id: command_context.path_id,
                        reason: CloseReason::Normal,
                    },
                )
                .await?;
                return Ok(false);
            }
            Ok(true)
        }
        TcpPathSessionCommand::OpenStream { .. } => Err(RuntimeError::Protocol(
            "server TCP path received client open command",
        )),
    }
}

pub(super) async fn server_write_tcp_path_frame(
    framed: &mut EncryptedTcpWriter,
    frame: &Frame,
) -> Result<bool, RuntimeError> {
    match framed.write_frame(frame).await {
        Ok(()) => {}
        Err(err) if encrypted_framed_peer_closed(&err) => return Ok(false),
        Err(err) => return Err(RuntimeError::Encrypted(err)),
    }
    match framed.flush().await {
        Ok(()) => Ok(true),
        Err(err) if encrypted_framed_peer_closed(&err) => Ok(false),
        Err(err) => Err(RuntimeError::Encrypted(err)),
    }
}

pub(super) fn encrypted_framed_peer_closed(err: &EncryptedFramedTransportError) -> bool {
    matches!(
        err,
        EncryptedFramedTransportError::Io(io)
            if matches!(
                io.kind(),
                std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::UnexpectedEof
            )
    )
}

pub(super) async fn run_server_tcp_stream(
    context: ServerPathContext,
    session_id: SessionId,
    stream: TcpPathStream,
    target: TargetAddr,
) -> Result<(), RuntimeError> {
    let stream_id = stream.stream_id;
    let result = async {
        let outbound_stream = match outbound::connect_tcp(
            &context.outbound,
            &context.outbound_dns,
            &target,
            Duration::from_secs(10),
        )
        .await
        {
            Ok(stream) => stream,
            Err(err) => {
                stream
                    .send_frame(Frame::StreamReset {
                        stream_id,
                        reason: ResetReason::Refused,
                    })
                    .await?;
                stream.close().await;
                return Err(RuntimeError::OutboundConnect(err));
            }
        };
        stream
            .send_frame(Frame::StreamMaxData {
                stream_id,
                max_offset: context.mux_limits.max_stream_window_bytes,
            })
            .await?;
        relay_tcp_stream(outbound_stream, stream, context.mux_limits, session_id)
            .await
            .map(|_| ())
    }
    .await;
    context.tcp_streams.close(session_id, stream_id);
    result
}

pub(super) fn tcp_server_session_command_queue(context: &ServerPathContext) -> usize {
    tcp_path_command_queue(context.mux_limits)
}
