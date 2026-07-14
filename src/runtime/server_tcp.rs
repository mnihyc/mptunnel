use super::*;
use crate::protocol::path_capacity::CapacityReceiveTracker;

pub(super) async fn handle_server_path(
    stream: TcpStream,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    #[cfg(target_os = "linux")]
    let mut tcp_metrics = TcpMetricPublisher::capture(&stream);
    let mut framed = EncryptedFramedStream::with_cipher_suite(
        stream,
        context.security.secret.as_bytes(),
        PeerRole::Server,
        context.codec_limits,
        context.security.cipher,
    )?;
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
    let path_registration =
        context
            .reliable_streams
            .register_carrier_path(session_id, UnderlayProtocol::Tcp, path_id);
    framed
        .write_frames(&[
            Frame::SessionReady,
            Frame::PathStatus {
                path_id,
                status: crate::protocol::PathStatus::Active,
                capabilities: path_capabilities,
            },
        ])
        .await?;
    if let Err(err) = framed.flush().await {
        if encrypted_framed_peer_closed(&err) {
            return Ok(());
        }
        return Err(RuntimeError::Encrypted(err));
    }
    #[cfg(target_os = "linux")]
    if let Some(metrics) = tcp_metrics.as_mut() {
        metrics.begin_epoch();
    }

    let (reader, mut writer) = framed.split()?;
    let mut path_frames =
        spawn_encrypted_tcp_reader(reader, reliable_path_writer_frame_queue(context.mux_limits));
    let (commands_tx, mut commands_rx) =
        reliable_path_command_channels(tcp_server_session_command_queue(&context));
    let mut attached_streams = HashSet::new();
    let mut datagram_flows = Vec::<ServerUdpDatagramFlow>::new();
    let mut draining = false;
    let mut pending_frames = Vec::<Frame>::new();
    let mut path_proofs = PathProofTracker::default();
    let mut request_capacity_receive = CapacityReceiveTracker::new(
        reliable_quic_capacity_calibration_session_limit_bytes(context.mux_limits),
    );
    let mut tcp_capacity_probe = None::<PendingTcpCapacityProbe>;

    loop {
        let event = if let Some(pending) = tcp_capacity_probe.as_ref() {
            match tokio::time::timeout_at(
                tokio::time::Instant::from_std(pending.probe.expires_at),
                recv_server_tcp_path_event(&mut path_frames, &mut commands_rx),
            )
            .await
            {
                Ok(event) => event?,
                Err(_) => {
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "response_tcp_capacity_probe",
                        format_args!(
                            "phase=rejected reason=receipt_timeout session_id={} path_id={} path_instance_id={} calibration_id={}",
                            session_id.0,
                            path_id.0,
                            path_registration.path_instance_id().as_u64(),
                            pending.probe.calibration_id,
                        ),
                    );
                    // A late receipt cannot be attributed after the lease is
                    // released, so fail-close this exact carrier.
                    return Ok(());
                }
            }
        } else {
            recv_server_tcp_path_event(&mut path_frames, &mut commands_rx).await?
        };
        let Some(event) = event else {
            return Ok(());
        };
        #[cfg(target_os = "linux")]
        if let Some(metrics) = tcp_metrics.as_mut().and_then(|publisher| {
            publisher.maybe_observe(path_id, PathMetricDirection::ServerToClient, false)
        }) {
            context
                .reliable_streams
                .record_local_path_metrics(&path_registration, metrics);
        }
        match event {
            ServerTcpPathEvent::Command(command) => {
                let keep_running = drain_server_tcp_path_commands(
                    command,
                    &mut commands_rx,
                    &mut writer,
                    &context,
                    &mut attached_streams,
                    &mut datagram_flows,
                    ServerTcpPathCommandContext {
                        session_id,
                        path_id,
                        path_instance_id: path_registration.path_instance_id(),
                        commands_tx: &commands_tx,
                        draining,
                        active_datagram_flows: 0,
                    },
                    &mut pending_frames,
                    &mut path_proofs,
                    &mut tcp_capacity_probe,
                )
                .await?;
                if !keep_running {
                    return Ok(());
                }
            }
            ServerTcpPathEvent::Frame(frame) => {
                match frame {
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
                        match context.reliable_streams.open_or_attach(
                            ServerReliableStreamOpenRequest {
                                session_id,
                                stream_id,
                                target: &target,
                                lane,
                                attachment: ServerReliablePathAttachment {
                                    path_registration: path_registration.clone(),
                                    commands: commands_tx.clone(),
                                    max_frame_payload_bytes: reliable_relay_buffer_len(
                                        context.mux_limits,
                                    ),
                                    role,
                                    initial_metrics: context
                                        .local_path_startup_metrics(UnderlayProtocol::Tcp, path_id),
                                },
                            },
                            context.mux_limits,
                            context.max_reliable_streams,
                        )? {
                            ServerReliableStreamOpen::New(stream) => {
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
                                        eprintln!("warning: server reliable stream failed: {err}");
                                    }
                                });
                            }
                            ServerReliableStreamOpen::Existing => {
                                if role != StreamOpenRole::Validation {
                                    attached_streams.insert(stream_id);
                                }
                                context
                                    .reliable_streams
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
                                        max_offset: reliable_stream_initial_advertised_window_bytes(
                                            UnderlayProtocol::Tcp,
                                            lane,
                                            context.mux_limits,
                                        ),
                                    },
                                )
                                .await?
                                {
                                    return Ok(());
                                }
                            }
                            ServerReliableStreamOpen::DuplicateLiveIgnored => {}
                            ServerReliableStreamOpen::Rejected => {
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
                        let realtime_registration =
                            context.reliable_streams.register_realtime_flow(session_id);
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
                        datagram_flows.push(ServerUdpDatagramFlow {
                            flow_id,
                            requests,
                            _realtime_registration: realtime_registration,
                        });
                    }
                    Frame::OpenDatagramFlow { flow_id, .. } => {
                        if !server_write_tcp_path_frame(
                            &mut writer,
                            &Frame::DatagramClose { flow_id },
                        )
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
                                eprintln!(
                                    "warning: TCP datagram worker queue full; dropping request"
                                );
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
                            .reliable_streams
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
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "server_tcp_stream_ack_ingress",
                            format_args!(
                                "stream_id={} path_id={} complete={} ranges={} frontier={} largest_end={}",
                                stream_id.0,
                                path_id.0,
                                complete,
                                ranges.len(),
                                stream_ack_contiguous_frontier(complete, &ranges),
                                ranges.last().map_or(0, |range| range.end),
                            ),
                        );
                        context
                            .reliable_streams
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
                            .reliable_streams
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
                            .reliable_streams
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
                        context.reliable_streams.detach_path(
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
                            .reliable_streams
                            .route_frame(
                                session_id,
                                stream_id,
                                Frame::StreamReset { stream_id, reason },
                            )
                            .await?;
                    }
                    Frame::Ping { nonce } => {
                        if !server_write_tcp_path_frame(&mut writer, &Frame::Pong { nonce }).await?
                        {
                            return Ok(());
                        }
                    }
                    Frame::PathProofData {
                        path_id: proof_path_id,
                        proof_id,
                        payload,
                    } if proof_path_id == path_id => {
                        if !server_write_tcp_path_frame(
                            &mut writer,
                            &path_proof_ack_frame(path_id, proof_id, payload.len()),
                        )
                        .await?
                        {
                            return Ok(());
                        }
                    }
                    Frame::PathProofAck {
                        path_id: proof_path_id,
                        proof_id,
                        payload_bytes,
                    } if proof_path_id == path_id => {
                        if let Some(observation) =
                            path_proofs.acknowledge(path_id, proof_id, payload_bytes)
                            && let Some(metrics) = path_proof_metrics(
                                path_id,
                                UnderlayProtocol::Tcp,
                                PathMetricDirection::ServerToClient,
                                observation,
                            )
                        {
                            context
                                .reliable_streams
                                .record_local_path_metrics(&path_registration, metrics);
                        }
                    }
                    Frame::PathCapacityReceipt {
                        path_id: receipt_path_id,
                        calibration_id,
                        received_payload_bytes,
                    } if receipt_path_id == path_id => {
                        let Some(pending) = tcp_capacity_probe.take() else {
                            return Err(RuntimeError::Protocol(
                                "TCP capacity receipt has no active epoch",
                            ));
                        };
                        let TcpCapacityProbeOwner::Response { path_instance_id } =
                            pending.probe.owner
                        else {
                            return Err(RuntimeError::Protocol(
                                "server TCP path received a request-owned capacity receipt",
                            ));
                        };
                        if path_instance_id != path_registration.path_instance_id()
                            || pending.probe.calibration_id != calibration_id
                            || pending.probe.train_payload_bytes != received_payload_bytes
                        {
                            return Err(RuntimeError::Protocol(
                                "TCP capacity receipt does not match active epoch",
                            ));
                        }
                        if Instant::now() >= pending.probe.expires_at {
                            #[cfg(feature = "lab-diagnostics")]
                            lab_diagnostic(
                                "response_tcp_capacity_probe",
                                format_args!(
                                    "phase=rejected reason=expired_before_receipt session_id={} path_id={} path_instance_id={} calibration_id={}",
                                    session_id.0,
                                    path_id.0,
                                    path_registration.path_instance_id().as_u64(),
                                    calibration_id,
                                ),
                            );
                            continue;
                        }
                        #[cfg(target_os = "linux")]
                        {
                            let elapsed = pending.started_at.elapsed();
                            let receipt_rate_bps =
                                tcp_capacity_receipt_rate_bps(received_payload_bytes, elapsed)
                                    .ok_or(RuntimeError::Protocol(
                                        "TCP capacity receipt has invalid timing",
                                    ))?;
                            let Some(mut metrics) = tcp_metrics.as_mut().and_then(|publisher| {
                                publisher.maybe_observe(
                                    path_id,
                                    PathMetricDirection::ServerToClient,
                                    true,
                                )
                            }) else {
                                #[cfg(feature = "lab-diagnostics")]
                                lab_diagnostic(
                                    "response_tcp_capacity_probe",
                                    format_args!(
                                        "phase=rejected reason=native_metrics_unavailable session_id={} path_id={} path_instance_id={} calibration_id={}",
                                        session_id.0,
                                        path_id.0,
                                        path_registration.path_instance_id().as_u64(),
                                        calibration_id,
                                    ),
                                );
                                continue;
                            };
                            let kernel_delivery_rate_bps = metrics.delivery_rate_bps;
                            let kernel_pacing_rate_bps = metrics.pacing_rate_bps;
                            let rate_bps = tcp_capacity_authoritative_rate_bps(
                                receipt_rate_bps,
                                kernel_delivery_rate_bps,
                                kernel_pacing_rate_bps,
                            );
                            metrics.delivery_rate_bps = rate_bps;
                            metrics.pacing_rate_bps = rate_bps;
                            metrics.has_ack_derived_data_sample = true;
                            metrics.data_sample_count = metrics.data_sample_count.max(1);
                            metrics.data_sample_bytes =
                                metrics.data_sample_bytes.max(received_payload_bytes);
                            metrics.confidence_ppm = 1_000_000;
                            let accepted_at = Instant::now();
                            let validity = tcp_capacity_proof_validity(metrics);
                            let candidate = TcpCapacityProofCandidate {
                                token: calibration_id,
                                train_bytes: pending.probe.train_payload_bytes,
                                received_bytes: received_payload_bytes,
                                rate_sample_bytes: received_payload_bytes,
                                proof_elapsed: elapsed,
                                receipt_rate_bps,
                                rate_bps,
                                accepted_at,
                                expires_at: accepted_at
                                    .checked_add(validity)
                                    .unwrap_or(accepted_at),
                            };
                            // Release carrier and session discovery ownership
                            // before proof publication wakes the sender.
                            drop(pending);
                            if !context.reliable_streams.record_local_tcp_capacity_proof(
                                &path_registration,
                                metrics,
                                candidate,
                            ) {
                                return Err(RuntimeError::Protocol(
                                    "TCP capacity proof publication was rejected",
                                ));
                            }
                            #[cfg(feature = "lab-diagnostics")]
                            lab_diagnostic(
                                "response_tcp_capacity_probe",
                                format_args!(
                                    "phase=confirmed session_id={} path_id={} path_instance_id={} calibration_id={} train_bytes={} elapsed_ms={} receipt_rate_mbps={:.3} published_rate_mbps={:.3} kernel_delivery_rate_mbps={:.3} kernel_pacing_rate_mbps={:.3} srtt_ms={:.3} inflight_limit_bytes={} app_limited={}",
                                    session_id.0,
                                    path_id.0,
                                    path_registration.path_instance_id().as_u64(),
                                    calibration_id,
                                    received_payload_bytes,
                                    elapsed.as_millis(),
                                    receipt_rate_bps as f64 / 1_000_000.0,
                                    rate_bps as f64 / 1_000_000.0,
                                    kernel_delivery_rate_bps as f64 / 1_000_000.0,
                                    kernel_pacing_rate_bps as f64 / 1_000_000.0,
                                    metrics.srtt_us as f64 / 1_000.0,
                                    metrics.inflight_limit_bytes,
                                    metrics.app_limited,
                                ),
                            );
                        }
                        #[cfg(not(target_os = "linux"))]
                        return Err(RuntimeError::Protocol(
                            "TCP capacity receipt is unsupported on this platform",
                        ));
                    }
                    Frame::PathCapacityData {
                        path_id: capacity_path_id,
                        calibration_id,
                        payload,
                    } if capacity_path_id == path_id => {
                        request_capacity_receive.record_data(calibration_id, payload.len())?;
                    }
                    Frame::PathCapacityFinish {
                        path_id: capacity_path_id,
                        calibration_id,
                        payload_bytes,
                    } if capacity_path_id == path_id => {
                        let received_payload_bytes =
                            request_capacity_receive.finish(calibration_id, payload_bytes)?;
                        if !server_write_tcp_path_frame(
                            &mut writer,
                            &Frame::PathCapacityReceipt {
                                path_id,
                                calibration_id,
                                received_payload_bytes,
                            },
                        )
                        .await?
                        {
                            return Ok(());
                        }
                    }
                    Frame::PathMetrics { metrics } if metrics.path_id == path_id => {
                        context
                            .reliable_streams
                            .record_path_metrics(&path_registration, metrics);
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
                }
                if let Some(command) = try_recv_reliable_path_command(&mut commands_rx) {
                    let keep_running = drain_server_tcp_path_commands(
                        command,
                        &mut commands_rx,
                        &mut writer,
                        &context,
                        &mut attached_streams,
                        &mut datagram_flows,
                        ServerTcpPathCommandContext {
                            session_id,
                            path_id,
                            path_instance_id: path_registration.path_instance_id(),
                            commands_tx: &commands_tx,
                            draining,
                            active_datagram_flows: 0,
                        },
                        &mut pending_frames,
                        &mut path_proofs,
                        &mut tcp_capacity_probe,
                    )
                    .await?;
                    if !keep_running {
                        return Ok(());
                    }
                }
            }
        }
    }
}

pub(super) struct ServerTcpPathCommandContext<'a> {
    session_id: SessionId,
    path_id: PathId,
    path_instance_id: ServerCarrierPathInstanceId,
    commands_tx: &'a ReliablePathCommandSender,
    draining: bool,
    active_datagram_flows: usize,
}

struct PendingTcpCapacityProbe {
    probe: TcpCapacityProbeCommand,
    started_at: Instant,
}

async fn drain_server_tcp_path_commands(
    first_command: ReliablePathCommand,
    commands_rx: &mut ReliablePathCommandReceivers,
    writer: &mut EncryptedTcpWriter,
    context: &ServerPathContext,
    attached_streams: &mut HashSet<StreamId>,
    datagram_flows: &mut Vec<ServerUdpDatagramFlow>,
    command_context: ServerTcpPathCommandContext<'_>,
    pending_frames: &mut Vec<Frame>,
    path_proofs: &mut PathProofTracker,
    tcp_capacity_probe: &mut Option<PendingTcpCapacityProbe>,
) -> Result<bool, RuntimeError> {
    #[cfg(feature = "lab-diagnostics")]
    let drain_started = Instant::now();
    let byte_budget = reliable_noninterlocked_tcp_writer_run_budget_bytes(context.mux_limits);
    let item_budget = reliable_path_command_writer_run_budget_items(context.mux_limits);
    let mut next_command = Some(first_command);
    pending_frames.clear();
    let mut sent_bytes = 0usize;
    let mut sent_items = 0usize;
    let mut wrote_frame = false;

    loop {
        let Some(command) = next_command
            .take()
            .or_else(|| try_recv_reliable_path_command(commands_rx))
        else {
            if try_coalesce_reliable_path_writer_run(
                commands_rx,
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
        if let ReliablePathCommand::SendFrame(Frame::DatagramClose { flow_id }) = &command {
            datagram_flows.retain(|flow| flow.flow_id != *flow_id);
        }
        match command {
            ReliablePathCommand::SendFrame(frame)
                if reliable_path_frame_requires_capacity_command(&frame) =>
            {
                commands_rx.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "server TCP path received an untyped capacity frame",
                ));
            }
            ReliablePathCommand::SendFrame(frame) => {
                let is_stream_detach = matches!(&frame, Frame::StreamDetach { .. });
                pending_frames.push(frame);
                commands_rx.release_pending_command_bytes(pending_bytes);
                wrote_frame = true;
                sent_bytes = sent_bytes.saturating_add(writer_run_bytes);
                sent_items = sent_items.saturating_add(1);
                if is_stream_detach || sent_bytes >= byte_budget || sent_items >= item_budget {
                    break;
                }
                continue;
            }
            ReliablePathCommand::SendQuicCapacityProbe(probe) => {
                probe.ticket.cancel();
                commands_rx.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "server TCP path received QUIC capacity command",
                ));
            }
            ReliablePathCommand::SendTcpCapacityProbe(probe) => {
                let TcpCapacityProbeOwner::Response { path_instance_id } = probe.owner else {
                    commands_rx.release_pending_command_bytes(pending_bytes);
                    return Err(RuntimeError::Protocol(
                        "server TCP path received request capacity command",
                    ));
                };
                if probe.path_id != command_context.path_id
                    || path_instance_id != command_context.path_instance_id
                    || probe.train_payload_bytes < probe.sample_floor_bytes
                    || tcp_capacity_probe.is_some()
                {
                    commands_rx.release_pending_command_bytes(pending_bytes);
                    return Err(RuntimeError::Protocol(
                        "server TCP capacity command does not match idle writer",
                    ));
                }
                if !server_write_tcp_path_frame_batch(writer, pending_frames, path_proofs).await? {
                    return Ok(false);
                }
                if Instant::now() >= probe.expires_at {
                    commands_rx.release_pending_command_bytes(pending_bytes);
                    return Ok(true);
                }
                let started_at = Instant::now();
                let wrote = match tokio::time::timeout_at(
                    tokio::time::Instant::from_std(probe.expires_at),
                    server_write_tcp_capacity_probe(
                        writer,
                        &probe,
                        context.mux_limits.max_payload_bytes,
                    ),
                )
                .await
                {
                    Ok(result) => result?,
                    Err(_) => {
                        commands_rx.release_pending_command_bytes(pending_bytes);
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "response_tcp_capacity_probe",
                            format_args!(
                                "phase=rejected reason=send_timeout session_id={} path_id={} path_instance_id={} calibration_id={}",
                                command_context.session_id.0,
                                command_context.path_id.0,
                                path_instance_id.as_u64(),
                                probe.calibration_id,
                            ),
                        );
                        // Receipt cannot identify a partial train, so close the
                        // exact carrier instead of releasing it for later work.
                        return Ok(false);
                    }
                };
                commands_rx.release_pending_command_bytes(pending_bytes);
                if !wrote {
                    return Ok(false);
                }
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "response_tcp_capacity_probe",
                    format_args!(
                        "phase=sent session_id={} path_id={} path_instance_id={} calibration_id={} train_bytes={} sample_floor_bytes={}",
                        command_context.session_id.0,
                        command_context.path_id.0,
                        path_instance_id.as_u64(),
                        probe.calibration_id,
                        probe.train_payload_bytes,
                        probe.sample_floor_bytes,
                    ),
                );
                *tcp_capacity_probe = Some(PendingTcpCapacityProbe { probe, started_at });
                return Ok(true);
            }
            command => {
                if !server_write_tcp_path_frame_batch(writer, pending_frames, path_proofs).await? {
                    return Ok(false);
                }
                let keep_running = handle_server_tcp_path_command(
                    command,
                    writer,
                    context,
                    attached_streams,
                    ServerTcpPathCommandContext {
                        active_datagram_flows: datagram_flows.len(),
                        ..command_context
                    },
                    false,
                )
                .await?;
                commands_rx.release_pending_command_bytes(pending_bytes);
                if !keep_running {
                    return Ok(false);
                }
                sent_items = sent_items.saturating_add(1);
            }
        }
        if sent_bytes >= byte_budget || sent_items >= item_budget {
            break;
        }
    }

    if !server_write_tcp_path_frame_batch(writer, pending_frames, path_proofs).await? {
        return Ok(false);
    }

    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "path_writer_drain",
        format_args!(
            "role=server underlay=Tcp path_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
            command_context.path_id.0,
            sent_items,
            sent_bytes,
            byte_budget,
            item_budget,
            commands_rx.pending_bytes(),
            drain_started.elapsed().as_micros(),
            sent_bytes >= byte_budget,
            sent_items >= item_budget,
        ),
    );
    if wrote_frame && !server_flush_tcp_path_writer(writer).await? {
        return Ok(false);
    }
    Ok(true)
}

async fn server_write_tcp_path_frame_batch(
    framed: &mut EncryptedTcpWriter,
    frames: &mut Vec<Frame>,
    path_proofs: &mut PathProofTracker,
) -> Result<bool, RuntimeError> {
    if frames.is_empty() {
        return Ok(true);
    }
    match framed.write_frames(frames).await {
        Ok(()) => {
            for frame in frames.iter() {
                path_proofs.record_sent_frame(frame);
            }
            frames.clear();
            Ok(true)
        }
        Err(err) if encrypted_framed_peer_closed(&err) => {
            frames.clear();
            Ok(false)
        }
        Err(err) => {
            frames.clear();
            Err(RuntimeError::Encrypted(err))
        }
    }
}

async fn server_write_tcp_capacity_probe(
    writer: &mut EncryptedTcpWriter,
    probe: &TcpCapacityProbeCommand,
    max_payload_bytes: usize,
) -> Result<bool, RuntimeError> {
    let frame_payload_bytes = max_payload_bytes.max(1) as u64;
    let mut remaining = probe.train_payload_bytes;
    while remaining > 0 {
        let payload_bytes = remaining.min(frame_payload_bytes) as usize;
        if !server_write_tcp_path_frame_maybe_flush(
            writer,
            &Frame::PathCapacityData {
                path_id: probe.path_id,
                calibration_id: probe.calibration_id,
                payload: Bytes::from(vec![0u8; payload_bytes]),
            },
            false,
        )
        .await?
        {
            return Ok(false);
        }
        remaining = remaining.saturating_sub(payload_bytes as u64);
    }
    server_write_tcp_path_frame(
        writer,
        &Frame::PathCapacityFinish {
            path_id: probe.path_id,
            calibration_id: probe.calibration_id,
            payload_bytes: probe.train_payload_bytes,
        },
    )
    .await
}

pub(super) async fn handle_server_tcp_path_command(
    command: ReliablePathCommand,
    writer: &mut EncryptedTcpWriter,
    context: &ServerPathContext,
    attached_streams: &mut HashSet<StreamId>,
    command_context: ServerTcpPathCommandContext<'_>,
    flush_after_frame: bool,
) -> Result<bool, RuntimeError> {
    match command {
        ReliablePathCommand::SendFrame(frame)
            if reliable_path_frame_requires_capacity_command(&frame) =>
        {
            Err(RuntimeError::Protocol(
                "server TCP path received an untyped capacity frame",
            ))
        }
        ReliablePathCommand::SendFrame(frame) => {
            server_write_tcp_path_frame_maybe_flush(writer, &frame, flush_after_frame).await
        }
        ReliablePathCommand::CloseStream(stream_id) => {
            attached_streams.remove(&stream_id);
            context.reliable_streams.detach_path(
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
        ReliablePathCommand::OpenStream { .. } => Err(RuntimeError::Protocol(
            "server TCP path received client open command",
        )),
        ReliablePathCommand::CancelTcpOpen { .. } => Err(RuntimeError::Protocol(
            "server TCP path received client open cancellation",
        )),
        ReliablePathCommand::SendQuicCapacityProbe(probe) => {
            probe.ticket.cancel();
            Err(RuntimeError::Protocol(
                "server TCP path received QUIC capacity command",
            ))
        }
        ReliablePathCommand::SendTcpCapacityProbe(_) => Err(RuntimeError::Protocol(
            "server TCP capacity command bypassed typed writer transaction",
        )),
    }
}

pub(super) async fn server_write_tcp_path_frame(
    framed: &mut EncryptedTcpWriter,
    frame: &Frame,
) -> Result<bool, RuntimeError> {
    server_write_tcp_path_frame_maybe_flush(framed, frame, true).await
}

async fn server_write_tcp_path_frame_maybe_flush(
    framed: &mut EncryptedTcpWriter,
    frame: &Frame,
    flush_after_frame: bool,
) -> Result<bool, RuntimeError> {
    match framed.write_frame(frame).await {
        Ok(()) => {}
        Err(err) if encrypted_framed_peer_closed(&err) => return Ok(false),
        Err(err) => return Err(RuntimeError::Encrypted(err)),
    }
    if !flush_after_frame {
        return Ok(true);
    }
    server_flush_tcp_path_writer(framed).await
}

async fn server_flush_tcp_path_writer(
    framed: &mut EncryptedTcpWriter,
) -> Result<bool, RuntimeError> {
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
    stream: ReliablePathStream,
    target: TargetAddr,
) -> Result<(), RuntimeError> {
    let stream_id = stream.stream_id;
    let result = async {
        let outbound_stream = match outbound::connect_tcp(
            &context.outbound,
            &context.outbound_dns,
            &target,
            context.outbound_connect_timeout,
        )
        .await
        {
            Ok(stream) => stream,
            Err(err) => {
                send_sender_service_control_frame(
                    &stream,
                    Frame::StreamReset {
                        stream_id,
                        reason: ResetReason::Refused,
                    },
                )
                .await?;
                stream.close().await;
                return Err(RuntimeError::OutboundConnect(err));
            }
        };
        send_sender_service_control_frame(
            &stream,
            Frame::StreamMaxData {
                stream_id,
                max_offset: reliable_stream_initial_advertised_window_bytes(
                    stream.underlay,
                    stream.lane,
                    context.mux_limits,
                ),
            },
        )
        .await?;
        relay_reliable_stream(
            outbound_stream,
            stream,
            context.mux_limits,
            context.performance,
            session_id,
        )
        .await
        .map(|_| ())
    }
    .await;
    context.reliable_streams.close(session_id, stream_id);
    result
}

pub(super) fn tcp_server_session_command_queue(context: &ServerPathContext) -> usize {
    reliable_path_command_queue(context.mux_limits)
}
