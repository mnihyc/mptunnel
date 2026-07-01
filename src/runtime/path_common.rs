use super::*;

pub(super) fn stream_demand_hint_for_lane(lane: FlowLane) -> StreamDemandHint {
    match lane {
        FlowLane::Control | FlowLane::Latency => StreamDemandHint::latency(),
        FlowLane::Throughput | FlowLane::Background => StreamDemandHint::throughput(),
        FlowLane::RealtimeDatagram => StreamDemandHint::realtime(),
    }
}

pub(super) fn flow_lane_from_stream_demand_hint(demand: StreamDemandHint) -> FlowLane {
    let latency = demand.latency_weight_ppm;
    let throughput = demand.throughput_weight_ppm;
    let realtime = demand.realtime_weight_ppm;
    if realtime > 0 && realtime >= latency && realtime >= throughput {
        FlowLane::RealtimeDatagram
    } else if throughput > 0 && throughput >= latency {
        FlowLane::Throughput
    } else {
        FlowLane::Latency
    }
}

pub(super) struct ServerUdpDatagramFlow {
    pub(super) flow_id: DatagramFlowId,
    pub(super) requests: mpsc::Sender<ServerUdpDatagramRequest>,
}

pub(super) struct ServerUdpDatagramRequest {
    pub(super) datagram_id: DatagramId,
    pub(super) ttl_ms: u32,
    pub(super) payload: Bytes,
}

fn server_udp_datagram_request_queue_len(mux_limits: MuxLimits) -> usize {
    let unit = mux_limits.max_payload_bytes.max(1);
    mux_limits
        .max_datagram_queue_bytes
        .saturating_div(unit)
        .clamp(1, 1024)
}

pub(super) fn spawn_server_udp_datagram_flow_worker(
    flow_id: DatagramFlowId,
    mut outbound_socket: outbound::OutboundUdpSocket,
    commands: TcpPathSessionCommandSender,
    mux_limits: MuxLimits,
) -> mpsc::Sender<ServerUdpDatagramRequest> {
    let (requests_tx, mut requests_rx) = mpsc::channel::<ServerUdpDatagramRequest>(
        server_udp_datagram_request_queue_len(mux_limits),
    );
    tokio::spawn(async move {
        let mut response_buffer = vec![0u8; mux_limits.max_payload_bytes.min(64 * 1024)];
        let mut pending_ttls = VecDeque::<(Instant, u32, DatagramId)>::new();
        loop {
            prune_server_udp_pending_ttls(&mut pending_ttls);
            tokio::select! {
                biased;
                received = outbound_socket.recv(&mut response_buffer) => {
                    let len = match received {
                        Ok(len) => len,
                        Err(err) => {
                            eprintln!("warning: UDP outbound receive failed: {err}");
                            let _ = try_send_server_datagram_realtime_frame(
                                &commands,
                                Frame::DatagramClose { flow_id },
                            );
                            break;
                        }
                    };
                    let Some((ttl_ms, datagram_id)) =
                        server_udp_next_response_ttl(&mut pending_ttls)
                    else {
                        continue;
                    };
                    let frame = Frame::DatagramData {
                        flow_id,
                        datagram_id,
                        ttl_ms,
                        payload: Bytes::copy_from_slice(&response_buffer[..len]),
                    };
                    match try_send_server_datagram_realtime_frame(&commands, frame) {
                        Ok(()) => {}
                        Err(RuntimeError::SenderServiceBlocked) => {
                            #[cfg(feature = "lab-diagnostics")]
                            lab_diagnostic(
                                "server_datagram_response_dropped",
                                format_args!(
                                    "flow_id={} datagram_id={} payload_bytes={} reason=carrier_credit",
                                    flow_id.0,
                                    datagram_id.0,
                                    len,
                                ),
                            );
                            continue;
                        }
                        Err(_) => break,
                    }
                }
                request = requests_rx.recv() => {
                    let Some(request) = request else {
                        break;
                    };
                    if request.ttl_ms == 0 {
                        continue;
                    }
                    match outbound_socket.send(&request.payload).await {
                        Ok(_) => {
                            pending_ttls.push_back((
                                Instant::now() + Duration::from_millis(u64::from(request.ttl_ms)),
                                request.ttl_ms,
                                request.datagram_id,
                            ));
                        }
                        Err(err) => {
                            eprintln!("warning: UDP outbound send failed: {err}");
                        }
                    }
                }
            }
        }
    });
    requests_tx
}

pub(super) fn try_send_server_datagram_realtime_frame(
    commands: &TcpPathSessionCommandSender,
    frame: Frame,
) -> Result<(), RuntimeError> {
    debug_assert!(matches!(
        frame,
        Frame::DatagramData { .. } | Frame::DatagramFeedback { .. } | Frame::DatagramClose { .. }
    ));
    commands.try_enqueue_admitted_frame(frame, FlowLane::RealtimeDatagram)
}

fn prune_server_udp_pending_ttls(pending_ttls: &mut VecDeque<(Instant, u32, DatagramId)>) {
    let now = Instant::now();
    while pending_ttls
        .front()
        .is_some_and(|(deadline, _, _)| *deadline <= now)
    {
        pending_ttls.pop_front();
    }
}

fn server_udp_next_response_ttl(
    pending_ttls: &mut VecDeque<(Instant, u32, DatagramId)>,
) -> Option<(u32, DatagramId)> {
    prune_server_udp_pending_ttls(pending_ttls);
    pending_ttls
        .pop_front()
        .map(|(_, ttl_ms, datagram_id)| (ttl_ms, datagram_id))
}

pub(super) fn frame_kind_name(frame: &Frame) -> &'static str {
    match frame {
        Frame::SessionHello { .. } => "SESSION_HELLO",
        Frame::SessionAuth { .. } => "SESSION_AUTH",
        Frame::SessionReady => "SESSION_READY",
        Frame::SessionClose { .. } => "SESSION_CLOSE",
        Frame::PathJoin { .. } => "PATH_JOIN",
        Frame::PathJoinOk { .. } => "PATH_JOIN_OK",
        Frame::PathChallenge { .. } => "PATH_CHALLENGE",
        Frame::PathResponse { .. } => "PATH_RESPONSE",
        Frame::PathStatus { .. } => "PATH_STATUS",
        Frame::PathDrain { .. } => "PATH_DRAIN",
        Frame::PathClose { .. } => "PATH_CLOSE",
        Frame::PathMtuProbe { .. } => "PATH_MTU_PROBE",
        Frame::PathMtuAck { .. } => "PATH_MTU_ACK",
        Frame::OpenStream { .. } => "OPEN_STREAM",
        Frame::StreamData { .. } => "STREAM_DATA",
        Frame::StreamAck { .. } => "STREAM_ACK",
        Frame::StreamMaxData { .. } => "STREAM_MAX_DATA",
        Frame::StreamFin { .. } => "STREAM_FIN",
        Frame::StreamDetach { .. } => "STREAM_DETACH",
        Frame::StreamReset { .. } => "STREAM_RESET",
        Frame::OpenDatagramFlow { .. } => "OPEN_DGRAM_FLOW",
        Frame::DatagramData { .. } => "DGRAM_DATA",
        Frame::DatagramClose { .. } => "DGRAM_CLOSE",
        Frame::DatagramFeedback { .. } => "DGRAM_FEEDBACK",
        Frame::PathMetrics { .. } => "PATH_METRICS",
        Frame::RxRateHint { .. } => "RX_RATE_HINT",
        Frame::MaxConnectionData { .. } => "MAX_CONNECTION_DATA",
        Frame::Ping { .. } => "PING",
        Frame::Pong { .. } => "PONG",
    }
}

fn frame_subject(frame: &Frame) -> String {
    match frame {
        Frame::SessionHello { session_id } => format!("session_id={}", session_id.0),
        Frame::SessionAuth { session_id, .. } => format!("session_id={}", session_id.0),
        Frame::SessionReady => "none".to_string(),
        Frame::SessionClose { reason } => format!("reason={reason:?}"),
        Frame::PathJoin {
            session_id,
            path_id,
            underlay,
            ..
        } => format!(
            "session_id={} path_id={} underlay={underlay:?}",
            session_id.0, path_id.0
        ),
        Frame::PathJoinOk { path_id, .. }
        | Frame::PathChallenge { path_id, .. }
        | Frame::PathResponse { path_id, .. }
        | Frame::PathDrain { path_id }
        | Frame::PathMtuProbe { path_id, .. }
        | Frame::PathMtuAck { path_id, .. }
        | Frame::RxRateHint { path_id, .. } => format!("path_id={}", path_id.0),
        Frame::PathStatus {
            path_id, status, ..
        } => format!("path_id={} status={status:?}", path_id.0),
        Frame::PathClose { path_id, reason } => {
            format!("path_id={} reason={reason:?}", path_id.0)
        }
        Frame::OpenStream { stream_id, .. } => format!("stream_id={}", stream_id.0),
        Frame::StreamData {
            stream_id,
            offset,
            payload,
            ..
        } => format!(
            "stream_id={} offset={} payload_len={}",
            stream_id.0,
            offset,
            payload.len()
        ),
        Frame::StreamAck {
            stream_id,
            complete,
            ranges,
        } => {
            format!(
                "stream_id={} complete={} ranges={}",
                stream_id.0,
                complete,
                ranges.len()
            )
        }
        Frame::StreamMaxData {
            stream_id,
            max_offset,
        } => format!("stream_id={} max_offset={max_offset}", stream_id.0),
        Frame::StreamFin { stream_id, .. } | Frame::StreamDetach { stream_id } => {
            format!("stream_id={}", stream_id.0)
        }
        Frame::StreamReset { stream_id, reason } => {
            format!("stream_id={} reason={reason:?}", stream_id.0)
        }
        Frame::OpenDatagramFlow { flow_id, .. } => format!("flow_id={}", flow_id.0),
        Frame::DatagramData {
            flow_id,
            datagram_id,
            ttl_ms,
            payload,
        } => format!(
            "flow_id={} datagram_id={} ttl_ms={} payload_len={}",
            flow_id.0,
            datagram_id.0,
            ttl_ms,
            payload.len()
        ),
        Frame::DatagramClose { flow_id } => format!("flow_id={}", flow_id.0),
        Frame::DatagramFeedback { flow_id, received } => {
            format!("flow_id={} ranges={}", flow_id.0, received.len())
        }
        Frame::PathMetrics { metrics } => format!("path_id={}", metrics.path_id.0),
        Frame::MaxConnectionData { max_bytes } => format!("max_bytes={max_bytes}"),
        Frame::Ping { nonce } | Frame::Pong { nonce } => format!("nonce={nonce}"),
    }
}

pub(super) fn log_unexpected_stream_relay_frame(
    kind: &'static str,
    expected: StreamId,
    frame: &Frame,
) {
    eprintln!(
        "warning: unexpected {kind} stream relay frame: expected_stream_id={} frame_kind={} {}",
        expected.0,
        frame_kind_name(frame),
        frame_subject(frame)
    );
}

pub(super) enum ServerTcpPathEvent {
    Frame(Frame),
    Command(TcpPathSessionCommand),
}

pub(super) async fn recv_server_tcp_path_event(
    path_frames: &mut mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>>,
    commands_rx: &mut TcpPathSessionCommandReceivers,
) -> Result<Option<ServerTcpPathEvent>, RuntimeError> {
    loop {
        let command_may_recv = !tcp_path_receivers_closed(commands_rx);
        tokio::select! {
            biased;
            frame = path_frames.recv() => {
                return match frame {
                    Some(Ok(frame)) => Ok(Some(ServerTcpPathEvent::Frame(frame))),
                    Some(Err(err)) => Err(RuntimeError::Encrypted(err)),
                    None => Err(RuntimeError::TcpPathSessionClosed),
                };
            }
            command = recv_tcp_path_command(commands_rx), if command_may_recv => {
                match command {
                    Some(command) => return Ok(Some(ServerTcpPathEvent::Command(command))),
                    None if tcp_path_receivers_closed(commands_rx) => return Ok(None),
                    None => continue,
                }
            }
        }
    }
}
