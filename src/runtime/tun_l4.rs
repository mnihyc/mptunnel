use super::*;

pub(super) async fn run_tun_l4_client(
    tun: TunL4Config,
    context: ClientPathContext,
) -> Result<(), RuntimeError> {
    let device = build_tun_device(&tun)?;
    let framed = DeviceFramed::new(device, BytesCodec::new());
    let (mut tun_sink, mut tun_stream) = framed.split();

    let (stack, runner, udp_socket, tcp_listener) = StackBuilder::default()
        .enable_tcp(true)
        .enable_udp(true)
        .enable_icmp(tun.enable_icmp)
        .mtu(usize::from(tun.mtu))
        .build()?;
    let runner = runner.ok_or(RuntimeError::Protocol("TUN stack runner is unavailable"))?;
    let udp_socket = udp_socket.ok_or(RuntimeError::Protocol("TUN UDP socket is unavailable"))?;
    let tcp_listener =
        tcp_listener.ok_or(RuntimeError::Protocol("TUN TCP listener is unavailable"))?;
    let (mut stack_sink, mut stack_stream) = stack.split();

    let stack_to_tun = async move {
        while let Some(packet) = stack_stream.next().await {
            let packet = packet?;
            tun_sink.send(BytesMut::from(packet.as_slice())).await?;
        }
        Ok::<(), RuntimeError>(())
    };
    let tun_to_stack = async move {
        while let Some(packet) = tun_stream.next().await {
            let packet = packet?;
            stack_sink.send(packet.to_vec()).await?;
        }
        Ok::<(), RuntimeError>(())
    };
    let stack_runner = async move { runner.await.map_err(RuntimeError::Io) };

    tokio::try_join!(
        stack_runner,
        stack_to_tun,
        tun_to_stack,
        run_tun_tcp_listener(tcp_listener, context.clone()),
        run_tun_udp_socket(udp_socket, context, tun)
    )?;
    Ok(())
}

pub(super) fn build_tun_device(tun: &TunL4Config) -> Result<tun_rs::AsyncDevice, RuntimeError> {
    let mut builder = DeviceBuilder::new().mtu(tun.mtu);
    if let Some(name) = &tun.name {
        builder = builder.name(name.clone());
    }
    if let Some(ipv4) = tun.ipv4 {
        builder = builder.ipv4(ipv4, tun.ipv4_prefix, tun.ipv4_gateway);
    }
    if let Some(ipv6) = tun.ipv6 {
        builder = builder.ipv6(ipv6, tun.ipv6_prefix);
    }
    builder.build_async().map_err(RuntimeError::TunDevice)
}

pub(super) async fn run_tun_tcp_listener(
    mut listener: TunTcpListener,
    context: ClientPathContext,
) -> Result<(), RuntimeError> {
    while let Some((stream, local, remote)) = listener.next().await {
        let context = context.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_tun_tcp_stream(stream, local, remote, context).await {
                eprintln!("warning: TUN TCP flow {local} -> {remote} failed: {err}");
            }
        });
    }
    Ok(())
}

pub(super) async fn handle_tun_tcp_stream<S>(
    stream: S,
    _local: SocketAddr,
    remote: SocketAddr,
    context: ClientPathContext,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let target = TargetAddr::Ip(remote);
    outbound::validate_target(&target)?;
    let remote = open_remote_stream(
        &context,
        target.clone(),
        IngressKind::TunTcp,
        TrafficClass::Interactive,
    )
    .await?;
    relay_migrating_tcp_stream(
        stream,
        &context,
        TcpRelayOpenSpec {
            target,
            ingress: IngressKind::TunTcp,
        },
        remote,
    )
    .await?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct TunUdpFlowKey {
    local: SocketAddr,
    remote: SocketAddr,
}

pub(super) struct TunUdpResponse {
    payload: Vec<u8>,
    source: SocketAddr,
    destination: SocketAddr,
}

pub(super) struct UdpEdgeRequest<M> {
    pub(super) target: TargetAddr,
    pub(super) payload: Bytes,
    pub(super) ttl_ms: u32,
    pub(super) metadata: M,
}

pub(super) struct UdpEdgeCompletion<M> {
    pub(super) lane_id: usize,
    pub(super) target: TargetAddr,
    pub(super) metadata: M,
    pub(super) result: Result<Bytes, RuntimeError>,
}

pub(super) struct UdpEdgeLane<M> {
    pub(super) lane_id: usize,
    pub(super) pending: usize,
    pub(super) successful_completions: usize,
    pub(super) requests: mpsc::Sender<UdpEdgeRequest<M>>,
    pub(super) handle: tokio::task::JoinHandle<()>,
}

pub(super) fn udp_edge_queue_slots(context: &ClientPathContext) -> usize {
    let payload = context.mux_limits.max_payload_bytes.max(1);
    (context.mux_limits.max_datagram_queue_bytes / payload).max(1)
}

pub(super) fn udp_edge_path_lane_parallelism(snapshot: PathSnapshot) -> usize {
    if snapshot.state == SchedulerPathState::Failed {
        return 0;
    }
    let model = UdpPathRuntimeModel::from_snapshot(
        snapshot,
        DEFAULT_SOCKS5_UDP_TTL_MS,
        UDP_DEFAULT_MTU_PAYLOAD_BYTES,
        false,
        UDP_MAX_MTU_PAYLOAD_BYTES,
    );
    (model.response_timeout.as_secs_f64() / UDP_MIN_RESPONSE_TIMEOUT.as_secs_f64())
        .ceil()
        .max(2.0) as usize
}

pub(super) fn udp_edge_lane_limit(context: &ClientPathContext) -> usize {
    let path_parallelism = (0..context.udp_paths.len())
        .filter_map(|index| context.udp_path_snapshot(index))
        .map(udp_edge_path_lane_parallelism)
        .sum::<usize>();
    udp_edge_queue_slots(context).min(path_parallelism.max(1))
}

pub(super) fn udp_edge_startup_lane_limit(context: &ClientPathContext) -> usize {
    let queue_slots = udp_edge_queue_slots(context);
    let hedge_lane = usize::from(queue_slots > 1 && !context.udp_paths.is_empty());
    udp_edge_lane_limit(context)
        .min(queue_slots)
        .min(1usize.saturating_add(hedge_lane))
        .max(1)
}

pub(super) fn udp_edge_lane_spawn_allowed(
    lane_count: usize,
    successful_lane_count: usize,
    context: &ClientPathContext,
) -> bool {
    if lane_count < udp_edge_startup_lane_limit(context) {
        return true;
    }
    successful_lane_count > 0
}

pub(super) fn udp_edge_lane_queue(context: &ClientPathContext) -> usize {
    let lanes = udp_edge_lane_limit(context).max(1);
    (udp_edge_queue_slots(context) / lanes).max(1)
}

pub(super) fn udp_edge_completion_queue(context: &ClientPathContext) -> usize {
    udp_edge_lane_limit(context)
        .saturating_mul(udp_edge_lane_queue(context))
        .max(1)
}

pub(super) fn spawn_udp_edge_lane<M: Send + 'static>(
    lane_id: usize,
    context: ClientPathContext,
    lane_queue: usize,
    completions: mpsc::Sender<UdpEdgeCompletion<M>>,
) -> UdpEdgeLane<M> {
    let (requests, rx) = mpsc::channel(lane_queue);
    let handle = tokio::spawn(run_udp_edge_lane(lane_id, context, rx, completions));
    UdpEdgeLane {
        lane_id,
        pending: 0,
        successful_completions: 0,
        requests,
        handle,
    }
}

pub(super) async fn run_udp_edge_lane<M: Send + 'static>(
    lane_id: usize,
    context: ClientPathContext,
    mut requests: mpsc::Receiver<UdpEdgeRequest<M>>,
    completions: mpsc::Sender<UdpEdgeCompletion<M>>,
) {
    let mut association = match DatagramClientAssociation::new(context).await {
        Ok(association) => association,
        Err(err) => {
            eprintln!("warning: UDP edge lane could not start: {err}");
            return;
        }
    };
    while let Some(request) = requests.recv().await {
        let UdpEdgeRequest {
            target,
            payload,
            ttl_ms,
            metadata,
        } = request;
        let result = association
            .send_to_with_adaptive_retries(target.clone(), payload, ttl_ms)
            .await;
        if completions
            .send(UdpEdgeCompletion {
                lane_id,
                target,
                metadata,
                result,
            })
            .await
            .is_err()
        {
            break;
        }
    }
    if let Err(err) = association.close().await {
        eprintln!("warning: UDP edge lane close failed: {err}");
    }
}

pub(super) fn dispatch_udp_edge_request<M: Send + 'static>(
    lanes: &mut Vec<UdpEdgeLane<M>>,
    next_lane_id: &mut usize,
    context: &ClientPathContext,
    completions: &mpsc::Sender<UdpEdgeCompletion<M>>,
    request: UdpEdgeRequest<M>,
) -> Result<(), UdpEdgeRequest<M>> {
    let lane_limit = udp_edge_lane_limit(context);
    let lane_queue = udp_edge_lane_queue(context);
    let successful_lane_count = lanes
        .iter()
        .filter(|lane| lane.successful_completions > 0)
        .count();
    if lanes.is_empty()
        || (lanes.len() < lane_limit
            && lanes.iter().all(|lane| lane.pending > 0)
            && udp_edge_lane_spawn_allowed(lanes.len(), successful_lane_count, context))
    {
        let lane_id = *next_lane_id;
        *next_lane_id = next_lane_id.saturating_add(1);
        lanes.push(spawn_udp_edge_lane(
            lane_id,
            context.clone(),
            lane_queue,
            completions.clone(),
        ));
    }

    let Some((position, _)) = lanes
        .iter()
        .enumerate()
        .min_by_key(|(_, lane)| (lane.pending, lane.lane_id))
    else {
        return Err(request);
    };

    match lanes[position].requests.try_send(request) {
        Ok(()) => {
            lanes[position].pending = lanes[position].pending.saturating_add(1);
            Ok(())
        }
        Err(mpsc::error::TrySendError::Full(request)) => Err(request),
        Err(mpsc::error::TrySendError::Closed(request)) => {
            lanes.swap_remove(position);
            Err(request)
        }
    }
}

pub(super) fn finish_udp_edge_completion<M>(
    lanes: &mut [UdpEdgeLane<M>],
    completion: &UdpEdgeCompletion<M>,
) {
    if let Some(lane) = lanes
        .iter_mut()
        .find(|lane| lane.lane_id == completion.lane_id)
    {
        lane.pending = lane.pending.saturating_sub(1);
        if completion.result.is_ok() {
            lane.successful_completions = lane.successful_completions.saturating_add(1);
        }
    }
}

pub(super) async fn close_udp_edge_lanes<M>(lanes: Vec<UdpEdgeLane<M>>) {
    let handles = lanes
        .into_iter()
        .map(|lane| lane.handle)
        .collect::<Vec<_>>();
    for handle in handles {
        if let Err(err) = handle.await {
            eprintln!("warning: UDP edge lane task failed: {err}");
        }
    }
}

pub(super) async fn run_tun_udp_socket(
    udp_socket: TunUdpSocket,
    context: ClientPathContext,
    tun: TunL4Config,
) -> Result<(), RuntimeError> {
    let (mut read_half, mut write_half) = udp_socket.split();
    let mut flows: HashMap<TunUdpFlowKey, mpsc::Sender<Vec<u8>>> = HashMap::new();
    let flow_limit = tun_udp_flow_limit(&context);
    let flow_queue = tun_udp_flow_queue(&context);
    let response_queue = tun_udp_response_queue(&context);
    let done_queue = flow_limit.clamp(1, 1024);
    let (response_tx, mut response_rx) = mpsc::channel::<TunUdpResponse>(response_queue);
    let (done_tx, mut done_rx) = mpsc::channel::<TunUdpFlowKey>(done_queue);

    loop {
        tokio::select! {
            received = read_half.next() => {
                let Some((payload, local, remote)) = received else {
                    return Ok(());
                };
                let key = TunUdpFlowKey { local, remote };
                if !flows.contains_key(&key) {
                    if flows.len() >= flow_limit {
                        eprintln!("warning: TUN UDP flow limit reached; dropping datagram from {local} to {remote}");
                        continue;
                    }
                    let (tx, rx) = mpsc::channel(flow_queue);
                    let flow_context = context.clone();
                    let flow_tun = tun.clone();
                    let flow_responses = response_tx.clone();
                    let flow_done = done_tx.clone();
                    tokio::spawn(async move {
                        let result =
                            handle_tun_udp_flow(key, flow_context, flow_tun, rx, flow_responses)
                                .await;
                        let _ = flow_done.send(key).await;
                        if let Err(err) = result {
                            eprintln!(
                                "warning: TUN UDP flow {} -> {} failed: {err}",
                                key.local, key.remote
                            );
                        }
                    });
                    flows.insert(key, tx);
                }
                let send_result = flows
                    .get(&key)
                    .ok_or(RuntimeError::Protocol("missing TUN UDP flow"))?
                    .try_send(payload);
                match send_result {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        eprintln!("warning: TUN UDP flow queue full; dropping datagram from {local} to {remote}");
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        flows.remove(&key);
                    }
                }
            }
            response = response_rx.recv() => {
                let Some(response) = response else {
                    return Ok(());
                };
                write_half
                    .send((response.payload, response.source, response.destination))
                    .await?;
            }
            done = done_rx.recv() => {
                if let Some(key) = done {
                    flows.remove(&key);
                }
            }
        }
    }
}

pub(super) async fn handle_tun_udp_flow(
    key: TunUdpFlowKey,
    context: ClientPathContext,
    tun: TunL4Config,
    mut datagrams: mpsc::Receiver<Vec<u8>>,
    responses: mpsc::Sender<TunUdpResponse>,
) -> Result<(), RuntimeError> {
    let (completion_tx, mut completion_rx) =
        mpsc::channel::<UdpEdgeCompletion<TunUdpFlowKey>>(udp_edge_completion_queue(&context));
    let mut lanes = Vec::<UdpEdgeLane<TunUdpFlowKey>>::new();
    let mut next_lane_id = 0usize;
    let target = TargetAddr::Ip(tun_udp_target_for_remote(key.remote, &tun));
    let ttl_ms = tun_udp_ttl_ms(key.remote, &tun);
    let result = loop {
        tokio::select! {
            payload = tokio::time::timeout(TUN_UDP_FLOW_IDLE_TIMEOUT, datagrams.recv()) => {
                let payload = match payload {
                    Ok(Some(payload)) => payload,
                    Ok(None) | Err(_) => break Ok(()),
                };
                if dispatch_udp_edge_request(
                    &mut lanes,
                    &mut next_lane_id,
                    &context,
                    &completion_tx,
                    UdpEdgeRequest {
                        target: target.clone(),
                        payload: Bytes::from(payload),
                        ttl_ms,
                        metadata: key,
                    },
                )
                .is_err()
                {
                    eprintln!("warning: TUN UDP lane queue full; dropping datagram from {} to {}", key.local, key.remote);
                }
            }
            completion = completion_rx.recv() => {
                let Some(completion) = completion else {
                    break Err(RuntimeError::Protocol("TUN UDP completion channel closed"));
                };
                finish_udp_edge_completion(&mut lanes, &completion);
                match completion.result {
                    Ok(response) => {
                        responses
                            .send(TunUdpResponse {
                                payload: response.to_vec(),
                                source: completion.metadata.remote,
                                destination: completion.metadata.local,
                            })
                            .await
                            .map_err(|_| RuntimeError::Protocol("TUN UDP response channel closed"))?;
                    }
                    Err(err) => {
                        eprintln!(
                            "warning: TUN UDP datagram {} -> {} failed: {err}",
                            completion.metadata.local, completion.metadata.remote
                        );
                    }
                }
            }
            else => break Ok(()),
        }
    };
    drop(completion_tx);
    close_udp_edge_lanes(lanes).await;
    result
}

pub(super) fn tun_udp_target_for_remote(remote: SocketAddr, tun: &TunL4Config) -> SocketAddr {
    if remote.port() != 53 || tun.dns_resolvers.is_empty() {
        return remote;
    }
    tun.dns_resolvers
        .iter()
        .copied()
        .find(|resolver| resolver.ip().is_ipv4() == remote.ip().is_ipv4())
        .unwrap_or(tun.dns_resolvers[0])
}

pub(super) fn tun_udp_ttl_ms(remote: SocketAddr, tun: &TunL4Config) -> u32 {
    if remote.port() == 53 {
        tun.dns_ttl_ms
    } else {
        DEFAULT_SOCKS5_UDP_TTL_MS
    }
}

pub(super) fn tun_udp_flow_limit(context: &ClientPathContext) -> usize {
    let payload = context.mux_limits.max_payload_bytes.max(1);
    (context.mux_limits.max_datagram_queue_bytes / payload).clamp(1, 4096)
}

pub(super) fn tun_udp_flow_queue(context: &ClientPathContext) -> usize {
    let payload = context.mux_limits.max_payload_bytes.max(1);
    (context.mux_limits.max_datagram_queue_bytes / payload).clamp(1, 256)
}

pub(super) fn tun_udp_response_queue(context: &ClientPathContext) -> usize {
    let payload = context.mux_limits.max_payload_bytes.max(1);
    (context.mux_limits.max_datagram_queue_bytes / payload).clamp(1, 1024)
}
