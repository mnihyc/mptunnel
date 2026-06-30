use super::*;

pub async fn client_udp_datagram_round_trip(
    path: &PathSpec,
    security: SecurityConfig,
    resources: ResourceLimits,
    target: TargetAddr,
    payload: Bytes,
    ttl_ms: u32,
) -> Result<Bytes, RuntimeError> {
    client_udp_datagram_round_trip_with_limits(
        path,
        security,
        resources.into(),
        resources.into(),
        target,
        payload,
        ttl_ms,
    )
    .await
}

async fn client_udp_datagram_round_trip_with_limits(
    path: &PathSpec,
    security: SecurityConfig,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    target: TargetAddr,
    payload: Bytes,
    ttl_ms: u32,
) -> Result<Bytes, RuntimeError> {
    let payload_len = payload.len();
    let mut session = UdpDatagramClientSession::open(
        path,
        0,
        security,
        codec_limits,
        mux_limits,
        UDP_PATH_HANDSHAKE_TIMEOUT,
    )
    .await?;
    let response = session
        .send_to(target, payload, ttl_ms, UDP_MAX_RESPONSE_TIMEOUT)
        .await
        .map_err(|err| match err {
            UdpPathSendError::Runtime(err) => err,
            UdpPathSendError::MtuExceeded { limit } => {
                RuntimeError::Datagram(DatagramError::PayloadTooLarge {
                    actual: payload_len,
                    limit,
                })
            }
            UdpPathSendError::Timeout { .. } => {
                RuntimeError::Protocol("UDP datagram response timed out")
            }
        })?;
    session.close().await?;
    Ok(response)
}

pub(super) enum DatagramClientAssociation {
    Udp {
        primary: Box<UdpDatagramClientAssociation>,
        tcp_relay: Option<Box<TcpDatagramClientAssociation>>,
    },
    Tcp(Box<TcpDatagramClientAssociation>),
}

impl DatagramClientAssociation {
    pub(super) async fn new(context: ClientPathContext) -> Result<Self, RuntimeError> {
        if !context.udp_paths.is_empty() {
            return Ok(Self::Udp {
                primary: Box::new(UdpDatagramClientAssociation::new(context)?),
                tcp_relay: None,
            });
        }
        if context.tcp_paths.is_empty() {
            return Err(RuntimeError::NoDatagramPath);
        }
        Ok(Self::Tcp(Box::new(
            TcpDatagramClientAssociation::open_best(context, PATH_OPEN_SCORE_BYTES).await?,
        )))
    }

    pub(super) async fn send_to_with_adaptive_retries(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        ttl_ms: u32,
    ) -> Result<Bytes, RuntimeError> {
        match self {
            Self::Udp { primary, tcp_relay } => {
                match primary
                    .send_to_with_adaptive_retries(target.clone(), payload.clone(), ttl_ms)
                    .await
                {
                    Ok(response) => Ok(response),
                    Err(err) if udp_datagram_should_try_tcp_underlay(&err, &primary.context) => {
                        if tcp_relay.is_none() {
                            match TcpDatagramClientAssociation::open_best(
                                primary.context.clone(),
                                payload.len(),
                            )
                            .await
                            {
                                Ok(association) => *tcp_relay = Some(Box::new(association)),
                                Err(tcp_err)
                                    if matches!(
                                        err,
                                        RuntimeError::NoUdpPath
                                            | RuntimeError::NoSchedulableUdpPath
                                    ) =>
                                {
                                    return Err(tcp_err);
                                }
                                Err(_) => return Err(err),
                            }
                        }
                        let relay = tcp_relay
                            .as_mut()
                            .ok_or(RuntimeError::NoSchedulableTcpPath)?;
                        relay
                            .send_to_with_adaptive_retries(target, payload, ttl_ms)
                            .await
                    }
                    Err(err) => Err(err),
                }
            }
            Self::Tcp(association) => {
                association
                    .send_to_with_adaptive_retries(target, payload, ttl_ms)
                    .await
            }
        }
    }

    pub(super) async fn close(&mut self) -> Result<(), RuntimeError> {
        match self {
            Self::Udp { primary, tcp_relay } => {
                let primary_result = primary.close().await;
                let tcp_relay_result = if let Some(relay) = tcp_relay {
                    relay.close().await
                } else {
                    Ok(())
                };
                primary_result.and(tcp_relay_result)
            }
            Self::Tcp(association) => association.close().await,
        }
    }
}

fn udp_datagram_should_try_tcp_underlay(err: &RuntimeError, context: &ClientPathContext) -> bool {
    !context.tcp_paths.is_empty()
        && matches!(
            err,
            RuntimeError::NoUdpPath
                | RuntimeError::NoSchedulableUdpPath
                | RuntimeError::Io(_)
                | RuntimeError::Udp(_)
                | RuntimeError::UdpCarrierTransport(_)
                | RuntimeError::UdpCarrierFrame(_)
                | RuntimeError::UdpCarrierConnection(_)
                | RuntimeError::QuicCarrier(_)
                | RuntimeError::Auth(_)
                | RuntimeError::RemoteClosed(_)
                | RuntimeError::Protocol(_)
        )
}

pub(super) struct TcpDatagramClientAssociation {
    context: ClientPathContext,
    session: TcpDatagramClientSession,
}

impl TcpDatagramClientAssociation {
    async fn open_best(
        context: ClientPathContext,
        payload_bytes: usize,
    ) -> Result<Self, RuntimeError> {
        if context.tcp_paths.is_empty() {
            return Err(RuntimeError::NoTcpPath);
        }
        let candidates = context.ordered_tcp_path_indices(
            FlowLane::RealtimeDatagram,
            payload_bytes.max(PATH_OPEN_SCORE_BYTES),
        );
        if candidates.is_empty() {
            return Err(RuntimeError::NoSchedulableTcpPath);
        }
        let mut last_retryable_error = None;
        for path_index in candidates {
            let started_at = Instant::now();
            match TcpDatagramClientSession::open(&context, path_index).await {
                Ok(session) => {
                    context.mark_tcp_path_open_success(
                        path_index,
                        started_at.elapsed(),
                        FlowLane::RealtimeDatagram,
                    );
                    return Ok(Self { context, session });
                }
                Err(err) if tcp_datagram_error_is_path_retryable(&err) => {
                    context.mark_tcp_path_failure(path_index);
                    last_retryable_error = Some(err);
                }
                Err(err) => return Err(err),
            }
        }
        Err(last_retryable_error.unwrap_or(RuntimeError::NoSchedulableTcpPath))
    }

    async fn send_to_with_adaptive_retries(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        ttl_ms: u32,
    ) -> Result<Bytes, RuntimeError> {
        let started_at = Instant::now();
        loop {
            match self
                .session
                .send_to(target.clone(), payload.clone(), ttl_ms)
                .await
            {
                Ok(response) => return Ok(response),
                Err(err) if tcp_datagram_error_is_path_retryable(&err) => {
                    let failed_path_index = self.session.path_index;
                    self.context.mark_tcp_path_failure(failed_path_index);
                    let _ = self.session.close().await;
                    if started_at.elapsed() >= self.adaptive_retry_budget(ttl_ms) {
                        return Err(err);
                    }
                    let replacement = Self::open_best(self.context.clone(), payload.len()).await?;
                    self.session = replacement.session;
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn adaptive_retry_budget(&self, ttl_ms: u32) -> Duration {
        datagram_retry_budget(self.session.response_timeout(ttl_ms), ttl_ms)
    }

    async fn close(&mut self) -> Result<(), RuntimeError> {
        let close_result = self.session.close().await;
        self.context
            .mark_tcp_path_delivery(self.session.path_index, self.session.delivery_stats());
        self.context
            .release_tcp_path_load(self.session.path_index, FlowLane::RealtimeDatagram);
        close_result
    }
}

struct TcpDatagramClientSession {
    connection: ClientTcpPathConnection,
    flows: Vec<UdpDatagramClientFlow>,
    next_flow_id: u64,
    mux_limits: MuxLimits,
    path_index: usize,
    path_id: PathId,
    path_snapshot: PathSnapshot,
    stats: PathDeliveryStats,
    sent_datagrams: HashMap<(DatagramFlowId, DatagramId), UdpSentDatagram>,
    last_datagram_rtt: Option<Duration>,
    response_rttvar: Option<Duration>,
}

impl TcpDatagramClientSession {
    async fn open(context: &ClientPathContext, path_index: usize) -> Result<Self, RuntimeError> {
        let path = context
            .tcp_paths
            .get(path_index)
            .ok_or(RuntimeError::NoSchedulableTcpPath)?;
        let path_snapshot = context
            .tcp_path_snapshot(path_index)
            .ok_or(RuntimeError::NoSchedulableTcpPath)?;
        let session_id = random_session_id()?;
        let security = context.tcp_path_security(path_index)?;
        let connection = connect_client_tcp_path(
            path,
            path_index,
            session_id,
            security,
            context.codec_limits,
            context.mux_limits,
        )
        .await?;
        Ok(Self {
            connection,
            flows: Vec::new(),
            next_flow_id: 0,
            mux_limits: context.mux_limits,
            path_index,
            path_id: PathId(path_index as u16),
            path_snapshot,
            stats: PathDeliveryStats::default(),
            sent_datagrams: HashMap::new(),
            last_datagram_rtt: None,
            response_rttvar: None,
        })
    }

    async fn send_to(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        ttl_ms: u32,
    ) -> Result<Bytes, RuntimeError> {
        if payload.len() > self.mux_limits.max_payload_bytes {
            return Err(RuntimeError::Datagram(DatagramError::PayloadTooLarge {
                actual: payload.len(),
                limit: self.mux_limits.max_payload_bytes,
            }));
        }
        tick_client_tcp_path_heartbeat(&mut self.connection, self.mux_limits, true).await?;
        let response_timeout = self.response_timeout(ttl_ms);
        let flow_id = self.ensure_flow(target).await?;
        let frame = {
            let flow = self
                .flows
                .iter_mut()
                .find(|flow| flow.flow_id == flow_id)
                .ok_or(RuntimeError::Protocol("missing TCP datagram flow"))?;
            flow.flow.enqueue(0, ttl_ms, payload)?;
            flow.flow
                .pop_frame(0)
                .ok_or(RuntimeError::Protocol("datagram expired before TCP send"))?
        };
        let (request_datagram_id, request_len) = match &frame {
            Frame::DatagramData {
                datagram_id,
                payload,
                ..
            } => (*datagram_id, payload.len()),
            _ => return Err(RuntimeError::Protocol("unexpected queued datagram frame")),
        };
        let request_key = (flow_id, request_datagram_id);
        let request_started_at = Instant::now();
        let mut request_acked = false;
        let mut retransmit_count = 0_u32;
        loop {
            self.sent_datagrams.insert(
                request_key,
                UdpSentDatagram {
                    sent_at: Instant::now(),
                    bytes: request_len,
                    ttl: Duration::from_millis(u64::from(ttl_ms)),
                },
            );
            self.connection.writer.write_frame(&frame).await?;
            self.connection.writer.flush().await?;
            loop {
                let attempt_timeout = datagram_attempt_timeout(response_timeout, retransmit_count);
                let received = match tokio::time::timeout(
                    attempt_timeout,
                    self.connection.frames.recv(),
                )
                .await
                {
                    Ok(Some(Ok(frame))) => frame,
                    Ok(Some(Err(err))) => return Err(RuntimeError::Encrypted(err)),
                    Ok(None) => return Err(RuntimeError::TcpPathSessionClosed),
                    Err(_)
                        if request_acked
                            && datagram_retry_deadline_allows(
                                request_started_at,
                                response_timeout,
                                ttl_ms,
                                retransmit_count.saturating_add(1),
                            ) =>
                    {
                        retransmit_count = retransmit_count.saturating_add(1);
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "tcp_datagram_retransmit_after_timeout",
                            format_args!(
                                "path_id={} path_index={} flow_id={} datagram_id={} response_timeout_ms={} attempt_timeout_ms={} request_acked={} retransmit_count={}",
                                self.path_id.0,
                                self.path_index,
                                flow_id.0,
                                request_datagram_id.0,
                                response_timeout.as_millis(),
                                attempt_timeout.as_millis(),
                                request_acked,
                                retransmit_count
                            ),
                        );
                        break;
                    }
                    Err(_) => {
                        self.sent_datagrams.remove(&request_key);
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "tcp_datagram_response_timeout",
                            format_args!(
                                "path_id={} path_index={} flow_id={} datagram_id={} response_timeout_ms={} attempt_timeout_ms={} request_acked={} retransmit_count={}",
                                self.path_id.0,
                                self.path_index,
                                flow_id.0,
                                request_datagram_id.0,
                                response_timeout.as_millis(),
                                attempt_timeout.as_millis(),
                                request_acked,
                                retransmit_count
                            ),
                        );
                        return Err(RuntimeError::Protocol("TCP datagram response timed out"));
                    }
                };
                refresh_client_tcp_path_liveness(&mut self.connection, self.mux_limits);
                match received {
                    Frame::DatagramFeedback { flow_id, received } => {
                        if flow_id == request_key.0
                            && datagram_id_is_in_ranges(request_datagram_id, &received)
                        {
                            request_acked = true;
                        }
                        self.handle_datagram_feedback(flow_id, &received)?;
                    }
                    Frame::DatagramData {
                        flow_id: response_flow_id,
                        datagram_id,
                        payload,
                        ..
                    } if response_flow_id == flow_id && datagram_id == request_datagram_id => {
                        let now = Instant::now();
                        let lost = self.expire_unacked_datagrams(now);
                        if let Some(sent) = self.sent_datagrams.remove(&request_key) {
                            self.observe_datagram_response(sent, now, lost);
                        }
                        self.connection
                            .writer
                            .write_frame(&Frame::DatagramFeedback {
                                flow_id,
                                received: vec![datagram_ack_range(datagram_id)?],
                            })
                            .await?;
                        self.connection.writer.flush().await?;
                        self.stats.record_payload_bytes(request_len);
                        self.stats.record_payload_bytes(payload.len());
                        return Ok(payload);
                    }
                    Frame::DatagramData {
                        flow_id: response_flow_id,
                        datagram_id,
                        ..
                    } if response_flow_id == flow_id => {
                        self.connection
                            .writer
                            .write_frame(&Frame::DatagramFeedback {
                                flow_id,
                                received: vec![datagram_ack_range(datagram_id)?],
                            })
                            .await?;
                        self.connection.writer.flush().await?;
                    }
                    Frame::DatagramClose {
                        flow_id: closed_flow_id,
                    } if closed_flow_id == flow_id => {
                        return Err(RuntimeError::Protocol("TCP datagram flow closed"));
                    }
                    Frame::Ping { nonce } => {
                        self.connection
                            .writer
                            .write_frame(&Frame::Pong { nonce })
                            .await?;
                        self.connection.writer.flush().await?;
                    }
                    Frame::Pong { nonce } => {
                        if self
                            .connection
                            .pending_heartbeat
                            .is_some_and(|(pending_nonce, _)| pending_nonce == nonce)
                        {
                            self.connection.pending_heartbeat = None;
                        }
                    }
                    Frame::PathStatus { .. } | Frame::SessionReady => {}
                    Frame::PathClose { .. } => return Err(RuntimeError::TcpPathSessionClosed),
                    Frame::SessionClose { reason } => {
                        return Err(RuntimeError::RemoteClosed(reason));
                    }
                    _ => return Err(RuntimeError::Protocol("unexpected TCP datagram frame")),
                }
            }
        }
    }

    async fn ensure_flow(&mut self, target: TargetAddr) -> Result<DatagramFlowId, RuntimeError> {
        if let Some(flow) = self.flows.iter().find(|flow| flow.target == target) {
            return Ok(flow.flow_id);
        }
        let flow_id = DatagramFlowId(self.next_flow_id);
        self.next_flow_id = self
            .next_flow_id
            .checked_add(1)
            .ok_or(RuntimeError::Protocol("TCP datagram flow id overflow"))?;
        self.connection
            .writer
            .write_frame(&Frame::OpenDatagramFlow {
                flow_id,
                target: target.clone(),
                ingress: IngressKind::Socks5,
                outbound: OutboundPolicy::Direct,
            })
            .await?;
        self.connection.writer.flush().await?;
        self.flows.push(UdpDatagramClientFlow {
            target,
            flow: DatagramFlow::new(flow_id, self.mux_limits),
            flow_id,
        });
        Ok(flow_id)
    }

    async fn close(&mut self) -> Result<(), RuntimeError> {
        for flow in &self.flows {
            self.connection
                .writer
                .write_frame(&Frame::DatagramClose {
                    flow_id: flow.flow_id,
                })
                .await?;
        }
        self.flows.clear();
        close_client_tcp_path(&mut self.connection, self.path_id, false).await
    }

    fn response_timeout(&self, ttl_ms: u32) -> Duration {
        tcp_datagram_response_timeout(
            self.path_snapshot,
            self.last_datagram_rtt,
            self.response_rttvar,
            ttl_ms,
        )
    }

    fn delivery_stats(&self) -> PathDeliveryStats {
        self.stats
    }

    fn handle_datagram_feedback(
        &mut self,
        flow_id: DatagramFlowId,
        ranges: &[OffsetRange],
    ) -> Result<(), RuntimeError> {
        let now = Instant::now();
        self.expire_unacked_datagrams(now);
        let acked_keys = self
            .sent_datagrams
            .keys()
            .copied()
            .filter(|(pending_flow_id, datagram_id)| {
                *pending_flow_id == flow_id && datagram_id_is_in_ranges(*datagram_id, ranges)
            })
            .collect::<Vec<_>>();
        for key in acked_keys {
            if let Some(sent) = self.sent_datagrams.remove(&key) {
                self.observe_datagram_response(sent, now, 0);
            }
        }
        Ok(())
    }

    fn expire_unacked_datagrams(&mut self, now: Instant) -> u64 {
        let expired = self
            .sent_datagrams
            .iter()
            .filter_map(|(key, sent)| {
                (now.duration_since(sent.sent_at) >= sent.ttl).then_some(*key)
            })
            .collect::<Vec<_>>();
        let lost = expired.len() as u64;
        for key in expired {
            self.sent_datagrams.remove(&key);
        }
        lost
    }

    fn observe_datagram_response(&mut self, sent: UdpSentDatagram, now: Instant, _lost: u64) {
        let rtt = now
            .duration_since(sent.sent_at)
            .max(MIN_RATE_SAMPLE_DURATION);
        let previous_srtt = self.last_datagram_rtt;
        let sample_var = previous_srtt
            .map(|previous| previous.abs_diff(rtt))
            .unwrap_or_else(|| rtt.div_f64(2.0));
        self.response_rttvar = Some(match self.response_rttvar {
            Some(previous) => previous.mul_f64(0.75) + sample_var.mul_f64(0.25),
            None => sample_var,
        });
        self.last_datagram_rtt = Some(match previous_srtt {
            Some(previous) => previous.mul_f64(0.875) + rtt.mul_f64(0.125),
            None => rtt,
        });
    }
}

pub(super) fn tcp_datagram_response_timeout(
    snapshot: PathSnapshot,
    response_srtt: Option<Duration>,
    response_rttvar: Option<Duration>,
    ttl_ms: u32,
) -> Duration {
    let ttl = Duration::from_millis(u64::from(ttl_ms));
    if ttl.is_zero() {
        return ttl;
    }
    let ttl_budget = ttl
        .mul_f64(UDP_DATAGRAM_MIN_TTL_FIT_RATIO)
        .max(UDP_MIN_RESPONSE_TIMEOUT.min(ttl));
    let initial_response_rtt = Duration::from_secs_f64(
        ((snapshot.srtt_ms * 2.0 + snapshot.jitter_ms.mul_add(4.0, 25.0)) / 1000.0)
            .max(UDP_MIN_RESPONSE_TIMEOUT.as_secs_f64()),
    );
    let srtt = response_srtt.unwrap_or(initial_response_rtt);
    let rttvar = response_rttvar.unwrap_or_else(|| initial_response_rtt.div_f64(2.0));
    let loss_gain = 1.0 + snapshot.loss_rate.clamp(0.0, 1.0);
    let hol_slack = Duration::from_secs_f64(
        ((snapshot.srtt_ms + snapshot.jitter_ms * 4.0) / 1000.0)
            .max(TCP_STREAM_STALL_MIN_TIMEOUT.as_secs_f64())
            .min(TCP_STREAM_STALL_MAX_TIMEOUT.as_secs_f64()),
    );
    let fluent_cap = Duration::from_secs_f64(
        ((snapshot.srtt_ms * 4.0 + snapshot.jitter_ms * 4.0) / 1000.0)
            .max(TCP_STREAM_STALL_MAX_TIMEOUT.as_secs_f64())
            .min(TCP_STREAM_STALL_MAX_TIMEOUT.mul_f64(3.0).as_secs_f64()),
    );
    (srtt + rttvar.mul_f64(4.0) + hol_slack)
        .mul_f64(loss_gain)
        .max(UDP_MIN_RESPONSE_TIMEOUT.min(ttl))
        .min(fluent_cap)
        .min(ttl_budget)
}

pub(super) fn datagram_retry_budget(response_timeout: Duration, ttl_ms: u32) -> Duration {
    let ttl_budget = datagram_useful_ttl_budget(ttl_ms);
    if ttl_budget.is_zero() {
        return ttl_budget;
    }
    let response_timeout = response_timeout
        .max(UDP_MIN_RESPONSE_TIMEOUT.min(ttl_budget))
        .min(ttl_budget);
    let geometric_budget =
        Duration::from_secs_f64((ttl_budget.as_secs_f64() * response_timeout.as_secs_f64()).sqrt());
    geometric_budget
        .max(UDP_MIN_RETRY_BUDGET.min(ttl_budget))
        .min(ttl_budget)
}

fn datagram_useful_ttl_budget(ttl_ms: u32) -> Duration {
    let ttl = Duration::from_millis(u64::from(ttl_ms));
    if ttl.is_zero() {
        return ttl;
    }
    Duration::from_secs_f64(ttl.as_secs_f64() * UDP_DATAGRAM_MIN_TTL_FIT_RATIO)
        .max(UDP_MIN_RESPONSE_TIMEOUT.min(ttl))
        .min(ttl)
}

fn datagram_attempt_timeout(response_timeout: Duration, retransmit_count: u32) -> Duration {
    response_timeout
        .max(UDP_MIN_RESPONSE_TIMEOUT)
        .saturating_mul(2_u32.saturating_pow(retransmit_count.min(10)))
}

fn datagram_retry_deadline_allows(
    request_started_at: Instant,
    response_timeout: Duration,
    ttl_ms: u32,
    next_retransmit_count: u32,
) -> bool {
    let budget = datagram_retry_budget(response_timeout, ttl_ms);
    let elapsed = request_started_at.elapsed();
    if elapsed >= budget {
        return false;
    }
    let remaining = budget.saturating_sub(elapsed);
    remaining >= datagram_attempt_timeout(response_timeout, next_retransmit_count).min(budget)
}

fn tcp_datagram_error_is_path_retryable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Tcp(_)
            | RuntimeError::Encrypted(_)
            | RuntimeError::Auth(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
            | RuntimeError::PathHeartbeatTimeout
            | RuntimeError::TcpPathSessionClosed
    )
}

pub(super) struct UdpDatagramClientAssociation {
    pub(super) context: ClientPathContext,
    pub(super) session_id: SessionId,
    pub(super) paths: Vec<UdpDatagramAssociationPath>,
    pub(super) suppressed_paths: HashMap<usize, Instant>,
    pub(super) last_successful_path: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct UdpPathCandidate {
    pub(super) path_index: usize,
    pub(super) eta_ms: f64,
}

#[derive(Debug, Clone, Copy)]
struct UdpAssociationCandidateScore {
    path_index: usize,
    completion_ms: f64,
    eta_ms: f64,
    opens_new_session: bool,
    rank: usize,
}

pub(super) struct UdpDatagramAssociationPath {
    pub(super) session: UdpDatagramClientSession,
    pub(super) pacer: UdpDatagramPacer,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct UdpDatagramPacer {
    pub(super) next_send_at: Instant,
}

impl UdpDatagramPacer {
    fn new() -> Self {
        Self {
            next_send_at: Instant::now(),
        }
    }

    pub(super) fn ready_at(self) -> Instant {
        self.next_send_at
    }

    pub(super) async fn wait_for_send(&mut self, model: UdpPathRuntimeModel, payload_bytes: usize) {
        let now = Instant::now();
        if self.next_send_at > now {
            tokio::time::sleep(self.next_send_at.duration_since(now)).await;
        }
        self.next_send_at = Instant::now() + model.pacing_interval(payload_bytes);
    }
}

pub(super) enum UdpPathSendError {
    MtuExceeded {
        limit: usize,
    },
    Timeout {
        path_was_acked: bool,
        response_timeout: Duration,
    },
    Runtime(RuntimeError),
}

impl UdpDatagramClientAssociation {
    pub(super) fn new(context: ClientPathContext) -> Result<Self, RuntimeError> {
        Ok(Self {
            context,
            session_id: random_session_id()?,
            paths: Vec::new(),
            suppressed_paths: HashMap::new(),
            last_successful_path: None,
        })
    }

    pub(super) async fn send_to(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        ttl_ms: u32,
    ) -> Result<Bytes, RuntimeError> {
        if payload.len() > self.context.mux_limits.max_payload_bytes {
            return Err(RuntimeError::Datagram(DatagramError::PayloadTooLarge {
                actual: payload.len(),
                limit: self.context.mux_limits.max_payload_bytes,
            }));
        }
        let candidates = self
            .context
            .ordered_udp_path_candidates_for_ttl(payload.len(), ttl_ms);
        if candidates.is_empty() {
            return Err(RuntimeError::NoSchedulableUdpPath);
        }

        self.prune_suppressed_paths();
        let mut attempted = HashSet::new();
        let mut retried_acked_timeout = HashSet::new();
        let mut last_retryable_error = None;
        while let Some(path_index) =
            self.select_path_candidate(&candidates, &attempted, payload.len(), ttl_ms)
        {
            attempted.insert(path_index);
            let has_unattempted_alternative = candidates
                .iter()
                .any(|candidate| !attempted.contains(&candidate.path_index));
            match self
                .send_to_path(
                    path_index,
                    target.clone(),
                    payload.clone(),
                    ttl_ms,
                    has_unattempted_alternative,
                )
                .await
            {
                Ok(response) => {
                    self.last_successful_path = Some(path_index);
                    return Ok(response);
                }
                Err(UdpPathSendError::MtuExceeded { limit }) => {
                    last_retryable_error =
                        Some(RuntimeError::Datagram(DatagramError::PayloadTooLarge {
                            actual: payload.len(),
                            limit,
                        }));
                }
                Err(UdpPathSendError::Timeout {
                    path_was_acked,
                    response_timeout,
                }) => {
                    if path_was_acked
                        && retried_acked_timeout.insert(path_index)
                        && self.path_session_is_open(path_index)
                    {
                        self.context.mark_udp_path_feedback(
                            path_index,
                            UdpDatagramPathObservation {
                                rtt: response_timeout,
                                jitter: Duration::ZERO,
                                loss_rate: 1.0,
                                rate_sample: None,
                            },
                        );
                        attempted.remove(&path_index);
                        last_retryable_error =
                            Some(RuntimeError::Protocol("UDP datagram response timed out"));
                        continue;
                    }
                    if path_was_acked
                        && self.path_session_is_open(path_index)
                        && !self.has_validated_udp_retry_alternative(
                            &candidates,
                            &attempted,
                            path_index,
                        )
                    {
                        self.context.mark_udp_path_feedback(
                            path_index,
                            UdpDatagramPathObservation {
                                rtt: response_timeout,
                                jitter: Duration::ZERO,
                                loss_rate: 1.0,
                                rate_sample: None,
                            },
                        );
                        return Err(RuntimeError::Protocol("UDP datagram response timed out"));
                    }
                    self.remove_path(path_index).await;
                    self.suppress_path_after_timeout(path_index, response_timeout, ttl_ms);
                    if !path_was_acked {
                        self.context.mark_udp_path_failure(path_index);
                    } else {
                        self.context.mark_udp_path_feedback(
                            path_index,
                            UdpDatagramPathObservation {
                                rtt: response_timeout,
                                jitter: Duration::ZERO,
                                loss_rate: 1.0,
                                rate_sample: None,
                            },
                        );
                    }
                    last_retryable_error =
                        Some(RuntimeError::Protocol("UDP datagram response timed out"));
                }
                Err(UdpPathSendError::Runtime(err))
                    if udp_datagram_error_is_path_retryable(&err) =>
                {
                    self.remove_path(path_index).await;
                    self.suppress_path_after_timeout(path_index, UDP_MIN_RESPONSE_TIMEOUT, ttl_ms);
                    self.context.mark_udp_path_failure(path_index);
                    last_retryable_error = Some(err);
                }
                Err(UdpPathSendError::Runtime(err)) => return Err(err),
            }
        }
        Err(last_retryable_error.unwrap_or(RuntimeError::NoSchedulableUdpPath))
    }

    pub(super) async fn send_to_with_adaptive_retries(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        ttl_ms: u32,
    ) -> Result<Bytes, RuntimeError> {
        let started_at = Instant::now();
        loop {
            match self.send_to(target.clone(), payload.clone(), ttl_ms).await {
                Ok(response) => return Ok(response),
                Err(err) if udp_datagram_error_is_path_retryable(&err) => {
                    if started_at.elapsed() >= self.adaptive_retry_budget(payload.len(), ttl_ms) {
                        return Err(err);
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub(super) fn adaptive_retry_budget(&self, payload_bytes: usize, ttl_ms: u32) -> Duration {
        let ttl = Duration::from_millis(u64::from(ttl_ms));
        let response_timeout = self
            .context
            .ordered_udp_path_candidates_for_ttl(payload_bytes, ttl_ms)
            .into_iter()
            .filter_map(|candidate| {
                self.context
                    .udp_path_runtime_model(candidate.path_index, ttl_ms)
                    .map(|model| model.response_timeout)
            })
            .min()
            .unwrap_or(UDP_MAX_RESPONSE_TIMEOUT);
        datagram_retry_budget(response_timeout, ttl_ms).min(ttl)
    }

    fn path_session_is_open(&self, path_index: usize) -> bool {
        self.paths
            .iter()
            .any(|path| path.session.path_index == path_index)
    }

    pub(super) fn has_validated_udp_retry_alternative(
        &self,
        candidates: &[UdpPathCandidate],
        attempted: &HashSet<usize>,
        current_path_index: usize,
    ) -> bool {
        candidates.iter().any(|candidate| {
            candidate.path_index != current_path_index
                && !attempted.contains(&candidate.path_index)
                && self.path_has_datagram_feedback_or_hint(candidate.path_index)
        })
    }

    pub(super) async fn close(&mut self) -> Result<(), RuntimeError> {
        let mut close_error = None;
        while let Some(mut path) = self.paths.pop() {
            let close_result = path.session.close().await;
            self.context
                .mark_udp_path_delivery(path.session.path_index, path.session.delivery_stats());
            self.context.release_udp_path_load(path.session.path_index);
            if close_error.is_none()
                && let Err(err) = close_result
            {
                close_error = Some(err);
            }
        }
        match close_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    pub(super) fn select_path_candidate(
        &self,
        candidates: &[UdpPathCandidate],
        attempted: &HashSet<usize>,
        payload_bytes: usize,
        ttl_ms: u32,
    ) -> Option<usize> {
        let now = Instant::now();
        let freshness_budget_ms = f64::from(ttl_ms) * UDP_DATAGRAM_MIN_TTL_FIT_RATIO;
        let mut viable = candidates
            .iter()
            .enumerate()
            .filter(|(_, candidate)| !attempted.contains(&candidate.path_index))
            .filter_map(|(rank, candidate)| {
                let open_ready_at = self
                    .paths
                    .iter()
                    .find(|path| path.session.path_index == candidate.path_index)
                    .map(|path| path.pacer.ready_at());
                let has_open_session = open_ready_at.is_some();
                let eta_ms = self.context.udp_path_eta_for_ttl(
                    candidate.path_index,
                    payload_bytes,
                    ttl_ms,
                    has_open_session,
                )?;
                let model = self
                    .context
                    .udp_path_runtime_model(candidate.path_index, ttl_ms)?;
                if !model.accepts_or_can_probe(payload_bytes) {
                    return None;
                }
                let ready_at = open_ready_at.unwrap_or(now);
                let ready_delay_ms = ready_at.saturating_duration_since(now).as_secs_f64() * 1000.0;
                let completion_ms = eta_ms + ready_delay_ms;
                (completion_ms <= freshness_budget_ms).then_some(UdpAssociationCandidateScore {
                    path_index: candidate.path_index,
                    completion_ms,
                    eta_ms,
                    opens_new_session: !has_open_session,
                    rank,
                })
            })
            .collect::<Vec<_>>();
        if viable.iter().any(|candidate| {
            self.path_has_datagram_feedback_or_hint(candidate.path_index)
                && !self.path_is_temporarily_suppressed(candidate.path_index, now)
        }) {
            viable
                .retain(|candidate| self.path_has_datagram_feedback_or_hint(candidate.path_index));
        }
        if self.context.udp_paths.iter().all(path_is_endpoint_only)
            && let Some(candidate) = viable
                .iter()
                .filter(|candidate| !self.path_is_temporarily_suppressed(candidate.path_index, now))
                .min_by(|left, right| left.path_index.cmp(&right.path_index))
        {
            return Some(candidate.path_index);
        }
        if let Some(path_index) = self.last_successful_path
            && let Some(candidate) = viable.iter().find(|candidate| {
                candidate.path_index == path_index
                    && !self.path_is_temporarily_suppressed(candidate.path_index, now)
            })
        {
            return Some(candidate.path_index);
        }
        let has_unsuppressed = viable
            .iter()
            .any(|candidate| !self.path_is_temporarily_suppressed(candidate.path_index, now));
        if has_unsuppressed {
            viable.retain(|candidate| {
                !self.path_is_temporarily_suppressed(candidate.path_index, now)
            });
        }
        viable
            .into_iter()
            .min_by(|left, right| {
                left.completion_ms
                    .total_cmp(&right.completion_ms)
                    .then_with(|| left.eta_ms.total_cmp(&right.eta_ms))
                    .then_with(|| left.opens_new_session.cmp(&right.opens_new_session))
                    .then_with(|| left.rank.cmp(&right.rank))
            })
            .map(|candidate| candidate.path_index)
    }

    pub(super) fn suppress_path_after_timeout(
        &mut self,
        path_index: usize,
        response_timeout: Duration,
        ttl_ms: u32,
    ) {
        let ttl = Duration::from_millis(u64::from(ttl_ms));
        let adaptive = Duration::from_secs_f64(response_timeout.as_secs_f64() * 4.0)
            .max(UDP_MIN_PATH_SUPPRESSION);
        let duration = adaptive.min(PATH_FAILURE_COOLDOWN).min(ttl);
        self.suppressed_paths
            .insert(path_index, Instant::now() + duration);
    }

    fn prune_suppressed_paths(&mut self) {
        let now = Instant::now();
        self.suppressed_paths
            .retain(|_, suppressed_until| *suppressed_until > now);
    }

    fn path_is_temporarily_suppressed(&self, path_index: usize, now: Instant) -> bool {
        self.suppressed_paths
            .get(&path_index)
            .is_some_and(|suppressed_until| *suppressed_until > now)
    }

    fn path_has_datagram_feedback_or_hint(&self, path_index: usize) -> bool {
        let Some(path) = self.context.udp_paths.get(path_index) else {
            return false;
        };
        let Some(observation) = self
            .context
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(path_index)
            .map(|record| record.observe(Instant::now()))
        else {
            return false;
        };
        udp_observation_has_datagram_feedback(&observation)
            || path.metadata.initial_srtt_ms.is_some()
            || path.metadata.initial_jitter_ms.is_some()
            || path.metadata.initial_rate != RateHint::Unknown
    }

    async fn send_to_path(
        &mut self,
        path_index: usize,
        target: TargetAddr,
        payload: Bytes,
        ttl_ms: u32,
        has_unattempted_alternative: bool,
    ) -> Result<Bytes, UdpPathSendError> {
        let model = self
            .context
            .udp_path_runtime_model(path_index, ttl_ms)
            .ok_or(UdpPathSendError::Runtime(
                RuntimeError::NoSchedulableUdpPath,
            ))?;
        if !model.accepts_or_can_probe(payload.len()) {
            return Err(UdpPathSendError::MtuExceeded {
                limit: model.mtu_payload_bytes,
            });
        }
        let handshake_timeout = udp_datagram_path_open_timeout(
            !self.paths.is_empty(),
            has_unattempted_alternative,
            model,
            ttl_ms,
        );
        let position = self
            .ensure_path_session(path_index, handshake_timeout)
            .await
            .map_err(UdpPathSendError::Runtime)?;
        let current_mtu = self
            .paths
            .get(position)
            .ok_or(UdpPathSendError::Runtime(
                RuntimeError::NoSchedulableUdpPath,
            ))?
            .session
            .mtu_payload_bytes();
        if payload.len() > current_mtu {
            let probe_result = {
                let path = self
                    .paths
                    .get_mut(position)
                    .ok_or(UdpPathSendError::Runtime(
                        RuntimeError::NoSchedulableUdpPath,
                    ))?;
                tokio::time::timeout(
                    model.response_timeout,
                    path.session.probe_mtu(payload.len()),
                )
                .await
            };
            match probe_result {
                Ok(Ok(probed_mtu)) => {
                    self.context.mark_udp_path_mtu(path_index, probed_mtu);
                }
                Ok(Err(err)) if udp_datagram_error_is_path_retryable(&err) => {
                    self.context.mark_udp_path_mtu(path_index, current_mtu);
                    return Err(UdpPathSendError::MtuExceeded { limit: current_mtu });
                }
                Ok(Err(err)) => return Err(UdpPathSendError::Runtime(err)),
                Err(_) => {
                    self.context.mark_udp_path_mtu(path_index, current_mtu);
                    return Err(UdpPathSendError::MtuExceeded { limit: current_mtu });
                }
            }
        }
        let (observation_path_index, observation, result) = {
            let path = self
                .paths
                .get_mut(position)
                .ok_or(UdpPathSendError::Runtime(
                    RuntimeError::NoSchedulableUdpPath,
                ))?;
            path.pacer.wait_for_send(model, payload.len()).await;
            let result = path
                .session
                .send_to(target, payload, ttl_ms, model.response_timeout)
                .await;
            let observation = path.session.take_feedback_observation();
            (path.session.path_index, observation, result)
        };
        if let Some(observation) = observation {
            self.context
                .mark_udp_path_feedback(observation_path_index, observation);
        }

        match result {
            Ok(response) => Ok(response),
            Err(UdpPathSendError::Timeout {
                path_was_acked,
                response_timeout,
            }) => Err(UdpPathSendError::Timeout {
                path_was_acked,
                response_timeout,
            }),
            Err(err) => Err(err),
        }
    }

    async fn ensure_path_session(
        &mut self,
        path_index: usize,
        handshake_timeout: Duration,
    ) -> Result<usize, RuntimeError> {
        if let Some(position) = self
            .paths
            .iter()
            .position(|path| path.session.path_index == path_index)
        {
            return Ok(position);
        }
        let session = open_udp_datagram_session_on_path(
            &self.context,
            path_index,
            self.session_id,
            handshake_timeout,
        )
        .await?;
        self.paths.push(UdpDatagramAssociationPath {
            session,
            pacer: UdpDatagramPacer::new(),
        });
        Ok(self.paths.len() - 1)
    }

    async fn remove_path(&mut self, path_index: usize) {
        let Some(position) = self
            .paths
            .iter()
            .position(|path| path.session.path_index == path_index)
        else {
            return;
        };
        let mut path = self.paths.swap_remove(position);
        let _ = path.session.close().await;
        self.context
            .mark_udp_path_delivery(path.session.path_index, path.session.delivery_stats());
        self.context.release_udp_path_load(path.session.path_index);
    }
}

fn udp_datagram_error_is_path_retryable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Udp(_)
            | RuntimeError::UdpCarrierTransport(_)
            | RuntimeError::UdpCarrierFrame(_)
            | RuntimeError::UdpCarrierConnection(_)
            | RuntimeError::QuicCarrier(_)
            | RuntimeError::Auth(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
    )
}

pub(super) fn udp_datagram_path_open_timeout(
    association_has_open_path: bool,
    has_unattempted_alternative: bool,
    model: UdpPathRuntimeModel,
    ttl_ms: u32,
) -> Duration {
    let ttl_timeout = Duration::from_millis(u64::from(ttl_ms));
    if !association_has_open_path && !has_unattempted_alternative {
        return UDP_PATH_HANDSHAKE_TIMEOUT.min(ttl_timeout);
    }
    let response_timeout = if association_has_open_path {
        model.response_timeout
    } else {
        Duration::from_secs_f64(
            model.response_timeout.as_secs_f64() * UDP_FIRST_OPEN_RTT_MULTIPLIER,
        )
    };
    response_timeout
        .max(UDP_MIN_RESPONSE_TIMEOUT)
        .min(UDP_PATH_HANDSHAKE_TIMEOUT)
        .min(ttl_timeout)
}

pub(super) struct UdpDatagramClientSession {
    _path_session: ClientUdpPathSessionHandle,
    stream: ClientUdpDatagramStream,
    flows: Vec<UdpDatagramClientFlow>,
    next_flow_id: u64,
    mux_limits: MuxLimits,
    pub(super) path_index: usize,
    path_id: PathId,
    stats: PathDeliveryStats,
    sent_datagrams: HashMap<(DatagramFlowId, DatagramId), UdpSentDatagram>,
    last_datagram_rtt: Option<Duration>,
    last_feedback_observation: Option<UdpDatagramPathObservation>,
    mtu_payload_bytes: usize,
}

struct UdpDatagramClientFlow {
    target: TargetAddr,
    flow: DatagramFlow,
    flow_id: DatagramFlowId,
}

#[derive(Debug, Clone, Copy)]
struct UdpSentDatagram {
    sent_at: Instant,
    bytes: usize,
    ttl: Duration,
}

impl UdpDatagramClientSession {
    pub(super) async fn open(
        path: &PathSpec,
        path_index: usize,
        security: SecurityConfig,
        codec_limits: CodecLimits,
        mux_limits: MuxLimits,
        handshake_timeout: Duration,
    ) -> Result<Self, RuntimeError> {
        let session_id = random_session_id()?;
        Self::open_for_session(
            path,
            path_index,
            session_id,
            security,
            codec_limits,
            mux_limits,
            handshake_timeout,
        )
        .await
    }

    pub(super) async fn open_for_session(
        path: &PathSpec,
        path_index: usize,
        session_id: SessionId,
        security: SecurityConfig,
        codec_limits: CodecLimits,
        mux_limits: MuxLimits,
        handshake_timeout: Duration,
    ) -> Result<Self, RuntimeError> {
        let health = Arc::new(Mutex::new(ClientPathHealth {
            tcp: Vec::new(),
            udp: vec![ClientPathHealthRecord::default(); path_index.saturating_add(1)],
        }));
        let path_session = ClientUdpPathSessionHandle::new(ClientUdpPathSessionRuntime {
            path: path.clone(),
            path_index,
            session_id,
            security,
            codec_limits,
            mux_limits,
            stream_frame_queue: tcp_stream_frame_queue(mux_limits),
            health,
        });
        Self::open_from_udp_session(path_session, path_index, mux_limits, handshake_timeout).await
    }

    pub(super) async fn open_from_udp_session(
        path_session: ClientUdpPathSessionHandle,
        path_index: usize,
        mux_limits: MuxLimits,
        handshake_timeout: Duration,
    ) -> Result<Self, RuntimeError> {
        let stream = tokio::time::timeout(handshake_timeout, path_session.open_datagram_stream())
            .await
            .map_err(|_| RuntimeError::Protocol("UDP carrier datagram stream open timed out"))??;
        let path_id = stream.path_id;
        Ok(Self {
            _path_session: path_session,
            stream,
            flows: Vec::new(),
            next_flow_id: 0,
            mux_limits,
            path_index,
            path_id,
            stats: PathDeliveryStats::default(),
            sent_datagrams: HashMap::new(),
            last_datagram_rtt: None,
            last_feedback_observation: None,
            mtu_payload_bytes: mux_limits.max_payload_bytes,
        })
    }

    pub(super) async fn send_to(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        ttl_ms: u32,
        response_timeout: Duration,
    ) -> Result<Bytes, UdpPathSendError> {
        let flow_id = self
            .ensure_flow(target)
            .await
            .map_err(UdpPathSendError::Runtime)?;
        let frame = {
            let flow = self
                .flows
                .iter_mut()
                .find(|flow| flow.flow_id == flow_id)
                .ok_or(UdpPathSendError::Runtime(RuntimeError::Protocol(
                    "missing UDP datagram flow",
                )))?;
            flow.flow
                .enqueue(0, ttl_ms, payload)
                .map_err(|err| UdpPathSendError::Runtime(RuntimeError::Datagram(err)))?;
            flow.flow
                .pop_frame(0)
                .ok_or(UdpPathSendError::Runtime(RuntimeError::Protocol(
                    "datagram expired before send",
                )))?
        };
        let (request_datagram_id, request_len) = match &frame {
            Frame::DatagramData {
                datagram_id,
                payload,
                ..
            } => (*datagram_id, payload.len()),
            _ => {
                return Err(UdpPathSendError::Runtime(RuntimeError::Protocol(
                    "unexpected queued datagram frame",
                )));
            }
        };
        let request_key = (flow_id, request_datagram_id);
        self.last_feedback_observation = None;
        let request_started_at = Instant::now();
        let mut request_acked = false;
        let mut retransmit_count = 0_u32;
        let mut observed_response_timeout = false;

        loop {
            self.sent_datagrams.insert(
                request_key,
                UdpSentDatagram {
                    sent_at: Instant::now(),
                    bytes: request_len,
                    ttl: Duration::from_millis(u64::from(ttl_ms)),
                },
            );
            udp_path_write_frame(
                &mut self.stream.send,
                &frame,
                self.stream.runtime.codec_limits,
            )
            .await
            .map_err(UdpPathSendError::Runtime)?;
            loop {
                let attempt_timeout = datagram_attempt_timeout(response_timeout, retransmit_count);
                let received = match tokio::time::timeout(
                    attempt_timeout,
                    self.stream.frames.recv(),
                )
                .await
                {
                    Ok(Some(Ok(frame))) => frame,
                    Ok(Some(Err(err))) => return Err(UdpPathSendError::Runtime(err)),
                    Ok(None) => {
                        return Err(UdpPathSendError::Runtime(
                            RuntimeError::TcpPathSessionClosed,
                        ));
                    }
                    Err(_)
                        if request_acked
                            && datagram_retry_deadline_allows(
                                request_started_at,
                                response_timeout,
                                ttl_ms,
                                retransmit_count.saturating_add(1),
                            ) =>
                    {
                        observed_response_timeout = true;
                        retransmit_count = retransmit_count.saturating_add(1);
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "udp_datagram_retransmit_after_timeout",
                            format_args!(
                                "path_id={} path_index={} flow_id={} datagram_id={} response_timeout_ms={} attempt_timeout_ms={} request_acked={} retransmit_count={}",
                                self.path_id.0,
                                self.path_index,
                                flow_id.0,
                                request_datagram_id.0,
                                response_timeout.as_millis(),
                                attempt_timeout.as_millis(),
                                request_acked,
                                retransmit_count
                            ),
                        );
                        break;
                    }
                    Err(_) => {
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "udp_datagram_response_timeout",
                            format_args!(
                                "path_id={} path_index={} flow_id={} datagram_id={} response_timeout_ms={} attempt_timeout_ms={} request_acked={} retransmit_count={}",
                                self.path_id.0,
                                self.path_index,
                                flow_id.0,
                                request_datagram_id.0,
                                response_timeout.as_millis(),
                                attempt_timeout.as_millis(),
                                request_acked,
                                retransmit_count
                            ),
                        );
                        return Err(UdpPathSendError::Timeout {
                            path_was_acked: request_acked,
                            response_timeout,
                        });
                    }
                };
                match received {
                    Frame::DatagramFeedback { flow_id, received } => {
                        if flow_id == request_key.0
                            && datagram_id_is_in_ranges(request_datagram_id, &received)
                        {
                            request_acked = true;
                        }
                        self.handle_datagram_feedback(flow_id, &received)
                            .map_err(UdpPathSendError::Runtime)?;
                    }
                    Frame::DatagramData {
                        flow_id: response_flow_id,
                        datagram_id,
                        payload,
                        ..
                    } if response_flow_id == flow_id && datagram_id == request_datagram_id => {
                        let request_ack = datagram_ack_range(request_datagram_id)
                            .map_err(UdpPathSendError::Runtime)?;
                        self.handle_datagram_feedback(flow_id, &[request_ack])
                            .map_err(UdpPathSendError::Runtime)?;
                        udp_path_write_frame(
                            &mut self.stream.send,
                            &Frame::DatagramFeedback {
                                flow_id,
                                received: vec![
                                    datagram_ack_range(datagram_id)
                                        .map_err(UdpPathSendError::Runtime)?,
                                ],
                            },
                            self.stream.runtime.codec_limits,
                        )
                        .await
                        .map_err(UdpPathSendError::Runtime)?;
                        self.stats.record_payload_bytes(request_len);
                        self.stats.record_payload_bytes(payload.len());
                        if observed_response_timeout {
                            self.last_feedback_observation = Some(UdpDatagramPathObservation {
                                rtt: response_timeout,
                                jitter: Duration::ZERO,
                                loss_rate: 1.0,
                                rate_sample: None,
                            });
                        }
                        return Ok(payload);
                    }
                    Frame::DatagramData {
                        flow_id: response_flow_id,
                        datagram_id,
                        ..
                    } if response_flow_id == flow_id => {
                        udp_path_write_frame(
                            &mut self.stream.send,
                            &Frame::DatagramFeedback {
                                flow_id,
                                received: vec![
                                    datagram_ack_range(datagram_id)
                                        .map_err(UdpPathSendError::Runtime)?,
                                ],
                            },
                            self.stream.runtime.codec_limits,
                        )
                        .await
                        .map_err(UdpPathSendError::Runtime)?;
                    }
                    Frame::PathMetrics { metrics } if metrics.path_id == self.path_id => {
                        self.observe_remote_path_metrics(metrics);
                    }
                    Frame::SessionReady => {}
                    Frame::RxRateHint { path_id, .. } if path_id == self.path_id => {}
                    Frame::DatagramClose {
                        flow_id: closed_flow_id,
                    } if closed_flow_id == flow_id => {
                        return Err(UdpPathSendError::Runtime(RuntimeError::Protocol(
                            "datagram flow closed",
                        )));
                    }
                    Frame::SessionClose { reason } => {
                        return Err(UdpPathSendError::Runtime(RuntimeError::RemoteClosed(
                            reason,
                        )));
                    }
                    _ => {
                        return Err(UdpPathSendError::Runtime(RuntimeError::Protocol(
                            "unexpected UDP datagram frame",
                        )));
                    }
                }
            }
        }
    }

    async fn ensure_flow(&mut self, target: TargetAddr) -> Result<DatagramFlowId, RuntimeError> {
        if let Some(flow) = self.flows.iter().find(|flow| flow.target == target) {
            return Ok(flow.flow_id);
        }
        let flow_id = DatagramFlowId(self.next_flow_id);
        self.next_flow_id = self
            .next_flow_id
            .checked_add(1)
            .ok_or(RuntimeError::Protocol("UDP datagram flow id overflow"))?;
        udp_path_write_frame(
            &mut self.stream.send,
            &Frame::OpenDatagramFlow {
                flow_id,
                target: target.clone(),
                ingress: IngressKind::Socks5,
                outbound: OutboundPolicy::Direct,
            },
            self.stream.runtime.codec_limits,
        )
        .await?;
        self.flows.push(UdpDatagramClientFlow {
            target,
            flow: DatagramFlow::new(flow_id, self.mux_limits),
            flow_id,
        });
        Ok(flow_id)
    }

    pub(super) async fn close(&mut self) -> Result<(), RuntimeError> {
        for flow in &self.flows {
            udp_path_write_frame(
                &mut self.stream.send,
                &Frame::DatagramClose {
                    flow_id: flow.flow_id,
                },
                self.stream.runtime.codec_limits,
            )
            .await?;
        }
        self.flows.clear();
        let _ = udp_path_finish_stream(&mut self.stream.send);
        Ok(())
    }

    pub(super) async fn ping(&mut self, probe_timeout: Duration) -> Result<(), RuntimeError> {
        let nonce = random_u64()?;
        udp_path_write_frame(
            &mut self.stream.send,
            &Frame::Ping { nonce },
            self.stream.runtime.codec_limits,
        )
        .await?;
        match tokio::time::timeout(probe_timeout, self.stream.frames.recv())
            .await
            .map_err(|_| RuntimeError::Protocol("UDP path probe ping timed out"))?
            .ok_or(RuntimeError::TcpPathSessionClosed)??
        {
            Frame::Pong {
                nonce: received_nonce,
            } if received_nonce == nonce => Ok(()),
            Frame::SessionClose { reason } => Err(RuntimeError::RemoteClosed(reason)),
            _ => Err(RuntimeError::Protocol("unexpected UDP path probe frame")),
        }
    }

    pub(super) async fn close_session(&mut self) -> Result<(), RuntimeError> {
        udp_path_write_frame(
            &mut self.stream.send,
            &Frame::SessionClose {
                reason: CloseReason::Normal,
            },
            self.stream.runtime.codec_limits,
        )
        .await?;
        let _ = udp_path_finish_stream(&mut self.stream.send);
        Ok(())
    }

    pub(super) fn delivery_stats(&self) -> PathDeliveryStats {
        self.stats
    }

    pub(super) fn mtu_payload_bytes(&self) -> usize {
        self.mtu_payload_bytes
    }

    pub(super) async fn probe_mtu(&mut self, payload_bytes: usize) -> Result<usize, RuntimeError> {
        if payload_bytes > self.mux_limits.max_payload_bytes {
            return Err(RuntimeError::Datagram(DatagramError::PayloadTooLarge {
                actual: payload_bytes,
                limit: self.mux_limits.max_payload_bytes,
            }));
        }
        self.mtu_payload_bytes = self.mux_limits.max_payload_bytes;
        Ok(self.mtu_payload_bytes)
    }

    fn take_feedback_observation(&mut self) -> Option<UdpDatagramPathObservation> {
        self.last_feedback_observation.take()
    }

    fn handle_datagram_feedback(
        &mut self,
        flow_id: DatagramFlowId,
        ranges: &[OffsetRange],
    ) -> Result<(), RuntimeError> {
        let now = Instant::now();
        let lost = self.expire_unacked_datagrams(now);
        let acked_keys = self
            .sent_datagrams
            .keys()
            .copied()
            .filter(|(pending_flow_id, datagram_id)| {
                *pending_flow_id == flow_id && datagram_id_is_in_ranges(*datagram_id, ranges)
            })
            .collect::<Vec<_>>();

        for key in acked_keys {
            if let Some(sent) = self.sent_datagrams.remove(&key) {
                self.observe_datagram_ack(sent, now, lost);
            }
        }
        Ok(())
    }

    fn observe_remote_path_metrics(&mut self, metrics: crate::protocol::PathMetrics) {
        self.last_feedback_observation = Some(UdpDatagramPathObservation {
            rtt: Duration::from_micros(u64::from(metrics.srtt_us)),
            jitter: Duration::from_micros(u64::from(metrics.jitter_us)),
            loss_rate: (f64::from(metrics.loss_ppm) / 1_000_000.0).clamp(0.0, 1.0),
            rate_sample: PathRateSample::new(
                metrics.delivery_rate_bps.max(8) / 8,
                Duration::from_secs(1),
            ),
        });
    }

    fn expire_unacked_datagrams(&mut self, now: Instant) -> u64 {
        let expired = self
            .sent_datagrams
            .iter()
            .filter_map(|(key, sent)| {
                (now.duration_since(sent.sent_at) >= sent.ttl).then_some(*key)
            })
            .collect::<Vec<_>>();
        let lost = expired.len() as u64;
        for key in expired {
            self.sent_datagrams.remove(&key);
        }
        lost
    }

    fn observe_datagram_ack(&mut self, sent: UdpSentDatagram, now: Instant, lost: u64) {
        let rtt = now
            .duration_since(sent.sent_at)
            .max(MIN_RATE_SAMPLE_DURATION);
        let jitter = self
            .last_datagram_rtt
            .map(|previous| previous.abs_diff(rtt))
            .unwrap_or(Duration::ZERO);
        self.last_datagram_rtt = Some(rtt);
        let delivered = 1_u64;
        let total = delivered.saturating_add(lost).max(1);
        self.last_feedback_observation = Some(UdpDatagramPathObservation {
            rtt,
            jitter,
            loss_rate: lost as f64 / total as f64,
            rate_sample: PathRateSample::new(sent.bytes as u64, rtt),
        });
    }
}

pub(super) fn datagram_ack_range(datagram_id: DatagramId) -> Result<OffsetRange, RuntimeError> {
    let end = datagram_id
        .0
        .checked_add(1)
        .ok_or(RuntimeError::Protocol("datagram ACK range overflow"))?;
    OffsetRange::new(datagram_id.0, end).ok_or(RuntimeError::Protocol("invalid datagram ACK range"))
}

fn datagram_id_is_in_ranges(datagram_id: DatagramId, ranges: &[OffsetRange]) -> bool {
    ranges
        .iter()
        .any(|range| datagram_id.0 >= range.start && datagram_id.0 < range.end)
}
