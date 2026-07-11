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
    let product_deadline = tokio::time::Instant::now() + Duration::from_millis(u64::from(ttl_ms));
    let handshake_timeout = UDP_PATH_HANDSHAKE_TIMEOUT
        .min(product_deadline.saturating_duration_since(tokio::time::Instant::now()));
    if handshake_timeout.is_zero() {
        return Err(RuntimeError::DatagramResponseTimedOut);
    }
    let mut session = UdpDatagramClientSession::open(
        path,
        0,
        security,
        codec_limits,
        mux_limits,
        handshake_timeout,
    )
    .await?;
    if tokio::time::Instant::now() >= product_deadline {
        return Err(RuntimeError::DatagramResponseTimedOut);
    }
    let response = session
        .send_to(
            target,
            payload,
            product_deadline,
            product_deadline,
            default_transport_pto().min(Duration::from_millis(u64::from(ttl_ms))),
        )
        .await
        .map_err(|err| match err {
            DatagramPathSendError::Runtime { source, .. } => source,
            DatagramPathSendError::MtuExceeded { limit } => {
                RuntimeError::Datagram(DatagramError::PayloadTooLarge {
                    actual: payload_len,
                    limit,
                })
            }
            DatagramPathSendError::Timeout { .. } => RuntimeError::DatagramResponseTimedOut,
        })?;
    session.close().await?;
    Ok(response)
}

pub(super) struct DatagramClientAssociation {
    context: ClientPathContext,
    udp: Option<Box<UdpDatagramClientAssociation>>,
    tcp: Option<Box<TcpDatagramClientAssociation>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct DatagramUnderlayCandidate {
    key: RelayPathKey,
    eta_ms: f64,
}

pub(super) enum DatagramUnderlaySendError {
    Timeout {
        path_was_acked: bool,
        product_attempts: usize,
        source: RuntimeError,
    },
    Runtime {
        path_was_acked: bool,
        product_attempts: usize,
        source: RuntimeError,
    },
    PathMtuExceeded {
        product_attempts: usize,
        source: RuntimeError,
    },
}

impl DatagramUnderlaySendError {
    fn into_runtime(self) -> RuntimeError {
        match self {
            Self::Timeout { source, .. }
            | Self::Runtime { source, .. }
            | Self::PathMtuExceeded { source, .. } => source,
        }
    }
}

impl DatagramClientAssociation {
    pub(super) async fn new(context: ClientPathContext) -> Result<Self, RuntimeError> {
        if context.udp_paths.is_empty() && context.tcp_paths.is_empty() {
            return Err(RuntimeError::NoDatagramPath);
        }
        Ok(Self {
            context,
            udp: None,
            tcp: None,
        })
    }

    #[cfg(test)]
    pub(super) fn select_underlay(
        context: &ClientPathContext,
        payload_bytes: usize,
        ttl_ms: u32,
    ) -> Option<UnderlayProtocol> {
        datagram_underlay_candidates(context, payload_bytes, ttl_ms)
            .first()
            .map(|candidate| candidate.key.underlay)
    }

    pub(super) async fn send_to_fresh_datagram_with_route_hint(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        ttl_ms: u32,
        route_hint: Option<RelayPathKey>,
    ) -> Result<Bytes, RuntimeError> {
        let mut last_retryable_error = None;
        let product_deadline =
            tokio::time::Instant::now() + Duration::from_millis(u64::from(ttl_ms));
        let mut remaining_product_attempts = 2usize;
        let mut candidates = datagram_underlay_candidates(&self.context, payload.len(), ttl_ms);
        if let Some(route_hint) = route_hint
            && let Some(position) = candidates
                .iter()
                .position(|candidate| candidate.key == route_hint)
        {
            let hinted = candidates.remove(position);
            candidates.insert(0, hinted);
        }
        let candidate_count = candidates.len();
        for (position, candidate) in candidates.into_iter().enumerate() {
            if remaining_product_attempts == 0 {
                break;
            }
            let remaining_ttl_ms = datagram_remaining_ttl_ms(product_deadline);
            if remaining_ttl_ms == 0 {
                return Err(RuntimeError::DatagramResponseTimedOut);
            }
            if candidate.eta_ms > f64::from(remaining_ttl_ms) {
                continue;
            }
            let has_unattempted_alternative = position + 1 < candidate_count;
            let attempt_limit = if has_unattempted_alternative {
                1
            } else {
                remaining_product_attempts
            };
            match candidate.key.underlay {
                UnderlayProtocol::Tcp => {
                    let result = self
                        .send_to_tcp(
                            target.clone(),
                            payload.clone(),
                            product_deadline,
                            attempt_limit,
                            has_unattempted_alternative,
                        )
                        .await;
                    match result {
                        Ok(response) => return Ok(response),
                        Err(DatagramUnderlaySendError::Timeout {
                            path_was_acked,
                            product_attempts,
                            source,
                        }) => {
                            remaining_product_attempts =
                                remaining_product_attempts.saturating_sub(product_attempts);
                            match datagram_timeout_action(
                                path_was_acked,
                                has_unattempted_alternative && remaining_product_attempts > 0,
                            ) {
                                DatagramTimeoutAction::RetryAlternative => {
                                    last_retryable_error = Some(source);
                                }
                                DatagramTimeoutAction::TerminalProductExpiry => {
                                    return Err(source);
                                }
                            }
                        }
                        Err(DatagramUnderlaySendError::Runtime {
                            path_was_acked,
                            product_attempts,
                            source,
                        }) if !path_was_acked && datagram_underlay_error_is_retryable(&source) => {
                            remaining_product_attempts =
                                remaining_product_attempts.saturating_sub(product_attempts);
                            if !has_unattempted_alternative || remaining_product_attempts == 0 {
                                return Err(source);
                            }
                            last_retryable_error = Some(source);
                        }
                        Err(DatagramUnderlaySendError::PathMtuExceeded {
                            product_attempts,
                            source,
                        }) => {
                            remaining_product_attempts =
                                remaining_product_attempts.saturating_sub(product_attempts);
                            if !has_unattempted_alternative || remaining_product_attempts == 0 {
                                return Err(source);
                            }
                            last_retryable_error = Some(source);
                        }
                        Err(err) => return Err(err.into_runtime()),
                    }
                }
                UnderlayProtocol::Udp => {
                    let result = self
                        .send_to_udp(
                            target.clone(),
                            payload.clone(),
                            product_deadline,
                            attempt_limit,
                            has_unattempted_alternative,
                        )
                        .await;
                    match result {
                        Ok(response) => return Ok(response),
                        Err(DatagramUnderlaySendError::Timeout {
                            path_was_acked,
                            product_attempts,
                            source,
                        }) => {
                            remaining_product_attempts =
                                remaining_product_attempts.saturating_sub(product_attempts);
                            match datagram_timeout_action(
                                path_was_acked,
                                has_unattempted_alternative && remaining_product_attempts > 0,
                            ) {
                                DatagramTimeoutAction::RetryAlternative => {
                                    last_retryable_error = Some(source);
                                }
                                DatagramTimeoutAction::TerminalProductExpiry => {
                                    return Err(source);
                                }
                            }
                        }
                        Err(DatagramUnderlaySendError::Runtime {
                            path_was_acked,
                            product_attempts,
                            source,
                        }) if !path_was_acked && datagram_underlay_error_is_retryable(&source) => {
                            remaining_product_attempts =
                                remaining_product_attempts.saturating_sub(product_attempts);
                            if !has_unattempted_alternative || remaining_product_attempts == 0 {
                                return Err(source);
                            }
                            last_retryable_error = Some(source);
                        }
                        Err(DatagramUnderlaySendError::PathMtuExceeded {
                            product_attempts,
                            source,
                        }) => {
                            remaining_product_attempts =
                                remaining_product_attempts.saturating_sub(product_attempts);
                            if !has_unattempted_alternative || remaining_product_attempts == 0 {
                                return Err(source);
                            }
                            last_retryable_error = Some(source);
                        }
                        Err(err) => return Err(err.into_runtime()),
                    }
                }
            }
        }
        Err(last_retryable_error.unwrap_or(RuntimeError::NoDatagramPath))
    }

    async fn send_to_udp(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        product_deadline: tokio::time::Instant,
        attempt_limit: usize,
        has_unattempted_outer_alternative: bool,
    ) -> Result<Bytes, DatagramUnderlaySendError> {
        if self.udp.is_none() {
            self.udp = Some(Box::new(
                UdpDatagramClientAssociation::new(self.context.clone()).map_err(|source| {
                    DatagramUnderlaySendError::Runtime {
                        path_was_acked: false,
                        product_attempts: 0,
                        source,
                    }
                })?,
            ));
        }
        let udp = self
            .udp
            .as_mut()
            .ok_or(DatagramUnderlaySendError::Runtime {
                path_was_acked: false,
                product_attempts: 0,
                source: RuntimeError::NoSchedulableUdpPath,
            })?;
        udp.send_to(
            target,
            payload,
            product_deadline,
            attempt_limit,
            has_unattempted_outer_alternative,
        )
        .await
    }

    async fn send_to_tcp(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        product_deadline: tokio::time::Instant,
        attempt_limit: usize,
        has_unattempted_outer_alternative: bool,
    ) -> Result<Bytes, DatagramUnderlaySendError> {
        if self.tcp.is_none() {
            self.tcp = Some(Box::new(
                TcpDatagramClientAssociation::open_best(
                    self.context.clone(),
                    payload.len(),
                    product_deadline,
                    has_unattempted_outer_alternative,
                )
                .await
                .map_err(|source| DatagramUnderlaySendError::Runtime {
                    path_was_acked: false,
                    product_attempts: 0,
                    source,
                })?,
            ));
        }
        let remaining_ttl_ms = datagram_remaining_ttl_ms(product_deadline);
        if remaining_ttl_ms == 0 {
            return Err(DatagramUnderlaySendError::Timeout {
                path_was_acked: false,
                product_attempts: 0,
                source: RuntimeError::DatagramResponseTimedOut,
            });
        }
        let (result, session_usable) = {
            let tcp = self
                .tcp
                .as_mut()
                .ok_or(DatagramUnderlaySendError::Runtime {
                    path_was_acked: false,
                    product_attempts: 0,
                    source: RuntimeError::NoSchedulableTcpPath,
                })?;
            let result = tcp
                .send_to_with_carrier_recovery(
                    target,
                    payload,
                    product_deadline,
                    attempt_limit,
                    has_unattempted_outer_alternative,
                )
                .await;
            (result, tcp.session.connection_usable)
        };
        match result {
            Ok((response, reusable)) => {
                if !reusable {
                    self.tcp = None;
                }
                Ok(response)
            }
            Err(err) => {
                if !session_usable {
                    self.tcp = None;
                }
                Err(err)
            }
        }
    }

    pub(super) async fn close(&mut self) -> Result<(), RuntimeError> {
        let udp_result = if let Some(udp) = &mut self.udp {
            udp.close().await
        } else {
            Ok(())
        };
        let tcp_result = if let Some(tcp) = &mut self.tcp {
            tcp.close().await
        } else {
            Ok(())
        };
        udp_result.and(tcp_result)
    }
}

fn datagram_underlay_candidates(
    context: &ClientPathContext,
    payload_bytes: usize,
    ttl_ms: u32,
) -> Vec<DatagramUnderlayCandidate> {
    if ttl_ms == 0 {
        return Vec::new();
    }
    let payload_bytes = payload_bytes.max(PATH_OPEN_SCORE_BYTES);
    let freshness_budget_ms = f64::from(ttl_ms);
    let mut candidates = Vec::new();

    if let Some(path_index) = context
        .ordered_tcp_path_indices(FlowLane::RealtimeDatagram, payload_bytes)
        .first()
        .copied()
    {
        let key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: path_index,
        };
        if let Some(eta_ms) =
            context.reliable_relay_path_eta_ms(key, FlowLane::RealtimeDatagram, payload_bytes)
            && eta_ms <= freshness_budget_ms
        {
            candidates.push(DatagramUnderlayCandidate { key, eta_ms });
        }
    }

    if let Some(candidate) = context
        .ordered_udp_path_candidates_for_ttl(payload_bytes, ttl_ms)
        .first()
        .copied()
    {
        candidates.push(DatagramUnderlayCandidate {
            key: RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: candidate.path_index,
            },
            eta_ms: candidate.eta_ms,
        });
    }

    candidates.sort_by(|left, right| {
        left.eta_ms
            .total_cmp(&right.eta_ms)
            .then_with(|| {
                context
                    .relay_path_config_ordinal(left.key)
                    .cmp(&context.relay_path_config_ordinal(right.key))
            })
            .then_with(|| left.key.index.cmp(&right.key.index))
            .then_with(|| relay_underlay_identity_order(left.key.underlay, right.key.underlay))
    });
    candidates
}

pub(super) fn datagram_underlay_candidate_keys(
    context: &ClientPathContext,
    payload_bytes: usize,
    ttl_ms: u32,
) -> Vec<RelayPathKey> {
    datagram_underlay_candidates(context, payload_bytes, ttl_ms)
        .into_iter()
        .map(|candidate| candidate.key)
        .collect()
}

pub(super) fn datagram_underlay_error_is_retryable(err: &RuntimeError) -> bool {
    if runtime_error_is_datagram_response_timeout(err) {
        return false;
    }
    matches!(
        err,
        RuntimeError::NoTcpPath
            | RuntimeError::NoSchedulableTcpPath
            | RuntimeError::NoSchedulableUdpPath
            | RuntimeError::Io(_)
            | RuntimeError::Tcp(_)
            | RuntimeError::Udp(_)
            | RuntimeError::Encrypted(_)
            | RuntimeError::QuicCarrier(_)
            | RuntimeError::Auth(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
            | RuntimeError::PathOpenTimedOut
            | RuntimeError::PathHeartbeatTimeout
            | RuntimeError::ReliablePathSessionClosed
    )
}

pub(super) fn runtime_error_is_datagram_response_timeout(err: &RuntimeError) -> bool {
    matches!(err, RuntimeError::DatagramResponseTimedOut)
}

pub(super) struct TcpDatagramClientAssociation {
    context: ClientPathContext,
    session: TcpDatagramClientSession,
}

impl TcpDatagramClientAssociation {
    async fn open_best(
        context: ClientPathContext,
        payload_bytes: usize,
        product_deadline: tokio::time::Instant,
        has_unattempted_alternative: bool,
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
        let candidate_count = candidates.len();
        let mut last_retryable_error = None;
        for (position, path_index) in candidates.into_iter().enumerate() {
            let remaining = product_deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let key = RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: path_index,
            };
            let snapshot = context.tcp_path_snapshot(path_index);
            let rtt_is_observed = context.reliable_path_rtt_is_observed(key);
            let eta_ms = context
                .reliable_relay_path_eta_ms(
                    key,
                    FlowLane::RealtimeDatagram,
                    payload_bytes.max(PATH_OPEN_SCORE_BYTES),
                )
                .unwrap_or(f64::INFINITY);
            if eta_ms > remaining.as_secs_f64() * 1000.0 {
                continue;
            }
            let path_budget = tcp_datagram_path_open_timeout(
                snapshot,
                rtt_is_observed,
                has_unattempted_alternative || position + 1 < candidate_count,
                remaining,
            );
            if path_budget.is_zero() {
                break;
            }
            let open_deadline = tokio::time::Instant::now() + path_budget;
            let started_at = Instant::now();
            match TcpDatagramClientSession::open(&context, path_index, open_deadline).await {
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

    async fn send_to_with_carrier_recovery(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        product_deadline: tokio::time::Instant,
        attempt_limit: usize,
        has_unattempted_outer_alternative: bool,
    ) -> Result<(Bytes, bool), DatagramUnderlaySendError> {
        let initial_ttl_ms = datagram_remaining_ttl_ms(product_deadline);
        let retry_deadline = tokio::time::Instant::now()
            + self
                .adaptive_retry_budget(initial_ttl_ms)
                .min(product_deadline.saturating_duration_since(tokio::time::Instant::now()));
        let mut product_attempts = 0usize;
        loop {
            let remaining_ttl_ms = datagram_remaining_ttl_ms(product_deadline);
            if remaining_ttl_ms == 0 || product_attempts >= attempt_limit {
                return Err(DatagramUnderlaySendError::Timeout {
                    path_was_acked: false,
                    product_attempts,
                    source: RuntimeError::DatagramResponseTimedOut,
                });
            }
            let has_tcp_alternative = product_attempts + 1 < attempt_limit
                && self
                    .context
                    .ordered_tcp_path_indices(FlowLane::RealtimeDatagram, payload.len())
                    .into_iter()
                    .any(|path_index| path_index != self.session.path_index);
            let remaining = Duration::from_millis(u64::from(remaining_ttl_ms));
            let attempt_budget = if has_unattempted_outer_alternative || has_tcp_alternative {
                remaining / 2
            } else {
                remaining
            };
            let attempt_deadline = tokio::time::Instant::now() + attempt_budget;
            product_attempts = product_attempts.saturating_add(1);
            match self
                .session
                .send_to(
                    target.clone(),
                    payload.clone(),
                    attempt_deadline,
                    product_deadline,
                )
                .await
            {
                Ok(response) => {
                    let reusable = self.session.connection_usable;
                    if !reusable {
                        let path_index = self.session.path_index;
                        self.context
                            .mark_tcp_path_delivery(path_index, self.session.delivery_stats());
                        self.context
                            .release_tcp_path_load(path_index, FlowLane::RealtimeDatagram);
                        self.context.mark_tcp_path_failure(path_index);
                    }
                    return Ok((response, reusable));
                }
                Err(DatagramPathSendError::Timeout {
                    path_was_acked,
                    response_timeout: _,
                }) => {
                    if path_was_acked {
                        return Err(DatagramUnderlaySendError::Timeout {
                            path_was_acked,
                            product_attempts,
                            source: RuntimeError::DatagramResponseTimedOut,
                        });
                    }
                    let failed_path_index = self.session.path_index;
                    self.session.connection_usable = false;
                    self.context
                        .mark_tcp_path_delivery(failed_path_index, self.session.delivery_stats());
                    self.context
                        .release_tcp_path_load(failed_path_index, FlowLane::RealtimeDatagram);
                    self.context.mark_tcp_path_failure(failed_path_index);
                    if has_unattempted_outer_alternative
                        || product_attempts >= attempt_limit
                        || tokio::time::Instant::now() >= retry_deadline
                    {
                        return Err(DatagramUnderlaySendError::Timeout {
                            path_was_acked,
                            product_attempts,
                            source: RuntimeError::DatagramResponseTimedOut,
                        });
                    }
                    match Self::open_best(
                        self.context.clone(),
                        payload.len(),
                        product_deadline,
                        false,
                    )
                    .await
                    {
                        Ok(replacement) => {
                            self.session = replacement.session;
                        }
                        Err(_) => {
                            return Err(DatagramUnderlaySendError::Timeout {
                                path_was_acked,
                                product_attempts,
                                source: RuntimeError::DatagramResponseTimedOut,
                            });
                        }
                    }
                }
                Err(DatagramPathSendError::Runtime {
                    path_was_acked,
                    source,
                }) if !path_was_acked && tcp_datagram_error_is_path_retryable(&source) => {
                    let failed_path_index = self.session.path_index;
                    self.session.connection_usable = false;
                    self.context
                        .mark_tcp_path_delivery(failed_path_index, self.session.delivery_stats());
                    self.context
                        .release_tcp_path_load(failed_path_index, FlowLane::RealtimeDatagram);
                    self.context.mark_tcp_path_failure(failed_path_index);
                    if has_unattempted_outer_alternative
                        || product_attempts >= attempt_limit
                        || tokio::time::Instant::now() >= retry_deadline
                    {
                        return Err(DatagramUnderlaySendError::Runtime {
                            path_was_acked,
                            product_attempts,
                            source,
                        });
                    }
                    let replacement = Self::open_best(
                        self.context.clone(),
                        payload.len(),
                        product_deadline,
                        false,
                    )
                    .await
                    .map_err(|source| DatagramUnderlaySendError::Runtime {
                        path_was_acked: false,
                        product_attempts,
                        source,
                    })?;
                    self.session = replacement.session;
                }
                Err(DatagramPathSendError::Runtime {
                    path_was_acked,
                    source,
                }) => {
                    let failed_path_index = self.session.path_index;
                    self.session.connection_usable = false;
                    self.context
                        .mark_tcp_path_delivery(failed_path_index, self.session.delivery_stats());
                    self.context
                        .release_tcp_path_load(failed_path_index, FlowLane::RealtimeDatagram);
                    self.context.mark_tcp_path_failure(failed_path_index);
                    return Err(DatagramUnderlaySendError::Runtime {
                        path_was_acked,
                        product_attempts,
                        source,
                    });
                }
                Err(DatagramPathSendError::MtuExceeded { limit }) => {
                    return Err(DatagramUnderlaySendError::Runtime {
                        path_was_acked: false,
                        product_attempts,
                        source: RuntimeError::Datagram(DatagramError::PayloadTooLarge {
                            actual: payload.len(),
                            limit,
                        }),
                    });
                }
            }
        }
    }

    fn adaptive_retry_budget(&self, ttl_ms: u32) -> Duration {
        datagram_response_deadline_budget(self.session.response_timeout(ttl_ms), ttl_ms)
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
    connection_usable: bool,
}

impl TcpDatagramClientSession {
    async fn open(
        context: &ClientPathContext,
        path_index: usize,
        open_deadline: tokio::time::Instant,
    ) -> Result<Self, RuntimeError> {
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
            open_deadline,
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
            connection_usable: true,
        })
    }

    async fn send_to(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        fallback_deadline: tokio::time::Instant,
        product_deadline: tokio::time::Instant,
    ) -> Result<Bytes, DatagramPathSendError> {
        if payload.len() > self.mux_limits.max_payload_bytes {
            return Err(DatagramPathSendError::MtuExceeded {
                limit: self.mux_limits.max_payload_bytes,
            });
        }
        let setup = async {
            tick_client_tcp_path_heartbeat(&mut self.connection, self.mux_limits, true).await?;
            self.ensure_flow(target).await
        };
        let flow_id = match tokio::time::timeout_at(fallback_deadline, setup).await {
            Ok(Ok(flow_id)) => flow_id,
            Ok(Err(err)) => return Err(DatagramPathSendError::runtime(err, false)),
            Err(_) => {
                return Err(DatagramPathSendError::Timeout {
                    path_was_acked: false,
                    response_timeout: Duration::ZERO,
                });
            }
        };
        let ttl_ms = datagram_remaining_ttl_ms(product_deadline);
        if ttl_ms == 0 {
            return Err(DatagramPathSendError::Timeout {
                path_was_acked: false,
                response_timeout: Duration::ZERO,
            });
        }
        let response_timeout = self.response_timeout(ttl_ms);
        let frame = {
            let flow = self
                .flows
                .iter_mut()
                .find(|flow| flow.flow_id == flow_id)
                .ok_or_else(|| {
                    DatagramPathSendError::runtime(
                        RuntimeError::Protocol("missing TCP datagram flow"),
                        false,
                    )
                })?;
            flow.flow.enqueue(0, ttl_ms, payload).map_err(|err| {
                DatagramPathSendError::runtime(RuntimeError::Datagram(err), false)
            })?;
            flow.flow.pop_frame(0).ok_or_else(|| {
                DatagramPathSendError::runtime(
                    RuntimeError::Protocol("datagram expired before TCP send"),
                    false,
                )
            })?
        };
        let (request_datagram_id, request_len) = match &frame {
            Frame::DatagramData {
                datagram_id,
                payload,
                ..
            } => (*datagram_id, payload.len()),
            _ => {
                return Err(DatagramPathSendError::runtime(
                    RuntimeError::Protocol("unexpected queued datagram frame"),
                    false,
                ));
            }
        };
        let request_key = (flow_id, request_datagram_id);
        let sent_at = Instant::now();
        let mut request_acked = false;
        let response_budget = datagram_response_deadline_budget(response_timeout, ttl_ms)
            .max(response_timeout)
            .min(fallback_deadline.saturating_duration_since(tokio::time::Instant::now()));
        let mut response_deadline = sent_at + response_budget;
        let product_response_deadline = product_deadline.into_std();
        self.sent_datagrams.insert(
            request_key,
            UdpSentDatagram {
                sent_at,
                bytes: request_len,
                ttl: Duration::from_millis(u64::from(ttl_ms)),
            },
        );
        let write_request = async {
            self.connection.writer.write_frame(&frame).await?;
            self.connection.writer.flush().await
        };
        match tokio::time::timeout_at(fallback_deadline, write_request).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                return Err(DatagramPathSendError::runtime(
                    RuntimeError::Encrypted(err),
                    false,
                ));
            }
            Err(_) => {
                self.sent_datagrams.remove(&request_key);
                return Err(DatagramPathSendError::Timeout {
                    path_was_acked: false,
                    response_timeout,
                });
            }
        }
        loop {
            let now = Instant::now();
            if now >= response_deadline {
                self.sent_datagrams.remove(&request_key);
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "tcp_datagram_response_timeout",
                    format_args!(
                        "path_id={} path_index={} flow_id={} datagram_id={} response_timeout_ms={} response_budget_ms={} request_acked={}",
                        self.path_id.0,
                        self.path_index,
                        flow_id.0,
                        request_datagram_id.0,
                        response_timeout.as_millis(),
                        response_budget.as_millis(),
                        request_acked,
                    ),
                );
                return Err(DatagramPathSendError::Timeout {
                    path_was_acked: request_acked,
                    response_timeout,
                });
            }
            let wait_for = response_deadline.saturating_duration_since(now);
            let received = match tokio::time::timeout(wait_for, self.connection.frames.recv()).await
            {
                Ok(Some(Ok(frame))) => frame,
                Ok(Some(Err(err))) => {
                    return Err(DatagramPathSendError::runtime(
                        RuntimeError::Encrypted(err),
                        request_acked,
                    ));
                }
                Ok(None) => {
                    return Err(DatagramPathSendError::runtime(
                        RuntimeError::ReliablePathSessionClosed,
                        request_acked,
                    ));
                }
                Err(_) => {
                    self.sent_datagrams.remove(&request_key);
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "tcp_datagram_response_timeout",
                        format_args!(
                            "path_id={} path_index={} flow_id={} datagram_id={} response_timeout_ms={} response_budget_ms={} request_acked={}",
                            self.path_id.0,
                            self.path_index,
                            flow_id.0,
                            request_datagram_id.0,
                            response_timeout.as_millis(),
                            response_budget.as_millis(),
                            request_acked,
                        ),
                    );
                    return Err(DatagramPathSendError::Timeout {
                        path_was_acked: request_acked,
                        response_timeout,
                    });
                }
            };
            refresh_client_tcp_path_liveness(&mut self.connection, self.mux_limits);
            match received {
                Frame::DatagramFeedback { flow_id, received } => {
                    if flow_id == request_key.0
                        && datagram_id_is_in_ranges(request_datagram_id, &received)
                    {
                        request_acked = true;
                        response_deadline = product_response_deadline;
                    }
                    self.handle_datagram_feedback(flow_id, &received)
                        .map_err(|err| DatagramPathSendError::runtime(err, request_acked))?;
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
                    let feedback = Frame::DatagramFeedback {
                        flow_id,
                        received: vec![
                            datagram_ack_range(datagram_id)
                                .map_err(|err| DatagramPathSendError::runtime(err, true))?,
                        ],
                    };
                    let send_feedback = async {
                        self.connection.writer.write_frame(&feedback).await?;
                        self.connection.writer.flush().await
                    };
                    if !matches!(
                        tokio::time::timeout_at(product_deadline, send_feedback).await,
                        Ok(Ok(()))
                    ) {
                        self.connection_usable = false;
                    }
                    self.stats.record_payload_bytes(request_len);
                    self.stats.record_payload_bytes(payload.len());
                    return Ok(payload);
                }
                Frame::DatagramData {
                    flow_id: response_flow_id,
                    datagram_id,
                    ..
                } if response_flow_id == flow_id => {
                    let feedback =
                        Frame::DatagramFeedback {
                            flow_id,
                            received: vec![datagram_ack_range(datagram_id).map_err(|err| {
                                DatagramPathSendError::runtime(err, request_acked)
                            })?],
                        };
                    let send_feedback = async {
                        self.connection.writer.write_frame(&feedback).await?;
                        self.connection.writer.flush().await
                    };
                    let io_deadline = if request_acked {
                        product_deadline
                    } else {
                        fallback_deadline
                    };
                    match tokio::time::timeout_at(io_deadline, send_feedback).await {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            return Err(DatagramPathSendError::runtime(
                                RuntimeError::Encrypted(err),
                                request_acked,
                            ));
                        }
                        Err(_) => {
                            return Err(DatagramPathSendError::Timeout {
                                path_was_acked: request_acked,
                                response_timeout,
                            });
                        }
                    }
                }
                Frame::DatagramClose {
                    flow_id: closed_flow_id,
                } if closed_flow_id == flow_id => {
                    return Err(DatagramPathSendError::runtime(
                        RuntimeError::Protocol("TCP datagram flow closed"),
                        request_acked,
                    ));
                }
                Frame::Ping { nonce } => {
                    let send_pong = async {
                        self.connection
                            .writer
                            .write_frame(&Frame::Pong { nonce })
                            .await?;
                        self.connection.writer.flush().await
                    };
                    let io_deadline = if request_acked {
                        product_deadline
                    } else {
                        fallback_deadline
                    };
                    match tokio::time::timeout_at(io_deadline, send_pong).await {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            return Err(DatagramPathSendError::runtime(
                                RuntimeError::Encrypted(err),
                                request_acked,
                            ));
                        }
                        Err(_) => {
                            return Err(DatagramPathSendError::Timeout {
                                path_was_acked: request_acked,
                                response_timeout,
                            });
                        }
                    }
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
                Frame::PathClose { .. } => {
                    return Err(DatagramPathSendError::runtime(
                        RuntimeError::ReliablePathSessionClosed,
                        request_acked,
                    ));
                }
                Frame::SessionClose { reason } => {
                    return Err(DatagramPathSendError::runtime(
                        RuntimeError::RemoteClosed(reason),
                        request_acked,
                    ));
                }
                _ => {
                    return Err(DatagramPathSendError::runtime(
                        RuntimeError::Protocol("unexpected TCP datagram frame"),
                        request_acked,
                    ));
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
        let rtt = now.duration_since(sent.sent_at).max(QUIC_TIMER_GRANULARITY);
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
    let ttl_budget = ttl;
    let initial_response_pto = transport_pto_from_snapshot(Some(snapshot));
    let srtt = response_srtt.unwrap_or(initial_response_pto);
    let rttvar = response_rttvar.unwrap_or_else(|| {
        Duration::from_secs_f64((snapshot.jitter_ms.max(snapshot.srtt_ms.max(1.0) / 8.0)) / 1000.0)
    });
    let loss_gain = 1.0 + snapshot.loss_rate.clamp(0.0, 1.0);
    (srtt + rttvar.mul_f64(4.0) + QUIC_MAX_ACK_DELAY)
        .mul_f64(loss_gain)
        .max(QUIC_TIMER_GRANULARITY.min(ttl))
        .min(ttl_budget)
}

pub(super) fn datagram_response_deadline_budget(
    response_timeout: Duration,
    ttl_ms: u32,
) -> Duration {
    let ttl_budget = datagram_useful_ttl_budget(ttl_ms);
    if ttl_budget.is_zero() {
        return ttl_budget;
    }
    let response_timeout = response_timeout.max(QUIC_TIMER_GRANULARITY).min(ttl_budget);
    response_timeout
        .saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD)
        .min(ttl_budget)
}

fn datagram_useful_ttl_budget(ttl_ms: u32) -> Duration {
    let ttl = Duration::from_millis(u64::from(ttl_ms));
    if ttl.is_zero() {
        return ttl;
    }
    ttl
}

fn datagram_remaining_ttl_ms(expires_at: tokio::time::Instant) -> u32 {
    let remaining = expires_at.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return 0;
    }
    remaining.as_millis().max(1).min(u128::from(u32::MAX)) as u32
}

pub(super) fn tcp_datagram_path_open_timeout(
    snapshot: Option<PathSnapshot>,
    _rtt_is_observed: bool,
    has_unattempted_alternative: bool,
    remaining_ttl: Duration,
) -> Duration {
    // This helper always opens a new TCP carrier, so a prior probe RTT cannot
    // replace the conservative initial TCP retransmission budget.
    let fresh_carrier_pto = path_open_pto(snapshot, false);
    if has_unattempted_alternative {
        fresh_carrier_pto
            .saturating_mul(active_path_open_serialized_exchanges(snapshot))
            .min(remaining_ttl / 2)
    } else {
        fresh_carrier_pto
            .saturating_mul(active_path_open_pto_multiplier(snapshot))
            .min(remaining_ttl)
    }
}

pub(super) fn tcp_datagram_error_is_path_retryable(err: &RuntimeError) -> bool {
    if runtime_error_is_datagram_response_timeout(err) {
        return false;
    }
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Tcp(_)
            | RuntimeError::Encrypted(_)
            | RuntimeError::Auth(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
            | RuntimeError::PathOpenTimedOut
            | RuntimeError::PathHeartbeatTimeout
            | RuntimeError::ReliablePathSessionClosed
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
    snapshot: PathSnapshot,
}

fn udp_association_candidate_order(
    left: &UdpAssociationCandidateScore,
    right: &UdpAssociationCandidateScore,
) -> std::cmp::Ordering {
    left.completion_ms
        .total_cmp(&right.completion_ms)
        .then_with(|| left.eta_ms.total_cmp(&right.eta_ms))
        .then_with(|| left.opens_new_session.cmp(&right.opens_new_session))
        .then_with(|| left.rank.cmp(&right.rank))
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

pub(super) enum DatagramPathSendError {
    MtuExceeded {
        limit: usize,
    },
    Timeout {
        path_was_acked: bool,
        response_timeout: Duration,
    },
    Runtime {
        path_was_acked: bool,
        source: RuntimeError,
    },
}

impl DatagramPathSendError {
    fn runtime(source: RuntimeError, path_was_acked: bool) -> Self {
        Self::Runtime {
            path_was_acked,
            source,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DatagramTimeoutAction {
    RetryAlternative,
    TerminalProductExpiry,
}

pub(super) fn datagram_timeout_action(
    path_was_acked: bool,
    has_unattempted_alternative: bool,
) -> DatagramTimeoutAction {
    if !path_was_acked && has_unattempted_alternative {
        DatagramTimeoutAction::RetryAlternative
    } else {
        DatagramTimeoutAction::TerminalProductExpiry
    }
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
        product_deadline: tokio::time::Instant,
        attempt_limit: usize,
        has_unattempted_outer_alternative: bool,
    ) -> Result<Bytes, DatagramUnderlaySendError> {
        if payload.len() > self.context.mux_limits.max_payload_bytes {
            return Err(DatagramUnderlaySendError::Runtime {
                path_was_acked: false,
                product_attempts: 0,
                source: RuntimeError::Datagram(DatagramError::PayloadTooLarge {
                    actual: payload.len(),
                    limit: self.context.mux_limits.max_payload_bytes,
                }),
            });
        }
        let ttl_ms = datagram_remaining_ttl_ms(product_deadline);
        let candidates = self
            .context
            .ordered_udp_path_candidates_for_ttl(payload.len(), ttl_ms);
        if candidates.is_empty() {
            #[cfg(feature = "lab-diagnostics")]
            {
                let now = Instant::now();
                let observations = self
                    .context
                    .health
                    .lock()
                    .expect("client path health lock")
                    .udp
                    .iter_mut()
                    .enumerate()
                    .map(|(index, record)| {
                        let observation = record.observe(now);
                        format!(
                            "{}:{:?}:srtt={:?}:rate={:?}:carrier_rate={:?}:flows={}:failed={:?}",
                            index,
                            observation.state,
                            observation.measured_srtt_ms,
                            observation.measured_rate_bps,
                            observation.carrier_delivery_rate_bps,
                            observation.active_flows,
                            record.failed_until.map(|deadline| deadline
                                .saturating_duration_since(now)
                                .as_millis())
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                lab_diagnostic(
                    "udp_datagram_no_candidates",
                    format_args!(
                        "udp_paths={} payload_bytes={} ttl_ms={} observations={}",
                        self.context.udp_paths.len(),
                        payload.len(),
                        ttl_ms,
                        observations
                    ),
                );
            }
            return Err(DatagramUnderlaySendError::Runtime {
                path_was_acked: false,
                product_attempts: 0,
                source: RuntimeError::NoSchedulableUdpPath,
            });
        }

        self.prune_suppressed_paths();
        let mut attempted = HashSet::new();
        let mut product_attempts = 0usize;
        let mut last_retryable_error = None;
        loop {
            let remaining_ttl_ms = datagram_remaining_ttl_ms(product_deadline);
            if remaining_ttl_ms == 0 || product_attempts >= attempt_limit {
                return Err(DatagramUnderlaySendError::Timeout {
                    path_was_acked: false,
                    product_attempts,
                    source: RuntimeError::DatagramResponseTimedOut,
                });
            }
            let Some(path_index) = self.select_path_candidate(
                &candidates,
                &attempted,
                payload.len(),
                remaining_ttl_ms,
            ) else {
                break;
            };
            attempted.insert(path_index);
            product_attempts = product_attempts.saturating_add(1);
            let has_unattempted_internal_alternative = product_attempts < attempt_limit
                && candidates
                    .iter()
                    .any(|candidate| !attempted.contains(&candidate.path_index));
            let has_unattempted_alternative =
                has_unattempted_outer_alternative || has_unattempted_internal_alternative;
            let remaining = Duration::from_millis(u64::from(remaining_ttl_ms));
            let fallback_deadline = if has_unattempted_alternative {
                tokio::time::Instant::now() + remaining / 2
            } else {
                product_deadline
            };
            match self
                .send_to_path(
                    path_index,
                    target.clone(),
                    payload.clone(),
                    fallback_deadline,
                    product_deadline,
                    has_unattempted_alternative,
                )
                .await
            {
                Ok(response) => {
                    self.last_successful_path = Some(path_index);
                    return Ok(response);
                }
                Err(DatagramPathSendError::MtuExceeded { limit }) => {
                    let source = RuntimeError::Datagram(DatagramError::PayloadTooLarge {
                        actual: payload.len(),
                        limit,
                    });
                    product_attempts = product_attempts.saturating_sub(1);
                    if !has_unattempted_internal_alternative {
                        return Err(DatagramUnderlaySendError::PathMtuExceeded {
                            product_attempts,
                            source,
                        });
                    }
                    last_retryable_error = Some(source);
                }
                Err(DatagramPathSendError::Timeout {
                    path_was_acked,
                    response_timeout,
                }) => {
                    match datagram_timeout_action(
                        path_was_acked,
                        has_unattempted_internal_alternative,
                    ) {
                        DatagramTimeoutAction::RetryAlternative => {
                            self.remove_path(path_index);
                            self.suppress_path_after_timeout(
                                path_index,
                                response_timeout,
                                remaining_ttl_ms,
                            );
                            self.context.mark_udp_path_failure(path_index);
                            last_retryable_error = Some(RuntimeError::DatagramResponseTimedOut);
                            #[cfg(feature = "lab-diagnostics")]
                            lab_diagnostic(
                                "udp_datagram_unacked_timeout_retry_alternative",
                                format_args!(
                                    "path_index={} response_timeout_ms={} ttl_ms={}",
                                    path_index,
                                    response_timeout.as_millis(),
                                    remaining_ttl_ms
                                ),
                            );
                            continue;
                        }
                        DatagramTimeoutAction::TerminalProductExpiry => {
                            if path_was_acked {
                                self.context.mark_udp_path_feedback(
                                    path_index,
                                    UdpDatagramPathObservation {
                                        rtt: response_timeout,
                                        jitter: Duration::ZERO,
                                        loss_rate: 1.0,
                                        rate_sample: None,
                                    },
                                );
                            } else {
                                self.remove_path(path_index);
                                self.suppress_path_after_timeout(
                                    path_index,
                                    response_timeout,
                                    remaining_ttl_ms,
                                );
                                self.context.mark_udp_path_failure(path_index);
                            }
                        }
                    }
                    return Err(DatagramUnderlaySendError::Timeout {
                        path_was_acked,
                        product_attempts,
                        source: RuntimeError::DatagramResponseTimedOut,
                    });
                }
                Err(DatagramPathSendError::Runtime {
                    path_was_acked,
                    source,
                }) if udp_datagram_error_is_path_retryable(&source) => {
                    self.remove_path(path_index);
                    self.suppress_path_after_timeout(
                        path_index,
                        default_transport_pto(),
                        remaining_ttl_ms,
                    );
                    self.context.mark_udp_path_failure(path_index);
                    if path_was_acked || !has_unattempted_internal_alternative {
                        return Err(DatagramUnderlaySendError::Runtime {
                            path_was_acked,
                            product_attempts,
                            source,
                        });
                    }
                    last_retryable_error = Some(source);
                }
                Err(DatagramPathSendError::Runtime {
                    path_was_acked,
                    source,
                }) => {
                    return Err(DatagramUnderlaySendError::Runtime {
                        path_was_acked,
                        product_attempts,
                        source,
                    });
                }
            }
        }
        Err(DatagramUnderlaySendError::Runtime {
            path_was_acked: false,
            product_attempts,
            source: last_retryable_error.unwrap_or(RuntimeError::NoSchedulableUdpPath),
        })
    }

    fn path_session_is_open(&self, path_index: usize) -> bool {
        self.paths
            .iter()
            .any(|path| path.session.path_index == path_index)
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
        let freshness_budget_ms = f64::from(ttl_ms);
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
                let snapshot = self.context.udp_path_snapshot(candidate.path_index)?;
                let ready_at = open_ready_at.unwrap_or(now);
                let ready_delay_ms = ready_at.saturating_duration_since(now).as_secs_f64() * 1000.0;
                let completion_ms = eta_ms + ready_delay_ms;
                (completion_ms <= freshness_budget_ms).then_some(UdpAssociationCandidateScore {
                    path_index: candidate.path_index,
                    completion_ms,
                    eta_ms,
                    opens_new_session: !has_open_session,
                    rank,
                    snapshot,
                })
            })
            .collect::<Vec<_>>();
        let evidenced_paths = viable
            .iter()
            .filter_map(|candidate| {
                self.path_has_datagram_feedback_or_hint(candidate.path_index)
                    .then_some(candidate.path_index)
            })
            .collect::<Vec<_>>();
        if evidenced_paths
            .iter()
            .any(|path_index| !self.path_is_temporarily_suppressed(*path_index, now))
        {
            viable.retain(|candidate| evidenced_paths.contains(&candidate.path_index));
        }
        let best_candidate = viable
            .iter()
            .min_by(|left, right| udp_association_candidate_order(left, right))
            .copied();
        if let Some(path_index) = self.last_successful_path
            && let Some(best) = best_candidate
            && let Some(candidate) = viable.iter().find(|candidate| {
                candidate.path_index == path_index
                    && !self.path_is_temporarily_suppressed(candidate.path_index, now)
            })
            && path_within_adaptive_lead_hysteresis(
                candidate.completion_ms,
                candidate.snapshot,
                best.completion_ms,
                best.snapshot,
                payload_bytes,
            )
        {
            return Some(candidate.path_index);
        }
        if evidenced_paths.is_empty()
            && self.context.udp_paths.iter().all(path_is_endpoint_only)
            && let Some(candidate) = viable
                .iter()
                .filter(|candidate| !self.path_is_temporarily_suppressed(candidate.path_index, now))
                .min_by(|left, right| left.path_index.cmp(&right.path_index))
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
            .min_by(udp_association_candidate_order)
            .map(|candidate| candidate.path_index)
    }

    pub(super) fn suppress_path_after_timeout(
        &mut self,
        path_index: usize,
        response_timeout: Duration,
        ttl_ms: u32,
    ) {
        let ttl = Duration::from_millis(u64::from(ttl_ms));
        let duration = response_timeout
            .max(QUIC_TIMER_GRANULARITY)
            .saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD)
            .min(ttl);
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
        fallback_deadline: tokio::time::Instant,
        product_deadline: tokio::time::Instant,
        has_unattempted_alternative: bool,
    ) -> Result<Bytes, DatagramPathSendError> {
        let ttl_ms = datagram_remaining_ttl_ms(product_deadline);
        if ttl_ms == 0 {
            return Err(DatagramPathSendError::Timeout {
                path_was_acked: false,
                response_timeout: Duration::ZERO,
            });
        }
        let model = self
            .context
            .udp_path_runtime_model(path_index, ttl_ms)
            .ok_or_else(|| {
                DatagramPathSendError::runtime(RuntimeError::NoSchedulableUdpPath, false)
            })?;
        if !model.accepts_or_can_probe(payload.len()) {
            return Err(DatagramPathSendError::MtuExceeded {
                limit: model.mtu_payload_bytes,
            });
        }
        let path_session_was_open = self.path_session_is_open(path_index);
        let association_had_open_path = !self.paths.is_empty();
        let handshake_timeout = udp_datagram_path_open_timeout(
            association_had_open_path,
            has_unattempted_alternative,
            model,
            ttl_ms,
        )
        .min(fallback_deadline.saturating_duration_since(tokio::time::Instant::now()));
        let response_timeout = udp_datagram_first_response_timeout(
            path_session_was_open,
            association_had_open_path,
            has_unattempted_alternative,
            model,
            ttl_ms,
        );
        let position = self
            .ensure_path_session(path_index, handshake_timeout)
            .await
            .map_err(|err| DatagramPathSendError::runtime(err, false))?;
        let current_mtu = self
            .paths
            .get(position)
            .ok_or_else(|| {
                DatagramPathSendError::runtime(RuntimeError::NoSchedulableUdpPath, false)
            })?
            .session
            .mtu_payload_bytes();
        if payload.len() > current_mtu {
            let probe_result = {
                let path = self.paths.get_mut(position).ok_or_else(|| {
                    DatagramPathSendError::runtime(RuntimeError::NoSchedulableUdpPath, false)
                })?;
                tokio::time::timeout_at(fallback_deadline, path.session.probe_mtu(payload.len()))
                    .await
            };
            match probe_result {
                Ok(Ok(probed_mtu)) => {
                    self.context.mark_udp_path_mtu(path_index, probed_mtu);
                }
                Ok(Err(err)) if udp_datagram_error_is_path_retryable(&err) => {
                    self.context.mark_udp_path_mtu(path_index, current_mtu);
                    return Err(DatagramPathSendError::MtuExceeded { limit: current_mtu });
                }
                Ok(Err(err)) => return Err(DatagramPathSendError::runtime(err, false)),
                Err(_) => {
                    self.context.mark_udp_path_mtu(path_index, current_mtu);
                    return Err(DatagramPathSendError::MtuExceeded { limit: current_mtu });
                }
            }
        }
        let (observation_path_index, observation, result, connection_usable) = {
            let path = self.paths.get_mut(position).ok_or_else(|| {
                DatagramPathSendError::runtime(RuntimeError::NoSchedulableUdpPath, false)
            })?;
            if tokio::time::timeout_at(
                fallback_deadline,
                path.pacer.wait_for_send(model, payload.len()),
            )
            .await
            .is_err()
            {
                return Err(DatagramPathSendError::Timeout {
                    path_was_acked: false,
                    response_timeout,
                });
            }
            let result = path
                .session
                .send_to(
                    target,
                    payload,
                    fallback_deadline,
                    product_deadline,
                    response_timeout,
                )
                .await;
            let observation = path.session.take_feedback_observation();
            (
                path.session.path_index,
                observation,
                result,
                path.session.connection_usable,
            )
        };
        if let Some(observation) = observation {
            self.context
                .mark_udp_path_feedback(observation_path_index, observation);
        }

        match result {
            Ok(response) => {
                if !connection_usable {
                    self.remove_path(path_index);
                    self.context.mark_udp_path_failure(path_index);
                }
                Ok(response)
            }
            Err(DatagramPathSendError::Timeout {
                path_was_acked,
                response_timeout,
            }) => Err(DatagramPathSendError::Timeout {
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

    fn remove_path(&mut self, path_index: usize) {
        let Some(position) = self
            .paths
            .iter()
            .position(|path| path.session.path_index == path_index)
        else {
            return;
        };
        let path = self.paths.swap_remove(position);
        self.context
            .mark_udp_path_delivery(path.session.path_index, path.session.delivery_stats());
        self.context.release_udp_path_load(path.session.path_index);
    }
}

pub(super) fn udp_datagram_error_is_path_retryable(err: &RuntimeError) -> bool {
    if runtime_error_is_datagram_response_timeout(err) {
        return false;
    }
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Udp(_)
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
        model.response_timeout
    };
    response_timeout
        .max(QUIC_TIMER_GRANULARITY)
        .min(UDP_PATH_HANDSHAKE_TIMEOUT)
        .min(ttl_timeout)
}

pub(super) fn udp_datagram_first_response_timeout(
    path_session_was_open: bool,
    association_had_open_path: bool,
    has_unattempted_alternative: bool,
    model: UdpPathRuntimeModel,
    ttl_ms: u32,
) -> Duration {
    if path_session_was_open || association_had_open_path {
        return model.response_timeout;
    }
    udp_datagram_path_open_timeout(false, has_unattempted_alternative, model, ttl_ms)
        .max(model.response_timeout)
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
    connection_usable: bool,
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
            stream_frame_queue: reliable_stream_frame_queue(mux_limits),
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
            .map_err(|_| {
                RuntimeError::Protocol("QUIC UDP path datagram stream open timed out")
            })??;
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
            connection_usable: true,
        })
    }

    pub(super) async fn send_to(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        fallback_deadline: tokio::time::Instant,
        product_deadline: tokio::time::Instant,
        response_timeout: Duration,
    ) -> Result<Bytes, DatagramPathSendError> {
        let flow_id =
            match tokio::time::timeout_at(fallback_deadline, self.ensure_flow(target)).await {
                Ok(Ok(flow_id)) => flow_id,
                Ok(Err(err)) => return Err(DatagramPathSendError::runtime(err, false)),
                Err(_) => {
                    return Err(DatagramPathSendError::Timeout {
                        path_was_acked: false,
                        response_timeout,
                    });
                }
            };
        let ttl_ms = datagram_remaining_ttl_ms(product_deadline);
        if ttl_ms == 0 {
            return Err(DatagramPathSendError::Timeout {
                path_was_acked: false,
                response_timeout,
            });
        }
        let frame = {
            let flow = self
                .flows
                .iter_mut()
                .find(|flow| flow.flow_id == flow_id)
                .ok_or_else(|| {
                    DatagramPathSendError::runtime(
                        RuntimeError::Protocol("missing UDP datagram flow"),
                        false,
                    )
                })?;
            flow.flow.enqueue(0, ttl_ms, payload).map_err(|err| {
                DatagramPathSendError::runtime(RuntimeError::Datagram(err), false)
            })?;
            flow.flow.pop_frame(0).ok_or_else(|| {
                DatagramPathSendError::runtime(
                    RuntimeError::Protocol("datagram expired before send"),
                    false,
                )
            })?
        };
        let (request_datagram_id, request_len) = match &frame {
            Frame::DatagramData {
                datagram_id,
                payload,
                ..
            } => (*datagram_id, payload.len()),
            _ => {
                return Err(DatagramPathSendError::runtime(
                    RuntimeError::Protocol("unexpected queued datagram frame"),
                    false,
                ));
            }
        };
        let request_key = (flow_id, request_datagram_id);
        self.last_feedback_observation = None;
        let request_started_at = Instant::now();
        let mut request_acked = false;
        let response_budget = datagram_response_deadline_budget(response_timeout, ttl_ms)
            .min(fallback_deadline.saturating_duration_since(tokio::time::Instant::now()));
        let mut response_deadline = request_started_at + response_budget;
        let product_response_deadline = product_deadline.into_std();

        self.sent_datagrams.insert(
            request_key,
            UdpSentDatagram {
                sent_at: request_started_at,
                bytes: request_len,
                ttl: Duration::from_millis(u64::from(ttl_ms)),
            },
        );
        match tokio::time::timeout_at(
            fallback_deadline,
            udp_path_write_frame(
                &mut self.stream.send,
                &frame,
                self.stream.runtime.codec_limits,
            ),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(DatagramPathSendError::runtime(err, false)),
            Err(_) => {
                self.sent_datagrams.remove(&request_key);
                return Err(DatagramPathSendError::Timeout {
                    path_was_acked: false,
                    response_timeout,
                });
            }
        }

        loop {
            let now = Instant::now();
            if now >= response_deadline {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "udp_datagram_response_timeout",
                    format_args!(
                        "path_id={} path_index={} flow_id={} datagram_id={} response_timeout_ms={} response_budget_ms={} request_acked={}",
                        self.path_id.0,
                        self.path_index,
                        flow_id.0,
                        request_datagram_id.0,
                        response_timeout.as_millis(),
                        response_budget.as_millis(),
                        request_acked,
                    ),
                );
                self.sent_datagrams.remove(&request_key);
                return Err(DatagramPathSendError::Timeout {
                    path_was_acked: request_acked,
                    response_timeout,
                });
            }
            let wait_for = response_deadline.saturating_duration_since(now);
            let received = match tokio::time::timeout(wait_for, self.stream.frames.recv()).await {
                Ok(Some(Ok(frame))) => frame,
                Ok(Some(Err(err))) => {
                    return Err(DatagramPathSendError::runtime(err, request_acked));
                }
                Ok(None) => {
                    return Err(DatagramPathSendError::runtime(
                        RuntimeError::ReliablePathSessionClosed,
                        request_acked,
                    ));
                }
                Err(_) => {
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "udp_datagram_response_timeout",
                        format_args!(
                            "path_id={} path_index={} flow_id={} datagram_id={} response_timeout_ms={} response_budget_ms={} request_acked={}",
                            self.path_id.0,
                            self.path_index,
                            flow_id.0,
                            request_datagram_id.0,
                            response_timeout.as_millis(),
                            response_budget.as_millis(),
                            request_acked,
                        ),
                    );
                    self.sent_datagrams.remove(&request_key);
                    return Err(DatagramPathSendError::Timeout {
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
                        response_deadline = product_response_deadline;
                    }
                    self.handle_datagram_feedback(flow_id, &received)
                        .map_err(|err| DatagramPathSendError::runtime(err, request_acked))?;
                }
                Frame::DatagramData {
                    flow_id: response_flow_id,
                    datagram_id,
                    payload,
                    ..
                } if response_flow_id == flow_id && datagram_id == request_datagram_id => {
                    let request_ack = datagram_ack_range(request_datagram_id)
                        .map_err(|err| DatagramPathSendError::runtime(err, true))?;
                    self.handle_datagram_feedback(flow_id, &[request_ack])
                        .map_err(|err| DatagramPathSendError::runtime(err, true))?;
                    let feedback = Frame::DatagramFeedback {
                        flow_id,
                        received: vec![
                            datagram_ack_range(datagram_id)
                                .map_err(|err| DatagramPathSendError::runtime(err, true))?,
                        ],
                    };
                    if !matches!(
                        tokio::time::timeout_at(
                            product_deadline,
                            udp_path_write_frame(
                                &mut self.stream.send,
                                &feedback,
                                self.stream.runtime.codec_limits,
                            ),
                        )
                        .await,
                        Ok(Ok(()))
                    ) {
                        self.connection_usable = false;
                    }
                    self.stats.record_payload_bytes(request_len);
                    self.stats.record_payload_bytes(payload.len());
                    return Ok(payload);
                }
                Frame::DatagramData {
                    flow_id: response_flow_id,
                    datagram_id,
                    ..
                } if response_flow_id == flow_id => {
                    let feedback =
                        Frame::DatagramFeedback {
                            flow_id,
                            received: vec![datagram_ack_range(datagram_id).map_err(|err| {
                                DatagramPathSendError::runtime(err, request_acked)
                            })?],
                        };
                    let io_deadline = if request_acked {
                        product_deadline
                    } else {
                        fallback_deadline
                    };
                    match tokio::time::timeout_at(
                        io_deadline,
                        udp_path_write_frame(
                            &mut self.stream.send,
                            &feedback,
                            self.stream.runtime.codec_limits,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            return Err(DatagramPathSendError::runtime(err, request_acked));
                        }
                        Err(_) => {
                            return Err(DatagramPathSendError::Timeout {
                                path_was_acked: request_acked,
                                response_timeout,
                            });
                        }
                    }
                }
                Frame::PathMetrics { metrics } if metrics.path_id == self.path_id => {
                    self.observe_remote_path_metrics(metrics);
                }
                Frame::SessionReady => {}
                Frame::RxRateHint { path_id, .. } if path_id == self.path_id => {}
                Frame::DatagramClose {
                    flow_id: closed_flow_id,
                } if closed_flow_id == flow_id => {
                    return Err(DatagramPathSendError::runtime(
                        RuntimeError::Protocol("datagram flow closed"),
                        request_acked,
                    ));
                }
                Frame::SessionClose { reason } => {
                    return Err(DatagramPathSendError::runtime(
                        RuntimeError::RemoteClosed(reason),
                        request_acked,
                    ));
                }
                _ => {
                    return Err(DatagramPathSendError::runtime(
                        RuntimeError::Protocol("unexpected UDP datagram frame"),
                        request_acked,
                    ));
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
            .ok_or(RuntimeError::ReliablePathSessionClosed)??
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
            loss_rate: metrics
                .loss_observed
                .then(|| (f64::from(metrics.loss_ppm) / 1_000_000.0).clamp(0.0, 1.0))
                .unwrap_or(0.0),
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
        let rtt = now.duration_since(sent.sent_at).max(QUIC_TIMER_GRANULARITY);
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
