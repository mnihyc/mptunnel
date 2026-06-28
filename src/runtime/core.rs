use super::datagram::*;
use super::error::RuntimeError;
use super::ingress_runtime::*;
use super::prelude::*;
use super::relay_control::*;
use super::relay_open::*;
use super::server_runtime::*;
use super::tcp_path::*;
use super::tun_l4::*;
use super::udp_path::*;

pub(super) const MAX_HTTP_CONNECT_HEADER_BYTES: usize = 64 * 1024;
pub(super) const PATH_OPEN_SCORE_BYTES: usize = 4 * 1024;
pub(super) const UDP_PATH_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);
pub(super) const PATH_FAILURE_COOLDOWN: Duration = Duration::from_secs(5);
pub(super) const TCP_STREAM_LOAD_BYTES: u64 = 256 * 1024;
pub(super) const UDP_SESSION_LOAD_BYTES: u64 = 64 * 1024;
pub(super) const MIN_RATE_SAMPLE_BYTES: u64 = PATH_OPEN_SCORE_BYTES as u64;
pub(super) const MIN_RATE_SAMPLE_DURATION: Duration = Duration::from_millis(1);
pub(super) const TCP_STREAM_STALL_MIN_TIMEOUT: Duration = Duration::from_millis(350);
pub(super) const TCP_STREAM_STALL_MAX_TIMEOUT: Duration = Duration::from_millis(1500);
pub(super) const UDP_DATAGRAM_MIN_TTL_FIT_RATIO: f64 = 0.9;
pub(super) const UDP_BBR_PACING_GAIN: f64 = 1.25;
pub(super) const UDP_FIRST_OPEN_RTT_MULTIPLIER: f64 = 8.0;
pub(super) const UDP_MIN_PACING_RATE_BPS: f64 = 64_000.0;
pub(super) const UDP_MAX_RESPONSE_TIMEOUT: Duration = Duration::from_secs(1);
pub(super) const UDP_MIN_RESPONSE_TIMEOUT: Duration = Duration::from_millis(50);
pub(super) const UDP_MIN_RETRY_BUDGET: Duration = Duration::from_millis(250);
pub(super) const UDP_MAX_RETRY_BUDGET: Duration = Duration::from_millis(500);
pub(super) const UDP_MIN_PATH_SUPPRESSION: Duration = Duration::from_millis(250);
pub(super) const UDP_DEFAULT_MTU_PAYLOAD_BYTES: usize = 1200;
pub(super) const UDP_MIN_MTU_PAYLOAD_BYTES: usize = 512;
pub(super) const UDP_MAX_MTU_PAYLOAD_BYTES: usize = 65_000;
pub(super) const TUN_UDP_FLOW_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

pub(super) fn current_unix_secs() -> Result<u64, RuntimeError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RuntimeError::Protocol("system clock is before UNIX epoch"))?
        .as_secs())
}

pub async fn run(config: AppConfig) -> Result<(), RuntimeError> {
    match config.command {
        CommandConfig::Client(client) => {
            run_client(client, config.security, config.resources).await
        }
        CommandConfig::Server(server) => {
            run_server(
                server.bind_paths,
                server.outbound,
                server.outbound_dns,
                config.security,
                config.resources,
            )
            .await
        }
    }
}

pub(super) async fn run_client(
    client: ClientConfig,
    security: SecurityConfig,
    resources: ResourceLimits,
) -> Result<(), RuntimeError> {
    let path_probe_interval = client.path_probe_interval;
    let path_probe_timeout = client.path_probe_timeout;
    let context = ClientPathContext::new_with_proxy_auth(
        client.paths,
        security,
        resources,
        client.proxy_auth,
    )?;
    start_client_path_probes(context.clone(), path_probe_interval, path_probe_timeout);
    let mut ingresses = tokio::task::JoinSet::new();
    for ingress in client.ingresses {
        let context = context.clone();
        match ingress {
            IngressConfig::Socks5 { listen } => {
                ingresses.spawn(async move { run_socks5_client_ingress(listen, context).await });
            }
            IngressConfig::HttpConnect { listen } => {
                ingresses
                    .spawn(async move { run_http_connect_client_ingress(listen, context).await });
            }
            IngressConfig::TunL4(tun) => {
                ingresses.spawn(async move { run_tun_l4_client(tun, context).await });
            }
        }
    }
    if let Some(result) = ingresses.join_next().await {
        match result {
            Ok(Ok(())) => Err(RuntimeError::Protocol("client ingress exited")),
            Ok(Err(err)) => Err(err),
            Err(err) => Err(RuntimeError::TaskJoin(err)),
        }
    } else {
        Err(RuntimeError::Protocol("client has no ingress tasks"))
    }
}

pub(super) fn start_client_path_probes(
    context: ClientPathContext,
    interval: Duration,
    timeout: Duration,
) {
    tokio::spawn(async move {
        run_client_path_probes(context, interval, timeout).await;
    });
}

pub(super) async fn run_client_path_probes(
    context: ClientPathContext,
    interval: Duration,
    timeout: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        probe_client_paths(&context, timeout).await;
    }
}

pub(super) async fn probe_client_paths(context: &ClientPathContext, timeout: Duration) {
    let mut probes = tokio::task::JoinSet::new();
    for path_index in 0..context.tcp_paths.len() {
        if !context.should_probe_tcp_path(path_index) {
            continue;
        }
        let context = context.clone();
        probes.spawn(async move {
            (
                UnderlayProtocol::Tcp,
                path_index,
                probe_tcp_client_path(&context, path_index, timeout).await,
            )
        });
    }
    for path_index in 0..context.udp_paths.len() {
        if !context.should_probe_udp_path(path_index) {
            continue;
        }
        let context = context.clone();
        probes.spawn(async move {
            (
                UnderlayProtocol::Udp,
                path_index,
                probe_udp_client_path(&context, path_index, timeout).await,
            )
        });
    }

    while let Some(result) = probes.join_next().await {
        match result {
            Ok((UnderlayProtocol::Tcp, path_index, Ok(elapsed))) => {
                context.mark_tcp_path_probe_success(path_index, elapsed);
            }
            Ok((UnderlayProtocol::Tcp, path_index, Err(_))) => {
                context.mark_tcp_path_failure(path_index);
            }
            Ok((UnderlayProtocol::Udp, path_index, Ok(elapsed))) => {
                context.mark_udp_path_probe_success(path_index, elapsed);
            }
            Ok((UnderlayProtocol::Udp, path_index, Err(_))) => {
                context.mark_udp_path_failure(path_index);
            }
            Err(err) => {
                eprintln!("warning: path probe task failed: {err}");
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClientPathContext {
    pub(super) tcp_paths: Arc<Vec<PathSpec>>,
    pub(super) udp_paths: Arc<Vec<PathSpec>>,
    pub(super) tcp_sessions: Arc<Vec<ClientTcpPathSessionHandle>>,
    pub(super) udp_sessions: Arc<Vec<ClientUdpPathSessionHandle>>,
    pub(super) next_tcp_stream_id: Arc<Mutex<u64>>,
    pub(super) health: Arc<Mutex<ClientPathHealth>>,
    pub(super) codec_limits: CodecLimits,
    pub(super) mux_limits: MuxLimits,
    pub(super) security: SecurityConfig,
    pub(super) proxy_auth: ProxyAuthConfig,
}

#[derive(Debug)]
pub(super) struct ClientPathHealth {
    pub(super) tcp: Vec<ClientPathHealthRecord>,
    pub(super) udp: Vec<ClientPathHealthRecord>,
}

#[derive(Debug, Clone)]
pub(super) struct ClientPathHealthRecord {
    pub(super) state: SchedulerPathState,
    pub(super) consecutive_failures: u32,
    pub(super) measured_srtt_ms: Option<f64>,
    pub(super) measured_jitter_ms: Option<f64>,
    pub(super) measured_rate_bps: Option<f64>,
    pub(super) measured_loss_rate: Option<f64>,
    pub(super) measured_mtu_payload_bytes: Option<usize>,
    pub(super) failed_until: Option<Instant>,
    pub(super) active_flows: u32,
    pub(super) active_latency_sensitive_flows: u32,
    pub(super) load_bytes: u64,
}

impl Default for ClientPathHealthRecord {
    fn default() -> Self {
        Self {
            state: SchedulerPathState::Active,
            consecutive_failures: 0,
            measured_srtt_ms: None,
            measured_jitter_ms: None,
            measured_rate_bps: None,
            measured_loss_rate: None,
            measured_mtu_payload_bytes: None,
            failed_until: None,
            active_flows: 0,
            active_latency_sensitive_flows: 0,
            load_bytes: 0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ClientPathObservation {
    pub(super) state: SchedulerPathState,
    pub(super) measured_srtt_ms: Option<f64>,
    pub(super) measured_jitter_ms: Option<f64>,
    pub(super) measured_rate_bps: Option<f64>,
    pub(super) measured_loss_rate: Option<f64>,
    pub(super) measured_mtu_payload_bytes: Option<usize>,
    pub(super) active_flows: u32,
    pub(super) active_latency_sensitive_flows: u32,
    pub(super) load_bytes: u64,
}

impl ClientPathHealthRecord {
    pub(super) fn observe(&mut self, now: Instant) -> ClientPathObservation {
        if self.state == SchedulerPathState::Failed
            && self.failed_until.is_some_and(|deadline| now >= deadline)
        {
            self.state = SchedulerPathState::Suspect;
            self.failed_until = None;
        }
        ClientPathObservation {
            state: self.state,
            measured_srtt_ms: self.measured_srtt_ms,
            measured_jitter_ms: self.measured_jitter_ms,
            measured_rate_bps: self.measured_rate_bps,
            measured_loss_rate: self.measured_loss_rate,
            measured_mtu_payload_bytes: self.measured_mtu_payload_bytes,
            active_flows: self.active_flows,
            active_latency_sensitive_flows: self.active_latency_sensitive_flows,
            load_bytes: self.load_bytes,
        }
    }

    pub(super) fn mark_success(&mut self, elapsed: Duration) {
        self.state = SchedulerPathState::Active;
        self.consecutive_failures = 0;
        self.failed_until = None;
        let sample_ms = elapsed.as_secs_f64() * 1000.0;
        self.measured_srtt_ms = Some(match self.measured_srtt_ms {
            Some(previous) => previous.mul_add(0.875, sample_ms * 0.125),
            None => sample_ms,
        });
    }

    pub(super) fn mark_open_success(
        &mut self,
        elapsed: Duration,
        load_bytes: u64,
        class: TrafficClass,
    ) {
        self.mark_success(elapsed);
        self.active_flows = self.active_flows.saturating_add(1);
        if tcp_relay_expects_interactive_response(class) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
        self.load_bytes = self.load_bytes.saturating_add(load_bytes);
    }

    pub(super) fn reserve_load(&mut self, load_bytes: u64, class: TrafficClass) {
        self.active_flows = self.active_flows.saturating_add(1);
        if tcp_relay_expects_interactive_response(class) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
        self.load_bytes = self.load_bytes.saturating_add(load_bytes);
    }

    pub(super) fn mark_reserved_open_success(&mut self, elapsed: Duration) {
        self.mark_success(elapsed);
    }

    pub(super) fn release_load(&mut self, load_bytes: u64, class: TrafficClass) {
        self.active_flows = self.active_flows.saturating_sub(1);
        if tcp_relay_expects_interactive_response(class) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_sub(1);
        }
        self.load_bytes = self.load_bytes.saturating_sub(load_bytes);
    }

    pub(super) fn reclassify_load(&mut self, from: TrafficClass, to: TrafficClass) {
        if tcp_relay_expects_interactive_response(from)
            && !tcp_relay_expects_interactive_response(to)
        {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_sub(1);
        } else if !tcp_relay_expects_interactive_response(from)
            && tcp_relay_expects_interactive_response(to)
        {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
    }

    pub(super) fn mark_delivery(&mut self, sample: PathRateSample) {
        self.state = SchedulerPathState::Active;
        self.consecutive_failures = 0;
        self.failed_until = None;
        let sample_bps = sample.rate_bps();
        self.measured_rate_bps = Some(match self.measured_rate_bps {
            Some(previous) => previous.mul_add(0.75, sample_bps * 0.25),
            None => sample_bps,
        });
    }

    pub(super) fn mark_udp_datagram_feedback(&mut self, observation: UdpDatagramPathObservation) {
        self.mark_success(observation.rtt);
        if let Some(sample) = observation.rate_sample {
            self.mark_delivery(sample);
        }
        let sample_jitter_ms = observation.jitter.as_secs_f64() * 1000.0;
        self.measured_jitter_ms = Some(match self.measured_jitter_ms {
            Some(previous) => previous.mul_add(0.875, sample_jitter_ms * 0.125),
            None => sample_jitter_ms,
        });
        self.measured_loss_rate = Some(match self.measured_loss_rate {
            Some(previous) => previous.mul_add(0.875, observation.loss_rate * 0.125),
            None => observation.loss_rate,
        });
    }

    pub(super) fn mark_udp_mtu(&mut self, payload_bytes: usize) {
        self.measured_mtu_payload_bytes = Some(payload_bytes);
    }

    pub(super) fn mark_failure(&mut self, now: Instant, has_schedulable_alternative: bool) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        if self.consecutive_failures == 1 || !has_schedulable_alternative {
            self.state = SchedulerPathState::Suspect;
            self.failed_until = None;
        } else {
            self.state = SchedulerPathState::Failed;
            self.failed_until = Some(now + PATH_FAILURE_COOLDOWN);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PathRateSample {
    bytes: u64,
    elapsed: Duration,
}

impl PathRateSample {
    pub(super) fn new(bytes: u64, elapsed: Duration) -> Option<Self> {
        if bytes < MIN_RATE_SAMPLE_BYTES {
            return None;
        }
        Some(Self {
            bytes,
            elapsed: elapsed.max(MIN_RATE_SAMPLE_DURATION),
        })
    }

    pub(super) fn rate_bps(self) -> f64 {
        self.bytes as f64 * 8.0 / self.elapsed.as_secs_f64()
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct UdpDatagramPathObservation {
    pub(super) rtt: Duration,
    pub(super) jitter: Duration,
    pub(super) loss_rate: f64,
    pub(super) rate_sample: Option<PathRateSample>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct PathDeliveryStats {
    pub(super) payload_bytes: u64,
    pub(super) first_payload_at: Option<Instant>,
    pub(super) last_payload_at: Option<Instant>,
}

impl PathDeliveryStats {
    pub(super) fn record_payload_bytes(&mut self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let now = Instant::now();
        self.payload_bytes = self.payload_bytes.saturating_add(bytes as u64);
        if self.first_payload_at.is_none() {
            self.first_payload_at = Some(now);
        }
        self.last_payload_at = Some(now);
    }

    pub(super) fn rate_sample(self) -> Option<PathRateSample> {
        let first = self.first_payload_at?;
        let last = self.last_payload_at.unwrap_or(first);
        PathRateSample::new(self.payload_bytes, last.duration_since(first))
    }
}

#[derive(Debug)]
pub(super) struct RecentIdCache<T>
where
    T: Copy + Eq + Hash,
{
    capacity: usize,
    order: VecDeque<T>,
    set: HashSet<T>,
}

impl<T> RecentIdCache<T>
where
    T: Copy + Eq + Hash,
{
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            order: VecDeque::with_capacity(capacity.min(1024)),
            set: HashSet::new(),
        }
    }

    pub(super) fn insert(&mut self, id: T) {
        if self.set.contains(&id) {
            return;
        }
        self.order.push_back(id);
        self.set.insert(id);
        while self.order.len() > self.capacity {
            if let Some(expired) = self.order.pop_front() {
                self.set.remove(&expired);
            }
        }
    }

    pub(super) fn contains(&self, id: &T) -> bool {
        self.set.contains(id)
    }
}

pub(super) fn tcp_closed_stream_cache_capacity(max_streams: usize) -> usize {
    max_streams.saturating_mul(2).clamp(128, 65_536)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct PathJoinReplayKey {
    pub(super) session_id: SessionId,
    pub(super) path_id: PathId,
    pub(super) underlay: UnderlayProtocol,
    pub(super) nonce: AuthNonce,
}

pub(super) fn path_join_replay_cache_capacity(max_streams: usize) -> usize {
    max_streams.saturating_mul(4).clamp(1024, 262_144)
}

impl ClientPathContext {
    #[cfg(test)]
    pub fn new(
        paths: Vec<PathSpec>,
        security: SecurityConfig,
        resources: ResourceLimits,
    ) -> Result<Self, RuntimeError> {
        Self::new_with_proxy_auth(paths, security, resources, ProxyAuthConfig::disabled())
    }

    pub fn new_with_proxy_auth(
        paths: Vec<PathSpec>,
        security: SecurityConfig,
        resources: ResourceLimits,
        proxy_auth: ProxyAuthConfig,
    ) -> Result<Self, RuntimeError> {
        if paths.len() > u16::MAX as usize {
            return Err(RuntimeError::PathIdOverflow);
        }
        let tcp_paths = paths
            .iter()
            .filter(|path| path.underlay == UnderlayProtocol::Tcp)
            .cloned()
            .collect::<Vec<_>>();
        let udp_paths = paths
            .into_iter()
            .filter(|path| path.underlay == UnderlayProtocol::Udp)
            .collect::<Vec<_>>();
        let health = ClientPathHealth {
            tcp: vec![ClientPathHealthRecord::default(); tcp_paths.len()],
            udp: vec![ClientPathHealthRecord::default(); udp_paths.len()],
        };
        let codec_limits = resources.into();
        let mux_limits = resources.into();
        let tcp_session_id = random_session_id()?;
        let udp_session_id = random_session_id()?;
        let reuse_tcp_latency_sessions = tcp_paths.len() > 1;
        let tcp_sessions = tcp_paths
            .iter()
            .cloned()
            .enumerate()
            .map(|(path_index, path)| {
                ClientTcpPathSessionHandle::new(ClientTcpPathSessionRuntime {
                    path,
                    path_index,
                    session_id: tcp_session_id,
                    security: security.clone(),
                    codec_limits,
                    mux_limits,
                    command_queue: tcp_session_command_queue(resources),
                    stream_frame_queue: tcp_stream_frame_queue(mux_limits),
                    closed_stream_cache_capacity: tcp_closed_stream_cache_capacity(
                        resources.max_streams,
                    ),
                    reuse_latency_session: reuse_tcp_latency_sessions,
                })
            })
            .collect::<Vec<_>>();
        let udp_sessions = udp_paths
            .iter()
            .cloned()
            .enumerate()
            .map(|(path_index, path)| {
                ClientUdpPathSessionHandle::new(ClientUdpPathSessionRuntime {
                    path,
                    path_index,
                    session_id: udp_session_id,
                    security: security.clone(),
                    codec_limits,
                    mux_limits,
                    stream_frame_queue: tcp_stream_frame_queue(mux_limits),
                })
            })
            .collect::<Vec<_>>();
        Ok(Self {
            tcp_paths: Arc::new(tcp_paths),
            udp_paths: Arc::new(udp_paths),
            tcp_sessions: Arc::new(tcp_sessions),
            udp_sessions: Arc::new(udp_sessions),
            next_tcp_stream_id: Arc::new(Mutex::new(0)),
            health: Arc::new(Mutex::new(health)),
            codec_limits,
            mux_limits,
            security,
            proxy_auth,
        })
    }

    pub(super) fn allocate_tcp_stream_id(&self) -> Result<StreamId, RuntimeError> {
        let mut next = self
            .next_tcp_stream_id
            .lock()
            .expect("client TCP stream ID lock");
        let stream_id = StreamId(*next);
        *next = next
            .checked_add(1)
            .ok_or(RuntimeError::Protocol("TCP stream ID overflow"))?;
        Ok(stream_id)
    }

    pub(super) fn ordered_tcp_path_indices(
        &self,
        class: TrafficClass,
        payload_bytes: usize,
    ) -> Vec<usize> {
        let observations = self.tcp_health_observations_for_class(class);
        if reliable_stream_latency_startup_should_use_configured_order(
            &self.tcp_paths,
            &observations,
            class,
        ) {
            return configured_order_path_indices(
                &self.tcp_paths,
                &observations,
                class,
                payload_bytes,
            );
        }
        ordered_reliable_path_indices(&self.tcp_paths, &observations, class, payload_bytes)
    }

    pub(super) fn reserve_tcp_stream_path(
        &self,
        class: TrafficClass,
        payload_bytes: usize,
        excluded: &[usize],
    ) -> Option<usize> {
        let mut health = self.health.lock().expect("client path health lock");
        let mut observations = health_observations(&mut health.tcp);
        apply_tcp_bulk_isolation(&mut observations, class, self.mux_limits);
        let active_udp_work = health
            .udp
            .iter()
            .any(|record| record.active_flows > 0 || record.load_bytes > 0);
        let indices = if endpoint_only_tcp_startup_should_spread_bulk_load(
            &self.tcp_paths,
            &observations,
            class,
            active_udp_work,
        ) {
            endpoint_only_reliable_startup_path_indices(
                &self.tcp_paths,
                &observations,
                class,
                payload_bytes,
            )
        } else {
            ordered_reliable_path_indices(&self.tcp_paths, &observations, class, payload_bytes)
        };
        let index = indices
            .into_iter()
            .find(|index| !excluded.contains(index))?;
        health
            .tcp
            .get_mut(index)?
            .reserve_load(TCP_STREAM_LOAD_BYTES, class);
        Some(index)
    }

    pub(super) fn reserve_tcp_path_load(&self, index: usize, class: TrafficClass) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.reserve_load(TCP_STREAM_LOAD_BYTES, class);
        }
    }

    pub(super) fn reserve_udp_stream_path(
        &self,
        class: TrafficClass,
        payload_bytes: usize,
        excluded: &[usize],
    ) -> Option<usize> {
        let mut health = self.health.lock().expect("client path health lock");
        let observations = health_observations(&mut health.udp);
        let index =
            ordered_reliable_path_indices(&self.udp_paths, &observations, class, payload_bytes)
                .into_iter()
                .find(|index| !excluded.contains(index))?;
        health
            .udp
            .get_mut(index)?
            .reserve_load(TCP_STREAM_LOAD_BYTES, class);
        Some(index)
    }

    pub(super) fn reserve_udp_stream_path_load(&self, index: usize, class: TrafficClass) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.reserve_load(TCP_STREAM_LOAD_BYTES, class);
        }
    }

    pub(super) fn ordered_tcp_repair_path_indices(
        &self,
        current_path_index: Option<usize>,
        class: TrafficClass,
        payload_bytes: usize,
    ) -> Vec<usize> {
        let scores = ordered_path_scores(
            &self.tcp_paths,
            &self.tcp_health_observations_for_class(class),
            class,
            payload_bytes,
        );
        if !matches!(class, TrafficClass::Bulk | TrafficClass::Background) {
            return scores.into_iter().map(|(index, _)| index).collect();
        }
        let current_eta = current_path_index.and_then(|current_path_index| {
            scores
                .iter()
                .find_map(|(index, eta)| (*index == current_path_index).then_some(*eta))
        });
        scores
            .into_iter()
            .filter(|(index, eta)| {
                Some(*index) != current_path_index
                    && current_eta.is_none_or(|current| *eta < current)
            })
            .map(|(index, _)| index)
            .collect()
    }

    pub(super) fn ordered_udp_stream_auto_bulk_discovery_indices(
        &self,
        current_path_index: Option<usize>,
        payload_bytes: usize,
    ) -> Vec<usize> {
        self.ordered_udp_stream_auto_bulk_discovery_scores(current_path_index, payload_bytes)
            .into_iter()
            .map(|(index, _)| index)
            .collect()
    }

    pub(super) fn ordered_udp_stream_repair_path_indices(
        &self,
        current_path_index: Option<usize>,
        class: TrafficClass,
        payload_bytes: usize,
        require_delivery_evidence: bool,
    ) -> Vec<usize> {
        let observations =
            health_observations(&mut self.health.lock().expect("client path health lock").udp);
        let scores = if reliable_stream_latency_startup_should_use_configured_order(
            &self.udp_paths,
            &observations,
            class,
        ) {
            configured_order_path_scores(&self.udp_paths, &observations, class, payload_bytes)
        } else {
            ordered_path_scores(&self.udp_paths, &observations, class, payload_bytes)
        };
        scores
            .into_iter()
            .filter(|(index, _)| Some(*index) != current_path_index)
            .filter(|(index, _)| {
                if !require_delivery_evidence {
                    return true;
                }
                let Some(path) = self.udp_paths.get(*index) else {
                    return false;
                };
                let observation =
                    observations
                        .get(*index)
                        .copied()
                        .unwrap_or(ClientPathObservation {
                            state: SchedulerPathState::Suspect,
                            measured_srtt_ms: None,
                            measured_jitter_ms: None,
                            measured_rate_bps: None,
                            measured_loss_rate: None,
                            measured_mtu_payload_bytes: None,
                            active_flows: 0,
                            active_latency_sensitive_flows: 0,
                            load_bytes: 0,
                        });
                udp_stream_path_can_be_auto_discovered(path, observation)
            })
            .map(|(index, _)| index)
            .collect()
    }

    pub(super) fn ordered_reliable_auto_bulk_discovery_path_keys(
        &self,
        current_tcp_path_index: Option<usize>,
        current_udp_path_index: Option<usize>,
        payload_bytes: usize,
    ) -> Vec<RelayPathKey> {
        let mut candidates = self
            .ordered_tcp_auto_bulk_discovery_scores(current_tcp_path_index, payload_bytes)
            .into_iter()
            .map(|(index, eta_ms)| {
                (
                    RelayPathKey {
                        underlay: UnderlayProtocol::Tcp,
                        index,
                    },
                    eta_ms,
                )
            })
            .chain(
                self.ordered_udp_stream_auto_bulk_discovery_scores(
                    current_udp_path_index,
                    payload_bytes,
                )
                .into_iter()
                .filter_map(|(index, eta_ms)| {
                    let snapshot = self.udp_path_snapshot(index)?;
                    Some((
                        RelayPathKey {
                            underlay: UnderlayProtocol::Udp,
                            index,
                        },
                        eta_ms
                            + udp_reliable_stream_loss_repair_penalty_ms(snapshot, payload_bytes),
                    ))
                }),
            )
            .collect::<Vec<_>>();
        if let Some(current_eta_ms) = self.reliable_stream_current_eta_ms(
            current_tcp_path_index,
            current_udp_path_index,
            payload_bytes,
        ) {
            candidates.retain(|(_, eta_ms)| *eta_ms < current_eta_ms);
        }
        candidates.sort_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| relay_path_key_order(left.0, right.0))
        });
        candidates.into_iter().map(|(key, _)| key).collect()
    }

    pub(super) fn reliable_stream_current_eta_ms(
        &self,
        current_tcp_path_index: Option<usize>,
        current_udp_path_index: Option<usize>,
        payload_bytes: usize,
    ) -> Option<f64> {
        [
            current_tcp_path_index.map(|index| RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index,
            }),
            current_udp_path_index.map(|index| RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index,
            }),
        ]
        .into_iter()
        .flatten()
        .filter_map(|key| {
            relay_path_snapshot(self, key).and_then(|snapshot| {
                scheduler::score_path(
                    snapshot,
                    TrafficClass::Bulk,
                    payload_bytes,
                    SchedulerPolicy::default(),
                )
                .map(|score| score.eta_ms)
            })
        })
        .min_by(|left, right| left.total_cmp(right))
    }

    pub(super) fn ordered_tcp_auto_bulk_discovery_scores(
        &self,
        current_path_index: Option<usize>,
        payload_bytes: usize,
    ) -> Vec<(usize, f64)> {
        let observations = self.tcp_health_observations_for_class(TrafficClass::Bulk);
        let any_measured_delivery = observations
            .iter()
            .any(|observation| observation.measured_rate_bps.is_some());
        if !any_measured_delivery && self.tcp_paths.iter().all(path_is_endpoint_only) {
            return Vec::new();
        }
        let scores = ordered_path_scores(
            &self.tcp_paths,
            &observations,
            TrafficClass::Bulk,
            payload_bytes,
        );
        reliable_auto_bulk_discovery_scores(
            &self.tcp_paths,
            &observations,
            scores,
            current_path_index,
            path_can_be_auto_discovered,
        )
    }

    pub(super) fn ordered_udp_stream_auto_bulk_discovery_scores(
        &self,
        current_path_index: Option<usize>,
        payload_bytes: usize,
    ) -> Vec<(usize, f64)> {
        let observations =
            health_observations(&mut self.health.lock().expect("client path health lock").udp);
        let any_measured_delivery = observations
            .iter()
            .any(|observation| observation.measured_rate_bps.is_some());
        if !any_measured_delivery && self.udp_paths.iter().all(path_is_endpoint_only) {
            return Vec::new();
        }
        let scores = ordered_path_scores(
            &self.udp_paths,
            &observations,
            TrafficClass::Bulk,
            payload_bytes,
        );
        reliable_auto_bulk_discovery_scores(
            &self.udp_paths,
            &observations,
            scores,
            current_path_index,
            udp_stream_path_can_be_auto_discovered,
        )
    }

    pub(super) fn tcp_health_observations_for_class(
        &self,
        class: TrafficClass,
    ) -> Vec<ClientPathObservation> {
        let mut observations =
            health_observations(&mut self.health.lock().expect("client path health lock").tcp);
        apply_tcp_bulk_isolation(&mut observations, class, self.mux_limits);
        observations
    }

    pub(super) fn tcp_path_snapshot(&self, index: usize) -> Option<PathSnapshot> {
        let path = self.tcp_paths.get(index)?;
        let observation = self
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)?
            .observe(Instant::now());
        Some(path_snapshot(path, index, observation))
    }

    pub(super) fn udp_path_snapshot(&self, index: usize) -> Option<PathSnapshot> {
        let path = self.udp_paths.get(index)?;
        let observation = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)?
            .observe(Instant::now());
        Some(path_snapshot(path, index, observation))
    }

    pub(super) fn ordered_udp_path_candidates_for_ttl(
        &self,
        payload_bytes: usize,
        ttl_ms: u32,
    ) -> Vec<UdpPathCandidate> {
        if ttl_ms == 0 {
            return Vec::new();
        }
        let observations =
            health_observations(&mut self.health.lock().expect("client path health lock").udp);
        if self.udp_paths.iter().all(path_is_endpoint_only)
            && !observations
                .iter()
                .any(udp_observation_has_datagram_feedback)
        {
            let freshness_budget_ms = f64::from(ttl_ms) * UDP_DATAGRAM_MIN_TTL_FIT_RATIO;
            return configured_order_path_indices(
                &self.udp_paths,
                &observations,
                TrafficClass::RealtimeDatagram,
                payload_bytes,
            )
            .into_iter()
            .find_map(|path_index| {
                let path = self.udp_paths.get(path_index)?;
                let observation = observations.get(path_index).copied()?;
                let eta_ms = scheduler::score_path(
                    path_snapshot(path, path_index, observation),
                    TrafficClass::RealtimeDatagram,
                    payload_bytes,
                    SchedulerPolicy::default(),
                )?
                .eta_ms;
                (eta_ms <= freshness_budget_ms).then_some(UdpPathCandidate { path_index, eta_ms })
            })
            .into_iter()
            .collect();
        }
        let mut candidates = ordered_path_scores_for_ttl(
            &self.udp_paths,
            &observations,
            TrafficClass::RealtimeDatagram,
            payload_bytes,
            ttl_ms,
        )
        .into_iter()
        .map(|(path_index, eta_ms)| UdpPathCandidate { path_index, eta_ms })
        .collect::<Vec<_>>();
        if candidates
            .iter()
            .any(|candidate| self.udp_path_candidate_has_realtime_model(*candidate, &observations))
        {
            candidates.retain(|candidate| {
                self.udp_path_candidate_has_realtime_model(*candidate, &observations)
            });
        }
        candidates
    }

    pub(super) fn udp_path_candidate_has_realtime_model(
        &self,
        candidate: UdpPathCandidate,
        observations: &[ClientPathObservation],
    ) -> bool {
        let Some(path) = self.udp_paths.get(candidate.path_index) else {
            return false;
        };
        observations
            .get(candidate.path_index)
            .copied()
            .is_some_and(|observation| udp_path_has_realtime_model(path, observation))
    }

    pub(super) fn udp_path_eta_for_ttl(
        &self,
        index: usize,
        payload_bytes: usize,
        ttl_ms: u32,
        discount_open_udp_session: bool,
    ) -> Option<f64> {
        if ttl_ms == 0 {
            return None;
        }
        let path = self.udp_paths.get(index)?;
        let mut observation = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)?
            .observe(Instant::now());
        if discount_open_udp_session {
            observation.active_flows = observation.active_flows.saturating_sub(1);
            observation.load_bytes = observation
                .load_bytes
                .saturating_sub(UDP_SESSION_LOAD_BYTES);
        }
        let score = scheduler::score_path(
            path_snapshot(path, index, observation),
            TrafficClass::RealtimeDatagram,
            payload_bytes,
            SchedulerPolicy::default(),
        )?;
        let freshness_budget_ms = f64::from(ttl_ms) * UDP_DATAGRAM_MIN_TTL_FIT_RATIO;
        (score.eta_ms <= freshness_budget_ms).then_some(score.eta_ms)
    }

    pub(super) fn udp_path_runtime_model(
        &self,
        index: usize,
        ttl_ms: u32,
    ) -> Option<UdpPathRuntimeModel> {
        if ttl_ms == 0 {
            return None;
        }
        let path = self.udp_paths.get(index)?;
        let observation = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)?
            .observe(Instant::now());
        let snapshot = path_snapshot(path, index, observation);
        scheduler::score_path(
            snapshot,
            TrafficClass::RealtimeDatagram,
            1,
            SchedulerPolicy::default(),
        )?;
        Some(UdpPathRuntimeModel::from_snapshot(
            snapshot,
            ttl_ms,
            udp_mtu_payload_bytes(path, observation, self.mux_limits.max_payload_bytes),
            observation.measured_mtu_payload_bytes.is_some(),
            udp_probe_ceiling_payload_bytes(self.mux_limits.max_payload_bytes),
        ))
    }

    pub(super) fn mark_tcp_path_open_success(
        &self,
        index: usize,
        elapsed: Duration,
        class: TrafficClass,
    ) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.mark_open_success(elapsed, TCP_STREAM_LOAD_BYTES, class);
        }
    }

    pub(super) fn mark_tcp_path_reserved_open_success(&self, index: usize, elapsed: Duration) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.mark_reserved_open_success(elapsed);
        }
    }

    pub(super) fn mark_tcp_path_probe_success(&self, index: usize, elapsed: Duration) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.mark_success(elapsed);
        }
    }

    pub(super) fn should_probe_tcp_path(&self, index: usize) -> bool {
        self.health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
            .is_some_and(|record| {
                path_observation_is_idle_for_probe(record.observe(Instant::now()))
            })
    }

    pub(super) fn release_tcp_path_load(&self, index: usize, class: TrafficClass) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.release_load(TCP_STREAM_LOAD_BYTES, class);
        }
    }

    pub(super) fn mark_udp_stream_reserved_open_success(&self, index: usize, elapsed: Duration) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_reserved_open_success(elapsed);
        }
    }

    pub(super) fn release_udp_stream_path_load(&self, index: usize, class: TrafficClass) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.release_load(TCP_STREAM_LOAD_BYTES, class);
        }
    }

    pub(super) fn mark_relay_path_failure(&self, underlay: UnderlayProtocol, index: usize) {
        match underlay {
            UnderlayProtocol::Tcp => self.mark_tcp_path_failure(index),
            UnderlayProtocol::Udp => self.mark_udp_path_failure(index),
        }
    }

    pub(super) fn release_relay_path_load(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        class: TrafficClass,
    ) {
        match underlay {
            UnderlayProtocol::Tcp => self.release_tcp_path_load(index, class),
            UnderlayProtocol::Udp => self.release_udp_stream_path_load(index, class),
        }
    }

    pub(super) fn reclassify_relay_path_load(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        from: TrafficClass,
        to: TrafficClass,
    ) {
        if from == to {
            return;
        }
        let mut health = self.health.lock().expect("client path health lock");
        match underlay {
            UnderlayProtocol::Tcp => {
                if let Some(current) = health.tcp.get_mut(index) {
                    current.reclassify_load(from, to);
                }
            }
            UnderlayProtocol::Udp => {
                if let Some(current) = health.udp.get_mut(index) {
                    current.reclassify_load(from, to);
                }
            }
        }
    }

    pub(super) fn mark_relay_path_delivery(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        stats: PathDeliveryStats,
    ) {
        match underlay {
            UnderlayProtocol::Tcp => self.mark_tcp_path_delivery(index, stats),
            UnderlayProtocol::Udp => self.mark_udp_path_delivery(index, stats),
        }
    }

    pub(super) fn mark_tcp_path_delivery(&self, index: usize, stats: PathDeliveryStats) {
        let Some(sample) = stats.rate_sample() else {
            return;
        };
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.mark_delivery(sample);
        }
    }

    pub(super) fn mark_tcp_path_failure(&self, index: usize) {
        let now = Instant::now();
        let mut health = self.health.lock().expect("client path health lock");
        let has_schedulable_alternative =
            path_records_have_schedulable_alternative(&mut health.tcp, index, now);
        if let Some(current) = health.tcp.get_mut(index) {
            current.mark_failure(now, has_schedulable_alternative);
        }
    }

    pub(super) fn mark_udp_path_open_success(&self, index: usize, elapsed: Duration) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_open_success(
                elapsed,
                UDP_SESSION_LOAD_BYTES,
                TrafficClass::RealtimeDatagram,
            );
        }
    }

    pub(super) fn mark_udp_path_probe_success(&self, index: usize, elapsed: Duration) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_success(elapsed);
        }
    }

    pub(super) fn should_probe_udp_path(&self, index: usize) -> bool {
        self.health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
            .is_some_and(|record| {
                path_observation_is_idle_for_probe(record.observe(Instant::now()))
            })
    }

    pub(super) fn release_udp_path_load(&self, index: usize) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.release_load(UDP_SESSION_LOAD_BYTES, TrafficClass::RealtimeDatagram);
        }
    }

    pub(super) fn mark_udp_path_delivery(&self, index: usize, stats: PathDeliveryStats) {
        let Some(sample) = stats.rate_sample() else {
            return;
        };
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_delivery(sample);
        }
    }

    pub(super) fn mark_udp_path_feedback(
        &self,
        index: usize,
        observation: UdpDatagramPathObservation,
    ) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_udp_datagram_feedback(observation);
        }
    }

    pub(super) fn mark_udp_path_mtu(&self, index: usize, payload_bytes: usize) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_udp_mtu(payload_bytes);
        }
    }

    pub(super) fn mark_udp_path_failure(&self, index: usize) {
        let now = Instant::now();
        let mut health = self.health.lock().expect("client path health lock");
        let has_schedulable_alternative =
            path_records_have_schedulable_alternative(&mut health.udp, index, now);
        if let Some(current) = health.udp.get_mut(index) {
            current.mark_failure(now, has_schedulable_alternative);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct UdpPathRuntimeModel {
    pub(super) pacing_rate_bps: f64,
    pub(super) response_timeout: Duration,
    pub(super) mtu_payload_bytes: usize,
    pub(super) mtu_is_measured: bool,
    pub(super) mtu_probe_ceiling_payload_bytes: usize,
}

impl UdpPathRuntimeModel {
    pub(super) fn from_snapshot(
        snapshot: PathSnapshot,
        ttl_ms: u32,
        mtu_payload_bytes: usize,
        mtu_is_measured: bool,
        mtu_probe_ceiling_payload_bytes: usize,
    ) -> Self {
        let loss_backoff = (1.0 - snapshot.loss_rate.clamp(0.0, 1.0)).clamp(0.25, 1.0);
        let pacing_rate_bps = (snapshot.delivery_rate_bps * UDP_BBR_PACING_GAIN * loss_backoff)
            .max(UDP_MIN_PACING_RATE_BPS);
        let timeout_loss_gain = 1.0 + snapshot.loss_rate.clamp(0.0, 1.0);
        let model_timeout = Duration::from_secs_f64(
            (((snapshot.srtt_ms + snapshot.jitter_ms.mul_add(4.0, 25.0)) * timeout_loss_gain)
                / 1000.0)
                .max(UDP_MIN_RESPONSE_TIMEOUT.as_secs_f64()),
        );
        let ttl_timeout = Duration::from_millis(u64::from(ttl_ms));
        let response_timeout = model_timeout.min(UDP_MAX_RESPONSE_TIMEOUT).min(ttl_timeout);
        Self {
            pacing_rate_bps,
            response_timeout,
            mtu_payload_bytes,
            mtu_is_measured,
            mtu_probe_ceiling_payload_bytes,
        }
    }

    pub(super) fn accepts_or_can_probe(self, payload_bytes: usize) -> bool {
        payload_bytes <= self.mtu_payload_bytes
            || (!self.mtu_is_measured && payload_bytes <= self.mtu_probe_ceiling_payload_bytes)
    }

    pub(super) fn pacing_interval(self, payload_bytes: usize) -> Duration {
        if payload_bytes == 0 {
            return Duration::ZERO;
        }
        Duration::from_secs_f64(payload_bytes as f64 * 8.0 / self.pacing_rate_bps)
    }
}

pub(super) fn udp_mtu_payload_bytes(
    path: &PathSpec,
    observation: ClientPathObservation,
    max_payload_bytes: usize,
) -> usize {
    let seeded = observation
        .measured_mtu_payload_bytes
        .or(path.metadata.initial_mtu_payload_bytes)
        .unwrap_or(UDP_DEFAULT_MTU_PAYLOAD_BYTES);
    seeded.clamp(
        UDP_MIN_MTU_PAYLOAD_BYTES,
        udp_probe_ceiling_payload_bytes(max_payload_bytes),
    )
}

pub(super) fn udp_probe_ceiling_payload_bytes(max_payload_bytes: usize) -> usize {
    max_payload_bytes.clamp(UDP_MIN_MTU_PAYLOAD_BYTES, UDP_MAX_MTU_PAYLOAD_BYTES)
}

pub(super) fn health_observations(
    records: &mut [ClientPathHealthRecord],
) -> Vec<ClientPathObservation> {
    let now = Instant::now();
    records
        .iter_mut()
        .map(|record| record.observe(now))
        .collect()
}

pub(super) fn path_records_have_schedulable_alternative(
    records: &mut [ClientPathHealthRecord],
    failed_index: usize,
    now: Instant,
) -> bool {
    records.iter_mut().enumerate().any(|(index, record)| {
        index != failed_index
            && !matches!(
                record.observe(now).state,
                SchedulerPathState::Failed | SchedulerPathState::Draining
            )
    })
}

pub(super) fn path_observation_is_idle_for_probe(observation: ClientPathObservation) -> bool {
    observation.active_flows == 0 && observation.load_bytes == 0
}

pub(super) fn apply_tcp_bulk_isolation(
    observations: &mut [ClientPathObservation],
    class: TrafficClass,
    mux_limits: MuxLimits,
) {
    if !matches!(class, TrafficClass::Bulk | TrafficClass::Background) {
        return;
    }
    if !observations
        .iter()
        .any(|observation| observation.measured_rate_bps.is_some())
    {
        return;
    }
    let isolation_bytes = mux_limits.max_tcp_path_inflight_bytes as u64;
    for observation in observations {
        let latency_flows = u64::from(observation.active_latency_sensitive_flows);
        observation.load_bytes = observation
            .load_bytes
            .saturating_add(latency_flows.saturating_mul(isolation_bytes));
    }
}

pub(super) fn reliable_stream_latency_startup_should_use_configured_order(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    class: TrafficClass,
) -> bool {
    tcp_relay_expects_interactive_response(class)
        && paths.iter().all(path_is_endpoint_only)
        && (!endpoint_only_startup_has_latency_sensitive_load(observations)
            || endpoint_only_startup_has_bulk_load(observations))
}

pub(super) fn reliable_stream_latency_startup_should_use_load_balanced_order(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    class: TrafficClass,
) -> bool {
    tcp_relay_expects_interactive_response(class)
        && paths.iter().all(path_is_endpoint_only)
        && endpoint_only_startup_has_latency_sensitive_load(observations)
        && !endpoint_only_startup_has_bulk_load(observations)
}

pub(super) fn endpoint_only_startup_has_latency_sensitive_load(
    observations: &[ClientPathObservation],
) -> bool {
    observations
        .iter()
        .any(|observation| observation.active_latency_sensitive_flows > 0)
}

pub(super) fn endpoint_only_startup_has_any_load(observations: &[ClientPathObservation]) -> bool {
    observations
        .iter()
        .any(|observation| observation.active_flows > 0 || observation.load_bytes > 0)
}

pub(super) fn endpoint_only_startup_has_bulk_load(observations: &[ClientPathObservation]) -> bool {
    observations
        .iter()
        .any(|observation| observation.active_flows > observation.active_latency_sensitive_flows)
}

pub(super) fn endpoint_only_tcp_startup_should_spread_bulk_load(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    class: TrafficClass,
    active_udp_work: bool,
) -> bool {
    tcp_relay_expects_interactive_response(class)
        && paths.iter().all(path_is_endpoint_only)
        && endpoint_only_startup_has_any_load(observations)
        && endpoint_only_startup_has_bulk_load(observations)
        && !endpoint_only_startup_has_latency_sensitive_load(observations)
        && !active_udp_work
}

pub(super) fn ordered_reliable_path_indices(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    class: TrafficClass,
    payload_bytes: usize,
) -> Vec<usize> {
    if reliable_stream_latency_startup_should_use_configured_order(paths, observations, class) {
        return configured_order_path_indices(paths, observations, class, payload_bytes);
    }
    if reliable_stream_latency_startup_should_use_load_balanced_order(paths, observations, class) {
        return endpoint_only_reliable_startup_path_indices(
            paths,
            observations,
            class,
            payload_bytes,
        );
    }
    ordered_path_indices(paths, observations, class, payload_bytes)
}

pub(super) fn endpoint_only_reliable_startup_path_indices(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    class: TrafficClass,
    payload_bytes: usize,
) -> Vec<usize> {
    let observations = observations
        .iter()
        .copied()
        .map(|observation| ClientPathObservation {
            measured_srtt_ms: None,
            measured_jitter_ms: None,
            measured_rate_bps: None,
            measured_loss_rate: None,
            measured_mtu_payload_bytes: observation.measured_mtu_payload_bytes,
            ..observation
        })
        .collect::<Vec<_>>();
    ordered_path_indices(paths, &observations, class, payload_bytes)
}

pub(super) fn path_is_endpoint_only(path: &PathSpec) -> bool {
    path.metadata.initial_srtt_ms.is_none()
        && path.metadata.initial_jitter_ms.is_none()
        && path.metadata.initial_rate == RateHint::Unknown
        && path.metadata.capabilities == crate::protocol::PathCapabilities::default()
}

pub(super) fn configured_order_path_indices(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    class: TrafficClass,
    payload_bytes: usize,
) -> Vec<usize> {
    configured_order_path_scores(paths, observations, class, payload_bytes)
        .into_iter()
        .map(|(index, _)| index)
        .collect()
}

pub(super) fn configured_order_path_scores(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    class: TrafficClass,
    payload_bytes: usize,
) -> Vec<(usize, f64)> {
    paths
        .iter()
        .enumerate()
        .filter_map(|(index, path)| {
            let observation = observations
                .get(index)
                .copied()
                .unwrap_or(ClientPathObservation {
                    state: SchedulerPathState::Suspect,
                    measured_srtt_ms: None,
                    measured_jitter_ms: None,
                    measured_rate_bps: None,
                    measured_loss_rate: None,
                    measured_mtu_payload_bytes: None,
                    active_flows: 0,
                    active_latency_sensitive_flows: 0,
                    load_bytes: 0,
                });
            scheduler::score_path(
                path_snapshot(path, index, observation),
                class,
                payload_bytes,
                SchedulerPolicy::default(),
            )
            .map(|score| (index, score.eta_ms))
        })
        .collect()
}

pub(super) fn ordered_path_indices(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    class: TrafficClass,
    payload_bytes: usize,
) -> Vec<usize> {
    ordered_path_scores(paths, observations, class, payload_bytes)
        .into_iter()
        .map(|(index, _)| index)
        .collect()
}

pub(super) fn ordered_path_scores_for_ttl(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    class: TrafficClass,
    payload_bytes: usize,
    ttl_ms: u32,
) -> Vec<(usize, f64)> {
    let scores = ordered_path_scores(paths, observations, class, payload_bytes);
    let freshness_budget_ms = f64::from(ttl_ms) * UDP_DATAGRAM_MIN_TTL_FIT_RATIO;
    scores
        .iter()
        .copied()
        .filter(|(_, eta_ms)| *eta_ms <= freshness_budget_ms)
        .collect::<Vec<_>>()
}

pub(super) fn ordered_path_scores(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    class: TrafficClass,
    payload_bytes: usize,
) -> Vec<(usize, f64)> {
    let mut scores = paths
        .iter()
        .enumerate()
        .filter_map(|(index, path)| {
            let observation = observations
                .get(index)
                .copied()
                .unwrap_or(ClientPathObservation {
                    state: SchedulerPathState::Suspect,
                    measured_srtt_ms: None,
                    measured_jitter_ms: None,
                    measured_rate_bps: None,
                    measured_loss_rate: None,
                    measured_mtu_payload_bytes: None,
                    active_flows: 0,
                    active_latency_sensitive_flows: 0,
                    load_bytes: 0,
                });
            scheduler::score_path(
                path_snapshot(path, index, observation),
                class,
                payload_bytes,
                SchedulerPolicy::default(),
            )
            .map(|score| (index, score.eta_ms))
        })
        .collect::<Vec<_>>();
    scores.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    scores
}

pub(super) fn path_snapshot(
    path: &PathSpec,
    index: usize,
    observation: ClientPathObservation,
) -> PathSnapshot {
    let hinted_delivery_rate_bps = match path.metadata.initial_rate {
        RateHint::Unknown => default_path_rate_bps(path.underlay),
        RateHint::Unlimited => 1_000_000_000_000.0,
        RateHint::BitsPerSecond(rate) => rate.max(1) as f64,
    };
    let delivery_rate_bps = observation
        .measured_rate_bps
        .unwrap_or(hinted_delivery_rate_bps)
        .max(1.0);
    PathSnapshot {
        id: PathId(index as u16),
        underlay: path.underlay,
        state: observation.state,
        flags: path.metadata.capabilities.into(),
        srtt_ms: observation.measured_srtt_ms.unwrap_or_else(|| {
            path.metadata
                .initial_srtt_ms
                .map_or_else(|| default_path_srtt_ms(path.underlay), f64::from)
        }),
        jitter_ms: observation
            .measured_jitter_ms
            .unwrap_or_else(|| f64::from(path.metadata.initial_jitter_ms.unwrap_or(0))),
        delivery_rate_bps,
        loss_rate: observation.measured_loss_rate.unwrap_or(0.0),
        queue_bytes: observation.load_bytes,
        bytes_in_flight: u64::from(observation.active_flows) * PATH_OPEN_SCORE_BYTES as u64,
    }
}

pub(super) fn udp_path_has_realtime_model(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> bool {
    observation.measured_srtt_ms.is_some()
        || observation.measured_jitter_ms.is_some()
        || observation.measured_rate_bps.is_some()
        || observation.measured_loss_rate.is_some()
        || path.metadata.initial_srtt_ms.is_some()
        || path.metadata.initial_jitter_ms.is_some()
        || path.metadata.initial_rate != RateHint::Unknown
}

pub(super) fn udp_observation_has_datagram_feedback(observation: &ClientPathObservation) -> bool {
    observation.measured_jitter_ms.is_some()
        || observation.measured_loss_rate.is_some()
        || observation.measured_rate_bps.is_some()
        || observation.measured_mtu_payload_bytes.is_some()
}

pub(super) fn reliable_auto_bulk_discovery_scores(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    scores: Vec<(usize, f64)>,
    current_path_index: Option<usize>,
    candidate_is_allowed: fn(&PathSpec, ClientPathObservation) -> bool,
) -> Vec<(usize, f64)> {
    let current_eta = current_path_index.and_then(|current_path_index| {
        scores
            .iter()
            .find_map(|(index, eta)| (*index == current_path_index).then_some(*eta))
    });
    let improves_current = |index: usize, eta: f64| {
        Some(index) != current_path_index && current_eta.is_none_or(|current| eta < current)
    };
    let measured = scores
        .iter()
        .copied()
        .filter(|(index, eta)| {
            improves_current(*index, *eta)
                && observations.get(*index).is_some_and(|observation| {
                    observation.measured_rate_bps.is_some()
                        && paths
                            .get(*index)
                            .is_some_and(|path| candidate_is_allowed(path, *observation))
                })
        })
        .collect::<Vec<_>>();
    if !measured.is_empty() {
        return measured;
    }
    scores
        .into_iter()
        .filter(|(index, eta)| {
            let Some(path) = paths.get(*index) else {
                return false;
            };
            let observation = observations
                .get(*index)
                .copied()
                .unwrap_or(ClientPathObservation {
                    state: SchedulerPathState::Suspect,
                    measured_srtt_ms: None,
                    measured_jitter_ms: None,
                    measured_rate_bps: None,
                    measured_loss_rate: None,
                    measured_mtu_payload_bytes: None,
                    active_flows: 0,
                    active_latency_sensitive_flows: 0,
                    load_bytes: 0,
                });
            improves_current(*index, *eta) && candidate_is_allowed(path, observation)
        })
        .collect()
}

pub(super) fn path_can_be_auto_discovered(
    path: &PathSpec,
    _observation: ClientPathObservation,
) -> bool {
    !path.metadata.capabilities.expensive
        && !path.metadata.capabilities.backup
        && !path.metadata.capabilities.probe_only
        && path.metadata.capabilities.bulk_allowed
}

pub(super) fn udp_stream_path_can_be_auto_discovered(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> bool {
    path_can_be_auto_discovered(path, observation)
        && (observation.measured_rate_bps.is_some()
            || path.metadata.initial_rate != RateHint::Unknown)
}

pub(super) fn udp_reliable_stream_loss_repair_penalty_ms(
    snapshot: scheduler::PathSnapshot,
    payload_bytes: usize,
) -> f64 {
    let loss = snapshot.loss_rate.clamp(0.0, 0.75);
    if loss <= f64::EPSILON {
        return 0.0;
    }
    let fragment_count = (payload_bytes as f64 / UDP_DEFAULT_MTU_PAYLOAD_BYTES as f64)
        .ceil()
        .max(1.0);
    let expected_repairs = fragment_count * loss / (1.0 - loss).max(0.01);
    let repair_rtt_ms = snapshot.srtt_ms + snapshot.jitter_ms.max(0.0) * 4.0;
    expected_repairs * repair_rtt_ms
}

pub(super) fn default_path_srtt_ms(underlay: UnderlayProtocol) -> f64 {
    match underlay {
        UnderlayProtocol::Tcp => 50.0,
        UnderlayProtocol::Udp => 40.0,
    }
}

pub(super) fn default_path_rate_bps(underlay: UnderlayProtocol) -> f64 {
    match underlay {
        UnderlayProtocol::Tcp | UnderlayProtocol::Udp => 100_000_000.0,
    }
}

#[derive(Debug, Clone)]
pub struct ServerPathContext {
    pub(super) outbound: OutboundConfig,
    pub(super) outbound_dns: DnsConfig,
    pub(super) codec_limits: CodecLimits,
    pub(super) mux_limits: MuxLimits,
    pub(super) security: SecurityConfig,
    pub(super) tcp_streams: Arc<ServerTcpStreamRegistry>,
    pub(super) path_join_replay: Arc<Mutex<RecentIdCache<PathJoinReplayKey>>>,
    pub(super) max_tcp_streams: usize,
    pub(super) max_udp_flows_per_session: usize,
}

impl ServerPathContext {
    pub(super) fn accept_path_join_nonce(
        &self,
        session_id: SessionId,
        path_id: PathId,
        underlay: UnderlayProtocol,
        nonce: AuthNonce,
    ) -> bool {
        let key = PathJoinReplayKey {
            session_id,
            path_id,
            underlay,
            nonce,
        };
        let mut replay = self.path_join_replay.lock().expect("path join replay lock");
        if replay.contains(&key) {
            return false;
        }
        replay.insert(key);
        true
    }
}

pub(super) fn random_session_id() -> Result<SessionId, RuntimeError> {
    Ok(SessionId(random_u64()?))
}

pub(super) fn random_u64() -> Result<u64, RuntimeError> {
    let mut bytes = [0u8; 8];
    getrandom::getrandom(&mut bytes).map_err(RuntimeError::Random)?;
    Ok(u64::from_be_bytes(bytes))
}

pub(super) fn random_nonce() -> Result<AuthNonce, RuntimeError> {
    let mut bytes = [0u8; 16];
    getrandom::getrandom(&mut bytes).map_err(RuntimeError::Random)?;
    Ok(AuthNonce(bytes))
}
