use super::bulk_admission::*;
use super::datagram::*;
use super::error::RuntimeError;
use super::ingress_runtime::*;
use super::management::*;
use super::prelude::*;
use super::relay_control::*;
use super::relay_io::*;
use super::relay_open::*;
use super::server_runtime::*;
use super::tcp_path::*;
use super::tun_l4::*;
use super::udp_path::*;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;

pub(super) const MAX_HTTP_CONNECT_HEADER_BYTES: usize = 64 * 1024;
pub(super) const PATH_OPEN_SCORE_BYTES: usize = 4 * 1024;
pub(super) const UDP_PATH_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);
pub(super) const PATH_FAILURE_COOLDOWN: Duration = Duration::from_secs(5);
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
            run_client(client, config.resources, config.management).await
        }
        CommandConfig::Server(server) => {
            run_server(
                server.bind_paths,
                server.outbound,
                server.outbound_dns,
                server.security,
                config.resources,
                config.management,
            )
            .await
        }
        CommandConfig::Node(node) => run_node(node, config.resources, config.management).await,
    }
}

pub(super) async fn run_node(
    node: NodeConfig,
    resources: ResourceLimits,
    management: ManagementConfig,
) -> Result<(), RuntimeError> {
    let mut services = tokio::task::JoinSet::new();

    let mut client_contexts = Vec::with_capacity(node.clients.len());
    for client in node.clients {
        let context = new_client_path_context(&client, resources)?;
        start_client_path_probes(
            context.clone(),
            client.path_probe_interval,
            client.path_probe_timeout,
        );
        spawn_client_ingresses(client.ingresses, context.clone(), &mut services);
        client_contexts.push(context);
    }

    let mut server_contexts = Vec::with_capacity(node.servers.len());
    for server in node.servers {
        let context = new_server_path_context_with_identity(
            server.tag.clone(),
            server.route_target.clone(),
            server.bind_paths.clone(),
            server.outbound,
            server.outbound_dns,
            server.security,
            resources,
        );
        let bound = bind_server_paths(server.bind_paths, &context).await?;
        spawn_server_listeners(bound, context.clone(), &mut services);
        server_contexts.push(context);
    }

    if management.enabled() {
        services.spawn(async move {
            run_node_management_api(management, client_contexts, server_contexts).await
        });
    }

    wait_node_services(services).await
}

pub(super) async fn run_client(
    client: ClientConfig,
    resources: ResourceLimits,
    management: ManagementConfig,
) -> Result<(), RuntimeError> {
    let path_probe_interval = client.path_probe_interval;
    let path_probe_timeout = client.path_probe_timeout;
    let context = new_client_path_context(&client, resources)?;
    start_client_path_probes(context.clone(), path_probe_interval, path_probe_timeout);
    let mut ingresses = tokio::task::JoinSet::new();
    if management.enabled() {
        let context = context.clone();
        ingresses.spawn(async move { run_client_management_api(management, context).await });
    }
    spawn_client_ingresses(client.ingresses, context, &mut ingresses);
    wait_client_ingresses(ingresses).await
}

fn new_client_path_context(
    client: &ClientConfig,
    resources: ResourceLimits,
) -> Result<ClientPathContext, RuntimeError> {
    ClientPathContext::new_with_path_configs_and_target(
        client.paths.clone(),
        resources,
        ProxyAuthConfig::disabled(),
        client.route_target.clone(),
        client.ingresses.clone(),
    )
}

fn spawn_client_ingresses(
    ingresses: Vec<LocalIngressConfig>,
    context: ClientPathContext,
    tasks: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) {
    for ingress in ingresses {
        let context = context.clone();
        match ingress.config {
            IngressConfig::Socks5 { listen, proxy_auth } => {
                tasks.spawn(
                    async move { run_socks5_client_ingress(listen, context, proxy_auth).await },
                );
            }
            IngressConfig::HttpConnect { listen, proxy_auth } => {
                tasks.spawn(async move {
                    run_http_connect_client_ingress(listen, context, proxy_auth).await
                });
            }
            IngressConfig::TunL4(tun) => {
                tasks.spawn(async move { run_tun_l4_client(tun, context).await });
            }
        }
    }
}

async fn wait_client_ingresses(
    mut ingresses: tokio::task::JoinSet<Result<(), RuntimeError>>,
) -> Result<(), RuntimeError> {
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

async fn wait_node_services(
    mut services: tokio::task::JoinSet<Result<(), RuntimeError>>,
) -> Result<(), RuntimeError> {
    if let Some(result) = services.join_next().await {
        match result {
            Ok(Ok(())) => Err(RuntimeError::Protocol("node service exited")),
            Ok(Err(err)) => Err(err),
            Err(err) => Err(RuntimeError::TaskJoin(err)),
        }
    } else {
        Err(RuntimeError::Protocol("node has no runtime services"))
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
    pub(super) route_target: Option<RouteTarget>,
    pub(super) ingresses: Arc<Vec<LocalIngressConfig>>,
    pub(super) tcp_paths: Arc<Vec<PathSpec>>,
    pub(super) udp_paths: Arc<Vec<PathSpec>>,
    pub(super) tcp_security: Arc<Vec<SecurityConfig>>,
    pub(super) tcp_sessions: Arc<Vec<ClientTcpPathSessionHandle>>,
    pub(super) udp_sessions: Arc<Vec<ClientUdpPathSessionHandle>>,
    pub(super) next_tcp_stream_id: Arc<Mutex<u64>>,
    pub(super) health: Arc<Mutex<ClientPathHealth>>,
    pub(super) codec_limits: CodecLimits,
    pub(super) mux_limits: MuxLimits,
    #[cfg(test)]
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
    pub(super) manual_disabled: bool,
    pub(super) consecutive_failures: u32,
    pub(super) measured_srtt_ms: Option<f64>,
    pub(super) measured_jitter_ms: Option<f64>,
    pub(super) measured_rate_bps: Option<f64>,
    pub(super) measured_loss_rate: Option<f64>,
    pub(super) measured_mtu_payload_bytes: Option<usize>,
    pub(super) delivery_samples: u32,
    pub(super) last_delivery_at: Option<Instant>,
    pub(super) failed_until: Option<Instant>,
    pub(super) active_flows: u32,
    pub(super) active_latency_sensitive_flows: u32,
    pub(super) relay_bytes_in_flight: u64,
    pub(super) relay_queue_bytes: u64,
    pub(super) carrier_srtt_ms: Option<f64>,
    pub(super) carrier_rttvar_ms: Option<f64>,
    pub(super) carrier_delivery_rate_bps: Option<f64>,
    pub(super) carrier_bytes_in_flight: u64,
    pub(super) carrier_queue_bytes: u64,
    pub(super) carrier_inflight_limit_bytes: u64,
    pub(super) carrier_delivery_samples: u32,
    pub(super) carrier_last_delivery_at: Option<Instant>,
    pub(super) carrier_app_limited: bool,
}

impl Default for ClientPathHealthRecord {
    fn default() -> Self {
        Self {
            state: SchedulerPathState::Active,
            manual_disabled: false,
            consecutive_failures: 0,
            measured_srtt_ms: None,
            measured_jitter_ms: None,
            measured_rate_bps: None,
            measured_loss_rate: None,
            measured_mtu_payload_bytes: None,
            delivery_samples: 0,
            last_delivery_at: None,
            failed_until: None,
            active_flows: 0,
            active_latency_sensitive_flows: 0,
            relay_bytes_in_flight: 0,
            relay_queue_bytes: 0,
            carrier_srtt_ms: None,
            carrier_rttvar_ms: None,
            carrier_delivery_rate_bps: None,
            carrier_bytes_in_flight: 0,
            carrier_queue_bytes: 0,
            carrier_inflight_limit_bytes: 0,
            carrier_delivery_samples: 0,
            carrier_last_delivery_at: None,
            carrier_app_limited: true,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ClientPathObservation {
    pub(super) state: SchedulerPathState,
    pub(super) manual_disabled: bool,
    pub(super) measured_srtt_ms: Option<f64>,
    pub(super) measured_jitter_ms: Option<f64>,
    pub(super) measured_rate_bps: Option<f64>,
    pub(super) measured_loss_rate: Option<f64>,
    pub(super) measured_mtu_payload_bytes: Option<usize>,
    pub(super) delivery_samples: u32,
    pub(super) last_delivery_at: Option<Instant>,
    pub(super) active_flows: u32,
    pub(super) active_latency_sensitive_flows: u32,
    pub(super) relay_bytes_in_flight: u64,
    pub(super) relay_queue_bytes: u64,
    pub(super) carrier_srtt_ms: Option<f64>,
    pub(super) carrier_rttvar_ms: Option<f64>,
    pub(super) carrier_delivery_rate_bps: Option<f64>,
    pub(super) carrier_bytes_in_flight: u64,
    pub(super) carrier_queue_bytes: u64,
    pub(super) carrier_inflight_limit_bytes: u64,
    pub(super) carrier_delivery_samples: u32,
    pub(super) carrier_last_delivery_at: Option<Instant>,
    pub(super) carrier_app_limited: bool,
}

impl Default for ClientPathObservation {
    fn default() -> Self {
        Self {
            state: SchedulerPathState::Suspect,
            manual_disabled: false,
            measured_srtt_ms: None,
            measured_jitter_ms: None,
            measured_rate_bps: None,
            measured_loss_rate: None,
            measured_mtu_payload_bytes: None,
            delivery_samples: 0,
            last_delivery_at: None,
            active_flows: 0,
            active_latency_sensitive_flows: 0,
            relay_bytes_in_flight: 0,
            relay_queue_bytes: 0,
            carrier_srtt_ms: None,
            carrier_rttvar_ms: None,
            carrier_delivery_rate_bps: None,
            carrier_bytes_in_flight: 0,
            carrier_queue_bytes: 0,
            carrier_inflight_limit_bytes: 0,
            carrier_delivery_samples: 0,
            carrier_last_delivery_at: None,
            carrier_app_limited: true,
        }
    }
}

impl ClientPathHealthRecord {
    pub(super) fn observe(&mut self, now: Instant) -> ClientPathObservation {
        if self.manual_disabled {
            return ClientPathObservation {
                state: SchedulerPathState::Failed,
                manual_disabled: true,
                measured_srtt_ms: self.measured_srtt_ms,
                measured_jitter_ms: self.measured_jitter_ms,
                measured_rate_bps: self.measured_rate_bps,
                measured_loss_rate: self.measured_loss_rate,
                measured_mtu_payload_bytes: self.measured_mtu_payload_bytes,
                delivery_samples: self.delivery_samples,
                last_delivery_at: self.last_delivery_at,
                active_flows: self.active_flows,
                active_latency_sensitive_flows: self.active_latency_sensitive_flows,
                relay_bytes_in_flight: self.relay_bytes_in_flight,
                relay_queue_bytes: self.relay_queue_bytes,
                carrier_srtt_ms: self.carrier_srtt_ms,
                carrier_rttvar_ms: self.carrier_rttvar_ms,
                carrier_delivery_rate_bps: self.carrier_delivery_rate_bps,
                carrier_bytes_in_flight: self.carrier_bytes_in_flight,
                carrier_queue_bytes: self.carrier_queue_bytes,
                carrier_inflight_limit_bytes: self.carrier_inflight_limit_bytes,
                carrier_delivery_samples: self.carrier_delivery_samples,
                carrier_last_delivery_at: self.carrier_last_delivery_at,
                carrier_app_limited: self.carrier_app_limited,
            };
        }
        if self.state == SchedulerPathState::Failed
            && self.failed_until.is_some_and(|deadline| now >= deadline)
        {
            self.state = SchedulerPathState::Suspect;
            self.failed_until = None;
        }
        ClientPathObservation {
            state: self.state,
            manual_disabled: false,
            measured_srtt_ms: self.measured_srtt_ms,
            measured_jitter_ms: self.measured_jitter_ms,
            measured_rate_bps: self.measured_rate_bps,
            measured_loss_rate: self.measured_loss_rate,
            measured_mtu_payload_bytes: self.measured_mtu_payload_bytes,
            delivery_samples: self.delivery_samples,
            last_delivery_at: self.last_delivery_at,
            active_flows: self.active_flows,
            active_latency_sensitive_flows: self.active_latency_sensitive_flows,
            relay_bytes_in_flight: self.relay_bytes_in_flight,
            relay_queue_bytes: self.relay_queue_bytes,
            carrier_srtt_ms: self.carrier_srtt_ms,
            carrier_rttvar_ms: self.carrier_rttvar_ms,
            carrier_delivery_rate_bps: self.carrier_delivery_rate_bps,
            carrier_bytes_in_flight: self.carrier_bytes_in_flight,
            carrier_queue_bytes: self.carrier_queue_bytes,
            carrier_inflight_limit_bytes: self.carrier_inflight_limit_bytes,
            carrier_delivery_samples: self.carrier_delivery_samples,
            carrier_last_delivery_at: self.carrier_last_delivery_at,
            carrier_app_limited: self.carrier_app_limited,
        }
    }

    pub(super) fn mark_success(&mut self, elapsed: Duration) {
        if self.manual_disabled {
            return;
        }
        self.mark_liveness_success();
        let sample_ms = elapsed.as_secs_f64() * 1000.0;
        self.measured_srtt_ms = Some(match self.measured_srtt_ms {
            Some(previous) => previous.mul_add(0.875, sample_ms * 0.125),
            None => sample_ms,
        });
    }

    pub(super) fn mark_liveness_success(&mut self) {
        if self.manual_disabled {
            return;
        }
        self.state = SchedulerPathState::Active;
        self.consecutive_failures = 0;
        self.failed_until = None;
    }

    pub(super) fn mark_open_success(&mut self, _elapsed: Duration, lane: FlowLane) {
        self.mark_liveness_success();
        self.active_flows = self.active_flows.saturating_add(1);
        if tcp_relay_expects_interactive_response(lane) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
    }

    pub(super) fn reserve_load(&mut self, lane: FlowLane) {
        self.active_flows = self.active_flows.saturating_add(1);
        if tcp_relay_expects_interactive_response(lane) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
    }

    pub(super) fn mark_reserved_open_success(&mut self, _elapsed: Duration) {
        self.mark_liveness_success();
    }

    pub(super) fn release_load(&mut self, lane: FlowLane) {
        self.active_flows = self.active_flows.saturating_sub(1);
        if tcp_relay_expects_interactive_response(lane) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_sub(1);
        }
    }

    pub(super) fn change_lane_load(&mut self, from: FlowLane, to: FlowLane) {
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
        if self.manual_disabled {
            return;
        }
        self.state = SchedulerPathState::Active;
        self.consecutive_failures = 0;
        self.failed_until = None;
        self.delivery_samples = self.delivery_samples.saturating_add(1);
        self.last_delivery_at = Some(Instant::now());
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

    pub(super) fn mark_udp_carrier_metrics(&mut self, metrics: udp_carrier::UdpCarrierPathMetrics) {
        if self.manual_disabled {
            return;
        }
        self.state = SchedulerPathState::Active;
        self.consecutive_failures = 0;
        self.failed_until = None;
        if metrics.min_rtt_observed {
            self.carrier_srtt_ms = Some(metrics.srtt.as_secs_f64() * 1000.0);
            self.carrier_rttvar_ms = Some(metrics.rttvar.as_secs_f64() * 1000.0);
        }
        if metrics.delivery_sample_count > 0 {
            self.carrier_delivery_rate_bps = Some(metrics.delivery_rate_bps.max(1.0));
            self.carrier_delivery_samples =
                u32::try_from(metrics.delivery_sample_count).unwrap_or(u32::MAX);
            self.carrier_last_delivery_at = metrics.last_delivery_sample_at;
        }
        self.carrier_bytes_in_flight = metrics.bytes_in_flight as u64;
        self.carrier_queue_bytes = metrics
            .pending_bytes
            .saturating_sub(metrics.bytes_in_flight) as u64;
        self.carrier_inflight_limit_bytes = metrics.inflight_hi as u64;
        self.carrier_app_limited = metrics.app_limited;
    }

    pub(super) fn mark_failure(&mut self, now: Instant, has_schedulable_alternative: bool) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.relay_bytes_in_flight = 0;
        self.relay_queue_bytes = 0;
        if self.consecutive_failures == 1 || !has_schedulable_alternative {
            self.state = SchedulerPathState::Suspect;
            self.failed_until = None;
        } else {
            self.state = SchedulerPathState::Failed;
            self.failed_until = Some(now + PATH_FAILURE_COOLDOWN);
        }
    }

    pub(super) fn mark_data_plane_failure(
        &mut self,
        now: Instant,
        has_schedulable_alternative: bool,
    ) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.relay_bytes_in_flight = 0;
        self.relay_queue_bytes = 0;
        if has_schedulable_alternative {
            self.state = SchedulerPathState::Failed;
            self.failed_until = Some(now + PATH_FAILURE_COOLDOWN);
        } else {
            self.state = SchedulerPathState::Suspect;
            self.failed_until = None;
        }
    }

    pub(super) fn record_relay_send(&mut self, bytes: usize) {
        self.relay_bytes_in_flight = self.relay_bytes_in_flight.saturating_add(bytes as u64);
    }

    pub(super) fn release_relay_inflight(&mut self, bytes: usize) {
        self.relay_bytes_in_flight = self.relay_bytes_in_flight.saturating_sub(bytes as u64);
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

    #[cfg(test)]
    pub fn new_with_proxy_auth(
        paths: Vec<PathSpec>,
        security: SecurityConfig,
        resources: ResourceLimits,
        proxy_auth: ProxyAuthConfig,
    ) -> Result<Self, RuntimeError> {
        let paths = paths
            .into_iter()
            .map(|spec| ClientPathConfig {
                spec,
                security: security.clone(),
            })
            .collect();
        Self::new_with_path_configs_and_target(paths, resources, proxy_auth, None, Vec::new())
    }

    pub fn new_with_path_configs_and_target(
        paths: Vec<ClientPathConfig>,
        resources: ResourceLimits,
        proxy_auth: ProxyAuthConfig,
        route_target: Option<RouteTarget>,
        ingresses: Vec<LocalIngressConfig>,
    ) -> Result<Self, RuntimeError> {
        if paths.len() > u16::MAX as usize {
            return Err(RuntimeError::PathIdOverflow);
        }
        let tcp_paths = paths
            .iter()
            .filter(|path| path.spec.underlay == UnderlayProtocol::Tcp)
            .map(|path| path.spec.clone())
            .collect::<Vec<_>>();
        let tcp_security = paths
            .iter()
            .filter(|path| path.spec.underlay == UnderlayProtocol::Tcp)
            .map(|path| path.security.clone())
            .collect::<Vec<_>>();
        let udp_paths = paths
            .into_iter()
            .filter(|path| path.spec.underlay == UnderlayProtocol::Udp)
            .collect::<Vec<_>>();
        let udp_security = udp_paths
            .iter()
            .map(|path| path.security.clone())
            .collect::<Vec<_>>();
        let udp_paths = udp_paths
            .into_iter()
            .map(|path| path.spec)
            .collect::<Vec<_>>();
        let health = Arc::new(Mutex::new(ClientPathHealth {
            tcp: vec![ClientPathHealthRecord::default(); tcp_paths.len()],
            udp: vec![ClientPathHealthRecord::default(); udp_paths.len()],
        }));
        let codec_limits = resources.into();
        let mux_limits = resources.into();
        let session_id = random_session_id()?;
        let reuse_tcp_latency_sessions = tcp_paths.len() > 1;
        let tcp_sessions = tcp_paths
            .iter()
            .cloned()
            .enumerate()
            .map(|(path_index, path)| {
                ClientTcpPathSessionHandle::new(ClientTcpPathSessionRuntime {
                    path,
                    path_index,
                    session_id,
                    security: tcp_security[path_index].clone(),
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
                    session_id,
                    security: udp_security[path_index].clone(),
                    codec_limits,
                    mux_limits,
                    stream_frame_queue: tcp_stream_frame_queue(mux_limits),
                    health: health.clone(),
                })
            })
            .collect::<Vec<_>>();
        #[cfg(not(test))]
        let _ = proxy_auth;
        Ok(Self {
            route_target,
            ingresses: Arc::new(ingresses),
            tcp_paths: Arc::new(tcp_paths),
            udp_paths: Arc::new(udp_paths),
            tcp_security: Arc::new(tcp_security),
            tcp_sessions: Arc::new(tcp_sessions),
            udp_sessions: Arc::new(udp_sessions),
            next_tcp_stream_id: Arc::new(Mutex::new(0)),
            health,
            codec_limits,
            mux_limits,
            #[cfg(test)]
            proxy_auth,
        })
    }

    pub(super) fn tcp_path_security(
        &self,
        path_index: usize,
    ) -> Result<&SecurityConfig, RuntimeError> {
        self.tcp_security
            .get(path_index)
            .ok_or(RuntimeError::NoSchedulableTcpPath)
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
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Vec<usize> {
        let observations = self.tcp_health_observations_for_lane(lane);
        if reliable_stream_latency_startup_should_use_configured_order(
            &self.tcp_paths,
            &observations,
            lane,
        ) {
            return configured_order_path_indices(
                &self.tcp_paths,
                &observations,
                lane,
                payload_bytes,
            );
        }
        ordered_reliable_path_indices(&self.tcp_paths, &observations, lane, payload_bytes)
    }

    pub(super) fn reserve_tcp_path_load(&self, index: usize, lane: FlowLane) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.reserve_load(lane);
        }
    }

    pub(super) fn reserve_udp_stream_path_load(&self, index: usize, lane: FlowLane) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.reserve_load(lane);
        }
    }

    pub(super) fn reserve_reliable_stream_path(
        &self,
        lane: FlowLane,
        payload_bytes: usize,
        excluded: &[RelayPathKey],
    ) -> Option<RelayPathKey> {
        let mut health = self.health.lock().expect("client path health lock");
        let mut tcp_observations = health_observations(&mut health.tcp);
        apply_tcp_bulk_isolation(&mut tcp_observations, lane, self.mux_limits);
        let udp_observations = health_observations(&mut health.udp);
        let mut candidates = reliable_stream_path_candidates(
            &self.tcp_paths,
            &tcp_observations,
            &self.udp_paths,
            &udp_observations,
            lane,
            payload_bytes,
        );
        candidates.retain(|candidate| !excluded.contains(&candidate.key));
        candidates.sort_by(|left, right| {
            left.eta_ms
                .total_cmp(&right.eta_ms)
                .then_with(|| {
                    reliable_stream_initial_underlay_order(lane, left.key.underlay).cmp(
                        &reliable_stream_initial_underlay_order(lane, right.key.underlay),
                    )
                })
                .then_with(|| left.key.index.cmp(&right.key.index))
        });
        let selected = candidates.first()?.key;
        match selected.underlay {
            UnderlayProtocol::Tcp => health.tcp.get_mut(selected.index)?.reserve_load(lane),
            UnderlayProtocol::Udp => health.udp.get_mut(selected.index)?.reserve_load(lane),
        }
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "reliable_stream_initial_path_selected",
            format_args!(
                "lane={:?} payload_bytes={} path_underlay={:?} path_index={} candidate_count={}",
                lane,
                payload_bytes,
                selected.underlay,
                selected.index,
                candidates.len(),
            ),
        );
        Some(selected)
    }

    pub(super) fn ordered_tcp_repair_path_indices(
        &self,
        current_path_index: Option<usize>,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Vec<usize> {
        let observations = self.tcp_health_observations_for_lane(lane);
        let scores = ordered_path_scores(&self.tcp_paths, &observations, lane, payload_bytes);
        if !matches!(lane, FlowLane::Throughput | FlowLane::Background) {
            return scores.into_iter().map(|(index, _)| index).collect();
        }
        let current_eta = current_path_index.and_then(|current_path_index| {
            scores
                .iter()
                .find_map(|(index, eta)| (*index == current_path_index).then_some(*eta))
        });
        let has_active_survivor = scores.iter().any(|(index, _)| {
            Some(*index) != current_path_index
                && observations
                    .get(*index)
                    .is_some_and(|observation| observation.state == SchedulerPathState::Active)
        });
        scores
            .into_iter()
            .filter(|(index, eta)| {
                Some(*index) != current_path_index
                    && current_eta.is_none_or(|current| *eta < current)
                    && (!has_active_survivor
                        || observations.get(*index).is_some_and(|observation| {
                            observation.state == SchedulerPathState::Active
                        }))
            })
            .map(|(index, _)| index)
            .collect()
    }

    pub(super) fn ordered_udp_stream_repair_path_indices(
        &self,
        current_path_index: Option<usize>,
        lane: FlowLane,
        payload_bytes: usize,
        require_delivery_evidence: bool,
    ) -> Vec<usize> {
        let observations =
            health_observations(&mut self.health.lock().expect("client path health lock").udp);
        let scores = if reliable_stream_latency_startup_should_use_configured_order(
            &self.udp_paths,
            &observations,
            lane,
        ) {
            configured_order_path_scores(&self.udp_paths, &observations, lane, payload_bytes)
        } else {
            ordered_path_scores(&self.udp_paths, &observations, lane, payload_bytes)
        };
        scores
            .into_iter()
            .filter(|(index, _)| Some(*index) != current_path_index)
            .filter(|(index, _)| {
                !matches!(lane, FlowLane::Throughput | FlowLane::Background)
                    || !observations
                        .iter()
                        .enumerate()
                        .any(|(candidate, observation)| {
                            Some(candidate) != current_path_index
                                && observation.state == SchedulerPathState::Active
                        })
                    || observations
                        .get(*index)
                        .is_some_and(|observation| observation.state == SchedulerPathState::Active)
            })
            .filter(|(index, _)| {
                if !require_delivery_evidence {
                    return true;
                }
                let Some(path) = self.udp_paths.get(*index) else {
                    return false;
                };
                let observation = observations.get(*index).copied().unwrap_or_default();
                path_can_be_auto_discovered(path, observation)
                    && bulk_candidate_has_delivery_evidence(path, observation)
            })
            .map(|(index, _)| index)
            .collect()
    }

    pub(super) fn ordered_reliable_bulk_striping_path_keys(
        &self,
        payload_bytes: usize,
    ) -> Vec<RelayPathKey> {
        let mut candidates = self.ordered_reliable_bulk_path_candidates(payload_bytes);
        let has_evidence = candidates.iter().any(|candidate| candidate.has_evidence);
        let has_active_bulk_work = candidates.iter().any(bulk_candidate_has_active_bulk_work);
        if has_evidence {
            candidates.retain(|candidate| candidate.has_evidence);
        } else if has_active_bulk_work {
            candidates.retain(bulk_candidate_has_active_bulk_work);
        } else if !bulk_candidates_span_underlays(&candidates)
            && candidates
                .iter()
                .any(|candidate| candidate.snapshot.active_flows > 0)
        {
            candidates.retain(|candidate| candidate.snapshot.active_flows > 0);
        }
        candidates.sort_by(|left, right| {
            left.eta_ms
                .total_cmp(&right.eta_ms)
                .then_with(|| relay_path_key_order(left.key, right.key))
        });
        if !has_evidence && !has_active_bulk_work {
            candidates.truncate(1);
        }
        bulk_striping_admitted_cohort(candidates, payload_bytes, self.mux_limits)
            .into_iter()
            .map(|candidate| candidate.key)
            .collect()
    }

    pub(super) fn ordered_reliable_bulk_validation_path_keys(
        &self,
        payload_bytes: usize,
    ) -> Vec<RelayPathKey> {
        let payload_bytes = payload_bytes
            .min(tcp_lane_startup_chunk_bytes(
                FlowLane::Latency,
                self.mux_limits,
            ))
            .max(PATH_OPEN_SCORE_BYTES);
        let mut candidates = self.ordered_reliable_bulk_path_candidates(payload_bytes);
        candidates.sort_by(|left, right| {
            left.eta_ms
                .total_cmp(&right.eta_ms)
                .then_with(|| relay_path_key_order(left.key, right.key))
        });
        let admitted = bulk_striping_admitted_cohort(candidates, payload_bytes, self.mux_limits)
            .into_iter()
            .filter(|candidate| !candidate.has_evidence && candidate.snapshot.active_flows == 0)
            .collect::<Vec<_>>();
        carrier_diverse_bulk_validation_order(admitted)
            .into_iter()
            .map(|candidate| candidate.key)
            .collect()
    }

    fn ordered_reliable_bulk_path_candidates(
        &self,
        payload_bytes: usize,
    ) -> Vec<BulkPathCandidate> {
        let mut health = self.health.lock().expect("client path health lock");
        let tcp_observations = health_observations(&mut health.tcp);
        let udp_observations = health_observations(&mut health.udp);
        let scoring_payload_bytes =
            bulk_service_horizon_payload_bytes(payload_bytes, self.mux_limits);
        ordered_path_scores(
            &self.tcp_paths,
            &tcp_observations,
            FlowLane::Throughput,
            scoring_payload_bytes,
        )
        .into_iter()
        .filter_map(|(index, eta_ms)| {
            let path = self.tcp_paths.get(index)?;
            let observation = tcp_observations.get(index).copied().unwrap_or_default();
            let snapshot = path_snapshot(path, index, observation);
            path_can_be_auto_discovered(path, observation).then_some(BulkPathCandidate {
                key: RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index,
                },
                eta_ms,
                has_evidence: bulk_candidate_has_evidence(path, observation),
                snapshot,
            })
        })
        .chain(
            ordered_path_scores(
                &self.udp_paths,
                &udp_observations,
                FlowLane::Throughput,
                scoring_payload_bytes,
            )
            .into_iter()
            .filter_map(|(index, eta_ms)| {
                let path = self.udp_paths.get(index)?;
                let observation = udp_observations.get(index).copied().unwrap_or_default();
                let snapshot = path_snapshot(path, index, observation);
                path_can_be_auto_discovered(path, observation).then_some(BulkPathCandidate {
                    key: RelayPathKey {
                        underlay: UnderlayProtocol::Udp,
                        index,
                    },
                    eta_ms: eta_ms
                        + udp_reliable_stream_loss_repair_penalty_ms(snapshot, payload_bytes),
                    has_evidence: bulk_candidate_has_evidence(path, observation),
                    snapshot,
                })
            }),
        )
        .collect::<Vec<_>>()
    }

    pub(super) fn tcp_health_observations_for_lane(
        &self,
        lane: FlowLane,
    ) -> Vec<ClientPathObservation> {
        let mut observations =
            health_observations(&mut self.health.lock().expect("client path health lock").tcp);
        apply_tcp_bulk_isolation(&mut observations, lane, self.mux_limits);
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

    pub(super) fn relay_path_metrics(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
    ) -> Option<PathMetrics> {
        let (path, observation) = match underlay {
            UnderlayProtocol::Tcp => {
                let path = self.tcp_paths.get(index)?;
                let observation = self
                    .health
                    .lock()
                    .expect("client path health lock")
                    .tcp
                    .get_mut(index)?
                    .observe(Instant::now());
                (path, observation)
            }
            UnderlayProtocol::Udp => {
                let path = self.udp_paths.get(index)?;
                let observation = self
                    .health
                    .lock()
                    .expect("client path health lock")
                    .udp
                    .get_mut(index)?
                    .observe(Instant::now());
                (path, observation)
            }
        };
        let snapshot = path_snapshot(path, index, observation);
        Some(path_metrics_from_snapshot(
            snapshot,
            observation,
            PathMetricDirection::ClientToServer,
        ))
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
                FlowLane::RealtimeDatagram,
                payload_bytes,
            )
            .into_iter()
            .find_map(|path_index| {
                let path = self.udp_paths.get(path_index)?;
                let observation = observations.get(path_index).copied()?;
                let eta_ms = scheduler::score_path(
                    path_snapshot(path, path_index, observation),
                    FlowLane::RealtimeDatagram,
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
            FlowLane::RealtimeDatagram,
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
        }
        let score = scheduler::score_path(
            path_snapshot(path, index, observation),
            FlowLane::RealtimeDatagram,
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
            FlowLane::RealtimeDatagram,
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
        lane: FlowLane,
    ) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.mark_open_success(elapsed, lane);
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

    pub(super) fn release_tcp_path_load(&self, index: usize, lane: FlowLane) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.release_load(lane);
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

    pub(super) fn release_udp_stream_path_load(&self, index: usize, lane: FlowLane) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.release_load(lane);
        }
    }

    pub(super) fn mark_relay_path_failure(&self, underlay: UnderlayProtocol, index: usize) {
        match underlay {
            UnderlayProtocol::Tcp => self.mark_tcp_path_failure(index),
            UnderlayProtocol::Udp => self.mark_udp_path_failure(index),
        }
    }

    pub(super) fn mark_relay_path_data_plane_failure(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
    ) {
        match underlay {
            UnderlayProtocol::Tcp => self.mark_tcp_path_data_plane_failure(index),
            UnderlayProtocol::Udp => self.mark_udp_path_data_plane_failure(index),
        }
    }

    pub(super) fn release_relay_path_load(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        lane: FlowLane,
    ) {
        match underlay {
            UnderlayProtocol::Tcp => self.release_tcp_path_load(index, lane),
            UnderlayProtocol::Udp => self.release_udp_stream_path_load(index, lane),
        }
    }

    pub(super) fn record_relay_path_send(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        bytes: usize,
    ) {
        if bytes == 0 {
            return;
        }
        let mut health = self.health.lock().expect("client path health lock");
        let records = match underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        if let Some(current) = records.get_mut(index) {
            current.record_relay_send(bytes);
        }
    }

    pub(super) fn relay_path_has_bulk_model_evidence(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
    ) -> bool {
        let mut health = self.health.lock().expect("client path health lock");
        match underlay {
            UnderlayProtocol::Tcp => {
                let Some(path) = self.tcp_paths.get(index) else {
                    return false;
                };
                health
                    .tcp
                    .get_mut(index)
                    .map(|record| {
                        bulk_candidate_has_delivery_evidence(path, record.observe(Instant::now()))
                    })
                    .unwrap_or(false)
            }
            UnderlayProtocol::Udp => {
                let Some(path) = self.udp_paths.get(index) else {
                    return false;
                };
                health
                    .udp
                    .get_mut(index)
                    .map(|record| {
                        bulk_candidate_has_delivery_evidence(path, record.observe(Instant::now()))
                    })
                    .unwrap_or(false)
            }
        }
    }

    pub(super) fn release_relay_path_inflight(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        bytes: usize,
    ) {
        if bytes == 0 {
            return;
        }
        let mut health = self.health.lock().expect("client path health lock");
        let records = match underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        if let Some(current) = records.get_mut(index) {
            current.release_relay_inflight(bytes);
        }
    }

    pub(super) fn change_relay_path_lane_load(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        from: FlowLane,
        to: FlowLane,
    ) {
        if from == to {
            return;
        }
        let mut health = self.health.lock().expect("client path health lock");
        match underlay {
            UnderlayProtocol::Tcp => {
                if let Some(current) = health.tcp.get_mut(index) {
                    current.change_lane_load(from, to);
                }
            }
            UnderlayProtocol::Udp => {
                if let Some(current) = health.udp.get_mut(index) {
                    current.change_lane_load(from, to);
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

    pub(super) fn mark_relay_path_rate_sample(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        sample: PathRateSample,
    ) {
        let mut health = self.health.lock().expect("client path health lock");
        let records = match underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        if let Some(current) = records.get_mut(index) {
            current.mark_delivery(sample);
        }
    }

    pub(super) fn relay_path_has_delivery_sample(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
    ) -> bool {
        let health = self.health.lock().expect("client path health lock");
        let observation = match underlay {
            UnderlayProtocol::Tcp => health.tcp.get(index),
            UnderlayProtocol::Udp => health.udp.get(index),
        };
        observation.is_some_and(|record| {
            record.delivery_samples > 0 || record.carrier_delivery_samples > 0
        })
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

    pub(super) fn mark_tcp_path_data_plane_failure(&self, index: usize) {
        let now = Instant::now();
        let mut health = self.health.lock().expect("client path health lock");
        let has_schedulable_alternative =
            path_records_have_schedulable_alternative(&mut health.tcp, index, now);
        if let Some(current) = health.tcp.get_mut(index) {
            current.mark_data_plane_failure(now, has_schedulable_alternative);
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
            current.mark_open_success(elapsed, FlowLane::RealtimeDatagram);
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
            current.release_load(FlowLane::RealtimeDatagram);
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

    pub(super) fn mark_udp_path_data_plane_failure(&self, index: usize) {
        let now = Instant::now();
        let mut health = self.health.lock().expect("client path health lock");
        let has_schedulable_alternative =
            path_records_have_schedulable_alternative(&mut health.udp, index, now);
        if let Some(current) = health.udp.get_mut(index) {
            current.mark_data_plane_failure(now, has_schedulable_alternative);
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
    observation.active_flows == 0
}

pub(super) fn apply_tcp_bulk_isolation(
    observations: &mut [ClientPathObservation],
    lane: FlowLane,
    mux_limits: MuxLimits,
) {
    if !matches!(lane, FlowLane::Throughput | FlowLane::Background) {
        return;
    }
    if !observations
        .iter()
        .any(|observation| observation.measured_rate_bps.is_some())
    {
        return;
    }
    let isolation_bytes =
        adaptive_tcp_relay_inflight_bytes(None, FlowLane::Latency, mux_limits) as u64;
    for observation in observations {
        let latency_flows = u64::from(observation.active_latency_sensitive_flows);
        observation.relay_queue_bytes = observation
            .relay_queue_bytes
            .saturating_add(latency_flows.saturating_mul(isolation_bytes));
    }
}

pub(super) fn reliable_stream_latency_startup_should_use_configured_order(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
) -> bool {
    tcp_relay_expects_interactive_response(lane)
        && paths.iter().all(path_is_endpoint_only)
        && (!endpoint_only_startup_has_latency_sensitive_load(observations)
            || endpoint_only_startup_has_bulk_load(observations))
}

pub(super) fn reliable_stream_latency_startup_should_use_load_balanced_order(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
) -> bool {
    tcp_relay_expects_interactive_response(lane)
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
        .any(|observation| observation.active_flows > 0)
}

pub(super) fn endpoint_only_startup_has_bulk_load(observations: &[ClientPathObservation]) -> bool {
    observations
        .iter()
        .any(|observation| observation.active_flows > observation.active_latency_sensitive_flows)
}

pub(super) fn endpoint_only_tcp_startup_should_spread_bulk_load(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
    active_udp_work: bool,
) -> bool {
    tcp_relay_expects_interactive_response(lane)
        && paths.iter().all(path_is_endpoint_only)
        && endpoint_only_startup_has_any_load(observations)
        && endpoint_only_startup_has_bulk_load(observations)
        && !endpoint_only_startup_has_latency_sensitive_load(observations)
        && !active_udp_work
}

pub(super) fn ordered_reliable_path_indices(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<usize> {
    reliable_stream_startup_path_scores(paths, observations, lane, payload_bytes)
        .into_iter()
        .map(|(index, _)| index)
        .collect()
}

pub(super) fn reliable_stream_startup_path_scores(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<(usize, f64)> {
    if reliable_stream_latency_startup_should_use_configured_order(paths, observations, lane) {
        return configured_order_path_scores(paths, observations, lane, payload_bytes);
    }
    if reliable_stream_latency_startup_should_use_load_balanced_order(paths, observations, lane) {
        return endpoint_only_reliable_startup_path_scores(
            paths,
            observations,
            lane,
            payload_bytes,
        );
    }
    ordered_path_scores(paths, observations, lane, payload_bytes)
}

fn reliable_stream_mixed_startup_path_scores(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<(usize, f64)> {
    if reliable_stream_latency_startup_should_use_configured_order(paths, observations, lane)
        || reliable_stream_latency_startup_should_use_load_balanced_order(paths, observations, lane)
    {
        return endpoint_only_reliable_startup_path_scores(
            paths,
            observations,
            lane,
            payload_bytes,
        );
    }
    ordered_path_scores(paths, observations, lane, payload_bytes)
}

pub(super) fn endpoint_only_reliable_startup_path_scores(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<(usize, f64)> {
    let observations = observations
        .iter()
        .copied()
        .map(|observation| ClientPathObservation {
            measured_srtt_ms: None,
            measured_jitter_ms: None,
            measured_rate_bps: None,
            measured_loss_rate: None,
            measured_mtu_payload_bytes: observation.measured_mtu_payload_bytes,
            delivery_samples: 0,
            last_delivery_at: None,
            carrier_srtt_ms: None,
            carrier_rttvar_ms: None,
            carrier_delivery_rate_bps: None,
            carrier_delivery_samples: 0,
            carrier_last_delivery_at: None,
            ..observation
        })
        .collect::<Vec<_>>();
    ordered_path_scores(paths, &observations, lane, payload_bytes)
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
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<usize> {
    configured_order_path_scores(paths, observations, lane, payload_bytes)
        .into_iter()
        .map(|(index, _)| index)
        .collect()
}

pub(super) fn configured_order_path_scores(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<(usize, f64)> {
    paths
        .iter()
        .enumerate()
        .filter_map(|(index, path)| {
            let observation = observations.get(index).copied().unwrap_or_default();
            scheduler::score_path(
                path_snapshot(path, index, observation),
                lane,
                payload_bytes,
                SchedulerPolicy::default(),
            )
            .map(|score| (index, score.eta_ms))
        })
        .collect()
}

pub(super) fn ordered_path_scores_for_ttl(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    lane: FlowLane,
    payload_bytes: usize,
    ttl_ms: u32,
) -> Vec<(usize, f64)> {
    let scores = ordered_path_scores(paths, observations, lane, payload_bytes);
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
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<(usize, f64)> {
    let mut scores = paths
        .iter()
        .enumerate()
        .filter_map(|(index, path)| {
            let observation = observations.get(index).copied().unwrap_or_default();
            scheduler::score_path(
                path_snapshot(path, index, observation),
                lane,
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

fn reliable_stream_path_candidates(
    tcp_paths: &[PathSpec],
    tcp_observations: &[ClientPathObservation],
    udp_paths: &[PathSpec],
    udp_observations: &[ClientPathObservation],
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<BulkPathCandidate> {
    let active_udp_work = endpoint_only_startup_has_any_load(udp_observations);
    let tcp_scores = if endpoint_only_tcp_startup_should_spread_bulk_load(
        tcp_paths,
        tcp_observations,
        lane,
        active_udp_work,
    ) {
        endpoint_only_reliable_startup_path_scores(tcp_paths, tcp_observations, lane, payload_bytes)
    } else {
        reliable_stream_mixed_startup_path_scores(tcp_paths, tcp_observations, lane, payload_bytes)
    };
    let udp_scores =
        reliable_stream_mixed_startup_path_scores(udp_paths, udp_observations, lane, payload_bytes);

    tcp_scores
        .into_iter()
        .filter_map(|(index, eta_ms)| {
            let path = tcp_paths.get(index)?;
            let observation = tcp_observations.get(index).copied().unwrap_or_default();
            let snapshot = path_snapshot(path, index, observation);
            path_can_be_auto_discovered_for_lane(path, observation, lane).then_some(
                BulkPathCandidate {
                    key: RelayPathKey {
                        underlay: UnderlayProtocol::Tcp,
                        index,
                    },
                    eta_ms: eta_ms
                        + reliable_stream_initial_lane_protection_penalty(snapshot, lane),
                    has_evidence: bulk_candidate_has_evidence(path, observation),
                    snapshot,
                },
            )
        })
        .chain(udp_scores.into_iter().filter_map(|(index, eta_ms)| {
            let path = udp_paths.get(index)?;
            let observation = udp_observations.get(index).copied().unwrap_or_default();
            let snapshot = path_snapshot(path, index, observation);
            let eta_ms = eta_ms
                + if matches!(lane, FlowLane::Throughput | FlowLane::Background) {
                    udp_reliable_stream_loss_repair_penalty_ms(snapshot, payload_bytes)
                } else {
                    0.0
                }
                + reliable_stream_initial_lane_protection_penalty(snapshot, lane);
            path_can_be_auto_discovered_for_lane(path, observation, lane).then_some(
                BulkPathCandidate {
                    key: RelayPathKey {
                        underlay: UnderlayProtocol::Udp,
                        index,
                    },
                    eta_ms,
                    has_evidence: bulk_candidate_has_evidence(path, observation),
                    snapshot,
                },
            )
        }))
        .collect()
}

fn reliable_stream_initial_underlay_order(lane: FlowLane, underlay: UnderlayProtocol) -> u8 {
    match (lane, underlay) {
        (
            FlowLane::Throughput | FlowLane::Background | FlowLane::RealtimeDatagram,
            UnderlayProtocol::Udp,
        ) => 0,
        (
            FlowLane::Throughput | FlowLane::Background | FlowLane::RealtimeDatagram,
            UnderlayProtocol::Tcp,
        ) => 1,
        (_, UnderlayProtocol::Tcp) => 0,
        (_, UnderlayProtocol::Udp) => 1,
    }
}

fn reliable_stream_initial_lane_protection_penalty(snapshot: PathSnapshot, lane: FlowLane) -> f64 {
    if snapshot.underlay == UnderlayProtocol::Udp
        && matches!(lane, FlowLane::Control | FlowLane::Latency)
        && snapshot.active_latency_sensitive_flows > 0
    {
        snapshot.srtt_ms.max(1.0)
    } else {
        0.0
    }
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
        .carrier_delivery_rate_bps
        .or(observation.measured_rate_bps)
        .unwrap_or(hinted_delivery_rate_bps)
        .max(1.0);
    let srtt_ms = path_model_srtt_ms(path, observation);
    let jitter_ms = observation
        .carrier_rttvar_ms
        .or(observation.measured_jitter_ms)
        .unwrap_or_else(|| f64::from(path.metadata.initial_jitter_ms.unwrap_or(0)));
    let confidence = path_model_confidence(observation);
    let bdp_bytes = (delivery_rate_bps / 8.0 * srtt_ms.max(1.0) / 1000.0)
        .ceil()
        .max(PATH_OPEN_SCORE_BYTES as f64) as u64;
    let pacing_rate_bps = delivery_rate_bps
        * (1.0
            - observation
                .measured_loss_rate
                .unwrap_or(0.0)
                .clamp(0.0, 0.75)
                * 0.5)
            .clamp(0.25, 1.0);
    let inflight_limit_bytes = if observation.carrier_inflight_limit_bytes > 0 {
        observation.carrier_inflight_limit_bytes
    } else {
        bdp_bytes
            .saturating_mul(2)
            .max(PATH_OPEN_SCORE_BYTES as u64)
    };
    PathSnapshot {
        id: PathId(index as u16),
        underlay: path.underlay,
        state: observation.state,
        flags: path.metadata.capabilities.into(),
        srtt_ms,
        jitter_ms,
        delivery_rate_bps,
        loss_rate: observation.measured_loss_rate.unwrap_or(0.0),
        queue_bytes: observation.carrier_queue_bytes,
        product_queue_bytes: observation.relay_queue_bytes,
        bytes_in_flight: observation
            .relay_bytes_in_flight
            .saturating_add(observation.carrier_bytes_in_flight),
        product_bytes_in_flight: observation.relay_bytes_in_flight,
        active_flows: observation.active_flows,
        active_latency_sensitive_flows: observation.active_latency_sensitive_flows,
        pacing_rate_bps,
        inflight_limit_bytes,
        confidence,
        app_limited: observation.relay_bytes_in_flight == 0
            && observation.carrier_bytes_in_flight == 0
            && observation.carrier_app_limited,
    }
}

pub(super) fn path_metrics_from_snapshot(
    snapshot: PathSnapshot,
    observation: ClientPathObservation,
    direction: PathMetricDirection,
) -> PathMetrics {
    let data_sample_count = observation
        .delivery_samples
        .saturating_add(observation.carrier_delivery_samples);
    PathMetrics {
        path_id: snapshot.id,
        underlay: snapshot.underlay,
        direction,
        metric_epoch: metric_epoch_now(),
        metric_age_us: 0,
        min_rtt_us: millis_to_micros_u32(snapshot.srtt_ms),
        srtt_us: millis_to_micros_u32(snapshot.srtt_ms),
        rttvar_us: millis_to_micros_u32(snapshot.jitter_ms.max(0.0)),
        jitter_us: millis_to_micros_u32(snapshot.jitter_ms.max(0.0)),
        delivery_rate_bps: snapshot.delivery_rate_bps.max(1.0).round() as u64,
        pacing_rate_bps: snapshot.pacing_rate_bps.max(1.0).round() as u64,
        loss_ppm: (snapshot.loss_rate.clamp(0.0, 1.0) * 1_000_000.0).round() as u32,
        ecn_ppm: 0,
        bytes_in_flight: snapshot.bytes_in_flight,
        queue_bytes: snapshot.queue_bytes,
        inflight_limit_bytes: snapshot.inflight_limit_bytes,
        inflight_hi_bytes: snapshot.inflight_limit_bytes,
        confidence_ppm: ratio_to_ppm(snapshot.confidence),
        app_limited: snapshot.app_limited,
        has_ack_derived_data_sample: data_sample_count > 0,
        data_sample_count,
    }
}

pub(super) fn metric_epoch_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

pub(super) fn ratio_to_ppm(value: f64) -> u32 {
    (value.clamp(0.0, 1.0) * 1_000_000.0).round() as u32
}

fn millis_to_micros_u32(ms: f64) -> u32 {
    let micros = (ms.max(0.0) * 1000.0).round();
    micros.clamp(0.0, f64::from(u32::MAX)) as u32
}

pub(super) fn path_model_srtt_ms(path: &PathSpec, observation: ClientPathObservation) -> f64 {
    observation
        .carrier_srtt_ms
        .or(observation.measured_srtt_ms)
        .unwrap_or_else(|| {
            path.metadata
                .initial_srtt_ms
                .map_or_else(|| default_path_srtt_ms(path.underlay), f64::from)
        })
}

pub(super) fn path_model_confidence(observation: ClientPathObservation) -> f64 {
    let delivery_confidence = (f64::from(
        observation
            .delivery_samples
            .saturating_add(observation.carrier_delivery_samples),
    ) / 8.0)
        .clamp(0.0, 1.0);
    let rtt_confidence = if observation
        .carrier_srtt_ms
        .or(observation.measured_srtt_ms)
        .is_some()
    {
        0.35
    } else {
        0.0
    };
    let freshness_confidence = observation
        .last_delivery_at
        .into_iter()
        .chain(observation.carrier_last_delivery_at)
        .max()
        .map(|seen| {
            let age = Instant::now().saturating_duration_since(seen).as_secs_f64();
            (1.0 - age / 30.0).clamp(0.0, 1.0) * 0.25
        })
        .unwrap_or(0.0);
    (delivery_confidence + rtt_confidence + freshness_confidence).clamp(0.1, 1.0)
}

pub(super) fn udp_path_has_realtime_model(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> bool {
    observation.measured_srtt_ms.is_some()
        || observation.carrier_srtt_ms.is_some()
        || observation.measured_jitter_ms.is_some()
        || observation.carrier_rttvar_ms.is_some()
        || observation.measured_rate_bps.is_some()
        || observation.carrier_delivery_rate_bps.is_some()
        || observation.measured_loss_rate.is_some()
        || path.metadata.initial_srtt_ms.is_some()
        || path.metadata.initial_jitter_ms.is_some()
        || path.metadata.initial_rate != RateHint::Unknown
}

pub(super) fn udp_observation_has_datagram_feedback(observation: &ClientPathObservation) -> bool {
    observation.measured_jitter_ms.is_some()
        || observation.measured_loss_rate.is_some()
        || observation.measured_rate_bps.is_some()
        || observation.carrier_delivery_rate_bps.is_some()
        || observation.measured_mtu_payload_bytes.is_some()
}

pub(super) fn path_can_be_auto_discovered(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> bool {
    observation.state == SchedulerPathState::Active
        && !path.metadata.capabilities.expensive
        && !path.metadata.capabilities.backup
        && !path.metadata.capabilities.probe_only
        && path.metadata.capabilities.bulk_allowed
}

fn path_can_be_auto_discovered_for_lane(
    path: &PathSpec,
    observation: ClientPathObservation,
    lane: FlowLane,
) -> bool {
    observation.state == SchedulerPathState::Active
        && !path.metadata.capabilities.expensive
        && !path.metadata.capabilities.backup
        && !path.metadata.capabilities.probe_only
        && (!matches!(lane, FlowLane::Throughput | FlowLane::Background)
            || path.metadata.capabilities.bulk_allowed)
}

fn bulk_candidate_has_evidence(path: &PathSpec, observation: ClientPathObservation) -> bool {
    observation.delivery_samples > 0
        || observation.carrier_delivery_samples > 0
        || observation.last_delivery_at.is_some()
        || observation.carrier_last_delivery_at.is_some()
        || observation.measured_srtt_ms.is_some()
        || observation.carrier_srtt_ms.is_some()
        || observation.measured_jitter_ms.is_some()
        || observation.carrier_rttvar_ms.is_some()
        || observation.measured_rate_bps.is_some()
        || observation.carrier_delivery_rate_bps.is_some()
        || observation.measured_loss_rate.is_some()
        || path.metadata.initial_srtt_ms.is_some()
        || path.metadata.initial_jitter_ms.is_some()
        || path.metadata.initial_rate != RateHint::Unknown
}

fn bulk_candidate_has_delivery_evidence(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> bool {
    observation.delivery_samples > 0
        || observation.carrier_delivery_samples > 0
        || observation.last_delivery_at.is_some()
        || observation.carrier_last_delivery_at.is_some()
        || observation.measured_rate_bps.is_some()
        || observation.carrier_delivery_rate_bps.is_some()
        || path.metadata.initial_rate != RateHint::Unknown
}

fn bulk_candidate_has_active_bulk_work(candidate: &BulkPathCandidate) -> bool {
    candidate.snapshot.active_flows > candidate.snapshot.active_latency_sensitive_flows
}

fn bulk_candidates_span_underlays(candidates: &[BulkPathCandidate]) -> bool {
    let Some(first) = candidates.first() else {
        return false;
    };
    candidates
        .iter()
        .any(|candidate| candidate.key.underlay != first.key.underlay)
}

fn carrier_diverse_bulk_validation_order(
    candidates: Vec<BulkPathCandidate>,
) -> Vec<BulkPathCandidate> {
    if !bulk_candidates_span_underlays(&candidates) {
        return candidates;
    }
    let mut remaining = candidates;
    let mut ordered = Vec::with_capacity(remaining.len());
    for underlay in [UnderlayProtocol::Udp, UnderlayProtocol::Tcp] {
        if let Some(position) = remaining
            .iter()
            .position(|candidate| candidate.key.underlay == underlay)
        {
            ordered.push(remaining.remove(position));
        }
    }
    ordered.extend(remaining);
    ordered
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
        UnderlayProtocol::Tcp | UnderlayProtocol::Udp => 50.0,
    }
}

pub(super) fn default_path_rate_bps(underlay: UnderlayProtocol) -> f64 {
    match underlay {
        UnderlayProtocol::Tcp | UnderlayProtocol::Udp => 100_000_000.0,
    }
}

#[derive(Debug, Clone)]
pub struct ServerPathContext {
    pub(super) tag: Option<String>,
    pub(super) route_target: Option<RouteTarget>,
    pub(super) server_paths: Arc<Vec<PathSpec>>,
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
