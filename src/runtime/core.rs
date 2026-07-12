use super::bulk_admission::*;
use super::datagram::*;
use super::error::RuntimeError;
use super::ingress_runtime::*;
use super::management::*;
use super::path_commands::*;
use super::path_model::*;
use super::path_proof::PathProofObservation;
use super::prelude::*;
use super::relay_control::*;
use super::relay_io::*;
use super::relay_open::*;
use super::reliable_path::*;
use super::server_runtime::*;
use super::tcp_path::*;
use super::tun_l4::*;
use super::udp_metrics::UdpPathMetrics;
use super::udp_path::*;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use std::sync::atomic::{AtomicU32, Ordering};

pub(super) const MAX_HTTP_CONNECT_HEADER_BYTES: usize = 64 * 1024;
// RFC 9002's recommended QUIC initial congestion window is based on ten max
// datagrams. mptunnel uses the same packet-count shape as the carrier-neutral
// reliable-stream startup window instead of an arbitrary byte constant. The
// QUIC alias below is for code that specifically consumes QUIC carrier metrics.
pub(super) const TRANSPORT_MSS_BYTES: usize = 1460;
pub(super) const RELIABLE_INITIAL_WINDOW_PACKETS: usize = 10;
pub(super) const QUIC_INITIAL_WINDOW_PACKETS: usize = RELIABLE_INITIAL_WINDOW_PACKETS;
pub(super) const PATH_OPEN_SCORE_BYTES: usize =
    RELIABLE_INITIAL_WINDOW_PACKETS * TRANSPORT_MSS_BYTES;
// BBR's model explicitly separates send quantum from inflight volume. These
// values are protocol-shape constants, not mptunnel tuning knobs: send quantum
// is pacing_rate*1ms, capped at 64 KiB and floored at 2*MSS; MinPipeCwnd is
// four MSS-sized packets.
pub(super) const BBR_SEND_QUANTUM_INTERVAL: Duration = Duration::from_millis(1);
pub(super) const BBR_MAX_SEND_QUANTUM_BYTES: usize = 64 * 1024;
pub(super) const BBR_MIN_SEND_QUANTUM_PACKETS: usize = 2;
pub(super) const BBR_MIN_PIPE_CWND_PACKETS: usize = 4;
pub(super) const BBR_DEFAULT_CWND_GAIN: f64 = 2.0;
// Shared measurement floor. QUIC supplies the 1 ms precedent, but TCP product
// ACK calibration and carrier-neutral rate samples use the same timer bound.
pub(super) const TRANSPORT_TIMER_GRANULARITY: Duration = Duration::from_millis(1);
pub(super) const QUIC_TIMER_GRANULARITY: Duration = TRANSPORT_TIMER_GRANULARITY;
// Generic reliable-stream startup prior. The value follows QUIC's conservative
// initial RTT recommendation, but product/scheduler code intentionally names it
// without tying it to UDP or QUIC.
pub(super) const RELIABLE_INITIAL_RTT: Duration = Duration::from_millis(333);
pub(super) const QUIC_MAX_ACK_DELAY: Duration = Duration::from_millis(25);
pub(super) const QUIC_PERSISTENT_CONGESTION_THRESHOLD: u32 = 3;
pub(super) const UDP_PATH_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);
pub(super) const MIN_RATE_SAMPLE_BYTES: u64 = PATH_OPEN_SCORE_BYTES as u64;
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
                server.outbound_connect_timeout,
                server.security,
                server.performance,
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
            server.outbound_connect_timeout,
            server.security,
            server.performance,
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
    // Configuration ownership: local inbounds and route target describe which
    // product flows this client accepts and which MPP outbound/balancer they use.
    pub(super) route_target: Option<RouteTarget>,
    pub(super) ingresses: Arc<Vec<LocalIngressConfig>>,
    // Carrier ownership: path specs, per-path security, and live sessions belong
    // to the MPP session's carrier path registry, not to individual streams.
    pub(super) tcp_paths: Arc<Vec<PathSpec>>,
    pub(super) udp_paths: Arc<Vec<PathSpec>>,
    pub(super) tcp_path_ordinals: Arc<Vec<usize>>,
    pub(super) udp_path_ordinals: Arc<Vec<usize>>,
    pub(super) tcp_security: Arc<Vec<SecurityConfig>>,
    pub(super) tcp_sessions: Arc<Vec<ClientTcpPathSessionHandle>>,
    pub(super) udp_sessions: Arc<Vec<ClientUdpPathSessionHandle>>,
    // Product ownership: reliable stream IDs live above TCP and QUIC UDP paths.
    pub(super) next_reliable_stream_id: Arc<Mutex<u64>>,
    // TCP exploration is amortized by distinct TCP-Service request flows, not
    // by path attachments or QUIC traffic in the same product session.
    active_tcp_service_request_bulk_flows: Arc<AtomicU32>,
    // Path-model ownership: health records are evidence snapshots consumed by
    // schedulers; they must not own product bytes or carrier queues.
    pub(super) health: Arc<Mutex<ClientPathHealth>>,
    pub(super) codec_limits: CodecLimits,
    pub(super) mux_limits: MuxLimits,
    #[cfg(test)]
    pub(super) proxy_auth: ProxyAuthConfig,
}

/// Cancellation-safe ownership of one logical flow's shared path load.
///
/// Validation attachment alone is not demand. The lease starts only when the
/// flow commits unique product bytes and releases the shared scheduler load if
/// that path, relay task, or enqueue attempt is dropped.
pub(super) struct RelayPathLoadLease {
    context: ClientPathContext,
    key: RelayPathKey,
    lane: FlowLane,
}

impl RelayPathLoadLease {
    pub(super) fn set_recorded_lane(&mut self, lane: FlowLane) {
        self.lane = lane;
    }
}

impl Drop for RelayPathLoadLease {
    fn drop(&mut self) {
        self.context
            .release_relay_path_load(self.key.underlay, self.key.index, self.lane);
    }
}

#[derive(Debug)]
struct ReliableTcpRequestBulkFlowRegistrationState {
    active_tcp_service_request_bulk_flows: Arc<AtomicU32>,
    counted: Mutex<bool>,
}

#[derive(Clone, Debug)]
pub(super) struct ReliableTcpRequestBulkFlowRegistration {
    state: Arc<ReliableTcpRequestBulkFlowRegistrationState>,
}

impl ReliableTcpRequestBulkFlowRegistration {
    pub(super) fn update(
        &self,
        request_bulk_active: bool,
        service_underlay: Option<UnderlayProtocol>,
    ) {
        let counted = request_bulk_active && service_underlay == Some(UnderlayProtocol::Tcp);
        let mut current = self
            .state
            .counted
            .lock()
            .expect("TCP-Service request bulk flow registration lock");
        if *current == counted {
            return;
        }
        if counted {
            self.state
                .active_tcp_service_request_bulk_flows
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    count.checked_add(1)
                })
                .expect("TCP-Service request bulk flow count overflow");
        } else {
            self.state
                .active_tcp_service_request_bulk_flows
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    count.checked_sub(1)
                })
                .expect("TCP-Service request bulk flow registration is unbalanced");
        }
        *current = counted;
    }
}

impl Drop for ReliableTcpRequestBulkFlowRegistrationState {
    fn drop(&mut self) {
        let counted = self
            .counted
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *counted {
            self.active_tcp_service_request_bulk_flows
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    count.checked_sub(1)
                })
                .expect("TCP-Service request bulk flow registration is unbalanced");
        }
    }
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
    // Reliable product rate is separate from generic/datagram path goodput.
    pub(super) product_delivery_rate_bps: Option<f64>,
    pub(super) product_delivery_sample_bytes: u64,
    pub(super) datagram_feedback_samples: u32,
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
    pub(super) carrier_delivery_sample_bytes: u64,
    pub(super) carrier_last_delivery_at: Option<Instant>,
    pub(super) carrier_app_limited: bool,
    pub(super) carrier_ack_derived_data_seen: bool,
    pub(super) path_proof_success: bool,
    path_proof_generation: u64,
    path_proof_valid_after: Instant,
    successful_path_proofs: HashMap<u64, SuccessfulPathProof>,
    successful_path_proof_order: VecDeque<u64>,
    successful_path_proof_limit: usize,
}

#[derive(Debug, Clone, Copy)]
struct SuccessfulPathProof {
    proof_id: u64,
    sent_at: Instant,
    acked_at: Instant,
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
            product_delivery_rate_bps: None,
            product_delivery_sample_bytes: 0,
            datagram_feedback_samples: 0,
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
            carrier_delivery_sample_bytes: 0,
            carrier_last_delivery_at: None,
            carrier_app_limited: true,
            carrier_ack_derived_data_seen: false,
            path_proof_success: false,
            path_proof_generation: 0,
            path_proof_valid_after: Instant::now(),
            successful_path_proofs: HashMap::new(),
            successful_path_proof_order: VecDeque::new(),
            successful_path_proof_limit: 1,
        }
    }
}

impl ClientPathHealthRecord {
    fn with_path_proof_limit(limit: usize) -> Self {
        Self {
            successful_path_proof_limit: limit.max(1),
            ..Self::default()
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
    pub(super) product_delivery_rate_bps: Option<f64>,
    pub(super) product_delivery_sample_bytes: u64,
    pub(super) datagram_feedback_samples: u32,
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
    pub(super) carrier_delivery_sample_bytes: u64,
    pub(super) carrier_last_delivery_at: Option<Instant>,
    pub(super) carrier_app_limited: bool,
    pub(super) carrier_ack_derived_data_seen: bool,
    pub(super) path_proof_success: bool,
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
            product_delivery_rate_bps: None,
            product_delivery_sample_bytes: 0,
            datagram_feedback_samples: 0,
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
            carrier_delivery_sample_bytes: 0,
            carrier_last_delivery_at: None,
            carrier_app_limited: true,
            carrier_ack_derived_data_seen: false,
            path_proof_success: false,
        }
    }
}

fn reliable_reservation_should_use_endpoint_only_startup_order(
    tcp_paths: &[PathSpec],
    tcp_observations: &[ClientPathObservation],
    udp_paths: &[PathSpec],
    udp_observations: &[ClientPathObservation],
    lane: FlowLane,
) -> bool {
    reliable_relay_expects_interactive_response(lane)
        && (!tcp_paths.is_empty() || !udp_paths.is_empty())
        && tcp_paths.iter().chain(udp_paths).all(path_is_endpoint_only)
        && !paths_have_sender_delivery_evidence(tcp_paths, tcp_observations)
        && !paths_have_sender_delivery_evidence(udp_paths, udp_observations)
}

fn paths_have_sender_delivery_evidence(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
) -> bool {
    paths.iter().enumerate().any(|(index, path)| {
        bulk_candidate_has_sender_delivery_evidence(
            path,
            observations.get(index).copied().unwrap_or_default(),
        )
    })
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
                product_delivery_rate_bps: self.product_delivery_rate_bps,
                product_delivery_sample_bytes: self.product_delivery_sample_bytes,
                datagram_feedback_samples: self.datagram_feedback_samples,
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
                carrier_delivery_sample_bytes: self.carrier_delivery_sample_bytes,
                carrier_last_delivery_at: self.carrier_last_delivery_at,
                carrier_app_limited: self.carrier_app_limited,
                carrier_ack_derived_data_seen: self.carrier_ack_derived_data_seen,
                path_proof_success: self.path_proof_success,
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
            product_delivery_rate_bps: self.product_delivery_rate_bps,
            product_delivery_sample_bytes: self.product_delivery_sample_bytes,
            datagram_feedback_samples: self.datagram_feedback_samples,
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
            carrier_delivery_sample_bytes: self.carrier_delivery_sample_bytes,
            carrier_last_delivery_at: self.carrier_last_delivery_at,
            carrier_app_limited: self.carrier_app_limited,
            carrier_ack_derived_data_seen: self.carrier_ack_derived_data_seen,
            path_proof_success: self.path_proof_success,
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

    pub(super) fn mark_path_proof_success(&mut self, observation: PathProofObservation) {
        if self.manual_disabled || observation.sent_at < self.path_proof_valid_after {
            return;
        }
        self.mark_success(observation.elapsed);
        self.path_proof_success = true;
        let proof = SuccessfulPathProof {
            proof_id: observation.proof_id,
            sent_at: observation.sent_at,
            acked_at: Instant::now(),
        };
        if self
            .successful_path_proofs
            .insert(observation.proof_id, proof)
            .is_none()
        {
            self.successful_path_proof_order
                .push_back(observation.proof_id);
        }
        while self.successful_path_proofs.len() > self.successful_path_proof_limit {
            if let Some(proof_id) = self.successful_path_proof_order.pop_front() {
                self.successful_path_proofs.remove(&proof_id);
            }
        }
    }

    pub(super) fn invalidate_path_proofs(&mut self) {
        self.path_proof_success = false;
        self.successful_path_proofs.clear();
        self.successful_path_proof_order.clear();
        self.path_proof_generation = self.path_proof_generation.wrapping_add(1);
        self.path_proof_valid_after = Instant::now();
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
        if reliable_relay_expects_interactive_response(lane) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
    }

    pub(super) fn reserve_load(&mut self, lane: FlowLane) {
        self.active_flows = self.active_flows.saturating_add(1);
        if reliable_relay_expects_interactive_response(lane) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
    }

    pub(super) fn mark_reserved_open_success(&mut self, _elapsed: Duration) {
        self.mark_liveness_success();
    }

    pub(super) fn release_load(&mut self, lane: FlowLane) {
        self.active_flows = self.active_flows.saturating_sub(1);
        if reliable_relay_expects_interactive_response(lane) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_sub(1);
        }
    }

    pub(super) fn change_lane_load(&mut self, from: FlowLane, to: FlowLane) {
        if reliable_relay_expects_interactive_response(from)
            && !reliable_relay_expects_interactive_response(to)
        {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_sub(1);
        } else if !reliable_relay_expects_interactive_response(from)
            && reliable_relay_expects_interactive_response(to)
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

    pub(super) fn mark_product_delivery(&mut self, sample: PathRateSample) {
        if self.manual_disabled {
            return;
        }
        let sample_bps = sample.rate_bps();
        self.product_delivery_rate_bps = Some(match self.product_delivery_rate_bps {
            Some(previous) => previous.mul_add(0.75, sample_bps * 0.25),
            None => sample_bps,
        });
        self.product_delivery_sample_bytes = self
            .product_delivery_sample_bytes
            .saturating_add(sample.bytes);
        self.mark_delivery(sample);
    }

    pub(super) fn mark_product_delivery_replacing_rate(&mut self, sample: PathRateSample) {
        if self.manual_disabled {
            return;
        }
        self.product_delivery_sample_bytes = self
            .product_delivery_sample_bytes
            .saturating_add(sample.bytes);
        self.product_delivery_rate_bps = Some(sample.rate_bps());
        self.state = SchedulerPathState::Active;
        self.consecutive_failures = 0;
        self.failed_until = None;
        self.delivery_samples = self.delivery_samples.saturating_add(1);
        self.last_delivery_at = Some(Instant::now());
        self.measured_rate_bps = Some(sample.rate_bps());
    }

    pub(super) fn mark_udp_datagram_feedback(&mut self, observation: UdpDatagramPathObservation) {
        self.mark_success(observation.rtt);
        if let Some(sample) = observation.rate_sample {
            self.mark_delivery(sample);
            self.datagram_feedback_samples = self.datagram_feedback_samples.saturating_add(1);
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

    pub(super) fn mark_quic_path_metrics(&mut self, metrics: UdpPathMetrics) {
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
        self.carrier_delivery_rate_bps =
            (metrics.delivery_sample_count > 0).then_some(metrics.delivery_rate_bps.max(1.0));
        self.carrier_delivery_samples =
            u32::try_from(metrics.delivery_sample_count).unwrap_or(u32::MAX);
        self.carrier_delivery_sample_bytes = metrics.delivery_sample_bytes;
        self.carrier_last_delivery_at = metrics.last_delivery_sample_at;
        if metrics.ack_derived_data_seen {
            self.carrier_ack_derived_data_seen = true;
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
        self.invalidate_path_proofs();
        if self.consecutive_failures == 1 || !has_schedulable_alternative {
            self.state = SchedulerPathState::Suspect;
            self.failed_until = None;
        } else {
            self.state = SchedulerPathState::Failed;
            self.failed_until = Some(now + path_record_failure_cooldown(self));
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
        // Product and native carrier evidence belongs to the failed association.
        self.product_delivery_rate_bps = None;
        self.product_delivery_sample_bytes = 0;
        self.carrier_delivery_rate_bps = None;
        self.carrier_bytes_in_flight = 0;
        self.carrier_queue_bytes = 0;
        self.carrier_inflight_limit_bytes = 0;
        self.carrier_delivery_samples = 0;
        self.carrier_delivery_sample_bytes = 0;
        self.carrier_last_delivery_at = None;
        self.carrier_app_limited = true;
        self.carrier_ack_derived_data_seen = false;
        self.invalidate_path_proofs();
        if has_schedulable_alternative {
            self.state = SchedulerPathState::Failed;
            self.failed_until = Some(now + path_record_failure_cooldown(self));
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
        Some(Self { bytes, elapsed })
    }

    pub(super) fn rate_bps(self) -> f64 {
        self.bytes as f64 * 8.0 / self.elapsed.max(TRANSPORT_TIMER_GRANULARITY).as_secs_f64()
    }

    pub(super) fn bytes(self) -> u64 {
        self.bytes
    }

    pub(super) fn elapsed(self) -> Duration {
        self.elapsed
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
        let capacity = capacity.max(1);
        Self {
            capacity,
            order: VecDeque::new(),
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

pub(super) fn reliable_closed_stream_cache_capacity(max_streams: usize) -> usize {
    // Closed-stream rejection state scales with the configured stream registry.
    // The cache is lazily allocated by RecentIdCache, so small deployments stay
    // cheap and high-fanout deployments are not silently capped by one fixed
    // slot count.
    max_streams.max(1).saturating_mul(2)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct PathJoinReplayKey {
    pub(super) session_id: SessionId,
    pub(super) path_id: PathId,
    pub(super) underlay: UnderlayProtocol,
    pub(super) nonce: AuthNonce,
}

pub(super) fn path_join_replay_cache_capacity(max_streams: usize) -> usize {
    // Replay protection scales with configured session concurrency. This is a
    // security/control-plane retention window, not a data-plane queue; keep it
    // lazy instead of imposing arbitrary min/max slot caps.
    max_streams
        .max(1)
        .saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD as usize + 1)
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
        let tcp_entries = paths
            .iter()
            .enumerate()
            .filter(|(_, path)| path.spec.underlay == UnderlayProtocol::Tcp)
            .map(|(ordinal, path)| (ordinal, path.spec.clone(), path.security.clone()))
            .collect::<Vec<_>>();
        let tcp_path_ordinals = tcp_entries
            .iter()
            .map(|(ordinal, _, _)| *ordinal)
            .collect::<Vec<_>>();
        let tcp_paths = tcp_entries
            .iter()
            .map(|(_, path, _)| path.clone())
            .collect::<Vec<_>>();
        let tcp_security = tcp_entries
            .into_iter()
            .map(|(_, _, security)| security)
            .collect::<Vec<_>>();
        let udp_entries = paths
            .into_iter()
            .enumerate()
            .filter(|(_, path)| path.spec.underlay == UnderlayProtocol::Udp)
            .map(|(ordinal, path)| (ordinal, path.spec, path.security))
            .collect::<Vec<_>>();
        let udp_path_ordinals = udp_entries
            .iter()
            .map(|(ordinal, _, _)| *ordinal)
            .collect::<Vec<_>>();
        let udp_paths = udp_entries
            .iter()
            .map(|(_, path, _)| path.clone())
            .collect::<Vec<_>>();
        let udp_security = udp_entries
            .into_iter()
            .map(|(_, _, security)| security)
            .collect::<Vec<_>>();
        let path_proof_limit = resources.max_streams.saturating_mul(2).max(1);
        let health = Arc::new(Mutex::new(ClientPathHealth {
            tcp: vec![
                ClientPathHealthRecord::with_path_proof_limit(path_proof_limit);
                tcp_paths.len()
            ],
            udp: vec![
                ClientPathHealthRecord::with_path_proof_limit(path_proof_limit);
                udp_paths.len()
            ],
        }));
        let codec_limits = resources.into();
        let mux_limits = resources.into();
        let session_id = random_session_id()?;
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
                    stream_frame_queue: reliable_stream_frame_queue(mux_limits),
                    closed_stream_cache_capacity: reliable_closed_stream_cache_capacity(
                        resources.max_streams,
                    ),
                    health: health.clone(),
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
                    stream_frame_queue: reliable_stream_frame_queue(mux_limits),
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
            tcp_path_ordinals: Arc::new(tcp_path_ordinals),
            udp_path_ordinals: Arc::new(udp_path_ordinals),
            tcp_security: Arc::new(tcp_security),
            tcp_sessions: Arc::new(tcp_sessions),
            udp_sessions: Arc::new(udp_sessions),
            next_reliable_stream_id: Arc::new(Mutex::new(0)),
            active_tcp_service_request_bulk_flows: Arc::new(AtomicU32::new(0)),
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

    pub(super) fn relay_path_config_ordinal(&self, key: RelayPathKey) -> usize {
        match key.underlay {
            UnderlayProtocol::Tcp => self.tcp_path_ordinals.get(key.index).copied(),
            UnderlayProtocol::Udp => self.udp_path_ordinals.get(key.index).copied(),
        }
        .unwrap_or(usize::MAX)
    }

    pub(super) fn relay_path_key_order(
        &self,
        left: RelayPathKey,
        right: RelayPathKey,
    ) -> std::cmp::Ordering {
        self.relay_path_config_ordinal(left)
            .cmp(&self.relay_path_config_ordinal(right))
            .then_with(|| left.index.cmp(&right.index))
            .then_with(|| relay_underlay_identity_order(left.underlay, right.underlay))
    }

    pub(super) fn allocate_reliable_stream_id(&self) -> Result<StreamId, RuntimeError> {
        let mut next = self
            .next_reliable_stream_id
            .lock()
            .expect("client reliable stream ID lock");
        let stream_id = StreamId(*next);
        *next = next
            .checked_add(1)
            .ok_or(RuntimeError::Protocol("reliable stream ID overflow"))?;
        Ok(stream_id)
    }

    pub(super) fn reliable_tcp_request_bulk_flow_registration(
        &self,
    ) -> ReliableTcpRequestBulkFlowRegistration {
        ReliableTcpRequestBulkFlowRegistration {
            state: Arc::new(ReliableTcpRequestBulkFlowRegistrationState {
                active_tcp_service_request_bulk_flows: self
                    .active_tcp_service_request_bulk_flows
                    .clone(),
                counted: Mutex::new(false),
            }),
        }
    }

    pub(super) fn active_tcp_service_request_bulk_flows(&self) -> u32 {
        self.active_tcp_service_request_bulk_flows
            .load(Ordering::Acquire)
    }

    pub(super) fn ordered_tcp_path_indices(
        &self,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Vec<usize> {
        let observations = self.tcp_health_observations_for_lane(lane);
        if endpoint_only_reliable_startup_should_preserve_configured_order(
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

    pub(super) fn try_reserve_relay_path_load_if_unchanged(
        &self,
        key: RelayPathKey,
        lane: FlowLane,
        expected_active_flows: u32,
        expected_active_latency_sensitive_flows: u32,
    ) -> Option<RelayPathLoadLease> {
        let mut health = self.health.lock().expect("client path health lock");
        let records = match key.underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        let Some(current) = records.get_mut(key.index) else {
            return None;
        };
        if current.manual_disabled
            || current.state != SchedulerPathState::Active
            || current.active_flows != expected_active_flows
            || current.active_latency_sensitive_flows != expected_active_latency_sensitive_flows
        {
            return None;
        }
        current.reserve_load(lane);
        drop(health);
        Some(RelayPathLoadLease {
            context: self.clone(),
            key,
            lane,
        })
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
        apply_bulk_latency_isolation(&mut tcp_observations, lane, self.mux_limits);
        let mut udp_observations = health_observations(&mut health.udp);
        apply_bulk_latency_isolation(&mut udp_observations, lane, self.mux_limits);
        let mut candidates = reliable_stream_path_candidates(
            &self.tcp_paths,
            &tcp_observations,
            &self.udp_paths,
            &udp_observations,
            lane,
            payload_bytes,
        );
        candidates.retain(|candidate| !excluded.contains(&candidate.key));
        if reliable_reservation_should_use_endpoint_only_startup_order(
            &self.tcp_paths,
            &tcp_observations,
            &self.udp_paths,
            &udp_observations,
            lane,
        ) {
            candidates.sort_by(|left, right| {
                left.snapshot
                    .active_latency_sensitive_flows
                    .cmp(&right.snapshot.active_latency_sensitive_flows)
                    .then_with(|| left.snapshot.active_flows.cmp(&right.snapshot.active_flows))
                    .then_with(|| self.relay_path_key_order(left.key, right.key))
            });
        } else {
            candidates.sort_by(|left, right| {
                left.eta_ms
                    .total_cmp(&right.eta_ms)
                    .then_with(|| self.relay_path_key_order(left.key, right.key))
            });
        }
        #[cfg(feature = "lab-diagnostics")]
        for (rank, candidate) in candidates.iter().enumerate() {
            lab_diagnostic(
                "reliable_stream_initial_path_candidate",
                format_args!(
                    "lane={:?} payload_bytes={} rank={} path_underlay={:?} path_index={} eta_ms={:.3} state={:?} active_flows={} active_latency_flows={} queue_bytes={} product_queue_bytes={} bytes_in_flight={} inflight_limit={} delivery_rate_bps={:.0} pacing_rate_bps={:.0} app_limited={} liveness_evidence={} path_proof_evidence={} ack_data_evidence={} bulk_rate_evidence={} sender_delivery_evidence={}",
                    lane,
                    payload_bytes,
                    rank,
                    candidate.key.underlay,
                    candidate.key.index,
                    candidate.eta_ms,
                    candidate.snapshot.state,
                    candidate.snapshot.active_flows,
                    candidate.snapshot.active_latency_sensitive_flows,
                    candidate.snapshot.queue_bytes,
                    candidate.snapshot.product_queue_bytes,
                    candidate.snapshot.bytes_in_flight,
                    candidate.snapshot.inflight_limit_bytes,
                    candidate.snapshot.delivery_rate_bps,
                    candidate.snapshot.pacing_rate_bps,
                    candidate.snapshot.app_limited,
                    candidate.has_liveness_evidence,
                    candidate.has_path_proof_evidence,
                    candidate.has_ack_data_evidence,
                    candidate.has_bulk_rate_evidence,
                    candidate.has_sender_delivery_evidence,
                ),
            );
        }
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

    pub(super) fn ordered_reliable_path_keys(
        &self,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Vec<RelayPathKey> {
        let mut health = self.health.lock().expect("client path health lock");
        let mut tcp_observations = health_observations(&mut health.tcp);
        apply_bulk_latency_isolation(&mut tcp_observations, lane, self.mux_limits);
        let mut udp_observations = health_observations(&mut health.udp);
        apply_bulk_latency_isolation(&mut udp_observations, lane, self.mux_limits);
        let mut candidates = reliable_stream_path_candidates(
            &self.tcp_paths,
            &tcp_observations,
            &self.udp_paths,
            &udp_observations,
            lane,
            payload_bytes,
        );
        candidates.sort_by(|left, right| {
            left.eta_ms
                .total_cmp(&right.eta_ms)
                .then_with(|| self.relay_path_key_order(left.key, right.key))
        });
        candidates
            .into_iter()
            .map(|candidate| candidate.key)
            .collect()
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
    ) -> Vec<usize> {
        let mut observations =
            health_observations(&mut self.health.lock().expect("client path health lock").udp);
        apply_bulk_latency_isolation(&mut observations, lane, self.mux_limits);
        let scores = if endpoint_only_reliable_startup_should_preserve_configured_order(
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
            .map(|(index, _)| index)
            .collect()
    }

    pub(super) fn ordered_reliable_repair_path_keys(
        &self,
        current_tcp_path_index: Option<usize>,
        current_udp_path_index: Option<usize>,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Vec<RelayPathKey> {
        let mut candidates = self
            .ordered_tcp_repair_path_indices(current_tcp_path_index, lane, payload_bytes)
            .into_iter()
            .map(|index| RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index,
            })
            .chain(
                self.ordered_udp_stream_repair_path_indices(
                    current_udp_path_index,
                    lane,
                    payload_bytes,
                )
                .into_iter()
                .map(|index| RelayPathKey {
                    underlay: UnderlayProtocol::Udp,
                    index,
                }),
            )
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            let left_eta = self
                .reliable_relay_path_eta_ms(*left, lane, payload_bytes)
                .unwrap_or(f64::INFINITY);
            let right_eta = self
                .reliable_relay_path_eta_ms(*right, lane, payload_bytes)
                .unwrap_or(f64::INFINITY);
            left_eta
                .total_cmp(&right_eta)
                .then_with(|| self.relay_path_key_order(*left, *right))
        });
        candidates
    }

    pub(super) fn ordered_reliable_bulk_striping_path_keys(
        &self,
        payload_bytes: usize,
    ) -> Vec<RelayPathKey> {
        let mut candidates = self.ordered_reliable_bulk_path_candidates(payload_bytes);
        let has_bulk_rate_evidence = candidates
            .iter()
            .any(|candidate| candidate.has_bulk_rate_evidence);
        let has_active_bulk_work = candidates.iter().any(bulk_candidate_has_active_bulk_work);
        if has_bulk_rate_evidence {
            candidates.retain(|candidate| candidate.has_bulk_rate_evidence);
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
                .then_with(|| self.relay_path_key_order(left.key, right.key))
        });
        if !has_bulk_rate_evidence && !has_active_bulk_work {
            candidates.truncate(1);
        }
        bulk_striping_admitted_subflows(candidates, payload_bytes, self.mux_limits)
            .into_iter()
            .map(|candidate| candidate.key)
            .collect()
    }

    pub(super) fn ordered_reliable_bulk_validation_path_keys(
        &self,
        payload_bytes: usize,
    ) -> Vec<RelayPathKey> {
        let payload_bytes = payload_bytes
            .min(relay_lane_startup_chunk_bytes(
                FlowLane::Latency,
                self.mux_limits,
            ))
            .max(PATH_OPEN_SCORE_BYTES);
        let mut candidates = self.ordered_reliable_bulk_path_candidates(payload_bytes);
        candidates.sort_by(|left, right| {
            left.eta_ms
                .total_cmp(&right.eta_ms)
                .then_with(|| self.relay_path_key_order(left.key, right.key))
        });
        let admitted = candidates
            .into_iter()
            .filter(|candidate| {
                !candidate.has_bulk_rate_evidence && candidate.snapshot.active_flows == 0
            })
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
            path_can_be_auto_discovered(path, observation).then_some(bulk_path_candidate(
                RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index,
                },
                eta_ms,
                path,
                observation,
                snapshot,
            ))
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
                path_can_be_auto_discovered(path, observation).then_some(bulk_path_candidate(
                    RelayPathKey {
                        underlay: UnderlayProtocol::Udp,
                        index,
                    },
                    eta_ms + udp_reliable_stream_loss_repair_penalty_ms(snapshot, payload_bytes),
                    path,
                    observation,
                    snapshot,
                ))
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
        apply_bulk_latency_isolation(&mut observations, lane, self.mux_limits);
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

    pub(super) fn reliable_path_snapshot(&self, key: RelayPathKey) -> Option<PathSnapshot> {
        match key.underlay {
            UnderlayProtocol::Tcp => self.tcp_path_snapshot(key.index),
            UnderlayProtocol::Udp => self.udp_path_snapshot(key.index),
        }
    }

    pub(super) fn reliable_path_rtt_is_observed(&self, key: RelayPathKey) -> bool {
        let health = self.health.lock().expect("client path health lock");
        let record = match key.underlay {
            UnderlayProtocol::Tcp => health.tcp.get(key.index),
            UnderlayProtocol::Udp => health.udp.get(key.index),
        };
        record.is_some_and(|record| {
            record.carrier_srtt_ms.is_some()
                || record.carrier_rttvar_ms.is_some()
                || record.measured_srtt_ms.is_some()
                || record.measured_jitter_ms.is_some()
        })
    }

    pub(super) fn reliable_relay_path_eta_ms(
        &self,
        key: RelayPathKey,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Option<f64> {
        self.reliable_path_snapshot(key).and_then(|snapshot| {
            scheduler::score_path(snapshot, lane, payload_bytes, SchedulerPolicy::default())
                .map(|score| score.eta_ms)
        })
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
            let freshness_budget_ms = f64::from(ttl_ms);
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
        let freshness_budget_ms = f64::from(ttl_ms);
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

    pub(super) fn mark_udp_stream_reserved_open_success(
        &self,
        index: usize,
        elapsed: Duration,
        accepted: bool,
    ) {
        if !accepted {
            return;
        }
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
                        bulk_candidate_has_bulk_rate_evidence(path, record.observe(Instant::now()))
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
                        bulk_candidate_has_bulk_rate_evidence(path, record.observe(Instant::now()))
                    })
                    .unwrap_or(false)
            }
        }
    }

    pub(super) fn relay_path_has_native_bulk_model_evidence_since(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        valid_after: Instant,
    ) -> bool {
        if underlay != UnderlayProtocol::Udp {
            return false;
        }
        let now = Instant::now();
        let mut health = self.health.lock().expect("client path health lock");
        health
            .udp
            .get_mut(index)
            .map(|record| {
                let observation = record.observe(now);
                bulk_candidate_has_fresh_native_carrier_rate_evidence(observation, valid_after, now)
            })
            .unwrap_or(false)
    }

    pub(super) fn relay_path_has_fresh_proof(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        proof_id: u64,
        attached_at: Instant,
    ) -> bool {
        self.relay_path_fresh_proof_acked_at(underlay, index, proof_id, attached_at)
            .is_some()
    }

    pub(super) fn relay_path_proof_generation(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
    ) -> Option<u64> {
        let health = self.health.lock().expect("client path health lock");
        match underlay {
            UnderlayProtocol::Tcp => health.tcp.get(index),
            UnderlayProtocol::Udp => health.udp.get(index),
        }
        .map(|record| record.path_proof_generation)
    }

    pub(super) fn relay_path_fresh_proof_acked_at(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        proof_id: u64,
        attached_at: Instant,
    ) -> Option<Instant> {
        let mut health = self.health.lock().expect("client path health lock");
        let record = match underlay {
            UnderlayProtocol::Tcp => health.tcp.get_mut(index),
            UnderlayProtocol::Udp => health.udp.get_mut(index),
        };
        record.and_then(|record| {
            let observation = record.observe(Instant::now());
            (observation.state == SchedulerPathState::Active && !observation.manual_disabled)
                .then(|| {
                    record
                        .successful_path_proofs
                        .get(&proof_id)
                        .filter(|proof| proof.proof_id == proof_id && proof.sent_at >= attached_at)
                        .map(|proof| proof.acked_at)
                })
                .flatten()
        })
    }

    pub(super) fn reliable_relay_has_latency_pressure(&self) -> bool {
        let mut health = self.health.lock().expect("client path health lock");
        let tcp_pressure = health.tcp.iter_mut().any(|record| {
            record
                .observe(Instant::now())
                .active_latency_sensitive_flows
                > 0
        });
        tcp_pressure
            || health.udp.iter_mut().any(|record| {
                record
                    .observe(Instant::now())
                    .active_latency_sensitive_flows
                    > 0
            })
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
            UnderlayProtocol::Udp => self.mark_udp_reliable_path_delivery(index, stats),
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
            current.mark_product_delivery(sample);
        }
    }

    pub(super) fn mark_relay_path_ack_clock_rate_sample(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        sample: PathRateSample,
        replace_startup_rate: bool,
    ) {
        let mut health = self.health.lock().expect("client path health lock");
        let records = match underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        if let Some(current) = records.get_mut(index) {
            if replace_startup_rate {
                current.mark_product_delivery_replacing_rate(sample);
            } else {
                current.mark_product_delivery(sample);
            }
        }
    }

    #[cfg(test)]
    pub(super) fn mark_relay_path_proof_observation(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        observation: PathProofObservation,
    ) {
        let mut health = self.health.lock().expect("client path health lock");
        let records = match underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        if let Some(current) = records.get_mut(index) {
            current.mark_path_proof_success(observation);
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
            current.mark_product_delivery(sample);
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

    fn mark_udp_reliable_path_delivery(&self, index: usize, stats: PathDeliveryStats) {
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
            current.mark_product_delivery(sample);
        }
    }

    pub(super) fn mark_udp_datagram_path_delivery(&self, index: usize, stats: PathDeliveryStats) {
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
            // Datagram goodput ranks datagram paths but never proves reliable
            // product ownership or unlocks ordered-stream overlap.
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

#[derive(Debug, Clone)]
pub struct ServerPathContext {
    // Configuration ownership: this MPP inbound tag and route target select the
    // egress outbound or balancer for streams accepted on this listener set.
    pub(super) tag: Option<String>,
    pub(super) route_target: Option<RouteTarget>,
    // Carrier ownership: bind path specs and security authenticate live path
    // joins. Per-stream byte ownership is delegated to `reliable_streams`.
    pub(super) server_paths: Arc<Vec<PathSpec>>,
    pub(super) outbound: OutboundConfig,
    pub(super) outbound_dns: DnsConfig,
    pub(super) outbound_connect_timeout: Duration,
    pub(super) performance: MppPerformanceConfig,
    pub(super) codec_limits: CodecLimits,
    pub(super) mux_limits: MuxLimits,
    pub(super) security: SecurityConfig,
    // Product ownership: server-side reliable streams are registered once per
    // session/stream ID and can attach TCP or QUIC UDP paths.
    pub(super) reliable_streams: Arc<ServerReliableStreamRegistry>,
    pub(super) path_join_replay: Arc<Mutex<RecentIdCache<PathJoinReplayKey>>>,
    pub(super) max_reliable_streams: usize,
    pub(super) max_udp_flows_per_session: usize,
}

impl ServerPathContext {
    pub(super) fn local_path_startup_metrics(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
    ) -> Option<PathMetrics> {
        let index = usize::from(path_id.0);
        let path = self.server_paths.get(index)?;
        (path.underlay == underlay)
            .then(|| path_startup_metrics(path, index, PathMetricDirection::ServerToClient))
    }

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
