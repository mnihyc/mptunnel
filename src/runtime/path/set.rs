//! Client carrier inventory and its transactional mutable path state.
//!
//! Capacity reservations stay beside health publication and lease rollback so
//! no sender or carrier can observe half of a request-probe transaction.

use super::commands::reliable_stream_frame_queue;
use super::health::{ClientPathHealth, ClientPathHealthRecord};
use super::model::{ClientPathObservation, path_metrics_from_snapshot, path_snapshot};
use super::quic::client::{ClientUdpPathSessionHandle, ClientUdpPathSessionRuntime};
use super::state::ClientPathState;
use super::tcp::client::{
    ClientTcpPathSessionHandle, ClientTcpPathSessionRuntime, tcp_session_command_queue,
};
use crate::config::{
    ClientPathConfig, LocalIngressConfig, ResourceLimits, RouteTarget, SecurityConfig,
};
#[cfg(test)]
use crate::ingress::ProxyAuthConfig;
use crate::model::path::RelayPathKey;
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{
    PathMetricDirection, PathUsage, PeerPathState, PeerPathStatus, SessionId, UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::identity::random_session_id;
use crate::runtime::peer_status::{PeerStatusBroker, PeerStatusSnapshotSource};
use crate::runtime::recent_ids::reliable_closed_stream_cache_capacity;
use crate::runtime::stream::SessionSendBuffer;
use crate::runtime::telemetry::{
    RuntimeTelemetry, RuntimeTelemetrySnapshot, active_flow_detail_capacity,
};
#[cfg(test)]
use crate::transport::SystemCarrierNetworkProvider;
use crate::transport::{CarrierNetworkProvider, CarrierPathIdentity, PathSpec};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Process-owned dependencies shared by every carrier in one client path group.
pub(in crate::runtime) struct ClientPathRuntimeOptions {
    pub(in crate::runtime) session_retention_timeout: Duration,
    pub(in crate::runtime) path_group_ordinal: usize,
    pub(in crate::runtime) carrier_network: Arc<dyn CarrierNetworkProvider>,
    pub(in crate::runtime) allow_peer_diagnostics: bool,
}

#[derive(Clone)]
pub struct ClientPathContext {
    // Configuration ownership: local inbounds and route target describe which
    // product flows this client accepts and which MPP outbound/balancer they use.
    pub(in crate::runtime) route_target: Option<RouteTarget>,
    pub(in crate::runtime) ingresses: Arc<Vec<LocalIngressConfig>>,
    // Carrier ownership: path specs, per-path security, and live sessions belong
    // to the MPP session's carrier path registry, not to individual streams.
    pub(in crate::runtime) tcp_paths: Arc<Vec<PathSpec>>,
    pub(in crate::runtime) udp_paths: Arc<Vec<PathSpec>>,
    pub(in crate::runtime) tcp_path_ordinals: Arc<Vec<usize>>,
    pub(in crate::runtime) udp_path_ordinals: Arc<Vec<usize>>,
    pub(in crate::runtime) path_group_ordinal: usize,
    pub(in crate::runtime) tcp_security: Arc<Vec<SecurityConfig>>,
    pub(in crate::runtime) tcp_sessions: Arc<Vec<ClientTcpPathSessionHandle>>,
    pub(in crate::runtime) udp_sessions: Arc<Vec<ClientUdpPathSessionHandle>>,
    pub(in crate::runtime) carrier_network: Arc<dyn CarrierNetworkProvider>,
    pub(super) state: Arc<ClientPathState>,
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) telemetry: RuntimeTelemetry,
    pub(in crate::runtime) peer_status: PeerStatusBroker,
    pub(in crate::runtime) codec_limits: CodecLimits,
    pub(in crate::runtime) mux_limits: MuxLimits,
    /// RFC 8684 break-before-make lifetime for established logical streams.
    pub(in crate::runtime) session_retention_timeout: std::time::Duration,
    // All reliable streams share one work-conserving unique-byte memory owner.
    // Per-stream peer windows and per-carrier congestion authority stay separate.
    pub(in crate::runtime) session_send_buffer: SessionSendBuffer,
    #[cfg(test)]
    pub(in crate::runtime) proxy_auth: ProxyAuthConfig,
}

impl ClientPathContext {
    #[cfg(test)]
    pub(in crate::runtime) fn peer_path_usage(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
    ) -> Option<crate::protocol::PathUsage> {
        self.state.peer_path_usage(underlay, index)
    }

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

    #[cfg(test)]
    pub fn new_with_path_configs_and_target(
        paths: Vec<ClientPathConfig>,
        resources: ResourceLimits,
        proxy_auth: ProxyAuthConfig,
        route_target: Option<RouteTarget>,
        ingresses: Vec<LocalIngressConfig>,
    ) -> Result<Self, RuntimeError> {
        let mut context = Self::new_with_carrier_network(
            paths,
            resources,
            route_target,
            ingresses,
            0,
            Arc::new(SystemCarrierNetworkProvider),
        )?;
        context.proxy_auth = proxy_auth;
        Ok(context)
    }

    #[cfg(test)]
    pub fn new_with_carrier_network(
        paths: Vec<ClientPathConfig>,
        resources: ResourceLimits,
        route_target: Option<RouteTarget>,
        ingresses: Vec<LocalIngressConfig>,
        path_group_ordinal: usize,
        carrier_network: Arc<dyn CarrierNetworkProvider>,
    ) -> Result<Self, RuntimeError> {
        Self::new_with_runtime_options(
            paths,
            resources,
            route_target,
            ingresses,
            ClientPathRuntimeOptions {
                session_retention_timeout: crate::config::DEFAULT_SESSION_RETENTION_TIMEOUT,
                path_group_ordinal,
                carrier_network,
                allow_peer_diagnostics: false,
            },
        )
    }

    pub(in crate::runtime) fn new_with_runtime_options(
        paths: Vec<ClientPathConfig>,
        resources: ResourceLimits,
        route_target: Option<RouteTarget>,
        ingresses: Vec<LocalIngressConfig>,
        runtime: ClientPathRuntimeOptions,
    ) -> Result<Self, RuntimeError> {
        let ClientPathRuntimeOptions {
            session_retention_timeout,
            path_group_ordinal,
            carrier_network,
            allow_peer_diagnostics,
        } = runtime;
        if paths.len() > u16::MAX as usize {
            return Err(RuntimeError::PathIdOverflow);
        }
        let mut tcp_paths = Vec::new();
        let mut tcp_path_ordinals = Vec::new();
        let mut tcp_security = Vec::new();
        let mut udp_paths = Vec::new();
        let mut udp_path_ordinals = Vec::new();
        let mut udp_security = Vec::new();
        for (ordinal, path) in paths.into_iter().enumerate() {
            let ClientPathConfig { spec, security } = path;
            match spec.underlay {
                UnderlayProtocol::Tcp => {
                    tcp_path_ordinals.push(ordinal);
                    tcp_paths.push(spec);
                    tcp_security.push(security);
                }
                UnderlayProtocol::Udp => {
                    udp_path_ordinals.push(ordinal);
                    udp_paths.push(spec);
                    udp_security.push(security);
                }
            }
        }
        // Context and carrier actors share one immutable configuration backing;
        // reconnecting a session must not deep-copy endpoint or secret material.
        let tcp_paths = Arc::new(tcp_paths);
        let tcp_path_ordinals = Arc::new(tcp_path_ordinals);
        let tcp_security = Arc::new(tcp_security);
        let udp_paths = Arc::new(udp_paths);
        let udp_path_ordinals = Arc::new(udp_path_ordinals);
        let udp_security = Arc::new(udp_security);
        let path_proof_limit = resources.max_streams.saturating_mul(2).max(1);
        let state = ClientPathState::new(ClientPathHealth {
            tcp: vec![
                ClientPathHealthRecord::with_path_proof_limit(path_proof_limit);
                tcp_paths.len()
            ],
            udp: vec![
                ClientPathHealthRecord::with_path_proof_limit(path_proof_limit);
                udp_paths.len()
            ],
        });
        let codec_limits = resources.into();
        let mux_limits = resources.into();
        let session_send_buffer = SessionSendBuffer::from_limits(mux_limits);
        let session_id = random_session_id()?;
        let telemetry = RuntimeTelemetry::new(active_flow_detail_capacity(resources.max_streams));
        let peer_status = PeerStatusBroker::new(allow_peer_diagnostics);
        let peer_status_snapshot = PeerStatusSnapshotSource::new({
            let tcp_paths = tcp_paths.clone();
            let udp_paths = udp_paths.clone();
            let state = state.clone();
            move || client_peer_status_snapshot(&tcp_paths, &udp_paths, &state)
        });
        let tcp_sessions = (0..tcp_paths.len())
            .map(|path_index| {
                ClientTcpPathSessionHandle::new(ClientTcpPathSessionRuntime {
                    paths: tcp_paths.clone(),
                    config_index: path_index,
                    path_index,
                    carrier_identity: CarrierPathIdentity {
                        group_ordinal: path_group_ordinal,
                        path_ordinal: tcp_path_ordinals[path_index],
                    },
                    session_id,
                    security: tcp_security.clone(),
                    codec_limits,
                    mux_limits,
                    command_queue: tcp_session_command_queue(resources),
                    stream_frame_queue: reliable_stream_frame_queue(mux_limits),
                    closed_stream_cache_capacity: reliable_closed_stream_cache_capacity(
                        resources.max_streams,
                    ),
                    state: state.clone(),
                    carrier_network: carrier_network.clone(),
                    peer_status: peer_status.clone(),
                    peer_status_snapshot: peer_status_snapshot.clone(),
                })
            })
            .collect::<Vec<_>>();
        let udp_sessions = (0..udp_paths.len())
            .map(|path_index| {
                ClientUdpPathSessionHandle::new(ClientUdpPathSessionRuntime {
                    paths: udp_paths.clone(),
                    config_index: path_index,
                    path_index,
                    carrier_identity: CarrierPathIdentity {
                        group_ordinal: path_group_ordinal,
                        path_ordinal: udp_path_ordinals[path_index],
                    },
                    session_id,
                    security: udp_security.clone(),
                    codec_limits,
                    mux_limits,
                    stream_frame_queue: reliable_stream_frame_queue(mux_limits),
                    state: state.clone(),
                    carrier_network: carrier_network.clone(),
                    peer_status: peer_status.clone(),
                    peer_status_snapshot: peer_status_snapshot.clone(),
                })
            })
            .collect::<Vec<_>>();
        Ok(Self {
            route_target,
            ingresses: Arc::new(ingresses),
            tcp_paths,
            udp_paths,
            tcp_path_ordinals,
            udp_path_ordinals,
            path_group_ordinal,
            tcp_security,
            tcp_sessions: Arc::new(tcp_sessions),
            udp_sessions: Arc::new(udp_sessions),
            carrier_network,
            state,
            session_id,
            telemetry,
            peer_status,
            codec_limits,
            mux_limits,
            session_retention_timeout,
            session_send_buffer,
            #[cfg(test)]
            proxy_auth: ProxyAuthConfig::disabled(),
        })
    }

    pub(in crate::runtime) fn tcp_path_security(
        &self,
        path_index: usize,
    ) -> Result<&SecurityConfig, RuntimeError> {
        self.tcp_security
            .get(path_index)
            .ok_or(RuntimeError::NoSchedulableTcpPath)
    }

    pub(in crate::runtime) fn telemetry_snapshot(&self) -> RuntimeTelemetrySnapshot {
        self.telemetry.snapshot()
    }

    pub(in crate::runtime) fn relay_path_config_ordinal(&self, key: RelayPathKey) -> usize {
        match key.underlay {
            UnderlayProtocol::Tcp => self.tcp_path_ordinals.get(key.index).copied(),
            UnderlayProtocol::Udp => self.udp_path_ordinals.get(key.index).copied(),
        }
        .unwrap_or(usize::MAX)
    }

    pub(in crate::runtime) fn carrier_path_identity(
        &self,
        key: RelayPathKey,
    ) -> CarrierPathIdentity {
        CarrierPathIdentity {
            group_ordinal: self.path_group_ordinal,
            path_ordinal: self.relay_path_config_ordinal(key),
        }
    }

    pub(in crate::runtime) fn relay_path_key_order(
        &self,
        left: RelayPathKey,
        right: RelayPathKey,
    ) -> std::cmp::Ordering {
        self.relay_path_config_ordinal(left)
            .cmp(&self.relay_path_config_ordinal(right))
            .then_with(|| left.index.cmp(&right.index))
            .then_with(|| left.underlay.cmp(&right.underlay))
    }
}

fn client_peer_status_snapshot(
    tcp_paths: &[PathSpec],
    udp_paths: &[PathSpec],
    state: &ClientPathState,
) -> Vec<PeerPathStatus> {
    let now = Instant::now();
    let health = state.health().lock().expect("client path health lock");
    let mut paths = Vec::with_capacity(tcp_paths.len() + udp_paths.len());
    paths.extend(
        tcp_paths
            .iter()
            .zip(&health.tcp)
            .enumerate()
            .map(|(index, (path, record))| {
                peer_path_status(path, index, record.observation_at(now))
            }),
    );
    paths.extend(
        udp_paths
            .iter()
            .zip(&health.udp)
            .enumerate()
            .map(|(index, (path, record))| {
                peer_path_status(path, index, record.observation_at(now))
            }),
    );
    paths
}

fn peer_path_status(
    path: &PathSpec,
    index: usize,
    observation: ClientPathObservation,
) -> PeerPathStatus {
    let snapshot = path_snapshot(path, index, observation);
    PeerPathStatus {
        state: match snapshot.state {
            crate::scheduler::PathState::Active => PeerPathState::Active,
            crate::scheduler::PathState::Suspect => PeerPathState::Suspect,
            crate::scheduler::PathState::Draining => PeerPathState::Draining,
            crate::scheduler::PathState::Failed => PeerPathState::Failed,
        },
        usage: if path.metadata.policy.backup {
            PathUsage::Backup
        } else {
            PathUsage::Available
        },
        metrics: path_metrics_from_snapshot(
            snapshot,
            observation,
            PathMetricDirection::ClientToServer,
        ),
    }
}
