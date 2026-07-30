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
use crate::config::{ClientPathConfig, ClientSecurityConfig};
#[cfg(test)]
use crate::ingress::ProxyAuthConfig;
use crate::model::path::RelayPathKey;
use crate::model::tcp_service::TcpServiceCarrierGroupId;
use crate::mux::MuxLimits;
use crate::performance::ResourceLimits;
use crate::product::OutboundId;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{
    PathMetricDirection, PathUsage, PeerPathState, PeerPathStatus, SessionId, UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::identity::random_session_id;
use crate::runtime::peer_status::{PeerStatusBroker, PeerStatusSnapshotSource};
use crate::runtime::recent_ids::reliable_closed_stream_cache_capacity;
use crate::runtime::stream::SessionSendBuffer;
use crate::runtime::telemetry::{ProductFlowScope, RuntimeTelemetry};
#[cfg(test)]
use crate::runtime::telemetry::{RuntimeTelemetrySnapshot, active_flow_detail_capacity};
#[cfg(test)]
use crate::transport::SystemCarrierNetworkProvider;
use crate::transport::encrypted::TcpClientTlsConfig;
use crate::transport::{CarrierNetworkProvider, CarrierPathIdentity, PathSpec, TcpCarrierRange};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Process-owned dependencies shared by every carrier in one client path group.
pub(in crate::runtime) struct ClientPathRuntimeOptions {
    pub(in crate::runtime) session_retention_timeout: Duration,
    pub(in crate::runtime) path_group_ordinal: usize,
    pub(in crate::runtime) carrier_network: Arc<dyn CarrierNetworkProvider>,
    pub(in crate::runtime) allow_peer_diagnostics: bool,
}

#[derive(Debug)]
pub(in crate::runtime) struct ClientTcpEndpointTopology {
    pub(in crate::runtime) config_index: usize,
    pub(in crate::runtime) range: TcpCarrierRange,
    pub(in crate::runtime) members: Vec<usize>,
}

#[derive(Clone)]
pub struct ClientPathContext {
    // Stable configured Product name of this MPP outbound. Local inbound
    // inventory is generation-owned and deliberately absent from carrier
    // context.
    pub(in crate::runtime) outbound: Option<OutboundId>,
    // Carrier ownership: path specs, per-path security, and live sessions belong
    // to the MPP session's carrier path registry, not to individual streams.
    /// Expanded TCP carrier slots used by Core scheduling.
    pub(in crate::runtime) tcp_paths: Arc<Vec<PathSpec>>,
    pub(in crate::runtime) udp_paths: Arc<Vec<PathSpec>>,
    /// Stable Product names aligned with each underlay-local path vector.
    pub(in crate::runtime) tcp_path_names: Arc<Vec<String>>,
    pub(in crate::runtime) udp_path_names: Arc<Vec<String>>,
    pub(in crate::runtime) tcp_path_ordinals: Arc<Vec<usize>>,
    pub(in crate::runtime) tcp_config_indices: Arc<Vec<usize>>,
    pub(in crate::runtime) tcp_member_ordinals: Arc<Vec<u16>>,
    pub(in crate::runtime) tcp_endpoint_topology: Arc<Vec<ClientTcpEndpointTopology>>,
    pub(in crate::runtime) udp_path_ordinals: Arc<Vec<usize>>,
    pub(in crate::runtime) path_group_ordinal: usize,
    pub(in crate::runtime) tcp_security: Arc<Vec<ClientSecurityConfig>>,
    pub(in crate::runtime) tcp_tls: Arc<Vec<TcpClientTlsConfig>>,
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
    pub(in crate::runtime) fn install_authenticated_path_for_test(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        path_id: crate::protocol::PathId,
        path_join_nonce: crate::protocol::AuthNonce,
        path_instance_id: crate::model::path::CarrierPathInstanceId,
        sequence: u64,
        usage: PathUsage,
    ) {
        self.state.install_authenticated_path(
            underlay,
            index,
            path_id,
            path_join_nonce,
            path_instance_id,
            sequence,
            usage,
        );
    }

    #[cfg(test)]
    pub(in crate::runtime) fn update_peer_path_usage_for_test(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        path_instance_id: crate::model::path::CarrierPathInstanceId,
        sequence: u64,
        usage: PathUsage,
    ) -> bool {
        self.state
            .update_peer_path_usage(underlay, index, path_instance_id, sequence, usage)
    }

    #[cfg(test)]
    pub fn new(
        paths: Vec<PathSpec>,
        security: ClientSecurityConfig,
        resources: ResourceLimits,
    ) -> Result<Self, RuntimeError> {
        Self::new_with_proxy_auth(paths, security, resources, ProxyAuthConfig::disabled())
    }

    #[cfg(test)]
    pub fn new_with_proxy_auth(
        paths: Vec<PathSpec>,
        security: ClientSecurityConfig,
        resources: ResourceLimits,
        proxy_auth: ProxyAuthConfig,
    ) -> Result<Self, RuntimeError> {
        let paths = paths
            .into_iter()
            .enumerate()
            .map(|(index, spec)| ClientPathConfig {
                name: format!("path-{}", index + 1),
                tls: crate::transport::encrypted::test_client_tls_config(),
                spec,
                security: security.clone(),
            })
            .collect();
        Self::new_with_path_configs_and_outbound(paths, resources, proxy_auth, None)
    }

    #[cfg(test)]
    pub fn new_with_path_configs_and_outbound(
        paths: Vec<ClientPathConfig>,
        resources: ResourceLimits,
        proxy_auth: ProxyAuthConfig,
        outbound: Option<OutboundId>,
    ) -> Result<Self, RuntimeError> {
        let mut context = Self::new_with_carrier_network(
            paths,
            resources,
            outbound,
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
        outbound: Option<OutboundId>,
        path_group_ordinal: usize,
        carrier_network: Arc<dyn CarrierNetworkProvider>,
    ) -> Result<Self, RuntimeError> {
        Self::new_with_runtime_options(
            paths,
            resources,
            outbound,
            ClientPathRuntimeOptions {
                session_retention_timeout: crate::config::DEFAULT_SESSION_RETENTION_TIMEOUT,
                path_group_ordinal,
                carrier_network,
                allow_peer_diagnostics: false,
            },
        )
    }

    #[cfg(test)]
    pub(in crate::runtime) fn new_with_runtime_options(
        paths: Vec<ClientPathConfig>,
        resources: ResourceLimits,
        outbound: Option<OutboundId>,
        runtime: ClientPathRuntimeOptions,
    ) -> Result<Self, RuntimeError> {
        let telemetry = RuntimeTelemetry::new(active_flow_detail_capacity(resources.max_streams));
        Self::new_with_runtime_options_and_telemetry(paths, resources, outbound, runtime, telemetry)
    }

    pub(in crate::runtime) fn new_with_runtime_options_and_telemetry(
        paths: Vec<ClientPathConfig>,
        resources: ResourceLimits,
        outbound: Option<OutboundId>,
        runtime: ClientPathRuntimeOptions,
        telemetry: RuntimeTelemetry,
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
        let mut tcp_config_paths = Vec::new();
        let mut tcp_path_names = Vec::new();
        let mut tcp_path_ordinals = Vec::new();
        let mut tcp_security = Vec::new();
        let mut tcp_tls = Vec::new();
        let mut udp_paths = Vec::new();
        let mut udp_path_names = Vec::new();
        let mut udp_path_ordinals = Vec::new();
        let mut udp_security = Vec::new();
        let mut udp_tls = Vec::new();
        for (ordinal, path) in paths.into_iter().enumerate() {
            let ClientPathConfig {
                name,
                spec,
                security,
                tls,
            } = path;
            match spec.underlay {
                UnderlayProtocol::Tcp => {
                    tcp_path_names.push(name);
                    tcp_path_ordinals.push(ordinal);
                    tcp_config_paths.push(spec);
                    tcp_security.push(security);
                    tcp_tls.push(tls);
                }
                UnderlayProtocol::Udp => {
                    udp_path_names.push(name);
                    udp_path_ordinals.push(ordinal);
                    udp_paths.push(spec);
                    udp_security.push(security);
                    udp_tls.push(tls);
                }
            }
        }

        let carrier_slots = tcp_config_paths
            .iter()
            .try_fold(udp_paths.len(), |total, path| {
                total.checked_add(usize::from(
                    path.tcp_carrier_range()
                        .expect("TCP configuration has TCP carrier bounds")
                        .max(),
                ))
            })
            .ok_or(RuntimeError::PathIdOverflow)?;
        if carrier_slots > resources.max_paths || carrier_slots > u16::MAX as usize {
            return Err(RuntimeError::PathIdOverflow);
        }

        // Preserve every configured endpoint's historical primary index, then
        // append its sibling slots. A second configured endpoint must never be
        // reinterpreted as the first endpoint's second carrier.
        let configured_tcp_path_names = tcp_path_names.clone();
        let configured_tcp_path_ordinals = tcp_path_ordinals.clone();
        let mut tcp_paths = tcp_config_paths.clone();
        let mut tcp_config_indices = (0..tcp_config_paths.len()).collect::<Vec<_>>();
        let mut tcp_member_ordinals = vec![0_u16; tcp_config_paths.len()];
        let mut tcp_endpoint_topology = tcp_config_paths
            .iter()
            .enumerate()
            .map(|(config_index, path)| ClientTcpEndpointTopology {
                config_index,
                range: path
                    .tcp_carrier_range()
                    .expect("TCP configuration has TCP carrier bounds"),
                members: vec![config_index],
            })
            .collect::<Vec<_>>();
        for endpoint in &mut tcp_endpoint_topology {
            for member_ordinal in 1..endpoint.range.max() {
                let path_index = tcp_paths.len();
                tcp_paths.push(tcp_config_paths[endpoint.config_index].clone());
                tcp_path_names.push(configured_tcp_path_names[endpoint.config_index].clone());
                tcp_path_ordinals.push(configured_tcp_path_ordinals[endpoint.config_index]);
                tcp_config_indices.push(endpoint.config_index);
                tcp_member_ordinals.push(member_ordinal);
                endpoint.members.push(path_index);
            }
        }

        // Context and carrier actors share one immutable configuration backing;
        // reconnecting a session must not deep-copy endpoint or secret material.
        let tcp_config_paths = Arc::new(tcp_config_paths);
        let tcp_paths = Arc::new(tcp_paths);
        let tcp_path_names = Arc::new(tcp_path_names);
        let tcp_path_ordinals = Arc::new(tcp_path_ordinals);
        let tcp_config_indices = Arc::new(tcp_config_indices);
        let tcp_member_ordinals = Arc::new(tcp_member_ordinals);
        let tcp_endpoint_topology = Arc::new(tcp_endpoint_topology);
        let tcp_security = Arc::new(tcp_security);
        let tcp_tls = Arc::new(tcp_tls);
        let udp_paths = Arc::new(udp_paths);
        let udp_path_names = Arc::new(udp_path_names);
        let udp_path_ordinals = Arc::new(udp_path_ordinals);
        let udp_security = Arc::new(udp_security);
        let udp_tls = Arc::new(udp_tls);
        let path_proof_limit = resources.max_streams.saturating_mul(2).max(1);
        let mut tcp_health = vec![
            ClientPathHealthRecord::with_path_proof_limit_and_eligibility(
                path_proof_limit,
                false,
            );
            tcp_paths.len()
        ];
        for endpoint in tcp_endpoint_topology.iter() {
            for path_index in endpoint
                .members
                .iter()
                .take(usize::from(endpoint.range.min()))
            {
                tcp_health[*path_index] =
                    ClientPathHealthRecord::with_path_proof_limit(path_proof_limit);
            }
        }
        let state = ClientPathState::new(ClientPathHealth {
            tcp: tcp_health,
            udp: vec![
                ClientPathHealthRecord::with_path_proof_limit(path_proof_limit);
                udp_paths.len()
            ],
        });
        let codec_limits = resources.into();
        let mux_limits = resources.into();
        let session_send_buffer = SessionSendBuffer::from_limits(mux_limits);
        let session_id = random_session_id()?;
        let peer_status = PeerStatusBroker::new(allow_peer_diagnostics);
        let peer_status_snapshot = PeerStatusSnapshotSource::new({
            let tcp_paths = tcp_paths.clone();
            let udp_paths = udp_paths.clone();
            let state = state.clone();
            move || client_peer_status_snapshot(&tcp_paths, &udp_paths, &state)
        });
        let tcp_sessions = (0..tcp_paths.len())
            .map(|path_index| {
                let config_index = tcp_config_indices[path_index];
                ClientTcpPathSessionHandle::new(ClientTcpPathSessionRuntime {
                    paths: tcp_config_paths.clone(),
                    config_index,
                    path_index,
                    carrier_identity: CarrierPathIdentity {
                        group_ordinal: path_group_ordinal,
                        path_ordinal: tcp_path_ordinals[path_index],
                    },
                    session_id,
                    security: tcp_security.clone(),
                    tls: tcp_tls.clone(),
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
                    candidate_selector: crate::transport::quic::QuicCandidateSelector::derive(
                        udp_security[path_index].credential.id().as_str(),
                        udp_security[path_index].credential.secret().as_bytes(),
                    ),
                    security: udp_security.clone(),
                    tls: udp_tls.clone(),
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
            outbound,
            tcp_paths,
            udp_paths,
            tcp_path_names,
            udp_path_names,
            tcp_path_ordinals,
            tcp_config_indices,
            tcp_member_ordinals,
            tcp_endpoint_topology,
            udp_path_ordinals,
            path_group_ordinal,
            tcp_security,
            tcp_tls,
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
    ) -> Result<&ClientSecurityConfig, RuntimeError> {
        let config_index = self
            .tcp_config_indices
            .get(path_index)
            .copied()
            .ok_or(RuntimeError::NoSchedulableTcpPath)?;
        self.tcp_security
            .get(config_index)
            .ok_or(RuntimeError::NoSchedulableTcpPath)
    }

    pub(in crate::runtime) fn tcp_path_tls(
        &self,
        path_index: usize,
    ) -> Result<&TcpClientTlsConfig, RuntimeError> {
        let config_index = self
            .tcp_config_indices
            .get(path_index)
            .copied()
            .ok_or(RuntimeError::NoSchedulableTcpPath)?;
        self.tcp_tls
            .get(config_index)
            .ok_or(RuntimeError::NoSchedulableTcpPath)
    }

    pub(in crate::runtime) fn tcp_config_index(&self, path_index: usize) -> Option<usize> {
        self.tcp_config_indices.get(path_index).copied()
    }

    pub(in crate::runtime) fn tcp_member_ordinal(&self, path_index: usize) -> Option<u16> {
        self.tcp_member_ordinals.get(path_index).copied()
    }

    pub(in crate::runtime) fn tcp_endpoint(
        &self,
        config_index: usize,
    ) -> Option<&ClientTcpEndpointTopology> {
        self.tcp_endpoint_topology.get(config_index)
    }

    pub(in crate::runtime) fn configured_tcp_endpoint_count(&self) -> usize {
        self.tcp_endpoint_topology.len()
    }

    pub(in crate::runtime) fn tcp_endpoint_for_path(
        &self,
        path_index: usize,
    ) -> Option<&ClientTcpEndpointTopology> {
        self.tcp_config_index(path_index)
            .and_then(|config_index| self.tcp_endpoint(config_index))
    }

    /// Returns the session-local TCP carrier-group identity for one slot.
    ///
    /// Configured endpoint indices are stable for this context. The wire never
    /// carries this identity, and zero remains unavailable for checked
    /// conversion from the zero-based configuration index.
    pub(in crate::runtime) fn tcp_service_carrier_group_id(
        &self,
        path_index: usize,
    ) -> Option<TcpServiceCarrierGroupId> {
        let config_index = self.tcp_config_index(path_index)?;
        let raw = u64::try_from(config_index).ok()?.checked_add(1)?;
        Some(TcpServiceCarrierGroupId::from_raw(raw))
    }

    pub(in crate::runtime) fn tcp_service_endpoint(
        &self,
        carrier_group_id: TcpServiceCarrierGroupId,
    ) -> Option<&ClientTcpEndpointTopology> {
        let config_index = carrier_group_id
            .raw()
            .checked_sub(1)
            .and_then(|index| usize::try_from(index).ok())?;
        self.tcp_endpoint(config_index)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn telemetry_snapshot(&self) -> RuntimeTelemetrySnapshot {
        self.telemetry.snapshot()
    }

    /// Clone one already-built MPP context with flow-local Product identity.
    ///
    /// Carrier registries remain shared; only the lightweight telemetry handle
    /// differs, so selection never duplicates path/session state.
    pub(in crate::runtime) fn with_product_flow_scope(&self, scope: ProductFlowScope) -> Self {
        let mut scoped = self.clone();
        scoped.telemetry = self.telemetry.scoped(scope);
        scoped
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
            .filter(|(_, (_, record))| record.is_locally_eligible())
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
