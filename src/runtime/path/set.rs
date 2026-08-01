//! Client carrier inventory and its transactional mutable path state.
//!
//! Capacity reservations stay beside health publication and lease rollback so
//! no sender or carrier can observe half of a request-probe transaction.

use super::carrier_inventory::AuthenticatedCarrierInventory;
use super::commands::reliable_stream_frame_queue;
use super::health::{ClientPathHealth, ClientPathHealthRecord};
use super::model::{
    ClientPathObservation, path_metrics_from_snapshot, path_snapshot, path_snapshot_with_id,
};
use super::quic::client::{ClientUdpPathSessionHandle, ClientUdpPathSessionRuntime};
use super::state::ClientPathState;
use super::tcp::client::{
    ClientTcpPathSessionHandle, ClientTcpPathSessionRuntime, tcp_session_command_queue,
};
use super::tcp::group::{ClientTcpCarrierGroup, ClientTcpCarrierGroups};
use super::tcp::retained::ClientTcpRetainedCarrierRegistry;
use crate::config::ClientPathConfig;
#[cfg(test)]
use crate::config::ClientSecurityConfig;
#[cfg(test)]
use crate::ingress::ProxyAuthConfig;
use crate::model::path::RelayPathKey;
use crate::mux::MuxLimits;
use crate::performance::ResourceLimits;
use crate::product::OutboundId;
use crate::protocol::{
    PathId, PathMetricDirection, PathUsage, PeerPathState, PeerPathStatus, SessionId,
    UnderlayProtocol,
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
#[cfg(test)]
use crate::transport::encrypted::TcpClientTlsConfig;
use crate::transport::{CarrierNetworkProvider, CarrierPathIdentity, PathSpec};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Process-owned dependencies shared by every carrier in one client path group.
pub(in crate::runtime) struct ClientPathRuntimeOptions {
    pub(in crate::runtime) session_retention_timeout: Duration,
    /// Existing carrier-establishment ceiling shared by minimum reconciliation
    /// and elastic validation; validation introduces no independent timer.
    pub(in crate::runtime) path_probe_timeout: Duration,
    pub(in crate::runtime) path_group_ordinal: usize,
    pub(in crate::runtime) carrier_network: Arc<dyn CarrierNetworkProvider>,
    pub(in crate::runtime) allow_peer_diagnostics: bool,
}

#[derive(Clone)]
pub struct ClientPathContext {
    // Stable configured Product name of this MPP outbound. Local inbound
    // inventory is generation-owned and deliberately absent from carrier
    // context.
    pub(in crate::runtime) outbound: Option<OutboundId>,
    // Carrier ownership: path specs, per-path security, and live sessions belong
    // to the MPP session's carrier path registry, not to individual streams.
    /// Configured-minimum TCP carriers visible to ordinary Core scheduling.
    /// Elastic candidates stay outside this inventory until validation commits.
    pub(in crate::runtime) tcp_paths: Arc<Vec<PathSpec>>,
    pub(in crate::runtime) udp_paths: Arc<Vec<PathSpec>>,
    /// Stable Product names aligned with each underlay-local path vector.
    pub(in crate::runtime) tcp_path_names: Arc<Vec<String>>,
    pub(in crate::runtime) udp_path_names: Arc<Vec<String>>,
    pub(in crate::runtime) tcp_path_ordinals: Arc<Vec<usize>>,
    pub(in crate::runtime) tcp_config_indices: Arc<Vec<usize>>,
    pub(in crate::runtime) tcp_member_ordinals: Arc<Vec<u16>>,
    pub(in crate::runtime) tcp_carrier_groups: Arc<ClientTcpCarrierGroups>,
    pub(in crate::runtime) tcp_retained_carriers: Arc<ClientTcpRetainedCarrierRegistry>,
    pub(in crate::runtime) udp_path_ordinals: Arc<Vec<usize>>,
    #[cfg(test)]
    pub(in crate::runtime) tcp_security: Arc<Vec<ClientSecurityConfig>>,
    #[cfg(test)]
    pub(in crate::runtime) tcp_tls: Arc<Vec<TcpClientTlsConfig>>,
    pub(in crate::runtime) tcp_sessions: Arc<Vec<ClientTcpPathSessionHandle>>,
    pub(in crate::runtime) udp_sessions: Arc<Vec<ClientUdpPathSessionHandle>>,
    pub(super) state: Arc<ClientPathState>,
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) telemetry: RuntimeTelemetry,
    pub(in crate::runtime) peer_status: PeerStatusBroker,
    pub(in crate::runtime) authenticated_carriers: AuthenticatedCarrierInventory,
    pub(in crate::runtime) mux_limits: MuxLimits,
    /// RFC 8684 break-before-make lifetime for established logical streams.
    pub(in crate::runtime) session_retention_timeout: std::time::Duration,
    pub(in crate::runtime) path_probe_timeout: std::time::Duration,
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
                path_probe_timeout: crate::config::DEFAULT_PATH_PROBE_TIMEOUT,
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
            path_probe_timeout,
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
        // append only its configured-minimum siblings. Unopened elastic
        // capacity is not an ordinary path, health record, or session actor.
        let configured_tcp_path_names = tcp_path_names.clone();
        let configured_tcp_path_ordinals = tcp_path_ordinals.clone();
        let mut tcp_paths = tcp_config_paths.clone();
        let mut tcp_config_indices = (0..tcp_config_paths.len()).collect::<Vec<_>>();
        let mut tcp_member_ordinals = vec![0_u16; tcp_config_paths.len()];
        let mut tcp_carrier_groups = tcp_config_paths
            .iter()
            .enumerate()
            .map(|(config_index, path)| {
                ClientTcpCarrierGroup::new(
                    config_index,
                    path.tcp_carrier_range()
                        .expect("TCP configuration has TCP carrier bounds"),
                    vec![config_index],
                )
            })
            .collect::<Vec<_>>();
        for group in &mut tcp_carrier_groups {
            for member_ordinal in 1..group.range.min() {
                let path_index = tcp_paths.len();
                tcp_paths.push(tcp_config_paths[group.config_index].clone());
                tcp_path_names.push(configured_tcp_path_names[group.config_index].clone());
                tcp_path_ordinals.push(configured_tcp_path_ordinals[group.config_index]);
                tcp_config_indices.push(group.config_index);
                tcp_member_ordinals.push(member_ordinal);
                group.members.push(path_index);
            }
        }
        let mut next_elastic_path_index = tcp_paths.len();
        for group in &mut tcp_carrier_groups {
            for _ in group.range.min()..group.range.max() {
                group.elastic_slots.push(next_elastic_path_index);
                next_elastic_path_index = next_elastic_path_index
                    .checked_add(1)
                    .ok_or(RuntimeError::PathIdOverflow)?;
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
        let tcp_carrier_groups = ClientTcpCarrierGroups::new(tcp_carrier_groups);
        let tcp_retained_carriers = ClientTcpRetainedCarrierRegistry::new();
        let tcp_security = Arc::new(tcp_security);
        let tcp_tls = Arc::new(tcp_tls);
        let udp_paths = Arc::new(udp_paths);
        let udp_path_names = Arc::new(udp_path_names);
        let udp_path_ordinals = Arc::new(udp_path_ordinals);
        let udp_security = Arc::new(udp_security);
        let udp_tls = Arc::new(udp_tls);
        let path_proof_limit = resources.max_streams.saturating_mul(2).max(1);
        let state = ClientPathState::new_with_tcp_path_slot_count(
            ClientPathHealth::new(
                vec![
                    ClientPathHealthRecord::with_path_proof_limit(path_proof_limit);
                    tcp_paths.len()
                ],
                vec![
                    ClientPathHealthRecord::with_path_proof_limit(path_proof_limit);
                    udp_paths.len()
                ],
            ),
            next_elastic_path_index,
        );
        let codec_limits = resources.into();
        let mux_limits = resources.into();
        let session_send_buffer = SessionSendBuffer::from_limits(mux_limits);
        let session_id = random_session_id()?;
        let peer_status = PeerStatusBroker::new(allow_peer_diagnostics);
        let authenticated_carriers = AuthenticatedCarrierInventory::default();
        let peer_status_snapshot = PeerStatusSnapshotSource::new({
            let tcp_paths = tcp_paths.clone();
            let udp_paths = udp_paths.clone();
            let tcp_carrier_groups = tcp_carrier_groups.clone();
            let state = state.clone();
            let authenticated_carriers = authenticated_carriers.clone();
            move || {
                client_peer_status_snapshot(
                    &tcp_paths,
                    &udp_paths,
                    &tcp_carrier_groups,
                    &state,
                    &authenticated_carriers,
                )
            }
        });
        let tcp_sessions = (0..tcp_paths.len())
            .map(|path_index| {
                let config_index = tcp_config_indices[path_index];
                let endpoint_policy = tcp_carrier_groups
                    .endpoint_policy(config_index)
                    .expect("TCP carrier group must own endpoint policy");
                ClientTcpPathSessionHandle::new(ClientTcpPathSessionRuntime {
                    paths: tcp_config_paths.clone(),
                    config_index,
                    path_index,
                    path_id: None,
                    remote_port: None,
                    purpose: crate::protocol::PathPurpose::Ordinary,
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
                    session_retention_timeout,
                    state: state.clone(),
                    carrier_network: carrier_network.clone(),
                    peer_status: peer_status.clone(),
                    peer_status_snapshot: peer_status_snapshot.clone(),
                    authenticated_carriers: authenticated_carriers.clone(),
                    endpoint_policy,
                    carrier_groups: tcp_carrier_groups.clone(),
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
                    authenticated_carriers: authenticated_carriers.clone(),
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
            tcp_carrier_groups,
            tcp_retained_carriers,
            udp_path_ordinals,
            #[cfg(test)]
            tcp_security,
            #[cfg(test)]
            tcp_tls,
            tcp_sessions: Arc::new(tcp_sessions),
            udp_sessions: Arc::new(udp_sessions),
            state,
            session_id,
            telemetry,
            peer_status,
            authenticated_carriers,
            mux_limits,
            session_retention_timeout,
            path_probe_timeout,
            session_send_buffer,
            #[cfg(test)]
            proxy_auth: ProxyAuthConfig::disabled(),
        })
    }

    #[cfg(test)]
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

    #[cfg(test)]
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
        self.tcp_config_indices
            .get(path_index)
            .copied()
            .or_else(|| {
                self.tcp_carrier_groups
                    .elastic_path_owner(path_index)
                    .map(|(config_index, _)| config_index)
            })
    }

    pub(in crate::runtime) fn tcp_member_ordinal(&self, path_index: usize) -> Option<u16> {
        self.tcp_member_ordinals
            .get(path_index)
            .copied()
            .or_else(|| {
                self.tcp_carrier_groups
                    .elastic_path_owner(path_index)
                    .map(|(_, member_ordinal)| member_ordinal)
            })
    }

    /// Resolves a configured-minimum member directly and an elastic slot
    /// through immutable endpoint ownership. Callers still require active
    /// health or an exact reservation before the slot has any authority.
    pub(in crate::runtime) fn tcp_path_spec(&self, path_index: usize) -> Option<&PathSpec> {
        self.tcp_paths.get(path_index).or_else(|| {
            self.tcp_carrier_groups
                .elastic_path_owner(path_index)
                .and_then(|(config_index, _)| self.tcp_paths.get(config_index))
        })
    }

    pub(in crate::runtime) fn tcp_path_config_ordinal(&self, path_index: usize) -> Option<usize> {
        self.tcp_path_ordinals.get(path_index).copied().or_else(|| {
            self.tcp_carrier_groups
                .elastic_path_owner(path_index)
                .and_then(|(config_index, _)| self.tcp_path_ordinals.get(config_index).copied())
        })
    }

    pub(in crate::runtime) fn tcp_endpoint(
        &self,
        config_index: usize,
    ) -> Option<&ClientTcpCarrierGroup> {
        self.tcp_carrier_groups.get(config_index)
    }

    pub(in crate::runtime) fn configured_tcp_endpoint_count(&self) -> usize {
        self.tcp_carrier_groups.len()
    }

    pub(in crate::runtime) fn tcp_endpoint_for_path(
        &self,
        path_index: usize,
    ) -> Option<&ClientTcpCarrierGroup> {
        self.tcp_config_index(path_index)
            .and_then(|config_index| self.tcp_endpoint(config_index))
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
            UnderlayProtocol::Tcp => self.tcp_path_config_ordinal(key.index),
            UnderlayProtocol::Udp => self.udp_path_ordinals.get(key.index).copied(),
        }
        .unwrap_or(usize::MAX)
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
    tcp_carrier_groups: &ClientTcpCarrierGroups,
    state: &ClientPathState,
    authenticated_carriers: &AuthenticatedCarrierInventory,
) -> Option<Vec<PeerPathStatus>> {
    let now = Instant::now();
    let health = state.health().lock().expect("client path health lock");
    if tcp_paths.len() != health.tcp.len() || udp_paths.len() != health.udp.len() {
        return None;
    }
    let represented_authenticated_carriers = health
        .tcp_records()
        .chain(&health.udp)
        .filter(|record| record.has_live_authenticated_carrier())
        .count();
    if represented_authenticated_carriers != authenticated_carriers.snapshot().live_count {
        return None;
    }

    let mut paths = Vec::with_capacity(health.tcp_records().count() + udp_paths.len());
    let mut tcp_path_ids = std::collections::HashSet::with_capacity(health.tcp_records().count());
    for (path_index, record) in health.tcp_records_with_indices() {
        if !record.has_live_authenticated_carrier() {
            continue;
        }
        let path = tcp_paths.get(path_index).or_else(|| {
            tcp_carrier_groups
                .elastic_path_owner(path_index)
                .and_then(|(config_index, _)| tcp_paths.get(config_index))
        })?;
        let observation = record.observation_at(now);
        let path_id = observation.wire_path_id?;
        if !tcp_path_ids.insert(path_id) {
            return None;
        }
        paths.push(peer_path_status_with_id(path, path_id, observation));
    }
    paths.extend(
        udp_paths
            .iter()
            .zip(&health.udp)
            .enumerate()
            .map(|(index, (path, record))| {
                peer_path_status(path, index, record.observation_at(now))
            }),
    );
    Some(paths)
}

fn peer_path_status_with_id(
    path: &PathSpec,
    path_id: PathId,
    observation: ClientPathObservation,
) -> PeerPathStatus {
    peer_path_status_from_snapshot(
        path,
        path_snapshot_with_id(path, path_id, observation),
        observation,
    )
}

#[cfg(test)]
#[path = "set_test.rs"]
mod tests;

fn peer_path_status(
    path: &PathSpec,
    index: usize,
    observation: ClientPathObservation,
) -> PeerPathStatus {
    let snapshot = path_snapshot(path, index, observation);
    peer_path_status_from_snapshot(path, snapshot, observation)
}

fn peer_path_status_from_snapshot(
    path: &PathSpec,
    snapshot: crate::scheduler::PathSnapshot,
    observation: ClientPathObservation,
) -> PeerPathStatus {
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
