//! Client carrier inventory and its transactional mutable path state.
//!
//! Capacity reservations stay beside health publication and lease rollback so
//! no sender or carrier can observe half of a request-probe transaction.

use super::commands::reliable_stream_frame_queue;
use super::quic::client::{ClientUdpPathSessionHandle, ClientUdpPathSessionRuntime};
use super::state::{ClientPathHealth, ClientPathHealthRecord, ClientPathState};
use super::tcp::client::{
    ClientTcpPathSessionHandle, ClientTcpPathSessionRuntime, tcp_session_command_queue,
};
use crate::config::{
    ClientPathConfig, LocalIngressConfig, ResourceLimits, RouteTarget, SecurityConfig,
};
use crate::ingress::ProxyAuthConfig;
use crate::model::path::RelayPathKey;
use crate::mux::MuxLimits;
use crate::protocol::UnderlayProtocol;
use crate::protocol::codec::CodecLimits;
use crate::runtime::error::RuntimeError;
use crate::runtime::identity::random_session_id;
use crate::runtime::recent_ids::reliable_closed_stream_cache_capacity;
#[cfg(test)]
use crate::transport::SystemCarrierNetworkProvider;
use crate::transport::{CarrierNetworkProvider, CarrierPathIdentity, PathSpec};
use std::sync::Arc;

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
    pub(in crate::runtime) codec_limits: CodecLimits,
    pub(in crate::runtime) mux_limits: MuxLimits,
    #[cfg(test)]
    pub(in crate::runtime) proxy_auth: ProxyAuthConfig,
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

    #[cfg(test)]
    pub fn new_with_path_configs_and_target(
        paths: Vec<ClientPathConfig>,
        resources: ResourceLimits,
        proxy_auth: ProxyAuthConfig,
        route_target: Option<RouteTarget>,
        ingresses: Vec<LocalIngressConfig>,
    ) -> Result<Self, RuntimeError> {
        Self::new_with_carrier_network(
            paths,
            resources,
            proxy_auth,
            route_target,
            ingresses,
            0,
            Arc::new(SystemCarrierNetworkProvider),
        )
    }

    pub fn new_with_carrier_network(
        paths: Vec<ClientPathConfig>,
        resources: ResourceLimits,
        proxy_auth: ProxyAuthConfig,
        route_target: Option<RouteTarget>,
        ingresses: Vec<LocalIngressConfig>,
        path_group_ordinal: usize,
        carrier_network: Arc<dyn CarrierNetworkProvider>,
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
        let session_id = random_session_id()?;
        let tcp_sessions = tcp_paths
            .iter()
            .cloned()
            .enumerate()
            .map(|(path_index, path)| {
                ClientTcpPathSessionHandle::new(ClientTcpPathSessionRuntime {
                    path,
                    path_index,
                    carrier_identity: CarrierPathIdentity {
                        group_ordinal: path_group_ordinal,
                        path_ordinal: tcp_path_ordinals[path_index],
                    },
                    session_id,
                    security: tcp_security[path_index].clone(),
                    codec_limits,
                    mux_limits,
                    command_queue: tcp_session_command_queue(resources),
                    stream_frame_queue: reliable_stream_frame_queue(mux_limits),
                    closed_stream_cache_capacity: reliable_closed_stream_cache_capacity(
                        resources.max_streams,
                    ),
                    state: state.clone(),
                    carrier_network: carrier_network.clone(),
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
                    carrier_identity: CarrierPathIdentity {
                        group_ordinal: path_group_ordinal,
                        path_ordinal: udp_path_ordinals[path_index],
                    },
                    session_id,
                    security: udp_security[path_index].clone(),
                    codec_limits,
                    mux_limits,
                    stream_frame_queue: reliable_stream_frame_queue(mux_limits),
                    state: state.clone(),
                    carrier_network: carrier_network.clone(),
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
            path_group_ordinal,
            tcp_security: Arc::new(tcp_security),
            tcp_sessions: Arc::new(tcp_sessions),
            udp_sessions: Arc::new(udp_sessions),
            carrier_network,
            state,
            codec_limits,
            mux_limits,
            #[cfg(test)]
            proxy_auth,
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
