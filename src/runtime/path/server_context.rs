//! Shared server configuration and carrier-path registries.

use crate::config::{RouteTarget, SecurityConfig};
use crate::mux::MuxLimits;
use crate::outbound::{DnsConfig, OutboundConfig};
use crate::protocol::codec::CodecLimits;
use crate::protocol::{
    AuthNonce, PathId, PathMetricDirection, PathMetrics, SessionId, UnderlayProtocol,
};
use crate::runtime::path::model::path_startup_metrics;
use crate::runtime::recent_ids::RecentIdCache;
use crate::runtime::stream::ServerReliableStreamRegistry;
use crate::transport::PathSpec;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Immutable server policy plus registries shared by every carrier listener.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct ServerPathContext {
    pub(in crate::runtime) tag: Option<String>,
    pub(in crate::runtime) route_target: Option<RouteTarget>,
    pub(in crate::runtime) server_paths: Arc<Vec<PathSpec>>,
    pub(in crate::runtime) outbound: OutboundConfig,
    pub(in crate::runtime) outbound_dns: DnsConfig,
    pub(in crate::runtime) outbound_connect_timeout: Duration,
    pub(in crate::runtime) codec_limits: CodecLimits,
    pub(in crate::runtime) mux_limits: MuxLimits,
    pub(in crate::runtime) security: SecurityConfig,
    pub(in crate::runtime) reliable_streams: Arc<ServerReliableStreamRegistry>,
    pub(in crate::runtime) path_join_replay: Arc<Mutex<RecentIdCache<PathJoinReplayKey>>>,
    pub(in crate::runtime) max_udp_flows_per_session: usize,
}

impl ServerPathContext {
    pub(in crate::runtime) fn local_path_startup_metrics(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
    ) -> Option<PathMetrics> {
        let index = usize::from(path_id.0);
        let path = self.server_paths.get(index)?;
        (path.underlay == underlay)
            .then(|| path_startup_metrics(path, index, PathMetricDirection::ServerToClient))
    }

    pub(in crate::runtime) fn accept_path_join_nonce(
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(in crate::runtime) struct PathJoinReplayKey {
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) underlay: UnderlayProtocol,
    pub(in crate::runtime) nonce: AuthNonce,
}
