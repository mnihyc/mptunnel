//! Shared server configuration and carrier-path registries.

use crate::config::SecurityConfig;
use crate::model::path::PathPolicy;
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{
    AuthNonce, PathId, PathMetricDirection, PathMetrics, PeerPathStatus, SessionId,
    UnderlayProtocol,
};
use crate::runtime::path::model::path_startup_metrics;
use crate::runtime::path::{ServerDatagramPort, ServerStreamPort};
use crate::runtime::peer_status::PeerStatusBroker;
use crate::runtime::recent_ids::RecentIdCache;
use crate::runtime::telemetry::{RuntimeTelemetry, RuntimeTelemetrySnapshot};
use crate::transport::PathSpec;
use std::sync::{Arc, Mutex};

/// Endpoint-local configuration retained by the accepting listener.
///
/// The authenticated peer `PathId` is a wire identity and must never index
/// this endpoint's independently ordered configuration.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct ServerLocalPath {
    config_ordinal: usize,
    spec: PathSpec,
}

impl ServerLocalPath {
    pub(in crate::runtime) fn new(config_ordinal: usize, spec: PathSpec) -> Self {
        Self {
            config_ordinal,
            spec,
        }
    }

    pub(in crate::runtime) fn underlay(&self) -> UnderlayProtocol {
        self.spec.underlay
    }

    pub(in crate::runtime) fn config_ordinal(&self) -> usize {
        self.config_ordinal
    }

    pub(in crate::runtime) fn policy(&self) -> PathPolicy {
        self.spec.metadata.policy
    }

    pub(in crate::runtime) fn startup_metrics(&self, path_id: PathId) -> PathMetrics {
        PathMetrics {
            path_id,
            ..path_startup_metrics(&self.spec, path_id, PathMetricDirection::ServerToClient)
        }
    }

    pub(in crate::runtime) fn advertised_usage(&self) -> crate::protocol::PathUsage {
        if self.policy().backup {
            crate::protocol::PathUsage::Backup
        } else {
            crate::protocol::PathUsage::Available
        }
    }
}

/// Immutable server policy plus registries shared by every carrier listener.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct ServerPathContext {
    pub(in crate::runtime) tag: Option<String>,
    pub(in crate::runtime) server_paths: Arc<Vec<PathSpec>>,
    pub(in crate::runtime) codec_limits: CodecLimits,
    pub(in crate::runtime) mux_limits: MuxLimits,
    pub(in crate::runtime) security: SecurityConfig,
    pub(in crate::runtime) reliable_streams: ServerStreamPort,
    pub(in crate::runtime) datagrams: ServerDatagramPort,
    pub(in crate::runtime) peer_status: PeerStatusBroker,
    pub(in crate::runtime) telemetry: RuntimeTelemetry,
    pub(in crate::runtime) path_join_replay: Arc<Mutex<RecentIdCache<PathJoinReplayKey>>>,
    pub(in crate::runtime) max_udp_flows_per_session: usize,
}

impl ServerPathContext {
    pub(in crate::runtime) fn peer_status_snapshot(
        &self,
        session_id: SessionId,
    ) -> Vec<PeerPathStatus> {
        self.reliable_streams.peer_status_snapshot(session_id)
    }

    pub(in crate::runtime) fn telemetry_snapshot(&self) -> RuntimeTelemetrySnapshot {
        self.telemetry.snapshot()
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

#[cfg(test)]
#[path = "server_context_test.rs"]
mod tests;
