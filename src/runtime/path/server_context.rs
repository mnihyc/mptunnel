//! Shared server configuration and carrier-path registries.

use crate::config::ServerSecurityConfig;
use crate::model::path::PathPolicy;
use crate::mux::MuxLimits;
use crate::product::{
    CredentialAdmissionError, CredentialAuthority, CredentialId, PrincipalPermit,
};
use crate::protocol::codec::CodecLimits;
use crate::protocol::{
    AuthNonce, PathId, PathMetricDirection, PathMetrics, PeerPathStatus, SessionId,
    UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::authentication::ProductCredentialAdmission;
use crate::runtime::path::model::path_startup_metrics;
use crate::runtime::path::{ServerDatagramPort, ServerStreamPort};
use crate::runtime::peer_status::PeerStatusBroker;
use crate::runtime::recent_ids::ExpiringReplayCache;
use crate::runtime::telemetry::RuntimeTelemetry;
use crate::runtime::tun_l3::{ServerIpTunnelDevice, ServerIpTunnelPort};
use crate::transport::PathSpec;
use crate::transport::encrypted::TcpServerTlsConfig;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

#[derive(Debug, Clone)]
pub(in crate::runtime) struct CredentialRetirementControl {
    deadlines: watch::Sender<Arc<HashMap<CredentialId, u64>>>,
}

impl CredentialRetirementControl {
    pub(in crate::runtime) fn new() -> Self {
        let (deadlines, _) = watch::channel(Arc::new(HashMap::new()));
        Self { deadlines }
    }

    fn contains(&self, credential_id: &CredentialId) -> bool {
        self.deadlines.borrow().contains_key(credential_id)
    }

    fn publish(&self, retirements: impl IntoIterator<Item = (CredentialId, u64)>) {
        let mut next = self.deadlines.borrow().as_ref().clone();
        for (credential_id, deadline) in retirements {
            next.entry(credential_id)
                .and_modify(|current| *current = (*current).min(deadline))
                .or_insert(deadline);
        }
        self.deadlines.send_replace(Arc::new(next));
    }

    pub(in crate::runtime) async fn wait_for(&self, permit: PrincipalPermit) {
        let mut deadlines = self.deadlines.subscribe();
        loop {
            let dynamic_deadline = deadlines.borrow().get(permit.credential_id()).copied();
            let deadline = match (permit.forced_close_at_unix_secs(), dynamic_deadline) {
                (Some(left), Some(right)) => Some(left.min(right)),
                (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
                (None, None) => None,
            };
            let Some(deadline) = deadline else {
                if deadlines.changed().await.is_err() {
                    std::future::pending::<()>().await;
                }
                continue;
            };
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            if now >= deadline {
                return;
            }
            // Re-evaluate absolute UTC periodically so wall-clock corrections
            // cannot postpone retirement indefinitely.
            let delay = Duration::from_secs(deadline.saturating_sub(now).clamp(1, 60));
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                changed = deadlines.changed() => {
                    if changed.is_err() {
                        std::future::pending::<()>().await;
                    }
                }
            }
        }
    }
}

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
    /// Stable configured Product inbound name; never serialized on the wire.
    pub(in crate::runtime) name: String,
    /// Generation-wide forwarding family. It gates data-plane admission but is
    /// never serialized or inferred from a carrier.
    pub(in crate::runtime) forwarding_mode: crate::config::ForwardingMode,
    /// Stable Product names aligned with `server_paths`; never serialized.
    pub(in crate::runtime) configured_path_names: Arc<Vec<String>>,
    pub(in crate::runtime) server_paths: Arc<Vec<PathSpec>>,
    pub(in crate::runtime) codec_limits: CodecLimits,
    pub(in crate::runtime) mux_limits: MuxLimits,
    pub(in crate::runtime) security: ServerSecurityConfig,
    pub(in crate::runtime) credential_admission: Arc<ProductCredentialAdmission>,
    pub(in crate::runtime) credential_retirements: CredentialRetirementControl,
    pub(in crate::runtime) pending_authentications: Arc<Semaphore>,
    /// Bounds sockets retained solely to make ordinary Noise rejection timing
    /// uniform. This budget is deliberately independent of authentication
    /// work so rejected peers cannot starve valid admission. Its capacity
    /// equals `pending_authentications`, keeping one configured resource
    /// envelope with at most N working and N silently retained peers.
    pub(in crate::runtime) silent_rejections: Arc<Semaphore>,
    pub(in crate::runtime) tls: TcpServerTlsConfig,
    pub(in crate::runtime) reliable_streams: ServerStreamPort,
    pub(in crate::runtime) datagrams: ServerDatagramPort,
    pub(in crate::runtime) ip_tunnels: Option<ServerIpTunnelPort>,
    pub(in crate::runtime) ip_tunnel_device: Arc<Mutex<Option<ServerIpTunnelDevice>>>,
    pub(in crate::runtime) peer_status: PeerStatusBroker,
    #[allow(
        dead_code,
        reason = "standalone server composition hands the shared generation owner to management through this context"
    )]
    pub(in crate::runtime) telemetry: RuntimeTelemetry,
    pub(in crate::runtime) path_join_replay: Arc<Mutex<ExpiringReplayCache<PathJoinReplayKey>>>,
    pub(in crate::runtime) max_udp_flows_per_session: usize,
    pub(in crate::runtime) session_retention_timeout: Duration,
}

impl ServerPathContext {
    pub(in crate::runtime) fn take_ip_tunnel_device(&self) -> Option<ServerIpTunnelDevice> {
        self.ip_tunnel_device
            .lock()
            .expect("server IP tunnel device lock")
            .take()
    }

    pub(in crate::runtime) fn validate_credential_authority_replacement(
        &self,
        authority: &CredentialAuthority,
    ) -> Result<(), &'static str> {
        credential_authority_retirements(
            &self.credential_admission.authority(),
            authority,
            &self.credential_retirements,
            current_unix_secs(),
        )
        .map(|_| ())
    }

    /// Publishes a validated authority and then wakes only actors admitted by
    /// credentials that became invalid. New handshakes observe the new
    /// authority immediately; existing actors retire at their bounded grace
    /// deadline without consulting policy on the data path.
    pub(in crate::runtime) fn publish_credential_authority(
        &self,
        authority: CredentialAuthority,
    ) -> Result<(), &'static str> {
        let retirements = credential_authority_retirements(
            &self.credential_admission.authority(),
            &authority,
            &self.credential_retirements,
            current_unix_secs(),
        )?;
        self.credential_admission.replace_authority(authority);
        self.credential_retirements.publish(retirements);
        Ok(())
    }

    pub(in crate::runtime) fn wait_for_credential_retirement(
        &self,
        permit: PrincipalPermit,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let retirements = self.credential_retirements.clone();
        async move {
            retirements.wait_for(permit).await;
        }
    }

    pub(in crate::runtime) fn try_begin_authentication(
        &self,
    ) -> Result<OwnedSemaphorePermit, RuntimeError> {
        self.pending_authentications
            .clone()
            .try_acquire_owned()
            .map_err(|_| RuntimeError::CredentialAdmission(CredentialAdmissionError::Overloaded))
    }

    pub(in crate::runtime) fn try_retain_silent_rejection(&self) -> Option<OwnedSemaphorePermit> {
        self.silent_rejections.clone().try_acquire_owned().ok()
    }

    pub(in crate::runtime) fn peer_status_snapshot(
        &self,
        session_id: SessionId,
    ) -> Vec<PeerPathStatus> {
        self.reliable_streams.peer_status_snapshot(session_id)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the security boundary verifies every transcript field explicitly before replay admission"
    )]
    pub(in crate::runtime) fn accept_path_join_nonce(
        &self,
        session_id: SessionId,
        credential_id: CredentialId,
        path_id: PathId,
        underlay: UnderlayProtocol,
        nonce: AuthNonce,
        issued_at_unix_secs: u64,
        verified_at_unix_secs: u64,
    ) -> bool {
        let freshness_window_secs = self.security.auth_freshness_window.as_secs();
        if freshness_window_secs == 0
            || issued_at_unix_secs.abs_diff(verified_at_unix_secs) > freshness_window_secs
        {
            return false;
        }
        let key = PathJoinReplayKey {
            session_id,
            credential_id,
            path_id,
            underlay,
            nonce,
        };
        let expires_at_unix_secs = issued_at_unix_secs.saturating_add(freshness_window_secs);
        let mut replay = self.path_join_replay.lock().expect("path join replay lock");
        replay.try_insert(key, expires_at_unix_secs, verified_at_unix_secs)
    }
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn credential_authority_retirements(
    current: &CredentialAuthority,
    next: &CredentialAuthority,
    retirement: &CredentialRetirementControl,
    now_unix_secs: u64,
) -> Result<Vec<(CredentialId, u64)>, &'static str> {
    for record in next.credentials() {
        if retirement.contains(record.id()) {
            return Err("a retired credential ID cannot be reused before process restart");
        }
        let Some(previous) = current.credential(record.id()) else {
            continue;
        };
        if previous.principal() != record.principal() || previous.secret() != record.secret() {
            return Err("credential principal or secret changes require a new credential ID");
        }
        if previous.revoked() && !record.revoked() {
            return Err("credential revocation is monotonic; rotate to a new credential ID");
        }
    }

    let mut retirements = Vec::new();
    for previous in current.credentials() {
        let Some(next_record) = next.credential(previous.id()) else {
            retirements.push((
                previous.id().clone(),
                now_unix_secs.saturating_add(previous.revocation_grace_secs()),
            ));
            continue;
        };
        if !previous.revoked() && next_record.revoked() {
            retirements.push((
                previous.id().clone(),
                now_unix_secs.saturating_add(next_record.revocation_grace_secs()),
            ));
            continue;
        }
        let previous_deadline = previous
            .expires_at_unix_secs()
            .map(|expiry| expiry.saturating_add(previous.revocation_grace_secs()));
        let next_deadline = next_record
            .expires_at_unix_secs()
            .map(|expiry| expiry.saturating_add(next_record.revocation_grace_secs()));
        if let Some(next_deadline) = next_deadline
            && previous_deadline.is_none_or(|previous_deadline| next_deadline < previous_deadline)
        {
            retirements.push((previous.id().clone(), next_deadline));
        }
    }
    Ok(retirements)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(in crate::runtime) struct PathJoinReplayKey {
    // PATH_JOIN is the completed authentication-flight replay identity: its
    // MAC covers the session, credential, path, underlay, nonce, and issue
    // time. A replayed SESSION_AUTH cannot enter
    // runtime state without a valid PATH_JOIN, so admitting only after this key
    // verifies also avoids reserving replay capacity for incomplete handshakes.
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) credential_id: CredentialId,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) underlay: UnderlayProtocol,
    pub(in crate::runtime) nonce: AuthNonce,
}

#[cfg(test)]
#[path = "tests_server_context.rs"]
mod tests;
