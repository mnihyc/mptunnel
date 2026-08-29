//! Shared client-path evidence and carrier-neutral capacity budgets.
//!
//! One health lock makes mixed TCP/QUIC observations coherent. Each carrier
//! module owns its reservation, proof, and rollback transaction.

use super::client_session::{ClientSessionLifecycle, ClientSessionRetirement};
use super::commands::CapacityProbeCommandTicket;
use super::health::{ClientPathHealth, ClientPathHealthRecord, RequestCapacityReconciliationView};
#[cfg(test)]
use super::model::PathDeliveryStats;
use super::model::{
    ClientPathObservation, UdpDatagramPathObservation, path_observation_is_idle_for_probe,
    path_records_have_schedulable_alternative,
};
#[cfg(test)]
use super::proof::PathProofObservation;
use super::set::ClientPathContext;
use super::tcp::capacity::{
    RequestTcpCapacityProbeLease, RequestTcpCapacityProbeSession, RequestTcpCapacityProofQuery,
};
use super::tcp::group::{ClientTcpCarrierGroups, ClientTcpEndpointPolicy};
#[cfg(test)]
use super::*;
use crate::model::capacity::{PathRateSample, reliable_capacity_measurement_session_limit_bytes};
use crate::model::path::{CarrierPathInstanceId, RelayPathInstance, RelayPathKey};
use crate::protocol::{DatagramFlowId, PathId, PathUsage, StreamId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::scheduler::TrafficClass;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

pub(in crate::runtime) type ArmedClientPathModelPublication =
    Pin<Box<dyn Future<Output = ()> + Send>>;

pub(in crate::runtime) struct ClientTcpCarrierPublication {
    pub(in crate::runtime) path_index: usize,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime) peer_usage_sequence: u64,
    pub(in crate::runtime) peer_usage: PathUsage,
    pub(in crate::runtime) readiness_rtt: Option<Duration>,
}

/// Coherent health/load state plus a narrow physical-lifecycle transaction.
#[derive(Debug)]
pub(in crate::runtime) struct ClientPathState {
    // Serializes only authority-bearing physical-carrier publication,
    // retirement, failure, and Product-open commit. Sampled RTT/rate/loss
    // telemetry remains on `health` alone and cannot delay lifecycle commits.
    carrier_lifecycle: Mutex<()>,
    health: Mutex<ClientPathHealth>,
    session_lifecycle: ClientSessionLifecycle,
    // Accessed only while `health` is locked so logical Product ownership and
    // physical replacement share one transaction boundary.
    active_product_flows: AtomicU64,
    next_reliable_stream_id: Mutex<u64>,
    next_datagram_flow_id: Mutex<u64>,
    request_tcp_capacity_probe: RequestTcpCapacityProbeSession,
    // Measurement and eligibility publication is separate from transient
    // Product load mutation. A blocked ACK-gap owner needs the former to make
    // a newly measured alternate selectable, while the latter would create a
    // self-wake loop during ordinary queue/in-flight accounting.
    path_model_generation: AtomicU64,
    path_model_publication: Arc<Notify>,
}

impl ClientPathState {
    pub(in crate::runtime) fn new(health: ClientPathHealth) -> Arc<Self> {
        let tcp_path_count = health.tcp.len();
        Arc::new(Self {
            carrier_lifecycle: Mutex::new(()),
            health: Mutex::new(health),
            session_lifecycle: ClientSessionLifecycle::new(),
            active_product_flows: AtomicU64::new(0),
            next_reliable_stream_id: Mutex::new(0),
            next_datagram_flow_id: Mutex::new(0),
            request_tcp_capacity_probe: RequestTcpCapacityProbeSession::new(tcp_path_count),
            path_model_generation: AtomicU64::new(0),
            path_model_publication: Arc::new(Notify::new()),
        })
    }

    pub(in crate::runtime) fn health(&self) -> &Mutex<ClientPathHealth> {
        &self.health
    }

    pub(in crate::runtime) fn session_lifecycle(&self) -> &ClientSessionLifecycle {
        &self.session_lifecycle
    }

    pub(in crate::runtime) fn session_retirement(&self) -> ClientSessionRetirement {
        self.session_lifecycle.retirement()
    }

    /// Applies one path mutation while owning the coherent health lock.
    pub(in crate::runtime) fn mutate_path_eligibility<R>(
        &self,
        key: RelayPathKey,
        mutation: impl FnOnce(&mut ClientPathHealthRecord) -> R,
    ) -> Option<R> {
        let mut health = self.health.lock().expect("client path health lock");
        let record = health.path_record_mut(key)?;
        let result = mutation(record);
        Some(result)
    }

    /// Publishes a measurement/eligibility mutation after releasing the
    /// coherent health lock. Transient Product load accounting deliberately
    /// continues to use `mutate_path_eligibility` directly.
    pub(in crate::runtime) fn mutate_path_model<R>(
        &self,
        key: RelayPathKey,
        mutation: impl FnOnce(&mut ClientPathHealthRecord) -> R,
    ) -> Option<R> {
        let result = self.mutate_path_eligibility(key, mutation)?;
        self.path_model_generation.fetch_add(1, Ordering::Release);
        self.path_model_publication.notify_waiters();
        Some(result)
    }

    pub(in crate::runtime) fn path_model_generation(&self) -> u64 {
        self.path_model_generation.load(Ordering::Acquire)
    }

    /// Arms the notification before re-reading the generation. This covers
    /// both publication-before-arm and publication-after-arm without polling.
    pub(in crate::runtime) fn arm_path_model_publication(
        &self,
        observed_generation: u64,
    ) -> ArmedClientPathModelPublication {
        let mut publication = Box::pin(self.path_model_publication.clone().notified_owned());
        publication.as_mut().enable();
        if self.path_model_generation() != observed_generation {
            Box::pin(std::future::ready(()))
        } else {
            publication
        }
    }

    pub(in crate::runtime) fn tcp_path_observation_for_instance(
        &self,
        index: usize,
        path_instance_id: CarrierPathInstanceId,
    ) -> Option<ClientPathObservation> {
        self.health
            .lock()
            .expect("client path health lock")
            .tcp_record(index)?
            .observation_for_instance_at(path_instance_id, Instant::now())
    }

    /// Installs the first preference from a newly authenticated carrier. A new
    /// carrier instance restarts its sequence space at zero.
    #[cfg(test)]
    pub(in crate::runtime) fn install_peer_path_usage(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        path_instance_id: CarrierPathInstanceId,
        sequence: u64,
        usage: PathUsage,
    ) {
        let _lifecycle = self
            .carrier_lifecycle
            .lock()
            .expect("client carrier lifecycle lock");
        let mut health = self.health.lock().expect("client path health lock");
        if let Some(record) = health.path_record_mut(RelayPathKey { underlay, index }) {
            record.install_peer_usage(path_instance_id, sequence, usage);
        }
    }

    pub(in crate::runtime) fn path_instance_id(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
    ) -> Option<CarrierPathInstanceId> {
        self.health
            .lock()
            .expect("client path health lock")
            .path_record(RelayPathKey { underlay, index })
            .and_then(ClientPathHealthRecord::path_instance_id)
    }

    /// Publishes the physical QUIC owner and its scheduler identity in one
    /// owner-held lifecycle transaction. The caller already owns the QUIC
    /// connection slot, establishing the global order
    /// `connection owner -> carrier lifecycle -> health`.
    pub(in crate::runtime) fn publish_udp_peer_path_usage_committed(
        &self,
        index: usize,
        path_instance_id: CarrierPathInstanceId,
        sequence: u64,
        usage: PathUsage,
        carrier_is_live: impl FnOnce() -> bool,
        publish_owner: impl FnOnce(),
    ) -> bool {
        let _lifecycle = self
            .carrier_lifecycle
            .lock()
            .expect("client carrier lifecycle lock");
        let mut health = self.health.lock().expect("client path health lock");
        if !carrier_is_live() {
            return false;
        }
        let Some(record) = health.udp.get_mut(index) else {
            return false;
        };
        record.install_peer_usage(path_instance_id, sequence, usage);
        publish_owner();
        true
    }

    /// Publishes exact physical failure and removes the matching QUIC owner in
    /// the same lifecycle transaction. `retire_owner` must not wait or acquire
    /// another lock; the caller already holds the connection-owner mutex.
    pub(in crate::runtime) fn settle_udp_path_instance_failure(
        &self,
        index: usize,
        path_instance_id: CarrierPathInstanceId,
        retire_owner: impl FnOnce(),
    ) -> bool {
        let _lifecycle = self
            .carrier_lifecycle
            .lock()
            .expect("client carrier lifecycle lock");
        let now = Instant::now();
        let mut health = self.health.lock().expect("client path health lock");
        let has_schedulable_alternative =
            path_records_have_schedulable_alternative(&health.udp, index, now);
        let marked = health.udp.get_mut(index).is_some_and(|record| {
            record.mark_data_plane_failure(path_instance_id, now, has_schedulable_alternative)
        });
        // The connection owner is authoritative for physical membership even
        // when an exact callback already published the same health failure.
        retire_owner();
        marked
    }

    pub(in crate::runtime) fn mark_udp_path_establishment_failure_if_current(
        &self,
        index: usize,
        expected_path_instance_id: Option<CarrierPathInstanceId>,
    ) -> bool {
        let _lifecycle = self
            .carrier_lifecycle
            .lock()
            .expect("client carrier lifecycle lock");
        let now = Instant::now();
        let mut health = self.health.lock().expect("client path health lock");
        let has_schedulable_alternative =
            path_records_have_schedulable_alternative(&health.udp, index, now);
        let Some(current) = health.udp.get_mut(index) else {
            return false;
        };
        if !current.accepts_endpoint_failure_for_expected_owner(expected_path_instance_id) {
            return false;
        }
        current.mark_failure(now, has_schedulable_alternative);
        true
    }

    /// Publishes while the caller owns the matching endpoint-policy
    /// commitment. This permits the carrier-group owner to swap its
    /// physical command owner in the same health transaction.
    pub(in crate::runtime) fn publish_tcp_peer_path_usage_committed(
        &self,
        publication: ClientTcpCarrierPublication,
        publish_readiness: impl FnOnce(),
    ) {
        {
            let _lifecycle = self
                .carrier_lifecycle
                .lock()
                .expect("client carrier lifecycle lock");
            let mut health = self.health.lock().expect("client path health lock");
            let record = health
                .tcp
                .get_mut(publication.path_index)
                .expect("TCP carrier actor must have one health record");
            record.install_tcp_peer_usage(
                publication.path_id,
                publication.path_instance_id,
                publication.peer_usage_sequence,
                publication.peer_usage,
            );
            publish_readiness();
        }
        if let Some(readiness_rtt) = publication.readiness_rtt {
            let _ = self.mutate_path_model(
                RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index: publication.path_index,
                },
                |record| {
                    record.mark_success_for_instance(publication.path_instance_id, readiness_rtt)
                },
            );
        }
    }

    /// Commits an authenticated TCP successor only while the expected
    /// predecessor still owns the stable member.
    ///
    /// Product admission uses this same health lock. Work committed before
    /// the swap remains fenced to the predecessor and follows ordinary ordered
    /// retirement; work committed after it observes only the successor.
    pub(in crate::runtime) fn publish_tcp_replacement_if_current(
        &self,
        predecessor_instance_id: CarrierPathInstanceId,
        publication: ClientTcpCarrierPublication,
        publish_readiness: impl FnOnce(),
    ) -> bool {
        {
            let _lifecycle = self
                .carrier_lifecycle
                .lock()
                .expect("client carrier lifecycle lock");
            let mut health = self.health.lock().expect("client path health lock");
            let record = health
                .tcp
                .get_mut(publication.path_index)
                .expect("TCP carrier actor must have one health record");
            if record.path_instance_id() != Some(predecessor_instance_id) {
                return false;
            }
            record.install_tcp_peer_usage(
                publication.path_id,
                publication.path_instance_id,
                publication.peer_usage_sequence,
                publication.peer_usage,
            );
            publish_readiness();
        }
        if let Some(readiness_rtt) = publication.readiness_rtt {
            let _ = self.mutate_path_model(
                RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index: publication.path_index,
                },
                |record| {
                    record.mark_success_for_instance(publication.path_instance_id, readiness_rtt)
                },
            );
        }
        true
    }

    fn acquire_product_flow(&self) {
        let _health = self.health.lock().expect("client path health lock");
        self.active_product_flows
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |active| {
                active.checked_add(1)
            })
            .expect("bounded Product flow count cannot overflow");
    }

    fn release_product_flow(&self) -> bool {
        let health = self.health.lock().expect("client path health lock");
        let previous = self.active_product_flows.fetch_sub(1, Ordering::Relaxed);
        assert!(previous > 0, "Product flow ownership released exactly once");
        previous == 1 && health.is_product_quiescent()
    }

    pub(in crate::runtime) fn mark_tcp_path_establishment_failure_for_endpoint_generation(
        &self,
        index: usize,
        endpoint_policy: &ClientTcpEndpointPolicy,
        endpoint_generation: u64,
    ) {
        endpoint_policy.with_current(endpoint_generation, || {
            let now = Instant::now();
            let mut health = self.health.lock().expect("client path health lock");
            let has_schedulable_alternative =
                health.tcp_records_have_schedulable_alternative(index, now);
            let record = health
                .tcp
                .get_mut(index)
                .expect("TCP carrier actor must have one health record");
            record.mark_failure(now, has_schedulable_alternative);
        });
    }

    pub(in crate::runtime) fn update_peer_path_usage(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        path_instance_id: CarrierPathInstanceId,
        sequence: u64,
        usage: PathUsage,
    ) -> bool {
        self.mutate_path_model(RelayPathKey { underlay, index }, |record| {
            record.update_peer_usage(path_instance_id, sequence, usage)
        })
        .unwrap_or(false)
    }

    pub(in crate::runtime) fn peer_path_usage(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
    ) -> Option<PathUsage> {
        let health = self.health.lock().expect("client path health lock");
        health
            .path_record(RelayPathKey { underlay, index })
            .and_then(|record| record.peer_usage)
    }

    /// Publishes an unexpected carrier loss once. Relay cleanup may observe the
    /// same loss later, so the health record deduplicates repeated reports.
    pub(in crate::runtime) fn mark_path_instance_data_plane_failure(
        &self,
        key: RelayPathKey,
        path_instance_id: CarrierPathInstanceId,
    ) -> bool {
        let _lifecycle = self
            .carrier_lifecycle
            .lock()
            .expect("client carrier lifecycle lock");
        let now = Instant::now();
        let mut health = self.health.lock().expect("client path health lock");
        let has_schedulable_alternative = match key.underlay {
            UnderlayProtocol::Tcp => {
                health.tcp_records_have_schedulable_alternative(key.index, now)
            }
            UnderlayProtocol::Udp => {
                path_records_have_schedulable_alternative(&health.udp, key.index, now)
            }
        };
        let Some(current) = health.path_record_mut(key) else {
            return false;
        };
        current.mark_data_plane_failure(path_instance_id, now, has_schedulable_alternative)
    }

    pub(in crate::runtime) fn retire_path_instance_planned(
        &self,
        key: RelayPathKey,
        path_instance_id: CarrierPathInstanceId,
    ) -> bool {
        let _lifecycle = self
            .carrier_lifecycle
            .lock()
            .expect("client carrier lifecycle lock");
        let mut health = self.health.lock().expect("client path health lock");
        let Some(record) = health.path_record_mut(key) else {
            return false;
        };
        record.retire_planned_instance(path_instance_id)
    }

    pub(in crate::runtime) fn begin_path_instance_planned_retirement(
        &self,
        key: RelayPathKey,
        path_instance_id: CarrierPathInstanceId,
    ) -> bool {
        let _lifecycle = self
            .carrier_lifecycle
            .lock()
            .expect("client carrier lifecycle lock");
        let mut health = self.health.lock().expect("client path health lock");
        let Some(record) = health.path_record_mut(key) else {
            return false;
        };
        record.begin_planned_instance_retirement(path_instance_id)
    }

    /// Linearization point between an accepted Product value and physical
    /// carrier replacement/failure. A successful return makes the accepted
    /// value the durable owner; a later replacement is subsequent lifecycle,
    /// not grounds for retroactively rejecting that value.
    pub(in crate::runtime) fn try_commit_path_instance(
        &self,
        key: RelayPathKey,
        path_instance_id: CarrierPathInstanceId,
    ) -> bool {
        let _lifecycle = self
            .carrier_lifecycle
            .lock()
            .expect("client carrier lifecycle lock");
        self.health
            .lock()
            .expect("client path health lock")
            .path_record(key)
            .is_some_and(|record| record.accepts_product_commit(path_instance_id))
    }

    pub(in crate::runtime::path) fn request_tcp_capacity_probe_session(
        &self,
    ) -> &RequestTcpCapacityProbeSession {
        &self.request_tcp_capacity_probe
    }

    fn release_relay_path_load(&self, key: RelayPathKey, lane: TrafficClass) -> bool {
        let mut health = self.health.lock().expect("client path health lock");
        let Some(record) = health.path_record_mut(key) else {
            return false;
        };
        record.release_load(lane);
        self.active_product_flows.load(Ordering::Relaxed) == 0 && health.is_product_quiescent()
    }
}

#[derive(Debug)]
pub(in crate::runtime::path) struct RequestCapacityProbeBudget {
    spent_bytes: AtomicU64,
    path_spent_bytes: Box<[AtomicU64]>,
    candidate_share_bytes: AtomicU64,
}

impl RequestCapacityProbeBudget {
    pub(in crate::runtime::path) fn new(path_count: usize) -> Self {
        Self {
            spent_bytes: AtomicU64::new(0),
            path_spent_bytes: (0..path_count)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            candidate_share_bytes: AtomicU64::new(0),
        }
    }

    pub(in crate::runtime::path) fn remaining_bytes(&self, limit: u64) -> u64 {
        limit.saturating_sub(self.spent_bytes.load(Ordering::Acquire))
    }

    pub(in crate::runtime::path) fn effective_candidate_share_bytes(
        &self,
        proposed_path_limit: u64,
        session_limit: u64,
    ) -> u64 {
        let frozen_limit = self.candidate_share_bytes.load(Ordering::Acquire);
        if frozen_limit == 0 {
            proposed_path_limit.min(session_limit)
        } else {
            frozen_limit
        }
    }

    pub(in crate::runtime::path) fn path_remaining_bytes(
        &self,
        path_index: usize,
        proposed_path_limit: u64,
        session_limit: u64,
    ) -> u64 {
        let Some(spent) = self.path_spent_bytes.get(path_index) else {
            return 0;
        };
        let frozen_limit = self.candidate_share_bytes.load(Ordering::Acquire);
        let path_limit = if frozen_limit == 0 {
            proposed_path_limit.min(session_limit)
        } else {
            frozen_limit
        };
        path_limit.saturating_sub(spent.load(Ordering::Acquire))
    }

    pub(in crate::runtime::path) fn try_reserve(
        &self,
        path_index: usize,
        bytes: u64,
        proposed_path_limit: u64,
        session_limit: u64,
    ) -> bool {
        let Some(path_spent) = self.path_spent_bytes.get(path_index) else {
            return false;
        };
        let proposed_path_limit = proposed_path_limit.min(session_limit);
        if proposed_path_limit == 0 {
            return false;
        }
        let path_limit = match self.candidate_share_bytes.compare_exchange(
            0,
            proposed_path_limit,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => proposed_path_limit,
            Err(frozen_limit) => frozen_limit,
        };
        if path_spent
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |spent| {
                spent.checked_add(bytes).filter(|next| *next <= path_limit)
            })
            .is_err()
        {
            return false;
        }
        if self
            .spent_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |spent| {
                spent
                    .checked_add(bytes)
                    .filter(|next| *next <= session_limit)
            })
            .is_err()
        {
            path_spent.fetch_sub(bytes, Ordering::AcqRel);
            return false;
        }
        true
    }

    pub(in crate::runtime::path) fn refund(&self, path_index: usize, bytes: u64) {
        if let Some(path_spent) = self.path_spent_bytes.get(path_index) {
            path_spent.fetch_sub(bytes, Ordering::AcqRel);
            self.spent_bytes.fetch_sub(bytes, Ordering::AcqRel);
        }
    }
}

/// One logical flow may spend one candidate share per protocol. The session
/// retains the full envelope so later flows can still discover other paths.
#[derive(Debug, Default)]
pub(in crate::runtime) struct RequestCapacityProbeCampaignBudget {
    spent_bytes: AtomicU64,
    limit_bytes: AtomicU64,
}

impl RequestCapacityProbeCampaignBudget {
    pub(in crate::runtime) fn remaining_bytes(&self, proposed_limit: u64) -> u64 {
        let frozen_limit = self.limit_bytes.load(Ordering::Acquire);
        let limit = if frozen_limit == 0 {
            proposed_limit
        } else {
            frozen_limit
        };
        limit.saturating_sub(self.spent_bytes.load(Ordering::Acquire))
    }

    pub(in crate::runtime::path) fn try_reserve(&self, bytes: u64, proposed_limit: u64) -> bool {
        if proposed_limit == 0 {
            return false;
        }
        let limit = match self.limit_bytes.compare_exchange(
            0,
            proposed_limit,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => proposed_limit,
            Err(frozen_limit) => frozen_limit,
        };
        self.spent_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |spent| {
                spent.checked_add(bytes).filter(|next| *next <= limit)
            })
            .is_ok()
    }

    pub(in crate::runtime::path) fn refund(&self, bytes: u64) {
        self.spent_bytes.fetch_sub(bytes, Ordering::AcqRel);
    }
}

/// Cancellation-safe ownership of one logical flow's shared path load.
///
/// The initial attachment acquires a lease before I/O and transfers it with the
/// attachment. Additional paths acquire one only after unique product bytes
/// commit. Dropping any transaction owner rolls the scheduler load back.
pub(in crate::runtime) struct RelayPathLoadLease {
    state: Arc<ClientPathState>,
    key: RelayPathKey,
    lane: TrafficClass,
    tcp_carrier_groups: Arc<ClientTcpCarrierGroups>,
}

/// Session-owned logical Product lifetime, independent of telemetry and exact
/// physical attachment membership. It covers peer-direction work, retention,
/// and recovery until the Product flow itself becomes terminal.
pub(in crate::runtime) struct ClientSessionProductFlowLease {
    state: Arc<ClientPathState>,
    tcp_carrier_groups: Arc<ClientTcpCarrierGroups>,
}

impl Drop for ClientSessionProductFlowLease {
    fn drop(&mut self) {
        if self.state.release_product_flow() {
            self.tcp_carrier_groups.publish_change();
        }
    }
}

impl RelayPathLoadLease {
    pub(super) fn new(
        state: Arc<ClientPathState>,
        key: RelayPathKey,
        lane: TrafficClass,
        tcp_carrier_groups: Arc<ClientTcpCarrierGroups>,
    ) -> Self {
        Self {
            state,
            key,
            lane,
            tcp_carrier_groups,
        }
    }

    pub(in crate::runtime) fn set_recorded_lane(&mut self, lane: TrafficClass) {
        self.lane = lane;
    }

    pub(in crate::runtime) fn key(&self) -> RelayPathKey {
        self.key
    }
}

impl Drop for RelayPathLoadLease {
    fn drop(&mut self) {
        let product_quiescent = self.state.release_relay_path_load(self.key, self.lane);
        if product_quiescent {
            self.tcp_carrier_groups.publish_change();
        }
    }
}

impl ClientPathContext {
    pub(in crate::runtime) fn health(&self) -> &Mutex<ClientPathHealth> {
        self.state.health()
    }

    pub(in crate::runtime) fn path_model_generation(&self) -> u64 {
        self.state.path_model_generation()
    }

    pub(in crate::runtime) fn arm_path_model_publication(
        &self,
        observed_generation: u64,
    ) -> ArmedClientPathModelPublication {
        self.state.arm_path_model_publication(observed_generation)
    }

    pub(in crate::runtime) fn session_retirement(&self) -> ClientSessionRetirement {
        self.state.session_retirement()
    }

    pub(in crate::runtime) fn ensure_session_active(&self) -> Result<(), RuntimeError> {
        self.state.session_lifecycle().ensure_active()
    }

    /// Completes one outward Product operation against the sticky SessionId
    /// terminal. The first terminal reason owns a concurrently ready result and
    /// is checked again after ordinary settlement.
    pub(in crate::runtime) async fn complete_session_operation<T>(
        &self,
        operation: impl std::future::Future<Output = Result<T, RuntimeError>>,
    ) -> Result<T, RuntimeError> {
        let retirement = self.session_retirement();
        let terminal = retirement.clone().wait();
        tokio::pin!(terminal);
        tokio::pin!(operation);
        tokio::select! {
            biased;
            reason = &mut terminal => Err(RuntimeError::RemoteClosed(reason)),
            result = &mut operation => match retirement.reason() {
                Some(reason) => Err(RuntimeError::RemoteClosed(reason)),
                None => result,
            },
        }
    }

    pub(in crate::runtime) fn commit_if_session_active<T>(
        &self,
        commit: impl FnOnce() -> T,
    ) -> Result<T, RuntimeError> {
        self.state
            .session_lifecycle()
            .commit_if_active(commit)
            .map_err(RuntimeError::RemoteClosed)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn retire_session(
        &self,
        reason: crate::protocol::CloseReason,
    ) -> crate::protocol::CloseReason {
        self.state.session_lifecycle().retire(reason)
    }

    pub(in crate::runtime) fn reserve_session_product_flow(
        &self,
    ) -> Result<ClientSessionProductFlowLease, RuntimeError> {
        self.commit_if_session_active(|| self.state.acquire_product_flow())?;
        Ok(ClientSessionProductFlowLease {
            state: self.state.clone(),
            tcp_carrier_groups: self.tcp_carrier_groups.clone(),
        })
    }

    pub(in crate::runtime) fn request_tcp_capacity_probe_remaining_bytes(&self) -> u64 {
        self.state.request_tcp_capacity_probe_remaining_bytes(
            reliable_capacity_measurement_session_limit_bytes(self.mux_limits),
        )
    }

    pub(in crate::runtime) fn request_tcp_capacity_probe_candidate_share_bytes(
        &self,
        proposed_path_limit: u64,
    ) -> u64 {
        self.state.request_tcp_capacity_probe_candidate_share_bytes(
            proposed_path_limit,
            reliable_capacity_measurement_session_limit_bytes(self.mux_limits),
        )
    }

    pub(in crate::runtime) fn request_tcp_capacity_probe_path_remaining_bytes(
        &self,
        path_index: usize,
        path_limit: u64,
    ) -> u64 {
        self.state.request_tcp_capacity_probe_path_remaining_bytes(
            path_index,
            path_limit,
            reliable_capacity_measurement_session_limit_bytes(self.mux_limits),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::runtime) fn try_reserve_request_tcp_capacity_probe(
        &self,
        stream_id: StreamId,
        path_index: usize,
        path_instance: RelayPathInstance,
        token: u64,
        train_bytes: u64,
        path_limit_bytes: u64,
        campaign: Arc<RequestCapacityProbeCampaignBudget>,
        required_timed_bytes: u64,
        valid_after: Instant,
        expires_at: Instant,
        ticket: CapacityProbeCommandTicket,
    ) -> Option<RequestTcpCapacityProbeLease> {
        self.state.try_reserve_request_tcp_capacity_probe(
            stream_id,
            path_index,
            path_instance,
            token,
            train_bytes,
            path_limit_bytes,
            reliable_capacity_measurement_session_limit_bytes(self.mux_limits),
            campaign,
            required_timed_bytes,
            valid_after,
            expires_at,
            ticket,
        )
    }

    pub(in crate::runtime) fn request_capacity_reconciliation_view(
        &self,
        stream_id: StreamId,
        tcp_queries: impl Iterator<Item = RequestTcpCapacityProofQuery>,
        now: Instant,
    ) -> RequestCapacityReconciliationView {
        let mut tcp_queries = tcp_queries.peekable();
        if tcp_queries.peek().is_none() {
            return RequestCapacityReconciliationView {
                observed_at: now,
                tcp_proofs: HashMap::new(),
            };
        }
        let health = self.state.health.lock().expect("client path health lock");
        let tcp_proofs = tcp_queries
            .filter_map(|query| {
                health
                    .tcp_record(query.target.key.index)
                    .and_then(|record| {
                        record.tcp_capacity.exact_proof_candidate_at(
                            stream_id,
                            query.target,
                            query.token,
                            now,
                        )
                    })
                    .map(|proof| (query.target, proof))
            })
            .collect();
        RequestCapacityReconciliationView {
            observed_at: now,
            tcp_proofs,
        }
    }

    pub(in crate::runtime) fn allocate_reliable_stream_id(&self) -> Result<StreamId, RuntimeError> {
        self.commit_if_session_active(|| {
            let mut next = self
                .state
                .next_reliable_stream_id
                .lock()
                .expect("client reliable stream ID lock");
            let stream_id = StreamId(*next);
            *next = next
                .checked_add(1)
                .ok_or(RuntimeError::Protocol("reliable stream ID overflow"))?;
            Ok(stream_id)
        })?
    }

    /// Allocates one data-level flow identity shared by every carrier retry.
    pub(in crate::runtime) fn allocate_datagram_flow_id(
        &self,
    ) -> Result<DatagramFlowId, RuntimeError> {
        self.commit_if_session_active(|| {
            let mut next = self
                .state
                .next_datagram_flow_id
                .lock()
                .expect("client datagram flow ID lock");
            let flow_id = DatagramFlowId(*next);
            *next = next
                .checked_add(1)
                .ok_or(RuntimeError::Protocol("datagram flow ID overflow"))?;
            Ok(flow_id)
        })?
    }

    /// Returns the only owner of the newly published scheduler load.
    pub(in crate::runtime) fn reserve_relay_path_load(
        &self,
        key: RelayPathKey,
        lane: TrafficClass,
    ) -> Option<RelayPathLoadLease> {
        let now = Instant::now();
        let reserved = self
            .commit_if_session_active(|| {
                self.state
                    .mutate_path_eligibility(key, |record| record.reserve_load(lane, now))
                    .unwrap_or(false)
            })
            .ok()?;
        if !reserved {
            return None;
        }
        Some(RelayPathLoadLease::new(
            self.state.clone(),
            key,
            lane,
            self.tcp_carrier_groups.clone(),
        ))
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_tcp_path_open_success(
        &self,
        index: usize,
        elapsed: Duration,
        lane: TrafficClass,
    ) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.mark_open_success(elapsed, lane);
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_tcp_path_reserved_open_success(
        &self,
        index: usize,
        elapsed: Duration,
    ) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.mark_reserved_open_success(elapsed);
        }
    }

    pub(in crate::runtime) fn mark_tcp_path_reserved_open_success_for_instance(
        &self,
        index: usize,
        path_instance_id: CarrierPathInstanceId,
        elapsed: Duration,
    ) -> bool {
        self.commit_if_session_active(|| {
            self.state
                .mutate_path_model(
                    RelayPathKey {
                        underlay: UnderlayProtocol::Tcp,
                        index,
                    },
                    |current| {
                        current.mark_reserved_open_success_for_instance(path_instance_id, elapsed)
                    },
                )
                .unwrap_or(false)
        })
        .unwrap_or(false)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_tcp_path_probe_success(&self, index: usize, elapsed: Duration) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.mark_success(elapsed);
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn should_probe_tcp_path(&self, index: usize) -> bool {
        let now = Instant::now();
        self.state
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
            .is_some_and(|record| {
                record.maintain(now);
                !record.manual_disabled
                    && path_observation_is_idle_for_probe(record.observation_at(now))
            })
    }

    #[cfg(test)]
    pub(in crate::runtime) fn release_tcp_path_load(&self, index: usize, lane: TrafficClass) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.release_load(lane);
        }
    }

    pub(in crate::runtime) fn mark_udp_stream_reserved_open_success_for_instance(
        &self,
        index: usize,
        path_instance_id: CarrierPathInstanceId,
        elapsed: Duration,
        accepted: bool,
    ) -> bool {
        if !accepted {
            return false;
        }
        self.commit_if_session_active(|| {
            self.state
                .mutate_path_model(
                    RelayPathKey {
                        underlay: UnderlayProtocol::Udp,
                        index,
                    },
                    |current| {
                        current.mark_reserved_open_success_for_instance(path_instance_id, elapsed)
                    },
                )
                .unwrap_or(false)
        })
        .unwrap_or(false)
    }

    pub(in crate::runtime) fn mark_relay_path_data_plane_failure(&self, path: RelayPathInstance) {
        if !self
            .state
            .mark_path_instance_data_plane_failure(path.key, path.path_instance_id)
            || path.key.underlay != UnderlayProtocol::Tcp
        {
            return;
        }

        if let Some(session) = self.tcp_sessions.get(path.key.index) {
            session.terminate_failed_instance(path.path_instance_id);
        }
    }

    pub(in crate::runtime) fn record_relay_path_send(
        &self,
        instance: RelayPathInstance,
        bytes: usize,
    ) {
        if bytes == 0 {
            return;
        }
        let mut health = self.state.health.lock().expect("client path health lock");
        if let Some(current) = health.path_record_mut(instance.key) {
            current.record_relay_send(instance.path_instance_id, bytes);
        }
    }

    pub(in crate::runtime) fn release_relay_path_inflight(
        &self,
        instance: RelayPathInstance,
        bytes: usize,
    ) {
        if bytes == 0 {
            return;
        }
        let mut health = self.state.health.lock().expect("client path health lock");
        let product_quiescent = health.path_record_mut(instance.key).is_some_and(|current| {
            current.release_relay_inflight(instance.path_instance_id, bytes)
        }) && self.state.active_product_flows.load(Ordering::Relaxed) == 0
            && health.is_product_quiescent();
        drop(health);
        if product_quiescent {
            self.tcp_carrier_groups.publish_change();
        }
    }

    pub(in crate::runtime) fn change_relay_path_lane_load(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        from: TrafficClass,
        to: TrafficClass,
    ) {
        if from == to {
            return;
        }
        let mut health = self.state.health.lock().expect("client path health lock");
        if let Some(current) = health.path_record_mut(RelayPathKey { underlay, index }) {
            current.change_lane_load(from, to);
        }
    }

    pub(in crate::runtime) fn mark_relay_path_rate_sample(
        &self,
        instance: RelayPathInstance,
        sample: PathRateSample,
    ) {
        let _ = self.state.mutate_path_model(instance.key, |current| {
            current.mark_product_delivery_for_instance(instance.path_instance_id, sample);
        });
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_relay_path_rate_sample_for_test(
        &self,
        key: RelayPathKey,
        sample: PathRateSample,
    ) {
        let _ = self
            .state
            .mutate_path_model(key, |current| current.mark_product_delivery(sample));
    }

    pub(in crate::runtime) fn mark_relay_path_ack_clock_rate_sample(
        &self,
        instance: RelayPathInstance,
        sample: PathRateSample,
        replace_startup_rate: bool,
    ) {
        let _ = self.state.mutate_path_model(instance.key, |current| {
            if replace_startup_rate {
                current.mark_product_delivery_replacing_rate_for_instance(
                    instance.path_instance_id,
                    sample,
                );
            } else {
                current.mark_product_delivery_for_instance(instance.path_instance_id, sample);
            }
        });
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_relay_path_proof_observation(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        observation: PathProofObservation,
    ) {
        let mut health = self.state.health.lock().expect("client path health lock");
        let records = match underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        if let Some(current) = records.get_mut(index) {
            current.mark_path_proof_success(observation);
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_tcp_path_delivery(
        &self,
        index: usize,
        stats: PathDeliveryStats,
    ) {
        let Some(sample) = stats.rate_sample() else {
            return;
        };
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.mark_product_delivery(sample);
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_tcp_path_failure(&self, index: usize) {
        let now = Instant::now();
        let mut health = self.state.health.lock().expect("client path health lock");
        let has_schedulable_alternative =
            health.tcp_records_have_schedulable_alternative(index, now);
        if let Some(current) = health.tcp_record_mut(index) {
            current.mark_failure(now, has_schedulable_alternative);
        }
    }

    pub(in crate::runtime) fn mark_udp_path_reserved_open_success_for_instance(
        &self,
        index: usize,
        path_instance_id: CarrierPathInstanceId,
        elapsed: Duration,
    ) -> bool {
        self.state
            .mutate_path_model(
                RelayPathKey {
                    underlay: UnderlayProtocol::Udp,
                    index,
                },
                |current| {
                    current.mark_reserved_open_success_for_instance(path_instance_id, elapsed)
                },
            )
            .unwrap_or(false)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_udp_path_open_success(&self, index: usize, elapsed: Duration) {
        let _ = self.commit_if_session_active(|| {
            let _ = self.state.mutate_path_model(
                RelayPathKey {
                    underlay: UnderlayProtocol::Udp,
                    index,
                },
                |current| current.mark_open_success(elapsed, TrafficClass::RealtimeDatagram),
            );
        });
    }

    pub(in crate::runtime) fn mark_udp_path_probe_success_for_instance(
        &self,
        index: usize,
        path_instance_id: CarrierPathInstanceId,
        elapsed: Duration,
    ) -> bool {
        self.state
            .mutate_path_model(
                RelayPathKey {
                    underlay: UnderlayProtocol::Udp,
                    index,
                },
                |current| current.mark_success_for_instance(path_instance_id, elapsed),
            )
            .unwrap_or(false)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_udp_path_probe_success(&self, index: usize, elapsed: Duration) {
        let _ = self.commit_if_session_active(|| {
            let _ = self.state.mutate_path_model(
                RelayPathKey {
                    underlay: UnderlayProtocol::Udp,
                    index,
                },
                |current| current.mark_success(elapsed),
            );
        });
    }

    /// Returns the exact physical carrier observed at the probe decision, or
    /// `None` inside `Some` when probing an endpoint without a carrier.
    pub(in crate::runtime) fn udp_path_probe_expected_instance(
        &self,
        index: usize,
    ) -> Option<Option<CarrierPathInstanceId>> {
        let now = Instant::now();
        self.state
            .mutate_path_model(
                RelayPathKey {
                    underlay: UnderlayProtocol::Udp,
                    index,
                },
                |record| {
                    record.maintain(now);
                    (!record.manual_disabled
                        && path_observation_is_idle_for_probe(record.observation_at(now)))
                    .then(|| record.path_instance_id())
                },
            )
            .flatten()
    }

    #[cfg(test)]
    pub(in crate::runtime) fn release_udp_path_load(&self, index: usize) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.release_load(TrafficClass::RealtimeDatagram);
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_udp_datagram_path_delivery(
        &self,
        index: usize,
        stats: PathDeliveryStats,
    ) {
        let Some(sample) = stats.rate_sample() else {
            return;
        };
        let _ = self.state.mutate_path_model(
            RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index,
            },
            |current| current.mark_delivery(sample),
        );
    }

    pub(in crate::runtime) fn mark_udp_path_feedback_for_instance(
        &self,
        index: usize,
        path_instance_id: CarrierPathInstanceId,
        observation: UdpDatagramPathObservation,
    ) -> bool {
        self.state
            .mutate_path_model(
                RelayPathKey {
                    underlay: UnderlayProtocol::Udp,
                    index,
                },
                |current| {
                    current.mark_udp_datagram_feedback_for_instance(path_instance_id, observation)
                },
            )
            .unwrap_or(false)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_udp_path_feedback(
        &self,
        index: usize,
        observation: UdpDatagramPathObservation,
    ) {
        let _ = self.state.mutate_path_model(
            RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index,
            },
            |current| current.mark_udp_datagram_feedback(observation),
        );
    }

    pub(in crate::runtime) fn mark_udp_path_establishment_failure_if_current(
        &self,
        index: usize,
        expected_path_instance_id: Option<CarrierPathInstanceId>,
    ) -> bool {
        self.state
            .mark_udp_path_establishment_failure_if_current(index, expected_path_instance_id)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_udp_path_failure(&self, index: usize) {
        let now = Instant::now();
        let mut health = self.state.health.lock().expect("client path health lock");
        let has_schedulable_alternative =
            path_records_have_schedulable_alternative(&health.udp, index, now);
        if let Some(current) = health.udp.get_mut(index) {
            current.mark_failure(now, has_schedulable_alternative);
        }
    }
}

#[cfg(test)]
#[path = "tests_state.rs"]
mod tests;
