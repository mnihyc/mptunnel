//! Shared client-path evidence and carrier-neutral capacity budgets.
//!
//! One health lock makes mixed TCP/QUIC observations coherent. Each carrier
//! module owns its reservation, proof, and rollback transaction.

use super::commands::CapacityProbeCommandTicket;
use super::health::{ClientPathHealth, ClientPathHealthRecord, RequestCapacityReconciliationView};
use super::model::{
    ClientPathObservation, PathDeliveryStats, UdpDatagramPathObservation,
    path_observation_is_idle_for_probe, path_records_have_schedulable_alternative,
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
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

pub(in crate::runtime) struct ClientTcpCarrierPublication {
    pub(in crate::runtime) path_index: usize,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime) peer_usage_sequence: u64,
    pub(in crate::runtime) peer_usage: PathUsage,
    pub(in crate::runtime) readiness_rtt: Option<Duration>,
}

/// One lock domain for coherent health, load, and carrier budget composition.
#[derive(Debug)]
pub(in crate::runtime) struct ClientPathState {
    health: Mutex<ClientPathHealth>,
    // Accessed only while `health` is locked so logical Product ownership and
    // physical replacement share one transaction boundary.
    active_product_flows: AtomicU64,
    next_reliable_stream_id: Mutex<u64>,
    next_datagram_flow_id: Mutex<u64>,
    request_tcp_capacity_probe: RequestTcpCapacityProbeSession,
}

impl ClientPathState {
    pub(in crate::runtime) fn new(health: ClientPathHealth) -> Arc<Self> {
        let tcp_path_count = health.tcp.len();
        Arc::new(Self {
            health: Mutex::new(health),
            active_product_flows: AtomicU64::new(0),
            next_reliable_stream_id: Mutex::new(0),
            next_datagram_flow_id: Mutex::new(0),
            request_tcp_capacity_probe: RequestTcpCapacityProbeSession::new(tcp_path_count),
        })
    }

    pub(in crate::runtime) fn health(&self) -> &Mutex<ClientPathHealth> {
        &self.health
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
    pub(in crate::runtime) fn install_peer_path_usage(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        path_instance_id: CarrierPathInstanceId,
        sequence: u64,
        usage: PathUsage,
    ) {
        let _ = self.mutate_path_eligibility(RelayPathKey { underlay, index }, |record| {
            record.install_peer_usage(path_instance_id, sequence, usage);
        });
    }

    /// Publishes while the caller owns the matching endpoint-policy
    /// commitment. This permits the carrier-group owner to swap its
    /// physical command owner in the same health transaction.
    pub(in crate::runtime) fn publish_tcp_peer_path_usage_committed(
        &self,
        publication: ClientTcpCarrierPublication,
        publish_readiness: impl FnOnce(),
    ) {
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
        if let Some(readiness_rtt) = publication.readiness_rtt {
            record.mark_success(readiness_rtt);
        }
        publish_readiness();
    }

    /// Commits a provisional TCP successor only while the predecessor still
    /// owns the stable member and has no Product ownership. Product admission
    /// uses this same health lock, so no open can cross the instance swap.
    pub(in crate::runtime) fn publish_tcp_replacement_if_product_quiescent(
        &self,
        predecessor_instance_id: CarrierPathInstanceId,
        publication: ClientTcpCarrierPublication,
        publish_readiness: impl FnOnce(),
    ) -> bool {
        let mut health = self.health.lock().expect("client path health lock");
        if self.active_product_flows.load(Ordering::Relaxed) != 0 || !health.is_product_quiescent()
        {
            return false;
        }
        let record = health
            .tcp
            .get_mut(publication.path_index)
            .expect("TCP carrier actor must have one health record");
        if !record.is_product_quiescent_for_instance(predecessor_instance_id) {
            return false;
        }
        record.install_tcp_peer_usage(
            publication.path_id,
            publication.path_instance_id,
            publication.peer_usage_sequence,
            publication.peer_usage,
        );
        if let Some(readiness_rtt) = publication.readiness_rtt {
            record.mark_success(readiness_rtt);
        }
        publish_readiness();
        true
    }

    pub(in crate::runtime) fn tcp_path_is_product_quiescent_for_instance(
        &self,
        index: usize,
        path_instance_id: CarrierPathInstanceId,
    ) -> bool {
        let health = self.health.lock().expect("client path health lock");
        self.active_product_flows.load(Ordering::Relaxed) == 0
            && health.is_product_quiescent()
            && health
                .tcp_record(index)
                .is_some_and(|record| record.is_product_quiescent_for_instance(path_instance_id))
    }

    /// Fences a no-spare replacement at the same exact Product-admission
    /// boundary. Marking the record draining makes all later load reservations
    /// fail before the physical actor receives its ordered drain request.
    pub(in crate::runtime) fn begin_tcp_replacement_if_product_quiescent(
        &self,
        index: usize,
        path_instance_id: CarrierPathInstanceId,
    ) -> bool {
        let mut health = self.health.lock().expect("client path health lock");
        if self.active_product_flows.load(Ordering::Relaxed) != 0 || !health.is_product_quiescent()
        {
            return false;
        }
        let Some(record) = health.tcp.get_mut(index) else {
            return false;
        };
        if !record.is_product_quiescent_for_instance(path_instance_id) {
            return false;
        }
        record.begin_planned_retirement();
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
        self.mutate_path_eligibility(RelayPathKey { underlay, index }, |record| {
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
        let mut health = self.health.lock().expect("client path health lock");
        let Some(record) = health.path_record_mut(key) else {
            return false;
        };
        record.begin_planned_instance_retirement(path_instance_id)
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

    pub(in crate::runtime) fn reserve_session_product_flow(&self) -> ClientSessionProductFlowLease {
        self.state.acquire_product_flow();
        ClientSessionProductFlowLease {
            state: self.state.clone(),
            tcp_carrier_groups: self.tcp_carrier_groups.clone(),
        }
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
    }

    /// Allocates one data-level flow identity shared by every carrier retry.
    pub(in crate::runtime) fn allocate_datagram_flow_id(
        &self,
    ) -> Result<DatagramFlowId, RuntimeError> {
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
    }

    /// Returns the only owner of the newly published scheduler load.
    pub(in crate::runtime) fn reserve_relay_path_load(
        &self,
        key: RelayPathKey,
        lane: TrafficClass,
    ) -> Option<RelayPathLoadLease> {
        let now = Instant::now();
        if !self
            .state
            .mutate_path_eligibility(key, |record| record.reserve_load(lane, now))?
        {
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
        self.state
            .mutate_path_eligibility(
                RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index,
                },
                |current| {
                    current.mark_reserved_open_success_for_instance(path_instance_id, elapsed)
                },
            )
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

    pub(in crate::runtime) fn mark_udp_stream_reserved_open_success(
        &self,
        index: usize,
        elapsed: Duration,
        accepted: bool,
    ) {
        if !accepted {
            return;
        }
        let _ = self.state.mutate_path_eligibility(
            RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index,
            },
            |current| {
                current.mark_reserved_open_success(elapsed);
            },
        );
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

    pub(in crate::runtime) fn mark_relay_path_delivery(
        &self,
        instance: RelayPathInstance,
        stats: PathDeliveryStats,
    ) {
        match instance.key.underlay {
            UnderlayProtocol::Tcp => self.mark_tcp_path_delivery_for_instance(
                instance.key.index,
                instance.path_instance_id,
                stats,
            ),
            UnderlayProtocol::Udp => {
                let Some(sample) = stats.rate_sample() else {
                    return;
                };
                let _ = self.state.mutate_path_eligibility(instance.key, |current| {
                    current.mark_product_delivery_for_instance(instance.path_instance_id, sample);
                });
            }
        }
    }

    pub(in crate::runtime) fn mark_relay_path_rate_sample(
        &self,
        instance: RelayPathInstance,
        sample: PathRateSample,
    ) {
        let _ = self.state.mutate_path_eligibility(instance.key, |current| {
            current.mark_product_delivery_for_instance(instance.path_instance_id, sample);
        });
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_relay_path_rate_sample_for_test(
        &self,
        key: RelayPathKey,
        sample: PathRateSample,
    ) {
        let mut health = self.state.health.lock().expect("client path health lock");
        let records = match key.underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        if let Some(current) = records.get_mut(key.index) {
            current.mark_product_delivery(sample);
        }
    }

    pub(in crate::runtime) fn mark_relay_path_ack_clock_rate_sample(
        &self,
        instance: RelayPathInstance,
        sample: PathRateSample,
        replace_startup_rate: bool,
    ) {
        let _ = self.state.mutate_path_eligibility(instance.key, |current| {
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

    pub(in crate::runtime) fn mark_tcp_path_delivery_for_instance(
        &self,
        index: usize,
        path_instance_id: CarrierPathInstanceId,
        stats: PathDeliveryStats,
    ) {
        let Some(sample) = stats.rate_sample() else {
            return;
        };
        let _ = self.state.mutate_path_eligibility(
            RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index,
            },
            |current| {
                current.mark_product_delivery_for_instance(path_instance_id, sample);
            },
        );
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

    pub(in crate::runtime) fn mark_udp_path_open_success(&self, index: usize, elapsed: Duration) {
        let _ = self.state.mutate_path_eligibility(
            RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index,
            },
            |current| {
                current.mark_open_success(elapsed, TrafficClass::RealtimeDatagram);
            },
        );
    }

    pub(in crate::runtime) fn mark_udp_path_probe_success(&self, index: usize, elapsed: Duration) {
        let _ = self.state.mutate_path_eligibility(
            RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index,
            },
            |current| {
                current.mark_success(elapsed);
            },
        );
    }

    pub(in crate::runtime) fn should_probe_udp_path(&self, index: usize) -> bool {
        let now = Instant::now();
        self.state
            .mutate_path_eligibility(
                RelayPathKey {
                    underlay: UnderlayProtocol::Udp,
                    index,
                },
                |record| {
                    record.maintain(now);
                    !record.manual_disabled
                        && path_observation_is_idle_for_probe(record.observation_at(now))
                },
            )
            .unwrap_or(false)
    }

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

    pub(in crate::runtime) fn mark_udp_datagram_path_delivery(
        &self,
        index: usize,
        stats: PathDeliveryStats,
    ) {
        let Some(sample) = stats.rate_sample() else {
            return;
        };
        let _ = self.state.mutate_path_eligibility(
            RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index,
            },
            |current| {
                // Datagram goodput ranks datagram paths but never proves reliable
                // product ownership or unlocks ordered-stream overlap.
                current.mark_delivery(sample);
            },
        );
    }

    pub(in crate::runtime) fn mark_udp_path_feedback(
        &self,
        index: usize,
        observation: UdpDatagramPathObservation,
    ) {
        let _ = self.state.mutate_path_eligibility(
            RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index,
            },
            |current| {
                current.mark_udp_datagram_feedback(observation);
            },
        );
    }

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
