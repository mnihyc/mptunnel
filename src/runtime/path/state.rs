//! Shared client-path evidence and carrier-neutral capacity budgets.
//!
//! One health lock makes mixed TCP/QUIC observations coherent. Each carrier
//! module owns its reservation, proof, and rollback transaction.

use super::commands::CapacityProbeCommandTicket;
use super::health::{ClientPathHealth, RequestCapacityReconciliationView};
use super::model::{
    PathDeliveryStats, UdpDatagramPathObservation, path_observation_is_idle_for_probe,
    path_records_have_schedulable_alternative,
};
#[cfg(test)]
use super::proof::PathProofObservation;
use super::set::ClientPathContext;
use super::tcp::capacity::{
    RequestTcpCapacityProbeLease, RequestTcpCapacityProbeSession, RequestTcpCapacityProofQuery,
};
#[cfg(test)]
use super::*;
use crate::model::capacity::{PathRateSample, reliable_capacity_measurement_session_limit_bytes};
use crate::model::path::{CarrierPathInstanceId, RelayPathInstance, RelayPathKey};
use crate::protocol::{DatagramFlowId, PathUsage, StreamId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::scheduler::TrafficClass;
use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

/// One lock domain for coherent health, load, and carrier budget composition.
#[derive(Debug)]
pub(in crate::runtime) struct ClientPathState {
    health: Mutex<ClientPathHealth>,
    next_reliable_stream_id: Mutex<u64>,
    next_datagram_flow_id: Mutex<u64>,
    request_tcp_capacity_probe: RequestTcpCapacityProbeSession,
}

impl ClientPathState {
    pub(in crate::runtime) fn new(health: ClientPathHealth) -> Arc<Self> {
        let tcp_path_count = health.tcp.len();
        Arc::new(Self {
            health: Mutex::new(health),
            next_reliable_stream_id: Mutex::new(0),
            next_datagram_flow_id: Mutex::new(0),
            request_tcp_capacity_probe: RequestTcpCapacityProbeSession::new(tcp_path_count),
        })
    }

    pub(in crate::runtime) fn health(&self) -> &Mutex<ClientPathHealth> {
        &self.health
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
        let mut health = self.health.lock().expect("client path health lock");
        let records = match underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        if let Some(record) = records.get_mut(index) {
            record.install_peer_usage(path_instance_id, sequence, usage);
        }
    }

    pub(in crate::runtime) fn update_peer_path_usage(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        path_instance_id: CarrierPathInstanceId,
        sequence: u64,
        usage: PathUsage,
    ) -> bool {
        let mut health = self.health.lock().expect("client path health lock");
        let records = match underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        records
            .get_mut(index)
            .is_some_and(|record| record.update_peer_usage(path_instance_id, sequence, usage))
    }

    pub(in crate::runtime) fn peer_path_usage(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
    ) -> Option<PathUsage> {
        let health = self.health.lock().expect("client path health lock");
        match underlay {
            UnderlayProtocol::Tcp => health.tcp.get(index),
            UnderlayProtocol::Udp => health.udp.get(index),
        }
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
        let records = match key.underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        let has_schedulable_alternative =
            path_records_have_schedulable_alternative(records, key.index, now);
        records.get_mut(key.index).is_some_and(|current| {
            current.mark_data_plane_failure(path_instance_id, now, has_schedulable_alternative)
        })
    }

    pub(in crate::runtime::path) fn request_tcp_capacity_probe_session(
        &self,
    ) -> &RequestTcpCapacityProbeSession {
        &self.request_tcp_capacity_probe
    }

    fn release_relay_path_load(&self, key: RelayPathKey, lane: TrafficClass) {
        let mut health = self.health.lock().expect("client path health lock");
        let records = match key.underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        if let Some(record) = records.get_mut(key.index) {
            record.release_load(lane);
        }
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
}

impl RelayPathLoadLease {
    pub(super) fn new(state: Arc<ClientPathState>, key: RelayPathKey, lane: TrafficClass) -> Self {
        Self { state, key, lane }
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
        self.state.release_relay_path_load(self.key, self.lane);
    }
}

impl ClientPathContext {
    pub(in crate::runtime) fn health(&self) -> &Mutex<ClientPathHealth> {
        self.state.health()
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
                    .tcp
                    .get(query.target.key.index)
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
        let mut health = self.state.health.lock().expect("client path health lock");
        let records = match key.underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        if !records.get_mut(key.index)?.reserve_load(lane, now) {
            return None;
        }
        drop(health);
        Some(RelayPathLoadLease::new(self.state.clone(), key, lane))
    }

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
                path_observation_is_idle_for_probe(record.observation_at(now))
            })
    }

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
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_reserved_open_success(elapsed);
        }
    }

    pub(in crate::runtime) fn mark_relay_path_failure(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
    ) {
        match underlay {
            UnderlayProtocol::Tcp => self.mark_tcp_path_failure(index),
            UnderlayProtocol::Udp => self.mark_udp_path_failure(index),
        }
    }

    pub(in crate::runtime) fn mark_relay_path_data_plane_failure(&self, path: RelayPathInstance) {
        self.state
            .mark_path_instance_data_plane_failure(path.key, path.path_instance_id);
    }

    pub(in crate::runtime) fn record_relay_path_send(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        bytes: usize,
    ) {
        if bytes == 0 {
            return;
        }
        let mut health = self.state.health.lock().expect("client path health lock");
        let records = match underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        if let Some(current) = records.get_mut(index) {
            current.record_relay_send(bytes);
        }
    }

    pub(in crate::runtime) fn release_relay_path_inflight(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        bytes: usize,
    ) {
        if bytes == 0 {
            return;
        }
        let mut health = self.state.health.lock().expect("client path health lock");
        let records = match underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        if let Some(current) = records.get_mut(index) {
            current.release_relay_inflight(bytes);
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

    pub(in crate::runtime) fn mark_relay_path_delivery(
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

    pub(in crate::runtime) fn mark_relay_path_rate_sample(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        sample: PathRateSample,
    ) {
        let mut health = self.state.health.lock().expect("client path health lock");
        let records = match underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        if let Some(current) = records.get_mut(index) {
            current.mark_product_delivery(sample);
        }
    }

    pub(in crate::runtime) fn mark_relay_path_ack_clock_rate_sample(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        sample: PathRateSample,
        replace_startup_rate: bool,
    ) {
        let mut health = self.state.health.lock().expect("client path health lock");
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
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.mark_product_delivery_for_instance(path_instance_id, sample);
        }
    }

    pub(in crate::runtime) fn mark_tcp_path_failure(&self, index: usize) {
        let now = Instant::now();
        let mut health = self.state.health.lock().expect("client path health lock");
        let has_schedulable_alternative =
            path_records_have_schedulable_alternative(&health.tcp, index, now);
        if let Some(current) = health.tcp.get_mut(index) {
            current.mark_failure(now, has_schedulable_alternative);
        }
    }

    pub(in crate::runtime) fn mark_udp_path_open_success(&self, index: usize, elapsed: Duration) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_open_success(elapsed, TrafficClass::RealtimeDatagram);
        }
    }

    pub(in crate::runtime) fn mark_udp_path_probe_success(&self, index: usize, elapsed: Duration) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_success(elapsed);
        }
    }

    pub(in crate::runtime) fn should_probe_udp_path(&self, index: usize) -> bool {
        let now = Instant::now();
        self.state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
            .is_some_and(|record| {
                record.maintain(now);
                path_observation_is_idle_for_probe(record.observation_at(now))
            })
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

    fn mark_udp_reliable_path_delivery(&self, index: usize, stats: PathDeliveryStats) {
        let Some(sample) = stats.rate_sample() else {
            return;
        };
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_product_delivery(sample);
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
        if let Some(current) = self
            .state
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

    pub(in crate::runtime) fn mark_udp_path_feedback(
        &self,
        index: usize,
        observation: UdpDatagramPathObservation,
    ) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_udp_datagram_feedback(observation);
        }
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
#[path = "state_test.rs"]
mod tests;
