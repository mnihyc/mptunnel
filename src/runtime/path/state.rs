//! Shared client-path evidence and carrier-neutral capacity budgets.
//!
//! One health lock makes mixed TCP/QUIC observations coherent. Each carrier
//! module owns its reservation, proof, and rollback transaction.

use super::commands::CapacityProbeCommandTicket;
use super::health::{
    ClientPathHealth, RequestCapacityReconciliationView,
    RequestQuicCapacityReconciliationObservation,
};
use super::model::{
    PathDeliveryStats, UdpDatagramPathObservation, path_observation_is_idle_for_probe,
    path_records_have_schedulable_alternative,
};
#[cfg(test)]
use super::proof::PathProofObservation;
use super::quic::{
    RequestQuicCapacityProbeLease, RequestQuicCapacityProbeSession,
    RequestQuicCapacityProductHandoffState, RequestQuicCapacityReconciliationQuery,
};
use super::set::ClientPathContext;
use super::tcp::capacity::{
    RequestTcpCapacityProbeLease, RequestTcpCapacityProbeSession, RequestTcpCapacityProofQuery,
};
#[cfg(test)]
use super::*;
use crate::model::capacity::{PathRateSample, reliable_capacity_calibration_session_limit_bytes};
use crate::model::path::{RelayPathInstance, RelayPathKey};
use crate::protocol::{StreamId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::scheduler::FlowLane;
use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU32, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

/// One lock domain for coherent health, load, and carrier budget composition.
#[derive(Debug)]
pub(in crate::runtime) struct ClientPathState {
    health: Mutex<ClientPathHealth>,
    next_reliable_stream_id: Mutex<u64>,
    active_tcp_service_request_bulk_flows: AtomicU32,
    request_quic_capacity_probe: RequestQuicCapacityProbeSession,
    request_tcp_capacity_probe: RequestTcpCapacityProbeSession,
}

impl ClientPathState {
    pub(in crate::runtime) fn new(health: ClientPathHealth) -> Arc<Self> {
        let tcp_path_count = health.tcp.len();
        let udp_path_count = health.udp.len();
        Arc::new(Self {
            health: Mutex::new(health),
            next_reliable_stream_id: Mutex::new(0),
            active_tcp_service_request_bulk_flows: AtomicU32::new(0),
            request_quic_capacity_probe: RequestQuicCapacityProbeSession::new(udp_path_count),
            request_tcp_capacity_probe: RequestTcpCapacityProbeSession::new(tcp_path_count),
        })
    }

    pub(in crate::runtime) fn health(&self) -> &Mutex<ClientPathHealth> {
        &self.health
    }

    pub(in crate::runtime::path) fn request_tcp_capacity_probe_session(
        &self,
    ) -> &RequestTcpCapacityProbeSession {
        &self.request_tcp_capacity_probe
    }

    pub(in crate::runtime::path) fn request_quic_capacity_probe_session(
        &self,
    ) -> &RequestQuicCapacityProbeSession {
        &self.request_quic_capacity_probe
    }

    fn release_relay_path_load(&self, key: RelayPathKey, lane: FlowLane) {
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
/// Initial Active opens acquire a lease before I/O and transfer it with the
/// attachment. Passive validation acquires one only after unique product bytes
/// commit. Dropping any transaction owner rolls the scheduler load back.
pub(in crate::runtime) struct RelayPathLoadLease {
    state: Arc<ClientPathState>,
    key: RelayPathKey,
    lane: FlowLane,
}

impl RelayPathLoadLease {
    pub(super) fn new(state: Arc<ClientPathState>, key: RelayPathKey, lane: FlowLane) -> Self {
        Self { state, key, lane }
    }

    pub(in crate::runtime) fn set_recorded_lane(&mut self, lane: FlowLane) {
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

#[derive(Debug)]
struct ReliableTcpRequestBulkFlowRegistrationState {
    path_state: Arc<ClientPathState>,
    counted: Mutex<bool>,
}

#[derive(Clone, Debug)]
pub(in crate::runtime) struct ReliableTcpRequestBulkFlowRegistration {
    state: Arc<ReliableTcpRequestBulkFlowRegistrationState>,
}

impl ReliableTcpRequestBulkFlowRegistration {
    pub(in crate::runtime) fn update(
        &self,
        request_bulk_active: bool,
        service_underlay: Option<UnderlayProtocol>,
    ) {
        let counted = request_bulk_active && service_underlay == Some(UnderlayProtocol::Tcp);
        let mut current = self
            .state
            .counted
            .lock()
            .expect("TCP-Service request bulk flow registration lock");
        if *current == counted {
            return;
        }
        if counted {
            self.state
                .path_state
                .active_tcp_service_request_bulk_flows
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    count.checked_add(1)
                })
                .expect("TCP-Service request bulk flow count overflow");
        } else {
            self.state
                .path_state
                .active_tcp_service_request_bulk_flows
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    count.checked_sub(1)
                })
                .expect("TCP-Service request bulk flow registration is unbalanced");
        }
        *current = counted;
    }
}

impl Drop for ReliableTcpRequestBulkFlowRegistrationState {
    fn drop(&mut self) {
        let counted = self
            .counted
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *counted {
            self.path_state
                .active_tcp_service_request_bulk_flows
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    count.checked_sub(1)
                })
                .expect("TCP-Service request bulk flow registration is unbalanced");
        }
    }
}

impl ClientPathContext {
    pub(in crate::runtime) fn health(&self) -> &Mutex<ClientPathHealth> {
        self.state.health()
    }

    pub(in crate::runtime) fn retire_request_quic_capacity_probe_token(&self, token: u64) {
        self.state.retire_request_quic_capacity_probe_token(token);
    }

    pub(in crate::runtime) fn request_quic_capacity_probe_remaining_bytes(&self) -> u64 {
        self.state.request_quic_capacity_probe_remaining_bytes(
            reliable_capacity_calibration_session_limit_bytes(self.mux_limits),
        )
    }

    pub(in crate::runtime) fn request_tcp_capacity_probe_remaining_bytes(&self) -> u64 {
        self.state.request_tcp_capacity_probe_remaining_bytes(
            reliable_capacity_calibration_session_limit_bytes(self.mux_limits),
        )
    }

    pub(in crate::runtime) fn request_quic_capacity_probe_candidate_share_bytes(
        &self,
        proposed_path_limit: u64,
    ) -> u64 {
        self.state
            .request_quic_capacity_probe_candidate_share_bytes(
                proposed_path_limit,
                reliable_capacity_calibration_session_limit_bytes(self.mux_limits),
            )
    }

    pub(in crate::runtime) fn request_tcp_capacity_probe_candidate_share_bytes(
        &self,
        proposed_path_limit: u64,
    ) -> u64 {
        self.state.request_tcp_capacity_probe_candidate_share_bytes(
            proposed_path_limit,
            reliable_capacity_calibration_session_limit_bytes(self.mux_limits),
        )
    }

    pub(in crate::runtime) fn request_quic_capacity_probe_path_remaining_bytes(
        &self,
        path_index: usize,
        path_limit: u64,
    ) -> u64 {
        self.state.request_quic_capacity_probe_path_remaining_bytes(
            path_index,
            path_limit,
            reliable_capacity_calibration_session_limit_bytes(self.mux_limits),
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
            reliable_capacity_calibration_session_limit_bytes(self.mux_limits),
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
            reliable_capacity_calibration_session_limit_bytes(self.mux_limits),
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
        quic_query: Option<RequestQuicCapacityReconciliationQuery>,
        now: Instant,
    ) -> RequestCapacityReconciliationView {
        let mut tcp_queries = tcp_queries.peekable();
        if tcp_queries.peek().is_none() && quic_query.is_none() {
            return RequestCapacityReconciliationView {
                observed_at: now,
                tcp_proofs: HashMap::new(),
                quic: None,
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
        let quic = quic_query.map(|query| {
            let reconciliation = health.udp.get(query.target.key.index).map(|record| {
                record
                    .quic_capacity
                    .reconciliation_at(stream_id, query.target, query.token, now)
            });
            RequestQuicCapacityReconciliationObservation {
                target: query.target,
                token: query.token,
                carrier_proven: reconciliation.is_some_and(|view| view.carrier_proven),
                handoff: reconciliation
                    .map_or(RequestQuicCapacityProductHandoffState::Absent, |view| {
                        view.handoff
                    }),
            }
        });
        RequestCapacityReconciliationView {
            observed_at: now,
            tcp_proofs,
            quic,
        }
    }

    pub(in crate::runtime) fn try_reserve_request_quic_capacity_probe(
        &self,
        stream_id: StreamId,
        path_index: usize,
        path_instance: RelayPathInstance,
        token: u64,
        train_bytes: u64,
        path_limit_bytes: u64,
        campaign: Arc<RequestCapacityProbeCampaignBudget>,
        valid_after: Instant,
        expires_at: Instant,
        proof_validity: Duration,
        ticket: CapacityProbeCommandTicket,
    ) -> Option<RequestQuicCapacityProbeLease> {
        self.state.try_reserve_request_quic_capacity_probe(
            stream_id,
            path_index,
            path_instance,
            token,
            train_bytes,
            path_limit_bytes,
            reliable_capacity_calibration_session_limit_bytes(self.mux_limits),
            campaign,
            valid_after,
            expires_at,
            proof_validity,
            ticket,
        )
    }

    #[cfg(test)]
    pub(in crate::runtime) fn request_quic_capacity_probe_proven_at(
        &self,
        path_index: usize,
        token: u64,
        now: Instant,
    ) -> bool {
        self.state
            .request_quic_capacity_probe_proven_at(path_index, token, now)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn request_quic_capacity_product_handoff_state_at(
        &self,
        path_index: usize,
        token: u64,
        now: Instant,
    ) -> RequestQuicCapacityProductHandoffState {
        self.state
            .request_quic_capacity_product_handoff_state_at(path_index, token, now)
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

    pub(in crate::runtime) fn reliable_tcp_request_bulk_flow_registration(
        &self,
    ) -> ReliableTcpRequestBulkFlowRegistration {
        ReliableTcpRequestBulkFlowRegistration {
            state: Arc::new(ReliableTcpRequestBulkFlowRegistrationState {
                path_state: self.state.clone(),
                counted: Mutex::new(false),
            }),
        }
    }

    pub(in crate::runtime) fn active_tcp_service_request_bulk_flows(&self) -> u32 {
        self.state
            .active_tcp_service_request_bulk_flows
            .load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn reserve_tcp_path_load(&self, index: usize, lane: FlowLane) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.reserve_load(lane);
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn reserve_udp_stream_path_load(&self, index: usize, lane: FlowLane) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.reserve_load(lane);
        }
    }

    /// Returns the only owner of the newly published scheduler load.
    pub(in crate::runtime) fn reserve_relay_path_load(
        &self,
        key: RelayPathKey,
        lane: FlowLane,
    ) -> Option<RelayPathLoadLease> {
        let mut health = self.state.health.lock().expect("client path health lock");
        let records = match key.underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        records.get_mut(key.index)?.reserve_load(lane);
        drop(health);
        Some(RelayPathLoadLease::new(self.state.clone(), key, lane))
    }

    pub(in crate::runtime) fn mark_tcp_path_open_success(
        &self,
        index: usize,
        elapsed: Duration,
        lane: FlowLane,
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

    pub(in crate::runtime) fn release_tcp_path_load(&self, index: usize, lane: FlowLane) {
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

    #[cfg(test)]
    pub(in crate::runtime) fn release_udp_stream_path_load(&self, index: usize, lane: FlowLane) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.release_load(lane);
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

    pub(in crate::runtime) fn mark_relay_path_data_plane_failure(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
    ) {
        match underlay {
            UnderlayProtocol::Tcp => self.mark_tcp_path_data_plane_failure(index),
            UnderlayProtocol::Udp => self.mark_udp_path_data_plane_failure(index),
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn release_relay_path_load(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        lane: FlowLane,
    ) {
        self.state
            .release_relay_path_load(RelayPathKey { underlay, index }, lane);
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

    pub(in crate::runtime) fn record_relay_path_product_ack(
        &self,
        stream_id: StreamId,
        path_instance: RelayPathInstance,
        bytes: usize,
        sent_at: Instant,
        acked_at: Instant,
    ) {
        if bytes == 0 || path_instance.key.underlay != UnderlayProtocol::Udp {
            return;
        }
        self.state.record_request_quic_capacity_product_ack(
            stream_id,
            path_instance,
            bytes,
            sent_at,
            acked_at,
        );
    }

    pub(in crate::runtime) fn change_relay_path_lane_load(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        from: FlowLane,
        to: FlowLane,
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

    pub(in crate::runtime) fn mark_tcp_path_failure(&self, index: usize) {
        let now = Instant::now();
        let mut health = self.state.health.lock().expect("client path health lock");
        let has_schedulable_alternative =
            path_records_have_schedulable_alternative(&mut health.tcp, index, now);
        if let Some(current) = health.tcp.get_mut(index) {
            current.mark_failure(now, has_schedulable_alternative);
        }
    }

    pub(in crate::runtime) fn mark_tcp_path_data_plane_failure(&self, index: usize) {
        let now = Instant::now();
        let mut health = self.state.health.lock().expect("client path health lock");
        let has_schedulable_alternative =
            path_records_have_schedulable_alternative(&mut health.tcp, index, now);
        if let Some(current) = health.tcp.get_mut(index) {
            current.mark_data_plane_failure(now, has_schedulable_alternative);
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
            current.mark_open_success(elapsed, FlowLane::RealtimeDatagram);
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
            current.release_load(FlowLane::RealtimeDatagram);
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

    pub(in crate::runtime) fn mark_udp_path_mtu(&self, index: usize, payload_bytes: usize) {
        if let Some(current) = self
            .state
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_udp_mtu(payload_bytes);
        }
    }

    pub(in crate::runtime) fn mark_udp_path_failure(&self, index: usize) {
        let now = Instant::now();
        let mut health = self.state.health.lock().expect("client path health lock");
        let has_schedulable_alternative =
            path_records_have_schedulable_alternative(&mut health.udp, index, now);
        if let Some(current) = health.udp.get_mut(index) {
            current.mark_failure(now, has_schedulable_alternative);
        }
    }

    pub(in crate::runtime) fn mark_udp_path_data_plane_failure(&self, index: usize) {
        let now = Instant::now();
        let mut health = self.state.health.lock().expect("client path health lock");
        let has_schedulable_alternative =
            path_records_have_schedulable_alternative(&mut health.udp, index, now);
        if let Some(current) = health.udp.get_mut(index) {
            current.mark_data_plane_failure(now, has_schedulable_alternative);
        }
    }
}

#[cfg(test)]
#[path = "state_test.rs"]
mod tests;
