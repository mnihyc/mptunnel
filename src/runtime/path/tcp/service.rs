//! Session-owned admission for TCP carrier validation.
//!
//! The serialized request sender reports only demand-lifecycle transitions,
//! successful ordinary placement, and the immediately following exact
//! saturation observation. This owner turns those cold facts into the one
//! bounded admission transaction defined by RFC 7.2 and 15.1. Native TCP
//! congestion control, candidate I/O, Product evidence, and verdicts remain
//! with their existing owners. Direction-specific admission sources share one
//! validation transaction and one client-issued validation-ID sequence.

use super::group::{
    ClientTcpCarrierGroups, ClientTcpCarrierReservation, ClientTcpEndpointPolicy,
    ClientTcpEndpointPolicySnapshot,
};
use crate::model::path::RelayPathInstance;
use crate::model::tcp_carrier::{
    TcpCarrierPolicyEpochs as ClientTcpCarrierPolicyEpochs,
    TcpCarrierStableGenerations as ClientTcpCarrierStableGenerations, TcpCarrierValidationGeometry,
    tcp_carrier_validation_geometry,
};
use crate::mux::MuxLimits;
#[cfg(test)]
use crate::protocol::{PathId, PathUsage};
use crate::protocol::{PathMetricDirection, StreamId};
use crate::runtime::path::ClientPathContext;
use crate::runtime::sender::{
    ProductWorkloadIdentity, RequestProductAckReceipt, RequestProductAckReceiptSink,
    RequestProductAckReceiptTarget,
};
use crate::scheduler::TrafficClass;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{mpsc, watch};

/// One frozen ordinary carrier and its established two-BDP Product service
/// pipe, already rounded and floored by the regular scheduling model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ClientTcpCarrierOrdinaryService {
    pub(in crate::runtime) instance: RelayPathInstance,
    pub(in crate::runtime) service_pipe_bytes: u64,
}

/// Exact ordinary-saturation facts supplied by the serialized request sender.
///
/// The target identity and continuous demand generation come from the
/// workload lease and therefore cannot be reconstructed by a stale caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) struct ClientTcpCarrierSaturation {
    pub(in crate::runtime) stable: ClientTcpCarrierStableGenerations,
    pub(in crate::runtime) ordinary_services: Box<[ClientTcpCarrierOrdinaryService]>,
    /// Locally eligible configured TCP groups, in Product policy preference
    /// order. A group is a local resource identity and never enters the wire.
    pub(in crate::runtime) eligible_tcp_groups: Box<[usize]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClientTcpCarrierAdmissionGenerationKey {
    target: ProductWorkloadIdentity,
    demand_generation: NonZeroU64,
    workload_generation: NonZeroU64,
    stable: ClientTcpCarrierStableGenerations,
    ordinary_instances: Box<[RelayPathInstance]>,
}

#[derive(Debug)]
struct ClientTcpCarrierAdmissionGeneration {
    id: NonZeroU64,
    key: ClientTcpCarrierAdmissionGenerationKey,
    attempted_groups: BTreeSet<usize>,
    candidate_started: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientTcpCarrierCandidatePhase {
    Establishing,
    Validating,
}

#[derive(Debug)]
struct ClientTcpCarrierCandidateState {
    validation_id: NonZeroU64,
    admission_generation: NonZeroU64,
    phase: ClientTcpCarrierCandidatePhase,
    withdrawn: bool,
    observations: Option<mpsc::Sender<ClientTcpCarrierObservation>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ClientTcpCarrierRetainedValidationState {
    validation_id: NonZeroU64,
    direction: PathMetricDirection,
    request_id: Option<NonZeroU64>,
    stream_id: Option<StreamId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientTcpCarrierServerToClientPhase {
    Establishing,
    Validating,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ClientTcpCarrierServerToClientState {
    request_id: NonZeroU64,
    validation_id: NonZeroU64,
    stream_id: StreamId,
    phase: ClientTcpCarrierServerToClientPhase,
}

impl ClientTcpCarrierServerToClientState {
    fn same_transaction(self, other: Self) -> bool {
        self.request_id == other.request_id
            && self.validation_id == other.validation_id
            && self.stream_id == other.stream_id
    }
}

#[derive(Debug)]
enum ClientTcpCarrierValidationTransaction {
    ClientToServerCandidate(ClientTcpCarrierCandidateState),
    ServerToClientCandidate(ClientTcpCarrierServerToClientState),
    RetainedDirection(ClientTcpCarrierRetainedValidationState),
}

impl ClientTcpCarrierValidationTransaction {
    fn client_to_server_candidate(&self) -> Option<&ClientTcpCarrierCandidateState> {
        match self {
            Self::ClientToServerCandidate(candidate) => Some(candidate),
            Self::ServerToClientCandidate(_) | Self::RetainedDirection(_) => None,
        }
    }

    fn client_to_server_candidate_mut(&mut self) -> Option<&mut ClientTcpCarrierCandidateState> {
        match self {
            Self::ClientToServerCandidate(candidate) => Some(candidate),
            Self::ServerToClientCandidate(_) | Self::RetainedDirection(_) => None,
        }
    }
}

/// Exact server-owned request accepted by the client for one fresh S2C
/// validation-purpose carrier. The local group reservation remains separate
/// so the carrier actor can retain it only after the matching acknowledgment
/// has been serialized.
pub(in crate::runtime) struct ClientTcpServerToClientAdmission {
    lease: Option<ClientTcpServerToClientAdmissionLease>,
    reservation: Option<ClientTcpCarrierReservation>,
}

pub(in crate::runtime) struct ClientTcpServerToClientAdmissionLease {
    owner: Weak<ClientTcpCarrierService>,
    identity: ClientTcpCarrierServerToClientState,
    config_index: usize,
    endpoint_policy: Arc<ClientTcpEndpointPolicy>,
    endpoint_snapshot: ClientTcpEndpointPolicySnapshot,
    armed: bool,
}

/// Exact sender facts routed to the one active directional validation owner.
/// No observation is created or captured while validation is inactive.
#[derive(Debug)]
pub(in crate::runtime) enum ClientTcpCarrierObservation {
    ProductAck(RequestProductAckReceipt),
}

/// Latest server-owned S2C expansion request observed by this client session.
/// `stream_id = None` is an explicit withdrawal, not an absent publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ClientTcpCarrierDemand {
    pub(in crate::runtime) request_id: NonZeroU64,
    pub(in crate::runtime) stream_id: Option<StreamId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ClientTcpCarrierDemandConflict;

#[derive(Debug, Clone, Copy)]
struct ClientTcpCarrierWorkload {
    identity: ProductWorkloadIdentity,
    lane: TrafficClass,
    queued_unique_original: bool,
    demand_generation: Option<NonZeroU64>,
}

#[derive(Debug)]
struct ClientTcpCarrierServiceState {
    next_workload_generation: Option<NonZeroU64>,
    next_demand_generation: Option<NonZeroU64>,
    next_admission_generation: Option<NonZeroU64>,
    next_validation_id: Option<NonZeroU64>,
    workload_generation: NonZeroU64,
    workloads: BTreeMap<StreamId, ClientTcpCarrierWorkload>,
    admission: Option<ClientTcpCarrierAdmissionGeneration>,
    validation_transaction: Option<ClientTcpCarrierValidationTransaction>,
    server_demand: Option<ClientTcpCarrierDemand>,
    observations_active: Arc<AtomicBool>,
}

/// One carrier-validation admission owner per client MPP session.
#[derive(Debug)]
pub(in crate::runtime) struct ClientTcpCarrierService {
    state: Mutex<ClientTcpCarrierServiceState>,
    policy_changes: watch::Sender<Option<ClientTcpCarrierPolicyEpochs>>,
    server_demand_changes: watch::Sender<Option<ClientTcpCarrierDemand>>,
    observations_active: Arc<AtomicBool>,
}

impl ClientTcpCarrierService {
    pub(in crate::runtime) fn new() -> Arc<Self> {
        let one = NonZeroU64::new(1).expect("one is nonzero");
        let initial_policy = ClientTcpCarrierPolicyEpochs {
            ordinary_eligibility_generation: one,
            admission_policy_generation: one,
            resource_policy_generation: one,
        };
        let (policy_changes, _) = watch::channel(Some(initial_policy));
        let (server_demand_changes, _) = watch::channel(None);
        let observations_active = Arc::new(AtomicBool::new(false));
        Arc::new(Self {
            state: Mutex::new(ClientTcpCarrierServiceState {
                next_workload_generation: Some(one),
                next_demand_generation: Some(one),
                next_admission_generation: Some(one),
                next_validation_id: Some(one),
                workload_generation: one,
                workloads: BTreeMap::new(),
                admission: None,
                validation_transaction: None,
                server_demand: None,
                observations_active: observations_active.clone(),
            }),
            policy_changes,
            server_demand_changes,
            observations_active,
        })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime) fn subscribe_server_demands(
        &self,
    ) -> watch::Receiver<Option<ClientTcpCarrierDemand>> {
        self.server_demand_changes.subscribe()
    }

    /// Applies the RFC monotonic request sequence shared by every TCP carrier
    /// in this client session. Older requests and exact duplicates are
    /// idempotent; reusing the current ID with different content is a protocol
    /// conflict and never changes the published request.
    pub(in crate::runtime) fn apply_server_demand(
        &self,
        demand: ClientTcpCarrierDemand,
    ) -> Result<(), ClientTcpCarrierDemandConflict> {
        let mut state = self.state.lock().expect("TCP carrier service lock");
        if let Some(current) = state.server_demand {
            match demand.request_id.cmp(&current.request_id) {
                std::cmp::Ordering::Less => return Ok(()),
                std::cmp::Ordering::Equal if demand == current => return Ok(()),
                std::cmp::Ordering::Equal => return Err(ClientTcpCarrierDemandConflict),
                std::cmp::Ordering::Greater => {}
            }
        }
        state.server_demand = Some(demand);
        self.server_demand_changes.send_replace(Some(demand));
        Ok(())
    }

    /// Claims the exact current nonzero S2C request and one locally bounded
    /// elastic slot. A peer request never chooses an endpoint or bypasses the
    /// configured minimum/maximum envelope; endpoint selection remains the
    /// client's existing Product-policy order.
    pub(in crate::runtime) fn try_claim_server_to_client_demand(
        self: &Arc<Self>,
        demand: ClientTcpCarrierDemand,
        groups: &Arc<ClientTcpCarrierGroups>,
    ) -> Option<ClientTcpServerToClientAdmission> {
        let stream_id = demand.stream_id?;
        let mut state = self.state.lock().expect("TCP carrier service lock");
        if state.server_demand != Some(demand) || state.validation_transaction.is_some() {
            return None;
        }

        let selected = (0..groups.len()).find_map(|config_index| {
            let group = groups.get(config_index)?;
            let occupied = groups.occupied(config_index)?;
            if occupied < group.range.min() || occupied >= group.range.max() {
                return None;
            }
            let endpoint_policy = groups.endpoint_policy(config_index)?;
            let endpoint_snapshot = endpoint_policy.snapshot();
            if !endpoint_snapshot.enabled {
                return None;
            }
            groups.reserve_elastic(config_index).map(|reservation| {
                (
                    config_index,
                    endpoint_policy,
                    endpoint_snapshot,
                    reservation,
                )
            })
        })?;
        let validation_id = take_sequence(&mut state.next_validation_id)?;
        let identity = ClientTcpCarrierServerToClientState {
            request_id: demand.request_id,
            validation_id,
            stream_id,
            phase: ClientTcpCarrierServerToClientPhase::Establishing,
        };
        state.validation_transaction =
            Some(ClientTcpCarrierValidationTransaction::ServerToClientCandidate(identity));
        let (config_index, endpoint_policy, endpoint_snapshot, reservation) = selected;
        Some(ClientTcpServerToClientAdmission {
            lease: Some(ClientTcpServerToClientAdmissionLease {
                owner: Arc::downgrade(self),
                identity,
                config_index,
                endpoint_policy,
                endpoint_snapshot,
                armed: true,
            }),
            reservation: Some(reservation),
        })
    }

    fn server_to_client_is_current(&self, identity: ClientTcpCarrierServerToClientState) -> bool {
        let state = self.state.lock().expect("TCP carrier service lock");
        state.server_demand
            == Some(ClientTcpCarrierDemand {
                request_id: identity.request_id,
                stream_id: Some(identity.stream_id),
            })
            && state
                .validation_transaction
                .as_ref()
                .is_some_and(|transaction| {
                    matches!(
                        transaction,
                        ClientTcpCarrierValidationTransaction::ServerToClientCandidate(current)
                            if current.same_transaction(identity)
                    )
                })
    }

    fn begin_server_to_client_validation(
        &self,
        identity: ClientTcpCarrierServerToClientState,
    ) -> bool {
        let mut state = self.state.lock().expect("TCP carrier service lock");
        if state.server_demand
            != Some(ClientTcpCarrierDemand {
                request_id: identity.request_id,
                stream_id: Some(identity.stream_id),
            })
        {
            return false;
        }
        let Some(ClientTcpCarrierValidationTransaction::ServerToClientCandidate(current)) =
            state.validation_transaction.as_mut()
        else {
            return false;
        };
        if !current.same_transaction(identity)
            || current.phase != ClientTcpCarrierServerToClientPhase::Establishing
        {
            return false;
        }
        current.phase = ClientTcpCarrierServerToClientPhase::Validating;
        true
    }

    fn finish_server_to_client_validation(
        &self,
        identity: ClientTcpCarrierServerToClientState,
    ) -> bool {
        let mut state = self.state.lock().expect("TCP carrier service lock");
        let exact = state
            .validation_transaction
            .as_ref()
            .is_some_and(|transaction| {
                matches!(
                    transaction,
                    ClientTcpCarrierValidationTransaction::ServerToClientCandidate(current)
                        if current.same_transaction(identity)
                )
            });
        if exact {
            state.validation_transaction = None;
        }
        exact
    }

    pub(in crate::runtime) fn subscribe_policy_epochs(
        &self,
    ) -> watch::Receiver<Option<ClientTcpCarrierPolicyEpochs>> {
        self.policy_changes.subscribe()
    }

    /// Invalidates an active comparison and publishes the path owner's exact
    /// replacement policy snapshot. The service never invents policy epochs.
    pub(in crate::runtime) fn publish_policy_epochs(
        &self,
        policy: Option<ClientTcpCarrierPolicyEpochs>,
    ) {
        let mut state = self.state.lock().expect("TCP carrier service lock");
        withdraw_candidate(&mut state);
        drop(state);
        self.policy_changes.send_replace(policy);
    }

    /// Registers one complete logical request-direction Product workload.
    /// Registration and drop are cold lifecycle operations; payload dispatch
    /// performs no service lock or shared counter update.
    pub(in crate::runtime) fn register_workload(
        self: &Arc<Self>,
        stream_id: StreamId,
    ) -> Option<ClientTcpCarrierWorkloadLease> {
        let mut state = self.state.lock().expect("TCP carrier service lock");
        if state.workloads.contains_key(&stream_id) {
            return None;
        }
        let identity = ProductWorkloadIdentity {
            stream_id,
            lifecycle_generation: take_sequence(&mut state.next_workload_generation)?,
        };
        let next_workload_generation = increment_generation(state.workload_generation)?;
        state.workload_generation = next_workload_generation;
        state.workloads.insert(
            stream_id,
            ClientTcpCarrierWorkload {
                identity,
                lane: TrafficClass::Latency,
                queued_unique_original: false,
                demand_generation: None,
            },
        );
        withdraw_candidate(&mut state);
        Some(ClientTcpCarrierWorkloadLease {
            owner: Arc::downgrade(self),
            observations_active: self.observations_active.clone(),
            identity,
            lane: TrafficClass::Latency,
            queued_unique_original: false,
            demand_generation: None,
            successful_placement: None,
        })
    }

    fn update_workload_demand(
        &self,
        identity: ProductWorkloadIdentity,
        lane: TrafficClass,
        queued_unique_original: bool,
    ) -> Option<Option<NonZeroU64>> {
        let mut state = self.state.lock().expect("TCP carrier service lock");
        let current = *state.workloads.get(&identity.stream_id)?;
        if current.identity != identity {
            return None;
        }

        // The classifier owns the continuous demand episode. A bounded sender
        // queue may momentarily drain because ordinary placement succeeded;
        // that is not an idle transition and must not mint a new generation.
        let throughput_demand = lane == TrafficClass::Throughput;
        let demand_generation = match (current.demand_generation, throughput_demand) {
            (Some(generation), true) => Some(generation),
            (None, true) => Some(take_sequence(&mut state.next_demand_generation)?),
            (_, false) => None,
        };
        if current.lane == lane
            && current.queued_unique_original == queued_unique_original
            && current.demand_generation == demand_generation
        {
            return Some(demand_generation);
        }

        let workload = state
            .workloads
            .get_mut(&identity.stream_id)
            .expect("validated workload remains registered");
        workload.lane = lane;
        workload.queued_unique_original = queued_unique_original;
        workload.demand_generation = demand_generation;

        if state
            .validation_transaction
            .as_ref()
            .and_then(ClientTcpCarrierValidationTransaction::client_to_server_candidate)
            .is_some_and(|candidate| {
                let target_changed = state.admission.as_ref().is_some_and(|admission| {
                    admission.key.target == identity
                        && Some(admission.key.demand_generation) != demand_generation
                });
                let latency_work_became_active =
                    queued_unique_original && lane.is_latency_sensitive();
                !candidate.withdrawn && (target_changed || latency_work_became_active)
            })
        {
            withdraw_candidate(&mut state);
        }
        Some(demand_generation)
    }

    fn unregister_workload(&self, identity: ProductWorkloadIdentity) {
        let mut state = self.state.lock().expect("TCP carrier service lock");
        if state
            .workloads
            .get(&identity.stream_id)
            .is_none_or(|workload| workload.identity != identity)
        {
            return;
        }
        state.workloads.remove(&identity.stream_id);
        if let Some(next) = increment_generation(state.workload_generation) {
            state.workload_generation = next;
        } else {
            // Exhaustion cannot alias an earlier comparison key. Retiring the
            // current candidate and denying later generation creation is the
            // only safe terminal local policy.
            state.next_admission_generation = None;
        }
        withdraw_candidate(&mut state);
    }

    fn try_admit(
        self: &Arc<Self>,
        identity: ProductWorkloadIdentity,
        demand_generation: NonZeroU64,
        successful_placement: ClientTcpCarrierStableGenerations,
        saturation: ClientTcpCarrierSaturation,
        groups: &Arc<ClientTcpCarrierGroups>,
        mux_limits: MuxLimits,
    ) -> Option<ClientTcpCarrierAdmission> {
        if successful_placement != saturation.stable {
            return None;
        }

        let mut ordinary_services = saturation.ordinary_services.into_vec();
        if ordinary_services.is_empty()
            || ordinary_services
                .iter()
                .any(|ordinary| ordinary.service_pipe_bytes == 0)
        {
            return None;
        }
        ordinary_services.sort_unstable_by_key(|ordinary| {
            (
                ordinary.instance.key.underlay,
                ordinary.instance.key.index,
                ordinary.instance.path_instance_id.as_u64(),
                ordinary.instance.attachment_id,
            )
        });
        if ordinary_services
            .windows(2)
            .any(|pair| pair[0].instance == pair[1].instance)
        {
            return None;
        }
        let ordinary_instances = ordinary_services
            .iter()
            .map(|ordinary| ordinary.instance)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let geometry = tcp_carrier_validation_geometry(
            ordinary_services
                .iter()
                .map(|ordinary| ordinary.service_pipe_bytes),
            mux_limits,
        )?;

        let mut eligible_groups = saturation.eligible_tcp_groups.into_vec();
        eligible_groups.sort_unstable();
        eligible_groups.dedup();
        if eligible_groups.is_empty() {
            return None;
        }

        let mut state = self.state.lock().expect("TCP carrier service lock");
        if state.validation_transaction.is_some() {
            return None;
        }
        let target = *state.workloads.get(&identity.stream_id)?;
        if target.identity != identity
            || target.demand_generation != Some(demand_generation)
            || target.lane != TrafficClass::Throughput
            || !target.queued_unique_original
            || state.workloads.values().any(|workload| {
                workload.queued_unique_original && workload.lane.is_latency_sensitive()
            })
        {
            return None;
        }

        let generation_key = ClientTcpCarrierAdmissionGenerationKey {
            target: identity,
            demand_generation,
            workload_generation: state.workload_generation,
            stable: saturation.stable,
            ordinary_instances,
        };
        if !install_or_reuse_admission_generation(&mut state, generation_key) {
            return None;
        }
        let admission = state
            .admission
            .as_ref()
            .expect("admission generation was installed");
        if admission.candidate_started {
            return None;
        }

        let selected = eligible_groups.into_iter().find_map(|config_index| {
            let attempted = state
                .admission
                .as_ref()
                .expect("admission generation remains installed")
                .attempted_groups
                .contains(&config_index);
            if attempted {
                return None;
            }
            let group = groups.get(config_index)?;
            let occupied = groups.occupied(config_index)?;
            if occupied < group.range.min() || occupied >= group.range.max() {
                return None;
            }
            let endpoint_policy = groups.endpoint_policy(config_index)?;
            let endpoint_snapshot = endpoint_policy.snapshot();
            if !endpoint_snapshot.enabled {
                return None;
            }
            groups.reserve_elastic(config_index).map(|reservation| {
                (
                    config_index,
                    endpoint_policy,
                    endpoint_snapshot,
                    reservation,
                )
            })
        })?;
        let (config_index, endpoint_policy, endpoint_snapshot, reservation) = selected;
        let validation_id = take_sequence(&mut state.next_validation_id)?;
        let admission_generation = state
            .admission
            .as_ref()
            .expect("admission generation remains installed")
            .id;
        {
            let admission = state
                .admission
                .as_mut()
                .expect("admission generation remains installed");
            admission.attempted_groups.insert(config_index);
            admission.candidate_started = true;
        }
        state.validation_transaction = Some(
            ClientTcpCarrierValidationTransaction::ClientToServerCandidate(
                ClientTcpCarrierCandidateState {
                    validation_id,
                    admission_generation,
                    phase: ClientTcpCarrierCandidatePhase::Establishing,
                    withdrawn: false,
                    observations: None,
                },
            ),
        );
        let workloads = state
            .workloads
            .values()
            .map(|workload| workload.identity)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        drop(state);

        Some(ClientTcpCarrierAdmission {
            lease: Some(ClientTcpCarrierAdmissionLease {
                owner: Arc::downgrade(self),
                validation_id,
                admission_generation,
                target: identity,
                stable: saturation.stable,
                workloads,
                ordinary_services: ordinary_services.into_boxed_slice(),
                geometry,
                config_index,
                endpoint_policy,
                endpoint_snapshot,
                armed: true,
            }),
            reservation: Some(reservation),
        })
    }

    fn begin_validation(
        &self,
        validation_id: NonZeroU64,
        admission_generation: NonZeroU64,
    ) -> bool {
        let mut state = self.state.lock().expect("TCP carrier service lock");
        let Some(candidate) = state
            .validation_transaction
            .as_mut()
            .and_then(ClientTcpCarrierValidationTransaction::client_to_server_candidate_mut)
        else {
            return false;
        };
        if candidate.validation_id != validation_id
            || candidate.admission_generation != admission_generation
            || candidate.withdrawn
            || candidate.phase != ClientTcpCarrierCandidatePhase::Establishing
        {
            return false;
        }
        candidate.phase = ClientTcpCarrierCandidatePhase::Validating;
        true
    }

    fn candidate_is_withdrawn(
        &self,
        validation_id: NonZeroU64,
        admission_generation: NonZeroU64,
    ) -> bool {
        self.state
            .lock()
            .expect("TCP carrier service lock")
            .validation_transaction
            .as_ref()
            .and_then(ClientTcpCarrierValidationTransaction::client_to_server_candidate)
            .is_none_or(|candidate| {
                candidate.validation_id != validation_id
                    || candidate.admission_generation != admission_generation
                    || candidate.withdrawn
            })
    }

    fn activate_observations(
        &self,
        validation_id: NonZeroU64,
        admission_generation: NonZeroU64,
        capacity: usize,
    ) -> Option<mpsc::Receiver<ClientTcpCarrierObservation>> {
        let mut state = self.state.lock().expect("TCP carrier service lock");
        let candidate = state
            .validation_transaction
            .as_mut()?
            .client_to_server_candidate_mut()?;
        if candidate.validation_id != validation_id
            || candidate.admission_generation != admission_generation
            || candidate.withdrawn
            || candidate.phase != ClientTcpCarrierCandidatePhase::Validating
            || candidate.observations.is_some()
        {
            return None;
        }
        let (observations, receiver) = mpsc::channel(capacity.max(1));
        candidate.observations = Some(observations);
        state.observations_active.store(true, Ordering::Release);
        Some(receiver)
    }

    fn publish_observation(&self, observation: ClientTcpCarrierObservation) {
        if !self.observations_active.load(Ordering::Acquire) {
            return;
        }
        let mut state = self.state.lock().expect("TCP carrier service lock");
        let Some(candidate) = state
            .validation_transaction
            .as_mut()
            .and_then(ClientTcpCarrierValidationTransaction::client_to_server_candidate_mut)
        else {
            state.observations_active.store(false, Ordering::Release);
            return;
        };
        if candidate.withdrawn {
            candidate.observations = None;
            state.observations_active.store(false, Ordering::Release);
            return;
        }
        let published = candidate
            .observations
            .as_ref()
            .is_some_and(|sender| sender.try_send(observation).is_ok());
        if !published {
            // A closed or saturated bounded evidence owner cannot support a
            // complete comparison. Withdraw; never drop a qualifying fact and
            // continue toward a verdict.
            candidate.withdrawn = true;
            candidate.observations = None;
            state.observations_active.store(false, Ordering::Release);
        }
    }

    fn revalidate_candidate(
        &self,
        validation_id: NonZeroU64,
        admission_generation: NonZeroU64,
        stable: ClientTcpCarrierStableGenerations,
        ordinary_instances: &[RelayPathInstance],
    ) -> bool {
        let mut canonical_instances = ordinary_instances.to_vec();
        canonical_instances.sort_unstable_by_key(|instance| {
            (
                instance.key.underlay,
                instance.key.index,
                instance.path_instance_id.as_u64(),
                instance.attachment_id,
            )
        });
        let mut state = self.state.lock().expect("TCP carrier service lock");
        let exact_candidate = state
            .validation_transaction
            .as_ref()
            .and_then(ClientTcpCarrierValidationTransaction::client_to_server_candidate)
            .is_some_and(|candidate| {
                candidate.validation_id == validation_id
                    && candidate.admission_generation == admission_generation
                    && !candidate.withdrawn
            });
        let exact_key = state.admission.as_ref().is_some_and(|admission| {
            admission.id == admission_generation
                && admission.key.stable == stable
                && admission.key.workload_generation == state.workload_generation
                && admission.key.ordinary_instances.as_ref() == canonical_instances.as_slice()
                && state
                    .workloads
                    .get(&admission.key.target.stream_id)
                    .is_some_and(|target| {
                        target.identity == admission.key.target
                            && target.demand_generation == Some(admission.key.demand_generation)
                            && target.lane == TrafficClass::Throughput
                    })
                && !state.workloads.values().any(|workload| {
                    workload.queued_unique_original && workload.lane.is_latency_sensitive()
                })
        });
        if exact_candidate && exact_key {
            true
        } else {
            withdraw_candidate(&mut state);
            false
        }
    }

    fn release_candidate(&self, validation_id: NonZeroU64, admission_generation: NonZeroU64) {
        let mut state = self.state.lock().expect("TCP carrier service lock");
        if state
            .validation_transaction
            .as_ref()
            .and_then(ClientTcpCarrierValidationTransaction::client_to_server_candidate)
            .is_some_and(|candidate| {
                candidate.validation_id == validation_id
                    && candidate.admission_generation == admission_generation
            })
        {
            state.validation_transaction = None;
            state.observations_active.store(false, Ordering::Release);
        }
    }

    /// Reserves this session's sole validation transaction for the opposite
    /// direction of an already-retained exact carrier. The same monotonic ID
    /// sequence is used by fresh and retained-carrier validation.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime) fn reserve_retained_direction_validation(
        self: &Arc<Self>,
        direction: PathMetricDirection,
    ) -> Option<ClientTcpRetainedDirectionValidationLease> {
        let mut state = self.state.lock().expect("TCP carrier service lock");
        if state.validation_transaction.is_some() {
            return None;
        }
        let validation_id = take_sequence(&mut state.next_validation_id)?;
        let validation = ClientTcpCarrierRetainedValidationState {
            validation_id,
            direction,
            request_id: None,
            stream_id: None,
        };
        state.validation_transaction = Some(
            ClientTcpCarrierValidationTransaction::RetainedDirection(validation),
        );
        Some(ClientTcpRetainedDirectionValidationLease {
            owner: Arc::downgrade(self),
            validation,
            active: true,
        })
    }

    fn release_retained_direction_validation(
        &self,
        validation: ClientTcpCarrierRetainedValidationState,
    ) {
        let mut state = self.state.lock().expect("TCP carrier service lock");
        if state
            .validation_transaction
            .as_ref()
            .is_some_and(|transaction| {
                matches!(
                    transaction,
                    ClientTcpCarrierValidationTransaction::RetainedDirection(current)
                        if *current == validation
                )
            })
        {
            state.validation_transaction = None;
        }
    }
}

fn install_or_reuse_admission_generation(
    state: &mut ClientTcpCarrierServiceState,
    key: ClientTcpCarrierAdmissionGenerationKey,
) -> bool {
    if state
        .admission
        .as_ref()
        .is_some_and(|admission| admission.key == key)
    {
        return true;
    }

    // Exact ordinary instances are governed by membership_generation. A
    // changed set under the same generation is an inconsistent observation,
    // not authority to mint another admission generation.
    if state.admission.as_ref().is_some_and(|admission| {
        admission.key.target == key.target
            && admission.key.demand_generation == key.demand_generation
            && admission.key.workload_generation == key.workload_generation
            && admission.key.stable == key.stable
            && admission.key.ordinary_instances != key.ordinary_instances
    }) {
        return false;
    }

    let Some(id) = take_sequence(&mut state.next_admission_generation) else {
        return false;
    };
    state.admission = Some(ClientTcpCarrierAdmissionGeneration {
        id,
        key,
        attempted_groups: BTreeSet::new(),
        candidate_started: false,
    });
    true
}

fn take_sequence(sequence: &mut Option<NonZeroU64>) -> Option<NonZeroU64> {
    let current = (*sequence)?;
    *sequence = current.get().checked_add(1).and_then(NonZeroU64::new);
    Some(current)
}

fn increment_generation(current: NonZeroU64) -> Option<NonZeroU64> {
    current.get().checked_add(1).and_then(NonZeroU64::new)
}

fn withdraw_candidate(state: &mut ClientTcpCarrierServiceState) {
    if let Some(candidate) = state
        .validation_transaction
        .as_mut()
        .and_then(ClientTcpCarrierValidationTransaction::client_to_server_candidate_mut)
    {
        candidate.withdrawn = true;
        candidate.observations = None;
        state.observations_active.store(false, Ordering::Release);
    }
}

impl ClientTcpServerToClientAdmissionLease {
    pub(in crate::runtime) fn request_id(&self) -> NonZeroU64 {
        self.identity.request_id
    }

    pub(in crate::runtime) fn validation_id(&self) -> NonZeroU64 {
        self.identity.validation_id
    }

    pub(in crate::runtime) fn stream_id(&self) -> StreamId {
        self.identity.stream_id
    }

    pub(in crate::runtime) fn config_index(&self) -> usize {
        self.config_index
    }

    pub(in crate::runtime) fn endpoint_generation(&self) -> u64 {
        self.endpoint_snapshot.generation
    }

    pub(in crate::runtime) fn is_current(&self) -> bool {
        self.endpoint_policy
            .allows(self.endpoint_snapshot.generation)
            && self
                .owner
                .upgrade()
                .is_some_and(|owner| owner.server_to_client_is_current(self.identity))
    }

    pub(in crate::runtime) fn begin_validation(&mut self) -> bool {
        self.is_current()
            && self
                .owner
                .upgrade()
                .is_some_and(|owner| owner.begin_server_to_client_validation(self.identity))
    }

    /// Completes the exact local S2C settlement after the result
    /// acknowledgment has entered this carrier's ordered writer. Directional
    /// publication remains the retained-carrier registry's responsibility.
    pub(in crate::runtime) fn finish(mut self) -> bool {
        let finished = self
            .owner
            .upgrade()
            .is_some_and(|owner| owner.finish_server_to_client_validation(self.identity));
        if finished {
            self.armed = false;
        }
        finished
    }

    fn cancel_inner(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        if let Some(owner) = self.owner.upgrade() {
            let _ = owner.finish_server_to_client_validation(self.identity);
        }
    }
}

impl Drop for ClientTcpServerToClientAdmissionLease {
    fn drop(&mut self) {
        self.cancel_inner();
    }
}

impl ClientTcpServerToClientAdmission {
    fn lease(&self) -> &ClientTcpServerToClientAdmissionLease {
        self.lease
            .as_ref()
            .expect("live S2C admission owns its service lease")
    }

    pub(in crate::runtime) fn request_id(&self) -> NonZeroU64 {
        self.lease().request_id()
    }

    pub(in crate::runtime) fn validation_id(&self) -> NonZeroU64 {
        self.lease().validation_id()
    }

    pub(in crate::runtime) fn stream_id(&self) -> StreamId {
        self.lease().stream_id()
    }

    pub(in crate::runtime) fn config_index(&self) -> usize {
        self.lease().config_index()
    }

    pub(in crate::runtime) fn into_parts(
        mut self,
    ) -> (
        ClientTcpServerToClientAdmissionLease,
        ClientTcpCarrierReservation,
    ) {
        (
            self.lease
                .take()
                .expect("live S2C admission owns its service lease"),
            self.reservation
                .take()
                .expect("live S2C admission owns its carrier reservation"),
        )
    }
}

/// Exact session reservation for one directional validation on a carrier that
/// already owns ordinary authority in the other direction.
pub(in crate::runtime) struct ClientTcpRetainedDirectionValidationLease {
    owner: Weak<ClientTcpCarrierService>,
    validation: ClientTcpCarrierRetainedValidationState,
    active: bool,
}

impl ClientTcpRetainedDirectionValidationLease {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime) fn validation_id(&self) -> NonZeroU64 {
        self.validation.validation_id
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime) fn direction(&self) -> PathMetricDirection {
        self.validation.direction
    }

    fn cancel_inner(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        if let Some(owner) = self.owner.upgrade() {
            owner.release_retained_direction_validation(self.validation);
        }
    }
}

impl Drop for ClientTcpRetainedDirectionValidationLease {
    fn drop(&mut self) {
        self.cancel_inner();
    }
}

/// Cold RAII registration for one complete reliable Product workload.
pub(in crate::runtime) struct ClientTcpCarrierWorkloadLease {
    owner: Weak<ClientTcpCarrierService>,
    observations_active: Arc<AtomicBool>,
    identity: ProductWorkloadIdentity,
    lane: TrafficClass,
    queued_unique_original: bool,
    demand_generation: Option<NonZeroU64>,
    successful_placement: Option<ClientTcpCarrierStableGenerations>,
}

impl ClientTcpCarrierWorkloadLease {
    pub(in crate::runtime) fn identity(&self) -> ProductWorkloadIdentity {
        self.identity
    }

    /// Updates the continuous target-demand episode at a sender lifecycle
    /// boundary. Fresh queued work is retained for admission, while a
    /// work-conserving queue drain inside the same throughput classification
    /// preserves the generation. Crossing the classifier's idle boundary and
    /// later returning to throughput creates a new generation.
    pub(in crate::runtime) fn update_demand(
        &mut self,
        lane: TrafficClass,
        queued_unique_original: bool,
    ) -> bool {
        if self.lane == lane && self.queued_unique_original == queued_unique_original {
            return true;
        }
        let Some(owner) = self.owner.upgrade() else {
            return false;
        };
        let Some(demand_generation) =
            owner.update_workload_demand(self.identity, lane, queued_unique_original)
        else {
            return false;
        };
        if self.demand_generation != demand_generation {
            self.successful_placement = None;
        }
        self.lane = lane;
        self.queued_unique_original = queued_unique_original;
        self.demand_generation = demand_generation;
        true
    }

    /// Arms exactly one possible saturation transition after successful fresh
    /// unique-original placement. Repeated successful payloads only overwrite
    /// this lease-local marker and never touch session-shared state.
    pub(in crate::runtime) fn record_successful_ordinary_placement(
        &mut self,
        stable: ClientTcpCarrierStableGenerations,
    ) -> bool {
        if self.demand_generation.is_none() {
            return false;
        }
        self.successful_placement = Some(stable);
        true
    }

    /// Enables exact original-release capture only while one validation owner
    /// is actively accepting bounded evidence. The normal ACK path pays one
    /// inactive atomic read and otherwise retains its allocation-free branch.
    pub(in crate::runtime) fn request_product_ack_receipt_target(
        &mut self,
    ) -> Option<RequestProductAckReceiptTarget<'_>> {
        if !self.observations_active.load(Ordering::Acquire) {
            return None;
        }
        Some(RequestProductAckReceiptTarget {
            identity: self.identity,
            sink: self,
        })
    }

    /// Consumes the exact successful-placement to saturation transition.
    /// Rechecking unchanged saturation cannot retry because the local marker is
    /// removed before any group or geometry admission check.
    pub(in crate::runtime) fn try_admit_saturation(
        &mut self,
        saturation: ClientTcpCarrierSaturation,
        groups: &Arc<ClientTcpCarrierGroups>,
        mux_limits: MuxLimits,
    ) -> Option<ClientTcpCarrierAdmission> {
        let successful_placement = self.successful_placement.take()?;
        let demand_generation = self.demand_generation?;
        self.owner.upgrade()?.try_admit(
            self.identity,
            demand_generation,
            successful_placement,
            saturation,
            groups,
            mux_limits,
        )
    }
}

impl RequestProductAckReceiptSink for ClientTcpCarrierWorkloadLease {
    fn publish_request_product_ack(&mut self, receipt: RequestProductAckReceipt) {
        if receipt.identity != self.identity {
            return;
        }
        if let Some(owner) = self.owner.upgrade() {
            owner.publish_observation(ClientTcpCarrierObservation::ProductAck(receipt));
        }
    }
}

impl Drop for ClientTcpCarrierWorkloadLease {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            owner.unregister_workload(self.identity);
        }
    }
}

/// Exact non-clone reservation and frozen comparison input for one candidate.
///
/// This value owns the physical group reservation from connect admission until
/// an acknowledged retain explicitly transfers it. Every other exit drops the
/// reservation and clears the session's one-candidate ownership.
pub(in crate::runtime) struct ClientTcpCarrierAdmission {
    lease: Option<ClientTcpCarrierAdmissionLease>,
    reservation: Option<ClientTcpCarrierReservation>,
}

/// Service ownership that accompanies the physical reservation inside the
/// validation actor. It is split only by `into_validation_parts`, so carrier
/// I/O can own the reservation while this non-clone lease continues to enforce
/// the one-candidate and one-validation session invariants.
pub(in crate::runtime) struct ClientTcpCarrierAdmissionLease {
    owner: Weak<ClientTcpCarrierService>,
    validation_id: NonZeroU64,
    admission_generation: NonZeroU64,
    target: ProductWorkloadIdentity,
    stable: ClientTcpCarrierStableGenerations,
    workloads: Box<[ProductWorkloadIdentity]>,
    ordinary_services: Box<[ClientTcpCarrierOrdinaryService]>,
    geometry: TcpCarrierValidationGeometry,
    config_index: usize,
    endpoint_policy: Arc<ClientTcpEndpointPolicy>,
    endpoint_snapshot: ClientTcpEndpointPolicySnapshot,
    armed: bool,
}

/// Exact adapter consumed by `ClientTcpValidationAdmission`.
///
/// The two fields have distinct single owners: the actor carries the service
/// admission lease through result settlement and independently carries the
/// physical reservation through ordered drain or retained handoff.
pub(in crate::runtime) struct ClientTcpCarrierValidationParts {
    pub(in crate::runtime) admission: ClientTcpCarrierAdmissionLease,
    pub(in crate::runtime) reservation: ClientTcpCarrierReservation,
}

impl ClientTcpCarrierAdmissionLease {
    pub(in crate::runtime) fn validation_id(&self) -> NonZeroU64 {
        self.validation_id
    }

    pub(in crate::runtime) fn admission_generation(&self) -> NonZeroU64 {
        self.admission_generation
    }

    pub(in crate::runtime) fn target(&self) -> ProductWorkloadIdentity {
        self.target
    }

    pub(in crate::runtime) fn stable(&self) -> ClientTcpCarrierStableGenerations {
        self.stable
    }

    pub(in crate::runtime) fn workloads(&self) -> &[ProductWorkloadIdentity] {
        &self.workloads
    }

    pub(in crate::runtime) fn ordinary_services(&self) -> &[ClientTcpCarrierOrdinaryService] {
        &self.ordinary_services
    }

    pub(in crate::runtime) fn geometry(&self) -> TcpCarrierValidationGeometry {
        self.geometry
    }

    pub(in crate::runtime) fn config_index(&self) -> usize {
        self.config_index
    }

    pub(in crate::runtime) fn endpoint_generation(&self) -> u64 {
        self.endpoint_snapshot.generation
    }

    pub(in crate::runtime) fn is_withdrawn(&self) -> bool {
        !self
            .endpoint_policy
            .allows(self.endpoint_snapshot.generation)
            || self.owner.upgrade().is_none_or(|owner| {
                owner.candidate_is_withdrawn(self.validation_id, self.admission_generation)
            })
    }

    /// Rechecks the exact current ordinary membership and all service-owned
    /// frozen inputs. Mutable queue, credit, and transport evidence are not
    /// comparison-key members and therefore are not accepted here.
    #[cfg(test)]
    pub(in crate::runtime) fn revalidate(
        &self,
        stable: ClientTcpCarrierStableGenerations,
        ordinary_instances: &[RelayPathInstance],
    ) -> bool {
        if !self
            .endpoint_policy
            .allows(self.endpoint_snapshot.generation)
        {
            if let Some(owner) = self.owner.upgrade() {
                let _ = owner.revalidate_candidate(
                    self.validation_id,
                    self.admission_generation,
                    stable,
                    &[],
                );
            }
            return false;
        }
        self.owner.upgrade().is_some_and(|owner| {
            owner.revalidate_candidate(
                self.validation_id,
                self.admission_generation,
                stable,
                ordinary_instances,
            )
        })
    }

    /// Marks the point at which candidate readiness and exact validation
    /// admission begin the sole active directional measurement.
    pub(in crate::runtime) fn begin_validation(&mut self) -> bool {
        if self.is_withdrawn() {
            return false;
        }
        self.owner.upgrade().is_some_and(|owner| {
            owner.begin_validation(self.validation_id, self.admission_generation)
        })
    }

    /// Commits the service transition only after the actor has processed the
    /// exact acknowledged `RETAIN`. The actor still owns the one physical
    /// reservation and may then move it into the retained handoff.
    pub(in crate::runtime) fn commit_retained(mut self) -> bool {
        if self.is_withdrawn() {
            return false;
        }
        let Some(owner) = self.owner.upgrade() else {
            return false;
        };
        let validating = owner
            .state
            .lock()
            .expect("TCP carrier service lock")
            .validation_transaction
            .as_ref()
            .and_then(ClientTcpCarrierValidationTransaction::client_to_server_candidate)
            .is_some_and(|candidate| {
                candidate.validation_id == self.validation_id
                    && candidate.admission_generation == self.admission_generation
                    && candidate.phase == ClientTcpCarrierCandidatePhase::Validating
                    && !candidate.withdrawn
            });
        if !validating {
            return false;
        }
        owner.release_candidate(self.validation_id, self.admission_generation);
        self.armed = false;
        true
    }

    fn cancel_inner(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        if let Some(owner) = self.owner.upgrade() {
            owner.release_candidate(self.validation_id, self.admission_generation);
        }
    }
}

impl Drop for ClientTcpCarrierAdmissionLease {
    fn drop(&mut self) {
        self.cancel_inner();
    }
}

impl ClientTcpCarrierAdmission {
    fn lease(&self) -> &ClientTcpCarrierAdmissionLease {
        self.lease
            .as_ref()
            .expect("live admission owns its service lease")
    }

    #[cfg(test)]
    fn lease_mut(&mut self) -> &mut ClientTcpCarrierAdmissionLease {
        self.lease
            .as_mut()
            .expect("live admission owns its service lease")
    }

    pub(in crate::runtime) fn validation_id(&self) -> NonZeroU64 {
        self.lease().validation_id()
    }

    pub(in crate::runtime) fn admission_generation(&self) -> NonZeroU64 {
        self.lease().admission_generation()
    }

    pub(in crate::runtime) fn target(&self) -> ProductWorkloadIdentity {
        self.lease().target()
    }

    pub(in crate::runtime) fn stable(&self) -> ClientTcpCarrierStableGenerations {
        self.lease().stable()
    }

    pub(in crate::runtime) fn workloads(&self) -> &[ProductWorkloadIdentity] {
        self.lease().workloads()
    }

    pub(in crate::runtime) fn ordinary_services(&self) -> &[ClientTcpCarrierOrdinaryService] {
        self.lease().ordinary_services()
    }

    pub(in crate::runtime) fn geometry(&self) -> TcpCarrierValidationGeometry {
        self.lease().geometry()
    }

    pub(in crate::runtime) fn config_index(&self) -> usize {
        self.lease().config_index()
    }

    #[cfg(test)]
    pub(in crate::runtime) fn path_id(&self) -> PathId {
        self.reservation
            .as_ref()
            .expect("live admission owns its carrier reservation")
            .path_id()
    }

    #[cfg(test)]
    pub(in crate::runtime) fn is_withdrawn(&self) -> bool {
        self.lease().is_withdrawn()
    }

    #[cfg(test)]
    pub(in crate::runtime) fn revalidate(
        &self,
        stable: ClientTcpCarrierStableGenerations,
        ordinary_instances: &[RelayPathInstance],
    ) -> bool {
        self.lease().revalidate(stable, ordinary_instances)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn begin_validation(&mut self) -> bool {
        self.lease_mut().begin_validation()
    }

    /// Splits the two exact ownership domains for the validation actor without
    /// cloning either. Dropping either field performs its own required release.
    pub(in crate::runtime) fn into_validation_parts(mut self) -> ClientTcpCarrierValidationParts {
        ClientTcpCarrierValidationParts {
            admission: self
                .lease
                .take()
                .expect("live admission owns its service lease"),
            reservation: self
                .reservation
                .take()
                .expect("live admission owns its carrier reservation"),
        }
    }
}

impl ClientPathContext {
    pub(in crate::runtime) fn subscribe_server_tcp_carrier_demands(
        &self,
    ) -> watch::Receiver<Option<ClientTcpCarrierDemand>> {
        self.state.tcp_carrier_service().subscribe_server_demands()
    }

    pub(in crate::runtime) fn claim_server_to_client_tcp_carrier(
        &self,
        demand: ClientTcpCarrierDemand,
    ) -> Option<ClientTcpServerToClientAdmission> {
        self.state
            .tcp_carrier_service()
            .try_claim_server_to_client_demand(demand, &self.tcp_carrier_groups)
    }

    pub(in crate::runtime) fn register_tcp_carrier_workload(
        &self,
        stream_id: StreamId,
    ) -> Option<ClientTcpCarrierWorkloadLease> {
        self.state
            .tcp_carrier_service()
            .register_workload(stream_id)
    }

    pub(in crate::runtime) fn activate_tcp_carrier_observations(
        &self,
        validation_id: NonZeroU64,
        admission_generation: NonZeroU64,
        capacity: usize,
    ) -> Option<mpsc::Receiver<ClientTcpCarrierObservation>> {
        self.state.tcp_carrier_service().activate_observations(
            validation_id,
            admission_generation,
            capacity,
        )
    }

    pub(in crate::runtime) fn subscribe_tcp_carrier_policy_epochs(
        &self,
    ) -> watch::Receiver<Option<ClientTcpCarrierPolicyEpochs>> {
        self.state.tcp_carrier_service().subscribe_policy_epochs()
    }

    pub(in crate::runtime) fn revalidate_tcp_carrier_candidate(
        &self,
        validation_id: NonZeroU64,
        admission_generation: NonZeroU64,
        stable: ClientTcpCarrierStableGenerations,
        ordinary_instances: &[RelayPathInstance],
    ) -> bool {
        self.state.tcp_carrier_service().revalidate_candidate(
            validation_id,
            admission_generation,
            stable,
            ordinary_instances,
        )
    }
}

#[cfg(test)]
#[path = "tests_service.rs"]
mod tests;
