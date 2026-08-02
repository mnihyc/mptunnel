//! Session-owned server-to-client TCP carrier demand and validation admission.
//!
//! Response senders report only logical workload transitions, successful
//! ordinary placement, exact ordinary saturation, and fully applied Product
//! Data ACK release. This owner serializes the RFC 7.2/15.1 demand and
//! validation transaction without establishing a carrier, choosing a path, or
//! changing transport policy.

use crate::model::path::{CarrierPathInstanceId, CarrierPathKey};
use crate::model::tcp_carrier::{
    TcpCarrierStableGenerations, TcpCarrierValidationGeometry, tcp_carrier_validation_geometry,
};
use crate::mux::MuxLimits;
use crate::protocol::{Frame, StreamId, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::ReliablePathCommandSender;
use crate::runtime::sender::ProductWorkloadIdentity;
use crate::runtime::stream::response::{
    ResponseProductAckOriginalRelease, ResponseProductAckOriginalResolution,
    ResponseSenderPathObservation, ServerTcpValidationOutput,
};
use crate::scheduler::{TrafficClass, path_is_backup};
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{mpsc, oneshot, watch};

pub(in crate::runtime::path::tcp) enum ServerTcpValidationControl {
    SerializeResult {
        result: crate::protocol::TcpCarrierValidationResult,
        response: oneshot::Sender<Result<(), RuntimeError>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ServerTcpValidationEvent {
    ResultAcknowledged(crate::protocol::TcpCarrierValidationResult),
    Retained,
}

/// Result and writer-boundary authority for one server-owned S2C carrier
/// validation. The carrier actor remains the sole wire writer.
#[derive(Clone)]
pub(in crate::runtime) struct ServerTcpValidationController {
    pub(in crate::runtime::path::tcp) controls: mpsc::Sender<ServerTcpValidationControl>,
    validation_data: ReliablePathCommandSender,
    validation_id: NonZeroU64,
}

impl ServerTcpValidationController {
    pub(in crate::runtime::path::tcp) fn new(
        controls: mpsc::Sender<ServerTcpValidationControl>,
        validation_data: ReliablePathCommandSender,
        validation_id: NonZeroU64,
    ) -> Self {
        Self {
            controls,
            validation_data,
            validation_id,
        }
    }

    pub(in crate::runtime) async fn writer_boundary(&self) -> Result<Instant, RuntimeError> {
        self.validation_data
            .tcp_carrier_validation_writer_boundary(self.validation_id)
            .await
    }

    pub(in crate::runtime) fn pending_bytes(&self) -> u64 {
        self.validation_data
            .pending_bytes()
            .saturating_add(self.validation_data.writer_pending_bytes())
    }

    pub(in crate::runtime) async fn serialize_result(
        &self,
        result: crate::protocol::TcpCarrierValidationResult,
    ) -> Result<(), RuntimeError> {
        let (response, receipt) = oneshot::channel();
        self.controls
            .send(ServerTcpValidationControl::SerializeResult { result, response })
            .await
            .map_err(|_| RuntimeError::ReliablePathSessionClosed)?;
        receipt
            .await
            .map_err(|_| RuntimeError::ReliablePathSessionClosed)?
    }
}

pub(in crate::runtime) struct ServerTcpCarrierValidationOffer {
    pub(in crate::runtime) admission: ServerTcpCarrierValidationAdmission,
    pub(in crate::runtime) output: ServerTcpValidationOutput,
    pub(in crate::runtime) controller: ServerTcpValidationController,
    pub(in crate::runtime) events: mpsc::Receiver<ServerTcpValidationEvent>,
}

/// Exact response-output lifetime used by one S2C comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ServerTcpCarrierOutputInstance {
    pub(in crate::runtime) key: CarrierPathKey,
    pub(in crate::runtime) path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime) output_incarnation: u64,
}

/// One frozen ordinary response output and its established two-BDP service
/// pipe, already rounded and floored by the regular scheduling model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ServerTcpCarrierOrdinaryService {
    pub(in crate::runtime) instance: ServerTcpCarrierOutputInstance,
    pub(in crate::runtime) service_pipe_bytes: u64,
}

/// Exact ordinary-saturation facts supplied by the serialized response sender.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) struct ServerTcpCarrierSaturation {
    pub(in crate::runtime) stable: TcpCarrierStableGenerations,
    pub(in crate::runtime) ordinary_services: Box<[ServerTcpCarrierOrdinaryService]>,
}

/// One monotonic session demand publication. A missing stream withdraws the
/// previously current request; it is not an absence of publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ServerTcpCarrierDemand {
    pub(in crate::runtime) request_id: NonZeroU64,
    pub(in crate::runtime) stream_id: Option<StreamId>,
}

impl ServerTcpCarrierDemand {
    pub(in crate::runtime) fn into_frame(self) -> Frame {
        Frame::TcpCarrierDemand {
            request_id: self.request_id.get(),
            stream_id: self.stream_id,
        }
    }
}

/// One indivisible, fully applied response Product Data-ACK transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) struct ResponseProductAckReceipt {
    pub(in crate::runtime) identity: ProductWorkloadIdentity,
    pub(in crate::runtime) completed_at: Instant,
    pub(in crate::runtime) original_releases: SmallVec<[ResponseProductAckOriginalRelease; 4]>,
}

pub(in crate::runtime) struct ResponseProductAckReceiptTarget<'a> {
    pub(in crate::runtime) identity: ProductWorkloadIdentity,
    validation: ServerTcpCarrierValidationIdentity,
    workload: &'a mut ServerTcpCarrierWorkloadLease,
}

impl ResponseProductAckReceiptTarget<'_> {
    pub(in crate::runtime) fn publish(self, receipt: ResponseProductAckReceipt) {
        if receipt.identity != self.identity {
            return;
        }
        self.workload.owner.publish_observation(
            self.validation,
            ServerTcpCarrierObservation::ProductAck(receipt),
        );
    }
}

/// Exact sender facts routed to the one active S2C validation owner. No
/// observation is captured or allocated while validation is inactive.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug)]
pub(in crate::runtime) enum ServerTcpCarrierObservation {
    ProductAck(ResponseProductAckReceipt),
}

#[derive(Debug)]
struct ServerTcpCarrierWorkload {
    identity: ProductWorkloadIdentity,
    lane: TrafficClass,
    queued_unique_original: bool,
    demand_generation: Option<NonZeroU64>,
    last_issued_demand_key: Option<ServerTcpCarrierDemandKey>,
    validation_offers: mpsc::Sender<ServerTcpCarrierValidationOffer>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ServerTcpCarrierDemandKey {
    target: ProductWorkloadIdentity,
    demand_generation: NonZeroU64,
    workload_generation: NonZeroU64,
    stable: TcpCarrierStableGenerations,
    ordinary_instances: Box<[ServerTcpCarrierOutputInstance]>,
}

#[derive(Debug)]
struct ServerTcpCarrierDemandState {
    #[cfg_attr(not(test), allow(dead_code))]
    publication: ServerTcpCarrierDemand,
    key: ServerTcpCarrierDemandKey,
    #[cfg_attr(not(test), allow(dead_code))]
    ordinary_services: Box<[ServerTcpCarrierOrdinaryService]>,
    #[cfg_attr(not(test), allow(dead_code))]
    geometry: TcpCarrierValidationGeometry,
    #[cfg_attr(not(test), allow(dead_code))]
    validation_started: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ServerTcpCarrierValidationIdentity {
    request_id: NonZeroU64,
    validation_id: NonZeroU64,
    candidate: ServerTcpCarrierOutputInstance,
}

#[derive(Debug)]
struct ServerTcpCarrierValidationState {
    identity: ServerTcpCarrierValidationIdentity,
    withdrawn: bool,
    observations: Option<mpsc::Sender<ServerTcpCarrierObservation>>,
}

#[derive(Debug)]
struct ServerTcpCarrierServiceState {
    next_workload_generation: Option<NonZeroU64>,
    next_demand_generation: Option<NonZeroU64>,
    next_request_id: Option<NonZeroU64>,
    workload_generation: NonZeroU64,
    workloads: BTreeMap<StreamId, ServerTcpCarrierWorkload>,
    realtime_workloads: usize,
    demand: Option<ServerTcpCarrierDemandState>,
    validation: Option<ServerTcpCarrierValidationState>,
}

/// One S2C demand and validation owner per server MPP session.
#[derive(Debug)]
pub(in crate::runtime) struct ServerTcpCarrierService {
    state: Mutex<ServerTcpCarrierServiceState>,
    demand_changes: watch::Sender<Option<ServerTcpCarrierDemand>>,
    observations_active: Arc<AtomicBool>,
}

/// Keeps one server-session demand owner alive for a ready TCP carrier while
/// exposing only its monotonic publications to the carrier actor.
pub(in crate::runtime) struct ServerTcpCarrierDemandSubscription {
    _owner: Arc<ServerTcpCarrierService>,
    changes: watch::Receiver<Option<ServerTcpCarrierDemand>>,
}

impl ServerTcpCarrierDemandSubscription {
    pub(in crate::runtime) fn current(&mut self) -> Option<ServerTcpCarrierDemand> {
        *self.changes.borrow_and_update()
    }

    pub(in crate::runtime) async fn changed(&mut self) -> Option<ServerTcpCarrierDemand> {
        self.changes.changed().await.ok()?;
        *self.changes.borrow_and_update()
    }

    pub(in crate::runtime) fn admit_validation_from_observation(
        &self,
        request_id: NonZeroU64,
        validation_id: NonZeroU64,
        stream_id: StreamId,
        output: &ServerTcpValidationOutput,
    ) -> Option<ServerTcpCarrierValidationAdmission> {
        self._owner
            .admit_validation_from_observation(request_id, validation_id, stream_id, output)
    }

    pub(in crate::runtime) fn publish_validation_offer(
        &self,
        offer: ServerTcpCarrierValidationOffer,
    ) -> Option<ServerTcpCarrierValidationOffer> {
        self._owner.publish_validation_offer(offer)
    }
}

impl ServerTcpCarrierService {
    pub(in crate::runtime) fn new() -> Arc<Self> {
        let one = NonZeroU64::new(1).expect("one is nonzero");
        let (demand_changes, _) = watch::channel(None);
        Arc::new(Self {
            state: Mutex::new(ServerTcpCarrierServiceState {
                next_workload_generation: Some(one),
                next_demand_generation: Some(one),
                next_request_id: Some(one),
                workload_generation: one,
                workloads: BTreeMap::new(),
                realtime_workloads: 0,
                demand: None,
                validation: None,
            }),
            demand_changes,
            observations_active: Arc::new(AtomicBool::new(false)),
        })
    }

    pub(in crate::runtime) fn subscribe_demands(
        self: &Arc<Self>,
    ) -> ServerTcpCarrierDemandSubscription {
        ServerTcpCarrierDemandSubscription {
            _owner: self.clone(),
            changes: self.demand_changes.subscribe(),
        }
    }

    fn admit_validation_from_observation(
        self: &Arc<Self>,
        request_id: NonZeroU64,
        validation_id: NonZeroU64,
        stream_id: StreamId,
        output: &ServerTcpValidationOutput,
    ) -> Option<ServerTcpCarrierValidationAdmission> {
        let authority_class = {
            let state = self.state.lock().expect("server TCP carrier service lock");
            let demand = state.demand.as_ref()?;
            if demand.publication.request_id != request_id
                || demand.publication.stream_id != Some(stream_id)
            {
                return None;
            }
            demand.key.stable.authority_class
        };
        let observation: ResponseSenderPathObservation = output.sender_path_observation();
        let (stable, ordinary_instances) =
            server_tcp_carrier_comparison_key(&observation, authority_class)?;
        let identity = output.identity();
        self.admit_validation(
            request_id,
            validation_id,
            stream_id,
            ServerTcpCarrierOutputInstance {
                key: identity.key,
                path_instance_id: identity.path_instance_id,
                output_incarnation: identity.incarnation,
            },
            stable,
            &ordinary_instances,
        )
    }

    fn publish_validation_offer(
        &self,
        offer: ServerTcpCarrierValidationOffer,
    ) -> Option<ServerTcpCarrierValidationOffer> {
        let target = offer.admission.target();
        let sender = {
            let state = self.state.lock().expect("server TCP carrier service lock");
            let Some(workload) = state.workloads.get(&target.stream_id) else {
                return Some(offer);
            };
            if workload.identity != target
                || state.validation.as_ref().is_none_or(|validation| {
                    validation.identity != offer.admission.validation || validation.withdrawn
                })
            {
                return Some(offer);
            }
            workload.validation_offers.clone()
        };
        sender.try_send(offer).err().map(|error| error.into_inner())
    }

    fn revalidate_from_observation(
        &self,
        identity: ServerTcpCarrierValidationIdentity,
        authority_class: crate::protocol::PathUsage,
        output: &ServerTcpValidationOutput,
    ) -> bool {
        let observation = output.sender_path_observation();
        let Some((stable, ordinary_instances)) =
            server_tcp_carrier_comparison_key(&observation, authority_class)
        else {
            let mut state = self.state.lock().expect("server TCP carrier service lock");
            if state
                .validation
                .as_ref()
                .is_some_and(|validation| validation.identity == identity)
            {
                withdraw_validation(&mut state, &self.observations_active);
            }
            return false;
        };
        self.revalidate(identity, stable, &ordinary_instances)
    }

    pub(in crate::runtime) fn register_workload(
        self: &Arc<Self>,
        stream_id: StreamId,
    ) -> Option<ServerTcpCarrierWorkloadLease> {
        let mut state = self.state.lock().expect("server TCP carrier service lock");
        if state.workloads.contains_key(&stream_id) {
            return None;
        }
        let identity = ProductWorkloadIdentity {
            stream_id,
            lifecycle_generation: take_sequence(&mut state.next_workload_generation)?,
        };
        state.workload_generation = increment_generation(state.workload_generation)?;
        let (validation_offers, validation_offer_receiver) = mpsc::channel(1);
        state.workloads.insert(
            stream_id,
            ServerTcpCarrierWorkload {
                identity,
                lane: TrafficClass::Latency,
                queued_unique_original: false,
                demand_generation: None,
                last_issued_demand_key: None,
                validation_offers,
            },
        );
        let withdrawal = withdraw_demand(&mut state, &self.observations_active);
        if withdrawal.is_some() {
            self.demand_changes.send_replace(withdrawal);
        }
        Some(ServerTcpCarrierWorkloadLease {
            owner: self.clone(),
            identity,
            lane: TrafficClass::Latency,
            queued_unique_original: false,
            demand_generation: None,
            successful_placement: None,
            validation_offers: Some(validation_offer_receiver),
        })
    }

    pub(in crate::runtime) fn register_realtime_workload(
        self: &Arc<Self>,
    ) -> ServerTcpCarrierRealtimeWorkloadLease {
        let mut state = self.state.lock().expect("server TCP carrier service lock");
        state.realtime_workloads = state
            .realtime_workloads
            .checked_add(1)
            .expect("realtime workloads stay inside the session resource envelope");
        if let Some(next) = increment_generation(state.workload_generation) {
            state.workload_generation = next;
        } else {
            state.next_request_id = None;
        }
        let withdrawal = withdraw_demand(&mut state, &self.observations_active);
        if withdrawal.is_some() {
            self.demand_changes.send_replace(withdrawal);
        }
        ServerTcpCarrierRealtimeWorkloadLease {
            owner: self.clone(),
            active: true,
        }
    }

    fn update_workload_demand(
        &self,
        identity: ProductWorkloadIdentity,
        lane: TrafficClass,
        queued_unique_original: bool,
    ) -> Option<Option<NonZeroU64>> {
        let mut state = self.state.lock().expect("server TCP carrier service lock");
        let (current_identity, current_lane, current_queued, current_demand_generation) = {
            let current = state.workloads.get(&identity.stream_id)?;
            (
                current.identity,
                current.lane,
                current.queued_unique_original,
                current.demand_generation,
            )
        };
        if current_identity != identity {
            return None;
        }
        // The classifier owns the continuous demand episode. A bounded sender
        // queue may momentarily drain because ordinary placement succeeded;
        // that is not an idle transition and must not mint a new generation.
        let throughput_demand = lane == TrafficClass::Throughput;
        let demand_generation = match (current_demand_generation, throughput_demand) {
            (Some(generation), true) => Some(generation),
            (None, true) => Some(take_sequence(&mut state.next_demand_generation)?),
            (_, false) => None,
        };
        if current_lane == lane
            && current_queued == queued_unique_original
            && current_demand_generation == demand_generation
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

        let current_target_invalid = state.demand.as_ref().is_some_and(|demand| {
            demand.key.target == identity && Some(demand.key.demand_generation) != demand_generation
        });
        let latency_work_became_active = queued_unique_original && lane.is_latency_sensitive();
        let withdrawal = (current_target_invalid || latency_work_became_active)
            .then(|| withdraw_demand(&mut state, &self.observations_active))
            .flatten();
        if withdrawal.is_some() {
            self.demand_changes.send_replace(withdrawal);
        }
        Some(demand_generation)
    }

    fn unregister_workload(&self, identity: ProductWorkloadIdentity) {
        let mut state = self.state.lock().expect("server TCP carrier service lock");
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
            state.next_request_id = None;
        }
        let withdrawal = withdraw_demand(&mut state, &self.observations_active);
        if withdrawal.is_some() {
            self.demand_changes.send_replace(withdrawal);
        }
    }

    fn unregister_realtime_workload(&self) {
        let mut state = self.state.lock().expect("server TCP carrier service lock");
        if state.realtime_workloads == 0 {
            return;
        }
        state.realtime_workloads -= 1;
        if let Some(next) = increment_generation(state.workload_generation) {
            state.workload_generation = next;
        } else {
            state.next_request_id = None;
        }
        let withdrawal = withdraw_demand(&mut state, &self.observations_active);
        if withdrawal.is_some() {
            self.demand_changes.send_replace(withdrawal);
        }
    }

    fn try_issue_demand(
        &self,
        identity: ProductWorkloadIdentity,
        demand_generation: NonZeroU64,
        successful_placement: TcpCarrierStableGenerations,
        saturation: ServerTcpCarrierSaturation,
        mux_limits: MuxLimits,
    ) -> Option<ServerTcpCarrierDemand> {
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
        ordinary_services.sort_unstable_by_key(|ordinary| output_instance_order(ordinary.instance));
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

        let mut state = self.state.lock().expect("server TCP carrier service lock");
        if state.validation.is_some() {
            return None;
        }
        let target = state.workloads.get(&identity.stream_id)?;
        if target.identity != identity
            || target.demand_generation != Some(demand_generation)
            || target.lane != TrafficClass::Throughput
            || state.realtime_workloads != 0
            || state.workloads.values().any(|workload| {
                workload.queued_unique_original && workload.lane.is_latency_sensitive()
            })
        {
            return None;
        }
        let key = ServerTcpCarrierDemandKey {
            target: identity,
            demand_generation,
            workload_generation: state.workload_generation,
            stable: saturation.stable,
            ordinary_instances,
        };
        // Membership generation governs the exact ordinary set. A changed set
        // under the same frozen generations is inconsistent sender input, not
        // authority to supersede the current request.
        if state.demand.as_ref().is_some_and(|demand| {
            demand.key.target == key.target
                && demand.key.demand_generation == key.demand_generation
                && demand.key.workload_generation == key.workload_generation
                && demand.key.stable == key.stable
                && demand.key.ordinary_instances != key.ordinary_instances
        }) {
            return None;
        }
        if state
            .demand
            .as_ref()
            .is_some_and(|demand| demand.key == key)
        {
            return None;
        }
        if state
            .demand
            .as_ref()
            .is_some_and(|demand| !demand.validation_started && demand.key.target != identity)
        {
            return None;
        }
        if state
            .workloads
            .get(&identity.stream_id)
            .and_then(|workload| workload.last_issued_demand_key.as_ref())
            == Some(&key)
        {
            return None;
        }
        let request_id = take_sequence(&mut state.next_request_id)?;
        let publication = ServerTcpCarrierDemand {
            request_id,
            stream_id: Some(identity.stream_id),
        };
        state
            .workloads
            .get_mut(&identity.stream_id)
            .expect("validated target remains registered")
            .last_issued_demand_key = Some(key.clone());
        state.demand = Some(ServerTcpCarrierDemandState {
            publication,
            key,
            ordinary_services: ordinary_services.into_boxed_slice(),
            geometry,
            validation_started: false,
        });
        self.demand_changes.send_replace(Some(publication));
        Some(publication)
    }

    /// Admits one exact client validation only for the current demand and the
    /// unchanged sender-owned comparison key.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime) fn admit_validation(
        self: &Arc<Self>,
        request_id: NonZeroU64,
        validation_id: NonZeroU64,
        stream_id: StreamId,
        candidate: ServerTcpCarrierOutputInstance,
        stable: TcpCarrierStableGenerations,
        ordinary_instances: &[ServerTcpCarrierOutputInstance],
    ) -> Option<ServerTcpCarrierValidationAdmission> {
        if candidate.key.underlay != UnderlayProtocol::Tcp || candidate.output_incarnation == 0 {
            return None;
        }
        let mut canonical_instances = ordinary_instances.to_vec();
        canonical_instances.sort_unstable_by_key(|instance| output_instance_order(*instance));
        if canonical_instances
            .windows(2)
            .any(|pair| pair[0] == pair[1])
            || canonical_instances.contains(&candidate)
        {
            return None;
        }

        let mut state = self.state.lock().expect("server TCP carrier service lock");
        if state.validation.is_some() {
            return None;
        }
        let (target, frozen_stable, ordinary_services, geometry) = {
            let demand = state.demand.as_ref()?;
            if demand.publication.request_id != request_id
                || demand.publication.stream_id != Some(stream_id)
                || demand.validation_started
                || demand.key.stable != stable
                || demand.key.ordinary_instances.as_ref() != canonical_instances.as_slice()
                || demand.key.workload_generation != state.workload_generation
            {
                return None;
            }
            let target = state.workloads.get(&stream_id)?;
            if target.identity != demand.key.target
                || target.demand_generation != Some(demand.key.demand_generation)
                || target.lane != TrafficClass::Throughput
                || state.realtime_workloads != 0
                || state.workloads.values().any(|workload| {
                    workload.queued_unique_original && workload.lane.is_latency_sensitive()
                })
            {
                return None;
            }
            (
                demand.key.target,
                demand.key.stable,
                demand.ordinary_services.clone(),
                demand.geometry,
            )
        };
        let workloads = state
            .workloads
            .values()
            .map(|workload| workload.identity)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let validation = ServerTcpCarrierValidationIdentity {
            request_id,
            validation_id,
            candidate,
        };
        let demand = state
            .demand
            .as_mut()
            .expect("validated demand remains current");
        demand.validation_started = true;
        let admission = ServerTcpCarrierValidationAdmission {
            owner: self.clone(),
            validation,
            target,
            stable: frozen_stable,
            workloads,
            ordinary_services,
            geometry,
            armed: true,
        };
        state.validation = Some(ServerTcpCarrierValidationState {
            identity: validation,
            withdrawn: false,
            observations: None,
        });
        Some(admission)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn validation_is_withdrawn(&self, identity: ServerTcpCarrierValidationIdentity) -> bool {
        self.state
            .lock()
            .expect("server TCP carrier service lock")
            .validation
            .as_ref()
            .is_none_or(|validation| validation.identity != identity || validation.withdrawn)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn activate_observations(
        &self,
        identity: ServerTcpCarrierValidationIdentity,
        capacity: usize,
    ) -> Option<mpsc::Receiver<ServerTcpCarrierObservation>> {
        let mut state = self.state.lock().expect("server TCP carrier service lock");
        let validation = state.validation.as_mut()?;
        if validation.identity != identity
            || validation.withdrawn
            || validation.observations.is_some()
        {
            return None;
        }
        let (observations, receiver) = mpsc::channel(capacity.max(1));
        validation.observations = Some(observations);
        self.observations_active.store(true, Ordering::Release);
        Some(receiver)
    }

    fn active_observation_validation(&self) -> Option<ServerTcpCarrierValidationIdentity> {
        if !self.observations_active.load(Ordering::Acquire) {
            return None;
        }
        self.state
            .lock()
            .expect("server TCP carrier service lock")
            .validation
            .as_ref()
            .filter(|validation| !validation.withdrawn && validation.observations.is_some())
            .map(|validation| validation.identity)
    }

    fn publish_observation(
        &self,
        identity: ServerTcpCarrierValidationIdentity,
        observation: ServerTcpCarrierObservation,
    ) {
        if !self.observations_active.load(Ordering::Acquire) {
            return;
        }
        let mut state = self.state.lock().expect("server TCP carrier service lock");
        let Some(validation) = state.validation.as_mut() else {
            self.observations_active.store(false, Ordering::Release);
            return;
        };
        if validation.identity != identity {
            return;
        }
        if validation.withdrawn {
            validation.observations = None;
            self.observations_active.store(false, Ordering::Release);
            return;
        }
        if validation
            .observations
            .as_ref()
            .is_none_or(|sender| sender.try_send(observation).is_err())
        {
            validation.withdrawn = true;
            validation.observations = None;
            self.observations_active.store(false, Ordering::Release);
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn revalidate(
        &self,
        identity: ServerTcpCarrierValidationIdentity,
        stable: TcpCarrierStableGenerations,
        ordinary_instances: &[ServerTcpCarrierOutputInstance],
    ) -> bool {
        let mut canonical_instances = ordinary_instances.to_vec();
        canonical_instances.sort_unstable_by_key(|instance| output_instance_order(*instance));
        let mut state = self.state.lock().expect("server TCP carrier service lock");
        let exact_validation = state
            .validation
            .as_ref()
            .is_some_and(|validation| validation.identity == identity && !validation.withdrawn);
        let exact_key = state.demand.as_ref().is_some_and(|demand| {
            demand.publication.request_id == identity.request_id
                && demand.key.stable == stable
                && demand.key.workload_generation == state.workload_generation
                && demand.key.ordinary_instances.as_ref() == canonical_instances.as_slice()
                && state
                    .workloads
                    .get(&demand.key.target.stream_id)
                    .is_some_and(|target| {
                        target.identity == demand.key.target
                            && target.demand_generation == Some(demand.key.demand_generation)
                            && target.lane == TrafficClass::Throughput
                    })
                && state.realtime_workloads == 0
                && !state.workloads.values().any(|workload| {
                    workload.queued_unique_original && workload.lane.is_latency_sensitive()
                })
        });
        if exact_validation && exact_key {
            true
        } else {
            withdraw_validation(&mut state, &self.observations_active);
            false
        }
    }

    fn release_validation(&self, identity: ServerTcpCarrierValidationIdentity) {
        let mut state = self.state.lock().expect("server TCP carrier service lock");
        if state
            .validation
            .as_ref()
            .is_some_and(|validation| validation.identity == identity)
        {
            state.validation = None;
            self.observations_active.store(false, Ordering::Release);
        }
    }
}

fn output_instance_order(
    instance: ServerTcpCarrierOutputInstance,
) -> (UnderlayProtocol, u64, u64, u64) {
    (
        instance.key.underlay,
        u64::from(instance.key.path_id.0),
        instance.path_instance_id.as_u64(),
        instance.output_incarnation,
    )
}

fn take_sequence(sequence: &mut Option<NonZeroU64>) -> Option<NonZeroU64> {
    let current = (*sequence)?;
    *sequence = current.get().checked_add(1).and_then(NonZeroU64::new);
    Some(current)
}

fn increment_generation(current: NonZeroU64) -> Option<NonZeroU64> {
    current.get().checked_add(1).and_then(NonZeroU64::new)
}

fn withdraw_validation(state: &mut ServerTcpCarrierServiceState, observations_active: &AtomicBool) {
    if let Some(validation) = state.validation.as_mut() {
        validation.withdrawn = true;
        validation.observations = None;
        observations_active.store(false, Ordering::Release);
    }
}

fn withdraw_demand(
    state: &mut ServerTcpCarrierServiceState,
    observations_active: &AtomicBool,
) -> Option<ServerTcpCarrierDemand> {
    state.demand.take()?;
    withdraw_validation(state, observations_active);
    Some(ServerTcpCarrierDemand {
        request_id: take_sequence(&mut state.next_request_id)?,
        stream_id: None,
    })
}

/// Keeps one active response-direction realtime Product workload inside the
/// same session comparison boundary as reliable response streams.
pub(in crate::runtime) struct ServerTcpCarrierRealtimeWorkloadLease {
    owner: Arc<ServerTcpCarrierService>,
    active: bool,
}

impl Drop for ServerTcpCarrierRealtimeWorkloadLease {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        self.owner.unregister_realtime_workload();
    }
}

/// Cold RAII registration for one complete response-direction Product workload.
pub(in crate::runtime) struct ServerTcpCarrierWorkloadLease {
    owner: Arc<ServerTcpCarrierService>,
    identity: ProductWorkloadIdentity,
    lane: TrafficClass,
    queued_unique_original: bool,
    demand_generation: Option<NonZeroU64>,
    successful_placement: Option<TcpCarrierStableGenerations>,
    validation_offers: Option<mpsc::Receiver<ServerTcpCarrierValidationOffer>>,
}

impl ServerTcpCarrierWorkloadLease {
    pub(in crate::runtime) fn identity(&self) -> ProductWorkloadIdentity {
        self.identity
    }

    pub(in crate::runtime) fn take_validation_offers(
        &mut self,
    ) -> Option<mpsc::Receiver<ServerTcpCarrierValidationOffer>> {
        self.validation_offers.take()
    }

    /// Tracks one classifier-owned throughput-demand episode. Fresh queued
    /// work remains an admission fact, but successful ordinary placement may
    /// drain the bounded queue without ending the episode or changing its
    /// generation.
    pub(in crate::runtime) fn update_demand(
        &mut self,
        lane: TrafficClass,
        queued_unique_original: bool,
    ) -> bool {
        if self.lane == lane && self.queued_unique_original == queued_unique_original {
            return true;
        }
        let Some(demand_generation) =
            self.owner
                .update_workload_demand(self.identity, lane, queued_unique_original)
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

    pub(in crate::runtime) fn record_successful_ordinary_placement(
        &mut self,
        stable: TcpCarrierStableGenerations,
    ) -> bool {
        if self.demand_generation.is_none() {
            return false;
        }
        self.successful_placement = Some(stable);
        true
    }

    pub(in crate::runtime) fn try_issue_saturation_demand(
        &mut self,
        saturation: ServerTcpCarrierSaturation,
        mux_limits: MuxLimits,
    ) -> Option<ServerTcpCarrierDemand> {
        let successful_placement = self.successful_placement.take()?;
        let demand_generation = self.demand_generation?;
        self.owner.try_issue_demand(
            self.identity,
            demand_generation,
            successful_placement,
            saturation,
            mux_limits,
        )
    }

    pub(in crate::runtime) fn response_product_ack_receipt_target(
        &mut self,
    ) -> Option<ResponseProductAckReceiptTarget<'_>> {
        let validation = self.owner.active_observation_validation()?;
        Some(ResponseProductAckReceiptTarget {
            identity: self.identity,
            validation,
            workload: self,
        })
    }
}

impl Drop for ServerTcpCarrierWorkloadLease {
    fn drop(&mut self) {
        self.owner.unregister_workload(self.identity);
    }
}

/// Frozen comparison input and exact service ownership for one S2C validation.
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::runtime) struct ServerTcpCarrierValidationAdmission {
    owner: Arc<ServerTcpCarrierService>,
    validation: ServerTcpCarrierValidationIdentity,
    target: ProductWorkloadIdentity,
    #[cfg_attr(not(test), allow(dead_code))]
    stable: TcpCarrierStableGenerations,
    workloads: Box<[ProductWorkloadIdentity]>,
    ordinary_services: Box<[ServerTcpCarrierOrdinaryService]>,
    geometry: TcpCarrierValidationGeometry,
    armed: bool,
}

#[cfg_attr(not(test), allow(dead_code))]
impl ServerTcpCarrierValidationAdmission {
    pub(in crate::runtime) fn request_id(&self) -> NonZeroU64 {
        self.validation.request_id
    }

    pub(in crate::runtime) fn validation_id(&self) -> NonZeroU64 {
        self.validation.validation_id
    }

    pub(in crate::runtime) fn candidate(&self) -> ServerTcpCarrierOutputInstance {
        self.validation.candidate
    }

    pub(in crate::runtime) fn target(&self) -> ProductWorkloadIdentity {
        self.target
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime) fn stable(&self) -> TcpCarrierStableGenerations {
        self.stable
    }

    pub(in crate::runtime) fn workloads(&self) -> &[ProductWorkloadIdentity] {
        &self.workloads
    }

    pub(in crate::runtime) fn ordinary_services(&self) -> &[ServerTcpCarrierOrdinaryService] {
        &self.ordinary_services
    }

    pub(in crate::runtime) fn geometry(&self) -> TcpCarrierValidationGeometry {
        self.geometry
    }

    pub(in crate::runtime) fn is_withdrawn(&self) -> bool {
        self.owner.validation_is_withdrawn(self.validation)
    }

    pub(in crate::runtime) fn activate_observations(
        &self,
        capacity: usize,
    ) -> Option<mpsc::Receiver<ServerTcpCarrierObservation>> {
        self.owner.activate_observations(self.validation, capacity)
    }

    pub(in crate::runtime) fn revalidate(
        &self,
        stable: TcpCarrierStableGenerations,
        ordinary_instances: &[ServerTcpCarrierOutputInstance],
    ) -> bool {
        self.owner
            .revalidate(self.validation, stable, ordinary_instances)
    }

    pub(in crate::runtime) fn revalidate_current(
        &self,
        output: &ServerTcpValidationOutput,
    ) -> bool {
        self.owner
            .revalidate_from_observation(self.validation, self.stable.authority_class, output)
    }

    pub(in crate::runtime) fn release(mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        self.owner.release_validation(self.validation);
    }
}

impl Drop for ServerTcpCarrierValidationAdmission {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        self.owner.release_validation(self.validation);
    }
}

fn server_tcp_carrier_comparison_key(
    observation: &ResponseSenderPathObservation,
    authority_class: crate::protocol::PathUsage,
) -> Option<(
    TcpCarrierStableGenerations,
    Vec<ServerTcpCarrierOutputInstance>,
)> {
    let stable = observation.tcp_carrier_stable_generations(authority_class)?;
    let ordinary_instances = observation
        .targets
        .iter()
        .filter(|target| !target.observation.stale_for_original_data)
        .filter(|target| {
            path_is_backup(target.observation.snapshot)
                == (authority_class == crate::protocol::PathUsage::Backup)
        })
        .map(|target| ServerTcpCarrierOutputInstance {
            key: target.observation.key,
            path_instance_id: target.observation.path_instance_id,
            output_incarnation: target.observation.incarnation,
        })
        .collect();
    Some((stable, ordinary_instances))
}

/// Filters one ACK release to the exact unambiguous candidate output. The
/// shared comparison model consumes only the returned Product byte count.
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::runtime) fn candidate_original_release_bytes(
    receipt: &ResponseProductAckReceipt,
    candidate: ServerTcpCarrierOutputInstance,
) -> Option<u64> {
    receipt
        .original_releases
        .iter()
        .filter(|release| {
            release.resolution == ResponseProductAckOriginalResolution::Unambiguous
                && release.key == candidate.key
                && release.path_instance_id == Some(candidate.path_instance_id)
                && release.output_incarnation == candidate.output_incarnation
                && release.range.start < release.range.end
                && usize::try_from(release.range.end - release.range.start).ok()
                    == Some(release.bytes)
        })
        .try_fold(0_u64, |total, release| {
            total.checked_add(u64::try_from(release.bytes).ok()?)
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ServerTcpCandidateOriginalRelease {
    pub(in crate::runtime) resolved_bytes: u64,
    pub(in crate::runtime) qualified_bytes: u64,
}

pub(in crate::runtime) fn candidate_original_release_progress(
    receipt: &ResponseProductAckReceipt,
    candidate: ServerTcpCarrierOutputInstance,
) -> Option<ServerTcpCandidateOriginalRelease> {
    let mut resolved_bytes = 0_u64;
    let mut qualified_bytes = 0_u64;
    for release in receipt.original_releases.iter().filter(|release| {
        release.key == candidate.key
            && release.path_instance_id == Some(candidate.path_instance_id)
            && release.output_incarnation == candidate.output_incarnation
    }) {
        if release.range.start >= release.range.end
            || usize::try_from(release.range.end - release.range.start).ok() != Some(release.bytes)
        {
            return None;
        }
        let bytes = u64::try_from(release.bytes).ok()?;
        resolved_bytes = resolved_bytes.checked_add(bytes)?;
        if release.resolution == ResponseProductAckOriginalResolution::Unambiguous {
            qualified_bytes = qualified_bytes.checked_add(bytes)?;
        }
    }
    Some(ServerTcpCandidateOriginalRelease {
        resolved_bytes,
        qualified_bytes,
    })
}

#[cfg(test)]
#[path = "tests_service.rs"]
mod tests;
