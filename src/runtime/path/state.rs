//! Shared client-path evidence and carrier-neutral capacity budgets.
//!
//! One health lock makes mixed TCP/QUIC observations coherent. Each carrier
//! module owns its reservation, proof, and rollback transaction.

use super::commands::CapacityProbeCommandTicket;
use super::health::{ClientPathHealth, ClientPathHealthRecord, RequestCapacityReconciliationView};
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
use crate::model::tcp_service::{
    TcpServiceCarrierFence, TcpServiceCarrierGroupId, TcpServiceStreamFence,
    TcpServiceWithdrawalReason, TcpServiceWriterLifecycle,
};
use crate::protocol::{
    AuthNonce, DatagramFlowId, PathId, PathMetricDirection, PathUsage, StreamId,
    TcpCarrierAcceptedPath, UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::stream::{RequestTcpServiceFrozenStream, RequestTcpServiceWriter};
use crate::runtime::tcp_service::{
    RequestTcpServiceObserverInstallation, TcpServiceFlightSidecarError,
    TcpServiceWriterCoordinator,
};
use crate::scheduler::TrafficClass;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

/// One lock domain for coherent health, load, and carrier budget composition.
#[derive(Debug)]
pub(in crate::runtime) struct ClientPathState {
    health: Mutex<ClientPathHealth>,
    registered_carriers: Mutex<ClientRegisteredCarrierPaths>,
    tcp_service_registry: Mutex<ClientTcpServiceRegistry>,
    next_reliable_stream_id: Mutex<u64>,
    next_datagram_flow_id: Mutex<u64>,
    request_tcp_capacity_probe: RequestTcpCapacityProbeSession,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ClientRegisteredCarrierPath {
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) path_join_nonce: AuthNonce,
    pub(in crate::runtime) path_instance_id: CarrierPathInstanceId,
}

#[derive(Debug)]
struct ClientRegisteredCarrierPaths {
    tcp: Vec<Option<ClientRegisteredCarrierPath>>,
    udp: Vec<Option<ClientRegisteredCarrierPath>>,
}

#[derive(Debug, Default)]
struct ClientTcpServiceRegistry {
    writers: HashMap<StreamId, RequestTcpServiceWriter>,
    active_request: Option<ClientRequestTcpServiceActiveLifecycle>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ClientRequestTcpServicePathBinding {
    key: RelayPathKey,
    carrier: TcpServiceCarrierFence,
}

#[derive(Debug, Clone)]
struct ClientRequestTcpServiceStreamBinding {
    stream: TcpServiceStreamFence,
    writer: RequestTcpServiceWriter,
    observer_installed: bool,
    cleanup_acknowledged: bool,
}

#[derive(Debug)]
struct ClientRequestTcpServiceActiveLifecycle {
    lifecycle: TcpServiceWriterLifecycle,
    coordinator: Arc<TcpServiceWriterCoordinator>,
    carrier_group_id: TcpServiceCarrierGroupId,
    accepted: Vec<ClientRequestTcpServicePathBinding>,
    candidate: ClientRequestTcpServicePathBinding,
    streams: Vec<ClientRequestTcpServiceStreamBinding>,
    withdrawal_reason: Option<TcpServiceWithdrawalReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ClientRequestTcpServiceLifecycleState {
    Current,
    CleanupPending,
    Withdrawn(TcpServiceWithdrawalReason),
}

impl ClientRequestTcpServiceActiveLifecycle {
    fn matches_frozen_stream(
        &self,
        frozen: &RequestTcpServiceFrozenStream,
    ) -> Result<bool, TcpServiceWithdrawalReason> {
        Ok(self.carrier_group_id == frozen.carrier_group_id()
            && self.candidate == request_tcp_service_candidate_binding(frozen)
            && request_tcp_service_accepted_bindings(frozen)? == self.accepted
            && self
                .streams
                .iter()
                .any(|current| current.stream == frozen.stream()))
    }

    fn stream_writer_is_current(
        &self,
        writers: &HashMap<StreamId, RequestTcpServiceWriter>,
        stream_id: StreamId,
    ) -> bool {
        let Some(armed) = self
            .streams
            .iter()
            .find(|current| current.stream.stream_id == stream_id)
        else {
            return false;
        };
        writers
            .get(&stream_id)
            .is_some_and(|current| current.same_actor(&armed.writer))
    }

    fn stream_mut(
        &mut self,
        stream: TcpServiceStreamFence,
    ) -> Option<&mut ClientRequestTcpServiceStreamBinding> {
        self.streams
            .iter_mut()
            .find(|current| current.stream == stream)
    }

    fn cleanup_is_complete(&self) -> bool {
        self.streams
            .iter()
            .all(|stream| stream.cleanup_acknowledged && !stream.observer_installed)
    }

    fn depends_on_any(&self, indices: &[usize]) -> bool {
        self.accepted
            .iter()
            .chain(std::iter::once(&self.candidate))
            .any(|binding| indices.contains(&binding.key.index))
    }
}

fn request_tcp_service_accepted_bindings(
    frozen: &RequestTcpServiceFrozenStream,
) -> Result<Vec<ClientRequestTcpServicePathBinding>, TcpServiceWithdrawalReason> {
    let mut accepted = Vec::new();
    accepted
        .try_reserve(frozen.accepted().len())
        .map_err(|_| TcpServiceWithdrawalReason::ResourceLimit)?;
    for binding in frozen.accepted() {
        let instance = binding.instance();
        let carrier = binding.carrier();
        if instance.key.underlay != UnderlayProtocol::Tcp
            || instance.path_instance_id != carrier.local_instance_id
        {
            return Err(TcpServiceWithdrawalReason::InvalidEvidence);
        }
        accepted.push(ClientRequestTcpServicePathBinding {
            key: instance.key,
            carrier,
        });
    }
    accepted.sort_unstable_by_key(|binding| (binding.key.underlay, binding.key.index));
    if accepted.windows(2).any(|pair| pair[0].key == pair[1].key) {
        return Err(TcpServiceWithdrawalReason::InvalidEvidence);
    }
    Ok(accepted)
}

fn request_tcp_service_candidate_binding(
    frozen: &RequestTcpServiceFrozenStream,
) -> ClientRequestTcpServicePathBinding {
    let (key, carrier) = frozen.candidate_path_binding();
    ClientRequestTcpServicePathBinding { key, carrier }
}

fn request_tcp_service_sidecar_error(
    reason: TcpServiceWithdrawalReason,
) -> TcpServiceFlightSidecarError {
    match reason {
        TcpServiceWithdrawalReason::ResourceLimit => TcpServiceFlightSidecarError::ResourceLimit,
        TcpServiceWithdrawalReason::InvalidEvidence => TcpServiceFlightSidecarError::InvalidRelease,
        TcpServiceWithdrawalReason::Deadline
        | TcpServiceWithdrawalReason::DemandEnded
        | TcpServiceWithdrawalReason::FenceChanged
        | TcpServiceWithdrawalReason::NoGainSuppressed => {
            TcpServiceFlightSidecarError::ObserverStopped
        }
    }
}

fn request_tcp_service_withdrawal_reason(
    error: TcpServiceFlightSidecarError,
) -> TcpServiceWithdrawalReason {
    match error {
        TcpServiceFlightSidecarError::ResourceLimit => TcpServiceWithdrawalReason::ResourceLimit,
        TcpServiceFlightSidecarError::InvalidRelease => TcpServiceWithdrawalReason::InvalidEvidence,
        TcpServiceFlightSidecarError::ObserverStopped => TcpServiceWithdrawalReason::FenceChanged,
    }
}

fn maintain_request_tcp_service_path_bindings(
    health: &mut ClientPathHealth,
    accepted: &[ClientRequestTcpServicePathBinding],
    candidate: ClientRequestTcpServicePathBinding,
) {
    let now = Instant::now();
    for binding in accepted.iter().chain(std::iter::once(&candidate)) {
        if let Some(record) = health.tcp.get_mut(binding.key.index) {
            record.maintain(now);
        }
    }
}

fn request_tcp_service_path_bindings_match(
    health: &ClientPathHealth,
    registered: &ClientRegisteredCarrierPaths,
    accepted: &[ClientRequestTcpServicePathBinding],
    candidate: ClientRequestTcpServicePathBinding,
) -> bool {
    accepted.iter().all(|binding| {
        request_tcp_service_path_binding_matches(health, registered, *binding, false)
    }) && request_tcp_service_path_binding_matches(health, registered, candidate, true)
}

fn request_tcp_service_path_binding_matches(
    health: &ClientPathHealth,
    registered: &ClientRegisteredCarrierPaths,
    binding: ClientRequestTcpServicePathBinding,
    candidate: bool,
) -> bool {
    if binding.key.underlay != UnderlayProtocol::Tcp {
        return false;
    }
    let Some(record) = health.tcp.get(binding.key.index) else {
        return false;
    };
    let generation = if candidate {
        record.request_tcp_service_candidate_generation()
    } else {
        record.request_tcp_service_eligibility_generation()
    };
    let Some(generation) = generation else {
        return false;
    };
    let Some(carrier) = registered.tcp.get(binding.key.index).copied().flatten() else {
        return false;
    };
    binding.carrier
        == (TcpServiceCarrierFence {
            accepted: TcpCarrierAcceptedPath {
                path_id: carrier.path_id,
                path_join_nonce: carrier.path_join_nonce,
            },
            local_instance_id: carrier.path_instance_id,
            eligibility_generation: generation,
        })
}

impl ClientPathState {
    pub(in crate::runtime) fn new(health: ClientPathHealth) -> Arc<Self> {
        let tcp_path_count = health.tcp.len();
        let udp_path_count = health.udp.len();
        Arc::new(Self {
            health: Mutex::new(health),
            registered_carriers: Mutex::new(ClientRegisteredCarrierPaths {
                tcp: vec![None; tcp_path_count],
                udp: vec![None; udp_path_count],
            }),
            tcp_service_registry: Mutex::new(ClientTcpServiceRegistry::default()),
            next_reliable_stream_id: Mutex::new(0),
            next_datagram_flow_id: Mutex::new(0),
            request_tcp_capacity_probe: RequestTcpCapacityProbeSession::new(tcp_path_count),
        })
    }

    pub(in crate::runtime) fn health(&self) -> &Mutex<ClientPathHealth> {
        &self.health
    }

    pub(in crate::runtime) fn register_tcp_service_writer(
        self: &Arc<Self>,
        stream_id: StreamId,
        writer: RequestTcpServiceWriter,
    ) -> Result<ClientTcpServiceWriterRegistration, RuntimeError> {
        let mut registry = self
            .tcp_service_registry
            .lock()
            .expect("client TCP service writer registry lock");
        match registry.writers.entry(stream_id) {
            Entry::Vacant(entry) => {
                entry.insert(writer.clone());
            }
            Entry::Occupied(_) => {
                return Err(RuntimeError::Protocol(
                    "duplicate client TCP service writer",
                ));
            }
        }
        Ok(ClientTcpServiceWriterRegistration {
            state: Arc::downgrade(self),
            stream_id,
            writer,
        })
    }

    pub(in crate::runtime) fn tcp_service_writer(
        &self,
        stream_id: StreamId,
    ) -> Option<RequestTcpServiceWriter> {
        self.tcp_service_registry
            .lock()
            .expect("client TCP service writer registry lock")
            .writers
            .get(&stream_id)
            .cloned()
    }

    /// Publishes one request-direction validation lifecycle before any stream
    /// observer installation. The opaque actor snapshots are the only source
    /// of stream attachment identity; this registry adds no placement credit.
    pub(in crate::runtime) fn arm_request_tcp_service_lifecycle(
        &self,
        frozen_streams: &[RequestTcpServiceFrozenStream],
        coordinator: Arc<TcpServiceWriterCoordinator>,
    ) -> Result<(), TcpServiceWithdrawalReason> {
        if frozen_streams.is_empty()
            || coordinator.lifecycle().direction() != PathMetricDirection::ClientToServer
        {
            return Err(TcpServiceWithdrawalReason::InvalidEvidence);
        }
        let first = &frozen_streams[0];
        let carrier_group_id = first.carrier_group_id();
        let accepted = request_tcp_service_accepted_bindings(first)?;
        let candidate = request_tcp_service_candidate_binding(first);
        if accepted.is_empty()
            || accepted
                .iter()
                .any(|binding| binding.carrier == candidate.carrier)
        {
            return Err(TcpServiceWithdrawalReason::InvalidEvidence);
        }

        let mut stream_fences = Vec::new();
        stream_fences
            .try_reserve(frozen_streams.len())
            .map_err(|_| TcpServiceWithdrawalReason::ResourceLimit)?;
        for frozen in frozen_streams {
            if frozen.carrier_group_id() != carrier_group_id
                || request_tcp_service_candidate_binding(frozen) != candidate
                || request_tcp_service_accepted_bindings(frozen)? != accepted
            {
                return Err(TcpServiceWithdrawalReason::FenceChanged);
            }
            stream_fences.push(frozen.stream());
        }
        stream_fences.sort_unstable_by_key(|stream| stream.stream_id);
        if stream_fences
            .windows(2)
            .any(|pair| pair[0].stream_id == pair[1].stream_id)
        {
            return Err(TcpServiceWithdrawalReason::InvalidEvidence);
        }

        let mut registry = self
            .tcp_service_registry
            .lock()
            .expect("client TCP service writer registry lock");
        if registry.active_request.is_some() {
            return Err(TcpServiceWithdrawalReason::ResourceLimit);
        }
        let mut streams = Vec::new();
        streams
            .try_reserve(stream_fences.len())
            .map_err(|_| TcpServiceWithdrawalReason::ResourceLimit)?;
        for stream in stream_fences {
            let writer = registry
                .writers
                .get(&stream.stream_id)
                .cloned()
                .ok_or(TcpServiceWithdrawalReason::DemandEnded)?;
            streams.push(ClientRequestTcpServiceStreamBinding {
                stream,
                writer,
                observer_installed: false,
                cleanup_acknowledged: false,
            });
        }

        let transaction = coordinator.lock();
        if !transaction.installation_is_current() {
            return Err(TcpServiceWithdrawalReason::FenceChanged);
        }
        let mut health = self.health.lock().expect("client path health lock");
        maintain_request_tcp_service_path_bindings(&mut health, &accepted, candidate);
        let registered = self
            .registered_carriers
            .lock()
            .expect("client registered carrier lock");
        if !request_tcp_service_path_bindings_match(&health, &registered, &accepted, candidate) {
            return Err(TcpServiceWithdrawalReason::FenceChanged);
        }
        registry.active_request = Some(ClientRequestTcpServiceActiveLifecycle {
            lifecycle: coordinator.lifecycle(),
            coordinator: coordinator.clone(),
            carrier_group_id,
            accepted,
            candidate,
            streams,
            withdrawal_reason: None,
        });
        drop(registered);
        drop(health);
        drop(transaction);
        Ok(())
    }

    pub(in crate::runtime) fn request_tcp_service_lifecycle_state(
        &self,
        lifecycle: TcpServiceWriterLifecycle,
    ) -> Option<ClientRequestTcpServiceLifecycleState> {
        let mut registry = self
            .tcp_service_registry
            .lock()
            .expect("client TCP service writer registry lock");
        let Some(active) = registry
            .active_request
            .as_ref()
            .filter(|active| active.lifecycle == lifecycle)
        else {
            return None;
        };
        if let Some(reason) = active.withdrawal_reason {
            return Some(ClientRequestTcpServiceLifecycleState::Withdrawn(reason));
        }
        let coordinator = active.coordinator.clone();
        let transaction = coordinator.lock();
        if let Some(error) = transaction.failure() {
            let reason = request_tcp_service_withdrawal_reason(error);
            registry
                .active_request
                .as_mut()
                .expect("matched request lifecycle")
                .withdrawal_reason = Some(reason);
            return Some(ClientRequestTcpServiceLifecycleState::Withdrawn(reason));
        }
        Some(if transaction.accepts_invalidation() {
            ClientRequestTcpServiceLifecycleState::Current
        } else {
            ClientRequestTcpServiceLifecycleState::CleanupPending
        })
    }

    /// Runs one actor-local installation while the armed session lifecycle,
    /// shared coordinator, authenticated carrier fence, and writer identity
    /// remain one indivisible authority check.
    pub(in crate::runtime) fn install_request_tcp_service_observer(
        &self,
        frozen: &RequestTcpServiceFrozenStream,
        coordinator: &Arc<TcpServiceWriterCoordinator>,
        install: impl FnOnce() -> Result<bool, TcpServiceFlightSidecarError>,
    ) -> Result<RequestTcpServiceObserverInstallation, TcpServiceWithdrawalReason> {
        let mut registry = self
            .tcp_service_registry
            .lock()
            .expect("client TCP service writer registry lock");
        let Some(active) = registry.active_request.as_ref() else {
            return Err(TcpServiceWithdrawalReason::FenceChanged);
        };
        let frozen_matches = active.matches_frozen_stream(frozen)?;
        if let Some(reason) = active.withdrawal_reason {
            return Err(reason);
        }
        if !Arc::ptr_eq(&active.coordinator, coordinator)
            || active.lifecycle != coordinator.lifecycle()
            || !frozen_matches
            || !active.stream_writer_is_current(&registry.writers, frozen.stream().stream_id)
        {
            return Err(TcpServiceWithdrawalReason::FenceChanged);
        }
        let coordinator = active.coordinator.clone();
        let mut transaction = coordinator.lock();
        if !transaction.installation_is_current() {
            return Err(TcpServiceWithdrawalReason::FenceChanged);
        }
        let health = self.health.lock().expect("client path health lock");
        let registered = self
            .registered_carriers
            .lock()
            .expect("client registered carrier lock");
        let fence_matches = request_tcp_service_path_bindings_match(
            &health,
            &registered,
            &active.accepted,
            active.candidate,
        );
        drop(registered);
        drop(health);
        if !fence_matches {
            if let Some(active) = registry.active_request.as_mut() {
                active
                    .withdrawal_reason
                    .get_or_insert(TcpServiceWithdrawalReason::FenceChanged);
            }
            transaction.fail(TcpServiceFlightSidecarError::ObserverStopped);
            return Err(TcpServiceWithdrawalReason::FenceChanged);
        }
        match install() {
            Ok(installed) => {
                let active = registry
                    .active_request
                    .as_mut()
                    .expect("validated request lifecycle");
                let binding = active
                    .stream_mut(frozen.stream())
                    .expect("validated request stream fence");
                if !installed && !binding.observer_installed {
                    active
                        .withdrawal_reason
                        .get_or_insert(TcpServiceWithdrawalReason::InvalidEvidence);
                    transaction.fail(TcpServiceFlightSidecarError::InvalidRelease);
                    return Err(TcpServiceWithdrawalReason::InvalidEvidence);
                }
                binding.observer_installed = true;
                Ok(if installed {
                    RequestTcpServiceObserverInstallation::Installed
                } else {
                    RequestTcpServiceObserverInstallation::AlreadyInstalled
                })
            }
            Err(error) => {
                let proposed = request_tcp_service_withdrawal_reason(error);
                let reason = *registry
                    .active_request
                    .as_mut()
                    .expect("validated request lifecycle")
                    .withdrawal_reason
                    .get_or_insert(proposed);
                transaction.fail(error);
                Err(reason)
            }
        }
    }

    /// Records an exact actor- or controller-observed withdrawal before the
    /// actor destroys its passive observer. A successfully settled lifecycle
    /// remains a verdict, not a late withdrawal.
    pub(in crate::runtime) fn withdraw_request_tcp_service_stream(
        &self,
        stream: TcpServiceStreamFence,
        lifecycle: TcpServiceWriterLifecycle,
        proposed: TcpServiceWithdrawalReason,
    ) -> Option<TcpServiceWithdrawalReason> {
        let mut registry = self
            .tcp_service_registry
            .lock()
            .expect("client TCP service writer registry lock");
        let active = registry.active_request.as_ref().filter(|active| {
            active.lifecycle == lifecycle
                && active
                    .streams
                    .iter()
                    .any(|current| current.stream == stream)
                && active.stream_writer_is_current(&registry.writers, stream.stream_id)
        })?;
        if let Some(reason) = active.withdrawal_reason {
            return Some(reason);
        }
        let coordinator = active.coordinator.clone();
        let mut transaction = coordinator.lock();
        let reason = if transaction.accepts_invalidation() {
            proposed
        } else if let Some(error) = transaction.failure() {
            request_tcp_service_withdrawal_reason(error)
        } else {
            return None;
        };
        registry
            .active_request
            .as_mut()
            .expect("matched request lifecycle")
            .withdrawal_reason = Some(reason);
        transaction.fail(request_tcp_service_sidecar_error(reason));
        Some(reason)
    }

    /// A rejected Install may withdraw only the exact frozen actor snapshot
    /// and shared coordinator that were armed together. A stale control with
    /// the same lifecycle value has no cleanup authority.
    pub(in crate::runtime) fn withdraw_request_tcp_service_installation(
        &self,
        frozen: &RequestTcpServiceFrozenStream,
        coordinator: &Arc<TcpServiceWriterCoordinator>,
        proposed: TcpServiceWithdrawalReason,
    ) -> Option<TcpServiceWithdrawalReason> {
        let mut registry = self
            .tcp_service_registry
            .lock()
            .expect("client TCP service writer registry lock");
        let active = registry.active_request.as_ref()?;
        let frozen_matches = active.matches_frozen_stream(frozen).ok()?;
        if !frozen_matches
            || !Arc::ptr_eq(&active.coordinator, coordinator)
            || active.lifecycle != coordinator.lifecycle()
            || !active.stream_writer_is_current(&registry.writers, frozen.stream().stream_id)
        {
            return None;
        }
        if let Some(reason) = active.withdrawal_reason {
            return Some(reason);
        }
        let coordinator = active.coordinator.clone();
        let mut transaction = coordinator.lock();
        let reason = if transaction.accepts_invalidation() {
            proposed
        } else if let Some(error) = transaction.failure() {
            request_tcp_service_withdrawal_reason(error)
        } else {
            return None;
        };
        registry
            .active_request
            .as_mut()
            .expect("matched request lifecycle")
            .withdrawal_reason = Some(reason);
        transaction.fail(request_tcp_service_sidecar_error(reason));
        Some(reason)
    }

    /// A serialized actor confirms that this exact lifecycle has no remaining
    /// passive observer on its exact stream incarnation.
    pub(in crate::runtime) fn acknowledge_request_tcp_service_cleanup(
        &self,
        stream: TcpServiceStreamFence,
        lifecycle: TcpServiceWriterLifecycle,
    ) -> bool {
        let mut registry = self
            .tcp_service_registry
            .lock()
            .expect("client TCP service writer registry lock");
        let writer_is_current = registry
            .active_request
            .as_ref()
            .filter(|active| active.lifecycle == lifecycle)
            .is_some_and(|active| {
                active.stream_writer_is_current(&registry.writers, stream.stream_id)
            });
        if !writer_is_current {
            return false;
        }
        let coordinator = registry
            .active_request
            .as_ref()
            .expect("matched request lifecycle")
            .coordinator
            .clone();
        let transaction = coordinator.lock();
        if !transaction.is_stopped() {
            return false;
        }
        let Some(binding) = registry
            .active_request
            .as_mut()
            .and_then(|active| active.stream_mut(stream))
        else {
            return false;
        };
        binding.observer_installed = false;
        binding.cleanup_acknowledged = true;
        true
    }

    /// The registered stream actor acknowledges absence by its current actor
    /// identity when a Remove control has no frozen stream payload.
    pub(in crate::runtime) fn acknowledge_request_tcp_service_actor_cleanup(
        &self,
        stream_id: StreamId,
        lifecycle: TcpServiceWriterLifecycle,
    ) -> bool {
        let mut registry = self
            .tcp_service_registry
            .lock()
            .expect("client TCP service writer registry lock");
        let writer_is_current = registry
            .active_request
            .as_ref()
            .filter(|active| active.lifecycle == lifecycle)
            .is_some_and(|active| active.stream_writer_is_current(&registry.writers, stream_id));
        if !writer_is_current {
            return false;
        }
        let coordinator = registry
            .active_request
            .as_ref()
            .expect("matched request lifecycle")
            .coordinator
            .clone();
        let transaction = coordinator.lock();
        if !transaction.is_stopped() {
            return false;
        }
        let Some(binding) = registry.active_request.as_mut().and_then(|active| {
            active
                .streams
                .iter_mut()
                .find(|binding| binding.stream.stream_id == stream_id)
        }) else {
            return false;
        };
        binding.observer_installed = false;
        binding.cleanup_acknowledged = true;
        true
    }

    /// Starts or reissues cleanup for one exact lifecycle. Never-installed
    /// actors require no message; installed actors remain in every returned
    /// snapshot until their serialized Remove control acknowledges absence.
    pub(in crate::runtime) fn begin_request_tcp_service_cleanup(
        &self,
        lifecycle: TcpServiceWriterLifecycle,
        proposed: Option<TcpServiceWithdrawalReason>,
    ) -> Result<Vec<RequestTcpServiceWriter>, TcpServiceWithdrawalReason> {
        let mut registry = self
            .tcp_service_registry
            .lock()
            .expect("client TCP service writer registry lock");
        let Some(active) = registry
            .active_request
            .as_mut()
            .filter(|active| active.lifecycle == lifecycle)
        else {
            return Err(TcpServiceWithdrawalReason::FenceChanged);
        };
        let coordinator = active.coordinator.clone();
        let mut transaction = coordinator.lock();
        let count = active
            .streams
            .iter()
            .filter(|stream| stream.observer_installed && !stream.cleanup_acknowledged)
            .count();
        let mut writers = Vec::new();
        writers
            .try_reserve(count)
            .map_err(|_| TcpServiceWithdrawalReason::ResourceLimit)?;
        writers.extend(
            active
                .streams
                .iter()
                .filter(|stream| stream.observer_installed && !stream.cleanup_acknowledged)
                .map(|stream| stream.writer.clone()),
        );
        let terminal_reason = if let Some(reason) = active.withdrawal_reason {
            Some(reason)
        } else if let Some(error) = transaction.failure() {
            Some(request_tcp_service_withdrawal_reason(error))
        } else if transaction.accepts_invalidation() {
            let Some(reason) = proposed else {
                return Err(TcpServiceWithdrawalReason::InvalidEvidence);
            };
            Some(reason)
        } else {
            None
        };
        if let Some(reason) = terminal_reason {
            active.withdrawal_reason.get_or_insert(reason);
            transaction.fail(request_tcp_service_sidecar_error(reason));
        } else if !transaction.is_stopped() {
            return Err(TcpServiceWithdrawalReason::InvalidEvidence);
        }
        for stream in &mut active.streams {
            if !stream.observer_installed {
                stream.cleanup_acknowledged = true;
            }
        }
        Ok(writers)
    }

    /// Clears only a stopped exact lifecycle after all actor observers have
    /// acknowledged cleanup. A running or replacement lifecycle is untouched.
    pub(in crate::runtime) fn disarm_request_tcp_service_lifecycle(
        &self,
        lifecycle: TcpServiceWriterLifecycle,
    ) -> bool {
        let mut registry = self
            .tcp_service_registry
            .lock()
            .expect("client TCP service writer registry lock");
        let Some(active) = registry
            .active_request
            .as_ref()
            .filter(|active| active.lifecycle == lifecycle)
        else {
            return false;
        };
        let coordinator = active.coordinator.clone();
        let transaction = coordinator.lock();
        if !transaction.is_stopped() || !active.cleanup_is_complete() {
            return false;
        }
        drop(transaction);
        registry.active_request = None;
        true
    }

    /// Serializes one cold TCP authority mutation with the active validation
    /// writer boundary. No actor send or asynchronous operation is permitted
    /// while this lock domain is held.
    fn mutate_request_tcp_service_authority<T>(
        &self,
        affected_indices: &[usize],
        mutation: impl FnOnce(&mut ClientPathHealth, &mut ClientRegisteredCarrierPaths) -> T,
    ) -> T {
        let mut registry = self
            .tcp_service_registry
            .lock()
            .expect("client TCP service writer registry lock");
        let coordinator = registry
            .active_request
            .as_ref()
            .filter(|active| {
                active.withdrawal_reason.is_none() && active.depends_on_any(affected_indices)
            })
            .map(|active| active.coordinator.clone());
        let mut transaction = coordinator.as_ref().map(|coordinator| coordinator.lock());
        let monitor_current = transaction
            .as_ref()
            .is_some_and(|transaction| transaction.accepts_invalidation());
        let mut health = self.health.lock().expect("client path health lock");
        let mut registered = self
            .registered_carriers
            .lock()
            .expect("client registered carrier lock");
        let result = mutation(&mut health, &mut registered);
        let fence_changed = monitor_current
            && registry.active_request.as_ref().is_some_and(|active| {
                !request_tcp_service_path_bindings_match(
                    &health,
                    &registered,
                    &active.accepted,
                    active.candidate,
                )
            });
        if fence_changed {
            registry
                .active_request
                .as_mut()
                .expect("monitored request lifecycle")
                .withdrawal_reason
                .get_or_insert(TcpServiceWithdrawalReason::FenceChanged);
            transaction
                .as_mut()
                .expect("current request lifecycle transaction")
                .fail(TcpServiceFlightSidecarError::ObserverStopped);
        }
        result
    }

    /// Installs the first preference from a newly authenticated carrier. A new
    /// carrier instance restarts its sequence space at zero.
    pub(in crate::runtime) fn install_authenticated_path(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        path_id: PathId,
        path_join_nonce: AuthNonce,
        path_instance_id: CarrierPathInstanceId,
        sequence: u64,
        usage: PathUsage,
    ) {
        if underlay == UnderlayProtocol::Tcp {
            self.mutate_request_tcp_service_authority(&[index], |health, registered| {
                if let (Some(record), Some(registration)) =
                    (health.tcp.get_mut(index), registered.tcp.get_mut(index))
                {
                    record.install_peer_usage(path_instance_id, sequence, usage);
                    *registration = Some(ClientRegisteredCarrierPath {
                        path_id,
                        path_join_nonce,
                        path_instance_id,
                    });
                }
            });
            return;
        }
        let mut health = self.health.lock().expect("client path health lock");
        let mut registered = self
            .registered_carriers
            .lock()
            .expect("client registered carrier lock");
        if let (Some(record), Some(registration)) =
            (health.udp.get_mut(index), registered.udp.get_mut(index))
        {
            record.install_peer_usage(path_instance_id, sequence, usage);
            *registration = Some(ClientRegisteredCarrierPath {
                path_id,
                path_join_nonce,
                path_instance_id,
            });
        }
    }

    /// Publishes one exact authenticated carrier for the lifetime of the
    /// returned connection-owned registration.
    pub(in crate::runtime) fn register_authenticated_path(
        self: &Arc<Self>,
        underlay: UnderlayProtocol,
        index: usize,
        path_id: PathId,
        path_join_nonce: AuthNonce,
        path_instance_id: CarrierPathInstanceId,
        sequence: u64,
        usage: PathUsage,
    ) -> ClientAuthenticatedPathRegistration {
        self.install_authenticated_path(
            underlay,
            index,
            path_id,
            path_join_nonce,
            path_instance_id,
            sequence,
            usage,
        );
        ClientAuthenticatedPathRegistration {
            state: Arc::downgrade(self),
            key: RelayPathKey { underlay, index },
            path_instance_id,
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn install_peer_path_usage(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        path_instance_id: CarrierPathInstanceId,
        sequence: u64,
        usage: PathUsage,
    ) {
        self.install_authenticated_path(
            underlay,
            index,
            PathId(index as u16),
            AuthNonce([0; 16]),
            path_instance_id,
            sequence,
            usage,
        );
    }

    pub(in crate::runtime) fn registered_carrier(
        &self,
        key: RelayPathKey,
        expected_instance: CarrierPathInstanceId,
    ) -> Option<ClientRegisteredCarrierPath> {
        let registered = self
            .registered_carriers
            .lock()
            .expect("client registered carrier lock");
        let records = match key.underlay {
            UnderlayProtocol::Tcp => &registered.tcp,
            UnderlayProtocol::Udp => &registered.udp,
        };
        records
            .get(key.index)
            .copied()
            .flatten()
            .filter(|record| record.path_instance_id == expected_instance)
    }

    /// Returns one lock-coherent authenticated request-direction authority.
    ///
    /// Source addresses and configured locators are deliberately absent. The
    /// physical instance and PATH_JOIN nonce fence carrier replacement; the
    /// generation changes only with effective directional eligibility.
    pub(in crate::runtime) fn current_request_tcp_service_carrier(
        &self,
        key: RelayPathKey,
    ) -> Option<TcpServiceCarrierFence> {
        if key.underlay != UnderlayProtocol::Tcp {
            return None;
        }
        let mut health = self.health.lock().expect("client path health lock");
        let record = health.tcp.get_mut(key.index)?;
        record.maintain(Instant::now());
        let eligibility_generation = record.request_tcp_service_eligibility_generation()?;
        let registered = self
            .registered_carriers
            .lock()
            .expect("client registered carrier lock");
        let carrier = registered.tcp.get(key.index).copied().flatten()?;
        Some(TcpServiceCarrierFence {
            accepted: TcpCarrierAcceptedPath {
                path_id: carrier.path_id,
                path_join_nonce: carrier.path_join_nonce,
            },
            local_instance_id: carrier.path_instance_id,
            eligibility_generation,
        })
    }

    /// Returns one lock-coherent authenticated validation-candidate authority.
    ///
    /// A candidate occupies a configured TCP carrier slot above the group's
    /// configured minimum. It is deliberately absent from ordinary Product
    /// scheduling while its physical instance and peer availability remain
    /// independently fenced for validation.
    pub(in crate::runtime) fn current_request_tcp_service_candidate(
        &self,
        key: RelayPathKey,
    ) -> Option<TcpServiceCarrierFence> {
        if key.underlay != UnderlayProtocol::Tcp {
            return None;
        }
        let mut health = self.health.lock().expect("client path health lock");
        let record = health.tcp.get_mut(key.index)?;
        record.maintain(Instant::now());
        let eligibility_generation = record.request_tcp_service_candidate_generation()?;
        let registered = self
            .registered_carriers
            .lock()
            .expect("client registered carrier lock");
        let carrier = registered.tcp.get(key.index).copied().flatten()?;
        Some(TcpServiceCarrierFence {
            accepted: TcpCarrierAcceptedPath {
                path_id: carrier.path_id,
                path_join_nonce: carrier.path_join_nonce,
            },
            local_instance_id: carrier.path_instance_id,
            eligibility_generation,
        })
    }

    pub(in crate::runtime) fn retire_authenticated_path(
        &self,
        key: RelayPathKey,
        expected_instance: CarrierPathInstanceId,
    ) -> bool {
        if key.underlay == UnderlayProtocol::Tcp {
            return self.mutate_request_tcp_service_authority(
                &[key.index],
                |health, registered| {
                    let Some(registration) = registered.tcp.get_mut(key.index) else {
                        return false;
                    };
                    if registration
                        .is_none_or(|current| current.path_instance_id != expected_instance)
                    {
                        return false;
                    }
                    if let Some(current) = health.tcp.get_mut(key.index) {
                        current.retire_request_tcp_service_authority(expected_instance);
                    }
                    *registration = None;
                    true
                },
            );
        }
        let mut health = self.health.lock().expect("client path health lock");
        let mut registered = self
            .registered_carriers
            .lock()
            .expect("client registered carrier lock");
        let Some(record) = registered.udp.get_mut(key.index) else {
            return false;
        };
        if record.is_none_or(|current| current.path_instance_id != expected_instance) {
            return false;
        }
        if let Some(current) = health.udp.get_mut(key.index) {
            current.retire_request_tcp_service_authority(expected_instance);
        }
        *record = None;
        true
    }

    pub(in crate::runtime) fn update_peer_path_usage(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        path_instance_id: CarrierPathInstanceId,
        sequence: u64,
        usage: PathUsage,
    ) -> bool {
        if underlay == UnderlayProtocol::Tcp {
            return self.mutate_request_tcp_service_authority(&[index], |health, _registered| {
                health.tcp.get_mut(index).is_some_and(|record| {
                    record.update_peer_usage(path_instance_id, sequence, usage)
                })
            });
        }
        let mut health = self.health.lock().expect("client path health lock");
        health
            .udp
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
        if key.underlay == UnderlayProtocol::Tcp {
            return self.mutate_request_tcp_service_authority(
                &[key.index],
                |health, _registered| {
                    let has_schedulable_alternative =
                        path_records_have_schedulable_alternative(&health.tcp, key.index, now);
                    health.tcp.get_mut(key.index).is_some_and(|current| {
                        current.mark_data_plane_failure(
                            path_instance_id,
                            now,
                            has_schedulable_alternative,
                        )
                    })
                },
            );
        }
        let mut health = self.health.lock().expect("client path health lock");
        let has_schedulable_alternative =
            path_records_have_schedulable_alternative(&health.udp, key.index, now);
        health.udp.get_mut(key.index).is_some_and(|current| {
            current.mark_data_plane_failure(path_instance_id, now, has_schedulable_alternative)
        })
    }

    pub(in crate::runtime) fn mark_tcp_path_failure(&self, index: usize) {
        let now = Instant::now();
        self.mutate_request_tcp_service_authority(&[index], |health, _registered| {
            let has_schedulable_alternative =
                path_records_have_schedulable_alternative(&health.tcp, index, now);
            if let Some(current) = health.tcp.get_mut(index) {
                current.mark_failure(now, has_schedulable_alternative);
            }
        });
    }

    /// Applies one explicit management transaction under the same authority
    /// ordering as authenticated TCP lifecycle changes.
    pub(in crate::runtime) fn update_managed_path_records(
        &self,
        underlay: UnderlayProtocol,
        indices: &[usize],
        mut update: impl FnMut(&mut ClientPathHealthRecord),
    ) -> bool {
        if underlay == UnderlayProtocol::Tcp {
            return self.mutate_request_tcp_service_authority(indices, |health, _registered| {
                if indices.iter().any(|index| health.tcp.get(*index).is_none()) {
                    return false;
                }
                for index in indices {
                    update(
                        health
                            .tcp
                            .get_mut(*index)
                            .expect("validated TCP path runtime state"),
                    );
                }
                true
            });
        }
        let mut health = self.health.lock().expect("client path health lock");
        if indices.iter().any(|index| health.udp.get(*index).is_none()) {
            return false;
        }
        for index in indices {
            update(
                health
                    .udp
                    .get_mut(*index)
                    .expect("validated UDP path runtime state"),
            );
        }
        true
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

pub(in crate::runtime) struct ClientTcpServiceWriterRegistration {
    state: Weak<ClientPathState>,
    stream_id: StreamId,
    writer: RequestTcpServiceWriter,
}

#[must_use = "dropping the registration retires the authenticated carrier path"]
pub(in crate::runtime) struct ClientAuthenticatedPathRegistration {
    state: Weak<ClientPathState>,
    key: RelayPathKey,
    path_instance_id: CarrierPathInstanceId,
}

impl Drop for ClientAuthenticatedPathRegistration {
    fn drop(&mut self) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        state.retire_authenticated_path(self.key, self.path_instance_id);
    }
}

impl Drop for ClientTcpServiceWriterRegistration {
    fn drop(&mut self) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let mut registry = state
            .tcp_service_registry
            .lock()
            .expect("client TCP service writer registry lock");
        if !registry
            .writers
            .get(&self.stream_id)
            .is_some_and(|current| current.same_actor(&self.writer))
        {
            return;
        }
        let active_stream = registry.active_request.as_ref().and_then(|active| {
            active
                .streams
                .iter()
                .position(|stream| {
                    stream.stream.stream_id == self.stream_id
                        && stream.writer.same_actor(&self.writer)
                })
                .map(|position| {
                    (
                        position,
                        active.coordinator.clone(),
                        active.withdrawal_reason,
                    )
                })
        });
        if let Some((position, coordinator, withdrawal_reason)) = active_stream {
            let mut transaction = coordinator.lock();
            let reason = if withdrawal_reason.is_some() {
                None
            } else if transaction.accepts_invalidation() {
                Some(TcpServiceWithdrawalReason::FenceChanged)
            } else {
                transaction
                    .failure()
                    .map(request_tcp_service_withdrawal_reason)
            };
            if reason.is_some() {
                transaction.fail(TcpServiceFlightSidecarError::ObserverStopped);
            }
            let active = registry
                .active_request
                .as_mut()
                .expect("armed request lifecycle");
            if let Some(reason) = reason {
                active.withdrawal_reason.get_or_insert(reason);
            }
            let binding = active
                .streams
                .get_mut(position)
                .expect("matched request stream position");
            binding.observer_installed = false;
            binding.cleanup_acknowledged = true;
        }
        registry.writers.remove(&self.stream_id);
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

    pub(in crate::runtime) fn register_tcp_service_writer(
        &self,
        stream_id: StreamId,
        writer: RequestTcpServiceWriter,
    ) -> Result<ClientTcpServiceWriterRegistration, RuntimeError> {
        self.state.register_tcp_service_writer(stream_id, writer)
    }

    pub(in crate::runtime) fn tcp_service_writer(
        &self,
        stream_id: StreamId,
    ) -> Option<RequestTcpServiceWriter> {
        self.state.tcp_service_writer(stream_id)
    }

    pub(in crate::runtime) fn arm_request_tcp_service_lifecycle(
        &self,
        frozen_streams: &[RequestTcpServiceFrozenStream],
        coordinator: Arc<TcpServiceWriterCoordinator>,
    ) -> Result<(), TcpServiceWithdrawalReason> {
        if coordinator.lifecycle().session_id() != self.session_id {
            return Err(TcpServiceWithdrawalReason::InvalidEvidence);
        }
        self.state
            .arm_request_tcp_service_lifecycle(frozen_streams, coordinator)
    }

    pub(in crate::runtime) fn request_tcp_service_lifecycle_state(
        &self,
        lifecycle: TcpServiceWriterLifecycle,
    ) -> Option<ClientRequestTcpServiceLifecycleState> {
        if lifecycle.session_id() != self.session_id {
            return None;
        }
        self.state.request_tcp_service_lifecycle_state(lifecycle)
    }

    pub(in crate::runtime) fn install_request_tcp_service_observer(
        &self,
        frozen: &RequestTcpServiceFrozenStream,
        coordinator: &Arc<TcpServiceWriterCoordinator>,
        install: impl FnOnce() -> Result<bool, TcpServiceFlightSidecarError>,
    ) -> Result<RequestTcpServiceObserverInstallation, TcpServiceWithdrawalReason> {
        self.state
            .install_request_tcp_service_observer(frozen, coordinator, install)
    }

    pub(in crate::runtime) fn withdraw_request_tcp_service_stream(
        &self,
        stream: TcpServiceStreamFence,
        lifecycle: TcpServiceWriterLifecycle,
        reason: TcpServiceWithdrawalReason,
    ) -> Option<TcpServiceWithdrawalReason> {
        if lifecycle.session_id() != self.session_id {
            return None;
        }
        self.state
            .withdraw_request_tcp_service_stream(stream, lifecycle, reason)
    }

    pub(in crate::runtime) fn withdraw_request_tcp_service_installation(
        &self,
        frozen: &RequestTcpServiceFrozenStream,
        coordinator: &Arc<TcpServiceWriterCoordinator>,
        reason: TcpServiceWithdrawalReason,
    ) -> Option<TcpServiceWithdrawalReason> {
        if coordinator.lifecycle().session_id() != self.session_id {
            return None;
        }
        self.state
            .withdraw_request_tcp_service_installation(frozen, coordinator, reason)
    }

    pub(in crate::runtime) fn acknowledge_request_tcp_service_cleanup(
        &self,
        stream: TcpServiceStreamFence,
        lifecycle: TcpServiceWriterLifecycle,
    ) -> bool {
        if lifecycle.session_id() != self.session_id {
            return false;
        }
        self.state
            .acknowledge_request_tcp_service_cleanup(stream, lifecycle)
    }

    pub(in crate::runtime) fn acknowledge_request_tcp_service_actor_cleanup(
        &self,
        stream_id: StreamId,
        lifecycle: TcpServiceWriterLifecycle,
    ) -> bool {
        if lifecycle.session_id() != self.session_id {
            return false;
        }
        self.state
            .acknowledge_request_tcp_service_actor_cleanup(stream_id, lifecycle)
    }

    pub(in crate::runtime) fn begin_request_tcp_service_cleanup(
        &self,
        lifecycle: TcpServiceWriterLifecycle,
        reason: Option<TcpServiceWithdrawalReason>,
    ) -> Result<Vec<RequestTcpServiceWriter>, TcpServiceWithdrawalReason> {
        if lifecycle.session_id() != self.session_id {
            return Err(TcpServiceWithdrawalReason::InvalidEvidence);
        }
        self.state
            .begin_request_tcp_service_cleanup(lifecycle, reason)
    }

    pub(in crate::runtime) fn disarm_request_tcp_service_lifecycle(
        &self,
        lifecycle: TcpServiceWriterLifecycle,
    ) -> bool {
        if lifecycle.session_id() != self.session_id {
            return false;
        }
        self.state.disarm_request_tcp_service_lifecycle(lifecycle)
    }

    pub(in crate::runtime) fn update_managed_path_records(
        &self,
        underlay: UnderlayProtocol,
        indices: &[usize],
        update: impl FnMut(&mut ClientPathHealthRecord),
    ) -> bool {
        self.state
            .update_managed_path_records(underlay, indices, update)
    }

    pub(in crate::runtime) fn current_request_tcp_service_carrier(
        &self,
        key: RelayPathKey,
    ) -> Option<TcpServiceCarrierFence> {
        self.state.current_request_tcp_service_carrier(key)
    }

    pub(in crate::runtime) fn current_request_tcp_service_candidate(
        &self,
        key: RelayPathKey,
    ) -> Option<TcpServiceCarrierFence> {
        self.state.current_request_tcp_service_candidate(key)
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
                record.is_locally_eligible()
                    && path_observation_is_idle_for_probe(record.observation_at(now))
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
        self.state.mark_tcp_path_failure(index);
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
