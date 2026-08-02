//! Session ownership for acknowledged elastic TCP carriers.
//!
//! The registry contains only live retained instances. Configured capacity
//! that has not passed directional validation has no entry, actor, queue, or
//! health state.

use super::client::session::{
    ClientTcpRetainedSessionStart, run_client_tcp_path_session_with_retained_stream,
};
use super::client::state::ClientTcpPathConnection;
use super::client::validation::ClientTcpValidationHandoff;
use super::service::{ClientTcpCarrierService, ClientTcpRetainedDirectionValidationLease};
use crate::model::capacity::reliable_relay_buffer_len;
use crate::model::path::{CarrierPathInstanceId, RelayPathKey};
use crate::protocol::{PathMetricDirection, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ClientPathContext;
use crate::runtime::path::commands::{ReliablePathCommandSender, reliable_path_command_channels};
use crate::runtime::path::model::{path_startup_metrics, path_startup_snapshot};
use crate::runtime::path::ports::OpenedReliableCarrierStream;
use crate::runtime::path::state::ClientTcpCarrierPublication;
use crate::runtime::stream::{
    OpenedRemoteStream, ReliableRelayAttachOutcome, ReliableRelayAttachmentReservation,
    ReliableRelayRemoteSet,
};
use crate::scheduler::TrafficClass;
use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::mpsc;

struct ClientTcpRetainedCarrier {
    config_index: usize,
    key: RelayPathKey,
    path_instance_id: CarrierPathInstanceId,
    commands: ReliablePathCommandSender,
    #[cfg_attr(not(test), allow(dead_code))]
    authorized_tcp_directions: u8,
    retiring: bool,
    validation: Option<ClientTcpRetainedCarrierValidation>,
}

#[cfg_attr(not(test), allow(dead_code))]
struct ClientTcpRetainedCarrierValidation {
    validation_id: NonZeroU64,
    direction: PathMetricDirection,
    _session_lease: ClientTcpRetainedDirectionValidationLease,
}

pub(in crate::runtime) struct ClientTcpRetainedCarrierRegistry {
    #[cfg_attr(not(test), allow(dead_code))]
    service: Arc<ClientTcpCarrierService>,
    carriers: Mutex<BTreeMap<usize, ClientTcpRetainedCarrier>>,
}

/// Exact provisional registry ownership established before a fresh S2C
/// `RETAIN` acknowledgment. The entry carries no directional authority until
/// `commit_direction`; dropping it removes only this physical instance.
pub(in crate::runtime) struct ClientTcpRetainedCarrierPublicationReservation {
    registry: Arc<ClientTcpRetainedCarrierRegistry>,
    key: RelayPathKey,
    path_instance_id: CarrierPathInstanceId,
    active: bool,
}

impl ClientTcpRetainedCarrierPublicationReservation {
    pub(in crate::runtime) fn commit_direction(&mut self, direction: PathMetricDirection) -> bool {
        let mut carriers = self
            .registry
            .carriers
            .lock()
            .expect("retained TCP carrier lock");
        let Some(carrier) = carriers.get_mut(&self.key.index) else {
            return false;
        };
        if carrier.key != self.key
            || carrier.path_instance_id != self.path_instance_id
            || carrier.retiring
            || carrier.authorized_tcp_directions != 0
        {
            return false;
        }
        carrier.authorized_tcp_directions = tcp_carrier_direction_bit(direction);
        true
    }

    pub(in crate::runtime) fn is_direction_authorized(
        &self,
        direction: PathMetricDirection,
    ) -> bool {
        self.registry
            .direction_authorized(self.key, self.path_instance_id, direction)
    }

    /// Transfers cleanup to the retained carrier actor after health and actor
    /// publication have completed synchronously.
    pub(in crate::runtime) fn commit_publication(mut self) {
        self.active = false;
    }
}

impl Drop for ClientTcpRetainedCarrierPublicationReservation {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            let _ = self.registry.remove(self.key, self.path_instance_id);
        }
    }
}

impl ClientTcpRetainedCarrierRegistry {
    pub(in crate::runtime) fn new(service: Arc<ClientTcpCarrierService>) -> Arc<Self> {
        Arc::new(Self {
            service,
            carriers: Mutex::new(BTreeMap::new()),
        })
    }

    pub(in crate::runtime) fn reserve_publication(
        self: &Arc<Self>,
        config_index: usize,
        key: RelayPathKey,
        path_instance_id: CarrierPathInstanceId,
        commands: ReliablePathCommandSender,
    ) -> Option<ClientTcpRetainedCarrierPublicationReservation> {
        if key.underlay != UnderlayProtocol::Tcp || key.index == usize::MAX {
            return None;
        }
        let carrier = ClientTcpRetainedCarrier {
            config_index,
            key,
            path_instance_id,
            commands,
            authorized_tcp_directions: 0,
            retiring: false,
            validation: None,
        };
        let mut carriers = self.carriers.lock().expect("retained TCP carrier lock");
        if carriers.contains_key(&key.index) {
            return None;
        }
        carriers.insert(key.index, carrier);
        Some(ClientTcpRetainedCarrierPublicationReservation {
            registry: self.clone(),
            key,
            path_instance_id,
            active: true,
        })
    }

    /// Publishes one exact retained instance. The elastic slot remains unique
    /// for the lifetime of its group reservation.
    pub(in crate::runtime) fn insert(
        &self,
        config_index: usize,
        key: RelayPathKey,
        path_instance_id: CarrierPathInstanceId,
        commands: ReliablePathCommandSender,
        initial_direction: PathMetricDirection,
    ) -> bool {
        if key.underlay != UnderlayProtocol::Tcp || key.index == usize::MAX {
            return false;
        }
        let carrier = ClientTcpRetainedCarrier {
            config_index,
            key,
            path_instance_id,
            commands,
            authorized_tcp_directions: tcp_carrier_direction_bit(initial_direction),
            retiring: false,
            validation: None,
        };
        let mut carriers = self.carriers.lock().expect("retained TCP carrier lock");
        if carriers.contains_key(&key.index) {
            return false;
        }
        carriers.insert(key.index, carrier);
        true
    }

    /// Removes only the terminal physical instance that originally published
    /// this slot; a stale actor cannot depublish a later occupant.
    pub(in crate::runtime) fn remove(
        &self,
        key: RelayPathKey,
        path_instance_id: CarrierPathInstanceId,
    ) -> bool {
        let mut carriers = self.carriers.lock().expect("retained TCP carrier lock");
        if !carriers.get(&key.index).is_some_and(|carrier| {
            carrier.key == key && carrier.path_instance_id == path_instance_id
        }) {
            return false;
        }
        let removed = carriers.remove(&key.index);
        drop(carriers);
        drop(removed);
        true
    }

    /// Reports ordinary payload authority only for the exact live instance.
    /// A draining carrier is immediately ineligible for fresh placement while
    /// its actor continues ordered settlement of already-admitted work.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime) fn direction_authorized(
        &self,
        key: RelayPathKey,
        path_instance_id: CarrierPathInstanceId,
        direction: PathMetricDirection,
    ) -> bool {
        self.carriers
            .lock()
            .expect("retained TCP carrier lock")
            .get(&key.index)
            .is_some_and(|carrier| {
                carrier.key == key
                    && carrier.path_instance_id == path_instance_id
                    && !carrier.retiring
                    && carrier.authorized_tcp_directions & tcp_carrier_direction_bit(direction) != 0
            })
    }

    /// Begins one opposite-direction validation on an exact retained carrier.
    /// No registry lock is held while session ownership is reserved; the exact
    /// carrier is then rechecked before publication. Terminal cleanup remains
    /// exact-instance fenced.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime) fn begin_directional_validation(
        self: &Arc<Self>,
        key: RelayPathKey,
        path_instance_id: CarrierPathInstanceId,
        direction: PathMetricDirection,
    ) -> Option<ClientTcpRetainedCarrierValidationLease> {
        let eligible = self
            .carriers
            .lock()
            .expect("retained TCP carrier lock")
            .get(&key.index)
            .is_some_and(|carrier| {
                carrier.key == key
                    && carrier.path_instance_id == path_instance_id
                    && !carrier.retiring
                    && carrier.authorized_tcp_directions & tcp_carrier_direction_bit(direction) == 0
                    && carrier.validation.is_none()
            });
        if !eligible {
            return None;
        }
        let session = self
            .service
            .reserve_retained_direction_validation(direction)?;
        let validation_id = session.validation_id();
        debug_assert_eq!(session.direction(), direction);
        let mut carriers = self.carriers.lock().expect("retained TCP carrier lock");
        let carrier = carriers.get_mut(&key.index)?;
        if carrier.key != key
            || carrier.path_instance_id != path_instance_id
            || carrier.retiring
            || carrier.authorized_tcp_directions & tcp_carrier_direction_bit(direction) != 0
            || carrier.validation.is_some()
        {
            return None;
        }
        carrier.validation = Some(ClientTcpRetainedCarrierValidation {
            validation_id,
            direction,
            _session_lease: session,
        });
        Some(ClientTcpRetainedCarrierValidationLease {
            registry: Arc::downgrade(self),
            key,
            path_instance_id,
            validation_id,
            direction,
            active: true,
        })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn finish_directional_validation(
        &self,
        lease: &ClientTcpRetainedCarrierValidationLease,
        retain: bool,
    ) -> bool {
        let validation = {
            let mut carriers = self.carriers.lock().expect("retained TCP carrier lock");
            let Some(carrier) = carriers.get_mut(&lease.key.index) else {
                return false;
            };
            if carrier.key != lease.key
                || carrier.path_instance_id != lease.path_instance_id
                || carrier.retiring
                || !carrier.validation.as_ref().is_some_and(|validation| {
                    validation.validation_id == lease.validation_id
                        && validation.direction == lease.direction
                })
            {
                return false;
            }
            if retain {
                carrier.authorized_tcp_directions |= tcp_carrier_direction_bit(lease.direction);
            }
            carrier.validation.take()
        };
        drop(validation);
        true
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn abandon_directional_validation(&self, lease: &ClientTcpRetainedCarrierValidationLease) {
        let validation = {
            let mut carriers = self.carriers.lock().expect("retained TCP carrier lock");
            let Some(carrier) = carriers.get_mut(&lease.key.index) else {
                return;
            };
            if carrier.key != lease.key
                || carrier.path_instance_id != lease.path_instance_id
                || !carrier.validation.as_ref().is_some_and(|validation| {
                    validation.validation_id == lease.validation_id
                        && validation.direction == lease.direction
                })
            {
                return;
            }
            carrier.validation.take()
        };
        drop(validation);
    }

    /// Endpoint retirement is actor-owned and ordered. A later enable does
    /// not revoke a drain already requested for an older generation.
    pub(in crate::runtime) fn begin_endpoint_drain(&self, config_index: usize) {
        let (commands, validations) = {
            let mut carriers = self.carriers.lock().expect("retained TCP carrier lock");
            let mut commands = Vec::new();
            let mut validations = Vec::new();
            for carrier in carriers
                .values_mut()
                .filter(|carrier| carrier.config_index == config_index)
            {
                carrier.retiring = true;
                if let Some(validation) = carrier.validation.take() {
                    validations.push(validation);
                }
                commands.push(carrier.commands.clone());
            }
            (commands, validations)
        };
        drop(validations);
        for commands in commands {
            commands.begin_path_drain();
        }
    }
}

fn tcp_carrier_direction_bit(direction: PathMetricDirection) -> u8 {
    match direction {
        PathMetricDirection::ClientToServer => 1,
        PathMetricDirection::ServerToClient => 2,
    }
}

/// Exact non-clone ownership of one validation on a retained physical carrier.
/// Dropping it abandons only the matching transaction and releases the shared
/// session slot; it cannot affect a replacement instance or later validation.
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::runtime) struct ClientTcpRetainedCarrierValidationLease {
    registry: Weak<ClientTcpRetainedCarrierRegistry>,
    key: RelayPathKey,
    path_instance_id: CarrierPathInstanceId,
    validation_id: NonZeroU64,
    direction: PathMetricDirection,
    active: bool,
}

impl ClientTcpRetainedCarrierValidationLease {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime) fn validation_id(&self) -> NonZeroU64 {
        self.validation_id
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime) fn direction(&self) -> PathMetricDirection {
        self.direction
    }

    #[cfg_attr(not(test), allow(dead_code))]
    /// Commits ordinary authority only for the measured direction and exact
    /// physical instance after the role-specific result boundary.
    pub(in crate::runtime) fn commit_retained(mut self) -> bool {
        let committed = self
            .registry
            .upgrade()
            .is_some_and(|registry| registry.finish_directional_validation(&self, true));
        if committed {
            self.active = false;
        }
        committed
    }

    #[cfg_attr(not(test), allow(dead_code))]
    /// Settles a complete negative result without revoking authority already
    /// granted to the carrier's other direction.
    pub(in crate::runtime) fn settle_without_retain(mut self) -> bool {
        let settled = self
            .registry
            .upgrade()
            .is_some_and(|registry| registry.finish_directional_validation(&self, false));
        if settled {
            self.active = false;
        }
        settled
    }
}

impl Drop for ClientTcpRetainedCarrierValidationLease {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        if let Some(registry) = self.registry.upgrade() {
            registry.abandon_directional_validation(self);
        }
    }
}

/// Commits an acknowledged C2S `RETAIN` as one active elastic carrier and
/// adopts the already-established target attachment. Every operation before
/// actor spawn is synchronous, so no scheduler turn can observe partial
/// publication.
pub(in crate::runtime) async fn adopt_client_to_server_retained_carrier(
    context: &ClientPathContext,
    remotes: &mut ReliableRelayRemoteSet,
    handoff: Box<ClientTcpValidationHandoff>,
    attachment: ReliableRelayAttachmentReservation,
    lane: TrafficClass,
    send_max_offset: u64,
    advertised_recv_max_offset: u64,
) -> Result<(), RuntimeError> {
    let ClientTcpValidationHandoff {
        candidate,
        endpoint_generation,
        runtime,
        connection: carrier,
        peer_status,
        path_proofs,
        reservation,
        server_to_client,
    } = *handoff;
    let expected = candidate.instance;
    if candidate.stream_id != remotes.stream_id()
        || expected.key.underlay != UnderlayProtocol::Tcp
        || reservation.elastic_path_index() != Some(expected.key.index)
        || reservation.path_id() != candidate.path_id
        || server_to_client.is_some()
    {
        return Err(RuntimeError::Protocol(
            "retained TCP carrier handoff identity mismatch",
        ));
    }

    let mut runtime = runtime.into_runtime();
    if runtime.config_index != reservation.config_index()
        || !runtime.endpoint_policy.allows(endpoint_generation)
    {
        return Err(RuntimeError::ReliablePathRetired);
    }
    runtime.path_index = expected.key.index;
    runtime.path_id = Some(candidate.path_id);
    runtime.remote_port = Some(candidate.remote_port);

    let readiness_rtt = carrier.readiness_rtt;
    let mut startup_snapshot = path_startup_snapshot(runtime.path(), candidate.path_id);
    startup_snapshot.peer_usage = Some(carrier.peer_usage);
    let startup_metrics = path_startup_metrics(
        runtime.path(),
        candidate.path_id,
        PathMetricDirection::ClientToServer,
    );
    let mut connection = ClientTcpPathConnection::new_with_path_proofs(
        expected.path_instance_id,
        startup_snapshot,
        startup_metrics,
        carrier,
        peer_status,
        path_proofs,
        runtime.mux_limits,
    );

    let (commands, command_receivers) =
        reliable_path_command_channels(runtime.command_queue.max(1));
    let (frames_tx, frames_rx) = mpsc::channel(runtime.stream_frame_queue.max(1));
    let opened = OpenedRemoteStream::from_opened_carrier(
        OpenedReliableCarrierStream {
            stream_id: candidate.stream_id,
            path_instance_id: expected.path_instance_id,
            max_offset: send_max_offset,
            lane,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(runtime.mux_limits),
            startup: startup_snapshot,
            commands: commands.clone(),
            mux_limits: runtime.mux_limits,
            frames: frames_rx,
        },
        expected.key.index,
        advertised_recv_max_offset,
    );
    if remotes.contains_path_key(expected.key) {
        return Err(RuntimeError::Protocol(
            "retained TCP carrier duplicates an attached path key",
        ));
    }
    let attached = remotes.adopt_reserved_attachment(opened, attachment)?;
    if attached != ReliableRelayAttachOutcome::Attached {
        return Err(RuntimeError::Protocol(
            "retained TCP carrier attachment was not committed",
        ));
    }

    let registry = context.tcp_retained_carriers.clone();
    let mut registry_published = false;
    let mut authenticated_carrier = None;
    let health_published = runtime
        .endpoint_policy
        .with_current(endpoint_generation, || {
            runtime.state.publish_retained_tcp_carrier(
                ClientTcpCarrierPublication {
                    path_index: expected.key.index,
                    path_id: candidate.path_id,
                    path_instance_id: expected.path_instance_id,
                    peer_usage_sequence: connection.carrier.peer_usage_sequence,
                    peer_usage: connection.carrier.peer_usage,
                    readiness_rtt: Some(readiness_rtt),
                },
                || {
                    registry_published = registry.insert(
                        runtime.config_index,
                        expected.key,
                        expected.path_instance_id,
                        commands.clone(),
                        PathMetricDirection::ClientToServer,
                    );
                    if registry_published {
                        authenticated_carrier = Some(runtime.authenticated_carriers.register());
                    }
                },
            )
        })
        .unwrap_or(false);
    if !health_published || !registry_published {
        if health_published {
            let _ = runtime
                .state
                .remove_retained_tcp_carrier(expected.key.index, expected.path_instance_id);
        }
        let _ = registry.remove(expected.key, expected.path_instance_id);
        remotes.retire_path_instance(expected).await;
        return Err(RuntimeError::ReliablePathRetired);
    }
    connection.retain_authenticated_carrier(
        authenticated_carrier.expect("retained TCP publication registers its carrier"),
    );

    let published_instance = Arc::new(AtomicU64::new(expected.path_instance_id.as_u64()));
    let published_remote_port = Arc::new(AtomicU32::new(u32::from(candidate.remote_port)));
    let actor_terminal = Arc::new(AtomicBool::new(false));
    let terminal_state = runtime.state.clone();
    let terminal_registry = Arc::downgrade(&context.tcp_retained_carriers);
    let terminal_cleanup = Box::new(move || {
        let _ = terminal_state
            .remove_retained_tcp_carrier(expected.key.index, expected.path_instance_id);
        if let Some(registry) = terminal_registry.upgrade() {
            let _ = registry.remove(expected.key, expected.path_instance_id);
        }
    });
    tokio::spawn(async move {
        run_client_tcp_path_session_with_retained_stream(ClientTcpRetainedSessionStart {
            runtime,
            commands: command_receivers,
            published_carrier_instance: published_instance,
            published_remote_port,
            actor_terminal,
            reservation,
            connection,
            stream_id: candidate.stream_id,
            stream_frames: frames_tx,
            terminal_cleanup,
        })
        .await;
    });
    Ok(())
}

/// Receive-only target attachment returned after an acknowledged fresh S2C
/// retain. It remains outside request-output membership, so it cannot acquire
/// C2S ordinary authority by representation accident.
pub(in crate::runtime) struct ClientTcpServerToClientRetainedInput {
    pub(in crate::runtime) instance: crate::model::path::RelayPathInstance,
    pub(in crate::runtime) commands: ReliablePathCommandSender,
    pub(in crate::runtime) frames: mpsc::Receiver<Result<crate::protocol::Frame, RuntimeError>>,
}

/// Publishes an acknowledged fresh S2C carrier and continues its existing
/// target attachment in the ordinary TCP actor. Unlike the C2S path above,
/// this returns only a receive/feedback attachment and never inserts the
/// carrier into the request sender's ordinary output membership.
pub(in crate::runtime) async fn adopt_server_to_client_retained_carrier(
    context: &ClientPathContext,
    handoff: Box<ClientTcpValidationHandoff>,
) -> Result<ClientTcpServerToClientRetainedInput, RuntimeError> {
    let ClientTcpValidationHandoff {
        candidate,
        endpoint_generation,
        runtime,
        connection: carrier,
        peer_status,
        path_proofs,
        reservation,
        server_to_client,
    } = *handoff;
    let Some(server_to_client) = server_to_client else {
        return Err(RuntimeError::Protocol(
            "retained S2C TCP carrier handoff has no acknowledged publication",
        ));
    };
    let super::client::validation::ClientTcpServerToClientRetainedPreparation {
        commands,
        command_receivers,
        publication,
    } = server_to_client;
    let expected = candidate.instance;
    if expected.key.underlay != UnderlayProtocol::Tcp
        || reservation.elastic_path_index() != Some(expected.key.index)
        || reservation.path_id() != candidate.path_id
    {
        return Err(RuntimeError::Protocol(
            "retained S2C TCP carrier handoff identity mismatch",
        ));
    }

    let mut runtime = runtime.into_runtime();
    if runtime.config_index != reservation.config_index()
        || !runtime.endpoint_policy.allows(endpoint_generation)
    {
        return Err(RuntimeError::ReliablePathRetired);
    }
    runtime.path_index = expected.key.index;
    runtime.path_id = Some(candidate.path_id);
    runtime.remote_port = Some(candidate.remote_port);

    let readiness_rtt = carrier.readiness_rtt;
    let mut startup_snapshot = path_startup_snapshot(runtime.path(), candidate.path_id);
    startup_snapshot.peer_usage = Some(carrier.peer_usage);
    let startup_metrics = path_startup_metrics(
        runtime.path(),
        candidate.path_id,
        PathMetricDirection::ServerToClient,
    );
    let mut connection = ClientTcpPathConnection::new_with_path_proofs(
        expected.path_instance_id,
        startup_snapshot,
        startup_metrics,
        carrier,
        peer_status,
        path_proofs,
        runtime.mux_limits,
    );
    let (frames_tx, frames) = mpsc::channel(runtime.stream_frame_queue.max(1));

    if !publication.is_direction_authorized(PathMetricDirection::ServerToClient) {
        return Err(RuntimeError::ReliablePathRetired);
    }
    let mut authenticated_carrier = None;
    let health_published = runtime
        .endpoint_policy
        .with_current(endpoint_generation, || {
            runtime.state.publish_retained_tcp_carrier(
                ClientTcpCarrierPublication {
                    path_index: expected.key.index,
                    path_id: candidate.path_id,
                    path_instance_id: expected.path_instance_id,
                    peer_usage_sequence: connection.carrier.peer_usage_sequence,
                    peer_usage: connection.carrier.peer_usage,
                    readiness_rtt: Some(readiness_rtt),
                },
                || {
                    authenticated_carrier = Some(runtime.authenticated_carriers.register());
                },
            )
        })
        .unwrap_or(false);
    if !health_published {
        return Err(RuntimeError::ReliablePathRetired);
    }
    connection.retain_authenticated_carrier(
        authenticated_carrier.expect("retained TCP publication registers its carrier"),
    );

    let published_instance = Arc::new(AtomicU64::new(expected.path_instance_id.as_u64()));
    let published_remote_port = Arc::new(AtomicU32::new(u32::from(candidate.remote_port)));
    let actor_terminal = Arc::new(AtomicBool::new(false));
    let terminal_state = runtime.state.clone();
    let terminal_registry = Arc::downgrade(&context.tcp_retained_carriers);
    let terminal_cleanup = Box::new(move || {
        let _ = terminal_state
            .remove_retained_tcp_carrier(expected.key.index, expected.path_instance_id);
        if let Some(registry) = terminal_registry.upgrade() {
            let _ = registry.remove(expected.key, expected.path_instance_id);
        }
    });
    publication.commit_publication();
    tokio::spawn(async move {
        run_client_tcp_path_session_with_retained_stream(ClientTcpRetainedSessionStart {
            runtime,
            commands: command_receivers,
            published_carrier_instance: published_instance,
            published_remote_port,
            actor_terminal,
            reservation,
            connection,
            stream_id: candidate.stream_id,
            stream_frames: frames_tx,
            terminal_cleanup,
        })
        .await;
    });
    Ok(ClientTcpServerToClientRetainedInput {
        instance: expected,
        commands,
        frames,
    })
}

#[cfg(test)]
#[path = "tests_retained.rs"]
mod tests;
