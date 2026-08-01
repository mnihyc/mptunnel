//! Session ownership for acknowledged elastic TCP carriers.
//!
//! The registry contains only live retained instances. Configured capacity
//! that has not passed directional validation has no entry, actor, queue, or
//! health state.

use super::client_session::{
    ClientTcpRetainedSessionStart, run_client_tcp_path_session_with_retained_stream,
};
use super::client_state::ClientTcpPathConnection;
use super::client_validation::ClientTcpValidationHandoff;
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
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

pub(in crate::runtime) struct ClientTcpRetainedCarrier {
    pub(in crate::runtime) config_index: usize,
    pub(in crate::runtime) key: RelayPathKey,
    pub(in crate::runtime) path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime) client_to_server_authority: bool,
    pub(in crate::runtime) commands: ReliablePathCommandSender,
}

pub(in crate::runtime) struct ClientTcpRetainedCarrierRegistry {
    carriers: Mutex<BTreeMap<usize, ClientTcpRetainedCarrier>>,
}

impl ClientTcpRetainedCarrierRegistry {
    pub(in crate::runtime) fn new() -> Arc<Self> {
        Arc::new(Self {
            carriers: Mutex::new(BTreeMap::new()),
        })
    }

    /// Publishes one exact retained instance. The elastic slot remains unique
    /// for the lifetime of its group reservation.
    pub(in crate::runtime) fn insert(&self, carrier: ClientTcpRetainedCarrier) -> bool {
        if carrier.key.index == usize::MAX || !carrier.client_to_server_authority {
            return false;
        }
        let mut carriers = self.carriers.lock().expect("retained TCP carrier lock");
        if carriers.contains_key(&carrier.key.index) {
            return false;
        }
        carriers.insert(carrier.key.index, carrier);
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
        carriers.remove(&key.index);
        true
    }

    /// Endpoint retirement is actor-owned and ordered. A later enable does
    /// not revoke a drain already requested for an older generation.
    pub(in crate::runtime) fn begin_endpoint_drain(&self, config_index: usize) {
        let commands = self
            .carriers
            .lock()
            .expect("retained TCP carrier lock")
            .values()
            .filter(|carrier| carrier.config_index == config_index)
            .map(|carrier| carrier.commands.clone())
            .collect::<Vec<_>>();
        for commands in commands {
            commands.begin_path_drain();
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
    } = *handoff;
    let expected = candidate.instance;
    if candidate.stream_id != remotes.stream_id()
        || expected.key.underlay != UnderlayProtocol::Tcp
        || reservation.elastic_path_index() != Some(expected.key.index)
        || reservation.path_id() != candidate.path_id
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
                    registry_published = registry.insert(ClientTcpRetainedCarrier {
                        config_index: runtime.config_index,
                        key: expected.key,
                        path_instance_id: expected.path_instance_id,
                        client_to_server_authority: true,
                        commands: commands.clone(),
                    });
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
