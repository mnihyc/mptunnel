//! Reconnecting reliable TCP actor and connection lifetime orchestration.
//!
//! This owner preserves the biased select loop, reconnect fencing, and the
//! lifetime coupling between one TCP carrier and all attached product streams.

use super::client_connection::{ClientTcpCarrierConnect, connect_client_tcp_carrier};
use super::client_datagram::ClientTcpDatagramState;
use super::client_receive::handle_client_tcp_path_frame;
use super::client_state::{ClientTcpPathConnection, ClientTcpPathSessionRuntime};
use super::client_stream::{
    ClientTcpOpenStreamRequest, ClientTcpPathStreamState, expire_client_tcp_pending_opens,
    fail_client_tcp_streams, next_client_tcp_pending_open_deadline,
    open_client_tcp_stream_on_connection,
};
use super::client_writer::handle_connected_client_tcp_command_run;
use super::group::ClientTcpCarrierGroups;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::path::{CarrierPathInstanceId, RelayPathKey, next_carrier_path_instance_id};
use crate::protocol::{Frame, PathId, PathMetricDirection, StreamId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ClientTcpOpenResponse, ReliablePathCommand, ReliablePathCommandReceivers,
    recv_reliable_path_command, reliable_path_command_pending_bytes,
    reliable_path_receivers_closed, try_recv_reliable_path_command,
};
use crate::runtime::path::model::{path_startup_metrics, path_startup_snapshot};
use crate::runtime::path::state::ClientTcpCarrierPublication;
use crate::runtime::recent_ids::RecentIdCache;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

struct ClientTcpCarrierReadiness {
    published_instance: Arc<AtomicU64>,
    groups: Arc<ClientTcpCarrierGroups>,
    current_instance: Option<CarrierPathInstanceId>,
}

impl ClientTcpCarrierReadiness {
    fn new(published_instance: Arc<AtomicU64>, groups: Arc<ClientTcpCarrierGroups>) -> Self {
        Self {
            published_instance,
            groups,
            current_instance: None,
        }
    }

    fn publish(&mut self, path_instance_id: CarrierPathInstanceId) {
        let instance = path_instance_id.as_u64();
        self.published_instance
            .compare_exchange(0, instance, Ordering::AcqRel, Ordering::Acquire)
            .expect("one TCP carrier actor owns readiness publication");
        self.current_instance = Some(path_instance_id);
        self.groups.publish_change();
    }

    fn clear(&mut self) {
        let Some(current_instance) = self.current_instance.take() else {
            return;
        };
        if self
            .published_instance
            .compare_exchange(
                current_instance.as_u64(),
                0,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.groups.publish_change();
        }
    }

    fn current_instance_raw(&self) -> u64 {
        self.current_instance
            .map_or(0, CarrierPathInstanceId::as_u64)
    }
}

impl Drop for ClientTcpCarrierReadiness {
    fn drop(&mut self) {
        self.clear();
    }
}

struct ClientTcpPathSessionState {
    connection: Option<ClientTcpPathConnection>,
    streams: HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: RecentIdCache<StreamId>,
    datagrams: ClientTcpDatagramState,
}

pub(super) async fn run_client_tcp_path_session(
    runtime: ClientTcpPathSessionRuntime,
    mut commands: ReliablePathCommandReceivers,
    published_carrier_instance: Arc<AtomicU64>,
) {
    let mut carrier_readiness =
        ClientTcpCarrierReadiness::new(published_carrier_instance, runtime.carrier_groups.clone());
    let mut state = ClientTcpPathSessionState {
        connection: None,
        streams: HashMap::new(),
        closed_streams: RecentIdCache::new(runtime.closed_stream_cache_capacity),
        datagrams: ClientTcpDatagramState::new(
            runtime.mux_limits.max_streams,
            runtime.closed_stream_cache_capacity,
        ),
    };
    let mut pending_frames = Vec::<Frame>::new();

    loop {
        if state.connection.is_none() {
            match recv_reliable_path_command(&mut commands).await {
                Some(command) => {
                    let pending_bytes = reliable_path_command_pending_bytes(&command);
                    handle_disconnected_client_tcp_command(
                        command,
                        &runtime,
                        &mut state,
                        &mut carrier_readiness,
                    )
                    .await;
                    commands.release_pending_command_bytes(pending_bytes);
                }
                None => return,
            }
            continue;
        }

        let heartbeat_at = state
            .connection
            .as_ref()
            .expect("checked connected TCP path session")
            .carrier
            .heartbeat_deadline();
        let heartbeat_timer = tokio::time::sleep_until(heartbeat_at);
        tokio::pin!(heartbeat_timer);
        let pending_open_deadline = next_client_tcp_pending_open_deadline(&state.streams);
        let pending_open_timer =
            tokio::time::sleep_until(pending_open_deadline.unwrap_or(heartbeat_at));
        tokio::pin!(pending_open_timer);

        let receivers_open = !reliable_path_receivers_closed(&commands);
        if !receivers_open {
            // No lifecycle owner remains. Native carrier termination is valid;
            // graceful retirement requires an actor-owned PATH_DRAIN followed
            // by the peer's PATH_CLOSE receipt.
            return;
        }
        let request_probe_deadline = state
            .connection
            .as_ref()
            .and_then(|connection| connection.capacity.request_deadline());
        let request_probe_timer =
            tokio::time::sleep_until(request_probe_deadline.unwrap_or(heartbeat_at));
        tokio::pin!(request_probe_timer);
        let request_probe_lease = state
            .connection
            .as_ref()
            .and_then(|connection| connection.capacity.request_lease());
        let request_probe_cancelled = async move {
            if let Some(lease) = request_probe_lease {
                lease.cancelled().await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::pin!(request_probe_cancelled);
        let request_probe_pending = request_probe_deadline.is_some();
        let command_may_recv = receivers_open && !request_probe_pending;

        let mut drop_connection = false;
        let connection = state
            .connection
            .as_mut()
            .expect("checked connected TCP path session");
        tokio::select! {
            biased;
            _ = &mut request_probe_cancelled, if request_probe_pending => {
                connection.capacity.discard_pending_receipt();
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_tcp_capacity_probe",
                    format_args!("phase=discarded reason=cancelled_after_finish path_index={}", runtime.path_index),
                );
            }
            _ = &mut request_probe_timer, if request_probe_pending => {
                connection.capacity.discard_pending_receipt();
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_tcp_capacity_probe",
                    format_args!("phase=discarded reason=receipt_timeout_after_finish path_index={}", runtime.path_index),
                );
            }
            _ = &mut pending_open_timer, if pending_open_deadline.is_some() => {
                if let Err(err) = expire_client_tcp_pending_opens(
                    connection,
                    &mut state.streams,
                    &mut state.closed_streams,
                ).await {
                    fail_client_tcp_products(&mut state.streams, &mut state.datagrams, &err);
                    crate::observability::process_event!(
                        Warn,
                        "tcp",
                        "stream_cleanup_failed",
                        "TCP pending stream cleanup failed: {err}"
                    );
                    drop_connection = true;
                }
            }
            request_id = connection.peer_status.recv_request() => {
                if let Some(request_id) = request_id {
                    let result = async {
                        connection
                            .carrier
                            .writer
                            .write_frame(&Frame::PeerStatusRequest { request_id })
                            .await?;
                        connection.carrier.writer.flush().await?;
                        Ok::<(), RuntimeError>(())
                    }
                    .await;
                    if let Err(err) = result {
                        fail_client_tcp_products(&mut state.streams, &mut state.datagrams, &err);
                        crate::observability::process_event!(
                            Warn,
                            "tcp",
                            "peer_status_failed",
                            "TCP peer status request failed: {err}"
                        );
                        drop_connection = true;
                    }
                }
            }
            frame = connection.carrier.frames.recv() => {
                match frame {
                    Some(Ok(frame)) => {
                        if let Err(err) = handle_client_tcp_path_frame(
                            frame,
                            connection,
                            &mut state.streams,
                            &mut state.closed_streams,
                            &mut state.datagrams,
                            &runtime,
                        )
                        .await
                        {
                            fail_client_tcp_products(&mut state.streams, &mut state.datagrams, &err);
                            crate::observability::process_event!(
                                Warn,
                                "tcp",
                                "session_frame_failed",
                                "TCP path session frame handling failed: {err}"
                            );
                            drop_connection = true;
                        } else if command_may_recv
                            && let Some(command) = try_recv_reliable_path_command(&mut commands)
                        {
                            let result = handle_connected_client_tcp_command_run(
                                command,
                                &mut commands,
                                connection,
                                &mut state.streams,
                                &mut state.closed_streams,
                                &mut state.datagrams,
                                &runtime,
                                carrier_readiness.current_instance_raw(),
                                runtime.stream_frame_queue,
                                runtime.mux_limits,
                                &mut pending_frames,
                            )
                            .await;
                            if let Err(err) = result {
                                fail_client_tcp_products(&mut state.streams, &mut state.datagrams, &err);
                                crate::observability::process_event!(
                                    Warn,
                                    "tcp",
                                    "session_command_failed",
                                    "TCP path session command failed: {err}"
                                );
                                drop_connection = true;
                            }
                        }
                    }
                    Some(Err(err)) => {
                        let err = RuntimeError::Encrypted(err);
                        fail_client_tcp_products(&mut state.streams, &mut state.datagrams, &err);
                        crate::observability::process_event!(
                            Warn,
                            "tcp",
                            "session_read_failed",
                            "TCP path session read failed: {err}"
                        );
                        drop_connection = true;
                    }
                    None => {
                        let err = RuntimeError::ReliablePathSessionClosed;
                        fail_client_tcp_products(&mut state.streams, &mut state.datagrams, &err);
                        drop_connection = true;
                    }
                }
            }
            command = recv_reliable_path_command(&mut commands), if command_may_recv => {
                match command {
                    Some(command) => {
                        let result = handle_connected_client_tcp_command_run(
                            command,
                            &mut commands,
                            connection,
                            &mut state.streams,
                            &mut state.closed_streams,
                            &mut state.datagrams,
                            &runtime,
                            carrier_readiness.current_instance_raw(),
                            runtime.stream_frame_queue,
                            runtime.mux_limits,
                            &mut pending_frames,
                        )
                        .await;
                        if let Err(err) = result {
                            fail_client_tcp_products(&mut state.streams, &mut state.datagrams, &err);
                            crate::observability::process_event!(
                                Warn,
                                "tcp",
                                "session_command_failed",
                                "TCP path session command failed: {err}"
                            );
                            drop_connection = true;
                        }
                    }
                    None => {
                        if reliable_path_receivers_closed(&commands) {
                            // See the receiver-closed boundary above. Do not
                            // manufacture the peer's PATH_CLOSE receipt.
                            return;
                        }
                    }
                }
            }
            _ = &mut heartbeat_timer, if !request_probe_pending => {
                if let Err(err) = connection.carrier.tick_heartbeat().await
                {
                    fail_client_tcp_products(&mut state.streams, &mut state.datagrams, &err);
                    crate::observability::process_event!(
                        Warn,
                        "tcp",
                        "heartbeat_failed",
                        "TCP path heartbeat failed: {err}"
                    );
                    drop_connection = true;
                }
            }
        }

        if drop_connection {
            retire_failed_client_tcp_connection(&runtime, &mut state, &mut carrier_readiness);
        }
    }
}

async fn handle_disconnected_client_tcp_command(
    command: ReliablePathCommand,
    runtime: &ClientTcpPathSessionRuntime,
    state: &mut ClientTcpPathSessionState,
    carrier_readiness: &mut ClientTcpCarrierReadiness,
) {
    match command {
        ReliablePathCommand::PrepareConnection {
            open_deadline,
            endpoint_generation,
            mut response,
        } => {
            if open_deadline <= tokio::time::Instant::now() {
                let _ = response.send(Err(RuntimeError::PathOpenTimedOut));
                return;
            }
            let connect = connect_client_tcp_path(runtime, open_deadline, endpoint_generation);
            tokio::pin!(connect);
            let connect_result = tokio::select! {
                biased;
                _ = response.closed() => return,
                result = &mut connect => result,
            };
            match connect_result {
                Ok(connected) => {
                    if response.is_closed() || open_deadline <= tokio::time::Instant::now() {
                        let _ = response.send(Err(RuntimeError::PathOpenTimedOut));
                        return;
                    }
                    let readiness_rtt = connected.carrier.readiness_rtt;
                    state.connection = Some(connected);
                    if !publish_client_tcp_connection(
                        runtime,
                        state,
                        carrier_readiness,
                        endpoint_generation,
                        Some(readiness_rtt),
                    ) {
                        state.connection = None;
                        let _ = response.send(Err(RuntimeError::NoSchedulableTcpPath));
                        return;
                    }
                    let _ = response.send(Ok(Some(readiness_rtt)));
                }
                Err(err) => {
                    runtime
                        .state
                        .mark_tcp_path_establishment_failure_for_endpoint_generation(
                            runtime.path_index,
                            &runtime.endpoint_policy,
                            endpoint_generation,
                        );
                    let _ = response.send(Err(err));
                }
            }
        }
        ReliablePathCommand::OpenStream {
            stream_id,
            attempt_id,
            observed_carrier_instance: _,
            target,
            lane,
            advertised_recv_max_offset,
            open_deadlines,
            session_commands,
            mut response,
        } => {
            let open_deadline = open_deadlines.setup;
            if response.is_closed() || open_deadline <= tokio::time::Instant::now() {
                let _ = response.send(ClientTcpOpenResponse::RejectedWithoutOpen(
                    RuntimeError::PathOpenTimedOut,
                ));
                return;
            }
            let endpoint_generation = match enabled_endpoint_generation(runtime) {
                Ok(generation) => generation,
                Err(error) => {
                    let _ = response.send(ClientTcpOpenResponse::RejectedWithoutOpen(error));
                    return;
                }
            };
            let connect = connect_client_tcp_path(runtime, open_deadline, endpoint_generation);
            tokio::pin!(connect);
            let connect_result = tokio::select! {
                biased;
                _ = response.closed() => return,
                result = &mut connect => result,
            };
            match connect_result {
                Ok(connected) => {
                    if response.is_closed() || open_deadline <= tokio::time::Instant::now() {
                        let _ = response.send(ClientTcpOpenResponse::RejectedWithoutOpen(
                            RuntimeError::PathOpenTimedOut,
                        ));
                        return;
                    }
                    state.connection = Some(connected);
                    if !publish_client_tcp_connection(
                        runtime,
                        state,
                        carrier_readiness,
                        endpoint_generation,
                        None,
                    ) {
                        state.connection = None;
                        let _ = response.send(ClientTcpOpenResponse::RejectedWithoutOpen(
                            RuntimeError::NoSchedulableTcpPath,
                        ));
                        return;
                    }
                    let open = ClientTcpOpenStreamRequest {
                        stream_id,
                        attempt_id,
                        target,
                        lane,
                        advertised_recv_max_offset,
                        open_deadline,
                        session_commands,
                        response,
                    };
                    let result = open_client_tcp_stream_on_connection(
                        state
                            .connection
                            .as_mut()
                            .expect("published TCP carrier remains actor-owned"),
                        open,
                        &mut state.streams,
                        runtime.stream_frame_queue,
                    )
                    .await;
                    if let Err(err) = result {
                        crate::observability::process_event!(
                            Warn,
                            "tcp",
                            "stream_open_failed",
                            "reliable stream open on new path session failed: {err}"
                        );
                        fail_client_tcp_streams(&mut state.streams, &err);
                        retire_failed_client_tcp_connection(runtime, state, carrier_readiness);
                    }
                }
                Err(err) => {
                    let _ = response.send(ClientTcpOpenResponse::RejectedWithoutOpen(err));
                }
            }
        }
        ReliablePathCommand::OpenDatagramAttachment {
            attachment_id,
            frames,
            failure,
            open_deadline,
            mut response,
        } => {
            if response.is_closed() || open_deadline <= tokio::time::Instant::now() {
                let _ = response.send(Err(RuntimeError::PathOpenTimedOut));
                return;
            }
            let endpoint_generation = match enabled_endpoint_generation(runtime) {
                Ok(generation) => generation,
                Err(error) => {
                    let _ = response.send(Err(error));
                    return;
                }
            };
            let connect = connect_client_tcp_path(runtime, open_deadline, endpoint_generation);
            tokio::pin!(connect);
            let connect_result = tokio::select! {
                biased;
                _ = response.closed() => return,
                result = &mut connect => result,
            };
            match connect_result {
                Ok(connected) => {
                    if response.is_closed() || open_deadline <= tokio::time::Instant::now() {
                        let _ = response.send(Err(RuntimeError::PathOpenTimedOut));
                        return;
                    }
                    let path_instance_id = connected.path_instance_id;
                    if let Err(err) = state.datagrams.attach(attachment_id, frames, failure) {
                        let _ = response.send(Err(err));
                        return;
                    }
                    state.connection = Some(connected);
                    if !publish_client_tcp_connection(
                        runtime,
                        state,
                        carrier_readiness,
                        endpoint_generation,
                        None,
                    ) {
                        state.connection = None;
                        state.datagrams.remove_attachment(attachment_id);
                        let _ = response.send(Err(RuntimeError::NoSchedulableTcpPath));
                        return;
                    }
                    if response.send(Ok(path_instance_id)).is_err() {
                        state.datagrams.remove_attachment(attachment_id);
                    }
                }
                Err(err) => {
                    let _ = response.send(Err(err));
                }
            }
        }
        ReliablePathCommand::OpenDatagramFlow { response, .. } => {
            let _ = response.send(Err(RuntimeError::ReliablePathSessionClosed));
        }
        ReliablePathCommand::SendDatagramFrame { response, .. } => {
            let _ = response.send(Err(RuntimeError::ReliablePathSessionClosed));
        }
        ReliablePathCommand::CloseDatagramAttachment { response, .. } => {
            if let Some(response) = response {
                let _ = response.send(Ok(()));
            }
        }
        ReliablePathCommand::SendTcpCapacityProbe(_) => {}
        ReliablePathCommand::CancelTcpOpen { .. }
        | ReliablePathCommand::SendFrame(_)
        | ReliablePathCommand::ResetAndCloseStream { .. }
        | ReliablePathCommand::CloseStream(_) => {}
    }
}

fn fail_client_tcp_products(
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    datagrams: &mut ClientTcpDatagramState,
    error: &RuntimeError,
) {
    fail_client_tcp_streams(streams, error);
    datagrams.clear();
}

fn retire_failed_client_tcp_connection(
    runtime: &ClientTcpPathSessionRuntime,
    state: &mut ClientTcpPathSessionState,
    carrier_readiness: &mut ClientTcpCarrierReadiness,
) {
    let path_instance_id = state
        .connection
        .as_ref()
        .expect("connected TCP path being retired")
        .path_instance_id;
    runtime.state.mark_path_instance_data_plane_failure(
        RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: runtime.path_index,
        },
        path_instance_id,
    );
    state.connection = None;
    // Readiness loss becomes externally visible only after exact health
    // invalidation and physical-instance removal.
    carrier_readiness.clear();
}

async fn connect_client_tcp_path(
    runtime: &ClientTcpPathSessionRuntime,
    open_deadline: tokio::time::Instant,
    endpoint_generation: u64,
) -> Result<ClientTcpPathConnection, RuntimeError> {
    if !runtime.endpoint_policy.allows(endpoint_generation) {
        return Err(RuntimeError::NoSchedulableTcpPath);
    }
    let mut startup_snapshot = path_startup_snapshot(runtime.path(), runtime.path_index);
    let startup_metrics = path_startup_metrics(
        runtime.path(),
        PathId(runtime.path_index as u16),
        PathMetricDirection::ClientToServer,
    );
    let connect = connect_client_tcp_carrier(
        ClientTcpCarrierConnect {
            path: runtime.path(),
            path_index: runtime.path_index,
            carrier_identity: runtime.carrier_identity,
            session_id: runtime.session_id,
            security: runtime.security(),
            tls: runtime.tls(),
            codec_limits: runtime.codec_limits,
            mux_limits: runtime.mux_limits,
            carrier_network: runtime.carrier_network.as_ref(),
        },
        open_deadline,
    );
    tokio::pin!(connect);
    let policy_changed =
        wait_for_endpoint_policy_change(runtime.endpoint_policy.subscribe(), endpoint_generation);
    tokio::pin!(policy_changed);
    let carrier = tokio::select! {
        biased;
        _ = &mut policy_changed => return Err(RuntimeError::NoSchedulableTcpPath),
        result = &mut connect => result?,
    };
    let path_instance_id = next_carrier_path_instance_id();
    startup_snapshot.peer_usage = Some(carrier.peer_usage);
    let peer_status = runtime.peer_status.register(runtime.session_id);
    Ok(ClientTcpPathConnection::new(
        path_instance_id,
        startup_snapshot,
        startup_metrics,
        carrier,
        peer_status,
        runtime.mux_limits,
    ))
}

fn publish_client_tcp_connection(
    runtime: &ClientTcpPathSessionRuntime,
    state: &ClientTcpPathSessionState,
    carrier_readiness: &mut ClientTcpCarrierReadiness,
    endpoint_generation: u64,
    readiness_rtt: Option<Duration>,
) -> bool {
    let connection = state
        .connection
        .as_ref()
        .expect("TCP readiness requires an actor-owned connection");
    runtime
        .state
        .publish_tcp_peer_path_usage_for_endpoint_generation(
            &runtime.endpoint_policy,
            ClientTcpCarrierPublication {
                path_index: runtime.path_index,
                endpoint_generation,
                path_instance_id: connection.path_instance_id,
                peer_usage_sequence: connection.carrier.peer_usage_sequence,
                peer_usage: connection.carrier.peer_usage,
                readiness_rtt,
            },
            || carrier_readiness.publish(connection.path_instance_id),
        )
}

fn enabled_endpoint_generation(runtime: &ClientTcpPathSessionRuntime) -> Result<u64, RuntimeError> {
    let policy = runtime.endpoint_policy.snapshot();
    policy
        .enabled
        .then_some(policy.generation)
        .ok_or(RuntimeError::NoSchedulableTcpPath)
}

async fn wait_for_endpoint_policy_change(
    mut policy: tokio::sync::watch::Receiver<super::group::ClientTcpEndpointPolicySnapshot>,
    endpoint_generation: u64,
) {
    loop {
        let current = *policy.borrow_and_update();
        if !current.enabled || current.generation != endpoint_generation {
            return;
        }
        policy
            .changed()
            .await
            .expect("endpoint policy lives with its TCP carrier actor");
    }
}
