//! Reconnecting reliable TCP actor and connection lifetime orchestration.
//!
//! This owner preserves the biased select loop, reconnect fencing, and the
//! lifetime coupling between one TCP carrier and all attached product streams.

use super::super::group::{ClientTcpCarrierGroups, ClientTcpCarrierReservation};
use super::connection::{ClientTcpCarrierConnect, connect_client_tcp_carrier};
use super::datagram::ClientTcpDatagramState;
use super::receive::handle_client_tcp_path_frame;
use super::state::{ClientTcpPathConnection, ClientTcpPathSessionRuntime};
use super::stream::{
    ClientTcpOpenStreamRequest, ClientTcpPathStreamState, expire_client_tcp_pending_opens,
    fail_client_tcp_streams, next_client_tcp_pending_open_deadline,
    open_client_tcp_stream_on_connection,
};
use super::writer::{
    handle_connected_client_tcp_command_run, write_client_tcp_frame_batch_interlocked,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::path::{
    CarrierPathInstanceId, RelayPathKey, carrier_path_instance_identity_is_available,
    try_next_carrier_path_instance_id,
};
use crate::protocol::{CloseReason, Frame, PathMetricDirection, StreamId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ClientTcpOpenResponse, ClientTcpOpenedDatagramAttachment, ReliablePathCarrierTerminalCause,
    ReliablePathCommand, ReliablePathCommandReceivers, recv_reliable_path_command,
    recv_reliable_path_command_during_drain, reliable_path_command_pending_bytes,
    reliable_path_receivers_closed, try_recv_reliable_path_command,
};
use crate::runtime::path::model::{path_startup_metrics, path_startup_snapshot};
use crate::runtime::path::state::ClientTcpCarrierPublication;
use crate::runtime::recent_ids::RecentIdCache;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

struct ClientTcpCarrierReadiness {
    published_instance: Arc<AtomicU64>,
    published_remote_port: Arc<AtomicU32>,
    groups: Arc<ClientTcpCarrierGroups>,
    current_instance: Option<CarrierPathInstanceId>,
}

impl ClientTcpCarrierReadiness {
    fn new(
        published_instance: Arc<AtomicU64>,
        published_remote_port: Arc<AtomicU32>,
        groups: Arc<ClientTcpCarrierGroups>,
    ) -> Self {
        Self {
            published_instance,
            published_remote_port,
            groups,
            current_instance: None,
        }
    }

    fn publish(&mut self, path_instance_id: CarrierPathInstanceId, remote_port: u16) {
        let instance = path_instance_id.as_u64();
        self.published_instance
            .compare_exchange(0, instance, Ordering::AcqRel, Ordering::Acquire)
            .expect("one TCP carrier actor owns readiness publication");
        self.published_remote_port
            .store(u32::from(remote_port), Ordering::Release);
        self.current_instance = Some(path_instance_id);
        self.groups.publish_change();
    }

    fn adopt_published(&mut self, path_instance_id: CarrierPathInstanceId) {
        debug_assert_eq!(
            self.published_instance.load(Ordering::Acquire),
            path_instance_id.as_u64()
        );
        self.current_instance = Some(path_instance_id);
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
            self.published_remote_port.store(0, Ordering::Release);
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

struct ClientTcpPathSessionOwnership {
    published_carrier_instance: Arc<AtomicU64>,
    published_remote_port: Arc<AtomicU32>,
    actor_terminal: Arc<AtomicBool>,
    reservation: ClientTcpCarrierReservation,
}

#[derive(Default)]
struct ClientTcpPathSessionStart {
    connection: Option<ClientTcpPathConnection>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientTcpPathActiveExit {
    Completed,
    CarrierFailed,
    DrainDeadline,
}

/// Applies exact carrier failure and planned-drain boundaries around the
/// complete actor future. Individual handlers still use the same absolute
/// deadline, but this outer boundary also cancels a native connect, read,
/// write, or routing await that never returns to the actor select loop.
async fn run_client_tcp_path_session_until_lifecycle_boundary(
    active: impl Future<Output = ()>,
    carrier_failure: impl Future<Output = ()>,
    drain_deadline: impl Future<Output = ()>,
) -> ClientTcpPathActiveExit {
    tokio::pin!(active);
    tokio::pin!(carrier_failure);
    tokio::pin!(drain_deadline);
    tokio::select! {
        biased;
        () = &mut active => ClientTcpPathActiveExit::Completed,
        () = &mut carrier_failure => ClientTcpPathActiveExit::CarrierFailed,
        () = &mut drain_deadline => ClientTcpPathActiveExit::DrainDeadline,
    }
}

pub(in crate::runtime::path::tcp) async fn run_client_tcp_path_session(
    runtime: ClientTcpPathSessionRuntime,
    commands: ReliablePathCommandReceivers,
    published_carrier_instance: Arc<AtomicU64>,
    published_remote_port: Arc<AtomicU32>,
    actor_terminal: Arc<AtomicBool>,
    reservation: ClientTcpCarrierReservation,
) {
    run_client_tcp_path_session_inner(
        runtime,
        commands,
        ClientTcpPathSessionOwnership {
            published_carrier_instance,
            published_remote_port,
            actor_terminal,
            reservation,
        },
        ClientTcpPathSessionStart::default(),
    )
    .await;
}

pub(in crate::runtime::path::tcp) async fn run_client_tcp_path_session_with_connection(
    runtime: ClientTcpPathSessionRuntime,
    commands: ReliablePathCommandReceivers,
    published_carrier_instance: Arc<AtomicU64>,
    published_remote_port: Arc<AtomicU32>,
    actor_terminal: Arc<AtomicBool>,
    reservation: ClientTcpCarrierReservation,
    connection: ClientTcpPathConnection,
) {
    run_client_tcp_path_session_inner(
        runtime,
        commands,
        ClientTcpPathSessionOwnership {
            published_carrier_instance,
            published_remote_port,
            actor_terminal,
            reservation,
        },
        ClientTcpPathSessionStart {
            connection: Some(connection),
        },
    )
    .await;
}

async fn run_client_tcp_path_session_inner(
    runtime: ClientTcpPathSessionRuntime,
    mut commands: ReliablePathCommandReceivers,
    ownership: ClientTcpPathSessionOwnership,
    start: ClientTcpPathSessionStart,
) {
    let ClientTcpPathSessionOwnership {
        published_carrier_instance,
        published_remote_port,
        actor_terminal,
        reservation,
    } = ownership;
    let ClientTcpPathSessionStart {
        connection: initial_connection,
    } = start;
    let mut actor_terminal = ClientTcpPathActorTerminal::new(actor_terminal, reservation);
    let mut carrier_readiness = ClientTcpCarrierReadiness::new(
        published_carrier_instance,
        published_remote_port,
        runtime.carrier_groups.clone(),
    );
    if let Some(connection) = initial_connection.as_ref() {
        carrier_readiness.adopt_published(connection.path_instance_id);
    }
    let mut state = ClientTcpPathSessionState {
        connection: initial_connection,
        streams: HashMap::new(),
        closed_streams: RecentIdCache::new(runtime.closed_stream_cache_capacity),
        datagrams: ClientTcpDatagramState::new(
            runtime.mux_limits.max_streams,
            runtime.closed_stream_cache_capacity,
        ),
    };
    let mut pending_frames = Vec::<Frame>::new();
    let session_closed = runtime.state.session_retirement().wait();
    tokio::pin!(session_closed);
    let drain_signal = commands.path_drain_signal();
    let drain_deadline = drain_signal.wait_for_drain_deadline(runtime.session_retention_timeout);
    let carrier_terminal_signal = commands.terminal_signal();
    let carrier_failure = async {
        match carrier_terminal_signal.wait().await {
            ReliablePathCarrierTerminalCause::Failed => {}
            ReliablePathCarrierTerminalCause::Retired => std::future::pending::<()>().await,
        }
    };
    let (terminal_reason, lifecycle_failed) = {
        let active = run_client_tcp_path_session_active(
            &runtime,
            &mut commands,
            &mut actor_terminal,
            &mut carrier_readiness,
            &mut state,
            &mut pending_frames,
        );
        let bounded_active = run_client_tcp_path_session_until_lifecycle_boundary(
            active,
            carrier_failure,
            drain_deadline,
        );
        tokio::pin!(bounded_active);
        tokio::select! {
            biased;
            reason = &mut session_closed => (Some(reason), false),
            exit = &mut bounded_active => match exit {
                ClientTcpPathActiveExit::Completed => (None, false),
                ClientTcpPathActiveExit::CarrierFailed => (None, true),
                ClientTcpPathActiveExit::DrainDeadline => (None, true),
            },
        }
    };
    if terminal_reason.is_none() && !lifecycle_failed {
        return;
    }

    let error = terminal_reason.map_or(RuntimeError::ReliablePathSessionClosed, |reason| {
        RuntimeError::RemoteClosed(reason)
    });
    fail_client_tcp_products(&mut state.streams, &mut state.datagrams, &error, &runtime);
    if state.connection.is_some() {
        retire_failed_client_tcp_connection(&runtime, &mut state, &mut carrier_readiness);
    }
    actor_terminal.finish();
}

/// Runs every await that owns client TCP carrier state. Session retirement
/// races this complete future so no platform-specific native socket wakeup can
/// defer terminal Product fanout or actor cleanup.
async fn run_client_tcp_path_session_active(
    runtime: &ClientTcpPathSessionRuntime,
    commands: &mut ReliablePathCommandReceivers,
    actor_terminal: &mut ClientTcpPathActorTerminal,
    carrier_readiness: &mut ClientTcpCarrierReadiness,
    state: &mut ClientTcpPathSessionState,
    pending_frames: &mut Vec<Frame>,
) {
    let mut draining = false;
    let mut drain_deadline = None;
    let drain_signal = commands.path_drain_signal();
    let drain_requested = drain_signal.wait();
    tokio::pin!(drain_requested);

    loop {
        if state.connection.is_none() {
            tokio::select! {
                biased;
                _ = &mut drain_requested => {
                    commands.close_for_path_drain();
                    while let Some(command) =
                        recv_reliable_path_command_during_drain(commands).await
                    {
                        let pending_bytes = reliable_path_command_pending_bytes(&command);
                        reject_client_tcp_command_for_path_drain(command);
                        commands.release_pending_command_bytes(pending_bytes);
                    }
                    actor_terminal.finish();
                    return;
                }
                command = recv_reliable_path_command(commands) => match command {
                    Some(command) => {
                        let pending_bytes = reliable_path_command_pending_bytes(&command);
                        handle_disconnected_client_tcp_command(
                            command,
                            runtime,
                            state,
                            carrier_readiness,
                        )
                        .await;
                        commands.release_pending_command_bytes(pending_bytes);
                        if state.connection.is_none() {
                            actor_terminal.finish();
                            return;
                        }
                    }
                    None => return,
                }
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

        let receivers_open = !reliable_path_receivers_closed(commands);
        if !receivers_open && !draining {
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
        let command_may_recv = (receivers_open || draining) && !request_probe_pending;
        let path_drain_timer = tokio::time::sleep_until(drain_deadline.unwrap_or(heartbeat_at));
        tokio::pin!(path_drain_timer);
        let mut drop_connection = false;
        let mut finish_drain = false;
        let connection = state
            .connection
            .as_mut()
            .expect("checked connected TCP path session");
        tokio::select! {
            biased;
            _ = &mut drain_requested, if !draining => {
                if drain_signal.is_terminal() {
                    let error = RuntimeError::ReliablePathSessionClosed;
                    fail_client_tcp_products(
                        &mut state.streams,
                        &mut state.datagrams,
                        &error,
                        runtime,
                    );
                    retire_failed_client_tcp_connection(
                        runtime,
                        state,
                        carrier_readiness,
                    );
                    actor_terminal.finish();
                    return;
                }
                begin_client_tcp_path_drain(
                    connection,
                    commands,
                    carrier_readiness,
                    runtime,
                );
                draining = true;
                drain_deadline = Some(
                    drain_signal
                        .drain_deadline(runtime.session_retention_timeout)
                        .expect("draining TCP path owns one absolute retention deadline"),
                );
            }
            _ = &mut path_drain_timer, if draining => {
                drop_connection = true;
            }
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
            _ = &mut pending_open_timer, if pending_open_deadline.is_some() && !draining => {
                if let Err(err) = expire_client_tcp_pending_opens(
                    connection,
                    &mut state.streams,
                    &mut state.closed_streams,
                ).await {
                    fail_client_tcp_products(
                        &mut state.streams,
                        &mut state.datagrams,
                        &err,
                        runtime,
                    );
                    crate::observability::process_event!(
                        Warn,
                        "tcp",
                        "stream_cleanup_failed",
                        "TCP pending stream cleanup failed: {err}"
                    );
                    drop_connection = true;
                }
            }
            request_id = connection.peer_status.recv_request(), if !draining => {
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
                        fail_client_tcp_products(
                            &mut state.streams,
                            &mut state.datagrams,
                            &err,
                            runtime,
                        );
                        crate::observability::process_event!(
                            Warn,
                            "tcp",
                            "peer_status_failed",
                            "TCP peer status request failed: path_index={} path_instance_id={} error={err}",
                            runtime.path_index,
                            connection.path_instance_id.as_u64(),
                        );
                        drop_connection = true;
                    }
                }
            }
            frame = connection.carrier.frames.recv() => {
                match frame {
                    Some(Ok(frame)) => {
                        let result = match frame {
                            Frame::PathClose { .. } if draining => Err(RuntimeError::Protocol(
                                "TCP path close preceded local drain settlement",
                            )),
                            frame if draining => match tokio::time::timeout_at(
                                drain_deadline
                                    .expect("draining TCP path owns one retention deadline"),
                                handle_client_tcp_path_frame(
                                    frame,
                                    connection,
                                    &mut state.streams,
                                    &mut state.closed_streams,
                                    &mut state.datagrams,
                                    runtime,
                                ),
                            )
                            .await
                            {
                                Ok(result) => result,
                                Err(_) => Err(RuntimeError::ReliablePathSessionClosed),
                            },
                            frame => handle_client_tcp_path_frame(
                                frame,
                                connection,
                                &mut state.streams,
                                &mut state.closed_streams,
                                &mut state.datagrams,
                                runtime,
                            )
                            .await,
                        };
                        if let Err(err) = result {
                            fail_client_tcp_products(
                                &mut state.streams,
                                &mut state.datagrams,
                                &err,
                                runtime,
                            );
                            crate::observability::process_event!(
                                Warn,
                                "tcp",
                                "session_frame_failed",
                                "TCP path session frame handling failed: path_index={} path_instance_id={} error={err}",
                                runtime.path_index,
                                connection.path_instance_id.as_u64(),
                            );
                            drop_connection = true;
                        } else if command_may_recv && !draining
                            && let Some(command) = try_recv_reliable_path_command(commands)
                        {
                            let result = handle_connected_client_tcp_command_run::<true>(
                                command,
                                commands,
                                connection,
                                &mut state.streams,
                                &mut state.closed_streams,
                                &mut state.datagrams,
                                runtime,
                                carrier_readiness.current_instance_raw(),
                                runtime.stream_frame_queue,
                                runtime.mux_limits,
                                pending_frames,
                            )
                            .await;
                            if let Err(err) = result {
                                fail_client_tcp_products(
                                    &mut state.streams,
                                    &mut state.datagrams,
                                    &err,
                                    runtime,
                                );
                                crate::observability::process_event!(
                                    Warn,
                                    "tcp",
                                    "session_command_failed",
                                    "TCP path session command failed: path_index={} path_instance_id={} error={err}",
                                    runtime.path_index,
                                    connection.path_instance_id.as_u64(),
                                );
                                drop_connection = true;
                            }
                        }
                    }
                    Some(Err(err)) => {
                        let err = RuntimeError::Encrypted(err);
                        fail_client_tcp_products(
                            &mut state.streams,
                            &mut state.datagrams,
                            &err,
                            runtime,
                        );
                        crate::observability::process_event!(
                            Warn,
                            "tcp",
                            "session_read_failed",
                            "TCP path session read failed: path_index={} path_instance_id={} error={err}",
                            runtime.path_index,
                            connection.path_instance_id.as_u64(),
                        );
                        drop_connection = true;
                    }
                    None => {
                        let err = RuntimeError::ReliablePathSessionClosed;
                        fail_client_tcp_products(
                            &mut state.streams,
                            &mut state.datagrams,
                            &err,
                            runtime,
                        );
                        drop_connection = true;
                    }
                }
            }
            command = recv_client_tcp_command(commands, draining), if command_may_recv => {
                match command {
                    Some(command) => {
                        let result = if draining {
                            match tokio::time::timeout_at(
                                drain_deadline
                                    .expect("draining TCP path owns one retention deadline"),
                                handle_draining_client_tcp_command(
                                    command,
                                    commands,
                                    connection,
                                    &mut state.streams,
                                    &mut state.closed_streams,
                                    &mut state.datagrams,
                                    runtime,
                                    carrier_readiness.current_instance_raw(),
                                    pending_frames,
                                ),
                            )
                            .await
                            {
                                Ok(result) => result,
                                Err(_) => Err(RuntimeError::ReliablePathSessionClosed),
                            }
                        } else {
                            handle_connected_client_tcp_command_run::<true>(
                                command,
                                commands,
                                connection,
                                &mut state.streams,
                                &mut state.closed_streams,
                                &mut state.datagrams,
                                runtime,
                                carrier_readiness.current_instance_raw(),
                                runtime.stream_frame_queue,
                                runtime.mux_limits,
                                pending_frames,
                            )
                            .await
                        };
                        if let Err(err) = result {
                            fail_client_tcp_products(
                                &mut state.streams,
                                &mut state.datagrams,
                                &err,
                                runtime,
                            );
                            crate::observability::process_event!(
                                Warn,
                                "tcp",
                                "session_command_failed",
                                "TCP path session command failed: path_index={} path_instance_id={} error={err}",
                                runtime.path_index,
                                connection.path_instance_id.as_u64(),
                            );
                            drop_connection = true;
                        }
                    }
                    None => {
                        if draining {
                            finish_drain = true;
                        } else if reliable_path_receivers_closed(commands) {
                            // See the receiver-closed boundary above. Do not
                            // manufacture the peer's PATH_CLOSE receipt.
                            return;
                        }
                    }
                }
            }
            _ = &mut heartbeat_timer, if !request_probe_pending && !draining => {
                if let Err(err) = connection.carrier.tick_heartbeat().await
                {
                    fail_client_tcp_products(
                        &mut state.streams,
                        &mut state.datagrams,
                        &err,
                        runtime,
                    );
                    crate::observability::process_event!(
                        Warn,
                        "tcp",
                        "heartbeat_failed",
                        "TCP path heartbeat failed: path_index={} path_instance_id={} error={err}",
                        runtime.path_index,
                        connection.path_instance_id.as_u64(),
                    );
                    drop_connection = true;
                }
            }
        }

        if finish_drain {
            let path_instance_id = connection.path_instance_id;
            let drain_result = match tokio::time::timeout_at(
                drain_deadline.expect("draining TCP path owns one retention deadline"),
                drain_client_tcp_path(
                    connection,
                    &mut state.streams,
                    &mut state.closed_streams,
                    &mut state.datagrams,
                    runtime,
                    pending_frames,
                ),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(RuntimeError::ReliablePathSessionClosed),
            };
            let planned_retirement_completed =
                drain_result.is_ok() && commands.finish_planned_path_retirement();
            match &drain_result {
                Ok(()) if planned_retirement_completed => {
                    runtime.state.retire_path_instance_planned(
                        RelayPathKey {
                            underlay: UnderlayProtocol::Tcp,
                            index: runtime.path_index,
                        },
                        path_instance_id,
                    );
                    fail_client_tcp_products(
                        &mut state.streams,
                        &mut state.datagrams,
                        &RuntimeError::ReliablePathRetired,
                        runtime,
                    );
                }
                Ok(()) => {
                    // Another exact lifecycle owner made failure terminal
                    // before ordered PATH_CLOSE completed. First terminal
                    // publication wins; do not relabel it as planned drain.
                    fail_client_tcp_products(
                        &mut state.streams,
                        &mut state.datagrams,
                        &RuntimeError::ReliablePathSessionClosed,
                        runtime,
                    );
                }
                Err(error) => {
                    fail_client_tcp_products(
                        &mut state.streams,
                        &mut state.datagrams,
                        error,
                        runtime,
                    );
                }
            }
            assert!(
                state.streams.is_empty() && state.datagrams.is_empty(),
                "terminal TCP path drain transfers or releases every Product attachment"
            );
            if !planned_retirement_completed {
                retire_failed_client_tcp_connection(runtime, state, carrier_readiness);
            } else {
                state.connection = None;
                carrier_readiness.clear();
            }
            actor_terminal.finish();
            return;
        }
        if drop_connection {
            retire_failed_client_tcp_connection(runtime, state, carrier_readiness);
            actor_terminal.finish();
            return;
        }
    }
}

struct ClientTcpPathActorTerminal {
    terminal: Arc<AtomicBool>,
    reservation: Option<ClientTcpCarrierReservation>,
    finished: bool,
}

impl ClientTcpPathActorTerminal {
    fn new(terminal: Arc<AtomicBool>, reservation: ClientTcpCarrierReservation) -> Self {
        Self {
            terminal,
            reservation: Some(reservation),
            finished: false,
        }
    }

    fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.terminal.store(true, Ordering::Release);
        self.finished = true;
        // Reconciliation wakes from the reservation's release only after the
        // slot is terminal and group capacity is actually available.
        drop(self.reservation.take());
    }
}

impl Drop for ClientTcpPathActorTerminal {
    fn drop(&mut self) {
        self.finish();
    }
}

async fn recv_client_tcp_command(
    commands: &mut ReliablePathCommandReceivers,
    draining: bool,
) -> Option<ReliablePathCommand> {
    if draining {
        recv_reliable_path_command_during_drain(commands).await
    } else {
        recv_reliable_path_command(commands).await
    }
}

fn begin_client_tcp_path_drain(
    connection: &mut ClientTcpPathConnection,
    commands: &mut ReliablePathCommandReceivers,
    carrier_readiness: &mut ClientTcpCarrierReadiness,
    runtime: &ClientTcpPathSessionRuntime,
) {
    runtime.state.begin_path_instance_planned_retirement(
        RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: runtime.path_index,
        },
        connection.path_instance_id,
    );
    carrier_readiness.clear();
    connection.capacity.discard_pending_receipt();
    connection.path_proofs.cancel_for_path_drain();
    commands.close_for_path_drain();
}

#[allow(clippy::too_many_arguments)]
async fn handle_draining_client_tcp_command(
    command: ReliablePathCommand,
    commands: &mut ReliablePathCommandReceivers,
    connection: &mut ClientTcpPathConnection,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    datagrams: &mut ClientTcpDatagramState,
    runtime: &ClientTcpPathSessionRuntime,
    carrier_instance: u64,
    pending_frames: &mut Vec<Frame>,
) -> Result<(), RuntimeError> {
    let pending_bytes = reliable_path_command_pending_bytes(&command);
    match command {
        ReliablePathCommand::PrepareConnection { response, .. } => {
            let _ = response.send(Err(RuntimeError::NoSchedulableTcpPath));
            commands.release_pending_command_bytes(pending_bytes);
            Ok(())
        }
        ReliablePathCommand::OpenStream { response, .. } => {
            let _ = response.send(ClientTcpOpenResponse::RejectedWithoutOpen(
                RuntimeError::NoSchedulableTcpPath,
            ));
            commands.release_pending_command_bytes(pending_bytes);
            Ok(())
        }
        ReliablePathCommand::OpenDatagramAttachment { response, .. } => {
            let _ = response.send(Err(RuntimeError::NoSchedulableTcpPath));
            commands.release_pending_command_bytes(pending_bytes);
            Ok(())
        }
        ReliablePathCommand::OpenDatagramFlow { response, .. } => {
            let _ = response.send(Err(RuntimeError::NoSchedulableTcpPath));
            commands.release_pending_command_bytes(pending_bytes);
            Ok(())
        }
        ReliablePathCommand::SendTcpCapacityProbe(probe) => {
            probe.request_lease().refund_if_unwritten();
            commands.release_pending_command_bytes(pending_bytes);
            Ok(())
        }
        ReliablePathCommand::SendFrame(frame) if client_tcp_frame_is_measurement_only(&frame) => {
            commands.release_pending_command_bytes(pending_bytes);
            Ok(())
        }
        command => {
            handle_connected_client_tcp_command_run::<false>(
                command,
                commands,
                connection,
                streams,
                closed_streams,
                datagrams,
                runtime,
                carrier_instance,
                runtime.stream_frame_queue,
                runtime.mux_limits,
                pending_frames,
            )
            .await
        }
    }
}

async fn drain_client_tcp_path(
    connection: &mut ClientTcpPathConnection,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    datagrams: &mut ClientTcpDatagramState,
    runtime: &ClientTcpPathSessionRuntime,
    pending_frames: &mut Vec<Frame>,
) -> Result<(), RuntimeError> {
    let path_id = connection.carrier.path_id;
    if !pending_frames.is_empty() {
        return Err(RuntimeError::Protocol(
            "TCP path drain found an uncommitted writer batch",
        ));
    }
    pending_frames.extend(
        streams
            .keys()
            .copied()
            .map(|stream_id| Frame::StreamDetach { stream_id }),
    );
    pending_frames.extend(
        datagrams
            .flow_ids()
            .into_iter()
            .map(|flow_id| Frame::DatagramClose { flow_id }),
    );
    pending_frames.push(Frame::PathDrain { path_id });
    let deferred_frame = write_client_tcp_frame_batch_interlocked(
        connection,
        pending_frames,
        streams,
        closed_streams,
        datagrams,
        runtime,
    )
    .await?;

    if let Some(frame) = deferred_frame
        && match apply_client_tcp_drain_frame(
            frame,
            connection,
            streams,
            closed_streams,
            datagrams,
            runtime,
        )
        .await?
        {
            ClientTcpDrainFrameDisposition::Continue => false,
            ClientTcpDrainFrameDisposition::Complete => true,
        }
    {
        return Ok(());
    }
    loop {
        let frame = connection.carrier.frames.recv().await;
        match frame {
            Some(Ok(frame)) => {
                match apply_client_tcp_drain_frame(
                    frame,
                    connection,
                    streams,
                    closed_streams,
                    datagrams,
                    runtime,
                )
                .await?
                {
                    ClientTcpDrainFrameDisposition::Continue => {}
                    ClientTcpDrainFrameDisposition::Complete => return Ok(()),
                }
            }
            Some(Err(error)) => return Err(RuntimeError::Encrypted(error)),
            None => return Err(RuntimeError::ReliablePathSessionClosed),
        }
    }
}

enum ClientTcpDrainFrameDisposition {
    Continue,
    Complete,
}

async fn apply_client_tcp_drain_frame(
    frame: Frame,
    connection: &mut ClientTcpPathConnection,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    datagrams: &mut ClientTcpDatagramState,
    runtime: &ClientTcpPathSessionRuntime,
) -> Result<ClientTcpDrainFrameDisposition, RuntimeError> {
    match frame {
        Frame::PathClose {
            path_id: close_path_id,
            reason: CloseReason::Normal,
        } if close_path_id == connection.carrier.path_id => {
            ensure_client_tcp_drain_control_is_idle(connection)?;
            Ok(ClientTcpDrainFrameDisposition::Complete)
        }
        Frame::PathClose {
            path_id: close_path_id,
            ..
        } if close_path_id != connection.carrier.path_id => Err(RuntimeError::Protocol(
            "TCP path close acknowledgment path mismatch",
        )),
        Frame::PathClose { reason, .. } => Err(RuntimeError::RemotePathClosed(reason)),
        Frame::PathDrain { .. } => Err(RuntimeError::Protocol(
            "TCP client received peer path drain request",
        )),
        frame => {
            handle_client_tcp_path_frame(
                frame,
                connection,
                streams,
                closed_streams,
                datagrams,
                runtime,
            )
            .await?;
            Ok(ClientTcpDrainFrameDisposition::Continue)
        }
    }
}

fn ensure_client_tcp_drain_control_is_idle(
    connection: &ClientTcpPathConnection,
) -> Result<(), RuntimeError> {
    if !connection.path_proofs.is_idle() || !connection.capacity.is_idle() {
        return Err(RuntimeError::Protocol(
            "TCP path close preceded local control settlement",
        ));
    }
    Ok(())
}

fn client_tcp_frame_is_measurement_only(frame: &Frame) -> bool {
    matches!(
        frame,
        Frame::PathProofData { .. }
            | Frame::PathProofAck { .. }
            | Frame::PathCapacityData { .. }
            | Frame::PathCapacityFinish { .. }
            | Frame::PathCapacityReceipt { .. }
    )
}

fn reject_client_tcp_command_for_path_drain(command: ReliablePathCommand) {
    match command {
        ReliablePathCommand::PrepareConnection { response, .. } => {
            let _ = response.send(Err(RuntimeError::NoSchedulableTcpPath));
        }
        ReliablePathCommand::OpenStream { response, .. } => {
            let _ = response.send(ClientTcpOpenResponse::RejectedWithoutOpen(
                RuntimeError::NoSchedulableTcpPath,
            ));
        }
        ReliablePathCommand::OpenDatagramAttachment { response, .. } => {
            let _ = response.send(Err(RuntimeError::NoSchedulableTcpPath));
        }
        ReliablePathCommand::OpenDatagramFlow { response, .. } => {
            let _ = response.send(Err(RuntimeError::NoSchedulableTcpPath));
        }
        ReliablePathCommand::SendDatagramFrame { response, .. } => {
            let _ = response.send(Err(RuntimeError::ReliablePathSessionClosed));
        }
        ReliablePathCommand::CloseDatagramAttachment { response, .. } => {
            if let Some(response) = response {
                let _ = response.send(Ok(()));
            }
        }
        ReliablePathCommand::SendTcpCapacityProbe(probe) => {
            probe.request_lease().refund_if_unwritten();
        }
        ReliablePathCommand::CancelTcpOpen { .. }
        | ReliablePathCommand::SendFrame(_)
        | ReliablePathCommand::ResetAndCloseStream { .. }
        | ReliablePathCommand::CloseStream(_) => {}
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
                        let _ = response.send(Err(client_tcp_publication_refusal(runtime)));
                        return;
                    }
                    let _ = response.send(Ok(Some(readiness_rtt)));
                }
                Err(err) => {
                    if client_tcp_establishment_error_has_health_authority(&err) {
                        runtime
                            .state
                            .mark_tcp_path_establishment_failure_for_endpoint_generation(
                                runtime.path_index,
                                &runtime.endpoint_policy,
                                endpoint_generation,
                            );
                    }
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
            initial_demand,
            return_plan,
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
                            client_tcp_publication_refusal(runtime),
                        ));
                        return;
                    }
                    let open = ClientTcpOpenStreamRequest {
                        stream_id,
                        attempt_id,
                        target,
                        lane,
                        initial_demand,
                        return_plan,
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
                    if client_tcp_establishment_error_has_health_authority(&err) {
                        runtime
                            .state
                            .mark_tcp_path_establishment_failure_for_endpoint_generation(
                                runtime.path_index,
                                &runtime.endpoint_policy,
                                endpoint_generation,
                            );
                    }
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
                        let _ = response.send(Err(client_tcp_publication_refusal(runtime)));
                        return;
                    }
                    let connection = state
                        .connection
                        .as_ref()
                        .expect("published TCP datagram carrier remains actor-owned");
                    let evidence = runtime.attachment_evidence(connection);
                    if response
                        .send(Ok(ClientTcpOpenedDatagramAttachment {
                            path_instance_id: connection.path_instance_id,
                            path_snapshot: evidence.snapshot,
                        }))
                        .is_err()
                    {
                        state.datagrams.remove_attachment(attachment_id);
                    }
                }
                Err(err) => {
                    if client_tcp_establishment_error_has_health_authority(&err) {
                        runtime
                            .state
                            .mark_tcp_path_establishment_failure_for_endpoint_generation(
                                runtime.path_index,
                                &runtime.endpoint_policy,
                                endpoint_generation,
                            );
                    }
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
    runtime: &ClientTcpPathSessionRuntime,
) {
    let terminal_error = runtime
        .state
        .session_lifecycle()
        .reason()
        .map(RuntimeError::RemoteClosed);
    let error = terminal_error.as_ref().unwrap_or(error);
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

fn client_tcp_establishment_error_has_health_authority(error: &RuntimeError) -> bool {
    !matches!(error, RuntimeError::ExactIdentityExhausted)
}

pub(in crate::runtime::path::tcp) async fn connect_client_tcp_path(
    runtime: &ClientTcpPathSessionRuntime,
    open_deadline: tokio::time::Instant,
    endpoint_generation: u64,
) -> Result<ClientTcpPathConnection, RuntimeError> {
    runtime.state.session_lifecycle().ensure_active()?;
    if !runtime.endpoint_policy.allows(endpoint_generation) {
        return Err(RuntimeError::NoSchedulableTcpPath);
    }
    if !carrier_path_instance_identity_is_available() {
        return Err(RuntimeError::ExactIdentityExhausted);
    }
    let path_id = runtime.path_id();
    let mut startup_snapshot = path_startup_snapshot(runtime.path(), path_id);
    let startup_metrics =
        path_startup_metrics(runtime.path(), path_id, PathMetricDirection::ClientToServer);
    let connect = connect_client_tcp_carrier(
        ClientTcpCarrierConnect {
            path: runtime.path(),
            path_id,
            carrier_identity: runtime.carrier_identity,
            session_id: runtime.session_id,
            security: runtime.security(),
            tls: runtime.tls(),
            codec_limits: runtime.codec_limits,
            mux_limits: runtime.mux_limits,
            carrier_network: runtime.carrier_network.as_ref(),
            session_lifecycle: runtime.state.session_lifecycle().clone(),
            remote_port: runtime.remote_port,
        },
        open_deadline,
    );
    tokio::pin!(connect);
    let mut policy_changes = runtime.endpoint_policy.subscribe();
    let policy_changed = wait_for_endpoint_policy_change(&mut policy_changes, endpoint_generation);
    tokio::pin!(policy_changed);
    let session_closed = runtime.state.session_retirement().wait();
    tokio::pin!(session_closed);
    let carrier_result = tokio::select! {
        biased;
        reason = &mut session_closed => return Err(RuntimeError::RemoteClosed(reason)),
        _ = &mut policy_changed => return Err(RuntimeError::NoSchedulableTcpPath),
        result = &mut connect => result,
    };
    let carrier = match carrier_result {
        Ok(carrier) => carrier,
        Err(RuntimeError::RemoteClosed(reason)) => {
            let reason = runtime.state.session_lifecycle().retire(reason);
            return Err(RuntimeError::RemoteClosed(reason));
        }
        Err(error) => return Err(error),
    };
    debug_assert_eq!(carrier.path_id, path_id);
    let path_instance_id =
        try_next_carrier_path_instance_id().ok_or(RuntimeError::ExactIdentityExhausted)?;
    startup_snapshot.peer_usage = Some(carrier.peer_usage);
    let peer_status = runtime.peer_status.register_path(
        runtime.session_id,
        UnderlayProtocol::Tcp,
        path_id,
        runtime.path_index,
        Some(carrier.remote_port),
    );
    Ok(ClientTcpPathConnection::new(
        path_instance_id,
        startup_snapshot,
        startup_metrics,
        carrier,
        peer_status,
        runtime.mux_limits,
    ))
}

/// Warms only the connection-local timing prior from this exact carrier's
/// authenticated readiness exchange. Immutable configured jitter/rate hints
/// remain intact; no predecessor or native TCP state enters the successor.
pub(super) fn apply_authenticated_readiness_to_startup_evidence(
    snapshot: &mut crate::scheduler::PathSnapshot,
    metrics: &mut crate::protocol::PathMetrics,
    readiness_rtt: Duration,
) {
    let srtt_us = u32::try_from(readiness_rtt.as_micros())
        .unwrap_or(u32::MAX)
        .max(1);
    snapshot.srtt_ms = f64::from(srtt_us) / 1_000.0;
    metrics.srtt_us = srtt_us;
}

fn publish_client_tcp_connection(
    runtime: &ClientTcpPathSessionRuntime,
    state: &mut ClientTcpPathSessionState,
    carrier_readiness: &mut ClientTcpCarrierReadiness,
    endpoint_generation: u64,
    readiness_rtt: Option<Duration>,
) -> bool {
    runtime
        .state
        .session_lifecycle()
        .commit_if_active(|| {
            runtime
                .endpoint_policy
                .with_current(endpoint_generation, || {
                    let connection = state
                        .connection
                        .as_mut()
                        .expect("TCP readiness requires an actor-owned connection");
                    publish_client_tcp_connection_committed(
                        runtime,
                        connection,
                        readiness_rtt,
                        |path_instance_id, remote_port| {
                            carrier_readiness.publish(path_instance_id, remote_port);
                        },
                    );
                })
        })
        .is_ok_and(|publication| publication.is_some())
}

fn client_tcp_publication_refusal(runtime: &ClientTcpPathSessionRuntime) -> RuntimeError {
    runtime.state.session_lifecycle().reason().map_or(
        RuntimeError::NoSchedulableTcpPath,
        RuntimeError::RemoteClosed,
    )
}

pub(in crate::runtime::path::tcp) fn publish_client_tcp_connection_committed(
    runtime: &ClientTcpPathSessionRuntime,
    connection: &mut ClientTcpPathConnection,
    readiness_rtt: Option<Duration>,
    publish_readiness: impl FnOnce(CarrierPathInstanceId, u16),
) {
    let path_instance_id = connection.path_instance_id;
    let remote_port = connection.carrier.remote_port;
    let mut authenticated_carrier = None;
    runtime.state.publish_tcp_peer_path_usage_committed(
        ClientTcpCarrierPublication {
            path_index: runtime.path_index,
            path_id: runtime.path_id(),
            path_instance_id,
            peer_usage_sequence: connection.carrier.peer_usage_sequence,
            peer_usage: connection.carrier.peer_usage,
            readiness_rtt,
        },
        || {
            authenticated_carrier = Some(runtime.authenticated_carriers.register());
            publish_readiness(path_instance_id, remote_port);
        },
    );
    connection.retain_authenticated_carrier(
        authenticated_carrier.expect("TCP readiness transaction publishes authenticated carrier"),
    );
}

/// Publishes an authenticated successor only if the predecessor still owns
/// the exact member. The shared path-state transaction serializes the swap
/// with every Product load reservation.
pub(in crate::runtime::path::tcp) fn publish_client_tcp_replacement_connection_committed(
    runtime: &ClientTcpPathSessionRuntime,
    connection: &mut ClientTcpPathConnection,
    predecessor_instance_id: CarrierPathInstanceId,
    readiness_rtt: Option<Duration>,
    publish_readiness: impl FnOnce(CarrierPathInstanceId, u16),
) -> bool {
    let path_instance_id = connection.path_instance_id;
    let remote_port = connection.carrier.remote_port;
    let mut authenticated_carrier = None;
    let published = runtime.state.publish_tcp_replacement_if_current(
        predecessor_instance_id,
        ClientTcpCarrierPublication {
            path_index: runtime.path_index,
            path_id: runtime.path_id(),
            path_instance_id,
            peer_usage_sequence: connection.carrier.peer_usage_sequence,
            peer_usage: connection.carrier.peer_usage,
            readiness_rtt,
        },
        || {
            authenticated_carrier = Some(runtime.authenticated_carriers.register());
            publish_readiness(path_instance_id, remote_port);
        },
    );
    if published {
        connection.retain_authenticated_carrier(
            authenticated_carrier
                .expect("TCP replacement transaction publishes authenticated carrier"),
        );
    }
    published
}

fn enabled_endpoint_generation(runtime: &ClientTcpPathSessionRuntime) -> Result<u64, RuntimeError> {
    let policy = runtime.endpoint_policy.snapshot();
    policy
        .enabled
        .then_some(policy.generation)
        .ok_or(RuntimeError::NoSchedulableTcpPath)
}

async fn wait_for_endpoint_policy_change(
    policy: &mut tokio::sync::watch::Receiver<super::super::group::ClientTcpEndpointPolicySnapshot>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_identity_exhaustion_has_no_tcp_path_health_authority() {
        assert!(!client_tcp_establishment_error_has_health_authority(
            &RuntimeError::ExactIdentityExhausted,
        ));
        assert!(client_tcp_establishment_error_has_health_authority(
            &RuntimeError::PathOpenTimedOut,
        ));
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn client_tcp_planned_drain_deadline_bounds_the_complete_actor() {
        let (commands, receivers) =
            crate::runtime::path::commands::reliable_path_command_channels(1);
        let signal = receivers.path_drain_signal();
        let retention = Duration::from_secs(5);
        let requested_at = tokio::time::Instant::now();
        let active_dropped = Arc::new(AtomicBool::new(false));
        let active_drop = DropFlag(active_dropped.clone());
        let active = async move {
            let _active_drop = active_drop;
            std::future::pending::<()>().await;
        };
        let deadline = signal.wait_for_drain_deadline(retention);
        let bounded = run_client_tcp_path_session_until_lifecycle_boundary(
            active,
            std::future::pending(),
            deadline,
        );
        tokio::pin!(bounded);

        commands.begin_path_drain();
        assert_eq!(
            signal.drain_deadline(retention),
            Some(requested_at + retention)
        );
        tokio::time::advance(retention - Duration::from_millis(1)).await;
        tokio::select! {
            biased;
            exit = &mut bounded => panic!("complete client TCP actor exited early: {exit:?}"),
            () = std::future::ready(()) => {}
        }

        commands.begin_path_drain();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(bounded.await, ClientTcpPathActiveExit::DrainDeadline);
        assert!(
            active_dropped.load(Ordering::Acquire),
            "whole-actor deadline did not cancel a blocked inner await"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn client_tcp_terminal_failure_cancels_a_blocked_complete_actor_without_planned_close() {
        let (commands, receivers) =
            crate::runtime::path::commands::reliable_path_command_channels(1);
        let drain_signal = receivers.path_drain_signal();
        let terminal_signal = commands.terminal_signal();
        let active_dropped = Arc::new(AtomicBool::new(false));
        let planned_close_attempted = Arc::new(AtomicBool::new(false));
        let active_drop = DropFlag(active_dropped.clone());
        let active_planned_close = planned_close_attempted.clone();
        let active = async move {
            let _active_drop = active_drop;
            std::future::pending::<()>().await;
            active_planned_close.store(true, Ordering::Release);
        };
        let carrier_failure = async move {
            match terminal_signal.wait().await {
                ReliablePathCarrierTerminalCause::Failed => {}
                ReliablePathCarrierTerminalCause::Retired => std::future::pending::<()>().await,
            }
        };
        let drain_deadline = drain_signal.wait_for_drain_deadline(Duration::from_secs(5));
        let bounded = run_client_tcp_path_session_until_lifecycle_boundary(
            active,
            carrier_failure,
            drain_deadline,
        );
        tokio::pin!(bounded);

        commands.terminate_failed_path();
        assert_eq!(
            bounded.await,
            ClientTcpPathActiveExit::CarrierFailed,
            "direct terminal failure remained hidden behind a blocked inner await"
        );
        assert!(
            active_dropped.load(Ordering::Acquire),
            "terminal failure did not cancel the complete actor future"
        );
        assert!(
            !planned_close_attempted.load(Ordering::Acquire),
            "terminal failure entered the planned PATH_CLOSE continuation"
        );
        assert_eq!(
            drain_signal.drain_started_at(),
            None,
            "terminal failure manufactured a planned-drain deadline"
        );
        assert!(
            !receivers.finish_planned_path_retirement(),
            "terminal failure was relabeled as planned retirement"
        );
    }

    #[test]
    fn authenticated_readiness_warms_only_successor_timing_startup_evidence() {
        let path = "tcp://127.0.0.1:12940?initial-srtt-s=0.75&initial-rttvar-s=0.125&initial-rate-mbps=420"
            .parse::<crate::transport::PathSpec>()
            .expect("TCP path with configured startup priors");
        let path_id = crate::protocol::PathId(37);
        let mut snapshot = path_startup_snapshot(&path, path_id);
        let mut metrics = path_startup_metrics(&path, path_id, PathMetricDirection::ClientToServer);

        apply_authenticated_readiness_to_startup_evidence(
            &mut snapshot,
            &mut metrics,
            Duration::from_millis(42),
        );

        assert_eq!(snapshot.srtt_ms, 42.0);
        assert_eq!(snapshot.jitter_ms, 125.0);
        assert_eq!(snapshot.delivery_rate_bps, 420_000_000.0);
        assert_eq!(metrics.srtt_us, 42_000);
        assert_eq!(metrics.rttvar_us, 125_000);
        assert_eq!(metrics.delivery_rate_bps, 420_000_000);
        assert!(!metrics.rate_observed);
        assert_eq!(metrics.rate_valid_for_us, 0);
        assert!(!metrics.pacing_rate_observed);
        assert!(!metrics.has_ack_derived_data_sample);
        assert!(!metrics.bytes_in_flight_observed);
        assert!(!metrics.queue_observed);
        assert_eq!(metrics.bytes_in_flight, 0);
        assert_eq!(metrics.queue_bytes, 0);
        assert_eq!(metrics.inflight_limit_bytes, 0);
        assert_eq!(metrics.confidence_ppm, 0);
    }
}
