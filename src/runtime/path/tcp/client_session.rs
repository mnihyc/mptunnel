//! Reconnecting reliable TCP actor and connection lifetime orchestration.
//!
//! This owner preserves the biased select loop, carrier generation, and the
//! lifetime coupling between one TCP carrier and all attached product streams.

use super::client_connection::{
    ClientTcpCarrierConnect, ClientTcpHeartbeatTimeoutDisposition, connect_client_tcp_carrier,
};
use super::client_receive::handle_client_tcp_path_frame;
use super::client_state::{ClientTcpPathConnection, ClientTcpPathSessionRuntime};
use super::client_stream::{
    ClientTcpOpenStreamRequest, ClientTcpPathStreamState, expire_client_tcp_pending_opens,
    fail_client_tcp_streams, next_client_tcp_pending_open_deadline,
    open_client_tcp_stream_on_connection,
};
use super::client_writer::handle_connected_client_tcp_command_run;
#[cfg(test)]
use crate::config::SecurityConfig;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
#[cfg(test)]
use crate::mux::MuxLimits;
#[cfg(test)]
use crate::protocol::SessionId;
#[cfg(test)]
use crate::protocol::codec::CodecLimits;
use crate::protocol::{Frame, PathId, PathMetricDirection, StreamId};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ClientTcpOpenResponse, ReliablePathCommand, ReliablePathCommandReceivers,
    recv_reliable_path_command, reliable_path_command_pending_bytes,
    reliable_path_receivers_closed, try_recv_reliable_path_command,
};
use crate::runtime::path::model::{path_startup_metrics, path_startup_snapshot};
use crate::runtime::recent_ids::RecentIdCache;
#[cfg(test)]
use crate::transport::PathSpec;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_CLIENT_TCP_CARRIER_GENERATION: AtomicU64 = AtomicU64::new(1);

struct ClientTcpCarrierGeneration {
    published: Arc<AtomicU64>,
    current: u64,
}

impl ClientTcpCarrierGeneration {
    fn new(published: Arc<AtomicU64>) -> Self {
        published.store(0, Ordering::Release);
        Self {
            published,
            current: 0,
        }
    }

    fn establish(&mut self) {
        let mut generation = NEXT_CLIENT_TCP_CARRIER_GENERATION.fetch_add(1, Ordering::Relaxed);
        if generation == 0 {
            generation = NEXT_CLIENT_TCP_CARRIER_GENERATION.fetch_add(1, Ordering::Relaxed);
        }
        self.current = generation;
        self.published.store(generation, Ordering::Release);
    }

    fn clear(&mut self) {
        self.published.store(0, Ordering::Release);
        self.current = 0;
    }
}

impl Drop for ClientTcpCarrierGeneration {
    fn drop(&mut self) {
        self.clear();
    }
}

struct ClientTcpPathSessionState {
    connection: Option<ClientTcpPathConnection>,
    streams: HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: RecentIdCache<StreamId>,
}

pub(super) async fn run_client_tcp_path_session(
    runtime: ClientTcpPathSessionRuntime,
    mut commands: ReliablePathCommandReceivers,
    carrier_generation: Arc<AtomicU64>,
) {
    let mut carrier_generation = ClientTcpCarrierGeneration::new(carrier_generation);
    let mut state = ClientTcpPathSessionState {
        connection: None,
        streams: HashMap::new(),
        closed_streams: RecentIdCache::new(runtime.closed_stream_cache_capacity),
    };
    let mut pending_frames = Vec::<Frame>::new();

    loop {
        if state.connection.is_none() {
            match recv_reliable_path_command(&mut commands).await {
                Some(command) => {
                    let pending_bytes = reliable_path_command_pending_bytes(&command);
                    handle_disconnected_client_tcp_command(command, &runtime, &mut state).await;
                    if state.connection.is_some() {
                        carrier_generation.establish();
                    }
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
            if let Some(connection_ref) = state.connection.as_mut() {
                let _ = close_client_tcp_path(
                    connection_ref,
                    PathId(runtime.path_index as u16),
                    !state.streams.is_empty(),
                )
                .await;
            }
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
        tokio::select! {
            biased;
            _ = &mut request_probe_cancelled, if request_probe_pending => {
                state
                    .connection
                    .as_mut()
                    .expect("checked connected TCP path session")
                    .capacity
                    .discard_pending_receipt();
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_tcp_capacity_probe",
                    format_args!("phase=discarded reason=cancelled_after_finish path_index={}", runtime.path_index),
                );
            }
            _ = &mut request_probe_timer, if request_probe_pending => {
                state
                    .connection
                    .as_mut()
                    .expect("checked connected TCP path session")
                    .capacity
                    .discard_pending_receipt();
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_tcp_capacity_probe",
                    format_args!("phase=discarded reason=receipt_timeout_after_finish path_index={}", runtime.path_index),
                );
            }
            _ = &mut pending_open_timer, if pending_open_deadline.is_some() => {
                if let Err(err) = expire_client_tcp_pending_opens(
                    state.connection.as_mut().expect("checked connected TCP path session"),
                    &mut state.streams,
                    &mut state.closed_streams,
                ).await {
                    carrier_generation.clear();
                    fail_client_tcp_streams(&mut state.streams, &err);
                    eprintln!("warning: TCP pending stream cleanup failed: {err}");
                    drop_connection = true;
                }
            }
            frame = state.connection.as_mut().expect("checked connected TCP path session").carrier.frames.recv() => {
                match frame {
                    Some(Ok(frame)) => {
                        if let Err(err) = handle_client_tcp_path_frame(
                            frame,
                            state.connection.as_mut().expect("checked connected TCP path session"),
                            &mut state.streams,
                            &mut state.closed_streams,
                            &runtime,
                        )
                        .await
                        {
                            carrier_generation.clear();
                            fail_client_tcp_streams(&mut state.streams, &err);
                            eprintln!("warning: TCP path session frame handling failed: {err}");
                            drop_connection = true;
                        } else if command_may_recv
                            && let Some(command) = try_recv_reliable_path_command(&mut commands)
                        {
                            let result = handle_connected_client_tcp_command_run(
                                command,
                                &mut commands,
                                state
                                    .connection
                                    .as_mut()
                                    .expect("checked connected TCP path session"),
                                &mut state.streams,
                                &mut state.closed_streams,
                                &runtime,
                                carrier_generation.current,
                                runtime.stream_frame_queue,
                                runtime.mux_limits,
                                &mut pending_frames,
                            )
                            .await;
                            if let Err(err) = result {
                                carrier_generation.clear();
                                fail_client_tcp_streams(&mut state.streams, &err);
                                eprintln!("warning: TCP path session command failed: {err}");
                                drop_connection = true;
                            }
                        }
                    }
                    Some(Err(err)) => {
                        let err = RuntimeError::Encrypted(err);
                        carrier_generation.clear();
                        fail_client_tcp_streams(&mut state.streams, &err);
                        eprintln!("warning: TCP path session read failed: {err}");
                        drop_connection = true;
                    }
                    None => {
                        let err = RuntimeError::ReliablePathSessionClosed;
                        carrier_generation.clear();
                        fail_client_tcp_streams(&mut state.streams, &err);
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
                            state.connection.as_mut().expect("checked connected TCP path session"),
                            &mut state.streams,
                            &mut state.closed_streams,
                            &runtime,
                            carrier_generation.current,
                            runtime.stream_frame_queue,
                            runtime.mux_limits,
                            &mut pending_frames,
                        )
                        .await;
                        if let Err(err) = result {
                            carrier_generation.clear();
                            fail_client_tcp_streams(&mut state.streams, &err);
                            eprintln!("warning: TCP path session command failed: {err}");
                            drop_connection = true;
                        }
                    }
                    None => {
                        if reliable_path_receivers_closed(&commands) {
                            if let Some(connection_ref) = state.connection.as_mut() {
                                let _ = close_client_tcp_path(
                                    connection_ref,
                                    PathId(runtime.path_index as u16),
                                    !state.streams.is_empty(),
                                )
                                .await;
                            }
                            return;
                        }
                    }
                }
            }
            _ = &mut heartbeat_timer, if !request_probe_pending => {
                let timeout_disposition = if state.streams.is_empty() {
                    ClientTcpHeartbeatTimeoutDisposition::FailCarrier
                } else {
                    ClientTcpHeartbeatTimeoutDisposition::KeepCarrierAlive
                };
                if let Err(err) = state
                    .connection
                    .as_mut()
                    .expect("checked connected TCP path session")
                    .carrier
                    .tick_heartbeat(timeout_disposition)
                    .await
                {
                    carrier_generation.clear();
                    fail_client_tcp_streams(&mut state.streams, &err);
                    eprintln!("warning: TCP path heartbeat failed: {err}");
                    drop_connection = true;
                }
            }
        }

        if drop_connection {
            carrier_generation.clear();
            state.connection = None;
        }
    }
}

async fn handle_disconnected_client_tcp_command(
    command: ReliablePathCommand,
    runtime: &ClientTcpPathSessionRuntime,
    state: &mut ClientTcpPathSessionState,
) {
    match command {
        ReliablePathCommand::OpenStream {
            stream_id,
            attempt_id,
            observed_carrier_generation: _,
            target,
            ingress,
            lane,
            role,
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
            let connect = connect_client_tcp_path(&runtime, open_deadline);
            tokio::pin!(connect);
            let connect_result = tokio::select! {
                biased;
                _ = response.closed() => return,
                result = &mut connect => result,
            };
            match connect_result {
                Ok(mut connected) => {
                    if response.is_closed() || open_deadline <= tokio::time::Instant::now() {
                        let _ = response.send(ClientTcpOpenResponse::RejectedWithoutOpen(
                            RuntimeError::PathOpenTimedOut,
                        ));
                        return;
                    }
                    let open = ClientTcpOpenStreamRequest {
                        stream_id,
                        attempt_id,
                        target,
                        ingress,
                        lane,
                        role,
                        open_deadline,
                        session_commands,
                        response,
                    };
                    let result = open_client_tcp_stream_on_connection(
                        &mut connected,
                        open,
                        &mut state.streams,
                        runtime.stream_frame_queue,
                    )
                    .await;
                    if result.is_ok() {
                        state.connection = Some(connected);
                    } else if let Err(err) = result {
                        eprintln!(
                            "warning: reliable stream open on new path session failed: {err}"
                        );
                        fail_client_tcp_streams(&mut state.streams, &err);
                    }
                }
                Err(err) => {
                    let _ = response.send(ClientTcpOpenResponse::RejectedWithoutOpen(err));
                }
            }
        }
        ReliablePathCommand::SendQuicCapacityProbe(probe) => {
            probe.ticket.cancel();
        }
        ReliablePathCommand::SendTcpCapacityProbe(_) => {}
        ReliablePathCommand::CancelTcpOpen { .. }
        | ReliablePathCommand::SendFrame(_)
        | ReliablePathCommand::ResetAndCloseStream { .. }
        | ReliablePathCommand::CloseStream(_) => {}
    }
}

async fn connect_client_tcp_path(
    runtime: &ClientTcpPathSessionRuntime,
    open_deadline: tokio::time::Instant,
) -> Result<ClientTcpPathConnection, RuntimeError> {
    let startup_snapshot = path_startup_snapshot(&runtime.path, runtime.path_index);
    let startup_metrics = path_startup_metrics(
        &runtime.path,
        runtime.path_index,
        PathMetricDirection::ClientToServer,
    );
    let carrier = connect_client_tcp_carrier(
        ClientTcpCarrierConnect {
            path: &runtime.path,
            path_index: runtime.path_index,
            config_ordinal: runtime.config_ordinal,
            session_id: runtime.session_id,
            security: &runtime.security,
            codec_limits: runtime.codec_limits,
            mux_limits: runtime.mux_limits,
            carrier_sockets: runtime.carrier_sockets.as_ref(),
        },
        open_deadline,
    )
    .await?;
    Ok(ClientTcpPathConnection::new(
        startup_snapshot,
        startup_metrics,
        carrier,
        runtime.mux_limits,
    ))
}

#[cfg(test)]
pub(in crate::runtime) async fn connect_client_tcp_path_for_test(
    path: &PathSpec,
    path_index: usize,
    session_id: SessionId,
    security: &SecurityConfig,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    open_deadline: tokio::time::Instant,
) -> Result<(), RuntimeError> {
    connect_client_tcp_carrier(
        ClientTcpCarrierConnect {
            path,
            path_index,
            config_ordinal: path_index,
            session_id,
            security,
            codec_limits,
            mux_limits,
            carrier_sockets: &crate::transport::SystemCarrierSocketProvider,
        },
        open_deadline,
    )
    .await
    .map(drop)
}

async fn close_client_tcp_path(
    connection: &mut ClientTcpPathConnection,
    path_id: PathId,
    drain: bool,
) -> Result<(), RuntimeError> {
    if drain {
        connection
            .carrier
            .writer
            .write_frame(&Frame::PathDrain { path_id })
            .await?;
    }
    connection.carrier.close(path_id).await
}
