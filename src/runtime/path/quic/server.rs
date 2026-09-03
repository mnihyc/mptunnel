//! Server QUIC listener, authentication, and stream dispatch.
//!
//! Listener and connection tasks own descendants so carrier shutdown retires
//! the full task tree.

use super::datagram::{ServerUdpDatagramStreamContext, handle_server_udp_datagram_stream};
use super::io::{
    UdpPathConnection, UdpPathEndpoint, UdpPathRecvStream, UdpPathSendStream,
    udp_path_finish_stream, udp_path_read_frame, udp_path_reject_stream, udp_path_write_frame,
    warn_unexpected_udp_operation_error, warn_unexpected_udp_runtime_error,
};
use super::ip_tunnel::handle_server_udp_ip_tunnel;
use super::metrics::run_server_quic_path_metrics;
use super::server_stream::{ServerUdpReliableStreamContext, handle_server_udp_reliable_stream};
use crate::protocol::{
    Frame, PathId, PathMetricDirection, PeerPathState, SessionId, UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::authentication::ServerPathAuthentication;
use crate::runtime::path::server_context::{ServerLocalPath, ServerPathContext};
use crate::runtime::path::{
    ServerCarrierPathRegistration, ServerCarrierPeer, ServerLocalPathProperties,
    fence_server_carrier_readiness,
};
use crate::runtime::peer_status::PeerStatusCarrier;
use crate::scheduler::TrafficClass;
use crate::transport::PathSpec;
use crate::transport::quic::QuicCarrierError;
use tokio::sync::OwnedSemaphorePermit;

#[cfg(test)]
pub(in crate::runtime) use super::server_stream::arm_server_udp_stream_abort_for_test;

pub(in crate::runtime) async fn bind_server_udp_endpoint(
    path: &PathSpec,
    context: &ServerPathContext,
) -> Result<UdpPathEndpoint, RuntimeError> {
    UdpPathEndpoint::bind_server(path, context).await
}

pub(in crate::runtime) async fn run_server_udp_listener(
    endpoint: UdpPathEndpoint,
    local_path: ServerLocalPath,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    if local_path.underlay() != UnderlayProtocol::Udp {
        return Err(RuntimeError::Protocol(
            "QUIC listener received non-UDP local path configuration",
        ));
    }
    let mut connections = tokio::task::JoinSet::new();
    loop {
        // Completed children retain their JoinSet slots until observed.
        // Reap them before admitting another connection so a continuously
        // readable listener cannot accumulate completed task outputs.
        while let Some(result) = connections.try_join_next() {
            if let Err(err) = result {
                crate::observability::process_event!(
                    Warn,
                    "quic",
                    "server_connection_task_failed",
                    "server QUIC UDP path connection task failed: {err}"
                );
            }
        }
        tokio::select! {
            accepted = endpoint.accept() => {
                let Some(connection) = accepted else {
                    return Err(RuntimeError::Protocol("QUIC UDP path endpoint closed"));
                };
                let authentication_slot = match context.try_begin_authentication() {
                    Ok(permit) => permit,
                    Err(_) => {
                        connection.close();
                        continue;
                    }
                };
                let context = context.clone();
                let local_path = local_path.clone();
                connections.spawn(async move {
                    if let Err(err) =
                        handle_server_udp_connection(
                            connection,
                            local_path,
                            context,
                            authentication_slot,
                        ).await
                    {
                        warn_unexpected_udp_runtime_error(
                            "server QUIC UDP path connection failed",
                            &err,
                        );
                    }
                });
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(err) = result {
                    crate::observability::process_event!(
                        Warn,
                        "quic",
                        "server_connection_task_failed",
                        "server QUIC UDP path connection task failed: {err}"
                    );
                }
            }
        }
    }
}

struct ServerUdpConnectionCloseGuard<'a> {
    connection: &'a UdpPathConnection,
}

impl Drop for ServerUdpConnectionCloseGuard<'_> {
    fn drop(&mut self) {
        self.connection.close();
    }
}

async fn handle_server_udp_connection(
    connection: UdpPathConnection,
    local_path: ServerLocalPath,
    context: ServerPathContext,
    authentication_slot: OwnedSemaphorePermit,
) -> Result<(), RuntimeError> {
    // The Native authority publisher retains a connection clone. Explicitly
    // close on every return *and cancellation* of this owner task so a failed
    // or aborted pre-ready lifetime cannot leave that publisher detached.
    let _connection_close_guard = ServerUdpConnectionCloseGuard {
        connection: &connection,
    };
    let (path_registration, control_send, control_recv) =
        match accept_server_udp_path_handshake(&connection, &local_path, &context).await {
            Ok(handshake) => handshake,
            Err(error) => {
                // Native authority binding can spawn a connection-owned
                // publisher before the readiness transaction completes. An
                // explicit close makes every failed pre-ready lifetime
                // terminal so that publisher cannot retain the connection.
                connection.close();
                return Err(error);
            }
        };
    drop(authentication_slot);
    let session_id = path_registration.session_id();
    let path_id = path_registration.path_id();
    let Some(native_rate_authority) = connection.native_rate_authority() else {
        connection.close();
        return Err(RuntimeError::Protocol(
            "server QUIC connection admitted before native rate authority binding",
        ));
    };
    let peer_status = context.register_peer_status(&path_registration);
    let control = run_server_udp_control_stream(
        control_send,
        control_recv,
        peer_status,
        context.clone(),
        session_id,
    );
    tokio::pin!(control);
    let mut control_active = true;
    let mut streams = tokio::task::JoinSet::new();
    streams.spawn(run_server_quic_path_metrics(
        context.clone(),
        path_registration.clone(),
        connection.clone(),
    ));
    let retirement =
        context.wait_for_credential_retirement(path_registration.principal_permit().clone());
    tokio::pin!(retirement);
    let session_retirement = path_registration.session_retirement().wait();
    tokio::pin!(session_retirement);
    let result = loop {
        // QUIC stream credit bounds live children. Eagerly reaping completed
        // actors also bounds retained JoinSet output under sustained churn.
        while let Some(result) = streams.try_join_next() {
            if let Err(err) = result {
                crate::observability::process_event!(
                    Warn,
                    "quic",
                    "server_stream_task_failed",
                    "server QUIC UDP path stream task failed: {err}"
                );
            }
        }
        tokio::select! {
            biased;
            reason = &mut session_retirement => {
                break Err(RuntimeError::RemoteClosed(reason));
            }
            () = &mut retirement => {
                break Ok(());
            }
            result = &mut control, if control_active => {
                match result {
                    Ok(()) => control_active = false,
                    Err(err) => {
                        #[cfg(feature = "lab-diagnostics")]
                        crate::lab_diagnostics::lab_diagnostic(
                            "server_quic_connection_loop_exit",
                            format_args!(
                                "session_id={} path_id={} cause=control error={}",
                                session_id.0,
                                path_id.0,
                                err,
                            ),
                        );
                        break Err(err);
                    }
                }
            }
            accepted = connection.accept_bi() => {
                let (send, recv) = match accepted {
                    Ok(stream) => stream,
                    Err(err) => {
                        #[cfg(feature = "lab-diagnostics")]
                        crate::lab_diagnostics::lab_diagnostic(
                            "server_quic_connection_loop_exit",
                            format_args!(
                                "session_id={} path_id={} cause=accept error={}",
                                session_id.0,
                                path_id.0,
                                err,
                            ),
                        );
                        break Err(err);
                    }
                };
                let context = context.clone();
                let path_registration = path_registration.clone();
                let native_rate_authority = native_rate_authority.clone();
                streams.spawn(async move {
                    if let Err(err) = handle_server_udp_bidi_stream_with_native_rate_authority(
                        send,
                        recv,
                        context,
                        session_id,
                        path_id,
                        path_registration,
                        native_rate_authority,
                    )
                    .await
                    {
                        warn_unexpected_udp_operation_error(
                            "server QUIC UDP path stream failed",
                            &err,
                        );
                    }
                });
            }
            Some(result) = streams.join_next(), if !streams.is_empty() => {
                if let Err(err) = result {
                    crate::observability::process_event!(
                        Warn,
                        "quic",
                        "server_stream_task_failed",
                        "server QUIC UDP path stream task failed: {err}"
                    );
                }
            }
        }
    };
    retire_server_udp_connection(&connection, &mut streams, &path_registration).await;
    result
}

async fn retire_server_udp_connection(
    connection: &UdpPathConnection,
    streams: &mut tokio::task::JoinSet<()>,
    path_registration: &ServerCarrierPathRegistration,
) {
    // Stop new selection first, then terminate every native child before the
    // registry snapshots this exact carrier's remaining attachments.
    path_registration.set_state(PeerPathState::Draining);
    connection.close();
    streams.shutdown().await;
    path_registration.begin_retirement().wait().await;
}

async fn accept_server_udp_path_handshake(
    connection: &UdpPathConnection,
    local_path: &ServerLocalPath,
    context: &ServerPathContext,
) -> Result<
    (
        ServerCarrierPathRegistration,
        UdpPathSendStream,
        UdpPathRecvStream,
    ),
    RuntimeError,
> {
    tokio::time::timeout(context.security.authentication_timeout, async {
        // The absolute Product authentication bound starts when the
        // carrier is accepted, not when a source-aware peer eventually
        // chooses to open a matching request stream.
        let (mut send, mut recv) = connection.accept_bi().await?;
        send.set_traffic_class(TrafficClass::Control)?;
        match admit_server_udp_path(connection, &mut send, &mut recv, local_path, context).await {
            Ok(registration) => Ok((registration, send, recv)),
            Err(err) => {
                let _ = udp_path_reject_stream(&mut send).await;
                Err(err)
            }
        }
    })
    .await
    .map_err(|_| RuntimeError::AuthenticationRejected("authentication timed out"))
    .and_then(|result| result)
}

#[cfg(test)]
pub(in crate::runtime) async fn accept_server_udp_path_handshake_for_test(
    connection: &UdpPathConnection,
    local_path: &ServerLocalPath,
    context: &ServerPathContext,
) -> Result<
    (
        ServerCarrierPathRegistration,
        UdpPathSendStream,
        UdpPathRecvStream,
    ),
    RuntimeError,
> {
    accept_server_udp_path_handshake(connection, local_path, context).await
}

async fn admit_server_udp_path(
    connection: &UdpPathConnection,
    send: &mut UdpPathSendStream,
    recv: &mut UdpPathRecvStream,
    local_path: &ServerLocalPath,
    context: &ServerPathContext,
) -> Result<ServerCarrierPathRegistration, RuntimeError> {
    let authentication = ServerPathAuthentication::from_session_hello(
        &context.security,
        context.credential_admission.clone(),
        udp_path_read_frame(recv, context.codec_limits).await?,
    )?
    .ok_or(RuntimeError::Protocol(
        "expected QUIC UDP path SESSION_HELLO",
    ))?;
    let authenticated_session = authentication
        .authenticate_session(udp_path_read_frame(recv, context.codec_limits).await?)?
        .ok_or(RuntimeError::Protocol("invalid QUIC UDP path SESSION_AUTH"))?;
    let path_join = authenticated_session
        .authenticate_path_join(
            UnderlayProtocol::Udp,
            udp_path_read_frame(recv, context.codec_limits).await?,
        )?
        .ok_or(RuntimeError::Protocol("invalid QUIC UDP path PATH_JOIN"))?;
    if !context.accept_path_join_nonce(
        path_join.session_id,
        path_join.credential_id.clone(),
        path_join.path_id,
        UnderlayProtocol::Udp,
        path_join.nonce,
        path_join.issued_at_unix_secs,
        path_join.verified_at_unix_secs,
    ) {
        return Err(RuntimeError::Protocol("invalid QUIC UDP path PATH_JOIN"));
    }
    let session_id = path_join.session_id;
    let path_id = path_join.path_id;
    let peer_usage = match udp_path_read_frame(recv, context.codec_limits).await? {
        Frame::PathStatus {
            path_id: status_path_id,
            sequence: 0,
            usage,
        } if status_path_id == path_id => usage,
        _ => {
            return Err(RuntimeError::Protocol(
                "invalid client QUIC path usage advertisement",
            ));
        }
    };
    let local_usage = local_path.advertised_usage();
    let local_metrics = local_path.startup_metrics(path_id);
    let observed = connection.clone();
    let path_registration = context
        .reliable_streams
        .register_carrier_path_with_observed_peer_and_authority(
            session_id,
            UnderlayProtocol::Udp,
            path_id,
            ServerLocalPathProperties {
                config_ordinal: local_path.config_ordinal(),
                policy: local_path.policy(),
                initial_metrics: Some(local_metrics),
            },
            peer_usage,
            connection.native_capacity_epoch(),
            path_join.principal_permit,
            ServerCarrierPeer::observed(move || observed.remote_address()),
            context.configured_path_name(local_path.config_ordinal()),
        )?;
    let native_scope = crate::model::carrier_rate_authority::CarrierRateAuthorityScope::new(
        path_registration.path_instance_id(),
        PathMetricDirection::ServerToClient,
    );
    let native_authority = connection
        .bind_native_rate_authority(native_scope, local_metrics.delivery_rate_bps)
        .await
        .map_err(|_| RuntimeError::Protocol("failed to bind server QUIC native rate authority"))?;
    let initial_native_shape = stage_current_server_native_scheduling_shape(
        &context,
        &path_registration,
        &native_authority,
        native_scope,
    )
    .await?;
    context
        .reliable_streams
        .fanout_native_scheduling_shape(&path_registration, initial_native_shape);
    fence_server_carrier_readiness(path_registration.session_retirement(), async {
        context.reliable_streams.record_local_path_metrics(
            &path_registration,
            local_metrics,
            false,
        );
        udp_path_write_frame(send, &Frame::SessionReady, context.codec_limits).await?;
        udp_path_write_frame(
            send,
            &Frame::PathStatus {
                path_id,
                sequence: 0,
                usage: local_usage,
            },
            context.codec_limits,
        )
        .await?;
        Ok(())
    })
    .await?;
    Ok(path_registration)
}

async fn stage_current_server_native_scheduling_shape(
    context: &ServerPathContext,
    path_registration: &ServerCarrierPathRegistration,
    native_authority: &std::sync::Arc<
        crate::runtime::path::authority::NativeCarrierRateAuthorityHandle,
    >,
    native_scope: crate::model::carrier_rate_authority::CarrierRateAuthorityScope,
) -> Result<crate::runtime::path::authority::NativeCarrierSchedulingShapeSnapshot, RuntimeError> {
    loop {
        let shape = match native_authority.refresh_scheduling_shape(native_scope) {
            Ok(shape) => shape,
            Err(error) if error.is_retryable_publication() => {
                tokio::task::yield_now().await;
                continue;
            }
            Err(_) => {
                return Err(RuntimeError::Protocol(
                    "failed to read server QUIC native scheduling shape",
                ));
            }
        };
        match native_authority.commit_if_current(shape.stamp(), || {
            context
                .reliable_streams
                .stage_native_scheduling_shape(path_registration, shape)
        }) {
            Ok(_) => return Ok(shape),
            Err(error) if error.is_retryable_publication() => {
                tokio::task::yield_now().await;
            }
            Err(_) => {
                return Err(RuntimeError::Protocol(
                    "server QUIC native scheduling shape failed before readiness",
                ));
            }
        }
    }
}

enum ServerUdpControlEvent {
    Frame(Result<Frame, RuntimeError>),
    Request(Option<u64>),
}

async fn run_server_udp_control_stream(
    mut send: UdpPathSendStream,
    mut recv: UdpPathRecvStream,
    mut peer_status: PeerStatusCarrier,
    context: ServerPathContext,
    session_id: SessionId,
) -> Result<(), RuntimeError> {
    loop {
        let event = tokio::select! {
            frame = udp_path_read_frame(&mut recv, context.codec_limits) => {
                ServerUdpControlEvent::Frame(frame)
            }
            request_id = peer_status.recv_request() => {
                ServerUdpControlEvent::Request(request_id)
            }
        };
        let outgoing = match event {
            ServerUdpControlEvent::Frame(Ok(Frame::PeerStatusRequest { request_id })) => Some(
                peer_status.response_frame(request_id, context.codec_limits, || {
                    Some(context.peer_status_snapshot(session_id))
                }),
            ),
            ServerUdpControlEvent::Frame(Ok(Frame::PeerStatusResponse {
                request_id,
                code,
                paths,
            })) => {
                let _ = peer_status.receive_response(request_id, code, paths);
                None
            }
            ServerUdpControlEvent::Frame(Ok(Frame::SessionClose { reason })) => {
                context.retire_session(session_id, reason);
                return Err(RuntimeError::RemoteClosed(reason));
            }
            ServerUdpControlEvent::Frame(Ok(_)) => {
                return Err(RuntimeError::Protocol(
                    "unexpected QUIC UDP control stream frame",
                ));
            }
            // Pre-control peers finish the handshake stream; keep their product
            // connection usable and simply withdraw this diagnostic carrier.
            ServerUdpControlEvent::Frame(Err(RuntimeError::QuicCarrier(
                QuicCarrierError::StreamFinished,
            ))) => return Ok(()),
            ServerUdpControlEvent::Frame(Err(err)) => return Err(err),
            ServerUdpControlEvent::Request(Some(request_id)) => {
                Some(Frame::PeerStatusRequest { request_id })
            }
            ServerUdpControlEvent::Request(None) => return Ok(()),
        };
        if let Some(frame) = outgoing {
            udp_path_write_frame(&mut send, &frame, context.codec_limits).await?;
        }
    }
}

async fn handle_server_udp_bidi_stream_with_native_rate_authority(
    send: UdpPathSendStream,
    recv: UdpPathRecvStream,
    context: ServerPathContext,
    session_id: SessionId,
    path_id: PathId,
    path_registration: ServerCarrierPathRegistration,
    native_rate_authority: std::sync::Arc<
        crate::runtime::path::authority::NativeCarrierRateAuthorityHandle,
    >,
) -> Result<(), RuntimeError> {
    handle_server_udp_bidi_stream_inner(
        send,
        recv,
        context,
        session_id,
        path_id,
        path_registration,
        Some(native_rate_authority),
    )
    .await
}

#[cfg(test)]
pub(super) async fn handle_server_udp_bidi_stream(
    send: UdpPathSendStream,
    recv: UdpPathRecvStream,
    context: ServerPathContext,
    session_id: SessionId,
    path_id: PathId,
    path_registration: ServerCarrierPathRegistration,
) -> Result<(), RuntimeError> {
    handle_server_udp_bidi_stream_inner(
        send,
        recv,
        context,
        session_id,
        path_id,
        path_registration,
        None,
    )
    .await
}

async fn handle_server_udp_bidi_stream_inner(
    mut send: UdpPathSendStream,
    mut recv: UdpPathRecvStream,
    context: ServerPathContext,
    session_id: SessionId,
    path_id: PathId,
    path_registration: ServerCarrierPathRegistration,
    native_rate_authority: Option<
        std::sync::Arc<crate::runtime::path::authority::NativeCarrierRateAuthorityHandle>,
    >,
) -> Result<(), RuntimeError> {
    match udp_path_read_frame(&mut recv, context.codec_limits).await? {
        Frame::OpenStream { stream_id, .. }
            if context.forwarding_mode == crate::config::ForwardingMode::L3 =>
        {
            send.set_traffic_class(TrafficClass::Control)?;
            udp_path_write_frame(
                &mut send,
                &Frame::StreamDetach { stream_id },
                context.codec_limits,
            )
            .await?;
            udp_path_finish_stream(&mut send).await?;
            Ok(())
        }
        Frame::OpenStream {
            stream_id,
            target,
            demand,
            return_plan,
        } => {
            handle_server_udp_reliable_stream(
                send,
                recv,
                context,
                ServerUdpReliableStreamContext {
                    session_id,
                    path_id,
                    path_registration,
                    stream_id,
                    target,
                    initial_demand: demand,
                    return_plan,
                    native_rate_authority,
                },
            )
            .await
        }
        Frame::OpenDatagramFlow { flow_id, .. }
            if context.forwarding_mode == crate::config::ForwardingMode::L3 =>
        {
            send.set_traffic_class(TrafficClass::Control)?;
            udp_path_write_frame(
                &mut send,
                &Frame::DatagramClose { flow_id },
                context.codec_limits,
            )
            .await?;
            udp_path_finish_stream(&mut send).await?;
            Ok(())
        }
        Frame::OpenDatagramFlow {
            flow_id, target, ..
        } => {
            send.set_traffic_class(TrafficClass::RealtimeDatagram)?;
            handle_server_udp_datagram_stream(
                send,
                recv,
                context,
                ServerUdpDatagramStreamContext {
                    session_id,
                    principal_permit: path_registration.principal_permit().clone(),
                    ingress: path_registration.mpp_ingress_observer().ok_or(
                        RuntimeError::Protocol(
                            "active QUIC carrier is missing its authenticated socket peer",
                        ),
                    )?,
                    flow_id,
                    target,
                },
            )
            .await
        }
        Frame::OpenIpTunnel { tunnel_id } => {
            let native_rate_authority = native_rate_authority.ok_or(RuntimeError::Protocol(
                "server QUIC IP tunnel stream missing native rate authority",
            ))?;
            handle_server_udp_ip_tunnel(
                send,
                recv,
                context,
                path_registration,
                tunnel_id,
                native_rate_authority,
            )
            .await
        }
        Frame::Ping { nonce } => {
            send.set_traffic_class(TrafficClass::Control)?;
            udp_path_write_frame(&mut send, &Frame::Pong { nonce }, context.codec_limits).await?;
            udp_path_finish_stream(&mut send).await?;
            Ok(())
        }
        _ => Err(RuntimeError::Protocol(
            "unexpected first QUIC UDP path stream frame",
        )),
    }
}

#[cfg(test)]
#[path = "tests_server.rs"]
mod tests;
