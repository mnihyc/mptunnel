//! Server QUIC listener, authentication, and stream dispatch.
//!
//! Listener and connection tasks own descendants so carrier shutdown retires
//! the full task tree.

use super::datagram::{ServerUdpDatagramStreamContext, handle_server_udp_datagram_stream};
use super::io::{
    UdpPathConnection, UdpPathEndpoint, UdpPathRecvStream, UdpPathSendStream,
    udp_path_finish_stream, udp_path_read_frame, udp_path_reject_stream, udp_path_write_frame,
    warn_unexpected_udp_runtime_error,
};
use super::metrics::run_server_quic_path_metrics;
use super::server_stream::{ServerUdpReliableStreamContext, handle_server_udp_reliable_stream};
use crate::product::PrincipalPermit;
use crate::protocol::{Frame, PathId, PathUsage, SessionId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::authentication::ServerPathAuthentication;
use crate::runtime::path::server_context::{ServerLocalPath, ServerPathContext};
use crate::runtime::path::{ServerCarrierPathRegistration, ServerLocalPathProperties};
use crate::runtime::peer_status::PeerStatusCarrier;
use crate::scheduler::{TrafficClass, traffic_class_from_stream_demand_hint};
use crate::transport::PathSpec;
use crate::transport::quic::QuicCarrierError;
use tokio::sync::OwnedSemaphorePermit;

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

async fn handle_server_udp_connection(
    connection: UdpPathConnection,
    local_path: ServerLocalPath,
    context: ServerPathContext,
    authentication_slot: OwnedSemaphorePermit,
) -> Result<(), RuntimeError> {
    let (session_id, path_id, peer_usage, principal_permit, control_send, control_recv) =
        accept_server_udp_path_handshake(&connection, &local_path, &context).await?;
    drop(authentication_slot);
    let local_metrics = local_path.startup_metrics(path_id);
    let path_registration = context.reliable_streams.register_carrier_path(
        session_id,
        UnderlayProtocol::Udp,
        path_id,
        ServerLocalPathProperties {
            config_ordinal: local_path.config_ordinal(),
            policy: local_path.policy(),
            initial_metrics: Some(local_metrics),
        },
        principal_permit,
    )?;
    context
        .reliable_streams
        .record_peer_path_usage(&path_registration, 0, peer_usage);
    context
        .reliable_streams
        .record_local_path_metrics(&path_registration, local_metrics, false);
    let peer_status = context.peer_status.register(session_id);
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
                streams.spawn(async move {
                    if let Err(err) = handle_server_udp_bidi_stream(
                        send,
                        recv,
                        context,
                        session_id,
                        path_id,
                        path_registration,
                    )
                    .await
                    {
                        warn_unexpected_udp_runtime_error(
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
    connection.close();
    result
}

async fn accept_server_udp_path_handshake(
    connection: &UdpPathConnection,
    local_path: &ServerLocalPath,
    context: &ServerPathContext,
) -> Result<
    (
        SessionId,
        PathId,
        PathUsage,
        PrincipalPermit,
        UdpPathSendStream,
        UdpPathRecvStream,
    ),
    RuntimeError,
> {
    let admitted = tokio::time::timeout(context.security.authentication_timeout, async {
        // The absolute Product authentication bound starts when the
        // carrier is accepted, not when a source-aware peer eventually
        // chooses to open a matching request stream.
        let (mut send, mut recv) = connection.accept_bi().await?;
        send.set_traffic_class(TrafficClass::Control)?;
        match authenticate_server_udp_path(&mut send, &mut recv, local_path, context).await {
            Ok(admitted) => Ok((admitted, send, recv)),
            Err(err) => {
                let _ = udp_path_reject_stream(&mut send).await;
                Err(err)
            }
        }
    })
    .await
    .map_err(|_| RuntimeError::AuthenticationRejected("authentication timed out"))
    .and_then(|result| result);
    let ((session_id, path_id, peer_usage, principal_permit), send, recv) = admitted?;
    Ok((
        session_id,
        path_id,
        peer_usage,
        principal_permit,
        send,
        recv,
    ))
}

async fn authenticate_server_udp_path(
    send: &mut UdpPathSendStream,
    recv: &mut UdpPathRecvStream,
    local_path: &ServerLocalPath,
    context: &ServerPathContext,
) -> Result<(SessionId, PathId, PathUsage, PrincipalPermit), RuntimeError> {
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
    Ok((session_id, path_id, peer_usage, path_join.principal_permit))
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
                    context.peer_status_snapshot(session_id)
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
            ServerUdpControlEvent::Frame(Ok(Frame::SessionClose { .. })) => return Ok(()),
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

async fn handle_server_udp_bidi_stream(
    mut send: UdpPathSendStream,
    mut recv: UdpPathRecvStream,
    context: ServerPathContext,
    session_id: SessionId,
    path_id: PathId,
    path_registration: ServerCarrierPathRegistration,
) -> Result<(), RuntimeError> {
    match udp_path_read_frame(&mut recv, context.codec_limits).await? {
        Frame::OpenStream {
            stream_id,
            target,
            demand,
            ..
        } => {
            let lane = traffic_class_from_stream_demand_hint(demand);
            send.set_traffic_class(lane)?;
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
                    lane,
                },
            )
            .await
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
                    flow_id,
                    target,
                },
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
