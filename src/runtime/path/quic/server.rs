//! Server QUIC listener, authentication, and stream dispatch.
//!
//! Listener and connection tasks own descendants so carrier shutdown retires
//! the full task tree.

use super::datagram::{ServerUdpDatagramStreamContext, handle_server_udp_datagram_stream};
use super::io::{
    UdpPathConnection, UdpPathEndpoint, UdpPathRecvStream, UdpPathSendStream,
    udp_path_finish_stream, udp_path_read_frame, udp_path_write_frame,
    warn_unexpected_udp_runtime_error,
};
use super::metrics::run_server_quic_path_metrics;
use super::server_stream::{ServerUdpReliableStreamContext, handle_server_udp_reliable_stream};
use crate::protocol::{Frame, PathCapabilities, PathId, SessionId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ServerCarrierPathRegistration;
use crate::runtime::path::authentication::ServerPathAuthentication;
use crate::runtime::path::server_context::ServerPathContext;
use crate::scheduler::{FlowLane, flow_lane_from_stream_demand_hint};
use crate::transport::PathSpec;

pub(in crate::runtime) async fn bind_server_udp_endpoint(
    path: &PathSpec,
    context: &ServerPathContext,
) -> Result<UdpPathEndpoint, RuntimeError> {
    UdpPathEndpoint::bind_server(path, context).await
}

pub(in crate::runtime) async fn run_server_udp_listener(
    endpoint: UdpPathEndpoint,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = endpoint.accept() => {
                let Some(connection) = accepted else {
                    return Err(RuntimeError::Protocol("QUIC UDP path endpoint closed"));
                };
                let context = context.clone();
                connections.spawn(async move {
                    if let Err(err) = handle_server_udp_connection(connection, context).await {
                        warn_unexpected_udp_runtime_error(
                            "server QUIC UDP path connection failed",
                            &err,
                        );
                    }
                });
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(err) = result {
                    eprintln!("warning: server QUIC UDP path connection task failed: {err}");
                }
            }
        }
    }
}

async fn handle_server_udp_connection(
    connection: UdpPathConnection,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    let (session_id, path_id, capabilities) =
        accept_server_udp_path_handshake(&connection, &context).await?;
    let path_registration =
        context
            .reliable_streams
            .register_carrier_path(session_id, UnderlayProtocol::Udp, path_id);
    let mut streams = tokio::task::JoinSet::new();
    streams.spawn(run_server_quic_path_metrics(
        context.clone(),
        path_registration.clone(),
        connection.clone(),
    ));
    loop {
        tokio::select! {
            accepted = connection.accept_bi() => {
                let (send, recv) = accepted?;
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
                        capabilities,
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
                    eprintln!("warning: server QUIC UDP path stream task failed: {err}");
                }
            }
        }
    }
}

async fn accept_server_udp_path_handshake(
    connection: &UdpPathConnection,
    context: &ServerPathContext,
) -> Result<(SessionId, PathId, PathCapabilities), RuntimeError> {
    let (mut send, mut recv) = connection.accept_bi().await?;
    let authentication = ServerPathAuthentication::from_session_hello(
        &context.security,
        udp_path_read_frame(&mut recv, context.codec_limits).await?,
    )?
    .ok_or(RuntimeError::Protocol(
        "expected QUIC UDP path SESSION_HELLO",
    ))?;
    let authenticated_session = authentication
        .authenticate_session(udp_path_read_frame(&mut recv, context.codec_limits).await?)
        .ok_or(RuntimeError::Protocol("invalid QUIC UDP path SESSION_AUTH"))?;
    let path_join = authenticated_session
        .authenticate_path_join(
            UnderlayProtocol::Udp,
            udp_path_read_frame(&mut recv, context.codec_limits).await?,
        )
        .ok_or(RuntimeError::Protocol("invalid QUIC UDP path PATH_JOIN"))?;
    if !context.accept_path_join_nonce(
        path_join.session_id,
        path_join.path_id,
        UnderlayProtocol::Udp,
        path_join.nonce,
    ) {
        return Err(RuntimeError::Protocol("invalid QUIC UDP path PATH_JOIN"));
    }
    let session_id = path_join.session_id;
    let path_id = path_join.path_id;
    let capabilities = path_join.capabilities;

    udp_path_write_frame(&mut send, &Frame::SessionReady, context.codec_limits).await?;
    udp_path_write_frame(
        &mut send,
        &Frame::PathStatus {
            path_id,
            status: crate::protocol::PathStatus::Active,
            capabilities,
        },
        context.codec_limits,
    )
    .await?;
    udp_path_finish_stream(&mut send)?;
    Ok((session_id, path_id, capabilities))
}

async fn handle_server_udp_bidi_stream(
    mut send: UdpPathSendStream,
    mut recv: UdpPathRecvStream,
    context: ServerPathContext,
    session_id: SessionId,
    path_id: PathId,
    path_registration: ServerCarrierPathRegistration,
    capabilities: PathCapabilities,
) -> Result<(), RuntimeError> {
    match udp_path_read_frame(&mut recv, context.codec_limits).await? {
        Frame::OpenStream {
            stream_id,
            target,
            demand,
            role,
            ..
        } => {
            let lane = flow_lane_from_stream_demand_hint(demand);
            handle_server_udp_reliable_stream(
                send,
                recv,
                context,
                ServerUdpReliableStreamContext {
                    session_id,
                    path_id,
                    path_registration,
                    capabilities,
                    stream_id,
                    target,
                    lane,
                    role,
                },
            )
            .await
        }
        Frame::OpenDatagramFlow {
            flow_id, target, ..
        } => {
            handle_server_udp_datagram_stream(
                send,
                recv,
                context,
                ServerUdpDatagramStreamContext {
                    session_id,
                    flow_id,
                    target,
                    lane: FlowLane::RealtimeDatagram,
                },
            )
            .await
        }
        Frame::Ping { nonce } => {
            udp_path_write_frame(&mut send, &Frame::Pong { nonce }, context.codec_limits).await?;
            udp_path_finish_stream(&mut send)?;
            Ok(())
        }
        _ => Err(RuntimeError::Protocol(
            "unexpected first QUIC UDP path stream frame",
        )),
    }
}
