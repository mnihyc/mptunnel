//! Server QUIC listener, authentication, and stream dispatch.

use super::datagram::*;
use super::io::*;
use super::metrics::*;
use super::server_stream::*;
use super::*;
use crate::scheduler::flow_lane_from_stream_demand_hint;

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
    loop {
        let Some(connection) = endpoint.accept().await else {
            return Err(RuntimeError::Protocol("QUIC UDP path endpoint closed"));
        };
        let context = context.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_server_udp_connection(connection, context).await {
                warn_unexpected_udp_runtime_error("server QUIC UDP path connection failed", &err);
            }
        });
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
    spawn_server_quic_path_metrics(
        context.clone(),
        path_registration.clone(),
        connection.clone(),
    );
    loop {
        let (send, recv) = match connection.accept_bi().await {
            Ok(streams) => streams,
            Err(err) => return Err(err),
        };
        let context = context.clone();
        let path_registration = path_registration.clone();
        tokio::spawn(async move {
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
                warn_unexpected_udp_runtime_error("server QUIC UDP path stream failed", &err);
            }
        });
    }
}

async fn accept_server_udp_path_handshake(
    connection: &UdpPathConnection,
    context: &ServerPathContext,
) -> Result<(SessionId, PathId, PathCapabilities), RuntimeError> {
    let (mut send, mut recv) = connection.accept_bi().await?;
    let session_id = match udp_path_read_frame(&mut recv, context.codec_limits).await? {
        Frame::SessionHello { session_id } => session_id,
        _ => {
            return Err(RuntimeError::Protocol(
                "expected QUIC UDP path SESSION_HELLO",
            ));
        }
    };
    let authenticator = SessionAuthenticator::new(context.security.secret.as_bytes())?;
    let now_unix_secs = current_unix_secs()?;
    let auth_freshness_window_secs = context.security.auth_freshness_window.as_secs();
    match udp_path_read_frame(&mut recv, context.codec_limits).await? {
        Frame::SessionAuth {
            session_id: auth_session_id,
            nonce,
            issued_at_unix_secs,
            auth_tag,
        } if auth_session_id == session_id
            && authenticator.verify_session_auth(SessionAuthCheck {
                session_id,
                nonce,
                issued_at_unix_secs,
                tag: auth_tag,
                now_unix_secs,
                freshness_window_secs: auth_freshness_window_secs,
            }) => {}
        _ => return Err(RuntimeError::Protocol("invalid QUIC UDP path SESSION_AUTH")),
    }
    let (path_id, capabilities) = match udp_path_read_frame(&mut recv, context.codec_limits).await?
    {
        Frame::PathJoin {
            session_id: join_session_id,
            path_id,
            underlay,
            nonce,
            issued_at_unix_secs,
            capabilities,
            auth_tag,
        } if join_session_id == session_id
            && underlay == UnderlayProtocol::Udp
            && authenticator.verify_path_join(PathJoinAuthCheck {
                session_id,
                path_id,
                underlay,
                nonce,
                issued_at_unix_secs,
                capabilities,
                tag: auth_tag,
                now_unix_secs,
                freshness_window_secs: auth_freshness_window_secs,
            })
            && context.accept_path_join_nonce(session_id, path_id, underlay, nonce) =>
        {
            (path_id, capabilities)
        }
        _ => return Err(RuntimeError::Protocol("invalid QUIC UDP path PATH_JOIN")),
    };

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
