//! Server TCP carrier admission.
//!
//! Authentication and path registration finish before the connection enters
//! the long-lived session actor, keeping unauthenticated sockets out of product
//! and scheduling state.

use super::io::{encrypted_framed_peer_closed, spawn_encrypted_tcp_reader};
use super::metrics::TcpMetricPublisher;
use super::server_evidence::ServerTcpEvidenceState;
use super::server_session::{ServerTcpPathAdmission, ServerTcpPathSession};
use super::server_writer::ServerTcpWriter;
use crate::protocol::auth::{PathJoinAuthCheck, SessionAuthCheck, SessionAuthenticator};
use crate::protocol::{Frame, PathStatus, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::identity::current_unix_secs;
use crate::runtime::path::commands::{
    reliable_path_command_channels, reliable_path_command_queue, reliable_path_writer_frame_queue,
};
use crate::runtime::path::server_context::ServerPathContext;
use crate::transport::encrypted::{EncryptedFramedStream, PeerRole};
use tokio::net::TcpStream;

pub(in crate::runtime) async fn handle_server_path(
    stream: TcpStream,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    let mut tcp_metrics = TcpMetricPublisher::capture(&stream);
    let mut framed = EncryptedFramedStream::with_cipher_suite(
        stream,
        context.security.secret.as_bytes(),
        PeerRole::Server,
        context.codec_limits,
        context.security.cipher,
    )?;
    let session_id = match framed.read_frame().await? {
        Frame::SessionHello { session_id } => session_id,
        _ => return Err(RuntimeError::Protocol("expected SESSION_HELLO")),
    };
    let authenticator = SessionAuthenticator::new(context.security.secret.as_bytes())?;
    let now_unix_secs = current_unix_secs()?;
    let auth_freshness_window_secs = context.security.auth_freshness_window.as_secs();
    match framed.read_frame().await? {
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
        _ => return Err(RuntimeError::Protocol("invalid SESSION_AUTH")),
    }
    let (path_id, path_capabilities) = match framed.read_frame().await? {
        Frame::PathJoin {
            session_id: join_session_id,
            path_id,
            underlay,
            nonce,
            issued_at_unix_secs,
            capabilities,
            auth_tag,
        } if join_session_id == session_id
            && underlay == UnderlayProtocol::Tcp
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
        _ => return Err(RuntimeError::Protocol("invalid PATH_JOIN")),
    };
    let path_registration =
        context
            .reliable_streams
            .register_carrier_path(session_id, UnderlayProtocol::Tcp, path_id);
    let local_metrics = context.local_path_startup_metrics(UnderlayProtocol::Tcp, path_id);
    framed
        .write_frames(&[
            Frame::SessionReady,
            Frame::PathStatus {
                path_id,
                status: PathStatus::Active,
                capabilities: path_capabilities,
            },
        ])
        .await?;
    if let Err(err) = framed.flush().await {
        if encrypted_framed_peer_closed(&err) {
            return Ok(());
        }
        return Err(RuntimeError::Encrypted(err));
    }
    if let Some(metrics) = tcp_metrics.as_mut() {
        metrics.begin_epoch();
    }

    let (reader, writer) = framed.split()?;
    let path_frames =
        spawn_encrypted_tcp_reader(reader, reliable_path_writer_frame_queue(context.mux_limits));
    let (commands_tx, commands_rx) =
        reliable_path_command_channels(reliable_path_command_queue(context.mux_limits));
    let evidence = ServerTcpEvidenceState::new(tcp_metrics, local_metrics, context.mux_limits);
    ServerTcpPathSession::new(ServerTcpPathAdmission {
        context,
        session_id,
        path_id,
        path_capabilities,
        path_registration,
        writer: ServerTcpWriter::new(writer),
        path_frames,
        commands_tx,
        commands_rx,
        evidence,
    })
    .run()
    .await
}
