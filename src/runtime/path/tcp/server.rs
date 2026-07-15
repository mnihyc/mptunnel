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
use crate::protocol::{Frame, PathStatus, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::authentication::ServerPathAuthentication;
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
    let authentication = ServerPathAuthentication::from_session_hello(
        &context.security,
        framed.read_frame().await?,
    )?
    .ok_or(RuntimeError::Protocol("expected SESSION_HELLO"))?;
    let authenticated_session = authentication
        .authenticate_session(framed.read_frame().await?)
        .ok_or(RuntimeError::Protocol("invalid SESSION_AUTH"))?;
    let path_join = authenticated_session
        .authenticate_path_join(UnderlayProtocol::Tcp, framed.read_frame().await?)
        .ok_or(RuntimeError::Protocol("invalid PATH_JOIN"))?;
    if !context.accept_path_join_nonce(
        path_join.session_id,
        path_join.path_id,
        UnderlayProtocol::Tcp,
        path_join.nonce,
    ) {
        return Err(RuntimeError::Protocol("invalid PATH_JOIN"));
    }
    let session_id = path_join.session_id;
    let path_id = path_join.path_id;
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
