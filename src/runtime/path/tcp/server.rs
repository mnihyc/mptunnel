//! Server TCP carrier admission.
//!
//! Authentication and path registration finish before the connection enters
//! the long-lived session actor, keeping unauthenticated sockets out of product
//! and scheduling state.

use super::admission::authenticate_prelude;
use super::io::{encrypted_framed_peer_closed, spawn_encrypted_tcp_reader};
use super::metrics::TcpMetricPublisher;
use super::server_evidence::ServerTcpEvidenceState;
use super::server_session::{ServerTcpPathAdmission, ServerTcpPathSession};
use super::server_validation::{ServerTcpValidationAdmission, ServerTcpValidationSession};
use super::server_writer::ServerTcpWriter;
use crate::protocol::{Frame, PathPurpose, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ServerLocalPathProperties;
use crate::runtime::path::commands::{
    reliable_path_command_channels, reliable_path_command_queue, reliable_path_writer_frame_queue,
};
use crate::runtime::path::server_context::{ServerLocalPath, ServerPathContext};
use crate::transport::encrypted::EncryptedFramedStream;
use tokio::net::TcpStream;
use tokio::sync::OwnedSemaphorePermit;

#[cfg(test)]
pub(in crate::runtime) async fn handle_server_path(
    stream: TcpStream,
    local_path: ServerLocalPath,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    if local_path.underlay() != UnderlayProtocol::Tcp {
        return Err(RuntimeError::Protocol(
            "TCP listener received non-TCP local path configuration",
        ));
    }
    let authentication_slot = context.try_begin_authentication()?;
    handle_server_path_with_authentication_slot(stream, local_path, context, authentication_slot)
        .await
}

pub(in crate::runtime) async fn handle_server_path_with_authentication_slot(
    stream: TcpStream,
    local_path: ServerLocalPath,
    context: ServerPathContext,
    authentication_slot: OwnedSemaphorePermit,
) -> Result<(), RuntimeError> {
    if local_path.underlay() != UnderlayProtocol::Tcp {
        return Err(RuntimeError::Protocol(
            "TCP listener received non-TCP local path configuration",
        ));
    }
    let mut tcp_metrics = TcpMetricPublisher::capture(&stream);
    let tls = &context.tls;
    let admitted = tokio::time::timeout(context.security.authentication_timeout, async {
        let mut framed = EncryptedFramedStream::accept(stream, tls, context.codec_limits).await?;
        let tls_exporter = framed.tcp_admission_exporter()?;
        let encoded = framed.read_tcp_admission().await?;
        let Some(authenticated_session) = authenticate_prelude(
            &context.security,
            context.credential_admission.clone(),
            &encoded,
            &tls_exporter,
        )?
        else {
            return Ok(None);
        };
        let path_join = authenticated_session
            .authenticate_path_join(UnderlayProtocol::Tcp, framed.read_frame().await?)?
            .ok_or(RuntimeError::Protocol("invalid PATH_JOIN"))?;
        if !context.accept_path_join_nonce(
            path_join.session_id,
            path_join.credential_id.clone(),
            path_join.path_id,
            UnderlayProtocol::Tcp,
            path_join.nonce,
            path_join.issued_at_unix_secs,
            path_join.verified_at_unix_secs,
        ) {
            return Err(RuntimeError::Protocol("invalid PATH_JOIN"));
        }
        let peer_usage = match framed.read_frame().await? {
            Frame::PathStatus {
                path_id: status_path_id,
                sequence: 0,
                usage,
            } if status_path_id == path_join.path_id => usage,
            _ => {
                return Err(RuntimeError::Protocol(
                    "invalid client TCP path usage advertisement",
                ));
            }
        };
        Ok::<_, RuntimeError>(Some((framed, path_join, peer_usage)))
    })
    .await
    .map_err(|_| RuntimeError::AuthenticationRejected("authentication timed out"))??;
    drop(authentication_slot);
    let Some((mut framed, path_join, peer_usage)) = admitted else {
        return Ok(());
    };
    let session_id = path_join.session_id;
    let path_id = path_join.path_id;
    let local_metrics = local_path.startup_metrics(path_id);
    let local_properties = ServerLocalPathProperties {
        config_ordinal: local_path.config_ordinal(),
        policy: local_path.policy(),
        initial_metrics: Some(local_metrics),
    };
    let path_registration = match path_join.purpose {
        PathPurpose::Ordinary => context.reliable_streams.register_carrier_path(
            session_id,
            UnderlayProtocol::Tcp,
            path_id,
            local_properties,
            path_join.principal_permit,
        )?,
        PathPurpose::Validation => context.reliable_streams.register_validation_carrier_path(
            session_id,
            path_id,
            local_properties,
            path_join.principal_permit,
        )?,
    };
    context
        .reliable_streams
        .record_peer_path_usage(&path_registration, 0, peer_usage);
    context
        .reliable_streams
        .record_local_path_metrics(&path_registration, local_metrics, false);
    let local_usage = local_path.advertised_usage();
    framed
        .write_frames(&[
            Frame::SessionReady,
            Frame::PathStatus {
                path_id,
                sequence: 0,
                usage: local_usage,
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
    let evidence =
        ServerTcpEvidenceState::new(tcp_metrics, Some(local_metrics), context.mux_limits);
    let peer_status = context.peer_status.register(session_id);
    match path_join.purpose {
        PathPurpose::Ordinary => {
            let (commands_tx, commands_rx) =
                reliable_path_command_channels(reliable_path_command_queue(context.mux_limits));
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
                peer_status,
            })
            .run()
            .await
        }
        PathPurpose::Validation => {
            ServerTcpValidationSession::new(ServerTcpValidationAdmission {
                context,
                session_id,
                path_id,
                path_registration,
                writer: ServerTcpWriter::new(writer),
                path_frames,
                evidence,
                peer_status,
            })
            .run()
            .await
        }
    }
}
