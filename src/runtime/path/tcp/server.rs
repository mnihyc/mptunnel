//! Server TCP carrier admission.
//!
//! Authentication and path registration finish before the connection enters
//! the long-lived session actor, keeping unauthenticated sockets out of product
//! and scheduling state.

mod datagram;
mod evidence;
mod ip_tunnel;
mod session;
mod stream;
mod writer;

use self::evidence::ServerTcpEvidenceState;
use self::session::{ServerTcpPathAdmission, ServerTcpPathSession};
use self::writer::ServerTcpWriter;
use super::admission::authenticate_prelude;
use super::io::{encrypted_framed_peer_closed, spawn_encrypted_tcp_reader_with_terminal_result};
use super::metrics::TcpMetricPublisher;
use crate::protocol::{Frame, PeerPathState, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    reliable_path_command_channels, reliable_path_command_queue, reliable_path_writer_frame_queue,
};
use crate::runtime::path::server_context::{ServerLocalPath, ServerPathContext};
use crate::runtime::path::{
    ServerCarrierPathStateHandle, ServerCarrierPeer, ServerLocalPathProperties,
    fence_server_carrier_readiness,
};
use crate::transport::encrypted::{EncryptedFramedStream, ServerEncryptedStreamAdmission};
use tokio::net::TcpStream;
use tokio::sync::OwnedSemaphorePermit;

fn tcp_carrier_peer(stream: &TcpStream) -> Result<ServerCarrierPeer, std::io::Error> {
    stream.peer_addr().map(ServerCarrierPeer::fixed)
}

/// Publishes exact-path drain intent at the first authenticated decode boundary.
///
/// The bounded actor queue remains the sole ordered frame consumer. This early
/// publication only closes fresh Product admission and starts the immutable
/// carrier drain clock; the actor must still consume PATH_DRAIN in order before
/// it may emit a successful PATH_CLOSE.
fn observe_authenticated_server_tcp_frame(
    frame: &Frame,
    session_id: crate::protocol::SessionId,
    path_id: crate::protocol::PathId,
    commands: &crate::runtime::path::commands::ReliablePathCommandSender,
    path_state: &ServerCarrierPathStateHandle,
    context: &ServerPathContext,
) {
    match frame {
        Frame::PathDrain {
            path_id: drain_path_id,
        } if *drain_path_id == path_id => {
            commands.begin_path_drain();
            path_state.set_state(PeerPathState::Draining);
        }
        // Session retirement is session-wide negative authority. Publish it at
        // the authenticated decode boundary so a following native EOF cannot
        // preempt the retained close reason behind a full actor queue.
        Frame::SessionClose { reason } => context.retire_session(session_id, *reason),
        _ => {}
    }
}

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
    let peer = tcp_carrier_peer(&stream)?;
    let mut tcp_metrics = TcpMetricPublisher::capture(&stream);
    let tls = &context.tls;
    let authentication_deadline =
        tokio::time::Instant::now() + context.security.authentication_timeout;
    let mut authentication_slot = Some(authentication_slot);
    let transport_admission = tokio::time::timeout_at(
        authentication_deadline,
        EncryptedFramedStream::accept_for_server_authentication(stream, tls, context.codec_limits),
    )
    .await
    .map_err(|_| RuntimeError::AuthenticationRejected("authentication timed out"))??;
    let mut framed = match transport_admission {
        ServerEncryptedStreamAdmission::Accepted(framed) => framed,
        ServerEncryptedStreamAdmission::Rejected(rejected) => {
            drop(authentication_slot.take());
            drop(tcp_metrics.take());
            if let Some(_retention_slot) = context.try_retain_silent_rejection() {
                tokio::time::sleep_until(authentication_deadline).await;
            }
            drop(rejected);
            return Ok(());
        }
    };
    let admitted = tokio::time::timeout_at(authentication_deadline, async {
        let transport_binding = framed.tcp_admission_binding()?;
        let encoded = framed.read_tcp_admission().await?;
        let Some(authenticated_session) = authenticate_prelude(
            &context.security,
            context.credential_admission.clone(),
            &encoded,
            &transport_binding,
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
    drop(authentication_slot.take());
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
    let path_registration = context
        .reliable_streams
        .register_carrier_path_with_observed_peer_and_authority(
            session_id,
            UnderlayProtocol::Tcp,
            path_id,
            local_properties,
            peer_usage,
            0,
            path_join.principal_permit,
            peer,
            context.configured_path_name(local_path.config_ordinal()),
        )?;
    let local_usage = local_path.advertised_usage();
    let ready = fence_server_carrier_readiness(path_registration.session_retirement(), async {
        context.reliable_streams.record_local_path_metrics(
            &path_registration,
            local_metrics,
            false,
        );
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
        match framed.flush().await {
            Ok(()) => Ok(true),
            Err(err) if encrypted_framed_peer_closed(&err) => Ok(false),
            Err(err) => Err(RuntimeError::Encrypted(err)),
        }
    })
    .await?;
    if !ready {
        return Ok(());
    }
    if let Some(metrics) = tcp_metrics.as_mut() {
        metrics.begin_epoch();
    }

    let (reader, writer) = framed.split()?;
    let (commands_tx, commands_rx) =
        reliable_path_command_channels(reliable_path_command_queue(context.mux_limits));
    let observed_commands = commands_tx.clone();
    let observed_path_state = path_registration.state_handle();
    let observed_context = context.clone();
    let terminal_commands = commands_tx.clone();
    let (path_frames, native_terminal) = spawn_encrypted_tcp_reader_with_terminal_result(
        reader,
        reliable_path_writer_frame_queue(context.mux_limits),
        move |frame| {
            observe_authenticated_server_tcp_frame(
                frame,
                session_id,
                path_id,
                &observed_commands,
                &observed_path_state,
                &observed_context,
            );
        },
        move |_| terminal_commands.terminate_failed_path(),
    );
    let evidence =
        ServerTcpEvidenceState::new(tcp_metrics, Some(local_metrics), context.mux_limits);
    let peer_status = context.register_peer_status(&path_registration);
    ServerTcpPathSession::new(ServerTcpPathAdmission {
        context,
        session_id,
        path_id,
        path_registration,
        writer: ServerTcpWriter::new(writer),
        path_frames,
        native_terminal: Some(native_terminal),
        commands_tx,
        commands_rx,
        evidence,
        peer_status,
    })
    .run()
    .await
}
