//! Authenticated client-side TCP carrier ownership.
//!
//! This module stops at the carrier boundary: encrypted I/O, optional native
//! telemetry, and liveness. Reliable-stream proof and capacity policy belong to
//! the reliable client actor, while datagram sessions reuse this carrier alone.

use super::io::{EncryptedTcpWriter, spawn_encrypted_tcp_reader};
use super::metrics::TcpMetricPublisher;
use crate::config::SecurityConfig;
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{CloseReason, Frame, PathId, SessionId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::identity::random_u64;
use crate::runtime::path::authentication::ClientPathAuthenticationFrames;
use crate::runtime::path::commands::reliable_path_writer_frame_queue;
use crate::transport::PathSpec;
use crate::transport::encrypted::{EncryptedFramedStream, EncryptedFramedTransportError, PeerRole};
use crate::transport::tcp::{self as tcp_transport, TcpConnectOptions};
use std::time::Duration;
use tokio::sync::mpsc;

pub(in crate::runtime) struct ClientTcpCarrierConnection {
    pub(in crate::runtime) writer: EncryptedTcpWriter,
    pub(in crate::runtime) frames: mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>>,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
    next_heartbeat_at: tokio::time::Instant,
    pending_heartbeat: Option<(u64, tokio::time::Instant)>,
    pub(in crate::runtime) tcp_metrics: Option<TcpMetricPublisher>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ClientTcpHeartbeatTimeoutDisposition {
    FailCarrier,
    KeepCarrierAlive,
}

impl ClientTcpCarrierConnection {
    pub(in crate::runtime) fn heartbeat_deadline(&self) -> tokio::time::Instant {
        self.pending_heartbeat
            .as_ref()
            .map(|(_, deadline)| *deadline)
            .unwrap_or(self.next_heartbeat_at)
    }

    pub(in crate::runtime) fn schedule_next_heartbeat(&mut self) {
        self.next_heartbeat_at = tokio::time::Instant::now() + self.heartbeat_interval;
    }

    /// Reschedules liveness after carrier activity. Successful local writes
    /// also call this, so it is timer policy rather than peer-receipt proof.
    pub(in crate::runtime) fn refresh_liveness(&mut self) {
        refresh_client_tcp_path_liveness_state(
            &mut self.next_heartbeat_at,
            self.heartbeat_interval,
            &mut self.pending_heartbeat,
            self.heartbeat_timeout,
        );
    }

    /// Reliable paths require an exact Pong and restart the idle interval.
    pub(in crate::runtime) fn complete_expected_heartbeat(
        &mut self,
        nonce: u64,
    ) -> Result<(), RuntimeError> {
        let Some((pending_nonce, _)) = self.pending_heartbeat.as_ref() else {
            return Err(RuntimeError::Protocol(
                "unexpected TCP path heartbeat response",
            ));
        };
        if *pending_nonce != nonce {
            return Err(RuntimeError::Protocol(
                "unexpected TCP path heartbeat response",
            ));
        }
        self.pending_heartbeat = None;
        self.next_heartbeat_at = tokio::time::Instant::now() + self.heartbeat_interval;
        Ok(())
    }

    /// Datagram request/response accepts only its matching carrier Pong, but a
    /// stray Pong is unrelated traffic rather than a fatal product response.
    pub(in crate::runtime) fn clear_matching_heartbeat(&mut self, nonce: u64) -> bool {
        if !self
            .pending_heartbeat
            .is_some_and(|(pending_nonce, _)| pending_nonce == nonce)
        {
            return false;
        }
        self.pending_heartbeat = None;
        true
    }

    pub(in crate::runtime) async fn tick_heartbeat(
        &mut self,
        timeout_disposition: ClientTcpHeartbeatTimeoutDisposition,
    ) -> Result<(), RuntimeError> {
        let now = tokio::time::Instant::now();
        if let Some((_, deadline)) = self.pending_heartbeat.as_ref()
            && now >= *deadline
        {
            if timeout_disposition == ClientTcpHeartbeatTimeoutDisposition::KeepCarrierAlive {
                self.pending_heartbeat = None;
                self.next_heartbeat_at = now + self.heartbeat_interval;
                return Ok(());
            }
            return Err(RuntimeError::PathHeartbeatTimeout);
        }
        if self.pending_heartbeat.is_none() && now >= self.next_heartbeat_at {
            let nonce = random_u64()?;
            self.writer.write_frame(&Frame::Ping { nonce }).await?;
            self.writer.flush().await?;
            self.pending_heartbeat = Some((nonce, now + self.heartbeat_timeout));
        }
        Ok(())
    }

    pub(in crate::runtime) async fn close(&mut self, path_id: PathId) -> Result<(), RuntimeError> {
        self.writer
            .write_frame(&Frame::PathClose {
                path_id,
                reason: CloseReason::Normal,
            })
            .await?;
        self.writer
            .write_frame(&Frame::SessionClose {
                reason: CloseReason::Normal,
            })
            .await?;
        self.writer.flush().await?;
        Ok(())
    }
}

/// Establishes TCP, authenticates the MPP session/path, and waits for active
/// status under one caller-supplied absolute deadline.
pub(in crate::runtime) async fn connect_client_tcp_carrier(
    path: &PathSpec,
    path_index: usize,
    session_id: SessionId,
    security: &SecurityConfig,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    open_deadline: tokio::time::Instant,
) -> Result<ClientTcpCarrierConnection, RuntimeError> {
    let connect = async {
        let connect_timeout = open_deadline.saturating_duration_since(tokio::time::Instant::now());
        let tcp_stream = tcp_transport::connect_path(
            path,
            TcpConnectOptions {
                timeout: connect_timeout,
                ..TcpConnectOptions::default()
            },
        )
        .await?;
        let mut tcp_metrics = TcpMetricPublisher::capture(&tcp_stream);
        let mut framed = EncryptedFramedStream::with_cipher_suite(
            tcp_stream,
            security.secret.as_bytes(),
            PeerRole::Client,
            codec_limits,
            security.cipher,
        )?;
        let path_id = PathId(path_index as u16);
        let authentication_frames = ClientPathAuthenticationFrames::for_session(
            security,
            path,
            path_id,
            UnderlayProtocol::Tcp,
            session_id,
        )?;

        framed
            .write_frames(&authentication_frames.into_array())
            .await?;
        framed.flush().await?;

        let mut session_ready = false;
        let mut path_active = false;
        while !session_ready || !path_active {
            match framed.read_frame().await? {
                Frame::SessionReady => session_ready = true,
                Frame::PathStatus {
                    status: crate::protocol::PathStatus::Active,
                    ..
                } => path_active = true,
                Frame::PathStatus { .. } => {
                    return Err(RuntimeError::Protocol(
                        "TCP path session did not become active",
                    ));
                }
                Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
                _ => {
                    return Err(RuntimeError::Protocol(
                        "unexpected TCP path handshake frame",
                    ));
                }
            }
        }

        if let Some(metrics) = tcp_metrics.as_mut() {
            metrics.begin_epoch();
        }

        let (reader, writer) = framed.split()?;
        let now = tokio::time::Instant::now();
        Ok(ClientTcpCarrierConnection {
            writer,
            frames: spawn_encrypted_tcp_reader(
                reader,
                reliable_path_writer_frame_queue(mux_limits),
            ),
            heartbeat_interval: mux_limits.tcp_path_heartbeat_interval,
            heartbeat_timeout: mux_limits.tcp_path_heartbeat_timeout,
            next_heartbeat_at: now + mux_limits.tcp_path_heartbeat_interval,
            pending_heartbeat: None,
            tcp_metrics,
        })
    };
    tokio::time::timeout_at(open_deadline, connect)
        .await
        .map_err(|_| RuntimeError::PathOpenTimedOut)?
}

pub(in crate::runtime) fn refresh_client_tcp_path_liveness_state(
    next_heartbeat_at: &mut tokio::time::Instant,
    heartbeat_interval: Duration,
    pending_heartbeat: &mut Option<(u64, tokio::time::Instant)>,
    heartbeat_timeout: Duration,
) {
    let now = tokio::time::Instant::now();
    *next_heartbeat_at = now + heartbeat_interval;
    if let Some((_, deadline)) = pending_heartbeat.as_mut() {
        *deadline = now + heartbeat_timeout;
    }
}
