//! Authenticated client-side TCP carrier ownership.
//!
//! This module stops at the carrier boundary: encrypted I/O, optional native
//! telemetry, and liveness. Reliable-stream proof and capacity policy belong to
//! the reliable client actor, while datagram sessions reuse this carrier alone.

use super::super::io::{EncryptedTcpWriter, spawn_encrypted_tcp_reader};
use super::super::metrics::TcpMetricPublisher;
use crate::config::ClientSecurityConfig;
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{Frame, PathId, PathUsage, SessionId};
use crate::runtime::error::RuntimeError;
use crate::runtime::identity::random_u64;
use crate::runtime::path::commands::reliable_path_writer_frame_queue;
use crate::runtime::path::tcp::admission::ClientTcpPathAuthentication;
use crate::transport::encrypted::{
    EncryptedFramedStream, EncryptedFramedTransportError, TcpClientTlsConfig,
};
use crate::transport::tcp::{self as tcp_transport, TcpConnectOptions};
use crate::transport::{CarrierNetworkProvider, CarrierPathIdentity, PathSpec};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

pub(in crate::runtime) struct ClientTcpCarrierConnection {
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) remote_port: u16,
    pub(in crate::runtime) writer: EncryptedTcpWriter,
    pub(in crate::runtime) frames: mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>>,
    heartbeat_interval: Duration,
    heartbeat_delay: Duration,
    heartbeat_timeout: Duration,
    next_heartbeat_at: tokio::time::Instant,
    pending_heartbeat: Option<(u64, tokio::time::Instant)>,
    pub(in crate::runtime) tcp_metrics: Option<TcpMetricPublisher>,
    pub(in crate::runtime) peer_usage_sequence: u64,
    pub(in crate::runtime) peer_usage: PathUsage,
    /// One authenticated readiness exchange, excluding TCP connection setup.
    pub(in crate::runtime) readiness_rtt: Duration,
}

/// Immutable inputs for one concrete TCP carrier instance.
pub(in crate::runtime) struct ClientTcpCarrierConnect<'a> {
    pub(in crate::runtime) path: &'a PathSpec,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) carrier_identity: CarrierPathIdentity,
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) security: &'a ClientSecurityConfig,
    pub(in crate::runtime) tls: &'a TcpClientTlsConfig,
    pub(in crate::runtime) codec_limits: CodecLimits,
    pub(in crate::runtime) mux_limits: MuxLimits,
    pub(in crate::runtime) carrier_network: &'a dyn CarrierNetworkProvider,
    /// Exact configured port selected by the lifecycle owner. Initial and
    /// failure establishment leave this unset and select uniformly once.
    pub(in crate::runtime) remote_port: Option<u16>,
}

impl ClientTcpCarrierConnection {
    pub(in crate::runtime) fn heartbeat_deadline(&self) -> tokio::time::Instant {
        self.pending_heartbeat
            .as_ref()
            .map(|(_, deadline)| *deadline)
            .unwrap_or(self.next_heartbeat_at)
    }

    pub(in crate::runtime) fn schedule_next_heartbeat(&mut self) {
        self.next_heartbeat_at = tokio::time::Instant::now() + self.heartbeat_delay;
    }

    /// Reschedules the idle probe only while no response is outstanding.
    /// Local writes are not peer evidence and cannot extend a Pong deadline.
    pub(in crate::runtime) fn refresh_liveness(&mut self) {
        refresh_client_tcp_path_liveness_state(
            &mut self.next_heartbeat_at,
            self.heartbeat_delay,
            self.pending_heartbeat.is_some(),
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
        self.heartbeat_delay = heartbeat_renewal_delay(self.heartbeat_interval, random_u64()?);
        self.next_heartbeat_at = tokio::time::Instant::now() + self.heartbeat_delay;
        Ok(())
    }

    pub(in crate::runtime) async fn tick_heartbeat(&mut self) -> Result<(), RuntimeError> {
        let now = tokio::time::Instant::now();
        if let Some((_, deadline)) = self.pending_heartbeat.as_ref()
            && now >= *deadline
        {
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
}

/// Establishes TCP, authenticates the MPP session/path, and exchanges the
/// endpoints' directional usage preferences under one absolute deadline.
pub(in crate::runtime) async fn connect_client_tcp_carrier(
    request: ClientTcpCarrierConnect<'_>,
    open_deadline: tokio::time::Instant,
) -> Result<ClientTcpCarrierConnection, RuntimeError> {
    let ClientTcpCarrierConnect {
        path,
        path_id,
        carrier_identity,
        session_id,
        security,
        tls,
        codec_limits,
        mux_limits,
        carrier_network,
        remote_port,
    } = request;
    let connect = async {
        let connect_timeout = open_deadline.saturating_duration_since(tokio::time::Instant::now());
        let remote_port = match remote_port {
            Some(remote_port) => remote_port,
            None => path
                .endpoint
                .ports()
                .select()
                .map_err(RuntimeError::Random)?,
        };
        let tcp_stream = tcp_transport::connect_path_with_provider_to_port(
            path,
            carrier_identity,
            remote_port,
            TcpConnectOptions {
                timeout: connect_timeout,
                ..TcpConnectOptions::default()
            },
            carrier_network,
        )
        .await?;
        let mut tcp_metrics = TcpMetricPublisher::capture(&tcp_stream);
        let mut framed = EncryptedFramedStream::connect(tcp_stream, tls, codec_limits).await?;
        let transport_binding = framed.tcp_admission_binding()?;
        let (admission_prelude, path_join) = ClientTcpPathAuthentication::for_session(
            security,
            path_id,
            session_id,
            &transport_binding,
        )?
        .into_parts();

        let readiness_started_at = Instant::now();
        framed
            .write_tcp_admission(
                &admission_prelude,
                &[
                    path_join,
                    Frame::PathStatus {
                        path_id,
                        sequence: 0,
                        usage: if path.metadata.policy.backup {
                            PathUsage::Backup
                        } else {
                            PathUsage::Available
                        },
                    },
                ],
            )
            .await?;
        framed.flush().await?;

        let mut session_ready = false;
        let mut peer_usage = None;
        while !session_ready || peer_usage.is_none() {
            match framed.read_frame().await? {
                Frame::SessionReady => session_ready = true,
                Frame::PathStatus {
                    path_id: status_path_id,
                    sequence: 0,
                    usage,
                } if status_path_id == path_id => peer_usage = Some(usage),
                Frame::PathStatus { .. } => {
                    return Err(RuntimeError::Protocol(
                        "invalid TCP path usage advertisement",
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
        let readiness_rtt = readiness_started_at.elapsed();

        if let Some(metrics) = tcp_metrics.as_mut() {
            metrics.begin_epoch();
        }

        let (reader, writer) = framed.split()?;
        let now = tokio::time::Instant::now();
        let heartbeat_delay =
            heartbeat_renewal_delay(mux_limits.tcp_path_heartbeat_interval, random_u64()?);
        Ok(ClientTcpCarrierConnection {
            path_id,
            remote_port,
            writer,
            frames: spawn_encrypted_tcp_reader(
                reader,
                reliable_path_writer_frame_queue(mux_limits),
            ),
            heartbeat_interval: mux_limits.tcp_path_heartbeat_interval,
            heartbeat_delay,
            heartbeat_timeout: mux_limits.tcp_path_heartbeat_timeout,
            next_heartbeat_at: now + heartbeat_delay,
            pending_heartbeat: None,
            tcp_metrics,
            peer_usage_sequence: 0,
            peer_usage: peer_usage.expect("path usage checked before carrier creation"),
            readiness_rtt,
        })
    };
    tokio::time::timeout_at(open_deadline, connect)
        .await
        .map_err(|_| RuntimeError::PathOpenTimedOut)?
}

pub(in crate::runtime) fn refresh_client_tcp_path_liveness_state(
    next_heartbeat_at: &mut tokio::time::Instant,
    heartbeat_delay: Duration,
    heartbeat_pending: bool,
) {
    if !heartbeat_pending {
        *next_heartbeat_at = tokio::time::Instant::now() + heartbeat_delay;
    }
}

/// Maps one uniformly random sample onto the RFC heartbeat renewal window.
///
/// The configured interval remains the maximum delay. Integer multiplication
/// avoids floating-point drift and introduces at most one-sample quantization
/// imbalance across a nanosecond-sized range.
pub(in crate::runtime::path::tcp) fn heartbeat_renewal_delay(
    maximum: Duration,
    sample: u64,
) -> Duration {
    let maximum_nanos = maximum.as_nanos();
    let minimum_nanos = maximum_nanos.saturating_mul(4) / 5;
    let span = maximum_nanos.saturating_sub(minimum_nanos);
    let offset = (u128::from(sample).saturating_mul(span.saturating_add(1))) >> 64;
    duration_from_nanos(minimum_nanos.saturating_add(offset).min(maximum_nanos))
}

fn duration_from_nanos(nanos: u128) -> Duration {
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    let seconds = nanos / NANOS_PER_SECOND;
    let subsecond_nanos = (nanos % NANOS_PER_SECOND) as u32;
    Duration::new(u64::try_from(seconds).unwrap_or(u64::MAX), subsecond_nanos)
}
