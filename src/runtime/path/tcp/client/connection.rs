//! Authenticated client-side TCP carrier ownership.
//!
//! This module stops at the carrier boundary: encrypted I/O, optional native
//! telemetry, and liveness. Reliable-stream proof and capacity policy belong to
//! the reliable client actor, while datagram sessions reuse this carrier alone.

use super::super::heartbeat::{TcpCarrierHeartbeat, TcpCarrierHeartbeatClaim};
use super::super::io::{
    AuthenticatedFrameDisposition, EncryptedTcpWriter,
    spawn_encrypted_tcp_reader_with_filtered_observer,
};
use super::super::metrics::TcpMetricPublisher;
use crate::config::ClientSecurityConfig;
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{ConfiguredMemberSlot, Frame, PathId, PathUsage, SessionId};
use crate::runtime::error::RuntimeError;
use crate::runtime::identity::{random_u64, random_u64_sample};
use crate::runtime::path::client_session::ClientSessionLifecycle;
use crate::runtime::path::commands::reliable_path_writer_frame_queue;
use crate::runtime::path::tcp::admission::ClientTcpPathAuthentication;
use crate::transport::encrypted::{
    EncryptedFramedStream, EncryptedFramedTransportError, TcpClientTlsConfig,
};
use crate::transport::tcp::{self as tcp_transport, TcpConnectOptions};
use crate::transport::tcp_write_admission::TcpWriteAdmission;
use crate::transport::{CarrierNetworkProvider, CarrierPathIdentity, PathSpec};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

pub(in crate::runtime) struct ClientTcpCarrierConnection {
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) remote_port: u16,
    pub(in crate::runtime) writer: EncryptedTcpWriter,
    pub(in crate::runtime) frames: mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>>,
    pub(in crate::runtime::path::tcp) heartbeat: Arc<TcpCarrierHeartbeat>,
    pub(in crate::runtime) tcp_metrics: Option<TcpMetricPublisher>,
    pub(super) write_admission: Option<TcpWriteAdmission>,
    pub(in crate::runtime) peer_usage_sequence: u64,
    pub(in crate::runtime) peer_usage: PathUsage,
    /// One authenticated readiness exchange, excluding TCP connection setup.
    pub(in crate::runtime) readiness_rtt: Duration,
    pub(in crate::runtime) local_addr: Option<std::net::SocketAddr>,
    pub(in crate::runtime) peer_addr: Option<std::net::SocketAddr>,
}

/// Immutable inputs for one concrete TCP carrier instance.
pub(in crate::runtime) struct ClientTcpCarrierConnect<'a> {
    pub(in crate::runtime) path: &'a PathSpec,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) configured_slot: ConfiguredMemberSlot,
    pub(in crate::runtime) carrier_identity: CarrierPathIdentity,
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) security: &'a ClientSecurityConfig,
    pub(in crate::runtime) tls: &'a TcpClientTlsConfig,
    pub(in crate::runtime) codec_limits: CodecLimits,
    pub(in crate::runtime) mux_limits: MuxLimits,
    pub(in crate::runtime) carrier_network: &'a dyn CarrierNetworkProvider,
    pub(in crate::runtime) session_lifecycle: ClientSessionLifecycle,
    /// Exact configured port selected by the lifecycle owner. Initial and
    /// failure establishment leave this unset and select uniformly once.
    pub(in crate::runtime) remote_port: Option<u16>,
    /// Address syscalls are captured only for interested lifecycle/snapshot
    /// observers. The disabled shape preserves existing establishment work.
    pub(in crate::runtime) capture_addresses: bool,
}

impl ClientTcpCarrierConnection {
    /// None preserves the legacy structural permission without claiming a
    /// measured native capacity. Only the exact supported socket can block it.
    pub(super) fn allows_original_handoff(&self) -> Result<bool, RuntimeError> {
        self.write_admission
            .as_ref()
            .map_or(Ok(true), TcpWriteAdmission::is_ready)
            .map_err(RuntimeError::Io)
    }

    /// Borrow only the capability so the actor can keep receiving frames and
    /// servicing lifecycle work while this exact socket is not writable.
    pub(super) async fn native_writable(
        admission: &Option<TcpWriteAdmission>,
    ) -> Result<(), RuntimeError> {
        match admission {
            Some(admission) => admission.writable().await.map_err(RuntimeError::Io),
            None => std::future::pending().await,
        }
    }

    pub(in crate::runtime) async fn tick_heartbeat(&mut self) -> Result<(), RuntimeError> {
        let now = tokio::time::Instant::now();
        let claim = self.heartbeat.claim_due_ping(now, random_u64_sample);
        let ping = match claim {
            TcpCarrierHeartbeatClaim::NotDue | TcpCarrierHeartbeatClaim::Draining => return Ok(()),
            TcpCarrierHeartbeatClaim::Failed(failure) => {
                return Err(self.heartbeat.runtime_error(failure));
            }
            TcpCarrierHeartbeatClaim::Claimed(ping) => ping,
        };
        self.writer
            .write_frame(&Frame::Ping { nonce: ping.nonce })
            .await?;
        self.writer.flush().await?;
        self.heartbeat
            .mark_ping_flushed(ping.nonce, tokio::time::Instant::now())
            .map_err(|failure| self.heartbeat.runtime_error(failure))
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
        configured_slot,
        carrier_identity,
        session_id,
        security,
        tls,
        codec_limits,
        mux_limits,
        carrier_network,
        session_lifecycle,
        remote_port,
        capture_addresses,
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
        let (local_addr, peer_addr) = if capture_addresses {
            (tcp_stream.local_addr().ok(), tcp_stream.peer_addr().ok())
        } else {
            (None, None)
        };
        #[cfg(feature = "lab-diagnostics")]
        let lab_local_addr = tcp_stream.local_addr().ok();
        let write_admission = match TcpWriteAdmission::capture(
            &tcp_stream,
            crate::model::capacity::MAX_RELIABLE_SERVICE_QUANTUM_BYTES,
        ) {
            Ok(admission) => admission,
            Err(error) => {
                // Failed optional acquisition leaves native policy unchanged.
                static WARNING: std::sync::Once = std::sync::Once::new();
                WARNING.call_once(|| {
                    crate::observability::process_event!(
                        Warn,
                        "tcp",
                        "write_admission_unavailable",
                        "TCP native write admission unavailable; retaining structural readiness: {error}"
                    );
                });
                None
            }
        };
        let mut tcp_metrics = TcpMetricPublisher::capture(&tcp_stream);
        let mut framed = EncryptedFramedStream::connect(tcp_stream, tls, codec_limits).await?;
        let transport_binding = framed.tcp_admission_binding()?;
        let (admission_prelude, path_join) = ClientTcpPathAuthentication::for_session(
            security,
            path_id,
            configured_slot,
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
        let heartbeat = Arc::new(TcpCarrierHeartbeat::new(
            mux_limits.tcp_path_heartbeat_interval,
            mux_limits.tcp_path_heartbeat_timeout,
            now,
            random_u64()?,
        ));
        let observed_lifecycle = session_lifecycle.clone();
        let observed_heartbeat = heartbeat.clone();
        let frames = spawn_encrypted_tcp_reader_with_filtered_observer(
            reader,
            reliable_path_writer_frame_queue(mux_limits),
            move |frame| {
                let decoded_at = tokio::time::Instant::now();
                #[cfg(feature = "lab-diagnostics")]
                if let Frame::StreamData {
                    stream_id,
                    offset,
                    payload,
                    ..
                } = frame
                    && crate::lab_diagnostics::lab_selected_stream_id() == Some(stream_id.0)
                    && !payload.is_empty()
                    && payload.len() <= 64
                {
                    crate::lab_diagnostics::lab_diagnostic(
                        "tcp_echo_authenticated",
                        format_args!(
                            "session_id={} path_namespace=wire path_id={} local_addr={:?} remote_port={} stream_id={} offset={} payload_bytes={}",
                            session_id.0,
                            path_id.0,
                            lab_local_addr,
                            remote_port,
                            stream_id.0,
                            offset,
                            payload.len(),
                        ),
                    );
                }
                if let Frame::SessionClose { reason } = frame {
                    observed_lifecycle.retire(*reason);
                }
                match observed_heartbeat.observe_authenticated_frame(
                    frame,
                    decoded_at,
                    random_u64_sample,
                ) {
                    crate::runtime::path::tcp::heartbeat::TcpCarrierHeartbeatFrameDisposition::Forward => {
                        AuthenticatedFrameDisposition::Forward
                    }
                    crate::runtime::path::tcp::heartbeat::TcpCarrierHeartbeatFrameDisposition::Consume => {
                        AuthenticatedFrameDisposition::Consume
                    }
                }
            },
        );
        Ok(ClientTcpCarrierConnection {
            path_id,
            remote_port,
            writer,
            frames,
            heartbeat,
            tcp_metrics,
            write_admission,
            peer_usage_sequence: 0,
            peer_usage: peer_usage.expect("path usage checked before carrier creation"),
            readiness_rtt,
            local_addr,
            peer_addr,
        })
    };
    tokio::time::timeout_at(open_deadline, connect)
        .await
        .map_err(|_| RuntimeError::PathOpenTimedOut)?
}
