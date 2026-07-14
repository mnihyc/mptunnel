//! Client QUIC path sessions and reliable stream lifecycle.

use super::client_stream::run_client_udp_stream;
use super::estimator::UdpPathMetricTracker;
use super::io::{
    UdpPathConnection, UdpPathEndpoint, UdpPathRecvStream, UdpPathSendStream,
    quic_path_open_error_is_retryable, resolve_first_socket_addr, spawn_quic_path_reader,
    udp_path_command_queue, udp_path_finish_stream, udp_path_max_stream_payload_bytes,
    udp_path_read_frame, udp_path_write_frame, udp_reliable_stream_frame_queue,
};
#[cfg(feature = "lab-diagnostics")]
use super::metrics::log_quic_ack_poll_diagnostics;
use super::metrics::quic_path_metrics_poll_interval;
use crate::config::SecurityConfig;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::timing::default_transport_pto;
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{
    Frame, IngressKind, OutboundPolicy, PathId, SessionId, StreamId, TargetAddr, UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::authentication::ClientPathAuthenticationFrames;
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::path::model::path_startup_snapshot;
use crate::runtime::path::state::ClientPathState;
use crate::runtime::relay::UdpStreamOpenOptions;
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use crate::scheduler::{FlowLane, stream_demand_hint_for_lane};
use crate::transport::{CarrierSocketProvider, CarrierSocketRequest, PathSpec};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;

#[derive(Clone)]
pub(in crate::runtime) struct ClientUdpPathSessionHandle {
    runtime: ClientUdpPathSessionRuntime,
    connection: Arc<AsyncMutex<Option<ClientUdpPathConnection>>>,
}

impl std::fmt::Debug for ClientUdpPathSessionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientUdpPathSessionHandle")
            .finish_non_exhaustive()
    }
}

impl ClientUdpPathSessionHandle {
    pub(in crate::runtime) fn new(runtime: ClientUdpPathSessionRuntime) -> Self {
        Self {
            runtime,
            connection: Arc::new(AsyncMutex::new(None)),
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn session_id(&self) -> SessionId {
        self.runtime.session_id
    }

    pub(in crate::runtime) async fn open_stream(
        &self,
        stream_id: StreamId,
        target: TargetAddr,
        ingress: IngressKind,
        lane: FlowLane,
        options: UdpStreamOpenOptions,
    ) -> Result<ReliablePathStream, RuntimeError> {
        let connection = self.ensure_connection().await?;
        match open_client_udp_stream_on_connection(
            connection,
            stream_id,
            target.clone(),
            ingress,
            lane,
            options,
            self.runtime.clone(),
        )
        .await
        {
            Ok(stream) => Ok(stream),
            Err(err) if quic_path_open_error_is_retryable(&err) => {
                self.drop_connection().await;
                let connection = self.ensure_connection().await?;
                open_client_udp_stream_on_connection(
                    connection,
                    stream_id,
                    target,
                    ingress,
                    lane,
                    options,
                    self.runtime.clone(),
                )
                .await
            }
            Err(err) => Err(err),
        }
    }

    pub(in crate::runtime) async fn open_datagram_stream(
        &self,
    ) -> Result<ClientUdpDatagramStream, RuntimeError> {
        let connection = self.ensure_connection().await?;
        match open_client_udp_datagram_stream(connection, self.runtime.clone()).await {
            Ok(stream) => Ok(stream),
            Err(err) if quic_path_open_error_is_retryable(&err) => {
                self.drop_connection().await;
                let connection = self.ensure_connection().await?;
                open_client_udp_datagram_stream(connection, self.runtime.clone()).await
            }
            Err(err) => Err(err),
        }
    }

    async fn ensure_connection(&self) -> Result<UdpPathConnection, RuntimeError> {
        let mut current = self.connection.lock().await;
        if let Some(connection) = current.as_ref() {
            return Ok(connection.connection.clone());
        }
        let connection = connect_client_udp_path(&self.runtime).await?;
        let carrier_connection = connection.connection.clone();
        *current = Some(connection);
        Ok(carrier_connection)
    }

    async fn drop_connection(&self) {
        let mut current = self.connection.lock().await;
        if let Some(connection) = current.take() {
            connection.connection.close();
        }
    }
}

#[derive(Clone)]
pub(in crate::runtime) struct ClientUdpPathSessionRuntime {
    pub(in crate::runtime) path: PathSpec,
    pub(in crate::runtime) path_index: usize,
    pub(in crate::runtime) config_ordinal: usize,
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) security: SecurityConfig,
    pub(in crate::runtime) codec_limits: CodecLimits,
    pub(in crate::runtime) mux_limits: MuxLimits,
    pub(in crate::runtime) stream_frame_queue: usize,
    pub(in crate::runtime) state: Arc<ClientPathState>,
    pub(in crate::runtime) carrier_sockets: Arc<dyn CarrierSocketProvider>,
}

struct ClientUdpPathConnection {
    _endpoint: UdpPathEndpoint,
    connection: UdpPathConnection,
    metrics_task: Option<tokio::task::JoinHandle<()>>,
}

// The metrics loop holds a carrier clone, so the session must retire it explicitly.
impl Drop for ClientUdpPathConnection {
    fn drop(&mut self) {
        self.connection.close();
        if let Some(task) = self.metrics_task.take() {
            task.abort();
        }
    }
}

fn spawn_client_udp_path_metrics(
    runtime: ClientUdpPathSessionRuntime,
    connection: UdpPathConnection,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tracker = UdpPathMetricTracker::default();
        #[cfg(feature = "lab-diagnostics")]
        let mut last_metrics_poll_at = None;
        loop {
            if connection.is_closed() {
                return;
            }
            let Some(mut metrics) = connection.tx_metrics(&mut tracker, 1).await else {
                tokio::time::sleep(default_transport_pto()).await;
                continue;
            };
            let capacity_candidate = metrics.capacity_proof_candidate;
            let capacity_probe = metrics.capacity_probe;
            #[cfg(feature = "lab-diagnostics")]
            let metrics_poll_at = Instant::now();
            #[cfg(feature = "lab-diagnostics")]
            let poll_elapsed = last_metrics_poll_at
                .replace(metrics_poll_at)
                .map(|previous| metrics_poll_at.saturating_duration_since(previous))
                .unwrap_or_default();
            #[cfg(feature = "lab-diagnostics")]
            log_quic_ack_poll_diagnostics(
                runtime.session_id,
                PathId(runtime.path_index as u16),
                0,
                metrics,
                poll_elapsed,
            );
            let published_proof = if let Some(record) = runtime
                .state
                .health()
                .lock()
                .expect("client QUIC UDP path health lock")
                .udp
                .get_mut(runtime.path_index)
            {
                record.mark_quic_path_metrics(metrics);
                capacity_candidate
                    .zip(capacity_probe)
                    .and_then(|(candidate, probe)| {
                        record.accept_request_quic_capacity_proof(candidate, probe, Instant::now())
                    })
            } else {
                None
            };
            if let (Some(candidate), Some((_rate_bps, _rate_sample_bytes, _native_tail_rate))) =
                (capacity_candidate, published_proof)
            {
                tracker.accept_capacity_proof(&mut metrics, candidate);
                let _retired = connection.retire_capacity_probe(candidate.token);
                tracker.retire_capacity_candidate(candidate.token);
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_quic_capacity_proof",
                    format_args!(
                        "phase=published session_id={} path_index={} calibration_id={} train_bytes={} receipt_rate_bps={} published_rate_bps={} rate_sample_bytes={} rate_source={} proof_validity_ms={} carrier_retired={}",
                        runtime.session_id.0,
                        runtime.path_index,
                        candidate.token,
                        candidate.train_bytes,
                        candidate.rate_bps,
                        _rate_bps,
                        _rate_sample_bytes,
                        if _native_tail_rate {
                            "native_tail"
                        } else {
                            "receipt_lower_bound"
                        },
                        candidate.proof_validity.as_millis(),
                        _retired,
                    ),
                );
            }
            tokio::time::sleep(quic_path_metrics_poll_interval(metrics)).await;
        }
    })
}

pub(in crate::runtime) struct ClientUdpDatagramStream {
    pub(in crate::runtime) send: UdpPathSendStream,
    pub(in crate::runtime) frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
    pub(in crate::runtime) runtime: ClientUdpPathSessionRuntime,
    pub(in crate::runtime) path_id: PathId,
}

async fn connect_client_udp_path(
    runtime: &ClientUdpPathSessionRuntime,
) -> Result<ClientUdpPathConnection, RuntimeError> {
    let remote_addr = resolve_first_socket_addr(&runtime.path).await?;
    let carrier = runtime.carrier_sockets.create(CarrierSocketRequest {
        path: &runtime.path,
        config_ordinal: runtime.config_ordinal,
        remote_addr,
    })?;
    let endpoint = UdpPathEndpoint::bind_client(carrier, runtime).await?;
    let connection = endpoint.connect(remote_addr).await?;
    perform_client_udp_path_handshake(&connection, runtime).await?;
    let metrics_task = spawn_client_udp_path_metrics(runtime.clone(), connection.clone());
    Ok(ClientUdpPathConnection {
        _endpoint: endpoint,
        connection,
        metrics_task: Some(metrics_task),
    })
}

async fn perform_client_udp_path_handshake(
    connection: &UdpPathConnection,
    runtime: &ClientUdpPathSessionRuntime,
) -> Result<(), RuntimeError> {
    let (mut send, mut recv) = connection.open_bi().await?;
    let path_id = PathId(runtime.path_index as u16);
    let [session_hello, session_auth, path_join] = ClientPathAuthenticationFrames::for_session(
        &runtime.security,
        &runtime.path,
        path_id,
        UnderlayProtocol::Udp,
        runtime.session_id,
    )?
    .into_array();
    udp_path_write_frame(&mut send, &session_hello, runtime.codec_limits).await?;
    udp_path_write_frame(&mut send, &session_auth, runtime.codec_limits).await?;
    udp_path_write_frame(&mut send, &path_join, runtime.codec_limits).await?;
    udp_path_finish_stream(&mut send)?;

    let mut session_ready = false;
    let mut path_active = false;
    while !session_ready || !path_active {
        match udp_path_read_frame(&mut recv, runtime.codec_limits).await? {
            Frame::SessionReady => session_ready = true,
            Frame::PathStatus {
                status: crate::protocol::PathStatus::Active,
                ..
            } => path_active = true,
            Frame::PathStatus { .. } => {
                return Err(RuntimeError::Protocol(
                    "UDP path session did not become active",
                ));
            }
            Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
            _ => {
                return Err(RuntimeError::Protocol(
                    "unexpected UDP path handshake frame",
                ));
            }
        }
    }
    Ok(())
}

async fn open_client_udp_stream_on_connection(
    connection: UdpPathConnection,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
    options: UdpStreamOpenOptions,
    runtime: ClientUdpPathSessionRuntime,
) -> Result<ReliablePathStream, RuntimeError> {
    let UdpStreamOpenOptions {
        wait_for_accept,
        role,
    } = options;
    let (mut send, mut recv) = connection.open_bi().await?;
    let open = Frame::OpenStream {
        stream_id,
        target,
        ingress,
        outbound: OutboundPolicy::Direct,
        demand: stream_demand_hint_for_lane(lane),
        role,
    };
    udp_path_write_frame(&mut send, &open, runtime.codec_limits).await?;
    let accepted_max_offset = if wait_for_accept {
        Some(read_client_udp_stream_open_accept(&mut recv, stream_id, runtime.codec_limits).await?)
    } else {
        None
    };
    let max_offset = udp_stream_open_initial_max_offset(options, accepted_max_offset);
    let (commands, receivers) = reliable_path_command_channels(udp_path_command_queue(
        runtime.mux_limits,
        runtime.codec_limits,
    ));
    let stream_frame_queue =
        udp_reliable_stream_frame_queue(runtime.codec_limits, runtime.mux_limits);
    let (frames_tx, frames_rx) = mpsc::channel(stream_frame_queue);
    tokio::spawn(run_client_udp_stream(
        send,
        recv,
        stream_id,
        runtime.path_index,
        runtime.codec_limits,
        runtime.mux_limits,
        stream_frame_queue,
        runtime.state.clone(),
        receivers,
        frames_tx,
    ));
    Ok(ReliablePathStream {
        stream_id,
        max_offset,
        lane,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: udp_path_max_stream_payload_bytes(
            runtime.codec_limits,
            runtime.mux_limits,
        ),
        output: ReliablePathStreamOutput::fixed_with_snapshot(
            path_startup_snapshot(&runtime.path, runtime.path_index),
            commands,
            runtime.mux_limits,
        ),
        frames: frames_rx,
    })
}

async fn read_client_udp_stream_open_accept(
    recv: &mut UdpPathRecvStream,
    stream_id: StreamId,
    codec_limits: CodecLimits,
) -> Result<u64, RuntimeError> {
    loop {
        match udp_path_read_frame(recv, codec_limits).await? {
            Frame::StreamMaxData {
                stream_id: max_stream_id,
                max_offset,
            } if max_stream_id == stream_id => return Ok(max_offset),
            Frame::StreamReset {
                stream_id: reset_stream_id,
                reason,
            } if reset_stream_id == stream_id => return Err(RuntimeError::RemoteReset(reason)),
            Frame::PathStatus { .. } | Frame::SessionReady => {}
            Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
            _ => {
                return Err(RuntimeError::Protocol(
                    "unexpected QUIC UDP path stream open frame",
                ));
            }
        }
    }
}

fn udp_stream_open_initial_max_offset(
    options: UdpStreamOpenOptions,
    accepted_max_offset: Option<u64>,
) -> u64 {
    if options.wait_for_accept {
        accepted_max_offset.unwrap_or(0)
    } else {
        0
    }
}

#[cfg(test)]
#[path = "client_test.rs"]
mod tests;

async fn open_client_udp_datagram_stream(
    connection: UdpPathConnection,
    runtime: ClientUdpPathSessionRuntime,
) -> Result<ClientUdpDatagramStream, RuntimeError> {
    let (send, recv) = connection.open_bi().await?;
    let frames = spawn_quic_path_reader(recv, runtime.codec_limits, runtime.stream_frame_queue);
    Ok(ClientUdpDatagramStream {
        send,
        frames,
        path_id: PathId(runtime.path_index as u16),
        runtime,
    })
}
