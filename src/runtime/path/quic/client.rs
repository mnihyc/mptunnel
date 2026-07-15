//! Client QUIC path sessions and reliable stream lifecycle.

use super::client_stream::run_client_udp_stream;
use super::estimator::UdpPathMetricTracker;
use super::io::{
    UdpPathConnection, UdpPathEndpoint, UdpPathRecvStream, UdpPathSendStream,
    interleave_udp_path_socket_addr_families, quic_path_open_error_is_retryable,
    spawn_quic_path_reader, udp_path_command_queue, udp_path_finish_stream,
    udp_path_max_stream_payload_bytes, udp_path_read_frame, udp_path_write_frame,
    udp_reliable_stream_frame_queue, usable_udp_path_socket_addrs,
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
use crate::runtime::path::ports::{OpenedReliableCarrierStream, UdpStreamOpenOptions};
use crate::runtime::path::state::ClientPathState;
use crate::scheduler::{FlowLane, stream_demand_hint_for_lane};
use crate::transport::{
    CarrierNetworkProvider, CarrierPathIdentity, CarrierResolutionRequest, CarrierSocketRequest,
    PathSpec,
};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;

// RFC 8305's default keeps a blackholed family from monopolizing setup without
// opening every resolver answer in one socket/TLS burst.
const QUIC_ADDRESS_ATTEMPT_DELAY: Duration = Duration::from_millis(250);
const MAX_QUIC_ADDRESS_ATTEMPTS: usize = 8;
use tokio::sync::mpsc;

fn quic_address_attempt_delay(remaining: Duration, unstarted: usize) -> Duration {
    debug_assert!(unstarted > 0 && unstarted < u32::MAX as usize);
    let slots = unstarted as u32 + 1;
    (remaining / slots).min(QUIC_ADDRESS_ATTEMPT_DELAY)
}

fn next_quic_address_attempt_at(
    open_deadline: tokio::time::Instant,
    unstarted: usize,
) -> tokio::time::Instant {
    let now = tokio::time::Instant::now();
    now + quic_address_attempt_delay(open_deadline.saturating_duration_since(now), unstarted)
}

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
        open_deadline: tokio::time::Instant,
    ) -> Result<OpenedReliableCarrierStream, RuntimeError> {
        let open = async {
            let connection = self.ensure_connection(open_deadline).await?;
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
                    let connection = self.ensure_connection(open_deadline).await?;
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
        };
        tokio::time::timeout_at(open_deadline, open)
            .await
            .map_err(|_| RuntimeError::PathOpenTimedOut)?
    }

    pub(in crate::runtime) async fn open_datagram_stream(
        &self,
        open_deadline: tokio::time::Instant,
    ) -> Result<ClientUdpDatagramStream, RuntimeError> {
        let open = async {
            let connection = self.ensure_connection(open_deadline).await?;
            match open_client_udp_datagram_stream(connection, self.runtime.clone()).await {
                Ok(stream) => Ok(stream),
                Err(err) if quic_path_open_error_is_retryable(&err) => {
                    self.drop_connection().await;
                    let connection = self.ensure_connection(open_deadline).await?;
                    open_client_udp_datagram_stream(connection, self.runtime.clone()).await
                }
                Err(err) => Err(err),
            }
        };
        tokio::time::timeout_at(open_deadline, open)
            .await
            .map_err(|_| RuntimeError::PathOpenTimedOut)?
    }

    async fn ensure_connection(
        &self,
        open_deadline: tokio::time::Instant,
    ) -> Result<UdpPathConnection, RuntimeError> {
        let mut current = self.connection.lock().await;
        if let Some(connection) = current.as_ref() {
            return Ok(connection.connection.clone());
        }
        let connection = connect_client_udp_path(&self.runtime, open_deadline).await?;
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
    pub(in crate::runtime) carrier_identity: CarrierPathIdentity,
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) security: SecurityConfig,
    pub(in crate::runtime) codec_limits: CodecLimits,
    pub(in crate::runtime) mux_limits: MuxLimits,
    pub(in crate::runtime) stream_frame_queue: usize,
    pub(in crate::runtime) state: Arc<ClientPathState>,
    pub(in crate::runtime) carrier_network: Arc<dyn CarrierNetworkProvider>,
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
    open_deadline: tokio::time::Instant,
) -> Result<ClientUdpPathConnection, RuntimeError> {
    let connect = async {
        let resolved = runtime
            .carrier_network
            .resolve(CarrierResolutionRequest {
                path: &runtime.path,
                identity: runtime.carrier_identity,
            })
            .await?;
        let resolved = usable_udp_path_socket_addrs(&runtime.path, resolved)?;
        let mut remote_addrs = interleave_udp_path_socket_addr_families(resolved)
            .into_iter()
            .take(MAX_QUIC_ADDRESS_ATTEMPTS)
            .collect::<VecDeque<_>>();
        let mut attempts = FuturesUnordered::new();
        let first_addr = remote_addrs
            .pop_front()
            .expect("resolver rejects an empty address set");
        attempts.push(connect_client_udp_addr(runtime, first_addr));
        let mut next_attempt_at = (!remote_addrs.is_empty())
            .then(|| next_quic_address_attempt_at(open_deadline, remote_addrs.len()));

        // A blackholed first DNS record must not consume the whole path budget.
        // Race establishment only; dropping the remaining futures closes losers.
        let mut last_error = None;
        let established = loop {
            let completed = if remote_addrs.is_empty() {
                attempts.next().await
            } else {
                tokio::select! {
                    biased;
                    completed = attempts.next() => completed,
                    _ = tokio::time::sleep_until(
                        next_attempt_at.expect("unstarted addresses have a launch time")
                    ) => {
                        if tokio::time::Instant::now() >= open_deadline {
                            return Err(RuntimeError::PathOpenTimedOut);
                        }
                        let remote_addr = remote_addrs
                            .pop_front()
                            .expect("address availability checked before stagger timer");
                        attempts.push(connect_client_udp_addr(runtime, remote_addr));
                        next_attempt_at = (!remote_addrs.is_empty()).then(|| {
                            next_quic_address_attempt_at(open_deadline, remote_addrs.len())
                        });
                        continue;
                    }
                }
            };
            match completed {
                Some(Ok(connection)) => break connection,
                Some(Err(err)) => {
                    last_error = Some(err);
                    tokio::task::yield_now().await;
                    if tokio::time::Instant::now() >= open_deadline {
                        return Err(RuntimeError::PathOpenTimedOut);
                    }
                    // A hard failure does not need the blackhole stagger.
                    if attempts.is_empty()
                        && let Some(remote_addr) = remote_addrs.pop_front()
                    {
                        attempts.push(connect_client_udp_addr(runtime, remote_addr));
                        next_attempt_at = (!remote_addrs.is_empty()).then(|| {
                            next_quic_address_attempt_at(open_deadline, remote_addrs.len())
                        });
                    }
                }
                None => {
                    return Err(last_error.unwrap_or(RuntimeError::Protocol(
                        "QUIC UDP path exhausted resolved socket addresses",
                    )));
                }
            }
        };
        drop(attempts);
        let (endpoint, connection) = established;

        // Address retry owns only carrier establishment. Authenticate exactly
        // once so a rejected MPP identity is never retried as a DNS decision.
        perform_client_udp_path_handshake(&connection, runtime).await?;
        let metrics_task = spawn_client_udp_path_metrics(runtime.clone(), connection.clone());
        Ok(ClientUdpPathConnection {
            _endpoint: endpoint,
            connection,
            metrics_task: Some(metrics_task),
        })
    };
    tokio::time::timeout_at(open_deadline, connect)
        .await
        .map_err(|_| RuntimeError::PathOpenTimedOut)?
}

async fn connect_client_udp_addr(
    runtime: &ClientUdpPathSessionRuntime,
    remote_addr: std::net::SocketAddr,
) -> Result<(UdpPathEndpoint, UdpPathConnection), RuntimeError> {
    // Each attempt needs its own family-correct, host-protected socket.
    let carrier = runtime
        .carrier_network
        .create_socket(CarrierSocketRequest {
            path: &runtime.path,
            identity: runtime.carrier_identity,
            remote_addr,
        })?;
    let endpoint = UdpPathEndpoint::bind_client(carrier, runtime).await?;
    let connection = endpoint.connect(remote_addr).await?;
    Ok((endpoint, connection))
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
) -> Result<OpenedReliableCarrierStream, RuntimeError> {
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
    Ok(OpenedReliableCarrierStream {
        stream_id,
        max_offset,
        lane,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: udp_path_max_stream_payload_bytes(
            runtime.codec_limits,
            runtime.mux_limits,
        ),
        startup: path_startup_snapshot(&runtime.path, runtime.path_index),
        commands,
        mux_limits: runtime.mux_limits,
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
