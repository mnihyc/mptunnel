//! Client QUIC path sessions and reliable stream lifecycle.

use super::client_stream::{apply_client_udp_path_status, run_client_udp_stream};
use super::estimator::UdpPathMetricTracker;
use super::io::{
    UdpPathConnection, UdpPathEndpoint, UdpPathRecvStream, UdpPathSendStream,
    quic_path_open_error_is_retryable, spawn_quic_path_reader, udp_path_command_queue,
    udp_path_max_stream_payload_bytes, udp_path_read_frame, udp_path_write_frame,
    udp_reliable_stream_frame_queue, usable_udp_path_socket_addrs,
    warn_unexpected_udp_runtime_error,
};
#[cfg(feature = "lab-diagnostics")]
use super::metrics::log_quic_ack_poll_diagnostics;
use super::metrics::quic_path_metrics_poll_interval;
use crate::config::SecurityConfig;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::path::{CarrierPathInstanceId, RelayPathKey, next_carrier_path_instance_id};
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{
    Frame, PathId, PathMetricDirection, PathUsage, SessionId, StreamId, TargetAddr,
    UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::identity::random_session_id;
use crate::runtime::path::authentication::ClientPathAuthenticationFrames;
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::path::health::{ClientPathHealth, ClientPathHealthRecord};
use crate::runtime::path::model::path_startup_snapshot;
use crate::runtime::path::ports::OpenedReliableCarrierStream;
use crate::runtime::path::state::ClientPathState;
use crate::runtime::peer_status::{PeerStatusBroker, PeerStatusCarrier, PeerStatusSnapshotSource};
use crate::scheduler::{TrafficClass, stream_demand_hint_for_traffic_class};
use crate::transport::quic::QuicCarrierError;
use crate::transport::{
    CarrierNetworkProvider, CarrierPathIdentity, CarrierResolutionRequest, CarrierSocketRequest,
    PathSpec, interleave_socket_addr_families,
};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
#[cfg(feature = "lab-diagnostics")]
use std::time::Instant;
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

    pub(in crate::runtime) fn transient_probe(&self) -> Result<Self, RuntimeError> {
        let mut runtime = self.runtime.clone();
        runtime.session_id = random_session_id()?;
        runtime.state = ClientPathState::new(ClientPathHealth {
            tcp: Vec::new(),
            udp: vec![ClientPathHealthRecord::default(); runtime.paths.len()],
        });
        runtime.peer_status = PeerStatusBroker::new(false);
        runtime.peer_status_snapshot = PeerStatusSnapshotSource::new(Vec::new);
        Ok(Self::new(runtime))
    }

    pub(in crate::runtime) async fn prepare_connection(
        &self,
        open_deadline: tokio::time::Instant,
    ) -> Result<Option<Duration>, RuntimeError> {
        let (carrier, newly_connected) = self.ensure_connection_with_status(open_deadline).await?;
        Ok(newly_connected.then(|| carrier.connection.rtt()))
    }

    pub(in crate::runtime) async fn open_stream(
        &self,
        stream_id: StreamId,
        target: TargetAddr,
        lane: TrafficClass,
        open_deadline: tokio::time::Instant,
    ) -> Result<OpenedReliableCarrierStream, RuntimeError> {
        let open = async {
            let connection = self.ensure_connection(open_deadline).await?;
            match open_client_udp_stream_on_connection(
                connection,
                stream_id,
                target.clone(),
                lane,
                self.runtime.clone(),
            )
            .await
            {
                Ok(stream) => Ok(stream),
                Err(err) if quic_path_open_error_is_retryable(&err) => {
                    self.drop_failed_connection().await;
                    let connection = self.ensure_connection(open_deadline).await?;
                    open_client_udp_stream_on_connection(
                        connection,
                        stream_id,
                        target,
                        lane,
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
                    self.drop_failed_connection().await;
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
    ) -> Result<ClientUdpCarrierInstance, RuntimeError> {
        Ok(self.ensure_connection_with_status(open_deadline).await?.0)
    }

    async fn ensure_connection_with_status(
        &self,
        open_deadline: tokio::time::Instant,
    ) -> Result<(ClientUdpCarrierInstance, bool), RuntimeError> {
        let mut current = self.connection.lock().await;
        if current
            .as_ref()
            .is_some_and(|connection| connection.carrier.connection.is_closed())
        {
            current.take();
        }
        if let Some(connection) = current.as_ref() {
            return Ok((connection.carrier.clone(), false));
        }
        let connection = connect_client_udp_path(&self.runtime, open_deadline).await?;
        let carrier = connection.carrier.clone();
        *current = Some(connection);
        Ok((carrier, true))
    }

    async fn drop_failed_connection(&self) {
        let mut current = self.connection.lock().await;
        if let Some(connection) = current.take() {
            self.runtime.state.mark_path_instance_data_plane_failure(
                RelayPathKey {
                    underlay: UnderlayProtocol::Udp,
                    index: self.runtime.path_index,
                },
                connection.carrier.path_instance_id,
            );
            connection.carrier.connection.close();
        }
    }
}

#[derive(Clone)]
pub(in crate::runtime) struct ClientUdpPathSessionRuntime {
    pub(in crate::runtime) paths: Arc<Vec<PathSpec>>,
    pub(in crate::runtime) config_index: usize,
    pub(in crate::runtime) path_index: usize,
    pub(in crate::runtime) carrier_identity: CarrierPathIdentity,
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) security: Arc<Vec<SecurityConfig>>,
    pub(in crate::runtime) codec_limits: CodecLimits,
    pub(in crate::runtime) mux_limits: MuxLimits,
    pub(in crate::runtime) stream_frame_queue: usize,
    pub(in crate::runtime) state: Arc<ClientPathState>,
    pub(in crate::runtime) carrier_network: Arc<dyn CarrierNetworkProvider>,
    pub(in crate::runtime) peer_status: PeerStatusBroker,
    pub(in crate::runtime) peer_status_snapshot: PeerStatusSnapshotSource,
}

impl ClientUdpPathSessionRuntime {
    pub(in crate::runtime) fn path(&self) -> &PathSpec {
        self.paths
            .get(self.config_index)
            .expect("UDP session path inventory matches its index")
    }

    pub(in crate::runtime) fn security(&self) -> &SecurityConfig {
        self.security
            .get(self.config_index)
            .expect("UDP session security inventory matches its index")
    }
}

#[derive(Clone)]
struct ClientUdpCarrierInstance {
    connection: UdpPathConnection,
    path_instance_id: CarrierPathInstanceId,
}

struct ClientUdpPathConnection {
    _endpoint: UdpPathEndpoint,
    carrier: ClientUdpCarrierInstance,
    metrics_task: Option<tokio::task::JoinHandle<()>>,
    control_task: Option<tokio::task::JoinHandle<()>>,
}

// The metrics loop holds a carrier clone, so the session must retire it explicitly.
impl Drop for ClientUdpPathConnection {
    fn drop(&mut self) {
        self.carrier.connection.close();
        if let Some(task) = self.metrics_task.take() {
            task.abort();
        }
        if let Some(task) = self.control_task.take() {
            task.abort();
        }
    }
}

fn spawn_client_udp_path_metrics(
    runtime: ClientUdpPathSessionRuntime,
    connection: UdpPathConnection,
    path_instance_id: CarrierPathInstanceId,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tracker = UdpPathMetricTracker::default();
        #[cfg(feature = "lab-diagnostics")]
        let mut last_metrics_poll_at = None;
        loop {
            if connection.is_closed() {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "quic_carrier_closed",
                    format_args!(
                        "session_id={} path_index={} path_instance_id={:?} locally_closed={} reason={}",
                        runtime.session_id.0,
                        runtime.path_index,
                        path_instance_id,
                        connection.is_locally_closed(),
                        connection
                            .close_reason()
                            .unwrap_or_else(|| "unknown".to_string()),
                    ),
                );
                if !connection.is_locally_closed() {
                    runtime.state.mark_path_instance_data_plane_failure(
                        RelayPathKey {
                            underlay: UnderlayProtocol::Udp,
                            index: runtime.path_index,
                        },
                        path_instance_id,
                    );
                }
                return;
            }
            let metrics = connection.tx_metrics(&mut tracker, PathMetricDirection::ClientToServer);
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
            if let Some(record) = runtime
                .state
                .health()
                .lock()
                .expect("client QUIC UDP path health lock")
                .udp
                .get_mut(runtime.path_index)
            {
                record.mark_quic_path_metrics(metrics);
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
    pub(in crate::runtime) path_instance_id: CarrierPathInstanceId,
}

async fn connect_client_udp_path(
    runtime: &ClientUdpPathSessionRuntime,
    open_deadline: tokio::time::Instant,
) -> Result<ClientUdpPathConnection, RuntimeError> {
    let connect = async {
        let resolved = runtime
            .carrier_network
            .resolve(CarrierResolutionRequest {
                path: runtime.path(),
                identity: runtime.carrier_identity,
            })
            .await?;
        let resolved = usable_udp_path_socket_addrs(runtime.path(), resolved)?;
        let mut remote_addrs = interleave_socket_addr_families(resolved)
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
        let (peer_usage, control_send, control_recv) =
            perform_client_udp_path_handshake(&connection, runtime).await?;
        let path_instance_id = next_carrier_path_instance_id();
        runtime.state.install_peer_path_usage(
            UnderlayProtocol::Udp,
            runtime.path_index,
            path_instance_id,
            0,
            peer_usage,
        );
        let metrics_task =
            spawn_client_udp_path_metrics(runtime.clone(), connection.clone(), path_instance_id);
        let peer_status = runtime.peer_status.register(runtime.session_id);
        let control_connection = connection.clone();
        let control_runtime = runtime.clone();
        let control_task = tokio::spawn(async move {
            if let Err(err) = run_client_udp_control_stream(
                control_send,
                control_recv,
                peer_status,
                control_runtime,
            )
            .await
            {
                warn_unexpected_udp_runtime_error("client QUIC control stream failed", &err);
                control_connection.close();
            }
        });
        Ok(ClientUdpPathConnection {
            _endpoint: endpoint,
            carrier: ClientUdpCarrierInstance {
                connection,
                path_instance_id,
            },
            metrics_task: Some(metrics_task),
            control_task: Some(control_task),
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
            path: runtime.path(),
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
) -> Result<(PathUsage, UdpPathSendStream, UdpPathRecvStream), RuntimeError> {
    let (mut send, mut recv) = connection.open_bi().await?;
    let path_id = PathId(runtime.path_index as u16);
    let [session_hello, session_auth, path_join] = ClientPathAuthenticationFrames::for_session(
        runtime.security(),
        path_id,
        UnderlayProtocol::Udp,
        runtime.session_id,
    )?
    .into_array();
    udp_path_write_frame(&mut send, &session_hello, runtime.codec_limits).await?;
    udp_path_write_frame(&mut send, &session_auth, runtime.codec_limits).await?;
    udp_path_write_frame(&mut send, &path_join, runtime.codec_limits).await?;
    udp_path_write_frame(
        &mut send,
        &Frame::PathStatus {
            path_id,
            sequence: 0,
            usage: if runtime.path().metadata.policy.backup {
                PathUsage::Backup
            } else {
                PathUsage::Available
            },
        },
        runtime.codec_limits,
    )
    .await?;
    let mut session_ready = false;
    let mut peer_usage = None;
    while !session_ready || peer_usage.is_none() {
        match udp_path_read_frame(&mut recv, runtime.codec_limits).await? {
            Frame::SessionReady => session_ready = true,
            Frame::PathStatus {
                path_id: status_path_id,
                sequence: 0,
                usage,
            } if status_path_id == path_id => peer_usage = Some(usage),
            Frame::PathStatus { .. } => {
                return Err(RuntimeError::Protocol(
                    "invalid UDP path usage advertisement",
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
    Ok((
        peer_usage.expect("path usage checked before handshake completion"),
        send,
        recv,
    ))
}

enum ClientUdpControlEvent {
    Frame(Result<Frame, RuntimeError>),
    Request(Option<u64>),
}

async fn run_client_udp_control_stream(
    mut send: UdpPathSendStream,
    mut recv: UdpPathRecvStream,
    mut peer_status: PeerStatusCarrier,
    runtime: ClientUdpPathSessionRuntime,
) -> Result<(), RuntimeError> {
    loop {
        let event = tokio::select! {
            frame = udp_path_read_frame(&mut recv, runtime.codec_limits) => {
                ClientUdpControlEvent::Frame(frame)
            }
            request_id = peer_status.recv_request() => {
                ClientUdpControlEvent::Request(request_id)
            }
        };
        let outgoing = match event {
            ClientUdpControlEvent::Frame(Ok(Frame::PeerStatusRequest { request_id })) => Some(
                peer_status.response_frame(request_id, runtime.codec_limits, || {
                    runtime.peer_status_snapshot.snapshot()
                }),
            ),
            ClientUdpControlEvent::Frame(Ok(Frame::PeerStatusResponse {
                request_id,
                code,
                paths,
            })) => {
                let _ = peer_status.receive_response(request_id, code, paths);
                None
            }
            ClientUdpControlEvent::Frame(Ok(Frame::SessionClose { reason })) => {
                return Err(RuntimeError::RemoteClosed(reason));
            }
            ClientUdpControlEvent::Frame(Ok(_)) => {
                return Err(RuntimeError::Protocol(
                    "unexpected QUIC UDP control stream frame",
                ));
            }
            // Pre-control peers finish the handshake stream; keep their product
            // connection usable and simply withdraw this diagnostic carrier.
            ClientUdpControlEvent::Frame(Err(RuntimeError::QuicCarrier(
                QuicCarrierError::StreamFinished,
            ))) => return Ok(()),
            ClientUdpControlEvent::Frame(Err(err)) => return Err(err),
            ClientUdpControlEvent::Request(Some(request_id)) => {
                Some(Frame::PeerStatusRequest { request_id })
            }
            ClientUdpControlEvent::Request(None) => return Ok(()),
        };
        if let Some(frame) = outgoing {
            udp_path_write_frame(&mut send, &frame, runtime.codec_limits).await?;
        }
    }
}

async fn open_client_udp_stream_on_connection(
    carrier: ClientUdpCarrierInstance,
    stream_id: StreamId,
    target: TargetAddr,
    lane: TrafficClass,
    runtime: ClientUdpPathSessionRuntime,
) -> Result<OpenedReliableCarrierStream, RuntimeError> {
    let (mut send, mut recv) = carrier.connection.open_bi().await?;
    let open = Frame::OpenStream {
        stream_id,
        target,
        demand: stream_demand_hint_for_traffic_class(lane),
    };
    udp_path_write_frame(&mut send, &open, runtime.codec_limits).await?;
    let path_id = PathId(runtime.path_index as u16);
    let max_offset = read_client_udp_stream_open_accept(
        &mut recv,
        stream_id,
        runtime.path_index,
        carrier.path_instance_id,
        &runtime.state,
        path_id,
        runtime.codec_limits,
    )
    .await?;
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
        carrier.path_instance_id,
        runtime.codec_limits,
        runtime.mux_limits,
        stream_frame_queue,
        runtime.state.clone(),
        receivers,
        frames_tx,
    ));
    let mut startup = path_startup_snapshot(runtime.path(), runtime.path_index);
    startup.peer_usage = runtime
        .state
        .peer_path_usage(UnderlayProtocol::Udp, runtime.path_index);
    Ok(OpenedReliableCarrierStream {
        stream_id,
        path_instance_id: carrier.path_instance_id,
        max_offset,
        lane,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: udp_path_max_stream_payload_bytes(
            runtime.codec_limits,
            runtime.mux_limits,
        ),
        startup,
        commands,
        mux_limits: runtime.mux_limits,
        frames: frames_rx,
    })
}

async fn read_client_udp_stream_open_accept(
    recv: &mut UdpPathRecvStream,
    stream_id: StreamId,
    path_index: usize,
    path_instance_id: CarrierPathInstanceId,
    state: &ClientPathState,
    path_id: PathId,
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
            Frame::PathStatus {
                path_id: status_path_id,
                sequence,
                usage,
            } => {
                let _ = apply_client_udp_path_status(
                    state,
                    path_index,
                    path_instance_id,
                    path_id,
                    status_path_id,
                    sequence,
                    usage,
                )?;
            }
            Frame::SessionReady => {}
            Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
            _ => {
                return Err(RuntimeError::Protocol(
                    "unexpected QUIC UDP path stream open frame",
                ));
            }
        }
    }
}

#[cfg(test)]
#[path = "client_test.rs"]
mod tests;

async fn open_client_udp_datagram_stream(
    carrier: ClientUdpCarrierInstance,
    runtime: ClientUdpPathSessionRuntime,
) -> Result<ClientUdpDatagramStream, RuntimeError> {
    let (send, recv) = carrier.connection.open_bi().await?;
    let frames = spawn_quic_path_reader(recv, runtime.codec_limits, runtime.stream_frame_queue);
    Ok(ClientUdpDatagramStream {
        send,
        frames,
        path_id: PathId(runtime.path_index as u16),
        path_instance_id: carrier.path_instance_id,
        runtime,
    })
}
