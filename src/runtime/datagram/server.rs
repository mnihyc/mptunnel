//! Target-side UDP flow service shared by TCP and QUIC carrier sessions.

use crate::mux::MuxLimits;
use crate::outbound::{self, DnsConfig, OutboundConfig, TargetProtocol};
use crate::protocol::{DatagramFlowId, DatagramId, Frame};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::ReliablePathCommandSender;
use crate::runtime::path::{
    AcceptedServerDatagramFlow, ServerDatagramOpenError, ServerDatagramOpenRequest,
    ServerDatagramPort, ServerDatagramPortBackend, ServerDatagramRequest, ServerStreamPort,
};
use crate::scheduler::FlowLane;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;

const OUTBOUND_UDP_RECV_BUFFER_BYTES: usize = u16::MAX as usize;

/// Composition-owned UDP target policy and worker factory.
pub(in crate::runtime) struct ServerDatagramService {
    outbound: OutboundConfig,
    outbound_dns: DnsConfig,
    outbound_connect_timeout: Duration,
    mux_limits: MuxLimits,
    reliable_streams: ServerStreamPort,
}

impl ServerDatagramService {
    pub(in crate::runtime) fn path_port(
        outbound: OutboundConfig,
        outbound_dns: DnsConfig,
        outbound_connect_timeout: Duration,
        mux_limits: MuxLimits,
        reliable_streams: ServerStreamPort,
    ) -> ServerDatagramPort {
        ServerDatagramPort::new(Arc::new(Self {
            outbound,
            outbound_dns,
            outbound_connect_timeout,
            mux_limits,
            reliable_streams,
        }))
    }
}

impl ServerDatagramPortBackend for ServerDatagramService {
    fn open<'a>(
        &'a self,
        request: ServerDatagramOpenRequest,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<AcceptedServerDatagramFlow, ServerDatagramOpenError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let ServerDatagramOpenRequest {
                session_id,
                flow_id,
                target,
                commands,
            } = request;
            if let Err(error) = outbound::validate_target(&target) {
                return Err(ServerDatagramOpenError::new(error.into()));
            }
            if let Err(error) = self.outbound.ensure_supports(TargetProtocol::Udp) {
                return Err(ServerDatagramOpenError::new(error.into()));
            }
            let realtime_registration = self.reliable_streams.register_realtime_flow(session_id);
            let outbound_socket = match outbound::connect_udp(
                &self.outbound,
                &self.outbound_dns,
                &target,
                self.outbound_connect_timeout,
            )
            .await
            {
                Ok(socket) => socket,
                Err(error) => {
                    return Err(ServerDatagramOpenError::holding(
                        RuntimeError::OutboundConnect(error),
                        realtime_registration,
                    ));
                }
            };
            let requests = spawn_server_datagram_flow_worker(
                flow_id,
                outbound_socket,
                commands,
                self.mux_limits,
            );
            Ok(AcceptedServerDatagramFlow::holding(
                flow_id,
                requests,
                realtime_registration,
            ))
        })
    }
}

fn server_datagram_request_queue_len(mux_limits: MuxLimits) -> usize {
    let unit = mux_limits.max_payload_bytes.max(1);
    mux_limits
        .max_datagram_queue_bytes
        .saturating_div(unit)
        .max(1)
}

pub(in crate::runtime) fn spawn_server_datagram_flow_worker(
    flow_id: DatagramFlowId,
    mut outbound_socket: outbound::OutboundUdpSocket,
    commands: ReliablePathCommandSender,
    mux_limits: MuxLimits,
) -> mpsc::Sender<ServerDatagramRequest> {
    let (requests_tx, mut requests_rx) =
        mpsc::channel::<ServerDatagramRequest>(server_datagram_request_queue_len(mux_limits));
    tokio::spawn(async move {
        let response_buffer_len = mux_limits
            .max_payload_bytes
            .min(OUTBOUND_UDP_RECV_BUFFER_BYTES);
        let mut response_buffer = bytes::BytesMut::zeroed(response_buffer_len);
        let mut pending_ttls = VecDeque::<(Instant, u32, DatagramId)>::new();
        loop {
            prune_server_pending_ttls(&mut pending_ttls);
            tokio::select! {
                biased;
                received = async {
                    response_buffer.resize(response_buffer_len, 0);
                    outbound_socket.recv(&mut response_buffer[..]).await
                } => {
                    let len = match received {
                        Ok(len) => len,
                        Err(err) => {
                            eprintln!("warning: UDP outbound receive failed: {err}");
                            let _ = try_send_server_datagram_realtime_frame(
                                &commands,
                                Frame::DatagramClose { flow_id },
                            );
                            break;
                        }
                    };
                    response_buffer.truncate(len);
                    let Some((ttl_ms, datagram_id)) =
                        server_next_response_ttl(&mut pending_ttls)
                    else {
                        continue;
                    };
                    let payload = response_buffer.split_to(len).freeze();
                    let frame = Frame::DatagramData {
                        flow_id,
                        datagram_id,
                        ttl_ms,
                        payload,
                    };
                    match try_send_server_datagram_realtime_frame(&commands, frame) {
                        Ok(()) => {}
                        Err(RuntimeError::SenderServiceBlocked) => {
                            #[cfg(feature = "lab-diagnostics")]
                            lab_diagnostic(
                                "server_datagram_response_dropped",
                                format_args!(
                                    "flow_id={} datagram_id={} payload_bytes={} reason=carrier_credit",
                                    flow_id.0,
                                    datagram_id.0,
                                    len,
                                ),
                            );
                            continue;
                        }
                        Err(_) => break,
                    }
                }
                request = requests_rx.recv() => {
                    let Some(request) = request else {
                        break;
                    };
                    if request.ttl_ms == 0 {
                        continue;
                    }
                    match outbound_socket.send(&request.payload).await {
                        Ok(_) => {
                            pending_ttls.push_back((
                                Instant::now() + Duration::from_millis(u64::from(request.ttl_ms)),
                                request.ttl_ms,
                                request.datagram_id,
                            ));
                        }
                        Err(err) => {
                            eprintln!("warning: UDP outbound send failed: {err}");
                        }
                    }
                }
            }
        }
    });
    requests_tx
}

pub(in crate::runtime) fn try_send_server_datagram_realtime_frame(
    commands: &ReliablePathCommandSender,
    frame: Frame,
) -> Result<(), RuntimeError> {
    debug_assert!(matches!(
        frame,
        Frame::DatagramData { .. } | Frame::DatagramFeedback { .. } | Frame::DatagramClose { .. }
    ));
    commands.try_enqueue_admitted_frame(frame, FlowLane::RealtimeDatagram)
}

fn prune_server_pending_ttls(pending_ttls: &mut VecDeque<(Instant, u32, DatagramId)>) {
    let now = Instant::now();
    while pending_ttls
        .front()
        .is_some_and(|(deadline, _, _)| *deadline <= now)
    {
        pending_ttls.pop_front();
    }
}

fn server_next_response_ttl(
    pending_ttls: &mut VecDeque<(Instant, u32, DatagramId)>,
) -> Option<(u32, DatagramId)> {
    prune_server_pending_ttls(pending_ttls);
    pending_ttls
        .pop_front()
        .map(|(_, ttl_ms, datagram_id)| (ttl_ms, datagram_id))
}

#[cfg(test)]
#[path = "server_test.rs"]
mod tests;
