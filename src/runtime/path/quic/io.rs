//! QUIC path endpoint, stream framing, and carrier-local I/O helpers.

use super::client::ClientUdpPathSessionRuntime;
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{CloseReason, DatagramFlowId, Frame};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    reliable_path_command_queue, reliable_stream_frame_queue_for_payload,
};
use crate::runtime::path::proof::PathProofTracker;
use crate::runtime::path::server_context::ServerPathContext;
use crate::scheduler::TrafficClass;
use crate::transport::PathSpec;
use crate::transport::quic as quic_transport;
use crate::transport::udp::UdpTransportError;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::net::lookup_host;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub(in crate::runtime) struct UdpPathEndpoint {
    endpoint: quic_transport::Endpoint,
}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct UdpPathConnection {
    pub(super) connection: quic_transport::Connection,
}

#[derive(Debug)]
pub(in crate::runtime) struct UdpPathSendStream {
    stream: quic_transport::SendStream,
}

#[derive(Debug)]
pub(in crate::runtime) struct UdpPathRecvStream {
    stream: quic_transport::RecvStream,
}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct UdpIpPacketSender {
    sender: quic_transport::IpPacketSender,
    limits: CodecLimits,
}

impl UdpIpPacketSender {
    pub(super) async fn send(&self, frame: &Frame) -> Result<(), RuntimeError> {
        match self.sender.send(frame, self.limits).await {
            Ok(()) => Ok(()),
            Err(quic_transport::QuicCarrierError::NativeDatagram(
                quinn::SendDatagramError::ConnectionLost(_),
            )) => Err(RuntimeError::ReliablePathRetired),
            Err(error) => Err(error.into()),
        }
    }
}

const QUIC_DEFAULT_STREAM_PRIORITY: i32 = 0;
const QUIC_LATENCY_STREAM_PRIORITY: i32 = 1;

fn quic_stream_priority(lane: TrafficClass) -> i32 {
    if lane.is_latency_sensitive() {
        QUIC_LATENCY_STREAM_PRIORITY
    } else {
        QUIC_DEFAULT_STREAM_PRIORITY
    }
}

impl UdpPathSendStream {
    /// QUIC stream priority orders locally buffered streams only. Quinn still
    /// owns connection flow control, congestion control, pacing, and recovery.
    pub(super) fn set_traffic_class(&mut self, lane: TrafficClass) -> Result<(), RuntimeError> {
        self.stream.set_priority(quic_stream_priority(lane))?;
        Ok(())
    }

    pub(super) async fn ip_packet_sender(
        &mut self,
        limits: CodecLimits,
    ) -> Result<UdpIpPacketSender, RuntimeError> {
        Ok(UdpIpPacketSender {
            sender: self.stream.ip_packet_sender().await?,
            limits,
        })
    }

    /// Cancels only a still-unanswered HTTP/3 request stream. Once an MPP
    /// response has started this is a no-op, preserving sibling flows sharing
    /// the same native datagram request stream.
    pub(super) fn cancel_pending_response(&mut self) -> bool {
        self.stream.cancel_pending_response()
    }
}

impl UdpPathRecvStream {
    pub(super) fn enable_ip_packets(&mut self) -> Result<(), RuntimeError> {
        self.stream.enable_ip_packets()?;
        Ok(())
    }
}

impl UdpPathEndpoint {
    pub(super) async fn bind_server(
        path: &PathSpec,
        context: &ServerPathContext,
    ) -> Result<Self, RuntimeError> {
        let addrs = resolve_udp_server_path_socket_addrs(path).await?;
        let mut last_error = None;
        for addr in addrs {
            match quic_transport::Endpoint::bind_server(
                addr,
                &context.tls,
                context.credential_admission.clone(),
                context.mux_limits,
            )
            .await
            {
                Ok(endpoint) => return Ok(Self { endpoint }),
                Err(err) => last_error = Some(err),
            }
        }
        Err(last_error
            .map(RuntimeError::from)
            .unwrap_or(RuntimeError::Protocol(
                "QUIC UDP path endpoint resolved no bindable socket addresses",
            )))
    }

    pub(super) async fn bind_client(
        socket: crate::transport::CarrierSocket,
        runtime: &ClientUdpPathSessionRuntime,
    ) -> Result<Self, RuntimeError> {
        Ok(Self {
            endpoint: quic_transport::Endpoint::bind_client_socket(
                socket,
                runtime.tls(),
                runtime.candidate_selector.clone(),
                runtime.mux_limits,
            )
            .await?,
        })
    }

    pub(super) async fn connect(
        &self,
        remote_addr: SocketAddr,
    ) -> Result<UdpPathConnection, RuntimeError> {
        Ok(UdpPathConnection::new(
            self.endpoint.connect(remote_addr).await?,
        ))
    }

    pub(super) fn migrate_destination_port(
        &self,
        socket: crate::transport::CarrierSocket,
        canonical_remote: SocketAddr,
        selected_remote: SocketAddr,
    ) -> Result<impl Future<Output = ()> + use<>, RuntimeError> {
        let receipt =
            self.endpoint
                .rebind_client_socket(socket, canonical_remote, selected_remote)?;
        Ok(receipt.wait())
    }

    pub(super) async fn accept(&self) -> Option<UdpPathConnection> {
        self.endpoint.accept().await.map(UdpPathConnection::new)
    }

    #[cfg(test)]
    pub(in crate::runtime) async fn accept_for_test(&self) -> Option<UdpPathConnection> {
        self.accept().await
    }

    pub(in crate::runtime) fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.endpoint.local_addr()
    }
}

impl UdpPathConnection {
    fn new(connection: quic_transport::Connection) -> Self {
        Self { connection }
    }

    pub(super) fn remote_address(&self) -> SocketAddr {
        self.connection.remote_address()
    }

    pub(super) fn delivery_activity_notify(&self) -> Arc<tokio::sync::Notify> {
        self.connection.delivery_activity_notify()
    }

    pub(super) fn is_locally_closed(&self) -> bool {
        self.connection.is_locally_closed()
    }

    #[cfg(feature = "lab-diagnostics")]
    pub(super) fn close_reason(&self) -> Option<String> {
        self.connection.close_reason()
    }

    pub(super) async fn open_bi(
        &self,
    ) -> Result<(UdpPathSendStream, UdpPathRecvStream), RuntimeError> {
        let (send, recv) = self.connection.open_bi().await?;
        Ok((
            UdpPathSendStream { stream: send },
            UdpPathRecvStream { stream: recv },
        ))
    }

    pub(super) async fn accept_bi(
        &self,
    ) -> Result<(UdpPathSendStream, UdpPathRecvStream), RuntimeError> {
        let (send, recv) = self.connection.accept_bi().await?;
        Ok((
            UdpPathSendStream { stream: send },
            UdpPathRecvStream { stream: recv },
        ))
    }

    pub(super) fn close(&self) {
        self.connection.close();
    }

    pub(super) fn is_closed(&self) -> bool {
        self.connection.is_closed()
    }

    pub(super) async fn wait_closed(&self) {
        self.connection.wait_closed().await;
    }

    pub(super) fn rtt(&self) -> std::time::Duration {
        self.connection.rtt()
    }
}

pub(in crate::runtime) async fn udp_path_read_frame(
    recv: &mut UdpPathRecvStream,
    codec_limits: CodecLimits,
) -> Result<Frame, RuntimeError> {
    Ok(quic_transport::read_frame(&mut recv.stream, codec_limits).await?)
}

pub(in crate::runtime) async fn udp_path_write_frame(
    send: &mut UdpPathSendStream,
    frame: &Frame,
    codec_limits: CodecLimits,
) -> Result<(), RuntimeError> {
    ensure_quic_data_plane_frames(std::slice::from_ref(frame))?;
    quic_transport::write_frame(&mut send.stream, frame, codec_limits).await?;
    Ok(())
}

pub(super) async fn udp_path_write_datagram_refusal(
    send: &mut UdpPathSendStream,
    flow_id: DatagramFlowId,
    evicted: Option<DatagramFlowId>,
    max_refusals: usize,
    codec_limits: CodecLimits,
) -> Result<(), RuntimeError> {
    quic_transport::write_datagram_refusal(
        &mut send.stream,
        flow_id,
        evicted,
        max_refusals,
        codec_limits,
    )
    .await?;
    Ok(())
}

pub(super) fn udp_path_retain_datagram_denial(
    send: &mut UdpPathSendStream,
    flow_id: DatagramFlowId,
    evicted: Option<DatagramFlowId>,
    max_refusals: usize,
) -> Result<(), RuntimeError> {
    quic_transport::retain_datagram_denial(&mut send.stream, flow_id, evicted, max_refusals)?;
    Ok(())
}

async fn udp_path_write_frames(
    send: &mut UdpPathSendStream,
    frames: &[Frame],
    codec_limits: CodecLimits,
) -> Result<(), RuntimeError> {
    ensure_quic_data_plane_frames(frames)?;
    quic_transport::write_frames(&mut send.stream, frames, codec_limits).await?;
    Ok(())
}

fn ensure_quic_data_plane_frames(frames: &[Frame]) -> Result<(), RuntimeError> {
    if frames.iter().any(Frame::is_path_capacity) {
        return Err(RuntimeError::Protocol(
            "PATH_CAPACITY frames are not valid on QUIC carriers",
        ));
    }
    Ok(())
}

pub(super) async fn flush_udp_frame_batch(
    send: &mut UdpPathSendStream,
    frames: &mut Vec<Frame>,
    codec_limits: CodecLimits,
) -> Result<(), RuntimeError> {
    if frames.is_empty() {
        return Ok(());
    }
    udp_path_write_frames(send, frames, codec_limits).await?;
    frames.clear();
    Ok(())
}

pub(super) async fn flush_udp_frame_batch_with_path_proofs(
    send: &mut UdpPathSendStream,
    frames: &mut Vec<Frame>,
    codec_limits: CodecLimits,
    path_proofs: &mut PathProofTracker,
) -> Result<(), RuntimeError> {
    if frames.is_empty() {
        return Ok(());
    }
    udp_path_write_frames(send, frames, codec_limits).await?;
    for frame in frames.iter() {
        path_proofs.record_sent_frame(frame);
    }
    frames.clear();
    Ok(())
}

pub(super) async fn flush_udp_frame_batch_with_path_proofs_interlocked<F>(
    send: &mut UdpPathSendStream,
    frames: &mut Vec<Frame>,
    codec_limits: CodecLimits,
    path_proofs: &mut PathProofTracker,
    carrier_frames: &mut mpsc::Receiver<Result<Frame, RuntimeError>>,
    deferred_input: &mut Option<Result<Frame, RuntimeError>>,
    try_route_frame: F,
) -> Result<usize, RuntimeError>
where
    F: FnMut(Frame) -> Result<Option<Frame>, RuntimeError>,
{
    if frames.is_empty() {
        return Ok(0);
    }
    debug_assert!(deferred_input.is_none());
    let write = udp_path_write_frames(send, frames, codec_limits);
    let (write_result, routed_frames) = await_udp_write_while_routing_stream_frames(
        write,
        carrier_frames,
        deferred_input,
        try_route_frame,
    )
    .await;
    write_result?;
    for frame in frames.iter() {
        path_proofs.record_sent_frame(frame);
    }
    frames.clear();
    Ok(routed_frames)
}

pub(super) async fn await_udp_write_while_routing_stream_frames<W, T, F>(
    write: W,
    carrier_frames: &mut mpsc::Receiver<Result<Frame, RuntimeError>>,
    deferred_input: &mut Option<Result<Frame, RuntimeError>>,
    mut try_route_frame: F,
) -> (T, usize)
where
    W: std::future::Future<Output = T>,
    F: FnMut(Frame) -> Result<Option<Frame>, RuntimeError>,
{
    tokio::pin!(write);
    let mut routed_frames = 0usize;
    loop {
        tokio::select! {
            biased;
            result = &mut write => return (result, routed_frames),
            incoming = carrier_frames.recv(), if deferred_input.is_none() => {
                match incoming {
                    Some(Ok(frame)) => match try_route_frame(frame) {
                        Ok(None) => routed_frames = routed_frames.saturating_add(1),
                        Ok(Some(frame)) => *deferred_input = Some(Ok(frame)),
                        Err(err) => *deferred_input = Some(Err(err)),
                    },
                    Some(Err(err)) => *deferred_input = Some(Err(err)),
                    None => {
                        *deferred_input = Some(Err(RuntimeError::ReliablePathSessionClosed));
                    }
                }
            }
        }
    }
}

pub(in crate::runtime) async fn udp_path_finish_stream(
    send: &mut UdpPathSendStream,
) -> Result<(), RuntimeError> {
    Ok(quic_transport::finish_stream(&mut send.stream).await?)
}

pub(in crate::runtime) async fn udp_path_reject_stream(
    send: &mut UdpPathSendStream,
) -> Result<(), RuntimeError> {
    Ok(send.stream.reject().await?)
}

// Product-level UDP reliable frame size. This is intentionally the same kind of
// BDP/service quantum used by TCP. Do not cap this to a QUIC packet train: doing
// so turns the carrier record size into the application pacing unit and
// underfeeds QUIC. QUIC-specific recordization is performed inside
// transport::quic while preserving this product quantum.
pub(super) fn udp_path_max_stream_payload_bytes(
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
) -> usize {
    codec_limits
        .max_payload_bytes
        .max(1)
        .min(mux_limits.max_reliable_relay_chunk_bytes)
        .max(1)
}

pub(super) fn udp_reliable_stream_frame_queue(
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
) -> usize {
    reliable_stream_frame_queue_for_payload(
        mux_limits,
        udp_path_max_stream_payload_bytes(codec_limits, mux_limits),
    )
}

pub(super) fn udp_path_frame_finished(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::QuicCarrier(
            quic_transport::QuicCarrierError::Read(_)
                | quic_transport::QuicCarrierError::Connection(_)
        )
    )
}

pub(super) fn udp_path_input_finished(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::StreamFinished)
    )
}

fn udp_runtime_error_is_expected_shutdown(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::QuicCarrier(
            quic_transport::QuicCarrierError::Read(_)
                | quic_transport::QuicCarrierError::StreamFinished
                | quic_transport::QuicCarrierError::Connection(_)
        ) | RuntimeError::RemoteClosed(CloseReason::Normal)
    )
}

pub(super) fn warn_unexpected_udp_runtime_error(message: &str, err: &RuntimeError) {
    if !udp_runtime_error_is_expected_shutdown(err) {
        crate::observability::process_event!(Warn, "quic", "runtime_error", "{message}: {err}");
    }
}

pub(super) fn udp_path_command_queue(mux_limits: MuxLimits, _codec_limits: CodecLimits) -> usize {
    // This queue is a sender-service work queue, not a QUIC record-buffer queue.
    // QUIC reliable streams may split OriginalData into smaller records to reduce
    // stream head-of-line burst size, but that packetization detail must not
    // multiply the number of commands admitted above the carrier. Otherwise a
    // 12--32 KiB QUIC record cap would inflate the queue from the logical
    // product-flight budget to thousands of commands and recreate the hidden
    // backlog that caused zero-goodput bursts.  Keep queue capacity tied to the
    // logical sender quantum; the QUIC writer/flow-control path performs the
    // lower-level pacing.
    reliable_path_command_queue(mux_limits)
}

/// Server listeners use local host resolution; client carriers resolve through
/// their host-selected network before applying QUIC's address-attempt policy.
async fn resolve_udp_server_path_socket_addrs(
    path: &PathSpec,
) -> Result<Vec<SocketAddr>, RuntimeError> {
    if !path.endpoint.ports().is_single() {
        return Err(RuntimeError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "server carrier paths require one listener port; forward any advertised port range to that listener",
        )));
    }
    let resolved = lookup_host((path.endpoint.host.as_str(), path.endpoint.ports().first()))
        .await?
        .collect::<Vec<_>>();
    usable_udp_path_socket_addrs(path, resolved)
}

pub(super) fn usable_udp_path_socket_addrs(
    path: &PathSpec,
    resolved: Vec<SocketAddr>,
) -> Result<Vec<SocketAddr>, RuntimeError> {
    if resolved.is_empty() {
        return Err(RuntimeError::Udp(UdpTransportError::ResolutionEmpty(
            path.endpoint.authority(),
        )));
    }
    let compatible = compatible_udp_path_socket_addrs(resolved, path.binding.source_ip);
    if compatible.is_empty() {
        Err(RuntimeError::Udp(UdpTransportError::NoCompatibleAddress))
    } else {
        Ok(compatible)
    }
}

fn compatible_udp_path_socket_addrs(
    resolved: impl IntoIterator<Item = SocketAddr>,
    source_ip: Option<IpAddr>,
) -> Vec<SocketAddr> {
    let mut compatible = Vec::new();
    for addr in resolved {
        if source_ip.is_some_and(|source| source.is_ipv4() != addr.is_ipv4())
            || compatible.contains(&addr)
        {
            continue;
        }
        compatible.push(addr);
    }
    compatible
}

pub(super) fn spawn_quic_path_reader(
    mut recv: UdpPathRecvStream,
    codec_limits: CodecLimits,
    queue_size: usize,
) -> mpsc::Receiver<Result<Frame, RuntimeError>> {
    let (frames_tx, frames_rx) = mpsc::channel(queue_size);
    tokio::spawn(async move {
        loop {
            let received = tokio::select! {
                _ = frames_tx.closed() => return,
                received = udp_path_read_frame(&mut recv, codec_limits) => received,
            };
            let frame = match received {
                Ok(frame) => Ok(frame),
                Err(err) if udp_path_input_finished(&err) => Err(err),
                Err(err) if udp_path_frame_finished(&err) => {
                    #[cfg(feature = "lab-diagnostics")]
                    crate::lab_diagnostics::lab_diagnostic(
                        "quic_path_reader_terminal",
                        format_args!("error={err}"),
                    );
                    Err(RuntimeError::ReliablePathSessionClosed)
                }
                Err(err) => Err(err),
            };
            let done = frame.is_err();
            if frames_tx.send(frame).await.is_err() || done {
                return;
            }
        }
    });
    frames_rx
}

#[cfg(test)]
#[path = "tests_io.rs"]
mod tests;
