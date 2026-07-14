//! QUIC path endpoint, stream framing, and carrier-local I/O helpers.

use super::*;
use tokio::net::lookup_host;

#[derive(Debug)]
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
    pub(super) connection: UdpPathConnection,
}

impl UdpPathSendStream {
    pub(super) fn transport_stream_mut(&mut self) -> &mut quic_transport::SendStream {
        &mut self.stream
    }
}

#[derive(Debug)]
pub(in crate::runtime) struct UdpPathRecvStream {
    stream: quic_transport::RecvStream,
}

impl UdpPathEndpoint {
    pub(super) async fn bind_server(
        path: &PathSpec,
        context: &ServerPathContext,
    ) -> Result<Self, RuntimeError> {
        let addr = resolve_first_socket_addr(path).await?;
        Ok(Self {
            endpoint: quic_transport::Endpoint::bind_server(
                addr,
                context.security.secret.as_bytes(),
                context.mux_limits,
            )
            .await?,
        })
    }

    pub(super) async fn bind_client(
        socket: crate::transport::CarrierSocket,
        runtime: &ClientUdpPathSessionRuntime,
    ) -> Result<Self, RuntimeError> {
        Ok(Self {
            endpoint: quic_transport::Endpoint::bind_client_socket(
                socket,
                runtime.security.secret.as_bytes(),
                runtime.mux_limits,
            )
            .await?,
        })
    }

    pub(super) async fn connect(
        &self,
        remote_addr: SocketAddr,
    ) -> Result<UdpPathConnection, RuntimeError> {
        Ok(UdpPathConnection {
            connection: self.endpoint.connect(remote_addr).await?,
        })
    }

    pub(super) async fn accept(&self) -> Option<UdpPathConnection> {
        self.endpoint
            .accept()
            .await
            .map(|connection| UdpPathConnection { connection })
    }

    #[cfg(test)]
    pub(in crate::runtime) fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.endpoint.local_addr()
    }
}

impl UdpPathConnection {
    pub(super) async fn open_bi(
        &self,
    ) -> Result<(UdpPathSendStream, UdpPathRecvStream), RuntimeError> {
        let (send, recv) = self.connection.open_bi().await?;
        Ok((
            UdpPathSendStream {
                stream: send,
                connection: self.clone(),
            },
            UdpPathRecvStream { stream: recv },
        ))
    }

    pub(super) async fn accept_bi(
        &self,
    ) -> Result<(UdpPathSendStream, UdpPathRecvStream), RuntimeError> {
        let (send, recv) = self.connection.accept_bi().await?;
        Ok((
            UdpPathSendStream {
                stream: send,
                connection: self.clone(),
            },
            UdpPathRecvStream { stream: recv },
        ))
    }

    pub(super) fn close(&self) {
        self.connection.close();
    }

    pub(super) fn capacity_probe_active(&self) -> bool {
        self.connection.measurement_active()
    }

    pub(super) async fn wait_for_capacity_probe_release(&self) {
        self.connection.wait_for_measurement_release().await;
    }

    pub(super) fn is_closed(&self) -> bool {
        self.connection.is_closed()
    }

    pub(super) fn retire_capacity_probe(&self, token: u64) -> bool {
        self.connection.retire_measurement(token)
    }

    pub(super) fn cancel_capacity_probe(&self, token: u64) -> bool {
        self.connection.cancel_measurement(token)
    }

    pub(super) fn confirm_capacity_probe_receipt(
        &self,
        token: u64,
        received_payload_bytes: u64,
        received_at: Instant,
    ) -> bool {
        self.connection
            .confirm_measurement_receipt(token, received_payload_bytes, received_at)
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
    ensure_quic_ordinary_frames(std::slice::from_ref(frame))?;
    quic_transport::write_frame(&mut send.stream, frame, codec_limits).await?;
    Ok(())
}

async fn udp_path_write_frames(
    send: &mut UdpPathSendStream,
    frames: &[Frame],
    codec_limits: CodecLimits,
) -> Result<(), RuntimeError> {
    ensure_quic_ordinary_frames(frames)?;
    quic_transport::write_frames(&mut send.stream, frames, codec_limits).await?;
    Ok(())
}

fn ensure_quic_ordinary_frames(frames: &[Frame]) -> Result<(), RuntimeError> {
    if frames.iter().any(|frame| {
        !matches!(
            frame.write_class(),
            crate::protocol::FrameWriteClass::Ordinary { .. }
        )
    }) {
        return Err(RuntimeError::Protocol(
            "QUIC measurement records require the dedicated writer",
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

pub(in crate::runtime) fn udp_path_finish_stream(
    send: &mut UdpPathSendStream,
) -> Result<(), RuntimeError> {
    Ok(quic_transport::finish_stream(&mut send.stream)?)
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
    match err {
        RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::Read(_)) => true,
        RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::UnexpectedEnd) => true,
        RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::Connection(_)) => true,
        _ => false,
    }
}

fn udp_runtime_error_is_expected_shutdown(err: &RuntimeError) -> bool {
    match err {
        RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::Read(_)) => true,
        RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::UnexpectedEnd) => true,
        RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::Connection(_)) => true,
        RuntimeError::RemoteClosed(CloseReason::Normal) => true,
        _ => false,
    }
}

pub(super) fn warn_unexpected_udp_runtime_error(message: &str, err: &RuntimeError) {
    if !udp_runtime_error_is_expected_shutdown(err) {
        eprintln!("warning: {message}: {err}");
    }
}

pub(super) fn quic_path_open_error_is_retryable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Udp(_)
            | RuntimeError::QuicCarrier(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
            | RuntimeError::ReliablePathSessionClosed
    )
}

pub(super) fn udp_path_command_queue(mux_limits: MuxLimits, _codec_limits: CodecLimits) -> usize {
    // This queue is a sender-service work queue, not a QUIC record-buffer queue.
    // QUIC reliable streams may split OwnerData into smaller records to reduce
    // stream head-of-line burst size, but that packetization detail must not
    // multiply the number of commands admitted above the carrier. Otherwise a
    // 12--32 KiB QUIC record cap would inflate the queue from the logical
    // product-flight budget to thousands of commands and recreate the hidden
    // backlog that caused zero-goodput bursts.  Keep queue capacity tied to the
    // logical sender quantum; the QUIC writer/flow-control path performs the
    // lower-level pacing.
    reliable_path_command_queue(mux_limits)
}

pub(super) async fn resolve_first_socket_addr(path: &PathSpec) -> Result<SocketAddr, RuntimeError> {
    let mut addrs = lookup_host((path.endpoint.host.as_str(), path.endpoint.port)).await?;
    addrs.next().ok_or(RuntimeError::Protocol(
        "QUIC UDP path endpoint resolved no socket addresses",
    ))
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
                Err(err) if udp_path_frame_finished(&err) => {
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
#[path = "io_test.rs"]
mod tests;
