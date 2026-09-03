//! Shared encrypted TCP I/O primitives.
//!
//! Both TCP roles use the same framed halves and close classification; keeping
//! them here prevents client policy from becoming an accidental server API.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_perf_record;
use crate::protocol::Frame;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::reliable_path_frame_pacing_bytes;
use crate::transport::encrypted::{
    EncryptedFramedReader, EncryptedFramedTransportError, EncryptedFramedWriter,
};
#[cfg(feature = "lab-diagnostics")]
use std::time::Instant;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

pub(in crate::runtime) type EncryptedTcpReader = EncryptedFramedReader<TcpStream>;
pub(in crate::runtime) type EncryptedTcpWriter = EncryptedFramedWriter<TcpStream>;

/// Observes one completely authenticated and decoded frame before bounded
/// actor delivery. Session-wide terminal publication uses this boundary so a
/// blocked ordered writer cannot defer a peer SESSION_CLOSE behind its write.
pub(in crate::runtime) fn spawn_encrypted_tcp_reader_with_observer<Observe>(
    mut reader: EncryptedTcpReader,
    queue_size: usize,
    mut observe: Observe,
) -> mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>>
where
    Observe: FnMut(&Frame) + Send + 'static,
{
    let (frames_tx, frames_rx) = mpsc::channel(queue_size);
    tokio::spawn(async move {
        loop {
            let frame = tokio::select! {
                _ = frames_tx.closed() => break,
                frame = reader.read_frame() => frame,
            };
            if let Ok(frame) = frame.as_ref() {
                observe(frame);
            }
            let done = frame.is_err();
            #[cfg(feature = "lab-diagnostics")]
            let bytes = frame
                .as_ref()
                .ok()
                .map(reliable_path_frame_pacing_bytes)
                .unwrap_or(0);
            #[cfg(feature = "lab-diagnostics")]
            let started = Instant::now();
            let send_result = frames_tx.send(frame).await;
            #[cfg(feature = "lab-diagnostics")]
            lab_perf_record("runtime.tcp_reader.queue_send", started.elapsed(), bytes);
            if send_result.is_err() || done {
                break;
            }
        }
    });
    frames_rx
}

/// Separates successful ordered frames from the server's owned native result.
///
/// The terminal callback freezes Product admission before the exact transport
/// error is moved through an unbounded one-shot result plane. A full ordered
/// frame queue therefore cannot defer carrier retirement or erase the prior
/// peer-close versus encrypted-error classification.
pub(in crate::runtime) fn spawn_encrypted_tcp_reader_with_terminal_result<
    Observe,
    ObserveTerminal,
>(
    mut reader: EncryptedTcpReader,
    queue_size: usize,
    mut observe: Observe,
    mut observe_terminal: ObserveTerminal,
) -> (
    mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>>,
    oneshot::Receiver<EncryptedFramedTransportError>,
)
where
    Observe: FnMut(&Frame) + Send + 'static,
    ObserveTerminal: FnMut(&EncryptedFramedTransportError) + Send + 'static,
{
    let (frames_tx, frames_rx) = mpsc::channel(queue_size);
    let (terminal_tx, terminal_rx) = oneshot::channel();
    tokio::spawn(async move {
        loop {
            let frame = tokio::select! {
                _ = frames_tx.closed() => break,
                frame = reader.read_frame() => frame,
            };
            match frame {
                Ok(frame) => {
                    observe(&frame);
                    #[cfg(feature = "lab-diagnostics")]
                    let bytes = reliable_path_frame_pacing_bytes(&frame);
                    #[cfg(feature = "lab-diagnostics")]
                    let started = Instant::now();
                    let send_result = frames_tx.send(Ok(frame)).await;
                    #[cfg(feature = "lab-diagnostics")]
                    lab_perf_record("runtime.tcp_reader.queue_send", started.elapsed(), bytes);
                    if send_result.is_err() {
                        break;
                    }
                }
                Err(error) => {
                    observe_terminal(&error);
                    let _ = terminal_tx.send(error);
                    break;
                }
            }
        }
    });
    (frames_rx, terminal_rx)
}

pub(in crate::runtime) fn encrypted_framed_peer_closed(
    err: &EncryptedFramedTransportError,
) -> bool {
    matches!(
        err,
        EncryptedFramedTransportError::Io(io)
            if matches!(
                io.kind(),
                std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::UnexpectedEof
            )
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Frame;
    use crate::runtime::CodecLimits;
    use crate::transport::encrypted::EncryptedFramedStream;
    use std::time::Duration;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn native_terminal_is_published_before_error_delivery_blocks_on_a_full_actor_queue() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind TCP reader test");
        let client_socket = TcpStream::connect(listener.local_addr().expect("listener address"));
        let server_socket = listener.accept();
        let (client_socket, server_socket) = tokio::join!(client_socket, server_socket);
        let client_socket = client_socket.expect("connect TCP reader test");
        let (server_socket, _) = server_socket.expect("accept TCP reader test");
        let client_tls = crate::transport::encrypted::test_client_tls_config();
        let server_tls = crate::transport::encrypted::test_server_tls_config();
        let codec_limits = CodecLimits::default();
        let (client, server) = tokio::join!(
            EncryptedFramedStream::connect(client_socket, &client_tls, codec_limits),
            EncryptedFramedStream::accept(server_socket, &server_tls, codec_limits),
        );
        let mut client = client.expect("client protected carrier");
        let server = server.expect("server protected carrier");
        let (server_reader, _server_writer) = server.split().expect("split protected carrier");
        let (observed_terminal_tx, observed_terminal_rx) = oneshot::channel();
        let mut observed_terminal_tx = Some(observed_terminal_tx);
        let (mut frames, native_terminal) = spawn_encrypted_tcp_reader_with_terminal_result(
            server_reader,
            1,
            |_| {},
            move |_| {
                if let Some(observed_terminal_tx) = observed_terminal_tx.take() {
                    let _ = observed_terminal_tx.send(());
                }
            },
        );

        client
            .write_frame(&Frame::Ping { nonce: 91 })
            .await
            .expect("write frame before native terminal");
        client.flush().await.expect("flush frame before terminal");
        drop(client);

        tokio::time::timeout(Duration::from_secs(5), observed_terminal_rx)
            .await
            .expect("native terminal observer timeout")
            .expect("native terminal observer dropped");
        let native_error = tokio::time::timeout(Duration::from_secs(5), native_terminal)
            .await
            .expect("native terminal result timeout")
            .expect("native terminal result dropped");
        assert!(
            encrypted_framed_peer_closed(&native_error),
            "ordinary authenticated peer EOF must retain its exact close classification"
        );
        assert!(matches!(
            frames
                .try_recv()
                .expect("actor queue retains decoded frame"),
            Ok(Frame::Ping { nonce: 91 })
        ));
    }
}
