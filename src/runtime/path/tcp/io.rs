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
use tokio::sync::mpsc;

pub(in crate::runtime) type EncryptedTcpReader = EncryptedFramedReader<TcpStream>;
pub(in crate::runtime) type EncryptedTcpWriter = EncryptedFramedWriter<TcpStream>;

/// Observes one completely authenticated and decoded frame before bounded
/// actor delivery. Session-wide terminal publication uses this boundary so a
/// blocked ordered writer cannot defer a peer SESSION_CLOSE behind its write.
pub(in crate::runtime) fn spawn_encrypted_tcp_reader_with_observer<Observe>(
    reader: EncryptedTcpReader,
    queue_size: usize,
    mut observe: Observe,
) -> mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>>
where
    Observe: FnMut(&Frame) + Send + 'static,
{
    spawn_encrypted_tcp_reader_with_observers(
        reader,
        queue_size,
        move |frame| observe(frame),
        |_| {},
    )
}

/// Adds an out-of-band native-terminal observer to the authenticated reader.
/// The terminal callback runs immediately after a read/decode error and before
/// delivery of that error through a bounded actor queue can block.
pub(in crate::runtime) fn spawn_encrypted_tcp_reader_with_observers<Observe, ObserveTerminal>(
    mut reader: EncryptedTcpReader,
    queue_size: usize,
    mut observe: Observe,
    mut observe_terminal: ObserveTerminal,
) -> mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>>
where
    Observe: FnMut(&Frame) + Send + 'static,
    ObserveTerminal: FnMut(&EncryptedFramedTransportError) + Send + 'static,
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
            } else if let Err(error) = frame.as_ref() {
                observe_terminal(error);
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
        let (terminal_tx, terminal_rx) = oneshot::channel();
        let mut terminal_tx = Some(terminal_tx);
        let mut frames = spawn_encrypted_tcp_reader_with_observers(
            server_reader,
            1,
            |_| {},
            move |_| {
                if let Some(terminal_tx) = terminal_tx.take() {
                    let _ = terminal_tx.send(());
                }
            },
        );

        client
            .write_frame(&Frame::Ping { nonce: 91 })
            .await
            .expect("write frame before native terminal");
        client.flush().await.expect("flush frame before terminal");
        drop(client);

        tokio::time::timeout(Duration::from_secs(5), terminal_rx)
            .await
            .expect("native terminal observer timeout")
            .expect("native terminal observer dropped");
        assert!(matches!(
            frames
                .try_recv()
                .expect("actor queue retains decoded frame"),
            Ok(Frame::Ping { nonce: 91 })
        ));
    }
}
