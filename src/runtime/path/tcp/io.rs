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

pub(in crate::runtime) type EncryptedTcpReader =
    EncryptedFramedReader<tokio::io::ReadHalf<TcpStream>>;
pub(in crate::runtime) type EncryptedTcpWriter =
    EncryptedFramedWriter<tokio::io::WriteHalf<TcpStream>>;

/// Decouples socket reads from actor work while preserving frame order and the
/// first terminal transport error.
pub(in crate::runtime) fn spawn_encrypted_tcp_reader(
    mut reader: EncryptedTcpReader,
    queue_size: usize,
) -> mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>> {
    let (frames_tx, frames_rx) = mpsc::channel(queue_size);
    tokio::spawn(async move {
        loop {
            let frame = tokio::select! {
                _ = frames_tx.closed() => break,
                frame = reader.read_frame() => frame,
            };
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
