//! Fallback for hosts without an implemented native TCP telemetry backend.

use super::TcpTelemetrySnapshot;
use std::io;
use tokio::net::TcpStream;

#[derive(Debug)]
pub(super) struct PlatformTcpTelemetrySocket;

impl PlatformTcpTelemetrySocket {
    pub(super) fn capture(_socket: &TcpStream) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "native TCP telemetry is unavailable on this platform",
        ))
    }

    pub(super) fn snapshot(&self) -> io::Result<Option<TcpTelemetrySnapshot>> {
        Ok(None)
    }
}
