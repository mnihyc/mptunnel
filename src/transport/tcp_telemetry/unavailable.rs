//! Fallback for hosts without an implemented native TCP telemetry backend.

use super::{TcpTelemetrySnapshot, TcpTelemetrySource};
use std::io;

#[derive(Debug)]
pub(super) struct PlatformTcpTelemetrySocket;

impl PlatformTcpTelemetrySocket {
    pub(super) fn capture<S>(_socket: &S) -> io::Result<Self>
    where
        S: TcpTelemetrySource + ?Sized,
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "native TCP telemetry is unavailable on this platform",
        ))
    }

    pub(super) fn snapshot(&self) -> io::Result<Option<TcpTelemetrySnapshot>> {
        Ok(None)
    }
}
