//! Optional exact-socket TCP telemetry.
//!
//! Transport owns native socket inspection and reports neutral counters. Runtime
//! policy decides whether those counters may influence path capacity.

use std::io;
use tokio::net::TcpStream;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(not(target_os = "linux"))]
mod unavailable;

#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(not(target_os = "linux"))]
use unavailable as platform;

/// Native sender counters captured from one exact carrier socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpTelemetrySnapshot {
    pub(crate) app_limited: bool,
    pub(crate) retransmits: u32,
    pub(crate) min_rtt_us: u32,
    pub(crate) srtt_us: u32,
    pub(crate) rttvar_us: u32,
    pub(crate) snd_mss_bytes: u32,
    pub(crate) unacked_packets: u32,
    pub(crate) snd_ssthresh_packets: u32,
    pub(crate) snd_cwnd_packets: u32,
    pub(crate) pacing_rate_bytes_per_second: u64,
    pub(crate) bytes_acked: u64,
    pub(crate) notsent_bytes: u32,
    pub(crate) data_segments_out: u32,
    pub(crate) delivery_rate_bytes_per_second: u64,
}

/// Owns a duplicate of the carrier socket when native telemetry is supported.
#[derive(Debug)]
pub(crate) struct TcpTelemetrySocket {
    platform: platform::PlatformTcpTelemetrySocket,
}

impl TcpTelemetrySocket {
    /// Captures the socket identity without moving ownership from its carrier.
    pub(crate) fn capture(socket: &TcpStream) -> io::Result<Self> {
        platform::PlatformTcpTelemetrySocket::capture(socket).map(|platform| Self { platform })
    }

    /// Returns `None` when the host cannot provide the required counter set.
    pub(crate) fn snapshot(&self) -> io::Result<Option<TcpTelemetrySnapshot>> {
        self.platform.snapshot()
    }
}

#[cfg(all(test, target_os = "linux"))]
#[path = "tcp_telemetry/linux_test.rs"]
mod tests;
