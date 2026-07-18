//! Optional exact-socket TCP telemetry.
//!
//! Transport owns native socket inspection and reports neutral counters. Runtime
//! policy decides whether those counters may influence path capacity.

use std::io;
use tokio::net::TcpStream;

#[cfg(target_os = "macos")]
mod apple;
#[cfg(any(target_os = "linux", target_os = "android"))]
mod linux;
#[cfg(windows)]
mod windows;

/// Coherent RTT fields available from one native snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpNativeRtt {
    pub(crate) srtt_us: u32,
    pub(crate) rttvar_us: Option<u32>,
}

/// Congestion-flight fields normalized to bytes at the platform boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpNativeFlight {
    pub(crate) bytes_in_flight: Option<u64>,
    pub(crate) inflight_limit_bytes: u64,
    pub(crate) inflight_hi_bytes: Option<u64>,
}

/// Counters that must advance together to produce a loss-rate observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpNativeLossCounters {
    pub(crate) retransmits: u32,
    pub(crate) data_segments_out: u32,
}

/// Capability-graded native counters captured from one exact carrier socket.
///
/// Every `Option` is independent evidence. Missing host fields are unknown and
/// must never be projected as measured zero, false, or delivery authority.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct TcpNativeSnapshot {
    pub(crate) rtt: Option<TcpNativeRtt>,
    pub(crate) flight: Option<TcpNativeFlight>,
    pub(crate) notsent_bytes: Option<u32>,
    pub(crate) bytes_acked: Option<u64>,
    /// Opaque monotonic retransmission evidence for this exact socket.
    ///
    /// Platforms report either segments or bytes, so only advancement is
    /// comparable. Runtime policy must not interpret the magnitude.
    pub(crate) retransmission_counter: Option<u64>,
    pub(crate) loss: Option<TcpNativeLossCounters>,
    pub(crate) pacing_rate_bytes_per_second: Option<u64>,
    pub(crate) delivery_rate_bytes_per_second: Option<u64>,
    pub(crate) app_limited: Option<bool>,
}

impl TcpNativeSnapshot {
    pub(crate) fn has_evidence(self) -> bool {
        self.rtt.is_some()
            || self.flight.is_some()
            || self.notsent_bytes.is_some()
            || self.bytes_acked.is_some()
            || self.retransmission_counter.is_some()
            || self.loss.is_some()
            || self.pacing_rate_bytes_per_second.is_some()
            || self.delivery_rate_bytes_per_second.is_some()
            || self.app_limited.is_some()
    }
}

/// Owns a duplicate of the carrier socket when native telemetry is supported.
#[derive(Debug)]
pub(crate) struct TcpTelemetrySocket {
    #[cfg(target_os = "macos")]
    platform: apple::PlatformTcpTelemetrySocket,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    platform: linux::PlatformTcpTelemetrySocket,
    #[cfg(windows)]
    platform: windows::PlatformTcpTelemetrySocket,
}

impl TcpTelemetrySocket {
    /// Distinguishes an unsupported host from failure to capture a supported
    /// native socket. Either outcome remains optional to carrier operation.
    pub(crate) fn capture(socket: &TcpStream) -> io::Result<Option<Self>> {
        #[cfg(target_os = "macos")]
        {
            apple::PlatformTcpTelemetrySocket::capture(socket)
                .map(|platform| Some(Self { platform }))
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            linux::PlatformTcpTelemetrySocket::capture(socket)
                .map(|platform| Some(Self { platform }))
        }
        #[cfg(windows)]
        {
            windows::PlatformTcpTelemetrySocket::capture(socket)
                .map(|platform| Some(Self { platform }))
        }
        #[cfg(not(any(
            target_os = "macos",
            target_os = "linux",
            target_os = "android",
            windows
        )))]
        {
            let _ = socket;
            Ok(None)
        }
    }

    /// Returns `None` when the host exposes no understood native counter group.
    pub(crate) fn snapshot(&self) -> io::Result<Option<TcpNativeSnapshot>> {
        #[cfg(target_os = "macos")]
        {
            self.platform.snapshot()
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            self.platform.snapshot()
        }
        #[cfg(windows)]
        {
            self.platform.snapshot()
        }
        #[cfg(not(any(
            target_os = "macos",
            target_os = "linux",
            target_os = "android",
            windows
        )))]
        {
            let _ = self;
            Ok(None)
        }
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
#[path = "tcp_telemetry/linux_test.rs"]
mod tests;
