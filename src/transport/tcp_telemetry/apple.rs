//! macOS implementation using the public `TCP_CONNECTION_INFO` socket option.

use super::{TcpNativeFlight, TcpNativeRtt, TcpNativeSnapshot};
use std::io;
use std::mem::{offset_of, size_of, zeroed};
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use tokio::net::TcpStream;

const TCP_CONNECTION_INFO_MIN_BYTES: usize =
    offset_of!(libc::tcp_connection_info, tcpi_rttvar) + size_of::<u32>();
const XNU_UNBOUNDED_SSTHRESH_BYTES: u32 = 65_535 << 14;

#[derive(Debug)]
pub(super) struct PlatformTcpTelemetrySocket {
    fd: OwnedFd,
}

impl PlatformTcpTelemetrySocket {
    pub(super) fn capture(socket: &TcpStream) -> io::Result<Self> {
        Ok(Self {
            fd: socket.as_fd().try_clone_to_owned()?,
        })
    }

    pub(super) fn snapshot(&self) -> io::Result<Option<TcpNativeSnapshot>> {
        // SAFETY: the zero value is valid for this C telemetry record.
        let mut info: libc::tcp_connection_info = unsafe { zeroed() };
        let mut returned = libc::socklen_t::try_from(size_of::<libc::tcp_connection_info>())
            .unwrap_or(libc::socklen_t::MAX);
        // SAFETY: `info` is writable for `returned` bytes and the duplicated
        // descriptor remains owned by `self` throughout the call.
        let result = unsafe {
            libc::getsockopt(
                self.fd.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_CONNECTION_INFO,
                (&mut info as *mut libc::tcp_connection_info).cast(),
                &mut returned,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        // Older XNU versions expose a shorter suffix, while the RTT and window
        // prefix consumed below has remained stable.
        if usize::try_from(returned).unwrap_or(0) < TCP_CONNECTION_INFO_MIN_BYTES {
            return Ok(None);
        }
        Ok(snapshot_from_connection_info(&info))
    }
}

fn snapshot_from_connection_info(info: &libc::tcp_connection_info) -> Option<TcpNativeSnapshot> {
    let inflight_limit_bytes = u64::from(info.tcpi_snd_cwnd);
    let flight = (inflight_limit_bytes > 0).then_some(TcpNativeFlight {
        // `tcpi_snd_sbbytes` is total socket-buffer occupancy, not exact
        // network flight, so it remains unknown here.
        bytes_in_flight: None,
        inflight_limit_bytes,
        inflight_hi_bytes: (info.tcpi_snd_ssthresh > 0).then_some(
            if info.tcpi_snd_ssthresh >= XNU_UNBOUNDED_SSTHRESH_BYTES {
                inflight_limit_bytes
            } else {
                u64::from(info.tcpi_snd_ssthresh)
            },
        ),
    });
    let snapshot = TcpNativeSnapshot {
        // XNU's exported TCP clock has one-millisecond granularity.
        rtt: Some(TcpNativeRtt {
            srtt_us: info.tcpi_srtt.saturating_mul(1_000),
            rttvar_us: Some(info.tcpi_rttvar.saturating_mul(1_000)),
        }),
        flight,
        notsent_bytes: None,
        bytes_acked: None,
        loss: None,
        pacing_rate_bytes_per_second: None,
        delivery_rate_bytes_per_second: None,
        app_limited: None,
    };
    snapshot.has_evidence().then_some(snapshot)
}

#[cfg(test)]
#[path = "apple_test.rs"]
mod tests;
