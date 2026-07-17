//! Windows implementation using the unprivileged per-socket `SIO_TCP_INFO` API.

use super::{TcpNativeFlight, TcpNativeRtt, TcpNativeSnapshot};
use std::io;
use std::mem::{size_of, zeroed};
use std::os::windows::io::{AsRawSocket, AsSocket, OwnedSocket};
use tokio::net::TcpStream;
use windows_sys::Win32::Networking::WinSock::{
    SIO_TCP_INFO, SOCKET, SOCKET_ERROR, TCP_INFO_v0, WSAGetLastError, WSAIoctl,
};

#[derive(Debug)]
pub(super) struct PlatformTcpTelemetrySocket {
    socket: OwnedSocket,
}

impl PlatformTcpTelemetrySocket {
    pub(super) fn capture(socket: &TcpStream) -> io::Result<Self> {
        Ok(Self {
            socket: socket.as_socket().try_clone_to_owned()?,
        })
    }

    pub(super) fn snapshot(&self) -> io::Result<Option<TcpNativeSnapshot>> {
        let version = 0_u32;
        // SAFETY: the zero value is valid for this C telemetry record.
        let mut info: TCP_INFO_v0 = unsafe { zeroed() };
        let mut returned = 0_u32;
        // SAFETY: input and output point to initialized storage of the declared
        // sizes. The duplicated Winsock socket remains owned by `self`.
        let result = unsafe {
            WSAIoctl(
                self.socket.as_raw_socket() as SOCKET,
                SIO_TCP_INFO,
                (&version as *const u32).cast(),
                size_of::<u32>() as u32,
                (&mut info as *mut TCP_INFO_v0).cast(),
                size_of::<TCP_INFO_v0>() as u32,
                &mut returned,
                std::ptr::null_mut(),
                None,
            )
        };
        if result == SOCKET_ERROR {
            // Winsock errors are not necessarily reflected by GetLastError.
            return Err(io::Error::from_raw_os_error(unsafe { WSAGetLastError() }));
        }
        if usize::try_from(returned).unwrap_or(0) < size_of::<TCP_INFO_v0>() {
            return Ok(None);
        }
        Ok(snapshot_from_tcp_info(&info))
    }
}

fn snapshot_from_tcp_info(info: &TCP_INFO_v0) -> Option<TcpNativeSnapshot> {
    let inflight_limit_bytes = u64::from(info.Cwnd);
    let flight = (inflight_limit_bytes > 0).then_some(TcpNativeFlight {
        bytes_in_flight: Some(u64::from(info.BytesInFlight)),
        inflight_limit_bytes,
        inflight_hi_bytes: Some(inflight_limit_bytes),
    });
    let snapshot = TcpNativeSnapshot {
        rtt: Some(TcpNativeRtt {
            srtt_us: info.RttUs,
            // Version 0 provides minimum RTT, not RTT variance.
            rttvar_us: None,
        }),
        flight,
        notsent_bytes: None,
        // BytesOut counts transmitted bytes, not cumulatively acknowledged bytes.
        bytes_acked: None,
        loss: None,
        pacing_rate_bytes_per_second: None,
        delivery_rate_bytes_per_second: None,
        app_limited: None,
    };
    snapshot.has_evidence().then_some(snapshot)
}

#[cfg(test)]
#[path = "windows_test.rs"]
mod tests;
