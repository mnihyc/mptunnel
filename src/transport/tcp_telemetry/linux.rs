//! Linux implementation of optional native TCP telemetry.

use super::{TcpNativeFlight, TcpNativeLossCounters, TcpNativeRtt, TcpNativeSnapshot};
use std::io;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use tokio::net::TcpStream;

pub(super) const TCP_INFO_V4_9_PREFIX_BYTES: usize = 168;

#[derive(Debug)]
pub(super) struct PlatformTcpTelemetrySocket {
    fd: OwnedFd,
}

impl PlatformTcpTelemetrySocket {
    /// Duplicates the exact carrier fd before higher-level framing hides it.
    pub(super) fn capture(socket: &TcpStream) -> io::Result<Self> {
        Ok(Self {
            fd: socket.as_fd().try_clone_to_owned()?,
        })
    }

    /// Missing or truncated kernel telemetry is not a carrier failure.
    pub(super) fn snapshot(&self) -> io::Result<Option<TcpNativeSnapshot>> {
        let mut bytes = [0u8; TCP_INFO_V4_9_PREFIX_BYTES];
        let mut returned = libc::socklen_t::try_from(bytes.len()).unwrap_or(libc::socklen_t::MAX);
        // SAFETY: `bytes` is writable for `returned` bytes and `returned` points
        // to initialized storage. The duplicated fd remains owned by `self`.
        let result = unsafe {
            libc::getsockopt(
                self.fd.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_INFO,
                bytes.as_mut_ptr().cast(),
                &mut returned,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(parse_tcp_info_prefix(
            &bytes,
            usize::try_from(returned).unwrap_or(usize::MAX),
        ))
    }
}

pub(super) fn parse_tcp_info_prefix(bytes: &[u8], returned: usize) -> Option<TcpNativeSnapshot> {
    let available = returned.min(bytes.len());
    let u32_at = |offset| {
        u32::from_ne_bytes(
            bytes[offset..offset + 4]
                .try_into()
                .expect("validated TCP_INFO u32 field"),
        )
    };
    let u64_at = |offset| {
        u64::from_ne_bytes(
            bytes[offset..offset + 8]
                .try_into()
                .expect("validated TCP_INFO u64 field"),
        )
    };

    let rtt = (available >= 76).then(|| TcpNativeRtt {
        srtt_us: u32_at(68),
        rttvar_us: u32_at(72),
    });
    let flight = (available >= 84).then(|| TcpNativeFlight {
        snd_mss_bytes: u32_at(16),
        unacked_packets: u32_at(24),
        snd_ssthresh_packets: u32_at(76),
        snd_cwnd_packets: u32_at(80),
    });
    let loss = (available >= 160).then(|| TcpNativeLossCounters {
        retransmits: u32_at(100),
        data_segments_out: u32_at(156),
    });
    // Linux 4.9 added both tcpi_delivery_rate and the app-limited bit. Older
    // kernels used byte 7 as padding, so a shorter reply cannot prove either.
    let has_delivery_fields = available >= TCP_INFO_V4_9_PREFIX_BYTES;
    let snapshot = TcpNativeSnapshot {
        rtt,
        flight,
        pacing_rate_bytes_per_second: (available >= 112).then(|| u64_at(104)),
        bytes_acked: (available >= 128).then(|| u64_at(120)),
        notsent_bytes: (available >= 148).then(|| u32_at(144)),
        loss,
        delivery_rate_bytes_per_second: has_delivery_fields.then(|| u64_at(160)),
        app_limited: has_delivery_fields.then(|| bytes[7] & 1 != 0),
    };
    snapshot.has_evidence().then_some(snapshot)
}
