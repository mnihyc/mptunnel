//! Exact-socket Linux TCP telemetry.
//!
//! Runtime policy owns how these carrier counters affect product placement.
//! This module only preserves the kernel ABI boundary and socket identity.

use std::io;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};

const TCP_INFO_V4_9_PREFIX_BYTES: usize = 168;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TcpInfoSnapshot {
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

#[derive(Debug)]
pub(crate) struct TcpInfoSocket {
    fd: OwnedFd,
}

impl TcpInfoSocket {
    /// Duplicates the exact carrier fd before higher-level framing hides it.
    pub(crate) fn capture(socket: &impl AsFd) -> io::Result<Self> {
        Ok(Self {
            fd: socket.as_fd().try_clone_to_owned()?,
        })
    }

    /// Missing or truncated kernel telemetry is not a carrier failure.
    pub(crate) fn snapshot(&self) -> io::Result<Option<TcpInfoSnapshot>> {
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

fn parse_tcp_info_prefix(bytes: &[u8], returned: usize) -> Option<TcpInfoSnapshot> {
    // Linux 4.9 added both tcpi_delivery_rate and the app-limited bit. Older
    // kernels used byte 7 as padding, so a shorter reply cannot prove either.
    if returned < TCP_INFO_V4_9_PREFIX_BYTES || bytes.len() < TCP_INFO_V4_9_PREFIX_BYTES {
        return None;
    }
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
    Some(TcpInfoSnapshot {
        app_limited: bytes[7] & 1 != 0,
        snd_mss_bytes: u32_at(16),
        unacked_packets: u32_at(24),
        srtt_us: u32_at(68),
        rttvar_us: u32_at(72),
        snd_ssthresh_packets: u32_at(76),
        snd_cwnd_packets: u32_at(80),
        retransmits: u32_at(100),
        pacing_rate_bytes_per_second: u64_at(104),
        bytes_acked: u64_at(120),
        notsent_bytes: u32_at(144),
        min_rtt_us: u32_at(148),
        data_segments_out: u32_at(156),
        delivery_rate_bytes_per_second: u64_at(160),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::{offset_of, size_of};

    #[cfg(target_os = "linux")]
    use std::time::Duration;
    #[cfg(target_os = "linux")]
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[cfg(target_os = "linux")]
    use tokio::net::{TcpListener, TcpStream};

    #[repr(C)]
    struct TcpInfoV49Prefix {
        flags: [u8; 8],
        rto_through_total_retrans: [u32; 24],
        pacing_rate: u64,
        max_pacing_rate: u64,
        bytes_acked: u64,
        bytes_received: u64,
        segments_out: u32,
        segments_in: u32,
        notsent_bytes: u32,
        min_rtt: u32,
        data_segments_in: u32,
        data_segments_out: u32,
        delivery_rate: u64,
    }

    #[test]
    fn tcp_info_v49_layout_matches_stable_uapi_prefix() {
        assert_eq!(size_of::<TcpInfoV49Prefix>(), TCP_INFO_V4_9_PREFIX_BYTES);
        assert_eq!(offset_of!(TcpInfoV49Prefix, pacing_rate), 104);
        assert_eq!(offset_of!(TcpInfoV49Prefix, bytes_acked), 120);
        assert_eq!(offset_of!(TcpInfoV49Prefix, notsent_bytes), 144);
        assert_eq!(offset_of!(TcpInfoV49Prefix, min_rtt), 148);
        assert_eq!(offset_of!(TcpInfoV49Prefix, data_segments_out), 156);
        assert_eq!(offset_of!(TcpInfoV49Prefix, delivery_rate), 160);
    }

    #[test]
    fn parser_requires_complete_delivery_rate_generation() {
        let mut bytes = [0u8; TCP_INFO_V4_9_PREFIX_BYTES];
        bytes[7] = 1;
        bytes[16..20].copy_from_slice(&1460u32.to_ne_bytes());
        bytes[24..28].copy_from_slice(&3u32.to_ne_bytes());
        bytes[68..72].copy_from_slice(&20_000u32.to_ne_bytes());
        bytes[72..76].copy_from_slice(&2_000u32.to_ne_bytes());
        bytes[80..84].copy_from_slice(&10u32.to_ne_bytes());
        bytes[100..104].copy_from_slice(&7u32.to_ne_bytes());
        bytes[104..112].copy_from_slice(&10_000_000u64.to_ne_bytes());
        bytes[120..128].copy_from_slice(&123_456u64.to_ne_bytes());
        bytes[144..148].copy_from_slice(&99u32.to_ne_bytes());
        bytes[148..152].copy_from_slice(&18_000u32.to_ne_bytes());
        bytes[156..160].copy_from_slice(&42u32.to_ne_bytes());
        bytes[160..168].copy_from_slice(&8_000_000u64.to_ne_bytes());

        for returned in [7, 8, 104, 120, 128, 144, 148, 160, 167] {
            assert_eq!(parse_tcp_info_prefix(&bytes, returned), None);
        }
        let parsed = parse_tcp_info_prefix(&bytes, 168).expect("complete v4.9 prefix");
        assert!(parsed.app_limited);
        assert_eq!(parsed.snd_mss_bytes, 1460);
        assert_eq!(parsed.srtt_us, 20_000);
        assert_eq!(parsed.pacing_rate_bytes_per_second, 10_000_000);
        assert_eq!(parsed.bytes_acked, 123_456);
        assert_eq!(parsed.delivery_rate_bytes_per_second, 8_000_000);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn duplicated_socket_survives_stream_split_and_observes_ack_progress() {
        tokio::time::timeout(Duration::from_secs(3), async {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind loopback listener");
            let client = TcpStream::connect(listener.local_addr().expect("listener address"))
                .await
                .expect("connect loopback client");
            let (mut server, _) = listener.accept().await.expect("accept loopback client");

            let telemetry = TcpInfoSocket::capture(&client).expect("duplicate client socket");
            let baseline = telemetry
                .snapshot()
                .expect("read initial TCP_INFO")
                .expect("Linux TCP_INFO v4.9 prefix");
            let (client_reader, mut client_writer) = client.into_split();

            let payload = vec![0xa5; 64 * 1024];
            let mut received = vec![0; payload.len()];
            let (write_result, read_result) = tokio::join!(
                client_writer.write_all(&payload),
                server.read_exact(&mut received)
            );
            write_result.expect("write loopback payload");
            read_result.expect("read loopback payload");
            assert_eq!(received, payload);

            let advanced = loop {
                let snapshot = telemetry
                    .snapshot()
                    .expect("read TCP_INFO after transfer")
                    .expect("Linux TCP_INFO v4.9 prefix");
                if snapshot.bytes_acked > baseline.bytes_acked {
                    break snapshot;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            };

            drop(client_reader);
            drop(client_writer);
            let after_original_drop = telemetry
                .snapshot()
                .expect("duplicated socket remains queryable")
                .expect("Linux TCP_INFO v4.9 prefix");
            assert!(after_original_drop.bytes_acked >= advanced.bytes_acked);
        })
        .await
        .expect("loopback TCP_INFO proof exceeded three seconds");
    }
}
