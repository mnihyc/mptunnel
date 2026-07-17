use super::{
    PlatformTcpTelemetrySocket, TCP_CONNECTION_INFO_MIN_BYTES, XNU_UNBOUNDED_SSTHRESH_BYTES,
    snapshot_from_connection_info,
};
use std::mem::{offset_of, size_of, zeroed};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[test]
fn connection_info_normalizes_milliseconds_and_byte_windows() {
    // SAFETY: the zero value is valid for this C telemetry record.
    let mut info: libc::tcp_connection_info = unsafe { zeroed() };
    info.tcpi_srtt = 25;
    info.tcpi_rttvar = 4;
    info.tcpi_snd_cwnd = 512 * 1024;
    info.tcpi_snd_ssthresh = 256 * 1024;
    info.tcpi_snd_sbbytes = 64 * 1024;

    let snapshot = snapshot_from_connection_info(&info).expect("macOS TCP telemetry");
    let rtt = snapshot.rtt.expect("RTT fields");
    assert_eq!(rtt.srtt_us, 25_000);
    assert_eq!(rtt.rttvar_us, Some(4_000));
    let flight = snapshot.flight.expect("congestion window");
    assert_eq!(flight.bytes_in_flight, None);
    assert_eq!(flight.inflight_limit_bytes, 512 * 1024);
    assert_eq!(flight.inflight_hi_bytes, Some(256 * 1024));
    assert_eq!(snapshot.notsent_bytes, None);
}

#[test]
fn minimum_reply_size_covers_exactly_the_consumed_prefix() {
    assert_eq!(
        TCP_CONNECTION_INFO_MIN_BYTES,
        offset_of!(libc::tcp_connection_info, tcpi_rttvar) + size_of::<u32>()
    );
    assert!(TCP_CONNECTION_INFO_MIN_BYTES <= size_of::<libc::tcp_connection_info>());
}

#[test]
fn unbounded_ssthresh_uses_current_cwnd_as_the_finite_shape() {
    // SAFETY: the zero value is valid for this C telemetry record.
    let mut info: libc::tcp_connection_info = unsafe { zeroed() };
    info.tcpi_snd_cwnd = 512 * 1024;
    info.tcpi_snd_ssthresh = XNU_UNBOUNDED_SSTHRESH_BYTES;

    let flight = snapshot_from_connection_info(&info)
        .expect("macOS TCP telemetry")
        .flight
        .expect("congestion window");
    assert_eq!(flight.inflight_hi_bytes, Some(512 * 1024));
}

#[tokio::test]
async fn connected_loopback_socket_exposes_native_window_shape() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let mut client = TcpStream::connect(listener.local_addr().expect("listener address"))
            .await
            .expect("connect loopback client");
        let (mut server, _) = listener.accept().await.expect("accept loopback client");
        let telemetry =
            PlatformTcpTelemetrySocket::capture(&client).expect("duplicate macOS TCP socket");

        client.write_all(b"mptunnel").await.expect("write payload");
        let mut payload = [0u8; 8];
        server.read_exact(&mut payload).await.expect("read payload");
        let snapshot = telemetry
            .snapshot()
            .expect("query TCP_CONNECTION_INFO")
            .expect("macOS TCP window evidence");

        assert!(snapshot.rtt.is_some());
        assert!(
            snapshot
                .flight
                .is_some_and(|flight| flight.inflight_limit_bytes > 0)
        );
    })
    .await
    .expect("macOS TCP telemetry proof exceeded three seconds");
}
