use super::{PlatformTcpTelemetrySocket, snapshot_from_tcp_info};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use windows_sys::Win32::Networking::WinSock::TCP_INFO_v0;

#[test]
fn tcp_info_v0_preserves_only_documented_windows_evidence() {
    let info = TCP_INFO_v0 {
        RttUs: 25_000,
        MinRttUs: 10_000,
        BytesInFlight: 128 * 1024,
        Cwnd: 512 * 1024,
        BytesOut: 2 * 1024 * 1024,
        ..TCP_INFO_v0::default()
    };

    let snapshot = snapshot_from_tcp_info(&info).expect("Windows TCP telemetry");
    let rtt = snapshot.rtt.expect("RTT fields");
    assert_eq!(rtt.srtt_us, 25_000);
    assert_eq!(rtt.rttvar_us, None);
    let flight = snapshot.flight.expect("congestion window");
    assert_eq!(flight.bytes_in_flight, Some(128 * 1024));
    assert_eq!(flight.inflight_limit_bytes, 512 * 1024);
    assert_eq!(snapshot.bytes_acked, None);
}

#[tokio::test]
async fn connected_loopback_socket_exposes_native_flight_and_window() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let mut client = TcpStream::connect(listener.local_addr().expect("listener address"))
            .await
            .expect("connect loopback client");
        let (mut server, _) = listener.accept().await.expect("accept loopback client");
        let telemetry =
            PlatformTcpTelemetrySocket::capture(&client).expect("duplicate Windows TCP socket");

        client.write_all(b"mptunnel").await.expect("write payload");
        let mut payload = [0u8; 8];
        server.read_exact(&mut payload).await.expect("read payload");
        let snapshot = telemetry
            .snapshot()
            .expect("query SIO_TCP_INFO")
            .expect("Windows TCP window evidence");

        assert!(snapshot.rtt.is_some());
        let flight = snapshot.flight.expect("Windows TCP flight evidence");
        assert!(flight.bytes_in_flight.is_some());
        assert!(flight.inflight_limit_bytes > 0);
    })
    .await
    .expect("Windows TCP telemetry proof exceeded three seconds");
}
