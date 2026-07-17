use super::TcpTelemetrySocket;
use super::linux::{TCP_INFO_V4_9_PREFIX_BYTES, parse_tcp_info_prefix};
use std::mem::{offset_of, size_of};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
fn parser_exposes_only_counter_groups_present_in_the_returned_prefix() {
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
    bytes[156..160].copy_from_slice(&42u32.to_ne_bytes());
    bytes[160..168].copy_from_slice(&8_000_000u64.to_ne_bytes());

    for returned in [7, 8, 75] {
        assert_eq!(parse_tcp_info_prefix(&bytes, returned), None);
    }
    let rtt_only = parse_tcp_info_prefix(&bytes, 76).expect("RTT prefix");
    assert_eq!(rtt_only.rtt.expect("RTT group").srtt_us, 20_000);
    assert_eq!(rtt_only.flight, None);

    let flight = parse_tcp_info_prefix(&bytes, 84).expect("flight prefix");
    assert_eq!(
        flight.flight.expect("flight group").inflight_limit_bytes,
        10 * 1_460
    );
    assert_eq!(flight.pacing_rate_bytes_per_second, None);

    let pacing = parse_tcp_info_prefix(&bytes, 112).expect("pacing prefix");
    assert_eq!(pacing.pacing_rate_bytes_per_second, Some(10_000_000));
    assert_eq!(pacing.bytes_acked, None);

    let acked = parse_tcp_info_prefix(&bytes, 128).expect("ACK prefix");
    assert_eq!(acked.bytes_acked, Some(123_456));
    assert_eq!(acked.notsent_bytes, None);

    let queue = parse_tcp_info_prefix(&bytes, 148).expect("queue prefix");
    assert_eq!(queue.notsent_bytes, Some(99));

    let loss = parse_tcp_info_prefix(&bytes, 160).expect("loss prefix");
    assert_eq!(loss.loss.expect("loss counters").retransmits, 7);
    assert_eq!(loss.delivery_rate_bytes_per_second, None);
    assert_eq!(loss.app_limited, None);
    let pre_delivery = parse_tcp_info_prefix(&bytes, 167).expect("pre-delivery prefix");
    assert_eq!(pre_delivery.delivery_rate_bytes_per_second, None);
    assert_eq!(pre_delivery.app_limited, None);

    let parsed = parse_tcp_info_prefix(&bytes, 168).expect("complete v4.9 prefix");
    assert_eq!(parsed.app_limited, Some(true));
    assert_eq!(
        parsed.flight.expect("flight group").bytes_in_flight,
        Some(3 * 1_460)
    );
    assert_eq!(parsed.rtt.expect("RTT group").srtt_us, 20_000);
    assert_eq!(parsed.pacing_rate_bytes_per_second, Some(10_000_000));
    assert_eq!(parsed.bytes_acked, Some(123_456));
    assert_eq!(parsed.delivery_rate_bytes_per_second, Some(8_000_000));
}

#[test]
fn parser_normalizes_unbounded_ssthresh_to_current_cwnd() {
    let mut bytes = [0u8; TCP_INFO_V4_9_PREFIX_BYTES];
    bytes[16..20].copy_from_slice(&1_460u32.to_ne_bytes());
    bytes[76..80].copy_from_slice(&0x7fff_ffffu32.to_ne_bytes());
    bytes[80..84].copy_from_slice(&10u32.to_ne_bytes());

    let flight = parse_tcp_info_prefix(&bytes, 84)
        .expect("flight prefix")
        .flight
        .expect("flight group");
    assert_eq!(flight.inflight_limit_bytes, 10 * 1_460);
    assert_eq!(flight.inflight_hi_bytes, Some(10 * 1_460));
}

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

        let telemetry = TcpTelemetrySocket::capture(&client)
            .expect("duplicate client socket")
            .expect("Linux native TCP telemetry capability");
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
            if snapshot
                .bytes_acked
                .zip(baseline.bytes_acked)
                .is_some_and(|(snapshot_bytes, baseline_bytes)| snapshot_bytes > baseline_bytes)
            {
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
        assert!(
            after_original_drop
                .bytes_acked
                .zip(advanced.bytes_acked)
                .is_some_and(|(after_bytes, advanced_bytes)| after_bytes >= advanced_bytes)
        );
    })
    .await
    .expect("loopback TCP_INFO proof exceeded three seconds");
}
