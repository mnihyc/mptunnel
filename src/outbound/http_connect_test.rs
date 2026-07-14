
use super::*;
use tokio::io::{AsyncWriteExt, duplex};

#[test]
fn builds_http_connect_request() {
    let request = connect_request(
        &TargetAddr::Domain {
            host: "example.com".to_string(),
            port: 443,
        },
        None,
    )
    .expect("request");

    assert_eq!(
            request,
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\nProxy-Connection: Keep-Alive\r\n\r\n"
        );
}

#[test]
fn builds_http_connect_udp_request_with_default_template() {
    let proxy: Endpoint = "proxy.example:8080".parse().expect("proxy");
    let request = connect_udp_request(
        &proxy,
        &TargetAddr::Ip("[2001:db8::42]:443".parse().expect("target")),
    )
    .expect("request");

    assert_eq!(
            request,
            b"GET http://proxy.example:8080/.well-known/masque/udp/2001%3Adb8%3A%3A42/443/ HTTP/1.1\r\nHost: proxy.example:8080\r\nConnection: Upgrade\r\nUpgrade: connect-udp\r\nCapsule-Protocol: ?1\r\n\r\n"
        );
}

#[test]
fn parses_http_connect_response() {
    let response = parse_connect_response(b"HTTP/1.1 200 Connection Established\r\n\r\npayload")
        .expect("response");

    assert_eq!(
        response,
        HttpConnectResponse {
            status: 200,
            header_len: 39
        }
    );
}

#[test]
fn parses_http_connect_udp_switching_protocols_response() {
    let response = parse_connect_udp_response(
            b"HTTP/1.1 101 Switching Protocols\r\nConnection: keep-alive, Upgrade\r\nUpgrade: connect-udp\r\nCapsule-Protocol: ?1\r\n\r\n",
        )
        .expect("response");

    assert_eq!(
        response,
        HttpConnectUdpResponse {
            status: 101,
            header_len: 113
        }
    );

    assert!(parse_connect_udp_response(
            b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nCapsule-Protocol: ?1\r\n\r\n",
        )
        .is_err());
}

#[tokio::test]
async fn datagram_capsules_round_trip_udp_context_payload() {
    let (mut client, mut server) = duplex(128);
    let capsule = datagram_capsule(b"ping").expect("capsule");
    server.write_all(&capsule).await.expect("write");

    let payload = read_datagram_capsule(&mut client).await.expect("read");

    assert_eq!(payload, b"ping");
}

#[tokio::test]
async fn datagram_capsule_reads_udp_payload_directly_into_caller_buffer() {
    let (mut client, mut server) = duplex(128);
    let capsule = datagram_capsule(b"pong").expect("capsule");
    server.write_all(&capsule).await.expect("write");

    let mut payload = [0u8; 16];
    let len = read_datagram_capsule_into(&mut client, &mut payload)
        .await
        .expect("read");

    assert_eq!(&payload[..len], b"pong");
}

#[tokio::test]
async fn datagram_capsule_into_rejects_truncated_context_without_reading_past_capsule() {
    let (mut client, mut server) = duplex(128);
    server
        .write_all(&[0x00, 0x01, 0x40])
        .await
        .expect("write truncated datagram capsule");

    let mut payload = [0u8; 16];
    let err = read_datagram_capsule_into(&mut client, &mut payload)
        .await
        .expect_err("truncated context should fail");

    assert!(matches!(err, HttpConnectClientError::InvalidVarint));
}
