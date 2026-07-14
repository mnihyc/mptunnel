use super::*;

#[test]
fn parses_no_auth_method_request() {
    let (request, consumed) = parse_auth_request(&[0x05, 0x02, 0x00, 0x02]).expect("auth");

    assert_eq!(consumed, 4);
    assert!(request.supports_no_auth());
    assert!(request.supports_username_password());
    assert_eq!(no_auth_response(), [0x05, 0x00]);
    assert_eq!(username_password_method_response(), [0x05, 0x02]);
    assert_eq!(username_password_auth_response(true), [0x01, 0x00]);
    assert_eq!(username_password_auth_response(false), [0x01, 0x01]);
    assert_eq!(no_acceptable_methods_response(), [0x05, 0xff]);
}

#[test]
fn parses_username_password_auth_request() {
    let input = [
        0x01, 0x04, b'u', b's', b'e', b'r', 0x06, b's', b'e', b'c', b'r', b'e', b't',
    ];
    let (request, consumed) = parse_username_password_auth_request(&input).expect("auth request");

    assert_eq!(consumed, input.len());
    assert_eq!(request.username, "user");
    assert_eq!(request.password, "secret");
}

#[test]
fn parses_domain_connect_request() {
    let mut input = vec![0x05, 0x01, 0x00, 0x03, 11];
    input.extend_from_slice(b"example.com");
    input.extend_from_slice(&443u16.to_be_bytes());

    let (request, consumed) = parse_connect_request(&input).expect("connect");

    assert_eq!(consumed, input.len());
    assert_eq!(
        request.target,
        TargetAddr::Domain {
            host: "example.com".to_string(),
            port: 443
        }
    );
}

#[test]
fn parses_ipv4_and_ipv6_connect_requests() {
    let ipv4 = [0x05, 0x01, 0x00, 0x01, 192, 0, 2, 10, 0x20, 0xfb];
    let (request, _) = parse_connect_request(&ipv4).expect("ipv4");
    assert_eq!(
        request.target,
        TargetAddr::Ip("192.0.2.10:8443".parse().expect("addr"))
    );

    let mut ipv6 = vec![0x05, 0x01, 0x00, 0x04];
    ipv6.extend_from_slice(&Ipv6Addr::LOCALHOST.octets());
    ipv6.extend_from_slice(&53u16.to_be_bytes());
    let (request, _) = parse_connect_request(&ipv6).expect("ipv6");
    assert_eq!(
        request.target,
        TargetAddr::Ip("[::1]:53".parse().expect("addr"))
    );
}

#[test]
fn rejects_udp_associate_as_unsupported_for_tcp_connect_parser() {
    let input = [0x05, 0x03, 0x00, 0x01, 127, 0, 0, 1, 0, 53];

    assert_eq!(
        parse_connect_request(&input),
        Err(Socks5Error::UnsupportedCommand(0x03))
    );
}

#[test]
fn parses_udp_associate_request_with_zero_client_endpoint() {
    let input = [0x05, 0x03, 0x00, 0x01, 0, 0, 0, 0, 0, 0];

    let (request, consumed) = parse_udp_associate_request(&input).expect("udp associate");

    assert_eq!(consumed, input.len());
    assert_eq!(
        request.client_endpoint,
        TargetAddr::Ip("0.0.0.0:0".parse().expect("addr"))
    );
}

#[test]
fn parses_and_builds_udp_datagram() {
    let target = TargetAddr::Domain {
        host: "example.com".to_string(),
        port: 53,
    };
    let packet = udp_datagram(&target, b"payload").expect("packet");
    let (parsed, consumed) = parse_udp_datagram(&packet).expect("datagram");

    assert_eq!(consumed, packet.len());
    assert_eq!(parsed.target, target);
    assert_eq!(parsed.payload, Bytes::from_static(b"payload"));
}

#[test]
fn rejects_fragmented_udp_datagram() {
    let input = [0x00, 0x00, 0x01, 0x01, 127, 0, 0, 1, 0, 53, b'x'];

    assert_eq!(
        parse_udp_datagram(&input),
        Err(Socks5Error::UnsupportedFragment(0x01))
    );
}

#[test]
fn builds_success_reply() {
    let reply = connect_reply(Socks5Reply::Succeeded, "127.0.0.1:0".parse().expect("bind"));

    assert_eq!(reply, vec![0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0]);
}
