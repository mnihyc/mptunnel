use super::*;

#[test]
fn builds_connect_request_for_domain() {
    let request = connect_request(&TargetAddr::Domain {
        host: "example.com".to_string(),
        port: 443,
    })
    .expect("request");

    let mut expected = vec![0x05, 0x01, 0x00, 0x03, 11];
    expected.extend_from_slice(b"example.com");
    expected.extend_from_slice(&443u16.to_be_bytes());
    assert_eq!(request, expected);
}

#[test]
fn builds_udp_associate_request() {
    let request = udp_associate_request("0.0.0.0:0".parse().expect("addr")).expect("request");

    assert_eq!(request, vec![0x05, 0x03, 0x00, 0x01, 0, 0, 0, 0, 0, 0]);
}

#[test]
fn parses_method_selection_and_connect_reply() {
    assert_eq!(
        parse_method_selection(&[0x05, 0x00]).expect("method"),
        Socks5MethodSelection { method: 0x00 }
    );

    let reply =
        parse_connect_reply(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0x1f, 0x90]).expect("reply");
    assert_eq!(reply.status, 0);
    assert_eq!(
        reply.bind,
        TargetAddr::Ip("127.0.0.1:8080".parse().expect("addr"))
    );
    assert_eq!(reply.consumed, 10);
}

#[test]
fn builds_and_parses_username_password_authentication() {
    let credentials = ProxyCredentials::new("alice".to_string(), "correct horse".to_string())
        .expect("credentials");

    assert_eq!(username_password_greeting(), [0x05, 0x01, 0x02]);
    assert_eq!(
        username_password_request(&credentials),
        b"\x01\x05alice\x0dcorrect horse"
    );
    assert!(parse_username_password_reply(&[0x01, 0x00]).is_ok());
    assert!(matches!(
        parse_username_password_reply(&[0x01, 0x01]),
        Err(Socks5ClientError::AuthenticationRejected)
    ));
}
