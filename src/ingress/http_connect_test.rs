
use super::*;

#[test]
fn parses_domain_connect_request() {
    let input = b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\nextra";
    let request = parse_connect_request(input).expect("connect");

    let expected_header_len = input
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|pos| pos + 4)
        .expect("header delimiter");
    assert_eq!(request.header_len, expected_header_len);
    assert_eq!(request.proxy_authorization, None);
    assert_eq!(
        request.target,
        TargetAddr::Domain {
            host: "example.com".to_string(),
            port: 443
        }
    );
}

#[test]
fn parses_proxy_authorization_header_case_insensitively() {
    let request = parse_connect_request(
        b"CONNECT example.com:443 HTTP/1.1\r\nproxy-authorization: Basic abc\r\n\r\n",
    )
    .expect("connect");

    assert_eq!(request.proxy_authorization.as_deref(), Some("Basic abc"));
}

#[test]
fn parses_ip_authorities() {
    let ipv4 = parse_connect_request(b"CONNECT 192.0.2.10:8443 HTTP/1.1\r\n\r\n").expect("ipv4");
    assert_eq!(
        ipv4.target,
        TargetAddr::Ip("192.0.2.10:8443".parse().expect("addr"))
    );

    let ipv6 = parse_connect_request(b"CONNECT [2001:db8::1]:443 HTTP/1.1\r\n\r\n").expect("ipv6");
    assert_eq!(
        ipv6.target,
        TargetAddr::Ip("[2001:db8::1]:443".parse().expect("addr"))
    );
}

#[test]
fn rejects_non_connect_and_bad_authority() {
    assert!(matches!(
        parse_connect_request(b"GET example.com:443 HTTP/1.1\r\n\r\n"),
        Err(HttpConnectError::UnsupportedMethod(_))
    ));
    assert_eq!(
        parse_connect_request(b"CONNECT example.com HTTP/1.1\r\n\r\n"),
        Err(HttpConnectError::InvalidAuthority)
    );
    assert_eq!(
        parse_connect_request(b"CONNECT example.com:0 HTTP/1.1\r\n\r\n"),
        Err(HttpConnectError::InvalidPort)
    );
}

#[test]
fn builds_responses() {
    assert_eq!(
        success_response(),
        b"HTTP/1.1 200 Connection Established\r\n\r\n"
    );
    assert_eq!(
        error_response(HttpStatus::BadGateway),
        b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n"
    );
    assert!(
        std::str::from_utf8(error_response(HttpStatus::ProxyAuthenticationRequired))
            .expect("response")
            .contains("407 Proxy Authentication Required")
    );
}
