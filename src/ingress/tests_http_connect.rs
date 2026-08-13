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
    assert!(!format!("{request:?}").contains("Basic abc"));
}

#[test]
fn rewrites_absolute_http_request_and_strips_proxy_and_hop_headers() {
    let request = parse_proxy_request(
        b"POST http://Example.COM:8080/upload?q=1 HTTP/1.1\r\n\
Host: attacker.invalid\r\n\
Proxy-Authorization: Basic dXNlcjpzZWNyZXQ=\r\n\
Proxy-Connection: keep-alive\r\n\
Connection: keep-alive, X-Remove\r\n\
Keep-Alive: timeout=5\r\n\
X-Remove: private-hop\r\n\
X-End-To-End: retained\r\n\
Content-Length: 4\r\n\r\n",
    )
    .expect("forward request");
    let HttpProxyRequest::Forward(request) = request else {
        panic!("expected forward request");
    };

    assert_eq!(
        request.target,
        TargetAddr::Domain {
            host: "example.com".to_string(),
            port: 8080,
        }
    );
    assert_eq!(request.body_len, 4);
    assert_eq!(
        request.proxy_authorization.as_deref(),
        Some("Basic dXNlcjpzZWNyZXQ=")
    );
    assert_eq!(
        request.rewritten_header,
        b"POST /upload?q=1 HTTP/1.1\r\n\
Host: example.com:8080\r\n\
X-End-To-End: retained\r\n\
Content-Length: 4\r\n\
Connection: close\r\n\r\n"
    );
    let debug = format!("{request:?}");
    assert!(!debug.contains("dXNlcjpzZWNyZXQ="));
    assert!(!debug.contains("X-End-To-End"));
}

#[test]
fn parses_default_port_and_query_only_absolute_http_targets() {
    let request = parse_proxy_request(b"HEAD http://127.0.0.1?health HTTP/1.0\r\n\r\n")
        .expect("forward request");
    let HttpProxyRequest::Forward(request) = request else {
        panic!("expected forward request");
    };
    assert_eq!(
        request.target,
        TargetAddr::Ip("127.0.0.1:80".parse().expect("target"))
    );
    assert!(
        request
            .rewritten_header
            .starts_with(b"HEAD /?health HTTP/1.0\r\nHost: 127.0.0.1:80\r\n")
    );
    assert_eq!(request.body_len, 0);
}

#[test]
fn rejects_ambiguous_or_unsafe_http_forward_framing() {
    for request in [
        &b"GET https://example.com/ HTTP/1.1\r\n\r\n"[..],
        &b"GET /origin-form HTTP/1.1\r\nHost: example.com\r\n\r\n"[..],
        &b"GET http://user@example.com/ HTTP/1.1\r\n\r\n"[..],
        &b"GET http://example.com/#fragment HTTP/1.1\r\n\r\n"[..],
        &b"POST http://example.com/ HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n"[..],
        &b"POST http://example.com/ HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\n"[..],
        &b"POST http://example.com/ HTTP/1.1\r\nConnection: Content-Length\r\nContent-Length: 1\r\n\r\n"[..],
    ] {
        assert!(parse_proxy_request(request).is_err(), "{request:?}");
    }
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
    assert_eq!(
        error_response(HttpStatus::ServiceUnavailable),
        b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n"
    );
    assert!(
        std::str::from_utf8(error_response(HttpStatus::ProxyAuthenticationRequired))
            .expect("response")
            .contains("407 Proxy Authentication Required")
    );
}
