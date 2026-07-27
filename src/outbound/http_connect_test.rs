use super::*;

#[test]
fn builds_http_connect_request() {
    let request = connect_request(
        &TargetAddr::Domain {
            host: "example.com".to_string(),
            port: 443,
        },
        None,
        None,
    )
    .expect("request");

    assert_eq!(
            request,
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\nProxy-Connection: Keep-Alive\r\n\r\n"
        );
}

#[test]
fn builds_bounded_basic_proxy_authorization_without_debug_secret_leakage() {
    let credentials = ProxyCredentials::new("alice".to_string(), "secret-password".to_string())
        .expect("credentials");
    let request = connect_request(
        &TargetAddr::Domain {
            host: "example.com".to_string(),
            port: 443,
        },
        None,
        Some(&credentials),
    )
    .expect("request");
    let request = String::from_utf8(request).expect("ASCII request");

    assert!(request.contains("Proxy-Authorization: Basic YWxpY2U6c2VjcmV0LXBhc3N3b3Jk\r\n"));
    assert!(!format!("{credentials:?}").contains("secret-password"));
    assert!(
        !OutboundError::ProxyPasswordEmpty
            .to_string()
            .contains("secret")
    );
}

#[test]
fn rejects_request_line_and_header_injection() {
    let err = connect_request(
        &TargetAddr::Domain {
            host: "example.com\r\nInjected: true".to_string(),
            port: 443,
        },
        None,
        None,
    )
    .expect_err("injected authority");

    assert_eq!(err, OutboundError::InvalidDomain);
}

#[test]
fn rejects_oversized_connect_request_headers() {
    let host = "a".repeat(17 * 1024);
    let err = connect_request(
        &TargetAddr::Domain {
            host: "example.com".to_string(),
            port: 443,
        },
        Some(&host),
        None,
    )
    .expect_err("oversized request");

    assert_eq!(err, OutboundError::ProxyRequestTooLarge);
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
