use super::*;

async fn parse(raw: &[u8]) -> Result<ManagementRequest, ManagementHttpError> {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let mut client = TcpStream::connect(listener.local_addr().expect("address"))
        .await
        .expect("client");
    let (mut server, _) = listener.accept().await.expect("accept");
    client.write_all(raw).await.expect("request");
    client.shutdown().await.expect("shutdown");
    read_request(&mut server).await
}

#[tokio::test]
async fn parser_accepts_one_complete_origin_form_request() {
    let request = parse(
        b"POST /api/diagnostics/peer?fresh=true HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}",
    )
    .await
    .expect("request");
    assert_eq!(request.method, "POST");
    assert_eq!(request.path_without_query(), "/api/diagnostics/peer");
    assert_eq!(request.body, b"{}");
}

#[tokio::test]
async fn parser_rejects_ambiguous_security_headers() {
    for raw in [
        b"GET /api/status HTTP/1.1\r\nAuthorization: Bearer first\r\nAuthorization: Bearer second\r\n\r\n".as_slice(),
        b"POST /api/control/path HTTP/1.1\r\nContent-Type: application/json\r\nContent-Type: text/plain\r\nContent-Length: 0\r\n\r\n".as_slice(),
        b"POST /api/control/path HTTP/1.1\r\nTransfer-Encoding: chunked\r\nContent-Length: 0\r\n\r\n".as_slice(),
    ] {
        assert_eq!(parse(raw).await.expect_err("ambiguous request").status, 400);
    }
}

#[tokio::test]
async fn parser_rejects_absolute_targets_and_pipelining() {
    let absolute = parse(b"GET http://localhost/api/status HTTP/1.1\r\n\r\n")
        .await
        .expect_err("absolute target");
    assert_eq!(absolute.status, 400);

    let pipelined = parse(b"GET /api/status HTTP/1.1\r\n\r\nGET /api/status HTTP/1.1\r\n\r\n")
        .await
        .expect_err("pipelined request");
    assert_eq!(pipelined.status, 400);
}

#[test]
fn dashboard_auth_form_cannot_navigate_without_javascript() {
    assert!(DASHBOARD_HTML.contains(r#"<form id="auth-form" class="auth-form" method="dialog">"#));
    assert!(DASHBOARD_HTML.contains(r#"<link rel="icon" href="data:image/svg+xml,"#));
    assert!(CONTENT_SECURITY_POLICY.contains("form-action 'none'"));
}
