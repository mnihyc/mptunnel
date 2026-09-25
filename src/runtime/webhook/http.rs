use super::{
    AttemptOutcome, Delivery, MAX_ATTEMPT_HEADER_BYTES, MAX_RENDERED_BODY_BYTES,
    MAX_RENDERED_HEADERS_BYTES, MAX_RENDERED_URL_BYTES,
};
use crate::runtime::outbound_registry::RuntimeOutboundRegistry;
use crate::runtime::webhook::egress::WebhookIo;
use crate::webhook::{RenderedWebhookRequest, WebhookScheme, WebhookTarget};
use http::{HeaderName, header};
use rustls::pki_types::{IpAddr as RustlsIpAddr, ServerName};
use rustls::{ClientConfig, RootCertStore};
use std::io;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_rustls::TlsConnector;

const RESPONSE_HEADER_COUNT_LIMIT: usize = 64;
const READ_CHUNK_BYTES: usize = 2048;

pub(super) async fn deliver(
    registry: &RuntimeOutboundRegistry,
    target: &WebhookTarget,
    delivery: &Delivery,
    envelope: &serde_json::Value,
    tls: Option<Arc<ClientConfig>>,
    deadline: tokio::time::Instant,
) -> AttemptOutcome {
    let rendered = match target.render(envelope) {
        Ok(rendered) => rendered,
        Err(_) => return permanent("template", None),
    };
    if rendered.uri.to_string().len() > MAX_RENDERED_URL_BYTES {
        return permanent("template", None);
    }
    if rendered
        .body
        .as_ref()
        .is_some_and(|body| body.len() > MAX_RENDERED_BODY_BYTES)
    {
        return permanent("template", None);
    }
    let request = match render_request(target, &rendered, delivery) {
        Ok(request) => request,
        Err(_) => return permanent("request", None),
    };

    let stream = match registry.open_webhook_tcp(target, deadline).await {
        Ok(stream) => stream,
        Err(error) => {
            return AttemptOutcome {
                status: None,
                stage: error.stage,
                success: false,
                retryable: error.retryable,
                retry_after: None,
            };
        }
    };
    let mut stream: Box<dyn WebhookIo> = Box::new(stream);
    if target.url.scheme == WebhookScheme::Https {
        let Some(tls) = tls else {
            return permanent("tls_config", None);
        };
        let server_name = match target.url.host.parse::<IpAddr>() {
            Ok(address) => ServerName::IpAddress(RustlsIpAddr::from(address)),
            Err(_) => match ServerName::try_from(target.url.host.clone()) {
                Ok(server_name) => server_name,
                Err(_) => return permanent("tls_name", None),
            },
        };
        stream = match TlsConnector::from(tls).connect(server_name, stream).await {
            Ok(stream) => Box::new(stream),
            Err(error) => {
                let retryable = error.kind() != io::ErrorKind::InvalidData;
                return AttemptOutcome {
                    status: None,
                    stage: "tls",
                    success: false,
                    retryable,
                    retry_after: None,
                };
            }
        };
    }

    if let Err(error) = write_request(&mut stream, &request, deadline).await {
        return match error {
            WriteRequestError::Timeout => retryable("write_timeout", None),
            WriteRequestError::Transport(error) => AttemptOutcome {
                status: None,
                stage: "write",
                success: false,
                retryable: error.kind() != io::ErrorKind::InvalidData,
                retry_after: None,
            },
        };
    }

    match read_final_headers(&mut stream, deadline).await {
        Ok((status, retry_after)) => {
            let success = (200..300).contains(&status);
            AttemptOutcome {
                status: Some(status),
                stage: if success { "response" } else { "http_status" },
                success,
                retryable: !success && retryable_status(status),
                retry_after,
            }
        }
        Err(ResponseReadError::Timeout) => retryable("response_timeout", None),
        Err(ResponseReadError::Transport(error)) => AttemptOutcome {
            status: None,
            stage: "response_read",
            success: false,
            retryable: error.kind() != io::ErrorKind::InvalidData,
            retry_after: None,
        },
        Err(ResponseReadError::Invalid) => permanent("response_headers", None),
    }
}

fn render_request(
    target: &WebhookTarget,
    rendered: &RenderedWebhookRequest,
    delivery: &Delivery,
) -> Result<Vec<u8>, ()> {
    if target.method == ::http::Method::CONNECT {
        return Err(());
    }
    let path = rendered
        .uri
        .path_and_query()
        .map(|path| path.as_str())
        .unwrap_or("/");
    let mut request = Vec::with_capacity(4096);
    append_header_bytes(&mut request, target.method.as_str().as_bytes())?;
    request.push(b' ');
    append_header_bytes(&mut request, path.as_bytes())?;
    request.extend_from_slice(b" HTTP/1.1\r\n");
    let authority = host_authority(&target.url.host, target.url.port, target.url.scheme);
    push_header(&mut request, b"Host", authority.as_bytes())?;
    push_header(
        &mut request,
        b"MPTUNNEL-Event-ID",
        delivery.event_id.as_bytes(),
    )?;
    push_header(
        &mut request,
        b"MPTUNNEL-Delivery-ID",
        delivery.delivery_id.as_bytes(),
    )?;
    push_header(&mut request, b"Connection", b"close")?;

    let mut has_content_type = false;
    let mut header_bytes = request.len();
    for (name, value) in &rendered.headers {
        if reserved_header(name) {
            return Err(());
        }
        has_content_type |= name == header::CONTENT_TYPE;
        header_bytes = header_bytes
            .saturating_add(name.as_str().len())
            .saturating_add(value.as_bytes().len())
            .saturating_add(4);
        if header_bytes > MAX_RENDERED_HEADERS_BYTES {
            return Err(());
        }
        push_header(&mut request, name.as_str().as_bytes(), value.as_bytes())?;
    }
    if rendered.body.is_some() {
        if !has_content_type {
            let content_type = rendered.content_type.ok_or(())?;
            push_header(&mut request, b"Content-Type", content_type.as_bytes())?;
        }
        let length = rendered.body.as_ref().map_or(0, Vec::len).to_string();
        push_header(&mut request, b"Content-Length", length.as_bytes())?;
    }
    request.extend_from_slice(b"\r\n");
    if request.len() > MAX_ATTEMPT_HEADER_BYTES {
        return Err(());
    }
    if let Some(body) = &rendered.body {
        request.extend_from_slice(body);
    }
    Ok(request)
}

fn reserved_header(name: &HeaderName) -> bool {
    name == header::HOST
        || name == header::CONTENT_LENGTH
        || name == header::TRANSFER_ENCODING
        || name == header::CONNECTION
        || name.as_str().eq_ignore_ascii_case("mptunnel-event-id")
        || name.as_str().eq_ignore_ascii_case("mptunnel-delivery-id")
        || name.as_str().eq_ignore_ascii_case("upgrade")
}

fn append_header_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), ()> {
    if value.is_empty() || value.iter().any(|byte| *byte <= b' ' || *byte == 0x7f) {
        return Err(());
    }
    output.extend_from_slice(value);
    Ok(())
}

fn push_header(output: &mut Vec<u8>, name: &[u8], value: &[u8]) -> Result<(), ()> {
    if name.is_empty()
        || name
            .iter()
            .any(|byte| !byte.is_ascii_alphanumeric() && !b"!#$%&'*+-.^_`|~".contains(byte))
        || value
            .iter()
            .any(|byte| *byte == b'\r' || *byte == b'\n' || *byte == 0)
    {
        return Err(());
    }
    output.extend_from_slice(name);
    output.extend_from_slice(b": ");
    output.extend_from_slice(value);
    output.extend_from_slice(b"\r\n");
    Ok(())
}

fn host_authority(host: &str, port: u16, scheme: WebhookScheme) -> String {
    let mut authority = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_owned()
    };
    if port != scheme.default_port() {
        authority.push(':');
        authority.push_str(&port.to_string());
    }
    authority
}

pub(super) fn origin_tls_config(target: &WebhookTarget) -> Result<Option<Arc<ClientConfig>>, ()> {
    if target.url.scheme != WebhookScheme::Https {
        return Ok(None);
    }
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    for certificate in &target.tls_roots {
        roots.add(certificate.clone()).map_err(|_| ())?;
    }
    let mut config = ClientConfig::builder_with_protocol_versions(&[
        &rustls::version::TLS13,
        &rustls::version::TLS12,
    ])
    .with_root_certificates(roots)
    .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Some(Arc::new(config)))
}

enum WriteRequestError {
    Timeout,
    Transport(io::Error),
}

async fn write_request<S>(
    stream: &mut S,
    request: &[u8],
    deadline: tokio::time::Instant,
) -> Result<(), WriteRequestError>
where
    S: AsyncWrite + Unpin,
{
    match tokio::time::timeout_at(deadline, stream.write_all(request)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => return Err(WriteRequestError::Transport(error)),
        Err(_) => return Err(WriteRequestError::Timeout),
    }
    match tokio::time::timeout_at(deadline, stream.flush()).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(WriteRequestError::Transport(error)),
        Err(_) => Err(WriteRequestError::Timeout),
    }
}

#[derive(Debug)]
enum ResponseReadError {
    Timeout,
    Transport(io::Error),
    Invalid,
}

async fn read_final_headers<S>(
    stream: &mut S,
    deadline: tokio::time::Instant,
) -> Result<(u16, Option<Duration>), ResponseReadError>
where
    S: AsyncRead + Unpin,
{
    let mut bytes = Vec::with_capacity(MAX_ATTEMPT_HEADER_BYTES.min(4096));
    let mut offset = 0usize;
    let mut interim_responses = 0usize;
    loop {
        let mut headers = [httparse::EMPTY_HEADER; RESPONSE_HEADER_COUNT_LIMIT];
        let mut response = httparse::Response::new(&mut headers);
        match response.parse(&bytes[offset..]) {
            Ok(httparse::Status::Complete(length)) => {
                let status = response.code.ok_or(ResponseReadError::Invalid)?;
                let retry_after = response
                    .headers
                    .iter()
                    .find(|header| header.name.eq_ignore_ascii_case("retry-after"))
                    .and_then(|header| std::str::from_utf8(header.value).ok())
                    .and_then(|value| value.trim().parse::<u64>().ok())
                    .map(|seconds| Duration::from_secs(seconds.min(86_400)));
                offset += length;
                if (100..200).contains(&status) {
                    if status == 101 {
                        return Err(ResponseReadError::Invalid);
                    }
                    interim_responses += 1;
                    if interim_responses > 8 {
                        return Err(ResponseReadError::Invalid);
                    }
                    if offset == bytes.len() {
                        bytes.clear();
                        offset = 0;
                    }
                    continue;
                }
                return Ok((status, retry_after));
            }
            Ok(httparse::Status::Partial) => {}
            Err(_) => return Err(ResponseReadError::Invalid),
        }
        if bytes.len() >= MAX_ATTEMPT_HEADER_BYTES {
            return Err(ResponseReadError::Invalid);
        }
        let mut chunk = [0u8; READ_CHUNK_BYTES];
        let room = (MAX_ATTEMPT_HEADER_BYTES - bytes.len()).min(chunk.len());
        let read = match tokio::time::timeout_at(deadline, stream.read(&mut chunk[..room])).await {
            Ok(Ok(read)) => read,
            Ok(Err(error)) => return Err(ResponseReadError::Transport(error)),
            Err(_) => return Err(ResponseReadError::Timeout),
        };
        if read == 0 {
            return Err(ResponseReadError::Transport(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "webhook endpoint closed before final response headers",
            )));
        }
        bytes.extend_from_slice(&chunk[..read]);
        if offset != 0 && offset > bytes.len() / 2 {
            bytes.drain(..offset);
            offset = 0;
        }
    }
}

fn retryable_status(status: u16) -> bool {
    status == 408 || status == 429 || (500..600).contains(&status)
}

fn permanent(stage: &'static str, status: Option<u16>) -> AttemptOutcome {
    AttemptOutcome {
        status,
        stage,
        success: false,
        retryable: false,
        retry_after: None,
    }
}

fn retryable(stage: &'static str, status: Option<u16>) -> AttemptOutcome {
    AttemptOutcome {
        status,
        stage,
        success: false,
        retryable: true,
        retry_after: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EgressRef;
    use crate::product::OutboundId;
    use crate::webhook::{WebhookBody, WebhookTarget, WebhookUrl};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncWrite, AsyncWriteExt};

    fn target(roots: Vec<rustls::pki_types::CertificateDer<'static>>) -> WebhookTarget {
        WebhookTarget {
            url: WebhookUrl::parse("https://webhook.test/hook", Vec::new()).unwrap(),
            method: http::Method::POST,
            egress: EgressRef::Outbound(OutboundId::parse("test").unwrap()),
            dns_policy: None,
            target_resolution: crate::product::TargetResolutionMode::FullResolve,
            headers: Vec::new(),
            body: WebhookBody::None,
            tls_roots: roots,
        }
    }

    struct PartialWriter {
        written: Vec<u8>,
    }

    impl AsyncWrite for PartialWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            if this.written.is_empty() {
                let written = buffer.len().min(7);
                this.written.extend_from_slice(&buffer[..written]);
                Poll::Ready(Ok(written))
            } else {
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "injected partial write failure",
                )))
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn partial_request_write_failure_is_not_reported_as_success() {
        let mut writer = PartialWriter {
            written: Vec::new(),
        };
        let error = write_request(
            &mut writer,
            b"POST /hook HTTP/1.1\r\n\r\nbody",
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, WriteRequestError::Transport(ref error) if error.kind() == io::ErrorKind::BrokenPipe)
        );
        assert_eq!(writer.written.len(), 7);
    }

    #[tokio::test]
    async fn final_http_status_and_retry_after_are_bounded_and_classified() {
        let (mut writer, mut reader) = tokio::io::duplex(1024);
        writer
            .write_all(
                b"HTTP/1.1 503 Service Unavailable\r\nRetry-After: 3\r\nContent-Length: 0\r\n\r\n",
            )
            .await
            .unwrap();
        drop(writer);
        let (status, retry_after) = read_final_headers(
            &mut reader,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(status, 503);
        assert!(retryable_status(status));
        assert_eq!(retry_after, Some(Duration::from_secs(3)));

        let (mut writer, mut reader) = tokio::io::duplex(1024);
        writer
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .await
            .unwrap();
        drop(writer);
        let (status, _) = read_final_headers(
            &mut reader,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(status, 204);
        assert!(!retryable_status(status));
    }

    #[tokio::test]
    async fn origin_tls_requires_a_trusted_certificate_and_accepts_an_added_root() {
        let certified =
            rcgen::generate_simple_self_signed(vec!["webhook.test".to_owned()]).unwrap();
        let certificate = certified.cert.der().clone();
        let key =
            rustls::pki_types::PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());
        let server_config = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![certificate.clone()], key.into())
                .unwrap(),
        );

        async fn handshake(
            client_config: Arc<ClientConfig>,
            server_config: Arc<rustls::ServerConfig>,
        ) -> Result<(), io::Error> {
            let (client_io, server_io) = tokio::io::duplex(4096);
            let server = tokio::spawn(async move {
                tokio_rustls::TlsAcceptor::from(server_config)
                    .accept(server_io)
                    .await
            });
            let server_name = ServerName::try_from("webhook.test".to_owned()).unwrap();
            let result = TlsConnector::from(client_config)
                .connect(server_name, client_io)
                .await
                .map(|_| ());
            let _ = server.await;
            result
        }

        let untrusted = origin_tls_config(&target(Vec::new())).unwrap().unwrap();
        assert!(handshake(untrusted, server_config.clone()).await.is_err());

        let trusted = origin_tls_config(&target(vec![certificate]))
            .unwrap()
            .unwrap();
        handshake(trusted, server_config).await.unwrap();
    }

    #[test]
    fn invalid_extra_root_is_rejected_before_attempts_start() {
        let target = target(vec![rustls::pki_types::CertificateDer::from(vec![
            0xde, 0xad,
        ])]);
        assert!(origin_tls_config(&target).is_err());
    }
}
