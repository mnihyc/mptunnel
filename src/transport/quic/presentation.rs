//! Genuine HTTP/3 presentation for QUIC carrier streams.
//!
//! Each logical MPP carrier stream is one full-duplex HTTP/3 POST. MPP
//! admission and its wire version remain in encrypted H3 DATA. The encrypted
//! `mpp-datagram` request field opts this application extension into RFC 9297
//! HTTP Datagrams without claiming CONNECT-UDP or Capsule Protocol semantics.

use super::{QuicCandidateSelector, QuicCandidateVerifier, QuicCarrierError};
use bytes::{Buf, Bytes};
use h3::ConnectionState;
use http::{HeaderValue, Method, Request, Response, StatusCode};
use std::future::poll_fn;
use std::sync::Arc;
use tokio::sync::{Mutex as AsyncMutex, mpsc};

const MAX_H3_FIELD_SECTION_BYTES: u64 = 4 * 1024;
const PUBLIC_NOT_FOUND_BODY: &[u8] = b"Not Found\n";
const MPP_CONTENT_TYPE: &str = "application/octet-stream";
const MPP_DATAGRAM_HEADER: &str = "mpp-datagram";
const MPP_DATAGRAM_OPT_IN: &str = "?1";
const CANDIDATE_AUTHORIZATION_PREFIX: &[u8] = b"Bearer ";
const CANDIDATE_SELECTOR_HEX_BYTES: usize = 64;
const CANDIDATE_AUTHORIZATION_BYTES: usize =
    CANDIDATE_AUTHORIZATION_PREFIX.len() + CANDIDATE_SELECTOR_HEX_BYTES;

type ClientRequestSender = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;
type ClientSendHalf = h3::client::RequestStream<h3_quinn::SendStream<Bytes>, Bytes>;
type ClientRecvHalf = h3::client::RequestStream<h3_quinn::RecvStream, Bytes>;
type ServerSendHalf = h3::server::RequestStream<h3_quinn::SendStream<Bytes>, Bytes>;
type ServerRecvHalf = h3::server::RequestStream<h3_quinn::RecvStream, Bytes>;

#[derive(Clone)]
pub(super) enum H3Presentation {
    Client {
        requests: ClientRequestSender,
        authority: Arc<str>,
        candidate_selector: QuicCandidateSelector,
    },
    Server {
        accepted: Arc<AsyncMutex<mpsc::Receiver<Result<H3Stream, QuicCarrierError>>>>,
    },
}

impl std::fmt::Debug for H3Presentation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Client { authority, .. } => f
                .debug_struct("H3Presentation::Client")
                .field("authority", authority)
                .finish_non_exhaustive(),
            Self::Server { .. } => f
                .debug_struct("H3Presentation::Server")
                .finish_non_exhaustive(),
        }
    }
}

pub(super) struct H3Stream {
    pub(super) send: H3SendStream,
    pub(super) recv: H3RecvStream,
    pub(super) request_stream_id: u64,
}

pub(super) struct H3SendStream {
    inner: Option<H3SendHalf>,
    response_state: ResponseState,
    finished: bool,
}

enum H3SendHalf {
    Client(ClientSendHalf),
    Server(ServerSendHalf),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseState {
    Client,
    ServerPending,
    ServerAccepted,
    ServerRejected,
}

pub(super) struct H3RecvStream {
    inner: H3RecvHalf,
    response_checked: bool,
}

enum H3RecvHalf {
    Client(ClientRecvHalf),
    Server(ServerRecvHalf),
}

impl std::fmt::Debug for H3SendStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("H3SendStream")
            .field("response_state", &self.response_state)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for H3RecvStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("H3RecvStream")
            .field("response_checked", &self.response_checked)
            .finish_non_exhaustive()
    }
}

impl H3Presentation {
    pub(super) async fn client(
        connection: quinn::Connection,
        authority: String,
        candidate_selector: QuicCandidateSelector,
    ) -> Result<Self, QuicCarrierError> {
        let quic = h3_quinn::Connection::new(connection);
        let (mut driver, requests) = h3::client::builder()
            .max_field_section_size(MAX_H3_FIELD_SECTION_BYTES)
            .enable_datagram(true)
            .build(quic)
            .await?;
        tokio::spawn(async move {
            let _ = poll_fn(|cx| driver.poll_close(cx)).await;
        });
        Ok(Self::Client {
            requests,
            authority: Arc::from(authority),
            candidate_selector,
        })
    }

    pub(super) async fn server(
        connection: quinn::Connection,
        accepted_queue: usize,
        candidate_verifier: Arc<dyn QuicCandidateVerifier>,
    ) -> Result<Self, QuicCarrierError> {
        let expected_authority = connection
            .handshake_data()
            .and_then(|data| data.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
            .and_then(|data| data.server_name)
            .map(Arc::<str>::from);
        let quic = h3_quinn::Connection::new(connection);
        let mut h3 = h3::server::builder()
            .max_field_section_size(MAX_H3_FIELD_SECTION_BYTES)
            .enable_datagram(true)
            .build(quic)
            .await?;
        let (accepted_tx, accepted_rx) = mpsc::channel(accepted_queue.max(1));
        tokio::spawn(async move {
            let mut candidate_gate =
                ConnectionCandidateGate::new(candidate_verifier, expected_authority);
            loop {
                let resolver = match h3.accept().await {
                    Ok(Some(resolver)) => resolver,
                    Ok(None) => return,
                    Err(err) => {
                        let _ = accepted_tx
                            .send(Err(QuicCarrierError::H3Connection(err)))
                            .await;
                        return;
                    }
                };
                let (request, mut request_stream) = match resolver.resolve_request().await {
                    Ok(request) => request,
                    Err(_) => continue,
                };
                if !candidate_gate.accepts_request(&request) {
                    let _ = send_public_not_found(&mut request_stream).await;
                    continue;
                }
                let request_stream_id = request_stream.send_id().into_inner();
                let (send, recv) = request_stream.split();
                let stream = H3Stream {
                    send: H3SendStream {
                        inner: Some(H3SendHalf::Server(send)),
                        response_state: ResponseState::ServerPending,
                        finished: false,
                    },
                    recv: H3RecvStream {
                        inner: H3RecvHalf::Server(recv),
                        response_checked: true,
                    },
                    request_stream_id,
                };
                if accepted_tx.send(Ok(stream)).await.is_err() {
                    return;
                }
            }
        });
        Ok(Self::Server {
            accepted: Arc::new(AsyncMutex::new(accepted_rx)),
        })
    }

    pub(super) async fn open(&self) -> Result<H3Stream, QuicCarrierError> {
        let Self::Client {
            requests,
            authority,
            candidate_selector,
        } = self
        else {
            return Err(QuicCarrierError::H3Role(
                "server HTTP/3 presentation cannot open requests",
            ));
        };
        let uri = format!("https://{authority}/");
        let request = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("content-type", MPP_CONTENT_TYPE)
            // RFC 9297 permits an HTTP extension to define its own explicit
            // datagram negotiation and DATA-stream protocol. This private
            // request field is encrypted as part of the H3 field section.
            .header(MPP_DATAGRAM_HEADER, MPP_DATAGRAM_OPT_IN)
            .header(
                http::header::AUTHORIZATION,
                candidate_authorization_value(candidate_selector),
            )
            .body(())
            .map_err(QuicCarrierError::H3Http)?;
        let mut sender = requests.clone();
        let stream = sender.send_request(request).await?;
        let request_stream_id = stream.id().into_inner();
        let (send, recv) = stream.split();
        Ok(H3Stream {
            send: H3SendStream {
                inner: Some(H3SendHalf::Client(send)),
                response_state: ResponseState::Client,
                finished: false,
            },
            recv: H3RecvStream {
                inner: H3RecvHalf::Client(recv),
                response_checked: false,
            },
            request_stream_id,
        })
    }

    pub(super) async fn accept(&self) -> Result<H3Stream, QuicCarrierError> {
        let Self::Server { accepted } = self else {
            return Err(QuicCarrierError::H3Role(
                "client HTTP/3 presentation cannot accept requests",
            ));
        };
        accepted
            .lock()
            .await
            .recv()
            .await
            .ok_or(QuicCarrierError::H3DriverClosed)?
    }
}

impl H3SendStream {
    pub(super) async fn ensure_datagrams_negotiated(&mut self) -> Result<(), QuicCarrierError> {
        if self.finished {
            return Err(QuicCarrierError::H3StreamFinished);
        }
        self.ensure_success_response().await?;
        // SETTINGS travels on an independent unidirectional stream and can be
        // reordered behind the request headers. Give the H3 driver a bounded
        // chance to publish the peer settings before the first native payload.
        for _ in 0..64 {
            let settings = match self
                .inner
                .as_ref()
                .ok_or(QuicCarrierError::H3StreamFinished)?
            {
                H3SendHalf::Client(stream) => stream.settings(),
                H3SendHalf::Server(stream) => stream.settings(),
            };
            if settings.enable_datagram() {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
        Err(QuicCarrierError::H3DatagramNotNegotiated)
    }

    pub(super) async fn send_data(&mut self, data: Bytes) -> Result<(), QuicCarrierError> {
        self.ensure_success_response().await?;
        match self
            .inner
            .as_mut()
            .ok_or(QuicCarrierError::H3StreamFinished)?
        {
            H3SendHalf::Client(stream) => stream.send_data(data).await?,
            H3SendHalf::Server(stream) => stream.send_data(data).await?,
        }
        Ok(())
    }

    pub(super) async fn finish(&mut self) -> Result<(), QuicCarrierError> {
        if self.finished {
            return Ok(());
        }
        self.ensure_success_response().await?;
        let finish_result = match self
            .inner
            .as_mut()
            .ok_or(QuicCarrierError::H3StreamFinished)?
        {
            H3SendHalf::Client(stream) => stream.finish().await,
            H3SendHalf::Server(stream) => stream.finish().await,
        };
        if let Err(error) = finish_result {
            // Dropping an unread Quinn receive half emits STOP_SENDING(0).
            // h3-quinn exposes that transport-level graceful abandonment as
            // RemoteTerminate rather than H3_NO_ERROR. If all application
            // bytes were already submitted and this operation is only adding
            // FIN, the peer has accepted that no more request/response DATA is
            // needed. Writes still surface the same signal as an error.
            if !matches!(
                &error,
                h3::error::StreamError::RemoteTerminate { code } if code.value() == 0
            ) {
                return Err(error.into());
            }
        }
        self.finished = true;
        Ok(())
    }

    pub(super) async fn reject(&mut self) -> Result<(), QuicCarrierError> {
        if self.response_state != ResponseState::ServerPending {
            return Ok(());
        }
        let Some(H3SendHalf::Server(stream)) = self.inner.as_mut() else {
            return Err(QuicCarrierError::H3Role(
                "only a pending server request can be rejected",
            ));
        };
        send_public_not_found(stream).await?;
        self.response_state = ResponseState::ServerRejected;
        self.finished = true;
        Ok(())
    }

    async fn ensure_success_response(&mut self) -> Result<(), QuicCarrierError> {
        if self.response_state != ResponseState::ServerPending {
            return Ok(());
        }
        let Some(H3SendHalf::Server(stream)) = self.inner.as_mut() else {
            return Err(QuicCarrierError::H3Role(
                "pending response exists on a non-server stream",
            ));
        };
        stream
            .send_response(
                Response::builder()
                    .status(StatusCode::OK)
                    .header("cache-control", "no-store")
                    .body(())
                    .map_err(QuicCarrierError::H3Http)?,
            )
            .await?;
        self.response_state = ResponseState::ServerAccepted;
        Ok(())
    }
}

impl Drop for H3SendStream {
    fn drop(&mut self) {
        if self.response_state != ResponseState::ServerPending {
            return;
        }
        let Some(H3SendHalf::Server(mut stream)) = self.inner.take() else {
            return;
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = send_public_not_found(&mut stream).await;
            });
        }
    }
}

impl H3RecvStream {
    pub(super) async fn ensure_success_response(&mut self) -> Result<(), QuicCarrierError> {
        if self.response_checked {
            return Ok(());
        }
        let H3RecvHalf::Client(stream) = &mut self.inner else {
            return Err(QuicCarrierError::H3Role(
                "only a client stream receives response headers",
            ));
        };
        let response = stream.recv_response().await?;
        self.response_checked = true;
        if !response.status().is_success() {
            return Err(QuicCarrierError::H3Status(response.status()));
        }
        Ok(())
    }

    pub(super) async fn recv_data(&mut self) -> Result<Option<Bytes>, QuicCarrierError> {
        self.ensure_success_response().await?;
        match &mut self.inner {
            H3RecvHalf::Client(stream) => {
                let data = stream.recv_data().await?;
                Ok(data.map(|mut data| data.copy_to_bytes(data.remaining())))
            }
            H3RecvHalf::Server(stream) => {
                let data = stream.recv_data().await?;
                Ok(data.map(|mut data| data.copy_to_bytes(data.remaining())))
            }
        }
    }
}

fn is_mpp_post(request: &Request<()>, expected_authority: Option<&str>) -> bool {
    request.method() == Method::POST
        && request.uri().scheme_str() == Some("https")
        && expected_authority.is_some_and(|expected| {
            request
                .uri()
                .authority()
                .is_some_and(|actual| actual.as_str() == expected)
        })
        && request
            .uri()
            .path_and_query()
            .is_some_and(|value| value.as_str() == "/")
        && request
            .headers()
            .get(http::header::CONTENT_TYPE)
            .is_some_and(|value| value == MPP_CONTENT_TYPE)
        && request
            .headers()
            .get(MPP_DATAGRAM_HEADER)
            .is_some_and(|value| value == MPP_DATAGRAM_OPT_IN)
}

struct ConnectionCandidateGate {
    verifier: Arc<dyn QuicCandidateVerifier>,
    expected_authority: Option<Arc<str>>,
    accepted: Option<QuicCandidateSelector>,
}

impl ConnectionCandidateGate {
    fn new(verifier: Arc<dyn QuicCandidateVerifier>, expected_authority: Option<Arc<str>>) -> Self {
        Self {
            verifier,
            expected_authority,
            accepted: None,
        }
    }

    fn accepts_request(&mut self, request: &Request<()>) -> bool {
        let (selector, canonical) = request_candidate_selector(request);
        let selector_matches = match &self.accepted {
            Some(accepted) => accepted.matches(&selector),
            None => self.verifier.accepts(&selector),
        };
        let accepted =
            is_mpp_post(request, self.expected_authority.as_deref()) & canonical & selector_matches;
        if accepted && self.accepted.is_none() {
            self.accepted = Some(selector);
        }
        accepted
    }
}

fn candidate_authorization_value(selector: &QuicCandidateSelector) -> HeaderValue {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let selector = selector.bytes();
    let mut encoded = Vec::with_capacity(CANDIDATE_AUTHORIZATION_BYTES);
    encoded.extend_from_slice(CANDIDATE_AUTHORIZATION_PREFIX);
    for byte in selector {
        encoded.push(HEX[(byte >> 4) as usize]);
        encoded.push(HEX[(byte & 0x0f) as usize]);
    }
    let mut value = HeaderValue::from_bytes(&encoded)
        .expect("canonical candidate selector is a valid HTTP header value");
    value.set_sensitive(true);
    value
}

fn request_candidate_selector(request: &Request<()>) -> (QuicCandidateSelector, bool) {
    let mut values = request
        .headers()
        .get_all(http::header::AUTHORIZATION)
        .iter();
    let Some(value) = values.next() else {
        return (QuicCandidateSelector::from_bytes([0; 32]), false);
    };
    if values.next().is_some() {
        return (QuicCandidateSelector::from_bytes([0; 32]), false);
    }
    let encoded = value.as_bytes();
    if encoded.len() != CANDIDATE_AUTHORIZATION_BYTES
        || !encoded.starts_with(CANDIDATE_AUTHORIZATION_PREFIX)
    {
        return (QuicCandidateSelector::from_bytes([0; 32]), false);
    }
    let mut selector = [0_u8; 32];
    for (output, pair) in selector
        .iter_mut()
        .zip(encoded[CANDIDATE_AUTHORIZATION_PREFIX.len()..].chunks_exact(2))
    {
        let Some(high) = decode_lower_hex(pair[0]) else {
            return (QuicCandidateSelector::from_bytes([0; 32]), false);
        };
        let Some(low) = decode_lower_hex(pair[1]) else {
            return (QuicCandidateSelector::from_bytes([0; 32]), false);
        };
        *output = (high << 4) | low;
    }
    (QuicCandidateSelector::from_bytes(selector), true)
}

const fn decode_lower_hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

async fn send_public_not_found<S>(
    stream: &mut h3::server::RequestStream<S, Bytes>,
) -> Result<(), QuicCarrierError>
where
    S: h3::quic::SendStream<Bytes>,
{
    stream
        .send_response(
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header("content-type", "text/plain; charset=utf-8")
                .header("content-length", PUBLIC_NOT_FOUND_BODY.len())
                .header("cache-control", "no-store")
                .body(())
                .map_err(QuicCarrierError::H3Http)?,
        )
        .await?;
    stream
        .send_data(Bytes::from_static(PUBLIC_NOT_FOUND_BODY))
        .await?;
    stream.finish().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Debug)]
    struct CountingVerifier {
        expected: QuicCandidateSelector,
        enabled: AtomicBool,
        calls: AtomicUsize,
    }

    impl QuicCandidateVerifier for CountingVerifier {
        fn accepts(&self, selector: &QuicCandidateSelector) -> bool {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.enabled.load(Ordering::Relaxed) & self.expected.matches(selector)
        }
    }

    fn mpp_request(selector: &QuicCandidateSelector) -> Request<()> {
        Request::builder()
            .method(Method::POST)
            .uri("https://example.test/")
            .header(http::header::CONTENT_TYPE, MPP_CONTENT_TYPE)
            .header(MPP_DATAGRAM_HEADER, MPP_DATAGRAM_OPT_IN)
            .header(
                http::header::AUTHORIZATION,
                candidate_authorization_value(selector),
            )
            .body(())
            .expect("MPP request")
    }

    #[test]
    fn mpp_post_contract_does_not_claim_registered_tunnel_semantics() {
        let selector = QuicCandidateSelector::derive("edge", b"selector test secret");
        let accepted = mpp_request(&selector);
        assert!(is_mpp_post(&accepted, Some("example.test")));
        assert!(!is_mpp_post(&accepted, None));

        let mut connect_udp = Request::builder()
            .method(Method::CONNECT)
            .uri("https://example.test/")
            .header("capsule-protocol", "?1")
            .body(())
            .expect("CONNECT-UDP probe");
        connect_udp
            .extensions_mut()
            .insert(h3::ext::Protocol::CONNECT_UDP);
        assert!(!is_mpp_post(&connect_udp, Some("example.test")));

        let no_datagram_opt_in = Request::builder()
            .method(Method::POST)
            .uri("https://example.test/")
            .header(http::header::CONTENT_TYPE, MPP_CONTENT_TYPE)
            .body(())
            .expect("ordinary POST");
        assert!(!is_mpp_post(&no_datagram_opt_in, Some("example.test")));

        for uri in [
            "http://example.test/",
            "https://other.test/",
            "https://example.test/?probe",
            "/",
        ] {
            let mut request = mpp_request(&selector);
            *request.uri_mut() = uri.parse().expect("probe URI");
            assert!(
                !is_mpp_post(&request, Some("example.test")),
                "{uri} must not pass the exact HTTP/3 presentation gate"
            );
        }
    }

    #[test]
    fn candidate_selector_is_canonical_and_binds_name_and_secret() {
        let selector = QuicCandidateSelector::derive("edge", b"selector test secret");
        let same = QuicCandidateSelector::derive("edge", b"selector test secret");
        let other_id = QuicCandidateSelector::derive("other", b"selector test secret");
        let other_secret = QuicCandidateSelector::derive("edge", b"different selector secret");
        assert!(selector.matches(&same));
        assert!(!selector.matches(&other_id));
        assert!(!selector.matches(&other_secret));

        let request = mpp_request(&selector);
        let authorization = request
            .headers()
            .get(http::header::AUTHORIZATION)
            .expect("candidate Authorization");
        assert!(authorization.is_sensitive());
        let (decoded, canonical) = request_candidate_selector(&request);
        assert!(canonical);
        assert!(selector.matches(&decoded));

        let uppercase = Request::builder()
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer {}", "AA".repeat(32)),
            )
            .body(())
            .expect("uppercase selector");
        assert!(!request_candidate_selector(&uppercase).1);

        let mut duplicate = mpp_request(&selector);
        duplicate.headers_mut().append(
            http::header::AUTHORIZATION,
            candidate_authorization_value(&selector),
        );
        assert!(!request_candidate_selector(&duplicate).1);
    }

    #[test]
    fn first_selector_latches_connection_without_weakening_full_authentication() {
        let selector = QuicCandidateSelector::derive("edge", b"selector test secret");
        let wrong = QuicCandidateSelector::derive("edge", b"wrong selector test secret");
        let verifier = Arc::new(CountingVerifier {
            expected: selector.clone(),
            enabled: AtomicBool::new(true),
            calls: AtomicUsize::new(0),
        });
        let mut gate =
            ConnectionCandidateGate::new(verifier.clone(), Some(Arc::<str>::from("example.test")));

        let ordinary = Request::get("https://example.test/")
            .body(())
            .expect("ordinary request");
        assert!(!gate.accepts_request(&ordinary));
        assert_eq!(verifier.calls.load(Ordering::Relaxed), 1);

        assert!(!gate.accepts_request(&mpp_request(&wrong)));
        assert_eq!(verifier.calls.load(Ordering::Relaxed), 2);

        assert!(gate.accepts_request(&mpp_request(&selector)));
        assert_eq!(verifier.calls.load(Ordering::Relaxed), 3);

        // Publishing a revocation must not make the transport re-check an
        // already gated connection. Existing authenticated permits own their
        // independently bounded retirement grace.
        verifier.enabled.store(false, Ordering::Relaxed);
        assert!(gate.accepts_request(&mpp_request(&selector)));
        assert!(!gate.accepts_request(&mpp_request(&wrong)));
        assert_eq!(verifier.calls.load(Ordering::Relaxed), 3);
    }
}
