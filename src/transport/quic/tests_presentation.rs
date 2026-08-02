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
