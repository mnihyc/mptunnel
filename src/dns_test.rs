use super::*;
use crate::product::{
    DnsHostSpec, DnsOutboundCapabilitySpec, DnsPlanSpec, DnsPolicySpec, DnsRuleMatch, DnsRuleSpec,
    DnsSecurityPolicy, DnsUpstreamSpec, DnsUpstreamStrategy, FakeDnsSpec, NetworkSet,
};
use hickory_proto::rr::rdata::{CNAME, TXT};
use std::collections::VecDeque;
use std::net::UdpSocket as StdUdpSocket;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::net::TcpSocket;
use tokio::net::UdpSocket;

fn upstream_id(value: &str) -> DnsUpstreamId {
    DnsUpstreamId::parse(value).expect("upstream ID")
}

fn plan_id(value: &str) -> DnsPlanId {
    DnsPlanId::parse(value).expect("plan ID")
}

fn rule_id(value: &str) -> DnsRuleId {
    DnsRuleId::parse(value).expect("rule ID")
}

fn domain(value: &str) -> DomainName {
    DomainName::parse(value).expect("domain")
}

fn wire_query(id: u16, name: &str, record_type: RecordType) -> Vec<u8> {
    let mut request = Message::query();
    request.metadata.id = id;
    request.metadata.recursion_desired = true;
    request.add_query(Query::query(
        Name::from_ascii(name).expect("DNS query name"),
        record_type,
    ));
    request.to_vec().expect("DNS wire query")
}

fn direct_udp(id: &str, address: &str) -> DnsUpstreamSpec {
    DnsUpstreamSpec::direct(
        upstream_id(id),
        DnsUpstreamEndpoint::Udp {
            bootstrap: address.parse().expect("bootstrap"),
        },
    )
}

fn ipv4_plan(id: &str, upstreams: Vec<DnsUpstreamId>) -> DnsPlanSpec {
    let mut plan = DnsPlanSpec::new(plan_id(id), upstreams);
    plan.ip_strategy = DnsIpStrategy::Ipv4Only;
    plan
}

fn policy(
    generation: u64,
    upstreams: Vec<DnsUpstreamSpec>,
    plans: Vec<DnsPlanSpec>,
    rules: Vec<DnsRuleSpec>,
    default_plan: &str,
) -> Arc<CompiledDnsPolicy> {
    policy_with_hosts(
        generation,
        upstreams,
        plans,
        rules,
        Vec::new(),
        default_plan,
    )
}

fn policy_with_hosts(
    generation: u64,
    upstreams: Vec<DnsUpstreamSpec>,
    plans: Vec<DnsPlanSpec>,
    rules: Vec<DnsRuleSpec>,
    hosts: Vec<DnsHostSpec>,
    default_plan: &str,
) -> Arc<CompiledDnsPolicy> {
    Arc::new(
        CompiledDnsPolicy::compile(
            generation,
            DnsPolicySpec {
                upstreams,
                outbound_capabilities: Vec::new(),
                plans,
                rules,
                hosts,
                fake_dns: None,
                default_plan: plan_id(default_plan),
            },
        )
        .expect("DNS policy"),
    )
}

#[derive(Clone)]
enum MockResult {
    Positive(Vec<IpAddr>),
    Response {
        code: ResponseCode,
        answers: Vec<Record>,
        authorities: Vec<Record>,
        additionals: Vec<Record>,
        ttl: Duration,
    },
    Negative,
    Failed,
}

struct MockBackend {
    result: MockResult,
    delay: Duration,
    calls: AtomicUsize,
}

impl MockBackend {
    fn new(result: MockResult) -> Arc<Self> {
        Arc::new(Self {
            result,
            delay: Duration::ZERO,
            calls: AtomicUsize::new(0),
        })
    }

    fn delayed(result: MockResult, delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            result,
            delay,
            calls: AtomicUsize::new(0),
        })
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Acquire)
    }
}

impl DnsQueryBackend for MockBackend {
    fn query(&self, question: DnsQuestion) -> DnsRecordBackendFuture {
        self.calls.fetch_add(1, Ordering::AcqRel);
        let result = self.result.clone();
        let delay = self.delay;
        Box::pin(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            mock_result(question, result)
        })
    }
}

struct MapFactory {
    backends: HashMap<DnsUpstreamId, Arc<dyn DnsQueryBackend>>,
}

fn test_backend<T>(backend: Arc<T>) -> Arc<dyn DnsQueryBackend>
where
    T: DnsQueryBackend + 'static,
{
    backend
}

#[derive(Clone)]
struct MockStep {
    result: MockResult,
    delay: Duration,
}

struct SequenceBackend {
    steps: StdMutex<VecDeque<MockStep>>,
    calls: AtomicUsize,
}

impl SequenceBackend {
    fn new(steps: impl IntoIterator<Item = MockStep>) -> Arc<Self> {
        Arc::new(Self {
            steps: StdMutex::new(steps.into_iter().collect()),
            calls: AtomicUsize::new(0),
        })
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Acquire)
    }
}

impl DnsQueryBackend for SequenceBackend {
    fn query(&self, question: DnsQuestion) -> DnsRecordBackendFuture {
        self.calls.fetch_add(1, Ordering::AcqRel);
        let step = self.steps.lock().expect("mock steps").pop_front();
        Box::pin(async move {
            let step = step.ok_or_else(|| {
                DnsBackendError::Failed("scripted DNS backend exhausted".to_string())
            })?;
            if !step.delay.is_zero() {
                tokio::time::sleep(step.delay).await;
            }
            mock_result(question, step.result)
        })
    }
}

fn mock_result(
    question: DnsQuestion,
    result: MockResult,
) -> Result<DnsBackendResponse, DnsBackendError> {
    match result {
        MockResult::Positive(addresses) => {
            let query = question.as_query()?;
            let mut message = Message::response(0, OpCode::Query);
            message.add_query(query.clone());
            for address in addresses {
                let data = match (question.record_type(), address) {
                    (RecordType::A, IpAddr::V4(address)) => RData::A(A(address)),
                    (RecordType::AAAA, IpAddr::V6(address)) => RData::AAAA(AAAA(address)),
                    _ => continue,
                };
                message.add_answer(Record::from_rdata(query.name().clone(), 60, data));
            }
            if message.answers.is_empty() {
                Err(DnsBackendError::NoRecords {
                    ttl: Some(Duration::from_secs(10)),
                })
            } else {
                Ok(DnsBackendResponse::new(
                    message,
                    Some(Duration::from_secs(60)),
                ))
            }
        }
        MockResult::Response {
            code,
            answers,
            authorities,
            additionals,
            ttl,
        } => {
            let mut message = Message::response(0, OpCode::Query);
            message.add_query(question.as_query()?);
            message.metadata.response_code = code;
            message.add_answers(answers);
            message.add_authorities(authorities);
            message.add_additionals(additionals);
            Ok(DnsBackendResponse::new(message, Some(ttl)))
        }
        MockResult::Negative => Err(DnsBackendError::NoRecords {
            ttl: Some(Duration::from_secs(10)),
        }),
        MockResult::Failed => Err(DnsBackendError::Failed("mock failure".to_string())),
    }
}

fn response_result(
    code: ResponseCode,
    answers: Vec<Record>,
    authorities: Vec<Record>,
    additionals: Vec<Record>,
    ttl: Duration,
) -> MockResult {
    MockResult::Response {
        code,
        answers,
        authorities,
        additionals,
        ttl,
    }
}

fn a_response_result(
    owner: &str,
    address: std::net::Ipv4Addr,
    record_ttl: u32,
    cache_ttl: Duration,
) -> MockResult {
    response_result(
        ResponseCode::NoError,
        vec![Record::from_rdata(
            Name::from_ascii(owner).expect("A record owner"),
            record_ttl,
            RData::A(A(address)),
        )],
        Vec::new(),
        Vec::new(),
        cache_ttl,
    )
}

#[derive(Default)]
struct RejectingDnsSocketConfigurator {
    requests: StdMutex<Vec<crate::transport::NativeSocketRequest>>,
}

impl crate::transport::NativeSocketConfigurator for RejectingDnsSocketConfigurator {
    fn configure_tcp(
        &self,
        _socket: &TcpSocket,
        request: crate::transport::NativeSocketRequest,
    ) -> std::io::Result<()> {
        self.requests.lock().expect("requests").push(request);
        Err(std::io::Error::other("test DNS TCP socket rejection"))
    }

    fn configure_udp(
        &self,
        _socket: &StdUdpSocket,
        request: crate::transport::NativeSocketRequest,
    ) -> std::io::Result<()> {
        self.requests.lock().expect("requests").push(request);
        Err(std::io::Error::other("test DNS UDP socket rejection"))
    }
}

impl DnsBackendFactory for MapFactory {
    fn build_backend(
        &self,
        _plan: &CompiledDnsPlan,
        upstream: &CompiledDnsUpstream,
    ) -> Result<Arc<dyn DnsQueryBackend>, DnsRuntimeError> {
        self.backends.get(upstream.id()).cloned().ok_or_else(|| {
            DnsRuntimeError::PolicyInvariant(format!("test backend missing for {}", upstream.id()))
        })
    }
}

#[tokio::test]
async fn split_selection_metadata_and_plan_caches_are_isolated() {
    let public = MockBackend::new(MockResult::Positive(vec![
        "192.0.2.10".parse().expect("address"),
    ]));
    let private = MockBackend::new(MockResult::Positive(vec![
        "10.0.0.10".parse().expect("address"),
    ]));
    let compiled_policy = policy_with_hosts(
        41,
        vec![
            direct_udp("public-upstream", "192.0.2.53:53"),
            direct_udp("private-upstream", "10.0.0.53:53"),
        ],
        vec![
            ipv4_plan("public", vec![upstream_id("public-upstream")]),
            ipv4_plan("private", vec![upstream_id("private-upstream")]),
        ],
        vec![DnsRuleSpec {
            id: rule_id("private-zone"),
            matcher: DnsRuleMatch::Suffix(domain("lan.example")),
            plan: plan_id("private"),
            explanation: Some("private split zone".to_string()),
        }],
        vec![DnsHostSpec {
            domain: domain("router.home.arpa"),
            addresses: vec![
                "192.168.1.1".parse().expect("IPv4 host"),
                "fd00::1".parse().expect("IPv6 host"),
            ],
        }],
        "public",
    );
    let factory = MapFactory {
        backends: HashMap::from([
            (upstream_id("public-upstream"), test_backend(public.clone())),
            (
                upstream_id("private-upstream"),
                test_backend(private.clone()),
            ),
        ]),
    };
    let runtime = DnsGeneration::compile_with_factory(compiled_policy, &factory).expect("runtime");

    let selected = runtime
        .resolve(&domain("router.lan.example"))
        .await
        .expect("split lookup");
    assert_eq!(
        selected.addresses().as_ref(),
        &["10.0.0.10".parse::<IpAddr>().expect("IP")]
    );
    assert_eq!(selected.metadata().generation(), 41);
    assert_eq!(selected.metadata().plan().as_str(), "private");
    assert_eq!(
        selected.metadata().rule().map(DnsRuleId::as_str),
        Some("private-zone")
    );
    assert_eq!(
        selected.metadata().explanation(),
        Some("private split zone")
    );
    let explanation = runtime.explain(&domain("router.lan.example"));
    assert_eq!(explanation.generation, 41);
    assert_eq!(explanation.plan.as_str(), "private");
    assert_eq!(
        explanation.rule.as_ref().map(DnsRuleId::as_str),
        Some("private-zone")
    );
    assert_eq!(explanation.upstreams.len(), 1);
    assert_eq!(
        explanation.upstreams[0].upstream.as_str(),
        "private-upstream"
    );
    assert_eq!(explanation.upstreams[0].transport, DnsTransport::Udp);
    assert_eq!(
        explanation.upstreams[0].bootstrap,
        Some("10.0.0.53:53".parse().expect("bootstrap"))
    );
    assert_eq!(explanation.upstreams[0].egress, DnsEgressSpec::Direct);

    let shared_name = domain("same.example");
    for _ in 0..2 {
        assert_eq!(
            runtime
                .resolve_in_plan(&plan_id("public"), &shared_name)
                .await
                .expect("public"),
            Arc::<[IpAddr]>::from(["192.0.2.10".parse().expect("IP")])
        );
        assert_eq!(
            runtime
                .resolve_in_plan(&plan_id("private"), &shared_name)
                .await
                .expect("private"),
            Arc::<[IpAddr]>::from(["10.0.0.10".parse().expect("IP")])
        );
    }
    assert_eq!(public.calls(), 1);
    assert_eq!(private.calls(), 2, "split name and isolated shared name");

    let host = runtime
        .resolve(&domain("router.home.arpa"))
        .await
        .expect("exact host override");
    assert_eq!(
        host.addresses().as_ref(),
        &["192.168.1.1".parse::<IpAddr>().expect("IPv4")]
    );
    let ipv6 = runtime
        .query_record(&domain("router.home.arpa"), RecordType::AAAA)
        .await
        .expect("local IPv6 host record");
    assert!(matches!(
        &ipv6.message().answers[0].data,
        RData::AAAA(address) if address.0 == "fd00::1".parse::<std::net::Ipv6Addr>().expect("IPv6")
    ));
    let txt = runtime
        .query_record(&domain("router.home.arpa"), RecordType::TXT)
        .await
        .expect("private host TXT is local NODATA");
    assert_eq!(txt.message().metadata.response_code, ResponseCode::NoError);
    assert!(txt.message().metadata.authoritative);
    assert!(txt.message().answers.is_empty());
    assert_eq!(
        public.calls(),
        1,
        "unmatched host record types must not leak upstream"
    );
    let explanation = runtime.explain(&domain("router.home.arpa"));
    assert!(explanation.host_addresses.is_some());
    assert!(explanation.upstreams.is_empty());
    let snapshot = runtime.runtime_snapshot();
    assert_eq!(snapshot.host_overrides, 1);
    assert_eq!(
        snapshot
            .plans
            .iter()
            .map(|plan| plan.host_answers)
            .sum::<u64>(),
        3,
        "dual-stack address resolution plus TXT used the local host override"
    );
}

#[tokio::test]
async fn same_plan_concurrent_lookups_are_coalesced() {
    let backend = MockBackend::delayed(
        MockResult::Positive(vec!["192.0.2.20".parse().expect("address")]),
        Duration::from_millis(30),
    );
    let policy = policy(
        1,
        vec![direct_udp("one", "192.0.2.53:53")],
        vec![ipv4_plan("default", vec![upstream_id("one")])],
        Vec::new(),
        "default",
    );
    let factory = MapFactory {
        backends: HashMap::from([(upstream_id("one"), test_backend(backend.clone()))]),
    };
    let runtime =
        DnsGeneration::compile_with_factory(policy, &factory).expect("resolver generation");
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let runtime = runtime.clone();
        tasks.spawn(async move { runtime.resolve(&domain("coalesce.example")).await });
    }
    while let Some(result) = tasks.join_next().await {
        assert_eq!(
            result.expect("task").expect("lookup").addresses().as_ref(),
            &["192.0.2.20".parse::<IpAddr>().expect("IP")]
        );
    }
    assert_eq!(backend.calls(), 1);
}

#[tokio::test]
async fn stale_refresh_is_coalesced_bounded_observable_and_flushable() {
    let backend = SequenceBackend::new([
        MockStep {
            result: a_response_result(
                "stale.example.",
                "192.0.2.70".parse().expect("initial address"),
                1,
                Duration::from_millis(40),
            ),
            delay: Duration::ZERO,
        },
        MockStep {
            result: MockResult::Failed,
            delay: Duration::from_millis(30),
        },
        MockStep {
            result: a_response_result(
                "stale.example.",
                "192.0.2.71".parse().expect("refreshed address"),
                30,
                Duration::from_secs(30),
            ),
            delay: Duration::ZERO,
        },
    ]);
    let runtime = DnsGeneration::compile_with_factory(
        policy(
            60,
            vec![direct_udp("sequence", "192.0.2.53:53")],
            vec![ipv4_plan("default", vec![upstream_id("sequence")])],
            Vec::new(),
            "default",
        ),
        &MapFactory {
            backends: HashMap::from([(upstream_id("sequence"), test_backend(backend.clone()))]),
        },
    )
    .expect("cache runtime");
    let name = domain("stale.example");

    let initial = runtime
        .query_record(&name, RecordType::A)
        .await
        .expect("initial answer");
    assert!(!initial.is_stale());
    tokio::time::sleep(Duration::from_millis(55)).await;

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let runtime = runtime.clone();
        let name = name.clone();
        tasks.spawn(async move { runtime.query_record(&name, RecordType::A).await });
    }
    while let Some(result) = tasks.join_next().await {
        let answer = result.expect("stale task").expect("stale answer");
        assert!(answer.is_stale());
        assert!(matches!(
            &answer.message().answers[0].data,
            RData::A(A(address))
                if *address == "192.0.2.70".parse::<std::net::Ipv4Addr>().expect("address")
        ));
        assert!(
            answer.message().answers[0].ttl <= 1,
            "serve-stale must not increase the upstream record TTL"
        );
    }
    assert_eq!(backend.calls(), 2, "one failed refresh must be coalesced");

    let snapshot = runtime.runtime_snapshot();
    let plan = &snapshot.plans[0];
    assert_eq!(plan.stale_cache_entries, 1);
    assert!(plan.coalesced_queries >= 23, "{plan:?}");
    assert_eq!(plan.stale_answers, 24);
    assert_eq!(plan.upstreams[0].attempts, 2);
    assert_eq!(plan.upstreams[0].successes, 1);
    assert_eq!(plan.upstreams[0].failures, 1);

    let flushed = runtime.flush_cache(None).expect("cache flush");
    assert_eq!(flushed.generation, 60);
    assert_eq!(flushed.plans, 1);
    assert_eq!(flushed.removed_entries, 1);
    let empty = runtime.runtime_snapshot();
    assert_eq!(empty.plans[0].cache_entries, 0);
    assert_eq!(empty.plans[0].cache_flushes, 1);

    let refreshed = runtime
        .query_record(&name, RecordType::A)
        .await
        .expect("post-flush refresh");
    assert!(!refreshed.is_stale());
    assert!(matches!(
        &refreshed.message().answers[0].data,
        RData::A(A(address))
            if *address == "192.0.2.71".parse::<std::net::Ipv4Addr>().expect("address")
    ));
    assert_eq!(backend.calls(), 3);
}

#[tokio::test]
async fn captured_dns_wire_answers_a_and_aaaa_without_cross_family_records() {
    let runtime = DnsGeneration::from_test_answers(HashMap::from([(
        "dual.example".to_string(),
        vec![
            "192.0.2.80".parse().expect("IPv4"),
            "2001:db8::80".parse().expect("IPv6"),
        ],
    )]));

    let ipv4 = runtime
        .answer_wire_query(
            &wire_query(0x1201, "dual.example.", RecordType::A),
            Duration::from_millis(5_500),
            1_232,
        )
        .await
        .expect("A response");
    let ipv4 = Message::from_vec(&ipv4).expect("decoded A response");
    assert_eq!(ipv4.metadata.id, 0x1201);
    assert_eq!(ipv4.metadata.message_type, MessageType::Response);
    assert!(ipv4.metadata.recursion_available);
    assert_eq!(ipv4.metadata.response_code, ResponseCode::NoError);
    assert_eq!(ipv4.queries.len(), 1);
    assert_eq!(ipv4.answers.len(), 1);
    assert_eq!(ipv4.answers[0].ttl, 5);
    assert!(matches!(
        &ipv4.answers[0].data,
        RData::A(A(address))
            if *address == "192.0.2.80".parse::<std::net::Ipv4Addr>().expect("IPv4")
    ));

    let ipv6 = runtime
        .answer_wire_query(
            &wire_query(0x1202, "dual.example.", RecordType::AAAA),
            Duration::from_secs(60),
            1_232,
        )
        .await
        .expect("AAAA response");
    let ipv6 = Message::from_vec(&ipv6).expect("decoded AAAA response");
    assert_eq!(ipv6.answers.len(), 1);
    assert!(matches!(
        &ipv6.answers[0].data,
        RData::AAAA(AAAA(address))
            if *address == "2001:db8::80".parse::<std::net::Ipv6Addr>().expect("IPv6")
    ));
}

#[tokio::test]
async fn fake_dns_capture_recovers_domains_once_without_replacing_real_resolution() {
    let backend = MockBackend::new(MockResult::Positive(vec![
        "203.0.113.20".parse().expect("real address"),
    ]));
    let upstream = direct_udp("real", "192.0.2.53:53");
    let plan = ipv4_plan("default", vec![upstream_id("real")]);
    let policy = Arc::new(
        CompiledDnsPolicy::compile(
            7,
            DnsPolicySpec {
                upstreams: vec![upstream],
                outbound_capabilities: Vec::new(),
                plans: vec![plan],
                rules: Vec::new(),
                hosts: Vec::new(),
                fake_dns: Some(FakeDnsSpec {
                    ipv4_pool: Some("198.18.0.0/24".parse().expect("IPv4 pool")),
                    ipv6_pool: Some("fd00:4d50::/120".parse().expect("IPv6 pool")),
                    max_entries: 32,
                    answer_ttl: Duration::from_millis(10),
                    recovery_ttl: Duration::from_millis(40),
                }),
                default_plan: plan_id("default"),
            },
        )
        .expect("FakeDNS policy"),
    );
    let runtime = DnsGeneration::compile_with_factory(
        policy,
        &MapFactory {
            backends: HashMap::from([(upstream_id("real"), test_backend(backend.clone()))]),
        },
    )
    .expect("FakeDNS runtime");

    let response = runtime
        .answer_wire_query(
            &wire_query(0x7001, "fake.example.", RecordType::A),
            Duration::from_secs(60),
            1_232,
        )
        .await
        .expect("FakeDNS A response");
    let response = Message::from_vec(&response).expect("decoded FakeDNS A response");
    let fake_ipv4 = match response.answers.as_slice() {
        [record] => match &record.data {
            RData::A(address) => IpAddr::V4(address.0),
            other => panic!("unexpected FakeDNS A data {other:?}"),
        },
        other => panic!("unexpected FakeDNS A answer count {}", other.len()),
    };
    assert!(
        "198.18.0.0/24"
            .parse::<IpNet>()
            .expect("pool")
            .contains(&fake_ipv4)
    );
    assert_eq!(
        backend.calls(),
        0,
        "captured FakeDNS must not query upstream"
    );
    assert_eq!(
        runtime.recover_fake_dns(fake_ipv4),
        FakeDnsRecovery::Recovered(domain("fake.example"))
    );

    let response = runtime
        .answer_wire_query(
            &wire_query(0x7002, "fake.example.", RecordType::AAAA),
            Duration::from_secs(60),
            1_232,
        )
        .await
        .expect("FakeDNS AAAA response");
    let response = Message::from_vec(&response).expect("decoded FakeDNS AAAA response");
    let fake_ipv6 = match response.answers.as_slice() {
        [record] => match &record.data {
            RData::AAAA(address) => IpAddr::V6(address.0),
            other => panic!("unexpected FakeDNS AAAA data {other:?}"),
        },
        other => panic!("unexpected FakeDNS AAAA answer count {}", other.len()),
    };
    assert!(
        "fd00:4d50::/120"
            .parse::<IpNet>()
            .expect("pool")
            .contains(&fake_ipv6)
    );

    let resolved = runtime
        .resolve(&domain("fake.example"))
        .await
        .expect("real dial-time resolution");
    assert_eq!(
        resolved.addresses().as_ref(),
        &["203.0.113.20".parse::<IpAddr>().expect("real address")]
    );
    assert_eq!(backend.calls(), 1);

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        runtime.recover_fake_dns(fake_ipv4),
        FakeDnsRecovery::Expired,
        "expired synthetic addresses must fail closed"
    );
    assert_eq!(
        runtime.recover_fake_dns("198.18.0.200".parse().expect("unknown fake")),
        FakeDnsRecovery::Unknown
    );
    assert_eq!(
        runtime.recover_fake_dns("192.0.2.1".parse().expect("ordinary address")),
        FakeDnsRecovery::NotFake
    );
    let snapshot = runtime.runtime_snapshot().fake_dns.expect("FakeDNS status");
    assert_eq!(snapshot.owned_entries, 1);
    assert_eq!(snapshot.active_entries, 0);
    assert_eq!(snapshot.answers, 2);
    assert_eq!(snapshot.recoveries, 1);
    assert_eq!(snapshot.expired_recoveries, 1);
    assert_eq!(snapshot.unknown_recoveries, 1);
}

#[tokio::test]
async fn fake_dns_capacity_never_reassigns_an_owned_address() {
    let backend = MockBackend::new(MockResult::Failed);
    let policy = Arc::new(
        CompiledDnsPolicy::compile(
            8,
            DnsPolicySpec {
                upstreams: vec![direct_udp("unused", "192.0.2.53:53")],
                outbound_capabilities: Vec::new(),
                plans: vec![ipv4_plan("default", vec![upstream_id("unused")])],
                rules: Vec::new(),
                hosts: Vec::new(),
                fake_dns: Some(FakeDnsSpec {
                    ipv4_pool: Some("198.18.1.0/30".parse().expect("IPv4 pool")),
                    ipv6_pool: None,
                    max_entries: 1,
                    answer_ttl: Duration::from_millis(5),
                    recovery_ttl: Duration::from_millis(10),
                }),
                default_plan: plan_id("default"),
            },
        )
        .expect("bounded FakeDNS policy"),
    );
    let runtime = DnsGeneration::compile_with_factory(
        policy,
        &MapFactory {
            backends: HashMap::from([(upstream_id("unused"), test_backend(backend.clone()))]),
        },
    )
    .expect("bounded FakeDNS runtime");
    let first = runtime
        .answer_wire_query(
            &wire_query(0x7101, "first.example.", RecordType::A),
            Duration::from_secs(1),
            1_232,
        )
        .await
        .expect("first FakeDNS response");
    let first = Message::from_vec(&first).expect("decoded first response");
    let first_address = match &first.answers[0].data {
        RData::A(address) => IpAddr::V4(address.0),
        other => panic!("unexpected first FakeDNS data {other:?}"),
    };
    tokio::time::sleep(Duration::from_millis(15)).await;

    let second = runtime
        .answer_wire_query(
            &wire_query(0x7102, "second.example.", RecordType::A),
            Duration::from_secs(1),
            1_232,
        )
        .await
        .expect("capacity response");
    let second = Message::from_vec(&second).expect("decoded capacity response");
    assert_eq!(second.metadata.response_code, ResponseCode::ServFail);
    assert!(second.answers.is_empty());
    assert_eq!(
        runtime.recover_fake_dns(first_address),
        FakeDnsRecovery::Expired
    );
    assert_eq!(backend.calls(), 0);
    assert_eq!(
        runtime
            .runtime_snapshot()
            .fake_dns
            .expect("FakeDNS status")
            .capacity_failures,
        1
    );
}

#[tokio::test]
async fn general_record_engine_is_shared_with_wire_capture_and_preserves_dns_semantics() {
    let owner = Name::from_ascii("service.example.").expect("owner");
    let target = Name::from_ascii("origin.example.").expect("target");
    let backend = MockBackend::new(response_result(
        ResponseCode::NoError,
        vec![
            Record::from_rdata(owner.clone(), 120, RData::CNAME(CNAME(target.clone()))),
            Record::from_rdata(
                owner,
                20,
                RData::TXT(TXT::new(vec!["mptunnel=daily-use".to_string()])),
            ),
        ],
        Vec::new(),
        vec![Record::from_rdata(
            target,
            90,
            RData::A(A("192.0.2.91".parse().expect("additional address"))),
        )],
        Duration::from_secs(20),
    ));
    let runtime = DnsGeneration::compile_with_factory(
        policy(
            61,
            vec![direct_udp("records", "192.0.2.53:53")],
            vec![ipv4_plan("default", vec![upstream_id("records")])],
            Vec::new(),
            "default",
        ),
        &MapFactory {
            backends: HashMap::from([(upstream_id("records"), test_backend(backend.clone()))]),
        },
    )
    .expect("record runtime");

    let record = runtime
        .query_record(&domain("service.example"), RecordType::TXT)
        .await
        .expect("general record query");
    assert_eq!(record.metadata().generation(), 61);
    assert!(!record.is_stale());
    assert_eq!(
        record.message().metadata.response_code,
        ResponseCode::NoError
    );
    assert!(matches!(
        &record.message().answers[0].data,
        RData::CNAME(CNAME(name)) if name == &Name::from_ascii("origin.example.").expect("target")
    ));
    assert!(matches!(
        &record.message().answers[1].data,
        RData::TXT(txt) if txt.to_string().contains("mptunnel=daily-use")
    ));
    assert!(matches!(
        &record.message().additionals[0].data,
        RData::A(A(address))
            if *address == "192.0.2.91".parse::<std::net::Ipv4Addr>().expect("address")
    ));

    let wire = runtime
        .answer_wire_query(
            &wire_query(0x6110, "service.example.", RecordType::TXT),
            Duration::from_secs(10),
            1_232,
        )
        .await
        .expect("cached wire response");
    let wire = Message::from_vec(&wire).expect("decoded wire response");
    assert_eq!(wire.metadata.id, 0x6110);
    assert_eq!(wire.metadata.response_code, ResponseCode::NoError);
    assert_eq!(wire.answers.len(), 2);
    assert_eq!(wire.additionals.len(), 1);
    assert!(
        wire.answers
            .iter()
            .chain(wire.additionals.iter())
            .all(|record| record.ttl <= 10)
    );
    assert_eq!(
        backend.calls(),
        1,
        "record API and wire capture must share one cache"
    );
}

#[tokio::test]
async fn nxdomain_and_nodata_remain_distinct_across_the_shared_negative_cache() {
    let backend = SequenceBackend::new([
        MockStep {
            result: response_result(
                ResponseCode::NXDomain,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Duration::from_secs(30),
            ),
            delay: Duration::ZERO,
        },
        MockStep {
            result: response_result(
                ResponseCode::NoError,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Duration::from_secs(30),
            ),
            delay: Duration::ZERO,
        },
    ]);
    let runtime = DnsGeneration::compile_with_factory(
        policy(
            62,
            vec![direct_udp("negative", "192.0.2.53:53")],
            vec![ipv4_plan("default", vec![upstream_id("negative")])],
            Vec::new(),
            "default",
        ),
        &MapFactory {
            backends: HashMap::from([(upstream_id("negative"), test_backend(backend.clone()))]),
        },
    )
    .expect("negative runtime");

    let nx_domain = domain("absent.example");
    let no_data = domain("empty.example");
    assert_eq!(
        runtime
            .query_record(&nx_domain, RecordType::TXT)
            .await
            .expect("NXDOMAIN")
            .message()
            .metadata
            .response_code,
        ResponseCode::NXDomain
    );
    assert_eq!(
        runtime
            .query_record(&no_data, RecordType::TXT)
            .await
            .expect("NODATA")
            .message()
            .metadata
            .response_code,
        ResponseCode::NoError
    );

    let nx_wire = runtime
        .answer_wire_query(
            &wire_query(0x6201, "absent.example.", RecordType::TXT),
            Duration::from_secs(5),
            1_232,
        )
        .await
        .expect("cached NXDOMAIN wire response");
    let no_data_wire = runtime
        .answer_wire_query(
            &wire_query(0x6202, "empty.example.", RecordType::TXT),
            Duration::from_secs(5),
            1_232,
        )
        .await
        .expect("cached NODATA wire response");
    assert_eq!(
        Message::from_vec(&nx_wire)
            .expect("decoded NXDOMAIN")
            .metadata
            .response_code,
        ResponseCode::NXDomain
    );
    let no_data_wire = Message::from_vec(&no_data_wire).expect("decoded NODATA");
    assert_eq!(no_data_wire.metadata.response_code, ResponseCode::NoError);
    assert!(no_data_wire.answers.is_empty());
    assert_eq!(backend.calls(), 2, "both negative forms must remain cached");
}

#[tokio::test]
async fn captured_dns_wire_uses_split_policy_and_maps_failures_to_dns_codes() {
    let public = MockBackend::new(MockResult::Positive(vec![
        "192.0.2.90".parse().expect("public"),
    ]));
    let private = MockBackend::new(MockResult::Positive(vec![
        "10.0.0.90".parse().expect("private"),
    ]));
    let failed = MockBackend::new(MockResult::Failed);
    let negative = MockBackend::new(MockResult::Negative);
    let compiled_policy = policy(
        51,
        vec![
            direct_udp("public-upstream", "192.0.2.53:53"),
            direct_udp("private-upstream", "10.0.0.53:53"),
            direct_udp("failed-upstream", "192.0.2.54:53"),
            direct_udp("negative-upstream", "192.0.2.55:53"),
        ],
        vec![
            DnsPlanSpec::new(plan_id("public"), vec![upstream_id("public-upstream")]),
            DnsPlanSpec::new(plan_id("private"), vec![upstream_id("private-upstream")]),
            DnsPlanSpec::new(plan_id("failed"), vec![upstream_id("failed-upstream")]),
            DnsPlanSpec::new(plan_id("negative"), vec![upstream_id("negative-upstream")]),
        ],
        vec![
            DnsRuleSpec {
                id: rule_id("private-zone"),
                matcher: DnsRuleMatch::Suffix(domain("lan.example")),
                plan: plan_id("private"),
                explanation: None,
            },
            DnsRuleSpec {
                id: rule_id("failed-zone"),
                matcher: DnsRuleMatch::Suffix(domain("failed.example")),
                plan: plan_id("failed"),
                explanation: None,
            },
            DnsRuleSpec {
                id: rule_id("negative-zone"),
                matcher: DnsRuleMatch::Suffix(domain("missing.example")),
                plan: plan_id("negative"),
                explanation: None,
            },
        ],
        "public",
    );
    let runtime = DnsGeneration::compile_with_factory(
        compiled_policy,
        &MapFactory {
            backends: HashMap::from([
                (upstream_id("public-upstream"), test_backend(public)),
                (upstream_id("private-upstream"), test_backend(private)),
                (upstream_id("failed-upstream"), test_backend(failed)),
                (upstream_id("negative-upstream"), test_backend(negative)),
            ]),
        },
    )
    .expect("runtime");

    let private = runtime
        .answer_wire_query(
            &wire_query(7, "router.lan.example.", RecordType::A),
            Duration::from_secs(30),
            1_232,
        )
        .await
        .expect("private response");
    let private = Message::from_vec(&private).expect("decoded private response");
    assert!(matches!(
        &private.answers[0].data,
        RData::A(A(address))
            if *address == "10.0.0.90".parse::<std::net::Ipv4Addr>().expect("private")
    ));

    let failed = runtime
        .answer_wire_query(
            &wire_query(8, "query.failed.example.", RecordType::A),
            Duration::from_secs(30),
            1_232,
        )
        .await
        .expect("SERVFAIL response");
    assert_eq!(
        Message::from_vec(&failed)
            .expect("decoded SERVFAIL")
            .metadata
            .response_code,
        ResponseCode::ServFail
    );

    let missing = runtime
        .answer_wire_query(
            &wire_query(9, "missing.example.", RecordType::A),
            Duration::from_secs(30),
            1_232,
        )
        .await
        .expect("NXDOMAIN response");
    assert_eq!(
        Message::from_vec(&missing)
            .expect("decoded NXDOMAIN")
            .metadata
            .response_code,
        ResponseCode::NXDomain
    );
}

#[tokio::test]
async fn captured_dns_wire_rejects_transfers_or_malformed_requests_and_truncates() {
    let addresses = (1..=16)
        .map(|suffix| format!("2001:db8::{suffix}").parse().expect("IPv6"))
        .collect();
    let runtime =
        DnsGeneration::from_test_answers(HashMap::from([("large.example".to_string(), addresses)]));

    let unsupported = runtime
        .answer_wire_query(
            &wire_query(11, "large.example.", RecordType::AXFR),
            Duration::from_secs(60),
            512,
        )
        .await
        .expect("NOTIMP response");
    assert_eq!(
        Message::from_vec(&unsupported)
            .expect("decoded NOTIMP")
            .metadata
            .response_code,
        ResponseCode::NotImp
    );

    let mut multiple = Message::query();
    multiple.add_query(Query::query(
        Name::from_ascii("one.example.").expect("name"),
        RecordType::A,
    ));
    multiple.add_query(Query::query(
        Name::from_ascii("two.example.").expect("name"),
        RecordType::A,
    ));
    let malformed = runtime
        .answer_wire_query(
            &multiple.to_vec().expect("multiple query"),
            Duration::from_secs(60),
            512,
        )
        .await
        .expect("FORMERR response");
    assert_eq!(
        Message::from_vec(&malformed)
            .expect("decoded FORMERR")
            .metadata
            .response_code,
        ResponseCode::FormErr
    );

    assert!(matches!(
        runtime
            .answer_wire_query(&[0, 1, 2], Duration::from_secs(60), 512)
            .await,
        Err(DnsWireError::Decode(_))
    ));
    assert!(matches!(
        runtime
            .answer_wire_query(
                &vec![0; MAX_DNS_WIRE_MESSAGE_BYTES + 1],
                Duration::from_secs(60),
                512
            )
            .await,
        Err(DnsWireError::RequestTooLarge { .. })
    ));

    let truncated = runtime
        .answer_wire_query(
            &wire_query(12, "large.example.", RecordType::AAAA),
            Duration::from_secs(60),
            100,
        )
        .await
        .expect("truncated response");
    let truncated = Message::from_vec(&truncated).expect("decoded truncated response");
    assert!(truncated.metadata.truncation);
    assert!(truncated.answers.is_empty());
}

#[tokio::test]
async fn negative_and_oversized_answers_fail_closed() {
    let negative = MockBackend::new(MockResult::Negative);
    let fallback = MockBackend::new(MockResult::Positive(vec![
        "192.0.2.30".parse().expect("address"),
    ]));
    let mut default = ipv4_plan(
        "default",
        vec![upstream_id("negative"), upstream_id("fallback")],
    );
    default.limits.max_answers = 2;
    let negative_policy = policy(
        1,
        vec![
            direct_udp("negative", "192.0.2.53:53"),
            direct_udp("fallback", "192.0.2.54:53"),
        ],
        vec![default],
        Vec::new(),
        "default",
    );
    let factory = MapFactory {
        backends: HashMap::from([
            (upstream_id("negative"), test_backend(negative.clone())),
            (upstream_id("fallback"), test_backend(fallback.clone())),
        ]),
    };
    let runtime = DnsGeneration::compile_with_factory(negative_policy, &factory).expect("runtime");
    assert!(matches!(
        runtime.resolve(&domain("missing.example")).await,
        Err(DnsRuntimeError::NoRecords { .. })
    ));
    assert_eq!(negative.calls(), 1);
    assert_eq!(
        fallback.calls(),
        0,
        "authoritative negative must not fall through"
    );

    let oversized = MockBackend::new(MockResult::Positive(vec![
        "192.0.2.1".parse().expect("address"),
        "192.0.2.2".parse().expect("address"),
        "192.0.2.3".parse().expect("address"),
    ]));
    let policy = policy(
        2,
        vec![direct_udp("oversized", "192.0.2.55:53")],
        vec![{
            let mut plan = ipv4_plan("default", vec![upstream_id("oversized")]);
            plan.limits.max_answers = 2;
            plan
        }],
        Vec::new(),
        "default",
    );
    let runtime = DnsGeneration::compile_with_factory(
        policy,
        &MapFactory {
            backends: HashMap::from([(upstream_id("oversized"), test_backend(oversized))]),
        },
    )
    .expect("runtime");
    assert!(matches!(
        runtime.resolve(&domain("many.example")).await,
        Err(DnsRuntimeError::TooManyAnswers {
            count: 3,
            maximum: 2,
            ..
        })
    ));
}

#[tokio::test]
async fn ordered_deadline_and_racing_expected_cidr_failover_are_bounded() {
    let failed = MockBackend::delayed(MockResult::Failed, Duration::from_millis(25));
    let slow = MockBackend::delayed(
        MockResult::Positive(vec!["192.0.2.40".parse().expect("address")]),
        Duration::from_millis(60),
    );
    let mut plan = ipv4_plan("default", vec![upstream_id("failed"), upstream_id("slow")]);
    plan.limits.lookup_timeout = Duration::from_millis(45);
    let ordered_policy = policy(
        1,
        vec![
            direct_udp("failed", "192.0.2.53:53"),
            direct_udp("slow", "192.0.2.54:53"),
        ],
        vec![plan],
        Vec::new(),
        "default",
    );
    let runtime = DnsGeneration::compile_with_factory(
        ordered_policy,
        &MapFactory {
            backends: HashMap::from([
                (upstream_id("failed"), test_backend(failed)),
                (upstream_id("slow"), test_backend(slow)),
            ]),
        },
    )
    .expect("runtime");
    let start = Instant::now();
    assert!(matches!(
        runtime.resolve(&domain("deadline.example")).await,
        Err(DnsRuntimeError::Timeout { .. })
    ));
    assert!(
        start.elapsed() < Duration::from_millis(90),
        "deadline was applied per upstream instead of once: {:?}",
        start.elapsed()
    );

    let primary = MockBackend::delayed(
        MockResult::Positive(vec!["198.51.100.40".parse().expect("address")]),
        Duration::from_millis(200),
    );
    let polluted = MockBackend::delayed(
        MockResult::Positive(vec!["203.0.113.40".parse().expect("address")]),
        Duration::from_millis(5),
    );
    let fallback = MockBackend::delayed(
        MockResult::Positive(vec!["192.0.2.40".parse().expect("address")]),
        Duration::from_millis(5),
    );
    let mut plan = ipv4_plan(
        "default",
        vec![
            upstream_id("primary"),
            upstream_id("polluted"),
            upstream_id("fallback"),
        ],
    );
    plan.limits.lookup_timeout = Duration::from_millis(100);
    plan.upstream_strategy = DnsUpstreamStrategy::Race {
        fallback_delay: Duration::from_millis(10),
    };
    plan.expected_cidrs = vec!["192.0.2.0/24".parse().expect("expected CIDR")];
    let runtime = DnsGeneration::compile_with_factory(
        policy(
            2,
            vec![
                direct_udp("primary", "192.0.2.53:53"),
                direct_udp("polluted", "192.0.2.54:53"),
                direct_udp("fallback", "192.0.2.55:53"),
            ],
            vec![plan],
            Vec::new(),
            "default",
        ),
        &MapFactory {
            backends: HashMap::from([
                (upstream_id("primary"), test_backend(primary.clone())),
                (upstream_id("polluted"), test_backend(polluted.clone())),
                (upstream_id("fallback"), test_backend(fallback.clone())),
            ]),
        },
    )
    .expect("racing runtime");

    let start = Instant::now();
    let raced = runtime
        .resolve(&domain("race.example"))
        .await
        .expect("CIDR-validated hedge");
    assert_eq!(
        raced.addresses().as_ref(),
        &["192.0.2.40".parse::<IpAddr>().expect("expected answer")]
    );
    assert!(
        start.elapsed() < Duration::from_millis(80),
        "hedged fallback did not beat the stalled primary: {:?}",
        start.elapsed()
    );
    assert_eq!(primary.calls(), 1);
    assert_eq!(polluted.calls(), 1);
    assert_eq!(fallback.calls(), 1);
    let snapshot = runtime.runtime_snapshot();
    assert_eq!(snapshot.plans[0].upstreams[1].rejected_answers, 1);
    assert_eq!(snapshot.plans[0].upstreams[0].canceled_attempts, 1);
}

#[cfg(any(target_os = "linux", target_os = "windows"))]
#[tokio::test]
async fn direct_factory_never_bypasses_a_named_outbound() {
    let proxy = OutboundId::parse("proxy").expect("outbound");
    let upstream = DnsUpstreamSpec {
        id: upstream_id("secure"),
        endpoint: DnsUpstreamEndpoint::Tls {
            bootstrap: "192.0.2.53:853".parse().expect("bootstrap"),
            server_name: domain("resolver.example"),
        },
        egress: DnsEgressSpec::Outbound(proxy.clone()),
    };
    let policy = Arc::new(
        CompiledDnsPolicy::compile(
            1,
            DnsPolicySpec {
                upstreams: vec![upstream],
                outbound_capabilities: vec![DnsOutboundCapabilitySpec::new(
                    proxy.clone(),
                    NetworkSet::TCP,
                    true,
                )],
                plans: vec![{
                    let mut plan =
                        DnsPlanSpec::new(plan_id("default"), vec![upstream_id("secure")]);
                    plan.security = DnsSecurityPolicy::RequireEncrypted;
                    plan
                }],
                rules: Vec::new(),
                hosts: vec![DnsHostSpec {
                    domain: domain("carrier.example"),
                    addresses: vec!["198.51.100.10".parse().expect("carrier address")],
                }],
                fake_dns: None,
                default_plan: plan_id("default"),
            },
        )
        .expect("policy"),
    );
    assert_eq!(
        DnsGeneration::compile(policy.clone()).expect_err("must require connector"),
        DnsRuntimeError::MissingEgressConnector {
            upstream: upstream_id("secure"),
            outbound: proxy.clone(),
        }
    );
    assert!(matches!(
        DnsGeneration::compile_prepublication(
            policy.clone(),
            &[domain("unmapped-carrier.example")]
        ),
        Err(DnsRuntimeError::PrepublicationDnsRequiresDirect {
            ref outbound,
            ..
        }) if outbound == &proxy
    ));

    let bootstrap = DnsGeneration::compile_prepublication(policy, &[domain("carrier.example")])
        .expect("exact host override needs no pre-publication backend");
    let resolved = bootstrap
        .resolve(&domain("carrier.example"))
        .await
        .expect("exact host bootstrap");
    assert_eq!(
        resolved.addresses().as_ref(),
        &["198.51.100.10".parse::<IpAddr>().expect("carrier address")]
    );
    assert!(
        bootstrap.runtime_snapshot().plans[0].upstreams.is_empty(),
        "host-backed pre-publication must not instantiate the routed DNS upstream"
    );
}

#[tokio::test]
async fn explicit_udp_never_falls_back_to_system_or_hosts() {
    let blackhole = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("blackhole resolver");
    let mut plan = DnsPlanSpec::new(plan_id("default"), vec![upstream_id("explicit")]);
    plan.limits.lookup_timeout = Duration::from_millis(40);
    let policy = policy(
        1,
        vec![direct_udp(
            "explicit",
            &blackhole.local_addr().expect("resolver").to_string(),
        )],
        vec![plan],
        Vec::new(),
        "default",
    );
    let runtime = DnsGeneration::compile(policy).expect("explicit runtime");
    let result = runtime.resolve(&domain("localhost")).await;
    assert!(
        matches!(
            result,
            Err(DnsRuntimeError::Timeout { .. })
                | Err(DnsRuntimeError::AllUpstreamsFailed { .. })
                | Err(DnsRuntimeError::NoRecords { .. })
        ),
        "explicit DNS unexpectedly used the system resolver: {result:?}"
    );
}

#[tokio::test]
async fn every_explicit_transport_configures_native_sockets_before_network_io() {
    let bootstrap: SocketAddr = "127.0.0.1:9".parse().expect("bootstrap");
    let endpoints = [
        DnsUpstreamEndpoint::Udp { bootstrap },
        DnsUpstreamEndpoint::Tcp { bootstrap },
        DnsUpstreamEndpoint::UdpTcp { bootstrap },
        DnsUpstreamEndpoint::Tls {
            bootstrap,
            server_name: domain("resolver.example"),
        },
        DnsUpstreamEndpoint::Https {
            bootstrap,
            server_name: domain("resolver.example"),
            path: "/dns-query".to_string(),
        },
        DnsUpstreamEndpoint::Quic {
            bootstrap,
            server_name: domain("resolver.example"),
        },
    ];
    for endpoint in endpoints {
        let policy = policy(
            1,
            vec![DnsUpstreamSpec::direct(upstream_id("explicit"), endpoint)],
            vec![{
                let mut plan = DnsPlanSpec::new(plan_id("default"), vec![upstream_id("explicit")]);
                plan.limits.lookup_timeout = Duration::from_millis(100);
                plan
            }],
            Vec::new(),
            "default",
        );
        let configurator = Arc::new(RejectingDnsSocketConfigurator::default());
        let runtime = DnsGeneration::compile_with_native_sockets(policy, configurator.clone())
            .expect("runtime");
        assert!(
            runtime
                .resolve(&domain("socket-policy.example"))
                .await
                .is_err(),
            "rejecting native socket hook must stop DNS I/O"
        );
        let requests = configurator.requests.lock().expect("requests");
        assert!(
            !requests.is_empty(),
            "transport created no protected socket"
        );
        assert!(requests.iter().all(|request| {
            request.remote_addr == bootstrap
                && request.purpose == crate::transport::NativeEgressPurpose::Dns
        }));
    }
}

#[test]
fn source_bound_dns_rejects_bootstrap_family_mismatch_at_compile_time() {
    let compiled = policy(
        1,
        vec![direct_udp("explicit", "192.0.2.53:53")],
        vec![DnsPlanSpec::new(
            plan_id("default"),
            vec![upstream_id("explicit")],
        )],
        Vec::new(),
        "default",
    );
    let plan = compiled.plan(&plan_id("default")).expect("plan");
    let upstream = compiled
        .upstream(&upstream_id("explicit"))
        .expect("upstream");
    let result = DirectDnsBackendFactory::build_backend_with_policy(
        plan,
        upstream,
        DnsNativeSocketPolicy::bind_source(
            Arc::new(crate::transport::SystemNativeSocketConfigurator),
            "2001:db8::1".parse().expect("source"),
        ),
    );
    assert!(matches!(result, Err(DnsRuntimeError::Build { .. })));
}

#[tokio::test]
async fn system_resolution_occurs_only_for_an_explicit_system_upstream() {
    let policy = policy(
        1,
        vec![DnsUpstreamSpec::direct(
            upstream_id("system"),
            DnsUpstreamEndpoint::System,
        )],
        vec![DnsPlanSpec::new(
            plan_id("default"),
            vec![upstream_id("system")],
        )],
        Vec::new(),
        "default",
    );
    let runtime = DnsGeneration::compile(policy).expect("explicit system runtime");
    let addresses = runtime
        .resolve(&domain("localhost"))
        .await
        .expect("system localhost");
    assert!(
        addresses.addresses().iter().any(IpAddr::is_loopback),
        "{addresses:?}"
    );
}

#[test]
fn dot_doh_and_doq_preserve_literal_bootstrap_and_tls_identity() {
    let dot = DnsUpstreamEndpoint::Tls {
        bootstrap: "192.0.2.53:853".parse().expect("bootstrap"),
        server_name: domain("resolver.example"),
    };
    let dot_config = name_server_config(&dot).expect("DoT config");
    assert_eq!(dot_config.ip, "192.0.2.53".parse::<IpAddr>().expect("IP"));
    assert!(matches!(
        &dot_config.connections[0].protocol,
        ProtocolConfig::Tls { server_name } if server_name.as_ref() == "resolver.example"
    ));
    let doq = DnsUpstreamEndpoint::Quic {
        bootstrap: "192.0.2.54:8853".parse().expect("bootstrap"),
        server_name: domain("doq.example"),
    };
    let doq_config = name_server_config(&doq).expect("DoQ config");
    assert_eq!(doq_config.ip, "192.0.2.54".parse::<IpAddr>().expect("IP"));
    assert_eq!(doq_config.connections[0].port, 8853);
    assert!(matches!(
        &doq_config.connections[0].protocol,
        ProtocolConfig::Quic { server_name } if server_name.as_ref() == "doq.example"
    ));

    let doh_policy = policy(
        1,
        vec![DnsUpstreamSpec::direct(
            upstream_id("doh"),
            DnsUpstreamEndpoint::Https {
                bootstrap: "198.51.100.53:8443".parse().expect("bootstrap"),
                server_name: domain("doh.example"),
                path: "/dns-query".to_string(),
            },
        )],
        vec![DnsPlanSpec::new(
            plan_id("default"),
            vec![upstream_id("doh")],
        )],
        Vec::new(),
        "default",
    );
    let plan = doh_policy.plan(&plan_id("default")).expect("plan");
    let upstream = doh_policy.upstream(&upstream_id("doh")).expect("upstream");
    let backend = DohDnsBackend::compile(plan, upstream, DnsNativeSocketPolicy::default())
        .expect("DoH backend");
    assert_eq!(
        backend.inner.bootstrap,
        "198.51.100.53:8443".parse().expect("bootstrap")
    );
    assert_eq!(backend.inner.server_name, "doh.example");
    assert_eq!(backend.inner.authority, "doh.example:8443");
    assert_eq!(backend.inner.path, "/dns-query");
    // `DohDnsBackend` emits RFC 8484 POST and binds `doh.example` to the
    // literal bootstrap while preserving `doh.example:8443` as authority.
}
